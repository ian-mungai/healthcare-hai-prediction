"""Check launch resource calculations through the real CLI with synthetic host and Docker readings.

Run ``.venv/bin/python -m scripts.lakehouse.run_resource_checks``. Reports stay in data/e2e/resource_limits/.
No Docker daemon, catalog or AWS service is contacted. These process-level contracts and configuration checks do not
prove live container enforcement. Failure modes 526 to 543 are in plans/resource_limits_20261008/failure_modes.md.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import shlex
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
GIB = 1024**3
SERVICES = ("spark", "analytics", "analytics-dbt", "analytics-ui")


def executable(path: Path, body: str) -> None:
    """Write a synthetic command response without executing downloaded code."""
    path.write_text("#!/bin/sh\nset -eu\n" + body + "\n")
    path.chmod(0o700)


def response(value: str) -> str:
    """Return a shell statement that prints only the supplied synthetic value."""
    return "printf '%s\\n' " + shlex.quote(value)


def scenario(
    folder: Path, *, mac_gib: int = 64, vm_gib: int = 2, host_cpus: str = "20", docker_cpus: str = "16", total: str = str(32 * GIB), usage: str = "3GiB"
) -> dict[str, Any]:
    """Run the launch planner twice against bounded synthetic command responses."""
    folder.mkdir(parents=True, exist_ok=True)
    binary = folder / "bin"
    binary.mkdir(exist_ok=True)
    executable(
        binary / "docker",
        'case "$*" in\n'
        + f"  'info --format {{{{.MemTotal}}}}') {response(total)} ;;\n"
        + f"  'info --format {{{{.NCPU}}}}') {response(docker_cpus)} ;;\n"
        + f"  'stats --no-stream --format {{{{.MemUsage}}}}') {response(usage + ' / 32GiB')} ;;\n"
        + '  *) echo "synthetic Docker rejects this command" >&2; exit 93 ;;\nesac',
    )
    executable(binary / "sysctl", response(host_cpus))
    executable(binary / "ps", response(f"{vm_gib * GIB // 1024} com.apple.Virtualization.VirtualMachine"))
    executable(
        binary / "vm_stat",
        response(
            f"Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: {mac_gib * GIB // 16384}.\nFile-backed pages: 0.\nPages purgeable: 0."
        ),
    )
    env = {**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"]}
    command = ["-m", "scripts.lakehouse.memory_budget", "--launch-json"]
    runs = [run_command(sys.executable, command, cwd=ROOT, env=env, timeout=20) for _ in range(2)]
    (folder / "process.log").write_text("\n".join(f"exit={run.returncode}\n{run.stdout}{run.stderr}" for run in runs))
    payload = json.loads(runs[0].stdout) if runs[0].returncode == 0 else None
    return {"exit": runs[0].returncode, "error": runs[0].stderr, "payload": payload, "repeat_equal": runs[0].stdout == runs[1].stdout}


def main() -> int:
    """Retain repeatable results and return nonzero for any broken launch contract."""
    out = ROOT / "data/e2e/resource_limits" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    out.mkdir(parents=True)
    cases: dict[str, tuple[dict[str, Any], dict[str, str]]] = {
        "docker_cap": ({}, {"JOB_MEMORY_LIMIT": str(21 * GIB), "DUCKDB_MEMORY_LIMIT": "18GB", "SPARK_JOB_MEMORY": "19g", "JOB_THREADS": "16"}),
        "mac_cap": (
            {"mac_gib": 10, "docker_cpus": "8"},
            {"JOB_MEMORY_LIMIT": str(8 * GIB), "DUCKDB_MEMORY_LIMIT": "6GB", "SPARK_JOB_MEMORY": "7g", "JOB_THREADS": "8"},
        ),
        "vm_at_cap": (
            {"mac_gib": 3, "vm_gib": 32, "host_cpus": "6"},
            {"JOB_MEMORY_LIMIT": str(21 * GIB), "DUCKDB_MEMORY_LIMIT": "18GB", "SPARK_JOB_MEMORY": "19g", "JOB_THREADS": "6"},
        ),
        "container_floor": ({"mac_gib": 6}, {"JOB_MEMORY_LIMIT": str(4 * GIB), "DUCKDB_MEMORY_LIMIT": "3GB", "SPARK_JOB_MEMORY": "3g", "JOB_THREADS": "16"}),
        # [543] Memory the VM already holds and no container uses is reusable: 6 + (14 - 1 - 2) - 2 = 15 GiB.
        "vm_holds_memory": (
            {"mac_gib": 6, "vm_gib": 14, "usage": "1GiB"},
            {"JOB_MEMORY_LIMIT": str(15 * GIB), "DUCKDB_MEMORY_LIMIT": "12GB", "SPARK_JOB_MEMORY": "13g", "JOB_THREADS": "16"},
        ),
    }
    results: dict[str, bool] = {}
    for name, (inputs, expected) in cases.items():
        observed = scenario(out / name, **inputs)
        results[name] = observed["exit"] == 0 and observed["payload"].get("environment") == expected
        results[name + "_repeat"] = observed["exit"] == 0 and observed["repeat_equal"]
    failures: dict[str, tuple[dict[str, Any], str]] = {
        "below_floor": ({"mac_gib": 5}, "free for DuckDB or Spark"),
        "bad_usage": ({"usage": "unknown"}, "unknown Docker size"),
        "bad_total": ({"total": "invalid"}, "Docker memory"),
        "zero_docker_cpus": ({"docker_cpus": "0"}, "CPU count"),
        "invalid_host_cpus": ({"host_cpus": "invalid"}, "CPU count"),
    }
    for name, (inputs, error) in failures.items():
        observed = scenario(out / name, **inputs)
        results[name] = observed["exit"] != 0 and error in observed["error"] and observed["payload"] is None
    # PyYAML ships no type hints; this matches the repository's existing safe-loader convention.
    compose = importlib.import_module("yaml").safe_load((ROOT / "docker-compose.yaml").read_text())
    for name in SERVICES:
        service = compose["services"][name]
        results[name + "_container_limit"] = service.get("mem_limit") == "${JOB_MEMORY_LIMIT:?run through a lakehouse launch script}"
        results[name + "_threads_passed"] = "JOB_THREADS" in service.get("environment", {})
    profiles = (ROOT / "dbt/profiles.yml").read_text()
    results["dbt_parallelism_computed"] = "env_var('JOB_THREADS')" in profiles and "threads: 4" not in profiles
    results["spark_parallelism_computed"] = "local[4]" not in (ROOT / "scripts/lakehouse/session.py").read_text()
    results.update(launch_contracts(out / "launches"))
    result = {
        "scope": "Real planner CLI, synthetic host/Docker command responses and source configuration checks; no live container enforcement",
        "command": ".venv/bin/python -m scripts.lakehouse.run_resource_checks",
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "python": sys.version.split()[0],
        "input_hash": hashlib.sha256(json.dumps({"cases": cases, "failures": failures}, sort_keys=True).encode()).hexdigest(),
        "source_hashes": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in ("scripts/lakehouse/memory_budget.py", "docker-compose.yaml", "dbt/profiles.yml")
        },
        "results": results,
        "passed": sum(results.values()),
        "total": len(results),
    }
    (out / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(f"Resource contracts: {result['passed']}/{result['total']}; {out.relative_to(ROOT)}/report.json\n")
    return 0 if all(results.values()) else 1


def launch_contracts(folder: Path) -> dict[str, bool]:
    """Exercise actual launch scripts with synthetic metrics, catalog setup and Docker execution [534] [538]."""
    scenario(folder)
    binary = folder / "bin"
    docker_path = binary / "docker"
    capture = (
        'case " $* " in *" --no-deps "*) no_deps=true ;; *) no_deps=false ;; esac\n'
        'printf \'{"cap":"%s","engine":"%s","threads":"%s","no_deps":%s}\\n\' '
        '"${JOB_MEMORY_LIMIT:-missing}" "${DUCKDB_MEMORY_LIMIT:-missing}" "${JOB_THREADS:-missing}" "$no_deps"'
    )
    docker_path.write_text(docker_path.read_text().replace("  *) echo", "  compose*) " + capture + " ;;\n  *) echo"))
    checkout = folder / "checkout"
    paths = ["scripts/process.py", "scripts/lakehouse/__init__.py"]
    paths += [f"scripts/lakehouse/{name}" for name in ("catalog.py", "memory_budget.py", "run_lock.py", "dbt.sh", "query.sh", "ui.sh")]
    for name in paths:
        dest = checkout / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, dest)
    python = checkout / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    executable(python, 'if [ "$*" = "-m scripts.lakehouse.catalog up" ]; then exit 0; fi\nexec ' + shlex.quote(sys.executable) + ' "$@"')
    private = checkout / "data/lakehouse/secrets/compose.env"
    private.parent.mkdir(parents=True)
    private.write_text("POLARIS_MEMORY_LIMIT=1024m\n")
    env = {**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"], "JOB_MEMORY_LIMIT": "1", "DUCKDB_MEMORY_LIMIT": "1GB", "JOB_THREADS": "999"}
    expected = {"cap": str(21 * GIB), "engine": "18GB", "threads": "16", "no_deps": True}
    results = {}
    for name in ("dbt", "query", "ui"):
        runs = [run_command("bash", [str(checkout / f"scripts/lakehouse/{name}.sh")], cwd=checkout, env=env, timeout=30) for _ in range(2)]
        (folder / f"{name}.log").write_text("\n".join(run.stdout + run.stderr for run in runs))
        results[name + "_launch_plan"] = all(run.returncode == 0 and json.loads(run.stdout) == expected for run in runs)
    spark = run_command(sys.executable, ["-m", "scripts.lakehouse.catalog", "job", "bronze_e2e"], cwd=checkout, env=env, timeout=30)
    (folder / "spark.log").write_text(spark.stdout + spark.stderr)
    results["spark_launch_plan"] = spark.returncode == 0 and json.loads(spark.stdout.splitlines()[-1]) == expected
    command = (
        "import sys; from scripts.lakehouse.run_staging_e2e import compose_run; "
        "code, out, err = compose_run(['analytics-dbt', 'build'], {'JOB_THREADS': '999', 'DUCKDB_MEMORY_LIMIT': '999GB'}); "
        "sys.stdout.write(out); sys.stderr.write(err); sys.exit(code)"
    )
    staging = run_command(sys.executable, ["-c", command], cwd=ROOT, env=env, timeout=30)
    (folder / "staging.log").write_text(staging.stdout + staging.stderr)
    results["staging_launch_overrides_stale_settings"] = staging.returncode == 0 and json.loads(staging.stdout) == expected
    # Only this copied launch environment is affected; the real catalog and secrets are never read or changed.
    executable(
        binary / "vm_stat", response("Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 1.\nFile-backed pages: 0.\nPages purgeable: 0.")
    )
    blocked = run_command("bash", [str(checkout / "scripts/lakehouse/dbt.sh")], cwd=checkout, env=env, timeout=30)
    results["low_memory_never_launches"] = blocked.returncode != 0 and not blocked.stdout and "free for DuckDB or Spark" in blocked.stderr
    # A pre-change env file and low free memory must not prevent stopping the old catalog [535].
    admin_env = {key: value for key, value in env.items() if key not in {"JOB_MEMORY_LIMIT", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS"}}
    stopped = run_command(sys.executable, ["-m", "scripts.lakehouse.catalog", "down"], cwd=checkout, env=admin_env, timeout=30)
    results["old_stack_down_needs_no_new_budget"] = stopped.returncode == 0
    return results


if __name__ == "__main__":
    raise SystemExit(main())
