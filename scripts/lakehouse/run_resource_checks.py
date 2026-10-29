"""Check launch resource calculations through the real CLI and the launcher with synthetic host and Docker readings.

Run ``.venv/bin/python -m scripts.lakehouse.run_resource_checks``. Reports stay in data/e2e/resource_limits/.
Each case starts a launcher on a test registry with a fake Docker daemon (scripts/lakehouse/fake_docker.py) and synthetic ``sysctl``,
``vm_stat`` and ``ps`` commands on ``PATH``; no Docker daemon, catalog or AWS service is contacted. These process-level contracts and
configuration checks do not prove live container enforcement. Failure modes 526 to 543 are in plans/resource_limits_20261008/failure_modes.md.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse.run_launcher_e2e import GIB, Env, creates, host_tools
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ("spark", "analytics", "analytics-dbt", "analytics-ui")


def scenario(
    report: Path,
    name: str,
    *,
    mac_gib: int = 64,
    vm_gib: int = 2,
    host_cpus: str = "20",
    docker_cpus: int = 16,
    total: int = 32 * GIB,
    usage: int | None = 3 * GIB,
) -> tuple[dict[str, Any], Env, dict[str, str]]:
    """Run the launch planner twice against synthetic readings; returns the results, the environment (still open) and the command environment."""
    env = Env(name, report)
    env.daemon.mem_total = total
    env.daemon.cpus = docker_cpus
    if usage is not None:
        env.daemon.create_rogue("other_project", {}, running=True)
        env.daemon.usage["other_project"] = usage
    binary = env.work / "bin"
    host_tools(binary, mac_gib=mac_gib, vm_gib=vm_gib, host_cpus=host_cpus)
    command_env = {**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"]}
    command = ["-m", "scripts.lakehouse.memory_budget", "--launch-json", "--test-registry", str(env.registry)]
    runs = [run_command(sys.executable, command, cwd=ROOT, env=command_env, timeout=120) for _ in range(2)]
    (env.work / "process.log").write_text("\n".join(f"exit={run.returncode}\n{run.stdout}{run.stderr}" for run in runs))
    payload = json.loads(runs[0].stdout) if runs[0].returncode == 0 else None
    result = {"exit": runs[0].returncode, "error": runs[0].stderr, "payload": payload, "repeat_equal": runs[0].stdout == runs[1].stdout}
    return result, env, command_env


def main() -> int:
    """Retain repeatable results and return nonzero for any broken launch contract."""
    out = ROOT / "data/e2e/resource_limits" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    out.mkdir(parents=True)
    cases: dict[str, tuple[dict[str, Any], dict[str, str]]] = {
        # Each allocation is 75% of the free figure: Docker free 32 - 3 - 8 = 21 GiB gives 15.75 GiB.
        "docker_cap": ({}, {"JOB_MEMORY_LIMIT": str(21 * GIB * 75 // 100), "DUCKDB_MEMORY_LIMIT": "13GB", "SPARK_JOB_MEMORY": "14g", "JOB_THREADS": "16"}),
        "mac_cap": (
            {"mac_gib": 10, "docker_cpus": 8},
            {"JOB_MEMORY_LIMIT": str(6 * GIB), "DUCKDB_MEMORY_LIMIT": "5GB", "SPARK_JOB_MEMORY": "5g", "JOB_THREADS": "8"},
        ),
        "vm_at_cap": (
            {"mac_gib": 3, "vm_gib": 32, "host_cpus": "6"},
            {"JOB_MEMORY_LIMIT": str(21 * GIB * 75 // 100), "DUCKDB_MEMORY_LIMIT": "13GB", "SPARK_JOB_MEMORY": "14g", "JOB_THREADS": "6"},
        ),
        # Mac free 8 - 2 = 6 GiB gives 4.5 GiB, above the 4 GiB container floor.
        "container_floor": (
            {"mac_gib": 8},
            {"JOB_MEMORY_LIMIT": str(9 * GIB // 2), "DUCKDB_MEMORY_LIMIT": "3GB", "SPARK_JOB_MEMORY": "4g", "JOB_THREADS": "16"},
        ),
        # [543] Memory the VM already holds and no container uses is reusable: 6 + (14 - 1 - 2) - 2 = 15 GiB free, 11.25 GiB allocated.
        "vm_holds_memory": (
            {"mac_gib": 6, "vm_gib": 14, "usage": 1 * GIB},
            {"JOB_MEMORY_LIMIT": str(45 * GIB // 4), "DUCKDB_MEMORY_LIMIT": "9GB", "SPARK_JOB_MEMORY": "10g", "JOB_THREADS": "16"},
        ),
    }
    results: dict[str, bool] = {}
    for name, (inputs, expected) in cases.items():
        observed, env, _ = scenario(out, name, **inputs)
        env.close()
        results[name] = observed["exit"] == 0 and observed["payload"].get("environment") == expected
        results[name + "_repeat"] = observed["exit"] == 0 and observed["repeat_equal"]
    failures: dict[str, tuple[dict[str, Any], str]] = {
        "below_floor": ({"mac_gib": 5}, "free for DuckDB or Spark"),
        # Free 6 - 2 = 4 GiB meets the floor but its 75% (3 GiB) does not.
        "cap_below_floor": ({"mac_gib": 6}, "after the 75% allocation cap"),
        "bad_total": ({"total": 0}, "Docker memory"),
        "zero_docker_cpus": ({"docker_cpus": 0}, "CPU count"),
        "invalid_host_cpus": ({"host_cpus": "invalid"}, "CPU count"),
    }
    for name, (inputs, error) in failures.items():
        observed, env, _ = scenario(out, name, **inputs)
        env.close()
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
    results.update(launch_contracts(out))
    results.update(cap_and_deadline_contracts())
    result = {
        "scope": "Real planner CLI and launcher, synthetic host readings, a fake Docker daemon and source configuration checks; no live container enforcement",
        "command": ".venv/bin/python -m scripts.lakehouse.run_resource_checks",
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "python": sys.version.split()[0],
        "input_hash": hashlib.sha256(json.dumps({"cases": cases, "failures": failures}, sort_keys=True).encode()).hexdigest(),
        "source_hashes": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in ("scripts/lakehouse/memory_budget.py", "orchestration/launcher.py", "docker-compose.yaml", "dbt/profiles.yml")
        },
        "results": results,
        "passed": sum(results.values()),
        "total": len(results),
    }
    (out / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(f"Resource contracts: {result['passed']}/{result['total']}; {out.relative_to(ROOT)}/report.json\n")
    return 0 if all(results.values()) else 1


def cap_and_deadline_contracts() -> dict[str, bool]:
    """The allocation cap and the memory wait deadline with stand-in figures; no process, Docker or host reading."""
    from scripts.lakehouse import memory_budget as mb

    budget = mb.compute(32 * GIB, [3 * GIB])
    plan = mb.LaunchPlan(budget, 16)
    pool = mb.pool_plan(plan, 2)
    reserve = mb.Budget(total=32 * GIB, used_by_containers=0, headroom=mb.HEADROOM, growth_reserve=40 * GIB)
    deadline = mb.MEMORY_WAIT_DEADLINE
    failure = mb.wait_failure(100.0, 100.0 + deadline, deadline, 2 * GIB, 6 * GIB)
    return {
        "cap_is_a_named_share_of_free": mb.ALLOCATION_SHARE_PERCENT == 75 and budget.free == 21 * GIB and budget.allocation == 21 * GIB * 75 // 100,
        "cap_never_exceeds_free": all(
            mb.Budget(32 * GIB, 0, mb.HEADROOM, growth_reserve=n).allocation <= mb.Budget(32 * GIB, 0, mb.HEADROOM).free for n in (0, GIB)
        ),
        "cap_of_a_negative_free_figure_stays_below_the_floor": reserve.free < 0 and reserve.allocation < mb.FLOOR,
        "plan_record_names_the_cap": budget.record()["allocation"] == budget.allocation and budget.record()["allocation_share_percent"] == 75,
        "pool_shares_split_the_capped_allocation": pool["record"]["share"] == budget.allocation // 2 and 2 * pool["record"]["share"] <= budget.allocation,
        "wait_deadline_is_positive_and_named": deadline > 0,
        "wait_before_the_deadline_goes_on": mb.wait_failure(100.0, 100.0 + deadline - 1, deadline, 2 * GIB, 6 * GIB) is None,
        "wait_at_the_deadline_names_cause_figures_and_deadline": failure is not None
        and "memory did not return within" in failure
        and "2.0 GiB free" in failure
        and "6.0 GiB needed" in failure,
        "wait_with_a_clock_set_back_does_not_expire": mb.wait_failure(100.0, 50.0, deadline, 2 * GIB, 6 * GIB) is None,
    }


def launch_contracts(folder: Path) -> dict[str, bool]:
    """Exercise the launch scripts and attempts with synthetic metrics: the plan reaches the container, stale settings do not [534] [538]."""
    results: dict[str, bool] = {}
    for name in ("dbt", "query", "ui"):
        text = (ROOT / f"scripts/lakehouse/{name}.sh").read_text()
        results[f"{name}_script_is_a_launcher_client"] = (
            "docker" not in text.replace("Docker", "").lower().replace("no docker command", "") and "orchestration.launcher" in text
        )
    observed, env, command_env = scenario(folder, "launches")
    expected = {"cap": str(21 * GIB * 75 // 100), "engine": "13GB", "threads": "16"}
    stale = {**command_env, "JOB_MEMORY_LIMIT": "1", "DUCKDB_MEMORY_LIMIT": "1GB", "JOB_THREADS": "999"}
    case_folder, _ = env.case_folders("rc")
    params = [
        f"case_host={json.dumps(str(case_folder))}",
        'case_container="/workspace/out/rc"',
        'database="/workspace/out/rc/f.duckdb"',
        'sql_file="/workspace/out/rc/b.sql"',
    ]
    arguments = ["-m", "orchestration.launcher", "--test-registry", str(env.registry), "run", "--template", "fixture_init", "--launch-id", "plan"]
    for param in params:
        arguments += ["--param", param]
    launched = run_command(sys.executable, arguments, cwd=ROOT, env=stale, timeout=120)
    body = (
        env.daemon.create_bodies[next(name for name in env.daemon.create_bodies if "-plan-" in name)]
        if creates(env, "-plan-")
        else {"Env": [], "HostConfig": {}}
    )
    container_env = dict(item.split("=", 1) for item in body["Env"])
    results["plan_reaches_the_container"] = (
        launched.returncode == 0
        and str(body["HostConfig"].get("Memory")) == expected["cap"]
        and container_env.get("DUCKDB_MEMORY_LIMIT") == expected["engine"]
        and container_env.get("JOB_THREADS") == expected["threads"]
    )
    results["staging_launch_overrides_stale_settings"] = container_env.get("DUCKDB_MEMORY_LIMIT") != "1GB" and container_env.get("JOB_THREADS") != "999"
    env.close()
    low, env_low, low_env = scenario(folder, "low_memory", mac_gib=5)
    del low
    blocked = run_command(
        sys.executable,
        [*arguments[:5], "run", "--template", "fixture_init", "--launch-id", "blocked", "--param", params[0]],
        cwd=ROOT,
        env=low_env,
        timeout=120,
    )
    blocked_args = ["-m", "orchestration.launcher", "--test-registry", str(env_low.registry), "run", "--template", "fixture_init", "--launch-id", "blocked"]
    blocked = run_command(sys.executable, blocked_args, cwd=ROOT, env=low_env, timeout=120)
    results["low_memory_never_launches"] = (
        blocked.returncode != 0 and not blocked.stdout and "free for DuckDB or Spark" in blocked.stderr and not creates(env_low, "-blocked-")
    )
    # A low reading must not prevent stopping the catalog (the old stack's stop needs no new budget) [535].
    stopped = run_command(
        sys.executable, ["-m", "scripts.lakehouse.catalog", "--test-registry", str(env_low.registry), "down"], cwd=ROOT, env=low_env, timeout=120
    )
    results["old_stack_down_needs_no_new_budget"] = stopped.returncode == 0
    env_low.close()
    return results


if __name__ == "__main__":
    raise SystemExit(main())
