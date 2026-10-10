"""Silver pipeline entry point: validate the build file with Great Expectations (step 6); build and publish come in step 7.

    .venv/bin/python -m scripts.lakehouse.silver validate [--database staging.duckdb] [--baselines PATH] [--suites PATH]

The validation runs in the ``quality`` Compose service with a memory limit read at launch, opens the build file
read-only and writes ``data/e2e/silver_quality/<run_id>/result.json`` with counts only. The record adds the launch
plan and the check that no Great Expectations expectation repeats a dbt test. Failure modes 639 to 651 and 713 in
plans/silver_processed_zone_20261009/plan.md.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import catalog, memory_budget
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD = REPO_ROOT / "data/analytics/dbt"
CONTRACTS = REPO_ROOT / "data_contracts/great_expectations"
RESULTS = REPO_ROOT / "data/e2e/silver_quality"
# The manifest of the build that wrote staging.duckdb last: the staging E2E's second real build.
MANIFEST = BUILD / "e2e/real_again/target/manifest.json"
TIMEOUT = 3600
# A Great Expectations type and the dbt generic test that checks the same rule on the same column [642].
DBT_EQUIVALENT = {
    "ExpectColumnValuesToNotBeNull": "not_null",
    "ExpectColumnValuesToBeInSet": "accepted_values",
    "ExpectColumnValuesToBeUnique": "unique",
}


class SilverError(RuntimeError):
    """A validation that cannot start or did not produce a result."""


def build_running() -> bool:
    """True when an analytics-dbt container of this project runs: a build may be writing the file [647]."""
    result = run_command("docker", ["ps", "-q", "--filter", "label=com.docker.compose.service=analytics-dbt"], cwd=REPO_ROOT, timeout=60)
    if result.returncode:
        raise SilverError("docker ps failed; cannot tell whether a build is running")
    return bool(result.stdout.strip())


def repeated_dbt_tests(suites: Path, manifest: Path) -> list[str]:
    """List expectations whose table, column and rule a dbt test already checks; the gates stay independent [642]."""
    if not manifest.is_file():
        raise SilverError(f"{manifest.relative_to(REPO_ROOT)} is missing: run a dbt build first")
    nodes = json.loads(manifest.read_text())["nodes"].values()
    tested = {
        (str(node.get("attached_node", "")).rsplit(".", 1)[-1], node.get("column_name"), node["test_metadata"]["name"])
        for node in nodes
        if node.get("resource_type") == "test" and node.get("test_metadata")
    }
    repeated = []
    for path in sorted(suites.glob("*.json")):
        for item in json.loads(path.read_text())["expectations"]:
            rule = DBT_EQUIVALENT.get(item["type"])
            if rule and (item["table"], item["kwargs"].get("column"), rule) in tested:
                repeated.append(item["id"])
    return repeated


def validate(database: Path, suites: Path, baselines: Path, manifest: Path = MANIFEST) -> tuple[int, Path]:
    """Run the quality container on one build file and write the run record; return the exit code and record path."""
    for path, kind in ((database, "build file"), (suites, "suites folder"), (baselines, "baselines file")):
        if not path.exists():
            raise SilverError(f"{kind} {path} does not exist")
    if not database.resolve().is_relative_to(BUILD.resolve()):
        raise SilverError("the build file must be under data/analytics/dbt")
    if build_running():
        raise SilverError("an analytics-dbt container is running; validate after the build ends")  # [647]
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS / run_id
    out.mkdir(parents=True, exist_ok=False)
    plan = memory_budget.launch_plan()
    environment = plan.environment()
    mounts = [
        "-v",
        f"{suites.resolve()}:/workspace/suites:ro",
        "-v",
        f"{baselines.resolve()}:/workspace/baselines.json:ro",
        "-v",
        f"{out}:/workspace/out",
    ]
    env_flags = [flag for name, value in environment.items() for flag in ("-e", f"{name}={value}")]
    args = ["compose", "--project-directory", str(REPO_ROOT), "-f", str(REPO_ROOT / "docker-compose.yaml"), "--env-file", str(catalog.COMPOSE_ENV)]
    args += ["--profile", "query", "run", "--rm", "--no-deps", "-T", "--quiet-pull", *env_flags, *mounts, "quality"]
    args += ["--database", f"/workspace/build/{database.resolve().relative_to(BUILD.resolve())}", "--suites", "/workspace/suites"]
    args += ["--baselines", "/workspace/baselines.json", "--output", "/workspace/out/result.json"]
    result = run_command("docker", args, cwd=REPO_ROOT, env={**catalog.system_environment(), **environment}, timeout=TIMEOUT)
    result_path = out / "result.json"
    if not result_path.is_file():
        raise SilverError(f"the quality container wrote no result (exit {result.returncode}): {result.stderr.strip().splitlines()[-1:]}")
    record: dict[str, Any] = json.loads(result_path.read_text())
    record["launch"] = plan.record()
    record["exit_code"] = result.returncode
    record["repeated_dbt_tests"] = repeated_dbt_tests(suites, manifest)
    path = out / "run.json"
    path.write_text(json.dumps(record, indent=2) + "\n")
    code = result.returncode or (1 if record["repeated_dbt_tests"] else 0)
    return code, path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("validate", help="validate a build file with Great Expectations (silver step 6)")
    check.add_argument("--database", type=Path, default=BUILD / "staging.duckdb")
    check.add_argument("--suites", type=Path, default=CONTRACTS / "suites")
    check.add_argument("--baselines", type=Path, default=CONTRACTS / "baselines.json")
    check.add_argument("--manifest", type=Path, default=MANIFEST, help="manifest.json of the dbt build that wrote the build file")
    args = parser.parse_args()
    try:
        code, path = validate(args.database, args.suites, args.baselines, args.manifest)
    except (SilverError, memory_budget.BudgetError) as error:
        sys.stderr.write(f"silver validate: stopped: {error}\n")
        return 2
    record = json.loads(path.read_text())
    counts = record.get("counts", {})
    sys.stdout.write(f"silver validate: {counts} repeated dbt tests {len(record['repeated_dbt_tests'])}; record {path.relative_to(REPO_ROOT)}\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
