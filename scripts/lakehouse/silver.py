"""Silver pipeline entry point: validate the build file with Great Expectations (step 6); build and publish come in step 7.

    .venv/bin/python -m scripts.lakehouse.silver validate [--database staging.duckdb] [--baselines PATH] [--suites PATH [PATH ...]]

The validation runs as an attempt of the launcher's ``quality_validate`` template (the ``quality`` Compose service) with the memory limit of
its allocation, opens the build file read-only and writes ``data/e2e/silver_quality/<run_id>/result.json`` with counts only. The record adds the launch
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

from orchestration import launcher

REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD = REPO_ROOT / "data/analytics/dbt"
CONTRACTS = REPO_ROOT / "data_contracts/great_expectations"
# The silver atomic suites and the gold suites run in one validation of one build file (failure mode 855).
DEFAULT_SUITES = [CONTRACTS / "suites", CONTRACTS / "gold"]
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


TEST_REGISTRY: str | None = None


def build_running(link: launcher.LauncherClient) -> bool:
    """True when the launcher has a ``real_dbt`` attempt open: a build may be writing the file [647]."""
    try:
        return "real_dbt" in link.call("status", {"what": "launcher"}).get("open_templates", [])
    except launcher.ClientError as error:
        raise SilverError(f"the launcher cannot tell whether a build is running: {error}") from error


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


def validate(database: Path, suites: Path | list[Path], baselines: Path, manifest: Path = MANIFEST) -> tuple[int, Path]:
    """Run the quality attempt on one build file and write the run record; return the exit code and record path.

    ``suites`` is one folder (the gold runner passes its gold folder) or a list of folders (the silver atomic suites and the gold suites).
    """
    folders = [suites] if isinstance(suites, Path) else list(suites)
    for path, kind in [(database, "build file"), *((folder, "suites folder") for folder in folders), (baselines, "baselines file")]:
        if not path.exists():
            raise SilverError(f"{kind} {path} does not exist")
    if not database.resolve().is_relative_to(BUILD.resolve()):
        raise SilverError("the build file must be under data/analytics/dbt")
    link = launcher.load_client(TEST_REGISTRY)
    if build_running(link):
        raise SilverError("a real_dbt attempt is open; validate after the build ends")  # [647]
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS / run_id
    out.mkdir(parents=True, exist_ok=False)
    try:
        allocation = link.allocate()
    except launcher.ClientError as error:
        raise SilverError(f"no allocation for the validation: {error}") from error
    principal = link.principal_alloc(allocation)
    try:
        params: dict[str, Any] = {"database": str(database.resolve()), "baselines_host": str(baselines.resolve()), "out_host": str(out)}
        if isinstance(suites, Path):
            params["suites_host"] = str(suites.resolve())
        else:
            params["suites_hosts"] = [str(folder.resolve()) for folder in folders]
        result = link.run_attempt(principal, "quality_validate", f"silver-{run_id}", [], params, checkout=REPO_ROOT)
        launch = link.call("status", {"what": "allocation", "id": allocation}, principal)
    except launcher.ClientError as error:
        raise SilverError(f"the launcher refused the validation: {error}") from error
    finally:
        link.release_allocation(allocation)
    result_path = out / "result.json"
    if not result_path.is_file():
        raise SilverError(f"the quality container wrote no result ({result.reason}, exit {result.exit_code}): {result.stderr.strip().splitlines()[-1:]}")
    record: dict[str, Any] = json.loads(result_path.read_text())
    record["launch"] = launch["record"]
    record["exit_code"] = result.exit_code
    record["repeated_dbt_tests"] = [name for folder in folders for name in repeated_dbt_tests(folder, manifest)]
    path = out / "run.json"
    path.write_text(json.dumps(record, indent=2) + "\n")
    code = (result.exit_code or 0) or (1 if record["repeated_dbt_tests"] else 0)
    return code, path


def main() -> int:
    global TEST_REGISTRY
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--test-registry", help="a test registry folder (for tests of this script only)")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("validate", help="validate a build file with Great Expectations (silver step 6)")
    check.add_argument("--database", type=Path, default=BUILD / "staging.duckdb")
    check.add_argument("--suites", type=Path, nargs="+", default=DEFAULT_SUITES, help="suite folders: the silver atomic suites and the gold suites")
    check.add_argument("--baselines", type=Path, default=CONTRACTS / "baselines.json")
    check.add_argument("--manifest", type=Path, default=MANIFEST, help="manifest.json of the dbt build that wrote the build file")
    args = parser.parse_args()
    TEST_REGISTRY = args.test_registry
    try:
        code, path = validate(args.database, args.suites, args.baselines, args.manifest)
    except SilverError as error:
        sys.stderr.write(f"silver validate: stopped: {error}\n")
        return 2
    record = json.loads(path.read_text())
    counts = record.get("counts", {})
    sys.stdout.write(f"silver validate: {counts} repeated dbt tests {len(record['repeated_dbt_tests'])}; record {path.relative_to(REPO_ROOT)}\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
