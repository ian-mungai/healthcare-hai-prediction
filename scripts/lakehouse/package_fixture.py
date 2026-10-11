"""Credential-free fixture builds, baselines and checks for the parallel work packages.

Run from a checkout or a package worktree:

    .venv/bin/python -m scripts.lakehouse.package_fixture prepare                 # base fixture build in data/package_fixture/
    .venv/bin/python -m scripts.lakehouse.package_fixture baseline --build-file <duckdb> --out <folder> [--fixture-lakehouse <duckdb>]
    .venv/bin/python -m scripts.lakehouse.package_fixture check --package S1 --selector <selectors/s1.yml> --patch <file> --record
    .venv/bin/python -m scripts.lakehouse.package_fixture check --package S1 --selector <selectors/s1.yml> --patch <file> --approved <json>

Every container is the pinned analytics image started with ``docker run --network none``: no Compose file, AWS profile,
catalog or secret is used, so a fresh worktree needs only the image and the dbt packages already installed in the main
checkout. Each launch is admitted through ``memory_budget.admit`` (a caller's ``HAI_RESERVATION`` is reused). A
baseline fingerprints every relation of a build file opened read-only: row count, ordered columns with types and an
order-independent content hash (the sum of each row's lower 64 MD5 bits and the XOR of each row's full MD5, as text in
the pinned DuckDB). ``check`` builds a package's patched project, selects its frozen required tests with their ancestors
(cautious indirect selection), records the effective selection once or verifies it against the lead's approved copy, then
compares the package's models with the beab859 baseline and runs its expected-failure mutations. Failure modes 737, 888
and 889 are in plans/wave0_20261010/failure_modes.md.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import io
import json
import os
import shutil
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import memory_budget, run_lock
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "hai-analytics:duckdb1.5.6-dbt1.11.15"
OUT = ROOT / "data" / "package_fixture"
CONTAINER_OUT = "/workspace/out"
TIMEOUT = 3600
# One query per relation; the attach is added for views that read the fixture bronze database.
FINGERPRINT_SQL = (
    "SELECT '{name}', '{kind}', count(*), coalesce(sum(md5_number_lower(CAST(t AS VARCHAR))::HUGEINT), 0)::VARCHAR, "
    'coalesce(bit_xor(md5_number(CAST(t AS VARCHAR))), 0)::VARCHAR FROM main."{name}" AS t;'
)


# One model over named columns, for comparing a changed model with its baseline on the columns it keeps.
SUBSET_SQL = (
    "SELECT count(*), coalesce(sum(md5_number_lower(CAST(struct_pack({pack}) AS VARCHAR))::HUGEINT), 0)::VARCHAR, "
    'coalesce(bit_xor(md5_number(CAST(struct_pack({pack}) AS VARCHAR))), 0)::VARCHAR FROM main."{name}";'
)
# The wave 0 contracts and the beab859 fixture base build the baseline was taken from (in the main checkout).
CONTRACTS = ROOT / "plans" / "parallel_work_20261010" / "contracts"
BASE_BUILD = Path("data/analytics/dbt/e2e/base/staging.duckdb")


class FixtureError(RuntimeError):
    """A container failed, an input is missing or a check did not hold."""


def image_id() -> str:
    """The local analytics image's ID; the pinned tag must exist (it is built by the lakehouse setup)."""
    result = run_command("docker", ["image", "inspect", IMAGE, "--format", "{{.Id}}"], timeout=60)
    if result.returncode:
        raise FixtureError(f"image {IMAGE} is missing: {result.stderr.strip()}")
    return result.stdout.strip()


@contextmanager
def reservation() -> Iterator[dict[str, Any]]:
    """Admit one heavy launch, or reuse the caller's reservation, and free a reservation this process took [759] [886]."""
    registry = run_lock.resolve(None, ROOT)
    plan = memory_budget.admit(registry, "job", os.getpid(), os.environ.get("HAI_RESERVATION"))
    try:
        yield plan
    finally:
        if not plan["reused"]:
            memory_budget.release(registry, plan["reservation_id"])


def docker_run(args: list[str], mounts: list[tuple[Path, str, bool]], environment: dict[str, str], entrypoint: str) -> tuple[int, str, str]:
    """Run the analytics image with no network, no secrets and only the named mounts [888]."""
    flags = ["run", "--rm", "--network", "none", "--entrypoint", entrypoint, "--memory", environment["JOB_MEMORY_LIMIT"]]
    for source, destination, read_only in mounts:
        flags += ["-v", f"{source}:{destination}{':ro' if read_only else ''}"]
    for name, value in environment.items():
        flags += ["-e", f"{name}={value}"]
    result = run_command("docker", [*flags, IMAGE, *args], timeout=TIMEOUT)
    return result.returncode, result.stdout, result.stderr


def duckdb_query(database: Path, sql: str, plan: dict[str, Any], attach: Path | None = None) -> list[list[str]]:
    """Run SQL against a database file opened read-only in the pinned image and return CSV rows."""
    mounts = [(database.parent.resolve(), "/workspace/in", True), (ROOT / "services/analytics/resources.sql", "/opt/analytics/resources.sql", True)]
    prefix = ""
    if attach is not None:
        mounts.append((attach.parent.resolve(), "/workspace/attach", True))
        prefix = f"ATTACH '/workspace/attach/{attach.name}' AS lakehouse (READ_ONLY);\n"
    environment = {key: plan["environment"][key] for key in ("JOB_MEMORY_LIMIT", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS")}
    args = ["-readonly", f"/workspace/in/{database.name}", "-cmd", ".read /opt/analytics/resources.sql", "-csv", "-noheader", "-c", prefix + sql]
    code, stdout, stderr = docker_run(args, mounts, environment, "duckdb")
    if code:
        raise FixtureError(f"duckdb query on {database.name} failed: {stderr.strip()[-500:]}")
    return [row for row in csv.reader(io.StringIO(stdout)) if row]


def fingerprint(build_file: Path, fixture_lakehouse: Path | None) -> dict[str, Any]:
    """Fingerprint every relation of a build file; views are included only when their bronze database is attached [737]."""
    with reservation() as plan:
        return fingerprint_with(build_file, fixture_lakehouse, plan)


def fingerprint_with(build_file: Path, fixture_lakehouse: Path | None, plan: dict[str, Any]) -> dict[str, Any]:
    """Fingerprint under an admitted plan."""
    columns = duckdb_query(
        build_file,
        "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' ORDER BY table_name, ordinal_position;",
        plan,
    )
    kinds = {
        name: kind for name, kind in duckdb_query(build_file, "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main';", plan)
    }
    relations = sorted(name for name, kind in kinds.items() if kind == "BASE TABLE" or fixture_lakehouse is not None)
    sql = "\n".join(FINGERPRINT_SQL.format(name=name, kind="view" if kinds[name] == "VIEW" else "table") for name in relations)
    result: dict[str, Any] = {}
    for name, kind, rows, lower_sum, xor in duckdb_query(build_file, sql, plan, fixture_lakehouse):
        result[name] = {"kind": kind, "rows": int(rows), "content": f"{lower_sum}:{xor}", "columns": []}
    for name, column, data_type in columns:
        if name in result:
            result[name]["columns"].append([column, data_type])
    skipped = sorted(set(kinds) - set(result))
    return {"relations": result, "views_skipped": skipped, "reservation": plan["reservation_id"]}


def baseline(build_file: Path, out: Path, fixture_lakehouse: Path | None) -> dict[str, Any]:
    """Write a baseline manifest for a build file; refuses to overwrite an existing manifest [888]."""
    if not build_file.is_file():
        raise FixtureError(f"build file {build_file} does not exist")
    manifest_path = out / "baseline.json"
    if manifest_path.exists():
        raise FixtureError(f"{manifest_path} exists; a baseline is written once")
    out.mkdir(parents=True, exist_ok=True)
    prints = fingerprint(build_file, fixture_lakehouse)
    manifest = {
        "written_utc": datetime.now(UTC).isoformat(),
        "build_file": str(build_file),
        "build_file_bytes": build_file.stat().st_size,
        "fixture_lakehouse": str(fixture_lakehouse) if fixture_lakehouse else None,
        "image": IMAGE,
        "image_id": image_id(),
        "commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        **prints,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"manifest": str(manifest_path), "relations": len(prints["relations"]), "views_skipped": len(prints["views_skipped"])}


def packages_from_main() -> Path:
    """The dbt packages installed in the main checkout, copied for offline builds [888]."""
    source = run_lock.main_checkout(ROOT) / "data" / "analytics" / "dbt" / "dbt_packages"
    if not (source / "dbt_utils").is_dir():
        raise FixtureError(f"dbt packages are not installed in {source}; run scripts/lakehouse/dbt.sh deps in the main checkout")
    return source


@dataclass(frozen=True)
class Case:
    """One fixture case folder under data/package_fixture/ with its container mounts and dbt environment."""

    name: str
    folder: Path
    mounts: list[tuple[Path, str, bool]]
    environment: dict[str, str]
    dbt_environment: dict[str, str]

    @property
    def project_flags(self) -> list[str]:
        """dbt's project and profile folders inside the container."""
        return ["--project-dir", f"{CONTAINER_OUT}/{self.name}/project", "--profiles-dir", f"{CONTAINER_OUT}/{self.name}/project"]


def make_case(name: str, plan: dict[str, Any], patches: list[Path]) -> Case:
    """Write a case: this checkout's dbt/ with the given patches applied, its seeds and the synthetic bronze database [888]."""
    from scripts.lakehouse import run_staging_e2e as staging

    # A new folder per run: Docker Desktop's file sharing can show a folder deleted and recreated at the same path as
    # missing inside the container, so earlier runs of the case are removed and this one gets its own name.
    for earlier in OUT.glob(f"{name}--*"):
        shutil.rmtree(earlier)
    name = f"{name}--{uuid.uuid4().hex[:8]}"
    folder = OUT / name
    folder.mkdir(parents=True)
    packages = OUT / "dbt_packages"
    if not (packages / "dbt_utils").is_dir():
        shutil.copytree(packages_from_main(), packages, dirs_exist_ok=True)
    # The project comes from this checkout's dbt/, never from the staging runner's last snapshot.
    staging.DBT_SNAPSHOT = ROOT / "dbt"
    staging.fixture_project(folder, staging.BASE, frozenset())
    for patch in patches:
        applied = run_command("git", ["apply", "-p2", str(patch.resolve())], cwd=folder / "project", timeout=60)
        if applied.returncode:
            raise FixtureError(f"patch {patch.name} does not apply to dbt/: {applied.stderr.strip()[-300:]}")
    objects = staging.BASE
    (folder / "bronze.csv").write_text(staging.fixture_csv(objects))
    (folder / "copies.csv").write_text(staging.copies_csv(objects))
    variables = f"SET VARIABLE fixture_csv = '{CONTAINER_OUT}/{name}/bronze.csv';\nSET VARIABLE copies_csv = '{CONTAINER_OUT}/{name}/copies.csv';\n"
    for table in staging.WIDE_COLUMNS:
        (folder / f"{table}.csv").write_text(staging.wide_csv(objects, table))
        variables += f"SET VARIABLE {table}_csv = '{CONTAINER_OUT}/{name}/{table}.csv';\n"
    (folder / "column_map.csv").write_text(staging.column_map_csv(objects))
    (folder / "file_preambles.csv").write_text(staging.file_preambles_csv(objects))
    variables += f"SET VARIABLE file_preambles_csv = '{CONTAINER_OUT}/{name}/file_preambles.csv';\n"
    variables += f"SET VARIABLE column_map_csv = '{CONTAINER_OUT}/{name}/column_map.csv';\n"
    (folder / "bronze.sql").write_text(variables + staging.FIXTURE_SQL)
    environment = {key: plan["environment"][key] for key in ("JOB_MEMORY_LIMIT", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS")}
    mounts = [(OUT.resolve(), CONTAINER_OUT, False), (ROOT / "services/analytics/resources.sql", "/opt/analytics/resources.sql", True)]
    database = f"{CONTAINER_OUT}/{name}/fixture_lakehouse.duckdb"
    code, _, stderr = docker_run(
        [database, "-cmd", ".read /opt/analytics/resources.sql", "-c", f".read {CONTAINER_OUT}/{name}/bronze.sql"], mounts, environment, "duckdb"
    )
    if code:
        raise FixtureError(f"fixture bronze database failed: {stderr.strip()[-500:]}")
    dbt_environment = {
        **environment,
        "STAGING_E2E_CASE": f"{CONTAINER_OUT}/{name}",
        "DBT_TARGET_PATH": f"{CONTAINER_OUT}/{name}/target",
        "DBT_LOG_PATH": f"{CONTAINER_OUT}/{name}/logs",
        "DBT_PACKAGES_INSTALL_PATH": f"{CONTAINER_OUT}/dbt_packages",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    return Case(name, folder, mounts, environment, dbt_environment)


def dbt(case: Case, args: list[str]) -> tuple[int, str, dict[str, str]]:
    """Run dbt in the case and return the exit code, the output tail and each node's status from this invocation."""
    results = case.folder / "target" / "run_results.json"
    results.unlink(missing_ok=True)
    code, stdout, stderr = docker_run([*args, "--target", "fixture", *case.project_flags], case.mounts, case.dbt_environment, "dbt")
    statuses = {item["unique_id"]: item["status"] for item in json.loads(results.read_text())["results"]} if results.exists() else {}
    return code, (stdout + stderr).strip()[-800:], statuses


def prepare() -> dict[str, Any]:
    """Build the synthetic base fixture case in this checkout's data/package_fixture/ without credentials or network."""
    from scripts.lakehouse import run_staging_e2e as staging

    with reservation() as plan:
        case = make_case("base", plan, [])
        code, tail, statuses = dbt(case, ["build"])
        failed = sorted(node for node, status in statuses.items() if status not in ("success", "pass"))
        record = {
            "written_utc": datetime.now(UTC).isoformat(),
            "dbt_tree_sha256": staging.dbt_tree_sha256(),
            "image_id": image_id(),
            "commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
            "exit_code": code,
            "nodes": len(statuses),
            "failed_nodes": failed,
            "reservation": plan["reservation_id"],
        }
    (OUT / "prepare.json").write_text(json.dumps(record, indent=2) + "\n")
    if code or failed or not statuses:
        raise FixtureError(f"base fixture build failed (exit {code}, {len(failed)} failed nodes): {tail}")
    return record


def manifest_sha256(case: Case) -> str:
    """A hash of the parsed project that ignores run timestamps: every node's ID and file checksum [889]."""
    manifest = json.loads((case.folder / "target" / "manifest.json").read_text())
    nodes = sorted([unique_id, node.get("checksum", {}).get("checksum", "")] for unique_id, node in {**manifest["nodes"], **manifest["sources"]}.items())
    return hashlib.sha256(json.dumps(nodes).encode()).hexdigest()


def subset_fingerprint(database: Path, model: str, columns: list[str], plan: dict[str, Any]) -> list[str]:
    """Rows and content of one model over the named columns, read-only [737]."""
    pack = ", ".join(f'"{column}" := "{column}"' for column in columns)
    sql = SUBSET_SQL.format(pack=pack, name=model)
    return duckdb_query(database, sql, plan)[0]


def compare(case: Case, selector: dict[str, Any], plan: dict[str, Any]) -> dict[str, str]:
    """Each owned model against the beab859 baseline: unchanged models in full, changed ones on their unchanged columns [737]."""
    baseline = json.loads((run_lock.main_checkout(ROOT) / selector["baseline"]).read_text())["relations"]
    contract = json.loads(CONTRACTS.joinpath("silver_columns.json").read_text())["models"]
    base_build = run_lock.main_checkout(ROOT) / BASE_BUILD
    built = case.folder / "staging.duckdb"
    outcome = {}
    for model in selector["compare_unchanged_with_baseline"]:
        columns = [column for column, _ in baseline[model]["columns"]]
        ours, theirs = subset_fingerprint(built, model, columns, plan), subset_fingerprint(base_build, model, columns, plan)
        outcome[model] = "equal" if ours == theirs else f"differs: rows {ours[0]} against {theirs[0]}"
    for model in selector["documented_changes"]:
        changes = contract[model]["changes"]
        if "grain" in changes:
            outcome[model] = "not compared: documented grain change"
            continue
        skip = set(changes.get("drop", [])) | {column for column, _, _ in changes.get("retype", [])}
        columns = [column for column, _ in baseline[model]["columns"] if column not in skip]
        ours, theirs = subset_fingerprint(built, model, columns, plan), subset_fingerprint(base_build, model, columns, plan)
        outcome[model] = "equal on unchanged columns" if ours == theirs else f"differs on unchanged columns: rows {ours[0]} against {theirs[0]}"
    return outcome


def check(package: str, selector_path: Path, patches: list[Path], record: bool, approved: Path | None, expected_failures: list[str]) -> dict[str, Any]:
    """Select, build and compare one package's patched project against its frozen selector contract [737] [889]."""
    # PyYAML ships no type hints, so it is imported by name, as in scripts/quality/repo_checks.py.
    selector = importlib.import_module("yaml").safe_load(selector_path.read_text())
    if selector["package"] != package:
        raise FixtureError(f"{selector_path.name} is the selector of {selector['package']}, not {package}")
    selector_sha256 = hashlib.sha256(selector_path.read_bytes()).hexdigest()
    expression = [f"+{test}" for test in selector["required_tests"]]
    # Records live outside the case folder, which every check rebuilds, so a recorded selection survives [889].
    folder = OUT / "records" / package.lower()
    folder.mkdir(parents=True, exist_ok=True)
    with reservation() as plan:
        case = make_case(f"check_{package.lower()}", plan, patches)
        nodes = selection(case, expression)
        missing = sorted(set(selector["required_tests"]) - {node_name(node) for node in nodes})
        effective = {"package": package, "selector_sha256": selector_sha256, "manifest_sha256": manifest_sha256(case), "nodes": nodes}
        artifact = folder / "effective_selection.json"
        if record:
            if artifact.exists():
                raise FixtureError(f"{artifact} exists; the effective selection is recorded once")
            artifact.write_text(json.dumps(effective, indent=2) + "\n")
        problems = [f"required test missing: {test}" for test in missing]
        if not record:
            if approved is None:
                raise FixtureError("a check without --record needs --approved <effective_selection.json the lead approved>")
            accepted = json.loads(approved.read_text())
            problems += [
                f"{key} differs from the approved selection" for key in ("selector_sha256", "manifest_sha256", "nodes") if accepted[key] != effective[key]
            ]
        code, tail, statuses = dbt(case, ["build", "--select", *expression, "--indirect-selection", "cautious"])
        not_passed = sorted(node for node, status in statuses.items() if status not in ("success", "pass"))
        problems += [f"node {node} {statuses[node]}" for node in not_passed]
        problems += [
            f"required test not run: {test}"
            for test in selector["required_tests"]
            if test not in missing and test not in {node_name(node) for node in statuses}
        ]
        comparisons = compare(case, selector, plan) if not not_passed else {}
        problems += [f"{model} {verdict}" for model, verdict in comparisons.items() if verdict.startswith("differs")]
        mutations = {}
        for item in expected_failures:
            name, patch, test = item.split(":", 2)
            mutant = make_case(f"check_{package.lower()}_{name}", plan, [*patches, Path(patch)])
            _, _, mutant_statuses = dbt(mutant, ["build", "--select", f"+{test}", "--indirect-selection", "cautious"])
            if not mutant_statuses:
                problems.append(f"expected failure {name}: the mutated build wrote no results")
            failed = sorted(node_name(node) for node, status in mutant_statuses.items() if status in ("fail", "error"))
            mutations[name] = {"test": test, "failed": failed, "ok": failed == [test]}
            if failed != [test]:
                problems.append(f"expected failure {name}: wanted only {test} to fail, got {failed}")
    report = {
        "package": package,
        "written_utc": datetime.now(UTC).isoformat(),
        "selector": str(selector_path),
        "patches": [str(patch) for patch in patches],
        "recorded": record,
        "effective_nodes": len(nodes),
        "comparisons": comparisons,
        "expected_failures": mutations,
        "problems": problems,
        "status": "pass" if not problems else "fail",
    }
    (folder / "check.json").write_text(json.dumps(report, indent=2) + "\n")
    if problems:
        raise FixtureError(f"check of {package} failed: {problems[:5]}")
    return report


def node_name(unique_id: str) -> str:
    """A node's name from its unique ID; a generic test's ID ends with a hash after the name."""
    return unique_id.split(".")[2]


def selection(case: Case, expression: list[str]) -> list[str]:
    """The node IDs dbt selects for the expression with cautious indirect selection, sorted."""
    args = ["ls", "--select", *expression, "--indirect-selection", "cautious", "--output", "json", "--output-keys", "unique_id", "--quiet"]
    code, stdout, stderr = docker_run([*args, "--target", "fixture", *case.project_flags], case.mounts, case.dbt_environment, "dbt")
    if code:
        raise FixtureError(f"dbt ls failed: {(stdout + stderr).strip()[-500:]}")
    return sorted(json.loads(line)["unique_id"] for line in stdout.splitlines() if line.startswith("{"))


def main() -> int:
    """Run one command; a failure prints its cause and exits 1."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("prepare", help="build the base fixture case in data/package_fixture/")
    command = commands.add_parser("baseline", help="fingerprint every relation of a build file, read-only")
    command.add_argument("--build-file", type=Path, required=True)
    command.add_argument("--out", type=Path, required=True)
    command.add_argument("--fixture-lakehouse", type=Path, help="the fixture bronze database, so the staging views can be read")
    command = commands.add_parser("check", help="select, build and compare a package's patched project")
    command.add_argument("--package", required=True)
    command.add_argument("--selector", type=Path, required=True, help="the frozen selector contract")
    command.add_argument("--patch", type=Path, action="append", default=[], help="a patch against dbt/ (paths a/dbt/...), applied in order")
    command.add_argument("--record", action="store_true", help="record the effective selection once")
    command.add_argument("--approved", type=Path, help="the effective selection the lead approved")
    command.add_argument("--expected-failure", action="append", default=[], metavar="NAME:PATCH:TEST", help="a mutation that must fail exactly TEST")
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            result = prepare()
        elif args.command == "baseline":
            result = baseline(args.build_file, args.out, args.fixture_lakehouse)
        else:
            result = check(args.package, args.selector, args.patch, args.record, args.approved, args.expected_failure)
    except (FixtureError, memory_budget.BudgetError, run_lock.Refused, run_lock.RegistryError) as error:
        sys.stderr.write(f"package fixture: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
