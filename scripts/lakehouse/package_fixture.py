"""Credential-free fixture builds, baselines and checks for the parallel work packages.

Run from a checkout or a package worktree:

    .venv/bin/python -m scripts.lakehouse.package_fixture prepare                 # base fixture build in data/package_fixture/
    .venv/bin/python -m scripts.lakehouse.package_fixture baseline --build-file <duckdb> --out <folder> [--fixture-lakehouse <duckdb>]
    .venv/bin/python -m scripts.lakehouse.package_fixture check --package S1 --selector <selectors/s1.yml> --patch <file> --record
    .venv/bin/python -m scripts.lakehouse.package_fixture check --package S1 --selector <selectors/s1.yml> --patch <file> --approved <json>

Every container is an attempt of a launcher template (``fixture_dbt``, ``fixture_init`` or ``duckdb_helper``) on the pinned analytics image with
no network: no AWS profile, catalog or secret is used, so a fresh worktree needs only the image and the dbt packages already installed in
the main checkout. The kit is a launcher client and makes no Docker call; its launches run under one allocation, which admits one heavy job
at a time across worktrees (a caller's ``HAI_ALLOCATION`` is reused). Every fixture enforces the physical release-copy invariant. A
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

from orchestration import launcher
from scripts.lakehouse import run_lock
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "hai-analytics:duckdb1.5.6-dbt1.11.15"
OUT = ROOT / "data" / "package_fixture"
CONTAINER_OUT = "/workspace/out"
TIMEOUT = 3600
# The test registry a test of the kit passes; the real run never sets it.
TEST_REGISTRY: str | None = None
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
# The same over the rows whose grain key the baseline build (attached as base) also has.
KEYED_SUBSET_SQL = SUBSET_SQL.removesuffix(";").replace('FROM main."{name}"', 'FROM main."{name}" WHERE "{key}" IN (SELECT "{key}" FROM base.main."{name}");')
# The grain key of every row the baseline does not have, one line per row so duplicates show; a null key is never a
# baseline key, so it is listed as an added row instead of escaping both comparisons.
ADDED_KEYS_SQL = (
    'SELECT CASE WHEN m."{key}" IS NULL THEN \'(null key)\' ELSE CAST(m."{key}" AS VARCHAR) END AS added FROM main."{name}" AS m '
    'WHERE m."{key}" IS NULL OR NOT EXISTS (SELECT 1 FROM base.main."{name}" AS b WHERE b."{key}" = m."{key}");'
)
# The wave 0 contracts and the beab859 fixture base build the baseline was taken from (in the main checkout).
CONTRACTS = ROOT / "data_contracts" / "lakehouse"
BASE_BUILD = Path("data/analytics/dbt/e2e/base/staging.duckdb")


class FixtureError(RuntimeError):
    """A container failed, an input is missing or a check did not hold."""


@dataclass(frozen=True)
class Session:
    """The launcher client and the allocation every launch of one kit command runs under."""

    client: launcher.LauncherClient
    allocation: str

    @property
    def principal(self) -> dict[str, Any]:
        """The allocation principal: this process or one of its descendants."""
        return self.client.principal_alloc(self.allocation)

    def run(self, template: str, launch: str, args: list[str], params: dict[str, Any]) -> launcher.AttemptResult:
        """One attempt of a template, to its end."""
        try:
            return self.client.run_attempt(self.principal, template, launch, args, params, checkout=ROOT, seconds=TIMEOUT)
        except launcher.ClientError as error:
            raise FixtureError(error.describe(template)) from error


def image_id() -> str:
    """The local analytics image's ID, read by the launcher; the pinned tag must exist (it is built by the lakehouse setup)."""
    try:
        return str(launcher.load_client(TEST_REGISTRY).call("image", {"service": "analytics-dbt", "checkout": str(ROOT)})["id"])
    except launcher.ClientError as error:
        raise FixtureError(f"the analytics image cannot be read: {error}") from error


# The sessions open in this process, innermost last: callers that hold an allocation through ``reservation`` need not pass it to ``dbt``.
ACTIVE: list[Session] = []


@contextmanager
def reservation() -> Iterator[Session]:
    """Take one allocation for a kit command, or reuse the caller's (``HAI_ALLOCATION``), and release the one this process took [759] [886]."""
    client = launcher.load_client(TEST_REGISTRY)
    inherited = os.environ.get("HAI_ALLOCATION")
    if inherited:
        session = Session(client, inherited)
        ACTIVE.append(session)
        try:
            yield session
        finally:
            ACTIVE.remove(session)
        return
    try:
        allocation = client.allocate()
    except launcher.ClientError as error:
        raise FixtureError(f"no allocation: {error}") from error
    session = Session(client, allocation)
    ACTIVE.append(session)
    try:
        yield session
    finally:
        ACTIVE.remove(session)
        client.release_allocation(allocation)


def duckdb_query(database: Path, sql: str, session: Session, attach: Path | None = None, alias: str = "lakehouse") -> list[list[str]]:
    """Run SQL against a database file opened read-only in the pinned image, with one more file attached read-only."""
    mounts = [{"host": str(database.parent.resolve()), "container": "/workspace/in"}]
    prefix = ""
    if attach is not None:
        mounts.append({"host": str(attach.parent.resolve()), "container": "/workspace/attach"})
        prefix = f"ATTACH '/workspace/attach/{attach.name}' AS {alias} (READ_ONLY);\n"
    params = {"mounts": mounts, "database": f"/workspace/in/{database.name}", "sql": prefix + sql, "flags": ["-csv", "-noheader"]}
    result = session.run("duckdb_helper", f"query-{uuid.uuid4().hex[:10]}", [], params)
    if not result.ok:
        raise FixtureError(f"duckdb query on {database.name} failed: {result.stderr.strip()[-500:]}")
    return [row for row in csv.reader(io.StringIO(result.stdout)) if row]


def fingerprint(build_file: Path, fixture_lakehouse: Path | None) -> dict[str, Any]:
    """Fingerprint every relation of a build file; views are included only when their bronze database is attached [737]."""
    with reservation() as session:
        return fingerprint_with(build_file, fixture_lakehouse, session)


def fingerprint_with(build_file: Path, fixture_lakehouse: Path | None, session: Session) -> dict[str, Any]:
    """Fingerprint under an allocation."""
    columns = duckdb_query(
        build_file,
        "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' ORDER BY table_name, ordinal_position;",
        session,
    )
    kinds = {
        name: kind
        for name, kind in duckdb_query(build_file, "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main';", session)
    }
    relations = sorted(name for name, kind in kinds.items() if kind == "BASE TABLE" or fixture_lakehouse is not None)
    sql = "\n".join(FINGERPRINT_SQL.format(name=name, kind="view" if kinds[name] == "VIEW" else "table") for name in relations)
    result: dict[str, Any] = {}
    for name, kind, rows, lower_sum, xor in duckdb_query(build_file, sql, session, fixture_lakehouse):
        result[name] = {"kind": kind, "rows": int(rows), "content": f"{lower_sum}:{xor}", "columns": []}
    for name, column, data_type in columns:
        if name in result:
            result[name]["columns"].append([column, data_type])
    skipped = sorted(set(kinds) - set(result))
    return {"relations": result, "views_skipped": skipped, "reservation": session.allocation}


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
    """One fixture case folder under data/package_fixture/ and the container paths the launcher mounts it at."""

    name: str
    folder: Path

    @property
    def params(self) -> dict[str, Any]:
        """Return fixture paths and an empty variable map for the launcher template."""
        return {
            "case_host": str(self.folder.resolve()),
            "case_container": f"{CONTAINER_OUT}/{self.name}",
            "packages_host": str((OUT / "dbt_packages").resolve()),
            "dbt_vars": {},
        }


def make_case(name: str, session: Session, patches: list[Path]) -> Case:
    """Write a fixture case with the given patches, generated seeds and synthetic bronze database."""
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
        # The case folder sits inside the repository's ignored data/ folder; without the ceiling, git apply resolves the
        # patch's paths from the repository root and skips every file outside the case project without an error.
        environment = {**os.environ, "GIT_CEILING_DIRECTORIES": str(folder.resolve())}
        applied = run_command("git", ["apply", "-p2", "--verbose", str(patch.resolve())], cwd=folder / "project", env=environment, timeout=60)
        if applied.returncode or "Applied patch" not in applied.stderr:
            raise FixtureError(f"patch {patch.name} does not apply to dbt/: {(applied.stderr or 'no file was patched').strip()[-300:]}")
    objects = staging.BASE
    (folder / "bronze.csv").write_text(staging.fixture_csv(objects))
    (folder / "copies.csv").write_text(staging.copies_csv(objects))
    (folder / "metadata.json").write_text(json.dumps(staging.fixture_metadata(objects)))
    variables = f"SET VARIABLE fixture_csv = '{CONTAINER_OUT}/{name}/bronze.csv';\nSET VARIABLE copies_csv = '{CONTAINER_OUT}/{name}/copies.csv';\n"
    variables += f"SET VARIABLE metadata_json = '{CONTAINER_OUT}/{name}/metadata.json';\n"
    for table in staging.WIDE_COLUMNS:
        (folder / f"{table}.csv").write_text(staging.wide_csv(objects, table))
        variables += f"SET VARIABLE {table}_csv = '{CONTAINER_OUT}/{name}/{table}.csv';\n"
    (folder / "column_map.csv").write_text(staging.column_map_csv(objects))
    (folder / "file_preambles.csv").write_text(staging.file_preambles_csv(objects))
    variables += f"SET VARIABLE file_preambles_csv = '{CONTAINER_OUT}/{name}/file_preambles.csv';\n"
    variables += f"SET VARIABLE column_map_csv = '{CONTAINER_OUT}/{name}/column_map.csv';\n"
    (folder / "bronze.sql").write_text(variables + staging.FIXTURE_SQL)
    params = {
        "case_host": str(folder.resolve()),
        "case_container": f"{CONTAINER_OUT}/{name}",
        "database": f"{CONTAINER_OUT}/{name}/fixture_lakehouse.duckdb",
        "sql_file": f"{CONTAINER_OUT}/{name}/bronze.sql",
    }
    result = session.run("fixture_init", f"init-{name}", [], params)
    if not result.ok:
        raise FixtureError(f"fixture bronze database failed: {result.stderr.strip()[-500:]}")
    return Case(name, folder)


def dbt(case: Case, args: list[str], session: Session | None = None) -> tuple[int, str, dict[str, str]]:
    """Run dbt in the case and return the exit code, the output tail and each node's status from this invocation; without a session, the innermost
    allocation of ``reservation`` is used."""
    session = session or ACTIVE[-1]
    results = case.folder / "target" / "run_results.json"
    results.unlink(missing_ok=True)
    result = session.run("fixture_dbt", f"dbt-{uuid.uuid4().hex[:10]}", args, case.params)
    statuses = {item["unique_id"]: item["status"] for item in json.loads(results.read_text())["results"]} if results.exists() else {}
    return (result.exit_code if result.exit_code is not None else 1), (result.stdout + result.stderr).strip()[-800:], statuses


def prepare() -> dict[str, Any]:
    """Build the synthetic base fixture case in this checkout's data/package_fixture/ without credentials or network."""
    from scripts.lakehouse import run_staging_e2e as staging

    with reservation() as session:
        case = make_case("base", session, [])
        code, tail, statuses = dbt(case, ["build"], session)
        failed = sorted(node for node, status in statuses.items() if status not in ("success", "pass"))
        record = {
            "written_utc": datetime.now(UTC).isoformat(),
            "dbt_tree_sha256": staging.dbt_tree_sha256(),
            "image_id": image_id(),
            "commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
            "exit_code": code,
            "nodes": len(statuses),
            "failed_nodes": failed,
            "reservation": session.allocation,
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


def subset_fingerprint(database: Path, model: str, columns: list[str], session: Session, baseline_keys: tuple[Path, str] | None = None) -> list[str]:
    """Rows and content of one model over the named columns, read-only; with ``baseline_keys`` (the baseline build and
    the grain key), only the rows whose key the baseline has, so documented added fixture rows are left out [737]."""
    pack = ", ".join(f'"{column}" := "{column}"' for column in columns)
    if baseline_keys is None:
        return duckdb_query(database, SUBSET_SQL.format(pack=pack, name=model), session)[0]
    base_build, key = baseline_keys
    return duckdb_query(database, KEYED_SUBSET_SQL.format(pack=pack, name=model, key=key), session, attach=base_build, alias="base")[0]


def compare(case: Case, selector: dict[str, Any], session: Session) -> dict[str, str]:
    """Each owned model against the beab859 baseline: unchanged models in full, changed ones on their unchanged columns [737]."""
    baseline = json.loads((run_lock.main_checkout(ROOT) / selector["baseline"]).read_text())["relations"]
    contract = json.loads(CONTRACTS.joinpath("silver_columns.json").read_text())["models"]
    base_build = run_lock.main_checkout(ROOT) / BASE_BUILD
    built = case.folder / "staging.duckdb"
    added = added_rows(selector)
    outcome = {}

    def ours(model: str, columns: list[str]) -> list[str]:
        """The case's fingerprint, on the baseline's keys when the selector documents added fixture rows."""
        return subset_fingerprint(built, model, columns, session, (base_build, added[model][0]) if model in added else None)

    def extra(model: str) -> str | None:
        """None when the rows beyond the baseline's keys are exactly the documented ones, else what differs [737]."""
        if model not in added:
            return None
        key, documented = added[model]
        found = sorted(row[0] for row in duckdb_query(built, ADDED_KEYS_SQL.format(name=model, key=key), session, attach=base_build, alias="base"))
        if found == documented:
            return None
        return f"added rows {found} against documented {documented}"

    def verdict(model: str, same: str, mine: list[str], theirs: list[str]) -> str:
        """The comparison's verdict; documented added rows must all be present and nothing else added."""
        if mine != theirs:
            return f"{same.replace('equal', 'differs')}: rows {mine[0]} against {theirs[0]}"
        problem = extra(model)
        if problem:
            return f"{same.replace('equal', 'differs')}: {problem}"
        return same + (f" ({len(added[model][1])} documented fixture rows added)" if model in added else "")

    for model in selector["compare_unchanged_with_baseline"]:
        columns = [column for column, _ in baseline[model]["columns"]]
        outcome[model] = verdict(model, "equal", ours(model, columns), subset_fingerprint(base_build, model, columns, session))
    for model in selector["documented_changes"]:
        changes = contract[model]["changes"]
        if "grain" in changes:
            outcome[model] = "not compared: documented grain change"
            continue
        skip = set(changes.get("drop", [])) | {column for column, _, _ in changes.get("retype", [])}
        columns = [column for column, _ in baseline[model]["columns"] if column not in skip]
        outcome[model] = verdict(model, "equal on unchanged columns", ours(model, columns), subset_fingerprint(base_build, model, columns, session))
    return outcome


def added_rows(selector: dict[str, Any]) -> dict[str, tuple[str, list[str]]]:
    """Each model's grain key and sorted documented added keys; any other form is refused before a build [737]."""
    added = {}
    for model, entry in (selector.get("fixture_rows_added") or {}).items():
        rows = entry.get("rows") if isinstance(entry, dict) else None
        if not isinstance(entry.get("key") if isinstance(entry, dict) else None, str) or not isinstance(rows, list) or not rows:
            raise FixtureError(f"fixture_rows_added.{model} must be {{key: <grain key column>, rows: [<each added key>]}}")
        if not all(isinstance(row, str) and row for row in rows):
            raise FixtureError(f"fixture_rows_added.{model}.rows must list each added key as a non-empty string")
        added[model] = (entry["key"], sorted(rows))
    return added


def check(package: str, selector_path: Path, patches: list[Path], record: bool, approved: Path | None, expected_failures: list[str]) -> dict[str, Any]:
    """Select, build and compare one package's patched project against its frozen selector contract [737] [889]."""
    # PyYAML ships no type hints, so it is imported by name, as in scripts/quality/repo_checks.py.
    selector = importlib.import_module("yaml").safe_load(selector_path.read_text())
    if selector["package"] != package:
        raise FixtureError(f"{selector_path.name} is the selector of {selector['package']}, not {package}")
    selector_sha256 = hashlib.sha256(selector_path.read_bytes()).hexdigest()
    added_rows(selector)
    expression = [f"+{test}" for test in selector["required_tests"]]
    # Tests the disposition list gives to another owner would join through cautious selection when their parents are
    # selected; they are excluded, never deleted, so the project under test stays the package's own.
    exclusion = ["--exclude", *selector["exclude_tests"]] if selector.get("exclude_tests") else []
    # Records live outside the case folder, which every check rebuilds, so a recorded selection survives [889].
    folder = OUT / "records" / package.lower()
    folder.mkdir(parents=True, exist_ok=True)
    with reservation() as session:
        case = make_case(f"check_{package.lower()}", session, patches)
        nodes = selection(case, [*expression, *exclusion], session)
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
        code, tail, statuses = dbt(case, ["build", "--select", *expression, *exclusion, "--indirect-selection", "cautious"], session)
        not_passed = sorted(node for node, status in statuses.items() if status not in ("success", "pass"))
        problems += [f"node {node} {statuses[node]}" for node in not_passed]
        problems += [
            f"required test not run: {test}"
            for test in selector["required_tests"]
            if test not in missing and test not in {node_name(node) for node in statuses}
        ]
        comparisons = compare(case, selector, session) if not not_passed else {}
        problems += [f"{model} {verdict}" for model, verdict in comparisons.items() if verdict.startswith("differs")]
        mutations = {}
        for item in expected_failures:
            name, patch, test = item.split(":", 2)
            mutant = make_case(f"check_{package.lower()}_{name}", session, [*patches, Path(patch)])
            _, _, mutant_statuses = dbt(mutant, ["build", "--select", f"+{test}", *exclusion, "--indirect-selection", "cautious"], session)
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


def selection(case: Case, arguments: list[str], session: Session) -> list[str]:
    """The node IDs dbt selects for the selection arguments (with any --exclude) and cautious indirect selection, sorted."""
    args = ["ls", "--select", *arguments, "--indirect-selection", "cautious", "--output", "json", "--output-keys", "unique_id", "--quiet"]
    result = session.run("fixture_dbt", f"ls-{uuid.uuid4().hex[:10]}", args, case.params)
    if not result.ok:
        raise FixtureError(f"dbt ls failed: {(result.stdout + result.stderr).strip()[-500:]}")
    return sorted(json.loads(line)["unique_id"] for line in result.stdout.splitlines() if line.startswith("{"))


def main() -> int:
    """Run one command; a failure prints its cause and exits 1."""
    global TEST_REGISTRY
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--test-registry", help="a test registry folder (for tests of the kit only)")
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
    TEST_REGISTRY = args.test_registry
    try:
        if args.command == "prepare":
            result = prepare()
        elif args.command == "baseline":
            result = baseline(args.build_file, args.out, args.fixture_lakehouse)
        else:
            result = check(args.package, args.selector, args.patch, args.record, args.approved, args.expected_failure)
    except (FixtureError, launcher.ClientError, run_lock.Refused, run_lock.RegistryError) as error:
        sys.stderr.write(f"package fixture: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
