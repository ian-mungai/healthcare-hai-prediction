"""Build the features layer from the pinned gold extract (the analytics image: dbt and DuckDB, no network).

    python -m scripts.features.features_build --run <run ID> --mode <mode> --source-sha256 <project digest> --input-sha256 <gold pin digest> \
        --extract-sha256 <gold input digest>

The build does not call ``feature_columns.run_features``, because that runs ``dbt run`` and gives no test results. It reuses the trusted DuckDB steps of
``scripts.lakehouse.feature_columns`` in process:

1. ``full.duckdb`` takes the exported gold snapshots (every file is hashed again while it is copied) and all seeds (``dbt seed``).
2. ``worker_restrict`` copies only what a feature model may read; ``worker_stage_two`` builds the bridge filter tables with trusted SQL.
3. ``dbt build --select tag:features --exclude resource_type:seed <filter models>`` runs on that file with external access off, the enforced contracts and the
   project's run hooks emptied. Every selected model and test must end ``success`` or ``pass``; the as-of, identity and contract tests are part of the build.
4. The published tables become plain tables of ``staging.duckdb`` (a view stores the catalog name of the file that built it, so a sealed copy under another
   name could not read one), together with ``feature_build_inputs`` (what this set was built from), ``feature_build_receipt`` (the node statuses) and
   ``validation_own_snapshot`` (the own POS snapshot of each window, for the validation's independent provenance check).

``build_receipt.json`` carries the immutable input identities. Nothing in ``feature_build_inputs`` is a time or an invocation ID, so a retry rebuilds identical
content.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import shutil
import sys
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from orchestration.pipeline_protocol import PipelineError, read_record, safe_name
from scripts.features import feature_schema
from scripts.features.features_input import INPUT_NAME, PIN_NAME, check_gold_pin
from scripts.lakehouse import duckdb_bounds
from scripts.lakehouse import feature_columns as columns
from scripts.lakehouse.gold_publish import load_plan
from scripts.lakehouse.gold_publish_record import PublishError
from scripts.process import TimeoutExpired, run_command

CONTRACT = "data_contracts/lakehouse/features_publish.json"
GOLD_PLAN = "data_contracts/lakehouse/gold_contract.json"
IMPORT_SQL = 'create or replace table main."{table}" as select * from read_parquet(?)'
ATTACH_SQL = "attach '{path}' as {catalog} (read_only)"
MATERIALIZE_SQL = 'create table main."{table}" as select * from {catalog}.main."{table}"'
# The steps of the build in order, each with the weight of its share of the time left. The weights are the proportions of the fixed limits that the steps had
# (600, 900, 1800, 1800 and 3300 seconds), so a step that needs more than its share finds the time the earlier steps did not use.
STEPS = (("copy", 600), ("seed", 900), ("restrict", 1800), ("stage_two", 1800), ("build", 3300))
RESERVE_PERCENT = 25  # the share of the time to the phase deadline that stays for the phases after the build: validate, export, publish, upload and feast
PHASE_FLOOR_SECONDS = 1800  # a build with less than this to use is refused at once, by name, instead of failing at its last step
DEFAULT_PHASE_SECONDS = 4 * 3600  # the time to the deadline when no driver sets one: the longest the layer driver lets a phase run
DEADLINE_VARIABLE = "HAI_PHASE_DEADLINE_EPOCH"  # set by scripts.lakehouse.layer_run: the time (epoch seconds) by which this phase must end
DBT_CODE = "import sys; from dbt.cli.main import dbtRunner; sys.exit(0 if dbtRunner().invoke(sys.argv[1:]).success else 1)"
INPUTS_TABLE = "feature_build_inputs"
OWN_SNAPSHOT_SQL = (
    "create table main.validation_own_snapshot as select spine.spine_key, snapshot.state_code, snapshot.county_fips, left(snapshot.zip_code, 5) as zip5, "
    "snapshot.provider_subtype_code, snapshot.control_type_code from {catalog}.main.int_hospital_spine as spine "
    "inner join {catalog}.main.fct_pos_snapshot as snapshot on snapshot.ccn = spine.ccn and snapshot.period_end = spine.pos_period_end"
)
INPUTS_SQL = (
    "create table main.feature_build_inputs as select 'current' as build_key, ? as features_run_id, ? as gold_run_id, ? as gold_snapshot_set, "
    "? as gold_snapshot_set_sha256, ? as source_sha256, ? as feature_schema_sha256, ? as hai_outcome_controls_sha256"
)
RECEIPT_SQL = "create table main.feature_build_receipt (node varchar, resource_type varchar, status varchar)"


class BuildBudget:
    """The time each step of the build may take, derived from the deadline of the phase instead of fixed.

    The build may use ``100 - RESERVE_PERCENT`` percent of the time left when it starts. Each step, when it starts, gets the share of what is still left of that
    time that its weight is of the weights of the steps not yet run, so the last step (the dbt build) gets all that remains."""

    def __init__(self, deadline: float, clock: Callable[[], float] = time.time) -> None:
        self.deadline = deadline
        self.clock = clock
        self.left = deadline - clock()
        usable = self.left * (100 - RESERVE_PERCENT) / 100
        if usable < PHASE_FLOOR_SECONDS:
            raise PipelineError(
                f"{max(self.left, 0):.0f} seconds are left to the phase deadline, so the build may use {max(usable, 0):.0f}: "
                f"less than the {PHASE_FLOOR_SECONDS} it needs at least"
            )
        self.usable = usable
        self.usable_end = clock() + usable

    @classmethod
    def from_environment(cls, environ: Mapping[str, str], clock: Callable[[], float] = time.time) -> BuildBudget:
        text = environ.get(DEADLINE_VARIABLE)
        if text is None:
            return cls(clock() + DEFAULT_PHASE_SECONDS, clock)
        try:
            deadline = float(text)
        except ValueError:
            raise PipelineError(f"{DEADLINE_VARIABLE} is {text!r}, not a time in epoch seconds") from None
        return cls(deadline, clock)

    def limit(self, step: str) -> int:
        """The seconds the step may run, from the time left now. Refuses a step that starts after the build's share has ended."""
        remaining = self.usable_end - self.clock()
        if remaining < 1:
            raise PipelineError(f"no time is left for {step}: the build's share of the phase deadline ended")
        names = [name for name, _ in STEPS]
        later = sum(weight for name, weight in STEPS[names.index(step) :])
        return max(1, int(remaining) * dict(STEPS)[step] // later)

    def describe(self, step: str) -> str:
        share = f"{self.usable:.0f} seconds that the build may use, {100 - RESERVE_PERCENT}% of the {self.left:.0f} left to the phase deadline"
        return f"{step} may run {self.limit(step)} seconds: its share of the {share}"

    def seconds(self, step: str) -> int:
        """The limit of a step, stated in the log before the step starts."""
        sys.stderr.write(f"features build: {self.describe(step)}\n")
        return self.limit(step)


def published_tables(root: Path) -> list[str]:
    """The tables the features contract publishes, in name order."""
    document = json.loads((root / CONTRACT).read_text())
    return sorted(document["publish"]["tables"])


def import_gold(folder: Path, expected: str, pin_sha: str, run_id: str, connection: Any, seconds: int) -> list[str]:
    """Materialize the exported gold snapshot bytes into the run's database within ``seconds``; returns the table names."""
    manifest, digest = read_record(folder / INPUT_NAME, "manifest.json")
    if digest != expected or manifest.get("pins_sha256") != pin_sha or manifest.get("run_id") != run_id:
        raise PipelineError("the gold extract manifest differs from its accepted input")
    tables = manifest.get("snapshot_set")
    files = manifest.get("files")
    if not isinstance(tables, dict) or not isinstance(files, dict) or not files:
        raise PipelineError("the gold extract manifest is incomplete")
    with tempfile.TemporaryDirectory(prefix="gold_import_", dir=folder) as temporary:
        private = Path(temporary)
        copied: dict[str, list[str]] = {safe_name(name): [] for name in tables}
        deadline = time.monotonic() + seconds
        for name, sha in sorted(files.items()):
            parts = Path(name).parts
            if len(parts) != 2 or parts[0] not in copied or not parts[1].endswith(".parquet") or ".." in parts:
                raise PipelineError("the gold extract names an unexpected file")
            source = folder / INPUT_NAME / name
            if source.parent.is_symlink():
                raise PipelineError("the gold extract table directory is a symlink")
            descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
            destination = private / f"{sum(len(paths) for paths in copied.values())}.parquet"
            digest_bytes = hashlib.sha256()
            try:
                with destination.open("xb") as writer, os.fdopen(descriptor, "rb", closefd=False) as reader:
                    while block := reader.read(1 << 20):
                        if time.monotonic() >= deadline:
                            raise PipelineError(f"gold snapshot import exceeded {seconds} seconds")
                        digest_bytes.update(block)
                        writer.write(block)
            finally:
                os.close(descriptor)
            if digest_bytes.hexdigest() != sha:
                raise PipelineError("a gold snapshot export changed before import")
            copied[parts[0]].append(str(destination))
        for table, members in copied.items():
            if not members:
                raise PipelineError(f"{table}: no snapshot file is present")
            connection.execute(IMPORT_SQL.format(table=table), [members])
    return sorted(copied)


def write_profile(folder: Path, database: Path, locked: bool) -> Path:
    """The dbt profile of the build: one DuckDB target on a file, external access off when ``locked`` (a seed reads its CSV, so the seed step is not),
    memory and threads from the job's allocation when it sets them."""
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "profiles.yml").write_text(duckdb_bounds.shell_profile(database, locked))
    return folder


def run_child(arguments: list[str], root: Path, label: str, seconds: int) -> None:
    """Run one step in a child Python process with a deadline: dbt and the DuckDB workers each hold their database file open until their process ends, so no
    two steps share a process. A failed or late step raises with the tail of its output (names and counts, never row values)."""
    environment = dict(os.environ)
    try:
        result = run_command(sys.executable, arguments, cwd=root, env=environment, timeout=seconds)
    except TimeoutExpired as error:
        raise PipelineError(f"{label} ran longer than {seconds} seconds") from error
    if result.returncode != 0:
        raise PipelineError(f"{label} exited {result.returncode}: {(result.stdout + result.stderr).strip()[-1500:]}")


def run_dbt(arguments: list[str], root: Path, label: str, seconds: int) -> None:
    """One dbt invocation in a child process; a failed invocation raises."""
    run_child(["-c", DBT_CODE, *arguments], root, f"dbt {label}", seconds)


def run_worker(root: Path, action: str, seconds: int, *arguments: str) -> None:
    """One DuckDB step of ``feature_columns`` in a child process, within ``seconds``."""
    run_child(["-m", "scripts.lakehouse.feature_columns", "--worker", action, *arguments], root, f"worker {action}", seconds)


def check_results(work: Path, root: Path) -> tuple[str, list[tuple[str, str, str]]]:
    """Require every selected node to end ``success`` or ``pass``, and every feature model that is not a bridge filter to be among them.

    Returns the invocation ID and the (node, type, status) rows.
    """
    results, _ = read_record(work, "target/run_results.json")
    rows = [(str(item["unique_id"]), str(item["unique_id"]).split(".")[0], str(item["status"])) for item in results.get("results", [])]
    if not rows or any(status not in ("success", "pass") for _, _, status in rows):
        raise PipelineError("the features build results contain an incomplete or nonpassing node")
    built = {node.split(".")[-1] for node, kind, _ in rows if kind == "model"}
    filters = {path.stem for path in (root / columns.FILTER_FOLDER).glob("*.sql")}
    missing = sorted(set(columns.sql_models(root)) - filters - built)
    if missing:
        raise PipelineError(f"the features build did not build {missing[0]} (and {len(missing) - 1} more)")
    return str(results["metadata"]["invocation_id"]), rows


def materialize(
    source: Path,
    destination: Path,
    tables: list[str],
    gold_pin: dict[str, Any],
    run_id: str,
    source_sha: str,
    schema_sha: str,
    controls_sha: str,
    rows: list[tuple[str, str, str]],
) -> None:
    """Write ``destination`` with the published tables as plain tables, the build inputs, the receipt and the own-snapshot reference."""
    duckdb: Any = importlib.import_module("duckdb")
    destination.unlink(missing_ok=True)
    connection = duckdb_bounds.connect(duckdb, destination)
    try:
        # Views store the catalog name of the file that built them (the file's stem), so the build file is attached under that name.
        catalog = safe_name(source.stem)
        connection.execute(ATTACH_SQL.format(path=source, catalog=catalog))
        for table in tables:
            if table != INPUTS_TABLE:
                connection.execute(MATERIALIZE_SQL.format(table=safe_name(table), catalog=catalog))
        snapshot_set = json.dumps(gold_pin["snapshot_set"], sort_keys=True)
        connection.execute(
            INPUTS_SQL,
            [run_id, gold_pin["source_run_id"], snapshot_set, hashlib.sha256(snapshot_set.encode()).hexdigest(), source_sha, schema_sha, controls_sha],
        )
        connection.execute(OWN_SNAPSHOT_SQL.format(catalog=catalog))
        connection.execute(RECEIPT_SQL)
        connection.executemany("insert into main.feature_build_receipt values (?, ?, ?)", rows)
        connection.execute(f"detach {catalog}")
    finally:
        connection.close()


def build(args: argparse.Namespace) -> dict[str, Any]:
    """Run the build and return the receipt."""
    duckdb: Any = importlib.import_module("duckdb")
    folder, root = args.folder, args.project.parent
    gold_pin, pin_sha = read_record(folder, PIN_NAME)
    plan = json.loads((root / GOLD_PLAN).read_text())
    contract_tables = {*plan["facts"], *plan["dimensions_and_bridges"]}
    check_gold_pin(gold_pin, args.run, args.mode, set(load_plan(root / GOLD_PLAN).tables))
    if pin_sha != args.input_sha256:
        raise PipelineError("the gold pin differs from the accepted input digest")
    budget = BuildBudget.from_environment(os.environ)
    work = folder / "features_work"
    if work.is_symlink():
        raise PipelineError("the build work folder is a symlink")
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir()
    full, shell, stage = work / "full.duckdb", work / "shell.duckdb", work / "features.duckdb"
    connection = duckdb_bounds.connect(duckdb, full)
    try:
        if os.environ.get("DUCKDB_MEMORY_LIMIT"):
            connection.execute("SET memory_limit = ?", [os.environ["DUCKDB_MEMORY_LIMIT"]])
        if os.environ.get("JOB_THREADS"):
            connection.execute("SET threads = ?", [int(os.environ["JOB_THREADS"])])
        imported = import_gold(folder, args.extract_sha256, args.input_sha256, args.run, connection, budget.seconds("copy"))
    finally:
        connection.close()
    if not set(imported) <= contract_tables:
        raise PipelineError("the gold extract holds a table outside the frozen gold contract")
    project = work / "dbt"
    columns.copy_project(root, project, contract=True)
    seed_profile = write_profile(work / "seed_profiles", full, locked=False)
    run_dbt(["seed", "--select", "resource_type:seed", *columns.dbt_arguments(project, seed_profile, work / "seed")], root, "seed", budget.seconds("seed"))
    seeds = sorted({path.stem for folder_name in (columns.FEATURE_SEEDS, columns.LIVE_SEEDS) for path in (root / folder_name).glob("*.csv")})
    run_worker(root, "restrict", budget.seconds("restrict"), str(full), str(shell), str(root / GOLD_PLAN), *seeds)
    filters = sorted(path.stem for path in (root / columns.FILTER_FOLDER).glob("*.sql") if columns.FILTER_NAME.fullmatch(path.stem))
    run_worker(root, "stage_two", budget.seconds("stage_two"), str(shell), str(stage), *filters)
    profile = write_profile(work / "profiles", stage, locked=True)
    filter_paths = [f"path:{columns.FILTER_PATH}/{name}.sql" for name in filters]
    run_dbt(
        ["build", "--select", "tag:features", "--exclude", "resource_type:seed", *filter_paths, *columns.dbt_arguments(project, profile, work / "build")],
        root,
        "build tag:features",
        budget.seconds("build"),
    )
    invocation, rows = check_results(work / "build", root)
    schema_sha = feature_schema.schema_sha256(root)
    controls_sha = hashlib.sha256((project / "seeds/features/frozen_hai_outcome_controls.csv").read_bytes()).hexdigest()
    materialize(stage, folder / "staging.partial.duckdb", published_tables(root), gold_pin, args.run, args.source_sha256, schema_sha, controls_sha, rows)
    os.replace(folder / "staging.partial.duckdb", folder / "staging.duckdb")
    receipt = {
        "schema": 1,
        "run_id": args.run,
        "layer": "features",
        "mode": args.mode,
        "source_sha256": args.source_sha256,
        "input_sha256": args.input_sha256,
        "extract_sha256": args.extract_sha256,
        "feature_schema_sha256": schema_sha,
        "hai_outcome_controls_sha256": controls_sha,
        "invocation_id": invocation,
        "nodes": len(rows),
    }
    (folder / "build_receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    """Run the features build and print one JSON line with the node count and the receipt's digest."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--layer", default="features", choices=("features",))
    parser.add_argument("--mode", required=True, choices=("publish_fixture", "real"))
    parser.add_argument("--run", required=True)
    parser.add_argument("--folder", type=Path, default=Path("/workspace/out"))
    parser.add_argument("--project", type=Path, default=Path("/workspace/dbt"))
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--extract-sha256", required=True)
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        if args.folder.joinpath("staging.duckdb").is_symlink():
            raise PipelineError("the build destination is a symlink")
        receipt = build(args)
        _, receipt_sha = read_record(args.folder, "build_receipt.json")
        sys.stdout.write(json.dumps({"layer": "features", "nodes": receipt["nodes"], "receipt_sha256": receipt_sha}) + "\n")
    except (OSError, ValueError, PipelineError, PublishError, columns.BuildError) as error:
        sys.stderr.write(f"features build: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
