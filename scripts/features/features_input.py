"""Pin and read the committed gold snapshot set that the features build consumes.

    python -m scripts.features.features_input pin  --run <run ID> --mode <mode> [--source-run <gold run ID>]
    python -m scripts.features.features_input read --run <run ID> --mode <mode> --expected-pin-sha256 <digest>

``pin`` reuses ``silver_publish.pin`` and ``checked_set`` with a gold context (the gold publish plan, so every gold table is named and each snapshot is checked
in the catalog) and writes ``gold_pins.json`` once: the chosen ``publish_runs`` row of the gold namespace (the head, or the run named by ``--source-run``).
With ``--newest-only`` (the features job of the AWS chain) the named gold run must be the newest committed one: the chain reads the set its own gold job just
published, and gold maintenance in that job keeps only the newest sets, so an older run is refused by name before anything is pinned.
A retry reads the same record and re-checks it. ``read`` exports exactly the pinned snapshots (``version as of``) to ``gold_input/`` with a manifest of file
digests, in the shape of ``silver_input.extract``, which hardcodes the silver paths. Both run in the Spark image and record names, counts and hashes only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from orchestration.pipeline_protocol import PipelineError, digest_file, read_record
from scripts.features import feature_schema, feature_sets, features_publish
from scripts.lakehouse import silver_publish
from scripts.lakehouse.gold_publish import MODES, Context, Plan, load_plan, published_head, published_runs, remove_tree, write_record
from scripts.lakehouse.gold_publish_record import RUN_ID, PublishError

PIN_NAME = "gold_pins.json"
INPUT_NAME = "gold_input"
BASE_PIN_NAME = "base_pins.json"
BASE_INPUT_NAME = "base_input"
NAMESPACES = {"publish_fixture": "gold_e2e", "real": "gold"}
BASE_NAMESPACES = {"publish_fixture": "features_e2e", "real": "features"}
SNAPSHOT_SQL = "select * from {table} version as of {snapshot}"
MAX_SNAPSHOT = 9223372036854775807


def gold_plan() -> Plan:
    """The gold publish plan, which names every gold table; refuses a plan whose namespaces are not the two gold ones."""
    plan = load_plan()
    if dict(plan.namespaces) != NAMESPACES:
        raise PublishError("the gold publish plan does not map the two modes to gold_e2e and gold")
    return plan


def as_gold(error: PublishError) -> PublishError:
    """The refusal of the silver pin code with its wording for this layer."""
    return PublishError(str(error).replace("silver", "gold"))


def require_newest(ctx: Context, source_run: str | None) -> None:
    """The chain rule: the named gold run is the newest committed gold run, or the pin is refused and names both runs."""
    if not source_run:
        raise PublishError("the chain pin names no gold run: the features job must name the gold run its chain just published")
    head = published_head(ctx)
    if head is None:
        raise PublishError(f"no committed gold run exists, so gold run {source_run} cannot be the newest")
    if head["run_id"] != source_run:
        raise PublishError(
            f"gold run {source_run} is not the newest committed gold run ({head['run_id']}): the chain reads only the set its gold job just published"
        )


def pin(ctx: Context, path: Path, source_run: str | None = None, newest_only: bool = False) -> dict[str, Any]:
    """Pin a committed gold set once: the silver pin function on a gold context, with its refusals worded for gold.

    ``newest_only`` applies the chain rule first (``require_newest``)."""
    if newest_only:
        require_newest(ctx, source_run)
    try:
        return silver_publish.pin(ctx, path, source_run)
    except PublishError as error:
        raise as_gold(error) from error


def check_pin(record: Mapping[str, Any], run_id: str, mode: str, tables: set[str], namespaces: Mapping[str, str], label: str) -> None:
    """Require a complete pin of the named layer for this run and mode: its shape, identity, namespace and the exact table set."""
    if set(record) != {"schema", "run_id", "mode", "namespace", "source_run_id", "snapshot_set"} or record.get("schema") != 1:
        raise PipelineError(f"the {label} pin has an unsupported shape")
    if (
        record["run_id"] != run_id
        or record["mode"] != mode
        or mode not in namespaces
        or record["namespace"] != namespaces[mode]
        or not RUN_ID.fullmatch(str(record["source_run_id"]))
    ):
        raise PipelineError(f"the {label} pin identity does not match this consumer")
    snapshots = record["snapshot_set"]
    if not isinstance(snapshots, dict) or set(snapshots) != tables:
        raise PipelineError(f"the {label} pin does not name exactly the {label} publication tables")
    if any(type(value) is not int or not 0 < value <= MAX_SNAPSHOT for value in snapshots.values()):
        raise PipelineError(f"the {label} pin contains an invalid snapshot ID")


def check_gold_pin(record: Mapping[str, Any], run_id: str, mode: str, tables: set[str]) -> None:
    """Require a complete gold pin for this run and mode: its shape, identity, namespace and the exact table set."""
    check_pin(record, run_id, mode, tables, NAMESPACES, "gold")


def check_base_pin(record: Mapping[str, Any], run_id: str, mode: str, tables: set[str]) -> None:
    """Require a complete base pin for this run and mode: the domain matrices of one committed base run in the features namespace."""
    check_pin(record, run_id, mode, tables, BASE_NAMESPACES, "base")


def base_tables(schema: Mapping[str, Any] | None = None) -> list[str]:
    """The tables a model set run reads from its base run: the domain matrices."""
    return feature_sets.domain_tables(schema if schema is not None else feature_schema.load_schema())


def pin_base(ctx: Context, path: Path, source_run: str | None) -> dict[str, Any]:
    """Pin the domain matrices of the newest committed base run once. The context is the features plan; the named run must be the newest base run.

    A model set reads only the base run that its own chain just published, so an older base run, a run of a model set and a missing run are refused by name
    before anything is pinned. A retry reads the same pin and checks it against the catalog again."""
    tables = base_tables()
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise PublishError("the base pin is a symlink")
        previous, _ = silver_publish.read_record(path)
        if previous.get("run_id") != ctx.run_id or previous.get("mode") != ctx.mode or previous.get("namespace") != ctx.namespace:
            raise PublishError("the base pin belongs to another run, mode or namespace")
        if source_run is not None and previous.get("source_run_id") != source_run:
            raise PublishError("the selected base run differs from the immutable pin")
        row = next((entry for entry in published_runs(ctx) if entry["run_id"] == previous.get("source_run_id")), None)
        if row is None or checked_tables(ctx, row, tables) != previous.get("snapshot_set"):
            raise PublishError("the base pin no longer matches its committed set")
        return previous
    if not source_run:
        raise PublishError("the model set pin names no base run: the job must name the base run its chain just published")
    newest = feature_sets.newest_base_row(published_runs(ctx), ctx.plan.tables, ctx.namespace)
    if newest is None:
        raise PublishError(f"no committed base run exists, so base run {source_run} cannot be the newest")
    if newest["run_id"] != source_run:
        raise PublishError(f"base run {source_run} is not the newest committed base run ({newest['run_id']}): a model set reads only the newest base run")
    record = {
        "schema": 1,
        "run_id": ctx.run_id,
        "mode": ctx.mode,
        "namespace": ctx.namespace,
        "source_run_id": newest["run_id"],
        "snapshot_set": checked_tables(ctx, newest, tables),
    }
    silver_publish.write_once(path, record)
    return record


def checked_tables(ctx: Context, row: Mapping[str, Any], tables: Sequence[str]) -> dict[str, int]:
    """The snapshot IDs a committed row gives the named tables, each checked in the catalog; the row must be of this mode and name every table."""
    if row.get("mode") != ctx.mode:
        raise PublishError("the committed base set belongs to another mode")
    snapshots = row.get("snapshot_set")
    if not isinstance(snapshots, dict) or not set(tables) <= set(snapshots):
        raise PublishError("the committed base set does not name every domain matrix")
    result = {}
    for name in sorted(tables):
        snapshot = snapshots[name]
        if type(snapshot) is not int or not 0 < snapshot <= MAX_SNAPSHOT:
            raise PublishError(f"{name}: the committed base snapshot ID is invalid")
        found = ctx.spark.sql(silver_publish.SNAPSHOT_QUERY.format(table=ctx.fq(name), snapshot=snapshot)).collect()
        if len(found) != 1 or int(found[0][0]) != snapshot:
            raise PublishError(f"{name}: the committed base snapshot is absent")
        result[name] = snapshot
    return result


def extract(ctx: Context, folder: Path, expected_pin: str, base: bool = False) -> dict[str, Any]:
    """Read exactly the pinned snapshots (gold, or with ``base`` the domain matrices of the base run) and export each table to Parquet with a manifest."""
    pin_name, input_name, label = (BASE_PIN_NAME, BASE_INPUT_NAME, "base") if base else (PIN_NAME, INPUT_NAME, "gold")
    pins, pin_sha = read_record(folder, pin_name)
    if pin_sha != expected_pin:
        raise PipelineError(f"the {label} pin changed after launcher acceptance")
    if base:
        check_base_pin(pins, ctx.run_id, ctx.mode, set(base_tables()))
    else:
        check_gold_pin(pins, ctx.run_id, ctx.mode, set(ctx.plan.tables))
    destination = folder / input_name
    partial = folder / f"{input_name}.partial"
    previous = folder / f"{input_name}.previous"
    for path in (destination, partial, previous):
        if path.is_symlink():
            raise PipelineError(f"a {label} extract path is a symlink")
    remove_tree(partial)
    partial.mkdir()
    files = {}
    for name, snapshot in pins["snapshot_set"].items():
        output = partial / name
        frame = ctx.spark.sql(SNAPSHOT_SQL.format(table=ctx.fq(name), snapshot=snapshot))
        frame.write.mode("errorifexists").parquet(str(output))
        members = sorted(output.glob("*.parquet"))
        if not members:
            raise PipelineError(f"{name}: Iceberg extraction produced no parquet files")
        for member in members:
            files[str(member.relative_to(partial))] = digest_file(member)
    document = {
        "schema": 1,
        "run_id": ctx.run_id,
        "mode": ctx.mode,
        "namespace": ctx.namespace,
        "pins_sha256": pin_sha,
        "snapshot_set": pins["snapshot_set"],
        "files": files,
    }
    write_record(partial / "manifest.json", document)
    for member in partial.rglob("*"):
        if member.is_file():
            member.chmod(0o444)
    remove_tree(previous)
    if destination.exists():
        os.rename(destination, previous)
    os.rename(partial, destination)
    remove_tree(previous)
    return document


def main(argv: Sequence[str] | None = None) -> int:
    """Pin a committed gold set or read the pinned snapshots; prints one JSON line with counts and the record's digest."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=("pin", "read"))
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--source-run", help="pin: the gold run to pin instead of the newest committed one")
    parser.add_argument("--newest-only", action="store_true", help="pin: refuse a --source-run that is not the newest committed gold run (the AWS chain)")
    parser.add_argument("--expected-pin-sha256", default="", help="read: the digest of the pin record that the launcher accepted")
    parser.add_argument("--folder", type=Path, default=Path("/workspace/out"))
    parser.add_argument(
        "--record", type=Path, help="pin: the pin record to write (default: gold_pins.json in the folder); read: the same file, which must be the folder's"
    )
    parser.add_argument("--local-warehouse", type=Path)
    parser.add_argument("--feature-set", default=os.environ.get(feature_sets.VARIABLE, ""), help="a model set pins the newest base run instead of a gold run")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    model_set = feature_sets.checked(args.feature_set) in feature_sets.MODEL_SETS
    pin_name = BASE_PIN_NAME if model_set else PIN_NAME
    if args.local_warehouse is not None and (args.mode != "publish_fixture" or os.environ.get("POLARIS_URI")):
        parser.error("a local warehouse is allowed only in publish_fixture without a remote catalog")
    if args.record is not None and args.record != args.folder / pin_name:
        parser.error(f"--record must be {pin_name} in the folder")
    if args.step == "read" and not args.expected_pin_sha256:
        parser.error("read needs --expected-pin-sha256")
    try:
        from scripts.lakehouse.session import spark_session

        spark = spark_session(f"features_{args.step}", args.local_warehouse)
        try:
            plan = features_publish.publish_plan(args.mode) if model_set else gold_plan()
            ctx = Context(spark, plan, args.run, args.mode, args.folder / "publish", "", needs_sealed=False)
            if args.step == "pin":
                record = args.record or args.folder / pin_name
                result = pin_base(ctx, record, args.source_run) if model_set else pin(ctx, record, args.source_run, args.newest_only)
                digest = hashlib.sha256(record.read_bytes()).hexdigest()
                output = {"step": "pin", "tables": len(result["snapshot_set"]), "sha256": digest}
            else:
                result = extract(ctx, args.folder, args.expected_pin_sha256, base=model_set)
                digest = hashlib.sha256((args.folder / (BASE_INPUT_NAME if model_set else INPUT_NAME) / "manifest.json").read_bytes()).hexdigest()
                output = {"step": "read", "tables": len(result["snapshot_set"]), "sha256": digest}
            sys.stdout.write(json.dumps(output) + "\n")
        finally:
            spark.stop()
    except (PipelineError, PublishError) as error:
        sys.stderr.write(f"features input: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
