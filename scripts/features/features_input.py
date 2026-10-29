"""Pin and read the committed gold snapshot set that the features build consumes.

    python -m scripts.features.features_input pin  --run <run ID> --mode <mode> [--source-run <gold run ID>]
    python -m scripts.features.features_input read --run <run ID> --mode <mode> --expected-pin-sha256 <digest>

``pin`` reuses ``silver_publish.pin`` and ``checked_set`` with a gold context (the gold publish plan, so every gold table is named and each snapshot is checked
in the catalog) and writes ``gold_pins.json`` once: the chosen ``publish_runs`` row of the gold namespace (the head, or the run named by ``--source-run``).
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
from scripts.lakehouse import silver_publish
from scripts.lakehouse.gold_publish import MODES, Context, Plan, load_plan, remove_tree, write_record
from scripts.lakehouse.gold_publish_record import RUN_ID, PublishError

PIN_NAME = "gold_pins.json"
INPUT_NAME = "gold_input"
NAMESPACES = {"publish_fixture": "gold_e2e", "real": "gold"}
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


def pin(ctx: Context, path: Path, source_run: str | None = None) -> dict[str, Any]:
    """Pin a committed gold set once: the silver pin function on a gold context, with its refusals worded for gold."""
    try:
        return silver_publish.pin(ctx, path, source_run)
    except PublishError as error:
        raise as_gold(error) from error


def check_gold_pin(record: Mapping[str, Any], run_id: str, mode: str, tables: set[str]) -> None:
    """Require a complete gold pin for this run and mode: its shape, identity, namespace and the exact table set."""
    if set(record) != {"schema", "run_id", "mode", "namespace", "source_run_id", "snapshot_set"} or record.get("schema") != 1:
        raise PipelineError("the gold pin has an unsupported shape")
    if (
        record["run_id"] != run_id
        or record["mode"] != mode
        or mode not in NAMESPACES
        or record["namespace"] != NAMESPACES[mode]
        or not RUN_ID.fullmatch(str(record["source_run_id"]))
    ):
        raise PipelineError("the gold pin identity does not match this consumer")
    snapshots = record["snapshot_set"]
    if not isinstance(snapshots, dict) or set(snapshots) != tables:
        raise PipelineError("the gold pin does not name exactly the gold publication tables")
    if any(type(value) is not int or not 0 < value <= MAX_SNAPSHOT for value in snapshots.values()):
        raise PipelineError("the gold pin contains an invalid snapshot ID")


def extract(ctx: Context, folder: Path, expected_pin: str) -> dict[str, Any]:
    """Read exactly the pinned gold snapshots and export each table to Parquet with a manifest of file digests."""
    pins, pin_sha = read_record(folder, PIN_NAME)
    if pin_sha != expected_pin:
        raise PipelineError("the gold pin changed after launcher acceptance")
    check_gold_pin(pins, ctx.run_id, ctx.mode, set(ctx.plan.tables))
    destination = folder / INPUT_NAME
    partial = folder / f"{INPUT_NAME}.partial"
    previous = folder / f"{INPUT_NAME}.previous"
    for path in (destination, partial, previous):
        if path.is_symlink():
            raise PipelineError("a gold extract path is a symlink")
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
    parser.add_argument("--expected-pin-sha256", default="", help="read: the digest of the pin record that the launcher accepted")
    parser.add_argument("--folder", type=Path, default=Path("/workspace/out"))
    parser.add_argument(
        "--record", type=Path, help="pin: the pin record to write (default: gold_pins.json in the folder); read: the same file, which must be the folder's"
    )
    parser.add_argument("--local-warehouse", type=Path)
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.local_warehouse is not None and (args.mode != "publish_fixture" or os.environ.get("POLARIS_URI")):
        parser.error("a local warehouse is allowed only in publish_fixture without a remote catalog")
    if args.record is not None and args.record != args.folder / PIN_NAME:
        parser.error(f"--record must be {PIN_NAME} in the folder")
    if args.step == "read" and not args.expected_pin_sha256:
        parser.error("read needs --expected-pin-sha256")
    try:
        from scripts.lakehouse.session import spark_session

        spark = spark_session(f"features_{args.step}", args.local_warehouse)
        try:
            ctx = Context(spark, gold_plan(), args.run, args.mode, args.folder / "publish", "", needs_sealed=False)
            if args.step == "pin":
                record = args.record or args.folder / PIN_NAME
                result = pin(ctx, record, args.source_run)
                digest = hashlib.sha256(record.read_bytes()).hexdigest()
                output = {"step": "pin", "tables": len(result["snapshot_set"]), "sha256": digest}
            else:
                result = extract(ctx, args.folder, args.expected_pin_sha256)
                digest = hashlib.sha256((args.folder / INPUT_NAME / "manifest.json").read_bytes()).hexdigest()
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
