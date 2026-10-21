"""Publish silver through candidate branches and pin only committed snapshot sets."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from scripts.lakehouse.gold_publish import (
    MODES,
    Context,
    Plan,
    build_store,
    catalog_backup,
    commit_publish,
    load_plan,
    publish_candidates,
    published_head,
    published_runs,
    reconcile_candidates,
)
from scripts.lakehouse.gold_publish_record import RUN_ID, PublishError, read_record

CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/lakehouse/silver_publish.json"
SNAPSHOT_QUERY = "SELECT snapshot_id FROM {table}.snapshots WHERE snapshot_id = {snapshot}"
MAX_SNAPSHOT = 9223372036854775807


def publish_plan(mode: str) -> Plan:
    """Return the fixed silver plan with backups scoped to that mode's storage prefix."""
    plan = load_plan(CONTRACT)
    if mode not in MODES or plan.namespaces != {"publish_fixture": "silver_e2e", "real": "silver"}:
        raise PublishError("the silver publish mode or namespace is invalid")
    return replace(plan, backup_prefix=f"lakehouse/{plan.namespaces[mode]}/catalog_backups/")


def write_once(path: Path, document: Mapping[str, Any]) -> None:
    """Persist immutable metadata, accepting an identical retry and refusing changed bytes or symlinks."""
    raw = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.urandom(8).hex()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != raw:
                raise PublishError(f"{path.name} already records another immutable input") from None
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def checked_set(ctx: Context, row: Mapping[str, Any]) -> dict[str, int]:
    """Validate a committed set's complete table membership and each snapshot's catalog existence."""
    if row.get("mode") != ctx.mode or not RUN_ID.fullmatch(str(row.get("run_id", ""))):
        raise PublishError("the committed silver set belongs to another mode or has no run identity")
    snapshots = row.get("snapshot_set")
    if not isinstance(snapshots, dict) or set(snapshots) != set(ctx.plan.tables):
        raise PublishError("the committed silver set does not name exactly the publish contract's tables")
    result = {}
    for name, snapshot in sorted(snapshots.items()):
        if type(snapshot) is not int or not 0 < snapshot <= MAX_SNAPSHOT:
            raise PublishError(f"{name}: the committed silver snapshot ID is invalid")
        rows = ctx.spark.sql(SNAPSHOT_QUERY.format(table=ctx.fq(name), snapshot=snapshot)).collect()
        if len(rows) != 1 or int(rows[0][0]) != snapshot:
            raise PublishError(f"{name}: the committed silver snapshot is absent")
        result[name] = snapshot
    return result


def pin(ctx: Context, path: Path, source_run: str | None = None) -> dict[str, Any]:
    """Pin a selected committed silver set once, preserving that choice on retries after the published head moves."""
    if source_run is not None and not RUN_ID.fullmatch(source_run):
        raise PublishError("the selected silver run ID is invalid")
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise PublishError("the silver pin is a symlink")
        previous, _ = read_record(path)
        if previous.get("run_id") != ctx.run_id or previous.get("mode") != ctx.mode or previous.get("namespace") != ctx.namespace:
            raise PublishError("the silver pin belongs to another run, mode or namespace")
        selected = previous.get("source_run_id")
        if not isinstance(selected, str) or (source_run is not None and selected != source_run):
            raise PublishError("the selected silver run differs from the immutable pin")
        row = next((entry for entry in published_runs(ctx) if entry["run_id"] == selected), None)
        if row is None or checked_set(ctx, row) != previous.get("snapshot_set"):
            raise PublishError("the silver pin no longer matches its committed set")
        return previous
    row = next((entry for entry in published_runs(ctx) if entry["run_id"] == source_run), None) if source_run else published_head(ctx)
    if row is None:
        raise PublishError("no matching committed silver snapshot set exists")
    record = {
        "schema": 1,
        "run_id": ctx.run_id,
        "mode": ctx.mode,
        "namespace": ctx.namespace,
        "source_run_id": row["run_id"],
        "snapshot_set": checked_set(ctx, row),
    }
    write_once(path, record)
    return record


def record_committed(ctx: Context, path: Path) -> dict[str, Any]:
    """Record this producer's committed set after comparing its commit artifact with the catalog."""
    record, _ = read_record(ctx.path("commit.json"))
    row = next((entry for entry in published_runs(ctx) if entry["run_id"] == ctx.run_id), None)
    if record.get("run_id") != ctx.run_id or record.get("state") != "committed" or row is None:
        raise PublishError("the silver producer has no committed publication")
    snapshots = checked_set(ctx, row)
    if record.get("snapshot_set") != snapshots:
        raise PublishError("the silver commit artifact differs from its catalog publication")
    output = {"schema": 1, "run_id": ctx.run_id, "mode": ctx.mode, "namespace": ctx.namespace, "snapshot_set": snapshots}
    write_once(path, output)
    return output


def main(argv: Sequence[str] | None = None) -> int:
    """Run one silver publish phase or record a verified committed input or output set."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=("candidates", "reconcile", "backup", "commit", "pin", "record"))
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--sealed-sha256", default="")
    parser.add_argument("--accepted-result-sha256", default="")
    parser.add_argument("--publish-dir", type=Path, default=Path("/workspace/out/publish"))
    parser.add_argument("--record", type=Path, default=Path("/workspace/out/silver_snapshot.json"))
    parser.add_argument("--source-run")
    parser.add_argument("--local-warehouse", type=Path)
    args = parser.parse_args(argv)
    if args.local_warehouse is not None and (args.mode != "publish_fixture" or os.environ.get("POLARIS_URI")):
        parser.error("a local warehouse is allowed only in publish_fixture without a remote catalog")
    try:
        from scripts.lakehouse.session import spark_session

        plan = publish_plan(args.mode)
        spark = spark_session(f"silver_{args.step}", args.local_warehouse)
        try:
            publishing = args.step not in ("pin", "record")
            ctx = Context(
                spark,
                plan,
                args.run,
                args.mode,
                args.publish_dir,
                args.sealed_sha256,
                build_store(plan, args.local_warehouse) if args.step == "backup" else None,
                create_namespace=args.local_warehouse is not None,
                needs_sealed=publishing,
                accepted_result_sha256=args.accepted_result_sha256,
            )
            if args.step == "pin":
                result = pin(ctx, args.record, args.source_run)
            elif args.step == "record":
                result = record_committed(ctx, args.record)
            else:
                result = {"candidates": publish_candidates, "reconcile": reconcile_candidates, "backup": catalog_backup, "commit": commit_publish}[args.step](
                    ctx
                )
            output = {"step": args.step, "tables": len(result.get("snapshot_set", {})) or result.get("tables", 0)}
            if args.step in ("pin", "record"):
                output["sha256"] = hashlib.sha256(args.record.read_bytes()).hexdigest()
            sys.stdout.write(json.dumps(output) + "\n")
        finally:
            spark.stop()
    except PublishError as error:
        sys.stderr.write(f"silver publish: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
