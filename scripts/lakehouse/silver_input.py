"""Extract a committed silver snapshot set through Iceberg for an isolated gold build."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from orchestration.pipeline_protocol import PipelineError, check_pin, digest_file, read_record
from scripts.lakehouse.gold_publish import Context, remove_tree, write_record
from scripts.lakehouse.gold_publish_record import PublishError
from scripts.lakehouse.silver_publish import publish_plan

SNAPSHOT_SQL = "select * from {table} version as of {snapshot}"


def extract(ctx: Context, folder: Path, expected_pin: str) -> dict[str, Any]:
    """Read exactly the accepted silver IDs and export each table with a manifest of file digests."""
    pins, pin_sha = read_record(folder, "silver_pins.json")
    if pin_sha != expected_pin:
        raise PipelineError("the silver pin changed after launcher acceptance")
    check_pin(pins, ctx.run_id, ctx.mode, set(ctx.plan.tables))
    destination = folder / "silver_input"
    if destination.is_symlink():
        raise PipelineError("the silver extract destination is a symlink")
    partial = folder / "silver_input.partial"
    if partial.is_symlink():
        raise PipelineError("the silver extract destination is a symlink")
    remove_tree(partial)
    partial.mkdir()
    files = {}
    for name, snapshot in pins["snapshot_set"].items():
        table = ctx.fq(name)
        output = partial / name
        frame = ctx.spark.sql(SNAPSHOT_SQL.format(table=table, snapshot=snapshot))
        frame.write.mode("errorifexists").parquet(str(output))
        members = sorted(output.glob("*.parquet"))
        if not members:
            raise PipelineError(f"{name}: Iceberg extraction produced no parquet files")
        for path in members:
            files[str(path.relative_to(partial))] = digest_file(path)
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
    for path in partial.rglob("*"):
        if path.is_file():
            path.chmod(0o444)
    previous = folder / "silver_input.previous"
    if previous.is_symlink():
        raise PipelineError("the previous extract destination is a symlink")
    remove_tree(previous)
    if destination.exists():
        os.rename(destination, previous)
    os.rename(partial, destination)
    remove_tree(previous)
    return document


def main() -> int:
    """Extract accepted silver snapshots without consulting a local silver build file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", required=True, choices=("publish_fixture", "real"))
    parser.add_argument("--expected-pin-sha256", required=True)
    parser.add_argument("--folder", type=Path, default=Path("/workspace/out"))
    parser.add_argument("--local-warehouse", type=Path)
    args = parser.parse_args()
    if args.local_warehouse is not None and (args.mode != "publish_fixture" or os.environ.get("POLARIS_URI")):
        parser.error("a local catalog is allowed only for isolated publish_fixture")
    try:
        from scripts.lakehouse.session import spark_session

        spark = spark_session("silver_input", args.local_warehouse)
        try:
            ctx = Context(spark, publish_plan(args.mode), args.run, args.mode, args.folder / "publish", "", needs_sealed=False)
            document = extract(ctx, args.folder, args.expected_pin_sha256)
            raw = (args.folder / "silver_input/manifest.json").read_bytes()
            sys.stdout.write(json.dumps({"tables": len(document["snapshot_set"]), "sha256": hashlib.sha256(raw).hexdigest()}) + "\n")
        finally:
            spark.stop()
    except (PipelineError, PublishError) as error:
        sys.stderr.write(f"silver input: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
