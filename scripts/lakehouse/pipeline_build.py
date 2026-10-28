"""Build one layer from frozen project sources and verified snapshot inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from orchestration.pipeline_protocol import PipelineError, read_record, safe_name

IMPORT_SQL = 'create or replace table main."{table}" as select * from read_parquet(?)'


def import_silver(folder: Path, expected: str, pin_sha: str, run_id: str, connection: Any) -> None:
    """Materialize exact exported snapshot bytes into the consuming run's database."""
    manifest, digest = read_record(folder / "silver_input", "manifest.json")
    if digest != expected or manifest.get("pins_sha256") != pin_sha or manifest.get("run_id") != run_id:
        raise PipelineError("the silver extract manifest differs from its accepted input")
    tables = manifest.get("snapshot_set")
    files = manifest.get("files")
    if not isinstance(tables, dict) or not isinstance(files, dict) or not files:
        raise PipelineError("the silver extract manifest is incomplete")
    with tempfile.TemporaryDirectory(prefix="silver_import_", dir=folder) as temporary:
        private = Path(temporary)
        copied: dict[str, list[str]] = {safe_name(name): [] for name in tables}
        deadline = time.monotonic() + 600
        for name, sha in sorted(files.items()):
            parts = Path(name).parts
            if len(parts) != 2 or parts[0] not in copied or not parts[1].endswith(".parquet") or ".." in parts:
                raise PipelineError("the silver extract names an unexpected file")
            source = folder / "silver_input" / name
            if source.parent.is_symlink():
                raise PipelineError("the silver extract table directory is a symlink")
            descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
            destination = private / f"{len([path for paths in copied.values() for path in paths])}.parquet"
            digest_bytes = hashlib.sha256()
            try:
                with destination.open("xb") as writer, os.fdopen(descriptor, "rb", closefd=False) as reader:
                    while block := reader.read(1 << 20):
                        if time.monotonic() >= deadline:
                            raise PipelineError("silver snapshot import exceeded 600 seconds")
                        digest_bytes.update(block)
                        writer.write(block)
            finally:
                os.close(descriptor)
            if digest_bytes.hexdigest() != sha:
                raise PipelineError("a silver snapshot export changed before import")
            copied[parts[0]].append(str(destination))
        for table, members in copied.items():
            if not members:
                raise PipelineError(f"{table}: no snapshot file is present")
            connection.execute(IMPORT_SQL.format(table=table), [members])


def main() -> int:
    """Run the fixed dbt build selection and write a receipt containing immutable input identities."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", required=True, choices=("silver", "gold"))
    parser.add_argument("--mode", required=True, choices=("publish_fixture", "real"))
    parser.add_argument("--run", required=True)
    parser.add_argument("--folder", type=Path, default=Path("/workspace/out"))
    parser.add_argument("--project", type=Path, default=Path("/workspace/dbt"))
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--extract-sha256")
    args = parser.parse_args()
    try:
        from dbt.cli.main import dbtRunner

        if args.layer == "gold":
            import duckdb

            if not args.extract_sha256:
                raise PipelineError("gold has no accepted Iceberg extract")
            temporary = args.folder / "staging.partial.duckdb"
            if temporary.is_symlink() or (args.folder / "staging.duckdb").is_symlink():
                raise PipelineError("the build destination is a symlink")
            temporary.unlink(missing_ok=True)
            connection = duckdb.connect(str(temporary))
            try:
                connection.execute("SET memory_limit = ?", [os.environ["DUCKDB_MEMORY_LIMIT"]])
                connection.execute("SET threads = ?", [int(os.environ["JOB_THREADS"])])
                import_silver(args.folder, args.extract_sha256, args.input_sha256, args.run, connection)
            finally:
                connection.close()
            os.replace(temporary, args.folder / "staging.duckdb")
        target = "pinned_silver" if args.layer == "gold" else "fixture" if args.mode == "publish_fixture" else "lakehouse"
        arguments = [
            "build",
            "--target",
            target,
            "--project-dir",
            str(args.project),
            "--profiles-dir",
            str(args.project),
            "--select",
            "resource_type:seed",
            ("+path:models/silver" if args.layer == "silver" else "path:models/gold"),
            "--indirect-selection",
            "cautious",
            *(["--exclude", "tag:features"] if args.layer == "gold" else []),
        ]
        result = dbtRunner().invoke(arguments)
        if not result.success:
            raise PipelineError("the layer build did not pass every selected dbt node")
        results, _ = read_record(args.folder, "target/run_results.json")
        statuses = results.get("results", [])
        if not statuses or any(item.get("status") not in ("success", "pass") for item in statuses):
            raise PipelineError("the layer build results contain an incomplete or nonpassing node")
        receipt = {
            "schema": 1,
            "run_id": args.run,
            "layer": args.layer,
            "mode": args.mode,
            "source_sha256": args.source_sha256,
            "input_sha256": args.input_sha256,
            "extract_sha256": args.extract_sha256,
            "invocation_id": results["metadata"]["invocation_id"],
            "nodes": len(statuses),
        }
        (args.folder / "build_receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        _, receipt_sha = read_record(args.folder, "build_receipt.json")
        sys.stdout.write(json.dumps({"layer": args.layer, "nodes": len(statuses), "receipt_sha256": receipt_sha}) + "\n")
    except (OSError, ValueError, PipelineError) as error:
        sys.stderr.write(f"layer build: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
