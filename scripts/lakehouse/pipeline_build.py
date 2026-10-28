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
from scripts.lakehouse.ecs_engine import TemporaryStorageError, duckdb_temp_settings, validate_task_credentials

IMPORT_SQL = 'create or replace table main."{table}" as select * from read_parquet(?)'


def build_target(layer: str, mode: str) -> str:
    """Select bounded profiles when configured and native task credentials for real silver."""
    bounded = bool(duckdb_temp_settings())
    if layer == "gold":
        return "pinned_silver_bounded" if bounded else "pinned_silver"
    if mode == "publish_fixture":
        return "fixture_bounded" if bounded else "fixture"
    if os.environ.get("AWS_EXECUTION_ENV") == "AWS_ECS_FARGATE":
        try:
            validate_task_credentials()
        except RuntimeError as error:
            raise PipelineError(str(error)) from None
        return "lakehouse_task"
    return "lakehouse_session_bounded" if bounded else "lakehouse_session"


def disk_observation(folder: Path, settings: dict[str, str]) -> dict[str, int]:
    """Measure free disk and current spill files without claiming a continuous peak."""
    information = os.statvfs(folder)
    spill = Path(settings["temp_directory"])
    logical = 0
    allocated = 0
    if spill.is_dir():
        for path in spill.rglob("*"):
            if path.is_symlink():
                raise PipelineError("temporary_storage_link_refused")
            if path.is_file():
                info = path.stat()
                logical += info.st_size
                allocated += info.st_blocks * 512
    return {"free_disk_bytes": information.f_bavail * information.f_frsize, "spill_logical_bytes": logical, "spill_allocated_bytes": allocated}


def import_silver(folder: Path, expected: str, pin_sha: str, run_id: str, connection: Any) -> dict[str, int | float]:
    """Materialize exact exported snapshot bytes into the consuming run's database."""
    started = time.monotonic()
    verified_bytes = 0
    copy_seconds = 0.0
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
            copy_started = time.monotonic()
            try:
                with destination.open("xb") as writer, os.fdopen(descriptor, "rb", closefd=False) as reader:
                    while block := reader.read(1 << 20):
                        if time.monotonic() >= deadline:
                            raise PipelineError("silver snapshot import exceeded 600 seconds")
                        digest_bytes.update(block)
                        writer.write(block)
                        verified_bytes += len(block)
            finally:
                os.close(descriptor)
            copy_seconds += time.monotonic() - copy_started
            if digest_bytes.hexdigest() != sha:
                raise PipelineError("a silver snapshot export changed before import")
            copied[parts[0]].append(str(destination))
        for table, members in copied.items():
            if not members:
                raise PipelineError(f"{table}: no snapshot file is present")
            connection.execute(IMPORT_SQL.format(table=table), [members])
    return {
        "verified_input_bytes": verified_bytes,
        "input_copy_peak_bytes": verified_bytes,
        "verified_copy_seconds": copy_seconds,
        "import_seconds": time.monotonic() - started,
    }


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
        try:
            settings = duckdb_temp_settings()
            target = build_target(args.layer, args.mode)
        except TemporaryStorageError as error:
            raise PipelineError(str(error)) from None
        observations = [disk_observation(args.folder, settings)] if settings else []
        imported: dict[str, int | float] = {}
        from dbt.cli.main import dbtRunner

        if args.layer == "gold":
            import duckdb

            if not args.extract_sha256:
                raise PipelineError("gold has no accepted Iceberg extract")
            temporary = args.folder / "staging.partial.duckdb"
            if temporary.is_symlink() or (args.folder / "staging.duckdb").is_symlink():
                raise PipelineError("the build destination is a symlink")
            temporary.unlink(missing_ok=True)
            connection = duckdb.connect(str(temporary), config=settings)
            try:
                connection.execute("SET memory_limit = ?", [os.environ["DUCKDB_MEMORY_LIMIT"]])
                connection.execute("SET threads = ?", [int(os.environ["JOB_THREADS"])])
                imported = import_silver(args.folder, args.extract_sha256, args.input_sha256, args.run, connection)
            finally:
                connection.close()
            os.replace(temporary, args.folder / "staging.duckdb")
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
        if settings:
            observations.append(disk_observation(args.folder, settings))
            receipt["runtime_measurements"] = {
                "minimum_observed_free_disk_bytes": min(item["free_disk_bytes"] for item in observations),
                "observed_spill_logical_bytes": max(item["spill_logical_bytes"] for item in observations),
                "observed_spill_allocated_bytes": max(item["spill_allocated_bytes"] for item in observations),
                "sampling": "build_boundaries",
                "complete_peak": False,
                "import": imported,
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
