"""Build one layer from frozen project sources and verified snapshot inputs.

Stand-in setup checks: ``python -m scripts.orchestration.ecs_engine_cases --report data/e2e/task_engines/new/report.json``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from orchestration.pipeline_protocol import PipelineError, read_record, safe_name
from scripts.lakehouse.ecs_engine import TemporaryStorageError, duckdb_temp_settings, validate_task_credentials

IMPORT_SQL = 'create or replace table main."{table}" as select * from read_parquet(?)'


def build_target(layer: str, mode: str) -> str:
    """Select bounded profiles when configured and native task credentials for real silver."""
    bounded = bool(duckdb_temp_settings())
    if (
        os.environ.get("AWS_EXECUTION_ENV") == "AWS_ECS_FARGATE"
        and mode == "publish_fixture"
        and any(os.environ.get(name) for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"))
    ):
        raise PipelineError("fixture_role_session_refused")
    try:
        task = validate_task_credentials(require_catalog=layer == "silver" and mode == "real")
    except RuntimeError as error:
        raise PipelineError(str(error)) from None
    if layer == "gold":
        return "pinned_silver_bounded" if bounded else "pinned_silver"
    if mode == "publish_fixture":
        return "fixture_bounded" if bounded else "fixture"
    if task:
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
                try:
                    info = path.stat()
                except FileNotFoundError:
                    continue
                logical += info.st_size
                allocated += info.st_blocks * 512
    return {"free_disk_bytes": information.f_bavail * information.f_frsize, "spill_logical_bytes": logical, "spill_allocated_bytes": allocated}


@contextmanager
def storage_measurements(folder: Path, settings: dict[str, str]) -> Iterator[dict[str, Any]]:
    """Sample bounded builds until their fixed deadline and report observed disk and spill extrema."""
    measurements: dict[str, Any] = {}
    if not settings:
        yield measurements
        return
    try:
        remaining = float(os.environ.get("HAI_DEADLINE_UTC", str(time.time() + 3600))) - time.time()
    except ValueError:
        raise PipelineError("temporary_storage_sampling_deadline") from None
    if not math.isfinite(remaining) or remaining <= 0:
        raise PipelineError("temporary_storage_sampling_deadline")
    deadline = time.monotonic() + remaining
    measurements.update(samples=0, sampling="periodic_and_boundaries", sampling_interval_seconds=0.1, complete_peak=False)
    errors: list[str] = []
    stopped = threading.Event()

    def sample() -> None:
        observed = disk_observation(folder, settings)
        if measurements["samples"] == 0:
            measurements["minimum_observed_free_disk_bytes"] = observed["free_disk_bytes"]
            measurements["observed_spill_logical_bytes"] = 0
            measurements["observed_spill_allocated_bytes"] = 0
        measurements["minimum_observed_free_disk_bytes"] = min(measurements["minimum_observed_free_disk_bytes"], observed["free_disk_bytes"])
        measurements["observed_spill_logical_bytes"] = max(measurements["observed_spill_logical_bytes"], observed["spill_logical_bytes"])
        measurements["observed_spill_allocated_bytes"] = max(measurements["observed_spill_allocated_bytes"], observed["spill_allocated_bytes"])
        measurements["samples"] += 1

    def monitor() -> None:
        while not stopped.wait(min(0.1, max(0.0, deadline - time.monotonic()))):
            if time.monotonic() >= deadline:
                errors.append("temporary_storage_sampling_deadline")
                return
            try:
                sample()
            except (OSError, PipelineError):
                errors.append("temporary_storage_sampling_failed")
                return

    try:
        sample()
    except (OSError, PipelineError):
        raise PipelineError("temporary_storage_sampling_failed") from None
    worker = threading.Thread(target=monitor, name="hai_storage_sampler", daemon=True)
    worker.start()
    try:
        yield measurements
    finally:
        stopped.set()
        worker.join(timeout=5)
        if worker.is_alive():
            raise PipelineError("temporary_storage_sampler_stop_timeout")
        if errors:
            raise PipelineError(errors[0])
        if time.monotonic() >= deadline:
            raise PipelineError("temporary_storage_sampling_deadline")
        try:
            sample()
        except (OSError, PipelineError):
            raise PipelineError("temporary_storage_sampling_failed") from None


def import_silver(folder: Path, expected: str, pin_sha: str, run_id: str, connection: Any) -> dict[str, int | float]:
    """Materialize exact exported snapshot bytes into the consuming run's database."""
    started = time.monotonic()
    verified_bytes = 0
    copy_seconds = 0.0
    hash_seconds = 0.0
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
                        hash_started = time.monotonic()
                        digest_bytes.update(block)
                        hash_seconds += time.monotonic() - hash_started
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
        "hash_seconds": hash_seconds,
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
        imported: dict[str, int | float] = {}
        from dbt.cli.main import dbtRunner

        with storage_measurements(args.folder, settings) as measurements:
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
            receipt["runtime_measurements"] = {**measurements, "import": imported}
        (args.folder / "build_receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        _, receipt_sha = read_record(args.folder, "build_receipt.json")
        sys.stdout.write(json.dumps({"layer": args.layer, "nodes": len(statuses), "receipt_sha256": receipt_sha}) + "\n")
    except (OSError, ValueError, PipelineError) as error:
        sys.stderr.write(f"layer build: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
