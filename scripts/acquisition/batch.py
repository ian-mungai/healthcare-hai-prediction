"""Explicit bounded batches; validation is offline and execution requires --execute."""

from __future__ import annotations

import argparse
import fcntl
import json
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition import s3_store
from scripts.acquisition.capture import capture, prepare_capture, receipt_validator, validate_receipt
from scripts.acquisition.cli_tools import executable, run_argv
from scripts.acquisition.reuse_capture import plan_identity, reuse_capture
from scripts.acquisition.s3_store import AwsCli, component, encoded_json, preflight, upload_snapshot, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json
from scripts.acquisition.storage_controls import reuse_identity
from scripts.acquisition.transport import CaptureError, Limits
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration
from scripts.process import SubprocessError


def validate_batch(batch: dict, registry: dict, validator: Any) -> list[dict]:
    """Validate immutable job identities, route permissions and limits; return the selected jobs."""
    if set(batch) != {"batch_version", "registry_sha256", "jobs"} or batch["batch_version"] != 1:
        raise CaptureError("Use an explicit versioned batch with a registry hash and jobs.")
    if batch["registry_sha256"] != canonical_hash(registry) or not isinstance(batch["jobs"], list) or not batch["jobs"]:
        raise CaptureError("The batch must retain the selected registry fingerprint and nonempty jobs.")
    ids: set[str] = set()
    for job in batch["jobs"]:
        if set(job) != {"job_id", "plan", "references", "limits"}:
            raise CaptureError("Each job needs only an ID, capture plan, references and bounded limits.")
        identifier = component(job["job_id"])
        if identifier in ids or not isinstance(job["references"], list):
            raise CaptureError("Job IDs must be unique and reference selections must be a list.")
        ids.add(identifier)
        prepare_capture(job["plan"], registry, validator)
        limits = Limits(**job["limits"])
        if limits.max_bytes > 512 * 1024**2:
            raise CaptureError("Files above 512 MiB need a separately tested larger-file strategy, not truncated model inputs.")
    return batch["jobs"]


def receipt_for_job(directory: Path, job: dict, registry: dict, validator: Any) -> Path | None:
    """Return a saved receipt only when its registry and capture scope match the job."""
    receipts = sorted(directory.glob("captures/*/*/receipt.json"))
    for path in reversed(receipts):
        receipt = read_json(path)
        validate_receipt(receipt, validator, path.parent)
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        _, mode, _, _, url, _, body, expected_hash, _ = prepare_capture(job["plan"], registry, validator)
        if receipt["source"]["source_record_id"] != job["plan"]["source_id"] or lineage["registry_sha256"] != canonical_hash(registry):
            raise CaptureError("Recovered capture does not belong to this job's source/registry.")
        expected = (mode, url, body or {}, job["plan"]["scope"], job["plan"]["release"], job["plan"]["measurement_periods"], expected_hash)
        actual = (
            lineage["mode"],
            receipt["acquisition"]["requested_url"],
            receipt["acquisition"]["request_parameters"],
            lineage["scope"],
            receipt["release"],
            receipt["measurement_periods"],
            lineage.get("expected_sha256"),
        )
        if actual != expected:
            raise CaptureError("Recovered capture scope differs from the immutable job plan.")
        return path
    return None


def reserve_bls_requests(root: Path, attempts: int, now: datetime | None = None) -> None:
    """Reserve bounded BLS requests against the local rolling 24-hour ledger."""
    now = now or datetime.now(UTC)
    directory = root / "quota" / "bls"
    directory.mkdir(parents=True, exist_ok=True)
    # A rolling 24-hour cap is conservative across uncertain publisher reset boundaries.
    used = 0
    for path in directory.glob("*.json"):
        reservation = read_json(path)
        timestamp = datetime.fromisoformat(reservation["reserved_at_utc"])
        if (now - timestamp).total_seconds() < 24 * 60 * 60:
            used += reservation["reserved_requests"]
    if used + attempts > 25:
        raise CaptureError("BLS v1 rolling 24-hour request budget exhausted; no new request was sent.")
    write_once(directory / f"{uuid.uuid4().hex}.json", encoded_json({"reserved_at_utc": now.isoformat(), "reserved_requests": attempts}))


def run_batch(batch: dict, root: Path, registry: dict, validator: Any, client: Any = None, outputs: dict | None = None, **options: Any) -> dict:
    """Validate a batch offline or execute it under a state-root lock when explicitly enabled."""
    jobs = validate_batch(batch, registry, validator)
    max_jobs, max_bytes = options.get("max_jobs", 5), options.get("max_download_bytes", 2 * 1024**3)
    if type(max_jobs) is not int or not 1 <= max_jobs <= 100 or type(max_bytes) is not int or max_bytes <= 0:
        raise CaptureError("Select 1-100 jobs and a positive per-run download budget.")
    if not options.get("execute", False):
        return {"status": "validated_offline", "job_count": len(jobs), "max_jobs_per_run": max_jobs, "aws_calls": 0, "downloads": 0}
    if client is None or outputs is None:
        raise CaptureError("Live execution requires explicit project storage configuration.")
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".execution.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CaptureError("Another acquisition batch is active in this state root.") from None
        preflight(client, outputs)
        return execute_jobs(batch, jobs, root, registry, validator, client, outputs, max_jobs, max_bytes, options)


def execute_jobs(
    batch: dict, jobs: list[dict], root: Path, registry: dict, validator: Any, client: Any, outputs: dict, max_jobs: int, max_bytes: int, options: dict
) -> dict:
    """Execute bounded jobs and return preserved completion and failure events."""
    batch_root = root / canonical_hash(batch)
    write_once(batch_root / "batch.json", encoded_json(batch))
    events: list[dict] = []
    shared: dict = {}
    capture_cache: dict[str, Path] = {}
    reserved_bytes, started = 0, 0
    capture_fn, sleep_fn = options.get("capture_fn", capture), options.get("sleep_fn", time.sleep)
    for job in jobs:
        layout_hash = s3_store.load_routes(registry)[job["plan"]["source_id"]]["layout_sha256"]
        directory = batch_root / "jobs" / job["job_id"]
        state_path = directory / "completed_collection_layout.json"
        if state_path.exists():
            state = read_json(state_path)
            if state.get("job_sha256") != canonical_hash(job) or state.get("layout_sha256") != layout_hash:
                raise CaptureError("Completed job differs from its immutable plan.")
            receipt_path = receipt_for_job(directory, job, registry, validator)
            if receipt_path is None:
                raise CaptureError("Completed job is missing its captured receipt.")
            receipt = read_json(receipt_path)
            reconciliation = read_json(Path(state["reconciliation_path"]))
            group = reuse_identity(receipt)
            capture_cache[group] = receipt_path
            for entry in reconciliation["objects"]:
                if entry["role"] in {"data", "api_page"}:
                    record = entry["object"]
                    shared[canonical_hash({"scope": group, "sha256": record["sha256"], "role": entry["role"], "key": record["key"]})] = record
            events.append(
                {"job_id": job["job_id"], "status": "already_completed", "reconciliation_path": state["reconciliation_path"], "live_s3_rechecked": False}
            )
            continue
        if started == max_jobs:
            break
        started += 1
        write_once(directory / "job.json", encoded_json(job))
        receipt_path = receipt_for_job(directory, job, registry, validator)
        limits = Limits(**job["limits"])
        try:
            if receipt_path is None:
                cached = capture_cache.get(plan_identity(job["plan"], registry, validator))
                if cached and job["plan"].get("mode", "file") == "file" and job["plan"].get("expected_sha256"):
                    receipt_path = reuse_capture(job["plan"], cached, directory / "captures", registry, validator)
                else:
                    pages = limits.max_pages if job["plan"].get("mode") == "il_directory" else 1
                    upper_bound = limits.max_bytes * pages * limits.attempts
                    if reserved_bytes + upper_bound > max_bytes:
                        raise CaptureError("Per-run download budget would be exceeded; job remains pending.")
                    if job["plan"].get("mode") == "bls_api":
                        reserve_bls_requests(root, limits.attempts)
                    reserved_bytes += upper_bound
                    sleep_fn(1)
                    receipt_path = capture_fn(job["plan"], directory / "captures", registry, validator, limits)
            receipt = read_json(receipt_path)
            incomplete = receipt["snapshot_status"] != "acquired_unvalidated"
            report = upload_snapshot(
                receipt_path, [] if incomplete else job["references"], client, outputs, registry, validator, audit_only=incomplete, shared=shared
            )
            state = {
                "job_id": job["job_id"],
                "job_sha256": canonical_hash(job),
                "layout_sha256": layout_hash,
                "status": "evidence_only" if incomplete else "stored_unvalidated",
                "receipt_path": str(receipt_path.resolve()),
                "reconciliation_path": str(Path(report["reconciliation_path"]).resolve()),
                "model_eligible": False,
            }
            if not incomplete:
                write_once(state_path, encoded_json(state))
                capture_cache[reuse_identity(receipt)] = receipt_path
            events.append(state)
        except (ValueError, OSError, KeyError, TypeError) as error:
            # Keep failures auditable without copying provider bodies, credentials or CLI stderr.
            events.append(
                {
                    "job_id": job["job_id"],
                    "status": "pending_failure",
                    "error_type": type(error).__name__,
                    "reason": str(error) if isinstance(error, CaptureError) else "Local validation or IO failed; inspect preserved evidence.",
                }
            )
    report = {
        "batch_sha256": canonical_hash(batch),
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "events": events,
        "unprocessed_jobs": len(jobs) - len(events),
        "reserved_download_bytes": reserved_bytes,
        "model_eligible": False,
    }
    write_once(batch_root / "runs" / f"{uuid.uuid4().hex}.json", encoded_json(report))
    return report


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description="Validate explicit batch plans offline; opt in to bounded execution separately.")
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--state-root", type=Path, default=REPO_ROOT / "data" / "acquisition_batches")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--max-jobs", type=int, default=5)
    parser.add_argument("--max-download-bytes", type=int, default=2 * 1024**3)
    args = parser.parse_args()
    try:
        registry, validator = load_registry(), receipt_validator()
        batch = read_json(args.plan)
        if batch["registry_sha256"] != canonical_hash(registry):
            registry = load_registry(expected_sha256=batch["registry_sha256"])
        validate_batch(batch, registry, validator)
        client, outputs = None, None
        if args.execute:
            from scripts.acquisition.source_registry import require_collection_scope

            for job in batch["jobs"]:
                require_collection_scope(job["plan"]["source_id"])
            settings, _ = load_configuration(REPO_ROOT / ".env")
            client = AwsCli(settings)
            result = run_argv(
                [executable("terraform"), f"-chdir={REPO_ROOT / 'infra'}", "output", "-json"], capture_output=True, text=True, timeout=30, check=True
            )
            outputs = json.loads(result.stdout)
        report = run_batch(
            batch,
            args.state_root,
            registry,
            validator,
            client,
            outputs,
            execute=args.execute,
            max_jobs=args.max_jobs,
            max_download_bytes=args.max_download_bytes,
        )
    except (ValueError, OSError, KeyError, TypeError, SubprocessError) as error:
        parser.error(str(error) if not isinstance(error, SubprocessError) else "Could not read local Terraform outputs.")
    sys.stdout.write(str(json.dumps(report, indent=2)) + "\n")
    if any(item["status"] in {"pending_failure", "evidence_only"} for item in report.get("events", [])):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
