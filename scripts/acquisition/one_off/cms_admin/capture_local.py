"""Capture the approved CMS batch jobs locally, into the batch runner's own job folders, without any S3 write.

Run from the repository root:

    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/cms_admin/capture_local.py

A later ``python -m scripts.acquisition.batch --plan <batch.json> --execute`` finds these receipts and stores them
only after the pre-storage screen passes. Already-captured jobs are reused.
"""

import json
import sys
import time

from scripts.acquisition.batch import receipt_for_job, validate_batch
from scripts.acquisition.capture import capture, receipt_validator
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json
from scripts.acquisition.transport import Limits

HERE = REPO_ROOT / "data/acquisition_planning/cms_admin_20260929"


def main() -> None:
    """Capture each job once; report counts."""
    registry, validator = load_registry(), receipt_validator()
    batch = read_json(HERE / "batch.json")
    jobs = validate_batch(batch, registry, validator)
    root = REPO_ROOT / "data/acquisition_batches" / canonical_hash(batch)
    write_once(root / "batch.json", encoded_json(batch))
    counts = {"captured": 0, "reused": 0}
    incomplete: list[str] = []
    for job in jobs:
        directory = root / "jobs" / job["job_id"]
        write_once(directory / "job.json", encoded_json(job))
        receipt_path = receipt_for_job(directory, job, registry, validator)
        if receipt_path is None:
            time.sleep(1)
            receipt_path = capture(job["plan"], directory / "captures", registry, validator, Limits(**job["limits"]))
            counts["captured"] += 1
        else:
            counts["reused"] += 1
        if read_json(receipt_path)["snapshot_status"] != "acquired_unvalidated":
            incomplete.append(job["job_id"])
    sys.stdout.write(json.dumps({**counts, "incomplete": incomplete}) + "\n")
    sys.exit(1 if incomplete else 0)


if __name__ == "__main__":
    main()
