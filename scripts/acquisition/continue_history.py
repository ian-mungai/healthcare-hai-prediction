"""Execute reviewed archive maps with bounded downloads and immutable storage receipts."""

from __future__ import annotations

import argparse
import copy
import fcntl
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from scripts.acquisition.capture import capture, receipt_validator, validate_receipt
from scripts.acquisition.cli_tools import executable, run_argv
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, preflight, upload_snapshot, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json
from scripts.acquisition.transport import CaptureError, Limits
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration


def run_archive(mapping: dict, plans: dict, registry: dict, validator: Any, root: Path, settings: dict, outputs: dict) -> dict:
    """Capture a reviewed archive map or reuse its receipt and return the storage outcome."""
    mapping = {**mapping, "registry_sha256": canonical_hash(registry)}
    candidates = [plans[route] for route in mapping["route_ids"] if plans[route]["capture_plan"] is not None]
    selected = next((row for row in candidates if row["source_id"] == "main-hai-pdc"), candidates[0])
    plan = copy.deepcopy(selected["capture_plan"])
    plan["expected_sha256"] = mapping["sha256"]
    job = {"plan": plan, "mapping": mapping, "registry_sha256": canonical_hash(registry)}
    directory = root / canonical_hash(job)
    write_once(directory / "job.json", encoded_json(job))
    receipts = sorted(directory.glob("captures/*/*/receipt.json"))
    path = next((p for p in reversed(receipts) if read_json(p)["snapshot_status"] == "acquired_unvalidated"), None)
    if path is None:
        path = capture(plan, directory / "captures", registry, validator, Limits(max_bytes=min(mapping["bytes"], 512 * 1024**2), attempts=2))
    receipt = read_json(path)
    validate_receipt(receipt, validator, path.parent)
    if receipt["snapshot_status"] != "acquired_unvalidated":
        return {
            "source_ids": mapping["source_ids"],
            "url": mapping["url"],
            "status": "blocked_download",
            "reasons": receipt["quality_profile"]["checks_failed"],
            "receipt_path": str(path),
        }
    result = upload_snapshot(path, [], AwsCli(settings), outputs, registry, validator, archive_mapping=mapping)
    event = {
        "source_ids": mapping["source_ids"],
        "url": mapping["url"],
        "status": "stored_unvalidated",
        "receipt_path": str(path),
        "reconciliation_path": result["reconciliation_path"],
        "model_eligible": False,
    }
    write_once(directory / "completed.json", encoded_json(event))
    return event


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-maps", type=Path, required=True)
    parser.add_argument("--plan-register", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, default=REPO_ROOT / "data/historical_acquisition/reviewed_archives")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--workers", type=int, default=3, choices=range(1, 5))
    args = parser.parse_args()
    registry, validator = load_registry(), receipt_validator()
    maps, register = read_json(args.archive_maps), read_json(args.plan_register)
    if maps["registry_sha256"] != canonical_hash(registry):
        registry = load_registry(expected_sha256=maps["registry_sha256"])
    if any(item["registry_sha256"] != canonical_hash(registry) for item in (maps, register)):
        raise CaptureError("Reviewed archive inputs do not match the current locked registry.")
    for filename, digest in maps["inputs_sha256"].items():
        if fingerprint(Path(filename))[0] != digest:
            raise CaptureError("Reviewed archive input changed; review again before execution.")
    plans = {row["route_id"]: row for row in register["routes"]}
    sources = {row["source_id"]: row for row in registry["sources"]}
    for mapping in maps["archives"]:
        if any(sources[sid]["preferred_route"] == "access_hold" for sid in mapping["source_ids"]):
            raise CaptureError("Reviewed map includes an access-held source.")
        if any(plans[rid]["url"] != mapping["url"] for rid in mapping["route_ids"]):
            raise CaptureError("Reviewed map and registered routes disagree.")
    sys.stdout.write(
        str(
            json.dumps(
                {
                    "reviewed_archive_groups": len(maps["archives"]),
                    "download_bytes_before_retries": sum(m["bytes"] for m in maps["archives"]),
                    "workers": args.workers,
                    "executing": args.execute,
                }
            )
        )
        + "\n"
    )
    sys.stdout.flush()
    if not args.execute:
        return
    from scripts.acquisition.source_registry import require_collection_scope

    for mapping in maps["archives"]:
        for source_id in mapping["source_ids"]:
            require_collection_scope(source_id)
    settings, _ = load_configuration(REPO_ROOT / ".env")
    response = run_argv([executable("terraform"), "-chdir=" + str(REPO_ROOT / "infra"), "output", "-json"], capture_output=True, text=True, check=True)
    outputs = json.loads(response.stdout)
    preflight(AwsCli(settings), outputs)
    args.state_root.mkdir(parents=True, exist_ok=True)
    with (args.state_root / ".execution.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            jobs = {pool.submit(run_archive, mapping, plans, registry, validator, args.state_root, settings, outputs): mapping for mapping in maps["archives"]}
            for job in as_completed(jobs):
                mapping = jobs[job]
                try:
                    event = job.result()
                except (ValueError, OSError, KeyError, TypeError) as error:
                    event = {
                        "source_ids": mapping["source_ids"],
                        "url": mapping["url"],
                        "status": "pending_failure",
                        "reason": str(error) if isinstance(error, CaptureError) else type(error).__name__,
                    }
                sys.stdout.write(str(json.dumps(event)) + "\n")
                sys.stdout.flush()


if __name__ == "__main__":
    main()
