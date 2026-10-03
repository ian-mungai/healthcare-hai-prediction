"""Register additive historical routes without modifying the original approved registry."""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from scripts.acquisition.capture import capture, prepare_capture, receipt_validator, validate_receipt
from scripts.acquisition.cli_tools import executable, run_argv
from scripts.acquisition.collection_layout import load_routes
from scripts.acquisition.dataset_layout import cms_members, source_folder
from scripts.acquisition.mapped_archives import expand_tree
from scripts.acquisition.plan_remaining import capture_plan
from scripts.acquisition.s3_store import ACCESS_RELEASE_PATH, TERMS_ACCEPTANCE_PATH, AwsCli, encoded_json, preflight, upload_snapshot, write_once
from scripts.acquisition.source_registry import LOCK_PATH, canonical_hash, load_registry, read_json, validate_registry
from scripts.acquisition.transport import CaptureError, Limits
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration


def bounded_jobs(jobs: list[dict], budget: int) -> tuple:
    """Return selected and deferred jobs plus their reserved retry-inclusive byte budget."""
    selected, deferred, reserved = [], [], 0
    for job in jobs:
        limit = job["candidate"].get("max_bytes", 512 * 1024**2)
        if not isinstance(limit, int) or not 0 < limit <= 512 * 1024**2:
            raise CaptureError("Historical file limit must be a positive integer at most 512 MiB.")
        if reserved + limit * 2 <= budget:
            selected.append(job)
            reserved += limit * 2
        else:
            deferred.append(job)
    return selected, deferred, reserved


# A held source's reference document names the dated record that releases its hold; storage rechecks the bound bytes.
RELEASE_BINDINGS = {"terms": ("terms_sha256", TERMS_ACCEPTANCE_PATH), "access_release": ("access_release_sha256", ACCESS_RELEASE_PATH)}


def lineage_bindings(source: dict, candidate: dict) -> dict:
    """Return the receipt lineage binding for a held source's reference document, or none for other sources."""
    binding = candidate.get("release_binding")
    held = source["preferred_route"] == "access_hold"
    if binding is None:
        return {}
    if binding not in RELEASE_BINDINGS or not held:
        raise CaptureError("A release binding must be terms or access_release and applies only to a held source.")
    key, path = RELEASE_BINDINGS[binding]
    return {key: hashlib.sha256(path.read_bytes()).hexdigest()}


def extend_registry(original: dict, lock: dict, candidates: list[dict], rules: dict) -> tuple:
    """Return an additive registry, lock and jobs while retaining the original source restrictions."""
    validate_registry(original, lock)
    registry = copy.deepcopy(original)
    by_source = {source["source_id"]: source for source in registry["sources"]}
    jobs = []
    seen = set()
    for candidate in candidates:
        from scripts.acquisition.source_registry import require_collection_scope

        require_collection_scope(candidate["source_id"])
        source = by_source[candidate["source_id"]]
        data = candidate["role"] == "data"
        bindings = lineage_bindings(source, candidate)
        if (
            source["preferred_route"] == "access_hold"
            and (data or not bindings)
            or data
            and source["source_id"] in set(rules["privacy_review_sources"] + rules["large_file_review_sources"])
        ):
            raise CaptureError("Historical route retains an access, privacy or large-file hold.")
        if candidate["url"] in seen:
            continue
        seen.add(candidate["url"])
        identifier = source["source_id"] + ":history:" + canonical_hash(candidate)[:16]
        route = {
            "route_id": identifier,
            "url": candidate["url"],
            "format": candidate["format"],
            "route_type": candidate.get("route_type", "published_file"),
            "scope": candidate.get("scope", "Complete publisher-listed historical file; no truncation or clinical approval."),
            "status": "publisher_listed_historical_file_pending_capture",
            "vintage_or_release": candidate.get("label"),
            "measurement_period": None,
            "remaining_checks": source["required_checks"],
            "history_evidence": candidate["evidence"],
        }
        source["file_routes"].append(route)
        source["approval"]["file_route_count"] = len(source["file_routes"])
        plan = capture_plan(source, route, {"reference_route_ids": []})
        role = "methodology" if candidate["role"] == "reference" else candidate["role"]
        plan.update(role=role, expected_format=candidate["format"])
        plan["release"]["release_date"] = candidate.get("release_date")
        if candidate.get("expected_sha256"):
            plan["expected_sha256"] = candidate["expected_sha256"]
        if bindings:
            plan["lineage_bindings"] = bindings
        jobs.append({"job_id": identifier.replace(":", "_"), "plan": plan, "candidate": candidate})
    urls = defaultdict(set)
    for source in registry["sources"]:
        for route in source["file_routes"]:
            urls[route["url"]].add(source["source_id"])
    previous = {item["url"]: item for item in registry["shared_file_routes"]}
    registry["shared_file_routes"] = [
        {**previous.get(url, {"url": url, "reuse_rule": "Verify release, scope and byte hashes before reuse."}), "source_ids": sorted(ids)}
        for url, ids in sorted(urls.items())
        if len(ids) > 1
    ]
    registry["historical_extension"] = {
        "parent_registry_sha256": canonical_hash(original),
        "candidate_sha256": canonical_hash(candidates),
        "authorization_scope": "user_requested_historical_acquisition_for_unblocked_sources",
        "model_eligible": False,
    }
    new_lock = {**copy.deepcopy(lock), "registry_sha256": canonical_hash(registry)}
    validate_registry(registry, new_lock)
    return registry, new_lock, jobs


def map_captured_archive(path: Path, registry: dict) -> dict:
    """Return a checksum-bound member map derived from a complete captured archive."""
    receipt = read_json(path)
    parent = receipt["artifacts"][0]
    documentation_only = parent["role"] not in {"data", "api_page"}
    leaves = expand_tree(path.parent / parent["storage_path"], path.parent / "history_members", parent["sha256"])
    route = load_routes(registry)[receipt["source"]["source_record_id"]]
    collection = f"{route['publisher']}/{route['collection']}"
    published = {}
    if collection == "cms/hospitals" and not documentation_only:
        releases = defaultdict(list)
        for chain, leaf in leaves.items():
            releases[chain[:-1]].append(
                {
                    "archive_member": chain[-1],
                    "storage_path": leaf["local_path"].relative_to(path.parent).as_posix(),
                    "role": "reference" if leaf["reference_kind"] else "data",
                }
            )
        for chain, entries in releases.items():
            # The annual manifest describes nested releases, not tables. Bind each
            # data-bearing release to its own table manifest, retaining its chain.
            if any(entry["role"] == "data" for entry in entries) and any(Path(entry["archive_member"]).name == "manifest.json" for entry in entries):
                published.update({(*chain, name): info for name, info in cms_members(path.parent, entries).items()})
    members = []
    for chain, leaf in leaves.items():
        name = Path(chain[-1]).name
        role = "reference" if documentation_only or leaf["reference_kind"] else "data"
        folder = source_folder(receipt["source"]["source_record_id"])
        if collection == "cms/hospitals" and role == "data":
            if chain in published:
                info = published[chain]
                if info["publisher_size_status"] != "matched":
                    raise CaptureError("Publisher archive member size differs; preserve locally for review.")
                folder = info["dataset_id"]
            else:
                folder = "cms_legacy_" + re.sub(r"[^a-z0-9]+", "_", Path(name).stem.lower()).strip("_")
                folder = folder[:108].rstrip("_")
        members.append(
            {
                "member_chain": list(chain),
                "sha256": leaf["sha256"],
                "bytes": leaf["byte_count"],
                "role": role,
                "dataset_id": folder if role == "data" else None,
                "mapping_basis": "Publisher manifest where present; otherwise literal native table identity. Clinical scope remains pending.",
            }
        )
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    return {
        "registry_sha256": canonical_hash(registry),
        "sha256": parent["sha256"],
        "bytes": parent["byte_count"],
        "members": members,
        "source_ids": [receipt["source"]["source_record_id"]],
        "route_ids": [lineage["route_id"]],
        "url": receipt["acquisition"]["requested_url"],
        "release_date": receipt["release"]["release_date"],
        "collection_root": collection,
    }


def run_job(job: dict, registry: dict, validator: Any, directory: Path, settings: dict, outputs: dict) -> dict:
    """Execute one historical capture and return its preserved completion or failure event."""
    if job["candidate"].get("reviewed_unsigned_redirect"):
        from scripts.acquisition.history_redirects import install_redirect

        install_redirect(job["candidate"]["url"], job["candidate"]["reviewed_unsigned_redirect"])
    root = directory / "jobs" / job["job_id"]
    write_once(root / "job.json", encoded_json(job))
    completed = root / "completed.json"
    if completed.exists():
        return {"job_id": job["job_id"], "status": "already_completed", "live_s3_rechecked": False}
    receipts = sorted(root.glob("captures/*/*/receipt.json"))
    path = next((item for item in reversed(receipts) if read_json(item)["snapshot_status"] == "acquired_unvalidated"), None)
    if path is None:
        path = capture(job["plan"], root / "captures", registry, validator, Limits(max_bytes=job["candidate"].get("max_bytes", 512 * 1024**2), attempts=2))
    receipt = read_json(path)
    validate_receipt(receipt, validator, path.parent)
    event = {"job_id": job["job_id"], "source_id": job["plan"]["source_id"], "url": job["candidate"]["url"], "receipt_path": str(path)}
    if receipt["snapshot_status"] != "acquired_unvalidated":
        return {**event, "status": "blocked_download", "reasons": receipt["quality_profile"]["checks_failed"]}
    mapping = map_captured_archive(path, registry) if job["plan"]["expected_format"] == "zip" else None
    result = upload_snapshot(path, [], AwsCli(settings), outputs, registry, validator, archive_mapping=mapping)
    event.update(status="stored_unvalidated", reconciliation_path=result["reconciliation_path"], model_eligible=False)
    write_once(completed, encoded_json(event))
    return event


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--workers", type=int, default=3, choices=range(1, 5))
    parser.add_argument("--max-download-bytes", type=int, default=8 * 1024**3)
    args = parser.parse_args()
    candidates = read_json(args.candidates)["candidates"]
    registry, lock, jobs = extend_registry(load_registry(), read_json(LOCK_PATH), candidates, read_json(REPO_ROOT / "config/acquisition/planning_rules.json"))
    validator = receipt_validator()
    for job in jobs:
        prepare_capture(job["plan"], registry, validator)
    pending_jobs = [job for job in jobs if not (args.state_root / "jobs" / job["job_id"] / "completed.json").exists()]
    jobs, deferred, reserved = bounded_jobs(pending_jobs, args.max_download_bytes)
    sys.stdout.write(
        str(
            json.dumps(
                {
                    "validated_historical_jobs": len(jobs),
                    "source_count": len({job["plan"]["source_id"] for job in jobs}),
                    "deferred_jobs": len(deferred),
                    "reserved_download_bytes": reserved,
                    "executing": args.execute,
                    "model_eligible": False,
                }
            )
        )
        + "\n"
    )
    sys.stdout.flush()
    if not args.execute:
        return
    settings, _ = load_configuration(REPO_ROOT / ".env")
    result = run_argv([executable("terraform"), "-chdir=" + str(REPO_ROOT / "infra"), "output", "-json"], capture_output=True, text=True, check=True)
    outputs = json.loads(result.stdout)
    preflight(AwsCli(settings), outputs)
    write_once(args.state_root / "registry.json", encoded_json(registry))
    write_once(args.state_root / "registry_lock.json", encoded_json(lock))
    execution_lock = (args.state_root / ".execution.lock").open("a")
    fcntl.flock(execution_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = {pool.submit(run_job, job, registry, validator, args.state_root, settings, outputs): job for job in jobs}
        for future in as_completed(pending):
            job = pending[future]
            try:
                event = future.result()
            except (ValueError, OSError, KeyError, TypeError) as error:
                event = {
                    "job_id": job["job_id"],
                    "source_id": job["plan"]["source_id"],
                    "url": job["candidate"]["url"],
                    "status": "pending_failure",
                    "reason": str(error) if isinstance(error, CaptureError) else type(error).__name__,
                }
            write_once(args.state_root / "events" / (canonical_hash(event) + ".json"), encoded_json(event))
            sys.stdout.write(str(json.dumps(event)) + "\n")
            sys.stdout.flush()
    execution_lock.close()


if __name__ == "__main__":
    main()
