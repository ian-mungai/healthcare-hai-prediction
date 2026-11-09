"""Capture CMS Hospital Compare release zips from the NBER mirror and store their mapped members, with verified S3 storage.

Offline by default. ``--fetch`` downloads from data.nber.org only (bounded; a redirect to another host is refused): first
the March 2019 fidelity-gate zip, which must equal the CMS archive hash, then each release zip, which must equal its plan hash and
size. ``--execute`` stores the members the reviewed map names as data and every other member as a reference, under ``raw/`` with a
receipt, hashes and S3 version readback; the zip itself stays on this Mac. Each release is independent: one that fails is held
and named, never partly stored, and the others continue.
"""

import argparse
import json
import logging
import os
import sys
import zipfile
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from scripts.acquisition import nber_hc_contract as contract
from scripts.acquisition.bls_api_transport import collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import AwsCli, component, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.single_copy import load_policy
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.transport import Limits, download
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/datasets/historical_acquisition/nber_hospital_compare"
# The largest known release zip is about 22 MB; 64 MiB leaves room for growth and refuses anything far larger.
LIMITS = Limits(attempts=3, timeout_seconds=30, max_seconds=600, max_bytes=64 * 1024**2)


def completed_download(folder: Path, name: str, sha256: str, size: int) -> tuple[Path, dict] | None:
    """The first complete earlier download that still matches the planned hash and size, if any."""
    for record in sorted(folder.glob("run_*/attempt_*/transport.json")):
        meta = read_json(record)
        path = record.parent / name
        if meta["complete"] and path.is_file() and not path.is_symlink() and fingerprint(path) == (sha256, size) == (meta["sha256"], meta["byte_count"]):
            return path, meta
    return None


def fetch(
    url: str, folder: Path, name: str, sha256: str, size: int, label: str, allowed: bool, opener: Any, limits: Limits, fresh: bool = False
) -> tuple[Path, dict]:
    """Reuse a verified download, or download one now; a wrong or failed download is refused and never reused."""
    found = None if fresh and allowed else completed_download(folder, name, sha256, size)
    if found is not None:
        return found
    require(allowed, f"NBER {label} not captured; run with --fetch")
    folder.mkdir(parents=True, exist_ok=True)
    run = folder / f"run_{len(list(folder.glob('run_*'))) + 1:02}"
    result = download(url, run, name, "zip", "data", limits, opener=opener)
    require(result.complete, f"NBER {label} download failed")
    require(result.sha256 == sha256, f"NBER {label} download hash differs from the plan")
    require(result.byte_count == size, f"NBER {label} download size differs from the plan")
    found = completed_download(folder, name, sha256, size)
    if found is None:
        raise ValueError(f"NBER {label} download failed")
    return found


def check_gate(plan: dict, root: Path, allow_fetch: bool, opener: Any, limits: Limits) -> None:
    """Compare NBER's March 2019 zip with the CMS archive hash; with --fetch the zip is downloaded again on every run."""
    gate = plan["fidelity_gate"]
    name = contract.zip_name(contract.GATE_FOLDER)
    fetch(
        gate["url"],
        root / "downloads" / f"gate_{gate['expected_sha256'][:16]}",
        name,
        gate["expected_sha256"],
        gate["bytes"],
        "fidelity gate",
        allow_fetch,
        opener,
        limits,
        True,
    )


def make_receipt(plan: dict, release: dict, original: Path, meta: dict, branch: Path) -> Path:
    """Write the zip, its transport record and the receipt; the zip stays local, the mapped members go to S3."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "NBER base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == contract.RECEIPT_SOURCE)
    snapshot = branch / "capture"
    raw = snapshot / "raw" / original.name
    write_once(raw, original.read_bytes())
    write_once(snapshot / "audit/request_0001/attempt_01/transport.json", encoded_json(meta))
    metadata = {
        "publisher": contract.PUBLISHER,
        "release": {
            "advertised_history": "Hospital Compare release folders on the NBER mirror, original CMS flat-file zips",
            "dataset_version": None,
            "observed_history": f"{release['release_label']}; members mapped as data: {', '.join(sorted(release['members']))}",
            "publisher_release_label": release["release_label"],
            "publisher_updated_at": None,
            "release_date": release["release_date"],
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {"label": None, "start_date": None, "end_date": None, "period_type": "unknown", "source_basis": "unknown", "target_alignment": "unknown"}
        ],
        "governance": plan["governance"],
    }
    stamp = meta["retrieved_at_utc"].replace("-", "").replace(":", "")[:15] + "Z"
    receipt = base_receipt(source, metadata, f"CARE_TIMELY_CMS_NBER__{stamp}__{release['id'][:32]}", validator)
    receipt["acquisition"].update(
        requested_url=release["url"],
        resolved_url=meta["resolved_url"],
        retrieved_at_utc=meta["retrieved_at_utc"],
        http_status=meta["http_status"],
        tool_name="HistoricalSourceCapture",
        tool_version="1.0.0",
        pagination={"required": False, "strategy": "One zip per release", "page_count": 1, "termination_verified": True, "deduplication_keys": []},
    )
    record = artifact(raw, snapshot, "data", 1, True)
    record.update(media_type="application/zip", compression="zip", hash_scope="complete_file")
    receipt["artifacts"] = [record]
    receipt["quality_profile"].update(
        checks_passed=["nber_host_only", "planned_sha256_and_size", "fidelity_gate_passed", "release_date_rule", "mapped_members_present"],
        warnings=[
            "Acquisition checks only. Dataset schemas, measure periods and model eligibility remain unreviewed.",
            "NBER is a third-party mirror of CMS's release zips, not CMS itself; the March 2019 fidelity gate compared its copy with the CMS archive.",
            "Members outside the reviewed map are stored as unloaded references.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": contract.MODE,
            "route_id": release["route_id"],
            "scope": f"{release['release_label']}, original CMS zip from the NBER mirror",
            "release_id": release["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": release["expected_sha256"],
            "gate_sha256": plan["fidelity_gate"]["expected_sha256"],
            "code_sha256": contract.require_code(),
            "model_eligible": False,
        }
    )
    path = snapshot / "receipt.json"
    if path.exists():
        receipt = read_json(path)
    validate_receipt(receipt, validator, snapshot)
    contract.verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), snapshot, False)
    write_once(path, encoded_json(receipt))
    return path


def verify_local(path: Path) -> dict:
    """Verify the stored zip and receipt against the plan without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    source = next(s for s in load_registry(expected_sha256=lineage["registry_sha256"])["sources"] if s["source_id"] == contract.RECEIPT_SOURCE)
    contract.verify_capture(receipt, source, lineage, path.parent, False)
    return receipt


def selected_policy(client: AwsCli | None) -> dict:
    """The single-copy policy the storage writer selects: the client's, or the committed one when replaying without a client."""
    return client.storage_policy if client is not None else load_policy()


def verify_storage(path: Path, receipt: dict, release: dict, settings: dict, policy: dict) -> None:
    """Check exact local artifacts, S3 destinations under the writer's policy, the stored member set, manifests and immutable version evidence."""
    root = path.parent
    reconciliation = read_json(root / "s3_collections_reconciliation.json")
    registry = load_registry(expected_sha256=json.loads(receipt["lineage"]["extraction_or_query"])["registry_sha256"])
    route = load_routes(registry)[contract.RECEIPT_SOURCE]
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"] and all(reconciliation[k] == v for k, v in route.items()), "NBER storage route differs")

    def check_object(record: dict, local: Path, folder: str, role: str, manifest: bool = False) -> None:
        sha, size = fingerprint(local)
        single = policy["enabled"] and not manifest
        prefix = object_prefix(
            route, folder, "transport_audit" if single and role == "capture_receipt" else role, release["release_date"], receipt["snapshot_id"], False
        )
        allowed = {f"{prefix}/{sha}/{component(record['key'].rsplit('/', 1)[-1])}"}
        if single:
            scope = "evidence_objects" if prefix.split("/")[2] == "audit" else "objects"
            known = policy[scope].get(sha)
            allowed = {f"{'bronze_evidence' if scope == 'evidence_objects' else 'bronze_objects'}/sha256/{sha}"} | ({known["key"]} if known else set())
            require(known is None or record["key"] != known["key"] or record["version_id"] == known["version_id"], "NBER storage key differs")
        require(record["key"] in allowed and record["bucket"] == settings["data_bucket_name"], "NBER storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "NBER stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "NBER storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "NBER storage readback missing")

    expected = {tuple(m["member_chain"]): (m["sha256"], m["bytes"], m["role"]) for m in contract.archive_mapping(path, registry, release)["members"]}
    members = [e for e in reconciliation["objects"] if e.get("member_chain") and e["role"] in {"data", "reference"}]
    stored = {tuple(e["member_chain"]): (e["object"]["sha256"], e["object"]["byte_count"], e["role"]) for e in members}
    require(stored == expected, "NBER stored members differ from the captured zip")
    require(not any(e["storage_path"].endswith(".zip") for e in reconciliation["objects"]), "NBER zip was uploaded")
    data_folders = {e["dataset_id"] for e in members if e["role"] == "data"}
    require(data_folders == set(release["members"].values()), "NBER data datasets differ from the member map")
    for entry in reconciliation["objects"]:
        check_object(entry["object"], root / entry["storage_path"], entry["dataset_id"], entry["role"])
    manifests = reconciliation["manifests"]
    require({m["dataset_id"] for m in manifests} == data_folders and len(manifests) == len(data_folders), "NBER storage manifest set differs")
    for item in manifests:
        manifest_path = root / "s3_collections" / item["dataset_id"] / "manifest.json"
        manifest = read_json(manifest_path)
        require(manifest["snapshot_id"] == receipt["snapshot_id"] and manifest["source_id"] == contract.RECEIPT_SOURCE, "NBER manifest differs")
        require(manifest["objects"] == [e for e in reconciliation["objects"] if e["dataset_id"] == item["dataset_id"]], "NBER manifest differs")
        require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "NBER manifest hold differs")
        check_object(item["object"], manifest_path, item["dataset_id"], "capture_receipt", True)


def execute(
    plan: dict,
    release: dict,
    root: Path,
    allow_fetch: bool,
    upload: bool,
    client: AwsCli | None,
    outputs: dict,
    opener: Any = None,
    limits: Limits | None = None,
) -> dict:
    """Run the integrated capture/storage path for one release; a valid completion, or a run with neither fetch nor upload, performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and release in plan["releases"], "NBER selected plan or release differs")
    branch = root / "batches" / release["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        original, meta = fetch(
            release["url"],
            root / "downloads" / release["id"],
            contract.zip_name(release["folder"]),
            release["expected_sha256"],
            release["bytes"],
            "release",
            allow_fetch,
            opener,
            limits or LIMITS,
        )
        require(contract.release_date_from_zip(original) == release["release_date"], "NBER release date differs from the member modification rule")
        with zipfile.ZipFile(original) as archive:
            require(set(release["members"]) <= set(archive.namelist()), "NBER mapped member missing from the release zip")
        if not (allow_fetch or upload):
            return {
                "release_id": release["id"],
                "folder": release["folder"],
                "status": "download_verified",
                "model_eligible": False,
                "plan_sha256": canonical_hash(plan),
            }
        receipt_path = make_receipt(plan, release, original, meta, branch)
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["release_id"] == release["id"], "NBER selected release differs from receipt")
    done_path = branch / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["release_id"] == release["id"], "NBER completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "NBER completion storage evidence differs")
        require(done["model_eligible"] is False and done["status"] == "stored" and plan_matches(plan, done["plan_sha256"]), "NBER completion hold differs")
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, release, settings, selected_policy(client))
        return done
    result = {
        "release_id": release["id"],
        "folder": release["folder"],
        "status": "capture_ready",
        "model_eligible": False,
        "receipt_path": str(receipt_path),
        "receipt_sha256": fingerprint(receipt_path)[0],
        "plan_sha256": canonical_hash(plan),
    }
    if upload:
        contract.require_code()
        if client is None:
            raise ValueError("NBER storage client missing")
        registry = load_registry(expected_sha256=plan["registry_sha256"])
        mapping = contract.archive_mapping(receipt_path, registry, release)
        stored = upload_snapshot(receipt_path, [], client, outputs, registry, receipt_validator(), archive_mapping=mapping)
        verify_storage(receipt_path, receipt, release, client.configuration, client.storage_policy)
        result.update(
            status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
        )
        write_once(done_path, encoded_json(result))
    return result


def run_all(
    plan: dict, root: Path, allow_fetch: bool, upload: bool, client: AwsCli | None, outputs: dict, opener: Any = None, limits: Limits | None = None
) -> tuple[list[dict], list[dict]]:
    """Gate first, then every release independently; return completed results and held releases with their public reason."""
    results, held = [], []

    def reason_of(error: Exception) -> str:
        # require() messages are fixed public text; other exceptions are reported by type only.
        return str(error) if isinstance(error, ValueError) and str(error).startswith("NBER") else type(error).__name__

    with collection_lock(root) if allow_fetch or upload else nullcontext():
        if any(not (root / "batches" / r["id"] / "completed.json").exists() for r in plan["releases"]):
            try:
                check_gate(plan, root, allow_fetch, opener, limits or LIMITS)
            except (ValueError, OSError, KeyError) as error:
                return [], [{"folder": r["folder"], "release_id": r["id"], "reason": reason_of(error)} for r in plan["releases"]]
        for release in plan["releases"]:
            try:
                results.append(execute(plan, release, root, allow_fetch, upload, client, outputs, opener, limits))
            except (ValueError, OSError, KeyError) as error:
                held.append({"folder": release["folder"], "release_id": release["id"], "reason": reason_of(error)})
    return results, held


def main() -> None:
    """Validate offline by default; opt into NBER downloads and S3 writes independently."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    os.umask(0o077)
    if args.validate_receipt:
        receipt = verify_local(args.validate_receipt)
        sys.stdout.write(json.dumps({"status": "valid", "snapshot_id": receipt["snapshot_id"], "model_eligible": False}) + "\n")
        return
    plan = contract.load_plan()
    client, outputs = runtime() if args.execute else (None, {})
    results, held = run_all(plan, ROOT, args.fetch, args.execute, client, outputs)
    for result in results:
        LOGGER.info(json.dumps({k: result[k] for k in ("folder", "status")}, sort_keys=True))
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    LOGGER.info(
        json.dumps({"completed": len(results), "held": [h["folder"] for h in held], "pending_releases": [p["folder"] for p in plan["pending_releases"]]})
    )
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
