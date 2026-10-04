"""Freeze the bounded redownload queue offline: one disposition per inventory entry, no network or AWS.

Usage (repository root): .venv/bin/python -m scripts.acquisition.one_off.redownload.build_queue_run1
Writes queue.json and queue.lock.json to data/redownload_checks/20260929/; refuses to overwrite a different queue.
"""

import collections
import glob
import hashlib
import json
import re
import sys
from pathlib import Path

from scripts.acquisition.data_paths import current

ROOT = Path(__file__).resolve().parents[4]
PLAN = ROOT / "data/acquisition_planning/full_redownload_20260929"
OUT = ROOT / "data/redownload_checks/20260929"
SNAPSHOT = re.compile(r"([A-Za-z0-9_.-]+?__\d{8}T\d{6}Z__[0-9a-f]{32})")
# Sources the user dropped (licensing rule and measure decisions); never reacquired.
DROPPED_SOURCES = {"AHRF", "S26_CLH", "AHA", "LEAP", "HASC", "NDNQI", "APIC", "NSSRN"}
DROPPED_URL_MARKERS = ("/NSSRN/",)
MANUAL_MODES = {"hud_xlsx": "HUD crosswalk files site", "wonder_export": "CDC WONDER query form"}
PRIVATE_MODES = {"cms_owners_org", "hcai_util_workbook", "onc_mu_hospital"}
API_MODES = {"bls_api_v2", "census_acs_api", "census_acs_detailed", "mmd_api", "hud_api", "il_directory"}
HTTP_EXPORT_SOURCES = {"IL", "CA", "PLACES", "HCAI_FINANCE"}
CURRENT_ONLY = {"HPSA", "MUA"}
# Privacy review finding F2 (Sep 27 2026): the CMS hospitals archives hold a table of named state contacts.
CONTACT_TABLE_SOURCES = {"main-hai-pdc"}
# The MMD browser exports moved to the API after parity checks on these two days (the flag value stays as recorded).
MMD_PARITY = "parity_verified_20260925_20260926"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def retired_snapshots() -> collections.Counter:
    counts: collections.Counter = collections.Counter()
    for name in sorted(glob.glob(str(ROOT / "data/privacy_review/*/retired_objects*.json"))):
        for item in json.loads(Path(name).read_text())["objects"]:
            match = SNAPSHOT.search(item.get("key", ""))
            if match:
                counts[match.group(1).split("/")[-1]] += 1
    return counts


def main() -> None:
    inventory = json.loads((PLAN / "capture_inventory.json").read_text())
    amendment = json.loads((PLAN / "capture_inventory_amendment1.json").read_text())
    failed = {item["snapshot_id"] for item in amendment["reclassified_candidates"]}
    retired = retired_snapshots()
    entries = []
    for candidate in inventory["candidates"] + amendment["added_candidates"]:
        # Receipts recorded before the dataset move (Oct 4 2026) are read through the path map; the queue keeps the recorded path.
        path = ROOT / current(candidate["receipt"])
        if sha(path) != candidate["receipt_sha256"]:
            sys.exit(f"Receipt changed since the inventory: {candidate['receipt']}")
        receipt = json.loads(path.read_text())
        acquisition = receipt["acquisition"]
        lineage = json.loads(receipt["lineage"].get("extraction_or_query") or "{}")
        entries.append(
            {
                "snapshot_id": candidate["snapshot_id"],
                "source_id": candidate["source_id"],
                "receipt": candidate["receipt"],
                "receipt_sha256": candidate["receipt_sha256"],
                "status": receipt["snapshot_status"],
                "transport": acquisition.get("transport_mode"),
                "mode": lineage.get("mode"),
                "url": acquisition.get("requested_url"),
                "identity": canonical(
                    [
                        acquisition.get("requested_url"),
                        acquisition.get("request_method"),
                        acquisition.get("request_parameters"),
                        acquisition.get("export_selections"),
                    ]
                ),
                "bytes": sum(item.get("byte_count") or 0 for item in receipt.get("artifacts", [])),
                "pages": (acquisition.get("pagination") or {}).get("page_count") or 1,
                "retrieved_at_utc": acquisition.get("retrieved_at_utc"),
                "retired_versions": retired.get(candidate["snapshot_id"], 0),
            }
        )
    seen: dict[str, str] = {}
    queue = []
    for entry in sorted(entries, key=lambda item: (item["retrieved_at_utc"] or "", item["snapshot_id"])):
        flags = []
        if entry["status"] != "acquired_unvalidated" or entry["snapshot_id"] in failed:
            disposition, route = "failed_attempt_excluded", None
        elif entry["source_id"] in DROPPED_SOURCES or any(marker in (entry["url"] or "") for marker in DROPPED_URL_MARKERS):
            disposition, route = "dropped", None
        elif entry["mode"] == "acs_derived":
            disposition, route = "derived_replay_only", None
        elif entry["identity"] in seen:
            disposition, route = "superseded_duplicate", None
            flags.append(f"same_request_as:{seen[entry['identity']]}")
        else:
            seen[entry["identity"]] = entry["snapshot_id"]
            if entry["mode"] in MANUAL_MODES or (entry["source_id"] == "ACS" and entry["transport"] == "web_export"):
                disposition, route = "approved_manual", "manual_browser_download"
            elif entry["mode"] in API_MODES:
                disposition, route = "api", "publisher_api"
            elif entry["source_id"] == "MMD" and entry["transport"] == "web_export":
                disposition, route = "api", "publisher_api"
                flags.append(f"browser_export_moved_to_api:{MMD_PARITY}")
            else:
                disposition, route = "direct_download", "publisher_url"
                if entry["transport"] == "web_export" and entry["source_id"] in HTTP_EXPORT_SOURCES:
                    flags.append("http_export_route_needs_pilot")
            if entry["mode"] in PRIVATE_MODES or entry["retired_versions"] or entry["source_id"] in CONTACT_TABLE_SOURCES:
                flags.append("original_contains_personal_details:private_owner_only:redacted_derivative_only")
            if entry["source_id"] in CURRENT_ONLY:
                flags.append("current_only:newly_dated_snapshot_never_backfilled")
            if entry["source_id"] == "NY":
                flags.append("publisher_returned_http_403_before:may_end_unavailable")
        queue.append(
            {
                **{key: entry[key] for key in ("snapshot_id", "source_id", "receipt", "receipt_sha256", "url", "mode", "bytes", "pages")},
                "disposition": disposition,
                "route": route,
                "flags": flags,
            }
        )
    unresolved = [
        item["snapshot_id"]
        for item in queue
        if item["disposition"]
        not in {"api", "direct_download", "approved_manual", "derived_replay_only", "superseded_duplicate", "dropped", "failed_attempt_excluded"}
    ]
    if unresolved or len(queue) != amendment["candidate_count_after"]:
        sys.exit(f"Unresolved or miscounted queue: {len(unresolved)} unresolved, {len(queue)} entries")
    active = [item for item in queue if item["route"]]
    by_route: collections.Counter = collections.Counter(item["route"] for item in active)
    bls = sum(1 for item in active if item["source_id"] == "BLS")
    inputs = {
        "capture_inventory.json": sha(PLAN / "capture_inventory.json"),
        "capture_inventory_amendment1.json": sha(PLAN / "capture_inventory_amendment1.json"),
        "plan.md": sha(PLAN / "plan.md"),
        "config/acquisition/source_registry.json": sha(ROOT / "config/acquisition/source_registry.json"),
        "config/acquisition/source_registry_lock.json": sha(ROOT / "config/acquisition/source_registry_lock.json"),
        "config/acquisition/registry_additions.json": sha(ROOT / "config/acquisition/registry_additions.json"),
        "config/acquisition/registry_versions.json": sha(ROOT / "config/acquisition/registry_versions.json"),
    }
    for name in sorted(glob.glob(str(ROOT / "data/acquisition_planning/measure_decisions_2026092*.json"))):
        inputs[str(Path(name).relative_to(ROOT))] = sha(Path(name))
    document = {
        "kind": "bounded_redownload_queue",
        "execution_status": "frozen_offline_no_requests_made",
        "model_eligible": False,
        "entries": len(queue),
        "dispositions": dict(sorted(collections.Counter(item["disposition"] for item in queue).items())),
        "budgets": {
            "acquisition_units": len(active),
            "units_by_route": dict(sorted(by_route.items())),
            "max_primary_attempts": 2 * len(active),
            "pages_recorded": sum(item["pages"] for item in active),
            "recorded_bytes": sum(item["bytes"] for item in active),
            "byte_cap": 128 * 2**30,
            "bls_calls": bls,
            "bls_rolling_24h_limit": 450,
            "bls_minimum_windows": -(-bls // 450),
        },
        "inputs_sha256": inputs,
        "queue": queue,
    }
    body = (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()
    target = OUT / "queue.json"
    if target.exists() and target.read_bytes() != body:
        sys.exit("A different frozen queue already exists; do not overwrite it.")
    target.write_bytes(body)
    (OUT / "queue.lock.json").write_text(json.dumps({"queue_sha256": hashlib.sha256(body).hexdigest(), "entries": len(queue)}, indent=1) + "\n")
    sys.stdout.write(json.dumps({key: document[key] for key in ("entries", "dispositions", "budgets")}, indent=1) + "\n")


if __name__ == "__main__":
    main()
