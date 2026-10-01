"""Offline full-pool planning; never downloads, uploads, clears holds or approves model use."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from collections import Counter
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.acquisition.batch import validate_batch
from scripts.acquisition.capture import prepare_capture, receipt_validator, validate_receipt
from scripts.acquisition.collection_layout import load_routes
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json
from scripts.acquisition.transport import CaptureError, Limits

RULES_PATH = REPO_ROOT / "config/acquisition/planning_rules.json"
FORMATS = {"csv", "txt", "zip", "xlsx", "xls", "pdf", "json", "html", "gzip", "docx"}
FAILURES = {
    "access_failed",
    "access_failed_or_empty_response",
    "HTTP_403_file_route_unverified",
    "HTTP_403_content_unverified",
    "advertised_prior_HTTP_403_not_retried",
    "access_hold_empty_HTTP_202",
    "http_200_error_page_not_data",
}


def strategies(registry: dict, rules: dict) -> dict[str, dict]:
    """Validate source-specific planning rules and return them indexed by source ID."""
    if rules.get("rules_version") != 1:
        raise CaptureError("Unsupported acquisition planning rules.")
    result = {}
    for group in rules["source_actions"]:
        for source_id in group["source_ids"]:
            if source_id in result or not group["history_action"] or not group["extraction_action"]:
                raise CaptureError("Source planning actions must be unique and nonempty.")
            result[source_id] = {key: group[key] for key in ("history_action", "extraction_action")}
    source_ids = {source["source_id"] for source in registry["sources"]}
    route_ids = {route["route_id"] for source in registry["sources"] for route in source["file_routes"]}
    if set(result) != source_ids or any(not set(rules[key]) <= source_ids for key in ("privacy_review_sources", "large_file_review_sources")):
        raise CaptureError("Planning rules must cover every source exactly, without unknown source exceptions.")
    if not set(rules["reference_route_ids"]) <= route_ids:
        raise CaptureError("Reference overrides contain unknown routes.")
    return result


def format_for(route: dict) -> str | None:
    """Return the supported capture format inferred from an approved route or None."""
    suffix = Path(unquote(urlsplit(route["url"]).path)).suffix.lower().removeprefix(".")
    if suffix in FORMATS:
        return suffix
    if suffix == "dat":
        return "txt"
    advertised = str(route.get("format") or "").lower()
    if advertised in FORMATS:
        return advertised
    if advertised.startswith("zip"):
        return "zip"
    return None


def role_for(source: dict, route: dict, rules: dict) -> str:
    """Return the planned artifact role under source-specific acquisition rules."""
    text = unquote(route["url"]) + " " + str(route.get("scope") or "")
    if re.search(r"dictionary|codebook|metadata|\bDD[_ .-]", text, re.I):
        return "dictionary"
    if source["preferred_route"] == "documentation_only" or route["route_id"] in rules["reference_route_ids"]:
        return "methodology"
    if route.get("status") in {"verified_documentation_only", "dictionary_rows_inspected", "documentation_only_export"}:
        return "methodology"
    return "data"


def export_selection(route: dict) -> dict:
    """Return the explicit selection metadata attached to a permitted export route."""
    parts = urlsplit(route["url"])
    if route["route_type"] != "permitted_export":
        return {}
    if parts.hostname == "data.chhs.ca.gov" and parts.path.startswith("/datastore/dump/"):
        return {"resource_id": parts.path.rsplit("/", 1)[-1], "scope": "all rows and columns", "query": parse_qs(parts.query)}
    if parts.hostname == "data.cdc.gov" and re.fullmatch(r"/api/views/[a-z0-9]+-[a-z0-9]+/rows.csv", parts.path):
        return {"dataset_id": parts.path.split("/")[3], "scope": "whole publisher release", "query": parse_qs(parts.query)}
    match = re.fullmatch(r"/hospital/([0-9]+)/exportAll", parts.path)
    if parts.hostname == "healthcarereportcard.illinois.gov" and match:
        return {"hospital_id": match.group(1), "scope": "all published export rows and periods; not statewide coverage"}
    return {}


def capture_plan(source: dict, route: dict, rules: dict) -> dict:
    """Return a capture plan retaining the source, route, period and governance constraints."""
    expected_format = format_for(route)
    filename = unquote(Path(urlsplit(route["url"]).path).name)
    if not filename or "." not in filename:
        filename = route["route_id"].replace(":", "_") + "." + str(expected_format)
    filename = re.sub(r"[^A-Za-z0-9._-]", "_", filename)[:190]
    if not filename[0].isalnum():
        filename = "source_" + filename
    label = route.get("vintage_or_release")
    label = label if isinstance(label, str) else json.dumps(label, sort_keys=True) if label is not None else None
    release_date = None
    # Only exact CMS hospital snapshot labels are established release dates in this register.
    if "/dataset-archives/theme/hospitals/" in route["url"] and label and re.fullmatch(r"\d{4}-\d{2}-\d{2}", label):
        release_date = date.fromisoformat(label).isoformat()
    history = source["measurement_history"]
    return {
        "source_id": source["source_id"],
        "mode": "file",
        "route_id": route["route_id"],
        "expected_format": expected_format,
        "file_name": filename,
        "role": role_for(source, route, rules),
        "publisher": urlsplit(source["official_landing_url"]).hostname,
        "scope": "Complete original response for the selected published file/export; no row truncation. Prior inspected scope: "
        + str(route.get("scope") or "not established")
        + ". All other table, period, privacy and measure checks remain pending.",
        "release": {
            "publisher_release_label": label,
            "release_date": release_date,
            "dataset_version": None,
            "publisher_updated_at": None,
            "revision_status": "unknown",
            "advertised_history": json.dumps(history.get("advertised"), sort_keys=True),
            "observed_history": json.dumps(history.get("observed"), sort_keys=True),
        },
        "measurement_periods": [
            {
                "label": "Preserved prior evidence, not a common interval: " + json.dumps(route.get("measurement_period")),
                "start_date": None,
                "end_date": None,
                "period_type": "unknown",
                "source_basis": "unknown",
                "target_alignment": "unknown",
            }
        ],
        "governance": {
            "access_class": "public",
            "license_or_terms_url": None,
            "credentials_required": False,
            "credential_reference": None,
            "contains_phi": False,
            "contains_pii": False,
            "use_restrictions": [
                source["validation"]["reviewed_access_terms"],
                "Public aggregate institutional/geographic data or documentation only; no patient records intended.",
                "This is a scope assessment, not a completed privacy-field audit. Stop on unexpected personal records.",
                "Preserve all source/measure holds, suppression and redistribution restrictions; no model approval.",
            ],
        },
        "export_selections": export_selection(route),
    }


def route_plan(source: dict, route: dict, rules: dict, stored: list[dict]) -> dict:
    """Return the route's acquisition decision with stored evidence and unresolved holds."""
    kind, role = format_for(route), role_for(source, route, rules)
    blockers = []
    if source["preferred_route"] == "access_hold":
        blockers.append("source_access_hold")
    if route["route_type"] == "file_index":
        blockers.append("discovery_index_not_an_artifact")
    if route.get("status") in FAILURES:
        blockers.append("publisher_access_unresolved")
    if kind is None:
        blockers.append("unimplemented_or_ambiguous_format")
    if kind == "html":
        blockers.append("html_requires_document_or_report_specific_capture")
    if route["route_type"] == "permitted_export" and not export_selection(route):
        blockers.append("interactive_export_selections_and_artifact_required")
    if source["source_id"] in rules["privacy_review_sources"] and role == "data":
        blockers.append("privacy_and_terms_review_before_capture")
    if source["source_id"] in rules["large_file_review_sources"] and role == "data":
        blockers.append("complete_size_and_large_file_strategy_required")
    matches = [item for item in stored if item["requested_url"] == route["url"]]
    direct = [item for item in matches if item["source_id"] == source["source_id"]]
    if direct:
        blockers.append("already_stored_capture_do_not_redownload")
    elif matches:
        blockers.append("bundled_bytes_present_source_scope_reconciliation_required")
    extraction: dict = {"mode": "keep_original_file", "table_mapping": "source_identity", "readiness": "supported_without_transformation"}
    if kind == "zip":
        extraction = {
            "mode": "bounded_crc_checked_member_extraction",
            "table_mapping": "publisher_manifest_or_reviewed_member_map",
            "readiness": "member_inventory_required",
            "nested_archives": "hold_until_recursive_member_plan_tested",
            "max_expanded_bytes": 1024**3,
            "max_members": 10000,
            "upload_zip": False,
        }
    elif kind in {"pdf", "docx", "xlsx", "xls"}:
        extraction["numeric_extraction"] = "deferred_to_post_acquisition_review; retain document/workbook intact"
    if kind == "zip" and not matches:
        blockers.append("archive_member_mapping_required_before_batch_upload")
    capture_blocked = any(reason != "archive_member_mapping_required_before_batch_upload" for reason in blockers)
    plan = None if capture_blocked else capture_plan(source, route, rules)
    return {
        "source_id": source["source_id"],
        "route_id": route["route_id"],
        "url": route["url"],
        "route_type": route["route_type"],
        "prior_evidence_status": route.get("status"),
        "route_sha256": canonical_hash(route),
        "role": role,
        "prior_release_evidence": route.get("vintage_or_release"),
        "prior_measurement_evidence": route.get("measurement_period"),
        "status": "ready_for_bounded_batch" if not blockers else "capture_only_pending_member_map" if plan else "blocked_or_discovery",
        "blockers": blockers,
        "capture_plan": plan,
        "extraction": extraction,
        "stored_matches": [{key: item[key] for key in ("snapshot_id", "source_id", "release", "status")} for item in matches],
        "remaining_checks": route["remaining_checks"],
        "live_route_rechecked": False,
        "model_eligible": False,
    }


def api_plan(source: dict, rules: dict) -> dict:
    """Return a source-specific API fallback decision under the planning rules."""
    result = {"approved_scope": source["api_fallback"], "execution_ready": False, "capture_plan": None}
    if source["source_id"] == "IL" and source["preferred_route"] == "file_plus_api_fallback" and source["api_fallback"]["needed"] is True:
        plan = capture_plan(source, source["file_routes"][0], rules)
        plan.update(
            mode="il_directory",
            expected_format="json",
            file_name="page_0001.json",
            role="api_page",
            export_selections={},
            scope="Complete published hospital directory for identifiers only; numeric histories remain file-first.",
            fallback_reason=source["api_fallback"]["reason"],
        )
        plan.pop("route_id")
        result.update(execution_ready=True, capture_plan=plan)
    return result


def coordinate_shared_routes(plans: list[dict]) -> None:
    """Annotate shared file routes with explicit reuse relationships in the supplied plans."""
    primary: dict[str, str] = {}
    evidence_rank = {"partial_response_not_parseable": 2, "advertised_not_downloaded": 1}
    for plan in sorted(plans, key=lambda item: evidence_rank.get(item["prior_evidence_status"], 0)):
        if plan["status"] != "ready_for_bounded_batch":
            continue
        key = plan["url"]
        if key in primary:
            plan.update(status="blocked_or_discovery", shared_primary_route_id=primary[key], capture_plan=None)
            plan["blockers"].append("shared_url_follow_primary_capture_then_verify_scope_and_hash")
        else:
            primary[key] = plan["route_id"]


def current_snapshot(receipt_path: Path, verification_path: Path, registry: dict, validator: Any) -> dict:
    """Verify saved capture and storage evidence and return its prior-checkpoint summary."""
    receipt, root = read_json(receipt_path), receipt_path.parent
    validate_receipt(receipt, validator, root)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    reconciliation_path = root / "s3_collections_reconciliation.json"
    reconciliation, verification = read_json(reconciliation_path), read_json(verification_path)
    if lineage["registry_sha256"] != canonical_hash(registry) or reconciliation["snapshot_id"] != receipt["snapshot_id"]:
        raise CaptureError("Stored capture does not match the locked registry and snapshot.")
    expected = {entry["object"]["key"] for entry in reconciliation["objects"] + reconciliation["manifests"]}
    collection = reconciliation["publisher"] + "/" + reconciliation["collection"]
    if (
        reconciliation.get("storage_contract_version") != "4.0.0"
        or verification.get("status") != "collection_inventory_verified"
        or verification["dataset_objects"] != len(expected)
        or verification["dataset_count"] != len(reconciliation["manifests"])
        or verification["collection_root"] != collection
    ):
        raise CaptureError("Current-layout inventory evidence does not reconcile with stored manifests.")
    checked, references = set(), {}
    for entry in reconciliation["objects"]:
        path = root / entry["storage_path"]
        if path not in checked:
            if not path.resolve().is_relative_to(root.resolve()) or fingerprint(path) != (entry["object"]["sha256"], entry["object"]["byte_count"]):
                raise CaptureError("Local stored-capture evidence changed.")
            checked.add(path)
        if entry["object"]["key"].split("/")[2] == "references":
            references[entry["object"]["key"]] = {
                "storage_path": entry["storage_path"],
                "reference_kind": entry.get("reference_kind"),
                "key": entry["object"]["key"],
                "sha256": entry["object"]["sha256"],
                "applicability": "release_and_section_mapping_pending",
            }
    return {
        "source_id": receipt["source"]["source_record_id"],
        "snapshot_id": receipt["snapshot_id"],
        "requested_url": receipt["acquisition"]["requested_url"],
        "release": receipt["release"],
        "status": "stored_unvalidated_at_prior_verified_checkpoint",
        "receipt_sha256": fingerprint(receipt_path)[0],
        "active_reconciliation": {"path": str(reconciliation_path), "sha256": fingerprint(reconciliation_path)[0]},
        "verification": {"path": str(verification_path), "sha256": fingerprint(verification_path)[0], "checked_at_utc": verification["checked_at_utc"]},
        "current_objects_at_checkpoint": len(expected),
        "held_members": verification["held_members"],
        "shared_references": list(references.values()),
        "collection_root": collection,
        "live_s3_rechecked": False,
        "local_files_rechecked": len(checked),
        "history_complete": False,
    }


def build_plans(registry: dict, inventory: dict, rules: dict, routes: dict, stored: list[dict], validator: Any, archive_review: dict | None = None) -> tuple:
    """Return the acquisition register and executable batch while carrying unresolved source holds."""
    actions = strategies(registry, rules)
    if inventory["registry_sha256"] != canonical_hash(registry) or set(routes) != set(actions):
        raise CaptureError("Inventory, layout and source registry must agree.")
    expected_routes = {item["route_id"] for source in registry["sources"] for item in source["file_routes"]}
    if {source["source_id"] for source in inventory["sources"]} != set(actions) or {item["route_id"] for item in inventory["known_routes"]} != expected_routes:
        raise CaptureError("The full source/route pool must be preserved in the inventory.")
    source_plans: list[dict] = []
    all_routes: list[dict] = []
    jobs: list[dict] = []
    for source in registry["sources"]:
        all_routes.extend(route_plan(source, route, rules, stored) for route in source["file_routes"])
    if archive_review is not None:
        if archive_review["registry_sha256"] != canonical_hash(registry):
            raise CaptureError("Archive member review belongs to a different source registry.")
        for plan in all_routes:
            matches = [item for item in archive_review["archives"] if plan["route_id"] in item["route_ids"] and item["url"] == plan["url"]]
            if matches:
                plan["extraction"]["local_member_evidence"] = matches
                plan["extraction"]["local_members_verified"] = True
                if len(matches) == 1 and plan["capture_plan"] is not None:
                    plan["capture_plan"]["expected_sha256"] = matches[0]["archive_sha256"]
    coordinate_shared_routes(all_routes)
    for source in registry["sources"]:
        source_id, layout = source["source_id"], routes[source["source_id"]]
        plans = [plan for plan in all_routes if plan["source_id"] == source_id]
        for plan in plans:
            if plan["capture_plan"]:
                prepare_capture(plan["capture_plan"], registry, validator)
            if plan["status"] == "ready_for_bounded_batch":
                jobs.append({"job_id": plan["route_id"].replace(":", "_"), "plan": plan["capture_plan"], "references": [], "limits": asdict(Limits())})
        fallback = api_plan(source, rules)
        if fallback["capture_plan"]:
            prepare_capture(fallback["capture_plan"], registry, validator)
            jobs.append({"job_id": source_id + "_directory", "plan": fallback["capture_plan"], "references": [], "limits": asdict(Limits())})
        privacy = rules["privacy_review_reason"] if source_id in rules["privacy_review_sources"] else "scope_assessed_not_field_audited"
        source_plans.append(
            {
                "source_id": source_id,
                "title": source["title"],
                "wave": source["planning"]["wave"],
                **layout,
                **actions[source_id],
                "preferred_route": source["preferred_route"],
                "access_hold_retained": source["preferred_route"] == "access_hold",
                "history_evidence": source["measurement_history"],
                "history_complete": False,
                "required_checks": source["required_checks"],
                "linked_measure_ids": source["linked_measure_ids"],
                "source_specific_gate": source["validation"]["source_specific_gate"],
                "privacy_review": privacy,
                "large_file_review": rules["large_file_review_reason"] if source_id in rules["large_file_review_sources"] else None,
                "documentation_route_ids": [plan["route_id"] for plan in plans if plan["role"] != "data"],
                "dictionary_coverage": "pending_release_and_section_mapping",
                "api_fallback": {**fallback, "remaining_action": actions[source_id]["history_action"]},
                "route_ids": [plan["route_id"] for plan in plans],
                "route_status_counts": dict(Counter(plan["status"] for plan in plans)),
            }
        )
    batch = {"batch_version": 1, "registry_sha256": canonical_hash(registry), "jobs": jobs}
    if jobs:
        validate_batch(batch, registry, validator)
    register = {
        "plan_version": 1,
        "registry_sha256": canonical_hash(registry),
        "rules_sha256": canonical_hash(rules),
        "archive_review_sha256": canonical_hash(archive_review) if archive_review is not None else None,
        "execution_authorized_by_plan": False,
        "sources": source_plans,
        "routes": all_routes,
        "discovery_candidates": inventory["discovery_candidates"],
        "stored_snapshots": stored,
        "counts": {
            "sources": len(source_plans),
            "known_routes": len(all_routes),
            "batch_jobs": len(jobs),
            "file_capture_plans": sum(plan["capture_plan"] is not None for plan in all_routes),
            "api_capture_plans": sum(source["api_fallback"]["execution_ready"] for source in source_plans),
            "access_holds": sum(source["access_hold_retained"] for source in source_plans),
            "route_statuses": dict(Counter(plan["status"] for plan in all_routes)),
            "blocker_counts_nonexclusive": dict(Counter(reason for plan in all_routes for reason in plan["blockers"])),
        },
        "limitations": [
            "Plans use preserved publisher evidence; no live route checks, AWS calls or new downloads were performed.",
            "Batch jobs are bounded capture candidates, not proof of current accessibility, completeness or model eligibility.",
            "Capture-only ZIP plans must not enter the upload batch until member mapping is verified.",
            "Discovery candidates are not registered capture routes. No history is declared complete.",
        ],
    }
    for source in source_plans:
        peers = [peer for peer in source_plans if (peer["publisher"], peer["collection"]) == (source["publisher"], source["collection"])]
        source["collection_reference_route_ids"] = [route_id for peer in peers for route_id in peer["documentation_route_ids"]]
        source["stored_reference_candidates"] = [
            reference
            for item in stored
            if item["collection_root"] == source["publisher"] + "/" + source["collection"]
            for reference in item["shared_references"]
        ]
    refreshed = copy.deepcopy(inventory)
    refreshed.update(
        inventory_version=2,
        previous_inventory_sha256=canonical_hash(inventory),
        stored_snapshots=stored,
        superseded_storage_evidence=inventory["stored_snapshots"],
        plan_register_sha256=canonical_hash(register),
        active_storage_contract="4.0.0",
    )
    by_id = {plan["route_id"]: plan for plan in all_routes}
    for item in refreshed["known_routes"]:
        item.update(prior_inventory_status=item["status"], status=by_id[item["route_id"]]["status"], blockers=by_id[item["route_id"]]["blockers"])
    by_source = {source["source_id"]: source for source in source_plans}
    for item in refreshed["sources"]:
        source = by_source[item["source_id"]]
        item.update(
            collection_root=source["publisher"] + "/" + source["collection"],
            next_action=source["history_action"],
            route_status_counts=source["route_status_counts"],
            history_complete=False,
        )
        item.setdefault("documentation", {}).update(
            collection_reference_route_ids=source["collection_reference_route_ids"],
            stored_reference_candidates=source["stored_reference_candidates"],
            status="release_and_section_mapping_pending",
        )
    refreshed["counts"].update(stored_snapshots=len(stored), route_statuses=register["counts"]["route_statuses"])
    return refreshed, register, batch


def checklist(register: dict) -> str:
    """Render the register's acquisition decisions as a human-readable checklist."""
    counts = register["counts"]
    rows = [
        "# Full-Pool Acquisition Checklist",
        "",
        "Planning only. No downloads or AWS calls. History and dictionary coverage remain open.",
        "",
        "Ready counts mean offline-valid bounded capture plans, not current access verification or model approval.",
        "",
        f"Batch: {counts['route_statuses'].get('ready_for_bounded_batch', 0)} file jobs and {counts['api_capture_plans']} identifier-directory API job(s).",
        "The table counts file routes only. Held/discovery includes shared aliases and already-stored captures, not just access failures.",
        "",
        "| Source | Collection | Ready batch routes | Capture-only ZIP routes | Held/discovery routes | Source access hold |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for source in register["sources"]:
        counts = source["route_status_counts"]
        rows.append(
            f"| {source['source_id']} | {source['publisher']}/{source['collection']} | {counts.get('ready_for_bounded_batch', 0)} | "
            f"{counts.get('capture_only_pending_member_map', 0)} | {counts.get('blocked_or_discovery', 0)} | "
            f"{'yes' if source['access_hold_retained'] else 'no'} |"
        )
    for source in register["sources"]:
        rows.extend(
            [
                "",
                f"## {source['source_id']}: {source['title']}",
                "",
                f"History: {source['history_action']}",
                "",
                f"Extraction: {source['extraction_action']}",
                "",
                "Dictionary coverage: pending release and section mapping.",
            ]
        )
    return "\n".join(rows) + "\n"


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--verification", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--checklist", required=True, type=Path)
    parser.add_argument("--archive-review", type=Path)
    args = parser.parse_args()
    try:
        if args.checklist.resolve().is_relative_to(REPO_ROOT.resolve()):
            raise CaptureError("The review checklist must remain outside the project repository.")
        registry, validator = load_registry(), receipt_validator()
        pinned_inventory = read_json(args.inventory)
        if pinned_inventory["registry_sha256"] != canonical_hash(registry):
            registry = load_registry(expected_sha256=pinned_inventory["registry_sha256"])
        stored = [current_snapshot(args.receipt, args.verification, registry, validator)]
        archive_review = read_json(args.archive_review) if args.archive_review else None
        inventory, register, batch = build_plans(
            registry, read_json(args.inventory), read_json(RULES_PATH), load_routes(registry), stored, validator, archive_review
        )
        for name, payload in (("historical_inventory_v2.json", inventory), ("acquisition_plans_v1.json", register), ("bounded_batch_v1.json", batch)):
            write_once(args.output_directory / name, encoded_json(payload))
        write_once(args.checklist, checklist(register).encode())
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    sys.stdout.write(str(json.dumps(register["counts"], indent=2)) + "\n")


if __name__ == "__main__":
    main()
