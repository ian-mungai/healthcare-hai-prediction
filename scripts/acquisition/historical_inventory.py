"""Preserve every source, known release and unresolved historical-discovery task."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json
from scripts.acquisition.transport import CaptureError


def cms_archive_candidates(payload: dict, sources: list[dict]) -> list[dict]:
    """Return historical archive candidates advertised by CMS for the supplied sources."""
    rows, meta = payload.get("data"), payload.get("meta", {})
    if not isinstance(rows, list) or meta.get("total_pages") != 1 or meta.get("current_page") != 1 or meta.get("total_items") != len(rows):
        raise CaptureError("CMS archive inventory pagination is incomplete; retain it as evidence only.")
    clinical = [source["source_id"] for source in sources if any("/dataset-archives/theme/hospitals/" in route["url"] for route in source["file_routes"])]
    found = []
    ids: set[str] = set()
    for row in rows:
        url = urljoin("https://data.cms.gov", row["url"])
        if row["id"] in ids or row.get("theme") != "hospitals" or row.get("access_level") != "public":
            raise CaptureError("CMS archive listing has duplicate identities or unexpected scope.")
        ids.add(row["id"])
        if urlsplit(url).hostname != "data.cms.gov" or not urlsplit(url).path.startswith("/provider-data/sites/default/files/dataset-archives/"):
            raise CaptureError("CMS archive link left its publisher's archive path.")
        found.append(
            {
                "url": url,
                "publisher_release_label": row["name"],
                "release_date_label": row["date"],
                "archive_kind": row["type"],
                "advertised_bytes": row.get("size"),
                "source_ids": clinical,
                "evidence_kind": "publisher_archive_index",
                "measurement_period": None,
                "status": "candidate_requires_source_member_and_route_review",
                "reuse_condition": "Annual bundles may duplicate snapshots; reconcile members and hashes before counting coverage.",
            }
        )
    return found


def catalog_candidates(catalogs: dict, sources: list[dict]) -> list[dict]:
    """Return source-bound historical candidates from preserved publisher catalogs."""
    candidates = []
    for name, catalog in catalogs.items():
        landing = catalog.get("landingPage")
        matching = []
        for source in sources:
            advertised = source["measurement_history"].get("advertised", {})
            catalog_names = advertised.get("catalog", {}) if isinstance(advertised, dict) else {}
            if source["official_landing_url"] == landing or name in catalog_names:
                matching.append(source["source_id"])
        for distribution in catalog.get("distribution", []):
            url = distribution.get("downloadURL") or distribution.get("accessURL")
            if isinstance(url, str):
                candidates.append(
                    {
                        "url": url,
                        "source_ids": matching,
                        "catalog_name": name,
                        "publisher_release_label": distribution.get("title"),
                        "format": distribution.get("format"),
                        "measurement_period": None,
                        "catalog_temporal": catalog.get("temporal"),
                        "distribution_temporal": distribution.get("temporal"),
                        "resources_metadata_url": distribution.get("resourcesAPI"),
                        "numeric_api_fallback_authorized": False,
                        "evidence_kind": "prior_publisher_catalog",
                        "status": "candidate_requires_source_member_and_route_review",
                    }
                )
        for url in [catalog.get("describedBy"), *catalog.get("references", [])]:
            if isinstance(url, str) and url.startswith(("http://", "https://")):
                candidates.append(
                    {
                        "url": url,
                        "source_ids": matching,
                        "catalog_name": name,
                        "evidence_kind": "prior_publisher_documentation",
                        "measurement_period": None,
                        "status": "reference_candidate_requires_vintage_review",
                    }
                )
    return candidates


def embedded_candidates(value: object, source_id: str) -> list[dict]:
    """Recursively collect historical file candidates embedded in source metadata."""
    found = []
    if isinstance(value, dict):
        if isinstance(value.get("url"), str) and value["url"].startswith(("http://", "https://")):
            found.append(
                {
                    "url": value["url"],
                    "source_ids": [source_id],
                    "publisher_release_label": value.get("name"),
                    "format": value.get("format"),
                    "measurement_period": None,
                    "evidence_kind": "preserved_source_history_metadata",
                    "status": "candidate_requires_source_member_and_route_review",
                }
            )
        for child in value.values():
            found.extend(embedded_candidates(child, source_id))
    elif isinstance(value, list):
        for child in value:
            found.extend(embedded_candidates(child, source_id))
    return found


def build_inventory(registry: dict, archive: dict | None = None, catalogs: dict | None = None, receipts: list[Path] | None = None) -> dict:
    """Return a historical-source inventory from registry, catalog and saved receipt evidence."""
    routes: list[dict] = []
    sources: list[dict] = []
    by_url: dict[str, list[str]] = defaultdict(list)
    candidates = cms_archive_candidates(archive, registry["sources"]) if archive is not None else []
    candidates.extend(catalog_candidates(catalogs or {}, registry["sources"]))
    acquired = []
    archive_documents: dict[str, list[dict]] = defaultdict(list)
    for path in receipts or []:
        receipt = read_json(path)
        names = ("s3_reconciliation.json", "s3_members_reconciliation.json", "s3_datasets_reconciliation.json", "s3_collections_reconciliation.json")
        reconciliations = [path.with_name(name) for name in names if path.with_name(name).is_file()]
        if reconciliations:
            acquired.append(
                {
                    "source_id": receipt["source"]["source_record_id"],
                    "snapshot_id": receipt["snapshot_id"],
                    "requested_url": receipt["acquisition"]["requested_url"],
                    "release": receipt["release"],
                    "status": "stored_unvalidated_at_prior_verified_checkpoint",
                    "receipt_sha256": fingerprint(path)[0],
                    "reconciliations": [{"file_name": item.name, "sha256": fingerprint(item)[0]} for item in reconciliations],
                    "live_s3_rechecked": False,
                }
            )
        for inventory_path in path.parent.glob("expanded/*/archive_inventory.json"):
            inventory = read_json(inventory_path)
            for name in inventory["dictionary_candidates"]:
                archive_documents[receipt["acquisition"]["requested_url"]].append(
                    {
                        "snapshot_id": receipt["snapshot_id"],
                        "archive_sha256": inventory["archive_sha256"],
                        "dictionary_member": name.removeprefix("members/"),
                        "mapping_status": "source_member_and_dictionary_coverage_review_pending",
                    }
                )
    for source in registry["sources"]:
        source_id, hold = source["source_id"], source["preferred_route"] == "access_hold"
        docs = [entry for route in source["file_routes"] for entry in archive_documents.get(route["url"], [])]
        reference_routes = [
            route["route_id"]
            for route in source["file_routes"]
            if source["preferred_route"] == "documentation_only"
            or re.search(r"dictionary|codebook|layout|methodology|definitions", str(route.get("scope", "")) + route["url"], re.IGNORECASE)
        ]
        candidates.extend(embedded_candidates(source["measurement_history"], source_id))
        for route in source["file_routes"]:
            status = "access_hold" if hold else "capture_plan_required"
            if not hold and route["route_type"] == "file_index":
                status = "discovery_index_not_data"
            elif not hold and route["route_type"] == "permitted_export":
                status = "explicit_export_selections_required"
            elif not hold and route.get("status") in {"access_failed", "HTTP_403_file_route_unverified"}:
                status = "route_access_unresolved"
            record = {
                "source_id": source_id,
                "route_id": route["route_id"],
                "url": route["url"],
                "route_type": route["route_type"],
                "advertised_format": route.get("format"),
                "release_label": route.get("vintage_or_release"),
                "scope": route.get("scope"),
                "measurement_period_evidence": route.get("measurement_period"),
                "prior_route_status": route.get("status"),
                "remaining_checks": route.get("remaining_checks"),
                "status": status,
                "research_reference": route.get("research_reference"),
            }
            routes.append(record)
            by_url[route["url"]].append(route["route_id"])
        sources.append(
            {
                "source_id": source_id,
                "title": source["title"],
                "preferred_route": source["preferred_route"],
                "wave": source["planning"]["wave"],
                "official_landing_url": source["official_landing_url"],
                "known_route_count": len(source["file_routes"]),
                "history_evidence": source["measurement_history"],
                "api_fallback": source["api_fallback"],
                "required_checks_sha256": canonical_hash(source["required_checks"]),
                "required_checks_reference": f"source_registry.json#/sources/{len(sources)}/required_checks",
                "linked_measure_ids": source["linked_measure_ids"],
                "access_hold_retained": hold,
                "documentation": {
                    "bundled_dictionary_candidates": docs,
                    "potential_reference_route_ids": reference_routes,
                    "status": "bundled_dictionary_mapping_pending"
                    if docs
                    else ("separate_reference_route_review_pending" if reference_routes else "dictionary_discovery_required"),
                },
                "history_complete": False,
                "common_period_truncation": False,
                "next_action": "Resolve permitted access without bypassing controls."
                if hold
                else "Reconcile listed releases, documentation and earliest recoverable history; build explicit capture plans.",
            }
        )
    unique_candidates = {canonical_hash(item): item for item in candidates}
    candidates = list(unique_candidates.values())
    for item in candidates:
        item["known_route_ids"] = by_url.get(item["url"], [])
        item["acquisition_authorized_by_this_inventory"] = False
    return {
        "inventory_version": 1,
        "registry_sha256": canonical_hash(registry),
        "sources": sources,
        "known_routes": routes,
        "discovery_candidates": candidates,
        "stored_snapshots": acquired,
        "shared_download_groups": [
            {"url": url, "route_ids": ids, "status": "reuse_requires_equal_release_scope_and_hash"} for url, ids in sorted(by_url.items()) if len(ids) > 1
        ],
        "counts": {
            "sources": len(sources),
            "known_source_route_entries": len(routes),
            "distinct_known_urls": len(by_url),
            "access_holds": sum(item["access_hold_retained"] for item in sources),
            "discovery_candidates": len(candidates),
            "new_candidate_urls": len({item["url"] for item in candidates} - by_url.keys()),
            "stored_snapshots": len(acquired),
            "route_statuses": dict(sorted(Counter(item["status"] for item in routes).items())),
        },
        "limitations": [
            "Known and newly enumerated publisher links are not a proof of complete historical coverage.",
            "New candidate links do not modify the locked route approvals or bypass access holds.",
            "Publication labels, advertised catalog intervals and measurement periods remain distinct.",
            "Every source is retained; earlier and intervening history requires source-specific discovery where unresolved.",
            "No downloads of numeric datasets, new S3 writes, schema review or model eligibility are performed by this inventory.",
        ],
    }


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description="Build a complete candidate-pool inventory without claiming complete historical acquisition.")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cms-archive", type=Path)
    parser.add_argument("--cms-catalogs", type=Path)
    parser.add_argument("--receipt", action="append", type=Path, default=[])
    args = parser.parse_args()
    try:
        inventory = build_inventory(
            load_registry(),
            read_json(args.cms_archive) if args.cms_archive else None,
            read_json(args.cms_catalogs) if args.cms_catalogs else None,
            args.receipt,
        )
        inventory["supplemental_input_hashes"] = {
            name: fingerprint(path)[0] for name, path in (("cms_archive", args.cms_archive), ("cms_catalogs", args.cms_catalogs)) if path
        }
        write_once(args.output, encoded_json(inventory))
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    sys.stdout.write(str(json.dumps(inventory["counts"], indent=2)) + "\n")


if __name__ == "__main__":
    main()
