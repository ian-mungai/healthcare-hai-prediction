"""Recheck approved transport scope before storing numeric or audit-only bytes."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.acquisition.api_fallbacks import api_specification, check_bls, check_il_page, il_page_url
from scripts.acquisition.source_registry import canonical_hash, read_json
from scripts.acquisition.transport import CaptureError


def verify_route(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Reject receipt lineage or API coverage that differs from the source's approved route."""
    mode = lineage.get("mode")
    if mode == "census_acs_api":
        from scripts.acquisition.census_acs_api_contract import verify_capture as verify_census_capture

        verify_census_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "hud_api":
        from scripts.acquisition.hud_api_contract import verify_capture as verify_hud_capture

        verify_hud_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "hud_xlsx":
        from scripts.acquisition.hud_xlsx_contract import verify_capture as verify_hud_xlsx_capture

        verify_hud_xlsx_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "wonder_export":
        from scripts.acquisition.wonder_export_contract import verify_capture as verify_wonder_capture

        verify_wonder_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode in {"census_acs_detailed", "acs_derived"}:
        from scripts.acquisition.census_acs_detailed_contract import verify_capture as verify_census_detailed_capture

        verify_census_detailed_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "onc_mu_hospital":
        from scripts.acquisition.onc_mu_contract import verify_capture as verify_onc_mu_capture

        verify_onc_mu_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "hcai_util_workbook":
        from scripts.acquisition.hcai_util_contract import verify_capture as verify_hcai_capture

        verify_hcai_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "cms_owners_org":
        from scripts.acquisition.cms_owners_contract import verify_capture as verify_cms_owners_capture

        verify_cms_owners_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "bls_api_v2":
        from scripts.acquisition.bls_api_contract import verify_capture as verify_bls_capture

        verify_bls_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "mmd_api":
        from scripts.acquisition.mmd_api_contract import verify_capture

        verify_capture(receipt, source, lineage, root, evidence_only)
        return
    if mode == "file":
        routes = [route for route in source["file_routes"] if route["route_id"] == lineage.get("route_id")]
        if len(routes) != 1 or routes[0]["url"] != receipt["acquisition"]["requested_url"]:
            raise CaptureError("Storage requires the receipt's exact approved file route.")
        if routes[0].get("route_type") == "file_index" or routes[0].get("status") in {"access_failed", "HTTP_403_file_route_unverified"}:
            raise CaptureError("Discovery indexes and failed routes cannot be promoted to data uploads.")
        return
    request = receipt["acquisition"]["request_parameters"]
    plan = {"mode": mode, "fallback_reason": lineage.get("fallback_reason")}
    if mode == "bls_api":
        plan.update(
            series_ids=request.get("seriesid"),
            start_year=int(request["startyear"]),
            end_year=int(request["endyear"]),
            expected_periods=lineage.get("expected_periods"),
        )
    url, body = api_specification(source, plan)
    if url != receipt["acquisition"]["requested_url"] or (body or {}) != request:
        raise CaptureError("API receipt parameters differ from the approved request.")
    if evidence_only:
        return
    pagination = receipt["acquisition"]["pagination"]
    artifacts = receipt["artifacts"]
    if pagination.get("termination_verified") is not True or pagination.get("page_count") != len(artifacts):
        raise CaptureError("API page count and complete termination must be verified before raw storage.")
    requests = lineage["requests"]
    if len(requests) != len(artifacts) or any(item["role"] != "api_page" for item in artifacts):
        raise CaptureError("API page lineage differs from artifacts.")
    baseline, following = None, None
    seen: set[str] = set()
    for page, artifact in enumerate(artifacts, 1):
        payload = read_json(root / artifact["storage_path"])
        if mode == "bls_api":
            if len(artifacts) != 1 or check_bls(payload, plan) is not None or requests[page - 1]["requested_url"] != url:
                raise CaptureError("BLS response does not cover the explicit approved request.")
        else:
            if requests[page - 1]["requested_url"] != il_page_url(page) or (page > 1 and following is None):
                raise CaptureError("Illinois page sequence differs from its request lineage.")
            baseline, following = check_il_page(payload, page, baseline, seen)
    if following is not None:
        raise CaptureError("Illinois directory ends before the final page.")


def reuse_identity(receipt: dict) -> str:
    """Return a stable hash of the receipt's requested scope, release and checksum pin."""
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    return canonical_hash(
        {
            "requested_url": receipt["acquisition"]["requested_url"],
            "request_parameters": receipt["acquisition"]["request_parameters"],
            "mode": lineage["mode"],
            "scope": lineage["scope"],
            "release": receipt["release"],
            "measurement_periods": receipt["measurement_periods"],
            "expected_sha256": lineage.get("expected_sha256"),
        }
    )
