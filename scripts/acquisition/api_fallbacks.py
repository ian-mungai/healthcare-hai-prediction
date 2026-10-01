"""Only the approved BLS LAUS and Illinois hospital-directory API scopes."""

from __future__ import annotations

import re
from typing import Any, cast
from urllib.parse import parse_qs, urlencode, urlsplit

from scripts.acquisition.transport import CaptureError

BLS_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
IL_URL = "https://healthcarereportcard.illinois.gov/api/hospitals"


def api_specification(source: dict, plan: dict) -> tuple[str, dict | None]:
    """Validate a source-approved fallback plan and return its endpoint and request body."""
    if source["preferred_route"] not in {"api_fallback", "file_plus_api_fallback"} or not source.get("api_fallback", {}).get("needed"):
        raise CaptureError("No API fallback is approved for this source.")
    if not isinstance(plan.get("fallback_reason"), str) or not plan["fallback_reason"].strip():
        raise CaptureError("Document the unmet file requirement before using an API.")
    endpoints = source["api_fallback"]["endpoint_urls"]
    if plan["mode"] == "bls_api" and source["source_id"] == "BLS":
        series, start, end, periods = (plan[key] for key in ("series_ids", "start_year", "end_year", "expected_periods"))
        if not isinstance(series, list) or not 1 <= len(series) <= 25 or any(not isinstance(item, str) for item in series):
            raise CaptureError("A BLS capture requires 1-25 distinct county LAUS series IDs.")
        if len(set(series)) != len(series) or any(not re.fullmatch(r"LAUCN[0-9]{15}", item) for item in series):
            raise CaptureError("Use the reviewed county LAUS series crosswalk, not arbitrary BLS series.")
        if type(start) is not int or type(end) is not int or not 1900 <= start <= end <= 2100 or end - start >= 10:
            raise CaptureError("A BLS v1 capture must specify at most ten calendar years.")
        if not isinstance(periods, list) or not periods or any(not isinstance(item, str) or not re.fullmatch(r"M(0[1-9]|1[0-3])", item) for item in periods):
            raise CaptureError("Specify the requested monthly or annual-average period codes.")
        if BLS_URL not in endpoints:
            raise CaptureError("BLS endpoint is not in the approved registry.")
        return BLS_URL, {"seriesid": series, "startyear": str(start), "endyear": str(end)}
    if plan["mode"] == "il_directory" and source["source_id"] == "IL":
        if not any(urlsplit(url)._replace(query="").geturl() == IL_URL for url in endpoints):
            raise CaptureError("Illinois directory endpoint is not in the approved registry.")
        return il_page_url(1), None
    raise CaptureError("This source-specific API scope has not been implemented or approved.")


def il_page_url(page: int) -> str:
    """Return the approved Illinois directory URL for a numbered page."""
    return f"{IL_URL}?{urlencode({'per_page': 100, 'page': page})}"


def check_bls(payload: dict, plan: dict) -> str | None:
    """Return a failure code if the response does not cover the requested series and periods."""
    if payload.get("status") != "REQUEST_SUCCEEDED" or payload.get("message"):
        return "bls_reported_error_or_warning"
    results = payload.get("Results")
    if isinstance(results, list):
        if len(results) != 1:
            return "bls_ambiguous_results"
        results = results[0]
    if not isinstance(results, dict) or not isinstance(results.get("series"), list):
        return "bls_missing_series"
    series = results["series"]
    ids = [item.get("seriesID") for item in series if isinstance(item, dict)]
    if len(ids) != len(series) or len(ids) != len(set(ids)) or set(ids) != set(plan["series_ids"]):
        return "bls_series_scope_mismatch"
    expected = {(str(year), period) for year in range(plan["start_year"], plan["end_year"] + 1) for period in plan["expected_periods"]}
    for item in series:
        rows = item.get("data")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            return "bls_invalid_rows"
        keys = [(row.get("year"), row.get("period")) for row in rows]
        if any(not isinstance(year, str) or not isinstance(period, str) for year, period in keys):
            return "bls_invalid_period"
        if len(keys) != len(set(keys)) or not expected <= set(keys):
            return "bls_missing_or_duplicate_periods"
        if any(not year.isdigit() or not plan["start_year"] <= int(year) <= plan["end_year"] for year, _ in keys):
            return "bls_unrequested_year"
    return None


def check_il_page(payload: dict, page: int, baseline: tuple[int, int] | None, seen: set[str]) -> tuple[tuple[int, int], str | None]:
    """Validate page counts and unique identities; return stable counts and the next URL."""
    counts = [payload.get(key) for key in ("current_page", "last_page", "per_page", "total")]
    if any(type(value) is not int for value in counts):
        raise CaptureError("Illinois pagination counts are missing or invalid.")
    current, last, size, total = cast(list[int], counts)
    if current != page or not 1 <= current <= last or size != 100 or total < 0 or last != max(1, (total + 99) // 100):
        raise CaptureError("Illinois pagination counts are inconsistent.")
    if baseline is not None and baseline != (last, total):
        raise CaptureError("Illinois pagination changed during capture; retry as a new snapshot.")
    rows: Any = payload.get("data")
    expected_count = min(size, max(0, total - (page - 1) * size))
    if not isinstance(rows, list) or len(rows) != expected_count or any(not isinstance(row, dict) for row in rows):
        raise CaptureError("Illinois page row count differs from its declared scope.")
    for row in rows:
        identity = row.get("entity_id")
        if type(identity) not in {str, int} or not str(identity) or str(identity) in seen:
            raise CaptureError("Illinois directory has missing or repeated hospital identifiers.")
        seen.add(str(identity))
    following = payload.get("next_page_url")
    if page == last:
        if following is not None or len(seen) != total:
            raise CaptureError("Illinois pagination termination is not proven.")
        return (last, total), None
    if not isinstance(following, str):
        raise CaptureError("Illinois next-page link is missing.")
    parts = urlsplit(following)
    if parts._replace(query="").geturl() != IL_URL or parse_qs(parts.query).get("page") != [str(page + 1)]:
        raise CaptureError("Illinois next-page link left the approved directory or skipped a page.")
    return (last, total), il_page_url(page + 1)
