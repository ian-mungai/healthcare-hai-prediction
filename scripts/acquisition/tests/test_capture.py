from __future__ import annotations

import copy
import hashlib
import http.client
import io
import json
import sys
import urllib.error
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from scripts.acquisition import api_fallbacks as api
from scripts.acquisition import capture as captures
from scripts.acquisition import transport
from scripts.acquisition.source_registry import read_json
from tests.support import check


class Response(io.BytesIO):
    def __init__(self, body: bytes, status: int = 200, headers: dict | None = None, failure: Exception | None = None) -> None:
        super().__init__(body)
        self.status = status
        self.headers: Any = {"Content-Length": str(len(body)), **(headers or {})}
        self.url = "https://example.org/data.csv"
        self.failure = failure

    def geturl(self) -> str:
        return self.url

    def read(self, size: int | None = -1) -> bytes:
        if self.failure is not None:
            raise self.failure
        return super().read(size)


class Opener:
    def __init__(self, *responses: Response | Exception) -> None:
        self.responses = list(responses)
        self.requests: list = []

    def open(self, request: Any, timeout: float) -> Response:
        self.requests.append((request, timeout))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        response.url = request.full_url
        return response


@pytest.fixture
def setup_capture(monkeypatch: pytest.MonkeyPatch) -> tuple[dict, dict, object]:
    monkeypatch.setattr(api, "BLS_URL", "https://example.org/bls/")
    monkeypatch.setattr(api, "IL_URL", "https://example.org/hospitals")
    source = {
        "source_id": "example_source",
        "title": "Example public aggregate data",
        "preferred_route": "file_download",
        "official_landing_url": "https://example.org/data",
        "original_source_family_ids": ["S01"],
        "linked_measure_ids": ["M001"],
        "file_routes": [{"route_id": "example_route", "url": "https://example.org/data.csv", "route_type": "published_file", "status": "reviewed"}],
    }
    plan = {
        "source_id": source["source_id"],
        "mode": "file",
        "route_id": "example_route",
        "expected_format": "csv",
        "file_name": "data.csv",
        "publisher": "Example publisher",
        "scope": "Explicit synthetic test population",
        "role": "data",
        "release": {
            "publisher_release_label": "2021 publication",
            "release_date": "2021-01-01",
            "dataset_version": None,
            "publisher_updated_at": None,
            "revision_status": "unknown",
            "advertised_history": None,
            "observed_history": None,
        },
        "measurement_periods": [
            {
                "label": "2020",
                "start_date": "2020-01-01",
                "end_date": "2020-12-31",
                "period_type": "calendar_year",
                "source_basis": "publisher_stated",
                "target_alignment": "unknown",
            }
        ],
        "governance": {
            "access_class": "public",
            "license_or_terms_url": "https://example.org/terms",
            "use_restrictions": ["Public aggregates only"],
            "credentials_required": False,
            "credential_reference": None,
            "contains_phi": False,
            "contains_pii": False,
        },
    }
    schema = copy.deepcopy(read_json(captures.SCHEMA_PATH))
    schema["properties"]["project"]["const"] = "example_project"
    validator = captures.Draft202012Validator(schema, format_checker=captures.FormatChecker())
    return source, plan, validator


def run_capture(setup: tuple, tmp_path: Path, *responses: Response | Exception, limits: transport.Limits | None = None) -> tuple[Path, dict, Opener]:
    source, plan, validator = setup
    opener = Opener(*responses)
    path = captures.capture(plan, tmp_path, {"sources": [source]}, validator, limits, opener)
    return path, read_json(path), opener


def test_complete_capture_preserves_bytes_and_metadata(setup_capture: tuple, tmp_path: Path) -> None:
    content = b"id,value\r\n0012A,2\r\n"
    path, receipt, opener = run_capture(setup_capture, tmp_path, Response(content))
    artifact = receipt["artifacts"][0]
    check(receipt["snapshot_status"] == "acquired_unvalidated", 'receipt["snapshot_status"] == "acquired_unvalidated"')
    check(receipt["verification"]["status"] == "pending", 'receipt["verification"]["status"] == "pending"')
    check(receipt["schema_profile"]["row_count"] is None, 'receipt["schema_profile"]["row_count"] is None')
    check(receipt["measurement_periods"][0]["start_date"] == "2020-01-01", 'receipt["measurement_periods"][0]["start_date"] == "2020-01-01"')
    check(receipt["release"]["release_date"] == "2021-01-01", 'receipt["release"]["release_date"] == "2021-01-01"')
    check(artifact["sha256"] == hashlib.sha256(content).hexdigest(), 'artifact["sha256"] == hashlib.sha256(content).hexdigest()')
    check((path.parent / artifact["storage_path"]).read_bytes() == content, '(path.parent / artifact["storage_path"]).read_bytes() == content')
    check(artifact["storage_path"].startswith("raw/"), 'artifact["storage_path"].startswith("raw/")')
    check(opener.requests[0][1] == 30, "opener.requests[0][1] == 30")
    captures.validate_receipt(receipt, setup_capture[2], path.parent)


@pytest.mark.parametrize(
    "response,status,reason",
    [
        (Response(b"id\n1\n", headers={"Content-Length": "90"}), "evidence_only_partial", "content_length_mismatch"),
        (Response(b"id\n1\n", status=206), "evidence_only_partial", "partial_http_response"),
        (Response(b"<html>Access denied</html>"), "rejected", "html_instead_of_data"),
        (Response(b"", status=404), "rejected", "http_404"),
        (Response(b""), "rejected", "empty_response"),
        (Response(b'{"error":"denied"}'), "rejected", "unexpected_text_payload"),
        (Response(b"id\n1\n", headers={"Content-Length": "invalid"}), "evidence_only_partial", "content_length_mismatch"),
    ],
)
def test_partial_and_error_responses_are_not_complete(setup_capture: tuple, tmp_path: Path, response: Response, status: str, reason: str) -> None:
    path, receipt, _ = run_capture(setup_capture, tmp_path, response)
    check(receipt["snapshot_status"] == status, 'receipt["snapshot_status"] == status')
    check(reason in receipt["quality_profile"]["checks_failed"], 'reason in receipt["quality_profile"]["checks_failed"]')
    check(receipt["artifacts"][0]["storage_path"].startswith("audit/"), 'receipt["artifacts"][0]["storage_path"].startswith("audit/")')
    check(not (path.parent / "raw").exists(), 'not (path.parent / "raw").exists()')


def test_bounded_retries_keep_failure_evidence(setup_capture: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []
    monkeypatch.setattr(transport.time, "sleep", delays.append)
    path, receipt, opener = run_capture(setup_capture, tmp_path, Response(b"temporary", 503), Response(b"id\n1\n"))
    check(receipt["snapshot_status"] == "acquired_unvalidated", 'receipt["snapshot_status"] == "acquired_unvalidated"')
    check(len(opener.requests) == 2 and delays == [1], "len(opener.requests) == 2 and delays == [1]")
    check(
        (path.parent / "audit/request_0001/attempt_01/data.csv").read_bytes() == b"temporary",
        '(path.parent / "audit/request_0001/attempt_01/data.csv").read_bytes() == b"temporary"',
    )
    check((path.parent / "raw/data.csv").read_bytes() == b"id\n1\n", '(path.parent / "raw/data.csv").read_bytes() == b"id\\n1\\n"')


def test_network_failures_stop_at_retry_limit(setup_capture: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transport.time, "sleep", lambda _: None)
    failures = [urllib.error.URLError("unavailable") for _ in range(3)]
    path, receipt, opener = run_capture(setup_capture, tmp_path, *failures)
    check(receipt["snapshot_status"] == "rejected" and len(opener.requests) == 3, 'receipt["snapshot_status"] == "rejected" and len(opener.requests) == 3')
    check(len(list(path.parent.glob("audit/*/attempt_*/transport.json"))) == 3, 'len(list(path.parent.glob("audit/*/attempt_*/transport.json"))) == 3')


def test_retry_after_is_not_ignored(setup_capture: tuple, tmp_path: Path) -> None:
    _, receipt, opener = run_capture(setup_capture, tmp_path, Response(b"limit", 429, {"Retry-After": "600"}))
    check(len(opener.requests) == 1 and receipt["snapshot_status"] == "rejected", 'len(opener.requests) == 1 and receipt["snapshot_status"] == "rejected"')


def test_incomplete_read_preserves_received_bytes(setup_capture: tuple, tmp_path: Path) -> None:
    response = Response(b"", failure=http.client.IncompleteRead(b"partial", 20))
    path, receipt, _ = run_capture(setup_capture, tmp_path, response, limits=transport.Limits(attempts=1))
    check(receipt["snapshot_status"] == "evidence_only_partial", 'receipt["snapshot_status"] == "evidence_only_partial"')
    check(
        (path.parent / receipt["artifacts"][0]["storage_path"]).read_bytes() == b"partial",
        '(path.parent / receipt["artifacts"][0]["storage_path"]).read_bytes() == b"partial"',
    )


def test_size_limit_does_not_write_excess_bytes(setup_capture: tuple, tmp_path: Path) -> None:
    path, receipt, _ = run_capture(setup_capture, tmp_path, Response(b"id\n123456\n"), limits=transport.Limits(max_bytes=5))
    check(receipt["snapshot_status"] == "evidence_only_partial", 'receipt["snapshot_status"] == "evidence_only_partial"')
    check(receipt["artifacts"][0]["byte_count"] == 5, 'receipt["artifacts"][0]["byte_count"] == 5')
    captures.validate_receipt(receipt, setup_capture[2], path.parent)


def test_repeated_capture_does_not_overwrite(setup_capture: tuple, tmp_path: Path) -> None:
    first, _, _ = run_capture(setup_capture, tmp_path, Response(b"id\n1\n"))
    original = first.read_bytes()
    second, _, _ = run_capture(setup_capture, tmp_path, Response(b"id\n1\n"))
    check(first != second and first.read_bytes() == original, "first != second and first.read_bytes() == original")


@pytest.mark.parametrize("route", ["access_hold", "documentation_only"])
def test_non_numeric_routes_cannot_enter_data_capture(setup_capture: tuple, tmp_path: Path, route: str) -> None:
    source, _, _ = setup_capture
    source["preferred_route"] = route
    with pytest.raises(transport.CaptureError):
        run_capture(setup_capture, tmp_path)
    check(not list(tmp_path.iterdir()), "not list(tmp_path.iterdir())")


@pytest.mark.parametrize("field,value", [("credentials_required", True), ("contains_phi", None), ("contains_pii", True), ("access_class", "restricted")])
def test_access_assessment_is_required(setup_capture: tuple, tmp_path: Path, field: str, value: object) -> None:
    setup_capture[1]["governance"][field] = value
    with pytest.raises(transport.CaptureError):
        run_capture(setup_capture, tmp_path)


def test_file_index_is_not_treated_as_a_download(setup_capture: tuple, tmp_path: Path) -> None:
    setup_capture[0]["file_routes"][0]["route_type"] = "file_index"
    with pytest.raises(transport.CaptureError, match="discovery index"):
        run_capture(setup_capture, tmp_path)


def test_export_records_selections(setup_capture: tuple, tmp_path: Path) -> None:
    setup_capture[0]["file_routes"][0]["route_type"] = "permitted_export"
    with pytest.raises(transport.CaptureError, match="export selections"):
        run_capture(setup_capture, tmp_path)
    setup_capture[1]["export_selections"] = {"hospital": "example_hospital", "all_measures": True}
    _, receipt, _ = run_capture(setup_capture, tmp_path, Response(b"id\n1\n"))
    check(receipt["acquisition"]["transport_mode"] == "web_export", 'receipt["acquisition"]["transport_mode"] == "web_export"')


def test_expected_checksum_mismatch_rejects(setup_capture: tuple, tmp_path: Path) -> None:
    setup_capture[1]["expected_sha256"] = "0" * 64
    _, receipt, _ = run_capture(setup_capture, tmp_path, Response(b"id\n1\n"))
    check(receipt["snapshot_status"] == "rejected", 'receipt["snapshot_status"] == "rejected"')
    check(
        receipt["quality_profile"]["checks_failed"] == ["expected_hash_mismatch"], 'receipt["quality_profile"]["checks_failed"] == ["expected_hash_mismatch"]'
    )


@pytest.mark.parametrize("mutation", ["bad_date", "reversed_period", "unexpected_field", "missing_governance"])
def test_formal_contract_validation_precedes_capture(setup_capture: tuple, tmp_path: Path, mutation: str) -> None:
    plan = setup_capture[1]
    if mutation == "bad_date":
        plan["release"]["release_date"] = "2021-02-30"
    elif mutation == "reversed_period":
        plan["measurement_periods"][0]["start_date"] = "2022-01-01"
    elif mutation == "unexpected_field":
        plan["release"]["extra"] = True
    else:
        plan["governance"].pop("use_restrictions")
    with pytest.raises(transport.CaptureError):
        run_capture(setup_capture, tmp_path)
    check(not list(tmp_path.iterdir()), "not list(tmp_path.iterdir())")


def test_receipt_verifier_rejects_tampering_and_unsafe_paths(setup_capture: tuple, tmp_path: Path) -> None:
    path, receipt, _ = run_capture(setup_capture, tmp_path, Response(b"id\n1\n"))
    changed = copy.deepcopy(receipt)
    changed["artifacts"][0]["storage_path"] = "../data.csv"
    with pytest.raises(transport.CaptureError, match="inside"):
        captures.validate_receipt(changed, setup_capture[2], path.parent)
    (path.parent / receipt["artifacts"][0]["storage_path"]).write_bytes(b"changed")
    with pytest.raises(transport.CaptureError, match="hash or byte count"):
        captures.validate_receipt(receipt, setup_capture[2], path.parent)


def test_real_snapshot_schema_compiles() -> None:
    validator = captures.receipt_validator()
    # jsonschema types a schema as bool | Mapping; a boolean schema would not be the real receipt schema.
    check(
        not isinstance(validator.schema, bool) and validator.schema["$schema"].endswith("2020-12/schema"),
        'not isinstance(validator.schema, bool) and validator.schema["$schema"].endswith("2020-12/schema")',
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://example.org/data",
        "https://u:p@example.org/data",
        "https://example.org/?token=private",
        "https://127.0.0.1/data",
        "https://10.0.0.1/data",
        "https://example.org/data#fragment",
        "file:///tmp/data",
    ],
)
def test_unsafe_urls_fail(url: str) -> None:
    with pytest.raises(transport.CaptureError):
        transport.safe_url(url)


@pytest.mark.parametrize(
    "format_name,body",
    [
        ("zip", b"PK truncated"),
        ("xlsx", b"not a workbook"),
        ("pdf", b"%PDF-1.7 truncated"),
        ("json", b"{invalid"),
        ("xls", b"not binary"),
        ("gzip", b"not compressed"),
    ],
)
def test_container_checks_reject_wrong_payloads(tmp_path: Path, format_name: str, body: bytes) -> None:
    path = tmp_path / "payload"
    path.write_bytes(body)
    check(transport.inspect_payload(path, format_name, "data"), 'transport.inspect_payload(path, format_name, "data")')


def test_zip_is_not_extracted_or_rewritten(tmp_path: Path) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("data.csv", "id\n1\n")
    body = buffer.getvalue()
    result = transport.download("https://example.org/data.zip", tmp_path, "data.zip", "zip", "data", transport.Limits(), opener=Opener(Response(body)))
    check(result.complete and result.path.read_bytes() == body, "result.complete and result.path.read_bytes() == body")
    check(not (result.path.parent / "data.csv").exists(), 'not (result.path.parent / "data.csv").exists()')


def configure_api(setup: tuple, source_id: str, mode: str) -> None:
    source, plan, _ = setup
    source["source_id"], plan["source_id"], plan["mode"] = source_id, source_id, mode
    source["preferred_route"] = "api_fallback" if mode == "bls_api" else "file_plus_api_fallback"
    source["api_fallback"] = {"needed": True, "endpoint_urls": [api.BLS_URL] if mode == "bls_api" else [api.il_page_url(1)]}
    plan["fallback_reason"] = "The approved file does not supply this required scope."


def bls_response(series: str, data: list | None = None, message: list | None = None) -> Response:
    payload = {
        "status": "REQUEST_SUCCEEDED",
        "message": message or [],
        "Results": {"series": [{"seriesID": series, "data": [{"year": "2020", "period": "M13", "value": "3.0"}] if data is None else data}]},
    }
    return Response(json.dumps(payload).encode())


def test_bls_explicit_scope_and_success(setup_capture: tuple, tmp_path: Path) -> None:
    configure_api(setup_capture, "BLS", "bls_api")
    plan = setup_capture[1]
    plan.update({"series_ids": ["LAUCN999990000000003"], "start_year": 2020, "end_year": 2020, "expected_periods": ["M13"]})
    _, receipt, opener = run_capture(setup_capture, tmp_path, bls_response(plan["series_ids"][0]))
    check(receipt["snapshot_status"] == "acquired_unvalidated", 'receipt["snapshot_status"] == "acquired_unvalidated"')
    check(receipt["acquisition"]["pagination"]["termination_verified"] is True, 'receipt["acquisition"]["pagination"]["termination_verified"] is True')
    check(json.loads(opener.requests[0][0].data)["startyear"] == "2020", 'json.loads(opener.requests[0][0].data)["startyear"] == "2020"')


@pytest.mark.parametrize("reason", ["missing_period", "error_message", "wrong_series"])
def test_bls_success_http_does_not_prove_requested_scope(setup_capture: tuple, tmp_path: Path, reason: str) -> None:
    configure_api(setup_capture, "BLS", "bls_api")
    plan = setup_capture[1]
    plan.update({"series_ids": ["LAUCN999990000000003"], "start_year": 2020, "end_year": 2020, "expected_periods": ["M13"]})
    response = bls_response(
        "OTHER" if reason == "wrong_series" else plan["series_ids"][0],
        data=[] if reason == "missing_period" else None,
        message=["Invalid series"] if reason == "error_message" else None,
    )
    _, receipt, _ = run_capture(setup_capture, tmp_path, response)
    check(receipt["snapshot_status"] == "evidence_only_partial", 'receipt["snapshot_status"] == "evidence_only_partial"')
    check(receipt["acquisition"]["pagination"]["termination_verified"] is False, 'receipt["acquisition"]["pagination"]["termination_verified"] is False')


def il_response(page: int, total: int = 101, duplicate: bool = False) -> Response:
    last = max(1, (total + 99) // 100)
    rows = [{"entity_id": str(index)} for index in range((page - 1) * 100, min(page * 100, total))]
    if duplicate and rows:
        rows[0]["entity_id"] = "0"
    payload = {
        "current_page": page,
        "last_page": last,
        "per_page": 100,
        "total": total,
        "data": rows,
        "next_page_url": None if page == last else f"{api.IL_URL}?page={page + 1}",
    }
    return Response(json.dumps(payload).encode())


def test_illinois_preserves_page_size_and_proves_termination(setup_capture: tuple, tmp_path: Path) -> None:
    configure_api(setup_capture, "IL", "il_directory")
    path, receipt, opener = run_capture(setup_capture, tmp_path, il_response(1), il_response(2))
    check(
        receipt["snapshot_status"] == "acquired_unvalidated" and len(receipt["artifacts"]) == 2,
        'receipt["snapshot_status"] == "acquired_unvalidated" and len(receipt["artifacts"]) == 2',
    )
    check(receipt["quality_profile"]["pagination_complete"] is True, 'receipt["quality_profile"]["pagination_complete"] is True')
    check(
        parse_qs(urlsplit(opener.requests[1][0].full_url).query)["per_page"] == ["100"],
        'parse_qs(urlsplit(opener.requests[1][0].full_url).query)["per_page"] == ["100"]',
    )
    check(
        all((path.parent / artifact["storage_path"]).exists() for artifact in receipt["artifacts"]),
        'all((path.parent / artifact["storage_path"]).exists() for artifact in receipt["artifacts"])',
    )


@pytest.mark.parametrize("failure", ["duplicate", "count_changed", "page_limit"])
def test_illinois_incomplete_directory_stays_evidence_only(setup_capture: tuple, tmp_path: Path, failure: str) -> None:
    configure_api(setup_capture, "IL", "il_directory")
    responses = [il_response(1)]
    if failure != "page_limit":
        responses.append(il_response(2, total=102 if failure == "count_changed" else 101, duplicate=failure == "duplicate"))
    _, receipt, _ = run_capture(setup_capture, tmp_path, *responses, limits=transport.Limits(max_pages=1 if failure == "page_limit" else 100))
    check(receipt["snapshot_status"] == "evidence_only_partial", 'receipt["snapshot_status"] == "evidence_only_partial"')


def test_unapproved_api_scope_fails_before_network(setup_capture: tuple, tmp_path: Path) -> None:
    setup_capture[1]["mode"] = "bls_api"
    with pytest.raises(transport.CaptureError, match="No API fallback"):
        run_capture(setup_capture, tmp_path)


def test_cli_receipt_verification(setup_capture: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    path, _, _ = run_capture(setup_capture, tmp_path, Response(b"id\n1\n"))
    monkeypatch.setattr(captures, "receipt_validator", lambda _: setup_capture[2])
    monkeypatch.setattr(sys, "argv", ["capture", "--validate-receipt", str(path)])
    captures.main()
    check("integrity verified" in capsys.readouterr().out, '"integrity verified" in capsys.readouterr().out')


@pytest.mark.parametrize("settings", [{"attempts": 0}, {"attempts": 6}, {"timeout_seconds": 0}, {"max_seconds": 0}, {"max_bytes": 0}, {"max_pages": 0}])
def test_invalid_limits_fail(settings: dict) -> None:
    with pytest.raises(transport.CaptureError):
        transport.Limits(**settings)


@pytest.mark.parametrize(
    "field,value",
    [
        ("file_name", "../escape.csv"),
        ("file_name", "receipt.json/extra"),
        ("expected_format", "unknown"),
        ("expected_format", "html"),
        ("expected_sha256", "bad"),
        ("route_id", "missing"),
        ("mode", "unapproved"),
        ("file_name", "transport.json"),
        ("scope", ""),
        ("misspelled_option", True),
    ],
)
def test_bad_plans_do_not_create_output(setup_capture: tuple, tmp_path: Path, field: str, value: object) -> None:
    setup_capture[1][field] = value
    with pytest.raises(transport.CaptureError):
        run_capture(setup_capture, tmp_path)
    check(not list(tmp_path.iterdir()), "not list(tmp_path.iterdir())")


def test_reference_document_and_publisher_filename(setup_capture: tuple, tmp_path: Path) -> None:
    source, plan, _ = setup_capture
    source["preferred_route"], plan["role"], plan["expected_format"] = "documentation_only", "methodology", "html"
    response = Response(b"<html>Methodology</html>", headers={"Content-Disposition": 'attachment; filename="publisher_original.html"'})
    _, receipt, _ = run_capture(setup_capture, tmp_path, response)
    check(receipt["acquisition"]["transport_mode"] == "reference_document", 'receipt["acquisition"]["transport_mode"] == "reference_document"')
    check(
        receipt["artifacts"][0]["original_file_name"] == "publisher_original.html", 'receipt["artifacts"][0]["original_file_name"] == "publisher_original.html"'
    )


def test_http_error_body_is_retained(setup_capture: tuple, tmp_path: Path) -> None:
    error = urllib.error.HTTPError("https://example.org/data.csv", 403, "Forbidden", transport.Message(), io.BytesIO(b"denied"))
    _, receipt, _ = run_capture(setup_capture, tmp_path, error)
    check(receipt["snapshot_status"] == "rejected", 'receipt["snapshot_status"] == "rejected"')
    check(receipt["artifacts"][0]["byte_count"] == 6, 'receipt["artifacts"][0]["byte_count"] == 6')


def test_unrequested_http_encoding_is_not_silently_decoded(setup_capture: tuple, tmp_path: Path) -> None:
    _, receipt, _ = run_capture(setup_capture, tmp_path, Response(b"opaque", headers={"Content-Encoding": "gzip"}))
    check(
        receipt["quality_profile"]["checks_failed"] == ["encoded_response_requires_review"],
        'receipt["quality_profile"]["checks_failed"] == ["encoded_response_requires_review"]',
    )


def test_redirects_cannot_change_host_or_downgrade() -> None:
    handler = transport.ReviewedRedirects({"example.org"})
    request = transport.urllib.request.Request("https://example.org/old")
    with pytest.raises(transport.CaptureError):
        handler.redirect_request(request, None, 302, "Found", {}, "https://other.example/data")
    with pytest.raises(transport.CaptureError):
        handler.redirect_request(request, None, 302, "Found", {}, "http://example.org/data")
    changed = handler.redirect_request(request, None, 302, "Found", {}, "https://example.org/new")
    check(changed.full_url == "https://example.org/new", 'changed.full_url == "https://example.org/new"')


@pytest.mark.parametrize(
    "field,value", [("series_ids", []), ("series_ids", ["INVALID"]), ("start_year", 2000), ("expected_periods", []), ("fallback_reason", "")]
)
def test_bls_request_bounds(setup_capture: tuple, tmp_path: Path, field: str, value: object) -> None:
    configure_api(setup_capture, "BLS", "bls_api")
    plan = setup_capture[1]
    plan.update({"series_ids": ["LAUCN999990000000003"], "start_year": 2020, "end_year": 2020, "expected_periods": ["M13"]})
    plan[field] = value
    with pytest.raises(transport.CaptureError):
        run_capture(setup_capture, tmp_path)


@pytest.mark.parametrize("change", ["missing_counts", "bad_counts", "missing_rows", "missing_next", "foreign_next", "premature_end"])
def test_illinois_page_contract_failures(change: str) -> None:
    payload = json.loads(il_response(1).getvalue())
    if change == "missing_counts":
        payload.pop("total")
    elif change == "bad_counts":
        payload["per_page"] = 15
    elif change == "missing_rows":
        payload["data"].pop()
    elif change == "missing_next":
        payload["next_page_url"] = None
    elif change == "foreign_next":
        payload["next_page_url"] = "https://other.example/hospitals?page=2"
    else:
        payload = json.loads(il_response(1, 1).getvalue())
        payload["next_page_url"] = api.il_page_url(2)
    with pytest.raises(transport.CaptureError):
        api.check_il_page(payload, 1, None, set())


def test_capture_cannot_promote_model_readiness(setup_capture: tuple, tmp_path: Path) -> None:
    _, receipt, _ = run_capture(setup_capture, tmp_path, Response(b"id\n1\n"))
    receipt["snapshot_status"] = "validated_raw_snapshot"
    with pytest.raises(transport.CaptureError, match="inconsistent"):
        captures.validate_receipt(receipt, setup_capture[2])
    receipt["verification"]["status"] = "accepted_raw_snapshot"
    with pytest.raises(transport.CaptureError, match="separate post-storage review"):
        captures.validate_receipt(receipt, setup_capture[2])


def test_local_dependency_record_pins_the_original_schema() -> None:
    dependency_path = captures.SCHEMA_PATH.with_name("runtime_dependencies.json")
    check(
        read_json(dependency_path)["snapshot_contract_file_sha256"] == hashlib.sha256(captures.SCHEMA_PATH.read_bytes()).hexdigest(),
        'read_json(dependency_path)["snapshot_contract_file_sha256"] == hashlib.sha256(captures.SCHEMA_PATH.read_bytes()).hexdigest()',
    )
