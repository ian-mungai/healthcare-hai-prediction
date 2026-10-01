"""Bounded, byte-preserving HTTP capture; failed attempts remain separate evidence."""

from __future__ import annotations

import hashlib
import http.client
import ipaddress
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import reject_constant, unique_object

# Exact publisher resource reviewed on 2026-09-24; no bucket-wide redirect permission.
SIGNED_REDIRECT_ROUTES = {
    "https://data.chhs.ca.gov/dataset/9c594ceb-fe46-4e6d-9553-c138a868bdc5/resource/e887fd1f-331c-45ea-ab9f-332ab21224bd/download/49hospitaldata.xlsx": "https://s3.amazonaws.com/og-production-open-data-chelseama-892364687672/resources/e887fd1f-331c-45ea-ab9f-332ab21224bd/49hospitaldata.xlsx"
}


class CaptureError(ValueError):
    """A capture validation failure that must not promote incomplete bytes."""

    pass


@dataclass(frozen=True)
class Limits:
    """Validated retry, time, byte and pagination bounds for one capture."""

    attempts: int = 3
    timeout_seconds: float = 30
    max_seconds: float = 300
    max_bytes: int = 512 * 1024 * 1024
    max_retry_delay: float = 30
    max_pages: int = 100

    def __post_init__(self) -> None:
        if not 1 <= self.attempts <= 5 or not 0 < self.timeout_seconds <= 120 or not 0 < self.max_seconds <= 3600:
            raise CaptureError("Invalid retry or timeout bounds.")
        if not 0 < self.max_bytes <= 4 * 1024**3 or not 0 <= self.max_retry_delay <= 300 or not 1 <= self.max_pages <= 1000:
            raise CaptureError("Invalid size, delay or pagination bounds.")


@dataclass(frozen=True)
class Download:
    """Immutable payload and transport metadata for one bounded download attempt."""

    path: Path
    requested_url: str
    resolved_url: str | None
    retrieved_at_utc: str
    http_status: int | None
    byte_count: int
    sha256: str
    media_type: str | None
    complete: bool
    failure: str | None
    partial: bool
    attempt: int
    original_file_name: str | None = None
    resolved_url_redacted: bool = False


def utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 form."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def safe_url(url: str) -> str:
    """Validate and return an unsigned public HTTPS URL without credential fields."""
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
        raise CaptureError("Capture requires a public HTTPS URL without credentials or a fragment.")
    secret_fields = {"key", "api_key", "apikey", "token", "access_token", "password", "authorization", "registrationkey", "signature", "x-amz-signature"}
    if any(key.lower() in secret_fields or key.lower().startswith(("x-amz-", "x-goog-")) for key, _ in urllib.parse.parse_qsl(parsed.query)):
        raise CaptureError("Credentials and signed URLs cannot be saved in capture metadata.")
    try:
        nonpublic = not ipaddress.ip_address(parsed.hostname).is_global
    except ValueError:
        nonpublic = parsed.hostname == "localhost" or parsed.hostname.endswith(".localhost")
    if nonpublic:
        raise CaptureError("Local and metadata-service URLs are not public source routes.")
    return url


class HttpsRequest(urllib.request.Request):
    """Construct only public HTTPS requests, retaining already-reviewed signed query strings."""

    def __init__(self, url: str, data: bytes | None = None, headers: dict[str, str] | None = None, method: str | None = None) -> None:
        unsigned = urllib.parse.urlunsplit(urllib.parse.urlsplit(url)._replace(query=""))
        safe_url(unsigned)
        super().__init__(url, data=data, headers=headers or {}, method=method)


class ReviewedRedirects(urllib.request.HTTPRedirectHandler):
    """Restrict redirects to reviewed HTTPS destinations and redact signed metadata."""

    def __init__(self, allowed_hosts: set[str], source_url: str | None = None) -> None:
        self.allowed_hosts = allowed_hosts
        self.source_url = source_url
        self.signed_destination: str | None = None

    def resolved_metadata(self, url: str) -> tuple[str, bool]:
        """Return the reviewed resolved URL and whether signed query metadata was removed."""
        if self.signed_destination is not None:
            if url != self.signed_destination:
                raise CaptureError("Resolved URL differs from the reviewed signed redirect.")
            return urllib.parse.urlunsplit(urllib.parse.urlsplit(url)._replace(query="")), True
        safe_url(url)
        if urllib.parse.urlsplit(url).hostname not in self.allowed_hosts:
            raise CaptureError("Resolved URL left the reviewed host.")
        return url, False

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        """Permit only a reviewed HTTPS redirect without forwarding sensitive headers across hosts."""
        if self.signed_destination is not None:
            raise CaptureError("Additional redirects after a signed download are not approved.")
        target = SIGNED_REDIRECT_ROUTES.get(self.source_url or "")
        if target is not None:
            parsed = urllib.parse.urlsplit(newurl)
            unsigned = urllib.parse.urlunsplit(parsed._replace(query=""))
            permitted_targets = {target, target.replace("https://s3.amazonaws.com/", "https://s3.amazonaws.com:443/")}
            pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
            parameters = dict(pairs)
            required = {"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature"}
            if (
                req.full_url != self.source_url
                or req.get_method() != "GET"
                or code not in {302, 303, 307, 308}
                or unsigned not in permitted_targets
                or len(pairs) != len(parameters)
                or not required <= parameters.keys()
                or parameters.keys() - required - {"X-Amz-Security-Token"}
                or not all(parameters.values())
                or parameters["X-Amz-Algorithm"] != "AWS4-HMAC-SHA256"
                or parameters["X-Amz-SignedHeaders"] != "host"
                or not re.fullmatch(r"[a-f0-9]{64}", parameters["X-Amz-Signature"])
                or not re.fullmatch(r"\d{8}T\d{6}Z", parameters["X-Amz-Date"])
                or not parameters["X-Amz-Expires"].isdigit()
                or not 1 <= int(parameters["X-Amz-Expires"]) <= 86400
            ):
                raise CaptureError("Signed redirect differs from the exact reviewed publisher resource.")
            self.signed_destination = newurl
            # Start a clean request so no publisher Authorization, cookies or custom headers cross hosts.
            # Canonical HTTPS authority omits the default port in the signed Host header.
            return HttpsRequest(newurl, headers={"Accept-Encoding": "identity", "User-Agent": "HistoricalSourceCapture/1.0", "Host": str(parsed.hostname)})
        safe_url(newurl)
        if urllib.parse.urlsplit(newurl).hostname not in self.allowed_hosts:
            raise CaptureError("Redirect host has not been reviewed for this capture.")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# Most specific first. Codes keep the spellings earlier receipts used, but always come from this table, never a run-time name.
FAILURE_CODES: tuple[tuple[type[BaseException], str], ...] = (
    (CaptureError, "CaptureError"),
    (urllib.error.HTTPError, "HTTPError"),
    (urllib.error.URLError, "URLError"),
    (TimeoutError, "TimeoutError"),
    (ConnectionResetError, "ConnectionResetError"),
    (OSError, "OSError"),
)
PAYLOAD_FAILURES = frozenset(
    {
        "empty_response",
        "html_instead_of_data",
        "invalid_archive_structure",
        "invalid_document_structure",
        "invalid_pdf_structure",
        "json_inspection_size_limit",
        "invalid_json",
        "invalid_xls_signature",
        "invalid_gzip_signature",
        "unexpected_text_payload",
    }
)
KNOWN_FAILURES = (
    PAYLOAD_FAILURES
    | {code for _kind, code in FAILURE_CODES}
    | {
        "invalid_content_length",
        "unexpected_signed_response_media_type",
        "size_limit",
        "partial_http_response",
        "encoded_response_requires_review",
        "content_length_mismatch",
        "incomplete_read",
        "other",
    }
)


def failure_code(error: BaseException) -> str:
    """Map a caught download exception to a fixed code, so a dynamically named subclass carries no text."""
    return next((code for kind, code in FAILURE_CODES if isinstance(error, kind)), "other")


def known_failure(code: object) -> str:
    """Return a recorded failure code only when it is a fixed code or ``http_`` plus three digits."""
    text = str(code)
    return text if text in KNOWN_FAILURES or re.fullmatch(r"http_\d{3}", text) else "unrecognised_failure"


def inspect_payload(path: Path, expected_format: str, role: str) -> str | None:
    """Return a failure code for empty, malformed or unexpected payload bytes."""
    with path.open("rb") as handle:
        prefix = handle.read(8192).lstrip(b"\xef\xbb\xbf \r\n\t").lower()
    if not prefix:
        return "empty_response"
    html = prefix.startswith((b"<!doctype html", b"<html", b"<head", b"<body"))
    if html and (expected_format != "html" or role in {"data", "api_page"}):
        return "html_instead_of_data"
    if expected_format in {"zip", "xlsx", "docx"}:
        try:
            with zipfile.ZipFile(path) as archive:
                if not archive.infolist() or (expected_format == "xlsx" and "[Content_Types].xml" not in archive.namelist()):
                    return "invalid_archive_structure"
                if expected_format == "docx" and not {"[Content_Types].xml", "word/document.xml"} <= set(archive.namelist()):
                    return "invalid_document_structure"
        except (zipfile.BadZipFile, OSError):
            return "invalid_archive_structure"
    elif expected_format == "pdf":
        with path.open("rb") as handle:
            handle.seek(max(0, path.stat().st_size - 4096))
            tail = handle.read()
        if not prefix.startswith(b"%pdf-") or b"%%EOF" not in tail:
            return "invalid_pdf_structure"
    elif expected_format == "json":
        if path.stat().st_size > 32 * 1024**2:
            return "json_inspection_size_limit"
        try:
            with path.open("rb") as handle:
                json.load(handle, object_pairs_hook=unique_object, parse_constant=reject_constant)
        except (ValueError, UnicodeError):
            return "invalid_json"
    elif expected_format == "xls" and not prefix.startswith(b"\xd0\xcf\x11\xe0"):
        return "invalid_xls_signature"
    elif expected_format == "gzip" and not prefix.startswith(b"\x1f\x8b"):
        return "invalid_gzip_signature"
    elif expected_format in {"csv", "txt"} and (b"\x00" in prefix or prefix.startswith((b"{", b"[", b"<?xml"))):
        return "unexpected_text_payload"
    return None


def validate_request(url: str, file_name: str, expected_format: str, role: str) -> None:
    """Reject an unsafe source URL, filename, format or artifact role before capture."""
    safe_url(url)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}", file_name) or file_name == "transport.json":
        raise CaptureError("Use a plain stored filename without directories.")
    if expected_format not in {"csv", "txt", "zip", "xlsx", "xls", "pdf", "json", "html", "gzip", "docx"}:
        raise CaptureError("Unsupported expected format; review a parser before capture.")
    if expected_format == "html" and role in {"data", "api_page"}:
        raise CaptureError("HTML routes cannot be numeric artifacts.")


def download(url: str, dest: Path, file_name: str, expected_format: str, role: str, limits: Limits, body: dict | None = None, opener: Any = None) -> Download:
    """Return bounded capture evidence while retaining each failed attempt separately."""
    from scripts.acquisition import redownload_controls as controls

    validate_request(url, file_name, expected_format, role)
    if (boundary := controls.CURRENT.get()) is not None:
        controls.validate_path(dest, boundary.root)
    host = urllib.parse.urlsplit(url).hostname
    supplied_opener = opener
    started = time.monotonic()
    last: Download | None = None
    for attempt in range(1, limits.attempts + 1):
        controls.begin(url)
        redirects = ReviewedRedirects({str(host)}, source_url=url)
        opener = supplied_opener or urllib.request.build_opener(redirects)
        resolve_metadata = getattr(supplied_opener, "resolved_metadata", redirects.resolved_metadata)
        directory = dest / f"attempt_{attempt:02}"
        directory.mkdir(parents=True, exist_ok=False)
        path = directory / file_name
        request = HttpsRequest(url, data=None if body is None else json.dumps(body).encode(), method="GET" if body is None else "POST")
        request.add_header("Accept-Encoding", "identity")
        request.add_header("User-Agent", "HistoricalSourceCapture/1.0")
        if body is not None:
            request.add_header("Content-Type", "application/json")
        status, resolved, media, length, retry_after = None, None, None, None, None
        original_name = None
        resolved_redacted = False
        failure, partial, count, digest = None, False, 0, hashlib.sha256()
        retrieved = utc_now()
        response = None
        try:
            try:
                response = opener.open(request, timeout=limits.timeout_seconds)
            except urllib.error.HTTPError as error:
                response = error
            status = response.status if hasattr(response, "status") else response.code
            controls.response_status(response)
            resolved, resolved_redacted = resolve_metadata(response.geturl())
            media = response.headers.get("Content-Type") if not resolved_redacted else response.headers.get_content_type()
            length = response.headers.get("Content-Length")
            retry_after = response.headers.get("Retry-After")
            if resolved_redacted and length is not None and not length.isdigit():
                length = None
                failure = "invalid_content_length"
            if resolved_redacted and status != 200:
                failure = f"http_{status}"
            format_media = {
                "pdf": {"application/pdf"},
                "xls": {"application/vnd.ms-excel"},
                "txt": {"text/plain"},
                "csv": {"text/csv", "application/csv", "text/plain"},
                "xlsx": {
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "application/vnd.ms-excel.sheet.macroenabled.12",
                    "application/vnd.ms-excel",
                },
                "docx": {"application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
                "zip": {"application/zip"},
            }
            permitted_media = {"application/octet-stream", "binary/octet-stream"} | format_media.get(expected_format, set())
            if resolved_redacted and not failure and (media or "").lower() not in permitted_media:
                failure = "unexpected_signed_response_media_type"
            disposition = Message()
            disposition["Content-Disposition"] = "" if resolved_redacted else response.headers.get("Content-Disposition", "")
            original_name = disposition.get_filename()
            if original_name is None:
                last_segment = urllib.parse.unquote(urllib.parse.urlsplit(resolved).path.rsplit("/", 1)[-1])
                original_name = last_segment if "." in last_segment else None
            with path.open("xb") as handle:
                # Signed error bodies may echo request credentials; retain status only, never their bytes.
                while not failure:
                    if time.monotonic() - started >= limits.max_seconds:
                        raise TimeoutError("capture deadline")
                    block = controls.read(response, min(1024 * 1024, limits.max_bytes - count + 1))
                    if not block:
                        break
                    if count + len(block) > limits.max_bytes:
                        allowed = block[: limits.max_bytes - count]
                        handle.write(allowed)
                        digest.update(allowed)
                        count += len(allowed)
                        failure, partial = "size_limit", True
                        break
                    handle.write(block)
                    digest.update(block)
                    count += len(block)
            if not failure and (status == 206 or response.headers.get("Content-Range")):
                failure, partial = "partial_http_response", True
            if not failure and status != 200:
                failure = f"http_{status}"
            if not failure and response.headers.get("Content-Encoding", "identity").lower() != "identity":
                failure = "encoded_response_requires_review"
            if not failure and length is not None and (not length.isdigit() or int(length) != count):
                failure, partial = "content_length_mismatch", True
            if not failure:
                failure = inspect_payload(path, expected_format, role)
        except http.client.IncompleteRead as error:
            block = error.partial[: max(0, limits.max_bytes - count)]
            with path.open("ab") as handle:
                handle.write(block)
            digest.update(block)
            count += len(block)
            failure, partial = "incomplete_read", True
        except (OSError, urllib.error.URLError, CaptureError) as error:
            failure, partial = failure_code(error), count > 0
        finally:
            if response is not None:
                response.close()
        if not path.exists():
            path.touch(exist_ok=False)
        last = Download(
            path,
            url,
            resolved,
            retrieved,
            status,
            count,
            digest.hexdigest(),
            media,
            failure is None,
            failure,
            partial,
            attempt,
            original_name,
            resolved_redacted,
        )
        with (directory / "transport.json").open("x", encoding="utf-8") as handle:
            metadata = json.dumps({**asdict(last), "path": file_name, "content_length": length, "request_body": body}, indent=2)
            controls.writing(directory / "transport.json", len(metadata.encode()))
            handle.write(metadata)
        retryable = status in {408, 429, 500, 502, 503, 504} or failure in {"URLError", "TimeoutError", "ConnectionResetError", "incomplete_read"}
        if not retryable or attempt == limits.attempts:
            break
        delay = min(2 ** (attempt - 1), limits.max_retry_delay)
        if retry_after is not None:
            if not retry_after.isdigit() or int(retry_after) > limits.max_retry_delay:
                break
            delay = max(delay, int(retry_after))
        if time.monotonic() - started + delay >= limits.max_seconds:
            break
        time.sleep(delay)
    if last is None:
        raise CaptureError("No capture attempt was made.")
    return last
