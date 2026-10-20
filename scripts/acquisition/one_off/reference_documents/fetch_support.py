"""Fetch named public files into a new local folder with type, header and SHA-256 checks and one write-once receipt; a rerun makes no requests."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.process import run_command
from scripts.acquisition.source_registry import REPO_ROOT
from scripts.acquisition.transport import CaptureError, Download, Limits, inspect_payload, utc_now
from scripts.process import TimeoutExpired

LIMITS = Limits(attempts=2, timeout_seconds=30, max_seconds=180, max_bytes=64 * 1024**2)
RECEIPT_NAME = "receipt.json"
DEADLINE_RECORD = "deadline.json"


class FetchError(ValueError):
    """A run that cannot start, a layout that differs from the expected one or a receipt that no longer matches its files or the requested rules."""


def media_base(value: str | None) -> str:
    """Return the media type without parameters, in lower case."""
    return (value or "").split(";")[0].strip().lower()


def file_sha256(path: Path) -> str:
    """Return the SHA-256 of a file read in blocks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def acceptance_reason(spec: dict[str, Any], media_type: str | None, path: Path, sha256: str) -> str | None:
    """Return why saved bytes fail the spec's media type, header and pin, or None; a fresh download and an offline recheck apply the same rules."""
    if media_base(media_type) not in spec["media_types"]:
        return "unexpected_media_type"
    magic = spec.get("magic")
    if magic is not None:
        with path.open("rb") as handle:
            if handle.read(len(magic)) != magic:
                return "unexpected_file_header"
    if spec.get("expected_sha256") not in (None, sha256):
        return "sha256_differs_from_pin"
    return None


def download_in_child(spec: dict[str, Any], captures: Path) -> Download:
    """Run one file's whole download (name lookup, connect, headers, body and retries) in its own process, ended after ``LIMITS.max_seconds``.

    Raises ``TimeoutExpired`` when the process outlives the deadline; the launcher has ended it by then.
    """
    completed = run_command(
        sys.executable,
        [
            "-m",
            "scripts.acquisition.one_off.reference_documents.fetch_child",
            "--url",
            spec["url"],
            "--file-name",
            spec["file_name"],
            "--format",
            spec["format"],
            "--captures",
            str(captures),
            "--attempts",
            str(LIMITS.attempts),
            "--timeout-seconds",
            str(LIMITS.timeout_seconds),
            "--max-seconds",
            str(LIMITS.max_seconds),
            "--max-bytes",
            str(LIMITS.max_bytes),
        ],
        cwd=REPO_ROOT,
        timeout=LIMITS.max_seconds,
    )
    if completed.returncode != 0:
        lines = completed.stderr.strip().splitlines()
        raise CaptureError(f"{spec['url']}: the download process exited {completed.returncode}: {lines[-1] if lines else 'no error text'}")
    try:
        fields = json.loads(completed.stdout)
        return Download(**{**fields, "path": Path(fields["path"])})
    except (ValueError, KeyError, TypeError) as error:
        raise CaptureError(f"{spec['url']}: the download process printed no usable result ({type(error).__name__})") from error


def deadline_entry(spec: dict[str, Any], captures: Path, started: str) -> dict[str, Any]:
    """Record a file whose download was ended at the deadline: its folder keeps the finished attempts and gets one ``deadline.json``; nothing is accepted."""
    folder = captures / spec["file_name"]
    folder.mkdir(parents=True, exist_ok=True)
    attempts = sum(1 for path in folder.glob("attempt_*") if path.is_dir())
    record = {
        "failure": "capture_deadline_exceeded",
        "url": spec["url"],
        "seconds": LIMITS.max_seconds,
        "attempts_started": attempts,
        "started_at_utc": started,
    }
    with (folder / DEADLINE_RECORD).open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return {
        "url": spec["url"],
        "file_name": spec["file_name"],
        "status": "rejected",
        "reason": "capture_deadline_exceeded",
        "http_status": None,
        "media_type": None,
        "bytes": None,
        "sha256": None,
        "retrieved_at_utc": started,
        "attempts": attempts,
        "path": folder.relative_to(captures.parent).as_posix(),
    }


def fetch_one(spec: dict[str, Any], captures: Path) -> dict[str, Any]:
    """Download one file within its total deadline and accept it only when transport, media type, header and any pinned hash agree; return its receipt entry."""
    captures = captures.resolve()
    started = utc_now()
    try:
        result = download_in_child(spec, captures)
    except TimeoutExpired:
        return deadline_entry(spec, captures, started)
    reason = result.failure
    if reason is None:
        reason = acceptance_reason(spec, result.media_type, result.path, result.sha256)
    return {
        "url": spec["url"],
        "file_name": spec["file_name"],
        "status": "accepted" if reason is None else "rejected",
        "reason": reason,
        "http_status": result.http_status,
        "media_type": result.media_type,
        "bytes": result.byte_count,
        "sha256": result.sha256,
        "retrieved_at_utc": result.retrieved_at_utc,
        "attempts": result.attempt,
        "path": result.path.relative_to(captures.parent).as_posix(),
    }


def recheck(receipt_path: Path, kind: str, specs: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a stored receipt after confirming it names the requested files and each accepted file keeps its hash and meets the requested rules."""
    receipt: dict[str, Any] = json.loads(receipt_path.read_text(encoding="utf-8"))
    stored = [(entry["url"], entry["file_name"]) for entry in receipt["files"]]
    if receipt.get("kind") != kind or stored != [(spec["url"], spec["file_name"]) for spec in specs]:
        raise FetchError(f"{receipt_path} does not match the requested files; use a new output folder")
    for entry, spec in zip(receipt["files"], specs, strict=True):
        if entry["status"] != "accepted":
            continue
        path = receipt_path.parent / entry["path"]
        if file_sha256(path) != entry["sha256"]:
            raise FetchError(f"{entry['file_name']} differs from its receipt")
        reason = acceptance_reason(spec, entry["media_type"], path, entry["sha256"]) or inspect_payload(path, spec["format"], "reference")
        if reason is not None:
            raise FetchError(f"{entry['file_name']} no longer satisfies the requested rules ({reason}); use a new output folder")
    return receipt


def run_fetch(kind: str, specs: list[dict[str, Any]], output: Path) -> tuple[dict[str, Any], bool]:
    """Fetch every spec into ``output`` and return the receipt and whether requests were made; a folder with a receipt is only rechecked."""
    receipt_path = output / RECEIPT_NAME
    if receipt_path.exists():
        return recheck(receipt_path, kind, specs), False
    if output.exists() and any(output.iterdir()):
        raise FetchError(f"{output} holds files but no receipt; use a new empty output folder")
    names = [spec["file_name"] for spec in specs]
    if len(set(names)) != len(names):
        raise FetchError("File names must be unique within one run")
    output.mkdir(parents=True, exist_ok=True)
    entries = [fetch_one(spec, output / "captures") for spec in specs]
    receipt = {
        "receipt_version": 1,
        "kind": kind,
        "created_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "files": entries,
        "all_accepted": all(entry["status"] == "accepted" for entry in entries),
    }
    with receipt_path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt, True
