"""Audit frozen capture receipts and profile remaining sources without changing their bytes.

Outputs contain structural metadata and counts, never arbitrary data values. Tabular
profiles are explicitly bounded samples, distinct from a complete semantic review.
"""

from __future__ import annotations

import argparse
import codecs
import csv
import hashlib
import io
import json
import logging
import re
import sys
import zipfile
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

from scripts.review.profile_sources import NUMERIC

SAMPLE_ROWS = 5000
SENTINELS = {"-666666666", "-888888888", "-999999999", "-222222222", "-333333333", "-555555555"}
TOKENS = {"(X)", "(D)", "(S)", "(Z)", "-", "--", "***", "*****", "N/A", "NA", "Suppressed", "Unreliable", "Not Available"}
log = logging.getLogger(__name__)


def digest(path: Path) -> str:
    """Hash a complete local file with bounded memory."""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, value: Any) -> None:
    """Atomically replace deterministic JSON output."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n")
    temporary.replace(path)


def safe_path(root: Path, relative: str) -> Path:
    """Reject paths that escape the read-only evidence root."""
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("path_outside_source_root")
    return path


def value_counts(values: list[str], counts: list[Counter[str]]) -> None:
    """Accumulate only safe categories, without retaining arbitrary tokens."""
    for value, stat in zip(values, counts, strict=True):
        stat["observed"] += 1
        v = value.strip()
        if not v:
            stat["blank"] += 1
        elif v in SENTINELS:
            stat["numeric_sentinel"] += 1
        elif v in TOKENS:
            stat["special_token"] += 1
        elif NUMERIC.fullmatch(v):
            stat["numeric"] += 1
            if len(v) > 1 and v.startswith("0") and re.fullmatch(r"\d+", v):
                stat["leading_zero"] += 1
        else:
            stat["text"] += 1
            if re.fullmatch(r"-?\d[\d,]*(?:\.\d+)?[+-]", v):
                stat["threshold_or_topcode"] += 1


def delimited(handle: BinaryIO, suffix: str, declared_delimiter: str | None = None) -> dict[str, Any]:
    """Profile a bounded prefix and retain explicit sample limits."""
    prefix = handle.read(65536)
    encoding = "utf-8-sig"
    try:
        preview = codecs.getincrementaldecoder(encoding)().decode(prefix, final=False)
    except UnicodeDecodeError:
        encoding = "cp1252"
        preview = prefix.decode(encoding, errors="replace")
    if "\x00" in preview:
        return {"status": "pending_binary_or_encoding_review"}
    delimiter = declared_delimiter if declared_delimiter in {",", "\t", "|", ";"} else None
    if delimiter is None:
        try:
            delimiter = csv.Sniffer().sniff(preview, delimiters=",\t|;").delimiter
        except csv.Error:
            delimiter = "," if suffix == ".csv" else None
    if delimiter is None:
        return {"status": "pending_fixed_width_or_free_text_review", "encoding": encoding}
    handle.seek(0)
    wrapper = io.TextIOWrapper(handle, encoding=encoding, errors="replace", newline="")
    try:
        reader = csv.reader(wrapper, delimiter=delimiter)
        header = next(reader, [])
        stats: list[Counter[str]] = [Counter() for _ in header]
        rows = ragged = replacements = 0
        for values in reader:
            if rows >= SAMPLE_ROWS:
                break
            rows += 1
            replacements += sum("\ufffd" in v for v in values)
            if len(values) != len(header):
                ragged += 1
                continue
            value_counts(values, stats)
        return {
            "status": "sampled_delimited_structure",
            "encoding": encoding,
            "delimiter": delimiter,
            "columns": [{"name": name, "counts": dict(stat)} for name, stat in zip(header, stats, strict=True)],
            "sample_rows": rows,
            "sample_limit": SAMPLE_ROWS,
            "sample_ragged_rows": ragged,
            "sample_replacement_cells": replacements,
            "duplicate_header_names": len(header) - len(set(header)),
            "limits": "First 5000 data records only; no complete row/grain/geography proof. Header may be a title or native metadata row.",
        }
    finally:
        wrapper.detach()


def container_profile(path: Path) -> dict[str, Any]:
    """Inspect archive inventory without extraction or arbitrary content values."""
    with zipfile.ZipFile(path) as archive:
        infos = [i for i in archive.infolist() if not i.is_dir()]
        extensions = Counter(PurePosixPath(i.filename).suffix.lower() for i in infos)
        unsafe = sum(PurePosixPath(i.filename).is_absolute() or ".." in PurePosixPath(i.filename).parts for i in infos)
        return {
            "status": "container_metadata_only",
            "member_count": len(infos),
            "member_extensions": dict(sorted(extensions.items())),
            "uncompressed_bytes": sum(i.file_size for i in infos),
            "unsafe_member_names": unsafe,
            "encrypted_members": sum(bool(i.flag_bits & 1) for i in infos),
            "limits": "No extraction, member checksum verification or content profiling; expanded stored members may be separate captures.",
        }


def inspect_file(path: Path, declared_delimiter: str | None) -> dict[str, Any]:
    """Inspect magic bytes before trusting extensions or media types."""
    with path.open("rb") as handle:
        magic = handle.read(4096).lstrip()
    suffix = path.suffix.lower()
    if magic.lower().startswith((b"<!doctype html", b"<html")):
        return {"status": "html_content", "extension_mismatch": suffix not in {".html", ".htm"}}
    if suffix == ".pdf":
        return {"status": "pending_pdf_content_review", "pdf_signature": magic.startswith(b"%PDF-"), "extension_mismatch": not magic.startswith(b"%PDF-")}
    if suffix in {".zip", ".xlsx", ".xlsm", ".docx"}:
        result = container_profile(path)
        result["container_type"] = suffix
        return result
    if suffix == ".xls":
        return {"status": "pending_legacy_excel_content_review", "ole_signature": magic.startswith(bytes.fromhex("d0cf11e0a1b11ae1"))}
    if suffix in {".csv", ".tsv", ".txt", ".dat"}:
        with path.open("rb") as handle:
            return delimited(handle, suffix, declared_delimiter)
    return {"status": "pending_unsupported_format", "extension": suffix}


def audit_artifact(root: Path, receipt_path: Path, artifact: dict[str, Any], schema: dict[str, Any], cache: dict[str, Any]) -> dict[str, Any]:
    """Validate complete bytes, then inspect their structure without exposing row values."""
    relative = str((receipt_path.parent / artifact["storage_path"]).relative_to(root))
    result: dict[str, Any] = {"path": relative, "role": artifact["role"], "sha256": artifact["sha256"], "media_type": artifact.get("media_type")}
    path = safe_path(root, relative)
    if not path.is_file():
        return {**result, "integrity": "missing"}
    observed_size = path.stat().st_size
    observed_sha = digest(path)
    valid = observed_sha == artifact["sha256"] and observed_size == artifact.get("byte_count", observed_size)
    result.update({"integrity": "verified" if valid else "mismatch", "bytes": observed_size, "observed_sha256": observed_sha})
    if valid:
        key = f"{observed_sha}:{path.suffix.lower()}:{schema.get('delimiter')}"
        if key not in cache:
            try:
                cache[key] = inspect_file(path, schema.get("delimiter"))
            except (OSError, ValueError, csv.Error, zipfile.BadZipFile, UnicodeError) as error:
                cache[key] = {"status": "parse_failed", "error_type": type(error).__name__}
        result["content"] = cache[key]
    return result


def audit(root: Path, inventory_path: Path, output: Path) -> dict[str, Any]:
    """Audit every candidate receipt and every referenced artifact, with explicit exclusions."""
    inventory = json.loads(inventory_path.read_text())
    candidates = inventory["candidates"]
    sources: dict[str, list[dict[str, Any]]] = {}
    excluded: Counter[str] = Counter()
    cache: dict[str, Any] = {}
    for index, candidate in enumerate(candidates):
        sid = candidate["source_id"]
        mmd_reviewed = sid == "MMD" and "/references/" not in candidate["receipt"]
        if mmd_reviewed or sid == "BLS" or "census_acs_api_history/20260926/" in candidate["receipt"]:
            excluded[sid] += 1
            continue
        receipt_path = safe_path(root, candidate["receipt"])
        entry: dict[str, Any] = {"receipt": candidate["receipt"], "receipt_sha256": candidate["receipt_sha256"]}
        sources.setdefault(sid, []).append(entry)
        if not receipt_path.is_file() or digest(receipt_path) != candidate["receipt_sha256"]:
            entry["status"] = "receipt_missing_or_drifted"
            continue
        receipt_bytes = receipt_path.read_bytes()
        if hashlib.sha256(receipt_bytes).hexdigest() != candidate["receipt_sha256"]:
            entry["status"] = "receipt_drifted_during_read"
            continue
        receipt = json.loads(receipt_bytes)
        pinned = output / "pinned_receipts" / f"{candidate['receipt_sha256']}.json"
        pinned.parent.mkdir(parents=True, exist_ok=True)
        temporary = pinned.with_suffix(".tmp")
        temporary.write_bytes(receipt_bytes)
        temporary.replace(pinned)
        schema = receipt.get("schema_profile", {})
        entry.update(
            {
                "status": "receipt_verified",
                "periods": receipt.get("measurement_periods", []),
                "release": receipt.get("release", {}),
                "recorded_schema": schema,
                "quality_checks_failed": receipt.get("quality_profile", {}).get("checks_failed", []),
                "artifacts": [audit_artifact(root, receipt_path, a, schema, cache) for a in receipt.get("artifacts", [])],
            }
        )
        if index % 50 == 0:
            log.info("Processed candidate %d of %d", index + 1, len(candidates))
    profiles = []
    for sid, entries in sorted(sources.items()):
        entries.sort(key=lambda e: e["receipt"])
        artifacts = [a for e in entries for a in e.get("artifacts", [])]
        data = [a for a in artifacts if a["role"] == "data"]
        profile = {
            "source_id": sid,
            "capture_candidates": len(entries),
            "model_eligible": False,
            "review_status": "preliminary_structure_and_metadata_checked; semantic_review_pending",
            "integrity": dict(Counter(a["integrity"] for a in artifacts)),
            "content_status": dict(Counter(a.get("content", {}).get("status", "not_profiled") for a in data)),
            "candidates": entries,
        }
        write_json(output / "sources" / f"{sid}.json", profile)
        profiles.append({k: v for k, v in profile.items() if k != "candidates"})
    report = {
        "version": 1,
        "inventory_sha256": digest(inventory_path),
        "code_sha256": digest(Path(__file__)),
        "numeric_classifier_sha256": digest(Path(__file__).with_name("profile_sources.py")),
        "candidate_total": len(candidates),
        "included_candidates": sum(len(v) for v in sources.values()),
        "excluded_candidates": dict(sorted(excluded.items())),
        "sources": profiles,
        "model_eligible": False,
        "limits": [
            "Offline local evidence only; no live S3 or publisher requests.",
            "Inventory contains capture candidates, including references and potentially superseded/retired evidence; not an authoritative live dataset list.",
            "Every referenced artifact checked in full by SHA-256 and size; delimited values sampled at first 5000 records.",
            "Archive/Office containers inventoried, not fully parsed; PDF and legacy Excel semantic review pending.",
            "No duplicate-grain, complete geography, source definition, leakage or eligibility clearance is inferred from this pass.",
        ],
    }
    write_json(output / "report.json", report)
    return report


def main() -> None:
    """Run the read-only source audit from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv.field_size_limit(16 << 20)
    report = audit(args.source_root.resolve(), args.inventory.resolve(), args.output.resolve())
    sys.stdout.write(json.dumps({"included_candidates": report["included_candidates"], "sources": len(report["sources"])}) + "\n")


if __name__ == "__main__":
    main()
