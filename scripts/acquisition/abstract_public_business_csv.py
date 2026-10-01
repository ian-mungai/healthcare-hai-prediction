"""Fail-closed abstraction of a hash-pinned, explicitly approved public-business CSV."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import tempfile
import unicodedata
from collections.abc import Iterator
from pathlib import Path

from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json
from scripts.acquisition.transport import safe_url


class PrivacyError(ValueError):
    """Contains only a static diagnostic code, never a source value."""


def require(condition: bool, code: str) -> None:
    """Raise the module-specific validation error when the required condition is false."""
    if not condition:
        raise PrivacyError(code)


def normalized(value: str) -> str:
    """Return normalized text for transient, case-insensitive sensitive-value matching."""
    return " ".join(unicodedata.normalize("NFKC", value).split()).casefold()


def validate_policy(policy: dict, source_url: str) -> None:
    """Reject an unapproved CSV policy or one that differs from the requested source URL."""
    fields = {
        "policy_version",
        "approved",
        "source_url",
        "input_sha256",
        "max_bytes",
        "expected_headers",
        "expected_row_count",
        "redact_fields",
        "scrub_text_fields",
        "replacement",
        "s3_retention",
    }
    require(set(policy) == fields and policy["policy_version"] == 1 and policy["approved"] is True, "unapproved_policy")
    require(safe_url(source_url) == policy["source_url"], "unapproved_source_route")
    require(bool(re.fullmatch(r"[a-f0-9]{64}", policy["input_sha256"])), "invalid_hash_pin")
    require(type(policy["max_bytes"]) is int and 0 < policy["max_bytes"] <= 256 * 1024**2, "invalid_size_bound")
    require(type(policy["expected_row_count"]) is int and policy["expected_row_count"] > 0, "invalid_row_count")
    require(policy["replacement"] == "[REDACTED]", "unapproved_replacement")
    require(policy["s3_retention"] in {"abstracted_only", "raw_private_and_abstracted"}, "unapproved_retention")
    for key in ["expected_headers", "redact_fields", "scrub_text_fields"]:
        values = policy[key]
        require(isinstance(values, list) and bool(values) and all(isinstance(v, str) and bool(v) for v in values), "invalid_fields")
        require(len(set(values)) == len(values), "duplicate_policy_field")
    headers, redacted, scrubbed = (set(policy[key]) for key in ["expected_headers", "redact_fields", "scrub_text_fields"])
    require(redacted <= headers and scrubbed <= headers and not redacted & scrubbed, "invalid_field_scope")


def rows(path: Path, headers: list[str]) -> Iterator[list[str]]:
    """Yield CSV rows with the exact approved headers and column count."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle, strict=True)
        observed = next(reader, None)
        require(observed == headers and len(set(headers)) == len(headers), "schema_drift")
        for row in reader:
            require(len(row) == len(headers), "row_width_mismatch")
            yield row


def abstract(source: Path, output: Path, policy: dict, source_url: str) -> dict:
    """Create or verify an abstracted CSV and return its immutable ingestion manifest."""
    validate_policy(policy, source_url)
    require(source.is_file() and not source.is_symlink(), "invalid_input_path")
    source = source.resolve()
    require(not output.is_symlink(), "invalid_output_path")
    output = output.resolve()
    require(not source.is_relative_to(output), "input_inside_output")
    raw_hash, raw_size = fingerprint(source)
    require(raw_hash == policy["input_sha256"], "input_hash_mismatch")
    require(0 < raw_size <= policy["max_bytes"], "input_size_limit")
    headers = policy["expected_headers"]
    redact_indexes = {headers.index(field): field for field in policy["redact_fields"]}
    scrub_indexes = {headers.index(field) for field in policy["scrub_text_fields"]}
    sensitive = set()
    counts = {field: 0 for field in policy["redact_fields"]}
    row_count = 0
    non_identity_tokens = {"", "n/a", "na", "none", "null", "unknown", "not available", "not applicable", "[redacted]"}
    for row in rows(source, headers):
        row_count += 1
        for index, field in redact_indexes.items():
            if row[index].strip():
                counts[field] += 1
                value = normalized(row[index])
                if value not in non_identity_tokens:
                    require(len(value) <= 4096, "oversized_sensitive_cell")
                    sensitive.add(value)
        require(len(sensitive) <= 100000, "sensitive_dictionary_limit")
    require(row_count == policy["expected_row_count"], "row_count_mismatch")
    # Transient value matching catches copied names/contact details without persisting an identity map.
    pattern = re.compile(r"(?<!\w)(?:" + "|".join(re.escape(v) for v in sorted(sensitive, key=lambda v: (-len(v), v))) + r")(?!\w)") if sensitive else None
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="privacy_ingestion_", dir=output.parent) as temp:
        staging = Path(temp)
        destination = staging / "data.csv"
        cross_field_count = 0
        cache: dict[str, str] = {}
        written = 0
        with destination.open("x", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(headers)
            for row in rows(source, headers):
                for index in redact_indexes:
                    row[index] = policy["replacement"] if row[index].strip() else row[index]
                for index in scrub_indexes:
                    original = row[index]
                    if original not in cache:
                        value = normalized(original)
                        cache[original] = pattern.sub(policy["replacement"], value) if pattern and pattern.search(value) else original
                    row[index] = cache[original]
                    cross_field_count += row[index] != original
                writer.writerow(row)
                written += 1
        require(written == row_count and fingerprint(source) == (raw_hash, raw_size), "input_changed_during_ingestion")
        output_hash, output_size = fingerprint(destination)
        manifest = {
            "kind": "pii_abstracted_csv",
            "policy_version": 1,
            "policy_sha256": canonical_hash(policy),
            "source_url": source_url,
            "raw_sha256": raw_hash,
            "raw_byte_count": raw_size,
            "output_sha256": output_hash,
            "output_byte_count": output_size,
            "row_count": row_count,
            "headers": headers,
            "redacted_field_nonblank_counts": counts,
            "cross_field_redacted_cells": cross_field_count,
            "replacement": policy["replacement"],
            "s3_retention": policy["s3_retention"],
            "original_unchanged": False,
            "raw_contains_personal_information": True,
            "unreviewed_free_text_pii_possible": True,
            "model_eligible": False,
            "identity_hashes_or_lookup_written": False,
            "transformations": [
                "Fixed-marker field abstraction; empty/whitespace-only cells retained.",
                "Known sensitive values matched in reviewed text fields with NFKC, whitespace and case normalization.",
                "Matched text cells normalized; other non-sensitive cell tokens and row order unchanged.",
                "CSV serialized as UTF-8 without BOM and with LF line endings.",
            ],
            "remaining_checks": [
                "free_text_privacy_review",
                "dictionary_applicability",
                "clinical_definitions",
                "measurement_intervals",
                "hospital_identity_and_joins",
                "model_eligibility",
            ],
        }
        write_once(staging / "ingestion_manifest.json", encoded_json(manifest))
        if output.exists():
            require(output.is_dir() and {p.name for p in output.iterdir()} == {"data.csv", "ingestion_manifest.json"}, "conflicting_output")
            for name in ["data.csv", "ingestion_manifest.json"]:
                require(not (output / name).is_symlink() and fingerprint(output / name) == fingerprint(staging / name), "conflicting_output")
        else:
            # The whole validated output appears atomically, never a partial CSV.
            os.rename(staging, output)
        return manifest


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", required=True, type=Path)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        result = abstract(args.input, args.output, read_json(args.policy), args.source_url)
    except (ValueError, OSError, TypeError, KeyError, csv.Error) as error:
        code = str(error) if isinstance(error, PrivacyError) else type(error).__name__
        logging.getLogger(__name__).error(json.dumps({"status": "rejected", "reason": code}), extra={"status": "rejected", "reason": code})
        raise SystemExit(1) from None
    sys.stdout.write(
        str(
            json.dumps(
                {"status": "abstracted_unvalidated", "row_count": result["row_count"], "output_sha256": result["output_sha256"], "model_eligible": False}
            )
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
