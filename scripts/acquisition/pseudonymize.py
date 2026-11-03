"""Keyed pseudonymization of a CSV that names individuals, and its derivative admission contract.

Each row of the original gives exactly one row of the derivative. Direct identifier columns are dropped. Each person becomes a keyed
pseudonym: HMAC-SHA-256 over a documented normalization of an authoritative person identifier, or of the name parts when the source has no identifier.
A name pseudonym supports matching and does not prove that two records name the same person. Pseudonymization is not anonymization.

The key is read from Parameter Store at use, as text, held in memory and never written, logged or printed. Tests pass a synthetic key reader.
run_pseudonymize_e2e.py verifies every behavior with synthetic inputs. Errors carry one fixed code and never a value, a name or a key.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import csv
import hashlib
import hmac
import io
import json
import logging
import os
import re
import sys
import tempfile
import unicodedata
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json

CONTRACT_VERSION = 1
NORMALIZATION_VERSION = "norm_v1"
CLASSIFICATION = "public_pseudonymized_derivative"
CONTRACT_NAME = "pseudonymization_contract.json"
DATA_NAME = "data.csv"
REPLACEMENT = "[REDACTED]"
PARAMETER_PREFIX = "/healthcare_hai_prediction/pseudonym_key_"
KEY_BYTES = 32
KEY_CHECK_LABEL = b"hai-pseudonym/key-check/" + NORMALIZATION_VERSION.encode()
DOMAIN = b"hai-pseudonym"
SEPARATOR = b"\x1f"
MAX_CELL_CHARACTERS = 4096
MAX_BYTES_LIMIT = 256 * 1024**2
KEY_VERSION = re.compile(r"v[1-9][0-9]{0,2}")
NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}")
PSEUDONYM_FORMAT = re.compile(r"v[1-9][0-9]{0,2}:[0-9a-f]{64}")
NO_IDENTITY = frozenset({"", "-", "n/a", "na", "none", "null", "unknown", "not available", "not applicable", "[redacted]"})
NAME_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv"})
APOSTROPHES = dict.fromkeys(map(ord, "'‘’ʼ`´."), None)
TREATMENTS = ("keep", "drop", "scrub_names")
KINDS = {"id": "authoritative_id", "name": "name_match_only"}
POLICY_FIELDS = {"policy_id", "policy_version", "source_id", "normalization_version", "headers", "treatments", "pseudonyms", "max_bytes", "min_rows"}
CLIENT_OPTIONS: dict[str, Any] = {
    "retries": {"total_max_attempts": 2, "mode": "standard"},
    "connect_timeout": 5,
    "read_timeout": 10,
    "ignore_configured_endpoint_urls": True,
}
KeyReader = Callable[[str], str | None]


class PseudonymizationError(ValueError):
    """Contains only a fixed code, never a source value, a name or a key."""


def require(condition: bool, code: str) -> None:
    """Raise the module error with the fixed code when the condition is false."""
    if not condition:
        raise PseudonymizationError(code)


def is_text_list(value: Any) -> bool:
    """True for a non-empty list of distinct non-empty strings."""
    return isinstance(value, list) and bool(value) and all(isinstance(v, str) and bool(v) for v in value) and len(set(value)) == len(value)


def validate_condition(condition: Any, headers: list[str]) -> None:
    """A condition names one header and the exact cell value that selects a row."""
    require(isinstance(condition, dict) and set(condition) == {"column", "equals"}, "policy_invalid")
    require(condition["column"] in headers and isinstance(condition["equals"], str), "policy_invalid")


def source_removed(treatment: dict, rule: dict) -> bool:
    """True when a pseudonym source column leaves the derivative: dropped, or blanked on exactly the rows the pseudonym covers."""
    return treatment["treatment"] == "drop" or (rule.get("when") is not None and treatment.get("blank_when") == rule["when"])


def validate_policy(policy: dict) -> None:
    """Reject a policy that is not closed, complete and fail-closed: every source column needs a reviewed treatment."""
    require(isinstance(policy, dict) and set(policy) == POLICY_FIELDS and policy["policy_version"] == 1, "policy_invalid")
    require(isinstance(policy["policy_id"], str) and NAME_PATTERN.fullmatch(policy["policy_id"]) is not None, "policy_invalid")
    require(isinstance(policy["source_id"], str) and NAME_PATTERN.fullmatch(policy["source_id"].lower()) is not None, "policy_invalid")
    require(policy["normalization_version"] == NORMALIZATION_VERSION, "unsupported_normalization")
    headers = policy["headers"]
    require(is_text_list(headers), "policy_invalid")
    require(type(policy["max_bytes"]) is int and 0 < policy["max_bytes"] <= MAX_BYTES_LIMIT, "policy_invalid")
    require(type(policy["min_rows"]) is int and policy["min_rows"] >= 0, "policy_invalid")
    treatments = policy["treatments"]
    require(isinstance(treatments, dict) and set(treatments) == set(headers), "policy_invalid")
    for rule in treatments.values():
        require(isinstance(rule, dict) and {"treatment"} <= set(rule) <= {"treatment", "blank_when"} and rule["treatment"] in TREATMENTS, "policy_invalid")
        if "blank_when" in rule:
            validate_condition(rule["blank_when"], headers)
            require(rule["treatment"] != "drop", "policy_invalid")
    pseudonyms = policy["pseudonyms"]
    require(isinstance(pseudonyms, list) and bool(pseudonyms), "policy_invalid")
    outputs = set()
    for rule in pseudonyms:
        require(
            isinstance(rule, dict)
            and {"output", "kind", "namespace", "columns"} <= set(rule) <= {"output", "kind", "namespace", "columns", "when", "middle_column"},
            "policy_invalid",
        )
        require(isinstance(rule["output"], str) and NAME_PATTERN.fullmatch(rule["output"]) is not None, "policy_invalid")
        require(rule["output"] not in headers and rule["output"] not in outputs, "policy_invalid")
        outputs.add(rule["output"])
        require(rule["kind"] in KINDS and isinstance(rule["namespace"], str) and NAME_PATTERN.fullmatch(rule["namespace"]) is not None, "policy_invalid")
        require(is_text_list(rule["columns"]) and set(rule["columns"]) <= set(headers), "policy_invalid")
        require(rule["kind"] == "name" or len(rule["columns"]) == 1, "policy_invalid")
        require(all(source_removed(treatments[c], rule) for c in rule["columns"]), "pseudonym_source_not_dropped")
        if "when" in rule:
            validate_condition(rule["when"], headers)
        if "middle_column" in rule:
            require(rule["kind"] == "name" and rule["middle_column"] in headers and source_removed(treatments[rule["middle_column"]], rule), "policy_invalid")


def read_policy(path: Path) -> dict:
    """Read and validate a policy file."""
    try:
        policy = read_json(path)
    except (OSError, ValueError, RuntimeError):
        raise PseudonymizationError("policy_unreadable") from None
    validate_policy(policy)
    return policy


def base_text(value: str) -> str:
    """NFKC, control and space characters as one space, trimmed and case-folded."""
    spaced = "".join(" " if unicodedata.category(c) in {"Cc", "Zs", "Zl", "Zp"} else c for c in unicodedata.normalize("NFKC", value))
    return " ".join(spaced.split()).casefold()


def normalize_id(value: str) -> str:
    """An authoritative identifier: base text without any space; leading zeros and every other character stay."""
    text = base_text(value)
    return "" if text in NO_IDENTITY else text.replace(" ", "")


def normalize_name(value: str) -> str:
    """A name part: base text without accents and apostrophes, other punctuation as a space, generational suffixes dropped when a word remains."""
    text = base_text(value)
    if text in NO_IDENTITY:
        return ""
    stripped = "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)).translate(APOSTROPHES)
    words = "".join(c if c.isalnum() else " " for c in stripped).split()
    while len(words) > 1 and words[-1] in NAME_SUFFIXES:
        words.pop()
    return " ".join(words)


def normalized_parts(kind: str, values: list[str]) -> list[str] | None:
    """The normalized parts of one identity, or None when any part is empty and the identity cannot match."""
    parts = [normalize_id(v) if kind == "id" else normalize_name(v) for v in values]
    return parts if all(parts) else None


def derive(key: bytes, key_version: str, namespace: str, kind: str, parts: list[str]) -> str:
    """The pseudonym: the key version and the HMAC-SHA-256 of the length-checked, domain-separated message."""
    fields = [DOMAIN, NORMALIZATION_VERSION.encode(), namespace.encode(), kind.encode(), str(len(parts)).encode(), *(p.encode() for p in parts)]
    return f"{key_version}:{hmac.new(key, SEPARATOR.join(fields), hashlib.sha256).hexdigest()}"


def key_check(key: bytes) -> str:
    """A short value that identifies the key without revealing it: the first 16 hex digits of the HMAC of a fixed label."""
    return hmac.new(key, KEY_CHECK_LABEL, hashlib.sha256).hexdigest()[:16]


def load_key(version: str, reader: KeyReader) -> bytes:
    """Read the key text for one version through the reader and decode it; the key stays in memory."""
    require(isinstance(version, str) and KEY_VERSION.fullmatch(version) is not None, "key_version_invalid")
    text = reader(version)
    stripped = (text or "").strip()
    require(bool(stripped), "key_missing")
    standard = stripped.replace("-", "+").replace("_", "/")
    try:
        key = base64.b64decode(standard + "=" * (-len(standard) % 4), validate=True)
    except (binascii.Error, ValueError):
        raise PseudonymizationError("key_malformed") from None
    require(len(key) == KEY_BYTES, "key_malformed")
    return key


def parameter_store_reader(client: Any) -> KeyReader:
    """A reader over a Parameter Store client: one SecureString per key version, read with decryption at use."""

    def read(version: str) -> str | None:
        try:
            value = client.get_parameter(Name=PARAMETER_PREFIX + version, WithDecryption=True)["Parameter"]["Value"]
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ParameterNotFound":
                return None
            raise PseudonymizationError("key_read_failed") from None
        except (BotoCoreError, KeyError, TypeError):
            raise PseudonymizationError("key_read_failed") from None
        return value if isinstance(value, str) else None

    return read


def open_parameter_client(profile: str, region: str) -> Any:
    """A bounded Parameter Store client for the project's own profile and region."""
    require(bool(profile) and bool(region) and "admin" not in profile.lower(), "aws_settings_invalid")
    import boto3

    try:
        return boto3.Session(profile_name=profile, region_name=region).client("ssm", config=Config(**CLIENT_OPTIONS))
    except BotoCoreError:
        raise PseudonymizationError("aws_settings_invalid") from None


def matches(condition: dict | None, row: dict[str, str]) -> bool:
    """True when no condition is set or the row cell equals the condition value."""
    return condition is None or row[condition["column"]] == condition["equals"]


def rows(path: Path, headers: list[str], max_bytes: int) -> Iterator[dict[str, str]]:
    """Yield each row as a dictionary after checking the exact header, the row width and the cell size."""
    require(path.is_file() and not path.is_symlink(), "invalid_input_path")
    require(0 < path.stat().st_size <= max_bytes, "input_size_limit")
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.reader(handle, strict=True)
            require(next(reader, None) == headers, "schema_drift")
            for row in reader:
                require(len(row) == len(headers), "row_width_mismatch")
                require(all(len(cell) <= MAX_CELL_CHARACTERS for cell in row), "oversized_cell")
                yield dict(zip(headers, row, strict=True))
    except UnicodeDecodeError:
        raise PseudonymizationError("encoding_invalid") from None
    except csv.Error:
        raise PseudonymizationError("csv_malformed") from None


def person_words(policy: dict, row: dict[str, str]) -> Iterator[tuple[str, ...]]:
    """The name word sequences of the people one row names, for matching inside kept text cells."""
    for rule in policy["pseudonyms"]:
        if rule["kind"] != "name" or not matches(rule.get("when"), row):
            continue
        parts = normalized_parts("name", [row[c] for c in rule["columns"]])
        if parts is not None:
            words = tuple(" ".join(parts).split())
            yield words
            if rule.get("middle_column") and (middle := normalize_name(row[rule["middle_column"]])):
                yield (*" ".join(parts[:-1]).split(), *middle.split(), *parts[-1].split())


def contains_name(text: str, names: set[tuple[str, ...]], longest: int) -> bool:
    """True when the text holds any known person's whole name as consecutive words."""
    words = tuple(normalize_name(text).split())
    return any(words[i : i + n] in names for n in range(2, longest + 1) for i in range(len(words) - n + 1))


def output_headers(policy: dict) -> list[str]:
    """The derivative header: kept columns in source order, then one column per pseudonym rule."""
    return [h for h in policy["headers"] if policy["treatments"][h]["treatment"] != "drop"] + [r["output"] for r in policy["pseudonyms"]]


def contract_body(policy: dict, key_version: str, key: bytes, source: tuple[str, int], produced: tuple[str, int], statistics: dict) -> dict:
    """The admission contract: input and output hashes, policy, key version and the counts, without a timestamp or any value."""
    body = {
        "kind": "pseudonymized_csv_contract",
        "contract_version": CONTRACT_VERSION,
        "derivative_policy": {
            "policy_id": policy["policy_id"],
            "policy_version": policy["policy_version"],
            "policy_sha256": canonical_hash(policy),
            "source_id": policy["source_id"],
            "classification": CLASSIFICATION,
        },
        "normalization_version": NORMALIZATION_VERSION,
        "key_version": key_version,
        "key_check": key_check(key),
        "input": {"sha256": source[0], "byte_count": source[1], "headers": policy["headers"], "row_count": statistics["rows"]},
        "output": {"sha256": produced[0], "byte_count": produced[1], "headers": output_headers(policy), "row_count": statistics["rows"]},
        "dropped_columns": [h for h in policy["headers"] if policy["treatments"][h]["treatment"] == "drop"],
        "conditionally_blanked_columns": [h for h in policy["headers"] if "blank_when" in policy["treatments"][h]],
        "pseudonyms": [
            {
                "output": rule["output"],
                "namespace": rule["namespace"],
                "match_strength": KINDS[rule["kind"]],
                "written": statistics["written"][rule["output"]],
                "withheld": statistics["rows"] - statistics["written"][rule["output"]],
                "distinct": statistics["distinct"][rule["output"]],
            }
            for rule in policy["pseudonyms"]
        ],
        "scrubbed_cells": statistics["scrubbed"],
        "blanked_cells": statistics["blanked"],
        "name_pseudonyms_prove_identity": False,
        "pseudonymization_is_anonymization": False,
        "lookup_table_written": False,
        "model_eligible": False,
    }
    return {**body, "contract_sha256": canonical_hash(body)}


def check_existing(output: Path, staging: Path, key_version: str, check_value: str) -> None:
    """A rerun leaves an identical result alone and refuses any other content, naming a changed key under the same version."""
    require(output.is_dir() and {p.name for p in output.iterdir()} == {DATA_NAME, CONTRACT_NAME}, "conflicting_output")
    require(all(not (output / n).is_symlink() for n in (DATA_NAME, CONTRACT_NAME)), "conflicting_output")
    try:
        earlier = json.loads((output / CONTRACT_NAME).read_text())
        changed = earlier["key_version"] == key_version and earlier["key_check"] != check_value
    except (OSError, ValueError, KeyError, TypeError):
        changed = False
    require(not changed, "key_changed_under_same_version")
    require(all(fingerprint(output / n) == fingerprint(staging / n) for n in (DATA_NAME, CONTRACT_NAME)), "conflicting_output")


def transform(source: Path, output: Path, policy: dict, key_version: str, reader: KeyReader, expect_sha256: str | None = None) -> dict:
    """Write or verify the derivative of one CSV and return its admission contract; the key is read here, held in memory and never stored."""
    validate_policy(policy)
    require(expect_sha256 is None or re.fullmatch(r"[a-f0-9]{64}", expect_sha256) is not None, "invalid_hash_pin")
    require(source.is_file() and not source.is_symlink() and not output.is_symlink(), "invalid_input_path")
    source, output = source.resolve(), output.resolve()
    require(not source.is_relative_to(output), "input_inside_output")
    source_print = fingerprint(source)
    require(expect_sha256 is None or source_print[0] == expect_sha256, "input_hash_mismatch")
    key = load_key(key_version, reader)
    headers, treatments = policy["headers"], policy["treatments"]
    names: set[tuple[str, ...]] = set()
    count = 0
    for row in rows(source, headers, policy["max_bytes"]):
        count += 1
        names.update(words for words in person_words(policy, row) if len(words) > 1)
    require(count >= policy["min_rows"], "row_count_below_minimum")
    longest = max((len(n) for n in names), default=0)
    kept = [h for h in headers if treatments[h]["treatment"] != "drop"]
    statistics: dict[str, Any] = {
        "rows": 0,
        "scrubbed": 0,
        "blanked": 0,
        "written": {r["output"]: 0 for r in policy["pseudonyms"]},
        "distinct": {},
    }
    seen: dict[str, set[str]] = {r["output"]: set() for r in policy["pseudonyms"]}
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pseudonymize_", dir=output.parent) as directory:
        staging = Path(directory)
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(output_headers(policy))
        for row in rows(source, headers, policy["max_bytes"]):
            statistics["rows"] += 1
            values = []
            for column in kept:
                rule, cell = treatments[column], row[column]
                if "blank_when" in rule and matches(rule["blank_when"], row) and cell:
                    cell = ""
                    statistics["blanked"] += 1
                elif rule["treatment"] == "scrub_names" and names and cell and contains_name(cell, names, longest):
                    cell = REPLACEMENT
                    statistics["scrubbed"] += 1
                values.append(cell)
            for rule in policy["pseudonyms"]:
                parts = normalized_parts(rule["kind"], [row[c] for c in rule["columns"]]) if matches(rule.get("when"), row) else None
                pseudonym = "" if parts is None else derive(key, key_version, rule["namespace"], rule["kind"], parts)
                if pseudonym:
                    statistics["written"][rule["output"]] += 1
                    seen[rule["output"]].add(pseudonym)
                values.append(pseudonym)
            writer.writerow(values)
        require(statistics["rows"] == count and fingerprint(source) == source_print, "input_changed_during_run")
        statistics["distinct"] = {name: len(found) for name, found in seen.items()}
        write_once(staging / DATA_NAME, buffer.getvalue().encode())
        contract = contract_body(policy, key_version, key, source_print, fingerprint(staging / DATA_NAME), statistics)
        write_once(staging / CONTRACT_NAME, encoded_json(contract))
        if output.exists():
            check_existing(output, staging, key_version, contract["key_check"])
        else:
            os.rename(staging, output)
        return contract


def verify_derivative(directory: Path, policy: dict) -> dict:
    """Check a stored derivative and its contract against the policy, without the original or the key."""
    validate_policy(policy)
    require(directory.is_dir() and {p.name for p in directory.iterdir()} == {DATA_NAME, CONTRACT_NAME}, "derivative_files_differ")
    try:
        contract = json.loads((directory / CONTRACT_NAME).read_text())
        body = {k: v for k, v in contract.items() if k != "contract_sha256"}
        require(contract["contract_sha256"] == canonical_hash(body), "contract_hash_differs")
        require(contract["derivative_policy"]["policy_sha256"] == canonical_hash(policy), "policy_differs")
        require(contract["output"]["headers"] == output_headers(policy), "derivative_schema_differs")
        require(fingerprint(directory / DATA_NAME) == (contract["output"]["sha256"], contract["output"]["byte_count"]), "derivative_hash_differs")
        version = contract["key_version"]
        require(isinstance(version, str) and KEY_VERSION.fullmatch(version) is not None, "key_version_invalid")
        pseudonym_columns = [r["output"] for r in policy["pseudonyms"]]
        with (directory / DATA_NAME).open(newline="", encoding="utf-8") as handle:
            table = list(csv.DictReader(handle, strict=True))
    except PseudonymizationError:
        raise
    except (OSError, ValueError, KeyError, TypeError, csv.Error):
        raise PseudonymizationError("contract_unreadable") from None
    require(len(table) == contract["output"]["row_count"] == contract["input"]["row_count"], "row_count_differs")
    for row in table:
        for column in pseudonym_columns:
            value = row[column]
            require(value == "" or (PSEUDONYM_FORMAT.fullmatch(value) is not None and value.startswith(version + ":")), "pseudonym_format_differs")
    return contract


COMPARED = ("derivative_policy", "normalization_version", "key_version", "key_check")


def comparable(first: dict, second: dict) -> list[str]:
    """The contract fields that differ between two derivatives: equal policy, normalization and key are needed before their hashes can compare."""
    return [field for field in COMPARED if first[field] != second[field]]


def run(args: argparse.Namespace) -> dict:
    """Run one command-line action."""
    policy = read_policy(args.policy)
    if args.action == "verify":
        return verify_derivative(args.output, policy)
    require(bool(args.profile) and bool(args.region), "aws_settings_invalid")
    reader = parameter_store_reader(open_parameter_client(args.profile, args.region))
    return transform(args.input, args.output, policy, args.key_version, reader, args.expect_input_sha256)


def main(argv: list[str] | None = None) -> None:
    """Run the command-line workflow: transform an original owner-side, or verify a derivative."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    make = sub.add_parser("transform")
    make.add_argument("--policy", required=True, type=Path)
    make.add_argument("--input", required=True, type=Path)
    make.add_argument("--output", required=True, type=Path)
    make.add_argument("--key-version", required=True)
    make.add_argument("--profile", default="")
    make.add_argument("--region", default="")
    make.add_argument("--expect-input-sha256")
    check = sub.add_parser("verify")
    check.add_argument("--policy", required=True, type=Path)
    check.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        result = run(args)
    except (PseudonymizationError, OSError, TypeError, KeyError, ValueError) as error:
        code = str(error) if isinstance(error, PseudonymizationError) else type(error).__name__
        logging.getLogger(__name__).error(json.dumps({"status": "rejected", "reason": code}), extra={"status": "rejected", "reason": code})
        raise SystemExit(1) from None
    summary = {
        "status": "admitted" if args.action == "verify" else "pseudonymized",
        "row_count": result["output"]["row_count"],
        "output_sha256": result["output"]["sha256"],
    }
    sys.stdout.write(json.dumps(summary) + "\n")


if __name__ == "__main__":
    main()
