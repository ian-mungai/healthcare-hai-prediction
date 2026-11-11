"""Keyed pseudonymization of a CSV that names individuals, and its derivative admission contract.

Each row of the original gives exactly one row of the derivative. Direct identifier columns are dropped. Each person becomes a keyed
pseudonym: HMAC-SHA-256 over a documented normalization of an authoritative person identifier, or of the name parts when the source has no identifier.
A name pseudonym supports matching and does not prove that two records name the same person. Pseudonymization is not anonymization.

The key is read from Parameter Store at use, as text, held in memory and never written, logged or printed. Tests pass a synthetic key reader.
verify admits a derivative only against the contract digest that the transformation run reported and the owner kept outside the derivative folder.
run_pseudonymize_e2e.py verifies every behavior with synthetic inputs. Errors carry one fixed code and never a value, a name or a key.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json
from scripts.pseudonym_core import (
    CLASSIFICATION,
    CLIENT_OPTIONS,
    COMPARED,
    CONTRACT_NAME,
    CONTRACT_VERSION,
    DATA_NAME,
    HEX16,
    HEX64,
    KEY_VERSION,
    KINDS,
    MAX_CELL_CHARACTERS,
    NAME_PATTERN,
    NORMALIZATION_VERSION,
    PARAMETER_PREFIX,
    PSEUDONYM_FORMAT,
    REPLACEMENT,
    SOURCE_ID,
    KeyReader,
    PseudonymizationError,
    convert_row,
    key_check,
    load_key,
    new_statistics,
    open_parameter_client,
    output_headers,
    parameter_store_reader,
    require,
    scan_rows,
    selects,
    validate_policy,
)

# The names the derivative tool's callers and its E2E reach through this module.
__all__ = [
    "CLASSIFICATION",
    "CLIENT_OPTIONS",
    "CONTRACT_NAME",
    "DATA_NAME",
    "KeyReader",
    "PARAMETER_PREFIX",
    "REPLACEMENT",
    "PseudonymizationError",
    "parameter_store_reader",
    "read_policy",
    "transform",
    "verify_derivative",
    "comparable",
]


def read_policy(path: Path) -> dict:
    """Read and validate a policy file."""
    try:
        policy = read_json(path)
    except (OSError, ValueError, RuntimeError):
        raise PseudonymizationError("policy_unreadable") from None
    validate_policy(policy)
    return policy


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
    headers = policy["headers"]
    scan = scan_rows(policy, rows(source, headers, policy["max_bytes"]))
    statistics, seen = new_statistics(policy)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pseudonymize_", dir=output.parent) as directory:
        staging = Path(directory)
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(output_headers(policy))
        for row in rows(source, headers, policy["max_bytes"]):
            writer.writerow(convert_row(policy, key, key_version, row, scan, statistics, seen))
        require(statistics["rows"] == scan.count and fingerprint(source) == source_print, "input_changed_during_run")
        statistics["distinct"] = {name: len(found) for name, found in seen.items()}
        write_once(staging / DATA_NAME, buffer.getvalue().encode())
        contract = contract_body(policy, key_version, key, source_print, fingerprint(staging / DATA_NAME), statistics)
        write_once(staging / CONTRACT_NAME, encoded_json(contract))
        if output.exists():
            check_existing(output, staging, key_version, contract["key_check"])
        else:
            os.rename(staging, output)
        return contract


def contract_shape(policy: dict) -> dict:
    """The complete closed schema of a contract for this policy: exact fields, types and fixed values; a type stands for a non-negative integer."""
    return {
        "kind": "pseudonymized_csv_contract",
        "contract_version": CONTRACT_VERSION,
        "derivative_policy": {
            "policy_id": NAME_PATTERN,
            "policy_version": 1,
            "policy_sha256": HEX64,
            "source_id": SOURCE_ID,
            "classification": CLASSIFICATION,
        },
        "normalization_version": NORMALIZATION_VERSION,
        "key_version": KEY_VERSION,
        "key_check": HEX16,
        "input": {"sha256": HEX64, "byte_count": int, "headers": list(policy["headers"]), "row_count": int},
        "output": {"sha256": HEX64, "byte_count": int, "headers": output_headers(policy), "row_count": int},
        "dropped_columns": [h for h in policy["headers"] if policy["treatments"][h]["treatment"] == "drop"],
        "conditionally_blanked_columns": [h for h in policy["headers"] if "blank_when" in policy["treatments"][h]],
        "pseudonyms": [
            {"output": r["output"], "namespace": r["namespace"], "match_strength": KINDS[r["kind"]], "written": int, "withheld": int, "distinct": int}
            for r in policy["pseudonyms"]
        ],
        "scrubbed_cells": int,
        "blanked_cells": int,
        "name_pseudonyms_prove_identity": False,
        "pseudonymization_is_anonymization": False,
        "lookup_table_written": False,
        "model_eligible": False,
        "contract_sha256": HEX64,
    }


def shape_ok(value: Any, spec: Any) -> bool:
    """True when the value has exactly the shape: objects with the same fields, lists of the same length, patterns, non-negative integers or equal constants."""
    if isinstance(spec, dict):
        return isinstance(value, dict) and set(value) == set(spec) and all(shape_ok(value[k], v) for k, v in spec.items())
    if isinstance(spec, list):
        return isinstance(value, list) and len(value) == len(spec) and all(shape_ok(v, s) for v, s in zip(value, spec, strict=True))
    if isinstance(spec, re.Pattern):
        return isinstance(value, str) and spec.fullmatch(value) is not None
    if spec is int:
        return type(value) is int and value >= 0
    return type(value) is type(spec) and value == spec


def read_derivative_table(directory: Path) -> list[list[str]]:
    """The rows of the data file, header first, read strictly."""
    try:
        with (directory / DATA_NAME).open(newline="", encoding="utf-8") as handle:
            return list(csv.reader(handle, strict=True))
    except (OSError, UnicodeDecodeError, csv.Error):
        raise PseudonymizationError("derivative_unreadable") from None


def check_conditional_rows(policy: dict, header: list[str], table: list[list[str]]) -> None:
    """On rows a condition selects, its blanked columns are empty; on other rows its pseudonym is empty; no row holds an unapproved value."""
    position = {name: i for i, name in enumerate(header)}
    checks = [(t["blank_when"], column, True) for column, t in policy["treatments"].items() if "blank_when" in t]
    checks += [(r["when"], r["output"], False) for r in policy["pseudonyms"] if "when" in r]
    for condition, column, blanked in checks:
        if condition["column"] not in position:
            continue
        for row in table:
            state = selects(condition, row[position[condition["column"]]])
            require(state is not None, "conditional_rows_differ")
            cell = row[position[column]]
            require(state is not True or cell == "" if blanked else state is True or cell == "", "conditional_rows_differ")


def verify_derivative(directory: Path, policy: dict, trusted_sha256: str) -> dict:
    """Admit a stored derivative only when its contract is the trusted one and everything it and the data claim holds, without the original or the key."""
    validate_policy(policy)
    require(isinstance(trusted_sha256, str) and HEX64.fullmatch(trusted_sha256) is not None, "invalid_hash_pin")
    require(directory.is_dir() and {p.name for p in directory.iterdir()} == {DATA_NAME, CONTRACT_NAME}, "derivative_files_differ")
    try:
        contract = json.loads((directory / CONTRACT_NAME).read_text())
    except (OSError, ValueError):
        raise PseudonymizationError("contract_unreadable") from None
    require(shape_ok(contract, contract_shape(policy)), "contract_schema_differs")
    require(contract["contract_sha256"] == canonical_hash({k: v for k, v in contract.items() if k != "contract_sha256"}), "contract_hash_differs")
    require(contract["contract_sha256"] == trusted_sha256, "trusted_digest_differs")
    require(contract["derivative_policy"]["policy_sha256"] == canonical_hash(policy), "policy_differs")
    require(fingerprint(directory / DATA_NAME) == (contract["output"]["sha256"], contract["output"]["byte_count"]), "derivative_hash_differs")
    table = read_derivative_table(directory)
    header, body = (table[0] if table else []), table[1:]
    require(header == output_headers(policy), "derivative_schema_differs")
    require(all(len(row) == len(header) for row in body), "row_width_differs")
    require(len(body) == contract["output"]["row_count"] == contract["input"]["row_count"], "row_count_differs")
    version = contract["key_version"]
    for rule, entry in zip(policy["pseudonyms"], contract["pseudonyms"], strict=True):
        values = [row[header.index(rule["output"])] for row in body]
        found = [v for v in values if v]
        require(all(PSEUDONYM_FORMAT.fullmatch(v) is not None and v.startswith(version + ":") for v in found), "pseudonym_format_differs")
        counts = (len(found), len(values) - len(found), len(set(found)))
        require(counts == (entry["written"], entry["withheld"], entry["distinct"]), "contract_counts_differ")
    check_conditional_rows(policy, header, body)
    return contract


def comparable(first: dict, second: dict) -> list[str]:
    """The contract fields that differ between two derivatives: equal policy, normalization and key are needed before their hashes can compare."""
    return [field for field in COMPARED if first[field] != second[field]]


def run(args: argparse.Namespace) -> dict:
    """Run one command-line action."""
    policy = read_policy(args.policy)
    if args.action == "verify":
        return verify_derivative(args.output, policy, args.expect_contract_sha256)
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
    check.add_argument("--expect-contract-sha256", required=True)
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
        "contract_sha256": result["contract_sha256"],
    }
    sys.stdout.write(json.dumps(summary) + "\n")


if __name__ == "__main__":
    main()
