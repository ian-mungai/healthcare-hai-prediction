"""Validate immutable output and input identities against their broker invocation."""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_SCHEMA = "data_contracts/orchestration/snapshot_record.schema.json"
INPUT_SCHEMA = "data_contracts/orchestration/input_snapshot_record.schema.json"


class SnapshotError(ValueError):
    """Snapshot metadata cannot prove its declared output or producer identity."""


@cache
def schema_validator(relative: str) -> Draft202012Validator:
    """Compile one closed snapshot contract once for this host process."""
    try:
        schema = json.loads((ROOT / relative).read_text())
        Draft202012Validator.check_schema(schema)
    except (OSError, ValueError, SchemaError):
        raise SnapshotError("snapshot schema is missing or invalid") from None
    return Draft202012Validator(schema)


def check_identity(identity: dict[str, str]) -> None:
    """Apply the identity bounds a JSON Schema pattern cannot express."""
    if identity["kind"] == "iceberg_snapshot" and int(identity["id"]) > 2**63 - 1:
        raise SnapshotError("Iceberg snapshot ID exceeds the signed 64-bit range")
    if identity["kind"] == "s3_object_version":
        try:
            size = len(identity["id"].encode("utf-8"))
        except UnicodeError:
            raise SnapshotError("S3 object version is not valid UTF-8") from None
        if size > 1024:
            raise SnapshotError("S3 object version exceeds 1024 UTF-8 bytes")


def validate_snapshot_record(value: Any, invocation: dict[str, Any], outputs: list[dict[str, str]], *, succeeded: bool) -> dict[str, Any]:
    """Return a private validated record; never infer snapshots from paths or environment."""
    if list(schema_validator(SNAPSHOT_SCHEMA).iter_errors(value)):
        raise SnapshotError("snapshot record violates its closed schema")
    if value["invocation"] != invocation:
        raise SnapshotError("snapshot record belongs to another broker invocation")
    if not succeeded:
        raise SnapshotError("snapshot record requires a successful uncancelled completion")
    declared = {(item["namespace"], item["name"]) for item in outputs}
    seen = set()
    for item in value["outputs"]:
        key = (item["namespace"], item["name"])
        if key not in declared:
            raise SnapshotError("snapshot output is not declared by the template lineage")
        if key in seen:
            raise SnapshotError("snapshot record repeats an output dataset")
        seen.add(key)
        check_identity(item["snapshot"])
    return dict(json.loads(json.dumps(value, ensure_ascii=True, allow_nan=False)))


def validate_input_record(
    value: Any, invocation: dict[str, Any], inputs: list[dict[str, str]], *, succeeded: bool, pinned: list[dict[str, Any]]
) -> dict[str, Any]:
    """Return a private validated input record that covers every declared input and equals the pins frozen at intent."""
    if list(schema_validator(INPUT_SCHEMA).iter_errors(value)):
        raise SnapshotError("input snapshot record violates its closed schema")
    if value["invocation"] != invocation:
        raise SnapshotError("input snapshot record belongs to another broker invocation")
    if not succeeded:
        raise SnapshotError("input snapshot record requires a successful uncancelled completion")
    declared = {(item["namespace"], item["name"]) for item in inputs}
    seen = set()
    for item in value["inputs"]:
        key = (item["namespace"], item["name"])
        if key not in declared:
            raise SnapshotError("input snapshot is not declared by the template lineage")
        if key in seen:
            raise SnapshotError("input snapshot record repeats an input dataset")
        seen.add(key)
        check_identity(item["snapshot"])
    if seen != declared:
        raise SnapshotError("input snapshot record does not pin every declared input")
    if value["inputs"] != pinned:
        raise SnapshotError("input snapshot record differs from the pins frozen at intent")
    return dict(json.loads(json.dumps(value, ensure_ascii=True, allow_nan=False)))
