"""Shape checks for the files an owner may supply to the registry (design v4 section 5.1, evidence addendum rule P).

In step S4 no file closes or reduces a readiness item, so nothing here decides readiness. A supplied evidence entry, evidence record, live check, manual outcome
record, retention input or IAM plan evidence is checked for its SHAPE only, and the report says ``shape accepted, not usable in this step`` or names the shape
refusal. The checks keep the producing steps honest. They never raise on bad input and they print no input value: a refusal names a position (a path of
schema-defined property names and array indexes) and the schema keyword that failed, never ``error.message``, an instance or a property name the schema does not
define. The evidence rules that would let a file close an item (selection, identity, manifest, layout, one fact set, live coverage, closing status) are built
later by the steps that produce real records: see design_v4_s4_evidence_addendum.md.
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from scripts.acquisition import completion_record

SCHEMAS = Path(__file__).resolve().parents[2] / "config/ingestion/schemas"
ACCEPTED = "shape accepted, not usable in this step"


class Refusal(ValueError):
    """A shape refusal with a content-free reason."""


@cache
def schema_names() -> frozenset[str]:
    """Every property name a registry schema defines: the only names a diagnostic may print."""
    found: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            found.update(node.get("properties", {}))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for path in SCHEMAS.glob("*.schema.json"):
        walk(json.loads(path.read_bytes()))
    return frozenset(found)


def position(error: ValidationError) -> str:
    """The path of a failure: array indexes and schema-defined names, with any other key printed as a placeholder."""
    steps = [str(step) if isinstance(step, int) or step in schema_names() else "<key>" for step in error.absolute_path]
    return "/".join(steps) or "(document)"


def describe(error: ValidationError) -> str:
    """One failure as position and keyword, plus the missing required names the schema itself defines. Never the message, the instance or an input key."""
    keyword = str(error.validator)
    if keyword == "required" and isinstance(error.validator_value, list):
        missing = [name for name in error.validator_value if isinstance(error.instance, dict) and name not in error.instance]
        return f"{position(error)}: required {', '.join(name for name in missing if name in schema_names()) or 'property'} is missing"
    if keyword == "additionalProperties":
        return f"{position(error)}: unknown property"
    return f"{position(error)}: {keyword}"


def describe_errors(schema: Draft202012Validator, document: Any, limit: int = 3) -> list[str]:
    """The first failures of a document against a schema, each content-free, in a stable order."""
    found = sorted(schema.iter_errors(document), key=lambda error: [str(step) for step in error.absolute_path])
    return [describe(error) for error in found[:limit]]


@cache
def schema(name: str) -> Draft202012Validator:
    return Draft202012Validator(json.loads((SCHEMAS / f"{name}.schema.json").read_bytes()))


def require(name: str, document: Any, what: str) -> None:
    """Raise the shape refusal of a document that does not match its exact schema."""
    errors = describe_errors(schema(name), document)
    if errors:
        raise Refusal(f"{what} does not match its schema: {'; '.join(errors)}")


def unique(identities: Sequence[tuple[str, ...]], what: str) -> None:
    if len(set(identities)) != len(identities):
        raise Refusal(f"{what} names an object identity more than once")


def parse(body: bytes, what: str) -> Any:
    """Raw bytes to a JSON value, or a refusal. Invalid text, a bad encoding and runaway nesting are all refusals."""
    try:
        return json.loads(body)
    except (ValueError, RecursionError):
        raise Refusal(f"{what} is not valid JSON") from None


def read(root: Path, relative: Any, what: str) -> bytes:
    """The bytes of a file the tree holds at a relative path, or a refusal: a path that leaves the tree or cannot be read names nothing of its own.

    A link that loops raises RuntimeError from ``resolve`` on Python 3.12, so it is refused with the other unreadable paths."""
    unreadable = f"{what} points at no readable file of the tree"
    if not isinstance(relative, str):
        raise Refusal(unreadable)
    try:
        path = (root / relative).resolve()
        if not path.is_relative_to(root.resolve()) or not path.is_file():
            raise Refusal(unreadable)
        return path.read_bytes()
    except (OSError, ValueError, RuntimeError):
        raise Refusal(unreadable) from None


def completion_shape(document: Any) -> None:
    """A completion record: the exact types first, so the real contract check never meets a value of the wrong type, then the real contract check."""
    require("completion_record", document, "the completion record")
    try:
        completion_record.check(document)
    except completion_record.RecordError:
        raise Refusal("the completion record does not satisfy the completion record contract") from None
    for group in ("manifests", "data_objects", "other_objects"):
        unique([(item["key"], item["version_id"]) for item in document[group]], f"the completion record's {group}")


def inventory_shape(document: Any) -> None:
    """An inventory record (design v4 section 7, version 1)."""
    require("inventory_record", document, "the inventory record")
    unique([(item["object_key"], item["version_id"]) for item in document["objects"]], "the inventory record")
    unique([(item["key"], item["version_id"]) for item in document["manifests"]], "the inventory record's manifests")
    manifests = {(item["key"], item["version_id"]) for item in document["manifests"]}
    if any((item["manifest_key"], item["manifest_version_id"]) not in manifests for item in document["objects"]):
        raise Refusal("the inventory record lists an object of a manifest it does not list")


def manual_shape(document: Any) -> None:
    """A manual outcome record, as S4 defined it. It is not a producer contract: the real stale record belongs to S9."""
    require("manual_record", document, "the manual outcome record")
    unique([(item["key"], item["version_id"]) for item in document["objects"]], "the manual outcome record")


def live_shape(document: Any) -> None:
    """A live object check record, as S4 defined it. A failed result has a valid shape; it is reported as shape only."""
    require("live_check", document, "the live check record")
    unique([(item["key"], item["version_id"]) for item in document["objects"]], "the live check record")


def retention_shape(document: Any) -> None:
    require("retention_input", document, "the retention input")


def iam_shape(document: Any) -> None:
    require("iam_plan_evidence", document, "the IAM plan evidence")


RECORD_SHAPES = {"unit_records": completion_shape, "inventory_record": inventory_shape, "manual_outcome": manual_shape}


def outcome(check: Any, document: Any) -> str:
    """The note of one shape check: accepted (and not usable) or the refusal's content-free reason."""
    try:
        check(document)
    except Refusal as refusal:
        return f"refused ({refusal})"
    return ACCEPTED


def file_outcome(root: Path, relative: Any, check: Any, what: str) -> str:
    """The note of one supplied file: read it, parse it and check its shape; no failure raises."""
    try:
        return outcome(check, parse(read(root, relative, what), what))
    except Refusal as refusal:
        return f"refused ({refusal})"


def evidence_envelope(document: Any) -> Any:
    """The tables of a whole evidence file whose envelope (kind, version, candidate digest, no unknown property, a tables array) is exact, or a refusal."""
    exact = json.loads((SCHEMAS / "input_evidence.schema.json").read_bytes())
    exact["properties"]["tables"] = {"type": "array"}
    errors = describe_errors(Draft202012Validator(exact), document)
    if errors:
        raise Refusal(f"it does not match its schema: {'; '.join(errors)}")
    return document["tables"]


def evidence_file_notes(root: Path, relative: str, mapped: frozenset[str]) -> list[str]:
    """Shape notes for the evidence file of a tree, read and parsed here. An empty evidence file (the committed one) has none."""
    try:
        return evidence_notes(root, parse(read(root, relative, "it"), "it"), mapped)
    except Refusal as refusal:
        return [f"shape: the evidence file: refused ({refusal})"]


def evidence_notes(root: Path, document: Any, mapped: frozenset[str]) -> list[str]:
    """Shape notes for a parsed evidence file: one for a refused envelope, or one for each entry."""
    try:
        tables = evidence_envelope(document)
    except Refusal as refusal:
        return [f"shape: the evidence file: refused ({refusal})"]
    if not tables:
        return []
    entry_schema = Draft202012Validator(json.loads((SCHEMAS / "input_evidence.schema.json").read_bytes())["properties"]["tables"]["items"])
    notes: list[str] = []
    for number, row in enumerate(tables):
        table = row.get("table") if isinstance(row, dict) else None
        name = table if isinstance(table, str) and table in mapped else f"entry {number}"
        errors = describe_errors(entry_schema, row)
        if errors:
            notes.append(f"shape: evidence {name}: refused (the entry does not match its schema: {'; '.join(errors)})")
            continue
        if len({(item["key"], item["version_id"]) for item in row["objects"]}) != len(row["objects"]):
            notes.append(f"shape: evidence {name}: refused (an object identity appears more than once)")
            continue
        notes.append(f"shape: evidence {name}: record {file_outcome(root, row['record']['path'], RECORD_SHAPES[row['mode']], 'the evidence record')}")
        notes.append(f"shape: evidence {name}: live check {file_outcome(root, row['live_check']['path'], live_shape, 'the live check record')}")
    return notes


def manual_notes(root: Path, document: Any) -> list[str]:
    """One shape note for the manual outcome records the authored outcomes point at, counted by outcome."""
    if not isinstance(document, dict) or not isinstance(document.get("outcomes"), list):
        return []
    pointers = [
        row["record"]["path"] for row in document["outcomes"] if isinstance(row, dict) and isinstance(row.get("record"), dict) and "path" in row["record"]
    ]
    if not pointers:
        return []
    results = [file_outcome(root, path, manual_shape, "the manual outcome record") for path in pointers]
    refused = [result for result in results if result != ACCEPTED]
    text = f"shape: manual outcome records: {len(results) - len(refused)} {ACCEPTED}; {len(refused)} refused"
    return [text + (f" (for example {refused[0]})" if refused else "")]


def r4_notes(root: Path, retention: str, iam: str) -> list[str]:
    """One shape note for each of the two R4 inputs that exists."""
    notes: list[str] = []
    for path, check in ((retention, retention_shape), (iam, iam_shape)):
        # A link that loops or points nowhere is a supplied file too, so presence is the entry and not what it leads to.
        if os.path.lexists(root / path):
            notes.append(f"shape: {path}: {file_outcome(root, path, check, 'the file')}")
    return notes


def shape_notes(root: Path, evidence: str, manual: Any, mapped: frozenset[str], retention: str, iam: str) -> list[str]:
    """Every shape note about the supplied files of a tree. None of them changes a readiness item."""
    return evidence_file_notes(root, evidence, mapped) + manual_notes(root, manual) + r4_notes(root, retention, iam)
