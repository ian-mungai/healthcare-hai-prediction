"""Shape checks for the files an owner may supply to the registry (design v4 section 5.1).

In step S4 no file closes or reduces a readiness item, so nothing here decides readiness. A supplied evidence entry, evidence record, live check, manual outcome
record, retention input or IAM plan evidence is checked for its SHAPE only, and the report says ``shape accepted, not usable in this step`` or names the shape
refusal. The checks keep the producing steps honest. They never raise on bad input and they print no input value: a refusal names a position (a path of
schema-defined property names and array indexes) and the schema keyword that failed, never ``error.message``, an instance or a property name the schema does not
define. The evidence rules that would let a file close an item (selection, identity, manifest, layout, one fact set, live coverage, closing status) are built
later by the steps that produce real records. Every schema comes from the tree that is checked (:class:`Schemas`), never from the checker's own folder.
"""

from __future__ import annotations

import json
import os
from collections.abc import Collection, Iterable, Sequence
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from scripts.acquisition import completion_record
from scripts.acquisition.source_registry import unique_object
from scripts.ingestion.registry_model import NEEDS

SCHEMA_FOLDER = "config/ingestion/schemas"
SCHEMA_SUFFIX = ".schema.json"
ACCEPTED = "shape accepted, not usable in this step"
# The schemas every tree must hold: the lock binds each whether or not the tree has it, so a missing one fails by name.
REQUIRED_SCHEMAS = (
    "collections",
    "completion_record",
    "iam_plan_evidence",
    "input_evidence",
    "inventory_record",
    "layout_contracts",
    "live_check",
    "manual_outcomes",
    "manual_record",
    "registry_lock",
    "retention_input",
    "unit_golden",
)


class Refusal(ValueError):
    """A shape refusal with a content-free reason."""


def property_names(documents: Iterable[Any]) -> frozenset[str]:
    """Every property name the schemas define: the only names a diagnostic may print."""
    found: set[str] = set()
    pending = list(documents)
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            if isinstance(node.get("properties"), dict):
                found.update(node["properties"])
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    return frozenset(found)


def position(error: ValidationError, names: Collection[str]) -> str:
    """The path of a failure: array indexes and schema-defined names, with any other key printed as a placeholder."""
    steps = [str(step) if isinstance(step, int) or step in names else "<key>" for step in error.absolute_path]
    return "/".join(steps) or "(document)"


def describe(error: ValidationError, names: Collection[str]) -> str:
    """One failure as position and keyword, plus the missing required names the schema itself defines. Never the message, the instance or an input key."""
    where = position(error, names)
    if error.validator is None:
        return f"{where}: the schema accepts nothing here"
    keyword = str(error.validator)
    if keyword == "required" and isinstance(error.validator_value, list):
        missing = [name for name in error.validator_value if isinstance(error.instance, dict) and name not in error.instance]
        return f"{where}: required {', '.join(name for name in missing if name in names) or 'property'} is missing"
    if keyword == "additionalProperties":
        return f"{where}: unknown property"
    return f"{where}: {keyword}"


def references_resolve(document: Any, registry: Registry[Any]) -> bool:
    """Whether every reference of a schema and of its subschemas resolves in the registry, which holds the bound schemas and retrieves nothing."""
    root = DRAFT202012.create_resource(document)
    pending: list[tuple[Resource[Any], Any]] = [(root, registry.resolver_with_root(root))]
    while pending:
        resource, resolver = pending.pop()
        try:
            if isinstance(resource.contents, dict):
                for keyword in ("$ref", "$dynamicRef"):
                    if keyword in resource.contents:
                        resolver.lookup(resource.contents[keyword])
            pending.extend((child, resolver.in_subresource(child)) for child in resource.subresources())
        except Exception:  # an unresolvable or malformed address raises library and standard errors of several types
            return False
    return True


class Schemas:
    """The registry schemas of one tree, read once. Integrity and the shape checks validate against these and no others.

    A reference resolves to a definition of its own schema or to another schema of the tree, by its file name or its ``$id``. The registry that holds them has
    no retriever, so no validation opens an address or a file."""

    def __init__(self, root: Path) -> None:
        self.documents: dict[str, Any] = {}
        # A file that is no usable JSON schema, by name, with why.
        self.unusable: dict[str, str] = {}
        for path in sorted((root / SCHEMA_FOLDER).glob(f"*{SCHEMA_SUFFIX}")):
            name = path.name.removesuffix(SCHEMA_SUFFIX)
            try:
                document = json.loads(path.read_bytes(), object_pairs_hook=unique_object)
                Draft202012Validator.check_schema(document)
            except Exception as error:  # a pattern the regex compiler cannot hold raises its own error, not a schema error
                self.unusable[name] = f"is not a usable JSON schema ({type(error).__name__})"
                continue
            self.documents[name] = document
        resources = {name: DRAFT202012.create_resource(document) for name, document in self.documents.items()}
        self.registry: Registry[Any] = Registry().with_resources(
            (address, resource) for name, resource in resources.items() for address in (f"{name}{SCHEMA_SUFFIX}", resource.id()) if address
        )
        for name, document in list(self.documents.items()):
            if not references_resolve(document, self.registry):
                self.unusable[name] = "has a reference that resolves to no definition of its own and to no schema of the tree"
                del self.documents[name]
        self.names = property_names([*self.documents.values(), *NEEDS.values()])

    def document(self, name: str) -> Any:
        if name not in self.documents:
            raise Refusal(f"the tree's {name} schema is missing or unusable")
        return self.documents[name]

    def errors_of(self, schema: Any, value: Any, limit: int = 3) -> list[str]:
        """The first failures of a value against a schema, each content-free, in a stable order."""
        try:
            validator = Draft202012Validator(schema, registry=self.registry)
            found = sorted(validator.iter_errors(value), key=lambda error: [str(step) for step in error.absolute_path])
            return [describe(error, self.names) for error in found[:limit]]
        except Exception as error:  # a missing or endless reference of a bound schema shows only here, as library errors of many types
            return [f"(document): the schema cannot be evaluated ({type(error).__name__})"]

    def errors(self, name: str, value: Any, limit: int = 3) -> list[str]:
        return self.errors_of(self.document(name), value, limit)

    def needs_errors(self, name: str, value: Any) -> list[str]:
        """Where a document lacks what its readers need (``NEEDS``), whatever its bound schema allows."""
        return [f"lacks the structure its reader needs: {text}" for text in self.errors_of(NEEDS[name], value)]


def require(name: str, document: Any, what: str, schemas: Schemas) -> None:
    """Raise the shape refusal of a document that does not match its exact schema."""
    errors = schemas.errors(name, document)
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


def completion_shape(document: Any, schemas: Schemas) -> None:
    """A completion record: the exact types first, so the real contract check never meets a value of the wrong type, then the real contract check."""
    require("completion_record", document, "the completion record", schemas)
    try:
        completion_record.check(document)
    except completion_record.RecordError:
        raise Refusal("the completion record does not satisfy the completion record contract") from None
    for group in ("manifests", "data_objects", "other_objects"):
        unique([(item["key"], item["version_id"]) for item in document[group]], f"the completion record's {group}")


def inventory_shape(document: Any, schemas: Schemas) -> None:
    """An inventory record (design v4 section 7, version 1)."""
    require("inventory_record", document, "the inventory record", schemas)
    unique([(item["object_key"], item["version_id"]) for item in document["objects"]], "the inventory record")
    unique([(item["key"], item["version_id"]) for item in document["manifests"]], "the inventory record's manifests")
    manifests = {(item["key"], item["version_id"]) for item in document["manifests"]}
    if any((item["manifest_key"], item["manifest_version_id"]) not in manifests for item in document["objects"]):
        raise Refusal("the inventory record lists an object of a manifest it does not list")


def manual_shape(document: Any, schemas: Schemas) -> None:
    """A manual outcome record, as S4 defined it. It is not a producer contract: the real stale record belongs to S9."""
    require("manual_record", document, "the manual outcome record", schemas)
    unique([(item["key"], item["version_id"]) for item in document["objects"]], "the manual outcome record")


def live_shape(document: Any, schemas: Schemas) -> None:
    """A live object check record, as S4 defined it. A failed result has a valid shape; it is reported as shape only."""
    require("live_check", document, "the live check record", schemas)
    unique([(item["key"], item["version_id"]) for item in document["objects"]], "the live check record")


def retention_shape(document: Any, schemas: Schemas) -> None:
    require("retention_input", document, "the retention input", schemas)


def iam_shape(document: Any, schemas: Schemas) -> None:
    require("iam_plan_evidence", document, "the IAM plan evidence", schemas)


RECORD_SHAPES = {"unit_records": completion_shape, "inventory_record": inventory_shape, "manual_outcome": manual_shape}
# What a check meets when a bound schema lets through a document it cannot read.
UNREADABLE = (LookupError, TypeError, AttributeError, ValueError, ArithmeticError, RecursionError)


def outcome(check: Any, document: Any, schemas: Schemas) -> str:
    """The note of one shape check: accepted (and not usable) or the refusal's content-free reason."""
    try:
        check(document, schemas)
    except Refusal as refusal:
        return f"refused ({refusal})"
    except UNREADABLE:
        return "refused (it passes its schema and cannot be read as this record)"
    return ACCEPTED


def file_outcome(root: Path, relative: Any, check: Any, what: str, schemas: Schemas) -> str:
    """The note of one supplied file: read it, parse it and check its shape; no failure raises."""
    try:
        return outcome(check, parse(read(root, relative, what), what), schemas)
    except Refusal as refusal:
        return f"refused ({refusal})"


def evidence_envelope(document: Any, schemas: Schemas) -> tuple[list[Any], Any]:
    """The tables of a whole evidence file whose envelope (kind, version, candidate digest, no unknown property, a tables array) is exact, with the bound
    schema of one entry, or a refusal."""
    bound = schemas.document("input_evidence")
    try:
        entry = bound["properties"]["tables"]["items"]
        # An entry is validated alone, so it takes the local definitions it may name with it.
        if "$defs" in bound and isinstance(entry, dict):
            entry = {"$defs": bound["$defs"], **entry}
        exact = {**bound, "properties": {**bound["properties"], "tables": {"type": "array"}}}
    except (KeyError, TypeError):
        raise Refusal("the tree's input_evidence schema describes no table entry") from None
    errors = schemas.errors_of(exact, document)
    if errors:
        raise Refusal(f"it does not match its schema: {'; '.join(errors)}")
    if not isinstance(document, dict) or not isinstance(document.get("tables"), list):
        raise Refusal("it has no tables array")
    return document["tables"], entry


def evidence_file_notes(root: Path, relative: str, mapped: frozenset[str], schemas: Schemas) -> list[str]:
    """Shape notes for the evidence file of a tree, read and parsed here. An empty evidence file (the committed one) has none."""
    try:
        return evidence_notes(root, parse(read(root, relative, "it"), "it"), mapped, schemas)
    except Refusal as refusal:
        return [f"shape: the evidence file: refused ({refusal})"]


def entry_notes(root: Path, name: str, row: Any, entry: Any, schemas: Schemas) -> list[str]:
    """The shape notes of one evidence entry: one for a refused entry, or one for its record and one for its live check."""
    errors = schemas.errors_of(entry, row)
    if errors:
        return [f"shape: evidence {name}: refused (the entry does not match its schema: {'; '.join(errors)})"]
    if len({(item["key"], item["version_id"]) for item in row["objects"]}) != len(row["objects"]):
        return [f"shape: evidence {name}: refused (an object identity appears more than once)"]
    return [
        f"shape: evidence {name}: record {file_outcome(root, row['record']['path'], RECORD_SHAPES[row['mode']], 'the evidence record', schemas)}",
        f"shape: evidence {name}: live check {file_outcome(root, row['live_check']['path'], live_shape, 'the live check record', schemas)}",
    ]


def evidence_notes(root: Path, document: Any, mapped: frozenset[str], schemas: Schemas) -> list[str]:
    """Shape notes for a parsed evidence file: one for a refused envelope, or the notes of each entry."""
    try:
        tables, entry = evidence_envelope(document, schemas)
    except Refusal as refusal:
        return [f"shape: the evidence file: refused ({refusal})"]
    notes: list[str] = []
    for number, row in enumerate(tables):
        table = row.get("table") if isinstance(row, dict) else None
        name = table if isinstance(table, str) and table in mapped else f"entry {number}"
        try:
            notes += entry_notes(root, name, row, entry, schemas)
        except UNREADABLE:
            notes.append(f"shape: evidence {name}: refused (the entry passes its schema and cannot be read as an evidence entry)")
    return notes


def manual_notes(root: Path, document: Any, schemas: Schemas) -> list[str]:
    """One shape note for the manual outcome records the authored outcomes point at, counted by outcome."""
    if not isinstance(document, dict) or not isinstance(document.get("outcomes"), list):
        return []
    pointers = [
        row["record"]["path"] for row in document["outcomes"] if isinstance(row, dict) and isinstance(row.get("record"), dict) and "path" in row["record"]
    ]
    if not pointers:
        return []
    results = [file_outcome(root, path, manual_shape, "the manual outcome record", schemas) for path in pointers]
    refused = [result for result in results if result != ACCEPTED]
    text = f"shape: manual outcome records: {len(results) - len(refused)} {ACCEPTED}; {len(refused)} refused"
    return [text + (f" (for example {refused[0]})" if refused else "")]


def r4_notes(root: Path, retention: str, iam: str, schemas: Schemas) -> list[str]:
    """One shape note for each of the two R4 inputs that exists."""
    notes: list[str] = []
    for path, check in ((retention, retention_shape), (iam, iam_shape)):
        # A link that loops or points nowhere is a supplied file too, so presence is the entry and not what it leads to.
        if os.path.lexists(root / path):
            notes.append(f"shape: {path}: {file_outcome(root, path, check, 'the file', schemas)}")
    return notes


def shape_notes(root: Path, evidence: str, manual: Any, mapped: frozenset[str], retention: str, iam: str, schemas: Schemas) -> list[str]:
    """Every shape note about the supplied files of a tree. None of them changes a readiness item."""
    return evidence_file_notes(root, evidence, mapped, schemas) + manual_notes(root, manual, schemas) + r4_notes(root, retention, iam, schemas)
