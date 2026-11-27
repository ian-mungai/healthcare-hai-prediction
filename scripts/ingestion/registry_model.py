"""The values the registry check passes between its predicates: problems, results, the parsed tree and the queued outputs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# The queue route of a unit that a person captures: such a unit is a manual unit.
MANUAL_ROUTE = "manual_browser_download"
# The owner decisions that R3 keeps open.
DECISIONS = ("D1", "D2", "D3", "Q4")


def _record(optional: Mapping[str, Any] | None = None, /, **fields: Any) -> dict[str, Any]:
    return {"type": "object", "required": sorted(fields), "properties": {**fields, **(optional or {})}}


def _array(items: Any) -> dict[str, Any]:
    return {"type": "array", "items": items}


def _mapping(values: Any) -> dict[str, Any]:
    return {"type": "object", "additionalProperties": values}


_ANY: dict[str, Any] = {}
_TEXT = {"type": "string"}
_DECISION = _record(value=_TEXT, recorded=_ANY)
_REFERENCE = {"anyOf": [{"type": "null"}, _record(source_id=_TEXT, sha256=_TEXT)]}
_SOURCE = _record({"match": _mapping(_TEXT)}, source_id=_TEXT, acquisition=_TEXT, admission=_TEXT, scope=_TEXT, decision_ref=_REFERENCE)
_LAYOUT = _record(id=_TEXT, columns_sha256=_TEXT, columns=_array(_TEXT))
_FILE = _record(path=_TEXT, sha256=_TEXT)
# The capture specification fields the queued outputs come from. The capture run's loader does not type them.
_MEMBER = _record(role=_TEXT, dataset_id=_TEXT, member_chain={**_array(_TEXT), "minItems": 1})
_TEMPLATE = {"anyOf": [{"type": "null"}, {"type": "object", "properties": {"role": {"type": ["string", "null"]}}}]}
_SPEC = {
    "type": "object",
    "properties": {"stored_data_files": _array(_TEXT), "archive_members": {**_array(_MEMBER), "minItems": 1}, "receipt_template": _TEMPLATE},
}
# What the readers of the lock, of the capture specifications and of each authored file need of it. A document with a bound schema is held to this after it,
# so a bound schema that allows more gives a named refusal and never an unreadable value. These are not the schemas: they list only what the code reads.
NEEDS: dict[str, dict[str, Any]] = {
    "registry_lock": _record(
        {"decisions": _mapping(_ANY)},
        files=_array(_FILE),
        map_sha256=_TEXT,
        scope_report_sha256=_TEXT,
        parents=_record(source_registry=_FILE),
        pins=_record(map_sha256=_TEXT, gaps=_array(_record(table=_TEXT, kind=_TEXT, step=_TEXT)), seat_gaps=_mapping(_array(_TEXT))),
    ),
    "collections": _record(
        collections=_array(_record(collection=_TEXT, sources=_array(_SOURCE))),
        decisions={
            "type": "object",
            "required": list(DECISIONS),
            "additionalProperties": _DECISION,
            "properties": {
                **dict.fromkeys(DECISIONS, _DECISION),
                "Q4": _record({"interim": {"type": "object"}}, value=_TEXT, recorded=_ANY),
            },
        },
        feeds=_array(_record(table=_TEXT, source_id=_TEXT, name_regex=_TEXT)),
        gaps=_mapping(_record(step=_TEXT)),
        plans=_array(_record(path=_TEXT, lifecycle=_TEXT)),
    ),
    "capture_specs": _record(specs=_mapping(_SPEC)),
    "manual_outcomes": _record(outcomes=_array(_record(unit=_TEXT))),
    "layout_contracts": _record(tables=_mapping(_record(consumer_columns={"anyOf": [{"const": "any"}, _array(_TEXT)]}, accepted_layouts=_array(_LAYOUT)))),
    "unit_golden": _record(
        counts=_record(units=_ANY, receipt_bearing=_ANY, file=_ANY, plan_mode=_ANY, manual=_ANY),
        units=_array(_record(unit=_TEXT, kind=_ANY, receipt=_ANY, **{"class": _TEXT})),
    ),
}


class RegistryFileError(Exception):
    """A registry file that cannot be read as the JSON document it should be."""


@dataclass(frozen=True)
class Problem:
    """One named failure: the predicate, the subject it names (a file, table, entry point or mode) and what is wrong."""

    predicate: str
    subject: str
    message: str

    def __str__(self) -> str:
        return f"{self.predicate} {self.subject}: {self.message}"


@dataclass
class Result:
    """What a check found: the problems, the readiness items still open and the computed facts the lock pins."""

    problems: list[Problem] = field(default_factory=list)
    release_problems: list[Problem] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    readiness: list[str] = field(default_factory=list)
    gaps: list[dict[str, str]] = field(default_factory=list)
    seat_gaps: dict[str, list[str]] = field(default_factory=dict)
    facts: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.problems


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def canonical(value: Any) -> bytes:
    """The one byte form a decision or layout digest hashes."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


@dataclass(frozen=True)
class Output:
    """One file a queued unit would store: where, in which role, under which dataset ID and name."""

    snapshot_id: str
    source_id: str
    collection: str
    role: str
    dataset_id: str
    name: str
    archive: bool


@dataclass
class Tree:
    """The parsed inputs the predicates share."""

    root: Path
    units: list[dict[str, Any]]
    specs: dict[str, dict[str, Any]]
    routes: dict[str, str]
    tables: list[dict[str, Any]]
    collections: dict[str, Any]
    golden: dict[str, Any]
    manual: dict[str, Any]
    contracts: dict[str, Any]
    lock: dict[str, Any]
    source_registry: dict[str, Any]
    leaving: frozenset[str] = frozenset()


def default_scope(report: Mapping[str, Any]) -> dict[str, str]:
    """The default-scope tables of a scope report, each with its kind."""
    return {**{row["table"]: "data_table" for row in report["in_scope"]}, **{row["table"]: "required_dictionary" for row in report["dictionary"]}}
