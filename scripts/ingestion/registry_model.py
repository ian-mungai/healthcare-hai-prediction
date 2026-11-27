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
