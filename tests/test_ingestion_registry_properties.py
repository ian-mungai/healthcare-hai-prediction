"""Property-style regression for the shape checks: from one valid fixture per file kind, every edit a careless producer could make is handled by name.

A generator takes the valid document of a kind and yields each variant: every required key removed, every value blanked, every object duplicated, every result
field flipped, a text sentinel set at every position, every single value replaced by an array and by an object and an unknown key named by the sentinel added
at every object. Each variant is shape checked. The checks
must never crash, never print the sentinel or any input value, and accept exactly the variants that still match the exact schema (an independent run of the
schema decides). In step S4 nothing a shape check accepts closes a readiness item: that is the central case of ``test_ingestion_registry_minimal.py``.
"""

from __future__ import annotations

import copy
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from scripts.acquisition import completion_record
from scripts.ingestion import registry
from scripts.ingestion import registry_records as records
from tests.registry_records_support import HPSA, completion, evidence_entry, iam_evidence, inventory, live_record, manual_record, retention_input
from tests.support import check

Path_ = tuple[Any, ...]
SCHEMAS = records.Schemas(registry.ROOT)
STOPS = ("failed", "owner_held", "pending_owner", "stale", "mismatched")
ALLOW_EMPTY = frozenset({"supersedes", "member_chain", "other_objects", "manifests"})
SENTINEL = "SENTINEL_INPUT_VALUE_7f3a"


def walk(document: Any, path: Path_ = ()) -> Iterator[tuple[Path_, Any]]:
    """Every path of a document, descending into the first element of a list so that each kind of object is reached once."""
    yield path, document
    if isinstance(document, dict):
        for key, value in document.items():
            yield from walk(value, (*path, key))
    elif isinstance(document, list) and document:
        yield from walk(document[0], (*path, 0))


def at(document: Any, path: Path_) -> Any:
    for step in path:
        document = document[step]
    return document


def replaced(document: Any, path: Path_, value: Any) -> Any:
    copy_ = copy.deepcopy(document)
    if not path:
        return value
    at(copy_, path[:-1])[path[-1]] = value
    return copy_


def removed(document: Any, path: Path_) -> Any:
    copy_ = copy.deepcopy(document)
    del at(copy_, path[:-1])[path[-1]]
    return copy_


def add_key(document: Any, path: Path_, name: str = "unexpected") -> Any:
    copy_ = copy.deepcopy(document)
    at(copy_, path)[name] = 1
    return copy_


def duplicated(document: Any, path: Path_) -> Any:
    copy_ = copy.deepcopy(document)
    target = at(copy_, path)
    target.append(copy.deepcopy(target[0]))
    return copy_


def variants(document: Any, flips: tuple[str, ...] = STOPS) -> Iterator[tuple[str, Any]]:
    """Each variant of a valid document, labelled by what was done where."""
    for path, value in list(walk(document)):
        where = "/".join(str(step) for step in path) or "(document)"
        last = path[-1] if path else None
        if isinstance(last, str):
            yield f"remove {where}", removed(document, path)
        if path:
            blanks: list[Any] = [None]
            if isinstance(value, str):
                blanks.append("")
            if isinstance(value, list) and last not in ALLOW_EMPTY:
                blanks.append([])
            if isinstance(value, dict):
                blanks.append({})
            if isinstance(value, bool):
                blanks = [None]
            for blank in blanks:
                if blank != value:
                    yield f"blank {where} to {blank!r}", replaced(document, path, blank)
        if isinstance(value, bool):
            yield f"flip {where}", replaced(document, path, not value)
        if last == "status" and isinstance(value, str):
            for stop in flips:
                if stop != value:
                    yield f"flip {where} to {stop}", replaced(document, path, stop)
        if isinstance(value, dict):
            yield f"add an unknown key at {where}", add_key(document, path)
            yield f"add a key named by the sentinel at {where}", add_key(document, path, SENTINEL)
        if path and not isinstance(value, (dict, list)):
            yield f"set {where} to the sentinel", replaced(document, path, SENTINEL)
            yield f"set {where} to an array", replaced(document, path, [SENTINEL])
            yield f"set {where} to an object", replaced(document, path, {SENTINEL: 1})
        if isinstance(value, list) and value and isinstance(value[0], dict):
            yield f"duplicate the first object of {where}", duplicated(document, path)


class Kind:
    """One file kind: its valid document, its shape check (which raises Refusal or returns) and an independent decision whether a document is valid."""

    def __init__(self, name: str, base: Callable[[Path], Any], shape: Callable[[Path, Any], None], valid: Callable[[Path, Any], bool]) -> None:
        self.name, self.base, self.shape, self.valid = name, base, shape, valid


def schema_valid(name: str) -> Callable[[Path, Any], bool]:
    return lambda root, document: not SCHEMAS.errors(name, document)


def distinct(rows: list[dict[str, Any]], key: str) -> bool:
    return len({(row[key], row["version_id"]) for row in rows}) == len(rows)


def inventory_valid(root: Path, document: Any) -> bool:
    """The exact schema, distinct object and manifest identities and every object's manifest listed."""
    if SCHEMAS.errors("inventory_record", document):
        return False
    manifests = {(row["key"], row["version_id"]) for row in document["manifests"]}
    linked = all((row["manifest_key"], row["manifest_version_id"]) in manifests for row in document["objects"])
    return distinct(document["objects"], "object_key") and distinct(document["manifests"], "key") and linked


def listed_valid(name: str) -> Callable[[Path, Any], bool]:
    """The exact schema and distinct object identities, for the kinds that list objects under ``objects``."""
    return lambda root, document: not SCHEMAS.errors(name, document) and distinct(document["objects"], "key")


def completion_valid(root: Path, document: Any) -> bool:
    if SCHEMAS.errors("completion_record", document):
        return False
    try:
        completion_record.check(document)
    except completion_record.RecordError:
        return False
    return all(
        len({(item["key"], item["version_id"]) for item in document[group]}) == len(document[group]) for group in ("manifests", "data_objects", "other_objects")
    )


def direct(shape: Callable[[Any, records.Schemas], None]) -> Callable[[Path, Any], None]:
    return lambda root, document: shape(document, SCHEMAS)


def evidence_shape(root: Path, document: Any) -> None:
    """The evidence file's notes: a refusal anywhere in them is a refusal of the file."""
    notes = records.evidence_notes(root, document, frozenset({HPSA}), SCHEMAS)
    if any("refused" in note for note in notes):
        raise records.Refusal("; ".join(notes))


def evidence_base(root: Path) -> Any:
    return {"kind": "input_evidence", "schema_version": 1, "candidate_map_sha256": "1" * 64, "tables": [evidence_entry(root, HPSA)]}


def evidence_valid(root: Path, document: Any) -> bool:
    """The whole evidence file against its exact schema (envelope and entries), each pointed file present and no object identity twice."""
    if SCHEMAS.errors("input_evidence", document):
        return False
    entries = document["tables"]
    pointed = [(root / row[part]["path"]).is_file() for row in entries for part in ("record", "live_check")]
    return all(pointed) and all(len({(item["key"], item["version_id"]) for item in row["objects"]}) == len(row["objects"]) for row in entries)


def kinds() -> list[Kind]:
    return [
        Kind("completion record", lambda root: completion(HPSA)[0], direct(records.completion_shape), completion_valid),
        Kind("inventory record", lambda root: inventory(HPSA), direct(records.inventory_shape), inventory_valid),
        Kind("manual outcome record", lambda root: manual_record(HPSA), direct(records.manual_shape), listed_valid("manual_record")),
        Kind("live check record", lambda root: live_record(evidence_entry(root, HPSA)["objects"]), direct(records.live_shape), listed_valid("live_check")),
        Kind("retention input", lambda root: retention_input(["hrsa_hpsa_detail"]), direct(records.retention_shape), schema_valid("retention_input")),
        Kind("IAM plan evidence", lambda root: iam_evidence(), direct(records.iam_shape), schema_valid("iam_plan_evidence")),
        Kind("evidence file", evidence_base, evidence_shape, evidence_valid),
    ]


def run_kinds(root: Path) -> tuple[dict[str, dict[str, int]], list[str]]:
    """Per kind the variants refused and accepted, and every breach: a crash, a leaked value, a valid variant refused or an invalid one accepted."""
    counts: dict[str, dict[str, int]] = {}
    breaches: list[str] = []
    for kind in kinds():
        base = kind.base(root)
        counts[kind.name] = {"variants": 0, "refused": 0, "accepted": 0}
        for label, variant in [("the valid document", copy.deepcopy(base)), *variants(base)]:
            try:
                kind.shape(root, variant)
                refusal = None
            except records.Refusal as error:
                refusal = str(error)
            except Exception as error:  # a crash is the breach this generator exists to find
                breaches.append(f"{kind.name}: {label}: crashed with {type(error).__name__}")
                continue
            if label != "the valid document":
                counts[kind.name]["variants"] += 1
                counts[kind.name]["refused" if refusal else "accepted"] += 1
            if refusal and SENTINEL in refusal:
                breaches.append(f"{kind.name}: {label}: the refusal leaked the sentinel")
            if refusal is None and not kind.valid(root, variant):
                breaches.append(f"{kind.name}: {label}: an invalid document was accepted")
            if refusal is not None and kind.valid(root, variant):
                breaches.append(f"{kind.name}: {label}: a valid document was refused")
    return counts, breaches


def test_every_variant_of_every_file_kind_is_handled_by_name_without_a_crash_or_a_leak(tmp_path: Path) -> None:
    counts, breaches = run_kinds(tmp_path)
    check(not breaches, f"{len(breaches)} breaches, for example: {breaches[:6]}")
    check(len(counts) == 7 and all(row["variants"] >= 20 and row["refused"] > 0 for row in counts.values()), f"The generator is too small: {counts}")
