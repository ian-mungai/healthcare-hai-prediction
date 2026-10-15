"""Validate dated, user-approved measure additions without rewriting the hash-locked source registry.

The base registry hash is recorded in existing plans, batches and receipts, so additions live in a companion file bound to that
hash. Additions never clear gates or holds and never approve a measure for modeling.

The optional ``source_metadata`` section adds the publishing agency and the bronze ``dataset_id`` values to registered sources. An
entry holds only the keys listed below, so it cannot carry, shadow or change a base field, gate or hold.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import LOCK_PATH, REGISTRY_PATH, RegistryError, canonical_hash, index_records, load_registry, read_json, require

ADDITIONS_PATH = REGISTRY_PATH.with_name("registry_additions.json")
ADDITIONS_LOCK_PATH = REGISTRY_PATH.with_name("registry_additions_lock.json")
SOURCE_METADATA_KEYS = frozenset({"source_id", "publisher", "publisher_evidence", "dataset_ids", "dataset_ids_evidence", "approval"})
PUBLISHER_EVIDENCE_KEYS = frozenset({"official_landing_url", "page_title", "naming_field", "naming_text", "retrieved_utc", "page_sha256"})
PUBLISHER_EVIDENCE_OPTIONAL_KEYS = frozenset({"evidence_note"})
DATASET_EVIDENCE_KEYS = frozenset({"dataset_id", "rule", "detail"})
ACQUIRED_VIA_ROUTES = frozenset({"main-hai-pdc"})
APPROVAL_KEYS = frozenset({"decided_by", "decided_on", "decision", "recorded_in"})
# Same shape as the dataset folder rule in dataset_layout.dataset_id; repeated here so this module stays free of capture imports.
DATASET_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_]{0,119}")
RETRIEVED_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+00:00")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
REVIEW_RULE = "Review additions and lock diffs together against the recorded user decision. This lock detects drift; it is not a signature or new approval."


def validate_addition(control: dict[str, Any], base_controls: dict[str, dict[str, Any]], base_sources: dict[str, dict[str, Any]]) -> None:
    """Reject an added measure control that could substitute, orphan or clear a registered control.

    Parameters
    ----------
    control : dict
        One entry from the additions file's ``measure_controls`` list.
    base_controls, base_sources : dict
        The base registry's measure controls and sources, indexed by ID.

    Raises
    ------
    RegistryError
        Through ``require`` when any rule fails.
    """
    control_id = control.get("id")
    require(isinstance(control_id, str) and control_id not in base_controls, f"Added control {control_id} collides with a registered control.")
    require(control.get("parent_id") in base_controls, f"Added control {control_id} names an unknown parent.")
    sources = control.get("source_ids")
    require(isinstance(sources, list) and bool(sources) and all(s in base_sources for s in sources), f"Added control {control_id} needs registered source IDs.")
    # A new control must start held and ungated, so adding it can never approve a measure for modeling.
    require(control.get("gate_status") == "not_run", f"Added control {control_id} cannot start with a cleared gate.")
    decision = (control.get("preserved_controls") or {}).get("current_review_decision")
    require(isinstance(decision, str) and decision.startswith("hold_"), f"Added control {control_id} must start on a hold_ decision.")
    require_user_approval(control.get("approval") or {}, f"Added control {control_id}")


def require_user_approval(approval: dict[str, Any], label: str) -> None:
    """Require the user's dated approval; ``label`` names the addition in each error message."""
    require(approval.get("decided_by") == "user", f"{label} needs the user's approval.")
    decided_on = approval.get("decided_on")
    require(isinstance(decided_on, str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", decided_on) is not None, f"{label} needs an ISO approval date.")
    try:
        date.fromisoformat(str(decided_on))
    except ValueError as error:
        raise RegistryError(f"{label} has an impossible approval date.") from error


def nonempty_text(value: Any) -> bool:
    """Return whether the value is text with something other than whitespace."""
    return isinstance(value, str) and bool(value.strip())


def validate_source_metadata(entries: Any, base_sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Reject a source metadata entry that is malformed, unregistered, doubled or that reuses a dataset_id.

    Parameters
    ----------
    entries : list
        The additions file's ``source_metadata`` section.
    base_sources : dict
        The base registry's sources, indexed by ID.

    Returns
    -------
    dict
        The entries indexed by ``source_id``.

    Raises
    ------
    RegistryError
        Through ``require`` when any rule fails.
    """
    indexed = index_records(entries, "source_id", "source metadata")
    owner: dict[str, str] = {}
    for source_id, entry in indexed.items():
        require(source_id in base_sources, f"Source metadata names an unregistered source: {source_id}.")
        require(set(entry) == SOURCE_METADATA_KEYS, f"Source metadata {source_id} must hold exactly the keys {sorted(SOURCE_METADATA_KEYS)}.")
        require(nonempty_text(entry["publisher"]), f"Source metadata {source_id} needs a publisher.")
        validate_publisher_evidence(entry["publisher_evidence"], source_id, base_sources[source_id])
        ids, evidence = entry["dataset_ids"], entry["dataset_ids_evidence"]
        require(isinstance(ids, list) and isinstance(evidence, list), f"Source metadata {source_id} needs dataset_ids and evidence lists.")
        for dataset_id in ids:
            require(
                isinstance(dataset_id, str) and DATASET_ID_PATTERN.fullmatch(dataset_id) is not None, f"Source metadata {source_id} has an invalid dataset_id."
            )
            require(dataset_id not in owner, f"The dataset_id {dataset_id} belongs to more than one source ({owner.get(dataset_id)}, {source_id}).")
            owner[dataset_id] = source_id
        require(len(evidence) == len(ids), f"Source metadata {source_id} needs one evidence record per dataset_id.")
        for dataset_id, record in zip(ids, evidence, strict=True):
            require(
                isinstance(record, dict) and DATASET_EVIDENCE_KEYS <= set(record) <= DATASET_EVIDENCE_KEYS | {"acquired_via"},
                f"Source metadata {source_id} has a malformed dataset_id evidence record.",
            )
            require(record["dataset_id"] == dataset_id, f"Source metadata {source_id} dataset_id evidence is out of order.")
            require(
                nonempty_text(record["rule"]) and nonempty_text(record["detail"]), f"Source metadata {source_id} dataset_id evidence needs a rule and a detail."
            )
            require(
                "acquired_via" not in record or nonempty_text(record["acquired_via"]),
                f"Source metadata {source_id} dataset_id evidence needs a non-empty acquired_via.",
            )
            require(
                "acquired_via" not in record or record["acquired_via"] in ACQUIRED_VIA_ROUTES,
                f"Dataset_id {dataset_id} acquired_via is not a reviewed route; allowed routes: {sorted(ACQUIRED_VIA_ROUTES)}.",
            )
        approval = entry["approval"]
        require(isinstance(approval, dict) and set(approval) == APPROVAL_KEYS, f"Source metadata {source_id} needs a complete approval block.")
        require(nonempty_text(approval["decision"]) and nonempty_text(approval["recorded_in"]), f"Source metadata {source_id} approval must cite its decision.")
        require_user_approval(approval, f"Source metadata {source_id}")
    return indexed


def validate_publisher_evidence(evidence: Any, source_id: str, base_source: dict[str, Any]) -> None:
    """Require the publisher evidence to cite the base registry's landing URL and one retrieved, hashed page."""
    require(isinstance(evidence, dict), f"Source metadata {source_id} needs publisher evidence.")
    keys = set(evidence)
    require(
        PUBLISHER_EVIDENCE_KEYS <= keys <= PUBLISHER_EVIDENCE_KEYS | PUBLISHER_EVIDENCE_OPTIONAL_KEYS,
        f"Source metadata {source_id} has malformed publisher evidence.",
    )
    require(
        evidence["official_landing_url"] == base_source.get("official_landing_url"),
        f"Source metadata {source_id} evidence differs from the registry landing URL.",
    )
    for key in ("page_title", "naming_field", "naming_text"):
        require(nonempty_text(evidence[key]), f"Source metadata {source_id} evidence needs {key}.")
    require("evidence_note" not in evidence or nonempty_text(evidence["evidence_note"]), f"Source metadata {source_id} has an empty evidence note.")
    retrieved = evidence["retrieved_utc"]
    require(isinstance(retrieved, str) and RETRIEVED_PATTERN.fullmatch(retrieved) is not None, f"Source metadata {source_id} needs a UTC retrieval time.")
    try:
        timestamp = datetime.fromisoformat(retrieved)
        require(timestamp.tzinfo == UTC, f"Source metadata {source_id} needs a UTC retrieval time.")
    except ValueError as error:
        raise RegistryError(f"Source metadata {source_id} has an impossible retrieval time.") from error
    require(
        isinstance(evidence["page_sha256"], str) and SHA256_PATTERN.fullmatch(evidence["page_sha256"]) is not None,
        f"Source metadata {source_id} needs a page SHA-256.",
    )


def additions_lock(additions: dict[str, Any], registry: dict[str, Any]) -> dict[str, Any]:
    """Return the drift-detection lock for an additions document bound to the given registry."""
    controls = index_records(additions.get("measure_controls"), "id", "added measure control")
    lock = {
        "lock_version": 1,
        "base_registry_sha256": canonical_hash(registry),
        "additions_sha256": canonical_hash(additions),
        "control_ids": sorted(controls),
        "review_rule": REVIEW_RULE,
    }
    # The key exists only when the additions hold the section, so locks written before the section stay valid.
    if "source_metadata" in additions:
        lock["source_metadata_source_ids"] = sorted(index_records(additions["source_metadata"], "source_id", "source metadata"))
    return lock


def read_bronze_lineage(path: Path) -> dict[str, set[str]]:
    """Read an explicit aggregate gate CSV, retaining every observed source per dataset."""
    lineage: dict[str, set[str]] = {}
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, strict=True)
            columns = {"bronze_table", "_source_id", "_dataset_id", "n"}
            require(
                reader.fieldnames is not None and len(reader.fieldnames) == 4 and set(reader.fieldnames) == columns,
                "Bronze lineage needs exactly the gate CSV columns: bronze_table, _source_id, _dataset_id, n.",
            )
            for number, row in enumerate(reader, 2):
                require(
                    set(row) == columns
                    and all(nonempty_text(value) for value in row.values())
                    and DATASET_ID_PATTERN.fullmatch(row["_dataset_id"]) is not None
                    and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", row["_source_id"]) is not None
                    and re.fullmatch(r"[0-9]+", row["n"]) is not None
                    and int(row["n"]) > 0,
                    f"Bronze lineage has an invalid bronze row at line {number}.",
                )
                lineage.setdefault(row["_dataset_id"], set()).add(row["_source_id"])
    except (UnicodeError, csv.Error, ValueError) as error:
        raise RegistryError(f"Cannot read bronze lineage: {error}.") from error
    require(bool(lineage), "Bronze lineage must contain at least one row.")
    return lineage


def validate_bronze_lineage(additions: dict[str, Any], lineage: dict[str, set[str]]) -> None:
    """Require coverage of every bronze ID and reject absent placements or unaccounted sources."""
    evidence = {record["dataset_id"]: record for entry in additions.get("source_metadata", []) for record in entry["dataset_ids_evidence"]}
    for placement in additions.get("dataset_id_placements", []):
        dataset_id, source_id = placement["dataset_id"], placement["source_id"]
        require(dataset_id in lineage, f"Placed dataset_id {dataset_id} is absent from bronze lineage.")
        observed = lineage[dataset_id]
        via = evidence.get(dataset_id, {}).get("acquired_via")
        require(via is None or via in observed, f"Dataset_id {dataset_id} acquired_via is not shown by bronze.")
        require(via is None or via != source_id, f"Dataset_id {dataset_id} acquired_via duplicates its catalog source.")
        allowed = {source_id} | ({via} if via is not None else set())
        require(observed <= allowed, f"Placement {dataset_id} does not account for bronze sources {sorted(observed - allowed)}.")
    covered = {record["dataset_id"] for key in ("dataset_id_placements", "unplaced_dataset_ids") for record in additions.get(key, [])}
    missing = sorted(lineage.keys() - covered)
    require(not missing, f"Bronze dataset_ids are neither placed nor listed as unplaced: {missing}.")


def validate_additions(
    additions: dict[str, Any], lock: dict[str, Any], registry: dict[str, Any], bronze_lineage: dict[str, set[str]] | None = None
) -> list[dict[str, Any]]:
    """Verify additions against their lock and the current base registry, returning the added controls."""
    require(additions.get("additions_version") == 1 and lock.get("lock_version") == 1, "Unsupported additions or lock version.")
    base_hash = canonical_hash(registry)
    require(additions.get("base_registry_sha256") == base_hash == lock.get("base_registry_sha256"), "Additions are bound to a different registry; review them.")
    controls = index_records(additions.get("measure_controls"), "id", "added measure control")
    base_controls = index_records(registry.get("measure_controls"), "id", "measure control")
    base_sources = index_records(registry.get("sources"), "source_id", "source")
    for control in controls.values():
        validate_addition(control, base_controls, base_sources)
    require(sorted(controls) == lock.get("control_ids"), "Added control IDs differ from the additions lock.")
    if "source_metadata" in additions:
        metadata = validate_source_metadata(additions["source_metadata"], base_sources)
        require(sorted(metadata) == lock.get("source_metadata_source_ids"), "Source metadata IDs differ from the additions lock.")
    else:
        require("source_metadata_source_ids" not in lock, "The additions lock lists source metadata that the additions lack.")
    from scripts.acquisition.source_metadata import validate_placements

    validate_placements(additions, registry)
    if bronze_lineage is not None:
        validate_bronze_lineage(additions, bronze_lineage)
    require(canonical_hash(additions) == lock.get("additions_sha256"), "Additions differ from their lock; review the change before relocking.")
    return list(controls.values())


def load_registry_additions(
    registry: dict[str, Any], path: Path = ADDITIONS_PATH, lock_path: Path = ADDITIONS_LOCK_PATH, bronze_lineage: dict[str, set[str]] | None = None
) -> list[dict[str, Any]]:
    """Return validated controls; optional absent files must still satisfy an explicitly requested lineage gate."""
    initial_presence = (path.exists(), lock_path.exists())
    if initial_presence == (False, False):
        if bronze_lineage is not None:
            validate_bronze_lineage({}, bronze_lineage)
        return []
    require(all(initial_presence), "Registry additions and their lock must exist together.")
    if path == ADDITIONS_PATH and lock_path == ADDITIONS_LOCK_PATH:
        from scripts.acquisition.source_registry import registry_version_paths

        current = read_json(ADDITIONS_PATH)
        if current.get("base_registry_sha256") != canonical_hash(registry):
            legacy = registry_version_paths(canonical_hash(registry))
            path, lock_path = legacy["additions"], legacy["additions_lock"]
    present = (path.exists(), lock_path.exists())
    if present == (False, False):
        if bronze_lineage is not None:
            validate_bronze_lineage({}, bronze_lineage)
        return []
    require(all(present), "Registry additions and their lock must exist together.")
    try:
        return validate_additions(read_json(path), read_json(lock_path), registry, bronze_lineage)
    except (KeyError, TypeError, AttributeError) as error:
        raise RegistryError("Malformed registry additions or lock.") from error


def write_additions_lock(registry: dict[str, Any], path: Path, lock_path: Path, bronze_lineage: dict[str, set[str]] | None = None) -> str:
    """Create the lock once after validating every addition; an identical lock is left unchanged."""
    additions = read_json(path)
    try:
        lock = additions_lock(additions, registry)
        validate_additions(additions, lock, registry, bronze_lineage)
    except (KeyError, TypeError, AttributeError) as error:
        raise RegistryError("Malformed registry additions.") from error
    if lock_path.exists():
        require(read_json(lock_path) == lock, "Refusing to replace a stale additions lock; review the diff and remove it deliberately.")
        return "Additions lock already current."
    with lock_path.open("x", encoding="utf-8") as handle:
        handle.write(canonical_json(lock))
    return "Additions lock written. Review additions and lock together."


def canonical_json(value: dict[str, Any]) -> str:
    """Return the project's deterministic, reviewable JSON text for configuration files."""
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"


def main() -> None:
    """Validate the base registry plus additions, or write the additions lock, and report the totals."""
    parser = argparse.ArgumentParser(description="Validate user-approved registry additions offline. No downloads, AWS access or gate clearance.")
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--lock", type=Path, default=LOCK_PATH)
    parser.add_argument("--additions", type=Path, default=ADDITIONS_PATH)
    parser.add_argument("--additions-lock", type=Path, default=ADDITIONS_LOCK_PATH)
    parser.add_argument("--write-lock", action="store_true", help="Create the additions lock if absent; verify it if present.")
    parser.add_argument(
        "--bronze-lineage", type=Path, help="Require coverage and gate placements against an explicit bronze provenance CSV; otherwise check derivation only."
    )
    arguments = parser.parse_args()
    try:
        registry = load_registry(arguments.registry, arguments.lock)
        lineage = read_bronze_lineage(arguments.bronze_lineage) if arguments.bronze_lineage is not None else None
        message = write_additions_lock(registry, arguments.additions, arguments.additions_lock, lineage) if arguments.write_lock else None
        added = load_registry_additions(registry, arguments.additions, arguments.additions_lock, lineage)
    except (RegistryError, OSError) as error:
        parser.error(str(error))
    if message:
        sys.stdout.write(message + "\n")
    base = len(registry["measure_controls"])
    ids = ", ".join(control["id"] for control in added) or "none"
    metadata = read_json(arguments.additions).get("source_metadata", []) if arguments.additions.exists() else []
    sys.stdout.write(f"Registry additions valid: {len(added)} user-added measure controls ({ids}); base registry {canonical_hash(registry)[:12]} unchanged.\n")
    sys.stdout.write(f"Measure controls: {base} registered + {len(added)} added = {base + len(added)}. All gates remain not_run.\n")
    dataset_total = sum(len(entry["dataset_ids"]) for entry in metadata)
    sys.stdout.write(f"Source metadata: {len(metadata)} sources with a publisher; {dataset_total} dataset_ids placed.\n")
    sys.stdout.write("Bronze lineage gate passed.\n" if lineage is not None else "Bronze lineage gate not run: no lineage CSV supplied.\n")


if __name__ == "__main__":
    main()
