"""Read the approved candidate pool without downloading data or clearing review holds."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "config" / "acquisition" / "source_registry.json"
LOCK_PATH = REGISTRY_PATH.with_name("source_registry_lock.json")
VERSION_CATALOG_PATH = REGISTRY_PATH.with_name("registry_versions.json")
VERSION_CATALOG_LOCK_PATH = REGISTRY_PATH.with_name("registry_versions.lock.json")
# Private, Git-ignored archives of exact earlier registries (revision 1, then revision 2).
LEGACY_ARCHIVES = ("data/acquisition_planning/registry_legacy_20260929", "data/acquisition_planning/acquisition_legacy_20261009")
ROUTE_RULES = {
    "file_download": ("approved_file_first_with_checks", "conditional_route"),
    "web_export": ("approved_permitted_export_with_checks", "conditional_route"),
    "api_fallback": ("approved_api_fallback_with_checks", "conditional_route"),
    "file_plus_api_fallback": ("approved_file_first_then_api_fallback_with_checks", "conditional_route"),
    "documentation_only": ("approved_reference_only", "reference_only"),
    "access_hold": ("retained_access_hold", "access_hold"),
}


class RegistryError(ValueError):
    """Invalid or inconsistent registry input that cannot be used for acquisition."""

    pass


def require(condition: bool, message: str) -> None:
    """Raise the module-specific validation error when the required condition is false."""
    if not condition:
        raise RegistryError(message)


def canonical_hash(value: Any) -> str:
    """Return the SHA-256 of the canonical finite JSON representation."""
    content = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Return a JSON object from key/value pairs, rejecting duplicate keys."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        require(key not in result, f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def reject_constant(value: str) -> None:
    """Reject non-finite numeric constants encountered while decoding JSON."""
    raise RegistryError(f"Invalid JSON number: {value}")


def parse_json(content: bytes, name: str) -> dict[str, Any]:
    """Decode a finite JSON object from bytes and report invalid input with its source label."""
    try:
        result = json.loads(content, object_pairs_hook=unique_object, parse_constant=reject_constant)
    except ValueError as error:
        raise RegistryError(f"Cannot read {name}: {error}") from error
    require(isinstance(result, dict), f"{name} must contain a JSON object.")
    return result


def read_json(path: Path) -> dict[str, Any]:
    """Read a finite JSON object from a path or raise a contextual registry error."""
    try:
        content = path.read_bytes()
    except OSError as error:
        raise RegistryError(f"Cannot read {path.name}: {error}") from error
    return parse_json(content, path.name)


def index_records(records: Any, key: str, label: str) -> dict[str, dict[str, Any]]:
    """Validate unique nonempty record keys and return records indexed by that key."""
    require(isinstance(records, list), f"{label} must be a list.")
    result: dict[str, dict[str, Any]] = {}
    for record in records:
        require(isinstance(record, dict), f"Invalid {label} record.")
        identifier = record.get(key)
        require(isinstance(identifier, str) and bool(identifier.strip()), f"Missing {label} {key}.")
        require(identifier not in result, f"Duplicate {label} ID: {identifier}")
        result[identifier] = record
    return result


def registry_counts(sources: list[dict[str, Any]]) -> dict[str, Any]:
    """Return source counts by retained transport approval and hold status."""
    routes = Counter(source["preferred_route"] for source in sources)
    decisions = Counter(source["approval"]["transport_decision"] for source in sources)
    return {
        "source_records": len(sources),
        "route_categories": dict(sorted(routes.items())),
        "transport_decisions": dict(sorted(decisions.items())),
        "numeric_routes_approved_with_conditions": sum(count for route, count in routes.items() if ROUTE_RULES[route][1] == "conditional_route"),
        "reference_only": routes["documentation_only"],
        "access_holds_retained": routes["access_hold"],
    }


def validate_registry(registry: dict[str, Any], lock: dict[str, Any]) -> None:
    """Verify registry contents against the lock and preserved acquisition restrictions."""
    require(registry.get("registry_version") == 1 and lock.get("lock_version") == 1, "Unsupported registry or lock version.")
    sources = index_records(registry.get("sources"), "source_id", "source")
    controls = index_records(registry.get("measure_controls"), "id", "measure control")
    require(sorted(sources) == lock.get("source_ids"), "Source IDs differ from the approved lock; a record is missing or substituted.")
    require(sorted(controls) == lock.get("control_ids"), "Measure controls differ from the approved lock.")
    require(registry.get("inputs") == lock.get("inputs"), "Input hashes differ from the approved lock.")
    require(registry.get("execution_status") == "not_started", "The registry is not an execution receipt.")
    route_ids: set[str] = set()
    url_sources: dict[str, set[str]] = defaultdict(set)
    for source_id, source in sources.items():
        route = source.get("preferred_route", "")
        require(route in ROUTE_RULES, f"Unknown preferred route for {source_id}.")
        decision, disposition = ROUTE_RULES[route]
        approval = source.get("approval", {})
        planning = source.get("planning", {})
        require(approval.get("source_id") == source_id, f"Approval identity differs for {source_id}.")
        require(approval.get("preferred_route") == route, f"Approval route differs for {source_id}.")
        require(approval.get("transport_decision") == decision, f"Approval decision differs for {source_id}.")
        require(approval.get("numeric_route") is (disposition == "conditional_route"), f"Numeric route flag differs for {source_id}.")
        require(approval.get("approval_scope") == "transport_strategy_only", f"Approval scope was broadened for {source_id}.")
        require(approval.get("modeling_eligibility") == "not_decided_by_transport_approval", f"Model eligibility was cleared for {source_id}.")
        require(approval.get("bulk_acquisition_executed") is False, f"Acquisition was incorrectly marked complete for {source_id}.")
        require(planning.get("source_id") == source_id and planning.get("disposition") == disposition, f"Planning differs for {source_id}.")
        require(planning.get("acquisition_executed") is False, f"Planning was incorrectly marked complete for {source_id}.")
        links = source.get("linked_measure_ids", [])
        require(isinstance(links, list) and links == approval.get("linked_measure_ids"), f"Measure links differ for {source_id}.")
        require(all(isinstance(item, str) and item in controls for item in links), f"Unknown linked measure for {source_id}.")
        require(source.get("required_checks") == approval.get("required_checks"), f"Required checks differ for {source_id}.")
        validation = source.get("validation", {})
        require(validation.get("source_id") == source_id, f"Validation identity differs for {source_id}.")
        require(validation.get("required_checks_verbatim") == source.get("required_checks"), f"Validation checks differ for {source_id}.")
        require(all(gate.get("status") == "not_run" for gate in validation.get("gates", [])), f"A gate was cleared for {source_id}.")
        files = index_records(source.get("file_routes"), "route_id", f"{source_id} file route")
        require(len(files) == approval.get("file_route_count"), f"File route count differs for {source_id}.")
        for route_id, file_route in files.items():
            require(route_id not in route_ids, f"Duplicate file route ID: {route_id}")
            route_ids.add(route_id)
            url = file_route.get("url", "")
            require(isinstance(url, str) and url.startswith(("https://", "http://")), f"Invalid file route URL: {route_id}")
            url_sources[url].add(source_id)
    for control_id, control in controls.items():
        require(control.get("gate_status") == "not_run", f"A measure gate was cleared for {control_id}.")
        require(set(control.get("source_ids", [])) <= sources.keys(), f"Unknown source for {control_id}.")
        require(control.get("parent_id") is None or control["parent_id"] in controls, f"Unknown parent for {control_id}.")
    expected_shared = {url: sorted(ids) for url, ids in url_sources.items() if len(ids) > 1}
    shared = index_records(registry.get("shared_file_routes"), "url", "shared file")
    require({url: item.get("source_ids") for url, item in shared.items()} == expected_shared, "Shared-file relationships differ from source routes.")
    require(registry_counts(list(sources.values())) == registry.get("counts") == lock.get("counts"), "Source counts differ from the approved lock.")
    require(canonical_hash(registry) == lock.get("registry_sha256"), "Registry content differs from the approved lock; review the change before rebuilding.")


@lru_cache(maxsize=2)
def _verified_exclusions(registry_raw: bytes, lock_raw: bytes) -> frozenset[str]:
    """Cache only immutable exclusions keyed by the exact validated metadata bytes."""
    registry = parse_json(registry_raw, "current registry")
    lock = parse_json(lock_raw, "current registry lock")
    validate_registry(registry, lock)
    return frozenset(source["source_id"] for source in registry["sources"] if source.get("collection_scope") == "excluded_by_user_decision")


def require_collection_scope(source_id: str) -> None:
    """Recheck metadata bytes and reject exclusions; this grants no route approval."""
    try:
        excluded = _verified_exclusions(REGISTRY_PATH.read_bytes(), LOCK_PATH.read_bytes())
    except OSError as error:
        raise RegistryError("Current collection scope is unavailable") from error
    require(source_id not in excluded, "Source excluded from collection")


def registry_version_paths(expected_sha256: str) -> dict[str, Path]:
    """Resolve one explicitly catalogued legacy version; reject drift, traversal and unknown hashes."""
    catalog = read_json(VERSION_CATALOG_PATH)
    require(catalog.get("catalog_version") == 1, "Unsupported registry version catalog")
    require(canonical_hash(catalog) == read_json(VERSION_CATALOG_LOCK_PATH).get("catalog_sha256"), "Registry version catalog differs from its lock")
    versions = [item for item in catalog.get("legacy_versions", []) if item.get("registry_sha256") == expected_sha256]
    require(len(versions) == 1, "Unknown or ambiguous registry fingerprint")
    paths = {}
    for key, relative in versions[0]["files"].items():
        path = REPO_ROOT / relative["path"]
        require(
            any(path.resolve().is_relative_to((REPO_ROOT / archive).resolve()) for archive in LEGACY_ARCHIVES),
            "Legacy registry path escapes its archive",
        )
        require(path.is_file() and not path.is_symlink(), "Legacy registry file is missing or symlinked")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == relative["sha256"], "Legacy registry file differs from its catalogued bytes")
        paths[key] = path
    return paths


@lru_cache(maxsize=2)
def _validated_registry_bytes(registry_raw: bytes, lock_raw: bytes) -> bool:
    """Cache validation only; exact immutable bytes bind both inputs, never returned mutable data."""
    try:
        validate_registry(parse_json(registry_raw, "registry"), parse_json(lock_raw, "lock"))
    except (KeyError, TypeError, AttributeError) as error:
        raise RegistryError("Malformed registry or approval lock.") from error
    return True


def load_registry(path: Path = REGISTRY_PATH, lock_path: Path = LOCK_PATH, *, expected_sha256: str | None = None) -> dict[str, Any]:
    """Read fresh inputs and return a private parsed object after exact-byte validation."""
    try:
        raw = path.read_bytes()
        lock_raw = lock_path.read_bytes()
        _validated_registry_bytes(raw, lock_raw)
        registry = json.loads(raw)
        if expected_sha256 is not None and expected_sha256 != canonical_hash(registry):
            paths = registry_version_paths(expected_sha256)
            path, lock_path = paths["registry"], paths["lock"]
            raw = path.read_bytes()
            lock_raw = lock_path.read_bytes()
            _validated_registry_bytes(raw, lock_raw)
            registry = json.loads(raw)
    except OSError as error:
        raise RegistryError(f"Cannot read registry inputs: {error}") from error
    _validated_registry_bytes(raw, lock_raw)
    require(expected_sha256 is None or canonical_hash(registry) == expected_sha256, "Selected registry fingerprint differs")
    return registry


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description="Validate the complete source registry offline. No downloads, AWS access or gate clearance.")
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--lock", type=Path, default=LOCK_PATH)
    arguments = parser.parse_args()
    try:
        registry = load_registry(arguments.registry, arguments.lock)
    except RegistryError as error:
        parser.error(str(error))
    counts = registry["counts"]
    sys.stdout.write(
        str(
            f"Source registry valid: {counts['source_records']} sources; {counts['numeric_routes_approved_with_conditions']} conditional numeric routes; "
            f"{counts['reference_only']} reference-only records; {counts['access_holds_retained']} access holds retained."
        )
        + "\n"
    )
    sys.stdout.write(
        str(f"Retained {len(registry['measure_controls'])} measure controls and {len(registry['shared_file_routes'])} shared-file relationships.") + "\n"
    )
    sys.stdout.write("No data acquired. Historical coverage and model eligibility remain subject to the preserved checks." + "\n")


if __name__ == "__main__":
    main()
