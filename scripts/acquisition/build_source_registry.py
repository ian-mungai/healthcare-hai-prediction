"""Import hash-bound external approvals into portable runtime configuration."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import RegistryError, canonical_hash, index_records, parse_json, read_json, registry_counts, require, validate_registry

INPUT_NAMES = ("acquisition-manifest.json", "acquisition-route-approvals.json", "acquisition-waves.json", "source-validation-gates.json")
SOURCE_FIELDS = (
    "source_id",
    "title",
    "preferred_route",
    "route_status",
    "official_landing_url",
    "api_fallback",
    "measurement_history",
    "api_file_equivalence",
    "required_checks",
    "evidence_urls",
    "linked_measure_ids",
    "original_source_family_ids",
)


def portable(value: Any) -> Any:
    """Recursively replace local home-directory prefixes with portable evidence references."""
    # Keep external evidence references without publishing workstation usernames or assuming local access.
    if isinstance(value, str):
        return re.sub(r"/(?:Users|home)/[^/\s]+", "external://home", value)
    if isinstance(value, list):
        return [portable(item) for item in value]
    if isinstance(value, dict):
        return {key: portable(item) for key, item in value.items()}
    return value


def reference(pointer: str, record: dict[str, Any]) -> dict[str, str]:
    """Return the JSON pointer and canonical checksum for an approved input record."""
    return {"input": "acquisition-manifest.json", "json_pointer": pointer, "record_sha256": canonical_hash(record)}


def build_registry(documents: dict[str, dict[str, Any]], hashes: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a registry and lock from hash-bound approvals without clearing source restrictions."""
    manifest, approvals, waves, gates = (documents[name] for name in INPUT_NAMES)
    require(approvals["input_manifest"]["sha256"] == hashes[INPUT_NAMES[0]], "Approvals do not match the manifest hash.")
    for document in (waves, gates):
        for name in INPUT_NAMES[:2]:
            require(document["inputs"][name]["sha256"] == hashes[name], f"Stale input binding: {name}")
    sources = index_records(manifest["source_records"], "source_id", "manifest source")
    approved = index_records(approvals["records"], "source_id", "approval")
    planned = index_records(waves["source_records"], "source_id", "wave source")
    validations = index_records(gates["source_records"], "source_id", "validation source")
    require(sources.keys() == approved.keys() == planned.keys() == validations.keys(), "Source identity sets differ between approved inputs.")
    originals: dict[str, dict[str, Any]] = {}
    for collection in ("original_entries", "child_entries", "supplemental_closed_field_variants"):
        records = index_records(manifest[collection], "id", collection)
        require(not originals.keys() & records.keys(), "Duplicate measure IDs across manifest collections.")
        for index, record in enumerate(manifest[collection]):
            originals[record["id"]] = reference(f"/{collection}/{index}", record)
    controls = index_records(gates["measure_controls"], "id", "measure control")
    require(originals.keys() == controls.keys(), "Measure control identities differ from the full manifest.")
    for control_id, control in controls.items():
        original = originals[control_id]
        require(control["canonical_record"]["record_sha256"] == original["record_sha256"], f"Stale measure control: {control_id}")
        require(control["canonical_record"]["json_pointer"] == original["json_pointer"], f"Wrong measure reference: {control_id}")
    output_sources = []
    for index, source in enumerate(manifest["source_records"]):
        source_id = source["source_id"]
        original = reference(f"/source_records/{index}", source)
        validation = validations[source_id]
        require(validation["source_reference"]["record_sha256"] == original["record_sha256"], f"Stale source validation: {source_id}")
        require(validation["source_reference"]["json_pointer"] == original["json_pointer"], f"Wrong source reference: {source_id}")
        output = {field: source[field] for field in SOURCE_FIELDS if field in source}
        output["file_routes"] = []
        for route_index, route in enumerate(source["file_routes"]):
            retained = {key: value for key, value in route.items() if key != "inspected_evidence"}
            retained["research_reference"] = reference(f"/source_records/{index}/file_routes/{route_index}", route)
            output["file_routes"].append(retained)
        output.update({"approval": approved[source_id], "planning": planned[source_id], "research_reference": original})
        output["validation"] = {key: value for key, value in validation.items() if key not in {"api_fallback_verbatim", "source_reference"}}
        output_sources.append(output)
    registry = portable(
        {
            "registry_version": 1,
            "execution_status": "not_started",
            "inputs": {name: {"sha256": hashes[name]} for name in INPUT_NAMES},
            "evidence_reference_policy": (
                "external://home references identify external research, not runtime files or acquired snapshots. Gates remain unresolved."
            ),
            "acquisition_policy": manifest["acquisition_policy"],
            "global_conditions": approvals["global_conditions"],
            "limitations": manifest["limitations"],
            "source_families": manifest["original_source_pool"],
            "counts": registry_counts(output_sources),
            "sources": output_sources,
            "measure_controls": gates["measure_controls"],
            "gate_catalog": gates["gate_catalog"],
            "shared_file_routes": waves["shared_file_routes"],
            "history_rule": waves["history_rule"],
            "comparison_rule": waves["comparison_rule"],
            "sequencing_rule": waves["sequencing_rule"],
            "validation_status_policy": gates["status_policy"],
            "validation_threshold_policy": gates["threshold_policy"],
        }
    )
    require(registry["counts"] == approvals["counts"], "Source counts differ from the approved input.")
    lock = {
        "lock_version": 1,
        "registry_sha256": canonical_hash(registry),
        "source_ids": sorted(sources),
        "control_ids": sorted(controls),
        "counts": registry["counts"],
        "inputs": registry["inputs"],
        "review_rule": "Review registry and lock diffs together against external approvals. This lock detects drift; it is not a signature or new approval.",
    }
    validate_registry(registry, lock)
    return registry, lock


def import_registry(audit_directory: Path, output_directory: Path, check: bool = False) -> None:
    """Write new registry files or verify existing files against the supplied approved inputs."""
    contents = {name: (audit_directory / name).read_bytes() for name in INPUT_NAMES}
    documents = {name: parse_json(content, name) for name, content in contents.items()}
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in contents.items()}
    registry, lock = build_registry(documents, hashes)
    outputs = {"source_registry.json": registry, "source_registry_lock.json": lock}
    for name, value in outputs.items():
        path = output_directory / name
        if check:
            require(path.is_file() and read_json(path) == value, f"Generated configuration is stale: {name}")
        else:
            require(not path.exists(), f"Refusing to replace {name}; generate into a separate directory and review both diffs.")
    if not check:
        output_directory.mkdir(parents=True, exist_ok=True)
        for name, value in outputs.items():
            with (output_directory / name).open("x", encoding="utf-8") as handle:
                handle.write(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n")


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description="Import the complete approved source registry. Does not download datasets or contact AWS.")
    parser.add_argument("--audit-directory", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--check", action="store_true", help="Compare existing output with approved inputs without writing files.")
    arguments = parser.parse_args()
    try:
        import_registry(arguments.audit_directory, arguments.output_directory, arguments.check)
    except (RegistryError, OSError, KeyError, TypeError, AttributeError) as error:
        parser.error(str(error))
    sys.stdout.write(
        str("Source registry matches approved inputs." if arguments.check else "Source registry generated. Review both registry and lock before use.") + "\n"
    )


if __name__ == "__main__":
    main()
