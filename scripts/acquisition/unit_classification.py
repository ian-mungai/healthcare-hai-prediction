"""The reviewed classification that lets a queue unit run: non personal, or personal and so kept out of the tool.

A unit runs only if the classification file beside the queue holds a reviewed non personal entry for it. A missing file, a missing entry or an entry still
marked ``proposed`` is never read as non personal. ``propose`` writes a first draft from public queue metadata and marks every unit ``proposed``. It opens
no receipt and no stored file. The owner reviews each entry, sets ``status`` to ``reviewed`` and names themselves in ``reviewed_by``, then ``seal`` writes the
file's digest again. ``seal`` changes no classification and no status.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any

from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import RegistryError

NAME = "unit_classification.json"
KIND = "unit_classification"
NON_PERSONAL, PERSONAL = "non_personal", "personal"
PROPOSED, REVIEWED = "proposed", "reviewed"
ACTIVE = ("api", "direct_download", "approved_manual")
# Public queue metadata that marks an original as naming people. Nothing here is read from a stored file.
PERSONAL_FLAG = "original_contains_personal_details"
PERSONAL_COLLECTORS = frozenset({"cms_owners_org", "hcai_util_workbook", "onc_mu_hospital"})
# The CMS hospitals archives hold a table of named state contacts (privacy review finding F2).
CONTACT_TABLE_SOURCES = frozenset({"main-hai-pdc"})
NO_MARKER = "no_personal_marker_in_queue_flags_collector_mode_or_contact_table_sources"


def classification_path(queue_path: Path) -> Path:
    return queue_path.with_name(NAME)


def body_digest(document: Mapping[str, Any]) -> str:
    return hashlib.sha256(encoded_json({key: value for key, value in document.items() if key != "classification_sha256"})).hexdigest()


def proposal_source(unit: Mapping[str, Any]) -> tuple[str, str]:
    """The classification this unit's public metadata suggests, and the metadata that suggests it."""
    flag = next((str(item) for item in unit.get("flags", []) if str(item).startswith(PERSONAL_FLAG)), None)
    if flag is not None:
        return PERSONAL, f"queue_flag:{PERSONAL_FLAG}"
    if unit.get("mode") in PERSONAL_COLLECTORS:
        return PERSONAL, f"collector_mode:{unit['mode']}"
    if unit.get("source_id") in CONTACT_TABLE_SOURCES:
        return PERSONAL, f"contact_table_source:{unit['source_id']}"
    return NON_PERSONAL, NO_MARKER


def propose(queue_body: bytes) -> dict[str, Any]:
    """Draft the classification of every active unit from the queue metadata alone. Every entry is ``proposed``, never ``reviewed``."""
    units: dict[str, dict[str, str]] = {}
    for unit in json.loads(queue_body)["queue"]:
        if unit["disposition"] in ACTIVE:
            classification, source = proposal_source(unit)
            units[unit["snapshot_id"]] = {"classification": classification, "status": PROPOSED, "source": source}
    document: dict[str, Any] = {"kind": KIND, "queue_sha256": hashlib.sha256(queue_body).hexdigest(), "reviewed_by": None, "units": units}
    return {**document, "classification_sha256": body_digest(document)}


def read_document(body: bytes, queue_sha256: str) -> dict[str, Any]:
    """Return the classification only if it is whole and belongs to this queue. Any other state refuses the run."""
    try:
        document = json.loads(body)
        units, reviewer = document["units"], document["reviewed_by"]
        valid = document["kind"] == KIND and isinstance(units, dict) and (reviewer is None or isinstance(reviewer, str))
        stale = document["queue_sha256"] != queue_sha256
        changed = document["classification_sha256"] != body_digest(document)
    except (ValueError, KeyError, TypeError, AttributeError):
        raise RegistryError("The unit classification is unreadable or incomplete") from None
    if not valid:
        raise RegistryError("The unit classification is unreadable or incomplete")
    if stale:
        raise RegistryError("The unit classification belongs to another queue")
    if changed:
        raise RegistryError("The unit classification differs from its digest; the owner seals it after a review")
    return document


def reviewed(body: bytes, queue_sha256: str) -> Mapping[str, str]:
    """The units the owner reviewed, mapped to their classification. A unit absent from the map has no reviewed classification and does not run."""
    document = read_document(body, queue_sha256)
    found: dict[str, str] = {}
    for identity, entry in document["units"].items():
        if isinstance(entry, dict) and entry.get("status") == REVIEWED and entry.get("classification") in (NON_PERSONAL, PERSONAL):
            found[identity] = entry["classification"]
    if found and not (document["reviewed_by"] or "").strip():
        raise RegistryError("The unit classification has reviewed entries but no named reviewer")
    return MappingProxyType(found)


def personal_units(body: bytes, queue_sha256: str) -> list[str]:
    """Every unit classified as personal, reviewed or only proposed. The owner's local script reads bytes for these and never for another unit."""
    return sorted(identity for identity, entry in read_document(body, queue_sha256)["units"].items() if entry.get("classification") == PERSONAL)


def counts(document: Mapping[str, Any]) -> dict[str, Any]:
    entries = document["units"].values()
    return {
        "units": len(document["units"]),
        "by_status": dict(sorted(Counter(entry["status"] for entry in entries).items())),
        "by_classification": dict(sorted(Counter(entry["classification"] for entry in entries).items())),
        "by_source": dict(sorted(Counter(f"{entry['classification']}:{entry['source']}" for entry in entries).items())),
    }


def seal(path: Path) -> dict[str, Any]:
    """Write the file's digest again after the owner's review. This changes no classification and no status."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("kind") != KIND or not isinstance(document.get("units"), dict):
        raise RegistryError("The file is not a unit classification")
    path.write_bytes(encoded_json({**document, "classification_sha256": body_digest(document)}))
    return document


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Propose the unit classification from queue metadata, or seal the owner's reviewed file.")
    commands = parser.add_subparsers(dest="command", required=True)
    proposing = commands.add_parser("propose")
    proposing.add_argument("--queue", required=True, type=Path)
    proposing.add_argument("--output", type=Path, help="defaults to the classification file beside the queue")
    sealing = commands.add_parser("seal")
    sealing.add_argument("--file", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "propose":
            document = propose(args.queue.read_bytes())
            write_once(args.output or classification_path(args.queue), encoded_json(document))
        else:
            document = seal(args.file)
    except (OSError, ValueError, KeyError, TypeError, RegistryError) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps(counts(document), indent=2) + "\n")


if __name__ == "__main__":
    main()
