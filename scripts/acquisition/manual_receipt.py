"""The receipt of a manual unit's checked files, written for the run's normal capture path.

A manual unit's files come from the seed folder or from a plain download that the handler (``manual_units.py``) already checked. They are not a download of
this task, so the receipt records them as a manual download: no HTTP status and no resolved address. The files are the stored data files of the unit's
specification (the original and, for a HUD or WONDER unit, the derived file the contract wrote), each linked into the snapshot folder by its own name. The
receipt then goes through the same storage check, intent, upload, read-back and completion record as any other unit. Capture mode's registry view and
validator decide whether the unit may be stored: this module adds no permission.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from scripts.acquisition import redownload
from scripts.acquisition.capture import artifact_record, base_receipt, validate_receipt
from scripts.acquisition.collection_layout import load_routes
from scripts.acquisition.history_routes import lineage_bindings
from scripts.acquisition.source_registry import canonical_hash
from scripts.acquisition.transport import Download

# What a manual unit of a mode without a reviewed receipt template says about itself. A unit that carries a receipt template (the CMS hospital archives)
# says what its template says.
SAVED_NOTE = "The original is the file the owner saved in a browser and the host run seeds. This capture checks its bytes and layout only."
RESTRICTION = "The project's terms and access records for this source govern its use. No reuse or model approval follows from this capture."
MODE_FACETS = {"ahrq_compendium": {"access_class": "public", "credentials_required": False}}
FORMAT_MEDIA = {".zip": "application/zip", ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xls": "application/vnd.ms-excel"}


def plan_of(unit: Mapping[str, Any], spec: Mapping[str, Any], route: Mapping[str, Any]) -> dict[str, Any]:
    """The receipt metadata of a unit: its specification's reviewed template, or the plain statement of a seeded manual original."""
    template = spec.get("receipt_template")
    if isinstance(template, dict):
        return {
            name: template[name] for name in ("publisher", "release", "measurement_periods", "governance", "export_selections", "scope") if name in template
        }
    facets = MODE_FACETS[unit["mode"]]
    saved: Mapping[str, Any] = spec["saved"] if isinstance(spec.get("saved"), dict) else {}
    return {
        "publisher": route["publisher"],
        "scope": f"One {unit['mode']} original as the owner saved it, with the file its contract derives. {SAVED_NOTE}",
        "release": {
            "publisher_release_label": saved.get("vintage"),
            "release_date": None,
            "dataset_version": None,
            "publisher_updated_at": None,
            "revision_status": "unknown",
            "advertised_history": None,
            "observed_history": SAVED_NOTE,
        },
        "measurement_periods": [
            {"label": None, "start_date": None, "end_date": None, "period_type": "unknown", "source_basis": "unknown", "target_alignment": "unknown"}
        ],
        "governance": {
            "access_class": facets["access_class"],
            "license_or_terms_url": None,
            "use_restrictions": [RESTRICTION],
            "credentials_required": facets["credentials_required"],
            "credential_reference": None,
            "contains_phi": False,
            "contains_pii": False,
        },
        "export_selections": {},
    }


def bindings_of(source: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, str]:
    """The release binding a held source needs before storage: the one the specification names, else the terms acceptance record."""
    if source["preferred_route"] != "access_hold":
        return {}
    return lineage_bindings(dict(source), {"release_binding": spec.get("release_binding", "terms")})


def local_download(path: Path, url: str, retrieved_at: str) -> Download:
    """A checked local file as the capture path's download record: complete, with no HTTP status and no resolved address."""
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    return Download(
        path,
        url,
        None,
        retrieved_at,
        None,
        path.stat().st_size,
        digest,
        FORMAT_MEDIA.get(path.suffix.lower()),
        True,
        None,
        False,
        1,
        original_file_name=path.name,
    )


def capture_local(
    unit: Mapping[str, Any],
    spec: Mapping[str, Any],
    files: Mapping[str, Path],
    folder: Path,
    registry: dict,
    validator: Draft202012Validator,
    now: datetime | None = None,
) -> Path:
    """Write the receipt of a unit's checked files under ``folder`` and return its path. The files keep their names; nothing is downloaded."""
    source = next(item for item in registry["sources"] if item["source_id"] == unit["source_id"])
    route = load_routes(registry)[unit["source_id"]]
    moment = now or datetime.now(UTC)
    retrieved = moment.isoformat().replace("+00:00", "Z")
    plan = plan_of(unit, spec, route)
    snapshot_id = f"{unit['source_id']}__{moment:%Y%m%dT%H%M%SZ}__{uuid.uuid4().hex}"
    root = folder / unit["source_id"] / snapshot_id
    (root / "raw").mkdir(parents=True)
    receipt = base_receipt(source | {"preferred_route": "file_download"}, plan, snapshot_id, validator)
    artifacts = []
    for number, (name, path) in enumerate(sorted(files.items(), key=lambda item: (item[0] not in spec["stored_data_files"][:1], item[0])), 1):
        target = root / "raw" / name
        os.link(path, target)
        record = artifact_record(local_download(target, unit["url"], retrieved), root, "data", number)
        compression = "zip" if target.suffix.lower() in {".zip", ".xlsx"} else None
        artifacts.append(record | {"hash_scope": "complete_file", "compression": compression})
    receipt["artifacts"] = artifacts
    receipt["acquisition"].update(
        transport_mode="download",
        requested_url=unit["url"],
        resolved_url=None,
        retrieved_at_utc=retrieved,
        http_status=None,
        request_method="manual_download",
        tool_name="manual_unit_seed_capture",
        tool_version="1.0.0",
        export_selections=plan["export_selections"],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "route_id": redownload.route_id_of(dict(unit), registry),
            "mode": unit["mode"],
            "scope": plan["scope"],
            "requests": [],
            "expected_periods": None,
            "expected_sha256": None,
            "fallback_reason": None,
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            **bindings_of(source, spec),
        }
    )
    receipt["quality_profile"]["checks_passed"] = ["manual_file_checked_by_unit_contract", "sha256_recorded"]
    validate_receipt(receipt, validator, root)
    path = root / "receipt.json"
    with path.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, ensure_ascii=True, allow_nan=False)
        handle.write("\n")
    return path
