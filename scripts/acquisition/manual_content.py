"""Acceptance of a plainly downloaded manual unit: its expected format, stored file names and source content contract.

The manual units are the HUD workbooks and the WONDER exports. A browser save carries file-birth and origin metadata that a host download cannot have,
so only the byte and content checks of the contracts run here: the plan's recorded bytes and the published layout. A failure never stores anything;
the caller marks the unit stale.
"""

from __future__ import annotations

import hashlib
import re
import tempfile
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from scripts.acquisition import ahrq_compendium_contract, hud_xlsx_contract, redownload_route, transport, wonder_export_contract
from scripts.acquisition.mapped_archives import expand_tree
from scripts.acquisition.source_registry import canonical_hash, read_json

# The mode of each unit that has a handler, its format and the derived file its contract writes beside the original.
MODES = {"hud_xlsx": ("xlsx", "crosswalk.csv"), "wonder_export": ("txt", "county_year.csv"), "ahrq_compendium": (None, None), "hospital_archive": ("zip", None)}
# The format of an AHRQ file comes from its name: the six linkage workbooks are .xls or .xlsx and the four technical documents are .pdf.
SUFFIX_FORMATS = {".xlsx": "xlsx", ".xls": "xls", ".pdf": "pdf", ".zip": "zip"}
WORKBOOK_PART = "xl/workbook.xml"
# The modes whose derived file is rebuilt from the original: the bootstrap seed holds the original only (owner, Nov 13 2026).
REBUILT_MODES = ("hud_xlsx", "wonder_export")
# Units that take their original from the seed only, bind it to a locked plan and have no derived file (owner, Nov 19 2026: the AHRQ Compendium files).
SEED_ONLY_MODES = redownload_route.SEED_ONLY_MODES
CONTRACT_ERRORS = (ValueError, KeyError, TypeError, zipfile.BadZipFile)


class ManualUnitError(ValueError):
    """A unit or specification this handler cannot run. The run stops before any request."""


class ContentRefusal(Exception):
    """The downloaded bytes failed a check. ``reason`` is the stale reason the unit gets."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Scopes:
    """The locked plans the byte and content checks bind to. A plan left out is read from the tracked configuration when first needed."""

    hud: dict | None = None
    wonder: dict | None = None
    ahrq: dict | None = None

    def plan(self, mode: str) -> dict:
        given = {"hud_xlsx": self.hud, "wonder_export": self.wonder, "ahrq_compendium": self.ahrq}[mode]
        paths = {
            "hud_xlsx": hud_xlsx_contract.PLAN_PATH,
            "wonder_export": wonder_export_contract.PLAN_PATH,
            "ahrq_compendium": ahrq_compendium_contract.PLAN_PATH,
        }
        return given if given is not None else locked_plan(paths[mode])


def locked_plan(path: Path) -> dict:
    """A tracked plan whose lock digest matches. The registry and release record checks belong to storage and the plan tests."""
    plan = read_json(path)
    if read_json(path.with_suffix(".lock.json"))["plan_sha256"] != canonical_hash(plan):
        raise ManualUnitError("A plan lock differs from its plan")
    return plan


def plan_unit(unit: dict, spec: dict | None) -> tuple[str, str]:
    """The handler mode and the stored name of the downloaded original, or a refusal before any request."""
    mode = unit.get("mode")
    if mode == "file":
        raise ManualUnitError("A unit of mode file is not a manual unit: its ACS source moves to the Census API")
    if mode not in MODES:
        raise ManualUnitError(f"Manual units of mode {mode} have no content contract")
    if not isinstance(spec, dict) or spec.get("mode") != mode:
        raise ManualUnitError("A manual unit needs a capture specification of its own mode")
    names = spec.get("stored_data_files")
    if not (isinstance(names, list) and names and all(isinstance(name, str) and name for name in names) and len(set(names)) == len(names)):
        raise ManualUnitError("A manual unit needs its stored file names")
    derived = MODES[mode][1]
    originals = [name for name in names if name != derived]
    if derived is None:
        if len(names) != 1 or Path(names[0]).suffix.lower() not in SUFFIX_FORMATS:
            raise ManualUnitError("A seed-only unit needs exactly one stored file of a supported format")
    elif len(originals) != 1 or derived not in names:
        raise ManualUnitError("A manual unit needs exactly one original and its derived file")
    try:
        transport.validate_request(str(unit.get("url", "")), originals[0], expected_format(mode, originals[0]), "data")
    except transport.CaptureError as refusal:
        raise ManualUnitError(str(refusal)) from refusal
    return mode, originals[0]


def expected_format(mode: str, name: str | None = None) -> str:
    """The format a unit's original must have: the mode's, or for a mode with none the format of the stored file's suffix."""
    fixed = MODES[mode][0]
    if fixed is not None:
        return fixed
    return SUFFIX_FORMATS[Path(name or "").suffix.lower()]


def accept(mode: str, original: Path, name: str, scopes: Scopes, binding: dict | None = None) -> dict[str, bytes]:
    """Check the downloaded original and return the derived file its contract writes. Raises ContentRefusal.

    A hospital archive is bound by ``binding`` (see ``archive_binding``), not by a locked plan: the unit and its specification record what the file must be.
    """
    try:
        if mode == "hospital_archive":
            return hospital(original, binding)
        if mode == "ahrq_compendium":
            return ahrq(original, name, scopes.plan(mode))
        return hud(original, name, scopes.plan(mode)) if mode == "hud_xlsx" else wonder(original, name, scopes.plan(mode))
    except CONTRACT_ERRORS:
        raise ContentRefusal("contract_failed") from None


def recorded_derived(mode: str, spec: Mapping) -> tuple[str, int] | None:
    """The SHA-256 and size the unit's last capture records for its derived file, or None when it records none."""
    for item in (spec.get("last_capture") or {}).get("artifacts", []):
        if item.get("role") == "data" and (item.get("stored_file_name") or str(item["key"]).rsplit("/", 1)[-1]) == MODES[mode][1]:
            return item["sha256"], item["byte_count"]
    return None


def rebuild(mode: str, original: Path, name: str, spec: Mapping, scopes: Scopes) -> dict[str, bytes]:
    """The derived file of a seeded original, rebuilt by the contract and equal to the recorded file's SHA-256 and size. Raises ContentRefusal otherwise.

    The plan binds the original to its recorded digest first. Nothing is written here: the caller captures only what this returns.
    """
    recorded = recorded_derived(mode, spec)
    if recorded is None:
        raise ManualUnitError(f"The specification of a {mode} unit records no digest of its derived file {MODES[mode][1]}")
    derived = accept(mode, original, name, scopes)
    if any((hashlib.sha256(body).hexdigest(), len(body)) != recorded for body in derived.values()):
        raise ContentRefusal("contract_failed")
    return derived


def bound(body: bytes, batch: dict | None) -> dict:
    """The planned batch for these bytes, or a refusal: the plan binds each original to its recorded digest and length."""
    if batch is None or hashlib.sha256(body).hexdigest() != batch["sha256"] or len(body) != batch["bytes"]:
        raise ContentRefusal("contract_failed")
    return batch


def hud(original: Path, name: str, plan: dict) -> dict[str, bytes]:
    body = original.read_bytes()
    batch = bound(body, next((item for item in plan["batches"] if item["file_name"] == name), None))
    derived, _statistics = hud_xlsx_contract.validate_workbook(body, batch, plan)
    return {"crosswalk.csv": derived}


def wonder(original: Path, name: str, plan: dict) -> dict[str, bytes]:
    body = original.read_bytes()
    batch = bound(body, next((item for item in plan["batches"] if wonder_export_contract.stored_name(item) == name), None))
    derived, _statistics = wonder_export_contract.validate_export(body, batch, plan)
    return {"county_year.csv": derived}


def ahrq(original: Path, name: str, plan: dict) -> dict[str, bytes]:
    """An AHRQ original bound to the plan by its SHA-256 and size and checked for the format of its name. It has no derived file."""
    body = original.read_bytes()
    bound(body, next((item for item in plan["files"] if item["file_name"] == name), None))
    form = expected_format("ahrq_compendium", name)
    if transport.inspect_payload(original, form, "data") is not None:
        raise ContentRefusal("contract_failed")
    # The generic check accepts any package with a content types part, so a Word or PowerPoint file bound by the plan would pass as a workbook.
    if form == "xlsx" and WORKBOOK_PART not in zipfile.ZipFile(original).namelist():
        raise ContentRefusal("contract_failed")
    return {}


def archive_binding(unit: Mapping, spec: Mapping) -> dict:
    """What binds a seeded hospital archive: the SHA-256 its specification records in the receipt template, the size of its unit and its reviewed members."""
    digest = (spec.get("receipt_template") or {}).get("expected_sha256")
    if not (isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest)):
        raise ManualUnitError("The specification of a hospital archive unit records no digest of its archive")
    members = {tuple(item["member_chain"]) for item in spec.get("archive_members") or []}
    if not members:
        raise ManualUnitError("The specification of a hospital archive unit lists no member")
    return {"sha256": digest, "bytes": unit["bytes"], "members": members}


def hospital(original: Path, binding: dict | None) -> dict[str, bytes]:
    """A hospital archive bound to its recorded SHA-256 and size and to its reviewed members, with no derived file.

    The expansion needs room for the declared size of the archive: ``binding["workspace"]`` names the folder for it, and none names the system temporary folder.

    The members are read as capture mode reads them (``expand_tree``: the bounded, checksum-pinned expansion of nested archives, without the operating
    system packaging files), so a seeded archive passes here exactly when its capture would pass the member check.
    """
    if binding is None:
        raise ManualUnitError("A hospital archive is checked against the digest, size and members its unit records")
    bound(original.read_bytes(), binding)
    if transport.inspect_payload(original, "zip", "data") is not None:
        raise ContentRefusal("contract_failed")
    with tempfile.TemporaryDirectory(dir=binding.get("workspace")) as scratch:
        leaves = expand_tree(original, Path(scratch) / "leaves", binding["sha256"])
    if {tuple(chain) for chain in leaves} != binding["members"]:
        raise ContentRefusal("contract_failed")
    return {}
