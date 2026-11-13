"""Acceptance of a plainly downloaded manual unit: its expected format, stored file names and source content contract.

The manual units are the HUD workbooks and the WONDER exports. A browser save carries file-birth and origin metadata that a host download cannot have,
so only the byte and content checks of the contracts run here: the plan's recorded bytes and the published layout. A failure never stores anything;
the caller marks the unit stale.
"""

from __future__ import annotations

import hashlib
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from scripts.acquisition import hud_xlsx_contract, transport, wonder_export_contract
from scripts.acquisition.source_registry import canonical_hash, read_json

# The mode of each unit that has a handler, its format and the derived file its contract writes beside the original.
MODES = {"hud_xlsx": ("xlsx", "crosswalk.csv"), "wonder_export": ("txt", "county_year.csv")}
# The modes whose derived file is rebuilt from the original: the bootstrap seed holds the original only (owner, Nov 13 2026).
REBUILT_MODES = ("wonder_export",)
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

    def plan(self, mode: str) -> dict:
        given = self.hud if mode == "hud_xlsx" else self.wonder
        return given if given is not None else locked_plan(hud_xlsx_contract.PLAN_PATH if mode == "hud_xlsx" else wonder_export_contract.PLAN_PATH)


def locked_plan(path: Path) -> dict:
    """A tracked plan whose lock digest matches. The terms and registry checks of the full loader belong to storage, not to this acceptance."""
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
    if len(originals) != 1 or derived not in names:
        raise ManualUnitError("A manual unit needs exactly one original and its derived file")
    try:
        transport.validate_request(str(unit.get("url", "")), originals[0], expected_format(mode), "data")
    except transport.CaptureError as refusal:
        raise ManualUnitError(str(refusal)) from refusal
    return mode, originals[0]


def expected_format(mode: str) -> str:
    return MODES[mode][0]


def accept(mode: str, original: Path, name: str, scopes: Scopes) -> dict[str, bytes]:
    """Check the downloaded original and return the derived file its contract writes. Raises ContentRefusal."""
    try:
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
    return {MODES["hud_xlsx"][1]: derived}


def wonder(original: Path, name: str, plan: dict) -> dict[str, bytes]:
    body = original.read_bytes()
    batch = bound(body, next((item for item in plan["batches"] if wonder_export_contract.stored_name(item) == name), None))
    derived, _statistics = wonder_export_contract.validate_export(body, batch, plan)
    return {MODES["wonder_export"][1]: derived}
