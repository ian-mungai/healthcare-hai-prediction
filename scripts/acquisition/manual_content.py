"""Acceptance of a plainly downloaded manual unit: its expected format, stored file names and source content contract.

A browser save carries file-birth and origin metadata that a host download cannot have, so only the byte and content checks of the contracts run
here: the plan's recorded bytes and the published layout. A failure never stores anything; the caller marks the unit stale.
"""

from __future__ import annotations

import hashlib
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.acquisition import hud_xlsx_contract, wonder_export_contract
from scripts.acquisition.source_registry import canonical_hash, read_json

# The mode of each unit that has a handler, its default format and the derived file its contract writes beside the original.
MODES = {"hud_xlsx": ("xlsx", "crosswalk.csv"), "wonder_export": ("txt", "county_year.csv"), "file": (None, None)}
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
    if mode not in MODES:
        raise ManualUnitError(f"Manual units of mode {mode} have no content contract")
    if not isinstance(spec, dict) or spec.get("mode") != mode:
        raise ManualUnitError("A manual unit needs a capture specification of its own mode")
    names = spec.get("stored_data_files")
    derived = MODES[mode][1]
    if not (isinstance(names, list) and names and all(isinstance(name, str) and name for name in names) and len(set(names)) == len(names)):
        raise ManualUnitError("A manual unit needs its stored file names")
    originals = [name for name in names if name != derived]
    if len(originals) != 1 or (derived is not None and derived not in names):
        raise ManualUnitError("A manual unit needs exactly one original and, for a derived mode, its derived file")
    if expected_format(mode, spec) is None:
        raise ManualUnitError("A manual file unit needs an expected format")
    return mode, originals[0]


def expected_format(mode: str, spec: dict) -> str | None:
    return MODES[mode][0] or spec.get("expected_format")


def accept(mode: str, spec: dict, original: Path, name: str, scopes: Scopes) -> dict[str, bytes]:
    """Check the downloaded original and return the derived files its contract writes (none for a plain file). Raises ContentRefusal."""
    try:
        if mode == "hud_xlsx":
            return hud(original, name, scopes.plan(mode))
        if mode == "wonder_export":
            return wonder(original, name, scopes.plan(mode))
        return archive(original, spec)
    except CONTRACT_ERRORS:
        raise ContentRefusal("contract_failed") from None


def bound(body: bytes, batch: dict | None) -> dict:
    """The planned batch for these bytes, or a refusal: the plan binds each original to its recorded digest and length."""
    if batch is None or hashlib.sha256(body).hexdigest() != batch["sha256"] or len(body) != batch["bytes"]:
        raise ContentRefusal("contract_failed")
    return batch


def hud(original: Path, name: str, plan: dict) -> dict[str, bytes]:
    body = original.read_bytes()
    batch = bound(body, next((item for item in plan["batches"] if item["file_name"] == name), None))
    derived, _statistics = hud_xlsx_contract.validate_workbook(body, batch, plan)
    return {MODES["hud_xlsx"][1] or "": derived}


def wonder(original: Path, name: str, plan: dict) -> dict[str, bytes]:
    body = original.read_bytes()
    batch = bound(body, next((item for item in plan["batches"] if wonder_export_contract.stored_name(item) == name), None))
    derived, _statistics = wonder_export_contract.validate_export(body, batch, plan)
    return {MODES["wonder_export"][1] or "": derived}


def archive(original: Path, spec: dict) -> dict[str, bytes]:
    """A reviewed archive must hold exactly the members its map lists, at the top level of each member chain."""
    members: Any = spec.get("archive_members")
    if members is None:
        return {}
    wanted = {chain[0] for item in members for chain in [item["member_chain"]]}
    with zipfile.ZipFile(original) as package:
        found = {info.filename for info in package.infolist()}
    if found != wanted:
        raise ContentRefusal("contract_failed")
    return {}
