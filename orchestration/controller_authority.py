"""Validate sibling controller ownership and persist owner commands in the registry transaction."""

from __future__ import annotations

import copy
import hashlib
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.lakehouse.catalog_journal import durable_directory, encoded, identifier, write_once

Document = dict[str, Any]
FIELDS = ("controller_owner", "controller_epoch", "controller_phase", "controller_transfer_id", "controller_state_digest")
BINDING_FIELDS = ("registry_id", "controller_owner", "controller_epoch", "controller_transfer_id", "controller_state_digest")


class ControllerError(ValueError):
    """A stable refusal at the controller ownership boundary."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def epoch_number(value: object, *, initial: bool = False) -> int:
    """Return a validated controller epoch, including zero only before initialization."""
    if type(value) is not int or value < (0 if initial else 1):
        raise ControllerError("controller_record")
    return value


def controller_record(value: object) -> Document:
    """Validate the closed controller record without changing the catalog schema."""
    if not isinstance(value, dict) or set(value) != {"version", *FIELDS} or type(value["version"]) is not int or value["version"] != 1:
        raise ControllerError("controller_record")
    epoch_number(value["controller_epoch"])
    owner, phase = value["controller_owner"], value["controller_phase"]
    if owner not in ("mac", "none", "aws") or phase not in ("active", "draining", "prepared", "recovering"):
        raise ControllerError("controller_record")
    if (owner == "none") != (phase in ("prepared", "recovering")):
        raise ControllerError("controller_record")
    try:
        identifier(value["controller_transfer_id"])
    except (TypeError, ValueError):
        raise ControllerError("controller_record") from None
    if not isinstance(value["controller_state_digest"], str) or re.fullmatch(r"[a-f0-9]{64}", value["controller_state_digest"]) is None:
        raise ControllerError("controller_record")
    return copy.deepcopy(value)


def read_authority(state: Mapping[str, Any]) -> Document:
    """Read the sibling record. Missing authority never means permission to dispatch."""
    return controller_record(state.get("launcher", {}).get("controller_authority"))


def confirmation_text(command: Mapping[str, Any]) -> str:
    """Return the exact confirmation required at the owner terminal."""
    return f"{command.get('command_id')} {command.get('expected_epoch')} {command.get('action')} {command.get('transfer_id')}"


def _proof(state: Mapping[str, Any], command: Document, evidence: Mapping[str, Any] | None, *facts: str) -> None:
    catalog = state["launcher"].get("catalog_authority")
    if (
        evidence is None
        or any(evidence.get(fact) is not True for fact in facts)
        or evidence.get("state_digest") != command["state_digest"]
        or evidence.get("catalog_sha256") != hashlib.sha256(encoded(catalog)).hexdigest()
        or not isinstance(catalog, dict)
        or catalog.get("state") != "aws_writer"
    ):
        raise ControllerError("controller_evidence")


def _next(state: Document, current: Document | None, command: Document, evidence: Mapping[str, Any] | None) -> Document:
    expected = epoch_number(command["expected_epoch"], initial=True)
    if expected != (current["controller_epoch"] if current else 0):
        raise ControllerError("stale_controller_epoch")
    action = command["action"]
    if current is None:
        if action != "initialize":
            raise ControllerError("controller_transition")
        return controller_record(
            {
                "version": 1,
                "controller_owner": "mac",
                "controller_epoch": 1,
                "controller_phase": "active",
                "controller_transfer_id": command["transfer_id"],
                "controller_state_digest": command["state_digest"],
            }
        )
    record = copy.deepcopy(current)
    owner, phase = current["controller_owner"], current["controller_phase"]
    if action != "drain" and command["transfer_id"] != current["controller_transfer_id"]:
        raise ControllerError("controller_transfer_conflict")
    if action not in ("drain", "prepare") and command["state_digest"] != current["controller_state_digest"]:
        raise ControllerError("controller_state_conflict")
    if action == "drain" and phase == "active" and owner in ("mac", "aws"):
        record.update(controller_phase="draining", controller_transfer_id=command["transfer_id"])
    elif action == "prepare" and phase == "draining" and owner == "mac":
        _proof(state, command, evidence, "import_verified", "source_stopped", "ecs_reconciled", "no_pending_writes", "no_source_mutations")
        record.update(controller_owner="none", controller_phase="prepared", controller_epoch=expected + 1, controller_state_digest=command["state_digest"])
    elif action == "activate" and phase in ("prepared", "recovering") and owner == "none":
        _proof(state, command, evidence, "import_verified", "source_stopped", "ecs_reconciled", "iam_fenced")
        record.update(controller_owner="aws", controller_phase="active")
    elif action == "recover" and phase in ("active", "draining", "prepared"):
        record.update(controller_owner="none", controller_phase="recovering")
    elif action == "rollback" and phase == "recovering" and owner == "none":
        if evidence is None or evidence.get("latch_absent") is not True:
            raise ControllerError("controller_latch_absence")
        if state["launcher"].get("controller_dispatch_latch", {}).get("status") in ("pending", "confirmed"):
            raise ControllerError("controller_latch_exists")
        _proof(state, command, evidence, "ecs_reconciled", "no_admitted_tries", "no_pending_writes", "no_source_mutations")
        record["controller_epoch"] = expected + 1
    elif action == "resume_mac" and phase == "recovering" and owner == "none":
        receipts = state["launcher"].get("controller_transfer_commands", {}).values()
        if not any(receipt.get("action") == "rollback" and receipt.get("outcome", {}).get("controller_epoch") == expected for receipt in receipts):
            raise ControllerError("controller_latch_absence")
        _proof(state, command, evidence, "ecs_reconciled", "no_admitted_tries", "copy_back_verified", "iam_reversed")
        record.update(controller_owner="mac", controller_phase="active", controller_epoch=expected + 1)
    else:
        raise ControllerError("controller_transition")
    return controller_record(record)


def owner_command(
    state: Document, command: Document, *, intent_directory: Path, owner: bool, tty: bool, confirmation: str, evidence: Mapping[str, Any] | None = None
) -> Document:
    """Persist an immutable intent before changing ownership in the caller's exclusive registry transaction.

    External transfer facts come from the approved transfer adapter. The launcher supplies its own remote latch absence
    for rollback. A completed command always replays its original outcome, even after another owner command.
    A conflict with a durable interrupted intent raises ControllerError with code ``command_conflict``.
    """
    if owner is not True or tty is not True or confirmation != confirmation_text(command):
        raise ControllerError("owner_confirmation")
    required = {"command_id", "action", "expected_epoch", "transfer_id", "state_digest", "reason", "time"}
    if not isinstance(command, dict) or set(command) != required:
        raise ControllerError("controller_command")
    try:
        command_id = identifier(command["command_id"])
        identifier(command["transfer_id"])
        observed = datetime.fromisoformat(command["time"])
        if observed.utcoffset() != timedelta(0) or not isinstance(command["reason"], str) or not command["reason"].strip():
            raise ValueError("invalid command metadata")
    except (TypeError, ValueError):
        raise ControllerError("controller_command") from None
    epoch_number(command["expected_epoch"], initial=True)
    if not isinstance(command["state_digest"], str) or re.fullmatch(r"[a-f0-9]{64}", command["state_digest"]) is None:
        raise ControllerError("controller_command")
    parent = state["launcher"]
    current = read_authority(state) if "controller_authority" in parent else None
    receipts = parent.get("controller_transfer_commands", {})
    if not isinstance(receipts, dict):
        raise ControllerError("controller_receipt")
    digest = hashlib.sha256(encoded(command)).hexdigest()
    if command_id in receipts:
        receipt = receipts[command_id]
        if not isinstance(receipt, dict) or receipt.get("command_sha256") != digest or current is None:
            raise ControllerError("command_conflict")
        return controller_record(receipt.get("outcome"))
    record = _next(state, current, command, evidence)
    durable_directory(intent_directory)
    try:
        write_once(
            intent_directory / f"{command_id}.json",
            {"registry_id": state["registry_id"], "command": command, "before": current, "after": record, "evidence": dict(evidence or {})},
        )
    except ValueError:
        raise ControllerError("command_conflict") from None
    parent["controller_authority"] = record
    parent["controller_transfer_commands"] = {**receipts, command_id: {"command_sha256": digest, "action": command["action"], "outcome": copy.deepcopy(record)}}
    return copy.deepcopy(record)


def dispatch_binding(state: Mapping[str, Any], *, owner: str) -> Document:
    """Bind dispatch to the active controller without changing any request identity."""
    record = read_authority(state)
    if record["controller_owner"] != owner or record["controller_phase"] != "active":
        raise ControllerError("controller_admission_closed")
    return {"registry_id": state["registry_id"], **{field: record[field] for field in BINDING_FIELDS if field != "registry_id"}}


def check_dispatch(state: Mapping[str, Any], binding: Mapping[str, Any], *, owner: str) -> None:
    """Compare persisted intent against current authority before any dispatch effect."""
    record = read_authority(state)
    if binding.get("controller_epoch") != record["controller_epoch"]:
        raise ControllerError("stale_controller_epoch")
    if dict(binding) != dispatch_binding(state, owner=owner):
        raise ControllerError("controller_binding")
