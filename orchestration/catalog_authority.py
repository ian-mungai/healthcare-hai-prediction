"""Catalog authority rules for the launcher's single registry transaction.

The authenticated owner command supplies terminal confirmation and immutable qualification or approval references.
The launcher commits the registry transaction before dispatch. This module starts no process and changes no catalog.
"""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.lakehouse.catalog_journal import LocalJournal, durable_directory, encoded, identifier, write_once

Document = dict[str, Any]
LAYERS = ("silver", "gold", "features", "bronze")
TRANSITIONS = {
    "mac_writer": ("freezing",),
    "freezing": ("source_fenced", "rollback_pending"),
    "source_fenced": ("refreshing", "rollback_pending"),
    "refreshing": ("aws_writer", "rollback_pending"),
    "aws_writer": ("rollback_pending", "recovering"),
    "rollback_pending": ("mac_writer", "source_verify"),
    "source_verify": ("mac_writer", "rollback_pending"),
    "reverse_fenced": ("mac_writer", "recovering"),
    "recovering": ("aws_writer", "reverse_fenced"),
}
PHASES = {
    "mac_writer": ("P1",),
    "freezing": ("P1",),
    "source_fenced": ("P2",),
    "refreshing": ("P3",),
    "aws_writer": ("P4", "P5"),
    "rollback_pending": ("P1", "P2", "P3", "P4"),
    "source_verify": ("P2", "P3", "P4"),
    "reverse_fenced": ("P4", "P5"),
    "recovering": ("P4", "P5"),
}
BRONZE_NAMESPACES = ("bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check")


class AuthorityError(ValueError):
    """The authority record or owner command cannot authorize the requested operation."""


def epoch_number(value: object, *, initial: bool = False) -> int:
    if type(value) is not int or value < (0 if initial else 1):
        raise AuthorityError("catalog epoch must be an integer in its allowed range")
    return value


def authority_record(value: object) -> Document:
    """Validate stored authority fields before a reader uses any admission decision."""
    required = {"version", "state", "epoch", "phase", "production_layers", "cutover_id", "command_id", "reason", "time", "target", "frozen"}
    if not isinstance(value, dict) or set(value) != required:
        raise AuthorityError("catalog authority record is missing or malformed")
    state = value["state"]
    if not isinstance(state, str) or state not in TRANSITIONS or value["version"] != 1 or type(value["version"]) is not int:
        raise AuthorityError("catalog authority state or version is invalid")
    epoch_number(value["epoch"])
    if value["phase"] not in PHASES[state]:
        raise AuthorityError("catalog authority phase is invalid for its state")
    layers = value["production_layers"]
    if not isinstance(layers, dict) or set(layers) != set(LAYERS) or any(item not in ("off", "on") for item in layers.values()):
        raise AuthorityError("catalog authority requires every production layer")
    if state == "mac_writer" and any(item != "off" for item in layers.values()):
        raise AuthorityError("Mac authority cannot retain AWS production activation")
    for key in ("cutover_id", "command_id", "reason", "time"):
        if not isinstance(value[key], str) or not value[key].strip():
            raise AuthorityError("catalog authority requires identity, reason and UTC time")
    try:
        observed = datetime.fromisoformat(value["time"])
    except ValueError:
        raise AuthorityError("catalog authority time must be ISO 8601 UTC") from None
    if observed.utcoffset() != timedelta(0):
        raise AuthorityError("catalog authority time must be ISO 8601 UTC")
    target = "mac" if state == "mac_writer" else "aws" if state == "aws_writer" else None
    if value["target"] != target or type(value["frozen"]) is not bool or value["frozen"] != (target is None):
        raise AuthorityError("catalog authority admission fields disagree with its state")
    return copy.deepcopy(value)


def read_authority(state: Mapping[str, Any], *, registry_kind: str) -> Document | None:
    """Read the real registry's authority, or the explicitly isolated rehearsal record. Tests bypass real authority."""
    if registry_kind == "test":
        return None
    if registry_kind not in ("real", "rehearsal"):
        raise AuthorityError("unknown registry kind")
    launcher = state.get("launcher")
    if not isinstance(launcher, Mapping):
        raise AuthorityError("catalog authority requires a launcher registry")
    return authority_record(launcher.get("catalog_authority"))


def confirmation_text(command: Mapping[str, Any]) -> str:
    """Return the exact text the authenticated owner must type at a terminal."""
    return f"{command.get('command_id')} {command.get('expected_epoch')} {command.get('action')} {command.get('layer', command.get('state'))}"


def recovery_clear(recovery: Mapping[str, Any] | None, *, layer: str | None = None) -> None:
    if recovery is None or not all(isinstance(recovery.get(key), list) for key in ("unresolved", "pending_intents", "blocked_layers")):
        raise AuthorityError("catalog recovery evidence is required")
    names = set(writer_namespaces(layer, "real") + writer_namespaces(layer, "aws_qualify")) if layer is not None else set()
    if (
        (layer is None and (recovery["unresolved"] or recovery["pending_intents"]))
        or layer in recovery["blocked_layers"]
        or names.intersection(recovery["unresolved"])
    ):
        raise AuthorityError("unresolved catalog namespaces prevent writer admission")


def next_record(current: Document | None, command: Document, recovery: Mapping[str, Any] | None) -> Document:
    action, target = command.get("action"), command.get("state")
    expected = epoch_number(command.get("expected_epoch"), initial=True)
    if expected != (current["epoch"] if current else 0):
        raise AuthorityError("stale catalog epoch")
    if current is None:
        if action != "initialize" or target != "mac_writer":
            raise AuthorityError("the first owner command must initialize Mac authority")
        record: Document = {"phase": "P1", "production_layers": dict.fromkeys(LAYERS, "off")}
    else:
        record = copy.deepcopy(current)
        starts_freeze = action == "transition" and target == "freezing"
        if not starts_freeze and command.get("cutover_id") != current["cutover_id"]:
            raise AuthorityError("catalog authority command names another cutover")
        if action == "set_layer":
            layer, enabled = command.get("layer"), command.get("enabled")
            if current["state"] != "aws_writer" or layer not in LAYERS or type(enabled) is not bool:
                raise AuthorityError("layer activation requires AWS authority and a known layer")
            if enabled:
                if not all(isinstance(command.get(key), str) and command[key].strip() for key in ("qualification", "approval")):
                    raise AuthorityError("layer qualification and approval are required")
                recovery_clear(recovery, layer=layer)
            record["production_layers"][layer] = "on" if enabled else "off"
            target = current["state"]
        elif action == "transition":
            if target not in TRANSITIONS[current["state"]]:
                raise AuthorityError("catalog authority transition is not allowed")
            if target == "rollback_pending" and current["phase"] == "P5":
                raise AuthorityError("transition cannot roll back an admitted writer")
            if current["state"] == "rollback_pending" and target == "mac_writer" and current["phase"] != "P1":
                raise AuthorityError("transition requires source verification")
            if current["state"] == "recovering" and target == "aws_writer" or current["state"] == "reverse_fenced" and target == "mac_writer":
                recovery_clear(recovery)
            if target == "freezing":
                if command.get("cutover_id") == current["cutover_id"]:
                    raise AuthorityError("a new freeze requires a new cutover identity")
                record.update(phase="P1", production_layers=dict.fromkeys(LAYERS, "off"))
            if target in ("mac_writer", "source_fenced", "refreshing"):
                record["phase"] = {"mac_writer": "P1", "source_fenced": "P2", "refreshing": "P3"}[target]
            if target == "mac_writer":
                record["production_layers"] = dict.fromkeys(LAYERS, "off")
            if target == "aws_writer" and current["state"] == "refreshing":
                record.update(phase="P4", production_layers=dict.fromkeys(LAYERS, "off"))
        else:
            raise AuthorityError("unknown catalog authority owner action")
    record.update({key: command.get(key) for key in ("cutover_id", "command_id", "reason", "time")})
    record.update(version=1, state=target, epoch=expected + 1, target="mac" if target == "mac_writer" else "aws" if target == "aws_writer" else None)
    record["frozen"] = record["target"] is None
    result = authority_record(record)
    if current is not None and datetime.fromisoformat(result["time"]) < datetime.fromisoformat(current["time"]):
        raise AuthorityError("owner command time precedes the authority record")
    return result


def owner_command(
    state: Document, command: Document, *, intent_directory: Path, owner: bool, tty: bool, confirmation: str, recovery: Mapping[str, Any] | None = None
) -> Document:
    """Persist the owner's intent before changing authority inside the caller's exclusive registry transaction.

    The caller supplies authenticated owner and terminal evidence, then commits the registry before acting.
    A repeated command identity with identical content leaves both the registry and the intent unchanged.
    """
    if owner is not True:
        raise AuthorityError("catalog changes require an authenticated owner")
    if tty is not True:
        raise AuthorityError("catalog changes require terminal confirmation")
    if confirmation != confirmation_text(command):
        raise AuthorityError("catalog changes require the exact typed confirmation")
    command_id = identifier(command.get("command_id", ""))
    launcher = state.get("launcher")
    if not isinstance(launcher, dict):
        raise AuthorityError("catalog authority requires a launcher registry")
    current = authority_record(launcher["catalog_authority"]) if "catalog_authority" in launcher else None
    digest = hashlib.sha256(encoded(command)).hexdigest()
    receipts = launcher.get("catalog_authority_commands", {})
    if not isinstance(receipts, dict):
        raise AuthorityError("catalog authority command receipts are malformed")
    if command_id in receipts:
        if receipts[command_id] != digest or current is None:
            raise AuthorityError("catalog authority command identity conflict")
        return current
    record = next_record(current, command, recovery)
    durable_directory(intent_directory)
    write_once(intent_directory / f"{command_id}.json", {"command": command, "before_epoch": current["epoch"] if current else 0, "after": record})
    launcher["catalog_authority"] = record
    launcher["catalog_authority_commands"] = {**receipts, command_id: digest}
    return copy.deepcopy(record)


def writer_namespaces(layer: str, mode: str) -> list[str]:
    """Return the closed namespace binding for one writer layer and run mode."""
    if layer not in LAYERS:
        raise AuthorityError("unknown production layer")
    if mode not in ("real", "aws_qualify"):
        raise AuthorityError("AWS writer mode must be real or aws_qualify")
    names = BRONZE_NAMESPACES if layer == "bronze" else (layer,)
    return sorted(f"{name}_e2e" if mode == "aws_qualify" else name for name in names)


def check_writer(
    record: Mapping[str, Any], *, epoch: int, layer: str, mode: str, target: str, run_id: str, namespaces: list[str], approval: Mapping[str, Any] | None
) -> Document:
    """Validate admission or bootstrap against the current authority and return the epoch and layer binding."""
    authority = authority_record(dict(record))
    if authority["state"] != "aws_writer":
        raise AuthorityError("AWS writer admission is closed")
    if target != "ecs":
        raise AuthorityError("AWS is the only writer target")
    if epoch_number(epoch) != authority["epoch"]:
        raise AuthorityError("stale catalog epoch")
    expected = writer_namespaces(layer, mode)
    if namespaces != expected:
        raise AuthorityError("writer namespace scope differs from its closed binding")
    if mode == "real":
        if authority["production_layers"][layer] != "on":
            raise AuthorityError("production layer is off")
        if not run_id or approval is None or any(approval.get(key) != value for key, value in (("run_id", run_id), ("layer", layer), ("epoch", epoch))):
            raise AuthorityError("per-run approval does not match run, layer and epoch")
    return {"epoch": epoch, "layer": layer}


def admit_writer(
    state: Document,
    journal: LocalJournal,
    *,
    intent_id: str,
    epoch: int,
    layer: str,
    mode: str,
    target: str,
    run_id: str,
    database_id: str,
    namespaces: list[str],
    registry_kind: str,
    approval: Mapping[str, Any] | None,
) -> Document:
    """Fsync a writer intent and mark the admitted phase under the launcher's existing registry lock."""
    record = read_authority(state, registry_kind=registry_kind)
    if record is None:
        raise AuthorityError("test registries cannot admit AWS writers")
    binding = check_writer(record, epoch=epoch, layer=layer, mode=mode, target=target, run_id=run_id, namespaces=namespaces, approval=approval)
    if journal.epoch != str(epoch):
        raise AuthorityError("writer journal does not match the catalog epoch")
    journal.intent(intent_id, layer, namespaces, database_id)
    state["launcher"]["catalog_authority"]["phase"] = "P5"
    return binding


def check_mac_start(record: Mapping[str, Any], *, owner: bool = False, fenced: bool = False, get_only: bool = False, job_network: bool = False) -> None:
    """Permit a normal Mac start only under Mac authority, or an isolated owner verification or reverse start."""
    state = authority_record(dict(record))["state"]
    if state == "mac_writer":
        return
    if state not in ("source_verify", "reverse_fenced") or not owner or not fenced or job_network or state == "source_verify" and not get_only:
        raise AuthorityError("Mac catalog start requires an isolated fenced owner state")


def reader_route(record: Mapping[str, Any], *, target: str) -> str:
    """Return the permitted direct or relay reader route, refusing admission while that target is paused."""
    state = authority_record(dict(record))["state"]
    if target == "mac" and state in ("aws_writer", "recovering"):
        return "relay"
    if target == "mac" and state == "mac_writer" or target == "aws" and state in ("mac_writer", "freezing", "source_fenced", "aws_writer"):
        return "direct"
    raise AuthorityError("catalog reader admission is closed")
