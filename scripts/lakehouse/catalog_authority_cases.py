"""Exercise catalog authority and owner journal commands against isolated synthetic registry files."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import stat
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

REPO = Path(__file__).resolve().parents[2]
STAMP = "2026-10-25T08:00:00Z"
STATES = ("mac_writer", "freezing", "source_fenced", "refreshing", "aws_writer", "rollback_pending", "source_verify", "reverse_fenced", "recovering")
FORWARD = ("freezing", "source_fenced", "refreshing", "aws_writer")
ROUTES = {
    "mac_writer": (),
    "freezing": FORWARD[:1],
    "source_fenced": FORWARD[:2],
    "refreshing": FORWARD[:3],
    "aws_writer": FORWARD,
    "rollback_pending": (*FORWARD[:2], "rollback_pending"),
    "source_verify": (*FORWARD[:2], "rollback_pending", "source_verify"),
    "recovering": (*FORWARD, "recovering"),
    "reverse_fenced": (*FORWARD, "recovering", "reverse_fenced"),
}
EDGES = {
    "mac_writer": {"freezing"},
    "freezing": {"source_fenced", "rollback_pending"},
    "source_fenced": {"refreshing", "rollback_pending"},
    "refreshing": {"aws_writer", "rollback_pending"},
    "aws_writer": {"rollback_pending", "recovering"},
    "rollback_pending": {"source_verify"},
    "source_verify": {"mac_writer", "rollback_pending"},
    "recovering": {"aws_writer", "reverse_fenced"},
    "reverse_fenced": {"mac_writer", "recovering"},
}
NAMESPACES = {
    "silver": ("silver",),
    "gold": ("gold",),
    "features": ("features",),
    "bronze": ("bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check"),
}


def head(number: int) -> dict[str, Any]:
    return {
        "uuid": "synthetic_uuid",
        "metadata_location": f"file:///synthetic/{number}.metadata.json",
        "version_id": f"synthetic_version_{number}",
        "snapshot_id": number,
        "refs": {"main": {"snapshot_id": number, "type": "branch"}},
    }


def namespace(number: int) -> dict[str, Any]:
    return {"state": "present", "properties": {"purpose": "synthetic"}, "tables": {"kept": head(number)}}


class Scenario:
    """Retain real command requests, persisted state and independently specified assertions."""

    def __init__(self, root: Path, subject: Path, deadline: float) -> None:
        self.root, self.subject, self.deadline = root, subject, deadline
        self.commands: list[dict[str, Any]] = []
        self.assertions: list[dict[str, Any]] = []
        self.transition_source: str | None = None
        root.mkdir(parents=True)

    def check(self, label: str, observed: Any, expected: Any) -> None:
        self.assertions.append({"label": label, "observed": observed, "expected": expected, "passed": observed == expected})

    def invoke(self, action: str, *, error: str | None = None, **values: Any) -> dict[str, Any]:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("authority case exceeded its absolute 90-second deadline")
        request_path = self.root / f"{len(self.commands):03d}_{action}_request.json"
        response_path = request_path.with_name(request_path.name.replace("_request", "_response"))
        request_path.write_text(json.dumps({"action": action, **values}, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        argv = [
            "-m",
            "scripts.lakehouse.catalog_authority_cases",
            "--driver-root",
            str(self.root / "driver"),
            "--request",
            str(request_path),
            "--response",
            str(response_path),
        ]
        result = run_command(sys.executable, argv, cwd=self.subject, timeout=min(20, remaining))
        self.commands.append({"argv": [sys.executable, *argv], "cwd": str(self.subject), "exit_code": result.returncode, "stderr": result.stderr})
        self.check(f"{action}: exit", result.returncode, 1 if error else 0)
        if error:
            self.check(f"{action}: intended rejection {error}", error in result.stderr, True)
            return {}
        if result.returncode or not response_path.exists():
            raise RuntimeError(f"{action} did not produce a response: {result.stderr}")
        return json.loads(response_path.read_bytes())

    def reach(self, target: str, *, admitted: bool = False) -> dict[str, Any]:
        result = self.invoke("seed")
        for name in ROUTES[target]:
            if name == "recovering" and admitted:
                self.invoke("admit", intent_id="qualified_try", layer="silver", mode="aws_qualify", run_id="synthetic_run")
            if name == "reverse_fenced" and admitted:
                self.invoke("complete", intent_id="qualified_try", namespaces={"silver_e2e": namespace(2)})
            result = self.owner(name)
        return result

    def owner(self, target: str, **values: Any) -> dict[str, Any]:
        return self.invoke("owner", target=target, **values)

    def variant(self, name: str) -> Scenario:
        child = Scenario(self.root / name, self.subject, self.deadline)
        child.commands, child.assertions = self.commands, self.assertions
        return child


def transition_graph(case: Scenario) -> None:
    for source in (case.transition_source,) if case.transition_source is not None else STATES:
        child = case.variant(source)
        child.reach(source)
        result = child.invoke("transition_matrix")
        for target in STATES:
            edge = f"{source} to {target}"
            allowed = target in EDGES[source]
            child.check(f"transition {edge}", result[target]["accepted"], allowed)
            if not allowed:
                child.check(f"transition rejection {edge}", "transition" in result[target].get("error", ""), True)


def epoch_and_idempotence(case: Scenario) -> None:
    first = case.reach("freezing")
    repeated = case.owner("freezing", repeat=True)
    case.check("exact command retry changes no registry bytes", repeated["registry_sha256"], first["registry_sha256"])
    case.check("initial freeze grows epoch", first["authority"]["epoch"], 2)
    second = case.owner("source_fenced")
    case.check("next transition grows epoch", second["authority"]["epoch"], 3)
    old = case.owner("freezing", repeat_command_id="owner_1")
    case.check("old exact command retry cannot rewind state", old["registry_sha256"], second["registry_sha256"])
    case.owner("freezing", repeat=True, changed_reason=True, error="command identity conflict")
    case.owner("refreshing", expected_epoch=2, error="stale catalog epoch")
    case.owner("refreshing", expected_epoch=True, error="catalog epoch must be an integer")
    case.owner("refreshing", cutover_id="different_cutover", error="another cutover")
    case.check("rejected epochs change no registry bytes", case.invoke("inspect")["registry_sha256"], second["registry_sha256"])
    freeze = case.variant("freeze_identity")
    initial = freeze.reach("mac_writer")
    freeze.owner("freezing", cutover_id=initial["authority"]["cutover_id"], error="a new freeze requires a new cutover identity")
    freeze.check("reused freeze identity changes no registry bytes", freeze.invoke("inspect")["registry_sha256"], initial["registry_sha256"])
    distinct = freeze.owner("freezing", cutover_id="distinct_cutover")
    freeze.check("distinct freeze identity is recorded", distinct["authority"]["cutover_id"], "distinct_cutover")


def owner_command_time(case: Scenario) -> None:
    for action, source, target in (
        ("transition", "freezing", "source_fenced"),
        ("set_layer", "aws_writer", "aws_writer"),
        ("new_freeze", "mac_writer", "freezing"),
    ):
        child = case.variant(action)
        before = child.reach(source)
        values: dict[str, Any] = {"layer": "silver", "enabled": False} if action == "set_layer" else {"target": target}
        entry = "set_layer" if action == "set_layer" else "owner"
        child.invoke(entry, **values, time="2020-01-01T00:00:00Z", error="owner command time precedes")
        child.check(f"{action}: earlier time changes no registry bytes", child.invoke("inspect")["registry_sha256"], before["registry_sha256"])
        later = child.invoke(entry, **values, time="2026-10-25T08:01:00+00:00")
        child.check(f"{action}: later UTC time is recorded", later["authority"]["time"], "2026-10-25T08:01:00+00:00")
        repeated = child.invoke(entry, **values, repeat=True)
        child.check(f"{action}: exact retry keeps registry bytes", repeated["registry_sha256"], later["registry_sha256"])
    child = case.variant("older_applied_retry")
    child.reach("freezing")
    latest = child.owner("source_fenced", time="2026-10-25T08:02:00Z")
    child.check(
        "older applied command remains an exact no-op", child.owner("freezing", repeat_command_id="owner_1")["registry_sha256"], latest["registry_sha256"]
    )


def phase_and_rollback(case: Scenario) -> None:
    for phase, target in enumerate(FORWARD, 1):
        child = case.variant(f"rollback_{phase}")
        child.reach(target)
        pending = child.owner("rollback_pending")
        child.check(f"rollback preserves reached phase {phase}", pending["authority"]["phase"], f"P{phase}")
        if phase > 1:
            child.owner("mac_writer", error="transition requires source verification")
            child.owner("source_verify")
        restored = child.owner("mac_writer")
        child.check(f"rollback {phase} restores Mac only after verification", restored["authority"]["state"], "mac_writer")
    child = case.variant("admitted")
    before = child.reach("aws_writer")
    admitted = child.invoke("admit", intent_id="first_test_try", layer="silver", mode="aws_qualify", run_id="synthetic_run")
    child.check("first admitted test try persists final phase", admitted["authority"]["phase"], "P5")
    child.check("phase change preserves admitted epoch", admitted["authority"]["epoch"], before["authority"]["epoch"])
    child.owner("rollback_pending", error="transition cannot roll back an admitted writer")
    child.owner("recovering")
    child.owner("aws_writer", error="unresolved catalog namespaces")


def layer_and_admission(case: Scenario) -> None:
    initial = case.reach("aws_writer")
    case.check("activation starts all layers off", initial["authority"]["production_layers"], dict.fromkeys(("silver", "gold", "features", "bronze"), "off"))
    for layer in ("silver", "gold", "features", "bronze"):
        child = case.variant(layer)
        before = child.reach("aws_writer")
        child.invoke("set_layer", layer=layer, enabled=False, cutover_id="different_cutover", error="another cutover")
        child.check(f"{layer} command cannot replace cutover identity", child.invoke("inspect")["registry_sha256"], before["registry_sha256"])
        child.invoke("admit", intent_id="off", layer=layer, mode="real", run_id="synthetic_run", error="production layer is off")
        child.invoke("set_layer", layer=layer, enabled=True, qualification=None, error="layer qualification and approval are required")
        active = child.invoke("set_layer", layer=layer, enabled=True)
        child.check(f"{layer} activation grows epoch", active["authority"]["epoch"], before["authority"]["epoch"] + 1)
        child.invoke(
            "admit",
            intent_id="wrong_journal",
            layer=layer,
            mode="real",
            run_id="synthetic_run",
            journal_epoch=active["authority"]["epoch"] + 1,
            error="writer journal does not match the catalog epoch",
        )
        child.check(f"{layer} wrong journal changes no registry bytes", child.invoke("inspect")["registry_sha256"], active["registry_sha256"])
        for key, wrong, expected in (
            ("target", "docker", "AWS is the only writer target"),
            ("epoch", 1, "stale catalog epoch"),
            ("approval", None, "per-run approval does not match"),
            ("approval", {"run_id": "other", "layer": layer, "epoch": active["authority"]["epoch"]}, "per-run approval does not match"),
            ("approval", {"run_id": "synthetic_run", "layer": "other", "epoch": active["authority"]["epoch"]}, "per-run approval does not match"),
        ):
            child.invoke("admit", intent_id=f"bad_{len(child.commands)}", layer=layer, mode="real", run_id="synthetic_run", error=expected, **{key: wrong})
        admitted = child.invoke("admit", intent_id="approved", layer=layer, mode="real", run_id="synthetic_run")
        child.check(f"{layer} admission binds layer and epoch", admitted["binding"], {"epoch": active["authority"]["epoch"], "layer": layer})
        child.invoke(
            "admit",
            intent_id="wrong_scope",
            layer=layer,
            mode="aws_qualify",
            run_id="synthetic_run",
            namespaces=[layer],
            error="writer namespace scope differs",
        )
        child.invoke("bootstrap", layer=layer, epoch=active["authority"]["epoch"], run_id="synthetic_run")
        child.invoke("set_layer", layer=layer, enabled=False)
        child.invoke("bootstrap", layer=layer, epoch=active["authority"]["epoch"], run_id="synthetic_run", error="stale catalog epoch")
    pending = case.variant("owner_scope")
    pending.reach("aws_writer")
    pending.invoke("owner_intent", intent_id="owner_scope", layer="owner", namespaces=["silver"], objects=["namespace:silver"])
    pending.invoke("set_layer", layer="silver", enabled=True, error="unresolved catalog namespaces")
    closed = case.variant("layer_outside_aws")
    original = closed.reach("freezing")
    closed.invoke("set_layer", layer="silver", enabled=False, error="layer activation requires AWS authority")
    closed.check("layer command outside AWS changes no registry bytes", closed.invoke("inspect")["registry_sha256"], original["registry_sha256"])
    case.invoke("admit", intent_id="unknown_layer", layer="other", mode="aws_qualify", run_id="synthetic_run", error="unknown production layer")


def owner_confirmation(case: Scenario) -> None:
    initial = case.reach("mac_writer")
    for values, error in (
        ({"owner": False}, "authenticated owner"),
        ({"tty": False}, "terminal confirmation"),
        ({"confirmation": "wrong"}, "typed confirmation"),
    ):
        case.owner("freezing", error=error, **values)
    after = case.invoke("inspect")
    case.check("owner rejection preserves registry", after["registry_sha256"], initial["registry_sha256"])
    case.check("owner rejection writes no command intent", after["authority_intent_count"], 1)


def record_boundary(case: Scenario) -> None:
    initial = case.reach("mac_writer")["authority"]
    for value in (None, {}, {"epoch": True}, {"state": "aws_writer", "epoch": 1, "target": "mac", "frozen": False}):
        result = case.invoke("read_record", record=value)
        case.check("malformed real authority fails closed", result["refused"], True)
    result = case.invoke("read_record", registry_kind="test", record={"state": "aws_writer"})
    case.check("test registry never reads real authority", result["authority"], None)
    invalid_phase = {**initial, "phase": "P5"}
    result = case.invoke("read_record", record=invalid_phase)
    case.check("otherwise valid record rejects incompatible phase", result["refused"], True)
    case.check("incompatible phase has its intended cause", "phase is invalid for its state" in result.get("error", ""), True)
    for field, changes, cause in (
        ("version", {"version": 2}, "state or version is invalid"),
        ("state", {"state": "unknown"}, "state or version is invalid"),
        ("epoch", {"epoch": 0}, "catalog epoch must be an integer"),
        ("layer_membership", {"production_layers": {"silver": "off"}}, "requires every production layer"),
        ("layer_value", {"production_layers": {**initial["production_layers"], "silver": "enabled"}}, "requires every production layer"),
        ("mac_activation", {"production_layers": {**initial["production_layers"], "silver": "on"}}, "Mac authority cannot retain"),
        ("reason", {"reason": ""}, "requires identity, reason and UTC time"),
        ("command_id", {"command_id": ""}, "requires identity, reason and UTC time"),
        ("cutover_id", {"cutover_id": ""}, "requires identity, reason and UTC time"),
        ("time_format", {"time": "invalid"}, "time must be ISO 8601 UTC"),
        ("time_zone", {"time": "2026-10-25T08:00:00+01:00"}, "time must be ISO 8601 UTC"),
        ("target", {"target": "aws"}, "admission fields disagree"),
        ("frozen", {"frozen": True}, "admission fields disagree"),
    ):
        result = case.invoke("read_record", record={**initial, **changes})
        case.check(f"record {field} has its intended rejection", result["refused"] and cause in result.get("error", ""), True)


def access_matrix(case: Scenario) -> None:
    for state in STATES:
        child = case.variant(state)
        child.reach(state, admitted=state == "reverse_fenced")
        result = child.invoke("access")
        child.check(f"normal Mac start in {state}", result["normal_start"], state == "mac_writer")
        child.check(f"owner isolated Mac start in {state}", result["owner_start"], state in {"mac_writer", "source_verify", "reverse_fenced"})
        child.check(f"owner start with job network in {state}", result["network_start"], state == "mac_writer")
        child.check(f"AWS reader in {state}", result["aws_reader"], state in {"mac_writer", "freezing", "source_fenced", "aws_writer"})
        child.check(f"Mac reader in {state}", result["mac_reader"], state in {"mac_writer", "aws_writer", "recovering"})


def durability_order(case: Scenario) -> None:
    for failure in ("none", "file", "directory"):
        child = case.variant(failure)
        child.reach("mac_writer")
        result = child.invoke("durability", failure=failure)
        child.check(f"{failure}: state unchanged during intent sync", result["unchanged_during_intent"], True)
        child.check(f"{failure}: both intent syncs precede authority change", result["synced_before_change"], True)
        child.check(f"{failure}: interruption leaves registry unchanged", result["failure_preserves_state"], True)
        child.check(f"{failure}: command resumes once", result["authority"]["epoch"], 2)


def owner_intent_lifecycle(case: Scenario) -> None:
    case.invoke("seed")
    first = case.invoke("owner_intent", intent_id="grant", namespaces=["silver", "silver.empty"], objects=["namespace:silver", "namespace:silver.empty"])
    again = case.invoke("owner_intent", intent_id="grant", namespaces=["silver.empty", "silver"], objects=["namespace:silver.empty", "namespace:silver"])
    case.check("identical owner intent is immutable", again["intent_sha256"], first["intent_sha256"])
    case.check("owner command scope is recorded", first["intent"]["objects"], ["namespace:silver", "namespace:silver.empty"])
    case.invoke("owner_intent", intent_id="grant", namespaces=["gold"], objects=["namespace:gold"], error="immutable record conflict")
    case.invoke("owner_intent", intent_id="empty", namespaces=[], objects=[], error="owner command must name its objects")
    case.invoke("owner_intent", intent_id="catalog_setup", namespaces=[], objects=["catalog:synthetic"])
    case.check("global owner command remains pending", case.invoke("recover")["pending_intents"], ["catalog_setup", "grant"])
    case.invoke("complete", intent_id="catalog_setup", namespaces={})
    case.invoke("complete", intent_id="grant", namespaces={"silver": namespace(2), "silver.empty": {"state": "present", "properties": {}, "tables": {}}})
    recovered = case.invoke("recover")
    case.check("confirmed owner command clears its intent", recovered["pending_intents"], [])
    case.check(
        "confirmed owner command creates empty namespace", recovered["namespace_states"]["silver.empty"], {"state": "present", "properties": {}, "tables": {}}
    )

    global_case = case.variant("global_pending_layer")
    global_case.reach("aws_writer")
    global_case.invoke("owner_intent", intent_id="global_gold", layer="gold", namespaces=[], objects=["catalog:synthetic"])
    pending = global_case.invoke("recover")
    global_case.check("global pending intent has no unresolved namespace", pending["unresolved"], [])
    global_case.check("global pending intent blocks its declared layer", pending["blocked_layers"], ["gold"])
    global_case.invoke("set_layer", layer="gold", enabled=True, error="unresolved catalog namespaces")
    global_case.owner("recovering")
    global_case.owner("aws_writer", error="unresolved catalog namespaces")
    global_case.invoke("complete", intent_id="global_gold", namespaces={})
    reopened = global_case.owner("aws_writer")
    global_case.check("confirmed global command permits recovery exit", reopened["authority"]["state"], "aws_writer")
    global_case.invoke("set_layer", layer="gold", enabled=True)


def reverse_pending_intents(case: Scenario) -> None:
    for name, writer_available, owner_available in (("confirmed", True, True), ("unavailable", False, False), ("one_unconfirmed", True, False)):
        child = case.variant(name)
        child.reach("aws_writer")
        child.invoke("admit", intent_id="reverse_writer", layer="silver", mode="aws_qualify", run_id="synthetic_run")
        child.invoke("owner_intent", intent_id="reverse_owner", layer="gold", namespaces=["gold"], objects=["namespace:gold"])
        child.owner("recovering")
        fenced = child.owner("reverse_fenced")
        child.check(f"{name}: reverse enters fenced state with pending intents", fenced["authority"]["state"], "reverse_fenced")
        child.owner("mac_writer", error="unresolved catalog namespaces")
        child.invoke("complete", intent_id="reverse_writer", namespaces={"silver_e2e": namespace(2)}, available=writer_available)
        child.invoke("complete", intent_id="reverse_owner", namespaces={"gold": namespace(2)}, available=owner_available)
        pending = child.invoke("recover")
        expected_pending = sorted(([] if writer_available else ["reverse_writer"]) + ([] if owner_available else ["reverse_owner"]))
        child.check(f"{name}: reverse live read names each unconfirmed intent", pending["pending_intents"], expected_pending)
        if expected_pending:
            child.owner("mac_writer", error="unresolved catalog namespaces")
            child.check(f"{name}: unconfirmed reverse keeps fenced authority", child.invoke("inspect")["registry_sha256"], fenced["registry_sha256"])
            child.invoke(
                "admit", intent_id="forbidden_reverse_try", layer="silver", mode="aws_qualify", run_id="synthetic_run", error="AWS writer admission is closed"
            )
        else:
            restored = child.owner("mac_writer")
            child.check("confirmed reverse returns Mac authority", restored["authority"]["state"], "mac_writer")
            child.check("confirmed reverse returns the no-fence phase", restored["authority"]["phase"], "P1")


def owner_pending_recovery(case: Scenario) -> None:
    case.invoke("seed")
    case.invoke("owner_intent", intent_id="older", namespaces=["silver"], objects=["namespace:silver"])
    case.invoke("complete", intent_id="older", namespaces={"silver": namespace(2)})
    case.invoke("owner_intent", intent_id="pending", namespaces=["silver", "created"], objects=["namespace:silver", "namespace:created"])
    first = case.invoke("recover")
    case.check("owner pending blocks all namespaces despite older completion", first["unresolved"], ["created", "silver"])
    case.check("pending namespace has no inventory fallback", "silver" in first["namespace_states"], False)
    case.invoke("complete", intent_id="pending", namespaces={"silver": namespace(3)}, error="completion must observe every namespace")
    case.invoke("complete", intent_id="older", namespaces={"silver": namespace(2)}, step="later_other")
    case.check("other intent completion cannot clear pending", case.invoke("recover")["pending_intents"], ["pending"])
    case.invoke("complete", intent_id="pending", namespaces={"silver": namespace(3), "created": namespace(1)}, kind="journal_rebuild")
    case.check("rebuilt owner observation remains pending", case.invoke("recover")["unresolved"], ["created", "silver"])
    case.invoke("complete", intent_id="pending", namespaces={"silver": namespace(3), "created": namespace(1)}, step="live")
    final = case.invoke("recover")
    case.check("live owner confirmation resolves complete scope", final["unresolved"], [])
    case.check("live owner head wins", final["namespace_states"]["silver"], namespace(3))
    case.check("untouched inventory namespace survives", final["namespace_states"]["gold"], namespace(1))


def confirmation_sources(case: Scenario) -> None:
    variants: tuple[dict[str, Any], ...] = (
        {"terminal": False},
        {"quiesced": False},
        {"available": False},
        {"kind": "restored_dump"},
        {"kind": "journal_rebuild"},
        {"database_id": "replacement"},
    )
    for index, values in enumerate(variants):
        child = case.variant(f"source_{index}")
        child.invoke("seed")
        child.invoke("owner_intent", intent_id="pending", namespaces=["silver"], objects=["namespace:silver"])
        child.invoke("complete", intent_id="pending", namespaces={"silver": namespace(2)}, **values)
        child.check(f"source {index} cannot confirm pending owner command", child.invoke("recover")["pending_intents"], ["pending"])
        child.invoke("complete", intent_id="pending", namespaces={"silver": namespace(2)})
        child.check(f"source {index} later original live source confirms", child.invoke("recover")["pending_intents"], [])


def recovery_source_order(case: Scenario) -> None:
    case.invoke("seed")
    for epoch, number in ((9, 2), (10, 3)):
        case.invoke("owner_intent", epoch=str(epoch), intent_id=f"epoch_{epoch}", namespaces=["silver"], objects=["namespace:silver"])
        case.invoke("complete", epoch=str(epoch), intent_id=f"epoch_{epoch}", namespaces={"silver": namespace(number)})
    result = case.invoke("recover")
    case.check("numeric epoch order replaces the inventory", result["namespace_states"]["silver"], namespace(3))
    for fault in ("reverse", "gap", "duplicate", "wrong_epoch"):
        case.invoke("replay_fault", fault=fault, error="journal")
    case.invoke("owner_intent", epoch="10", intent_id="delete", namespaces=["silver"], objects=["namespace:silver"])
    replacement = {"state": "present", "properties": {"changed": "synthetic"}, "tables": {}}
    case.invoke("complete", epoch="10", intent_id="delete", namespaces={"silver": replacement})
    case.check("newest complete namespace replaces deleted membership", case.invoke("recover")["namespace_states"]["silver"], replacement)
    case.invoke("complete", epoch="10", intent_id="delete", namespaces={"silver": namespace(999)}, step="unavailable", available=False)
    case.check("latest unconfirmed observation blocks an already confirmed intent", case.invoke("recover")["unresolved"], ["silver"])
    case.invoke("complete", epoch="10", intent_id="delete", namespaces={"silver": replacement}, step="live_again")
    case.invoke("owner_intent", epoch="10", intent_id="last_unconfirmed", namespaces=["silver"], objects=["namespace:silver"])
    case.invoke("complete", epoch="10", intent_id="last_unconfirmed", namespaces={"silver": namespace(999)}, available=False)
    case.check("newest unconfirmed record prevents older fallback", case.invoke("recover")["unresolved"], ["silver"])


def freeze_admission_order(case: Scenario) -> None:
    first = case.variant("freeze_first")
    first.reach("aws_writer")
    first.owner("rollback_pending")
    first.invoke("admit", intent_id="late", layer="silver", mode="aws_qualify", run_id="synthetic_run", error="AWS writer admission is closed")
    second = case.variant("admission_first")
    second.reach("aws_writer")
    second.invoke("admit", intent_id="first", layer="silver", mode="aws_qualify", run_id="synthetic_run")
    second.owner("rollback_pending", error="transition cannot roll back an admitted writer")


CASES = {
    "transition_graph": transition_graph,
    "epoch_and_idempotence": epoch_and_idempotence,
    "owner_command_time": owner_command_time,
    "phase_and_rollback": phase_and_rollback,
    "layer_and_admission": layer_and_admission,
    "owner_confirmation": owner_confirmation,
    "record_boundary": record_boundary,
    "access_matrix": access_matrix,
    "durability_order": durability_order,
    "owner_intent_lifecycle": owner_intent_lifecycle,
    "reverse_pending_intents": reverse_pending_intents,
    "owner_pending_recovery": owner_pending_recovery,
    "confirmation_sources": confirmation_sources,
    "recovery_source_order": recovery_source_order,
    "freeze_admission_order": freeze_admission_order,
}


def command(state: dict[str, Any], target: str, serial: int) -> dict[str, Any]:
    record = state["launcher"].get("catalog_authority")
    return {
        "command_id": f"owner_{serial}",
        "action": "transition" if record else "initialize",
        "expected_epoch": record["epoch"] if record else 0,
        "state": target,
        "cutover_id": "cutover_synthetic" if target == "freezing" else record["cutover_id"] if record else "initial_synthetic",
        "reason": "synthetic owner command",
        "time": record["time"] if record else STAMP,
    }


def snapshot(root: Path, state: dict[str, Any]) -> dict[str, Any]:
    return {
        "authority": state["launcher"].get("catalog_authority"),
        "registry_sha256": hashlib.sha256((root / "registry.json").read_bytes()).hexdigest(),
        "authority_intent_count": len(list((root / "authority_intents").glob("*.json"))),
    }


def driver(root: Path, request: dict[str, Any]) -> dict[str, Any]:
    from orchestration import catalog_authority as authority
    from scripts.lakehouse import catalog_journal as journal

    action = request["action"]
    if action == "seed":
        root.mkdir(parents=True, exist_ok=False)
        state: dict[str, Any] = {"registry_kind": "rehearsal", "launcher": {}, "commands": []}
        initial = command(state, "mac_writer", 0)
        authority.owner_command(
            state, initial, intent_directory=root / "authority_intents", owner=True, tty=True, confirmation=authority.confirmation_text(initial)
        )
        state["commands"].append(initial)
        journal.replace_document(root / "registry.json", state)
        journal.write_once(root / "inventory.json", {"silver": namespace(1), "gold": namespace(1)})
        return snapshot(root, state)
    state = journal.read_document(root / "registry.json")
    if state.get("registry_kind") != "rehearsal":
        raise ValueError("synthetic driver requires its rehearsal registry")
    record = authority.read_authority(state, registry_kind="rehearsal")
    if record is None:
        raise ValueError("synthetic driver requires an authority record")
    journals = [journal.LocalJournal(root, path.name) for path in sorted((root / "dispatch").glob("*")) if path.is_dir()]
    recovery = journal.recover_from_journals(journal.read_document(root / "inventory.json"), journals)
    current_journal = journal.LocalJournal(root, str(request.get("epoch", 5)))
    if action == "inspect":
        return snapshot(root, state)
    if action in {"owner", "set_layer"}:
        owner_request = command(state, request["target"] if "target" in request else record["state"], len(state["commands"]))
        if action == "set_layer":
            owner_request.update(action="set_layer", layer=request["layer"], enabled=request["enabled"])
            owner_request["qualification"] = request.get("qualification", "synthetic_qualification")
            owner_request["approval"] = request.get("approval", "synthetic_activation_approval")
        if request.get("repeat") or "repeat_command_id" in request:
            owner_request = copy.deepcopy(
                next(
                    value
                    for value in reversed(state["commands"])
                    if value["command_id"] == request.get("repeat_command_id", state["commands"][-1]["command_id"])
                )
            )
        if request.get("changed_reason"):
            owner_request["reason"] = "conflicting content"
        if "expected_epoch" in request:
            owner_request["expected_epoch"] = request["expected_epoch"]
        for key in ("cutover_id", "time"):
            if key in request:
                owner_request[key] = request[key]
        authority.owner_command(
            state,
            owner_request,
            intent_directory=root / "authority_intents",
            owner=request.get("owner", True),
            tty=request.get("tty", True),
            confirmation=request.get("confirmation", authority.confirmation_text(owner_request)),
            recovery=recovery,
        )
        if not request.get("repeat") and "repeat_command_id" not in request:
            state["commands"].append(owner_request)
        journal.replace_document(root / "registry.json", state)
        return snapshot(root, state)
    if action == "transition_matrix":
        results: dict[str, Any] = {}
        for target in STATES:
            variant = copy.deepcopy(state)
            value = command(variant, target, len(state["commands"]))
            try:
                authority.owner_command(
                    variant, value, intent_directory=root / target, owner=True, tty=True, confirmation=authority.confirmation_text(value), recovery=recovery
                )
                results[target] = {"accepted": True}
            except authority.AuthorityError as error:
                results[target] = {"accepted": False, "error": str(error)}
            journal.replace_document(root / f"{target}.json", variant)
        return results
    if action in {"admit", "bootstrap"}:
        layer, mode = request["layer"], request.get("mode", "real")
        epoch = request["epoch"] if "epoch" in request else record["epoch"]
        names = sorted(f"{name}_e2e" if mode == "aws_qualify" else name for name in NAMESPACES.get(layer, (layer,)))
        approval = request.get("approval", {"run_id": request["run_id"], "layer": layer, "epoch": epoch})
        if action == "bootstrap":
            return authority.check_writer(
                record, epoch=epoch, layer=layer, mode=mode, target="ecs", run_id=request["run_id"], namespaces=names, approval=approval
            )
        binding = authority.admit_writer(
            state,
            journal.LocalJournal(root, str(request.get("journal_epoch", record["epoch"]))),
            intent_id=request["intent_id"],
            epoch=epoch,
            layer=layer,
            mode=mode,
            target=request.get("target", "ecs"),
            run_id=request["run_id"],
            database_id="original_synthetic_database",
            namespaces=request.get("namespaces", names),
            registry_kind="rehearsal",
            approval=approval,
        )
        journal.replace_document(root / "registry.json", state)
        return {**snapshot(root, state), "binding": binding}
    if action == "owner_intent":
        intent = current_journal.owner_intent(
            request["intent_id"],
            request.get("layer", "silver"),
            request["namespaces"],
            "original_synthetic_database",
            objects=request["objects"],
            command="grant",
        )
        return {"intent": intent, "intent_sha256": hashlib.sha256((current_journal.intents_path / f"{request['intent_id']}.json").read_bytes()).hexdigest()}
    if action == "complete":
        observation = {
            "kind": request.get("kind", "live"),
            "database_id": request.get("database_id", "original_synthetic_database"),
            "available": request.get("available", True),
            "namespaces": request["namespaces"],
        }
        return current_journal.complete(
            request["intent_id"], request.get("step", "terminal"), observation, terminal=request.get("terminal", True), quiesced=request.get("quiesced", True)
        )
    if action == "recover":
        return recovery
    if action == "replay_fault":
        intents = {key: value for item in journals for key, value in item.intents().items()}
        records = [value for item in sorted(journals, key=lambda value: int(value.epoch)) for value in item.completions()]
        if request["fault"] == "reverse":
            records.reverse()
        elif request["fault"] == "gap":
            records[-1]["sequence"] = 3
        elif request["fault"] == "duplicate":
            records.append(copy.deepcopy(records[-1]))
        else:
            records[-1]["epoch"] = "11"
        return journal.replay(journal.read_document(root / "inventory.json"), intents, records)
    if action == "read_record":
        state["launcher"]["catalog_authority"] = request.get("record")
        try:
            return {"refused": False, "authority": authority.read_authority(state, registry_kind=request.get("registry_kind", "real"))}
        except authority.AuthorityError as error:
            return {"refused": True, "error": str(error)}
    if action == "access":
        outcomes = {}
        operations = {
            "normal_start": lambda: authority.check_mac_start(record),
            "owner_start": lambda: authority.check_mac_start(record, owner=True, fenced=True, get_only=True),
            "network_start": lambda: authority.check_mac_start(record, owner=True, fenced=True, get_only=True, job_network=True),
            "aws_reader": lambda: authority.reader_route(record, target="aws"),
            "mac_reader": lambda: authority.reader_route(record, target="mac"),
        }
        for name, operation in operations.items():
            try:
                operation()
                outcomes[name] = True
            except authority.AuthorityError:
                outcomes[name] = False
        return outcomes
    if action == "durability":
        original = copy.deepcopy(state)
        value = command(state, "freezing", len(state["commands"]))
        real_sync = os.fsync
        trace: list[str] = []
        unchanged = True
        failed = False

        def traced_sync(fd: Any) -> None:
            nonlocal unchanged, failed
            kind = "directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file"
            unchanged = unchanged and state == original
            if not failed and request["failure"] == kind:
                failed = True
                raise OSError("synthetic intent sync interruption")
            real_sync(fd)
            trace.append(kind)

        os.fsync = traced_sync
        failure_preserves_state = True
        try:
            try:
                authority.owner_command(
                    state, value, intent_directory=root / "authority_intents", owner=True, tty=True, confirmation=authority.confirmation_text(value)
                )
            except OSError as error:
                if str(error) != "synthetic intent sync interruption":
                    raise
                failure_preserves_state = state == original
                trace.clear()
                authority.owner_command(
                    state, value, intent_directory=root / "authority_intents", owner=True, tty=True, confirmation=authority.confirmation_text(value)
                )
        finally:
            os.fsync = real_sync
        journal.replace_document(root / "registry.json", state)
        return {
            "unchanged_during_intent": unchanged,
            "synced_before_change": "file" in trace and "directory" in trace,
            "failure_preserves_state": failure_preserves_state,
            **snapshot(root, state),
        }
    raise ValueError("unknown synthetic authority action")


def fingerprints(subject: Path) -> dict[str, str | None]:
    paths = ("orchestration/catalog_authority.py", "scripts/lakehouse/catalog_journal.py", "scripts/lakehouse/catalog_authority_cases.py", "scripts/process.py")
    return {name: hashlib.sha256((subject / name).read_bytes()).hexdigest() if (subject / name).exists() else None for name in paths}


def main() -> int:
    """Run bounded stand-in cases and retain both failed and successful evidence under data/e2e."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--subject-root", type=Path, default=REPO)
    parser.add_argument("--case", choices=tuple(CASES), action="append")
    parser.add_argument("--transition-source", choices=STATES)
    parser.add_argument("--driver-root", type=Path)
    parser.add_argument("--request", type=Path)
    parser.add_argument("--response", type=Path)
    args = parser.parse_args()
    if args.driver_root is not None:
        if args.request is None or args.response is None:
            parser.error("a driver command needs --request and --response")
        result = driver(args.driver_root, json.loads(args.request.read_bytes()))
        args.response.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return 0
    if args.output is None:
        parser.error("a case run needs --output")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    before = fingerprints(args.subject_root)
    started = time.monotonic()
    results = []
    for name in args.case or CASES:
        case = Scenario(output / name, args.subject_root, min(started + 240, time.monotonic() + 90))
        case.transition_source = args.transition_source
        error = None
        try:
            CASES[name](case)
        except (OSError, RuntimeError, ValueError, KeyError, TypeError, TimeoutExpired) as failure:
            error = f"{type(failure).__name__}: {failure}"
        results.append(
            {
                "case": name,
                "passed": error is None and bool(case.assertions) and all(value["passed"] for value in case.assertions),
                "assertions": case.assertions,
                "commands": case.commands,
                "error": error,
            }
        )
    unchanged = before == fingerprints(args.subject_root)
    timestamp = datetime.now(UTC).isoformat()
    report = {
        "feature": "synthetic catalog authority and owner journal",
        "timestamp": timestamp,
        "environment": sys.version,
        "seed": "fixed synthetic commands",
        "source_hashes": before,
        "source_unchanged": unchanged,
        "mode": "full",
        "command": [sys.executable, "-m", "scripts.lakehouse.catalog_authority_cases", *sys.argv[1:]],
        "cases": results,
        "elapsed_seconds": time.monotonic() - started,
        "passed": unchanged and all(value["passed"] for value in results),
        "limits": [
            "File-backed synthetic registry and catalog observations",
            "Owner and terminal evidence supplied by the stand-in",
            "No launcher, AWS, network, Docker or native Polaris",
        ],
        "reset": "Use a new --output directory. Every driver owns a fresh isolated directory.",
        "cleanup": "done: synchronous bounded child commands finished",
    }
    (output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    (output / "retention.json").write_text(
        json.dumps({"class": 5, "purpose": report["feature"], "created": timestamp, "end_condition": "A newer accepted run replaces it"}) + "\n",
        encoding="utf-8",
    )
    sys.stdout.write(json.dumps({"passed": report["passed"], "cases": len(results), "report": str(output / "report.json")}) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
