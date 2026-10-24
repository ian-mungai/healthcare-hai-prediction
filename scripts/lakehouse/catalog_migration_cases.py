"""Run synthetic recovery scenarios through the journal command and inspect persisted results."""

from __future__ import annotations

import copy
import hashlib
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any

from scripts.process import run_command

REHEARSAL = "rehearsal_2468abcd"
PREFIX = "migration_rehearsal_2468abcd"
SILVER, GOLD, EMPTY, ABSENT = (f"{PREFIX}.{name}" for name in ("silver", "gold", "empty", "absent"))


def head(snapshot: int) -> dict[str, Any]:
    """Return an independently specified synthetic table head, with no cloud location."""
    return {
        "uuid": "synthetic_table_uuid",
        "metadata_location": f"file:///synthetic/metadata/{snapshot}.metadata.json",
        "version_id": f"version_{snapshot}",
        "snapshot_id": snapshot,
        "refs": {"main": {"snapshot_id": snapshot, "type": "branch"}, "retained": {"snapshot_id": 1, "type": "tag"}},
    }


def namespace(tables: dict[str, Any], *, properties: dict[str, str] | None = None, present: bool = True) -> dict[str, Any]:
    """Specify namespace existence, properties and complete table membership."""
    return {"state": "present" if present else "absent", "properties": properties or {}, "tables": tables}


class Scenario:
    """Keep each command, input, response and assertion under one case's evidence directory."""

    def __init__(self, root: Path, subject_root: Path, deadline: float) -> None:
        self.root = root
        self.subject_root = subject_root
        self.deadline = deadline
        self.commands: list[dict[str, Any]] = []
        self.assertions: list[dict[str, Any]] = []
        root.mkdir(parents=True)

    def invoke(self, action: str, *, expected_exit: int = 0, expected_error: str | None = None, **values: Any) -> dict[str, Any]:
        """Execute the real file-backed driver with a bounded command and retain its output."""
        request = self.root / f"{len(self.commands):02d}_{action}_request.json"
        response = request.with_name(request.name.replace("_request.json", "_response.json"))
        request.write_text(json.dumps({"action": action, "rehearsal_id": REHEARSAL, **values}, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        args = ["-m", "scripts.lakehouse.catalog_journal", "--stand-in", str(self.root / "driver"), "--request", str(request), "--response", str(response)]
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("recovery stand-in exceeded its absolute case deadline")
        result = run_command(sys.executable, args, cwd=self.subject_root, timeout=min(10, remaining))
        self.commands.append({"argv": [sys.executable, *args], "cwd": str(self.subject_root), "exit_code": result.returncode, "stderr": result.stderr})
        self.check(f"{action} exit", result.returncode, expected_exit)
        if expected_error is not None:
            self.check(f"{action} rejection cause", expected_error in result.stderr, True)
        if expected_exit:
            return {}
        if not response.exists():
            raise RuntimeError(f"{action} produced no response: {result.stderr}")
        return json.loads(response.read_text(encoding="utf-8"))

    def check(self, label: str, actual: Any, expected: Any) -> None:
        """Compare an observable result with an expectation supplied by the scenario."""
        self.assertions.append({"label": label, "expected": expected, "observed": actual, "passed": actual == expected})

    def scoped(self, name: str) -> Scenario:
        """Retain a variant's persisted state without replacing another variant's evidence."""
        child = Scenario(self.root / name, self.subject_root, self.deadline)
        child.commands, child.assertions = self.commands, self.assertions
        return child


def intent_without_completion(case: Scenario) -> None:
    """Cover the three missing-hook orders, old records, rebuilt catalogs and older dumps."""
    parent = case
    for fault in ("kill_after_commit", "database_before_hook", "before_hook"):
        case = parent.scoped(fault)
        initial = {SILVER: namespace({"kept": head(1)}), GOLD: namespace({"other": head(1)})}
        case.invoke("seed", inventory=initial, layers={SILVER: "silver", GOLD: "gold"})
        case.invoke("write", intent_id="older", layer="silver", changes={SILVER: initial[SILVER]})
        case.invoke("save_dump")
        case.invoke(
            "write",
            expected_exit=-signal.SIGKILL if fault == "kill_after_commit" else 0,
            intent_id="pending",
            layer="silver",
            changes={SILVER: namespace({"kept": head(2)})},
            fault=fault,
        )
        persisted = case.invoke("inspect")
        case.check(f"{fault}: pointer committed before interruption", persisted["catalog"][SILVER]["tables"]["kept"], head(2))
        before = case.invoke("recover")
        case.check(f"{fault}: unresolved despite older confirmation", before["unresolved"], [SILVER])
        case.check(f"{fault}: pending intent named", before["pending_intents"], ["pending"])
        case.check(f"{fault}: unresolved namespace not rebuilt", SILVER in before["catalog"], False)
        case.check(f"{fault}: unrelated namespace rebuilt", before["catalog"].get(GOLD), initial[GOLD])
        case.check(f"{fault}: blocked layer", before["blocked_layers"], ["silver"])
        case.invoke("complete", intent_id="pending", source="rebuilt", step="rebuilt")
        rebuilt = case.invoke("recover")
        case.check(f"{fault}: rebuilt database cannot confirm", rebuilt["pending_intents"], ["pending"])
        case.invoke("restore_dump")
        case.invoke("complete", intent_id="pending", source="live", step="dump")
        restored = case.invoke("recover")
        case.check(f"{fault}: older dump cannot confirm", restored["unresolved"], [SILVER])
        case.check(f"{fault}: dump keeps intent pending", restored["pending_intents"], ["pending"])
        if fault == "before_hook":
            case = parent.scoped("live_confirmation")
            case.invoke("seed", inventory=initial, layers={SILVER: "silver", GOLD: "gold"})
            case.invoke("write", intent_id="live_pending", layer="silver", changes={SILVER: namespace({"kept": head(2)})}, fault="before_hook")
            case.invoke("complete", intent_id="live_pending", source="live", step="reconcile")
            confirmed = case.invoke("recover")
            case.check("original quiesced live source can confirm", confirmed["pending_intents"], [])
            case.check("confirmed commit is replayed", confirmed["catalog"][SILVER]["tables"]["kept"], head(2))

    replacement = parent.scoped("replacement_live")
    initial = {SILVER: namespace({"kept": head(1)})}
    replacement.invoke("seed", inventory=initial, layers={SILVER: "silver"})
    replacement.invoke("write", intent_id="pending", layer="silver", changes={SILVER: namespace({"kept": head(2)})}, fault="before_hook")
    provenance = replacement.root / "driver/provenance.json"
    provenance.write_text(json.dumps({"kind": "live", "database_id": "replacement_database", "available": True}), encoding="utf-8")
    replacement.invoke("complete", intent_id="pending", step="replacement")
    rejected = replacement.invoke("recover")
    replacement.check("replacement live database cannot confirm", rejected["pending_intents"], ["pending"])
    replacement.check("replacement live database leaves namespace unresolved", rejected["unresolved"], [SILVER])

    late = parent.scoped("same_step_confirmation")
    late.invoke("seed", inventory=initial, layers={SILVER: "silver"})
    late.invoke("write", intent_id="pending", layer="silver", changes={SILVER: namespace({"kept": head(2)})}, fault="database_before_hook")
    late.check("unavailable terminal read stays unconfirmed", late.invoke("recover")["pending_intents"], ["pending"])
    provenance = late.root / "driver/provenance.json"
    provenance.write_text(json.dumps({"kind": "live", "database_id": REHEARSAL, "available": True}), encoding="utf-8")
    late.invoke("complete", intent_id="pending")
    confirmed = late.invoke("recover")
    late.check("same terminal step later clears the intent", confirmed["pending_intents"], [])
    late.check("same terminal step replays the committed head", confirmed["catalog"][SILVER]["tables"]["kept"], head(2))
    late.invoke("complete", intent_id="pending")
    late.check("repeated later confirmation appends only once", late.invoke("inspect")["completion_count"], 2)


def namespace_replay(case: Scenario) -> None:
    """Replay confirmed deletion, properties, empty namespaces and absence, then compare both ways."""
    initial = {SILVER: namespace({"deleted": head(1), "kept": head(1)}, properties={"location": "file:///synthetic/old"}), ABSENT: namespace({"old": head(1)})}
    final = {
        SILVER: namespace({"kept": head(2)}, properties={"location": "file:///synthetic/new", "owner": "synthetic"}),
        EMPTY: namespace({}, properties={"purpose": "empty"}),
        ABSENT: namespace({}, present=False),
    }
    case.invoke("seed", inventory=initial, layers={SILVER: "silver", EMPTY: "silver", ABSENT: "silver"})
    metadata = case.root / "driver/metadata/seed_heads.json"
    original_metadata = hashlib.sha256(metadata.read_bytes()).hexdigest()
    case.invoke("write", intent_id="replacement", layer="silver", changes=final)
    first = case.invoke("recover")
    expected = {key: value for key, value in final.items() if value["state"] == "present"}
    case.check("newest namespace state replaces earlier membership", first["catalog"], expected)
    case.check(
        "non-purging deletion preserves original metadata", hashlib.sha256(metadata.read_bytes()).hexdigest() if metadata.exists() else None, original_metadata
    )
    create = {"create_namespace": SILVER, "properties": final[SILVER]["properties"]}
    register = {"register_table": f"{SILVER}.kept", "head": head(2)}
    case.check("namespace properties restored", create in first["operations"], True)
    case.check("properties precede table registration", first["operations"].index(create) < first["operations"].index(register), True)
    case.check("empty namespace exists", first["catalog"].get(EMPTY), final[EMPTY])
    case.check("confirmed absence registers nothing", ABSENT in first["catalog"], False)
    case.check("normal replay has complete parity", case.invoke("parity", actual=first["catalog"], expected=expected)["equal"], True)
    second = case.invoke("recover")
    case.check("repeated rebuild is stable", second, first)
    case.invoke("write", intent_id="replacement", layer="silver", changes=final)
    case.check("repeated intent and completion do not append", case.invoke("inspect")["completion_count"], 1)
    extra = copy.deepcopy(expected)
    extra[ABSENT] = namespace({})
    case.check("extra namespace fails parity", case.invoke("parity", actual=extra, expected=expected)["equal"], False)
    extra = copy.deepcopy(expected)
    extra[SILVER]["tables"]["deleted"] = head(1)
    case.check("extra table fails parity", case.invoke("parity", actual=extra, expected=expected)["equal"], False)
    missing = copy.deepcopy(expected)
    missing.pop(EMPTY)
    case.check("missing empty namespace fails parity", case.invoke("parity", actual=missing, expected=expected)["equal"], False)
    wrong_properties = copy.deepcopy(expected)
    wrong_properties[SILVER]["properties"] = initial[SILVER]["properties"]
    case.check("wrong namespace properties fail parity", case.invoke("parity", actual=wrong_properties, expected=expected)["equal"], False)

    invalid = case.scoped("invalid_fault")
    invalid.invoke("seed", inventory=initial, layers={SILVER: "silver", ABSENT: "silver"})
    driver = invalid.root / "driver"
    before = {str(path.relative_to(driver)): hashlib.sha256(path.read_bytes()).hexdigest() for path in driver.rglob("*") if path.is_file()}
    invalid.invoke(
        "write",
        expected_exit=1,
        expected_error="unknown stand-in interruption point",
        intent_id="invalid",
        layer="silver",
        changes={SILVER: namespace({"kept": head(999)})},
        fault="mistyped_fault",
    )
    after = {str(path.relative_to(driver)): hashlib.sha256(path.read_bytes()).hexdigest() for path in driver.rglob("*") if path.is_file()}
    invalid.check("invalid fault leaves every persisted file unchanged", after, before)


def database_loss(case: Scenario) -> None:
    """Leave the unresolved namespace uncreated while confirmed neighbors rebuild from recorded heads."""
    initial = {SILVER: namespace({"kept": head(1)}), GOLD: namespace({"other": head(1)})}
    case.invoke("seed", inventory=initial, layers={SILVER: "silver", GOLD: "gold"})
    case.invoke("write", intent_id="confirmed_gold", layer="gold", changes={GOLD: namespace({"other": head(2)})})
    case.invoke("write", intent_id="failed_pointer", layer="gold", changes={GOLD: namespace({"other": head(999)})}, fault="metadata_before_commit")
    case.invoke("write", intent_id="lost_database", layer="silver", changes={SILVER: namespace({"kept": head(2)})}, fault="before_hook")
    case.invoke("plant_uncommitted_files", guessed_head=head(999))
    case.invoke("lose_database")
    case.invoke("complete", intent_id="lost_database", source="live", step="terminal")
    result = case.invoke("recover")
    case.check("loss names the unresolved namespace", result["unresolved"], [SILVER])
    case.check("loss names the unresolved intent", result["pending_intents"], ["lost_database"])
    case.check("loss leaves unresolved namespace uncreated", SILVER in result["catalog"], False)
    case.check("other namespace uses confirmed head", result["catalog"].get(GOLD), namespace({"other": head(2)}))
    case.check("affected layer stays closed", result["blocked_layers"], ["silver"])
    case.check("no file or backup head is inferred", "999" in json.dumps(result["catalog"]), False)


CASES = {"intent_without_completion": intent_without_completion, "namespace_replay": namespace_replay, "database_loss": database_loss}
