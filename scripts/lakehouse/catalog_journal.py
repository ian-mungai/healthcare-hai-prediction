"""Match durable mutation intents to catalog confirmations and replay complete namespace states.

The local journal and file-backed rehearsal driver make no network calls. A later catalog adapter must supply
the original database's provenance and establish writer quiescence before it can confirm an intent.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import shutil
import signal
import stat
from collections.abc import Sequence
from pathlib import Path
from typing import Any

Document = dict[str, Any]


def encoded(value: Document) -> bytes:
    """Encode a journal or catalog document deterministically."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def read_document(path: Path) -> Document:
    """Load a JSON object, refusing non-object documents at the storage boundary."""
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object at {path.name}")
    return value


def sync_directory(path: Path) -> None:
    """Persist directory entries before a mutation can rely on a record."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def durable_directory(path: Path) -> None:
    """Create a journal directory and persist each new directory entry before using it."""
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for current in reversed(missing):
        current.mkdir(exist_ok=True)
        sync_directory(current)
        sync_directory(current.parent)


def private_directory(path: Path) -> None:
    """Create a directory only this user can enter; tighten a wider one this user owns and refuse any other."""
    durable_directory(path.parent)
    if not path.exists() and not path.is_symlink():
        path.mkdir(mode=0o700)
        sync_directory(path.parent)
    status = path.lstat()
    if not stat.S_ISDIR(status.st_mode) or status.st_uid != os.getuid():
        raise PermissionError("transport_folder_must_be_a_private_directory")
    if status.st_mode & 0o077:
        path.chmod(0o700)
    sync_directory(path)


def write_once(path: Path, value: Document) -> None:
    """Create a durable immutable record, accepting an exact retry and refusing a conflicting retry."""
    payload = encoded(value)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise ValueError(f"immutable record conflict: {path.name}") from None
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    else:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    sync_directory(path.parent)


def replace_document(path: Path, value: Document) -> None:
    """Replace a single-owner stand-in database durably without exposing a partial document."""
    temporary = path.with_name(path.name + ".pending")
    with temporary.open("wb") as stream:
        stream.write(encoded(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    sync_directory(path.parent)


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("record identifiers must contain only letters, digits, underscores and hyphens")
    return value


def namespace_states(value: Document) -> Document:
    """Validate complete namespace observations, including absence, properties and pinned table heads."""
    for name, state in value.items():
        if not isinstance(name, str) or not isinstance(state, dict) or set(state) != {"state", "properties", "tables"}:
            raise ValueError("invalid namespace observation")
        if state["state"] not in {"present", "absent"} or not isinstance(state["properties"], dict) or not isinstance(state["tables"], dict):
            raise ValueError("namespace observations require existence, properties and complete tables")
        if state["state"] == "absent" and (state["properties"] or state["tables"]):
            raise ValueError("an absent namespace cannot hold properties or tables")
        if not all(isinstance(key, str) and isinstance(item, str) for key, item in state["properties"].items()):
            raise ValueError("namespace properties must be strings")
        for table, head in state["tables"].items():
            if not isinstance(table, str) or not isinstance(head, dict) or set(head) != {"uuid", "metadata_location", "version_id", "snapshot_id", "refs"}:
                raise ValueError("incomplete table head")
            if not all(isinstance(head[key], str) and head[key] for key in ("uuid", "metadata_location", "version_id")):
                raise ValueError("table heads require UUID, metadata location and version")
            if (head["snapshot_id"] is not None and type(head["snapshot_id"]) is not int) or not isinstance(head["refs"], dict):
                raise ValueError("invalid snapshot or references")
    return copy.deepcopy(value)


class LocalJournal:
    """Persist one rehearsal's dispatch intents and ordered, write-once completions under a single owner."""

    def __init__(self, root: Path, epoch: str) -> None:
        self.epoch = identifier(epoch)
        self.intents_path = root / "dispatch" / epoch
        self.completions_path = root / "lakehouse" / "catalog_journal" / epoch

    def intent(self, intent_id: str, layer: str, namespaces: list[str], database_id: str) -> Document:
        """Fsync an intent before its writer's first effect; exact retries keep the same identity."""
        intent = {
            "intent_id": identifier(intent_id),
            "epoch": self.epoch,
            "layer": identifier(layer),
            "namespaces": sorted(set(namespaces)),
            "database_id": database_id,
        }
        if not intent["namespaces"]:
            raise ValueError("an intent must name every namespace it may mutate")
        durable_directory(self.intents_path)
        write_once(self.intents_path / f"{intent_id}.json", intent)
        return intent

    def owner_intent(self, command_id: str, layer: str, namespaces: list[str], database_id: str, *, objects: list[str], command: str) -> Document:
        """Fsync an owner catalog command's complete object scope before its first catalog mutation."""
        if not objects or any(not isinstance(item, str) or not item.strip() for item in objects):
            raise ValueError("an owner command must name its objects")
        intent = {
            "intent_id": identifier(command_id),
            "command_id": command_id,
            "kind": "owner_command",
            "command": identifier(command),
            "epoch": self.epoch,
            "layer": identifier(layer),
            "namespaces": sorted(set(namespaces)),
            "database_id": database_id,
            "objects": sorted(set(objects)),
        }
        durable_directory(self.intents_path)
        write_once(self.intents_path / f"{command_id}.json", intent)
        return intent

    def intents(self) -> dict[str, Document]:
        """Read the complete rehearsal dispatch ledger without consulting any other epoch."""
        return {path.stem: read_document(path) for path in sorted(self.intents_path.glob("*.json"))}

    def completions(self) -> list[Document]:
        """Read all ordered completions, rejecting missing or inconsistent sequence numbers."""
        records = [read_document(path) for path in sorted(self.completions_path.glob("*.json"))]
        if [record["sequence"] for record in records] != list(range(1, len(records) + 1)):
            raise ValueError("journal sequence is incomplete")
        return records

    def complete(self, intent_id: str, step: str, observation: Document, *, terminal: bool, quiesced: bool) -> Document:
        """Append a full observation; a rebuilt database or a dump never confirms an intent."""
        intent = self.intents()[identifier(intent_id)]
        states = namespace_states(observation["namespaces"])
        if set(states) != set(intent["namespaces"]):
            raise ValueError("completion must observe every namespace of its intent")
        confirmed = terminal and quiesced and observation["available"] and observation["kind"] == "live" and observation["database_id"] == intent["database_id"]
        record: Document = {
            "epoch": self.epoch,
            "intent_id": intent_id,
            "step": identifier(step),
            "status": "confirmed" if confirmed else "unconfirmed",
            "source": {key: observation[key] for key in ("kind", "database_id", "available")},
            "terminal": terminal,
            "quiesced": quiesced,
            "namespaces": states,
        }
        records = self.completions()
        prior = next((item for item in records if item["intent_id"] == intent_id and item["step"] == step and item["status"] == record["status"]), None)
        record["sequence"] = prior["sequence"] if prior else len(records) + 1
        durable_directory(self.completions_path)
        write_once(self.completions_path / f"{record['sequence']:020d}_{intent_id}_{step}.json", record)
        return record


def replay(inventory: Document, intents: dict[str, Document], completions: list[Document]) -> Document:
    """Replace namespace state from confirmed records and withhold every namespace touched by a pending intent."""
    states = namespace_states(inventory)
    confirmed_intents: set[str] = set()
    unconfirmed: set[str] = set()
    sequences: dict[str, int] = {}
    previous_epoch: str | None = None
    for record in completions:
        intent = intents.get(record["intent_id"])
        if intent is None or record["epoch"] != intent["epoch"] or set(record["namespaces"]) != set(intent["namespaces"]):
            raise ValueError("journal completion does not match a dispatch intent")
        epoch = record["epoch"]
        if (
            previous_epoch is not None
            and epoch != previous_epoch
            and (not epoch.isdecimal() or not previous_epoch.isdecimal() or int(epoch) <= int(previous_epoch))
        ):
            raise ValueError("journal epochs are out of order")
        if type(record["sequence"]) is not int or record["sequence"] != sequences.get(epoch, 0) + 1:
            raise ValueError("journal sequence is incomplete or out of order")
        sequences[epoch], previous_epoch = record["sequence"], epoch
        confirmed = record["status"] == "confirmed"
        if confirmed:
            source = record["source"]
            if (
                source["kind"] != "live"
                or source["database_id"] != intent["database_id"]
                or not source["available"]
                or not record["terminal"]
                or not record["quiesced"]
            ):
                raise ValueError("confirmed completion lacks a quiesced original live source")
            confirmed_intents.add(record["intent_id"])
            for name, state in namespace_states(record["namespaces"]).items():
                states[name] = state
                unconfirmed.discard(name)
        elif record["status"] == "unconfirmed":
            unconfirmed.update(intent["namespaces"])
        else:
            raise ValueError("unknown completion status")
    pending = sorted(set(intents) - confirmed_intents)
    unresolved = unconfirmed | {name for intent_id in pending for name in intents[intent_id]["namespaces"]}
    return {
        "namespace_states": {name: value for name, value in states.items() if name not in unresolved},
        "pending_intents": pending,
        "unresolved": sorted(unresolved),
        "blocked_layers": sorted(
            {item["layer"] for item in intents.values() if unresolved.intersection(item["namespaces"])} | {intents[intent_id]["layer"] for intent_id in pending}
        ),
    }


def recover_from_journals(inventory: Document, journals: Sequence[LocalJournal]) -> Document:
    """Apply the cutover inventory and numeric authority epochs, preserving each journal's sequence order."""
    if len({item.epoch for item in journals}) != len(journals):
        raise ValueError("journal epoch is duplicated")
    if len(journals) > 1 and any(not item.epoch.isdecimal() for item in journals):
        raise ValueError("recovery cannot mix rehearsal and real journal epochs")
    ordered = sorted(journals, key=lambda item: int(item.epoch) if item.epoch.isdecimal() else 0)
    intents: dict[str, Document] = {}
    completions: list[Document] = []
    for journal in ordered:
        for intent_id, intent in journal.intents().items():
            if intent_id in intents or intent["epoch"] != journal.epoch or intent["intent_id"] != intent_id:
                raise ValueError("journal intent identity or epoch conflicts")
            intents[intent_id] = intent
        completions.extend(journal.completions())
    return replay(inventory, intents, completions)


def parity(actual: Document, expected: Document) -> Document:
    """Compare complete namespace membership, properties and table heads in both directions."""
    actual, expected = namespace_states(actual), namespace_states(expected)
    extra, missing = sorted(set(actual) - set(expected)), sorted(set(expected) - set(actual))
    different = sorted(name for name in actual.keys() & expected.keys() if actual[name] != expected[name])
    return {"equal": not extra and not missing and not different, "extra_namespaces": extra, "missing_namespaces": missing, "different_namespaces": different}


class StandIn:
    """Drive a local rehearsal database and journal without real credentials, services or writer admission."""

    def __init__(self, root: Path, rehearsal_id: str) -> None:
        if not re.fullmatch(r"rehearsal_[0-9a-f]{8}", rehearsal_id):
            raise ValueError("a stand-in requires a rehearsal identifier")
        self.root, self.rehearsal_id = root, rehearsal_id
        self.marker = root / "rehearsal_owner.json"
        self.journal = LocalJournal(root, rehearsal_id)

    def seed(self, request: Document) -> Document:
        if self.root.exists():
            self.owned()
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True)
        replace_document(self.marker, {"rehearsal_id": self.rehearsal_id})
        for path in (self.journal.intents_path, self.journal.completions_path, self.root / "metadata"):
            path.mkdir(parents=True)
            current = path
            while current != self.root.parent:
                sync_directory(current)
                current = current.parent
        inventory = namespace_states(request["inventory"])
        self.check_names(inventory)
        write_once(self.root / "inventory.json", inventory)
        write_once(self.root / "layers.json", request["layers"])
        write_once(self.root / "metadata" / "seed_heads.json", inventory)
        replace_document(self.root / "catalog.json", inventory)
        replace_document(self.root / "provenance.json", {"kind": "live", "database_id": self.rehearsal_id, "available": True})
        return {"seeded": True}

    def owned(self) -> None:
        if read_document(self.marker) != {"rehearsal_id": self.rehearsal_id}:
            raise ValueError("stand-in directory belongs to another rehearsal")

    def check_names(self, states: Document) -> None:
        prefix = "migration_" + self.rehearsal_id
        if any(name != prefix and not name.startswith(prefix + ".") for name in states):
            raise ValueError("stand-in namespaces must belong to this rehearsal")

    def complete(self, request: Document) -> Document:
        intent = self.journal.intents()[identifier(request["intent_id"])]
        provenance = read_document(self.root / "provenance.json")
        if request.get("source", "live") == "rebuilt":
            catalog = read_document(self.root / "rebuilt.json")
            provenance = {"kind": "journal_rebuild", "database_id": "rebuilt", "available": True}
        else:
            catalog = read_document(self.root / "catalog.json")
        absent = {"state": "absent", "properties": {}, "tables": {}}
        observation = {**provenance, "namespaces": {name: catalog.get(name, absent) for name in intent["namespaces"]}}
        return self.journal.complete(request["intent_id"], request.get("step", "terminal"), observation, terminal=True, quiesced=True)

    def write(self, request: Document) -> Document:
        fault = request.get("fault")
        if fault not in {None, "metadata_before_commit", "kill_after_commit", "database_before_hook", "before_hook"}:
            raise ValueError("unknown stand-in interruption point")
        changes = namespace_states(request["changes"])
        self.check_names(changes)
        provenance = read_document(self.root / "provenance.json")
        if provenance["kind"] != "live" or not provenance["available"]:
            raise ValueError("stand-in writes require the original available database")
        self.journal.intent(request["intent_id"], request["layer"], list(changes), provenance["database_id"])
        catalog = read_document(self.root / "catalog.json")
        write_once(self.root / "metadata" / f"{identifier(request['intent_id'])}.json", changes)
        if fault != "metadata_before_commit":
            for name, state in changes.items():
                if state["state"] == "present":
                    catalog[name] = state
                else:
                    catalog.pop(name, None)
            replace_document(self.root / "catalog.json", catalog)
        if fault == "kill_after_commit":
            os.kill(os.getpid(), signal.SIGKILL)
        if fault == "before_hook":
            return {"completion": "not_started"}
        if fault == "database_before_hook":
            replace_document(self.root / "provenance.json", {**provenance, "available": False})
        return self.complete(request)

    def recover(self) -> Document:
        result = replay(read_document(self.root / "inventory.json"), self.journal.intents(), self.journal.completions())
        expected = {name: state for name, state in result["namespace_states"].items() if state["state"] == "present"}
        rebuilt: Document = {}
        operations: list[Document] = []
        for name, state in sorted(expected.items()):
            rebuilt[name] = {"state": "present", "properties": state["properties"], "tables": {}}
            operations.append({"create_namespace": name, "properties": state["properties"]})
            for table, head in sorted(state["tables"].items()):
                rebuilt[name]["tables"][table] = head
                operations.append({"register_table": f"{name}.{table}", "head": head})
        replace_document(self.root / "rebuilt.json", rebuilt)
        return {
            **result,
            "catalog": rebuilt,
            "operations": operations,
        }

    def dispatch(self, request: Document) -> Document:
        action = request["action"]
        if action == "seed":
            return self.seed(request)
        self.owned()
        if action == "write":
            return self.write(request)
        if action == "complete":
            return self.complete(request)
        if action == "recover":
            return self.recover()
        if action == "inspect":
            return {
                "catalog": read_document(self.root / "catalog.json"),
                "completion_count": len(self.journal.completions()),
                "intents": self.journal.intents(),
            }
        if action == "parity":
            return parity(request["actual"], request["expected"])
        if action == "save_dump":
            replace_document(self.root / "older_dump.json", read_document(self.root / "catalog.json"))
        elif action == "restore_dump":
            replace_document(self.root / "catalog.json", read_document(self.root / "older_dump.json"))
            replace_document(self.root / "provenance.json", {"kind": "restored_dump", "database_id": self.rehearsal_id, "available": True})
        elif action == "lose_database":
            replace_document(self.root / "catalog.json", {})
            replace_document(self.root / "provenance.json", {"kind": "lost", "database_id": self.rehearsal_id, "available": False})
        elif action == "plant_uncommitted_files":
            replace_document(self.root / "metadata" / "999999.metadata.json", request["guessed_head"])
            replace_document(self.root / "publish_backup.json", request["guessed_head"])
        else:
            raise ValueError("unknown stand-in action")
        return {"action": action, "done": True}


def main() -> int:
    """Execute one local synthetic rehearsal command from a recorded JSON request."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stand-in", type=Path, required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--response", type=Path, required=True)
    args = parser.parse_args()
    request = read_document(args.request)
    result = StandIn(args.stand_in.resolve(), request["rehearsal_id"]).dispatch(request)
    replace_document(args.response, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
