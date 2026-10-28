"""Drive the controller transfer entry point with versioned S3 and logical database stand-ins."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
import sys
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.lakehouse import run_lock
from scripts.lakehouse.catalog_journal import encoded
from scripts.orchestration.run_controller_authority_e2e import catalog

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "orchestration/controller_manifest.py",
    "orchestration/controller_import.py",
    "orchestration/controller_transfer.py",
    "scripts/orchestration/controller_transfer.py",
    "scripts/orchestration/run_controller_transfer_e2e.py",
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def subject() -> Any:
    return importlib.import_module("orchestration.controller_transfer")


def manifests() -> Any:
    return importlib.import_module("orchestration.controller_manifest")


class SimulatedCrash(BaseException):
    """Lose adapter memory after an external effect without writing an error receipt."""


class Store:
    """Keep all versions and delete markers; enforce conditional creates and pinned reads."""

    def __init__(self) -> None:
        self.values: dict[str, list[dict[str, Any]]] = {}
        self.calls: list[tuple[str, str, float]] = []
        self.sequence = 0
        self.lost = False
        self.interrupt_after_export = False
        self.corrupt = False
        self.interrupt: str | None = None
        self.deadline_hook: Callable[[], None] | None = None

    def seed(self, key: str, raw: bytes, *, marker: bool = False) -> dict[str, Any]:
        self.sequence += 1
        value = {"key": key, "version_id": f"version_{self.sequence}", "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "delete_marker": marker}
        self.values.setdefault(key, []).append({**value, "raw": raw})
        return value

    def _call(self, operation: str, key: str, deadline: float) -> None:
        require(time.monotonic() < deadline, "port received an expired deadline")
        self.calls.append((operation, key, deadline))
        if self.interrupt == operation:
            self.interrupt = None
            raise InterruptedError("synthetic interruption")
        if self.deadline_hook is not None:
            self.deadline_hook()

    def create(self, key: str, chunks: Iterable[bytes], *, if_none_match: str, deadline: float) -> None:
        self._call("create", key, deadline)
        require(if_none_match == "*", "conditional create header missing")
        if self.values.get(key) and not self.values[key][-1]["delete_marker"]:
            raise subject().ObjectExists()
        self.seed(key, b"".join(chunks))
        if self.interrupt_after_export and "/exports/" in key:
            self.interrupt_after_export = False
            raise SimulatedCrash()
        if self.lost:
            self.lost = False
            raise OSError("synthetic lost response")

    def current(self, key: str, *, deadline: float) -> dict[str, Any] | None:
        self._call("current", key, deadline)
        versions = self.values.get(key, [])
        return {name: item for name, item in versions[-1].items() if name != "raw"} if versions and not versions[-1]["delete_marker"] else None

    def read(self, reference: dict[str, Any], *, deadline: float) -> Iterator[bytes]:
        self._call("read", reference["key"], deadline)
        require(bool(reference.get("version_id")), "download has no version pin")
        version = next(item for item in self.values[reference["key"]] if item["version_id"] == reference["version_id"])
        raw = bytes(version["raw"])
        yield bytes([raw[0] ^ 1]) + raw[1:] if self.corrupt and raw else raw

    def versions(self, key: str, *, token: str | None, deadline: float) -> dict[str, Any]:
        self._call("versions", key, deadline)
        values = self.values.get(key, [])
        index = int(token or 0)
        entries = [{name: value for name, value in item.items() if name != "raw"} for item in values[index : index + 1]]
        return {"versions": entries, "next_token": str(index + 1) if index + 1 < len(values) else None}

    def delete(self, reference: dict[str, Any], *, deadline: float) -> None:
        self._call("delete", reference["key"], deadline)
        self.values[reference["key"]] = [item for item in self.values[reference["key"]] if item["version_id"] != reference["version_id"]]


class Databases:
    """Supply independent source and destination inventories, including a protected catalog sentinel."""

    def __init__(self) -> None:
        self.source = {
            name: {
                "database": name,
                "server_major": 16,
                "schema_sha256": hashlib.sha256(name.encode()).hexdigest(),
                "tables": {"runs": {"rows": 2, "sha256": "b" * 64}},
            }
            for name in ("airflow", "marquez")
        }
        self.target: dict[str, dict[str, Any] | None] = {"airflow": None, "marquez": None, "polaris": {"sentinel": "preserved"}}
        self.restores: list[str] = []
        self.corrupt = False
        self.after_restore = False
        self.target_major = 16
        self.variable_exports = False
        self.exports: list[str] = []

    def inventory(self, name: str, *, source: bool, deadline: float) -> dict[str, Any] | None:
        require(time.monotonic() < deadline, "database inventory received an expired deadline")
        return copy.deepcopy(self.source[name] if source else self.target[name])

    def export(self, name: str, *, deadline: float) -> Iterator[bytes]:
        require(time.monotonic() < deadline, "export received an expired deadline")
        self.exports.append(name)
        yield (b" " * len(self.exports) if self.variable_exports else b"") + encoded(self.source[name])

    def restore(self, name: str, chunks: Iterable[bytes], *, options: tuple[str, ...], deadline: float) -> None:
        require(time.monotonic() < deadline, "restore received an expired deadline")
        require(options == ("--no-owner", "--no-privileges", "--single-transaction", "--exit-on-error"), "restore options widen database scope")
        require(name in ("airflow", "marquez"), "restore touched the catalog")
        self.restores.append(name)
        self.target[name] = {**json.loads(b"".join(chunks)), "server_major": self.target_major}
        if self.corrupt:
            self.target[name] = {**self.source[name], "tables": {}}
        if self.after_restore:
            self.after_restore = False
            raise InterruptedError("synthetic restore response loss")


class Fixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.source = root / "source"
        self.destination = root / "destination"
        self.receipt = root / "receipt.json"
        self.store = Store()
        self.databases = Databases()
        state = run_lock.empty_state("synthetic_registry", 2)
        state["launcher"].update(
            catalog_authority=catalog(),
            controller_authority={
                "version": 1,
                "controller_owner": "mac",
                "controller_epoch": 3,
                "controller_phase": "draining",
                "controller_transfer_id": "synthetic_transfer",
                "controller_state_digest": "a" * 64,
            },
        )
        state["runs"] = {"retained": {"state": "failed_retained", "generation": 5, "payload_sha256": "c" * 64, "pid": 123, "pid_start": "source_start"}}
        state["requests"] = {"completed": {"payload_sha256": "d" * 64, "request": {"path": "/source/checkout/immutable"}}}
        state["readers"] = {"reader": {"state": "registered", "snapshot_set": "snapshot_1", "publish_run": "retained", "pid": 123, "pid_start": "source_start"}}
        self.state = state
        self.files = {
            "registry/registry_id": encoded({"registry_id": "synthetic_registry", "kind": "real"}),
            "registry/registry.json": encoded(state),
            "ledger/retained.jsonl": encoded({"submission_id": "original_submission", "payload_sha256": "d" * 64}),
            "records/completed.json": encoded({"run_id": "retained", "generation": 5, "payload_sha256": "d" * 64}),
        }
        for path, raw in self.files.items():
            destination = self.source / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
        roles = {
            "registry/registry_id": "registry_identity",
            "registry/registry.json": "registry",
            "ledger/retained.jsonl": "ledger",
            "records/completed.json": "broker_record",
        }
        self.manifest = manifests().build_manifest(
            self.source,
            transfer_id="synthetic_transfer",
            source_machine="source_machine_boot_1",
            declarations=[{"relative_path": path, "role": roles[path], "classification": "Confidential"} for path in self.files],
        )
        cloud = self.store.seed("lakehouse/run_stage/retained/snapshot_1", b"approved synthetic cloud bytes")
        self.options: dict[str, Any] = {
            "version": 1,
            "path_mappings": [{"source": "/source/checkout", "destination": "/aws/checkout"}],
            "historical_paths": ["/requests/completed/request/path"],
            "active_inputs": {"snapshot_1": {**cloud, "approved": True, "run_id": "retained", "generation": 5}},
            "fernet_key_version": 4,
        }
        self.specs = [
            {"database": name, "dump_major": 16, "restore_major": 16, "target_major": 16, "inventory": self.databases.source[name]}
            for name in ("airflow", "marquez")
        ]
        self.privacy = {"approved": True, "manifest_sha256": manifests().digest(self.manifest), "database_specs_sha256": manifests().digest(self.specs)}

    def execute(self, *, deadline: float | None = None) -> dict[str, Any]:
        entry = importlib.import_module("scripts.orchestration.controller_transfer")
        return entry.execute_transfer(
            manifest=self.manifest,
            source=self.source,
            destination=self.destination,
            receipt_path=self.receipt,
            store=self.store,
            databases=self.databases,
            database_specs=self.specs,
            import_options=self.options,
            privacy=self.privacy,
            deadline=time.monotonic() + 20 if deadline is None else deadline,
        )


def refuses(operation: Callable[[], Any], code: str) -> None:
    try:
        operation()
    except (ValueError, RuntimeError, OSError) as error:
        require(getattr(error, "code", str(error)) == code, f"expected {code}, observed {type(error).__name__}: {error}")
    else:
        raise AssertionError(f"accepted operation requiring {code}")


def success(f: Fixture) -> None:
    result = f.execute()
    require(result["import_verified"] is True and result["admission_closed"] is True, "import opened admission or omitted proof")
    imported = json.loads((f.destination / "registry/registry.json").read_text())
    require(imported["requests"] == f.state["requests"], "import changed immutable replay bodies or hashes")
    require(imported["launcher"]["catalog_authority"] == f.state["launcher"]["catalog_authority"], "catalog changed")
    require(imported["runs"]["retained"]["generation"] == 5 and imported["readers"]["reader"]["snapshot_set"] == "snapshot_1", "generation or reservation lost")
    require("pid" not in imported["runs"]["retained"] and "pid" not in imported["readers"]["reader"], "source PID remains actionable")
    require(f.databases.target["polaris"] == {"sentinel": "preserved"}, "catalog sentinel changed")
    require(f.databases.restores == ["airflow", "marquez"], "databases were not restored logically")
    require(all(not versions for key, versions in f.store.values.items() if "/exports/" in key), "exports remain after import")
    before = f.store.sequence
    first_calls = len(f.store.calls)
    require(f.execute() == result and f.store.sequence == before and f.databases.restores == ["airflow", "marquez"], "repeat transfer duplicated effects")
    require(len({deadline for _, _, deadline in f.store.calls[:first_calls]}) == 1, "operations did not share one deadline")
    require(len(f.store.calls) == first_calls, "terminal receipt retried object operations")
    require(all(record["classification"] == "Restricted" for record in result["exports"].values()), "database exports lost their classification")


def exclusions(f: Fixture) -> None:
    for path in (
        "registry/forbidden.duckdb",
        "copies/run/registry.json",
        "raw/source.json",
        "downloads/source.json",
        "records/hcai_financial_original.json",
        "records/cms_contacts_original.json",
        "records/hcai_financial/x.json",
        "records/cms_contacts/x.json",
        ".env",
        "config/acquisition/source_registry.json",
        "registry/launcher.lock",
        "registry/token.json",
    ):
        refuses(
            partial(
                manifests().build_manifest,
                f.source,
                transfer_id="synthetic_transfer",
                source_machine="source",
                declarations=[{"relative_path": path, "role": "broker_record", "classification": "Confidential"}],
            ),
            "excluded_member",
        )
    (f.source / "records/link.json").symlink_to(f.source / "registry/registry.json")
    refuses(f.execute, "symlink_member")
    require(not f.store.calls, "exclusion reached object store")


def source_membership(f: Fixture) -> None:
    (f.source / "records/extra.json").write_bytes(b"{}")
    refuses(f.execute, "source_membership")


def destination_membership(f: Fixture) -> None:
    f.databases.after_restore = True
    refuses(f.execute, "transfer_interrupted")
    (f.destination / "records/extra.json").write_bytes(b"{}")
    refuses(f.execute, "destination_membership")


def source_digest(f: Fixture) -> None:
    (f.source / "records/completed.json").write_bytes(b"changed bytes")
    refuses(f.execute, "member_digest")


def immutable_conflict(f: Fixture) -> None:
    f.store.seed("controller_state_transfers/synthetic_transfer/files/registry/registry_id", b"different existing bytes")
    refuses(f.execute, "object_conflict")


def lost_upload(f: Fixture) -> None:
    f.store.lost = True
    f.execute()
    require(f.databases.restores == ["airflow", "marquez"], "lost upload did not reconcile")


def pinned_download(f: Fixture) -> None:
    f.store.corrupt = True
    refuses(f.execute, "object_digest")
    require(not f.destination.exists() and not f.databases.restores, "corrupt pinned bytes were imported")


def registry_schema(f: Fixture) -> None:
    state = copy.deepcopy(f.state)
    state["schema_version"] = 999
    (f.source / "registry/registry.json").write_bytes(encoded(state))
    for record in f.manifest["records"]:
        if record["relative_path"] == "registry/registry.json":
            raw = encoded(state)
            record.update(size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    f.privacy["manifest_sha256"] = manifests().digest(f.manifest)
    refuses(f.execute, "registry_schema")


def path_process(f: Fixture) -> None:
    f.options["historical_paths"] = []
    refuses(f.execute, "unrecognized_path")


def active_input(f: Fixture) -> None:
    f.options["active_inputs"] = {}
    refuses(f.execute, "active_input_missing")


def database_scope(f: Fixture) -> None:
    f.specs[0]["database"] = "polaris"
    f.privacy["database_specs_sha256"] = manifests().digest(f.specs)
    refuses(f.execute, "database_scope")
    require(not f.databases.restores, "catalog refusal followed a restore")


def database_conflict(f: Fixture) -> None:
    f.databases.target["airflow"] = {**f.databases.source["airflow"], "tables": {}}
    refuses(f.execute, "database_conflict")
    require(not f.databases.restores, "divergent target was overwritten")


def database_parity(f: Fixture) -> None:
    f.databases.corrupt = True
    refuses(f.execute, "database_parity")
    require(not any(versions for key, versions in f.store.values.items() if "/exports/" in key), "failed transfer retained an export")


def interrupted_resume(f: Fixture) -> None:
    f.databases.after_restore = True
    refuses(f.execute, "transfer_interrupted")
    result = f.execute()
    require(result["import_verified"] and f.databases.restores == ["airflow", "marquez"], "resume repeated a completed restore")


def identity_conflict(f: Fixture) -> None:
    f.execute()
    f.options["fernet_key_version"] = 5
    refuses(f.execute, "transfer_identity_conflict")


def deadline(f: Fixture) -> None:
    limit = time.monotonic() + 0.02
    f.store.deadline_hook = lambda: time.sleep(max(0, limit - time.monotonic()) + 0.002)
    refuses(lambda: f.execute(deadline=limit), "transfer_deadline")
    receipt = json.loads(f.receipt.read_text())
    require(receipt["admission_closed"] and receipt["stage"] and receipt["error_code"] == "transfer_deadline", "timeout lost closed stage evidence")
    require(not f.databases.restores, "timeout started a restore")


def export_cleanup(f: Fixture) -> None:
    key = "controller_state_transfers/synthetic_transfer/exports/airflow.dump"
    f.store.seed(key, b"older export")
    f.store.seed(key, b"", marker=True)
    f.execute()
    require(not f.store.values[key], "cleanup did not delete every export version and marker")
    require(
        all(operation != "delete" or key.startswith("controller_state_transfers/synthetic_transfer/") for operation, key, _ in f.store.calls),
        "cleanup deleted an unrelated key",
    )


def cleanup_resume(f: Fixture) -> None:
    f.store.interrupt = "delete"
    refuses(f.execute, "transfer_interrupted")
    result = f.execute()
    require(result["import_verified"] and f.databases.restores == ["airflow", "marquez"], "cleanup resume restored again")


def privacy_gate(f: Fixture) -> None:
    f.privacy["approved"] = False
    refuses(f.execute, "privacy_gate")
    require(not f.store.calls, "privacy refusal reached object store")


def replace_record(f: Fixture, name: str, raw: bytes, *, role: str | None = None) -> None:
    path = f.source / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    declarations = [{key: record[key] for key in ("relative_path", "role", "classification")} for record in f.manifest["records"]]
    if not any(record["relative_path"] == name for record in declarations):
        require(role is not None, "new case member has no role")
        declarations.append({"relative_path": name, "role": str(role), "classification": "Synthetic"})
    f.manifest = manifests().build_manifest(f.source, transfer_id="synthetic_transfer", source_machine="source_machine_boot_1", declarations=declarations)
    f.privacy["manifest_sha256"] = manifests().digest(f.manifest)


def record_format(f: Fixture) -> None:
    replace_record(f, "ledger/retained.jsonl", b'{"incomplete":true')
    refuses(f.execute, "record_format")
    require(not f.store.calls, "torn ledger reached object store")


def broker_path(f: Fixture) -> None:
    original = encoded({"run_id": "retained", "registry_path": "/source/checkout/registry", "payload_sha256": "e" * 64})
    replace_record(f, "records/completed.json", original)
    f.execute()
    require((f.destination / "records/completed.json").read_bytes() == original, "broker immutable bytes changed")
    mapping = json.loads((f.destination / "controller_import.json").read_bytes())
    require(
        any(row.get("record") == "records/completed.json" and row["destination"] == "/aws/checkout/registry" for row in mapping["resolved_paths"]),
        "broker path has no portable import resolution",
    )


def unknown_broker_path(f: Fixture) -> None:
    replace_record(f, "records/completed.json", encoded({"surprise_location": "/source/checkout/registry"}))
    refuses(f.execute, "unrecognized_path")


def worker_history(f: Fixture) -> None:
    original = encoded({"event": "source_history", "pid": 123, "pgid": 123, "machine": "source_machine_boot_1"})
    replace_record(f, "registry/workers.jsonl", original, role="worker_history")
    f.execute()
    require(not (f.destination / "registry/workers.jsonl").exists(), "source worker ledger remains actionable")
    require((f.destination / "worker_history/source_workers.jsonl").read_bytes() == original, "source process history lost")


def outbox_format(f: Fixture) -> None:
    replace_record(f, "lineage/outbox.sqlite", b"SQLite format 3\x00truncated", role="lineage_outbox")
    refuses(f.execute, "outbox_integrity")


def outbox_snapshot(f: Fixture) -> None:
    from orchestration.lineage import Outbox

    path = f.source / "lineage/outbox.sqlite"
    with Outbox(path) as box:
        box.enqueue_body("synthetic_event", {"synthetic": "exact event bytes"}, 1)
        box.connection.commit()
        box.checkpoint("/source/checkout/records", "old_cursor")
        box.checkpoint("!provider-record-cycle", "7")
        before = box.pending(10)
    replace_record(f, "lineage/outbox.sqlite", path.read_bytes(), role="lineage_outbox")
    f.execute()
    with Outbox(f.destination / "lineage/outbox.sqlite") as imported:
        require(imported.pending(10) == before, "outbox event identity or payload changed")
        require(
            imported.cursor("/source/checkout/records") is None and imported.cursor("/aws/checkout/records") is None, "outbox retained a stale traversal cursor"
        )
        require(imported.record_cycle() == 7, "provider correlation cycle changed")


def destination_digest(f: Fixture) -> None:
    f.databases.after_restore = True
    refuses(f.execute, "transfer_interrupted")
    (f.destination / "records/completed.json").write_bytes(b"changed")
    refuses(f.execute, "destination_digest")


def source_work(f: Fixture) -> None:
    state = copy.deepcopy(f.state)
    state["operations"] = {"open": {"state": "running"}}
    replace_record(f, "registry/registry.json", encoded(state))
    refuses(f.execute, "source_work_active")


def cleanup_scope(f: Fixture) -> None:
    original = f.store.versions

    def wrong_key(key: str, *, token: str | None, deadline: float) -> dict[str, Any]:
        result = original(key, token=token, deadline=deadline)
        if result["versions"]:
            result["versions"][0]["key"] = "controller_state_transfers/another_transfer/exports/airflow.dump"
        return result

    with mock.patch.object(f.store, "versions", side_effect=wrong_key):
        refuses(f.execute, "object_reference")
    require(all(operation != "delete" for operation, _, _ in f.store.calls), "malformed listing caused deletion")


def database_version(f: Fixture) -> None:
    f.specs[0]["restore_major"] = 15
    f.privacy["database_specs_sha256"] = manifests().digest(f.specs)
    refuses(f.execute, "database_version")


def resume_deadline(f: Fixture) -> None:
    limit = time.monotonic() + 0.02
    f.store.deadline_hook = lambda: time.sleep(max(0, limit - time.monotonic()) + 0.002)
    refuses(lambda: f.execute(deadline=limit), "transfer_deadline")
    before = len(f.store.calls)
    f.store.deadline_hook = None
    refuses(f.execute, "transfer_deadline")
    require(len(f.store.calls) == before, "resume extended the original transfer window")


def source_database_changed(f: Fixture) -> None:
    f.databases.source["airflow"] = {**f.databases.source["airflow"], "tables": {}}
    refuses(f.execute, "database_source_changed")
    require(not f.databases.restores, "changed source reached restore")


def owner_prepare(f: Fixture) -> None:
    from orchestration.controller_authority import confirmation_text, dispatch_binding, owner_command

    adapter = subject().Transfer(
        manifest=f.manifest,
        source=f.source,
        destination=f.destination,
        receipt_path=f.receipt,
        store=f.store,
        databases=f.databases,
        database_specs=f.specs,
        import_options=f.options,
        privacy=f.privacy,
    )
    result = adapter.run(deadline=time.monotonic() + 20)
    proof = adapter.proof()
    require(proof["state_digest"] == result["converted_manifest_sha256"], "owner proof names another converted manifest")
    registry = run_lock.Registry(f.destination / "registry", "real")
    with run_lock.transaction(registry) as state:
        refuses(lambda: dispatch_binding(state, owner="aws"), "controller_admission_closed")
        command = {
            "command_id": "prepare_import",
            "action": "prepare",
            "expected_epoch": 3,
            "transfer_id": "synthetic_transfer",
            "state_digest": proof["state_digest"],
            "reason": "synthetic verified import",
            "time": "2026-10-25T00:00:00Z",
        }
        facts = {**proof, **dict.fromkeys(("source_stopped", "ecs_reconciled", "no_pending_writes", "no_source_mutations"), True)}
        prepared = owner_command(
            state, command, intent_directory=f.root / "owner_intents", owner=True, tty=True, confirmation=confirmation_text(command), evidence=facts
        )
        require(
            prepared["controller_owner"] == "none" and prepared["controller_phase"] == "prepared" and prepared["controller_epoch"] == 4,
            "import proof cannot prepare the existing owner command",
        )
        require(state["launcher"]["catalog_authority"] == f.state["launcher"]["catalog_authority"], "owner preparation changed the catalog")


def changed_export_after_cleanup(f: Fixture) -> None:
    f.store.interrupt = "delete"
    refuses(f.execute, "transfer_interrupted")
    key = "controller_state_transfers/synthetic_transfer/exports/airflow.dump"
    f.store.seed(key, b"late owner export version")
    f.execute()
    require(not f.store.values[key] and f.databases.restores == ["airflow", "marquez"], "resume retained a later export version or restored again")


def unknown_array_path(f: Fixture) -> None:
    replace_record(f, "records/completed.json", encoded({"unrecognized_paths": ["/source/checkout/registry"]}))
    refuses(f.execute, "unrecognized_path")


def retained_history(f: Fixture) -> None:
    state = copy.deepcopy(f.state)
    state["domains"] = {"transform": "retained"}
    state["services"] = {"catalog_memory": 64}
    state["attempts"] = {"attempt_1": {"phase": "ended", "reason": "exited", "exit_code": 0}}
    state["operations"] = {"operation_1": {"state": "settled", "result": "finished"}}
    state["allocations"] = {"allocation_1": {"state": "released", "holder_pid": 123, "holder_start": "source_start"}}
    state["containers"] = {"container_1": {"ended": "2026-10-25T00:00:00Z"}}
    state["copies"] = {
        "copy_1": {"phase": "complete", "generation": 5, "folder": "/source/checkout/runs/retained/snapshots/copy_1", "guard": "/source/checkout/packages"}
    }
    state["guards"] = {"/source/checkout/packages": {"readers": {}, "writer": None}}
    state["reservations"] = {"reservation_1": {"allocation": "allocation_1"}}
    replace_record(f, "registry/registry.json", encoded(state))
    f.execute()
    imported = json.loads((f.destination / "registry/registry.json").read_bytes())
    require(imported["reservations"] == state["reservations"], "durable reservation membership changed")
    require(imported["attempts"] == state["attempts"] and imported["operations"] == state["operations"], "terminal history changed")
    require("/aws/checkout/packages" in imported["guards"] and imported["copies"]["copy_1"]["generation"] == 5, "path-key mapping or copy identity lost")


def held_consumer_input(f: Fixture) -> None:
    state = copy.deepcopy(f.state)
    state["runs"]["retained"].update(state="held", snapshot="active_copy")
    replace_record(f, "registry/registry.json", encoded(state))
    refuses(f.execute, "active_input_missing")


def cloud_binding(f: Fixture) -> None:
    f.options["active_inputs"]["snapshot_1"]["generation"] = 6
    refuses(f.execute, "active_input_binding")


def sealed_binding(f: Fixture) -> None:
    state = copy.deepcopy(f.state)
    state["runs"]["retained"].update(state="held", build_state="sealed", completion={"sha256": "f" * 64, "size": 100, "file": "sealed/historical.duckdb"})
    replace_record(f, "registry/registry.json", encoded(state))
    f.options["active_inputs"]["f" * 64] = copy.deepcopy(f.options["active_inputs"]["snapshot_1"])
    refuses(f.execute, "active_input_binding")


def source_token_reference(f: Fixture) -> None:
    state = copy.deepcopy(f.state)
    state["runs"]["retained"]["token_file"] = str(Path("/source/private") / "projection")
    replace_record(f, "registry/registry.json", encoded(state))
    f.execute()
    imported = json.loads((f.destination / "registry/registry.json").read_bytes())
    require(imported["runs"]["retained"]["token_file"] is None, "import retained a live credential projection")
    require(imported["runs"]["retained"]["payload_sha256"] == "c" * 64, "credential conversion changed replay identity")
    metadata = json.loads((f.destination / "controller_import.json").read_bytes())
    require(len(metadata["credential_references"]) == 1, "credential reference history lost")


def database_upgrade(f: Fixture) -> None:
    for spec in f.specs:
        spec.update(dump_major=17, restore_major=17, target_major=17)
    f.databases.target_major = 17
    f.privacy["database_specs_sha256"] = manifests().digest(f.specs)
    f.execute()
    for name in ("airflow", "marquez"):
        target = f.databases.target[name]
        require(target is not None and target["server_major"] == 17, "compatible target upgrade failed")
    require(f.databases.target["polaris"] == {"sentinel": "preserved"}, "upgrade changed catalog state")


def runtime_layout(f: Fixture) -> None:
    members = {
        "records/retained/step.json": ("broker_record", encoded({"run_id": "retained", "outcome": "completed"})),
        "forwards/retained/abcdef.json": ("forward", encoded({"attempt_id": "original_attempt"})),
        "ledger/retained.abcdef.sent": ("dispatch_marker", encoded({"version": 1, "submission_id": "original_submission", "at": "2026-10-25T00:00:00Z"})),
        "registry/history/20261025.jsonl": ("registry_archive", encoded({"collection": "requests", "key": "historical", "record": {"state": "done"}})),
        "registry/controller_commands/drain.json": ("owner_intent", encoded({"command_id": "drain", "transfer_id": "synthetic_transfer"})),
        "registry/launcher.log": ("sanitized_log", b"synthetic stopped launcher\n"),
        "broker/broker.log": ("sanitized_log", b"synthetic stopped broker\n"),
        "airflow/logs/dag_id=gold/run_id=manual__2026-10-25T00:00:00+00:00/task.log": ("sanitized_log", b"synthetic terminal task\n"),
    }
    for name, (role, raw) in members.items():
        replace_record(f, name, raw, role=role)
    f.execute()
    require(all((f.destination / name).read_bytes() == raw for name, (_, raw) in members.items()), "native controller records were omitted or changed")


def interrupted_export(f: Fixture) -> None:
    f.store.interrupt_after_export = True
    f.databases.variable_exports = True
    try:
        f.execute()
    except SimulatedCrash:
        pass
    else:
        raise AssertionError("export did not reach the planned crash boundary")
    result = f.execute()
    require(result["import_verified"] and f.databases.exports == ["airflow", "marquez"], "resume regenerated a committed export")
    require(all(not versions for key, versions in f.store.values.items() if "/exports/" in key), "resumed export versions remain")


def terminal_retry(f: Fixture, expected: dict[str, Any], *, expired_deadline: bool = False) -> None:
    before = f.receipt.read_bytes()
    calls = list(f.store.calls)
    exports, restores = list(f.databases.exports), list(f.databases.restores)
    with mock.patch.object(f.databases, "inventory", side_effect=AssertionError("terminal receipt rechecked a database")):
        require(f.execute(deadline=time.monotonic() + (-1 if expired_deadline else 20)) == expected, "terminal receipt changed on retry")
    require(f.receipt.read_bytes() == before, "terminal receipt bytes changed")
    require(f.store.calls == calls and f.databases.exports == exports and f.databases.restores == restores, "terminal receipt retried external effects")
    adapter = subject().Transfer(
        manifest=f.manifest,
        source=f.source,
        destination=f.destination,
        receipt_path=f.receipt,
        store=f.store,
        databases=f.databases,
        database_specs=f.specs,
        import_options=f.options,
        privacy=f.privacy,
    )
    adapter.run(deadline=time.monotonic() - 1)
    require(adapter.proof()["state_digest"] == expected["converted_manifest_sha256"], "terminal retry erased import proof")


def complete_expired(f: Fixture) -> None:
    result = f.execute()
    result["window_end_utc"] = "2000-01-01T00:00:00+00:00"
    f.receipt.write_bytes(encoded(result))
    terminal_retry(f, result, expired_deadline=True)


def complete_changed_source(f: Fixture) -> None:
    result = f.execute()
    (f.source / "records/completed.json").write_bytes(b"changed after completion")
    (f.destination / "registry/registry.json").write_bytes(b"later owner state")
    terminal_retry(f, result)
    require((f.destination / "registry/registry.json").read_bytes() == b"later owner state", "retry overwrote later owner state")


def path_part_exclusions(f: Fixture) -> None:
    for part in ("raw", "downloads", "runs", "sealed", "postgres", "copies", "HCAI_Financial", "CMS_Contacts"):
        path = f"records/{part}/x.json"
        refuses(partial(manifests().allowed_member, "broker_record", path), "excluded_member")
    require(not f.store.calls, "direct exclusion opened a port")


def null_version(f: Fixture) -> None:
    original = f.store.current

    def unversioned(key: str, *, deadline: float) -> dict[str, Any] | None:
        value = original(key, deadline=deadline)
        return {**value, "version_id": "null"} if value is not None else None

    with mock.patch.object(f.store, "current", side_effect=unversioned):
        refuses(f.execute, "object_reference")
    require(not f.databases.restores, "unversioned reference reached restore")


def driver_error(f: Fixture) -> None:
    with mock.patch.object(f.databases, "export", side_effect=RuntimeError("synthetic driver details")):
        refuses(f.execute, "transfer_port_error")
    receipt = json.loads(f.receipt.read_bytes())
    require(receipt["stage"] == "export:airflow" and receipt["error_code"] == "transfer_port_error", "driver failure lost coded stage")
    require(receipt["admission_closed"] is True and receipt["import_verified"] is False, "driver failure opened admission or verified import")
    require(b"synthetic driver details" not in f.receipt.read_bytes(), "receipt retained driver details")


def driver_restore_error(f: Fixture) -> None:
    original = f.databases.restore

    def failed_restore(name: str, chunks: Iterable[bytes], *, options: tuple[str, ...], deadline: float) -> None:
        original(name, chunks, options=options, deadline=deadline)
        raise RuntimeError("synthetic restore driver details")

    with mock.patch.object(f.databases, "restore", side_effect=failed_restore):
        refuses(f.execute, "transfer_port_error")
    require(f.databases.restores == ["airflow"], "unexpected restore failure did not consume the staged export")
    receipt = json.loads(f.receipt.read_bytes())
    require(receipt["stage"] == "restore:airflow" and receipt["error_code"] == "transfer_port_error", "unexpected restore failure lost coded stage")
    require(receipt["admission_closed"] is True and receipt["import_verified"] is False, "unexpected restore failure changed admission or proof")
    require(b"synthetic restore driver details" not in f.receipt.read_bytes(), "receipt retained restore driver details")
    require(receipt.get("abort_cleanup", {}).get("status") == "complete", "unexpected restore failure omitted abort cleanup")
    require(
        not any(versions for key, versions in f.store.values.items() if "/exports/" in key),
        "unexpected restore failure left staged export versions",
    )


def record_cleanup(f: Fixture) -> None:
    prefix = "controller_state_transfers/synthetic_transfer/"
    names = ["manifest.json", *("files/" + record["relative_path"] for record in f.manifest["records"])]
    foreign = f.store.seed("controller_state_transfers/foreign/manifest.json", b"unrelated transfer")
    for name in names:
        f.store.seed(prefix + name, b"historical record")
        f.store.seed(prefix + name, b"", marker=True)
    original = f.store.delete

    def after_verified(reference: dict[str, Any], *, deadline: float) -> None:
        if "/exports/" not in reference["key"]:
            require(json.loads(f.receipt.read_bytes())["import_verified"] is True, "record deleted before verified import")
        original(reference, deadline=deadline)

    with mock.patch.object(f.store, "delete", side_effect=after_verified):
        result = f.execute()
    require(all(not versions for key, versions in f.store.values.items() if key.startswith(prefix)), "transfer records remain after verified import")
    require(
        all(reference["key"] in f.store.values and f.store.values[reference["key"]] for reference in (foreign, f.options["active_inputs"]["snapshot_1"])),
        "cleanup deleted an unrelated object",
    )
    require(result["record_cleanup"] == "complete", "record cleanup receipt missing")


def record_cleanup_resume(f: Fixture) -> None:
    original = f.store.delete
    deleted: list[str] = []

    def interrupted(reference: dict[str, Any], *, deadline: float) -> None:
        if "/exports/" not in reference["key"]:
            if deleted:
                raise InterruptedError("synthetic record cleanup interruption")
            deleted.append(reference["key"])
        original(reference, deadline=deadline)

    with mock.patch.object(f.store, "delete", side_effect=interrupted):
        refuses(f.execute, "transfer_interrupted")
    receipt = json.loads(f.receipt.read_bytes())
    require(receipt["import_verified"] is True and receipt["stage"].startswith("delete_record:"), "record cleanup lost verified import stage")
    require(receipt["admission_closed"] is True, "record cleanup opened admission")
    before = len(f.store.calls)
    sequence = f.store.sequence
    (f.source / "records/completed.json").write_bytes(b"changed after verification")
    result = f.execute()
    require(result["stage"] == "complete" and result["record_cleanup"] == "complete", "record cleanup did not resume")
    require(f.store.sequence == sequence and f.databases.restores == ["airflow", "marquez"], "record cleanup repeated import")
    require(all(operation in ("versions", "delete") for operation, _, _ in f.store.calls[before:]), "record cleanup reopened import objects")
    require(
        all(not values for key, values in f.store.values.items() if key.startswith("controller_state_transfers/synthetic_transfer/")),
        "record cleanup left versions after resume",
    )


def abort_cleanup(f: Fixture) -> None:
    key = "controller_state_transfers/synthetic_transfer/exports/airflow.dump"
    f.store.seed(key, b"historical export")
    f.store.seed(key, b"", marker=True)
    with mock.patch.object(f.databases, "restore", side_effect=RuntimeError("synthetic failed restore")):
        refuses(f.execute, "transfer_port_error")
    receipt = json.loads(f.receipt.read_bytes())
    require(receipt["abort_cleanup"]["status"] == "complete", "failed transfer omitted abort cleanup")
    require(receipt["stage"] == "restore:airflow" and receipt["error_code"] == "transfer_port_error", "abort cleanup replaced the original failure")
    require(receipt["admission_closed"] is True and receipt["import_verified"] is False, "abort cleanup changed admission or proof")
    require(not any(versions for key, versions in f.store.values.items() if "/exports/" in key), "abort cleanup left staged export versions")
    require(any(operation == "create" and "/exports/" in key for operation, key, _ in f.store.calls), "failed restore had no staged export")
    result = f.execute()
    require(result["import_verified"] and f.databases.restores == ["airflow", "marquez"], "abort cleanup left stale object receipts")


def abort_cleanup_resume(f: Fixture) -> None:
    key = "controller_state_transfers/synthetic_transfer/exports/airflow.dump"
    f.store.seed(key, b"historical export")
    f.store.seed(key, b"", marker=True)
    original = f.store.delete

    def lost_delete(reference: dict[str, Any], *, deadline: float) -> None:
        original(reference, deadline=deadline)
        raise InterruptedError("synthetic delete response loss")

    with (
        mock.patch.object(f.databases, "restore", side_effect=RuntimeError("synthetic failed restore")),
        mock.patch.object(f.store, "delete", side_effect=lost_delete),
    ):
        refuses(f.execute, "transfer_port_error")
    receipt = json.loads(f.receipt.read_bytes())
    require(
        receipt["abort_cleanup"]["status"] == "pending" and receipt["abort_cleanup"]["error_code"] == "transfer_interrupted",
        "abort cleanup failure was not recorded",
    )
    require(receipt["stage"] == "restore:airflow" and receipt["error_code"] == "transfer_port_error", "cleanup failure hid the original failure")
    require(f.execute()["import_verified"] is True, "pending abort cleanup did not resume")
    require(not any(versions for key, versions in f.store.values.items() if "/exports/" in key), "resumed abort cleanup retained exports")


def abort_cleanup_deadline(f: Fixture) -> None:
    now = [time.monotonic()]
    limit = now[0] + 20
    calls: list[int] = []

    def expired_restore(*args: object, **kwargs: object) -> None:
        calls.append(len(f.store.calls))
        now[0] = limit + 1
        raise TimeoutError("synthetic deadline during restore")

    with mock.patch.object(subject().time, "monotonic", side_effect=lambda: now[0]), mock.patch.object(f.databases, "restore", side_effect=expired_restore):
        refuses(partial(f.execute, deadline=limit), "transfer_deadline")
    receipt = json.loads(f.receipt.read_bytes())
    require(
        receipt["abort_cleanup"]["status"] == "pending" and receipt["abort_cleanup"]["error_code"] == "transfer_deadline",
        "expired abort cleanup was not recorded",
    )
    require(
        receipt["stage"] == "restore:airflow" and receipt["error_code"] == "transfer_deadline" and receipt["admission_closed"] is True,
        "expired cleanup lost the original closed stage",
    )
    require(calls == [len(f.store.calls)], "abort cleanup called a port after the shared deadline")


CASES = {
    "success": success,
    "exclusions": exclusions,
    "source_membership": source_membership,
    "destination_membership": destination_membership,
    "source_digest": source_digest,
    "immutable_conflict": immutable_conflict,
    "lost_upload": lost_upload,
    "pinned_download": pinned_download,
    "registry_schema": registry_schema,
    "path_process": path_process,
    "active_input": active_input,
    "database_scope": database_scope,
    "database_conflict": database_conflict,
    "database_parity": database_parity,
    "interrupted_resume": interrupted_resume,
    "identity_conflict": identity_conflict,
    "deadline": deadline,
    "export_cleanup": export_cleanup,
    "cleanup_resume": cleanup_resume,
    "privacy_gate": privacy_gate,
    "record_format": record_format,
    "broker_path": broker_path,
    "unknown_broker_path": unknown_broker_path,
    "worker_history": worker_history,
    "outbox_format": outbox_format,
    "outbox_snapshot": outbox_snapshot,
    "destination_digest": destination_digest,
    "source_work": source_work,
    "cleanup_scope": cleanup_scope,
    "database_version": database_version,
    "resume_deadline": resume_deadline,
    "source_database_changed": source_database_changed,
    "owner_prepare": owner_prepare,
    "changed_export_after_cleanup": changed_export_after_cleanup,
    "unknown_array_path": unknown_array_path,
    "retained_history": retained_history,
    "held_consumer_input": held_consumer_input,
    "cloud_binding": cloud_binding,
    "sealed_binding": sealed_binding,
    "source_token_reference": source_token_reference,
    "database_upgrade": database_upgrade,
    "runtime_layout": runtime_layout,
    "interrupted_export": interrupted_export,
    "complete_expired": complete_expired,
    "complete_changed_source": complete_changed_source,
    "path_part_exclusions": path_part_exclusions,
    "null_version": null_version,
    "driver_error": driver_error,
    "driver_restore_error": driver_restore_error,
    "record_cleanup": record_cleanup,
    "record_cleanup_resume": record_cleanup_resume,
    "abort_cleanup": abort_cleanup,
    "abort_cleanup_resume": abort_cleanup_resume,
    "abort_cleanup_deadline": abort_cleanup_deadline,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=tuple(CASES), action="append")
    args = parser.parse_args(argv)
    args.output.mkdir(parents=True, exist_ok=False)
    before = {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() if (ROOT / path).exists() else None for path in SOURCES}
    results: list[dict[str, Any]] = []
    for name in args.case or CASES:
        started = time.monotonic()
        try:
            with tempfile.TemporaryDirectory(prefix="controller_transfer_") as folder:
                CASES[name](Fixture(Path(folder).resolve()))
            results.append({"case": name, "status": "pass", "seconds": time.monotonic() - started})
        except (AssertionError, ImportError, ValueError, RuntimeError, OSError, KeyError, TypeError) as error:
            results.append({"case": name, "status": "fail", "error": f"{type(error).__name__}: {error}", "seconds": time.monotonic() - started})
    after = {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() if (ROOT / path).exists() else None for path in SOURCES}
    report = {
        "time": datetime.now(UTC).isoformat(),
        "proof": "Synthetic",
        "environment": {"python": sys.version, "platform": sys.platform},
        "limits": ["No real S3, PostgreSQL, IAM, ECS or transfer-task image", "Native ports must enforce blocking deadlines and secure credential delivery"],
        "command": [sys.executable, "-m", "scripts.orchestration.run_controller_transfer_e2e", *sys.argv[1:]],
        "sources": before,
        "sources_unchanged": before == after,
        "substitutions": ["versioned in-memory S3", "logical database streams and independent inventories"],
        "cases": results,
        "cleanup": "done",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if before == after and all(result["status"] == "pass" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
