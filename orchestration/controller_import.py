"""Convert controller records through a versioned schema map while preserving replay and catalog identity."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import time
from collections.abc import Mapping
from contextlib import closing
from pathlib import PurePosixPath
from typing import Any

from orchestration.catalog_authority import AuthorityError, authority_record
from orchestration.controller_authority import ControllerError, read_authority
from orchestration.controller_backend import local_work_blockers
from orchestration.controller_manifest import Document, TransferError, digest, encoded, identifier, sha256
from scripts.lakehouse import run_lock

PATH_FIELDS = {
    "folder",
    "guard",
    "checkout",
    "registry_root",
    "packages_host",
    "registry_path",
    "runs_root",
    "artifacts_root",
    "code_root",
    "root",
    "path",
    "output_path",
    "log_path",
    "source_path",
    "snapshot_path",
}
PROCESS_FIELDS = {"pid", "pid_start", "holder_pid", "holder_start", "pgid", "process_start", "worker_pid", "worker_start", "process_group", "launcher_pid"}
RESTORE_OPTIONS = ("--no-owner", "--no-privileges", "--single-transaction", "--exit-on-error")


def _absolute(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or "\\" in value
        or PurePosixPath(value).as_posix() != value
        or ".." in PurePosixPath(value).parts
    ):
        raise TransferError("import_mapping")
    return value


def validate_options(value: object) -> Document:
    """Require explicit roots, historical payload pointers, cloud inputs and a pinned Fernet key version."""
    fields = {"version", "path_mappings", "historical_paths", "active_inputs", "fernet_key_version"}
    if not isinstance(value, dict) or set(value) != fields or type(value["version"]) is not int or value["version"] != 1:
        raise TransferError("import_mapping")
    if type(value["fernet_key_version"]) is not int or value["fernet_key_version"] < 1:
        raise TransferError("import_mapping")
    if not isinstance(value["path_mappings"], list) or not isinstance(value["historical_paths"], list) or not isinstance(value["active_inputs"], dict):
        raise TransferError("import_mapping")
    sources: set[str] = set()
    for mapping in value["path_mappings"]:
        if not isinstance(mapping, dict) or set(mapping) != {"source", "destination"}:
            raise TransferError("import_mapping")
        source = _absolute(mapping["source"])
        destination = _absolute(mapping["destination"])
        if source == "/" or destination == "/" or source in sources:
            raise TransferError("import_mapping")
        sources.add(source)
    for pointer in value["historical_paths"]:
        if not isinstance(pointer, str) or not pointer.startswith("/requests/"):
            raise TransferError("import_mapping")
    for name, reference in value["active_inputs"].items():
        identifier(name)
        if not isinstance(reference, dict) or set(reference) != {"key", "version_id", "size", "sha256", "delete_marker", "approved", "run_id", "generation"}:
            raise TransferError("active_input_missing")
        if (
            reference["approved"] is not True
            or reference["delete_marker"] is not False
            or not isinstance(reference["key"], str)
            or not reference["key"].startswith("lakehouse/run_stage/")
            or ".." in PurePosixPath(reference["key"]).parts
            or not isinstance(reference["version_id"], str)
            or not reference["version_id"]
            or reference["version_id"] == "null"
            or type(reference["size"]) is not int
            or reference["size"] < 1
        ):
            raise TransferError("active_input_missing")
        sha256(reference["sha256"])
        identifier(reference["run_id"])
        if type(reference["generation"]) is not int or reference["generation"] < 1:
            raise TransferError("active_input_binding")
    return copy.deepcopy(value)


def resolve_import_path(path: str, mappings: list[Document]) -> str:
    """Resolve one recognized absolute path using the longest explicit source root."""
    _absolute(path)
    matching = [mapping for mapping in mappings if path == mapping["source"] or path.startswith(mapping["source"] + "/")]
    if not matching:
        raise TransferError("unrecognized_path")
    mapping = max(matching, key=lambda item: len(item["source"]))
    return str(mapping["destination"]) + path[len(mapping["source"]) :]


def database_specs(value: object) -> list[Document]:
    """Permit only database-specific Airflow and Marquez logical imports with independent parity inventories."""
    if not isinstance(value, list):
        raise TransferError("database_scope")
    names: set[str] = set()
    for spec in value:
        if not isinstance(spec, dict) or set(spec) != {"database", "dump_major", "restore_major", "target_major", "inventory"}:
            raise TransferError("database_scope")
        name = spec["database"]
        if not isinstance(name, str) or name not in ("airflow", "marquez") or name in names:
            raise TransferError("database_scope")
        names.add(name)
        inventory = spec["inventory"]
        if not isinstance(inventory, dict) or set(inventory) != {"database", "server_major", "schema_sha256", "tables"} or inventory["database"] != name:
            raise TransferError("database_inventory")
        if (
            type(spec["dump_major"]) is not int
            or type(spec["restore_major"]) is not int
            or type(spec["target_major"]) is not int
            or type(inventory["server_major"]) is not int
            or not 12 <= inventory["server_major"] <= spec["dump_major"] == spec["restore_major"] <= spec["target_major"]
            or not isinstance(inventory["tables"], dict)
            or not inventory["tables"]
        ):
            raise TransferError("database_version")
        sha256(inventory["schema_sha256"])
        for table, parity in inventory["tables"].items():
            identifier(table)
            if not isinstance(parity, dict) or set(parity) != {"rows", "sha256"} or type(parity["rows"]) is not int or parity["rows"] < 0:
                raise TransferError("database_inventory")
            sha256(parity["sha256"])
    if names != {"airflow", "marquez"}:
        raise TransferError("database_scope")
    return copy.deepcopy(value)


def target_inventory(spec: Mapping[str, Any]) -> Document:
    """Compare logical data unchanged while checking the explicitly pinned target server version."""
    return {**copy.deepcopy(spec["inventory"]), "server_major": spec["target_major"]}


def _registry(raw: bytes, identity_raw: bytes, transfer_id: str) -> Document:
    try:
        identity = json.loads(identity_raw)
        if not isinstance(identity, dict) or set(identity) != {"registry_id", "kind"} or identity["kind"] not in ("real", "test"):
            raise ValueError("identity")
        identifier(identity["registry_id"])
        state = run_lock.parse_state(raw, identity["registry_id"])
        expected = {"schema_version", "registry_id", *run_lock.SECTIONS, *run_lock.LAUNCHER_SECTIONS, "reservation_history", "launcher"}
        if set(state) != expected or state["schema_version"] != run_lock.LAUNCHER_SCHEMA:
            raise ValueError("schema")
        catalog = authority_record(state["launcher"].get("catalog_authority"))
        controller = read_authority(state)
        if catalog["state"] != "aws_writer" or controller["controller_phase"] != "draining" or controller["controller_owner"] != "mac":
            raise ValueError("authority")
        if controller["controller_transfer_id"] != transfer_id:
            raise ValueError("transfer")
        if any(not isinstance(run_id, str) for run_id in state["domains"].values()):
            raise ValueError("domain shape")
        if any(type(limit) is not int or limit < 0 for limit in state["services"].values()):
            raise ValueError("service shape")
        for section in ("runs", "readers", "reservations", *run_lock.LAUNCHER_SECTIONS):
            if any(not isinstance(row, dict) for row in state[section].values()):
                raise ValueError("record shape")
        if local_work_blockers(state):
            raise TransferError("source_work_active")
        closed = {
            "allocations": ("state", {"released", "failed"}),
            "attempts": ("phase", {"ended"}),
            "operations": ("state", {"settled"}),
            "copies": ("phase", {"complete", "abandoned"}),
        }
        for section, (field, states) in closed.items():
            if any(row.get(field) not in states for row in state[section].values()):
                raise TransferError("source_work_active")
        return state
    except (ValueError, TypeError, KeyError, run_lock.RegistryError, AuthorityError, ControllerError):
        raise TransferError("registry_schema") from None


def _json_object(raw: bytes) -> Document:
    def unique(pairs: list[tuple[str, Any]]) -> Document:
        result: Document = {}
        for name, value in pairs:
            if name in result:
                raise TransferError("record_format")
            result[name] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeError):
        raise TransferError("record_format") from None
    if not isinstance(value, dict):
        raise TransferError("record_format")
    return value


def _outbox(raw: bytes, options: Document, deadline: float) -> tuple[bytes, Document]:
    queries = {
        "events": "select id, payload, sequence, acked from events order by id",
        "provider_runs": "select caller, link from provider_runs order by caller",
        "provider_events": "select id, envelope, seen_cycle, flushed from provider_events order by id",
    }
    try:
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.deserialize(raw)
            connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            if connection.execute("pragma integrity_check").fetchall() != [("ok",)]:
                raise TransferError("outbox_integrity")
            tables = {row[0] for row in connection.execute("select name from sqlite_master where type='table'")}
            if tables != {*queries, "sweep_cursors"}:
                raise TransferError("outbox_schema")
            before = {name: connection.execute(query).fetchall() for name, query in queries.items()}
            cursor_changes = []
            for root, cursor in connection.execute("select root, cursor from sweep_cursors order by root").fetchall():
                if not isinstance(root, str):
                    raise TransferError("outbox_schema")
                if not root.startswith("/"):
                    if root != "!provider-record-cycle":
                        raise TransferError("outbox_schema")
                    continue
                destination = resolve_import_path(root, options["path_mappings"])
                connection.execute("delete from sweep_cursors where root = ?", (root,))
                cursor_changes.append({"source": root, "destination": destination, "prior_cursor_sha256": digest(cursor), "rebuild": True})
            connection.commit()
            after = {name: connection.execute(query).fetchall() for name, query in queries.items()}
            if before != after:
                raise TransferError("outbox_parity")
            if time.monotonic() >= deadline:
                raise TransferError("transfer_deadline")
            return connection.serialize(), {
                "tables": {name: {"rows": len(rows), "sha256": digest(rows)} for name, rows in before.items()},
                "cursor_changes": cursor_changes,
            }
    except sqlite3.Error:
        if time.monotonic() >= deadline:
            raise TransferError("transfer_deadline") from None
        raise TransferError("outbox_integrity") from None


def convert_snapshot(
    files: Mapping[str, bytes], manifest: Mapping[str, Any], options: Mapping[str, Any], *, deadline: float | None = None
) -> tuple[dict[str, bytes], Document]:
    """Preserve immutable request bodies; translate recognized mutable paths and archive source process handles."""
    checked = validate_options(dict(options))
    deadline = time.monotonic() + 30 if deadline is None else deadline
    for name, raw in files.items():
        if name.endswith((".json", ".sent")) or name == "registry/registry_id":
            _json_object(raw)
        elif name.endswith(".jsonl"):
            if raw and not raw.endswith(b"\n"):
                raise TransferError("record_format")
            for line in raw.splitlines():
                _json_object(line)
    state = _registry(files["registry/registry.json"], files["registry/registry_id"], str(manifest["transfer_id"]))
    needed: dict[str, tuple[str, int, Document | None]] = {}
    for row in state["readers"].values():
        reference = row.get("snapshot_set") or row.get("publish_run")
        run_id = row.get("publish_run")
        run = state["runs"].get(run_id, {})
        if not isinstance(reference, str) or not reference or not isinstance(run_id, str) or type(run.get("generation")) is not int:
            raise TransferError("active_input_missing")
        needed[reference] = (run_id, run["generation"], None)
    for run_id, run in state["runs"].items():
        if run.get("state") != "held":
            continue
        generation = run.get("generation")
        if type(generation) is not int or generation < 1:
            raise TransferError("registry_schema")
        if run.get("snapshot"):
            needed[str(run["snapshot"])] = (run_id, generation, None)
        if run.get("build_state") == "sealed":
            completion = run.get("completion")
            if not isinstance(completion, dict):
                raise TransferError("active_input_missing")
            key = sha256(completion.get("sha256"))
            needed[key] = (run_id, generation, completion)
    if needed.keys() - checked["active_inputs"].keys():
        raise TransferError("active_input_missing")
    for name, (run_id, generation, completion) in needed.items():
        reference = checked["active_inputs"][name]
        if reference["run_id"] != run_id or reference["generation"] != generation:
            raise TransferError("active_input_binding")
        if completion is not None and (reference["sha256"], reference["size"]) != (completion.get("sha256"), completion.get("size")):
            raise TransferError("active_input_binding")
    historical_processes: list[Document] = []
    credential_references: list[Document] = []
    resolved_paths: list[Document] = []
    seen_historical: set[str] = set()
    current_record = "registry/registry.json"

    def convert(value: Any, pointer: str, *, immutable: bool = False) -> Any:
        if isinstance(value, str) and value.startswith("/"):
            raise TransferError("unrecognized_path")
        if isinstance(value, list):
            return [convert(item, f"{pointer}/{index}", immutable=immutable) for index, item in enumerate(value)]
        if not isinstance(value, dict):
            return value
        result: Document = {}
        for key, item in value.items():
            original_key = key
            if isinstance(key, str) and key.startswith("/"):
                if pointer != "/guards" or immutable:
                    raise TransferError("unrecognized_path")
                key = resolve_import_path(key, checked["path_mappings"])
                resolved_paths.append({"record": current_record, "pointer": pointer, "source": original_key, "destination": key, "historical": False})
            escaped = str(original_key).replace("~", "~0").replace("/", "~1")
            at = f"{pointer}/{escaped}"
            preserved = immutable or at == "/requests"
            if key == "token_file" and item is not None and not preserved:
                _absolute(item)
                credential_references.append(
                    {"record": current_record, "pointer": at, "source_path": item, "source_machine": manifest["source_machine"], "historical_only": True}
                )
                result[key] = None
                continue
            if key in PROCESS_FIELDS and not preserved:
                historical_processes.append(
                    {"record": current_record, "pointer": at, "value": item, "source_machine": manifest["source_machine"], "historical_only": True}
                )
                continue
            if isinstance(item, str) and item.startswith("/"):
                if key not in PATH_FIELDS:
                    raise TransferError("unrecognized_path")
                resolved = resolve_import_path(item, checked["path_mappings"])
                if preserved and current_record == "registry/registry.json":
                    if at not in checked["historical_paths"]:
                        raise TransferError("unrecognized_path")
                    seen_historical.add(at)
                resolved_paths.append({"record": current_record, "pointer": at, "source": item, "destination": resolved, "historical": preserved})
                result[key] = item if preserved else resolved
            else:
                result[key] = convert(item, at, immutable=preserved)
        return result

    converted = convert(state, "")
    if seen_historical != set(checked["historical_paths"]):
        raise TransferError("import_mapping")
    if converted["requests"] != state["requests"] or converted["launcher"]["catalog_authority"] != state["launcher"]["catalog_authority"]:
        raise TransferError("registry_parity")
    output = dict(files)
    output["registry/registry.json"] = encoded(converted)
    for name, raw in files.items():
        if name.startswith("registry/") and name != "registry/workers.jsonl":
            continue
        current_record = name
        if name.endswith((".json", ".sent")):
            convert(_json_object(raw), "", immutable=True)
        elif name.endswith(".jsonl"):
            for index, line in enumerate(raw.splitlines()):
                convert(_json_object(line), f"/{index}", immutable=True)
    if "registry/workers.jsonl" in output:
        if "worker_history/source_workers.jsonl" in output:
            raise TransferError("import_mapping")
        output["worker_history/source_workers.jsonl"] = output.pop("registry/workers.jsonl")
    outbox = None
    if "lineage/outbox.sqlite" in output:
        output["lineage/outbox.sqlite"], outbox = _outbox(output["lineage/outbox.sqlite"], checked, deadline)
    roles = {row["relative_path"]: row for row in manifest["records"]}
    if "registry/workers.jsonl" in roles:
        roles["worker_history/source_workers.jsonl"] = roles.pop("registry/workers.jsonl")
    metadata = {
        "version": 1,
        "conversion_version": 1,
        "transfer_id": manifest["transfer_id"],
        "registry_id": state["registry_id"],
        "source_machine": manifest["source_machine"],
        "source_manifest_sha256": digest(manifest),
        "catalog_sha256": digest(state["launcher"]["catalog_authority"]),
        "import_options": checked,
        "resolved_paths": resolved_paths,
        "historical_processes": historical_processes,
        "credential_references": credential_references,
        "outbox": outbox,
        "records": [
            {
                "relative_path": name,
                "role": roles[name]["role"],
                "classification": roles[name]["classification"],
                "source_root": roles[name]["source_root"],
                "size": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
            for name, raw in sorted(output.items())
        ],
    }
    output["controller_import.json"] = encoded(metadata)
    return output, metadata
