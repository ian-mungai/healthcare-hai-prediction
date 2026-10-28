"""Stage, verify and resume an allowlisted controller transfer with admission held closed."""

from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import math
import os
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Protocol

from orchestration.controller_import import RESTORE_OPTIONS, convert_snapshot, target_inventory, validate_options
from orchestration.controller_import import database_specs as validate_database_specs
from orchestration.controller_manifest import (
    Document,
    TransferError,
    digest,
    encoded,
    member_path,
    safe_root,
    sha256,
    tree_members,
    validate_manifest,
    verify_tree,
)
from scripts.lakehouse.catalog_journal import durable_directory


class ObjectExists(OSError):
    """Conditional creation found an existing object; exact readback must reconcile it."""


class ObjectStore(Protocol):
    """A TLS transport with SSE-S3, conditional completion and blocking bounds enforced at every method.

    Read yields bytes from the exact version, not the latest key. Create consumes one stream with If-None-Match: *.
    Create must observe iterator exhaustion before publishing; exhaustion persists the exact expected digest first.
    Multipart implementations apply that condition at completion and abort incomplete uploads within the same deadline.
    Versions lists exact-key versions and delete markers with complete pagination. Delete always supplies VersionId.
    """

    def create(self, key: str, chunks: Iterable[bytes], *, if_none_match: str, deadline: float) -> None: ...
    def current(self, key: str, *, deadline: float) -> Document | None: ...
    def read(self, reference: Document, *, deadline: float) -> Iterator[bytes]: ...
    def versions(self, key: str, *, token: str | None, deadline: float) -> Document: ...
    def delete(self, reference: Document, *, deadline: float) -> None: ...


class LogicalDatabases(Protocol):
    """A bounded logical PostgreSQL client; never a cluster-directory copier.

    Inventory independently checks target identity, stopped writers, schema, complete tables and canonical row hashes.
    None means an initialized, verified empty named target. Export streams pg_dump custom format without plaintext files.
    Restore uses the exact database owner, secret delivery in memory and one transaction. It returns only after commit.
    Compatible client binaries, Fernet key version and source/destination quiescence are deployment prerequisites.
    """

    def inventory(self, name: str, *, source: bool, deadline: float) -> Document | None: ...
    def export(self, name: str, *, deadline: float) -> Iterator[bytes]: ...
    def restore(self, name: str, chunks: Iterable[bytes], *, options: tuple[str, ...], deadline: float) -> None: ...


class DigestStream:
    def __init__(self, chunks: Iterable[bytes], check: Callable[[], None], on_exhaust: Callable[[int, str], None] | None = None) -> None:
        self.chunks = iter(chunks)
        self.check = check
        self.hash = hashlib.sha256()
        self.size = 0
        self.on_exhaust = on_exhaust
        self.sealed = False

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        self.check()
        try:
            chunk = next(self.chunks)
        except StopIteration:
            if not self.sealed:
                if self.on_exhaust is not None:
                    self.on_exhaust(self.size, self.hash.hexdigest())
                self.sealed = True
            raise
        if not isinstance(chunk, bytes):
            raise TransferError("object_stream")
        self.hash.update(chunk)
        self.size += len(chunk)
        return chunk

    def finish(self) -> None:
        for _ in self:
            pass

    def close(self) -> None:
        close = getattr(self.chunks, "close", None)
        if callable(close):
            close()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _save_receipt(path: Path, value: Document) -> None:
    temporary = path.with_name(path.name + ".pending")
    if temporary.is_symlink():
        raise TransferError("symlink_member")
    with temporary.open("wb") as handle:
        os.chmod(temporary, 0o600)
        handle.write(encoded(value))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _reference(value: object, key: str) -> Document:
    fields = {"key", "version_id", "size", "sha256", "delete_marker"}
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value["key"] != key
        or not isinstance(value["version_id"], str)
        or not value["version_id"]
        or value["version_id"] == "null"
        or type(value["size"]) is not int
        or value["size"] < 0
        or type(value["delete_marker"]) is not bool
    ):
        raise TransferError("object_reference")
    sha256(value["sha256"])
    return copy.deepcopy(value)


def _error_code(error: Exception) -> str:
    if isinstance(error, TransferError):
        return error.code
    if isinstance(error, InterruptedError):
        return "transfer_interrupted"
    if isinstance(error, TimeoutError):
        return "transfer_deadline"
    if isinstance(error, (OSError, ValueError, TypeError, KeyError)):
        return "transfer_io"
    return "transfer_port_error"


class Transfer:
    """Own the transfer receipt, filesystem publication and database parity before returning import proof.

    The caller supplies the authenticated owner window and bounded ports. This adapter grants no deployment or IAM rights.
    Every invocation holds a transfer lock. Resume preserves the original wall-clock window and the same transfer identity.
    """

    def __init__(
        self,
        *,
        manifest: Mapping[str, Any],
        source: Path,
        destination: Path,
        receipt_path: Path,
        store: ObjectStore,
        databases: LogicalDatabases,
        database_specs: list[Document],
        import_options: Mapping[str, Any],
        privacy: Mapping[str, Any],
    ) -> None:
        self.manifest = validate_manifest(dict(manifest))
        self.specs = validate_database_specs(database_specs)
        self.options = validate_options(dict(import_options))
        self.source, self.destination, self.receipt_path = source, destination, receipt_path
        self.store, self.databases = store, databases
        for root in (source, destination, receipt_path.parent):
            safe_root(root)
        if receipt_path.is_symlink() or source == destination or source in destination.parents or destination in source.parents:
            raise TransferError("import_mapping")
        if {record["source_root"] for record in self.manifest["records"]} != {str(source)}:
            raise TransferError("manifest_root")
        if (
            any(record["size"] > 16 * 1024 * 1024 for record in self.manifest["records"])
            or sum(record["size"] for record in self.manifest["records"]) > 64 * 1024 * 1024
        ):
            raise TransferError("record_buffer_limit")
        expected_privacy = {"approved": True, "manifest_sha256": digest(self.manifest), "database_specs_sha256": digest(self.specs)}
        if dict(privacy) != expected_privacy:
            raise TransferError("privacy_gate")
        self.plan = {
            "version": 1,
            "manifest": self.manifest,
            "database_specs": self.specs,
            "import_options": self.options,
            "destination": str(destination),
            "privacy": expected_privacy,
        }
        self.prefix = f"controller_state_transfers/{self.manifest['transfer_id']}/"
        self.state: Document = {}
        self.deadline = 0.0

    def _check(self) -> None:
        if time.monotonic() >= self.deadline:
            raise TransferError("transfer_deadline")

    def _stage(self, name: str) -> None:
        self.state["stage"] = name
        self.state["error_code"] = None
        _save_receipt(self.receipt_path, self.state)
        self._check()

    def _read(self, reference: Document) -> Iterator[bytes]:
        reference = _reference(reference, reference["key"])
        if reference["delete_marker"]:
            raise TransferError("object_reference")
        stream = DigestStream(self.store.read(reference, deadline=self.deadline), self._check)
        try:
            for chunk in stream:
                if stream.size > reference["size"]:
                    raise TransferError("object_digest")
                yield chunk
            self._check()
            if stream.size != reference["size"] or stream.hash.hexdigest() != reference["sha256"]:
                raise TransferError("object_digest")
        finally:
            stream.close()

    def _pending(self, name: str, size: int, hash_value: str) -> None:
        self._check()
        self.state.setdefault("pending_objects", {})[name] = {"key": self.prefix + name, "size": size, "sha256": hash_value}
        _save_receipt(self.receipt_path, self.state)

    def _immutable(self, name: str, key: str, chunks: Iterable[bytes]) -> Document:
        self._check()
        stream = DigestStream(chunks, self._check, partial(self._pending, name))
        try:
            try:
                self.store.create(key, stream, if_none_match="*", deadline=self.deadline)
            except InterruptedError:
                raise
            except OSError:
                self._check()
            stream.finish()
            self._check()
            observed = self.store.current(key, deadline=self.deadline)
            self._check()
            if observed is None:
                raise TransferError("object_unconfirmed")
            reference = _reference(observed, key)
            if reference["delete_marker"] or reference["size"] != stream.size or reference["sha256"] != stream.hash.hexdigest():
                raise TransferError("object_conflict")
            for _ in self._read(reference):
                pass
            return reference
        finally:
            stream.close()

    def _object(self, name: str, chunks: Callable[[], Iterable[bytes]]) -> Document:
        self._check()
        objects = self.state["objects"]
        key = self.prefix + name
        pending = self.state.get("pending_objects", {})
        if name in objects:
            reference = _reference(objects[name], key)
            observed = self.store.current(key, deadline=self.deadline)
            self._check()
            if observed != reference:
                raise TransferError("object_conflict")
            for _ in self._read(reference):
                pass
            return reference
        if name in pending:
            observed = self.store.current(key, deadline=self.deadline)
            self._check()
            if observed is not None:
                reference = _reference(observed, key)
                expected = pending[name]
                if reference["delete_marker"] or any(reference[field] != expected[field] for field in ("key", "size", "sha256")):
                    raise TransferError("object_conflict")
                for _ in self._read(reference):
                    pass
                objects[name] = reference
                pending.pop(name)
                _save_receipt(self.receipt_path, self.state)
                return reference
        reference = self._immutable(name, key, chunks())
        objects[name] = reference
        self.state.get("pending_objects", {}).pop(name, None)
        _save_receipt(self.receipt_path, self.state)
        return reference

    def _publish(self, files: Mapping[str, bytes]) -> None:
        expected = set(files)

        def verify(root: Path) -> None:
            if tree_members(root) != expected:
                raise TransferError("destination_membership")
            for name, raw in files.items():
                self._check()
                if member_path(root, name).read_bytes() != raw:
                    raise TransferError("destination_digest")

        if self.destination.exists():
            verify(self.destination)
            return
        staging = self.destination.with_name(f".{self.destination.name}.{self.manifest['transfer_id']}.import")
        safe_root(staging)
        durable_directory(staging)
        os.chmod(staging, 0o700)
        if tree_members(staging) - expected:
            raise TransferError("destination_membership")
        for name, raw in sorted(files.items()):
            self._check()
            path = member_path(staging, name)
            durable_directory(path.parent)
            os.chmod(path.parent, 0o700)
            if path.exists():
                if path.read_bytes() != raw:
                    raise TransferError("destination_digest")
            else:
                with path.open("xb") as handle:
                    os.chmod(path, 0o600)
                    handle.write(raw)
                    handle.flush()
                    os.fsync(handle.fileno())
                _fsync_directory(path.parent)
        verify(staging)
        for directory, _, _ in os.walk(staging, topdown=False):
            _fsync_directory(Path(directory))
        self._check()
        os.rename(staging, self.destination)
        _fsync_directory(self.destination.parent)
        verify(self.destination)

    def _versions(self, key: str) -> list[Document]:
        token: str | None = None
        seen_tokens: set[str] = set()
        versions: list[Document] = []
        seen_versions: set[str] = set()
        while True:
            self._check()
            page = self.store.versions(key, token=token, deadline=self.deadline)
            self._check()
            if not isinstance(page, dict) or set(page) != {"versions", "next_token"} or not isinstance(page["versions"], list):
                raise TransferError("export_listing")
            for value in page["versions"]:
                reference = _reference(value, key)
                if reference["version_id"] in seen_versions:
                    raise TransferError("export_listing")
                seen_versions.add(reference["version_id"])
                versions.append(reference)
            token = page["next_token"]
            if token is None:
                return versions
            if not isinstance(token, str) or not token or token in seen_tokens:
                raise TransferError("export_listing")
            seen_tokens.add(token)

    def _delete_object(self, name: str) -> None:
        key = self.prefix + name
        for reference in self._versions(key):
            self._check()
            self.store.delete(reference, deadline=self.deadline)
            self._check()
        if self._versions(key):
            raise TransferError("export_cleanup_unconfirmed")

    def _delete_export(self, name: str) -> None:
        self._stage(f"delete_export:{name}")
        self._delete_object(f"exports/{name}.dump")
        self.state["databases"][name] = "deleted"
        _save_receipt(self.receipt_path, self.state)

    def _abort_exports(self) -> bool:
        cleanup: Document = {"status": "pending", "stage": "start", "error_code": None}
        self.state["abort_cleanup"] = cleanup
        _save_receipt(self.receipt_path, self.state)
        try:
            for spec in self.specs:
                name = f"exports/{spec['database']}.dump"
                if name not in self.state["objects"] and name not in self.state.get("pending_objects", {}):
                    continue
                cleanup["stage"] = f"delete_export:{spec['database']}"
                _save_receipt(self.receipt_path, self.state)
                self._delete_object(name)
                self.state["objects"].pop(name, None)
                self.state.get("pending_objects", {}).pop(name, None)
                _save_receipt(self.receipt_path, self.state)
        except Exception as error:
            cleanup["error_code"] = _error_code(error)
            _save_receipt(self.receipt_path, self.state)
            return False
        cleanup.update(status="complete", stage="complete")
        _save_receipt(self.receipt_path, self.state)
        return True

    def _complete(self) -> Document:
        self.state["record_cleanup"] = "pending"
        _save_receipt(self.receipt_path, self.state)
        for name in ("manifest.json", *("files/" + record["relative_path"] for record in self.manifest["records"])):
            self._stage(f"delete_record:{name}")
            self._delete_object(name)
        self.state.update(record_cleanup="complete", stage="complete", error_code=None)
        _save_receipt(self.receipt_path, self.state)
        return copy.deepcopy(self.state)

    def _database(self, spec: Document) -> None:
        name, expected = spec["database"], target_inventory(spec)
        self._stage(f"database_inventory:{name}")
        target = self.databases.inventory(name, source=False, deadline=self.deadline)
        self._check()
        status = self.state["databases"].get(name)
        if status in ("verified", "deleted"):
            if target != expected:
                raise TransferError("database_parity")
            self._delete_export(name)
            return
        if target is not None and target != expected:
            raise TransferError("database_conflict")
        self._stage(f"export:{name}")
        if self.databases.inventory(name, source=True, deadline=self.deadline) != spec["inventory"]:
            raise TransferError("database_source_changed")
        self._check()
        reference = self._object(f"exports/{name}.dump", partial(self.databases.export, name, deadline=self.deadline))
        if self.databases.inventory(name, source=True, deadline=self.deadline) != spec["inventory"]:
            raise TransferError("database_source_changed")
        self._check()
        self.state.setdefault("exports", {})[name] = {"role": "database_export", "classification": "Restricted", "format": "pg_custom", **reference}
        _save_receipt(self.receipt_path, self.state)
        self._stage(f"restore:{name}")
        if target is None:
            self.state["databases"][name] = "restoring"
            _save_receipt(self.receipt_path, self.state)
            stream = self._read(reference)
            self.databases.restore(name, stream, options=RESTORE_OPTIONS, deadline=self.deadline)
            self._check()
            for _ in stream:
                pass
        observed = self.databases.inventory(name, source=False, deadline=self.deadline)
        self._check()
        if observed != expected:
            raise TransferError("database_parity")
        self.state["databases"][name] = "verified"
        _save_receipt(self.receipt_path, self.state)
        self._delete_export(name)

    def _execute(self) -> Document:
        if self.state.get("abort_cleanup", {}).get("status") == "pending" and not self._abort_exports():
            raise TransferError("export_cleanup_pending")
        if self.state["import_verified"] is True:
            return self._complete()
        self._stage("validate_source")
        verify_tree(self.manifest, self.source, deadline=self.deadline)
        self._check()
        source_files = {record["relative_path"]: member_path(self.source, record["relative_path"]).read_bytes() for record in self.manifest["records"]}
        for record in self.manifest["records"]:
            raw = source_files[record["relative_path"]]
            if (len(raw), hashlib.sha256(raw).hexdigest()) != (record["size"], record["sha256"]):
                raise TransferError("member_digest")
        converted, metadata = convert_snapshot(source_files, self.manifest, self.options, deadline=self.deadline)
        self._stage("verify_active_inputs")
        for value in self.options["active_inputs"].values():
            reference = {key: item for key, item in value.items() if key not in ("approved", "run_id", "generation")}
            for _ in self._read(reference):
                pass
        for spec in self.specs:
            observed = self.databases.inventory(spec["database"], source=False, deadline=self.deadline)
            self._check()
            if observed is not None and observed != target_inventory(spec):
                raise TransferError("database_conflict")
        self._stage("upload_manifest")
        self._object("manifest.json", partial(iter, (encoded(self.manifest),)))
        for record in self.manifest["records"]:
            name = record["relative_path"]
            self._stage(f"upload:{name}")
            reference = self._object("files/" + name, partial(iter, (source_files[name],)))
            if (reference["size"], reference["sha256"]) != (record["size"], record["sha256"]):
                raise TransferError("member_digest")
        self._stage("download")
        downloaded = {
            record["relative_path"]: b"".join(self._read(self.state["objects"]["files/" + record["relative_path"]])) for record in self.manifest["records"]
        }
        if downloaded != source_files:
            raise TransferError("member_digest")
        destination_files, destination_metadata = convert_snapshot(downloaded, self.manifest, self.options, deadline=self.deadline)
        if destination_files != converted or destination_metadata != metadata:
            raise TransferError("registry_parity")
        self._stage("publish_files")
        self._publish(destination_files)
        self.state["converted_manifest_sha256"] = digest(metadata)
        self.state["catalog_sha256"] = metadata["catalog_sha256"]
        _save_receipt(self.receipt_path, self.state)
        for spec in self.specs:
            self._database(spec)
        self._stage("verify_import")
        self._publish(destination_files)
        self.state["import_verified"] = True
        _save_receipt(self.receipt_path, self.state)
        return self._complete()

    def run(self, *, deadline: float) -> Document:
        """Resume one identity within its original window; timeout and every refusal retain a closed receipt."""
        if not math.isfinite(deadline):
            raise TransferError("transfer_deadline")
        self.deadline = deadline
        durable_directory(self.receipt_path.parent)
        lock_path = self.receipt_path.with_name(self.receipt_path.name + ".lock")
        if lock_path.is_symlink():
            raise TransferError("symlink_member")
        with lock_path.open("a") as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    self._check()
                    time.sleep(min(0.01, max(0, deadline - time.monotonic())))
            try:
                if self.receipt_path.exists():
                    self.state = json.loads(self.receipt_path.read_bytes())
                    if (
                        not isinstance(self.state, dict)
                        or self.state.get("version") != 1
                        or self.state.get("plan_sha256") != digest(self.plan)
                        or self.state.get("transfer_id") != self.manifest["transfer_id"]
                        or self.state.get("admission_closed") is not True
                    ):
                        raise TransferError("transfer_identity_conflict")
                    if self.state.get("stage") == "complete" and self.state.get("import_verified") is True:
                        return copy.deepcopy(self.state)
                    window = datetime.fromisoformat(self.state["window_end_utc"])
                    if window.utcoffset() != timedelta(0):
                        raise TransferError("transfer_identity_conflict")
                    self.deadline = min(deadline, time.monotonic() + (window - datetime.now(UTC)).total_seconds())
                else:
                    window = datetime.now(UTC) + timedelta(seconds=max(0, deadline - time.monotonic()))
                    self.state = {
                        "version": 1,
                        "transfer_id": self.manifest["transfer_id"],
                        "plan_sha256": digest(self.plan),
                        "source_manifest_sha256": digest(self.manifest),
                        "window_end_utc": window.isoformat(),
                        "admission_closed": True,
                        "stage": "start",
                        "error_code": None,
                        "objects": {},
                        "pending_objects": {},
                        "databases": {},
                        "import_verified": False,
                    }
                    _save_receipt(self.receipt_path, self.state)
                try:
                    return self._execute()
                except Exception as error:
                    code = _error_code(error)
                    self.state.update(error_code=code, admission_closed=True)
                    _save_receipt(self.receipt_path, self.state)
                    if code != "export_cleanup_pending":
                        self._abort_exports()
                    raise TransferError(code) from None
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def proof(self) -> Document:
        """Return only the verified import facts consumed by the existing owner prepare command."""
        if self.state.get("import_verified") is not True or self.state.get("stage") != "complete":
            raise TransferError("import_unverified")
        return {"import_verified": True, "state_digest": self.state["converted_manifest_sha256"], "catalog_sha256": self.state["catalog_sha256"]}
