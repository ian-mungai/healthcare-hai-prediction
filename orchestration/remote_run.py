"""Materialize verified live-try records with durable gates and forward crash recovery."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

from orchestration.ecs_stage import validate_result
from orchestration.pipeline_protocol import (
    MATERIALIZATION_GATE,
    MAX_RECORD,
    PipelineError,
    fixed_layer,
    output_record,
    read_record,
    require_materialized,
    validation_evidence,
)
from scripts.lakehouse import run_lock
from services.hai.bootstrap.integrity import StageError, canonical, check_deadline, digest_descriptor, held_directory, held_file, parse_json, safe_path
from services.hai.bootstrap.outputs import Declaration, Output
from services.hai.bootstrap.redaction import scan_file
from services.hai.bootstrap.transfer import FileRef, StorageError, Transfer

STREAMS = ("stdout.txt", "stderr.txt")
JOURNAL_LIMIT = 3 * MAX_RECORD + 1024


def updated_result_map(current: Mapping[str, Any], returned: Mapping[str, Any], template: str) -> dict[str, Any]:
    """Merge verified references, replacing the shared export scope for an export result.

    The caller verifies the complete declared result first and persists this map
    under the registry lock. Both remote collection and staged local fallback
    use this operation when their verified result supersedes the current export.
    """
    kept = {path: value for path, value in current.items() if template != "pipeline_export" or not path.startswith("publish/export/")}
    return {**kept, **returned}


class RemoteRunStore:
    """Apply one stopped live result under the launcher's existing registry lock.

    The caller runs this operation outside an existing registry transaction. It
    supplies a worker-backed Transfer and one absolute deadline. Terminal attempt
    callbacks and consumers run only after this operation returns. ``recover``
    must finish pending work before another result for this run is dispatched.
    """

    def __init__(self, folder: Path, registry: run_lock.Registry, run_id: str, transfer: Transfer, deadline: float) -> None:
        self.folder = folder
        self.registry = registry
        self.run_id = run_id
        self.transfer = transfer
        self.deadline = deadline
        if folder.name != run_id:
            raise StageError("result_identity", 71)

    @contextmanager
    def _transaction(self, write: bool = False) -> Iterator[dict[str, Any]]:
        check_deadline(self.deadline)
        with (self.registry.path / "registry.lock").open("a") as lock:
            while True:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= self.deadline:
                        raise StageError("registry_deadline", 72) from None
                    time.sleep(min(0.05, max(0, self.deadline - time.monotonic())))
            try:
                check_deadline(self.deadline)
                identity = run_lock.read_identity(self.registry)
                state = run_lock.parse_state(self.registry.state_file.read_bytes(), identity["registry_id"])
                original = json.dumps(state, indent=2, sort_keys=True) + "\n"
                yield state
                check_deadline(self.deadline)
                after = json.dumps(state, indent=2, sort_keys=True) + "\n"
                if write and after != original:
                    run_lock.write_atomic(self.registry.state_file, after)
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _identity(self, state: Mapping[str, Any], job: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        run = state["runs"].get(self.run_id)
        attempt = state["attempts"].get(job.get("attempt_id"))
        if (
            not isinstance(run, dict)
            or not isinstance(attempt, dict)
            or job.get("run_id") != self.run_id
            or run.get("generation", 1) != job.get("generation")
            or run.get("state") != "held"
            or attempt.get("owner") != "run:" + self.run_id
            or attempt.get("generation") != job.get("generation")
            or attempt.get("template") != job.get("template")
            or attempt.get("cancel_intent")
        ):
            raise StageError("result_identity", 71)
        execution = attempt.get("execution", {})
        matching = [item for item in execution.get("placements", []) if item.get("try_id") == job.get("try_id")]
        if execution.get("live_try") != job.get("try_id") or len(matching) != 1:
            raise StageError("result_identity", 71)
        placement = matching[0]
        if placement.get("target") != "ecs" or placement.get("state") != "complete" or not placement.get("task_arn"):
            raise StageError("result_identity", 71)
        return run, attempt, placement

    def _head(self, reference: FileRef) -> None:
        try:
            self.transfer.verify_reference(reference, self.deadline)
        except StorageError:
            raise StageError("remote_object", 71) from None

    def _sync(self, folder: Path) -> None:
        with held_directory(folder) as descriptor:
            check_deadline(self.deadline)
            os.fsync(descriptor)

    def _mkdir(self, folder: Path) -> None:
        missing = []
        parent = folder
        while not parent.exists():
            missing.append(parent)
            parent = parent.parent
        for path in reversed(missing):
            check_deadline(self.deadline)
            path.mkdir(mode=0o700)
            self._sync(path.parent)
        with held_directory(folder):
            check_deadline(self.deadline)

    def _write(self, destination: Path, raw: bytes) -> None:
        self._mkdir(destination.parent)
        temporary = ".runstore_" + destination.name + ".partial"
        with held_directory(destination.parent) as parent:
            handle = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600, dir_fd=parent)
            try:
                with os.fdopen(handle, "wb") as writer:
                    writer.write(raw)
                    writer.flush()
                    check_deadline(self.deadline)
                    os.fsync(writer.fileno())
                os.replace(temporary, destination.name, src_dir_fd=parent, dst_dir_fd=parent)
                os.fsync(parent)
            finally:
                with suppress(FileNotFoundError):
                    os.unlink(temporary, dir_fd=parent)

    def _raw(self, path: Path, maximum: int = MAX_RECORD) -> bytes:
        with held_file(path.parent, path.name) as descriptor:
            _, size = digest_descriptor(descriptor, self.deadline)
            if size > maximum:
                raise StageError("record_bound")
            return os.pread(descriptor, size, 0)

    def _pending(self) -> dict[str, Any] | None:
        path = self.folder / MATERIALIZATION_GATE
        if not path.exists() and not path.is_symlink():
            return None
        value = parse_json(self._raw(path, JOURNAL_LIMIT))
        if not isinstance(value, dict) or set(value) != {"schema", "identity", "job", "marker", "declaration", "job_sha256"} or value.get("schema") != 1:
            raise StageError("materialization_journal")
        return value

    @staticmethod
    def _logical(job: Mapping[str, Any], name: str) -> tuple[str, str]:
        if name in STREAMS:
            return "attempt", name
        if name == "result.json" and job["template"] in ("pipeline_validate_silver", "pipeline_validate_gold", "pipeline_validate_cross"):
            return "run", "quality_" + str(job["template"]).removeprefix("pipeline_validate_") + "/result.json"
        return "run", name

    def _prepare(
        self,
        job: Mapping[str, Any],
        marker: Mapping[str, Any],
        references: Sequence[FileRef],
        declaration: Declaration,
        spool: Path,
        secret_values: Mapping[str, str],
    ) -> list[dict[str, Any]]:
        specs = {output.name: output for output in (*declaration.required, *declaration.optional)}
        entries: list[dict[str, Any]] = []
        for reference in references:
            self._head(reference)
        for reference in references:
            if reference.path not in STREAMS and specs[reference.path].kind == "data":
                continue
            if reference.size > MAX_RECORD:
                raise StageError("record_bound")
            scope, logical = self._logical(job, reference.path)
            path = spool / scope / logical
            self._mkdir(path.parent)
            if path.exists() or path.is_symlink():
                with held_file(path.parent, path.name) as descriptor:
                    digest, size = digest_descriptor(descriptor, self.deadline)
                if (digest, size) != (reference.sha256, reference.size):
                    path.unlink()
            if not path.exists():
                self.transfer.download(reference, path, self.deadline)
            scan_file(path, secret_values, self.deadline)
            entries.append({"scope": scope, "path": logical, "sha256": reference.sha256, "size": reference.size})
        stdout = self._raw(spool / "attempt/stdout.txt")
        stderr = self._raw(spool / "attempt/stderr.txt")
        tail = b"".join((stdout + stderr).splitlines(keepends=True)[-200:])[-MAX_RECORD:]
        for name, raw in (("log_tail.txt", tail), ("exit.json", canonical({"exit_code": marker["exit_code"]}) + b"\n")):
            path = spool / "attempt" / name
            self._write(path, raw)
            scan_file(path, secret_values, self.deadline)
            entries.append({"scope": "attempt", "path": name, "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)})
        return entries

    def _destination(self, entry: Mapping[str, Any], logs: Path) -> Path:
        safe_path(entry["path"])
        return (self.folder if entry["scope"] == "run" else logs) / str(entry["path"])

    def _verify_entries(self, entries: Sequence[Mapping[str, Any]], logs: Path) -> None:
        for entry in entries:
            path = self._destination(entry, logs)
            try:
                with held_file(path.parent, path.name) as descriptor:
                    digest, size = digest_descriptor(descriptor, self.deadline)
            except StageError as error:
                if error.code == "stage_deadline":
                    raise
                raise StageError("materialized_record", 71) from None
            if (digest, size) != (entry["sha256"], entry["size"]):
                raise StageError("materialized_record", 71)

    def _publish_entry(self, entry: Mapping[str, Any], spool: Path, logs: Path) -> None:
        raw = self._raw(spool / entry["scope"] / entry["path"])
        self._write(self._destination(entry, logs), raw)

    @staticmethod
    def _references(job: Mapping[str, Any], references: Sequence[FileRef]) -> dict[str, dict[str, Any]]:
        result = {}
        for reference in references:
            scope, path = RemoteRunStore._logical(job, reference.path)
            if scope == "run":
                result[path] = {
                    "key": reference.key,
                    "version_id": reference.version_id,
                    "sha256": reference.sha256,
                    "size": reference.size,
                    "try_id": job["try_id"],
                }
        return result

    def _unlink_gate(self, root: Path, identity: str) -> None:
        path = root / MATERIALIZATION_GATE
        if not path.exists() and not path.is_symlink():
            return
        value = parse_json(self._raw(path, JOURNAL_LIMIT))
        if not isinstance(value, dict) or value.get("identity") != identity:
            raise StageError("materialization_busy")
        path.unlink()
        self._sync(root)

    def materialize(
        self, job: Mapping[str, Any], marker: Mapping[str, Any], declaration: Declaration, job_sha256: str, secret_values: Mapping[str, str]
    ) -> dict[str, Any]:
        """Verify the complete result, publish its records and persist applied state.

        Required data objects remain immutable references even when small. Records
        are never downgraded to remote data to evade their 32 MiB return limit.
        A failure after the journal is published leaves reads gated for recovery.
        """
        check_deadline(self.deadline)
        job_raw = canonical(job)
        if len(job_raw) > MAX_RECORD or len(canonical(marker)) > MAX_RECORD:
            raise StageError("record_bound")
        if hashlib.sha256(job_raw).hexdigest() != job_sha256:
            raise StageError("job_digest", 71)
        if job.get("expected_outputs") != declaration.document():
            raise StageError("result_declaration", 71)
        with self._transaction() as state:
            _, attempt, placement = self._identity(state, job)
            references = validate_result(job, marker, declaration, str(job["try_id"]), str(placement["task_arn"]), job_sha256)
            logs = Path(str(attempt["log_tail"])).parent
        journal = {"schema": 1, "job": dict(job), "marker": dict(marker), "declaration": declaration.document(), "job_sha256": job_sha256}
        identity = hashlib.sha256(canonical(journal)).hexdigest()
        journal["identity"] = identity
        pending = self._pending()
        if pending is not None and pending != journal:
            raise StageError("materialization_busy")
        spool = self.folder / (".runstore_stage_" + identity)
        self._mkdir(spool)
        try:
            entries = self._prepare(job, marker, references, declaration, spool, secret_values)
        except BaseException:
            shutil.rmtree(spool)
            raise
        applied = {"try_id": job["try_id"], "identity": identity}
        mapped = self._references(job, references)
        with self._transaction(write=True) as state:
            run, attempt, _ = self._identity(state, job)
            previous = attempt.get("ecs", {}).get("applied")
            if previous is not None and previous != applied:
                raise StageError("result_identity", 71)
            stage = run.setdefault("stage", {"generation": job["generation"], "objects": {}})
            if stage.get("generation") != job["generation"]:
                raise StageError("result_identity", 71)
            if previous == applied:
                self._verify_entries(entries, logs)
                if stage["objects"] != updated_result_map(stage["objects"], mapped, str(job["template"])):
                    raise StageError("materialized_record", 71)
            else:
                current = self._pending()
                if current is not None and current != journal:
                    raise StageError("materialization_busy")
                self._write(self.folder / MATERIALIZATION_GATE, canonical(journal))
                self._write(logs / MATERIALIZATION_GATE, canonical({"identity": identity}))
                for entry in entries:
                    self._publish_entry(entry, spool, logs)
                self._verify_entries(entries, logs)
                stage["objects"] = updated_result_map(stage["objects"], mapped, str(job["template"]))
                attempt.setdefault("ecs", {})["applied"] = applied
        with self._transaction() as state:
            _, attempt, _ = self._identity(state, job)
            if attempt.get("ecs", {}).get("applied") != applied:
                raise StageError("materialization_uncommitted")
            self._verify_entries(entries, logs)
            shutil.rmtree(spool)
            self._sync(self.folder)
            self._unlink_gate(logs, identity)
            self._unlink_gate(self.folder, identity)
        return applied

    def _discard_spools(self) -> None:
        for spool in sorted(self.folder.glob(".runstore_stage_*")):
            check_deadline(self.deadline)
            if re.fullmatch(r"\.runstore_stage_[a-f0-9]{64}", spool.name) and not spool.is_symlink() and spool.is_dir():
                shutil.rmtree(spool)
                self._sync(self.folder)

    def recover(self, secret_values: Mapping[str, str]) -> dict[str, Any] | None:
        """Recover pinned results or discard orphan spools while this run's collection is quiescent."""
        check_deadline(self.deadline)
        with self._transaction():
            pending = self._pending()
            if pending is None:
                self._discard_spools()
                return None
        declaration = pending["declaration"]
        if not isinstance(declaration, dict) or set(declaration) != {"required", "optional", "scratch", "scratch_gib"}:
            raise StageError("materialization_journal")
        contract = Declaration(
            tuple(Output(**item) for item in declaration["required"]),
            tuple(Output(**item) for item in declaration["optional"]),
            tuple(declaration["scratch"]),
            declaration["scratch_gib"],
        )
        return self.materialize(pending["job"], pending["marker"], contract, pending["job_sha256"], secret_values)

    def seal(self, layer: str, expected: str) -> dict[str, str]:
        """Accept a materialized seal task without writing layer completion authority.

        The launcher dispatches and materializes the dedicated task first. This
        method then binds its independent receipt, latest validation and exact
        source version under the registry lock. Missing evidence needs dispatch.
        """
        from scripts.orchestration.pipeline_seal import evidence

        fixed_layer(layer)
        try:
            with self._transaction(write=True) as state:
                require_materialized(self.folder)
                run = state["runs"][self.run_id]
                stage = run.get("stage", {})
                mapped = stage.get("objects", {})
                returned = mapped.get("seal.json")
                if not isinstance(returned, dict):
                    raise PipelineError("remote_seal_unavailable")
                record, digest = read_record(self.folder, "seal.json")
                source = mapped.get("staging.duckdb", {})
                validation_id = record.get("validation_attempt")
                validation = validation_evidence(state, self.run_id, layer, str(validation_id), self.folder)
                reference, wanted = evidence(
                    {
                        "run_id": self.run_id,
                        "generation": run.get("generation", 1),
                        "layer": layer,
                        "validation_attempt": validation_id,
                        "sha256": expected,
                        "source": {"path": "staging.duckdb", **{key: source.get(key) for key in ("key", "version_id", "sha256", "size")}},
                    }
                )
                if record != wanted or validation["sha256"] != expected or stage.get("generation") != run.get("generation", 1):
                    raise StageError("seal_binding", 71)
                candidates = [
                    attempt
                    for attempt in state["attempts"].values()
                    if attempt.get("owner") == "run:" + self.run_id
                    and attempt.get("template") == "pipeline_seal"
                    and attempt.get("ecs", {}).get("applied", {}).get("try_id") == returned.get("try_id")
                ]
                if len(candidates) != 1:
                    raise StageError("seal_binding", 71)
                attempt = candidates[0]
                execution = attempt.get("execution", {})
                placements = execution.get("placements", [])
                if (
                    run.get("state") != "held"
                    or attempt.get("generation") != wanted["generation"]
                    or attempt.get("cancel_intent")
                    or execution.get("live_try") not in (None, returned.get("try_id"))
                    or not placements
                    or placements[-1].get("try_id") != returned.get("try_id")
                    or placements[-1].get("target") != "ecs"
                    or placements[-1].get("state") != "complete"
                    or not placements[-1].get("task_arn")
                ):
                    raise StageError("result_identity", 71)
                if (
                    attempt.get("phase") != "ended"
                    or attempt.get("reason") != "exited"
                    or attempt.get("exit_code") != 0
                    or attempt.get("ecs", {}).get("applied", {}).get("try_id") != returned.get("try_id")
                    or output_record(attempt).get("seal_sha256") != digest
                    or returned.get("sha256") != digest
                ):
                    raise StageError("seal_receipt", 71)
                self._head(reference)
                self._head(FileRef("seal.json", returned["key"], returned["version_id"], digest, returned["size"]))
                previous = stage.setdefault("sealed", {}).get(layer)
                if previous is not None and previous != record:
                    raise StageError("seal_conflict", 71)
                stage["sealed"][layer] = record
        except StageError as error:
            raise PipelineError(error.code) from None
        except OSError as error:
            raise PipelineError("remote seal evidence is unreadable") from error
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise PipelineError("remote seal evidence is malformed") from error
        return {"file": f"layer_sealed/{layer}/{expected}.duckdb", "sha256": expected}

    def _seal_reference(self, state: Mapping[str, Any], layer: str, expected: str) -> FileRef | None:
        run = state["runs"][self.run_id]
        record = run.get("stage", {}).get("sealed", {}).get(layer)
        if (
            not isinstance(record, dict)
            or record.get("run_id") != self.run_id
            or record.get("layer") != layer
            or record.get("generation") != run.get("generation", 1)
            or record.get("sha256") != expected
        ):
            return None
        if not {"source_key", "source_version_id", "size"} <= record.keys():
            return None
        return FileRef("staging.duckdb", record["source_key"], record["source_version_id"], expected, record["size"])

    def sealed_digest(self, layer: str, expected: str, *, state: Mapping[str, Any] | None = None) -> bool:
        """Check the accepted seal and pinned head using the acceptance caller's state.

        A caller that already holds the registry lock supplies that state. A
        standalone read takes its own bounded transaction without nesting locks.
        """
        fixed_layer(layer)
        try:
            if state is None:
                with self._transaction() as current:
                    reference = self._seal_reference(current, layer, expected)
            else:
                reference = self._seal_reference(state, layer, expected)
            if reference is None:
                return False
            self._head(reference)
        except StageError as error:
            if error.code in ("stage_deadline", "registry_deadline"):
                raise PipelineError(f"remote seal verification exceeded its deadline ({error.code})") from None
            return False
        return True

    def clear_build_artifacts(self, run: Mapping[str, Any]) -> None:
        """Remove stale target references and their materialized records for this generation."""
        try:
            with self._transaction(write=True) as state:
                current = state["runs"][self.run_id]
                if run.get("run_id") != self.run_id or run.get("generation", 1) != current.get("generation", 1) or self._pending() is not None:
                    raise StageError("result_identity", 71)
                stage = current.get("stage", {})
                objects = stage.get("objects", {})
                for path in list(objects):
                    if path.startswith("target/"):
                        safe_path(path)
                        destination = self.folder / path
                        with held_directory(destination.parent) as descriptor:
                            with suppress(FileNotFoundError):
                                os.unlink(destination.name, dir_fd=descriptor)
                            os.fsync(descriptor)
                        del objects[path]
        except StageError as error:
            raise PipelineError(error.code) from None
