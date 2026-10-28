"""Exercise run-store materialization and restart boundaries with Synthetic S3 objects."""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import os
import shutil
import signal
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from functools import partial
from importlib.machinery import SourceFileLoader
from importlib.metadata import version
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock, create_autospec, patch

from orchestration import pipeline_protocol as protocol
from orchestration.interface import SubmissionRequest
from scripts.lakehouse import provenance, run_lock
from scripts.orchestration.broker import Broker
from scripts.orchestration.ecs_fake_s3 import FakeS3, Principal, S3Refusal
from scripts.orchestration.pipeline_control import perform
from scripts.orchestration.run_control import ControlFailed
from scripts.process import run_command
from services.hai.bootstrap.integrity import StageError, canonical, run_prefix, try_prefix
from services.hai.bootstrap.outputs import read_declaration
from services.hai.bootstrap.transfer import StorageError, Transfer

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261024T000000Z-a1b2c3d4"
ATTEMPT = "synthetic_attempt"
TRY = "hai_synthetic_attempt_1"
TASK = ":".join(("arn", "aws", "ecs", "us-west-2", "0" * 12, "task/synthetic_cluster/" + "0" * 32))
LIMIT = 33_554_432
SOURCE_FILES = (
    "pyproject.toml",
    "orchestration/__init__.py",
    "scripts/lakehouse/__init__.py",
    "scripts/orchestration/__init__.py",
    "services/hai/bootstrap/__init__.py",
    "scripts/orchestration/pipeline_control.py",
    "orchestration/launcher.py",
    "orchestration/interface.py",
    "scripts/orchestration/broker.py",
    "scripts/orchestration/ledger.py",
    "scripts/orchestration/run_control.py",
    "orchestration/remote_run.py",
    "orchestration/pipeline_protocol.py",
    "orchestration/ecs_stage.py",
    "services/hai/bootstrap/integrity.py",
    "services/hai/bootstrap/outputs.py",
    "services/hai/bootstrap/transfer.py",
    "services/hai/bootstrap/redaction.py",
    "scripts/lakehouse/run_lock.py",
    "scripts/lakehouse/provenance.py",
    "scripts/lakehouse/gold_publish_record.py",
    "scripts/process.py",
    "scripts/orchestration/ecs_fake_s3.py",
    "scripts/orchestration/run_runstore_e2e.py",
    "data_contracts/orchestration/ecs_outputs.schema.json",
    "data_contracts/orchestration/ecs_input_map.schema.json",
    "data_contracts/orchestration/ecs_job.schema.json",
)


def require(value: bool, message: str) -> None:
    if not value:
        raise AssertionError(message)


def refused(cause: str, action: Callable[[], Any]) -> None:
    try:
        action()
    except StageError as error:
        require(error.code == cause, f"wrong refusal: expected {cause}, observed {error.code}")
    else:
        raise AssertionError("accepted invalid result: " + cause)


class Storage:
    """Read exact FakeS3 versions through the production transfer request format."""

    def __init__(self, model: FakeS3) -> None:
        self.model = model
        self.principal = Principal("project", "synthetic_project")

    def invoke(self, operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]:
        require(time.monotonic() < deadline, "transport call exceeded its deadline")
        key = request["key"]
        selected = request.get("version_id")
        try:
            head = self.model.head(self.principal, key, selected)
            if operation == "download":
                body = self.model.get(self.principal, key, selected)
                with Path(request["destination"]).open("r+b") as target:
                    target.seek(request["offset"])
                    target.write(body[request["offset"] : request["offset"] + request["length"]])
            elif operation != "head":
                raise AssertionError("unexpected storage operation: " + operation)
        except S3Refusal as error:
            raise StorageError(error.code, error.status) from None
        return {"version_id": head.version_id, "size": head.size, "metadata": dict(head.metadata)}


class Fixture:
    """A real test registry, independent object versions and one stopped live try."""

    def __init__(self, folder: Path, template: str = "pipeline_dbt", params: dict[str, Any] | None = None) -> None:
        from orchestration.remote_run import RemoteRunStore

        self.base = folder
        self.folder = folder / "runs" / RUN
        self.folder.mkdir(parents=True)
        self.logs = folder / "attempts" / ATTEMPT
        self.logs.mkdir(parents=True)
        self.registry = run_lock.Registry(folder / "registry", "test")
        self.registry.path.mkdir()
        self.registry.identity_file.write_bytes(canonical({"registry_id": "synthetic_registry", "kind": "test"}))
        initial = run_lock.empty_state("synthetic_registry", run_lock.LAUNCHER_SCHEMA)
        initial["launcher"]["migrated_at"] = "2026-10-24T00:00:00Z"
        self.registry.state_file.write_bytes(canonical(initial))
        self.s3 = FakeS3(run_prefix(RUN, 1))
        self.storage = Storage(self.s3)
        self.transfer = Transfer(self.storage)
        self.deadline = time.monotonic() + 120
        self.store = RemoteRunStore(self.folder, self.registry, RUN, self.transfer, self.deadline)
        self.params = {"layer": "silver"} if params is None else params
        self.declaration = read_declaration(template, self.params, ["synthetic_table"], 1 << 40, 0)
        self.job = {
            "run_id": RUN,
            "generation": 1,
            "attempt_id": ATTEMPT,
            "try_id": TRY,
            "template": template,
            "input_map_sha256": "1" * 64,
            "expected_outputs": self.declaration.document(),
        }
        self.job_sha256 = hashlib.sha256(canonical(self.job)).hexdigest()
        self.marker: dict[str, Any] = {
            "schema_version": 1,
            "timings": {"execute_seconds": 1.0},
            "bytes": 0,
            "minimum_free_disk_bytes": 1024,
            "discarded_scratch": [],
            **{key: value for key, value in self.job.items() if key != "expected_outputs"},
            "task_arn": TASK,
            "job_spec_sha256": self.job_sha256,
            "exit_code": 0,
            "objects": [],
        }
        self.bodies: dict[str, bytes] = {}
        for output in self.declaration.required:
            body = b"synthetic database" if output.kind == "data" else canonical({"synthetic": output.name})
            self.add(output.name, body, output.kind)
        self.add("stdout.txt", b'{"synthetic_receipt":true}\n', "controller_record")
        self.add("stderr.txt", b"synthetic diagnostic\n", "controller_record")
        with run_lock.transaction(self.registry, create_schema=run_lock.LAUNCHER_SCHEMA) as state:
            state["runs"][RUN] = {"run_id": RUN, "generation": 1, "state": "held"}
            state["attempts"][ATTEMPT] = {
                "id": ATTEMPT,
                "owner": "run:" + RUN,
                "template": template,
                "generation": 1,
                "phase": "ended",
                "reason": "exited",
                "exit_code": 0,
                "created_at": "2026-10-24T00:00:00Z",
                "params": {"database": str(self.folder / "staging.duckdb"), "out_host": str(self.folder / "quality_silver")},
                "log_tail": str(self.logs / "log_tail.txt"),
                "execution": {
                    "live_try": TRY,
                    "placements": [{"try_id": TRY, "target": "ecs", "state": "complete", "task_arn": TASK}],
                },
            }

    def add(self, name: str, body: bytes, kind: str) -> None:
        self.bodies[name] = body
        key = try_prefix(RUN, 1, TRY) + "out/" + name
        digest = hashlib.sha256(body).hexdigest()
        head = self.s3.seed(key, body, {"sha256": digest})
        self.marker["objects"] = [item for item in self.marker["objects"] if item["name"] != name]
        self.marker["objects"].append({"name": name, "key": key, "version_id": head.version_id, "sha256": digest, "size": len(body), "kind": kind})

    def apply(self) -> dict[str, Any]:
        self.marker["bytes"] = sum(item["size"] for item in self.marker["objects"])
        return self.store.materialize(self.job, self.marker, self.declaration, self.job_sha256, {})

    def state(self) -> dict[str, Any]:
        with run_lock.transaction(self.registry, write=False) as state:
            return copy.deepcopy(state)

    def change(self, edit: Callable[[dict[str, Any]], None]) -> None:
        with run_lock.transaction(self.registry) as state:
            edit(state)

    def applied(self) -> bool:
        return bool(self.state()["attempts"][ATTEMPT].get("ecs", {}).get("applied"))


def complete_result(folder: Path) -> None:
    fixture = Fixture(folder)
    result = fixture.apply()
    require(fixture.applied(), "complete result has no persisted applied state")
    for name in ("build_receipt.json", "target/manifest.json", "target/run_results.json"):
        raw = protocol.read_bytes(fixture.folder, name)
        require(raw == fixture.bodies[name], "materialized record differs: " + name)
    for name in ("stdout.txt", "stderr.txt"):
        require(protocol.read_bytes(fixture.logs, name) == fixture.bodies[name], "independent stream differs: " + name)
    require(protocol.output_record(fixture.state()["attempts"][ATTEMPT]) == {"synthetic_receipt": True}, "stdout receipt was synthesized")
    require(json.loads((fixture.logs / "exit.json").read_bytes())["exit_code"] == 0, "missing exit metadata")
    require((fixture.logs / "log_tail.txt").read_bytes().endswith(b"synthetic diagnostic\n"), "missing log tail")
    require(not (fixture.folder / "staging.duckdb").exists(), "database downloaded to Mac")
    reference = fixture.state()["runs"][RUN]["stage"]["objects"]["staging.duckdb"]
    source = next(item for item in fixture.marker["objects"] if item["name"] == "staging.duckdb")
    require(reference == {key: source[key] for key in ("key", "version_id", "sha256", "size")} | {"try_id": TRY}, "run map lost immutable identity")
    saved = fixture.state()
    require(fixture.apply() == result and fixture.state() == saved, "repeat changed applied state")
    require(fixture.store.recover({}) is None, "completed operation retained pending recovery")


def partial_result(folder: Path) -> None:
    fixture = Fixture(folder)
    fixture.marker["objects"] = [item for item in fixture.marker["objects"] if item["name"] != "build_receipt.json"]
    refused("missing_output", fixture.apply)
    require(not fixture.applied() and not (fixture.folder / "target").exists(), "partial result changed logical files")


def identity_failure(folder: Path, variant: str) -> None:
    fixture = Fixture(folder)
    if variant == "stale":
        fixture.change(lambda state: state["attempts"][ATTEMPT]["execution"].update(live_try="another_try"))
    elif variant == "running":
        fixture.change(lambda state: state["attempts"][ATTEMPT]["execution"]["placements"][0].update(state="running"))
    elif variant == "cancelled":
        fixture.change(lambda state: state["attempts"][ATTEMPT].update(cancel_intent=True))
    elif variant == "generation":
        fixture.change(lambda state: state["runs"][RUN].update(generation=2))
    elif variant == "task":
        fixture.marker["task_arn"] = TASK[:-1] + "1"
    else:
        raise AssertionError("unknown identity variant")
    refused("result_identity", fixture.apply)
    require(not fixture.applied(), "invalid identity marked applied")


def version_failure(folder: Path, variant: str) -> None:
    fixture = Fixture(folder)
    entry = fixture.marker["objects"][0]
    if variant == "version":
        entry["version_id"] = "missing_version"
        refused("remote_object", fixture.apply)
    elif variant == "size":
        entry["size"] += 1
        refused("stage_conflict", fixture.apply)
    elif variant == "hash":
        entry["sha256"] = "e" * 64
        refused("stage_conflict", fixture.apply)
    elif variant == "head_version":
        original = fixture.storage.invoke

        def mismatched(operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]:
            reply = original(operation, request, deadline)
            if operation == "head" and request["key"].endswith("staging.duckdb"):
                return {**reply, "version_id": "different_version"}
            return reply

        with patch.object(fixture.storage, "invoke", mismatched):
            refused("stage_conflict", fixture.apply)
    elif variant == "body":
        original = fixture.storage.invoke

        def corrupt(operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]:
            reply = original(operation, request, deadline)
            if operation == "download":
                path = Path(request["destination"])
                path.write_bytes(b"x" * path.stat().st_size)
            return reply

        with patch.object(fixture.storage, "invoke", corrupt):
            refused("input_digest", fixture.apply)
    require(not fixture.applied(), "bad object marked applied")


def record_boundary(folder: Path, template: str, params: dict[str, Any], name: str, size: int) -> None:
    fixture = Fixture(folder, template, params)
    fixture.add(name, b" " * size, "controller_record")
    if size > LIMIT:
        try:
            fixture.apply()
        except StageError as error:
            require(error.code in ("invalid_result", "output_bound", "record_bound"), "wrong size refusal: " + error.code)
        else:
            raise AssertionError("accepted oversized controller record: " + name)
        require(not fixture.applied(), "oversized record marked applied")
        return
    fixture.apply()
    root = fixture.logs if name in ("stdout.txt", "stderr.txt") else fixture.folder
    logical = name
    if name == "result.json":
        logical = {
            "pipeline_validate_silver": "quality_silver/result.json",
            "pipeline_validate_gold": "quality_gold/result.json",
            "pipeline_validate_cross": "quality_cross/result.json",
        }.get(template, name)
    require(len(protocol.read_bytes(root, logical)) == size, "inclusive record limit rejected: " + name)


def reader_bound(folder: Path) -> None:
    path = folder / "record.json"
    path.write_bytes(b" " * (LIMIT + 1))
    try:
        protocol.read_bytes(folder, path.name)
    except protocol.PipelineError as error:
        require("size limit" in str(error), "reader failed for a different cause")
    else:
        raise AssertionError("reader accepted an oversized record")


def representative_manifest(folder: Path) -> None:
    fixture = Fixture(folder)
    invocation = {"metadata": {"invocation_id": "synthetic_invocation"}}
    manifest = canonical(invocation)
    manifest += b" " * (7_999_899 - len(manifest))
    fixture.add("target/manifest.json", manifest, "controller_record")
    fixture.add("target/run_results.json", canonical({**invocation, "args": {"which": "build"}, "results": [{"status": "success"}]}), "controller_record")
    fixture.apply()
    with provenance.held(fixture.folder.parent, (RUN,)) as descriptor:
        observed = provenance.gold_eligible_invocation(descriptor, RUN)
    require(observed == "synthetic_invocation", "unchanged dbt reader rejected representative manifest")
    require(protocol.read_record(fixture.folder, "target/manifest.json")[0] == invocation, "protocol manifest reader changed")


class Crash(BaseException):
    """A simulated process death that cannot be mistaken for an ordinary failure."""


@contextmanager
def operations(events: list[str], stop: int | None = None) -> Iterator[None]:
    import orchestration.remote_run as remote

    def wrap(name: str, function: Callable[..., Any]) -> Callable[..., Any]:
        def invoke(*args: Any, **kwargs: Any) -> Any:
            for phase in ("before", "after"):
                events.append(name + ":" + phase)
                if len(events) == stop:
                    raise Crash(name + ":" + phase)
                if phase == "before":
                    value = function(*args, **kwargs)
            return value

        return invoke

    with ExitStack() as stack:
        for subject, name, label, function in (
            (Transfer, "download", "download", Transfer.download),
            (remote, "scan_file", "scan", remote.scan_file),
            (os, "replace", "rename", os.replace),
            (os, "fsync", "sync", os.fsync),
            (run_lock, "write_atomic", "registry", run_lock.write_atomic),
        ):
            stack.enter_context(patch.object(subject, name, wrap(label, function)))
        yield


def assert_no_partial_acceptance(fixture: Fixture) -> None:
    if fixture.applied():
        for name in ("build_receipt.json", "target/manifest.json", "target/run_results.json"):
            require(
                (fixture.folder / name).is_file() and (fixture.folder / name).read_bytes() == fixture.bodies[name], "applied state precedes complete records"
            )
        return
    changed = any((fixture.folder / name).exists() for name in ("build_receipt.json", "target/manifest.json", "target/run_results.json"))
    if changed:
        try:
            protocol.read_record(fixture.folder, "build_receipt.json")
        except protocol.PipelineError as error:
            require("materialization" in str(error), "wrong partial-read rejection")
        else:
            raise AssertionError("partial materialization became readable")
    if (fixture.logs / "stdout.txt").exists():
        try:
            protocol.output_record(fixture.state()["attempts"][ATTEMPT])
        except protocol.PipelineError as error:
            require("materialization" in str(error), "wrong attempt-receipt rejection")
        else:
            raise AssertionError("partial attempt receipt became readable")


def crash_schedule(folder: Path, report: list[dict[str, Any]]) -> None:
    baseline = Fixture(folder / "schedule")
    schedule: list[str] = []
    with operations(schedule):
        baseline.apply()
    require(any(item.startswith("registry:") for item in schedule), "schedule missed registry persistence")
    for index, event in enumerate(schedule, 1):
        fixture = Fixture(folder / f"crash_{index:03}")
        observed: list[str] = []
        try:
            with operations(observed, index):
                fixture.apply()
        except Crash:
            pass
        else:
            raise AssertionError("crash boundary not reached: " + event)
        require(observed == schedule[:index], "crash schedule changed")
        assert_no_partial_acceptance(fixture)
        recovered = fixture.store.recover({})
        if recovered is None and not fixture.applied():
            fixture.apply()
        require(fixture.applied(), "restart left a complete result unapplied")
        require(protocol.read_bytes(fixture.folder, "build_receipt.json") == fixture.bodies["build_receipt.json"], "recovery lost a record")
        saved = fixture.state()
        fixture.apply()
        require(fixture.state() == saved, "recovered replay duplicated state")
        report.append({"step": index, "operation": event, "ok": True})


def authority_after_prepare(folder: Path, variant: str) -> None:
    fixture = Fixture(folder)
    original = fixture.store._prepare

    def changed(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        entries = original(*args, **kwargs)
        if variant == "cancel":
            fixture.change(lambda state: state["attempts"][ATTEMPT].update(cancel_intent=True))
        else:
            fixture.change(lambda state: state["attempts"][ATTEMPT]["execution"].update(live_try="superseding_try"))
        return entries

    with patch.object(fixture.store, "_prepare", changed):
        refused("result_identity", fixture.apply)
    require(not fixture.applied(), "authority changed during prepare was applied")
    require(not (fixture.folder / "build_receipt.json").exists(), "authority changed during prepare published records")


def conflicting_applied(folder: Path) -> None:
    fixture = Fixture(folder)
    previous = {"try_id": "previous_try", "identity": "0" * 64}
    fixture.change(lambda state: state["attempts"][ATTEMPT].update(ecs={"applied": previous}))
    refused("result_identity", fixture.apply)
    require(fixture.state()["attempts"][ATTEMPT]["ecs"]["applied"] == previous, "conflicting applied identity was replaced")
    require(not (fixture.folder / "build_receipt.json").exists(), "conflicting applied identity published records")


def seam_error(folder: Path, variant: str) -> None:
    fixture = Fixture(folder)
    broker = create_autospec(Broker, instance=True)
    broker.runs_root = fixture.folder.parent
    broker.principal = Mock(return_value={"kind": "run", "run_id": RUN})
    submissions = {
        "validation": [
            {"template": "silver_validate", "attempt": 1},
            {"event": "finished", "exit_code": 0, "cancelled": False, "build_file": {"run_id": RUN, "sha256": "a" * 64, "attempt_id": ATTEMPT}},
        ]
    }
    broker.ledger = Mock(submissions=Mock(return_value=submissions))
    broker.call = Mock(side_effect=AssertionError("failed seam reached acceptance"))
    request = SubmissionRequest("seal_silver", RUN, "publish_fixture", 1, "silver_transform_dag", "seal_silver", "manual__synthetic")
    expected = "remote_seal_unavailable"
    with ExitStack() as stack:
        if variant != "seal":
            current = fixture.state()["runs"][RUN]
            if variant == "clear_identity":
                current = {**current, "run_id": "different_run"}
                expected = "result_identity"
            elif variant == "clear_deadline":
                fixture.store.deadline = time.monotonic() - 1
                expected = "stage_deadline"
            else:
                raise AssertionError("unknown seam error variant")

            def clear(layer: str, digest: str) -> dict[str, str]:
                fixture.store.clear_build_artifacts(current)
                raise AssertionError("artifact clearing accepted invalid state")

            stack.enter_context(patch.object(fixture.store, "seal", clear))
        try:
            perform(broker, request, "seal_silver", {}, store=fixture.store)
        except ControlFailed as error:
            require(str(error) == expected, "wrong control failure: " + str(error))
        else:
            raise AssertionError("control accepted a failed store seam")


def wait_child(pid: int, deadline: float) -> int:
    while time.monotonic() < deadline:
        reaped, status = os.waitpid(pid, os.WNOHANG)
        if reaped:
            return os.waitstatus_to_exitcode(status)
        time.sleep(0.01)
    raise AssertionError("forked child exceeded its 30 second deadline")


def stop_child(pid: int) -> None:
    try:
        reaped, _ = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return
    if reaped:
        return
    os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        reaped, _ = os.waitpid(pid, os.WNOHANG)
        if reaped:
            return
        time.sleep(0.01)
    raise ChildSurvived(f"owned child {pid} survived SIGKILL for 5 seconds")


class ChildSurvived(BaseException):
    """The run must stop with its owned fixture retained for process cleanup."""


@contextmanager
def scratch_folder(parent: Path, prefix: str) -> Iterator[Path]:
    folder = Path(tempfile.mkdtemp(prefix=prefix, dir=parent))
    remove = True
    try:
        yield folder
    except ChildSurvived:
        remove = False
        raise
    finally:
        if remove:
            shutil.rmtree(folder)


def process_death(folder: Path, checkpoint: str, report: list[dict[str, Any]] | None = None) -> None:
    fixture = Fixture(folder)
    unrelated = fixture.folder / ".runstore_stage_notes"
    unrelated.mkdir()
    (unrelated / "keep.txt").write_bytes(b"unrelated synthetic record")
    pid = os.fork()
    if pid == 0:
        try:
            if checkpoint == "before_journal":
                prepared = fixture.store._prepare

                def die_prepared(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
                    prepared(*args, **kwargs)
                    os._exit(77)

                with patch.object(fixture.store, "_prepare", die_prepared):
                    fixture.apply()
            elif checkpoint == "after_gates":
                unlinked = fixture.store._unlink_gate

                def die_unlinked(path: Path, identity: str) -> None:
                    unlinked(path, identity)
                    if path == fixture.folder:
                        os._exit(77)

                with patch.object(fixture.store, "_unlink_gate", die_unlinked):
                    fixture.apply()
            else:
                raise AssertionError("unknown process-death checkpoint")
        except BaseException as error:
            (folder / "child_error.json").write_bytes(canonical({"type": type(error).__name__, "cause": str(error)}))
            os._exit(79)
        os._exit(78)
    event: dict[str, Any] = {"checkpoint": checkpoint, "pid": pid, "reaped": False, "ok": False}
    if report is not None:
        report.append(event)
    reaped = False
    try:
        code = wait_child(pid, time.monotonic() + 30)
        reaped = True
        event.update(exit_code=code, reaped=True)
    finally:
        if not reaped:
            stop_child(pid)
            event["reaped"] = True
    require(code == 77, f"child failed before its death checkpoint: {checkpoint}:{code}")
    before = sorted(path.name for path in fixture.folder.glob(".runstore_stage_*") if path != unrelated)
    event["spools_after_death"] = before
    if checkpoint == "after_gates":
        require(not before, "completed process death retained a spool")
        require(fixture.applied(), "completed process death lost applied state")
    else:
        require(bool(before), "pre-journal death did not leave a real spool")
        require(not fixture.applied(), "pre-journal death applied a result")
    fixture.store.recover({})
    require(not [path for path in fixture.folder.glob(".runstore_stage_*") if path != unrelated], "pre-journal recovery retained a spool")
    require((unrelated / "keep.txt").read_bytes() == b"unrelated synthetic record", "spool recovery removed an unrelated directory")
    fixture.apply()
    require(fixture.applied(), "process-death recovery lost applied state")
    require(protocol.read_bytes(fixture.folder, "build_receipt.json") == fixture.bodies["build_receipt.json"], "process-death recovery lost a result")
    event["ok"] = True


def secret_failure(folder: Path) -> None:
    fixture = Fixture(folder)
    fixture.add("build_receipt.json", canonical({"synthetic": "synthetic_secret_value"}), "controller_record")
    refused(
        "secret_in_output",
        lambda: fixture.store.materialize(fixture.job, fixture.marker, fixture.declaration, fixture.job_sha256, {"synthetic_name": "synthetic_secret_value"}),
    )
    require(not fixture.applied() and not (fixture.folder / "target").exists(), "scan failure exposed a partial result")
    require(not list(fixture.folder.glob(".runstore_stage_*")), "failed secret scan retained staged record bytes")


def local_store(folder: Path) -> None:
    source = folder / "staging.duckdb"
    source.write_bytes(b"synthetic sealed bytes")
    expected = hashlib.sha256(source.read_bytes()).hexdigest()
    store = protocol.LocalRunStore(folder)
    first = store.seal("silver", expected)
    second = protocol.seal_copy(folder, "silver", expected)
    require(first == second and store.sealed_digest("silver", expected), "local seam changed seal behavior")
    require((folder / first["file"]).read_bytes() == source.read_bytes(), "local seam changed sealed bytes")


def receipt_mismatch(folder: Path) -> None:
    fixture = Fixture(folder, "pipeline_validate_silver", {})
    digest = "a" * 64
    fixture.add("result.json", canonical({"counts": {"fail": 0, "warn": 0}, "build_file_sha256": digest}), "controller_record")
    fixture.add("stdout.txt", canonical({"result_sha256": "f" * 64}) + b"\n", "controller_record")
    fixture.apply()
    state = fixture.state()
    try:
        protocol.accept_validation(state, RUN, "silver", ATTEMPT, fixture.folder, store=fixture.store)
    except protocol.PipelineError as error:
        require("producer receipt" in str(error), "receipt mismatch failed for another cause")
    else:
        raise AssertionError("independent stdout disagreement was accepted")


def clear_artifacts(folder: Path) -> None:
    fixture = Fixture(folder)
    fixture.apply()
    fixture.store.clear_build_artifacts(fixture.state()["runs"][RUN])
    objects = fixture.state()["runs"][RUN]["stage"]["objects"]
    require(not any(name.startswith("target/") for name in objects), "stale target reference survived clear")
    require("staging.duckdb" in objects and "build_receipt.json" in objects, "clear removed unrelated references")
    require(not (fixture.folder / "target/manifest.json").exists(), "stale materialized manifest survived clear")


def expired_deadline(folder: Path) -> None:
    fixture = Fixture(folder)
    fixture.store.deadline = time.monotonic() - 1
    refused("stage_deadline", fixture.apply)
    require(not fixture.applied(), "expired operation was applied")


def changed_materialization(folder: Path) -> None:
    fixture = Fixture(folder)
    fixture.apply()
    (fixture.folder / "build_receipt.json").write_bytes(b"changed")
    refused("materialized_record", fixture.apply)


def quality_acceptance(folder: Path, locked: bool = False, expired: bool = False, malformed: str | None = None) -> None:
    fixture = Fixture(folder, "pipeline_validate_silver", {})
    database = b"Synthetic remote validated database"
    digest = hashlib.sha256(database).hexdigest()
    key = try_prefix(RUN, 1, TRY) + "out/staging.duckdb"
    head = fixture.s3.seed(key, database, {"sha256": digest})
    seal = {
        "run_id": RUN,
        "layer": "silver",
        "generation": 1,
        "sha256": digest,
        "size": len(database),
        "source_key": key,
        "source_version_id": head.version_id,
        "validation_attempt": ATTEMPT,
    }
    fixture.change(lambda state: state["runs"][RUN].update(stage={"generation": 1, "objects": {}, "sealed": {"silver": seal}}))
    result = canonical({"counts": {"fail": 0, "warn": 0}, "build_file_sha256": digest})
    fixture.add("result.json", result, "controller_record")
    fixture.add("stdout.txt", canonical({"result_sha256": hashlib.sha256(result).hexdigest()}) + b"\n", "controller_record")
    fixture.apply()
    if malformed is not None:
        fixture.change(lambda state: state["runs"][RUN]["stage"]["sealed"]["silver"].pop(malformed))
        try:
            protocol.accept_validation(fixture.state(), RUN, "silver", ATTEMPT, fixture.folder, store=fixture.store)
        except protocol.PipelineError as error:
            require("sealed bytes" in str(error), "wrong malformed-seal refusal")
        except KeyError:
            raise AssertionError("malformed seal key escaped protocol") from None
        else:
            raise AssertionError("malformed seal was accepted")
        return
    if expired:
        fixture.store.deadline = time.monotonic() - 1
        try:
            protocol.accept_validation(fixture.state(), RUN, "silver", ATTEMPT, fixture.folder, store=fixture.store)
        except protocol.PipelineError as error:
            require("deadline" in str(error), "remote seal deadline lost its cause")
        else:
            raise AssertionError("remote seal verification ignored its deadline")
        return
    if locked:
        with (
            run_lock.transaction(fixture.registry, write=False) as current,
            patch.object(fixture.store, "_transaction", side_effect=AssertionError("seal verification reopened the caller's registry lock")),
        ):
            completion = protocol.accept_validation(current, RUN, "silver", ATTEMPT, fixture.folder, store=fixture.store)
            current["runs"][RUN]["stage"]["sealed"]["silver"]["source_version_id"] = "missing"
            try:
                protocol.accept_validation(current, RUN, "silver", ATTEMPT, fixture.folder, store=fixture.store)
            except protocol.PipelineError as error:
                require("sealed bytes" in str(error), "wrong caller-state seal refusal")
            else:
                raise AssertionError("seal verification ignored the caller's current state")
    else:
        completion = protocol.accept_validation(fixture.state(), RUN, "silver", ATTEMPT, fixture.folder, store=fixture.store)
    require(completion["sha256"] == digest and completion["validation_attempt"] == ATTEMPT, "remote seal binding rejected")
    require(not (fixture.folder / "staging.duckdb").exists(), "quality acceptance downloaded remote database")
    fixture.change(lambda state: state["runs"][RUN]["stage"]["sealed"]["silver"].update(source_version_id="missing"))
    try:
        protocol.accept_validation(fixture.state(), RUN, "silver", ATTEMPT, fixture.folder, store=fixture.store)
    except protocol.PipelineError as error:
        require("sealed bytes" in str(error), "wrong remote seal refusal")
    else:
        raise AssertionError("remote seal accepted a missing pinned version")


def large_data_reference(folder: Path) -> None:
    fixture = Fixture(folder, "pipeline_export", {"layer": "gold"})
    fixture.add("publish/export/synthetic_table.parquet", b"d" * (LIMIT + 7), "data")
    original = fixture.storage.invoke
    downloads: list[str] = []

    def observe(operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]:
        if operation == "download":
            downloads.append(request["key"])
        return original(operation, request, deadline)

    with patch.object(fixture.storage, "invoke", observe):
        fixture.apply()
    path = "publish/export/synthetic_table.parquet"
    reference = fixture.state()["runs"][RUN]["stage"]["objects"][path]
    require(reference["size"] == LIMIT + 7 and reference["try_id"] == TRY, "large data reference lost identity")
    require(not any(key.endswith(".parquet") for key in downloads) and not (fixture.folder / path).exists(), "large data downloaded to Mac")


def pinned_version(folder: Path) -> None:
    fixture = Fixture(folder)
    original = next(item for item in fixture.marker["objects"] if item["name"] == "build_receipt.json")
    fixture.s3.seed(original["key"], b"new untrusted head", {"sha256": hashlib.sha256(b"new untrusted head").hexdigest()})
    fixture.apply()
    require(protocol.read_bytes(fixture.folder, "build_receipt.json") == fixture.bodies["build_receipt.json"], "collector used the latest head")
    reference = fixture.state()["runs"][RUN]["stage"]["objects"]["build_receipt.json"]
    require(reference["version_id"] == original["version_id"], "run map replaced the pinned version")


def independent_record_limits(folder: Path) -> None:
    fixture = Fixture(folder)
    example: dict[str, Any] = {
        "path": "inputs/input_000000",
        "key": run_prefix(RUN, 1) + "inputs/files/" + "a" * 64 + "/000000",
        "version_id": "synthetic_input_version",
        "sha256": hashlib.sha256(b"").hexdigest(),
        "size": 0,
    }
    fixture.job["inputs"] = []
    base_size = len(canonical(fixture.job))
    count = (LIMIT - 256 - base_size) // (len(canonical(example)) + 1)
    inputs = [{**example, "path": f"inputs/input_{index:06}", "key": example["key"][:-6] + f"{index:06}"} for index in range(count)]
    fixture.job["inputs"] = inputs
    fixture.job["input_map_sha256"] = hashlib.sha256(canonical(inputs)).hexdigest()
    fixture.marker["input_map_sha256"] = fixture.job["input_map_sha256"]
    fixture.job_sha256 = hashlib.sha256(canonical(fixture.job)).hexdigest()
    fixture.marker["job_spec_sha256"] = fixture.job_sha256
    require(len(canonical(fixture.job)) <= LIMIT and len(canonical(fixture.marker)) <= LIMIT, "fixture exceeded an individual control record limit")
    require(len(canonical(fixture.job)) + len(canonical(fixture.marker)) > LIMIT, "fixture did not cross the combined journal size")
    fixture.apply()
    require(fixture.applied(), "individually valid control records were rejected by an aggregate limit")
    require(protocol.read_bytes(fixture.folder, "build_receipt.json") == fixture.bodies["build_receipt.json"], "large input map lost a result record")


def job_digest_binding(folder: Path) -> None:
    fixture = Fixture(folder)
    fixture.job["input_map_sha256"] = "9" * 64
    fixture.marker["input_map_sha256"] = "9" * 64
    refused("job_digest", fixture.apply)
    require(not fixture.applied(), "changed job was applied")


def declaration_binding(folder: Path) -> None:
    from services.hai.bootstrap.outputs import Declaration

    fixture = Fixture(folder)
    fixture.declaration = Declaration((), (), (), 0)
    refused("result_declaration", fixture.apply)
    require(not fixture.applied(), "weakened output declaration was applied")


def pending_conflict(folder: Path) -> None:
    fixture = Fixture(folder)
    original_body = fixture.bodies["build_receipt.json"]
    with patch.object(fixture.store, "_publish_entry", side_effect=Crash("publication")):
        try:
            fixture.apply()
        except Crash:
            pass
        else:
            raise AssertionError("pending operation was not planted")
    fixture.add("build_receipt.json", b"different result", "controller_record")
    refused("materialization_busy", fixture.apply)
    fixture.store.recover({})
    require(protocol.read_bytes(fixture.folder, "build_receipt.json") == original_body, "recovery changed its pinned operation")


def stale_recovery(folder: Path) -> None:
    fixture = Fixture(folder)
    with patch.object(fixture.store, "_publish_entry", side_effect=Crash("publication")):
        try:
            fixture.apply()
        except Crash:
            pass
        else:
            raise AssertionError("pending operation was not planted")
    fixture.change(lambda state: state["attempts"][ATTEMPT]["execution"].update(live_try="different_try"))
    refused("result_identity", lambda: fixture.store.recover({}))
    require(not fixture.applied(), "stale pending recovery was applied")


def registry_lock_deadline(folder: Path) -> None:
    fixture = Fixture(folder)
    with (fixture.registry.path / "registry.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            fixture.store.deadline = time.monotonic() + 0.02
            refused("registry_deadline", fixture.apply)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    require(not fixture.applied(), "lock timeout applied a result")


def fingerprints() -> dict[str, str]:
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else "missing" for name in SOURCE_FILES}


def main() -> int:
    """Save bounded stand-in evidence, including failed and interrupted runs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--mutations", action="store_true")
    args = parser.parse_args()
    destination = args.out.resolve()
    if not destination.is_relative_to(ROOT / "data/e2e"):
        parser.error("output must stay under data/e2e")
    destination.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {
        "at_utc": datetime.now(UTC).isoformat(),
        "classification": "Synthetic",
        "mode": "full",
        "revision": provenance.git_revision(str(ROOT)),
        "dependency_versions": {package: version(package) for package in ("jsonschema", "referencing", "ruff", "mypy")},
        "scope": "stand-in result validation, FakeS3, real filesystem and registry persistence, unchanged protocol readers",
        "limits": "no AWS, Docker, HTTP, IAM, seal job or full fixture flow",
        "command": [sys.executable, "-m", "scripts.orchestration.run_runstore_e2e", *sys.argv[1:]],
        "python": sys.version,
        "source_sha256": fingerprints(),
        "cases": {},
        "crashes": [],
        "process_deaths": [],
        "mutations": {},
    }
    cases: dict[str, Callable[[Path], None]] = {
        "complete_result_and_repeat": complete_result,
        "partial_result": partial_result,
        "authority_cancel_after_prepare": partial(authority_after_prepare, variant="cancel"),
        "authority_supersede_after_prepare": partial(authority_after_prepare, variant="supersede"),
        "conflicting_applied_identity": conflicting_applied,
        "control_remote_seal_error": partial(seam_error, variant="seal"),
        "control_clear_identity_error": partial(seam_error, variant="clear_identity"),
        "control_clear_deadline_error": partial(seam_error, variant="clear_deadline"),
        "seal_missing_source_key": partial(quality_acceptance, malformed="source_key"),
        "seal_missing_source_version": partial(quality_acceptance, malformed="source_version_id"),
        "seal_missing_size": partial(quality_acceptance, malformed="size"),
        "process_death_before_journal": partial(process_death, checkpoint="before_journal", report=report["process_deaths"]),
        "process_death_after_gates": partial(process_death, checkpoint="after_gates", report=report["process_deaths"]),
        "large_data_reference": large_data_reference,
        "pinned_version": pinned_version,
        "declaration_binding": declaration_binding,
        "job_digest_binding": job_digest_binding,
        "independent_record_limits": independent_record_limits,
        "pending_result_conflict": pending_conflict,
        "stale_recovery": stale_recovery,
        "registry_lock_deadline": registry_lock_deadline,
        "clear_build_artifacts": clear_artifacts,
        "expired_deadline": expired_deadline,
        "changed_materialized_record": changed_materialization,
        "quality_acceptance": quality_acceptance,
        "quality_acceptance_under_registry_lock": partial(quality_acceptance, locked=True),
        "quality_acceptance_deadline": partial(quality_acceptance, expired=True),
        "reader_record_limit": reader_bound,
        "representative_manifest": representative_manifest,
        "secret_scan": secret_failure,
        "local_store": local_store,
        "independent_receipt": receipt_mismatch,
        "crash_every_operation": lambda folder: crash_schedule(folder, report["crashes"]),
    }
    for variant in ("stale", "running", "cancelled", "generation", "task"):
        cases["identity_" + variant] = partial(identity_failure, variant=variant)
    for variant in ("version", "size", "hash", "body", "head_version"):
        cases["object_" + variant] = partial(version_failure, variant=variant)
    templates: tuple[tuple[str, dict[str, Any]], ...] = (
        ("pipeline_dbt", {"layer": "silver"}),
        ("pipeline_validate_silver", {}),
        ("pipeline_validate_gold", {}),
        ("pipeline_validate_cross", {}),
        ("pipeline_export", {"layer": "gold"}),
        ("pipeline_publish", {"layer": "silver", "step": "reconcile"}),
        ("pipeline_spark", {"job": "pin_silver"}),
        ("pipeline_spark", {"job": "record_silver_snapshot"}),
        ("pipeline_spark", {"job": "pin_bronze"}),
    )
    for template, params in templates:
        declaration = read_declaration(template, params, ["synthetic_table"], 1 << 40, 0)
        for name in [item.name for item in (*declaration.required, *declaration.optional) if item.kind == "controller_record"] + ["stdout.txt", "stderr.txt"]:
            for size in (LIMIT, LIMIT + 1):
                label = f"bound_{template}_{params}_{name}_{size}"
                cases[label] = partial(record_boundary, template=template, params=params, name=name, size=size)
    interrupted = False
    try:
        for index, (name, case) in enumerate(cases.items()):
            started = time.monotonic()
            try:
                with scratch_folder(destination, f"case_{index}_") as scratch:
                    case(scratch)
            except (AssertionError, RuntimeError, OSError, ValueError, ImportError, AttributeError) as error:
                report["cases"][name] = {"ok": False, "cause": str(error), "seconds": time.monotonic() - started}
            else:
                report["cases"][name] = {"ok": True, "seconds": time.monotonic() - started}
        if args.mutations:
            report["mutations"] = planted_defects(destination)
    except ChildSurvived:
        interrupted = True
        report["cleanup"] = "failed"
        raise
    except BaseException:
        interrupted = True
        raise
    finally:
        report["sources_unchanged"] = report["source_sha256"] == fingerprints()
        report["interrupted"] = interrupted
        report.setdefault("cleanup", "done")
        (destination / "report.json").write_bytes(canonical(report) + b"\n")
        (destination / "retention.json").write_bytes(
            canonical(
                {"class": 4, "purpose": "Synthetic run-store verification summary", "created": report["at_utc"], "end_condition": "nothing cites this report"}
            )
            + b"\n"
        )
    failures = [name for name, item in report["cases"].items() if not item["ok"]]
    failed_mutations = [name for name, item in report["mutations"].items() if not item["caught"]]
    sys.stdout.write(
        json.dumps(
            {
                "report": str(destination / "report.json"),
                "cases": len(cases),
                "failed": failures,
                "crash_points": len(report["crashes"]),
                "uncaught": failed_mutations,
            }
        )
        + "\n"
    )
    return int(bool(failures or failed_mutations or not report["sources_unchanged"]))


def load_mutant(source: Path, replacements: tuple[tuple[str, str], ...], folder: Path, name: str) -> tuple[ModuleType, dict[str, Any]]:
    raw = source.read_text()
    for before, after in replacements:
        require(raw.count(before) == 1, "mutation anchor changed: " + name)
        raw = raw.replace(before, after, 1)
    path = folder / (name + ".py")
    path.write_text(raw)
    checks = []
    for arguments in (["format", "--config", str(ROOT / "pyproject.toml"), str(path)], ["check", "--config", str(ROOT / "pyproject.toml"), str(path)]):
        result = run_command(sys.executable, ["-m", "ruff", *arguments], timeout=30)
        checks.append({"arguments": arguments, "exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
        (folder.parent / ("lint_" + name + ".json")).write_bytes(canonical(checks) + b"\n")
        require(result.returncode == 0, "mutant lint failed: " + name)
    package = ".".join(source.relative_to(ROOT).parts[:-1])
    loader = SourceFileLoader(package + "._runstore_mutant_" + name, str(path))
    spec = spec_from_loader(loader.name, loader)
    require(spec is not None, "mutant loader has no spec")
    if spec is None:
        raise AssertionError("mutant loader has no spec")
    module = module_from_spec(spec)
    sys.modules[loader.name] = module
    try:
        loader.exec_module(module)
    finally:
        del sys.modules[loader.name]
    module.__file__ = str(source)
    return module, {"source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "lint": checks}


def planted_defects(destination: Path) -> dict[str, Any]:
    """Run the required production defects against their observable cases."""
    from orchestration import remote_run

    skip = (
        "for entry in entries:\n                    self._publish_entry(entry, spool, logs)",
        'for entry in entries:\n                    if entry["path"] != "build_receipt.json":\n                        self._publish_entry(entry, spool, logs)',
    )
    partial_result_guard = (
        'if not exit_code and not (required | {"stdout.txt", "stderr.txt"}) <= result.keys():',
        'if not exit_code and required and not {"stdout.txt", "stderr.txt"} <= result.keys():',
    )
    early_apply = (
        "                self._write(self.folder / MATERIALIZATION_GATE, canonical(journal))",
        "                premature = json.loads(self.registry.state_file.read_bytes())\n"
        '                premature["attempts"][job["attempt_id"]].setdefault("ecs", {})["applied"] = applied\n'
        "                run_lock.write_atomic(self.registry.state_file, json.dumps(premature))\n"
        "                self._write(self.folder / MATERIALIZATION_GATE, canonical(journal))",
    )
    size = (
        ("while len(result) <= limit:", "while True:"),
        ("os.read(handle, min(1 << 20, limit + 1 - len(result)))", "os.read(handle, 1 << 20)"),
    )
    probes: tuple[tuple[str, str, tuple[tuple[str, str], ...], Any, str, Callable[[Path], None], str], ...] = (
        ("skip_record", "orchestration/remote_run.py", (skip,), remote_run, "RemoteRunStore", complete_result, "materialized_record"),
        (
            "accept_partial_result",
            "orchestration/ecs_stage.py",
            (partial_result_guard,),
            remote_run,
            "validate_result",
            partial_result,
            "accepted invalid result: missing_output",
        ),
        (
            "apply_before_registry_order",
            "orchestration/remote_run.py",
            (early_apply,),
            remote_run,
            "RemoteRunStore",
            lambda folder: crash_schedule(folder, []),
            "applied state precedes complete records",
        ),
        ("drop_reader_size_check", "orchestration/pipeline_protocol.py", size, protocol, "read_bytes", reader_bound, "reader accepted an oversized record"),
    )
    probes += (
        (
            "omit_attempt_gate",
            "orchestration/remote_run.py",
            (('                self._write(logs / MATERIALIZATION_GATE, canonical({"identity": identity}))\n', ""),),
            remote_run,
            "RemoteRunStore",
            lambda folder: crash_schedule(folder, []),
            "partial attempt receipt became readable",
        ),
        (
            "omit_apply_authority_recheck",
            "orchestration/remote_run.py",
            (
                (
                    "            run, attempt, _ = self._identity(state, job)\n",
                    '            run = state["runs"][self.run_id]\n            attempt = state["attempts"][job["attempt_id"]]\n',
                ),
            ),
            remote_run,
            "RemoteRunStore",
            partial(authority_after_prepare, variant="cancel"),
            "authority changed during prepare was applied",
        ),
        (
            "overwrite_applied_identity",
            "orchestration/remote_run.py",
            (('            if previous is not None and previous != applied:\n                raise StageError("result_identity", 71)\n', ""),),
            remote_run,
            "RemoteRunStore",
            conflicting_applied,
            "accepted invalid result: result_identity",
        ),
        (
            "leak_seal_stage_error",
            "orchestration/remote_run.py",
            (
                ('                    raise PipelineError("remote_seal_unavailable")', '                    raise StageError("remote_seal_unavailable")'),
                (
                    '                stage["sealed"][layer] = record\n'
                    "        except StageError as error:\n"
                    "            raise PipelineError(error.code) from None",
                    '                stage["sealed"][layer] = record\n        except StageError:\n            raise',
                ),
            ),
            remote_run,
            "RemoteRunStore",
            partial(seam_error, variant="seal"),
            "remote_seal_unavailable",
        ),
        (
            "leak_clear_stage_error",
            "orchestration/remote_run.py",
            (
                (
                    "                        del objects[path]\n        except StageError as error:\n            raise PipelineError(error.code) from None",
                    "                        del objects[path]\n        except StageError:\n            raise",
                ),
            ),
            remote_run,
            "RemoteRunStore",
            partial(seam_error, variant="clear_identity"),
            "result_identity",
        ),
        (
            "index_missing_seal_key",
            "orchestration/remote_run.py",
            (('        if not {"source_key", "source_version_id", "size"} <= record.keys():\n            return None\n', ""),),
            remote_run,
            "RemoteRunStore",
            partial(quality_acceptance, malformed="source_key"),
            "malformed seal key escaped protocol",
        ),
        (
            "keep_completed_spool",
            "orchestration/remote_run.py",
            (
                (
                    "            shutil.rmtree(spool)\n            self._sync(self.folder)\n            self._unlink_gate(logs, identity)",
                    "            self._unlink_gate(logs, identity)",
                ),
                ("        return applied\n", "        shutil.rmtree(spool)\n        self._sync(self.folder)\n        return applied\n"),
            ),
            remote_run,
            "RemoteRunStore",
            partial(process_death, checkpoint="after_gates"),
            "completed process death retained a spool",
        ),
        (
            "omit_orphan_spool_sweep",
            "orchestration/remote_run.py",
            (("                self._discard_spools()\n", ""),),
            remote_run,
            "RemoteRunStore",
            partial(process_death, checkpoint="before_journal"),
            "pre-journal recovery retained a spool",
        ),
        (
            "ignore_head_version",
            "services/hai/bootstrap/transfer.py",
            (
                (
                    "        if object_identity(reply, reference.size, reference.sha256) != reference.version_id:",
                    "        if not object_identity(reply, reference.size, reference.sha256):",
                ),
            ),
            Transfer,
            "verify_reference",
            partial(version_failure, variant="head_version"),
            "accepted invalid result: stage_conflict",
        ),
    )
    results: dict[str, Any] = {}
    for name, source, replacements, target, attribute, case, expected in probes:
        with scratch_folder(destination, "mutant_" + name + "_") as folder:
            mutant, evidence = load_mutant(ROOT / source, replacements, folder, name)
            fixture = folder / "fixture"
            fixture.mkdir()
            observed = ""
            replacement = mutant.Transfer.verify_reference if attribute == "verify_reference" else getattr(mutant, attribute)
            with patch.object(target, attribute, replacement):
                try:
                    case(fixture)
                except (AssertionError, StageError) as error:
                    observed = str(error)
            results[name] = {**evidence, "caught": observed == expected, "expected": expected, "observed": observed}
            (destination / "mutation_results.json").write_bytes(canonical(results) + b"\n")
    return results


if __name__ == "__main__":
    sys.exit(main())
