"""Check provider projection, caller correlation and supervisor collection without services or native provider packages."""

from __future__ import annotations

import argparse
import copy
import errno
import hashlib
import importlib
import json
import os
import sqlite3
import stat
import sys
import tempfile
import threading
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager, nullcontext
from datetime import UTC, datetime
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from orchestration import lineage
from scripts.lakehouse.run_lock import Registry
from scripts.orchestration import supervisor
from scripts.orchestration.broker import BrokerPaths
from scripts.orchestration.run_lineage_e2e import record_fixture

ROOT = Path(__file__).resolve().parents[2]
NATIVE_RUN = "13d9e9ef-93b2-4efc-90d5-066b1becf67d"
NATIVE_DAG = "3589fe84-f7d0-49fe-b456-5fa256a72938"
SENTINEL = "synthetic-credential-not-for-lineage"
OBSERVED: dict[str, Any] = {}


def require(value: Any, message: str) -> None:
    if not value:
        raise AssertionError(message)


def provider_module() -> Any:
    require((ROOT / "services/lineage/provider_events.py").is_file(), "safe provider projection is not implemented")
    return importlib.import_module("services.lineage.provider_events")


def native(
    record: dict[str, Any],
    *,
    dag: bool = False,
    state: str = "START",
    run_uuid: str = NATIVE_RUN,
    parent_uuid: str = NATIVE_DAG,
    event_time: str = "2026-10-17T01:00:00+00:00",
) -> Any:
    caller = {name: record[name] for name in ("dag_id", "task_id", "dag_run_id", "attempt")}
    metadata = SimpleNamespace(
        dag={"dag_id": caller["dag_id"], "params": {"credential": SENTINEL}},
        dagRun={"run_id": caller["dag_run_id"], "conf": {"secret": SENTINEL}},
        task={"task_id": caller["task_id"], "source_code": SENTINEL},
        taskInstance={"try_number": caller["attempt"], "map_index": -1, "environment": SENTINEL},
    )
    parent = SimpleNamespace(run=SimpleNamespace(runId=parent_uuid), job=SimpleNamespace(namespace=lineage.JOB_NAMESPACE, name=caller["dag_id"]))
    return SimpleNamespace(
        eventType=SimpleNamespace(value=state),
        eventTime=event_time,
        run=SimpleNamespace(
            runId=parent_uuid if dag else run_uuid, facets={"airflowDagRun" if dag else "airflow": metadata, "parent": parent, "errorMessage": SENTINEL}
        ),
        job=SimpleNamespace(
            namespace=lineage.JOB_NAMESPACE, name=caller["dag_id"] if dag else f"{caller['dag_id']}.{caller['task_id']}", facets={"sql": SENTINEL}
        ),
        inputs=[{"namespace": SENTINEL, "name": SENTINEL}],
        outputs=[{"namespace": SENTINEL, "name": SENTINEL}],
        producer=SENTINEL,
    )


def projection() -> None:
    provider = provider_module()
    record = record_fixture()
    for is_dag in (False, True):
        event = provider.project(native(record, dag=is_dag))
        require(event["event"]["inputs"] == [] and event["event"]["outputs"] == [], "provider invented dataset authority")
        require(SENTINEL not in json.dumps(event), "provider projection retained a sentinel")
        require(provider.validate(event) == event, "provider envelope does not validate after projection")
    event = native(record, state="FAIL")
    require(provider.project(event)["event"]["eventType"] == "FAIL", "provider lost the forced failure state")
    require(
        provider.project(native(record, state="RUNNING"))["id"]
        != provider.project(native(record, state="RUNNING", event_time="2026-10-17T01:00:01+00:00"))["id"],
        "distinct native callbacks share an immutable event identity",
    )
    event.run.facets["airflow"].taskInstance["map_index"] = None
    require(provider.project(event)["caller"]["attempt"] == 1, "native unmapped-task encoding lost caller correlation")
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve() / "spool"
        require(provider.emit(root, event), "provider did not persist the sanitized event")
        require(provider.emit(root, event), "identical provider replay was refused")
        files = list(root.glob("*.json"))
        require(len(files) == 1 and SENTINEL not in files[0].read_text(), "provider replay duplicated or leaked bytes")
        event.job.name = "other_dag.other_task"
        require(not provider.emit(root, event), "provider accepted a job that conflicts with caller metadata")
        event = native(record)
        event.run.facets["airflow"].taskInstance["map_index"] = 0
        require(not provider.emit(root, event), "mapped task guessed a v2 caller identity")
        event = native(record)
        event.run.facets["airflow"].taskInstance["try_number"] = 100
        require(not provider.emit(root, event), "provider accepted an out-of-contract attempt")
        event = native(record, run_uuid="bad-native-id")
        require(not provider.emit(root, event), "provider accepted an invalid native UUID")


def correlation() -> None:
    provider = provider_module()
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve()
        records, spool, database = root / "records", root / "spool", root / "outbox.sqlite"
        records.mkdir()
        spool.mkdir()
        record = record_fixture()
        (records / "one.json").write_text(json.dumps(record))
        missing = lineage.sweep(records, database, require_provider=True)
        require(missing["queued"] == 0 and missing["deferred"] == 1, "missing native correlation was guessed or lost")
        require(provider.emit(spool, native(record)), "provider spool setup failed")
        lineage.collect_provider(spool, database)
        first = lineage.sweep(records, database, require_provider=True)
        require(first["queued"] == 2 and first["deferred"] == 0, "correlated record did not reach outbox")
        lineage.sweep(records, database, require_provider=True)
        with lineage.Outbox(database) as box:
            box.flush_provider(limit=1000, deadline_seconds=10)
        with lineage.Outbox(database) as box:
            queued = box.pending(100)
            events = [json.loads(body) for _, body in queued]
            require(all(event["run"]["runId"] == NATIVE_RUN for event in events), "host and provider task run UUIDs differ")
            require(any(event["run"]["facets"].get("parent", {}).get("run", {}).get("runId") == NATIVE_DAG for event in events), "native DAG parent was lost")
        lineage.collect_provider(spool, database)
        require(lineage.sweep(records, database, require_provider=True)["queued"] == 0, "repeated collection duplicated events")
        with lineage.Outbox(database) as box:
            require(box.pending(100) == queued, "reopen or resend changed immutable queued bytes")
        second = copy.deepcopy(record)
        second["dag_id"] = "gold_transform_dag"
        second["dag_run_id"] = "manual__gold"
        second["lineage"]["inputs"] = copy.deepcopy(record["lineage"]["outputs"])
        second["lineage"]["outputs"] = [{"namespace": "file", "name": "fixture/gold.duckdb"}]
        second["ledger"][0].update({key: copy.deepcopy(second[key]) for key in ("dag_id", "dag_run_id", "lineage")})
        (records / "two.json").write_text(json.dumps(second))
        other = native(second, run_uuid="7fb14c03-c931-44ce-9c46-cbf965d4a104", parent_uuid="7053e47a-b57a-4552-9e9a-c11e3a1f96e3")
        require(provider.emit(spool, other), "second DAG spool setup failed")
        lineage.collect_provider(spool, database)
        lineage.sweep(records, database, require_provider=True)
        lineage.sweep(records, database, require_provider=True)
        with lineage.Outbox(database) as box:
            box.flush_provider(limit=1000, deadline_seconds=10)
        with lineage.Outbox(database) as box:
            events = [json.loads(body) for _, body in box.pending(100)]
            complete = {event["job"]["name"]: event for event in events if event["eventType"] == "COMPLETE"}
            require(
                complete[f"{record['dag_id']}.{record['task_id']}"]["outputs"] == complete[f"{second['dag_id']}.{second['task_id']}"]["inputs"],
                "two DAGs lost shared dataset identity",
            )
        retry = copy.deepcopy(record)
        retry["attempt"] = 2
        retry["ledger"][0]["attempt"] = 2
        (records / "three.json").write_text(json.dumps(retry))
        retry_uuid = "91ea107d-e153-4016-a12e-4c09778e8110"
        require(provider.emit(spool, native(retry, run_uuid=retry_uuid)), "retry native callback setup failed")
        lineage.collect_provider(spool, database)
        lineage.sweep(records, database, require_provider=True)
        lineage.sweep(records, database, require_provider=True)
        with lineage.Outbox(database) as box:
            box.flush_provider(limit=1000, deadline_seconds=10)
            events = [json.loads(body) for _, body in box.pending(100)]
        retried = [event for event in events if event["run"]["facets"].get("haiIdentity", {}).get("attempt") == 2]
        require(retried and {event["run"]["runId"] for event in retried} == {retry_uuid}, "retry reused the previous native task run")
        conflicting = native(record, state="COMPLETE", run_uuid=str(uuid.uuid4()))
        require(provider.emit(spool, conflicting), "conflicting native mapping fixture was not stored")
        try:
            lineage.collect_provider(spool, database)
        except lineage.LineageError as error:
            require("correlation" in str(error), "conflicting mapping failed for another cause")
        else:
            raise AssertionError("conflicting native run mapping replaced caller correlation")


def event_ordering() -> None:
    provider = provider_module()
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve()
        spool, database = root / "spool", root / "outbox.sqlite"
        record = record_fixture()
        start = native(record, dag=True)
        complete = native(record, dag=True, state="COMPLETE", event_time="2026-10-17T01:00:02+00:00")
        require(provider.emit(spool, complete) and provider.emit(spool, start), "ordered spool setup failed")
        lineage.collect_provider(spool, database, limit=1)
        with lineage.Outbox(database) as box:
            require([json.loads(body)["eventType"] for _, body in box.pending(100)] == ["START"], "bounded provider collection queued terminal before START")
        lineage.collect_provider(spool, database, limit=1)
        with lineage.Outbox(database) as box:
            require([json.loads(body)["eventType"] for _, body in box.pending(100)] == ["START", "COMPLETE"], "provider queue lost chronological order")
        with lineage.Outbox(root / "reversed.sqlite") as box:
            box.enqueue_provider(provider.project(complete))
            box.enqueue_provider(provider.project(start))
            require([json.loads(body)["eventType"] for _, body in box.pending(100)] == ["START", "COMPLETE"], "outbox insertion order reversed run lifecycle")


def declared_retention() -> None:
    provider = provider_module()
    for late_start in (False, True):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            records, spool, database = root / "records", root / "spool", root / "outbox.sqlite"
            records.mkdir()
            record = record_fixture()
            (records / "one.json").write_text(json.dumps(record))
            if not late_start:
                require(provider.emit(spool, native(record)), "native START setup failed")
            require(provider.emit(spool, native(record, state="COMPLETE", event_time="2026-10-17T02:00:00+00:00")), "native terminal setup failed")
            worker = lineage.CompletionWorker(records, spool, database, port=None)
            for _ in range(3):
                worker.run_once()
            with lineage.Outbox(database) as box:
                events = [json.loads(body) for _, body in box.pending(100)]
                task = [event for event in events if event["run"]["runId"] == NATIVE_RUN]
                require(
                    task and task[-1]["inputs"] == record["lineage"]["inputs"] and task[-1]["outputs"] == record["lineage"]["outputs"],
                    "native callback erased declared dataset edges",
                )
                with box.connection:
                    box.connection.execute("UPDATE events SET acked=1")
            if late_start:
                require(provider.emit(spool, native(record)), "late native START setup failed")
                for _ in range(3):
                    worker.run_once()
                with lineage.Outbox(database) as box:
                    require(box.pending(100) == [], "late native START regressed an acknowledged terminal run")
            require(all(json.loads(path.read_text())["event"]["outputs"] == [] for path in spool.glob("*.json")), "provider spool gained dataset authority")
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve()
        records, spool, database = root / "records", root / "spool", root / "outbox.sqlite"
        records.mkdir()
        for name in ("b.json", "c.json"):
            (records / name).write_text("{}")
        lineage.sweep(records, database, limit=1, require_provider=True)
        record = record_fixture()
        (records / "a.json").write_text(json.dumps(record))
        require(provider.emit(spool, native(record, state="COMPLETE", event_time="2026-10-17T02:00:00+00:00")), "mid-traversal callback setup failed")
        worker = lineage.CompletionWorker(records, spool, database, port=None, limit=1)
        worker.run_once()
        with lineage.Outbox(database) as box:
            require(box.pending(100) == [], "callback escaped before a complete record traversal")
        for _ in range(5):
            worker.run_once()
        with lineage.Outbox(database) as box:
            events = [json.loads(body) for _, body in box.pending(100)]
        require(events and events[-1]["outputs"] == record["lineage"]["outputs"], "bounded traversal lost the completed task metadata")


def sql_deadline() -> None:
    with tempfile.TemporaryDirectory() as folder, lineage.Outbox(Path(folder) / "outbox.sqlite") as box:
        try:
            with box.queries_until(time.monotonic() + 0.001):
                box.connection.execute("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000) SELECT sum(x) FROM n").fetchone()
        except sqlite3.OperationalError as error:
            require("interrupted" in str(error), "SQL deadline failed for an unrelated cause")
        else:
            raise AssertionError("outbox query ignored its absolute deadline")
        require(box.connection.execute("SELECT 1").fetchone()[0] == 1, "SQL deadline leaked into the next operation")


def automatic_collection() -> None:
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve()
        paths = BrokerPaths(Registry(root / "registry", "test"), root)
        platform = {"listeners": {"airflow_api": {"port": 18080}, "broker": {"port": 18790, "host": "127.0.0.1"}}}
        subject = supervisor.Supervisor(paths, platform, MagicMock())
        paths.records.mkdir()
        (subject.state / "lineage_provider").mkdir(parents=True)
        record = record_fixture()
        (paths.records / "one.json").write_text(json.dumps(record))
        with (
            patch.object(subject, "stack", return_value={"phase": "down"}),
            patch.object(subject, "check_images", return_value={}),
            patch.object(subject, "prepare_state"),
            patch.object(subject, "call", return_value={}),
            patch.object(subject, "wait_for_stack", side_effect=lambda phases, _seconds: {"phase": phases[0]}),
            patch.object(subject, "ready", return_value={}),
            patch.object(supervisor, "Broker"),
            patch.object(supervisor, "Listener"),
        ):
            try:
                subject.start()
                worker = getattr(subject, "emitter", None)
                if worker is None:
                    raise AssertionError("supervisor did not start automatic lineage collection")
                provider = provider_module()
                require(provider.emit(subject.state / "lineage_provider", native(record)), "automatic collector fixture setup failed")
                worker.wake()
                deadline = time.monotonic() + 3
                collected = False
                while time.monotonic() < deadline:
                    if worker.outbox.exists():
                        with lineage.Outbox(worker.outbox) as box:
                            collected = len(box.pending(100)) >= 3
                        if collected:
                            break
                    worker.wake()
                    time.sleep(0.01)
                require(collected, "automatic collection did not queue finished broker records")
            finally:
                require(not subject.teardown().get("errors"), "supervisor did not stop its emitter")
        require(not any(thread.name == "lineage-outbox" for thread in threading.enumerate()), "lineage worker remained alive")


def independent_delivery() -> None:
    require(hasattr(lineage, "CompletionWorker"), "bounded independent lineage worker is not implemented")
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve()
        (root / "records").mkdir()
        (root / "provider").mkdir()
        entered, release = threading.Event(), threading.Event()

        def stalled(*_args: Any, **_kwargs: Any) -> dict[str, int]:
            entered.set()
            require(release.wait(1), "synthetic delivery exceeded its deadline")
            return {"delivered": 0, "pending": 0}

        worker = lineage.CompletionWorker(root / "records", root / "provider", root / "outbox.sqlite", port=19009, interval_seconds=0.02, pass_seconds=1)
        with patch.object(lineage.Outbox, "deliver", stalled):
            worker.start()
            try:
                require(entered.wait(2), "independent delivery never reached its transport")
                require(worker.thread.is_alive(), "delivery ran inline instead of in the worker")
            finally:
                release.set()
                worker.stop()
        require(not worker.thread.is_alive(), "independent delivery did not stop")
        worker = lineage.CompletionWorker(root / "records", root / "provider", root / "recovery.sqlite", port=None, interval_seconds=0.02, pass_seconds=1)
        original = worker.run_once
        attempts = 0

        def recovering() -> dict[str, int]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError(SENTINEL)
            return original()

        with patch.object(worker, "run_once", side_effect=recovering):
            worker.start()
            try:
                deadline = time.monotonic() + 1
                while time.monotonic() < deadline and "queued" not in worker.outcome:
                    time.sleep(0.001)
            finally:
                worker.stop()
        require(worker.failures == 1 and "queued" in worker.outcome and SENTINEL not in json.dumps(worker.outcome), "worker did not recover safely")


@contextmanager
def standin_marquez() -> Iterator[dict[str, Any]]:
    """Record real HTTP bytes and count modeled logical rows without claiming native Marquez semantics."""
    state: dict[str, Any] = {"available": True, "attempts": [], "received": [], "runs": [], "terminals": [], "edges": [], "handler_errors": []}

    def store_row(table: str, row: tuple[str, ...]) -> None:
        if row not in state[table]:
            state[table].append(row)

    class Receiver(BaseHTTPRequestHandler):
        timeout = 1.0

        def log_message(self, _format: str, *args: Any) -> None:
            """Keep synthetic HTTP requests out of the console."""

        def do_POST(self) -> None:
            """Record assertion failures for the main case and return a visible HTTP failure."""
            try:
                require(self.path == "/api/v1/lineage", "delivery used another HTTP route")
                body = self.rfile.read(int(self.headers["Content-Length"]))
                state["attempts"].append(body)
                if state["available"]:
                    event = json.loads(body)
                    key = (event["job"]["namespace"], event["job"]["name"], event["run"]["runId"])
                    state["received"].append(body)
                    store_row("runs", key)
                    if event["eventType"] in ("COMPLETE", "FAIL", "ABORT"):
                        store_row("terminals", (*key, event["eventType"]))
                    for direction in ("inputs", "outputs"):
                        for dataset in event[direction]:
                            store_row("edges", (*key, direction, dataset["namespace"], dataset["name"]))
                status = 200 if state["available"] else 503
            except AssertionError as error:
                state["handler_errors"].append(str(error))
                status = 500
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = HTTPServer(("127.0.0.1", 0), Receiver)
    server.timeout = 0.1
    stopping = threading.Event()
    deadline = time.monotonic() + 15

    def serve() -> None:
        while not stopping.is_set() and time.monotonic() < deadline:
            server.handle_request()

    thread = threading.Thread(target=serve, name="lineage_http_standin")
    state["port"] = server.server_port
    thread.start()
    try:
        yield state
    finally:
        stopping.set()
        thread.join(timeout=2)
        server.server_close()
        require(not thread.is_alive(), "stand-in HTTP server exceeded its shutdown deadline")
        require(not state["handler_errors"], "stand-in handler assertion failed: " + " | ".join(state["handler_errors"]))


def spool_snapshot(spool: Path) -> dict[str, tuple[int, int, int, bytes]]:
    """Capture published inode identity, permissions and exact immutable bytes."""
    return {path.name: (path.stat().st_ino, stat.S_IMODE(path.stat().st_mode), path.stat().st_mtime_ns, path.read_bytes()) for path in spool.glob("*.json")}


@contextmanager
def controller_group_reader(spool: Path) -> Iterator[None]:
    """Model group-only read access while exercising the actual collector's filesystem calls."""
    opened = os.open
    spool_identity = (spool.stat().st_dev, spool.stat().st_ino)

    def read_only(path: Any, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        parent = os.fstat(dir_fd) if dir_fd is not None else None
        inside = parent is not None and (parent.st_dev, parent.st_ino) == spool_identity
        shared = inside or Path(path) == spool
        if shared:
            require(flags & os.O_ACCMODE == os.O_RDONLY and not flags & (os.O_CREAT | os.O_TRUNC | os.O_APPEND), "collector requested writable spool access")
        descriptor = opened(path, flags, mode, dir_fd=dir_fd)
        metadata = os.fstat(descriptor)
        needed = 0o050 if stat.S_ISDIR(metadata.st_mode) else 0o040
        if shared and metadata.st_mode & needed != needed:
            os.close(descriptor)
            raise PermissionError("controller group cannot read the provider spool")
        return descriptor

    with patch.object(os, "open", read_only):
        yield


def spool_permissions() -> None:
    provider = provider_module()
    observations: list[dict[str, Any]] = []
    OBSERVED["spool_permissions"] = observations
    for existing in (False, True):
        for mask in (0o077, 0o027):
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder).resolve()
                spool = root / "spool"
                registry = root / "registry"
                registry.mkdir(mode=0o700)
                private = root / "private_outbox"
                with lineage.Outbox(private / "outbox.sqlite"):
                    pass
                private_modes = {path: stat.S_IMODE(path.stat().st_mode) for path in (root, registry, private, private / "outbox.sqlite")}
                if existing:
                    spool.mkdir(mode=0o700)
                previous_mask = os.umask(mask)
                try:
                    require(provider.emit(spool, native(record_fixture())), "provider publication failed under restrictive umask")
                finally:
                    os.umask(previous_mask)
                published = list(spool.glob("*.json"))
                require(len(published) == 1, "provider did not publish exactly one envelope")
                envelope = published[0]
                observations.append(
                    {
                        "existing": existing,
                        "umask": oct(mask),
                        "directory_mode": oct(stat.S_IMODE(spool.stat().st_mode)),
                        "envelope_mode": oct(stat.S_IMODE(envelope.stat().st_mode)),
                    }
                )
                require(stat.S_IMODE(spool.stat().st_mode) == 0o750, "provider spool mode must be 0750 despite umask")
                require(stat.S_IMODE(envelope.stat().st_mode) == 0o640, "published provider envelope mode must be 0640 despite umask")
                require(envelope.stat().st_uid == os.geteuid() and envelope.stat().st_gid == spool.stat().st_gid, "publication changed spool ownership")
                require(all(stat.S_IMODE(path.stat().st_mode) == mode for path, mode in private_modes.items()), "spool publication widened an unrelated path")
                before = spool_snapshot(spool)
                require(provider.emit(spool, native(record_fixture())), "identical provider replay failed")
                require(spool_snapshot(spool) == before, "identical provider replay replaced or changed an envelope")
                require(not list(spool.glob(".pending-*")), "provider left temporary spool files")


def spool_permission_failure() -> None:
    provider = provider_module()
    with tempfile.TemporaryDirectory() as folder:
        spool = Path(folder).resolve() / "spool"
        spool.mkdir(mode=0o700)
        chmod = os.fchmod
        descriptors: list[int] = []

        def denied(descriptor: int, mode: int) -> None:
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                descriptors.append(descriptor)
                raise PermissionError(SENTINEL)
            chmod(descriptor, mode)

        with patch.object(os, "fchmod", denied):
            try:
                provider.emit(spool, native(record_fixture()))
            except RuntimeError as error:
                require(type(error).__name__ == "SpoolPermissionError", "directory mode failure reported another error type")
                require("directory permissions" in str(error) and "not published" in str(error), "directory mode failure lost its intended cause")
                require(SENTINEL not in str(error), "producer error leaked the original storage exception")
            else:
                raise AssertionError("directory permission failure was not reported to the producer caller")
        require(len(descriptors) == 1, "permission failure did not reach directory fchmod exactly once")
        try:
            os.fstat(descriptors[0])
        except OSError as error:
            require(error.errno == errno.EBADF, "directory descriptor check failed for another cause")
        else:
            raise AssertionError("directory permission failure leaked its descriptor")
        require(list(spool.iterdir()) == [], "directory mode failure left an unpublished temporary event")
        require(provider.emit(spool, native(record_fixture())), "producer did not recover after directory permissions succeeded")
        require(len(list(spool.glob("*.json"))) == 1, "permission recovery lost or duplicated its event")
        OBSERVED["spool_permission_failure"] = {"error": "SpoolPermissionError", "descriptor_closed": True, "temporary_files": 0, "recovery_events": 1}


def standin_handler_failure() -> None:
    response_status: int | str = "no response"
    try:
        with standin_marquez() as server:
            connection = HTTPConnection("127.0.0.1", server["port"], timeout=1)
            try:
                connection.request("POST", "/wrong-route", body=b"{}", headers={"Content-Type": "application/json"})
                try:
                    response = connection.getresponse()
                    response_status = response.status
                    response.read()
                except OSError:
                    response_status = "connection closed"
            finally:
                connection.close()
    except AssertionError as error:
        require("delivery used another HTTP route" in str(error), "handler failure reached the main case with another cause")
        require(response_status == 500, "handler assertion did not return a visible HTTP failure")
    else:
        raise AssertionError("stand-in handler assertion did not reach the main case")
    OBSERVED["standin_handler_failure"] = {"http_status": response_status, "main_thread_error": "delivery used another HTTP route"}


def spool_legacy_replay() -> None:
    provider = provider_module()
    with tempfile.TemporaryDirectory() as folder:
        spool = Path(folder).resolve() / "spool"
        event = native(record_fixture())
        require(provider.emit(spool, event), "legacy replay setup failed")
        envelope = next(spool.glob("*.json"))
        os.chmod(envelope, 0o600)
        before = spool_snapshot(spool)
        require(provider.emit(spool, event), "identical legacy provider replay was refused")
        repaired = spool_snapshot(spool)
        require(repaired[envelope.name][1] == 0o640, "identical replay did not repair the legacy envelope to 0640")
        require(
            before[envelope.name][0] == repaired[envelope.name][0] and before[envelope.name][2:] == repaired[envelope.name][2:],
            "legacy permission repair replaced or changed immutable envelope bytes",
        )
        envelope.write_bytes(envelope.read_bytes() + b" ")
        os.chmod(envelope, 0o600)
        conflict = spool_snapshot(spool)
        require(not provider.emit(spool, event), "provider accepted conflicting legacy envelope bytes")
        require(spool_snapshot(spool) == conflict, "conflicting replay widened or changed the existing envelope")
        require(not list(spool.glob(".pending-*")), "legacy replay left a temporary spool file")
        OBSERVED["spool_legacy_replay"] = {"repaired_mode": "0o640", "inode_and_bytes_unchanged": True, "conflict_refused_without_chmod": True}


def delivery_fixture(root: Path, *, failures: bool = False) -> tuple[Path, Path, Path]:
    """Persist successful and optional failed records with independent native callbacks."""
    records, spool, database = root / "records", root / "spool", root / "private" / "outbox.sqlite"
    records.mkdir()
    provider = provider_module()
    entries = [(record_fixture(), NATIVE_RUN, "COMPLETE")]
    if failures:
        failed = record_fixture()
        failed.update(task_id="failed_build", exit_code=7)
        failed["ledger"][0]["task_id"] = failed["task_id"]
        entries.append((failed, "91ea107d-e153-4016-a12e-4c09778e8110", "FAIL"))
    for index, (record, run_uuid, terminal) in enumerate(entries):
        (records / f"{index}.json").write_text(json.dumps(record))
        require(provider.emit(spool, native(record, run_uuid=run_uuid)), "native START publication failed")
        require(
            provider.emit(spool, native(record, run_uuid=run_uuid, state=terminal, event_time="2026-10-17T02:00:00+00:00")),
            "native terminal publication failed",
        )
    if failures:
        provider_only = {**record_fixture(), "task_id": "provider_only"}
        require(
            provider.emit(spool, native(provider_only, run_uuid="7fb14c03-c931-44ce-9c46-cbf965d4a104", state="COMPLETE")),
            "provider-only callback publication failed",
        )
    return records, spool, database


def spool_read_only() -> None:
    with tempfile.TemporaryDirectory() as folder:
        records, spool, database = delivery_fixture(Path(folder).resolve())
        before = spool_snapshot(spool)
        blocked = next(spool.glob("*.json"))
        os.chmod(blocked, 0o600)
        worker = lineage.CompletionWorker(records, spool, database, port=None)
        with controller_group_reader(spool):
            try:
                worker.run_once()
            except PermissionError as error:
                require("controller group" in str(error), "permission case failed for another cause")
            else:
                raise AssertionError("unreadable provider envelope did not withhold completion")
        with lineage.Outbox(database) as box:
            require(box.pending(100) == [], "permission failure allowed completion into the outbox")
        os.chmod(blocked, 0o640)
        with controller_group_reader(spool):
            for _ in range(3):
                worker.run_once()
        require(spool_snapshot(spool) == before, "read-only controller collection changed published envelopes")
        with lineage.Outbox(database) as box:
            require(len(box.pending(100)) == 4, "read-only collection did not recover both native and completion events")
        require(
            stat.S_IMODE(database.stat().st_mode) == 0o600 and stat.S_IMODE(database.parent.stat().st_mode) == 0o700,
            "controller outbox permissions were widened",
        )
        OBSERVED["spool_read_only"] = {"permission_failure": "PermissionError", "pending_after_recovery": 4, "spool_unchanged": True, "outbox_mode": "0o600"}


def graph_observation(server: dict[str, Any]) -> dict[str, Any]:
    """Export only synthetic request fingerprints and the stand-in's logical graph."""
    return {
        "http_attempts": len(server["attempts"]),
        "accepted_sha256": [hashlib.sha256(body).hexdigest() for body in server["received"]],
        **{key: sorted(server[key]) for key in ("runs", "terminals", "edges")},
    }


def lost_ack_replay() -> None:
    with tempfile.TemporaryDirectory() as folder, standin_marquez() as server:
        records, spool, database = delivery_fixture(Path(folder).resolve())
        worker = lineage.CompletionWorker(records, spool, database, port=None)
        for _ in range(3):
            worker.run_once()
        with lineage.Outbox(database) as box:
            original = dict(box.pending(100))
            require(len(original) == 4, "replay setup did not collect the expected events")
            with box.connection:
                box.connection.execute(
                    "CREATE TRIGGER lose_ack BEFORE UPDATE OF acked ON events "
                    "WHEN NEW.acked=1 AND json_extract(NEW.payload,'$.eventType')='COMPLETE' "
                    "BEGIN SELECT RAISE(ABORT, 'injected lost local acknowledgment'); END"
                )
        worker.port = server["port"]
        try:
            worker.run_once()
        except sqlite3.IntegrityError as error:
            require("injected lost local acknowledgment" in str(error), "lost acknowledgment failed for another cause")
        else:
            raise AssertionError("the accepted event did not reach the local acknowledgment failure")
        require(
            server["received"] and json.loads(server["received"][-1])["eventType"] == "COMPLETE",
            "HTTP 200 was not accepted before the local acknowledgment failed",
        )
        lost = server["received"][-1]
        with lineage.Outbox(database) as box:
            pending = dict(box.pending(100))
            require(lost.decode() in pending.values(), "accepted event was lost after local acknowledgment rollback")
            require(all(original[key] == value for key, value in pending.items()), "restart changed durable pending bytes")
            with box.connection:
                box.connection.execute("DROP TRIGGER lose_ack")
        restarted = lineage.CompletionWorker(records, spool, database, port=server["port"])
        restarted.run_once()
        inventory = tuple(Counter(server[key]) for key in ("runs", "terminals", "edges"))
        restarted.run_once()
        with lineage.Outbox(database) as box:
            rows = dict(box.connection.execute("SELECT id,payload FROM events"))
            require(box.pending(100) == [] and rows == original, "acknowledged replay lost, duplicated or changed a durable event")
        counts = Counter(server["received"])
        require(
            counts == Counter({body.encode(): 2 if body.encode() == lost else 1 for body in original.values()}),
            "lost acknowledgment resend changed exact HTTP bytes or duplicated another event",
        )
        expected_key = (lineage.JOB_NAMESPACE, "silver_transform_dag.silver_build", NATIVE_RUN)
        require(Counter(server["runs"]) == Counter([expected_key]), "replay duplicated a logical run")
        require(Counter(server["terminals"]) == Counter([(*expected_key, "COMPLETE")]), "replay duplicated a terminal state")
        expected_edges = [(*expected_key, direction, "file", f"fixture/{layer}.duckdb") for direction, layer in (("inputs", "bronze"), ("outputs", "silver"))]
        require(
            Counter(server["edges"]) == Counter(expected_edges) and inventory == tuple(Counter(server[key]) for key in ("runs", "terminals", "edges")),
            "replay duplicated, erased or changed declared dataset edges",
        )
        OBSERVED["lost_ack_replay"] = {**graph_observation(server), "pending": 0, "durable_events": len(original), "lost_ack_resend_count": counts[lost]}


def outage_recovery() -> None:
    with tempfile.TemporaryDirectory() as folder, standin_marquez() as server:
        records, spool, database = delivery_fixture(Path(folder).resolve(), failures=True)
        worker = lineage.CompletionWorker(records, spool, database, port=None)
        for _ in range(3):
            worker.run_once()
        before = spool_snapshot(spool)
        with lineage.Outbox(database) as box:
            pending = dict(box.pending(100))
        require(len(pending) == 9, "outage setup did not collect success, failure and provider-only events")
        server["available"] = False
        for _ in range(2):
            worker = lineage.CompletionWorker(records, spool, database, port=server["port"])
            outcome = worker.run_once()
            require(outcome["delivered"] == 0 and outcome["pending"] == len(pending), "Marquez outage acknowledged or lost a pending event")
            with lineage.Outbox(database) as box:
                require(dict(box.pending(100)) == pending, "worker restart during outage changed pending bytes")
        require(len(server["attempts"]) == 2 and not server["received"], "outage did not reach the HTTP endpoint or accepted an event")
        server["available"] = True
        worker = lineage.CompletionWorker(records, spool, database, port=server["port"])
        require(worker.run_once()["pending"] == 0, "restored Marquez did not drain the durable outbox")
        worker.run_once()
        success = (lineage.JOB_NAMESPACE, "silver_transform_dag.silver_build", NATIVE_RUN)
        failure = (lineage.JOB_NAMESPACE, "silver_transform_dag.failed_build", "91ea107d-e153-4016-a12e-4c09778e8110")
        provider_only = (lineage.JOB_NAMESPACE, "silver_transform_dag.provider_only", "7fb14c03-c931-44ce-9c46-cbf965d4a104")
        require(
            Counter(server["runs"]) == Counter([success, failure, provider_only]),
            "provider and completion records created competing or duplicate task identities",
        )
        require(
            Counter(server["terminals"]) == Counter([(*success, "COMPLETE"), (*failure, "FAIL"), (*provider_only, "COMPLETE")]),
            "outage recovery lost or changed a terminal state",
        )
        expected_edges = [
            (*success, "inputs", "file", "fixture/bronze.duckdb"),
            (*success, "outputs", "file", "fixture/silver.duckdb"),
            (*failure, "inputs", "file", "fixture/bronze.duckdb"),
        ]
        require(
            Counter(server["edges"]) == Counter(expected_edges),
            "outage recovery duplicated or erased declared edges or published successful outputs for a failure",
        )
        require(Counter(server["received"]) == Counter(body.encode() for body in pending.values()), "outage recovery changed or duplicated HTTP event bytes")
        require(spool_snapshot(spool) == before, "outage recovery rewrote the read-only provider spool")
        OBSERVED["outage_recovery"] = {**graph_observation(server), "pending_during_outage": len(pending), "pending_after_recovery": 0, "spool_unchanged": True}


CASES = {
    "projection": projection,
    "correlation": correlation,
    "event_ordering": event_ordering,
    "declared_retention": declared_retention,
    "sql_deadline": sql_deadline,
    "automatic_collection": automatic_collection,
    "independent_delivery": independent_delivery,
    "spool_permissions": spool_permissions,
    "spool_permission_failure": spool_permission_failure,
    "standin_handler_failure": standin_handler_failure,
    "spool_legacy_replay": spool_legacy_replay,
    "spool_read_only": spool_read_only,
    "lost_ack_replay": lost_ack_replay,
    "outage_recovery": outage_recovery,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    parser.add_argument(
        "--mutation",
        choices=("projection", "correlation", "retry", "recovery", "sql_deadline"),
        help="a deliberate contract violation for failing-first evidence",
    )
    args = parser.parse_args()
    output = ROOT / "data/e2e/lineage_completion" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    sources = [
        ROOT / path
        for path in (
            "orchestration/lineage.py",
            "scripts/orchestration/run_lineage_e2e.py",
            "scripts/orchestration/supervisor.py",
            "scripts/orchestration/run_lineage_completion_e2e.py",
            "services/lineage/provider_events.py",
            "services/lineage/provider_transport.py",
            "services/lineage/delivery.json",
            "services/lineage/openlineage.yml",
        )
    ]
    before = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    results = []
    with ExitStack() as mutations:
        if args.mutation == "projection":
            provider = provider_module()
            original_project = provider.project

            def leaking(event: Any) -> dict[str, Any]:
                envelope = original_project(event)
                envelope["event"]["inputs"] = [{"namespace": SENTINEL, "name": SENTINEL}]
                return envelope

            mutations.enter_context(patch.object(provider, "project", leaking))
        elif args.mutation == "retry":
            provider = provider_module()
            original_key = provider.caller_key
            mutations.enter_context(patch.object(provider, "caller_key", lambda caller: original_key({**caller, "attempt": 1})))
        elif args.mutation == "correlation":
            original_link = lineage.Outbox.provider_run

            def unrelated_run(box: lineage.Outbox, request: dict[str, Any]) -> dict[str, Any] | None:
                link = original_link(box, request)
                return {**link, "run_id": str(uuid.UUID("60100754-d9f9-4d7e-ad83-f19201c06689"))} if link is not None else None

            mutations.enter_context(patch.object(lineage.Outbox, "provider_run", unrelated_run))
        elif args.mutation == "sql_deadline":
            mutations.enter_context(patch.object(lineage.Outbox, "queries_until", lambda *_args, **_kwargs: nullcontext()))
        elif args.mutation == "recovery":

            def no_retry(worker: Any) -> None:
                try:
                    worker.outcome = worker.run_once()
                except OSError:
                    worker.failures += 1
                    worker.outcome = {"error": "OSError"}

            mutations.enter_context(patch.object(lineage.CompletionWorker, "run", no_retry))
        for name in args.case or CASES:
            try:
                CASES[name]()
            except (AssertionError, OSError, ValueError, RuntimeError, AttributeError, TypeError, sqlite3.Error) as error:
                results.append({"case": name, "pass": False, "error": f"{type(error).__name__}: {error}"})
            else:
                results.append({"case": name, "pass": True})
    require(before == {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}, "source changed during check")
    result = {
        "created_at": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.run_lineage_completion_e2e", *sys.argv[1:]],
        "environment": {"python": sys.version.split()[0], "platform": sys.platform},
        "input_fixtures": "record_fixture and fixed native callback UUIDs in the source hash inventory",
        "boundary": (
            "synthetic provider callbacks, real filesystem and SQLite, CompletionWorker and loopback HTTP stand-in; "
            "modeled group access and graph semantics; no native provider, UID boundary, read-only mount, daemon or Marquez"
        ),
        "observed": OBSERVED,
        "cases": results,
        "pass": all(row["pass"] for row in results),
        "source_sha256": before,
        "mutation": args.mutation,
        "cleanup": "no services started; owned threads joined",
    }
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "retention.json").write_text(
        json.dumps(
            {
                "class": 4,
                "purpose": "lineage completion boundary evidence",
                "created": datetime.now(UTC).isoformat(),
                "end_condition": "no record cites this run",
            }
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"pass": result["pass"], "report": str(output / "report.json"), "cases": results}) + "\n")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
