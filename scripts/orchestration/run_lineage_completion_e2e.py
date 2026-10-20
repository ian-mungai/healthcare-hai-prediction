"""Check provider projection, caller correlation and supervisor collection without services or native provider packages."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from contextlib import ExitStack, nullcontext
from datetime import UTC, datetime
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


CASES = {
    "projection": projection,
    "correlation": correlation,
    "event_ordering": event_ordering,
    "declared_retention": declared_retention,
    "sql_deadline": sql_deadline,
    "automatic_collection": automatic_collection,
    "independent_delivery": independent_delivery,
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
            except (AssertionError, OSError, ValueError, AttributeError, TypeError, sqlite3.Error) as error:
                results.append({"case": name, "pass": False, "error": f"{type(error).__name__}: {error}"})
            else:
                results.append({"case": name, "pass": True})
    require(before == {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}, "source changed during check")
    result = {
        "boundary": "in-process projection and supervisor lifecycle with synthetic provider objects; no native provider, daemon or Marquez",
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
