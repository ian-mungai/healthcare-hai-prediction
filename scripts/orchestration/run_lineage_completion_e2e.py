"""Check the provider projection that the Airflow host loads, without services or native provider packages."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
import sys
import tempfile
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
NATIVE_RUN = "13d9e9ef-93b2-4efc-90d5-066b1becf67d"
NATIVE_DAG = "3589fe84-f7d0-49fe-b456-5fa256a72938"
SENTINEL = "synthetic-credential-not-for-lineage"
JOB_NAMESPACE = "hai-prediction-local"
RUN = "20261017T010000Z-1234abcd"
IDENTITY = {"dag_id": "silver_transform_dag", "task_id": "silver_build", "dag_run_id": "manual__2026-10-17T01:00:00+00:00"}
ENVELOPE = {
    "version": 1,
    "inputs": [{"namespace": "file", "name": "fixture/bronze.duckdb"}],
    "outputs": [{"namespace": "file", "name": "fixture/silver.duckdb"}],
}


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
    parent = SimpleNamespace(run=SimpleNamespace(runId=parent_uuid), job=SimpleNamespace(namespace=JOB_NAMESPACE, name=caller["dag_id"]))
    return SimpleNamespace(
        eventType=SimpleNamespace(value=state),
        eventTime=event_time,
        run=SimpleNamespace(
            runId=parent_uuid if dag else run_uuid, facets={"airflowDagRun" if dag else "airflow": metadata, "parent": parent, "errorMessage": SENTINEL}
        ),
        job=SimpleNamespace(namespace=JOB_NAMESPACE, name=caller["dag_id"] if dag else f"{caller['dag_id']}.{caller['task_id']}", facets={"sql": SENTINEL}),
        inputs=[{"namespace": SENTINEL, "name": SENTINEL}],
        outputs=[{"namespace": SENTINEL, "name": SENTINEL}],
        producer=SENTINEL,
    )


def record_fixture() -> dict[str, Any]:
    return {
        "request_version": 2,
        "submission_id": f"{RUN}:silver_build:fixture:1",
        "template": "silver_build",
        "run_id": RUN,
        "mode": "fixture",
        "attempt": 1,
        **IDENTITY,
        "lineage": copy.deepcopy(ENVELOPE),
        "exit_code": 0,
        "written_at": "2026-10-17T01:02:00+00:00",
        "launcher": {"id": "at-synthetic", "n": 1, "ended_at": "2026-10-17T01:01:00+00:00"},
        "ledger": [
            {
                "event": "intent",
                "at": "2026-10-17T01:00:00+00:00",
                "request_version": 2,
                "template": "silver_build",
                "run_id": RUN,
                "mode": "fixture",
                "attempt": 1,
                **IDENTITY,
                "lineage": copy.deepcopy(ENVELOPE),
            }
        ],
    }


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


CASES = {"projection": projection}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    parser.add_argument("--mutation", choices=("projection",), help="a deliberate contract violation for failing-first evidence")
    args = parser.parse_args()
    output = ROOT / "data/e2e/lineage_completion" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    sources = [ROOT / path for path in ("scripts/orchestration/run_lineage_completion_e2e.py", "services/lineage/provider_events.py")]
    before = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    results = []
    mutation: AbstractContextManager[Any]
    if args.mutation == "projection":
        provider = provider_module()
        original_project = provider.project

        def leaking(event: Any) -> dict[str, Any]:
            envelope = original_project(event)
            envelope["event"]["inputs"] = [{"namespace": SENTINEL, "name": SENTINEL}]
            return envelope

        mutation = patch.object(provider, "project", leaking)
    else:
        mutation = nullcontext()
    with mutation:
        for name in args.case or CASES:
            try:
                CASES[name]()
            except (AssertionError, OSError, ValueError, RuntimeError, AttributeError, TypeError) as error:
                results.append({"case": name, "pass": False, "error": f"{type(error).__name__}: {error}"})
            else:
                results.append({"case": name, "pass": True})
    require(before == {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}, "source changed during check")
    result = {
        "created_at": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.run_lineage_completion_e2e", *sys.argv[1:]],
        "environment": {"python": sys.version.split()[0], "platform": sys.platform},
        "boundary": "synthetic provider callbacks and the real filesystem; no native provider, daemon or Marquez",
        "cases": results,
        "pass": all(row["pass"] for row in results),
        "source_sha256": before,
        "mutation": args.mutation,
        "cleanup": "no services started",
    }
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": "lineage provider boundary evidence", "created": datetime.now(UTC).isoformat(), "end_condition": "no record cites this run"}
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"pass": result["pass"], "report": str(output / "report.json"), "cases": results}) + "\n")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
