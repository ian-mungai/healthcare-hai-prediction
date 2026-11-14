"""Cases of the lineage that starts at acquisition: the raw files an acquisition run stored and the raw files a bronze run loaded.

Both sides name a raw file the same way (``s3://<bucket>`` and the object key, with the object version as the dataset version facet), so OpenMetadata joins
the acquisition run's outputs to the bronze run's inputs. The saved acquisition states are stand-ins written by the case in a temporary folder, in the
shapes ``scripts/acquisition/batch.py`` (``completed_collection_layout.json``) and ``scripts/acquisition/s3_store.py`` (the reconciliation record) write, with
a receipt in the shape of ``config/acquisition/snapshot_contract.schema.json``. No acquisition runs and nothing reaches S3.
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import os
import re
import tempfile
import time
import traceback
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import airflow_steps, ecs_lineage, raw_lineage
from orchestration.ecs_launch import LaunchRefused, S3LaunchStore
from orchestration.ecs_launch_operator import JobFailed
from orchestration.interface import SubmissionRequest
from scripts.orchestration.ecs_launch_cases import BOUND, NEED, PREFIX, RUN, Rig, spec
from scripts.orchestration.ecs_launch_cases import BUCKET as BUCKET_LAUNCH
from scripts.orchestration.ecs_lineage_cases import CREDENTIAL, END, START, evidence_bucket, invocation, operator, receiving, refuses, require, validator
from scripts.orchestration.ecs_lineage_cases import context as gold_context

BUCKET = "example-acquisition-bucket"
SNAPSHOT = "20261101T090000Z-0a1b2c3d"
PUBLISHER_URL = "https://data.example.invalid/api/views/abcd-1234/rows.csv"
LANDING_URL = "https://data.example.invalid/dataset/abcd-1234"
OBJECTS = [
    {"bucket": BUCKET, "key": "cms/hospitals/datasets/abcd_1234/release_date=2026-10-01/hai_hospital.csv", "version_id": "v-data-1"},
    {"bucket": BUCKET, "key": "cms/hospitals/references/release_date=2026-10-01/dictionary.pdf", "version_id": "v-dict-1"},
]


def receipt() -> dict[str, Any]:
    return {
        "snapshot_id": SNAPSHOT,
        "snapshot_status": "acquired_unvalidated",
        "source": {"source_record_id": "cms_hai_hospital", "publisher": "Centers for Medicare & Medicaid Services", "official_landing_url": LANDING_URL},
        "acquisition": {"requested_url": PUBLISHER_URL, "resolved_url": PUBLISHER_URL + "?redirected=1", "retrieved_at_utc": START, "http_status": 200},
        "artifacts": [
            {"artifact_id": "a1", "role": "data", "storage_path": "files/hai_hospital.csv", "sha256": "1" * 64, "byte_count": 10},
            {"artifact_id": "a2", "role": "dictionary", "storage_path": "files/dictionary.pdf", "sha256": "2" * 64, "byte_count": 20},
        ],
    }


def reconciliation() -> dict[str, Any]:
    entries = []
    for artifact, stored in zip(receipt()["artifacts"], OBJECTS, strict=True):
        entries.append(
            {
                "storage_path": artifact["storage_path"],
                "role": artifact["role"],
                "artifact_id": artifact["artifact_id"],
                "object": {**stored, "sha256": artifact["sha256"], "byte_count": artifact["byte_count"], "verification": "version_get_sha256_and_length_match"},
            }
        )
    return {"storage_contract_version": "4.0.0", "publisher": "cms", "collection": "hospitals", "manifests": [], "snapshot_id": SNAPSHOT, "objects": entries}


def saved_batch(root: Path, change: Callable[[dict[str, Any], dict[str, Any]], None] | None = None) -> Path:
    """A batch state root with one completed job, as batch.py leaves it."""
    job = root / "jobs" / "job_cms_hai"
    job.mkdir(parents=True)
    body, record = receipt(), reconciliation()
    if change is not None:
        change(body, record)
    (job / "receipt.json").write_text(json.dumps(body))
    (job / "reconciliation.json").write_text(json.dumps(record))
    state = {
        "job_id": "job_cms_hai",
        "status": "stored_unvalidated",
        "receipt_path": str(job / "receipt.json"),
        "reconciliation_path": str(job / "reconciliation.json"),
    }
    (job / "completed_collection_layout.json").write_text(json.dumps(state))
    return root


def pair(items: list[dict[str, Any]]) -> list[tuple[str, str, str]]:
    return [(item["namespace"], item["name"], item["facets"]["version"]["datasetVersion"]) for item in items]


def names_match_on_both_sides() -> None:
    """The acquisition output and the bronze input of one stored object are the same dataset, with the object version as its version facet."""
    with tempfile.TemporaryDirectory() as folder:
        stored = raw_lineage.acquisition_events(saved_batch(Path(folder)), "job_cms_hai")[1]["outputs"]
    loaded = raw_lineage.bronze_inputs(OBJECTS)
    require(pair(stored) == pair(loaded), f"acquisition {pair(stored)} and bronze {pair(loaded)} name the files differently")
    require(raw_lineage.raw_file(BUCKET, OBJECTS[0]["key"]) == {"namespace": f"s3://{BUCKET}", "name": OBJECTS[0]["key"]}, "the raw file naming")
    refuses(ecs_lineage.LineageRefused, lambda: raw_lineage.raw_file("Bad_Bucket", "x"))
    refuses(ecs_lineage.LineageRefused, lambda: raw_lineage.raw_file(BUCKET, "../escape"))


def acquisition_events_are_valid() -> None:
    """START and COMPLETE of the acquisition job: every stored file is an output with its version and the receipt's publisher address as a facet."""
    check = validator()
    with tempfile.TemporaryDirectory() as folder:
        events = raw_lineage.acquisition_events(saved_batch(Path(folder)), "job_cms_hai")
    require([event["eventType"] for event in events] == ["START", "COMPLETE"], f"types {[event['eventType'] for event in events]}")
    for event in events:
        problems = [problem for problem in check(event) if not problem.startswith("haiLaunch")]
        require(not problems, f"{event['eventType']}: {problems[:3]}")
        raw_lineage.check_event(event)
    outputs = events[1]["outputs"]
    require(len(outputs) == 2 and events[0]["outputs"] == [], "COMPLETE names every stored file and START none")
    receipt_facet = outputs[0]["facets"]["haiReceipt"]
    require(receipt_facet["publisherUrl"] == PUBLISHER_URL + "?redirected=1", f"the publisher address is the resolved URL: {receipt_facet}")
    require(receipt_facet["requestedUrl"] == PUBLISHER_URL and receipt_facet["landingUrl"] == LANDING_URL, f"receipt facet {receipt_facet}")
    require(receipt_facet["sha256"] == "1" * 64 and receipt_facet["role"] == "data", f"receipt facet {receipt_facet}")
    require(events[1]["inputs"] == [], "a publisher address is a facet, not an input dataset OpenMetadata would report as unresolved")


def acquisition_refusals() -> None:
    """A state whose receipt and reconciliation disagree, or that names an unverified object, is refused by name."""
    cases: list[tuple[Callable[[dict[str, Any], dict[str, Any]], None], str]] = [
        (lambda body, record: record.update(snapshot_id="20261101T090000Z-ffffffff"), "another snapshot"),
        (lambda body, record: record["objects"][0]["object"].update(sha256="9" * 64), "differs from its receipt artifact"),
        (lambda body, record: record["objects"][0]["object"].update(verification="none"), "not verified"),
        (lambda body, record: record["objects"][0]["object"].pop("version_id"), "version"),
        (lambda body, record: body["acquisition"].update(resolved_url=None, requested_url="ftp://x"), "publisher address"),
    ]
    for change, phrase in cases:
        with tempfile.TemporaryDirectory() as folder:
            root = saved_batch(Path(folder), change)
            error = refuses(ecs_lineage.LineageRefused, lambda root=root: raw_lineage.acquisition_events(root, "job_cms_hai"))
        require("job_cms_hai" in str(error) and phrase in str(error), f"refusal must name the job and '{phrase}': {error}")
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        (root / "jobs" / "job_cms_hai").mkdir(parents=True)
        error = refuses(ecs_lineage.LineageRefused, lambda: raw_lineage.acquisition_events(root, "job_cms_hai"))
        require("not completed" in str(error), f"a job without its completed state: {error}")


def acquisition_replay_is_stable() -> None:
    """The replay of a saved batch gives the same bytes every time, one line per event, in job order, and changes nothing in the state folder."""
    with tempfile.TemporaryDirectory() as folder:
        root = saved_batch(Path(folder))
        before = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
        first, second = raw_lineage.replay_acquisition(root), raw_lineage.replay_acquisition(root)
        after = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    require(first == second and len(first) == 2, "two replays give the same lines")
    require(before == after, "a replay writes nothing")
    require([json.loads(line)["eventType"] for line in first] == ["START", "COMPLETE"], "the lines are the job's events in order")


BRONZE_RUN = "20261104T100000Z-abcdef01"
RECORD_KEY = f"lakehouse/bronze_runs/{BRONZE_RUN}/check.json"


def check_record(**change: Any) -> dict[str, Any]:
    """The bronze run record ``lakehouse/bronze_runs/<run>/check.json`` as the collapse branch writes it, with the proposed ``objects`` list."""
    record: dict[str, Any] = {
        "schema_version": 1,
        "run_id": BRONZE_RUN,
        "target": "real",
        "namespaces": ["bronze", "bronze_dictionary"],
        "tables": ["cms_hai_hospital"],
        "report_sha256": "d" * 64,
        "metadata_generation": "generation-0001",
        "snapshots": {"cms_hai_hospital": 51},
        "objects": [{"table": "cms_hai_hospital", **item, "sha256": str(index) * 64, "byte_count": 10 + index} for index, item in enumerate(OBJECTS, 1)],
    }
    record.update(change)
    return record


def bronze_bucket(record: Any) -> Any:
    bucket = Rig().bucket
    bucket.allowed_prefix = ""
    bucket.objects[RECORD_KEY] = record if isinstance(record, bytes) else json.dumps(record).encode()
    return bucket


def bronze_built(record: Any, outcome: str = "succeeded") -> list[dict[str, Any]]:
    declared = ecs_lineage.declared_lineage("bronze")
    facts = ecs_lineage.LaunchFacts(BRONZE_RUN, "bronze", 1, START, END, outcome, "latest")
    return raw_lineage.bronze_run_events(ecs_lineage.S3Evidence(bronze_bucket(record), BUCKET_LAUNCH), declared, invocation("bronze"), facts)


def two_table_record() -> dict[str, Any]:
    """A run that loaded two tables, one object each."""
    objects = [
        {"table": "cms_hai_hospital", **OBJECTS[0], "sha256": "1" * 64, "byte_count": 11},
        {"table": "cms_hai_state", **OBJECTS[1], "sha256": "2" * 64, "byte_count": 12},
    ]
    return check_record(tables=["cms_hai_hospital", "cms_hai_state"], snapshots={"cms_hai_hospital": 51, "cms_hai_state": 52}, objects=objects)


def bronze_events_name_the_loaded_files() -> None:
    """Each table a bronze run loaded is its own run, a child of the step's run: only that table's objects in, only that table out, with its snapshot.
    A table the run did not load is claimed by no event, and a repeat gives the same identities."""
    check = validator()
    events = bronze_built(two_table_record())
    for event in events:
        problems = check(event)
        require(not problems, f"bronze {event['eventType']}: {problems[:3]}")
    complete = {event["job"]["name"].rsplit(".", 1)[1]: event for event in events if event["eventType"] == "COMPLETE"}
    require(sorted(complete) == ["cms_hai_hospital", "cms_hai_state"], f"one run per loaded table: {sorted(complete)}")
    for table, stored, snapshot in (("cms_hai_hospital", OBJECTS[0], "51"), ("cms_hai_state", OBJECTS[1], "52")):
        event = complete[table]
        require(pair(event["inputs"]) == pair(raw_lineage.bronze_inputs([stored])), f"{table}: only its own objects are inputs: {pair(event['inputs'])}")
        outputs = [(item["name"], item["facets"]["version"]["datasetVersion"]) for item in event["outputs"]]
        require(outputs == [(f"hai_lakehouse.hai_mirror_bronze.{table}", snapshot)], f"{table}: only its own table is an output: {outputs}")
        require(event["run"]["facets"]["haiLaunch"]["snapshotState"] == "complete", f"{table}: {event['run']['facets']['haiLaunch']}")
    parents = {event["run"]["facets"]["parent"]["run"]["runId"] for event in events}
    runs = {event["run"]["runId"] for event in events}
    require(len(parents) == 1 and len(runs) == 2 and not parents & runs, "the table runs are distinct children of the step's run")
    require(bronze_built(two_table_record()) == events, "a repeat gives the same events and identities")
    tables = {(item["namespace"], item["name"]) for event in events for item in event["outputs"]}
    require(
        {namespace for namespace, _ in tables} == {"iceberg://glue_mirror"},
        f"a bronze table is a table of the glue_mirror service, in its Glue database hai_mirror_bronze: {tables}",
    )
    claimed = {item["name"] for event in events for item in event["outputs"]}
    require(
        claimed == {"hai_lakehouse.hai_mirror_bronze.cms_hai_hospital", "hai_lakehouse.hai_mirror_bronze.cms_hai_state"},
        f"no unloaded table is claimed: {claimed}",
    )
    failed = bronze_built(two_table_record(), outcome="failed")
    require([event["eventType"] for event in failed] == ["START", "FAIL"], f"a failed run has one step run: {failed}")
    require(all(event["inputs"] == [] and event["outputs"] == [] for event in failed), "a failed bronze run claims no file and no table")


def bronze_record_refusals() -> None:
    """A run record that is missing, unreadable or wrong is refused by its key and the rule."""
    first = OBJECTS[0]
    cases: list[tuple[Any, str]] = [
        (check_record(run_id="20261104T100000Z-ffffffff"), "another run"),
        (check_record(target="qualify"), "real target"),
        (check_record(objects=[]), "no loaded object"),
        (check_record(objects=[{"table": "cms_hai_hospital", **first, "sha256": "1" * 64, "byte_count": 1}] * 2), "listed twice"),
        (check_record(objects=[{"table": "other_table", **first, "sha256": "1" * 64, "byte_count": 1}]), "not a requested table"),
        (check_record(objects=[{"table": "cms_hai_hospital", **first, "version_id": "", "sha256": "1" * 64, "byte_count": 1}]), "version"),
        (check_record(objects=[{"table": "cms_hai_hospital", **first}]), "exactly the fields"),
        (check_record(snapshots={"cms_hai_hospital": "51"}), "snapshot"),
        (
            check_record(
                tables=["not_declared_table"],
                snapshots={"not_declared_table": 5},
                objects=[{"table": "not_declared_table", **first, "sha256": "1" * 64, "byte_count": 1}],
            ),
            "not declared",
        ),
        (b'{"schema_version": 1, "schema_version": 1}', "duplicate"),
    ]
    for record, phrase in cases:
        error = refuses(ecs_lineage.LineageRefused, lambda record=record: bronze_built(record))
        require(RECORD_KEY in str(error) and phrase in str(error), f"refusal must name {RECORD_KEY} and '{phrase}': {error}")
    bucket = bronze_bucket(check_record())
    bucket.objects.pop(RECORD_KEY)
    declared = ecs_lineage.declared_lineage("bronze")
    facts = ecs_lineage.LaunchFacts(BRONZE_RUN, "bronze", 1, START, END, "succeeded", "latest")
    error = refuses(
        ecs_lineage.LineageRefused, lambda: raw_lineage.bronze_run_events(ecs_lineage.S3Evidence(bucket, BUCKET_LAUNCH), declared, invocation("bronze"), facts)
    )
    require("missing" in str(error), f"a succeeded run without its record is refused: {error}")
    bucket.objects[RECORD_KEY] = b"{}"
    bucket.deny_keys.add(RECORD_KEY)
    refuses(
        ecs_lineage.EvidenceUnavailable,
        lambda: raw_lineage.bronze_run_events(ecs_lineage.S3Evidence(bucket, BUCKET_LAUNCH), declared, invocation("bronze"), facts),
    )


BRONZE_ARGUMENTS = [
    *("--bucket", BUCKET_LAUNCH, "--job", "bronze", "--run-id", RUN, "--attempt", "1", "--template", "bronze_load"),
    *("--dag-id", "bronze_chain_dag", "--task-id", "bronze_load", "--dag-run-id", f"manual__{RUN}"),
]
BRONZE_REQUEST = ("bronze_load", RUN, "real", 1, "bronze_chain_dag", "bronze_load", f"manual__{RUN}")
TABLES = ("hai_lakehouse.hai_mirror_bronze.cms_hai_hospital", "hai_lakehouse.hai_mirror_bronze.cms_hai_state")


def http_on(url: str, **change: str) -> dict[str, str]:
    """The HTTP lineage settings of ``infra/services/airflow.tf`` as the task's environment has them, with the receiver's address, and ``change`` on top."""
    return {
        "OPENLINEAGE__TRANSPORT__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__URL": url,
        "OPENLINEAGE__TRANSPORT__AUTH__API_KEY": CREDENTIAL,
        **change,
    }


@contextmanager
def lineage_environment(values: dict[str, str]) -> Iterator[None]:
    """The process environment with the OpenLineage transport variables given and no other."""
    kept = {name: value for name, value in os.environ.items() if not name.startswith("OPENLINEAGE__")}
    with mock.patch.dict(os.environ, {**kept, **values}, clear=True):
        yield


def bronze_launch(record: Any, outcome: str = "succeeded") -> tuple[Rig, Any, ecs_lineage.LineageSource]:
    """A rig whose bronze job has run and holds the run record (and failed, when ``outcome`` says so), the job, and the lineage source of its step."""
    rig = Rig()
    rig.adapter.settings = replace(rig.adapter.settings, families=frozenset({"gold", "bronze"}))
    revision = rig.ecs.register("example_dev_job_bronze_host", NEED.memory_mib, None)  # the host definition revision the preflight checks for the bronze family
    rig.bucket.allowed_prefix = ""
    rig.bucket.objects[RECORD_KEY] = json.dumps(record).encode()
    job = spec(job="bronze", log_container="bronze", log_prefix="bronze", bound={**BOUND, "input_run": "latest"}, task_definition=revision)
    launch = operator(rig, None)
    launch.job = job
    if outcome == "succeeded":
        launch.execute({"ti": SimpleNamespace(dag_id="bronze_chain_dag", task_id="bronze_load", run_id=f"manual__{RUN}", map_index=-1, try_number=1)})
    else:
        rig.ecs.plan = {"gold": 3}
        refuses(
            JobFailed,
            lambda: launch.execute(
                {"ti": SimpleNamespace(dag_id="bronze_chain_dag", task_id="bronze_load", run_id=f"manual__{RUN}", map_index=-1, try_number=1)}
            ),
        )
    declared = ecs_lineage.declared_lineage("bronze")
    evidence, store = ecs_lineage.S3Evidence(rig.bucket, BUCKET_LAUNCH), S3LaunchStore(rig.bucket, BUCKET_LAUNCH, PREFIX)
    return rig, job, ecs_lineage.LineageSource("bronze", RUN, 1, invocation("bronze"), declared, evidence, store)


def hooked_operator(rig: Rig, job: Any, template: str = "bronze_load", dag: str = "bronze_chain_dag") -> Any:
    """The launch operator of a step with the lineage source that the step's factory builds, as the provider's listener reaches it."""
    request = SubmissionRequest(template, RUN, "real", 1, dag, template, f"manual__{RUN}")
    factory = airflow_steps.lineage_factory(ecs_lineage.S3Evidence(rig.bucket, BUCKET_LAUNCH), S3LaunchStore(rig.bucket, BUCKET_LAUNCH, PREFIX))
    hooked = operator(rig, factory(request, job))
    hooked.job = job
    return hooked


def table_pairs(events: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """The input to output pairs that OpenMetadata makes of these events: it pairs every input of an event with every output of the same event."""
    return {(i["name"], o["name"]) for event in events if event["eventType"] == "COMPLETE" for i in event["inputs"] for o in event["outputs"]}


def bronze_job_lineage_is_the_run_record() -> None:
    """The job's events and its replay come from the bronze run record: a record that evidences one table gives that table only, with its raw files in and
    its snapshot out, not the hundreds of tables the declared lineage names. The task's own live event names no bronze dataset, only its run facet. A
    missing record is refused, so the record is read from S3."""
    rig, _, found = bronze_launch(check_record())
    require(len(found.declared["outputs"]) > 1, "the declared bronze lineage names every bronze table")
    check = validator()
    built = found.events()
    for event in built:
        require(not check(event), f"bronze {event['eventType']}: {check(event)[:3]}")
    complete = [event for event in built if event["eventType"] == "COMPLETE" and "parent" in event["run"]["facets"]]
    require(len(complete) == 1, f"one run for the one table the record evidences: {[event['job']['name'] for event in built]}")
    require([item["name"] for item in complete[0]["outputs"]] == [TABLES[0]], f"outputs {complete[0]['outputs']}")
    require(pair(complete[0]["inputs"]) == pair(raw_lineage.bronze_inputs(OBJECTS)), f"inputs {pair(complete[0]['inputs'])}")
    for done in (True, False):
        live = found.facets(complete=done)
        require(live["inputs"] == [] and live["outputs"] == [], f"the task's own event names no bronze dataset (complete={done}): {live}")
        require(live["run_facets"]["haiLaunch"]["snapshotState"] == "not_applicable", f"live facet {live['run_facets']}")
    require([json.loads(line) for line in ecs_lineage.replay(rig.bucket, BRONZE_ARGUMENTS, declared=found.declared)] == built, "the replay equals the events")
    rig.bucket.objects[RECORD_KEY] = json.dumps(check_record(run_id="20261104T100000Z-ffffffff")).encode()
    refused = found.facets()
    require(refused["outputs"] == [] and refused["inputs"] == [], f"a refused record claims no dataset: {refused}")
    require(refused["run_facets"]["haiLaunch"]["snapshotState"] == "refused" and RECORD_KEY in refused["run_facets"]["haiLaunch"]["refusal"], f"{refused}")
    rig.bucket.objects.pop(RECORD_KEY)
    error = refuses(ecs_lineage.LineageRefused, found.events)
    require("missing" in str(error) and RECORD_KEY in str(error), f"the record is read from S3: {error}")


def bronze_hook_sends_no_table_events() -> None:
    """The bronze task's completion hook runs inside the provider's forked process, which the provider kills after its 5 second execution timeout, so it
    sends nothing: with HTTP lineage on and a succeeded run it gives the task's own facets, which name no table, and no request reaches the receiver."""
    rig, job, _ = bronze_launch(two_table_record())
    with receiving() as receiver, lineage_environment(http_on(receiver.url)):
        live = hooked_operator(rig, job).get_openlineage_facets_on_complete(None)
    require(live is not None and live["inputs"] == [] and live["outputs"] == [], f"the task's own event names no bronze dataset: {live}")
    require(live["run_facets"]["haiLaunch"]["snapshotState"] == "not_applicable", f"the task's own facet: {live['run_facets']}")
    require(not receiver.requests, f"the hook sent {len(receiver.requests)} requests inside the provider's fork")


EVIDENCE_REQUEST = SubmissionRequest("bronze_load", RUN, "real", 1, "bronze_chain_dag", "evidence", f"manual__{RUN}")
EVIDENCE_ARGUMENTS = [
    *BRONZE_ARGUMENTS[:-4],
    "--task-id",
    "job",
    *BRONZE_ARGUMENTS[-2:],
]  # the job task of the chain is the one that launched, not the evidence task


def run_evidence(rig: Rig, request: SubmissionRequest = EVIDENCE_REQUEST) -> Any:
    """The evidence task of a collapsed job DAG over the launch records of ``rig``: ``airflow_steps.evidence_step`` with the services the Airflow image has."""
    store = S3LaunchStore(rig.bucket, BUCKET_LAUNCH, PREFIX)
    found = SimpleNamespace(preflight=SimpleNamespace(store=store), evidence=ecs_lineage.S3Evidence(rig.bucket, BUCKET_LAUNCH), report=SimpleNamespace())
    accepted = {
        "report": "stand-in report"
    }  # the layer acceptance report of a gold job is another module's check; only the bronze sends are the case's subject
    with mock.patch.object(airflow_steps, "services", return_value=found), mock.patch.object(airflow_steps.layer_report, "accept", return_value=accepted):
        return airflow_steps.evidence_step(request, {}, {})


@contextmanager
def sinks() -> Iterator[Callable[[], str]]:
    """Everything the process writes to stdout, stderr (a sender thread's uncaught exception included) and any log record, as one text."""
    out, err, logs = io.StringIO(), io.StringIO(), io.StringIO()
    handler = logging.StreamHandler(logs)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    before = root.level
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            yield lambda: out.getvalue() + err.getvalue() + logs.getvalue()
    finally:
        root.removeHandler(handler)
        root.setLevel(before)


def bronze_evidence_step_sends_the_replay_events() -> None:
    """A bronze run of two tables with disjoint raw files: the evidence step, which runs outside the provider's fork, sends the per-table events, byte for
    byte the replay's, to the receiver with the bearer credential, so each table keeps exactly its own files (two pairs, not four). Its own record is the
    one it always gave."""
    rig, _, found = bronze_launch(two_table_record())
    replayed = ecs_lineage.replay(rig.bucket, EVIDENCE_ARGUMENTS, declared=found.declared)
    pairs = table_pairs([json.loads(line) for line in replayed])
    own = {
        (raw_lineage.raw_file(item["bucket"], item["key"])["name"], f"hai_lakehouse.hai_mirror_bronze.{item['table']}")
        for item in two_table_record()["objects"]
    }
    require(len(replayed) == 4 and pairs == own and {output for _, output in pairs} == set(TABLES), f"replay {len(replayed)} lines, pairs {pairs}")
    with receiving() as quiet, lineage_environment(http_on(quiet.url)):
        without = run_evidence(bronze_launch(two_table_record())[0])
    require(len(quiet.requests) == 4, f"the evidence step sent {len(quiet.requests)} events")
    with receiving() as receiver, lineage_environment(http_on(receiver.url)):
        record = run_evidence(rig)
    require(record == without and sorted(record) == ["attempt", "job", "run_id", "task_arn"] and record["job"] == "bronze", f"the evidence record: {record}")
    require([item["body"].decode() for item in receiver.requests] == replayed, "the evidence step sent the replay's events, byte for byte and in order")
    require(all(item["authorization"] == f"Bearer {CREDENTIAL}" and item["path"] == ecs_lineage.ROUTE for item in receiver.requests), "bearer and route")
    require(table_pairs([json.loads(item["body"]) for item in receiver.requests]) == pairs, "live and replay name the same table to file pairs")


def many_table_record(count: int) -> dict[str, Any]:
    """A run that loaded ``count`` declared tables, one object each."""
    declared = ecs_lineage.declared_lineage("bronze")["outputs"]
    names = [item["name"].rsplit(".", 1)[1] for item in declared][:count]
    objects = []
    for index, table in enumerate(names):
        key = f"cms/hospitals/datasets/t{index:03d}/release_date=2026-10-01/file_{index:03d}.csv"
        objects.append({"table": table, "bucket": BUCKET, "key": key, "version_id": f"v-{index:03d}", "sha256": f"{index:064x}", "byte_count": 10 + index})
    return check_record(tables=names, snapshots={table: 100 + index for index, table in enumerate(names)}, objects=objects)


def bronze_evidence_step_sends_every_table_past_the_providers_timeout() -> None:
    """The provider's execution timeout is 5 seconds (``AIRFLOW__OPENLINEAGE__EXECUTION_TIMEOUT``). A receiver that takes 0.06 s per event gets 120 events of 60
    tables in more than 7 seconds, and all of them arrive, from the evidence step; none arrives from the hook."""
    require(ecs_lineage.TABLE_EVENTS_SECONDS == 300.0, f"the total deadline is {ecs_lineage.TABLE_EVENTS_SECONDS} s")
    rig, job, found = bronze_launch(many_table_record(60))
    replayed = ecs_lineage.replay(rig.bucket, EVIDENCE_ARGUMENTS, declared=found.declared)
    require(len(replayed) == 120, f"60 tables are {len(replayed)} events")
    with receiving(delay=0.06) as receiver, lineage_environment(http_on(receiver.url)):
        hooked_operator(rig, job).get_openlineage_facets_on_complete(None)
        require(not receiver.requests, f"the hook sent {len(receiver.requests)} events")
        began = time.monotonic()
        run_evidence(rig)
        spent = time.monotonic() - began
    require(spent > 5.0, f"the sends must outlast the provider's 5 s timeout to prove the move: {spent:.2f} s")
    require([item["body"].decode() for item in receiver.requests] == replayed, f"all {len(replayed)} events arrive in order: {len(receiver.requests)}")


def bronze_evidence_step_is_cut_at_the_total_deadline() -> None:
    """A receiver that stalls is cut at the one total deadline across every send, a receiver that refuses the connection is left at once, and the evidence
    step still gives its record: nothing the sends do can fail it."""
    rig, _, _ = bronze_launch(two_table_record())
    with receiving(delay=30.0) as stalled, mock.patch.object(ecs_lineage, "TABLE_EVENTS_SECONDS", 1.0), lineage_environment(http_on(stalled.url)):
        began = time.monotonic()
        record = run_evidence(rig)
        spent = time.monotonic() - began
    require(record["job"] == "bronze" and record["run_id"] == RUN, f"a stalled receiver does not fail the evidence step: {record}")
    require(0.8 <= spent < 3.0, f"the stalled sends are cut at the 1 s total deadline, not before and not much later: {spent:.2f} s")
    require(len(stalled.requests) == 1, f"nothing is sent after the deadline: {len(stalled.requests)} requests")
    with receiving(delay=0.4) as slow, mock.patch.object(ecs_lineage, "TABLE_EVENTS_SECONDS", 1.0), lineage_environment(http_on(slow.url)):
        began = time.monotonic()
        run_evidence(rig)
        spent = time.monotonic() - began
    require(1 <= len(slow.requests) < 4 and spent < 3.0, f"the deadline spans all sends, not each one: {len(slow.requests)} of 4 sent in {spent:.2f} s")
    with lineage_environment(http_on("http://127.0.0.1:1")):
        require(run_evidence(rig)["job"] == "bronze", "a refused connection does not fail the evidence step")


def bronze_evidence_step_sends_only_when_http_lineage_is_on_and_the_task_passed() -> None:
    """Without HTTP lineage (no settings, a transport type other than http, no URL or no key) nothing is sent; a failed bronze run is refused by the evidence
    step and sends nothing for its tables; a job other than bronze sends no table events."""
    rig, _, _ = bronze_launch(two_table_record())
    failed_rig, _, _ = bronze_launch(two_table_record(), outcome="failed")
    with receiving() as receiver:
        off = [{}, http_on(receiver.url, OPENLINEAGE__TRANSPORT__TYPE="console"), http_on(""), http_on(receiver.url, OPENLINEAGE__TRANSPORT__AUTH__API_KEY="")]
        for values in off:
            with lineage_environment(values):
                found = run_evidence(rig)
            require(found["job"] == "bronze", f"the evidence record is given with HTTP lineage off ({sorted(values)}): {found}")
        require(not receiver.requests, f"HTTP lineage off sent {len(receiver.requests)} requests")
        with lineage_environment(http_on(receiver.url)):
            refuses(LaunchRefused, lambda: run_evidence(failed_rig))
        require(not receiver.requests, f"a failed run sent {len(receiver.requests)} requests")
        gold = Rig()
        evidence_bucket(gold)
        operator(gold, None).execute(gold_context())
        with lineage_environment(http_on(receiver.url)):
            found = run_evidence(gold, SubmissionRequest("gold_build", RUN, "real", 1, "gold_transform_dag", "evidence", f"manual__{RUN}"))
        require(found["job"] == "gold", f"a job other than bronze gives its evidence: {found}")
        require(not receiver.requests, f"a job other than bronze sent {len(receiver.requests)} table events")


BAD_KEYS = ("SYNTHETIC_KEY_NEWLINE\n", "SYNTHETIC_KEY_RETURN\r\nX-Injected: yes", "SYNTHETIC_KEY_WIDE\u20ac")


def credential_errors_are_sanitized_delivery_failures() -> None:
    """A key the HTTP client refuses as a header value (a trailing newline, a line break, a character outside Latin-1) is a delivery failure that names no key
    text: not in the error, its traceback, stdout, stderr or any log record; the evidence step logs the failure, never "sent", and still gives its record."""
    rig, _, _ = bronze_launch(two_table_record())
    for key in BAD_KEYS:
        marker = key.splitlines()[0].rstrip("\u20ac")
        with sinks() as seen, receiving() as receiver:
            error = refuses(ecs_lineage.DeliveryFailed, lambda key=key, receiver=receiver: ecs_lineage.post_event(receiver.url, b"{}", key))
            direct = refuses(ecs_lineage.DeliveryFailed, lambda key=key, receiver=receiver: ecs_lineage.send_within(["{}"], receiver.url, key, 5.0))
            with lineage_environment(http_on(receiver.url, OPENLINEAGE__TRANSPORT__AUTH__API_KEY=key)):
                record = run_evidence(rig)
            text = seen()
        shown = "".join(traceback.format_exception(error)) + "".join(traceback.format_exception(direct))
        require(marker not in str(error) + str(direct) + shown, f"the key text is in a delivery error for {marker}: {error}")
        require(marker not in text, f"the key text {marker} reached stdout, stderr or a log record: {text[:300]}")
        require(not receiver.requests, f"{marker}: a request with a refused header reached the receiver")
        require(record["job"] == "bronze", f"{marker}: the evidence record is still given: {record}")
        require("were not delivered" in text and not re.search(r"\bsent \d+ bronze", text), f"{marker}: the failure is logged and no success: {text[:300]}")


def sender_exceptions_are_delivery_failures() -> None:
    """An exception of the sender thread that is not a delivery failure reaches the caller as one, with its text hidden (it can hold a header or a key), and
    the thread writes nothing to stderr; the evidence step logs the failure and gives its record."""
    rig, _, _ = bronze_launch(two_table_record())
    leaked = "SYNTHETIC_SENDER_SECRET"
    with sinks() as seen, mock.patch.object(ecs_lineage, "post_event", side_effect=RuntimeError(leaked)):
        error = refuses(ecs_lineage.DeliveryFailed, lambda: ecs_lineage.send_within(["{}"], "http://127.0.0.1:1", "synthetic-key", 5.0))
        with lineage_environment(http_on("http://127.0.0.1:1")):
            record = run_evidence(rig)
        text = seen()
    require(leaked not in str(error) + text and "RuntimeError" in str(error), f"the sender's exception is named by type only: {error}; {text[:300]}")
    require(record["job"] == "bronze" and "were not delivered" in text and not re.search(r"\bsent \d+ bronze", text), f"the failure is logged: {text[:300]}")


CASES: dict[str, Callable[[], None]] = {
    "bronze_job_lineage_is_the_run_record": bronze_job_lineage_is_the_run_record,
    "bronze_hook_sends_no_table_events": bronze_hook_sends_no_table_events,
    "bronze_evidence_step_sends_the_replay_events": bronze_evidence_step_sends_the_replay_events,
    "bronze_evidence_step_sends_every_table_past_the_providers_timeout": bronze_evidence_step_sends_every_table_past_the_providers_timeout,
    "bronze_evidence_step_is_cut_at_the_total_deadline": bronze_evidence_step_is_cut_at_the_total_deadline,
    "bronze_evidence_step_sends_only_when_http_lineage_is_on_and_the_task_passed": bronze_evidence_step_sends_only_when_http_lineage_is_on_and_the_task_passed,
    "credential_errors_are_sanitized_delivery_failures": credential_errors_are_sanitized_delivery_failures,
    "sender_exceptions_are_delivery_failures": sender_exceptions_are_delivery_failures,
    "names_match_on_both_sides": names_match_on_both_sides,
    "acquisition_events_are_valid": acquisition_events_are_valid,
    "acquisition_refusals": acquisition_refusals,
    "acquisition_replay_is_stable": acquisition_replay_is_stable,
    "bronze_events_name_the_loaded_files": bronze_events_name_the_loaded_files,
    "bronze_record_refusals": bronze_record_refusals,
}
