"""Cases of the lineage that starts at acquisition and of the evidence step of the job DAGs.

An acquisition run names each raw file it stored as the dataset ``s3://<bucket>`` and the object key, with the object version as the dataset version facet.
The COMPLETE event of the acquisition job carries every stored file as an output and its START event carries none. A state whose receipt and reconciliation
disagree, or that names an unverified object, is refused by name, and the replay of a saved batch gives the same lines every time. A bronze run sends no table
events: its task events are a START and a COMPLETE that name no dataset, and its run record, which the catalog writer reads for each table's raw files, is
refused by its key and the rule when it is missing, unreadable or wrong. The evidence step of a gold job sends the per-table events byte for byte as the replay
gives them, with the bearer credential and only when HTTP lineage is on and the task passed. It sends every one of them even when the sends outlast the
provider's execution timeout, it cuts a stalled receiver at one total deadline, and no failure of the sends fails its record. A refused credential or an
exception of the sender thread is a delivery failure that shows no text of the key. The saved acquisition states are stand-ins written by the case in a
temporary folder, in the shapes ``scripts/acquisition/batch.py`` (``completed_collection_layout.json``) and ``scripts/acquisition/s3_store.py`` (the
reconciliation record) write, with a receipt in the shape of ``config/acquisition/snapshot_contract.schema.json``. No acquisition runs and nothing reaches S3.
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
from scripts.orchestration.ecs_lineage_cases import (
    CREDENTIAL,
    START,
    invocation,
    operator,
    real_edges,
    receiving,
    refuses,
    require,
    table_pairs,
    template_bucket,
    validator,
)
from scripts.orchestration.ecs_lineage_cases import context as gold_context

BUCKET = "example-acquisition-bucket"
SNAPSHOT = "silver_0a1b2c3d"
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


def raw_files_are_named_by_bucket_key_and_version() -> None:
    """A stored object is the dataset ``s3://<bucket>`` and its key, with the object version as its version facet."""
    with tempfile.TemporaryDirectory() as folder:
        stored = raw_lineage.acquisition_events(saved_batch(Path(folder)), "job_cms_hai")[1]["outputs"]
    wanted = [(f"s3://{item['bucket']}", item["key"], item["version_id"]) for item in OBJECTS]
    require(pair(stored) == wanted, f"acquisition {pair(stored)} names the files differently from their bucket, key and version {wanted}")
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
        (lambda body, record: record.update(snapshot_id="silver_ffffffff"), "another snapshot"),
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


BRONZE_RUN = "bronze_abcdef01"
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
        "skipped_tables": [{"table": "empty_table", "reason": "no source object"}],
    }
    record.update(change)
    return record


def bronze_bucket(record: Any) -> Any:
    bucket = Rig().bucket
    bucket.allowed_prefix = ""
    bucket.objects[RECORD_KEY] = record if isinstance(record, bytes) else json.dumps(record).encode()
    return bucket


def two_table_record() -> dict[str, Any]:
    """A run that loaded two tables, one object each."""
    objects = [
        {"table": "cms_hai_hospital", **OBJECTS[0], "sha256": "1" * 64, "byte_count": 11},
        {"table": "cms_hai_state", **OBJECTS[1], "sha256": "2" * 64, "byte_count": 12},
    ]
    return check_record(tables=["cms_hai_hospital", "cms_hai_state"], snapshots={"cms_hai_hospital": 51, "cms_hai_state": 52}, objects=objects)


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
        launch.execute({"ti": SimpleNamespace(dag_id="bronze_dag", task_id="bronze_load", run_id=f"manual__{RUN}", map_index=-1, try_number=1)})
    else:
        rig.ecs.plan = {"gold": 3}
        refuses(
            JobFailed,
            lambda: launch.execute({"ti": SimpleNamespace(dag_id="bronze_dag", task_id="bronze_load", run_id=f"manual__{RUN}", map_index=-1, try_number=1)}),
        )
    declared = ecs_lineage.declared_lineage("bronze")
    evidence, store = ecs_lineage.S3Evidence(rig.bucket, BUCKET_LAUNCH), S3LaunchStore(rig.bucket, BUCKET_LAUNCH, PREFIX)
    return rig, job, ecs_lineage.LineageSource("bronze", RUN, 1, invocation("bronze"), declared, evidence, store)


GOLD_REQUEST = SubmissionRequest("gold_build", RUN, "real", 1, "gold_transform_dag", "evidence", f"manual__{RUN}")


def run_evidence(rig: Rig, request: SubmissionRequest = GOLD_REQUEST) -> Any:
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


BAD_KEYS = ("SYNTHETIC_KEY_NEWLINE\n", "SYNTHETIC_KEY_RETURN\r\nX-Injected: yes", "SYNTHETIC_KEY_WIDE\u20ac")


def gold_rig(outcome: str = "succeeded") -> tuple[Rig, ecs_lineage.LineageSource]:
    """A rig whose gold job ran over the evidence of the real declared tables (and failed, when ``outcome`` says so), and the lineage source of its step."""
    rig = Rig()
    template_bucket(rig)
    if outcome == "succeeded":
        operator(rig, None).execute(gold_context())
    else:
        rig.ecs.plan = {"gold": 3}
        refuses(JobFailed, lambda: operator(rig, None).execute(gold_context()))
    task = ecs_lineage.step_invocation(replace(GOLD_REQUEST, task_id="job").as_dict())
    evidence, store = ecs_lineage.S3Evidence(rig.bucket, BUCKET_LAUNCH), S3LaunchStore(rig.bucket, BUCKET_LAUNCH, PREFIX)
    return rig, ecs_lineage.LineageSource("gold", RUN, 1, task, ecs_lineage.declared_lineage("gold"), evidence, store)


def layer_evidence_step_sends_the_replay_events() -> None:
    """A gold run: the evidence step, which runs outside the provider's fork, sends the per-table events, byte for byte the replay's table events, to the
    receiver with the bearer credential, so each table keeps exactly its own inputs. Its own record is the one it always gave."""
    rig, found = gold_rig()
    lines = found.table_lines()
    declared = {job: ecs_lineage.declared_lineage(job) for job in ("silver", "gold", "features")}
    wanted = real_edges(declared)["gold"]
    require(len(lines) == 2 * len(wanted), f"{len(lines)} table events for {len(wanted)} tables")
    replayed = [ecs_lineage.canonical(event) for event in found.events()]
    require(replayed[2:] == lines, "the replay is the task's two events and then the table events, byte for byte")
    with receiving() as quiet, lineage_environment(http_on(quiet.url)):
        without = run_evidence(gold_rig()[0])
    require(len(quiet.requests) == len(lines), f"the evidence step sent {len(quiet.requests)} events")
    with receiving() as receiver, lineage_environment(http_on(receiver.url)):
        record = run_evidence(rig)
    require(record == without and record["job"] == "gold", f"the evidence record: {record}")
    require([item["body"].decode() for item in receiver.requests] == lines, "the evidence step sent the table events, byte for byte and in order")
    require(all(item["authorization"] == f"Bearer {CREDENTIAL}" and item["path"] == ecs_lineage.ROUTE for item in receiver.requests), "bearer and route")
    require(
        len(table_pairs([json.loads(item["body"]) for item in receiver.requests])) == sum(len(reads) for reads in wanted.values()), "the recorded edges only"
    )


def layer_evidence_step_sends_every_table_past_the_providers_timeout() -> None:
    """The provider's execution timeout is 5 seconds (``AIRFLOW__OPENLINEAGE__EXECUTION_TIMEOUT``). A receiver that takes 0.06 s per event gets the 142
    events of the 71 gold tables in more than 8 seconds, and all of them arrive, from the evidence step; none from the task's own hook."""
    require(ecs_lineage.TABLE_EVENTS_SECONDS == 300.0, f"the total deadline is {ecs_lineage.TABLE_EVENTS_SECONDS} s")
    rig, found = gold_rig()
    lines = found.table_lines()
    with receiving(delay=0.06) as receiver, lineage_environment(http_on(receiver.url)):
        require(found.facets(True)["inputs"] == [], "the hook's facets carry no table event")
        require(not receiver.requests, f"the hook sent {len(receiver.requests)} events")
        began = time.monotonic()
        run_evidence(rig)
        spent = time.monotonic() - began
    require(spent > 5.0, f"the sends must outlast the provider's 5 s timeout to prove the move: {spent:.2f} s")
    require([item["body"].decode() for item in receiver.requests] == lines, f"all {len(lines)} events arrive in order: {len(receiver.requests)}")


def layer_evidence_step_is_cut_at_the_total_deadline() -> None:
    """A receiver that stalls is cut at the one total deadline across every send, a receiver that refuses the connection is left at once, and the evidence
    step still gives its record: nothing the sends do can fail it."""
    rig, found = gold_rig()
    total = len(found.table_lines())
    with receiving(delay=30.0) as stalled, mock.patch.object(ecs_lineage, "TABLE_EVENTS_SECONDS", 1.0), lineage_environment(http_on(stalled.url)):
        began = time.monotonic()
        record = run_evidence(rig)
        spent = time.monotonic() - began
    require(record["job"] == "gold" and record["run_id"] == RUN, f"a stalled receiver does not fail the evidence step: {record}")
    require(0.8 <= spent < 3.0, f"the stalled sends are cut at the 1 s total deadline, not before and not much later: {spent:.2f} s")
    require(len(stalled.requests) == 1, f"nothing is sent after the deadline: {len(stalled.requests)} requests")
    with receiving(delay=0.4) as slow, mock.patch.object(ecs_lineage, "TABLE_EVENTS_SECONDS", 1.0), lineage_environment(http_on(slow.url)):
        began = time.monotonic()
        run_evidence(rig)
        spent = time.monotonic() - began
    require(
        1 <= len(slow.requests) < total and spent < 3.0, f"the deadline spans all sends, not each one: {len(slow.requests)} of {total} sent in {spent:.2f} s"
    )
    with lineage_environment(http_on("http://127.0.0.1:1")):
        require(run_evidence(rig)["job"] == "gold", "a refused connection does not fail the evidence step")


def layer_evidence_step_sends_only_when_http_lineage_is_on_and_the_task_passed() -> None:
    """Without HTTP lineage (no settings, a transport type other than http, no URL or no key) nothing is sent; a failed gold run is refused by the evidence
    step and sends nothing; a succeeded layer job sends one event pair per published table, each with its own inputs, and not the layer's cross product."""
    rig, _ = gold_rig()
    failed_rig, _ = gold_rig("failed")
    with receiving() as receiver:
        off = [{}, http_on(receiver.url, OPENLINEAGE__TRANSPORT__TYPE="console"), http_on(""), http_on(receiver.url, OPENLINEAGE__TRANSPORT__AUTH__API_KEY="")]
        for values in off:
            with lineage_environment(values):
                found = run_evidence(rig)
            require(found["job"] == "gold", f"the evidence record is given with HTTP lineage off ({sorted(values)}): {found}")
        require(not receiver.requests, f"HTTP lineage off sent {len(receiver.requests)} requests")
        with lineage_environment(http_on(receiver.url)):
            refuses(LaunchRefused, lambda: run_evidence(failed_rig))
        require(not receiver.requests, f"a failed run sent {len(receiver.requests)} requests")
        with lineage_environment(http_on(receiver.url)):
            found = run_evidence(rig)
        declared = {job: ecs_lineage.declared_lineage(job) for job in ("silver", "gold", "features")}
        sent = [json.loads(item["body"]) for item in receiver.requests]
        wanted = real_edges(declared)["gold"]
        require(found["job"] == "gold" and len(sent) == 2 * len(wanted), f"a gold run sends a START and a COMPLETE per published table: {len(sent)}")
        pairs = table_pairs(sent)
        require(len(pairs) == sum(len(reads) for reads in wanted.values()), f"the pairs are the recorded edges, not the layer's cross product: {len(pairs)}")


def bronze_sends_no_table_events() -> None:
    """A bronze run sends no table events: bronze is not a job with table events, its launch holds no table lines, its task events are a START and a COMPLETE
    that name no dataset, and the evidence step of a succeeded bronze run sends nothing with HTTP lineage on."""
    require(ecs_lineage.BRONZE_JOB not in ecs_lineage.TABLE_EVENT_JOBS, "bronze must not be a job with table events")
    rig, _, found = bronze_launch(two_table_record())
    require(found.table_lines() == [], "a bronze run has no table lines")
    task = found.events()
    require(
        [item["eventType"] for item in task] == ["START", "COMPLETE"] and all(item["inputs"] == [] and item["outputs"] == [] for item in task),
        "task events only",
    )
    bronze_request = SubmissionRequest("bronze_load", RUN, "real", 1, "bronze_dag", "bronze_load", f"manual__{RUN}")
    with receiving() as receiver, lineage_environment(http_on(receiver.url)):
        record = run_evidence(rig, bronze_request)
    require(record["job"] == "bronze" and not receiver.requests, f"the bronze evidence step sent {len(receiver.requests)} events")


def bronze_record_refusals() -> None:
    """The bronze run record, which the catalog writer reads for each table's raw files, is refused by its key and the rule when it is missing, unreadable or
    wrong."""
    first = OBJECTS[0]
    older = check_record()
    older.pop("skipped_tables")
    cases: list[tuple[Any, str]] = [
        (older, "skipped_tables"),
        (check_record(skipped_tables="empty_data"), "skipped tables"),
        (check_record(run_id="silver_fffffff0"), "another run"),
        (check_record(target="qualify"), "real target"),
        (check_record(objects=[]), "no loaded object"),
        (check_record(objects=[{"table": "other_table", **first, "sha256": "1" * 64, "byte_count": 1}]), "not a requested table"),
        (check_record(objects=[{"table": "cms_hai_hospital", **first}]), "exactly the fields"),
        (check_record(snapshots={"cms_hai_hospital": "51"}), "snapshot"),
        (b'{"schema_version": 1, "schema_version": 1}', "duplicate"),
    ]
    for record, phrase in cases:
        evidence = ecs_lineage.S3Evidence(bronze_bucket(record), BUCKET_LAUNCH)
        error = refuses(ecs_lineage.LineageRefused, lambda evidence=evidence: raw_lineage.loaded_objects(evidence.read(RECORD_KEY), RECORD_KEY, BRONZE_RUN))
        require(RECORD_KEY in str(error) and phrase in str(error), f"refusal must name {RECORD_KEY} and '{phrase}': {error}")
    objects, snapshots = raw_lineage.loaded_objects(two_table_record(), RECORD_KEY, BRONZE_RUN)
    require(len(objects) == 2 and snapshots == {"cms_hai_hospital": 51, "cms_hai_state": 52}, f"a good record is read: {snapshots}")
    bucket = bronze_bucket(check_record())
    bucket.objects.pop(RECORD_KEY)
    require(ecs_lineage.S3Evidence(bucket, BUCKET_LAUNCH).read(RECORD_KEY) is None, "a missing record reads as none, for the caller to refuse")
    bucket.objects[RECORD_KEY] = b"{}"
    bucket.deny_keys.add(RECORD_KEY)
    refuses(ecs_lineage.EvidenceUnavailable, lambda: ecs_lineage.S3Evidence(bucket, BUCKET_LAUNCH).read(RECORD_KEY))


BAD_KEYS = ("SYNTHETIC_KEY_NEWLINE\n", "SYNTHETIC_KEY_RETURN\r\nX-Injected: yes", "SYNTHETIC_KEY_WIDE\u20ac")


def credential_errors_are_sanitized_delivery_failures() -> None:
    """A key the HTTP client refuses as a header value (a trailing newline, a line break, a character outside Latin-1) is a delivery failure that names no key
    text: not in the error, its traceback, stdout, stderr or any log record; the evidence step logs the failure, never "sent", and still gives its record."""
    rig, _ = gold_rig()
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
        require(record["job"] == "gold", f"{marker}: the evidence record is still given: {record}")
        require("were not delivered" in text and not re.search(r"\bsent \d+ gold", text), f"{marker}: the failure is logged and no success: {text[:300]}")


def sender_exceptions_are_delivery_failures() -> None:
    """An exception of the sender thread that is not a delivery failure reaches the caller as one, with its text hidden (it can hold a header or a key), and
    the thread writes nothing to stderr; the evidence step logs the failure and gives its record."""
    rig, _ = gold_rig()
    leaked = "SYNTHETIC_SENDER_SECRET"
    with sinks() as seen, mock.patch.object(ecs_lineage, "post_event", side_effect=RuntimeError(leaked)):
        error = refuses(ecs_lineage.DeliveryFailed, lambda: ecs_lineage.send_within(["{}"], "http://127.0.0.1:1", "synthetic-key", 5.0))
        with lineage_environment(http_on("http://127.0.0.1:1")):
            record = run_evidence(rig)
        text = seen()
    require(leaked not in str(error) + text and "RuntimeError" in str(error), f"the sender's exception is named by type only: {error}; {text[:300]}")
    require(record["job"] == "gold" and "were not delivered" in text and not re.search(r"\bsent \d+ gold", text), f"the failure is logged: {text[:300]}")


CASES: dict[str, Callable[[], None]] = {
    "layer_evidence_step_sends_the_replay_events": layer_evidence_step_sends_the_replay_events,
    "layer_evidence_step_sends_every_table_past_the_providers_timeout": layer_evidence_step_sends_every_table_past_the_providers_timeout,
    "layer_evidence_step_is_cut_at_the_total_deadline": layer_evidence_step_is_cut_at_the_total_deadline,
    "layer_evidence_step_sends_only_when_http_lineage_is_on_and_the_task_passed": layer_evidence_step_sends_only_when_http_lineage_is_on_and_the_task_passed,
    "bronze_sends_no_table_events": bronze_sends_no_table_events,
    "credential_errors_are_sanitized_delivery_failures": credential_errors_are_sanitized_delivery_failures,
    "sender_exceptions_are_delivery_failures": sender_exceptions_are_delivery_failures,
    "raw_files_are_named_by_bucket_key_and_version": raw_files_are_named_by_bucket_key_and_version,
    "acquisition_events_are_valid": acquisition_events_are_valid,
    "acquisition_refusals": acquisition_refusals,
    "acquisition_replay_is_stable": acquisition_replay_is_stable,
    "bronze_record_refusals": bronze_record_refusals,
}
