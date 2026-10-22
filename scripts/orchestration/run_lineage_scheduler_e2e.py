"""Check native scheduler lineage, Marquez graph, recovery and database privacy on a disposable fixture stack.

Run one case per exclusive heavy grant in a private checkout.
This runner never pulls or installs, never opens a registry file and retains its private work folder for review.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import signal
import sys
import tempfile
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import quote, urlencode

from orchestration import launcher, lineage
from scripts.orchestration import run_transform_e2e as fixture
from scripts.orchestration import sentinel, transform_fixtures
from scripts.orchestration.airflow_api import AirflowApiError
from scripts.orchestration.supervisor import Supervisor, SupervisorError
from services.lineage import provider_events

ROOT = Path(__file__).resolve().parents[2]
MAX_JSON = 10 * 1024**2
MAX_DUMP = 64 * 1024**2
COPY_ESCAPES = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v", "\\": "\\"}
RowKey = tuple[str, str, str]
EVENT_TIME = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(?:Z|\+00:00)")
MUTATION_CASES = {
    "correlation": "graph",
    "delivery": "outage",
    "sentinel": "graph",
    "resend_early": "outage",
    "input_facet": "graph",
    "input_facet_selective": "graph",
}


class CheckFailure(AssertionError):
    """A fixed check description that can be reported without exposing runtime exception values."""


@dataclass
class ResourceState:
    """Known finite-check reservations and any unconfirmed acquisition that must prevent a clean result."""

    allocations: set[str] = field(default_factory=set)
    acquisition_unknown: bool = False


def require(value: Any, message: str) -> None:
    if not value:
        raise CheckFailure(message)


@contextmanager
def deadline(seconds: int) -> Iterator[None]:
    """Bound an entire stage, including requests and setup, with a visible timeout."""

    def expired(_signum: int, _frame: Any) -> None:
        raise TimeoutError("lineage scheduler stage exceeded its absolute deadline")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds, 1)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def error_label(error: BaseException) -> str:
    """The error type, with the text and code of a launcher refusal (composed without fixture values); any other error reports its type only."""
    if isinstance(error, launcher.ClientError):
        return f"{type(error).__name__}: {error} [{error.code}]"[:500]
    return type(error).__name__


def wait(predicate: Any, seconds: float, label: str) -> Any:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        answer = predicate()
        if answer:
            return answer
        time.sleep(0.2)
    raise CheckFailure(f"lineage check did not finish: {label}")


def get(port: int, path: str) -> dict[str, Any]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        data = response.read(MAX_JSON + 1)
        require(response.status == 200 and len(data) <= MAX_JSON, "Marquez query failed or exceeded its response bound")
        answer = json.loads(data)
        require(isinstance(answer, dict), "Marquez response is not an object")
        return answer
    finally:
        connection.close()


class CachedSupervisor(Supervisor):
    """Keep the real supervisor and require cached pinned images without a pull fallback."""

    lineage_port = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["readiness_seconds"] = 120
        kwargs["lineage_api_port"] = self.lineage_port
        super().__init__(*args, **kwargs)

    def check_images(self) -> dict[str, Any]:
        """Inspect cached PostgreSQL and Airflow through the launcher and validate the existing image labels."""
        images = {}
        for service in ("postgres", "airflow-check"):
            found = self.call("image", {"service": service, "compose": "orchestration", "checkout": str(self.repo_root)}, timeout=15)
            expected = self.platform["metadata_database" if service == "postgres" else "airflow"]["image"]
            require(found["image"] == expected, "cached orchestration image differs from the platform pin")
            images[service] = {"reference": found["image"], "id": found["id"]}
            if service == "airflow-check":
                labels = (found["inspect"].get("Config") or {}).get("Labels") or {}
                require(all(labels.get(key) == value for key, value in self.platform["airflow"]["labels"].items()), "Airflow deadline labels differ")
        return images


def start_marquez(stack: fixture.Stack, port: int, web_port: int) -> dict[str, Any]:
    state = stack.state / "lineage"
    for name in ("postgres", "config"):
        (state / name).mkdir(parents=True, exist_ok=True, mode=0o700)
    for service in launcher.ComposeModel(ROOT / launcher.LINEAGE_COMPOSE).services:
        found = stack.client.call("image", {"service": service, "compose": "lineage", "checkout": str(stack.checkout)}, timeout=15)
        require("@sha256:" in found["image"], "Marquez image is not pinned by digest")
    stack.client.call("lineage_up", {"state": str(state), "api_port": port, "web_port": web_port, "checkout": str(stack.checkout)}, timeout=10)

    def healthy() -> Any:
        view = stack.client.call("status", {"what": "lineage"}, timeout=5)
        if view["phase"] == "down":
            raise RuntimeError("Marquez startup failed")
        return view if view["phase"] == "up" else None

    ready = wait(healthy, 90, "Marquez health")
    return {"phase": ready["phase"], "health": ready["health"]}


def native_events(stack: fixture.Stack, run_id: str) -> list[dict[str, Any]]:
    events = []
    for path in sorted((stack.state / "lineage_provider").glob("*.json")):
        require(path.stat().st_size <= provider_events.MAX_BYTES and not path.is_symlink(), "provider evidence is not a bounded regular file")
        event = provider_events.validate(json.loads(path.read_text()))
        if event["caller"]["dag_run_id"] == run_id:
            events.append(event)
    return events


def expected_events(stack: fixture.Stack, run_id: str, native: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected = [row["event"] for row in native]
    links = {provider_events.caller_key(row["caller"]): row for row in native if "task_id" in row["caller"]}
    for path in sorted(stack.supervisor.paths.records.rglob("*.json")):
        require(path.stat().st_size <= lineage.MAX_RECORD_BYTES and not path.is_symlink(), "broker record evidence is not bounded")
        record = json.loads(path.read_text())
        if record.get("request_version") != 2 or record.get("dag_run_id") != run_id:
            continue
        provider = links.get(provider_events.caller_key(record))
        require(provider is not None, "completed broker record has no exact native task correlation")
        if provider is None:
            raise AssertionError("native task correlation is missing")
        link = {"caller": provider["caller"], "run_id": provider["event"]["run"]["runId"], "parent": provider["event"]["run"]["facets"]["parent"]}
        expected.extend(lineage.build_events(record, link))
    require(any(".model." in row["job"]["name"] for row in expected), "fixture produced no accepted native dbt model lineage")
    return expected


def event_ids(expected: list[dict[str, Any]]) -> set[str]:
    return {event["run"]["facets"].get("haiIdentity", {}).get("eventId") or provider_events.event_id(event) for event in expected}


def drain(worker: lineage.CompletionWorker, expected: list[dict[str, Any]]) -> int:
    identities = event_ids(expected)
    worker.wake()

    def empty() -> Any:
        if not worker.outbox.exists():
            return None
        with lineage.Outbox(worker.outbox) as box:
            rows = dict(box.connection.execute("SELECT id,acked FROM events"))
            covered = {row[0] for row in box.connection.execute("SELECT id FROM provider_events WHERE flushed=2")}
        for event in expected:
            identity = next(iter(event_ids([event])))
            if identity not in covered:
                continue
            require(event["eventType"] in ("START", "RUNNING"), "native terminal event was covered without delivery")
            require(
                any(
                    item["eventType"] in ("COMPLETE", "FAIL", "ABORT")
                    and item["run"]["runId"] == event["run"]["runId"]
                    and item["run"]["facets"].get("haiIdentity")
                    and rows.get(next(iter(event_ids([item]))))
                    for item in expected
                ),
                "covered native phase lacks an acknowledged host terminal event",
            )
        return len(rows) if identities <= rows.keys() | covered and all(rows.values()) else None

    return int(wait(empty, 60, "durable outbox delivery"))


def runs(port: int, expected: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    wanted: dict[str, set[str]] = {}
    for event in expected:
        wanted.setdefault(event["job"]["name"], set()).add(event["run"]["runId"])
    result = {}
    for job, identities in wanted.items():
        path = f"/api/v1/namespaces/{quote(lineage.JOB_NAMESPACE, safe='')}/jobs/{quote(job, safe='')}/runs?limit=1000&offset=0"
        found = get(port, path)["runs"]
        require(len(found) < 1000, "Marquez runs exceed the complete single-page fixture bound")
        counts = Counter(row["id"] for row in found)
        require(set(counts) == identities and all(count == 1 for count in counts.values()), "Marquez lost a native run or duplicated a run UUID")
        result[job] = {row["id"]: row["state"] for row in found}
    return result


def graph(port: int, expected: list[dict[str, Any]]) -> dict[str, int]:
    silver = next(row for row in expected if row["job"]["name"].endswith(".silver_build") and row["eventType"] == "COMPLETE" and row["inputs"])
    gold = next(row for row in expected if row["job"]["name"].endswith(".gold_build") and row["eventType"] == "COMPLETE" and row["outputs"])

    def dataset_id(row: dict[str, Any]) -> str:
        return f"dataset:{row['namespace']}:{row['name']}"

    origin, target = dataset_id(silver["inputs"][0]), dataset_id(gold["outputs"][0])
    nodes = get(port, "/api/v1/lineage?" + urlencode({"nodeId": origin, "depth": 20}))["graph"]
    ids = {row["id"] for row in nodes}
    edges = {(edge["origin"], edge["destination"]) for row in nodes for edge in [*row["inEdges"], *row["outEdges"]]}
    for event in expected:
        if event["eventType"] != "COMPLETE" or not (event["inputs"] or event["outputs"]):
            continue
        job = f"job:{lineage.JOB_NAMESPACE}:{event['job']['name']}"
        require(job in ids, "Marquez graph omitted a declared task or model")
        require(all((dataset_id(item), job) in edges for item in event["inputs"]), "Marquez graph omitted a declared model or task input edge")
        require(all((job, dataset_id(item)) in edges for item in event["outputs"]), "Marquez graph omitted a declared model or task output edge")
    reached = {origin}
    for _ in range(len(ids)):
        added = {destination for source, destination in edges if source in reached} - reached
        if not added:
            break
        reached.update(added)
    require(target in reached, "fixture bronze to gold graph is disconnected")
    return {"nodes": len(ids), "edges": len(edges), "connected": 1}


def copy_field(text: str) -> str:
    """Undo the backslash escaping of one COPY text field."""
    decoded, index = [], 0
    while index < len(text):
        if text[index] != "\\":
            decoded.append(text[index])
            index += 1
            continue
        require(index + 1 < len(text) and text[index + 1] in COPY_ESCAPES, "database dump holds an unknown COPY escape")
        decoded.append(COPY_ESCAPES[text[index + 1]])
        index += 2
    return "".join(decoded)


def stored_events(dump: str) -> list[dict[str, Any]]:
    """Every raw OpenLineage event row of the dump's lineage_events table, whatever the column order."""
    events: list[dict[str, Any]] = []
    found = inside = False
    for line in dump.split("\n"):
        if line.startswith("-- table "):
            inside = line == "-- table lineage_events"
            found = found or inside
            continue
        if not inside or not line:
            continue
        for column in line.split("\t"):
            if not column.startswith("{"):
                continue
            try:
                event = json.loads(copy_field(column))
            except ValueError:
                raise CheckFailure("a lineage_events field is not valid JSON") from None
            if isinstance(event, dict) and {"eventType", "eventTime", "run"} <= event.keys():
                events.append(event)
                break
        else:
            raise CheckFailure("a lineage_events row holds no OpenLineage event")
    require(found, "database dump holds no lineage_events table")
    return events


def exact_instant(value: Any) -> str:
    """The event time as a UTC instant with all nine fraction digits, so a time that differs by one nanosecond gives another value."""
    match = EVENT_TIME.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise CheckFailure("an event time is not a UTC instant")
    try:
        datetime.fromisoformat(match.group(1))
    except ValueError:
        raise CheckFailure("an event time is not a UTC instant") from None
    return f"{match.group(1)}.{(match.group(2) or '').ljust(9, '0')}Z"


def row_key(event: dict[str, Any]) -> RowKey:
    """Identify one event by run UUID, event type and its whole canonical form.

    Marquez stores the event time as a UTC instant ending in Z while the outbox holds the same instant as +00:00, so both sides spell the time one way.
    Everything else must match exactly: a dropped, doubled or altered event changes the key.
    """
    return str(event["run"]["runId"]), str(event["eventType"]), lineage.canonical({**event, "eventTime": exact_instant(event["eventTime"])})


def stored_event_rows(dump: str) -> Counter[RowKey]:
    """Count every raw event row by its canonical event."""
    return Counter(row_key(event) for event in stored_events(dump))


def delivered_events(outbox: Path) -> list[dict[str, Any]]:
    with lineage.Outbox(outbox) as box:
        return [json.loads(row[0]) for row in box.connection.execute("SELECT payload FROM events WHERE acked=1")]


def delivered_rows(outbox: Path) -> Counter[RowKey]:
    """Count the acknowledged outbox events by the same key as the stored rows."""
    return Counter(row_key(item) for item in delivered_events(outbox))


def compare_rows(stored: Counter[RowKey], delivered: Counter[RowKey]) -> dict[str, int]:
    """Counts only, so a report never holds an event value."""
    return {
        "delivered": sum(delivered.values()),
        "stored": sum(stored.values()),
        "duplicated": sum(max(stored[key] - count, 0) for key, count in delivered.items()),
        "missing": sum(max(count - stored[key], 0) for key, count in delivered.items()),
        "unexpected": sum(count for key, count in stored.items() if key not in delivered),
    }


def require_one_row_per_event(dump: Path, outbox: Path) -> dict[str, int]:
    """After recovery the dump holds exactly one raw row for each delivered event."""
    delivered = delivered_rows(outbox)
    require(bool(delivered), "no delivered event to count")
    counts = compare_rows(stored_event_rows(dump.read_text(encoding="utf-8")), delivered)
    require(counts["stored"] > 0, "database dump holds no raw event row")
    require(
        not (counts["duplicated"] or counts["missing"] or counts["unexpected"]),
        f"raw event rows differ from delivered events (duplicated {counts['duplicated']}, missing {counts['missing']}, unexpected {counts['unexpected']})",
    )
    return counts


def dump_database(stack: fixture.Stack, resources: ResourceState) -> Path:
    """Run the finite read-only dump job under its own allocation and return the private dump file."""
    resources.acquisition_unknown = True
    allocation = stack.client.allocate(seconds=30)
    resources.acquisition_unknown = False
    resources.allocations.add(allocation)
    try:
        answer = stack.client.run_attempt(
            stack.client.principal_alloc(allocation),
            "lineage_dump",
            "lineage-privacy",
            [],
            {"state": str(stack.state / "lineage")},
            checkout=stack.checkout,
            seconds=60,
        )
        require(answer.ok, "finite read-only Marquez database dump failed")
    finally:
        released = stack.client.release_allocation(allocation)
        if released == launcher.ReleaseOutcome.RELEASED:
            resources.allocations.discard(allocation)
        require(released == launcher.ReleaseOutcome.RELEASED, "lineage dump reservation was retained")
    dump = stack.state / "lineage" / "dumps" / "marquez.sql"
    require(dump.is_file() and 0 < dump.stat().st_size <= MAX_DUMP, "Marquez database dump is missing or exceeds its bound")
    return dump


def database_scan(stack: fixture.Stack, positive: str, values: list[str], progress: dict[str, Any], resources: ResourceState) -> dict[str, Any]:
    worker = stack.supervisor.emitter
    require(worker is not None, "lineage worker is unavailable before privacy scan")
    if worker is None:
        raise AssertionError("lineage worker is missing")
    worker.stop()
    stack.supervisor.emitter = None
    dump = dump_database(stack, resources)
    require(positive.encode() in dump.read_bytes(), "database dump missed the known native run positive control")
    scanned = sentinel.scan([stack.state / "lineage_provider", worker.outbox, dump], values)
    progress["sentinel"] = {**scanned.record(), "database_positive_control": True}
    require(scanned.ok, "lineage sentinel scan failed or lacked complete coverage")
    return {**scanned.record(), "database_positive_control": True}


def resend(worker: lineage.CompletionWorker, expected: list[dict[str, Any]]) -> int:
    """Forget every acknowledgment, as a lost reply would, and wait until the worker has delivered the same events again."""
    with lineage.Outbox(worker.outbox) as box, box.connection:
        box.connection.execute("UPDATE events SET acked=0")
    return drain(worker, expected)


def consumer_records(stack: fixture.Stack, run_id: str) -> list[dict[str, Any]]:
    """The completion records of this run's successful fixture invocations that declare inputs."""
    records = [json.loads(path.read_text()) for path in sorted(stack.supervisor.paths.records.rglob("*.json"))]
    return [
        row
        for row in records
        if row.get("dag_run_id") == run_id and row.get("mode") == "fixture" and row.get("exit_code") == 0 and (row.get("lineage") or {}).get("inputs")
    ]


def invocation_runs(records: list[dict[str, Any]], native: list[dict[str, Any]]) -> dict[str, str]:
    """The native run UUID of each invocation, resolved from its exact caller link as the expected events are."""
    links = {provider_events.caller_key(row["caller"]): row for row in native if "task_id" in row["caller"]}
    runs: dict[str, str] = {}
    for record in records:
        provider = links.get(provider_events.caller_key(record))
        require(provider is not None, "a consuming invocation has no exact native task correlation")
        if provider is None:
            raise AssertionError("native task correlation is missing")
        runs[record["submission_id"]] = provider_events.native_uuid(provider["event"]["run"]["runId"])
    return runs


def verify_consumer_facets(records: list[dict[str, Any]], events: list[dict[str, Any]], run_uuids: dict[str, str]) -> dict[str, int]:
    """Each consuming invocation froze pins equal to its record and its declared inputs, and its own START and terminal events carry exactly those versions.

    An event belongs to an invocation only by job namespace, job name and native run UUID, and by its identity facet when it has one. Another invocation's
    events never count.
    """
    require(bool(records), "no successful fixture invocation declares inputs")
    for record in records:
        job = f"{record['dag_id']}.{record['task_id']}"
        declared = [(item["namespace"], item["name"]) for item in record["lineage"]["inputs"]]
        pins = record["ledger"][0].get("input_pins") or []
        require(
            bool(pins) and pins == (record.get("input_snapshot_record") or {}).get("inputs") and [(pin["namespace"], pin["name"]) for pin in pins] == declared,
            f"the intent pins of {job} do not match its record and declared inputs",
        )
        run_uuid = run_uuids.get(record["submission_id"])
        require(run_uuid is not None, f"{job} has no native run to match")
        wanted = {(pin["namespace"], pin["name"]): pin["snapshot"]["id"] for pin in pins}
        seen = {"START": 0, "COMPLETE": 0}
        for event in events:
            identity = (event["run"].get("facets") or {}).get("haiIdentity") or {}
            own = (
                event["job"].get("namespace") == lineage.JOB_NAMESPACE
                and event["job"]["name"] == job
                and event["run"]["runId"] == run_uuid
                and identity.get("runId", record["run_id"]) == record["run_id"]
                and identity.get("attempt", record["attempt"]) == record["attempt"]
            )
            if not own or event["eventType"] not in seen:
                continue
            versions = {(item["namespace"], item["name"]): ((item.get("facets") or {}).get("version") or {}).get("datasetVersion") for item in event["inputs"]}
            require(
                all(found is None or found == wanted.get(key) for key, found in versions.items()),
                f"a {event['eventType']} event of {job} carries another input version",
            )
            seen[event["eventType"]] += all(versions.get(key) == value for key, value in wanted.items())
        require(all(seen.values()), f"the events of {job} lack the pinned input versions on START and terminal")
    return {"consumers": len(records), "inputs": sum(len(record["ledger"][0]["input_pins"]) for record in records)}


def input_pins(
    stack: fixture.Stack, port: int, run_id: str, dump: Path, host: list[dict[str, Any]], outbox: Path, native: list[dict[str, Any]]
) -> dict[str, Any]:
    """Every consuming invocation's pins appear on its own host, delivered and Marquez-stored events, and the silver input is the planted bytes in Marquez."""
    wanted = hashlib.sha256(transform_fixtures.FIXTURE_BRONZE_BYTES).hexdigest()
    records = consumer_records(stack, run_id)
    silver = next((row for row in records if row["template"] == "silver_build"), None)
    require(silver is not None, "silver build completion record is missing")
    if silver is None:
        raise AssertionError("silver build record is missing")
    pins = silver["ledger"][0].get("input_pins") or []
    require(len(pins) == 1 and pins[0]["snapshot"] == {"kind": "file_sha256", "id": wanted}, "the silver build intent did not freeze the planted fixture bytes")
    listing = get(port, f"/api/v1/namespaces/{quote(pins[0]['namespace'], safe='')}/datasets?limit=1000&offset=0")
    found = [row for row in listing["datasets"] if row["name"] == pins[0]["name"]]
    require(len(found) == 1, "Marquez omitted the fixture input dataset")
    facet = (found[0].get("facets") or {}).get("version") or {}
    require("datasetVersion" in facet, "Marquez shows no pinned identity for the fixture input")
    require(facet["datasetVersion"] == wanted, "Marquez shows another identity for the fixture input")
    runs = invocation_runs(records, native)
    verify_consumer_facets(records, host, runs)
    verify_consumer_facets(records, delivered_events(outbox), runs)
    return {**verify_consumer_facets(records, stored_events(dump.read_text(encoding="utf-8")), runs), "sources": ["host", "outbox", "marquez raw rows"]}


def check(
    stack: fixture.Stack, case: str, api_port: int, web_port: int, progress: dict[str, Any], resources: ResourceState, mutation: str | None = None
) -> dict[str, Any]:
    discovered = stack.wait_for_dags((fixture.DAG,), seconds=30)
    require(all(discovered["present"].values()) and not discovered["import_errors"], "real scheduler did not import the fixture DAG")
    worker = stack.supervisor.emitter
    require(worker is not None, "automatic supervisor emitter was not scheduled")
    if worker is None:
        raise AssertionError("automatic emitter is missing")
    if case != "outage":
        start_marquez(stack, api_port, web_port)
    else:
        require(stack.client.call("status", {"what": "lineage"}, timeout=5)["phase"] == "down", "outage fixture unexpectedly has Marquez running")
    progress["setup_complete"] = True
    run_id = fixture.new_run_id()
    stack.seen_runs.add(run_id)
    values = [*fixture.SENTINELS.values(), stack.supervisor.token]
    observation = stack.run(
        fixture.DAG,
        run_id,
        overlay="bad_gold_build" if case == "failure" else "good",
        conf={"run_id": run_id, "mode": "fixture", "lineage_sentinels": values},
        wait=150,
    )
    progress.update(subject_reached=True, dag_state=observation["dag_run_state"])
    require(observation["dag_run_state"] == ("failed" if case == "failure" else "success"), "DAG outcome changed under lineage conditions")
    require(all(value in ("success", "failed", "skipped", "upstream_failed") for value in observation["tasks"].values()), "fixture tasks did not settle")
    native = native_events(stack, run_id)
    progress["native_events"] = len(native)
    executed = {task for task, state in observation["tasks"].items() if state in ("success", "failed")}
    require(executed <= {row["caller"].get("task_id") for row in native}, "native provider omitted an executed task run")
    dag_events = [row for row in native if "task_id" not in row["caller"]]
    require({row["event"]["eventType"] for row in dag_events} >= {"START", "FAIL" if case == "failure" else "COMPLETE"}, "native DAG lifecycle is incomplete")
    expected = expected_events(stack, run_id, native)
    if case == "outage":
        worker.wake()

        def collected() -> bool:
            if not worker.outbox.exists():
                return False
            with lineage.Outbox(worker.outbox) as box:
                queued = {row[0] for row in box.connection.execute("SELECT id FROM events")}
                queued.update(row[0] for row in box.connection.execute("SELECT id FROM provider_events WHERE flushed=2"))
                deferred = box.connection.execute("SELECT COUNT(*) FROM provider_events WHERE flushed=0").fetchone()[0]
                cycle = box.record_cycle()
            return event_ids(expected) <= queued and not deferred and worker.outcome.get("record_cycle", -1) >= cycle and "pending" in worker.outcome

        wait(collected, 30, "outage collects all expected callbacks before checking delivery")
        with lineage.Outbox(worker.outbox) as box:
            require(bool(box.pending(1000)), "outage has no durable unacknowledged event bytes")
        start_marquez(stack, api_port, web_port)
    delivered = drain(worker, expected)
    before = runs(api_port, expected)
    graph_result: dict[str, Any]
    if case == "failure":
        failed = [row for row in expected if row["eventType"] == "FAIL" and row["job"]["name"].endswith(".gold_build")]
        require(failed and before[failed[0]["job"]["name"]][failed[0]["run"]["runId"]] == "FAILED", "forced task failure lost its identity or FAIL state")
        require(
            any(job.endswith(".silver_build") and "COMPLETED" in states.values() for job, states in before.items()),
            "forced failure erased an upstream successful task",
        )
        graph_result = {"not_run": "failed gold has no successful output"}
    else:
        graph_result = graph(api_port, expected)
    evidence: dict[str, Any] = {}
    if case == "outage":
        if mutation == "resend_early":
            progress["mutation_invocations"] = 1
            resend(worker, expected)
        evidence["recovery_rows"] = progress["recovery_rows"] = require_one_row_per_event(dump_database(stack, resources), worker.outbox)
        require(
            resend(worker, expected) == delivered and runs(api_port, expected) == before, "lost-acknowledgment resend changed the Marquez run set or states"
        )
    scan = database_scan(stack, dag_events[0]["event"]["run"]["runId"], values, progress, resources)
    dump = stack.state / "lineage" / "dumps" / "marquez.sql"
    if case == "graph":
        evidence["input_pins"] = input_pins(stack, api_port, run_id, dump, expected, worker.outbox, native)
    if case == "outage":
        evidence["raw_rows_after_resend"] = {
            **compare_rows(stored_event_rows(dump.read_text(encoding="utf-8")), delivered_rows(worker.outbox)),
            "asserted": False,
        }
    return {
        "dag_state": observation["dag_run_state"],
        "executed_tasks": len(executed),
        "native_events": len(native),
        "events_delivered": delivered,
        "marquez_jobs": len(before),
        "graph": graph_result,
        "sentinel": scan,
        **evidence,
    }


def cleanup(stack: fixture.Stack, resources: ResourceState) -> dict[str, Any]:
    errors = []
    if resources.acquisition_unknown:
        errors.append("lineage allocation acquisition outcome is unknown")
    for allocation in sorted(resources.allocations):
        try:
            if stack.client.release_allocation(allocation) == launcher.ReleaseOutcome.RELEASED:
                resources.allocations.discard(allocation)
            else:
                errors.append("lineage allocation remains retained")
        except (OSError, RuntimeError, launcher.ClientError) as error:
            errors.append(f"lineage allocation cleanup: {type(error).__name__}")
    if hasattr(stack, "supervisor"):
        for run_id in sorted(stack.seen_runs):
            try:
                principal = {**stack.client.principal_run(run_id), "owner": True}
                record = stack.work / f"{run_id}_lineage_owner.json"
                record.write_text(json.dumps({"run_id": run_id}))
                state = stack.client.call("status", {"what": "run", "id": run_id}, principal, timeout=5)["state"]
                if state == "held":
                    stack.client.call("finalize", {}, principal, timeout=5)
                released = stack.client.call("release", {"record": str(record), "owner": True}, principal, timeout=15)
                require(released.get("state") == "released", "known fixture run reservation was not released")
            except (OSError, RuntimeError, launcher.ClientError, AssertionError) as error:
                errors.append(f"known run release: {type(error).__name__}")
        result = stack.supervisor.teardown()
        errors.extend(result.get("errors", []))
    if hasattr(stack, "client"):
        try:
            stack.client.call("lineage_down", timeout=5)
            wait(lambda: stack.client.call("status", {"what": "lineage"}, timeout=5)["phase"] == "down", 30, "Marquez teardown")
            state = stack.client.call("status", {"what": "launcher"}, timeout=5)
            require(not (state["open_attempts"] or state["containers"] or state["networks"]), "launcher still owns fixture work")
            stack.client.start = False
            stack.client.call("shutdown", {"owner": True, "launch_nonce": stack.client.launch_nonce}, {"kind": "any", "owner": True}, timeout=5)
            require(bool(stack.launcher_process), "launcher process identity was not captured")
            problem = fixture.launcher_exit_problem(stack.launcher_process)
            require(problem is None, "launcher process exit was not confirmed")
        except (OSError, RuntimeError, launcher.ClientError, AssertionError) as error:
            errors.append(f"launcher cleanup: {error_label(error)}")
    return {
        "status": "done" if not errors else "incomplete",
        "errors": errors,
        "retained_work": str(stack.work),
        "known_allocations_retained": sorted(resources.allocations),
        "allocation_acquisition_unknown": resources.acquisition_unknown,
        "registry_count_inspection": "excluded",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("graph", "outage", "failure"), required=True)
    parser.add_argument("--mutation", choices=sorted(MUTATION_CASES), help="a deliberate violation for native failing-first evidence")
    args = parser.parse_args()
    if args.mutation is not None and args.case != MUTATION_CASES[args.mutation]:
        parser.error(f"the {args.mutation} mutation requires the {MUTATION_CASES[args.mutation]} case")
    output = ROOT / "data/e2e/lineage_scheduler" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    stack = fixture.Stack(Path(tempfile.mkdtemp(prefix="hls_", dir="/tmp")).resolve())
    resources = ResourceState()
    result: dict[str, Any] = {
        "status": "stop",
        "case": args.case,
        "mutation": args.mutation,
        "work_seconds": 420,
        "cleanup_seconds": 120,
        "progress": {"setup_complete": False, "subject_reached": False},
        "mutation_invocations": 0,
    }
    api_port, web_port = fixture.free_port(), fixture.free_port()
    require(api_port != web_port, "temporary lineage ports collided")
    CachedSupervisor.lineage_port = api_port
    sources = (
        "orchestration/launcher.py",
        "orchestration/lineage.py",
        "scripts/orchestration/supervisor.py",
        "scripts/orchestration/transform_fixtures.py",
        "scripts/orchestration/run_lineage_scheduler_e2e.py",
        "scripts/orchestration/broker.py",
        "orchestration/snapshots.py",
        "data_contracts/orchestration/input_snapshot_record.schema.json",
        "services/orchestration/docker-compose.orchestration.yaml",
        "services/lineage/docker-compose.lineage.yaml",
        "services/lineage/docker-compose.checks.yaml",
        "services/lineage/db_dump.py",
        "services/lineage/provider_events.py",
        "services/lineage/provider_transport.py",
        "services/lineage/delivery.json",
        "services/lineage/openlineage.yml",
    )
    before = {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in sources}
    try:
        with deadline(420), patch.object(fixture, "Supervisor", CachedSupervisor), patch.object(fixture, "START_ATTEMPTS", 1), ExitStack() as mutations:
            if args.mutation == "correlation":
                original_link = lineage.Outbox.provider_run

                def unrelated(box: lineage.Outbox, request: dict[str, Any]) -> dict[str, Any] | None:
                    link = original_link(box, request)
                    if link is not None:
                        result["mutation_invocations"] += 1
                        return {**link, "run_id": str(uuid.uuid5(uuid.UUID(link["run_id"]), "uncorrelated-fixture"))}
                    return None

                mutations.enter_context(patch.object(lineage.Outbox, "provider_run", unrelated))
            elif args.mutation == "delivery":

                def forget(_host: str, _port: int, _payload: str, _deadline: float) -> bool:
                    result["mutation_invocations"] += 1
                    return True

                mutations.enter_context(patch.object(lineage, "post_event", forget))
            elif args.mutation == "sentinel":
                original_enqueue = lineage.Outbox.enqueue_body

                def leak(box: lineage.Outbox, event_id: str, event: dict[str, Any], sequence: int) -> int:
                    result["mutation_invocations"] += 1
                    return original_enqueue(
                        box, event_id, {**event, "producer": provider_events.PRODUCER + "?leak=" + fixture.SENTINELS["AWS_SECRET_ACCESS_KEY"]}, sequence
                    )

                mutations.enter_context(patch.object(lineage.Outbox, "enqueue_body", leak))
            elif args.mutation in ("input_facet", "input_facet_selective"):
                kept_enqueue = lineage.Outbox.enqueue_body

                def unpinned(box: lineage.Outbox, event_id: str, event: dict[str, Any], sequence: int) -> int:
                    selected = args.mutation == "input_facet" or event["job"]["name"].endswith(".gold_build")
                    stripped = [{key: value for key, value in item.items() if key != "facets"} for item in event["inputs"]] if selected else event["inputs"]
                    result["mutation_invocations"] += stripped != event["inputs"]
                    return kept_enqueue(box, event_id, {**event, "inputs": stripped}, sequence)

                mutations.enter_context(patch.object(lineage.Outbox, "enqueue_body", unpinned))
            stack.start()
            result["checks"] = check(stack, args.case, api_port, web_port, result["progress"], resources, args.mutation)
            result["status"] = "pass"
    except (OSError, ValueError, RuntimeError, launcher.ClientError, SupervisorError, AirflowApiError, AssertionError, KeyError, StopIteration) as error:
        result["error_type"] = type(error).__name__
        if isinstance(error, launcher.ClientError):
            result["error_text"] = error_label(error)
        if isinstance(error, CheckFailure):
            result["failed_check"] = str(error)
    finally:
        try:
            with deadline(120):
                result["cleanup"] = cleanup(stack, resources)
        except (OSError, ValueError, RuntimeError, SupervisorError, launcher.ClientError) as error:
            result["cleanup"] = {"status": "incomplete", "error_type": type(error).__name__, "retained_work": str(stack.work)}
        for name in fixture.SENTINELS:
            os.environ.pop(name, None)
    if result["cleanup"]["status"] != "done":
        result["status"] = "stop"
    result["mutation_invocations"] += result["progress"].get("mutation_invocations", 0)
    result["source_unchanged"] = before == {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in sources}
    if not result["source_unchanged"]:
        result["status"] = "stop"
    result["source_sha256"] = before
    result["boundary"] = "real scheduler and tiny native fixture; no real catalog, bronze, AWS or S3; no other LAN host"
    values = [*fixture.SENTINELS.values(), getattr(getattr(stack, "supervisor", None), "token", "")]
    if any(value and value in json.dumps(result) for value in values):
        result = {"status": "stop", "error": "report withheld because it contains a planted value", "cleanup": {"status": result["cleanup"]["status"]}}
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": "native lineage verification", "created": datetime.now(UTC).isoformat(), "end_condition": "no record cites this run"}
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"status": result["status"], "report": str(output / "report.json"), "cleanup": result["cleanup"]["status"]}) + "\n")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
