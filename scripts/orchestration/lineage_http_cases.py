"""The scheduler's OpenLineage transport against a loopback stand-in for OpenMetadata's lineage route (no live call).

Run inside the pinned Airflow image by ``in_image_checks.py`` (section ``lineage_http_transport``), where the OpenLineage client and ``hai_lineage`` import.
The cases build the client the way the scheduler does, from the OPENLINEAGE__TRANSPORT__ variables of ``infra/services/airflow.tf``, and judge what the stand-in
receives. The Z form of the time is accepted by OpenMetadata 2.0.7 and the +00:00 form is refused with 400 (observed live Nov 16 2026).
"""

from __future__ import annotations

import calendar
import json
import os
import re
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import DatasetEvent, Job, JobEvent, Run, RunEvent, RunState, StaticDataset
from openlineage.client.facet_v2 import parent_run
from openlineage.client.serde import Serde
from openlineage.client.transport.http import HttpTransport

TRANSPORT_TYPE = "hai_lineage.http_transport.UtcHttpTransport"
ENDPOINT = "api/v1/openlineage/lineage"
API_KEY = "synthetic-lineage-key"
SLOW_SECONDS = 1.5
RUN_ID, PARENT_ID = "13d9e9ef-93b2-4efc-90d5-066b1becf67d", "3589fe84-f7d0-49fe-b456-5fa256a72938"
NAMESPACE, PRODUCER = "hai-prediction-aws", "https://example.invalid/synthetic-producer"
FAST_FAIL = {"total": 0, "read": 0, "connect": 0}

# (sent eventTime, posted eventTime, milliseconds after TIME_BASE that OpenMetadata has to store): every offset form, naive time, already-Z time and
# fractional seconds of 1, 3, 6 and 9 digits. OpenMetadata 2.0.7 reads the digits after the point as whole milliseconds (see ``stored_millis``), so the
# transport posts exactly three digits (cut, not rounded) or none.
TIME_BASE = calendar.timegm((2026, 11, 16, 19, 31, 22)) * 1000  # 2026-11-16T19:31:22Z, worked out here and not by the code under test
TIME_CASES = [
    ("2026-11-16T19:31:22+00:00", "2026-11-16T19:31:22Z", 0),
    ("2026-11-16T19:31:22.123+00:00", "2026-11-16T19:31:22.123Z", 123),
    ("2026-11-16T19:31:22.123456+00:00", "2026-11-16T19:31:22.123Z", 123),
    ("2026-11-16T19:31:22.123456789+00:00", "2026-11-16T19:31:22.123Z", 123),
    ("2026-11-16T19:31:22.999999+00:00", "2026-11-16T19:31:22.999Z", 999),
    ("2026-11-16T19:31:22.000456+00:00", "2026-11-16T19:31:22Z", 0),
    ("2026-11-16T19:31:22.1+00:00", "2026-11-16T19:31:22.100Z", 100),
    ("2026-11-16T19:31:22+0000", "2026-11-16T19:31:22Z", 0),
    ("2026-11-16T11:31:22-08:00", "2026-11-16T19:31:22Z", 0),
    ("2026-11-17T01:01:22.250+05:30", "2026-11-16T19:31:22.250Z", 250),
    ("2026-11-16T23:59:59.5-02:00", "2026-11-17T01:59:59.500Z", 500 + 6 * 3600 * 1000 + 28 * 60 * 1000 + 37 * 1000),
    ("2026-11-16T19:31:22", "2026-11-16T19:31:22Z", 0),
    ("2026-11-16T19:31:22.123", "2026-11-16T19:31:22.123Z", 123),
    ("2026-11-16T19:31:22Z", "2026-11-16T19:31:22Z", 0),
    ("2026-11-16T19:31:22.1Z", "2026-11-16T19:31:22.100Z", 100),
    ("2026-11-16T19:31:22.123Z", "2026-11-16T19:31:22.123Z", 123),
    ("2026-11-16T19:31:22.123456Z", "2026-11-16T19:31:22.123Z", 123),
    ("2026-11-16T19:31:22.123456789Z", "2026-11-16T19:31:22.123Z", 123),
]
JAVA_TIME = re.compile(r"(20\d{2})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{3}))?Z")


def stored_millis(text: str) -> int | None:
    """The epoch milliseconds OpenMetadata 2.0.7 stores for an eventTime, or None when it refuses the text. Its mapper reads a time with
    ``SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")`` (JsonUtils.java, 2.0.7-release, line 121) after padding a bare-second time with ``.000000``. That
    format takes the digits after the point as one whole number of milliseconds, so ``.123`` is 123 ms and ``.123456`` is 123456 ms. Only ``Z`` is read.
    The same rule is in ``scripts/orchestration/ecs_lineage_cases.py`` (this file runs in the Airflow image, where that module does not import).
    The stand-in accepts only the form production sends (owner A, Nov 17 2026): a year from 2000 to 2099, valid calendar fields, three fraction digits
    or none, and Z. In that form Java's reading equals the instant the text says. Every other form returns None (refused), so no corner of Java's
    lenient parser (other fraction lengths, rolled-over fields, the 1582 calendar cutover) can pass a case."""
    found = JAVA_TIME.fullmatch(text)
    if found is None:
        return None
    year, month, day, hour, minute, second = (int(part) for part in found.groups()[:6])
    try:
        datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None
    seconds = calendar.timegm((year, month, day, hour, minute, second))
    return seconds * 1000 + int(found.group(7) or 0)


class StandIn(ThreadingHTTPServer):
    """Records every POST. ``mode`` is a status code or ``slow``."""

    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.received: list[dict[str, Any]] = []
        self.mode: int | str = 200

    def handle_error(self, request: Any, client_address: Any) -> None:
        """A client that gave up on the slow answer is the case under test, so its broken pipe is not printed."""

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


class Handler(BaseHTTPRequestHandler):
    server: StandIn

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.server.received.append({"path": self.path, "authorization": self.headers.get("Authorization"), "body": json.loads(body)})
        if self.server.mode == "slow":
            time.sleep(SLOW_SECONDS)
        status = 200 if self.server.mode == "slow" else int(self.server.mode)
        self.send_response(status)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *_: Any) -> None:
        return


@contextmanager
def stand_in() -> Iterator[StandIn]:
    server = StandIn()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def scheduler_environment(url: str) -> dict[str, str]:
    """The settings of ``airflow_lineage_http_environment`` and ``airflow_lineage_http_secrets`` in infra/services/airflow.tf, pointed at the stand-in."""
    return {
        "OPENLINEAGE__TRANSPORT__TYPE": TRANSPORT_TYPE,
        "OPENLINEAGE__TRANSPORT__URL": url,
        "OPENLINEAGE__TRANSPORT__ENDPOINT": ENDPOINT,
        "OPENLINEAGE__TRANSPORT__TIMEOUT": "5",
        "OPENLINEAGE__TRANSPORT__AUTH__TYPE": "api_key",
        "OPENLINEAGE__TRANSPORT__AUTH__API_KEY": API_KEY,
    }


@contextmanager
def scheduler_client(url: str) -> Iterator[OpenLineageClient]:
    """The client as the scheduler builds it: no config file, no OPENLINEAGE_ switches, only the variables."""
    saved_environment, saved_folder = dict(os.environ), os.getcwd()
    with tempfile.TemporaryDirectory() as folder:
        try:
            for name in [name for name in os.environ if name.startswith("OPENLINEAGE")]:
                del os.environ[name]
            os.environ.update(scheduler_environment(url))
            os.chdir(folder)
            client = OpenLineageClient()
            try:
                yield client
            finally:
                client.close(0)
        finally:
            os.chdir(saved_folder)
            os.environ.clear()
            os.environ.update(saved_environment)


def run_event(state: RunState, event_time: str) -> RunEvent:
    facet = parent_run.ParentRunFacet(run=parent_run.Run(runId=PARENT_ID), job=parent_run.Job(namespace=NAMESPACE, name="transform_dag"))
    return RunEvent(
        eventType=state,
        eventTime=event_time,
        run=Run(runId=RUN_ID, facets={"parent": facet}),
        job=Job(namespace=NAMESPACE, name="transform_dag.silver_build"),
        producer=PRODUCER,
    )


def expected_body(event: Any, event_time: str) -> dict[str, Any]:
    """The body the plain client would post, with only eventTime changed."""
    body: dict[str, Any] = json.loads(Serde.to_json(event))
    body["eventTime"] = event_time
    return body


def posted_once(server: StandIn, send: Any) -> dict[str, Any]:
    server.received.clear()
    send()
    if len(server.received) != 1:
        raise ValueError(f"expected one post, the stand-in saw {len(server.received)}")
    return server.received[0]


def failure_type(transport: Any, event: Any) -> str:
    """The class name of what ``transport.emit`` raises, or ``none``; the transport is closed afterwards."""
    try:
        transport.emit(event)
    except Exception as error:
        return type(error).__name__
    finally:
        transport.close()
    return "none"


def time_cases(server: StandIn, failures: list[str]) -> int:
    count = 0
    with scheduler_client(server.url) as client:
        for sent, wanted, after in TIME_CASES:
            for state in RunState:
                event = run_event(state, sent)
                got = posted_once(server, lambda event=event: client.emit(event))
                count += 1
                if got["path"] != f"/{ENDPOINT}" or got["authorization"] != f"Bearer {API_KEY}":
                    failures.append(f"{state.value} {sent}: route or credential differs from the plain HTTP transport ({got['path']})")
                elif got["body"] != expected_body(event, wanted):
                    failures.append(f"{state.value} {sent}: posted {got['body']['eventTime']}, wanted {wanted} with the rest of the event unchanged")
                elif stored_millis(got["body"]["eventTime"]) != TIME_BASE + after:
                    failures.append(f"{state.value} {sent}: OpenMetadata stores {stored_millis(got['body']['eventTime'])}, the instant is {TIME_BASE + after}")
                if event.eventTime != sent:
                    failures.append(f"{state.value} {sent}: the caller's event was changed to {event.eventTime}")
        job = Job(namespace=NAMESPACE, name="transform_dag.silver_build")
        others: list[tuple[DatasetEvent | JobEvent, str]] = [
            (
                DatasetEvent(eventTime="2026-11-16T11:31:22.5-08:00", dataset=StaticDataset(namespace=NAMESPACE, name="gold.encounters"), producer=PRODUCER),
                "2026-11-16T19:31:22.500Z",
            ),
            (JobEvent(eventTime="2026-11-16T19:31:22+00:00", job=job, producer=PRODUCER), "2026-11-16T19:31:22Z"),
        ]
        for other, wanted in others:
            got = posted_once(server, lambda other=other: client.emit(other))
            count += 1
            if got["body"] != expected_body(other, wanted):
                failures.append(f"{type(other).__name__}: posted {got['body']['eventTime']}, wanted {wanted}")
    return count


def resolution(server: StandIn, failures: list[str]) -> str:
    from hai_lineage import http_transport

    with scheduler_client(server.url) as client:
        transport = client.transport
        if type(transport) is not http_transport.UtcHttpTransport or not isinstance(transport, HttpTransport):
            failures.append(f"the scheduler settings resolve to {type(transport).__module__}.{type(transport).__qualname__}, not the project transport")
        elif (transport.url, transport.endpoint, transport.timeout) != (server.url, ENDPOINT, 5):
            failures.append(f"url, endpoint or timeout differ from the settings: {transport.url} {transport.endpoint} {transport.timeout}")
        return f"{type(transport).__module__}.{type(transport).__qualname__}"


def helper_cases(failures: list[str]) -> int:
    """The rewrite on objects that are not events, a missing or odd time and a naive or aware datetime."""
    import attr
    from hai_lineage import http_transport

    @attr.define
    class Timed:
        eventTime: Any
        other: str = "kept"

    untouched = [object(), "text", None, {"eventTime": "2026-11-16T19:31:22+00:00"}, Timed(None), Timed(""), Timed("not a time"), Timed("2026-11-16"), Timed(7)]
    for value in untouched:
        if http_transport.utc_event(value) is not value:
            failures.append(f"{value!r} was replaced although it has no time to rewrite")
    aware, naive = datetime(2026, 11, 16, 19, 31, 22, 500000, tzinfo=UTC), datetime(2026, 11, 16, 19, 31, 22)
    for value, wanted in ((aware, "2026-11-16T19:31:22.500Z"), (naive, "2026-11-16T19:31:22Z")):
        got = http_transport.utc_event(Timed(value))
        if got.eventTime != wanted or got.other != "kept":
            failures.append(f"datetime {value!r} became {got.eventTime!r}, wanted {wanted!r}")
    event = Timed("2026-11-16T19:31:22+00:00")
    if http_transport.utc_event(event).eventTime != "2026-11-16T19:31:22Z" or event.eventTime != "2026-11-16T19:31:22+00:00":
        failures.append("an attrs object was changed in place or not rewritten")
    return len(untouched) + 3


def error_cases(server: StandIn, failures: list[str]) -> int:
    """A refused, failed, unreachable or slow send raises exactly what the plain HTTP transport raises, so the provider's own handler (adapter.emit) logs it."""
    from hai_lineage import http_transport

    event = run_event(RunState.COMPLETE, "2026-11-16T19:31:22+00:00")
    closed = StandIn()
    dead_url = closed.url
    closed.server_close()
    settings = {"type": TRANSPORT_TYPE, "endpoint": ENDPOINT, "timeout": 0.5, "auth": {"type": "api_key", "api_key": API_KEY}, "retry": FAST_FAIL}
    cases = (("400", server.url, 400), ("500", server.url, 500), ("slow", server.url, "slow"), ("unreachable", dead_url, 200))
    for label, url, mode in cases:
        server.mode = mode
        project = http_transport.UtcHttpTransport(http_transport.UtcHttpTransport.config_class.from_dict({**settings, "url": url}))
        plain = HttpTransport(HttpTransport.config_class.from_dict({**settings, "url": url}))
        got, base = failure_type(project, event), failure_type(plain, event)
        if got == "none" or got != base:
            failures.append(f"{label}: project transport raised {got}, the plain HTTP transport {base}")
    server.mode = 200
    return len(cases)


def run() -> dict[str, Any]:
    """Every case; ``ok`` is true only when no case failed."""
    failures: list[str] = []
    with stand_in() as server:
        resolved = resolution(server, failures)
        posts = time_cases(server, failures)
        others = helper_cases(failures) + error_cases(server, failures)
    return {"ok": not failures, "resolved": resolved, "posts": posts, "other_cases": others, "failures": failures[:20], "network": "loopback stand-in only"}


if __name__ == "__main__":
    report = run()
    sys.stdout.write(json.dumps(report, indent=2) + "\n")
    raise SystemExit(0 if report["ok"] else 1)
