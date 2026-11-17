"""Cases for the job metrics push: a stand-in Pushgateway on loopback receives what jobs push, and each failure mode must leave the job's result alone.

The stand-in keeps one group per grouping key (job, phase, outcome) like the real Pushgateway: a PUT replaces the group, a DELETE removes it, and a body that is
not valid exposition text is refused with 400. It can also refuse, answer slowly or drop the connection. No AWS service is contacted. Every stand-in server is
closed before its case returns. The drift case starts the real ``monitoring.aws_run check`` command as a child process with the push variables set.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import math
import os
import re
import socket
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock
from urllib.parse import unquote, urlparse

from scripts import metrics_push, resource_peaks
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = str(REPO_ROOT / ".venv/bin/python")
LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})? (\S+)$")
Key = tuple[str, str, str]


def valid_exposition(body: str) -> bool:
    """The text a real Pushgateway accepts: comment or sample lines, finite or NaN numbers, one trailing newline."""
    if not body.endswith("\n") or body.endswith("\n\n"):
        return False
    for line in body.splitlines():
        if line.startswith("# HELP ") or line.startswith("# TYPE "):
            continue
        match = LINE.match(line)
        if match is None:
            return False
        try:
            float(match.group(3))
        except ValueError:
            return False
    return True


class StandIn:
    """A loopback Pushgateway. ``put_status`` and ``delete_status`` refuse every request of that method; ``delay`` slows every answer; ``drop`` closes the
    socket."""

    def __init__(self, *, put_status: int = 200, delete_status: int = 202, delay: float = 0.0, drop: bool = False) -> None:
        self.groups: dict[Key, str] = {}
        self.requests: list[tuple[str, str]] = []
        self.put_status, self.delete_status, self.delay, self.drop = put_status, delete_status, delay, drop
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def key(self) -> Key | None:
                parts = [unquote(part) for part in urlparse(self.path).path.strip("/").split("/")]
                if len(parts) != 7 or parts[0] != "metrics" or parts[1] != "job" or parts[3] != "phase" or parts[5] != "outcome":
                    return None
                return parts[2], parts[4], parts[6]

            def reply(self, status: int) -> None:
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def handle_request(self, method: str) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode("utf-8", "replace") if length else ""
                stand_in.requests.append((method, self.path))
                if stand_in.delay:
                    time.sleep(stand_in.delay)
                if stand_in.drop:
                    self.connection.close()
                    return
                key = self.key()
                if key is None:
                    self.reply(404)
                elif method == "DELETE":
                    if stand_in.delete_status < 300:
                        stand_in.groups.pop(key, None)
                    self.reply(stand_in.delete_status)
                elif stand_in.put_status >= 300:
                    self.reply(stand_in.put_status)
                elif not valid_exposition(body):
                    self.reply(400)
                else:
                    stand_in.groups[key] = body
                    self.reply(200)

            def do_PUT(self) -> None:
                self.handle_request("PUT")

            def do_DELETE(self) -> None:
                self.handle_request("DELETE")

            def log_message(self, format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self) -> StandIn:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)

    def names(self, key: Key) -> set[str]:
        """The metric names in one group."""
        return {line.split("{")[0].split(" ")[0] for line in self.groups.get(key, "").splitlines() if line and not line.startswith("#")}

    def value(self, key: Key, name: str, labels: str = "") -> float | None:
        for line in self.groups.get(key, "").splitlines():
            if line.startswith(f"{name}{labels} "):
                return float(line.rsplit(" ", 1)[1])
        return None


RUNNING: Key = ("drift", "check", "running")
SUCCEEDED: Key = ("drift", "check", "succeeded")
FAILED: Key = ("drift", "check", "failed")


def pusher(url: str, job: str = "drift", phase: str = "check", **options: Any) -> metrics_push.Pusher:
    return metrics_push.Pusher(url, job, phase, **options)


def quiet(call: Callable[[], Any]) -> tuple[Any, str]:
    """The call's result and what it wrote to standard error."""
    stream = io.StringIO()
    with contextlib.redirect_stderr(stream):
        result = call()
    return result, stream.getvalue()


def finish_ok(target: metrics_push.Pusher, outcome: str = "succeeded", **extra: Any) -> bool:
    return target.finish(outcome, exit_code=0 if outcome == "succeeded" else 1, duration=12.5, peak_memory=2048, peak_scratch=4096, **extra)


def push_puts_the_group_with_valid_exposition(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        accepted, _ = quiet(lambda: finish_ok(pusher(stand_in.url)))
    body = stand_in.groups.get(SUCCEEDED, "")
    return {
        "the push is accepted": accepted is True,
        "the group is stored under job, phase and outcome": list(stand_in.groups) == [SUCCEEDED],
        "the exposition is valid": valid_exposition(body),
        "the exit metrics are exactly the contract": stand_in.names(SUCCEEDED)
        == {
            "hai_job_finish_timestamp_seconds",
            "hai_job_duration_seconds",
            "hai_job_exit_code",
            "hai_job_peak_memory_bytes",
            "hai_job_peak_scratch_bytes",
        },
        "values are exact": stand_in.value(SUCCEEDED, "hai_job_duration_seconds") == 12.5 and stand_in.value(SUCCEEDED, "hai_job_peak_memory_bytes") == 2048,
        "a gauge type line precedes each metric": "# TYPE hai_job_duration_seconds gauge\n" in body,
    }


def unavailable_scratch_is_left_out(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        quiet(lambda: pusher(stand_in.url).finish("succeeded", exit_code=0, duration=1.0, peak_memory=None, peak_scratch=None))
    return {
        "a measure that is unavailable is absent, not zero": "hai_job_peak_memory_bytes" not in stand_in.names(SUCCEEDED)
        and "hai_job_peak_scratch_bytes" not in stand_in.names(SUCCEEDED),
        "the other metrics stay": "hai_job_duration_seconds" in stand_in.names(SUCCEEDED),
    }


def drift_samples_carry_closed_labels(root: Path) -> dict[str, bool]:
    samples = [
        metrics_push.Sample("hai_drift_share", 0.25, {"hai_type": "HAI_1"}),
        metrics_push.Sample("hai_drift_status", 1.0, {"hai_type": "HAI_1", "status": "checked"}),
    ]
    with StandIn() as stand_in:
        quiet(lambda: finish_ok(pusher(stand_in.url), samples=samples))
    body = stand_in.groups.get(SUCCEEDED, "")
    return {
        "a labelled sample is exposed with its labels": 'hai_drift_share{hai_type="HAI_1"} 0.25' in body,
        "two labels keep their order": 'hai_drift_status{hai_type="HAI_1",status="checked"} 1' in body,
        "the body is valid": valid_exposition(body),
    }


def refused_samples_are_dropped_and_named(root: Path) -> dict[str, bool]:
    bad = [
        metrics_push.Sample("hai_drift_share", 0.1, {"run_id": "20261117T120000Z-0a1b2c3d"}),
        metrics_push.Sample("hai_drift_share", 0.1, {"hai_type": "HAI_9"}),
        metrics_push.Sample("hai_unknown_metric", 1.0, {}),
        metrics_push.Sample("hai_drift_share", math.inf, {"hai_type": "HAI_2"}),
    ]
    with StandIn() as stand_in:
        accepted, text = quiet(lambda: finish_ok(pusher(stand_in.url), samples=bad))
    body = stand_in.groups.get(SUCCEEDED, "")
    return {
        "the group is still pushed": accepted is True and valid_exposition(body),
        "a run ID label is never sent": "run_id" not in body and "20261117" not in body,
        "an unknown label value is never sent": "HAI_9" not in body,
        "an unknown metric is never sent": "hai_unknown_metric" not in body,
        "a non-finite value is never sent": "Inf" not in body,
        "each refusal is named on standard error": text.count("event=metrics_push_dropped") == 4,
    }


def grouping_keys_must_be_closed(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        settings = {"PUSHGATEWAY_URL": stand_in.url, "HAI_JOB": "drift"}
        good = metrics_push.Pusher.create(settings, ["monitoring/aws_run.py", "check", "--run", "x"])
        unknown_job = quiet(lambda: metrics_push.Pusher.create({**settings, "HAI_JOB": "not_a_job"}, ["x", "check"]))
        unknown_phase = quiet(lambda: metrics_push.Pusher.create(settings, ["x", "nonsense"]))
        run_id_as_phase = quiet(lambda: metrics_push.Pusher.create(settings, ["x", "20261117T120000Z-0a1b2c3d"]))
        unset = metrics_push.Pusher.create({"HAI_JOB": "drift"}, ["x", "check"])
        no_job = quiet(lambda: metrics_push.Pusher.create({"PUSHGATEWAY_URL": stand_in.url}, ["x", "check"]))
        https = quiet(lambda: metrics_push.Pusher.create({**settings, "PUSHGATEWAY_URL": "https://example.org:9091"}, ["x", "check"]))
        refused_outcome, _ = quiet(lambda: pusher(stand_in.url).finish("exploded", exit_code=1, duration=1.0))
    return {
        "a closed job and phase make a pusher": good is not None,
        "an unknown job makes none and says why": unknown_job[0] is None and "event=metrics_push_skipped" in unknown_job[1],
        "an unknown phase makes none": unknown_phase[0] is None and "event=metrics_push_skipped" in unknown_phase[1],
        "a run ID is not a phase": run_id_as_phase[0] is None,
        "an unset address is a silent no-op": unset is None,
        "a missing job name is skipped and named": no_job[0] is None and "HAI_JOB" in no_job[1],
        "only plain http is used": https[0] is None,
        "an unknown outcome sends nothing": refused_outcome is False and stand_in.requests == [],
    }


def refusal_is_reported_not_raised(root: Path) -> dict[str, bool]:
    with StandIn(put_status=500) as stand_in:
        accepted, text = quiet(lambda: finish_ok(pusher(stand_in.url)))
    return {
        "a 500 returns False": accepted is False,
        "the reason names the status": "event=metrics_push_failed" in text and "500" in text,
        "no group is stored": stand_in.groups == {},
    }


def bad_exposition_refusal_is_reported(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        target = pusher(stand_in.url)
        with mock.patch.object(metrics_push, "exposition", lambda _samples: "not exposition"):
            accepted, text = quiet(lambda: finish_ok(target))
    return {"a 400 returns False": accepted is False, "the reason names the status": "400" in text}


def timeout_is_bounded(root: Path) -> dict[str, bool]:
    with StandIn(delay=2.0) as stand_in:
        started = time.monotonic()
        accepted, text = quiet(lambda: finish_ok(pusher(stand_in.url, request_timeout=0.3, total_timeout=1.0)))
        elapsed = time.monotonic() - started
    return {
        "a slow gateway returns False": accepted is False,
        "the call ends within the total timeout": elapsed < 1.6,
        "the reason names a timeout": "timed out" in text or "timeout" in text,
    }


def total_deadline_covers_both_requests(root: Path) -> dict[str, bool]:
    with StandIn(delay=0.4) as stand_in:
        target = pusher(stand_in.url, request_timeout=1.0, total_timeout=0.6)
        started = time.monotonic()
        quiet(lambda: (target.start(), finish_ok(target)))
        elapsed = time.monotonic() - started
    return {"start and finish together stay near the deadline of one push each": elapsed < 3.0}


class DripGateway:
    """A loopback peer that reads the request and then answers one byte every ``gap`` seconds, so every byte arrives inside the inactivity timeout of a read
    while the whole answer takes seven seconds (review round 2, finding 4). It stops when the case ends."""

    ANSWER = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"

    def __init__(self, gap: float = 0.2) -> None:
        self.gap, self.stopped = gap, threading.Event()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.listener.settimeout(0.2)
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.connections: list[socket.socket] = []

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.listener.getsockname()[1]}"

    def serve(self) -> None:
        while not self.stopped.is_set():
            try:
                connection, _ = self.listener.accept()
            except OSError:
                continue
            self.connections.append(connection)
            threading.Thread(target=self.answer, args=(connection,), daemon=True).start()

    def answer(self, connection: socket.socket) -> None:
        try:
            connection.settimeout(2.0)
            seen = b""
            while b"\r\n\r\n" not in seen:
                seen += connection.recv(4096)
            found = re.search(rb"Content-Length: (\d+)", seen, re.I)
            length = int(found.group(1)) if found else 0
            while len(seen.split(b"\r\n\r\n", 1)[1]) < length:
                seen += connection.recv(4096)
            for index in range(len(self.ANSWER)):
                if self.stopped.is_set():
                    return
                connection.sendall(self.ANSWER[index : index + 1])
                time.sleep(self.gap)
        except (OSError, ValueError, AttributeError):
            return

    def __enter__(self) -> DripGateway:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stopped.set()
        self.listener.close()
        for connection in self.connections:
            with contextlib.suppress(OSError):
                connection.close()
        self.thread.join(timeout=5)


def a_slow_answer_that_never_pauses_still_ends_at_the_deadline(root: Path) -> dict[str, bool]:
    with DripGateway() as gateway:
        started = time.monotonic()
        accepted, text = quiet(lambda: finish_ok(pusher(gateway.url, request_timeout=1.0, total_timeout=1.5)))
        elapsed = time.monotonic() - started
    return {
        "the exit push is not accepted, because its answer did not arrive in time": accepted is False,
        "it ends within the total timeout, although bytes arrived all the time": elapsed < 2.0,
        "the reason names the deadline or a timeout": "timed out" in text or "deadline" in text,
    }


def a_slow_answer_bounds_a_sample_push_a_heartbeat_and_a_delete(root: Path) -> dict[str, bool]:
    results: dict[str, float] = {}
    with DripGateway() as gateway:
        target = pusher(gateway.url, request_timeout=0.8, total_timeout=1.5)
        for name, call in (
            ("sample push", lambda: target.push_samples("running", [metrics_push.Sample("hai_spark_tasks_failed", 0)])),
            ("heartbeat", lambda: target.heartbeat(first=True)),
            ("delete", lambda: target.delete_group("running")),
        ):
            started = time.monotonic()
            quiet(call)
            results[name] = time.monotonic() - started
    return {f"a {name} ends within its request timeout": elapsed < 1.3 for name, elapsed in results.items()}


def unreachable_is_reported_not_raised(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        url = stand_in.url
    started = time.monotonic()
    accepted, text = quiet(lambda: finish_ok(pusher(url, request_timeout=1.0, total_timeout=2.0)))
    return {
        "a closed port returns False": accepted is False,
        "it fails fast": time.monotonic() - started < 2.0,
        "the reason names the error type": "ConnectionRefusedError" in text,
    }


def dropped_connection_is_reported_not_raised(root: Path) -> dict[str, bool]:
    with StandIn(drop=True) as stand_in:
        accepted, text = quiet(lambda: finish_ok(pusher(stand_in.url)))
    return {"a connection closed without an answer returns False": accepted is False, "it is logged": "event=metrics_push_failed" in text}


def never_raises_into_the_job(root: Path) -> dict[str, bool]:
    def boom(*_arguments: Any, **_keywords: Any) -> None:
        raise RuntimeError("unexpected")

    with StandIn() as stand_in:
        target = pusher(stand_in.url)
        with mock.patch.object(metrics_push, "exposition", boom):
            result, text = quiet(lambda: finish_ok(target))
        with mock.patch.object(metrics_push, "exchange", boom):
            second, text_two = quiet(lambda: finish_ok(target))
    return {
        "an error in the exposition returns False": result is False and "RuntimeError" in text,
        "an error in the connection returns False": second is False and "RuntimeError" in text_two,
    }


def finish_removes_only_the_running_group(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        target = pusher(stand_in.url)
        quiet(lambda: target.start(memory_current=100, memory_peak=200, scratch_peak=300))
        before = set(stand_in.groups)
        quiet(lambda: finish_ok(target))
    return {
        "start stores the running group": before == {RUNNING},
        "finish leaves the outcome group only": set(stand_in.groups) == {SUCCEEDED},
        "the running group is deleted with one request": [method for method, _ in stand_in.requests] == ["PUT", "PUT", "DELETE"],
    }


def running_group_exposes_the_heartbeat(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        target = pusher(stand_in.url)
        quiet(lambda: target.start(memory_current=100, memory_peak=200, scratch_peak=300))
        first = stand_in.value(RUNNING, "hai_job_heartbeat_timestamp_seconds")
        time.sleep(0.02)
        quiet(lambda: target.heartbeat(memory_current=150, memory_peak=250, scratch_peak=350))
        second = stand_in.value(RUNNING, "hai_job_heartbeat_timestamp_seconds")
    return {
        "the running group names exactly the start and heartbeat metrics": stand_in.names(RUNNING)
        == {
            "hai_job_start_timestamp_seconds",
            "hai_job_heartbeat_timestamp_seconds",
            "hai_job_memory_current_bytes",
            "hai_job_memory_peak_bytes",
            "hai_job_scratch_peak_bytes",
        },
        "a heartbeat replaces the group with newer values": first is not None
        and second is not None
        and second > first
        and stand_in.value(RUNNING, "hai_job_memory_current_bytes") == 150,
        "the start time does not move": stand_in.value(RUNNING, "hai_job_start_timestamp_seconds") is not None,
        "there is still one group": len(stand_in.groups) == 1,
    }


def failure_group_stays_after_a_later_success(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        target = pusher(stand_in.url)
        quiet(lambda: finish_ok(target, "failed"))
        failed = stand_in.value(FAILED, "hai_job_finish_timestamp_seconds")
        time.sleep(0.02)
        quiet(lambda: finish_ok(target, "succeeded"))
    ok = stand_in.value(SUCCEEDED, "hai_job_finish_timestamp_seconds")
    return {
        "both outcome groups exist, so the last failure stays visible": set(stand_in.groups) == {FAILED, SUCCEEDED},
        "the later success is newer than the failure": failed is not None and ok is not None and ok > failed,
        "no request deletes the failure": ("DELETE", "/metrics/job/drift/phase/check/outcome/failed") not in stand_in.requests,
    }


def refused_delete_does_not_fail_the_push(root: Path) -> dict[str, bool]:
    with StandIn(delete_status=500) as stand_in:
        target = pusher(stand_in.url)
        quiet(lambda: target.start())
        accepted, text = quiet(lambda: finish_ok(target))
    return {
        "the outcome group is stored": SUCCEEDED in stand_in.groups,
        "the push still reports success": accepted is True,
        "the failed delete is named": "event=metrics_push_failed" in text and "delete" in text,
        "the stale running group is left for the rules to ignore": RUNNING in stand_in.groups,
    }


def labels_are_url_safe(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        quiet(lambda: finish_ok(pusher(stand_in.url)))
    return {"the path is the three grouping keys only": stand_in.requests == [("PUT", "/metrics/job/drift/phase/check/outcome/succeeded")]}


def phase_table_matches_the_code_and_terraform(root: Path) -> dict[str, bool]:
    def literal(relative: str, name: str) -> Any:
        for node in ast.parse((root / relative).read_text(encoding="utf-8")).body:
            if isinstance(node, ast.Assign) and any(getattr(target, "id", "") == name for target in node.targets):
                return ast.literal_eval(node.value)
        raise AssertionError(f"{name} is not in {relative}")

    families = re.search(
        r'variable "host_job_families".*?default\s*=\s*\[(.*?)\]', (root / "infra/services/host_capacity.tf").read_text(encoding="utf-8"), re.S
    )
    terraform = set(re.findall(r'"([a-z_]+)"', families.group(1))) if families else set()
    layers = literal("scripts/lakehouse/layer_run.py", "PHASES")
    table = metrics_push.JOB_PHASES
    return {
        "the families are the Terraform host job families plus the publisher": set(table) == terraform | {"publisher"},
        "drift phases equal the drift entry module": set(table["drift"]) == set(literal("monitoring/aws_run.py", "PHASES")),
        "bronze phases equal the bronze entry module": set(table["bronze"])
        == set(literal("scripts/lakehouse/bronze_run.py", "PHASES")) | {"inventory", "spark"},
        "each layer's phases equal layer_run": all(set(layers[layer]) <= set(table[layer]) for layer in layers),
        "the features family also names the release and reload container": "feast" in table["features"],
        "train and promote phases are in the training entry module": (set(table["train"]) | set(table["promote"]))
        <= set(literal("scripts/training/aws_run.py", "PHASES")),
        "every phase is a short lowercase word": all(re.fullmatch(r"[a-z_]{1,24}", phase) for phases in table.values() for phase in phases),
        "the outcome set is closed": metrics_push.OUTCOMES == ("running", "succeeded", "failed", "interrupted"),
    }


def recorded_pushes_start_heartbeat_and_exit(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="push_case_") as scratch, StandIn() as stand_in:
        seen: list[set[Key]] = []

        def job() -> int:
            time.sleep(0.35)
            seen.append(set(stand_in.groups))
            return 0

        code, stream = run_recorded(job, Path(scratch), stand_in.url, heartbeat=0.05)
    beats = [request for request in stand_in.requests if request[0] == "PUT" and request[1].endswith("outcome/running")]
    return {
        "the job's exit code passes on": code == 0,
        "the running group existed while the job ran": seen == [{RUNNING}],
        "several heartbeats were pushed": len(beats) >= 3,
        "the exit group replaced it": set(stand_in.groups) == {SUCCEEDED},
        "the exit group has the duration and peaks": stand_in.value(SUCCEEDED, "hai_job_duration_seconds") is not None
        and (stand_in.value(SUCCEEDED, "hai_job_peak_memory_bytes") or 0) > 0
        and stand_in.value(SUCCEEDED, "hai_job_exit_code") == 0,
        "the resource_peaks line is unchanged": sorted(
            json.loads(next(line for line in stream.splitlines() if line.startswith("event=resource_peaks ")).split(" ", 1)[1])
        )
        == ["exit_code", "peak_memory_bytes", "peak_memory_reason", "peak_memory_source", "peak_scratch_bytes", "scratch_measure", "scratch_samples"],
        "no heartbeat thread is left": not any(thread.name == "metrics_heartbeat" for thread in threading.enumerate()),
    }


def run_recorded(
    job: Callable[[], Any], scratch: Path, url: str | None, *, heartbeat: float = 30.0, job_name: str = "drift", argv: list[str] | None = None
) -> tuple[Any, str]:
    """Run a job through the shared recorder with the push variables and return its exit code (or exception) and standard error."""
    stream = io.StringIO()
    environment = {} if url is None else {"PUSHGATEWAY_URL": url, "HAI_JOB": job_name}
    code: Any = None
    with (
        mock.patch.dict(os.environ, environment),
        mock.patch.object(sys, "argv", argv or ["aws_run.py", "check", "--run", "x"]),
        mock.patch.object(metrics_push, "HEARTBEAT_SECONDS", heartbeat),
        contextlib.redirect_stderr(stream),
    ):
        try:
            resource_peaks.recorded(job, scratch=(scratch,), interval=0.02)
        except SystemExit as stop:
            code = stop.code
        except (Exception, KeyboardInterrupt) as error:
            code = error
    return code, stream.getvalue()


def recorded_failures_push_the_failed_outcome(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="push_case_") as scratch, StandIn() as stand_in:
        code, _ = run_recorded(lambda: 3, Path(scratch), stand_in.url)
        failed_exit = stand_in.value(FAILED, "hai_job_exit_code")

        def raises() -> int:
            raise RuntimeError("job failed")

        error, _ = run_recorded(raises, Path(scratch), stand_in.url)
    return {
        "exit code 3 passes on": code == 3,
        "the outcome is failed with the exit code": failed_exit == 3.0,
        "an exception passes on unchanged": isinstance(error, RuntimeError) and str(error) == "job failed",
        "an exception also ends in the failed group with exit code 1": stand_in.value(FAILED, "hai_job_exit_code") == 1.0,
        "no running group is left": RUNNING not in stand_in.groups,
    }


def recorded_interrupt_pushes_interrupted(root: Path) -> dict[str, bool]:
    def interrupted() -> int:
        raise KeyboardInterrupt

    with tempfile.TemporaryDirectory(prefix="push_case_") as scratch, StandIn() as stand_in:
        error, _ = run_recorded(interrupted, Path(scratch), stand_in.url)
    return {
        "the interrupt passes on": isinstance(error, KeyboardInterrupt),
        "a handled interrupt is its own outcome": ("drift", "check", "interrupted") in stand_in.groups,
    }


def recorded_without_an_address_pushes_nothing(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="push_case_") as scratch, StandIn() as stand_in:
        code, stream = run_recorded(lambda: 0, Path(scratch), None)
    return {"the job runs as before": code == 0, "nothing is sent": stand_in.requests == [], "no push line is written": "metrics_push" not in stream}


def recorded_survives_an_unreachable_gateway(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        url = stand_in.url
    with tempfile.TemporaryDirectory(prefix="push_case_") as scratch:
        started = time.monotonic()
        with mock.patch.object(metrics_push, "REQUEST_TIMEOUT", 0.5), mock.patch.object(metrics_push, "TOTAL_TIMEOUT", 1.0):
            code, stream = run_recorded(lambda: 7, Path(scratch), url, heartbeat=0.05)
        elapsed = time.monotonic() - started
    return {
        "the job's own exit code passes on": code == 7,
        "the push failures add less than the total timeout twice": elapsed < 4.0,
        "the failure is logged": "event=metrics_push_failed" in stream,
        "the resource_peaks line is still written once": stream.count("event=resource_peaks ") == 1,
        "repeated heartbeat failures are not logged every time": stream.count("event=metrics_push_failed") <= 4,
    }


def recorded_survives_a_slow_gateway(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="push_case_") as scratch, StandIn(delay=3.0) as stand_in:
        started = time.monotonic()
        with mock.patch.object(metrics_push, "REQUEST_TIMEOUT", 0.3), mock.patch.object(metrics_push, "TOTAL_TIMEOUT", 0.8):
            code, _ = run_recorded(lambda: 0, Path(scratch), stand_in.url)
        elapsed = time.monotonic() - started
    return {"the job exits 0 although the gateway never answers in time": code == 0, "the delay is bounded by the timeouts": elapsed < 4.0}


def drift_check_phase_pushes_end_to_end(root: Path) -> dict[str, bool]:
    from monitoring import aws_cases

    with tempfile.TemporaryDirectory(prefix="push_case_") as scratch, StandIn() as stand_in:
        folder = Path(scratch)
        work = aws_cases.prepared_workspace(folder, current_year=2021)
        environment = {**os.environ, "PUSHGATEWAY_URL": stand_in.url, "HAI_JOB": "drift"}
        done = run_command(PYTHON, ["-m", "monitoring.aws_run", "check", f"--run={aws_cases.RUN}", f"--work={work}"], cwd=root, env=environment, timeout=300)
        wrote = (work / "drift" / aws_cases.RUN / "records" / "drift_HAI_1.json").is_file()
    return {
        "the real command exits 0": done.returncode == 0,
        "the outcome group exists and the running group is gone": set(stand_in.groups) == {SUCCEEDED},
        "the common metrics are pushed": {"hai_job_duration_seconds", "hai_job_exit_code", "hai_job_peak_memory_bytes"} <= stand_in.names(SUCCEEDED),
        "the exit code metric is zero": stand_in.value(SUCCEEDED, "hai_job_exit_code") == 0.0,
        "the drift result is pushed per type": stand_in.value(SUCCEEDED, "hai_drift_status", '{hai_type="HAI_1",status="skipped"}') == 1.0,
        "the trigger flag is zero for a skipped check": stand_in.value(SUCCEEDED, "hai_drift_triggered", '{hai_type="HAI_1"}') == 0.0,
        "no run ID appears in any pushed body": all("HAI_1" in body and aws_cases.RUN not in body for body in stand_in.groups.values() if "hai_drift" in body),
        "the real command still writes its record": wrote,
    }


def pending() -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    """What the exit push of this process would send: {(metric, sorted labels): value}; draining empties the list."""
    return {(sample.name, tuple(sorted(sample.labels.items()))): sample.value for sample in metrics_push.drain()}


MATERIALIZE_LOAD = '{"entities": 12, "feature_set": "m1", "load_id": "x", "mode": "real", "online": "load", "step": "materialize"}\n'
MATERIALIZE_HOLD = '{"feature_set": "m2", "mode": "real", "online": "hold", "step": "materialize"}\n'
RUN = "20261117T120000Z-0a1b2c3d"
RELOAD_LOADED = json.dumps({"base_run": RUN, "feature_set": "m4", "load_id": "1", "outcome": "loaded", "run_id": RUN, "skipped_held": []}) + "\n"
RELEASE_DONE = json.dumps({"base_run": RUN, "feature_set": "m1", "outcome": "already_released", "run_id": RUN}) + "\n"


def feature_state_is_parsed_from_the_step_output(root: Path) -> dict[str, bool]:
    from scripts.features import feature_metrics

    metrics_push.drain()
    feature_metrics.materialized(f"noise before\n{MATERIALIZE_LOAD}")
    feature_metrics.materialized(MATERIALIZE_HOLD)
    feature_metrics.materialized('{"feature_set": "m3", "online": "none", "step": "materialize"}\n')
    feature_metrics.materialized('{"feature_set": "base", "online": "offline", "step": "apply"}\n')
    feature_metrics.reloaded(RELOAD_LOADED)
    feature_metrics.released(RELEASE_DONE)
    found = pending()
    return {
        "a loaded set exposes its online mode": found.get(("hai_feature_set_online_mode", (("feature_set", "m1"), ("mode", "load")))) == 1,
        "a held set exposes the hold mode": found.get(("hai_feature_set_online_mode", (("feature_set", "m2"), ("mode", "hold")))) == 1,
        "a publish time is exposed for each materialized set": ("hai_feature_set_publish_timestamp_seconds", (("feature_set", "m1"),)) in found,
        "a set with no online store has no mode series": not any(
            name == "hai_feature_set_online_mode" and dict(labels).get("feature_set") == "m3" for name, labels in found
        ),
        "an apply step is not a publish": not any(dict(labels).get("feature_set") == "base" for name, labels in found),
        "the reload outcome is a result label": found.get(("hai_feature_reload_result", (("feature_set", "m4"), ("result", "loaded")))) == 1,
        "the release outcome is a result label": found.get(("hai_feature_release_result", (("feature_set", "m1"), ("result", "already_released")))) == 1,
        "no run ID is a label anywhere": all("2026" not in value for _, labels in found for _, value in labels),
    }


def unreadable_step_output_adds_nothing_and_never_raises(root: Path) -> dict[str, bool]:
    from scripts.features import feature_metrics

    metrics_push.drain()
    for text in (
        "",
        "not json\n",
        "[1, 2]\n",
        '{"feature_set": 3, "online": "load", "step": "materialize"}\n',
        '{"outcome": "<script>", "feature_set": "m1"}\n',
    ):
        feature_metrics.materialized(text)
        feature_metrics.reloaded(text)
        feature_metrics.released(text)
    with StandIn() as stand_in:
        accepted, _ = quiet(lambda: finish_ok(pusher(stand_in.url), samples=metrics_push.drain()))
    return {
        "nothing valid is pending after garbage": True,
        "a bad outcome word never reaches the gateway": "script" not in "".join(stand_in.groups.values()),
        "the push is still accepted": accepted is True,
    }


def layer_run_reload_and_release_push_their_result(root: Path) -> dict[str, bool]:
    from scripts.lakehouse import layer_run

    job = layer_run.Job(layer="features", run="20261117T120000Z-0a1b2c3d", mode="real", code=Path("/nonexistent"), feature_set="")
    metrics_push.drain()
    with (
        mock.patch.dict(os.environ, {layer_run.MODEL_VARIABLE: "m4", layer_run.FEATURES_RUN_VARIABLE: "20261117T120000Z-0a1b2c3d"}),
        mock.patch.object(layer_run, "run_module", lambda *_a, **_k: RELOAD_LOADED),
    ):
        layer_run.reload(job)
    after_reload = pending()
    with (
        mock.patch.dict(os.environ, {layer_run.MODEL_VARIABLE: "m1", layer_run.FEATURES_RUN_VARIABLE: "20261117T120000Z-0a1b2c3d"}),
        mock.patch.object(layer_run, "run_module", lambda *_a, **_k: RELEASE_DONE),
    ):
        layer_run.release(job)
    after_release = pending()
    return {
        "reload hands its result to the exit push": ("hai_feature_reload_result", (("feature_set", "m4"), ("result", "loaded"))) in after_reload,
        "release hands its result to the exit push": ("hai_feature_release_result", (("feature_set", "m1"), ("result", "already_released"))) in after_release,
    }


def dbt_results_become_counts_by_status(root: Path) -> dict[str, bool]:
    from scripts.lakehouse import layer_metrics

    metrics_push.drain()
    document = {
        "results": [
            {"unique_id": "model.hai.dim_hospital", "status": "success"},
            {"unique_id": "model.hai.fct_hai_outcome", "status": "success"},
            {"unique_id": "model.hai.bridge_x", "status": "error"},
            {"unique_id": "model.hai.later", "status": "skipped"},
            {"unique_id": "test.hai.not_null_dim_hospital_ccn.abc", "status": "pass"},
            {"unique_id": "test.hai.unique_dim_hospital_ccn.def", "status": "pass"},
            {"unique_id": "test.hai.relationships_x.ghi", "status": "fail"},
            {"unique_id": "test.hai.accepted_values_y.jkl", "status": "warn"},
            {"unique_id": "seed.hai.some_seed", "status": "success"},
            {"unique_id": "model.hai.odd", "status": "<script>"},
        ]
    }
    layer_metrics.dbt_results(document)
    found = pending()
    return {
        "models are counted by status": found.get(("hai_dbt_models", (("status", "success"),))) == 2
        and found.get(("hai_dbt_models", (("status", "error"),))) == 1
        and found.get(("hai_dbt_models", (("status", "skipped"),))) == 1,
        "tests are counted by status": found.get(("hai_dbt_tests", (("status", "pass"),))) == 2
        and found.get(("hai_dbt_tests", (("status", "fail"),))) == 1
        and found.get(("hai_dbt_tests", (("status", "warn"),))) == 1,
        "a seed is neither a model nor a test": not any("seed" in str(labels) for _, labels in found),
        "a status outside the closed set is never a label": not any("script" in str(labels) for _, labels in found),
        "no table or model name is a label": all(
            name in {"hai_dbt_models", "hai_dbt_tests"} and [key for key, _ in labels] == ["status"] for name, labels in found
        ),
    }


def quality_results_become_counts_by_kind(root: Path) -> dict[str, bool]:
    from scripts.lakehouse import layer_metrics

    metrics_push.drain()
    layer_metrics.quality_result("silver", {"counts": {"pass": 40, "fail": 0, "warn": 2}})
    layer_metrics.quality_result("cross", {"counts": {"pass": 7, "fail": 1, "warn": 0}})
    layer_metrics.quality_result("unknown_kind", {"counts": {"pass": 1}})
    layer_metrics.quality_result("gold", {"counts": "not a mapping"})
    layer_metrics.quality_result("features", {})
    found = pending()
    return {
        "checks run are the sum of the counts": found.get(("hai_quality_checks_run", (("kind", "silver"),))) == 42,
        "failed and warned checks have their own series": found.get(("hai_quality_checks_failed", (("kind", "cross"),))) == 1
        and found.get(("hai_quality_checks_warned", (("kind", "silver"),))) == 2,
        "an unknown kind or an unreadable count adds nothing": not any(dict(labels).get("kind") in {"unknown_kind", "gold"} for _, labels in found),
        "an empty result adds nothing and raises nothing": not any(dict(labels).get("kind") == "features" for _, labels in found),
    }


def unreadable_dbt_and_quality_files_add_nothing(root: Path) -> dict[str, bool]:
    from scripts.lakehouse import layer_metrics

    metrics_push.drain()
    with tempfile.TemporaryDirectory(prefix="layer_metrics_") as scratch:
        folder = Path(scratch)
        (folder / "target").mkdir()
        (folder / "target/run_results.json").write_text("not json", encoding="utf-8")
        layer_metrics.dbt_file(folder / "target/run_results.json")
        layer_metrics.dbt_file(folder / "target/missing.json")
        layer_metrics.dbt_results(cast(Any, None))
        layer_metrics.quality_file("silver", folder / "nothing.json")
    return {"nothing is pending after garbage and missing files": pending() == {}}


def layer_run_hands_its_dbt_and_quality_results_to_the_exit_push(root: Path) -> dict[str, bool]:
    from scripts.lakehouse import layer_run

    with tempfile.TemporaryDirectory(prefix="layer_run_metrics_") as scratch:
        code = Path(scratch) / "code"
        job = layer_run.Job(layer="gold", run="20261117T120000Z-0a1b2c3d", mode="real", code=code)
        (job.folder / "target").mkdir(parents=True)
        (job.folder / "target/run_results.json").write_text(json.dumps({"results": [{"unique_id": "model.hai.a", "status": "success"}]}), encoding="utf-8")
        (job.folder / "quality_gold").mkdir()
        (job.folder / "quality_gold/result.json").write_text(json.dumps({"counts": {"pass": 5, "fail": 0, "warn": 0}}), encoding="utf-8")
        metrics_push.drain()
        layer_run.push_dbt_results(job)
        layer_run.push_quality_results(job)
        found = pending()
    return {
        "the dbt run of the build is counted": found.get(("hai_dbt_models", (("status", "success"),))) == 1,
        "the gold quality checks are counted": found.get(("hai_quality_checks_run", (("kind", "gold"),))) == 5,
    }


class FakeStatus:
    def __init__(self, records: int, failures: int) -> None:
        self.records, self.failures = [f"entity_{index}" for index in range(records)], [f"failure_{index}" for index in range(failures)]


class FakeWorkflow:
    def __init__(self, records: int = 5, failures: int = 0, sink_failures: int = 0, broken: bool = False) -> None:
        self.source, self.sink, self.broken = (
            SimpleNamespace(get_status=lambda: FakeStatus(records, failures)),
            SimpleNamespace(get_status=lambda: FakeStatus(0, sink_failures)),
            broken,
        )
        self.calls: list[str] = []

    def execute(self) -> None:
        self.calls.append("execute")

    def raise_from_status(self) -> None:
        self.calls.append("raise_from_status")
        if self.source.get_status().failures:
            raise RuntimeError("the workflow recorded a failure")

    def print_status(self) -> None:
        self.calls.append("print_status")

    def stop(self) -> None:
        self.calls.append("stop")


def openmetadata_ingest_counts_entities_and_failures_by_source(root: Path) -> dict[str, bool]:
    from scripts.infrastructure import openmetadata_metrics

    metrics_push.drain()
    openmetadata_metrics.record({"source": {"type": "glue"}}, FakeWorkflow(records=525, failures=1, sink_failures=2))
    openmetadata_metrics.record({"source": {"type": "dbt"}}, FakeWorkflow(records=7))
    openmetadata_metrics.record({"source": {"type": "surprise"}}, FakeWorkflow())
    openmetadata_metrics.record({"source": {}}, FakeWorkflow())
    openmetadata_metrics.record({"source": {"type": "glue"}}, SimpleNamespace())
    found = pending()
    return {
        "entities ingested per source": found.get(("hai_openmetadata_ingest_entities", (("source", "glue"),))) == 525
        and found.get(("hai_openmetadata_ingest_entities", (("source", "dbt"),))) == 7,
        "failures add the source and the sink": found.get(("hai_openmetadata_ingest_failures", (("source", "glue"),))) == 3,
        "an unknown source type or a workflow without a status adds nothing": not any(dict(labels).get("source") == "surprise" for _, labels in found)
        and len(found) == 4,
    }


def run_workflow_records_its_status_before_it_can_raise(root: Path) -> dict[str, bool]:
    from scripts.infrastructure import openmetadata_ingest

    metrics_push.drain()
    workflow = FakeWorkflow(records=4, failures=1)
    fake_module = SimpleNamespace(MetadataWorkflow=SimpleNamespace(create=lambda _config: workflow))
    with mock.patch.object(openmetadata_ingest.importlib, "import_module", lambda _name: fake_module):
        try:
            openmetadata_ingest.run_workflow({"source": {"type": "glue"}})
            raised = False
        except RuntimeError:
            raised = True
    found = pending()
    return {
        "the failure still raises as before": raised and workflow.calls[:2] == ["execute", "raise_from_status"],
        "the counts were recorded before the raise": found.get(("hai_openmetadata_ingest_failures", (("source", "glue"),))) == 1
        and found.get(("hai_openmetadata_ingest_entities", (("source", "glue"),))) == 4,
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "push_puts_the_group_with_valid_exposition": push_puts_the_group_with_valid_exposition,
    "push_unavailable_scratch_is_left_out": unavailable_scratch_is_left_out,
    "push_drift_samples_carry_closed_labels": drift_samples_carry_closed_labels,
    "push_refused_samples_are_dropped_and_named": refused_samples_are_dropped_and_named,
    "push_grouping_keys_must_be_closed": grouping_keys_must_be_closed,
    "push_refusal_is_reported_not_raised": refusal_is_reported_not_raised,
    "push_bad_exposition_refusal_is_reported": bad_exposition_refusal_is_reported,
    "push_timeout_is_bounded": timeout_is_bounded,
    "push_total_deadline_covers_both_requests": total_deadline_covers_both_requests,
    "push_a_slow_answer_that_never_pauses_still_ends_at_the_deadline": a_slow_answer_that_never_pauses_still_ends_at_the_deadline,
    "push_a_slow_answer_bounds_a_sample_push_a_heartbeat_and_a_delete": a_slow_answer_bounds_a_sample_push_a_heartbeat_and_a_delete,
    "push_unreachable_is_reported_not_raised": unreachable_is_reported_not_raised,
    "push_dropped_connection_is_reported_not_raised": dropped_connection_is_reported_not_raised,
    "push_never_raises_into_the_job": never_raises_into_the_job,
    "push_finish_removes_only_the_running_group": finish_removes_only_the_running_group,
    "push_running_group_exposes_the_heartbeat": running_group_exposes_the_heartbeat,
    "push_failure_group_stays_after_a_later_success": failure_group_stays_after_a_later_success,
    "push_refused_delete_does_not_fail_the_push": refused_delete_does_not_fail_the_push,
    "push_labels_are_url_safe": labels_are_url_safe,
    "push_phase_table_matches_the_code_and_terraform": phase_table_matches_the_code_and_terraform,
    "push_recorded_pushes_start_heartbeat_and_exit": recorded_pushes_start_heartbeat_and_exit,
    "push_recorded_failures_push_the_failed_outcome": recorded_failures_push_the_failed_outcome,
    "push_recorded_interrupt_pushes_interrupted": recorded_interrupt_pushes_interrupted,
    "push_recorded_without_an_address_pushes_nothing": recorded_without_an_address_pushes_nothing,
    "push_recorded_survives_an_unreachable_gateway": recorded_survives_an_unreachable_gateway,
    "push_recorded_survives_a_slow_gateway": recorded_survives_a_slow_gateway,
    "push_drift_check_phase_pushes_end_to_end": drift_check_phase_pushes_end_to_end,
    "push_feature_state_is_parsed_from_the_step_output": feature_state_is_parsed_from_the_step_output,
    "push_unreadable_step_output_adds_nothing_and_never_raises": unreadable_step_output_adds_nothing_and_never_raises,
    "push_layer_run_reload_and_release_push_their_result": layer_run_reload_and_release_push_their_result,
    "push_dbt_results_become_counts_by_status": dbt_results_become_counts_by_status,
    "push_quality_results_become_counts_by_kind": quality_results_become_counts_by_kind,
    "push_unreadable_dbt_and_quality_files_add_nothing": unreadable_dbt_and_quality_files_add_nothing,
    "push_layer_run_hands_its_dbt_and_quality_results_to_the_exit_push": layer_run_hands_its_dbt_and_quality_results_to_the_exit_push,
    "push_openmetadata_ingest_counts_entities_and_failures_by_source": openmetadata_ingest_counts_entities_and_failures_by_source,
    "push_run_workflow_records_its_status_before_it_can_raise": run_workflow_records_its_status_before_it_can_raise,
}
