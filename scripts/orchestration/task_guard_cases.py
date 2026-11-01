"""Cases of the guard container of a layer task, against a local stand-in of the ECS task metadata endpoint.

    .venv/bin/python -m scripts.orchestration.task_guard_cases

A phase that exits with an error does not stop a layer task: the phases after it wait for its success and never start, and the one essential container
(the last phase) never runs, so the task stays PENDING until somebody stops it. The guard is a second essential container that ends the task as soon as a
phase has failed. Each case runs ``scripts/orchestration/task_guard.py`` as the container does, with the endpoint address in its environment."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

GUARD = Path(__file__).resolve().with_name("task_guard.py")
PHASES = ["init", "fetch", "pin", "build", "validate", "verify", "export", "publish", "upload"]


def container(name: str, status: str, exit_code: int | None = None, kind: str = "NORMAL") -> dict[str, Any]:
    entry: dict[str, Any] = {"Name": name, "KnownStatus": status, "Type": kind}
    if exit_code is not None:
        entry["ExitCode"] = exit_code
    return entry


def task(states: dict[str, tuple[str, int | None]]) -> dict[str, Any]:
    """The task metadata as the endpoint returns it: every phase and the guard, in the state given, and the pause container the agent adds."""
    listed = [container(name, *states.get(name, ("PENDING", None))) for name in PHASES]
    return {"Containers": [*listed, container("guard", "RUNNING"), container("~internal~ecs~pause", "RESOURCES_PROVISIONED", kind="CNI_PAUSE")]}


class Endpoint:
    """A stand-in of the metadata endpoint. ``documents`` is what each request returns, in order, and the last one repeats."""

    def __init__(self, documents: list[Any]) -> None:
        self.documents, self.requests, self.delay = documents, 0, 0.0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def do_GET(self) -> None:
                time.sleep(outer.delay)
                found = outer.documents[min(outer.requests, len(outer.documents) - 1)]
                outer.requests += 1
                if found is None:
                    self.send_response(500)
                    self.end_headers()
                    return
                data = json.dumps(found).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> Endpoint:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()

    @property
    def address(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/v4/task-id"


def load_guard() -> Any:
    """The script as the container runs it, loaded as a module so that no process is started."""
    spec = importlib.util.spec_from_file_location("task_guard", GUARD)
    if spec is None or spec.loader is None:
        raise RuntimeError("the guard script cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_guard(documents: list[Any], limit: str = "1.5", changes: dict[str, str | None] | None = None) -> tuple[int, str]:
    """Run the guard against the stand-in until it exits. Returns its exit code and what it printed. A task it leaves alone ends at its time limit.

    ``changes`` replaces or, with None, removes a setting of the guard's environment."""
    guard = load_guard()
    with Endpoint(documents) as endpoint:
        environment: dict[str, str | None] = {"ECS_CONTAINER_METADATA_URI_V4": endpoint.address, "GUARD_INTERVAL_SECONDS": "0.1", "GUARD_SECONDS": limit}
        environment.update(changes or {})
        output = io.StringIO()
        with mock.patch.dict(os.environ, {name: value for name, value in environment.items() if value is not None}), contextlib.redirect_stdout(output):
            for name, value in environment.items():
                if value is None:
                    os.environ.pop(name, None)
            code = guard.main()
        return code, output.getvalue()


def left_alone(documents: list[Any], changes: dict[str, str | None] | None = None) -> bool:
    """The guard ended only at its time limit, so it never saw a failed phase."""
    code, output = run_guard(documents, changes=changes)
    return code == 1 and "time limit" in output and "phase" not in output.replace("watching the phases", "")


def stopped_by_ecs() -> tuple[int, str]:
    """ECS stops the task when the essential phase ends: SIGTERM reaches the guard while it watches a healthy task."""
    guard = load_guard()
    previous = signal.getsignal(signal.SIGTERM)
    output = io.StringIO()
    with Endpoint([task({"init": ("STOPPED", 0), "build": ("RUNNING", None)})]) as endpoint:
        environment = {"ECS_CONTAINER_METADATA_URI_V4": endpoint.address, "GUARD_INTERVAL_SECONDS": "0.1", "GUARD_SECONDS": "30"}
        timer = threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGTERM))
        try:
            with mock.patch.dict(os.environ, environment), contextlib.redirect_stdout(output):
                timer.start()
                try:
                    code = guard.main()
                except SystemExit as stop:
                    code = int(stop.code or 0)
        finally:
            timer.cancel()
            signal.signal(signal.SIGTERM, previous)
    return code, output.getvalue()


def cases() -> dict[str, bool]:
    done = ("STOPPED", 0)
    working: dict[str, tuple[str, int | None]] = {"init": done, "fetch": done, "pin": done, "build": ("RUNNING", None)}
    checks: dict[str, bool] = {}
    checks["a task whose phases run and succeed is left alone"] = left_alone([task(working)])
    code, output = run_guard([task({**working, "build": ("STOPPED", 1)})])
    checks["a phase that exited with an error ends the task with an error"] = code == 1 and "phase build failed" in output
    code, output = run_guard([task({**working, "build": ("STOPPED", 1), "validate": ("STOPPED", None)})])
    checks["the phase that never started because its dependency failed does not hide the failure"] = code == 1 and "failed" in output
    code, output = run_guard([task({**working, "build": done, "validate": ("STOPPED", None)})])
    checks["a phase that stopped without an exit code is a failure"] = code == 1 and "phase validate failed" in output
    checks["one reading of a failure is not enough"] = left_alone([task({**working, "build": ("STOPPED", 1)}), task(working)])
    checks["an endpoint that does not answer never ends a task"] = left_alone([None, None, task(working)])
    checks["a reply that is not a task never ends a task"] = left_alone([{"Containers": "no list"}, {}, task(working)])
    pending = task({"init": done, "fetch": done, "pin": done, "build": done, "validate": ("PENDING", None)})
    checks["phases that still wait for their dependency are not failures"] = left_alone([pending])
    checks["a task whose phases all succeeded is left to its essential container"] = left_alone([task({name: done for name in PHASES})])
    code, output = run_guard([task(working)], limit="0.5")
    checks["a task that passes its time limit ends with an error"] = code == 1 and "time limit" in output
    checks["the guard does not count itself"] = left_alone([task({**working, "guard": ("STOPPED", 1)})])
    code, output = stopped_by_ecs()
    checks["SIGTERM from ECS ends the guard with exit 0 at once"] = code == 0
    code, output = run_guard([None, None, None], limit="0.5")
    checks["an endpoint that errors is logged as unreadable and does not end the task"] = (
        code == 1 and "cannot read" in output and "time limit" in output and "failed" not in output
    )
    checks["an endpoint that never answered is not reported as readable"] = "can be read" not in output
    code, output = run_guard([task(working)], limit="0.5")
    checks["an endpoint that answers is logged once as readable"] = output.count("can be read") == 1
    checks["a task metadata address that is missing never ends a task"] = left_alone([task(working)], {"ECS_CONTAINER_METADATA_URI_V4": None})
    checks["an address that refuses the connection never ends a task"] = left_alone(
        [task(working)], {"ECS_CONTAINER_METADATA_URI_V4": "http://127.0.0.1:9/v4/task-id"}
    )
    checks["an address that is not a URL never ends a task"] = left_alone([task(working)], {"ECS_CONTAINER_METADATA_URI_V4": "not a url"})
    slow = Endpoint([task({**working, "build": ("STOPPED", 1)})])
    slow.delay = 1.0
    with slow:
        code, output = run_guard([task(working)], changes={"ECS_CONTAINER_METADATA_URI_V4": slow.address, "GUARD_READ_SECONDS": "0.1"}, limit="1.2")
    checks["a reading that times out is a failed reading and not a failed phase"] = code == 1 and "cannot read" in output and "phase build" not in output
    guard = load_guard()
    checks["a missing setting falls back to the default"] = guard.number("", 7.0) == 7.0
    checks["a setting that is not positive falls back to the default"] = guard.number("-3", 7.0) == 7.0 and guard.number("0", 7.0) == 7.0
    checks["a setting that is not a number falls back to the default"] = guard.number("abc", 7.0) == 7.0
    checks["a valid setting is used"] = guard.number("2.5", 7.0) == 2.5
    return checks


def main() -> int:
    results = cases()
    for name, ok in results.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    failed = [name for name, ok in results.items() if not ok]
    sys.stdout.write(f"task guard cases: {len(results) - len(failed)} of {len(results)} passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
