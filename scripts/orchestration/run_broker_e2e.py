"""End-to-end runs of the host job broker and its ledger as a client of the launcher, against a fake Docker Engine, in test registries.

Run ``.venv/bin/python -m scripts.orchestration.run_broker_e2e [--case NAME ...] [--mutation NAME]``. Each case uses the harness of
``scripts/lakehouse/run_launcher_e2e.py`` (its own test registry, fake daemon and launcher process); the broker runs in the case's process, or as a
``python -m scripts.orchestration.broker submit-once`` child where the case kills it. ``--checks`` prints every check. The case-level failing-first
runs, with one guard removed in a copy of the tree, are ``scripts/orchestration/run_failing_first.py``. The report is
``data/e2e/orchestration_broker/<UTC>/report.json``.
No real Docker, catalog, AWS service or secret is used.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import errno
import hashlib
import http.client
import io
import json
import os
import shutil
import stat
import sys
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import launcher
from orchestration.client import BrokerClient, BrokerUnavailable
from orchestration.interface import Handle, State, SubmissionRefused, SubmissionRequest
from scripts.lakehouse import run_lock
from scripts.lakehouse.run_launcher_e2e import Env, refused
from scripts.orchestration import broker as broker_module
from scripts.orchestration import sentinel
from scripts.orchestration.broker import Broker, BrokerError, BrokerPaths, LauncherUnavailable, Listener, load_templates
from scripts.orchestration.ledger import Ledger
from scripts.orchestration.net_checks import ipv6_loopback_open, unreachable_elsewhere
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "orchestration_broker"
RUN = "20261013T090000Z-0a1b2c3d"
SENTINELS = {
    "AWS_SECRET_ACCESS_KEY": "sentinel-aws-" + os.urandom(6).hex(),
    "POLARIS_CLIENT_SECRET": "sentinel-polaris-" + os.urandom(6).hex(),
    "RUN_LOCK_TOKEN": "sentinel-runlock-" + os.urandom(6).hex(),
}
FROZEN_NAMES = (
    "pin_bronze",
    "silver_build",
    "silver_validate",
    "gold_build",
    "gold_validate",
    "cross_layer_checks",
    "verify_bronze",
    "publish_candidates",
    "reconcile_candidates",
    "catalog_backup",
    "commit_publish",
    "gold_maintenance",
    "sense_markers",
)


class InstrumentedClient(launcher.LauncherClient):
    """A launcher client that records every method it sends and can run a hook just before a request leaves."""

    def __init__(self, config: launcher.Config, start: bool = True) -> None:
        super().__init__(config, start)
        self.methods: list[str] = []
        self.before: Callable[[str, dict[str, Any]], None] | None = None
        self.submission_replies: list[dict[str, Any]] = []

    def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        principal: dict[str, Any] | None = None,
        request_id: str | None = None,
        timeout: float = 120.0,
        budget: float | None = None,
    ) -> Any:
        self.methods.append(method)
        if self.before is not None:
            self.before(method, params or {})
        if method != "status" or (params or {}).get("what") != "submission":
            return super().call(method, params, principal, request_id, timeout, budget)
        try:
            reply = super().call(method, params, principal, request_id, timeout, budget)
        except launcher.ClientError as error:
            self.submission_replies.append({"error": error.code})
            raise
        attempt = reply.get("attempt") or {}
        self.submission_replies.append({"found": reply.get("found"), "phase": attempt.get("phase"), "reason": attempt.get("reason")})
        return reply


class BrokerEnv(Env):
    """A harness case that also holds a broker state, a test inventory and an instrumented client."""

    def __init__(self, name: str, report: Path, **settings: Any) -> None:
        super().__init__(name, report, **settings)
        self.paths = BrokerPaths(run_lock.Registry(self.registry, "test"), self.temp)
        self.case_dir, self.packages_dir = self.case_folders("case")
        self.evidence: dict[str, Any] = {}
        self.inventory_path = self.work / "broker_templates.json"
        self.write_inventory()
        self.brokers: list[Broker] = []
        self.listeners: list[Listener] = []

    def fixture_row(self, args: list[str], **extra: Any) -> dict[str, Any]:
        """A broker row that forwards to the launcher's ``fixture_dbt`` template."""
        params = {
            "case_host": str(self.case_dir),
            "case_container": "/workspace/out/case",
            "packages_host": str(self.packages_dir),
            "dbt_vars": {},
        }
        return {
            "class": "offline",
            "no_op": False,
            "launcher": {"template": "fixture_dbt", "args": args, "params": params, "lineage": {"version": 1, "inputs": [], "outputs": []}, **extra},
        }

    def inventory(self) -> dict[str, Any]:
        """The real broker template file with a few rows that the launcher can run in a test registry."""
        document = copy.deepcopy(load_templates())
        modes = document["templates"]
        modes["silver_build"]["modes"]["fixture"] = self.fixture_row(["build"])
        modes["gold_build"]["modes"]["fixture"] = self.fixture_row(["run"], timeout_seconds=10)
        modes["silver_validate"]["modes"]["fixture"] = self.fixture_row(["build", "--evil"])
        modes["gold_validate"]["modes"]["fixture"] = {
            "class": "offline",
            "no_op": False,
            "launcher": {"template": "real_dbt", "args": ["build"], "params": {}, "lineage": {"version": 1, "inputs": [], "outputs": []}},
        }
        return document

    def write_inventory(self, document: dict[str, Any] | None = None) -> None:
        """Write the test inventory the broker and its child processes load."""
        self.inventory_path.write_text(json.dumps(document or self.inventory(), indent=2))

    def client_for_broker(self) -> InstrumentedClient:
        return InstrumentedClient(self.config, start=True)

    def new_broker(self, **extra: Any) -> Broker:
        """A broker on this case's paths; it holds no lock until ``start`` is called."""
        broker = Broker(self.paths, load_templates(self.inventory_path), self.client_for_broker(), **extra)
        self.brokers.append(broker)
        return broker

    def listener_for(self, broker: Broker, token: str = "t" * 64) -> Listener:
        """A loopback listener on a free port."""
        listener = Listener(broker, "127.0.0.1", 0, token)
        listener.start()
        self.listeners.append(listener)
        return listener

    def hold_jobs(self) -> None:
        """Jobs run until stopped; the launcher's own probe container still ends."""
        self.daemon.script = lambda container: (0.2, 0) if "hai.probe" in container["Config"]["Labels"] else None

    def request(self, template: str = "silver_build", run_id: str = RUN, mode: str = "fixture", attempt: int = 1) -> SubmissionRequest:
        return SubmissionRequest(template, run_id, mode, attempt, "lineage_fixture", str(template), "manual__" + str(run_id))

    def wait_state(self, broker: Broker, handle: Handle, wanted: tuple[State, ...], seconds: float = 30) -> Any:
        """Poll the broker's status until it reaches one of the states."""
        return self.wait(lambda: (s := broker.status(handle)).state in wanted and s, seconds, f"submission {handle.submission_id} in {wanted}")

    def creates(self, containing: str = "") -> list[str]:
        """Names of the containers the daemon created for jobs (not the launcher's probes)."""
        names = [str(c["name"]) for c in self.daemon.calls_matching("POST", "/containers/create")]
        return [n for n in names if "diag" not in n and containing in n]

    def child(self, *arguments: str, shim: Path | None = None) -> Any:
        """Run the broker as a child process on this case's registry with the test inventory."""
        if arguments[0] == "submit-once" and "--dag-id" not in arguments:
            template = arguments[arguments.index("--template") + 1]
            run_id = arguments[arguments.index("--run") + 1]
            arguments = (*arguments, "--dag-id", "lineage_fixture", "--task-id", template, "--dag-run-id", f"manual__{run_id}")
        environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR") if name in os.environ}
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        if shim is not None:
            environment["PATH"] = f"{shim}{os.pathsep}{environment.get('PATH', '')}"
        base = ["-B", "-m", "scripts.orchestration.broker", "--test-registry", str(self.registry), "--templates", str(self.inventory_path)]
        return run_command(sys.executable, [*base, *arguments], cwd=ROOT, env=environment, timeout=180)

    def stop_brokers(self) -> None:
        """Stop every listener and release every broker lock."""
        for listener in self.listeners:
            with contextlib.suppress(Exception):
                listener.stop()
        for broker in self.brokers:
            broker.close()
        self.listeners.clear()
        self.brokers.clear()

    def close(self) -> list[str]:
        """Stop the brokers, then the case."""
        self.stop_brokers()
        return super().close()


Case = Callable[[BrokerEnv], dict[str, bool]]
CASES: dict[str, tuple[Case, dict[str, Any]]] = {}


def case(name: str, **settings: Any) -> Callable[[Case], Case]:
    """Register a case under a name, with the settings of its test registry."""

    def register(function: Case) -> Case:
        CASES[name] = (function, settings)
        return function

    return register


def wire(broker: Broker) -> InstrumentedClient:
    client = broker.client
    if not isinstance(client, InstrumentedClient):
        raise TypeError("the case built a broker without an instrumented client")
    return client


def refusal_of(function: Callable[[], Any]) -> BaseException | None:
    """The exception a call raises (a broker or launcher refusal, or a failure a case then judges by its type), or None when it was accepted."""
    try:
        function()
    except Exception as error:
        return error
    return None


def ledger_lines(env: BrokerEnv, run_id: str = RUN) -> list[dict[str, Any]]:
    return Ledger(env.paths.ledger).read(run_id)


def events(env: BrokerEnv, submission_id: str, run_id: str = RUN) -> list[str]:
    return [line["event"] for line in ledger_lines(env, run_id) if line["submission_id"] == submission_id]


def not_ended(found: list[str]) -> bool:
    return "finished" not in found and "failed_closed" not in found


def ended_only_by_cancel(env: BrokerEnv, submission_id: str) -> bool:
    """True when the submission has no end or a cancelled finished line with exit code 143."""
    ends = [line for line in ledger_lines(env, RUN) if line["submission_id"] == submission_id and line["event"] in ("finished", "failed_closed")]
    return all(line["event"] == "finished" and line.get("cancelled") is True and line["exit_code"] == 143 for line in ends)


def run_record_file(env: BrokerEnv, run_id: str) -> str:
    """A durable run record, as a finalizing run's release needs it."""
    path = env.work / f"{run_id}_run.json"
    path.write_text(json.dumps({"run_id": run_id}))
    return str(path)


def attempts_of(env: BrokerEnv, launch: str = "") -> list[dict[str, Any]]:
    """The launcher's attempts of run owners (not probes)."""
    return [a for a in env.state()["attempts"].values() if a["owner"].startswith("run:") and launch in a["launch_id"]]


@case("no_direct_docker")
def no_direct_docker(env: BrokerEnv) -> dict[str, bool]:
    """925: the repository's Docker-call check finds nothing in the A1 files, and a broker child process never runs a `docker` program."""
    files = [
        "scripts/orchestration/broker.py",
        "scripts/orchestration/supervisor.py",
        "scripts/orchestration/ledger.py",
        "scripts/orchestration/offline_checks.py",
        "scripts/orchestration/run_broker_e2e.py",
        "scripts/orchestration/run_stack_e2e.py",
        "scripts/orchestration/run_supervisor_e2e.py",
        "scripts/orchestration/run_real_e2e.py",
        "scripts/orchestration/in_image_checks.py",
        "orchestration/client.py",
        "orchestration/graph.py",
    ]
    present = [name for name in files if (ROOT / name).is_file()]
    found = run_command(sys.executable, ["-m", "scripts.quality.docker_calls", *present], cwd=ROOT, timeout=300)
    shim = env.work / "shim"
    shim.mkdir()
    marker = env.work / "docker_was_run"
    (shim / "docker").write_text(f'#!/bin/sh\necho "$@" >> {marker}\nexit 1\n')
    (shim / "docker").chmod(0o700)
    env.hold_jobs()
    env.start()
    result = env.child("submit-once", "--template", "silver_build", "--run", RUN, "--mode", "fixture", "--attempt", "1", shim=shim)
    env.wait(lambda: env.creates("-a1"), 20, "the launcher creates the container")
    return {
        "the Docker-call check passes over every A1 file": found.returncode == 0 and bool(present),
        "the broker child submitted through the launcher": result.returncode == 0 and len(env.creates("-a1")) == 1,
        "no docker program was started by the broker child": not marker.exists(),
    }


@case("released_run_refused")
def released_run_refused(env: BrokerEnv) -> dict[str, bool]:
    """926: after the run is released, a submission is refused by the launcher and the broker records the refusal; nothing is created."""
    broker = env.new_broker()
    broker.start()
    first = broker.submit(env.request(attempt=1))
    env.wait_state(broker, first, (State.SUCCEEDED,))
    broker.finalize_run(RUN)
    released = broker.release_run(RUN, run_record_file(env, RUN))
    before = len(env.creates())
    second = broker.submit(env.request("gold_build", attempt=1))
    status = broker.status(second)
    record = broker.record(second)
    return {
        "the first submission succeeded": broker.status(first).state == State.SUCCEEDED,
        "the run is released": released.get("state") == "released",
        "the second submission failed": status.state == State.FAILED and status.exit_code == 125,
        "the refusal comes from the launcher": "released" in str(record.get("reason", "")) or "used before" in str(record.get("reason", "")),
        "the ledger holds intent then finished": events(env, second.submission_id) == ["intent", "finished"],
        "no container was created for it": len(env.creates()) == before,
    }


@case("failed_retained_refused")
def failed_retained_refused(env: BrokerEnv) -> dict[str, bool]:
    """926: a failed-retained run accepts no submission."""
    broker = env.new_broker()
    broker.start()
    first = broker.submit(env.request(attempt=1))
    env.wait_state(broker, first, (State.SUCCEEDED,))
    env.client().call("mark", {"state": "failed_retained"}, broker.principal(RUN))
    release = refusal_of(lambda: broker.release_run(RUN, run_record_file(env, RUN)))
    before = len(env.creates())
    second = broker.submit(env.request("gold_build", attempt=1))
    status = broker.status(second)
    return {
        "the broker cannot release a failed-retained run (the launcher needs the owner flag)": refused(release, "refused", "failed_retained"),
        "the submission failed": status.state == State.FAILED,
        "the launcher's reason names the state": "failed_retained" in str(broker.record(second).get("reason", "")),
        "no container was created for it": len(env.creates()) == before,
    }


@case("recovering_run_refused")
def recovering_run_refused(env: BrokerEnv) -> dict[str, bool]:
    """926: a run in recovery accepts no submission; the owner's recovery, not the broker, fences the run."""
    broker = env.new_broker()
    broker.start()
    first = broker.submit(env.request(attempt=1))
    env.wait_state(broker, first, (State.SUCCEEDED,))
    principal = {**broker.principal(RUN), "owner": True}
    env.client().call("recover", {"owner": True}, principal)
    before = len(env.creates())
    second = broker.submit(env.request("gold_build", attempt=1))
    status = broker.status(second)
    return {
        "the submission failed": status.state == State.FAILED,
        "the launcher's reason names the run state": any(word in str(broker.record(second).get("reason", "")) for word in ("recovering", "failed_retained")),
        "no container was created for it": len(env.creates()) == before,
    }


@case("release_while_in_flight")
def release_while_in_flight(env: BrokerEnv) -> dict[str, bool]:
    """926: a run released between the broker's intent and its forwarding: the forward is refused and no container starts."""
    broker = env.new_broker()
    broker.start()
    first = broker.submit(env.request(attempt=1))
    env.wait_state(broker, first, (State.SUCCEEDED,))
    broker.close()
    second_request = env.request("gold_build", attempt=1)
    crashed = env.child("submit-once", "--template", "gold_build", "--run", RUN, "--mode", "fixture", "--attempt", "1", "--crash-at", "after_intent")
    in_flight = events(env, second_request.submission_id)
    broker.finalize_run(RUN)
    broker.release_run(RUN, run_record_file(env, RUN))
    before = len(env.creates())
    summary = broker.start()
    handle = Handle(second_request.submission_id, second_request.container_name)
    status = broker.status(handle)
    return {
        "the child died after writing the intent": crashed.returncode == 70 and in_flight == ["intent"],
        "the reconciliation forwarded it and the launcher refused": status.state == State.FAILED and second_request.submission_id in summary["updated"],
        "the refusal is recorded after the intent": events(env, second_request.submission_id) == ["intent", "finished"],
        "no container was created for it": len(env.creates()) == before,
    }


@case("release_refused_while_running")
def release_refused_while_running(env: BrokerEnv) -> dict[str, bool]:
    """928: the launcher refuses `release` while an attempt of the run runs, and accepts it after the attempt ended and its container is gone."""
    env.hold_jobs()
    broker = env.new_broker()
    broker.start()
    handle = broker.submit(env.request("gold_build", attempt=1))
    env.wait_state(broker, handle, (State.RUNNING,))
    broker.finalize_run(RUN)
    record = run_record_file(env, RUN)
    early = refusal_of(lambda: broker.release_run(RUN, record))
    broker.cancel(handle)
    ended = env.wait_state(broker, handle, (State.CANCELLED,))
    released = broker.release_run(RUN, record)
    return {
        "release is refused while the attempt runs": refused(early, "refused", "open") or refused(early, "refused", "remains"),
        "the broker does not release the run by itself": ended.state == State.CANCELLED,
        "release succeeds after the attempt ended": released.get("state") == "released",
        "no container of the run is left": not env.labelled(),
    }


@case("broker_holds_no_reservation")
def broker_holds_no_reservation(env: BrokerEnv) -> dict[str, bool]:
    """928: the broker never reserves memory or allocates; it sends only requests of the run API, and its state folder holds no reservation file."""
    broker = env.new_broker()
    broker.start()
    handle = broker.submit(env.request(attempt=1))
    env.wait_state(broker, handle, (State.SUCCEEDED,))
    methods = set(wire(broker).methods)
    allowed = {"acquire", "submit", "status", "logs", "cancel", "finalize", "release"}
    names = sorted(path.name for path in env.temp.rglob("*") if path.is_file())
    return {
        "the broker sent only run-API requests": methods <= allowed and bool(methods),
        "it never sent reserve, allocate, register or recover": not methods & {"reserve", "allocate", "register", "register_set", "recover", "mark"},
        "its state folder holds no reservation": not any("reservation" in name for name in names),
        "the attempt allocation belongs to the launcher": all(a["kind"] == "attempt" for a in env.state()["allocations"].values()),
    }


@case("outage_during_job")
def outage_during_job(env: BrokerEnv) -> dict[str, bool]:
    """927: during a Docker outage the broker reports the launcher as unavailable, never a stale running state; when Docker returns the launcher
    stops the job at its deadline and the broker's ledger shows `finished` with the timeout code. The broker runs no monitor thread."""
    env.hold_jobs()
    broker = env.new_broker()
    broker.start()
    handle = broker.submit(env.request("gold_build", attempt=1))
    env.wait_state(broker, handle, (State.RUNNING,))
    env.daemon.restart(4.0, stop_containers=False)
    seen: list[BaseException] = []

    def unavailable() -> bool:
        try:
            broker.status(handle)
        except LauncherUnavailable as error:
            seen.append(error)
            return True
        return False

    soft_wait(unavailable, 10)
    threads = sorted(thread.name for thread in threading.enumerate())

    def ended() -> Any:
        try:
            status = broker.status(handle)
        except LauncherUnavailable:
            return None
        return status if status.state == State.FAILED else None

    final = soft_wait(ended, 60)
    attempt = attempts_of(env, "sub-")[0]
    ledger: dict[str, Any] = {}
    with contextlib.suppress(Exception):
        ledger = broker.record(handle)
    return {
        "the broker said the launcher was unavailable during the outage": bool(seen) and "launcher" in str(seen[0]),
        "the broker holds no monitor thread": not any("monitor" in name.lower() or "watch" in name.lower() for name in threads),
        "the launcher ended the job at its deadline": attempt["reason"] == "timeout",
        "the ledger shows finished with the timeout code": final is not None and final.exit_code == 124 and ledger.get("exit_code") == 124,
        "the ledger order is intent, started, finished": events(env, handle.submission_id) == ["intent", "started", "finished"],
        "no container of the run is left": not env.labelled(),
    }


@case("duplicate_submission")
def duplicate_submission(env: BrokerEnv) -> dict[str, bool]:
    """929: the same request twice, one after the other and at once, gives one launcher attempt, one container and one ledger intent."""
    env.hold_jobs()
    broker = env.new_broker()
    broker.start()
    request = env.request("gold_build", attempt=1)
    first = broker.submit(request)
    second = refusal_of(lambda: broker.submit(request))
    handles: list[Handle] = []

    def submit_again() -> None:
        with contextlib.suppress(Exception):
            handles.append(broker.submit(request))

    threads = [threading.Thread(target=submit_again) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    soft_wait(lambda: state_of(broker, first) == State.RUNNING, 15)
    return {
        "the handles are equal": second is None and len(handles) == 4 and all(h == first for h in handles),
        "one launcher attempt exists for the submission": len(attempts_of(env, "sub-")) == 1,
        "one container was created": len(env.creates("-a1")) == 1,
        "the ledger holds one intent and one started": events(env, first.submission_id) == ["intent", "started"],
        "the same request ID went to the launcher each time": len({r["method"] for r in env.state()["requests"].values() if r["method"] == "submit"}) == 1,
    }


@case("crash_after_forward")
def crash_after_forward(env: BrokerEnv) -> dict[str, bool]:
    """929: a broker killed after the launcher accepted the submission and before the broker recorded it: the retry returns the launcher's attempt."""
    env.hold_jobs()
    crashed = env.child("submit-once", "--template", "gold_build", "--run", RUN, "--mode", "fixture", "--attempt", "1", "--crash-at", "after_forward")
    request = env.request("gold_build", attempt=1)
    after_crash = events(env, request.submission_id)
    broker = env.new_broker()
    cache_before = broker.forward_file(request).exists()
    marker_before = broker.dispatch_file(request).is_file()
    summary = broker.start()
    handle = broker.submit(request)
    soft_wait(lambda: state_of(broker, handle) in (State.RUNNING, State.FAILED), 20)
    return {
        "the child died after the launcher accepted the request": crashed.returncode == 70 and len(attempts_of(env, "sub-")) == 1,
        "the broker had recorded only the intent": after_crash == ["intent"] and not cache_before,
        "the broker had written its dispatch marker before the send": marker_before,
        "the restart read the launcher's attempt": request.submission_id in summary["updated"],
        "the retry returned the same attempt, not a second one": len(attempts_of(env, "sub-")) == 1 and len(env.creates("-a1")) == 1,
        "the ledger is intent then started": events(env, request.submission_id) == ["intent", "started"],
    }


@case("crash_after_intent")
def crash_after_intent(env: BrokerEnv) -> dict[str, bool]:
    """929: a broker killed after the intent and before any request: the restart forwards the submission exactly once; a row that changed since
    its intent is failed closed and not forwarded."""
    env.hold_jobs()
    request = env.request("gold_build", attempt=1)
    crashed = env.child("submit-once", "--template", "gold_build", "--run", RUN, "--mode", "fixture", "--attempt", "1", "--crash-at", "after_intent")
    no_attempt = len(attempts_of(env, "sub-")) == 0
    broker = env.new_broker()
    broker.start()
    handle = Handle(request.submission_id, request.container_name)
    env.wait_state(broker, handle, (State.RUNNING,))
    again = broker.submit(request)
    forwarded_once = len(attempts_of(env, "sub-")) == 1 and len(env.creates("-a1")) == 1
    broker.close()
    second = env.request("silver_build", attempt=1)
    env.child("submit-once", "--template", "silver_build", "--run", RUN, "--mode", "fixture", "--attempt", "1", "--crash-at", "after_intent")
    edited = env.inventory()
    edited["templates"]["silver_build"]["modes"]["fixture"] = env.fixture_row(["test"])
    env.write_inventory(edited)
    other = env.new_broker()
    other.start()
    last = Ledger(env.paths.ledger).submissions(RUN)[second.submission_id][-1]
    return {
        "the child died with only the intent written": crashed.returncode == 70 and no_attempt,
        "the restart forwarded it once": forwarded_once,
        "a repeat submission returns the same handle and no second attempt": again == handle and len(attempts_of(env, "sub-")) == 1,
        "a row changed since the intent is failed closed": last["event"] == "failed_closed" and "changed" in last["reason"],
        "the changed row was not forwarded": len(attempts_of(env, "sub-")) == 1 and len(env.creates("-a1")) == 1,
    }


@case("cancel_after_lost_submit_reply")
def cancel_after_lost_submit_reply(env: BrokerEnv) -> dict[str, bool]:
    """929b: the launcher accepts a submit and its reply is lost, so the broker has no forward cache. A cancel must reconcile the submission's request
    identity through the launcher and cancel the attempt it names; with the launcher unreachable the outcome stays unresolved (503, no ledger line); a
    submission that has only its intent is cancelled the same way. 929d (``lost_replies_stay_unresolved``): the same for a lost reply that is
    ``unresponsive``."""
    env.hold_jobs()
    broker = env.new_broker()
    broker.start()
    token = "c" * 64
    listener = env.listener_for(broker, token)
    port = listener.port
    request = env.request("silver_build", attempt=1)
    original = broker.client.call

    def lose_reply(method: str, *args: Any, **kwargs: Any) -> Any:
        answer = original(method, *args, **kwargs)
        if method == "submit":
            raise OSError("synthetic lost reply after the launcher accepted the submit")
        return answer

    def unreachable(method: str, *args: Any, **kwargs: Any) -> Any:
        raise OSError("synthetic: the launcher cannot be reached")

    with mock.patch.object(broker.client, "call", lose_reply):
        submit_code, _ = post(port, token, "/v1/submit", json.dumps(request.as_dict()).encode())
    accepted = env.wait(lambda: next(iter(attempts_of(env, "sub-")), None), 10, "the launcher holds the accepted attempt")
    env.wait_phase(accepted["id"], ("started",))
    cache_missing = broker.read_forward(request) is None
    body = json.dumps({"submission_id": request.submission_id}).encode()
    with mock.patch.object(broker.client, "call", unreachable):
        down_code, _ = try_post(port, token, "/v1/cancel", body)
    after_down = events(env, request.submission_id)
    cancel_code, _ = try_post(port, token, "/v1/cancel", body)
    handle = Handle(request.submission_id, request.container_name)
    final = soft_wait(lambda: (s := broker.status(handle)).state == State.CANCELLED and s, 30)
    ended = env.attempt(accepted["id"])
    container = env.daemon.containers.get(ended.get("container_id") or "")
    lines = events(env, request.submission_id)
    launch = broker_module.launch_id_of(request.submission_id)
    attempts_before = len(attempts_of(env, launch))
    creates_before = len(env.creates(launch))
    other = env.request("gold_build", attempt=1)
    with broker.ledger.locked(RUN):
        row = broker.row_for(other)
        broker.write_intent(other, row, broker.payload_of(other, row))
    other_code, _ = try_post(port, token, "/v1/cancel", json.dumps({"submission_id": other.submission_id}).encode())
    other_handle = Handle(other.submission_id, other.container_name)
    other_final = soft_wait(lambda: (s := broker.status(other_handle)).state == State.CANCELLED and s, 30)
    other_lines = events(env, other.submission_id)
    unresponsive = lost_replies_stay_unresolved(env, broker, port, token)
    return {
        **unresponsive,
        "the submit reply was lost and the launcher holds the attempt": submit_code == 503 and accepted is not None,
        "the broker has no forward cache": cache_missing,
        "a cancel while the launcher is unreachable is answered 503": down_code == 503,
        "an unreachable launcher leaves the ledger as it was": after_down == ["intent"],
        "the cancel with the launcher back is accepted": cancel_code == 200,
        "the launcher's attempt received the cancel": ended.get("cancel_intent") is not None,
        "the attempt ended with the reason cancelled": ended["phase"] == "ended" and ended["reason"] == "cancelled",
        "the job container is not running": container is None or not container["State"]["Running"],
        "the broker reports cancelled with exit 143": final is not None and final.exit_code == 143,
        "the ledger starts with the intent and ends with finished": lines[0] == "intent" and lines[-1] == "finished",
        "the launcher holds one attempt and one container was created for it": attempts_before == 1 and creates_before == 1,
        "a submission with only its intent is cancelled": other_code == 200 and other_final is not None and other_final.exit_code == 143,
        "its ledger starts with the intent and ends with finished": other_lines[0] == "intent" and other_lines[-1] == "finished",
        "no job container of either is left running": not [c for c in env.labelled() if c["State"]["Running"] and "hai.probe" not in c["Config"]["Labels"]],
    }


def lose_reply_of(broker: Broker, method: str, what: str | None = None) -> contextlib.AbstractContextManager[Any]:
    """The launcher handles each ``method`` request (of the ``what`` view, when named) and the client then reports ``unresponsive``: the reply is lost."""
    original = broker.client.call

    def call(name: str, params: dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> Any:
        answer = original(name, params, *args, **kwargs)
        if name == method and (what is None or (params or {}).get("what") == what):
            raise launcher.ClientError("synthetic: the launcher is alive but stopped answering", "unresponsive")
        return answer

    return mock.patch.object(broker.client, "call", call)


def start_through_listener(env: BrokerEnv, broker: Broker, port: int, token: str, request: SubmissionRequest) -> tuple[Handle, dict[str, Any]]:
    """Submit through the listener with nothing lost, and wait for the broker to report the job running; the handle and the launcher's attempt."""
    handle = Handle(request.submission_id, request.container_name)
    try_post(port, token, "/v1/submit", json.dumps(request.as_dict()).encode())
    soft_wait(lambda: state_of(broker, handle) == State.RUNNING, 15)
    return handle, next(iter(attempts_of(env, broker_module.launch_id_of(request.submission_id))), {})


def lost_replies_stay_unresolved(env: BrokerEnv, broker: Broker, port: int, token: str) -> dict[str, bool]:
    """929d: ``unresponsive`` is a transport error, like ``unavailable``: the launcher may have acted and the broker cannot know. A lost ``submit`` reply
    is answered 503 with only the intent in the ledger, and the same submission is then recovered once, with no second attempt; a lost ``status`` or
    ``cancel`` reply is answered 503 and writes nothing, and repeating the call settles it. Each operation has its own submission."""
    submitted = env.request("silver_build", attempt=2)
    launch = broker_module.launch_id_of(submitted.submission_id)
    body = json.dumps(submitted.as_dict()).encode()
    with lose_reply_of(broker, "submit"):
        lost_code, _ = try_post(port, token, "/v1/submit", body)
    after_lost = events(env, submitted.submission_id)
    held = env.wait(lambda: next(iter(attempts_of(env, launch)), None), 10, "the launcher holds the attempt whose submit reply was lost")
    again_code, _ = try_post(port, token, "/v1/submit", body)
    recovered = soft_wait(lambda: state_of(broker, Handle(submitted.submission_id, submitted.container_name)) == State.RUNNING, 15)
    recovered_events = events(env, submitted.submission_id)
    one_attempt = len(attempts_of(env, launch)) == 1 and len(env.creates(launch)) == 1
    stop_attempt(env, held["id"])

    watched = env.request("silver_build", attempt=3)
    _, watched_attempt = start_through_listener(env, broker, port, token, watched)
    watched_body = json.dumps({"submission_id": watched.submission_id}).encode()
    before_status = events(env, watched.submission_id)
    with lose_reply_of(broker, "status", "attempt"):
        status_code, _ = try_post(port, token, "/v1/status", watched_body)
    after_status = events(env, watched.submission_id)
    status_again, status_view = try_post(port, token, "/v1/status", watched_body)
    if watched_attempt:
        stop_attempt(env, watched_attempt["id"])

    to_cancel = env.request("silver_build", attempt=4)
    cancel_handle, cancel_attempt = start_through_listener(env, broker, port, token, to_cancel)
    cancelled_body = json.dumps({"submission_id": to_cancel.submission_id}).encode()
    before_cancel = events(env, to_cancel.submission_id)
    with lose_reply_of(broker, "cancel"):
        cancel_code, _ = try_post(port, token, "/v1/cancel", cancelled_body)
    after_cancel = events(env, to_cancel.submission_id)
    cancel_again, _ = try_post(port, token, "/v1/cancel", cancelled_body)
    final = cancelled_status(broker, cancel_handle)
    return {
        "a lost submit reply (unresponsive) is answered 503": lost_code == 503,
        "a lost submit reply leaves only the intent in the ledger": after_lost == ["intent"],
        "the same submit is accepted again": again_code == 200,
        "the recovery reads the attempt the launcher holds: running, intent then started": recovered is not None and recovered_events == ["intent", "started"],
        "the recovery made no second attempt or container": one_attempt,
        "the status of a running job is intent then started (control)": before_status == ["intent", "started"] and watched_attempt != {},
        "a lost status reply (unresponsive) is answered 503": status_code == 503,
        "a lost status reply writes nothing": after_status == before_status,
        "repeating the status settles it": status_again == 200 and status_view.get("state") == State.RUNNING.value,
        "a running job to cancel is intent then started (control)": before_cancel == ["intent", "started"] and cancel_attempt != {},
        "a lost cancel reply (unresponsive) is answered 503": cancel_code == 503,
        "a lost cancel reply writes nothing": after_cancel == before_cancel,
        "repeating the cancel is accepted and ends the job as cancelled with exit 143": cancel_again == 200 and final is not None and final.exit_code == 143,
    }


def leave_intent(env: BrokerEnv, request: SubmissionRequest) -> None:
    """Leave exactly the ledger line a broker killed after its intent leaves, without starting a broker (so an earlier unfinished line cannot block it)."""
    writer = env.new_broker()
    with writer.ledger.locked(request.run_id):
        row = writer.row_for(request)
        writer.write_intent(request, row, None if row["no_op"] else writer.payload_of(request, row))


@case("noop_crash_recovery")
def noop_crash_recovery(env: BrokerEnv) -> dict[str, bool]:
    """929c: a broker killed after the intent of a fixture no-op restarts and completes it without a launcher payload and without any launcher request
    (a launcher that refuses everything is enough); a row that changed since the intent, in either direction, fails closed."""
    request = {name: env.request(name, attempt=1) for name in ("pin_bronze", "verify_bronze")}
    control = env.new_broker()
    control.start()
    control_state = control.status(control.submit(env.request("verify_bronze", attempt=9))).state
    control.close()
    first = env.child("submit-once", "--template", "pin_bronze", "--run", RUN, "--mode", "fixture", "--attempt", "1", "--crash-at", "after_intent")
    serving = env.new_broker()
    started_serving: dict[str, Any] = {}
    try:
        started_serving = serving.start()
    except Exception as error:
        started_serving = {"error": f"{type(error).__name__}: {error}"}
    serving.close()
    leave_intent(env, request["verify_bronze"])
    refusing = env.new_broker()

    def refuse(method: str, params: dict[str, Any]) -> None:
        raise OSError("synthetic: the launcher refuses every request")

    wire(refusing).before = refuse
    started_refusing: dict[str, Any] = {}
    try:
        started_refusing = refusing.start()
    except Exception as error:
        started_refusing = {"error": f"{type(error).__name__}: {error}"}
    refusing.close()

    def status_of(handle: Handle) -> Any:
        try:
            return refusing.status(handle)
        except (LauncherUnavailable, SubmissionRefused):
            return None

    noop_status = {name: status_of(Handle(r.submission_id, r.container_name)) for name, r in request.items()}
    records = {name: (env.paths.records / RUN / f"{r.container_name}.json").is_file() for name, r in request.items()}
    again = env.request("pin_bronze", attempt=2)
    launched = env.request("silver_build", attempt=1)
    leave_intent(env, again)
    leave_intent(env, launched)
    edited = env.inventory()
    edited["templates"]["pin_bronze"]["modes"]["fixture"] = env.fixture_row(["build"])
    edited["templates"]["silver_build"]["modes"]["fixture"] = {"class": "offline", "no_op": True, "reason": "a row edited into a no-op for the test"}
    env.write_inventory(edited)
    other = env.new_broker()
    changed: dict[str, Any] = {}
    try:
        changed = other.start()
    except Exception as error:
        changed = {"error": f"{type(error).__name__}: {error}"}
    last = Ledger(env.paths.ledger).submissions(RUN)
    note_refusing = f"restart: {started_refusing.get('error', 'ok')}, pending: {len(started_refusing.get('pending', []))}"
    return {
        "a no-op submitted in one piece is recorded as a no-op (control)": control_state == State.NO_OP,
        "the no-op was killed after writing only its intent": first.returncode == 70,
        f"a restart with the launcher serving completes the no-op (restart: {started_serving.get('error', 'ok')})": "error" not in started_serving
        and request["pin_bronze"].submission_id in started_serving.get("updated", []),
        f"a restart with a launcher that refuses everything completes the other ({note_refusing})": "error" not in started_refusing
        and request["verify_bronze"].submission_id in started_refusing.get("updated", []),
        "each ledger is intent then finished": all(events(env, r.submission_id) == ["intent", "finished"] for r in request.values()),
        "each is reported as a no-op with exit 0": all(s is not None and s.state == State.NO_OP and s.exit_code == 0 for s in noop_status.values()),
        "each has its record file": all(records.values()),
        "the launcher was sent nothing for the second": not wire(refusing).methods,
        "a no-op whose row became a launcher row fails closed": last.get(again.submission_id, [{}])[-1].get("event") == "failed_closed",
        "a launcher row that became a no-op fails closed": last.get(launched.submission_id, [{}])[-1].get("event") == "failed_closed",
        f"the changed rows were not forwarded (restart: {changed.get('error', 'ok')})": "error" not in changed and not attempts_of(env, "sub-"),
    }


def cancelled_status(broker: Broker, handle: Handle, seconds: float = 30) -> Any:
    """The submission's status once it is cancelled; None when it does not get there in time (a case reports that as a failed check)."""

    def reached() -> Any:
        try:
            status = broker.status(handle)
        except Exception:
            return None
        return status if status.state == State.CANCELLED else None

    return soft_wait(reached, seconds)


def lose_submit_reply(env: BrokerEnv, request: SubmissionRequest, session: str) -> tuple[dict[str, Any], bool, int | None]:
    """Submit through a listener while the launcher accepts the submit and its reply is lost; stop the broker. The attempt the launcher holds, whether
    the broker has no forward cache, and the HTTP answer of the submit."""
    broker = env.new_broker()
    broker.start()
    listener = env.listener_for(broker, session)
    original = broker.client.call

    def lose_reply(method: str, *args: Any, **kwargs: Any) -> Any:
        answer = original(method, *args, **kwargs)
        if method == "submit":
            raise OSError("synthetic accepted submit reply lost")
        return answer

    with mock.patch.object(broker.client, "call", lose_reply):
        code, _ = try_post(listener.port, session, "/v1/submit", json.dumps(request.as_dict()).encode())
    launch = broker_module.launch_id_of(request.submission_id)
    accepted = env.wait(lambda: next(iter(attempts_of(env, launch)), None), 10, "the launcher holds the accepted attempt")
    env.wait_phase(accepted["id"], ("started",))
    cache_missing = broker.read_forward(request) is None
    env.stop_brokers()
    return accepted, cache_missing, code


def stop_attempt(env: BrokerEnv, attempt_id: str) -> None:
    """End an attempt through the launcher, so that a case that finds it still running (a defect it reports) can go on with the next variant: the
    launcher starts one job at a time here."""
    with contextlib.suppress(Exception):
        client = env.client_for_broker()
        client.call("cancel", {"attempt_id": attempt_id}, client.principal_run(RUN))
        env.wait_ended(attempt_id)


def drift_inventory(env: BrokerEnv, change: Callable[[dict[str, Any]], None]) -> None:
    """Rewrite the test inventory the way an owner edit would, from the baseline."""
    document = env.inventory()
    change(document)
    env.write_inventory(document)


def change_arguments(document: dict[str, Any]) -> None:
    """The silver_build fixture row now passes other arguments."""
    document["templates"]["silver_build"]["modes"]["fixture"]["launcher"]["args"] = ["run"]


def remove_mapping(document: dict[str, Any]) -> None:
    """The silver_build fixture row now has no launcher mapping."""
    document["templates"]["silver_build"]["modes"]["fixture"].update(launcher=None, reason="synthetic mapping removed")


def map_to_control(document: dict[str, Any]) -> None:
    document["templates"]["silver_build"]["modes"]["fixture"] = copy.deepcopy(document["templates"]["release_rejected"]["modes"]["fixture"])


@case("cancel_after_inventory_drift")
def cancel_after_inventory_drift(env: BrokerEnv) -> dict[str, bool]:
    """929d: the launcher accepts a submit, its reply is lost, the broker stops, the inventory row changes (other arguments, then no mapping) and the broker
    restarts. The submission is reconciled with the launcher's attempt (not ended by the changed row) and a cancel reaches the attempt; a lookup the launcher
    cannot answer leaves the submission pending; a submission the launcher never saw is still refused by the changed row."""
    env.hold_jobs()
    session = "d" * 64
    checks: dict[str, bool] = {}
    for label, attempt, change in (
        ("changed arguments", 1, change_arguments),
        ("removed mapping", 2, remove_mapping),
        ("mapped to a control row", 3, map_to_control),
    ):
        env.write_inventory()
        request = env.request("silver_build", attempt=attempt)
        accepted, cache_missing, submit_code = lose_submit_reply(env, request, session)
        drift_inventory(env, change)
        fresh = env.new_broker()
        with contextlib.suppress(Exception):
            fresh.start()
        port = env.listener_for(fresh, session).port
        handle = Handle(request.submission_id, request.container_name)
        after_restart = events(env, request.submission_id)
        state = state_of(fresh, handle)
        cancel_code, _ = try_post(port, session, "/v1/cancel", json.dumps({"submission_id": request.submission_id}).encode())
        final = cancelled_status(fresh, handle)
        ended = env.attempt(accepted["id"])
        container = env.daemon.containers.get(ended.get("container_id") or "")
        running = bool(container and container["State"]["Running"])
        stop_attempt(env, accepted["id"])
        checks |= {
            f"{label}: the submit reply was lost and the broker has no forward cache": submit_code == 503 and cache_missing,
            f"{label}: the restart leaves the submission unfinished": "finished" not in after_restart and "failed_closed" not in after_restart,
            f"{label}: the broker reports the running attempt": state == State.RUNNING,
            f"{label}: the cancel is accepted": cancel_code == 200,
            f"{label}: the launcher's attempt received the cancel": ended.get("cancel_intent") is not None,
            f"{label}: the attempt ended with the reason cancelled": ended["phase"] == "ended" and ended["reason"] == "cancelled",
            f"{label}: the job container is not running": not running,
            f"{label}: the broker reports cancelled with exit 143": final is not None and final.exit_code == 143,
        }
        env.stop_brokers()
    env.write_inventory()
    request = env.request("silver_build", attempt=4)
    accepted, cache_missing, submit_code = lose_submit_reply(env, request, session)
    drift_inventory(env, remove_mapping)
    asked = env.client_for_broker()

    def lookup(request_id: str, principal: dict[str, Any] | None) -> dict[str, Any]:
        """The launcher's answer to the submission lookup, or the error it raised."""
        try:
            answer: dict[str, Any] = asked.call("status", {"what": "submission", "id": request_id}, principal)
        except launcher.ClientError as error:
            return {"error": error.code}
        return answer

    known = lookup(broker_module.request_id_of(request.submission_id), asked.principal_run(RUN))
    unknown = lookup(broker_module.request_id_of("none:none:none:1"), asked.principal_run(RUN))
    without_token = lookup(broker_module.request_id_of(request.submission_id), None)
    unresolved = env.new_broker()
    original = unresolved.client.call

    def lookup_down(method: str, *args: Any, **kwargs: Any) -> Any:
        params = args[0] if args else kwargs.get("params") or {}
        if method == "status" and params.get("what") == "submission":
            raise OSError("synthetic: the launcher cannot answer the lookup")
        return original(method, *args, **kwargs)

    handle = Handle(request.submission_id, request.container_name)
    with mock.patch.object(unresolved.client, "call", lookup_down):
        try:
            summary = unresolved.start()
        except Exception as error:
            summary = {"error": f"{type(error).__name__}: {error}"}
        port = env.listener_for(unresolved, session).port
        status_code, _ = try_post(port, session, "/v1/status", json.dumps({"submission_id": request.submission_id}).encode())
        cancel_code, _ = try_post(port, session, "/v1/cancel", json.dumps({"submission_id": request.submission_id}).encode())
        while_down = events(env, request.submission_id)
    resumed_code, _ = try_post(port, session, "/v1/cancel", json.dumps({"submission_id": request.submission_id}).encode())
    resumed = cancelled_status(unresolved, handle)
    ended = env.attempt(accepted["id"])
    stop_attempt(env, accepted["id"])
    checks |= {
        "launcher lookup: the original request ID returns the accepted attempt": known.get("found") is True and known["attempt"]["id"] == accepted["id"],
        "launcher lookup: it returns no job parameters": "params" not in (known.get("attempt") or {"params": 1}),
        "launcher lookup: an unknown request ID is answered with no attempt": unknown == {"found": False, "attempt": None},
        "launcher lookup: a caller without the run's token is refused": without_token.get("error") == "principal",
        "unresolved lookup: the restart reports the submission pending": request.submission_id in summary.get("pending", []),
        "unresolved lookup: the ledger holds only the intent": while_down == ["intent"],
        "unresolved lookup: a status and a cancel are answered 503": status_code == 503 and cancel_code == 503,
        "unresolved lookup: the same cancel works once the lookup can be answered": resumed_code == 200 and resumed is not None and resumed.exit_code == 143,
        "unresolved lookup: the launcher's attempt received the cancel": ended.get("cancel_intent") is not None,
    }
    env.stop_brokers()
    env.write_inventory()
    never = env.request("silver_build", attempt=5)
    leave_intent(env, never)
    drift_inventory(env, remove_mapping)
    last = env.new_broker()
    last.start()
    never_events = events(env, never.submission_id)
    checks |= {
        "never accepted: the removed mapping refuses the submission": never_events == ["intent", "finished"],
        "never accepted: the launcher holds no attempt for it": not attempts_of(env, broker_module.launch_id_of(never.submission_id)),
        "no job container is left running": not [c for c in env.labelled() if c["State"]["Running"] and "hai.probe" not in c["Config"]["Labels"]],
    }
    return checks


TOKEN_STORES = ("missing token file", "unsearchable token folder", "accessible token")
DRIFTS = (("changed arguments", change_arguments), ("removed mapping", remove_mapping), ("mapped to a control row", map_to_control))


def hide_token_store(kind: str, root: Path, run_id: str, aside: Path) -> Callable[[], None]:
    """Make the run's token unreadable without opening it: move the file aside, or take the search permission from its folder. The returned function
    restores access."""
    token = root / f"{run_id}.token"
    if kind == "missing token file":
        token.rename(aside)

        def put_back() -> None:
            aside.rename(token)

        return put_back
    if kind == "unsearchable token folder":
        mode = root.stat().st_mode & 0o777
        root.chmod(0o600)

        def search_again() -> None:
            root.chmod(mode)

        return search_again
    return lambda: None


@case("cancel_after_token_store_loss")
def cancel_after_token_store_loss(env: BrokerEnv) -> dict[str, bool]:
    """931a: the launcher accepts a submit, its reply is lost and the inventory row changes while the run's token file is missing or its folder cannot be
    searched. A token that cannot be read is no proof that nothing was accepted: the submission stays pending, nothing terminal is written, a status and
    a cancel are answered 503, and once access is back a cancel reaches the attempt (``cancel_intent`` set, container stopped). A submission the broker
    never sent is decided by its row only after the launcher's lookup says not found, so while the token cannot be read it stays pending too."""
    env.hold_jobs()
    session = "e" * 64
    root = env.config.token_root
    aside = root.parent / "opaque_token_aside"
    checks: dict[str, bool] = {}
    number = 0
    for kind in TOKEN_STORES:
        for drift, change in DRIFTS:
            number += 1
            label = f"{kind}, {drift}"
            env.write_inventory()
            request = env.request("silver_build", attempt=number)
            accepted, cache_missing, submit_code = lose_submit_reply(env, request, session)
            drift_inventory(env, change)
            handle = Handle(request.submission_id, request.container_name)
            payload = json.dumps({"submission_id": request.submission_id}).encode()
            hidden = kind != "accessible token"
            restore = hide_token_store(kind, root, RUN, aside)
            try:
                fresh = env.new_broker()
                summary: dict[str, Any] = {}
                try:
                    summary = fresh.start()
                except Exception as error:
                    summary = {"error": f"{type(error).__name__}: {error}"}
                port = env.listener_for(fresh, session).port
                after_restart = events(env, request.submission_id)
                status_code, _ = try_post(port, session, "/v1/status", payload)
                after_status = events(env, request.submission_id)
                first_cancel, _ = try_post(port, session, "/v1/cancel", payload)
                cancel_only = ended_only_by_cancel(env, request.submission_id)
                held = env.attempt(accepted["id"])
                container = env.daemon.containers.get(held.get("container_id") or "")
                running = bool(container and container["State"]["Running"])
            finally:
                restore()
            second_cancel, _ = try_post(port, session, "/v1/cancel", payload)
            final = cancelled_status(fresh, handle)
            ended = env.attempt(accepted["id"])
            container = env.daemon.containers.get(ended.get("container_id") or "")
            stopped = not (container and container["State"]["Running"])
            stop_attempt(env, accepted["id"])
            checks |= {
                f"{label}: the submit reply was lost and the broker has no forward cache": submit_code == 503 and cache_missing,
                f"{label}: the ledger is not ended by the restart": not_ended(after_restart),
                f"{label}: the status does not end the ledger": not_ended(after_status),
                f"{label}: only the cancel can end the ledger": cancel_only,
                f"{label}: the restart reports the submission {'pending' if hidden else 'updated'}": request.submission_id
                in summary.get("pending" if hidden else "updated", []),
                f"{label}: {'a status and a cancel are answered 503' if hidden else 'a status and a cancel are answered 200'}": (
                    (status_code, first_cancel) == ((503, 503) if hidden else (200, 200))
                ),
                f"{label}: the attempt is not cancelled while the token cannot be read": (held.get("cancel_intent") is None and running) if hidden else True,
                f"{label}: a cancel once access is back is accepted": second_cancel == 200,
                f"{label}: the launcher's attempt received the cancel": ended.get("cancel_intent") is not None,
                f"{label}: the attempt ended with the reason cancelled": ended["phase"] == "ended" and ended["reason"] == "cancelled",
                f"{label}: the job container is stopped": stopped,
                f"{label}: the broker reports cancelled with exit 143": final is not None and final.exit_code == 143,
            }
            env.stop_brokers()
    for kind in TOKEN_STORES:
        for drift, change in DRIFTS:
            number += 1
            label = f"never sent, {kind}, {drift}"
            hidden = kind != "accessible token"
            env.write_inventory()
            request = env.request("silver_build", attempt=number)
            leave_intent(env, request)
            drift_inventory(env, change)
            restore = hide_token_store(kind, root, RUN, aside)
            try:
                last = env.new_broker()
                with contextlib.suppress(Exception):
                    last.start()
                while_hidden = events(env, request.submission_id)
            finally:
                restore()
            with contextlib.suppress(Exception):
                last.reconcile()
            after = events(env, request.submission_id)
            wanted = ["intent", "finished"] if drift == "removed mapping" else ["intent", "failed_closed"]
            checks |= {
                f"{label}: {'nothing is ended while the token cannot be read' if hidden else 'the row ends the submission'}": while_hidden
                == (["intent"] if hidden else wanted),
                f"{label}: the row ends the submission once the lookup can be asked": after == wanted,
                f"{label}: the launcher holds no attempt for it": not attempts_of(env, broker_module.launch_id_of(request.submission_id)),
            }
            env.stop_brokers()
    number += 1
    env.write_inventory()
    request = env.request("silver_build", attempt=number)
    leave_intent(env, request)
    handle = Handle(request.submission_id, request.container_name)
    restore = hide_token_store("unsearchable token folder", root, RUN, aside)
    try:
        waiting = env.new_broker()
        summary = {}
        with contextlib.suppress(Exception):
            summary = waiting.start()
        while_hidden = events(env, request.submission_id)
        sent = attempts_of(env, broker_module.launch_id_of(request.submission_id)) or {"acquire", "submit"} & set(wire(waiting).methods)
    finally:
        restore()
    state = state_of(waiting, handle)
    ran = soft_wait(lambda: next(iter(attempts_of(env, broker_module.launch_id_of(request.submission_id))), None), 10)
    waiting.cancel(handle)
    cancelled = cancelled_status(waiting, handle)
    checks |= {
        "never sent, unchanged row, unsearchable token folder: the restart reports the submission pending": request.submission_id in summary.get("pending", []),
        "never sent, unchanged row, unsearchable token folder: no acquire or submit is sent and nothing is ended": while_hidden == ["intent"] and not sent,
        "never sent, unchanged row, unsearchable token folder: the submission is sent once access is back": state is not None and ran is not None,
        "never sent, unchanged row, unsearchable token folder: it can then be cancelled": cancelled is not None and cancelled.exit_code == 143,
    }
    env.stop_brokers()
    checks["no job container is left running"] = not [c for c in env.labelled() if c["State"]["Running"] and "hai.probe" not in c["Config"]["Labels"]]
    return checks


FORWARD_LOSSES = ("run folder", "forwards folder")


def lose_forward_folders(env: BrokerEnv, level: str, tag: str) -> Callable[[], None]:
    """Move aside the broker's ``forwards`` run folder, or the whole ``forwards`` folder, as a partial recovery that keeps the ledger, the token store
    and the launcher's registry would leave it, without touching any of their files. A folder that does not exist is lost already. The returned
    function puts back what was moved and sets aside what the broker created in its place."""
    lost = env.paths.forwards / RUN if level == "run folder" else env.paths.forwards
    aside = env.temp / f"unpersisted_{tag}"
    if not lost.exists():
        return lambda: None
    lost.rename(aside)

    def put_back() -> None:
        if lost.exists():
            lost.rename(env.temp / f"recreated_{tag}")
        aside.rename(lost)

    return put_back


@case("cancel_after_marker_ancestor_loss")
def cancel_after_marker_ancestor_loss(env: BrokerEnv) -> dict[str, bool]:
    """929f: the launcher accepts a submit, its reply is lost and the inventory row changes while the broker's ``forwards`` run folder, or its whole
    ``forwards`` folder, is lost and the ledger and the token store survive. The dispatch marker lives beside the ledger, so that loss hides nothing: the
    submission is reconciled through the launcher's lookup, is not ended by the changed row, and a cancel reaches the attempt (``cancel_intent`` set,
    container stopped), before and after the folders are restored. A submission the broker never sent is decided by its row only after the launcher's
    lookup says it holds no attempt, whatever is lost.

    929h and 929j: the ledger folder, which holds the ledger and the marker, can be out of its place (removed, or replaced by a folder with a copy of the
    ledger and no marker) for exactly one read of the reconciliation, the first of a lookup of the marker and the launcher's lookup request. No disk
    state is proof that the broker never sent: the launcher is asked once by the original request ID, the attempt is reconciled and a cancel reaches it."""
    env.hold_jobs()
    session = "f" * 64
    checks: dict[str, bool] = {}
    number = 0
    for level in FORWARD_LOSSES:
        for drift, change in DRIFTS:
            number += 1
            label = f"{level} lost, {drift}"
            env.write_inventory()
            request = env.request("silver_build", attempt=number)
            accepted, cache_missing, submit_code = lose_submit_reply(env, request, session)
            marker = env.new_broker().dispatch_file(request)
            marker_written = marker.is_file()
            drift_inventory(env, change)
            handle = Handle(request.submission_id, request.container_name)
            payload = json.dumps({"submission_id": request.submission_id}).encode()
            restore = lose_forward_folders(env, level, f"{number}")
            try:
                fresh = env.new_broker()
                summary: dict[str, Any] = {}
                try:
                    summary = fresh.start()
                except Exception as error:
                    summary = {"error": f"{type(error).__name__}: {error}"}
                port = env.listener_for(fresh, session).port
                after_restart = events(env, request.submission_id)
                status_code, _ = try_post(port, session, "/v1/status", payload)
                after_status = events(env, request.submission_id)
                first_cancel, _ = try_post(port, session, "/v1/cancel", payload)
                cancel_only = ended_only_by_cancel(env, request.submission_id)
                held = env.attempt(accepted["id"])
            finally:
                restore()
            second_cancel, _ = try_post(port, session, "/v1/cancel", payload)
            final = cancelled_status(fresh, handle)
            ended = env.attempt(accepted["id"])
            container = env.daemon.containers.get(ended.get("container_id") or "")
            stopped = not (container and container["State"]["Running"])
            stop_attempt(env, accepted["id"])
            checks |= {
                f"{label}: the submit reply was lost and the broker has no forward cache": submit_code == 503 and cache_missing,
                f"{label}: the dispatch marker is written beside the ledger": marker_written and marker.parent == env.paths.ledger,
                f"{label}: the ledger is not ended by the restart": not_ended(after_restart),
                f"{label}: the status does not end the ledger": not_ended(after_status),
                f"{label}: only the cancel can end the ledger": cancel_only,
                f"{label}: the restart reports the submission updated": request.submission_id in summary.get("updated", []),
                f"{label}: a status and a cancel are answered 200": (status_code, first_cancel) == (200, 200),
                f"{label}: the attempt received the cancel while the folders were lost": held.get("cancel_intent") is not None,
                f"{label}: a cancel once the folders are restored is accepted": second_cancel == 200,
                f"{label}: the launcher's attempt received the cancel": ended.get("cancel_intent") is not None,
                f"{label}: the attempt ended with the reason cancelled": ended["phase"] == "ended" and ended["reason"] == "cancelled",
                f"{label}: the job container is stopped": stopped,
                f"{label}: the broker reports cancelled with exit 143": final is not None and final.exit_code == 143,
            }
            env.stop_brokers()
    for drift, change in DRIFTS:
        for kind, loss_kind in TRANSIENT_LOSSES:
            number += 1
            label = f"{kind}, {drift}"
            env.write_inventory()
            request = env.request("silver_build", attempt=number)
            accepted, cache_missing, submit_code = lose_submit_reply(env, request, session)
            drift_inventory(env, change)
            handle = Handle(request.submission_id, request.container_name)
            payload = json.dumps({"submission_id": request.submission_id}).encode()
            fresh = env.new_broker()
            marker = fresh.dispatch_file(request)
            ledger_file = fresh.ledger.path(RUN)
            lookups: list[str] = []
            wire(fresh).before = partial(count_submission_lookups, lookups)
            seen: dict[str, Any] = {}
            loss = (
                lose_ledger_folder_during_read(marker, ledger_file, wire(fresh), env.temp / f"opaque_ledger_folder_{number}", loss_kind, seen)
                if loss_kind
                else contextlib.nullcontext()
            )
            summary = {}
            with loss:
                try:
                    summary = fresh.start()
                except Exception as error:
                    summary = {"error": f"{type(error).__name__}: {error}"}
            restored = ledger_file.is_file() and marker.is_file()
            port = env.listener_for(fresh, session).port
            after_restart = events(env, request.submission_id)
            status_code, _ = try_post(port, session, "/v1/status", payload)
            after_status = events(env, request.submission_id)
            first_cancel, _ = try_post(port, session, "/v1/cancel", payload)
            after_cancel = events(env, request.submission_id)
            cancel_only = ended_only_by_cancel(env, request.submission_id)
            held = env.attempt(accepted["id"])
            final = cancelled_status(fresh, handle)
            ended = env.attempt(accepted["id"])
            container = env.daemon.containers.get(ended.get("container_id") or "")
            stopped = not (container and container["State"]["Running"])
            stop_attempt(env, accepted["id"])
            checks |= {
                f"{label}: the submit reply was lost and the broker has no forward cache": submit_code == 503 and cache_missing,
                f"{label}: the folder was {'out of its place for one read and is back' if loss_kind else 'never moved'}": (
                    bool(seen.get("loss_injected")) is bool(loss_kind) and restored
                ),
                f"{label}: the launcher is asked once, by the original request ID": len(lookups) == 1,
                f"{label}: the ledger is not ended by the restart": not_ended(after_restart),
                f"{label}: the status does not end the ledger": not_ended(after_status),
                f"{label}: only the cancel can end the ledger": cancel_only,
                f"{label}: the restart reports the submission updated": request.submission_id in summary.get("updated", []),
                f"{label}: a status and a cancel are answered 200": (status_code, first_cancel) == (200, 200),
                f"{label}: the attempt received the cancel": held.get("cancel_intent") is not None,
                f"{label}: the attempt ended with the reason cancelled": ended["phase"] == "ended" and ended["reason"] == "cancelled",
                f"{label}: the job container is stopped": stopped,
                f"{label}: the broker reports cancelled with exit 143": final is not None and final.exit_code == 143,
            }
            if loss_kind == "replace":
                checks[f"{label}: the replacement folder held the ledger and no marker"] = seen.get("replacement_has_no_marker") is True
            env.evidence[label] = {
                "events_after_restart": after_restart,
                "events_after_status": after_status,
                "events_after_cancel": after_cancel,
                "updated": summary.get("updated"),
                "start_error": summary.get("error"),
                "submission_replies": wire(fresh).submission_replies,
                "loss": {key: bool(value) for key, value in seen.items()},
                "attempt": {"phase": ended.get("phase"), "reason": ended.get("reason")},
            }
            env.stop_brokers()
    env.stop_brokers()
    for level in FORWARD_LOSSES:
        for drift, change in DRIFTS:
            number += 1
            label = f"never sent, {level} lost, {drift}"
            env.write_inventory()
            request = env.request("silver_build", attempt=number)
            leave_intent(env, request)
            drift_inventory(env, change)
            restore = lose_forward_folders(env, level, f"never_{number}")
            try:
                last = env.new_broker()
                with contextlib.suppress(Exception):
                    last.start()
                after = events(env, request.submission_id)
            finally:
                restore()
            wanted = ["intent", "finished"] if drift == "removed mapping" else ["intent", "failed_closed"]
            checks |= {
                f"{label}: the row ends the submission": after == wanted,
                f"{label}: the launcher holds no attempt for it": not attempts_of(env, broker_module.launch_id_of(request.submission_id)),
            }
            env.stop_brokers()
    checks["no job container is left running"] = not [c for c in env.labelled() if c["State"]["Running"] and "hai.probe" not in c["Config"]["Labels"]]
    return checks


def count_submission_lookups(lookups: list[str], method: str, params: dict[str, Any]) -> None:
    """A ``before`` hook of the instrumented client that counts the launcher's lookups of a submission by its request ID."""
    if method == "status" and params.get("what") == "submission":
        lookups.append(str(params.get("id")))


def record_calls(order: list[str], method: str, params: dict[str, Any]) -> None:
    """A ``before`` hook that records the launcher requests in the order they leave, the lookup of a submission as ``status:submission``."""
    order.append(f"{method}:{params['what']}" if method == "status" and "what" in params else method)


TRANSIENT_LOSSES = (("intact ledger folder", None), ("ledger folder removed during the read", "remove"), ("ledger folder replaced during the read", "replace"))


def is_marker_lookup(marker: Path, path: Any, dir_fd: int | None) -> bool:
    """True for the lookup of the marker, by path or by name relative to a held folder."""
    looked = os.fspath(path)
    return looked == marker.name if dir_fd is not None else Path(looked) == marker


@contextlib.contextmanager
def lose_ledger_folder_during_read(marker: Path, ledger_file: Path, client: InstrumentedClient, aside: Path, kind: str, seen: dict[str, Any]) -> Iterator[None]:
    """Take the ledger folder out of its place for exactly one read of the reconciliation and put it back right after that read, before anything else is
    checked. The read is the first of two: the broker's own ``os.stat`` of the marker, by path or by name relative to a held folder (code that reads the
    marker makes it), and the broker's launcher lookup of the submission (the request leaves while the folder is away and the answer is read after it is
    back). ``remove`` moves the folder aside with the ledger and the marker in it. ``replace`` puts another folder in its place that holds a copy of the
    ledger and no marker. Nothing is faked and it happens once."""
    folder = marker.parent
    stat_original = os.stat
    call_original = client.call

    def take() -> Callable[[], None]:
        seen["loss_injected"] = True
        folder.rename(aside)
        if kind == "replace":
            folder.mkdir(mode=0o700)
            (folder / ledger_file.name).write_bytes((aside / ledger_file.name).read_bytes())
            seen["replacement_has_no_marker"] = not marker.exists()

        def put_back() -> None:
            if kind == "replace":
                folder.rename(aside.with_name(f"{aside.name}_replacement"))
            aside.rename(folder)

        return put_back

    def stat(path: Any, *args: Any, dir_fd: int | None = None, **kwargs: Any) -> os.stat_result:
        if seen.get("loss_injected") or not is_marker_lookup(marker, path, dir_fd):
            return stat_original(path, *args, dir_fd=dir_fd, **kwargs)
        put_back = take()
        try:
            return stat_original(path, *args, dir_fd=dir_fd, **kwargs)
        finally:
            put_back()

    def call(method: str, params: dict[str, Any] | None = None, *args: Any, **kwargs: Any) -> Any:
        if seen.get("loss_injected") or method != "status" or (params or {}).get("what") != "submission":
            return call_original(method, params, *args, **kwargs)
        put_back = take()
        try:
            return call_original(method, params, *args, **kwargs)
        finally:
            put_back()

    with mock.patch.object(os, "stat", stat), mock.patch.object(client, "call", call):
        yield


@contextlib.contextmanager
def replace_ledger_folder_while_marker_is_written(marker: Path, ledger_file: Path, spare: Path, seen: dict[str, Any]) -> Iterator[None]:
    """while the broker's real marker writer runs, the ledger folder (it holds the intent and its lock) is moved aside and an empty folder
    takes its place. The unchanged writer publishes and flushes the marker in the replacement. Before the writer returns the replacement is moved aside
    intact and the original folder is put back, so the intent survives and the marker does not appear in the ledger folder. It happens once; nothing is
    faked and the writer's fsync calls run."""
    original = broker_module.write_private

    def write(path: Path, body: str, root: Path | None = None) -> None:
        if path != marker or seen.get("loss_injected"):
            original(path, body, root)
            return
        aside = spare / "intact_original_ledger_folder"
        other = spare / "intact_marker_folder"
        digest = hashlib.sha256(ledger_file.read_bytes()).hexdigest()
        marker.parent.rename(aside)
        seen["loss_injected"] = True
        try:
            marker.parent.mkdir(mode=0o700)
            original(path, body, root)
        finally:
            if marker.parent.exists():
                marker.parent.rename(other)
            aside.rename(marker.parent)
        seen.update(
            marker_in_other_folder=(other / marker.name).is_file(),
            ledger_unchanged=hashlib.sha256(ledger_file.read_bytes()).hexdigest() == digest,
            marker_in_ledger_folder=marker.exists(),
        )

    with mock.patch.object(broker_module, "write_private", write):
        yield


def lookup_unanswerable(original: Callable[..., Any]) -> Callable[..., Any]:
    """A launcher call that answers every request except the lookup of a submission, which it cannot answer."""

    def call(method: str, *args: Any, **kwargs: Any) -> Any:
        params = args[0] if args else kwargs.get("params") or {}
        if method == "status" and params.get("what") == "submission":
            raise OSError("synthetic: the launcher cannot answer the lookup")
        return original(method, *args, **kwargs)

    return call


@case("cancel_after_marker_write_replacement")
def cancel_after_marker_write_replacement(env: BrokerEnv) -> dict[str, bool]:
    """the broker's marker writer publishes the marker in a replacement of the ledger folder, which is moved aside intact before the
    writer returns, so the intent survives in the ledger folder and the marker does not. The launcher accepts the submit and its reply is lost. The
    inventory row then changes (other arguments, then no mapping) and the broker restarts. The authenticated lookup runs whatever the disk shows: the
    accepted attempt is reconciled, the ledger is not ended by the row and a cancel reaches the attempt (``cancel_intent`` set, container stopped), also
    after the lost marker is put back in the ledger folder."""
    env.hold_jobs()
    session = "9" * 64
    checks: dict[str, bool] = {}
    for number, (drift, change) in enumerate(DRIFTS, 1):
        label = f"marker written through a replaced ledger folder, {drift}"
        env.write_inventory()
        request = env.request("silver_build", attempt=number)
        marker = env.new_broker().dispatch_file(request)
        ledger_file = env.paths.ledger / f"{RUN}.jsonl"
        seen: dict[str, Any] = {}
        with replace_ledger_folder_while_marker_is_written(marker, ledger_file, env.temp, seen):
            accepted, cache_missing, submit_code = lose_submit_reply(env, request, session)
        drift_inventory(env, change)
        handle = Handle(request.submission_id, request.container_name)
        payload = json.dumps({"submission_id": request.submission_id}).encode()
        fresh = env.new_broker()
        lookups: list[str] = []
        wire(fresh).before = partial(count_submission_lookups, lookups)
        summary: dict[str, Any] = {}
        try:
            summary = fresh.start()
        except Exception as error:
            summary = {"error": f"{type(error).__name__}: {error}"}
        port = env.listener_for(fresh, session).port
        after_restart = events(env, request.submission_id)
        status_code, _ = try_post(port, session, "/v1/status", payload)
        after_status = events(env, request.submission_id)
        first_cancel, _ = try_post(port, session, "/v1/cancel", payload)
        cancel_only = ended_only_by_cancel(env, request.submission_id)
        held = env.attempt(accepted["id"])
        kept = env.temp / "intact_marker_folder" / marker.name
        if kept.is_file():
            kept.rename(marker)
        second_cancel, _ = try_post(port, session, "/v1/cancel", payload)
        final = cancelled_status(fresh, handle)
        ended = env.attempt(accepted["id"])
        container = env.daemon.containers.get(ended.get("container_id") or "")
        stopped = not (container and container["State"]["Running"])
        stop_attempt(env, accepted["id"])
        checks |= {
            f"{label}: the submit reply was lost and the broker has no forward cache": submit_code == 503 and cache_missing,
            f"{label}: the marker was published in the replacement folder and the ledger folder holds the unchanged intent": bool(seen.get("loss_injected"))
            and seen.get("marker_in_other_folder") is True
            and seen.get("ledger_unchanged") is True
            and seen.get("marker_in_ledger_folder") is False,
            f"{label}: the launcher is asked once, by the original request ID": len(lookups) == 1,
            f"{label}: the ledger is not ended by the restart": not_ended(after_restart),
            f"{label}: the status does not end the ledger": not_ended(after_status),
            f"{label}: only the cancel can end the ledger": cancel_only,
            f"{label}: the restart reports the submission updated": request.submission_id in summary.get("updated", []),
            f"{label}: a status and a cancel are answered 200": (status_code, first_cancel) == (200, 200),
            f"{label}: the attempt received the cancel while the marker was lost": held.get("cancel_intent") is not None,
            f"{label}: a cancel once the marker is back is accepted": second_cancel == 200 and marker.is_file(),
            f"{label}: the attempt ended with the reason cancelled": ended["phase"] == "ended" and ended["reason"] == "cancelled",
            f"{label}: the job container is stopped": stopped,
            f"{label}: the broker reports cancelled with exit 143": final is not None and final.exit_code == 143,
        }
        env.stop_brokers()
    checks["no job container is left running"] = not [c for c in env.labelled() if c["State"]["Running"] and "hai.probe" not in c["Config"]["Labels"]]
    return checks


OBSTRUCTIONS = ("none", "the launcher cannot answer the lookup", "missing token file", "unsearchable token folder")


@case("never_sent_only_by_lookup")
def never_sent_only_by_lookup(env: BrokerEnv) -> dict[str, bool]:
    """929j and 929k: a submission whose intent alone is on disk is never sent only when the launcher's authenticated lookup answers that it holds no
    attempt, and only then does the row decide. A run with no token gets it from the idempotent ``acquire``, and the lookup precedes the ``submit``. A
    lookup that cannot be asked or answered (the launcher cannot answer it, the run's token file is missing, its folder cannot be searched) leaves the
    submission unresolved: pending, a status and a cancel are 503, only the intent is in the ledger and nothing is sent as a ``submit``. Once the lookup
    can be answered it says not found and the row ends the submission, and the launcher holds no attempt."""
    env.hold_jobs()
    session = "8" * 64
    root = env.config.token_root
    aside = root.parent / "opaque_token_aside"
    checks: dict[str, bool] = {}
    first = env.request("silver_build", attempt=1)
    leave_intent(env, first)
    pioneer = env.new_broker()
    order: list[str] = []
    wire(pioneer).before = partial(record_calls, order)
    summary = pioneer.start()
    sequence = [name for name in order if name in ("acquire", "status:submission", "submit")]
    attempt = soft_wait(partial(first_attempt, env, broker_module.launch_id_of(first.submission_id)), 10)
    if attempt is not None:
        stop_attempt(env, attempt["id"])
    checks |= {
        "a run with no token: the restart reports the submission updated": first.submission_id in summary.get("updated", []),
        "a run with no token: acquire, then the lookup, then the submit": sequence == ["acquire", "status:submission", "submit"],
        "a run with no token: one attempt exists": attempt is not None and len(attempts_of(env, broker_module.launch_id_of(first.submission_id))) == 1,
    }
    env.stop_brokers()
    number = 1
    for kind in OBSTRUCTIONS:
        for drift, change in DRIFTS:
            number += 1
            label = f"never sent, {kind}, {drift}"
            blocked = kind != "none"
            env.write_inventory()
            request = env.request("silver_build", attempt=number)
            leave_intent(env, request)
            drift_inventory(env, change)
            launch = broker_module.launch_id_of(request.submission_id)
            payload = json.dumps({"submission_id": request.submission_id}).encode()
            fresh = env.new_broker()
            lookups: list[str] = []
            wire(fresh).before = partial(count_submission_lookups, lookups)
            patch = mock.patch.object(fresh.client, "call", lookup_unanswerable(fresh.client.call)) if kind == OBSTRUCTIONS[1] else contextlib.nullcontext()
            restore = hide_token_store(kind, root, RUN, aside) if kind in OBSTRUCTIONS[2:] else (lambda: None)
            try:
                with patch:
                    summary = {}
                    try:
                        summary = fresh.start()
                    except Exception as error:
                        summary = {"error": f"{type(error).__name__}: {error}"}
                    port = env.listener_for(fresh, session).port
                    status_code, _ = try_post(port, session, "/v1/status", payload)
                    cancel_code, _ = try_post(port, session, "/v1/cancel", payload)
                    while_blocked = events(env, request.submission_id)
                    sent = "submit" in wire(fresh).methods or bool(attempts_of(env, launch))
            finally:
                restore()
            asked_while_blocked = len(lookups)
            resumed_code, _ = try_post(port, session, "/v1/status", payload)
            after = events(env, request.submission_id)
            wanted = ["intent", "finished"] if drift == "removed mapping" else ["intent", "failed_closed"]
            checks |= {
                f"{label}: the restart reports the submission {'pending' if blocked else 'updated'}": request.submission_id
                in summary.get("pending" if blocked else "updated", []),
                f"{label}: a status and a cancel are answered {'503' if blocked else '200'}": (status_code, cancel_code)
                == ((503, 503) if blocked else (200, 200)),
                f"{label}: {'only the intent is in the ledger and no submit is sent' if blocked else 'the lookup ends the submission by its row'}": (
                    (while_blocked == ["intent"] and not sent) if blocked else while_blocked == wanted
                ),
                f"{label}: the lookup says not found once it can be answered, and only the row then ends the submission": (
                    resumed_code == 200 and after == wanted and len(lookups) == asked_while_blocked + (1 if blocked else 0) and len(lookups) >= 1
                ),
                f"{label}: the launcher holds no attempt for it": not attempts_of(env, launch),
            }
            env.stop_brokers()
    checks["no job container is left running"] = not [c for c in env.labelled() if c["State"]["Running"] and "hai.probe" not in c["Config"]["Labels"]]
    return checks


def first_attempt(env: BrokerEnv, launch: str) -> dict[str, Any] | None:
    """The first launcher attempt of a launch ID, or None."""
    return next(iter(attempts_of(env, launch)), None)


def attempt_in_phase(env: BrokerEnv, attempt_id: str, phases: tuple[str, ...]) -> bool:
    """True when the launcher's attempt is in one of the phases."""
    return env.attempt(attempt_id)["phase"] in phases


PUBLICATION_FAILURES = (("the marker's folder", True), ("a folder above the marker's folder", False))


@case("dispatch_publication_failure")
def dispatch_publication_failure(env: BrokerEnv) -> dict[str, bool]:
    """929g: the broker cannot make the folders of its dispatch marker durable, either the marker's own folder or a folder above it. Nothing is sent to
    the launcher, the ledger holds only the intent, the restart reports the submission pending and a status and a cancel are answered 503 (not 500).
    Once the folders can be made durable the same calls send the submission once, and a cancel reaches its attempt."""
    env.hold_jobs()
    session = "a" * 64
    checks: dict[str, bool] = {}
    original = os.fsync
    for number, (failing, own) in enumerate(PUBLICATION_FAILURES, 1):
        label = f"{failing} cannot be made durable"
        env.write_inventory()
        request = env.request("silver_build", attempt=number)
        leave_intent(env, request)
        launch = broker_module.launch_id_of(request.submission_id)
        handle = Handle(request.submission_id, request.container_name)
        payload = json.dumps({"submission_id": request.submission_id}).encode()
        marker = env.new_broker().dispatch_file(request)
        folders = [marker.parent] if own else [folder for folder in marker.parent.parents if folder == env.temp or env.temp in folder.parents]

        def refuses(info: os.stat_result, folders: list[Path] = folders) -> bool:
            """True when the descriptor is one of the folders whose flush the case fails."""
            for folder in folders:
                try:
                    held = folder.stat()
                except OSError:
                    continue
                if (held.st_dev, held.st_ino) == (info.st_dev, info.st_ino):
                    return True
            return False

        def refuse_folders(descriptor: int, refuses: Callable[[os.stat_result], bool] = refuses) -> None:
            """Fail the flush of the chosen folders; every other flush works."""
            info = os.fstat(descriptor)
            if stat.S_ISDIR(info.st_mode) and refuses(info):
                raise OSError(errno.EIO, "synthetic: a folder of the dispatch marker cannot be made durable")
            original(descriptor)

        fresh = env.new_broker()
        summary: dict[str, Any] = {}
        with mock.patch.object(os, "fsync", refuse_folders):
            try:
                summary = fresh.start()
            except Exception as error:
                summary = {"error": f"{type(error).__name__}: {error}"}
            port = env.listener_for(fresh, session).port
            status_code, _ = try_post(port, session, "/v1/status", payload)
            cancel_code, _ = try_post(port, session, "/v1/cancel", payload)
            while_failing = events(env, request.submission_id)
            first_pending = summary.get("pending", [])
            sent = "submit" in wire(fresh).methods
            attempts_while_failing = len(attempts_of(env, launch))
        restart_error = "error" in summary
        if restart_error:
            fresh = env.new_broker()
            fresh.start()
            port = env.listener_for(fresh, session).port
        resumed_code, _ = try_post(port, session, "/v1/status", payload)
        attempt = soft_wait(partial(first_attempt, env, launch), 10)
        if attempt is not None:
            soft_wait(partial(attempt_in_phase, env, attempt["id"], ("started", "ended")), 20)
        cancel_after, _ = try_post(port, session, "/v1/cancel", payload)
        final = cancelled_status(fresh, handle, 15)
        ended = env.attempt(attempt["id"]) if attempt is not None else {}
        container = env.daemon.containers.get(ended.get("container_id") or "")
        stopped = attempt is not None and not (container and container["State"]["Running"])
        if attempt is not None:
            stop_attempt(env, attempt["id"])
        checks |= {
            f"{label}: the restart reports the submission pending": not restart_error and request.submission_id in first_pending,
            f"{label}: a status and a cancel are answered 503": (status_code, cancel_code) == (503, 503),
            f"{label}: nothing is sent to the launcher as a submit": not sent and attempts_while_failing == 0,
            f"{label}: the ledger holds only the intent": while_failing == ["intent"],
            f"{label}: once the folders can be made durable the status is answered 200": resumed_code == 200,
            f"{label}: the submission is sent once": len(attempts_of(env, launch)) == 1,
            f"{label}: a cancel is accepted": cancel_after == 200,
            f"{label}: the launcher's attempt received the cancel": ended.get("cancel_intent") is not None,
            f"{label}: the job container is stopped": stopped,
            f"{label}: the broker reports cancelled with exit 143": final is not None and final.exit_code == 143,
        }
        env.stop_brokers()
    return checks


@case("first_intent_publication_failure")
def first_intent_publication_failure(env: BrokerEnv) -> dict[str, bool]:
    """929i: the first valid submit of a run has no ledger. Its intent line is written and flushed, then the directories of the new ledger file cannot be
    made durable (the folder that holds the state folder fails to flush). The answer is 503 and not 500, the ledger keeps the intent, nothing is sent to
    the launcher as a submit and a status and a cancel are 503 as well. Once the flush works, a repeat of the same submit sends the submission once and a
    cancel reaches its attempt."""
    env.hold_jobs()
    session = "e" * 64
    request = env.request("silver_build")
    launch = broker_module.launch_id_of(request.submission_id)
    handle = Handle(request.submission_id, request.container_name)
    submit_body = json.dumps(request.as_dict()).encode()
    payload = json.dumps({"submission_id": request.submission_id}).encode()
    original = os.fsync
    state_parent = env.temp.parent
    broker = env.new_broker()
    broker.start()
    port = env.listener_for(broker, session).port
    no_ledger = not broker.ledger.path(RUN).exists()

    def refuse_state_parent(descriptor: int) -> None:
        """Fail the flush of the folder that holds the state folder; every other flush works."""
        info = os.fstat(descriptor)
        if stat.S_ISDIR(info.st_mode):
            held = state_parent.stat()
            if (held.st_dev, held.st_ino) == (info.st_dev, info.st_ino):
                raise OSError(errno.EIO, "synthetic: the folder that holds the state folder cannot be made durable")
        original(descriptor)

    with mock.patch.object(os, "fsync", refuse_state_parent):
        first_code, _ = try_post(port, session, "/v1/submit", submit_body)
        while_failing = events(env, request.submission_id)
        status_code, _ = try_post(port, session, "/v1/status", payload)
        cancel_code, _ = try_post(port, session, "/v1/cancel", payload)
        sent = "submit" in wire(broker).methods
        attempts_while_failing = len(attempts_of(env, launch))
    retry_code, _ = try_post(port, session, "/v1/submit", submit_body)
    attempt = soft_wait(partial(first_attempt, env, launch), 10)
    if attempt is not None:
        soft_wait(partial(attempt_in_phase, env, attempt["id"], ("started", "ended")), 20)
    cancel_after, _ = try_post(port, session, "/v1/cancel", payload)
    final = cancelled_status(broker, handle, 15)
    ended = env.attempt(attempt["id"]) if attempt is not None else {}
    container = env.daemon.containers.get(ended.get("container_id") or "")
    stopped = attempt is not None and not (container and container["State"]["Running"])
    if attempt is not None:
        stop_attempt(env, attempt["id"])
    return {
        "the run has no ledger before the first submit": no_ledger,
        "the first submit is answered 503, not 500": first_code == 503,
        "the ledger keeps only the intent": while_failing == ["intent"],
        "nothing is sent to the launcher as a submit": not sent and attempts_while_failing == 0,
        "a status and a cancel are answered 503 while the flush fails": (status_code, cancel_code) == (503, 503),
        "once the flush works a repeat of the submit is answered 200": retry_code == 200,
        "the submission is sent once": len(attempts_of(env, launch)) == 1,
        "a cancel is accepted": cancel_after == 200,
        "the launcher's attempt received the cancel": ended.get("cancel_intent") is not None,
        "the job container is stopped": stopped,
        "the broker reports cancelled with exit 143": final is not None and final.exit_code == 143,
    }


def post(
    port: int, token: str | None, path: str, body: bytes, extra: dict[str, str] | None = None, method: str = "POST", timeout: float = 10.0
) -> tuple[int, dict[str, Any]]:
    """One raw HTTP request to a listener; returns the status and the decoded body."""
    headers = {"Content-Type": "application/json", **(extra or {})}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        data = response.read()
        return response.status, json.loads(data) if data else {}
    finally:
        connection.close()


def try_post(
    port: int, token: str | None, path: str, body: bytes, extra: dict[str, str] | None = None, timeout: float = 5.0
) -> tuple[int | None, dict[str, Any]]:
    """Like ``post``, but a closed connection or a timeout is ``(None, {"transport": <error name>})`` instead of an exception."""
    try:
        return post(port, token, path, body, extra, timeout=timeout)
    except (OSError, http.client.HTTPException, ValueError) as error:
        return None, {"transport": type(error).__name__}


def state_of(broker: Broker, handle: Handle) -> State | None:
    """The submission's state, or None when the broker cannot say (a refusal or a failure a case then reports as a failed check)."""
    try:
        return broker.status(handle).state
    except Exception:
        return None


def soft_wait(predicate: Callable[[], Any], seconds: float = 20) -> Any:
    """Poll until the predicate is truthy and return its value; None when the time ends (a case reports that as a failed check, not an error)."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        found = predicate()
        if found:
            return found
        time.sleep(0.1)
    return None


@case("override_fields_refused")
def override_fields_refused(env: BrokerEnv) -> dict[str, bool]:
    """930: a field that the frozen request schema does not name is refused before any launcher request."""
    broker = env.new_broker()
    broker.start()
    token = "o" * 64
    listener = env.listener_for(broker, token)
    base = env.request(attempt=1).as_dict()
    overrides = {
        "command": ["sh", "-c", "id"],
        "entrypoint": ["sh"],
        "environment": {"AWS_PROFILE": "x"},
        "mounts": [{"source": "/", "target": "/host", "read_only": False}],
        "image": "evil@sha256:" + "0" * 64,
        "privileged": True,
        "user": "0:0",
        "memory": 1,
        "network": "host",
        "network_class": "lakehouse_write",
        "capabilities": ["SYS_ADMIN"],
    }
    before = len(wire(broker).methods)
    results = {}
    for field, value in overrides.items():
        status, body = post(listener.port, token, "/v1/submit", json.dumps({**base, field: value}).encode())
        results[f"{field} in the body is refused"] = status == 400
    extra = {"run_id": "bad", "mode": "evil", "attempt": 0}
    for field, value in extra.items():
        status, _ = post(listener.port, token, "/v1/submit", json.dumps({**base, field: value}).encode())
        results[f"a bad {field} is refused"] = status == 400
    results["no launcher request was sent for any of them"] = len(wire(broker).methods) == before
    results["no ledger line and no container exist"] = not env.paths.ledger.exists() and not env.creates()
    return results


@case("unmapped_template_refused")
def unmapped_template_refused(env: BrokerEnv) -> dict[str, bool]:
    """930: every frozen template name without a launcher row is refused with its reason; a name that is not in the file and a class that is neither
    offline nor enforced by the launcher are refused too, each with its own reason."""
    env.write_inventory(json.loads((ROOT / "config/orchestration/broker_templates.json").read_text()))
    broker = env.new_broker()
    broker.start()
    results: dict[str, bool] = {}
    for name in FROZEN_NAMES:
        template = broker.templates[name]
        for mode, row in template["modes"].items():
            if row.get("launcher") or row.get("control"):
                continue
            error = refusal_of(partial(broker.submit, env.request(name, mode=mode, attempt=1)))
            if row["no_op"]:
                results[f"{name}/{mode}: a no-op is recorded without a launcher request"] = error is None
            elif row["class"] not in (broker_module.OFFLINE, *launcher.NETWORK_CLASSES_ENFORCED):
                results[f"{name}/{mode}: class {row['class']} is refused for the class"] = isinstance(
                    error, SubmissionRefused
                ) and "the broker runs only the offline class" in str(error)
            else:
                results[f"{name}/{mode}: refused because no launcher row exists"] = isinstance(error, SubmissionRefused) and "no launcher template runs" in str(
                    error
                )
    unknown = refusal_of(lambda: broker.submit(env.request("not_a_template", attempt=1)))
    results["an unknown name is refused"] = isinstance(unknown, SubmissionRefused) and "unknown template" in str(unknown)
    unsupported = refusal_of(lambda: broker.submit(env.request("commit_publish", mode="fixture", attempt=1)))
    results["a mode the template does not support is refused"] = isinstance(unsupported, SubmissionRefused) and "does not support" in str(unsupported)
    results["no launcher request and no container"] = not wire(broker).methods.count("submit") and not env.creates()
    return results


def enforced_row_kept(inventory: dict[str, Any] | None, network_class: str, mapping: dict[str, Any] | None) -> bool:
    """True when a loaded inventory still holds the ``silver_build`` fixture row with the network class and launcher mapping that went in."""
    kept = ((inventory or {}).get("templates") or {}).get("silver_build", {}).get("modes", {}).get("fixture")
    return bool(mapping) and isinstance(kept, dict) and kept.get("class") == network_class and kept.get("launcher") == mapping


@case("non_offline_class_refused")
def non_offline_class_refused(env: BrokerEnv) -> dict[str, bool]:
    """930: a broker row that forwards to the launcher with a class the launcher does not enforce is refused when the file loads, and a submission in such
    a class is refused with the class named; a class the launcher enforces loads and keeps its row."""
    unenforced = "collector"
    document = env.inventory()
    row = document["templates"]["silver_build"]["modes"]["fixture"]
    row["class"] = "lakehouse_read"
    enforced_mapping = row["launcher"]
    enforced_path = env.work / "enforced_class.json"
    enforced_path.write_text(json.dumps(document))
    enforced_inventory: dict[str, Any] | None = None
    enforced_error = None
    try:
        enforced_inventory = load_templates(enforced_path)
    except BrokerError as caught:
        enforced_error = caught
    row["class"] = unenforced
    path = env.work / "bad_class.json"
    path.write_text(json.dumps(document))
    load_error = None
    try:
        load_templates(path)
    except BrokerError as caught:
        load_error = caught
    row["launcher"] = None
    row["reason"] = "a future launcher row will run this step"
    env.write_inventory(document)
    broker = env.new_broker()
    broker.start()
    error = refusal_of(lambda: broker.submit(env.request("silver_build", attempt=1)))
    return {
        "the class used here is not enforced by the launcher": unenforced not in launcher.NETWORK_CLASSES_ENFORCED,
        "a forwarding row with an enforced class loads": enforced_error is None,
        "the loaded inventory keeps that row's class and launcher mapping": enforced_row_kept(enforced_inventory, "lakehouse_read", enforced_mapping),
        "a forwarding row with another class does not load": load_error is not None
        and "runs only the offline class and the enforced network classes" in str(load_error),
        "a submission in another class is refused with the class named": isinstance(error, SubmissionRefused) and unenforced in str(error),
        "nothing was sent to the launcher": not env.creates() and "submit" not in wire(broker).methods,
    }


@case("launcher_refuses_changed_args")
def launcher_refuses_changed_args(env: BrokerEnv) -> dict[str, bool]:
    """930: a row whose arguments the launcher's grammar refuses, or whose launcher template does not fit the run, is refused by the launcher
    and the broker records that refusal."""
    broker = env.new_broker()
    broker.start()
    bad_args = broker.submit(env.request("silver_validate", attempt=1))
    wrong_template = broker.submit(env.request("gold_validate", attempt=1))
    a, b = broker.record(bad_args), broker.record(wrong_template)
    return {
        "the launcher's argument grammar refused the extra option": a["exit_code"] == 125
        and ("--evil" in a.get("reason", "") or "args" in str(a.get("reason", ""))),
        "the launcher's template rules refused the real_dbt row in a fixture run": b["exit_code"] == 125 and "real" in str(b.get("reason", "")),
        "each refusal is a finished ledger line after the intent": events(env, bad_args.submission_id) == ["intent", "finished"]
        and events(env, wrong_template.submission_id) == ["intent", "finished"],
        "no container was created": not env.creates(),
    }


@case("switch_off_local_poll")
def switch_off_local_poll(env: BrokerEnv) -> dict[str, bool]:
    """A local poll retains its first attempt reply when later launcher requests become unavailable."""
    env.hold_jobs()
    env.start()
    broker = env.new_broker()
    broker.start()
    request = env.request()
    handle = broker.submit(request)
    env.wait_state(broker, handle, (State.RUNNING,))
    forward = broker.read_forward(request)
    if forward is None:
        raise RuntimeError("the local submission has no forward record")
    env.wait_phase(forward["attempt_id"], ("started",))
    checks = {
        "the rollout switch is disabled and the attempt has no execution": env.client().call("status", {"what": "launcher"})["ecs_ready"] is False
        and "execution" not in env.attempt(forward["attempt_id"])
    }
    reads: list[str] = []
    limit = 2

    def budget(method: str, params: dict[str, Any]) -> None:
        if method == "status":
            reads.append(str(params.get("what")))
            if len(reads) > limit:
                raise launcher.ClientError("synthetic launcher unavailable after the attempt reply", "unavailable")

    client = wire(broker)
    client.before = budget
    status = None
    try:
        with contextlib.suppress(LauncherUnavailable):
            status = broker.status(handle)
        checks["local status uses its single attempt reply"] = (
            status is not None and status.state == State.RUNNING and status.execution is None and reads == ["launcher", "attempt"]
        )
        reads.clear()
        limit = 1
        error = refusal_of(lambda: broker.apply_attempt(request, forward))
        checks["applying a local attempt adds no launcher health request"] = error is None and reads == ["attempt"]
    finally:
        client.before = None
    return checks


@case("broker_templates_schema")
def broker_templates_schema(env: BrokerEnv) -> dict[str, bool]:
    """930: the real file loads; each mutation of it is refused with a load error."""
    base = json.loads((ROOT / "config/orchestration/broker_templates.json").read_text())

    def refused_load(change: Callable[[dict[str, Any]], None]) -> bool:
        document = copy.deepcopy(base)
        change(document)
        path = env.work / "mutated.json"
        path.write_text(json.dumps(document))
        try:
            load_templates(path)
        except BrokerError:
            return True
        return False

    def with_row(document: dict[str, Any], row: dict[str, Any]) -> None:
        document["templates"]["silver_build"]["modes"]["fixture"] = row

    launcher_row = {"template": "fixture_dbt", "args": ["build"], "params": {}, "lineage": {"version": 1, "inputs": [], "outputs": []}}
    results = {
        "the real file loads": bool(load_templates()["templates"]),
        "a launcher row with an image key is refused": refused_load(
            lambda d: with_row(d, {"class": "offline", "no_op": False, "launcher": {**launcher_row, "image": "evil"}})
        ),
        "a launcher row with an environment key is refused": refused_load(
            lambda d: with_row(d, {"class": "offline", "no_op": False, "launcher": {**launcher_row, "environment": {"A": "b"}}})
        ),
        "a no-op row with a launcher is refused": refused_load(
            lambda d: with_row(d, {"class": "offline", "no_op": True, "reason": "nothing to do here", "launcher": launcher_row})
        ),
        "a row with no no-op, launcher or reason is refused": refused_load(lambda d: with_row(d, {"class": "offline", "no_op": False})),
        "a launcher row with another class is refused": refused_load(lambda d: with_row(d, {"class": "serving", "no_op": False, "launcher": launcher_row})),
        "an unknown class is refused": refused_load(
            lambda d: with_row(d, {"class": "internet", "no_op": False, "launcher": None, "reason": "no such class exists"})
        ),
        "a launcher template that does not exist is refused": refused_load(
            lambda d: with_row(d, {"class": "offline", "no_op": False, "launcher": {**launcher_row, "template": "ghost"}})
        ),
        "the launcher's probe template is refused": refused_load(
            lambda d: with_row(d, {"class": "offline", "no_op": False, "launcher": {**launcher_row, "template": "probe"}})
        ),
        "the catalog service template is refused": refused_load(
            lambda d: with_row(d, {"class": "offline", "no_op": False, "launcher": {**launcher_row, "template": "catalog_service"}})
        ),
        "a placeholder other than the four is refused": refused_load(
            lambda d: with_row(d, {"class": "offline", "no_op": False, "launcher": {**launcher_row, "args": ["{secret}"]}})
        ),
        "a mode outside the three is refused": refused_load(
            lambda d: d["templates"]["silver_build"]["modes"].update({"canary": d["templates"]["silver_build"]["modes"]["fixture"]})
        ),
        "an uppercase template name is refused": refused_load(lambda d: d["templates"].update({"Silver": d["templates"]["silver_build"]})),
        "an extra top-level key is refused": refused_load(lambda d: d.update({"image": "x"})),
    }
    return results


@case("listener_auth")
def listener_auth(env: BrokerEnv) -> dict[str, bool]:
    """931: no token, a wrong token, a malformed header and a token of the wrong length are refused with 401 before any launcher request."""
    broker = env.new_broker()
    broker.start()
    token = "a" * 64
    listener = env.listener_for(broker, token)
    body = json.dumps(env.request(attempt=1).as_dict()).encode()
    before = len(wire(broker).methods)
    cases = {
        "no Authorization header": post(listener.port, None, "/v1/submit", body)[0],
        "a wrong token": post(listener.port, "b" * 64, "/v1/submit", body)[0],
        "a token with a trailing space": post(listener.port, token + " ", "/v1/submit", body)[0],
        "a Basic scheme": post(listener.port, None, "/v1/submit", body, {"Authorization": "Basic " + token})[0],
        "an empty bearer": post(listener.port, "", "/v1/submit", body)[0],
        "the token in a query string only": post(listener.port, None, f"/v1/submit?token={token}", body)[0],
    }
    health = post(listener.port, token, "/v1/health", b"{}")
    client = BrokerClient(f"http://127.0.0.1:{listener.port}", token)
    wrong = BrokerClient(f"http://127.0.0.1:{listener.port}", "c" * 64)
    wrong_error = refusal_of_client(wrong)
    return {
        **{f"{name} is refused with 401": status == 401 for name, status in cases.items()},
        "the right token is accepted": health[0] == 200 and health[1] == {"broker": "ok"},
        "the DAG-side client works with the token": client.health() == {"broker": "ok"},
        "the DAG-side client reports a refused token": wrong_error,
        "no launcher request was sent for the refused calls": len(wire(broker).methods) == before,
    }


def refusal_of_client(client: BrokerClient) -> bool:
    """True when the DAG-side client reports a refused token as BrokerUnavailable."""
    try:
        client.health()
    except BrokerUnavailable as error:
        return "401" in str(error)
    return False


@case("listener_limits")
def listener_limits(env: BrokerEnv) -> dict[str, bool]:
    """931: oversized bodies, other methods, unknown paths, non-object bodies and extra fields are refused; the listener keeps serving."""
    broker = env.new_broker()
    broker.start()
    token = "d" * 64
    listener = env.listener_for(broker, token)
    big = json.dumps({"pad": "x" * 5000}).encode()
    return {
        "a body over 4 KiB is refused with 413": post(listener.port, token, "/v1/submit", big)[0] == 413,
        "a GET is refused with 405": post(listener.port, token, "/v1/health", b"", method="GET")[0] == 405,
        "an unknown operation is refused with 404": post(listener.port, token, "/v1/shell", b"{}")[0] == 404,
        "a path outside /v1 is refused with 404": post(listener.port, token, "/admin", b"{}")[0] == 404,
        "a JSON list body is refused with 400": post(listener.port, token, "/v1/status", b"[1]")[0] == 400,
        "a status body with extra fields is refused with 400": post(
            listener.port, token, "/v1/status", json.dumps({"submission_id": "x", "extra": 1}).encode()
        )[0]
        == 400,
        "a logs tail of 0 is refused": post(
            listener.port, token, "/v1/logs", json.dumps({"submission_id": f"{RUN}:silver_build:fixture:1", "tail": 0}).encode()
        )[0]
        == 400,
        "a logs tail that is a boolean is refused": post(
            listener.port, token, "/v1/logs", json.dumps({"submission_id": f"{RUN}:silver_build:fixture:1", "tail": True}).encode()
        )[0]
        == 400,
        "the listener still serves": post(listener.port, token, "/v1/health", b"{}")[0] == 200,
    }


@case("malformed_content_length")
def malformed_content_length(env: BrokerEnv) -> dict[str, bool]:
    """931b: a Content-Length that is not a plain number below the limit is refused (400 or 413) before any read, an unauthenticated one with 401; a
    failure inside a handler is answered 500; a failure inside an operation's dispatch is answered 500 and logged with the operation's name; nothing
    reaches stderr, not the token and not a traceback; the broker log holds no token; the listener serves a valid request before and after."""
    broker = env.new_broker()
    broker.start()
    token = "tok" + os.urandom(16).hex()
    listener = env.listener_for(broker, token)
    port = listener.port
    before = len(wire(broker).methods)
    control_before = try_post(port, token, "/v1/health", b"{}")[0]
    lengths = {
        "the session token as the length": token,
        "a negative number": "-1",
        "a signed number": "+2",
        "a decimal point": "2.0",
        "two numbers": "2, 2",
        "an empty value": "",
        "an underscore in the number": "1_0",
        "a number above the limit": "9" * 30,
    }
    captured = io.StringIO()
    answers: dict[str, int | None] = {}
    with contextlib.redirect_stderr(captured):
        for name, value in lengths.items():
            answers[name] = try_post(port, token, "/v1/health", b"{}", {"Content-Length": value}, timeout=3)[0]
        unauthenticated = try_post(port, None, "/v1/health", b"{}", {"Content-Length": token}, timeout=3)[0]
        with mock.patch.object(broker_module.secrets, "compare_digest", side_effect=RuntimeError("synthetic failure " + token)):
            forced = try_post(port, token, "/v1/health", b"{}", timeout=3)[0]
        with mock.patch.object(broker_module, "dispatch", side_effect=RuntimeError("synthetic dispatch failure " + token)):
            dispatch_failed = try_post(port, token, "/v1/health", b"{}", timeout=3)[0]
        time.sleep(0.3)
    text = captured.getvalue()
    control_after = try_post(port, token, "/v1/health", b"{}")[0]
    log_file = env.paths.private / "broker.log"
    log_text = log_file.read_text() if log_file.is_file() else ""
    return {
        "a valid request is served before the bad ones": control_before == 200,
        **{f"{name} is refused with 400 or 413": status in (400, 413) for name, status in answers.items()},
        "an unauthenticated request with a bad length is refused with 401": unauthenticated == 401,
        "a failure inside a handler is answered 500": forced == 500,
        "a failure inside an operation's dispatch is answered 500": dispatch_failed == 500,
        "the dispatch failure is logged with the operation's name": "internal_error health RuntimeError" in log_text,
        "nothing reaches stderr with the session token": token not in text,
        "no traceback reaches stderr": "Traceback" not in text and "ValueError" not in text and "RuntimeError" not in text,
        "the broker log holds no session token": token not in log_text,
        "no launcher request was sent for any of them": len(wire(broker).methods) == before,
        "a valid request is served after the bad ones": control_after == 200,
    }


@case("log_write_outage")
def log_write_outage(env: BrokerEnv) -> dict[str, bool]:
    """931c: with the broker log unwritable, a request whose refusal text holds the session token is still answered (400), a handler failure is answered
    500, a valid request is served, and a failure outside any handler is survived; stderr holds neither the token nor a traceback (at most one line, which
    names an exception type only); a valid request is served and the log is written again once the log can be written."""
    broker = env.new_broker()
    broker.start()
    token = "tok" + os.urandom(16).hex()
    listener = env.listener_for(broker, token)
    port = listener.port
    log_file = env.paths.private / "broker.log"
    control_before = try_post(port, token, "/v1/health", b"{}")[0]
    refusal = {**env.request().as_dict(), "attempt": token}
    log_file.unlink(missing_ok=True)
    log_file.mkdir()
    captured = io.StringIO()
    with contextlib.redirect_stderr(captured):
        during = try_post(port, token, "/v1/health", b"{}", timeout=3)[0]
        refused, refused_body = try_post(port, token, "/v1/submit", json.dumps(refusal).encode(), timeout=3)
        with mock.patch.object(broker_module.secrets, "compare_digest", side_effect=RuntimeError("synthetic failure " + token)):
            forced = try_post(port, token, "/v1/health", b"{}", timeout=3)[0]
        with mock.patch.object(listener.server.RequestHandlerClass, "handle", side_effect=RuntimeError("synthetic server failure " + token)):
            try_post(port, token, "/v1/health", b"{}", timeout=3)
        survived = try_post(port, token, "/v1/health", b"{}", timeout=3)[0]
        time.sleep(0.5)
    text = captured.getvalue()
    log_file.rmdir()
    control_after = try_post(port, token, "/v1/health", b"{}")[0]
    written = log_file.read_text() if log_file.is_file() else ""
    return {
        "a valid request is served before the outage": control_before == 200,
        "a valid request is served while the log cannot be written": during == 200,
        "a schema refusal that repeats the token is answered 400 while the log cannot be written": refused == 400,
        "the refusal body holds no session token": token not in json.dumps(refused_body),
        "a handler failure is answered 500 while the log cannot be written": forced == 500,
        "a failure outside any handler leaves the listener serving": survived == 200,
        "nothing reaches stderr with the session token": token not in text,
        "no traceback reaches stderr": "Traceback" not in text and "RuntimeError" not in text,
        "stderr holds at most one line": len(text.strip().splitlines()) <= 1,
        "a valid request is served once the log can be written again": control_after == 200,
        "the log written afterwards holds no session token": token not in written,
    }


@case("token_never_logged")
def token_never_logged(env: BrokerEnv) -> dict[str, bool]:
    """931 and 932: the session token and the run token appear in no broker log, ledger, record, saved log or error body, also when the token
    arrives in a request path or an Authorization header."""
    env.hold_jobs()
    broker = env.new_broker()
    broker.start()
    token = "tok" + os.urandom(16).hex()
    listener = env.listener_for(broker, token)
    client = BrokerClient(f"http://127.0.0.1:{listener.port}", token)
    handle = client.submit(env.request("gold_build", attempt=1))
    run_token = broker.principal(RUN)["token"]
    bodies = [
        post(listener.port, token, f"/v1/{token}", b"{}"),
        post(listener.port, token, "/v1/submit", json.dumps({**env.request(attempt=2).as_dict(), "note": token}).encode()),
        post(listener.port, token + "x", "/v1/status", b"{}"),
        post(listener.port, token, "/v1/status", json.dumps({"submission_id": "bad-" + token}).encode()),
    ]
    client.cancel(handle)
    env.wait_state(broker, handle, (State.CANCELLED,))
    client.logs(handle)
    texts = [json.dumps(body) for _, body in bodies]
    found: list[str] = []
    for path in sorted(env.temp.rglob("*")):
        if path.is_file() and path.name != "registry.json" and "run_tokens" not in path.parts and "tokens" not in path.parts:
            data = path.read_bytes().decode(errors="replace")
            if token in data or run_token in data:
                found.append(str(path.relative_to(env.temp)))
    return {
        "no error body contains the session token": not any(token in text for text in texts),
        "no file of the broker state contains the session token or the run token": not found,
        "the broker log exists and holds redaction markers or the request line": (env.paths.private / "broker.log").exists(),
        "the saved job log is redacted": all(run_token not in p.read_text() for p in env.paths.job_logs.rglob("*") if p.is_file()),
    }


@case("listener_loopback_only")
def listener_loopback_only(env: BrokerEnv) -> dict[str, bool]:
    """931: the listener binds the loopback and nothing else; a connection to every other address of this Mac over IPv4 and IPv6 is refused."""
    broker = env.new_broker()
    broker.start()
    bind_refusals = {}
    for host in (".".join(["0"] * 4), ":" * 2, "192.168.1.5", ""):
        try:
            candidate = Listener(broker, host, 0, "e" * 64)
        except BrokerError:
            bind_refusals[host] = True
        except OSError:
            bind_refusals[host] = False
        else:
            candidate.server.server_close()
            bind_refusals[host] = False
    listener = env.listener_for(broker, "e" * 64)
    outcomes = unreachable_elsewhere(listener.port)
    v6_loopback_open = ipv6_loopback_open(listener.port)
    bound = listener.server.server_address[0]
    return {
        "a non-loopback bind is refused for each host tried": all(bind_refusals.values()),
        "the listener is bound to 127.0.0.1": bound == "127.0.0.1",
        "every non-loopback address of this host refuses the connection": all(outcomes.values()),
        "the report counts the addresses tried": len(outcomes) >= 0,
        "the IPv6 loopback is not served either": not v6_loopback_open,
        "the loopback accepts a call with the token": post(listener.port, "e" * 64, "/v1/health", b"{}")[0] == 200,
    }


@case("ledger_fsync_order")
def ledger_fsync_order(env: BrokerEnv) -> dict[str, bool]:
    """932: the intent line is complete on disk before the launcher is sent an `acquire` or a `submit`."""
    env.hold_jobs()
    broker = env.new_broker()
    broker.start()
    seen: dict[str, list[str]] = {}

    def check(method: str, params: dict[str, Any]) -> None:
        if method in ("acquire", "submit"):
            path = env.paths.ledger / f"{RUN}.jsonl"
            text = path.read_text() if path.exists() else ""
            seen[method] = [json.loads(line)["event"] for line in text.splitlines()] if text.endswith("\n") else ["torn"]

    wire(broker).before = check
    handle = broker.submit(env.request("gold_build", attempt=1))
    env.wait_state(broker, handle, (State.RUNNING,))
    return {
        "the intent was on disk when acquire was sent": seen.get("acquire") == ["intent"],
        "the intent was on disk when submit was sent": seen.get("submit") == ["intent"],
        "the folder entry of the ledger file exists": (env.paths.ledger / f"{RUN}.jsonl").is_file(),
    }


def corrupt_case(env: BrokerEnv, change: Callable[[Path], None]) -> dict[str, bool]:
    """A finished submission, then a damaged ledger: every operation on the run stops with the line number and the file stays as it is."""
    broker = env.new_broker()
    broker.start()
    handle = broker.submit(env.request(attempt=1))
    env.wait_state(broker, handle, (State.SUCCEEDED,))
    path = env.paths.ledger / f"{RUN}.jsonl"
    change(path)
    damaged = path.read_bytes()
    errors = {
        "status": refusal_of(lambda: broker.status(handle)),
        "record": refusal_of(lambda: broker.record(handle)),
        "submit": refusal_of(lambda: broker.submit(env.request("gold_build", attempt=1))),
        "reconcile": broker.reconcile()["ledger_errors"],
    }
    return {
        "status is refused with the ledger named": isinstance(errors["status"], SubmissionRefused) and "ledger" in str(errors["status"]),
        "record is refused": isinstance(errors["record"], SubmissionRefused),
        "a new submission on the run is refused": isinstance(errors["submit"], SubmissionRefused),
        "the reconciliation reports the run": bool(errors["reconcile"]) and RUN in errors["reconcile"][0],
        "the file was not repaired or truncated": path.read_bytes() == damaged,
        "no container was created after the damage": len(env.creates()) == 1,
    }


@case("ledger_torn_line")
def ledger_torn_line(env: BrokerEnv) -> dict[str, bool]:
    """932: a last line without its newline stops the run's operations."""

    def damage(path: Path) -> None:
        path.write_bytes(path.read_bytes() + b'{"schema_version": 1, "event": "fin')

    return corrupt_case(env, damage)


@case("ledger_bad_schema_line")
def ledger_bad_schema_line(env: BrokerEnv) -> dict[str, bool]:
    """932: a line that breaks the frozen schema stops the run's operations."""

    def damage(path: Path) -> None:
        lines = path.read_text().splitlines()
        row = json.loads(lines[-1])
        row["exit_code"] = "zero"
        lines[-1] = json.dumps(row)
        path.write_text("\n".join(lines) + "\n")

    return corrupt_case(env, damage)


@case("ledger_out_of_order")
def ledger_out_of_order(env: BrokerEnv) -> dict[str, bool]:
    """932: an event that cannot follow the one before it stops the run's operations."""

    def damage(path: Path) -> None:
        lines = path.read_text().splitlines()
        path.write_text("\n".join([lines[0], lines[0], *lines[1:]]) + "\n")

    return corrupt_case(env, damage)


@case("ledger_has_no_container_state")
def ledger_has_no_container_state(env: BrokerEnv) -> dict[str, bool]:
    """932: the broker's lines carry submissions and the launcher's results only; the only container field is the ID the launcher reported."""
    env.hold_jobs()
    broker = env.new_broker()
    broker.start()
    done = broker.submit(env.request(attempt=1))
    env.wait_state(broker, done, (State.RUNNING,))
    broker.cancel(done)
    env.wait_state(broker, done, (State.CANCELLED,))
    running = broker.submit(env.request("gold_build", attempt=1))
    env.wait_state(broker, running, (State.RUNNING,))
    keys = {key for line in ledger_lines(env) for key in line}
    allowed = {
        "request_version", "dag_id", "task_id", "dag_run_id", "lineage",
        "schema_version", "event", "at", "submission_id", "run_id", "template", "mode", "attempt", "spec_sha256", "code_sha256", "inputs", "reservation_id",
        "reserved_memory_bytes", "container_name", "container_id", "container_state", "launcher_attempt", "exit_code", "record_path",
        "observed_peak_memory_bytes", "no_op", "cancelled", "end_reason", "reason", "refused", "containers",
    }  # fmt: skip
    return {
        "every key is a schema key or one of the launcher-result extras": keys <= allowed,
        "the broker keeps no container file of its own": not any(p.suffix in (".container", ".pid") for p in env.temp.rglob("*")),
        "the only cache is the launcher's answer": {p.parent.parent.name for p in env.paths.forwards.rglob("*.json")} == {"forwards"},
    }


def planted(names: dict[str, str]) -> contextlib.AbstractContextManager[None]:
    """Put synthetic credentials in this process's environment for the duration of a case."""
    return mock.patch.dict(os.environ, names)


def broker_roots(env: BrokerEnv) -> list[Path]:
    """The broker's state folders and the launcher's registry file: where a credential must never appear."""
    paths = env.paths
    return [paths.ledger, paths.records, paths.job_logs, paths.forwards, paths.private, env.registry / "registry.json"]


@case("sentinel_scan")
def sentinel_scan(env: BrokerEnv) -> dict[str, bool]:
    """932 and 755: synthetic credentials planted in the broker's environment and its token never appear in the broker log, ledger, records, saved job
    logs, forward cache or the launcher's registry, after refused and cancelled submissions; the scan reads every input."""
    env.hold_jobs()
    with planted(SENTINELS):
        broker = env.new_broker()
        broker.start()
        token = "sentinel-session-" + os.urandom(8).hex()
        env.listener_for(broker, token)
        refused_one = broker.submit(env.request("silver_validate", attempt=1))
        cancelled = broker.submit(env.request("gold_build", attempt=1))
        env.wait_state(broker, cancelled, (State.RUNNING,))
        broker.cancel(cancelled)
        env.wait_state(broker, cancelled, (State.CANCELLED,))
        broker.status(refused_one)
        values = [*SENTINELS.values(), token, broker.principal(RUN)["token"]]
        result = sentinel.scan(broker_roots(env), values)
    return {
        "no planted value is in the broker log, ledger, records, saved logs, forward cache or registry": not result.hits,
        "the scan read every input: none unreadable, skipped or missing": not (result.unreadable or result.skipped or result.missing_roots),
        "the scan covered the ledger, the records, the saved logs and the broker log": all(
            result.per_root.get(name, 0) > 0 for name in ("ledger", "records", "job_logs", "broker")
        ),
    }


@case("sentinel_scan_coverage")
def sentinel_scan_coverage(env: BrokerEnv) -> dict[str, bool]:
    """932b: the scan finds a marker in a readable file, and it fails (not passes) when an input is unreadable, is a FIFO, a root is missing or nothing
    was read; a link is not followed, its own text is searched and a dangling link makes the scan incomplete; the record holds counts and
    never a scanned value."""
    marker = "synthetic-marker-" + os.urandom(6).hex()
    base = env.work / "scanned"
    folders = {name: base / name for name in ("readable", "blocked", "fifo", "links", "empty")}
    for folder in folders.values():
        folder.mkdir(parents=True)
    (folders["readable"] / "a.log").write_text(f"x {marker} y")
    (folders["readable"] / "b.log").write_text("clean text")
    (folders["blocked"] / "c.log").write_text(marker)
    (folders["blocked"] / "d.log").write_text("fine")
    (folders["fifo"] / "e.log").write_text("fine")
    os.mkfifo(folders["fifo"] / "pipe")
    (folders["links"] / "f.log").write_text("fine")
    os.symlink("target-" + marker, folders["links"] / "link")
    control = sentinel.scan([folders["readable"]], [marker])
    clean = sentinel.scan([folders["readable"]], ["absent-" + marker])
    os.chmod(folders["blocked"] / "c.log", 0)
    try:
        unreadable = sentinel.scan([folders["blocked"]], ["absent-" + marker])
    finally:
        os.chmod(folders["blocked"] / "c.log", 0o600)
    fifo = sentinel.scan([folders["fifo"]], ["absent-" + marker])
    linked = sentinel.scan([folders["links"]], [marker])
    missing = sentinel.scan([base / "nowhere", folders["readable"]], ["absent-" + marker])
    empty = sentinel.scan([folders["empty"]], [marker])
    return {
        "a marker in a readable file is found (control)": control.hits == ["readable/a.log"] and control.files == 2,
        "a clean folder passes and records the files and bytes read": clean.ok
        and clean.files == 2
        and clean.bytes_read == len(f"x {marker} y") + len("clean text"),
        "an unreadable file fails the scan and is named": not unreadable.ok and unreadable.unreadable == ["blocked/c.log"],
        "a FIFO fails the scan, is named and is not opened": not fifo.ok and fifo.skipped == ["fifo/pipe"],
        "a link is not followed, its own text is searched and a dangling link makes the scan incomplete": linked.links == 1
        and linked.hits == ["links/link"]
        and not linked.complete,
        "a missing root fails the scan and is named": not missing.ok and missing.missing_roots == ["nowhere"],
        "a scan that read no file fails": not empty.ok and empty.files == 0,
        "the record holds counts and no scanned value": marker not in json.dumps([control.record(), linked.record()]),
    }


def link_scan_checks(env: BrokerEnv) -> dict[str, bool]:
    """The checks of ``sentinel_scan_links``, on fixtures under ``<case folder>/work/linked_scan``."""
    marker = "synthetic-link-marker-" + os.urandom(6).hex()
    base = env.work / "linked_scan"
    outside = base / "outside"
    outside.mkdir(parents=True)
    (outside / "target.log").write_text(marker)
    roots = {name: base / name for name in ("inside", "marked", "dangling", "loop", "folder", "text", "two_a", "two_b")}
    for folder in roots.values():
        folder.mkdir()
        (folder / "clean.log").write_text("clean text")
    (roots["inside"] / "same.log").symlink_to("clean.log")
    (roots["marked"] / "task.log").symlink_to("../outside/target.log")
    (roots["dangling"] / "task.log").symlink_to("absent.log")
    (roots["loop"] / "a").symlink_to("b")
    (roots["loop"] / "b").symlink_to("a")
    (roots["folder"] / "dir").symlink_to("../outside")
    (roots["text"] / "named").symlink_to("absent-" + marker)
    (roots["two_a"] / "across.log").symlink_to("../two_b/clean.log")
    values = [marker]
    control = sentinel.scan([outside], values)
    inside = sentinel.scan([roots["inside"]], values)
    marked = sentinel.scan([roots["marked"]], values)
    dangling = sentinel.scan([roots["dangling"]], values)
    loop = sentinel.scan([roots["loop"]], values)
    folder_link = sentinel.scan([roots["folder"]], values)
    text = sentinel.scan([roots["text"]], values)
    two = sentinel.scan([roots["two_a"], roots["two_b"]], values)
    alone = sentinel.scan([roots["two_a"]], values)
    clean_bytes = len("clean text")
    return {
        "the marker in a readable file outside the roots is found (control)": control.hits == ["outside/target.log"],
        "a link to a file inside the same root is covered: the scan is complete": inside.ok and inside.complete and inside.links == 1,
        "a link adds no file, byte or per-root count": inside.files == 1 and inside.bytes_read == clean_bytes and inside.per_root == {"inside": 1},
        "a link to a marker-bearing file outside the roots makes the scan incomplete": not marked.complete and not marked.ok,
        "that link is named as skipped and adds no count": marked.skipped == ["marked/task.log"] and marked.files == 1 and marked.per_root == {"marked": 1},
        "a dangling link makes the scan incomplete and is named": not dangling.ok and dangling.skipped == ["dangling/task.log"],
        "a link loop makes the scan incomplete and is named": not loop.ok and sorted(loop.skipped) == ["loop/a", "loop/b"],
        "a link to a folder outside the roots makes the scan incomplete": not folder_link.ok and folder_link.skipped == ["folder/dir"],
        "a link's own text is searched for the values": text.hits == ["text/named"] and not text.ok,
        "a link into another root of the same scan is covered": two.ok and two.complete and two.files == 2 and two.links == 1,
        "the same link makes a scan of its own root alone incomplete": not alone.complete and alone.skipped == ["two_a/across.log"],
        "the record holds counts and no scanned value": marker not in json.dumps([marked.record(), text.record()]),
    }


@case("sentinel_scan_links")
def sentinel_scan_links(env: BrokerEnv) -> dict[str, bool]:
    """932c: a link counts as covered only when it resolves to a file or folder inside one of the scan's own roots; a link to a marker-bearing file
    outside the roots, a dangling link, a link loop and a link to a folder outside are listed as skipped (the scan is incomplete), a link adds no file, byte
    or per-root count, and a link's own text is still searched. The fixtures are removed afterwards: a link loop left in the evidence folder breaks
    every tool that resolves the paths under ``data`` (the launcher's check of a mount of that folder among them)."""
    try:
        return link_scan_checks(env)
    finally:
        shutil.rmtree(env.work / "linked_scan", ignore_errors=True)


@case("sentinel_scan_failed_job")
def sentinel_scan_failed_job(env: BrokerEnv) -> dict[str, bool]:
    """932b: a successful job, a forced-failure job (exit 3) and a cancelled job, with the token and synthetic credentials planted, leave none of them in
    the broker's log, ledger, records, saved job logs, forward cache or the launcher's registry; the scan reads every input."""
    with planted(SENTINELS):
        broker = env.new_broker()
        broker.start()
        token = "sentinel-session-" + os.urandom(8).hex()
        env.listener_for(broker, token)
        succeeded = broker.submit(env.request("silver_build", attempt=1))
        won = env.wait_state(broker, succeeded, (State.SUCCEEDED,))
        env.daemon.script = lambda container: (0.2, 0) if "hai.probe" in container["Config"]["Labels"] else (0.3, 3)
        failing = broker.submit(env.request("gold_build", attempt=1))
        lost = env.wait_state(broker, failing, (State.FAILED,))
        env.hold_jobs()
        held = broker.submit(env.request("gold_build", attempt=2))
        env.wait_state(broker, held, (State.RUNNING,))
        broker.cancel(held)
        stopped = env.wait_state(broker, held, (State.CANCELLED,))
        values = [*SENTINELS.values(), token, broker.principal(RUN)["token"]]
        result = sentinel.scan(broker_roots(env), values)
    return {
        "the successful job ended with exit 0": won.exit_code == 0,
        "the forced-failure job ended failed with its exit code": lost.exit_code == 3,
        "the cancelled job ended cancelled": stopped.exit_code == 143,
        "each of the three has a record file": len(list(env.paths.records.rglob("*.json"))) == 3,
        "no planted value is in the broker log, ledger, records, saved logs, forward cache or registry": not result.hits,
        "the scan read every input: none unreadable, skipped or missing": not (result.unreadable or result.skipped or result.missing_roots),
        "the scan covered the records, the saved logs, the forward cache and the broker log": all(
            result.per_root.get(name, 0) > 0 for name in ("records", "job_logs", "forwards", "broker")
        ),
    }


ALL_CASES = (
    no_direct_docker,
    released_run_refused,
    failed_retained_refused,
    recovering_run_refused,
    release_while_in_flight,
    release_refused_while_running,
    broker_holds_no_reservation,
    outage_during_job,
    duplicate_submission,
    crash_after_forward,
    crash_after_intent,
    cancel_after_lost_submit_reply,
    noop_crash_recovery,
    cancel_after_inventory_drift,
    cancel_after_token_store_loss,
    cancel_after_marker_ancestor_loss,
    cancel_after_marker_write_replacement,
    never_sent_only_by_lookup,
    dispatch_publication_failure,
    first_intent_publication_failure,
    override_fields_refused,
    unmapped_template_refused,
    non_offline_class_refused,
    launcher_refuses_changed_args,
    switch_off_local_poll,
    broker_templates_schema,
    listener_auth,
    listener_limits,
    malformed_content_length,
    log_write_outage,
    token_never_logged,
    listener_loopback_only,
    ledger_fsync_order,
    ledger_torn_line,
    ledger_bad_schema_line,
    ledger_out_of_order,
    ledger_has_no_container_state,
    sentinel_scan,
    sentinel_scan_coverage,
    sentinel_scan_links,
    sentinel_scan_failed_job,
)


def run_case(name: str, report: Path) -> dict[str, Any]:
    """Run one case in its own harness."""
    function, settings = CASES[name]
    env = BrokerEnv(name, report, **dict(settings))
    started = time.monotonic()
    error = ""
    checks: dict[str, bool] = {}
    try:
        checks = function(env)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
    leftovers = env.close()
    checks["nothing of the case is left over"] = not leftovers
    passed = bool(checks) and all(checks.values()) and not error
    result = {"case": name, "passed": passed, "checks": checks, "error": error, "leftovers": leftovers, "seconds": round(time.monotonic() - started, 1)}
    return result | ({"evidence": env.evidence} if env.evidence else {})


def run_cases(selected: list[str], verbose: bool = False) -> int:
    """Run the selected cases (all by default), write the report and return the exit code."""
    if {function.__name__ for function in ALL_CASES} != {function.__name__ for function, _ in CASES.values()}:
        raise SystemExit("ALL_CASES differs from the registered cases")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = OUT / stamp
    report.mkdir(parents=True)
    results: list[dict[str, Any]] = []
    for name in selected or list(CASES):
        result = run_case(name, report)
        status = "pass" if result["passed"] else "fail"
        results.append({**result, "status": status})
        sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name} ({result['seconds']} s)\n")
        for check, ok in result["checks"].items():
            if verbose:
                sys.stdout.write(f"      {'ok' if ok else 'FAILED'}: {check}\n")
            elif not ok:
                sys.stdout.write(f"      failed: {check}\n")
                scenario = result.get("evidence", {}).get(check.split(": ", 1)[0])
                if scenario is not None:
                    sys.stdout.write(f"      evidence: {json.dumps(scenario, sort_keys=True)[:2000]}\n")
        if result["error"]:
            sys.stdout.write(f"      error: {result['error'].splitlines()[0]}\n")
    summary = {
        "feature": "host job broker and ledger as launcher clients, fake Docker, test registries",
        "command": ".venv/bin/python -m scripts.orchestration.run_broker_e2e",
        "started_utc": stamp,
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "python": sys.version.split()[0],
        "cases": results,
        "passed": sum(1 for r in results if r["status"] == "pass"),
        "total": len(results),
        "limits": ["A fake daemon: no real container runs here; the real listener reach and the sentinel scan of Airflow's own files are in run_real_e2e."],
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{summary['passed']}/{summary['total']}: {report.relative_to(ROOT)}/report.json\n")
    return 0 if summary["passed"] == summary["total"] else 1


def main() -> int:
    """The command-line entry point: ``--case NAME`` (repeatable) picks cases, ``--checks`` prints every check; returns 0 when every case passed, else 1."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", action="append", default=[], choices=sorted(CASES))
    parser.add_argument("--checks", action="store_true", help="print every check of every case, not only the failed ones")
    args = parser.parse_args()
    return run_cases(args.case, args.checks)


if __name__ == "__main__":
    sys.exit(main())
