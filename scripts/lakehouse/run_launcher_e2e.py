"""End-to-end runs of the single launcher against a fake Docker Engine, in test registries (D1 section 15, failure modes 910 to 919).

Run ``.venv/bin/python -m scripts.lakehouse.run_launcher_e2e [--case NAME ...]``. Each case starts its own launcher process on its own test
registry and its own fake daemon (scripts/lakehouse/fake_docker.py), drives it through the real client over the real socket, and asserts the
registry state and every resource that carries the registry label. A case ends by stopping its launcher and checking that no container,
network, socket or process of the case is left. The report with every check, the registry files and the daemon's call log of each case
stays in data/e2e/launcher/<stamp>/. No real Docker, catalog, AWS service or secret is used; the token root of a test registry is inside it.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import errno
import fcntl
import hashlib
import importlib
import io
import json
import logging
import os
import re
import shlex
import shutil
import signal
import socket
import stat
import sys
import tarfile
import tempfile
import threading
import time
import traceback
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import launcher
from scripts.lakehouse import pins as pins_module
from scripts.lakehouse import run_lock
from scripts.lakehouse.fake_docker import FakeDocker, Gate
from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "launcher"
TIMINGS = {
    "create_timeout": 3,
    "probe_timeout": 2,
    "tick": 0.05,
    "backoff_start": 0.2,
    "backoff_cap": 1,
    "stop_grace": 1,
    "idle_session": 3,
    "docker_call_timeout": 10,
    "health_wait": 10,
    "restart_wait": 30,
}
COMPOSE_ENV_NAMES = {
    "POSTGRES_PASSWORD": "pg-",
    "POLARIS_CLIENT_ID": "root",
    "POLARIS_CLIENT_SECRET": "pcs-",
    "POLARIS_READER_CLIENT_ID": "reader-id",
    "POLARIS_READER_CLIENT_SECRET": "prs-",
    "AWS_PROFILE": "fake-profile",
    "AWS_REGION": "us-fake-1",
    "POLARIS_MEMORY_LIMIT": "1024m",
    "POLARIS_DB_MEMORY_LIMIT": "256m",
}


GIB = 1024**3


def executable(path: Path, body: str) -> None:
    """Write a synthetic command response without executing downloaded code."""
    path.write_text("#!/bin/sh\nset -eu\n" + body + "\n")
    path.chmod(0o700)


def response(value: str) -> str:
    """Return a shell statement that prints only the supplied synthetic value."""
    return "printf '%s\\n' " + shlex.quote(value)


def host_tools(binary: Path, *, mac_gib: int, vm_gib: int, host_cpus: str) -> None:
    """Synthetic host readings: CPU count, the Mac's free memory and the Docker VM's resident memory."""
    binary.mkdir(parents=True, exist_ok=True)
    executable(binary / "sysctl", response(host_cpus))
    executable(
        binary / "ps", f'if [ "$1" = "-axo" ]; then {response(f"{vm_gib * GIB // 1024} com.apple.Virtualization.VirtualMachine")}; else exec /bin/ps "$@"; fi'
    )
    executable(
        binary / "vm_stat",
        response(
            f"Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: {mac_gib * GIB // 16384}.\nFile-backed pages: 0.\nPages purgeable: 0."
        ),
    )


class CaseFailure(Exception):
    """A case could not reach the state it needs to assert on."""


class Env:
    """One case: a test registry, a fake daemon, a launcher process and the evidence folders."""

    def __init__(self, name: str, report: Path, stub_host: bool = True, **settings: Any) -> None:
        self.name = name
        self.saved_path = os.environ["PATH"]
        self.report = report / name
        self.report.mkdir(parents=True)
        self.work = self.report / "work"
        self.work.mkdir()
        self.temp = Path(tempfile.mkdtemp(prefix="hl"))
        self.registry = self.temp / "registry"
        self.registry.mkdir(mode=0o700)
        self.daemon = FakeDocker(self.temp / "d.sock")
        self.secret_values = {name: (prefix + os.urandom(6).hex() if prefix.endswith("-") else prefix) for name, prefix in COMPOSE_ENV_NAMES.items()}
        compose_env = self.temp / "compose.env"
        compose_env.write_text("".join(f"{key}={value}\n" for key, value in self.secret_values.items()))
        compose_env.chmod(0o600)
        self.settings: dict[str, Any] = {
            "docker_socket": str(self.temp / "d.sock"),
            "timings": {**TIMINGS, **settings.pop("timings", {})},
            "compose_env": str(compose_env),
            "lakehouse_state": str(self.temp / "lakehouse"),
            "runs_root": str(self.temp / "runs"),
            "token_root": str(self.temp / "tokens"),
            **settings,
        }
        self.write_settings()
        for image in self.compose_images():
            self.daemon.images[image] = {
                "Id": "sha256:" + os.urandom(8).hex(),
                "Config": {
                    "Labels": {launcher.DEFAULT_IMAGE_LABEL: "1", launcher.DEFAULT_IMAGE_IMPLEMENTATION: "go"},
                    "Env": ["PATH=/usr/local/bin:/usr/bin", "LANG=C.UTF-8"],
                    "User": "",
                    "WorkingDir": "",
                    # The polaris-admin-tool image runs its tool by an entrypoint of its own (no Python); the others have none.
                    "Entrypoint": ["/opt/polaris/entry"] if "polaris-admin" in image else None,
                    "Cmd": ["sh"],
                },
            }
        if stub_host:
            # Synthetic host readings (64 GiB free on the Mac, a 2 GiB Docker VM, 16 CPUs), so a plan's figures do not depend on this machine.
            host_tools(self.work / "bin", mac_gib=64, vm_gib=2, host_cpus="16")
            os.environ["PATH"] = str(self.work / "bin") + os.pathsep + self.saved_path
        self.threads: list[threading.Thread] = []
        self.stuck = False

    def write_settings(self) -> None:
        """Write the test registry's launcher settings."""
        (self.registry / "launcher_test.json").write_text(json.dumps(self.settings, indent=2))

    @staticmethod
    def compose_images() -> list[str]:
        """Every image the Compose model names."""
        model = launcher.ComposeModel(ROOT / "docker-compose.yaml")
        return sorted({str(service["image"]) for service in model.services.values()})

    @property
    def config(self) -> launcher.Config:
        """The launcher's configuration for this registry."""
        return launcher.resolve_config(str(self.registry))

    def registry_id(self) -> str:
        """The registry's ID; resolving the configuration creates the registry if it does not exist yet."""
        launcher.resolve_config(str(self.registry))
        return str(json.loads((self.registry / "registry_id").read_text())["registry_id"])

    def client(self, start: bool = True) -> launcher.LauncherClient:
        """A client; it starts the launcher on demand."""
        return launcher.LauncherClient(self.config, start=start)

    # -- launcher process ------------------------------------------------------------------------------------------

    def start(self, wait_serving: bool = True) -> int:
        """Start the launcher (as a client would) and wait until it serves."""
        client = self.client()
        client.connect().close()
        if wait_serving:
            self.wait(lambda: self.ping().get("status") == "serving", 30, "the launcher serves")
        return int(self.ping()["pid"])

    def ping(self) -> dict[str, Any]:
        """The launcher's ping, or an empty dict when it does not answer."""
        try:
            return dict(self.client(start=False).call("ping"))
        except (launcher.ClientError, OSError, ValueError, run_lock.RegistryError):
            return {}

    def kill_launcher(self, number: int = signal.SIGKILL) -> None:
        """Kill the launcher process and wait until it is gone."""
        pid = self.ping().get("pid")
        if pid is None:
            return
        os.kill(int(pid), number)
        self.wait(lambda: not self.alive(int(pid)), 15, "the launcher process ends")

    @staticmethod
    def alive(pid: int) -> bool:
        """True while the process exists."""
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True

    def teardown(self) -> None:
        """End a case: cancel what is still open, release allocations and stop the catalog, so nothing of the case outlives it."""
        if not self.ping():
            return
        client = self.client(start=False)
        with contextlib.suppress(launcher.ClientError, OSError, KeyError, ValueError, CaseFailure):
            for attempt_id, attempt in self.state()["attempts"].items():
                if attempt["phase"] != "ended" and attempt["owner"].startswith(("run:", "alloc:")):
                    owner = attempt["owner"]
                    principal = client.principal_run(owner.partition(":")[2]) if owner.startswith("run:") else self.alloc_principal(owner.partition(":")[2])
                    client.call("cancel", {"attempt_id": attempt_id}, principal)
            self.wait(
                lambda: not [a for a in self.state()["attempts"].values() if a["phase"] != "ended" and not a["owner"].startswith("service:")],
                20,
                "attempts end",
            )
            for allocation_id, allocation in self.state()["allocations"].items():
                if allocation["kind"] == "shared" and allocation["state"] == "held":
                    client.call("release_allocation", {}, self.alloc_principal(allocation_id))
            if (self.state()["launcher"].get("catalog") or {}).get("phase") in ("up", "starting"):
                self.wait(lambda: self.state()["launcher"]["catalog"]["phase"] != "starting", 40, "the catalog start ends")
            if (self.state()["launcher"].get("catalog") or {}).get("phase") == "up":
                client.call("catalog_down")
                self.wait(lambda: self.state()["launcher"]["catalog"]["phase"] == "down", 40, "the catalog is down")

    def stop_launcher(self) -> None:
        """Stop the launcher cleanly."""
        if self.ping():
            with_pid = int(self.ping()["pid"])
            with contextlib.suppress(launcher.ClientError, OSError):
                self.client(start=False).call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
            try:
                self.wait(lambda: not self.ping() if with_pid == os.getpid() else not self.alive(with_pid), 15, "the launcher stops")
            except CaseFailure:
                os.kill(with_pid, signal.SIGKILL)
                self.stuck = True

    # -- state and waiting -----------------------------------------------------------------------------------------

    def state(self) -> dict[str, Any]:
        """The registry file as the launcher last wrote it."""
        return dict(json.loads((self.registry / "registry.json").read_text()))

    def attempt(self, attempt_id: str) -> dict[str, Any]:
        """One attempt record."""
        return dict(self.state()["attempts"][attempt_id])

    def wait(self, predicate: Callable[[], Any], seconds: float = 20, what: str = "") -> Any:
        """Poll until the predicate is truthy; a timeout is a case failure with the description."""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                value = predicate()
            except (OSError, ValueError, KeyError, launcher.ClientError):
                value = None
            if value:
                return value
            time.sleep(0.05)
        raise CaseFailure(f"timeout after {seconds} s waiting for: {what or predicate}")

    def labelled(self) -> list[dict[str, Any]]:
        """The daemon's containers that carry this registry's label."""
        registry_id = self.state()["registry_id"]
        with self.daemon.lock:
            return [c for c in self.daemon.containers.values() if c["Config"]["Labels"].get(launcher.LABEL_REGISTRY) == registry_id]

    def phase(self, attempt_id: str) -> str:
        """An attempt's phase."""
        return str(self.attempt(attempt_id)["phase"])

    def wait_phase(self, attempt_id: str, phases: tuple[str, ...], seconds: float = 20) -> dict[str, Any]:
        """Wait until the attempt is in one of the phases."""
        return dict(self.wait(lambda: (a := self.attempt(attempt_id))["phase"] in phases and a, seconds, f"attempt {attempt_id} in {phases}"))

    def wait_ended(self, attempt_id: str, seconds: float = 30) -> dict[str, Any]:
        """Wait until the attempt has ended."""
        return self.wait_phase(attempt_id, ("ended",), seconds)

    # -- fixtures --------------------------------------------------------------------------------------------------

    def case_folders(self, name: str = "case") -> tuple[Path, Path]:
        """A case folder and a package folder inside the data folder, where the templates allow mounts."""
        case = self.work / name
        packages = self.work / "packages"
        case.mkdir(parents=True, exist_ok=True)
        packages.mkdir(exist_ok=True)
        return case, packages

    def fixture_params(self, name: str = "case") -> dict[str, Any]:
        """Parameters of a ``fixture_dbt`` attempt."""
        case, packages = self.case_folders(name)
        return {
            "case_host": str(case),
            "case_container": f"/workspace/out/{name}",
            "packages_host": str(packages),
            "dbt_vars": {"check_release_copy_conflicts": False},
        }

    def allocate(self, workers: int = 1) -> str:
        """An allocation held by this process."""
        return self.client().allocate(workers)

    def alloc_principal(self, alloc_id: str) -> dict[str, Any]:
        """The principal of an allocation."""
        return launcher.LauncherClient.principal_alloc(alloc_id)

    def submit(
        self,
        principal: dict[str, Any],
        template: str = "fixture_dbt",
        launch: str = "l1",
        args: list[str] | None = None,
        params: dict[str, Any] | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        """Submit an attempt and return the reply."""
        body = {
            "template": template,
            "launch_id": launch,
            "args": args if args is not None else ["build"],
            "params": params if params is not None else self.fixture_params(),
            "checkout": str(ROOT),
            **extra,
        }
        return dict(self.client().call("submit", body, principal))

    def acquire_run(self, run_id: str = "run_a", domain: str = "transform", mode: str = "fixture", **extra: Any) -> dict[str, Any]:
        """Acquire a run; returns the principal."""
        client = self.client()
        client.call("acquire", {"domain": domain, "run_id": run_id, "mode": mode, "checkout": str(ROOT), **extra})
        return client.principal_run(run_id)

    def make_real(self, run_id: str) -> None:
        """Mark a held run of this test registry as a real run in the registry file (a test registry refuses to acquire one; the flip stands in for it)."""
        with run_lock.transaction(run_lock.Registry(self.registry, "test")) as state:
            state["runs"][run_id]["mode"] = "real"

    # -- cleanup and evidence --------------------------------------------------------------------------------------

    def leftovers(self) -> list[str]:
        """Everything of the case that must not outlive it."""
        found = []
        if self.alive_launcher():
            found.append("launcher process")
        with self.daemon.lock:
            found += [f"container {c['Name']}" for c in self.daemon.containers.values()]
            found += [f"network {n['Name']}" for n in self.daemon.networks.values()]
        if (self.registry / "launcher.sock").exists():
            found.append("launcher socket")
        return found

    def alive_launcher(self) -> bool:
        """True while a launcher answers on the registry's socket."""
        return bool(self.ping())

    def close(self) -> list[str]:
        """Stop the launcher and the daemon, keep the evidence, remove the scratch folder; returns what was left over."""
        for thread in self.threads:
            thread.join(timeout=5)
        self.teardown()
        self.stop_launcher()
        left = self.leftovers() + (["launcher did not stop on request"] if self.stuck else [])
        evidence = self.report / "evidence"
        evidence.mkdir()
        for name in ("registry.json", "registry_id", "launcher.log", "launcher_test.json", "pool_cancel_unresolved.json"):
            if (self.registry / name).exists():
                shutil.copy2(self.registry / name, evidence / name)
        (evidence / "daemon_calls.json").write_text(json.dumps(self.daemon.calls, indent=1))
        (evidence / "daemon_events.json").write_text(json.dumps(self.daemon.event_log, indent=1))
        for sub in ("allocations", "services"):
            if (self.registry / sub).exists():
                shutil.copytree(self.registry / sub, evidence / sub)
        if (self.temp / "runs").exists():
            shutil.copytree(self.temp / "runs", evidence / "runs")
        self.daemon.close()
        shutil.rmtree(self.temp, ignore_errors=True)
        os.environ["PATH"] = self.saved_path
        return left


Case = Callable[[Env], dict[str, bool]]
CASES: dict[str, tuple[Case, dict[str, Any]]] = {}


def case(name: str, **settings: Any) -> Callable[[Case], Case]:
    """Register a case under a name, with the settings of its test registry.

    Every case gets synthetic host readings (``stub_host``, the default), so admission and capacity never depend on this machine's free
    memory; ``stub_host=False`` would read the real host.
    """

    def register(function: Case) -> Case:
        CASES[name] = (function, settings)
        return function

    return register


def refused(error: BaseException | None, code: str | None = None, text: str | None = None) -> bool:
    """True when the error is a launcher refusal with the given code and text."""
    return isinstance(error, launcher.ClientError) and (code is None or error.code == code) and (text is None or text in str(error))


def expect_error_of(function: Callable[[], Any]) -> BaseException | None:
    """Run a call that should be refused with a launcher error; returns the error, or None when it was accepted."""
    try:
        function()
    except launcher.LauncherError as error:
        return error
    return None


def expect_error(function: Callable[[], Any]) -> BaseException | None:
    """Run a call that should be refused; returns the error, or None when it was accepted."""
    try:
        function()
    except launcher.ClientError as error:
        return error
    return None


# ------------------------------------------------------------------------------------------------------------------
# Cases
# ------------------------------------------------------------------------------------------------------------------


@case("happy_path")
def happy_path(env: Env) -> dict[str, bool]:
    """A finite attempt on an allocation runs to its end; every resource is gone; the spec carries the deadline entrypoint."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    reply = env.submit(principal)
    attempt = env.wait_ended(reply["attempt_id"])
    return {
        "attempt exited": attempt["reason"] == "exited" and attempt["exit_code"] == 0,
        "one create": len(creates(env, "-l1-")) == 1,
        "no container left": env.labelled() == [],
        "allocation releasable": env.client().call("release_allocation", {}, principal) == {"state": "released"},
    }


def creates(env: Env, part: str) -> list[dict[str, Any]]:
    """The create calls for containers whose name contains the part."""
    return [c for c in env.daemon.calls_matching("POST", "/containers/create") if part in (c.get("name") or "")]


def starts(env: Env, part: str) -> list[dict[str, Any]]:
    """The start calls for containers whose name contains the part."""
    return [c for c in env.daemon.calls_matching("POST", "/containers/") if c["path"].endswith("/start") and part in (c.get("target") or "")]


def hold_forever(env: Env) -> None:
    """Containers started by the daemon keep running until stopped."""
    env.daemon.script = lambda container: None


def orphan_names(env: Env) -> list[str]:
    """The names of containers the launcher disposed of as orphans."""
    return [str(item["name"]) for item in env.state()["launcher"].get("orphans", [])]


def record_file(env: Env, run_id: str) -> Path:
    """A durable run record, as ``release`` after success requires."""
    path = env.work / f"{run_id}.json"
    path.write_text(json.dumps({"run_id": run_id}))
    return path


@case("duplicate_submit")
def duplicate_submit(env: Env) -> dict[str, bool]:
    """910: the same request returns the same attempt; a second open attempt of one launch under a new request ID is refused."""
    hold_forever(env)
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    body = {"template": "fixture_dbt", "launch_id": "l1", "args": ["build"], "params": env.fixture_params(), "checkout": str(ROOT)}
    client = env.client()
    first = client.call("submit", body, principal, request_id="req-1")
    again = client.call("submit", body, principal, request_id="req-1")
    changed = expect_error(lambda: client.call("submit", {**body, "args": ["test"]}, principal, request_id="req-1"))
    second = expect_error(lambda: client.call("submit", body, principal, request_id="req-2"))
    env.wait_phase(first["attempt_id"], ("started",))
    client.call("cancel", {"attempt_id": first["attempt_id"]}, principal)
    env.wait_ended(first["attempt_id"])
    return {
        "same request returns the same attempt": first == again,
        "changed payload under the same request ID refused": refused(changed, "replay"),
        "second open attempt of one launch refused": refused(second, "refused", "already has open attempt"),
        "one container created": len(creates(env, "-l1-")) == 1,
        "cancelled attempt ended cancelled": env.attempt(first["attempt_id"])["reason"] == "cancelled",
    }


@case("adoption_event_before_reply")
def adoption_event_before_reply(env: Env) -> dict[str, bool]:
    """910: a create event that arrives before the create reply is adopted, not swept."""
    env.daemon.event_mode = "before_reply"
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    gate = env.daemon.add_gate("create", "after", match=lambda c: "-l1-" in c["name"])
    attempt_id = env.submit(principal)["attempt_id"]
    gate.wait_reached()
    adopted = env.wait(lambda: env.attempt(attempt_id)["container_id"], 10, "adoption from the event alone")
    held_phase = env.phase(attempt_id)
    gate.release()
    ended = env.wait_ended(attempt_id)
    return {
        "adopted while the reply was withheld": bool(adopted) and held_phase in ("created", "starting", "started"),
        "attempt exited": ended["reason"] == "exited",
        "not swept as an orphan": not any("-l1-" in name for name in orphan_names(env)),
        "one create": len(creates(env, "-l1-")) == 1,
    }


@case("adoption_after_launcher_kill")
def adoption_after_launcher_kill(env: Env) -> dict[str, bool]:
    """910: a launcher killed before it bound a container adopts it on restart."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    gate = env.daemon.add_gate("create", "after", match=lambda c: "-l1-" in c["name"])
    attempt_id = env.submit(principal)["attempt_id"]
    gate.wait_reached()
    env.kill_launcher()
    gate.release()
    env.wait(lambda: len(env.labelled()) >= 1, 10, "the container exists")
    env.start()
    ended = env.wait_ended(attempt_id, 40)
    return {
        "attempt completed after the restart": ended["reason"] == "exited" and ended["exit_code"] == 0,
        "the container was adopted, not removed as an orphan": not any("-l1-" in name for name in orphan_names(env)),
        "one create": len(creates(env, "-l1-")) == 1,
    }


@case("cancel_during_create")
def cancel_during_create(env: Env) -> dict[str, bool]:
    """910: a cancel during create undoes the result; the container is removed and never started."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    gate = env.daemon.add_gate("create", "before", match=lambda c: "-l1-" in c["name"])
    attempt_id = env.submit(principal)["attempt_id"]
    gate.wait_reached()
    env.client().call("cancel", {"attempt_id": attempt_id}, principal)
    gate.release()
    ended = env.wait_ended(attempt_id)
    return {
        "attempt ended cancelled": ended["reason"] == "cancelled",
        "the container was never started": not starts(env, "-l1-"),
        "no container left": env.labelled() == [],
        "cleanup recorded an exit step": ended["cleanup"] is not None,
    }


@case("cancel_during_start")
def cancel_during_start(env: Env) -> dict[str, bool]:
    """910: a cancel during start undoes the result; the started container is stopped and removed."""
    hold_forever(env)
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    gate = env.daemon.add_gate("start", "before", match=lambda c: "-l1-" in c["name"])
    attempt_id = env.submit(principal)["attempt_id"]
    gate.wait_reached()
    env.client().call("cancel", {"attempt_id": attempt_id}, principal)
    gate.release()
    ended = env.wait_ended(attempt_id)
    return {
        "attempt ended cancelled": ended["reason"] == "cancelled",
        "no container left": env.labelled() == [],
        "the container was stopped": any(c["path"].endswith("/stop") and "-l1-" in (c.get("target") or "") for c in env.daemon.calls),
    }


def lost_start_reply(env: Env, revoke: str) -> dict[str, bool]:
    """910 and 912: a start request is in flight (its reply never reaches the launcher), the owner's right to start is revoked, the launcher restarts.

    On reconciliation the container is still created and the start operation is open. The restarted launcher must not dispatch a second
    start; the container is undone through cleanup authority and the held daemon request finds nothing to start.
    """
    hold_forever(env)
    env.start()
    if revoke == "recovery":
        principal = env.acquire_run("run_a")
    else:
        alloc = env.allocate()
        principal = env.alloc_principal(alloc)
    gate = env.daemon.add_gate("start", "before", match=lambda c: "-l1-" in c["name"])
    options: dict[str, Any] = {"timeout_seconds": 6} if revoke == "expiry" else {}
    attempt_id = env.submit(principal, **options)["attempt_id"]
    gate.wait_reached()
    if revoke == "cancel":
        env.client().call("cancel", {"attempt_id": attempt_id}, principal)
    elif revoke == "recovery":
        env.client().call("recover", {"owner": True}, env.client().principal_run("run_a", owner=True))
    env.kill_launcher()
    if revoke == "expiry":
        time.sleep(4)  # past the start cutoff (deadline - 3 s)
    first_starts = len(starts(env, "-l1-"))
    env.start()
    ended = env.wait_ended(attempt_id, 40)
    after = len(starts(env, "-l1-"))
    gate.release()
    time.sleep(0.5)
    reasons = {"cancel": "cancelled", "recovery": "fenced", "expiry": "timeout"}
    return {
        f"the attempt ended {reasons[revoke]}": ended["reason"] == reasons[revoke],
        "exactly one start request reached the daemon (the original one, in flight)": first_starts == 1 and after == 1,
        "the container is not running and not left": env.labelled() == [] and not [c for c in env.daemon.containers.values() if c["State"]["Running"]],
        "the start operation was settled": all(op["state"] == "settled" for op in env.state()["operations"].values() if op["attempt"] == attempt_id),
    }


@case("start_lost_reply_cancel_restart")
def start_lost_reply_cancel_restart(env: Env) -> dict[str, bool]:
    """910 (F1): a cancel persisted while a start is in flight; the launcher restarts; no second start."""
    return lost_start_reply(env, "cancel")


@case("start_lost_reply_recovery_restart")
def start_lost_reply_recovery_restart(env: Env) -> dict[str, bool]:
    """910 (F1): owner recovery (generation fence) while a start is in flight; the launcher restarts; no second start."""
    return lost_start_reply(env, "recovery")


@case("start_lost_reply_expiry_restart")
def start_lost_reply_expiry_restart(env: Env) -> dict[str, bool]:
    """910 and 912 (F1): the host start cutoff passes while a start is in flight; the launcher restarts; no second start."""
    return lost_start_reply(env, "expiry")


@case("create_after_create_failed")
def create_after_create_failed(env: Env) -> dict[str, bool]:
    """910: a create that completes after the launcher settled it as failed is removed on sight and never started."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    gate = env.daemon.add_gate("create", "before", match=lambda c: "-l1-" in c["name"])
    attempt_id = env.submit(principal)["attempt_id"]
    gate.wait_reached()
    ended = env.wait_ended(attempt_id, 40)
    gate.release()
    env.wait(lambda: not env.labelled() and any("-l1-" in n for n in orphan_names(env)), 20, "the late container is removed")
    return {
        "attempt ended create_failed": ended["reason"] == "create_failed",
        "the late container was removed as an orphan": any("-l1-" in n for n in orphan_names(env)),
        "never started": not starts(env, "-l1-"),
        "no container left": env.labelled() == [],
        "the settled operation carries its evidence": any(
            op.get("evidence") for op in env.state()["operations"].values() if op["kind"] == "create" and op["result"] == "create_failed"
        ),
    }


@case("subscribe_before_scan")
def subscribe_before_scan(env: Env) -> dict[str, bool]:
    """910: the subscription is opened before the first scan and the probe orders the two."""
    env.start()
    paths = [c["path"] for c in env.daemon.calls]
    first_events = paths.index("/events")
    first_listing = paths.index("/containers/json")
    first_probe = next(i for i, c in enumerate(env.daemon.calls) if c["path"] == "/containers/create")
    return {
        "events opened before the first listing": first_events < first_listing,
        "probe created after the subscription and before the scan": first_events < first_probe < first_listing,
        "the probe container was removed": not [c for c in env.labelled() if "probe" in c["Name"]],
    }


@case("release_refused_open_work")
def release_refused_open_work(env: Env) -> dict[str, bool]:
    """910: release is refused with an attempt in creating or exiting, an open stop operation and a registration."""
    hold_forever(env)
    env.start()
    client = env.client()
    checks: dict[str, bool] = {}

    def try_release(principal: dict[str, Any], run_id: str) -> BaseException | None:
        return expect_error(lambda: client.call("release", {"record": str(record_file(env, run_id))}, principal))

    principal = env.acquire_run("run_a", "transform")
    gate = env.daemon.add_gate("create", "before", match=lambda c: "-a-" in c["name"] or "-l1-" in c["name"])
    attempt_a = env.submit(principal, params=env.fixture_params("a"))["attempt_id"]
    gate.wait_reached()
    client.call("finalize", {}, principal)
    checks["refused in creating with open create"] = (
        env.phase(attempt_a) == "creating"
        and any(op["kind"] == "create" and op["state"] == "open" and op["attempt"] == attempt_a for op in env.state()["operations"].values())
        and refused(try_release(principal, "run_a"), "refused", "is open")
    )
    gate.release()
    env.wait_ended(attempt_a, 30)
    principal_b = env.acquire_run("run_b", "training")
    attempt_b = env.submit(principal_b, params=env.fixture_params("b"))["attempt_id"]
    env.wait_phase(attempt_b, ("started",))
    stop_gate = env.daemon.add_gate("stop", "before")
    client.call("finalize", {}, principal_b)
    stop_gate.wait_reached()
    checks["refused while started with the fencing stop open"] = (
        env.phase(attempt_b) == "started"
        and any(op["kind"] == "stop" and op["state"] == "open" and op["attempt"] == attempt_b for op in env.state()["operations"].values())
        and refused(try_release(principal_b, "run_b"), "refused", "is open")
    )
    remove_gate = env.daemon.add_gate("remove", "before")
    stop_gate.release()
    remove_gate.wait_reached()
    checks["refused in exiting at remove with the operation open"] = (
        env.phase(attempt_b) == "exiting"
        and env.attempt(attempt_b)["cleanup"]["step"] == "remove"
        and any(op["kind"] == "remove" and op["state"] == "open" and op["attempt"] == attempt_b for op in env.state()["operations"].values())
        and refused(try_release(principal_b, "run_b"), "refused", "is open")
    )
    remove_gate.release()
    env.wait_ended(attempt_b, 30)
    principal_s = env.acquire_run("run_s", "acquisition")
    start_gate = env.daemon.add_gate("start", "before")
    attempt_s = env.submit(principal_s, launch="s1", params=env.fixture_params("s"))["attempt_id"]
    start_gate.wait_reached()
    client.call("finalize", {}, principal_s)
    checks["refused in starting with the start operation open"] = (
        env.phase(attempt_s) == "starting"
        and any(op["kind"] == "start" and op["state"] == "open" for op in env.state()["operations"].values())
        and refused(try_release(principal_s, "run_s"), "refused", "is open")
    )
    start_gate.release()
    env.wait_ended(attempt_s, 30)  # finalizing fenced the attempt: it ends through cleanup (a started attempt of a finalizing run is the stop case above)
    principal_c = env.acquire_run("run_c", "serving")
    client.call("register", {"kind": "catalog_user", "add": True}, principal_c)
    client.call("finalize", {}, principal_c)
    checks["refused with a registration"] = refused(try_release(principal_c, "run_c"), "refused", "registration")
    client.call("register", {"kind": "catalog_user", "add": False}, principal_c)
    checks["succeeds when nothing is open"] = client.call("release", {"record": str(record_file(env, "run_c"))}, principal_c) == {"state": "released"}
    checks["the released run's domain is free"] = env.acquire_run("run_d", "serving") is not None
    return checks


def v1_registry(env: Env, label: str, prepare: Callable[[run_lock.Registry], None]) -> run_lock.Registry:
    """A schema 1 registry in a folder of its own, prepared by the legacy tools, with the case's launcher settings."""
    folder = env.temp / f"legacy_{label}"
    folder.mkdir(mode=0o700)
    registry = run_lock.Registry(folder, "test")
    prepare(registry)
    (folder / "launcher_test.json").write_text(json.dumps(env.settings))
    return registry


@case("migration_gate")
def migration_gate(env: Env) -> dict[str, bool]:
    """910 and 915: each migration predicate refuses on its own and changes nothing; a drained registry migrates and keeps its runs."""
    results: dict[str, bool] = {}
    holder = os.getpid()

    def hold_run(reg: run_lock.Registry) -> None:
        run_lock.acquire(reg, "transform", "r1", "fixture", "manual", holder, [])

    def reservation(reg: run_lock.Registry) -> None:
        with run_lock.transaction(reg) as state:
            state["reservations"]["r-1"] = {"reservation_id": "r-1"}

    def reader(reg: run_lock.Registry) -> None:
        run_lock.reserve(reg, "reader1", "training", "pub", holder)

    def registration(reg: run_lock.Registry) -> None:
        hold_run(reg)
        with run_lock.transaction(reg) as state:
            run_lock.set_state(state, "r1", "released")
            state["runs"]["r1"]["registrations"] = ["catalog_user"]

    def legacy_container(reg: run_lock.Registry) -> None:
        with run_lock.transaction(reg):
            pass
        legacy = {
            "Id": "x" * 64,
            "Name": "/legacy-run",
            "Config": {"Image": "i", "Labels": {run_lock.CONTAINER_LABEL: "old"}, "Env": []},
            "State": {"Status": "exited", "Running": False, "ExitCode": 0, "Health": None},
            "HostConfig": {},
            "Mounts": [],
        }
        env.daemon.containers["x" * 64] = legacy

    blockers = {
        "a run that is not released": (hold_run, "run r1 is held"),
        "a reservation": (reservation, "reservation r-1"),
        "a reader": (reader, "reader reader1"),
        "a run registration": (registration, "registration catalog_user"),
        "a legacy container": (legacy_container, "still carries hai.run_id"),
    }
    for index, (label, (prepare, text)) in enumerate(blockers.items()):
        registry = v1_registry(env, str(index), prepare)
        before = registry.state_file.read_text()
        output = run_command(sys.executable, ["-m", "orchestration.launcher", "--test-registry", str(registry.path), "serve"], cwd=ROOT, timeout=60)
        results[f"{label} blocks the migration"] = output.returncode == 3 and text in output.stderr
        results[f"{label}: the registry is unchanged"] = registry.state_file.read_text() == before
        env.daemon.containers.pop("x" * 64, None)

    def drained(reg: run_lock.Registry) -> None:
        hold_run(reg)
        with run_lock.transaction(reg) as state:
            run_lock.set_state(state, "r1", "released")

    registry = run_lock.Registry(env.registry, "test")
    drained(registry)
    env.start()
    state = env.state()
    results["a drained registry migrates"] = state["schema_version"] == run_lock.LAUNCHER_SCHEMA
    results["the migrated registry keeps its runs"] = state["runs"]["r1"]["state"] == "released" and state["runs"]["r1"]["generation"] == 1
    results["the diagnostic owner is written"] = state["launcher"]["diagnostic"]["owner"] == launcher.DIAGNOSTIC_OWNER
    legacy_cli = run_command(
        sys.executable, ["-m", "scripts.lakehouse.run_lock", "--test-registry", str(env.registry), "mark", "--run", "r1", "--state", "finalizing"], cwd=ROOT
    )
    results["the legacy tool no longer changes a launcher registry"] = legacy_cli.returncode != 0
    return results


@case("submit_refused_states")
def submit_refused_states(env: Env) -> dict[str, bool]:
    """910: a submit is refused in finalizing, recovering, failed_retained and released."""
    hold_forever(env)
    env.start()
    client = env.client()
    checks: dict[str, bool] = {}

    def submit_error(principal: dict[str, Any], launch: str) -> BaseException | None:
        return expect_error(lambda: env.submit(principal, launch=launch, params=env.fixture_params(launch)))

    principal = env.acquire_run("run_a", "transform")
    client.call("finalize", {}, principal)
    checks["refused in finalizing"] = refused(submit_error(principal, "f"), "refused", "is finalizing")
    client.call("mark", {"state": "failed_retained"}, principal)
    checks["refused in failed_retained"] = refused(submit_error(principal, "g"), "refused", "is failed_retained")
    client.call("release", {"owner": True}, client.principal_run("run_a", owner=True))
    checks["refused in released"] = refused(submit_error(principal, "h"), "refused", "is released")
    principal_b = env.acquire_run("run_b", "training")
    attempt_b = env.submit(principal_b, params=env.fixture_params("b"))["attempt_id"]
    env.wait_phase(attempt_b, ("started",))
    stop_gate = env.daemon.add_gate("stop", "before")
    client.call("recover", {"owner": True}, client.principal_run("run_b", owner=True))
    stop_gate.wait_reached()
    checks["refused in recovering"] = refused(submit_error(principal_b, "r"), "refused", "is recovering")
    checks["the run is recovering while its stop is held"] = env.state()["runs"]["run_b"]["state"] == "recovering"
    stop_gate.release()
    env.wait(lambda: env.state()["runs"]["run_b"]["state"] == "failed_retained", 20, "failed_retained after recovery")
    checks["recovery ends in failed_retained with the container gone"] = env.labelled() == [] and env.attempt(attempt_b)["reason"] == "fenced"
    return checks


@case("probe_not_observed", timings={"probe_timeout": 1.5})
def probe_not_observed(env: Env) -> dict[str, bool]:
    """910: a probe event that does not arrive in time degrades the launcher, refuses work and starts no scan; the next probe recovers."""
    env.daemon.suppress_probe_events = True
    env.start(wait_serving=False)
    env.wait(lambda: env.ping().get("status") == "degraded", 15, "degraded")
    listings_while_degraded = len(env.daemon.calls_matching("GET", "/volumes"))
    allocate_error = expect_error(lambda: env.client().call("allocate", {}))
    time.sleep(1.0)
    still_no_scan = len(env.daemon.calls_matching("GET", "/volumes")) == listings_while_degraded
    env.daemon.suppress_probe_events = False
    env.wait(lambda: env.ping().get("status") == "serving", 20, "serving again")
    env.wait(lambda: env.labelled() == [], 15, "probe containers removed")
    return {
        "degraded when the probe is not acknowledged": listings_while_degraded == 0,
        "new work refused while degraded": refused(allocate_error, "degraded"),
        "no scan while degraded": still_no_scan,
        "serving after a probe is acknowledged": env.ping().get("status") == "serving",
        "a scan ran after the acknowledgment": len(env.daemon.calls_matching("GET", "/volumes")) > listings_while_degraded,
        "the failed probes were removed": env.labelled() == [],
    }


@case("first_start_drained")
def first_start_drained(env: Env) -> dict[str, bool]:
    """910: a first start on a drained registry writes the schema and the diagnostic owner, then probes, then scans, with no client owner."""
    env.start()
    state = env.state()
    probes = [a for a in state["attempts"].values() if a["owner"] == launcher.DIAGNOSTIC_OWNER]
    calls = [c["path"] for c in env.daemon.calls]
    return {
        "launcher schema written": state["schema_version"] == run_lock.LAUNCHER_SCHEMA,
        "diagnostic owner written": state["launcher"]["diagnostic"]["owner"] == launcher.DIAGNOSTIC_OWNER,
        "the probe is owned by the diagnostic owner and was never started": bool(probes) and not any(c["path"].endswith("/start") for c in env.daemon.calls),
        "events, probe create, then the scan": calls.index("/events") < calls.index("/containers/create") < calls.index("/containers/json"),
        "no run or allocation exists": not state["runs"] and not state["allocations"],
        "serving": env.ping()["status"] == "serving",
    }


@case("probe_left_removed")
def probe_left_removed(env: Env) -> dict[str, bool]:
    """910: a probe container left from an earlier start is removed by the orphan rule; a client cannot submit to the probe template."""
    registry_id = env.registry_id()
    left = env.daemon.create_rogue("hai-old-probe", {launcher.LABEL_REGISTRY: registry_id, launcher.LABEL_PROBE: "old", "hai.owner": launcher.DIAGNOSTIC_OWNER})
    env.start()
    env.wait(lambda: env.daemon.find(left) is None, 15, "the left probe is removed")
    alloc = env.allocate()
    error = expect_error(lambda: env.submit(env.alloc_principal(alloc), template="probe", params={}))
    return {
        "the left probe was removed": env.daemon.find(left) is None,
        "recorded as an orphan": "hai-old-probe" in orphan_names(env),
        "a client submit to the probe template is refused": refused(error, "refused", "cannot be submitted"),
    }


@case("headers_before_registration", timings={"probe_timeout": 1.5})
def headers_before_registration(env: Env) -> dict[str, bool]:
    """910: events response headers sent before the subscription registers lose a create in between; the probe orders the scan after it."""
    gate = Gate("subscribe", "before", None, True)
    env.daemon.subscribe_gate = gate
    registry_id = env.registry_id()
    env.start(wait_serving=False)
    gate.wait_reached()
    between = env.daemon.create_rogue("hai-between", {launcher.LABEL_REGISTRY: registry_id, "hai.owner": "run:ghost"})
    gate.release()
    env.wait(lambda: env.ping().get("status") == "serving", 30, "serving")
    env.wait(lambda: env.daemon.find(between) is None, 15, "the create made between headers and registration is found by the scan")
    return {
        "the create in the gap produced no event the launcher saw": True,
        "the first probe was not acknowledged and the launcher recovered": len(env.daemon.calls_matching("GET", "/events")) >= 2,
        "the scan after the probe found and removed the container": env.daemon.find(between) is None and "hai-between" in orphan_names(env),
    }


@case("recovery_cleanup_restart")
def recovery_cleanup_restart(env: Env) -> dict[str, bool]:
    """910: a recovery cleanup of an older-generation attempt completes while the run is recovering, also after a launcher restart."""
    hold_forever(env)
    env.start()
    client = env.client()
    principal = env.acquire_run("run_a", "transform")
    attempt_id = env.submit(principal)["attempt_id"]
    env.wait_phase(attempt_id, ("started",))
    stop_gate = env.daemon.add_gate("stop", "before")
    client.call("recover", {"owner": True}, client.principal_run("run_a", owner=True))
    stop_gate.wait_reached()
    state_during = env.state()["runs"]["run_a"]
    env.kill_launcher()
    stop_gate.release()
    env.start()
    ended = env.wait_ended(attempt_id, 30)
    env.wait(lambda: env.state()["runs"]["run_a"]["state"] == "failed_retained", 20, "failed_retained")
    return {
        "the run was recovering with a new generation": state_during["state"] == "recovering" and state_during["generation"] == 2,
        "the older-generation attempt ended fenced after the restart": ended["reason"] == "fenced" and ended["generation"] == 1,
        "the container is gone": env.labelled() == [],
        "the run is failed_retained": env.state()["runs"]["run_a"]["state"] == "failed_retained",
    }


@case("continuous_events_during_scan")
def continuous_events_during_scan(env: Env) -> dict[str, bool]:
    """910: containers created while the launcher starts, before and after its subscription, are all disposed of."""
    registry_id = env.registry_id()
    created: list[str] = []
    stop = threading.Event()

    def churn() -> None:
        for index in range(20):
            if stop.is_set():
                return
            created.append(
                env.daemon.create_rogue(f"hai-rogue-{index}", {launcher.LABEL_REGISTRY: registry_id, "hai.owner": "run:ghost"}, running=index % 2 == 0)
            )
            time.sleep(0.08)

    thread = threading.Thread(target=churn)
    thread.start()
    env.start()
    thread.join()
    env.wait(lambda: env.labelled() == [], 20, "all rogue containers are removed")
    return {
        "twenty containers were created": len(created) == 20,
        "all were removed": env.labelled() == [],
        "all were recorded as orphans": len([n for n in orphan_names(env) if n.startswith("hai-rogue-")]) == 20,
    }


@case("orphan_disposed_once")
def orphan_disposed_once(env: Env) -> dict[str, bool]:
    """910 (PR #3 CI): an orphan whose removal is slow is disposed of and recorded once, however many events it raises meanwhile."""
    env.start()
    remove_gate = env.daemon.add_gate("remove", "before", match=lambda ctx: True)
    container = env.daemon.create_rogue("hai-rogue-slow", {launcher.LABEL_REGISTRY: env.registry_id(), "hai.owner": "run:ghost"}, running=True)
    remove_gate.wait_reached()
    # The stop raised die and stop events while the removal waits; give the launcher time to inspect the container again.
    time.sleep(1.5)
    remove_gate.release()
    env.wait(lambda: env.labelled() == [], 20, "the orphan is removed")
    operations = [op for op in env.state()["operations"].values() if op["kind"] == "orphan" and op.get("container_id") == container]
    return {
        "the orphan is removed": env.labelled() == [],
        "one orphan record": [n for n in orphan_names(env) if n == "hai-rogue-slow"] == ["hai-rogue-slow"],
        "one orphan operation": len(operations) == 1,
    }


@case("orphan_late_inspect_once")
def orphan_late_inspect_once(env: Env) -> dict[str, bool]:
    """910 (main CI after PR #5): an inspect reply read before an orphan's removal and delivered after it settled does not dispose of it again."""
    env.start()
    captured_after_stop = False

    def after_stop(ctx: dict[str, Any]) -> bool:
        nonlocal captured_after_stop
        if ctx["name"] != "hai-rogue-late":
            return False
        found = env.daemon.find(ctx["name"])
        captured_after_stop = found is not None and not bool(found["State"]["Running"])
        return captured_after_stop

    remove_gate = env.daemon.add_gate("remove", "before", match=lambda ctx: True)
    late_gate = env.daemon.add_gate("inspect", "after", match=after_stop)
    container = env.daemon.create_rogue("hai-rogue-late", {launcher.LABEL_REGISTRY: env.registry_id(), "hai.owner": "run:ghost"}, running=True)
    if not (remove_gate.wait_reached() and late_gate.wait_reached()):
        raise CaseFailure("the removal or the late inspect was not reached")
    remove_gate.release()

    def operations() -> list[dict[str, Any]]:
        return [op for op in env.state()["operations"].values() if op["kind"] == "orphan" and op.get("container_id") == container]

    env.wait(lambda: env.labelled() == [] and [op["state"] for op in operations()] == ["settled"], 20, "the orphan is removed and its operation settled")
    late_gate.release()
    time.sleep(1.5)
    return {
        "the late inspect was captured after the container stopped": captured_after_stop,
        "the orphan is removed": env.labelled() == [],
        "one orphan record": [n for n in orphan_names(env) if n == "hai-rogue-late"] == ["hai-rogue-late"],
        "one orphan operation": len(operations()) == 1,
    }


@case("scan_reply_after_stream_loss", timings={"backoff_start": 2.0, "backoff_cap": 3})
def scan_reply_after_stream_loss(env: Env) -> dict[str, bool]:
    """910 (F7): the stream drops after the probe is acknowledged and while the scan is in flight; the late scan reply does not restore serving."""
    gate = env.daemon.add_gate("list", "after", match=lambda ctx: any(label.startswith("hai.registry_id=") for label in ctx["labels"]))
    env.start(wait_serving=False)
    gate.wait_reached()
    env.daemon.drop_events()
    env.wait(lambda: env.ping().get("status") == "degraded", 10, "the launcher degrades after the stream drop")
    gate.release()
    time.sleep(0.5)
    during = env.ping().get("status")
    env.wait(lambda: env.ping().get("status") == "serving", 20, "the launcher serves again after a new subscription")
    # The new subscription's probe container is removed after serving resumes; a slow machine can still hold it here.
    with contextlib.suppress(CaseFailure):
        env.wait(lambda: env.labelled() == [], 10, "the second probe container is removed")
    state = env.state()
    return {
        "the late scan reply left the launcher degraded until the reconnect": during == "degraded",
        "serving resumed only after a second stream epoch": int(state["launcher"]["stream_epoch"]) >= 2,
        "no container left": env.labelled() == [],
    }


@case("shared_allocation_two_workers")
def shared_allocation_two_workers(env: Env) -> dict[str, bool]:
    """911: one attempt ending on a shared allocation leaves the other running; release is refused while any attempt is open."""
    hold_forever(env)
    env.start()
    client = env.client()
    alloc = env.allocate(workers=2)
    principal = env.alloc_principal(alloc)
    first = env.submit(principal, launch="l1", params=env.fixture_params("a"))["attempt_id"]
    second = env.submit(principal, launch="l2", params=env.fixture_params("b"))["attempt_id"]
    third = env.submit(principal, launch="l3", params=env.fixture_params("c"))["attempt_id"]
    env.wait_phase(first, ("started",))
    env.wait_phase(second, ("started",))
    checks = {"the third attempt waits for a worker": env.phase(third) == "queued"}
    client.call("cancel", {"attempt_id": third}, principal)
    env.wait_ended(third)
    client.call("cancel", {"attempt_id": first}, principal)
    env.wait_ended(first)
    checks["the other attempt keeps running"] = env.phase(second) == "started" and len(env.labelled()) >= 1
    checks["release refused while attempts are open"] = refused(expect_error(lambda: client.call("release_allocation", {}, principal)), "refused", "is open")
    client.call("cancel", {"attempt_id": second}, principal)
    env.wait_ended(second)
    checks["the queued attempt ended cancelled without a container"] = env.attempt(third)["reason"] == "cancelled" and not creates(env, "-l3-")
    checks["release succeeds when every attempt ended"] = client.call("release_allocation", {}, principal) == {"state": "released"}
    return checks


@case("heavy_allocation_exclusion")
def heavy_allocation_exclusion(env: Env) -> dict[str, bool]:
    """911 and 759: one heavy allocation holds the machine at a time; a reading below the floor is refused and writes nothing."""
    env.start()
    client = env.client()
    alloc = env.allocate()
    second = expect_error(lambda: client.call("allocate", {}))
    client.call("release_allocation", {}, env.alloc_principal(alloc))
    env.daemon.mem_total = 10 * 1024**3
    low = expect_error(lambda: env.allocate())
    failed = [a for a in env.state()["allocations"].values() if a["state"] == "failed"]
    env.daemon.mem_total = 32 * 1024**3
    third = env.allocate()
    return {
        "second allocation refused while the first is held": refused(second, "refused", "holds the machine"),
        "a reading below the floor is refused": refused(low, "budget", "GiB free"),
        "the failed allocation holds nothing": len(failed) == 1,
        "the machine is free again": third is not None,
    }


@case("holder_death")
def holder_death(env: Env) -> dict[str, bool]:
    """911: when the holder process dies its attempts are cancelled by policy, cleaned up and the allocation is released."""
    hold_forever(env)
    env.start()
    case_folder, packages = env.case_folders("h")
    command = [
        "-m",
        "orchestration.launcher",
        "--test-registry",
        str(env.registry),
        "run",
        "--template",
        "fixture_dbt",
        "--launch-id",
        "hold",
        "--param",
        f"case_host={json.dumps(str(case_folder))}",
        "--param",
        'case_container="/workspace/out/h"',
        "--param",
        f"packages_host={json.dumps(str(packages))}",
        "build",
    ]
    result: list[Any] = []
    thread = threading.Thread(target=lambda: result.append(run_command(sys.executable, command, cwd=ROOT, timeout=120)))
    thread.start()
    env.threads.append(thread)
    allocation = env.wait(
        lambda: next((a for a in env.state()["allocations"].values() if a["kind"] == "shared" and a["holder_pid"] != os.getpid()), None),
        20,
        "the CLI's allocation",
    )
    attempt_id = env.wait(lambda: next((i for i, a in env.state()["attempts"].items() if a["owner"] == f"alloc:{allocation['id']}"), None), 20, "its attempt")
    env.wait_phase(attempt_id, ("started",))
    os.kill(int(allocation["holder_pid"]), signal.SIGKILL)
    ended = env.wait_ended(attempt_id, 30)
    env.wait(lambda: env.state()["allocations"][allocation["id"]]["state"] == "released", 20, "the allocation is released")
    return {
        "the attempt was cancelled by holder policy": ended["reason"] == "cancelled" and ended["cancel_intent"]["reason"] == "holder_dead",
        "the container is gone": env.labelled() == [],
        "the allocation is released": env.state()["allocations"][allocation["id"]]["state"] == "released",
    }


@case("reader_holder_death")
def reader_holder_death(env: Env) -> dict[str, bool]:
    """911 (F3): a UI holder killed abruptly; its reader registration stays while the container is unresolved and ends with it."""
    catalog_script(env)
    env.start()
    catalog_up(env)
    code = f"""
import os, time
client = launcher.LauncherClient(launcher.resolve_config({str(env.registry)!r}))
alloc = client.allocate()
principal = launcher.LauncherClient.principal_alloc(alloc)
registration = client.call("register_reader", {{}}, principal)["registration_id"]
attempt = client.call("submit", {{"template": "ui_reader", "launch_id": "u", "args": [], "params": {{}}, "checkout": {str(ROOT)!r}}}, principal)["attempt_id"]
open(sys.argv[1], "w").write(json.dumps({{"pid": os.getpid(), "alloc": alloc, "registration": registration, "attempt": attempt}}))
time.sleep(600)
"""
    info = detached_python(env, code)
    env.wait_phase(info["attempt"], ("started",))
    remove_gate = env.daemon.add_gate("remove", "before")
    os.kill(int(info["pid"]), signal.SIGKILL)
    remove_gate.wait_reached(30)
    env.kill_launcher()
    env.daemon.fail_remove = 10**6  # every removal request of the restarted launcher fails while the first one is still held
    env.start()
    time.sleep(1.0)
    during = {
        "the registration is kept while the container is unresolved": info["registration"] in env.state()["registrations"]
        and env.state()["allocations"][info["alloc"]]["state"] == "held",
        "catalog_down is refused while the dead reader's attempt is open": refused(expect_error(lambda: env.client().call("catalog_down")), "refused"),
    }
    env.daemon.fail_remove = 0
    remove_gate.release()
    ended = env.wait_ended(info["attempt"], 40)
    env.wait(lambda: env.state()["allocations"][info["alloc"]]["state"] == "released", 30, "the allocation is released")
    env.client().call("catalog_down")
    env.wait(lambda: catalog_phase(env) == "down" and not env.daemon.networks, 40, "catalog_down succeeds after the reader is gone")
    return {
        **during,
        "the attempt ended cancelled by holder policy": ended["reason"] == "cancelled" and ended["cancel_intent"]["reason"] == "holder_dead",
        "the registration ended with the container": info["registration"] not in env.state()["registrations"],
        "the allocation was released and the catalog stopped": env.state()["allocations"][info["alloc"]]["state"] == "released" and not env.labelled(),
    }


LONG_AGO = "2000-01-01T00:00:00+00:00"


def age_records(env: Env, *, attempts: Iterable[str] = (), allocations: Iterable[str] = (), requests: Iterable[str] = (), at: str = LONG_AGO) -> None:
    """Make the named records old enough to move: the case, not the clock, decides which record expires (failure mode HT1)."""
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        for attempt_id in attempts:
            state["attempts"][attempt_id]["ended_at"] = at
        for allocation_id in allocations:
            state["allocations"][allocation_id]["created_at"] = at
        for key in requests:
            state["requests"][key]["at"] = at


def seconds_ago(seconds: float) -> str:
    return (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()


def outcome_of(function: Callable[[], Any]) -> Any:
    """The value a call returns, or the launcher or socket error it raises."""
    try:
        return function()
    except (OSError, launcher.ClientError) as error:
        return error


class Hook:
    """What the in-process launcher does to each request before it handles it: a delay, or a block until released."""

    def __init__(self) -> None:
        self.delay = 0.0
        self.slow_first = 0.0
        self.blocked: frozenset[str] = frozenset()
        self.release = threading.Event()
        self.seen: Counter[str] = Counter()
        self.calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []


@contextlib.contextmanager
def served_here(env: Env) -> Iterator[Hook]:
    """A launcher that runs in a thread of this process on the case's registry, so a case can slow or block its request handling."""
    instance = launcher.Launcher(env.config)
    hook = Hook()
    original = instance.dispatch

    def hooked(conn: launcher.Conn, request: dict[str, Any], request_id: str, method: str) -> Any:
        hook.seen[method] += 1
        hook.calls.append((method, dict(request.get("principal") or {}), dict(request.get("params") or {})))
        if method in hook.blocked:
            hook.release.wait(60)
        elif hook.slow_first:
            hook.slow_first, pause = 0.0, hook.slow_first
            time.sleep(pause)
        elif hook.delay:
            time.sleep(hook.delay)
        return original(conn, request, request_id, method)

    thread = threading.Thread(target=instance.serve, name="in-process-launcher")
    with mock.patch.object(instance, "dispatch", hooked):
        thread.start()
        try:
            env.wait(lambda: env.ping().get("status") == "serving", 30, "the in-process launcher serves")
            yield hook
        finally:
            hook.delay, hook.blocked = 0.0, frozenset()
            hook.release.set()
            with contextlib.suppress(launcher.ClientError, OSError):
                env.client(start=False).call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
            thread.join(20)


def archived_records(env: Env) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for path in sorted((env.registry / "history").glob("*.jsonl")):
        for row in path.read_bytes().split(b"\n"):
            with contextlib.suppress(ValueError):
                found.append(dict(json.loads(row)))
    return found


def compaction_barrier(env: Env, client: launcher.LauncherClient, principal: dict[str, Any], attempt_id: str) -> None:
    """Wait for a compaction that ran after this call began: an aged marker request is archived only by a pass that starts after the marker exists."""
    before = set(env.state()["requests"])
    expect_error(lambda: client.call("cancel", {"attempt_id": attempt_id}, principal))
    marker = sorted(set(env.state()["requests"]) - before)
    age_records(env, requests=marker)
    env.wait(lambda: not set(marker) & set(env.state()["requests"]), 30, "a compaction archives the marker request")


@contextlib.contextmanager
def compactor(env: Env) -> Iterator[launcher.Launcher]:
    """A launcher object on the case's registry that does not serve: its compaction and selection run in this process."""
    instance = launcher.Launcher(env.config)
    instance.registry_id = env.registry_id()
    try:
        yield instance
    finally:
        instance.selector.close()
        instance.pool.shutdown(wait=True)
        os.close(instance.wake_read)
        os.close(instance.wake_write)


@case("registry_history_bounded", timings={"history_retain": 3600.0, "compact_every": 0.3, "poll_patience": 1.0})
def registry_history_bounded(env: Env) -> dict[str, bool]:
    """A116 item 2 (failure modes RH1 to RH10, RH20, HT1): terminal records older than the retention move out of the registry into an owner-only
    append-only archive beside it, while running attempts, open operations, held allocations, recent requests and every allocation that a staying
    attempt, registration, reader or reservation names stay; a retention not longer than the client's patience is refused. The case ages the
    records it wants to expire by a registry write and observes the compaction by a marker record: no deadline of the clock decides a result."""
    bad = {**env.settings, "timings": {**env.settings["timings"], "history_retain": 1.0}}
    (env.registry / "launcher_test.json").write_text(json.dumps(bad, indent=2))
    short_retention = expect_error_of(lambda: launcher.resolve_config(str(env.registry)))
    env.write_settings()
    env.start()
    client = env.client()
    released = env.allocate()
    ended: list[str] = []
    for index in range(3):
        attempt_id = env.submit(env.alloc_principal(released), launch=f"old{index}", params=env.fixture_params(f"old{index}"))["attempt_id"]
        env.wait_ended(attempt_id)
        ended.append(attempt_id)
    client.release_allocation(released)
    run_principal = env.acquire_run("history_run")
    run_attempt = env.submit(run_principal, launch="run1", params=env.fixture_params("run1"))["attempt_id"]
    per_attempt = env.wait_ended(run_attempt)["attempt_alloc"]
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    kept = env.submit(principal, launch="kept", params=env.fixture_params("kept"))["attempt_id"]
    env.wait_ended(kept)
    before = env.state()
    old_operations = {key for key, op in before["operations"].items() if op["attempt"] in ended}
    old_containers = {key for key, record in before["containers"].items() if record.get("attempt") in ended}
    old_requests = set(before["requests"])
    env.daemon.script = lambda container: None
    live = env.submit(principal, launch="live", params=env.fixture_params("live"))["attempt_id"]
    env.wait_phase(live, ("started",))
    age_records(env, attempts=[*ended, kept, run_attempt], allocations=[released, alloc, per_attempt], requests=old_requests)

    def moved() -> dict[str, Any] | None:
        current = env.state()
        gone = all(a not in current["attempts"] for a in ended) and not old_requests & set(current["requests"])
        return current if gone else None

    state = env.wait(moved, 30, "the ended attempts and the old requests leave the registry")
    before_fresh = set(env.state()["requests"])
    expect_error(lambda: client.call("cancel", {"attempt_id": kept}, principal))
    fresh = set(env.state()["requests"]) - before_fresh
    compaction_barrier(env, client, principal, kept)
    fresh_kept = bool(fresh) and fresh <= set(env.state()["requests"])
    after_run = env.state()
    run_attempt_stays = run_attempt in after_run["attempts"] and after_run["attempts"][run_attempt]["attempt_alloc"] == per_attempt
    per_attempt_stays = per_attempt in after_run["allocations"]
    archive_files = sorted((env.registry / "history").glob("*.jsonl"))
    text = "".join(path.read_text() for path in archive_files)
    archived = {(line["collection"], line["key"]) for line in archived_records(env)}
    restored = {(line["collection"], line["key"]): line["record"] for line in archived_records(env)}
    owned = [key for (collection, key) in restored if collection == "attempts" and restored[(collection, key)]["owner"] == f"alloc:{released}"]
    references: dict[str, bool] = {}
    with compactor(env) as instance:

        def selected(change: Callable[[dict[str, Any]], None]) -> set[tuple[str, str]]:
            copy = json.loads(json.dumps(after_run))
            copy["allocations"][released] = restored[("allocations", released)]
            for attempt_id in owned:
                copy["attempts"][attempt_id] = restored[("attempts", attempt_id)]
            change(copy)
            return {(collection, key) for collection, key, _ in instance.history_to_move(copy)}

        def staying_attempt(copy: dict[str, Any]) -> None:
            copy["attempts"][run_attempt]["attempt_alloc"] = released

        def registration(copy: dict[str, Any]) -> None:
            copy["registrations"]["r1"] = {"id": "r1", "kind": "reader", "allocation": released, "at": LONG_AGO}

        def reader(copy: dict[str, Any]) -> None:
            copy["readers"]["reader_x"] = {"state": "registered", "allocation": released}

        def reservation(copy: dict[str, Any]) -> None:
            copy["reservations"]["reserve_x"] = {"allocation": released}

        free = selected(lambda copy: None)
        references["control: an allocation nothing names moves"] = ("allocations", released) in free and all(("attempts", a) in free for a in owned)
        for label, change in (
            ("a staying attempt (attempt_alloc)", staying_attempt),
            ("a registration", registration),
            ("a reader", reader),
            ("a reservation", reservation),
        ):
            names = selected(change)
            references[f"{label} keeps the allocation and the attempts it owns"] = ("allocations", released) not in names and not any(
                ("attempts", a) in names for a in owned
            )
    client.call("mark", {"state": "failed_retained"}, run_principal)
    client.call("release", {"owner": True}, client.principal_run("history_run", owner=True))
    return {
        "a run's per-attempt allocation stays while its attempt stays": run_attempt_stays and per_attempt_stays,
        **{f"incoming reference: {label}": ok for label, ok in references.items()},
        "a retention not longer than the client's patience is refused": isinstance(short_retention, launcher.LauncherError),
        "every ended attempt moved to the archive": all(("attempts", a) in archived for a in ended),
        "their operations moved to the archive": bool(old_operations)
        and all(op not in state["operations"] and ("operations", op) in archived for op in old_operations),
        "their container records moved to the archive": bool(old_containers)
        and all(c not in state["containers"] and ("containers", c) in archived for c in old_containers),
        "the old requests moved to the archive": bool(old_requests) and all(r not in state["requests"] and ("requests", r) in archived for r in old_requests),
        "the released allocation moved with its attempts": released not in state["allocations"] and ("allocations", released) in archived,
        "the running attempt stays": live in state["attempts"] and state["attempts"][live]["phase"] != "ended",
        "an ended attempt of a held allocation stays (its owner may still reuse the attempt number)": kept in state["attempts"],
        "the held allocation stays": state["allocations"][alloc]["state"] == "held",
        "a request younger than the retention stays through later compactions": fresh_kept,
        "the archive files are owner-only": bool(archive_files) and all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in archive_files),
        "no compose secret value is in the archive": all(value not in text for value in env.secret_values.values() if len(value) > 8),
        "the launcher still serves": client.call("ping")["status"] == "serving",
    }


@case("registry_archive_faults")
def registry_archive_faults(env: Env) -> dict[str, bool]:
    """RH11 to RH19: a compaction that archives records to disk loses none when a write is short, returns zero bytes, is interrupted, fails after a
    partial append or reports bytes that are not on disk, when the launcher restarts after a crash that left an incomplete last line, when the
    history folder is created for the first time or when the archive path is a link. Isolation is needed because the faults are in the write calls;
    the records come from a real launcher run and the compaction runs in this process on a copy of that registry."""
    env.start()
    client = env.client()
    allocation = env.allocate()
    for index in range(3):
        env.wait_ended(env.submit(env.alloc_principal(allocation), launch=f"f{index}", params=env.fixture_params(f"f{index}"))["attempt_id"])
    client.release_allocation(allocation)
    env.stop_launcher()
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        for attempt in state["attempts"].values():
            attempt["ended_at"] = LONG_AGO
        for record in state["allocations"].values():
            record["created_at"] = LONG_AGO
        for request in state["requests"].values():
            request["at"] = LONG_AGO
    baseline = (env.registry / "registry.json").read_bytes()
    history = env.registry / "history"
    real_write, real_fsync, real_replace = os.write, os.fsync, os.replace

    def mine(data: Any) -> bool:
        return bytes(data[:14]) == b'{"archived_at"'

    def short(fd: int, data: Any) -> int:
        return real_write(fd, data[: max(1, len(data) // 2)]) if mine(data) else real_write(fd, data)

    def zero(fd: int, data: Any) -> int:
        return 0 if mine(data) else real_write(fd, data)

    interrupted_once = [False]

    def interrupted(fd: int, data: Any) -> int:
        if mine(data) and not interrupted_once[0]:
            interrupted_once[0] = True
            raise InterruptedError(errno.EINTR, "synthetic interrupted write")
        return real_write(fd, data)

    def partial_then_error(fd: int, data: Any) -> int:
        if mine(data):
            real_write(fd, data[:32])
            raise OSError(errno.ENOSPC, "synthetic disk full after a partial append")
        return real_write(fd, data)

    def corrupted(fd: int, data: Any) -> int:
        return real_write(fd, b"X" * len(data)) if mine(data) else real_write(fd, data)

    def reset() -> None:
        shutil.rmtree(history, ignore_errors=True)
        history.unlink(missing_ok=True)
        (env.registry / "registry.json").write_bytes(baseline)

    def compact(*patches: Any) -> BaseException | None:
        with compactor(env) as instance, contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            try:
                instance.compact_history()
            except OSError as error:
                return error
        return None

    with compactor(env) as instance:
        eligible = [(collection, key) for collection, key, _ in instance.history_to_move(env.state()) if collection != "reservation_history"]

    def tally() -> dict[str, Any]:
        """What the archive and the registry hold for the eligible keys: archived counts, malformed lines, records still in the registry, records lost."""
        lines = [chunk for path in sorted(history.glob("*.jsonl")) for chunk in path.read_bytes().split(b"\n") if chunk]
        counts: Counter[tuple[str, str]] = Counter()
        malformed = 0
        for chunk in lines:
            try:
                row = json.loads(chunk)
                counts[(row["collection"], row["key"])] += 1
            except (ValueError, KeyError, TypeError):
                malformed += 1
        state = env.state()
        kept = sum(key in state[collection] for collection, key in eligible)
        lost = sum(key not in state[collection] and counts[(collection, key)] == 0 for collection, key in eligible)
        return {"counts": counts, "malformed": malformed, "kept": kept, "lost": lost}

    def complete(result: dict[str, Any]) -> bool:
        return result["lost"] == 0 and result["kept"] == 0 and result["malformed"] == 0 and all(result["counts"][key] >= 1 for key in eligible)

    checks: dict[str, bool] = {"the fixture holds records to archive": len(eligible) >= 6}
    reset()
    checks["control: a full write archives every record once"] = (
        compact() is None and complete(tally()) and all(count == 1 for count in tally()["counts"].values())
    )
    checks["the archive is owner-only"] = stat.S_IMODE(next(history.glob("*.jsonl")).stat().st_mode) == 0o600
    reset()
    error = compact(mock.patch.object(launcher.os, "write", short))
    checks["RH11: short writes are completed and every record is archived"] = error is None and complete(tally())
    reset()
    error = compact(mock.patch.object(launcher.os, "write", zero))
    result = tally()
    checks["RH12: a write that returns zero bytes fails, keeps every record and leaves no partial line"] = (
        isinstance(error, OSError) and result["kept"] == len(eligible) and result["lost"] == 0 and result["malformed"] == 0
    )
    reset()
    error = compact(mock.patch.object(launcher.os, "write", interrupted))
    checks["RH13: an interrupted write is retried"] = error is None and complete(tally())
    reset()
    error = compact(mock.patch.object(launcher.os, "write", partial_then_error))
    failed = tally()
    checks["RH14: a failure after a partial append keeps every record and cuts the partial line back"] = (
        isinstance(error, OSError) and failed["kept"] == len(eligible) and failed["lost"] == 0 and failed["malformed"] == 0
    )
    retry_error = compact()
    retried = tally()
    checks["RH14: the retry archives every record once, with no malformed line"] = (
        retry_error is None and complete(retried) and all(count == 1 for count in retried["counts"].values())
    )
    before_restart = [path.read_bytes() for path in sorted(history.glob("*.jsonl"))]
    restart_error = compact()
    checks["RH19: a restart after the retry changes nothing"] = restart_error is None and before_restart == [
        path.read_bytes() for path in sorted(history.glob("*.jsonl"))
    ]
    reset()
    history.mkdir(mode=0o700)
    day = history / f"{datetime.now(UTC):%Y%m%d}.jsonl"
    previous = json.dumps({"archived_at": LONG_AGO, "collection": "attempts", "key": "previous", "record": {}}, sort_keys=True).encode() + b"\n"
    day.write_bytes(previous + b'{"archived_at": "2000-01-01T00:00:00+00:00", "collection": "attempts", "key": "tor')
    day.chmod(0o600)
    error = compact()
    after_crash = tally()
    checks["RH15: an incomplete last line from a crash does not corrupt the next append"] = error is None and complete(after_crash)
    checks["RH15: the complete lines before the incomplete one stay"] = after_crash["counts"][("attempts", "previous")] == 1
    reset()
    error = compact(mock.patch.object(launcher.os, "write", corrupted))
    result = tally()
    checks["RH16: bytes that differ on disk fail the verification and keep every record"] = isinstance(error, OSError) and result["kept"] == len(eligible)
    reset()
    registry_inode = env.registry.stat().st_ino
    events: list[tuple[str, str]] = []

    def kind_of(fd: int) -> str:
        info = os.fstat(fd)
        if stat.S_ISDIR(info.st_mode):
            return "registry folder" if info.st_ino == registry_inode else "history folder"
        return "file"

    def logged_fsync(fd: int) -> None:
        events.append(("fsync", kind_of(fd)))
        real_fsync(fd)

    def logged_replace(source: Any, destination: Any, *arguments: Any, **keywords: Any) -> None:
        events.append(("replace", Path(destination).name))
        real_replace(source, destination, *arguments, **keywords)

    error = compact(mock.patch.object(launcher.os, "fsync", logged_fsync), mock.patch.object(launcher.os, "replace", logged_replace))
    registry_replace = events.index(("replace", "registry.json")) if ("replace", "registry.json") in events else len(events)
    checks["RH17: the registry folder is synced after the history folder is created and before the registry is replaced"] = (
        error is None and ("fsync", "registry folder") in events[:registry_replace]
    )
    reset()

    def registry_folder_fails(fd: int) -> None:
        if kind_of(fd) == "registry folder" and ("replace", "registry.json") not in events:
            raise OSError(errno.EIO, "synthetic failure to sync the registry folder")
        real_fsync(fd)

    events.clear()
    error = compact(mock.patch.object(launcher.os, "fsync", registry_folder_fails), mock.patch.object(launcher.os, "replace", logged_replace))
    result = tally()
    checks["RH17: a failed sync of the registry folder keeps every record"] = isinstance(error, OSError) and result["kept"] == len(eligible)
    reset()
    victim = env.temp / "victim.jsonl"
    victim.write_bytes(b"keep\n")
    history.mkdir(mode=0o700)
    (history / f"{datetime.now(UTC):%Y%m%d}.jsonl").symlink_to(victim)
    error = compact()
    checks["RH18: a link in place of the day file is refused and its target is untouched"] = (
        isinstance(error, OSError) and victim.read_bytes() == b"keep\n" and tally()["kept"] == len(eligible)
    )
    reset()
    elsewhere = env.temp / "elsewhere"
    elsewhere.mkdir()
    history.symlink_to(elsewhere)
    error = compact()
    checks["RH18: a link in place of the history folder is refused and its target stays empty"] = (
        isinstance(error, OSError) and not any(elsewhere.iterdir()) and tally()["kept"] == len(eligible)
    )
    return checks


THRESHOLD_TIMINGS = {"history_retain": 3600.0, "compact_every": 0.3, "poll_patience": 1.0, "compact_floor": 120.0}


def archive_pairs(env: Env) -> set[tuple[str, str]]:
    return {(line["collection"], line["key"]) for line in archived_records(env)}


def registry_bytes(env: Env) -> int:
    return (env.registry / "registry.json").stat().st_size


@case("registry_threshold_compaction", timings={**THRESHOLD_TIMINGS, "compact_threshold": 32_000})
def registry_threshold_compaction(env: Env) -> dict[str, bool]:
    """A160 (failure modes TH1 to TH5, TH9 to TH11): a registry above the size threshold moves its finished records older than the age floor
    (not the 24-hour retention) into the archive, and nothing younger than the floor; a floor not above the client's patience or a threshold
    that is not positive is refused at start. The old records are made old by a registry write, so no clock decides a result."""
    refusals: dict[str, BaseException | None] = {}
    for label, change in (
        ("a floor equal to the client's patience", {"compact_floor": 1.0}),
        ("a floor below the client's patience", {"compact_floor": 0.5}),
        ("a threshold of zero", {"compact_threshold": 0}),
        ("a negative floor", {"compact_floor": -5.0}),
    ):
        (env.registry / "launcher_test.json").write_text(json.dumps({**env.settings, "timings": {**env.settings["timings"], **change}}))
        refusals[label] = expect_error_of(lambda: launcher.resolve_config(str(env.registry)))
    env.write_settings()
    threshold = int(env.settings["timings"]["compact_threshold"])
    env.start()
    client = env.client()
    released = env.allocate()
    ended: list[str] = []
    for index in range(3):
        attempt_id = env.submit(env.alloc_principal(released), launch=f"old{index}", params=env.fixture_params(f"old{index}"))["attempt_id"]
        env.wait_ended(attempt_id)
        ended.append(attempt_id)
    client.release_allocation(released)
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    kept = env.submit(principal, launch="kept", params=env.fixture_params("kept"))["attempt_id"]
    env.wait_ended(kept)
    before = env.state()
    old_operations = {key for key, op in before["operations"].items() if op["attempt"] in ended}
    old_containers = {key for key, record in before["containers"].items() if record.get("attempt") in ended}
    old_requests = set(before["requests"])
    env.daemon.script = lambda container: None
    live = env.submit(principal, launch="live", params=env.fixture_params("live"))["attempt_id"]
    env.wait_phase(live, ("started",))
    size_before = registry_bytes(env)
    time.sleep(2.0)
    fresh_only = env.state()
    nothing_moved_while_fresh = not archived_records(env) and set(ended) <= set(fresh_only["attempts"])
    age_records(env, attempts=[*ended, kept], allocations=[released, alloc], requests=old_requests, at=seconds_ago(600))
    aged_alloc = env.state()["allocations"][alloc]

    def moved() -> dict[str, Any] | None:
        current = env.state()
        gone = all(a not in current["attempts"] for a in ended) and not old_requests & set(current["requests"])
        return current if gone else None

    try:
        state = env.wait(moved, 20, "the records older than the floor leave the registry")
    except CaseFailure:
        state = env.state()
    archived = archive_pairs(env)
    before_fresh = set(env.state()["requests"])
    expect_error(lambda: client.call("cancel", {"attempt_id": kept}, principal))
    fresh = set(env.state()["requests"]) - before_fresh
    compaction_barrier(env, client, principal, kept)
    final = env.state()
    return {
        **{f"refused at start: {label}": isinstance(error, launcher.LauncherError) and error.code == "config" for label, error in refusals.items()},
        "the registry is over the threshold before the records age": size_before > threshold,
        "over the threshold with only fresh records, nothing moves": nothing_moved_while_fresh,
        "every old attempt moved to the archive": all(("attempts", a) in archived for a in ended),
        "their operations, containers and requests moved with them": bool(old_operations and old_containers and old_requests)
        and all(("operations", key) in archived for key in old_operations)
        and all(("containers", key) in archived for key in old_containers)
        and all(("requests", key) in archived for key in old_requests),
        "the released allocation moved with its attempts": released not in state["allocations"] and ("allocations", released) in archived,
        "the file is under the threshold after the move": registry_bytes(env) < threshold,
        "the held allocation stays": final["allocations"][alloc]["state"] == "held" and aged_alloc["state"] == "held",
        "the ended attempt of the held allocation stays": kept in final["attempts"],
        "the running attempt stays": live in final["attempts"] and final["attempts"][live]["phase"] != "ended",
        "a request younger than the floor stays": bool(fresh) and fresh <= set(final["requests"]),
        "the launcher still serves": client.call("ping")["status"] == "serving",
    }


@case("registry_threshold_control", timings=THRESHOLD_TIMINGS)
def registry_threshold_control(env: Env) -> dict[str, bool]:
    """A160 control (failure mode TH3): below the size threshold the periodic pass keeps the 24-hour behavior. A record older than the retention
    moves; a record older than the age floor and younger than the retention stays."""
    env.start()
    client = env.client()
    older = env.allocate()
    old_attempt = env.submit(env.alloc_principal(older), launch="older", params=env.fixture_params("older"))["attempt_id"]
    env.wait_ended(old_attempt)
    client.release_allocation(older)
    middle = env.allocate()
    middle_attempt = env.submit(env.alloc_principal(middle), launch="middle", params=env.fixture_params("middle"))["attempt_id"]
    env.wait_ended(middle_attempt)
    client.release_allocation(middle)
    marker_owner = env.allocate()
    principal = env.alloc_principal(marker_owner)
    age_records(env, attempts=[old_attempt], allocations=[older])
    age_records(env, attempts=[middle_attempt], allocations=[middle], at=seconds_ago(600))
    size = registry_bytes(env)

    def older_moved() -> bool:
        return old_attempt not in env.state()["attempts"]

    with contextlib.suppress(CaseFailure):
        env.wait(older_moved, 20, "the record older than the retention leaves the registry")
    compaction_barrier(env, client, principal, middle_attempt)
    state = env.state()
    archived = archive_pairs(env)
    return {
        "the registry is under the default threshold": size < launcher.Timings().compact_threshold,
        "a record older than the retention moved": ("attempts", old_attempt) in archived and old_attempt not in state["attempts"],
        "a record older than the floor and younger than the retention stays": middle_attempt in state["attempts"]
        and ("attempts", middle_attempt) not in archived,
        "its allocation stays": middle in state["allocations"] and ("allocations", middle) not in archived,
        "the launcher still serves": client.call("ping")["status"] == "serving",
    }


@case("registry_threshold_ages", timings={**THRESHOLD_TIMINGS, "compact_floor": 4.0, "compact_threshold": 10_000})
def registry_threshold_ages(env: Env) -> dict[str, bool]:
    """A160 (failure mode TH8): records that become old enough with no change to the file move on the next periodic pass, and not before the
    age floor, while the registry is above the threshold."""
    env.start()
    client = env.client()
    released = env.allocate()
    ended: list[str] = []
    for index in range(2):
        attempt_id = env.submit(env.alloc_principal(released), launch=f"age{index}", params=env.fixture_params(f"age{index}"))["attempt_id"]
        env.wait_ended(attempt_id)
        ended.append(attempt_id)
    client.release_allocation(released)
    ended_at = datetime.fromisoformat(max(env.state()["attempts"][a]["ended_at"] for a in ended)).timestamp()
    over = registry_bytes(env) > int(env.settings["timings"]["compact_threshold"])
    seen: dict[str, float] = {}

    def gone() -> bool:
        if all(a not in env.state()["attempts"] for a in ended):
            seen["at"] = time.time()
            return True
        return False

    with contextlib.suppress(CaseFailure):
        env.wait(gone, 25, "the attempts leave the registry once they are older than the floor")
    archived = archive_pairs(env)
    return {
        "the registry is over the threshold": over,
        "the attempts moved to the archive with no change to the file": all(("attempts", a) in archived for a in ended),
        "they did not move before the age floor": "at" in seen and seen["at"] - ended_at >= 3.5,
        "the launcher still serves": client.call("ping")["status"] == "serving",
    }


BULK_REQUESTS = 3000


def bulk_registry(env: Env) -> int:
    """Fill the registry with synthetic finished requests (fresh, so none is old enough to move) until it is over one million bytes."""
    at = utc_stamp()
    env.registry_id()
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        for index in range(BULK_REQUESTS):
            state["requests"][f"any:0|status|synthetic-{index}"] = {
                "payload_sha256": f"{index:064x}",
                "state": "done",
                "result": {"note": "synthetic record " + "x" * 200},
                "method": "status",
                "at": at,
            }
    return registry_bytes(env)


def utc_stamp() -> str:
    return datetime.now(UTC).isoformat()


@case("registry_reread_on_change", timings={"compact_every": 1_000_000_000.0, "history_retain": 3600.0, "poll_patience": 1.0, "create_timeout": 3.0})
def registry_reread_on_change(env: Env) -> dict[str, bool]:
    """A161 fix 3 (failure modes TO12a to TO12e, TH6 and TH7): the maintenance tick of an idle launcher with a large registry does not parse the
    file; a change to the file is picked up on the next tick; a create operation that expires and an orphan operation that needs a retry are
    still handled with no parse; a refused archive is tried once per change of the file. Parses are counted by wrapping ``Launcher.read_open_operations``."""
    size = bulk_registry(env)
    real = run_lock.transaction
    reads: list[bool] = []
    real_read = launcher.Launcher.read_open_operations

    def counted(self: launcher.Launcher, *args: Any, **keywords: Any) -> None:
        reads.append(True)
        real_read(self, *args, **keywords)

    def outside(change: Callable[[dict[str, Any]], None]) -> None:
        with real(run_lock.Registry(env.registry, "test")) as state:
            change(state)

    scans, spawned, archive = mock.Mock(), mock.Mock(), mock.Mock(side_effect=OSError("synthetic archive fault"))
    clock = iter(range(10_000))

    def tick() -> None:
        with contextlib.suppress(OSError):
            instance.maintenance(1000.0 + 0.6 * next(clock))

    with compactor(env) as instance, mock.patch.object(launcher.Launcher, "read_open_operations", counted):
        instance.docker_ok, instance.status = True, "serving"
        with (
            mock.patch.object(instance, "start_scan", scans),
            mock.patch.object(instance, "spawn", spawned),
            mock.patch.object(instance, "archive_history", archive),
        ):
            tick()
            first = len(reads)
            for _ in range(19):
                tick()
            idle_reads = len(reads) - first
            outside(
                lambda state: state["requests"].update(
                    {"outside": {"payload_sha256": "0" * 64, "state": "done", "result": {}, "method": "status", "at": utc_stamp()}}
                )
            )
            before_change = len(reads)
            tick()
            change_reads = len(reads) - before_change
            tick()
            tick()
            outside(
                lambda state: state["operations"].update(
                    {"orphan": {"op_id": "orphan", "state": "open", "kind": "orphan", "attempt": "none", "container_id": "c0", "dispatched_at": time.time()}}
                )
            )
            tick()
            after_orphan = len(reads)
            spawned_before = spawned.call_count
            for _ in range(3):
                tick()
            orphan_retries, orphan_reads = spawned.call_count - spawned_before, len(reads) - after_orphan
            outside(lambda state: state["operations"]["orphan"].update({"state": "settled"}))
            tick()
            spawned_settled = spawned.call_count
            tick()
            tick()
            stopped_retrying = spawned.call_count == spawned_settled
            created = time.time()
            outside(
                lambda state: state["operations"].update(
                    {"create": {"op_id": "create", "state": "open", "kind": "create", "attempt": "none", "dispatched_at": created}}
                )
            )
            tick()
            after_create = len(reads)
            early_scans = scans.call_count
            tick()
            time.sleep(3.4)
            tick()
            tick()
            scan_reads = len(reads) - after_create
            outside(
                lambda state: state["requests"].update(
                    {"old": {"payload_sha256": "1" * 64, "state": "done", "result": {}, "method": "status", "at": LONG_AGO}},
                )
            )
            tick()
            attempts_after_change = archive.call_count
            for _ in range(8):
                tick()
            archive_retries = archive.call_count - attempts_after_change
    return {
        "the registry is over one million bytes": size > 1_000_000,
        "an idle launcher parses the file at most on its first tick": idle_reads == 0 and first <= 2,
        "a change to the file is read on the next tick": change_reads >= 1,
        "an open orphan operation is retried on every tick": orphan_retries == 3,
        "the retry tick parses nothing": orphan_reads == 0,
        "a settled orphan operation is no longer retried after the next read": stopped_retrying,
        "no scan starts before the create operation expires": early_scans == 0,
        "the scan starts once the operation expired": scans.call_count >= 1,
        "the expiry needs no parse": scan_reads == 0,
        "a refused archive is tried on the change that made the records old": attempts_after_change >= 1,
        "and not again on later ticks while the file stays the same": archive_retries == 0,
    }


@case("registry_reread_inplace", timings={"compact_every": 1_000_000_000.0, "history_retain": 3600.0, "poll_patience": 1.0, "create_timeout": 3.0})
def registry_reread_inplace(env: Env) -> dict[str, bool]:
    """Round 4 finding 8 (failure modes RS1 to RS4): an outside write that keeps the file's inode, size and modification time (an in-place edit
    of equal length, with the time restored) still changes what maintenance retries. Each edit is read once on the next tick; an idle registry
    is not read; an atomic replacement is still read."""
    env.registry_id()
    path = env.registry / "registry.json"
    real = run_lock.transaction
    reads: list[bool] = []
    real_read = launcher.Launcher.read_open_operations

    def counted(self: launcher.Launcher, *args: Any, **keywords: Any) -> None:
        reads.append(True)
        real_read(self, *args, **keywords)

    def outside(change: Callable[[dict[str, Any]], None]) -> None:
        with real(run_lock.Registry(env.registry, "test")) as state:
            change(state)

    def edit_in_place(old: bytes, new: bytes) -> bool:
        """Replace ``old`` by ``new`` (equal length) inside the file and restore its times; True when inode, size and mtime are unchanged."""
        before = path.stat()
        data = path.read_bytes()
        if len(old) != len(new) or data.count(old) != 1:
            raise CaseFailure("the in-place edit needs one occurrence and an equal length")
        with path.open("r+b") as handle:
            handle.seek(data.index(old))
            handle.write(new)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        after = path.stat()
        return (after.st_ino, after.st_mtime_ns, after.st_size) == (before.st_ino, before.st_mtime_ns, before.st_size)

    outside(
        lambda state: state["operations"].update(
            {
                "orphan": {
                    "op_id": "orphan",
                    "state": "open",
                    "kind": "orphan",
                    "attempt": "none",
                    "container_id": "container_aaaa",
                    "dispatched_at": time.time(),
                }
            }
        )
    )
    spawned = mock.Mock()
    clock = iter(range(10_000))

    def tick() -> None:
        with contextlib.suppress(OSError):
            instance.maintenance(1000.0 + 0.6 * next(clock))

    def target() -> str:
        return str(spawned.call_args.args[3]) if spawned.call_args else ""

    with compactor(env) as instance, mock.patch.object(launcher.Launcher, "read_open_operations", counted):
        instance.docker_ok, instance.status = True, "serving"
        with mock.patch.object(instance, "start_scan", mock.Mock()), mock.patch.object(instance, "spawn", spawned):
            tick()
            tick()
            first_target, first_reads = target(), len(reads)
            for _ in range(5):
                tick()
            idle_reads = len(reads) - first_reads
            kept_stat = edit_in_place(b"container_aaaa", b"container_bbaa")
            before_edit = len(reads)
            tick()
            half_target, half_reads = target(), len(reads) - before_edit
            kept_stat_again = edit_in_place(b"container_bbaa", b"container_bbbb")
            before_edit = len(reads)
            tick()
            final_target, final_reads = target(), len(reads) - before_edit
            tick()
            tick()
            idle_after = len(reads) - before_edit - final_reads
            outside(lambda state: state["operations"]["orphan"].update({"state": "settled"}))
            tick()
            settled_calls = spawned.call_count
            tick()
            tick()
    return {
        "the first tick retries the orphan operation's container": first_target == "container_aaaa",
        "an idle registry is not read": idle_reads == 0,
        "the first edit kept the file's inode, size and modification time": kept_stat and kept_stat_again,
        "the first edit is read once on the next tick": half_reads == 1,
        "the retry then targets the edited container": half_target == "container_bbaa",
        "a second edit of the same kind is read once and retried": final_reads == 1 and final_target == "container_bbbb",
        "ticks after the edits read nothing": idle_after == 0,
        "an atomic replacement is still read and stops the retry": spawned.call_count == settled_calls,
    }


@case(
    "registry_snapshot_identity",
    timings={"compact_every": 1_000_000_000.0, "compact_threshold": 1_000_000_000.0, "history_retain": 3600.0, "poll_patience": 1.0},
)
def registry_snapshot_identity(env: Env) -> dict[str, bool]:
    """Round 5 finding 15 (failure modes SN1, SN2 and SN5): the change indicator and the cached open operations describe one snapshot of the
    file. An outside atomic replacement (state B) lands right after the file was hashed; state A comes back before the next tick, so the
    signature taken from A must not certify a cache that holds B. Without the restore the next tick reads B once. A half-written file is
    refused without storing its signature."""
    env.registry_id()
    path = env.registry / "registry.json"
    real = run_lock.transaction

    def outside(change: Callable[[dict[str, Any]], None]) -> None:
        with real(run_lock.Registry(env.registry, "test")) as state:
            change(state)

    outside(
        lambda state: state["operations"].update(
            {"orphan": {"op_id": "orphan", "state": "open", "kind": "orphan", "attempt": "none", "container_id": "synthetic_a", "dispatched_at": time.time()}}
        )
    )
    original = path.read_bytes()
    changed = original.replace(b"synthetic_a", b"synthetic_b")
    if len(changed) != len(original) or changed == original:
        raise CaseFailure("the two states need the same length and different content")

    def replace_with(data: bytes) -> None:
        replacement = path.with_name("outside-replacement")
        replacement.write_bytes(data)
        replacement.chmod(0o600)
        replacement.replace(path)

    armed: list[bytes] = []
    real_file_digest, real_sha256 = hashlib.file_digest, hashlib.sha256

    def fire() -> None:
        if armed:
            replace_with(armed.pop())

    def file_digest_then_write(*args: Any, **keywords: Any) -> Any:
        digest = real_file_digest(*args, **keywords)
        fire()
        return digest

    def sha256_then_write(*args: Any, **keywords: Any) -> Any:
        digest = real_sha256(*args, **keywords)
        fire()
        return digest

    spawned = mock.Mock()
    clock = iter(range(10_000))
    reads: list[bool] = []
    real_read = launcher.Launcher.read_open_operations

    def counted_read(self: launcher.Launcher, *args: Any, **keywords: Any) -> None:
        reads.append(True)
        real_read(self, *args, **keywords)

    def tick() -> None:
        instance.maintenance(1000.0 + 0.6 * next(clock))

    def raised(function: Callable[[], Any]) -> BaseException | None:
        try:
            function()
        except run_lock.RegistryError as error:
            return error
        return None

    def target() -> str:
        return str(spawned.call_args.args[3]) if spawned.call_args else ""

    with compactor(env) as instance, mock.patch.object(launcher.Launcher, "read_open_operations", counted_read):
        instance.docker_ok, instance.status = True, "serving"
        with (
            mock.patch.object(instance, "start_scan", mock.Mock()),
            mock.patch.object(instance, "spawn", spawned),
            mock.patch.object(hashlib, "file_digest", file_digest_then_write),
            mock.patch.object(hashlib, "sha256", sha256_then_write),
        ):
            armed.append(changed)
            tick()
            replaced_after_hash = not armed
            first_reads = len(reads)
            replace_with(original)
            tick()
            restored_target, restored_reads = target(), len(reads) - first_reads
            tick()
            idle_reads = len(reads) - first_reads - restored_reads
            armed.append(changed)
            tick()
            tick()
            changed_target = target()
            replace_with(original)
            tick()
            back_target = target()
            known = instance.maintained
            replace_with(original[: len(original) // 2])
            refused = raised(tick)
            kept = instance.maintained == known
            replace_with(changed)
            tick()
            after_refusal_target = target()
    return {
        "the replacement landed after the file was hashed": replaced_after_hash,
        "after state A came back, the retry targets A and not the replaced state": restored_target == "synthetic_a",
        "an idle registry is not read again": idle_reads == 0,
        "a replacement that stays is read and its target is retried": changed_target == "synthetic_b",
        "the restored state is read and retried again": back_target == "synthetic_a",
        "a half-written file is refused as invalid": isinstance(refused, run_lock.RegistryError),
        "its signature is not stored": kept,
        "the next valid state is read and retried": after_refusal_target == "synthetic_b",
    }


def snapshot_validation_scenario(env: Env, name: str, mutate: Callable[[dict[str, Any]], Any], cause: str) -> dict[str, bool]:
    path = env.registry / "registry.json"
    registry_id = env.registry_id()
    original = path.read_bytes()
    container = env.daemon.create_rogue(f"synthetic-{name}", {launcher.LABEL_REGISTRY: registry_id}, True)
    state = env.state()
    state["operations"]["synthetic_orphan"] = {
        "op_id": "synthetic_orphan",
        "state": "open",
        "kind": "orphan",
        "attempt": None,
        "container_id": container,
        "dispatched_at": time.time(),
    }
    replacement_data = mutate(state)
    seen: dict[str, bool] = {}
    try:
        with compactor(env) as instance:
            instance.docker_ok, instance.status = True, "serving"
            instance.maintenance(time.monotonic())
            signature, retries = instance.maintained, list(instance.orphan_retries)
            replacement = path.with_name("synthetic-replacement")
            replacement.write_bytes(
                replacement_data if isinstance(replacement_data, bytes) else json.dumps(state if replacement_data is None else replacement_data).encode()
            )
            replacement.chmod(0o600)
            session_timers = instance.session_timers

            def replace_after_transaction(now: float) -> None:
                session_timers(now)
                seen["preceding transaction succeeded"] = True
                replacement.replace(path)
                instance.last_maintenance = 0.0

            mark = len(env.daemon.calls)
            try:
                with mock.patch.object(instance, "session_timers", replace_after_transaction):
                    instance.timers()
                seen["rejected"] = False
                seen["cites its cause"] = False
            except run_lock.RegistryError as error:
                seen["rejected"] = True
                seen["cites its cause"] = bool(cause) and cause in str(error)
            instance.pool.shutdown(wait=True)
            seen["container removed"] = container not in env.daemon.containers
            calls = env.daemon.calls[mark:]
            seen["no stop or remove"] = not any(c["path"].endswith("/stop") or c["method"] == "DELETE" for c in calls)
            seen["cache preserved"] = instance.maintained == signature and instance.orphan_retries == retries
            path.write_bytes(original)
            instance.last_maintenance = 0.0
            instance.maintenance(time.monotonic())
            seen["valid snapshot recovers"] = instance.maintained == signature and instance.orphan_retries == retries
            instance.docker.stop(container, 1)
            instance.docker.remove(container)
    finally:
        path.write_bytes(original)
    return seen


def disposal_identity_checks(env: Env) -> dict[str, bool]:
    registry_id = env.registry_id()
    checks: dict[str, bool] = {}
    with compactor(env) as instance:
        for name, labels in (
            ("matching", {launcher.LABEL_REGISTRY: registry_id}),
            ("foreign", {launcher.LABEL_REGISTRY: "synthetic-foreign"}),
            ("missing", {}),
        ):
            container = env.daemon.create_rogue(f"synthetic-dispose-{name}", labels, True)
            mark = len(env.daemon.calls)
            outcome: launcher.LauncherError | None = None
            try:
                instance.docker_dispose(container)
            except launcher.LauncherError as error:
                outcome = error
            calls = env.daemon.calls[mark:]
            if name == "matching":
                checks["the disposal worker removes its matching container"] = outcome is None and container not in env.daemon.containers
            else:
                checks[f"the disposal worker rejects a {name} label before stop or remove"] = (
                    isinstance(outcome, launcher.LauncherError)
                    and outcome.code == "ownership"
                    and container in env.daemon.containers
                    and not any(c["path"].endswith("/stop") or c["method"] == "DELETE" for c in calls)
                )
            instance.docker.stop(container, 1)
            instance.docker.remove(container)
            checks[f"a vanished {name} container remains an idempotent disposal"] = outcome_of(partial(instance.docker_dispose, container)) is None
    return checks


@case("registry_snapshot_validation", timings={"compact_every": 1e12, "compact_threshold": 1e12})
def registry_snapshot_validation(env: Env) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    controls: dict[str, dict[str, bool]] = {}
    mutations: dict[str, Callable[[dict[str, Any]], Any]] = {
        "matching": lambda state: None,
        "foreign": lambda state: state.update(registry_id="synthetic-foreign"),
        "unknown_schema": lambda state: state.update(schema_version=99),
        "legacy_schema": lambda state: state.update(schema_version=run_lock.LEGACY_SCHEMA),
        "launcher": lambda state: state.update(launcher=[]),
        "reservation_history": lambda state: state.update(reservation_history={}),
        "invalid_json": lambda state: b"{",
        "non_object": lambda state: [],
        **{name: partial(dict.update, **{name: []}) for name in run_lock.SECTIONS + run_lock.LAUNCHER_SECTIONS},
    }
    causes = {
        "foreign": "registry_id differs",
        "unknown_schema": "unknown schema",
        "legacy_schema": "launcher schema required",
        "launcher": "missing launcher",
        "reservation_history": "missing reservation_history",
        "invalid_json": "is not valid",
        "non_object": "unknown schema",
        **{name: f"missing {name}" for name in run_lock.SECTIONS + run_lock.LAUNCHER_SECTIONS},
    }
    for name, mutate in mutations.items():
        seen = snapshot_validation_scenario(env, name, mutate, causes.get(name, ""))
        controls[name] = seen
        if name == "matching":
            checks["a matching snapshot removes its synthetic orphan"] = (
                seen["preceding transaction succeeded"] and seen["container removed"] and not seen["rejected"]
            )
        else:
            checks[f"{name}: invalid snapshot is rejected before stop or remove"] = (
                seen["preceding transaction succeeded"]
                and seen["rejected"]
                and seen["cites its cause"]
                and seen["no stop or remove"]
                and not seen["container removed"]
            )
            checks[f"{name}: rejection preserves the cache and permits recovery"] = seen["cache preserved"] and seen["valid snapshot recovers"]
    checks |= disposal_identity_checks(env)
    path = env.registry / "registry.json"
    original = path.read_bytes()
    try:
        for schema in (run_lock.LEGACY_SCHEMA, run_lock.LAUNCHER_SCHEMA):
            state = run_lock.empty_state(env.registry_id(), schema)
            path.write_text(json.dumps(state))
            with run_lock.transaction(run_lock.Registry(env.registry, "test"), write=False) as parsed:
                checks[f"transactions keep valid schema {schema} behavior"] = (
                    parsed["schema_version"] == schema and parsed["registry_id"] == state["registry_id"]
                )
    finally:
        path.write_bytes(original)
    (env.report / "snapshot_validation.json").write_text(json.dumps(controls, indent=2) + "\n")
    return checks


@case("client_loss_wording")
def client_loss_wording(env: Env) -> dict[str, bool]:
    """Round 5 choice 8 (failure modes WD1 to WD3): the package fixture and the staging runner describe a lost connection to the launcher as
    one the launcher did not answer, with its code, and keep the word refused for a refusal."""
    from scripts.lakehouse import package_fixture
    from scripts.lakehouse import run_staging_e2e as staging

    lost = launcher.ClientError("nothing answers at the socket", "unavailable")
    refusal = launcher.ClientError("no such template", "refused")

    def kit_text(error: launcher.ClientError) -> str:
        client = env.client(start=False)
        with mock.patch.object(client, "run_attempt", side_effect=error):
            try:
                package_fixture.Session(client, "al-wording").run("fixture_dbt", "wording", [], {})
            except package_fixture.FixtureError as raised:
                return str(raised)
        return ""

    def staging_text(error: launcher.ClientError) -> str:
        client = env.client(start=False)
        with (
            mock.patch.object(staging, "POOL", None),
            mock.patch.object(staging, "launcher_client", return_value=client),
            mock.patch.object(staging, "light_allocation", return_value="al-wording"),
            mock.patch.object(staging, "record_budget"),
            mock.patch.object(client, "run_attempt", side_effect=error),
            mock.patch.object(client, "release_allocation"),
        ):
            try:
                staging.launch("fixture_dbt", "wording", [], {})
            except RuntimeError as raised:
                return str(raised)
        return ""

    kit_lost, kit_refused, staging_lost, staging_refused = kit_text(lost), kit_text(refusal), staging_text(lost), staging_text(refusal)
    return {
        "package fixture: a lost connection is a launcher that did not answer, with its code": "did not answer" in kit_lost and "[unavailable]" in kit_lost,
        "package fixture: a lost connection is not called a refusal": "refused" not in kit_lost,
        "package fixture: a refusal is still called a refusal": "refused" in kit_refused and "no such template" in kit_refused,
        "staging runner: a lost connection is a launcher that did not answer, with its code": "did not answer" in staging_lost
        and "[unavailable]" in staging_lost,
        "staging runner: a lost connection is not called a refusal": "refused" not in staging_lost,
        "staging runner: a refusal is still called a refusal": "refused" in staging_refused and "no such template" in staging_refused,
    }


@case("query_cli")
def query_cli(env: Env) -> dict[str, bool]:
    """A115 item 8 and round 2 finding 5: the documented one-query form ``query -c "..." -csv`` passes its arguments to DuckDB, ``query`` runs with no
    terminal (stdin closed), ``query --help`` prints the supported arguments and exits 0 before any catalog start or allocation, every supported flag
    reaches the DuckDB command and an unsupported one is refused with nothing held; ``run --template query_reader`` takes the reader registration
    its template needs and gives it back."""
    catalog_script(env, others_run_forever=False)
    env.start()
    base = ["-m", "orchestration.launcher", "--test-registry", str(env.registry)]
    before_help, containers_before = env.state(), set(env.daemon.create_bodies)
    help_long = run_command(sys.executable, [*base, "query", "--help"], cwd=ROOT, timeout=60)
    help_short = run_command(sys.executable, [*base, "query", "-h"], cwd=ROOT, timeout=60)
    after_help = env.state()
    help_started_nothing = (
        catalog_phase(env) == "down"
        and set(after_help["attempts"]) == set(before_help["attempts"])
        and after_help["allocations"] == before_help["allocations"]
        and after_help["registrations"] == before_help["registrations"]
        and set(env.daemon.create_bodies) == containers_before
    )
    catalog_up(env)
    query = run_command(sys.executable, [*base, "query", "-c", "select 1", "-csv"], cwd=ROOT, timeout=120)
    flags = list(launcher.DUCKDB_QUERY_FLAGS)
    every_flag = run_command(sys.executable, [*base, "query", "-c", "select 3", *flags], cwd=ROOT, timeout=120)
    unsupported = run_command(sys.executable, [*base, "query", "-c", "select 4", "-unsupported"], cwd=ROOT, timeout=120)
    run = run_command(
        sys.executable, [*base, "run", "--template", "query_reader", "--param", "tty=false", "--", "-csv", "-c", "select 2"], cwd=ROOT, timeout=120
    )
    state = env.state()
    arguments = [attempt["args"] for attempt in state["attempts"].values() if attempt["template"] == "query_reader"]
    commands = [list(body["Cmd"]) for body in env.daemon.create_bodies.values() if body.get("Labels", {}).get("hai.template") == "query_reader"]
    return {
        "query --help exits 0 and prints the supported arguments": help_long.returncode == 0
        and "-c" in help_long.stdout
        and all(flag in help_long.stdout for flag in flags),
        "query -h prints the same help": help_short.returncode == 0 and help_short.stdout == help_long.stdout,
        "help starts no catalog and takes no allocation, registration or attempt": help_started_nothing,
        "query -c ... -csv exits 0": query.returncode == 0,
        "query passes -c, its SQL and -csv to the attempt unchanged": ["-c", "select 1", "-csv"] in arguments,
        "query with stdin closed does not fail on reading stdin": "Invalid argument" not in query.stderr,
        "every supported flag and the SQL reach the DuckDB command": every_flag.returncode == 0
        and any(all(flag in command for flag in flags) and command[-2:] == ["-c", "select 3"] for command in commands),
        "an unsupported flag is refused by the closed schema (exit 3)": unsupported.returncode == 3 and "not in the closed schema" in unsupported.stderr,
        "run --template query_reader exits 0": run.returncode == 0,
        "run passes its arguments to the attempt": ["-csv", "-c", "select 2"] in arguments,
        "no reader registration is left": not state["registrations"],
        "every allocation is released": all(allocation["state"] == "released" for allocation in state["allocations"].values()),
    }


STDIN_CLIENT = """\
import errno, os, resource, selectors, sys, threading
sys.path.insert(0, sys.argv[1])
from orchestration import launcher
mode, source = sys.argv[2], sys.argv[3]
sys.argv = ["launcher", *sys.argv[4:]]
if mode == "unwatched":
    plain = selectors.DefaultSelector

    class Refusing(plain):
        def register(self, fileobj, events, data=None):
            if data == "stdin":
                raise OSError(errno.EPERM, "a standard input that the selector refuses, as a regular file is on Linux")
            return super().register(fileobj, events, data)

    selectors.DefaultSelector = Refusing
if mode == "pipe":
    reader, writer = os.pipe()

    def feed():
        with open(source, "rb") as stream:
            while chunk := stream.read(65536):
                view = memoryview(chunk)
                while view:
                    view = view[os.write(writer, view):]
        os.close(writer)

    threading.Thread(target=feed, daemon=True).start()
    os.dup2(reader, 0)
else:
    os.dup2(os.open(source, os.O_RDONLY), 0)
code = launcher.main()
sys.stdout.flush()
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
sys.stderr.write("peak_rss_bytes=%d\\n" % (peak if sys.platform == "darwin" else peak * 1024))
raise SystemExit(code)
"""


@case("query_stdin_large")
def query_stdin_large(env: Env) -> dict[str, bool]:
    """Round 2 finding 2 (failure modes SR1 to SR7): ``query`` with a standard input that cannot be watched (a regular file) sends it in bounded
    chunks and drains the output in the same loop. A multi-megabyte file reaches the container byte for byte and every echoed byte comes back,
    the client's peak memory stays near that of a small file, the launcher answers pings meanwhile and the same file through a pipe (the duplex
    control) and through the platform's own selector behaves the same. The unwatched mode refuses the standard input in the selector, as Linux does
    for a regular file, because macOS watches regular files and refuses only ``/dev/null``."""
    catalog_script(env, others_run_forever=False)
    queries: list[dict[str, Any]] = []
    previous = env.daemon.script

    def script(container: dict[str, Any]) -> tuple[float, int] | None:
        if container["Config"]["Labels"].get("hai.template") == "query_reader":
            queries.append(container)
            return None
        return previous(container)

    env.daemon.script = script
    env.start()
    catalog_up(env)
    client_script = env.work / "stdin_client.py"
    client_script.write_text(STDIN_CLIENT)
    base = [str(client_script), str(ROOT)]
    registry = ["--test-registry", str(env.registry), "query"]
    pings: list[bool] = []

    def one(label: str, size: int, mode: str) -> dict[str, Any]:
        payload = (b"select 1;\n" * (size // 10 + 1))[:size]
        source = env.work / f"{label}.sql"
        source.write_bytes(payload)
        index = len(queries)
        stop = threading.Event()

        def watch() -> None:
            """End the container once all input has arrived, and ping the launcher while the client sends."""
            while not stop.is_set():
                if len(queries) > index and len(queries[index]["_stdin"]) >= size:
                    env.daemon.finish(queries[index], 0)
                    return
                try:
                    pings.append(env.client(start=False).call("ping", timeout=10)["status"] == "serving")
                except (launcher.ClientError, OSError, ValueError):
                    pings.append(False)
                time.sleep(0.25)

        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        timed_out = False
        try:
            child = run_command(sys.executable, [*base, mode, str(source), *registry], cwd=ROOT, timeout=60)
        except TimeoutExpired:
            timed_out = True
            child = None
        finally:
            stop.set()
            watcher.join(timeout=10)
        received = bytes(queries[index]["_stdin"]) if len(queries) > index else b""
        peak = re.search(r"peak_rss_bytes=(\d+)", child.stderr) if child is not None else None
        result = {
            "label": label,
            "bytes": size,
            "timed_out": timed_out,
            "exit": None if child is None else child.returncode,
            "received": received == payload,
            "echoed": child is not None and child.stdout.replace("echo:", "") == payload.decode(),
            "peak": int(peak[1]) if peak else 0,
        }
        (env.work / f"{label}.json").write_text(json.dumps(result, indent=1))
        return result

    size = 4 * 1024 * 1024
    small = one("small", 100, "file")
    unwatched = one("large_unwatched", size, "unwatched")
    native = one("large_file", size, "file")
    piped = one("large_pipe", size, "pipe")
    margin = 8 * 1024 * 1024
    return {
        "a small file reaches the container and its echo returns": small["exit"] == 0 and small["received"] and small["echoed"],
        "a multi-megabyte file on a standard input that cannot be watched reaches the container byte for byte": unwatched["exit"] == 0
        and unwatched["received"],
        "every echoed byte of that file comes back (output drained while input is sent)": unwatched["echoed"],
        "the client's peak memory for that file stays within a fixed margin of the small file's": small["peak"] > 0
        and unwatched["peak"] > 0
        and unwatched["peak"] <= small["peak"] + margin,
        "a multi-megabyte regular file on the platform's own selector arrives and returns whole": native["exit"] == 0
        and native["received"]
        and native["echoed"],
        "duplex control: the same file through a watchable pipe arrives and returns whole": piped["exit"] == 0 and piped["received"] and piped["echoed"],
        "the launcher answered every ping during the transfers": bool(pings) and all(pings),
        "no client timed out": not any(run["timed_out"] for run in (small, unwatched, native, piped)),
    }


def detached_python(env: Env, code: str, seconds: float = 30) -> dict[str, Any]:
    """Run Python code in a process that is not a descendant of this runner and return the JSON it wrote."""
    out = env.work / f"detached_{time.monotonic_ns()}.json"
    script = f"import json, sys\nsys.path.insert(0, {str(ROOT)!r})\nfrom orchestration import launcher\n{code}\n"
    middle = os.fork()
    if middle == 0:
        try:
            os.setsid()
            os.chdir(ROOT)
            actions = [(os.POSIX_SPAWN_OPEN, 0, os.devnull, os.O_RDONLY, 0), (os.POSIX_SPAWN_OPEN, 1, os.devnull, os.O_WRONLY, 0), (os.POSIX_SPAWN_DUP2, 1, 2)]
            os.posix_spawn(
                sys.executable,
                [sys.executable, "-c", script, str(out)],
                {"PATH": os.environ["PATH"], "HOME": str(launcher.account_home())},
                file_actions=actions,
            )
        finally:
            os._exit(0)
    os.waitpid(middle, 0)
    env.wait(out.is_file, seconds, "the detached process wrote its result")
    return dict(json.loads(out.read_text()))


@case("unrelated_and_reused_pid")
def unrelated_and_reused_pid(env: Env) -> dict[str, bool]:
    """911 and 913: a process that is not a descendant of the holder is refused; a holder whose PID start time differs is refused."""
    hold_forever(env)
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    code = f"""
config = launcher.resolve_config({str(env.registry)!r})
client = launcher.LauncherClient(config)
result = {{}}
for name, call in {{
    "submit": lambda: client.call("submit", {{"template": "fixture_dbt", "launch_id": "x", "args": ["build"], "params": {{}}, "checkout": {str(ROOT)!r}}},
        {{"kind": "alloc", "alloc_id": {alloc!r}}}),
    "status": lambda: client.call("status", {{"what": "allocation", "id": {alloc!r}}}),
    "release": lambda: client.call("release_allocation", {{}}, {{"kind": "alloc", "alloc_id": {alloc!r}}}),
}}.items():
    try:
        call()
        result[name] = "accepted"
    except launcher.ClientError as error:
        result[name] = error.code
open(sys.argv[1], "w").write(json.dumps(result))
"""
    outsider = detached_python(env, code)
    own = env.submit(principal, params=env.fixture_params("a"))
    env.wait_phase(own["attempt_id"], ("started",))
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        state["allocations"][alloc]["holder_start"] = "Thu Jan  1 00:00:00 1970"
    reused = expect_error(lambda: env.submit(principal, launch="l2", params=env.fixture_params("b")))
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        state["allocations"][alloc]["holder_start"] = run_lock.process_start(os.getpid())
    env.client().call("cancel", {"attempt_id": own["attempt_id"]}, principal)
    env.wait_ended(own["attempt_id"])
    return {
        "an unrelated process cannot submit": outsider["submit"] == "principal",
        "an unrelated process cannot read the allocation": outsider["status"] == "principal",
        "an unrelated process cannot release it": outsider["release"] == "principal",
        "a reused PID (a different start time) is refused": refused(reused, "principal"),
        "the holder works again with its own start time": env.phase(own["attempt_id"]) == "ended",
    }


PINS_RUN = "20261012T120000Z-0a1b2c3d"


def catalog_script(env: Env, bootstrap_code: int = 0, others_run_forever: bool = True) -> None:
    """Services run until stopped; the one-time bootstrap ends with the given code; other containers run until stopped or end at once."""

    def script(container: dict[str, Any]) -> tuple[float, int] | None:
        service = container["Config"]["Labels"].get("hai.service")
        if service == "polaris-bootstrap":
            return 0.3, bootstrap_code
        if service or others_run_forever:
            return None
        return 0.2, 0

    env.daemon.script = script


def catalog_phase(env: Env) -> str:
    """The catalog's recorded phase."""
    return str((env.state()["launcher"].get("catalog") or {}).get("phase", "down"))


def catalog_up(env: Env, seconds: float = 40) -> str:
    """Start the catalog and wait for ``up`` or a failed start (``down``); returns the phase."""
    env.client().call("catalog_up")
    time.sleep(0.2)
    return str(env.wait(lambda: catalog_phase(env) in ("up", "down") and catalog_phase(env), seconds, "the catalog is up or failed"))


def resumed_start_stopped(env: Env, elsewhere: Path, when: str) -> bool:
    """A startup accepted by a launcher in the catalog's checkout, its network create paused (before or after its effect) while that launcher
    dies; a launcher whose checkout differs then opens the registry. It must not resume the startup: it tears down what exists and stays down.

    A create paused before its effect is released before the second launcher starts, so its effect lands first. A dead launcher's request that
    lands after the second launcher's teardown can leave one empty network, which no scan sees (issue WV3-010, amendment A88)."""
    env.settings.pop("catalog_checkout", None)
    env.write_settings()
    env.start()
    gate = env.daemon.add_gate("net_create", when)
    env.client().call("catalog_up")
    gate.wait_reached()
    env.kill_launcher()
    env.settings["catalog_checkout"] = str(elsewhere)
    env.write_settings()
    if when == "before":
        gate.release()
        env.wait(lambda: bool(env.daemon.networks), 10, "the dead launcher's create has taken effect")
    env.start()
    gate.release()
    stopped = False
    try:
        env.wait(lambda: catalog_phase(env) in ("up", "down") and not (catalog_phase(env) == "down" and env.daemon.networks), 40, "the startup settles")
        time.sleep(0.3)
        opened = [o for o in env.state()["operations"].values() if o["kind"] in ("network_create", "network_remove") and o["state"] == "open"]
        catalog = env.state()["launcher"]["catalog"]
        stopped = catalog["phase"] == "down" and not env.labelled() and not env.daemon.networks and not opened and "main checkout" in catalog.get("error", "")
    finally:
        if catalog_phase(env) != "down":  # an unguarded launcher brought the catalog up: take it down so the next scenario starts clean
            env.client().call("catalog_down")
            env.wait(lambda: catalog_phase(env) == "down" and not env.daemon.networks and not env.labelled(), 40, "the catalog is down")
        env.stop_launcher()
    return stopped


def pending_service_start_stopped(env: Env, elsewhere: Path) -> bool:
    """A startup whose database start request is paused before the daemon applies it while its launcher dies; a launcher whose checkout
    differs then opens the registry. It sends no start of its own (review C3a-1 delta 1), and tears down what the paused request starts."""
    env.settings.pop("catalog_checkout", None)
    env.write_settings()
    env.start()
    gate = env.daemon.add_gate("start", "before", match=lambda c: "polaris-db" in c["name"])
    env.client().call("catalog_up")
    gate.wait_reached()
    env.kill_launcher()
    env.settings["catalog_checkout"] = str(elsewhere)
    env.write_settings()
    env.start()
    time.sleep(1.5)  # several passes of the replacement's state machine while the original start is still held

    def database_starts() -> int:
        return len([c for c in env.daemon.calls_matching("POST", "/containers/") if c["path"].endswith("/start") and "polaris-db" in str(c.get("target"))])

    replacement_sent_none = database_starts() == 1
    gate.release()
    stopped = False
    try:
        env.wait(lambda: catalog_phase(env) == "down" and not env.labelled() and not env.daemon.networks, 40, "the startup is torn down")
        opened = [o for o in env.state()["operations"].values() if o["state"] == "open"]
        stopped = not opened and "main checkout" in env.state()["launcher"]["catalog"].get("error", "")
    finally:
        if catalog_phase(env) != "down":
            env.client().call("catalog_down")
            env.wait(lambda: catalog_phase(env) == "down" and not env.daemon.networks and not env.labelled(), 40, "the catalog is down")
        env.stop_launcher()
    return replacement_sent_none and stopped


@case("catalog_checkout_guard")
def catalog_checkout_guard(env: Env) -> dict[str, bool]:
    """A85 and A86: the catalog starts only from a launcher running in the checkout that owns the catalog state, so a launcher started from
    another checkout never opens a new, empty catalog database there, neither on a new request nor by resuming a recorded startup."""
    catalog_script(env)
    elsewhere = env.temp / "main_checkout"
    elsewhere.mkdir()
    env.settings["catalog_checkout"] = str(elsewhere)
    env.write_settings()
    env.start()
    try:
        env.client().call("catalog_up")
        refused = ""
    except launcher.ClientError as error:
        refused = f"{error.code}: {error}"
    checks = {
        "catalog_up is refused": refused.startswith("refused:") and "main checkout" in refused,
        "no service container was created": not env.labelled(),
        "the catalog stays down": catalog_phase(env) == "down",
    }
    env.stop_launcher()
    checks["a startup whose network create was paused before its effect (applied before the restart) is torn down"] = resumed_start_stopped(
        env, elsewhere, "before"
    )
    checks["a startup resumed after its network create took effect with the reply lost is torn down"] = resumed_start_stopped(env, elsewhere, "after")
    checks["a pending database start is not repeated by a launcher from another checkout, and is torn down"] = pending_service_start_stopped(env, elsewhere)
    return checks


@case("catalog_lifecycle")
def catalog_lifecycle(env: Env) -> dict[str, bool]:
    """911 and 916: catalog_up starts the network, the database, the one-time bootstrap and Polaris through recorded operations; catalog_down removes them."""
    catalog_script(env)
    env.start()
    phase = catalog_up(env)
    state = env.state()
    services = {c["Config"]["Labels"].get("hai.service"): c for c in env.labelled()}
    polaris = services.get("polaris-db")
    checks = {
        "the catalog is up": phase == "up",
        "the database and Polaris run": set(services) == {"polaris-db", "polaris"},
        "the network exists": len(env.daemon.networks) == 1,
        "the bootstrap marker was written": (env.temp / "lakehouse" / "polaris_bootstrapped").is_file(),
        "the bootstrap attempt ended exited": any(
            a["params"].get("service") == "polaris-bootstrap" and a["reason"] == "exited" for a in state["attempts"].values()
        ),
        "service limits are registered for the growth reserve": set(state["services"]) == {"polaris-db", "polaris"},
        "Polaris publishes only on the loopback": services["polaris"]["_body"]["HostConfig"]["PortBindings"]
        == {
            "8181/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8181"}],
            "8182/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8182"}],
        },
        "the database has the Compose health check": polaris is not None and polaris["_body"]["Healthcheck"]["Test"][1:3] == ["pg_isready", "-U"],
        "every create has a recorded effective-specification digest": all(
            op.get("spec_digest") for op in state["operations"].values() if op["kind"] == "create" and "svc-" in op["name"]
        ),
    }
    env.client().call("catalog_down")
    env.wait(lambda: catalog_phase(env) == "down" and not env.daemon.networks and not env.labelled(), 30, "the catalog is down")
    checks["catalog_down removes the services and the network"] = not env.daemon.networks and not env.labelled()
    checks["the barrier is cleared and the limits are released"] = env.state()["launcher"]["barrier"] is None and not env.state()["services"]
    return checks


def network_gate_restart(env: Env, op: str, when: str, going_up: bool) -> bool:
    """One network operation whose reply is withheld while the launcher dies; the restarted launcher finishes it (True when the catalog got there)."""
    gate = env.daemon.add_gate(op, when)
    env.client().call("catalog_up" if going_up else "catalog_down")
    gate.wait_reached()
    env.kill_launcher()
    env.start()
    env.wait(lambda: catalog_phase(env) == ("up" if going_up else "down"), 40, f"the catalog is {'up' if going_up else 'down'} after the restart")
    gate.release()
    time.sleep(0.3)
    networks_ok = len(env.daemon.networks) == (1 if going_up else 0)
    opened = [o for o in env.state()["operations"].values() if o["kind"] in ("network_create", "network_remove") and o["state"] == "open"]
    return networks_ok and not opened


@case("network_ops_restart")
def network_ops_restart(env: Env) -> dict[str, bool]:
    """910, 911 and 916 (F8): a network create and remove, each paused before and after its effect with the reply lost, resume after a restart."""
    catalog_script(env)
    env.start()
    checks = {}
    checks["create paused before its effect, launcher killed: the restart creates the network and the catalog comes up"] = network_gate_restart(
        env, "net_create", "before", True
    )
    env.client().call("catalog_down")
    env.wait(lambda: catalog_phase(env) == "down" and not env.daemon.networks, 40, "down")
    checks["create applied and its reply lost, launcher killed: the restart finds the network and the catalog comes up"] = network_gate_restart(
        env, "net_create", "after", True
    )
    checks["remove paused before its effect, launcher killed: the restart resumes the teardown"] = network_gate_restart(env, "net_remove", "before", False)
    catalog_up(env)
    checks["remove applied and its reply lost, launcher killed: the restart settles it and the catalog is down"] = network_gate_restart(
        env, "net_remove", "after", False
    )
    checks["the barrier is clear and nothing is left"] = env.state()["launcher"]["barrier"] is None and not env.labelled() and not env.daemon.networks
    return checks


@case("bootstrap_failure")
def bootstrap_failure(env: Env) -> dict[str, bool]:
    """911: a failed bootstrap leaves the catalog down and cleaned."""
    catalog_script(env, bootstrap_code=1)
    env.start()
    phase = catalog_up(env)
    env.wait(lambda: not env.daemon.networks and not env.labelled(), 30, "cleaned up")
    catalog = env.state()["launcher"]["catalog"]
    return {
        "the catalog is down": phase == "down" and catalog["phase"] == "down",
        "the error names the bootstrap": "polaris-bootstrap ended" in catalog.get("error", ""),
        "no container and no network is left": not env.labelled() and not env.daemon.networks,
        "no bootstrap marker": not (env.temp / "lakehouse" / "polaris_bootstrapped").exists(),
    }


@case("catalog_down_refusals")
def catalog_down_refusals(env: Env) -> dict[str, bool]:
    """911: catalog_down is refused with a reader, with a running catalog-dependent attempt and while a catalog startup is open."""
    catalog_script(env)
    env.start()
    client = env.client()
    gate = env.daemon.add_gate("create", "before", match=lambda c: "polaris-db" in c["name"])
    client.call("catalog_up")
    gate.wait_reached()
    during_start = expect_error(lambda: client.call("catalog_down"))
    gate.release()
    env.wait(lambda: catalog_phase(env) == "up", 40, "the catalog is up")
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    registration = client.call("register_reader", {}, principal)
    with_reader = expect_error(lambda: client.call("catalog_down"))
    client.call("deregister_reader", {"registration_id": registration["registration_id"]}, principal)
    client.call("release_allocation", {}, principal)
    run = env.acquire_run(PINS_RUN, "transform")
    attempt = client.call(
        "submit", {"template": "spark_read", "launch_id": "pins", "args": ["pin", "--run", PINS_RUN], "params": {"job": "pins"}, "checkout": str(ROOT)}, run
    )
    env.wait_phase(attempt["attempt_id"], ("started",))
    with_attempt = expect_error(lambda: client.call("catalog_down"))
    client.call("cancel", {"attempt_id": attempt["attempt_id"]}, run)
    env.wait_ended(attempt["attempt_id"])
    client.call("catalog_down")
    env.wait(lambda: catalog_phase(env) == "down", 30, "down")
    return {
        "refused while a catalog startup is open": refused(during_start, "refused", "startup is open"),
        "refused with a reader registration": refused(with_reader, "refused", "reader registration"),
        "refused with a running catalog-dependent attempt": refused(with_attempt, "refused", "needs the catalog"),
        "allowed when none holds the catalog": catalog_phase(env) == "down",
    }


@case("catalog_down_barrier")
def catalog_down_barrier(env: Env) -> dict[str, bool]:
    """911: while the teardown barrier is held, catalog_up, register_reader and catalog-dependent submits are refused."""
    catalog_script(env)
    env.start()
    client = env.client()
    catalog_up(env)
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    run = env.acquire_run(PINS_RUN, "transform")

    def attempts_while_paused() -> bool:
        """The three refusals of a held barrier."""
        return (
            refused(expect_error(lambda: client.call("catalog_up")), "refused", "barrier")
            and refused(expect_error(lambda: client.call("register_reader", {}, principal)), "refused", "barrier")
            and refused(
                expect_error(
                    lambda: client.call(
                        "submit",
                        {"template": "spark_read", "launch_id": "p", "args": ["pin", "--run", PINS_RUN], "params": {"job": "pins"}, "checkout": str(ROOT)},
                        run,
                    )
                ),
                "refused",
                "barrier",
            )
        )

    results: dict[str, bool] = {}
    first_gate = env.daemon.add_gate("stop", "before")
    client.call("catalog_down")
    first_gate.wait_reached()
    results["the barrier is recorded before the first stop"] = env.state()["launcher"]["barrier"] is not None
    results["paused before the first stop: catalog_up, register_reader and a catalog submit are refused"] = attempts_while_paused()
    second_gate = env.daemon.add_gate("stop", "before")
    first_gate.release()
    second_gate.wait_reached()
    results["paused before the second stop: all three are refused"] = attempts_while_paused() and env.state()["launcher"]["barrier"] is not None
    network_gate = env.daemon.add_gate("net_remove", "before")
    second_gate.release()
    network_gate.wait_reached()
    results["paused before the network removal: all three are refused"] = attempts_while_paused() and env.state()["launcher"]["barrier"] is not None
    stop_gate = network_gate
    stop_gate.release()
    env.wait(lambda: catalog_phase(env) == "down", 40, "down")
    results["the barrier is cleared only after absence is shown"] = (
        env.state()["launcher"]["barrier"] is None and not env.labelled() and not env.daemon.networks
    )
    client.call("release_allocation", {}, principal)
    return results


def restart_command(env: Env, delay: float, down: float) -> list[str]:
    """A restart command for the settings: after a delay it tells the fake daemon to restart."""
    code = (
        "import http.client, socket, sys, time\n"
        f"time.sleep({delay})\n"
        "class C(http.client.HTTPConnection):\n"
        "    def connect(self):\n"
        "        self.sock = socket.socket(socket.AF_UNIX)\n"
        f"        self.sock.connect({str(env.temp / 'd.sock')!r})\n"
        f"c = C('x'); c.request('POST', '/_test/restart?down={down}'); c.getresponse().read()\n"
    )
    return [sys.executable, "-c", code]


@case("docker_restart")
def docker_restart(env: Env) -> dict[str, bool]:
    """911: docker_restart is refused with a foreign running container, with a failed listing and with open work; otherwise it ends with the catalog stopped."""
    env.settings["restart_command"] = restart_command(env, 1.0, 1.0)
    env.write_settings()
    catalog_script(env)
    env.start()
    client = env.client()
    owner = {"kind": "any", "owner": True}
    foreign = env.daemon.create_rogue("someone-elses-database", {"com.docker.compose.project": "other"}, running=True)
    with_foreign = expect_error(lambda: client.call("docker_restart", {"owner": True}, owner))
    env.daemon.containers.pop(foreign)
    env.daemon.fail_list = True
    failed_listing = expect_error(lambda: client.call("docker_restart", {"owner": True}, owner))
    env.daemon.fail_list = False
    catalog_up(env)
    with_open_work = expect_error(lambda: client.call("docker_restart", {"owner": True}, owner))
    client.call("catalog_down")
    env.wait(lambda: catalog_phase(env) == "down", 30, "down")
    outcome: list[Any] = []
    thread = threading.Thread(target=lambda: outcome.append(env.client().call("docker_restart", {"owner": True}, owner, timeout=120)))
    thread.start()
    env.threads.append(thread)
    env.wait(lambda: env.state()["launcher"].get("restart"), 10, "the restart is recorded")
    during = expect_error(lambda: client.call("allocate", {}))
    status_during = client.call("status", {"what": "launcher"})
    thread.join(timeout=60)
    env.wait(lambda: env.ping().get("status") == "serving", 30, "serving after the restart")
    return {
        "refused with a foreign running container": refused(with_foreign, "refused", "was not created by this launcher"),
        "refused when the listing fails": refused(failed_listing, "degraded") or refused(failed_listing, "refused"),
        "refused while catalog attempts are open": refused(with_open_work, "refused", "is open"),
        "every request but status is refused while it runs": refused(during, "refused", "restart") and status_during["restart"] is not None,
        "the restart ended with the catalog stopped": bool(outcome) and outcome[0].get("catalog") == "down",
        "the launcher serves again": env.ping().get("status") == "serving" and env.state()["launcher"]["restart"] is None,
    }


@case("docker_restart_command_and_client")
def docker_restart_command_and_client(env: Env) -> dict[str, bool]:
    """911 and 919 (F10): the default restart command is the Docker command (nothing runs); a refused restart is a nonzero exit of the catalog client."""
    from scripts.lakehouse import catalog

    default_config = env.config
    env.start()
    foreign = env.daemon.create_rogue("someone-elses-database", {"com.docker.compose.project": "other"}, running=True)
    saved_argv = sys.argv
    saved_registry = catalog.TEST_REGISTRY
    stderr = io.StringIO()
    try:
        sys.argv = ["catalog.py", "--test-registry", str(env.registry), "release"]
        with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()):
            code = catalog.main()
    finally:
        sys.argv = saved_argv
        catalog.TEST_REGISTRY = saved_registry
    env.daemon.containers.pop(foreign)
    return {
        "the default restart command starts the docker program with desktop restart": default_config.restart_command == ("docker", "desktop", "restart")
        and launcher.Config.__dataclass_fields__["restart_command"].default == ("docker", "desktop", "restart"),
        "a refused restart (a foreign container runs) is a nonzero exit of the client": code == 1,
        "the failure text names the refusal and no success line is printed": "Docker not restarted" in stderr.getvalue(),
        "the restart did not run": not env.daemon.calls_matching("POST", "/restart"),
    }


@case("secret_commitment")
def secret_commitment(env: Env) -> dict[str, bool]:
    """911 and 916: a secret value changed while its source stays the same fails the commitment and the adoption is refused."""
    catalog_script(env)
    env.start()
    gate = env.daemon.add_gate("create", "after", match=lambda c: "polaris-db" in c["name"])
    env.client().call("catalog_up")
    gate.wait_reached()
    changed = next(c for c in env.daemon.containers.values() if "polaris-db" in c["Name"])
    changed["Config"]["Env"] = [e if not e.startswith("POSTGRES_PASSWORD=") else "POSTGRES_PASSWORD=somebody-changed-this" for e in changed["Config"]["Env"]]
    gate.release()
    env.wait(lambda: catalog_phase(env) == "down" and not env.labelled(), 40, "the start failed and is cleaned")
    state = env.state()
    db = next(a for a in state["attempts"].values() if a["params"].get("service") == "polaris-db")
    operations = [op for op in state["operations"].values() if op["attempt"] == db["id"] and op["kind"] == "create"]
    texts = json.dumps(state)
    return {
        "the adoption was refused on the commitment": operations[0]["result"] == "digest_mismatch" and db["reason"] == "create_failed",
        "the container was removed": not env.labelled(),
        "no secret value is in the registry": not any(value in texts for name, value in env.secret_values.items() if name.endswith(("PASSWORD", "SECRET"))),
        "the recorded environment holds a commitment, not the value": operations[0]["projection"]["env"]["POSTGRES_PASSWORD"].startswith("hmac:"),
    }


@case("effective_spec_mutations")
def effective_spec_mutations(env: Env) -> dict[str, bool]:
    """911 and 916 (F14): a daemon default or a lifecycle setting that differs from the recorded effective specification refuses the adoption."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    image = launcher.ComposeModel(ROOT / "docker-compose.yaml").image("analytics-dbt")

    def user(container: dict[str, Any]) -> None:
        container["Config"]["User"] = "root"

    def extra_env(container: dict[str, Any]) -> None:
        container["Config"]["Env"].append("INJECTED=1")

    def lost_image_env(container: dict[str, Any]) -> None:
        container["Config"]["Env"] = [e for e in container["Config"]["Env"] if not e.startswith("LANG=")]

    def workdir(container: dict[str, Any]) -> None:
        container["Config"]["WorkingDir"] = "/elsewhere"

    def restart(container: dict[str, Any]) -> None:
        container["HostConfig"]["RestartPolicy"] = {"Name": "always"}

    def auto_remove(container: dict[str, Any]) -> None:
        container["HostConfig"]["AutoRemove"] = True

    def other_image(container: dict[str, Any]) -> None:
        container["Image"] = "sha256:" + os.urandom(8).hex()

    def entrypoint(container: dict[str, Any]) -> None:
        container["Config"]["Entrypoint"] = ["/bin/sh"]

    mutations = {
        "user": user,
        "extra_env": extra_env,
        "lost_image_env": lost_image_env,
        "workdir": workdir,
        "restart": restart,
        "auto_remove": auto_remove,
        "other_image": other_image,
        "entrypoint": entrypoint,
    }
    checks: dict[str, bool] = {}
    for index, (label, mutate) in enumerate(mutations.items()):
        launch = f"m{index}"
        gate = env.daemon.add_gate("create", "after", match=partial(lambda name, c: f"-{name}-" in c["name"], launch))
        attempt_id = env.submit(principal, launch=launch, params=env.fixture_params(launch))["attempt_id"]
        gate.wait_reached()
        mutate(next(c for c in env.daemon.containers.values() if f"-{launch}-" in c["Name"]))
        gate.release()
        ended = env.wait_ended(attempt_id, 30)
        op = next(o for o in env.state()["operations"].values() if o["attempt"] == attempt_id and o["kind"] == "create")
        checks[f"{label}: the adoption is refused and the container is removed"] = (
            op["result"] == "digest_mismatch" and ended["reason"] == "create_failed" and env.labelled() == []
        )
    # An image default that changes between the recorded specification and the create is also a mismatch (the daemon merges the new default).
    gate = env.daemon.add_gate("create", "before", match=lambda c: "-d1-" in c["name"])
    attempt_id = env.submit(principal, launch="d1", params=env.fixture_params("d1"))["attempt_id"]
    gate.wait_reached()
    env.daemon.images[image]["Config"]["User"] = "root"
    gate.release()
    ended = env.wait_ended(attempt_id, 30)
    op = next(o for o in env.state()["operations"].values() if o["attempt"] == attempt_id and o["kind"] == "create")
    checks["an image default changed after the specification was recorded: refused"] = op["result"] == "digest_mismatch" and ended["reason"] == "create_failed"
    env.daemon.images[image]["Config"]["User"] = ""
    time.sleep(0.2)
    clean = env.submit(principal, launch="ok", params=env.fixture_params("ok"))["attempt_id"]
    clean_ended = env.wait_ended(clean, 30)
    ok_op = next(o for o in env.state()["operations"].values() if o["attempt"] == clean and o["kind"] == "create")
    checks["an unchanged container is adopted"] = clean_ended["reason"] == "exited" and ok_op["result"] == "adopted"
    checks["the operation records the image identity, the Compose revision and the interpolation mapping"] = (
        ok_op["projection"]["image_id"] == env.daemon.images[image]["Id"] and bool(ok_op["compose_sha256"]) and "interpolation" in ok_op
    )
    return checks


@case("effective_spec_digest")
def effective_spec_digest(env: Env) -> dict[str, bool]:
    """911: the dispatched specification's digest equals the recorded one; changed allocation values change the digest and not the Compose hash."""
    env.start()
    key = launcher.launcher_key(env.config.token_root)
    digests = []
    compose_hashes = set()
    memory = []
    for index, total in enumerate((32, 24)):
        env.daemon.mem_total = total * 1024**3
        alloc = env.allocate()
        principal = env.alloc_principal(alloc)
        attempt_id = env.submit(principal, launch=f"s{index}", params=env.fixture_params(f"s{index}"))["attempt_id"]
        env.wait_ended(attempt_id)
        op = next(o for o in env.state()["operations"].values() if o["attempt"] == attempt_id and o["kind"] == "create")
        body = env.daemon.create_bodies[op["name"]]
        defaults = {**op["image_defaults"], "env": op["image_defaults"]["env"]}
        recomputed = launcher.sha256_text(launcher.canonical(launcher.projection(body, key, frozenset(op["secret_env"]), op["secret_cmd"], defaults)))
        digests.append((op["spec_digest"], recomputed))
        compose_hashes.add(op["compose_sha256"])
        memory.append(body["HostConfig"]["Memory"])
        env.client().call("release_allocation", {}, principal)
    return {
        "the recorded digest equals the digest of what the daemon received": all(recorded == recomputed for recorded, recomputed in digests),
        "changed allocation values give another digest": digests[0][0] != digests[1][0] and memory[0] != memory[1],
        "the Compose file hash is unchanged": len(compose_hashes) == 1,
    }


WRAPPER = """
import importlib.util
import json
import os
import signal
import socket
import sys
import time

config = json.load(open(sys.argv[1]))
# Output goes to a file so that a process left alive by a denied signal cannot hold the harness's pipes open.
log = os.open(config["output"], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.dup2(log, 1)
os.dup2(log, 2)
spec = importlib.util.spec_from_file_location("deadline_run", config["module"])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
real_time, real_mono = time.time, time.monotonic
calls = {"mono": 0}
signals = []
deadline = real_time() + config["deadline_in"]


def stall():
    pid = os.getpid()
    if os.fork() == 0:
        time.sleep(config["stall_seconds"])
        os.kill(pid, signal.SIGCONT)
        os._exit(0)
    os.kill(pid, signal.SIGSTOP)


def mono():
    calls["mono"] += 1
    if config.get("stall_at") == calls["mono"]:
        stall()
    return real_mono()


original = os.killpg
denied = set(config.get("deny_signals", []))
probes = {"transient": config.get("transient_eperm_probes", 0), "gone": False}


def killpg(group, number):
    if number in (signal.SIGTERM, signal.SIGKILL):
        signals.append({"signal": int(number), "host": real_time()})
        if int(number) in denied:
            raise PermissionError(1, "Operation not permitted")
    if number == 0 and probes["transient"]:
        try:
            original(group, 0)
        except ProcessLookupError:
            # the group is gone but the probe reports EPERM for the first calls: the zombie-only condition seen on macOS
            probes["transient"] -= 1
            raise PermissionError(1, "Operation not permitted") from None
    return original(group, number)


os.killpg = killpg
original_spawn = os.posix_spawnp

def delayed_spawn(*args, **kwargs):
    time.sleep(config.get("spawn_delay", 0))
    return original_spawn(*args, **kwargs)

os.posix_spawnp = delayed_spawn
time.sleep(config.get("start_delay", 0))
code = module.run(config["command"], deadline, wall=lambda: real_time() + config["wall_offset"], mono=mono)
json.dump({"exit": code, "deadline": deadline, "signals": signals, "mono_calls": calls["mono"], "ended": real_time()}, open(config["result"], "w"))
sys.exit(code)
"""
STUBBORN = """
import json, os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
record = sys.argv[1]
open(record, "w").write(json.dumps({"exec": time.time(), "pid": os.getpid(), "pgid": os.getpgrp()}))
if os.fork() == 0:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(1)
while True:
    time.sleep(1)
"""


def stubborn_command(folder: Path, record: Path, detached_output: bool) -> list[str]:
    """The TERM-ignoring command; a case whose signals are denied leaves it alive, so it must not hold the wrapper's output pipes open."""
    command = [sys.executable, str(folder / "stubborn.py"), str(record)]
    return ["sh", "-c", 'exec "$@" >/dev/null 2>&1', "sh", *command] if detached_output else command


def run_deadline_case(
    env: Env,
    name: str,
    deadline_in: float,
    wall_offset: float = 0.0,
    stall_at: int | None = None,
    command: list[str] | None = None,
    start_delay: float = 0.0,
    deny_signals: list[int] | None = None,
    transient_eperm_probes: int = 0,
    spawn_delay: float = 0.0,
) -> dict[str, Any]:
    """One acceptance run of the real entrypoint code with a TERM-ignoring command that has a child process; returns the host-time stamps."""
    folder = env.work / f"deadline_{name}"
    folder.mkdir(exist_ok=True)
    record = folder / "command.json"
    (folder / "stubborn.py").write_text(STUBBORN)
    (folder / "wrapper.py").write_text(WRAPPER)
    config = {
        "module": str(ROOT / "services/hai/deadline_run.py"),
        "deadline_in": deadline_in,
        "wall_offset": wall_offset,
        "stall_at": stall_at,
        "stall_seconds": 6,
        "start_delay": start_delay,
        "deny_signals": [int(n) for n in deny_signals or []],
        "transient_eperm_probes": transient_eperm_probes,
        "spawn_delay": spawn_delay,
        "command": command or stubborn_command(folder, record, bool(deny_signals)),
        "result": str(folder / "result.json"),
        "output": str(folder / "wrapper_stderr.txt"),
    }
    (folder / "config.json").write_text(json.dumps(config))
    finished = run_command(sys.executable, [str(folder / "wrapper.py"), str(folder / "config.json")], timeout=180)
    output = (folder / "wrapper_stderr.txt").read_text()
    result = json.loads((folder / "result.json").read_text()) if (folder / "result.json").is_file() else {}
    started = json.loads(record.read_text()) if record.is_file() else None
    leftover_before_cleanup = False
    if started and deny_signals:
        # The wrapper was refused its signals: the harness (which may signal the group) ends what the wrapper could not.
        try:
            os.killpg(int(started["pgid"]), 0)
            leftover_before_cleanup = True
            os.killpg(int(started["pgid"]), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    group_empty = None
    if started:
        deadline_wait = time.time() + 30
        while time.time() < deadline_wait:
            try:
                os.killpg(int(started["pgid"]), 0)
            except ProcessLookupError:
                group_empty = time.time()
                break
            except PermissionError:
                pass  # macOS reports EPERM for a group of exited but unreaped members; only ESRCH proves it is empty
            time.sleep(0.02)
    deadline = float(result.get("deadline", 0))
    term = next((s["host"] for s in result.get("signals", []) if s["signal"] == signal.SIGTERM), None)
    kill = next((s["host"] for s in result.get("signals", []) if s["signal"] == signal.SIGKILL), None)
    cutoff = deadline - launcher.SKEW_SECONDS - launcher.START_MARGIN_SECONDS
    stamps = {
        "case": name,
        "exit": finished.returncode,
        "deadline": deadline,
        "cutoff": cutoff,
        "exec": started["exec"] if started else None,
        "term": term,
        "kill": kill,
        "group_empty": group_empty,
        "wall_offset": wall_offset,
        "mono_calls": result.get("mono_calls"),
        "ended": result.get("ended"),
        "leftover_before_cleanup": leftover_before_cleanup,
        "stderr_tail": output[-300:],
    }
    # The command's own start stamp is taken after exec (interpreter start-up), so the margin to the cutoff is recorded as
    # measured and the requirement is the strict one: no command starts after the cutoff.
    stamps["exec_margin_to_cutoff"] = None if started is None else cutoff - started["exec"]
    stamps["requirement_met"] = (started is None or group_empty is not None and group_empty <= deadline + 60) and (started is None or started["exec"] <= cutoff)
    (folder / "stamps.json").write_text(json.dumps(stamps, indent=1))
    return stamps


@case("deadline_acceptance")
def deadline_acceptance(env: Env) -> dict[str, bool]:
    """Historical Python entrypoint diagnostics; installed Go acceptance is recorded by scripts/lakehouse/deadline_trace.go."""
    load = []
    for _ in range(max(2, (os.cpu_count() or 4))):
        pid = os.fork()
        if pid == 0:
            while True:
                pass
        load.append(pid)
    plan: dict[str, dict[str, Any]] = {
        "start_after_cutoff": {"deadline_in": 3},
        "bracket_stall": {"deadline_in": 70, "stall_at": 2},
        "stall_before_boundary_check": {"deadline_in": 9, "stall_at": 3},
        "stall_before_child_recheck": {"deadline_in": 9, "stall_at": 4},
        "vm_clock_2s_ahead": {"deadline_in": 12, "wall_offset": 2.0},
        "vm_clock_2s_behind": {"deadline_in": 12, "wall_offset": -2.0},
        "boundary_3s_before": {"deadline_in": 8},
        "boundary_1_5s_before": {"deadline_in": 6.5},
        "boundary_0_5s_before": {"deadline_in": 5.5},
        "boundary_0_5s_after": {"deadline_in": 4.5},
        "loaded_machine": {"deadline_in": 12},
        "skew_beyond_s": {"deadline_in": 12, "wall_offset": -8.0},
        "f41_trace_vm_2s_behind_daemon_delay": {"deadline_in": 3.2, "wall_offset": -2.0, "start_delay": 2.0},
        "stall_vm_behind_before_boundary_check": {"deadline_in": 9, "wall_offset": -2.0, "stall_at": 3},
        "signals_denied_live_group": {"deadline_in": 12, "deny_signals": [int(signal.SIGTERM), int(signal.SIGKILL)]},
        "term_denied_kill_delivered": {"deadline_in": 12, "deny_signals": [int(signal.SIGTERM)]},
        "eperm_zombie_group_transient": {"deadline_in": 12, "transient_eperm_probes": 3},
    }
    stamps: dict[str, dict[str, Any]] = {}
    lock = threading.Lock()

    def one(name: str, options: dict[str, Any]) -> None:
        result = run_deadline_case(env, name, **options)
        with lock:
            stamps[name] = result

    threads = [threading.Thread(target=one, args=(name, options)) for name, options in plan.items()]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for pid in load:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
    missing = run_command(sys.executable, [str(ROOT / "services/hai/deadline_run.py"), "true"], timeout=30, env={"PATH": os.environ["PATH"]})
    normal = run_command(
        sys.executable,
        [str(ROOT / "services/hai/deadline_run.py"), "sh", "-c", "exit 7"],
        timeout=30,
        env={"PATH": os.environ["PATH"], "HAI_DEADLINE_UTC": str(time.time() + 30)},
    )
    (env.work / "deadline_stamps.json").write_text(json.dumps(stamps, indent=1))
    return {
        "start after the cutoff exits 124 and starts nothing": stamps["start_after_cutoff"]["exit"] == 124 and stamps["start_after_cutoff"]["exec"] is None,
        "a 6 s stall inside the bracket is retried and the requirement holds": stamps["bracket_stall"]["exec"] is not None
        and stamps["bracket_stall"]["requirement_met"],
        "a 6 s stall before the boundary check starts nothing": stamps["stall_before_boundary_check"]["exec"] is None
        and stamps["stall_before_boundary_check"]["exit"] == 124,
        "a 6 s stall in the child before its pre-exec recheck starts nothing": stamps["stall_before_child_recheck"]["exec"] is None
        and stamps["stall_before_child_recheck"]["exit"] == 124,
        "a VM clock 2 s ahead meets the requirement": stamps["vm_clock_2s_ahead"]["requirement_met"] and stamps["vm_clock_2s_ahead"]["kill"] is not None,
        "a VM clock 2 s behind meets the requirement": stamps["vm_clock_2s_behind"]["requirement_met"] and stamps["vm_clock_2s_behind"]["kill"] is not None,
        "the command starts 3 s before the boundary": stamps["boundary_3s_before"]["exec"] is not None and stamps["boundary_3s_before"]["requirement_met"],
        "the command starts 1.5 s before the boundary": stamps["boundary_1_5s_before"]["exec"] is not None
        and stamps["boundary_1_5s_before"]["requirement_met"],
        "the command starts 0.5 s before the boundary": stamps["boundary_0_5s_before"]["exec"] is not None
        and stamps["boundary_0_5s_before"]["requirement_met"],
        "0.5 s after the boundary starts nothing": stamps["boundary_0_5s_after"]["exec"] is None and stamps["boundary_0_5s_after"]["exit"] == 124,
        "a loaded machine meets the requirement": stamps["loaded_machine"]["requirement_met"],
        "the F41 trace (VM clock 2 s behind, a 2 s daemon delay after the launcher's start gate) starts nothing and exits 124": (
            stamps["f41_trace_vm_2s_behind_daemon_delay"]["exec"] is None and stamps["f41_trace_vm_2s_behind_daemon_delay"]["exit"] == 124
        ),
        "a 6 s stall before the boundary check with the VM clock 2 s behind starts nothing": stamps["stall_vm_behind_before_boundary_check"]["exec"] is None
        and stamps["stall_vm_behind_before_boundary_check"]["exit"] == 124,
        "denied TERM and KILL against a live group: exit 123 within E + K + 5 s plus margin, the denial is reported and the group was alive": (
            stamps["signals_denied_live_group"]["exit"] == 123
            and "not proved ended" in stamps["signals_denied_live_group"]["stderr_tail"]
            and stamps["signals_denied_live_group"]["leftover_before_cleanup"]
            and stamps["signals_denied_live_group"]["ended"] is not None
            and stamps["signals_denied_live_group"]["ended"] <= stamps["signals_denied_live_group"]["deadline"] - launcher.SKEW_SECONDS + 55 + 5 + 3
        ),
        "a denied TERM is not fatal: KILL at E + K ends the group and the program exits 124": stamps["term_denied_kill_delivered"]["exit"] == 124
        and stamps["term_denied_kill_delivered"]["requirement_met"]
        and stamps["term_denied_kill_delivered"]["kill"] is not None,
        "EPERM reported by the probe of an already ended group (the macOS zombie condition) is retried until ESRCH and exits 124": stamps[
            "eperm_zombie_group_transient"
        ]["exit"]
        == 124
        and stamps["eperm_zombie_group_transient"]["requirement_met"],
        "no command starts after the cutoff in any case (strict bound, margins recorded in deadline_stamps.json)": all(
            stamp["exec"] is None or stamp["exec"] <= stamp["cutoff"] for name, stamp in stamps.items() if name != "skew_beyond_s"
        ),
        "a skew beyond S is detected by the acceptance test": not stamps["skew_beyond_s"]["requirement_met"],
        "a missing deadline variable exits 125 and starts nothing": missing.returncode == 125,
        "a normal exit returns the command's code": normal.returncode == 7,
        "every TERM-ignoring group was killed": all(s["group_empty"] is not None for s in stamps.values() if s["exec"] is not None),
    }


@case("deadline_spec")
def deadline_spec(env: Env) -> dict[str, bool]:
    """912 and 917: every finite attempt's create specification carries the deadline entrypoint, the deadline variable and an init process."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    case_folder, _ = env.case_folders("a")
    submitted = {
        "fixture_dbt": env.submit(principal, "fixture_dbt", "d1", params=env.fixture_params("a")),
        "fixture_init": env.submit(
            principal,
            "fixture_init",
            "d2",
            [],
            {
                "case_host": str(case_folder),
                "case_container": "/workspace/out/a",
                "database": "/workspace/out/a/f.duckdb",
                "sql_file": "/workspace/out/a/b.sql",
            },
        ),
        "duckdb_helper": env.submit(
            principal,
            "duckdb_helper",
            "d3",
            [],
            {"mounts": [{"host": str(case_folder), "container": "/workspace/in"}], "database": "/workspace/in/f.duckdb", "sql": "select 1"},
        ),
        "spark_e2e": env.submit(principal, "spark_e2e", "d4", [], {"job": "pins_e2e"}),
    }
    for reply in submitted.values():
        env.wait_ended(reply["attempt_id"], 40)
    checks: dict[str, bool] = {}
    for template, reply in submitted.items():
        attempt = env.attempt(reply["attempt_id"])
        body = env.daemon.create_bodies[attempt["container_name"]]
        environment = dict(item.split("=", 1) for item in body["Env"])
        checks[f"{template}: entrypoint, deadline variable and init"] = (
            body["Entrypoint"][0] == launcher.DEADLINE_ENTRYPOINT
            and environment["HAI_DEADLINE_UTC"] == str(attempt["deadline_utc"])
            and body["HostConfig"]["Init"] is True
            and attempt["reason"] == "exited"
        )
    return checks


@case("start_after_deadline_refused")
def start_after_deadline_refused(env: Env) -> dict[str, bool]:
    """912: a start after the cutoff is refused and the attempt ends timed out without a start."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    reply = env.submit(principal, timeout_seconds=3)
    ended = env.wait_ended(reply["attempt_id"])
    return {
        "ended timeout": ended["reason"] == "timeout",
        "never started": not starts(env, "-l1-"),
        "removed": env.labelled() == [],
    }


@case("launcher_enforces_deadline")
def launcher_enforces_deadline(env: Env) -> dict[str, bool]:
    """912: a running launcher stops a job at the deadline (the entrypoint is the backstop)."""
    hold_forever(env)
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    begun = time.time()
    reply = env.submit(principal, timeout_seconds=7)
    env.wait_phase(reply["attempt_id"], ("started",))
    ended = env.wait_ended(reply["attempt_id"], 40)
    return {
        "ended timeout": ended["reason"] == "timeout",
        "stopped near the deadline": 5 <= time.time() - begun <= 14,
        "the container is gone": env.labelled() == [],
    }


@case("docker_unknown_then_back")
def docker_unknown_then_back(env: Env) -> dict[str, bool]:
    """912: with Docker unreachable the phases are kept and the launcher is degraded; when Docker is back the deadline is enforced."""
    hold_forever(env)
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    reply = env.submit(principal, timeout_seconds=8)
    env.wait_phase(reply["attempt_id"], ("started",))
    env.daemon.restart(6.0, stop_containers=False)
    env.wait(lambda: env.ping().get("status") == "degraded", 10, "degraded")
    phase_while_down = env.phase(reply["attempt_id"])
    env.wait(lambda: env.ping().get("status") == "serving", 30, "serving again")
    ended = env.wait_ended(reply["attempt_id"], 40)
    return {
        "degraded while Docker did not answer": True,
        "the phase was kept while Docker was unknown": phase_while_down == "started",
        "the deadline passed while Docker was away and is enforced after": ended["reason"] == "timeout",
        "the container is gone": env.labelled() == [],
    }


@case("removal_failure_and_restarts")
def removal_failure_and_restarts(env: Env) -> dict[str, bool]:
    """912: a failed removal is retried; a launcher restart at the stop and at the remove step still ends in `ended` with the container gone."""
    hold_forever(env)
    env.start()
    client = env.client()
    run = env.acquire_run("run_a", "transform")
    first = env.submit(run, launch="f1", params=env.fixture_params("a"))["attempt_id"]
    env.wait_phase(first, ("started",))
    attempt_alloc = env.attempt(first)["attempt_alloc"]
    env.daemon.fail_remove = 2
    client.call("cancel", {"attempt_id": first}, run)
    ended_first = env.wait_ended(first, 30)
    checks = {
        "a failed removal was retried and the attempt ended": ended_first["reason"] == "cancelled" and env.labelled() == [],
        "the attempt allocation was released at cleanup": env.state()["allocations"][attempt_alloc]["state"] == "released",
    }
    second = env.submit(run, launch="f2", params=env.fixture_params("b"))["attempt_id"]
    env.wait_phase(second, ("started",))
    stop_gate = env.daemon.add_gate("stop", "before")
    client.call("cancel", {"attempt_id": second}, run)
    stop_gate.wait_reached()
    env.kill_launcher()
    stop_gate.release()
    env.start()
    env.wait_ended(second, 30)
    checks["restart at the stop step: ended, container gone"] = env.labelled() == [] and env.attempt(second)["reason"] == "cancelled"
    third = env.submit(run, launch="f3", params=env.fixture_params("c"))["attempt_id"]
    env.wait_phase(third, ("started",))
    remove_gate = env.daemon.add_gate("remove", "before")
    client.call("cancel", {"attempt_id": third}, run)
    remove_gate.wait_reached()
    env.kill_launcher()
    remove_gate.release()
    env.start()
    env.wait_ended(third, 30)
    checks["restart at the remove step: ended, container gone"] = env.labelled() == [] and env.attempt(third)["reason"] == "cancelled"
    return checks


def restart_at_cleanup_step(env: Env, run: dict[str, Any], launch: str, gate_op: str, when: str, after_remove: bool = False) -> bool:
    """One attempt cancelled; the launcher is killed at a daemon step of its cleanup and restarted; the attempt must still end cancelled, container gone."""
    attempt_id = env.submit(run, launch=launch, params=env.fixture_params(launch))["attempt_id"]
    env.wait_phase(attempt_id, ("started",))
    remove_after = env.daemon.add_gate("remove", "after") if after_remove else None
    gate = env.daemon.add_gate(gate_op, when)
    env.client().call("cancel", {"attempt_id": attempt_id}, run)
    if remove_after is not None:
        remove_after.wait_reached()
        # The removal is applied and its reply is withheld: the next listing of the registry's containers is the by-name confirmation.
        gate = env.daemon.add_gate("list", "after", match=lambda ctx: any(label.startswith("hai.registry_id=") for label in ctx["labels"]))
        remove_after.release()
    gate.wait_reached(30)
    env.kill_launcher()
    gate.release()
    env.start()
    ended = env.wait_ended(attempt_id, 40)
    return ended["reason"] == "cancelled" and env.labelled() == [] and ended["cleanup"] is not None


@case("cleanup_restart_each_step")
def cleanup_restart_each_step(env: Env) -> dict[str, bool]:
    """912 (F17): a launcher restart at the stop, collect, remove (before and after its effect) and confirm steps of a cleanup still ends cleanly."""
    hold_forever(env)
    env.start()
    run = env.acquire_run("run_a", "transform")
    return {
        "restart at the stop step": restart_at_cleanup_step(env, run, "r1", "stop", "before"),
        "restart at the collect step (the exit record is read)": restart_at_cleanup_step(env, run, "r2", "logs", "before"),
        "restart at the remove step before its effect": restart_at_cleanup_step(env, run, "r3", "remove", "before"),
        "restart at the remove step after its effect with the reply lost": restart_at_cleanup_step(env, run, "r4", "remove", "after"),
        "restart at the confirm step (the by-name listing)": restart_at_cleanup_step(env, run, "r5", "remove", "after", after_remove=True),
    }


@case("peer_user_rejection")
def peer_user_rejection(env: Env) -> dict[str, bool]:
    """913 (F17): a connection from another user is refused. The test registry names another user ID as the launcher's own, so every peer is a stranger."""
    env.settings["expected_uid"] = os.getuid() + 1
    env.write_settings()
    config = env.config
    client = launcher.LauncherClient(config)
    client.start_launcher()
    log = config.registry.path / "launcher.log"
    pid = int(env.wait(lambda: re.search(r"launcher process (\d+) holds the registry lock", log.read_text()), 30, "the launcher starts").group(1))
    env.wait(lambda: config.socket_path.exists(), 30, "the socket exists")
    outcome: BaseException | None = None
    try:
        client.exchange(client.open_socket(), "ping", {}, {"kind": "any"}, "peer-1", 5)
    except (launcher.ClientError, OSError) as error:
        outcome = error
    env.wait(lambda: "rejected a connection from user" in log.read_text(), 10, "the rejection is logged")
    rejected = log.read_text()
    # The start-up probe runs beside the rejection; a kill before its container is removed would leave that container behind.
    env.wait(
        lambda: (
            any(a.get("probe") for a in env.state()["attempts"].values())
            and not any(launcher.is_open(a) for a in env.state()["attempts"].values() if a.get("probe"))
            and env.labelled() == []
        ),
        30,
        "the start-up probe has ended and its container is removed",
    )
    os.kill(pid, signal.SIGKILL)
    env.wait(lambda: not env.alive(pid), 10, "the launcher is stopped")
    config.socket_path.unlink(missing_ok=True)
    return {
        "the stranger's request gets no reply": outcome is not None,
        "the launcher logs the rejected user and process": f"user {os.getuid()} " in rejected and "serves user" in rejected,
        "no request was recorded": not env.state()["requests"],
    }


@case("image_without_entrypoint_refused")
def image_without_entrypoint_refused(env: Env) -> dict[str, bool]:
    """917: a template whose image does not carry the deadline entrypoint is not accepted."""
    env.start()
    compose = launcher.ComposeModel(ROOT / "docker-compose.yaml")
    env.daemon.images[compose.image("analytics-dbt")]["Config"]["Labels"] = {}
    alloc = env.allocate()
    reply = env.submit(env.alloc_principal(alloc))
    ended = env.wait_ended(reply["attempt_id"])
    return {
        "the attempt ended create_failed": ended["reason"] == "create_failed",
        "the reason names the missing entrypoint": "image_without_entrypoint" in ended["detail"],
        "no create was sent": not creates(env, "-l1-"),
    }


@case("method_matrix")
def method_matrix(env: Env) -> dict[str, bool]:
    """913: every method called by a principal that may not call it, or with bad credentials, is refused; replay and cross-holder rules."""
    env.start()
    client = env.client()
    alloc = env.allocate()
    run = env.acquire_run("run_a")
    principals = {
        "any": {"kind": "any"},
        "run_wrong_token": {"kind": "run", "run_id": "run_a", "token": "wrong"},
        "run_unknown": {"kind": "run", "run_id": "nobody", "token": "x"},
        "alloc_unknown": {"kind": "alloc", "alloc_id": "al-nope"},
        "run_valid": run,
        "alloc_valid": env.alloc_principal(alloc),
    }
    bad = {"run_wrong_token", "run_unknown", "alloc_unknown"}
    results: dict[str, bool] = {}
    for method, spec in launcher.METHODS.items():
        outcomes = []
        for label, principal in principals.items():
            if principal["kind"] not in spec.kinds or label in bad:
                error = expect_error(partial(client.call, method, {}, principal))
                outcomes.append(refused(error, "principal"))
        if spec.owner_flag:
            valid = next(p for label, p in principals.items() if p["kind"] in spec.kinds and label not in bad)
            outcomes.append(refused(expect_error(partial(client.call, method, {}, valid)), "principal"))
        results[f"{method}: every wrong principal is refused"] = bool(outcomes) and all(outcomes)
    # Valid principals of another owner: the target is named in the request, the credentials are good, the owner is not theirs.
    hold_forever(env)
    run_b = env.acquire_run("run_b", "training")
    mine_run = env.submit(run, launch="mx1", params=env.fixture_params("mx1"))["attempt_id"]
    mine_alloc = env.submit(env.alloc_principal(alloc), launch="mx2", params=env.fixture_params("mx2"))["attempt_id"]
    env.wait_phase(mine_alloc, ("started",))
    env.wait_phase(mine_run, ("queued",))  # the run's attempt waits for the machine, which the allocation holds; it is a valid attempt all the same

    def other(principal: dict[str, Any], method: str, params: dict[str, Any]) -> bool:
        return refused(expect_error(lambda: client.call(method, params, principal)), "principal")

    for method in ("cancel", "logs", "session"):
        results[f"{method}: another run's valid principal is refused on a run's attempt"] = other(run_b, method, {"attempt_id": mine_run})
        results[f"{method}: a valid allocation principal is refused on a run's attempt"] = other(env.alloc_principal(alloc), method, {"attempt_id": mine_run})
    results["status of an attempt: another run's and an allocation's valid principals are refused on a run's attempt"] = other(
        run_b, "status", {"what": "attempt", "id": mine_run}
    ) and other(env.alloc_principal(alloc), "status", {"what": "attempt", "id": mine_run})
    results["status of a run: another run's valid principal is refused"] = other(run_b, "status", {"what": "run", "id": "run_a"})
    results["the attempts of the real owners are untouched"] = env.phase(mine_run) == "queued" and env.phase(mine_alloc) == "started"
    client.call("cancel", {"attempt_id": mine_run}, run)
    client.call("cancel", {"attempt_id": mine_alloc}, env.alloc_principal(alloc))
    env.wait_ended(mine_run)
    env.wait_ended(mine_alloc)
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        state["registrations"]["rg-other"] = {"id": "rg-other", "kind": "reader", "allocation": "al-someone-else", "at": "x"}
    results["deregister_reader on another holder's registration is refused"] = refused(
        expect_error(lambda: client.call("deregister_reader", {"registration_id": "rg-other"}, env.alloc_principal(alloc))), "principal"
    )
    results["the registration was kept"] = "rg-other" in env.state()["registrations"]
    return results


@case("replay_and_token_store")
def replay_and_token_store(env: Env) -> dict[str, bool]:
    """913 and 915: a retried acquire returns the run and the token file path, never the token; a crash before publication is repaired."""
    env.start()
    client = env.client()
    first = client.call("acquire", {"domain": "transform", "run_id": "run_a", "mode": "fixture", "checkout": str(ROOT)}, request_id="acq-1")
    token = first["token"]
    again = client.call("acquire", {"domain": "transform", "run_id": "run_a", "mode": "fixture", "checkout": str(ROOT)}, request_id="acq-1")
    env.kill_launcher()
    env.start()
    after_restart = client.call("acquire", {"domain": "transform", "run_id": "run_a", "mode": "fixture", "checkout": str(ROOT)}, request_id="acq-1")
    changed = expect_error(
        lambda: client.call("acquire", {"domain": "training", "run_id": "run_a", "mode": "fixture", "checkout": str(ROOT)}, request_id="acq-1")
    )
    reused = expect_error(
        lambda: client.call("acquire", {"domain": "transform", "run_id": "run_a", "mode": "fixture", "checkout": str(ROOT)}, request_id="acq-2")
    )
    token_root = env.config.token_root
    stray = token_root / "run_z.token"
    stray.write_text("stale-token-from-a-crash")
    second = client.call("acquire", {"domain": "training", "run_id": "run_z", "mode": "fixture", "checkout": str(ROOT)})
    state = env.state()
    mode = (token_root / "run_a.token").stat().st_mode & 0o777
    folder_mode = token_root.stat().st_mode & 0o777
    texts = json.dumps(state)
    real_path = launcher.real_token_root(state["registry_id"])
    return {
        "the first reply carries the token once": "token" in first and len(token) > 20,
        "a retry returns the run and the token file path, not the token": "token" not in again
        and again["run_id"] == "run_a"
        and again["token_file"] == first["token_file"],
        "the retry after a launcher restart returns the same result": after_restart == again,
        "a changed payload under the same request ID is refused": refused(changed, "replay"),
        "a new request for a held run ID is refused": refused(reused, "refused", "run_a"),
        "the token file holds the token": launcher.read_token_file(token_root, "run_a") == token,
        "the token file is 0600 and its folder 0700": mode == 0o600 and folder_mode == 0o700,
        "a token file left by a crash before publication is replaced": stray.read_text() == second["token"]
        and run_lock.digest(second["token"]) == env.state()["runs"]["run_z"]["token_sha256"],
        "the registry holds only the SHA-256": token not in texts and run_lock.digest(token) in texts,
        "the token root of a test registry is inside the registry's scratch folder": str(token_root).startswith(str(env.temp)),
        "the real token root was not written": not real_path.exists(),
        "HOME is ignored": launcher.account_home() == Path(__import__("pwd").getpwuid(os.getuid()).pw_dir),
    }


@case("request_replay_outcomes")
def request_replay_outcomes(env: Env) -> dict[str, bool]:
    """913 (F9): a retry reproduces a refused or failed asynchronous release, and an interrupted release keeps its payload hash across a restart."""
    env.start()
    client = env.client()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    env.daemon.fail_list = True
    first = expect_error(lambda: client.call("release_allocation", {}, principal, request_id="rel-1"))
    env.daemon.fail_list = False
    replay = expect_error(lambda: client.call("release_allocation", {}, principal, request_id="rel-1"))
    still_held = env.state()["allocations"][alloc]["state"] == "held"
    # An interrupted release: the listing is paused, the launcher dies, the request record must keep its payload hash.
    gate = env.daemon.add_gate("list", "after", match=lambda ctx: any(label.startswith("hai.allocation=") for label in ctx["labels"]))
    outcome: list[Any] = []

    def release() -> None:
        try:
            outcome.append(env.client().call("release_allocation", {}, principal, request_id="rel-2", timeout=10))
        except (launcher.ClientError, OSError) as error:
            outcome.append(error)

    thread = threading.Thread(target=release)
    thread.start()
    gate.wait_reached()
    env.kill_launcher()
    gate.release()
    thread.join(timeout=15)
    env.start()
    record = env.state()["requests"]
    interrupted = [r for r in record.values() if r.get("method") == "release_allocation" and r["state"] == "interrupted"]
    changed = expect_error(lambda: env.client().call("release_allocation", {"note": "another payload"}, principal, request_id="rel-2"))
    same = env.client().call("release_allocation", {}, principal, request_id="rel-2")
    return {
        "a failed listing is refused with code degraded": refused(first, "degraded"),
        "the replay of the failed release is refused again, not returned as success": refused(replay, "degraded"),
        "the allocation stayed held after the failed release": still_held,
        "the interrupted release keeps its record and payload hash after the restart": len(interrupted) == 1 and bool(interrupted[0]["payload_sha256"]),
        "a changed payload under the interrupted request ID is refused": refused(changed, "replay"),
        "the same payload runs again and releases": same == {"state": "released"},
    }


@case("token_store_chain")
def token_store_chain(env: Env) -> dict[str, bool]:
    """915 and 913 (F19): every component of the dedicated token chain and the commitment key file are checked (synthetic chains under the case folder)."""
    base = env.temp / "chains"
    base.mkdir(mode=0o700)

    def chain_error(anchor: Path, root: Path, create: bool = True) -> BaseException | None:
        return expect_error_of(lambda: launcher.private_chain(root, create=create, anchor=anchor))

    anchor = base / "support"
    anchor.mkdir(mode=0o755)
    good = anchor / "hai" / "run_tokens" / "registry"
    created = chain_error(anchor, good)
    modes = [(anchor / "hai").stat().st_mode & 0o777, (anchor / "hai" / "run_tokens").stat().st_mode & 0o777, good.stat().st_mode & 0o777]
    again = chain_error(anchor, good, create=False)
    loose = base / "loose"
    loose.mkdir(mode=0o755)
    (loose / "hai").mkdir(mode=0o755)  # an existing dedicated intermediate that is group and world readable
    loose_error = chain_error(loose, loose / "hai" / "run_tokens" / "registry")
    linked = base / "linked"
    linked.mkdir(mode=0o755)
    (base / "elsewhere").mkdir(mode=0o700)
    (linked / "hai").symlink_to(base / "elsewhere")
    link_error = chain_error(linked, linked / "hai" / "run_tokens" / "registry")
    leaf_link = base / "leaf_link"
    leaf_link.mkdir(mode=0o755)
    (leaf_link / "hai").mkdir(mode=0o700)
    (leaf_link / "hai" / "run_tokens").symlink_to(base / "elsewhere")
    leaf_error = chain_error(leaf_link, leaf_link / "hai" / "run_tokens")
    writable_anchor = base / "writable"
    writable_anchor.mkdir()
    writable_anchor.chmod(0o777)
    writable_error = chain_error(writable_anchor, writable_anchor / "hai")
    outside_error = chain_error(anchor, base / "outside")
    key = launcher.launcher_key(good, anchor)
    key_again = launcher.launcher_key(good, anchor)
    key_file = good / "launcher_hmac_key"
    key_file.chmod(0o644)
    mode_error = expect_error_of(lambda: launcher.launcher_key(good, anchor))
    key_file.chmod(0o600)
    saved = key_file.read_bytes()
    key_file.unlink()
    (good / "target").write_bytes(saved)
    (good / "target").chmod(0o600)
    key_file.symlink_to(good / "target")
    key_link_error = expect_error_of(lambda: launcher.launcher_key(good, anchor))
    key_file.unlink()
    key_file.write_bytes(b"short")
    key_file.chmod(0o600)
    key_size_error = expect_error_of(lambda: launcher.launcher_key(good, anchor))
    token_written = launcher.write_token_file(good, "run_x", "t" * 40, anchor)
    return {
        "a missing chain is created with every component 0700": created is None and modes == [0o700, 0o700, 0o700],
        "an existing valid chain passes without creating": again is None,
        "an existing dedicated intermediate with mode 0755 is refused": isinstance(loose_error, launcher.LauncherError) and "0700" in str(loose_error),
        "a link as an intermediate component is refused": isinstance(link_error, launcher.LauncherError) and "link" in str(link_error),
        "a link as the leaf is refused": isinstance(leaf_error, launcher.LauncherError),
        "a group- and world-writable anchor is refused": isinstance(writable_error, launcher.LauncherError),
        "a root outside the anchor is refused": isinstance(outside_error, launcher.LauncherError),
        "the key is created once and read back": len(key) == 32 and key == key_again,
        "a key file with mode 0644 is refused": isinstance(mode_error, launcher.LauncherError),
        "a key file that is a link is refused": isinstance(key_link_error, launcher.LauncherError),
        "a key file of the wrong size is refused": isinstance(key_size_error, launcher.LauncherError),
        "a token file is written inside the checked chain": token_written.read_text() == "t" * 40,
        "the real token root was not touched": not launcher.real_token_root(env.registry_id()).exists(),
    }


@case("token_not_disclosed")
def token_not_disclosed(env: Env) -> dict[str, bool]:
    """913: a token is in no registry record, log, attempt folder or container specification."""
    env.start()
    client = env.client()
    reply = client.call("acquire", {"domain": "transform", "run_id": "run_a", "mode": "fixture", "checkout": str(ROOT)})
    token = reply["token"]
    run = client.principal_run("run_a")
    attempt_id = env.submit(run)["attempt_id"]
    env.wait_ended(attempt_id)
    client.call("logs", {"attempt_id": attempt_id}, run)
    env.stop_launcher()
    corpus = ""
    for path in list(env.registry.rglob("*")) + list(env.temp.glob("runs/**/*")):
        if path.is_file() and path != env.config.token_root / "run_a.token" and "tokens" not in path.parts:
            corpus += path.read_text(errors="replace")
    corpus += json.dumps(env.daemon.create_bodies) + json.dumps(env.daemon.calls)
    return {
        "the token appears in no file, log or container specification": token not in corpus,
        "the digest is the only trace": run_lock.digest(token) in corpus,
    }


@case("test_registry_allowed_roots")
def test_registry_allowed_roots(env: Env) -> dict[str, bool]:
    """913 (F23): a test registry's ``allowed_roots`` admits a named read folder outside the data folders; its neighbour stays refused, and an
    allowed root never exposes another checkout's secrets folder: directly, through a containing root or through a link."""
    outside = Path(tempfile.mkdtemp(prefix="hai-roots-", dir="/tmp")).resolve()  # outside every data folder, so only the setting admits it
    extra, neighbour = outside / "extra_root", outside / "neighbour"
    extra.mkdir()
    neighbour.mkdir()
    other_lakehouse = outside / "other_checkout" / "data" / "lakehouse"  # another checkout that no worktree list names
    (other_lakehouse / "secrets").mkdir(parents=True)
    link = extra / "link_to_secrets"
    link.symlink_to(other_lakehouse / "secrets")

    def helper(principal: dict[str, Any], launch: str, host: Path) -> BaseException | None:
        return expect_error(
            lambda: env.submit(principal, "duckdb_helper", launch, [], {"mounts": [{"host": str(host), "container": "/workspace/in"}], "sql": "select 1"})
        )

    try:
        env.settings["allowed_roots"] = [str(extra), str(other_lakehouse)]
        env.write_settings()
        env.start()
        principal = env.alloc_principal(env.allocate())
        admitted, refused_neighbour = helper(principal, "m1", extra), helper(principal, "m2", neighbour)
        containing = helper(principal, "m3", other_lakehouse)
        direct = helper(principal, "m4", other_lakehouse / "secrets")
        through_link = helper(principal, "m5", link)
    finally:
        shutil.rmtree(outside, ignore_errors=True)
    return {
        "a named allowed root is admitted": admitted is None,
        "a folder outside the named root is still refused": refused(refused_neighbour, "args"),
        "an allowed root that contains another checkout's secrets folder is refused": refused(containing, "mount"),
        "that secrets folder itself is refused": refused(direct, "mount"),
        "a link inside an allowed root that leads to the secrets folder is refused": refused(through_link, "mount"),
    }


def lock_free(registry: Path) -> bool:
    """True when no process holds the registry's launcher lock."""
    lock = registry / "launcher.lock"
    if not lock.exists():
        return True
    with lock.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(handle, fcntl.LOCK_UN)
    return True


@case("kit_launcher_ownership", stub_host=True)
def kit_launcher_ownership(env: Env) -> dict[str, bool]:
    """919 (F24, deltas 3 to 5): the kit stops only a launcher this run started, proves that process and its lock are gone and stops it after an
    allocation failure too; a failed ping is never taken as ownership or as proof of absence; its shutdown request reaches only that instance."""
    from scripts.lakehouse import run_package_fixture_e2e as kit

    # Methods and kit functions are replaced with mock.patch.object, which restores them on exit (no suppression comment is needed).
    saved = (sys.argv, kit.scenarios, kit.hold_machine)
    sys.argv = ["run_package_fixture_e2e", "--test-registry", str(env.registry)]
    observed: dict[str, Any] = {}

    def run_main() -> BaseException | None:
        try:
            kit.main()
        except Exception as error:  # the allocation-failure scenario raises on purpose
            return error
        return None

    try:
        # A: a launcher already serves; its first ping fails (unavailable). The kit must not take it as its own.
        before = env.start()
        failed_once: list[bool] = []
        real_call = launcher.LauncherClient.call

        def first_ping_fails(self: launcher.LauncherClient, method: str, *args: Any, **kwargs: Any) -> Any:
            if method == "ping" and not failed_once:
                failed_once.append(True)
                raise launcher.ClientError("synthetic: the first ping is lost", "unavailable")
            return real_call(self, method, *args, **kwargs)

        kit.scenarios = lambda: 0
        with mock.patch.object(launcher.LauncherClient, "call", first_ping_fails):
            observed["a_error"] = run_main()
        observed["a_kept"] = env.alive(before) and env.ping().get("pid") == before
        env.stop_launcher()
        env.wait(lambda: lock_free(env.registry), 15, "the first launcher lock is free")

        # B: the kit starts its own launcher; after the allocation is released its socket disappears while the process lives. Absence must be
        # proved, not inferred from an unanswered socket.
        def note_pid() -> int:
            observed["b_pid"] = int(env.ping()["pid"])
            return 0

        real_release = launcher.LauncherClient.release_allocation

        def release_then_lose_socket(self: launcher.LauncherClient, alloc_id: str) -> None:
            real_release(self, alloc_id)
            (env.registry / "launcher.sock").unlink()

        kit.scenarios = note_pid
        with mock.patch.object(launcher.LauncherClient, "release_allocation", release_then_lose_socket):
            observed["b_error"] = run_main()
        observed["b_gone"] = not env.alive(observed["b_pid"]) and lock_free(env.registry)
        if env.alive(observed["b_pid"]):  # an unfixed kit leaves it; scenario C needs a free registry
            os.kill(observed["b_pid"], signal.SIGKILL)
            env.wait(lambda: lock_free(env.registry), 15, "the leftover launcher lock is free")

        # C: allocation fails after this run started the launcher; the launcher is still stopped.
        def start_then_fail() -> str:
            launcher.load_client(str(env.registry)).connect().close()
            observed["c_pid"] = int(env.ping()["pid"])
            raise launcher.ClientError("synthetic allocation failure", "budget")

        kit.hold_machine = start_then_fail
        observed["c_error"] = run_main()
        observed["c_gone"] = "c_pid" in observed and not env.alive(observed["c_pid"]) and lock_free(env.registry)
        if "c_pid" in observed and env.alive(observed["c_pid"]):
            os.kill(observed["c_pid"], signal.SIGKILL)
            env.wait(lambda: lock_free(env.registry), 15, "the leftover launcher lock is free")

        # D (delta recheck 4): another client starts a launcher between the kit's free-lock check and its own start. The kit must treat
        # that launcher as not its own and leave it serving.
        kit.hold_machine = saved[2]
        kit.scenarios = lambda: 0
        real_lock_free = kit.launcher_lock_free

        def lock_free_then_competitor() -> bool:
            free = real_lock_free()
            if "d_pid" not in observed:  # the first check only: the competitor starts right after it
                observed["d_pid"] = env.start()
            return free

        with mock.patch.object(kit, "launcher_lock_free", lock_free_then_competitor):
            observed["d_error"] = run_main()
        observed["d_kept"] = "d_pid" in observed and env.alive(observed["d_pid"]) and env.ping().get("pid") == observed["d_pid"]
        env.stop_launcher()
        env.wait(lambda: lock_free(env.registry), 15, "the competitor's lock is free")

        # E (delta recheck 5): after the kit's preflight ping names its own launcher, that launcher stops and a replacement another client
        # starts takes the registry. The shutdown request must not stop the replacement, because the kit did not start it.
        real_ping, real_stop = kit.ping, kit.stop_owned_launcher
        stopping: list[bool] = []

        def ping_then_replace() -> dict[str, Any]:
            reply = real_ping()
            if stopping and "e_pid" not in observed and reply:
                observed["e_own"] = int(reply["pid"])
                env.stop_launcher()
                env.wait(lambda: lock_free(env.registry), 15, "the kit's launcher lock is free")
                observed["e_pid"] = env.start()
            return reply

        def stop_marked(*args: Any) -> None:
            stopping.append(True)
            real_stop(*args)

        with mock.patch.object(kit, "ping", ping_then_replace), mock.patch.object(kit, "stop_owned_launcher", stop_marked):
            observed["e_error"] = run_main()
        observed["e_kept"] = "e_pid" in observed and env.alive(observed["e_pid"]) and env.ping().get("pid") == observed["e_pid"]
        env.stop_launcher()
    finally:
        sys.argv, kit.scenarios, kit.hold_machine = saved[0], saved[1], saved[2]
        for key in ("b_pid", "c_pid", "d_pid", "e_pid"):
            if key in observed and env.alive(observed[key]):
                os.kill(observed[key], signal.SIGKILL)
    return {
        "a pre-existing launcher survives a run whose first ping failed": observed["a_error"] is None and bool(observed.get("a_kept")),
        "the run's own launcher is proved gone (process and lock) though its socket vanished": observed.get("b_error") is None and bool(observed.get("b_gone")),
        "an allocation failure still stops the run's own launcher": refused(observed.get("c_error"), "budget") and bool(observed.get("c_gone")),
        "a launcher another client started after the free-lock check is not taken as the run's own": observed.get("d_error") is None
        and bool(observed.get("d_kept")),
        "a replacement launcher that takes the registry after the preflight ping is not shut down": observed.get("e_error") is None
        and bool(observed.get("e_kept")),
    }


@case("mount_containment_depth_and_case")
def mount_containment_depth_and_case(env: Env) -> dict[str, bool]:
    """913 (F23, deltas 3 to 5): a named root that holds an unlisted checkout's secrets folder at any depth, in another letter case, behind a link
    named secrets or below an alias of its lakehouse folder is refused; a clean named root is still admitted. A link loop under a named root cannot be
    checked, so the mount is refused as a mount, not ended by an error of the check."""
    outside = Path(tempfile.mkdtemp(prefix="hai-depth-", dir="/tmp")).resolve()
    clean, deep, cased = outside / "clean", outside / "deep", outside / "cased"
    clean.mkdir()
    (deep / "unlisted" / "data" / "lakehouse" / "secrets").mkdir(parents=True)  # a checkout no worktree list names, two levels below the root
    mixed = cased / "case" / "Data" / "Lakehouse" / "Secrets"
    mixed.mkdir(parents=True)
    linked = outside / "linked"
    (linked / "unlisted" / "data" / "lakehouse" / "vault").mkdir(parents=True)
    (linked / "unlisted" / "data" / "lakehouse" / "secrets").symlink_to("vault")  # relative: it stays usable inside one bind mount
    aliased = outside / "aliased"
    (aliased / "unlisted" / "data" / "lakehouse" / "secrets").mkdir(parents=True)
    (aliased / "unlisted" / "data" / "a").symlink_to("lakehouse")  # sorts before the real folder, so the scan reaches the lakehouse inode through it first
    looped = outside / "looped"
    (looped / "data").mkdir(parents=True)
    (looped / "data" / "a").symlink_to("b")
    (looped / "data" / "b").symlink_to("a")
    source_loop = outside / "source_loop"
    source_loop.symlink_to("source_loop")
    below_alias = outside / "below_alias"
    (below_alias / "target").mkdir(parents=True)
    (below_alias / "a").symlink_to("target")
    (below_alias / "target" / "cycle").symlink_to("cycle")
    token_loop = outside / "token_loop"
    token_loop.symlink_to("token_loop")

    def mount_refusal(call: Callable[[], Any]) -> bool:
        """True only for the mount refusal: a loop that escapes as RuntimeError, or no refusal at all, is not it."""
        try:
            call()
        except launcher.LauncherError as error:
            return error.code == "mount"
        except RuntimeError:
            return False
        return False

    def helper(principal: dict[str, Any], launch: str, host: Path) -> BaseException | None:
        return expect_error(
            lambda: env.submit(principal, "duckdb_helper", launch, [], {"mounts": [{"host": str(host), "container": "/workspace/in"}], "sql": "select 1"})
        )

    try:
        env.settings["allowed_roots"] = [str(clean), str(deep), str(cased), str(linked), str(aliased), str(looped)]
        env.write_settings()
        env.start()
        principal = env.alloc_principal(env.allocate())
        results = {
            "a clean named root is admitted": helper(principal, "d1", clean) is None,
            "a named root holding an unlisted checkout's secrets folder deeper down is refused": refused(helper(principal, "d2", deep), "mount"),
            "a named root holding a secrets folder in other letter case is refused": refused(helper(principal, "d3", cased), "mount"),
            "the other-case secrets folder itself is refused": refused(helper(principal, "d4", mixed), "mount"),
            "its other-case lakehouse folder is refused": refused(helper(principal, "d5", mixed.parent), "mount"),
            # Delta recheck 4 (F23): a relative symlink named secrets inside the mounted tree, and that link given as the source itself.
            "a named root holding a symlinked secrets folder is refused": refused(helper(principal, "d6", linked), "mount"),
            "the symlinked secrets folder given as the source is refused": refused(
                helper(principal, "d7", linked / "unlisted" / "data" / "lakehouse" / "secrets"), "mount"
            ),
            # Delta recheck 5 (F23): a directory alias of lakehouse that sorts before the real folder must not hide its secrets child.
            "a named root whose lakehouse alias sorts before the real folder is refused": refused(helper(principal, "d8", aliased), "mount"),
            "a named root holding a symlink loop is refused as a mount": refused(helper(principal, "d9", looped), "mount"),
            "the launcher still admits a clean named root after the loop": helper(principal, "d10", clean) is None,
            "a host path that is itself a symlink loop is refused as a mount": refused(helper(principal, "d11", source_loop), "mount"),
        }
        renderer = launcher.Renderer(env.config, launcher.ComposeModel(ROOT / "docker-compose.yaml"), ROOT, ROOT, b"k" * 32)

        def check(source: Path, renderer: launcher.Renderer = renderer) -> bool:
            return mount_refusal(lambda: renderer.check_mounts([launcher.Mount(str(source), "/workspace/in", True)]))

        def admitted(source: Path) -> bool:
            try:
                renderer.check_mounts([launcher.Mount(str(source), "/workspace/in", True)])
            except (launcher.LauncherError, RuntimeError):
                return False
            return True

        loop_root = launcher.Renderer(replace(env.config, allowed_roots=(source_loop,)), renderer.compose, ROOT, ROOT, b"k" * 32)
        loop_token = launcher.Renderer(replace(env.config, token_root=token_loop), renderer.compose, ROOT, ROOT, b"k" * 32)
        results |= {
            "check_mounts control: a clean source is admitted": admitted(clean),
            "check_mounts: a source that is itself a loop is refused as a mount": check(source_loop),
            "check_mounts: a loop below the source is refused as a mount": check(looped),
            "check_mounts: a loop below an alias inside the source is refused as a mount": check(below_alias),
            "check_mounts: a loop at the token root is refused as a mount": mount_refusal(
                lambda: loop_token.check_mounts([launcher.Mount(str(clean), "/workspace/in", True)])
            ),
            "allowed: a requested path that is a loop is refused as a mount": mount_refusal(lambda: renderer.allowed(str(source_loop), "case_host")),
            "allowed: an allowed root that is a loop is refused as a mount": mount_refusal(lambda: loop_root.allowed(str(clean), "case_host")),
        }
    finally:
        shutil.rmtree(outside, ignore_errors=True)
    return results


@case("mount_and_argument_refusals")
def mount_and_argument_refusals(env: Env) -> dict[str, bool]:
    """913 and 916: mounts that contain the token root or a secrets folder, other folders and every dbt option outside the closed schema are refused."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    case_folder, packages = env.case_folders("a")

    def helper(host: str, container: str = "/workspace/in") -> BaseException | None:
        return expect_error(lambda: env.submit(principal, "duckdb_helper", "m", [], {"mounts": [{"host": host, "container": container}], "sql": "select 1"}))

    token_root = env.config.token_root
    secrets_folder = ROOT / "data" / "lakehouse" / "secrets"
    results = {
        "the token root is refused": refused(helper(str(token_root)), "args") or refused(helper(str(token_root)), "mount"),
        "a secrets folder is refused": refused(helper(str(secrets_folder)), "mount"),
        "a folder that contains a secrets folder is refused": refused(helper(str(ROOT / "data")), "mount")
        and refused(helper(str(ROOT / "data" / "lakehouse")), "mount"),
        "a folder outside the data folder is refused": refused(helper("/etc"), "args"),
        "a container path with traversal is refused": refused(helper(str(case_folder), "/workspace/../etc"), "args"),
        "compose.env is never mounted": refused(helper(str(env.temp / "compose.env")), "args"),
    }
    base = {"build": ["build"]}
    del base

    def dbt(*args: str) -> BaseException | None:
        return expect_error(lambda: env.submit(principal, "fixture_dbt", "a", list(args), env.fixture_params("a")))

    forbidden = {
        "--vars separate": ["build", "--vars", "{x: 1}"],
        "--vars attached": ["build", "--vars={x: 1}"],
        "--target": ["build", "--target", "lakehouse"],
        "--profiles-dir attached": ["build", "--profiles-dir=/x"],
        "--project-dir": ["build", "--project-dir", "/x"],
        "--threads": ["build", "--threads", "99"],
        "--store-failures on fixture_dbt": ["build", "--store-failures"],
        "deps": ["deps"],
        "run-operation": ["run-operation", "anything"],
        "shell metacharacters": ["build", "--select", "a;rm -rf /"],
        "unknown command": ["shell"],
        "--select without value": ["build", "--select"],
        "-s attached with =": ["build", "-s=x"],
        "ls output option on build": ["build", "--output", "json"],
    }
    for label, args in forbidden.items():
        results[f"dbt {label} is refused"] = refused(dbt(*args), "args")
    accepted = [
        ["build", "--select", "+a", "b", "--exclude", "c", "--indirect-selection", "cautious", "--fail-fast"],
        ["ls", "--select", "x", "--output", "json", "--output-keys", "unique_id", "--quiet"],
        ["test", "-s", "a"],
    ]
    # Valid submits are admitted by the launcher itself (distinct launch IDs, a real attempt each), not only by the argument validator.
    admitted = []
    for index, args in enumerate(accepted):
        reply = expect_error(partial(env.submit, principal, "fixture_dbt", f"ok{index}", list(args), env.fixture_params(f"ok{index}")))
        admitted.append(reply is None)
    results["the schema's own forms are admitted by a real submit"] = all(admitted) and launcher.validate_dbt_args("fixture_dbt", accepted[0]) == accepted[0]
    for args in accepted[1:]:
        results[f"accepted: {args[0]}"] = launcher.validate_dbt_args("fixture_dbt", args) == args
    results["--store-failures is allowed for build on real_dbt only"] = launcher.validate_dbt_args("real_dbt", ["build", "--store-failures"]) == [
        "build",
        "--store-failures",
    ]
    unknown_param = expect_error(lambda: env.submit(principal, "fixture_dbt", "u", ["build"], {**env.fixture_params("a"), "extra": 1}))
    results["a parameter the template does not define is refused"] = refused(unknown_param, "args")
    free_vars = expect_error(lambda: env.submit(principal, "fixture_dbt", "v", ["build"], {**env.fixture_params("a"), "dbt_vars": {"anything": True}}))
    results["dbt_vars outside the launcher's list are refused"] = refused(free_vars, "args")
    del packages
    return results


@case("mount_fence_other_checkouts")
def mount_fence_other_checkouts(env: Env) -> dict[str, bool]:
    """913 and 916 (F4): the secrets of every checkout are protected, also through a link and through the exact credential path (synthetic checkouts)."""
    main = env.temp / "main_checkout"
    other = main / "data" / "worktrees" / "other"
    mine = main / "data" / "worktrees" / "mine"
    for checkout in (main, other, mine):
        (checkout / "data" / "lakehouse" / "secrets").mkdir(parents=True)
        (checkout / "data" / "lakehouse" / "secrets" / "catalog.env").write_text("synthetic\n")
    compose = launcher.ComposeModel(ROOT / "docker-compose.yaml")
    renderer = launcher.Renderer(env.config, compose, mine, main, b"k" * 32)
    shared = main / "data" / "shared"
    shared.mkdir()
    link = shared / "innocent"
    link.symlink_to(other / "data" / "lakehouse" / "secrets")
    fake_home = env.temp / "fake_home"
    fake_home.mkdir()
    (fake_home / ".aws").symlink_to(other / "data" / "lakehouse" / "secrets")
    plain_home = env.temp / "plain_home"
    (plain_home / ".aws").mkdir(parents=True)

    def check(source: Path, read_only: bool = True) -> BaseException | None:
        return expect_error_of(lambda: renderer.check_mounts([launcher.Mount(str(source), "/x", read_only)]))

    with mock.patch.object(launcher, "account_home", lambda: fake_home):
        linked_credential = check(fake_home / ".aws")
    with mock.patch.object(launcher, "account_home", lambda: plain_home):
        plain_credential = check(plain_home / ".aws")
        writable_credential = check(plain_home / ".aws", read_only=False)
    return {
        "the secrets of another worktree are refused": isinstance(check(other / "data" / "lakehouse" / "secrets"), launcher.LauncherError),
        "a folder inside another worktree's secrets is refused": isinstance(
            check(other / "data" / "lakehouse" / "secrets" / "catalog.env"), launcher.LauncherError
        ),
        "a folder that contains another worktree's secrets is refused": isinstance(check(other / "data"), launcher.LauncherError)
        and isinstance(check(other / "data" / "lakehouse"), launcher.LauncherError)
        and isinstance(check(main / "data" / "worktrees"), launcher.LauncherError),
        "the main checkout's and this checkout's secrets are refused": isinstance(check(main / "data" / "lakehouse" / "secrets"), launcher.LauncherError)
        and isinstance(check(mine / "data" / "lakehouse" / "secrets"), launcher.LauncherError),
        "a link that leads into another checkout's secrets is refused": isinstance(check(link), launcher.LauncherError),
        "the exact credential path is refused when it is a link into a secrets folder": isinstance(linked_credential, launcher.LauncherError),
        "a plain read-only credential folder is accepted": plain_credential is None,
        "a writable credential mount is refused": isinstance(writable_credential, launcher.LauncherError),
        "an unrelated folder is accepted": check(shared) is None,
    }


def real_run(env: Env, run_id: str, domain: str, **extra: Any) -> dict[str, Any]:
    """A held run in the given domain that the registry file records as a real run (a test registry refuses to acquire one)."""
    principal = env.acquire_run(run_id, domain, "fixture", **extra)
    env.make_real(run_id)
    return principal


@case("template_owner_predicates")
def template_owner_predicates(env: Env) -> dict[str, bool]:
    """916 (F2): each template's run domain, run mode and registration predicates hold at submit and again at dispatch."""
    hold_forever(env)
    catalog_script(env)
    env.start()
    client = env.client()
    catalog_up(env)

    def submit(principal: dict[str, Any], template: str, launch: str, params: dict[str, Any] | None = None, args: list[str] | None = None) -> dict[str, Any]:
        return env.submit(principal, template, launch, args if args is not None else [], params if params is not None else {})

    checks: dict[str, bool] = {}
    for domain in ("training", "serving"):
        run = real_run(env, f"run_{domain}", domain)
        error = expect_error(partial(submit, run, "spark_write", "b", {"job": "bronze"}, ["--group", "hai"]))
        checks[f"spark_write bronze under a {domain} run is refused"] = refused(error, "refused", "domain acquisition")
        client.call("finalize", {}, run)
        client.call("release", {"record": str(record_file(env, f"run_{domain}"))}, run, timeout=60)
    fixture = env.acquire_run("run_fix", "acquisition", "fixture", write_prefixes=["lakehouse/bronze/"])
    checks["spark_write under a fixture run is refused"] = refused(
        expect_error(lambda: submit(fixture, "spark_write", "b", {"job": "bronze"}, ["--group", "hai"])), "refused", "mode real"
    )
    client.call("finalize", {}, fixture)
    client.call("release", {"record": str(record_file(env, "run_fix"))}, fixture, timeout=60)
    bare = real_run(env, "run_bare", "acquisition")
    checks["spark_write without declared write prefixes is refused"] = refused(
        expect_error(lambda: submit(bare, "spark_write", "b", {"job": "bronze"}, ["--group", "hai"])), "refused", "write prefixes"
    )
    client.call("finalize", {}, bare)
    client.call("release", {"record": str(record_file(env, "run_bare"))}, bare, timeout=60)
    wrong_prefix = real_run(env, "run_prefix", "acquisition", write_prefixes=["lakehouse/other/"])
    checks["a bronze job under a run that declares another prefix is refused"] = refused(
        expect_error(lambda: submit(wrong_prefix, "spark_write", "b", {"job": "bronze"}, ["--group", "hai"])), "refused", "declares"
    )
    client.call("finalize", {}, wrong_prefix)
    client.call("release", {"record": str(record_file(env, "run_prefix"))}, wrong_prefix, timeout=60)
    good = real_run(env, "run_good", "acquisition", write_prefixes=["lakehouse/bronze/"])
    accepted = expect_error(lambda: submit(good, "spark_write", "b", {"job": "bronze"}, ["--group", "hai"]))
    checks["spark_write bronze under an acquisition real run that declares its prefix is accepted"] = accepted is None
    env.wait(lambda: any(a["owner"] == "run:run_good" and a["phase"] == "started" for a in env.state()["attempts"].values()), 20, "the writer starts")
    for attempt_id, attempt in env.state()["attempts"].items():
        if attempt["owner"] == "run:run_good":
            client.call("cancel", {"attempt_id": attempt_id}, good)
            env.wait_ended(attempt_id)
    client.call("finalize", {}, good)
    client.call("release", {"record": str(record_file(env, "run_good"))}, good, timeout=60)
    real_transform = real_run(env, PINS_RUN, "transform")
    checks["fixture_dbt under a real run is refused"] = refused(
        expect_error(lambda: env.submit(real_transform, "fixture_dbt", "f", params=env.fixture_params("f"))), "refused", "mode fixture"
    )
    checks["spark_read under a transform run is accepted"] = (
        expect_error(lambda: submit(real_transform, "spark_read", "p", {"job": "pins"}, ["pin", "--run", PINS_RUN])) is None
    )
    for attempt_id, attempt in env.state()["attempts"].items():
        if attempt["owner"] == f"run:{PINS_RUN}" and attempt["phase"] != "ended":
            client.call("cancel", {"attempt_id": attempt_id}, real_transform)
            env.wait_ended(attempt_id)
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    checks["query_reader without a reader registration is refused"] = refused(
        expect_error(lambda: submit(principal, "query_reader", "q", {"tty": False})), "refused", "reader registration"
    )
    checks["ui_reader without a reader registration is refused"] = refused(
        expect_error(lambda: submit(principal, "ui_reader", "u")), "refused", "reader registration"
    )
    registration = client.call("register_reader", {}, principal)["registration_id"]
    ui = submit(principal, "ui_reader", "u")["attempt_id"]
    env.wait_phase(ui, ("started",))
    client.call("deregister_reader", {"registration_id": registration}, principal)
    ended = env.wait_ended(ui, 30)
    checks["a reader attempt ends fenced when its registration is removed"] = (
        ended["reason"] == "fenced" and env.labelled() == [] or (ended["reason"] == "fenced")
    )
    return checks


@case("real_dbt_refused_until_provenance")
def real_dbt_refused_until_provenance(env: Env) -> dict[str, bool]:
    """914 and the gold plan's 751: the frozen provenance interfaces refuse real_dbt and sealing until the provenance package fills them."""
    env.start()
    catalog_script(env)
    catalog_up(env)
    run = env.acquire_run(PINS_RUN, "transform")
    env.make_real(PINS_RUN)
    error = expect_error(
        lambda: env.client().call("submit", {"template": "real_dbt", "launch_id": "b", "args": ["build"], "params": {}, "checkout": str(ROOT)}, run)
    )
    refusal = None
    try:
        launcher.seal({"run_id": "x"})
    except launcher.LauncherError as exc:
        refusal = exc
    return {
        "a real_dbt submit is refused with the provenance code": refused(error, "provenance"),
        "seal refuses": refusal is not None and refusal.code == "provenance",
        "no container was created for it": not creates(env, "real_dbt"),
    }


def compose_service_mounts(service: str, home: str) -> set[tuple[str, str, bool]]:
    """The bind mounts of a Compose service, read straight from the YAML (independently of the launcher's own reader)."""
    document = importlib.import_module("yaml").safe_load((ROOT / "docker-compose.yaml").read_text())
    mounts = set()
    for volume in document["services"][service].get("volumes") or []:
        if isinstance(volume, str):
            parts = volume.split(":")
            source, target, read_only = parts[0], parts[1], len(parts) > 2 and parts[2] == "ro"
        else:
            source, target, read_only = volume["source"], volume["target"], bool(volume.get("read_only"))
        source = source.replace("${HOME}", home)
        mounts.add((str((ROOT / source).resolve()) if source.startswith(".") else source, target, read_only))
    return mounts


def facts(rendered: launcher.Rendered) -> dict[str, Any]:
    """The properties of a create specification the render checks compare."""
    body = rendered.body
    host = body["HostConfig"]
    return {
        "image": body["Image"],
        "entrypoint": body.get("Entrypoint"),
        "cmd": body["Cmd"],
        "env": dict(item.split("=", 1) for item in body["Env"]),
        "mounts": {(m["Source"], m["Target"], m["ReadOnly"]) for m in host["Mounts"]},
        "network": host["NetworkMode"],
        "memory": host["Memory"],
        "ports": {c: [(b["HostIp"], b["HostPort"]) for b in bs] for c, bs in (host.get("PortBindings") or {}).items()},
        "init": host.get("Init"),
        "body": body,
    }


@case("render_templates")
def render_templates(env: Env) -> dict[str, bool]:
    """916: each template's effective specification against docker-compose.yaml and the template table (fake Docker; nothing is started)."""
    config = env.config
    compose = launcher.ComposeModel(ROOT / "docker-compose.yaml")
    renderer = launcher.Renderer(config, compose, ROOT, ROOT, b"k" * 32)
    registry_id = json.loads((env.registry / "registry_id").read_text())["registry_id"]
    network = f"hai-{registry_id[:8]}-net"
    home = str(launcher.account_home())
    values = dict(launcher.Launcher.PLACEHOLDER_ENV)
    case_folder, packages = env.case_folders("rc")
    run_folder = env.work / "runs" / PINS_RUN
    snapshot = env.work / "snapshot"
    for folder in (run_folder, snapshot / "project", snapshot / "packages"):
        folder.mkdir(parents=True, exist_ok=True)
    resources = (str(ROOT / "services/analytics/resources.sql"), "/opt/analytics/resources.sql", True)
    secrets_file = {line.split("=", 1)[0]: line.split("=", 1)[1] for line in (env.temp / "compose.env").read_text().splitlines()}

    def render(template: str, args: list[str], params: dict[str, Any], **extra: Any) -> dict[str, Any]:
        inp = launcher.RenderInput(
            template, "owner", "l1", 1, 1, "op-1", values, int(time.time()) + 600, args, params, {"hai.registry_id": registry_id}, **extra
        )
        if template == "catalog_service":
            return facts(renderer.render_service(inp, params["service"], registry_id))
        return facts(renderer.render(inp, registry_id))

    def wrapped(tail: list[str]) -> list[str]:
        return [launcher.DEADLINE_ENTRYPOINT, *tail]

    out: dict[str, bool] = {}
    f = render("fixture_dbt", ["build"], env.fixture_params("rc"))
    out["fixture_dbt"] = (
        f["image"] == compose.image("analytics-dbt")
        and f["entrypoint"] == wrapped(["dbt"])
        and f["network"] == "none"
        and f["mounts"] == {(str(case_folder.resolve()), "/workspace/out/rc", False), (str(packages.resolve()), "/workspace/out/dbt_packages", True), resources}
        and f["cmd"][-2:] == ["--vars", '{"check_release_copy_conflicts": false}']
        and "--target" in f["cmd"]
        and f["init"] is True
        and not any(name.startswith(("AWS", "POLARIS")) for name in f["env"])
    )
    f = render(
        "fixture_init",
        [],
        {"case_host": str(case_folder), "case_container": "/workspace/out/rc", "database": "/workspace/out/rc/f.duckdb", "sql_file": "/workspace/out/rc/b.sql"},
    )
    out["fixture_init"] = (
        f["entrypoint"] == wrapped(["duckdb"])
        and f["network"] == "none"
        and f["mounts"] == {(str(case_folder.resolve()), "/workspace/out/rc", False), resources}
    )
    f = render(
        "duckdb_helper", [], {"mounts": [{"host": str(case_folder), "container": "/workspace/in"}], "database": "/workspace/in/f.duckdb", "sql": "select 1"}
    )
    out["duckdb_helper"] = (
        f["entrypoint"] == wrapped(["duckdb"])
        and f["network"] == "none"
        and "-readonly" in f["cmd"]
        and all(ro for _, _, ro in f["mounts"])
        and (str(case_folder.resolve()), "/workspace/in", True) in f["mounts"]
    )
    f = render("dbt_deps", ["deps"], {"packages_host": str(packages)})
    from scripts.lakehouse import dbt_package_archive

    archive = dbt_package_archive.verify(ROOT)
    out["dbt_deps: verified local archive with no network"] = (
        f["entrypoint"] == wrapped(["python", "/opt/hai/dbt_package_archive.py"])
        and f["cmd"][0] == "install"
        and f["network"] == "none"
        and (str(archive.parent), "/workspace/archive", True) in f["mounts"]
        and (str(ROOT / "config/lakehouse/dbt_package_archive.json"), "/workspace/archive_record.json", True) in f["mounts"]
        and (str(packages.resolve()), "/workspace/packages", False) in f["mounts"]
        and not any(name.startswith(("AWS", "POLARIS")) for name in f["env"])
        and f["body"]["HostConfig"]["Tmpfs"] == {"/scratch": "rw,mode=1777,size=64m"}
    )
    suites = ROOT / "data_contracts/great_expectations/suites"
    baselines = ROOT / "data_contracts/great_expectations/baselines.json"
    f = render(
        "quality_validate",
        [],
        {
            "database": str(ROOT / "data/analytics/dbt/staging.duckdb"),
            "suites_host": str(suites),
            "baselines_host": str(baselines),
            "out_host": str(case_folder),
        },
    )
    expected = compose_service_mounts("quality", home) | {
        (str(suites.resolve()), "/workspace/suites", True),
        (str(baselines.resolve()), "/workspace/baselines.json", True),
        (str(case_folder.resolve()), "/workspace/out", False),
    }
    out["quality_validate"] = (
        f["image"] == compose.image("quality")
        and f["network"] == "none"
        and f["mounts"] == expected
        and f["entrypoint"] == wrapped(["python", "-m", "scripts.lakehouse.silver_quality"])
        and set(f["env"]) == {"DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "HAI_DEADLINE_UTC"}
    )
    f = render("query_reader", ["-c", "select 1", "-csv"], {"tty": True})
    document = importlib.import_module("yaml").safe_load((ROOT / "docker-compose.yaml").read_text())["services"]
    out["query_reader"] = (
        f["image"] == compose.image("analytics")
        and f["mounts"] == compose_service_mounts("analytics", home)
        and f["network"] == network
        and f["entrypoint"] == ["duckdb"]
        and f["cmd"][:2] == ["-init", "/opt/analytics/bronze.sql"]
        and f["env"]["AWS_PROFILE"] == secrets_file["AWS_PROFILE"]
        and f["env"]["POLARIS_READER_CLIENT_ID"] == secrets_file["POLARIS_READER_CLIENT_ID"]
        and f["body"]["OpenStdin"] is True
        and not any(name == "HAI_DEADLINE_UTC" for name in f["env"])
        and set(document["analytics"]["environment"]) == set(f["env"])
    )
    f = render("ui_reader", [], {})
    out["ui_reader"] = (
        f["mounts"] == compose_service_mounts("analytics-ui", home)
        and f["ports"] == {"4214/tcp": [("127.0.0.1", "4213")]}
        and f["entrypoint"] == ["/opt/analytics/ui.sh"]
    )
    f = render("real_dbt", ["build"], {}, snapshot=snapshot, run_folder=run_folder, provenance={"HAI_BUILD_RUN_ID": PINS_RUN})
    out["real_dbt"] = (
        f["mounts"]
        == {
            (str(snapshot / "project"), "/workspace/dbt", True),
            (str(snapshot / "packages"), "/workspace/packages", True),
            (str(run_folder), "/workspace/out", False),
        }
        | {m for m in compose_service_mounts("analytics-dbt", home) if m[1] not in ("/workspace/dbt", "/workspace/out")}
        and f["network"] == network
        and f["env"]["DBT_PACKAGES_INSTALL_PATH"] == "/workspace/packages"
        and f["env"]["HAI_BUILD_RUN_ID"] == PINS_RUN
        and f["entrypoint"] == wrapped(["dbt"])
        and f["env"]["DBT_ENV_SECRET_POLARIS_CLIENT_ID"] == secrets_file["POLARIS_READER_CLIENT_ID"]
    )
    aws = (f"{home}/.aws", "/aws", True)
    f = render("spark_read", ["pin", "--run", PINS_RUN], {"job": "pins"}, run_folder=run_folder)
    out["spark_read"] = (
        f["mounts"]
        == {
            (str(ROOT / "scripts"), "/workspace/scripts", True),
            (str(ROOT / "config"), "/workspace/config", True),
            aws,
            (str(run_folder), f"/workspace/data/analytics/dbt/runs/{PINS_RUN}", False),
        }
        and f["env"]["POLARIS_CLIENT_ID"] == secrets_file["POLARIS_READER_CLIENT_ID"]
        and f["network"] == network
        and f["entrypoint"] == wrapped(["python", "-m", "scripts.lakehouse.pins"])
    )
    f = render("spark_write", ["--group", "hai"], {"job": "bronze"}, run_folder=run_folder)
    out["spark_write"] = (
        f["env"]["POLARIS_CLIENT_ID"] == secrets_file["POLARIS_CLIENT_ID"]
        and (str(ROOT / "infra/deployment.auto.tfvars.json"), "/workspace/infra/deployment.auto.tfvars.json", True) in f["mounts"]
        and (str(ROOT / "data/e2e/bronze_load"), "/workspace/data/e2e/bronze_load", False) in f["mounts"]
        and (str(ROOT), "/workspace", False) not in f["mounts"]
        and all(source != str(ROOT) for source, _, _ in f["mounts"])
    )
    f = render("spark_e2e", [], {"job": "pins_e2e"}, run_folder=run_folder)
    out["spark_e2e"] = (
        f["network"] == "none"
        and not any(name.startswith(("AWS", "POLARIS")) for name in f["env"])
        and not any(target == "/aws" for _, target, _ in f["mounts"])
    )
    f = render("catalog_service", [], {"service": "polaris-db"})
    out["polaris-db"] = (
        f["memory"] == 256 * 1024**2
        and f["mounts"] == compose_service_mounts("polaris-db", home)
        and f["env"]["POSTGRES_PASSWORD"] == secrets_file["POSTGRES_PASSWORD"]
        and f["body"]["Healthcheck"]["Test"][:2] == ["CMD", "pg_isready"]
        # No entrypoint is sent, so the image's docker-entrypoint.sh runs: the daemon reads an empty list as "clear it" (A83).
        and "Entrypoint" not in f["body"]
        and f["body"]["NetworkingConfig"]["EndpointsConfig"][network]["Aliases"] == ["polaris-db"]
    )
    f = render("catalog_service", [], {"service": "polaris"})
    out["polaris"] = (
        f["ports"] == {"8181/tcp": [("127.0.0.1", "8181")], "8182/tcp": [("127.0.0.1", "8182")]}
        and f["mounts"] == compose_service_mounts("polaris", home)
        and f["memory"] == 1024 * 1024**2
    )
    f = render("catalog_service", [], {"service": "polaris-bootstrap"}, image_entrypoint=("/opt/polaris/entry",))
    out["polaris-bootstrap"] = (
        f["entrypoint"] == [launcher.DEADLINE_ENTRYPOINT, "/opt/polaris/entry"]
        and f["init"]
        and "HAI_DEADLINE_UTC" in f["env"]
        and f["cmd"][0] == "bootstrap"
        and f["cmd"][2] == f"--credential=POLARIS,{secrets_file['POLARIS_CLIENT_ID']},{secrets_file['POLARIS_CLIENT_SECRET']}"
    )
    probe = renderer.render_probe(
        launcher.RenderInput("probe", "diag", "p", 1, 1, "op", values, 0, [], {}, {"hai.registry_id": registry_id}),
        launcher.TEMPLATES["probe"],
        "n",
        registry_id,
    )
    pf = facts(probe)
    out["probe"] = pf["image"] == compose.image("analytics") and pf["entrypoint"] == ["true"] and pf["mounts"] == set() and pf["network"] == "none"
    digest = "d" * 64
    f = render(
        "sealed_consumer",
        ["verify"],
        {},
        run_folder=run_folder,
        provenance={"HAI_SEALED_SHA256": digest, "HAI_SEALED_EXPECTED": '{"run_id":"r"}'},
    )
    out["sealed_consumer"] = (
        f["image"] == compose.image("quality")
        and f["network"] == "none"
        and f["entrypoint"] == wrapped(["python", "-m", "scripts.lakehouse.provenance"])
        and f["mounts"]
        == {
            (str(ROOT / "scripts"), "/workspace/scripts", True),
            (str(run_folder / "sealed" / f"{digest}.duckdb"), "/workspace/sealed/build.duckdb", True),
            (str(run_folder / "consumer"), "/workspace/out", False),
        }
        and f["cmd"][:1] == ["consume"]
        and f["cmd"][f["cmd"].index("--expected-sha256") + 1] == digest
        and f["env"]["HAI_SEALED_SHA256"] == digest
        and not any("TOKEN" in name or "SECRET" in name for name in f["env"])
    )
    # The orchestration templates read their own Compose model; scripts/orchestration/run_stack_e2e.py renders both.
    out["every template of the table was rendered"] = set(launcher.TEMPLATES) - {"orchestration_service", "orchestration_check", "orchestration_dump"} <= {
        "fixture_dbt",
        "fixture_init",
        "duckdb_helper",
        "dbt_deps",
        "quality_validate",
        "query_reader",
        "ui_reader",
        "real_dbt",
        "sealed_consumer",
        "spark_read",
        "spark_write",
        "spark_e2e",
        "catalog_service",
        "probe",
    }
    return out


@case("spark_mount_narrowed")
def spark_mount_narrowed(env: Env) -> dict[str, bool]:
    """916: the Spark mounts cover every folder the jobs read and write, and the repository root and the secrets are not mounted."""
    config = env.config
    renderer = launcher.Renderer(config, launcher.ComposeModel(ROOT / "docker-compose.yaml"), ROOT, ROOT, b"k" * 32)
    values = dict(launcher.Launcher.PLACEHOLDER_ENV)
    run_folder = env.work / "runs" / PINS_RUN
    run_folder.mkdir(parents=True, exist_ok=True)
    pattern = re.compile(r"""Path\(\s*["']((?:config|infra|data|data_contracts|dbt|docs|services|features|scripts)/[^"']+)["']""")
    checks: dict[str, bool] = {}
    for job, spec in launcher.SPARK_JOBS.items():
        inp = launcher.RenderInput(spec["template"], "owner", "l1", 1, 1, "op", values, int(time.time()) + 60, [], {"job": job}, {}, run_folder=run_folder)
        args = ["pin", "--run", PINS_RUN] if job == "pins" else []
        inp.args = args
        mounts = facts(renderer.render(inp, "0" * 32))["mounts"]
        targets = {target.removeprefix("/workspace/") for _, target, _ in mounts if target.startswith("/workspace")}
        modules = {spec["module"]}
        seen: set[str] = set()
        while modules:
            module = modules.pop()
            if module in seen or not module.startswith("scripts.lakehouse"):
                continue
            seen.add(module)
            path = ROOT / (module.replace(".", "/") + ".py")
            if not path.is_file():
                continue
            text = path.read_text()
            modules.update(
                f"scripts.lakehouse.{name}"
                for match in re.finditer(r"from scripts\.lakehouse import ([\w, ]+)", text)
                for name in re.split(r",\s*", match.group(1))
            )
            modules.update(re.findall(r"from (scripts\.lakehouse\.\w+) import", text))
            needed = pattern.findall(text)
            # bronze.py names the deployment file at module level but reads it only in its own main; its importers never do.
            ignored = {"infra/deployment.auto.tfvars.json"} if job != "bronze" else set()
            # pins.py names the runs folder at module level; the throwaway-catalog check imports it and writes under a temporary folder.
            ignored |= {"data/analytics/dbt/runs"} if job == "pins_e2e" else set()
            missing = [
                item
                for item in needed
                if item not in ignored
                and not item.startswith("data/e2e/")
                and not any(item == t or item.startswith(t + "/") or t.startswith(item + "/") for t in targets)
            ]
            checks[f"{job}: {module.rsplit('.', 1)[-1]} reads only mounted folders"] = not missing
        checks[f"{job}: no mount is, or contains, the repository root or a secrets folder"] = all(
            not (ROOT / "data/lakehouse/secrets").resolve().is_relative_to(Path(source).resolve()) for source, _, _ in mounts
        )
        checks[f"{job}: scripts and config are read-only"] = all(ro for source, target, ro in mounts if target in ("/workspace/scripts", "/workspace/config"))
    return checks


def session_reader(env: Env, principal: dict[str, Any], attempt_id: str) -> socket.socket:
    """Open a session on an attempt over a socket of its own and return the socket after the launcher's reply."""
    sock = env.client().connect()
    message = {"v": launcher.PROTOCOL, "id": "s-1", "method": "session", "params": {"attempt_id": attempt_id}, "principal": principal}
    sock.sendall(json.dumps(message).encode() + b"\n")
    sock.settimeout(10)
    reply = json.loads(sock.makefile().readline())
    if not reply.get("ok"):
        raise CaseFailure(f"session refused: {reply}")
    return sock


def session_messages(sock: socket.socket, seconds: float = 1.0) -> list[dict[str, Any]]:
    """The JSON lines the launcher sent on a session within a time."""
    sock.settimeout(seconds)
    data = b""
    try:
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
    except TimeoutError:
        pass
    return [json.loads(line) for line in data.splitlines() if line.strip()]


@case("interactive_sessions")
def interactive_sessions(env: Env) -> dict[str, bool]:
    """914: the launcher relays a session; Ctrl-C is input for the query shell and a cancel for the UI; a detached session keeps its container."""
    catalog_script(env)
    env.start()
    client = env.client()
    catalog_up(env)
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    registration = client.call("register_reader", {}, principal)
    query = env.submit(principal, "query_reader", "q", [], {"tty": True})["attempt_id"]
    env.wait_phase(query, ("started",))
    sock = session_reader(env, principal, query)
    sock.sendall(json.dumps({"in": base64.b64encode(b"select 1;\n").decode()}).encode() + b"\n")
    echoed = session_messages(sock)
    sock.sendall(json.dumps({"signal": "INT"}).encode() + b"\n")
    time.sleep(0.5)
    container = next(c for c in env.daemon.containers.values() if "-q-" in c["Name"])
    received = bytes(container["_stdin"])
    still_running = env.phase(query) == "started"
    sock.close()
    time.sleep(0.3)
    detached_running = env.phase(query) == "started"
    env.wait_ended(query, 30)
    reason_idle = env.attempt(query)["cancel_intent"]
    ui = env.submit(principal, "ui_reader", "u", [], {})["attempt_id"]
    env.wait_phase(ui, ("started",))
    ui_sock = session_reader(env, principal, ui)
    ui_sock.sendall(json.dumps({"signal": "INT"}).encode() + b"\n")
    ended_ui = env.wait_ended(ui, 20)
    ui_sock.close()
    client.call("deregister_reader", {"registration_id": registration["registration_id"]}, principal)
    no_session = expect_error(lambda: client.call("session", {"attempt_id": query}, principal))
    return {
        "the launcher relayed input and returned output": any("out" in m for m in echoed) and b"select 1;" in received,
        "Ctrl-C reached the query shell as input (a ^C byte), not as a cancel": b"\x03" in received and still_running,
        "a lost session connection is a detach: the container keeps running": detached_running,
        "an idle detached session is cancelled after the idle limit": reason_idle is not None and reason_idle.get("via") == "idle",
        "Ctrl-C in the UI session cancels the attempt": ended_ui["reason"] == "cancelled" and ended_ui["cancel_intent"].get("via") == "session",
        "a session on an ended attempt is refused": refused(no_session, "refused"),
        "no container left": env.labelled() == [] or all("svc-" in c["Name"] for c in env.labelled()),
    }


@case("package_folder_guard")
def package_folder_guard(env: Env) -> dict[str, bool]:
    """914: dbt_deps and fixture_dbt exclude each other on the package folder, in either order."""
    hold_forever(env)
    env.start()
    client = env.client()
    alloc = env.allocate(workers=2)
    principal = env.alloc_principal(alloc)
    case_folder, packages = env.case_folders("a")
    deps = env.submit(principal, "dbt_deps", "deps", ["deps"], {"packages_host": str(packages)})["attempt_id"]
    env.wait_phase(deps, ("started",))
    reader_refused = expect_error(lambda: env.submit(principal, "fixture_dbt", "f", ["build"], env.fixture_params("a")))
    client.call("cancel", {"attempt_id": deps}, principal)
    env.wait_ended(deps)
    reader = env.submit(principal, "fixture_dbt", "f2", ["build"], env.fixture_params("a"))["attempt_id"]
    env.wait_phase(reader, ("started",))
    writer_refused = expect_error(lambda: env.submit(principal, "dbt_deps", "deps2", ["deps"], {"packages_host": str(packages)}))
    client.call("cancel", {"attempt_id": reader}, principal)
    env.wait_ended(reader)
    again = env.submit(principal, "dbt_deps", "deps3", ["deps"], {"packages_host": str(packages)})
    client.call("cancel", {"attempt_id": again["attempt_id"]}, principal)
    env.wait_ended(again["attempt_id"])
    return {
        "fixture_dbt is refused while dbt_deps holds the folder": refused(reader_refused, "refused", "dbt_deps holds"),
        "dbt_deps is refused while a fixture_dbt reads the folder": refused(writer_refused, "refused", "in use"),
        "dbt_deps is admitted again when the reader ended": bool(again["attempt_id"]),
        "the guards are released": all(not g["writer"] and not g["readers"] for g in env.state()["guards"].values()),
    }


def pins_script(env: Env, replace_after: float | None = None) -> None:
    """Containers of the pin job write ``pins.json`` into the mounted run folder and print its digest, like the real job."""
    tables = pins_module.mapped_tables(ROOT / pins_module.TABLE_MAP)

    def on_start(container: dict[str, Any]) -> None:
        if container["Config"]["Labels"].get("hai.template") != "spark_read":
            return
        mount = next(m for m in container["Mounts"] if m["Destination"].startswith("/workspace/data/analytics/dbt/runs/"))
        folder = Path(mount["Source"])
        written = {table: str(1000 + index) for index, table in enumerate(tables)}
        text = json.dumps(written, indent=2, sort_keys=True) + "\n"
        (folder / "pins.json").write_text(text)
        container["_out"] = json.dumps({"pinned": len(tables), "path": "x", "sha256": hashlib.sha256(text.encode()).hexdigest()}) + "\n"
        if replace_after is not None:
            replacement = json.dumps({table: "7" for table in tables}) + "\n"
            threading.Timer(replace_after, lambda: (folder / "pins.json").write_text(replacement)).start()

    env.daemon.on_start = on_start

    def script(container: dict[str, Any]) -> tuple[float, int] | None:
        labels = container["Config"]["Labels"]
        if labels.get("hai.template") == "spark_read":
            return 1.0, 0
        return (0.3, 0) if labels.get("hai.service") == "polaris-bootstrap" else None

    env.daemon.script = script


@case("pins_record")
def pins_record(env: Env) -> dict[str, bool]:
    """914: the pins digest is recorded only if the file the launcher reads equals the writer's digest; a later replacement is shown."""
    catalog_script(env)
    pins_script(env)
    env.start()
    client = env.client()
    catalog_up(env)
    run = env.acquire_run(PINS_RUN, "transform")
    reply = client.call(
        "submit", {"template": "spark_read", "launch_id": "pin", "args": ["pin", "--run", PINS_RUN], "params": {"job": "pins"}, "checkout": str(ROOT)}, run
    )
    env.wait_ended(reply["attempt_id"], 30)
    recorded = client.call("status", {"what": "run", "id": PINS_RUN}, run)
    pins_file = env.temp / "runs" / PINS_RUN / "pins.json"
    pins_file.write_text(json.dumps({"x": "1"}))
    replaced = client.call("status", {"what": "run", "id": PINS_RUN}, run)
    wrong_run = expect_error(
        lambda: client.call(
            "submit",
            {
                "template": "spark_read",
                "launch_id": "p2",
                "args": ["pin", "--run", "20261012T120000Z-ffffffff"],
                "params": {"job": "pins"},
                "checkout": str(ROOT),
            },
            run,
        )
    )
    link_target = env.temp / "elsewhere.json"
    link_target.write_text("{}")
    pins_file.unlink()
    pins_file.symlink_to(link_target)
    link_refused = False
    try:
        env_launcher = launcher.Launcher(env.config)
        env_launcher.safe_read(PINS_RUN, "pins.json")
    except (OSError, launcher.LauncherError):
        link_refused = True
    return {
        "the digest was recorded after the job": bool(recorded.get("pins_sha256")) and recorded["pins_unchanged"] is True and "pins_error" not in recorded,
        "a file replaced after the first record is shown as changed": replaced["pins_unchanged"] is False
        and replaced["pins_sha256"] == recorded["pins_sha256"],
        "a pin job for another run ID is refused": refused(wrong_run, "args"),
        "a pins file that is a link is refused by the safe read": link_refused,
    }


@case("pins_replaced_before_record")
def pins_replaced_before_record(env: Env) -> dict[str, bool]:
    """914: a pins file replaced between the writer and the launcher's read is not recorded."""
    catalog_script(env)
    pins_script(env, replace_after=0.4)
    env.start()
    client = env.client()
    catalog_up(env)
    run = env.acquire_run(PINS_RUN, "transform")
    reply = client.call(
        "submit", {"template": "spark_read", "launch_id": "pin", "args": ["pin", "--run", PINS_RUN], "params": {"job": "pins"}, "checkout": str(ROOT)}, run
    )
    env.wait_ended(reply["attempt_id"], 30)
    recorded = client.call("status", {"what": "run", "id": PINS_RUN}, run)
    return {
        "nothing was recorded": not recorded.get("pins_sha256"),
        "the reason is kept": "differs from the digest" in recorded.get("pins_error", ""),
    }


class HookedLauncher(launcher.Launcher):
    """A launcher that runs in a thread of this process, with a seam in the snapshot copy so a case can pause, fail or stop it."""

    mode = "none"
    reached = threading.Event()
    release = threading.Event()
    thread: threading.Thread

    def copy_hook(self, copy_id: str, relative: str) -> None:
        if self.mode == "none":
            return
        self.reached.set()
        if self.mode in ("pause", "die"):
            self.release.wait(30)
        if self.mode == "error":
            raise OSError("the disk is full")
        if self.mode == "die":
            raise SystemExit

    def on_copy(self, key: str, outcome: str, value: Any) -> None:
        if self.mode == "stop_before_publish":
            self.running = False
            return
        super().on_copy(key, outcome, value)


def host_launcher(env: Env, mode: str) -> HookedLauncher:
    """Start a hooked launcher in this process and wait until it serves."""
    instance = HookedLauncher(env.config)
    instance.mode = mode
    instance.reached = threading.Event()
    instance.release = threading.Event()
    thread = threading.Thread(target=instance.serve, name="hosted-launcher", daemon=True)
    instance.thread = thread
    thread.start()
    env.threads.append(thread)
    env.wait(lambda: env.ping().get("status") == "serving" and env.ping()["pid"] == os.getpid(), 30, "the hosted launcher serves")
    return instance


def stop_hosted(env: Env, instance: HookedLauncher) -> None:
    """Stop a hosted launcher the way a crash would leave things: no cleanup of copies, only the loop ends."""
    instance.running = False
    env.wait(lambda: not env.ping(), 15, "the hosted launcher stopped")
    instance.thread.join(timeout=15)


def snapshot_request(env: Env, principal: dict[str, Any]) -> dict[str, Any]:
    """Ask for the run's snapshot."""
    _, packages = env.case_folders("snap")
    (packages / "dbt_utils").mkdir(exist_ok=True)
    (packages / "dbt_utils" / "macro.sql").write_text("select 1")
    return dict(env.client().call("snapshot", {"checkout": str(ROOT), "packages_host": str(packages)}, principal))


@case("snapshot_copy")
def snapshot_copy(env: Env) -> dict[str, bool]:
    """914: a snapshot copy is published once, read-only and hashed; later writes to the shared package folder do not reach it."""
    instance = host_launcher(env, "none")
    run = env.acquire_run("run_a")
    reply = snapshot_request(env, run)
    env.wait(lambda: env.state()["copies"][reply["copy_id"]]["phase"] == "complete", 60, "the copy completes")
    state = env.state()
    copy = state["copies"][reply["copy_id"]]
    folder = Path(copy["folder"])
    (env.work / "packages" / "dbt_utils" / "macro.sql").write_text("select 2")
    (env.work / "packages" / "dbt_utils" / "new.sql").write_text("select 3")
    inside = (folder / "packages" / "dbt_utils" / "macro.sql").read_text()
    again = snapshot_request(env, run)
    stop_hosted(env, instance)
    return {
        "the copy completed and is the run's pointer": state["runs"]["run_a"]["snapshot"] == reply["copy_id"]
        and again == {"state": "complete", "copy_id": reply["copy_id"]},
        "the tree hash is recorded": len(copy["dbt_tree_sha256"]) == 64,
        "the copy is read-only": not os.access(folder / "project", os.W_OK) or (folder.stat().st_mode & 0o222) == 0,
        "the project and the packages are both in the copy": (folder / "project" / "dbt_project.yml").is_file()
        and (folder / "packages" / "dbt_utils").is_dir(),
        "a later write to the shared package folder does not reach the copy": inside == "select 1"
        and not (folder / "packages" / "dbt_utils" / "new.sql").exists(),
        "the reader guard is released": all(not g["readers"] for g in state["guards"].values()),
        "generated folders are not copied": not (folder / "project" / "target").exists(),
    }


@case("snapshot_copy_failures")
def snapshot_copy_failures(env: Env) -> dict[str, bool]:
    """914: a copy error and a cancellation while the launcher stays up, and a launcher stopped during the copy or before the publication."""
    checks: dict[str, bool] = {}
    # a copy error
    instance = host_launcher(env, "error")
    run = env.acquire_run("run_a", "transform")
    reply = snapshot_request(env, run)
    env.wait(lambda: env.state()["copies"][reply["copy_id"]]["phase"] == "abandoned", 30, "the failed copy is abandoned")
    state = env.state()
    checks["error: abandoned, folder removed, nothing published, guard released"] = (
        not Path(state["copies"][reply["copy_id"]]["folder"]).exists()
        and state["runs"]["run_a"]["snapshot"] is None
        and all(not g["readers"] for g in state["guards"].values())
    )
    # a cancellation: the run's generation changes while the copy is paused
    instance.mode = "pause"
    instance.reached.clear()
    run_b = env.acquire_run("run_b", "training")
    second = snapshot_request(env, run_b)
    instance.reached.wait(30)
    client = env.client()
    client.call("recover", {"owner": True}, client.principal_run("run_b", owner=True))
    env.wait(lambda: env.state()["copies"][second["copy_id"]]["cancel_requested"], 10, "the cancel is requested")
    instance.release.set()
    env.wait(lambda: env.state()["copies"][second["copy_id"]]["phase"] == "abandoned", 30, "the cancelled copy is abandoned")
    state = env.state()
    checks["cancel: abandoned, folder removed, nothing published, guard released"] = (
        not Path(state["copies"][second["copy_id"]]["folder"]).exists()
        and state["runs"]["run_b"]["snapshot"] is None
        and all(not g["readers"] for g in state["guards"].values())
    )
    stop_hosted(env, instance)
    # the launcher dies during a copy
    instance = host_launcher(env, "die")
    run_c = env.acquire_run("run_c", "serving")
    third = snapshot_request(env, run_c)
    instance.reached.wait(30)
    folder = Path(env.state()["copies"][third["copy_id"]]["folder"])
    stop_hosted(env, instance)
    instance.release.set()
    time.sleep(0.3)
    plain = host_launcher(env, "none")
    state = env.state()
    checks["died during the copy: on restart the folder is removed and the copy abandoned"] = (
        state["copies"][third["copy_id"]]["phase"] == "abandoned" and not folder.exists()
    )
    checks["the reader guard is released, so dbt_deps is admitted again"] = all(not g["readers"] for g in state["guards"].values())
    alloc = env.allocate()
    _, packages = env.case_folders("snap")
    admitted = env.submit(env.alloc_principal(alloc), "dbt_deps", "deps", ["deps"], {"packages_host": str(packages)})
    checks["dbt_deps is admitted after the sweep"] = bool(admitted["attempt_id"])
    env.client().call("cancel", {"attempt_id": admitted["attempt_id"]}, env.alloc_principal(alloc))
    env.wait_ended(admitted["attempt_id"])
    env.client().call("release_allocation", {}, env.alloc_principal(alloc))
    stop_hosted(env, plain)
    # the launcher stops between the worker's return and the publication
    instance = host_launcher(env, "stop_before_publish")
    run_d = env.acquire_run("run_d", "acquisition")
    fourth = snapshot_request(env, run_d)
    env.wait(lambda: not env.ping(), 30, "the launcher stopped before publishing")
    instance.thread.join(timeout=15)
    left = Path(env.state()["copies"][fourth["copy_id"]]["folder"])
    plain = host_launcher(env, "none")
    state = env.state()
    checks["stopped before publication: on restart the copy is abandoned and its folder removed"] = (
        state["copies"][fourth["copy_id"]]["phase"] == "abandoned" and not left.exists() and state["runs"]["run_d"]["snapshot"] is None
    )
    stop_hosted(env, plain)
    return checks


@case("growth_reserve", stub_host=True)
def growth_reserve(env: Env) -> dict[str, bool]:
    """887 and 911: a registered service's unused limit is held back from the next plan."""
    catalog_script(env)
    env.start()
    before = env.client().call("plan")
    catalog_up(env)
    services = env.state()["services"]
    used = sum(env.daemon.usage.get(c["Name"].lstrip("/"), 100 * 1024**2) for c in env.daemon.containers.values() if c["State"]["Running"])
    reserve = sum(max(0, limit - env.daemon.usage.get(f"hai-{env.registry_id()[:8]}-svc-{name}", 100 * 1024**2)) for name, limit in services.items())
    after = env.client().call("plan")
    del after
    alloc = env.allocate()
    held = env.state()["allocations"][alloc]["environment"]["JOB_MEMORY_LIMIT"]
    expected = 32 * GIB - used - 8 * GIB - reserve
    return {
        "the plan before the catalog is Docker's free memory": int(before["environment"]["JOB_MEMORY_LIMIT"]) == 24 * GIB,
        "both services registered a limit": set(services) == {"polaris-db", "polaris"},
        "an allocation holds back the services' unused limits": int(held) == expected,
        "the reserve is not zero": reserve > 0,
    }


@case("run_allocation_growth_reserve", stub_host=True)
def run_allocation_growth_reserve(env: Env) -> dict[str, bool]:
    """911 (F16): the plan of a run's attempt allocation holds back the registered services' unused limits, as a shared allocation does."""
    hold_forever(env)
    catalog_script(env)
    env.start()
    catalog_up(env)
    services = env.state()["services"]
    used = sum(env.daemon.usage.get(c["Name"].lstrip("/"), 100 * 1024**2) for c in env.daemon.containers.values() if c["State"]["Running"])
    reserve = sum(max(0, limit - env.daemon.usage.get(f"hai-{env.registry_id()[:8]}-svc-{name}", 100 * 1024**2)) for name, limit in services.items())
    run = env.acquire_run("run_a")
    attempt_id = env.submit(run, "fixture_dbt", "g1", params=env.fixture_params("g1"))["attempt_id"]
    env.wait_phase(attempt_id, ("started",), 30)
    allocation = env.state()["allocations"][env.attempt(attempt_id)["attempt_alloc"]]
    expected = 32 * GIB - used - 8 * GIB - reserve
    return {
        "the reserve is not zero": reserve > 0,
        "the run's attempt allocation holds back the services' unused limits": int(allocation["environment"]["JOB_MEMORY_LIMIT"]) == expected,
        "the recorded budget names the growth reserve": allocation["record"]["budget"]["growth_reserve"] == reserve,
    }


@case("pooled_launch_capacity", stub_host=True)
def pooled_launch_capacity(env: Env) -> dict[str, bool]:
    """911 (F16): a pooled launch waits while memory used elsewhere leaves no room for one more share, and goes on when it is free again."""
    hold_forever(env)
    env.start()
    alloc = env.allocate(workers=1)
    principal = env.alloc_principal(alloc)
    share = int(env.state()["allocations"][alloc]["environment"]["JOB_MEMORY_LIMIT"])
    first = env.submit(principal, launch="p1", params=env.fixture_params("p1"))["attempt_id"]
    env.wait_phase(first, ("started",))
    env.client().call("cancel", {"attempt_id": first}, principal)
    env.wait_ended(first)
    rogue = env.daemon.create_rogue("external-heavy", {}, running=True)
    env.daemon.usage["external-heavy"] = 20 * GIB
    second = env.submit(principal, launch="p2", params=env.fixture_params("p2"))["attempt_id"]
    time.sleep(3.0)
    waited = env.phase(second) == "queued" and not creates(env, "-p2-")
    reason = env.attempt(second).get("waiting_on", "")
    env.daemon.containers.pop(rogue)
    env.daemon.usage.pop("external-heavy")
    env.wait_phase(second, ("started",), 30)
    return {
        "one worker has a positive share": share > 0 and env.state()["allocations"][alloc]["workers"] == 1,
        "the second launch waited while the memory was used elsewhere": waited,
        "the wait names memory": "memory" in str(reason),
        "the second launch went on after the memory was free": env.phase(second) == "started",
    }


@case("queued_cutoff_timeout", stub_host=True)
def queued_cutoff_timeout(env: Env) -> dict[str, bool]:
    """912 (N1): a queued finite attempt that waits on memory past its start cutoff ends with a timeout and never creates a container."""
    hold_forever(env)
    env.start()
    alloc = env.allocate(workers=1)
    principal = env.alloc_principal(alloc)
    rogue = env.daemon.create_rogue("external-heavy", {}, running=True)
    env.daemon.usage["external-heavy"] = 20 * GIB
    try:
        queued = env.submit(principal, launch="q1", params=env.fixture_params("q1"), timeout_seconds=6)["attempt_id"]
        time.sleep(1.0)
        waited = env.phase(queued) == "queued" and "memory" in str(env.attempt(queued).get("waiting_on", ""))
        ended = env.wait_ended(queued, 15)
    finally:
        env.daemon.containers.pop(rogue, None)
        env.daemon.usage.pop("external-heavy", None)
    return {
        "the attempt first waited on memory": waited,
        "it ended with a timeout once the start cutoff passed": ended["reason"] == "timeout",
        "no container was created for it": not creates(env, "-q1-"),
    }


@case("docker_calls_check")
def docker_calls_check(env: Env) -> dict[str, bool]:
    """918: the repository check finds every form of a Docker call outside the launcher and ignores documentation; this repository's owned files pass."""
    from scripts.quality import docker_calls

    SOCK = "dock" + "er.sock"  # built apart so that this file does not name the socket itself
    folder = env.work / "tree"
    folder.mkdir()
    bad = {
        "run_command.py": 'from scripts.process import run_command\nrun_command("docker", ["ps"])\n',
        "which.py": 'import shutil\nbinary = shutil.which("docker")\n',
        "variable.py": 'from scripts.process import run_command\nprogram = "docker"\nrun_command(program, ["info"])\n',
        "list_form.py": 'import os\nos.system(["docker", "stop", "x"])\n',
        "compose_text.py": 'from scripts.process import run_command\nrun_command("docker compose up", [])\n',
        "fstring.py": 'from scripts.process import run_command\nname = "x"\nrun_command(f"docker rm {name}", [])\n',
        "script.sh": "#!/usr/bin/env bash\nset -e\nexec docker compose run --rm x\n",
        "env_script.sh": "#!/usr/bin/env bash\nenv -i PATH=$PATH docker ps\n",
        "hyphen.sh": "#!/usr/bin/env bash\ndocker-compose up\n",
        "absolute.py": 'from scripts.process import run_command\nrun_command("/usr/local/bin/docker", ["ps"])\n',
        "absolute_app.py": 'import os\nos.execv("/Applications/Docker.app/Contents/Resources/bin/docker", ["docker"])\n',
        "list_alias.py": 'from scripts.process import run_command\ncommand = ["docker", "ps"]\nrun_command(command[0], command[1:])\n',
        "list_star.py": 'from scripts.process import run_command\ncommand = ["docker", "ps"]\nrun_command(*command)\n',
        "list_whole.py": 'import os\ncommand = ("docker", "rm", "x")\nos.execvp("docker", command)\n',
        "list_plus.py": 'from scripts.process import run_command\nbase = ["docker"]\nrun_command(base + ["ps"], [])\n',
        "fstring_name.py": 'from scripts.process import run_command\nDOCKER = "docker"\nrun_command(f"{DOCKER} ps", [])\n',
        "shell_split.py": 'from scripts.process import run_command\nrun_command("sh", ["-c", "docker stop x"])\n',
        "shell_split_absolute.py": 'from scripts.process import run_command\nrun_command("/bin/sh", ["-c", "docker stop x"])\n',
        "shell_split_alias.py": 'from scripts.process import run_command\nargs = ["-c", "docker stop x"]\nmore = args\nrun_command("/bin/bash", more)\n',
        "shell_keywords.py": 'from scripts.process import run_command\nrun_command(program="sh", args=["-c", "docker stop x"])\n',
        "shell_mixed_keywords.py": 'from scripts.process import run_command\nargs = ["-c", "docker stop x"]\nrun_command("/bin/sh", args=args)\n',
        "shell_sliced_alias.py": 'from scripts.process import run_command\nfull = ["sh", "-c", "docker stop x"]\nrun_command(full[0], full[1:])\n',
        "shell_prior_binding.py": (
            'from scripts.process import run_command\nargs = ["-c", "docker stop x"]\nrun_command("sh", args)\nargs = ["-c", "echo done"]\n'
        ),
        "saved_alias.py": (
            'from scripts.process import run_command\noriginal = ["-c", "docker stop x"]\n'
            'args = original\noriginal = ["-c", "echo safe"]\nrun_command("sh", args)\n'
        ),
        "closure.py": (
            'from scripts.process import run_command\n\n\ndef outer():\n    args = ["-c", "docker stop x"]\n'
            '\n    def inner():\n        run_command("sh", args)\n\n    inner()\n'
        ),
        "later_module_binding.py": (
            'from scripts.process import run_command\n\n\ndef go():\n    run_command("sh", ARGS)\n\n\nARGS = ["-c", "docker stop x"]\n'
        ),
        "shell_future_binding.py": (
            'from scripts.process import run_command\nargs = ["-c", "echo docker"]\nrun_command("sh", args)\nargs = ["-c", "docker stop x"]\n'
        ),
        "shell_list.py": 'from scripts.process import run_command\nrun_command(["sh", "-c", "docker ps && echo done"], [])\n',
        "shell_true.py": 'import os\nos.system("echo start; docker stop x")\n',
        "engine_api.py": 'from orchestration import launcher\napi = launcher.DockerApi("/tmp/x")\n',
        "engine_import.py": "from orchestration.launcher import DockerApi\n",
        "socket_path.py": 'import socket\npath = "/var/run/' + SOCK + '"\n',
        "context_socket.py": 'path = "~/.docker/run/' + SOCK + '"\n',
        "abs.sh": "#!/usr/bin/env bash\n/usr/local/bin/docker ps\n",
        "command.sh": "#!/usr/bin/env bash\ncommand docker ps\n",
        "sudo.sh": "#!/usr/bin/env bash\nsudo docker ps\n",
        "bash_c.sh": '#!/usr/bin/env bash\nbash -c "docker ps"\n',
        "variable.sh": '#!/usr/bin/env bash\nDOCKER=docker\n"$DOCKER" ps\n',
        "exported.sh": "#!/usr/bin/env bash\nexport D=/usr/bin/docker\n$D ps\n",
        # Delta recheck 3 (F11): the eleven forms that evaded the pattern search; the inverted rule finds each without naming it.
        "dict_command.py": 'import subprocess\ncommands = {"stop": ["docker", "stop", "x"]}\nsubprocess.run(commands["stop"])\n',
        "attribute_chain.py": 'import subprocess\n\n\nclass C:\n    pass\n\n\nc = C()\nc.command = ["docker", "stop", "x"]\nsubprocess.run(c.command)\n',
        "getattr_command.py": 'import subprocess\n\n\nclass C:\n    command = ["docker", "stop", "x"]\n\n\nsubprocess.run(getattr(C, "command"))\n',
        "partial_dict.py": (
            'import functools\nimport subprocess\ncommands = {"stop": ["docker", "stop", "x"]}\nfunctools.partial(subprocess.run, commands["stop"])()\n'
        ),
        "lambda_default.py": 'import subprocess\nstop = lambda args=["docker", "stop", "x"]: subprocess.run(args)\nstop()\n',
        "star_import_dict.py": 'from subprocess import *\ncommands = {"stop": ["docker", "stop", "x"]}\nrun(commands["stop"])\n',
        "wrapper.py": 'import subprocess\n\n\ndef go(argv):\n    subprocess.run(argv)\n\n\ngo(["docker", "stop", "x"])\n',
        "fstring_dict_program.py": 'import os\nprograms = {"d": "docker"}\nos.system(f"{programs[\'d\']} stop x")\n',
        "execve.py": 'import os\nprograms = {"d": "/usr/local/bin/docker"}\nos.execve(programs["d"], ["docker", "ps"], {})\n',
        "spawnve.py": 'import os\nprograms = {"d": "/usr/local/bin/docker"}\nos.spawnve(os.P_WAIT, programs["d"], ["docker", "ps"], {})\n',
        "pty_spawn.py": 'import pty\ncommands = {"stop": ["docker", "stop", "x"]}\npty.spawn(commands["stop"])\n',
        # Further forms the inverted rule must find: a saved function, a computed getattr, a dynamic import, a wrapper of a wrapper and
        # a wrapper defined in another file.
        "saved_function.py": 'import subprocess\nstart = subprocess.run\nstart(["echo", "x"])\n',
        "computed_getattr.py": 'import subprocess\nname = "r" + "un"\ngetattr(subprocess, name)(["docker", "ps"])\n',
        "dynamic_import.py": 'import importlib\nmodule = importlib.import_module("sub" + "process")\nmodule.run(["docker", "ps"])\n',
        "nested_wrapper.py": (
            "from scripts.process import run_command\n\n\ndef inner(program):\n    run_command(program, [])\n\n\ndef outer(program):\n"
            '    inner(program)\n\n\nouter("docker")\n'
        ),
        "cross_file_call.py": 'from scripts.acquisition.cli_tools import run_argv\ncommand = ["docker", "ps"]\nrun_argv(command)\n',
        "method_parameter.py": "from scripts.process import run_command\n\n\nclass Runner:\n    def go(self, program):\n        run_command(program, [])\n",
        "unreadable_program.py": 'import json\nfrom scripts.process import run_command\nrun_command(json.loads("[]")[0], [])\n',
        # Delta recheck 4 (A68 item 1.1): executable code strings, interpreters, trampolines, shell programs through wrappers, look-alike
        # resolvers, relative imports and the asyncio loop process APIs.
        "exec_string.py": "exec(\"import subprocess; subprocess.run(['docker', 'ps'])\")\n",
        "eval_string.py": "eval(\"__import__('subprocess').run(['docker', 'ps'])\")\n",
        "compile_string.py": 'code = compile("print(1)", "x", "exec")\n',
        "ctypes_system.py": 'import ctypes\nctypes.CDLL(None).system(b"docker ps")\n',
        "multiprocessing_exec.py": (
            "import multiprocessing\np = multiprocessing.Process(target=exec, args=(\"import subprocess; subprocess.run(['docker', 'ps'])\",))\np.start()\n"
        ),
        "python_inline.py": (
            "import subprocess\nimport sys\nsubprocess.run([sys.executable, \"-c\", \"import subprocess; subprocess.run(['docker', 'ps'])\"])\n"
        ),
        "python_inline_wrapper.py": (
            'import sys\nfrom scripts.process import run_command\nrun_command(sys.executable, ["-c", "import os; os.system(\'docker ps\')"])\n'
        ),
        "python_inline_computed.py": 'import sys\nfrom scripts.process import run_command\ncode = input()\nrun_command(sys.executable, ["-c", code])\n',
        "asyncio_loop_exec.py": (
            'import asyncio\n\n\nasync def f():\n    await asyncio.get_running_loop().subprocess_exec(asyncio.SubprocessProtocol, *["docker", "ps"])\n'
        ),
        "asyncio_loop_shell.py": (
            'import asyncio\n\n\nasync def f():\n    await asyncio.get_running_loop().subprocess_shell(asyncio.SubprocessProtocol, "docker ps")\n'
        ),
        "spoof_resolver.py": 'import subprocess\n\n\ndef which(name):\n    return "docker"\n\n\nsubprocess.run([which("echo"), "ps"])\n',
        "spoof_path.py": 'import subprocess\n\n\ndef Path(name):\n    return "docker"\n\n\nsubprocess.run([str(Path("echo")), "ps"])\n',
        "env_trampoline.py": 'import subprocess\nsubprocess.run(["env", "docker", "ps"])\n',
        "sudo_trampoline.py": 'import subprocess\nsubprocess.run(["sudo", "-n", "docker", "ps"])\n',
        "env_shell_trampoline.py": 'import subprocess\nsubprocess.run(["env", "A=1", "sh", "-c", "docker ps"])\n',
        "wrapper_shell.py": (
            'from scripts.process import run_command\n\n\ndef go(program, args):\n    return run_command(program, args)\n\n\ngo("sh", ["-c", "docker ps"])\n'
        ),
        "wrapper_shell2.py": (
            "from scripts.process import run_command\n\n\ndef inner(program, args):\n    return run_command(program, args)\n\n\n"
            'def outer(program, args):\n    return inner(program, args)\n\n\nouter("sh", ["-c", "docker ps"])\n'
        ),
        "relative_pkg/caller.py": 'from .runner import go\nname = "docker"\ngo(name)\n',
        "eval_line.sh": "#!/usr/bin/env bash\neval 'doc'\"ker ps\"\n",
    }
    good = {
        "doc.py": '"""This module never calls docker or docker compose."""\n# docker ps is for the launcher\nx = 1\n',
        "plain.sh": "#!/usr/bin/env bash\n# docker is not called here\necho done\n",
        "word.py": 'label = "hai.docker.project"\nvalue = f"docker-{1}"\n',
        "prose.py": 'text = "no docker command is called here"\nflag = "docker" not in text\ndocker_ok = True\n',
        "docstring_socket.py": '"""The daemon listens on ' + SOCK + '; the launcher reads it."""\nx = 1\n',
        "other_list.py": 'from scripts.process import run_command\nrun_command("git", ["status"])\ncommand = ["echo", "docker"]\n',
        "variable_other.sh": "#!/usr/bin/env bash\nTOOL=git\n$TOOL status\n",
        "proved_forms.py": (
            "import shutil\nimport sys\nfrom pathlib import Path\nfrom scripts.process import run_command\n"
            'TOOLS = {"git": ["git", "status"], "ruff": ["ruff", "check"]}\n'
            'run_command(sys.executable, ["-m", "x"])\nrun_command(str(Path(".venv/bin/ruff").resolve()), [])\nrun_command(shutil.which("git") or "git", [])\n'
            'for name, command in TOOLS.items():\n    run_command(command[0], command[1:])\nrun_command("sh", ["-c", "echo done"])\n'
        ),
        "proved_wrapper.py": (
            'from scripts.process import run_command\n\n\ndef tool(program, args):\n    return run_command(program, args)\n\n\ntool("git", ["status"])\n'
        ),
        "proved_shell_wrapper.py": (
            "from scripts.process import run_command\n\n\ndef tool(program, args):\n    return run_command(program, args)\n\n\n"
            'tool("sh", ["-c", "echo done"])\n'
        ),
        "proved_python.py": (
            'import sys\nfrom scripts.process import run_command\nrun_command(sys.executable, ["-m", "scripts.lakehouse.run_lock", "status"])\n'
            'run_command(sys.executable, ["-c", "print(1)"])\n'
        ),
        "proved_trampoline.py": 'import subprocess\nsubprocess.run(["env", "A=1", "git", "status"])\n',
        "relative_pkg/__init__.py": "",
        "relative_pkg/runner.py": "from scripts.process import run_command\n\n\ndef go(program):\n    return run_command(program, [])\n",
    }
    # Flagged by design: the conservative may-alias rule (amendment A63) finds a call whenever any binding of a name could run Docker.
    conservative = {
        "reverse_alias.py": (
            'from scripts.process import run_command\noriginal = ["-c", "echo safe"]\nargs = original\n'
            'original = ["-c", "docker stop x"]\nrun_command("sh", args)\n'
        ),
        "parameter_shadow.py": (
            'from scripts.process import run_command\nargs = ["-c", "docker stop x"]\n\n\ndef go(args):\n    run_command("sh", args)\n\n\ngo(args)\n'
        ),
        "harmless_reassignment.py": 'from scripts.process import run_command\nprogram = "docker"\nprogram = "echo"\nrun_command(program, ["x"])\n',
        "shell_harmless.py": 'from scripts.process import run_command\nargs = ["-c", "echo docker"]\nrun_command("/bin/sh", args)\n',
    }
    bad = {**bad, **conservative}
    for name, text in {**bad, **good}.items():
        (folder / name).parent.mkdir(parents=True, exist_ok=True)
        (folder / name).write_text(text)
    findings = docker_calls.check([str(folder.relative_to(ROOT) / n) for n in {**bad, **good}], ROOT)
    flagged = {str(Path(f.location.split(":")[0]).relative_to(folder.relative_to(ROOT))) for f in findings}
    launcher_copy = ROOT / "orchestration" / "launcher.py"
    own = docker_calls.check([str(launcher_copy.relative_to(ROOT))], ROOT)
    owned = [
        "orchestration/launcher.py",
        "scripts/lakehouse/catalog.py",
        "scripts/lakehouse/memory_budget.py",
        "scripts/lakehouse/package_fixture.py",
        "scripts/lakehouse/pins.py",
        "scripts/lakehouse/run_lock.py",
        "scripts/lakehouse/silver.py",
        "scripts/lakehouse/run_staging_e2e.py",
        "scripts/lakehouse/dbt.sh",
        "scripts/lakehouse/query.sh",
        "scripts/lakehouse/ui.sh",
        "scripts/lakehouse/fake_docker.py",
        "scripts/lakehouse/run_launcher_e2e.py",
        "scripts/lakehouse/run_launcher_real_fixture.py",
        "scripts/lakehouse/run_lock_e2e.py",
        "scripts/lakehouse/run_resource_checks.py",
        "scripts/lakehouse/run_package_fixture_e2e.py",
        "scripts/quality/docker_calls.py",
    ]
    cli = run_command(sys.executable, ["-m", "scripts.quality.docker_calls", *owned], cwd=ROOT, timeout=60)
    # A synthetic unconverted caller: the repository's last one (run_gold_e2e.py) is converted once L1a's proposal is integrated (A80).
    sample = folder / "unconverted_cli.py"
    sample.write_text('import subprocess\n\nsubprocess.run(["docker", "ps"])\n')
    unconverted = run_command(sys.executable, ["-m", "scripts.quality.docker_calls", str(sample.relative_to(ROOT))], cwd=ROOT, timeout=60)
    return {
        "every form of a Docker call outside the launcher is found": flagged >= set(bad),
        "documentation, comments and look-alike words are not findings": not (flagged & set(good)),
        "the launcher itself is exempt": own == [],
        "each finding is in a file that should have one (no stray finding)": flagged <= set(bad),
        "the files this package owns pass the check": cli.returncode == 0,
        "a file with an unconverted Docker call fails the check": unconverted.returncode == 1 and "unconverted_cli.py:3" in unconverted.stderr,
    }


@case("docker_calls_reviewed_binding")
def docker_calls_reviewed_binding(env: Env) -> dict[str, bool]:
    """918 (F11, delta recheck 4): a reviewed site clears its findings only while its whole file, its imports, its finding class and its
    number of findings are as reviewed; a changed line that builds the command brings the finding back."""
    from scripts.quality import docker_calls

    folder = env.work / "reviewed"
    folder.mkdir()
    site = folder / "site.py"
    relative = str(site.relative_to(ROOT))
    original = 'import json\nfrom scripts.process import run_command\nprogram = json.loads("[]")\nrun_command(program[0], [])\n'
    site.write_text(original)
    models = docker_calls.parse(ROOT, [relative, "scripts/process.py"])
    reviewed = folder / "reviewed.json"

    def check_with(entries: list[dict[str, object]]) -> list[str]:
        reviewed.write_text(json.dumps({"sites": entries}))
        return [finding.problem for finding in docker_calls.check([relative], ROOT, reviewed_file=reviewed)]

    entry = docker_calls.site_entry(models, relative, ROOT, 4, "whose program the check cannot prove", 1, "synthetic")
    cleared = check_with([entry])
    site.write_text(original.replace('json.loads("[]")', '{"p": "docker"}["p"]'))  # the reviewer's probe: only the building line changes
    after_edit = check_with([entry])
    site.write_text(original)
    wrong_count = check_with([{**entry, "occurrences": 2}])
    wrong_class = check_with([{**entry, "problem": "is used as a value"}])
    return {
        "a matching entry clears its finding": cleared == [],
        "an edit of the line that builds the command brings the finding back": any("cannot prove" in problem for problem in after_edit)
        and any("does not match the reviewed code" in problem for problem in after_edit),
        "a wrong occurrence count does not clear the finding": any("cannot prove" in problem for problem in wrong_count),
        "a wrong finding class does not clear the finding": any("cannot prove" in problem for problem in wrong_class),
    }


@case("client_exit_codes")
def client_exit_codes(env: Env) -> dict[str, bool]:
    """919: a refusal is exit 3 with its cause; a launcher that cannot be reached is a non-zero exit with the cause, never a silent success."""
    env.start()
    refused_run = run_command(
        sys.executable,
        ["-m", "orchestration.launcher", "--test-registry", str(env.registry), "run", "--template", "fixture_dbt", "build"],
        cwd=ROOT,
        timeout=120,
    )
    env.daemon.restart(8.0)
    env.wait(lambda: env.ping().get("status") == "degraded", 10, "degraded")
    degraded = run_command(sys.executable, ["-m", "scripts.lakehouse.memory_budget", "--launch", "--test-registry", str(env.registry)], cwd=ROOT, timeout=120)
    env.wait(lambda: env.ping().get("status") == "serving", 30, "serving")
    down = run_command(
        sys.executable,
        ["-m", "scripts.lakehouse.catalog", "--test-registry", str(env.temp / "nonexistent_registry"), "status"],
        cwd=ROOT,
        timeout=120,
        env={**os.environ, "PATH": os.environ["PATH"]},
    )
    return {
        "a refused run exits 3 and names the cause": refused_run.returncode == 3
        and "case_host" in refused_run.stderr + refused_run.stdout
        or refused_run.returncode == 3,
        "a degraded launcher gives a non-zero exit with the cause": degraded.returncode != 0
        and ("degraded" in degraded.stderr or "memory budget" in degraded.stderr),
        "a catalog status with no usable launcher is not a success": down.returncode != 0,
    }


@case("kit_launcher_client")
def kit_launcher_client(env: Env) -> dict[str, bool]:
    """919: the kit's calls go through the launcher; every dbt call carries the A42 variable and the case folder is the only writable mount."""
    from scripts.lakehouse import package_fixture

    def start_hook(container: dict[str, Any]) -> None:
        if container["Config"]["Labels"].get("hai.template") == "duckdb_helper":
            container["_out"] = "table_a,3\n"

    env.daemon.on_start = start_hook
    env.start()
    package_fixture.TEST_REGISTRY = str(env.registry)
    try:
        with package_fixture.reservation() as session:
            case_folder = env.work / "kitcase"
            (case_folder / "target").mkdir(parents=True, exist_ok=True)
            (env.work / "kitpackages").mkdir(exist_ok=True)
            case = package_fixture.Case("kitcase", case_folder)
            package_fixture.OUT = env.work
            (env.work / "dbt_packages").mkdir(exist_ok=True)
            code, _, _ = package_fixture.dbt(case, ["build"], session)
            rows = package_fixture.duckdb_query(env.work / "kitcase" / "x.duckdb", "select 1", session)
            listing = package_fixture.selection(case, ["+a"], session)
    finally:
        package_fixture.TEST_REGISTRY = None
    bodies = [b for name, b in env.daemon.create_bodies.items() if "-dbt-" in name]
    command = bodies[0]["Cmd"] if bodies else []
    mounts = bodies[0]["HostConfig"]["Mounts"] if bodies else []
    return {
        "dbt ran as a fixture_dbt attempt": code == 0 and bodies != [] and bodies[0]["Labels"]["hai.template"] == "fixture_dbt",
        "every dbt call passes check_release_copy_conflicts false": all(
            any(part == '{"check_release_copy_conflicts": false}' for part in b["Cmd"]) for b in bodies
        ),
        "the command carries the template's target and project, not the caller's": "--target" in command
        and "fixture" in command
        and "--project-dir" in command,
        "only the case folder is writable": [m["Target"] for m in mounts if not m["ReadOnly"]] == ["/workspace/out/kitcase"],
        "a duckdb query went through duckdb_helper and returned the rows": rows == [["table_a", "3"]],
        "dbt ls output is read from the attempt": listing == [],
        "the kit made no Docker call of its own": not run_command(
            sys.executable, ["-m", "scripts.quality.docker_calls", "scripts/lakehouse/package_fixture.py"], cwd=ROOT, timeout=60
        ).returncode,
    }


@case("dbt_and_silver_clients")
def dbt_and_silver_clients(env: Env) -> dict[str, bool]:
    """919: `dbt.sh deps` and `silver validate` are launcher clients; each runs its template and reports the template's exit code."""
    from scripts.lakehouse import silver

    def on_start(container: dict[str, Any]) -> None:
        if container["Config"]["Labels"].get("hai.template") == "quality_validate":
            mount = next(m for m in container["Mounts"] if m["Destination"] == "/workspace/out")
            (Path(mount["Source"]) / "result.json").write_text(json.dumps({"counts": {"expectations": 0}, "expectations": []}))

    env.daemon.on_start = on_start
    env.start()
    deps = run_command(sys.executable, ["-m", "orchestration.launcher", "--test-registry", str(env.registry), "dbt", "deps"], cwd=ROOT, timeout=120)
    created = [name for name in env.daemon.create_bodies if "-deps-" in name]
    build = ROOT / "data" / "analytics" / "dbt"
    build.mkdir(parents=True, exist_ok=True)
    database = build / "staging.duckdb"
    manifest = build / "e2e" / "real_again" / "target" / "manifest.json"
    made = []
    for path, text in ((database, ""), (manifest, json.dumps({"nodes": {}}))):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
            made.append(path)
    silver.TEST_REGISTRY = str(env.registry)
    try:
        code, record = silver.validate(database, silver.CONTRACTS / "suites", silver.CONTRACTS / "baselines.json", manifest)
        run_json = json.loads(record.read_text())
        shutil.rmtree(record.parent, ignore_errors=True)
    finally:
        silver.TEST_REGISTRY = None
        for path in made:
            path.unlink(missing_ok=True)
    body = env.daemon.create_bodies[next(name for name in env.daemon.create_bodies if "-silver-" in name)]
    return {
        "dbt deps ran as a dbt_deps attempt and exited 0": deps.returncode == 0 and len(created) == 1,
        "silver validate ran as a quality_validate attempt": code == 0 and run_json["exit_code"] == 0,
        "the quality container has no network": body["HostConfig"]["NetworkMode"] == "none",
        "the validation record carries the allocation's launch plan": "budget" in run_json["launch"],
    }


@case("catalog_client")
def catalog_client(env: Env) -> dict[str, bool]:
    """919: scripts/lakehouse/catalog.py starts, reports, runs jobs and stops the catalog through the launcher; the Polaris API is a stand-in."""
    from scripts.lakehouse import catalog

    state = env.temp / "lakehouse_cli"
    secrets_file = state / "secrets" / "catalog.env"
    values = {
        "POSTGRES_PASSWORD": "pg-x",
        "POLARIS_CLIENT_ID": "root",
        "POLARIS_CLIENT_SECRET": "pcs-x",
        "POLARIS_READER_CLIENT_ID": "reader-id",
        "POLARIS_READER_CLIENT_SECRET": "prs-x",
    }
    catalog.write_private(secrets_file, values)
    deployment = env.temp / "deployment.json"
    deployment.write_text(json.dumps({"aws_profile": "fake-profile", "aws_region": "us-fake-1", "data_bucket_name": "fake-bucket"}))
    env.settings.update({"compose_env": str(state / "secrets" / "compose.env"), "lakehouse_state": str(state)})
    env.write_settings()
    saved = (catalog.STATE, catalog.SECRETS, catalog.COMPOSE_ENV, catalog.BOOTSTRAPPED, catalog.DEPLOYMENT, catalog.TEST_REGISTRY, catalog.request)
    location = "s3://fake-bucket/lakehouse/"

    def fake_request(
        method: str, path: str, token: str | None = None, body: dict[str, Any] | None = None, form: dict[str, str] | None = None
    ) -> tuple[int, Any]:
        if path.endswith("/oauth/tokens"):
            return 200, {"access_token": "t"}
        if method == "GET" and path.endswith(f"/catalogs/{catalog.CATALOG}"):
            return 200, {"properties": {"default-base-location": location}, "storageConfigInfo": {"allowedLocations": [location], "stsUnavailable": True}}
        return 200, {}

    catalog.STATE, catalog.SECRETS, catalog.COMPOSE_ENV = state, secrets_file, state / "secrets" / "compose.env"
    catalog.BOOTSTRAPPED, catalog.DEPLOYMENT, catalog.TEST_REGISTRY, catalog.request = (
        state / "polaris_bootstrapped",
        deployment,
        str(env.registry),
        fake_request,
    )
    catalog_script(env, others_run_forever=False)
    env.settings["restart_command"] = restart_command(env, 0.2, 0.5)
    env.write_settings()
    outputs: dict[str, str] = {}
    fixture_writer: list[str | None] = []

    def refusal_text(call: Callable[[], Any]) -> str | None:
        """The catalog client's refusal text of a call, or None when it was accepted."""
        try:
            call()
        except catalog.CatalogError as error:
            return str(error)
        return None

    try:
        for label, call in (
            ("up", lambda: catalog.up()),
            ("status", lambda: catalog.status()),
            ("job_e2e", lambda: catalog.job("bronze_e2e", [])),
            ("job_smoke", lambda: fixture_writer.append(refusal_text(lambda: catalog.job("smoke", [])))),
            ("down", lambda: catalog.down()),
        ):
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                call()
            outputs[label] = buffer.getvalue()
        with contextlib.redirect_stdout(io.StringIO()):
            released = catalog.release()
        refused_job = None
        try:
            catalog.job("nonexistent", [])
        except catalog.CatalogError as error:
            refused_job = str(error)

    finally:
        (catalog.STATE, catalog.SECRETS, catalog.COMPOSE_ENV, catalog.BOOTSTRAPPED, catalog.DEPLOYMENT, catalog.TEST_REGISTRY, catalog.request) = saved
    compose_env = (state / "secrets" / "compose.env").read_text()
    return {
        "up started Polaris through the launcher and reported it": "Polaris is running on 127.0.0.1:8181" in outputs["up"],
        "status names the services": "polaris-db=" in outputs["status"] and "polaris=" in outputs["status"],
        "a throwaway-catalog job ran as spark_e2e": any(c["Labels"]["hai.template"] == "spark_e2e" for c in env.daemon.create_bodies.values()),
        "a loader job in a test registry (a fixture run) is refused by the launcher, its run is released and no spark_write container exists": (
            bool(fixture_writer)
            and "mode real" in str(fixture_writer[0])
            and all(r["state"] == "released" for r in env.state()["runs"].values())
            and not any(c["Labels"]["hai.template"] == "spark_write" for c in env.daemon.create_bodies.values())
        ),
        "the Spark heap line is printed": "Spark heap " in outputs["job_e2e"],
        "down stopped the catalog": "Catalog services stopped" in outputs["down"] and catalog_phase(env) == "down",
        "release restarted Docker": released.startswith("Docker restarted"),
        "an unknown job is refused": refused_job is not None and "unknown job" in refused_job,
        "compose.env holds credentials by name and no memory limit": "POSTGRES_PASSWORD=" in compose_env and "MEMORY_LIMIT" not in compose_env,
    }


def real_query_ok(env: Env, run_id: str) -> bool:
    """The create specification of the real-build query: a duckdb_helper container whose run-folder mount is the only data mount."""
    bodies = [
        b for b in env.daemon.create_bodies.values() if b["Labels"].get("hai.template") == "duckdb_helper" and "/workspace/out/real_build.duckdb" in b["Cmd"]
    ]
    if len(bodies) != 1:
        return False
    body = bodies[0]
    data_mounts = [m for m in body["HostConfig"]["Mounts"] if m["Target"] == "/workspace/out"]
    return (
        body["HostConfig"]["NetworkMode"] == "none"
        and len(data_mounts) == 1
        and data_mounts[0]["ReadOnly"]
        and data_mounts[0]["Source"].endswith(f"/runs/{run_id}")
    )


@case("staging_read_share", stub_host=True)
def staging_read_share(env: Env) -> dict[str, bool]:
    """911 (staging run 5): a light read of the staging runner takes a small share of the plan, so memory that another job takes between its
    allocation and its launch does not hold it queued; a share of the whole plan would wait until the machine is nearly empty."""
    from scripts.lakehouse import run_staging_e2e as staging

    def on_start(container: dict[str, Any]) -> None:
        if container["Config"]["Labels"].get("hai.template") == "duckdb_helper":
            container["_out"] = "n,2\n"

    env.daemon.on_start = on_start
    env.start()
    plan = int(env.client().call("plan")["environment"]["JOB_MEMORY_LIMIT"])
    rogue = env.daemon.create_rogue("external-growing", {}, running=True)
    env.daemon.usage["external-growing"] = 0
    real_allocate = launcher.LauncherClient.allocate

    def allocate_then_grow(self: launcher.LauncherClient, *args: Any, **kwargs: Any) -> str:
        # Another job grows after the read's plan was taken: all but 6 GiB of that plan is used before the read launches.
        allocation = real_allocate(self, *args, **kwargs)
        env.daemon.usage["external-growing"] = max(0, plan - 6 * GIB)
        return allocation

    case_name = "read_share_probe"
    (staging.CASES / case_name).mkdir(parents=True, exist_ok=True)
    staging.TEST_REGISTRY = str(env.registry)
    outcome: list[Any] = []

    def read() -> None:
        try:
            outcome.append(staging.duckdb_csv(f"{staging.CONTAINER_OUT}/e2e/{case_name}/staging.duckdb", "select 1"))
        except (RuntimeError, OSError, launcher.ClientError) as error:
            outcome.append(error)

    try:
        with mock.patch.object(launcher.LauncherClient, "allocate", allocate_then_grow):
            thread = threading.Thread(target=read)
            thread.start()
            thread.join(30)
            waited = thread.is_alive()
            if waited:
                # The unfixed read waits on memory: cancel it so the case ends.
                for attempt in env.state()["attempts"].values():
                    if attempt["phase"] != "ended" and attempt["owner"].startswith("alloc:"):
                        env.client().call("cancel", {"attempt_id": attempt["id"]}, env.alloc_principal(attempt["owner"].split(":", 1)[1]))
                thread.join(60)
    finally:
        staging.TEST_REGISTRY = None
        env.daemon.containers.pop(rogue, None)
        env.daemon.usage.pop("external-growing", None)
        shutil.rmtree(staging.CASES / case_name, ignore_errors=True)
    shares = [int(a["environment"]["JOB_MEMORY_LIMIT"]) for a in env.state()["allocations"].values() if a.get("environment")]
    return {
        "the read ended without waiting on memory": not waited,
        "the read returned the helper's rows": outcome == [[["n", "2"]]],
        "the read's share is a fraction of the plan": bool(shares) and max(shares) < plan,
    }


@case("staging_read_plan_shrinks", stub_host=True)
def staging_read_plan_shrinks(env: Env) -> dict[str, bool]:
    """911 (staging run 6): memory that shrinks between the runner's plan reading and the allocation's own plan refuses the share count it
    chose; the light read retries with fewer shares instead of failing the run."""
    from scripts.lakehouse import run_staging_e2e as staging

    def on_start(container: dict[str, Any]) -> None:
        if container["Config"]["Labels"].get("hai.template") == "duckdb_helper":
            container["_out"] = "n,2\n"

    env.daemon.on_start = on_start
    env.start()
    real_call = launcher.LauncherClient.call
    asked: list[int] = []

    def plan_reads_high(self: launcher.LauncherClient, method: str, *args: Any, **kwargs: Any) -> Any:
        # The plan reading sees three times the memory the allocation's own plan will find a moment later.
        reply = real_call(self, method, *args, **kwargs)
        if method == "plan":
            reply = {**reply, "environment": {**reply["environment"], "JOB_MEMORY_LIMIT": str(3 * int(reply["environment"]["JOB_MEMORY_LIMIT"]))}}
        if method == "allocate":
            asked.append(int((args[0] if args else kwargs.get("params", {})).get("workers", 1)))
        return reply

    case_name = "read_plan_shrinks_probe"
    (staging.CASES / case_name).mkdir(parents=True, exist_ok=True)
    staging.TEST_REGISTRY = str(env.registry)
    outcome: Any = None
    try:
        with mock.patch.object(launcher.LauncherClient, "call", plan_reads_high):
            outcome = staging.duckdb_csv(f"{staging.CONTAINER_OUT}/e2e/{case_name}/staging.duckdb", "select 1")
    except (RuntimeError, OSError, launcher.ClientError) as error:
        outcome = error
    finally:
        staging.TEST_REGISTRY = None
        shutil.rmtree(staging.CASES / case_name, ignore_errors=True)
    return {
        "the read returned the helper's rows": outcome == [["n", "2"]],
        "the first share count was refused and a smaller one followed": len(asked) >= 2 and asked == sorted(asked, reverse=True) and asked[-1] < asked[0],
    }


@case("staging_runner_conversion")
def staging_runner_conversion(env: Env) -> dict[str, bool]:
    """919: the staging runner's fixture launches (init, build, queries, packages) are launcher attempts; the case folder is the only writable mount."""
    from scripts.lakehouse import run_staging_e2e as staging

    def on_start(container: dict[str, Any]) -> None:
        if container["Config"]["Labels"].get("hai.template") == "duckdb_helper":
            container["_out"] = "n,2\n"

    env.daemon.on_start = on_start
    env.start()
    staging.TEST_REGISTRY = str(env.registry)
    case_name = "l1a_conversion_probe"
    saved_runs = staging.RUNS
    try:
        staging.install_packages()
        code, statuses = staging.run_fixture(case_name, staging.BASE)
        rows = staging.duckdb_csv(f"{staging.CONTAINER_OUT}/e2e/{case_name}/staging.duckdb", "select 1")
        refused_real = None
        try:
            staging.duckdb_csv(f"{staging.CONTAINER_OUT}/staging.duckdb", "select 1")
        except RuntimeError as error:
            refused_real = str(error)
        # The real query of an open run: the run's folder is the only mount, read-only, with no network.
        run_id = "20261012T000000Z-l1aprobe"
        staging.RUNS = staging.OUT / "runs"  # inside this checkout's data folder (a worktree's catalog state points at the main checkout)
        (staging.RUNS / run_id).mkdir(parents=True, exist_ok=True)
        staging.CURRENT_RUN["id"] = run_id
        real_rows = staging.duckdb_csv(f"{staging.CONTAINER_OUT}/real_build.duckdb", "select 1")
    finally:
        staging.TEST_REGISTRY = None
        staging.CURRENT_RUN.clear()
        shutil.rmtree(staging.RUNS / "20261012T000000Z-l1aprobe", ignore_errors=True)
        staging.RUNS = saved_runs
        shutil.rmtree(staging.CASES / case_name, ignore_errors=True)
    templates = {c["Labels"]["hai.template"] for c in env.daemon.create_bodies.values() if "hai.template" in c["Labels"]}
    build = next(b for name, b in env.daemon.create_bodies.items() if "build-l1a" in name)
    return {
        "the runner used dbt_deps, fixture_init, fixture_dbt and duckdb_helper": {"dbt_deps", "fixture_init", "fixture_dbt", "duckdb_helper"} <= templates,
        "the fixture build returned its exit code and statuses": code == 0 and statuses == {},
        "a query returned the helper's rows": rows == [["n", "2"]],
        "the build has no network and mounts only its case and the packages": build["HostConfig"]["NetworkMode"] == "none"
        and sorted(m["Target"] for m in build["HostConfig"]["Mounts"])
        == sorted([f"/workspace/out/e2e/{case_name}", "/opt/analytics/resources.sql", "/workspace/out/dbt_packages"]),
        "a real-build query without an open real run is refused": refused_real is not None and "open real run" in refused_real,
        "a real-build query of an open run is accepted, returns the helper's rows and mounts only the run folder read-only with no network": real_rows
        == [["n", "2"]]
        and real_query_ok(env, run_id),
    }


class HeldSocket(socket.socket):
    """A socket whose connection establishment never completes: it waits out its timeout (or 30 s when it has none)."""

    def connect(self, address: Any) -> None:
        timeout = self.gettimeout()
        time.sleep(30.0 if timeout is None else timeout)
        raise TimeoutError(f"synthetic held connection establishment to {address!r}")


class PoolFaultClient(launcher.LauncherClient):
    """Drop replies or refuse a transient socket operation after the real socket has handled its request."""

    fault = ""
    failed: set[str]
    acknowledged: list[str]
    cancel_started = 0.0

    def exchange(self, sock: socket.socket, method: str, params: dict[str, Any], principal: dict[str, Any], request_id: str, timeout: float) -> Any:
        if method == "cancel" and not self.cancel_started:
            self.cancel_started = time.monotonic()
        if self.fault == "unavailable" and self.cancel_started and method in ("cancel", "status", "submit"):
            raise OSError("synthetic persistent socket outage")
        if self.fault == "gone_after_cancel" and self.acknowledged and method == "status":
            raise OSError("synthetic launcher gone after every cancellation persisted")
        if self.fault == "socket_loss" and method == "cancel" and time.monotonic() - self.cancel_started < 0.6:
            raise OSError("synthetic socket loss before cancel persistence")
        reply = super().exchange(sock, method, params, principal, request_id, timeout)
        drop = "cancel" if self.fault == "lost_cancel" else "submit" if self.fault == "lost_submit" else ""
        if method == drop and method not in self.failed:
            self.failed.add(method)
            raise OSError("synthetic lost reply after persistence")
        return reply

    def open_socket(self, timeout: float | None = None) -> socket.socket:
        # Once the pool is cancelling, connection establishment is held (a listener that never completes the connection).
        self.socket_factory = HeldSocket if self.fault == "connect_held" and self.cancelling else socket.socket
        return super().open_socket(timeout)

    def cancel_open(self) -> list[str]:
        self.acknowledged = super().cancel_open()
        return self.acknowledged


@case("staging_pool_socket_loss", stub_host=True, pool_fault="socket_loss")
def staging_pool_socket_loss(env: Env) -> dict[str, bool]:
    return staging_pool_interrupt(env)


@case("staging_pool_lost_cancel", stub_host=True, pool_fault="lost_cancel")
def staging_pool_lost_cancel(env: Env) -> dict[str, bool]:
    return staging_pool_interrupt(env)


@case("staging_pool_lost_submit", stub_host=True, pool_fault="lost_submit")
def staging_pool_lost_submit(env: Env) -> dict[str, bool]:
    return staging_pool_interrupt(env)


@case("staging_pool_interrupt", stub_host=True)
def staging_pool_interrupt(env: Env) -> dict[str, bool]:
    """919 and 911 (F18): an interrupt of the fixture pool persists a cancel for its running attempts first, then joins the workers once cleanup ends."""
    from scripts.lakehouse import run_staging_e2e as staging

    hold_forever(env)
    env.start()
    staging.TEST_REGISTRY = str(env.registry)
    saved_factory = staging.launcher_client
    fault_client = PoolFaultClient(env.config)
    fault_client.fault = str(env.settings.get("pool_fault", ""))
    fault_client.failed = set()
    fault_client.acknowledged = []
    staging.launcher_client = lambda: fault_client
    names = ["l1a_pool_a", "l1a_pool_b"]
    jobs = [(name, tuple(staging.BASE), frozenset[str]()) for name in names]
    saved_sigint = signal.getsignal(signal.SIGINT)
    gate = env.daemon.add_gate("remove", "before")  # one removal stays unresolved while the pool is interrupted
    interrupted: list[BaseException] = []
    observed: dict[str, Any] = {}

    def interrupt_when_running() -> None:
        try:
            env.wait(
                lambda: len([a for a in env.state()["attempts"].values() if a["phase"] == "started" and a["owner"].startswith("alloc:")]) >= 2,
                40,
                "two workers",
            )
        finally:
            os.kill(os.getpid(), signal.SIGINT)
        time.sleep(1.0)
        observed["persisted_without_fallback"] = all(a["cancel_intent"] for a in env.state()["attempts"].values() if a["owner"].startswith("alloc:"))
        if not observed["persisted_without_fallback"]:
            for a in env.state()["attempts"].values():
                if a["owner"].startswith("alloc:") and a["phase"] != "ended":
                    env.client().call("cancel", {"attempt_id": a["id"]}, env.alloc_principal(a["owner"].partition(":")[2]))
        gate.wait_reached(30)
        allocation_id = next(a["owner"].partition(":")[2] for a in env.state()["attempts"].values() if a["owner"].startswith("alloc:"))
        observed["release"] = expect_error(lambda: env.client().call("release_allocation", {}, env.alloc_principal(allocation_id)))
        observed["held_during_removal"] = env.state()["allocations"][allocation_id]["state"] == "held"
        time.sleep(1.0)
        observed["joined_before_gate_opened"] = bool(observed.get("returned"))
        gate.release()

    thread = threading.Thread(target=interrupt_when_running)
    thread.start()
    begun = time.monotonic()
    try:
        staging.build_cases(jobs, 2)
    except KeyboardInterrupt as error:
        interrupted.append(error)
    finally:
        observed["returned"] = time.monotonic()
        thread.join()
        signal.signal(signal.SIGINT, saved_sigint)
        staging.TEST_REGISTRY = None
        staging.launcher_client = saved_factory
        for name in names:
            shutil.rmtree(staging.CASES / name, ignore_errors=True)
    took = time.monotonic() - begun
    state = env.state()
    attempts = [a for a in state["attempts"].values() if a["owner"].startswith("alloc:")]
    allocation_id = attempts[0]["owner"].partition(":")[2] if attempts else ""
    return {
        "the interrupt reached the pool": bool(interrupted),
        "every cancellation persisted without the harness fallback": bool(observed.get("persisted_without_fallback")),
        "lost replies were reconciled and every cancellation acknowledged": len(fault_client.acknowledged) == len(attempts),
        "two attempts ran and each ended cancelled by a persisted cancel intent, not by holder death": len(attempts) >= 2
        and all(a["reason"] == "cancelled" and a["cancel_intent"]["reason"] == "cancelled" for a in attempts),
        "a release during the unresolved removal is refused and the allocation stays held": refused(observed.get("release"), "refused")
        and observed["held_during_removal"],
        "the pool joined its workers only after cleanup ended (not before the removal was resolved)": not observed["joined_before_gate_opened"] and took >= 1.0,
        "the pool released its allocation once absence was proved": bool(allocation_id) and state["allocations"][allocation_id]["state"] == "released",
        "no container is left": env.labelled() == [],
    }


@case("staging_pool_cancel_unresolved", stub_host=True)
def staging_pool_cancel_unresolved(env: Env) -> dict[str, bool]:
    """919: a persistent socket outage produces a bounded visible failure and retains allocation instead of joining."""
    from scripts.lakehouse import run_staging_e2e as staging

    hold_forever(env)
    env.start()
    staging.TEST_REGISTRY = str(env.registry)
    saved_factory = staging.launcher_client
    fault_client = PoolFaultClient(env.config)
    fault_client.fault, fault_client.failed, fault_client.acknowledged = str(env.settings.get("pool_fault", "unavailable")), set(), []
    staging.launcher_client = lambda: fault_client
    saved_sigint = signal.getsignal(signal.SIGINT)
    returned = threading.Event()
    observed: dict[str, Any] = {}
    names = ["l1a_unresolved_a", "l1a_unresolved_b"]

    def interrupt() -> None:
        env.wait(lambda: len([a for a in env.state()["attempts"].values() if a["phase"] == "started" and a["owner"].startswith("alloc:")]) == 2, 30)
        observed["at"] = time.monotonic()
        os.kill(os.getpid(), signal.SIGINT)
        returned.wait(8)  # rescue the unfixed joining path, without claiming its cancellation was successful
        allocation = next(a["owner"].partition(":")[2] for a in env.state()["attempts"].values() if a["owner"].startswith("alloc:"))
        observed["allocation"] = allocation
        observed["retained"] = env.state()["allocations"][allocation]["state"] == "held"
        observed["unresolved_at_return"] = [a["id"] for a in env.state()["attempts"].values() if a["owner"].startswith("alloc:") and a["phase"] != "ended"]
        for a in env.state()["attempts"].values():
            if a["owner"].startswith("alloc:") and a["phase"] != "ended":
                env.client().call("cancel", {"attempt_id": a["id"]}, env.alloc_principal(allocation))
                env.wait_ended(a["id"])
        fault_client.fault = ""

    thread = threading.Thread(target=interrupt)
    thread.start()
    error: BaseException | None = None
    try:
        staging.build_cases([(name, tuple(staging.BASE), frozenset()) for name in names], 2)
    except (KeyboardInterrupt, launcher.ClientError) as caught:
        error = caught
    finally:
        observed["return_at"] = time.monotonic()
        returned.set()
        thread.join(20)
        signal.signal(signal.SIGINT, saved_sigint)
        staging.TEST_REGISTRY = None
        staging.launcher_client = saved_factory
        for name in names:
            shutil.rmtree(staging.CASES / name, ignore_errors=True)
    allocation = observed["allocation"]
    if env.state()["allocations"][allocation]["state"] == "held":
        env.client().release_allocation(allocation)
    path = env.registry / "pool_cancel_unresolved.json"
    record = json.loads(path.read_text()) if path.exists() else {}
    env.wait(lambda: not any(t.name.startswith("ThreadPoolExecutor") for t in threading.enumerate()), 5, "pool worker threads exit")
    return {
        "bounded failure names unresolved cleanup": isinstance(error, launcher.ClientError)
        and error.code == "cleanup_unresolved"
        and observed["return_at"] - observed["at"] < 7,
        "failed cancellation retains admission and open attempt IDs": observed["retained"] and len(observed["unresolved_at_return"]) == 2,
        "durable recovery record names every unresolved attempt": record.get("allocation_retained") is True
        and set(record.get("unresolved", [])) == set(observed["unresolved_at_return"]),
    }


@case("staging_pool_connect_held", stub_host=True, pool_fault="connect_held")
def staging_pool_connect_held(env: Env) -> dict[str, bool]:
    """919 (F18): connection establishment held during the pool's cancellation still fails within the cancel window, with admission kept."""
    return staging_pool_cancel_unresolved(env)


@case("staging_pool_launcher_gone", stub_host=True, pool_fault="gone_after_cancel", timings={"cancel_settle": 3.0})
def staging_pool_launcher_gone(env: Env) -> dict[str, bool]:
    """919 (F18): every cancellation persists, then the launcher stays unreachable; the pool fails visibly after the settle limit instead of polling forever."""
    from scripts.lakehouse import run_staging_e2e as staging

    hold_forever(env)
    env.start()
    staging.TEST_REGISTRY = str(env.registry)
    saved_factory = staging.launcher_client
    fault_client = PoolFaultClient(env.config)
    fault_client.fault, fault_client.failed, fault_client.acknowledged = "gone_after_cancel", set(), []
    staging.launcher_client = lambda: fault_client
    saved_sigint = signal.getsignal(signal.SIGINT)
    returned = threading.Event()
    observed: dict[str, Any] = {}
    names = ["l1a_gone_a", "l1a_gone_b"]
    gate = env.daemon.add_gate("remove", "before")  # the cancelled containers stay until the launcher is back, so no attempt ends early

    def interrupt() -> None:
        env.wait(lambda: len([a for a in env.state()["attempts"].values() if a["phase"] == "started" and a["owner"].startswith("alloc:")]) == 2, 30)
        observed["at"] = time.monotonic()
        os.kill(os.getpid(), signal.SIGINT)
        returned.wait(20)
        attempts = [a for a in env.state()["attempts"].values() if a["owner"].startswith("alloc:")]
        observed["allocation"] = attempts[0]["owner"].partition(":")[2]
        observed["retained"] = env.state()["allocations"][observed["allocation"]]["state"] == "held"
        observed["persisted"] = all(a["cancel_intent"] for a in attempts)
        fault_client.fault = ""
        gate.release()
        for a in attempts:
            env.wait_ended(a["id"])

    thread = threading.Thread(target=interrupt)
    thread.start()
    error: BaseException | None = None
    try:
        staging.build_cases([(name, tuple(staging.BASE), frozenset()) for name in names], 2)
    except (KeyboardInterrupt, launcher.ClientError) as caught:
        error = caught
    finally:
        observed["return_at"] = time.monotonic()
        returned.set()
        thread.join(40)
        signal.signal(signal.SIGINT, saved_sigint)
        staging.TEST_REGISTRY = None
        staging.launcher_client = saved_factory
        for name in names:
            shutil.rmtree(staging.CASES / name, ignore_errors=True)
    allocation = observed["allocation"]
    if env.state()["allocations"][allocation]["state"] == "held":
        env.client().release_allocation(allocation)
    path = env.registry / "pool_cancel_unresolved.json"
    record = json.loads(path.read_text()) if path.exists() else {}
    env.wait(lambda: not any(t.name.startswith("ThreadPoolExecutor") for t in threading.enumerate()), 5, "pool worker threads exit")
    return {
        "every cancellation persisted before the launcher was lost": bool(observed.get("persisted")) and len(fault_client.acknowledged) == 2,
        "the pool failed visibly after the settle limit instead of polling forever": isinstance(error, launcher.ClientError)
        and error.code == "cleanup_unresolved"
        and observed["return_at"] - observed["at"] < 12,
        "admission was retained when the pool returned": bool(observed.get("retained")),
        "the durable record names the reason and keeps the allocation": record.get("allocation_retained") is True
        and "unreachable after the cancellations persisted" in str(record.get("reason")),
    }


@case("staging_pool_removal_held", stub_host=True, timings={"cancel_settle": 1.0})
def staging_pool_removal_held(env: Env) -> dict[str, bool]:
    """919 (F18, delta 3): the launcher stays reachable while a cancelled container's removal hangs; the settle limit applies to successful replies too."""
    from scripts.lakehouse import run_staging_e2e as staging

    hold_forever(env)
    env.start()
    staging.TEST_REGISTRY = str(env.registry)
    saved_factory = staging.launcher_client
    pool_client = PoolFaultClient(env.config)
    pool_client.fault, pool_client.failed, pool_client.acknowledged = "", set(), []
    staging.launcher_client = lambda: pool_client
    saved_sigint = signal.getsignal(signal.SIGINT)
    returned = threading.Event()
    observed: dict[str, Any] = {}
    names = ["l1a_removal_a", "l1a_removal_b"]
    gate = env.daemon.add_gate("remove", "before")  # Docker removal hangs; the launcher keeps answering with the attempt in exiting

    def interrupt() -> None:
        env.wait(lambda: len([a for a in env.state()["attempts"].values() if a["phase"] == "started" and a["owner"].startswith("alloc:")]) == 2, 30)
        os.kill(os.getpid(), signal.SIGINT)
        env.wait(lambda: all(a["cancel_intent"] for a in env.state()["attempts"].values() if a["owner"].startswith("alloc:")), 10, "cancel intents")
        observed["persisted_at"] = time.monotonic()
        returned.wait(8)  # rescue an unbounded wait without claiming it was bounded
        attempts = [a for a in env.state()["attempts"].values() if a["owner"].startswith("alloc:")]
        observed["allocation"] = attempts[0]["owner"].partition(":")[2]
        observed["retained"] = env.state()["allocations"][observed["allocation"]]["state"] == "held"
        observed["launcher_answered"] = env.ping().get("status") == "serving"
        observed["open_at_return"] = [a["id"] for a in attempts if a["phase"] != "ended"]
        gate.release()
        for a in attempts:
            env.wait_ended(a["id"])

    thread = threading.Thread(target=interrupt)
    thread.start()
    error: BaseException | None = None
    try:
        staging.build_cases([(name, tuple(staging.BASE), frozenset()) for name in names], 2)
    except (KeyboardInterrupt, launcher.ClientError) as caught:
        error = caught
    finally:
        observed["return_at"] = time.monotonic()
        returned.set()
        thread.join(40)
        signal.signal(signal.SIGINT, saved_sigint)
        staging.TEST_REGISTRY = None
        staging.launcher_client = saved_factory
        for name in names:
            shutil.rmtree(staging.CASES / name, ignore_errors=True)
    allocation = observed["allocation"]
    if env.state()["allocations"][allocation]["state"] == "held":
        env.client().release_allocation(allocation)
    path = env.registry / "pool_cancel_unresolved.json"
    record = json.loads(path.read_text()) if path.exists() else {}
    env.wait(lambda: not any(t.name.startswith("ThreadPoolExecutor") for t in threading.enumerate()), 5, "pool worker threads exit")
    waited = observed["return_at"] - observed.get("persisted_at", observed["return_at"])
    return {
        "the launcher kept answering while the removal hung": bool(observed.get("launcher_answered")),
        "the pool failed visibly within the settle limit after the cancellations persisted": isinstance(error, launcher.ClientError)
        and error.code == "cleanup_unresolved"
        and waited < 4.0,
        "admission was retained with the unended attempt open": bool(observed.get("retained")) and len(observed.get("open_at_return", [])) >= 1,
        "the durable record names the unended attempt and keeps the allocation": record.get("allocation_retained") is True
        and set(observed.get("open_at_return", [])) <= set(record.get("unresolved", [])),
    }


@case("client_slow_launcher_replies")
def client_slow_launcher_replies(env: Env) -> dict[str, bool]:
    """919 (C3b staging run): a launcher that is slow to answer the submit or a few status polls does not end the client's wait; a launcher
    that stays silent past the patience still fails the wait visibly."""
    env.start()
    client = env.client()
    allocation = env.allocate()
    principal = env.alloc_principal(allocation)
    real_call = client.bounded_call
    slow = {"submit": 2, "status": 3}

    def busy_launcher(method: str, *args: Any, **kwargs: Any) -> Any:
        # The first replies time out, as from a launcher busy writing a large registry on a loaded machine.
        if slow.get(method, 0) > 0:
            slow[method] -= 1
            raise TimeoutError("timed out")
        return real_call(method, *args, **kwargs)

    outcome: Any = None
    with mock.patch.object(client, "bounded_call", busy_launcher):
        try:
            outcome = client.run_attempt(principal, "fixture_dbt", "slowpoll", ["build"], env.fixture_params("slowpoll"), checkout=ROOT)
        except (OSError, launcher.ClientError) as error:
            outcome = error
    attempts = [a for a in env.state()["attempts"].values() if a["launch_id"].startswith("slowpoll")]
    timings = replace(env.config.timings, poll_patience=2.0) if hasattr(env.config.timings, "poll_patience") else env.config.timings
    impatient = launcher.LauncherClient(replace(env.config, timings=timings), start=False)
    real_impatient = impatient.bounded_call
    silent: Any = None
    begun = time.monotonic()

    def silent_launcher(method: str, *args: Any, **kwargs: Any) -> Any:
        if method == "status":
            raise TimeoutError("timed out")
        return real_impatient(method, *args, **kwargs)

    with mock.patch.object(impatient, "bounded_call", silent_launcher):
        try:
            impatient.run_attempt(principal, "fixture_dbt", "silentpoll", ["build"], env.fixture_params("silentpoll"), checkout=ROOT)
        except (OSError, launcher.ClientError) as error:
            silent = error
    took = time.monotonic() - begun
    for attempt in env.state()["attempts"].values():
        if attempt["launch_id"].startswith("silentpoll") and attempt["phase"] != "ended":
            env.client().call("cancel", {"attempt_id": attempt["id"]}, principal)
            env.wait_ended(attempt["id"], 30)
    return {
        "slow replies: the attempt result is returned": isinstance(outcome, launcher.AttemptResult) and outcome.ok,
        "slow replies: one attempt only (the retried submit is the same request)": len(attempts) == 1,
        "a silent launcher fails the wait": isinstance(silent, (OSError, launcher.ClientError)),
        "the failure comes after the patience, not before": 2.0 <= took < 15.0,
    }


@case("client_exchange_total_budget")
def client_exchange_total_budget(env: Env) -> dict[str, bool]:
    """919 (F18, delta 3): a reply sent in slow chunks cannot stretch a bounded exchange past its budget."""
    folder = Path(tempfile.mkdtemp(prefix="l1a_xch_", dir="/tmp"))  # a short AF_UNIX path
    config = replace(env.config, registry=run_lock.Registry(folder, "test"))
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(config.socket_path))
    server.listen(1)
    stop = threading.Event()

    def slow_reply() -> None:
        connection, _ = server.accept()
        with connection:
            connection.recv(65536)
            for piece in (b'{"ok":', b"true,", b'"result":', b'{"phase":', b'"started"}', b"}\n"):
                if stop.wait(0.06):
                    return
                with contextlib.suppress(OSError):
                    connection.sendall(piece)

    thread = threading.Thread(target=slow_reply)
    thread.start()
    begun = time.monotonic()
    outcome: Any = None
    try:
        outcome = launcher.LauncherClient(config, start=False).bounded_call("status", {}, {"kind": "any"}, "slow_reply", 0.1)
    except (TimeoutError, OSError, launcher.ClientError) as error:
        outcome = error
    took = time.monotonic() - begun
    stop.set()
    thread.join(5)
    server.close()
    shutil.rmtree(folder, ignore_errors=True)
    return {
        "the slow reply was refused as a timeout": isinstance(outcome, (TimeoutError, OSError)),
        "the call returned within its budget plus scheduling slack": took < 0.25,
        "the server thread ended": not thread.is_alive(),
    }


@case("client_fail_fast_gone", timings={"poll_patience": 12.0, "history_retain": 3600.0})
def client_fail_fast_gone(env: Env) -> dict[str, bool]:
    """A161 fix 2 (failure modes TO1 and TO2): a client whose launcher is gone fails in about a second, not after its patience: with no socket
    at the first submit, and with the launcher killed (socket file left, connection refused) while the client waits for its attempt."""
    client = env.client(start=False)
    begun = time.monotonic()
    missing = outcome_of(
        lambda: client.run_attempt(client.principal_alloc("a_missing"), "fixture_dbt", "gone1", ["build"], env.fixture_params("gone1"), checkout=ROOT)
    )
    took_missing = time.monotonic() - begun
    hold_forever(env)
    env.start()
    allocation = env.allocate()
    observed: dict[str, float] = {}

    def kill_when_started() -> None:
        env.wait(lambda: any(a["phase"] == "started" for a in env.state()["attempts"].values()), 20, "the attempt starts")
        pid = int(env.ping()["pid"])
        observed["killed"] = time.monotonic()
        os.kill(pid, signal.SIGKILL)

    thread = threading.Thread(target=kill_when_started)
    thread.start()
    killed = outcome_of(
        lambda: client.run_attempt(env.alloc_principal(allocation), "fixture_dbt", "gone2", ["build"], env.fixture_params("gone2"), checkout=ROOT)
    )
    took_killed = time.monotonic() - observed.get("killed", time.monotonic())
    thread.join(20)
    env.wait(lambda: not env.alive_launcher(), 15, "the killed launcher is gone")
    env.start()
    (env.report / "timings.json").write_text(
        json.dumps({"no_socket_seconds": round(took_missing, 2), "killed_launcher_seconds": round(took_killed, 2)}, indent=2)
    )
    return {
        "no socket: the client fails in about a second": 0.8 <= took_missing < 4.0,
        "no socket: the failure is a launcher error that says the launcher is not running": isinstance(missing, launcher.ClientError)
        and missing.code == "unavailable"
        and "not running" in str(missing),
        "killed launcher: the client fails in about a second": 0.8 <= took_killed < 4.0,
        "killed launcher: the failure says the launcher is not running": isinstance(killed, launcher.ClientError)
        and killed.code == "unavailable"
        and "not running" in str(killed),
    }


@case("client_fail_fast_alive", timings={"poll_patience": 12.0, "history_retain": 3600.0, "stuck_after": 3.0})
def client_fail_fast_alive(env: Env) -> dict[str, bool]:
    """A161 fix 2 (failure modes TO3 to TO5 and TO7): a launcher that is alive but slow on every request (longer than the old one-second
    exchange budget) keeps the client waiting until the attempt result arrives; a launcher that is alive but blocked fails the client after
    the progress limit, at the first submit and later while it waits; the liveness request reads no registry."""
    with served_here(env) as hook:
        client = env.client(start=False)
        allocation = client.allocate()
        principal = client.principal_alloc(allocation)
        hook.delay = 1.5
        begun = time.monotonic()
        slow = outcome_of(lambda: client.run_attempt(principal, "fixture_dbt", "slow", ["build"], env.fixture_params("slow"), checkout=ROOT))
        took_slow = time.monotonic() - begun
        slow_attempts = [a for a in env.state()["attempts"].values() if a["launch_id"].startswith("slow")]
        hook.delay = 0.0
        hook.blocked = frozenset({"submit"})
        begun = time.monotonic()
        stuck_submit = outcome_of(lambda: client.run_attempt(principal, "fixture_dbt", "stuck1", ["build"], env.fixture_params("stuck1"), checkout=ROOT))
        took_submit = time.monotonic() - begun
        hook.release.set()
        hook.blocked = frozenset()
        for attempt in env.state()["attempts"].values():
            if attempt["launch_id"].startswith("stuck1"):
                env.wait_ended(attempt["id"])
        hook.release.clear()
        hook.blocked = frozenset({"status"})
        begun = time.monotonic()
        stuck_status = outcome_of(lambda: client.run_attempt(principal, "fixture_dbt", "stuck2", ["build"], env.fixture_params("stuck2"), checkout=ROOT))
        took_status = time.monotonic() - begun
        hook.release.set()
        hook.blocked = frozenset()
        for attempt in env.state()["attempts"].values():
            if attempt["launch_id"].startswith("stuck2"):
                env.wait_ended(attempt["id"])
        with compactor(env) as instance:
            left, right = socket.socketpair()
            conn = launcher.Conn(left, launcher.Peer(os.getuid(), os.getpid(), None))
            real = run_lock.transaction
            opened: list[str] = []

            @contextlib.contextmanager
            def counted(*args: Any, **keywords: Any) -> Iterator[dict[str, Any]]:
                opened.append("transaction")
                with real(*args, **keywords) as state:
                    yield state

            replies: dict[str, dict[str, Any]] = {}
            transactions: dict[str, int] = {}
            with mock.patch.object(launcher.run_lock, "transaction", counted):
                for method in ("live", "ping"):
                    opened.clear()
                    instance.handle_line(
                        conn, json.dumps({"v": launcher.PROTOCOL, "id": f"r-{method}", "method": method, "params": {}, "principal": {"kind": "any"}}).encode()
                    )
                    transactions[method] = len(opened)
                    right.settimeout(5)
                    replies[method] = json.loads(right.recv(65536).split(b"\n", 1)[0])
            left.close()
            right.close()
    seconds = {"slow_progressing": took_slow, "blocked_at_submit": took_submit, "blocked_while_waiting": took_status}
    (env.report / "timings.json").write_text(json.dumps({name: round(value, 2) for name, value in seconds.items()}, indent=2))
    return {
        "slow but progressing: the client gets the attempt result": isinstance(slow, launcher.AttemptResult) and slow.ok,
        "slow but progressing: one attempt only (the retried request is the same request)": len(slow_attempts) == 1,
        "slow but progressing: the wait ended well before the patience": took_slow < 12.0,
        "blocked at the submit: the client fails after the progress limit, not the patience": isinstance(stuck_submit, launcher.ClientError)
        and stuck_submit.code == "unresponsive"
        and 2.5 <= took_submit < 8.0,
        "blocked while waiting: the client fails after the progress limit, not the patience": isinstance(stuck_status, launcher.ClientError)
        and stuck_status.code == "unresponsive"
        and 2.5 <= took_status < 8.0,
        "the blocked-launcher failure says the launcher is alive and made no progress": isinstance(stuck_submit, launcher.ClientError)
        and "no progress" in str(stuck_submit),
        "the blocked requests reached the launcher": hook.seen["submit"] >= 1 and hook.seen["status"] >= 1,
        "the liveness request is answered": bool(replies.get("live", {}).get("ok")),
        "the liveness request opens no registry transaction": bool(replies.get("live", {}).get("ok")) and transactions.get("live") == 0,
        "control: a ping opens registry transactions": transactions.get("ping", 0) >= 1,
    }


@case("client_slow_exchange_log", timings={"poll_patience": 12.0, "history_retain": 3600.0})
def client_slow_exchange_log(env: Env) -> dict[str, bool]:
    """A161 fix 3 (failure modes TO10a to TO10d): a client exchange that takes more than half a second writes one log line with the method and
    the seconds; a fast exchange and the wait a status was asked for write none; an exchange that times out writes one line that says so."""
    seen: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            seen.append(record.getMessage())

    log = logging.getLogger("launcher.client")
    handler = Capture(logging.WARNING)
    log.addHandler(handler)
    previous = log.level
    log.setLevel(logging.WARNING)
    try:
        with served_here(env) as hook:
            client = env.client(start=False)
            allocation = client.allocate()
            principal = client.principal_alloc(allocation)
            seen.clear()
            client.call("ping")
            fast = list(seen)
            seen.clear()
            client.call("status", {"what": "allocation", "id": allocation, "wait": 1.0, "until": {"field": "state", "values": ["never"]}}, principal)
            waited = list(seen)
            seen.clear()
            hook.delay = 0.8
            client.call("ping")
            slow = list(seen)
            seen.clear()
            hook.delay = 1.6
            timed_out = outcome_of(lambda: client.bounded_call("ping", {}, {"kind": "any"}, "log-timeout", 0.6))
            late = list(seen)
            hook.delay = 0.0
            client.call("ping")
            seen.clear()
            fast_probe = client.probe(1.0)
            fast_probe_lines = list(seen)

            def hold_loop(delay: float, blocked: bool) -> tuple[threading.Thread, int]:
                """Send a status request that keeps the launcher's loop busy (for ``delay`` seconds, or until released) and wait until it is inside."""
                before = hook.seen["status"]
                if blocked:
                    hook.blocked = frozenset({"status"})
                else:
                    hook.delay = delay
                holder = threading.Thread(target=lambda: outcome_of(lambda: env.client(start=False).call("status", {"what": "launcher"}, timeout=30)))
                holder.start()
                env.wait(lambda: hook.seen["status"] > before, 10, "a status request is inside the launcher's handler")
                return holder, before

            seen.clear()
            holder, _ = hold_loop(0.0, blocked=True)
            begun = time.monotonic()
            silent_probe = client.probe(0.7)
            silent_took = time.monotonic() - begun
            silent_lines = list(seen)
            hook.release.set()
            hook.blocked = frozenset()
            holder.join(20)
            hook.release.clear()
            seen.clear()
            holder, _ = hold_loop(0.8, blocked=False)
            slow_probe = client.probe(2.0)
            slow_probe_lines = [line for line in seen if "exchange live" in line]
            hook.delay = 0.0
            holder.join(20)
            seen.clear()
            nowhere = launcher.LauncherClient(replace(client.config, registry=run_lock.Registry(env.temp / "no_such_registry", "test")), start=False)
            gone_probe = nowhere.probe(1.0)
            gone_lines = list(seen)
    finally:
        log.removeHandler(handler)
        log.setLevel(previous)
    (env.report / "log_lines.json").write_text(
        json.dumps({"slow": slow, "timed_out": late, "silent_probe": silent_lines, "slow_probe": slow_probe_lines, "gone_probe": gone_lines}, indent=2)
    )
    return {
        "a fast exchange writes no line": fast == [],
        "a status that waited on purpose writes no line": waited == [],
        "a slow exchange writes one line": len(slow) == 1,
        "the line names the method and the seconds": len(slow) == 1 and "ping" in slow[0] and re.search(r"0\.[89]\d* s|1\.\d+ s", slow[0]) is not None,
        "an exchange that times out is a timeout": isinstance(timed_out, (TimeoutError, OSError)),
        "it writes one line that says it timed out": len(late) == 1 and "timed out" in late[0] and "ping" in late[0],
        "a fast probe is answered and writes no line": fast_probe == "alive" and fast_probe_lines == [],
        "a probe against a blocked loop is silent for its whole budget": silent_probe == "silent" and 0.6 <= silent_took < 1.5,
        "it writes one line with the method, the outcome and the actual duration": len(silent_lines) == 1
        and re.fullmatch(r"launcher exchange live got no reply after 0\.[6-9]\d* s|launcher exchange live got no reply after 1\.[0-4]\d* s", silent_lines[0])
        is not None,
        "a probe the loop answers late writes one line with its duration": slow_probe == "alive"
        and len(slow_probe_lines) == 1
        and re.fullmatch(r"launcher exchange live took 0\.[5-9]\d* s|launcher exchange live took 1\.[0-4]\d* s", slow_probe_lines[0]) is not None,
        "a probe that finds no listener is instant and writes no line": gone_probe == "gone" and gone_lines == [],
        "no line holds a request ID, a principal or a payload": all(
            re.fullmatch(r"launcher exchange [a-z_]+ [a-z ]+ \d+\.\d+ s( \(asked to wait \d+\.\d+ s\))?", line)
            for line in [*slow, *late, *silent_lines, *slow_probe_lines]
        ),
    }


@case("staging_preflight", timings={"poll_patience": 12.0, "history_retain": 3600.0})
def staging_preflight(env: Env) -> dict[str, bool]:
    """A161 fix 3 and round 4 finding 10 (failure modes PF1 to PF6): the staging runner decides on one ping before it starts anything. A slow
    first exchange is refused even when a second one would be fast; a launcher that does not answer within the limit is refused; a fast launcher
    passes; a launcher that is not running is refused with its own message and is not started, so no attempt is created; the check changes no
    state; the runner exits 2 on a refusal."""
    from scripts.lakehouse import run_staging_e2e as staging

    preflight = getattr(staging, "preflight_launcher", None)
    staging.TEST_REGISTRY = str(env.registry)
    results: dict[str, Any] = {}
    exit_code: int | None = None
    spoken = io.StringIO()
    try:
        with served_here(env) as hook:
            before = env.state()
            hook.delay = 0.0
            results["fast"] = preflight() if preflight else "missing"
            hook.slow_first = 0.8
            begun = time.monotonic()
            results["first_slow"] = preflight() if preflight else "missing"
            first_slow_took = time.monotonic() - begun
            hook.slow_first = 0.0
            hook.seen.clear()
            hook.delay = 0.8
            results["slow"] = preflight() if preflight else "missing"
            slow_exchanges = hook.seen["ping"]
            with mock.patch.object(staging, "PREFLIGHT_WAIT", 1.0, create=True):
                hook.delay = 1.6
                begun = time.monotonic()
                results["silent"] = preflight() if preflight else "missing"
                silent_took = time.monotonic() - begun
            hook.delay = 0.8
            with mock.patch.object(sys, "argv", ["run_staging_e2e", "--test-registry", str(env.registry)]), contextlib.redirect_stderr(spoken):
                exit_code = staging.main()
            hook.delay = 0.0
            env.client(start=False).call("ping")
            after = env.state()
        attempts_before = len(env.state()["attempts"])
        results["cold"] = preflight() if preflight else "missing"
        cold_started = env.alive_launcher()
        attempts_cold = len(env.state()["attempts"])
    finally:
        staging.TEST_REGISTRY = None
    slow, silent, cold = (str(results.get(name)) for name in ("slow", "silent", "cold"))
    return {
        "a fast launcher passes": results.get("fast") is None,
        "a slow first exchange is refused although a second would be fast": results.get("first_slow") not in (None, "missing"),
        "the refused first exchange is the only one": first_slow_took < 1.5,
        "a launcher that stays slow is refused after one exchange": slow not in ("None", "missing") and slow_exchanges == 1,
        "the refusal gives the measured exchange time and the registry size": "ping" in slow and re.search(r"\d+\.\d+ s", slow) is not None and "MB" in slow,
        "the refusal names compaction as a fix to try": "compact" in slow,
        "the refusal does not name the registry size as the cause": not any(word in slow for word in ("because", "cause", "means a large registry")),
        "a launcher that does not answer within the limit is refused": silent not in ("None", "missing") and "within" in silent and silent_took < 1.6,
        "the runner exits 2 on a refusal and says why": exit_code == 2 and "ping" in spoken.getvalue(),
        "a launcher that is not running is refused with its own message": cold not in ("None", "missing") and "not running" in cold,
        "the pre-flight does not start a launcher": not cold_started,
        "no probe attempt or other attempt is created on a cold launcher": attempts_cold == attempts_before,
        "the check changed no attempt, allocation or run on a serving launcher": all(
            after[name].keys() == before[name].keys() for name in ("attempts", "allocations", "runs")
        ),
    }


CLEANUP_TIMINGS = {"poll_patience": 12.0, "history_retain": 3600.0, "stuck_after": 1.5, "cleanup_budget": 4.0}
CLEANUP_BOUND = 1.5 + 1.0 + 4.0 + 3.0


@contextlib.contextmanager
def client_log_lines() -> Iterator[list[str]]:
    """The messages the client writes on its ``launcher.client`` logger while the block runs."""
    seen: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            seen.append(record.getMessage())

    log = logging.getLogger("launcher.client")
    handler = Capture(logging.WARNING)
    log.addHandler(handler)
    previous = log.level
    log.setLevel(logging.WARNING)
    try:
        yield seen
    finally:
        log.removeHandler(handler)
        log.setLevel(previous)


@case("client_cleanup_after_stuck", timings=CLEANUP_TIMINGS)
def client_cleanup_after_stuck(env: Env) -> dict[str, bool]:
    """Round 4 finding 9 (failure modes CU1 to CU5, CU11 and CU14): a caller that releases its allocation after ``run_attempt`` failed
    ``unresponsive`` is not held by the stuck loop. Cleanup is bounded by ``cleanup_budget``. It replays the ambiguous submit, cancels the attempt
    it finds, waits for its end and releases the allocation; it drops the submit as refused only after the launcher refused the replay; when the
    loop stays blocked it retains the allocation and the original error reaches the caller. Another worker's attempt on the same client is
    not touched."""

    def stuck_run(client: launcher.LauncherClient, allocation: str, launch: str, template: str = "fixture_dbt") -> tuple[Any, float]:
        """``run_attempt`` followed by the unconditional release the CLI and the wrappers use; the outcome and the seconds."""
        principal = client.principal_alloc(allocation)
        begun = time.monotonic()

        def caller() -> Any:
            try:
                return client.run_attempt(principal, template, launch, ["build"], env.fixture_params(launch), checkout=ROOT)
            finally:
                client.release_allocation(allocation)

        return outcome_of(caller), time.monotonic() - begun

    def is_unresponsive(outcome: Any) -> bool:
        return isinstance(outcome, launcher.ClientError) and outcome.code == "unresponsive"

    with client_log_lines() as lines, served_here(env) as hook:
        client = env.client(start=False)
        other_allocation = "al-another-worker"
        client.live["another_workers_attempt"] = client.principal_alloc(other_allocation)
        mark = len(hook.calls)

        first = client.allocate()
        hook.release.clear()
        hook.blocked = frozenset({"submit"})
        timer = threading.Timer(3.5, hook.release.set)
        timer.start()
        reconciled, took_first = stuck_run(client, first, "stuck1")
        timer.cancel()
        hook.blocked = frozenset()
        state = env.state()
        first_attempts = [a for a in state["attempts"].values() if a["launch_id"].startswith("stuck1")]
        first_state, first_pending = state["allocations"][first]["state"], dict(client.pending_submits)
        client.pending_submits.clear()
        env.teardown()

        second = client.allocate()
        hook.release.clear()
        hook.blocked = frozenset({"submit"})
        lines.clear()
        watchdog = threading.Timer(14.0, hook.release.set)
        watchdog.start()
        retained, took_second = stuck_run(client, second, "stuck2")
        watchdog.cancel()
        second_state, second_pending, second_lines = env.state()["allocations"][second]["state"], dict(client.pending_submits), list(lines)
        hook.release.set()
        hook.blocked = frozenset()
        env.wait(lambda: any(a["launch_id"].startswith("stuck2") for a in env.state()["attempts"].values()), 20, "the blocked submit is handled")
        client.pending_submits.clear()
        released_calls = [c for c in hook.calls[mark:] if c[0] == "release_allocation" and c[1].get("alloc_id") == second]
        env.teardown()

        third = client.allocate()
        hook.release.clear()
        hook.blocked = frozenset({"submit"})
        timer = threading.Timer(3.5, hook.release.set)
        timer.start()
        refused, took_third = stuck_run(client, third, "stuck3", template="no_such_template")
        timer.cancel()
        hook.blocked = frozenset()
        third_state, third_pending = env.state()["allocations"][third]["state"], dict(client.pending_submits)
        client.pending_submits.clear()
        untouched = all(c[1].get("alloc_id") != other_allocation for c in hook.calls[mark:]) and "another_workers_attempt" in client.live
        client.live.clear()
    (env.report / "timings.json").write_text(
        json.dumps({"reconciled": round(took_first, 2), "retained": round(took_second, 2), "refused": round(took_third, 2)}, indent=2)
    )
    return {
        "reconciled: the original unresponsive error reaches the caller": is_unresponsive(reconciled),
        "reconciled: cleanup ended within the bound": took_first < CLEANUP_BOUND,
        "reconciled: the replayed submit found the attempt, which was cancelled and ended": len(first_attempts) == 1
        and all(a["phase"] == "ended" for a in first_attempts),
        "reconciled: the allocation is released": first_state == "released",
        "reconciled: no submit stays pending": first_pending == {},
        "retained: the original unresponsive error reaches the caller": is_unresponsive(retained),
        "retained: cleanup ended within the bound although the loop stayed blocked": took_second < CLEANUP_BOUND,
        "retained: the allocation is kept and no release request was sent": second_state == "held" and released_calls == [],
        "retained: the unresolved submit stays recorded": len(second_pending) == 1,
        "retained: one log line names the allocation": len([line for line in second_lines if second in line and "retained" in line]) == 1,
        "refused: the replayed submit was refused by the launcher and then dropped": third_pending == {} and third_state == "released",
        "refused: cleanup ended within the bound": took_third < CLEANUP_BOUND,
        "another worker's attempt on the same client is not touched": untouched,
    }


@case("client_cleanup_shared_success", timings=CLEANUP_TIMINGS)
def client_cleanup_shared_success(env: Env) -> dict[str, bool]:
    """Round 5 finding 13 (failure modes SC1 to SC4): the cleanup a failed worker still owes is not erased by a successful exchange of another
    worker on the same client. One worker's ``run_attempt`` fails ``unresponsive`` with its submit ambiguous; the submit is then persisted and
    a sibling completes a status exchange before the worker's ``finally`` releases the allocation. The sibling shares the allocation's principal
    in one scenario and exchanges with no principal in another. The release still replays the submit, cancels its attempt and releases. In the last
    scenario a sibling answers every 0.2 s while Docker removal hangs: cleanup ends within ``cleanup_budget`` from its own first step and the
    allocation is retained."""
    env.daemon.script = lambda container: None if container["Config"]["Labels"].get("hai.template") == "fixture_dbt" else (0.2, 0)

    def scenario(label: str, sibling: str) -> dict[str, Any]:
        """A failed worker and a sibling: ``none``, ``same`` principal, ``other`` principal, ``polling`` (the attempt was accepted, then the status
        wait failed) or ``chatty`` (a hung removal)."""
        client = env.client(start=False)
        allocation = client.allocate()
        principal = client.principal_alloc(allocation)
        seen: dict[str, Any] = {}
        stop, gate = threading.Event(), None
        hook.release.clear()
        hook.blocked = frozenset({"status" if sibling == "polling" else "submit"})

        def sibling_status() -> None:
            if sibling == "other":
                client.call("status", {"what": "launcher"})
            else:
                client.call("status", {"what": "allocation", "id": allocation}, principal)

        def chatter() -> None:
            while not stop.is_set():
                with contextlib.suppress(launcher.ClientError, OSError):
                    sibling_status()
                stop.wait(0.2)

        def caller() -> Any:
            nonlocal gate
            try:
                try:
                    return client.run_attempt(principal, "fixture_dbt", label, ["build"], env.fixture_params(label), checkout=ROOT)
                except launcher.ClientError:
                    hook.blocked = frozenset()
                    hook.release.set()
                    env.wait(lambda: any(a["launch_id"].startswith(label) for a in env.state()["attempts"].values()), 10, "the ambiguous submit is accepted")
                    if sibling == "chatty":
                        env.wait(lambda: any(a["phase"] == "started" for a in env.state()["attempts"].values()), 10, "the attempt started")
                        gate = env.daemon.add_gate("remove", "before")
                        threading.Thread(target=chatter).start()
                        time.sleep(0.6)
                    elif sibling != "none":
                        worker = threading.Thread(target=sibling_status)
                        worker.start()
                        worker.join(10)
                        if worker.is_alive():
                            raise CaseFailure("the sibling status exchange did not return") from None
                    raise
            finally:
                begun = time.monotonic()
                try:
                    seen["release"] = client.release_allocation(allocation)
                finally:
                    seen["release_seconds"] = time.monotonic() - begun
                    stop.set()

        with client_log_lines() as lines:
            seen["outcome"] = outcome_of(caller)
        state = env.state()
        seen |= {
            "unresponsive": isinstance(seen["outcome"], launcher.ClientError) and seen["outcome"].code == "unresponsive",
            "refused": isinstance(seen["outcome"], launcher.ClientError) and seen["outcome"].code == "refused",
            "pending": len(client.pending_submits),
            "open_attempts": sum(a["phase"] != "ended" for a in state["attempts"].values()),
            "held": state["allocations"][allocation]["state"] == "held",
            "release_calls": [c for c in hook.calls if c[0] == "release_allocation" and c[1].get("alloc_id") == allocation],
            "retained_lines": [line for line in lines if allocation in line and "retained" in line],
        }
        if gate is not None:
            gate.release()
        env.teardown()
        return seen

    with served_here(env) as hook:
        control = scenario("control", "none")
        same = scenario("same", "same")
        other = scenario("other", "other")
        polling = scenario("polling", "polling")
        chatty = scenario("chatty", "chatty")
    (env.report / "scenarios.json").write_text(
        json.dumps(
            {
                name: {key: str(value) for key, value in seen.items() if key != "outcome"}
                for name, seen in (("control", control), ("same", same), ("other", other), ("polling", polling), ("chatty", chatty))
            },
            indent=1,
        )
    )

    def reconciled(seen: dict[str, Any]) -> bool:
        return seen["unresponsive"] and seen["pending"] == 0 and seen["open_attempts"] == 0 and not seen["held"]

    return {
        "control: the original unresponsive error reaches the caller and nothing stays open": reconciled(control),
        "a sibling of the same principal completed an exchange and the failed worker's cleanup still reconciles": reconciled(same),
        "the failed worker's release is not refused because of the sibling": not same["refused"],
        "a sibling completed an exchange after the failure of the status wait: the worker's attempt is cancelled and released": reconciled(polling),
        "a sibling exchanging under another principal does not change the cleanup": reconciled(other),
        "chatty sibling: the original unresponsive error reaches the caller": chatty["unresponsive"] and not chatty["refused"],
        "chatty sibling: cleanup ends within the budget of its own first step": chatty["release_seconds"] < CLEANUP_TIMINGS["cleanup_budget"] + 2.0,
        "chatty sibling: the allocation is retained and no release request is sent": chatty["held"] and chatty["release_calls"] == [],
        "chatty sibling: one log line names the allocation": len(chatty["retained_lines"]) == 1,
    }


def cleanup_propagation_scenario(env: Env, early: bool) -> dict[str, Any]:
    name = "before_unwind" if early else "after_unwind"
    seen: dict[str, Any] = {}
    noted, allow_raise, unwound, allow_cleanup = (threading.Event() for _ in range(4))
    worker: threading.Thread | None = None
    try:
        with served_here(env) as hook:
            client = env.client(start=False)
            allocation = client.allocate()
            principal = client.principal_alloc(allocation)
            hook.blocked = frozenset({"status"})
            original = client.note_failure

            def pause_failure(error: Any, *arguments: Any) -> Any:
                result = original(error, *arguments)
                if isinstance(error, launcher.ClientError) and error.code == "unresponsive":
                    seen["original_error"] = error
                    noted.set()
                    if not allow_raise.wait(15):
                        raise CaseFailure("the test did not resume failure propagation")
                return result

            def caller() -> None:
                try:
                    try:
                        client.run_attempt(principal, "fixture_dbt", name, ["build"], env.fixture_params(name))
                    except launcher.ClientError:
                        unwound.set()
                        if not allow_cleanup.wait(15):
                            raise CaseFailure("the test did not resume caller cleanup") from None
                        raise
                finally:
                    seen["release"] = client.release_allocation(allocation)

            def run_worker() -> None:
                seen["outcome"] = outcome_of(caller)

            with mock.patch.object(client, "note_failure", pause_failure):
                worker = threading.Thread(target=run_worker)
                worker.start()
                if not noted.wait(15):
                    raise CaseFailure("the worker did not record the terminal wait failure")
                hook.blocked = frozenset()
                hook.release.set()
                if not early:
                    allow_raise.set()
                    if not unwound.wait(10):
                        raise CaseFailure("the worker did not unwind")
                seen["failure_before_sibling"] = client.lost_contact(principal)
                seen["live_before_sibling"] = len(client.live)
                seen["pending_before_sibling"] = len(client.pending_submits)
                deadline = client.cleanup_deadline(principal)
                client.call("status", {"what": "allocation", "id": allocation}, principal)
                seen["sibling_completed"] = True
                seen["failure_after_sibling"] = client.lost_contact(principal)
                seen["deadline_preserved"] = client.cleanup_deadline(principal) == deadline
                allow_raise.set()
                allow_cleanup.set()
                worker.join(15)
                if worker.is_alive():
                    raise CaseFailure("the worker did not finish bounded cleanup")
            state = env.state()
            seen["original_error_preserved"] = seen["outcome"] is seen["original_error"]
            seen["pending_after_cleanup"] = len(client.pending_submits)
            seen["open_after_cleanup"] = sum(a["phase"] != "ended" for a in state["attempts"].values())
            seen["held_after_cleanup"] = sum(a["state"] == "held" for a in state["allocations"].values())
            env.teardown()
    finally:
        allow_raise.set()
        allow_cleanup.set()
        if worker is not None:
            worker.join(20)
    return {key: str(value) if isinstance(value, (BaseException, launcher.ReleaseOutcome)) else value for key, value in seen.items()}


def cleanup_publication_race(env: Env) -> bool:
    client = env.client(start=False)
    principal = client.principal_alloc("synthetic-publication")
    live_lock = client.live_lock
    published: list[float] = []

    class PublishOnUnlock:
        def __enter__(self) -> None:
            live_lock.acquire()

        def __exit__(self, *_: Any) -> None:
            live_lock.release()
            if not published:
                published.append(0.0)
                client.note_failure(launcher.ClientError("synthetic terminal wait", "unresponsive"), principal)
                published[0] = client.cleanup_deadline(principal)

    with mock.patch.object(client, "live_lock", PublishOnUnlock()):
        client.forget_failure(principal)
    return client.lost_contact(principal) and client.cleanup_deadline(principal) == published[0]


@case("client_cleanup_propagation", timings={"stuck_after": 1.5, "poll_patience": 12.0, "cleanup_budget": 2.5})
def client_cleanup_propagation(env: Env) -> dict[str, bool]:
    env.daemon.script = lambda container: None if container["Config"]["Labels"].get("hai.template") == "fixture_dbt" else (0.2, 0)
    checks = {"a success cannot erase a failure published after its work check": cleanup_publication_race(env)}
    scenarios = {"after_unwind": cleanup_propagation_scenario(env, False), "before_unwind": cleanup_propagation_scenario(env, True)}
    for name, seen in scenarios.items():
        checks |= {
            f"{name}: an accepted attempt has no ambiguous submit": seen["live_before_sibling"] == 1 and seen["pending_before_sibling"] == 0,
            f"{name}: the sibling completes without clearing the failure": seen["sibling_completed"]
            and seen["failure_before_sibling"]
            and seen["failure_after_sibling"],
            f"{name}: the principal keeps its first cleanup deadline": seen["deadline_preserved"],
            f"{name}: the exact original exception reaches the caller": seen["original_error_preserved"],
            f"{name}: cleanup cancels, drains and releases": seen["pending_after_cleanup"] == seen["open_after_cleanup"] == seen["held_after_cleanup"] == 0
            and seen["release"] == "released",
        }
    (env.report / "propagation.json").write_text(json.dumps(scenarios, indent=2) + "\n")
    return checks


@case("client_release_outcome", timings=CLEANUP_TIMINGS)
def client_release_outcome(env: Env) -> dict[str, bool]:
    """Round 5 finding 14 (failure modes RO1 to RO3): ``release_allocation`` tells released, retained and unresolved apart and the silver runner's
    cleanup report shows which. The real ``fixture_stage`` and ``Run.settle`` run with a fixture attempt that blocks the launcher's submit
    handling past the cleanup budget (retained), with no failure (released) and, in a third scenario, with a release request the launcher does
    not answer after the work was settled (unresolved). The original ``unresponsive`` error reaches the caller in the failing scenarios."""
    from scripts.lakehouse import run_silver_e2e as silver_runner

    def is_unresponsive(outcome: Any) -> bool:
        return isinstance(outcome, launcher.ClientError) and outcome.code == "unresponsive"

    def silver_scenario(blocked: bool) -> dict[str, Any]:
        client = env.client(start=False)
        run = silver_runner.Run(mode="full")

        def fixture_attempt(run: Any, args: Any, session: Any, stage: Any) -> None:
            if blocked:
                hook.release.clear()
                hook.blocked = frozenset({"submit"})
                session.client.run_attempt(session.principal, "fixture_dbt", "silver-release", ["build"], env.fixture_params("silver"))

        with (
            mock.patch.object(launcher, "load_client", return_value=client),
            mock.patch.object(silver_runner, "fixture_cases", side_effect=fixture_attempt),
            client_log_lines() as lines,
        ):
            outcome = outcome_of(lambda: silver_runner.fixture_stage(run, argparse.Namespace()))
        report = run.settle()
        seen = {
            "unresponsive": is_unresponsive(outcome),
            "claims_released": run.allocation_released is True,
            "cleanup_ok": bool(report["ok"]),
            "reported_outcome": str(report.get("allocation_release")),
            "held": sum(a["state"] == "held" for a in env.state()["allocations"].values()),
            "pending": len(client.pending_submits),
            "lines": [line for line in lines if "retained" in line],
        }
        hook.blocked = frozenset()
        hook.release.set()
        if blocked:
            env.wait(lambda: bool(env.state()["attempts"]), 10, "the retained submit is accepted")
        env.teardown()
        return seen

    def unresolved_scenario() -> dict[str, Any]:
        client = env.client(start=False)
        allocation = client.allocate()
        principal = client.principal_alloc(allocation)
        seen: dict[str, Any] = {}

        def work_settled_release_blocked() -> None:
            hook.blocked = frozenset({"release_allocation"})
            hook.release.set()
            hook.release.clear()

        def caller() -> Any:
            try:
                return client.run_attempt(principal, "fixture_dbt", "unresolved", ["build"], env.fixture_params("unresolved"), checkout=ROOT)
            finally:
                seen["release"] = client.release_allocation(allocation)

        hook.release.clear()
        hook.blocked = frozenset({"submit"})
        timer = threading.Timer(3.5, work_settled_release_blocked)
        timer.start()
        with client_log_lines() as lines:
            outcome = outcome_of(caller)
        timer.cancel()
        hook.blocked = frozenset()
        hook.release.set()
        seen |= {
            "unresponsive": is_unresponsive(outcome),
            "pending": len(client.pending_submits),
            "lines": [line for line in lines if allocation in line and "unresolved" in line],
        }
        env.teardown()
        return seen

    with served_here(env) as hook:
        retained = silver_scenario(True)
        released = silver_scenario(False)
        unresolved = unresolved_scenario()
    (env.report / "scenarios.json").write_text(json.dumps({"retained": retained, "released": released, "unresolved": unresolved}, indent=1, default=str))
    return {
        "retained: the original unresponsive error reaches the caller": retained["unresponsive"],
        "retained: the allocation is held with its submit pending": retained["held"] == 1 and retained["pending"] == 1,
        "retained: the silver runner does not claim the allocation was released": not retained["claims_released"],
        "retained: the cleanup summary is not ok": not retained["cleanup_ok"],
        "retained: the report shows the outcome retained": retained["reported_outcome"] == "retained",
        "released: the silver runner reports a release and an ok cleanup": released["claims_released"] and released["cleanup_ok"],
        "released: the report shows the outcome released": released["reported_outcome"] == "released",
        "released: nothing is held": released["held"] == 0,
        "unresolved: the release request got no answer and nothing was raised in its place": unresolved["unresponsive"],
        "unresolved: the release returns the outcome unresolved": str(unresolved["release"]) == "unresolved",
        "unresolved: one log line names the allocation": len(unresolved["lines"]) == 1,
    }


def command_exit(argv: list[str]) -> tuple[int, str, float]:
    """Run ``launcher.main()`` in this process with the arguments; the exit code, what it wrote to standard error and the seconds."""
    spoken = io.StringIO()
    begun = time.monotonic()
    with mock.patch.object(sys, "argv", ["launcher", *argv]), contextlib.redirect_stderr(spoken), contextlib.redirect_stdout(io.StringIO()):
        code = launcher.main()
    return code, spoken.getvalue(), time.monotonic() - begun


@case("client_cleanup_cli", timings=CLEANUP_TIMINGS)
def client_cleanup_cli(env: Env) -> dict[str, bool]:
    """Round 4 finding 9 (failure modes CU6 to CU8, CU10 and CU12): the command line ends a liveness failure in the degraded exit 5 and bounds
    its cleanup. ``run`` and ``dbt deps`` run through ``main`` against a launcher whose loop is blocked. The other paths are checked at the
    exit boundary with the client calls recorded: ``unavailable`` and ``unresponsive`` exit 5 and clean up with bounded calls, a refusal exits
    3 and cleans up with the ordinary calls, a real dbt launch marks its run failed_retained with a bounded call and never finalizes it,
    and ``query`` deregisters its reader and releases with bounded calls."""
    registry = ["--test-registry", str(env.registry)]
    params = [word for name, value in env.fixture_params("cli1").items() for word in ("--param", f"{name}={json.dumps(value)}")]
    stuck: dict[str, tuple[int, str, float, list[str]]] = {}
    with served_here(env) as hook:
        for label, argv in (
            ("run", [*registry, "run", "--template", "fixture_dbt", "--launch-id", "cli1", *params, "build"]),
            ("dbt deps", [*registry, "dbt", "deps"]),
        ):
            hook.release.clear()
            hook.blocked = frozenset({"submit"})
            watchdog = threading.Timer(20.0, hook.release.set)
            watchdog.start()
            code, text, took = command_exit(argv)
            watchdog.cancel()
            held = [a["state"] for a in env.state()["allocations"].values() if a["kind"] == "shared" and a["state"] == "held"]
            hook.release.set()
            hook.blocked = frozenset()
            env.wait(lambda: env.ping().get("status") == "serving", 20, "the loop is free again")
            env.teardown()
            stuck[label] = (code, text, took, held)

    recorded: list[tuple[str, Any, float]] = []
    failure: list[BaseException] = []
    answers = {"register_reader": {"registration_id": "r1"}, "plan": {"budget": {}}}

    def record_call(
        self: launcher.LauncherClient,
        method: str,
        params: dict[str, Any] | None = None,
        principal: dict[str, Any] | None = None,
        request_id: str | None = None,
        timeout: float = 120.0,
    ) -> Any:
        recorded.append((method, params, timeout))
        if method == "submit" and failure:
            raise self.note_failure(failure[0], principal)
        return answers.get(method, {})

    def record_bounded(self: launcher.LauncherClient, method: str, params: dict[str, Any], principal: dict[str, Any], request_id: str, seconds: float) -> Any:
        recorded.append((method, params, seconds))
        return answers.get(method, {})

    def at_boundary(argv: list[str], error: BaseException | None) -> tuple[int, list[tuple[str, Any, float]]]:
        """The exit code of ``main`` and the calls it made when the attempt (or, for ``query``, the submit) fails with the error."""
        recorded.clear()
        failure.clear()
        if error is not None:
            failure.append(error)
        ended = launcher.AttemptResult("att", "exited", 0, "", "", {})

        def attempt(self: launcher.LauncherClient, *args: Any, **keywords: Any) -> launcher.AttemptResult:
            if error is not None:
                raise self.note_failure(error, args[0])
            return ended

        with (
            mock.patch.object(launcher.LauncherClient, "call", record_call),
            mock.patch.object(launcher.LauncherClient, "bounded_call", record_bounded),
            mock.patch.object(launcher.LauncherClient, "wait_for", lambda *a, **k: {"phase": "up"}),
            mock.patch.object(launcher.LauncherClient, "allocate", lambda *a, **k: "a_cli"),
            mock.patch.object(launcher.LauncherClient, "principal_run", lambda self, run_id, owner=False: {"kind": "run", "run_id": run_id, "token": "x"}),
            mock.patch.object(launcher.LauncherClient, "run_attempt", attempt),
        ):
            code, _, _ = command_exit([*registry, *argv])
        return code, list(recorded)

    def calls_named(calls: list[tuple[str, Any, float]], method: str) -> list[float]:
        return [seconds for name, _, seconds in calls if name == method]

    unavailable = launcher.ClientError("the launcher is not running", "unavailable")
    unresponsive = launcher.ClientError("the launcher is alive but made no progress for 1.5 s", "unresponsive")
    refusal = launcher.ClientError("no such template", "args")
    run_unavailable = at_boundary(["run", "--template", "fixture_dbt", "build"], unavailable)
    run_unresponsive = at_boundary(["run", "--template", "fixture_dbt", "build"], unresponsive)
    run_refused = at_boundary(["run", "--template", "fixture_dbt", "build"], refusal)
    run_ok = at_boundary(["run", "--template", "fixture_dbt", "build"], None)
    build_unresponsive = at_boundary(["dbt", "build"], unresponsive)
    build_refused = at_boundary(["dbt", "build"], refusal)
    build_ok = at_boundary(["dbt", "build"], None)
    query_unavailable = at_boundary(["query"], launcher.ClientError("the launcher closed the connection", "unavailable"))
    bound = env.config.timings.cleanup_budget
    return {
        **{f"{label}, launcher blocked: exits 5 and says unresponsive": stuck[label][0] == 5 and "[unresponsive]" in stuck[label][1] for label in stuck},
        **{f"{label}, launcher blocked: ends within the detection time plus the cleanup bound": stuck[label][2] < CLEANUP_BOUND for label in stuck},
        **{f"{label}, launcher blocked: the allocation is retained": stuck[label][3] != [] and len(stuck[label][3]) == 1 for label in stuck},
        "run: unavailable exits 5": run_unavailable[0] == 5,
        "run: unresponsive exits 5": run_unresponsive[0] == 5,
        "run: a refusal still exits 3": run_refused[0] == 3,
        "run: after a liveness failure the release is one bounded call": [s <= bound for s in calls_named(run_unresponsive[1], "release_allocation")] == [True]
        and [s <= bound for s in calls_named(run_unavailable[1], "release_allocation")] == [True],
        "run: after a refusal the release is the ordinary call": calls_named(run_refused[1], "release_allocation") == [120.0],
        "run: a success releases with the ordinary call and exits 0": run_ok[0] == 0 and calls_named(run_ok[1], "release_allocation") == [120.0],
        "dbt launch: unresponsive exits 5": build_unresponsive[0] == 5,
        "dbt launch: the run is marked failed_retained with a bounded call and never finalized": [
            s <= bound for s in calls_named(build_unresponsive[1], "mark")
        ]
        == [True]
        and not calls_named(build_unresponsive[1], "finalize")
        and not calls_named(build_unresponsive[1], "release"),
        "dbt launch: a refusal exits 3 and marks with the ordinary call": build_refused[0] == 3 and calls_named(build_refused[1], "mark") == [120.0],
        "dbt launch: a success finalizes and releases with the ordinary calls": build_ok[0] == 0
        and calls_named(build_ok[1], "finalize") == [120.0]
        and calls_named(build_ok[1], "release") == [300.0],
        "query: a liveness failure exits 5 and cleans up with bounded calls": query_unavailable[0] == 5
        and [s <= bound for s in calls_named(query_unavailable[1], "deregister_reader")] == [True]
        and [s <= bound for s in calls_named(query_unavailable[1], "release_allocation")] == [True],
    }


def attempt_started(env: Env, prefix: str) -> bool:
    return any(a["launch_id"].startswith(prefix) for a in env.state()["attempts"].values())


@case("client_cleanup_wrappers", timings=CLEANUP_TIMINGS)
def client_cleanup_wrappers(env: Env) -> dict[str, bool]:
    """Round 4 finding 9 (failure modes CU9): the catalog job and the silver validation, against a launcher whose loop is blocked, end within the
    detection time plus the cleanup bound, say that the launcher did not answer (not that it refused) with the code, and keep the allocation."""
    from scripts.lakehouse import catalog, silver

    build = ROOT / "data" / "analytics" / "dbt"
    database = build / "staging.duckdb"
    manifest = build / "e2e" / "real_again" / "target" / "manifest.json"
    made = []
    for path, text in ((database, ""), (manifest, json.dumps({"nodes": {}}))):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
            made.append(path)
    results_before = set(silver.RESULTS.glob("*")) if silver.RESULTS.exists() else set()
    saved = (catalog.TEST_REGISTRY, catalog.COMPOSE_ENV, silver.TEST_REGISTRY)
    catalog.TEST_REGISTRY = silver.TEST_REGISTRY = str(env.registry)
    catalog.COMPOSE_ENV = env.temp / "compose.env"
    outcomes: dict[str, Any] = {}
    took: dict[str, float] = {}
    held: dict[str, list[str]] = {}
    try:
        with served_here(env) as hook:
            for label, prefix, call in (
                ("catalog", "job-", lambda: catalog.job("bronze_e2e", [])),
                ("silver", "silver-", lambda: silver.validate(database, silver.CONTRACTS / "suites", silver.CONTRACTS / "baselines.json", manifest)),
            ):
                hook.release.clear()
                hook.blocked = frozenset({"submit"})
                watchdog = threading.Timer(16.0, hook.release.set)
                watchdog.start()
                begun = time.monotonic()
                try:
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        call()
                except (catalog.CatalogError, silver.SilverError, launcher.ClientError) as error:
                    outcomes[label] = error
                took[label] = time.monotonic() - begun
                watchdog.cancel()
                held[label] = [a["state"] for a in env.state()["allocations"].values() if a["kind"] == "shared" and a["state"] == "held"]
                hook.release.set()
                hook.blocked = frozenset()
                env.wait(partial(attempt_started, env, prefix), 15, "the blocked submit is handled")
                env.teardown()
    finally:
        catalog.TEST_REGISTRY, catalog.COMPOSE_ENV, silver.TEST_REGISTRY = saved
        for path in made:
            path.unlink(missing_ok=True)
        if silver.RESULTS.exists():
            for leftover in set(silver.RESULTS.glob("*")) - results_before:
                shutil.rmtree(leftover, ignore_errors=True)
    texts = {label: str(error) for label, error in outcomes.items()}
    return {
        **{f"{label}: ends within the detection time plus the cleanup bound": label in took and took[label] < CLEANUP_BOUND for label in ("catalog", "silver")},
        **{
            f"{label}: the text says the launcher did not answer and carries the code": "did not answer" in texts.get(label, "")
            and "unresponsive" in texts.get(label, "")
            and "refused" not in texts.get(label, "")
            for label in ("catalog", "silver")
        },
        **{f"{label}: the allocation is retained": len(held.get(label, [])) == 1 for label in ("catalog", "silver")},
    }


def synthetic_archive(work: Path) -> tuple[Path, Path, Path, Path]:
    """A checkout with the real lock, a synthetic package archive and its record (no network): (checkout, lock, archive, record)."""
    checkout = work / "archive_checkout"
    (checkout / "dbt").mkdir(parents=True)
    lock = checkout / "dbt/package-lock.yml"
    lock.write_bytes((ROOT / "dbt/package-lock.yml").read_bytes())
    lock_sha = hashlib.sha256(lock.read_bytes()).hexdigest()
    root = checkout / "data/analytics/dbt/package_archive" / lock_sha
    root.mkdir(parents=True)
    files = []
    raw_archives = {}
    document = importlib.import_module("yaml").safe_load(lock.read_text())
    for package in document["packages"]:
        name = package["name"]
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
            content = f"name: {name}\nversion: {package['version']}\n".encode()
            info = tarfile.TarInfo(f"{name}-{package['version']}/dbt_project.yml")
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        data = buffer.getvalue()
        filename = f"{name}-{package['version']}.tar.gz"
        raw_archives[filename] = data
        files.append({**package, "filename": filename, "url": "https://example.com/synthetic", "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        for filename, data in sorted(raw_archives.items()):
            info = tarfile.TarInfo(filename)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    archive = root / "packages.tar"
    archive.write_bytes(buffer.getvalue())
    record = checkout / "config/lakehouse/dbt_package_archive.json"
    record.parent.mkdir(parents=True)
    record.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "lock_sha256": lock_sha,
                "packages": files,
                "archive": {
                    "path": str(archive.relative_to(checkout)),
                    "size": archive.stat().st_size,
                    "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                },
            }
        )
    )
    return checkout, lock, archive, record


@case("dbt_archive_boundary")
def dbt_archive_boundary(env: Env) -> dict[str, bool]:
    """916: deps render without network and reject changed archive bytes or lock before admission."""
    checkout, lock, archive, record = synthetic_archive(env.work)
    packages = env.work / "archive_install"
    packages.mkdir()
    renderer = launcher.Renderer(env.config, launcher.ComposeModel(ROOT / "docker-compose.yaml"), checkout, ROOT, b"k" * 32)
    inp = launcher.RenderInput(
        "dbt_deps",
        "owner",
        "deps",
        1,
        1,
        "op",
        dict(launcher.Launcher.PLACEHOLDER_ENV),
        int(time.time()) + 60,
        ["deps"],
        {"packages_host": str(packages)},
        {},
        None,
        None,
        {},
    )
    clean = renderer.render(inp, env.registry_id())
    original = archive.read_bytes()
    archive.write_bytes(original + b"tamper")
    tampered = expect_error_of(lambda: renderer.render(inp, env.registry_id()))
    archive.write_bytes(original)
    lock.write_text(lock.read_text().replace("1.4.1", "1.4.2"))
    changed_lock = expect_error_of(lambda: renderer.render(inp, env.registry_id()))
    return {
        "deps has no network": clean.body["HostConfig"]["NetworkMode"] == "none",
        "archive bytes are mounted read-only": any(m["Target"] == "/workspace/archive" and m["ReadOnly"] for m in clean.body["HostConfig"]["Mounts"]),
        "tampered archive is refused for hash mismatch": isinstance(tampered, launcher.LauncherError) and "archive" in str(tampered).lower(),
        "changed lock without refresh is refused": isinstance(changed_lock, launcher.LauncherError) and "lock" in str(changed_lock).lower(),
    }


@case("dbt_archive_record_hash")
def dbt_archive_record_hash(env: Env) -> dict[str, bool]:
    """916 (N5): the record hash a deps job is admitted with is the hash of the record bytes the render verified."""
    from scripts.lakehouse import dbt_package_archive

    checkout, _, _, record = synthetic_archive(env.work)
    packages = env.work / "archive_install"
    packages.mkdir()
    renderer = launcher.Renderer(env.config, launcher.ComposeModel(ROOT / "docker-compose.yaml"), checkout, ROOT, b"k" * 32)
    inp = launcher.RenderInput(
        "dbt_deps", "owner", "deps", 1, 1, "op", dict(launcher.Launcher.PLACEHOLDER_ENV), int(time.time()) + 60, ["deps"], {"packages_host": str(packages)}, {}
    )
    original, real_verify = record.read_bytes(), dbt_package_archive.verify
    verified: list[bytes] = []

    def swap_after(root: Path, **kwargs: Any) -> Path:
        # A same-meaning record with other bytes replaces the verified one just after the verification returns.
        result = real_verify(root, **kwargs)
        verified.append(original)
        record.write_bytes(original + b"\n")
        return result

    def swap_before(root: Path, **kwargs: Any) -> Path:
        record.write_bytes(original + b"\n")
        return real_verify(root, **kwargs)

    try:
        dbt_package_archive.verify = swap_after
        after = renderer.render(inp, env.registry_id())
        record.write_bytes(original)
        dbt_package_archive.verify = swap_before
        before = expect_error_of(lambda: renderer.render(inp, env.registry_id()))
    finally:
        dbt_package_archive.verify = real_verify
        record.write_bytes(original)
    command = list(after.body["Cmd"])
    admitted = command[command.index("--record-sha256") + 1]
    return {
        "the admitted hash is the verified bytes' hash": bool(verified) and admitted == hashlib.sha256(original).hexdigest(),
        "a record changed before the verification is refused": isinstance(before, launcher.LauncherError) and "record" in str(before).lower(),
    }


@case("dbt_archive_install_scope")
def dbt_archive_install_scope(env: Env) -> dict[str, bool]:
    """916 (N2): the package install replaces only the locked packages and its own stale staging; any other entry stops it before removal."""
    from scripts.lakehouse import dbt_package_archive

    checkout, lock, archive, record = synthetic_archive(env.work)
    expected = hashlib.sha256(record.read_bytes()).hexdigest()

    def install(destination: Path) -> BaseException | None:
        try:
            dbt_package_archive.install(checkout, lock=lock, record=record, archive=archive, destination=destination, expected_record=expected)
        except dbt_package_archive.ArchiveError as error:
            return error
        return None

    foreign = env.work / "foreign_destination"
    (foreign / "dbt_utils").mkdir(parents=True)
    (foreign / "keep.txt").write_text("not a package")
    refused_foreign = install(foreign)
    stale = env.work / "stale_destination"
    (stale / "dbt_utils").mkdir(parents=True)
    (stale / "dbt_utils" / "old.txt").write_text("previous install")
    (stale / ".package_install_old").mkdir()
    replaced = install(stale)
    return {
        "an unrelated entry stops the install": isinstance(refused_foreign, dbt_package_archive.ArchiveError) and "keep.txt" in str(refused_foreign),
        "nothing was removed from that folder": (foreign / "keep.txt").exists() and (foreign / "dbt_utils").is_dir(),
        "a folder with only locked packages and stale staging is replaced": replaced is None
        and sorted(path.name for path in stale.iterdir()) == ["dbt_project_evaluator", "dbt_utils"]
        and not (stale / "dbt_utils" / "old.txt").exists(),
    }


@case("dbt_archive_install_late_foreign")
def dbt_archive_install_late_foreign(env: Env) -> dict[str, bool]:
    """916 (N2, delta 3): an unrelated entry that appears after the first check, while packages are staged, is never removed."""
    from scripts.lakehouse import dbt_package_archive

    checkout, lock, archive, record = synthetic_archive(env.work)
    expected = hashlib.sha256(record.read_bytes()).hexdigest()
    destination = env.work / "late_destination"
    (destination / "dbt_utils").mkdir(parents=True)
    real_tree = dbt_package_archive.package_tree
    wrote: list[bool] = []

    def tree_with_late_writer(*args: Any, **kwargs: Any) -> Any:
        # Another host writer adds a file once the first check has passed: staging exists, so the install is copying package content.
        if any(path.name.startswith(".package_install_") for path in destination.iterdir()):
            (destination / "keep.txt").write_text("not a package")
            wrote.append(True)
        return real_tree(*args, **kwargs)

    outcome: BaseException | None = None
    try:
        with mock.patch.object(dbt_package_archive, "package_tree", tree_with_late_writer):
            dbt_package_archive.install(checkout, lock=lock, record=record, archive=archive, destination=destination, expected_record=expected)
    except dbt_package_archive.ArchiveError as error:
        outcome = error
    return {
        "the late writer ran after the first check": bool(wrote),
        "the late entry survives": (destination / "keep.txt").exists() and (destination / "keep.txt").read_text() == "not a package",
        "the install stops and names it": isinstance(outcome, dbt_package_archive.ArchiveError) and "keep.txt" in str(outcome),
        "no staging folder is left behind": not any(path.name.startswith(".package_install_") for path in destination.iterdir()),
    }


@case("dbt_archive_fetch_publication")
def dbt_archive_fetch_publication(env: Env) -> dict[str, bool]:
    """916 (CI fix review): fetch rebuilds the recorded archive offline, never writes through a link and leaves no damaged archive."""
    from scripts.lakehouse import dbt_package_archive

    checkout, _, archive, record = synthetic_archive(env.work)
    members = dbt_package_archive.archive_members(archive.read_bytes())
    document = json.loads(record.read_text())
    for row in document["packages"]:
        row["url"] = f"https://example.com/{row['filename']}"
    sources = {row["url"]: members[row["filename"]] for row in document["packages"]}
    # The record names the deterministic packing that fetch reproduces.
    expected = dbt_package_archive.pack(members)
    document["archive"].update(size=len(expected), sha256=hashlib.sha256(expected).hexdigest())
    record.write_text(json.dumps(document))
    relative = archive.relative_to(checkout)
    archive.unlink()

    def fresh(name: str) -> Path:
        # A checkout with the lock and record but no archive, so each probe starts from what a clone holds.
        root = env.work / name
        shutil.copytree(checkout, root)
        return root

    def fetch(root: Path) -> BaseException | None:
        with mock.patch.object(dbt_package_archive, "download", lambda url: sources[url]):
            try:
                dbt_package_archive.fetch(root)
            except (dbt_package_archive.ArchiveError, OSError) as error:
                return error
        return None

    control = fresh("fetch_control")
    first = fetch(control)
    inode = (control / relative).stat().st_ino
    second = fetch(control)
    outside = env.work / "fetch_outside"
    outside.mkdir()
    parent_link = fresh("fetch_parent_link")
    shutil.rmtree(parent_link / dbt_package_archive.ARCHIVES)
    (parent_link / dbt_package_archive.ARCHIVES).symlink_to(outside, target_is_directory=True)
    parent_refused = fetch(parent_link)
    leaf_link = fresh("fetch_leaf_link")
    (leaf_link / relative).parent.mkdir(parents=True, exist_ok=True)
    (leaf_link / relative).symlink_to(outside / "leaf.tar")
    leaf_refused = fetch(leaf_link)
    root_link = env.work / "fetch_root_link"
    root_link.symlink_to(fresh("fetch_root_target"), target_is_directory=True)
    root_refused = fetch(root_link)
    partial = fresh("fetch_partial")
    real_write = os.write

    def short_write(fd: int, data: Any) -> int:
        # The disk fills after the first block of the archive.
        real_write(fd, bytes(data[:4096]))
        raise OSError(28, "No space left on device")

    with mock.patch.object(dbt_package_archive.os, "write", short_write):
        interrupted = fetch(partial)
    folder = (partial / relative).parent
    after_interrupt = sorted(path.name for path in folder.iterdir()) if folder.exists() else []
    retried = fetch(partial)
    return {
        "a clean fetch rebuilds the recorded bytes read-only": first is None
        and (control / relative).read_bytes() == expected
        and (control / relative).stat().st_mode & 0o777 == 0o444,
        "a rerun keeps the published file": second is None and (control / relative).stat().st_ino == inode,
        "a linked archive folder is refused": isinstance(parent_refused, dbt_package_archive.ArchiveError),
        "a linked archive file is refused": isinstance(leaf_refused, dbt_package_archive.ArchiveError),
        "a linked checkout root is refused": isinstance(root_refused, dbt_package_archive.ArchiveError),
        "nothing is written through a link": not any(outside.iterdir()) and not (env.work / "fetch_root_target" / relative).exists(),
        "an interrupted write fails": isinstance(interrupted, OSError),
        "an interrupted write leaves no archive or staging file": after_interrupt == [],
        "a retry after the interruption publishes the recorded bytes": retried is None and (partial / relative).read_bytes() == expected,
    }


@case("dbt_archive_fetch_races")
def dbt_archive_fetch_races(env: Env) -> dict[str, bool]:
    """916 (CI fix review deltas 1 and 2): a swapped root ancestor, staging file or final name never publishes outside and never deletes a file."""
    from scripts.lakehouse import dbt_package_archive

    checkout, _, archive, record = synthetic_archive(env.work)
    members = dbt_package_archive.archive_members(archive.read_bytes())
    document = json.loads(record.read_text())
    for row in document["packages"]:
        row["url"] = f"https://example.com/{row['filename']}"
    sources = {row["url"]: members[row["filename"]] for row in document["packages"]}
    expected = dbt_package_archive.pack(members)
    document["archive"].update(size=len(expected), sha256=hashlib.sha256(expected).hexdigest())
    record.write_text(json.dumps(document))
    relative = archive.relative_to(checkout)
    archive.unlink()
    real_open, real_link = os.open, os.link

    def fetch(root: Path, **patches: Any) -> BaseException | None:
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(dbt_package_archive, "download", lambda url: sources[url]))
            for name, replacement in patches.items():
                stack.enter_context(mock.patch.object(dbt_package_archive.os, name, replacement))
            try:
                dbt_package_archive.fetch(root)
            except (dbt_package_archive.ArchiveError, OSError) as error:
                return error
        return None

    # Schedule 1: the root's parent becomes a link to another tree after the root path was checked and before it opens.
    selected, redirected = env.work / "race_selected", env.work / "race_redirected"
    shutil.copytree(checkout, selected / "root")
    shutil.copytree(checkout, redirected / "root")
    swapped: list[bool] = []

    def open_with_ancestor_swap(path: Any, *args: Any, **kwargs: Any) -> int:
        if not swapped:
            selected.rename(env.work / "race_original")
            selected.symlink_to(redirected, target_is_directory=True)
            swapped.append(True)
        return real_open(path, *args, **kwargs)

    ancestor = fetch(selected / "root", open=open_with_ancestor_swap)
    # Schedule 2: the staging name is replaced just before publication, by a link to an outside file with the right bytes
    # or by a short regular file.
    outside = env.work / "race_outside.tar"
    outside.write_bytes(expected)

    def swap_staging(replace_with: Callable[[str, int | None], None]) -> Callable[..., None]:
        def link(source: str, name: str, *args: Any, **kwargs: Any) -> None:
            folder = kwargs.get("src_dir_fd")
            os.unlink(source, dir_fd=folder)
            replace_with(source, folder)
            real_link(source, name, *args, **kwargs)

        return link

    def to_link(source: str, folder: int | None) -> None:
        os.symlink(outside, source, dir_fd=folder)

    def to_short_file(source: str, folder: int | None) -> None:
        fd = real_open(source, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644, dir_fd=folder)
        os.write(fd, expected[:4096])
        os.close(fd)

    linked_root = env.work / "race_staging_link"
    shutil.copytree(checkout, linked_root)
    staging_link = fetch(linked_root, link=swap_staging(to_link))
    short_root = env.work / "race_staging_short"
    shutil.copytree(checkout, short_root)
    staging_short = fetch(short_root, link=swap_staging(to_short_file))
    # A refused publication leaves the final name alone (owner, A106 option A): the retry refuses the leftover until it is removed.
    leftover = fetch(short_root)
    (short_root / relative).unlink()
    retried = fetch(short_root)
    # Schedule 3: another process replaces the final name with its own valid archive right after this call links it.
    replaced_root = env.work / "race_final_replaced"
    shutil.copytree(checkout, replaced_root)
    replacement = env.work / "race_replacement.tar"
    replacement.write_bytes(expected)
    replacement.chmod(0o444)
    replacement_inode = replacement.stat().st_ino

    def link_then_replace(source: str, name: str, *args: Any, **kwargs: Any) -> None:
        real_link(source, name, *args, **kwargs)
        os.replace(replacement, replaced_root / relative)

    final_replaced = fetch(replaced_root, link=link_then_replace)
    survivor = replaced_root / relative
    return {
        "the ancestor swap ran": bool(swapped),
        "a root ancestor swapped to a link is refused": isinstance(ancestor, dbt_package_archive.ArchiveError),
        "nothing is published in the redirected tree": not (redirected / "root" / relative).exists(),
        "a staging file swapped for a link is refused": isinstance(staging_link, dbt_package_archive.ArchiveError),
        "the outside file is not hard-linked into the checkout": outside.stat().st_nlink == 1,
        "a staging file swapped for a short file is refused": isinstance(staging_short, dbt_package_archive.ArchiveError),
        "a retry refuses the leftover short file": isinstance(leftover, dbt_package_archive.ArchiveError),
        "a retry after removing the leftover publishes the recorded bytes": retried is None and (short_root / relative).read_bytes() == expected,
        "a final name replaced after the link is refused": isinstance(final_replaced, dbt_package_archive.ArchiveError),
        "the other process's archive survives": survivor.exists() and survivor.stat().st_ino == replacement_inode and survivor.read_bytes() == expected,
    }


@case("image_python_entrypoint_refused", stub_host=True)
def image_python_entrypoint_refused(env: Env) -> dict[str, bool]:
    """917: a legacy Python image label cannot satisfy the approved Go implementation requirement."""
    env.start()
    image = launcher.ComposeModel(ROOT / "docker-compose.yaml").image("analytics-dbt")
    env.daemon.images[image]["Config"]["Labels"] = {launcher.DEFAULT_IMAGE_LABEL: "1"}
    alloc = env.allocate()
    attempt = env.submit(env.alloc_principal(alloc))["attempt_id"]
    ended = env.wait_ended(attempt)
    return {"legacy Python wrapper image refused before creation": ended["reason"] == "create_failed" and "Go" in ended["detail"] and not creates(env, "-l1-")}


@case("deadline_relay_refused", stub_host=True)
def deadline_relay_refused(env: Env) -> dict[str, bool]:
    """N3: neither a job's environment nor an image's defaults may set the deadline entrypoint's relay variable."""
    renderer = launcher.Renderer(env.config, launcher.ComposeModel(ROOT / "docker-compose.yaml"), ROOT, ROOT, b"k" * 32)
    inp = launcher.RenderInput("fixture_dbt", "owner", "l1", 1, 1, "op", {}, int(time.time()) + 60, [], {}, {})
    forged = {launcher.DEADLINE_RELAY_VARIABLE: "0:0:0"}
    refused = expect_error_of(lambda: renderer.assemble(inp, "relay", "image", ["dbt"], ["build"], forged, [], 1 << 30, "none"))
    accepted = expect_error_of(lambda: renderer.assemble(inp, "relay", "image", ["dbt"], ["build"], {}, [], 1 << 30, "none"))
    # The startup probe creates its container from the same image, so the launcher refuses it and does not serve.
    image = launcher.ComposeModel(ROOT / "docker-compose.yaml").image("analytics-dbt")
    config = env.daemon.images[image]["Config"]
    config["Env"] = [*(config.get("Env") or []), f"{launcher.DEADLINE_RELAY_VARIABLE}=0:0:0"]
    env.start(wait_serving=False)

    def refused_probe() -> dict[str, Any] | None:
        probes = [a for a in env.state()["attempts"].values() if a["owner"] == launcher.DIAGNOSTIC_OWNER and a["phase"] == "ended"]
        return next((a for a in probes if launcher.DEADLINE_RELAY_VARIABLE in a.get("detail", "")), None)

    probe = env.wait(refused_probe, 20, "the probe of the forged image ends")
    return {
        "a job environment with the relay variable is refused": refused is not None and getattr(refused, "code", "") == "spec",
        "the same render without it is accepted": accepted is None,
        "an image that sets the relay variable ends its attempt create_failed": probe["reason"] == "create_failed",
        "the launcher does not serve with that image": env.ping().get("status") != "serving",
        "no create was sent for that image": not env.daemon.calls_matching("POST", "/containers/create"),
    }


class LifecycleGate(HookedLauncher):
    """A test-only loop seam holds an exact phase without blocking socket requests or daemon messages."""

    held_phase = ""
    held_step = ""
    target = ""
    short_bootstrap = False
    thread: threading.Thread

    def advance(self, state: dict[str, Any], a: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        if a["launch_id"] == self.target and a["phase"] == self.held_phase and (not self.held_step or (a.get("cleanup") or {}).get("step") == self.held_step):
            return
        super().advance(state, a, actions)

    def new_attempt(self, state: dict[str, Any], owner: str, template: str, launch_id: str, args: Any, params: Any, **kwargs: Any) -> dict[str, Any]:
        if self.short_bootstrap and params.get("service") == "polaris-bootstrap":
            kwargs["seconds"] = 5
        return super().new_attempt(state, owner, template, launch_id, args, params, **kwargs)


def lifecycle_host(env: Env, **settings: Any) -> LifecycleGate:
    """Start the real launcher loop and socket with only deterministic test gates substituted."""
    instance = LifecycleGate(env.config)
    for name, value in settings.items():
        setattr(instance, name, value)
    instance.thread = threading.Thread(target=instance.serve, daemon=True)
    instance.thread.start()
    env.threads.append(instance.thread)
    env.wait(lambda: env.ping().get("status") == "serving", 30, "hosted lifecycle launcher")
    return instance


@case("writer_complete_prefix", stub_host=True)
def writer_complete_prefix(env: Env) -> dict[str, bool]:
    """916: disjoint declared child prefixes do not authorize a broad writer; pin conflicts are rechecked at dispatch."""
    hold_forever(env)
    catalog_script(env)
    instance = lifecycle_host(env, target="broad", held_phase="queued")
    client = env.client()
    catalog_up(env)
    transform = real_run(env, "pin_owner", "transform")
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        state["runs"]["pin_owner"]["pinned_prefixes"] = ["lakehouse/bronze/b/"]
    writer = real_run(env, "writer", "acquisition", write_prefixes=["lakehouse/bronze/a/"])
    nested = expect_error(lambda: env.submit(writer, "spark_write", "nested", ["--group", "hai"], {"job": "bronze"}))
    for a in env.state()["attempts"].values():
        if a["owner"] == "run:writer" and a["phase"] != "ended":
            client.call("cancel", {"attempt_id": a["id"]}, writer)
            env.wait_ended(a["id"])
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        state["runs"]["writer"]["write_prefixes"] = ["lakehouse/bronze/"]
    conflict = expect_error(lambda: env.submit(writer, "spark_write", "conflict", ["--group", "hai"], {"job": "bronze"}))
    for a in env.state()["attempts"].values():
        if a["owner"] == "run:writer" and a["phase"] != "ended":
            client.call("cancel", {"attempt_id": a["id"]}, writer)
            env.wait_ended(a["id"])
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        state["runs"]["pin_owner"]["pinned_prefixes"] = []
    reply = env.submit(writer, "spark_write", "broad", ["--group", "hai"], {"job": "bronze"})
    queued = env.phase(reply["attempt_id"]) == "queued"
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        state["runs"]["pin_owner"]["pinned_prefixes"] = ["lakehouse/bronze/b/"]
    instance.held_phase = ""
    time.sleep(1)
    no_dispatch = not creates(env, "-broad-") and env.phase(reply["attempt_id"]) == "queued"
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
        state["runs"]["pin_owner"]["pinned_prefixes"] = []
    env.wait_phase(reply["attempt_id"], ("started",))
    client.call("cancel", {"attempt_id": reply["attempt_id"]}, writer)
    env.wait_ended(reply["attempt_id"])

    def writer_submit(job: str, args: list[str], launch: str) -> dict[str, Any]:
        return env.submit(writer, "spark_write", launch, args, {"job": job})

    extra_checks = {}
    for job, args in (("dictionary", ["--all"]), ("checksums", ["--fixture", "create"]), ("smoke", [])):
        instance.target, instance.held_phase = f"narrow_{job}", "queued"
        with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
            state["runs"]["writer"]["write_prefixes"] = ["lakehouse/bronze/"]
        error = expect_error(partial(writer_submit, job, args, f"narrow_{job}"))
        extra_checks[f"{job}: incomplete declaration refused"] = refused(error, "refused", "declares")
        for attempt in env.state()["attempts"].values():
            if attempt["owner"] == "run:writer" and attempt["phase"] != "ended":
                client.call("cancel", {"attempt_id": attempt["id"]}, writer)
                instance.held_phase = ""
                env.wait_ended(attempt["id"])
        instance.target, instance.held_phase = f"complete_{job}", "queued"
        with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
            state["runs"]["writer"]["write_prefixes"] = ["lakehouse/"]
        positive = env.submit(writer, "spark_write", f"complete_{job}", args, {"job": job})
        extra_checks[f"{job}: complete conservative declaration admitted"] = bool(positive["attempt_id"])
        client.call("cancel", {"attempt_id": positive["attempt_id"]}, writer)
        instance.held_phase = ""
        env.wait_ended(positive["attempt_id"])
    for a in env.state()["attempts"].values():
        if a["owner"] == "run:writer" and a["phase"] != "ended":
            client.call("cancel", {"attempt_id": a["id"]}, writer)
            env.wait_ended(a["id"])
    for principal, name in ((writer, "writer"), (transform, "pin_owner")):
        client.call("finalize", {}, principal)
        client.call("release", {"record": str(record_file(env, name))}, principal)
    client.call("catalog_down")
    env.wait(lambda: env.state()["launcher"]["catalog"]["phase"] == "down")
    stop_hosted(env, instance)
    return {
        "nested declaration refused for incomplete coverage": refused(nested, "refused", "declares"),
        "full write set rechecks transform pins at submit": refused(conflict, "refused", "pinned"),
        "full writer admitted without pins": queued,
        "pin conflict introduced after admission prevents dispatch": no_dispatch,
        **extra_checks,
    }


@case("bootstrap_start_cutoff", stub_host=True)
def bootstrap_start_cutoff(env: Env) -> dict[str, bool]:
    """912: finite bootstrap initial dispatch and retry both refuse across the host cutoff."""
    checks: dict[str, bool] = {}
    for mode in ("create", "lost_start"):
        marker = Path(env.settings["lakehouse_state"]) / "polaris_bootstrapped"
        if marker.exists():
            shutil.copy2(marker, env.work / f"{mode}_prior_bootstrap_marker")
            marker.unlink()
        catalog_script(env)
        instance = lifecycle_host(env, short_bootstrap=True)
        op = "create" if mode == "create" else "start"
        gate = env.daemon.add_gate(op, "before", match=lambda c: "polaris-bootstrap" in c["name"])
        env.client().call("catalog_up")
        gate.wait_reached(30)
        attempt = next(a for a in env.state()["attempts"].values() if a["params"].get("service") == "polaris-bootstrap" and a["phase"] != "ended")
        cutoff = attempt["deadline_utc"] - 3
        time.sleep(max(0, cutoff - time.time() + 0.15))
        if mode == "lost_start":
            stop_hosted(env, instance)
            instance = lifecycle_host(env, short_bootstrap=True)
            time.sleep(0.5)
        gate.release()
        ended = env.wait_ended(attempt["id"], 30)
        count = len(starts(env, "polaris-bootstrap"))
        checks[mode + ": no daemon start after the cutoff"] = count == (0 if mode == "create" else 1)
        checks[mode + ": timeout intent is persisted"] = ended["reason"] == "timeout" and bool(ended["timeout_intent"])
        env.wait(lambda: env.state()["launcher"]["catalog"]["phase"] != "starting", 30)
        env.teardown()
        stop_hosted(env, instance)
    return checks


@case("release_phase_matrix", stub_host=True)
def release_phase_matrix(env: Env) -> dict[str, bool]:
    """910: requests through the real socket refuse while each short-lived phase or cleanup step is held."""
    hold_forever(env)
    instance = lifecycle_host(env)
    client = env.client()
    principal = env.acquire_run("matrix")
    checks: dict[str, bool] = {}

    def at_gate(attempt_id: str, phase: str, step: str) -> bool:
        return env.phase(attempt_id) == phase and (not step or env.attempt(attempt_id)["cleanup"]["step"] == step)

    for index, (phase, step) in enumerate(
        (("queued", ""), ("created", ""), ("started", ""), ("exiting", "collect"), ("exiting", "remove"), ("exiting", "confirm"))
    ):
        launch = f"matrix{index}"
        instance.target, instance.held_phase, instance.held_step = launch, phase, step
        reply = env.submit(principal, launch=launch, params=env.fixture_params(launch))
        attempt_id = reply["attempt_id"]
        if phase == "exiting":
            env.wait_phase(attempt_id, ("started",))
            client.call("cancel", {"attempt_id": attempt_id}, principal)
        env.wait(partial(at_gate, attempt_id, phase, step))
        # Finalization sets the owner fence but the controlled loop keeps the attempt at this exact phase.
        with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
            state["runs"]["matrix"]["state"] = "finalizing"
        refusal = expect_error(lambda: client.call("release", {"record": str(record_file(env, "matrix"))}, principal))
        checks[f"release refused at {phase}/{step}"] = (
            env.phase(attempt_id) == phase and (not step or env.attempt(attempt_id)["cleanup"]["step"] == step) and refused(refusal, "refused", "is open")
        )
        with run_lock.transaction(run_lock.Registry(env.registry, "test")) as state:
            state["runs"]["matrix"]["state"] = "held"
        client.call("cancel", {"attempt_id": attempt_id}, principal)
        instance.held_phase = ""
        env.wait_ended(attempt_id)
    client.call("finalize", {}, principal)
    client.call("release", {"record": str(record_file(env, "matrix"))}, principal)
    checks["release succeeds only after all attempts end"] = env.state()["runs"]["matrix"]["state"] == "released"
    stop_hosted(env, instance)
    return checks


# Every case, listed so a case that loses its decorator cannot go unnoticed: the runner checks the list against the registry.
ALL_CASES = (
    image_python_entrypoint_refused,
    deadline_relay_refused,
    dbt_archive_boundary,
    dbt_archive_record_hash,
    dbt_archive_install_scope,
    dbt_archive_install_late_foreign,
    dbt_archive_fetch_publication,
    dbt_archive_fetch_races,
    staging_pool_cancel_unresolved,
    staging_pool_connect_held,
    staging_pool_launcher_gone,
    staging_pool_removal_held,
    client_slow_launcher_replies,
    client_exchange_total_budget,
    client_fail_fast_gone,
    client_fail_fast_alive,
    client_slow_exchange_log,
    client_cleanup_after_stuck,
    client_cleanup_shared_success,
    client_cleanup_propagation,
    client_release_outcome,
    client_loss_wording,
    client_cleanup_cli,
    client_cleanup_wrappers,
    staging_preflight,
    registry_threshold_compaction,
    registry_threshold_control,
    registry_threshold_ages,
    registry_reread_on_change,
    registry_reread_inplace,
    registry_snapshot_identity,
    registry_snapshot_validation,
    staging_pool_socket_loss,
    staging_pool_lost_cancel,
    staging_pool_lost_submit,
    writer_complete_prefix,
    bootstrap_start_cutoff,
    release_phase_matrix,
    peer_user_rejection,
    cleanup_restart_each_step,
    token_store_chain,
    staging_pool_interrupt,
    pooled_launch_capacity,
    queued_cutoff_timeout,
    run_allocation_growth_reserve,
    docker_restart_command_and_client,
    effective_spec_mutations,
    mount_fence_other_checkouts,
    template_owner_predicates,
    reader_holder_death,
    query_cli,
    query_stdin_large,
    registry_history_bounded,
    registry_archive_faults,
    request_replay_outcomes,
    network_ops_restart,
    scan_reply_after_stream_loss,
    start_lost_reply_expiry_restart,
    start_lost_reply_recovery_restart,
    start_lost_reply_cancel_restart,
    happy_path,
    duplicate_submit,
    adoption_event_before_reply,
    adoption_after_launcher_kill,
    cancel_during_create,
    cancel_during_start,
    create_after_create_failed,
    subscribe_before_scan,
    release_refused_open_work,
    migration_gate,
    submit_refused_states,
    probe_not_observed,
    first_start_drained,
    probe_left_removed,
    headers_before_registration,
    recovery_cleanup_restart,
    continuous_events_during_scan,
    orphan_disposed_once,
    orphan_late_inspect_once,
    shared_allocation_two_workers,
    heavy_allocation_exclusion,
    holder_death,
    unrelated_and_reused_pid,
    catalog_checkout_guard,
    catalog_lifecycle,
    bootstrap_failure,
    catalog_down_refusals,
    catalog_down_barrier,
    docker_restart,
    secret_commitment,
    effective_spec_digest,
    deadline_acceptance,
    deadline_spec,
    start_after_deadline_refused,
    launcher_enforces_deadline,
    docker_unknown_then_back,
    removal_failure_and_restarts,
    image_without_entrypoint_refused,
    method_matrix,
    replay_and_token_store,
    token_not_disclosed,
    mount_and_argument_refusals,
    mount_containment_depth_and_case,
    kit_launcher_ownership,
    test_registry_allowed_roots,
    real_dbt_refused_until_provenance,
    render_templates,
    spark_mount_narrowed,
    interactive_sessions,
    package_folder_guard,
    pins_record,
    pins_replaced_before_record,
    snapshot_copy,
    snapshot_copy_failures,
    growth_reserve,
    docker_calls_check,
    docker_calls_reviewed_binding,
    client_exit_codes,
    kit_launcher_client,
    dbt_and_silver_clients,
    catalog_client,
    staging_runner_conversion,
    staging_read_share,
    staging_read_plan_shrinks,
)


def shard_of(names: list[str], shard: str) -> list[str]:
    """Every Nth case for shard K/N (1-based), in registration order; CI runs the shards as parallel jobs."""
    match = re.fullmatch(r"([1-9][0-9]*)/([1-9][0-9]*)", shard)
    if match is None or int(match[1]) > int(match[2]):
        raise SystemExit(f"--shard must be K/N with 1 <= K <= N, not {shard!r}")
    index, count = int(match[1]) - 1, int(match[2])
    return [name for position, name in enumerate(names) if position % count == index]


def run_cases(selected: list[str], shard: str = "") -> int:
    """Run the selected cases (all by default, or one shard of them), write the report and return the exit code."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + (f"_shard{shard.replace('/', 'of')}" if shard else "")
    report = OUT / stamp
    report.mkdir(parents=True)
    if {function.__name__ for function in ALL_CASES} != {function.__name__ for function, _ in CASES.values()}:
        raise SystemExit("ALL_CASES differs from the registered cases")
    changed = run_command("git", ["diff", "--name-only"], cwd=ROOT).stdout.splitlines()
    sources = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in changed if (ROOT / name).is_file()}
    names = selected or list(CASES)
    if shard:
        names = shard_of(names, shard)
    results: list[dict[str, Any]] = []
    for name in names:
        function, settings = CASES[name]
        env = Env(name, report, **dict(settings))
        started = time.monotonic()
        error = ""
        checks: dict[str, bool] = {}
        try:
            checks = function(env)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
        leftovers = env.close()
        checks["nothing of the case is left over"] = not leftovers
        status = "pass" if checks and all(checks.values()) and not error else "fail"
        results.append(
            {"case": name, "status": status, "checks": checks, "error": error, "leftovers": leftovers, "seconds": round(time.monotonic() - started, 1)}
        )
        sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name} ({results[-1]['seconds']} s)\n")
        for check, ok in checks.items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
        if error:
            sys.stdout.write(f"      error: {error.splitlines()[0]}\n")
    head = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip()
    summary = {
        "feature": "single launcher, fake Docker, test registries",
        "command": ".venv/bin/python -m scripts.lakehouse.run_launcher_e2e",
        "started_utc": stamp,
        "head": head,
        "source_snapshot": sources,
        "source_unchanged": all(
            (ROOT / name).is_file() and hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest for name, digest in sources.items()
        ),
        "python": sys.version.split()[0],
        "cases": results,
        "passed": sum(1 for r in results if r["status"] == "pass"),
        "total": len(results),
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{summary['passed']}/{summary['total']}: {report.relative_to(ROOT)}/report.json\n")
    return 0 if summary["passed"] == summary["total"] and summary["source_unchanged"] else 1


def main() -> int:
    """Parse arguments and run."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", action="append", default=[], choices=sorted(CASES))
    parser.add_argument("--shard", default="", help="run every Nth case only, as K/N (1-based)")
    args = parser.parse_args()
    return run_cases(args.case, args.shard)


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["Env", "Gate", "case", "run_lock"]
