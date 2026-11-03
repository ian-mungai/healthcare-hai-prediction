"""Cases of publication host report completeness, start-up, clean-up and the sentinel scan of the final report.

Run ``.venv/bin/python -m scripts.lakehouse.run_gold_publish_e2e --mode runner``. Each case runs the driver's own ``main`` with a stand-in for the launcher
client, so no launcher process, container or Docker call exists: the stand-in records what the driver asks of it, and a case reads the report the driver
wrote, the folder it left and the calls it made. The stand-in takes the place of the launcher only; the Session, the clean-up, the sentinel scan and the
report writer are the driver's own code. The PID cases put a stand-in process table in place of the operating system's: no process is signalled.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import shutil
import signal
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from orchestration import launcher
from scripts.features import run_feature_store_e2e as feature_driver
from scripts.lakehouse import run_gold_publish_e2e as driver

FAKE_PID = 2**30

OUT_ROOT = [Path()]
RUNS = [0]
Check = dict[str, bool]


@dataclass
class Behavior:
    """What the stand-in launcher does wrong, if anything."""

    ping_fails: bool = False
    allocate_fails: bool = False
    status_fails: bool = False
    shutdown_fails: bool = False
    stays_alive: bool = False
    containers: list[str] = field(default_factory=list)
    containers_after_attempt: list[str] = field(default_factory=list)
    attempt: Callable[[str, str, list[str], dict[str, Any]], Any] | None = None


class FakeClient:
    """Stands for ``launcher.LauncherClient``: records the calls and reports whether its launcher would still be alive."""

    instances: list[FakeClient] = []
    behavior = Behavior()

    def __init__(self, config: Any, start: bool = True) -> None:
        self.config = config
        self.launch_nonce = ""
        self.calls: list[str] = []
        self.alive = False
        FakeClient.instances.append(self)

    def connect(self) -> FakeClient:
        self.calls.append("connect")
        self.alive = True
        return self

    def close(self) -> None:
        return None

    def open_socket(self, timeout: float | None = None) -> FakeClient:
        if not self.alive:
            raise OSError("no listener")
        return self

    def call(self, method: str, params: dict[str, Any] | None = None, principal: dict[str, Any] | None = None, **_: Any) -> Any:
        self.calls.append(method)
        behavior = FakeClient.behavior
        if method == "ping":
            if behavior.ping_fails:
                raise launcher.ClientError("readiness failure", "unavailable")
            return {"status": "serving", "pid": FAKE_PID, "launch_nonce": self.launch_nonce}
        if method == "status":
            if behavior.status_fails and "allocate" in self.calls:
                raise launcher.ClientError("status failure", "unavailable")
            return {"containers": list(behavior.containers)}
        if method == "shutdown":
            if behavior.shutdown_fails:
                raise launcher.ClientError("shutdown failure", "unavailable")
            self.alive = behavior.stays_alive
            return {"ok": True}
        return {}

    def allocate(self, workers: int = 1, seconds: float = 120.0) -> str:
        self.calls.append("allocate")
        if FakeClient.behavior.allocate_fails:
            raise launcher.ClientError("allocation failure", "budget")
        return "alloc-1"

    @staticmethod
    def principal_alloc(alloc_id: str) -> dict[str, Any]:
        return {"kind": "alloc", "alloc_id": alloc_id, "token": "token-" + os.urandom(6).hex()}

    def release_allocation(self, alloc_id: str) -> None:
        self.calls.append("release_allocation")

    def run_attempt(self, principal: Any, template: str, launch: str, args: list[str], params: dict[str, Any], **_: Any) -> Any:
        self.calls.append(f"attempt:{launch}")
        if FakeClient.behavior.attempt is None:
            raise RuntimeError("no synthetic transport configured")
        result = FakeClient.behavior.attempt(template, launch, args, params)
        if FakeClient.behavior.containers_after_attempt:
            FakeClient.behavior.containers = list(FakeClient.behavior.containers_after_attempt)
        return result


@dataclass
class Outcome:
    """What one run of the driver left behind."""

    status: int
    report: dict[str, Any]
    text: str
    folders: list[Path]
    clients: list[FakeClient]
    preserved: bool

    @property
    def shutdowns(self) -> int:
        return sum(client.calls.count("shutdown") for client in self.clients)


def drive(
    root: Path,
    mode: str,
    behavior: Behavior,
    steps: Callable[[Any, Path], list[dict[str, Any]]] | None = None,
    *,
    real_readers: bool = False,
    selected: list[str] | None = None,
    feature: bool = False,
) -> Outcome:
    """Run ``main`` of the driver once with the stand-in client; a folder the driver preserved is removed here, after it is recorded."""
    RUNS[0] += 1
    out = OUT_ROOT[0] / f"driver_run_{RUNS[0]:02d}"
    if feature:
        out = feature_driver.OUT / "report_consumers" / OUT_ROOT[0].parent.name / OUT_ROOT[0].name / out.name
    folders: list[Path] = []
    real_mkdtemp = tempfile.mkdtemp

    def mkdtemp(suffix: str | None = None, prefix: str | None = None, dir: str | None = None) -> str:
        path = real_mkdtemp(suffix=suffix, prefix=prefix, dir=str(root))
        folders.append(Path(path))
        return path

    FakeClient.instances = []
    FakeClient.behavior = behavior
    produced = steps or (lambda session, report: [driver.step("a stand-in step", True)])
    stdout = io.StringIO()
    stderr = io.StringIO()
    with (
        patch.object(driver, "OUT", out),
        patch.object(driver.sys, "argv", ["runner", "--mode", mode, *(["--case", *selected] if selected else [])]),
        patch.object(driver.tempfile, "mkdtemp", mkdtemp),
        patch.object(launcher, "docker_socket_path", return_value=str(root / "engine.sock")),
        patch.object(launcher, "LauncherClient", FakeClient),
        patch.object(driver, "EXIT_SECONDS", 1.0, create=True),
        patch.object(driver, "CONTAINER_SECONDS", 1.0, create=True),
        contextlib.ExitStack() as substitutes,
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        if not real_readers:
            substitutes.enter_context(patch.object(driver, "suite", lambda session, report, selected=None: produced(session, report)))
            substitutes.enter_context(patch.object(driver, "publish_offline", lambda session, report: produced(session, report)))
        if feature:
            substitutes.enter_context(patch.object(feature_driver, "OUT", out))
            substitutes.enter_context(patch.object(feature_driver, "publish_exports", synthetic_feature_exports))
        status = feature_driver.main() if feature else driver.main()
    (out / "driver_stdout.log").write_text(stdout.getvalue())
    (out / "driver_stderr.log").write_text(stderr.getvalue())
    reports = sorted(out.glob("*/report.json"))
    text = reports[-1].read_text() if reports else ""
    outcome = Outcome(status, json.loads(text) if text else {}, text, list(folders), list(FakeClient.instances), any(folder.exists() for folder in folders))
    for folder in folders:
        shutil.rmtree(folder, ignore_errors=True)
    return outcome


def startup_cases(root: Path) -> Check:
    """A launcher the driver started is stopped and proven gone also when the start-up fails; an unresolved clean-up fails the run and keeps what is
    needed to finish it."""
    checks: Check = {}
    control = drive(root, "suite", Behavior())
    checks["control: a healthy run passes and asks the launcher to stop once"] = (
        control.status == 0 and control.report.get("status") == "pass" and control.shutdowns == 1
    )
    checks["control: a healthy run removes its folder"] = not control.preserved
    for label, behavior in (
        ("the launcher answers but is not ready", Behavior(ping_fails=True)),
        ("the allocation fails after the launcher started", Behavior(allocate_fails=True)),
    ):
        failed = drive(root, "suite", behavior)
        checks[f"[{label}] the run fails"] = failed.status != 0 and failed.report.get("status") == "fail"
        checks[f"[{label}] the launcher that was started is asked to stop"] = failed.shutdowns >= 1
        checks[f"[{label}] the launcher is gone and the folder is removed"] = all(not client.alive for client in failed.clients) and not failed.preserved
    stuck = drive(root, "suite", Behavior(shutdown_fails=True, stays_alive=True))
    checks["[the launcher cannot be stopped] the run fails and the report says the launcher is left"] = (
        stuck.status != 0 and stuck.report.get("status") == "fail" and bool(stuck.report.get("cleanup", {}).get("launcher_left"))
    )
    checks["[the launcher cannot be stopped] the registry folder is kept and named in the report"] = stuck.preserved and any(
        str(folder) in json.dumps(stuck.report) for folder in stuck.folders
    )
    unread = drive(root, "suite", Behavior(status_fails=True))
    checks["[the containers cannot be read] the run fails and the folder is kept"] = unread.status != 0 and unread.preserved
    left = drive(root, "suite", Behavior(containers=["hai-test-container"]))
    checks["[containers are left] the run fails, names them and keeps the folder"] = (
        left.status != 0 and "hai-test-container" in json.dumps(left.report) and left.preserved
    )
    unstopped = drive(root, "suite", Behavior(stays_alive=True))
    checks["[the launcher answers after the stop request] the run fails and keeps the folder"] = unstopped.status != 0 and unstopped.preserved
    return checks


def sentinel_cases(root: Path) -> Check:
    """The scan reads the final serialized report; a planted value in a result tail, a case error or the run's error fails the run, and the persisted
    report does not hold the value."""
    checks: Check = {}
    seen: list[str] = []

    def planted(session: Any, kind: str) -> str:
        value = driver.ROW_SENTINEL if kind == "the planted row value" else str(session.sentinels["POLARIS_GOLD_WRITER_CLIENT_SECRET"])
        seen.append(value)
        return value

    def produce(place: str, kind: str) -> Callable[[Any, Path], list[dict[str, Any]]]:
        def steps(session: Any, report: Path) -> list[dict[str, Any]]:
            value = planted(session, kind)
            if place == "the run's own error":
                raise RuntimeError("job failed with " + value)
            if place == "a case error":
                return [driver.step("a case", True, error="Failure: " + value)]
            return [driver.step("a job result", True, tail="exit 0 " + value)]

        return steps

    control = drive(root, "publish_offline", Behavior(), lambda session, report: [driver.step("a job result", True, tail="exit 0 clean output")])
    checks["control: a result tail without a planted value passes the scan"] = control.status == 0 and control.report.get("status") == "pass"
    for place in ("a result tail", "a case error", "the run's own error"):
        for kind in ("the planted row value", "a catalog credential"):
            seen.clear()
            outcome = drive(root, "publish_offline", Behavior(), produce(place, kind))
            label = f"[{kind} in {place}]"
            checks[f"{label} the run fails"] = outcome.status != 0 and outcome.report.get("status") == "fail"
            checks[f"{label} the persisted report does not hold the value"] = bool(seen) and all(value not in outcome.text for value in seen)
            checks[f"{label} the scan of the final report is a failed step"] = any(
                item.get("ok") is False and "final report" in str(item.get("step")) for item in outcome.report.get("steps", [])
            )
    return checks


class ProcessTable:
    """Stands for the operating system's process table: a start text, a command and a session per PID. It records every signal sent; a signalled process ends.

    It also stands for the races and refusals of a real table: ``plan`` runs an action (a process replaced by another) right after the n-th read of a kind
    (``start``, ``command`` or ``session``), ``refuse`` makes a PID exist without a readable start (the access refusal of a process of another user), and
    ``mute`` makes its command unreadable.
    """

    def __init__(self) -> None:
        self.starts: dict[int, str] = {}
        self.commands: dict[int, str] = {}
        self.sessions: dict[int, int] = {}
        self.signals: list[tuple[int, int]] = []

        self.reads_at_signal: dict[str, int] | None = None
        self.refused: set[int] = set()
        self.muted: set[int] = set()
        self.reads = {"start": 0, "command": 0, "session": 0}
        self.plans: list[tuple[str, int, Callable[[], None]]] = []

    def add(self, pid: int, start: str, command: str, session: int | None = None) -> None:
        self.starts[pid] = start
        self.commands[pid] = command
        if session is not None:
            self.sessions[pid] = session

    def plan(self, kind: str, nth: int, action: Callable[[], None]) -> None:
        self.plans.append((kind, nth, action))

    def refuse(self, pid: int) -> None:
        self.refused.add(pid)

    def mute(self, pid: int) -> None:
        self.muted.add(pid)

    def read(self, kind: str) -> None:
        self.reads[kind] += 1
        for plan in [item for item in self.plans if item[0] == kind and item[1] == self.reads[kind]]:
            self.plans.remove(plan)
            plan[2]()

    def kill(self, pid: int, sig: int) -> None:
        if pid in self.refused and not sig:
            raise PermissionError(pid)
        if pid not in self.starts:
            raise ProcessLookupError(pid)
        if sig:
            if self.reads_at_signal is None:
                self.reads_at_signal = dict(self.reads)
            self.signals.append((pid, sig))
            del self.starts[pid]
            del self.commands[pid]

    def process_start(self, pid: int) -> str | None:
        value = None if pid in self.refused else self.starts.get(pid)
        self.read("start")
        return value

    def process_session(self, pid: int) -> int | None:
        value = self.sessions.get(pid)
        self.read("session")
        return value

    def run_command(self, program: str, arguments: list[str], **_: Any) -> SimpleNamespace:
        if "-p" in arguments:
            pid = int(arguments[arguments.index("-p") + 1])
            found = pid in self.commands and pid not in self.muted
            value = self.commands[pid] if found else ""
            self.read("command")
            return SimpleNamespace(stdout=value + "\n", returncode=0 if found else 1)
        listing = "".join(f"{pid} {command}\n" for pid, command in self.commands.items())
        return SimpleNamespace(stdout=listing, returncode=0)


class GoneClient:
    """A launcher client whose launcher no longer listens: the socket is refused, the shutdown request is accepted and no container exists."""

    @staticmethod
    def open_socket(timeout: float | None = None) -> None:
        raise OSError("the launcher is gone")

    @staticmethod
    def call(method: str, params: dict[str, Any] | None = None, principal: dict[str, Any] | None = None, **_: Any) -> Any:
        return {"containers": []} if method == "status" else {}


def stamp(offset: float) -> str:
    """A start time as ``ps -o lstart=`` prints it, ``offset`` seconds from now."""
    return time.strftime("%a %b %d %H:%M:%S %Y", time.localtime(time.time() + offset))


@contextlib.contextmanager
def stood_in(root: Path, table: ProcessTable, saved: tuple[int | None, str | None], command: str = "", saved_session: int | None = None) -> Iterator[Any]:
    """A session whose launcher is gone from its socket, over the stand-in table.

    ``saved`` is the PID and start identity the session recorded when its launcher answered and ``saved_session`` the session recorded with it; ``command``
    (with ``{registry}``) is added to the table as the PID's command line when given.
    """
    real_mkdtemp = tempfile.mkdtemp
    with (
        patch.object(driver.tempfile, "mkdtemp", lambda *args, **kwargs: real_mkdtemp(*args, **{**kwargs, "dir": str(root)})),
        patch.object(driver, "run_command", table.run_command),
        patch.object(driver.os, "kill", table.kill),
        patch.object(driver.run_lock, "process_start", table.process_start),
        patch.object(driver, "process_session", table.process_session, create=True),
        patch.object(driver, "EXIT_SECONDS", 0.3, create=True),
        patch.object(driver, "CONTAINER_SECONDS", 1.0, create=True),
    ):
        session = driver.Session()
        session.client = GoneClient()
        session.pid, session.pid_start = saved
        session.pid_session = saved_session
        if command:
            table.commands = {pid: command.format(registry=session.registry) for pid in table.starts}
        yield session


def close_session(
    root: Path, table: ProcessTable, saved: tuple[int | None, str | None], command: str = "", capture: bool = False, saved_session: int | None = None
) -> tuple[dict[str, Any], bool]:
    """Close a session over the stand-in table; returns the clean-up report and whether the folder is still there.

    With ``capture`` the driver's own capture (``launcher_start``) takes the identity from the table, as it does when the launcher answers: ``saved`` then
    holds the PID only.
    """
    with stood_in(root, table, saved, command, saved_session) as session:
        if capture:
            session.pid_start = session.launcher_start(session.pid)
        report = session.close()
    kept = session.temp.exists()
    shutil.rmtree(session.temp, ignore_errors=True)
    return report, kept


def stayed(report: dict[str, Any], kept: bool) -> bool:
    """The clean-up kept a launcher proven to be this session's own: it did not stop after the shutdown request, was not signalled, and its PID is named."""
    return bool(report["launcher_left"]) and kept and any(str(FAKE_PID) in error and "did not stop" in error for error in report["errors"])


def pid_cases(root: Path) -> Check:
    """The driver tells the processes it started from all others; it signals none of them. A process proven to be this session's launcher that
    stays fails the clean-up as one that did not stop; a PID that names a different process is left alone; an unproven one fails the clean-up. Every check
    also proves that no signal reached the stand-in process table."""
    checks: Check = {}
    launcher_command = "/usr/bin/python -m orchestration.launcher --test-registry {registry} serve"

    table = ProcessTable()
    table.add(FAKE_PID, "Mon Oct 12 01:02:03 2026", "")
    report, kept = close_session(root, table, (FAKE_PID, "Mon Oct 12 01:02:03 2026"), launcher_command)
    checks["control: the launcher with its saved start identity is not signalled"] = table.signals == [] and FAKE_PID in table.starts
    checks["control: the clean-up fails, names the PID as this session's launcher that did not stop and keeps the registry"] = stayed(report, kept)

    table = ProcessTable()
    table.add(FAKE_PID, "Tue Oct 13 09:09:09 2026", "unrelated-stand-in")
    report, kept = close_session(root, table, (FAKE_PID, "Mon Oct 12 01:02:03 2026"))
    checks["[the PID is reused by another process] no signal is sent"] = table.signals == []
    checks["[the PID is reused by another process] the other process is still running"] = FAKE_PID in table.starts
    checks["[the PID is reused by another process] the launcher is proven gone and the clean-up is resolved"] = (
        not report["launcher_left"] and not report["errors"] and not kept
    )

    table = ProcessTable()
    table.add(FAKE_PID, "Mon Oct 12 01:02:03 2026", "")
    report, kept = close_session(root, table, (FAKE_PID, None), launcher_command)
    checks["[the start identity was never recorded] no signal is sent"] = table.signals == []
    checks["[the start identity was never recorded] the clean-up fails, names the PID and keeps the registry"] = (
        bool(report["launcher_left"]) and kept and any(str(FAKE_PID) in error and "identity" in error for error in report["errors"])
    )

    for label, command, offset, owned in (
        ("a process that names the registry but is not a launcher", "/usr/bin/tail -f --test-registry {registry} log", 5, False),
        ("a launcher that started before the session", launcher_command, -3600, False),
        ("a launcher that started after the session", launcher_command, 5, True),
    ):
        table = ProcessTable()
        table.add(FAKE_PID, stamp(offset), "")
        report, kept = close_session(root, table, (None, None), command)
        checks[f"[no saved identity; {label}] no signal is sent"] = table.signals == [] and FAKE_PID in table.starts
        if owned:
            checks[f"[no saved identity; {label}] the clean-up fails as this session's launcher that did not stop"] = stayed(report, kept)
        else:
            checks[f"[no saved identity; {label}] the clean-up is resolved and the folder is removed"] = not report["launcher_left"] and not kept
    # The launcher's command line shows the resolved registry path (on macOS /tmp is a link): the session must hold that path to prove ownership from it.
    (root / "real").mkdir()
    (root / "link").symlink_to(root / "real")
    real_mkdtemp = tempfile.mkdtemp
    with patch.object(driver.tempfile, "mkdtemp", lambda *args, **kwargs: real_mkdtemp(*args, **{**kwargs, "dir": str(root / "link")})):
        linked = driver.Session()
    checks["the session's registry path is the resolved path that the launcher's command line shows"] = linked.registry.is_relative_to(
        (root / "real").resolve()
    )
    shutil.rmtree(linked.temp, ignore_errors=True)
    real = driver.run_lock.process_start(os.getpid())
    checks["the start time ps prints for a real process (this one) has the format the driver parses"] = (
        real is not None and datetime.strptime(real, getattr(driver, "LSTART", "%a %b %d %H:%M:%S %Y")).timestamp() <= time.time()
    )
    return checks


def identity_cases(root: Path) -> Check:
    """The driver counts a process as its own only on consistent evidence that it is the launcher it started, and signals none.
    The identity is taken around the command and the session and is refused when the process changes; a launcher is an actual ``-m orchestration.launcher``
    invocation on the exact registry, parsed from the arguments; a refused or unreadable look at a process proves nothing, so the process is kept, and the
    clean-up fails and keeps the registry."""
    checks: Check = {}
    launcher_command = "/usr/bin/python -m orchestration.launcher --test-registry {registry} serve"
    saved_start = "Mon Oct 12 01:02:03 2026"

    def resolved(report: dict[str, Any], kept: bool) -> bool:
        return not report["launcher_left"] and not report["errors"] and not kept

    def unresolved(report: dict[str, Any], kept: bool) -> bool:
        return bool(report["launcher_left"]) and kept and any(str(FAKE_PID) in error and "identity" in error for error in report["errors"])

    for label, same_second in (("the PID is reused after the command is read", False), ("the PID is reused in the same second, in another session", True)):
        table = ProcessTable()
        table.add(FAKE_PID, stamp(-5), "", session=7001)

        replacement = (stamp(-5), "unrelated-stand-in", 7002) if same_second else (stamp(0), "unrelated-stand-in", 7001)
        table.plan("command", 1, partial(table.add, FAKE_PID, *replacement))
        report, kept = close_session(root, table, (FAKE_PID, None), launcher_command, capture=True)
        checks[f"[{label} while the identity is taken] no identity is recorded"] = report["launcher_identity"] == "not recorded"
        checks[f"[{label} while the identity is taken] no signal is sent and the other process is still running"] = (
            table.signals == [] and FAKE_PID in table.starts
        )
        checks[f"[{label} while the identity is taken] the launcher is proven gone and the clean-up is resolved"] = resolved(report, kept)
    table = ProcessTable()
    table.add(FAKE_PID, stamp(-5), "unrelated-stand-in", session=7002)
    report, kept = close_session(root, table, (FAKE_PID, None), capture=True)
    checks["control: [the PID names another process before the identity is taken] no identity is recorded and no signal is sent"] = (
        report["launcher_identity"] == "not recorded" and table.signals == [] and FAKE_PID in table.starts
    )
    table = ProcessTable()
    table.add(FAKE_PID, stamp(-5), "", session=7001)
    report, kept = close_session(root, table, (FAKE_PID, None), launcher_command, capture=True)
    checks["control: [a steady launcher] its identity is recorded and it is not signalled"] = report["launcher_identity"] == "proven" and table.signals == []
    checks["control: [a steady launcher that stays] the clean-up fails as this session's launcher that did not stop"] = stayed(report, kept)

    for label, start, session_id, runs_launcher in (
        ("by a launcher invocation of the registry that started later", stamp(5), 7001, True),
        ("in the same second, by a launcher invocation of the registry in another session", saved_start, 7002, True),
        ("by a process that has the launcher's start and session but another command", saved_start, 7001, False),
    ):
        table = ProcessTable()
        table.add(FAKE_PID, start, "unrelated-stand-in", session=session_id)
        close_session(root, table, (FAKE_PID, saved_start), launcher_command if runs_launcher else "", saved_session=7001)
        checks[f"[the PID is reused {label}] no signal is sent and the other process is still running"] = table.signals == [] and FAKE_PID in table.starts
    table = ProcessTable()
    table.add(FAKE_PID, saved_start, "", session=7001)
    report, kept = close_session(root, table, (FAKE_PID, saved_start), launcher_command, saved_session=7001)
    checks["control: [the launcher keeps its saved start, session and command] it is not signalled and fails the clean-up as one that did not stop"] = (
        table.signals == [] and stayed(report, kept)
    )

    table = ProcessTable()
    table.add(FAKE_PID, saved_start, "", session=7001)
    table.plan("start", 1, lambda: table.add(FAKE_PID, stamp(0), "unrelated-stand-in", 7002))
    report, kept = close_session(root, table, (FAKE_PID, saved_start), launcher_command)
    checks["control: [the PID is reused while the saved identity is compared] no signal is sent and the other process is still running"] = (
        table.signals == [] and FAKE_PID in table.starts
    )

    for label, command, owned in (
        (
            "a Python program that only mentions the module",
            '/usr/bin/python -c "import time; time.sleep(600)" orchestration.launcher --test-registry {registry} serve',
            False,
        ),
        (
            "a script that is given -m and the module as arguments",
            "/usr/bin/python worker.py -m orchestration.launcher --test-registry {registry} serve",
            False,
        ),
        (
            "a -c program that is given -m and the module as arguments",
            "/usr/bin/python -c pass -m orchestration.launcher --test-registry {registry} serve",
            False,
        ),
        ("another module that is given the module name", "/usr/bin/python -m other.module orchestration.launcher --test-registry {registry} serve", False),
        ("a program that is not Python", "/bin/echo -m orchestration.launcher --test-registry {registry} serve", False),
        ("a launcher client (status) on the registry", "/usr/bin/python -m orchestration.launcher --test-registry {registry} status", False),
        ("a launcher of another registry", "/usr/bin/python -m orchestration.launcher --test-registry {registry}-other serve", False),
        (
            "a launcher whose interpreter has options",
            "/usr/bin/python3.12 -u -W ignore -X dev -m orchestration.launcher --test-registry {registry} serve",
            True,
        ),
    ):
        table = ProcessTable()
        table.add(FAKE_PID, stamp(5), "")
        report, kept = close_session(root, table, (None, None), command)
        checks[f"[no saved identity; {label}] no signal is sent and the process is still running"] = table.signals == [] and FAKE_PID in table.starts
        if owned:
            checks[f"[no saved identity; {label}] the clean-up fails as this session's launcher that did not stop"] = stayed(report, kept)
        else:
            checks[f"[no saved identity; {label}] the clean-up is resolved and the folder is removed"] = resolved(report, kept)

    for label, saved, refuse in (
        ("the access to a PID with a saved identity is refused", (FAKE_PID, saved_start), True),
        ("the access to a launcher without a saved identity is refused", (None, None), True),
        ("the command of a PID with a matching saved identity cannot be read", (FAKE_PID, saved_start), False),
    ):
        table = ProcessTable()
        table.add(FAKE_PID, saved_start if saved[0] else stamp(5), "")
        (table.refuse if refuse else table.mute)(FAKE_PID)
        report, kept = close_session(root, table, saved, launcher_command)
        checks[f"[{label}] no signal is sent and the process is still running"] = table.signals == [] and FAKE_PID in table.starts
        checks[f"[{label}] the clean-up fails, names the PID and keeps the registry"] = unresolved(report, kept)
    return checks


def real_process_cases(root: Path) -> Check:
    """The same decisions over the real process table (``ps`` and the session): a decoy that has the module name among its arguments is a process of this
    run that is no launcher, so the clean-up neither signals it nor fails; the observation of a real process is steady and parses."""
    checks: Check = {}
    real_mkdtemp = tempfile.mkdtemp
    with (
        patch.object(driver.tempfile, "mkdtemp", lambda *args, **kwargs: real_mkdtemp(*args, **{**kwargs, "dir": str(root)})),
        patch.object(driver, "EXIT_SECONDS", 0.3, create=True),
        patch.object(driver, "CONTAINER_SECONDS", 1.0, create=True),
    ):
        session = driver.Session()
        session.client = GoneClient()
        program = "import time; time.sleep(120)"
        decoy = os.posix_spawn(
            sys.executable, [sys.executable, "-c", program, "orchestration.launcher", "--test-registry", str(session.registry), "serve"], os.environ
        )
        try:
            report = session.close()
            ended = os.waitpid(decoy, os.WNOHANG)[0] == decoy
        finally:
            with contextlib.suppress(ProcessLookupError, ChildProcessError):
                os.kill(decoy, signal.SIGTERM)
                os.waitpid(decoy, 0)
    checks["[a real decoy that names the module among its arguments] it is not signalled"] = not ended
    checks["[a real decoy that names the module among its arguments] the clean-up is resolved and the folder is removed"] = (
        not report["launcher_left"] and not report["errors"] and not session.temp.exists()
    )
    shutil.rmtree(session.temp, ignore_errors=True)
    here = session.observe(os.getpid()) if hasattr(session, "observe") else None
    checks["the observation of a real process (this one) is steady and has a start, a command and a session"] = (
        here is not None and here.state == "steady" and bool(here.start) and bool(here.command) and here.session is not None
    )
    return checks


def key_cases(root: Path) -> Check:
    """The final scan and the redaction cover the keys of the report as well as its values; a place in the hit list never repeats a leaked key; two leaked
    keys do not merge into one entry."""
    checks: Check = {}
    seen: list[str] = []

    def keyed(shape: str) -> Callable[[Any, Path], list[dict[str, Any]]]:
        def steps(session: Any, report: Path) -> list[dict[str, Any]]:
            row, secret = driver.ROW_SENTINEL, str(session.sentinels["POLARIS_GOLD_WRITER_CLIENT_SECRET"])
            seen.extend([row, secret])
            if shape == "a checks key":
                return [driver.step("a keyed result", True, checks={row: True, "clean assertion": True})]
            if shape == "a nested key in a list":
                return [driver.step("a keyed result", True, detail=[{"inner": {secret: True}}])]
            if shape == "a key whose value holds the value too":
                return [driver.step("a keyed result", True, checks={row: "output " + row})]
            return [driver.step("a keyed result", True, checks={row: True, secret: True})]

        return steps

    control = drive(root, "publish_offline", Behavior(), lambda session, report: [driver.step("a keyed result", True, checks={"clean assertion": True})])
    checks["control: a result whose keys hold no planted value passes the scan"] = control.status == 0 and control.report.get("status") == "pass"
    for shape in ("a checks key", "a nested key in a list", "a key whose value holds the value too", "two leaked keys"):
        seen.clear()
        outcome = drive(root, "publish_offline", Behavior(), keyed(shape))
        label = f"[planted values in {shape}]"
        checks[f"{label} the run fails"] = outcome.status != 0 and outcome.report.get("status") == "fail"
        checks[f"{label} the persisted report holds neither value, also not in a place of the hit list"] = bool(seen) and all(
            value not in outcome.text for value in seen
        )
        checks[f"{label} the scan of the final report is a failed step"] = any(
            item.get("ok") is False and "final report" in str(item.get("step")) for item in outcome.report.get("steps", [])
        )
    merged: dict[str, Any] = next((item for item in outcome.report.get("steps", []) if item.get("step") == "a keyed result"), {})
    checks["[two leaked keys] both entries are still in the report"] = len(merged.get("checks", {})) == 2
    row, other = driver.ROW_SENTINEL, "writer-" + "0" * 16
    places = driver.places_of({other: {row: {"inner": row}}}, row, [row, other])
    checks["[leaked keys] a place names a key by its position and never by its text"] = places == ["<key 0>.<key 0>", "<key 0>.<key 0>.inner"]
    return checks


class StoppingClient:
    """A launcher client whose launcher honors the shutdown request: the request ends its process in the stand-in table; the socket is refused."""

    def __init__(self, table: ProcessTable, pid: int) -> None:
        self.table, self.pid, self.requests = table, pid, 0

    @staticmethod
    def open_socket(timeout: float | None = None) -> None:
        raise OSError("the launcher does not listen")

    def call(self, method: str, params: dict[str, Any] | None = None, principal: dict[str, Any] | None = None, **_: Any) -> Any:
        if method == "shutdown":
            self.requests += 1
            self.table.starts.pop(self.pid, None)
            self.table.commands.pop(self.pid, None)
        return GoneClient.call(method, params, principal)


def no_signal_cases(root: Path) -> Check:
    """The driver never signals a process by its ID: a PID can name another process between any look and the signal. The launcher stops
    itself on the shutdown request; a process of this session that stays is kept, never signalled, and the clean-up fails, names its PID and keeps the
    registry. A PID that names another process is left alone and does not fail the clean-up."""
    checks: Check = {}
    launcher_command = "/usr/bin/python -m orchestration.launcher --test-registry {registry} serve"
    saved_start = "Mon Oct 12 01:02:03 2026"

    def failed_naming_pid(report: dict[str, Any], kept: bool) -> bool:
        return bool(report["launcher_left"]) and kept and any(str(FAKE_PID) in error for error in report["errors"])

    table = ProcessTable()
    table.add(FAKE_PID, saved_start, "", session=7001)
    with stood_in(root, table, (FAKE_PID, saved_start), launcher_command, saved_session=7001) as session:
        client = StoppingClient(table, FAKE_PID)
        session.client = client
        report = session.close()
    kept = session.temp.exists()
    shutil.rmtree(session.temp, ignore_errors=True)
    checks["control: [the launcher stops on the shutdown request] no signal is sent and the clean-up is resolved"] = (
        client.requests >= 1 and table.signals == [] and not report["launcher_left"] and not report["errors"] and not kept
    )

    for label, saved, offset in (
        ("a launcher with its saved start, session and command", (FAKE_PID, saved_start), None),
        ("a launcher on this registry that started after the session, with no saved identity", (None, None), 5.0),
    ):
        table = ProcessTable()
        table.add(FAKE_PID, saved_start if offset is None else stamp(offset), "", session=7001)
        report, kept = close_session(root, table, saved, launcher_command, saved_session=7001 if saved[0] else None)
        checks[f"[{label} stays after the shutdown request] no signal is sent and the process is still running"] = (
            table.signals == [] and FAKE_PID in table.starts
        )
        checks[f"[{label} stays after the shutdown request] the clean-up fails, names the PID and keeps the registry"] = failed_naming_pid(report, kept)

    dry = ProcessTable()
    dry.add(FAKE_PID, saved_start, "", session=7001)
    close_session(root, dry, (FAKE_PID, saved_start), launcher_command, saved_session=7001)
    for label, kind in (("after the last start read before the signal", "start"), ("after the last session read before the signal", "session")):
        table = ProcessTable()
        table.add(FAKE_PID, saved_start, "", session=7001)
        if dry.reads_at_signal is not None:
            table.plan(kind, dry.reads_at_signal[kind], partial(table.add, FAKE_PID, stamp(0), "foreign-replacement", 7001))
        close_session(root, table, (FAKE_PID, saved_start), launcher_command, saved_session=7001)
        checks[f"[the PID is reused {label}] no signal is sent and the other process is still running"] = table.signals == [] and FAKE_PID in table.starts
    return checks


GOLD_DEFAULT = (
    "iceberg_check",
    "happy_path",
    "branch_isolation",
    "interruptions",
    "retry_no_double_commit",
    "tamper_and_order",
    "main_moved_refused",
    "export_and_consumer",
    "rejected_result_commit",
    "namespace_guard",
    "backup_store",
    "maintenance",
    "maintenance_protection",
    "reader_discovery",
    "contract_plan",
)
FEATURE_DEFAULT = ("happy_and_types", "retry_and_new_publication", "version_two", "interruption_between_fast_forwards", "tamper")


def passing_rows(names: tuple[str, ...]) -> list[dict[str, Any]]:
    return [{"case": name, "status": "pass", "checks": {f"synthetic assertion {name}": True}, "error": ""} for name in names]


def synthetic_feature_exports(base: Path, report: Path) -> dict[str, Any]:
    document: dict[str, Any] = {"runs": {}}
    (report / "inputs.json").write_text(json.dumps(document))
    return document


@dataclass
class ReportReplay:
    """Synthetic fixture effects and job transport, with host file readers and publication aggregation left real."""

    files: dict[str, str | None]
    failed_launch: str = ""
    versions: bool = True
    folder: Path | None = None
    requests: list[tuple[str, list[str]]] = field(default_factory=list)

    def __call__(self, template: str, launch: str, args: list[str], params: dict[str, Any]) -> SimpleNamespace:
        self.requests.append((launch, list(args)))
        ok = launch != self.failed_launch
        if template == "fixture_init":
            self.folder = Path(params["case_host"]).parent
            (self.folder / "case" / "sealed.duckdb").write_bytes(b"synthetic sealed fixture, not a database")
        elif template == "gold_export":
            folder = Path(params["publish_host"])
            digest = driver.gold_publish.sha256_file(Path(params["sealed_host"]))
            tables: dict[str, dict[str, Any]] = {name: {"columns": []} for name in driver.gold_publish.load_plan().tables}
            tables["dim_release"]["columns"] = [["publication_dates", "DATE[]"], ["download_run_ids", "VARCHAR[]"]]
            manifest = json.dumps({"tables": tables})
            if launch == "export":
                (folder / "export").mkdir()
                (folder / "export" / "manifest.json").write_text(manifest)
            else:
                ok = False
            (folder / "consumer_result.json").write_text(
                json.dumps(
                    {
                        "run_id": params["run"],
                        "in_container_sha256": digest,
                        "expected_sha256": params["sealed_sha256"],
                        "exported": launch == "export",
                        "manifest_sha256": driver.hashlib.sha256(manifest.encode()).hexdigest(),
                    }
                )
            )
        elif template == "spark_e2e" and params["job"] == "gold_publish_local":
            if self.folder is None:
                raise RuntimeError("synthetic publication ran before fixture transport")
            namespace = self.folder / "multi" / "warehouse" / "gold_e2e"
            namespace.mkdir(parents=True, exist_ok=True)
            for name in (*gold_table_names(), *(("publish_runs",) if launch == "commit" else ())):
                (namespace / name).mkdir(exist_ok=True)
        elif template == "spark_e2e" and params["job"] in ("gold_publish_e2e", "features_publish_e2e"):
            base = feature_driver.OUT if params["job"] == "features_publish_e2e" else driver.OUT
            folder = base / args[args.index("--stamp") + 1]
            name = "cases_readback.json" if launch == "readback" else "cases_iceberg_check.json" if launch == "iceberg" else "cases_all.json"
            if "--case" in args and launch == "suite":
                selected = args[args.index("--case") + 1 : args.index("--stamp")]
                name = f"cases_{'_'.join(selected)}.json"
            payload = self.files[name]
            if payload is not None:
                (folder / name).write_text(payload)
            if launch == "iceberg" and self.versions:
                (folder / "iceberg_versions.json").write_text(json.dumps({"spark": "synthetic", "iceberg": "synthetic"}))
        else:
            raise RuntimeError(f"unexpected synthetic transport: {template} {launch}")
        return SimpleNamespace(ok=ok, exit_code=0 if ok else 7, reason="" if ok else "synthetic transport failure", stdout="", stderr="")


def gold_table_names() -> tuple[str, ...]:
    return tuple(driver.gold_publish.load_plan().tables)


@dataclass
class ReportInput:
    label: str
    payload: str | None
    reason: tuple[str, ...]
    failed_row: dict[str, Any] | None = None


def report_inputs(names: tuple[str, ...]) -> list[ReportInput]:
    """Malformed evidence and contradictory rows, each paired with its intended diagnostic or failed case."""
    rows = passing_rows(names)
    inputs = [
        ReportInput("absent report", None, ("missing",)),
        ReportInput("zero bytes", "", ("JSON",)),
        ReportInput("whitespace", " \n\t", ("JSON",)),
        ReportInput("invalid JSON", "[", ("JSON",)),
        ReportInput("empty inventory", "[]", (names[0],)),
        ReportInput("object instead of list", "{}", ("list",)),
        ReportInput("null instead of list", "null", ("list",)),
        ReportInput("string instead of list", '"not a list"', ("list",)),
        ReportInput("non-object row", json.dumps([None, *rows[1:]]), ("row",)),
        ReportInput("unknown case", json.dumps([{**rows[0], "case": "unrequested_case"}, *rows[1:]]), ("unrequested_case",)),
        ReportInput("extra passing duplicate", json.dumps([*rows, rows[0]]), (names[0],)),
    ]
    if len(names) > 1:
        inputs += [
            ReportInput("partial inventory", json.dumps(rows[:1]), (names[1],)),
            ReportInput("duplicate replaces expected case", json.dumps([rows[0], rows[0], *rows[2:]]), (names[0], names[1])),
        ]
    for field_name in ("case", "status", "checks", "error"):
        missing = dict(rows[0])
        del missing[field_name]
        inputs.append(ReportInput(f"missing {field_name}", json.dumps([missing, *rows[1:]]), (field_name,)))
    for field_name, value in (("case", 1), ("status", True), ("checks", []), ("error", False)):
        inputs.append(ReportInput(f"wrong {field_name} type", json.dumps([{**rows[0], field_name: value}, *rows[1:]]), (field_name,)))
    for label, changes, reason in (
        ("empty checks", {"checks": {}}, ("checks",)),
        ("false check despite pass", {"checks": {"synthetic assertion": False}}, ("checks",)),
        ("integer check despite pass", {"checks": {"synthetic assertion": 1}}, ("checks",)),
        ("string check despite pass", {"checks": {"synthetic assertion": "true"}}, ("checks",)),
        ("error despite pass", {"error": "synthetic producer error"}, ("error",)),
        ("failed status with true checks", {"status": "fail"}, ("status",)),
        ("pending status", {"status": "pending"}, ("status",)),
        ("unknown status", {"status": "unrecognized"}, ("status",)),
    ):
        row = {**rows[0], **changes}
        inputs.append(ReportInput(label, json.dumps([row, *rows[1:]]), reason, row))
    failed = {**rows[0], "status": "fail", "error": "synthetic duplicate failure"}
    inputs.append(ReportInput("failed row among passing duplicates", json.dumps([*rows, failed]), (names[0],), failed))
    inputs.append(ReportInput("only first-row checks", json.dumps([{"checks": {}}]), ("case",)))
    return inputs


def replay_run(
    root: Path,
    mode: str,
    filename: str,
    payload: str | None,
    *,
    feature: bool = False,
    selected: tuple[str, ...] = (),
    behavior: Behavior | None = None,
    failed_launch: str = "",
    versions: bool = True,
) -> tuple[Outcome, ReportReplay]:
    files = {
        "cases_all.json": json.dumps(passing_rows(FEATURE_DEFAULT if feature else GOLD_DEFAULT)),
        "cases_readback.json": json.dumps(passing_rows(("readback",))),
        "cases_iceberg_check.json": json.dumps(passing_rows(("iceberg_check",))),
        filename: payload,
    }
    replay = ReportReplay(files, failed_launch, versions)
    behavior = behavior or Behavior()
    behavior.attempt = replay
    return drive(root, mode, behavior, real_readers=True, selected=list(selected), feature=feature), replay


def cleanup_of(outcome: Outcome) -> dict[str, Any]:
    if "cleanup" in outcome.report:
        return dict(outcome.report["cleanup"])
    return next((dict(item["cleanup"]) for item in outcome.report.get("steps", []) if "cleanup" in item), {})


def cleaned(outcome: Outcome) -> bool:
    cleanup = cleanup_of(outcome)
    return (
        bool(cleanup)
        and cleanup.get("launcher_left") is False
        and cleanup.get("containers_left") == []
        and cleanup.get("errors") == []
        and cleanup.get("registry_kept") is None
        and len(outcome.clients) == 1
        and outcome.shutdowns == 1
        and outcome.clients[0].calls.count("release_allocation") == 1
        and not outcome.clients[0].alive
        and not outcome.preserved
    )


def case_names(outcome: Outcome) -> list[str]:
    return [name.removeprefix("case ") for item in outcome.report.get("steps", []) if (name := item.get("step", item.get("name", ""))).startswith("case ")]


def complete_control(outcome: Outcome, names: tuple[str, ...], probes: tuple[str, ...] = ()) -> bool:
    text = json.dumps(outcome.report.get("steps", []))
    case_steps = [item for item in outcome.report.get("steps", []) if item.get("step", item.get("name", "")).startswith("case ")]
    return (
        outcome.status == 0
        and outcome.report.get("status") == "pass"
        and outcome.report.get("error") == ""
        and case_names(outcome) == list(names)
        and [item.get("checks") for item in case_steps] == [row["checks"] for row in passing_rows(names)]
        and all(item.get("error") == "" for item in case_steps)
        and all(f"synthetic assertion {name}" in text for name in (*names, *probes))
        and all(item.get("ok", item.get("verdict") == "pass") is True for item in outcome.report.get("steps", []))
        and cleaned(outcome)
    )


def refused_for(outcome: Outcome, filename: str, invalid: ReportInput) -> bool:
    failed = [item for item in outcome.report.get("steps", []) if item.get("ok") is False or item.get("verdict") == "fail"]
    messages = [outcome.report.get("error"), *(item.get("error") for item in failed)]
    for message in messages:
        if not isinstance(message, str) or not message:
            continue
        diagnostic = message.splitlines()[0].lower()
        reason = re.sub(r"\S*" + re.escape(filename.lower()), "", diagnostic)
        if filename.lower() in diagnostic and all(re.search(r"(?<!\w)" + re.escape(word.lower()) + r"(?!\w)", reason) for word in invalid.reason):
            return True
    row = invalid.failed_row
    return row is not None and any(
        item.get("step", item.get("name")) == f"case {row['case']}" and item.get("checks") == row["checks"] and item.get("error") == row["error"]
        for item in failed
    )


def diagnostic_reason_cases(root: Path) -> Check:
    """Reject generic report refusals that identify no defective field."""
    filename = "cases_all.json"
    checks: Check = {}
    for field_name in ("case", "error"):
        invalid = ReportInput(f"missing {field_name}", None, (field_name,))
        for label, message, expected in (
            ("generic filename only", f"{filename}: invalid report", False),
            ("traceback source text", f"{filename}: invalid report\nTraceback mentions {field_name}", False),
            ("specific missing field", f"{filename}: missing {field_name}", True),
        ):
            outcome = Outcome(1, {"status": "fail", "error": message, "steps": []}, "", [], [], False)
            checks[f"{field_name}: {label}"] = refused_for(outcome, filename, invalid) is expected
    return checks


def report_consumer_cases(root: Path, boundary: str) -> Check:
    feature = boundary == "feature"
    mode = "publish_offline" if boundary in ("feature", "publish", "readback") else "local_iceberg" if boundary == "iceberg" else "suite"
    names = FEATURE_DEFAULT if feature else ("readback",) if boundary == "readback" else ("iceberg_check",) if boundary == "iceberg" else GOLD_DEFAULT
    filename = "cases_readback.json" if boundary == "readback" else "cases_iceberg_check.json" if boundary == "iceberg" else "cases_all.json"
    expected = GOLD_DEFAULT if boundary == "readback" else () if boundary == "iceberg" else names
    probes = names if boundary in ("readback", "iceberg") else ()
    launch = "readback" if boundary == "readback" else "iceberg" if boundary == "iceberg" else "suite"
    control, transport = replay_run(root, mode, filename, json.dumps(passing_rows(names)), feature=feature)
    checks = {f"{boundary}: complete positive control has exact inventory, exit 0 and persisted cleanup": complete_control(control, expected, probes)}
    checks[f"{boundary}: positive control reaches the requested transport"] = sum(name == launch for name, _ in transport.requests) == 1
    if feature:
        checks["feature: default contains exactly five cases and two host controls"] = control.report.get("counts") == {"pass": 7, "fail": 0, "pending": 0}
    for invalid in report_inputs(names):
        outcome, transport = replay_run(root, mode, filename, invalid.payload, feature=feature)
        label = f"{boundary}: {invalid.label}"
        checks[f"{label}: exit 1 and persisted fail"] = outcome.status == 1 and outcome.report.get("status") == "fail"
        checks[f"{label}: failure names the intended report defect"] = refused_for(outcome, filename, invalid)
        checks[f"{label}: real session cleanup is persisted and resolved"] = cleaned(outcome)
        checks[f"{label}: report transport was reached once"] = sum(name == launch for name, _ in transport.requests) == 1
    return checks


def selected_report_cases(root: Path) -> Check:
    checks: Check = {}
    for selected in (("contract_plan",), ("happy_path", "contract_plan"), ("contract_plan", "contract_plan")):
        filename = f"cases_{'_'.join(selected)}.json"
        control, transport = replay_run(root, "suite", filename, json.dumps(passing_rows(selected)), selected=selected)
        checks[f"{selected}: exact requested order and multiplicity pass"] = complete_control(control, selected)
        checks[f"{selected}: transport receives the explicit selection unchanged"] = transport.requests[0][1][:-2] == ["--case", *selected]
        missing = ReportInput("requested occurrence missing", json.dumps(passing_rows(selected[:-1])), (selected[-1],))
        outcome, _ = replay_run(root, "suite", filename, missing.payload, selected=selected)
        checks[f"{selected}: a missing requested occurrence fails for its name with cleanup"] = (
            outcome.status == 1 and outcome.report.get("status") == "fail" and refused_for(outcome, filename, missing) and cleaned(outcome)
        )
    return checks


def report_lifecycle_cases(root: Path) -> Check:
    checks: Check = {}
    for feature in (False, True):
        mode = "publish_offline" if feature else "suite"
        names: tuple[str, ...] = FEATURE_DEFAULT if feature else GOLD_DEFAULT
        payload = json.dumps(passing_rows(names))
        label = "feature" if feature else "gold"
        control, _ = replay_run(root, mode, "cases_all.json", payload, feature=feature)
        checks[f"{label}: lifecycle positive control is complete"] = complete_control(control, names)
        failed, _ = replay_run(root, mode, "cases_all.json", payload, feature=feature, failed_launch="suite")
        checks[f"{label}: complete passing report cannot override failed transport"] = (
            failed.status == 1
            and failed.report.get("status") == "fail"
            and any(
                item.get("step", item.get("name")) == "the case suite ran in the Spark image"
                and item.get("ok", item.get("verdict") == "pass") is False
                and item.get("exit") == 7
                for item in failed.report.get("steps", [])
            )
            and case_names(failed) == list(names)
            and cleaned(failed)
        )
        for body in (payload, "["):
            unresolved, _ = replay_run(root, mode, "cases_all.json", body, feature=feature, behavior=Behavior(status_fails=True))
            cleanup = cleanup_of(unresolved)
            checks[f"{label}: cleanup error survives {'invalid JSON' if body == '[' else 'complete report'}"] = (
                unresolved.status == 1
                and unresolved.report.get("status") == "fail"
                and unresolved.preserved
                and bool(cleanup.get("registry_kept"))
                and "the containers cannot be read: ClientError" in cleanup.get("errors", [])
                and unresolved.shutdowns == 1
            )
            if body == "[":
                checks[f"{label}: invalid JSON and cleanup failure are both persisted"] = refused_for(
                    unresolved, "cases_all.json", ReportInput("invalid JSON", "[", ("JSON",))
                )
        left, _ = replay_run(root, mode, "cases_all.json", payload, feature=feature, behavior=Behavior(stays_alive=True))
        checks[f"{label}: launcher left after valid report fails and retains its registry"] = (
            left.status == 1
            and left.report.get("status") == "fail"
            and cleanup_of(left).get("launcher_left") is True
            and bool(cleanup_of(left).get("registry_kept"))
            and left.preserved
            and left.shutdowns == 1
        )
        containers, _ = replay_run(
            root, mode, "cases_all.json", payload, feature=feature, behavior=Behavior(containers_after_attempt=["synthetic-leftover-container"])
        )
        checks[f"{label}: container left after valid report fails and retains its registry"] = (
            containers.status == 1
            and containers.report.get("status") == "fail"
            and cleanup_of(containers).get("containers_left") == ["synthetic-leftover-container"]
            and bool(cleanup_of(containers).get("registry_kept"))
            and containers.preserved
            and containers.shutdowns == 1
        )
    for mode, filename, names, expected, launch in (
        ("publish_offline", "cases_all.json", GOLD_DEFAULT, GOLD_DEFAULT, "suite"),
        ("publish_offline", "cases_readback.json", ("readback",), GOLD_DEFAULT, "readback"),
        ("local_iceberg", "cases_iceberg_check.json", ("iceberg_check",), (), "iceberg"),
    ):
        payload = json.dumps(passing_rows(names))
        control, _ = replay_run(root, mode, filename, payload)
        checks[f"{mode} {launch}: complete transport positive control"] = complete_control(control, expected, names)
        failed, _ = replay_run(root, mode, filename, payload, failed_launch=launch)
        checks[f"{mode} {launch}: failed transport cannot be hidden by passing report"] = (
            failed.status == 1
            and failed.report.get("status") == "fail"
            and any(item.get("ok") is False and item.get("exit") == 7 for item in failed.report.get("steps", []))
            and all(f"synthetic assertion {name}" in failed.text for name in names)
            and cleaned(failed)
        )
    control, _ = replay_run(root, "local_iceberg", "cases_iceberg_check.json", json.dumps(passing_rows(("iceberg_check",))))
    checks["iceberg: versions positive control is complete"] = complete_control(control, (), ("iceberg_check",))
    missing_versions, _ = replay_run(root, "local_iceberg", "cases_iceberg_check.json", json.dumps(passing_rows(("iceberg_check",))), versions=False)
    checks["iceberg: missing versions fail their existing gate despite complete case evidence"] = (
        missing_versions.status == 1
        and missing_versions.report.get("status") == "fail"
        and any(item.get("step") == "Spark and Iceberg versions" and item.get("ok") is False for item in missing_versions.report.get("steps", []))
        and cleaned(missing_versions)
    )
    return checks


def producer_report_cases(root: Path) -> Check:
    """Retain producer defaults, selection grammar and failure serialization without Spark case execution."""
    from scripts.features import features_publish_cases as features
    from scripts.lakehouse import run_gold_publish_cases as gold

    checks: Check = {}
    for module, names, harness_name in ((gold, GOLD_DEFAULT, "Harness"), (features, FEATURE_DEFAULT, "Features")):
        checks[f"{module.__name__}: production default inventory matches the fixed contract"] = tuple(n for n in module.CASES if n != "readback") == names
        scenarios = (
            ((), "pass"),
            ((names[-1],), "pass"),
            ((names[-1], names[-1]), "pass"),
            ((names[0],), "empty"),
            ((names[0],), "false"),
            ((names[0],), "raise"),
        )
        for index, (selection, kind) in enumerate(scenarios):
            out = OUT_ROOT[0] / "producer_reports" / module.__name__.split(".")[-1] / str(index)
            (out / "probe").mkdir(parents=True)
            (out / "probe" / "inputs.json").write_text('{"runs": {}}')
            seen: list[str] = []
            stopped: list[bool] = []

            def one(name: str, harness: Any, kind: str = kind, seen: list[str] = seen) -> Check:
                seen.append(name)
                if kind == "raise":
                    harness.partial = {"synthetic partial assertion": True}
                    raise RuntimeError("synthetic producer failure after partial checks")
                return {} if kind == "empty" else {"synthetic assertion": kind != "false"}

            harness = SimpleNamespace(spark=SimpleNamespace(stop=partial(stopped.append, True)), partial={})
            with (
                patch.object(module, "REPORT_ROOT", out),
                patch.object(module, "STAMP", ""),
                patch.object(module, harness_name, return_value=harness),
                patch.dict(module.CASES, {name: partial(one, name) for name in module.CASES}),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                status = module.main(["--stamp", "probe", *(["--case", *selection] if selection else [])])
            filename = f"cases_{'_'.join(selection) if selection else 'all'}.json"
            rows = json.loads((out / "probe" / filename).read_text())
            expected = list(selection or names)
            label = f"{module.__name__}: {kind} {selection}"
            checks[f"{label}: real serialization preserves exact names and Spark stop"] = [row["case"] for row in rows] == expected == seen and stopped == [
                True
            ]
            checks[f"{label}: exit and status agree with independent expectations"] = status == (0 if kind == "pass" else 1) and all(
                row["status"] == ("pass" if kind == "pass" else "fail") for row in rows
            )
            expected_checks = {} if kind == "empty" else {"synthetic partial assertion": True} if kind == "raise" else {"synthetic assertion": kind != "false"}
            checks[f"{label}: exact checks and intended error survive"] = all(
                row["checks"] == expected_checks
                and ("RuntimeError: synthetic producer failure after partial checks" in row["error"] if kind == "raise" else row["error"] == "")
                for row in rows
            )
        errors = io.StringIO()
        with (
            patch.object(module, harness_name) as construction,
            contextlib.redirect_stderr(errors),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            try:
                module.main(["--case", "unrequested_case"])
            except SystemExit as error:
                checks[f"{module.__name__}: unknown explicit case rejected by parser before harness"] = (
                    error.code == 2 and not construction.called and "invalid choice: 'unrequested_case'" in errors.getvalue() and "--case" in errors.getvalue()
                )
            else:
                checks[f"{module.__name__}: unknown explicit case rejected by parser before harness"] = False
    return checks


CASES: dict[str, Callable[[Path], Check]] = {
    "startup_cleanup": startup_cases,
    "final_report_scan": sentinel_cases,
    "pid_ownership": pid_cases,
    "pid_identity": identity_cases,
    "real_process": real_process_cases,
    "final_report_keys": key_cases,
    "no_pid_signal": no_signal_cases,
    "diagnostic_reason": diagnostic_reason_cases,
    "suite_report_consumer": partial(report_consumer_cases, boundary="suite"),
    "publish_report_consumer": partial(report_consumer_cases, boundary="publish"),
    "readback_report_consumer": partial(report_consumer_cases, boundary="readback"),
    "iceberg_report_consumer": partial(report_consumer_cases, boundary="iceberg"),
    "feature_report_consumer": partial(report_consumer_cases, boundary="feature"),
    "selected_report_consumer": selected_report_cases,
    "report_transport_cleanup": report_lifecycle_cases,
    "producer_report_semantics": producer_report_cases,
}


def run(selected: list[str], report: Path) -> list[dict[str, Any]]:
    """Run the selected cases (all by default); returns one result per case."""
    results: list[dict[str, Any]] = []
    OUT_ROOT[0] = report
    for name in selected or list(CASES):
        error = ""
        checks: Check = {}
        with tempfile.TemporaryDirectory(prefix="p1runner") as folder:
            try:
                checks = CASES[name](Path(folder))
            except Exception as exc:
                import traceback

                error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
        results.append({"case": name, "status": "pass" if checks and all(checks.values()) and not error else "fail", "checks": checks, "error": error})
    return results
