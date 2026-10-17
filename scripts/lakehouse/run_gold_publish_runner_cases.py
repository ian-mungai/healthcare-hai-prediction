"""Cases of the gold publish host driver itself: start-up and clean-up failures and the sentinel scan of the final report.

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


def drive(root: Path, mode: str, behavior: Behavior, steps: Callable[[Any, Path], list[dict[str, Any]]] | None = None) -> Outcome:
    """Run ``main`` of the driver once with the stand-in client; a folder the driver preserved is removed here, after it is recorded."""
    RUNS[0] += 1
    out = OUT_ROOT[0] / f"driver_run_{RUNS[0]:02d}"
    folders: list[Path] = []
    real_mkdtemp = tempfile.mkdtemp

    def mkdtemp(*args: Any, **kwargs: Any) -> str:
        path = real_mkdtemp(*args, **{**kwargs, "dir": str(root)})
        folders.append(Path(path))
        return path

    FakeClient.instances = []
    FakeClient.behavior = behavior
    produced = steps or (lambda session, report: [driver.step("a stand-in step", True)])
    stdout = io.StringIO()
    with (
        patch.object(driver, "OUT", out),
        patch.object(driver.sys, "argv", ["runner", "--mode", mode]),
        patch.object(driver.tempfile, "mkdtemp", mkdtemp),
        patch.object(launcher, "docker_socket_path", return_value=str(root / "engine.sock")),
        patch.object(launcher, "LauncherClient", FakeClient),
        patch.object(driver, "EXIT_SECONDS", 1.0, create=True),
        patch.object(driver, "CONTAINER_SECONDS", 1.0, create=True),
        patch.object(driver, "suite", lambda session, report, selected=None: produced(session, report)),
        patch.object(driver, "publish_offline", lambda session, report: produced(session, report)),
        contextlib.redirect_stdout(stdout),
    ):
        status = driver.main()
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


CASES: dict[str, Callable[[Path], Check]] = {
    "startup_cleanup": startup_cases,
    "final_report_scan": sentinel_cases,
    "pid_ownership": pid_cases,
    "pid_identity": identity_cases,
    "real_process": real_process_cases,
    "final_report_keys": key_cases,
    "no_pid_signal": no_signal_cases,
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
