"""Host session cases for the failure paths of ``session``: a second signal, a closed output at every output point and a record that cannot be written.

The rule under test: after the host was asked to turn on, the session always ends with a capacity-zero request and a record, unless the job wait refused the
stop explicitly. No output failure, no second signal and no record failure may bypass that, and none may replace the error the caller sees.
"""

from __future__ import annotations

import signal
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infrastructure.host_session import HostSessionError, Session, TerminationRequested, install_termination_handler
from scripts.infrastructure.host_session_dev_cases import HOLD_SECONDS, folder, records
from scripts.infrastructure.host_session_refresh_cases import OLD_SHA, build
from scripts.infrastructure.host_session_stand_in import StandInHost

READ_ONLY = 0o500
WRITABLE = 0o700


def host_off_requested(host: StandInHost) -> bool:
    return any(entry["operation"] == "set-desired-capacity" and entry["options"]["--desired-capacity"] == ["0"] for entry in host.log)


def run(session: Session, directory: Path) -> BaseException | None:
    """The exception the session raised (a session error, a signal or an output error), or None."""
    try:
        session.dev_session(HOLD_SECONDS, 0, directory)
    except (HostSessionError, TerminationRequested, KeyboardInterrupt, OSError) as error:
        return error
    return None


def two_signals(second: int, where: str) -> dict[str, bool]:
    """A SIGTERM while the status after the start is read, then ``second`` at the cleanup report (``where`` is ``report``) or while the closing status is
    read (``where`` is ``status``)."""
    host, session, lines = build(checkout=OLD_SHA)
    directory = folder()
    calls, fired = {"status": 0}, {"report": False}
    original_status, original_report = session.status, session.report

    def status() -> dict[str, Any]:
        calls["status"] += 1
        if calls["status"] == 1:
            signal.raise_signal(signal.SIGTERM)
        if calls["status"] == 2 and where == "status":
            signal.raise_signal(second)
        return original_status()

    def report(line: str) -> None:
        if where == "report" and line.startswith("session_failed") and not fired["report"]:
            fired["report"] = True
            signal.raise_signal(second)
        original_report(line)

    session.report = report
    restore = install_termination_handler()
    try:
        with mock.patch.object(session, "status", status):
            raised = run(session, directory)
    finally:
        restore()
    record = (records(directory) or [{}])[0]
    return {
        "the first signal reaches the caller": isinstance(raised, TerminationRequested),
        "the second signal was delivered while the cleanup ran": fired["report"] or calls["status"] >= 2,
        "the second signal was held and reported": any(line.startswith("signal_deferred") for line in lines),
        "the host is off and verified": host.desired == 0 and any(line.startswith("host_off_verified") for line in lines),
        "the record names the cleanup": record.get("cleanup") == "host_off_verified",
        "the record is closed with the status and an end time": str(record.get("ended_at", "")).endswith("Z")
        and record.get("after_cleanup", {}).get("desired_capacity") == 0,
    }


def a_second_signal_cannot_cut_the_cleanup_or_the_record_short() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, second, where in (
        ("a second SIGTERM at the cleanup report", signal.SIGTERM, "report"),
        ("a SIGINT at the cleanup report", signal.SIGINT, "report"),
        ("a SIGINT while the closing status is read", signal.SIGINT, "status"),
        ("a SIGTERM while the closing status is read", signal.SIGTERM, "status"),
    ):
        checks |= {f"{label}: {name}": value for name, value in two_signals(second, where).items()}
    return checks


def broken_at(index: int, onward: bool, faults: set[str] | None = None) -> tuple[StandInHost, Session, BaseException | None, dict[str, Any]]:
    """A session whose output raises BrokenPipeError at report number ``index`` (and at every later one when ``onward``)."""
    host, session, _ = build(faults, checkout=OLD_SHA)
    directory, seen = folder(), {"count": 0}

    def report(_line: str) -> None:
        number = seen["count"]
        seen["count"] += 1
        if number == index or (onward and number > index):
            raise BrokenPipeError(32, "Broken pipe")

    session.report = report
    raised = run(session, directory)
    return host, session, raised, (records(directory) or [{}])[0]


def output_points(faults: set[str] | None = None) -> int:
    """How many lines a session reports, counted on a session whose output works."""
    host, session, _ = build(faults, checkout=OLD_SHA)
    seen = {"count": 0}

    def report(_line: str) -> None:
        seen["count"] += 1

    session.report = report
    run(session, folder())
    del host
    return seen["count"]


def a_closed_output_at_any_point_never_skips_the_stop() -> dict[str, bool]:
    total = output_points()
    leaving_host_on: list[str] = []
    raising: list[str] = []
    unrecorded: list[str] = []
    for index in range(total):
        for onward in (False, True):
            host, _, raised, record = broken_at(index, onward)
            label = f"{index}{'+' if onward else ''}"
            if host.desired != 0:
                leaving_host_on.append(label)
            if raised is not None:
                raising.append(label)
            if record.get("stop") != "stopped" or record.get("output_errors", 0) < 1:
                unrecorded.append(label)
    return {
        "a normal session has output points to test": total > 20,
        "no closed output leaves the host on": not leaving_host_on,
        "no closed output ends the session with an error": not raising,
        "every closed output is recorded and the stop is recorded": not unrecorded,
    }


def a_closed_output_during_a_failed_start_still_stops_the_host() -> dict[str, bool]:
    total = output_points({"never_stable"})
    leaving_host_on: list[str] = []
    replaced: list[str] = []
    unrecorded: list[str] = []
    for index in range(total):
        for onward in (False, True):
            host, _, raised, record = broken_at(index, onward, {"never_stable"})
            label = f"{index}{'+' if onward else ''}"
            if host.desired != 0:
                leaving_host_on.append(label)
            if not (isinstance(raised, HostSessionError) and "stable" in str(raised)):
                replaced.append(label)
            if record.get("start") != "failed" or record.get("start_cleanup") != "host_off_verified":
                unrecorded.append(label)
    return {
        "a failed start has output points to test": total > 10,
        "no closed output leaves the host on": not leaving_host_on,
        "the start error is still the error the caller sees": not replaced,
        "the record names the failed start and its cleanup": not unrecorded,
    }


def folder_locked_at(session: Session, directory: Path, name: str, closed_from: str | None = None) -> None:
    """Make ``directory`` read-only when the report line ``name`` appears and, with ``closed_from``, close the output from the line of that name on."""
    original, state = session.report, {"closed": False}

    def report(line: str) -> None:
        if line.startswith(name):
            directory.chmod(READ_ONLY)
        if closed_from is not None and line.startswith(closed_from):
            state["closed"] = True
        if state["closed"]:
            raise BrokenPipeError(32, "Broken pipe")
        original(line)

    session.report = report


def an_unwritable_record_never_hides_the_failure_or_skips_the_cleanup() -> dict[str, bool]:
    host, session, lines = build({"never_stable", "deny_host_off"}, checkout=OLD_SHA)
    directory = folder()
    folder_locked_at(session, directory, "session_record", closed_from="session_failed")
    raised = run(session, directory)
    directory.chmod(WRITABLE)
    notes = " ".join(getattr(raised, "__notes__", []))
    failed_start = {
        "the start error is the error raised": isinstance(raised, HostSessionError) and "stable" in str(raised) and "AccessDenied" not in str(raised),
        "the capacity-zero request was sent": host_off_requested(host),
        "the denied cleanup is carried on the raised error": "host_off_failed" in notes and "AccessDenied" in notes,
        "the record failure is carried on the raised error": "record" in notes and "not written" in notes,
        "the host is still on and the error says so": host.desired == 1 and "stop" in notes,
    }
    host, session, _ = build(checkout=OLD_SHA)
    directory = folder()
    folder_locked_at(session, directory, "hold_ended")
    raised = run(session, directory)
    directory.chmod(WRITABLE)
    unsaved_end = {
        "an unsaved final record fails the session by name": isinstance(raised, HostSessionError) and "record" in str(raised),
        "an unsaved final record leaves the host off": host.desired == 0,
    }
    host, session, lines = build(checkout=OLD_SHA)
    directory = folder()
    folder_locked_at(session, directory, "session_ready")
    status_calls = {"count": 0}
    original_status = session.status

    def status() -> dict[str, Any]:
        status_calls["count"] += 1
        if status_calls["count"] == 2:
            raise KeyboardInterrupt
        return original_status()

    with mock.patch.object(session, "status", status):
        raised = run(session, directory)
    directory.chmod(WRITABLE)
    notes = " ".join(getattr(raised, "__notes__", []))
    interrupted = {
        "an interrupt with an unwritable record reaches the caller": isinstance(raised, KeyboardInterrupt),
        "the host is off": host.desired == 0,
        "the unwritable record is carried on the raised error": "not written" in notes,
    }
    return (
        {f"a failed start, a denied cleanup, a closed output and an unwritable record: {name}": value for name, value in failed_start.items()}
        | {f"an unwritable final record: {name}": value for name, value in unsaved_end.items()}
        | {f"an interrupt after the start and an unwritable record: {name}": value for name, value in interrupted.items()}
    )


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "a_second_signal_cannot_cut_the_cleanup_or_the_record_short": a_second_signal_cannot_cut_the_cleanup_or_the_record_short,
    "a_closed_output_at_any_point_never_skips_the_stop": a_closed_output_at_any_point_never_skips_the_stop,
    "a_closed_output_during_a_failed_start_still_stops_the_host": a_closed_output_during_a_failed_start_still_stops_the_host,
    "an_unwritable_record_never_hides_the_failure_or_skips_the_cleanup": an_unwritable_record_never_hides_the_failure_or_skips_the_cleanup,
}
