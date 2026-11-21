"""Host session cases for ``session``: the development command that starts the host, holds it for a bounded time, stops it and records its status.

Every case builds a stand-in host, runs the real ``Session`` and checks the calls the stand-in saw, the lines the session reported and the record file it wrote.
The command line cases run ``python -m scripts.infrastructure.host_session session`` in a child process under the entry stand-in.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infrastructure.host_session import HostSessionError, Session, TerminationRequested
from scripts.infrastructure.host_session_entry_cases import clean, entry, host_is_off
from scripts.infrastructure.host_session_refresh_cases import JOB_PREFIX, OLD_SHA, SERIAL, build, failure
from scripts.infrastructure.host_session_stand_in import HOST_ADDRESS, StandInHost

HOLD_SECONDS = 120
FORBIDDEN_TEXT = ("arn:aws", "111111111111", "i-stand-in")


def folder() -> Path:
    return Path(tempfile.mkdtemp(prefix="host_session_dev_case_"))


def records(directory: Path) -> list[dict[str, Any]]:
    """Every session record of the folder, oldest first."""
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(directory.glob("session_*.json"))]


def ready(lines: list[str]) -> bool:
    return any(line.startswith("session_ready") for line in lines)


def desired_capacities(host: StandInHost) -> list[str]:
    return [entry["options"]["--desired-capacity"][0] for entry in host.log if entry["operation"] == "set-desired-capacity"]


def session_starts_holds_and_stops_the_host_and_records_its_status() -> dict[str, bool]:
    host, session, lines = build(checkout=OLD_SHA)
    directory = folder()
    session.dev_session(HOLD_SECONDS, 0, directory)
    found = records(directory)
    record = found[0] if found else {}
    text = "\n".join(lines) + "\n".join(path.read_text(encoding="utf-8") for path in directory.glob("session_*.json"))
    after_start, after_stop = record.get("after_start", {}), record.get("after_stop", {})
    ready_at = next((index for index, line in enumerate(lines) if line.startswith("session_ready")), -1)
    return {
        "the host is turned on and then off": desired_capacities(host) == ["1", "0"] and host.desired == 0,
        "the host is verified off": any(line.startswith("host_off_verified") for line in lines),
        "the start comes before the ready line and the stop after it": 0
        < ready_at
        < next(index for index, line in enumerate(lines) if line.startswith("job_tasks")),
        "the hold lasts the asked time": host.now >= HOLD_SECONDS,
        "the ready line says the end of the hold tries the stop and a running job keeps the host on": any(
            line.startswith("session_ready") and "tries to stop" in line and "stays on" in line and "stops by itself" not in line for line in lines
        ),
        "the ready line names the host address": any(line.startswith("host_address") and HOST_ADDRESS in line for line in lines),
        "one record is written": len(found) == 1,
        "the record names the phases that ran": record.get("start") == "started" and record.get("hold_ended") == "time" and record.get("stop") == "stopped",
        "the status after the start shows the host on and every service up": after_start.get("desired_capacity") == 1
        and bool(after_start.get("services"))
        and all(item["running"] == 1 for item in after_start["services"].values()),
        "the status after the stop shows the host off": after_stop.get("desired_capacity") == 0 and after_stop.get("active_container_instances") == 0,
        "the record keeps the services state serial": record.get("services_state_serial") == SERIAL,
        "the record names the host address and the environment": record.get("host_address") == HOST_ADDRESS and record.get("environment") == "dev",
        "the record has start and end times in UTC": all(str(record.get(name, "")).endswith("Z") for name in ("started_at", "ended_at")),
        "no output or record names an ARN, account ID or instance ID": not any(word in text for word in FORBIDDEN_TEXT),
    }


def session_refuses_in_production_before_any_call() -> dict[str, bool]:
    host, session, lines = build()
    session.environment = "prod"
    directory = folder()
    message = failure(lambda: session.dev_session(HOLD_SECONDS, 0, directory))
    return {
        "the refusal says the production host is always on": message is not None and "always on" in message and "production" in message,
        "no AWS call is made": not host.log,
        "no record is written": not records(directory),
        "nothing is reported as started": not any(line.startswith(("host_on", "host_prepare")) for line in lines),
    }


def a_start_that_fails_stops_the_host_and_records_the_failure() -> dict[str, bool]:
    host, session, lines = build({"never_stable"})
    directory = folder()
    message = failure(lambda: session.dev_session(HOLD_SECONDS, 0, directory))
    found = records(directory)
    record = found[0] if found else {}
    return {
        "the start failure is raised": message is not None,
        "the host is off again": host.desired == 0 and any(line.startswith("host_off_verified") for line in lines),
        "no hold ran": not ready(lines),
        "one record names the failed start and the reason": len(found) == 1 and record.get("start") == "failed" and bool(record.get("start_error")),
        "the record has no hold and no stop entry": "hold_ended" not in record and "stop" not in record,
    }


def jobs_running_at_the_end_refuse_the_stop_and_the_record_says_so() -> dict[str, bool]:
    host, session, lines = build(checkout=OLD_SHA)
    directory = folder()
    started = {"task": ""}
    sleep = session.clock.sleep

    def sleeping(seconds: float) -> None:
        if ready(lines) and not started["task"]:
            started["task"] = host.add_task(f"{JOB_PREFIX}read_silver")
        sleep(seconds)

    session.clock = replace(session.clock, sleep=sleeping)
    message = failure(lambda: session.dev_session(HOLD_SECONDS, 0, directory))
    found = records(directory)
    record = found[0] if found else {}
    return {
        "the stop is refused with the job task named": message is not None and "stop refused" in message and f"{JOB_PREFIX}read_silver" in message,
        "the host stays on and says so": host.desired == 1 and any(line.startswith("session_host_left_on") for line in lines),
        "the record names the refusal": record.get("stop") == "refused" and "stop refused" in str(record.get("stop_error")),
        "the status after the refusal shows the host on": record.get("after_stop", {}).get("desired_capacity") == 1,
    }


def the_stop_waits_for_a_job_task_when_asked() -> dict[str, bool]:
    host, session, lines = build(checkout=OLD_SHA)
    directory = folder()
    sleep = session.clock.sleep
    added = {"arn": ""}

    def sleeping(seconds: float) -> None:
        if ready(lines) and not added["arn"]:
            added["arn"] = host.add_task(f"{JOB_PREFIX}read_silver")
            host.tasks[added["arn"]]["stops_at"] = host.now + HOLD_SECONDS + 100
        sleep(seconds)

    session.clock = replace(session.clock, sleep=sleeping)
    session.dev_session(HOLD_SECONDS, 600, directory)
    record = records(directory)[0]
    return {
        "the host is off after the task ended": host.desired == 0,
        "the wait is reported": any(line.startswith("stop_waiting") for line in lines),
        "the record says stopped": record.get("stop") == "stopped",
    }


def an_interrupt_during_the_hold_ends_the_hold_and_stops_the_host() -> dict[str, bool]:
    host, session, lines = build(checkout=OLD_SHA)
    directory = folder()
    sleep, holds = session.clock.sleep, {"count": 0}

    def sleeping(seconds: float) -> None:
        if ready(lines):
            holds["count"] += 1
            if holds["count"] == 2:
                raise KeyboardInterrupt
        sleep(seconds)

    session.clock = replace(session.clock, sleep=sleeping)
    session.dev_session(3600, 0, directory)
    record = records(directory)[0]
    return {
        "the hold ended before its time": host.now < 3600,
        "the host is off and verified": host.desired == 0 and any(line.startswith("host_off_verified") for line in lines),
        "the record says the hold was interrupted": record.get("hold_ended") == "interrupted" and record.get("stop") == "stopped",
    }


def a_record_that_cannot_be_written_refuses_before_the_host_turns_on() -> dict[str, bool]:
    host, session, _ = build()
    blocked = folder() / "not_a_folder"
    blocked.write_text("a file where the folder should be\n", encoding="utf-8")
    message = failure(lambda: session.dev_session(HOLD_SECONDS, 0, blocked))
    return {
        "the refusal names the record": message is not None and "record" in message,
        "no AWS call is made": not host.log,
    }


def the_hold_is_bounded() -> dict[str, bool]:
    host, session, _ = build()
    checks: dict[str, bool] = {}
    for label, seconds in (("zero", 0), ("negative", -5), ("over the limit", 10**6)):
        message = failure(partial(session.dev_session, seconds, 0, folder()))
        checks[f"a hold of {label} seconds is refused before any call"] = message is not None and "hold" in message and not host.log
    return checks


def session_command_line() -> dict[str, bool]:
    directory = folder()
    prod, prod_state = entry("session", "--hold-seconds", "60", "--record-folder", str(directory), stage="prod", host_on=True)
    low, low_state = entry("session", "--hold-seconds", "0", "--record-folder", str(directory), host_on=True)
    unset, _ = entry("session", "--record-folder", str(directory))
    refused, refused_state = entry("session", "--hold-seconds", "60", "--record-folder", str(directory), host_on=False)
    found = records(directory)
    return {
        "production exits 1 with a named refusal and no traceback": prod.returncode == 1 and "session failed: HostSessionError" in prod.stderr and clean(prod),
        "production changes nothing and the host stays on": prod_state.get("desired") == 1 and not prod_state.get("writes"),
        "a hold of zero exits 2 with a usage line": low.returncode == 2 and "hold" in low.stderr and clean(low),
        "a hold of zero changes nothing": low_state.get("desired") == 1 and not low_state.get("writes"),
        "no hold given uses the default and goes on to the start": unset.returncode == 1 and "start" in unset.stderr and clean(unset),
        "a start the stack cannot do exits 1 with a named line and no traceback": refused.returncode == 1
        and "session failed:" in refused.stderr
        and clean(refused),
        "that start leaves the host off and writes one record that names the failure": host_is_off(refused_state)
        and len([item for item in found if item.get("start") == "failed"]) >= 1,
    }


def raising_once(session: Session, error: BaseException) -> Any:
    """A patch that makes the first ``status`` call raise ``error``; every later call works as before."""
    original, calls = session.status, {"count": 0}

    def status() -> dict[str, Any]:
        calls["count"] += 1
        if calls["count"] == 1:
            raise error
        return original()

    return mock.patch.object(session, "status", status)


def an_interrupt_or_signal_while_the_start_status_is_read_stops_the_host() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, error, kind in (("Ctrl+C", KeyboardInterrupt(), KeyboardInterrupt), ("SIGTERM", TerminationRequested("SIGTERM"), TerminationRequested)):
        host, session, lines = build(checkout=OLD_SHA)
        directory = folder()
        raised: BaseException | None = None
        with raising_once(session, error):
            try:
                session.dev_session(HOLD_SECONDS, 0, directory)
            except (HostSessionError, KeyboardInterrupt, TerminationRequested) as caught:
                raised = caught
        record = (records(directory) or [{}])[0]
        checks |= {
            f"{label}: the interruption reaches the caller": isinstance(raised, kind),
            f"{label}: the host is off and verified": host.desired == 0 and any(line.startswith("host_off_verified") for line in lines),
            f"{label}: the record names the started phase and the cleanup": record.get("start") == "started" and record.get("cleanup") == "host_off_verified",
            f"{label}: the record has an end time and the status after the cleanup": str(record.get("ended_at", "")).endswith("Z")
            and record.get("after_cleanup", {}).get("desired_capacity") == 0,
            f"{label}: no hold ran": not any(line.startswith("hold_ended") for line in lines),
        }
    return checks


def a_failed_start_with_a_failed_cleanup_records_both() -> dict[str, bool]:
    host, session, lines = build({"never_stable", "deny_host_off"}, checkout=OLD_SHA)
    directory = folder()
    message = failure(lambda: session.dev_session(HOLD_SECONDS, 0, directory))
    record = (records(directory) or [{}])[0]
    return {
        "the start error is still the one raised": message is not None and "stable" in message and "AccessDenied" not in message,
        "the host is still on": host.desired == 1,
        "the record keeps the start error": record.get("start") == "failed" and "stable" in str(record.get("start_error")),
        "the record names the failed cleanup and its reason": str(record.get("start_cleanup", "")).startswith("host_off_failed")
        and "AccessDenied" in str(record.get("start_cleanup")),
        "the record holds the status after the cleanup, with the host on": record.get("after_cleanup", {}).get("desired_capacity") == 1,
        "the output says the host may still be on": any(line.startswith("session_host_left_on") for line in lines),
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "session_starts_holds_and_stops_the_host_and_records_its_status": session_starts_holds_and_stops_the_host_and_records_its_status,
    "session_refuses_in_production_before_any_call": session_refuses_in_production_before_any_call,
    "a_start_that_fails_stops_the_host_and_records_the_failure": a_start_that_fails_stops_the_host_and_records_the_failure,
    "jobs_running_at_the_end_refuse_the_stop_and_the_record_says_so": jobs_running_at_the_end_refuse_the_stop_and_the_record_says_so,
    "the_stop_waits_for_a_job_task_when_asked": the_stop_waits_for_a_job_task_when_asked,
    "an_interrupt_during_the_hold_ends_the_hold_and_stops_the_host": an_interrupt_during_the_hold_ends_the_hold_and_stops_the_host,
    "a_record_that_cannot_be_written_refuses_before_the_host_turns_on": a_record_that_cannot_be_written_refuses_before_the_host_turns_on,
    "the_hold_is_bounded": the_hold_is_bounded,
    "session_command_line": session_command_line,
    "an_interrupt_or_signal_while_the_start_status_is_read_stops_the_host": an_interrupt_or_signal_while_the_start_status_is_read_stops_the_host,
    "a_failed_start_with_a_failed_cleanup_records_both": a_failed_start_with_a_failed_cleanup_records_both,
}
