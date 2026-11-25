"""Host session cases for the final dump of the databases that ``stop`` takes before the catalog and the host go down.

The backup service dumps every database at an interval and the restore reads the newest completed set, so what happened after the last interval dump is lost at
the next start. ``stop`` therefore scales the backup service to zero, waits until its task has stopped and runs the final backup one-shot (the same dump and
upload steps, one pass each) after the job task check and before the catalog drain and the host-off request. The launch names the revision of the final backup
task definition whose embedded scripts were checked against this checkout, and carries a task ID that the dump and upload steps check. A failed or late final
dump is reported by name and never holds the host on; an interruption or SIGTERM still ends in a verified host-off. A stack without the one-shot (the backup
is off) stops as before. The cleanup of a failed ``start`` takes the same dump once the services had become stable, and the next start sets the backup
service back to one task.
"""

from __future__ import annotations

import importlib
import json
import re
import tempfile
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infrastructure import host_session_refresh_cases as refresh
from scripts.infrastructure.host_session_stand_in import BACKUP_FAMILY, BACKUP_SERVICE, CHECKED_REVISION, OPENMETADATA_SERVICE, StandInError, StandInHost, parse
from scripts.infrastructure.host_session_stop_cases import READER, host_is_on, on_wall_time
from scripts.process import CompletedProcess, TimeoutExpired

__all__ = ["CASES", "CASE_ERRORS"]
CASE_ERRORS = refresh.CASE_ERRORS


def build(faults: set[str] | None = None, backup: bool = True, started: bool = True, openmetadata: bool = False) -> tuple[StandInHost, Any, list[str]]:
    """A stand-in host and a session with the backup service (a task record each) and the final backup one-shot in the state's names when ``backup``; the
    session is started when ``started``, and its report lines start empty again."""
    cases = importlib.import_module("scripts.infrastructure.host_session_cases")
    host, session, lines = cases.build(faults, openmetadata=openmetadata)
    if backup:
        host.families["postgres_final_backup"] = BACKUP_FAMILY
        host.services[BACKUP_SERVICE] = {"desired": 1, "forced_at": None, "state": "IN_PROGRESS", "running": 0, "stop_at": None}
        host.service_tasks_for = {BACKUP_SERVICE}
        session.names = replace(
            session.names,
            services={**session.names.services, "postgres_backup": BACKUP_SERVICE},
            one_shot={**session.names.one_shot, "postgres_final_backup": BACKUP_FAMILY},
        )
    session.job_prefix = refresh.JOB_PREFIX
    if started:
        session.start()
        on_wall_time(host, session)
        lines.clear()
    return host, session, lines


def final_runs(calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The calls that launch the final backup one-shot."""
    return [
        entry
        for entry in calls
        if entry["operation"] == "run-task" and entry["options"]["--task-definition"][0].rsplit("/", 1)[-1].split(":")[0] == BACKUP_FAMILY
    ]


def step_names(lines: list[str]) -> list[str]:
    return [line.split(":", 1)[0] for line in lines]


def final_dump_follows_the_job_tasks_and_precedes_the_host_stop() -> dict[str, bool]:
    host, session, lines = build()
    arn = host.add_task(READER)
    count, begin = len(host.log), host.now
    host.tasks[arn]["stops_at"] = begin + 100
    session.stop(wait_seconds=600)
    calls = refresh.after(host, count)
    runs = final_runs(calls)
    snapshot = host.final_backup_runs[0] if host.final_backup_runs else {}
    backup = snapshot.get("services", {}).get(BACKUP_SERVICE, {})
    scale_down = [entry for entry in calls if entry["operation"] == "update-service" and entry["options"].get("--service") == [BACKUP_SERVICE]]
    host_off = next((entry for entry in calls if entry["operation"] == "set-desired-capacity"), {"at": -1})
    return {
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
        "exactly one final backup ran": len(runs) == 1 and len(host.final_backup_runs) == 1,
        "it started after the job task had ended": bool(runs) and runs[0]["at"] >= begin + 100 and snapshot.get("running_job_families") == [],
        "the backup service was already at zero with its task stopped, so the one-shot had the backup folder to itself": backup.get("desired") == 0
        and backup.get("running") == 0
        and set(backup.get("tasks", ["RUNNING"])) <= {"STOPPED"},
        "the host was still up when it ran": snapshot.get("instance_active") is True,
        "it ended before the host-off request": bool(runs) and runs[0]["at"] < host_off["at"],
        "the backup service was scaled down before the one-shot launched": len(scale_down) == 1
        and scale_down[0]["options"].get("--desired-count") == ["0"]
        and scale_down[0]["at"] <= runs[0]["at"],
        "the steps are reported in order": step_names(lines).index("final_backup_service_scaled_down")
        < step_names(lines).index("final_backup_taken")
        < step_names(lines).index("host_off_verified"),
        "the outcome is taken": session.final_backup_outcome == "taken",
    }


def the_final_launch_names_the_task_definition_revision_that_was_checked() -> dict[str, bool]:
    host, session, lines = build()
    session.stop()
    runs = final_runs(host.log)
    described = [entry for entry in host.log if entry["operation"] == "describe-task-definition"]
    named = runs[0]["options"]["--task-definition"][0] if runs else ""
    tasks = [task for task in host.tasks.values() if task["key"] == "postgres_final_backup"]
    scale_down = next(
        (
            entry
            for entry in host.log
            if entry["operation"] == "update-service"
            and entry["options"].get("--service") == [BACKUP_SERVICE]
            and entry["options"].get("--desired-count") == ["0"]
        ),
        {},
    )
    return {
        "the definition was read once, by family, before the backup service was touched": len(described) == 1
        and described[0]["options"]["--task-definition"] == [BACKUP_FAMILY]
        and described[0]["at"] <= scale_down.get("at", -1),
        "the launch names the revision that was checked (7), not the family that ECS resolves to the latest (8)": named.endswith(f":{CHECKED_REVISION}")
        and "task-definition/" in named,
        "the task that ran used the checked revision": len(tasks) == 1 and tasks[0]["definition"] == named,
        "the dump is taken": session.final_backup_outcome == "taken" and "final_backup_taken" in step_names(lines),
    }


def older_final_backup_scripts_take_no_dump_and_the_host_still_stops() -> dict[str, bool]:
    host, session, lines = build({"old_final_scripts"})
    count = len(host.log)
    message = refresh.failure(session.stop)
    calls = refresh.after(host, count)
    return {
        "stop does not raise": message is None,
        "the dump is reported as not taken and names the apply": (session.final_backup_outcome or "").startswith("failed:")
        and "apply the services stack" in (session.final_backup_outcome or "")
        and "final_backup_failed" in step_names(lines),
        "nothing was launched and the backup service was not scaled down": not final_runs(calls)
        and not any(entry["operation"] == "update-service" and entry["options"].get("--service") == [BACKUP_SERVICE] for entry in calls),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def the_final_backup_launch_carries_a_new_task_id_in_both_containers() -> dict[str, bool]:
    containers = refresh.module().FINAL_BACKUP_CONTAINERS
    found: list[set[str]] = []
    for _ in range(2):
        host, session, _lines = build()
        session.stop()
        runs = final_runs(host.log)
        launched = json.loads(runs[0]["options"]["--overrides"][0])["containerOverrides"] if runs and "--overrides" in runs[0]["options"] else []
        by_name = {entry["name"]: {item["name"]: item["value"] for item in entry.get("environment", [])} for entry in launched}
        found.append({values.get("PG_BACKUP_TASK_ID", "") for values in by_name.values()} if set(by_name) == set(containers) else set())
    return {
        "each launch names the dump and the upload container with one ID for both": all(len(item) == 1 for item in found),
        "the ID has 32 hexadecimal characters": all(re.fullmatch(r"[0-9a-f]{32}", value) for item in found for value in item),
        "another stop launches with another ID": len(found[0] | found[1]) == 2,
    }


def a_stopped_reply_for_only_one_container_is_not_a_taken_dump() -> dict[str, bool]:
    host, session, lines = build({"final_backup_one_container"})
    message = refresh.failure(session.stop)
    return {
        "stop does not raise": message is None,
        "the dump is not reported as taken": "final_backup_taken" not in step_names(lines) and session.final_backup_outcome != "taken",
        "it is reported as failed and names the container that did not report": (session.final_backup_outcome or "").startswith("failed:")
        and "postgres_backup_upload" in (session.final_backup_outcome or ""),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def a_final_backup_task_of_an_earlier_stop_is_awaited_then_a_new_one_runs() -> dict[str, bool]:
    host, session, _lines = build()
    prior = host.add_task(BACKUP_FAMILY)
    host.tasks[prior]["stops_at"] = host.now + 200
    ends = host.tasks[prior]["stops_at"]
    session.stop()
    runs = final_runs(host.log)
    return {
        "a new final backup was launched, after the earlier task had stopped": len(runs) == 1 and runs[0]["at"] >= ends,
        "the earlier task is not the one reported as taken": session.final_backup_outcome == "taken"
        and session.last_task.get("postgres_final_backup") != prior,
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def a_failed_final_dump_is_reported_by_name_and_the_host_still_stops() -> dict[str, bool]:
    host, session, lines = build({"exit_code:postgres_final_backup=1"})
    message = refresh.failure(session.stop)
    shown = [line for line in lines if line.startswith("final_backup_failed")]
    return {
        "stop does not raise: a failed stop would leave a billed host on, so the failure is reported": message is None,
        "the report names the one-shot and its exit code": len(shown) == 1 and "postgres_final_backup" in shown[0] and "exit codes" in shown[0],
        "the report says the newest usable completed set stays the restore point": len(shown) == 1
        and "newest usable completed set" in shown[0]
        and "interval dump" not in shown[0],
        "no success is reported": "final_backup_taken" not in step_names(lines),
        "the outcome records the failure": (session.final_backup_outcome or "").startswith("failed:"),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None and "host_off_verified" in step_names(lines),
    }


def a_final_dump_past_its_deadline_fails_visibly_and_the_host_stops() -> dict[str, bool]:
    host, session, lines = build({"task_seconds:postgres_final_backup=100000"})
    begin = host.now
    message = refresh.failure(session.stop)
    module = refresh.module()
    shown = [line for line in lines if line.startswith("final_backup_failed")]
    return {
        "stop does not raise": message is None,
        "the report names the wait and its limit": len(shown) == 1 and "postgres_final_backup" in shown[0] and f"{module.FINAL_BACKUP_SECONDS} s" in shown[0],
        "the wait stayed inside the deadline plus the service stop and the host-off": host.now - begin
        <= module.FINAL_BACKUP_SECONDS + module.FINAL_BACKUP_HOLD_SECONDS + 300,
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def an_interrupt_during_the_final_dump_still_turns_the_host_off() -> dict[str, bool]:
    module = refresh.module()
    restore = module.install_termination_handler()
    checks: dict[str, bool] = {}
    try:
        for name, fault, expected in (("SIGINT", "interrupt_run_task", "KeyboardInterrupt"), ("SIGTERM", "signal_run_task", "TerminationRequested")):
            host, session, lines = build(openmetadata=True)
            host.faults.add(fault)
            count = len(host.log)
            raised = None
            try:
                session.stop()
            except (KeyboardInterrupt, module.TerminationRequested) as error:
                raised = type(error).__name__
            calls = refresh.after(host, count)
            drain = next(
                (entry for entry in calls if entry["operation"] == "update-service" and entry["options"].get("--service") == [OPENMETADATA_SERVICE]), None
            )
            host_off = next((entry for entry in calls if entry["operation"] == "set-desired-capacity"), {"at": -1})
            checks[f"a {name} during the final dump propagates"] = raised == expected
            checks[f"a {name} during the final dump is reported"] = "final_backup_interrupted" in step_names(lines)
            checks[f"a {name} during the final dump still turns the host off and verifies it"] = host.desired == 0 and host.instance_up_at is None
            checks[f"a {name} during the final dump leaves the outcome interrupted"] = session.final_backup_outcome == "interrupted"
            checks[f"a {name} during the final dump still drains the catalog service before the host-off request"] = (
                drain is not None
                and drain["options"].get("--desired-count") == ["0"]
                and drain["at"] <= host_off["at"]
                and host.services[OPENMETADATA_SERVICE]["running"] == 0
                and "openmetadata_stopped" in step_names(lines)
            )
    finally:
        restore()
    return checks


def a_backup_service_that_will_not_stop_blocks_the_dump_and_not_the_host() -> dict[str, bool]:
    host, session, lines = build({"slow_container_stop:5000"})
    message = refresh.failure(session.stop)
    shown = [line for line in lines if line.startswith("final_backup_failed")]
    return {
        "stop does not raise": message is None,
        "the failure names the backup service stop": len(shown) == 1 and "backup service stop" in shown[0],
        "no one-shot ran beside the still running service": not final_runs(host.log),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def a_stack_with_the_backup_off_stops_without_a_dump() -> dict[str, bool]:
    host, session, lines = build(backup=False)
    count = len(host.log)
    session.stop()
    calls = refresh.after(host, count)
    return {
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
        "no final backup one-shot ran": not final_runs(calls) and "run-task" not in [entry["operation"] for entry in calls],
        "no service was scaled for a backup": not any(entry["operation"] == "update-service" for entry in calls),
        "the skip is reported by name": "final_backup_skipped" in step_names(lines) and "final_backup_taken" not in step_names(lines),
        "the outcome says skipped": (session.final_backup_outcome or "").startswith("skipped"),
    }


def a_postgres_setup_that_is_not_complete_takes_no_dump() -> dict[str, bool]:
    host, session, lines = build()
    session.setup_state = "postgres_databases RUNNING"
    count = len(host.log)
    with mock.patch.object(session, "postgres_setup", return_value=False):
        session.stop()
    calls = refresh.after(host, count)
    return {
        "no final backup one-shot ran and the backup service was left alone": not final_runs(calls)
        and not any(entry["operation"] == "update-service" and entry["options"].get("--service") == [BACKUP_SERVICE] for entry in calls),
        "the skip names the unfinished setup": any(line.startswith("final_backup_skipped") and "postgres_databases RUNNING" in line for line in lines),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def the_next_start_sets_the_backup_service_back_to_one_task() -> dict[str, bool]:
    host, session, _ = build()
    session.stop()
    after_stop = dict(host.services[BACKUP_SERVICE])
    session.start()
    return {
        "the stop left the backup service at zero": after_stop["desired"] == 0,
        "the next start sets its count back to one": host.services[BACKUP_SERVICE]["desired"] == 1,
        "the backup service runs again with a task": host.services[BACKUP_SERVICE]["running"] == 1
        and host.service_snapshot()[BACKUP_SERVICE]["tasks"][-1] == "RUNNING",
    }


def a_failed_start_with_stable_services_takes_the_final_dump_first() -> dict[str, bool]:
    host, session, lines = build({"reload_fails:m2"}, started=False)
    message = refresh.failure(lambda: session.start(code=(refresh.CODE_RUN, refresh.CODE_SHA)))
    names = step_names(lines)
    runs = final_runs(host.log)
    host_off = next(
        (entry for entry in host.log if entry["operation"] == "set-desired-capacity" and entry["options"].get("--desired-capacity") == ["0"]), {"at": -1}
    )
    return {
        "start still fails with its own error": message is not None and "reload" in message,
        "the final dump ran once, before the host-off request": len(runs) == 1 and runs[0]["at"] <= host_off["at"],
        "it is reported and the host is off and verified": "final_backup_taken" in names and host.desired == 0 and "host_off_verified" in names,
    }


def a_failed_start_before_stable_services_takes_no_dump() -> dict[str, bool]:
    host, session, lines = build({"exit_code:host_prepare=1"}, started=False)
    message = refresh.failure(session.start)
    return {
        "start fails": message is not None,
        "no final dump ran, because the databases were never restored": not final_runs(host.log) and "final_backup_taken" not in step_names(lines),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def a_failed_final_dump_in_the_cleanup_keeps_the_first_error() -> dict[str, bool]:
    host, session, lines = build({"reload_fails:m2", "exit_code:postgres_final_backup=1"}, started=False)
    message = refresh.failure(lambda: session.start(code=(refresh.CODE_RUN, refresh.CODE_SHA)))
    return {
        "start raises the reload error, not the dump failure": message is not None and "reload" in message and "postgres_final_backup" not in message,
        "the dump failure is reported by name": any(line.startswith("final_backup_failed") and "postgres_final_backup" in line for line in lines),
        "the cleanup report names the newest usable completed set as the restore point": any(
            line.startswith("final_backup_failed") and "newest usable completed set" in line and "interval dump" not in line for line in lines
        ),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None and session.cleanup_outcome == "host_off_verified",
    }


def a_start_that_keeps_the_host_on_takes_no_dump() -> dict[str, bool]:
    host, session, lines = build({"reload_fails:m2"}, started=False)
    refresh.failure(lambda: session.start(keep_on=True, code=(refresh.CODE_RUN, refresh.CODE_SHA)))
    return {
        "no final dump ran": not final_runs(host.log),
        "the host stays on": host_is_on(host),
    }


def a_missing_script_in_the_cleanup_keeps_the_first_error_and_stops_the_host() -> dict[str, bool]:
    host, session, lines = build({"reload_fails:m2"}, started=False)
    module = refresh.module()
    operation = ("ecs", "describe-task-definition")
    reply = host.handlers[operation]({"--task-definition": [BACKUP_FAMILY]})
    host.handlers[operation] = lambda options: reply
    missing = {**module.FINAL_BACKUP_SCRIPTS, "postgres_backup_dump": Path(tempfile.mkdtemp(prefix="host_session_case_")) / "missing_dump.sh"}
    with mock.patch.object(module, "FINAL_BACKUP_SCRIPTS", missing):
        message = refresh.failure(lambda: session.start(code=(refresh.CODE_RUN, refresh.CODE_SHA)))
    shown = [line for line in lines if line.startswith("final_backup_failed")]
    return {
        "start raises the reload error and not the script error": message is not None and "reload" in message and "missing_dump.sh" not in message,
        "the unreadable script is reported as a failed final dump": len(shown) == 1 and "missing_dump.sh" in shown[0],
        "no final dump ran and the backup service was not scaled down": not final_runs(host.log)
        and not any(
            entry["operation"] == "update-service"
            and entry["options"].get("--service") == [BACKUP_SERVICE]
            and entry["options"].get("--desired-count") == ["0"]
            for entry in host.log
        ),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None and session.cleanup_outcome == "host_off_verified",
    }


def a_missing_script_in_a_stop_is_reported_and_the_host_stops() -> dict[str, bool]:
    host, session, lines = build()
    module = refresh.module()
    operation = ("ecs", "describe-task-definition")
    reply = host.handlers[operation]({"--task-definition": [BACKUP_FAMILY]})
    host.handlers[operation] = lambda options: reply
    missing = {**module.FINAL_BACKUP_SCRIPTS, "postgres_backup_upload": Path(tempfile.mkdtemp(prefix="host_session_case_")) / "missing_upload.py"}
    with mock.patch.object(module, "FINAL_BACKUP_SCRIPTS", missing):
        message = refresh.failure(session.stop)
    return {
        "stop does not raise": message is None,
        "the dump is reported as not taken and names the script": (session.final_backup_outcome or "").startswith("failed:")
        and "missing_upload.py" in (session.final_backup_outcome or ""),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def an_unexpected_error_in_the_cleanup_dump_still_turns_the_host_off() -> dict[str, bool]:
    host, session, lines = build({"reload_fails:m2"}, started=False)
    with mock.patch.object(session, "check_final_backup_scripts", side_effect=RuntimeError("stand-in failure")):
        message = refresh.failure(lambda: session.start(code=(refresh.CODE_RUN, refresh.CODE_SHA)))
    shown = [line for line in lines if line.startswith("final_backup_failed")]
    return {
        "start raises the reload error, not the stand-in failure": message is not None and "reload" in message and "stand-in failure" not in message,
        "the failure is reported by name": len(shown) == 1 and "RuntimeError" in shown[0],
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None and session.cleanup_outcome == "host_off_verified",
    }


def interval_writes(host: StandInHost, count: int) -> list[str | None]:
    """The desired counts that the stand-in saw for the interval backup service after its first ``count`` calls, in order."""
    return [
        (entry["options"].get("--desired-count") or [None])[0]
        for entry in host.log[count:]
        if entry["operation"] == "update-service" and entry["options"].get("--service") == [BACKUP_SERVICE]
    ]


def a_refused_host_off_gives_the_interval_backup_service_back() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for name, faults in (
        ("a taken dump", {"deny_host_off"}),
        ("a failed dump", {"deny_host_off", "exit_code:postgres_final_backup=1"}),
        ("a refused launch", {"deny_host_off", "run_task_failure"}),
    ):
        host, session, lines = build()
        host.faults.update(faults)
        count = len(host.log)
        message = refresh.failure(session.stop)
        host.advance(600)
        snapshot = host.service_snapshot()[BACKUP_SERVICE]
        checks[f"{name}: the host-off error is the one reported"] = message is not None and "AccessDeniedException" in message
        checks[f"{name}: the interval backup service is back at its count of 1 and runs"] = snapshot["desired"] == 1 and snapshot["running"] == 1
        checks[f"{name}: it was scaled to zero first and given back once"] = interval_writes(host, count) == ["0", "1"]
        checks[f"{name}: the recovery is reported and the host stays on"] = "backup_service_restored" in step_names(lines) and host.desired == 1
    return checks


def an_unanswered_host_off_request_does_not_give_the_interval_backup_back() -> dict[str, bool]:
    host, session, lines = build()
    module = refresh.module()
    call = session.call
    count = len(host.log)

    def lose_reply(service: str, operation: str, arguments: list[str] | None = None) -> dict[str, Any]:
        if operation == "set-desired-capacity":
            raise module.AwsCallFailed("set-desired-capacity reply lost", module.UNCONFIRMED)
        return call(service, operation, arguments)

    with mock.patch.object(session, "call", lose_reply):
        message = refresh.failure(session.stop)
    return {
        "the host-off error is the one reported": message is not None and "reply lost" in message,
        "the interval backup service stays at zero, because the request may have been applied": interval_writes(host, count) == ["0"],
        "no recovery is reported": "backup_service_restored" not in step_names(lines),
    }


def final_tasks(host: StandInHost) -> int:
    """The final backup tasks that the stand-in started."""
    return sum(task["key"] == "postgres_final_backup" for task in host.tasks.values())


def a_refused_final_launch_at_the_api_gives_the_interval_backup_back() -> dict[str, bool]:
    host, session, lines = build({"deny_host_off"})
    original = host.handlers[("ecs", "run-task")]

    def run_task(options: dict[str, list[str]]) -> dict[str, Any]:
        if options["--task-definition"][0].rsplit("/", 1)[-1].split(":")[0] == BACKUP_FAMILY:
            raise StandInError("AccessDeniedException")
        return original(options)

    host.handlers[("ecs", "run-task")] = run_task
    count = len(host.log)
    message = refresh.failure(session.stop)
    host.advance(600)
    snapshot = host.service_snapshot()[BACKUP_SERVICE]
    return {
        "the host-off error is the one reported": message is not None and "set-desired-capacity" in message and "AccessDeniedException" in message,
        "no final task started": final_tasks(host) == 0,
        "the interval backup service is back at its count of 1 and runs": snapshot["desired"] == 1 and snapshot["running"] == 1,
        "it was scaled to zero first and given back once": interval_writes(host, count) == ["0", "1"],
        "the recovery is reported": "backup_service_restored" in step_names(lines),
    }


def a_refused_final_launch_keeps_the_interval_backup_off_beside_a_final_task() -> dict[str, bool]:
    host, session, lines = build({"deny_host_off"})
    host.tasks[host.add_task(BACKUP_FAMILY)]["stops_at"] = host.now + 10**6
    count = len(host.log)
    message = refresh.failure(session.stop)
    return {
        "the host-off error is the one reported": message is not None and "AccessDeniedException" in message,
        "the interval backup service stays at zero, because a final task is still listed": interval_writes(host, count) == ["0"],
        "no recovery is reported": "backup_service_restored" not in step_names(lines),
    }


def a_refusal_after_a_lost_launch_reply_keeps_the_interval_backup_off() -> dict[str, bool]:
    host, session, lines = build({"deny_host_off"})
    original = host.handlers[("ecs", "run-task")]
    calls: list[str] = []

    def run_task(options: dict[str, list[str]]) -> dict[str, Any]:
        if options["--task-definition"][0].rsplit("/", 1)[-1].split(":")[0] != BACKUP_FAMILY:
            return original(options)
        calls.append("run-task")
        if len(calls) == 1:
            original(options)  # the launch is applied and its reply is lost
            raise TimeoutExpired("stand-in run-task", 180)
        raise StandInError("AccessDeniedException")

    host.handlers[("ecs", "run-task")] = run_task
    count = len(host.log)
    message = refresh.failure(session.stop)
    return {
        "the host-off error is the one reported": message is not None and "set-desired-capacity" in message and "AccessDeniedException" in message,
        "the first launch started a task and the second was refused": len(calls) == 2 and final_tasks(host) == 1,
        "the interval backup service stays at zero, because ECS lists a final task": interval_writes(host, count) == ["0"],
        "no recovery is reported": "backup_service_restored" not in step_names(lines),
    }


def a_scale_down_whose_reply_is_lost_still_gives_the_interval_backup_back() -> dict[str, bool]:
    host, session, lines = build({"deny_host_off"})
    update = host.handlers[("ecs", "update-service")]

    def applied_then_timeout(options: dict[str, list[str]]) -> dict[str, Any]:
        answer = update(options)
        if options.get("--service") == [BACKUP_SERVICE] and options.get("--desired-count") == ["0"]:
            raise TimeoutExpired("stand-in update-service", 180)
        return answer

    host.handlers[("ecs", "update-service")] = applied_then_timeout
    count = len(host.log)
    message = refresh.failure(session.stop)
    host.advance(600)
    snapshot = host.service_snapshot()[BACKUP_SERVICE]
    return {
        "the host-off error is the one reported": message is not None and "AccessDeniedException" in message,
        "no final task was attempted": final_tasks(host) == 0 and not final_runs(refresh.after(host, count)),
        "the interval backup service is back at its count of 1 and runs": snapshot["desired"] == 1 and snapshot["running"] == 1,
        "the recovery is reported": "backup_service_restored" in step_names(lines),
    }


def a_read_error_without_a_provider_code_is_no_refusal_of_the_host_off() -> dict[str, bool]:
    host, session, lines = build()
    runner = session.cli.runner

    def applied_then_read_timeout(argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        answer = runner(argv, **kwargs)
        if argv[1:3] == ["autoscaling", "set-desired-capacity"] and parse(argv).get("--desired-capacity") == ["0"]:
            return CompletedProcess(argv, 255, "", 'Read timeout on endpoint URL: "http://127.0.0.1:9"')
        return answer

    session.cli.runner = applied_then_read_timeout
    count = len(host.log)
    message = refresh.failure(session.stop)
    host.advance(600)
    return {
        "the error is the one reported": message is not None and "request_failed" in message,
        "the interval backup service is not given back, because the request was applied": interval_writes(host, count) == ["0"],
        "no recovery and no claim that the host stays on": "backup_service_restored" not in step_names(lines)
        and not any("host stays on" in line for line in lines),
        "the host went off": host.desired == 0 and host.instance_up_at is None,
    }


def a_failed_interval_recovery_keeps_the_host_off_error() -> dict[str, bool]:
    host, session, lines = build({"deny_host_off"})
    update = host.handlers[("ecs", "update-service")]

    def refuse_restore(options: dict[str, list[str]]) -> dict[str, Any]:
        if options.get("--service") == [BACKUP_SERVICE] and options.get("--desired-count") == ["1"]:
            raise StandInError("AccessDeniedException")
        return update(options)

    host.handlers[("ecs", "update-service")] = refuse_restore
    message = refresh.failure(session.stop)
    shown = [line for line in lines if line.startswith("backup_service_restore_failed")]
    return {
        "the host-off error is the one reported": message is not None and "set-desired-capacity" in message and "AccessDeniedException" in message,
        "the failed recovery is reported by name": len(shown) == 1 and BACKUP_SERVICE in shown[0] and "AccessDeniedException" in shown[0],
        "no recovery is reported": "backup_service_restored" not in step_names(lines),
    }


def a_refused_host_off_leaves_the_interval_backup_while_the_final_task_may_run() -> dict[str, bool]:
    host, session, lines = build({"deny_host_off", "task_seconds:postgres_final_backup=100000"})
    count = len(host.log)
    message = refresh.failure(session.stop)
    return {
        "the host-off error is the one reported": message is not None and "AccessDeniedException" in message,
        "the interval backup service stays at zero, because the final task may still hold the backup folder": interval_writes(host, count) == ["0"],
        "no recovery is reported": "backup_service_restored" not in step_names(lines),
    }


def a_stop_that_turns_the_host_off_leaves_the_interval_backup_at_zero() -> dict[str, bool]:
    host, session, lines = build()
    count = len(host.log)
    session.stop()
    return {
        "the interval backup service was set to zero once and not given back": interval_writes(host, count) == ["0"],
        "no recovery is reported": "backup_service_restored" not in step_names(lines),
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "final_dump_follows_the_job_tasks_and_precedes_the_host_stop": final_dump_follows_the_job_tasks_and_precedes_the_host_stop,
    "the_final_launch_names_the_task_definition_revision_that_was_checked": the_final_launch_names_the_task_definition_revision_that_was_checked,
    "older_final_backup_scripts_take_no_dump_and_the_host_still_stops": older_final_backup_scripts_take_no_dump_and_the_host_still_stops,
    "the_final_backup_launch_carries_a_new_task_id_in_both_containers": the_final_backup_launch_carries_a_new_task_id_in_both_containers,
    "a_stopped_reply_for_only_one_container_is_not_a_taken_dump": a_stopped_reply_for_only_one_container_is_not_a_taken_dump,
    "a_final_backup_task_of_an_earlier_stop_is_awaited_then_a_new_one_runs": a_final_backup_task_of_an_earlier_stop_is_awaited_then_a_new_one_runs,
    "a_failed_final_dump_is_reported_by_name_and_the_host_still_stops": a_failed_final_dump_is_reported_by_name_and_the_host_still_stops,
    "a_final_dump_past_its_deadline_fails_visibly_and_the_host_stops": a_final_dump_past_its_deadline_fails_visibly_and_the_host_stops,
    "an_interrupt_during_the_final_dump_still_turns_the_host_off": an_interrupt_during_the_final_dump_still_turns_the_host_off,
    "a_backup_service_that_will_not_stop_blocks_the_dump_and_not_the_host": a_backup_service_that_will_not_stop_blocks_the_dump_and_not_the_host,
    "a_stack_with_the_backup_off_stops_without_a_dump": a_stack_with_the_backup_off_stops_without_a_dump,
    "a_postgres_setup_that_is_not_complete_takes_no_dump": a_postgres_setup_that_is_not_complete_takes_no_dump,
    "the_next_start_sets_the_backup_service_back_to_one_task": the_next_start_sets_the_backup_service_back_to_one_task,
    "a_failed_start_with_stable_services_takes_the_final_dump_first": a_failed_start_with_stable_services_takes_the_final_dump_first,
    "a_failed_start_before_stable_services_takes_no_dump": a_failed_start_before_stable_services_takes_no_dump,
    "a_failed_final_dump_in_the_cleanup_keeps_the_first_error": a_failed_final_dump_in_the_cleanup_keeps_the_first_error,
    "a_start_that_keeps_the_host_on_takes_no_dump": a_start_that_keeps_the_host_on_takes_no_dump,
    "a_missing_script_in_the_cleanup_keeps_the_first_error_and_stops_the_host": a_missing_script_in_the_cleanup_keeps_the_first_error_and_stops_the_host,
    "a_missing_script_in_a_stop_is_reported_and_the_host_stops": a_missing_script_in_a_stop_is_reported_and_the_host_stops,
    "a_refused_host_off_gives_the_interval_backup_service_back": a_refused_host_off_gives_the_interval_backup_service_back,
    "an_unexpected_error_in_the_cleanup_dump_still_turns_the_host_off": an_unexpected_error_in_the_cleanup_dump_still_turns_the_host_off,
    "an_unanswered_host_off_request_does_not_give_the_interval_backup_back": an_unanswered_host_off_request_does_not_give_the_interval_backup_back,
    "a_failed_interval_recovery_keeps_the_host_off_error": a_failed_interval_recovery_keeps_the_host_off_error,
    "a_refused_final_launch_at_the_api_gives_the_interval_backup_back": a_refused_final_launch_at_the_api_gives_the_interval_backup_back,
    "a_refused_final_launch_keeps_the_interval_backup_off_beside_a_final_task": a_refused_final_launch_keeps_the_interval_backup_off_beside_a_final_task,
    "a_refusal_after_a_lost_launch_reply_keeps_the_interval_backup_off": a_refusal_after_a_lost_launch_reply_keeps_the_interval_backup_off,
    "a_scale_down_whose_reply_is_lost_still_gives_the_interval_backup_back": a_scale_down_whose_reply_is_lost_still_gives_the_interval_backup_back,
    "a_read_error_without_a_provider_code_is_no_refusal_of_the_host_off": a_read_error_without_a_provider_code_is_no_refusal_of_the_host_off,
    "a_refused_host_off_leaves_the_interval_backup_while_the_final_task_may_run": a_refused_host_off_leaves_the_interval_backup_while_the_final_task_may_run,
    "a_stop_that_turns_the_host_off_leaves_the_interval_backup_at_zero": a_stop_that_turns_the_host_off_leaves_the_interval_backup_at_zero,
}
