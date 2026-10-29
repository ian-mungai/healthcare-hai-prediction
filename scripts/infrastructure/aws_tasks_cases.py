"""End-to-end cases for ``aws_tasks`` against the in-process ECS stand-in; each case returns its named checks.

Every case builds a new stand-in host with job, writer, service and one-shot tasks, runs the real ``TaskCleanup`` through ``Session`` and ``AwsCli`` with the
stand-in as the runner and a virtual clock, and checks the calls the stand-in saw and the state it ended in. A clock that runs past an hour of virtual time
raises, so a planted defect that removes a deadline ends as a failed case and not as a hang.
"""

from __future__ import annotations

import importlib
import io
import json
import tempfile
from collections.abc import Callable, Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure.host_session import Clock, HostSessionError, Session
from scripts.infrastructure.host_session_cases import ARN_OR_ACCOUNT, REPO_ROOT, names
from scripts.infrastructure.host_session_stand_in import ACCOUNT_MARK, FAMILIES, PROFILE, REGION, SERVICES, STOP_TASK_SECONDS, StandInHost
from scripts.process import CompletedProcess, TimeoutExpired

__all__ = ["CASES", "CASE_ERRORS", "REPO_ROOT"]

PREFIX = "standin_dev_job_"
READ = f"{PREFIX}read_silver"
WRITER = f"{PREFIX}write_bronze"
RUNAWAY_SECONDS = 3600
CASE_ERRORS = (AssertionError, LookupError, StopIteration, TypeError, ValueError, AttributeError, NameError, KeyError, HostSessionError)
STOP_CALLS = {"stop-task"}
LISTING = {"list-tasks", "describe-tasks"}


def tasks_module() -> Any:
    """The ``aws_tasks`` module as it is loaded now (a plant replaces it in ``sys.modules``)."""
    return importlib.import_module("scripts.infrastructure.aws_tasks")


def runaway_sleep(host: StandInHost) -> Callable[[float], None]:
    """The sleep of a case clock: it moves virtual time and fails the case when a wait has no deadline."""

    def sleep(seconds: float) -> None:
        if host.now > RUNAWAY_SECONDS:
            raise AssertionError("a wait ran past an hour of virtual time")
        host.advance(seconds)

    return sleep


def build(faults: set[str] | None = None) -> tuple[StandInHost, Any, list[str]]:
    """A stand-in host and a ``TaskCleanup`` wired to it; the third value collects the report lines."""
    host, lines = StandInHost(faults), list[str]()
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION}, host.runner)
    marker = Path(tempfile.mkdtemp(prefix="aws_tasks_case_")) / "unused.launched"
    session = Session(cli, names(), Clock(lambda: host.now, runaway_sleep(host)), marker, lines.append)
    return host, tasks_module().TaskCleanup(session, PREFIX), lines


def run_main(host: StandInHost, arguments: list[str], setup_seconds: float) -> int:
    """The exit code of the real ``main`` with a configuration, an ``AwsCli`` and a Terraform lookup that the stand-in and a virtual clock replace.

    The lookup of the names takes ``setup_seconds`` of virtual time, like slow Terraform outputs. Output of the command is discarded.
    """
    module = tasks_module()
    clock = Clock(lambda: host.now, runaway_sleep(host))

    def slow_names(_settings: dict[str, str], *_limit: Any) -> Any:
        host.advance(setup_seconds)
        return names()

    class TimedSession(Session):
        def __init__(self, cli: AwsCli, session_names: Any, session_clock: Clock | None = None) -> None:
            super().__init__(cli, session_names, session_clock, Path(tempfile.mkdtemp(prefix="aws_tasks_case_")) / "unused.launched")

    settings = {"project_name": "standin", "environment": "dev", "aws_profile": PROFILE, "aws_region": REGION}
    with (
        patch.object(module, "load_configuration", lambda _path: (settings, {})),
        patch.object(module, "AwsCli", lambda configuration: AwsCli(configuration, host.runner)),
        patch.object(module, "load_names", slow_names),
        patch.object(module, "Session", TimedSession),
        patch.object(module, "install_termination_handler", lambda: lambda: None),
        redirect_stdout(io.StringIO()),
        redirect_stderr(io.StringIO()),
    ):
        return int(module.main(arguments, clock))


def run_main_with_lookups(host: StandInHost, arguments: list[str], durations: Sequence[float]) -> tuple[int, list[float]]:
    """The exit code of the real ``main`` and the timeout of each Terraform lookup when the real ``load_names`` runs against a replaced process runner.

    Lookup ``n`` would take ``durations[n]`` seconds of virtual time. A real runner stops the process at its timeout, so a lookup that would take longer ends
    at its timeout with ``TimeoutExpired``. Output of the command is discarded.
    """
    module = tasks_module()
    renderer = importlib.import_module("scripts.infrastructure.render_platform_config")
    clock = Clock(lambda: host.now, runaway_sleep(host))
    session_names = names()
    outputs = [
        {
            "cluster_name": {"value": session_names.cluster},
            "host_autoscaling_group_name": {"value": session_names.group},
            "ec2_capacity_provider_name": {"value": session_names.provider},
            "project_tag": {"value": session_names.project_tag},
        },
        {
            "service_names": {"value": session_names.services},
            "one_shot_run_task": {"value": {key: {"task_definition": value} for key, value in session_names.one_shot.items()}},
        },
    ]
    timeouts: list[float] = []

    def fake_run(program: str, argv: Any, *, env: Any, timeout: float) -> CompletedProcess[str]:
        timeouts.append(timeout)
        duration = durations[len(timeouts) - 1]
        host.advance(min(duration, timeout))
        if duration > timeout:
            raise TimeoutExpired([program], timeout)
        return CompletedProcess([program, *argv], 0, json.dumps(outputs[len(timeouts) - 1]), "")

    class TimedSession(Session):
        def __init__(self, cli: AwsCli, session_names: Any, session_clock: Clock | None = None) -> None:
            super().__init__(cli, session_names, session_clock, Path(tempfile.mkdtemp(prefix="aws_tasks_case_")) / "unused.launched")

    settings = {"project_name": "standin", "environment": "dev", "aws_profile": PROFILE, "aws_region": REGION}
    with (
        patch.object(renderer, "run_command", fake_run),
        patch.object(module, "load_configuration", lambda _path: (settings, {})),
        patch.object(module, "AwsCli", lambda configuration: AwsCli(configuration, host.runner)),
        patch.object(module, "Session", TimedSession),
        patch.object(module, "install_termination_handler", lambda: lambda: None),
        redirect_stdout(io.StringIO()),
        redirect_stderr(io.StringIO()),
    ):
        return int(module.main(arguments, clock)), timeouts


def suffix(arn: str) -> str:
    """The last 8 characters of a task ID."""
    return arn.rsplit("/", 1)[-1][-8:]


def populate(host: StandInHost, writer: bool = False) -> dict[str, str]:
    """Three job tasks (and a writer task when asked) beside a service task, a one-shot task and a task of another project."""
    found = {"writer": host.add_task(WRITER)} if writer else {}
    found |= {f"read{index}": host.add_task(f"{READ}_{index}") for index in range(3)}
    found["service"] = host.add_task(SERVICES["polaris"], group=f"service:{SERVICES['polaris']}")
    found["one_shot"] = host.add_task(FAMILIES["host_prepare"])
    found["other_project"] = host.add_task("other_dev_job_read_silver")
    return found


def operations(host: StandInHost, names_: set[str]) -> list[dict[str, Any]]:
    """The logged calls with one of the operations."""
    return [entry for entry in host.log if entry["operation"] in names_]


def failure(action: Callable[[], object]) -> str | None:
    """The message of the HostSessionError the action raised, or None."""
    try:
        action()
    except HostSessionError as error:
        return str(error)
    return None


def status(host: StandInHost, arn: str) -> str:
    """The stand-in status of a task."""
    return str(host.tasks[arn]["status"])


def listing_follows_pages() -> dict[str, bool]:
    host, cleanup, lines = build({"page_size:2"})
    found = populate(host)
    for index in range(2):
        found[f"extra{index}"] = host.add_task(f"{READ}_extra{index}")
    listed = cleanup.list_tasks()
    calls = [call for call in operations(host, {"list-tasks"}) if call["options"].get("--desired-status") == ["RUNNING"]]
    return {
        "every page is read": len(listed) == 5,
        "the calls turn off the CLI's own joining": all("--no-paginate" in call["options"] for call in calls),
        "each later call carries the token of the page before": [call["options"].get("--next-token") for call in calls]
        == [None, *[[str(start)] for start in range(2, len(host.tasks), 2)]],
        "the report names the count": any("5" in line for line in lines),
    }


def listing_skips_other_tasks() -> dict[str, bool]:
    host, cleanup, lines = build()
    found = populate(host, writer=True)
    listed = cleanup.list_tasks()
    suffixes = {item.suffix for item in listed}
    return {
        "the job and writer tasks are listed": suffixes == {suffix(found[key]) for key in ("read0", "read1", "read2", "writer")},
        "a service task is not listed": suffix(found["service"]) not in suffixes,
        "a one-shot task is not listed": suffix(found["one_shot"]) not in suffixes,
        "a task of another project is not listed": suffix(found["other_project"]) not in suffixes,
        "the writer is marked": {item.suffix for item in listed if item.writer} == {suffix(found["writer"])},
        "a listing reports no skipped family": not any("service" in line or "host-prepare" in line or "other_dev" in line for line in lines),
    }


def listing_is_read_only() -> dict[str, bool]:
    host, cleanup, _ = build()
    populate(host, writer=True)
    cleanup.list_tasks()
    return {
        "only list and describe calls": {entry["operation"] for entry in host.log} <= LISTING,
        "no task changed": all(task["status"] == "RUNNING" for task in host.tasks.values()),
    }


def listing_in_chunks() -> dict[str, bool]:
    host, cleanup, _ = build()
    for index in range(101):
        host.add_task(f"{READ}_{index}")
    listed = cleanup.list_tasks()
    describes = operations(host, {"describe-tasks"})
    return {
        "all 101 tasks are listed": len(listed) == 101,
        "no describe call has more than 100 tasks": all(len(call["options"]["--tasks"]) <= 100 for call in describes),
    }


def listing_token_loop() -> dict[str, bool]:
    host, cleanup, _ = build({"page_size:1", "repeat_token"})
    populate(host)
    message = failure(cleanup.list_tasks)
    return {"a repeated token fails": message is not None and "token" in message, "it stops after the first repeat": len(operations(host, {"list-tasks"})) <= 3}


def listing_ghost_task() -> dict[str, bool]:
    host, cleanup, _ = build({"ghost_task"})
    populate(host)
    listed = cleanup.list_tasks()
    return {"a task that is gone by description time is ignored": len(listed) == 3}


def stop_waits_for_stopped() -> dict[str, bool]:
    host, cleanup, lines = build()
    found = populate(host)
    cleanup.stop(frozenset())
    stops = operations(host, STOP_CALLS)
    return {
        "every job task is stopped": all(status(host, found[key]) == "STOPPED" for key in ("read0", "read1", "read2")),
        "the service task is untouched": status(host, found["service"]) == "RUNNING",
        "the one-shot task is untouched": status(host, found["one_shot"]) == "RUNNING",
        "the task of another project is untouched": status(host, found["other_project"]) == "RUNNING",
        "one stop call per job task": len(stops) == 3,
        "the stop calls name the cluster and a reason": all(
            call["options"].get("--reason") == ["aws_tasks stop"] and "--cluster" in call["options"] for call in stops
        ),
        "the command returned only after the tasks stopped": host.now >= STOP_TASK_SECONDS,
        "the command calls only list, describe and stop": {entry["operation"] for entry in host.log} <= LISTING | STOP_CALLS,
        "the report says each task stopped": sum("stopped" in line for line in lines) >= 3,
    }


def stop_not_stopped_fails_in_bound() -> dict[str, bool]:
    host, cleanup, lines = build({"never_stop_task"})
    found = populate(host)
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    return {
        "the command fails": bool(message),
        "the message names each task by family and suffix": all(suffix(found[key]) in message for key in ("read0", "read1", "read2")) and READ in message,
        "the wait ends within the stop grace": host.now <= 90 + 5,
        "the wait polled": len(operations(host, {"describe-tasks"})) >= 3,
        "no ARN or account ID in the message": not ARN_OR_ACCOUNT.search(message) and ACCOUNT_MARK not in message,
    }


def stop_deadline_with_slow_calls() -> dict[str, bool]:
    host, cleanup, _ = build({"never_stop_task", "slow_calls:25"})
    populate(host)
    message = failure(lambda: cleanup.stop(frozenset()))
    return {
        "the command fails visibly": message is not None,
        "the whole command ends within the cleanup bound": host.now <= 120 + 1,
        "no call was given more than the remaining time": all(entry["timeout"] is None or entry["timeout"] <= 120 for entry in host.log),
    }


def listing_deadline_with_slow_calls() -> dict[str, bool]:
    host, cleanup, _ = build({"slow_calls:70"})
    populate(host)
    message = failure(cleanup.list_tasks)
    return {"the listing fails visibly": message is not None, "it ends within the cleanup bound": host.now <= 120 + 1}


def writer_refused_without_confirmation() -> dict[str, bool]:
    host, cleanup, _ = build()
    found = populate(host, writer=True)
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    stopped_arns = [call["options"]["--task"][0] for call in operations(host, STOP_CALLS)]
    return {
        "the command fails": bool(message),
        "no stop call names the writer": found["writer"] not in stopped_arns,
        "the writer keeps running": status(host, found["writer"]) == "RUNNING",
        "the other job tasks are stopped": all(status(host, found[key]) == "STOPPED" for key in ("read0", "read1", "read2")),
        "the message names the writer and the flag": suffix(found["writer"]) in message and "--confirm-writer-stop" in message,
    }


def writer_confirmed_stop() -> dict[str, bool]:
    host, cleanup, lines = build()
    found = populate(host, writer=True)
    cleanup.stop(frozenset({suffix(found["writer"])}))
    reasons = {call["options"]["--task"][0]: call["options"]["--reason"][0] for call in operations(host, STOP_CALLS)}
    return {
        "the writer is stopped": status(host, found["writer"]) == "STOPPED",
        "the reason records the owner's confirmation": "owner confirmed writer stop" in reasons[found["writer"]],
        "other tasks keep the plain reason": reasons[found["read0"]] == "aws_tasks stop",
        "the report names the recovery hold": any("recovery hold" in line for line in lines),
    }


def confirmation_covers_only_named() -> dict[str, bool]:
    host, cleanup, _ = build()
    found = populate(host, writer=True)
    second = host.add_task(WRITER)
    message = failure(lambda: cleanup.stop(frozenset({suffix(found["writer"]), "0badf00d"}))) or ""
    return {
        "the named writer is stopped": status(host, found["writer"]) == "STOPPED",
        "the other writer keeps running": status(host, second) == "RUNNING",
        "an unmatched confirmation is reported": "0badf00d" in message,
        "the unconfirmed writer is reported": suffix(second) in message,
    }


def stop_task_that_vanishes() -> dict[str, bool]:
    host, cleanup, _ = build({"vanish_after_stop"})
    populate(host)
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    return {"a task ECS no longer describes is not confirmed as stopped": "not STOPPED" in message}


def stop_failure_does_not_hide_others() -> dict[str, bool]:
    host, cleanup, _ = build({"stop_task_error"})
    found = populate(host)
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    return {
        "every task got its stop call": len(operations(host, STOP_CALLS)) == 3,
        "the command fails": bool(message),
        "the message names every task still running": all(suffix(found[key]) in message for key in ("read0", "read1", "read2")),
    }


def stop_rerun() -> dict[str, bool]:
    host, cleanup, lines = build()
    populate(host)
    cleanup.stop(frozenset())
    calls = len(operations(host, STOP_CALLS))
    lines.clear()
    cleanup.stop(frozenset())
    return {
        "the second run sends no stop call": len(operations(host, STOP_CALLS)) == calls,
        "the second run succeeds and says no job task is running": any("no job task" in line for line in lines),
    }


def stop_retry_finishes_stopping_tasks() -> dict[str, bool]:
    host, cleanup, lines = build({"never_stop_task"})
    found = populate(host)
    first = failure(lambda: cleanup.stop(frozenset()))
    host.faults.discard("never_stop_task")
    lines.clear()
    first_stops = len(operations(host, STOP_CALLS))
    cleanup.stop(frozenset())
    return {
        "the first stop fails with the tasks left": first is not None,
        "the retry sends a stop call again for each task that is still running": len(operations(host, STOP_CALLS)) == first_stops + 3,
        "the retry ends with every job task STOPPED": all(status(host, found[key]) == "STOPPED" for key in ("read0", "read1", "read2")),
        "the retry does not say that no job task is running": not any("no job task" in line for line in lines),
    }


def stop_retry_still_running_fails() -> dict[str, bool]:
    host, cleanup, _ = build({"never_stop_task"})
    found = populate(host)
    failure(lambda: cleanup.stop(frozenset()))
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    return {
        "the retry fails while the tasks run": bool(message),
        "the retry names each task": all(suffix(found[key]) in message for key in ("read0", "read1", "read2")),
    }


def listing_shows_stopping_tasks() -> dict[str, bool]:
    host, cleanup, _ = build({"never_stop_task"})
    populate(host)
    failure(lambda: cleanup.stop(frozenset()))
    listed = cleanup.list_tasks()
    desired = {call["options"].get("--desired-status", [""])[0] for call in operations(host, {"list-tasks"})}
    return {
        "a task with desired status STOPPED and a running status is listed": len(listed) == 3,
        "both desired statuses are listed": {"RUNNING", "STOPPED"} <= desired,
    }


def listing_pages_both_statuses() -> dict[str, bool]:
    host, cleanup, _ = build({"page_size:2", "never_stop_task"})
    populate(host)
    for index in range(3):
        host.add_task(f"{READ}_more{index}")
    failure(lambda: cleanup.stop(frozenset()))
    host.log.clear()
    listed = cleanup.list_tasks()
    calls = operations(host, {"list-tasks"})
    stopped_calls = [call for call in calls if call["options"].get("--desired-status") == ["STOPPED"]]
    return {
        "all six stopping tasks are listed once": len(listed) == 6 and len({item.arn for item in listed}) == 6,
        "the STOPPED listing follows its own pages": len(stopped_calls) == 3,
        "no listing ends without its last page": all(call["options"].get("--next-token") != ["6"] for call in calls),
    }


def writer_classified_by_definition() -> dict[str, bool]:
    host, cleanup, _ = build()
    found = populate(host)
    disguised = host.add_task(WRITER, group=f"family:{READ}_0")
    message = failure(lambda: cleanup.stop(frozenset({suffix(disguised)}))) or ""
    stopped_arns = [call["options"]["--task"][0] for call in operations(host, STOP_CALLS)]
    return {
        "the writer with a job group is not stopped": disguised not in stopped_arns and status(host, disguised) == "RUNNING",
        "a confirmation does not override the mismatch": status(host, disguised) == "RUNNING",
        "the command fails and names the task": bool(message) and suffix(disguised) in message,
        "the other job tasks are stopped": all(status(host, found[key]) == "STOPPED" for key in ("read0", "read1", "read2")),
    }


def mismatch_refused_every_way() -> dict[str, bool]:
    host, cleanup, lines = build()
    populate(host)
    reader_in_writer_group = host.add_task(f"{READ}_9", group=f"family:{WRITER}")
    job_in_service_group = host.add_task(f"{READ}_8", group=f"service:{SERVICES['polaris']}")
    foreign_definition = host.add_task("other_dev_job_read_silver", group=f"family:{READ}_7")
    listed = cleanup.list_tasks()
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    refused = (reader_in_writer_group, job_in_service_group, foreign_definition)
    stopped_arns = [call["options"]["--task"][0] for call in operations(host, STOP_CALLS)]
    return {
        "no mismatched task is stopped": not any(arn in stopped_arns for arn in refused),
        "each mismatched task is named by its suffix": all(suffix(arn) in message for arn in refused),
        "the listing shows each mismatched task": {item.suffix for item in listed} >= {suffix(arn) for arn in refused},
        "the listing marks the mismatch": sum("differ" in line for line in lines) == 3,
    }


def missing_definition_refuses() -> dict[str, bool]:
    host, cleanup, _ = build({"no_definition"})
    populate(host)
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    return {"no task is stopped without a task definition": not operations(host, STOP_CALLS), "the command fails": bool(message)}


def mismatch_output_privacy() -> dict[str, bool]:
    host, cleanup, lines = build()
    leak = f"{PREFIX}x {ACCOUNT_MARK}task/leak i-0123456789abcdef0 123456789012"
    host.add_task(WRITER, group=f"family:{leak}")
    host.add_task("other_dev_job_read_silver", group=f"family:{leak}")
    cleanup.list_tasks()
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    text = "\n".join([*lines, message])
    return {"no ARN, account ID or instance ID from the group": not ARN_OR_ACCOUNT.search(text) and ACCOUNT_MARK not in text and "123456789012" not in text}


def stop_failed_call_is_described() -> dict[str, bool]:
    host, cleanup, _ = build({"stop_task_error"})
    found = populate(host)
    message = failure(lambda: cleanup.stop(frozenset())) or ""
    calls = [entry["operation"] for entry in host.log]
    last_stop = max(index for index, operation in enumerate(calls) if operation == "stop-task")
    return {
        "a describe call follows the last stop call": "describe-tasks" in calls[last_stop + 1 :],
        "the message says each task is not STOPPED": all(suffix(found[key]) in message for key in ("read0", "read1", "read2")) and "not STOPPED" in message,
    }


def stop_lost_reply_is_confirmed() -> dict[str, bool]:
    host, cleanup, _ = build({"stop_task_lost_reply"})
    found = populate(host)
    message = failure(lambda: cleanup.stop(frozenset()))
    return {
        "a stop call that failed but took effect is confirmed by description": message is None,
        "every job task is STOPPED": all(status(host, found[key]) == "STOPPED" for key in ("read0", "read1", "read2")),
    }


def setup_counts_toward_the_deadline() -> dict[str, bool]:
    host = StandInHost({"slow_calls:30"})
    populate(host)
    code = run_main(host, ["list"], setup_seconds=100)
    stop_host = StandInHost({"slow_calls:30", "never_stop_task"})
    populate(stop_host)
    stop_code = run_main(stop_host, ["stop"], setup_seconds=100)
    return {
        "a listing after 100 s of setup fails visibly": code == 1,
        "the listing run ends within the 120 s bound, setup included": host.now <= 120 + 1,
        "no listing call is given more than the time left": all(entry["timeout"] is not None and entry["timeout"] <= 20 + 1 for entry in host.log),
        "a stop after 100 s of setup fails visibly": stop_code == 1,
        "the stop run ends within the 120 s bound, setup included": stop_host.now <= 120 + 1,
        "no stop-run call is given more than the time left": all(entry["timeout"] is not None and entry["timeout"] <= 20 + 1 for entry in stop_host.log),
    }


def lookups_share_the_deadline() -> dict[str, bool]:
    results: dict[str, bool] = {}
    for name, durations in (("119 s and 119 s", [119, 119]), ("119 s and a hung lookup", [119, 500]), ("a first lookup of 120 s", [120, 5])):
        host = StandInHost()
        populate(host)
        code, timeouts = run_main_with_lookups(host, ["list"], durations)
        results[f"{name}: the command ends within the 120 s bound"] = host.now <= 120 + 1
        results[f"{name}: the command fails and sends no task call"] = code in (1, 2) and not host.log
        results[f"{name}: no lookup is given more than the time left"] = all(
            timeout <= 120 - spent + 1e-6 for timeout, spent in zip(timeouts, [0, durations[0]], strict=False)
        )
    host = StandInHost()
    found = populate(host)
    code, timeouts = run_main_with_lookups(host, ["stop"], [10, 10])
    results["two quick lookups: the second gets the time left after the first"] = len(timeouts) == 2 and timeouts[0] == 120 and timeouts[1] == 110
    results["two quick lookups: the stop runs"] = code == 0 and all(status(host, found[key]) == "STOPPED" for key in ("read0", "read1", "read2"))
    return results


def setup_that_used_the_time_sends_no_call() -> dict[str, bool]:
    host = StandInHost()
    found = populate(host)
    list_code = run_main(host, ["list"], setup_seconds=120)
    stop_code = run_main(host, ["stop"], setup_seconds=120)
    return {
        "a listing after 120 s of setup fails": list_code == 1,
        "a stop after 120 s of setup fails": stop_code == 1,
        "no task call was sent": not host.log,
        "no task was stopped": all(status(host, arn) == "RUNNING" for arn in found.values()),
    }


def setup_within_the_bound_runs() -> dict[str, bool]:
    host = StandInHost()
    found = populate(host)
    list_code = run_main(host, ["list"], setup_seconds=10)
    stop_code = run_main(host, ["stop"], setup_seconds=10)
    return {
        "a listing after 10 s of setup succeeds": list_code == 0,
        "a stop after 10 s of setup succeeds": stop_code == 0,
        "the job tasks are stopped": all(status(host, found[key]) == "STOPPED" for key in ("read0", "read1", "read2")),
        "the other tasks are untouched": all(status(host, found[key]) == "RUNNING" for key in ("service", "one_shot", "other_project")),
    }


def stale_confirmation_on_empty_cluster() -> dict[str, bool]:
    host, cleanup, lines = build()
    message = failure(lambda: cleanup.stop(frozenset({"deadbeef"}))) or ""
    plain = failure(lambda: cleanup.stop(frozenset()))
    main_host = StandInHost()
    code = run_main(main_host, ["stop", "--confirm-writer-stop", "deadbeef"], setup_seconds=1)
    return {
        "a stale confirmation on an empty cluster fails": bool(message),
        "the message names the confirmation": "deadbeef" in message and "matches no running writer" in message,
        "no stop call was sent": not operations(host, STOP_CALLS),
        "the report still says no job task is running": any("no job task" in line for line in lines),
        "no confirmation on an empty cluster still succeeds": plain is None,
        "the command line exits 1": code == 1,
    }


def stale_confirmation_beside_job_tasks() -> dict[str, bool]:
    host, cleanup, _ = build()
    found = populate(host)
    message = failure(lambda: cleanup.stop(frozenset({"deadbeef"}))) or ""
    return {
        "the job tasks are stopped": all(status(host, found[key]) == "STOPPED" for key in ("read0", "read1", "read2")),
        "the stale confirmation fails the command": "deadbeef" in message,
    }


def output_privacy() -> dict[str, bool]:
    host, cleanup, lines = build({"leaky_reasons", "never_stop_task"})
    found = populate(host, writer=True)
    cleanup.list_tasks()
    message = failure(lambda: cleanup.stop(frozenset({suffix(found["writer"])}))) or ""
    text = "\n".join([*lines, message])
    return {
        "no ARN, account ID or instance ID": not ARN_OR_ACCOUNT.search(text),
        "no account marker": ACCOUNT_MARK not in text,
        "no full task ID": "ababababab" not in text,
        "no provider reason text": "Essential container" not in text and "123456789012" not in text,
        "family and suffix are shown": READ in text and suffix(found["writer"]) in text,
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "listing_follows_pages": listing_follows_pages,
    "listing_skips_other_tasks": listing_skips_other_tasks,
    "listing_is_read_only": listing_is_read_only,
    "listing_in_chunks": listing_in_chunks,
    "listing_token_loop": listing_token_loop,
    "listing_ghost_task": listing_ghost_task,
    "stop_waits_for_stopped": stop_waits_for_stopped,
    "stop_not_stopped_fails_in_bound": stop_not_stopped_fails_in_bound,
    "stop_deadline_with_slow_calls": stop_deadline_with_slow_calls,
    "listing_deadline_with_slow_calls": listing_deadline_with_slow_calls,
    "writer_refused_without_confirmation": writer_refused_without_confirmation,
    "writer_confirmed_stop": writer_confirmed_stop,
    "confirmation_covers_only_named": confirmation_covers_only_named,
    "stop_task_that_vanishes": stop_task_that_vanishes,
    "stop_failure_does_not_hide_others": stop_failure_does_not_hide_others,
    "stop_rerun": stop_rerun,
    "stop_retry_finishes_stopping_tasks": stop_retry_finishes_stopping_tasks,
    "stop_retry_still_running_fails": stop_retry_still_running_fails,
    "listing_shows_stopping_tasks": listing_shows_stopping_tasks,
    "listing_pages_both_statuses": listing_pages_both_statuses,
    "writer_classified_by_definition": writer_classified_by_definition,
    "mismatch_refused_every_way": mismatch_refused_every_way,
    "missing_definition_refuses": missing_definition_refuses,
    "mismatch_output_privacy": mismatch_output_privacy,
    "stop_failed_call_is_described": stop_failed_call_is_described,
    "stop_lost_reply_is_confirmed": stop_lost_reply_is_confirmed,
    "setup_counts_toward_the_deadline": setup_counts_toward_the_deadline,
    "lookups_share_the_deadline": lookups_share_the_deadline,
    "setup_that_used_the_time_sends_no_call": setup_that_used_the_time_sends_no_call,
    "setup_within_the_bound_runs": setup_within_the_bound_runs,
    "stale_confirmation_on_empty_cluster": stale_confirmation_on_empty_cluster,
    "stale_confirmation_beside_job_tasks": stale_confirmation_beside_job_tasks,
    "output_privacy": output_privacy,
}
