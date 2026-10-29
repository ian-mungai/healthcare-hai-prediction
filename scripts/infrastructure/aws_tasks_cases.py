"""End-to-end cases for ``aws_tasks`` against the in-process ECS stand-in; each case returns its named checks.

Every case builds a new stand-in host with job, writer, service and one-shot tasks, runs the real ``TaskCleanup`` through ``Session`` and ``AwsCli`` with the
stand-in as the runner and a virtual clock, and checks the calls the stand-in saw and the state it ended in. A clock that runs past an hour of virtual time
raises, so a planted defect that removes a deadline ends as a failed case and not as a hang.
"""

from __future__ import annotations

import importlib
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure.host_session import Clock, HostSessionError, Session
from scripts.infrastructure.host_session_cases import ARN_OR_ACCOUNT, REPO_ROOT, names
from scripts.infrastructure.host_session_stand_in import ACCOUNT_MARK, FAMILIES, PROFILE, REGION, SERVICES, STOP_TASK_SECONDS, StandInHost

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
    calls = operations(host, {"list-tasks"})
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
    host, cleanup, _ = build({"never_stop_task", "slow_calls:50"})
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
    "output_privacy": output_privacy,
}
