"""Host session cases for ``stop``: it lists the running job tasks first and refuses, waits up to a deadline or is forced.

Every case starts a stand-in host with the real ``Session``, adds job tasks the way a DAG launch would and then runs ``stop``, so the checks see exactly
what the command listed, printed and changed. Time is virtual, so a wait of ten minutes takes no time.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from functools import partial
from typing import Any
from unittest import mock

from scripts.infrastructure import host_session_refresh_cases as refresh
from scripts.infrastructure.host_session_stand_in import EPOCH, StandInHost

READER = f"{refresh.JOB_PREFIX}read_silver"
WRITER = f"{refresh.JOB_PREFIX}write_gold"
POLL = 15


def on_wall_time(host: StandInHost, session: Any, sleep: Callable[[float], None] | None = None) -> None:
    """Give the session the wall clock of the stand-in (``EPOCH`` plus the virtual time), so a task's run time is the virtual time since it was added."""
    session.clock = replace(session.clock, wall=lambda: EPOCH.timestamp() + host.now, **({} if sleep is None else {"sleep": sleep}))


def scene(*families: str) -> tuple[StandInHost, Any, list[str], list[str]]:
    """A started host with one running job task per family (added at virtual time 0), the session on the wall clock and the ARNs of the tasks."""
    host, session, lines = refresh.running()
    arns = [host.add_task(family) for family in families]
    on_wall_time(host, session)
    return host, session, lines, arns


def listing_times(calls: list[dict[str, Any]]) -> list[float]:
    """The virtual times of the job task listings the calls hold (each listing reads the tasks with desired status RUNNING with ``--no-paginate``)."""
    return [
        entry["at"]
        for entry in calls
        if entry["operation"] == "list-tasks" and "--no-paginate" in entry["options"] and entry["options"]["--desired-status"] == ["RUNNING"]
    ]


def listings(calls: list[dict[str, Any]]) -> int:
    """How many job task listings the calls hold."""
    return len(listing_times(calls))


def host_is_on(host: StandInHost) -> bool:
    return host.desired == 1 and host.instance_up_at is not None


def stop_with_no_job_task_proceeds_as_before() -> dict[str, bool]:
    host, session, lines, _ = scene()
    host.add_task("other_dev_job_read_silver")
    host.add_task(refresh.SERVICES["polaris"], group=f"service:{refresh.SERVICES['polaris']}")
    count = len(host.log)
    session.stop()
    calls = refresh.after(host, count)
    operations = [entry["operation"] for entry in calls]
    first_write = next(index for index, entry in enumerate(calls) if entry["operation"] in {"update-service", "set-desired-capacity"})
    return {
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
        "the job tasks are listed before the catalog service or the host is touched": listings(calls[:first_write]) == 1,
        "the check says that no job task runs": any(line.startswith("job_tasks") and "no job task" in line for line in lines),
        "service tasks and the tasks of another project do not count": "set-desired-capacity" in operations,
        "the catalog stops first, then the host": refresh.writes(calls) == ["update-service", "set-desired-capacity"],
        "stop never stops a task itself": "stop-task" not in operations,
    }


def stop_refuses_while_a_job_task_runs() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, family in (("a reader", READER), ("a writer", WRITER)):
        host, session, lines, arns = scene(family)
        host.advance(125)
        count = len(host.log)
        message = refresh.failure(session.stop)
        shown = [line for line in lines if line.startswith("job_task")]
        checks |= {
            f"{label}: stop is refused": message is not None and message.startswith("stop refused"),
            f"{label}: the reason names the family, the ID suffix and the run time": message is not None
            and family in message
            and arns[0][-8:] in message
            and "2 min 05 s" in message,
            f"{label}: each task is printed with its family and how long it has run": any(family in line and "2 min 05 s" in line for line in shown),
            f"{label}: the reason names the wait and the force option": message is not None and "--wait-seconds" in message and "--force-stop" in message,
            f"{label}: nothing is changed, the catalog service and the host stay on": not refresh.writes(refresh.after(host, count)) and host_is_on(host),
            f"{label}: the host is not reported off": not any(line.startswith("host_off") for line in lines),
            f"{label}: the output names no ARN": not any("arn:aws" in line for line in lines) and "arn:aws" not in (message or ""),
        }
    host, session, lines, _ = scene(READER, WRITER, f"{refresh.JOB_PREFIX}read_gold")
    host.advance(3725)
    message = refresh.failure(session.stop)
    checks["every running task is named and a long run time is shown in hours"] = (
        message is not None and "3 job task(s)" in message and "1 h 02 min" in message and all(name in message for name in (READER, WRITER))
    )
    host, session, _, _ = scene(READER)
    host.faults.add("no_started_at")
    message = refresh.failure(session.stop)
    checks["a task without a start time still refuses and says its run time is unknown"] = (
        message is not None and "run time unknown" in message and host_is_on(host)
    )
    return checks


def stop_waits_and_then_proceeds_when_the_jobs_finish() -> dict[str, bool]:
    host, session, lines, arns = scene(READER, WRITER)
    count, begin = len(host.log), host.now
    host.tasks[arns[0]]["stops_at"] = begin + 100
    host.tasks[arns[1]]["stops_at"] = begin + 190
    session.stop(wait_seconds=600)
    calls = refresh.after(host, count)
    times = listing_times(calls)
    first_write = next(index for index, entry in enumerate(calls) if entry["operation"] in {"update-service", "set-desired-capacity"})
    return {
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
        "stop polled the listing more than twice": len(times) > 2,
        "the last listing, the one that found no task, came after the last task ended": begin + 190 <= times[-1] < begin + 190 + POLL,
        "no catalog or host change came before that listing": listings(calls[:first_write]) == len(times),
        "the wait stayed inside its deadline": times[-1] - begin <= 600,
        "stop never stopped a task itself": all(entry["operation"] != "stop-task" for entry in calls),
        "the tasks that run are printed once and the end is reported": sum(1 for line in lines if line.startswith("job_task")) >= 2
        and any(line.startswith("job_tasks") and "no job task" in line for line in lines),
        "the wait reports its progress": any(line.startswith("stop_waiting") and "600 s" in line for line in lines),
        "the output names no ARN": not any("arn:aws" in line for line in lines),
    }


def stop_waits_and_then_refuses_at_the_deadline() -> dict[str, bool]:
    host, session, lines, _ = scene(READER)
    count, begin = len(host.log), host.now
    message = refresh.failure(partial(session.stop, wait_seconds=60))
    elapsed = host.now - begin
    return {
        "stop is refused": message is not None and message.startswith("stop refused"),
        "the reason names the task, the run time and the wait that passed": message is not None
        and READER in message
        and "60 s" in message
        and "1 min 00 s" in message,
        "the wait lasted its deadline and no longer": 60 <= elapsed <= 60 + POLL,
        "the listing was polled more than once": listings(refresh.after(host, count)) > 2,
        "the catalog service and the host were never touched": not refresh.writes(refresh.after(host, count)) and host_is_on(host),
    }


def stop_force_skips_the_check() -> dict[str, bool]:
    host, session, lines, _ = scene(READER, WRITER)
    count = len(host.log)
    session.stop(force_stop=True)
    calls = refresh.after(host, count)
    return {
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
        "no job task listing is made": listings(calls) == 0,
        "the report says that the check was skipped and why": any(line.startswith("stop_forced") and "--force-stop" in line for line in lines),
        "no task is stopped by the tool": all(entry["operation"] != "stop-task" for entry in calls),
        "the catalog stops first, then the host": refresh.writes(calls) == ["update-service", "set-desired-capacity"],
    }


def fail_api(host: StandInHost) -> None:
    host.faults.add("api_error")


def throttle_once(host: StandInHost) -> None:
    host.throttled = 1


def hang_listings(host: StandInHost) -> None:
    host.latency = 200


def stop_refuses_when_the_listing_fails() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, fail in (("a failing API", fail_api), ("a throttled API", throttle_once), ("a hung listing", hang_listings)):
        host, session, lines, _ = scene(READER)
        fail(host)
        count, begin = len(host.log), host.now
        message = refresh.failure(session.stop)
        calls = refresh.after(host, count)
        checks |= {
            f"{label}: stop is refused": message is not None and message.startswith("stop refused"),
            f"{label}: the reason says that the job tasks could not be listed and names the force option": message is not None
            and "could not be listed" in message
            and "--force-stop" in message,
            f"{label}: an unreadable listing is never read as no job task": not refresh.writes(calls) and host_is_on(host),
            f"{label}: the check ended inside the listing limit": host.now - begin <= 125,
            f"{label}: nothing is reported as no job task": not any("no job task" in line for line in lines),
        }
    host, session, _, _ = scene(READER)
    host.throttled = 3
    count = len(host.log)
    message = refresh.failure(partial(session.stop, wait_seconds=600))
    checks["with a wait, three failed listings in a row refuse before the deadline"] = (
        message is not None and "could not be listed" in message and host_is_on(host) and not refresh.writes(refresh.after(host, count))
    )
    host, session, _, arns = scene(READER)
    host.tasks[arns[0]]["stops_at"] = host.now + 40
    host.throttled = 1
    session.stop(wait_seconds=600)
    checks["with a wait, one failed listing is retried and the stop then goes on"] = host.desired == 0 and host.instance_up_at is None
    host, session, _, _ = scene()
    session.job_prefix = None
    message = refresh.failure(session.stop)
    checks["a session without a job family prefix refuses and changes nothing"] = message is not None and "prefix" in message and host_is_on(host)
    return checks


def stop_retries_a_transient_describe_failure() -> dict[str, bool]:
    host, session, lines, arns = scene(READER)
    host.describe_task_failures = 1
    host.tasks[arns[0]]["stops_at"] = host.now + 20
    count = len(host.log)
    session.stop(wait_seconds=60)
    off = next(entry for entry in refresh.after(host, count) if entry["operation"] == "set-desired-capacity")
    other, single, _, _ = scene(READER)
    other.describe_task_failures = 1
    message = refresh.failure(single.stop)
    return {
        "a describe failure that is not a missing task is retried and the stop then goes on": host.desired == 0 and host.instance_up_at is None,
        "the host went off only after the task ended, not after the failed listing": off["at"] >= 20,
        "without a wait the same failure refuses by name and the host stays on": message is not None
        and message.startswith("stop refused")
        and "could not be listed" in message
        and host_is_on(other),
    }


def stop_interrupted_during_the_wait_leaves_the_host_on() -> dict[str, bool]:
    module = refresh.module()
    checks: dict[str, bool] = {}
    for name, raised in (("SIGINT", KeyboardInterrupt), ("SIGTERM", module.TerminationRequested)):
        host, session, lines, arns = scene(READER)
        slept = [0]

        def interrupting(
            seconds: float, host: StandInHost = host, slept: list[int] = slept, raised: type[BaseException] = raised, signal_name: str = name
        ) -> None:
            slept[0] += 1
            if slept[0] == 2:
                raise raised(signal_name)
            host.advance(seconds)

        on_wall_time(host, session, interrupting)
        count = len(host.log)
        caught: str | None = None
        try:
            session.stop(wait_seconds=600)
        except (KeyboardInterrupt, module.TerminationRequested) as error:
            caught = type(error).__name__
        checks |= {
            f"a {name} during the wait propagates": caught == raised.__name__,
            f"a {name} during the wait leaves the catalog service and the host on": host_is_on(host) and not refresh.writes(refresh.after(host, count)),
            f"a {name} during the wait says that the host is still on and nothing was stopped": any(
                line.startswith("stop_interrupted") and "host is still on" in line and "run stop again" in line for line in lines
            ),
        }
        host.tasks[arns[0]]["stops_at"] = host.now
        session.clock = replace(session.clock, sleep=host.advance)
        session.stop(wait_seconds=600)
        checks[f"after a {name} during the wait, running stop again turns the host off"] = host.desired == 0 and host.instance_up_at is None
    return checks


def a_failed_command_still_stops_the_host_while_a_job_task_runs() -> dict[str, bool]:
    host, session, lines = refresh.build({"exit_code:host_prepare=2"})
    host.add_task(READER)
    message = refresh.failure(session.start)
    return {
        "the start fails": message is not None,
        "the host is stopped and verified although a job task is listed as running": host.desired == 0 and host.instance_up_at is None,
        "the stop after a failure makes no job task listing": listings(host.log) == 0,
        "the host off is reported": any(line.startswith("host_off_verified") for line in lines),
    }


def a_task_start_time_is_read_in_every_form_the_cli_shows() -> dict[str, bool]:
    from scripts.infrastructure.aws_tasks import started_epoch

    return {
        "an ISO time with an offset": started_epoch("2026-11-17T12:30:00.250000-08:00") == 1794947400.25,
        "an ISO time in UTC with Z": started_epoch("2026-11-17T20:30:00Z") == 1794947400.0,
        "a number of epoch seconds": started_epoch(1794947400) == 1794947400.0 and started_epoch(1794947400.5) == 1794947400.5,
        "a missing value (a task that has not started)": started_epoch(None) is None,
        "text that is not a time": started_epoch("yesterday") is None,
        "a boolean is no time": started_epoch(True) is None,
    }


def stop_command_line() -> dict[str, bool]:
    host_session = refresh.module()
    called: list[dict[str, Any]] = []
    outcome: dict[str, Any] = {"raise": None}

    class Recorder:
        def __init__(self, *_: object) -> None:
            pass

        def stop(self, wait_seconds: int = 0, force_stop: bool = False) -> None:
            called.append({"wait_seconds": wait_seconds, "force_stop": force_stop})
            if outcome["raise"] is not None:
                raise outcome["raise"]

    def exit_code(argv: list[str]) -> int:
        try:
            return int(host_session.main(argv))
        except SystemExit as error:
            return int(error.code or 0)

    refused = {
        "a negative wait": ["stop", "--wait-seconds", "-1"],
        "a wait that is not a number": ["stop", "--wait-seconds", "soon"],
        "a wait with the force option": ["stop", "--wait-seconds", "60", "--force-stop"],
        "the keep-on option, which stop does not have": ["stop", "--keep-on"],
    }
    with mock.patch.object(host_session, "open_session", Recorder), mock.patch.object(host_session, "install_termination_handler", lambda: lambda: None):
        codes = {name: exit_code(argv) for name, argv in refused.items()}
        refused_calls = len(called)
        plain = exit_code(["stop"])
        waited = exit_code(["stop", "--wait-seconds", "600"])
        forced = exit_code(["stop", "--force-stop"])
        outcome["raise"] = host_session.HostSessionError("stop refused: a job task is running")
        refusal = exit_code(["stop"])
        outcome["raise"] = KeyboardInterrupt()
        interrupted = exit_code(["stop", "--wait-seconds", "600"])
    return {
        **{f"{name} is a usage error": code == 2 for name, code in codes.items()},
        "a refused option opens no session": refused_calls == 0,
        "stop without options refuses at once and does not force": plain == 0 and called[0] == {"wait_seconds": 0, "force_stop": False},
        "the wait reaches stop in seconds": waited == 0 and called[1] == {"wait_seconds": 600, "force_stop": False},
        "the force option reaches stop": forced == 0 and called[2] == {"wait_seconds": 0, "force_stop": True},
        "a refusal exits 1": refusal == 1,
        "an interrupt during the wait exits 1": interrupted == 1,
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "stop_with_no_job_task_proceeds_as_before": stop_with_no_job_task_proceeds_as_before,
    "stop_refuses_while_a_job_task_runs": stop_refuses_while_a_job_task_runs,
    "stop_waits_and_then_proceeds_when_the_jobs_finish": stop_waits_and_then_proceeds_when_the_jobs_finish,
    "stop_waits_and_then_refuses_at_the_deadline": stop_waits_and_then_refuses_at_the_deadline,
    "stop_force_skips_the_check": stop_force_skips_the_check,
    "stop_refuses_when_the_listing_fails": stop_refuses_when_the_listing_fails,
    "stop_retries_a_transient_describe_failure": stop_retries_a_transient_describe_failure,
    "stop_interrupted_during_the_wait_leaves_the_host_on": stop_interrupted_during_the_wait_leaves_the_host_on,
    "a_failed_command_still_stops_the_host_while_a_job_task_runs": a_failed_command_still_stops_the_host_while_a_job_task_runs,
    "a_task_start_time_is_read_in_every_form_the_cli_shows": a_task_start_time_is_read_in_every_form_the_cli_shows,
    "stop_command_line": stop_command_line,
}
