"""Host session cases for the settle of launches by a host stop (owner decision A, Nov 24 2026): ``stop``, ``stop_after_failure`` and ``host_on`` end every
claimed launch that has no end record as ``cancelled`` once the host is shown stopped, so a launch whose task ECS no longer describes stops blocking its family.

The stand-in host here keeps the launch records as S3 objects (``s3api get-object``, ``put-object`` with ``--if-none-match``, and
``list-objects-v2``) over the data bucket, as the real store sees them through the AWS CLI. Time is virtual: a claim is old or young by the virtual time
between its ``at`` and the session's wall clock.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from orchestration.ecs_launch import launch_record_prefix
from scripts.infrastructure import host_session_restore_cases as restore
from scripts.infrastructure.host_session import Session, TerminationRequested
from scripts.infrastructure.host_session_stand_in import ACCOUNT_MARK, EPOCH, OPENMETADATA_SERVICE, StandInError
from scripts.infrastructure.host_session_stop_cases import on_wall_time
from scripts.process import CompletedProcess, LaunchesNotStopped

PREFIX = launch_record_prefix()
JOB_PREFIX = "stand_in_dev_job_"
RUN = "gold_abcdef01"
OTHER_RUN = "silver_abcdef02"
JOBS = ("gold", "silver")
SAVED_TASK = f"{ACCOUNT_MARK}task/stand-in-cluster/aabbccddeeff00112233445566778899"
OLD = 7200.0  # a claim two hours old: past the send window plus the margin
YOUNG = 900.0  # inside the 20 minutes
CASE_ERRORS = (*restore.CASE_ERRORS, KeyError, RuntimeError)


class LaunchHost(restore.RecordHost):
    """The record host with conditional writes, listing and an ETag per object, and the launch records of the cases.

    ``faults`` may also hold ``ghost_container_instance`` (ECS lists an ACTIVE container instance that the group does not show), ``group_read_fails_when_off``
    (the host group cannot be read once it shows no instance), ``slow_down_end`` (the next write of an end record answers SlowDown) and ``store_denied``
    (every launch record call answers AccessDenied), ``host_up_on_records`` (the host group comes up when a started record is read, as another session's
    start does), ``not_stopped_on_list`` (the AWS runner raises ``LaunchesNotStopped`` for a record listing) and ``interrupt_in_cleanup`` (a record listing
    receives SIGTERM once the capacity was raised: the cleanup after a failed start), ``interrupt_in_settle`` (a record listing receives SIGTERM),
    ``not_stopped_from_signal`` and ``not_stopped_from_termination`` (the AWS runner raises ``LaunchesNotStopped`` chained from a SIGINT or a SIGTERM),
    ``no_task_arns_key``, ``no_instances_key`` and ``empty_definition`` (a successful ECS reply that lacks the task list or the container instance list, or
    describes a task with an empty task definition and group)."""

    def __init__(self, faults: set[str] | None = None) -> None:
        super().__init__(faults, None, False, False, checkout=restore.SHA_OLD, code_run=restore.RUN_OLD, openmetadata=True, catalog_setup=True)

    raised = False
    mangle: dict[tuple[str, str], Callable[[dict[str, Any]], dict[str, Any]]] = {}

    def runner(self, argv: list[str], **kwargs: Any) -> Any:
        result = self.answer(argv, **kwargs)
        change = self.mangle.get((argv[1], argv[2]))
        if change is None or result.returncode != 0:
            return result
        return CompletedProcess(result.args, 0, json.dumps(change(json.loads(result.stdout))), result.stderr)

    def answer(self, argv: list[str], **kwargs: Any) -> Any:
        if argv[2] == "list-objects-v2":
            for fault, signal_error in (("not_stopped_from_signal", KeyboardInterrupt()), ("not_stopped_from_termination", TerminationRequested("SIGTERM"))):
                if fault in self.faults:
                    try:
                        raise signal_error
                    except (KeyboardInterrupt, TerminationRequested) as caught:
                        raise LaunchesNotStopped("a stand-in stop was not confirmed") from caught
        if "not_stopped_on_list" in self.faults and argv[2] == "list-objects-v2":
            raise LaunchesNotStopped("a stand-in stop was not confirmed")
        return super().runner(argv, **kwargs)

    def autoscaling_set_desired_capacity(self, options: dict[str, list[str]]) -> dict[str, Any]:
        self.raised = self.raised or options["--desired-capacity"] == ["1"]
        return super().autoscaling_set_desired_capacity(options)

    def launch_key(self, run: str, job: str, *parts: str) -> str:
        return "/".join((PREFIX, run, job, *parts))

    def add_launch(self, run: str, job: str, age: float, saved: bool = False, send_token: bool = False, ended: bool = False) -> None:
        """A claimed launch, its family index entry and optional records, claimed ``age`` virtual seconds before the wall clock now."""
        at = (EPOCH.timestamp() + self.now) - age
        stamp = EPOCH.fromtimestamp(at, EPOCH.tzinfo).isoformat()
        base = {"schema_version": 1, "run_id": run, "job": job, "attempt": 1, "at": stamp}
        claim = {**base, "event": "claim", "bound": {"code_digest": "a" * 64, "request_hash": "b" * 64, "input_run": "silver_12345678"}}
        claim |= {"deadline_seconds": 3600, "acceptance": ["verify"], "need": {"cpu_units": 1, "memory_mib": 1, "scratch_bytes": 1}}
        self.objects[f"{PREFIX}/families/{job}/{run}.json"] = json.dumps({**base, "event": "family"})
        self.objects[self.launch_key(run, job, "claim.json")] = json.dumps(claim)
        if saved:
            self.objects[self.launch_key(run, job, "01", "intent.json")] = json.dumps({**claim, "event": "intent"})
            self.objects[self.launch_key(run, job, "01", "started.json")] = json.dumps(
                {**base, "event": "started", "task_arn": SAVED_TASK, "job_started_at": stamp}
            )
        if send_token:
            self.objects[self.launch_key(run, job, "decision.json")] = json.dumps({**base, "event": "decision", "decision": "send", "holder": "gone"})
        if ended:
            end = {**base, "event": "end", "outcome": "succeeded", "exit_code": 0, "reason": "done", "task_arn": ""}
            self.objects[self.launch_key(run, job, "01", "end.json")] = json.dumps(end)

    def end_of(self, run: str, job: str) -> dict[str, Any] | None:
        text = self.objects.get(self.launch_key(run, job, "01", "end.json"))
        return None if text is None else dict(json.loads(text))

    def store_calls(self) -> int:
        return sum(1 for entry in self.log if entry["service"] == "s3api" and any(PREFIX in str(value) for value in entry["options"].get("--key", [])))

    def store_writes(self) -> int:
        return sum(1 for entry in self.log if entry["service"] == "s3api" and entry["operation"] == "put-object" and "--if-none-match" in entry["options"])

    def s3_get_object(self, options: dict[str, list[str]]) -> dict[str, Any]:
        key = options["--key"][0]
        if "store_denied" in self.faults and key.startswith(PREFIX):
            raise StandInError("AccessDenied")
        if "host_up_on_records" in self.faults and key.endswith("/started.json"):
            self.faults.discard("host_up_on_records")
            self.desired, self.instance_up_at = 1, self.now
        answer = super().s3_get_object(options)
        return {**answer, "ETag": f'"{hash(self.objects[key]) & 0xFFFF:x}"'}

    def s3_put_object(self, options: dict[str, list[str]]) -> dict[str, Any]:
        key = options["--key"][0]
        if key.startswith(PREFIX):
            if "store_denied" in self.faults:
                raise StandInError("AccessDenied")
            if key.endswith("/end.json") and "slow_down_end" in self.faults:
                self.faults.discard("slow_down_end")
                raise StandInError("SlowDown")
            if options.get("--if-none-match") != ["*"]:
                raise StandInError("AccessDenied")  # an unconditional write under the launch prefix is refused, as the IAM policy of the role refuses it
            if "--if-none-match" in options and key in self.objects:
                raise StandInError("PreconditionFailed")
        return super().s3_put_object(options)

    def s3_list_objects(self, options: dict[str, list[str]]) -> dict[str, Any]:
        prefix = options["--prefix"][0]
        if "interrupt_in_settle" in self.faults or ("interrupt_in_cleanup" in self.faults and self.raised):
            raise TerminationRequested("SIGTERM")
        if "store_denied" in self.faults:
            raise StandInError("AccessDenied")
        return {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(prefix)], "IsTruncated": False}

    def ecs_list_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        return {} if "no_task_arns_key" in self.faults else super().ecs_list_tasks(options)

    def ecs_describe_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        answer = super().ecs_describe_tasks(options)
        if "empty_definition" in self.faults:
            for task in answer.get("tasks", []):
                task["taskDefinitionArn"], task["group"] = "", ""
        return answer

    def ecs_list_container_instances(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if "no_instances_key" in self.faults:
            return {}
        answer = super().ecs_list_container_instances(options)
        if "ghost_container_instance" in self.faults and not answer["containerInstanceArns"]:
            return {"containerInstanceArns": [f"{ACCOUNT_MARK}container-instance/ghost"]}
        return answer

    def autoscaling_describe_auto_scaling_groups(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if "group_read_fails_when_off" in self.faults and self.desired == 0 and self.instance_up_at is None and self.settling:
            raise StandInError("ThrottlingException")
        return super().autoscaling_describe_auto_scaling_groups(options)

    settling = False


def build(faults: set[str] | None = None) -> tuple[LaunchHost, Session, list[str]]:
    """A started host, a session on the wall clock of the stand-in with the launch settle switched on for the jobs ``JOBS``, and its report lines."""
    from scripts.infrastructure.host_launch_settle import HostLaunchSettle

    host = LaunchHost(faults)
    host.handlers[("s3api", "list-objects-v2")] = host.s3_list_objects
    _, session, lines = restore.build(host=host, faults=faults)
    session.job_prefix = JOB_PREFIX
    on_wall_time(host, session)
    session.launch_settle = HostLaunchSettle(session, lambda: JOBS)
    session.start()
    lines.clear()
    host.log.clear()
    return host, session, lines


def stopped(host: LaunchHost) -> bool:
    return host.desired == 0 and host.instance_up_at is None


def line_with(lines: list[str], prefix: str) -> str:
    return next((line for line in lines if line.startswith(prefix)), "")


def stop_settles_a_launch_whose_saved_task_ecs_forgot() -> dict[str, bool]:
    host, session, lines = build()
    host.add_launch(RUN, "gold", OLD, saved=True)
    session.stop()
    end = host.end_of(RUN, "gold")
    shown = line_with(lines, "launch_settled")
    off = next(index for index, line in enumerate(lines) if line.startswith("host_off_verified"))
    return {
        "the host is off and verified": stopped(host),
        "the launch has a cancelled end record that keeps the saved task": end is not None and end["outcome"] == "cancelled" and end["task_arn"] == SAVED_TASK,
        "the reason names the host stop": end is not None and "host stopped" in end["reason"],
        "the report names run, job and attempt": RUN in shown and "gold" in shown and "attempt 1" in shown,
        "the report shows the end of the task ID, never an ARN": SAVED_TASK[-8:] in shown and not any("arn:aws" in line for line in lines),
        "the settle comes after the host-off is verified": lines.index(shown) > off,
        "a second settle writes nothing": again(session, host),
    }


def rerun(session: Session, point: str) -> None:
    """Run the settle of the session again, as a later stop or start does."""
    if session.launch_settle is None:
        raise RuntimeError("the session has no launch settle")
    session.launch_settle.run(point)


def again(session: Session, host: LaunchHost) -> bool:
    before = host.store_writes()
    rerun(session, "stop")
    return host.store_writes() == before


def stop_after_failure_settles_and_keep_on_does_not() -> dict[str, bool]:
    host, session, lines = build()
    host.add_launch(RUN, "gold", OLD, saved=True)
    session.stop_after_failure("start", keep_on=True)
    kept = host.end_of(RUN, "gold") is None and host.desired == 1 and not host.store_calls()
    session.stop_after_failure("start", keep_on=False)
    end = host.end_of(RUN, "gold")
    return {
        "a host that is kept on settles nothing": kept,
        "a cleanup that stops the host settles the launch": stopped(host) and end is not None and end["outcome"] == "cancelled",
        "the cleanup outcome is still the verified host-off": session.cleanup_outcome == "host_off_verified",
    }


def a_launch_with_a_possible_send_waits_for_the_window_and_margin() -> dict[str, bool]:
    host, session, lines = build()
    host.add_launch(RUN, "gold", YOUNG, send_token=True)
    session.stop()
    held = host.end_of(RUN, "gold") is None
    shown = line_with(lines, "launch_unresolved")
    host.advance(1000)  # the claim is now older than the send window plus the margin
    rerun(session, "start")
    end = host.end_of(RUN, "gold")
    return {
        "the host-off was verified and reported": stopped(host) and any(line.startswith("host_off_verified") for line in lines),
        "a claim inside the window and margin is not ended": held,
        "it is reported by run, job and attempt with the reason": RUN in shown and "gold" in shown and "attempt 1" in shown and "1200" in shown,
        "the retry after the margin cancels it": end is not None and end["outcome"] == "cancelled" and end["task_arn"] == "",
    }


def a_host_that_is_not_empty_settles_nothing() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, faults in (("the group shows an instance", set()), ("ECS lists an ACTIVE container instance", {"ghost_container_instance"})):
        host, session, lines = build(faults)
        host.add_launch(RUN, "gold", OLD, saved=True)
        if not faults:
            rerun(session, "stop")  # the host is on: the group shows desired capacity 1 and an instance
        else:
            session.stop()
        checks[f"{label}: no end record"] = host.end_of(RUN, "gold") is None
        checks[f"{label}: nothing was written to the launch records"] = host.store_writes() == 0
    host, session, lines = build({"group_read_fails_when_off"})
    session.stop()
    host.add_launch(RUN, "gold", OLD, saved=True)
    host.settling = True
    lines.clear()
    rerun(session, "stop")
    checks["a group read that fails after the host-off: nothing is written and the failure is reported"] = (
        stopped(host) and host.end_of(RUN, "gold") is None and host.store_writes() == 0 and any(line.startswith("launch_settle_failed") for line in lines)
    )
    return checks


def an_unstopped_task_of_the_family_holds_only_that_family() -> dict[str, bool]:
    host, session, lines = build()
    host.add_launch(RUN, "gold", OLD, saved=True)
    host.add_launch(OTHER_RUN, "silver", OLD, send_token=True)
    host.add_task(f"{JOB_PREFIX}gold_host")
    session.stop(force_stop=True)
    other = host.end_of(OTHER_RUN, "silver")
    return {
        "the host is off": stopped(host),
        "the family with a task that is not stopped keeps its launch": host.end_of(RUN, "gold") is None and bool(line_with(lines, "launch_unresolved")),
        "the other family is settled": other is not None and other["outcome"] == "cancelled",
    }


def an_ended_launch_is_never_touched() -> dict[str, bool]:
    host, session, lines = build()
    host.add_launch(RUN, "gold", OLD, saved=True, ended=True)
    before = dict(host.objects)
    session.stop()
    return {
        "the host is off": stopped(host),
        "no launch record changed": host.objects == before,
        "nothing is reported for it": not line_with(lines, "launch_settled") and not line_with(lines, "launch_unresolved"),
    }


def a_record_store_failure_never_stops_the_host_off() -> dict[str, bool]:
    host, session, lines = build({"slow_down_end"})
    host.add_launch(RUN, "gold", OLD, saved=True)
    session.stop()
    failed = line_with(lines, "launch_settle_failed")
    held = host.end_of(RUN, "gold") is None
    rerun(session, "stop")
    end = host.end_of(RUN, "gold")
    closed, closed_session, shown = build({"store_denied"})
    closed.add_launch(RUN, "gold", OLD, saved=True)
    closed_session.stop()
    return {
        "the host-off is verified and reported": stopped(host) and any(line.startswith("host_off_verified") for line in lines),
        "the failure is reported with the job": "silver" in failed or "gold" in failed,
        "no end record was written": held,
        "the retry settles the launch": end is not None and end["outcome"] == "cancelled",
        "a store that denies every call still lets the host off": stopped(closed) and any(line.startswith("launch_settle_failed") for line in shown),
        "an incomplete scan is reported as incomplete": any(line.startswith("launch_settle_incomplete") for line in shown),
    }


def host_on_settles_before_it_raises_the_capacity() -> dict[str, bool]:
    host, session, lines = build()
    session.stop()
    host.add_launch(RUN, "gold", OLD, saved=True)
    count = len(host.log)
    session.host_on()
    calls = host.log[count:]
    end = host.end_of(RUN, "gold")
    write = next((index for index, entry in enumerate(calls) if entry["operation"] == "put-object" and "--if-none-match" in entry["options"]), None)
    raise_capacity = next(index for index, entry in enumerate(calls) if entry["operation"] == "set-desired-capacity")
    return {
        "the stale launch is cancelled": end is not None and end["outcome"] == "cancelled",
        "the record was written before the capacity was raised": write is not None and write < raise_capacity,
        "the host is on": host.desired == 1,
    }


def host_on_with_an_instance_settles_nothing() -> dict[str, bool]:
    host, session, lines = build()
    host.add_launch(RUN, "gold", OLD, saved=True)
    session.host_on()
    return {
        "no launch record was read or written": host.store_calls() == 0 and host.end_of(RUN, "gold") is None,
        "no settle line is reported": not any(line.startswith("launch_") for line in lines),
    }


def a_session_without_the_settle_makes_no_record_call() -> dict[str, bool]:
    host, session, lines = build()
    session.launch_settle = None
    host.add_launch(RUN, "gold", OLD, saved=True)
    session.stop()
    return {"the host is off": stopped(host), "no launch record call": host.store_calls() == 0}


def an_unreadable_data_bucket_name_skips_the_settle() -> dict[str, bool]:
    host, session, lines = build()
    session.names = replace(session.names, code_record_bucket="")
    host.add_launch(RUN, "gold", OLD, saved=True)
    session.stop()
    return {
        "the host is off": stopped(host),
        "the skip is reported": bool(line_with(lines, "launch_settle_skipped")),
        "no launch record call": host.store_calls() == 0,
    }


def after_stop(faults: set[str] | None = None) -> tuple[LaunchHost, Session, list[str]]:
    """A stopped host with a stale launch, and the faults armed after the host-off."""
    host, session, lines = build()
    session.stop()
    host.add_launch(RUN, "gold", OLD, saved=True)
    host.faults |= faults or set()
    host.raised = False
    lines.clear()
    host.log.clear()
    return host, session, lines


def a_host_that_comes_up_before_the_end_write_stops_the_write() -> dict[str, bool]:
    host, session, lines = after_stop({"host_up_on_records"})
    rerun(session, "stop")
    return {
        "nothing was written to the launch records": host.store_writes() == 0 and host.end_of(RUN, "gold") is None,
        "the launch is reported unresolved with the host's state": "capacity" in line_with(lines, "launch_unresolved"),
    }


def a_listed_task_that_ecs_cannot_describe_holds_every_family() -> dict[str, bool]:
    host, session, lines = after_stop({"ghost_task"})
    rerun(session, "stop")
    return {
        "nothing was written to the launch records": host.store_writes() == 0 and host.end_of(RUN, "gold") is None,
        "the launch is reported unresolved": bool(line_with(lines, "launch_unresolved")),
    }


def a_runner_failure_never_stops_the_start() -> dict[str, bool]:
    host, session, lines = after_stop({"not_stopped_on_list"})
    session.host_on()
    return {
        "the capacity was raised": host.desired == 1 and host.raised,
        "the failure is reported as the runner's own error": "LaunchesNotStopped" in line_with(lines, "launch_settle_failed"),
    }


def a_signal_inside_a_runner_failure_stops_the_start() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for fault, expected in (("not_stopped_from_signal", KeyboardInterrupt), ("not_stopped_from_termination", TerminationRequested)):
        host, session, lines = after_stop({fault})
        raised = signal_raised(session.host_on)
        checks[f"{fault}: host_on raises the signal"] = isinstance(raised, expected)
        checks[f"{fault}: the capacity was not raised"] = host.desired == 0 and not host.raised
    return checks


def signal_raised(action: Callable[[], object]) -> BaseException | None:
    try:
        action()
    except (KeyboardInterrupt, TerminationRequested) as error:
        return error
    return None


def a_signal_during_the_settle_keeps_the_first_stop_error() -> dict[str, bool]:
    host, session, lines = build()
    session.names = replace(session.names, services={**session.names.services, "openmetadata": OPENMETADATA_SERVICE})
    host.add_launch(RUN, "gold", OLD, saved=True)
    host.faults |= {"interrupt_catalog_stop", "interrupt_in_settle"}
    lines.clear()
    raised = signal_raised(session.stop)
    return {
        "stop raises the first error, the interrupt of the catalog drain": isinstance(raised, KeyboardInterrupt),
        "the host-off was verified": stopped(host),
        "the interrupted settle is reported": bool(line_with(lines, "launch_settle_interrupted")),
    }


def incomplete_ecs_replies_never_authorize_a_cancel() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, fault, task in (
        ("a task list reply without the list", "no_task_arns_key", False),
        ("a container instance reply without the list", "no_instances_key", False),
        ("a running task with an empty definition and group", "empty_definition", True),
    ):
        host, session, lines = after_stop()
        if task:
            host.add_task(f"{JOB_PREFIX}gold_host")
        host.faults.add(fault)
        rerun(session, "stop")
        checks[f"{label}: nothing was written"] = host.store_writes() == 0 and host.end_of(RUN, "gold") is None
        checks[f"{label}: the launch stays unresolved or the failure is reported"] = bool(
            line_with(lines, "launch_unresolved") or line_with(lines, "launch_settle_failed")
        )
    return checks


def an_interrupt_in_the_cleanup_keeps_the_first_error() -> dict[str, bool]:
    host, session, lines = after_stop({"never_register", "interrupt_in_cleanup"})
    message = restore.failure(session.start)
    return {
        "start fails with its own error, not the signal": message is not None and "host registration" in message,
        "the host-off was verified": stopped(host) and session.cleanup_outcome == "host_off_verified",
        "the interrupted settle is reported": bool(line_with(lines, "launch_settle_interrupted")),
    }


def one_deadline_bounds_the_whole_settle() -> dict[str, bool]:
    host, session, lines = after_stop()
    host.latency = 100  # every describe and list call takes 100 virtual seconds, so the whole settle needs more than its 900
    begin = host.now
    rerun(session, "stop")
    return {
        "the settle stopped within its deadline": host.now - begin <= 900,
        "the failure is reported": bool(line_with(lines, "launch_settle_failed")),
        "nothing was written": host.store_writes() == 0,
    }


def edit(path: tuple[str | int, ...], value: Any = None, drop: bool = False) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """A change of one reply: set the value at the path, or drop the key at its end."""

    def change(reply: dict[str, Any]) -> dict[str, Any]:
        changed = copy.deepcopy(reply)
        target: Any = changed
        for step in path[:-1]:
            target = target[step]
        if drop:
            del target[path[-1]]
        else:
            target[path[-1]] = value
        return changed

    return change


def repeating_token(reply: dict[str, Any]) -> dict[str, Any]:
    return {**reply, "nextToken": "same"}


GROUP = ("autoscaling", "describe-auto-scaling-groups")
INSTANCES = ("ecs", "list-container-instances")
LISTING = ("ecs", "list-tasks")
DESCRIPTION = ("ecs", "describe-tasks")
ASG = ("AutoScalingGroups", 0)
TASK = ("tasks", 0)
MALFORMED: dict[tuple[str, str], dict[str, Callable[[dict[str, Any]], dict[str, Any]]]] = {
    GROUP: {
        "another group's name": edit((*ASG, "AutoScalingGroupName"), "another-host-group"),
        "group name omitted": edit((*ASG, "AutoScalingGroupName"), drop=True),
        "Instances omitted": edit((*ASG, "Instances"), drop=True),
        "Instances null": edit((*ASG, "Instances"), None),
        "Instances a string": edit((*ASG, "Instances"), "none"),
        "DesiredCapacity a string": edit((*ASG, "DesiredCapacity"), "0"),
        "DesiredCapacity a boolean": edit((*ASG, "DesiredCapacity"), False),
        "no group": edit(("AutoScalingGroups",), []),
        "two groups": lambda reply: {"AutoScalingGroups": reply["AutoScalingGroups"] * 2},
    },
    INSTANCES: {
        "list omitted": edit(("containerInstanceArns",), drop=True),
        "list null": edit(("containerInstanceArns",), None),
        "an entry that is no ARN": edit(("containerInstanceArns",), ["one"]),
        "token false": edit(("nextToken",), False),
        "token empty": edit(("nextToken",), ""),
        "token a number": edit(("nextToken",), 5),
        "token with more to read": edit(("nextToken",), "more"),
    },
    LISTING: {
        "list omitted": edit(("taskArns",), drop=True),
        "list null": edit(("taskArns",), None),
        "an entry that is no ARN": edit(("taskArns", 0), "task"),
        "token false": edit(("nextToken",), False),
        "token empty": edit(("nextToken",), ""),
        "token a number": edit(("nextToken",), 5),
        "token repeated": repeating_token,
    },
    DESCRIPTION: {
        "tasks not a list": edit(("tasks",), "none"),
        "tasks omitted": edit(("tasks",), drop=True),
        "failures omitted": edit(("failures",), drop=True),
        "failures not a list": edit(("failures",), "none"),
        "truncated task definition ARN": edit((*TASK, "taskDefinitionArn"), "arn:aws:ecs:stand-in:111111111111:task-definition/gold_host"),
        "task definition ARN omitted": edit((*TASK, "taskDefinitionArn"), drop=True),
        "group omitted": edit((*TASK, "group"), drop=True),
        "group null": edit((*TASK, "group"), None),
        "lastStatus unknown": edit((*TASK, "lastStatus"), "GONE"),
        "lastStatus omitted": edit((*TASK, "lastStatus"), drop=True),
        "taskArn not a listed ARN": edit((*TASK, "taskArn"), f"{ACCOUNT_MARK}task/stand-in-cluster/other"),
    },
}


def a_stopped_host_with_a_stopped_task() -> tuple[LaunchHost, Session, list[str]]:
    """A stopped host with a stale launch and one task that has stopped, so that every valid reply lets the settle write."""
    host, session, lines = after_stop()
    arn = host.add_task(f"{JOB_PREFIX}gold_host")
    host.tasks[arn]["status"], host.tasks[arn]["desired"] = "STOPPED", "STOPPED"
    return host, session, lines


def every_malformed_reply_stops_the_settle_with_no_write() -> dict[str, bool]:
    host, session, lines = a_stopped_host_with_a_stopped_task()
    rerun(session, "stop")
    checks: dict[str, bool] = {"control: the same host with valid replies settles the launch": host.end_of(RUN, "gold") is not None}
    for operation, variants in MALFORMED.items():
        for label, change in variants.items():
            host, session, lines = a_stopped_host_with_a_stopped_task()
            host.mangle = {operation: change}
            rerun(session, "stop")
            checks[f"{operation[1]}, {label}: nothing was written"] = host.store_writes() == 0 and host.end_of(RUN, "gold") is None
    return checks


def a_task_that_ecs_cannot_identify_is_reported_as_unverifiable() -> dict[str, bool]:
    host, session, lines = after_stop({"ghost_task"})
    rerun(session, "stop")
    shown = line_with(lines, "launch_unresolved")
    return {"the reason says the family could not be verified": "could not be verified" in shown and "not STOPPED" not in shown}


def an_unknown_family_holds_every_job() -> dict[str, bool]:
    host, session, lines = after_stop()
    host.add_task("stand_in_dev_other_thing")
    rerun(session, "stop")
    return {"nothing was written": host.store_writes() == 0 and host.end_of(RUN, "gold") is None}


def an_earlier_signal_never_relabels_a_later_failure() -> dict[str, bool]:
    host, session, lines = after_stop({"interrupt_run_task", "not_stopped_on_list"})
    raised = signal_raised(session.start)
    return {
        "start raises its own interrupt": isinstance(raised, KeyboardInterrupt),
        "the host-off was verified": stopped(host),
        "the later runner failure is reported as a failure, not as an interrupt": any(
            line.startswith("launch_settle_failed") and "LaunchesNotStopped" in line for line in lines
        )
        and not line_with(lines, "launch_settle_interrupted"),
    }


def first_page_only(change: Callable[[dict[str, Any]], dict[str, Any]]) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """A change of the first reply only; later replies pass unchanged."""
    calls = [0]

    def once(reply: dict[str, Any]) -> dict[str, Any]:
        calls[0] += 1
        return change(reply) if calls[0] == 1 else reply

    return once


def a_false_token_once_is_refused_by_its_shape() -> dict[str, bool]:
    host, session, lines = a_stopped_host_with_a_stopped_task()
    host.mangle = {LISTING: first_page_only(edit(("nextToken",), False))}
    rerun(session, "stop")
    return {
        "nothing was written": host.store_writes() == 0 and host.end_of(RUN, "gold") is None,
        "the failure names the invalid token": "invalid nextToken" in line_with(lines, "launch_settle_failed"),
    }


def a_project_service_task_holds_no_job() -> dict[str, bool]:
    host, session, lines = after_stop()
    service = next(iter(session.names.services.values()))
    arn = host.add_task(f"{service}_task", group=f"service:{service}")
    host.tasks[arn]["status"] = "DEPROVISIONING"
    rerun(session, "stop")
    return {"the launch is settled": host.end_of(RUN, "gold") is not None}


def a_task_of_an_unknown_service_holds_every_job() -> dict[str, bool]:
    host, session, lines = after_stop()
    host.add_task("stand_in_dev_other_service", group="service:not-a-project-service")
    rerun(session, "stop")
    return {"nothing was written": host.store_writes() == 0 and host.end_of(RUN, "gold") is None}


def a_deleted_task_holds_no_job() -> dict[str, bool]:
    host, session, lines = after_stop()
    arn = host.add_task(f"{JOB_PREFIX}gold_host")
    host.tasks[arn]["status"], host.tasks[arn]["desired"] = "DELETED", "STOPPED"
    rerun(session, "stop")
    return {"the launch is settled": host.end_of(RUN, "gold") is not None}


def the_reason_names_only_active_container_instances() -> dict[str, bool]:
    host, session, lines = after_stop()
    rerun(session, "stop")
    end = host.end_of(RUN, "gold")
    reason = str(end.get("reason", "")) if end else ""
    return {"the end reason says no ACTIVE container instance": "no ACTIVE container instance" in reason}


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "host_stop_settles_a_launch_whose_saved_task_ecs_forgot": stop_settles_a_launch_whose_saved_task_ecs_forgot,
    "host_stop_after_failure_settles_and_keep_on_does_not": stop_after_failure_settles_and_keep_on_does_not,
    "host_stop_holds_a_possible_send_for_the_window_and_margin": a_launch_with_a_possible_send_waits_for_the_window_and_margin,
    "host_stop_settles_nothing_on_a_host_that_is_not_empty": a_host_that_is_not_empty_settles_nothing,
    "host_stop_holds_only_the_family_with_an_unstopped_task": an_unstopped_task_of_the_family_holds_only_that_family,
    "host_stop_never_touches_an_ended_launch": an_ended_launch_is_never_touched,
    "host_stop_record_store_failure_never_stops_the_host_off": a_record_store_failure_never_stops_the_host_off,
    "host_on_settles_before_it_raises_the_capacity": host_on_settles_before_it_raises_the_capacity,
    "host_on_with_an_instance_settles_nothing": host_on_with_an_instance_settles_nothing,
    "host_session_without_the_settle_makes_no_record_call": a_session_without_the_settle_makes_no_record_call,
    "host_stop_stops_the_write_when_the_host_comes_up_first": a_host_that_comes_up_before_the_end_write_stops_the_write,
    "host_stop_holds_every_family_for_a_task_ecs_cannot_describe": a_listed_task_that_ecs_cannot_describe_holds_every_family,
    "host_on_goes_on_after_any_settle_failure": a_runner_failure_never_stops_the_start,
    "host_stop_cleanup_interrupt_keeps_the_start_error": an_interrupt_in_the_cleanup_keeps_the_first_error,
    "host_stop_settle_has_one_deadline": one_deadline_bounds_the_whole_settle,
    "host_stop_every_malformed_reply_stops_the_settle_with_no_write": every_malformed_reply_stops_the_settle_with_no_write,
    "host_stop_reports_a_task_ecs_cannot_identify_as_unverifiable": a_task_that_ecs_cannot_identify_is_reported_as_unverifiable,
    "host_stop_unknown_family_holds_every_job": an_unknown_family_holds_every_job,
    "host_stop_earlier_signal_never_relabels_a_later_failure": an_earlier_signal_never_relabels_a_later_failure,
    "host_on_never_hides_a_signal_inside_a_runner_failure": a_signal_inside_a_runner_failure_stops_the_start,
    "host_stop_signal_during_the_settle_keeps_the_first_error": a_signal_during_the_settle_keeps_the_first_stop_error,
    "host_stop_incomplete_ecs_replies_never_authorize_a_cancel": incomplete_ecs_replies_never_authorize_a_cancel,
    "host_stop_skips_the_settle_without_a_data_bucket": an_unreadable_data_bucket_name_skips_the_settle,
    "host_stop_refuses_a_false_token_on_the_first_page": a_false_token_once_is_refused_by_its_shape,
    "host_stop_project_service_task_holds_no_job": a_project_service_task_holds_no_job,
    "host_stop_unknown_service_task_holds_every_job": a_task_of_an_unknown_service_holds_every_job,
    "host_stop_deleted_task_holds_no_job": a_deleted_task_holds_no_job,
    "host_stop_reason_names_active_container_instances": the_reason_names_only_active_container_instances,
}
