"""Host session cases for the settle of launches by a host stop (owner decision A, Nov 24 2026): ``stop``, ``stop_after_failure`` and ``host_on`` end every
claimed launch that has no end record as ``cancelled`` once the host is shown stopped, so a launch whose task ECS no longer describes stops blocking its family.

The stand-in host here keeps the launch records as S3 objects (``s3api get-object``, ``put-object`` with ``--if-none-match`` or ``--if-match``, and
``list-objects-v2``) over the data bucket, as the real store sees them through the AWS CLI. Time is virtual: a claim is old or young by the virtual time
between its ``at`` and the session's wall clock.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from orchestration.ecs_launch import launch_record_prefix
from scripts.infrastructure import host_session_restore_cases as restore
from scripts.infrastructure.host_session import Session
from scripts.infrastructure.host_session_stand_in import ACCOUNT_MARK, EPOCH, StandInError
from scripts.infrastructure.host_session_stop_cases import on_wall_time

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
    (every launch record call answers AccessDenied)."""

    def __init__(self, faults: set[str] | None = None) -> None:
        super().__init__(faults, None, False, False, checkout=restore.SHA_OLD, code_run=restore.RUN_OLD, openmetadata=True, catalog_setup=True)

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
            if options.get("--if-none-match") != ["*"] and "--if-match" not in options:
                raise StandInError("AccessDenied")  # an unconditional write under the launch prefix is refused, as the IAM policy of the role refuses it
            if "--if-none-match" in options and key in self.objects:
                raise StandInError("PreconditionFailed")
        return super().s3_put_object(options)

    def s3_list_objects(self, options: dict[str, list[str]]) -> dict[str, Any]:
        prefix = options["--prefix"][0]
        if "store_denied" in self.faults:
            raise StandInError("AccessDenied")
        return {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(prefix)], "IsTruncated": False}

    def ecs_list_container_instances(self, options: dict[str, list[str]]) -> dict[str, Any]:
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
    "host_stop_skips_the_settle_without_a_data_bucket": an_unreadable_data_bucket_name_skips_the_settle,
}
