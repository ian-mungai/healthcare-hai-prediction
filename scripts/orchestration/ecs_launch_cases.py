"""The failing-first matrix of the direct ECS launch core, against the fake ECS API, the fake log API and an in-memory write-once bucket.

Isolation is justified because the matrix has to make the cloud fail on cue (a lost reply, a refused call, a stop that does not finish) and move time by
minutes. Every case drives the real ``Launcher`` and the real ``S3LaunchStore``. A "restart" is a second ``Launcher`` over the same cloud and bucket.
Each case builds its own rig, so cases run in any order.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any

from botocore.exceptions import ReadTimeoutError

from orchestration.ecs_adapter import EcsAdapter, EcsSettings
from orchestration.ecs_launch import (
    CleanupFailed,
    EcsFitProbe,
    JobSpec,
    Launcher,
    LaunchError,
    LaunchRefused,
    LaunchUnavailable,
    Resources,
    S3LaunchStore,
    StopFailed,
    launch_record_prefix,
)
from scripts.orchestration.ecs_broker_fake import CLUSTER, FakeEcs, FakeLogs, client_error
from scripts.orchestration.ecs_launch_fake import FakeClock, FakeLaunchBucket

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261104T100000Z-abcdef01"
BUCKET = "example-private-bucket"
PREFIX = launch_record_prefix()
SETTINGS = EcsSettings(
    cluster=CLUSTER,
    family_prefix="example_dev_job_",
    subnets=(),
    networks={},
    project_tag="example",
    log_group="example_jobs_dev",
    families=frozenset({"gold", "drift"}),
    host_capacity_provider="stand-in_host",
)
NEED = Resources(cpu_units=3072, memory_mib=16384, scratch_bytes=100 * 2**30)
BOUND = {"code_digest": "a" * 64, "request_hash": "b" * 64, "input_run": "20261103T100000Z-12345678"}
ENVIRONMENT = {"gold": {"RUN_ID": RUN}, "verify": {"RUN_ID": RUN}}
EVENT_ORDER = ("intent", "started", "end")
THROTTLE = client_error("ThrottlingException", "DescribeTasks")


class Rig:
    """One stand-in cloud, bucket and clock. ``launcher`` makes a new process over them."""

    def __init__(self) -> None:
        self.ecs, self.logs = FakeEcs(), FakeLogs()
        self.bucket = FakeLaunchBucket(BUCKET, f"{PREFIX}/")
        self.time = FakeClock()
        self.adapter = EcsAdapter(self.ecs, self.logs, SETTINGS)
        self.scratch_free = 2 * NEED.scratch_bytes

    def launcher(self, cleanup: Callable[[JobSpec], None] | None = None) -> Launcher:
        probe = EcsFitProbe(self.ecs, CLUSTER, lambda: self.scratch_free)
        return Launcher(self.adapter, S3LaunchStore(self.bucket, BUCKET, PREFIX), probe, self.time.clock(), cleanup)

    def key(self, attempt: int, event: str, job: str = "gold") -> str:
        return f"{PREFIX}/{RUN}/{job}/{attempt:02d}/{event}.json"

    def events(self, attempt: int) -> list[str]:
        return [event for event in EVENT_ORDER if self.key(attempt, event) in self.bucket.objects]

    def record(self, attempt: int, event: str) -> dict[str, Any]:
        return dict(json.loads(self.bucket.objects[self.key(attempt, event)]))

    def task_arns(self) -> list[str]:
        return sorted(self.ecs.tasks)


class BrokenLogs(FakeLogs):
    """A log service that throttles every call."""

    def get_log_events(self, **arguments: Any) -> dict[str, Any]:
        raise client_error("ThrottlingException", "GetLogEvents")


def task_id(key: str) -> str:
    """The ID the fake gives the task started for a job key: the last part of its ARN and of its log stream name."""
    token = hashlib.sha256(key.encode()).hexdigest()
    return hashlib.sha256(token.encode()).hexdigest()[:32]


def spec(attempt: int = 1, **changes: Any) -> JobSpec:
    base = JobSpec(
        run_id=RUN,
        job="gold",
        attempt=attempt,
        environment=ENVIRONMENT,
        bound=BOUND,
        deadline_seconds=3600,
        need=NEED,
        acceptance=("verify",),
        log_container="gold",
        log_prefix="gold",
    )
    return replace(base, **changes)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def refuses(error: type[BaseException], action: Callable[[], Any]) -> BaseException:
    try:
        action()
    except error as caught:
        return caught
    raise AssertionError(f"expected {error.__name__}")


def die_while_running(rig: Rig, job: JobSpec) -> str:
    """A process that launches the task and then dies: the task keeps running and the records stay."""
    launcher = rig.launcher()
    launch = launcher.attach(job)
    launcher.start(launch)
    require(launch.arn is not None, "the task did not start")
    return str(launch.arn)


def launch_failure() -> None:
    rig = Rig()
    rig.ecs.faults["run_task"] = [client_error("AccessDeniedException", "RunTask")]
    first = rig.launcher().run(spec())
    require(first.outcome == "failed_closed" and not first.ok and "AccessDeniedException" in first.reason, f"refused launch: {first}")
    require(rig.events(1) == ["intent", "end"], f"a refused launch records an intent and an end only: {rig.events(1)}")
    again = rig.launcher().run(spec())
    require(again == first and rig.ecs.count("run_task") == 1, "a rerun of the attempt must return the recorded failure and not call RunTask")
    retry = rig.launcher().run(spec(2))
    require(retry.ok and rig.ecs.count("run_task") == 2, f"a new attempt may launch after a failed launch: {retry}")


def lost_reply() -> None:
    rig = Rig()
    rig.ecs.lose_reply_once = True
    done = rig.launcher().run(spec())
    require(done.ok and len(rig.task_arns()) == 1 and rig.ecs.count("run_task") == 1, f"lost reply inside one process: {done}, {rig.task_arns()}")
    rig = Rig()
    rig.bucket.put_faults["started.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require(rig.events(1) == ["intent"] and len(rig.task_arns()) == 1, "the task runs but only the intent is recorded")
    done = rig.launcher().run(spec())
    require(done.ok and len(rig.task_arns()) == 1 and rig.ecs.count("run_task") == 1, f"adoption after a restart: {done}")
    require(rig.events(1) == ["intent", "started", "end"] and rig.record(1, "started")["task_arn"] == rig.task_arns()[0], "started names the adopted task")


def restart_same_attempt() -> None:
    rig = Rig()
    rig.ecs.hold = True
    arn = die_while_running(rig, spec())
    started = rig.record(1, "started")
    rig.time.now += 600
    rig.ecs.hold = False
    done = rig.launcher().run(spec())
    require(done.ok and done.task_arn == arn and rig.ecs.count("run_task") == 1, f"reattach after a restart: {done}")
    require(rig.record(1, "started") == started, "the started record is never rewritten")


def restart_next_attempt() -> None:
    rig = Rig()
    rig.ecs.hold = True
    arn = die_while_running(rig, spec())
    first = rig.record(1, "started")
    rig.time.now += 600
    rig.ecs.hold = False
    done = rig.launcher().run(spec(2))
    require(done.ok and done.task_arn == arn and rig.ecs.count("run_task") == 1 and len(rig.task_arns()) == 1, f"attempt 2 reattaches: {done}")
    second = rig.record(2, "started")
    require(second["task_arn"] == arn and second["job_started_at"] == first["job_started_at"], f"attempt 2 keeps the task and its start time: {second}")
    require(rig.record(2, "intent")["reattached_from"] == 1, "the intent of attempt 2 names the attempt it reattached to")


def completion_before_recovery() -> None:
    rig = Rig()
    rig.ecs.hold = True
    arn = die_while_running(rig, spec())
    rig.ecs.finish(arn)
    require(rig.ecs.list_tasks(cluster=CLUSTER, startedBy=rig.ecs.tasks[arn]["startedBy"])["taskArns"] == [], "a stopped task is invisible to list_tasks")
    done = rig.launcher().run(spec())
    require(done.ok and rig.events(1) == ["intent", "started", "end"] and rig.ecs.count("run_task") == 1, f"recovery through the recorded ARN: {done}")
    rig = Rig()
    rig.ecs.hold = True
    arn = die_while_running(rig, spec())
    rig.ecs.finish(arn)
    adopted = rig.launcher().run(spec(2))
    require(adopted.ok and adopted.adopted and rig.ecs.count("run_task") == 1, f"a finished success is adopted, not relaunched: {adopted}")
    require(rig.events(1) == ["intent", "started", "end"], "the earlier attempt's end is recovered and recorded")
    rig = Rig()
    rig.ecs.hold = True
    arn = die_while_running(rig, spec())
    rig.ecs.finish(arn, {"gold": 3})
    rig.ecs.hold = False
    retry = rig.launcher().run(spec(2))
    require(retry.ok and not retry.adopted and rig.ecs.count("run_task") == 2, f"a failed attempt allows a new launch: {retry}")
    require(rig.record(1, "end")["outcome"] == "failed" and rig.record(1, "end")["exit_code"] == 3, "the failure of attempt 1 stays recorded")


def earlier_success_adopted() -> None:
    rig = Rig()
    first = rig.launcher().run(spec())
    second = rig.launcher().run(spec(2))
    require(second.ok and second.adopted and second.task_arn == first.task_arn, f"a recorded success is adopted by the next attempt: {second}")
    require(rig.ecs.count("run_task") == 1 and rig.events(2) == [], "no task and no record for the adopting attempt")


def corrupt_record() -> None:
    rig = Rig()
    rig.bucket.objects[rig.key(1, "intent")] = b'{"event": "intent"}'
    error = refuses(LaunchError, lambda: rig.launcher().run(spec()))
    require(not isinstance(error, LaunchRefused | LaunchUnavailable) and "invalid" in str(error), f"a damaged record stops the launch loudly: {error}")
    require(rig.ecs.count("run_task") == 0, "nothing launches on a damaged history")


def fit_probe_refused() -> None:
    rig = Rig()
    rig.ecs.faults["describe_container_instances"] = [client_error("AccessDeniedException", "DescribeContainerInstances")]
    done = rig.launcher().run(spec())
    require(
        done.outcome == "failed_closed" and "fit_probe_refused" in done.reason and rig.ecs.count("run_task") == 0, f"a refused fit probe fails closed: {done}"
    )


def duplicate() -> None:
    rig = Rig()
    first = rig.launcher().run(spec())
    writes = rig.bucket.count("put_object")
    objects = dict(rig.bucket.objects)
    second = rig.launcher().run(spec())
    require(first.ok and second == first, f"same outcome: {first} {second}")
    require(rig.ecs.count("run_task") == 1 and len(rig.task_arns()) == 1, "one task")
    require(rig.bucket.count("put_object") == writes and rig.bucket.objects == objects, "a repeat writes nothing")


def overlap_refused() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    objects = dict(rig.bucket.objects)
    error = refuses(LaunchRefused, lambda: rig.launcher().run(spec(2, bound={**BOUND, "code_digest": "c" * 64})))
    require("code_digest" in str(error), f"the refusal names the changed input: {error}")
    require(rig.ecs.count("run_task") == 1 and rig.bucket.objects == objects, "nothing is launched or recorded beside the live task")


def overlap_unknown() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    rig.ecs.faults["describe_tasks"] = [THROTTLE]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec(2)))
    require(rig.ecs.count("run_task") == 1 and rig.events(2) == [], "an unknown earlier task blocks the new attempt")
    rig.ecs.hold = False
    done = rig.launcher().run(spec(2))
    require(done.ok and rig.ecs.count("run_task") == 1, f"the next call reattaches: {done}")


def overlap_intent_only() -> None:
    rig = Rig()
    rig.ecs.hold = True
    rig.bucket.put_faults["started.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    rig.ecs.hold = False
    done = rig.launcher().run(spec(2))
    require(done.ok and rig.ecs.count("run_task") == 1 and len(rig.task_arns()) == 1, f"the task of an intent without started is found by identity: {done}")
    require(rig.record(2, "started")["task_arn"] == rig.task_arns()[0], "attempt 2 is bound to that task")


def changed_inputs() -> None:
    rig = Rig()
    require(rig.launcher().run(spec()).ok, "first attempt")
    objects, runs = dict(rig.bucket.objects), rig.ecs.count("run_task")
    for name in BOUND:
        other = {**BOUND, name: "d" * len(BOUND[name])}
        error = refuses(LaunchRefused, partial(rig.launcher().run, spec(2, bound=other)))
        require(name in str(error), f"{name} must be named: {error}")
    require(rig.ecs.count("run_task") == runs and rig.bucket.objects == objects, "a refused attempt records nothing and calls nothing")


def validation_failure() -> None:
    rig = Rig()
    wrong_type: dict[str, Any] = {"gold": {"RUN_ID": 3}}
    bad = [
        spec(run_id="not-a-run"),
        spec(job="train"),
        spec(job="Gold"),
        spec(attempt=0),
        spec(deadline_seconds=0),
        spec(bound={"code_digest": "a", "request_hash": "b"}),
        spec(bound={**BOUND, "input_run": ""}),
        spec(bound={**BOUND, "extra": "x"}),
        spec(need=Resources(-1, 1, 1)),
        spec(environment=wrong_type),
    ]
    for item in bad:
        refuses(LaunchRefused, partial(rig.launcher().run, item))
    require(not rig.ecs.calls and not rig.bucket.calls, "a refused request reaches neither the cloud nor the bucket")


def acceptance_missing() -> None:
    rig = Rig()
    rig.ecs.omit = {"verify"}
    done = rig.launcher().run(spec())
    require(done.outcome == "failed" and done.exit_code == 125 and "acceptance_check_missing:verify" in done.reason, f"missing container: {done}")
    require(rig.record(1, "end")["exit_code"] == 125, "the end record carries it")
    rig = Rig()
    rig.ecs.plan = {"verify": 2}
    done = rig.launcher().run(spec())
    require(done.outcome == "failed" and done.exit_code == 2, f"failing acceptance container: {done}")


def guard_exit_ignored() -> None:
    rig = Rig()
    rig.ecs.plan = {"guard": 1}
    done = rig.launcher().run(spec(environment={**ENVIRONMENT, "guard": {"RUN_ID": RUN}}))
    require(done.ok, f"the guard's exit code says nothing about the job: {done}")
    rig = Rig()
    rig.ecs.plan = {"gold": 4, "guard": 0}
    done = rig.launcher().run(spec(environment={**ENVIRONMENT, "guard": {"RUN_ID": RUN}}))
    require(done.outcome == "failed" and done.exit_code == 4, f"a failed phase fails the job: {done}")


def timeout() -> None:
    rig = Rig()
    rig.ecs.hold = True
    begin = rig.time.now
    done = rig.launcher().run(spec(deadline_seconds=100))
    require(done.outcome == "timeout" and done.exit_code == 124, f"timeout: {done}")
    require(100 < rig.time.now - begin < 100 + 30, f"the wait ends at the deadline: {rig.time.now - begin}")
    task = rig.ecs.tasks[rig.task_arns()[0]]
    require(task["lastStatus"] == "STOPPED" and task["stopCode"] == "UserInitiated", "the task was stopped")
    require(rig.record(1, "end")["outcome"] == "timeout", "recorded")


def timeout_after_restart() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec(deadline_seconds=300))
    rig.time.now += 500
    begin = rig.time.now
    done = rig.launcher().run(spec(deadline_seconds=100000))
    require(done.outcome == "timeout", f"the deadline counts from the recorded start and the recorded deadline: {done}")
    require(rig.time.now - begin < 30, "no further waiting after the restart")


def deadline_from_ledger() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec(deadline_seconds=200))
    begin = rig.time.now
    done = rig.launcher().run(spec(2, deadline_seconds=100000))
    require(done.outcome == "timeout" and rig.time.now - begin < 200 + 30, f"attempt 2 keeps the deadline of the task it joined: {done}")
    require(rig.record(2, "intent")["deadline_seconds"] == 200, "recorded with the original deadline")


def stop_unconfirmed() -> None:
    rig = Rig()
    rig.ecs.hold = True
    rig.ecs.stop_lag = 10**6
    refuses(StopFailed, lambda: rig.launcher().run(spec(deadline_seconds=10)))
    require("end" not in rig.events(1) and rig.events(1) == ["intent", "started"], f"nothing recorded after an unconfirmed stop: {rig.events(1)}")
    require(rig.ecs.count("stop_task") == 1, "one stop request")
    waited = sum(rig.time.slept)
    require(120 <= waited < 120 + 10 + 60, f"the stop wait is bounded at 120 s: {waited}")
    rig.ecs.stop_lag = 0
    done = rig.launcher().run(spec(deadline_seconds=10))
    require(done.outcome == "timeout" and rig.events(1) == ["intent", "started", "end"], f"the stop can be repeated: {done}")


def stop_refused() -> None:
    rig = Rig()
    rig.ecs.hold = True
    rig.ecs.faults["stop_task"] = [client_error("AccessDeniedException", "StopTask")]
    refuses(StopFailed, lambda: rig.launcher().run(spec(deadline_seconds=10)))
    require(rig.events(1) == ["intent", "started"], "nothing recorded after a refused stop")
    done = rig.launcher().run(spec(deadline_seconds=10))
    require(done.outcome == "timeout", f"the stop can be repeated: {done}")


def cancel() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    done = rig.launcher().cancel(RUN, "gold", 1)
    require(done.outcome == "cancelled" and done.exit_code == 143, f"cancel: {done}")
    require(rig.ecs.tasks[rig.task_arns()[0]]["lastStatus"] == "STOPPED", "stopped and confirmed")
    require(rig.launcher().cancel(RUN, "gold", 1) == done, "a second cancel returns the record")


def failure_kept_through_cleanup() -> None:
    cleaned: list[str] = []
    rig = Rig()
    rig.ecs.plan = {"gold": 3}
    rig.logs.streams[f"gold/gold/{task_id(spec().key)}"] = ["phase gold failed"]
    done = rig.launcher(lambda job: cleaned.append(job.key)).run(spec())
    require(done.outcome == "failed" and done.exit_code == 3 and not cleaned, f"a failed job is not cleaned: {done}, {cleaned}")
    require(done.logs == "phase gold failed", f"the log tail comes with the failure: {done.logs!r}")
    again = rig.launcher(lambda job: cleaned.append(job.key)).run(spec())
    require(again == done and not cleaned and rig.ecs.count("run_task") == 1, "the failure is returned again, still without cleanup")
    rig = Rig()
    done = rig.launcher(lambda job: cleaned.append(job.key)).run(spec())
    require(done.ok and cleaned == [spec().key], f"a good job is cleaned once: {done}, {cleaned}")
    rig.launcher(lambda job: cleaned.append(job.key)).run(spec())
    require(cleaned == [spec().key], "a repeat does not clean again")

    def broken(job: JobSpec) -> None:
        raise CleanupFailed("scratch busy")

    rig = Rig()
    done = rig.launcher(broken).run(spec())
    require(done.ok and "scratch busy" in done.cleanup_note and rig.record(1, "end")["outcome"] == "succeeded", f"cleanup failure keeps the outcome: {done}")
    rig = Rig()
    rig.ecs.plan = {"gold": 3}
    rig.adapter = EcsAdapter(rig.ecs, BrokenLogs(), SETTINGS)
    done = rig.launcher().run(spec())
    require(done.outcome == "failed" and done.exit_code == 3 and done.logs == "", f"a log failure keeps the outcome: {done}")


def does_not_fit() -> None:
    rig = Rig()
    rig.ecs.instance_free = {"CPU": 1024, "MEMORY": 31000}
    begin = rig.time.now
    done = rig.launcher().run(spec())
    waited = rig.time.now - begin
    require(done.outcome == "failed_closed" and "does_not_fit" in done.reason and "cpu_units" in done.reason, f"fit report: {done}")
    require("3072" in done.reason and "1024" in done.reason, f"the report shows needed and free: {done.reason}")
    require(600 <= waited < 600 + 20, f"the wait is the 10 minutes: {waited}")
    require(rig.ecs.count("run_task") == 0 and rig.events(1) == ["intent", "end"], "nothing started, nothing on another provider")
    rig = Rig()
    rig.scratch_free = 10
    done = rig.launcher().run(spec())
    require(done.outcome == "failed_closed" and "scratch_bytes" in done.reason, f"scratch is checked: {done}")
    rig = Rig()
    rig.ecs.host_full = True
    done = rig.launcher().run(spec())
    require(
        done.outcome == "failed_closed" and "RESOURCE:CPU" in done.reason and rig.ecs.count("run_task") > 1,
        f"RunTask capacity answers are retried then reported: {done}",
    )


def fits_after_wait() -> None:
    rig = Rig()
    rig.ecs.instance_free = {"CPU": 1024, "MEMORY": 31000}
    begin = rig.time.now

    def free_up(now: float) -> None:
        if now - begin >= 100:
            rig.ecs.instance_free = {"CPU": 4096, "MEMORY": 31000}

    rig.time.on_sleep = free_up
    done = rig.launcher().run(spec())
    require(done.ok and sum(rig.time.slept) < 600, f"a job that fits later runs: {done}")


def intent_before_runtask() -> None:
    rig = Rig()
    rig.bucket.faults["put_object"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require(rig.ecs.count("run_task") == 0, "no task without a recorded intent")
    require(rig.launcher().run(spec()).ok, "the next call works")


def put_reply_lost() -> None:
    rig = Rig()
    rig.bucket.lose_reply_once = True
    done = rig.launcher().run(spec())
    require(
        done.ok and rig.events(1) == ["intent", "started", "end"] and rig.ecs.count("run_task") == 1, f"a write whose reply was lost counts as written: {done}"
    )


def store_scope() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    rig.ecs.hold = False
    done = rig.launcher().run(spec(3))
    require(done.ok, f"records read by key across a gap in the attempts: {done}")
    require(rig.launcher().run(spec(3)) == done, "a repeat reads the records and returns the outcome")
    require(not hasattr(rig.bucket, "list_objects_v2"), "the role cannot list")
    operations = {name for name, _ in rig.bucket.calls}
    require(operations <= {"put_object", "get_object"}, f"no delete, list or other call: {operations}")
    for name, arguments in rig.bucket.calls:
        key = arguments["Key"]
        pattern = rf"{re.escape(PREFIX)}/{RUN}/gold/[0-9]{{2}}/(intent|started|end)\.json"
        require(re.fullmatch(pattern, key) is not None, f"{name} {key} is not a launch record key")
        require(name != "put_object" or arguments.get("IfNoneMatch") == "*", "every write is conditional")


def read_denied_is_loud() -> None:
    rig = Rig()
    rig.bucket.deny_reads = True
    error = refuses(LaunchRefused, lambda: rig.launcher().run(spec()))
    require("read back" in str(error), f"a role that cannot read its records is named: {error}")
    require(rig.ecs.count("run_task") == 0, "nothing launches when the history cannot be read")


def record_prefix_from_configuration() -> None:
    configured = json.loads((ROOT / "config" / "orchestration" / "ecs_launch.json").read_text())["record_prefix"]
    require(launch_record_prefix() == configured.strip("/") and configured.endswith("/"), f"the prefix comes from the configuration file: {configured}")
    rig = Rig()
    require(rig.launcher().run(spec()).ok, "the rig works with the configured prefix only")
    require(all(key.startswith(configured) for key in rig.bucket.objects), f"every record is under {configured}: {list(rig.bucket.objects)}")


def poll_errors() -> None:
    rig = Rig()
    rig.ecs.faults["describe_tasks"] = [THROTTLE] * 5
    require(rig.launcher().run(spec()).ok, "five transient poll errors in a row are tolerated")
    rig = Rig()
    rig.ecs.hold = True
    rig.ecs.faults["describe_tasks"] = [THROTTLE] * 6
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require(rig.ecs.tasks[rig.task_arns()[0]]["lastStatus"] != "STOPPED", "the task keeps running")
    rig.ecs.hold = False
    require(rig.launcher().run(spec()).ok, "a later call reattaches")


CASES: dict[str, Callable[[], None]] = {
    "launch_failure": launch_failure,
    "lost_reply": lost_reply,
    "restart_same_attempt": restart_same_attempt,
    "restart_next_attempt": restart_next_attempt,
    "completion_before_recovery": completion_before_recovery,
    "duplicate": duplicate,
    "earlier_success_adopted": earlier_success_adopted,
    "corrupt_record": corrupt_record,
    "fit_probe_refused": fit_probe_refused,
    "overlap_refused": overlap_refused,
    "overlap_unknown": overlap_unknown,
    "overlap_intent_only": overlap_intent_only,
    "changed_inputs": changed_inputs,
    "validation_failure": validation_failure,
    "acceptance_missing": acceptance_missing,
    "guard_exit_ignored": guard_exit_ignored,
    "timeout": timeout,
    "timeout_after_restart": timeout_after_restart,
    "deadline_from_ledger": deadline_from_ledger,
    "stop_unconfirmed": stop_unconfirmed,
    "stop_refused": stop_refused,
    "cancel": cancel,
    "failure_kept_through_cleanup": failure_kept_through_cleanup,
    "does_not_fit": does_not_fit,
    "fits_after_wait": fits_after_wait,
    "intent_before_runtask": intent_before_runtask,
    "put_reply_lost": put_reply_lost,
    "store_scope": store_scope,
    "read_denied_is_loud": read_denied_is_loud,
    "record_prefix_from_configuration": record_prefix_from_configuration,
    "poll_errors": poll_errors,
}
