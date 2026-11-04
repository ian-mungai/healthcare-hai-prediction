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

from orchestration.ecs_adapter import HOST, EcsAdapter, EcsSettings
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
    iso,
    launch_record_prefix,
)
from scripts.orchestration.ecs_broker_fake import CLUSTER, FakeEcs, FakeLogs, client_error
from scripts.orchestration.ecs_launch_fake import START_TIME, FakeClock, FakeLaunchBucket

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261104T100000Z-abcdef01"
OTHER_RUN = "20261104T110000Z-abcdef02"
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


class Interrupted(BaseException):
    """The process ends while it waits, as when the scheduler is restarted."""


class Rig:
    """One stand-in cloud, bucket and clock. ``launcher`` makes a new process over them."""

    def __init__(self) -> None:
        self.ecs, self.logs = FakeEcs(), FakeLogs()
        self.bucket = FakeLaunchBucket(BUCKET, f"{PREFIX}/")
        self.time = FakeClock()
        self.ecs.clock = lambda: self.time.now
        self.adapter = EcsAdapter(self.ecs, self.logs, SETTINGS)
        self.scratch_free = 2 * NEED.scratch_bytes

    def launcher(self, cleanup: Callable[[JobSpec], None] | None = None) -> Launcher:
        probe = EcsFitProbe(self.ecs, CLUSTER, lambda: self.scratch_free)
        return Launcher(self.adapter, S3LaunchStore(self.bucket, BUCKET, PREFIX), probe, self.time.clock(), cleanup)

    def key(self, attempt: int, event: str, job: str = "gold") -> str:
        return f"{PREFIX}/{RUN}/{job}/{attempt:02d}/{event}.json"

    def claim_key(self, job: str = "gold") -> str:
        return f"{PREFIX}/{RUN}/{job}/claim.json"

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


def gold_tasks(rig: Rig) -> list[str]:
    """The ARNs of the tasks started for the gold job, whatever their state."""
    return [arn for arn, task in rig.ecs.tasks.items() if "job_gold" in str(task["taskDefinitionArn"])]


def stopped_decoys(rig: Rig, count: int) -> None:
    """Tasks of other jobs that have already stopped, so a lookup of stopped tasks has to read past them."""
    for number in range(count):
        arn = rig.adapter.start("drift", f"2026110{number + 1}T090000Z-1111111{number}:drift:1", {"drift": {"RUN_ID": "x"}}, "", HOST)
        rig.ecs.finish(arn)


def launch_failure() -> None:
    rig = Rig()
    rig.ecs.faults["run_task"] = [client_error("AccessDeniedException", "RunTask")]
    first = rig.launcher().run(spec())
    require(first.outcome == "failed_closed" and not first.ok and "AccessDeniedException" in first.reason, f"refused launch: {first}")
    require(rig.events(1) == ["intent", "end"], f"a refused launch records an intent and an end only: {rig.events(1)}")
    again = rig.launcher().run(spec())
    require(again == first and rig.ecs.count("run_task") == 1, "a rerun of the attempt must return the recorded failure and not call RunTask")
    retry = rig.launcher().run(spec(2))
    require(retry.outcome == first.outcome and retry.reason == first.reason and retry.adopted, f"a retry reports the closed launch: {retry}")
    require(rig.ecs.count("run_task") == 1, "and launches nothing")


def failed_attempt_is_terminal() -> None:
    rig = Rig()
    rig.ecs.plan = {"gold": 3}
    first = rig.launcher().run(spec())
    require(first.outcome == "failed" and first.exit_code == 3, f"first attempt: {first}")
    runs = rig.ecs.count("run_task")
    retry = rig.launcher().run(spec(2))
    require(retry.outcome == "failed" and retry.exit_code == 3 and retry.adopted and not retry.ok, f"a retry reports the earlier failure: {retry}")
    require(rig.ecs.count("run_task") == runs and rig.events(2) == [], "a retry after a failure launches nothing and records nothing")
    rig.ecs.plan = {}
    fresh = rig.launcher().run(spec(1, run_id=OTHER_RUN))
    require(fresh.ok and rig.ecs.count("run_task") == runs + 1, f"trying again needs a new run, and a new run launches: {fresh}")
    rig = Rig()
    rig.ecs.hold = True
    first = rig.launcher().run(spec(deadline_seconds=100))
    retry = rig.launcher().run(spec(2))
    require(first.outcome == "timeout" and retry.outcome == "timeout" and retry.adopted, f"a timeout is terminal too: {retry}")
    require(rig.ecs.count("run_task") == 1 and rig.events(2) == [], "a retry after a timeout launches nothing")
    rig = Rig()
    rig.ecs.instance_free = {"CPU": 1024, "MEMORY": 31000}
    first = rig.launcher().run(spec())
    rig.ecs.instance_free = {"CPU": 4096, "MEMORY": 31000}
    retry = rig.launcher().run(spec(2))
    require(first.outcome == "failed_closed" and retry.outcome == "failed_closed" and retry.adopted, f"a launch that did not fit is terminal too: {retry}")
    require(rig.ecs.count("run_task") == 0, "a retry after a fit failure launches nothing")
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    cancelled = rig.launcher().cancel(RUN, "gold", 1)
    retry = rig.launcher().run(spec(2))
    require(cancelled.outcome == "cancelled" and retry.outcome == "cancelled" and rig.ecs.count("run_task") == 1, f"a cancel is terminal too: {retry}")


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
    require(rig.record(1, "end")["outcome"] == "succeeded", "the attempt that joined also ends the launch it joined")


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
    require(retry.outcome == "failed" and retry.exit_code == 3 and retry.adopted, f"the failure that ended in the cloud is recovered and reported: {retry}")
    require(rig.ecs.count("run_task") == 1 and rig.events(2) == [], "a retry after a failure launches nothing")
    require(rig.record(1, "end")["outcome"] == "failed" and rig.record(1, "end")["exit_code"] == 3, "the failure of attempt 1 is recorded")


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


def unresolved_launch_task_finished() -> None:
    rig = Rig()
    rig.ecs.forget_stopped_tokens = True
    rig.ecs.list_page = 2
    stopped_decoys(rig, 5)
    rig.ecs.hold = True
    rig.bucket.put_faults["started.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require(rig.events(1) == ["intent"] and len(gold_tasks(rig)) == 1, f"the task runs but only the intent is recorded: {rig.events(1)}")
    arn = gold_tasks(rig)[0]
    rig.ecs.finish(arn)
    rig.ecs.hold = False
    rig.time.now += 300
    done = rig.launcher().run(spec(2))
    require(done.ok and done.adopted, f"the finished task of the unresolved launch is adopted: {done}")
    require(len(gold_tasks(rig)) == 1, f"a second task was started beside the first: {len(gold_tasks(rig))}")
    require(rig.record(1, "started")["task_arn"] == arn and rig.record(1, "end")["outcome"] == "succeeded", "the unresolved launch is resolved in the records")
    require(rig.record(1, "started")["job_started_at"] == iso(START_TIME), "the task starts when ECS created it, not when the recovery ran")


def ambiguous_replies_stay_unresolved() -> None:
    rig = Rig()
    rig.ecs.hold = True
    rig.ecs.lose_reply_once = True

    def break_the_lookups(now: float) -> None:
        rig.time.on_sleep = None
        rig.ecs.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks")] * 500
        rig.ecs.faults["run_task"] = [ReadTimeoutError(endpoint_url="https://ecs.invalid")] * 500

    rig.time.on_sleep = break_the_lookups
    begin = rig.time.now
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require(rig.time.now - begin >= 600, "the launch waits its 10 minutes before it gives up")
    require(rig.events(1) == ["intent"] and len(gold_tasks(rig)) == 1, f"an unknown reply must not become a failure record: {rig.events(1)}")
    rig.ecs.faults.clear()
    rig.ecs.hold = False
    done = rig.launcher().run(spec())
    require(done.ok and len(gold_tasks(rig)) == 1, f"the next call finds the accepted task: {done}")
    require(rig.events(1) == ["intent", "started", "end"], "the launch is resolved by a lookup")


def concurrent_attempts_one_task() -> None:
    rig = Rig()
    rig.ecs.hold = True
    first, second = rig.launcher(), rig.launcher()
    a, b = first.attach(spec(1)), second.attach(spec(2))
    first.start(a)
    second.start(b)
    require(len(gold_tasks(rig)) == 1, f"two callers that both saw an empty history started {len(gold_tasks(rig))} tasks")
    require(a.arn is not None and b.arn == a.arn, "the second caller joins the task of the first")
    require(rig.record(2, "intent")["reattached_from"] == 1, "recorded as a reattachment")
    rig.ecs.hold = False
    first.wait(a)
    second.wait(b)
    require(first.conclude(a).ok and second.conclude(b).ok, "both callers see the job end well")


def concurrent_loser_bindings_refused() -> None:
    rig = Rig()
    rig.ecs.hold = True
    first, second = rig.launcher(), rig.launcher()
    a, b = first.attach(spec(1)), second.attach(spec(1, bound={**BOUND, "request_hash": "e" * 64}))
    first.start(a)
    runs = rig.ecs.count("run_task")
    error = refuses(LaunchRefused, lambda: second.start(b))
    require("request_hash" in str(error), f"the refusal names the changed input: {error}")
    require(rig.ecs.count("run_task") == runs and len(gold_tasks(rig)) == 1 and b.arn is None, "the loser takes no ECS action")


def capacity_wait_rechecks_identity() -> None:
    rig = Rig()
    rig.ecs.forget_stopped_tokens = True
    rig.ecs.instance_free = {"CPU": 1024, "MEMORY": 31000}
    seen: dict[str, Any] = {}

    def other_caller_runs(now: float) -> None:
        rig.time.on_sleep = None
        rig.ecs.instance_free = {"CPU": 4096, "MEMORY": 31000}
        seen["other"] = rig.launcher().run(spec(2))

    rig.time.on_sleep = other_caller_runs
    done = rig.launcher().run(spec(1))
    require(seen["other"].ok and done.ok, f"both callers end well: {seen.get('other')} {done}")
    require(len(gold_tasks(rig)) == 1, f"the waiting caller started a task after the other one had run and finished: {len(gold_tasks(rig))}")


def restart_keeps_job_start() -> None:
    rig = Rig()
    rig.ecs.hold = True
    rig.bucket.put_faults["started.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec(deadline_seconds=600)))
    rig.time.now += 500
    launcher = rig.launcher()
    launch = launcher.attach(spec(deadline_seconds=600))
    launcher.start(launch)
    require(rig.record(1, "started")["job_started_at"] == iso(START_TIME), f"a recovery must not restart the job clock: {rig.record(1, 'started')}")
    begin = rig.time.now
    launcher.wait(launch)
    done = launcher.conclude(launch)
    require(done.outcome == "timeout" and rig.time.now - begin < 130, f"the deadline counts from the original start: {done}, {rig.time.now - begin}")


def fit_deadline_survives_restart() -> None:
    rig = Rig()
    rig.ecs.instance_free = {"CPU": 1024, "MEMORY": 31000}

    def die(now: float) -> None:
        if now - START_TIME >= 300:
            raise Interrupted

    rig.time.on_sleep = die
    refuses(Interrupted, lambda: rig.launcher().run(spec()))
    rig.time.on_sleep = None
    done = rig.launcher().run(spec())
    require(done.outcome == "failed_closed" and "does_not_fit" in done.reason, f"fit report: {done}")
    require(rig.time.now - START_TIME < 600 + 30, f"the 10 minutes count from the claim, not from the restart: {rig.time.now - START_TIME}")


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
    later = rig.launcher().run(spec(3))
    require(later.outcome == "timeout" and later.adopted and rig.ecs.count("run_task") == 1, f"a later attempt reports the timeout, not a stop code: {later}")


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
    require(not hasattr(rig.bucket, "list_objects_v2"), "the code never lists")
    operations = {name for name, _ in rig.bucket.calls}
    require(operations <= {"put_object", "get_object"}, f"no delete, list or other call: {operations}")
    for name, arguments in rig.bucket.calls:
        key = arguments["Key"]
        pattern = rf"{re.escape(PREFIX)}/({RUN}/gold/([0-9]{{2}}/(intent|started|end)|claim)|absent-probe)\.json"
        require(re.fullmatch(pattern, key) is not None, f"{name} {key} is not a launch record key")
        require(name != "put_object" or arguments.get("IfNoneMatch") == "*", "every write is conditional")


def read_denied_is_loud() -> None:
    rig = Rig()
    rig.bucket.deny_reads = True
    error = refuses(LaunchRefused, lambda: rig.launcher().run(spec()))
    require("404" in str(error), f"a role that cannot read its records is named: {error}")
    require(rig.ecs.count("run_task") == 0 and rig.bucket.count("put_object") == 0, "nothing launches and nothing is written when the history cannot be read")


def role_without_list_refuses() -> None:
    rig = Rig()
    rig.bucket.absent_reads_denied = True
    error = refuses(LaunchRefused, lambda: rig.launcher().run(spec()))
    require("ListBucket" in str(error) and "404" in str(error), f"the refusal names the missing grant: {error}")
    require(rig.ecs.count("run_task") == 0 and rig.bucket.count("put_object") == 0, "a role that cannot tell a missing record from a denied one starts nothing")


def selective_denial_refuses() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    writes, runs = rig.bucket.count("put_object"), rig.ecs.count("run_task")
    rig.bucket.deny_keys = {rig.key(1, "intent"), rig.key(1, "started"), rig.key(1, "end")}
    error = refuses(LaunchRefused, lambda: rig.launcher().run(spec(2)))
    require("cannot be read" in str(error), f"the refusal says the history cannot be read: {error}")
    require(len(gold_tasks(rig)) == 1 and rig.ecs.count("run_task") == runs, "no second task starts beside a task whose records cannot be read")
    require(rig.bucket.count("put_object") == writes, "nothing is written for the refused attempt")
    rig.bucket.deny_keys = {rig.key(1, "intent")}
    refuses(LaunchRefused, lambda: rig.launcher().run(spec(2)))
    refuses(LaunchRefused, lambda: rig.launcher().run(spec(1)))
    require(len(gold_tasks(rig)) == 1 and rig.bucket.count("put_object") == writes, "one denied record is enough to block both the retry and the owner's rerun")
    rig.bucket.deny_keys = set()
    rig.ecs.hold = False
    done = rig.launcher().run(spec(2))
    require(done.ok and len(gold_tasks(rig)) == 1, f"once the history can be read the retry reattaches: {done}")


def authority_unreadable_refuses() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    rig.bucket.deny_keys = {rig.claim_key()}
    for attempt in (1, 2):
        error = refuses(LaunchRefused, partial(rig.launcher().run, spec(attempt)))
        require("cannot be read" in str(error), f"attempt {attempt}: {error}")
    require(len(gold_tasks(rig)) == 1, "no second task")
    rig = Rig()
    rig.bucket.deny_keys = {rig.claim_key()}
    refuses(LaunchRefused, lambda: rig.launcher().run(spec()))
    require(rig.ecs.count("run_task") == 0, "a denied read of the authority is not a missing authority: nothing starts")


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
    "failed_attempt_is_terminal": failed_attempt_is_terminal,
    "lost_reply": lost_reply,
    "restart_same_attempt": restart_same_attempt,
    "restart_next_attempt": restart_next_attempt,
    "completion_before_recovery": completion_before_recovery,
    "unresolved_launch_task_finished": unresolved_launch_task_finished,
    "ambiguous_replies_stay_unresolved": ambiguous_replies_stay_unresolved,
    "concurrent_attempts_one_task": concurrent_attempts_one_task,
    "concurrent_loser_bindings_refused": concurrent_loser_bindings_refused,
    "capacity_wait_rechecks_identity": capacity_wait_rechecks_identity,
    "restart_keeps_job_start": restart_keeps_job_start,
    "fit_deadline_survives_restart": fit_deadline_survives_restart,
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
    "role_without_list_refuses": role_without_list_refuses,
    "selective_denial_refuses": selective_denial_refuses,
    "authority_unreadable_refuses": authority_unreadable_refuses,
    "record_prefix_from_configuration": record_prefix_from_configuration,
    "poll_errors": poll_errors,
}
