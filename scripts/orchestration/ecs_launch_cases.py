"""The failing-first matrix of the direct ECS launch core, against the fake ECS API, the fake log API and an in-memory write-once bucket.

Isolation is justified because the matrix has to make the cloud fail on cue (a lost reply, a refused call, a stop that does not finish) and move time by
minutes. Every case drives the real ``Launcher`` and the real ``S3LaunchStore``. A "restart" is a second ``Launcher`` over the same cloud and bucket.
Each case builds its own rig, so cases run in any order.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
import threading
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
    FamilyBusy,
    JobSpec,
    Launch,
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
from scripts.orchestration.ecs_broker_fake import ACCOUNT, CLUSTER, FakeEcs, FakeLogs, SdkStub, client_error
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

    def launcher(self, cleanup: Callable[[JobSpec], None] | None = None, scratch_usage: Callable[[], dict[str, int]] | None = None) -> Launcher:
        probe = EcsFitProbe(self.ecs, CLUSTER, lambda: self.scratch_free)
        return Launcher(self.adapter, S3LaunchStore(self.bucket, BUCKET, PREFIX), probe, self.time.clock(), cleanup, scratch_usage)

    def key(self, attempt: int, event: str, job: str = "gold") -> str:
        return f"{PREFIX}/{RUN}/{job}/{attempt:02d}/{event}.json"

    def claim_key(self, job: str = "gold") -> str:
        return f"{PREFIX}/{RUN}/{job}/claim.json"

    def decision_key(self, job: str = "gold") -> str:
        return f"{PREFIX}/{RUN}/{job}/decision.json"

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


def blind_and_broken(rig: Rig, how: str) -> None:
    """Make the cloud fail the way the reviewer's probes did, right after a RunTask whose reply was lost. The lookups are throttled in every variant."""
    task = gold_tasks(rig)[0]
    rig.ecs.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks")] * 500
    if how == "probe_denied":
        rig.ecs.faults["list_container_instances"] = [client_error("AccessDeniedException", "ListContainerInstances")] * 500
    elif how == "runtask_denied":
        rig.ecs.finish(task)
        rig.ecs.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks")]
        rig.ecs.faults["run_task"] = [client_error("AccessDeniedException", "RunTask")] * 500
    elif how == "no_room":
        rig.ecs.finish(task)
        rig.ecs.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks"), None] * 300
        rig.ecs.host_full = True
        rig.ecs.forget_stopped_tokens = True
    elif how == "cancel_requested":
        with contextlib.suppress(LaunchUnavailable):
            rig.launcher().cancel(RUN, "gold", 1)


def no_terminal_outcome_while_a_launch_may_be_live() -> None:
    for how in ("probe_denied", "runtask_denied", "no_room", "cancel_requested"):
        rig = Rig()
        rig.ecs.hold = True
        rig.ecs.lose_reply_once = True

        def inject(now: float, rig: Rig = rig, how: str = how) -> None:
            rig.time.on_sleep = None
            blind_and_broken(rig, how)

        rig.time.on_sleep = inject
        error = refuses(LaunchUnavailable, partial(rig.launcher().run, spec()))
        require("end" not in rig.events(1), f"{how}: a terminal outcome was recorded while a task may be live: {rig.events(1)}")
        require(len(gold_tasks(rig)) == 1, f"{how}: {len(gold_tasks(rig))} tasks, error: {error}")
        rig.ecs.faults.clear()
        rig.ecs.host_full = False
        rig.ecs.forget_stopped_tokens = False
        rig.ecs.hold = False
        rig.time.on_sleep = None
        done = rig.launcher().run(spec())
        require(len(gold_tasks(rig)) == 1 and "end" in rig.events(1), f"{how}: the next call finds the task and settles the launch: {done}")


def aged_recovery_never_replays_runtask() -> None:
    rig = Rig()
    rig.ecs.token_ttl, rig.ecs.purge_after = 900.0, 3600.0
    rig.ecs.lose_reply_once = True
    rig.time.on_sleep = lambda now: (_ for _ in ()).throw(Interrupted())
    refuses(Interrupted, partial(rig.launcher().run, spec()))
    arn = gold_tasks(rig)[0]
    rig.ecs.finish(arn)
    rig.time.on_sleep = None
    rig.time.now += 7200
    runs = rig.ecs.count("run_task")
    error = refuses(LaunchUnavailable, partial(rig.launcher().run, spec(2)))
    require(
        "Manual owner path" in str(error) and "orchestration.airflow_steps settle" in str(error) and "admits a new run" in str(error),
        f"the message names the manual owner path and its settle command: {error}",
    )
    require(rig.ecs.count("run_task") == runs and len(gold_tasks(rig)) == 1, "no RunTask is repeated after the window, the token life and what ECS remembers")
    require("end" not in rig.events(1), "the unresolved launch is not closed, however old it is")


def claim_that_sent_nothing_an_hour_later() -> None:
    rig = Rig()
    rig.bucket.put_faults["intent.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, partial(rig.launcher().run, spec()))
    require(rig.ecs.count("run_task") == 0, "the first call claimed and sent nothing")
    rig.time.now += 3600
    done = rig.launcher().run(spec())
    require(done.outcome == "failed_closed" and "no task was started" in done.reason, f"a claim that never sent anything is closed: {done}")
    require(rig.ecs.count("run_task") == 0, "and nothing is launched an hour later")
    retry = rig.launcher().run(spec(2))
    require(retry.outcome == "failed_closed" and retry.adopted and rig.ecs.count("run_task") == 0, f"a retry reports it: {retry}")


def cancel_before_runtask_is_accepted() -> None:
    rig = Rig()
    rig.ecs.hold = True
    seen: dict[str, str] = {}

    def cancel_midway(phase: str) -> None:
        if phase == "before":
            rig.ecs.run_task_hook = None
            try:
                rig.launcher().cancel(RUN, "gold", 1)
            except LaunchUnavailable as error:
                seen["cancel"] = str(error)

    rig.ecs.run_task_hook = cancel_midway
    done = rig.launcher().run(spec())
    require("unresolved" in seen.get("cancel", ""), f"a cancel with a RunTask outstanding stays unresolved: {seen}")
    require(done.outcome == "cancelled" and len(gold_tasks(rig)) == 1, f"the launching call sees the cancel: {done}")
    require(rig.ecs.tasks[gold_tasks(rig)[0]]["lastStatus"] == "STOPPED", "and stops the task it just started")
    require(rig.record(1, "end")["outcome"] == "cancelled" and rig.record(1, "end")["task_arn"] == gold_tasks(rig)[0], "cancelled is recorded with the ARN")
    rig = Rig()
    rig.ecs.instance_free = {"CPU": 1024, "MEMORY": 31000}
    outcome: dict[str, Any] = {}

    def cancel_while_waiting(now: float) -> None:
        rig.time.on_sleep = None
        outcome["cancel"] = rig.launcher().cancel(RUN, "gold", 1)
        rig.ecs.instance_free = {"CPU": 4096, "MEMORY": 31000}

    rig.time.on_sleep = cancel_while_waiting
    done = rig.launcher().run(spec())
    require(done.outcome == "cancelled" and outcome["cancel"].outcome == "cancelled", f"a cancel during the wait for room: {done}")
    require(rig.ecs.count("run_task") == 0, "stops the launch before it sends anything")


def recovery_enforces_the_claim_needs() -> None:
    rig = Rig()
    rig.bucket.put_faults["intent.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, partial(rig.launcher().run, spec()))
    rig.scratch_free = 10
    done = rig.launcher().run(spec(2))
    require(rig.ecs.count("run_task") == 0, "a recovering attempt must not launch 100 GiB of scratch into 10 bytes")
    require(done.outcome == "failed_closed" and "scratch_bytes" in done.reason, f"it reports the fit failure: {done}")


def joined_attempts_report_the_owners_outcome() -> None:
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    second = rig.launcher()
    joined = second.attach(spec(2))
    second.start(joined)
    cancelled = rig.launcher().cancel(RUN, "gold", 2)
    require(cancelled.outcome == "cancelled", f"cancel by the joined attempt: {cancelled}")
    require(rig.record(1, "end")["outcome"] == "cancelled" and rig.record(2, "end")["outcome"] == "cancelled", "the owner's end record is finalized with it")
    later = rig.launcher().run(spec(3))
    require(later.outcome == "cancelled" and later.adopted and rig.ecs.count("run_task") == 1, f"every later attempt reports the same outcome: {later}")
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    second = rig.launcher()
    second.start(second.attach(spec(2)))
    rig.ecs.hold = False
    require(rig.launcher().run(spec(1)).ok, "the owner finishes first")
    stops = rig.ecs.count("stop_task")
    late = rig.launcher().cancel(RUN, "gold", 2)
    require(
        late.outcome == "succeeded" and late.adopted and rig.ecs.count("stop_task") == stops,
        f"a late cancel reports the owner's outcome and stops nothing: {late}",
    )
    require(rig.record(2, "end")["outcome"] == "succeeded", "and records it")
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    second = rig.launcher()
    joined = second.attach(spec(2))
    second.start(joined)
    rig.ecs.hold = False
    require(rig.launcher().run(spec(1)).ok, "the owner finishes and records success")
    joined.timed_out = True
    verdict = second.conclude(joined)
    require(
        verdict.outcome == "succeeded" and rig.record(2, "end")["outcome"] == "succeeded",
        f"a joined attempt that decided on a timeout too late reports the owner's success: {verdict}",
    )


def concurrent_recoverers_one_sends_one_closes() -> None:
    """The owner waits for room with its own view of the claim while a second caller sends. A gate pauses the owner at 585 seconds. It runs again while
    the sender's RunTask call is in flight and the window has ended, and must neither close nor send. Gates last at most 10 seconds and the thread is joined."""
    rig = Rig()
    rig.ecs.hold = True
    rig.ecs.instance_free = {"CPU": 1024, "MEMORY": 31000}
    paused, resume = threading.Event(), threading.Event()
    result: dict[str, Any] = {}

    def pause_owner(now: float) -> None:
        if now - START_TIME >= 585 and not paused.is_set():
            paused.set()
            if not resume.wait(10):
                raise AssertionError("the owner was never resumed")

    def owner() -> None:
        try:
            result["owner"] = rig.launcher().run(spec(1))
        except BaseException as error:
            result["owner"] = error

    rig.time.on_sleep = pause_owner
    thread = threading.Thread(target=owner)
    thread.start()
    require(paused.wait(10), "the owner never reached 585 seconds")
    rig.time.on_sleep = None
    rig.ecs.instance_free = {"CPU": 4096, "MEMORY": 31000}

    def owner_runs_during_the_send(phase: str) -> None:
        if phase == "before":
            rig.ecs.run_task_hook = None
            rig.time.now += 40
            resume.set()
            thread.join(10)
            result["events_during"] = rig.events(1)

    rig.ecs.run_task_hook = owner_runs_during_the_send
    sender = rig.launcher()
    joined = sender.attach(spec(2))
    sender.start(joined)
    require(not thread.is_alive(), "the owner did not finish")
    require(isinstance(result["owner"], LaunchUnavailable), f"the owner must leave the launch unresolved while a send is in flight: {result['owner']}")
    require("end" not in result["events_during"], f"and record nothing: {result['events_during']}")
    require(len(gold_tasks(rig)) == 1 and joined.arn == gold_tasks(rig)[0], "the sender's task lands and is the only one")
    require("end" not in rig.events(1), "no terminal outcome exists while the task is live")


def race_of_a_closer(moment: str) -> None:
    rig = Rig()
    rig.bucket.put_faults["intent.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, partial(rig.launcher().run, spec()))
    rig.time.now += 700
    claim = json.loads(rig.bucket.objects[rig.claim_key()])
    sender = Launch(spec(2))

    def sender_wins(key: str) -> None:
        rig.bucket.read_hook = rig.bucket.write_hook = None
        require(rig.launcher().win_send(sender, claim), "the sender takes the send token")

    if moment == "after_the_decision_read":
        rig.bucket.read_hook = lambda key: sender_wins(key) if key.endswith("/cancel.json") else None
    else:
        rig.bucket.write_hook = lambda key: sender_wins(key) if key.endswith("/decision.json") else None
    error = refuses(LaunchUnavailable, partial(rig.launcher().run, spec()))
    require("unresolved" in str(error), f"{moment}: a closer that lost the race leaves the launch unresolved: {error}")
    stands = json.loads(rig.bucket.objects[rig.decision_key()])["decision"] == "send"
    require(rig.events(1) in ([], ["intent"]) and stands, f"{moment}: the send token stands and nothing is closed")


def closer_loses_to_a_sender_that_wins_in_between() -> None:
    race_of_a_closer("after_the_decision_read")
    race_of_a_closer("at_the_close_write")


def stale_listing_blocks_recovery_and_cancel() -> None:
    for action in ("recover", "cancel"):
        rig = Rig()
        rig.ecs.hold = True
        rig.ecs.visible_delay = 720.0
        rig.bucket.put_faults["started.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
        refuses(LaunchUnavailable, partial(rig.launcher().run, spec()))
        require(len(gold_tasks(rig)) == 1, f"{action}: the first call sent one task")
        rig.time.now += 700
        if action == "recover":
            error = refuses(LaunchUnavailable, partial(rig.launcher().run, spec(2)))
        else:
            error = refuses(LaunchUnavailable, partial(rig.launcher().cancel, RUN, "gold", 1))
        require("unresolved" in str(error), f"{action}: an empty listing proves nothing: {error}")
        require("end" not in rig.events(1) and len(gold_tasks(rig)) == 1, f"{action}: no terminal outcome while the task provisions: {rig.events(1)}")
        require(rig.ecs.count("run_task") == 1, f"{action}: and no second task")
        rig.time.now += 100
        if action == "recover":
            rig.ecs.hold = False
            done = rig.launcher().run(spec(2))
            require(done.ok and len(gold_tasks(rig)) == 1, f"{action}: once the task is listed it is joined: {done}")
        else:
            done = rig.launcher().cancel(RUN, "gold", 1)
            require(
                done.outcome == "cancelled" and rig.ecs.tasks[gold_tasks(rig)[0]]["lastStatus"] == "STOPPED", f"{action}: found, stopped, then recorded: {done}"
            )


def counts(rig: Rig, operation: str) -> bool:
    """False for the family gate's task listings (by family), which run before the claim; the discovery listing of a send asks by its identity."""
    return operation != "list_tasks" or "startedBy" in rig.ecs.calls[-1][1]


def send_is_withheld_after_the_reads() -> None:
    def run_with(operation: str, nth: int, interfere: Callable[[Rig], None], low_room: bool = False) -> tuple[Rig, Any]:
        rig = Rig()
        if low_room:
            rig.ecs.instance_free = {"CPU": 4096, "MEMORY": 31000}
        seen = {"count": 0}

        def hook(name: str) -> None:
            if name == operation and counts(rig, name):
                seen["count"] += 1
                if seen["count"] == nth:
                    rig.ecs.call_hook = None
                    interfere(rig)

        rig.ecs.call_hook = hook
        return rig, rig.launcher().run(spec())

    def cancel_now(rig: Rig) -> None:
        with contextlib.suppress(LaunchUnavailable):
            rig.launcher().cancel(RUN, "gold", 1)

    def window_over(rig: Rig) -> None:
        rig.time.now += 700

    rig, done = run_with("describe_container_instances", 1, cancel_now)
    require(rig.ecs.count("run_task") == 0 and done.outcome == "cancelled", f"a cancel during the capacity read withholds the send: {done}")
    rig, done = run_with("list_tasks", 1, cancel_now)
    require(rig.ecs.count("run_task") == 0 and done.outcome == "cancelled", f"a cancel during the discovery read withholds the send: {done}")
    rig, done = run_with("list_tasks", 1, window_over)
    require(rig.ecs.count("run_task") == 0 and done.outcome == "failed_closed", f"the window ending during the discovery read withholds the send: {done}")
    rig, done = run_with("describe_container_instances", 1, window_over)
    require(rig.ecs.count("run_task") == 0 and done.outcome == "failed_closed", f"the window ending during the capacity read withholds the send: {done}")
    rig = Rig()
    rig.ecs.host_full = True
    reads = {"room": 0}

    def cancel_on_second_room_read(name: str) -> None:
        if name == "describe_container_instances":
            reads["room"] += 1
            if reads["room"] == 2:
                rig.ecs.call_hook = None
                cancel_now(rig)

    rig.ecs.call_hook = cancel_on_second_room_read
    done = rig.launcher().run(spec())
    require(done.outcome == "cancelled" and rig.ecs.count("run_task") == 1, f"a repeat after a capacity answer is guarded after the capacity read: {done}")
    rig = Rig()
    rig.ecs.host_full = True
    calls = {"list": 0}

    def cancel_on_repeat(name: str) -> None:
        if name == "list_tasks" and counts(rig, name):
            calls["list"] += 1
            if calls["list"] == 2:
                rig.ecs.call_hook = None
                cancel_now(rig)

    rig.ecs.call_hook = cancel_on_repeat
    done = rig.launcher().run(spec())
    require(
        done.outcome == "cancelled" and rig.ecs.count("run_task") == 1, f"a repeat after a capacity answer is guarded too: {done}, {rig.ecs.count('run_task')}"
    )


def suspended_sender_never_cancelled_while_the_send_may_land() -> None:
    rig = Rig()
    rig.ecs.hold = True
    rig.ecs.visible_delay = 720.0
    seen: dict[str, Any] = {}

    def cancel_while_suspended(phase: str) -> None:
        if phase == "before":
            rig.ecs.run_task_hook = None
            try:
                rig.launcher().cancel(RUN, "gold", 1)
            except LaunchUnavailable as error:
                seen["error"] = str(error)
            seen["events"] = rig.events(1)
            seen["decision"] = rig.bucket.objects.get(f"{PREFIX}/{RUN}/gold/decision.json", b"")

    rig.ecs.run_task_hook = cancel_while_suspended
    stopping = outcome_or_error(partial(rig.launcher().run, spec()))
    require("unresolved" in seen.get("error", ""), f"the cancel cannot close while the send may land: {seen}")
    require("end" not in seen["events"] and b'"send"' in seen["decision"], f"nothing was recorded as cancelled at that moment: {seen}")
    require(isinstance(stopping, LaunchUnavailable) and rig.ecs.count("stop_task") == 1, f"the sender stops the task it started: {stopping!r}")
    require("end" not in rig.events(1), "ECS does not describe the task yet, so the stop is not confirmed and nothing is recorded")
    rig.time.now += 720
    done = rig.launcher().cancel(RUN, "gold", 1)
    require(done.outcome == "cancelled" and rig.ecs.tasks[gold_tasks(rig)[0]]["lastStatus"] == "STOPPED", f"once the task is described STOPPED: {done}")
    require(rig.record(1, "end")["task_arn"] == gold_tasks(rig)[0], "and cancelled is recorded with the ARN of that task")


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
    operations = {name for name, _ in rig.bucket.calls}
    require(operations <= {"put_object", "get_object", "list_objects_v2"}, f"no delete or other call: {operations}")
    listings = [arguments for name, arguments in rig.bucket.calls if name == "list_objects_v2"]
    require(
        len(listings) == 1 and set(listings[0]) <= {"Bucket", "Prefix", "ContinuationToken"} and listings[0]["Prefix"] == f"{PREFIX}/families/gold/",
        f"the one listing is the family index of the job, before the claim: {listings}",
    )
    for name, arguments in rig.bucket.calls:
        if name == "list_objects_v2":
            continue
        key = arguments["Key"]
        pattern = rf"{re.escape(PREFIX)}/({RUN}/gold/([0-9]{{2}}/(intent|started|end)|claim|decision|cancel)|families/gold/{RUN}|absent-probe)\.json"
        require(re.fullmatch(pattern, key) is not None, f"{name} {key} is not a launch record key")
        require(
            name != "put_object" or arguments.get("IfNoneMatch") == "*" or ("IfMatch" in arguments and key.endswith("/decision.json")),
            "every write is conditional",
        )


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


def sdk_launcher(rig: Rig, stub: SdkStub) -> tuple[Launcher, list[str]]:
    """A launcher whose ECS adapter is built as production builds it, over real botocore clients with scripted HTTP answers. The host room still comes
    from the fake cluster. ``order`` lists every guard call and every RunTask request that reached the HTTP layer, in order."""
    probe = EcsFitProbe(rig.ecs, CLUSTER, lambda: rig.scratch_free)
    order: list[str] = []
    launcher = GuardCounting(EcsAdapter.from_session(stub, SETTINGS), S3LaunchStore(rig.bucket, BUCKET, PREFIX), probe, rig.time.clock())
    launcher.order = order
    stub.on_send = lambda operation: order.append(operation) if operation == "RunTask" else None
    return launcher, order


class GuardCounting(Launcher):
    """The real launcher that also notes each guard call in ``order``."""

    order: list[str]

    def guard(self, run_id: str, job: str, end: float) -> None:
        self.order.append("guard")
        super().guard(run_id, job, end)


def outcome_or_error(action: Callable[[], Any]) -> Any:
    try:
        return action()
    except Exception as error:
        return error


def every_send_guarded(order: list[str]) -> bool:
    return all(index > 0 and order[index - 1] == "guard" for index, item in enumerate(order) if item == "RunTask")


SDK_TASK = {"taskArn": f"arn:aws:ecs:us-east-1:{ACCOUNT}:task/{CLUSTER}/{'c' * 32}", "lastStatus": "PROVISIONING", "containers": []}
SDK_NOT_LISTED: dict[str, list[Any]] = {"ListTasks": [{"taskArns": []}]}  # ECS documents no listing guarantee: the accepted task is not listed yet


def sdk_retries_never_hide_an_accepted_send() -> None:
    """The review's probe on the real SDK: the first RunTask request creates the task and its reply is lost (ReadTimeoutError). An SDK retry would send again
    and could answer AccessDeniedException, which reads as a definite refusal and closes the claim with no ARN while the task provisions."""
    rig = Rig()
    stub = SdkStub({**SDK_NOT_LISTED, "RunTask": [ReadTimeoutError(endpoint_url="https://ecs.stand-in.invalid"), ("AccessDeniedException", 400)]})
    launcher, order = sdk_launcher(rig, stub)
    result = outcome_or_error(partial(launcher.run, spec()))
    require(isinstance(result, LaunchUnavailable) and "unresolved" in str(result), f"a send that may have reached ECS keeps the launch unresolved: {result!r}")
    require("end" not in rig.events(1), f"no terminal outcome while the accepted task provisions: {rig.events(1)}")
    require(json.loads(rig.bucket.objects[rig.decision_key()])["decision"] == "send", "the send token is never handed back after an unknown reply")
    require(every_send_guarded(order), f"each RunTask request is preceded by its own guard call: {order}")
    again = outcome_or_error(partial(sdk_launcher(rig, stub)[0].run, spec(2)))
    require(isinstance(again, LaunchUnavailable) and "end" not in rig.events(1), f"a later caller keeps the uncertainty too: {again!r}")


def sdk_sends_only_through_the_guard() -> None:
    """Each request that reaches ECS is preceded by a guard call: no SDK retry resends after a durable cancel or after the end of the window."""
    rig = Rig()
    stub = SdkStub({**SDK_NOT_LISTED, "RunTask": [ReadTimeoutError(endpoint_url="https://ecs.stand-in.invalid"), {"tasks": [SDK_TASK], "failures": []}]})
    launcher, order = sdk_launcher(rig, stub)
    after_cancel: list[str] = []

    def cancel_during_the_first_send(operation: str) -> None:
        if operation != "RunTask":
            return
        order.append(operation)
        if rig.bucket.objects.get(f"{PREFIX}/{RUN}/gold/cancel.json") is not None:
            after_cancel.append(operation)
            return
        with contextlib.suppress(LaunchUnavailable):
            rig.launcher().cancel(RUN, "gold", 1)

    stub.on_send = cancel_during_the_first_send
    result = outcome_or_error(partial(launcher.run, spec()))
    require(after_cancel == [], f"no RunTask request after a durable cancel: {after_cancel}, order {order}")
    require(isinstance(result, LaunchUnavailable) and "end" not in rig.events(1), f"the cancel cannot close a launch whose send may have landed: {result!r}")
    require(every_send_guarded(order), f"each RunTask request is preceded by its own guard call: {order}")
    rig = Rig()
    rig.ecs.instance_free = {"CPU": 1024, "MEMORY": 31000}
    stub = SdkStub({**SDK_NOT_LISTED, "RunTask": [ReadTimeoutError(endpoint_url="https://ecs.stand-in.invalid"), ("AccessDeniedException", 400)]})
    launcher, order = sdk_launcher(rig, stub)
    ages: list[float] = []

    def room_at_599(now: float) -> None:
        if now - START_TIME >= 585:
            rig.time.on_sleep = None
            rig.time.now = START_TIME + 599
            rig.ecs.instance_free = {"CPU": 4096, "MEMORY": 31000}

    def two_seconds_per_send(operation: str) -> None:
        if operation == "RunTask":
            order.append(operation)
            ages.append(rig.time.now - START_TIME)
            rig.time.now += 2

    rig.time.on_sleep = room_at_599
    stub.on_send = two_seconds_per_send
    result = outcome_or_error(partial(launcher.run, spec()))
    require(ages == [599.0], f"one request at claim age 599 s and none after the 600 s window: {ages}")
    require(isinstance(result, LaunchUnavailable) and "end" not in rig.events(1), f"the unknown reply keeps the launch unresolved: {result!r}")
    require(order.count("guard") >= order.count("RunTask") and every_send_guarded(order), f"each request is preceded by a guard call: {order}")


EMPTY_DESCRIPTION: dict[str, Any] = {"tasks": [], "failures": [{"arn": "stand-in", "reason": "MISSING"}]}


def empty_description_is_never_an_outcome() -> None:
    """ECS documents no guarantee that a task it created is described. A reply that names no task, or has no task list at all, is no evidence: the launch
    stays unresolved, and a failed, timed-out or cancelled outcome needs the task shown STOPPED."""
    rig = Rig()
    rig.ecs.describe_overrides = [EMPTY_DESCRIPTION, {}]
    done = rig.launcher().run(spec())
    require(done.ok and rig.record(1, "end")["outcome"] == "succeeded", f"a running task with an empty description is watched until it ends: {done}")
    rig = Rig()
    rig.ecs.hold = True
    arn = die_while_running(rig, spec())
    rig.ecs.describe_overrides = [EMPTY_DESCRIPTION]
    error = refuses(LaunchUnavailable, partial(rig.launcher().run, spec(2)))
    require("end" not in rig.events(1) and rig.events(2) == [], f"another attempt records no outcome for an undescribed task: {rig.events(1)}, {error}")
    require(rig.ecs.tasks[arn]["lastStatus"] != "STOPPED" and rig.ecs.count("run_task") == 1, "the task keeps running and nothing else starts")
    rig.ecs.hold = False
    require(rig.launcher().run(spec(2)).ok, "once the task is described it is joined and settled")
    for reply in (EMPTY_DESCRIPTION, {}):
        rig = Rig()
        rig.ecs.hold = True
        rig.ecs.stop_lag = 3
        arn = die_while_running(rig, spec())
        rig.ecs.describe_overrides = [reply]
        refuses(LaunchUnavailable, partial(rig.launcher().cancel, RUN, "gold", 1))
        require("end" not in rig.events(1) and rig.ecs.tasks[arn]["lastStatus"] != "STOPPED", f"{reply}: a stop that is not shown STOPPED records nothing")
        cancelled = rig.launcher().cancel(RUN, "gold", 1)
        require(
            cancelled.outcome == "cancelled" and rig.ecs.tasks[arn]["lastStatus"] == "STOPPED",
            f"cancelled is recorded only after the task is shown STOPPED: {cancelled}, {rig.ecs.tasks[arn]['lastStatus']}",
        )
    rig = Rig()
    rig.ecs.hold = True
    rig.ecs.stop_lag = 3
    launcher = rig.launcher()
    launch = launcher.attach(spec(deadline_seconds=60))
    launcher.start(launch)
    rig.time.now += 120
    rig.ecs.describe_overrides = [None, {}]
    refuses(LaunchUnavailable, partial(launcher.wait, launch))
    require(not launch.timed_out and "end" not in rig.events(1), "a deadline stop that is not shown STOPPED records nothing")
    launcher.wait(launch)
    require(rig.ecs.tasks[str(launch.arn)]["lastStatus"] == "STOPPED", "a timeout is recorded only after the stopped task is shown STOPPED")
    require(launcher.conclude(launch).outcome == "timeout", "and then it is a timeout")


THIRD_RUN = "20261104T120000Z-abcdef03"


def family_key(run: str, job: str = "gold") -> str:
    return f"{PREFIX}/families/{job}/{run}.json"


def claim_of(rig: Rig, run: str, job: str = "gold") -> bool:
    return f"{PREFIX}/{run}/{job}/claim.json" in rig.bucket.objects


def family_busy_while_a_task_of_the_family_runs() -> None:
    """Owner decision A (Nov 7 2026): the host keeps one scratch folder per family, so a run of a family never starts beside a running task of the family's
    host definition. The refusal comes before the claim, so the refused run is not burned and launches once the family is free."""
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(rig, spec())
    error = refuses(FamilyBusy, lambda: rig.launcher().run(spec(1, run_id=OTHER_RUN)))
    require("has not stopped" in str(error) and "example_dev_job_gold_host" in str(error), f"the refusal names the unstopped task of the family: {error}")
    require(not claim_of(rig, OTHER_RUN) and family_key(OTHER_RUN) not in rig.bucket.objects, "the refused run was claimed or indexed, so it is burned")
    require(rig.ecs.count("run_task") == 1, "a second task of the family was started")
    rig.ecs.hold = False
    for arn in gold_tasks(rig):
        rig.ecs.finish(arn)
    rig.launcher().run(spec())
    done = rig.launcher().run(spec(1, run_id=OTHER_RUN))
    require(done.ok and claim_of(rig, OTHER_RUN), f"the refused run did not launch once the family was free: {done}")


def family_busy_while_an_earlier_launch_has_no_end() -> None:
    """A claimed launch of the family that has no end record may still have a task the cloud does not show (a lost reply, a failed read): the family stays
    closed until that launch is settled, even when ECS lists no running task."""
    rig = Rig()
    rig.ecs.forget_stopped_tokens = True
    rig.ecs.hold = True
    rig.bucket.put_faults["started.json"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require(family_key(RUN) in rig.bucket.objects, "the claimed launch is not in the family index")
    for arn in gold_tasks(rig):
        rig.ecs.finish(arn)
    rig.ecs.hold = False
    rig.ecs.faults["list_tasks"] = []
    error = refuses(FamilyBusy, lambda: rig.launcher().run(spec(1, run_id=OTHER_RUN)))
    require(RUN in str(error) and "no end record" in str(error), f"the refusal names the unsettled launch: {error}")
    require(not claim_of(rig, OTHER_RUN), "the refused run was claimed")
    rig.time.now += 300
    settled = rig.launcher().run(spec(2))
    require(settled.outcome is not None and rig.launcher().run(spec(1, run_id=OTHER_RUN)).ok, "after the earlier launch is settled the next run launches")


def family_gate_reads_or_refuses() -> None:
    """A gate that cannot read the cloud or the index never reads as free: the run is refused unclaimed."""
    rig = Rig()
    rig.ecs.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks")]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require(not claim_of(rig, RUN) and rig.ecs.count("run_task") == 0, "a run started without reading the family's tasks")
    rig = Rig()
    rig.bucket.faults["list_objects_v2"] = [client_error("SlowDown", "ListObjectsV2")]
    refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require(not claim_of(rig, RUN) and rig.ecs.count("run_task") == 0, "a run started without reading the family index")
    rig = Rig()
    rig.bucket.faults["list_objects_v2"] = [client_error("AccessDenied", "ListObjectsV2")]
    refuses(LaunchRefused, lambda: rig.launcher().run(spec()))
    require(not claim_of(rig, RUN), "a run started on a family index it may not list")


def family_gate_spares_other_families_and_its_own_run() -> None:
    """Another family's running task never closes this family, and a retry of a claimed run settles its own launch instead of meeting its own index."""
    rig = Rig()
    rig.ecs.hold = True
    die_while_running(
        rig,
        spec(
            job="drift",
            environment={"drift": {"RUN_ID": RUN}},
            acceptance=(),
            log_container="drift",
            log_prefix="drift",
            need=Resources(cpu_units=512, memory_mib=1024, scratch_bytes=1),
        ),
    )
    launcher = rig.launcher()
    launch = launcher.attach(spec())
    launcher.start(launch)
    require(launch.arn is not None and claim_of(rig, RUN), "a running drift task closed the gold family")
    again = rig.launcher()
    repeat = again.attach(spec(2))
    again.start(repeat)
    require(repeat.arn == launch.arn and rig.ecs.count("run_task") == 2, "a retry of the same run met its own index instead of joining its task")
    require(
        sorted(key for key in rig.bucket.objects if "/families/" in key) == sorted([family_key(RUN), family_key(RUN, "drift")]),
        "the index is not one key per run",
    )
    require(json.loads(rig.bucket.objects[family_key(RUN)])["attempt"] == 1, "the index does not name the claimed attempt")
    require(not claim_of(rig, THIRD_RUN), "a run that was never asked for was claimed")


def family_busy_while_a_task_of_the_family_is_stopping() -> None:
    """A stopping task (desired status STOPPED, containers still running) still writes to the family's scratch folder, so the gate admits a new run only
    when every listed task of the family's host definition shows last status STOPPED (round 2 review, H1). The task here was not started by the launcher,
    so only the cloud shows it."""
    rig = Rig()
    rig.ecs.stop_lag = 3
    arn = rig.adapter.start("gold", "20261104T090000Z-99999999:gold:1", ENVIRONMENT, "", HOST)
    rig.ecs.stop_task(cluster=CLUSTER, task=arn, reason="owner stop")
    require(rig.ecs.tasks[arn]["desiredStatus"] == "STOPPED" and rig.ecs.tasks[arn]["lastStatus"] != "STOPPED", "the stand-in task is not stopping")
    error = refuses(FamilyBusy, lambda: rig.launcher().run(spec()))
    require(arn in str(error) and "has not stopped" in str(error), f"the refusal does not name the stopping task: {error}")
    require(not claim_of(rig, RUN) and rig.ecs.count("run_task") == 1, "a run was claimed or started beside a stopping task of its family")
    while rig.ecs.tasks[arn]["lastStatus"] != "STOPPED":
        rig.ecs.describe_tasks(cluster=CLUSTER, tasks=[arn])
    done = rig.launcher().run(spec())
    require(done.ok, f"the run did not launch once the stopping task showed STOPPED: {done}")


def family_index_write_with_a_lost_reply() -> None:
    """The family index write can lose its reply; the store then reads the entry back with the index reader, not the launch record reader (review M5)."""
    rig = Rig()
    rig.bucket.lose_reply_once = True
    done = rig.launcher().run(spec())
    require(done.ok and family_key(RUN) in rig.bucket.objects, f"a lost reply of the index write stopped the launch: {done}")
    rig = Rig()
    rig.bucket.objects[family_key(RUN)] = b'{"event": "family"}'
    rig.bucket.faults["put_object"] = [ReadTimeoutError(endpoint_url="https://s3.invalid")]
    error = refuses(LaunchError, lambda: rig.launcher().run(spec()))
    require("family index" in str(error) and not claim_of(rig, RUN), f"a malformed index entry was accepted or the run was claimed: {error}")


def unresolved_gold(rig: Rig) -> None:
    """A launch whose every RunTask call lost its reply before ECS created anything: the send token is held, no task exists, and the launch stays
    unresolved after its window."""
    rig.ecs.faults["run_task"] = [ReadTimeoutError(endpoint_url="https://ecs.invalid")] * 200
    error = refuses(LaunchUnavailable, lambda: rig.launcher().run(spec()))
    require("Manual owner path" in str(error) and "settle" in str(error), f"the unresolved launch does not name the owner's settle path: {error}")
    rig.ecs.faults["run_task"] = []
    require(not gold_tasks(rig) and "end" not in rig.events(1), "the stand-in launch is not unresolved")


def owner_settles_an_unresolved_launch_and_admits_a_new_run() -> None:
    """The owner path ends an unresolved launch durably (review M7): after the send window, with no task of the family that has not stopped, and with the
    owner's statement of what was checked, the claim gets a failed_closed end record, every later caller reports it, and a new run of the family launches."""
    rig = Rig()
    unresolved_gold(rig)
    refuses(FamilyBusy, lambda: rig.launcher().run(spec(1, run_id=OTHER_RUN)))
    refuses(LaunchRefused, lambda: rig.launcher().owner_settle(RUN, "gold", 2, "checked"))
    require("end" not in rig.events(1), "a settle without a statement of what was checked ended the launch")
    statement = "ECS console and CloudTrail show no RunTask of this identity created a task"
    settled = rig.launcher().owner_settle(RUN, "gold", 2, statement)
    require(settled.outcome == "failed_closed" and settled.reason.startswith("owner_settled:"), f"the settle did not end the launch: {settled}")
    require(rig.record(1, "end")["outcome"] == "failed_closed" and f"{PREFIX}/{RUN}/gold/cancel.json" in rig.bucket.objects, "no durable end or cancel")
    sent = rig.ecs.count("run_task")
    again = rig.launcher().run(spec(3))
    require(
        again.outcome == "failed_closed" and rig.ecs.count("run_task") == sent and not gold_tasks(rig), f"a later caller of the settled run launched: {again}"
    )
    fresh = rig.launcher().run(spec(1, run_id=OTHER_RUN))
    require(fresh.ok, f"a new run of the family was not admitted after the settle: {fresh}")


def owner_settle_needs_the_family_stopped_and_the_window_over() -> None:
    """The settle never ends a launch while a task of the family has not stopped, or while a sender of the launch may still send."""
    rig = Rig()
    unresolved_gold(rig)
    rig.ecs.hold = True
    other = rig.adapter.start("gold", "20261104T090000Z-99999999:gold:1", ENVIRONMENT, "", HOST)
    statement = "ECS console and CloudTrail show no RunTask of this identity created a task"
    error = refuses(FamilyBusy, lambda: rig.launcher().owner_settle(RUN, "gold", 2, statement))
    require(other in str(error) and "end" not in rig.events(1), f"the settle ended a launch beside an unstopped task of the family: {error}")
    rig = Rig()
    rig.ecs.faults["run_task"] = [ReadTimeoutError(endpoint_url="https://ecs.invalid")]
    launcher = rig.launcher()
    launch = launcher.attach(spec())
    rig.time.on_sleep = lambda now: (_ for _ in ()).throw(Interrupted())
    with contextlib.suppress(Interrupted):
        launcher.start(launch)
    rig.time.on_sleep = None
    require(not gold_tasks(rig) and rig.decision_key() in rig.bucket.objects, "the stand-in sender did not hold the send token without a task")
    error = refuses(LaunchUnavailable, lambda: rig.launcher().owner_settle(RUN, "gold", 2, statement))
    require("window" in str(error) and "end" not in rig.events(1), f"the settle ended a launch whose sender may still send: {error}")


GIB = 2**30


def fit_credits_the_family_own_idle_scratch() -> None:
    """Owner decision A (Nov 7 2026, H2): the task's first container empties the launching family's own scratch folder, and the gate has shown the family
    idle, so admission counts the bytes in that folder as free. The host's free space alone is short here."""
    rig = Rig()
    rig.scratch_free = NEED.scratch_bytes - 10 * GIB
    done = rig.launcher(scratch_usage=lambda: {"gold": 20 * GIB, "silver": 0}).run(spec())
    require(done.ok and rig.ecs.count("run_task") == 1, f"the family's own reclaimable scratch was not counted: {done}")


def fit_never_credits_another_family_and_names_it() -> None:
    """Another family's leftovers are not this task's to empty: they still block, and the fit report names each family that holds scratch."""
    rig = Rig()
    rig.scratch_free = NEED.scratch_bytes - 10 * GIB
    done = rig.launcher(scratch_usage=lambda: {"gold": 0, "silver": 30 * GIB, "drift": 5 * GIB, "train": 0}).run(spec())
    require(done.outcome == "failed_closed" and rig.ecs.count("run_task") == 0, f"another family's scratch was counted as free: {done}")
    require("silver" in done.reason and "drift" in done.reason and "train" not in done.reason, f"the fit report does not name the holders: {done.reason}")


def fit_without_a_usage_reader_counts_free_space_only() -> None:
    """Without a usage reader (a caller that cannot see the scratch folders) the free space alone decides, as before."""
    rig = Rig()
    rig.scratch_free = NEED.scratch_bytes - 10 * GIB
    done = rig.launcher().run(spec())
    require(done.outcome == "failed_closed" and "scratch_bytes" in done.reason, f"a short host was admitted without evidence of reclaimable scratch: {done}")


CASES: dict[str, Callable[[], None]] = {
    "fit_credits_the_family_own_idle_scratch": fit_credits_the_family_own_idle_scratch,
    "fit_never_credits_another_family_and_names_it": fit_never_credits_another_family_and_names_it,
    "fit_without_a_usage_reader_counts_free_space_only": fit_without_a_usage_reader_counts_free_space_only,
    "family_busy_while_a_task_of_the_family_is_stopping": family_busy_while_a_task_of_the_family_is_stopping,
    "family_index_write_with_a_lost_reply": family_index_write_with_a_lost_reply,
    "owner_settles_an_unresolved_launch_and_admits_a_new_run": owner_settles_an_unresolved_launch_and_admits_a_new_run,
    "owner_settle_needs_the_family_stopped_and_the_window_over": owner_settle_needs_the_family_stopped_and_the_window_over,
    "family_busy_while_a_task_of_the_family_runs": family_busy_while_a_task_of_the_family_runs,
    "family_busy_while_an_earlier_launch_has_no_end": family_busy_while_an_earlier_launch_has_no_end,
    "family_gate_reads_or_refuses": family_gate_reads_or_refuses,
    "family_gate_spares_other_families_and_its_own_run": family_gate_spares_other_families_and_its_own_run,
    "no_terminal_outcome_while_a_launch_may_be_live": no_terminal_outcome_while_a_launch_may_be_live,
    "aged_recovery_never_replays_runtask": aged_recovery_never_replays_runtask,
    "claim_that_sent_nothing_an_hour_later": claim_that_sent_nothing_an_hour_later,
    "cancel_before_runtask_is_accepted": cancel_before_runtask_is_accepted,
    "recovery_enforces_the_claim_needs": recovery_enforces_the_claim_needs,
    "joined_attempts_report_the_owners_outcome": joined_attempts_report_the_owners_outcome,
    "concurrent_recoverers_one_sends_one_closes": concurrent_recoverers_one_sends_one_closes,
    "closer_loses_to_a_sender_that_wins_in_between": closer_loses_to_a_sender_that_wins_in_between,
    "stale_listing_blocks_recovery_and_cancel": stale_listing_blocks_recovery_and_cancel,
    "send_is_withheld_after_the_reads": send_is_withheld_after_the_reads,
    "suspended_sender_never_cancelled_while_the_send_may_land": suspended_sender_never_cancelled_while_the_send_may_land,
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
    "sdk_retries_never_hide_an_accepted_send": sdk_retries_never_hide_an_accepted_send,
    "sdk_sends_only_through_the_guard": sdk_sends_only_through_the_guard,
    "empty_description_is_never_an_outcome": empty_description_is_never_an_outcome,
}
