"""Direct cases for the ECS adapter: the failures a real broker run cannot readily produce (lost replies, throttling, container exit codes).

Isolation is justified because the fake cluster has to be told when a reply is lost or a container exits non-zero. The broker E2E reaches the same
adapter through the HTTP route. Every case runs offline against ``FakeEcs``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from botocore.config import Config

from orchestration import ecs_adapter
from orchestration.ecs_adapter import EcsAdapter, EcsSettings, TaskRefused, TaskUnavailable
from scripts.orchestration.ecs_broker_fake import ACCOUNT, CLUSTER, FakeEcs, FakeLogs, client_error

SETTINGS = EcsSettings(
    cluster=CLUSTER,
    family_prefix="example_dev_job_",
    subnets=("subnet-0example",),
    security_groups=("sg-0example",),
    project_tag="example",
    log_group="example_jobs_dev",
    families=frozenset({"train"}),
)
ENVIRONMENT = {"fetch": {"RUN_ID": "20261030T100000Z-abcdef01"}, "train": {"RUN_ID": "20261030T100000Z-abcdef01"}}
KEY = "20261030T100000Z-abcdef01:train:1"


def build() -> tuple[EcsAdapter, FakeEcs, FakeLogs]:
    ecs, logs = FakeEcs(), FakeLogs()
    return EcsAdapter(ecs, logs, SETTINGS), ecs, logs


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def raises(error: type[Exception], action: Callable[[], Any]) -> None:
    try:
        action()
    except error:
        return
    raise AssertionError(f"expected {error.__name__}")


def settle(adapter: EcsAdapter, arn: str) -> ecs_adapter.TaskReport:
    """Poll until the task leaves the pending and running states (the fake stops it on its third poll)."""
    for _ in range(10):
        report = adapter.poll(arn)
        if report.state not in ("pending", "running"):
            return report
    raise AssertionError("the task never ended")


def start_twice_one_task() -> None:
    adapter, ecs, _ = build()
    first, second = adapter.start("train", KEY, ENVIRONMENT), adapter.start("train", KEY, ENVIRONMENT)
    require(first == second and len(ecs.tasks) == 1, "a second start with the same key made a second task")


def lost_reply_one_task() -> None:
    adapter, ecs, _ = build()
    ecs.lose_reply_once = True
    raises(TaskUnavailable, lambda: adapter.start("train", KEY, ENVIRONMENT))
    require(len(ecs.tasks) == 1, "the lost reply case did not create the task")
    arn = adapter.start("train", KEY, ENVIRONMENT)
    require(len(ecs.tasks) == 1 and arn in ecs.tasks, "the retry after a lost reply made a second task")


def another_key_another_task() -> None:
    adapter, ecs, _ = build()
    adapter.start("train", KEY, ENVIRONMENT)
    adapter.start("train", KEY.replace(":1", ":2"), ENVIRONMENT)
    require(len(ecs.tasks) == 2, "a new attempt did not start its own task")


def run_task_request() -> None:
    adapter, ecs, _ = build()
    adapter.start("train", KEY, ENVIRONMENT)
    request = next(arguments for name, arguments in ecs.calls if name == "run_task")
    network = request["networkConfiguration"]["awsvpcConfiguration"]
    names = [item["name"] for item in request["overrides"]["containerOverrides"]]
    require(request["taskDefinition"] == "example_dev_job_train" and request["count"] == 1, "wrong task definition or count")
    require(request["capacityProviderStrategy"] == [{"capacityProvider": "FARGATE", "weight": 1}], "the capacity provider is not on-demand Fargate")
    require(network["assignPublicIp"] == "DISABLED" and network["subnets"] == ["subnet-0example"], "wrong network")
    require(request["propagateTags"] == "TASK_DEFINITION" and {"key": "Project", "value": "example"} in request["tags"], "tags are not propagated")
    require(names == ["fetch", "train"], "overrides name a container that was not asked for")
    require(0 < len(request["clientToken"]) <= 64 and request["startedBy"].startswith("hai-"), "no client token or startedBy")


def family_not_allowed() -> None:
    adapter, ecs, _ = build()
    raises(TaskRefused, lambda: adapter.start("write_gold", KEY, ENVIRONMENT))
    require(not ecs.calls, "an unnamed family reached the cloud")


def all_containers_zero_passes() -> None:
    adapter, _, _ = build()
    arn = adapter.start("train", KEY, ENVIRONMENT)
    require(adapter.poll(arn).state == "pending", "a new task was not pending")
    report = settle(adapter, arn)
    require(report.state == "succeeded" and report.exit_code == 0, f"all zero exits did not pass: {report}")


def one_container_nonzero_fails() -> None:
    adapter, ecs, _ = build()
    ecs.plan = {"init": 0, "fetch": 0, "prepare": 0, "extract": 0, "train": 3}
    report = settle(adapter, adapter.start("train", KEY, ENVIRONMENT))
    require(report.state == "failed" and report.exit_code == 3, f"a non-zero exit did not fail the job: {report}")


def early_failure_names_first_nonzero() -> None:
    adapter, ecs, _ = build()
    ecs.plan = {"init": 0, "fetch": 2, "prepare": None, "extract": None, "train": None}
    report = settle(adapter, adapter.start("train", KEY, ENVIRONMENT))
    require(report.state == "failed" and report.exit_code == 2, f"the fetch failure was not reported: {report}")


def stopped_without_exit_codes_fails() -> None:
    adapter, ecs, _ = build()
    ecs.plan = {name: None for name in ("init", "fetch", "train")}
    report = settle(adapter, adapter.start("train", KEY, ENVIRONMENT))
    require(report.state == "failed" and report.exit_code == 125, f"a task with no exit codes passed or lost its code: {report}")


def unknown_task_is_gone() -> None:
    adapter, _, _ = build()
    require(adapter.poll(f"arn:aws:ecs:stand-in:{ACCOUNT}:task/stand-in-cluster/missing").state == "gone", "a missing task was not reported gone")


def stop_is_repeatable() -> None:
    adapter, ecs, _ = build()
    arn = adapter.start("train", KEY, ENVIRONMENT)
    adapter.stop(arn, "cancel")
    adapter.stop(arn, "cancel")
    require(ecs.tasks[arn]["lastStatus"] == "STOPPED", "the task was not stopped")
    report = adapter.poll(arn)
    require(report.state == "failed" and report.exit_code == 137, f"a stopped task was not reported: {report}")


def throttled_start_is_transient() -> None:
    adapter, ecs, _ = build()
    ecs.faults["run_task"] = [client_error("ThrottlingException", "RunTask")]
    raises(TaskUnavailable, lambda: adapter.start("train", KEY, ENVIRONMENT))
    require(not ecs.tasks, "a throttled call created a task")


def denied_start_is_permanent() -> None:
    adapter, ecs, _ = build()
    ecs.faults["run_task"] = [client_error("AccessDeniedException", "RunTask")]
    raises(TaskRefused, lambda: adapter.start("train", KEY, ENVIRONMENT))


def throttled_poll_is_transient() -> None:
    adapter, ecs, _ = build()
    arn = adapter.start("train", KEY, ENVIRONMENT)
    ecs.faults["describe_tasks"] = [client_error("ThrottlingException", "DescribeTasks")]
    raises(TaskUnavailable, lambda: adapter.poll(arn))


def reasons_hide_identifiers() -> None:
    adapter, ecs, _ = build()
    arn = adapter.start("train", KEY, ENVIRONMENT)
    account = "7" * 12
    ecs.tasks[arn]["stoppedReason"] = f"failed for {arn} in account {account}"
    ecs.tasks[arn]["lastStatus"] = "STOPPED"
    report = adapter.poll(arn)
    require("arn:" not in report.reason and account not in report.reason, f"the reason shows an identifier: {report.reason}")


def tail_reads_the_container_stream() -> None:
    adapter, ecs, logs = build()
    arn = adapter.start("train", KEY, ENVIRONMENT)
    stream = f"train/train/{arn.rsplit('/', 1)[-1]}"
    logs.streams[stream] = ["one", "two", "three"]
    require(adapter.tail(arn, "train", "train", 2) == "two\nthree", "the tail is wrong")
    require(adapter.tail(arn, "train", "missing", 2) == "", "a missing stream did not give an empty tail")
    require(logs.calls[0]["logGroupName"] == "example_jobs_dev", "wrong log group")


def client_timeouts() -> None:
    config = ecs_adapter.client_config()
    require(isinstance(config, Config), "no client configuration")
    require(config.connect_timeout == 5 and config.read_timeout == 10 and config.retries == {"max_attempts": 2, "mode": "standard"}, "calls have no deadline")


CASES: dict[str, Callable[[], None]] = {
    "start_twice_one_task": start_twice_one_task,
    "lost_reply_one_task": lost_reply_one_task,
    "another_key_another_task": another_key_another_task,
    "run_task_request": run_task_request,
    "family_not_allowed": family_not_allowed,
    "all_containers_zero_passes": all_containers_zero_passes,
    "one_container_nonzero_fails": one_container_nonzero_fails,
    "early_failure_names_first_nonzero": early_failure_names_first_nonzero,
    "stopped_without_exit_codes_fails": stopped_without_exit_codes_fails,
    "unknown_task_is_gone": unknown_task_is_gone,
    "stop_is_repeatable": stop_is_repeatable,
    "throttled_start_is_transient": throttled_start_is_transient,
    "denied_start_is_permanent": denied_start_is_permanent,
    "throttled_poll_is_transient": throttled_poll_is_transient,
    "reasons_hide_identifiers": reasons_hide_identifiers,
    "tail_reads_the_container_stream": tail_reads_the_container_stream,
    "client_timeouts": client_timeouts,
}
