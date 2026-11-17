"""Direct cases for the ECS adapter: the failures a real launch cannot readily produce (lost replies, throttling, container exit codes).

Isolation is justified because the fake cluster has to be told when a reply is lost or a container exits non-zero. The launch E2E reaches the same
adapter through the launch library. Every case runs offline against ``FakeEcs``.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError, ReadTimeoutError

from orchestration import ecs_adapter
from orchestration.ecs_adapter import EcsAdapter, EcsSettings, TaskRefused, TaskUnavailable
from scripts.orchestration.ecs_fake import ACCOUNT, CLUSTER, FakeEcs, FakeLogs, SdkStub, client_error

JOBS_FILE = Path(__file__).resolve().parents[2] / "config/orchestration/ecs_broker_jobs.json"
SETTINGS = EcsSettings(
    cluster=CLUSTER,
    family_prefix="example_dev_job_",
    project_tag="example",
    log_group="example_jobs_dev",
    families=frozenset({"train"}),
    host_capacity_provider=FakeEcs().host_provider,
)
ENVIRONMENT = {"fetch": {"RUN_ID": "silver_abcdef01"}, "train": {"RUN_ID": "silver_abcdef01"}}
KEY = "silver_abcdef01:train:1"


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
    names = [item["name"] for item in request["overrides"]["containerOverrides"]]
    require(request["taskDefinition"] == "example_dev_job_train_host" and request["count"] == 1, "wrong task definition or count")
    require(request["capacityProviderStrategy"] == [{"capacityProvider": ecs.host_provider, "weight": 1}], "the capacity provider is not the host")
    require("networkConfiguration" not in request, "a bridge task was given an awsvpc network")
    require("launchType" not in request, "a launch type was sent beside the capacity provider strategy")
    require(
        request["propagateTags"] == "TASK_DEFINITION" and request["tags"] == [{"key": "Project", "value": "example"}, {"key": "HaiJob", "value": "train"}],
        "wrong tags: the HaiJob tag is the plain family name",
    )
    require(names == ["fetch", "train"], "overrides name a container that was not asked for")
    require(
        request["clientToken"] == hashlib.sha256(KEY.encode()).hexdigest() and request["startedBy"] == EcsAdapter.identity(KEY), "the launch identity changed"
    )


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


def layer_task(layer: str, codes: dict[str, int | None], stop_code: str = "EssentialContainerExited") -> tuple[EcsAdapter, str]:
    """A stopped task of a layer job as ECS reports it: init, the containers of the job catalog in order and the guard, each with the exit code given."""
    adapter, ecs, _ = build()
    arn = adapter.start("train", KEY, ENVIRONMENT)
    table = json.loads(JOBS_FILE.read_text())["jobs"][layer]["containers"]
    containers = []
    for name in ["init", *table, "guard"]:
        entry: dict[str, Any] = {"name": name, "lastStatus": "STOPPED"}
        if codes.get(name) is not None:
            entry["exitCode"] = codes[name]
        containers.append(entry)
    ecs.tasks[arn].update(lastStatus="STOPPED", containers=containers, stopCode=stop_code, stoppedReason="Essential container in task exited")
    return adapter, arn


def layer_tasks_with_a_guard() -> None:
    """The guard is an extra container of every layer task. A good run ends with its exit code 0, 143 (SIGTERM) or 137 (SIGKILL) or none, and still passes; a
    failed phase fails the job with the phase's own exit code and never with the guard's."""
    for layer in ("silver", "gold", "features"):
        table = json.loads(JOBS_FILE.read_text())["jobs"][layer]["containers"]
        good = {name: 0 for name in ["init", *table]}
        for guard in (0, 143, 137, None):
            adapter, arn = layer_task(layer, {**good, "guard": guard})
            report = adapter.poll(arn)
            require(report.state == "succeeded" and report.exit_code == 0, f"{layer}: a good run with guard exit {guard} did not pass: {report}")
        phases = list(table)
        adapter, arn = layer_task(layer, {"init": 0, "fetch": 0, "pin": 0, phases[2]: 1, "guard": 1})
        report = adapter.poll(arn)
        require(report.state == "failed" and report.exit_code == 1, f"{layer}: a failed {phases[2]} did not fail the job with its code: {report}")
        adapter, arn = layer_task(layer, {**good, phases[-2]: 3, phases[-1]: None, "guard": 143})
        report = adapter.poll(arn)
        require(report.state == "failed" and report.exit_code == 3, f"{layer}: the exit code of the guard replaced the exit code of the phase: {report}")
        adapter, arn = layer_task(layer, {"guard": 1})
        report = adapter.poll(arn)
        require(
            report.state == "failed" and report.exit_code == 125, f"{layer}: a task whose phases never ran and whose guard stopped it did not fail: {report}"
        )
        adapter, arn = layer_task(layer, {name: 137 for name in ["init", *table, "guard"]}, "UserInitiated")
        report = adapter.poll(arn)
        require(report.state == "failed" and report.exit_code == 137, f"{layer}: a task stopped by the owner did not fail with the stop code: {report}")


def undescribed_task_is_unavailable() -> None:
    """A DescribeTasks reply that describes no task, or has no task list, says nothing about the task: the read is unavailable, never an end."""
    adapter, _, _ = build()
    raises(TaskUnavailable, lambda: adapter.poll(f"arn:aws:ecs:stand-in:{ACCOUNT}:task/stand-in-cluster/missing"))
    stub = SdkStub({"DescribeTasks": [{}, {"tasks": [], "failures": []}]})
    adapter = EcsAdapter.from_session(stub, SETTINGS)
    for shape in ("no task list", "an empty task list"):
        try:
            report = adapter.poll(f"arn:aws:ecs:us-east-1:{ACCOUNT}:task/{CLUSTER}/{'d' * 32}")
        except TaskUnavailable:
            continue
        raise AssertionError(f"real botocore, a reply with {shape} was read as {report}")
    require(stub.sent == ["DescribeTasks", "DescribeTasks"], f"one request each: {stub.sent}")


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
    options = vars(config)
    require(
        options["connect_timeout"] == 5 and options["read_timeout"] == 10 and options["retries"] == {"max_attempts": 2, "mode": "standard"},
        "calls have no deadline",
    )
    sending = vars(ecs_adapter.send_config())
    require(
        sending["connect_timeout"] == 5 and sending["read_timeout"] == 10 and sending["retries"] == {"total_max_attempts": 1, "mode": "standard"},
        f"RunTask has the same deadline and makes one attempt: {sending['retries']}",
    )


LOST = ReadTimeoutError(endpoint_url="https://ecs.stand-in.invalid")


def runtask_sends_once_through_the_sdk() -> None:
    """Real botocore with scripted HTTP answers: the first RunTask request reaches ECS and its reply is lost, a second one would be refused. The adapter
    built as production builds it sends one request and reports the unknown answer, never the refusal of a retry it did not make."""
    stub = SdkStub({"ListTasks": [{"taskArns": []}], "RunTask": [LOST, ("AccessDeniedException", 400)]})
    adapter = EcsAdapter.from_session(stub, replace(SETTINGS, host_capacity_provider="stand-in_host"))
    guards: list[str] = []
    try:
        adapter.start("train", KEY, ENVIRONMENT, guard=lambda: guards.append("guard"))
    except ecs_adapter.SendUnknown:
        pass
    except TaskRefused as error:
        raise AssertionError(f"an SDK retry hid the lost reply of an accepted request behind its refusal: {error}, sent {stub.sent}") from None
    else:
        raise AssertionError("expected SendUnknown")
    require(
        stub.sent.count("RunTask") == 1 and guards == ["guard", "guard"],
        f"one request, and the guard ran before the call and again at the send boundary: {stub.sent}, {guards}",
    )
    stub = SdkStub({"ListTasks": [{"taskArns": []}], "RunTask": [("ThrottlingException", 400), ("AccessDeniedException", 400)]})
    adapter = EcsAdapter.from_session(stub, replace(SETTINGS, host_capacity_provider="stand-in_host"))
    raises(TaskUnavailable, lambda: adapter.start("train", KEY, ENVIRONMENT))
    require(stub.sent.count("RunTask") == 1, f"a throttled RunTask is not resent by the SDK either: {stub.sent}")
    reads = SdkStub({"ListTasks": [("ThrottlingException", 400), {"taskArns": []}]})
    require(EcsAdapter.from_session(reads, SETTINGS).find(KEY) is None and reads.sent == ["ListTasks", "ListTasks"], f"reads keep their retries: {reads.sent}")


def retrying_runtask_client_refused() -> None:
    """A RunTask client with SDK retries could resend after the caller's guard and hide a lost reply, so the adapter refuses it when it is built."""
    stub = SdkStub({})
    for config in (ecs_adapter.client_config(), None):
        try:
            EcsAdapter(stub.client("ecs", config=config), FakeLogs(), SETTINGS)
        except ValueError as error:
            require("one attempt" in str(error), f"the refusal names the rule: {error}")
        else:
            raise AssertionError(f"a RunTask client with retries {config and vars(config)['retries']} was accepted")
    EcsAdapter(stub.client("ecs", config=ecs_adapter.client_config()), FakeLogs(), SETTINGS, sender=stub.client("ecs", config=ecs_adapter.send_config()))


NINE_FAMILIES = frozenset({"train", "silver", "gold", "features", "drift", "promote", "serve", "metadata", "bronze"})


def host_family_map() -> None:
    adapter, _, _ = build()
    jobs = json.loads(JOBS_FILE.read_text())["jobs"]
    require(set(jobs) == NINE_FAMILIES, f"the job table names other families than the nine: {sorted(jobs)}")
    for family in jobs:
        require(adapter.definition(family) == f"example_dev_job_{family}_host", f"{family}: the host definition is not <prefix><family>_host")
    require(len({adapter.definition(family) for family in jobs}) == len(jobs), "two definitions share a name")


def host_provider_required() -> None:
    ecs, logs = FakeEcs(), FakeLogs()
    adapter = EcsAdapter(ecs, logs, replace(SETTINGS, host_capacity_provider=""))
    raises(TaskRefused, lambda: adapter.start("train", KEY, ENVIRONMENT))
    require(not ecs.calls, "a start without a host capacity provider reached the cloud, where the cluster default would place it")


def host_start_twice_one_task() -> None:
    adapter, ecs, _ = build()
    first, second = (
        adapter.start("train", KEY, ENVIRONMENT),
        adapter.start("train", KEY, ENVIRONMENT),
    )
    require(first == second and len(ecs.tasks) == 1, "a second host start with the same key made a second task")
    ecs.lose_reply_once = True
    lost = KEY.replace(":1", ":2")
    raises(TaskUnavailable, lambda: adapter.start("train", lost, ENVIRONMENT))
    arn = adapter.start("train", lost, ENVIRONMENT)
    require(len(ecs.tasks) == 2 and arn in ecs.tasks, "the retry after a lost reply made a second host task")


def host_task_shape() -> None:
    adapter, ecs, _ = build()
    host = ecs.tasks[adapter.start("train", KEY, ENVIRONMENT)]
    require(host["launchType"] == "EC2" and host["capacityProviderName"] == ecs.host_provider, f"not an EC2 task of the host provider: {host}")
    require("containerInstanceArn" in host and "attachments" not in host, "a bridge task has a container instance and no network interface")


def host_capacity_exhausted_is_transient() -> None:
    adapter, ecs, _ = build()
    ecs.host_full = True
    raises(TaskUnavailable, lambda: adapter.start("train", KEY, ENVIRONMENT))
    require(not ecs.tasks, "a full host created a task")
    ecs.host_full = False
    adapter.start("train", KEY, ENVIRONMENT)
    require(len(ecs.tasks) == 1, "the retry after the host had room made no task")


def capacity_answer_is_definite() -> None:
    adapter, ecs, _ = build()
    ecs.host_full = True
    try:
        adapter.start("train", KEY, ENVIRONMENT)
    except ecs_adapter.CapacityUnavailable:
        pass
    else:
        raise AssertionError("a full host must answer with the capacity error: no task was created")
    ecs.host_full = False
    ecs.lose_reply_once = True
    try:
        adapter.start("train", KEY, ENVIRONMENT)
    except ecs_adapter.CapacityUnavailable:
        raise AssertionError("a lost reply is not a capacity answer: a task may exist") from None
    except TaskUnavailable:
        pass
    require(len(ecs.tasks) == 1, "the lost reply case did not create the task")


def guard_runs_just_before_the_send() -> None:
    adapter, ecs, _ = build()
    order: list[str] = []
    ecs.call_hook = order.append

    def refuse() -> None:
        order.append("guard")
        raise ecs_adapter.SendWithheld

    raises(ecs_adapter.SendWithheld, lambda: adapter.start("train", KEY, ENVIRONMENT, guard=refuse))
    require(order == ["list_tasks", "guard"] and not ecs.tasks, f"the guard comes after the discovery and before RunTask, and a refusal sends nothing: {order}")
    seen: list[str] = []
    ecs.call_hook = None
    adapter.start("train", KEY, ENVIRONMENT, guard=lambda: seen.append("guard"))
    adapter.start("train", KEY, ENVIRONMENT, guard=lambda: seen.append("guard"))
    require(seen == ["guard"], "a task that already runs is returned without the guard or a send")


def unknown_reply_is_told_from_an_unsent_call() -> None:
    adapter, ecs, _ = build()
    ecs.lose_reply_once = True
    raises(ecs_adapter.SendUnknown, lambda: adapter.start("train", KEY, ENVIRONMENT))
    adapter2, ecs2, _ = build()
    ecs2.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks")]
    try:
        adapter2.start("train", KEY, ENVIRONMENT)
    except ecs_adapter.SendUnknown:
        raise AssertionError("a throttled discovery sent nothing and must not read as an unknown send") from None
    except TaskUnavailable:
        pass
    require(ecs2.count("run_task") == 0, "and no RunTask call was made")


TRAIN_HOST = "example_dev_job_train_host"


def lookup_finds_running_and_stopped_tasks() -> None:
    adapter, ecs, _ = build()
    require(adapter.lookup(KEY, TRAIN_HOST) is None, "a task was found where none was started")
    arn = adapter.start("train", KEY, ENVIRONMENT)
    found = adapter.lookup(KEY, TRAIN_HOST)
    require(found is not None and found.arn == arn and found.created_at is None, f"running task not found: {found}")
    ecs.finish(arn)
    require(adapter.find(KEY) is None, "the running listing must miss a stopped task: that is what lookup has to cover")
    for number in range(5):
        decoy = adapter.start("train", KEY.replace(":1", f":{number + 2}"), ENVIRONMENT)
        ecs.finish(decoy)
    ecs.list_page = 2
    found = adapter.lookup(KEY, TRAIN_HOST)
    require(found is not None and found.arn == arn, f"a stopped task past the first listing page was not found: {found}")
    require(adapter.lookup(KEY.replace(":1", ":9"), TRAIN_HOST) is None, "another identity found a task")
    ecs.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks")]
    raises(TaskUnavailable, lambda: adapter.lookup(KEY, TRAIN_HOST))


def lookup_describes_only_its_own_family() -> None:
    """The launch role may describe only tasks tagged with a family it may start (airflow_launch_jobs.json), so the stopped listing names the task
    definition family before any task is described: a stopped task of another family in the cluster is never described."""
    ecs, logs = FakeEcs(), FakeLogs()
    adapter = EcsAdapter(ecs, logs, replace(SETTINGS, host_capacity_provider=ecs.host_provider, families=frozenset({"train", "silver"})))
    for number in range(3):
        other = adapter.start("silver", KEY.replace(":train:1", f":silver:{number + 1}"), {"layer": {"RUN_ID": "x"}})
        ecs.finish(other)
    arn = adapter.start("train", KEY, ENVIRONMENT)
    ecs.finish(arn)
    ecs.calls.clear()
    found = adapter.lookup(KEY, TRAIN_HOST)
    require(found is not None and found.arn == arn, f"the stopped train task was not found: {found}")
    listings = [arguments for name, arguments in ecs.calls if name == "list_tasks" and arguments.get("desiredStatus") == "STOPPED"]
    require(bool(listings) and all(item.get("family") == TRAIN_HOST for item in listings), f"the stopped listing does not name the family: {listings}")
    described = [arn for name, arguments in ecs.calls if name == "describe_tasks" for arn in arguments["tasks"]]
    others = [item for item in described if "silver" in str(ecs.tasks[item]["request"]["taskDefinition"])]
    require(not others, f"a task of another family was described: {others}")


def unstopped_lists_running_and_stopping_tasks_of_the_family() -> None:
    """A task of the family is idle only when ECS shows its last status STOPPED: a running task and a stopping one (desired STOPPED, containers still
    running) both count, another family's task never does, and a describe reply that leaves a listed task out is no evidence (round 2 review, H1)."""
    ecs, logs = FakeEcs(), FakeLogs()
    adapter = EcsAdapter(ecs, logs, replace(SETTINGS, host_capacity_provider=ecs.host_provider, families=frozenset({"train", "silver"})))
    require(adapter.unstopped(TRAIN_HOST) == [], "an empty cluster has an unstopped task")
    ecs.hold = True
    running = adapter.start("train", KEY, ENVIRONMENT)
    other = adapter.start("silver", KEY.replace(":train:1", ":silver:1"), {"layer": {"RUN_ID": "x"}})
    require(adapter.unstopped(TRAIN_HOST) == [running], f"the running task of the family is not named, or another family's is: {adapter.unstopped(TRAIN_HOST)}")
    ecs.stop_lag = 5
    ecs.stop_task(cluster=CLUSTER, task=running, reason="stop")
    require(ecs.tasks[running]["desiredStatus"] == "STOPPED", "the stand-in task is not stopping")
    require(adapter.unstopped(TRAIN_HOST) == [running], "a stopping task of the family was taken as idle")
    ecs.describe_overrides = [{"tasks": [], "failures": []}]
    raises(TaskUnavailable, lambda: adapter.unstopped(TRAIN_HOST))
    while ecs.tasks[running]["lastStatus"] != "STOPPED":
        ecs.describe_tasks(cluster=CLUSTER, tasks=[running])
    require(adapter.unstopped(TRAIN_HOST) == [] and other in ecs.tasks, "a stopped task of the family still counts")
    ecs.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks")]
    raises(TaskUnavailable, lambda: adapter.unstopped(TRAIN_HOST))


def fake_refuses_a_wrong_shape() -> None:
    """The fake must refuse what ECS refuses, or the adapter cases prove nothing about placement."""
    ecs = FakeEcs()
    host = [{"capacityProvider": ecs.host_provider, "weight": 1}]
    fargate = [{"capacityProvider": "FARGATE", "weight": 1}]
    awsvpc = {"awsvpcConfiguration": {"subnets": ["subnet-0example"], "securityGroups": ["sg-0lake"], "assignPublicIp": "DISABLED"}}
    base = {"cluster": CLUSTER, "count": 1, "startedBy": "hai-example"}
    host_definition, fargate_definition = "example_dev_job_train_host", "example_dev_job_train"
    wrong: dict[str, tuple[dict[str, Any], str]] = {
        "bridge task with an awsvpc network": (
            {"taskDefinition": host_definition, "capacityProviderStrategy": host, "networkConfiguration": awsvpc},
            "network",
        ),
        "host task on a Fargate provider, which the cluster no longer registers": (
            {"taskDefinition": host_definition, "capacityProviderStrategy": fargate},
            "not found",
        ),
        "Fargate task on the host provider": (
            {"taskDefinition": fargate_definition, "capacityProviderStrategy": host, "networkConfiguration": awsvpc},
            "compatible",
        ),
        "Fargate task on a Fargate provider, which the cluster no longer registers": (
            {"taskDefinition": fargate_definition, "capacityProviderStrategy": fargate, "networkConfiguration": awsvpc},
            "not found",
        ),
        "unknown capacity provider": (
            {"taskDefinition": host_definition, "capacityProviderStrategy": [{"capacityProvider": "typo", "weight": 1}]},
            "not found",
        ),
    }
    for index, (label, (arguments, reason)) in enumerate(wrong.items()):
        try:
            ecs.run_task(**base, clientToken=f"wrong-{index}", **arguments)
        except ClientError as error:
            message = str(error.response["Error"]["Message"]).lower()
            require(error.response["Error"]["Code"] == "InvalidParameterException" and reason in message, f"{label}: refused for another cause: {message}")
        else:
            raise AssertionError(f"the fake accepted a {label}")
    require(not ecs.tasks, "a refused call created a task")
    accepted = ecs.run_task(**base, clientToken="right-0", taskDefinition=host_definition, capacityProviderStrategy=host)["tasks"]
    require(len(accepted) == 1 and len(ecs.tasks) == 1, "the fake refused a correct host request")
    by_default = ecs.run_task(**base, clientToken="right-1", taskDefinition=host_definition)["tasks"]
    require(len(by_default) == 1 and by_default[0]["capacityProviderName"] == ecs.host_provider, "the cluster default is not the host provider")


def no_fargate_mode() -> None:
    """The adapter has no Fargate placement: no name, no network settings and no placement or network argument, and its source never names one."""
    fields = set(EcsSettings.__dataclass_fields__)
    require(not fields & {"subnets", "networks"}, f"the settings still carry awsvpc network fields: {sorted(fields)}")
    require(not hasattr(ecs_adapter, "FARGATE"), "the adapter still names a Fargate placement")
    for method in ("start", "definition", "is_revision", "task_definition"):
        taken = set(inspect.signature(getattr(EcsAdapter, method)).parameters)
        require(not taken & {"placement", "network"}, f"{method} still takes a placement or a network: {sorted(taken)}")
    require("fargate" not in Path(ecs_adapter.__file__).read_text().lower(), "the adapter source still mentions Fargate")
    adapter, ecs, _ = build()
    arn = adapter.start("train", KEY, ENVIRONMENT)
    request = ecs.tasks[arn]["request"]
    require(
        set(item["capacityProvider"] for item in request["capacityProviderStrategy"]) == {ecs.host_provider},
        "a task was sent to a provider other than the host",
    )
    require(ecs.tasks[arn]["launchType"] == "EC2" and "attachments" not in ecs.tasks[arn], "the task is not an EC2 bridge task")


CASES: dict[str, Callable[[], None]] = {
    "start_twice_one_task": start_twice_one_task,
    "lost_reply_one_task": lost_reply_one_task,
    "another_key_another_task": another_key_another_task,
    "run_task_request": run_task_request,
    "family_not_allowed": family_not_allowed,
    "all_containers_zero_passes": all_containers_zero_passes,
    "layer_tasks_with_a_guard": layer_tasks_with_a_guard,
    "one_container_nonzero_fails": one_container_nonzero_fails,
    "early_failure_names_first_nonzero": early_failure_names_first_nonzero,
    "stopped_without_exit_codes_fails": stopped_without_exit_codes_fails,
    "undescribed_task_is_unavailable": undescribed_task_is_unavailable,
    "stop_is_repeatable": stop_is_repeatable,
    "throttled_start_is_transient": throttled_start_is_transient,
    "denied_start_is_permanent": denied_start_is_permanent,
    "throttled_poll_is_transient": throttled_poll_is_transient,
    "reasons_hide_identifiers": reasons_hide_identifiers,
    "tail_reads_the_container_stream": tail_reads_the_container_stream,
    "client_timeouts": client_timeouts,
    "host_family_map": host_family_map,
    "host_provider_required": host_provider_required,
    "host_start_twice_one_task": host_start_twice_one_task,
    "host_task_shape": host_task_shape,
    "host_capacity_exhausted_is_transient": host_capacity_exhausted_is_transient,
    "fake_refuses_a_wrong_shape": fake_refuses_a_wrong_shape,
    "no_fargate_mode": no_fargate_mode,
    "capacity_answer_is_definite": capacity_answer_is_definite,
    "guard_runs_just_before_the_send": guard_runs_just_before_the_send,
    "unknown_reply_is_told_from_an_unsent_call": unknown_reply_is_told_from_an_unsent_call,
    "lookup_finds_running_and_stopped_tasks": lookup_finds_running_and_stopped_tasks,
    "lookup_describes_only_its_own_family": lookup_describes_only_its_own_family,
    "unstopped_lists_running_and_stopping_tasks_of_the_family": unstopped_lists_running_and_stopping_tasks_of_the_family,
    "runtask_sends_once_through_the_sdk": runtask_sends_once_through_the_sdk,
    "retrying_runtask_client_refused": retrying_runtask_client_refused,
}
