"""Run every DAG that launches a host job in the pinned Airflow image, with Airflow's own test runner and the real Amazon provider operator.

Every other launch-path test runs over stand-ins of the Airflow SDK and of the Amazon provider (``airflow_mode_cases.py``, ``ecs_launch_operator_cases.py``),
so a class of fault cannot show there: how Airflow's own classes treat ours (the operator metaclass, task parameters, the task SDK's context and state
calls, the DAG form). This module runs in the image with no network. It starts a local SQLite metadata database and runs each DAG with ``DAG.test()``,
which runs every task through the task SDK runner in process (``run_task_in_process``) against Airflow's execution API. A task runs the production code:
``orchestration/dag_runtime.py``, ``orchestration/airflow_steps.py`` with the real ``services()`` (real boto3 clients, session, configuration and
environment), the launch library and ``BrokerlessEcsRunTaskOperator`` on the real ``EcsRunTaskOperator``.

Only the cloud is replaced. ``aws_http_bridge.AwsBridge`` serves the existing fakes (``Rig`` of ``airflow_mode_cases``: the fake ECS with the host task
definition revisions, the Logs, SSM and S3 objects of the staged inputs and the launch records) on loopback, and ``AWS_ENDPOINT_URL`` points every boto3
client at it, with fake credentials. Three more substitutions, each named in the report: the free space of the scratch volume (the host volume is not in the
container), a staged input that is not there yet is waited for one second instead of ten minutes, and the fake task ends at its first status read.

``main`` prints one line ``REAL_IMAGE_DAGS <json>`` with one entry per scenario. A scenario fails with the first expectation it breaks, and the others still
run, so one run lists every fault. ``scripts/orchestration/run_real_image_dags_e2e.py`` records the report.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, TypedDict
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
REPORT_PREFIX = "REAL_IMAGE_DAGS "
SUBSTITUTIONS = (
    "the cloud: the fake ECS, Logs, SSM and S3 of the airflow_mode_cases rig behind a loopback bridge (aws_http_bridge.py), "
    "reached with AWS_ENDPOINT_URL and fake credentials",
    "the bridge checks the SigV4 credential scope and the request rules of the bundled service models, not the signature, IAM policy or quotas",
    "the free space of the scratch volume: shutil.disk_usage answers 400 GiB, because the host's scratch volume is not in the container",
    "airflow_steps.UNAVAILABLE_SECONDS is 1 second: a staged input that is not there yet is waited for one second, not ten minutes",
    "the fake task ends at its first status read (finish_after 1) and its exit codes are planned by the scenario",
    "USER is set when the environment has none: the check runs python without the image entrypoint that adds the passwd entry for the user id",
    "the Airflow metadata database is SQLite in a temporary folder and DAG.test() runs the tasks in process (no scheduler, no API server)",
)
FAKE_CREDENTIAL = "stand-in"
LOG_GROUP = "example_jobs_dev"
STAGE_PREFIXES = ("lakehouse/run_stage/", "drift/")  # the staged inputs the launch role reads (the run stage, the drift records)
SCRATCH_FREE = 400 * 2**30
DONE_STATE = "success"
BAG: dict[str, Any] = {}


@dataclass(frozen=True)
class Plant:
    """A fault planted in the launch code's own boto3 calls: ``rewrite`` changes the arguments of every call of ``operation`` for which ``when`` is true,
    above the SDK's serialization and the HTTP transport, which stay real. A scenario with a plant passes only when the cloud (the bridge) refuses the call
    as AWS would and the DAG run fails closed."""

    operation: str
    rewrite: Callable[[dict[str, Any]], dict[str, Any]]
    when: Callable[[dict[str, Any]], bool] = lambda _params: True


@contextlib.contextmanager
def planted(plant: Plant | None) -> Iterator[None]:
    """Apply ``plant`` to every boto3 client call in the block."""
    if plant is None:
        yield
        return
    from botocore.client import BaseClient

    original = vars(BaseClient)["_make_api_call"]

    def make_api_call(self: Any, operation_name: str, api_params: dict[str, Any]) -> Any:
        if operation_name == plant.operation and plant.when(api_params):
            api_params = plant.rewrite(dict(api_params))
        return original(self, operation_name, api_params)

    with mock.patch.object(BaseClient, "_make_api_call", make_api_call):
        yield


@dataclass(frozen=True)
class Step:
    """One DAG run of a scenario: the DAG, its trigger values and what the run must do.

    ``states`` maps task ids to the states they must end in (``None`` means every task of the DAG ends in ``success``), ``starts`` is the number of RunTask
    calls the run sends, and ``family`` the host task definition family they name. ``reason`` is text each failing task's error must contain, ``skip`` the
    tasks that need the Airflow API server or other DAGs (they are marked as passed and not run), and ``plan`` the exit code per container of the task."""

    dag_id: str
    params: Mapping[str, Any]
    starts: int
    family: str | None = None
    states: Mapping[str, str] | None = None
    run_state: str = "success"
    reason: str | None = None
    skip: str | None = None
    plan: Mapping[str, int | None] | None = None
    end: str | None = None
    environment: Mapping[str, str] = field(default_factory=dict)
    prepare: Callable[[Any], None] | None = None
    plant: Plant | None = None
    check: Callable[[Any, dict[str, Any]], None] | None = None


class RefusedRun(TypedDict):
    """The end states of a run the DAG refuses: the run state and the state of each task."""

    run_state: str
    states: dict[str, str]


@dataclass(frozen=True)
class Scenario:
    """Steps that share one rig, so a later DAG finds the records of an earlier one. ``rig`` builds the rig of the scenario."""

    name: str
    steps: tuple[Step, ...]
    rig: Callable[[], Any]
    after: Callable[[Any], None] | None = None


class Expectation(AssertionError):
    """A scenario broke an expectation."""


def require(condition: object, message: str) -> None:
    if not condition:
        raise Expectation(message)


def configure_airflow(folder: str) -> None:
    """The Airflow settings of the run, set before Airflow is imported."""
    os.environ.update(
        {
            "AIRFLOW_HOME": folder,
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{folder}/airflow.db",
            "AIRFLOW__CORE__DAGS_FOLDER": str(ROOT / "dags"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
            "AIRFLOW__CORE__UNIT_TEST_MODE": "false",
            "AIRFLOW__OPENLINEAGE__DISABLED": "true",
        }
    )
    os.environ.setdefault(
        "USER", "airflow"
    )  # the check runs python without the image entrypoint, which adds the passwd entry that Airflow needs for any other user id


class Observer:
    """Records the error each launch step raises (the task SDK keeps only the task's state) and lets it go on."""

    NAMES = ("launch_step", "preflight_step", "job_step", "evidence_step")

    def __init__(self) -> None:
        self.errors: list[str] = []

    @contextlib.contextmanager
    def watching(self) -> Iterator[None]:
        import importlib

        steps = importlib.import_module("orchestration.airflow_steps")
        patches = [mock.patch.object(steps, name, self.watch(name, getattr(steps, name))) for name in self.NAMES]
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            yield

    def watch(self, name: str, function: Callable[..., Any]) -> Callable[..., Any]:
        def watched(*arguments: Any, **keywords: Any) -> Any:
            try:
                return function(*arguments, **keywords)
            except Exception as error:
                self.errors.append(f"{name}: {type(error).__name__}: {error}")
                raise

        return watched


def rig_modules() -> SimpleNamespace:
    """The rig of the offline cases: its fakes, constants and staged inputs. ``orchestration`` may already be imported from a folder that holds only a few of
    its files (the in-image check mounts those), so the full package folder of this checkout is added to its path."""
    sys.path.insert(0, str(ROOT))
    import orchestration

    if str(ROOT / "orchestration") not in orchestration.__path__:
        orchestration.__path__.append(str(ROOT / "orchestration"))
    import scripts.orchestration.airflow_mode_cases as cases
    from scripts.orchestration import ecs_fake

    return SimpleNamespace(cases=cases, fake=ecs_fake)


def make_rig(*, capacity: str | None = None, hold: str | Exception | None = None) -> Any:
    """A rig with the fake ECS finishing its task at the first status read, the real clock for task times, and the SSM answers of a services stack that
    is not applying (the hold clear), unless ``hold`` says otherwise."""
    cases = rig_modules().cases
    rig = cases.Rig(capacity=cases.CAPACITY if capacity is None else capacity)
    rig.ecs.clock = time.time
    rig.ecs.finish_after = 1
    answer: list[str | Exception] = [cases.CLEAR if hold is None else hold]
    rig.ssm = cases.LaunchSsm({cases.HOLD_PARAMETER: answer, cases.CAP_PARAMETER: [str(cases.JOB_MEMORY)], cases.LOCK_PARAMETER: cases.no_lock()})
    return rig


def launch_environment(rig: Any, endpoint: str, scratch: str) -> dict[str, str]:
    """The environment of the Airflow service, as the services stack renders it, with the endpoint of the bridge and fake credentials."""
    cases, fake = rig_modules().cases, rig_modules().fake
    return {
        "AWS_REGION": fake.REGION,
        "AWS_DEFAULT_REGION": fake.REGION,
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": FAKE_CREDENTIAL,
        "AWS_SECRET_ACCESS_KEY": FAKE_CREDENTIAL,
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_EC2_METADATA_DISABLED": "true",
        "HAI_LAUNCH_CLUSTER": fake.CLUSTER,
        "HAI_LAUNCH_FAMILY_PREFIX": "example_dev_job_",
        "HAI_LAUNCH_PROJECT_TAG": "example",
        "HAI_LAUNCH_LOG_GROUP": LOG_GROUP,
        "HAI_LAUNCH_HOST_PROVIDER": "stand-in_host",
        "HAI_LAUNCH_BUCKET": cases.BUCKET,
        "HAI_LAUNCH_POLARIS_URI": cases.POLARIS,
        "HAI_LAUNCH_MLFLOW_URI": cases.MLFLOW,
        "HAI_LAUNCH_SCRATCH_PATH": scratch,
        "HAI_LAUNCH_HOST_CAPACITY": rig.preflight.host_capacity,
        "HAI_LAUNCH_HOST_CAP_PARAMETER": cases.CAP_PARAMETER,
        "HAI_LAUNCH_HOST_HOLD_PARAMETER": cases.HOLD_PARAMETER,
        "HAI_LAUNCH_APPLY_LOCK_PARAMETER": cases.LOCK_PARAMETER,
    }


def launch_records(rig: Any, run: str, job: str) -> dict[str, dict[str, Any]]:
    """The launch records of a run and job in the fake record bucket, by event."""
    prefix = rig.launcher.store.prefix if hasattr(rig.launcher.store, "prefix") else rig_modules().cases.PREFIX
    return {
        key.rsplit("/", 1)[-1].removesuffix(".json"): json.loads(body)
        for key, body in rig.records.objects.items()
        if key.startswith(f"{prefix}/") and f"/{run}/" in key and f"/{job}/" in key
    }


def states_of(run: Any) -> dict[str, str]:
    """The state each task instance of the DAG run ended in."""
    return {item.task_id: str(getattr(item.state, "value", item.state)) for item in run.get_task_instances()}


def make_bridge(rig: Any) -> Any:
    """The loopback cloud of a rig: its fakes behind the bridge."""
    from scripts.orchestration.aws_http_bridge import Bridge

    modules = rig_modules()
    return Bridge(
        {"ecs": rig.ecs, "ssm": rig.ssm, "logs": rig.logs},
        rig.stage,
        rig.records,
        f"{modules.cases.PREFIX}/",
        bucket=modules.cases.BUCKET,
        region=modules.fake.REGION,
        access_key=FAKE_CREDENTIAL,
        log_group=LOG_GROUP,
        stage_prefixes=STAGE_PREFIXES,
    )


def run_step(bag: Any, rig: Any, step: Step, scratch: str) -> dict[str, Any]:
    """Run one DAG of a scenario in the image and check it against its step. Returns the facts of the run."""
    from orchestration import airflow_steps

    if step.prepare is not None:
        step.prepare(rig)
    rig.ecs.plan = dict(step.plan or {})
    started = len([name for name, _ in rig.ecs.calls if name == "run_task"])
    sent = len(rig.ecs.sent)
    observer = Observer()
    dag = bag.get_dag(step.dag_id)
    require(dag is not None, f"{step.dag_id} is not in the DAG folder")
    with make_bridge(rig) as bridge:
        environment = launch_environment(rig, bridge.endpoint, scratch) | dict(step.environment)
        with (
            mock.patch.dict(os.environ, environment),
            mock.patch.object(airflow_steps, "UNAVAILABLE_SECONDS", 1.0),
            mock.patch.object(shutil, "disk_usage", lambda _path: SimpleNamespace(total=SCRATCH_FREE, used=0, free=SCRATCH_FREE)),
            observer.watching(),
            planted(step.plant),
        ):
            run = dag.test(run_conf=dict(step.params), mark_success_pattern=step.skip)
        states = states_of(run)
        run_state = str(getattr(run.state, "value", run.state))
        requests = list(bridge.requests)
    calls = [arguments for name, arguments in rig.ecs.calls if name == "run_task"][started:]
    facts: dict[str, Any] = {
        "dag_id": step.dag_id,
        "run_state": run_state,
        "tasks": states,
        "run_task_calls": len(calls),
        "task_definitions": rig.ecs.sent[sent:],
        "errors": observer.errors,
        "bridge_requests": len(requests),
    }
    check_run(rig, step, facts, calls)
    return facts


def check_run(rig: Any, step: Step, facts: dict[str, Any], calls: list[dict[str, Any]]) -> None:
    """Break an expectation when the run is not the step's."""
    where = f"{step.dag_id} {dict(step.params).get('run_id', '')}"
    wanted = dict(step.states) if step.states is not None else {task: DONE_STATE for task in facts["tasks"]}
    require(facts["tasks"], f"{where}: no task ran: {facts}")
    require(facts["run_state"] == step.run_state, f"{where}: the DAG run ended {facts['run_state']}, not {step.run_state}: {facts['tasks']} {facts['errors']}")
    require(facts["tasks"] == wanted, f"{where}: task states {facts['tasks']}, expected {wanted}: {facts['errors']}")
    require(facts["run_task_calls"] == step.starts, f"{where}: {facts['run_task_calls']} RunTask calls, expected {step.starts}: {facts['errors']}")
    if step.reason is not None:
        text = " | ".join(facts["errors"])
        require(step.reason in text, f"{where}: no failing task named {step.reason!r}: {text or 'no error was raised'}")
    if step.check is not None:
        step.check(rig, facts)
        return
    if step.starts == 0:
        require(step.run_state == "success" or not rig.ecs.count("describe_tasks"), f"{where}: a refused run read task state")
        return
    cases = rig_modules().cases
    family = step.family
    require(family is not None, f"{where}: the step names no family")
    require(
        all(sent == rig.bound[str(family)] for sent in facts["task_definitions"]),
        f"{where}: RunTask did not name the revision the preflight checked ({rig.bound[str(family)]}): {facts['task_definitions']}",
    )
    first = calls[0]
    require(first["cluster"] == rig_modules().fake.CLUSTER and first["count"] == 1, f"{where}: the RunTask call is not one task on the cluster: {first}")
    require(
        [item["capacityProvider"] for item in first["capacityProviderStrategy"]] == ["stand-in_host"] and "networkConfiguration" not in first,
        f"{where}: the RunTask call is not a host (bridge) launch: {first}",
    )
    names = {item["name"] for item in first["overrides"]["containerOverrides"]}
    planned = set(rig_modules().cases.host_planned_containers()[str(family).removeprefix("example_dev_job_").removesuffix("_host")]["containers"])
    require(bool(names) and names <= planned - {"init"}, f"{where}: overrides for containers {names} that the host definition {planned} does not run")
    run_id = str(dict(step.params)["run_id"])
    environment = {pair["name"]: pair["value"] for item in first["overrides"]["containerOverrides"] for pair in item["environment"]}
    require(environment.get("RUN_ID", run_id) == run_id or run_id in environment.values(), f"{where}: no override carries the run id: {environment}")
    job = str(family).removeprefix("example_dev_job_").removesuffix("_host")
    records = launch_records(rig, run_id, job)
    require({"claim", "intent", "started", "end"} <= set(records), f"{where}: the launch records are {sorted(records)}")
    outcome = step.end or "succeeded"
    require(records["end"]["outcome"] == outcome, f"{where}: the end record says {records['end']['outcome']}, not {outcome}: {records['end']}")
    arns = [task for task in rig.ecs.tasks]
    require(
        len(arns) >= 1 and records["started"].get("task_arn", records["end"].get("task_arn")) in (*arns, None),
        f"{where}: the record names no task of the cluster",
    )
    del cases


def execute(scenario: Scenario, bag: Any, scratch: str) -> dict[str, Any]:
    """Run a scenario's steps in order over one rig. Every step runs even when an earlier one broke an expectation, and the first break is the verdict."""
    started = time.monotonic()
    rig = scenario.rig()
    steps: list[dict[str, Any]] = []
    broken: str | None = None
    for step in scenario.steps:
        try:
            steps.append(run_step(bag, rig, step, scratch))
        except Exception as error:
            detail = f"{type(error).__name__}: {error}" if isinstance(error, Expectation) else "".join(traceback.format_exception(error))[-1500:]
            steps.append({"dag_id": step.dag_id, "error": detail[-1500:]})
            broken = broken or detail
    if scenario.after is not None and broken is None:
        try:
            scenario.after(rig)
        except Exception as error:
            broken = f"{type(error).__name__}: {error}"
    return {"name": scenario.name, "ok": broken is None, "detail": broken, "seconds": round(time.monotonic() - started, 1), "steps": steps}


def form_check(_rig: Any) -> None:
    """The trigger form of every shipped DAG, validated by Airflow's own ``Param`` code as the trigger does (the DAG's params updated with the trigger's
    values, then validated). Each DAG accepts a trigger with only its required values and fills the defaults. It refuses a missing run ID, a run ID that
    breaks its pattern, ``latest`` where a producer run is required and a value of another JSON type."""
    import copy

    c = rig_modules().cases
    run, other = c.SILVER_RUN, c.GOLD_RUN
    required: dict[str, dict[str, Any]] = {
        "ingestion_dag": {"run_id": c.PUBLISHER_RUN},
        "silver_transform_dag": {"run_id": run},
        "gold_transform_dag": {"run_id": other, "silver_snapshot_run": run},
        "features_base_dag": {"run_id": other, "gold_snapshot_run": run},
        **{f"features_{model}_dag": {"run_id": other, "base_snapshot_run": run} for model in ("m1", "m2", "m3", "m4", "m5", "m6")},
        "drift_monitoring_dag": {"run_id": run, "model": "m1"},
        "training_dag": {"run_id": run, "model": "m1"},
        "model_promotion_dag": {"run_id": run, "hai_type": "HAI_1", "model_version": "3"},
        "serving_deploy_dag": {"run_id": run},
        "metadata_ingest_dag": {"run_id": run},
        "bronze_chain_dag": {"run_id": run},
    }
    wrong: tuple[dict[str, Any], ...] = ({}, {"run_id": "latest"}, {"run_id": "x" + run}, {"run_id": 20261104}, {"run_id": run, "mode": "mock"})

    def validated(dag_id: str, conf: Mapping[str, Any]) -> dict[str, Any]:
        dag = BAG["bag"].get_dag(dag_id)
        params = copy.deepcopy(dag.params)
        params.update(dict(conf))
        return dict(params.validate())

    def refused(dag_id: str, conf: Mapping[str, Any]) -> bool:
        try:
            validated(dag_id, conf)
        except Exception as error:
            return bool(str(error))
        return False

    problems = []
    for dag_id, conf in required.items():
        try:
            filled = validated(dag_id, conf)
            if "mode" in filled and filled["mode"] not in ("real", "promote"):
                problems.append(f"{dag_id}: the default mode is {filled['mode']!r}")
        except Exception as error:
            problems.append(f"{dag_id} refused its required values {conf}: {type(error).__name__}: {str(error)[:200]}")
        problems.extend(f"{dag_id} accepted {bad}" for bad in wrong if not refused(dag_id, {**conf, **bad} if bad else {}))
    for dag_id, name in (
        ("gold_transform_dag", "silver_snapshot_run"),
        ("features_base_dag", "gold_snapshot_run"),
        *((f"features_{model}_dag", "base_snapshot_run") for model in ("m1", "m2", "m3", "m4", "m5", "m6")),
    ):
        if not refused(dag_id, {**required[dag_id], name: "latest"}):
            problems.append(f"{dag_id} accepted latest for {name}")
    missing = sorted(set(BAG["bag"].dag_ids) - set(required))
    require(not missing, f"DAGs without a form case: {missing}")
    require(not problems, "; ".join(problems))


def aws_error(call: Callable[[], Any]) -> str:
    """The error code AWS would answer for ``call``: ``accepted`` when the call went through, the botocore error class when the SDK or the transport
    failed instead of the service answering."""
    from botocore.exceptions import ClientError

    try:
        call()
    except ClientError as error:
        return str(error.response.get("Error", {}).get("Code", ""))
    except Exception as error:
        return type(error).__name__
    return "accepted"


def bridge_conformance(rig: Any) -> None:
    """The bridge answers real botocore clients as AWS answers requests the services refuse: what the launch code sends is valid and passes (the DAG
    scenarios), and each request below, which the launch code never sends, is refused with its error code. The cases are the rules of the ECS, SSM, Logs
    and S3 services for the fields the launch library uses (the bundled service models state them) and the bridge's own scope: one bucket, the staged
    prefixes, the operations the launch library calls, the region of its credentials."""
    import boto3
    from botocore.config import Config

    modules = rig_modules()
    c, fake = modules.cases, modules.fake
    group, bucket, other = LOG_GROUP, c.BUCKET, "nonexistent-review-bucket"
    with make_bridge(rig) as bridge:

        def client(service: Literal["ecs", "ssm", "logs", "s3"], region: str = fake.REGION) -> Any:
            return boto3.session.Session(region_name=region, aws_access_key_id=FAKE_CREDENTIAL, aws_secret_access_key=FAKE_CREDENTIAL).client(
                service, endpoint_url=bridge.endpoint, config=Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=5, read_timeout=10)
            )

        ecs, ssm, logs, s3 = client("ecs"), client("ssm"), client("logs"), client("s3")
        base = {
            "cluster": fake.CLUSTER,
            "taskDefinition": "example_dev_job_publisher_host",
            "count": 1,
            "clientToken": "x" * 40,
            "startedBy": "conformance",
            "capacityProviderStrategy": [{"capacityProvider": "stand-in_host", "weight": 1}],
        }
        arn = f"arn:aws:ecs:{fake.REGION}:{fake.ACCOUNT}:task/{fake.CLUSTER}/" + "0" * 32
        refused: dict[str, tuple[Callable[[], Any], tuple[str, ...]]] = {
            "RunTask with a launch type and a capacity provider strategy": (lambda: ecs.run_task(**base, launchType="EC2"), ("InvalidParameterException",)),
            "RunTask with a startedBy of unlisted characters": (lambda: ecs.run_task(**{**base, "startedBy": "has space"}), ("InvalidParameterException",)),
            "RunTask with overrides over 8192 characters": (
                lambda: ecs.run_task(**base, overrides={"containerOverrides": [{"name": "run", "environment": [{"name": "A", "value": "v" * 8200}]}]}),
                ("InvalidParameterException",),
            ),
            "RunTask with a repeated container override": (
                lambda: ecs.run_task(**base, overrides={"containerOverrides": [{"name": "run"}, {"name": "run"}]}),
                ("InvalidParameterException",),
            ),
            "RunTask with more than 50 tags": (
                lambda: ecs.run_task(**base, tags=[{"key": f"k{index}", "value": "v"} for index in range(51)]),
                ("InvalidParameterException",),
            ),
            "RunTask with a task definition that is not registered": (
                lambda: ecs.run_task(**{**base, "taskDefinition": "example_dev_job_unknown_host"}),
                ("ClientException",),
            ),
            "RunTask with a malformed task definition": (lambda: ecs.run_task(**{**base, "taskDefinition": "no good"}), ("InvalidParameterException",)),
            "DescribeTasks of more than 100 tasks": (lambda: ecs.describe_tasks(cluster=fake.CLUSTER, tasks=[arn] * 101), ("InvalidParameterException",)),
            "DescribeTasks of a malformed task": (lambda: ecs.describe_tasks(cluster=fake.CLUSTER, tasks=["not a task"]), ("InvalidParameterException",)),
            "DescribeTasks with an unknown include": (
                lambda: ecs.describe_tasks(cluster=fake.CLUSTER, tasks=[arn], include=["EVERYTHING"]),
                ("InvalidParameterException",),
            ),
            "ListTasks with an unknown desired status": (lambda: ecs.list_tasks(cluster=fake.CLUSTER, desiredStatus="DONE"), ("InvalidParameterException",)),
            "StopTask with a reason over 255 characters": (
                lambda: ecs.stop_task(cluster=fake.CLUSTER, task=arn, reason="r" * 256),
                ("InvalidParameterException",),
            ),
            "DescribeContainerInstances of a malformed instance": (
                lambda: ecs.describe_container_instances(cluster=fake.CLUSTER, containerInstances=["not an instance"]),
                ("InvalidParameterException",),
            ),
            "ListContainerInstances of another cluster": (lambda: ecs.list_container_instances(cluster="another-cluster"), ("ClusterNotFoundException",)),
            "an ECS operation the launch library never calls": (lambda: ecs.delete_cluster(cluster=fake.CLUSTER), ("AccessDeniedException",)),
            "GetParameter of a name with characters SSM refuses": (lambda: ssm.get_parameter(Name="/not a name!"), ("ValidationException",)),
            "GetParameter of a name the launch role may not read": (lambda: ssm.get_parameter(Name="/other/parameter"), ("AccessDeniedException",)),
            "an SSM operation the launch library never calls": (lambda: ssm.put_parameter(Name="/x/y", Value="v", Type="String"), ("AccessDeniedException",)),
            "GetLogEvents of another log group": (
                lambda: logs.get_log_events(logGroupName="another_group", logStreamName="stream", limit=10),
                ("ResourceNotFoundException",),
            ),
            "GetLogEvents of a stream name with a colon": (
                lambda: logs.get_log_events(logGroupName=group, logStreamName="a:b", limit=10),
                ("InvalidParameterException",),
            ),
            "GetObject from another bucket": (
                lambda: s3.get_object(Bucket=other, Key=c.RUN_KEY.format(run=c.SILVER_RUN, name="code.sha256")),
                ("NoSuchBucket",),
            ),
            "GetObject of a key outside the staged prefixes": (lambda: s3.get_object(Bucket=bucket, Key="private/secret.json"), ("AccessDenied",)),
            "PutObject to another bucket": (lambda: s3.put_object(Bucket=other, Key=f"{c.PREFIX}/x.json", Body=b"{}", IfNoneMatch="*"), ("NoSuchBucket",)),
            "PutObject of a key outside the launch prefix": (
                lambda: s3.put_object(Bucket=bucket, Key="private/x.json", Body=b"{}", IfNoneMatch="*"),
                ("AccessDenied",),
            ),
            "ListObjectsV2 of another bucket": (lambda: s3.list_objects_v2(Bucket=other, Prefix=f"{c.PREFIX}/families/"), ("NoSuchBucket",)),
            "GetObject of a key over 1024 bytes": (lambda: s3.get_object(Bucket=bucket, Key="lakehouse/run_stage/" + "k" * 1100), ("KeyTooLongError",)),
            "an ECS request signed for another region": (
                lambda: client("ecs", "other-region").list_container_instances(cluster=fake.CLUSTER),
                ("InvalidSignatureException",),
            ),
            "an S3 request signed for another region": (
                lambda: client("s3", "other-region").get_object(Bucket=bucket, Key=c.RUN_KEY.format(run=c.SILVER_RUN, name="code.sha256")),
                ("AuthorizationHeaderMalformed",),
            ),
        }
        problems = []
        for name, (call, codes) in refused.items():
            answer = aws_error(call)
            if answer not in codes:
                problems.append(f"{name}: answered {answer}, expected {codes[0]}")
        accepted = {
            "RunTask by capacity provider strategy alone": lambda: ecs.run_task(**base),
            "GetObject of a staged input": lambda: s3.get_object(Bucket=bucket, Key=c.RUN_KEY.format(run=c.SILVER_RUN, name="code.sha256")),
            "GetParameter of the launch hold": lambda: ssm.get_parameter(Name=c.HOLD_PARAMETER),
        }
        for name, call in accepted.items():
            answer = aws_error(call)
            if answer != "accepted":
                problems.append(f"{name}: answered {answer}, expected acceptance")
    require(not problems, "; ".join(problems))


def cases_catalog() -> dict[str, Scenario]:
    """Every scenario by name."""
    import importlib

    modules = rig_modules()
    c = modules.cases
    preflight = importlib.import_module("orchestration.ecs_preflight")
    training = importlib.import_module("orchestration.training_contract")
    derived = preflight.derived_run_id
    silver, gold = derived(c.BRONZE_RUN, "silver"), None
    gold = derived(silver, "gold")
    features = derived(gold, "features")
    drift = derived(features, "drift")
    train = training.training_run_id(drift)
    marker = json.dumps({"run_id": drift, "files": {}}).encode()
    approval = {
        "run_id": c.PROMOTE_RUN,
        "dag_id": "model_promotion_dag",
        "scope": "promote",
        "approved_on": "2026-10-30",
        "hai_type": "HAI_1",
        "model_version": "3",
    }
    chain = ("preflight", "job", "evidence", "gate")

    def job_states(*, failed: tuple[str, ...] = ()) -> dict[str, str]:
        return {name: "failed" if name in failed else DONE_STATE for name in chain}

    def stage_drift(rig: Any) -> None:
        rig.stage.objects[f"drift/{drift}/_complete.json"] = marker

    def stage_promotion(rig: Any) -> None:
        rig.stage.stage(c.PROMOTE_RUN, "promotion_approval.json", approval)
        rig.stage.stage(c.SERVE_RUN, "serving_request.json", c.SERVING_REQUEST)

    def stage_publisher(rig: Any, run: str = c.PUBLISHER_RUN) -> None:
        rig.stage.stage(run, "publisher_request.json", c.publisher_body(c.PUBLISHER_REQUEST))

    def stage_other(rig: Any) -> None:
        rig.stage_runs(c.OTHER_RUN)

    def family(job: str) -> str:
        return f"example_dev_job_{job}_host"

    pipeline = Scenario(
        "every_host_dag_runs_end_to_end",
        (
            Step("bronze_chain_dag", {"run_id": c.BRONZE_RUN, "mode": "real"}, 0, skip=r"run_.*|start_metadata"),
            Step("silver_transform_dag", {"run_id": silver, "mode": "real"}, 1, family("silver"), job_states()),
            Step("gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}, 1, family("gold"), job_states()),
            Step("features_base_dag", {"run_id": features, "mode": "real", "gold_snapshot_run": gold}, 1, family("features")),
            *(
                Step(
                    f"features_{model}_dag",
                    {"run_id": training.model_features_run_id(features, model), "mode": "real", "base_snapshot_run": features},
                    1,
                    family("features"),
                )
                for model in ("m1", "m2", "m3", "m4", "m5", "m6")
            ),
            Step(
                "drift_monitoring_dag",
                {"run_id": drift, "mode": "real", "mlflow": "aws", "model": "m1"},
                1,
                family("drift"),
                {"drift_run": DONE_STATE, "decide": DONE_STATE, "trigger_training": "skipped"},  # a quiet run maps no trigger
                prepare=stage_drift,
            ),
            Step("training_dag", {**c.TRAIN_PARAMS, "run_id": train}, 1, family("train")),
            Step("model_promotion_dag", dict(c.PROMOTE_PARAMS), 1, family("promote"), job_states(), prepare=stage_promotion),
            Step("serving_deploy_dag", {"run_id": c.SERVE_RUN, "mode": "real"}, 1, family("serve"), job_states()),
            Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 1, family("publisher"), job_states(), prepare=stage_publisher),
            Step("metadata_ingest_dag", {"run_id": c.OTHER_RUN, "mode": "real"}, 1, family("metadata"), prepare=stage_other),
            Step("gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}, 0, family("gold"), job_states()),  # joins the launch
            Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 0, family("publisher"), job_states()),
        ),
        make_rig,
    )

    def ingestion(name: str, rig: Callable[[], Any], **changes: Any) -> Scenario:
        step = Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 0, family("publisher"), prepare=stage_publisher, **changes)
        return Scenario(name, (step,), rig, after=no_launch)

    def no_launch(rig: Any) -> None:
        require(not rig.ecs.tasks and not rig.records.objects, f"a refused run started a task or wrote a launch record: {sorted(rig.records.objects)}")

    refused: RefusedRun = {"run_state": "failed", "states": {"preflight": "failed", "job": "upstream_failed", "evidence": "upstream_failed", "gate": "failed"}}

    def other_capacity() -> Any:
        return make_rig(capacity="usable_memory_mib=65536 system_reserve_mib=2048")

    def unbound() -> Any:
        rig = make_rig()
        rig.ecs.register("example_dev_job_publisher_host", 25600, None)
        return rig

    def no_digest() -> Any:
        rig = make_rig()
        del rig.stage.objects[c.RUN_KEY.format(run=c.PUBLISHER_RUN, name="code.sha256")]
        return rig

    refusals = (
        ingestion("hold_set_refuses_before_any_launch", lambda: make_rig(hold=c.HELD), reason="launch hold", **refused),
        ingestion(
            "hold_parameter_missing_refuses_before_any_launch",
            lambda: make_rig(hold=rig_modules().fake.client_error("ParameterNotFound", "GetParameter")),
            reason="launch hold",
            **refused,
        ),
        ingestion("capacity_mismatch_refuses_before_any_launch", other_capacity, reason="bound to", **refused),
        ingestion("unbound_revision_refuses_before_any_launch", unbound, reason="carries no host capacity binding", **refused),
        ingestion("missing_code_binding_refuses_before_any_launch", no_digest, reason="code_digest_not_staged", **refused),
        Scenario(
            "missing_request_refuses_before_any_launch",
            (Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 0, family("publisher"), reason="publisher_request", **refused),),
            make_rig,
            after=no_launch,
        ),
        Scenario(
            "a_task_that_exits_nonzero_fails_the_run",
            (
                Step(
                    "ingestion_dag",
                    {"run_id": c.PUBLISHER_RUN, "mode": "real"},
                    1,
                    family("publisher"),
                    {"preflight": DONE_STATE, "job": "failed", "evidence": "upstream_failed", "gate": "failed"},
                    run_state="failed",
                    reason="ended failed",
                    end="failed",
                    plan={"run": 1},
                    prepare=stage_publisher,
                ),
            ),
            make_rig,
        ),
    )

    def no_task(rig: Any, facts: dict[str, Any]) -> None:
        require(not rig.ecs.tasks, f"a task started although the cloud must refuse the call: {sorted(rig.ecs.tasks)}")
        end = launch_records(rig, c.PUBLISHER_RUN, "publisher").get("end")
        require(end is None or end["outcome"] != "succeeded", f"the launch record says succeeded: {end}")

    def no_record(rig: Any, facts: dict[str, Any]) -> None:
        require(not rig.ecs.tasks and not rig.records.objects, f"a refused read left a task or a record: {sorted(rig.records.objects)}")

    def planted_run(name: str, plant: Plant, states: dict[str, str], starts: int, check: Callable[[Any, dict[str, Any]], None], reason: str) -> Scenario:
        step = Step(
            "ingestion_dag",
            {"run_id": c.PUBLISHER_RUN, "mode": "real"},
            starts,
            family("publisher"),
            states,
            run_state="failed",
            prepare=stage_publisher,
            plant=plant,
            check=check,
            reason=reason,
        )
        return Scenario(name, (step,), make_rig)

    REFUSED_BY_ECS = "launch_refused:InvalidParameterException"
    sent_refused = {"preflight": DONE_STATE, "job": "failed", "evidence": "upstream_failed", "gate": "failed"}

    def too_large(params: dict[str, Any]) -> dict[str, Any]:
        containers = [dict(item) for item in params["overrides"]["containerOverrides"]]
        containers[0]["environment"] = [*containers[0]["environment"], {"name": "PLANTED", "value": "v" * 9000}]
        return {**params, "overrides": {"containerOverrides": containers}}

    def digest_key(params: dict[str, Any]) -> bool:
        return str(params.get("Key", "")).endswith("code.sha256")

    planted_cases = (
        planted_run(
            "planted_launch_type_with_a_capacity_provider_strategy_fails_closed",
            Plant("RunTask", lambda p: {**p, "launchType": "EC2"}),
            sent_refused,
            1,
            no_task,
            REFUSED_BY_ECS,
        ),
        planted_run("planted_overrides_over_the_ecs_limit_fail_closed", Plant("RunTask", too_large), sent_refused, 1, no_task, REFUSED_BY_ECS),
        planted_run(
            "planted_started_by_with_unlisted_characters_fails_closed",
            Plant("RunTask", lambda p: {**p, "startedBy": p["startedBy"] + " x"}),
            sent_refused,
            1,
            no_task,
            REFUSED_BY_ECS,
        ),
        planted_run(
            "planted_wrong_bucket_for_the_code_digest_fails_closed",
            Plant("GetObject", lambda p: {**p, "Bucket": "nonexistent-review-bucket"}, digest_key),
            refused["states"],
            0,
            no_record,
            "code_digest_unreadable",
        ),
        planted_run(
            "planted_key_outside_the_staged_prefixes_fails_closed",
            Plant("GetObject", lambda p: {**p, "Key": "private/" + p["Key"].rsplit("/", 1)[-1]}, digest_key),
            refused["states"],
            0,
            no_record,
            "code_digest_unreadable",
        ),
        planted_run(
            "planted_wrong_bucket_for_the_launch_records_fails_closed",
            Plant("PutObject", lambda p: {**p, "Bucket": "nonexistent-review-bucket"}),
            sent_refused | {"job": "failed"},
            0,
            no_task,
            "launch record store refused the call: NoSuchBucket",
        ),
    )
    form = Scenario("trigger_forms_validate_in_airflow", (), make_rig, after=form_check)
    conformance = Scenario("bridge_refuses_what_aws_refuses", (), make_rig, after=bridge_conformance)
    return {item.name: item for item in (pipeline, *refusals, *planted_cases, form, conformance)}


def versions() -> dict[str, str]:
    from importlib import metadata

    names = ("apache-airflow", "apache-airflow-providers-amazon", "apache-airflow-task-sdk", "boto3", "botocore")
    return {name: metadata.version(name) for name in names}


def run_all(only: str | None) -> dict[str, Any]:
    """Run the scenarios and return the report."""
    folder = tempfile.mkdtemp(prefix="real_image_dags_")
    scratch = tempfile.mkdtemp(prefix="scratch_", dir=folder)
    configure_airflow(folder)
    started = time.monotonic()
    from airflow.dag_processing.dagbag import DagBag
    from airflow.utils import db

    db.initdb()
    bag = DagBag(dag_folder=str(ROOT / "dags"))
    BAG["bag"] = bag
    imports = {Path(path).name: text[-300:] for path, text in bag.import_errors.items()}
    scenarios = cases_catalog()
    results = [execute(scenario, bag, scratch) for name, scenario in scenarios.items() if only in (None, name)]
    shutil.rmtree(folder, ignore_errors=True)
    covered = sorted({step["dag_id"] for item in results for step in item["steps"] if "dag_id" in step})
    return {
        "at": datetime.now(UTC).isoformat(),
        "versions": versions(),
        "substitutions": list(SUBSTITUTIONS),
        "import_errors": imports,
        "dags": covered,
        "ok": not imports and all(item["ok"] for item in results),
        "scenarios": results,
        "seconds": round(time.monotonic() - started, 1),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", help="run one scenario by name")
    arguments = parser.parse_args()
    report = run_all(arguments.case)
    sys.stdout.write(REPORT_PREFIX + json.dumps(report, default=str) + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
