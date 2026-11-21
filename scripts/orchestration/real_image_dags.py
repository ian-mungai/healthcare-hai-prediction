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
import hashlib
import importlib
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from collections.abc import Callable, Iterator, Mapping, Sequence
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
    "the layer acceptance report (orchestration/layer_report.py): its listing, closing evidence and acceptance functions are replaced by the stand-ins of the "
    "airflow_mode_cases rig, because they read the S3 object versions, the ECS hosts and the staged code archive, which the bridge does not model",
    "the fake task ends at its first status read (finish_after 1) and its exit codes are planned by the scenario",
    "USER is set when the environment has none: the check runs python without the image entrypoint that adds the passwd entry for the user id",
    "the Airflow metadata database is SQLite in a temporary folder and DAG.test() runs the tasks in process (no scheduler, no API server)",
    "the Airflow REST API that the first DAG's tasks call (orchestration/dag_runtime.py): the in-memory DAG runs of airflow_api_fake.py behind a loopback "
    "server that checks the HS512 token, reached with AIRFLOW_API_PORT and a stand-in signing value. A child DAG run of the first DAG is a record that ends "
    "over a fake clock (dag_runtime.CLOCK and SLEEP), not a run of the child DAG: each stage DAG runs in a step of its own. No task is marked passed without "
    "running",
)
FAKE_CREDENTIAL = "stand-in"
API_SIGNING = "stand-in-signing-value"
LOG_GROUP = "example_jobs_dev"
STAGE_PREFIXES = ("lakehouse/run_stage/", "drift/", "research/")  # the staged inputs the launch role reads (the run stage, the drift and the research records)
SCRATCH_FREE = 400 * 2**30
DONE_STATE = "success"
TERMINAL_STATES = frozenset({"success", "failed"})  # the states in which a DAG run is finished; every other state is evidence of nothing
BAG: dict[str, Any] = {}
SOURCES = (
    "orchestration/airflow_steps.py",
    "airflow_plugins/hai_timetables.py",
    "orchestration/dag_runtime.py",
    "orchestration/ingestion_schedule.py",
    "orchestration/ingestion_timetables.py",
    "orchestration/ecs_launch.py",
    "orchestration/ecs_launch_operator.py",
    "orchestration/ecs_launch_provider.py",
    "orchestration/ecs_preflight.py",
    "orchestration/promotion_chain.py",
    "orchestration/training_contract.py",
    "scripts/orchestration/airflow_api_fake.py",
    "scripts/orchestration/aws_http_bridge.py",
    "scripts/orchestration/ecs_fake.py",
    "scripts/orchestration/ecs_launch_fake.py",
    "scripts/orchestration/real_image_dags.py",
    "scripts/orchestration/schedule_timetable_cases.py",
    "scripts/orchestration/in_image_checks.py",
    "scripts/orchestration/run_real_image_dags_e2e.py",
    "services/orchestration/docker-compose.orchestration.yaml",
)  # the sources the verdict binds to, with the DAG files; the host hashes all of them, the image only those it has mounted
HOST_ONLY = ("services/orchestration/docker-compose.orchestration.yaml",)
IMAGE_SOURCES = tuple(name for name in SOURCES if name not in HOST_ONLY)


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
    calls the run sends, and ``family`` the host task definition family they name. ``reason`` is text each failing task's error must contain, ``skipped`` the
    tasks that must end skipped (a mapped trigger with no request, a research run that waits), ``plan`` the exit code per container of the task, ``sent`` a
    check of the environment of each container of the first RunTask call and ``after`` a check of the rig once the run ended. Every task of a run runs: no
    task is ever marked passed without running. A task that calls the Airflow REST API (the first DAG's) gets ``api``, which builds the in-memory API of the
    step from the rig, and ``api_after`` checks that API once the run ended."""

    dag_id: str
    params: Mapping[str, Any]
    starts: int
    family: str | None = None
    states: Mapping[str, str] | None = None
    run_state: str = "success"
    reason: str | None = None
    plan: Mapping[str, int | None] | None = None
    end: str | None = None
    environment: Mapping[str, str] = field(default_factory=dict)
    prepare: Callable[[Any], None] | None = None
    plant: Plant | None = None
    check: Callable[[Any, dict[str, Any]], None] | None = None
    skipped: tuple[str, ...] = ()
    sent: Callable[[dict[str, dict[str, str]]], None] | None = None
    after: Callable[[Any], None] | None = None
    api: Callable[[Any], Any] | None = None
    api_after: Callable[[Any, Any], None] | None = None


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
            # dag.test() syncs the sibling DAGs with the creation default (paused). The mapped trigger of the training DAG fails with DagIsPaused for a paused
            # research_dag (fail_when_dag_is_paused), so the rig leaves its DAGs unpaused as the owner does before a deployed run. Production keeps "true".
            "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "false",
            "AIRFLOW__CORE__UNIT_TEST_MODE": "false",
            "AIRFLOW__OPENLINEAGE__DISABLED": "true",
        }
    )
    os.environ.setdefault(
        "USER", "airflow"
    )  # the check runs python without the image entrypoint, which adds the passwd entry that Airflow needs for any other user id


class Observer:
    """Records the error each launch step raises (the task SDK keeps only the task's state) and lets it go on."""

    NAMES = ("launch_step", "preflight_step", "job_step", "evidence_step", "research_missing_models", "research_chain_step")

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
    rig.ssm = cases.LaunchSsm({cases.HOLD_PARAMETER: answer, cases.LOCK_PARAMETER: cases.no_lock(), cases.CURRENT_CODE_PARAMETER: cases.no_lock()})
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
        cases.CAP_SETTING: str(cases.JOB_MEMORY),
        "HAI_LAUNCH_HOST_HOLD_PARAMETER": cases.HOLD_PARAMETER,
        "HAI_LAUNCH_APPLY_LOCK_PARAMETER": cases.LOCK_PARAMETER,
        "HAI_LAUNCH_CURRENT_CODE_RUN_PARAMETER": cases.CURRENT_CODE_PARAMETER,
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
    api = step.api(rig) if step.api is not None else None
    with make_bridge(rig) as bridge, contextlib.ExitStack() as served:
        environment = launch_environment(rig, bridge.endpoint, scratch) | dict(step.environment)
        if api is not None:
            from scripts.orchestration.airflow_api_fake import ApiServer

            runtime = importlib.import_module("orchestration.dag_runtime")
            server = served.enter_context(ApiServer(api, API_SIGNING))
            request = runtime.airflow_call

            def signed_call(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
                """The runtime's own request, with the port and the signing value in the environment for this call only (Airflow's own services read theirs)."""
                with mock.patch.dict(os.environ, {"AIRFLOW_API_PORT": str(server.port), "AIRFLOW__API_AUTH__JWT_SECRET": API_SIGNING}):
                    answer: tuple[int, Any] = request(method, path, body)
                return answer

            served.enter_context(mock.patch.object(runtime, "airflow_call", signed_call))
            served.enter_context(mock.patch.object(runtime, "CLOCK", api.clock))  # a wait of hours takes no time: the clock and the wait are the API's
            served.enter_context(mock.patch.object(runtime, "SLEEP", api.sleep))
        with (
            mock.patch.dict(os.environ, environment),
            mock.patch.object(airflow_steps, "UNAVAILABLE_SECONDS", 1.0),
            mock.patch.object(shutil, "disk_usage", lambda _path: SimpleNamespace(total=SCRATCH_FREE, used=0, free=SCRATCH_FREE)),
            mock.patch.object(airflow_steps.layer_report, "save_before_listing", lambda access, layer, run: rig.reports.append(f"listing {layer} {run}")),
            mock.patch.object(
                airflow_steps.layer_report, "save_closing_evidence", lambda access, layer, run, **_: rig.reports.append(f"closing {layer} {run}")
            ),
            mock.patch.object(
                airflow_steps.layer_report,
                "accept",
                lambda access, layer, run: rig.reports.append(f"report {layer} {run}") or {"report": "stand_in", "passed": True},
            ),
            observer.watching(),
            planted(step.plant),
        ):
            run = dag.test(run_conf=dict(step.params))
        states = states_of(run)
        run_state = str(getattr(run.state, "value", run.state))
        requests = list(bridge.requests)
    calls = [arguments for name, arguments in rig.ecs.calls if name == "run_task"][started:]
    facts: dict[str, Any] = {
        "dag_id": step.dag_id,
        "run_state": run_state,
        "expected": step.run_state,
        "tasks": states,
        "run_task_calls": len(calls),
        "task_definitions": rig.ecs.sent[sent:],
        "errors": observer.errors,
        "bridge_requests": len(requests),
    }
    check_run(rig, step, facts, calls)
    if api is not None and step.api_after is not None:
        step.api_after(rig, api)
    if step.after is not None:
        step.after(rig)
    return facts


def launch_run_id(step: Step) -> str:
    """The run ID the launch of a step runs under: the run ID of its form, or for the retry DAG the new run ID that its original run and day give."""
    params = dict(step.params)
    if "original_run" in params:
        from orchestration.ingestion_schedule import retry_run_id

        return retry_run_id(str(params["original_run"]), int(params["day"]))
    return str(params["run_id"])


def check_run(rig: Any, step: Step, facts: dict[str, Any], calls: list[dict[str, Any]]) -> None:
    """Break an expectation when the run is not the step's."""
    where = f"{step.dag_id} {launch_run_id(step)}"
    wanted = dict(step.states) if step.states is not None else {task: "skipped" if task in step.skipped else DONE_STATE for task in facts["tasks"]}
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
    run_id = launch_run_id(step)
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
    if step.sent is not None:
        containers = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in first["overrides"]["containerOverrides"]}
        step.sent(containers)
        if "SERVING_REQUEST" in containers.get("serve", {}):
            serving_request_is_bound(rig, run_id, containers)
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


def trigger_forms() -> dict[str, dict[str, Any]]:
    """The values each shipped DAG's trigger form requires (a parameter with no default), by DAG id. The form check runs each DAG with only these."""
    import importlib

    c, training = rig_modules().cases, importlib.import_module("orchestration.training_contract")
    silver, gold, base, bronze = c.SILVER_RUN, c.GOLD_RUN, c.FEATURES_RUN, c.BRONZE_RUN
    parts = training.run_id_parts(bronze)
    pipeline_run = training.compose_run_id(training.PIPELINE_RUN, parts.hex, parts.attempt)
    models = ("m1", "m2", "m3", "m4", "m5", "m6")
    return {
        "ingestion_dag": {"run_id": c.PUBLISHER_RUN},
        "ingestion_retry_dag": {"original_run": c.PUBLISHER_RUN, "day": 2},
        "silver_transform_dag": {"run_id": silver},
        "gold_transform_dag": {"run_id": gold, "silver_snapshot_run": silver},
        "features_base_dag": {"run_id": base, "gold_snapshot_run": gold},
        **{f"features_{model}_dag": {"run_id": training.model_features_run_id(base, model), "base_snapshot_run": base} for model in models},
        "drift_monitoring_dag": {"run_id": c.DRIFT_RUN, "model": "m1", "features_run": c.SET_FEATURES_RUN},
        "drift_live_check_dag": {"run_id": silver, "model": "m1", "hai_type": "HAI_1"},
        "training_dag": {"run_id": training.compose_run_id("training", "0a1b2c3d", 1, "ols"), "model": "m1"},
        "model_promotion_dag": {"run_id": c.PROMOTE_RUN, "hai_type": "HAI_1", "model_version": "3"},
        "research_dag": {"run_id": training.research_run_id(c.SERVE_BASE), "base_run": c.SERVE_BASE},
        "serving_deploy_dag": {"run_id": c.SERVE_RUN, "hai_type": "HAI_1", "deploy_mode": "stable"},
        "metadata_ingest_dag": {"run_id": training.compose_run_id("metadata", "0a1b2c3d"), "features_run": base},
        "bronze_dag": {"run_id": bronze},
        training.PIPELINE_DAG: {"run_id": pipeline_run},
    }


def form_check(_rig: Any) -> None:
    """The trigger form of every shipped DAG, validated by Airflow's own ``Param`` code as the trigger does (the DAG's params updated with the trigger's
    values, then validated). Each DAG accepts a trigger with only its required values and fills the defaults. It refuses a missing run ID, a run ID that
    breaks its pattern, ``latest`` where a producer run is required, a value of another JSON type, and a serve mode, an infection type or a model that its
    closed request does not list. The ingestion DAG also refuses a run kind it does not list and a capture root that is not ``raw/`` or one test folder,
    and it fills the bootstrap kind under ``raw/``."""
    import copy

    c = rig_modules().cases
    run = c.SILVER_RUN
    required = trigger_forms()
    own_run = {"ingestion_retry_dag": "original_run"}  # the retry DAG's form names the run it retries, and the retry's own run ID comes from it and the day

    def wrong_forms(dag_id: str) -> tuple[dict[str, Any], ...]:
        name = own_run.get(dag_id, "run_id")
        return ({}, {name: "latest"}, {name: "x" + run}, {name: 20261104}, {name: run, "mode": "mock"})

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
        problems.extend(f"{dag_id} accepted {bad}" for bad in wrong_forms(dag_id) if not refused(dag_id, {**conf, **bad} if bad else {}))
    for dag_id, name in (
        ("gold_transform_dag", "silver_snapshot_run"),
        ("features_base_dag", "gold_snapshot_run"),
        *((f"features_{model}_dag", "base_snapshot_run") for model in ("m1", "m2", "m3", "m4", "m5", "m6")),
    ):
        if not refused(dag_id, {**required[dag_id], name: "latest"}):
            problems.append(f"{dag_id} accepted latest for {name}")
    filled = validated("ingestion_dag", required["ingestion_dag"])
    if (filled.get("run_kind"), filled.get("capture_root")) != ("bootstrap", "raw/"):
        problems.append(f"ingestion_dag fills {filled.get('run_kind')!r} and {filled.get('capture_root')!r} instead of the bootstrap kind under raw/")
    for bad in (
        {"run_kind": "retry"},
        {"run_kind": ""},
        {"capture_root": "raw"},
        {"capture_root": "lakehouse/"},
        {"capture_root": "ingestion_test/t1/"},
        {"capture_root": "raw/\n"},
    ):
        if not refused("ingestion_dag", {**required["ingestion_dag"], **bad}):
            problems.append(f"ingestion_dag accepted {bad}")
    for dag_id, name, value in (
        ("ingestion_retry_dag", "day", 8),
        ("ingestion_retry_dag", "day", "1"),
        ("research_dag", "base_run", "latest"),
        ("research_dag", "hai_types", ["HAI_9"]),
        ("drift_monitoring_dag", "features_run", "latest"),
        ("drift_monitoring_dag", "model", "m7"),
        ("drift_live_check_dag", "model", "m7"),
        ("drift_live_check_dag", "hai_type", "HAI_9"),
        ("training_dag", "model", "m7"),
        ("serving_deploy_dag", "deploy_mode", "rollback"),
        ("serving_deploy_dag", "deploy_mode", "withdraw"),
        ("serving_deploy_dag", "hai_type", "HAI_9"),
        ("model_promotion_dag", "mode", "withdraw"),
    ):
        if not refused(dag_id, {**required[dag_id], name: value}):
            problems.append(f"{dag_id} accepted {name}={value!r}")
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
            "RunTask with a strategy whose weights are all zero": (
                lambda: ecs.run_task(**{**base, "capacityProviderStrategy": [{"capacityProvider": "stand-in_host", "weight": 0}]}),
                ("InvalidParameterException",),
            ),
            "RunTask with propagateTags SERVICE": (lambda: ecs.run_task(**base, propagateTags="SERVICE"), ("InvalidParameterException",)),
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
            "RunTask with propagateTags TASK_DEFINITION": lambda: ecs.run_task(**base, propagateTags="TASK_DEFINITION"),
            "GetObject of a staged input": lambda: s3.get_object(Bucket=bucket, Key=c.RUN_KEY.format(run=c.SILVER_RUN, name="code.sha256")),
            "GetParameter of the launch hold": lambda: ssm.get_parameter(Name=c.HOLD_PARAMETER),
        }
        for name, call in accepted.items():
            answer = aws_error(call)
            if answer != "accepted":
                problems.append(f"{name}: answered {answer}, expected acceptance")
    require(not problems, "; ".join(problems))


def serving_request_is(expected: Mapping[str, Any]) -> Callable[[dict[str, dict[str, str]]], None]:
    """A check of the serve container's environment: the request the preflight derived from the closed launch request."""

    def check(environment: dict[str, dict[str, str]]) -> None:
        body = environment["serve"]["SERVING_REQUEST"]
        require(json.loads(body) == dict(expected), f"the derived serving request is {body}, not {dict(expected)}")

    return check


def serving_request_is_bound(rig: Any, run: str, environment: dict[str, dict[str, str]]) -> None:
    """Break unless the claim of the serve launch binds the digest of the request the serve container got: the digest is the launch's request hash, which a
    reattached attempt must share, and no container variable."""
    body = environment["serve"]["SERVING_REQUEST"]
    claim = launch_records(rig, run, "serve").get("claim")
    if claim is None:
        raise Expectation(f"the serve launch of {run} has no claim record")
    bound = claim["bound"]["request_hash"]
    require(bound == hashlib.sha256(body.encode()).hexdigest(), f"the claim of {run} binds the request hash {bound}, not the digest of the serving request")


def drift_inputs_are(model: str, features_run: str, code_run: str) -> Callable[[dict[str, dict[str, str]]], None]:
    """A check of the drift launch's containers: the baseline container is told the model, the current-window container the model's features run and the fetch
    container the run whose code the binding gate bound. A missing container or value, ``latest`` or another run is a break."""

    def check(environment: dict[str, dict[str, str]]) -> None:
        for container, name, value in (("baseline", "FEATURE_SET", model), ("current", "INPUT_RUN_ID", features_run), ("fetch", "CODE_RUN_ID", code_run)):
            found = environment.get(container, {}).get(name)
            require(found == value, f"the {container} container got {name}={found!r}, not {value!r}")

    return check


def code_bindings(rig: Any) -> dict[str, bytes]:
    """The code run bindings staged in the rig, by key."""
    return {key: body for key, body in rig.stage.objects.items() if key.endswith("/code_run.json")}


def research_conf(base_run: str, hai_types: Sequence[str]) -> dict[str, Any]:
    """The configuration of the research run that a training run of a promotion model with this base run starts."""
    import importlib

    return {
        "run_id": importlib.import_module("orchestration.training_contract").research_run_id(base_run),
        "mode": "real",
        "base_run": base_run,
        "population": "primary",
        "hai_types": list(hai_types),
        "mlflow": "aws",
    }


def research_trigger_run(training_run: str) -> str:
    """The DAG run ID of the research run that a training run starts, so that each model's trigger has a run of its own."""
    from orchestration import training_contract

    return training_contract.research_dag_run_id(training_run)


def triggered_confs(dag_id: str, run_id: str) -> list[Any]:
    """The configuration of each DAG run of ``dag_id`` with ``run_id`` in the Airflow metadata database."""
    import importlib

    runs = importlib.import_module("airflow.models.dagrun").DagRun.find(dag_id=dag_id, run_id=run_id)
    return [item.conf for item in runs]


def drift_objects(drift_run: str, model: str, *, gated: bool = True, legacy: bool = False) -> dict[str, bytes]:
    """The staged objects of a quiet drift run of ``model``: one drift record of HAI_1 that did not trigger, and the marker that lists it. ``gated`` adds the
    gate decision of the drift rule (``decide_drift_gate``), as the drift task writes it; a marker without it is the one the drift DAG refuses. The record has
    the shape the drift task writes now (schema version 2, with ``excluded_features``); ``legacy`` gives the version 1 shape of a run stored before it."""
    import importlib

    training = importlib.import_module("orchestration.training_contract")
    record: dict[str, Any] = {
        "kind": "drift_record",
        "hai_type": "HAI_1",
        "run_id": drift_run,
        "triggered": False,
        "status": "checked",
        "population": "primary",
    }
    record |= {"schema_version": 1} if legacy else {"schema_version": 2, "excluded_features": []}
    body = json.dumps(record, sort_keys=True).encode()
    marker: dict[str, Any] = {"run_id": drift_run, "files": {"drift_HAI_1.json": hashlib.sha256(body).hexdigest()}}
    if gated:
        baseline: dict[str, Any] = {"monitored": ["HAI_1"], "unmonitored": {}}
        marker |= {"model": model, **baseline, "gate": training.decide_drift_gate(model, baseline, {"HAI_1": record})}
    return {f"drift/{drift_run}/drift_HAI_1.json": body, f"drift/{drift_run}/_complete.json": json.dumps(marker, sort_keys=True).encode()}


def research_objects(
    base_run: str,
    research_run: str,
    challengers: Mapping[str, Mapping[str, Any] | None],
    canary: str | None,
    production: str | None = "1",
) -> dict[str, bytes]:
    """The records that the research phase and the canary check store: a research record for each type (naming the Production version the research saw,
    None for none, and the features run of each model) and a canary result for each type with a challenger when ``canary`` names a decision (promote or
    hold). The features job's hold record is not staged: the chain reads none, and the release phase reads its own."""
    import importlib

    training = importlib.import_module("orchestration.training_contract")
    found: dict[str, bytes] = {}
    for hai_type, challenger in challengers.items():
        features_runs: dict[str, str] = {model: training.model_features_run_id(base_run, model) for model in training.PROMOTION_MODELS}
        research = {
            "schema": 1,
            "hai_type": hai_type,
            "base_run": base_run,
            "runs": {},
            "features_runs": features_runs,
            "promotion": {"registered_model": f"hai_model_{hai_type}", "candidates": [], "challenger": challenger, "production_version": production},
        }
        found[training.record_key("research", research_run, hai_type)] = json.dumps(research).encode()
        if challenger is not None and canary is not None and production is not None:
            result = {
                "schema": 1,
                "research_run": research_run,
                "hai_type": hai_type,
                "model": challenger["model"],
                "version": challenger["version"],
                "decision": canary,
                "failed": [],
            }
            found[training.record_key("canary", research_run, hai_type)] = json.dumps(result).encode()
    return found


def cases_catalog() -> dict[str, Scenario]:
    """Every scenario by name."""
    import importlib

    modules = rig_modules()
    c = modules.cases
    training = importlib.import_module("orchestration.training_contract")
    silver, gold, features = (training.chain_layer_run(layer, c.BRONZE_RUN) for layer in ("silver", "gold", "features"))
    parts = training.run_id_parts(c.BRONZE_RUN)
    pipeline_run = training.compose_run_id(training.PIPELINE_RUN, parts.hex, parts.attempt)
    features_m1 = training.model_features_run_id(features, "m1")  # the drift check and the training run of a model chain from the run of its own set
    drift = training.model_drift_run_id(features_m1)
    train = training.trained_run_id(features, "m1")
    chain = ("preflight", "job", "evidence", "gate")
    challenger = {"model": "m2", "version": "4", "verdict": "pass", "source_run": "mlrun4"}
    stable_request = {"hai_type": "HAI_1", "deploy_mode": "stable", "candidate_version": None, "percent": 0, "canary_ttl_hours": None, "research_run": None}
    canary_request = {
        "hai_type": "HAI_1",
        "deploy_mode": "canary",
        "candidate_version": challenger["version"],
        "percent": 10,
        "canary_ttl_hours": 24,  # the lifetime the job table fixes: the serve job gives the canary an expiry after it
        "research_run": c.SERVE_RESEARCH,
    }

    def job_states(*, failed: tuple[str, ...] = ()) -> dict[str, str]:
        return {name: "failed" if name in failed else DONE_STATE for name in chain}

    bound: dict[str, dict[str, bytes]] = {}

    def stage_drift(rig: Any, gated: bool = True) -> None:
        rig.stage.objects.update(drift_objects(drift, "m1", gated=gated))
        bound["before"] = code_bindings(rig)

    def drift_bound_nothing(rig: Any) -> None:
        require(code_bindings(rig) == bound["before"], "the drift launch bound a run: the binding gate binds every run of the pipeline")

    metadata_run = training.metadata_run_for_base(features)  # the catalog run a training run of this base starts

    def stage_metadata(rig: Any) -> None:
        rig.stage_runs(metadata_run)

    def stage_retry(rig: Any) -> None:
        import importlib

        rig.stage_runs(importlib.import_module("orchestration.ingestion_schedule").retry_run_id(c.PUBLISHER_RUN, 2))

    def family(job: str) -> str:
        return f"example_dev_job_{job}_host"

    def joined(rig: Any, facts: dict[str, Any]) -> None:
        require(not facts["task_definitions"], f"a joined run sent task definitions: {facts['task_definitions']}")

    drift_params = {"run_id": drift, "mode": "real", "mlflow": "aws", "model": "m1", "features_run": features_m1}
    drift_states = {"drift_run": DONE_STATE, "decide": DONE_STATE, "trigger_training": "skipped"}  # a quiet run maps no training request
    # The training run that the drift run starts when its gate trains: its own model's set, the primary population and the drift run as its trigger.
    train_params = {**c.TRAIN_PARAMS, "run_id": train, "model": "m1", "features_run": features_m1, "trigger": f"drift:{drift}"}

    # The first DAG runs every task in the image. Its check of the pipeline's DAGs, each stage's join of a child run and its wait for the model routes call the
    # Airflow REST API, which an in-memory API behind a loopback server stands in for (airflow_api_fake.py; the runtime's own signed requests reach it). The
    # child DAG runs are records of that API, so each stage DAG runs in a step of its own. The binding task runs against the rig's staged code.
    from scripts.orchestration.airflow_api_fake import Cluster

    layer_runs = {layer: training.chain_layer_run(layer, pipeline_run) for layer in training.PIPELINE_STAGES}
    stage_dags = dict(training.PIPELINE_STAGE_DAGS)
    first_tasks = ("check_dags", "run_ingestion", "bind_code", "run_bronze", "run_silver", "run_gold", "run_features", "await_downstream", "gate")
    models = ("m1", "m2", "m3", "m4", "m5", "m6")
    watched: dict[str, Any] = {}

    def pipeline_form(start: str = "ingestion") -> dict[str, str]:
        return {"run_id": pipeline_run, "mode": "real", "start_from": start, "run_kind": "bootstrap", "capture_root": "raw/"}

    def bound_keys() -> set[str]:
        """The binding objects the binding task writes: the silver, gold and features runs, and for each model its features, drift and training run, each
        bound to the code run of the bronze run. The rig stages code under the bronze run itself, which a bronze run keeps (a bronze run of an ingestion run is
        bound to that run's code), so it has no binding object here."""
        runs = [layer_runs[layer] for layer in ("silver", "gold", "features")]
        for model in models:
            model_features = training.model_features_run_id(layer_runs["features"], model)
            model_drift = training.model_drift_run_id(model_features)
            runs += [model_features, model_drift, training.model_training_run_id(model_drift, model)]
        return {f"lakehouse/run_stage/{run}/inputs/code_run.json" for run in runs}

    def first_api(
        failing: tuple[str, ...] = (),
        existing: Mapping[str, str] | None = None,
        paused: tuple[str, ...] = (),
        started_with: Mapping[str, Any] | None = None,
    ) -> Callable[[Any], Any]:
        """The API of one step of the first DAG: the stage runs in ``failing`` end failed, ``existing`` maps a stage to the state of a run of it that exists
        already (started with the settings of the stage unless ``started_with`` gives others) and ``paused`` are DAGs that are paused. It notes which runs
        were bound to code whenever a run is created."""

        def build(rig: Any) -> Any:
            api = Cluster()
            api.failing = {(stage_dags[layer], layer_runs[layer]) for layer in failing}
            api.paused = set(paused)
            for layer, state in (existing or {}).items():
                key = (stage_dags[layer], layer_runs[layer])
                api.create(*key, "earlier", (started_with or {}).get(layer, stage_conf(layer)))
                api.runs[key]["state"] = state
            api.log.clear()
            seen: dict[tuple[str, str], frozenset[str]] = {}
            watched.update(seen=seen, before=frozenset(code_bindings(rig)))

            def note_binding(dag: str, run: str) -> None:
                seen.setdefault((dag, run), frozenset(code_bindings(rig)))

            api.on_create = note_binding
            return api

        return build

    def stage_conf(layer: str) -> dict[str, str]:
        conf = {"run_id": layer_runs[layer], "mode": "real"}
        if layer == "ingestion":
            conf |= {"run_kind": "bootstrap", "capture_root": "raw/"}
        if layer == "gold":
            conf["silver_snapshot_run"] = layer_runs["silver"]
        if layer == "features":
            conf["gold_snapshot_run"] = layer_runs["gold"]
        return conf

    def made_exactly(*layers: str, bound: bool = True, failed: str | None = None) -> Callable[[Any, Any], None]:
        """The first DAG created the runs of exactly these stages, in this order and with the settings of the stage, after it read every DAG it needs. Each run
        ended in success but the one named by ``failed``. The binding task (when it ran: ``bound``) bound its runs to one code run before every later stage."""

        def check(rig: Any, api: Any) -> None:
            made = api.created_by("api")
            wanted = [(stage_dags[layer], layer_runs[layer]) for layer in layers]
            require(made == wanted, f"the first DAG created {made}, expected {wanted}")
            for layer in layers:
                conf = api.runs[(stage_dags[layer], layer_runs[layer])]["conf"]
                require(conf == stage_conf(layer), f"the {layer} run was created with {conf}, expected {stage_conf(layer)}")
                state = api.runs[(stage_dags[layer], layer_runs[layer])]["state"]
                require(state == ("failed" if layer == failed else "success"), f"the {layer} run ended {state}")
            first_post = next((index for index, item in enumerate(api.calls) if item[0] == "POST"), len(api.calls))
            reads = {path for method, path in api.calls[:first_post] if method == "GET" and path.count("/") == 4}
            require(len(reads) >= 14, f"the first DAG read {len(reads)} DAGs before it created a run: {sorted(reads)}")
            seen, before, keys = watched["seen"], watched["before"], bound_keys()
            for layer in layers:
                at = seen[(stage_dags[layer], layer_runs[layer])]
                if layer == "ingestion":
                    require(not keys & at, f"ingestion started after the binding task ran: {sorted(keys & at)}")
                else:
                    require(keys <= at, f"the {layer} run was created before the binding task bound {sorted(keys - at)}")
            if bound:
                bindings = code_bindings(rig)
                require(keys <= set(bindings), f"the binding task did not bind {sorted(keys - set(bindings))}")
                require(len({bindings[key] for key in keys}) == 1, "the binding task bound the runs to different code runs")
            else:
                require(set(code_bindings(rig)) == before, "a run was bound although the binding task never ran")

        return check

    def whole_run_checked(rig: Any, api: Any) -> None:
        made_exactly(*training.PIPELINE_STAGES)(rig, api)
        for model in models:
            model_features = training.model_features_run_id(layer_runs["features"], model)
            route = [(next(dag for dag, name in training.FEATURE_DAGS.items() if name == model), model_features)]
            route.append(("drift_monitoring_dag", training.model_drift_run_id(model_features)))
            require(all(api.runs[key]["state"] == "success" for key in route), f"the route of {model} did not end in success: {route}")
        for model in training.PROMOTION_MODELS:
            drift_run = training.model_drift_run_id(training.model_features_run_id(layer_runs["features"], model))
            research = ("research_dag", training.research_dag_run_id(training.model_training_run_id(drift_run, model)))
            require(research in api.runs and api.runs[research]["state"] == "success", f"the research run of {model}: {sorted(api.runs)}")

    def ended_before_next(layer: str, *layers: str) -> Callable[[Any, Any], None]:
        """``made_exactly`` the stages in ``layers``, and the run of ``layer`` that existed and was running ended before the stage after it was created."""
        made = made_exactly(*layers)

        def check(rig: Any, api: Any) -> None:
            made(rig, api)
            key = (stage_dags[layer], layer_runs[layer])
            after = stages[stages.index(layer) + 1]
            ended = next((index for index, item in enumerate(api.log) if item[:3] == ("end", *key)), -1)
            created = next((index for index, item in enumerate(api.log) if item[:3] == ("create", stage_dags[after], layer_runs[after])), -1)
            require(0 <= ended < created, f"the {after} run was created at {created}, the running {layer} run ended at {ended}: {api.log}")

        return check

    def refused_before_any_child(rig: Any, api: Any) -> None:
        require(not api.created_by("api") and frozenset(code_bindings(rig)) == watched["before"], f"a child or a binding came before the refusal: {api.log}")

    def other_settings(layer: str) -> tuple[tuple[str, dict[str, str]], ...]:
        """For each state a child can be in, the settings of a run of this stage that the first DAG would not have started: another logical run, another mode
        and, for the stages that read a producer run, another producer run."""
        right = stage_conf(layer)
        changes = [{**right, "run_id": training.chain_layer_run(layer, "pipeline_a1a1a1a1")}, {**right, "mode": "fixture"}]
        if layer in ("gold", "features"):
            producer, before = ("silver_snapshot_run", "silver") if layer == "gold" else ("gold_snapshot_run", "gold")
            changes.append({**right, producer: training.chain_layer_run(before, "pipeline_a1a1a1a1")})
        return tuple((state, change) for change in changes for state in ("success", "running", "failed"))

    def stopped_at(task: str) -> dict[str, str]:
        """The task states of a run that failed at ``task``: the tasks before it passed, the tasks after it never ran and the gate, which runs after a failure,
        rejected the run."""
        index = first_tasks.index(task)
        states = {
            name: "success" if position < index else "failed" if position == index else "upstream_failed" for position, name in enumerate(first_tasks[:-1])
        }
        return {**states, "gate": "failed"}

    def stage_bronze_request(rig: Any) -> None:
        pair, _ = c.table_pair()
        rig.stage.stage(c.BRONZE_RUN, "bronze_request.json", c.bronze_request(pair))

    def bronze_jobs_ran(rig: Any, facts: dict[str, Any]) -> None:
        """The stage job then the load job launched, each with the full launch record of a job that succeeded."""
        sent = [arguments for name, arguments in rig.ecs.calls if name == "run_task"][-2:]
        require(
            facts["run_task_calls"] == 2 and len(sent) == 2,
            f"{facts['run_task_calls']} RunTask calls for the bronze DAG, expected the stage job and the load job",
        )
        names = [str(arguments["taskDefinition"]) for arguments in sent]
        require("bronze_stage" in names[0] and "bronze_stage" not in names[1] and "bronze" in names[1], f"the bronze jobs launched as {names}")
        for job in ("bronze_stage", "bronze"):
            records = launch_records(rig, c.BRONZE_RUN, job)
            require({"claim", "intent", "started", "end"} <= set(records), f"the launch records of the {job} job are {sorted(records)}")
            require(records["end"]["outcome"] == "succeeded", f"the {job} job ended {records['end']['outcome']}")

    pipeline_steps = (
        Step(training.PIPELINE_DAG, pipeline_form(), 0, api=first_api(), api_after=whole_run_checked),
        # The bronze DAG's two jobs launch through the same launch library as every layer: the stage job lists the sources and the load job runs its request.
        Step("bronze_dag", {"run_id": c.BRONZE_RUN, "mode": "real"}, 2, prepare=stage_bronze_request, check=bronze_jobs_ran),
    )
    bind_step = pipeline_steps[0]
    stages = training.PIPELINE_STAGES
    first_dag_scenarios = (
        # Each start runs exactly the stages from the chosen one on, after the binding, and creates none of the earlier ones.
        *(
            Scenario(
                f"the_first_dag_starts_from_{layer}",
                (Step(training.PIPELINE_DAG, pipeline_form(layer), 0, api=first_api(), api_after=made_exactly(*stages[position:])),),
                make_rig,
            )
            for position, layer in enumerate(stages)
            if position
        ),
        # A fresh child that fails stops the run: the stage task fails, no later stage exists and the gate rejects the run.
        *(
            Scenario(
                f"the_first_dag_stops_when_the_{layer}_run_fails",
                (
                    Step(
                        training.PIPELINE_DAG,
                        pipeline_form(),
                        0,
                        states=stopped_at(f"run_{layer}"),
                        run_state="failed",
                        api=first_api(failing=(layer,)),
                        api_after=made_exactly(*stages[: position + 1], bound=position > 0, failed=layer),
                    ),
                ),
                make_rig,
            )
            for position, layer in enumerate(stages)
        ),
        # A run that exists is joined by its own outcome and never created again: a success is adopted, a running run is waited for and a failed run fails the
        # stage with no later stage created.
        Scenario(
            "the_first_dag_joins_a_child_that_exists_already",
            (
                Step(
                    training.PIPELINE_DAG,
                    pipeline_form("bronze"),
                    0,
                    api=first_api(existing={"bronze": "success"}),
                    api_after=made_exactly("silver", "gold", "features"),
                ),
                Step(
                    training.PIPELINE_DAG,
                    pipeline_form("bronze"),
                    0,
                    api=first_api(existing={"bronze": "running"}),
                    api_after=ended_before_next("bronze", "silver", "gold", "features"),
                ),
                Step(
                    training.PIPELINE_DAG,
                    pipeline_form("bronze"),
                    0,
                    states=stopped_at("run_bronze"),
                    run_state="failed",
                    api=first_api(existing={"bronze": "running"}, failing=("bronze",)),
                    api_after=made_exactly(failed="bronze"),
                ),
                Step(
                    training.PIPELINE_DAG,
                    pipeline_form("bronze"),
                    0,
                    states=stopped_at("run_bronze"),
                    run_state="failed",
                    api=first_api(existing={"bronze": "failed"}),
                    api_after=made_exactly(),
                ),
            ),
            make_rig,
        ),
        # A child that exists but was started with other settings than the first DAG's (another logical run, another producer run, another mode) is not reused:
        # the stage task fails, nothing after it is created and the child is left as it was.
        *(
            Scenario(
                f"the_first_dag_refuses_a_{layer}_run_started_with_other_settings",
                tuple(
                    Step(
                        training.PIPELINE_DAG,
                        pipeline_form(layer),
                        0,
                        states=stopped_at(f"run_{layer}"),
                        run_state="failed",
                        api=first_api(existing={layer: state}, started_with={layer: changed}),
                        api_after=made_exactly(),
                    )
                    for state, changed in other_settings(layer)
                ),
                make_rig,
            )
            for layer in ("bronze", "gold", "features")
        ),
        # A paused DAG of the pipeline stops the run before any child or binding.
        Scenario(
            "the_first_dag_refuses_a_paused_dag_before_any_child",
            (
                Step(
                    training.PIPELINE_DAG,
                    pipeline_form(),
                    0,
                    states=stopped_at("check_dags"),
                    run_state="failed",
                    api=first_api(paused=("gold_transform_dag",)),
                    api_after=refused_before_any_child,
                ),
            ),
            make_rig,
        ),
    )
    pipeline = Scenario(
        "every_host_dag_runs_end_to_end",
        (
            *pipeline_steps,
            Step("silver_transform_dag", {"run_id": silver, "mode": "real"}, 1, family("silver"), job_states()),
            Step("gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}, 1, family("gold"), job_states()),
            Step("features_base_dag", {"run_id": features, "mode": "real", "gold_snapshot_run": gold}, 1, family("features")),
            *(
                Step(
                    f"features_{model}_dag",
                    {"run_id": training.model_features_run_id(features, model), "mode": "real", "base_snapshot_run": features},
                    1,
                    family(f"features_{model}"),
                )
                for model in ("m1", "m2", "m3", "m4", "m5", "m6")
            ),
            Step(
                "drift_monitoring_dag",
                drift_params,
                1,
                family("drift"),
                drift_states,
                prepare=stage_drift,
                sent=drift_inputs_are("m1", features_m1, c.BRONZE_RUN),
                after=drift_bound_nothing,
            ),
            # The same run's marker without the gate decision: the drift job joins its launch, and the decision step refuses the marker and trains nothing.
            Step(
                "drift_monitoring_dag",
                drift_params,
                0,
                family("drift"),
                {"drift_run": DONE_STATE, "decide": "failed", "trigger_training": "upstream_failed"},
                run_state="failed",
                prepare=lambda rig: stage_drift(rig, gated=False),
                check=joined,
            ),
            # A hand-built training run names no base run, so it starts neither the research nor the catalog; the automatic route names its base (drift DAG).
            Step("training_dag", train_params, 1, family("train"), skipped=("trigger_research", "start_metadata")),
            Step("model_promotion_dag", dict(c.PROMOTE_PARAMS), 1, family("promote"), job_states()),
            Step("serving_deploy_dag", dict(c.SERVE_FORM), 1, family("serve"), job_states(), sent=serving_request_is(stable_request)),
            Step("drift_live_check_dag", dict(c.LIVE_PARAMS), 1, family("drift"), job_states(), prepare=lambda rig: rig.stage_runs(c.LIVE_RUN)),
            Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 1, family("publisher"), job_states()),
            Step(
                "ingestion_retry_dag",
                {"original_run": c.PUBLISHER_RUN, "day": 2, "mode": "real"},
                1,
                family("publisher"),
                {**job_states(), "start_pipeline": "skipped"},  # a hand-started retry starts no pipeline run (the last retry day of a scheduled period does)
                prepare=stage_retry,
            ),
            Step("metadata_ingest_dag", {"run_id": metadata_run, "mode": "real", "features_run": features}, 1, family("metadata"), prepare=stage_metadata),
            Step("gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}, 0, family("gold"), job_states()),  # joins the launch
            Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 0, family("publisher"), job_states()),
        ),
        make_rig,
    )

    def ingestion(name: str, rig: Callable[[], Any], **changes: Any) -> Scenario:
        step = Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 0, family("publisher"), **changes)
        return Scenario(name, (step,), rig, after=no_launch)

    base, research = features, training.research_run_id(features)
    types = ["HAI_1", "HAI_2"]
    research_params = {"run_id": research, "mode": "real", "base_run": base, "population": "primary", "hai_types": types, "mlflow": "aws"}

    def stage_research(rig: Any) -> None:
        rig.stage.objects.update(research_objects(base, research, {"HAI_1": challenger, "HAI_2": None}, "promote"))

    def trained(model: str) -> Step:
        """The training run of a model on the base run, as the chain of its features, drift and training run IDs names it, which the gate bound to the code."""
        params = {**c.TRAIN_PARAMS, "run_id": training.trained_run_id(base, model), "model": model}
        return Step("training_dag", params, 1, family("train"), skipped=("trigger_research", "start_metadata"))

    def stage_first_champion(rig: Any) -> None:
        """A research record that names no Production version and no canary result."""
        rig.stage.objects.update(research_objects(base, research, {"HAI_1": challenger, "HAI_2": None}, None, production=None))

    def first_champion_ran(rig: Any, facts: dict[str, Any]) -> None:
        """Research, the promotion, the stable-only rollout and the owner notice: four tasks, no canary deploy or check, no release task and no read of a
        hold key, and the research record the chain read names no Production version, which makes the notice say the one-time exception (the text is checked in
        ``scripts.training.auto_promotion_cases``, because the training code is not in the Airflow image)."""
        first = rig.tasks_started() - facts["run_task_calls"]
        environments = [rig.environment(first + index) for index in range(4)]
        found = [item["serve"] if "serve" in item else item["promote"] for item in environments]
        runs = [research, *(training.chain_run_id(research, "HAI_1", step) for step in ("promote", "stable_deploy", "notice"))]
        require([item["RUN_ID"] for item in found] == runs, f"the chain ran under {[item['RUN_ID'] for item in found]}, not {runs}")
        phases = [item.get("PHASE") for item in found]
        require(phases == ["research", "promote", None, "notify"], f"the chain ran the phases {phases}")
        parameters = [json.loads(item["TRAINING_PARAMETERS"]) if "TRAINING_PARAMETERS" in item else None for item in found]
        expected = [
            {"base_run": base, "population": "primary", "hai_types": types, "mlflow": "aws"},
            {"hai_type": "HAI_1", "model_version": challenger["version"], "promotion_mode": "promote", "mlflow": "aws"},
            None,
            {"research_run": research, "hai_type": "HAI_1", "outcome": "promoted", "mlflow": "aws"},
        ]
        require(parameters == expected, f"the chain sent the parameters {parameters}")
        serving_request_is(stable_request)(environments[2])
        serving_request_is_bound(rig, runs[2], environments[2])
        for run, job in zip(runs, ("promote", "promote", "serve", "promote"), strict=True):
            end = rig.end(run, job)
            require(end is not None and end["outcome"] == "succeeded", f"{job} of {run} has no success record: {end}")
        started = [entry for entry in rig.stage.log if "online_hold" in entry]
        require(not started, f"the first champion read a hold key: {started}")
        require(not [item for item in environments if "feast" in item], "the first champion launched the release phase")
        record = json.loads(rig.stage.objects[training.record_key("research", research, "HAI_1")])
        promotion = record["promotion"]
        require("production_version" in promotion and promotion["production_version"] is None, f"the research record names a Production version: {promotion}")

    release_run = training.chain_run_id(research, "HAI_1", "release")
    release_features = training.model_features_run_id(base, challenger["model"])

    def release_task(environment: dict[str, dict[str, str]]) -> None:
        """The release task of the features family: its feast container runs only the release phase, for the challenger's model and the features run its
        training run read."""
        task = environment["feast"]
        require(task["RUN_ID"] == release_run, f"the release ran {task['RUN_ID']}, not {release_run}")
        require(
            (task["LAYER_PHASE"], task["RELEASE_MODEL"], task["FEATURES_RUN"]) == ("release", challenger["model"], release_features),
            f"the feast container was told {task}",
        )
        require("release" not in environment, f"the features task has a release container: {sorted(environment)}")
        other = {name: container["LAYER_PHASE"] for name, container in environment.items() if name != "feast" and "LAYER_PHASE" in container}
        require(set(other.values()) == {"release"} and "pin" in other, f"a build phase was not told the release phase: {other}")

    def chain_with_release_ran(rig: Any, facts: dict[str, Any]) -> None:
        """With a Production model: research, the canary deploy and check, the promotion, the release phase of the features family, the stable-only rollout
        and the owner notice: seven tasks in that order, each under the chain run of its own, and no read of a hold key (the release phase reads its own)."""
        first = rig.tasks_started() - facts["run_task_calls"]
        environments = [rig.environment(first + index) for index in range(7)]
        found = [item["serve"] if "serve" in item else item["feast"] if "feast" in item else item["promote"] for item in environments]
        runs = [
            research,
            *(training.chain_run_id(research, "HAI_1", step) for step in ("canary_deploy", "canary", "promote", "release", "stable_deploy", "notice")),
        ]
        require([item["RUN_ID"] for item in found] == runs, f"the chain ran under {[item['RUN_ID'] for item in found]}, not {runs}")
        require([item.get("PHASE") for item in found] == ["research", None, "canary", "promote", None, None, "notify"], f"the chain ran {found}")
        release_task(environments[4])
        for run, job in zip(runs, ("promote", "serve", "promote", "promote", "features", "serve", "promote"), strict=True):
            end = rig.end(run, job)
            require(end is not None and end["outcome"] == "succeeded", f"{job} of {run} has no success record: {end}")
        require(json.loads(found[6]["TRAINING_PARAMETERS"])["outcome"] == "promoted", "the owner was not told promoted")
        started = [entry for entry in rig.stage.log if "online_hold" in entry]
        require(not started, f"the chain read a hold key: {started}")

    def closed_after_the_notice(rig: Any, facts: dict[str, Any]) -> None:
        """With a Production model whose release phase refuses (its container exits 1), the chain fails closed by name after the promotion: it launches
        research, the canary deploy and check, the promotion, the release and the notice, and no rollout."""
        first = rig.tasks_started() - facts["run_task_calls"]
        environments = [rig.environment(first + index) for index in range(6)]
        found = [item["serve"] if "serve" in item else item["feast"] if "feast" in item else item["promote"] for item in environments]
        require([item.get("PHASE") for item in found] == ["research", None, "canary", "promote", None, "notify"], f"the chain ran {found}")
        release_task(environments[4])
        end = rig.end(release_run, "features")
        require(end is not None and end["outcome"] == "failed", f"the refused release left {end}")
        notice = json.loads(found[5]["TRAINING_PARAMETERS"])
        require(notice["outcome"] == "promoted_not_rolled_out", f"the owner was told {notice['outcome']}")

    def research_started(rig: Any) -> None:
        found = triggered_confs("research_dag", research_trigger_run(trigger_run))
        require(found == [research_conf(trigger_base, trigger_types)], f"the training run started the research runs {found}")

    def no_launch(rig: Any) -> None:
        require(not rig.ecs.tasks and not rig.records.objects, f"a refused run started a task or wrote a launch record: {sorted(rig.records.objects)}")

    trigger_base, trigger_types = c.SERVE_BASE, list(c.TRAIN_PARAMS["hai_types"])
    trigger_run = training.trained_run_id(trigger_base, "m4")
    waiting = {"ready": "skipped", "research": "skipped", "chain": "skipped", "gate": DONE_STATE}

    def release_refused(starts: int, **changes: Any) -> Step:
        """A research run with a Production model whose release phase refuses: the chain fails closed by name (the run fails at the chain and the gate)."""
        states = {"ready": DONE_STATE, "research": DONE_STATE, "chain": "failed", "gate": "failed"}
        return Step("research_dag", research_params, starts, states=states, run_state="failed", reason="HAI_1 release:", **changes)

    research_cases = (
        Scenario(
            "research_dag_promotes_a_challenger_with_a_recorded_release",
            (
                bind_step,
                *(trained(model) for model in training.PROMOTION_MODELS),
                Step("research_dag", research_params, 7, check=chain_with_release_ran, prepare=stage_research),
                # A second run for the base joins the launches of the first and starts nothing again.
                Step("research_dag", research_params, 0, check=joined),
            ),
            make_rig,
        ),
        Scenario(
            "research_dag_promotes_a_first_champion_without_a_canary",
            (
                bind_step,
                *(trained(model) for model in training.PROMOTION_MODELS),
                Step("research_dag", research_params, 4, check=first_champion_ran, prepare=stage_first_champion),
            ),
            make_rig,
        ),
        Scenario(
            "research_dag_fails_after_the_notice_when_the_release_is_refused",
            (
                bind_step,
                *(trained(model) for model in training.PROMOTION_MODELS),
                release_refused(6, check=closed_after_the_notice, plan={"feast": 1}, prepare=stage_research),
            ),
            make_rig,
        ),
        Scenario(
            "training_dag_starts_the_research_for_its_base",
            (
                Step(
                    "training_dag",
                    {**c.TRAIN_PARAMS, "run_id": trigger_run, "model": "m4", "base_run": trigger_base},
                    1,
                    family("train"),
                    prepare=lambda rig: rig.stage_runs(trigger_run),
                    after=research_started,
                ),
            ),
            make_rig,
        ),
        Scenario(
            "research_dag_waits_until_the_three_models_have_trained", (Step("research_dag", research_params, 0, states=waiting),), make_rig, after=no_launch
        ),
        Scenario(
            "research_dag_refuses_the_local_lane_before_anything_runs",
            (
                Step(
                    "research_dag",
                    {**research_params, "mlflow": "local"},
                    0,
                    states={"ready": "failed", "research": "upstream_failed", "chain": "upstream_failed", "gate": "failed"},
                    run_state="failed",
                ),
            ),
            make_rig,
            after=no_launch,
        ),
    )

    refused: RefusedRun = {"run_state": "failed", "states": {"preflight": "failed", "job": "upstream_failed", "evidence": "upstream_failed", "gate": "failed"}}

    def serve_refused(name: str, form: Mapping[str, Any], reason: str, *staged: Callable[[Any], None]) -> Scenario:
        """A serve deploy whose closed request the launch preflight refuses: no task starts and no launch record is written."""

        def prepare(rig: Any) -> None:
            for item in staged:
                item(rig)

        step = Step("serving_deploy_dag", dict(form), 0, family("serve"), reason=reason, prepare=prepare, **refused)
        return Scenario(name, (step,), make_rig, after=no_launch)

    def stage_challenger(rig: Any) -> None:
        c.research_record_of(rig, challenger)

    def stage_held(rig: Any) -> None:
        stage_challenger(rig)
        rig.stage.objects.update(research_objects(c.SERVE_BASE, c.SERVE_RESEARCH, {"HAI_1": challenger}, "hold"))

    serve_cases = (
        Scenario(
            "serve_canary_deploys_the_research_challenger_with_its_expiry",
            (
                Step(
                    "serving_deploy_dag",
                    dict(c.SERVE_CANARY),
                    1,
                    family("serve"),
                    job_states(),
                    prepare=stage_challenger,
                    sent=serving_request_is(canary_request),
                ),
            ),
            make_rig,
        ),
        serve_refused("serve_canary_without_a_challenger_refuses_before_any_launch", c.SERVE_CANARY, "serving_candidate_refused"),
        serve_refused(
            "serve_canary_without_a_research_run_refuses_before_any_launch", {**c.SERVE_CANARY, "research_run": "latest"}, "names none", stage_challenger
        ),
        serve_refused("serve_canary_held_by_its_check_refuses_before_any_launch", c.SERVE_CANARY, "serving_canary_held", stage_held),
    )

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

    def launch_record_key(params: dict[str, Any]) -> bool:
        return str(params.get("Key", "")).startswith(f"{c.PREFIX}/")

    def code_binding_key(params: dict[str, Any]) -> bool:
        return str(params.get("Key", "")).endswith("/inputs/code_run.json")

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
            "planted_zero_weight_strategy_fails_closed",
            Plant("RunTask", lambda p: {**p, "capacityProviderStrategy": [{**item, "weight": 0} for item in p["capacityProviderStrategy"]]}),
            sent_refused,
            1,
            no_task,
            REFUSED_BY_ECS,
        ),
        planted_run(
            "planted_service_tag_propagation_fails_closed",
            Plant("RunTask", lambda p: {**p, "propagateTags": "SERVICE"}),
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
            "planted_wrong_bucket_for_the_code_run_binding_fails_closed",
            Plant("PutObject", lambda p: {**p, "Bucket": "nonexistent-review-bucket"}, code_binding_key),
            refused["states"],
            0,
            no_record,
            "was refused: NoSuchBucket",
        ),
        planted_run(
            "planted_wrong_bucket_for_the_launch_records_fails_closed",
            Plant("PutObject", lambda p: {**p, "Bucket": "nonexistent-review-bucket"}, launch_record_key),
            sent_refused,
            0,
            no_task,
            "launch record store refused the call: NoSuchBucket",
        ),
    )
    form = Scenario("trigger_forms_validate_in_airflow", (), make_rig, after=form_check)
    conformance = Scenario("bridge_refuses_what_aws_refuses", (), make_rig, after=bridge_conformance)
    timetables = Scenario("scheduled_ingestion_runs_have_the_run_id_of_their_job", (), make_rig, after=timetable_check)
    return {
        item.name: item for item in (pipeline, *first_dag_scenarios, *research_cases, *serve_cases, *refusals, *planted_cases, form, conformance, timetables)
    }


def timetable_check(_rig: Any) -> None:
    """Airflow's own timetable classes name the scheduled runs of the two ingestion schedules as their jobs, from November 2026 to April 2028 (day 1, the retry
    days, a year end and a leap February), and Airflow's DAG parser gives the two production DAG files their timetables (``schedule_timetable_cases``). The
    plugins folder is the checkout's (the ``airflow-check`` service sets it, because Airflow reads it once at import)."""
    from scripts.orchestration import schedule_timetable_cases

    failed = [f"{item['case']}: {item['evidence']}" for item in schedule_timetable_cases.run_cases() if not item["passed"]]
    require(not failed, "; ".join(failed))


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
    covered = finished_dags(results)
    inputs = inventory(ROOT, IMAGE_SOURCES)
    missing = f"no scenario is named {only}" if only is not None and only not in scenarios else None
    report: dict[str, Any] = {
        "dag_ids": sorted(bag.dag_ids),
        "at": datetime.now(UTC).isoformat(),
        "inputs": inputs,
        "versions": versions(),
        "substitutions": list(SUBSTITUTIONS),
        "import_errors": imports,
        "dags": covered,
        "ok": not imports and not missing and bool(results) and all(item["ok"] for item in results),
        "scenarios": results,
        "seconds": round(time.monotonic() - started, 1),
    }
    if missing:
        report["error"] = missing  # a section with an error is a refusal in every consumer
    return report


def finished_dags(scenarios: Sequence[Mapping[str, Any]]) -> list[str]:
    """The DAG ids with a DAG run that finished in a terminal state its scenario expected, in a scenario that passed. A step that raised, a run that is
    queued, running or skipped, and a run that ended in another state than its step expects are not counted."""
    return sorted(
        {
            str(step["dag_id"])
            for item in scenarios
            if item.get("ok") is True
            for step in item.get("steps", [])
            if step.get("run_state") in TERMINAL_STATES and step.get("run_state") == step.get("expected")
        }
    )


def inventory(root: Path, sources: Sequence[str] = SOURCES) -> dict[str, dict[str, str]]:
    """The SHA-256 of each listed source that exists under ``root`` and of every ``dags/*.py`` file. A source that is not there is absent from the map, so a
    removal or an addition shows when two inventories are compared."""
    return {
        "sources": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in sources if (root / name).is_file()},
        "dag_files": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted((root / "dags").glob("*.py"))},
    }


def step_problems(scenarios: Sequence[Mapping[str, Any]]) -> list[str]:
    """Every recorded step that did not end in ``success`` or ``failed``, or in another state than its scenario expected, whatever other step covers the same
    DAG. A scenario with no DAG step has no problem. DAG coverage is a separate check (``finished_dags``)."""
    problems = []
    for item in scenarios:
        steps = item.get("steps", [])
        if not isinstance(steps, list):
            problems.append(f"{item.get('name')}: the steps are not a list")
            continue
        for step in steps:
            if not isinstance(step, Mapping):
                problems.append(f"{item.get('name')}: a step is not a mapping")
            elif step.get("run_state") not in TERMINAL_STATES or step.get("run_state") != step.get("expected"):
                problems.append(f"{item.get('name')}: {step.get('dag_id')} ended {step.get('run_state')!r}, expected {step.get('expected')!r}")
    return problems


def moment(value: object) -> datetime | None:
    """The time zone aware time that ``value`` writes, or None."""
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None
    return parsed if parsed is not None and parsed.tzinfo is not None else None


def binding_problems(section: Mapping[str, Any], run: object) -> list[str]:
    """Why ``section`` is not the result of the attempt that ``run`` describes, or an empty list. ``run`` is what the host knows of its own attempt: the
    attempt id, the times it began and ended and the inventory of the inputs before and after it. The section must carry a run time from ``began`` to
    ``ended`` inclusive, with no allowance for a clock that differs, and the same inventory. The host's inventory must not have changed while the attempt
    ran."""
    if not isinstance(run, Mapping) or not isinstance(run.get("attempt_id"), str) or not run["attempt_id"]:
        return ["the result has no attempt identity: it is not bound to this execution"]
    began, ended = moment(run.get("started")), moment(run.get("ended"))
    if began is None or ended is None or ended < began:
        return ["the window of this attempt is missing or invalid"]
    problems = []
    at = moment(section.get("at"))
    if at is None:
        problems.append("the image run has no usable time zone aware run time")
    elif not began <= at <= ended:
        problems.append(f"the image run time {at.isoformat()} lies outside this attempt ({began.isoformat()} to {ended.isoformat()})")
    before, after = run.get("inputs"), run.get("inputs_after")
    if not isinstance(before, Mapping) or before != after:
        problems.append("the inputs changed while the attempt ran")
    elif section.get("inputs") != before:
        problems.append("the image ran other inputs than this checkout holds")
    return problems


def exit_code(report: Mapping[str, Any]) -> int:
    """0 only when the run passed and ran at least one scenario: a failed scenario, an unknown ``--case`` or an empty run exits 1."""
    return 0 if report.get("ok") is True and report.get("scenarios") and not report.get("error") else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", help="run one scenario by name")
    arguments = parser.parse_args()
    report = run_all(arguments.case)
    sys.stdout.write(REPORT_PREFIX + json.dumps(report, default=str) + "\n")
    sys.stdout.flush()
    return exit_code(report)


if __name__ == "__main__":
    sys.exit(main())
