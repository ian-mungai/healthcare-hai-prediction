"""Cases of the DAG steps on the direct ECS launch library: every shipped DAG step launches its job through ``orchestration/airflow_steps.py``.

Isolation is justified because the cloud has to fail on cue (a refused record, a throttled call, a staged file that is missing or denied) and time has to
move by minutes. Every case calls ``orchestration.dag_runtime.run_step`` or runs a shipped DAG file, over the real preflight, the real ``Launcher`` and
the real launch operator guarantees. The Airflow SDK and the provider operator are stand-ins, because Airflow is not installed here: rerun the cases in
the Airflow image after a provider or Airflow upgrade. The failure modes, each with the case that covers it:

1. A DAG file or the DAG runtime reaches a broker client, or a step reaches anything but the launch library (``dags_hold_no_broker_client``).
2. The pool applies only where the deployment names it, and every task that launches a job takes it (``pool_only_when_configured``).
3. Each gate refuses before any record or ECS call: real mode, the previous job's success, the training parameters, the
   promotion approval, the staged bronze and serving requests, the staged code digest (``gates_refuse_before_any_cloud_call`` and the request and digest
   cases). A staged input that is not ready yet is waited for within a bound. A ``LaunchUnavailable`` of the library is never repeated by the step
   (``unavailable_is_bounded``).
4. A chain step and the chain gate run without a signed record, and the gate binds the chain's code runs (``chain_runs_without_a_signed_record``).
   A run binds its code run once, an unreadable binding is not read as missing, and the drift run binds its training run (``code_run_is_bound_once``).
5. A control step launches nothing (``control_step_launches_nothing``). A job launches once, later steps of its DAG join it, a failure fails every step,
   and a retry launches nothing (``job_runs_once_and_steps_join``, ``failed_job_fails_every_step``).
6. The silver, gold, promotion and serving DAGs are one task per job: preflight, job, evidence and gate, and a refused preflight or a failed job fails
   the gate with nothing launched again (``collapsed_job_dags_are_one_chain``). The five DAGs of the chain run end to end (``five_dags_run_end_to_end``),
   and every DAG triggered with only its required values runs with its form's defaults: no form defaults to a mode the direct path refuses
   (``dags_run_with_their_form_defaults``). The feature store DAG's form requires the gold run its features job publishes from, and a run without it
   is refused before any launch (``feature_store_takes_its_gold_run``).
7. The modules, the mount list and the host table agree, and a container without the launch module names it (``launch_modules_are_in_the_mount_list``,
   ``host_table_matches_the_job_table``, ``missing_launch_module_is_named``).
8. The launch services come from the named environment, with the session adapter (``services_come_from_named_environment``,
   ``services_build_the_session_adapter``).
9. The metadata job sends overrides only for the one container of its host definition (``metadata_uses_the_host_containers``). The rig's fake ECS holds
   the containers that Terraform plans for the nine host definitions (``infra/jobs/tests/host_definitions.json``, which a Terraform test keeps equal to the
   plan) and the case ``host_definitions_hold_the_init_step_and_the_layer_guards`` checks them against what the job table and the owner decisions expect.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import inspect
import io
import json
import os
import re
import runpy
import sys
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from botocore.exceptions import ClientError

from orchestration.ecs_adapter import HOST, EcsAdapter, EcsSettings
from orchestration.ecs_launch import EcsFitProbe, Launcher, LaunchRefused, LaunchUnavailable, S3LaunchStore, launch_record_prefix
from orchestration.ecs_launch_operator import JobFailed
from orchestration.ecs_preflight import InputNotReady
from orchestration.interface import SubmissionRequest
from orchestration.training_contract import training_run_id
from scripts.orchestration.ecs_adapter_cases import require
from scripts.orchestration.ecs_broker_cases import (
    BRONZE_RUN,
    DIGEST,
    DRIFT_RUN,
    GOLD_RUN,
    INVENTORY,
    PROMOTE_RUN,
    SERVE_RUN,
    SERVING_REQUEST,
    SILVER_RUN,
    collections_of_table_map,
    table_pair,
)
from scripts.orchestration.ecs_broker_cases import TRAIN_PARAMS as BROKER_TRAIN_PARAMS
from scripts.orchestration.ecs_broker_fake import CLUSTER, FakeEcs, FakeLogs, FakeS3, client_error
from scripts.orchestration.ecs_chain_cases import Dag, Operator
from scripts.orchestration.ecs_launch_cases import refuses
from scripts.orchestration.ecs_launch_fake import START_TIME, FakeClock, FakeLaunchBucket
from scripts.orchestration.ecs_launch_operator_cases import StandInLaunchOperator

ROOT = Path(__file__).resolve().parents[2]
# Every DAG step launches on AWS, so the training parameters name the aws MLflow lane (lead decision, Nov 7 2026); the broker cases keep their own.
TRAIN_PARAMS = {**BROKER_TRAIN_PARAMS, "mlflow": "aws"}
TRAIN_LAUNCH = {key: TRAIN_PARAMS[key] for key in ("features_run", "population", "hai_types", "trigger", "mlflow")}
BUCKET = "example-private-bucket"
PREFIX = launch_record_prefix()
SCRATCH_FREE = 400 * 2**30
FEATURES_RUN = "20261104T100000Z-feed5eed"
OTHER_RUN = "20261104T110000Z-0f0f0f0f"
RUN_KEY = "lakehouse/run_stage/{run}/inputs/{name}"
LAUNCH_MOUNT = re.compile(r'=\s*"orchestration/([A-Za-z0-9_]+\.py)"')
DAG_ID = re.compile(r'dag_id="([a-z_]+)"')
MOUNT = re.compile(r'containerPath\s*=\s*"/opt/hai/orchestration/([A-Za-z0-9_]+\.py)"')
HOST_DEFINITIONS = ROOT / "infra/jobs/tests/host_definitions.json"
LAYER_JOBS = frozenset({"silver", "gold", "features"})
POLARIS = "http://10.0.0.5:8181/api/catalog"
MLFLOW = "http://10.0.0.5:5100"


class StageBucket(FakeS3):
    """The staged inputs of a run: reads that can be denied and write-once puts, as the launch role would see them."""

    def __init__(self) -> None:
        super().__init__()
        self.denied: set[str] = set()
        self.puts: list[str] = []

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        if arguments["Key"] in self.denied:
            raise client_error("AccessDenied", "GetObject")
        return super().get_object(**arguments)

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        self.puts.append(arguments["Key"])
        if arguments.get("IfNoneMatch") != "*":
            raise client_error("AccessDenied", "PutObject", "an unconditional write is not allowed")
        if arguments["Key"] in self.objects:
            raise client_error("PreconditionFailed", "PutObject")
        body = arguments["Body"]
        self.objects[arguments["Key"]] = body if isinstance(body, bytes) else str(body).encode()
        return {}

    def stage(self, run: str, name: str, body: object) -> None:
        self.objects[RUN_KEY.format(run=run, name=name)] = body if isinstance(body, bytes) else json.dumps(body).encode()


def host_planned_containers() -> dict[str, dict[str, list[str]]]:
    """The containers of each host family as Terraform plans them: ``containers`` in order and ``start_with_scratch_wipe``, the containers whose first command
    starts with the scratch wipe (``infra/jobs/tests/host_definitions.json``, written by ``host_definitions_fixture`` from the plan)."""
    return dict(json.loads(HOST_DEFINITIONS.read_text()))


def host_definitions() -> dict[str, frozenset[str]]:
    """The containers of each host task definition the rig's fake ECS knows, by the family name ECS sees."""
    return {f"example_dev_job_{family}_host": frozenset(planned["containers"]) for family, planned in host_planned_containers().items()}


class Rig:
    """The stand-in cloud, the staged inputs and the launch services of one case."""

    def __init__(self) -> None:
        preflight, steps = importlib.import_module("orchestration.ecs_preflight"), importlib.import_module("orchestration.airflow_steps")
        self.ecs, self.logs, self.stage = FakeEcs(), FakeLogs(), StageBucket()
        self.records = FakeLaunchBucket(BUCKET, f"{PREFIX}/")
        self.time = FakeClock()
        self.ecs.clock = lambda: self.time.now
        table, host = preflight.load_tables(ROOT)
        self.ecs.definitions = host_definitions()
        settings = EcsSettings(
            cluster=CLUSTER,
            family_prefix="example_dev_job_",
            subnets=(),
            networks={},
            project_tag="example",
            log_group="example_jobs_dev",
            families=frozenset(table["jobs"]),
            host_capacity_provider="stand-in_host",
        )
        self.adapter = EcsAdapter(self.ecs, self.logs, settings)
        store = S3LaunchStore(self.records, BUCKET, PREFIX)
        self.launcher = Launcher(self.adapter, store, EcsFitProbe(self.ecs, CLUSTER, lambda: SCRATCH_FREE), self.time.clock())
        self.preflight = preflight.Preflight(
            table=table,
            host=host,
            settings=preflight.PreflightSettings(polaris_uri=POLARIS, mlflow_uri=MLFLOW),
            code_digest=preflight.s3_digest_reader(self.stage, BUCKET),
            approvals=preflight.s3_object_reader(self.stage, BUCKET, "promotion_approval.json"),
            store=store,
            code_runs=preflight.S3CodeRuns(self.stage, BUCKET),
            clock=lambda: self.time.now,
            bronze_request=preflight.bronze_request_reader(self.stage, BUCKET, collections_of_table_map()),
            serving_request=preflight.serving_request_reader(self.stage, BUCKET),
        )
        self.services = steps.LaunchServices(
            preflight=self.preflight,
            launcher=self.launcher,
            operator_class=StandInLaunchOperator,
            cluster=CLUSTER,
            definition=lambda job: self.adapter.definition(job, HOST),
            clock=self.time.clock(),
        )
        self.stage_runs(BRONZE_RUN, SILVER_RUN, GOLD_RUN, FEATURES_RUN, PROMOTE_RUN, DRIFT_RUN, SERVE_RUN, str(TRAIN_PARAMS["run_id"]))

    def stage_runs(self, *runs: str) -> None:
        for run in runs:
            self.stage.objects[RUN_KEY.format(run=run, name="code.sha256")] = DIGEST.encode() + b"\n"

    def tasks_started(self) -> int:
        return self.ecs.count("run_task")

    def environment(self, index: int = 0) -> dict[str, dict[str, str]]:
        sent = [arguments for name, arguments in self.ecs.calls if name == "run_task"][index]
        return {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in sent["overrides"]["containerOverrides"]}

    def end(self, run: str, job: str) -> dict[str, Any] | None:
        claim = self.launcher.store.run_record(run, job, "claim")
        if claim is None:
            return None
        found = {record["event"]: record for record in self.launcher.store.records(run, job, [claim["attempt"]])}
        return found.get("end")


class FormRefused(Exception):
    """Airflow refused the trigger at the DAG's form: no DAG run and no task."""


class World:
    """The DAG runtime with the SDK stand-in, the rig's Variable and launch services, and the current task's context."""

    def __init__(self, rig: Rig, runtime: ModuleType, dag_id: str, params: dict[str, Any]) -> None:
        self.rig, self.runtime, self.dag_id, self.params = rig, runtime, dag_id, params
        self.context: dict[str, Any] = {}
        self.states: dict[str, str] = {}
        self.results: dict[str, Any] = {}
        self.built: list[Operator] = []

    def enter(self, task_id: str, attempt: int = 1) -> None:
        def get_task_states(dag_id: str, task_ids: list[str], run_ids: list[str]) -> dict[str, dict[str, str]]:
            return {run_ids[0]: {name: self.states[name] for name in task_ids if name in self.states}}

        def xcom_pull(task_ids: str) -> Any:
            return self.results.get(task_ids)

        instance = SimpleNamespace(
            try_number=attempt,
            dag_id=self.dag_id,
            task_id=task_id,
            run_id="manual__test",
            map_index=-1,
            get_task_states=get_task_states,
            xcom_pull=xcom_pull,
        )
        self.context = {"params": self.params, "ti": instance}

    def step(self, template: str, attempt: int = 1) -> str:
        self.enter(template, attempt)
        return str(self.runtime.run_step(template))

    def run_dag(self, dag_file: str) -> dict[str, str]:
        """Run the tasks of a shipped DAG file in order, as the scheduler would: a task runs when its upstream tasks passed, or always for ``all_done``."""
        built = self.built
        built.clear()

        def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
            def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
                def call() -> SimpleNamespace:
                    node = Operator(**settings)
                    node.function = function
                    built.append(node)
                    return SimpleNamespace(operator=node)

                return call

            return decorate

        forms: list[dict[str, Any]] = []

        class FormDag(Dag):
            def __init__(self, **settings: Any) -> None:
                super().__init__(**settings)
                forms.append(settings.get("params", {}))

        with (
            mock.patch.object(sys.modules["airflow.sdk"], "task", task),
            mock.patch.object(sys.modules["airflow.sdk"], "DAG", FormDag),
            mock.patch.object(self.runtime, "task", task),
        ):
            runpy.run_path(str(ROOT / "dags" / dag_file))
        for form in forms:  # Airflow fills a parameter the trigger leaves out with the form's default, and refuses the trigger before any task runs
            for name, (arguments, settings) in form.items():  # when a required parameter is missing or a value breaks its pattern.
                if arguments and name not in self.params:
                    self.params[name] = arguments[0]
                if name not in self.params:
                    raise FormRefused(f"{name} is required")
                pattern = settings.get("pattern")
                if pattern and isinstance(self.params[name], str) and re.search(pattern, self.params[name]) is None:
                    raise FormRefused(f"{name}={self.params[name]!r} does not match {pattern}")
        for node in built:
            self.enter(node.task_id)
            upstream_ok = all(self.states.get(before.task_id) == "success" for before in node.upstream)
            if not upstream_ok and node.settings.get("trigger_rule") != "all_done":
                self.states[node.task_id] = "upstream_failed"
                continue
            try:
                self.results[node.task_id] = node.function() if node.function else None
                self.states[node.task_id] = "success"
            except Exception as error:
                self.results[node.task_id] = f"{type(error).__name__}: {error}"
                self.states[node.task_id] = "failed"
        return dict(self.states)


@contextmanager
def world(rig: Rig, dag_id: str, params: dict[str, Any]) -> Iterator[World]:
    """``run_step`` of the shipped DAG runtime over the SDK stand-in, with the rig's launch services."""
    sdk = ModuleType("airflow.sdk")
    for name, value in (
        ("Asset", str),
        ("TriggerRule", SimpleNamespace(ALL_SUCCESS="all_success", ALL_DONE="all_done")),
        ("DAG", Dag),
        ("Param", lambda *a, **k: (a, k)),
        ("task", lambda **_: lambda f: f),
        ("get_current_context", dict),
    ):
        setattr(sdk, name, value)
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    modules = {"airflow": ModuleType("airflow"), "airflow.sdk": sdk}
    try:
        with mock.patch.dict(sys.modules, modules):
            runtime = importlib.import_module("orchestration.dag_runtime")
            steps = importlib.import_module("orchestration.airflow_steps")
            current = World(rig, runtime, dag_id, params)
            with (
                mock.patch.object(runtime, "get_current_context", lambda: current.context),
                mock.patch.object(steps, "services", lambda: rig.services),
            ):
                yield current
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous


SILVER = {"run_id": SILVER_RUN, "mode": "real"}
GOLD = {"run_id": GOLD_RUN, "mode": "real", "silver_snapshot_run": SILVER_RUN}


def silver_passed(rig: Rig) -> None:
    with world(rig, "silver_transform_dag", SILVER) as current:
        require(current.step("pin_bronze") == "succeeded", "the silver job did not pass")


def pool_only_when_configured() -> None:
    rig = Rig()
    with world(rig, "silver_transform_dag", SILVER) as current:
        runtime = current.runtime
        with mock.patch.dict("os.environ", {}, clear=True):
            require(runtime.launch_pool() == "default_pool", "an unconfigured deployment got a pool that may not exist")
        with mock.patch.dict("os.environ", {"HAI_LAUNCH_POOL": "host_jobs"}, clear=True):
            require(runtime.launch_pool() == "host_jobs", "the configured pool was not used")
        for bad in ("", "host jobs", "x" * 300):
            with mock.patch.dict("os.environ", {"HAI_LAUNCH_POOL": bad}, clear=True):
                refuses(ValueError, runtime.launch_pool)
        built: list[Operator] = []

        def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
            def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
                def call() -> SimpleNamespace:
                    node = Operator(**settings)
                    built.append(node)
                    return SimpleNamespace(operator=node)

                return call

            return decorate

        with (
            mock.patch.dict("os.environ", {"HAI_LAUNCH_POOL": "host_jobs"}, clear=True),
            mock.patch.object(sys.modules["airflow.sdk"], "task", task),
            mock.patch.object(runtime, "task", task),
        ):
            runpy.run_path(str(ROOT / "dags/features_transform_dag.py"))
        pools = {node.task_id: node.settings.get("pool") for node in built}
        require(pools["features_build"] == "host_jobs" and pools["catalog_release"] == "host_jobs", f"the work steps are not in the pool: {pools}")
        require(pools["final_gate"] is None and pools["release_run"] is None, f"the gate and release take a pool slot: {pools}")
        for dag_file in COLLAPSED_FILES:
            built.clear()
            with (
                mock.patch.dict("os.environ", {"HAI_LAUNCH_POOL": "host_jobs"}, clear=True),
                mock.patch.object(sys.modules["airflow.sdk"], "task", task),
                mock.patch.object(runtime, "task", task),
            ):
                runpy.run_path(str(ROOT / "dags" / dag_file))
            pools = {node.task_id: node.settings.get("pool") for node in built}
            require(pools == {"preflight": None, "job": "host_jobs", "evidence": None, "gate": None}, f"{dag_file}: only the job task takes the pool: {pools}")
        trigger = ModuleType("airflow.providers.standard.operators.trigger_dagrun")
        trigger.__dict__["TriggerDagRunOperator"] = SimpleNamespace(partial=lambda **_: SimpleNamespace(expand_kwargs=lambda *_: None))
        providers = {name: ModuleType(name) for name in ("airflow.providers", "airflow.providers.standard", "airflow.providers.standard.operators")}
        for dag_file, launch_task in (("drift_monitoring_dag.py", "drift_run"), ("metadata_ingest_dag.py", "metadata_run")):
            built.clear()
            with (
                mock.patch.dict("os.environ", {"HAI_LAUNCH_POOL": "host_jobs"}, clear=True),
                mock.patch.dict(sys.modules, {**providers, trigger.__name__: trigger}),
                mock.patch.object(sys.modules["airflow.sdk"], "task", task),
                mock.patch.object(runtime, "task", task),
            ):
                runpy.run_path(str(ROOT / "dags" / dag_file))
            pools = {node.task_id: node.settings.get("pool") for node in built}
            require(pools.get(launch_task) == "host_jobs", f"{launch_task} of {dag_file} launches a job outside the pool: {pools}")
    text = (ROOT / "infra/services/airflow.tf").read_text()
    require(
        re.search(r"airflow pools set \$\{local\.airflow_launch_pool\} 1 ", text) is not None
        and re.search(r"HAI_LAUNCH_POOL\s*=\s*local\.airflow_launch_pool", text) is not None,
        "the deployment neither creates the one-slot pool nor names it to the tasks",
    )


def gates_refuse_before_any_cloud_call() -> None:
    rig = Rig()
    with world(rig, "silver_transform_dag", {**SILVER, "mode": "publish_fixture"}) as current:
        error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
        require("real mode" in str(error) and not rig.ecs.calls, f"a job step outside real mode was not refused: {error}")
    rig = Rig()
    with world(rig, "silver_transform_dag", SILVER) as current:
        error = refuses(LaunchRefused, lambda: current.step("ghost_step"))
        require("no ECS job" in str(error) and not rig.ecs.calls, f"an unknown step was not refused: {error}")


def chain_runs_without_a_signed_record() -> None:
    """Signing the first publishes is the owner's manual step (owner decision, Nov 10 2026), not a code gate: the direct path reads no signed record and
    takes no setting for one, a chain step and the chain gate run without it, and the gate still binds the chain's code runs."""
    preflight, steps = importlib.import_module("orchestration.ecs_preflight"), importlib.import_module("orchestration.airflow_steps")
    require("signed" not in inspect.signature(preflight.Preflight).parameters, "the preflight still takes a signed record reader")
    require(not hasattr(preflight.Preflight, "require_signed"), "the preflight still has the signed gate")
    require(not any("SIGNED" in name for name in steps.SETTINGS), f"the launch settings still name a signed record: {steps.SETTINGS}")
    require("ssm_signed_reader" not in (ROOT / "orchestration/airflow_steps.py").read_text(), "the launch services still build a signed record reader")
    rig = Rig()
    with world(rig, "silver_transform_dag", SILVER) as current:
        require(current.step("pin_bronze") == "succeeded", "a chain step did not run without a signed record")
    require(rig.tasks_started() == 1, "the chain step did not launch its job")
    rig = Rig()
    silver = preflight.derived_run_id(BRONZE_RUN, "silver")
    with world(rig, "bronze_chain_dag", {"run_id": BRONZE_RUN, "mode": "real"}) as current:
        require(current.step("chain_gate") == "succeeded", "the chain gate did not run without a signed record")
    require(rig.preflight.code_runs.stored(silver) == BRONZE_RUN, "the chain gate did not bind the silver run to the bronze run's code")
    require(not rig.ecs.calls, "the chain gate launched a task")


def after_needs_the_previous_job_to_pass() -> None:
    rig = Rig()
    with world(rig, "gold_transform_dag", GOLD) as current:
        error = refuses(LaunchRefused, lambda: current.step("pin_silver"))
        require("silver" in str(error) and not rig.ecs.calls, f"gold ran before silver: {error}")
    failing = Rig()
    failing.ecs.plan = {"validate": 1}
    with world(failing, "silver_transform_dag", SILVER) as current:
        refuses(JobFailed, lambda: current.step("pin_bronze"))
    with world(failing, "gold_transform_dag", GOLD) as current:
        refuses(LaunchRefused, lambda: current.step("pin_silver"))
    require(failing.tasks_started() == 1, "gold launched after a failed silver")
    latest = Rig()
    silver_passed(latest)
    with world(latest, "gold_transform_dag", {**GOLD, "silver_snapshot_run": "latest"}) as current:
        error = refuses(LaunchRefused, lambda: current.step("pin_silver"))
        require("silver run it publishes from" in str(error), f"a gold run with no input run was not refused: {error}")
    passed = Rig()
    silver_passed(passed)
    with world(passed, "gold_transform_dag", GOLD) as current:
        require(current.step("pin_silver") == "succeeded", "gold did not run after a passed silver")
        require(passed.environment(1)["pin"]["INPUT_RUN_ID"] == SILVER_RUN, "the gold pin container lacks the input run")
        current.step("gold_build")
        require(passed.tasks_started() == 2, "a later gold step launched again")


def training_parameters_are_checked() -> None:
    rig = Rig()
    bad = {**TRAIN_PARAMS, "hai_types": ["HAI_9"]}
    with world(rig, "training_dag", bad) as current:
        refuses(Exception, lambda: current.step("guard"))
    require(not rig.ecs.calls, "bad training parameters reached the cloud")
    with world(rig, "training_dag", TRAIN_PARAMS) as current:
        require(current.step("guard") == "succeeded", "the training job did not pass")
    values = rig.environment()
    require(json.loads(values["prepare"]["TRAINING_PARAMETERS"])["mlflow"] == "aws", "the training lane is not the AWS one")


def aws_mlflow_lane_reaches_the_jobs() -> None:
    """Every DAG step runs on AWS through the launch library, so the training and promotion DAGs accept mlflow=aws (review, Nov 7 2026)."""
    rig = Rig()
    with world(rig, "training_dag", {**TRAIN_PARAMS, "mlflow": "aws"}) as current:
        require(current.step("guard") == "succeeded", "a training run with mlflow=aws did not launch")
    require(json.loads(rig.environment()["prepare"]["TRAINING_PARAMETERS"])["mlflow"] == "aws", "the training lane is not the AWS one")
    rig.stage.stage(
        PROMOTE_RUN,
        "promotion_approval.json",
        {"run_id": PROMOTE_RUN, "dag_id": "model_promotion_dag", "scope": "promote", "approved_on": "2026-10-30", "hai_type": "HAI_1", "model_version": "3"},
    )
    params = {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "aws"}
    with world(rig, "model_promotion_dag", params) as current:
        require(current.step("promote") == "succeeded", "a promotion with mlflow=aws did not launch")
    with world(rig, "training_dag", {**TRAIN_PARAMS, "mlflow": "other"}) as current:
        refuses(Exception, lambda: current.step("guard"))


def local_mlflow_is_refused_visibly() -> None:
    """Every DAG step launches its job on AWS, where the MLflow lane is aws. A run that names mlflow=local is refused by name before anything is read or
    launched, instead of being turned into aws without a word (lead decision, Nov 7 2026). The shipped DAGs and the chain default to aws."""
    rig = Rig()
    with world(rig, "training_dag", {**TRAIN_PARAMS, "mlflow": "local"}) as current:
        error = refuses(ValueError, lambda: current.step("guard"))
    require("mlflow=local" in str(error) and "mlflow=aws" in str(error), f"the refusal does not name the lane and its fix: {error}")
    params = {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "local"}
    with world(rig, "model_promotion_dag", params) as current:
        refuses(ValueError, lambda: current.step("promote"))
    require(not rig.ecs.calls and not rig.records.calls, "a run that named mlflow=local reached the cloud or the launch records")
    preflight = rig.preflight
    request = SubmissionRequest(
        "guard", str(TRAIN_PARAMS["run_id"]), "real", 1, "training_dag", "guard", "manual__test", None, training={**TRAIN_LAUNCH, "mlflow": "local"}
    )
    refuses(LaunchRefused, lambda: preflight.prepare(request))
    for name in ("training_dag.py", "model_promotion_dag.py", "drift_monitoring_dag.py"):
        text = (ROOT / "dags" / name).read_text()
        require('"mlflow": Param("aws"' in text, f"{name} does not default to the aws lane")
    require('conf["mlflow"] = "aws"' in (ROOT / "dags/bronze_chain_dag.py").read_text(), "the chain starts drift with another MLflow lane than aws")


def owner_settle_command_reaches_the_launcher() -> None:
    """The owner's settle path is one command in the scheduler container, which runs as the launch role (round 2 review, M7):
    ``python -m orchestration.airflow_steps settle --run <run> --job <job> --statement <what was checked>``. It settles through the launch library and
    prints the recorded outcome; a short statement is refused before anything is written."""
    rig = Rig()
    steps = importlib.import_module("orchestration.airflow_steps")
    claim = {
        "schema_version": 1,
        "event": "claim",
        "run_id": SILVER_RUN,
        "job": "silver",
        "attempt": 1,
        "at": "2026-11-04T10:00:00Z",
        "bound": {"code_digest": DIGEST, "request_hash": "0" * 64, "input_run": "latest"},
        "deadline_seconds": 21600,
        "acceptance": [],
        "need": {"cpu_units": 3072, "memory_mib": 16384, "scratch_bytes": 1},
    }
    rig.launcher.store.put_once(claim)
    output = io.StringIO()
    with mock.patch.object(steps, "services", lambda: rig.services), contextlib.redirect_stdout(output):
        refuses(LaunchRefused, lambda: steps.main(["settle", "--run", SILVER_RUN, "--job", "silver", "--statement", "short"]))
        require(rig.end(SILVER_RUN, "silver") is None, "a short statement settled the launch")
        code = steps.main(["settle", "--run", SILVER_RUN, "--job", "silver", "--statement", "ECS console and CloudTrail show no task for this launch"])
    end = rig.end(SILVER_RUN, "silver")
    require(code == 0 and end is not None and end["outcome"] in output.getvalue(), f"the settle command did not settle and report: {output.getvalue()} {end}")


def promotion_needs_the_approval() -> None:
    rig = Rig()
    params = {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "aws"}
    with world(rig, "model_promotion_dag", params) as current:
        error = refuses(LaunchRefused, lambda: current.step("promote"))
        require("approval" in str(error) and not rig.ecs.calls, f"a promotion ran without approval: {error}")
        body = {
            "run_id": PROMOTE_RUN,
            "dag_id": "model_promotion_dag",
            "scope": "promote",
            "approved_on": "2026-10-30",
            "hai_type": "HAI_1",
            "model_version": "3",
        }
        for wrong in ({"run_id": SILVER_RUN}, {"model_version": "4"}, {"approved_on": "2999-01-01"}, {"extra": 1}):
            rig.stage.stage(PROMOTE_RUN, "promotion_approval.json", {**body, **wrong})
            refuses(LaunchRefused, lambda: current.step("promote"))
        rig.stage.stage(PROMOTE_RUN, "promotion_approval.json", body)
        require(current.step("promote") == "succeeded", "an approved promotion did not run")


def bronze_request_is_checked_and_bound() -> None:
    pair, other = table_pair()
    good = {"tables": pair, "target": "real", "dry_run": False, "inventory_sha256": INVENTORY}
    params = {"run_id": BRONZE_RUN, "mode": "real"}
    rig = Rig()
    with world(rig, "ingestion_dag", params) as current:
        raised = refuses(LaunchUnavailable, lambda: current.step("bronze_load"))
        require("not_staged" in str(raised) and rig.time.now - 1_800_000_000.0 <= 700, f"a missing request was not retried within a bound: {raised}")
        for bad in (b"not json", {**good, "extra": 1}, {**good, "tables": [other, "ghost"]}, {**good, "dry_run": "no"}, {**good, "target": "other"}):
            rig.stage.stage(BRONZE_RUN, "bronze_request.json", bad)
            refuses(LaunchRefused, lambda: current.step("bronze_load"))
        require(not rig.ecs.calls, "a bad bronze request reached the cloud")
        rig.stage.stage(BRONZE_RUN, "bronze_request.json", good)
        require(current.step("bronze_load") == "succeeded", "a good bronze request did not run")
        values = rig.environment()
        require(
            values["load"]["BRONZE_TARGET"] == "real" and len(values["load"]["BRONZE_REQUEST_SHA256"]) == 64, f"the checked values are wrong: {values['load']}"
        )
        rig.stage.objects[RUN_KEY.format(run=BRONZE_RUN, name="bronze_request.json")] = json.dumps({**good, "inventory_sha256": "ef" * 32}).encode()
        error = refuses(LaunchRefused, lambda: current.step("bronze_load", 2))
        require("bound" in str(error) and rig.tasks_started() == 1, f"a changed request on a retry was not refused: {error}")


def serving_request_is_checked() -> None:
    rig = Rig()
    params = {"run_id": SERVE_RUN, "mode": "real"}
    with world(rig, "serving_deploy_dag", params) as current:
        refuses(LaunchUnavailable, lambda: current.step("serve_deploy"))
        for bad in (b"[]", {**SERVING_REQUEST, "percent": 90}, {**SERVING_REQUEST, "extra": 1}):
            rig.stage.stage(SERVE_RUN, "serving_request.json", bad)
            refuses(LaunchRefused, lambda: current.step("serve_deploy"))
        rig.stage.stage(SERVE_RUN, "serving_request.json", SERVING_REQUEST)
        require(current.step("serve_deploy") == "succeeded", "a good serving request did not run")
        require(json.loads(rig.environment()["serve"]["SERVING_REQUEST"]) == SERVING_REQUEST, "the checked request did not reach the container")


def code_digest_is_checked() -> None:
    rig = Rig()
    key = RUN_KEY.format(run=SILVER_RUN, name="code.sha256")
    del rig.stage.objects[key]
    with world(rig, "silver_transform_dag", SILVER) as current:
        error = refuses(LaunchUnavailable, lambda: current.step("pin_bronze"))
        require("code_digest_not_staged" in str(error), f"a missing digest was not named: {error}")
        rig.stage.objects[key] = b"not a digest\n"
        refuses(LaunchRefused, lambda: current.step("pin_bronze"))
        rig.stage.objects[key] = DIGEST.encode()
        rig.stage.denied.add(key)
        error = refuses(LaunchUnavailable, lambda: current.step("pin_bronze"))
        require("unreadable" in str(error), f"a denied digest read was taken for a missing one: {error}")
    require(not rig.ecs.calls, "a bad digest reached the cloud")


def code_run_is_bound_once() -> None:
    rig = Rig()
    layers = importlib.import_module("orchestration.ecs_preflight").derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    params = {"run_id": BRONZE_RUN, "mode": "real"}
    with world(rig, "bronze_chain_dag", params) as current:
        require(current.step("chain_gate") == "succeeded" and not rig.ecs.calls, "the gate failed or reached ECS")
        require(current.step("chain_gate") == "succeeded", "a repeated gate failed")
    bound = json.loads(rig.stage.objects[RUN_KEY.format(run=silver, name="code_run.json")])
    require(bound == {"code_run": BRONZE_RUN}, f"the layer run is not bound to the bronze run: {bound}")
    with world(rig, "silver_transform_dag", {"run_id": silver, "mode": "real"}) as current:
        current.step("pin_bronze")
        require(rig.environment()["fetch"]["CODE_RUN_ID"] == BRONZE_RUN, "the layer run fetches its own code, not its bronze run's")
    other = layers("20261031T090000Z-b1b1b1b1", "silver")
    require(other != silver, "two bronze runs share a layer run")
    with world(rig, "bronze_chain_dag", params) as current:
        rig.stage.objects[RUN_KEY.format(run=silver, name="code_run.json")] = json.dumps({"code_run": "20261031T090000Z-b1b1b1b1"}).encode()
        error = refuses(LaunchRefused, lambda: current.step("chain_gate"))
        require("another run" in str(error), f"a rebind to another code run was not refused: {error}")
        rig.stage.denied.add(RUN_KEY.format(run=silver, name="code_run.json"))
        refuses(LaunchUnavailable, lambda: current.step("chain_gate"))
    fresh = Rig()
    with world(fresh, "silver_transform_dag", SILVER) as current:
        fresh.stage.denied.add(RUN_KEY.format(run=SILVER_RUN, name="code_run.json"))
        error = refuses(LaunchUnavailable, lambda: current.step("pin_bronze"))
        require("binding" in str(error) and not fresh.ecs.calls, f"a denied binding read was taken for no binding: {error}")
    drift = Rig()
    drift.stage_runs(BRONZE_RUN)
    with world(drift, "bronze_chain_dag", params) as current:
        current.step("chain_gate")
    drift_run = layers(layers(layers(layers(BRONZE_RUN, "silver"), "gold"), "features"), "drift")
    with world(drift, "drift_monitoring_dag", {"run_id": drift_run, "mode": "real", "mlflow": "aws"}) as current:
        current.step("drift_run")
    training = json.loads(drift.stage.objects[RUN_KEY.format(run=training_run_id(drift_run), name="code_run.json")])
    require(training == {"code_run": BRONZE_RUN}, f"the training run is not bound to the drift run's code: {training}")


def control_step_launches_nothing() -> None:
    rig = Rig()
    with world(rig, "silver_transform_dag", SILVER) as current:
        for control in ("acquire_run", "catalog_up", "seal_silver", "silver_final_gate_accepted", "release_accepted"):
            require(current.step(control) == "succeeded", f"{control} failed")
    require(not rig.ecs.calls and not rig.records.calls, "a control step reached ECS or the launch records")


def job_runs_once_and_steps_join() -> None:
    rig = Rig()
    with world(rig, "silver_transform_dag", SILVER) as current:
        require(current.step("pin_bronze") == "succeeded", "the first silver step did not pass")
        sent = [arguments for name, arguments in rig.ecs.calls if name == "run_task"][0]
        require(sent["taskDefinition"] == "example_dev_job_silver_host" and "networkConfiguration" not in sent, "silver did not run as a host task")
        require(sent["capacityProviderStrategy"] == [{"capacityProvider": "stand-in_host", "weight": 1}], "the capacity provider is wrong")
        values = rig.environment()
        require(values["fetch"] == {"RUN_ID": SILVER_RUN, "CODE_RUN_ID": SILVER_RUN, "CODE_SHA256": DIGEST}, f"fetch variables: {values['fetch']}")
        require(values["pin"]["POLARIS_URI"] == "http://10.0.0.5:8181/api/catalog", f"pin variables: {values['pin']}")
        for step in ("silver_build", "silver_commit", "record_silver_snapshot", "pin_bronze"):
            require(current.step(step) == "succeeded", f"{step} did not pass")
    require(rig.tasks_started() == 1, "later steps started more tasks")
    end = rig.end(SILVER_RUN, "silver")
    require(end is not None and end["outcome"] == "succeeded", f"the launch has no success record: {end}")


def failed_job_fails_every_step() -> None:
    rig = Rig()
    rig.ecs.plan = {"validate": 3}
    with world(rig, "silver_transform_dag", SILVER) as current:
        error = refuses(JobFailed, lambda: current.step("pin_bronze"))
        require("3" in str(error), f"the exit code is not named: {error}")
        error = refuses(JobFailed, lambda: current.step("silver_build"))
        require("failed" in str(error) and "3" in str(error), f"a later step did not report the failed job: {error}")
        error = refuses(JobFailed, lambda: current.step("pin_bronze", 2))
        require("earlier attempt" in str(error) and "new run" in str(error), f"a retry did not say that a new run is needed: {error}")
    require(rig.tasks_started() == 1, "a retry launched beside a failed job")


def unavailable_is_bounded() -> None:
    """A staged input that is not ready is waited for within a bound. A launch the library cannot finish is not repeated by the step."""
    rig = Rig()
    rig.ecs.faults["run_task"] = [client_error("ThrottlingException", "RunTask")]
    with world(rig, "silver_transform_dag", SILVER) as current:
        require(current.step("pin_bronze") == "succeeded" and rig.tasks_started() == 2, "a throttled start was not retried by the library to success")
    rig = Rig()
    started = rig.time.now

    def never_ends(now: float) -> None:
        if now - started > 7200:
            raise AssertionError("the wait for a staged input never ended")

    rig.time.on_sleep = never_ends
    del rig.stage.objects[RUN_KEY.format(run=SILVER_RUN, name="code.sha256")]
    with world(rig, "silver_transform_dag", SILVER) as current:
        error = refuses(InputNotReady, lambda: current.step("pin_bronze"))
    require(0 < rig.time.now - started <= 700, f"the wait for the input did not end within its bound: {rig.time.now - started} s")
    require("still not ready" in str(error) and not rig.ecs.calls and not rig.records.calls, f"the cause is not named, or the cloud was called: {error}")
    rig = Rig()
    rig.records.faults["put_object"] = [client_error("SlowDown", "PutObject")] * 500
    with world(rig, "silver_transform_dag", SILVER) as current:
        error = refuses(LaunchUnavailable, lambda: current.step("pin_bronze"))
    require(not isinstance(error, InputNotReady) and "SlowDown" in str(error), f"the library's unavailable signal was changed or lost: {error}")
    require(
        rig.time.now == START_TIME and rig.records.count("put_object") == 1 and rig.ecs.count("run_task") == 0,
        "the step repeated a launch the library could not finish (the family gate's ECS listing comes before the claim; no RunTask may follow)",
    )


def five_dags_run_end_to_end() -> None:
    rig = Rig()
    layers = importlib.import_module("orchestration.ecs_preflight").derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    gold = layers(silver, "gold")
    features = layers(gold, "features")
    drift = layers(features, "drift")
    training = training_run_id(drift)
    with world(rig, "bronze_chain_dag", {"run_id": BRONZE_RUN, "mode": "real"}) as current:
        require(current.step("chain_gate") == "succeeded", "the chain gate failed")
    plan = (
        ("silver_transform_dag.py", "silver_transform_dag", {"run_id": silver, "mode": "real"}, "silver", silver),
        ("gold_transform_dag.py", "gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}, "gold", gold),
        ("features_transform_dag.py", "features_transform_dag", {"run_id": features, "mode": "real", "gold_snapshot_run": gold}, "features", features),
    )
    for file, dag_id, params, job, run in plan:
        with world(rig, dag_id, params) as current:
            states = current.run_dag(file)
            failed = {name: state for name, state in states.items() if state != "success"}
            last = ("gate", "accepted") if f"{dag_id}.py" in COLLAPSED_FILES else ("release_run", "released")
            require(not failed and current.results[last[0]] == last[1], f"{dag_id} did not finish: {failed} {current.results}")
        end = rig.end(run, job)
        require(end is not None and end["outcome"] == "succeeded", f"{job} has no success record")
    with world(rig, "drift_monitoring_dag", {"run_id": drift, "mode": "real", "mlflow": "aws"}) as current:
        require(current.step("drift_run") == "succeeded", "the drift job did not pass")
    with world(rig, "training_dag", {**TRAIN_PARAMS, "run_id": training}) as current:
        states = current.run_dag("training_dag.py")
        failed = {name: state for name, state in states.items() if state != "success"}
        require(not failed and current.results["release_run"] == "released", f"training_dag did not finish: {failed} {current.results}")
    require(
        rig.tasks_started() == 5,
        f"one task per job was expected: {rig.tasks_started()} tasks",
    )
    require(
        all((rig.end(run, job) or {}).get("outcome") == "succeeded" for run, job in ((drift, "drift"), (training, "train"))),
        "drift or training has no success record",
    )
    again = rig.tasks_started()
    with world(rig, "gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}) as current:
        current.run_dag("gold_transform_dag.py")
    require(rig.tasks_started() == again, "a second run of the same DAG run launched again")


MODE_REFUSAL = "runs on AWS in real mode only"


def form_mode_defaults() -> dict[str, Any]:
    """The default of the ``mode`` parameter in each shipped DAG file's trigger form, read from the file's ``Param`` call."""
    found: dict[str, Any] = {}
    for path in sorted((ROOT / "dags").glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values, strict=True):
                    if isinstance(key, ast.Constant) and key.value == "mode" and isinstance(value, ast.Call) and value.args:
                        found[path.stem] = ast.literal_eval(value.args[0])
    return found


PRODUCER_RUNS = (
    ("gold_transform_dag.py", "silver_snapshot_run"),
    ("features_transform_dag.py", "gold_snapshot_run"),
    ("feature_store_dag.py", "gold_snapshot_run"),
)


def dags_run_with_their_form_defaults() -> None:
    """Each DAG triggered with only its required inputs runs to success on the direct path (failure mode 3): no form default is a mode the preflight
    refuses, and a DAG whose job publishes from a producer run requires that run as a run ID in its form, so a trigger without it is refused at the form
    before any launch instead of failing its preflight."""
    defaults = form_mode_defaults()
    accepted = {"model_promotion_dag": "promote"}  # the promotion form's mode is the operation; the step maps it to real
    wrong = {dag: mode for dag, mode in defaults.items() if mode != accepted.get(dag, "real")}
    require(len(defaults) >= 10 and not wrong, f"DAG forms default to a mode the direct path refuses: {wrong}")
    for dag_file, name in PRODUCER_RUNS:
        declared = form_parameter(dag_file, name)
        arguments, settings = declared or ([], {})
        pattern = str(settings.get("pattern", ""))
        require(declared is not None and not arguments, f"{dag_file} does not require {name}: {declared}")
        require(
            bool(pattern) and re.search(pattern, GOLD_RUN) is not None and not any(re.search(pattern, bad) for bad in ("latest", "", "x" + GOLD_RUN)),
            f"{dag_file}'s {name} pattern does not accept exactly a run ID: {pattern}",
        )
    rig = Rig()
    layers = importlib.import_module("orchestration.ecs_preflight").derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    gold = layers(silver, "gold")
    features = layers(gold, "features")
    training = str(TRAIN_PARAMS["run_id"])
    with world(rig, "bronze_chain_dag", {"run_id": BRONZE_RUN, "mode": "real"}) as current:
        require(current.step("chain_gate") == "succeeded", "the chain gate failed")
    plan = (
        ("silver_transform_dag", {"run_id": silver}, "silver", silver),
        ("gold_transform_dag", {"run_id": gold, "silver_snapshot_run": silver}, "gold", gold),
        ("features_transform_dag", {"run_id": features, "gold_snapshot_run": gold}, "features", features),
        ("feature_store_dag", {"run_id": STORE_RUN, "gold_snapshot_run": gold}, "features", STORE_RUN),
        ("training_dag", {"run_id": training}, "train", training),
    )
    for dag_id, params, job, run in plan:
        rig.stage_runs(run)
        before = rig.tasks_started()
        with world(rig, dag_id, dict(params)) as current:
            states = current.run_dag(f"{dag_id}.py")
            failed = {name: current.results[name] for name, state in states.items() if state != "success"}
            require(bool(states) and not failed, f"{dag_id} with only its required inputs did not finish: {failed}")
        require(rig.tasks_started() == before + 1, f"{dag_id} started {rig.tasks_started() - before} tasks, not one")
        require((rig.end(run, job) or {}).get("outcome") == "succeeded", f"{dag_id}: {job} has no success record")
    for dag_file, name in PRODUCER_RUNS:
        dag_id = dag_file.removesuffix(".py")
        for params in ({"run_id": OTHER_RUN}, {"run_id": OTHER_RUN, name: "latest"}):
            rig = Rig()
            rig.stage_runs(OTHER_RUN)
            with world(rig, dag_id, params) as current:
                error = refuses(FormRefused, lambda current=current, dag_file=dag_file: current.run_dag(dag_file))
                require(name in str(error) and not current.states, f"{dag_id} without its producer run was not refused at the form: {error}")
            require(not rig.ecs.calls and not rig.records.calls, f"{dag_id} without its producer run reached ECS or the launch records")


COLLAPSED_FILES = ("silver_transform_dag.py", "gold_transform_dag.py", "model_promotion_dag.py", "serving_deploy_dag.py")
COLLAPSED_TASKS = ["preflight", "job", "evidence", "gate"]
PROMOTE_PARAMS = {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "aws"}
PROMOTE_APPROVAL = {
    "run_id": PROMOTE_RUN,
    "dag_id": "model_promotion_dag",
    "scope": "promote",
    "approved_on": "2026-10-30",
    "hai_type": "HAI_1",
    "model_version": "3",
}


def task_graph(world_: World) -> dict[str, Any]:
    """The graph rules' view of the tasks the last ``run_dag`` built."""
    graph = importlib.import_module("orchestration.graph")
    return {
        node.task_id: graph.TaskNode(frozenset(before.task_id for before in node.upstream), str(node.settings.get("trigger_rule", graph.ALL_SUCCESS)))
        for node in world_.built
    }


def collapsed_job_dags_are_one_chain() -> None:
    """The silver, gold, promotion and serving DAGs are one task per job: preflight (the gates, no launch), job (the one ECS task, in the pool),
    evidence (the launch record says the job passed) and gate (runs after a failure, fails the DAG run unless the three passed, carries the asset)."""
    graph = importlib.import_module("orchestration.graph")
    rig = Rig()
    rig.stage.stage(PROMOTE_RUN, "promotion_approval.json", PROMOTE_APPROVAL)
    rig.stage.stage(SERVE_RUN, "serving_request.json", SERVING_REQUEST)
    plan: tuple[tuple[str, dict[str, Any], str, str, list[str]], ...] = (
        ("silver_transform_dag", SILVER, "silver", SILVER_RUN, []),
        ("gold_transform_dag", GOLD, "gold", GOLD_RUN, ["gold"]),
        ("model_promotion_dag", PROMOTE_PARAMS, "promote", PROMOTE_RUN, []),
        ("serving_deploy_dag", {"run_id": SERVE_RUN, "mode": "real"}, "serve", SERVE_RUN, []),
    )
    for dag_id, params, job, run, assets in plan:
        before = rig.tasks_started()
        with world(rig, dag_id, dict(params)) as current:
            states = current.run_dag(f"{dag_id}.py")
            nodes = {node.task_id: node for node in current.built}
            require(list(nodes) == COLLAPSED_TASKS, f"{dag_id} is not preflight, job, evidence and gate: {list(nodes)}")
            violations = graph.validate_transform_graph(task_graph(current), dag_id)
            require(not violations, f"{dag_id} breaks the graph rules: {violations}")
            require(nodes["gate"].settings.get("outlets") == assets, f"{dag_id}'s gate carries {nodes['gate'].settings.get('outlets')}, not {assets}")
            require(all(not nodes[name].settings.get("outlets") for name in COLLAPSED_TASKS[:3]), f"{dag_id}: only the gate carries the asset")
            failed = {name: current.results[name] for name, state in states.items() if state != "success"}
            require(not failed and current.results["gate"] == "accepted", f"{dag_id} did not pass: {failed}")
            require(current.results["preflight"] == job, f"{dag_id}'s preflight named {current.results['preflight']}, not {job}")
            evidence = current.results["evidence"]
            require(evidence["job"] == job and evidence["run_id"] == run and evidence["task_arn"], f"{dag_id}'s evidence is {evidence}")
        require(rig.tasks_started() == before + 1, f"{dag_id} started {rig.tasks_started() - before} tasks, not one")
        require((rig.end(run, job) or {}).get("outcome") == "succeeded", f"{job} has no success record")
    rig = Rig()
    with world(rig, "silver_transform_dag", {**SILVER, "mode": "publish_fixture"}) as current:
        states = current.run_dag("silver_transform_dag.py")
        expected = {"preflight": "failed", "job": "upstream_failed", "evidence": "upstream_failed", "gate": "failed"}
        require(states == expected, f"a refused preflight did not stop the job and fail the gate: {states}")
        require("real mode only" in current.results["preflight"] and "preflight failed" in current.results["gate"], f"causes: {current.results}")
    require(not rig.ecs.calls and not rig.records.calls, "a refused preflight reached ECS or the launch records")
    rig = Rig()
    rig.ecs.plan = {"validate": 3}
    with world(rig, "silver_transform_dag", SILVER) as current:
        states = current.run_dag("silver_transform_dag.py")
        expected = {"preflight": "success", "job": "failed", "evidence": "upstream_failed", "gate": "failed"}
        require(states == expected and "job failed" in current.results["gate"], f"a failed job did not fail the gate: {states} {current.results}")
    require(rig.tasks_started() == 1, "a failed job was launched again")
    rig = Rig()
    with world(rig, "silver_transform_dag", SILVER) as current:
        current.enter("evidence")
        error = refuses(LaunchRefused, lambda: current.runtime.collapsed_step("evidence", "pin_bronze"))
    require("no successful launch" in str(error) and not rig.ecs.calls, f"evidence without a launch was not refused: {error}")


STORE_RUN = "20261104T120000Z-5a5a5a5a"
GOLD_RUN_PARAMETER = "gold_snapshot_run"


def form_parameter(dag_file: str, name: str) -> tuple[list[Any], dict[str, Any]] | None:
    """The positional and keyword arguments of the ``Param`` call that declares ``name`` in a shipped DAG file's trigger form."""
    for node in ast.walk(ast.parse((ROOT / "dags" / dag_file).read_text())):
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if isinstance(key, ast.Constant) and key.value == name and isinstance(value, ast.Call):
                    return [ast.literal_eval(item) for item in value.args], {item.arg: ast.literal_eval(item.value) for item in value.keywords if item.arg}
    return None


def feature_store_takes_its_gold_run() -> None:
    """The feature store DAG's Feast steps run the features job, whose preflight needs the passed gold run it publishes from. The form requires that run
    (a run ID, not latest), the run reaches the launch, and a trigger without it is refused before any launch record or ECS call."""
    declared = form_parameter("feature_store_dag.py", GOLD_RUN_PARAMETER)
    require(declared is not None, f"feature_store_dag declares no {GOLD_RUN_PARAMETER} parameter")
    arguments, settings = declared or ([], {})
    pattern = str(settings.get("pattern", ""))
    require(not arguments and settings.get("type") == "string", f"the gold run must be required, with no default: {declared}")
    require(
        bool(pattern)
        and re.fullmatch(pattern.strip("^$"), GOLD_RUN) is not None
        and not any(re.search(pattern, bad) for bad in ("latest", "", "x" + GOLD_RUN)),
        f"the form's pattern does not accept exactly a run ID: {pattern}",
    )
    rig = Rig()
    rig.stage_runs(STORE_RUN)
    layers = importlib.import_module("orchestration.ecs_preflight").derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    gold = layers(silver, "gold")
    with world(rig, "bronze_chain_dag", {"run_id": BRONZE_RUN, "mode": "real"}) as current:
        require(current.step("chain_gate") == "succeeded", "the chain gate failed")
    for dag_id, params in (("silver_transform_dag", {"run_id": silver}), ("gold_transform_dag", {"run_id": gold, "silver_snapshot_run": silver})):
        with world(rig, dag_id, params) as current:
            current.run_dag(f"{dag_id}.py")
            require(current.results.get("gate") == "accepted", f"{dag_id} did not pass: {current.results}")
    before = rig.tasks_started()
    with world(rig, "feature_store_dag", {"run_id": STORE_RUN, GOLD_RUN_PARAMETER: gold}) as current:
        states = current.run_dag("feature_store_dag.py")
        failed = {name: current.results[name] for name, state in states.items() if state != "success"}
        require(bool(states) and not failed, f"feature_store_dag with a passed gold run did not finish: {failed}")
    require(rig.tasks_started() == before + 1, f"feature_store_dag started {rig.tasks_started() - before} tasks, not one")
    claim = rig.launcher.store.run_record(STORE_RUN, "features", "claim")
    require((rig.end(STORE_RUN, "features") or {}).get("outcome") == "succeeded" and claim is not None, "the features job of the store run has no success")
    rig = Rig()
    rig.stage_runs(STORE_RUN)
    with world(rig, "feature_store_dag", {"run_id": STORE_RUN}) as current:
        error = refuses(FormRefused, lambda: current.run_dag("feature_store_dag.py"))
        require(GOLD_RUN_PARAMETER in str(error) and not current.states, f"feature_store_dag without a gold run was not refused at the form: {error}")
    with world(rig, "feature_store_dag", {"run_id": STORE_RUN, GOLD_RUN_PARAMETER: STORE_RUN}) as current:
        states = current.run_dag("feature_store_dag.py")
        refused = [str(result) for result in current.results.values() if "needs the gold job" in str(result)]
        require(states.get("release_run") == "failed" and bool(refused), f"a gold run that never passed was not refused by the preflight: {current.results}")
    require(
        rig.ecs.count("run_task") == 0 and rig.records.count("put_object") == 0,
        "a feature store run without a passed gold run started a task or wrote a launch record",
    )


def promotion_and_serving_dags_run_end_to_end() -> None:
    rig = Rig()
    approval = {
        "run_id": PROMOTE_RUN,
        "dag_id": "model_promotion_dag",
        "scope": "promote",
        "approved_on": "2026-10-30",
        "hai_type": "HAI_1",
        "model_version": "3",
    }
    rig.stage.stage(PROMOTE_RUN, "promotion_approval.json", approval)
    rig.stage.stage(SERVE_RUN, "serving_request.json", SERVING_REQUEST)
    plan = (
        (
            "model_promotion_dag.py",
            "model_promotion_dag",
            {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "aws"},
        ),
        ("serving_deploy_dag.py", "serving_deploy_dag", {"run_id": SERVE_RUN, "mode": "real"}),
    )
    for file, dag_id, params in plan:
        with world(rig, dag_id, params) as current:
            states = current.run_dag(file)
            failed = {name: state for name, state in states.items() if state != "success"}
            require(bool(states) and not failed and current.results["gate"] == "accepted", f"{dag_id} did not finish: {failed} {current.results}")
    require(rig.tasks_started() == 2, f"one task per job was expected: {rig.tasks_started()} tasks")
    for run, job in ((PROMOTE_RUN, "promote"), (SERVE_RUN, "serve")):
        require((rig.end(run, job) or {}).get("outcome") == "succeeded", f"{job} has no success record")
    with world(rig, "serving_deploy_dag", {"run_id": SERVE_RUN, "mode": "real"}) as current:
        current.run_dag("serving_deploy_dag.py")
    require(rig.tasks_started() == 2, "a second run of the same serving DAG run launched again")


def orchestration_imports(name: str) -> set[str]:
    """The orchestration modules that ``orchestration/<name>.py`` imports, at the top or inside a function."""
    found: set[str] = set()
    for node in ast.walk(ast.parse((ROOT / "orchestration" / f"{name}.py").read_text())):
        if isinstance(node, ast.ImportFrom) and node.module == "orchestration":
            found |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("orchestration."):
            found.add(node.module.split(".")[1])
    return found


def launch_modules_are_in_the_mount_list() -> None:
    text = (ROOT / "infra/services/airflow.tf").read_text()
    mounted = set(MOUNT.findall(text)) | set(LAUNCH_MOUNT.findall(text))
    needed, queue = set(), ["airflow_steps", "dag_runtime"]
    while queue:
        name = queue.pop()
        if name not in needed:
            needed.add(name)
            queue.extend(orchestration_imports(name))
    lacking = sorted(f"{name}.py" for name in needed if f"{name}.py" not in mounted)
    require(not lacking, f"the launch modules {lacking} are not in the Airflow mount list")
    for path in (
        "config/orchestration/ecs_broker_jobs.json",
        "config/orchestration/ecs_broker_jobs.schema.json",
        "config/orchestration/host_jobs.json",
        "config/orchestration/ecs_launch.json",
        "config/lakehouse/bronze_tables.json",
        "scripts/lakehouse/bronze_dispatch.py",
    ):
        require(path in text, f"{path} is not in the Airflow mount list")


def host_table_matches_the_job_table() -> None:
    preflight = importlib.import_module("orchestration.ecs_preflight")
    table, host = preflight.load_tables(ROOT)
    require(set(host["jobs"]) == set(table["jobs"]), f"the host table and the job table name other jobs: {set(host['jobs']) ^ set(table['jobs'])}")
    for name, need in host["jobs"].items():
        require(
            all(isinstance(need[key], int) and need[key] >= 0 for key in ("cpu_units", "memory_mib", "scratch_gib")) and need["cpu_units"] <= 3072,
            f"{name}: {need}",
        )
    shipped = {found.group(1) for path in (ROOT / "dags").glob("*.py") if "dag_runtime" in path.read_text() and (found := DAG_ID.search(path.read_text()))}
    require(shipped <= set(host["dags"]), f"DAG files with no family in the host table: {shipped - set(host['dags'])}")
    require(set(host["dags"].values()) <= set(table["jobs"]) | {"chain"}, "a DAG names a family that is not a job")


# The host scratch volume: scratch_volume_gb of infra/platform (60 GiB, PR 83, owner decision of Oct 30 to Nov 3 2026). The filesystem keeps about 5 GiB, so
# a job may ask for at most 55 GiB of free space, or its launch can never fit and ends failed_closed after the fit window (owner A, Nov 7 2026).
HOST_SCRATCH_GIB = 60
SCRATCH_RESERVE_GIB = 5


def every_family_fits_the_host_scratch_volume() -> None:
    preflight = importlib.import_module("orchestration.ecs_preflight")
    _, host = preflight.load_tables(ROOT)
    too_big = {name: need["scratch_gib"] for name, need in host["jobs"].items() if need["scratch_gib"] > HOST_SCRATCH_GIB - SCRATCH_RESERVE_GIB}
    require(not too_big, f"these families ask for more scratch than the {HOST_SCRATCH_GIB} GiB host volume can free: {too_big}")
    note = str(host.get("scratch_note", ""))
    require("measured" in note and str(HOST_SCRATCH_GIB) in note, f"the host table does not say that a measured host layer run sets the final sizes: {note!r}")


def missing_launch_module_is_named() -> None:
    rig = Rig()

    def without_the_module(name: str) -> ModuleType:
        raise ImportError(f"No module named {name!r}")

    with (
        world(rig, "silver_transform_dag", SILVER) as current,
        mock.patch.object(current.runtime, "importlib", SimpleNamespace(import_module=without_the_module)),
    ):
        error = refuses(RuntimeError, lambda: current.step("pin_bronze"))
    require("not in this container's mounts" in str(error) and not rig.ecs.calls, f"a container without the module did not say so: {error}")


def services_come_from_named_environment() -> None:
    steps = importlib.import_module("orchestration.airflow_steps")
    with mock.patch.dict("os.environ", {}, clear=True):
        error = refuses(Exception, steps.services)
    require("is not set" in str(error) and "AWS_REGION" in str(error), f"a missing setting is not named: {error}")


def scratch_usage_counts_only_what_it_can_read() -> None:
    """Owner decision A (Nov 7 2026, H2): admission credits the bytes in the launching family's own scratch folder, measured through Airflow's read-only
    mount. A folder the reader cannot open counts as zero (never more than it can see), a link is not followed, and a family without a folder holds 0."""
    steps = importlib.import_module("orchestration.airflow_steps")
    with tempfile.TemporaryDirectory() as folder, tempfile.TemporaryDirectory() as outside:
        root = Path(folder)
        (root / "train" / "work").mkdir(parents=True)
        (root / "train" / "work" / "model.bin").write_bytes(b"x" * 2**20)
        (root / "train" / "tmp" / "private").mkdir(parents=True)
        (root / "train" / "tmp" / "private" / "hidden.bin").write_bytes(b"x" * 4 * 2**20)
        (Path(outside) / "big.bin").write_bytes(b"x" * 8 * 2**20)
        (root / "train" / "work" / "link").symlink_to(Path(outside) / "big.bin")
        (root / "silver").mkdir()
        (root / "train" / "tmp" / "private").chmod(0)
        try:
            usage = steps.scratch_usage(root, ["train", "silver", "gold"])
        finally:
            (root / "train" / "tmp" / "private").chmod(0o700)
    require(set(usage) == {"train", "silver", "gold"} and usage["silver"] == 0 and usage["gold"] == 0, f"the families are not all measured: {usage}")
    ceiling = 7 * 2**20 if os.geteuid() == 0 else 3 * 2**20  # root reads the private folder too; the link's 8 MiB never counts
    require(2**20 <= usage["train"] < ceiling, f"the train folder is not its readable bytes (a private folder or a link was counted): {usage['train']}")


def services_build_the_session_adapter() -> None:
    """The Airflow image builds the adapter with ``EcsAdapter.from_session``: reads and RunTask go through two ECS clients, and no cloud call is made."""
    steps = importlib.import_module("orchestration.airflow_steps")
    asked: list[tuple[str, bool]] = []
    fake = FakeEcs()

    class Session:
        def __init__(self, region_name: str) -> None:
            self.region = region_name

        def client(self, name: str, config: Any = None) -> Any:
            asked.append((name, config is not None))
            return fake if name == "ecs" else FakeLogs() if name == "logs" else FakeS3()

    with tempfile.TemporaryDirectory() as scratch:
        environment = {name: scratch if name.endswith("SCRATCH_PATH") else f"example-{name.lower()}" for name in steps.SETTINGS}
        provider = SimpleNamespace(BrokerlessEcsRunTaskOperator=StandInLaunchOperator)
        with (
            mock.patch.dict("os.environ", environment, clear=True),
            mock.patch.dict(sys.modules, {"orchestration.ecs_launch_provider": provider}),
            mock.patch("boto3.session.Session", Session),
        ):
            built = steps.services()
    require(
        sorted(name for name, _ in asked).count("ecs") == 2 and all(configured for _, configured in asked),
        f"the clients are not made with their own configs: {asked}",
    )
    require(
        built.launcher.adapter.sender is fake and built.cluster == environment["HAI_LAUNCH_CLUSTER"] and not fake.calls,
        "the adapter or the cluster is wrong, or a cloud call was made",
    )
    require(built.definition("silver") == "example-hai_launch_family_prefixsilver_host", f"the host definition is wrong: {built.definition('silver')}")
    usage = built.launcher.scratch_usage
    require(usage is not None, "the launcher has no reader of the scratch folders, so a family's own leftovers block it")


def metadata_uses_the_host_containers() -> None:
    """The host metadata definition has one client container, ``ingest``. The launch sends overrides for it alone and reads its log, and the fake ECS
    refuses an override for a container the definition lacks, as ECS does. The job table's Fargate containers are unchanged."""
    probe = FakeEcs()
    probe.definitions = {"example_dev_job_metadata_host": frozenset({"ingest"})}
    error = refuses(
        ClientError,
        lambda: probe.run_task(
            cluster=CLUSTER,
            taskDefinition="example_dev_job_metadata_host",
            count=1,
            clientToken="stand-in-token",
            capacityProviderStrategy=[{"capacityProvider": "stand-in_host", "weight": 1}],
            overrides={"containerOverrides": [{"name": "ingest_polaris", "environment": []}]},
        ),
    )
    require("ingest_polaris" in str(error) and not probe.tasks, f"the fake took an override for a container its definition lacks: {error}")
    rig = Rig()
    run = OTHER_RUN
    with world(rig, "metadata_ingest_dag", {"run_id": run, "mode": "real"}) as current:
        require(current.step("metadata_run") == "succeeded", "the metadata job did not pass")
    values = rig.environment()
    require(values == {"ingest": {"RUN_ID": run, "POLARIS_URI": POLARIS}}, f"the metadata overrides are not the host container's: {values}")
    request = SubmissionRequest("metadata_run", run, "real", 1, "metadata_ingest_dag", "metadata_run", "manual__test", None)
    spec = rig.preflight.prepare(request)
    require(spec is not None and spec.log_container == "ingest", f"the metadata log is not read from the host container: {spec}")
    table = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["jobs"]["metadata"]
    require(
        set(table["containers"]) == {"ingest_polaris", "ingest_dbt"} and table["log_container"] == "ingest_polaris",
        "the job table's metadata containers changed",
    )


RUNTIME_MODULES = ("orchestration.client", "scripts.orchestration.broker", "scripts.orchestration.ecs_broker")
RUNTIME_NAMES = ("BrokerClient", "submit_and_wait", "from_session", "hai_job_launch_mode")


def dags_hold_no_broker_client() -> None:
    """No DAG file and not the DAG runtime import a broker module or name a broker client, and ``run_step`` hands its request to the launch library alone."""
    for path in [ROOT / "orchestration/dag_runtime.py", *sorted((ROOT / "dags").glob("*.py"))]:
        text = path.read_text()
        tree = ast.parse(text)
        imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}
        imported |= {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
        named = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)} | {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        reached = sorted((imported & set(RUNTIME_MODULES)) | (named & set(RUNTIME_NAMES)) | {name for name in RUNTIME_NAMES if f'"{name}"' in text})
        require(not reached, f"{path.relative_to(ROOT)} reaches the broker: {reached}")
    rig = Rig()
    sent: list[tuple[str, str]] = []

    def recorded(request: SubmissionRequest, context: Any, selections: Any) -> str:
        sent.append((request.template, request.dag_id))
        return "succeeded"

    with (
        world(rig, "silver_transform_dag", SILVER) as current,
        mock.patch.object(importlib.import_module("orchestration.airflow_steps"), "launch_step", recorded),
    ):
        require(current.step("pin_bronze") == "succeeded", "the step did not return the launch library's state")
    require(sent == [("pin_bronze", "silver_transform_dag")] and not rig.ecs.calls, f"the step did not go to the launch library alone: {sent}")


def host_definitions_hold_the_init_step_and_the_layer_guards() -> None:
    """The containers that Terraform plans for the host definitions are the ones the job table expects: the init step first and wiping the family's scratch
    folders, then the containers of the table row (the host table's where it names them), then the guard for the three layer jobs only."""
    table, host = importlib.import_module("orchestration.ecs_preflight").load_tables(ROOT)
    planned = host_planned_containers()
    require(set(planned) == set(table["jobs"]) and len(planned) == 9, f"nine host families were expected: {sorted(planned)}")
    for job, row in table["jobs"].items():
        expected = ["init", *(host["jobs"].get(job, {}).get("containers") or row["containers"]), *(["guard"] if job in LAYER_JOBS else [])]
        require(planned[job]["containers"] == expected, f"the {job} host definition holds {planned[job]['containers']}, expected {expected}")
        require(
            planned[job]["start_with_scratch_wipe"] == ["init"],
            f"the {job} host definition does not start with its scratch wipe in the init step: {planned[job]}",
        )
    require(
        host_definitions()["example_dev_job_metadata_host"] == frozenset({"init", "ingest"}),
        "the metadata host definition is not the init step and its one client",
    )


CASES: dict[str, Callable[[], None]] = {
    "dags_hold_no_broker_client": dags_hold_no_broker_client,
    "pool_only_when_configured": pool_only_when_configured,
    "gates_refuse_before_any_cloud_call": gates_refuse_before_any_cloud_call,
    "chain_runs_without_a_signed_record": chain_runs_without_a_signed_record,
    "after_needs_the_previous_job_to_pass": after_needs_the_previous_job_to_pass,
    "training_parameters_are_checked": training_parameters_are_checked,
    "aws_mlflow_lane_reaches_the_jobs": aws_mlflow_lane_reaches_the_jobs,
    "local_mlflow_is_refused_visibly": local_mlflow_is_refused_visibly,
    "promotion_needs_the_approval": promotion_needs_the_approval,
    "owner_settle_command_reaches_the_launcher": owner_settle_command_reaches_the_launcher,
    "bronze_request_is_checked_and_bound": bronze_request_is_checked_and_bound,
    "serving_request_is_checked": serving_request_is_checked,
    "code_digest_is_checked": code_digest_is_checked,
    "code_run_is_bound_once": code_run_is_bound_once,
    "control_step_launches_nothing": control_step_launches_nothing,
    "job_runs_once_and_steps_join": job_runs_once_and_steps_join,
    "failed_job_fails_every_step": failed_job_fails_every_step,
    "unavailable_is_bounded": unavailable_is_bounded,
    "five_dags_run_end_to_end": five_dags_run_end_to_end,
    "promotion_and_serving_dags_run_end_to_end": promotion_and_serving_dags_run_end_to_end,
    "dags_run_with_their_form_defaults": dags_run_with_their_form_defaults,
    "collapsed_job_dags_are_one_chain": collapsed_job_dags_are_one_chain,
    "feature_store_takes_its_gold_run": feature_store_takes_its_gold_run,
    "launch_modules_are_in_the_mount_list": launch_modules_are_in_the_mount_list,
    "host_table_matches_the_job_table": host_table_matches_the_job_table,
    "every_family_fits_the_host_scratch_volume": every_family_fits_the_host_scratch_volume,
    "missing_launch_module_is_named": missing_launch_module_is_named,
    "services_come_from_named_environment": services_come_from_named_environment,
    "services_build_the_session_adapter": services_build_the_session_adapter,
    "scratch_usage_counts_only_what_it_can_read": scratch_usage_counts_only_what_it_can_read,
    "metadata_uses_the_host_containers": metadata_uses_the_host_containers,
    "host_definitions_hold_the_init_step_and_the_layer_guards": host_definitions_hold_the_init_step_and_the_layer_guards,
}
