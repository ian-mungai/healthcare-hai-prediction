"""Cases of the DAG steps on the direct ECS launch library: every shipped DAG step launches its job through ``orchestration/airflow_steps.py``.

Isolation is justified because the cloud has to fail on cue (a refused record, a throttled call, a staged file that is missing or denied) and time has to
move by minutes. Every case calls ``orchestration.dag_runtime.run_step`` or runs a shipped DAG file, over the real preflight, the real ``Launcher`` and
the real launch operator guarantees. The Airflow SDK and the provider operator are stand-ins, because Airflow is not installed here: rerun the cases in
the Airflow image after a provider or Airflow upgrade. The failure modes, each with the case that covers it:

1. A DAG file or the DAG runtime reaches a broker client, or a step reaches anything but the launch library (``dags_hold_no_broker_client``).
2. The pool applies only where the deployment names it, and every task that launches a job takes it (``pool_only_when_configured``).
3. Each gate refuses before any record or ECS call: real mode, the previous job's success, the training parameters, the
   promotion approval, the staged bronze request, the closed serving request, the staged code digest (``gates_refuse_before_any_cloud_call`` and the request
   and digest cases). A staged input that is not ready yet is waited for within a bound. A ``LaunchUnavailable`` of the library is never repeated by the step
   (``unavailable_is_bounded``).
4. A chain step and the binding gate run without a signed record, and the gate binds the pipeline's code runs (``chain_runs_without_a_signed_record``).
   A run binds its code run once, an unreadable binding is not read as missing, and the drift run binds its training run (``code_run_is_bound_once``).
5. A control step launches nothing (``control_step_launches_nothing``). A job launches once, later steps of its DAG join it, a failure fails every step,
   and a retry launches nothing (``job_runs_once_and_steps_join``, ``failed_job_fails_every_step``).
6. The silver, gold, features base, promotion and serving DAGs are one task per job: preflight, job, evidence and gate, and a refused preflight or a failed
   job fails the gate with nothing launched again (``collapsed_job_dags_are_one_chain``). The five DAGs of the chain run end to end
   (``five_dags_run_end_to_end``), and every DAG triggered with only its required values runs with its form's defaults: no form defaults to a mode
   the direct path refuses
   (``dags_run_with_their_form_defaults``). The features base DAG's form requires the gold run its features job publishes from, and a run without it
   is refused before any launch (``base_dag_takes_its_gold_run``). Its launch names the feature set ``base`` from the DAG, and a DAG that names no
   feature set cannot launch the features job (``features_job_names_its_feature_set``).
7. The modules, the mount list and the host table agree, and a container without the launch module names it (``launch_modules_are_in_the_mount_list``,
   ``host_table_matches_the_job_table``, ``missing_launch_module_is_named``).
8. The launch services come from the named environment, with the session adapter (``services_come_from_named_environment``,
   ``services_build_the_session_adapter``).
9. The metadata job sends overrides only for the one container of its host definition (``metadata_uses_the_host_containers``). The rig's fake ECS holds
   the containers that Terraform plans for the nine host definitions (``infra/jobs/tests/host_definitions.json``, which a Terraform test keeps equal to the
   plan) and the case ``host_definitions_hold_the_init_step_and_the_layer_guards`` checks them against what the job table and the owner decisions expect.
10. Host launch capacity binding (owner A, Nov 10 2026): a host job runs only the revision whose binding the preflight checked against the current host
   capacity. A latest active revision without a binding (registered before ``host_usable_memory_mib`` set the job memory) is refused
   (``unbound_revision_is_refused``), so is one bound to another capacity or sized to other memory (``revision_bound_to_another_capacity_is_refused``).
   The capacity is a required launch setting, since the services stack always renders it, and an empty or malformed one refuses every host job
   (``malformed_capacity_refuses``). Every refusal comes before any write, and the malformed capacity before any AWS call: no staged-input or record
   read and no ECS call masks it (``malformed_capacity_refuses``), and a drift job refused for its unbound
   revision writes no training code-run binding
   (``unbound_revision_is_refused``). A bound revision runs by its exact ARN,
   so a revision registered after the check never runs in its place (``bound_revision_runs_by_its_exact_arn``).
11. Host job cap setting (Nov 11 2026, replacing the SSM parameter of owner A, Nov 10 2026): the services stack renders the cap into every Airflow
   container as ``HAI_LAUNCH_HOST_JOB_CAP_MIB`` from its one variable ``host_job_cap_mib``, and the preflight refuses a revision whose label gives a job
   more memory, or any launch while the cap is missing or malformed (``revision_above_the_host_job_cap_is_refused``). A revision at or below the cap
   runs (``revision_within_the_host_job_cap_runs``). The production wiring of ``airflow_steps.services()`` reads the cap from the environment alone, with
   no SSM call, and fails visibly on a missing or invalid value; a process with a lower cap refuses the revision and one with a higher cap runs it
   (``production_cap_comes_from_the_environment``). A constant reader in its place fails those checks (``a_constant_cap_reader_is_caught``), and the
   services stack renders the setting from its variable with no cap parameter left (``cap_setting_is_rendered_by_the_services_stack``).
12. Launch hold during services applies (owner A, Nov 10 2026): the preflight refuses every host launch while the hold is set or its parameter is
   missing (``launch_during_the_hold_is_refused``); the fit probe rereads the hold before every RunTask, so a hold set after the preflight sends no
   RunTask (``a_hold_set_after_the_preflight_withholds_runtask``, ``a_hold_set_after_the_probe_withholds_runtask``), the fit probe never offers more
   memory than the cap (``the_fit_probe_never_offers_more_than_the_cap``) and a cleared hold lets a launch run (``a_cleared_hold_lets_the_launch_run``).
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
import itertools
import json
import os
import re
import runpy
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from functools import partial
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest import mock

import jinja2
from botocore.exceptions import ClientError

from orchestration import graph, training_contract
from orchestration.ecs_adapter import EcsAdapter, EcsSettings
from orchestration.ecs_launch import EcsFitProbe, Launcher, LaunchRefused, LaunchUnavailable, S3LaunchStore, launch_record_prefix
from orchestration.ecs_launch_operator import JobFailed
from orchestration.ecs_preflight import HeldCapProbe, InputNotReady
from orchestration.interface import SubmissionRequest
from orchestration.training_contract import PIPELINE_DAG, compose_run_id, model_param_schema, record_key, research_run_id, run_id_parts
from orchestration.training_contract import PIPELINE_RUN as PIPELINE_RUN_NAME
from scripts.orchestration.ecs_adapter_cases import require
from scripts.orchestration.ecs_chain_cases import Dag, Operator
from scripts.orchestration.ecs_fake import CLUSTER, FakeEcs, FakeLogs, FakeS3, client_error
from scripts.orchestration.ecs_launch_cases import refuses
from scripts.orchestration.ecs_launch_fake import START_TIME, FakeClock, FakeLaunchBucket, RevisionEcs
from scripts.orchestration.ecs_launch_operator_cases import StandInLaunchOperator
from scripts.orchestration.ecs_run_fixtures import (
    BRONZE_RUN,
    DIGEST,
    DRIFT_RUN,
    GOLD_RUN,
    INVENTORY,
    PROMOTE_RUN,
    SERVE_RUN,
    SILVER_RUN,
    TRAIN_PARAMS,
    bronze_request,
    collections_of_table_map,
    table_pair,
)


def derived_run_id(parent_run: str, layer: str) -> str:
    """The run ID of ``layer`` in the pipeline run of ``parent_run``: the layer's name and the parent's hex and attempt."""
    parts = run_id_parts(parent_run)
    return compose_run_id(layer, parts.hex, parts.attempt)


PIPELINE_RUN = derived_run_id(BRONZE_RUN, PIPELINE_RUN_NAME)


CONTRACT_NAMES = {
    name: getattr(training_contract, name) for name in ("BASE_RUN", "SET_RUN", "DRIFT_RUN", "RESEARCH_RUN", "RUN_ID", "run_id_pattern", "model_run_pattern")
}


def static_value(node: ast.expr) -> Any:
    """The value of a DAG form's constant expression: a literal, or an f-string over the contract's run ID patterns and the DAG's own constants."""
    if isinstance(node, ast.JoinedStr):
        return "".join(str(static_value(part.value)) if isinstance(part, ast.FormattedValue) else str(cast(ast.Constant, part).value) for part in node.values)
    if isinstance(node, ast.Name):
        return {**CONTRACT_NAMES, "MODEL": "m1", "RETRY_FIRST_DAY": training_contract.RETRY_FIRST_DAY, "RETRY_LAST_DAY": training_contract.RETRY_LAST_DAY}[
            node.id
        ]
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in CONTRACT_NAMES:
        return CONTRACT_NAMES[node.func.id](
            *(static_value(item) for item in node.args), **{item.arg: static_value(item.value) for item in node.keywords if item.arg}
        )
    return ast.literal_eval(node)


ROOT = Path(__file__).resolve().parents[2]
TRAIN_LAUNCH = {key: TRAIN_PARAMS[key] for key in ("features_run", "population", "hai_types", "trigger", "mlflow")}
BUCKET = "example-private-bucket"
PREFIX = launch_record_prefix()
SCRATCH_FREE = 400 * 2**30
FEATURES_RUN = "features_feed5eed"
SET_FEATURES_RUN = "features_ols_feed5eed"
OTHER_RUN = "silver_0f0f0f0f"
OTHER_BASE = "features_0f0f0f0f"
OTHER_SET_RUN = "features_elastic_net_0f0f0f0f"
RUN_KEY = "lakehouse/run_stage/{run}/inputs/{name}"
LAUNCH_MOUNT = re.compile(r'=\s*"orchestration/([A-Za-z0-9_]+\.py)"')
DAG_ID = re.compile(r'dag_id="([a-z0-9_]+)"')
MOUNT = re.compile(r'containerPath\s*=\s*"/opt/hai/orchestration/([A-Za-z0-9_]+\.py)"')
HOST_DEFINITIONS = ROOT / "infra/jobs/tests/host_definitions.json"
# The publisher host definition as infra/jobs/publisher.tf plans it (publisher_host): its containers, the label of its init container and its task memory.
# A Terraform test (tests/publisher.tftest.hcl) keeps this file equal to that plan.
PUBLISHER_HOST_DEFINITION = ROOT / "infra/jobs/tests/publisher_host_definition.json"
PUBLISHER_RUN = "ingestion_ab12cd34"
PUBLISHER_TEST_ROOT = "ingestion_test/ingestion_0a1b2c3d/"
PUBLISHER_PARAMS = {"run_id": PUBLISHER_RUN, "mode": "real", "run_kind": "bootstrap", "capture_root": "raw/"}
LAYER_JOBS = frozenset({"silver", "gold", "features"})
POLARIS = "http://10.0.0.5:8181/api/catalog"
MLFLOW = "http://10.0.0.5:5100"
# TEST FIXTURE ONLY, not measured values: the usable-memory inputs the services stack renders (HAI_LAUNCH_HOST_CAPACITY), the binding label of a host
# revision sized from them (the services output host_capacity_binding) and the job memory it gives.
JOB_MEMORY = 23552
CAPACITY = "usable_memory_mib=30720 system_reserve_mib=2048"
BINDING = f"job_memory_mib={JOB_MEMORY} {CAPACITY} service_reservations_mib=5120"
NOMINAL_BINDING = "job_memory_mib=25600 usable_memory_mib=32768 system_reserve_mib=2048 service_reservations_mib=5120"
TRIGGER_MODULE = "airflow.providers.standard.operators.trigger_dagrun"


class StageBucket(FakeS3):
    """The staged inputs of a run: reads that can be denied and write-once puts, as the launch role would see them."""

    def __init__(self, log: list[str] | None = None) -> None:
        super().__init__()
        self.denied: set[str] = set()
        self.puts: list[str] = []
        self.log = [] if log is None else log

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        self.log.append(f"s3:GetObject {arguments['Key']}")
        if arguments["Key"] in self.denied:
            raise client_error("AccessDenied", "GetObject")
        return super().get_object(**arguments)

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        self.log.append(f"s3:PutObject {arguments['Key']}")
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


def host_planned_containers() -> dict[str, dict[str, Any]]:
    """The containers of each host family as Terraform plans them: ``containers`` in order and ``start_with_scratch_wipe``, the containers whose first command
    starts with the scratch wipe (``infra/jobs/tests/host_definitions.json``, written by ``host_definitions_fixture`` from the plan)."""
    return {**json.loads(HOST_DEFINITIONS.read_text()), **json.loads(PUBLISHER_HOST_DEFINITION.read_text())}


def planned_binding(family: str) -> tuple[int, str | None]:
    """The task memory and the capacity label of the init container of a host revision as the rig registers it: the pinned plan where it names them (the
    publisher's), the shared job memory and binding for the other families."""
    name = family.removeprefix("example_dev_job_").removesuffix("_host")
    planned = host_planned_containers()[name]
    need = json.loads((ROOT / "config/orchestration/host_jobs.json").read_text())["jobs"][name]
    if need.get("reserve"):  # a model set: its row's memory, and the binding with that job memory (infra/jobs/host_sets.tf reads the same row)
        return need["memory_mib"], BINDING.replace(f"job_memory_mib={JOB_MEMORY}", f"job_memory_mib={need['memory_mib']}")
    return planned.get("memory_mib", JOB_MEMORY), planned.get("init_labels", {"hai.host_capacity": BINDING}).get("hai.host_capacity")


def host_definitions() -> dict[str, frozenset[str]]:
    """The containers of each host task definition the rig's fake ECS knows, by the family name ECS sees."""
    return {f"example_dev_job_{family}_host": frozenset(planned["containers"]) for family, planned in host_planned_containers().items()}


class Rig:
    """The stand-in cloud, the staged inputs and the launch services of one case."""

    def __init__(self, capacity: str = CAPACITY, host_cap: str | None = str(JOB_MEMORY)) -> None:
        preflight, steps = importlib.import_module("orchestration.ecs_preflight"), importlib.import_module("orchestration.airflow_steps")
        self.host_cap = host_cap
        self.built: Any = None  # the production launch services, when held_rig builds them
        self.cap_reads = 0  # how often the preflight asked for the host job cap
        self.cloud: list[str] = []  # the staged-input S3 reads and writes, in order
        self.ecs, self.logs, self.stage = RevisionEcs(), FakeLogs(), StageBucket(self.cloud)
        self.records = FakeLaunchBucket(BUCKET, f"{PREFIX}/")
        self.time = FakeClock()
        self.ecs.clock = lambda: self.time.now
        table, host = preflight.load_tables(ROOT)
        self.ecs.definitions = host_definitions()
        self.bound = {family: self.ecs.register(family, *planned_binding(family)) for family in self.ecs.definitions}
        settings = EcsSettings(
            cluster=CLUSTER,
            family_prefix="example_dev_job_",
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
            store=store,
            code_runs=preflight.S3CodeRuns(self.stage, BUCKET),
            clock=lambda: self.time.now,
            bronze_request=preflight.bronze_request_reader(self.stage, BUCKET, collections_of_table_map()),
            host_capacity=capacity,
            task_definition=self.adapter.task_definition,
            host_job_cap=self.read_host_cap,
            read_record=steps.object_reader(self.stage, BUCKET),
        )
        self.preflight.host_launch_hold = lambda: "clear"  # no services apply is running; held_rig puts the production hold reader here
        self.services = steps.LaunchServices(
            preflight=self.preflight,
            launcher=self.launcher,
            operator_class=StandInLaunchOperator,
            cluster=CLUSTER,
            definition=lambda job: self.adapter.definition(job),
            clock=self.time.clock(),
            report=steps.layer_report.ReportAccess(
                s3=None,
                bucket=BUCKET,
                ecs=None,
                autoscaling=None,
                cluster=CLUSTER,
                family_prefix="",
                host_group="stand_in_host",
                directory=Path("stand_in_reports"),
            ),
            read_object=steps.object_reader(self.stage, BUCKET),
        )
        self.reports: list[str] = []
        self.stage_runs(
            BRONZE_RUN, SILVER_RUN, GOLD_RUN, FEATURES_RUN, SET_FEATURES_RUN, PROMOTE_RUN, DRIFT_RUN, SERVE_RUN, PUBLISHER_RUN, str(TRAIN_PARAMS["run_id"])
        )

    def read_host_cap(self) -> str | None:
        """The stand-in of the host job cap reader: the value the services stack renders into the environment."""
        self.cap_reads += 1
        return self.host_cap

    def aws_calls(self) -> list[str]:
        """Every AWS call of the case so far: the staged-input S3 reads and writes, the launch-record S3 calls, ECS and CloudWatch Logs."""
        return [
            *self.cloud,
            *(f"s3:{name} {arguments.get('Key', '')}" for name, arguments in self.records.calls),
            *(f"ecs:{name}" for name, _ in self.ecs.calls),
            *("logs:GetLogEvents" for _ in self.logs.calls),
        ]

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


def json_type_matches(value: Any, declared: str | list[str]) -> bool:
    """Whether ``value`` has the JSON Schema type the form declares (as Airflow's form validation does): a bool is never a number."""
    kinds = {
        "string": lambda item: isinstance(item, str),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "number": lambda item: isinstance(item, int | float) and not isinstance(item, bool),
        "boolean": lambda item: isinstance(item, bool),
        "array": lambda item: isinstance(item, list),
        "object": lambda item: isinstance(item, dict),
        "null": lambda item: item is None,
    }
    return any(kinds[kind](value) for kind in ([declared] if isinstance(declared, str) else declared))


class Skipped(Exception):
    """The stand-in of Airflow's ``AirflowSkipException``: the task ends skipped, not failed."""


class FormRefused(Exception):
    """Airflow refused the trigger at the DAG's form: no DAG run and no task."""


class World:
    """The DAG runtime with the SDK stand-in, the rig's Variable and launch services, and the current task's context."""

    def __init__(self, rig: Rig, runtime: ModuleType, dag_id: str, params: dict[str, Any]) -> None:
        self.rig, self.runtime, self.dag_id, self.params = rig, runtime, dag_id, params
        # The tasks that the DAG ships. The live drift DAG is outside the graph inventory but builds the one-job chain. No other DAG outside it has any.
        self.tasks = (
            set(graph.reference_graph(dag_id)) if dag_id in graph.GRAPH_DAGS else set(graph.COLLAPSED_UPSTREAM) if dag_id == "drift_live_check_dag" else set()
        )
        self.context: dict[str, Any] = {}
        self.states: dict[str, str] = {}
        self.results: dict[str, Any] = {}
        self.built: list[Operator] = []
        self.dag_settings: dict[str, Any] = {}
        self.strict_enum = False  # a case that tests a form's enum turns this on; the other cases keep the stand-in's lenient form
        self.dag_runs: list[tuple[str, str, dict[str, str]]] = []  # the DAG runs that a task asked the Airflow API to create
        self.dag_run_exists = False  # the API answers that the run exists already (409)
        self.dag_run_error: Exception | None = None  # the API call raises this
        self.paused: set[str] = set()  # the DAGs that Airflow holds paused
        self.existing_conf: Any = "same"  # the settings the existing run was started with ("same" as the request that found it, or a dict, or None)

    def dag_is_paused(self, dag_id: str) -> bool:
        """The stand-in of ``dag_runtime.dag_is_paused``."""
        return dag_id in self.paused

    def dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any] | None:
        """The stand-in of ``dag_runtime.dag_run``: the run that a request found existing, with the settings it was started with."""
        asked = [conf for dag, run, conf in self.dag_runs if (dag, run) == (dag_id, dag_run_id)]
        if not asked:
            return None
        return {"dag_id": dag_id, "dag_run_id": dag_run_id, "state": "success", "conf": asked[-1] if self.existing_conf == "same" else self.existing_conf}

    def create_dag_run(self, dag_id: str, dag_run_id: str, conf: dict[str, str]) -> bool:
        """The stand-in of ``dag_runtime.create_dag_run``: it records the request and answers as configured."""
        if self.dag_run_error is not None:
            raise self.dag_run_error
        self.dag_runs.append((dag_id, dag_run_id, conf))
        return not self.dag_run_exists

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
        """Run the work step of ``template``. A template that is no task of the DAG is the launch of its one-job chain, which runs in the task ``job``."""
        self.enter("job" if "job" in self.tasks and template not in self.tasks else template, attempt)
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
        dag_settings = self.dag_settings
        dag_settings.clear()

        class FormDag(Dag):
            def __init__(self, **settings: Any) -> None:
                super().__init__(**settings)
                forms.append(settings.get("params", {}))
                dag_settings.clear()
                dag_settings.update(settings)

        class Trigger:
            """The stand-in of ``TriggerDagRunOperator``: called, or partial with the requests mapped over a task's output (``expand_kwargs``)."""

            def __call__(self, **settings: Any) -> Operator:
                node = Operator(**settings)
                built.append(node)
                return node

            def partial(self, **settings: Any) -> SimpleNamespace:
                def expand_kwargs(source: Any) -> Operator:
                    node = self(**settings)
                    source.operator >> node
                    return node

                return SimpleNamespace(expand_kwargs=expand_kwargs)

        trigger = Trigger()

        with (
            mock.patch.object(sys.modules["airflow.sdk"], "task", task),
            mock.patch.object(sys.modules["airflow.sdk"], "DAG", FormDag),
            mock.patch.object(sys.modules["airflow.sdk"], "get_current_context", lambda: self.context),
            mock.patch.object(self.runtime, "task", task),
            mock.patch.object(sys.modules[TRIGGER_MODULE], "TriggerDagRunOperator", trigger),
        ):
            runpy.run_path(str(ROOT / "dags" / dag_file))
        for form in forms:  # Airflow fills a parameter the trigger leaves out with the form's default, and refuses the trigger before any task runs
            for name, (arguments, settings) in form.items():  # when a required parameter is missing or a value breaks its pattern.
                if arguments and name not in self.params:
                    self.params[name] = arguments[0]
                if name not in self.params:
                    raise FormRefused(f"{name} is required")
                value, declared = self.params[name], settings.get("type")
                if declared is not None and not json_type_matches(value, declared):
                    raise FormRefused(f"{name}={value!r} is not of the declared type {declared}")
                if self.strict_enum and "enum" in settings and value not in settings["enum"]:  # an enum limits the value to the listed ones, as Airflow does
                    raise FormRefused(f"{name}={value!r} is not one of {settings['enum']}")
                pattern = settings.get("pattern")
                if pattern and re.search(pattern, value) is None:  # the form's patterns apply to strings, which the type check above guarantees
                    raise FormRefused(f"{name}={value!r} does not match {pattern}")
        for node in built:
            self.enter(node.task_id)
            before_states = [self.states.get(before.task_id) for before in node.upstream]
            if not all(state == "success" for state in before_states) and node.settings.get("trigger_rule") != "all_done":
                # A skipped upstream task skips an all_success task and a failed one fails it, as Airflow does.
                self.states[node.task_id] = "skipped" if all(state in ("success", "skipped") for state in before_states) else "upstream_failed"
                continue
            try:
                self.results[node.task_id] = node.function() if node.function else None
                self.states[node.task_id] = "success"
            except Skipped as error:
                self.results[node.task_id] = f"{type(error).__name__}: {error}"
                self.states[node.task_id] = "skipped"
            except Exception as error:
                self.results[node.task_id] = f"{type(error).__name__}: {error}"
                self.states[node.task_id] = "failed"
        return dict(self.states)


class StockCronTimetable:
    """Stand-in of Airflow's ``CronTriggerTimetable``: the cron and the time zone it was given, and the name its scheduler gives a run (``<type>__<time>``)."""

    def __init__(self, cron: str, *, timezone: str, **_: Any) -> None:
        self.cron, self.timezone = cron, timezone

    def generate_run_id(self, *, run_type: Any, run_after: Any, data_interval: Any, **_: Any) -> str:
        return f"{getattr(run_type, 'value', run_type)}__{run_after.isoformat()}"


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
    modules: dict[str, Any] = {
        name: ModuleType(name) for name in ("airflow", "airflow.providers", "airflow.providers.standard", "airflow.providers.standard.operators")
    }
    modules["airflow.sdk"] = sdk
    modules["airflow.timetables"] = ModuleType("airflow.timetables")
    modules["airflow.timetables.trigger"] = SimpleNamespace(CronTriggerTimetable=StockCronTimetable)
    modules["airflow.sdk.exceptions"] = ModuleType("airflow.sdk.exceptions")
    modules["airflow.sdk.exceptions"].__dict__["AirflowSkipException"] = Skipped
    modules[TRIGGER_MODULE] = SimpleNamespace(TriggerDagRunOperator=lambda **settings: Operator(**settings))
    try:
        with mock.patch.dict(sys.modules, modules):
            runtime = importlib.import_module("orchestration.dag_runtime")
            steps = importlib.import_module("orchestration.airflow_steps")
            current = World(rig, runtime, dag_id, params)

            def accepted_report(access: Any, layer: str, run: str) -> dict[str, Any]:
                rig.reports.append(f"report {layer} {run}")
                return {"report": "stand_in", "passed": True}

            with (
                mock.patch.object(runtime, "get_current_context", lambda: current.context),
                mock.patch.object(runtime, "create_dag_run", current.create_dag_run),
                mock.patch.object(runtime, "dag_is_paused", current.dag_is_paused, create=True),
                mock.patch.object(runtime, "dag_run", current.dag_run, create=True),
                mock.patch.object(steps, "services", lambda: rig.services),
                mock.patch.object(steps.layer_report, "save_before_listing", lambda access, layer, run: rig.reports.append(f"listing {layer} {run}")),
                mock.patch.object(steps.layer_report, "save_closing_evidence", lambda access, layer, run: rig.reports.append(f"closing {layer} {run}")),
                mock.patch.object(steps.layer_report, "accept", accepted_report),
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
            built.clear()
            with (
                mock.patch.dict("os.environ", POOL_ENVIRONMENT, clear=True),
                mock.patch.dict(sys.modules, {**providers, trigger.__name__: trigger}),
                mock.patch.object(sys.modules["airflow.sdk"], "task", task),
                mock.patch.object(runtime, "task", task),
            ):
                runpy.run_path(str(ROOT / "dags" / dag_file))
            slots = {node.task_id: node.settings.get("pool_slots") for node in built}
            want = {"drift_run": 32, "metadata_run": 16}[launch_task]  # drift 2,048 units, metadata 1,024 units: slots of 64
            require(slots.get(launch_task) == want, f"{launch_task} of {dag_file} takes the slots {slots}, expected {want}")
    text = (ROOT / "infra/services/airflow.tf").read_text()
    require(
        re.search(r"airflow pools set \$\{local\.airflow_launch_pool\} \$\{local\.airflow_pool_slots\} ", text) is not None
        and re.search(r"HAI_LAUNCH_POOL\s*=\s*local\.airflow_launch_pool", text) is not None,
        "the deployment neither creates the pool with its computed slots nor names it to the tasks",
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
    takes no setting for one, a chain step and the binding gate run without it, and the gate still binds the pipeline's code runs."""
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
    silver = derived_run_id(BRONZE_RUN, "silver")
    with world(rig, PIPELINE_DAG, {"run_id": PIPELINE_RUN, "mode": "real"}) as current:
        require(current.step("bind_code") == "succeeded", "the binding gate did not run without a signed record")
    require(rig.preflight.code_runs.stored(silver) == BRONZE_RUN, "the binding gate did not bind the silver run to the bronze run's code")
    require(not rig.ecs.calls, "the binding gate launched a task")


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
        lineage = {name: values["LINEAGE_INVOCATION"] for name, values in passed.environment(1).items() if "LINEAGE_INVOCATION" in values}
        require(
            set(lineage) == {"pin", "upload"} and lineage["pin"] == lineage["upload"],
            f"only the gold pin and upload containers get the lineage identity: {sorted(lineage)}",
        )
        wanted = {
            "template": "pin_silver",
            "dag_id": "gold_transform_dag",
            "task_id": "job",
            "dag_run_id": "manual__test",
            "attempt": 1,
            "input_run": SILVER_RUN,
        }
        found = json.loads(lineage["pin"])
        deadline = found.pop("deadline_epoch")
        require(found == wanted, f"the lineage identity is {lineage['pin']}")
        # the absolute launch deadline: the request's build time plus the job's 6 hours, never later than the launcher's (task start plus 6 hours)
        require(
            int(passed.time.now) - 60 <= deadline - 21600 <= int(passed.time.now),
            f"the lineage deadline {deadline} is not the launch time plus the job's deadline",
        )
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
    for dag_id in MODEL_DAGS:
        require('"mlflow": "aws"' in (ROOT / "dags" / f"{dag_id}.py").read_text(), f"{dag_id} starts its drift check with another MLflow lane than aws")


FRESH_SILVER_RUN = "silver_f2e5f2e5"


def unfinished_silver_launch(rig: Rig) -> str:
    """A silver launch whose scheduler died after the task started: the records stop at ``intent`` and ``started``. The owner then stopped the task
    (``aws_tasks stop``), so ECS shows it STOPPED. Returns the task ARN."""
    rig.ecs.hold = True
    request = SubmissionRequest("pin_bronze", SILVER_RUN, "real", 1, "silver_transform_dag", "job", "manual__test", None)
    spec = rig.preflight.prepare(request)
    require(spec is not None, "the silver step names no job")
    launch = rig.launcher.attach(spec)
    rig.launcher.start(launch)
    events = sorted(rig.launcher.events(SILVER_RUN, "silver", 1))
    require(events == ["intent", "started"] and launch.arn is not None, f"the unfinished launch has other records: {events}")
    rig.ecs.stop_task(cluster=CLUSTER, task=launch.arn, reason="aws_tasks stop")
    rig.ecs.hold = False
    return str(launch.arn)


def fresh_run_settles_the_unfinished_launch_itself() -> None:
    """Automatic launch recovery: a silver launch whose scheduler died after its task started, with the task now STOPPED, has no end record. The next
    run of the family meets it at the family gate, settles it through the scheduler's own launch services (the cancel finds the stopped task) and launches
    in the same step. No DAG and no owner command runs between the two runs."""
    rig = Rig()
    unfinished_silver_launch(rig)
    require(rig.end(SILVER_RUN, "silver") is None, "the earlier launch has an end record before the settle")
    rig.stage_runs(FRESH_SILVER_RUN)
    fresh = {"run_id": FRESH_SILVER_RUN, "mode": "real"}
    started = rig.tasks_started()
    with world(rig, "silver_transform_dag", fresh) as current:
        require(current.step("pin_bronze") == "succeeded", "the fresh run did not launch after settling the unfinished launch")
    end = rig.end(SILVER_RUN, "silver")
    require(end is not None and end["outcome"] == "cancelled", f"the unfinished launch was not ended by the fresh run: {end}")
    require(rig.tasks_started() == started + 1, "the fresh run did not start its own task")
    require((rig.end(FRESH_SILVER_RUN, "silver") or {}).get("outcome") == "succeeded", "the fresh run has no success record")


def promotion_needs_no_approval() -> None:
    """A promotion launches with no owner approval staged: a challenger that passes the promotion rule goes to the canary automatically and the owner is
    notified (owner decision "Per-model feature DAGs", Q10, Nov 9 2026). The launch reads no promotion_approval.json."""
    rig = Rig()
    params = {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "aws"}
    with world(rig, "model_promotion_dag", params) as current:
        require(current.step("promote") == "succeeded", "a promotion without a staged approval did not launch")
    require(not any("promotion_approval.json" in str(key) for key in rig.stage.reads), f"the launch read a promotion approval: {rig.stage.reads}")


def bronze_request_is_checked_and_bound() -> None:
    pair, other = table_pair()
    good = bronze_request(pair)
    params = {"run_id": BRONZE_RUN, "mode": "real"}
    rig = Rig()
    with world(rig, "ingestion_dag", params) as current:
        raised = refuses(LaunchUnavailable, lambda: current.step("bronze_load"))
        require("not_staged" in str(raised) and rig.time.now - 1_800_000_000.0 <= 700, f"a missing request was not retried within a bound: {raised}")
        bads = (b"not json", {**good, "extra": 1}, {**good, "tables": [other, "ghost"]}, {**good, "dry_run": "no"}, {**good, "target": "other"})
        no_list = {key: value for key, value in good.items() if key != "skipped_tables"}
        for bad in (*bads, no_list, {**good, "skipped_tables": [pair[0]]}, {**good, "skipped_tables": ["ghost"]}, {**good, "skipped_tables": other}):
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


def a_full_bronze_run_fits_the_runtask_overrides() -> None:
    """The bronze_stage job stages a request for every table of the table map (330 at Nov 18 2026, about 11 KB) and the bronze job runs it. A RunTask call
    takes at most 8192 characters of container overrides, all containers together (the fake refuses more, as the service does). The tables travel in the
    staged request, which each phase reads and checks against its digest, and never in the overrides of the pin, load or check container."""
    names = list(collections_of_table_map())
    good = bronze_request(names)
    ingestion, bronze = "ingestion_0f0f0f0f", "bronze_0f0f0f0f"
    rig = Rig()
    rig.stage_runs(ingestion)
    rig.stage.stage(bronze, "bronze_request.json", good)
    require(len(rig.stage.objects[RUN_KEY.format(run=bronze, name="bronze_request.json")]) > 4096, "the request is not as large as a full run's")
    with world(rig, "bronze_dag", {"run_id": bronze, "mode": "real"}) as current:
        require(current.step("bronze_stage") == "succeeded", "the stage job did not launch")
        require(current.step("bronze_load") == "succeeded", "a full-map bronze request did not launch")
    sent = [arguments for name, arguments in rig.ecs.calls if name == "run_task"]
    require(len(sent) == 2, f"{len(sent)} tasks started")
    for arguments in sent:
        size = len(json.dumps(arguments["overrides"], separators=(",", ":")))
        require(size < 4096, f"the overrides of {arguments['taskDefinition']} hold {size} characters, with a service limit of 8192")
    values = rig.environment(1)
    require(set(values) == {"fetch", "pin", "load", "check"}, f"{sorted(values)}")
    require(all(not value or "," not in value for variables in values.values() for value in variables.values()), "a table list is in the container overrides")
    require(values["pin"]["BRONZE_TABLES"] == values["load"]["BRONZE_TABLES"] == values["check"]["BRONZE_TABLES"] == "", f"{values}")
    require(len(values["load"]["BRONZE_REQUEST_SHA256"]) == 64 and values["pin"]["BRONZE_INVENTORY_SHA256"] == INVENTORY, f"{values['pin']}")


def a_bronze_run_runs_the_code_of_its_ingestion_run() -> None:
    """A bronze run that its ingestion run started has no code staged under its own run ID. Its own jobs bind it, and the binding gate binds the layer runs of
    its pipeline, to the code of its ingestion run, and its bronze job fetches that code. A bronze run with code staged by hand keeps that code, and a
    binding is never replaced."""
    pair, _ = table_pair()
    good = bronze_request(pair)
    ingestion, bronze = "ingestion_0b0b0b0b", "bronze_0b0b0b0b"
    rig = Rig()
    rig.stage_runs(ingestion)
    rig.stage.stage(bronze, "bronze_request.json", good)
    pipeline = derived_run_id(bronze, PIPELINE_RUN_NAME)
    with world(rig, PIPELINE_DAG, {"run_id": pipeline, "mode": "real"}) as current:
        require(current.step("bind_code") == "succeeded" and not rig.ecs.calls, "the binding gate failed or reached ECS")
        require(rig.preflight.code_runs.stored(bronze) == ingestion, "the bronze run is not bound to the code of its ingestion run")
        for layer in ("silver", "gold", "features"):
            require(rig.preflight.code_runs.stored(f"{layer}_0b0b0b0b") == ingestion, f"the {layer} run is not bound to the code of the ingestion run")
    with world(rig, "bronze_dag", {"run_id": bronze, "mode": "real"}) as current:
        require(current.step("bronze_load") == "succeeded", "the bronze job did not run")
    require(rig.environment()["fetch"]["CODE_RUN_ID"] == ingestion, "the bronze job fetches its own code, not its ingestion run's")
    own = Rig()
    own.stage_runs("ingestion_0c0c0c0c", "bronze_0c0c0c0c")
    with world(own, PIPELINE_DAG, {"run_id": "pipeline_0c0c0c0c", "mode": "real"}) as current:
        current.step("bind_code")
    require(own.preflight.code_runs.stored("bronze_0c0c0c0c") is None, "a bronze run with its own staged code was bound away from it")
    require(own.preflight.code_runs.stored("silver_0c0c0c0c") == "bronze_0c0c0c0c", "its layers do not run its own code")
    none = Rig()
    with world(none, PIPELINE_DAG, {"run_id": "pipeline_0d0d0d0d", "mode": "real"}) as current:
        current.step("bind_code")
    require(
        none.preflight.code_runs.stored("silver_0d0d0d0d") == "ingestion_0d0d0d0d",
        "a run with no code anywhere is bound to its ingestion run, which then refuses it by name",
    )
    alone = Rig()
    alone.stage_runs("ingestion_0e1e0e1e")
    with world(alone, "bronze_dag", {"run_id": "bronze_0e1e0e1e", "mode": "real"}) as current:
        require(current.step("bronze_stage") == "succeeded" and alone.tasks_started() == 1, "a hand-started bronze DAG did not launch its stage job")
    require(
        alone.preflight.code_runs.stored("bronze_0e1e0e1e") == "ingestion_0e1e0e1e", "the stage job did not bind the bronze run to its ingestion run's code"
    )
    require(
        all(alone.preflight.code_runs.stored(f"{layer}_0e1e0e1e") is None for layer in ("silver", "gold", "features")),
        "a hand-started bronze DAG bound a layer run: only the binding gate binds the layers",
    )


def the_binding_gate_takes_only_a_pipeline_run() -> None:
    """The binding gate binds the code of a pipeline run's layers: it refuses a run of another name by that name, before it binds anything."""
    rig = Rig()
    rig.stage_runs("ingestion_0f1f0f1f")
    for run in ("bronze_0f1f0f1f", "silver_0f1f0f1f", "ingestion_0f1f0f1f", "latest", "20261116T201053Z-b10a4f2d"):
        with world(rig, PIPELINE_DAG, {"run_id": run, "mode": "real"}) as current:
            error = refuses(LaunchRefused, lambda current=current: current.step("bind_code"))
        require(
            "bind_code" in str(error) and PIPELINE_RUN_NAME in str(error), f"bind_code on {run} was refused without naming the step and the run name: {error}"
        )
    require(not any("code_run.json" in key for key in rig.stage.objects) and not rig.ecs.calls, "the gate bound a run or reached ECS for a run of another name")


def the_stage_job_runs_before_the_request_exists_and_the_bronze_job_after() -> None:
    """The owner chose to stage a pipeline run's bronze inputs from Airflow: the chain launches the ``bronze_stage`` job, which writes the request and the
    source inventory, and then the ``bronze`` job under the same run ID. A launch is keyed by the run and the job, so each job has its own launch record.
    The stage launch needs no staged request and runs the code of the ingestion run. Airflow writes none of the staged inputs."""
    pair, _ = table_pair()
    good = bronze_request(pair)
    ingestion, bronze = "ingestion_0e0e0e0e", "bronze_0e0e0e0e"
    rig = Rig()
    rig.stage_runs(ingestion)
    with world(rig, "bronze_dag", {"run_id": bronze, "mode": "real"}) as current:
        require(current.step("bronze_stage") == "succeeded", "the stage job did not launch with no staged request")
        require(rig.tasks_started() == 1, f"the stage launch started {rig.tasks_started()} tasks")
        sent = [arguments for name, arguments in rig.ecs.calls if name == "run_task"][0]
        require(sent["taskDefinition"].endswith("_job_bronze_stage_host") or "_job_bronze_stage_host" in sent["taskDefinition"], f"{sent['taskDefinition']}")
        values = rig.environment()
        require(set(values) == {"fetch", "stage"}, f"the stage task overrides other containers: {sorted(values)}")
        require(values["fetch"]["CODE_RUN_ID"] == ingestion, "the stage job does not fetch the code of the ingestion run")
        require(values["stage"]["RUN_ID"] == bronze and not any(name.startswith("BRONZE_") for name in values["stage"]), f"{values['stage']}")
        require(
            not any("bronze_request.json" in str(key) or "bronze_inventory.json" in str(key) for key in rig.stage.objects), "the launch wrote a staged input"
        )
        raised = refuses(LaunchUnavailable, lambda: current.step("bronze_load"))
        require("not_staged" in str(raised) and rig.tasks_started() == 1, f"the bronze job launched before its request was staged: {raised}")
        rig.stage.stage(bronze, "bronze_request.json", good)
        require(current.step("bronze_load") == "succeeded" and rig.tasks_started() == 2, "the bronze job did not launch once the request was staged")
    require((rig.end(bronze, "bronze_stage") or {}).get("outcome") == "succeeded", "the stage job has no success record under the bronze run ID")
    require((rig.end(bronze, "bronze") or {}).get("outcome") == "succeeded", "the bronze job has no success record under the same run ID")
    require(rig.preflight.code_runs.stored(bronze) == ingestion, "the run is not bound to the code of its ingestion run")


SERVE_BASE = "features_b45e0001"
SERVE_RESEARCH = research_run_id(SERVE_BASE)
SERVE_FORM = {"run_id": SERVE_RUN, "mode": "real", "hai_type": "HAI_1", "deploy_mode": "stable", "research_run": "latest"}
SERVE_CANARY = {**SERVE_FORM, "deploy_mode": "canary", "research_run": SERVE_RESEARCH}


def research_record_of(rig: Rig, challenger: dict[str, Any] | None, hai_type: str = "HAI_1") -> None:
    """Store a research record the way the research phase does, naming ``challenger`` (or none) as the type's challenger."""
    document = {"schema": 1, "hai_type": hai_type, "base_run": SERVE_BASE, "runs": {}, "promotion": {"challenger": challenger}}
    rig.stage.objects[record_key("research", SERVE_RESEARCH, hai_type)] = json.dumps(document).encode()


def serving_request_is_closed_and_derived() -> None:
    """The launch form carries the infection type and the mode and nothing else. The preflight derives the candidate version of a canary from the type's
    research record, fixes the share from the job table and gives a stable deploy no version, so no request can name a version or a share."""
    rig = Rig()
    challenger = {"model": "m2", "version": "4", "verdict": "pass", "source_run": "mlrun4"}
    with world(rig, "serving_deploy_dag", SERVE_CANARY) as current:
        for name, record in (("no research record", None), ("a type without a challenger", {"challenger": None})):
            if record is not None:
                research_record_of(rig, None)
            error = refuses(LaunchRefused, lambda: current.step("serve_deploy"))
            require("serving_candidate_refused" in str(error), f"{name}: {error}")
        require(not rig.ecs.calls, "a canary with no candidate reached the cloud")
        research_record_of(rig, challenger)
        require(current.step("serve_deploy") == "succeeded", "a canary of the research record's challenger did not run")
        derived = json.loads(rig.environment()["serve"]["SERVING_REQUEST"])
        require(
            derived
            == {"hai_type": "HAI_1", "deploy_mode": "canary", "candidate_version": "4", "percent": 10, "canary_ttl_hours": 24, "research_run": SERVE_RESEARCH},
            f"the derived request is {derived}",
        )
    with world(rig, "serving_deploy_dag", {**SERVE_CANARY, "deploy_mode": "promote"}) as current:
        current.strict_enum = True
        refuses(FormRefused, partial(current.run_dag, "serving_deploy_dag.py"))
    with world(rig, "serving_deploy_dag", {**SERVE_CANARY, "research_run": "latest"}) as current:
        error = refuses(LaunchRefused, lambda: current.step("serve_deploy"))
        require("names none" in str(error), f"a canary without a research run: {error}")
    stable_run = "serving_5e5e5e5f"
    rig.stage_runs(stable_run)
    with world(rig, "serving_deploy_dag", {**SERVE_FORM, "run_id": stable_run}) as current:
        require(current.step("serve_deploy") == "succeeded", "a stable deploy did not run")
        derived = json.loads(rig.environment(1)["serve"]["SERVING_REQUEST"])
        require(
            derived == {"hai_type": "HAI_1", "deploy_mode": "stable", "candidate_version": None, "percent": 0, "canary_ttl_hours": None, "research_run": None},
            f"the derived stable request is {derived}",
        )
    before = rig.tasks_started()
    for extra in ({"candidate_version": "9"}, {"percent": 40}, {"candidate_version": "9", "percent": 40}, {"deploy_mode": "rollback"}):
        training = {"hai_type": "HAI_1", "deploy_mode": "canary", **extra}
        request = SubmissionRequest(
            "serve_deploy", "serving_5e5e5e60", "real", 1, "serving_deploy_dag", "job", "manual__test", SERVE_RESEARCH, training=training
        )
        rig.stage_runs(request.run_id)
        refuses(LaunchRefused, partial(rig.preflight.prepare, request))
    require(rig.tasks_started() == before, "a request that named a version or a share reached the cloud")


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
    layers = derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    params = {"run_id": PIPELINE_RUN, "mode": "real"}
    with world(rig, PIPELINE_DAG, params) as current:
        require(current.step("bind_code") == "succeeded" and not rig.ecs.calls, "the gate failed or reached ECS")
        require(current.step("bind_code") == "succeeded", "a repeated gate failed")
    bound = json.loads(rig.stage.objects[RUN_KEY.format(run=silver, name="code_run.json")])
    require(bound == {"code_run": BRONZE_RUN}, f"the layer run is not bound to the bronze run: {bound}")
    with world(rig, "silver_transform_dag", {"run_id": silver, "mode": "real"}) as current:
        current.step("pin_bronze")
        require(rig.environment()["fetch"]["CODE_RUN_ID"] == BRONZE_RUN, "the layer run fetches its own code, not its bronze run's")
    other = layers("silver_b1b1b1b1", "silver")
    require(other != silver, "two bronze runs share a layer run")
    with world(rig, PIPELINE_DAG, params) as current:
        rig.stage.objects[RUN_KEY.format(run=silver, name="code_run.json")] = json.dumps({"code_run": "20261031T090000Z-b1b1b1b1"}).encode()
        error = refuses(LaunchRefused, lambda: current.step("bind_code"))
        require("another run" in str(error), f"a rebind to another code run was not refused: {error}")
        rig.stage.denied.add(RUN_KEY.format(run=silver, name="code_run.json"))
        refuses(LaunchUnavailable, lambda: current.step("bind_code"))
    fresh = Rig()
    with world(fresh, "silver_transform_dag", SILVER) as current:
        fresh.stage.denied.add(RUN_KEY.format(run=SILVER_RUN, name="code_run.json"))
        error = refuses(LaunchUnavailable, lambda: current.step("pin_bronze"))
        require("binding" in str(error) and not fresh.ecs.calls, f"a denied binding read was taken for no binding: {error}")
    chained = Rig()
    _, features_model = model_chain_passed(chained, "m1")
    drift_run = drift_run_of(features_model)
    started = chained.tasks_started()
    with world(
        chained, "drift_monitoring_dag", {"run_id": drift_run, "mode": "real", "mlflow": "aws", "model": "m1", "features_run": features_model}
    ) as current:
        current.step("drift_run")
    require(chained.environment(started)["fetch"]["CODE_RUN_ID"] == BRONZE_RUN, "the drift run fetches its own code, not its bronze run's")
    training = json.loads(chained.stage.objects[RUN_KEY.format(run=training_run_of(drift_run, "m1"), name="code_run.json")])
    require(training == {"code_run": BRONZE_RUN}, f"the training run is not bound to the bronze run's code: {training}")


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


def unbound_revision_is_refused() -> None:
    """Owner decision A (Nov 10 2026, host launch capacity binding): the latest active revision of a family carries no capacity binding when it was
    registered before ``host_usable_memory_mib`` set the job memory (sized from the nominal host memory). The step is refused by name, nothing is
    recorded and no task starts. An empty label is refused the same way."""
    for label in (None, ""):
        rig = Rig()
        old = rig.ecs.register("example_dev_job_silver_host", 25600, label)
        with world(rig, "silver_transform_dag", SILVER) as current:
            error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
        require(
            "carries no host capacity binding" in str(error) and f"example_dev_job_silver_host:{old.rsplit(':', 1)[1]}" in str(error),
            f"the unbound revision is not named: {error}",
        )
        require("000000000000" not in str(error) and "arn:" not in str(error), f"the refusal shows an ARN or an account: {error}")
        require(rig.tasks_started() == 0 and not rig.records.calls, "an unbound revision launched or wrote a record")
    rig = Rig()
    _, run = model_chain_passed(rig, "m2")
    rig.ecs.register("example_dev_job_drift_host", 25600, None)
    started = rig.tasks_started()
    writes = len([call for call in rig.aws_calls() if call.startswith("s3:Put")])
    params = {"run_id": drift_run_of(run), "mode": "real", "mlflow": "aws", "model": "m2", "features_run": run}
    with world(rig, "drift_monitoring_dag", params) as current:
        error = refuses(LaunchRefused, lambda: current.step("drift_run"))
    require("carries no host capacity binding" in str(error), f"the unbound drift revision is not named: {error}")
    after = [call for call in rig.aws_calls() if call.startswith("s3:Put")]
    require(len(after) == writes, f"a drift job refused for its unbound revision wrote before the refusal: {after[writes:]}")
    require(rig.tasks_started() == started, "an unbound drift revision launched")


def revision_bound_to_another_capacity_is_refused() -> None:
    """A revision bound to another usable memory (here the nominal 32768 MiB) is refused. So is a revision whose label names the current inputs but
    gives a job more than the room, and one whose task memory is not the job memory of its label (a label that does not describe its own revision)."""
    for memory, label, named in (
        (25600, NOMINAL_BINDING, ("bound to job_memory_mib=25600", CAPACITY)),
        (25600, BINDING.replace(f"job_memory_mib={JOB_MEMORY}", "job_memory_mib=25600"), ("more than the room",)),
        (25600, BINDING, ("25600 MiB", f"{JOB_MEMORY} MiB")),
    ):
        rig = Rig()
        rig.ecs.register("example_dev_job_silver_host", memory, label)
        with world(rig, "silver_transform_dag", SILVER) as current:
            error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
        require(all(part in str(error) for part in named), f"the refusal does not name {named}: {error}")
        require(rig.tasks_started() == 0 and not rig.records.calls, f"a revision with {label} and {memory} MiB launched or wrote a record")


def malformed_capacity_refuses() -> None:
    """The services stack always renders HAI_LAUNCH_HOST_CAPACITY (host_usable_memory_mib defaults to the current host size, owner decision Nov 10
    2026), so the Airflow environment requires it like every other launch setting. A capacity that is empty or not the rendered form is refused by
    name before any ECS call or record, and no cloud-reading gate masks it."""
    steps = importlib.import_module("orchestration.airflow_steps")
    require(steps.HOST_CAPACITY in steps.SETTINGS, "the rendered host capacity is not a required launch setting")
    environment = {name: value for name, value in launch_environment("example-scratch").items() if name != steps.HOST_CAPACITY}
    with mock.patch.dict("os.environ", environment, clear=True):
        error = refuses(Exception, steps.services)
    require(f"{steps.HOST_CAPACITY} is not set" in str(error), f"a missing host capacity setting is not named: {error}")
    for capacity in ("", BINDING):
        rig = Rig(capacity=capacity)
        with world(rig, "silver_transform_dag", SILVER) as current:
            error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
        require("not in the rendered form" in str(error), f"the malformed capacity {capacity!r} is not named: {error}")
        require("host_usable_memory_mib" not in str(error), f"the refusal still asks the owner to set the defaulted input: {error}")
        require(not rig.aws_calls(), f"a malformed capacity made an AWS call before the refusal: {rig.aws_calls()}")
    for dag_id, params, template in (
        ("silver_transform_dag", SILVER, "pin_bronze"),
        ("gold_transform_dag", GOLD, "pin_silver"),
        ("drift_monitoring_dag", {"run_id": DRIFT_RUN, "mode": "real", "mlflow": "aws", "model": "m1", "features_run": SET_FEATURES_RUN}, "drift_run"),
    ):
        rig = Rig(capacity="")
        with world(rig, dag_id, params) as current:
            error = refuses(LaunchRefused, partial(current.step, template))
        require("not in the rendered form" in str(error), f"{template}: a cloud-reading gate masked the malformed capacity: {error}")
        require(not rig.aws_calls(), f"{template}: a malformed capacity made an AWS call before the refusal: {rig.aws_calls()}")


CAP_SETTING = "HAI_LAUNCH_HOST_JOB_CAP_MIB"


def revision_above_the_host_job_cap_is_refused() -> None:
    """Host job cap setting (Nov 11 2026): the services stack renders the cap (the job room the owner chose) into the Airflow environment, and the
    preflight checks it at every host launch. A revision whose label gives a job more memory than that cap is refused by name; so is a launch while the
    cap is missing or not a whole number of MiB. Nothing is recorded and no task starts."""
    steps = importlib.import_module("orchestration.airflow_steps")
    require(CAP_SETTING in steps.SETTINGS, "the host job cap setting is not a required launch setting")
    for cap, named in (
        (str(JOB_MEMORY - 1), ("host job cap", str(JOB_MEMORY - 1), str(JOB_MEMORY))),
        (None, ("host job cap", CAP_SETTING, "missing")),
        ("23552.5", ("host job cap", "not a whole number")),
        ("", ("host job cap", "not a whole number")),
    ):
        rig = Rig(host_cap=cap)
        with world(rig, "silver_transform_dag", SILVER) as current:
            error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
        require(all(part in str(error) for part in named), f"the refusal for the cap {cap!r} does not name {named}: {error}")
        require(rig.cap_reads >= 1, f"the cap {cap!r} was not read")
        require(rig.tasks_started() == 0 and not rig.records.calls, f"a revision over the cap {cap!r} launched or wrote a record")


def revision_within_the_host_job_cap_runs() -> None:
    """A revision whose job memory equals the cap, or is below it, runs, and the preflight reads the cap once per launch."""
    for cap in (str(JOB_MEMORY), str(JOB_MEMORY + 1024)):
        rig = Rig(host_cap=cap)
        with world(rig, "silver_transform_dag", SILVER) as current:
            require(current.step("pin_bronze") == "succeeded", f"a revision within the cap {cap} did not run")
        require(rig.tasks_started() == 1 and rig.cap_reads == 1, f"the cap {cap} was not read once: {rig.cap_reads}")
    rig = Rig()
    rig.ecs.register("example_dev_job_silver_host", 20480, BINDING.replace(f"job_memory_mib={JOB_MEMORY}", "job_memory_mib=20480"))
    with world(rig, "silver_transform_dag", SILVER) as current:
        require(current.step("pin_bronze") == "succeeded", "a revision sized to a cap below the room did not run")


def launch_environment(scratch: str, cap: str | None = str(JOB_MEMORY), **extra: str) -> dict[str, str]:
    """The Airflow launch environment the services stack renders, in stand-in values: the scratch folder is a real folder, the capacity and the host job
    cap are in their rendered forms, and ``cap`` None leaves the cap setting out."""
    steps = importlib.import_module("orchestration.airflow_steps")
    environment = {name: scratch if name.endswith("SCRATCH_PATH") else f"example-{name.lower()}" for name in steps.SETTINGS}
    environment |= {"HAI_LAUNCH_HOST_CAPACITY": CAPACITY, **extra}
    environment.pop(CAP_SETTING, None)
    if cap is not None:
        environment[CAP_SETTING] = cap
    return environment


class RecordingSsm:
    """The SSM client of the production launch services when no parameter is meant to be read: it records every request and refuses it, so a reader that
    still asks for the host job cap fails the launch."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def get_parameter(self, **request: Any) -> dict[str, Any]:
        self.requests.append(dict(request))
        raise client_error("AccessDeniedException", "GetParameter", f"the launch role may not read {request.get('Name')}")


def production_services(ssm: Any, environment: dict[str, str]) -> Any:
    """The launch services that ``airflow_steps.services()`` builds from ``environment`` with a stand-in boto3 session whose SSM client is ``ssm``."""
    steps = importlib.import_module("orchestration.airflow_steps")

    class Session:
        def __init__(self, region_name: str) -> None:
            self.region = region_name

        def client(self, name: str, config: Any = None) -> Any:
            return ssm if name == "ssm" else FakeEcs() if name == "ecs" else FakeLogs() if name == "logs" else FakeS3()

    provider = SimpleNamespace(BrokerlessEcsRunTaskOperator=StandInLaunchOperator)
    with (
        mock.patch.dict("os.environ", environment, clear=True),
        mock.patch.dict(sys.modules, {"orchestration.ecs_launch_provider": provider}),
        mock.patch("boto3.session.Session", Session),
    ):
        return steps.services()


def check_production_cap_setting() -> None:
    """The production wiring, through the shipped preflight and DAG runtime: the cap is the value of the environment setting, a process with a cap
    below the revision refuses it, one with the cap at or above it runs it, no SSM call asks for the cap, and a missing or invalid setting fails the
    build of the launch services by its name."""
    for cap, runs in ((str(JOB_MEMORY), True), (str(JOB_MEMORY + 1024), True), (str(JOB_MEMORY - 1024), False)):
        ssm = RecordingSsm()
        with tempfile.TemporaryDirectory() as scratch:
            built = production_services(ssm, launch_environment(scratch, cap))
        require(built.preflight.host_job_cap() == cap, f"the production reader does not return the environment cap {cap}: {built.preflight.host_job_cap()!r}")
        rig = Rig()
        rig.preflight.host_job_cap = built.preflight.host_job_cap
        with world(rig, "silver_transform_dag", SILVER) as current:
            if runs:
                require(current.step("pin_bronze") == "succeeded", f"a revision at or below the environment cap {cap} did not run")
                require(rig.tasks_started() == 1, f"the launch under the cap {cap} started {rig.tasks_started()} tasks")
            else:
                error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
                require("host job cap" in str(error) and cap in str(error), f"a revision above the environment cap {cap} was not refused by name: {error}")
                require(rig.tasks_started() == 0 and not rig.records.calls, f"a revision above the environment cap {cap} launched or wrote a record")
        require(not ssm.requests, f"the host job cap was read through SSM: {ssm.requests}")
    room = 30720 - 2048  # the usable memory less the system reserve of CAPACITY (30720 and 2048): the most a cap can be before any service reservation
    for cap, runs in ((str(room), True), (str(room + 1), False), ("999999", False)):
        with tempfile.TemporaryDirectory() as scratch:
            built = production_services(RecordingSsm(), launch_environment(scratch, cap))
        rig = Rig()
        rig.preflight.host_job_cap = built.preflight.host_job_cap
        with world(rig, "silver_transform_dag", SILVER) as current:
            if runs:
                require(current.step("pin_bronze") == "succeeded", f"a cap at the room {cap} did not run")
            else:
                error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
                require(CAP_SETTING in str(error) and str(room) in str(error), f"the cap {cap} above the host room {room} is not refused by name: {error}")
                require(
                    not rig.aws_calls() and rig.tasks_started() == 0 and not rig.records.calls,
                    f"the cap {cap} above the host room reached the cloud: {rig.aws_calls()}",
                )
    for bad_cap in (None, "", "0", "-1", "23552.5", "many", "0x5c00"):
        with tempfile.TemporaryDirectory() as scratch:
            error = refuses(Exception, partial(production_services, RecordingSsm(), launch_environment(scratch, bad_cap)))
        require(CAP_SETTING in str(error), f"the launch services build did not name {CAP_SETTING} for the cap {bad_cap!r}: {type(error).__name__}: {error}")


def production_cap_comes_from_the_environment() -> None:
    """The launch suite runs the production reader and its wiring in ``airflow_steps.services()``, not a reader of its own."""
    check_production_cap_setting()


def a_constant_cap_reader_is_caught() -> None:
    """Mutation: the production cap reader swapped for a reader that always answers 999999 MiB whatever the environment says. The production checks
    must fail, so the suite cannot pass with the environment read removed."""
    preflight = importlib.import_module("orchestration.ecs_preflight")
    steps = importlib.import_module("orchestration.airflow_steps")

    def constant(_value: str) -> Callable[[], str | None]:
        return lambda: "999999"

    with mock.patch.object(preflight, "host_cap_reader", constant), mock.patch.object(steps, "host_cap_reader", constant):
        try:
            check_production_cap_setting()
        except AssertionError:
            return
    raise AssertionError("a constant 999999 MiB cap reader passed the production cap checks")


def migration_states_launch_nothing_and_the_procedure_is_written() -> None:
    """The move of the cap to the environment and every later cap change pass through mixed states, and none of them starts a task. New code with
    the old container environment (no cap setting) fails the build of the launch services by the setting's name before any AWS call, held or not. New
    code with the new environment and a revision above a lowered cap is refused by the revision check, and a revision at or below the cap runs. The
    owner procedure that keeps the hold until the jobs apply is written where the services apply command is documented, and names only commands that
    exist."""
    for hold in ("clear", "held"):
        ssm = RecordingSsm()
        with tempfile.TemporaryDirectory() as scratch:
            environment = launch_environment(scratch, None, HAI_LAUNCH_HOST_HOLD_PARAMETER=f"{HOLD_PARAMETER}_{hold}")
            error = refuses(Exception, partial(production_services, ssm, environment))
        require(
            CAP_SETTING in str(error) and "not set" in str(error), f"new code with the old environment ({hold} hold) is not refused by the setting: {error}"
        )
        require(not ssm.requests, f"new code with the old environment ({hold} hold) made an SSM call: {ssm.requests}")
    old_revision = BINDING.replace(f"job_memory_mib={JOB_MEMORY}", "job_memory_mib=23552")
    for cap, runs in ((str(JOB_MEMORY), True), (str(JOB_MEMORY - 1024), False)):
        with tempfile.TemporaryDirectory() as scratch:
            built = production_services(RecordingSsm(), launch_environment(scratch, cap))
        rig = Rig()
        rig.ecs.register("example_dev_job_silver_host", JOB_MEMORY, old_revision)
        rig.preflight.host_job_cap = built.preflight.host_job_cap
        with world(rig, "silver_transform_dag", SILVER) as current:
            if runs:
                require(current.step("pin_bronze") == "succeeded", f"the old revision did not run under the cap {cap}")
            else:
                error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
                require(
                    "Render and apply the jobs stack" in str(error) and rig.tasks_started() == 0,
                    f"the old revision was not refused under the lowered cap: {error}",
                )
    apply = (ROOT / "scripts/infrastructure/services_apply.py").read_text()
    wrapper = (ROOT / "scripts/infrastructure/terraform.sh").read_text()
    text = apply[apply.index("Changing the host job cap") : apply.index("Run from the repository root")]
    require(
        "--keep-launch-hold" in wrapper and "--continue-launch-hold" in wrapper and "--keep-hold" in apply,
        "the procedure names a hold option that the commands lack",
    )
    order = (
        "services_apply status",
        "plan -out=host_job_cap.tfplan",
        '--keep-launch-hold --jobs-inputs "$JOBS_INPUTS" --jobs-sha256 "$JOBS_SHA" apply',
        '--iam --admin-profile "$ADMIN_PROFILE" apply',
        "manage_policies apply",
        '"${JOBS_FLAGS[@]}" apply',
        "services_apply finish",
    )
    where = [text.find(part) for part in order]
    require(all(position >= 0 for position in where) and where == sorted(where), f"the owner procedure is incomplete or out of order: {where}")
    require("--preparation-inputs" in text and "--preparation-sha256" in text, "the procedure leaves out the preparation inputs of the reviewed plan")
    # Every command of the procedure is run as written by scripts.infrastructure.run_activation_inputs_e2e (procedure_cases).


def cap_setting_is_rendered_by_the_services_stack() -> None:
    """The services stack renders the cap into every Airflow container from its one variable, the job room it hands to the jobs stack is that variable,
    and the stack manages no Parameter Store parameter."""
    airflow = (ROOT / "infra/services/airflow.tf").read_text()
    main = (ROOT / "infra/services/main.tf").read_text()
    variables = (ROOT / "infra/services/variables.tf").read_text()
    require(
        re.search(r"HAI_LAUNCH_HOST_JOB_CAP_MIB\s*=\s*tostring\(local\.host_job_memory_mib\)", airflow) is not None,
        "the services stack does not render the cap into the Airflow environment from the job memory local",
    )
    require(
        re.search(r"host_job_memory_mib\s*=\s*var\.host_job_cap_mib\b", main) is not None,
        "the job memory local is not the one variable host_job_cap_mib",
    )
    require('variable "host_job_cap_mib"' in variables, "the services stack has no host_job_cap_mib variable")
    require("aws_ssm_parameter" not in main + airflow, "the services stack still manages a Parameter Store parameter")


HOLD_PARAMETER = "/healthcare_hai_prediction/host_launch_hold"
LOCK_PARAMETER = "/healthcare_hai_prediction/services_apply_lock"
CURRENT_CODE_PARAMETER = "/healthcare_hai_prediction/current_code_run"
APPLY_LOCK_VALUE = "apply-host:4242:20261110T230000Z:0badc0de"  # an apply lock value in its token shape, not a credential
CLEAR = "clear"
HELD = "held by services_apply at 2026-11-10T20:00:00Z"


class LaunchSsm:
    """The SSM client of the production launch services for the launch hold and the apply lock: a queue of answers per name (the last repeats),
    every request recorded in order, and an access refusal for any other name (the host job cap is not a parameter)."""

    def __init__(self, answers: Mapping[str, Sequence[str | Exception]]) -> None:
        self.answers = {name: list(values) for name, values in answers.items()}
        self.requests: list[dict[str, Any]] = []
        self.slow_after: int | None = None  # reads after this many take slow_seconds on slow_clock
        self.slow_seconds = 0.0
        self.slow_clock: Any = None

    def get_parameter(self, **request: Any) -> dict[str, Any]:
        self.requests.append(dict(request))
        if self.slow_after is not None and len(self.requests) > self.slow_after:
            self.slow_clock.sleep(self.slow_seconds)
        queue = self.answers.get(str(request.get("Name")))
        if not queue:
            raise client_error("AccessDeniedException", "GetParameter", f"the launch role may not read {request.get('Name')}")
        answer = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(answer, Exception):
            raise answer
        return {"Parameter": {"Name": request["Name"], "Type": "String", "Value": answer}}

    def reads(self, name: str) -> int:
        return sum(1 for item in self.requests if item == {"Name": name})


def no_lock() -> list[str | Exception]:
    """The apply lock answers of a stack with no services apply running: the parameter does not exist."""
    return [client_error("ParameterNotFound", "GetParameter")]


def held_rig(hold: Sequence[str | Exception], lock: Sequence[str | Exception] | None = None, cap: str = str(JOB_MEMORY)) -> tuple[Rig, LaunchSsm]:
    """A rig whose preflight hold and cap readers and whose fit probe wrapper are the production ones from ``airflow_steps.services()``, wired to a
    stand-in SSM client for the hold and the apply lock and to the environment cap ``cap``. Before the launch hold exists the production pieces are
    missing and the case fails on that, not on a type error."""
    steps = importlib.import_module("orchestration.airflow_steps")
    ssm = LaunchSsm({HOLD_PARAMETER: hold, LOCK_PARAMETER: no_lock() if lock is None else lock})

    with tempfile.TemporaryDirectory() as scratch:
        built = production_services(
            ssm, launch_environment(scratch, cap, HAI_LAUNCH_HOST_HOLD_PARAMETER=HOLD_PARAMETER, HAI_LAUNCH_APPLY_LOCK_PARAMETER=LOCK_PARAMETER)
        )
    require("HAI_LAUNCH_HOST_HOLD_PARAMETER" in steps.SETTINGS, "the launch hold parameter name is not a required launch setting")
    hold_reader = getattr(built.preflight, "host_launch_hold", None)
    require(callable(hold_reader), "the production preflight has no launch hold reader")
    require(callable(getattr(steps, "host_fit_probe", None)), "airflow_steps has no host_fit_probe that rechecks the hold and the cap before RunTask")
    probe = built.launcher.probe
    require(
        getattr(probe, "hold", None) is hold_reader and getattr(probe, "cap", None) is built.preflight.host_job_cap,
        "the production fit probe does not recheck the production hold and cap readers",
    )
    require(not ssm.requests, f"building the launch services read SSM before any launch: {ssm.requests}")
    rig = Rig()
    rig.preflight.host_job_cap = built.preflight.host_job_cap
    rig.preflight.host_launch_hold = hold_reader
    fitted = steps.host_fit_probe(rig.launcher.probe, hold_reader, built.preflight.host_job_cap)
    fitted.monotonic = rig.time.clock().monotonic  # the rig's virtual clock in place of the process clock
    rig.launcher.probe = fitted
    rig.launcher.send_check = getattr(fitted, "authorize", None)
    rig.built = built
    return rig, ssm


def the_production_launcher_rechecks_at_send() -> None:
    """The launcher that ``airflow_steps.services()`` builds rereads the hold and the cap at the send boundary: its ``send_check`` is the fit
    probe's ``authorize``."""
    rig, _ = held_rig([CLEAR])
    launcher = rig.built.launcher
    check = getattr(launcher, "send_check", None)
    require(
        check is not None and getattr(check, "__self__", None) is launcher.probe and getattr(check, "__name__", "") == "authorize",
        "the production launcher does not recheck the hold and the cap at the send boundary (Launcher.send_check of the fit probe)",
    )


def launch_during_the_hold_is_refused() -> None:
    """Launch hold during services applies (owner A, Nov 10 2026): while the hold is set, or while the hold parameter is missing (fail closed), the
    preflight refuses every host launch by name before any record or ECS call: zero tasks."""
    structured = json.dumps({"state": "held", "by": "services_apply", "at": "2026-11-10T20:00:00Z", "reason": "clear"})
    pending = json.dumps({"state": "held for a host job cap change", "by": "services_apply", "at": "2026-11-10T20:00:00Z", "reason": "jobs apply pending"})
    for hold, named in (
        ([HELD], "launch hold"),
        ([structured], "launch hold"),
        ([pending], "launch hold"),
        ([client_error("ParameterNotFound", "GetParameter")], "launch hold"),
    ):
        rig, ssm = held_rig(hold)
        with world(rig, "silver_transform_dag", SILVER) as current:
            error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
        require(named in str(error), f"the hold {hold[0]!r} is not named: {error}")
        require(ssm.reads(HOLD_PARAMETER) >= 1, f"the hold was not read: {ssm.requests}")
        require(rig.tasks_started() == 0 and not rig.records.calls and not rig.ecs.count("run_task"), "a launch went ahead during the hold")


def a_hold_set_after_the_preflight_withholds_runtask() -> None:
    """Forced order: the preflight reads a clear hold, then the services apply sets the hold before the launcher's fit check. The fit probe rereads
    the hold before every RunTask, so no RunTask is sent; the launch closes without a task when its window ends."""
    rig, ssm = held_rig([CLEAR, HELD])
    with world(rig, "silver_transform_dag", SILVER) as current:
        refuses(Exception, lambda: current.step("pin_bronze"))
    require(ssm.reads(HOLD_PARAMETER) >= 2, f"the fit probe did not reread the hold: {ssm.requests}")
    require(rig.ecs.count("run_task") == 0 and rig.tasks_started() == 0, "RunTask was sent after the hold was set")


def a_hold_set_after_the_probe_withholds_runtask() -> None:
    """Forced order: the preflight and the fit probe read a clear hold, then the services apply sets the hold before the send. The send boundary
    rereads the hold immediately before RunTask, so no RunTask is sent."""
    rig, ssm = held_rig([CLEAR, CLEAR, HELD])
    with world(rig, "silver_transform_dag", SILVER) as current:
        refuses(Exception, lambda: current.step("pin_bronze"))
    require(ssm.reads(HOLD_PARAMETER) >= 3, f"the send boundary did not reread the hold: {ssm.requests}")
    require(rig.ecs.count("run_task") == 0 and rig.tasks_started() == 0, "RunTask was sent after the hold was set behind the probe")


def a_slow_authorization_withholds_runtask() -> None:
    """The send boundary's reads are bounded: when they take longer than the authorization window, the send is withheld, so the time from the
    observed clear hold to the RunTask reply stays below the services apply command's drain wait."""
    preflight = importlib.import_module("orchestration.ecs_preflight")
    adapter = importlib.import_module("orchestration.ecs_adapter")
    apply = importlib.import_module("scripts.infrastructure.services_apply")
    window = getattr(preflight, "SEND_AUTHORIZATION_SECONDS", None)
    if window is None:
        raise AssertionError("the preflight has no SEND_AUTHORIZATION_SECONDS")
    config = adapter.send_config()
    require(
        window + config.connect_timeout + config.read_timeout < apply.HOLD_SETTLE_SECONDS,
        f"authorization {window} s plus one RunTask attempt is not below the drain wait {apply.HOLD_SETTLE_SECONDS} s",
    )
    rig, ssm = held_rig([CLEAR])
    ssm.slow_after, ssm.slow_seconds, ssm.slow_clock = 2, window + 1, rig.time
    with world(rig, "silver_transform_dag", SILVER) as current:
        refuses(Exception, lambda: current.step("pin_bronze"))
    require(rig.ecs.count("run_task") == 0, "RunTask was sent although the authorization reads took longer than the window")


def a_present_or_unreadable_apply_lock_refuses_launch() -> None:
    """Apply lock in the launch authorization (round 4, owner A): a services apply that crashed between taking the lock and setting the hold
    (the hold still reads clear), or between settle's clear and the lock delete, leaves the lock present; an unreadable lock is not known to be
    absent. Each refuses every host launch at the preflight, by name, with zero tasks."""
    for label, lock in (
        ("crash between lock and hold", [APPLY_LOCK_VALUE]),
        ("crash between the hold clear and the lock delete", [APPLY_LOCK_VALUE]),
        ("lock unreadable", [client_error("AccessDeniedException", "GetParameter")]),
    ):
        rig, ssm = held_rig([CLEAR], lock)
        with world(rig, "silver_transform_dag", SILVER) as current:
            error = refuses(Exception, lambda: current.step("pin_bronze"))
        require(ssm.reads(LOCK_PARAMETER) >= 1, f"{label}: the apply lock was not read: {ssm.requests}")
        require("apply lock" in str(error) or "launch_hold_unreadable" in str(error), f"{label}: the refusal does not name the lock: {error}")
        require(rig.tasks_started() == 0 and not rig.ecs.count("run_task"), f"{label}: a launch went ahead")


def an_apply_lock_taken_after_the_probe_withholds_runtask() -> None:
    """Forced order: the preflight and the fit probe find no apply lock, then a services apply takes the lock before the send. The send
    boundary rereads the lock, so no RunTask is sent."""
    missing = client_error("ParameterNotFound", "GetParameter")
    rig, ssm = held_rig([CLEAR], [missing, missing, APPLY_LOCK_VALUE])
    with world(rig, "silver_transform_dag", SILVER) as current:
        refuses(Exception, lambda: current.step("pin_bronze"))
    require(ssm.reads(LOCK_PARAMETER) >= 3, f"the send boundary did not reread the apply lock: {ssm.requests}")
    require(rig.ecs.count("run_task") == 0 and rig.tasks_started() == 0, "RunTask was sent after a services apply took the lock")


def a_padded_clear_value_holds_launches() -> None:
    """The hold is clear only for the exact value "clear": a padded or differently written value holds every host launch."""
    for value in (" clear ", "clear\n", "Clear", "CLEAR"):
        rig, _ = held_rig([value])
        with world(rig, "silver_transform_dag", SILVER) as current:
            error = refuses(LaunchRefused, lambda: current.step("pin_bronze"))
        require("launch hold" in str(error) and rig.tasks_started() == 0, f"the hold value {value!r} let a launch run: {error}")


def the_fit_probe_never_offers_more_than_the_cap() -> None:
    """The fit probe the launcher asks before every RunTask gives the job no more memory than the environment cap, even when the host has more free."""
    cap = JOB_MEMORY - 1024
    rig, _ = held_rig([CLEAR], cap=str(cap))
    probe = rig.launcher.probe
    if not isinstance(probe, HeldCapProbe):
        raise AssertionError(f"the launcher's fit probe is {type(probe).__name__}, not the probe that holds the cap")
    free = probe.inner.free().memory_mib
    require(free > cap, f"the stand-in host has no more free memory than the cap {cap}: {free}")
    require(probe.free().memory_mib == cap, f"the fit probe offers {probe.free().memory_mib} MiB, not the cap {cap}")


def a_cleared_hold_lets_the_launch_run() -> None:
    """After the apply reaches steady state the hold is cleared: the launch runs, and the hold is read by the preflight and before the RunTask."""
    rig, ssm = held_rig([CLEAR])
    with world(rig, "silver_transform_dag", SILVER) as current:
        require(current.step("pin_bronze") == "succeeded", "a launch with a clear hold did not run")
    require(rig.tasks_started() == 1, "a launch with a clear hold did not start exactly one task")
    require(ssm.reads(HOLD_PARAMETER) >= 2, f"the hold was not read twice: {ssm.requests}")


def bound_revision_runs_by_its_exact_arn() -> None:
    """A bound revision is described once and runs by its exact ARN. A revision registered after the check (here an unbound one, made latest while
    the task starts) never runs in its place: the RunTask call names the checked revision, not the family."""
    rig = Rig()
    checked = rig.bound["example_dev_job_silver_host"]

    def register_late(operation: str) -> None:
        if operation == "list_tasks" and rig.ecs.count("describe_task_definition") == 1:
            rig.ecs.register("example_dev_job_silver_host", 25600, None)
            rig.ecs.call_hook = None

    rig.ecs.call_hook = register_late
    with world(rig, "silver_transform_dag", SILVER) as current:
        require(current.step("pin_bronze") == "succeeded", "the bound revision did not run")
    require(rig.ecs.sent == [checked], f"the task did not run the checked revision: {rig.ecs.sent}")
    require(rig.ecs.count("describe_task_definition") == 1, "the revision was not resolved exactly once")
    require(rig.ecs.revisions["example_dev_job_silver_host"]["taskDefinitionArn"] != checked, "the case did not register a later revision")


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
    """The chain's three layers, one model's features DAG, its drift check and its training run, each launched once and each run bound to the bronze code."""
    rig = Rig()
    layers = derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    gold = layers(silver, "gold")
    features = layers(gold, "features")
    model_features = model_run(features, "m1")
    drift = drift_run_of(model_features)
    training = training_run_of(drift, "m1")
    with world(rig, PIPELINE_DAG, {"run_id": PIPELINE_RUN, "mode": "real"}) as current:
        require(current.step("bind_code") == "succeeded", "the binding gate failed")
    plan = (
        ("silver_transform_dag.py", "silver_transform_dag", {"run_id": silver, "mode": "real"}, "silver", silver),
        ("gold_transform_dag.py", "gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}, "gold", gold),
        ("features_base_dag.py", "features_base_dag", {"run_id": features, "mode": "real", "gold_snapshot_run": gold}, "features", features),
        ("features_m1_dag.py", "features_m1_dag", {"run_id": model_features, "mode": "real", "base_snapshot_run": features}, "features_m1", model_features),
    )
    for file, dag_id, params, job, run in plan:
        with world(rig, dag_id, params) as current:
            states = current.run_dag(file)
            failed = {name: state for name, state in states.items() if state != "success"}
            last = ("gate", "accepted") if file in COLLAPSED_FILES else ("release_run", "released")
            require(not failed and current.results[last[0]] == last[1], f"{dag_id} did not finish: {failed} {current.results}")
        end = rig.end(run, job)
        require(end is not None and end["outcome"] == "succeeded", f"{job} has no success record")
    with world(rig, "drift_monitoring_dag", {"run_id": drift, "mode": "real", "mlflow": "aws", "model": "m1", "features_run": model_features}) as current:
        require(current.step("drift_run") == "succeeded", "the drift job did not pass")
    with world(
        rig, "training_dag", {**TRAIN_PARAMS, "run_id": training, "features_run": model_features, "trigger": f"drift:{drift}", "model": "m1"}
    ) as current:
        states = current.run_dag("training_dag.py")
        require(states.pop("start_metadata") == "skipped", "a hand-started training run with no base run started the catalog")
        failed = {name: state for name, state in states.items() if state != "success"}
        require(not failed and current.results["release_run"] == "released", f"training_dag did not finish: {failed} {current.results}")
    require(rig.tasks_started() == 6, f"one task per job was expected: {rig.tasks_started()} tasks")
    require(
        all((rig.end(run, job) or {}).get("outcome") == "succeeded" for run, job in ((drift, "drift"), (training, "train"))),
        "drift or training has no success record",
    )
    again = rig.tasks_started()
    with world(rig, "gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}) as current:
        current.run_dag("gold_transform_dag.py")
    require(rig.tasks_started() == again, "a second run of the same DAG run launched again")


def form_mode_defaults() -> dict[str, Any]:
    """The default of the ``mode`` parameter in each shipped DAG file's trigger form, read from the file's ``Param`` call."""
    found: dict[str, Any] = {}
    for path in sorted((ROOT / "dags").glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values, strict=True):
                    if isinstance(key, ast.Constant) and key.value == "mode" and isinstance(value, ast.Call) and value.args:
                        found[path.stem] = static_value(value.args[0])
    return found


# Values of every JSON type but string, for a parameter the form declares as a string.
NON_STRINGS: tuple[Any, ...] = (None, 20261104, 1.5, True, [GOLD_RUN], {"run": GOLD_RUN})
PRODUCER_RUNS = (
    ("gold_transform_dag.py", "silver_snapshot_run", "gold_0f0f0f0f", SILVER_RUN),
    ("features_base_dag.py", "gold_snapshot_run", "features_0f0f0f0f", GOLD_RUN),
)


def dags_run_with_their_form_defaults() -> None:
    """Each DAG triggered with only its required inputs runs to success on the direct path (failure mode 3): no form default is a mode the preflight
    refuses, and a DAG whose job publishes from a producer run requires that run as a run ID in its form, so a trigger without it is refused at the form
    before any launch instead of failing its preflight."""
    defaults = form_mode_defaults()
    accepted = {"model_promotion_dag": "promote"}  # the promotion form's mode is the operation; the step maps it to real
    wrong = {dag: mode for dag, mode in defaults.items() if mode != accepted.get(dag, "real")}
    require(len(defaults) >= 10 and not wrong, f"DAG forms default to a mode the direct path refuses: {wrong}")
    for dag_file, name, _, sample in PRODUCER_RUNS:
        declared = form_parameter(dag_file, name)
        arguments, settings = declared or ([], {})
        pattern = str(settings.get("pattern", ""))
        require(declared is not None and not arguments, f"{dag_file} does not require {name}: {declared}")
        require(
            bool(pattern) and re.search(pattern, sample) is not None and not any(re.search(pattern, bad) for bad in ("latest", "", "x" + sample)),
            f"{dag_file}'s {name} pattern does not accept exactly a run ID: {pattern}",
        )
    rig = Rig()
    layers = derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    gold = layers(silver, "gold")
    features = layers(gold, "features")
    training = str(TRAIN_PARAMS["run_id"])
    with world(rig, PIPELINE_DAG, {"run_id": PIPELINE_RUN, "mode": "real"}) as current:
        require(current.step("bind_code") == "succeeded", "the binding gate failed")
    plan = (
        ("silver_transform_dag", {"run_id": silver}, "silver", silver),
        ("gold_transform_dag", {"run_id": gold, "silver_snapshot_run": silver}, "gold", gold),
        ("features_base_dag", {"run_id": features, "gold_snapshot_run": gold}, "features", features),
        ("training_dag", {"run_id": training, "model": "m1"}, "train", training),
    )
    for dag_id, params, job, run in plan:
        rig.stage_runs(run)
        before = rig.tasks_started()
        with world(rig, dag_id, dict(params)) as current:
            states = current.run_dag(f"{dag_id}.py")
            if dag_id == "training_dag":
                require(states.pop("start_metadata") == "skipped", "a training run with no base run started the catalog")
            failed = {name: current.results[name] for name, state in states.items() if state != "success"}
            require(bool(states) and not failed, f"{dag_id} with only its required inputs did not finish: {failed}")
        require(rig.tasks_started() == before + 1, f"{dag_id} started {rig.tasks_started() - before} tasks, not one")
        require((rig.end(run, job) or {}).get("outcome") == "succeeded", f"{dag_id}: {job} has no success record")
    for dag_file, name, own_run, _ in PRODUCER_RUNS:
        dag_id = dag_file.removesuffix(".py")
        # Omitted, latest, and values of another JSON type than the declared string: each is refused at the form.
        triggers: list[dict[str, Any]] = [
            {"run_id": own_run},
            {"run_id": own_run, name: "latest"},
            *({"run_id": own_run, name: value} for value in NON_STRINGS),
        ]
        for params in triggers:
            rig = Rig()
            rig.stage_runs(own_run)
            with world(rig, dag_id, params) as current:
                error = refuses(FormRefused, partial(current.run_dag, dag_file))
                require(name in str(error) and not current.states, f"{dag_id} with {params} was not refused at the form: {error}")
            require(
                not rig.ecs.calls and not rig.records.calls and not rig.stage.reads and not rig.stage.puts,
                f"{dag_id} with {params} reached ECS, the launch records or the staged inputs",
            )


COLLAPSED_FILES = (
    "silver_transform_dag.py",
    "gold_transform_dag.py",
    "features_base_dag.py",
    *(f"features_m{index}_dag.py" for index in range(1, 7)),
    "model_promotion_dag.py",
    "serving_deploy_dag.py",
)
COLLAPSED_TASKS = ["preflight", "job", "evidence", "gate"]
PROMOTE_PARAMS = {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "aws"}


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
    job_task = importlib.import_module("orchestration.airflow_steps").JOB_TASK
    require(COLLAPSED_TASKS[1] == job_task, f"the evidence step names the job task {job_task!r}, not {COLLAPSED_TASKS[1]!r} of the chain")
    rig = Rig()
    plan: tuple[tuple[str, dict[str, Any], str, str, list[str]], ...] = (
        ("silver_transform_dag", SILVER, "silver", SILVER_RUN, []),
        ("gold_transform_dag", GOLD, "gold", GOLD_RUN, ["gold"]),
        ("model_promotion_dag", PROMOTE_PARAMS, "promote", PROMOTE_RUN, []),
        ("serving_deploy_dag", SERVE_FORM, "serve", SERVE_RUN, []),
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


BASE_DAG_RUN = "features_5a5a5a5a"
GOLD_RUN_PARAMETER = "gold_snapshot_run"


def training_form_requires_a_model() -> None:
    """The training DAG's form requires the model a run fits, with no default and exactly the contract's models: the launch step reads params["model"],
    so a form trigger without it raised KeyError and launched nothing."""
    models = list(importlib.import_module("orchestration.training_contract").MODELS)
    declared = form_parameter("training_dag.py", "model")
    require(declared is not None, "training_dag declares no model parameter")
    arguments, settings = declared or ([], {})
    require(not arguments and "default" not in settings, f"the training form gives the model a default: {declared}")
    require(settings.get("enum") == models, f"the training form's models differ from the contract's: {settings.get('enum')} {models}")


def form_parameter(dag_file: str, name: str) -> tuple[list[Any], dict[str, Any]] | None:
    """The positional and keyword arguments of the ``Param`` call that declares ``name`` in a shipped DAG file's trigger form."""
    for node in ast.walk(ast.parse((ROOT / "dags" / dag_file).read_text())):
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if isinstance(key, ast.Constant) and key.value == name and isinstance(value, ast.Call):
                    return [static_value(item) for item in value.args], keyword_settings(value)
    return None


def keyword_settings(call: ast.Call) -> dict[str, Any]:
    """The keyword arguments of a ``Param`` call: its literals, and the keys that ``**model_param_schema()`` spreads in. Any other spread is refused, so that
    a key a case reads is never silently missing."""
    settings: dict[str, Any] = {}
    for item in call.keywords:
        if item.arg:
            settings[item.arg] = static_value(item.value)
        elif isinstance(item.value, ast.Call) and isinstance(item.value.func, ast.Name) and item.value.func.id == "model_param_schema":
            settings.update(model_param_schema())
        else:
            raise ValueError(f"a Param call spreads keys that the case cannot read: {ast.unparse(item.value)}")
    return settings


def base_dag_takes_its_gold_run() -> None:
    """The features base DAG launches the features job, whose preflight needs the passed gold run it publishes from. The form requires that run
    (a run ID, not latest), the run reaches the launch, and a trigger without it is refused before any launch record or ECS call."""
    declared = form_parameter("features_base_dag.py", GOLD_RUN_PARAMETER)
    require(declared is not None, f"features_base_dag declares no {GOLD_RUN_PARAMETER} parameter")
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
    rig.stage_runs(BASE_DAG_RUN)
    layers = derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    gold = layers(silver, "gold")
    with world(rig, PIPELINE_DAG, {"run_id": PIPELINE_RUN, "mode": "real"}) as current:
        require(current.step("bind_code") == "succeeded", "the binding gate failed")
    for dag_id, params in (("silver_transform_dag", {"run_id": silver}), ("gold_transform_dag", {"run_id": gold, "silver_snapshot_run": silver})):
        with world(rig, dag_id, params) as current:
            current.run_dag(f"{dag_id}.py")
            require(current.results.get("gate") == "accepted", f"{dag_id} did not pass: {current.results}")
    before = rig.tasks_started()
    with world(rig, "features_base_dag", {"run_id": BASE_DAG_RUN, GOLD_RUN_PARAMETER: gold}) as current:
        states = current.run_dag("features_base_dag.py")
        failed = {name: current.results[name] for name, state in states.items() if state != "success"}
        require(bool(states) and not failed, f"features_base_dag with a passed gold run did not finish: {failed}")
    require(rig.tasks_started() == before + 1, f"features_base_dag started {rig.tasks_started() - before} tasks, not one")
    claim = rig.launcher.store.run_record(BASE_DAG_RUN, "features", "claim")
    require((rig.end(BASE_DAG_RUN, "features") or {}).get("outcome") == "succeeded" and claim is not None, "the features job of the base run has no success")
    rig = Rig()
    rig.stage_runs(BASE_DAG_RUN)
    with world(rig, "features_base_dag", {"run_id": BASE_DAG_RUN}) as current:
        error = refuses(FormRefused, lambda: current.run_dag("features_base_dag.py"))
        require(GOLD_RUN_PARAMETER in str(error) and not current.states, f"features_base_dag without a gold run was not refused at the form: {error}")
    with world(rig, "features_base_dag", {"run_id": BASE_DAG_RUN, GOLD_RUN_PARAMETER: BASE_DAG_RUN}) as current:
        error = refuses(FormRefused, lambda: current.run_dag("features_base_dag.py"))
        require(GOLD_RUN_PARAMETER in str(error) and not current.states, f"a features run as the gold run was not refused at the form: {error}")
    rig.stage_runs("gold_5a5a5a5a")
    with world(rig, "features_base_dag", {"run_id": BASE_DAG_RUN, GOLD_RUN_PARAMETER: "gold_5a5a5a5a"}) as current:
        states = current.run_dag("features_base_dag.py")
        refused = [str(result) for result in current.results.values() if "needs the gold job" in str(result)]
        require(states.get("gate") == "failed" and bool(refused), f"a gold run that never passed was not refused by the preflight: {current.results}")
    require(
        rig.ecs.count("run_task") == 0 and rig.records.count("put_object") == 0,
        "a features base run without a passed gold run started a task or wrote a launch record",
    )


def features_job_names_its_feature_set() -> None:
    """The features job's containers read FEATURE_SET, and the launch library names it from the DAG that starts the job: the base DAG launches base. A DAG
    that names no feature set cannot launch the features job, so a job never runs the single path by default."""
    layers = derived_run_id
    gold = layers(layers(BRONZE_RUN, "silver"), "gold")
    rig = Rig()
    rig.stage_runs(BASE_DAG_RUN)
    with world(rig, PIPELINE_DAG, {"run_id": PIPELINE_RUN, "mode": "real"}) as current:
        require(current.step("bind_code") == "succeeded", "the binding gate failed")
    for dag_id, params in (
        ("silver_transform_dag", {"run_id": layers(BRONZE_RUN, "silver")}),
        ("gold_transform_dag", {"run_id": gold, "silver_snapshot_run": layers(BRONZE_RUN, "silver")}),
    ):
        with world(rig, dag_id, params) as current:
            current.run_dag(f"{dag_id}.py")
    before = rig.tasks_started()
    with world(rig, "features_base_dag", {"run_id": BASE_DAG_RUN, GOLD_RUN_PARAMETER: gold}) as current:
        current.run_dag("features_base_dag.py")
    sent = rig.environment(before)
    named = {container: values["FEATURE_SET"] for container, values in sent.items() if "FEATURE_SET" in values}
    require(
        set(named) == {"pin", "build", "validate", "export", "publish", "upload", "feast"} and set(named.values()) == {"base"},
        f"the features containers did not all get FEATURE_SET=base: {named}",
    )
    phases = {container: values["LAYER_PHASE"] for container, values in sent.items() if "LAYER_PHASE" in values}
    require(
        set(phases) == {"pin", "build", "validate", "export", "publish", "upload", "feast"} and set(phases.values()) == {""},
        f"a features build was told to run one phase only: {phases}",
    )
    require(
        "release" not in sent and sent["feast"]["RELEASE_MODEL"] == "" and sent["feast"]["FEATURES_RUN"] == "",
        f"a features build was told to release a model set's online load: {sent}",
    )
    rig.stage_runs("gold_0f0f0f0f")
    started = rig.tasks_started()
    with world(rig, "gold_transform_dag", {"run_id": "gold_0f0f0f0f", "mode": "real", GOLD_RUN_PARAMETER: gold}) as current:
        error = refuses(LaunchRefused, lambda: current.step("pin_gold"))
    require("feature set" in str(error) and rig.tasks_started() == started, f"a DAG that names no feature set launched the features job: {error}")


BASE_RUN_PARAMETER = "base_snapshot_run"
MODEL_DAGS = {f"features_{model}_dag": model for model in ("m1", "m2", "m3", "m4", "m5", "m6")}
FEATURE_CONTAINERS = {"pin", "build", "validate", "export", "publish", "upload", "feast"}


def base_run_passed(rig: Rig, base_fails: bool = False) -> tuple[str, str]:
    """Run the binding gate, silver, gold and the features base DAG over the rig and return the gold run and the base run that its features job published.
    With ``base_fails`` the base DAG's job fails instead, and the base DAG is left to the caller to run again for its states."""
    layers = derived_run_id
    silver = layers(BRONZE_RUN, "silver")
    gold = layers(silver, "gold")
    base = layers(gold, "features")
    with world(rig, PIPELINE_DAG, {"run_id": PIPELINE_RUN, "mode": "real"}) as current:
        require(current.step("bind_code") == "succeeded", "the binding gate failed")
    for dag_id, params in (
        ("silver_transform_dag", {"run_id": silver}),
        ("gold_transform_dag", {"run_id": gold, "silver_snapshot_run": silver}),
        ("features_base_dag", {"run_id": base, GOLD_RUN_PARAMETER: gold}),
    ):
        if base_fails and dag_id == "features_base_dag":
            return gold, base
        with world(rig, dag_id, params) as current:
            current.run_dag(f"{dag_id}.py")
            require(current.results.get("gate") == "accepted", f"{dag_id} did not pass: {current.results}")
    return gold, base


def model_run(base: str, model: str) -> str:
    return str(importlib.import_module("orchestration.training_contract").model_features_run_id(base, model))


def drift_run_of(features_run: str) -> str:
    """The drift run that checks one model run's set: derived from that run."""
    return str(importlib.import_module("orchestration.training_contract").model_drift_run_id(features_run))


def training_run_of(drift_run: str, model: str) -> str:
    """The training run that a drift run starts for its model."""
    return str(importlib.import_module("orchestration.training_contract").model_training_run_id(drift_run, model))


def model_chain_passed(rig: Rig, model: str) -> tuple[str, str]:
    """Run the chain through the base DAG and one model's features DAG over the rig, and return the base run and the model run that passed."""
    _, base = base_run_passed(rig)
    run = model_run(base, model)
    with world(rig, f"features_{model}_dag", {"run_id": run, "mode": "real", BASE_RUN_PARAMETER: base}) as current:
        current.run_dag(f"features_{model}_dag.py")
        require(current.results.get("gate") == "accepted", f"features_{model}_dag did not pass: {current.results}")
    return base, run


def model_dags_are_one_chain_each() -> None:
    """Each model DAG is the one-job features chain for its set: one ECS task, every features container told FEATURE_SET=<model> and the base run as the
    producer run, the graph rules accepted, and no asset carried (the base DAG's gate carries the features asset)."""
    graph = importlib.import_module("orchestration.graph")
    rig = Rig()
    _, base = base_run_passed(rig)
    for dag_id, model in MODEL_DAGS.items():
        run = model_run(base, model)
        before = rig.tasks_started()
        with world(rig, dag_id, {"run_id": run, "mode": "real", BASE_RUN_PARAMETER: base}) as current:
            states = current.run_dag(f"{dag_id}.py")
            nodes = {node.task_id: node for node in current.built}
            require(list(nodes) == [*COLLAPSED_TASKS, "trigger_drift"], f"{dag_id} is not preflight, job, evidence, gate and the drift trigger: {list(nodes)}")
            violations = graph.validate_transform_graph(task_graph(current), dag_id)
            require(not violations, f"{dag_id} breaks the graph rules: {violations}")
            require(not any(node.settings.get("outlets") for node in nodes.values()), f"{dag_id} carries an asset: the base DAG's gate carries it")
            failed = {name: current.results[name] for name, state in states.items() if state != "success"}
            require(not failed and current.results["gate"] == "accepted", f"{dag_id} did not pass: {failed}")
        require(rig.tasks_started() == before + 1, f"{dag_id} started {rig.tasks_started() - before} tasks, not one")
        named = {container: values["FEATURE_SET"] for container, values in rig.environment(before).items() if "FEATURE_SET" in values}
        require(set(named) == FEATURE_CONTAINERS and set(named.values()) == {model}, f"{dag_id}'s containers did not all get FEATURE_SET={model}: {named}")
        require(rig.environment(before)["pin"]["INPUT_RUN_ID"] == base, f"{dag_id}'s pin container did not get the base run as its input run")
        require((rig.end(run, f"features_{model}") or {}).get("outcome") == "succeeded", f"{dag_id} left no success record for its run in its own family")
        require(rig.end(run, "features") is None, f"{dag_id} left a record under the base family")
    require((rig.end(base, "features") or {}).get("outcome") == "succeeded", "a model run changed the base run's record")


def model_dag_takes_its_base_run() -> None:
    """A model DAG's form requires the base run it builds from (a run ID, never latest) and a trigger without it is refused before any launch."""
    for dag_id in MODEL_DAGS:
        declared = form_parameter(f"{dag_id}.py", BASE_RUN_PARAMETER)
        require(declared is not None, f"{dag_id} declares no {BASE_RUN_PARAMETER} parameter")
        arguments, settings = declared or ([], {})
        pattern = str(settings.get("pattern", ""))
        require(not arguments and settings.get("type") == "string", f"{dag_id}: the base run must be required, with no default: {declared}")
        require(
            bool(pattern)
            and re.fullmatch(pattern.strip("^$"), FEATURES_RUN) is not None
            and not any(re.search(pattern, bad) for bad in ("latest", "", "x" + FEATURES_RUN)),
            f"{dag_id}: the form's pattern does not accept exactly a run ID: {pattern}",
        )
        rig = Rig()
        with world(rig, dag_id, {"run_id": model_run(OTHER_BASE, MODEL_DAGS[dag_id]), "mode": "real"}) as current:
            error = refuses(FormRefused, partial(current.run_dag, f"{dag_id}.py"))
            require(BASE_RUN_PARAMETER in str(error) and not current.states, f"{dag_id} without a base run was not refused at the form: {error}")
        require(not rig.ecs.calls and not rig.records.calls, f"{dag_id} reached ECS or the launch records without a base run")


def model_dag_needs_the_base_job_to_pass() -> None:
    """A model run launches only when the features job of its base run passed. The gold run passing is not enough, and a run that never ran is refused
    before any ECS call or launch record."""
    rig = Rig()
    gold, base = base_run_passed(rig)
    started = rig.tasks_started()
    run = model_run(base, "m1")
    with world(rig, "features_m1_dag", {"run_id": run, "mode": "real", BASE_RUN_PARAMETER: gold}) as current:
        error = refuses(FormRefused, lambda: current.run_dag("features_m1_dag.py"))
        require(BASE_RUN_PARAMETER in str(error) and not current.states, f"a gold run as the base run was not refused at the form: {error}")
    refused_runs = {"a base run that never ran": OTHER_BASE}
    for label, producer in refused_runs.items():
        with world(rig, "features_m1_dag", {"run_id": run, "mode": "real", BASE_RUN_PARAMETER: producer}) as current:
            states = current.run_dag("features_m1_dag.py")
            refused = [str(result) for result in current.results.values() if "needs the features job" in str(result)]
            require(states.get("gate") == "failed" and bool(refused), f"{label} as the base run was not refused by the preflight: {current.results}")
    require(rig.tasks_started() == started, "a model run with a base run that never passed started a task")


def model_run_code_is_bound() -> None:
    """The binding gate binds every model run of the chain to the bronze run's staged code, so a model launch fetches that code. A model run no binding gate
    bound has no staged code of its own and is refused before any ECS call."""
    rig = Rig()
    _, base = base_run_passed(rig)
    for model in MODEL_DAGS.values():
        key = RUN_KEY.format(run=model_run(base, model), name="code_run.json")
        require(key in rig.stage.objects and json.loads(rig.stage.objects[key]) == {"code_run": BRONZE_RUN}, f"the {model} run is not bound to the bronze code")
        drift = drift_run_of(model_run(base, model))
        for name, run in (("drift", drift), ("training", training_run_of(drift, model))):
            bound = rig.stage.objects.get(RUN_KEY.format(run=run, name="code_run.json"))
            require(bound is not None and json.loads(bound) == {"code_run": BRONZE_RUN}, f"the {model} {name} run is not bound to the bronze code")
    before = rig.tasks_started()
    with world(rig, "features_m2_dag", {"run_id": model_run(base, "m2"), "mode": "real", BASE_RUN_PARAMETER: base}) as current:
        current.run_dag("features_m2_dag.py")
    require(rig.environment(before)["fetch"]["CODE_RUN_ID"] == BRONZE_RUN, "the model run fetches its own code, not its bronze run's")
    unbound = Rig()
    _, other_base = base_run_passed(unbound)
    started = unbound.tasks_started()
    with world(unbound, "features_m2_dag", {"run_id": model_run(OTHER_BASE, "m2"), "mode": "real", BASE_RUN_PARAMETER: other_base}) as current:
        states = current.run_dag("features_m2_dag.py")
        require(states.get("preflight") == "failed" and states.get("gate") == "failed", f"an unbound model run was not refused: {states} {current.results}")
    require(unbound.tasks_started() == started, "an unbound model run started a task")


def rendered(settings: dict[str, Any], dag_settings: dict[str, Any], base: str) -> dict[str, Any]:
    """The templated fields of a trigger as Airflow renders them: the base DAG's macros and its ``params``."""
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=True)
    context = {"params": SimpleNamespace(run_id=base, mode="real"), **dag_settings["user_defined_macros"]}
    return {
        "trigger_run_id": environment.from_string(settings["trigger_run_id"]).render(**context),
        "conf": {name: environment.from_string(value).render(**context) for name, value in settings["conf"].items()},
    }


def base_dag_triggers_six_model_dags() -> None:
    """A published base run starts the six model DAGs and none waits: the triggers follow only the gate, name each model's DAG and derived run, pass the
    base run, skip a run that exists and never reset one. A failed base job starts none."""
    rig = Rig()
    gold, base = base_run_passed(rig)
    with world(rig, "features_base_dag", {"run_id": base, GOLD_RUN_PARAMETER: gold}) as current:
        states = current.run_dag("features_base_dag.py")
        nodes = {node.task_id: node for node in current.built}
        triggers = {name: node for name, node in nodes.items() if name.startswith("trigger_")}
        require(set(triggers) == {f"trigger_{model}" for model in MODEL_DAGS.values()}, f"the base DAG's triggers are {sorted(triggers)}")
        for model in MODEL_DAGS.values():
            node = triggers[f"trigger_{model}"]
            require([before.task_id for before in node.upstream] == ["gate"], f"trigger_{model} does not follow only the gate")
            require(node.settings["trigger_dag_id"] == f"features_{model}_dag", f"trigger_{model} starts {node.settings['trigger_dag_id']}")
            require(node.settings["wait_for_completion"] is False, f"trigger_{model} waits for its DAG: one model's failure must not stop the base DAG")
            require(node.settings["skip_when_already_exists"] is True and node.settings["reset_dag_run"] is False, f"trigger_{model} may restart a run")
            sent = rendered(node.settings, current.dag_settings, base)
            require(
                sent["conf"] == {"run_id": model_run(base, model), "mode": "real", BASE_RUN_PARAMETER: base},
                f"trigger_{model} sends {sent['conf']}",
            )
            require(
                sent["trigger_run_id"] == model_run(base, model) == sent["conf"]["run_id"],
                f"trigger_{model}'s run id is {sent['trigger_run_id']}",
            )
            require(states[f"trigger_{model}"] == "success", f"trigger_{model} did not run after a passed base run: {states}")
    failed = Rig()
    gold, base = base_run_passed(failed, base_fails=True)
    failed.ecs.plan = {"validate": 3}
    with world(failed, "features_base_dag", {"run_id": base, GOLD_RUN_PARAMETER: gold}) as current:
        states = current.run_dag("features_base_dag.py")
        require(states.get("gate") == "failed", f"the base DAG did not fail with its job: {states}")
        started = [name for name, state in states.items() if name.startswith("trigger_") and state != "upstream_failed"]
        require(not started, f"a failed base run still started model DAGs: {started}")


def one_model_failure_stops_only_its_dag() -> None:
    """A model DAG that fails fails only its own run: its gate fails and its job is not launched again, while the other five model runs and the base run
    keep their success records."""
    rig = Rig()
    _, base = base_run_passed(rig)
    outcomes: dict[str, str] = {}
    drift_checks: dict[str, str] = {}
    for dag_id, model in MODEL_DAGS.items():
        run = model_run(base, model)
        rig.ecs.plan = {"validate": 3} if model == "m3" else {}
        with world(rig, dag_id, {"run_id": run, "mode": "real", BASE_RUN_PARAMETER: base}) as current:
            states = current.run_dag(f"{dag_id}.py")
            outcomes[model], drift_checks[model] = states["gate"], states["trigger_drift"]
    require(outcomes == {model: ("failed" if model == "m3" else "success") for model in MODEL_DAGS.values()}, f"the gates ended {outcomes}")
    require(
        drift_checks == {model: ("upstream_failed" if model == "m3" else "success") for model in MODEL_DAGS.values()},
        f"a failed model run still started its drift check, or a passed one did not: {drift_checks}",
    )
    ends = {model: (rig.end(model_run(base, model), f"features_{model}") or {}).get("outcome") for model in MODEL_DAGS.values()}
    require(ends == {model: ("failed" if model == "m3" else "succeeded") for model in MODEL_DAGS.values()}, f"the launch records are {ends}")
    require((rig.end(base, "features") or {}).get("outcome") == "succeeded", "a failed model run changed the base run's record")
    streams = {call["logStreamName"] for call in rig.logs.calls}
    require(
        bool(streams) and all(name.startswith("features/") for name in streams),
        f"a failed set reads the log streams {sorted(streams)}: the containers log under the stream prefix features, the family's layer",
    )


def model_dags_share_the_host_pool() -> None:
    """Every model DAG's job task takes the host jobs pool where the deployment names it, with the slots of its share of the host; no other
    task of the DAG takes it."""
    rig = Rig()
    _, base = base_run_passed(rig)
    for dag_id in MODEL_DAGS:
        with (
            world(rig, dag_id, {"run_id": model_run(base, MODEL_DAGS[dag_id]), "mode": "real", BASE_RUN_PARAMETER: base}) as current,
            mock.patch.dict("os.environ", {"HAI_LAUNCH_POOL": "host_jobs"}),
        ):
            current.run_dag(f"{dag_id}.py")
            pools = {node.task_id: node.settings.get("pool") for node in current.built}
            require(
                pools == {"preflight": None, "job": "host_jobs", "evidence": None, "gate": None, "trigger_drift": None},
                f"{dag_id}'s tasks take the pools {pools}",
            )


# The model sets' reservations from the measured peaks of the runs 20261113T192848Z-* (the memory peak is at most 70% of the reservation; scratch peaked at
# 413 MiB): m2 and m4 peaked at 8976 MiB in the feast phase, the others at 2215 MiB.
SET_SIZES = {
    "m1": {"cpu_units": 512, "memory_mib": 4096, "scratch_gib": 2},
    "m2": {"cpu_units": 1024, "memory_mib": 13312, "scratch_gib": 2},
    "m3": {"cpu_units": 512, "memory_mib": 4096, "scratch_gib": 2},
    "m4": {"cpu_units": 1024, "memory_mib": 13312, "scratch_gib": 2},
    "m5": {"cpu_units": 512, "memory_mib": 4096, "scratch_gib": 2},
    "m6": {"cpu_units": 512, "memory_mib": 4096, "scratch_gib": 2},
}
# The smaller jobs keep their own sizes (owner A, Nov 3 2026), so they share the host with the model sets: Fargate-era table sizes, not host peaks.
OWN_SIZES = {
    "drift": {"cpu_units": 2048, "memory_mib": 8192, "scratch_gib": 30},
    "promote": {"cpu_units": 1024, "memory_mib": 4096, "scratch_gib": 21},
    "serve": {"cpu_units": 1024, "memory_mib": 4096, "scratch_gib": 21},
    "metadata": {"cpu_units": 1024, "memory_mib": 2048, "scratch_gib": 1},
    "publisher": {"cpu_units": 2048, "memory_mib": 8192, "scratch_gib": 30},
    "bronze_stage": {"cpu_units": 512, "memory_mib": 2048, "scratch_gib": 4},
}
SET_PEAKS_MIB = {"m1": 1348, "m2": 8976, "m3": 1844, "m4": 8969, "m5": 1613, "m6": 2215}
# The capacity of the r7g.xlarge host as the services stack computes it: 4096 CPU units less the services' 1024 leave 3072 for jobs; 32768 MiB usable less
# the 2048 system reserve and the 8592 service reservations (every service on, the optional ones included) leave the 22128 MiB job cap (22640 with only
# the default services). A pool slot is 64 CPU units, so the pool has 48 slots.
POOL_SLOTS = 48
POOL_ENVIRONMENT = {
    "HAI_LAUNCH_POOL": "host_jobs",
    "HAI_LAUNCH_POOL_SLOTS": str(POOL_SLOTS),
    "HAI_LAUNCH_POOL_SLOT_CPU_UNITS": "64",
    "HAI_LAUNCH_HOST_JOB_CAP_MIB": "22128",
}
JOB_CAPS_MIB = (22128, 22640, 22384)
JOB_ROOM_CPU = 3072
JOB_ROOM_MIB = 22128
# How long one set's job ran on the host, in minutes: the gaps between the task start times of the one-at-a-time run 20261113T192848Z-* (describe.json of
# evidence_run_m<n>_20261113T195509Z), which include the launch. The run of m6 has no recorded start of a successor, so m6 takes the small-set value.
SET_MINUTES = {"m1": 3.4, "m2": 4.9, "m3": 3.6, "m4": 4.9, "m5": 3.5, "m6": 3.5}
# The six sets must finish within the two large runs end to end: the 9.8 minutes of m2 then m4 (owner, Nov 13 2026: "finished in about 10 minutes").
TWO_WAVE_MINUTES = 9.8


def model_sets_have_their_own_families() -> None:
    """Each model set is a host family of its own, like the features job, so its launch records, scratch folder and task definition are its own and the family
    gate lets the six run beside each other. The set families reserve their measured size; the base and every other family reserve the whole job room."""
    table, host = importlib.import_module("orchestration.ecs_preflight").load_tables(ROOT)
    require(host["dags"]["features_base_dag"] == "features", f"the base DAG names {host['dags']['features_base_dag']}")
    for dag_id, model in MODEL_DAGS.items():
        family = f"features_{model}"
        require(host["dags"][dag_id] == family, f"{dag_id} names the family {host['dags'][dag_id]}, not {family}")
        require(table["jobs"][family] == table["jobs"]["features"], f"the {family} row is not the features row")
        require(
            re.search(r"max_active_runs\s*=\s*1\b", (ROOT / "dags" / f"{dag_id}.py").read_text()) is not None,
            f"{dag_id} may run twice at once: the pool no longer keeps two runs of one set apart, so the DAG allows one active run",
        )
        need = host["jobs"][family]
        size = SET_SIZES[model]
        require({key: need.get(key) for key in size} == size and need.get("reserve") is True, f"{family} does not reserve its own size: {need}")
        require(
            SET_PEAKS_MIB[model] <= 0.7 * need["memory_mib"],
            f"{family} reserves {need['memory_mib']} MiB, but its measured peak {SET_PEAKS_MIB[model]} MiB is above 70% of it",
        )
    require(
        {name for name, need in host["jobs"].items() if need.get("reserve")} == {f"features_{model}" for model in SET_SIZES} | set(OWN_SIZES),
        "the families that reserve less than the job room are not the model sets and the six smaller jobs (silver, gold, features, bronze, train: the room)",
    )
    for name, size in OWN_SIZES.items():
        require({key: host["jobs"][name].get(key) for key in size} == size, f"{name} does not keep its own size: {host['jobs'][name]}")
    require(table["steps"]["pin_gold"] == "features", "the step table must keep naming the base family; the DAG picks the set family")
    note = str(host.get("reserve_note", ""))
    require(
        "waves" in note and "expected, not a failure" in note,
        "the host table does not say that the six model sets run in bounded waves and that this is expected (owner A, Nov 13 2026)",
    )
    require(
        "host_jobs.json" in (ROOT / "infra/jobs/host_sets.tf").read_text() and 'variable "' not in (ROOT / "infra/jobs/host_sets.tf").read_text(),
        "infra/jobs/host_sets.tf must read the model set sizes from the host table, with no copy of them",
    )


def model_sets_launch_in_their_own_task_definitions() -> None:
    """Each model run sends RunTask for the host definition of its own family and starts exactly one task there."""
    rig = Rig()
    _, base = base_run_passed(rig)
    for dag_id, model in MODEL_DAGS.items():
        before = len(rig.ecs.sent)
        with world(rig, dag_id, {"run_id": model_run(base, model), "mode": "real", BASE_RUN_PARAMETER: base}) as current:
            current.run_dag(f"{dag_id}.py")
        sent = rig.ecs.sent[before:]
        require(len(sent) == 1 and sent[0].rsplit("/", 1)[-1].rsplit(":", 1)[0] == f"example_dev_job_features_{model}_host", f"{dag_id} sent {sent}")
        claim = rig.launcher.store.run_record(model_run(base, model), f"features_{model}", "claim")
        need = (claim or {}).get("need")
        size = SET_SIZES[model]
        require(
            need == {"cpu_units": size["cpu_units"], "memory_mib": size["memory_mib"], "scratch_bytes": size["scratch_gib"] * 2**30},
            f"the claim of {dag_id} asks the host for {need}, not its own reservation",
        )
    base_need = ((rig.launcher.store.run_record(base, "features", "claim") or {}).get("need")) or {}
    require(
        base_need.get("cpu_units") == 3072 and base_need.get("memory_mib") == JOB_MEMORY,
        f"the features base claim asks the host for {base_need}, not the whole job room (3072 units and {JOB_MEMORY} MiB)",
    )


def pool_makespan(order: tuple[str, ...], weights: dict[str, int], priorities: dict[str, int]) -> float:
    """The minutes the six sets take in a pool of ``POOL_SLOTS`` slots when their job tasks arrive in ``order``: the scheduler walks the waiting tasks by
    descending priority weight, then by arrival, and starts each one that fits; a task that does not fit is skipped and a smaller one behind it can start
    (scheduler_job_runner.py of apache-airflow-core 3.4.0, ``_executable_task_instances_to_queued``)."""
    waiting = sorted(order, key=lambda model: (-priorities[model], order.index(model)))
    running: dict[str, float] = {}
    now = 0.0
    while waiting or running:
        used = sum(weights[model] for model in running)
        for model in list(waiting):
            if used + weights[model] <= POOL_SLOTS:
                running[model] = now + SET_MINUTES[model]
                used += weights[model]
                waiting.remove(model)
        first = min(running, key=lambda model: running[model])
        now = running.pop(first)
    return now


def six_model_sets_fit_the_host_in_two_waves() -> None:
    """The set reservations in waves: all six together do not fit the job room (the large m2 and m4 hold 13312 MiB each), but at each job cap the pool lets one
    large and two small sets run together and never two large ones, and the large sets have the higher priority weight, so the six finish in two waves
    whatever order their tasks arrive in; a full-size job takes every slot so it waits for the sets and they wait for it."""
    with world(Rig(), "silver_transform_dag", SILVER) as current:
        runtime = current.runtime
    _, host = importlib.import_module("orchestration.ecs_preflight").load_tables(ROOT)
    sets = {model: host["jobs"][f"features_{model}"] for model in MODEL_DAGS.values()}
    for cap in JOB_CAPS_MIB:
        with mock.patch.dict("os.environ", {**POOL_ENVIRONMENT, "HAI_LAUNCH_HOST_JOB_CAP_MIB": str(cap)}, clear=True):
            weights = {model: runtime.pool_slots_of(f"features_{model}") for model in sets}
            priorities = {model: runtime.pool_settings(f"features_{model}")["priority_weight"] for model in sets}
            full = runtime.pool_slots_of(None)
        require(
            weights == {"m1": 9, "m2": 29, "m3": 9, "m4": 29, "m5": 9, "m6": 9} and full == POOL_SLOTS,
            f"at {cap} MiB the pool weights are {weights} and {full}",
        )
        require(
            sum(weights.values()) > POOL_SLOTS and full + min(weights.values()) > POOL_SLOTS,
            f"at {cap} MiB the pool lets a full-size job run beside a set, or all six sets at once",
        )
        require(weights["m2"] + weights["m1"] + weights["m3"] <= POOL_SLOTS, f"at {cap} MiB one large and two small sets do not fit the pool: {weights}")
        require(weights["m2"] + weights["m4"] > POOL_SLOTS, f"at {cap} MiB the two large sets fit the pool together: {weights}")
        for model, need in sets.items():
            require(weights[model] * cap // POOL_SLOTS >= need["memory_mib"], f"at {cap} MiB the {model} weight covers less memory than the set reserves")
            require(weights[model] * JOB_ROOM_CPU // POOL_SLOTS >= need["cpu_units"], f"the {model} weight covers less CPU than the set reserves")
        require(
            max(priorities[model] for model in ("m1", "m3", "m5", "m6")) < min(priorities["m2"], priorities["m4"]),
            f"the large sets do not outrank the small ones: {priorities}",
        )
        slowest = max(pool_makespan(order, weights, priorities) for order in itertools.permutations(sets))
        require(
            slowest <= TWO_WAVE_MINUTES + 0.05,
            f"at {cap} MiB some arrival order takes {slowest:.1f} minutes, above the two waves of {TWO_WAVE_MINUTES} minutes",
        )
        no_priority = {model: 0 for model in sets}
        require(
            max(pool_makespan(order, weights, no_priority) for order in itertools.permutations(sets)) > TWO_WAVE_MINUTES + 0.05,
            f"at {cap} MiB the arrival order alone already gives two waves, so the priority weight proves nothing",
        )
    with mock.patch.dict("os.environ", {"HAI_LAUNCH_POOL": "host_jobs"}, clear=True):
        require(runtime.pool_slots_of("features_m1") == 1, "a deployment with no slot setting must keep the one-slot pool")
        require("priority_weight" not in runtime.pool_settings("features_m1"), "a deployment with no slot setting gives no priority weight")


WEIGHTS = {
    **{f"features_{model}_dag.py": (29 if model in ("m2", "m4") else 9) for model in ("m1", "m2", "m3", "m4", "m5", "m6")},
    "model_promotion_dag.py": 16,  # 1,024 units: 16 slots of 64
    "serving_deploy_dag.py": 16,
}


def model_jobs_weigh_their_pool_slots() -> None:
    """With the deployment's slot settings, the job task of a model DAG takes the slots of its set (nine, or twenty-nine for m2 and m4) and the same number
    as its priority weight, so the large sets come first. The job task of every other host DAG takes the slots of its share and no priority weight, and no
    other task takes slots. Without the settings no task names slots."""
    rig = Rig()
    with world(rig, "silver_transform_dag", SILVER) as current:
        runtime = current.runtime
        built: list[Operator] = []

        def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
            def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
                def call() -> SimpleNamespace:
                    node = Operator(**settings)
                    built.append(node)
                    return SimpleNamespace(operator=node)

                return call

            return decorate

        for environment, expected in ((POOL_ENVIRONMENT, True), ({"HAI_LAUNCH_POOL": "host_jobs"}, False)):
            for dag_file in (*COLLAPSED_FILES, *(f"{dag_id}.py" for dag_id in MODEL_DAGS)):
                built.clear()
                with (
                    mock.patch.dict("os.environ", environment, clear=True),
                    mock.patch.object(sys.modules["airflow.sdk"], "task", task),
                    mock.patch.object(runtime, "task", task),
                ):
                    runpy.run_path(str(ROOT / "dags" / dag_file))
                slots = {node.task_id: node.settings.get("pool_slots") for node in built}
                want = {"job": (WEIGHTS.get(dag_file, POOL_SLOTS))} if expected else {}
                require(
                    {name: value for name, value in slots.items() if value is not None} == want,
                    f"{dag_file} with {'' if expected else 'no '}slot settings has the pool slots {slots}, expected {want}",
                )
                priorities = {node.task_id: node.settings.get("priority_weight") for node in built if node.settings.get("priority_weight") is not None}
                wanted = {"job": WEIGHTS[dag_file]} if expected and dag_file.startswith("features_m") else {}
                require(
                    priorities == wanted,
                    f"{dag_file} has the priority weights {priorities}, expected {wanted}: only a model set takes its slots as its priority",
                )


def graph_rules_cover_the_feature_dags() -> None:
    """The graph rules accept the six model DAGs as one-job chains and the base DAG as a one-job chain with its six triggers behind the gate, and refuse a
    base DAG with a missing, extra or misplaced trigger."""
    graph = importlib.import_module("orchestration.graph")
    triggers = [f"trigger_{model}" for model in MODEL_DAGS.values()]
    for dag_id in (*MODEL_DAGS, "features_base_dag"):
        require(dag_id in graph.COLLAPSED_DAGS and dag_id in graph.GRAPH_DAGS, f"{dag_id} is not a graph-checked one-job DAG")
        require(graph.validate_transform_graph(graph.reference_graph(dag_id), dag_id) == [], f"the reference graph of {dag_id} is refused")
    reference = graph.reference_graph("features_base_dag")
    require(
        set(triggers) <= set(reference) and all(reference[name].upstream == frozenset({"gate"}) for name in triggers), "the base reference has no six triggers"
    )

    def rules(broken: dict[str, Any]) -> set[str]:
        return {violation.rule for violation in graph.validate_transform_graph(broken, "features_base_dag")}

    require("missing_task" in rules({name: node for name, node in reference.items() if name != "trigger_m4"}), "a base DAG without trigger_m4 was accepted")
    require("unexpected_task" in rules({**reference, "trigger_m7": graph.TaskNode(frozenset({"gate"}))}), "a base DAG with a seventh trigger was accepted")
    require(
        "upstream" in rules({**reference, "trigger_m2": graph.TaskNode(frozenset({"job"}))}), "a trigger that follows the job instead of the gate was accepted"
    )
    require(
        "leaf" in rules({**reference, "trigger_m1": graph.TaskNode(frozenset({"gate"})), "tail": graph.TaskNode(frozenset({"trigger_m1"}))}),
        "a tail task after a trigger was accepted",
    )
    rig = Rig()
    gold, base = base_run_passed(rig)
    with world(rig, "features_base_dag", {"run_id": base, GOLD_RUN_PARAMETER: gold}) as current:
        current.run_dag("features_base_dag.py")
        require(graph.validate_transform_graph(task_graph(current), "features_base_dag") == [], "the shipped base DAG breaks the graph rules")


def model_dags_trigger_their_drift_check() -> None:
    """Each model DAG starts its own model's drift check once its set is published and its acceptance checks passed: one trigger behind the gate only, no
    wait, a run derived from the model run (a retry names the run that exists), the model run as the features run and the aws MLflow lane."""
    rig = Rig()
    _, base = base_run_passed(rig)
    named: set[str] = set()
    for dag_id, model in MODEL_DAGS.items():
        run = model_run(base, model)
        with world(rig, dag_id, {"run_id": run, "mode": "real", BASE_RUN_PARAMETER: base}) as current:
            states = current.run_dag(f"{dag_id}.py")
            node = {item.task_id: item for item in current.built}["trigger_drift"]
            settings = node.settings
            require([before.task_id for before in node.upstream] == ["gate"], f"{dag_id}'s drift trigger does not follow only the gate")
            require(settings["trigger_dag_id"] == "drift_monitoring_dag", f"{dag_id} starts {settings['trigger_dag_id']}, not the drift DAG")
            require(settings["wait_for_completion"] is False, f"{dag_id} waits for its drift check")
            require(settings["skip_when_already_exists"] is True and settings["reset_dag_run"] is False, f"{dag_id} may restart a drift run")
            sent = rendered(settings, current.dag_settings, run)
            expected = {"run_id": drift_run_of(run), "mode": "real", "mlflow": "aws", "model": model, "features_run": run}
            require(sent["conf"] == expected, f"{dag_id} sends {sent['conf']}, expected {expected}")
            require(sent["trigger_run_id"].endswith(drift_run_of(run)), f"{dag_id}'s drift trigger run id is {sent['trigger_run_id']}")
            require(states["trigger_drift"] == "success", f"{dag_id}'s drift trigger did not run after a passed set: {states}")
            named.add(sent["trigger_run_id"])
    require(len(named) == len(MODEL_DAGS), "two model DAGs name the same drift trigger run")


def drift_dag_takes_its_model_and_features_run() -> None:
    """The drift DAG's form requires the model it checks and the model run whose set it reads (a run ID, never latest): the drift job is a check of one
    published set, not of whatever is newest."""
    model = form_parameter("drift_monitoring_dag.py", "model")
    require(model is not None and not model[0], f"the drift form gives the model a default: {model}")
    require(
        (model or ([], {}))[1].get("enum") == list(importlib.import_module("orchestration.training_contract").MODELS),
        f"the drift form's models differ: {model}",
    )
    declared = form_parameter("drift_monitoring_dag.py", "features_run")
    require(declared is not None, "the drift DAG declares no features_run parameter")
    arguments, settings = declared or ([], {})
    pattern = str(settings.get("pattern", ""))
    require(not arguments and settings.get("type") == "string", f"the features run must be required, with no default: {declared}")
    require(
        bool(pattern)
        and re.fullmatch(pattern.strip("^$"), SET_FEATURES_RUN) is not None
        and not any(re.search(pattern, bad) for bad in ("latest", "", "x" + SET_FEATURES_RUN)),
        f"the form's pattern does not accept exactly a run ID: {pattern}",
    )


def drift_job_takes_its_model_and_features_run() -> None:
    """The drift job checks one model's published set: the baseline container is told the model (FEATURE_SET) and the current-window container the model
    run (INPUT_RUN_ID). The run's code was bound by the binding gate, so the drift launch binds nothing."""
    rig = Rig()
    _, run = model_chain_passed(rig, "m2")
    puts, started = len(rig.stage.puts), rig.tasks_started()
    params = {"run_id": drift_run_of(run), "mode": "real", "mlflow": "aws", "model": "m2", "features_run": run}
    with world(rig, "drift_monitoring_dag", params) as current:
        require(current.step("drift_run") == "succeeded", "the drift job did not pass")
    values = rig.environment(started)
    require(values["baseline"]["FEATURE_SET"] == "m2", f"the baseline container was not told the model: {values['baseline']}")
    require(values["current"]["INPUT_RUN_ID"] == run, f"the current-window container was not told the model run: {values['current']}")
    require(values["fetch"]["CODE_RUN_ID"] == BRONZE_RUN, "the drift run fetches its own code, not its bronze run's")
    require(len(rig.stage.puts) == puts, "the drift launch bound a run: the binding gate binds every run of the pipeline")
    require(rig.tasks_started() == started + 1, "the drift check did not start exactly one task")


def drift_job_needs_its_model_run_to_pass() -> None:
    """A drift run starts only for a model run whose features job passed, with a drift run derived from that model run and a model of the contract. Each
    refusal comes before any ECS call."""
    rig = Rig()
    _, run = model_chain_passed(rig, "m2")
    rig.stage_runs(OTHER_SET_RUN, drift_run_of(OTHER_SET_RUN))
    started = rig.tasks_started()
    params = {"mode": "real", "mlflow": "aws", "model": "m2"}
    with world(rig, "drift_monitoring_dag", {**params, "run_id": drift_run_of(OTHER_SET_RUN), "features_run": OTHER_SET_RUN}) as current:
        error = refuses(LaunchRefused, lambda: current.step("drift_run"))
    require("needs the features_m2 job" in str(error), f"a model run that never passed was not refused by the job of its set's family: {error}")
    with world(rig, "drift_monitoring_dag", {**params, "run_id": "drift_elastic_net_0f0f0f0f", "features_run": run}) as current:
        error = refuses(LaunchRefused, lambda: current.step("drift_run"))
    require("derive" in str(error), f"a drift run that does not derive from its model run was not refused by name: {error}")
    with world(rig, "drift_monitoring_dag", {**params, "run_id": drift_run_of(run), "features_run": run, "model": "m7"}) as current:
        error = refuses(LaunchRefused, lambda: current.step("drift_run"))
    require("m7" in str(error), f"a model outside the contract was not refused by name: {error}")
    require(rig.tasks_started() == started, "a refused drift run started a task")


def drift_attempt_ids_and_form() -> None:
    """The drift run ID of attempt 1 is the ID every model run has, the ID of a later attempt derives from the attempt too, and an attempt the contract refuses
    raises by name. The form takes the attempt as an integer from 1 to 99 that defaults to 1."""
    contract = importlib.import_module("orchestration.training_contract")
    features = model_run(base_run_passed(Rig())[1], "m2")
    unchanged = features.replace("features_", "drift_", 1)
    require(contract.model_drift_run_id(features) == unchanged == contract.model_drift_run_id(features, 1), "attempt 1 changed the drift run ID")
    later = {attempt: contract.model_drift_run_id(features, attempt) for attempt in (2, 3, 99)}
    require(
        len({unchanged, *later.values()}) == 4 and all(run == f"{unchanged}_{attempt}" for attempt, run in later.items()), f"the attempts share a run: {later}"
    )
    require(contract.model_drift_run_id(features, 2) == later[2], "an attempt derives another run for every call")
    for bad in (0, 100, -1, True, "2", 2.0):
        try:
            contract.model_drift_run_id(features, bad)
        except ValueError as error:
            require("drift attempt" in str(error), f"{bad!r} was refused for another cause: {error}")
        else:
            raise AssertionError(f"the drift attempt {bad!r} was accepted")
    declared = form_parameter("drift_monitoring_dag.py", "drift_attempt")
    arguments, settings = declared or ([], {})
    require(
        declared is not None and arguments == [1] and settings == {"type": "integer", "minimum": 1, "maximum": 99},
        f"the form does not take the attempt as 1 to 99, default 1: {declared}",
    )


def drift_attempt_runs_the_code_staged_for_its_own_run() -> None:
    """The first drift run is bound to the bronze code by the binding gate. A later attempt exists to run fixed code, so it runs the code staged for its own
    run and binds the training run it may start to the same code. Attempt 1 and its training run stay as they were."""
    contract = importlib.import_module("orchestration.training_contract")
    rig = Rig()
    _, run = model_chain_passed(rig, "m2")
    second = contract.model_drift_run_id(run, 2)
    rig.stage_runs(second)
    params = {"run_id": second, "mode": "real", "mlflow": "aws", "model": "m2", "features_run": run, "drift_attempt": 2}
    first_training = RUN_KEY.format(run=training_run_of(drift_run_of(run), "m2"), name="code_run.json")
    started = rig.tasks_started()
    with world(rig, "drift_monitoring_dag", params) as current:
        require(current.step("drift_run") == "succeeded", "the drift job of attempt 2 did not pass")
    values = rig.environment(started)
    require(values["fetch"]["CODE_RUN_ID"] == second, f"attempt 2 ran the code of {values['fetch']['CODE_RUN_ID']}, not its own staged code")
    require(values["baseline"]["FEATURE_SET"] == "m2" and values["current"]["INPUT_RUN_ID"] == run, f"attempt 2 checks another set: {values}")
    bound = json.loads(rig.stage.objects[RUN_KEY.format(run=training_run_of(second, "m2"), name="code_run.json")])
    require(bound == {"code_run": second}, f"the training run of attempt 2 is bound to {bound}, not to the code of its drift run")
    require(RUN_KEY.format(run=second, name="code_run.json") not in rig.stage.objects, "attempt 2 was bound to another run's code")
    require(json.loads(rig.stage.objects[first_training]) == {"code_run": BRONZE_RUN}, "the training run of attempt 1 was rebound")
    training = {**TRAIN_PARAMS, "run_id": training_run_of(second, "m2"), "features_run": run, "trigger": f"drift:{second}", "model": "m2"}
    started = rig.tasks_started()
    with world(rig, "training_dag", training) as current:
        require(current.step("guard") == "succeeded", "the training run of attempt 2 did not launch")
    require(rig.environment(started)["fetch"]["CODE_RUN_ID"] == second, "the training run of attempt 2 did not run the code staged for its drift run")
    first = Rig()
    _, run = model_chain_passed(first, "m2")
    started = first.tasks_started()
    with world(first, "drift_monitoring_dag", {**params, "run_id": drift_run_of(run), "features_run": run, "drift_attempt": 1}) as current:
        require(current.step("drift_run") == "succeeded", "the drift job of attempt 1 did not pass")
    require(first.environment(started)["fetch"]["CODE_RUN_ID"] == BRONZE_RUN, "attempt 1 did not run the bronze run's code")


def drift_attempt_without_staged_code_is_refused_by_name() -> None:
    """A later attempt with nothing staged for its own run is refused naming the run and the missing object, even with the bronze code staged: it never falls
    back to that code. The refusal comes before any binding, launch record or task."""
    contract = importlib.import_module("orchestration.training_contract")
    rig = Rig()
    _, run = model_chain_passed(rig, "m2")
    second = contract.model_drift_run_id(run, 2)
    params = {"run_id": second, "mode": "real", "mlflow": "aws", "model": "m2", "features_run": run, "drift_attempt": 2}
    puts, started, records = len(rig.stage.puts), rig.tasks_started(), len(rig.records.calls)
    with world(rig, "drift_monitoring_dag", params) as current:
        error = refuses(LaunchRefused, lambda: current.step("drift_run"))
    require(second in str(error) and "code.sha256" in str(error) and "attempt 2" in str(error), f"the refusal does not name the run and the object: {error}")
    written = [call for call in rig.records.calls[records:] if call[0] != "get_object"]
    after = (len(rig.stage.puts), rig.tasks_started())
    require(after == (puts, started) and not written, f"a refused attempt wrote or launched: puts and tasks {after}, record writes {written}")


def drift_attempt_and_run_must_agree() -> None:
    """The drift run derives from the features run and the attempt the request names: the first attempt's ID with attempt 2 and a later attempt's ID with no
    attempt are both refused, attempt 1 is never spelled out, and an attempt outside 1 to 99 is refused before any launch."""
    contract = importlib.import_module("orchestration.training_contract")
    rig = Rig()
    _, run = model_chain_passed(rig, "m2")
    second = contract.model_drift_run_id(run, 2)
    rig.stage_runs(second)
    base = {"mode": "real", "mlflow": "aws", "model": "m2", "features_run": run}
    started = rig.tasks_started()
    for given, attempt in ((drift_run_of(run), 2), (second, None)):
        extra = {} if attempt is None else {"drift_attempt": attempt}
        with world(rig, "drift_monitoring_dag", {**base, "run_id": given, **extra}) as current:
            error = refuses(LaunchRefused, lambda: current.step("drift_run"))
        require("derive" in str(error), f"run {given} with attempt {attempt} was not refused as not derived: {error}")
    request = SubmissionRequest(
        "drift_run", drift_run_of(run), "real", 1, "drift_monitoring_dag", "drift_run", "manual__test", run, training={"model": "m2", "drift_attempt": 1}
    )
    error = refuses(LaunchRefused, partial(rig.preflight.prepare, request))
    require("drift_attempt" in str(error), f"the preflight took the attempt 1 spelled out: {error}")
    for attempt in (0, 100):
        with world(rig, "drift_monitoring_dag", {**base, "run_id": second, "drift_attempt": attempt}) as current:
            error = refuses(ValueError, lambda: current.step("drift_run"))
        require("drift attempt" in str(error), f"attempt {attempt} was refused for another cause: {error}")
    require(rig.tasks_started() == started, "a refused drift run started a task")


LIVE_RUN = "drift_elastic_net_1ee70001"
LIVE_PARAMS = {"run_id": LIVE_RUN, "mode": "real", "model": "m2", "hai_type": "HAI_3"}


def live_drift_job_takes_its_model_and_type() -> None:
    """The on-demand live check launches the drift family's one task for a model and a type, with no model run: the baseline container is told the model and
    the type, the check and publish containers the type, the current-window container the type and no model run. It runs the code staged for its own run,
    binds no run and starts exactly one task, and the DAG has no task that starts another DAG."""
    rig = Rig()
    rig.stage_runs(LIVE_RUN)
    puts, started = len(rig.stage.puts), rig.tasks_started()
    with world(rig, "drift_live_check_dag", LIVE_PARAMS) as current:
        states = current.run_dag("drift_live_check_dag.py")
        failed = {name: state for name, state in states.items() if state != "success"}
        require(bool(states) and not failed and current.results["gate"] == "accepted", f"the live check did not finish: {failed} {current.results}")
    values = rig.environment(started)
    require(values["baseline"]["FEATURE_SET"] == "m2", f"the baseline container was not told the model: {values['baseline']}")
    require(
        {name: values[name]["LIVE_HAI_TYPE"] for name in ("baseline", "current", "check", "publish")}
        == dict.fromkeys(("baseline", "current", "check", "publish"), "HAI_3"),
        f"a phase container was not told the type: {values}",
    )
    require(values["current"]["INPUT_RUN_ID"] == "latest", f"the live check named a model run: {values['current']}")
    require(values["fetch"]["CODE_RUN_ID"] == LIVE_RUN, f"the live check ran the code of {values['fetch']['CODE_RUN_ID']}, not its own staged code")
    require(len(rig.stage.puts) == puts, "the live check bound a run")
    require(rig.tasks_started() == started + 1, "the live check did not start exactly one task")
    gate = Rig()
    gate.stage_runs(OTHER_RUN)
    with world(gate, "drift_monitoring_dag", {"run_id": OTHER_RUN, "mode": "real", "mlflow": "aws", "model": "m2", "features_run": OTHER_RUN}) as current:
        error = refuses(LaunchRefused, lambda: current.step("drift_run"))
    require("derive" in str(error) or "features" in str(error), f"the gate DAG no longer refuses a run that derives from nothing: {error}")


def live_drift_job_is_refused_by_name_before_any_launch() -> None:
    """A live check with nothing staged for its own run, a model or a type outside the contract, or a request with another key is refused by name before any
    binding, launch record or task. The gate runs of the drift DAG keep the reason they had."""
    rig = Rig()
    started, records = rig.tasks_started(), len(rig.records.calls)
    with world(rig, "drift_live_check_dag", LIVE_PARAMS) as current:
        error = refuses(LaunchRefused, lambda: current.step("drift_live_run"))
    require(
        LIVE_RUN in str(error) and "code.sha256" in str(error) and "live drift check" in str(error),
        f"the refusal does not name the run and the object: {error}",
    )
    rig.stage_runs(LIVE_RUN)
    for name, value in (("model", "m7"), ("hai_type", "HAI_9")):
        with world(rig, "drift_live_check_dag", {**LIVE_PARAMS, name: value}) as current:
            error = refuses(LaunchRefused, lambda: current.step("drift_live_run"))
        require(value in str(error), f"{name}={value} was not refused by name: {error}")
    request = SubmissionRequest("drift_live_run", LIVE_RUN, "real", 1, "drift_live_check_dag", "job", "manual__test", None, training={"model": "m2"})
    error = refuses(LaunchRefused, partial(rig.preflight.prepare, request))
    require("exactly" in str(error), f"a request without the type was not refused as not closed: {error}")
    written = [call for call in rig.records.calls[records:] if call[0] != "get_object"]
    require(rig.tasks_started() == started and not written, f"a refused live check wrote or launched: tasks {rig.tasks_started()}, record writes {written}")


def training_run_fits_its_model_only() -> None:
    """A drift-started training run names one model and that model's own run: m5 trains with m5's set and m6 with m6's, each bound to the bronze code,
    so the run fits only that model and its own tests (the research tests of m5 and m6)."""
    rig = Rig()
    _, base = base_run_passed(rig)
    for model in ("m5", "m6"):
        features = model_run(base, model)
        drift = drift_run_of(features)
        started = rig.tasks_started()
        params = {**TRAIN_PARAMS, "run_id": training_run_of(drift, model), "features_run": features, "trigger": f"drift:{drift}", "model": model}
        with world(rig, "training_dag", params) as current:
            require(current.step("guard") == "succeeded", f"the {model} training run did not launch")
        values = rig.environment(started)
        sent = json.loads(values["prepare"]["TRAINING_PARAMETERS"])
        require(sent["model"] == model and sent["features_run"] == features and sent["trigger"] == f"drift:{drift}", f"the {model} run sends {sent}")
        require(values["fetch"]["CODE_RUN_ID"] == BRONZE_RUN, f"the {model} training run fetches its own code, not its bronze run's")


def the_pipeline_runs_no_model_less_drift_layer() -> None:
    """The pipeline runs ingestion, bronze, silver, gold and the features base only: each model's own DAG starts its drift check. The binding gate binds no
    drift layer run and no training run of one, and the bronze DAG's text names no drift DAG. The first DAG waits for each model's route, which starts with that
    model's own drift check."""
    preflight = importlib.import_module("orchestration.ecs_preflight")
    contract = importlib.import_module("orchestration.training_contract")
    require(preflight.CHAIN_LAYERS == ("silver", "gold", "features"), f"the chain layers are {preflight.CHAIN_LAYERS}")
    require("drift" not in (ROOT / "dags/bronze_dag.py").read_text(), "the bronze DAG file names a drift layer")
    rig = Rig()
    with world(rig, PIPELINE_DAG, {"run_id": PIPELINE_RUN, "mode": "real"}) as current:
        require(current.step("bind_code") == "succeeded", "the binding gate failed")
    old_drift = f"drift_{BRONZE_RUN.removeprefix('bronze_')}"
    require(RUN_KEY.format(run=old_drift, name="code_run.json") not in rig.stage.objects, "the binding gate still binds the model-less drift layer's runs")
    require(re.fullmatch(contract.RUN_ID, old_drift) is None, "a drift run without a model is a run ID")


def graph_rules_cover_the_model_drift_trigger() -> None:
    """A model DAG is the one-job chain with one drift trigger behind the gate: the graph rules refuse a model DAG without it, with a second trigger, with a
    trigger behind the job instead of the gate, or with a task after it."""
    graph = importlib.import_module("orchestration.graph")
    for dag_id in MODEL_DAGS:
        reference = graph.reference_graph(dag_id)
        require("trigger_drift" in reference and reference["trigger_drift"].upstream == frozenset({"gate"}), f"{dag_id}'s reference has no drift trigger")

        def rules(broken: dict[str, Any], dag_id: str = dag_id) -> set[str]:
            return {violation.rule for violation in graph.validate_transform_graph(broken, dag_id)}

        require(
            "missing_task" in rules({name: node for name, node in reference.items() if name != "trigger_drift"}), f"{dag_id} without its trigger was accepted"
        )
        require("unexpected_task" in rules({**reference, "trigger_extra": graph.TaskNode(frozenset({"gate"}))}), f"{dag_id} with a second trigger was accepted")
        require("upstream" in rules({**reference, "trigger_drift": graph.TaskNode(frozenset({"job"}))}), f"{dag_id} with a trigger behind the job was accepted")
        require("leaf" in rules({**reference, "tail": graph.TaskNode(frozenset({"trigger_drift"}))}), f"{dag_id} with a task after the trigger was accepted")
    rig = Rig()
    _, base = base_run_passed(rig)
    with world(rig, "features_m1_dag", {"run_id": model_run(base, "m1"), "mode": "real", BASE_RUN_PARAMETER: base}) as current:
        current.run_dag("features_m1_dag.py")
        require(graph.validate_transform_graph(task_graph(current), "features_m1_dag") == [], "the shipped m1 DAG breaks the graph rules")


def promotion_and_serving_dags_run_end_to_end() -> None:
    rig = Rig()
    plan = (
        (
            "model_promotion_dag.py",
            "model_promotion_dag",
            {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "aws"},
        ),
        ("serving_deploy_dag.py", "serving_deploy_dag", SERVE_FORM),
    )
    for file, dag_id, params in plan:
        with world(rig, dag_id, params) as current:
            states = current.run_dag(file)
            failed = {name: state for name, state in states.items() if state != "success"}
            require(bool(states) and not failed and current.results["gate"] == "accepted", f"{dag_id} did not finish: {failed} {current.results}")
    require(rig.tasks_started() == 2, f"one task per job was expected: {rig.tasks_started()} tasks")
    for run, job in ((PROMOTE_RUN, "promote"), (SERVE_RUN, "serve")):
        require((rig.end(run, job) or {}).get("outcome") == "succeeded", f"{job} has no success record")
    with world(rig, "serving_deploy_dag", SERVE_FORM) as current:
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


def script_imports(path: Path) -> set[str]:
    """The files below ``scripts/`` that a module imports, at the top or inside a function: ``from scripts.a import b`` names ``scripts/a/b.py`` when that
    file exists, else ``scripts/a.py``."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module and (node.module == "scripts" or node.module.startswith("scripts.")):
            base = node.module.replace(".", "/")
            for alias in node.names:
                found.add(f"{base}/{alias.name}.py" if (ROOT / base / f"{alias.name}.py").is_file() else f"{base}.py")
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
    wanted, queue_ = set(), [f"orchestration/{name}.py" for name in needed]
    while queue_:
        path = queue_.pop()
        for imported in script_imports(ROOT / path):
            if imported not in wanted and (ROOT / imported).is_file() and not imported.endswith("__init__.py"):
                wanted.add(imported)
                queue_.append(imported)
    unmounted = sorted(path for path in wanted if f"/opt/hai/{path}" not in text and path not in text)
    require(not unmounted, f"the scripts {unmounted} that the launch modules import are not in the Airflow mount list")
    for path in (
        "config/orchestration/ecs_broker_jobs.json",
        "config/orchestration/ecs_broker_jobs.schema.json",
        "config/orchestration/host_jobs.json",
        "config/orchestration/direct_jobs.json",
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


def host_table_agrees_with_the_reservations() -> None:
    """The launcher's fit check must ask for what ECS reserves. A family without ``reserve`` reserves the whole job room, so its row says 3072 units and
    the services default job cap; a family with ``reserve`` reserves the row's own size, which infra/jobs/host_sets.tf reads from this same file. A row that
    disagrees (CPU below the room, a reserve above the room, a missing size) is refused when the table loads."""
    preflight = importlib.import_module("orchestration.ecs_preflight")
    _, host = preflight.load_tables(ROOT)
    cap_found = re.search(r'variable "host_job_cap_mib" \{.*?default\s*=\s*(\d+)', (ROOT / "infra/services/variables.tf").read_text(), re.DOTALL)
    room_found = re.search(r'variable "job_cpu_units" \{.*?default\s*=\s*(\d+)', (ROOT / "infra/services/host_capacity.tf").read_text(), re.DOTALL)
    require(cap_found is not None and room_found is not None, "the services job cap or job CPU default was not found")
    cap, room = int((cap_found or ["", "0"])[1]), int((room_found or ["", "0"])[1])
    require(
        room == JOB_ROOM_CPU and cap == JOB_ROOM_MIB,
        f"the services defaults are {room} units and {cap} MiB, not the job room {JOB_ROOM_CPU} and {JOB_ROOM_MIB}",
    )
    for name, need in host["jobs"].items():
        if not need.get("reserve"):
            require(
                (need["cpu_units"], need["memory_mib"]) == (room, cap), f"{name} reserves the whole job room ({room} units, {cap} MiB) but its row says {need}"
            )
    mutations: dict[str, Callable[[dict[str, Any]], object]] = {
        "a whole-room family below the room's CPU": lambda jobs: jobs["features"].update(cpu_units=2048),
        "a whole-room family with another memory": lambda jobs: jobs["silver"].update(memory_mib=8192),
        "a reserve above the room's CPU": lambda jobs: jobs["features_m1"].update(cpu_units=4096),
        "a reserve above the job cap": lambda jobs: jobs["features_m2"].update(memory_mib=30000),
        "a reserve with no memory": lambda jobs: jobs["features_m3"].pop("memory_mib"),
    }
    for label, mutate in mutations.items():
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            shutil.copytree(ROOT / "config/orchestration", root / "config/orchestration")
            path = root / "config/orchestration/host_jobs.json"
            document = json.loads(path.read_text())
            mutate(document["jobs"])
            path.write_text(json.dumps(document))
            error = refuses(LaunchRefused, partial(preflight.load_tables, root))
            require("host_jobs.json" in str(error), f"{label} was not named by the table check: {error}")


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


def scratch_usage_needs_search_permission_on_the_root() -> None:
    """The live host failure: Airflow (uid 50000) reads the scratch root through a read-only mount. With the root at 0700 for another owner, ``is_dir``
    on a family folder raises PermissionError, so the fit probe fails closed. With the root at 0711 (traverse without listing) the sizes come back."""
    if os.geteuid() == 0:
        return  # root passes every permission check, so the failure cannot be reproduced
    steps = importlib.import_module("orchestration.airflow_steps")
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder) / "job_scratch"
        (root / "train" / "work").mkdir(parents=True)
        (root / "train" / "work" / "model.bin").write_bytes(b"x" * 2**20)
        (root / "silver").mkdir()
        try:
            root.chmod(0o600)  # no search bit: the family folders cannot be reached
            refuses(PermissionError, lambda: steps.scratch_usage(root, ["train", "silver"]))
            root.chmod(0o711)  # the search bit that other users get on the host root
            usage = steps.scratch_usage(root, ["train", "silver", "gold"])
        finally:
            root.chmod(0o700)
    require(set(usage) == {"train", "silver", "gold"} and usage["silver"] == 0 and usage["gold"] == 0, f"the families are not all measured: {usage}")
    require(usage["train"] >= 2**20, f"the train folder's bytes are not counted through a 0711 root: {usage['train']}")


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
        environment = launch_environment(scratch)
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
    refuses an override for a container the definition lacks, as ECS does. The job table names the same one container."""
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
            overrides={"containerOverrides": [{"name": "a_container_the_definition_lacks", "environment": []}]},
        ),
    )
    require("a_container_the_definition_lacks" in str(error) and not probe.tasks, f"the fake took an override for a container its definition lacks: {error}")
    rig = Rig()
    run = "metadata_0f0f0f0f"
    features_run = "features_feed0001"
    with world(rig, "metadata_ingest_dag", {"run_id": run, "mode": "real", "features_run": features_run}) as current:
        require(current.step("metadata_run") == "succeeded", "the metadata job did not pass")
    values = rig.environment()
    require(
        values == {"ingest": {"RUN_ID": run, "POLARIS_URI": POLARIS, "FEATURES_RUN": features_run, "MLFLOW_TRACKING_URI": MLFLOW}},
        f"the metadata overrides are not the host container's, with the features run of the form: {values}",
    )
    for missing in (None, "latest", "", "20261104T120000Z-feed0001"):
        bad = SubmissionRequest("metadata_run", run, "real", 1, "metadata_ingest_dag", "metadata_run", "manual__test", missing)
        error = refuses(LaunchRefused, lambda bad=bad: Rig().preflight.prepare(bad))
        require("FEATURES_RUN" in str(error) and "latest" in str(error), f"the features run {missing!r} was not refused by name: {error}")
    request = SubmissionRequest("metadata_run", run, "real", 1, "metadata_ingest_dag", "metadata_run", "manual__test", features_run)
    spec = rig.preflight.prepare(request)
    require(spec is not None and spec.log_container == "ingest", f"the metadata log is not read from the host container: {spec}")
    table = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["jobs"]["metadata"]
    require(
        set(table["containers"]) == {"ingest"} and table["log_container"] == "ingest",
        "the job table's metadata containers are not the host container",
    )


RUNTIME_NAMES = ("submit_and_wait", "from_session", "hai_job_launch_mode")


def dags_hold_no_broker_client() -> None:
    """No DAG file and not the DAG runtime import a broker module or name a broker client, and ``run_step`` hands its request to the launch library alone."""
    for path in [ROOT / "orchestration/dag_runtime.py", *sorted((ROOT / "dags").glob("*.py"))]:
        text = path.read_text()
        tree = ast.parse(text)
        named = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)} | {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        reached = sorted((named & set(RUNTIME_NAMES)) | {name for name in RUNTIME_NAMES if f'"{name}"' in text})
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
    # Ten host twins (nine jobs and the bronze stage job) and the six model sets planned by infra/jobs/host.tf, and the publisher's own host definition
    # (pinned from feat/run3-route-direct's plan).
    require(set(planned) == set(table["jobs"]) and len(planned) == 17, f"seventeen host families were expected: {sorted(planned)}")
    for job, row in table["jobs"].items():
        expected = [
            "init",
            *(host["jobs"].get(job, {}).get("containers") or row["containers"]),
            *(["guard"] if host["jobs"][job].get("like", job) in LAYER_JOBS else []),
        ]
        require(planned[job]["containers"] == expected, f"the {job} host definition holds {planned[job]['containers']}, expected {expected}")
        require(
            planned[job]["start_with_scratch_wipe"] == ["init"],
            f"the {job} host definition does not start with its scratch wipe in the init step: {planned[job]}",
        )
    require(
        host_definitions()["example_dev_job_metadata_host"] == frozenset({"init", "ingest"}),
        "the metadata host definition is not the init step and its one client",
    )


def ingestion_dag_runs_the_publisher_on_the_host() -> None:
    """ingestion_dag is one chain of four tasks: the Run 3 publisher job launches once on its host definition, with the run kind and the capture root of the
    DAG run and nothing staged for it but its code. A trigger that names neither gets the bootstrap kind under raw/. The DAG starts no run after it: the first
    DAG runs the later stages."""
    for trigger, kind, root in (
        ({"run_id": PUBLISHER_RUN, "mode": "real"}, "bootstrap", "raw/"),
        ({"run_id": PUBLISHER_RUN, "mode": "real", "run_kind": "monthly", "capture_root": PUBLISHER_TEST_ROOT}, "monthly", PUBLISHER_TEST_ROOT),
    ):
        rig = Rig()
        with world(rig, "ingestion_dag", dict(trigger)) as current:
            states = current.run_dag("ingestion_dag.py")
        require(
            states == {"preflight": "success", "job": "success", "evidence": "success", "gate": "success"},
            f"the ingestion chain did not pass: {states}",
        )
        started = [(dag, run_id) for dag, run_id, _ in current.dag_runs]
        require(started == [], f"the ingestion DAG started {started} under {root}")
        sent = [arguments for name, arguments in rig.ecs.calls if name == "run_task"]
        require(len(sent) == 1 and sent[0]["taskDefinition"] == "example_dev_job_publisher_host", f"the publisher did not run once on the host: {sent}")
        run = rig.environment()["run"]
        require(
            run
            == {
                "RUN_ID": PUBLISHER_RUN,
                "CODE_SHA256": DIGEST,
                "PUBLISHER_RUN_KIND": kind,
                "HAI_CAPTURE_ROOT": root,
                "HAI_DEADLINE_UTC": run["HAI_DEADLINE_UTC"],
            },
            f"the run container did not get its code digest, the run kind and the capture root alone: {run}",
        )
        require(rig.environment()["fetch"]["CODE_SHA256"] == DIGEST, "the fetch container did not get the staged code digest")
        end = rig.end(PUBLISHER_RUN, "publisher")
        require(end is not None and end["outcome"] == "succeeded", f"the launch record does not say the publisher passed: {end}")
        require(
            not any("publisher_request" in entry or "inputs/publisher" in entry for entry in rig.stage.log),
            f"the launch read a staged publisher file: {rig.stage.log}",
        )


def ingestion_dag_form_refuses_a_capture_root_it_does_not_list() -> None:
    """Airflow refuses the trigger at the form, before any task, when the root is not raw/ or one test folder, and the form holds only the run, the mode, the
    kind and the root. The stand-in checks types and patterns; the kinds are an enum, which the real image check (real_image_dags.form_check) and the launch
    refuse."""
    base = {"run_id": PUBLISHER_RUN, "mode": "real"}
    for label, trigger in (
        ("a root without its slash", base | {"capture_root": "raw"}),
        ("another folder", base | {"capture_root": "lakehouse/"}),
        ("a test folder without a run id", base | {"capture_root": "ingestion_test/t1/"}),
        ("a test folder inside raw", base | {"capture_root": f"raw/{PUBLISHER_TEST_ROOT}"}),
        ("a root with a line break", base | {"capture_root": "raw/\n"}),
    ):
        rig = Rig()
        with world(rig, "ingestion_dag", dict(trigger)) as current:
            error = refuses(FormRefused, partial(current.run_dag, "ingestion_dag.py"))
            require(not current.states, f"{label}: a task ran after the refused form")
        require(not rig.ecs.calls and not rig.records.objects, f"{label}: the refused form reached the cloud or the launch records: {error}")
    declared = ingestion_form()
    require(
        set(declared) == {"run_id", "mode", "run_kind", "capture_root"},
        f"the form holds other fields than the run, the mode, the kind and the root: {declared}",
    )


def ingestion_form() -> dict[str, Any]:
    """The parameters of the shipped ingestion_dag form, as the stand-in reads them."""
    rig = Rig()
    with world(rig, "ingestion_dag", dict(PUBLISHER_PARAMS)) as current:
        current.run_dag("ingestion_dag.py")
    return dict(current.dag_settings["params"])


def publisher_request_is_checked_and_bound() -> None:
    """The launch step refuses a kind or root it does not list before any record or cloud call, runs a good one once and refuses another kind or root for
    a run that launched."""
    rig = Rig()
    with world(rig, "ingestion_dag", dict(PUBLISHER_PARAMS)) as current:
        for label, changes in (
            ("a retry kind", {"run_kind": "retry"}),
            ("no kind", {"run_kind": ""}),
            ("a root without its slash", {"capture_root": "raw"}),
            ("a test folder without a run id", {"capture_root": "ingestion_test/t1/"}),
            ("a root that leaves its folder", {"capture_root": "ingestion_test/../raw/"}),
        ):
            current.params.update(PUBLISHER_PARAMS | changes)
            refuses(LaunchRefused, partial(current.step, "publisher_run"))
            require(not rig.ecs.calls and not rig.records.objects, f"{label}: a refused request reached the cloud or the launch records")
        for root in ("", "ingestion_test/", "ingestion_test/x/", "raw", "lakehouse/"):
            current.params.update(PUBLISHER_PARAMS | {"capture_root": root})
            error = refuses(LaunchRefused, partial(current.step, "publisher_run"))
            require("wrong_capture_root" in str(error), f"capture root {root!r} was not refused by name: {error}")
        require(not rig.ecs.calls, "a bad publisher request reached the cloud")
        current.params.update(PUBLISHER_PARAMS)
        require(current.step("publisher_run") == "succeeded", "a good publisher request did not run")
        values = rig.environment()["run"]
        require(values["PUBLISHER_RUN_KIND"] == "bootstrap" and values["HAI_CAPTURE_ROOT"] == "raw/", f"the checked request did not reach the task: {values}")
        require(
            not any(name.startswith("PUBLISHER_") and name != "PUBLISHER_RUN_KIND" for name in values),
            f"the run container got a variable of the earlier flow: {values}",
        )
        current.params.update(PUBLISHER_PARAMS | {"run_kind": "monthly"})
        error = refuses(LaunchRefused, lambda: current.step("publisher_run", 2))
        require("bound" in str(error) and rig.tasks_started() == 1, f"another kind on a retry was not refused: {error}")
        current.params.update(PUBLISHER_PARAMS | {"capture_root": PUBLISHER_TEST_ROOT})
        error = refuses(LaunchRefused, lambda: current.step("publisher_run", 2))
        require("bound" in str(error) and rig.tasks_started() == 1, f"another root on a retry was not refused: {error}")


def publisher_request_is_refused_before_any_cloud_read() -> None:
    """A malformed publisher request ends in a refusal with no cloud call at all: no launch hold read, no host cap read, no staged-input read, no launch
    record and no ECS call, and an unreadable code binding cannot hold the refusal back for the bounded wait."""
    prior = {"inputs_sha256": "a" * 64, "prior_state": None}
    forms: tuple[tuple[str, dict[str, Any], dict[str, Any] | None], ...] = (
        ("a retry kind", {"run_kind": "retry"}, None),
        ("an empty kind", {"run_kind": ""}, None),
        ("a root without its slash", {"capture_root": "raw"}, None),
        ("a test folder without a run id", {"capture_root": "ingestion_test/t1/"}, None),
        ("a run id that is not a run id", {"run_id": "run_a"}, None),
        ("a run id with a line break", {"run_id": PUBLISHER_RUN + "\n"}, None),
        ("an inputs archive and a prior state", {}, {"kind": "bootstrap", "capture_root": "raw/", **prior}),
        ("an extra key", {}, {"kind": "bootstrap", "capture_root": "raw/", "hosts": "all"}),
        ("no root", {}, {"kind": "bootstrap"}),
    )
    for unreadable in (False, True):
        for label, changes, record in forms:
            rig = Rig()
            holds: list[str] = []

            def hold(holds: list[str] = holds) -> str:
                holds.append("the launch hold was read")
                return "clear"

            rig.preflight.host_launch_hold = hold

            def denied(**_arguments: Any) -> dict[str, Any]:
                raise client_error("AccessDenied", "GetObject")

            started = rig.time.now
            binding = mock.patch.object(rig.stage, "get_object", denied) if unreadable else nullcontext()
            with binding, world(rig, "ingestion_dag", dict(PUBLISHER_PARAMS) | changes) as current:
                if record is None:
                    refuses(LaunchRefused, partial(current.step, "publisher_run"))
                else:
                    request = replace(step_request_of(current), publisher=record)
                    refuses(LaunchRefused, partial(rig.preflight.prepare, request))
            where = f"{label}, binding {'unreadable' if unreadable else 'readable'}"
            require(not holds and rig.cap_reads == 0, f"{where}: the refusal came after a hold or cap read: {holds}, {rig.cap_reads}")
            require(not rig.aws_calls(), f"{where}: the refusal came after cloud calls: {rig.aws_calls()}")
            require(rig.time.now == started, f"{where}: the refusal waited {rig.time.now - started} virtual seconds")
            require(not rig.records.objects and not rig.ecs.calls, f"{where}: a launch record or an ECS call followed")


def step_request_of(current: World) -> SubmissionRequest:
    """The request that the current task of the publisher step would send."""
    current.enter("publisher_run")
    return current.runtime.step_request("publisher_run", current.context)


def publisher_host_definition_matches_the_job_table() -> None:
    """The host table, the job table and the pinned publisher host definition name the same containers and variables as the launch request sets."""
    table, host = importlib.import_module("orchestration.ecs_preflight").load_tables(ROOT)
    planned = json.loads(PUBLISHER_HOST_DEFINITION.read_text())["publisher"]
    job = table["jobs"]["publisher"]
    require(
        planned["containers"] == ["init", "fetch", "run"] and set(job["containers"]) == {"fetch", "run"}, f"the publisher containers differ: {planned}, {job}"
    )
    require(job["log_container"] == "run" and job["acceptance"] == ["run"] and job["deadline_seconds"] == 86_400, f"the publisher job row is wrong: {job}")
    require(table["steps"]["publisher_run"] == "publisher" and "publisher" in host["jobs"], "the publisher step or its host row is missing")
    require(
        job["containers"]["run"]
        == ["RUN_ID", "CODE_SHA256", "PUBLISHER_RUN_KIND", "HAI_CAPTURE_ROOT", "PUBLISHER_ORIGINAL_RUN", "PUBLISHER_RETRY_DAY", "HAI_DEADLINE_UTC"]
        and job.get("needs_publisher_run_request") is True,
        f"the publisher run container does not take its code digest, the run kind, the capture root and the retry variables alone: {job}",
    )


def publisher_host_definition_passes_the_launch_preflight() -> None:
    """The publisher host revision as Terraform plans it (the label of its init container and its task memory, pinned in publisher_host_definition.json)
    passes the same capacity check as every host job revision, so the preflight cannot refuse a revision the jobs stack registers. The two shapes that
    refused the first live launch are refused: a revision without the label, and one whose task memory is not the job memory of its binding."""
    planned = json.loads(PUBLISHER_HOST_DEFINITION.read_text())["publisher"]
    memory, label = planned["memory_mib"], planned["init_labels"]["hai.host_capacity"]
    family = "example_dev_job_publisher_host"
    own = OWN_SIZES["publisher"]["memory_mib"]  # the publisher keeps its own size: its row in the host table is the one source
    require(
        label == BINDING.replace(f"job_memory_mib={JOB_MEMORY}", f"job_memory_mib={own}") and memory == own,
        f"the pinned publisher revision is not bound to the rig's capacity at its own memory: {memory}, {label}",
    )
    rig = Rig()
    revision, job_memory = rig.preflight.checked_revision("publisher", JOB_MEMORY)
    require((revision, job_memory) == (rig.bound[family], memory), f"the planned publisher revision did not pass the check: {revision}, {job_memory}")
    for other_memory, other_label, named in (
        (memory, None, "carries no host capacity binding"),
        (JOB_MEMORY, label, f"{JOB_MEMORY} MiB of task memory, but its binding gives a job {memory} MiB"),
    ):
        rig = Rig()
        rig.ecs.register(family, other_memory, other_label)
        error = refuses(LaunchRefused, partial(rig.preflight.checked_revision, "publisher", JOB_MEMORY))
        require(named in str(error), f"the refusal does not say {named!r}: {error}")


HTTP_LINEAGE = {
    "OPENLINEAGE__TRANSPORT__TYPE": "hai_lineage.http_transport.UtcHttpTransport",  # as infra/services/airflow.tf sets it
    "OPENLINEAGE__TRANSPORT__URL": "http://10.0.0.5:8585",
    "OPENLINEAGE__TRANSPORT__ENDPOINT": "api/v1/openlineage/lineage",
    "OPENLINEAGE__TRANSPORT__TIMEOUT": "5",
}


def extractor_manager_facets(task: Any, state: str, instance: Any) -> Any:
    """What the OpenLineage provider's extractor manager gets from a task (apache-airflow-providers-openlineage 2.20.1, ``get_extractor_class`` and the
    default extractor): no extractor is registered for a decorated Python task's type, so the default one serves a task only when the task itself has a
    ``get_openlineage_facets_on_<state>`` method, and the facets are what that method returns. The real manager is not run: Airflow is not installed here."""
    methods = {name: getattr(task, f"get_openlineage_facets_on_{name}", None) for name in ("start", "complete", "failure")}
    method = methods[state] if any(callable(found) for found in methods.values()) else None
    return method(instance) if callable(method) else None


def job_task_hands_the_listener_its_lineage() -> None:
    """The job task that a collapsed DAG registers carries the lineage of the launch it runs, so the provider's listener gets the run facet with the state of
    the job's versions when the task succeeds or fails (no dataset: the layer's tables travel in per-table events). A task whose preflight refused gives
    none."""
    from dataclasses import replace

    from orchestration import ecs_lineage
    from scripts.orchestration.ecs_lineage_cases import RUN_OF, template_bucket

    run = RUN_OF["silver"]
    steps = importlib.import_module("orchestration.airflow_steps")
    params = {"run_id": run, "mode": "real"}

    def run_dag(rig: Rig, mode: str = "real") -> tuple[dict[str, Any], Any]:
        rig.stage_runs(run)
        evidence = ecs_lineage.S3Evidence(template_bucket(), BUCKET)
        rig.services = replace(rig.services, lineage=steps.lineage_factory(evidence, rig.launcher.store))
        with mock.patch.dict("os.environ", HTTP_LINEAGE), world(rig, "silver_transform_dag", {**params, "mode": mode}) as current:
            current.run_dag("silver_transform_dag.py")
            nodes = {node.task_id: node for node in current.built}
            instance = current.context["ti"]
            found = {state: extractor_manager_facets(nodes["job"], state, instance) for state in ("complete", "failure")}
            return found, {name: extractor_manager_facets(nodes[name], "complete", instance) for name in ("preflight", "evidence", "gate")}

    found, others = run_dag(Rig())
    complete = found["complete"]
    require(complete is not None, "the provider's extractor manager got no lineage from the job task")
    require(
        complete["inputs"] == [] and complete["outputs"] == [],
        f"a layer's task facets name no dataset, its tables travel in their own events: {complete['run_facets']}",
    )
    require(complete["run_facets"]["haiLaunch"]["snapshotState"] == "complete", f"facet {complete['run_facets']['haiLaunch']}")
    failure = found["failure"]
    require(failure is not None and failure["outputs"] == [] and failure["inputs"] == [], f"on failure: {failure}")
    require(all(value is None for value in others.values()), f"a task that launches nothing handed over lineage: {others}")
    refused, _ = run_dag(Rig(), "publish_fixture")
    require(refused["complete"] is None and refused["failure"] is None, f"a task whose preflight refused launched nothing: {refused}")
    require("airflow_lineage_http_enabled" in (ROOT / "infra/services/airflow.tf").read_text(), "the HTTP lineage setting is not in the Airflow service")


def model_set_jobs_hand_over_the_lineage_of_the_features_layer() -> None:
    """Each model set's features DAG launches in its own family (``features_m1`` to ``features_m6``) and, with the production lineage factory installed, its
    job task hands the listener a ``haiLaunch`` facet of the features layer and its evidence step reaches the per-table event dispatch under that family,
    which is the family of its launch records."""
    from dataclasses import replace

    from orchestration import ecs_lineage
    from scripts.orchestration.ecs_lineage_cases import template_bucket

    steps = importlib.import_module("orchestration.airflow_steps")
    rig = Rig()
    _, base = base_run_passed(rig)
    evidence = ecs_lineage.S3Evidence(template_bucket(), BUCKET)
    rig.services = replace(rig.services, lineage=steps.lineage_factory(evidence, rig.launcher.store), evidence=evidence)
    sent: list[str] = []
    seen: dict[str, Any] = {}

    def record(*args: Any) -> None:
        sent.append(str(args[2]))

    with mock.patch.dict("os.environ", HTTP_LINEAGE), mock.patch.object(steps, "send_job_table_events", record):
        for dag_id, model in MODEL_DAGS.items():
            run = model_run(base, model)
            with world(rig, dag_id, {"run_id": run, "mode": "real", BASE_RUN_PARAMETER: base}) as current:
                current.run_dag(f"{dag_id}.py")
                require(current.results.get("gate") == "accepted", f"{dag_id} did not pass: {current.results}")
                nodes = {node.task_id: node for node in current.built}
                seen[model] = extractor_manager_facets(nodes["job"], "complete", current.context["ti"])
    for model, facets in seen.items():
        require(facets is not None, f"the job task of model set {model} handed the listener no lineage")
        launch = facets["run_facets"]["haiLaunch"]
        require(launch["job"] == "features" and launch["attempt"] == 1, f"the facet of model set {model} names {launch}")
        require(
            "not a job" not in str(launch.get("refusal", "")) and "not a job" not in str(launch),
            f"the model set {model} is refused as an unknown job: {launch}",
        )
    require(sorted(sent) == [f"features_{model}" for model in MODEL_DAGS.values()], f"the table events were dispatched for {sent}")


def model_set_lineage_reads_the_base_pins_and_its_scoped_table() -> None:
    """A model set's features run pins a base run (``base_pins.json``, with the digest of the base row) and publishes its one scoped table. With that
    evidence each of the six model DAGs hands the listener a complete facet, its evidence step delivers the table's events and the replay of its attempt
    rebuilds them; the base features run stays on ``gold_pins.json``."""
    from dataclasses import replace

    from orchestration import ecs_lineage
    from scripts.features import feature_sets, features_input, features_publish
    from scripts.orchestration import ecs_lineage_cases as fixtures

    steps = importlib.import_module("orchestration.airflow_steps")
    domain = features_input.base_tables()
    for dag_id, model in MODEL_DAGS.items():
        rig = Rig()
        _, base = base_run_passed(rig)
        run = model_run(base, model)
        bucket = fixtures.template_bucket()
        bucket.objects.update(rig.records.objects)
        base_set = fixtures.tables_of(ecs_lineage.declared_lineage("features")["outputs"])
        base_row = {"run_id": base, "mode": "real", "snapshot_set": base_set, "committed_at": fixtures.END, "base_run": None}
        pin = {
            "schema": 1,
            "run_id": run,
            "mode": "real",
            "namespace": "features",
            "source_run_id": base,
            "snapshot_set": {name: base_set[name] for name in domain},
            "base_row_sha256": feature_sets.base_row_digest(base_row),
        }
        features_input.check_base_pin(pin, run, "real", set(domain))
        outputs = {name: 800 + index for index, name in enumerate(features_publish.publish_plan("real", feature_set=model).tables)}
        folder = f"lakehouse/layer_runs/features/{run}"
        base_record = {"schema": 1, "run_id": base, "mode": "real", "namespace": "features", "snapshot_set": base_set, "feature_set": "base"}
        fixtures.put(bucket, f"lakehouse/layer_runs/features/{base}/features_snapshot.json", base_record)
        sealed = hashlib.sha256(json.dumps(dict(sorted(base_set.items())), separators=(",", ":")).encode()).hexdigest()
        publication = {"run_id": base, "committed_at": fixtures.END, "tables": len(base_set), "snapshot_set_sha256": sealed}
        applied = {"schema": 1, "step": "apply", "run_id": base, "mode": "real", "publication": publication}
        fixtures.put(bucket, f"lakehouse/layer_runs/features/{base}/feast_apply.json", applied)
        fixtures.put(bucket, f"{folder}/base_pins.json", pin)
        record = {"schema": 1, "run_id": run, "mode": "real", "namespace": "features", "snapshot_set": outputs, "feature_set": model, "base_run": base}
        fixtures.put(bucket, f"{folder}/features_snapshot.json", record)
        reads = {name: [{"layer": "features", "table": table} for table in domain] for name in outputs}
        fixtures.put(bucket, f"metadata/dbt/features/{run}/table_inputs.json", {"schema": 1, "layer": "features", "run_id": run, "tables": reads})
        evidence = ecs_lineage.S3Evidence(bucket, BUCKET)
        rig.services = replace(rig.services, lineage=steps.lineage_factory(evidence, rig.launcher.store), evidence=evidence)
        captured: list[Any] = []
        sent: list[str] = []
        original = steps.send_table_events

        def record_source(source: Any, original: Any = original, bucket: Any = bucket, rig: Rig = rig, captured: list[Any] = captured) -> None:
            captured.append(source)
            bucket.objects.update(rig.records.objects)
            original(source)

        def deliver(lines: list[str], *args: Any, sent: list[str] = sent) -> None:
            sent.extend(lines)

        environment = {**HTTP_LINEAGE, ecs_lineage.TRANSPORT_KEY: "synthetic-test-only"}
        with (
            mock.patch.dict("os.environ", environment),
            mock.patch.object(steps, "send_table_events", record_source),
            mock.patch.object(ecs_lineage, "send_within", deliver),
            world(rig, dag_id, {"run_id": run, "mode": "real", BASE_RUN_PARAMETER: base}) as current,
        ):
            current.run_dag(f"{dag_id}.py")
            require(current.results.get("gate") == "accepted", f"{dag_id} did not pass: {current.results}")
            nodes = {node.task_id: node for node in current.built}
            facets = extractor_manager_facets(nodes["job"], "complete", current.context["ti"])
        require(facets is not None, f"the job task of model set {model} handed the listener no lineage")
        launch = facets["run_facets"]["haiLaunch"]
        require(launch["snapshotState"] == "complete", f"the facet of model set {model} is {launch}")
        require(facets["inputs"] == [] and facets["outputs"] == [], f"a layer task facet names no dataset: {facets}")
        require(len(captured) == 1 and len(sent) == 2 * len(outputs), f"model set {model} delivered {len(sent)} lines for {len(outputs)} tables")
        source = captured[0]
        arguments = ["--bucket", BUCKET, "--job", source.job, "--attempt", str(source.attempt)]
        for name in ("run_id", "template", "dag_id", "task_id", "dag_run_id"):
            arguments += ["--" + name.replace("_", "-"), str(source.invocation[name])]
        replayed = ecs_lineage.replay(bucket, arguments)
        require(len(replayed) == 2 + 2 * len(outputs), f"the replay of model set {model} rebuilt {len(replayed)} events")


CASES: dict[str, Callable[[], None]] = {
    "model_set_lineage_reads_the_base_pins_and_its_scoped_table": model_set_lineage_reads_the_base_pins_and_its_scoped_table,
    "job_task_hands_the_listener_its_lineage": job_task_hands_the_listener_its_lineage,
    "model_set_jobs_hand_over_the_lineage_of_the_features_layer": model_set_jobs_hand_over_the_lineage_of_the_features_layer,
    "dags_hold_no_broker_client": dags_hold_no_broker_client,
    "pool_only_when_configured": pool_only_when_configured,
    "gates_refuse_before_any_cloud_call": gates_refuse_before_any_cloud_call,
    "chain_runs_without_a_signed_record": chain_runs_without_a_signed_record,
    "after_needs_the_previous_job_to_pass": after_needs_the_previous_job_to_pass,
    "training_parameters_are_checked": training_parameters_are_checked,
    "aws_mlflow_lane_reaches_the_jobs": aws_mlflow_lane_reaches_the_jobs,
    "local_mlflow_is_refused_visibly": local_mlflow_is_refused_visibly,
    "promotion_needs_no_approval": promotion_needs_no_approval,
    "fresh_run_settles_the_unfinished_launch_itself": fresh_run_settles_the_unfinished_launch_itself,
    "training_form_requires_a_model": training_form_requires_a_model,
    "bronze_request_is_checked_and_bound": bronze_request_is_checked_and_bound,
    "a_full_bronze_run_fits_the_runtask_overrides": a_full_bronze_run_fits_the_runtask_overrides,
    "a_bronze_run_runs_the_code_of_its_ingestion_run": a_bronze_run_runs_the_code_of_its_ingestion_run,
    "the_stage_job_runs_before_the_request_exists_and_the_bronze_job_after": the_stage_job_runs_before_the_request_exists_and_the_bronze_job_after,
    "ingestion_dag_runs_the_publisher_on_the_host": ingestion_dag_runs_the_publisher_on_the_host,
    "ingestion_dag_form_refuses_a_capture_root_it_does_not_list": ingestion_dag_form_refuses_a_capture_root_it_does_not_list,
    "publisher_request_is_checked_and_bound": publisher_request_is_checked_and_bound,
    "publisher_request_is_refused_before_any_cloud_read": publisher_request_is_refused_before_any_cloud_read,
    "publisher_host_definition_matches_the_job_table": publisher_host_definition_matches_the_job_table,
    "publisher_host_definition_passes_the_launch_preflight": publisher_host_definition_passes_the_launch_preflight,
    "serving_request_is_closed_and_derived": serving_request_is_closed_and_derived,
    "code_digest_is_checked": code_digest_is_checked,
    "code_run_is_bound_once": code_run_is_bound_once,
    "control_step_launches_nothing": control_step_launches_nothing,
    "job_runs_once_and_steps_join": job_runs_once_and_steps_join,
    "failed_job_fails_every_step": failed_job_fails_every_step,
    "unavailable_is_bounded": unavailable_is_bounded,
    "unbound_revision_is_refused": unbound_revision_is_refused,
    "revision_bound_to_another_capacity_is_refused": revision_bound_to_another_capacity_is_refused,
    "malformed_capacity_refuses": malformed_capacity_refuses,
    "revision_above_the_host_job_cap_is_refused": revision_above_the_host_job_cap_is_refused,
    "revision_within_the_host_job_cap_runs": revision_within_the_host_job_cap_runs,
    "production_cap_comes_from_the_environment": production_cap_comes_from_the_environment,
    "cap_setting_is_rendered_by_the_services_stack": cap_setting_is_rendered_by_the_services_stack,
    "migration_states_launch_nothing_and_the_procedure_is_written": migration_states_launch_nothing_and_the_procedure_is_written,
    "a_constant_cap_reader_is_caught": a_constant_cap_reader_is_caught,
    "launch_during_the_hold_is_refused": launch_during_the_hold_is_refused,
    "a_hold_set_after_the_preflight_withholds_runtask": a_hold_set_after_the_preflight_withholds_runtask,
    "the_production_launcher_rechecks_at_send": the_production_launcher_rechecks_at_send,
    "a_hold_set_after_the_probe_withholds_runtask": a_hold_set_after_the_probe_withholds_runtask,
    "the_fit_probe_never_offers_more_than_the_cap": the_fit_probe_never_offers_more_than_the_cap,
    "a_slow_authorization_withholds_runtask": a_slow_authorization_withholds_runtask,
    "a_padded_clear_value_holds_launches": a_padded_clear_value_holds_launches,
    "a_present_or_unreadable_apply_lock_refuses_launch": a_present_or_unreadable_apply_lock_refuses_launch,
    "an_apply_lock_taken_after_the_probe_withholds_runtask": an_apply_lock_taken_after_the_probe_withholds_runtask,
    "a_cleared_hold_lets_the_launch_run": a_cleared_hold_lets_the_launch_run,
    "bound_revision_runs_by_its_exact_arn": bound_revision_runs_by_its_exact_arn,
    "five_dags_run_end_to_end": five_dags_run_end_to_end,
    "promotion_and_serving_dags_run_end_to_end": promotion_and_serving_dags_run_end_to_end,
    "dags_run_with_their_form_defaults": dags_run_with_their_form_defaults,
    "collapsed_job_dags_are_one_chain": collapsed_job_dags_are_one_chain,
    "base_dag_takes_its_gold_run": base_dag_takes_its_gold_run,
    "features_job_names_its_feature_set": features_job_names_its_feature_set,
    "model_dags_are_one_chain_each": model_dags_are_one_chain_each,
    "model_dag_takes_its_base_run": model_dag_takes_its_base_run,
    "model_dag_needs_the_base_job_to_pass": model_dag_needs_the_base_job_to_pass,
    "model_run_code_is_bound": model_run_code_is_bound,
    "base_dag_triggers_six_model_dags": base_dag_triggers_six_model_dags,
    "one_model_failure_stops_only_its_dag": one_model_failure_stops_only_its_dag,
    "model_dags_share_the_host_pool": model_dags_share_the_host_pool,
    "model_sets_have_their_own_families": model_sets_have_their_own_families,
    "model_sets_launch_in_their_own_task_definitions": model_sets_launch_in_their_own_task_definitions,
    "six_model_sets_fit_the_host_in_two_waves": six_model_sets_fit_the_host_in_two_waves,
    "model_jobs_weigh_their_pool_slots": model_jobs_weigh_their_pool_slots,
    "graph_rules_cover_the_feature_dags": graph_rules_cover_the_feature_dags,
    "model_dags_trigger_their_drift_check": model_dags_trigger_their_drift_check,
    "drift_dag_takes_its_model_and_features_run": drift_dag_takes_its_model_and_features_run,
    "drift_job_takes_its_model_and_features_run": drift_job_takes_its_model_and_features_run,
    "drift_job_needs_its_model_run_to_pass": drift_job_needs_its_model_run_to_pass,
    "drift_attempt_ids_and_form": drift_attempt_ids_and_form,
    "drift_attempt_runs_the_code_staged_for_its_own_run": drift_attempt_runs_the_code_staged_for_its_own_run,
    "drift_attempt_without_staged_code_is_refused_by_name": drift_attempt_without_staged_code_is_refused_by_name,
    "drift_attempt_and_run_must_agree": drift_attempt_and_run_must_agree,
    "live_drift_job_takes_its_model_and_type": live_drift_job_takes_its_model_and_type,
    "live_drift_job_is_refused_by_name_before_any_launch": live_drift_job_is_refused_by_name_before_any_launch,
    "training_run_fits_its_model_only": training_run_fits_its_model_only,
    "the_pipeline_runs_no_model_less_drift_layer": the_pipeline_runs_no_model_less_drift_layer,
    "the_binding_gate_takes_only_a_pipeline_run": the_binding_gate_takes_only_a_pipeline_run,
    "graph_rules_cover_the_model_drift_trigger": graph_rules_cover_the_model_drift_trigger,
    "launch_modules_are_in_the_mount_list": launch_modules_are_in_the_mount_list,
    "host_table_matches_the_job_table": host_table_matches_the_job_table,
    "host_table_agrees_with_the_reservations": host_table_agrees_with_the_reservations,
    "every_family_fits_the_host_scratch_volume": every_family_fits_the_host_scratch_volume,
    "missing_launch_module_is_named": missing_launch_module_is_named,
    "services_come_from_named_environment": services_come_from_named_environment,
    "services_build_the_session_adapter": services_build_the_session_adapter,
    "scratch_usage_counts_only_what_it_can_read": scratch_usage_counts_only_what_it_can_read,
    "scratch_usage_needs_search_permission_on_the_root": scratch_usage_needs_search_permission_on_the_root,
    "metadata_uses_the_host_containers": metadata_uses_the_host_containers,
    "host_definitions_hold_the_init_step_and_the_layer_guards": host_definitions_hold_the_init_step_and_the_layer_guards,
}
