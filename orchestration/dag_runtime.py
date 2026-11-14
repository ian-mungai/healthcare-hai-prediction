"""Build manual pipeline tasks that launch their jobs through the direct ECS launch library and retain failed runs.

A DAG of one job is one chain of four tasks (``build_job_chain``): ``preflight`` (the gates, no launch), ``job`` (the one ECS task of the job, in the
launch pool), ``evidence`` (the durable launch record says the job and its acceptance checks passed) and ``gate`` (runs after a failure too, fails the DAG
run unless the three passed and carries the asset event). The features and training DAGs still have one task per step (``build_tasks``): ``run_step`` runs
a task's fixed template through ``orchestration/airflow_steps.py``, where a job step launches or joins its job's one ECS task and a control step (acquire,
seal, release, the gates) launches nothing.
There is no other path: the DAGs hold no broker client.
"""

from __future__ import annotations

import importlib
import json
import logging
import math
import os
import re
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import Asset, TriggerRule, get_current_context, task

from orchestration.interface import SubmissionRequest
from orchestration.training_contract import DAGS, FEATURE_DAGS, LAUNCH_LANES, parameters_for, serving_parameters_for

LOG = logging.getLogger(__name__)
RESEARCH_DAG = "research_dag"
STEP_TIMEOUT = timedelta(hours=26)
# The DAG parameter that selects the producer run a pin reads ("latest" when absent), and the asset each committing task emits.
SELECTIONS = {"pin_silver": "silver_snapshot_run", "pin_gold": "gold_snapshot_run"}
# A model set's features DAG builds from the base run that the base DAG published: its form names that run, not a gold run.
MODEL_SELECTIONS = {"pin_gold": "base_snapshot_run"}
# The serving deployment's form names the research run whose record names the candidate of a canary.
SERVING_DAG = "serving_deploy_dag"
SERVING_SELECTIONS = {"serve_deploy": "research_run"}
# The drift check reads one model run's published set: its form names that run, and the check's job needs the features job of that run to have passed.
DRIFT_DAG = "drift_monitoring_dag"
DRIFT_SELECTIONS = {"drift_run": "features_run"}
ASSETS = {"features_commit": "features"}
# The time limits of the one-job chain's tasks that start no job.
PREFLIGHT_TIMEOUT = timedelta(minutes=30)
CHECK_TIMEOUT = timedelta(minutes=15)
CHAIN_TASKS = ("preflight", "job", "evidence")
# The step of the Run 3 publisher: its DAG form names the run kind and the capture root, and the launch checks them.
PUBLISHER_TEMPLATE = "publisher_run"
RESEARCH_TASKS = ("ready", "research", "chain")
# The module that launches a step, and the environment setting that names the pool of host jobs.
LAUNCH_MODULE = "orchestration.airflow_steps"
POOL_SETTING = "HAI_LAUNCH_POOL"
POOL_NAME = re.compile(r"[A-Za-z0-9_.-]{1,200}")
DEFAULT_POOL = "default_pool"
# The pool has one slot for each POOL_SLOT_CPU_UNITS of CPU the host leaves for jobs (the services stack computes both and renders them into every Airflow
# container). A job takes the slots of its share of the host: the larger of its CPU share and its memory share of the host job cap.
POOL_SLOTS_SETTING = "HAI_LAUNCH_POOL_SLOTS"
POOL_SLOT_CPU_SETTING = "HAI_LAUNCH_POOL_SLOT_CPU_UNITS"
JOB_CAP_SETTING = "HAI_LAUNCH_HOST_JOB_CAP_MIB"
HOST_TABLE = Path(__file__).resolve().parents[1] / "config/orchestration/host_jobs.json"


def launch_pool() -> str:
    """The pool that serializes host jobs where the deployment names one (``HAI_LAUNCH_POOL``), else Airflow's default pool."""
    name = os.environ.get(POOL_SETTING)
    if name is None:
        return DEFAULT_POOL
    if not POOL_NAME.fullmatch(name):
        raise ValueError(f"{POOL_SETTING} must be a pool name of letters, digits, dots, dashes and underscores")
    return name


def whole_setting(name: str) -> int:
    """The positive whole number in the setting ``name``, or a refusal that names the setting."""
    raw = os.environ.get(name, "")
    if not raw.isdecimal() or int(raw) < 1:
        raise ValueError(f"{name} must be a whole number above 0 while {POOL_SLOTS_SETTING} is set")
    return int(raw)


def pool_slots_of(family: str | None) -> int:
    """The pool slots of a job in ``family``: all of them for a family that reserves the whole job room (``None`` too, as the steps of the other DAGs
    do), so such a job runs alone; the larger of its CPU slots and its memory slots for a family that reserves its own size, so several run together
    (``config/orchestration/host_jobs.json``). A deployment that sets no slot count has the one-slot pool: one slot each."""
    if POOL_SLOTS_SETTING not in os.environ:
        return 1
    slots = whole_setting(POOL_SLOTS_SETTING)
    need = json.loads(HOST_TABLE.read_text())["jobs"][family] if family is not None else {}
    if not need.get("reserve"):
        return slots
    by_cpu = math.ceil(int(need["cpu_units"]) / whole_setting(POOL_SLOT_CPU_SETTING))
    by_memory = math.ceil(int(need["memory_mib"]) * slots / whole_setting(JOB_CAP_SETTING))
    return min(slots, max(by_cpu, by_memory))


def pool_settings(family: str | None = None) -> dict[str, Any]:
    """The task settings that put a launching task in the host pool: its name, and its slots where the deployment sets a slot count."""
    settings: dict[str, Any] = {"pool": launch_pool()}
    if POOL_SLOTS_SETTING in os.environ:
        settings["pool_slots"] = pool_slots_of(family)
    return settings


def selections_of(dag_id: str) -> dict[str, str]:
    """The DAG parameter that names the producer run of each template's job in ``dag_id``."""
    if dag_id == SERVING_DAG:
        return SERVING_SELECTIONS
    if dag_id == DRIFT_DAG:
        return DRIFT_SELECTIONS
    return MODEL_SELECTIONS if dag_id in FEATURE_DAGS else SELECTIONS


def step_request(template: str, context: dict[str, Any]) -> SubmissionRequest:
    """The request of the current task's fixed template."""
    params = context["params"]
    instance = context["ti"]
    selections = selections_of(instance.dag_id)
    selected = params.get(selections[template], "latest") if template in selections else "latest"
    training = None
    mode = str(params["mode"])
    if instance.dag_id == RESEARCH_DAG:
        raw = {name: params[name] for name in ("base_run", "population", "hai_types", "mlflow")}
        training = parameters_for(RESEARCH_DAG, raw, LAUNCH_LANES)  # the research step runs on AWS: mlflow=local is refused by name
    elif instance.dag_id == SERVING_DAG:
        training = serving_parameters_for({name: params[name] for name in ("hai_type", "deploy_mode")})  # a closed request: no version and no share
    elif instance.dag_id in DAGS:
        names = (
            ("features_run", "population", "hai_types", "trigger", "mlflow", "model")
            if instance.dag_id == "training_dag"
            else ("hai_type", "model_version", "mlflow")
        )
        raw = {name: params[name] for name in names}
        if instance.dag_id == "model_promotion_dag":
            raw["promotion_mode"] = mode
            mode = "real"
        training = parameters_for(instance.dag_id, raw, LAUNCH_LANES)  # every step runs on AWS: mlflow=local is refused by name
    elif instance.dag_id == DRIFT_DAG:
        training = {"model": params["model"]}  # the model whose set the drift job checks; the preflight refuses a model outside the contract
    publisher = {"kind": params["run_kind"], "capture_root": params["capture_root"]} if template == PUBLISHER_TEMPLATE else None
    return SubmissionRequest(
        template,
        str(params["run_id"]),
        mode,
        int(instance.try_number),
        instance.dag_id,
        instance.task_id,
        instance.run_id,
        None if selected == "latest" else str(selected),
        training=training,
        publisher=publisher,
    )


def launch_module() -> Any:
    """The module that launches a step. It is imported by name when a step runs, so a container without it still parses the DAGs and the step names it."""
    try:
        airflow_steps = importlib.import_module(LAUNCH_MODULE)  # dynamic: the launch library's step, mounted where the DAGs launch jobs
    except ImportError as error:
        raise RuntimeError(f"{LAUNCH_MODULE} is not in this container's mounts, so the step cannot launch its job: {error}") from None
    return airflow_steps


def run_step(template: str) -> str:
    """Run the current task's fixed template through the direct ECS launch library and return its successful state."""
    context = get_current_context()
    return str(launch_module().launch_step(step_request(template, context), context, selections_of(context["ti"].dag_id)))


def collapsed_step(kind: str, template: str, built: Callable[[Any], None] | None = None) -> Any:
    """Run one task of the one-job chain whose job ``template`` names: its preflight, its launch or its evidence. The launch passes ``built`` the launch
    operator before it runs."""
    context = get_current_context()
    steps = launch_module()
    request = step_request(template, context)
    selections = selections_of(context["ti"].dag_id)
    if kind == "job":
        return steps.job_step(request, context, selections, built)
    return {"preflight": steps.preflight_step, "evidence": steps.evidence_step}[kind](request, context, selections)


class JobLineage:
    """The lineage hooks of a job task. The OpenLineage listener asks the operator it was given, here the task that runs the launch operator inside it, for
    ``get_openlineage_facets_on_complete`` and ``..._on_failure``, so the task forwards them to the launch operator once the launch has built it. A task
    that failed before it launched has no launch operator and gives the listener nothing."""

    def __init__(self) -> None:
        self.launch: Any = None

    def built(self, operator: Any) -> None:
        self.launch = operator

    def get_openlineage_facets_on_complete(self, task_instance: Any) -> Any:
        return None if self.launch is None else self.launch.get_openlineage_facets_on_complete(task_instance)

    def get_openlineage_facets_on_failure(self, task_instance: Any) -> Any:
        return None if self.launch is None else self.launch.get_openlineage_facets_on_failure(task_instance)


def build_job_chain(template: str, asset: str | None = None, family: str | None = None) -> Any:
    """Create preflight, job, evidence and gate for the job that ``template`` names and return the gate operator. The gate carries ``asset``. ``family`` is
    the host family the DAG launches in when it is not the job's own (a model set's); it sets the slots of the job task."""

    @task(task_id="preflight", retries=0, execution_timeout=PREFLIGHT_TIMEOUT)
    def preflight() -> str:
        return str(collapsed_step("preflight", template))

    lineage = JobLineage()

    @task(task_id="job", retries=0, execution_timeout=STEP_TIMEOUT, **pool_settings(family))
    def job() -> str:
        return str(collapsed_step("job", template, lineage.built))

    @task(task_id="evidence", retries=0, execution_timeout=CHECK_TIMEOUT)
    def evidence() -> Any:
        return collapsed_step("evidence", template)

    @task(task_id="gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=CHECK_TIMEOUT, outlets=[Asset(asset)] if asset else [])
    def gate() -> str:
        states = task_states(CHAIN_TASKS)
        failures = [name for name, state in states.items() if state != "success"]
        if failures:
            raise RuntimeError(f"the job gate rejected the run: {', '.join(f'{name} {states[name]}' for name in failures)}")
        return "accepted"

    first, launched, found, last = preflight().operator, job().operator, evidence().operator, gate().operator
    launched.get_openlineage_facets_on_complete = lineage.get_openlineage_facets_on_complete
    launched.get_openlineage_facets_on_failure = lineage.get_openlineage_facets_on_failure
    first >> launched >> found >> last
    return last


def research_request() -> SubmissionRequest:
    """The research DAG's request: its research run, with the form's closed parameters. A form that names the local lane is refused here, by name."""
    context = get_current_context()
    return step_request("research", context)


def build_research_tasks() -> Any:
    """Create ready, research, chain and gate for ``research_dag`` and return the gate operator.

    ``ready`` launches nothing: it ends skipped, not failed, until m1, m2 and m4 each have a passed training run on the base run, and every task after it
    is then skipped too. ``research`` is the one-job step of the promote family (its preflight and launch run in the one task, joined by a retry). ``chain``
    runs the promotion chain of each infection type that has a challenger (``promotion_chain``). ``gate`` runs after a failure too and fails the DAG run unless
    the three passed, or every one was skipped.
    """

    @task(task_id="ready", retries=0, execution_timeout=CHECK_TIMEOUT)
    def ready() -> str:
        from airflow.sdk.exceptions import AirflowSkipException

        missing = launch_module().research_missing_models(research_request())
        if missing:
            raise AirflowSkipException(f"research waits for a passed training run of {', '.join(missing)} on this base run")
        return "ready"

    @task(task_id="research", retries=0, execution_timeout=STEP_TIMEOUT, pool=launch_pool())
    def research() -> str:
        request = research_request()
        context = get_current_context()
        steps = launch_module()
        steps.job_step(request, context, {})
        return str(steps.evidence_step(request, context, {})["run_id"])

    @task(task_id="chain", retries=0, execution_timeout=STEP_TIMEOUT, pool=launch_pool())
    def chain() -> str:
        return str(launch_module().research_chain_step(research_request(), get_current_context()))

    @task(task_id="gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=CHECK_TIMEOUT)
    def gate() -> str:
        states = task_states(RESEARCH_TASKS)
        if all(state == "skipped" for state in states.values()):
            return "skipped"
        failures = [name for name, state in states.items() if state != "success"]
        if failures:
            raise RuntimeError(f"the research gate rejected the run: {', '.join(f'{name} {states[name]}' for name in failures)}")
        return "accepted"

    first, researched, chained, last = ready().operator, research().operator, chain().operator, gate().operator
    first >> researched >> chained >> last
    return last


def task_states(task_ids: tuple[str, ...]) -> dict[str, str | None]:
    """Read this DAG run's named task states from Airflow's task-instance API."""
    ti = get_current_context()["ti"]
    found = ti.get_task_states(dag_id=ti.dag_id, task_ids=list(task_ids), run_ids=[ti.run_id])
    states = found.get(ti.run_id, {})
    return {name: states.get(name) for name in task_ids}


def build_tasks(work_steps: tuple[str, ...], family: str) -> Any:
    """Create an ordered work chain, an always-run catalog release and a failure-preserving release leaf, and return the release operator."""

    def step(name: str, rule: TriggerRule = TriggerRule.ALL_SUCCESS) -> Any:
        outlets = [Asset(ASSETS[name])] if name in ASSETS else []

        @task(task_id=name, trigger_rule=rule, retries=0, execution_timeout=STEP_TIMEOUT, outlets=outlets, **pool_settings())
        def run() -> str:
            return run_step(name)

        return run().operator

    work = [step(name) for name in work_steps]
    for before, after in zip(work, work[1:], strict=False):
        before >> after
    catalog_release = step("catalog_release", TriggerRule.ALL_DONE)
    for operation in work:
        operation >> catalog_release
    predecessors = (*work_steps, "catalog_release")

    @task(task_id="final_gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=STEP_TIMEOUT)
    def final_gate() -> str:
        states = task_states(predecessors)
        failures = [name for name, state in states.items() if state != "success"]
        verdict = "rejected" if failures else "accepted"
        run_step(f"{family}_final_gate_{verdict}")
        if failures:
            LOG.warning("Pipeline gate rejected tasks: %s", ", ".join(failures))
        return verdict

    @task(task_id="release_run", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=STEP_TIMEOUT)
    def release_run() -> str:
        state = task_states(("final_gate",))["final_gate"]
        verdict = get_current_context()["ti"].xcom_pull(task_ids="final_gate") if state == "success" else None
        accepted = state == "success" and verdict == "accepted"
        run_step("release_accepted" if accepted else "release_rejected")
        if not accepted:
            raise RuntimeError("The pipeline failed its final gate; its lock and recovery inputs are retained")
        return "released"

    gate = final_gate().operator
    release = release_run().operator
    for operation in [*work, catalog_release]:
        operation >> gate
    gate >> release
    return release
