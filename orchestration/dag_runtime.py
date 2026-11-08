"""Build manual pipeline tasks that launch their jobs through the direct ECS launch library and retain failed runs.

``run_step`` runs a task's fixed template through ``orchestration/airflow_steps.py``: a job step launches or joins its job's one ECS task, and a control step
(acquire, seal, release, the gates) launches nothing. There is no other path: the DAGs hold no broker client.
"""

from __future__ import annotations

import importlib
import logging
import os
import re
from datetime import timedelta
from typing import Any

from airflow.sdk import Asset, TriggerRule, get_current_context, task

from orchestration.interface import SubmissionRequest
from orchestration.training_contract import DAGS, LAUNCH_LANES, parameters_for

LOG = logging.getLogger(__name__)
STEP_TIMEOUT = timedelta(hours=26)
# The DAG parameter that selects the producer run a pin reads ("latest" when absent), and the asset each committing task emits.
SELECTIONS = {"pin_silver": "silver_snapshot_run", "pin_gold": "gold_snapshot_run"}
ASSETS = {"commit_publish": "gold", "features_commit": "features"}
# The module that launches a step, and the environment setting that names the pool of host jobs.
LAUNCH_MODULE = "orchestration.airflow_steps"
POOL_SETTING = "HAI_LAUNCH_POOL"
POOL_NAME = re.compile(r"[A-Za-z0-9_.-]{1,200}")
DEFAULT_POOL = "default_pool"


def launch_pool() -> str:
    """The pool that serializes host jobs where the deployment names one (``HAI_LAUNCH_POOL``), else Airflow's default pool."""
    name = os.environ.get(POOL_SETTING)
    if name is None:
        return DEFAULT_POOL
    if not POOL_NAME.fullmatch(name):
        raise ValueError(f"{POOL_SETTING} must be a pool name of letters, digits, dots, dashes and underscores")
    return name


def step_request(template: str, context: dict[str, Any]) -> SubmissionRequest:
    """The request of the current task's fixed template."""
    params = context["params"]
    selected = params.get(SELECTIONS[template], "latest") if template in SELECTIONS else "latest"
    instance = context["ti"]
    training = None
    mode = str(params["mode"])
    if instance.dag_id in DAGS:
        names = (
            ("features_run", "population", "hai_types", "trigger", "mlflow") if instance.dag_id == "training_dag" else ("hai_type", "model_version", "mlflow")
        )
        raw = {name: params[name] for name in names}
        if instance.dag_id == "model_promotion_dag":
            raw["promotion_mode"] = mode
            mode = "real"
        training = parameters_for(instance.dag_id, raw, LAUNCH_LANES)  # every step runs on AWS: mlflow=local is refused by name
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
    return str(launch_module().launch_step(step_request(template, context), context, SELECTIONS))


def task_states(task_ids: tuple[str, ...]) -> dict[str, str | None]:
    """Read this DAG run's named task states from Airflow's task-instance API."""
    ti = get_current_context()["ti"]
    found = ti.get_task_states(dag_id=ti.dag_id, task_ids=list(task_ids), run_ids=[ti.run_id])
    states = found.get(ti.run_id, {})
    return {name: states.get(name) for name in task_ids}


def build_tasks(work_steps: tuple[str, ...], family: str) -> None:
    """Create an ordered work chain, an always-run catalog release and a failure-preserving sole release leaf."""

    def step(name: str, rule: TriggerRule = TriggerRule.ALL_SUCCESS) -> Any:
        outlets = [Asset(ASSETS[name])] if name in ASSETS else []

        @task(task_id=name, trigger_rule=rule, retries=0, execution_timeout=STEP_TIMEOUT, outlets=outlets, pool=launch_pool())
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
