"""Run the other DAGs of one pipeline run in order and wait for each: ingestion, bronze, silver, gold and the features base, then every model's route after it.

This is the single entry point. The owner starts it by hand with ``run_id`` set to ``pipeline_<hex>`` (``trigger_dag --dag pipeline_dag`` makes a fresh hex and
unpauses every DAG of the pipeline), and the last day of a scheduled ingestion run's retry period starts it from the bronze stage (``ingestion_retry_dag``).
``start_from`` names the first stage to run: a stage before it does nothing (its task succeeds at once and no run is created), so a run whose ingestion is over
starts at bronze, and ``ingestion`` runs the whole pipeline. ``run_kind`` and ``capture_root`` are the ingestion DAG's own settings and reach only that stage.

Each stage task starts its DAG with ``dag_runtime.join_child`` and waits for that run's own outcome. A child run takes the pipeline run's hex and attempt in its
name (``silver_<hex>``) and Airflow's run ID of a child is its run ID. A child that exists already (an earlier start, a cleared parent) is joined, never created
again or reset, and only when it was started with the settings this DAG would start it with (its run ID, the producer run of the stage before it, the mode and
every other parameter of its form, with the form's defaults filled in): another configuration fails the stage by name before any wait. A child that succeeded
is adopted, one that is running is waited for and one that failed fails the stage, so no later stage ever starts after a failed one. Every task runs under
``all_success``, which this DAG relies on alone: it has no skipped task and no branch, so a failed stage can never be taken for a skipped one. Only the final
gate runs after a failure, and it fails unless every task succeeded.

``check_dags`` runs first and refuses a run while a DAG of the pipeline is paused or not registered (a fresh deployment creates DAGs paused). ``bind_code`` runs
after ingestion and before bronze, whatever the start: it binds every layer run of the pipeline to the code the bronze run runs (the code of its ingestion run,
which that run bound at its own launch), and the run of each model that the features base starts. ``await_downstream`` then waits for each model's route after
the features base (its features run, its drift check, its training run when the gate trains and, for a promotion model, the research that training starts),
checks that each of those runs was started with the settings the DAG before it gives it, and fails when any route failed. The pipeline's result is the result of
those runs. It does not wait for the catalog start of a training run (a failed catalog never fails a pipeline) or for the promotion and serving deploys. The
research run it waits for starts those itself: ``research_dag`` runs the canary check, the promotion, the online release and the owner notice with no owner
approval, and starts the serving deploys. ``model_promotion_dag`` stays for an explicit promote or rollback.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.sdk import DAG, Param, TriggerRule, get_current_context, task

from orchestration import dag_runtime
from orchestration.ingestion_schedule import RUN_ID_PATTERN as INGESTION_RUN_PATTERN
from orchestration.training_contract import CATALOG_DAG, PIPELINE_ACTIVE_DAGS, PIPELINE_DAG, PIPELINE_RUN, PIPELINE_STAGES, chain_layer_run, run_id_pattern

# The stage, the DAG it runs and the parameter that names the stage before it (the producer run the stage reads).
STAGES = (
    ("ingestion", "ingestion_dag", None),
    ("bronze", "bronze_dag", None),
    ("silver", "silver_transform_dag", None),
    ("gold", "gold_transform_dag", "silver_snapshot_run"),
    ("features", "features_base_dag", "gold_snapshot_run"),
)
START_NAMES = tuple(layer for layer, _, _ in STAGES)
BIND_TASK = "bind_code"
# Each stage's wait ends after its own limit. A job's deadline is six hours (the ingestion job's is a day); the bronze stage covers its own DAG's run limit (an
# hour for the stage job, eight for the bronze job and an hour more). The model routes run side by side, so one limit covers them all.
STAGE_LIMITS = {
    "ingestion": timedelta(hours=27),
    "bronze": timedelta(hours=10),
    "silver": timedelta(hours=8),
    "gold": timedelta(hours=8),
    "features": timedelta(hours=8),
}
CHECK_LIMIT = timedelta(minutes=15)
BIND_LIMIT = timedelta(minutes=30)
ROUTES_LIMIT = timedelta(hours=30)
GATE_LIMIT = timedelta(minutes=15)
PIPELINE_LIMIT = sum(STAGE_LIMITS.values(), CHECK_LIMIT + BIND_LIMIT + ROUTES_LIMIT + GATE_LIMIT + timedelta(hours=1))
# The DAGs that must be active for a run: every DAG of the pipeline but this one (it is running) and the catalog (a paused catalog fails no pipeline).
REQUIRED_DAGS = tuple(dag for dag in PIPELINE_ACTIVE_DAGS if dag not in (PIPELINE_DAG, CATALOG_DAG))
TASKS = ("check_dags", *(f"run_{layer}" for layer in START_NAMES[:1]), BIND_TASK, *(f"run_{layer}" for layer in START_NAMES[1:]), "await_downstream")


def layer_run(layer: str, run: str) -> str:
    """The run ID of ``layer`` in the pipeline run ``run``: the layer's name and the pipeline run's hex and attempt."""
    if layer not in START_NAMES:
        raise ValueError(f"{layer} is not a stage of the pipeline")
    return chain_layer_run(layer, run)


def start_from() -> str:
    """The stage the run starts from, checked here too (a run created through the API is checked by Airflow's form, not by this DAG)."""
    start = str(get_current_context()["params"]["start_from"])
    if start not in START_NAMES:
        raise ValueError(f"start_from must be one of {', '.join(START_NAMES)}, not {start!r}")
    return start


def stage_conf(index: int, params: dict[str, Any]) -> dict[str, str]:
    """The settings of one stage's DAG run: its run ID and the mode, the ingestion form's own values and the producer run of the stage before it."""
    layer, _, parent_key = STAGES[index]
    run = str(params["run_id"])
    conf = {"run_id": layer_run(layer, run)}
    conf["mode"] = str(params["mode"])  # not a dict literal: the form check reads a literal with a "mode" key as the form's own default
    if layer == "ingestion":
        conf |= {"run_kind": str(params["run_kind"]), "capture_root": str(params["capture_root"])}
    if parent_key is not None:
        conf[parent_key] = layer_run(STAGES[index - 1][0], run)
    return conf


def stage_task(index: int) -> Any:
    """The task of one stage: nothing for a stage before ``start_from``, otherwise the join of the stage's own DAG run."""
    layer, dag_id, _ = STAGES[index]

    @task(task_id=f"run_{layer}", retries=0, execution_timeout=STAGE_LIMITS[layer])
    def run_stage() -> str:
        start = start_from()
        if START_NAMES.index(layer) < START_NAMES.index(start):
            return f"the {layer} stage was not run: the run starts from {start}"
        conf = stage_conf(index, get_current_context()["params"])
        return dag_runtime.join_child(dag_id, conf["run_id"], conf, STAGE_LIMITS[layer])

    return run_stage().operator


with DAG(
    dag_id="pipeline_dag",  # a literal, because the DAG inventory reads the DAG IDs from the files; a case keeps it equal to PIPELINE_DAG
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=PIPELINE_LIMIT,
    tags=["orchestration", PIPELINE_RUN],
    params={
        "run_id": Param(type="string", pattern=f"^{run_id_pattern(PIPELINE_RUN)}$"),
        "mode": Param("real", type="string", enum=["real"]),
        "start_from": Param(PIPELINE_STAGES[0], type="string", enum=list(PIPELINE_STAGES)),
        "run_kind": Param("bootstrap", type="string", enum=["bootstrap", "monthly"]),
        "capture_root": Param("raw/", type="string", pattern=rf"^(raw/|ingestion_test/{INGESTION_RUN_PATTERN}/)$(?![\s\S])"),
    },
    user_defined_macros={"layer_run": layer_run},
    default_args={"retries": 0},
) as dag:

    @task(task_id="check_dags", retries=0, execution_timeout=CHECK_LIMIT)
    def check_dags() -> str:
        start_from()
        return dag_runtime.require_active(REQUIRED_DAGS)

    @task(task_id=BIND_TASK, retries=0, execution_timeout=BIND_LIMIT)
    def bind_code() -> str:
        return dag_runtime.run_step(BIND_TASK)

    @task(task_id="await_downstream", retries=0, execution_timeout=ROUTES_LIMIT)
    def await_downstream() -> str:
        return dag_runtime.join_model_routes(layer_run("features", str(get_current_context()["params"]["run_id"])), ROUTES_LIMIT)

    @task(task_id="gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=GATE_LIMIT)
    def gate() -> str:
        states = dag_runtime.task_states(TASKS)
        failures = [f"{name} {state}" for name, state in states.items() if state != "success"]
        if failures:
            raise RuntimeError(f"the pipeline gate rejected the run: {', '.join(failures)}")
        return "accepted"

    previous = check_dags().operator
    for operator in (
        stage_task(0),
        bind_code().operator,
        *(stage_task(position) for position in range(1, len(STAGES))),
        await_downstream().operator,
        gate().operator,
    ):
        previous >> operator
        previous = operator
