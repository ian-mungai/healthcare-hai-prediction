"""Run silver, gold and the features base in order after one bronze run, with no calendar schedule.

``ingestion_dag`` starts this DAG once the retry period is over (a hand-started ingestion run starts it at once), with ``run_id`` set to ``bronze_<hex>`` of the
ingestion run (``orchestration/ingestion_schedule.py``); the owner may start it by hand too. The first task binds every layer run of the chain to the bronze
run's staged code. The bronze job then loads the sources of the staged bronze request under that same run ID (``bronze_load``); a source file that no run of the
ingestion run's retry period captured keeps its earlier capture, and the run record and the owner notice name each such carried-over file.
Each layer then runs as a triggered DAG run and the chain waits for its success before the next layer, so a failed layer or a failed acceptance check stops
the chain before the later layers. Every wait has a limit. The features base DAG starts the six model feature DAGs once it published, and each of those runs
its own model's checks and training, so the chain does not wait for them and names no model.

Every run ID is the layer's name and the hex and attempt of the bronze run (``silver_<hex>``), and the Airflow run ID of a layer is its run ID. A second trigger
for the same bronze run names the same DAG runs, and a DAG run that already exists is skipped, so a retry of one bronze snapshot starts nothing new. A failed
layer is retried by clearing that layer's own DAG run.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, get_current_context, task

from orchestration.dag_runtime import create_dag_run, pool_settings, run_step
from orchestration.training_contract import chain_layer_run, run_id_pattern

LOG = logging.getLogger(__name__)
CHAIN = (
    ("silver", "silver_transform_dag", None),
    ("gold", "gold_transform_dag", "silver_snapshot_run"),
    ("features", "features_base_dag", "gold_snapshot_run"),
)
POLL_SECONDS = 60
# Each layer's wait ends after its own limit (a job's deadline is six hours) and the whole chain ends after the sum plus an hour for the gate.
LAYER_LIMITS = {"silver": timedelta(hours=8), "gold": timedelta(hours=8), "features": timedelta(hours=8)}
BRONZE_LIMIT = timedelta(hours=8)
CHAIN_LIMIT = sum(LAYER_LIMITS.values(), BRONZE_LIMIT + timedelta(hours=1))


def chain_run(layer: str, bronze_run: str) -> str:
    """The run ID of ``layer`` in the chain that starts at ``bronze_run``: the layer's name and the bronze run's hex and attempt."""
    if layer not in {name for name, _, _ in CHAIN}:
        raise ValueError(f"{layer} is not in the chain")
    return chain_layer_run(layer, bronze_run)


def metadata_run_id(bronze_run: str) -> str:
    """The run ID of the catalog start in the chain that starts at ``bronze_run``."""
    return chain_layer_run("metadata", bronze_run)


def dag_run_id(dag_id: str, run: str) -> str:
    """The DAG run ID a trigger uses: the run ID itself, so a repeated trigger names the run that already exists."""
    return run


def trigger_for(index: int) -> TriggerDagRunOperator:
    """The trigger of one layer. Its run IDs and settings are templates over the DAG's own ``run_id`` and ``mode``."""
    layer, dag_id, parent_key = CHAIN[index]
    conf = {"run_id": f"{{{{ chain_run('{layer}', params.run_id) }}}}", "mode": "{{ params.mode }}"}
    if parent_key is not None:
        conf[parent_key] = f"{{{{ chain_run('{CHAIN[index - 1][0]}', params.run_id) }}}}"
    return TriggerDagRunOperator(
        task_id=f"run_{layer}",
        trigger_dag_id=dag_id,
        trigger_run_id=f"{{{{ dag_run_id('{dag_id}', chain_run('{layer}', params.run_id)) }}}}",
        conf=conf,
        wait_for_completion=True,
        poke_interval=POLL_SECONDS,
        allowed_states=["success"],
        failed_states=["failed"],
        skip_when_already_exists=True,
        reset_dag_run=False,
        execution_timeout=LAYER_LIMITS[layer],
        retries=0,
    )


with DAG(
    dag_id="bronze_chain_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=CHAIN_LIMIT,
    tags=["orchestration", "chain"],
    params={
        "run_id": Param(type="string", pattern=f"^{run_id_pattern('bronze')}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    user_defined_macros={"chain_run": chain_run, "dag_run_id": dag_run_id, "metadata_run_id": metadata_run_id},
    default_args={"retries": 0},
) as dag:

    @task(task_id="chain_gate", retries=0)
    def gate() -> str:
        return run_step("chain_gate")

    @task(task_id="bronze_load", retries=0, execution_timeout=BRONZE_LIMIT, **pool_settings("bronze"))
    def bronze_load() -> str:
        return run_step("bronze_load")

    previous = gate().operator >> bronze_load().operator
    for position in range(len(CHAIN)):
        current = trigger_for(position)
        previous >> current
        previous = current

    # The catalog start is the last step and nothing waits for it. It runs only after the features layer succeeded, so a failure upstream still fails the chain.
    # It creates the catalog DAG run through the Airflow REST API with a bounded call. A trigger operator reports success by raising an exception, which this
    # handler would swallow. An error of its own (a missing or paused catalog DAG, a refused request, no answer in time) is logged at error level and returned
    # in the task result, never raised, so the chain result does not depend on the catalog. A retried bronze run names the same DAG run, which counts as done.
    @task(task_id="start_metadata", retries=0)
    def start_metadata() -> str:
        params = get_current_context()["params"]
        run = metadata_run_id(params["run_id"])
        try:
            conf = {"run_id": run, "features_run": chain_run("features", params["run_id"]), "mode": params["mode"]}
            created = create_dag_run("metadata_ingest_dag", dag_run_id("metadata_ingest_dag", run), conf)
        except Exception as error:
            LOG.error("the catalog start for %s failed: %s: %s", run, type(error).__name__, error)
            return f"catalog start failed: {type(error).__name__}: {error}"
        return f"catalog start requested: {run} ({'created' if created else 'already exists'})"

    previous >> start_metadata().operator
