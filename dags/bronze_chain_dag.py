"""Run silver, gold, features and drift in order after one bronze run, with no calendar schedule.

The owner starts this DAG by hand after a new bronze snapshot, with ``run_id`` set to the run of that snapshot. A sensor that starts it comes later.
The first task asks the broker whether the first silver and gold publishes are signed. Without that record the broker refuses and the chain stops.
Each layer then runs as a triggered DAG run and the chain waits for its success before the next layer, so a failed layer or a failed acceptance check stops
the chain before the later layers. Every wait has a limit.

Every run ID and every DAG run ID derives from the bronze run ID. A second trigger for the same bronze run names the same DAG runs, and a DAG run that already
exists is skipped, so a retry of one bronze snapshot starts nothing new. A failed layer is retried by clearing that layer's own DAG run.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, get_current_context, task

from orchestration.dag_runtime import run_step

LOG = logging.getLogger(__name__)
RUN_ID = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"
CHAIN = (
    ("silver", "silver_transform_dag", None),
    ("gold", "gold_transform_dag", "silver_snapshot_run"),
    ("features", "features_transform_dag", "gold_snapshot_run"),
    ("drift", "drift_monitoring_dag", None),
)
POLL_SECONDS = 60
# Each layer's wait ends after its own limit (a job's deadline is six hours, drift two) and the whole chain ends after the sum plus an hour for the gate.
LAYER_LIMITS = {"silver": timedelta(hours=8), "gold": timedelta(hours=8), "features": timedelta(hours=8), "drift": timedelta(hours=3)}
CHAIN_LIMIT = sum(LAYER_LIMITS.values(), timedelta(hours=1))


def derived_run_id(parent_run: str, layer: str) -> str:
    """The run ID of a layer: the parent's time stamp and eight hex digits of a hash of the layer and the parent."""
    return f"{parent_run[:16]}-{hashlib.sha256(f'{layer}:{parent_run}'.encode()).hexdigest()[:8]}"


def chain_run(layer: str, bronze_run: str) -> str:
    """The run ID of ``layer`` in the chain that starts at ``bronze_run``."""
    run = bronze_run
    for name, _, _ in CHAIN:
        run = derived_run_id(run, name)
        if name == layer:
            return run
    raise ValueError(f"{layer} is not in the chain")


def metadata_run_id(bronze_run: str) -> str:
    """The run ID of the catalog start in the chain that starts at ``bronze_run``."""
    return derived_run_id(chain_run("drift", bronze_run), "metadata")


def dag_run_id(dag_id: str, run: str) -> str:
    """The DAG run ID a trigger uses, so a repeated trigger names the run that already exists."""
    return f"chain__{dag_id}__{run}"


def trigger_for(index: int) -> TriggerDagRunOperator:
    """The trigger of one layer. Its run IDs and settings are templates over the DAG's own ``run_id`` and ``mode``."""
    layer, dag_id, parent_key = CHAIN[index]
    conf = {"run_id": f"{{{{ chain_run('{layer}', params.run_id) }}}}", "mode": "{{ params.mode }}"}
    if parent_key is not None:
        conf[parent_key] = f"{{{{ chain_run('{CHAIN[index - 1][0]}', params.run_id) }}}}"
    if layer == "drift":
        conf["mlflow"] = "local"
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
        "run_id": Param(type="string", pattern=f"^{RUN_ID}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    user_defined_macros={"chain_run": chain_run, "dag_run_id": dag_run_id, "metadata_run_id": metadata_run_id},
    default_args={"retries": 0},
) as dag:

    @task(task_id="chain_gate", retries=0)
    def gate() -> str:
        return run_step("chain_gate")

    previous = gate().operator
    for position in range(len(CHAIN)):
        current = trigger_for(position)
        previous >> current
        previous = current

    # The catalog start is the last step and nothing waits for it. It runs only after the drift layer succeeded, so an upstream failure still fails the chain.
    # Its own errors (a missing or paused catalog DAG, a refused trigger) are logged and returned and never raised, so the chain result does not depend on the
    # catalog. A retried bronze run names the same DAG run, which is skipped.
    @task(task_id="start_metadata", retries=0)
    def start_metadata() -> str:
        context = get_current_context()
        params = context["params"]
        run = metadata_run_id(params["run_id"])
        try:
            TriggerDagRunOperator(
                task_id="trigger_metadata_ingest",
                trigger_dag_id="metadata_ingest_dag",
                trigger_run_id=dag_run_id("metadata_ingest_dag", run),
                conf={"run_id": run, "mode": params["mode"]},
                wait_for_completion=False,
                skip_when_already_exists=True,
                reset_dag_run=False,
                retries=0,
            ).execute(context)
        except Exception as error:
            LOG.error("the catalog start for %s failed: %s", run, type(error).__name__)
            return f"catalog start failed: {type(error).__name__}"
        return f"catalog start requested: {run}"

    previous >> start_metadata().operator
