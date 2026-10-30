"""Run silver, gold, features and drift in order after one bronze run, with no calendar schedule.

``ingestion_dag`` starts this DAG by hand with ``run_id`` set to the run of the bronze snapshot. The first task asks the broker whether the first silver and
gold publishes are signed. Without that record the broker refuses and the chain stops. Each layer then runs as a triggered DAG run and the chain waits for its
success before the next layer, so a failed layer or a failed acceptance check stops the chain before the later layers.

Every run ID and every DAG run ID derives from the bronze run ID. A second trigger for the same bronze run names the same DAG runs, and a DAG run that already
exists is skipped, so a retry of one bronze snapshot starts nothing new. A failed layer is retried by clearing that layer's own DAG run.
"""

from __future__ import annotations

import hashlib

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, task

from orchestration.dag_runtime import run_step

RUN_ID = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"
CHAIN = (
    ("silver", "silver_transform_dag", None),
    ("gold", "gold_transform_dag", "silver_snapshot_run"),
    ("features", "features_transform_dag", "gold_snapshot_run"),
    ("drift", "drift_monitoring_dag", None),
)
POLL_SECONDS = 60


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
        retries=0,
    )


with DAG(
    dag_id="bronze_chain_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "chain"],
    params={
        "run_id": Param(type="string", pattern=f"^{RUN_ID}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    user_defined_macros={"chain_run": chain_run, "dag_run_id": dag_run_id},
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
