"""Build the m4 model set's feature table from the published base run through the direct ECS launch library, then start that model's drift check.

Once the set is published and its acceptance checks passed, the drift check of the set runs (``drift_monitoring_dag``) and starts the model's training only
when its gate says so. The trigger does not wait, and its run derives from this run, so a retried run starts nothing new."""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain
from orchestration.training_contract import model_drift_run_id

MODEL = "m4"
TRIGGER_TIMEOUT = timedelta(minutes=5)

with DAG(
    dag_id="features_m4_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,  # two runs of one set would share its family: the host pool no longer keeps them apart
    tags=["orchestration", "features"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
        # The base run this set builds from: its features job must have passed, and the job refuses a run that is not the newest committed base run (no latest).
        "base_snapshot_run": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
    },
    user_defined_macros={"model_drift_run_id": model_drift_run_id},
    default_args={"retries": 0},
) as dag:
    gate = build_job_chain("pin_gold", family="features_m4")
    drift_run = "{{ model_drift_run_id(params.run_id) }}"
    gate >> TriggerDagRunOperator(
        task_id="trigger_drift",
        trigger_dag_id="drift_monitoring_dag",
        trigger_run_id=f"features_m4_dag__drift__{drift_run}",
        conf={"run_id": drift_run, "mode": "{{ params.mode }}", "mlflow": "aws", "model": MODEL, "features_run": "{{ params.run_id }}"},
        wait_for_completion=False,
        skip_when_already_exists=True,
        reset_dag_run=False,
        execution_timeout=TRIGGER_TIMEOUT,
        retries=0,
    )
