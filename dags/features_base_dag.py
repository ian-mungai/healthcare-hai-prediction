"""Build and publish the features base from a committed gold snapshot set: the spine, the intermediate models, the label table and the domain matrices.

Once the base is published and its acceptance checks passed, the DAG starts the six model feature DAGs, one trigger task each, behind the gate only. A trigger
does not wait for its DAG, so one model's failure stops only that model's DAG. Each model's DAG run derives from the base run and the model, so a second
trigger for the same base run names the runs that already exist and starts nothing new.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain
from orchestration.training_contract import FEATURE_DAGS, model_features_run_id

TRIGGER_TIMEOUT = timedelta(minutes=5)

with DAG(
    dag_id="features_base_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "features"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
        # The passed gold run this job publishes from: its preflight refuses a run without it, so the form requires a run ID (no latest).
        "gold_snapshot_run": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
    },
    user_defined_macros={"model_features_run_id": model_features_run_id},
    default_args={"retries": 0},
) as dag:
    gate = build_job_chain("pin_gold", asset="features")
    for dag_id, model in FEATURE_DAGS.items():
        run = f"{{{{ model_features_run_id(params.run_id, '{model}') }}}}"
        gate >> TriggerDagRunOperator(
            task_id=f"trigger_{model}",
            trigger_dag_id=dag_id,
            trigger_run_id=f"features__{dag_id}__{run}",
            conf={"run_id": run, "mode": "{{ params.mode }}", "base_snapshot_run": "{{ params.run_id }}"},
            wait_for_completion=False,
            skip_when_already_exists=True,
            reset_dag_run=False,
            execution_timeout=TRIGGER_TIMEOUT,
            retries=0,
        )
