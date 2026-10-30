"""Deploy a registered model version beside the production model as a canary, or roll the canary back, through the broker."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_tasks

WORK_STEPS = ("acquire_run", "snapshot_serving", "serve_deploy", "run_record")

with DAG(
    dag_id="serving_deploy_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["serving"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    default_args={"retries": 0},
) as dag:
    build_tasks(WORK_STEPS, "serving")
