"""Publish validated atomic silver tables from pinned bronze snapshots."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain
from orchestration.training_contract import run_id_pattern

with DAG(
    dag_id="silver_transform_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "silver"],
    params={
        "run_id": Param(type="string", pattern=f"^{run_id_pattern('silver')}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("pin_bronze")
