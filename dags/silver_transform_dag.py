"""Publish validated atomic silver tables from pinned bronze snapshots."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain

with DAG(
    dag_id="silver_transform_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "silver"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("pin_bronze")
