"""Build and publish atomic gold from a committed silver snapshot set."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain

with DAG(
    dag_id="gold_transform_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "gold"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
        "silver_snapshot_run": Param("latest", type="string", pattern=r"^(latest|[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8})$"),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("pin_silver", asset="gold")
