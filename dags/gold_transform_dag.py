"""Build and publish atomic gold from a committed silver snapshot set."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain
from orchestration.training_contract import run_id_pattern

with DAG(
    dag_id="gold_transform_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "gold"],
    params={
        "run_id": Param(type="string", pattern=f"^{run_id_pattern('gold')}$"),
        "mode": Param("real", type="string", enum=["real"]),
        # The passed silver run this job publishes from: its preflight refuses a run without it, so the form requires a run ID (no latest).
        "silver_snapshot_run": Param(type="string", pattern=f"^{run_id_pattern('silver')}$"),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("pin_silver", asset="gold")
