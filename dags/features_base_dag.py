"""Build and publish the features base from a committed gold snapshot set: the spine, the intermediate models, the label table and the domain matrices."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain

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
    default_args={"retries": 0},
) as dag:
    build_job_chain("pin_gold", asset="features")
