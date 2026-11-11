"""Build the m6 model set's feature table from the published base run through the direct ECS launch library."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain

with DAG(
    dag_id="features_m6_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "features"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
        # The base run this set builds from: its features job must have passed, and the job refuses a run that is not the newest committed base run (no latest).
        "base_snapshot_run": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("pin_gold")
