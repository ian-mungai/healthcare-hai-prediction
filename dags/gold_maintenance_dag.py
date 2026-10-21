"""Maintain gold snapshots while preserving registered and pending readers."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_tasks

WORK_STEPS = ("acquire_run", "catalog_up", "gold_maintenance", "run_record")

with DAG(
    dag_id="gold_maintenance_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "maintenance"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("publish_fixture", type="string", enum=["publish_fixture", "real"]),
    },
    default_args={"retries": 0},
) as dag:
    build_tasks(WORK_STEPS, "maintenance")
