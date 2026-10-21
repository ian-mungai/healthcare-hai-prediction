"""Publish validated atomic silver tables from pinned bronze snapshots."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_tasks

WORK_STEPS = (
    "acquire_run",
    "catalog_up",
    "snapshot_project",
    "pin_bronze",
    "silver_build",
    "silver_validate",
    "verify_bronze",
    "seal_silver",
    "silver_export",
    "silver_candidates",
    "silver_reconcile",
    "silver_backup",
    "silver_commit",
    "record_silver_snapshot",
    "run_record",
)

with DAG(
    dag_id="silver_transform_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "silver"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("publish_fixture", type="string", enum=["publish_fixture", "real"]),
    },
    default_args={"retries": 0},
) as dag:
    build_tasks(WORK_STEPS, "silver")
