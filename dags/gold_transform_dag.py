"""Build and publish atomic gold from a committed silver snapshot set."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_tasks

WORK_STEPS = (
    "acquire_run",
    "catalog_up",
    "snapshot_project",
    "reserve_silver",
    "pin_silver",
    "read_silver",
    "gold_build",
    "gold_validate",
    "cross_layer_checks",
    "seal_gold",
    "gold_export",
    "publish_candidates",
    "reconcile_candidates",
    "catalog_backup",
    "commit_publish",
    "run_record",
)

with DAG(
    dag_id="gold_transform_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "gold"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("publish_fixture", type="string", enum=["publish_fixture", "real"]),
        "silver_snapshot_run": Param("latest", type="string", pattern=r"^(latest|[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8})$"),
    },
    default_args={"retries": 0},
) as dag:
    build_tasks(WORK_STEPS, "gold")
