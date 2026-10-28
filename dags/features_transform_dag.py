"""Build, validate and publish the feature tables from a committed gold snapshot set, then apply and materialize the Feast repository."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_tasks

WORK_STEPS = (
    "acquire_run",
    "catalog_up",
    "snapshot_project",
    "reserve_gold",
    "pin_gold",
    "read_gold",
    "features_build",
    "features_validate",
    "seal_features",
    "features_export",
    "features_candidates",
    "features_reconcile",
    "features_backup",
    "features_commit",
    "record_features_snapshot",
    "online_store_up",
    "feast_apply",
    "feast_materialize",
    "run_record",
)

with DAG(
    dag_id="features_transform_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "features"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("publish_fixture", type="string", enum=["publish_fixture", "real"]),
        "gold_snapshot_run": Param("latest", type="string", pattern=r"^(latest|[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8})$"),
    },
    default_args={"retries": 0},
) as dag:
    build_tasks(WORK_STEPS, "features")
