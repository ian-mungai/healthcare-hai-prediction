"""Promote or restore an explicitly approved model version through the broker."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_tasks
from orchestration.training_contract import HAI_TYPES, PROMOTION_STEPS

with DAG(
    dag_id="model_promotion_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["training", "promotion"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "hai_type": Param(type="string", enum=list(HAI_TYPES)),
        "model_version": Param(type="string", pattern=r"^[1-9][0-9]{0,18}$"),
        "mode": Param("promote", type="string", enum=["promote", "rollback"]),
        "mlflow": Param("local", type="string", enum=["local", "aws"]),
    },
    default_args={"retries": 0},
) as dag:
    build_tasks(PROMOTION_STEPS, "promotion")
