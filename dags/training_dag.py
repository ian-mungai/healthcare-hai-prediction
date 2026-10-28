"""Fit and evaluate a recorded analysis plan through fixed local broker jobs."""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_tasks
from orchestration.training_contract import HAI_TYPES, TRAINING_STEPS

with DAG(
    dag_id="training_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["training"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("fixture", type="string", enum=["fixture", "real"]),
        "features_run": Param("latest", type="string", pattern=r"^(latest|[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8})$"),
        "population": Param("primary", type="string", enum=["primary", "sensitivity"]),
        "hai_types": Param(list(HAI_TYPES), type="array", minItems=1, maxItems=6, uniqueItems=True, items={"enum": list(HAI_TYPES)}),
        "trigger": Param("manual", type="string", pattern=r"^(manual|drift:[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8})$"),
        "mlflow": Param("local", type="string", enum=["local", "aws"]),
    },
    default_args={"retries": 0},
) as dag:
    build_tasks(TRAINING_STEPS, "training")
