"""Check a Production model's real baseline for drift on demand, with Evidently's first live comparison, and start nothing after it.

The form names the run, the model and the HAI type. One host job (the drift family, in its live mode) fetches the Production model's baseline matrix, splits it
by window year (the years before the newest as the reference, the newest year as the current window), runs the drift check on the split and uploads the drift
record and Evidently's HTML report to ``drift_demo/<run>/`` of the data bucket, never to the ``drift/<run>/`` prefix that ``drift_monitoring_dag`` reads. The
report is also added as a snapshot to the Evidently UI workspace on the host. The DAG has no schedule, takes no features run, makes no gate decision and
triggers no training.
"""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain
from orchestration.training_contract import HAI_TYPES, RUN_ID, model_choices, model_param_schema

with DAG(
    dag_id="drift_live_check_dag",
    doc_md=f"{__doc__}\n\nModels: {model_choices()}.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["monitoring"],
    params={
        "run_id": Param(type="string", pattern=f"^{RUN_ID}$"),
        "mode": Param("real", type="string", enum=["real"]),
        # The model whose Production baseline is checked (orchestration.training_contract.MODELS) and the HAI type of that baseline.
        "model": Param(type="string", **model_param_schema()),
        "hai_type": Param(type="string", enum=list(HAI_TYPES)),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("drift_live_run", family="drift")
