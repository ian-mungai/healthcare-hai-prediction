"""Run the Run 3 publisher once on the host: download every non-personal unit of the locked plans and store each capture under raw/.

One job, one chain: preflight (the staged publisher_request.json and code digest are checked, nothing is launched), job (the publisher_host task on the
host through the direct ECS launch library), evidence (its launch record says the run container passed) and gate. The owner stages the request with
scripts.acquisition.stage_publisher and triggers this DAG by hand with the run's ``run_id``. The monthly production schedule, the plan rebuilds and holds,
the daily retries of failed files and the start of a bronze DAG (with its bronze_request.json staged from the publisher's output) are later slices.
"""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain

with DAG(
    dag_id="ingestion_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "ingestion"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("publisher_run")
