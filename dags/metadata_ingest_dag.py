"""Start the on-demand OpenMetadata catalog task through the direct ECS launch library and wait for it to end.

The task rebuilds the catalog on each start from Polaris, dbt and Marquez, then serves the browsing UI privately until its own deadline and stops itself. The
one step launches the task and waits for it. The step has no retry: a retry would start a second task that costs money, and the launch library refuses a
second launch for the same run anyway. The step's limit is the longest task lifetime plus the job deadline's grace and a margin, so the step ends in Airflow.
The chain DAG starts this DAG without waiting for it, so a failed catalog task never fails a chain run.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import DAG, Param, task

from orchestration.dag_runtime import launch_pool, run_step

RUN_ID = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"
# The Terraform variable openmetadata_lifetime_seconds may not exceed this. The job deadline adds 900 seconds of grace.
LIFETIME_CEILING_SECONDS = 14400
STEP_LIMIT = timedelta(seconds=LIFETIME_CEILING_SECONDS + 1800)

with DAG(
    dag_id="metadata_ingest_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=STEP_LIMIT + timedelta(minutes=30),
    tags=["metadata"],
    params={
        "run_id": Param(type="string", pattern=f"^{RUN_ID}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    default_args={"retries": 0},
) as dag:

    @task(task_id="metadata_run", retries=0, execution_timeout=STEP_LIMIT, pool=launch_pool())
    def metadata_run() -> str:
        return run_step("metadata_run")

    metadata_run()
