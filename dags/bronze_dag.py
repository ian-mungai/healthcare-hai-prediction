"""Load the raw sources into bronze for one bronze run, with no calendar schedule and no later layer.

The DAG runs two jobs under its run ID (``bronze_<hex>``, the hex and attempt of the pipeline run): the bronze stage job (``bronze_stage``) lists the raw
sources and stages the run's bronze request and source inventory under the bronze task role, and the bronze job loads those sources under that same run ID
(``bronze_load``). A launch is keyed by the run and the job, so each job has its own launch record, and each job's launch binds the bronze run to the code
of its ingestion run (``orchestration/ecs_preflight.py``), so a hand start of this DAG alone works. A source file that no run of the ingestion run's retry
period captured keeps its earlier capture, and the run record and the owner notice name each such carried-over file.

The first DAG (``pipeline_dag``) runs this DAG as its second stage and waits for it. This DAG starts no other DAG: the later layers, their code binding and
their run IDs belong to the first DAG. A failed job fails the run, and a failed job is retried by clearing its own task.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import DAG, Param, task

from orchestration.dag_runtime import pool_settings, run_step
from orchestration.training_contract import run_id_pattern

BRONZE_LIMIT = timedelta(hours=8)
# The stage job lists the raw sources once and writes two objects; its own deadline is one hour (config/orchestration/direct_jobs.json).
STAGE_LIMIT = timedelta(hours=1)
BRONZE_RUN_LIMIT = STAGE_LIMIT + BRONZE_LIMIT + timedelta(hours=1)

with DAG(
    dag_id="bronze_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=BRONZE_RUN_LIMIT,
    tags=["orchestration", "bronze"],
    params={
        "run_id": Param(type="string", pattern=f"^{run_id_pattern('bronze')}$"),
        "mode": Param("real", type="string", enum=["real"]),
    },
    default_args={"retries": 0},
) as dag:

    @task(task_id="bronze_stage", retries=0, execution_timeout=STAGE_LIMIT, **pool_settings("bronze_stage"))
    def bronze_stage() -> str:
        return run_step("bronze_stage")

    @task(task_id="bronze_load", retries=0, execution_timeout=BRONZE_LIMIT, **pool_settings("bronze"))
    def bronze_load() -> str:
        return run_step("bronze_load")

    bronze_stage().operator >> bronze_load().operator
