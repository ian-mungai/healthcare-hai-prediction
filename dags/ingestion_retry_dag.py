"""Retry the files the monthly publisher run could not fetch, once a day for up to seven days, through the same publisher job on the host.

The DAG carries its run kind as launch parameters: the original run, the retry day (1 to 7) and the capture root. The retry run's own ID is fixed by the
original run and the day, so the same day always has the same ID, and a repeat of it joins the recorded launch and starts nothing new. The retry is a new
run of the publisher: capture mode skips every unit that has a completion record, so a retry tries exactly the units that have none, and nothing is
restored or pruned. Nothing is staged for the run but its code, and no step waits for the owner.
In production (``HAI_ENVIRONMENT=prod``) the DAG also runs on days 2 to 8 of each month at midnight UTC, and a scheduled run takes the monthly run of
day 1 as its original and the day of the month less one as its day (orchestration/ingestion_schedule.py); development keeps the hand trigger.
"""

from __future__ import annotations

import os

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain
from orchestration.ingestion_schedule import RUN_ID_PATTERN, SCHEDULED_RETRY_DAY, SCHEDULED_RUN, retry_schedule_for
from orchestration.training_contract import RETRY_DAYS

SCHEDULE = retry_schedule_for(os.environ)
# A scheduled DAG needs a default for every parameter, so the scheduled form carries ``scheduled`` and day 0, which only a scheduled run resolves from its
# logical date. The development form has no schedule and keeps its required original run and day.
ORIGINAL_RUN = (
    Param(SCHEDULED_RUN, type="string", pattern=rf"^({SCHEDULED_RUN}|{RUN_ID_PATTERN})$") if SCHEDULE else Param(type="string", pattern=rf"^{RUN_ID_PATTERN}$")
)
DAY = Param(SCHEDULED_RETRY_DAY, type="integer", minimum=0, maximum=RETRY_DAYS) if SCHEDULE else Param(type="integer", minimum=1, maximum=RETRY_DAYS)

with DAG(
    dag_id="ingestion_retry_dag",
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "ingestion"],
    params={
        "original_run": ORIGINAL_RUN,
        "day": DAY,
        "mode": Param("real", type="string", enum=["real"]),
        "capture_root": Param("raw/", type="string", pattern=rf"^(raw/|ingestion_test/{RUN_ID_PATTERN}/)$(?![\s\S])"),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("publisher_run")
