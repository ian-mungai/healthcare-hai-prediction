"""Run the Run 3 publisher once on the host: download every non-personal unit of the tracked queue and store each capture under its capture root.

One job, one chain: preflight (the run kind, the capture root and the code digest are checked, nothing is launched), job (the publisher_host task on the
host through the direct ECS launch library), evidence (its launch record says the run container passed) and gate. Nothing is staged for the run but its code
archive, which carries the tracked inputs. The DAG run names the kind (``bootstrap``, which skips every unit that has a completion record, or ``monthly``)
and the capture root (``raw/``, or one ``ingestion_test/<run ID>/`` folder for a test) as the launch parameters, and the owner triggers the DAG by hand with
the run's ``run_id`` (``ingestion_<hex>`` with a fresh random hex). In production (``HAI_ENVIRONMENT=prod``) the DAG also runs on a monthly schedule: a
scheduled run takes the run ID of its logical date (``ingestion_`` and the first eight hex digits of the digest of that date) as its job's run ID and as its
own Airflow run ID (the monthly timetable of orchestration/ingestion_timetables.py names it), and is always the ``monthly`` kind, and only the hand trigger
starts ``bootstrap`` (orchestration/ingestion_schedule.py). Development keeps the hand trigger.

A fifth task, ``start_bronze``, starts ``bronze_chain_dag`` under ``bronze_<hex>`` of this run once the run passed, when the owner started it by hand (a
zero-day retry period). A scheduled run starts none: the last day of its retry period does (``ingestion_retry_dag``). A test capture root starts none.
The plan rebuilds and holds are a later slice.
"""

from __future__ import annotations

import os
from typing import Any

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_bronze_start, build_job_chain
from orchestration.ingestion_schedule import RUN_ID_PATTERN, SCHEDULED_RUN, schedule_for

SCHEDULE = schedule_for(os.environ)


def timetable() -> Any:
    """The timetable of the monthly schedule, which names a scheduled DAG run as its job. Only a deployment with the schedule on imports it."""
    from orchestration.ingestion_timetables import monthly_timetable

    return monthly_timetable()


# A scheduled DAG needs a default for every parameter, so the scheduled form's run id defaults to ``scheduled``, which only a scheduled run resolves. The
# development form has no schedule and keeps its required run id.
RUN_ID = (
    Param(SCHEDULED_RUN, type="string", pattern=rf"^({SCHEDULED_RUN}|{RUN_ID_PATTERN})$") if SCHEDULE else Param(type="string", pattern=rf"^{RUN_ID_PATTERN}$")
)

with DAG(
    dag_id="ingestion_dag",
    schedule=timetable() if SCHEDULE else None,
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "ingestion"],
    params={
        "run_id": RUN_ID,
        "mode": Param("real", type="string", enum=["real"]),
        "run_kind": Param("bootstrap", type="string", enum=["bootstrap", "monthly"]),
        "capture_root": Param("raw/", type="string", pattern=rf"^(raw/|ingestion_test/{RUN_ID_PATTERN}/)$(?![\s\S])"),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("publisher_run", family="publisher") >> build_bronze_start(retry=False)
