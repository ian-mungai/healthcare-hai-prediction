"""The timetables of the two ingestion schedules, which give a scheduled DAG run the run ID of its job.

Airflow names a scheduled DAG run ``scheduled__<time>``, and the launch gives its job a run ID of the form ``ingestion_<hex>``, so the two would differ.
The scheduler asks the DAG's timetable for the run ID (``Timetable.generate_run_id``), so each timetable here is the cron trigger timetable of its schedule
with that one method replaced: a scheduled run is named by ``orchestration/ingestion_schedule.py`` (``scheduled_dag_run_id``), and a run of any other type
(a hand trigger) keeps Airflow's own name. Nothing else of the timetable changes: the cron times, the data intervals and the serialized form are the stock ones.

Airflow finds a custom timetable class only through a plugin, which ``airflow_plugins/hai_timetables.py`` is. This module imports Airflow, so only the
production form of the two DAG files imports it.
"""

from __future__ import annotations

from typing import Any

from airflow.timetables.trigger import CronTriggerTimetable

from orchestration.ingestion_schedule import MONTHLY, MONTHLY_KIND, RETRY_KIND, RETRY_SCHEDULE, SCHEDULED_RUN_TYPE, scheduled_dag_run_id

TIMEZONE = "UTC"


class _CanonicalRunIdTimetable(CronTriggerTimetable):
    """A cron trigger timetable whose scheduled runs have the run ID of their job. ``kind`` is the kind of schedule (``monthly`` or ``retry``)."""

    kind = ""

    def generate_run_id(self, *, run_type: Any, run_after: Any, data_interval: Any, **extra: Any) -> str:
        if str(getattr(run_type, "value", run_type)) != SCHEDULED_RUN_TYPE:
            return str(super().generate_run_id(run_type=run_type, run_after=run_after, data_interval=data_interval, **extra))
        return scheduled_dag_run_id(self.kind, run_after)


class IngestionMonthlyTimetable(_CanonicalRunIdTimetable):
    """The monthly ingestion run at midnight UTC on day 1: ``ingestion_<hex>``."""

    kind = MONTHLY_KIND


class IngestionRetryTimetable(_CanonicalRunIdTimetable):
    """The daily retry at midnight UTC on days 2 and 3: ``ingestion_retry_<hex>_<day>``."""

    kind = RETRY_KIND


def monthly_timetable() -> IngestionMonthlyTimetable:
    return IngestionMonthlyTimetable(MONTHLY, timezone=TIMEZONE)


def retry_timetable() -> IngestionRetryTimetable:
    return IngestionRetryTimetable(RETRY_SCHEDULE, timezone=TIMEZONE)
