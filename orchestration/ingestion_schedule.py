"""The monthly schedule of ``ingestion_dag``, the run ID of a scheduled run and the run kind of a scheduled run.

The schedule is on only where the deployment names its environment ``prod`` in ``HAI_ENVIRONMENT`` (the Terraform ``environment`` of the services stack);
development and a deployment that names none keep the hand trigger. Any other name is refused, because a misspelt production name must not turn the schedule
off without a sign. A scheduled run has no owner to name its run, so its run ID is a function of its logical date alone: ``ingestion_<hex>``, where the hex is
the first eight hex digits of the SHA-256 digest of the logical date, so the same logical date always gives the same run ID, and its launch records make
a repeat join the run it repeats and start nothing new. A scheduled run is always the ``monthly`` kind: only the hand trigger can start ``bootstrap``. The
daily retry of failed files runs on days 2 and 3 after the monthly run of day 1 (the retry period) under a run ID fixed by the original run and the day
(``ingestion_retry_<hex>_<day>``, the original run's hex), so the same day always joins its recorded launch. Bronze starts after the retry period
(``orchestration/bronze_start.py``). The module reads no Airflow object and makes no call.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from orchestration.training_contract import RETRY_DAYS, compose_run_id, new_run_hex, run_id_parts, run_id_pattern, scheduled_run_hex

ENVIRONMENT_SETTING = "HAI_ENVIRONMENT"
PRODUCTION = "prod"
DEVELOPMENT = "dev"
# Midnight UTC on the first day of each month.
MONTHLY = "0 0 1 * *"
# The form's pattern for an ingestion run ID (``ingestion_<hex>``, with an attempt suffix from attempt 2).
RUN_ID_PATTERN = run_id_pattern("ingestion")
# The run ID the form carries by default. Only a scheduled run replaces it; a hand trigger must name its run.
SCHEDULED_RUN = "scheduled"
SCHEDULED_RUN_TYPE = "scheduled"
MONTHLY_KIND = "monthly"
RETRY_KIND = "retry"
# The retry period is days 2 and 3 of the month at midnight UTC, the original being the monthly run of day 1: retry day 1 is day 2 of the month and retry day 2
# is day 3 (owner, Nov 16 2026, 9:06 AM PST). ``RETRY_DAYS`` is the number of retry days.
RETRY_SCHEDULE = f"0 0 2-{RETRY_DAYS + 1} * *"
# The retry day a scheduled retry's form carries by default. Only a scheduled run replaces it; a hand trigger must name its original run and its day.
SCHEDULED_RETRY_DAY = 0


def schedule_for(environment: Mapping[str, str]) -> str | None:
    """The cron schedule of the deployment: monthly in production, none in development or where no environment is named."""
    if ENVIRONMENT_SETTING not in environment:
        return None
    name = environment[ENVIRONMENT_SETTING]
    if name == PRODUCTION:
        return MONTHLY
    if name == DEVELOPMENT:
        return None
    raise ValueError(f"{ENVIRONMENT_SETTING} must be {DEVELOPMENT} or {PRODUCTION}")


def retry_schedule_for(environment: Mapping[str, str]) -> str | None:
    """The cron schedule of the daily retry: on in production only, by the same rule as the monthly schedule."""
    return RETRY_SCHEDULE if schedule_for(environment) is not None else None


def scheduled_run_id(logical_date: datetime) -> str:
    """The run ID of the scheduled run of ``logical_date``: ``ingestion_`` and the first eight hex digits of the digest of its UTC time."""
    return compose_run_id("ingestion", scheduled_run_hex(logical_date))


def new_ingestion_run_id() -> str:
    """The run ID of a hand-started ingestion run: ``ingestion_`` and a fresh random hex."""
    return compose_run_id("ingestion", new_run_hex())


def is_scheduled(context: Mapping[str, Any]) -> bool:
    """Whether the current DAG run is a scheduled run that has a logical date."""
    dag_run = context.get("dag_run")
    run_type = getattr(dag_run, "run_type", None)  # Airflow's DagRunType is an enum whose value is the name
    return dag_run is not None and str(getattr(run_type, "value", run_type)) == SCHEDULED_RUN_TYPE and getattr(dag_run, "logical_date", None) is not None


def resolve_run_id(requested: str, context: Mapping[str, Any]) -> str:
    """The run id of the current DAG run: a named run id stays as it is, the form's default becomes the id of the logical date of a scheduled run."""
    if requested != SCHEDULED_RUN:
        return requested
    if not is_scheduled(context):
        raise ValueError("this run is not scheduled and names no run")
    return scheduled_run_id(context["dag_run"].logical_date)


def resolve_kind(requested: str, context: Mapping[str, Any]) -> str:
    """The run kind of the current DAG run: a scheduled run is the monthly kind whatever the form's default says, a hand trigger keeps the kind it names."""
    return MONTHLY_KIND if is_scheduled(context) else requested


def resolve_retry(original: str, day: int, context: Mapping[str, Any]) -> tuple[str, int]:
    """The original run and the day of the current retry. A hand trigger names both. A scheduled run names neither: its logical date, which must be midnight
    UTC on day 2 or 3 of a month, gives the day (the day of the month less one) and the monthly run of day 1 as the original. Half a name is refused."""
    if original != SCHEDULED_RUN and day != SCHEDULED_RETRY_DAY:
        return original, day
    if original != SCHEDULED_RUN or day != SCHEDULED_RETRY_DAY:
        raise ValueError("a retry names both its original run and its day, or neither")
    if not is_scheduled(context):
        raise ValueError("this retry is not scheduled and names no original run and day")
    utc = context["dag_run"].logical_date.astimezone(UTC)
    if (utc.hour, utc.minute, utc.second, utc.microsecond) != (0, 0, 0, 0) or not 2 <= utc.day <= RETRY_DAYS + 1:
        raise ValueError("a scheduled retry runs at midnight UTC on day 2 or 3 of the month")
    return scheduled_run_id(utc.replace(day=1)), utc.day - 1


def retry_run_id(original_run: str, day: int) -> str:
    """The run ID of the retry of ``original_run`` on retry day ``day`` (1 to ``RETRY_DAYS``): ``ingestion_retry_<hex>_<day>`` with the original run's hex.

    A later attempt of the original run puts its suffix before the day (``ingestion_retry_<hex>_2_1``). The same run and day always give the same ID, so a
    repeat of a retry day joins its recorded launch and starts nothing new.
    """
    if not isinstance(original_run, str) or not re.fullmatch(RUN_ID_PATTERN, original_run):
        raise ValueError("the original run ID is not a run ID of an ingestion run")
    if isinstance(day, bool) or not isinstance(day, int) or not 1 <= day <= RETRY_DAYS:
        raise ValueError(f"the retry day is outside one to {RETRY_DAYS}")
    parts = run_id_parts(original_run)
    attempt = "" if parts.attempt == 1 else f"_{parts.attempt}"
    return f"ingestion_retry_{parts.hex}{attempt}_{day}"
