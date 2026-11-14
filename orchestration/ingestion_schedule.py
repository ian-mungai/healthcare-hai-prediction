"""The monthly schedule of ``ingestion_dag``, the run id of a scheduled run and the run kind of a scheduled run.

The schedule is on only where the deployment names its environment ``prod`` in ``HAI_ENVIRONMENT`` (the Terraform ``environment`` of the services stack);
development and a deployment that names none keep the hand trigger. Any other name is refused, because a misspelt production name must not turn the schedule
off without a sign. A scheduled run has no owner to name its run, so its run id is a function of its logical date alone: the same logical date always gives
the same run id, and the launch records of that run id make a repeat join the run it repeats and start nothing new. A scheduled run is always the
``monthly`` kind: only the hand trigger can start ``bootstrap``. The daily retry of failed files runs on each of the seven days after the monthly run
(days 2 to 8) under a run id fixed by the original run and the day, so the same day always joins its recorded launch. The module reads no Airflow object
and makes no call.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

ENVIRONMENT_SETTING = "HAI_ENVIRONMENT"
PRODUCTION = "prod"
DEVELOPMENT = "dev"
# Midnight UTC on the first day of each month.
MONTHLY = "0 0 1 * *"
# The form's pattern for a run id, which is also the pattern of the run's code archive id.
RUN_ID_PATTERN = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"
# The run id the form carries by default. Only a scheduled run replaces it; a hand trigger must name its run.
SCHEDULED_RUN = "scheduled"
SCHEDULED_RUN_TYPE = "scheduled"
MONTHLY_KIND = "monthly"
RETRY_KIND = "retry"
# A retry runs on each of the seven days after the original run: days 2 to 8 of the month at midnight UTC, the original being the monthly run of day 1.
RETRY_DAYS = 7
RETRY_SCHEDULE = "0 0 2-8 * *"
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
    """The run id of the scheduled run of ``logical_date``: its UTC time and the first eight hexadecimal digits of the digest of that time."""
    if logical_date.tzinfo is None:
        raise ValueError("the logical date has no time zone")
    utc = logical_date.astimezone(UTC)
    digest = hashlib.sha256(utc.isoformat().encode()).hexdigest()[:8]
    return f"{utc.strftime('%Y%m%dT%H%M%SZ')}-{digest}"


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
    UTC on day 2 to 8 of a month, gives the day (the day of the month less one) and the monthly run of day 1 as the original. Half a name is refused."""
    if original != SCHEDULED_RUN and day != SCHEDULED_RETRY_DAY:
        return original, day
    if original != SCHEDULED_RUN or day != SCHEDULED_RETRY_DAY:
        raise ValueError("a retry names both its original run and its day, or neither")
    if not is_scheduled(context):
        raise ValueError("this retry is not scheduled and names no original run and day")
    utc = context["dag_run"].logical_date.astimezone(UTC)
    if (utc.hour, utc.minute, utc.second, utc.microsecond) != (0, 0, 0, 0) or not 2 <= utc.day <= RETRY_DAYS + 1:
        raise ValueError("a scheduled retry runs at midnight UTC on day 2 to 8 of the month")
    return scheduled_run_id(utc.replace(day=1)), utc.day - 1


def retry_run_id(original_run: str, day: int) -> str:
    """The run id of the retry of ``original_run`` on ``day`` (1 to 7): the original's time moved by the day, with a suffix fixed by the run and the day.

    The same run and day always give the same id, so a repeat of a retry day joins its recorded launch and starts nothing new.
    """
    found = re.fullmatch(r"([0-9]{8}T[0-9]{6}Z)-[a-f0-9]{8}", original_run)
    if found is None:
        raise ValueError("the original run id is not a run id")
    if isinstance(day, bool) or not isinstance(day, int) or not 1 <= day <= RETRY_DAYS:
        raise ValueError("the retry day is outside one to seven")
    moment = datetime.strptime(found[1], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC) + timedelta(days=day)
    return f"{moment.strftime('%Y%m%dT%H%M%SZ')}-{hashlib.sha256(f'{original_run}/{day}'.encode()).hexdigest()[:8]}"
