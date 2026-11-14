"""The monthly schedule of ``ingestion_dag``, the run id of a scheduled run and the run kind of a scheduled run.

The schedule is on only where the deployment names its environment ``prod`` in ``HAI_ENVIRONMENT`` (the Terraform ``environment`` of the services stack);
development and a deployment that names none keep the hand trigger. Any other name is refused, because a misspelt production name must not turn the schedule
off without a sign. A scheduled run has no owner to name its run, so its run id is a function of its logical date alone: the same logical date always gives
the same run id, and the launch records of that run id make a repeat join the run it repeats and start nothing new. A scheduled run is always the
``monthly`` kind: only the hand trigger can start ``bootstrap``. The module reads no Airflow object and makes no call.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
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
