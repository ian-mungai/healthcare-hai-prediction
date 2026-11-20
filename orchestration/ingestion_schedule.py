"""The monthly schedule of ``ingestion_dag``, the run ID of a scheduled run and the run kind of a scheduled run.

The schedule is on only where the deployment names its environment ``prod`` in ``HAI_ENVIRONMENT`` (the Terraform ``environment`` of the services stack);
development and a deployment that names none keep the hand trigger. Any other name is refused, because a misspelt production name must not turn the schedule
off without a sign. A scheduled run has no owner to name its run, so its run ID is a function of its logical date alone: ``ingestion_<hex>``, where the hex is
the first eight hex digits of the SHA-256 digest of the logical date, so the same logical date always gives the same run ID, and its launch records make
a repeat join the run it repeats and start nothing new. A scheduled run is always the ``monthly`` kind: only the hand trigger can start ``bootstrap``. The
daily retry of failed files runs on days 2 and 3 after the monthly run of day 1 (the retry period) under a run ID fixed by the original run and the day
(``ingestion_retry_<hex>_<day>``, the original run's hex), so the same day always joins its recorded launch. The pipeline run starts from bronze after the
retry period (``dag_runtime.build_pipeline_start``, which the retry DAG ends with). The module reads no Airflow object and makes no call.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from orchestration.training_contract import (
    PIPELINE_RUN,
    RETRY_FIRST_DAY,
    RETRY_LAST_DAY,
    compose_run_id,
    new_run_hex,
    run_id_parts,
    run_id_pattern,
    scheduled_run_hex,
)

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
# The retry period is days 2 and 3 of the month at midnight UTC, the original being the monthly run of day 1 (owner, Nov 16 2026, 9:06 AM PST). A retry day
# is its day of the month, and a retry run ID ends in it (``ingestion_retry_<hex>_2``).
RETRY_SCHEDULE = f"0 0 {RETRY_FIRST_DAY}-{RETRY_LAST_DAY} * *"
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


def scheduled_retry_of(logical_date: datetime) -> tuple[str, int]:
    """The original run and the day of the scheduled retry at ``logical_date``, which must be midnight UTC on day 2 or 3 of a month: the day is the day of the
    month and the original is the monthly run of day 1 of that month."""
    if logical_date.tzinfo is None:
        raise ValueError("a scheduled retry has a logical date with a time zone")
    utc = logical_date.astimezone(UTC)
    if (utc.hour, utc.minute, utc.second, utc.microsecond) != (0, 0, 0, 0) or not RETRY_FIRST_DAY <= utc.day <= RETRY_LAST_DAY:
        raise ValueError("a scheduled retry runs at midnight UTC on day 2 or 3 of the month")
    return scheduled_run_id(utc.replace(day=1)), utc.day


def scheduled_dag_run_id(kind: str, run_after: datetime) -> str:
    """The run ID that a scheduled run of ``kind`` (``monthly`` or ``retry``) at ``run_after`` has: the Airflow run ID of the DAG run and the run ID of its job.

    The timetables of ``orchestration/ingestion_timetables.py`` give it to the scheduler, and the task derives the same ID from the logical date
    (``resolve_run_id``, ``resolve_retry``), which is the time of the cron. A time the cron never gives names no run.
    """
    if kind == RETRY_KIND:
        original, day = scheduled_retry_of(run_after)
        return retry_run_id(original, day)
    if kind != MONTHLY_KIND:
        raise ValueError(f"the kind of a scheduled run is {MONTHLY_KIND} or {RETRY_KIND}")
    if run_after.tzinfo is None:
        raise ValueError("a scheduled run has a logical date with a time zone")
    utc = run_after.astimezone(UTC)
    if (utc.day, utc.hour, utc.minute, utc.second, utc.microsecond) != (1, 0, 0, 0, 0):
        raise ValueError("a scheduled monthly run runs at midnight UTC on day 1 of the month")
    return scheduled_run_id(utc)


def resolve_retry(original: str, day: int, context: Mapping[str, Any]) -> tuple[str, int]:
    """The original run and the day of the current retry. A hand trigger names both. A scheduled run names neither: its logical date, which must be midnight
    UTC on day 2 or 3 of a month, gives the day (the day of the month) and the monthly run of day 1 as the original. Half a name is refused."""
    if original != SCHEDULED_RUN and day != SCHEDULED_RETRY_DAY:
        return original, day
    if original != SCHEDULED_RUN or day != SCHEDULED_RETRY_DAY:
        raise ValueError("a retry names both its original run and its day, or neither")
    if not is_scheduled(context):
        raise ValueError("this retry is not scheduled and names no original run and day")
    return scheduled_retry_of(context["dag_run"].logical_date)


def retry_run_id(original_run: str, day: int) -> str:
    """The run ID of the retry of ``original_run`` on day ``day`` of the month (2 or 3): ``ingestion_retry_<hex>_<day>`` with the original run's hex.

    A later attempt of the original run puts its suffix before the day (``ingestion_retry_<hex>_2_3`` is attempt 2 on day 3). The same run and day always
    give the same ID, so a repeat of a retry day joins its recorded launch and starts nothing new.
    """
    if not isinstance(original_run, str) or not re.fullmatch(RUN_ID_PATTERN, original_run):
        raise ValueError("the original run ID is not a run ID of an ingestion run")
    if isinstance(day, bool) or not isinstance(day, int) or not RETRY_FIRST_DAY <= day <= RETRY_LAST_DAY:
        raise ValueError(f"the retry day is not day {RETRY_FIRST_DAY} or {RETRY_LAST_DAY} of the month")
    parts = run_id_parts(original_run)
    attempt = "" if parts.attempt == 1 else f"_{parts.attempt}"
    return f"ingestion_retry_{parts.hex}{attempt}_{day}"


def bronze_run_for(ingestion_run: str) -> str:
    """The bronze run that follows an ingestion run or one of its retry days: ``bronze_`` and the hex and attempt of the ingestion run."""
    if not isinstance(ingestion_run, str) or not re.fullmatch(f"{RUN_ID_PATTERN}|{run_id_pattern('ingestion_retry')}", ingestion_run):
        raise ValueError("a bronze run follows an ingestion run or one of its retry days")
    parts = run_id_parts(ingestion_run)
    return compose_run_id("bronze", parts.hex, parts.attempt)


def ingestion_runs_of(bronze_run: str) -> tuple[str, tuple[str, ...]]:
    """The ingestion run that a bronze run follows and the run of each retry day (day 2 and 3), all with the bronze run's hex and attempt."""
    if not isinstance(bronze_run, str) or not re.fullmatch(run_id_pattern("bronze"), bronze_run):
        raise ValueError("only a bronze run follows ingestion runs")
    parts = run_id_parts(bronze_run)
    original = compose_run_id("ingestion", parts.hex, parts.attempt)
    return original, tuple(retry_run_id(original, day) for day in range(RETRY_FIRST_DAY, RETRY_LAST_DAY + 1))


def pipeline_run_for(ingestion_run: str) -> str:
    """The pipeline run that follows an ingestion run or one of its retry days: ``pipeline_`` and the hex and attempt of the ingestion run."""
    if not isinstance(ingestion_run, str) or not re.fullmatch(f"{RUN_ID_PATTERN}|{run_id_pattern('ingestion_retry')}", ingestion_run):
        raise ValueError("a pipeline run follows an ingestion run or one of its retry days")
    parts = run_id_parts(ingestion_run)
    return compose_run_id(PIPELINE_RUN, parts.hex, parts.attempt)


def pipeline_after_retry(original_run: str, day: int, context: Mapping[str, Any]) -> str | None:
    """The pipeline run that a retry day starts (from bronze), or None: the last day of a scheduled retry period ends the period. A hand retry starts no
    pipeline run; the owner starts one with an attempt suffix when one is wanted."""
    return pipeline_run_for(original_run) if is_scheduled(context) and day == RETRY_LAST_DAY else None


CAPTURED_STATES = ("completed", "changed", "unchanged")
# The states the capture's own check counts (``capture_mode.outcome_problems``): a failed or refused unit did not complete, a held one was not attempted.
FAILED_STATES = ("failed", "refused")
HELD_STATES = ("paused_not_attempted", "keyed_not_attempted")


def snapshot_ids(record: Mapping[str, Any], states: Sequence[str], wrapped: bool) -> set[str]:
    """The snapshot IDs a run record lists in the named states: each state is a list of units (``wrapped``, each with a ``snapshot_id``) or of IDs. A missing
    list, a value that is not a list and an ID that is not a string raise ``KeyError`` or ``TypeError``."""
    found: list[Any] = []
    for name in states:
        units = record["units"][name]
        if not isinstance(units, list):
            raise TypeError(name)
        found.extend(unit["snapshot_id"] if wrapped else unit for unit in units)
    if not all(isinstance(snapshot_id, str) for snapshot_id in found):
        raise TypeError("a snapshot ID is not a string")
    return set(found)


def capture_counts(original: Mapping[str, Any], retries: Sequence[Mapping[str, Any]]) -> tuple[set[str], set[str], set[str]]:
    """The snapshot IDs the capture captured, failed and held, from the original run record and the retry records read. A failure or hold in any of them
    counts unless a record of the period captured that unit, and a unit counts once by its snapshot ID (a held unit that a retry then failed is failed). A
    record whose unit lists are missing or of the wrong type raises ``ValueError``."""
    try:
        records = (original, *retries)
        captured, failed, held = (
            set().union(*(snapshot_ids(record, states, wrapped) for record in records))
            for states, wrapped in zip((CAPTURED_STATES, FAILED_STATES, HELD_STATES), (True, True, False), strict=True)
        )
    except (KeyError, TypeError):
        raise ValueError("a run record names its failed, held and captured units") from None
    return captured, failed - captured, held - captured - failed


def carried_over(original: Mapping[str, Any], retries: Sequence[Mapping[str, Any]]) -> list[str]:
    """The source files no run of the retry period captured, failed or held: bronze keeps each one's earlier capture."""
    _, failed, held = capture_counts(original, retries)
    return sorted(failed | held)
