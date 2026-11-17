"""The Airflow run ID of a scheduled ingestion run is the run ID of its job, from the same functions, across day 1, the retry days and month and year ends."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from orchestration import ingestion_schedule as schedule
from orchestration import training_contract as contract
from tests.support import check


def hex_of(moment: datetime) -> str:
    """The hex of a scheduled run, written out here from its definition: the first eight digits of the SHA-256 of the UTC logical date."""
    return hashlib.sha256(moment.astimezone(UTC).isoformat().encode()).hexdigest()[:8]


def month_starts(first: tuple[int, int], last: tuple[int, int]) -> list[datetime]:
    found, (year, month) = [], first
    while (year, month) <= last:
        found.append(datetime(year, month, 1, tzinfo=UTC))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return found


MONTHS = month_starts((2026, 11), (2028, 4))  # a year end, a leap February (2028) and every length of month in between


@pytest.mark.parametrize("start", MONTHS, ids=lambda moment: moment.strftime("%Y-%m"))
def test_the_monthly_run_id_is_the_ingestion_hex_of_its_logical_date(start: datetime) -> None:
    run = schedule.scheduled_dag_run_id(schedule.MONTHLY_KIND, start)
    check(run == f"ingestion_{hex_of(start)}", f"{start}: {run}")
    check(re.fullmatch(contract.RUN_ID, run) is not None and re.fullmatch(schedule.RUN_ID_PATTERN, run) is not None, f"{run} is not a canonical run ID")
    check(run == schedule.scheduled_run_id(start), "the timetable and the task name the run differently")
    context = {"dag_run": SimpleNamespace(run_type="scheduled", logical_date=start)}
    check(schedule.resolve_run_id(schedule.SCHEDULED_RUN, context) == run, "the task resolves another run ID than the Airflow run ID")


@pytest.mark.parametrize("start", MONTHS, ids=lambda moment: moment.strftime("%Y-%m"))
def test_each_retry_day_of_a_month_has_the_retry_run_id_of_that_months_monthly_run(start: datetime) -> None:
    original = f"ingestion_{hex_of(start)}"
    for day in (schedule.RETRY_FIRST_DAY, schedule.RETRY_LAST_DAY):
        moment = start.replace(day=day)
        run = schedule.scheduled_dag_run_id(schedule.RETRY_KIND, moment)
        check(run == f"ingestion_retry_{hex_of(start)}_{day}", f"{moment}: {run}")
        check(re.fullmatch(contract.RUN_ID, run) is not None, f"{run} is not a canonical run ID")
        context = {"dag_run": SimpleNamespace(run_type="scheduled", logical_date=moment)}
        resolved = schedule.resolve_retry(schedule.SCHEDULED_RUN, schedule.SCHEDULED_RETRY_DAY, context)
        check(resolved == (original, day) and schedule.retry_run_id(*resolved) == run, f"the task resolves {resolved}, not the original and day of {run}")


def test_the_year_end_and_the_leap_february_name_the_right_original_and_day() -> None:
    check(
        schedule.scheduled_dag_run_id("retry", datetime(2027, 1, 3, tzinfo=UTC)) == f"ingestion_retry_{hex_of(datetime(2027, 1, 1, tzinfo=UTC))}_3", "January 3"
    )
    check(
        schedule.scheduled_dag_run_id("retry", datetime(2026, 12, 2, tzinfo=UTC)) == f"ingestion_retry_{hex_of(datetime(2026, 12, 1, tzinfo=UTC))}_2",
        "December 2",
    )
    check(
        schedule.scheduled_dag_run_id("retry", datetime(2028, 3, 2, tzinfo=UTC)) == f"ingestion_retry_{hex_of(datetime(2028, 3, 1, tzinfo=UTC))}_2",
        "March 2 after a leap",
    )
    ids = {schedule.scheduled_dag_run_id("monthly", moment) for moment in MONTHS}
    check(len(ids) == len(MONTHS), "two months share a run ID")


def test_an_instant_in_another_time_zone_names_the_run_of_its_utc_instant() -> None:
    local = datetime(2026, 12, 1, 0, 0, tzinfo=UTC).astimezone(timezone(timedelta(hours=-8)))
    check(schedule.scheduled_dag_run_id("monthly", local) == f"ingestion_{hex_of(datetime(2026, 12, 1, tzinfo=UTC))}", "a Pacific time named another run")


@pytest.mark.parametrize(
    ("kind", "moment"),
    [
        ("monthly", datetime(2026, 12, 2, tzinfo=UTC)),
        ("monthly", datetime(2026, 12, 1, 1, tzinfo=UTC)),
        ("monthly", datetime(2026, 12, 1)),
        ("retry", datetime(2026, 12, 1, tzinfo=UTC)),
        ("retry", datetime(2026, 12, 4, tzinfo=UTC)),
        ("retry", datetime(2026, 12, 2, 0, 1, tzinfo=UTC)),
        ("retry", datetime(2026, 12, 2)),
        ("weekly", datetime(2026, 12, 1, tzinfo=UTC)),
    ],
)
def test_a_time_the_cron_never_gives_names_no_run(kind: str, moment: datetime) -> None:
    with pytest.raises(ValueError, match=r"midnight|kind|zone"):
        schedule.scheduled_dag_run_id(kind, moment)
