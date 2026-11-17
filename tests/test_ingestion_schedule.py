"""The monthly schedule of ingestion_dag: production only, a run id from the logical date alone, and a scheduled run that is always the monthly kind."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from orchestration import ingestion_schedule as schedule
from scripts.acquisition import publisher_run
from tests.support import check

RUN_ID = re.compile(r"^ingestion_[a-f0-9]{8}$")
FIRST = datetime(2026, 12, 1, tzinfo=UTC)


def context(run_type: str) -> dict:
    return {"dag_run": SimpleNamespace(run_type=run_type, logical_date=FIRST)}


def test_the_schedule_is_monthly_in_production_only() -> None:
    check(schedule.schedule_for({"HAI_ENVIRONMENT": "prod"}) == "0 0 1 * *", "production got no monthly schedule")
    check(schedule.schedule_for({"HAI_ENVIRONMENT": "dev"}) is None, "development got a schedule")
    check(schedule.schedule_for({}) is None, "a deployment that names no environment got a schedule")


@pytest.mark.parametrize("value", ["", "production", "Prod", "PROD", " prod", "staging"])
def test_an_unknown_environment_name_fails_visibly(value: str) -> None:
    with pytest.raises(ValueError, match="HAI_ENVIRONMENT"):
        schedule.schedule_for({"HAI_ENVIRONMENT": value})


def test_the_run_id_depends_on_the_logical_date_alone() -> None:
    first = schedule.scheduled_run_id(FIRST)
    check(RUN_ID.fullmatch(first) is not None, f"the run id does not match the DAG form's pattern: {first}")
    check(first == f"ingestion_{hashlib.sha256(FIRST.isoformat().encode()).hexdigest()[:8]}", f"the hex is not the digest of the logical date: {first}")
    check(schedule.scheduled_run_id(FIRST) == first, "the same logical date gave a different run id")
    other_zone = FIRST.astimezone(timezone(timedelta(hours=-8)))
    check(schedule.scheduled_run_id(other_zone) == first, "the same instant in another zone gave a different run id")
    check(schedule.scheduled_run_id(datetime(2027, 1, 1, tzinfo=UTC)) != first, "two months share a run id")


def test_a_logical_date_without_a_zone_is_refused() -> None:
    with pytest.raises(ValueError, match="time zone"):
        schedule.scheduled_run_id(datetime(2026, 12, 1))


def test_a_scheduled_run_takes_the_run_id_of_its_logical_date() -> None:
    expected = schedule.scheduled_run_id(FIRST)
    check(schedule.resolve_run_id(schedule.SCHEDULED_RUN, context("scheduled")) == expected, "a scheduled run did not take the run id of its date")
    check(schedule.resolve_run_id(expected, context("manual")) == expected, "a named run id was changed")


def test_a_hand_trigger_must_name_its_run() -> None:
    with pytest.raises(ValueError, match="names no run"):
        schedule.resolve_run_id(schedule.SCHEDULED_RUN, context("manual"))
    with pytest.raises(ValueError, match="names no run"):
        schedule.resolve_run_id(schedule.SCHEDULED_RUN, {})


def test_a_scheduled_run_is_always_the_monthly_kind() -> None:
    check(schedule.resolve_kind("bootstrap", context("scheduled")) == "monthly", "a scheduled run kept the form's bootstrap default")
    check(schedule.resolve_kind("monthly", context("scheduled")) == "monthly", "a scheduled monthly run changed its kind")


def test_a_hand_trigger_keeps_the_kind_it_names() -> None:
    check(schedule.resolve_kind("bootstrap", context("manual")) == "bootstrap", "a hand-triggered bootstrap changed its kind")
    check(schedule.resolve_kind("monthly", context("manual")) == "monthly", "a hand-triggered monthly run changed its kind")
    check(schedule.resolve_kind("bootstrap", {}) == "bootstrap", "a run with no DAG run changed its kind")


ORIGINAL = schedule.scheduled_run_id(FIRST)


def retry_context(day: int, hour: int = 0) -> dict:
    return {"dag_run": SimpleNamespace(run_type="scheduled", logical_date=datetime(2026, 12, day, hour, tzinfo=UTC))}


def test_the_retry_schedule_is_days_two_and_three_in_production_only() -> None:
    check(schedule.retry_schedule_for({"HAI_ENVIRONMENT": "prod"}) == "0 0 2-3 * *", "production got no retry schedule on days 2 and 3")
    check(schedule.retry_schedule_for({"HAI_ENVIRONMENT": "dev"}) is None, "development got a retry schedule")
    check(schedule.retry_schedule_for({}) is None, "a deployment that names no environment got a retry schedule")
    with pytest.raises(ValueError, match="HAI_ENVIRONMENT"):
        schedule.retry_schedule_for({"HAI_ENVIRONMENT": "production"})


def test_a_retry_run_id_depends_on_the_original_run_and_the_day_alone() -> None:
    ids = [schedule.retry_run_id(ORIGINAL, day) for day in range(1, 3)]
    digits = ORIGINAL.removeprefix("ingestion_")
    check(ids == [f"ingestion_retry_{digits}_1", f"ingestion_retry_{digits}_2"], f"a retry run id is not the original's hex and its day: {ids}")
    check(ORIGINAL not in ids and schedule.retry_run_id(ORIGINAL, 2) == ids[1], "the retry days do not have run ids of their own or a repeat differs")
    second_attempt = f"{ORIGINAL}_2"
    check(schedule.retry_run_id(second_attempt, 1) == f"ingestion_retry_{digits}_2_1", "the attempt of the original run is not kept before the day")


@pytest.mark.parametrize("day", [0, 3, 8, -1, True, 1.0, "1"])
def test_a_retry_day_outside_one_to_two_is_refused(day: Any) -> None:
    with pytest.raises(ValueError, match="one to 2"):
        schedule.retry_run_id(ORIGINAL, day)


@pytest.mark.parametrize("original", ["latest", "20261201T000000Z-0a1b2c3d", "silver_0a1b2c3d", "bronze_0a1b2c3d"])
def test_an_original_run_that_is_not_an_ingestion_run_id_is_refused(original: str) -> None:
    with pytest.raises(ValueError, match="not a run ID"):
        schedule.retry_run_id(original, 1)


def test_a_hand_started_ingestion_run_has_a_fresh_random_hex() -> None:
    ids = {schedule.new_ingestion_run_id() for _ in range(10)}
    check(len(ids) == 10 and all(RUN_ID.fullmatch(item) for item in ids), f"{ids}")


def test_a_scheduled_retry_takes_its_day_and_its_original_run_from_its_logical_date() -> None:
    for day in range(2, 4):
        found = schedule.resolve_retry(schedule.SCHEDULED_RUN, schedule.SCHEDULED_RETRY_DAY, retry_context(day))
        check(found == (ORIGINAL, day - 1), f"day {day} of the month resolved to {found}")


@pytest.mark.parametrize("context_of", [retry_context(1), retry_context(4), retry_context(4, 6), {}, context("manual")])
def test_a_scheduled_retry_outside_its_window_or_not_scheduled_is_refused(context_of: dict) -> None:
    with pytest.raises(ValueError, match="retry"):
        schedule.resolve_retry(schedule.SCHEDULED_RUN, schedule.SCHEDULED_RETRY_DAY, context_of)


def test_a_hand_triggered_retry_names_both_its_run_and_its_day_or_is_refused() -> None:
    check(schedule.resolve_retry(ORIGINAL, 2, context("manual")) == (ORIGINAL, 2), "a retry that names both was changed")
    with pytest.raises(ValueError, match="both"):
        schedule.resolve_retry(ORIGINAL, schedule.SCHEDULED_RETRY_DAY, context("manual"))
    with pytest.raises(ValueError, match="both"):
        schedule.resolve_retry(schedule.SCHEDULED_RUN, 2, context("manual"))


def test_the_job_and_the_launch_derive_the_same_retry_run_id() -> None:
    for day in range(1, 3):
        check(
            publisher_run.retry_run_id(ORIGINAL, day) == schedule.retry_run_id(ORIGINAL, day),
            f"the retry run id of day {day} differs between the job and the launch",
        )
