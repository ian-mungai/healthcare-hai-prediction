"""The monthly schedule of ingestion_dag: production only, a run id from the logical date alone, and a scheduled run that is always the monthly kind."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from orchestration import ingestion_schedule as schedule
from tests.support import check

RUN_ID = re.compile(r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$")
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
    check(first.startswith("20261201T000000Z-"), f"the run id does not carry its logical date: {first}")
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
