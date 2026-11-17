"""The start of bronze: a hand start runs it at once, a scheduled run after its retry period, and the files that period did not capture are named."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from orchestration import ingestion_schedule as schedule
from tests.support import check

HEX = "3f9a1c07"
ORIGINAL = f"ingestion_{HEX}"


def context(run_type: str) -> dict[str, Any]:
    return {"dag_run": SimpleNamespace(run_type=run_type, logical_date=datetime(2026, 12, 1, tzinfo=UTC))}


@pytest.mark.parametrize(
    ("ingestion", "bronze"),
    [
        (ORIGINAL, f"bronze_{HEX}"),
        (f"{ORIGINAL}_2", f"bronze_{HEX}_2"),
        (f"ingestion_retry_{HEX}_2", f"bronze_{HEX}"),
        (f"ingestion_retry_{HEX}_3_1", f"bronze_{HEX}_3"),
    ],
)
def test_the_bronze_run_takes_the_hex_and_the_attempt_of_its_ingestion_run(ingestion: str, bronze: str) -> None:
    check(schedule.bronze_run_for(ingestion) == bronze, f"{ingestion} gave {schedule.bronze_run_for(ingestion)}")


@pytest.mark.parametrize("run", ["latest", f"silver_{HEX}", "20261116T201053Z-b10a4f2d", f"bronze_{HEX}"])
def test_only_an_ingestion_run_starts_a_bronze_run(run: str) -> None:
    with pytest.raises(ValueError, match="ingestion"):
        schedule.bronze_run_for(run)


def test_a_hand_started_ingestion_run_starts_bronze_at_once_and_a_scheduled_one_waits_for_its_retry_period() -> None:
    check(schedule.bronze_after_ingestion(ORIGINAL, context("manual")) == f"bronze_{HEX}", "a hand start did not start bronze")
    check(schedule.bronze_after_ingestion(ORIGINAL, {}) == f"bronze_{HEX}", "a run with no DAG run is a hand start")
    check(schedule.bronze_after_ingestion(ORIGINAL, context("scheduled")) is None, "a scheduled run started bronze before its retry period")


def test_only_the_last_scheduled_retry_day_starts_bronze() -> None:
    last = schedule.RETRY_DAYS
    check(schedule.bronze_after_retry(ORIGINAL, last, context("scheduled")) == f"bronze_{HEX}", "the last retry day did not start bronze")
    for day in range(1, last):
        check(schedule.bronze_after_retry(ORIGINAL, day, context("scheduled")) is None, f"retry day {day} started bronze inside the period")
    check(schedule.bronze_after_retry(ORIGINAL, last, context("manual")) is None, "a hand retry started bronze: only the owner starts a suffixed bronze run")
    check(schedule.bronze_after_retry(f"{ORIGINAL}_2", last, context("scheduled")) == f"bronze_{HEX}_2", "the attempt did not carry")


def test_the_ingestion_runs_of_a_bronze_run_are_its_original_and_each_retry_day() -> None:
    check(
        schedule.ingestion_runs_of(f"bronze_{HEX}") == (ORIGINAL, tuple(f"ingestion_retry_{HEX}_{day}" for day in range(1, schedule.RETRY_DAYS + 1))),
        f"{schedule.ingestion_runs_of(f'bronze_{HEX}')}",
    )
    check(schedule.ingestion_runs_of(f"bronze_{HEX}_2")[1][0] == f"ingestion_retry_{HEX}_2_1", "the attempt is not before the day")
    with pytest.raises(ValueError, match="bronze"):
        schedule.ingestion_runs_of(ORIGINAL)


def record(failed: list[str], paused: list[str], completed: list[str] | None = None) -> dict[str, Any]:
    wrap = [{"snapshot_id": name} for name in failed]
    done = [{"snapshot_id": name} for name in completed or []]
    return {"units": {"failed": wrap, "paused_not_attempted": paused, "completed": done, "changed": [], "unchanged": []}}


def test_a_file_no_run_of_the_period_captured_is_carried_over() -> None:
    first = record(["a__1__x", "b__1__x"], ["c__1__x"])
    check(schedule.carried_over(first, []) == ["a__1__x", "b__1__x", "c__1__x"], "a file the original run failed is not carried over")
    day1 = record([], [], ["b__1__x"])
    check(schedule.carried_over(first, [day1]) == ["a__1__x", "c__1__x"], "a file a retry day captured is carried over")
    check(schedule.carried_over(first, [day1, record([], [], ["a__1__x", "c__1__x"])]) == [], "nothing is carried over when every file was captured")
    check(schedule.carried_over(record([], []), []) == [], "a run that failed nothing carries nothing")
    again = record(["a__1__x", "a__1__x"], ["a__1__x"])
    check(schedule.carried_over(again, []) == ["a__1__x"], "a file is named twice")


def test_a_record_without_its_units_is_refused() -> None:
    for bad in ({}, {"units": {}}, {"units": {"failed": [{"no_id": 1}], "paused_not_attempted": []}}):
        with pytest.raises(ValueError, match="run record"):
            schedule.carried_over(bad, [])


def test_the_notice_names_every_carried_over_file_and_the_run() -> None:
    files = [f"source_{index}__20261101T000000Z__abc" for index in range(150)]
    subject, message = schedule.carried_over_notice(f"bronze_{HEX}", files)
    check(f"bronze_{HEX}" in subject and "150" in subject, subject)
    check(all(name in message for name in files), "the notice leaves a carried-over file out")
    check(len(subject) <= 100, "an SNS subject holds 100 characters")
    none_subject, none_message = schedule.carried_over_notice(f"bronze_{HEX}", [])
    check("no file" in none_subject.lower() and "no file" in none_message.lower(), f"{none_subject} {none_message}")


def test_the_bronze_run_record_names_the_carried_over_files_and_the_notice(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.lakehouse import bronze_run

    class Body:
        def __init__(self, data: bytes) -> None:
            self.data = data

        def read(self) -> bytes:
            return self.data

    class Client:
        def __init__(self, objects: dict[str, bytes]) -> None:
            self.objects, self.puts, self.published = objects, {}, []

        def get_object(self, **arguments: str) -> dict[str, Any]:
            from botocore.exceptions import ClientError

            if arguments["Key"] not in self.objects:
                raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
            return {"Body": Body(self.objects[arguments["Key"]])}

        def put_object(self, **arguments: Any) -> dict[str, Any]:
            self.puts[arguments["Key"]] = arguments["Body"]
            return {}

        def publish(self, **arguments: str) -> dict[str, str]:
            self.published.append(arguments)
            return {"MessageId": "m1"}

    objects = {
        f"raw/ingestion_runs/{ORIGINAL}/run.json": json.dumps(record(["a__1__x", "b__1__x"], [])).encode(),
        f"raw/ingestion_runs/ingestion_retry_{HEX}_1/run.json": json.dumps(record([], [], ["b__1__x"])).encode(),
    }
    client = Client(objects)
    result = bronze_run.record_carried_over(client, "bucket", f"bronze_{HEX}", "raw/", topic=f"arn:aws:sns:us-west-2:{'1' * 12}:alerts", notify=client)
    check(result["carried_over"] == ["a__1__x"] and result["notice"] == "sent", f"{result}")
    stored = json.loads(client.puts[f"lakehouse/bronze_runs/bronze_{HEX}/carried_over.json"])
    check(stored["carried_over"] == ["a__1__x"] and stored["ingestion_run"] == ORIGINAL and stored["notice"] == "sent", f"{stored}")
    check(len(client.published) == 1 and "a__1__x" in client.published[0]["Message"], "the notice does not name the file")
    quiet = Client(objects)
    unset = bronze_run.record_carried_over(quiet, "bucket", f"bronze_{HEX}", "raw/", topic="", notify=quiet)
    check(unset["notice"] == "not sent: no alerts topic" and not quiet.published, f"{unset}")
    missing = Client({})
    gone = bronze_run.record_carried_over(missing, "bucket", f"bronze_{HEX}", "raw/", topic="", notify=missing)
    check(gone["carried_over"] is None and "no run record" in gone["note"], f"{gone}")
