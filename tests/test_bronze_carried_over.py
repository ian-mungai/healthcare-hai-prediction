"""The owner notice of the carried-over files is sent once per bronze run, however often the check phase runs again."""

from __future__ import annotations

import json
from typing import Any

import pytest
from botocore.exceptions import ClientError

from scripts.lakehouse import bronze_run
from tests.support import check

HEX = "3f9a1c07"
RUN = f"bronze_{HEX}"
ORIGINAL = f"ingestion_{HEX}"
BUCKET = "example-bucket"
RECORD_KEY = f"lakehouse/bronze_runs/{RUN}/carried_over.json"
NOTICE_KEY = f"lakehouse/bronze_runs/{RUN}/carried_over_notice.json"
TOPIC = "arn:aws:sns:us-west-2:" + "1" * 12 + ":alerts"


def run_record(failed: list[str], completed: list[str] | None = None) -> bytes:
    units = {"failed": [{"snapshot_id": name} for name in failed], "paused_not_attempted": [], "completed": [{"snapshot_id": name} for name in completed or []]}
    return json.dumps({"units": units | {"changed": [], "unchanged": []}}).encode()


class Body:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self) -> bytes:
        return self.data


class Bucket:
    """A bucket that refuses a second write of a key that carries ``IfNoneMatch``, answers 403 for a missing key (the role cannot list the folder of its own
    records) and keeps every call the notice service receives."""

    def __init__(self, objects: dict[str, bytes], fail_publish: bool = False) -> None:
        self.objects, self.published, self.fail_publish, self.puts = dict(objects), [], fail_publish, []

    def list_object_versions(self, **arguments: str) -> dict[str, Any]:
        return {"Versions": [{"Key": key, "VersionId": f"v-{key}", "IsLatest": True} for key in self.objects if key.startswith(arguments["Prefix"])]}

    def get_object(self, **arguments: str) -> dict[str, Any]:
        key = arguments["Key"]
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "missing key, no list grant"}}, "GetObject")
        if "VersionId" in arguments and arguments["VersionId"] != f"v-{key}":
            raise ClientError({"Error": {"Code": "NoSuchVersion", "Message": "x"}}, "GetObject")
        return {"Body": Body(self.objects[key])}

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        key = arguments["Key"]
        if arguments.get("IfNoneMatch") == "*" and key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        self.objects[key] = arguments["Body"]
        self.puts.append(key)
        return {}

    def publish(self, **arguments: str) -> dict[str, str]:
        if self.fail_publish:
            raise OSError("the topic does not answer")
        self.published.append(arguments)
        return {"MessageId": f"m{len(self.published)}"}


def bucket(failed: list[str], **options: Any) -> Bucket:
    return Bucket({f"raw/ingestion_runs/{ORIGINAL}/run.json": run_record(failed)}, **options)


EMPTY = [{"table": "empty_data", "reason": "no source object"}]


def carried(store: Bucket, topic: str = TOPIC, root: str = "raw/", skipped: list[dict[str, str]] | None = None) -> dict[str, Any]:
    return bronze_run.record_carried_over(store, BUCKET, RUN, root, topic=topic, notify=store, skipped=skipped or [])


def test_a_second_call_for_the_same_run_sends_no_second_notice_and_reports_the_recorded_outcome() -> None:
    store = bucket(["a__1__x", "b__1__x"])
    first = carried(store)
    check(first["carried_over"] == ["a__1__x", "b__1__x"] and first["notice"] == "sent" and len(store.published) == 1, f"{first} {store.published}")
    again = carried(store)
    check(len(store.published) == 1, f"a repeated call published {len(store.published)} notices")
    check(again == first, f"the repeated call reported another record than the first: {again} {first}")
    check(json.loads(store.objects[RECORD_KEY])["carried_over"] == ["a__1__x", "b__1__x"], "the first record was replaced")


def test_a_repeated_call_reports_the_retained_record_even_when_the_sources_changed_since() -> None:
    store = bucket(["a__1__x"])
    first = carried(store)
    store.objects[f"raw/ingestion_runs/{ORIGINAL}/run.json"] = run_record(["a__1__x", "z__9__z"])
    again = carried(store)
    check(again["carried_over"] == ["a__1__x"] and again == first, f"{again}")
    check(len(store.published) == 1, "a changed source made a second notice")


def test_a_failed_notice_is_recorded_once_and_not_tried_again() -> None:
    store = bucket(["a__1__x"], fail_publish=True)
    first = carried(store)
    check(first["notice"] == "failed: OSError", f"{first}")
    store.fail_publish = False
    again = carried(store)
    check(again["notice"] == "failed: OSError" and not store.published, f"a retry sent a notice after a failed one was recorded: {again} {store.published}")


def test_an_interrupted_attempt_that_won_the_claim_but_recorded_no_outcome_is_reported_as_unknown_and_never_sent_again() -> None:
    store = bucket(["a__1__x"])
    carried(store)
    del store.objects[NOTICE_KEY]
    again = carried(store)
    check(again["notice"].startswith("unknown") and "no outcome" in again["notice"], f"{again}")
    check(len(store.published) == 1, "an interrupted attempt was sent again")


@pytest.mark.parametrize(
    ("failed", "topic", "root", "notice"),
    [
        ([], TOPIC, "raw/", "not needed: no file carried over"),
        (["a__1__x"], "", "raw/", "not sent: no alerts topic"),
        (["a__1__x"], TOPIC, "", "not sent"),
    ],
)
def test_a_record_with_no_notice_to_send_is_written_once_and_read_back_the_same(failed: list[str], topic: str, root: str, notice: str) -> None:
    store = bucket(failed)
    first = carried(store, topic, root)
    again = carried(store, topic, root)
    check(first["notice"] == notice and again == first and not store.published, f"{first} {again}")
    check(store.puts.count(RECORD_KEY) == 1, f"the record was written {store.puts.count(RECORD_KEY)} times")


def test_a_run_with_no_ingestion_run_record_says_so_and_names_no_file_and_a_repeat_agrees() -> None:
    store = Bucket({})
    first = carried(store)
    check(first["carried_over"] is None and "no run record" in first["note"], f"{first}")
    check(carried(store) == first, "a repeat reported another record")


def test_the_notice_outcome_is_written_after_the_notice_and_only_once() -> None:
    store = bucket(["a__1__x"])
    carried(store)
    carried(store)
    check(store.puts == [RECORD_KEY, NOTICE_KEY], f"the writes were {store.puts}")
    check(json.loads(store.objects[NOTICE_KEY])["notice"] == "sent", "the outcome record does not say sent")


def test_a_skipped_table_with_no_carried_over_file_is_recorded_and_noticed_once_and_a_repeat_sends_nothing() -> None:
    store = bucket([])
    first = carried(store, skipped=EMPTY)
    check(first["carried_over"] == [] and first["skipped_tables"] == EMPTY and first["notice"] == "sent", f"{first}")
    check(
        len(store.published) == 1 and "empty_data" in store.published[0]["Message"] and "no source object" in store.published[0]["Message"],
        f"{store.published}",
    )
    check("left out 1 table" in store.published[0]["Subject"], store.published[0]["Subject"])
    stored = json.loads(store.objects[RECORD_KEY])
    check(stored["skipped_tables"] == EMPTY, f"the immutable record does not name the skipped table: {stored}")
    again = carried(store, skipped=EMPTY)
    check(again == first and len(store.published) == 1, f"a repeat changed the report or sent another notice: {again} {len(store.published)}")


def test_the_notice_names_both_the_carried_over_files_and_the_skipped_tables() -> None:
    store = bucket(["a__1__x"])
    carried(store, skipped=EMPTY)
    check(len(store.published) == 1, f"{len(store.published)} notices")
    message = store.published[0]["Message"]
    check("a__1__x" in message and "empty_data" in message, message)


def test_a_skipped_table_is_noticed_even_when_the_files_carried_over_are_not_known() -> None:
    store = Bucket({})
    first = carried(store, skipped=EMPTY)
    check(first["carried_over"] is None and first["skipped_tables"] == EMPTY and first["notice"] == "sent" and len(store.published) == 1, f"{first}")
    check("empty_data" in store.published[0]["Message"], "the notice does not name the skipped table")


def test_a_skipped_table_with_no_alerts_topic_is_recorded_and_says_no_notice_was_sent() -> None:
    store = bucket([])
    first = carried(store, topic="", skipped=EMPTY)
    check(first["notice"] == "not sent: no alerts topic" and first["skipped_tables"] == EMPTY and not store.published, f"{first}")
    check(json.loads(store.objects[RECORD_KEY])["skipped_tables"] == EMPTY, "the record does not name the skipped table")


def test_an_interrupted_attempt_for_a_skipped_table_is_reported_as_unknown_and_never_sent_again() -> None:
    store = bucket([])
    carried(store, skipped=EMPTY)
    del store.objects[NOTICE_KEY]
    again = carried(store, skipped=EMPTY)
    check(again["notice"].startswith("unknown") and len(store.published) == 1, f"{again} {len(store.published)}")


def test_a_failed_notice_for_a_skipped_table_is_recorded_once_and_not_tried_again() -> None:
    store = bucket([], fail_publish=True)
    first = carried(store, skipped=EMPTY)
    check(first["notice"] == "failed: OSError", f"{first}")
    store.fail_publish = False
    check(carried(store, skipped=EMPTY)["notice"] == "failed: OSError" and not store.published, "a retry sent a notice after a failed one was recorded")


def test_no_file_and_no_skipped_table_still_needs_no_notice_and_records_an_empty_list() -> None:
    store = bucket([])
    first = carried(store)
    check(first["notice"] == "not needed: no file carried over" and first["skipped_tables"] == [] and not store.published, f"{first}")
