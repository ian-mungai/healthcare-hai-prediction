"""The owner notice of a bronze run is sent first and then recorded once. A repeat that finds the record sends nothing. A notice that did not get
sent leaves no record, so the next check tries again."""

from __future__ import annotations

import json
import re
from types import SimpleNamespace
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from orchestration import owner_notice
from scripts.lakehouse import bronze_run
from tests.support import check

HEX = "3f9a1c07"
RUN = f"bronze_{HEX}"
ORIGINAL = f"ingestion_{HEX}"
BUCKET = "example-bucket"
RECORD_KEY = f"lakehouse/bronze_runs/{RUN}/carried_over.json"
NOTICE_KEY = f"lakehouse/bronze_runs/{RUN}/carried_over_notice.json"
DENIED = ClientError({"Error": {"Code": "AccessDenied", "Message": "no write grant"}}, "PutObject")
REFUSED, STALLED = EndpointConnectionError(endpoint_url="https://example.invalid"), ReadTimeoutError(endpoint_url="https://example.invalid")
FAULTS = pytest.mark.parametrize("fault", [OSError("the topic does not answer"), REFUSED, STALLED], ids=["error", "refused connection", "stalled read"])
TOPIC = "arn:aws:sns:us-west-2:" + "1" * 12 + ":alerts"


def run_record(
    failed: list[str],
    completed: list[str] | None = None,
    refused: list[str] = (),
    keyed: list[str] = (),
    changed: list[str] = (),
    unchanged: list[str] = (),
    raw: dict | None = None,
) -> bytes:
    """A run record: the names become units, and ``raw`` replaces whole unit lists as they are."""
    units = {"failed": [{"snapshot_id": name} for name in failed], "refused": [{"snapshot_id": name} for name in refused], "keyed_not_attempted": list(keyed)}
    units |= {"paused_not_attempted": [], "completed": [{"snapshot_id": name} for name in completed or []]}
    units |= {"changed": [{"snapshot_id": name} for name in changed], "unchanged": [{"snapshot_id": name} for name in unchanged]}
    return json.dumps({"units": units | (raw or {})}).encode()


class Body:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self) -> bytes:
        return self.data


class Bucket:
    """A bucket whose calls can raise a fault. It refuses a second write of a key that carries ``IfNoneMatch``, answers 403 for a missing key (the role cannot
    list the folder of its own records) and keeps every call the notice service receives."""

    def __init__(self, objects: dict[str, bytes], fail_publish: bool = False) -> None:
        self.objects, self.fail_publish = dict(objects), fail_publish
        self.published: list[Any] = []
        self.puts: list[Any] = []

    def list_object_versions(self, **arguments: str) -> dict[str, Any]:
        self.listed += 1
        self.fail("list_object_versions")
        return {"Versions": [{"Key": key, "VersionId": f"v-{key}", "IsLatest": True} for key in self.objects if key.startswith(arguments["Prefix"])]}

    def get_object(self, **arguments: str) -> dict[str, Any]:
        key = arguments["Key"]
        self.fail("get_object")
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "missing key, no list grant"}}, "GetObject")
        if "VersionId" in arguments and arguments["VersionId"] != f"v-{key}":
            raise ClientError({"Error": {"Code": "NoSuchVersion", "Message": "x"}}, "GetObject")
        return {"Body": Body(self.objects[key])}

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        key = arguments["Key"]
        self.fail("put_object")
        if arguments.get("IfNoneMatch") == "*" and key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        self.objects[key] = arguments["Body"]
        self.puts.append(key)
        return {}

    def publish(self, **arguments: str) -> dict[str, str]:
        self.puts_at_send, self.send_calls = list(self.puts), self.send_calls + 1
        self.fail("publish")
        self.published.append(arguments)
        return {"MessageId": f"m{len(self.published)}"}


def retry_key(day: int) -> str:
    return f"raw/ingestion_runs/ingestion_retry_{HEX}_{day}/run.json"


def bucket(failed: list[str], original: bytes | None = None, retries: dict[int, bytes] | None = None, **options: Any) -> Bucket:
    """The original run record and, unless ``retries`` says otherwise, an empty record for each retry day."""
    objects = {f"raw/ingestion_runs/{ORIGINAL}/run.json": run_record(failed) if original is None else original}
    return Bucket(objects | {retry_key(day): body for day, body in (retries or {2: run_record([]), 3: run_record([])}).items()}, **options)


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


@pytest.mark.parametrize(
    ("failed", "topic", "root", "notice"),
    [
        ([], "", "raw/", "not sent: no alerts topic"),
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
    message = store.published[0]["Message"]
    check(len(store.published) == 1 and f"The run record of {ORIGINAL} is missing, so its files are not counted." in message, message)
    check("Captured files" not in message and "not captured" not in message and "Every file" not in message, f"a missing record was counted:\n{message}")


def test_the_notice_is_sent_first_and_then_one_record_holds_what_was_reported_and_the_outcome() -> None:
    store = bucket(["a__1__x"])
    carried(store)
    carried(store)
    check(store.puts_at_send == [] and store.puts == [RECORD_KEY], f"the record was written before the send, or twice: {store.puts_at_send} {store.puts}")
    stored = json.loads(store.objects[RECORD_KEY])
    check(stored["notice"] == "sent" and stored["carried_over"] == ["a__1__x"] and NOTICE_KEY not in store.objects, f"{stored}")


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


def test_a_good_month_is_noticed_once_with_its_three_counts_and_a_repeat_sends_nothing() -> None:
    store = bucket([], run_record([], ["a__1__x", "b__1__x", "c__1__x"]))
    first = carried(store)
    again = carried(store)
    sent = store.published[0]
    check(first["notice"] == "sent" and first["carried_over"] == [] and again == first and len(store.published) == 1, f"{first} {again} {store.published}")
    check(sent["Subject"] == f"Bronze run {RUN} captured 3 files", sent["Subject"])
    lines = sent["Message"].split("\n")
    check(all(line in lines for line in ("Captured files: 3", "Failed files: 0", "Held files: 0", "Every file was captured.")), sent["Message"])
    check("not counted" not in sent["Message"] and "No action is needed." in lines, sent["Message"])


def test_failed_refused_and_held_files_are_counted_as_the_capture_counts_them_and_a_retry_recovers_one() -> None:
    original = run_record(["a__1__x"], ["d__1__x"], refused=["b__1__x"], keyed=["c__1__x"])
    store = bucket([], original, {2: run_record([], ["a__1__x"]), 3: run_record([])})
    first = carried(store)
    lines = store.published[0]["Message"].split("\n")
    check(first["carried_over"] == ["b__1__x", "c__1__x"], f"{first}")
    check(all(line in lines for line in ("Captured files: 2", "Failed files: 1", "Held files: 1")), store.published[0]["Message"])
    check("Every file was captured." not in lines and "- b__1__x" in lines and "- c__1__x" in lines, store.published[0]["Message"])


def test_a_missing_retry_record_is_named_and_its_files_are_not_claimed_as_not_captured() -> None:
    store = bucket(["a__1__x"], retries={2: run_record([])})
    first = carried(store)
    message, subject = store.published[0]["Message"], store.published[0]["Subject"]
    check(first["notice"] == "sent" and first["unread_records"] == {f"ingestion_retry_{HEX}_3": "is missing"}, f"{first}")
    check(f"The run record of ingestion_retry_{HEX}_3 is missing, so its files are not counted." in message, message)
    check("Every file was captured." not in message and "In the run records that could be read" in message, message)
    check("could not read 1 run record" in subject, subject)


def test_a_month_with_no_failed_file_and_a_missing_retry_record_does_not_say_every_file_was_captured() -> None:
    store = bucket([], run_record([], ["a__1__x"]), {2: run_record([])})
    carried(store)
    message = store.published[0]["Message"]
    check("Captured files: 1" in message and "Every file was captured." not in message and "No action is needed." not in message, message)
    check("could not read 1 run record" in store.published[0]["Subject"] and "captured" not in store.published[0]["Subject"], store.published[0]["Subject"])


@pytest.mark.parametrize(
    "body", [b"not json", b"[]", json.dumps({"units": {"completed": [], "changed": [], "unchanged": [], "paused_not_attempted": []}}).encode()]
)
def test_an_original_record_that_is_not_json_not_an_object_or_has_no_failed_list_is_named_and_never_raises(body: bytes) -> None:
    store = bucket([], body)
    first = carried(store)
    message = store.published[0]["Message"]
    check(first["notice"] == "sent" and first["unread_records"] == {ORIGINAL: "cannot be read"}, f"{first}")
    check(f"The run record of {ORIGINAL} cannot be read, so its files are not counted." in message, message)
    check("Every file" not in message and "Captured files: 0" in message, message)


@pytest.mark.parametrize(
    "units",
    [
        {"failed": [{"snapshot_id": 1}]},
        {"keyed_not_attempted": [17]},
        {"keyed_not_attempted": "abc"},
        {"refused": [{"snapshot_id": None}]},
        {"completed": "abc"},
        {"changed": [{"snapshot_id": ["x"]}]},
        {"paused_not_attempted": {"a": 1}},
    ],
)
def test_a_unit_list_or_id_of_the_wrong_type_makes_an_unreadable_record_left_out_of_the_counts_and_never_an_error(units: dict[str, Any]) -> None:
    store = bucket([], run_record([], ["a__1__x"], raw=units))
    first = carried(store)
    message = store.published[0]["Message"]
    check(first["notice"] == "sent" and first["unread_records"] == {ORIGINAL: "cannot be read"}, f"{first}")
    check("Every file" not in message and f"The run record of {ORIGINAL} cannot be read" in message, message)


def test_a_failure_in_a_retry_record_counts_although_the_original_failed_nothing_and_no_success_is_claimed() -> None:
    store = bucket([], run_record([], ["a__1__x"]), {2: run_record(["b__1__x"]), 3: run_record(["b__1__x"])})
    first = carried(store)
    message = store.published[0]["Message"]
    lines = message.split("\n")
    check(first["carried_over"] == ["b__1__x"] and first["counts"] == {"captured": 1, "failed": 1, "held": 0}, f"{first}")
    check("Every file was captured." not in lines and "- b__1__x" in lines and "Failed files: 1" in lines, message)


def test_a_unit_is_counted_once_by_its_snapshot_id_and_a_held_unit_that_later_failed_is_failed() -> None:
    store = bucket(
        [],
        run_record([], ["a__1__x"], keyed=["k__1__x"]),
        {2: run_record(["k__1__x"], ["a__1__x"], unchanged=["a__1__x"]), 3: run_record(["k__1__x"], changed=["a__1__x"])},
    )
    carried(store)
    lines = store.published[0]["Message"].split("\n")
    check(all(line in lines for line in ("Captured files: 1", "Failed files: 1", "Held files: 0")), store.published[0]["Message"])


def test_files_a_retry_changed_or_found_unchanged_are_captured_and_no_longer_failed() -> None:
    store = bucket([], run_record(["a__1__x", "b__1__x"]), {2: run_record([], changed=["a__1__x"]), 3: run_record([], unchanged=["b__1__x"])})
    first = carried(store)
    check(first["counts"] == {"captured": 2, "failed": 0, "held": 0} and first["carried_over"] == [], f"{first}")


def test_a_run_with_no_capture_root_reads_no_capture_record() -> None:
    store = bucket([])
    carried(store, root="", skipped=EMPTY)
    check(store.listed == 0 and len(store.published) == 1, f"{store.listed} listings, {len(store.published)} notices")


def test_the_message_says_the_bronze_run_completed_and_names_the_capture_records_it_read_without_claiming_a_load() -> None:
    store = bucket(["a__1__x"], retries={2: run_record([])})
    carried(store)
    lines = store.published[0]["Message"].split("\n")
    check(f"Bronze run {RUN} completed." in lines and f"Capture records read: {ORIGINAL}, ingestion_retry_{HEX}_2." in lines, store.published[0]["Message"])
    check("loaded the data" not in store.published[0]["Message"], "the message links the load to the ingestion run")
    none = Bucket({})
    carried(none, root="", skipped=EMPTY)
    check("Capture records read: none." in none.published[0]["Message"] and "loaded the data" not in none.published[0]["Message"], none.published[0]["Message"])


def test_the_notice_clients_have_short_timeouts_and_one_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    options = bronze_run.notice_storage().client.meta.config
    check(
        (options.connect_timeout, options.read_timeout, options.retries["total_max_attempts"]) == (2, 3, 1),
        f"{options.connect_timeout} {options.read_timeout} {options.retries}",
    )


def test_the_sns_client_of_the_notice_is_made_with_the_short_timeouts_when_none_is_given(monkeypatch: pytest.MonkeyPatch) -> None:
    store, made = bucket(["a__1__x"]), []
    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: made.append((args, kwargs)) or store)
    bronze_run.record_carried_over(store, BUCKET, RUN, "raw/", topic=TOPIC)
    options = made[0][1]["config"]
    check(made[0][0] == ("sns",) and (options.connect_timeout, options.read_timeout, options.retries["total_max_attempts"]) == (2, 3, 1), f"{made}")
    check(len(store.published) == 1, "the notice was not sent through the client")


def test_a_notice_that_cannot_even_make_its_client_leaves_the_check_phase_going(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    def refuse() -> Any:
        raise KeyError("AWS_REGION")

    monkeypatch.setattr(bronze_run, "notice_storage", refuse)
    bronze_run.run_notice(SimpleNamespace(bucket=BUCKET, run=RUN), [])
    check("the capture notice did not start" in capsys.readouterr().err, "the failure is not shown on standard error")


def test_the_counts_of_every_record_read_are_given_when_the_original_record_is_missing() -> None:
    store = Bucket({retry_key(2): run_record(["b__1__x"], changed=["a__1__x"], keyed=["c__1__x"])})
    first = carried(store)
    lines = store.published[0]["Message"].split("\n")
    check(first["counts"] == {"captured": 1, "failed": 1, "held": 1} and first["carried_over"] == ["b__1__x", "c__1__x"], f"{first}")
    check(all(line in lines for line in ("Captured files: 1", "Failed files: 1", "Held files: 1", f"Capture records read: ingestion_retry_{HEX}_2.")), lines)
    check(f"The run record of {ORIGINAL} is missing, so its files are not counted." in lines and "Every file was captured." not in lines, lines)
    check(f"Bronze run {RUN} completed." in lines and "loaded the data" not in store.published[0]["Message"], lines)


@FAULTS
def test_a_fault_on_the_earlier_record_stops_the_notice_with_no_record_and_the_next_attempt_sends(fault: Exception, capsys: pytest.CaptureFixture[str]) -> None:
    store = bucket(["a__1__x"], broken={"get_object": fault})
    first = carried(store)
    check(first["notice"] == f"stopped: {type(fault).__name__}" and not store.published and RECORD_KEY not in store.objects, f"{first}")
    check("the capture notice stopped" in capsys.readouterr().err, "the stop is not shown on standard error")
    store.broken.clear()
    check(carried(store)["notice"] == "sent" and len(store.published) == 1, "the healthy attempt did not send")


@FAULTS
def test_a_fault_on_the_capture_records_is_named_in_the_sent_notice_and_recorded(fault: Exception) -> None:
    store = bucket(["a__1__x"], broken={"list_object_versions": fault})
    first = carried(store)
    check(first["notice"] == "sent" and set(first["unread_records"]) == {ORIGINAL, f"ingestion_retry_{HEX}_2", f"ingestion_retry_{HEX}_3"}, f"{first}")
    check(store.published[0]["Message"].count("cannot be read, so its files are not counted.") == 3, store.published[0]["Message"])
    carried(store)
    check(len(store.published) == 1 and json.loads(store.objects[RECORD_KEY])["notice"] == "sent", "a repeat sent again or the record is missing")


@FAULTS
def test_a_fault_on_the_send_is_tried_once_leaves_no_record_and_the_next_attempt_sends(fault: Exception, capsys: pytest.CaptureFixture[str]) -> None:
    store = bucket(["a__1__x"], broken={"publish": fault})
    first = carried(store)
    check(first["notice"] == f"stopped: {type(fault).__name__}" and store.send_calls == 1 and RECORD_KEY not in store.objects, f"{first} {store.send_calls}")
    check("the capture notice stopped" in capsys.readouterr().err, "the stop is not shown on standard error")
    store.broken.clear()
    check(carried(store)["notice"] == "sent" and len(store.published) == 1, "the healthy attempt did not send")
    check(json.loads(store.objects[RECORD_KEY])["notice"] == "sent", "the sent notice is not recorded")


@pytest.mark.parametrize("fault", [DENIED, REFUSED, STALLED], ids=["denied", "refused connection", "stalled read"])
def test_a_fault_on_the_record_write_stops_nothing_and_the_next_attempt_sends_again(fault: Exception, capsys: pytest.CaptureFixture[str]) -> None:
    store = bucket(["a__1__x"], broken={"put_object": fault})
    first = carried(store)
    check(len(store.published) == 1 and first["notice"] == f"sent; stopped: {type(fault).__name__}" and RECORD_KEY not in store.objects, f"{first}")
    check("the capture notice stopped" in capsys.readouterr().err, "the stop is not shown on standard error")
    store.broken.clear()
    carried(store)
    check(len(store.published) == 2 and store.puts == [RECORD_KEY], f"{len(store.published)} sent, writes {store.puts}")


def test_the_counts_of_both_retry_days_are_given_when_the_original_record_is_missing() -> None:
    day2, day3 = run_record(["b__1__x"], changed=["a__1__x"]), run_record([], changed=["c__1__x"], keyed=["d__1__x"])
    store = Bucket({retry_key(2): day2, retry_key(3): day3})
    first = carried(store)
    check(first["counts"] == {"captured": 2, "failed": 1, "held": 1} and first["carried_over"] == ["b__1__x", "d__1__x"], f"{first}")
    check(
        f"Capture records read: ingestion_retry_{HEX}_2, ingestion_retry_{HEX}_3." in store.published[0]["Message"].split("\n"), store.published[0]["Message"]
    )


LIVE_CLAIMED = {"schema": 1, "bronze_run": RUN, "carried_over": None, "notice": "claimed", "note": "no capture root is configured (HAI_CAPTURE_ROOT): x"}


@pytest.mark.parametrize(
    "earlier",
    [LIVE_CLAIMED, {"notice": "sent"}, {"notice": "failed: OSError"}, {"notice": "not needed: no file carried over"}, {"notice": "not sent: no alerts topic"}],
    ids=["claimed", "sent", "failed", "not needed", "not sent"],
)
def test_any_existing_record_is_final_and_is_reported_without_a_send(earlier: dict[str, Any]) -> None:
    store = bucket(["a__1__x"])
    store.objects[RECORD_KEY] = json.dumps(earlier).encode()
    done = carried(store)
    check(not store.published and not store.puts and done == earlier, f"{len(store.published)} sent, writes {store.puts}, reported {done}")


POINTERS = ("carried_over", "lists them", "full text", "could be saved", "Run record:", "in the run record", "project data bucket", "Truncated", "file list in")


def email_of(files: list[str], skipped: list[dict[str, str]] | None = None) -> str:
    """The message of the notice for a month that failed ``files``."""
    store = bucket(files)
    carried(store, skipped=skipped)
    return store.published[0]["Message"]


def shown(message: str, title: str) -> tuple[list[str], list[str]]:
    """The names in the list of the message under ``title`` and the lines that follow them up to the blank line."""
    lines = message.split("\n")
    rest = lines[lines.index(f"{title}:") + 1 :]
    rest = rest[: next((number for number, line in enumerate(rest) if line == "" or (line.endswith(":") and not line.startswith("- "))), len(rest))]
    return [line[2:] for line in rest if line.startswith("- ")], [line for line in rest if not line.startswith("- ")]


STATES = {
    "good month": lambda: bucket([], run_record([], ["a__1__x"])),
    "files carried over": lambda: bucket(["a__1__x"]),
    "table left out": lambda: bucket([]),
    "missing original record": lambda: Bucket({}),
    "malformed record": lambda: bucket([], b"not json"),
    "no capture root and a table left out": lambda: bucket([]),
}


@pytest.mark.parametrize("state", list(STATES))
def test_no_state_of_the_email_points_to_a_record_or_to_any_place_outside_the_message(state: str) -> None:
    store = STATES[state]()
    no_root = state.startswith("no capture root")
    carried(store, root="" if no_root else "raw/", skipped=EMPTY if "table" in state else None)
    message = store.published[0]["Message"]
    check(not [word for word in POINTERS if word in message], f"{state}: {[word for word in POINTERS if word in message]}\n{message}")
    check(
        f"Bronze run in Airflow: {RUN}" in message and f"Capture runs in Airflow: {ORIGINAL}, ingestion_retry_{HEX}_2, ingestion_retry_{HEX}_3" in message,
        message,
    )


def check_shown(message: str, title: str, total: int) -> int:
    """The list under ``title`` says exactly how many names it shows and how many it does not, and the text is within the limit."""
    names, notes = shown(message, title)
    check(len(message.encode()) <= owner_notice.BODY_LIMIT and owner_notice.TRUNCATED not in message, f"{len(message.encode())} bytes")
    if len(names) == total:
        check(notes == [], f"a complete list carries a note: {notes}")
    else:
        check(notes == [f"Showing {len(names)} of {total}. Not shown: {total - len(names)}."], f"{len(names)} names, notes {notes}")
    return len(names)


def test_a_list_cut_by_the_item_limit_says_how_many_it_shows_and_how_many_it_does_not() -> None:
    message = email_of([f"file-{number:04d}" for number in range(1001)])
    check(check_shown(message, "Carried-over files", 1001) == 1000 and "Showing 1000 of 1001. Not shown: 1." in message, message[-300:])


@pytest.mark.parametrize("pad", [87, 99])
def test_a_list_cut_by_the_byte_limit_counts_the_names_that_are_in_the_final_text(pad: int) -> None:
    names = [f"file-{number:04d}-" + "x" * pad for number in range(1001)]
    message = email_of(names)
    count = check_shown(message, "Carried-over files", 1001)
    check(0 < count < 1000 and len(message.encode()) > owner_notice.BODY_LIMIT - 300, f"{count} names, {len(message.encode())} bytes")
    check(not [word for word in POINTERS if word in message], "a pointer reached the text")


def test_the_largest_list_that_fits_is_complete_and_one_more_name_is_cut_with_exact_numbers() -> None:
    def make(total: int) -> list[str]:
        return [f"file-{number:04d}-" + "x" * 99 for number in range(total)]

    low, high = 1, 1000
    while low < high:
        middle = (low + high + 1) // 2
        low, high = (middle, high) if "Showing" not in email_of(make(middle)) else (low, middle - 1)
    check(low < 1000, "the boundary is the item limit, not the byte limit")
    fits, over = email_of(make(low)), email_of(make(low + 1))
    check(check_shown(fits, "Carried-over files", low) == low, f"{low} names do not all fit")
    check(check_shown(over, "Carried-over files", low + 1) < low + 1, "one more name was not cut")


def test_a_very_long_name_that_alone_nears_the_byte_limit_is_not_shown_and_the_numbers_say_so() -> None:
    message = email_of(["a" * 99_800, "short"])
    check(check_shown(message, "Carried-over files", 2) == 0 and "Not shown: 2." in message, message[-200:])


def test_two_long_lists_share_the_byte_limit_and_each_counts_the_names_it_shows() -> None:
    names = [f"source_{number:04d}__20261101T000000Z__" + "a" * 60 for number in range(2000)]
    tables = [{"table": f"table_{number}", "reason": "no source object"} for number in range(2000)]
    message = email_of(names, tables)
    files = check_shown(message, "Carried-over files", 2000)
    tables_shown = check_shown(message, "Skipped tables", 2000)
    check(files + tables_shown < 4000 and len(message.encode()) > owner_notice.BODY_LIMIT - 400, f"{files} files and {tables_shown} tables shown")
    check(re.search(r"Showing \d+ of 2000\. Not shown: \d+\.", message) is not None, "the numbers are missing")


READ_ALL = "Capture records read: ingestion_3f9a1c07, ingestion_retry_3f9a1c07_2, ingestion_retry_3f9a1c07_3."
NOT_COUNTED = "The run record of {} {}, so its files are not counted."
UNREAD = "Find out why a run record is missing or cannot be read.\nThe run may have stopped before it saved its record, after it captured files."
LEFT_OUT = "This run left the table out.\nIts published data does not include the table."
CHECK_TABLE = "Check if the skipped table should have a source file.\nIf it should, find out why the ingestion run did not capture one."
WHERE = f"Bronze run in Airflow: {RUN}\nCapture runs in Airflow: {ORIGINAL}, ingestion_retry_{HEX}_2, ingestion_retry_{HEX}_3"
COUNTS = "Captured files: {}\nFailed files: 0\nHeld files: 0"
APPROVED = {
    "good month": (
        f"Bronze run {RUN} completed.\n{READ_ALL}\n{COUNTS.format(1)}\nEvery file was captured.",
        "No file is missing and no table was left out.",
        "No action is needed.",
    ),
    "files carried over": (
        f"Bronze run {RUN} completed.\n{READ_ALL}\nCaptured files: 0\nFailed files: 1\nHeld files: 0\n1 source file is not captured after the retry days.",
        "The tables built from these files may hold older data.",
        "Read the file list below.\nThen decide if the earlier capture is good enough.\nIf it is, no action is needed.",
    ),
    "table left out": (
        f"Bronze run {RUN} completed.\n{READ_ALL}\n{COUNTS.format(0)}\nThe run left out 1 table.\nIt has no source file yet.",
        LEFT_OUT,
        CHECK_TABLE,
    ),
    "missing original record": (
        f"Bronze run {RUN} completed.\nCapture records read: none.\nThe notice does not count the captured, failed or held files.\n"
        + "\n".join(NOT_COUNTED.format(name, "is missing") for name in (ORIGINAL, f"ingestion_retry_{HEX}_2", f"ingestion_retry_{HEX}_3")),
        "The counts leave out the files of every run record that could not be read.",
        UNREAD,
    ),
    "malformed record": (
        f"Bronze run {RUN} completed.\nCapture records read: ingestion_retry_{HEX}_2, ingestion_retry_{HEX}_3.\n{COUNTS.format(0)}\n"
        + NOT_COUNTED.format(ORIGINAL, "cannot be read"),
        "The counts leave out the files of every run record that could not be read.",
        UNREAD,
    ),
    "no capture root and a table left out": (
        f"Bronze run {RUN} completed.\nCapture records read: none.\nThe notice does not count the captured, failed or held files.\n"
        "The run left out 1 table.\nIt has no source file yet.",
        LEFT_OUT,
        CHECK_TABLE,
    ),
}


@pytest.mark.parametrize("state", list(STATES))
def test_every_paragraph_of_the_email_has_the_approved_wording_in_every_state(state: str) -> None:
    store = STATES[state]()
    carried(store, root="" if state.startswith("no capture root") else "raw/", skipped=EMPTY if "table" in state else None)
    message = store.published[0]["Message"]
    paragraphs = {
        title: message.split(f"{title}:\n", 1)[1].split("\n\n", 1)[0] for title in ("What happened", "What it affects", "What to do next", "Where to look")
    }
    happened, affects, next_step = APPROVED[state]
    check(paragraphs == {"What happened": happened, "What it affects": affects, "What to do next": next_step, "Where to look": WHERE}, f"{state}: {paragraphs}")
