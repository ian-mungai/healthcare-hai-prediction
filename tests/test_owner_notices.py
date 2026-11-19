"""The two owner notices that Python code sends to the alerts topic read as plain text, in one shape.

Each case sends the notice through its real entry point (the bronze check phase and the promotion chain) to a stand-in topic client and judges the subject
and the message that the client received, the way the email shows them.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfoNotFoundError

import pytest

from orchestration import owner_notice, training_contract
from scripts.lakehouse import bronze_run
from scripts.training import auto_promotion
from tests.support import check
from tests.test_bronze_carried_over import Bucket as RunBucket
from tests.test_bronze_carried_over import run_record

BLOCKS = ("What happened:", "When:", "What it affects:", "What to do next:")
LATER_BLOCKS = ("Where to look:", "Details:")
TIME_LINE = re.compile(r"^(\d{1,2}:\d{2} [AP]M) (P[SD]T), ([A-Z][a-z]{2} \d{1,2} \d{4}) \[(\d{2}:\d{2}) UTC\]$")
BARE_KEY = re.compile(r"(?<![\w(])(m[1-6]|HAI_[1-6])(?![\w])")
TOPIC = "arn:aws:sns:us-west-2:" + "1" * 12 + ":alerts"
BRONZE = "bronze_b60c616e"
INGESTION = "ingestion_b60c616e"
TABLE = "hcai_util_workbook_sheet_rows"
RESEARCH = "research_gradient_boosting_0a1b2c3d"


def problems_with(subject: str, body: str) -> list[str]:
    """Everything wrong with a notice: the subject length, the block order, the time line, bare keys and template debris."""
    found: list[str] = []
    if len(subject) > 100:
        found.append(f"the subject has {len(subject)} characters, and SNS refuses more than 100")
    if not subject.strip() or "\n" in subject:
        found.append("the subject is empty or has a line break")
    lines = body.split("\n")
    positions = [lines.index(name) if name in lines else -1 for name in BLOCKS]
    if -1 in positions or positions != sorted(positions):
        found.append(f"the blocks {', '.join(BLOCKS)} are not all there in this order: {positions}")
    later = [lines.index(name) for name in LATER_BLOCKS if name in lines]
    if later != sorted(later) or (later and later[0] < max(positions)):
        found.append("Where to look and Details do not come after the four blocks, in that order")
    if "\n\n\n" in body or body != body.strip() + "":
        found.append("the body has a doubled blank line or spaces around it")
    for name in BLOCKS + LATER_BLOCKS:
        if name in lines:
            at = lines.index(name)
            following = lines[at + 1] if at + 1 < len(lines) else ""
            if not following.strip():
                found.append(f"the block {name} is empty")
            if at > 0 and lines[at - 1] != "":
                found.append(f"the block {name} has no blank line before it")
    when = lines[lines.index("When:") + 1] if "When:" in lines else ""
    if not TIME_LINE.match(when):
        found.append(f"the time line is not in the form '10:13 AM PST, Nov 19 2026 [18:13 UTC]': {when!r}")
    for debris in ("<no value>", "None", "{", "}", "'", "—", "–", ";"):
        if debris in subject + body:
            found.append(f"the text holds {debris!r}")
    for match in BARE_KEY.finditer(subject + "\n" + body):
        found.append(f"the key {match.group(1)} stands without its name")
    return found


def stamped_within_a_minute(body: str) -> bool:
    """The UTC time in the body is the time of the send, not a stale or invented one."""
    lines = body.split("\n")
    found = TIME_LINE.match(lines[lines.index("When:") + 1])
    if found is None:
        return False
    today = datetime.now(UTC)
    shown = datetime.strptime(f"{today:%Y-%m-%d} {found.group(4)}", "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
    return abs(shown - today) < timedelta(minutes=2) or abs(shown - today) > timedelta(hours=23, minutes=58)


class Bucket:
    """A stand-in bucket and topic: it keeps the first write of each key and every notice published."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.published: list[dict[str, str]] = []

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        self.objects[arguments["Key"]] = arguments["Body"]
        return {}

    def publish(self, **arguments: str) -> dict[str, str]:
        self.published.append(arguments)
        return {"MessageId": "m-1"}

    def list_object_versions(self, **arguments: str) -> dict[str, Any]:
        return {"Versions": []}


def bronze_notice(files: list[str], skipped: list[dict[str, str]]) -> dict[str, str]:
    """The one notice that the check phase of a bronze run sends for the carried-over files and the skipped tables."""
    bucket = RunBucket({f"raw/ingestion_runs/{INGESTION}/run.json": run_record(files)})
    bronze_run.record_carried_over(bucket, "bucket", BRONZE, "raw/", topic=TOPIC, notify=bucket, skipped=skipped)
    check(len(bucket.published) == 1, f"{len(bucket.published)} notices went out")
    return bucket.published[0]


def promotion_notice(outcome: str, **arguments: Any) -> dict[str, str]:
    """The notice of one promotion outcome for the first infection type."""
    bucket = Bucket()
    record = {"promotion": {"challenger": {"model": "m2", "version": 4}, "production_version": "3"}}
    detail = auto_promotion.notice_detail(record, {"failed": ["within_time_limit"]}, outcome)
    auto_promotion.notify(bucket, TOPIC, "HAI_1", outcome, detail, **arguments)
    return bucket.published[0]


def test_a_skipped_table_notice_has_the_shape_and_names_the_table_and_the_run() -> None:
    sent = bronze_notice([], [{"table": TABLE, "reason": "no source object"}])
    subject, body = sent["Subject"], sent["Message"]
    check(not problems_with(subject, body), f"{problems_with(subject, body)}\n{subject}\n{body}")
    check(subject.startswith("Bronze run"), f"the subject does not say what happened to what: {subject}")
    check(TABLE in body and "no source object" in body and BRONZE in body, f"the table or the run is missing:\n{body}")
    check(INGESTION in body, f"the ingestion run is missing:\n{body}")
    check(f"lakehouse/bronze_runs/{BRONZE}/carried_over.json" in body, f"the run record is not named:\n{body}")
    check(stamped_within_a_minute(body), f"the time is not the time of the send:\n{body}")
    check("Files carried over" not in body, f"the notice names an absent list:\n{body}")


def test_a_carried_over_notice_names_every_file_and_counts_them() -> None:
    files = [f"source_{index}__20261101T000000Z__abc" for index in range(150)]
    sent = bronze_notice(files, [])
    subject, body = sent["Subject"], sent["Message"]
    check(not problems_with(subject, body), f"{problems_with(subject, body)}\n{subject}")
    check("150" in subject and all(name in body for name in files), "the count or a file name is missing")
    check("Tables skipped" not in body and "skipped" not in subject, f"the notice names an absent list:\n{subject}")


def test_a_notice_of_both_kinds_says_both_in_the_subject_and_the_body() -> None:
    sent = bronze_notice(["a__1__x"], [{"table": TABLE, "reason": "no source object"}])
    subject, body = sent["Subject"], sent["Message"]
    check(not problems_with(subject, body), f"{problems_with(subject, body)}\n{subject}")
    check("1 file" in subject and "1 table" in subject, subject)
    check("a__1__x" in body and TABLE in body, body)


@pytest.mark.parametrize("outcome", training_contract.PROMOTION_OUTCOMES)
def test_every_promotion_notice_has_the_shape_and_the_names_of_the_type(outcome: str) -> None:
    sent = promotion_notice(outcome, run=RESEARCH)
    subject, body = sent["Subject"], sent["Message"]
    check(sent["TopicArn"] == TOPIC, "the topic changed")
    check(not problems_with(subject, body), f"{problems_with(subject, body)}\n{subject}\n{body}")
    check("CLABSI (HAI_1)" in subject and "CLABSI (HAI_1)" in body, f"the type has no name beside its key:\n{subject}\n{body}")
    check(RESEARCH in body and "Elastic Net (m2)" in body, f"the research run or the model name is missing:\n{body}")
    check(stamped_within_a_minute(body), f"the time is not the time of the send:\n{body}")


def test_the_subjects_of_the_four_outcomes_differ_and_say_what_happened() -> None:
    subjects = {outcome: promotion_notice(outcome)["Subject"] for outcome in training_contract.PROMOTION_OUTCOMES}
    check(len(set(subjects.values())) == 4, f"{subjects}")
    check("not rolled out" in subjects["promoted_not_rolled_out"] and "rolled back" in subjects["rolled_back"], f"{subjects}")
    check("held" in subjects["held"] and "rolled" not in subjects["promoted"], f"{subjects}")


def test_a_promotion_notice_without_a_run_leaves_out_where_to_look() -> None:
    body = promotion_notice("promoted")["Message"]
    check("Where to look:" not in body and "None" not in body and "Research run" not in body, body)
    check(not problems_with("Model promoted", body), f"{problems_with('Model promoted', body)}\n{body}")


def test_the_detail_of_a_challenger_names_the_model_beside_its_key() -> None:
    record = {"promotion": {"challenger": {"model": "m2", "version": 4}, "production_version": "3"}}
    text = auto_promotion.notice_detail(record, {"failed": ["within_time_limit"]}, "held")
    check("Elastic Net (m2) version 4" in text and not BARE_KEY.search(text), text)


MOMENTS = (
    (datetime(2026, 11, 19, 18, 13, tzinfo=UTC), "10:13 AM PST, Nov 19 2026 [18:13 UTC]"),
    (datetime(2026, 7, 1, 7, 5, tzinfo=UTC), "12:05 AM PDT, Jul 1 2026 [07:05 UTC]"),
    (datetime(2026, 3, 8, 9, 59, tzinfo=UTC), "1:59 AM PST, Mar 8 2026 [09:59 UTC]"),
    (datetime(2026, 3, 8, 10, 0, tzinfo=UTC), "3:00 AM PDT, Mar 8 2026 [10:00 UTC]"),
    (datetime(2026, 11, 1, 8, 59, tzinfo=UTC), "1:59 AM PDT, Nov 1 2026 [08:59 UTC]"),
    (datetime(2026, 11, 1, 9, 0, tzinfo=UTC), "1:00 AM PST, Nov 1 2026 [09:00 UTC]"),
    (datetime(2026, 11, 19, 20, 0, tzinfo=UTC), "12:00 PM PST, Nov 19 2026 [20:00 UTC]"),
)


@pytest.mark.parametrize(("moment", "expected"), MOMENTS)
def test_the_time_line_names_the_los_angeles_clock_and_utc(moment: datetime, expected: str) -> None:
    check(owner_notice.when_text(moment) == expected, f"{owner_notice.when_text(moment)} != {expected}")


@pytest.mark.parametrize(("moment", "expected"), MOMENTS)
def test_a_machine_without_the_time_zone_database_reads_the_same_clock(monkeypatch: pytest.MonkeyPatch, moment: datetime, expected: str) -> None:
    def missing(name: str) -> Any:
        raise ZoneInfoNotFoundError(name)

    monkeypatch.setattr(owner_notice, "ZoneInfo", missing)
    check(owner_notice.when_text(moment) == expected, f"{owner_notice.when_text(moment)} != {expected}")


def test_a_subject_over_the_limit_is_cut_at_a_word_and_a_notice_without_one_is_refused() -> None:
    long = "Bronze run " + "word " * 40
    cut = owner_notice.shorten(long)
    check(len(cut) <= 100 and cut.endswith("...") and "\n" not in cut, cut)
    check(owner_notice.shorten("one\nline  here") == "one line here", "line breaks stay in a subject")
