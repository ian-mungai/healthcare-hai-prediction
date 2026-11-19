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


SUBJECT_LIMIT = 90
BODY_LIMIT = 100_000
HOSTILE_TEXT = (
    "line\nbreak",
    "tab\there",
    "carriage\rreturn",
    "bell\x07x",
    "rocket \U0001f680",
    "café",
    "x" + "界" * 60,
    "  leading space",
    "prefix\u2028What happened:\u2028FAKE",
    "prefix\u2029What happened:\u2029FAKE",
    "prefix\u0085What happened:\u0085FAKE",
    "word " * 60,
)


def subject_problems(subject: str) -> list[str]:
    """Why an SNS subject would be invalid or cut: it must be printable ASCII on one line, not start with a space and stay under the chosen limit."""
    found = []
    if not subject or subject != subject.strip() or not subject.isascii() or not subject.isprintable():
        found.append(f"not printable ASCII on one line without outer spaces: {subject!r}")
    if len(subject) > SUBJECT_LIMIT:
        found.append(f"{len(subject)} characters, over {SUBJECT_LIMIT}")
    return found


@pytest.mark.parametrize("text", HOSTILE_TEXT)
def test_a_subject_is_ascii_on_one_line_and_under_the_limit_whatever_the_text(text: str) -> None:
    check(not subject_problems(owner_notice.shorten(text)), f"{owner_notice.shorten(text)!r}")
    check(not subject_problems(owner_notice.shorten(f"Bronze run {text} left out 1 table")), "a run name with hostile text")


@pytest.mark.parametrize("outcome", training_contract.PROMOTION_OUTCOMES)
def test_every_python_subject_follows_the_subject_rule(outcome: str) -> None:
    check(not subject_problems(promotion_notice(outcome)["Subject"]), promotion_notice(outcome)["Subject"])
    long = [f"source_{index}__20261101T000000Z__abc" for index in range(5)]
    check(not subject_problems(bronze_notice(long, [{"table": "t", "reason": "r"}])["Subject"]), "the bronze subject")


def test_a_long_promotion_detail_is_cut_before_it_is_rendered_and_the_body_stays_under_the_cap() -> None:
    bucket = Bucket()
    auto_promotion.notify(bucket, TOPIC, "HAI_1", "held", "x" * 262_144, run=RESEARCH)
    body = bucket.published[0]["Message"]
    check(len(body.encode()) <= BODY_LIMIT, f"the body has {len(body.encode())} bytes")
    check(len(body.encode()) < 5_000, "the detail kept more than its 1000 characters")
    check("What to do next:" in body and "Details:" in body, "a cut detail removed a block")


def test_a_huge_bronze_list_is_cut_on_a_line_with_a_truncated_line_and_stays_under_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    files = [f"source_{index}__20261101T000000Z__" + "a" * 60 for index in range(2_000)]
    tables = [{"table": f"table_{index}", "reason": "no source object"} for index in range(2_000)]
    body = bronze_notice(files, tables)["Message"]
    monkeypatch.setattr(owner_notice, "cap_body", lambda text, limit=0: text)
    whole = bronze_notice(files, tables)["Message"].split("\n")
    lines = body.split("\n")
    check(len(whole) > len(lines) and len(body.encode()) <= BODY_LIMIT, f"the body has {len(body.encode())} bytes and {len(lines)} of {len(whole)} lines")
    check(lines[-1] == owner_notice.TRUNCATED and "run record" in lines[-1], f"the last line is {lines[-1]!r}")
    check(lines[:-2] == whole[: len(lines) - 2] and lines[-2] == "", "the cut is not on a line boundary of the full notice")
    check(len(body.encode()) > BODY_LIMIT - 400, "the cut dropped more lines than it had to")


def test_a_separator_in_a_file_or_table_name_adds_no_line_and_no_block_header() -> None:
    hostile = "prefix\u2028What happened:\u2028Where to look:\u2029FAKE\u0085end"
    sent = bronze_notice([hostile], [{"table": hostile, "reason": hostile}])
    body = sent["Message"]
    headers = [line for line in body.splitlines() if line in BLOCKS + LATER_BLOCKS]
    check(len(headers) == len(set(headers)), f"a name added a header: {headers}")
    check(not any(char in body for char in "\u0085\u2028\u2029"), "a line separator reached the body")
    check("prefix What happened: Where to look: FAKE end" in body, "the name was not kept as one line")


ROLLED_BACK_RECORD = {"promotion": {"challenger": {"model": "m2", "version": 4}, "production_version": "3"}}


@pytest.mark.parametrize("canary", [{"failed": []}, {"failed": ["within_time_limit"]}, None])
def test_a_rollback_says_it_was_reversed_whatever_the_canary_said(canary: dict[str, Any] | None) -> None:
    text = auto_promotion.notice_detail(ROLLED_BACK_RECORD, canary, "rolled_back")
    check("reversed" in text and "promotion record" in text and "Elastic Net (m2) version 4" in text, text)
    check("stays a challenger" not in text and "canary passed" not in text and "canary failed" not in text, f"a hold reason in a rollback: {text}")
    body = promotion_notice("rolled_back")["Message"]
    check("stays a challenger" not in body and "was rolled back." in body, body)
