"""The mirror attempts of a bronze run are a numbered chain that the bucket orders, joined to the immutable load receipt.

``check.json`` is the receipt of the load (create-only, kept on a retry). The Glue mirror runs again on every retry of the load phase, so each attempt
is its own create-once record ``lakehouse/bronze_runs/<run>/mirror_<NNN>.json``. The number is the order: the check phase takes the first free number with a
conditional create, so two attempts can never share one and no clock, no digest and no report time decides which came last.
The report time stays in the record for display.
``acceptance`` joins the chain to the receipt: the first record must be the receipt's own report, every record must carry the receipt's metadata generation, the
numbers must run from 001 without a gap and no report may appear twice. Anything else is ``unavailable``, never a pass that another record lends.
"""

from __future__ import annotations

import hashlib
import io
import json
from typing import Any
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from scripts.lakehouse import bronze_mirror, bronze_run
from tests.support import check

RUN = "bronze_3f9a1c07"
BUCKET = "example-bucket"
OK = {"status": "ok", "tables": {"alpha_data": "created", "file_metadata": "created"}}
FAILED = {
    "status": "failed",
    "reason": "alpha_data failed: AccessDeniedException from Glue",
    "tables": {"alpha_data": "failed: AccessDeniedException from Glue"},
}
RECEIPT_KEY = f"lakehouse/bronze_runs/{RUN}/check.json"


def report(mirror: dict[str, Any], stamp: str = "20261118T230000Z", generation: str = "7") -> tuple[dict[str, Any], str]:
    """A loader report and its real SHA-256, as the check phase computes it from the report's bytes."""
    document = {"run_at_utc": stamp, "mode": "load", "metadata_index": {"generation": generation, "path": "x", "sha256": "a" * 64}, "mirror": mirror}
    return document, hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()


class Client:
    """The S3 client of the records: create-only writes, reads (the task role reads an absent key as 403 or 404: both are missing) and the owner's listing."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts: list[dict[str, Any]] = []
        self.refuse_put: set[str] = set()

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        self.puts.append(arguments)
        if any(part in arguments["Key"] for part in self.refuse_put):
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no grant"}}, "PutObject")
        if arguments.get("IfNoneMatch") == "*" and arguments["Key"] in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        self.objects[arguments["Key"]] = arguments["Body"]
        return {}

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        keys = sorted(key for key in self.objects if key.startswith(arguments["Prefix"]))
        return {"Contents": [{"Key": key} for key in keys], "IsTruncated": False}

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        if arguments["Key"] not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "no such key"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[arguments["Key"]])}


def receipt(client: Client, document: dict[str, Any], digest: str, run: str = RUN) -> None:
    client.objects[f"lakehouse/bronze_runs/{run}/check.json"] = json.dumps(
        {"run_id": run, "report_sha256": digest, "metadata_generation": document["metadata_index"]["generation"]}
    ).encode()


def record(client: Client, mirror: dict[str, Any], stamp: str = "20261118T230000Z", generation: str = "7") -> tuple[tuple[str, str], str]:
    document, digest = report(mirror, stamp, generation)
    outcome, key = bronze_run.store_mirror_attempt(client, BUCKET, RUN, document, digest)
    return (outcome, key), digest


def test_an_attempt_is_bound_to_the_run_the_generation_the_report_and_its_number() -> None:
    document, digest = report(OK)
    attempt = bronze_mirror.attempt_record(RUN, document, digest, 1)
    check(
        attempt
        == {
            "schema": 2,
            "bronze_run": RUN,
            "sequence": 1,
            "metadata_generation": "7",
            "report_sha256": digest,
            "report_run_at_utc": "20261118T230000Z",
            "mirror": OK,
        },
        f"{attempt}",
    )
    check(bronze_mirror.attempt_key(RUN, 1) == f"lakehouse/bronze_runs/{RUN}/mirror_001.json", bronze_mirror.attempt_key(RUN, 1))


@pytest.mark.parametrize(
    "broken",
    [
        {"metadata_index": None},
        {"metadata_index": {"generation": ""}},
        {"run_at_utc": "yesterday"},
        {"run_at_utc": "99999999T999999Z"},
        {"run_at_utc": 5},
        {"mirror": {"status": "great", "tables": {}}},
    ],
)
def test_a_report_that_cannot_bind_an_attempt_gives_no_record(broken: dict[str, Any]) -> None:
    document, digest = report(OK)
    with pytest.raises(bronze_mirror.AttemptError):
        bronze_mirror.attempt_record(RUN, document | broken, digest, 1)


def test_the_bucket_numbers_the_attempts_and_the_same_report_is_never_recorded_twice() -> None:
    client = Client()
    first, digest = record(client, OK)
    check(first == ("written", bronze_mirror.attempt_key(RUN, 1)) and client.puts[0]["IfNoneMatch"] == "*", f"{first} {client.puts[0]}")
    again, _ = record(client, OK)
    check(again == ("kept", bronze_mirror.attempt_key(RUN, 1)) and len(client.objects) == 1, f"a repeat of the same report made another record: {again}")
    second, other = record(client, FAILED, "20261118T231500Z")
    check(second == ("written", bronze_mirror.attempt_key(RUN, 2)) and other != digest, f"{second}")
    check(json.loads(client.objects[bronze_mirror.attempt_key(RUN, 2)])["sequence"] == 2, "the second record does not carry its number")


def test_a_lost_race_for_a_number_takes_the_next_free_one_and_never_replaces() -> None:
    """Another attempt creates ``mirror_001`` between this attempt's read and its write: the create is refused and the attempt reads the key again."""
    client = Client()
    document, digest = report(FAILED, "20261118T231500Z")
    rival, _ = report(OK)
    reads = {"count": 0}
    original = client.get_object

    def racing(**arguments: Any) -> dict[str, Any]:
        reads["count"] += 1
        if reads["count"] == 1:
            client.objects[bronze_mirror.attempt_key(RUN, 1)] = json.dumps(bronze_mirror.attempt_record(RUN, rival, "c" * 64, 1)).encode()
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no such key"}}, "GetObject")
        return original(**arguments)

    # The bucket answers 403 for the key that does not exist yet, then the rival's record exists.
    with mock.patch.object(client, "get_object", racing):
        outcome, key = bronze_run.store_mirror_attempt(client, BUCKET, RUN, document, digest)
    check((outcome, key) == ("written", bronze_mirror.attempt_key(RUN, 2)), f"{outcome} {key}")
    check(json.loads(client.objects[bronze_mirror.attempt_key(RUN, 1)])["report_sha256"] == "c" * 64, "the rival's record was replaced")


def test_the_latest_attempt_is_the_last_number_whatever_the_report_times_say() -> None:
    """Reversed clock, equal times, with real digests: the number decides, never the time and never the digest."""
    for first_stamp, second_stamp in (("20261118T231500Z", "20261118T230000Z"), ("20261118T230000Z", "20261118T230000Z")):
        for first, second in ((OK, FAILED), (FAILED, OK)):
            client = Client()
            (_, key_one), _ = record(client, first, first_stamp)
            (_, key_two), _ = record(client, second, second_stamp)
            found = bronze_mirror.attempts(client, BUCKET, RUN)
            check(
                [item["sequence"] for item in found] == [1, 2] and [item["mirror"] for item in found] == [first, second],
                f"{first_stamp} {second_stamp}: {found}",
            )
            latest = bronze_mirror.latest_attempt(client, BUCKET, RUN)
            check(latest is not None and latest["mirror"] == second and key_one != key_two, f"the latest attempt is not the last number: {latest}")


def test_a_gap_in_the_numbers_and_a_report_recorded_twice_are_refused_not_ordered_by_digest() -> None:
    client = Client()
    record(client, OK)
    record(client, FAILED, "20261118T231500Z")
    document, digest = report(OK, "20261118T233000Z")
    client.objects[bronze_mirror.attempt_key(RUN, 4)] = json.dumps(bronze_mirror.attempt_record(RUN, document, digest, 4)).encode()
    with pytest.raises(bronze_mirror.AttemptError, match="mirror_003.json is missing"):
        bronze_mirror.attempts(client, BUCKET, RUN)
    twice = Client()
    record(twice, OK)
    first = json.loads(twice.objects[bronze_mirror.attempt_key(RUN, 1)])
    twice.objects[bronze_mirror.attempt_key(RUN, 2)] = json.dumps(first | {"sequence": 2}).encode()
    with pytest.raises(bronze_mirror.AttemptError, match="same report"):
        bronze_mirror.attempts(twice, BUCKET, RUN)


@pytest.mark.parametrize(
    "change",
    [
        {"schema": 1},
        {"bronze_run": "bronze_aaaaaaaa"},
        {"sequence": 2},
        {"metadata_generation": ""},
        {"metadata_generation": 7},
        {"report_sha256": "short"},
        {"report_run_at_utc": "99999999T999999Z"},
        {"mirror": {"status": "great", "tables": {}}},
        {"extra": 1},
    ],
)
def test_a_stored_record_is_checked_in_full_and_a_fabricated_one_is_refused(change: dict[str, Any]) -> None:
    client = Client()
    record(client, OK)
    key = bronze_mirror.attempt_key(RUN, 1)
    client.objects[key] = json.dumps(json.loads(client.objects[key]) | change).encode()
    with pytest.raises(bronze_mirror.AttemptError):
        bronze_mirror.attempts(client, BUCKET, RUN)


def test_a_record_that_misses_a_key_is_refused() -> None:
    client = Client()
    record(client, OK)
    key = bronze_mirror.attempt_key(RUN, 1)
    stored = json.loads(client.objects[key])
    del stored["report_run_at_utc"]
    client.objects[key] = json.dumps(stored).encode()
    with pytest.raises(bronze_mirror.AttemptError):
        bronze_mirror.attempts(client, BUCKET, RUN)


def test_acceptance_passes_only_when_the_chain_starts_at_the_receipts_own_report() -> None:
    client = Client()
    document, digest = report(OK)
    receipt(client, document, digest)
    record(client, OK)
    record(client, FAILED, "20261118T231500Z")
    verdict = bronze_mirror.acceptance(client, BUCKET, RUN)
    check(verdict["state"] == "failed" and "AccessDeniedException" in verdict["reason"], f"{verdict}")
    repaired = Client()
    receipt(repaired, document, digest)
    record(repaired, OK)
    record(repaired, OK, "20261118T231500Z")
    check(bronze_mirror.acceptance(repaired, BUCKET, RUN)["state"] == "ok", "a recorded repair is not accepted")


def test_acceptance_is_unavailable_with_no_receipt_no_record_or_the_first_load_unrecorded() -> None:
    document, digest = report(OK)
    nothing = Client()
    check(bronze_mirror.acceptance(nothing, BUCKET, RUN)["state"] == "unavailable", "a run with no receipt passed")
    no_record = Client()
    receipt(no_record, document, digest)
    verdict = bronze_mirror.acceptance(no_record, BUCKET, RUN)
    check(verdict["state"] == "unavailable" and bronze_mirror.attempt_key(RUN, 1) in verdict["reason"], f"{verdict}")
    # The first check could not write its record (before the IAM apply); a later retry then wrote record 001 with its own, later report.
    later, later_digest = report(OK, "20261118T231500Z")
    skipped = Client()
    receipt(skipped, document, digest)
    bronze_run.store_mirror_attempt(skipped, BUCKET, RUN, later, later_digest)
    verdict = bronze_mirror.acceptance(skipped, BUCKET, RUN)
    check(verdict["state"] == "unavailable" and "first" in verdict["reason"], f"a record of another report passed for the receipt's load: {verdict}")


def test_acceptance_refuses_a_record_whose_generation_is_not_the_receipts() -> None:
    client = Client()
    document, digest = report(OK)
    receipt(client, document, digest)
    record(client, OK)
    record(client, OK, "20261118T231500Z", generation="invented")
    verdict = bronze_mirror.acceptance(client, BUCKET, RUN)
    check(verdict["state"] == "unavailable" and "generation" in verdict["reason"], f"{verdict}")


def test_a_fabricated_future_dated_ok_record_does_not_outrank_a_genuine_failure() -> None:
    client = Client()
    document, digest = report(OK)
    receipt(client, document, digest)
    record(client, OK)
    record(client, FAILED, "20261118T231500Z")
    forged, forged_digest = report(OK, "20261118T231500Z", generation="7")
    client.objects[bronze_mirror.attempt_key(RUN, 4)] = json.dumps(bronze_mirror.attempt_record(RUN, forged, forged_digest, 4)).encode()
    verdict_or_error: Any
    try:
        verdict_or_error = bronze_mirror.acceptance(client, BUCKET, RUN)["state"]
    except bronze_mirror.AttemptError:
        verdict_or_error = "refused"
    check(verdict_or_error != "ok", "a record that skips a number outranked the genuine failure")


def test_the_show_command_prints_each_attempt_in_number_order_with_its_cause_and_the_verdict(capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    document, digest = report(OK, "20261118T231500Z")
    receipt(client, document, digest)
    record(client, OK, "20261118T231500Z")
    record(client, FAILED, "20261118T230000Z")
    check(bronze_mirror.main(["show", "--run", RUN, "--bucket", BUCKET], client=client) == 1, "a failed latest attempt exits 0")
    shown = capsys.readouterr().out.splitlines()
    check(shown[0].startswith("001 ") and shown[1].startswith("002 ") and "AccessDeniedException" in shown[1], shown)
    check(shown[-1].startswith("acceptance: failed"), shown)
    clean = Client()
    receipt(clean, document, digest)
    record(clean, OK, "20261118T231500Z")
    check(bronze_mirror.main(["show", "--run", RUN, "--bucket", BUCKET], client=clean) == 0, "a passing verdict exits 1")
    unavailable = Client()
    check(bronze_mirror.main(["show", "--run", RUN, "--bucket", BUCKET], client=unavailable) == 1, "an unavailable verdict exits 0")
    check("acceptance: unavailable" in capsys.readouterr().out, "the unavailable verdict is not printed")


def test_a_denied_write_names_the_exact_key_the_report_and_the_generation() -> None:
    client = Client()
    client.refuse_put.add("mirror_")
    document, digest = report(OK)
    with pytest.raises(bronze_run.AttemptWriteError) as caught:
        bronze_run.store_mirror_attempt(client, BUCKET, RUN, document, digest)
    text = str(caught.value)
    check(bronze_mirror.attempt_key(RUN, 1) in text and digest[:12] in text and "generation 7" in text and "AccessDenied" in text, text)


def test_a_number_that_exists_but_cannot_be_read_is_named_and_not_retried_forever() -> None:
    """The role may create the key but not read it (the write grant came before the read grant): the create is refused as existing and the read says missing."""
    client = Client()
    existing, _ = report(OK)
    client.objects[bronze_mirror.attempt_key(RUN, 1)] = json.dumps(bronze_mirror.attempt_record(RUN, existing, "c" * 64, 1)).encode()
    document, digest = report(FAILED, "20261118T231500Z")

    def denied(**arguments: Any) -> dict[str, Any]:
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no read grant"}}, "GetObject")

    with mock.patch.object(client, "get_object", denied), pytest.raises(bronze_run.AttemptWriteError) as caught:
        bronze_run.store_mirror_attempt(client, BUCKET, RUN, document, digest)
    text = str(caught.value)
    check(bronze_mirror.attempt_key(RUN, 1) in text and "cannot be read" in text and digest[:12] in text, text)
    check(len(client.puts) == 1, f"the create was retried {len(client.puts)} times")


def test_an_unavailable_outcome_names_its_cause_and_never_passes_the_acceptance() -> None:
    unavailable = {
        "status": "unavailable",
        "reason": "alpha_data failed: AccessDeniedException from Glue",
        "tables": {"alpha_data": "failed: AccessDeniedException from Glue"},
    }
    check(
        bronze_mirror.outcome_shaped(unavailable) and not bronze_mirror.outcome_shaped({**unavailable, "reason": None}), "the shape of an unavailable outcome"
    )
    check(not bronze_mirror.outcome_shaped({"status": "unavailable", "tables": {}}), "an unavailable outcome without a cause was accepted")
    client = Client()
    document, digest = report(unavailable)
    receipt(client, document, digest)
    record(client, unavailable)
    verdict = bronze_mirror.acceptance(client, BUCKET, RUN)
    check(verdict["state"] == "unavailable" and "AccessDeniedException" in verdict["reason"], f"{verdict}")


@pytest.mark.parametrize("code", ["AccessDenied", "403", "NoSuchKey", "404"])
def test_a_key_that_answers_as_absent_is_no_history(code: str) -> None:
    """The task role cannot list its folder, so an absent key and a key it may not read answer alike: both read as no history."""

    class Client403(Client):
        def get_object(self, **arguments: Any) -> dict[str, Any]:
            raise ClientError({"Error": {"Code": code, "Message": "x"}}, "GetObject")

    check(bronze_run.read_attempt(Client403(), BUCKET, bronze_mirror.attempt_key(RUN, 1)) is None, code)


@pytest.mark.parametrize(("body", "code"), [(b"[]", None), (b"not json", None), (b"{}", "InternalError"), (b"{}", "SlowDown")])
def test_an_unreadable_history_or_a_stored_body_that_is_not_a_record_is_history_never_absence(body: bytes, code: str | None) -> None:
    class Odd(Client):
        def get_object(self, **arguments: Any) -> dict[str, Any]:
            if code:
                raise ClientError({"Error": {"Code": code, "Message": "x"}}, "GetObject")
            return {"Body": io.BytesIO(body)}

    with pytest.raises(bronze_run.AttemptWriteError) as caught:
        bronze_run.read_attempt(Odd(), BUCKET, bronze_mirror.attempt_key(RUN, 1))
    check(caught.value.history and not caught.value.denied and bronze_mirror.attempt_key(RUN, 1) in str(caught.value), f"{caught.value}")


def test_a_refused_write_after_a_recorded_attempt_carries_the_history_and_without_one_it_does_not() -> None:
    client = Client()
    record(client, OK)
    client.refuse_put.add("mirror_")
    document, digest = report(FAILED, "20261118T231500Z")
    with pytest.raises(bronze_run.AttemptWriteError) as caught:
        bronze_run.store_mirror_attempt(client, BUCKET, RUN, document, digest)
    check(caught.value.denied and caught.value.history, f"denied {caught.value.denied} history {caught.value.history}")
    fresh = Client()
    fresh.refuse_put.add("mirror_")
    with pytest.raises(bronze_run.AttemptWriteError) as bare:
        bronze_run.store_mirror_attempt(fresh, BUCKET, RUN, document, digest)
    check(bare.value.denied and not bare.value.history, f"denied {bare.value.denied} history {bare.value.history}")
