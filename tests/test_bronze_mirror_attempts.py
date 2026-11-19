"""Each mirror attempt of a bronze run is its own create-once record, so a retry shows the final state without replacing the load receipt.

Review round 2 (Medium): ``check.json`` is the immutable receipt of the load (create-only, kept on a retry). The Glue mirror runs again on every retry of
the load phase, so its outcome cannot live in that receipt. Each attempt is written as ``lakehouse/bronze_runs/<run>/mirror_<report digest>.json``: bound to
the run, the committed metadata generation and the loader report it came from, created once (a repeat of the same check phase writes nothing new), and the
latest attempt by the report's time is the state of the mirror.
"""

from __future__ import annotations

import io
import json
from typing import Any

import pytest
from botocore.exceptions import ClientError

from scripts.lakehouse import bronze_mirror, bronze_run
from tests.support import check

RUN = "bronze_3f9a1c07"
BUCKET = "example-bucket"
SHA_A, SHA_B = "a" * 64, "b" * 64
OK = {"status": "ok", "tables": {"alpha_data": "created", "file_metadata": "created"}}
FAILED = {
    "status": "failed",
    "reason": "alpha_data failed: AccessDeniedException from Glue",
    "tables": {"alpha_data": "failed: AccessDeniedException from Glue"},
}


def report(mirror: dict[str, Any], stamp: str = "20261118T230000Z") -> dict[str, Any]:
    return {"run_at_utc": stamp, "mode": "load", "metadata_index": {"generation": "7", "path": "x", "sha256": SHA_A}, "mirror": mirror}


class Client:
    """The S3 client of the attempt records: create-only writes, a listing and reads."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts: list[dict[str, Any]] = []

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        self.puts.append(arguments)
        if arguments.get("IfNoneMatch") == "*" and arguments["Key"] in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        self.objects[arguments["Key"]] = arguments["Body"]
        return {}

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        keys = sorted(key for key in self.objects if key.startswith(arguments["Prefix"]))
        return {"Contents": [{"Key": key} for key in keys], "IsTruncated": False}

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        return {"Body": io.BytesIO(self.objects[arguments["Key"]])}


def test_an_attempt_is_bound_to_the_run_the_generation_and_the_report_it_came_from() -> None:
    attempt = bronze_mirror.attempt_record(RUN, report(OK), SHA_A)
    check(
        attempt
        == {
            "schema": 1,
            "bronze_run": RUN,
            "metadata_generation": "7",
            "report_sha256": SHA_A,
            "report_run_at_utc": "20261118T230000Z",
            "mirror": OK,
        },
        f"{attempt}",
    )
    check(bronze_mirror.attempt_key(RUN, SHA_A) == f"lakehouse/bronze_runs/{RUN}/mirror_{SHA_A}.json", bronze_mirror.attempt_key(RUN, SHA_A))


@pytest.mark.parametrize(
    "broken",
    [
        {"metadata_index": None},
        {"metadata_index": {"generation": ""}},
        {"run_at_utc": "yesterday"},
        {"run_at_utc": 5},
        {"mirror": {"status": "great", "tables": {}}},
    ],
)
def test_a_report_that_cannot_bind_an_attempt_gives_no_record(broken: dict[str, Any]) -> None:
    with pytest.raises(bronze_mirror.AttemptError):
        bronze_mirror.attempt_record(RUN, report(OK) | broken, SHA_A)


def test_an_attempt_is_created_once_and_a_repeat_of_the_same_report_writes_nothing_new() -> None:
    client = Client()
    attempt = bronze_mirror.attempt_record(RUN, report(OK), SHA_A)
    check(bronze_run.store_mirror_attempt(client, BUCKET, attempt) == "written", "the first write did not happen")
    check(client.puts[0]["IfNoneMatch"] == "*" and client.puts[0]["Key"] == bronze_mirror.attempt_key(RUN, SHA_A), f"{client.puts[0]}")
    check(
        bronze_run.store_mirror_attempt(client, BUCKET, attempt) == "kept" and len(client.objects) == 1, "a repeat made a second record or replaced the first"
    )


def test_the_latest_attempt_is_the_state_after_a_failure_then_a_repair_and_after_a_success_then_a_failure() -> None:
    for first, second in ((FAILED, OK), (OK, FAILED)):
        client = Client()
        bronze_run.store_mirror_attempt(client, BUCKET, bronze_mirror.attempt_record(RUN, report(first, "20261118T230000Z"), SHA_A))
        bronze_run.store_mirror_attempt(client, BUCKET, bronze_mirror.attempt_record(RUN, report(second, "20261118T231500Z"), SHA_B))
        attempts = bronze_mirror.attempts(client, BUCKET, RUN)
        check([item["mirror"]["status"] for item in attempts] == [first["status"], second["status"]], f"{attempts}")
        latest = bronze_mirror.latest_attempt(client, BUCKET, RUN)
        check(latest is not None and latest["mirror"] == second, f"the latest attempt is not the retry: {latest}")


def test_a_run_with_no_attempt_has_no_latest_one_and_another_runs_attempts_are_not_read() -> None:
    client = Client()
    check(bronze_mirror.latest_attempt(client, BUCKET, RUN) is None, "an empty run has an attempt")
    bronze_run.store_mirror_attempt(client, BUCKET, bronze_mirror.attempt_record("bronze_aaaaaaaa", report(OK), SHA_A))
    check(bronze_mirror.latest_attempt(client, BUCKET, RUN) is None, "another run's attempt was read")


def test_an_attempt_record_that_is_not_the_runs_own_is_refused_when_read() -> None:
    client = Client()
    other = bronze_mirror.attempt_record("bronze_aaaaaaaa", report(OK), SHA_A)
    client.objects[bronze_mirror.attempt_key(RUN, SHA_A)] = json.dumps(other).encode()
    with pytest.raises(bronze_mirror.AttemptError, match="another run"):
        bronze_mirror.attempts(client, BUCKET, RUN)


def test_the_show_command_prints_each_attempt_with_its_cause_and_names_the_latest(capsys: pytest.CaptureFixture[str]) -> None:
    client = Client()
    bronze_run.store_mirror_attempt(client, BUCKET, bronze_mirror.attempt_record(RUN, report(FAILED, "20261118T230000Z"), SHA_A))
    bronze_run.store_mirror_attempt(client, BUCKET, bronze_mirror.attempt_record(RUN, report(OK, "20261118T231500Z"), SHA_B))
    check(bronze_mirror.main(["show", "--run", RUN, "--bucket", BUCKET], client=client) == 0, "the show command failed")
    shown = capsys.readouterr().out
    check("AccessDeniedException" in shown and shown.index("20261118T230000Z") < shown.index("20261118T231500Z"), shown)
    check(shown.strip().splitlines()[-1] == "latest: ok", shown)
    stale = Client()
    bronze_run.store_mirror_attempt(stale, BUCKET, bronze_mirror.attempt_record(RUN, report(FAILED), SHA_A))
    check(bronze_mirror.main(["show", "--run", RUN, "--bucket", BUCKET], client=stale) == 1, "a latest attempt that failed exits 0")
