"""The bronze check phase writes the run record that the lineage and the catalog read: ``lakehouse/bronze_runs/<run>/check.json``, once."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from orchestration import raw_lineage
from orchestration.ecs_lineage import LineageRefused
from scripts.lakehouse import bronze_mirror, bronze_run
from tests.support import check

RUN = "bronze_3f9a1c07"
BUCKET = "example-bucket"
SHA = "a" * 64
REPORT: dict[str, Any] = {"mode": "load", "passed": True, "tables": ["alpha_data", "beta_data"], "snapshots": {"alpha_data": 41, "beta_data": 42}}
REPORT |= {"metadata_index": {"generation": "7", "path": "x", "sha256": SHA}}
INVENTORY = {
    "version": 1,
    "tables": ["alpha_data", "beta_data"],
    "manifests": [],
    "documents": [],
    "objects": [
        ["alpha_data", "raw/a/1.csv", "v1", SHA, 10],
        ["beta_data", "raw/b/2.csv", "v2", "b" * 64, 20],
        ["gamma_data", "raw/c/3.csv", "v3", "c" * 64, 30],
    ],
}


def job(target: str = "real") -> bronze_run.Job:
    return bronze_run.Job(RUN, Path("/workspace"), BUCKET, ("alpha_data", "beta_data"), target)


SKIPPED = [{"table": "gamma_data", "reason": "no source object"}]


def record(**changes: Any) -> dict[str, Any]:
    report = REPORT | changes.pop("report", {})
    body = json.dumps(report, sort_keys=True).encode()
    return bronze_run.run_record(job(), report, body, changes.pop("inventory", INVENTORY), changes.pop("skipped", []))


def test_the_record_has_the_keys_the_lineage_reader_demands_and_names_the_loaded_objects() -> None:
    document = record()
    objects, snapshots = raw_lineage.loaded_objects(document, "check.json", RUN)
    check(snapshots == {"alpha_data": 41, "beta_data": 42}, f"{snapshots}")
    check([item["key"] for item in objects] == ["raw/a/1.csv", "raw/b/2.csv"], f"an object of an unrequested table is named: {objects}")
    check(all(item["bucket"] == BUCKET for item in objects), "an object does not name the bucket")
    body = json.dumps(REPORT, sort_keys=True).encode()
    check(document["report_sha256"] == hashlib.sha256(body).hexdigest() and document["metadata_generation"] == "7", f"{document}")


@pytest.mark.parametrize(
    "changes",
    [
        {"report": {"snapshots": {"alpha_data": 41}}},
        {"report": {"snapshots": {"alpha_data": 41, "beta_data": 0}}},
        {"report": {"snapshots": {"alpha_data": 41, "beta_data": True}}},
        {"report": {"metadata_index": None}},
        {"inventory": {**INVENTORY, "objects": []}},
    ],
)
def test_a_report_without_its_snapshots_or_generation_or_an_inventory_without_objects_gives_no_record(changes: dict[str, Any]) -> None:
    with pytest.raises(bronze_run.StepError):
        record(**changes)


def test_the_record_a_reader_refuses_never_leaves_the_phase() -> None:
    document = record()
    document["objects"][0]["sha256"] = "short"
    with pytest.raises(LineageRefused):
        raw_lineage.loaded_objects(document, "check.json", RUN)


class Client:
    def __init__(self, existing: bytes | None = None) -> None:
        self.existing, self.puts = existing, []

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        if self.existing is not None:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        self.puts.append(arguments)
        return {}


def test_the_record_is_written_once_and_a_retry_keeps_the_first() -> None:
    client = Client()
    check(bronze_run.store_run_record(client, BUCKET, RUN, record()) == "written", "the first write did not happen")
    put = client.puts[0]
    check(put["Key"] == f"lakehouse/bronze_runs/{RUN}/check.json" and put["IfNoneMatch"] == "*", f"{put}")
    check(json.loads(put["Body"])["run_id"] == RUN, "the stored body is not the record")
    again = Client(existing=b"{}")
    check(bronze_run.store_run_record(again, BUCKET, RUN, record()) == "kept", "a retry replaced the first record")


def test_the_record_names_the_tables_the_stage_left_out_with_their_reason_and_the_lineage_reader_accepts_it() -> None:
    document = record(skipped=SKIPPED)
    check(document["skipped_tables"] == SKIPPED, f"{document.get('skipped_tables')}")
    check(record()["skipped_tables"] == [], "a run with no report has no skipped table")
    raw_lineage.loaded_objects(document, "check.json", RUN)


def test_a_record_of_the_older_shape_without_skipped_tables_is_refused_with_a_message_that_says_why() -> None:
    document = record()
    del document["skipped_tables"]
    with pytest.raises(LineageRefused, match=r"check\.json.*older shape.*skipped_tables"):
        raw_lineage.loaded_objects(document, "check.json", RUN)


@pytest.mark.parametrize(
    "bad",
    [
        "gamma_data",
        [{"table": "gamma_data"}],
        [{"table": "gamma_data", "reason": 3}],
        [{"table": "gamma_data", "reason": "x", "extra": 1}],
        [{"table": "alpha_data", "reason": "no source object"}],
        [{"table": "gamma_data", "reason": "no source object"}, {"table": "gamma_data", "reason": "no source object"}],
    ],
)
def test_a_record_with_a_malformed_or_requested_skipped_table_is_refused_by_the_lineage_reader(bad: Any) -> None:
    document = record()
    document["skipped_tables"] = bad
    with pytest.raises(LineageRefused):
        raw_lineage.loaded_objects(document, "check.json", RUN)


def staged_report(**changes: Any) -> bytes:
    body = {"schema": 1, "bronze_run": RUN, "requested_tables": ["alpha_data", "beta_data"], "skipped_tables": SKIPPED, "skipped_count": 1} | changes
    return json.dumps(body).encode()


def staged_request(**changes: Any) -> bytes:
    body = {"tables": ["alpha_data", "beta_data"], "target": "real", "dry_run": False, "inventory_sha256": SHA, "skipped_tables": ["gamma_data"]} | changes
    return json.dumps(body, sort_keys=True).encode()


def bound(request: bytes | None = None) -> bronze_run.Job:
    """The job of a run whose preflight bound the digest of ``request`` (the standard staged request when none is given)."""
    digest = hashlib.sha256(staged_request() if request is None else request).hexdigest()
    return bronze_run.Job(RUN, Path("/workspace"), BUCKET, ("alpha_data", "beta_data"), "real", False, digest, SHA)


def staged(monkeypatch: pytest.MonkeyPatch, request: bytes | None, report: bytes | None) -> None:
    """The staged inputs as the task role reads them: a key that was never staged answers 403."""
    from botocore.exceptions import ClientError

    def read(run: str, bucket: str, name: str) -> bytes:
        body = {bronze_run.REQUEST_FILE: request, bronze_run.STAGE_REPORT_FILE: report}[name]
        if body is None:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "GetObject")
        return body

    monkeypatch.setattr(bronze_run, "read_staged", read)


def test_the_check_phase_reads_the_skipped_tables_from_the_staged_request_and_the_stage_report_that_names_the_same_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staged(monkeypatch, staged_request(), staged_report())
    check(bronze_run.staged_skipped(bound()) == SKIPPED, "the skipped tables were not read")


def test_a_hand_run_with_no_request_digest_has_no_skipped_table(monkeypatch: pytest.MonkeyPatch) -> None:
    staged(monkeypatch, staged_request(), staged_report())
    check(bronze_run.staged_skipped(job()) == [], "a run with no bound request reported a skipped table")


def test_a_request_that_changed_after_the_preflight_bound_its_digest_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    staged(monkeypatch, staged_request(skipped_tables=[]), staged_report(skipped_tables=[], skipped_count=0))
    with pytest.raises(bronze_run.StepError, match="differs from the one the preflight checked"):
        bronze_run.staged_skipped(bound())


def test_a_report_altered_alone_to_drop_or_add_a_skipped_table_is_refused_against_the_request(monkeypatch: pytest.MonkeyPatch) -> None:
    for report in (staged_report(skipped_tables=[], skipped_count=0), staged_report(skipped_tables=[{"table": "delta_data", "reason": "no source object"}])):
        staged(monkeypatch, staged_request(), report)
        with pytest.raises(bronze_run.StepError, match="request"):
            bronze_run.staged_skipped(bound())


def test_a_request_that_names_a_skipped_table_with_no_stage_report_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    staged(monkeypatch, staged_request(), None)
    with pytest.raises(bronze_run.StepError, match="stage report"):
        bronze_run.staged_skipped(bound())


@pytest.mark.parametrize("code", ["AccessDenied", "NoSuchKey", "404", "403"])
def test_a_hand_staged_run_that_skips_nothing_needs_no_stage_report(monkeypatch: pytest.MonkeyPatch, code: str) -> None:
    from botocore.exceptions import ClientError

    request = staged_request(skipped_tables=[])

    def read(run: str, bucket: str, name: str) -> bytes:
        if name == bronze_run.REQUEST_FILE:
            return request
        raise ClientError({"Error": {"Code": code, "Message": "x"}}, "GetObject")

    monkeypatch.setattr(bronze_run, "read_staged", read)
    check(bronze_run.staged_skipped(bound(request)) == [], "a hand-staged run reported a skipped table")


def test_another_failure_reading_the_stage_report_is_raised_and_never_read_as_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from botocore.exceptions import ClientError

    request = staged_request()

    def read(run: str, bucket: str, name: str) -> bytes:
        if name == bronze_run.REQUEST_FILE:
            return request
        raise ClientError({"Error": {"Code": "SlowDown", "Message": "x"}}, "GetObject")

    monkeypatch.setattr(bronze_run, "read_staged", read)
    with pytest.raises(ClientError):
        bronze_run.staged_skipped(bound())


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        staged_report(bronze_run="bronze_aaaaaaaa"),
        staged_report(skipped_count=2),
        staged_report(skipped_tables=[{"table": "alpha_data", "reason": "no source object"}], skipped_count=1),
        staged_report(skipped_tables="x"),
        staged_report(requested_tables=["alpha_data"]),
        staged_report(skipped_tables=[{"table": "gamma_data", "reason": "another reason"}]),
    ],
)
def test_a_stage_report_of_another_run_or_with_a_wrong_count_or_naming_a_requested_table_is_refused(monkeypatch: pytest.MonkeyPatch, body: bytes) -> None:
    staged(monkeypatch, staged_request(), body)
    with pytest.raises(bronze_run.StepError):
        bronze_run.staged_skipped(bound())


def test_the_run_record_is_the_load_receipt_alone_and_names_no_mirror_outcome_even_when_the_report_has_one() -> None:
    document = record(report={"mirror": {"status": "ok", "tables": {"alpha_data": "created"}}})
    check("mirror" not in document, f"the immutable load receipt carries a mutable mirror outcome: {document.get('mirror')}")
    raw_lineage.loaded_objects(document, "check.json", RUN)


def test_a_run_record_with_a_mirror_key_is_refused_by_the_lineage_reader_so_the_receipt_keeps_one_shape() -> None:
    document = record()
    document["mirror"] = {"status": "ok", "tables": {}}
    with pytest.raises(LineageRefused):
        raw_lineage.loaded_objects(document, "check.json", RUN)


def test_a_malformed_mirror_outcome_in_the_loader_report_is_refused_by_the_check_phase() -> None:
    for bad in (
        None,
        "ok",
        {"status": "ok"},
        {"status": "great", "tables": {}},
        {"status": "failed", "tables": {}},
        {"status": "ok", "tables": {}, "reason": "x"},
    ):
        check(not bronze_mirror.outcome_shaped(bad), f"the driver accepts {bad!r}")
    check(bronze_mirror.outcome_shaped({"status": "ok", "tables": {"alpha_data": "created"}}), "a good outcome is refused")


def test_the_loader_is_told_the_bronze_run_it_registers_the_tables_for(monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[list[str]] = []
    monkeypatch.setattr(bronze_run, "pinned_inventory", lambda job: {})
    monkeypatch.setattr(bronze_run, "staged_skipped", lambda job: [])
    monkeypatch.setattr(bronze_run, "run_module", lambda job, module, arguments, environment=None: started.append([str(item) for item in arguments]))
    bronze_run.load(bound())
    arguments = started[0]
    check(arguments[arguments.index("--run-id") + 1] == RUN, f"{arguments}")
