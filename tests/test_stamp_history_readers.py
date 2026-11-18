"""A reader of stored history accepts the stamp-form run IDs of before the run ID redesign, and a new run or request takes the run ID form alone.

Each case sends a stored record, row, pin or file name that names a stamp-form run through the real reader. The same record with a run ID of the form passes
in the same case, so a case fails for the run's form and nothing else. A stored value that is of neither form is still refused.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from orchestration import training_contract as contract
from tests.support import check

STAMP = "20261107T133742Z-14bad53c"
OTHER_STAMP = "20261111T224035Z-3f1a9c7d"
NEW = "silver_abcdef01"
BAD = ("20261107T133742Z-14bad53", "UPPER_abcdef01", "silver_abcdef0", "latest", "")


def refused(action: Any, *names: str) -> str:
    """The message of the refusal an action raises, which must be one of the named types (by class name)."""
    try:
        action()
    except Exception as error:
        check(type(error).__name__ in names, f"the refusal is {type(error).__name__}, not one of {names}: {error}")
        return str(error)
    raise AssertionError(f"nothing refused; expected one of {names}")


# publication reads: one stamp row must not block the other rows


class Rows:
    """A ``publish_runs`` table stand-in that scans to the given rows."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def scan(self, selected_fields: tuple[str, ...]) -> Rows:
        return self

    def to_arrow(self) -> Rows:
        return self

    def to_pylist(self) -> list[dict[str, Any]]:
        return self.rows


def publication_rows(monkeypatch: pytest.MonkeyPatch, *rows: dict[str, Any]) -> Any:
    from scripts.features import publication_reads as reads

    monkeypatch.setattr(reads, "load_table", lambda catalog, namespace, name: Rows(list(rows)))
    monkeypatch.setattr(
        reads, "iceberg_columns", lambda table: {"run_id": "string", "committed_at": "timestamptz", "snapshot_set": "string", "base_run": "string"}
    )
    return reads


def row(run: str, committed: str, base: str | None = None) -> dict[str, Any]:
    return {"run_id": run, "committed_at": committed, "snapshot_set": json.dumps({"t1": 7}), "base_run": base}


def test_a_retired_stamp_publication_row_does_not_block_the_other_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    reads = publication_rows(monkeypatch, row(STAMP, "2026-11-07T13:37:42+00:00"), row("features_abcdef01", "2026-11-13T10:00:00+00:00"))
    check(reads.read_publication(object(), "features").run_id == "features_abcdef01", "the head is not the valid row")
    check(reads.read_publication(object(), "features", "features_abcdef01").run_id == "features_abcdef01", "the named valid row is not read")
    message = refused(lambda: reads.read_publication(object(), "features", STAMP), "ReadError")
    check("retired" in str(message), f"a named retired stamp row is not refused as retired: {message}")


def test_a_stamp_publication_row_and_a_stamp_base_run_are_history_and_other_row_identities_refuse(monkeypatch: pytest.MonkeyPatch) -> None:
    reads = publication_rows(monkeypatch, row(OTHER_STAMP, "2026-11-13T10:00:00+00:00", base=STAMP))
    found = reads.read_publication(object(), "features")
    check((found.run_id, found.base_run) == (OTHER_STAMP, STAMP), f"a stamp row with a stamp base run is not read: {found}")
    for bad in BAD:
        reads = publication_rows(monkeypatch, row(bad, "2026-11-13T10:00:00+00:00"))
        refused(lambda reads=reads: reads.read_publication(object(), "features"), "ReadError")
        reads = publication_rows(monkeypatch, row("features_abcdef01", "2026-11-13T10:00:00+00:00", base=bad or "x"))
        refused(lambda reads=reads: reads.read_publication(object(), "features"), "ReadError")


# layer acceptance: the claim of a stamp-form launch names the writes of its recovery


def listing(*keys: str) -> list[tuple[str, str, str, int]]:
    return [(key, "v1", "e", 1) for key in keys]


def test_the_claim_of_a_stamp_form_launch_names_its_recovery_writes_as_another_runs() -> None:
    from scripts.lakehouse import layer_acceptance as acceptance

    for run in (STAMP, "gold_abcdef02"):
        base = f"lakehouse/launch_records/{run}/gold"
        claimed = acceptance.other_runs_claims([listing(f"{base}/claim.json")])
        check(claimed == {run: {"gold"}}, f"the claim of {run} is not read: {claimed}")
        for key in (f"{base}/cancel.json", f"{base}/decision.json", f"{base}/01/end.json", f"lakehouse/launch_records/families/gold/{run}.json"):
            check(acceptance.other_runs_key(key, claimed), f"{key} is not another run's write")
        check(
            not acceptance.other_runs_key(f"lakehouse/launch_records/families/silver/{run}.json", claimed),
            "a family index key of another job is another run's write",
        )
    check(acceptance.other_runs_claims([listing("lakehouse/launch_records/20261107T133742Z-14bad53/gold/claim.json")]) == {}, "a malformed run names a claim")
    check(acceptance.other_runs_claims([listing("lakehouse/launch_records/x/gold/claim.json")]) == {}, "a name that is no run names a claim")


def test_the_pin_check_of_a_report_reads_a_stamp_form_producer_run() -> None:
    from scripts.lakehouse import layer_acceptance as acceptance

    check(acceptance.STORED_RUN.fullmatch(STAMP) is not None and acceptance.STORED_RUN.fullmatch(NEW) is not None, "the report does not read both forms")
    check(all(acceptance.STORED_RUN.fullmatch(bad) is None for bad in BAD), "the report reads a malformed run")
    check(acceptance.RUN_ID.fullmatch(STAMP) is None, "the run of a new report takes a stamp form")


# committed sets, pins and lineage


def test_a_committed_silver_set_of_a_stamp_run_is_read_and_a_malformed_one_is_refused() -> None:
    from scripts.lakehouse import silver_publish

    spark = SimpleNamespace(sql=lambda query: SimpleNamespace(collect=lambda: [(5,)]))
    ctx = SimpleNamespace(mode="real", plan=SimpleNamespace(tables=("t1",)), spark=spark, fq=lambda name: name)
    for run in (STAMP, NEW):
        done = silver_publish.checked_set(ctx, {"mode": "real", "run_id": run, "snapshot_set": {"t1": 5}})
        check(done == {"t1": 5}, f"the committed set of {run} is not read: {done}")
    for bad in BAD:
        refused(lambda bad=bad: silver_publish.checked_set(ctx, {"mode": "real", "run_id": bad, "snapshot_set": {"t1": 5}}), "PublishError")


@pytest.mark.parametrize("source", [STAMP, NEW])
def test_a_pin_names_a_stamp_or_redesigned_source_run(source: str) -> None:
    from orchestration import pipeline_protocol
    from scripts.features import features_input

    pin = {"schema": 1, "run_id": "gold_abcdef02", "mode": "real", "namespace": "silver", "source_run_id": source, "snapshot_set": {"t1": 5}}
    pipeline_protocol.check_pin(pin, "gold_abcdef02", "real", {"t1"})
    features_pin = {**pin, "namespace": "gold"}
    features_input.check_pin(features_pin, "gold_abcdef02", "real", {"t1"}, {"real": "gold"}, "gold")
    for bad in BAD:
        refused(lambda bad=bad: pipeline_protocol.check_pin({**pin, "source_run_id": bad}, "gold_abcdef02", "real", {"t1"}), "PipelineError")
        refused(
            lambda bad=bad: features_input.check_pin({**features_pin, "source_run_id": bad}, "gold_abcdef02", "real", {"t1"}, {"real": "gold"}, "gold"),
            "PipelineError",
        )


@pytest.mark.parametrize("source", [STAMP, NEW])
def test_the_input_lineage_of_a_pin_reads_a_stamp_source_run(source: str) -> None:
    from orchestration import ecs_lineage

    pin_key = f"{ecs_lineage.EVIDENCE_PREFIX}/gold/gold_abcdef02/silver_pins.json"
    pin = {"schema": 1, "run_id": "gold_abcdef02", "mode": "real", "namespace": "silver", "source_run_id": source, "snapshot_set": {"s1": 21}}
    evidence = SimpleNamespace(read=lambda key: pin if key == pin_key else None)
    facts = ecs_lineage.LaunchFacts("gold_abcdef02", "gold", 1, "2026-11-18T00:00:00+00:00", "2026-11-18T01:00:00+00:00", "succeeded", "latest")
    checks = ecs_lineage.Checks(frozen=False)
    try:
        _, _, _, found = ecs_lineage.input_pins(evidence, facts, {}, checks)
    except ecs_lineage.LineageRefused as error:
        check("is not a run ID" not in str(error), f"the pin source {source} is refused as no run ID: {error}")
    else:
        check(found == source, f"the input run is {found}, not {source}")
    bad_pin = {**pin, "source_run_id": "silver_abcdef0"}
    evidence = SimpleNamespace(read=lambda key: bad_pin if key == pin_key else None)
    check(
        "is not a run ID" in refused(lambda: ecs_lineage.input_pins(evidence, facts, {}, checks), "LineageRefused"),
        "a malformed source is not refused as no run ID",
    )


# research records: reading the record of a stamp-form research run


def test_the_research_records_of_a_stamp_run_are_read_and_the_key_of_a_new_one_is_written_in_the_run_id_form_only() -> None:
    from orchestration import promotion_chain

    key = f"research/{STAMP}/research_HAI_1.json"
    check(contract.stored_record_key("research", STAMP, "HAI_1") == key, "the stored key of a stamp research run is wrong")
    check(contract.stored_record_key("research", "research_abcdef01", "HAI_1") == contract.record_key("research", "research_abcdef01", "HAI_1"), "keys differ")
    refused(lambda: contract.record_key("research", STAMP, "HAI_1"), "ValueError")
    for bad in BAD:
        refused(lambda bad=bad: contract.stored_record_key("research", bad, "HAI_1"), "ValueError")
    refused(lambda: contract.stored_record_key("research", STAMP, "HAI_9"), "ValueError")
    stored = {key: json.dumps({"hai_type": "HAI_1", "promotion": {"challenger": {"model": "m2", "version": "3"}}}).encode()}
    key_name, document = promotion_chain.research_document(stored.get, STAMP, "HAI_1")
    check(key_name == key and document["hai_type"] == "HAI_1", "the stamp research record is not read")
    canary_key = f"research/{STAMP}/canary_HAI_1.json"
    canary = {"research_run": STAMP, "hai_type": "HAI_1", "model": "m2", "version": "3", "decision": "hold"}
    check(
        promotion_chain.stored_decision({canary_key: json.dumps(canary).encode()}.get, STAMP, "HAI_1", "m2", "3") == "hold",
        "the stamp canary decision is not read",
    )


def test_the_features_run_of_a_research_record_is_read_in_either_form() -> None:
    from orchestration import promotion_chain

    for features_run in (STAMP, "features_ols_abcdef01"):
        record = {
            "hai_type": "HAI_1",
            "features_runs": {"m1": features_run},
            "promotion": {"challenger": {"model": "m1", "version": "3"}, "production_version": None},
        }
        stored = {f"research/{STAMP}/research_HAI_1.json": json.dumps(record).encode()}
        found = promotion_chain.challenger_of(stored.get, STAMP, "HAI_1")
        check(found is not None and found.features_run == features_run, f"the features run {features_run} of a research record is not read")
    record["features_runs"] = {"m1": "features_ols_abcdef0"}
    stored = {f"research/{STAMP}/research_HAI_1.json": json.dumps(record).encode()}
    refused(lambda: promotion_chain.challenger_of(stored.get, STAMP, "HAI_1"), "RecordRefused")


# stored protection records, receipts, holds and file names


def test_a_protected_file_registration_of_a_stamp_run_is_read(tmp_path: Path) -> None:
    from scripts.lakehouse import gold_maintenance

    def protected(run: str) -> Path:
        path = tmp_path / f"{run}.json"
        document = {
            "schema": 1,
            "fence": {"run_id": "gold_abcdef02", "attempt_id": "a1", "taken_at": "2026-11-18T00:00:00Z"},
            "items": [{"kind": "registration", "id": "reader", "run_id": run}],
        }
        path.write_text(json.dumps(document))
        return path

    for run in (STAMP, NEW):
        items = gold_maintenance.load_protected(protected(run)).items
        check(items[0].run_id == run, f"the registration of {run} is not read")
    for bad in BAD:
        refused(lambda bad=bad: gold_maintenance.load_protected(protected(bad)), "PublishError")


def test_a_prerequisite_receipt_of_a_stamp_publication_is_read() -> None:
    from scripts.lakehouse import register_aws_catalog as registration

    required = {("silver", "t1"), ("gold", "t2"), ("silver", "publish_runs"), ("gold", "publish_runs")}

    def receipts(run: str) -> dict[str, bytes]:
        def body(table: str) -> bytes:
            return json.dumps({"schema": 1, "state": "committed", "run_id": run, "backup_sha256": "a" * 64, "snapshot_set": {table: 5}}).encode()

        return {"silver": body("t1"), "gold": body("t2")}

    for run in (STAMP, NEW):
        done = registration.checked_receipts(receipts(run), required)
        check(done["silver"]["run_id"] == run, f"the receipt of {run} is not read")
    refused(lambda: registration.checked_receipts(receipts("silver_abcdef0"), required), "RegistrationError")


def test_the_holds_and_loads_of_a_stamp_base_run_are_read() -> None:
    from scripts.features import feast_sets, online_reload

    for base in (STAMP, "features_abcdef01"):
        hold = {"feature_set": "m1", "run_id": "features_ols_abcdef02", "set_schema_sha256": "b" * 64, "base_run": base}
        holds = SimpleNamespace(read=lambda name, run, hold=hold: hold)
        online_reload.check_held_run("m1", "features_ols_abcdef02", "b" * 64, holds)
        record = {
            "step": "materialize",
            "feature_set": "m1",
            "run_id": "features_ols_abcdef02",
            "mode": "real",
            "online": feast_sets.feature_sets.LOAD,
            "set_schema_sha256": "b" * 64,
            "load_id": "load-1",
            "base_run": base,
        }
        feast_sets.direct_load_record("m1", "features_ols_abcdef02", "real", "b" * 64, SimpleNamespace(read=lambda name, run, record=record: record))
    refused(
        lambda: online_reload.check_held_run(
            "m1", "features_ols_abcdef02", "b" * 64, SimpleNamespace(read=lambda name, run: {**hold, "base_run": "features_abcdef0"})
        ),
        "ReadError",
    )


def test_the_pin_source_and_launch_claim_input_run_of_the_openmetadata_chain_are_read_in_either_form() -> None:
    from scripts.infrastructure import openmetadata_chain as chain

    for run in (STAMP, NEW):
        pin = {"schema": 1, "run_id": "gold_abcdef02", "mode": "real", "namespace": "silver", "source_run_id": run, "snapshot_set": {"t1": 5}}
        s3 = SimpleNamespace(get_object=lambda Bucket, Key, pin=pin: {"Body": SimpleNamespace(read=lambda limit, pin=pin: json.dumps(pin).encode())})
        check(chain.source_run(s3, "bucket", "gold", "gold_abcdef02", "silver_pins.json", "silver") == run, f"the source {run} is not read")
        claim = {"event": "claim", "run_id": "silver_abcdef03", "job": "silver", "bound": {"input_run": run}}
        s3 = SimpleNamespace(get_object=lambda Bucket, Key, claim=claim: {"Body": SimpleNamespace(read=lambda limit, claim=claim: json.dumps(claim).encode())})
        check(chain.bronze_run(s3, "bucket", "silver_abcdef03") == run, f"the input run {run} of a claim is not read")
    pin["source_run_id"] = "silver_abcdef0"
    s3 = SimpleNamespace(get_object=lambda Bucket, Key: {"Body": SimpleNamespace(read=lambda limit: json.dumps(pin).encode())})
    refused(lambda: chain.source_run(s3, "bucket", "gold", "gold_abcdef02", "silver_pins.json", "silver"), "ChainError")


def test_a_gold_report_reads_the_stamp_run_its_pin_names(tmp_path: Path) -> None:
    from orchestration import layer_report

    pin = {"source_run_id": STAMP}
    access = SimpleNamespace(directory=tmp_path)
    try:
        layer_report.silver_input(access, pin, tmp_path)
    except layer_report.ReportError as error:
        check("names no silver run" not in str(error), f"the gold pin that names a stamp silver run is refused: {error}")
    refused(lambda: layer_report.silver_input(access, {"source_run_id": "silver_abcdef0"}, tmp_path), "ReportError")


# the patterns themselves


def test_the_stored_run_pattern_is_the_stamp_form_and_the_run_id_form_and_nothing_else() -> None:
    from scripts.lakehouse import gold_publish_record as record

    for pattern in (contract.STORED_RUN, record.STORED_RUN_PATTERN):
        for run in (STAMP, NEW, "features_elastic_net_abcdef01_2", "ingestion_retry_abcdef01_2"):
            check(re.fullmatch(pattern, run) is not None, f"{run} is refused")
        for bad in (*BAD, f"{STAMP}\n", f"x{STAMP}", f"{STAMP}0"):
            check(re.fullmatch(pattern, bad) is None, f"{bad!r} is accepted")
    check(re.fullmatch(contract.RUN_ID, STAMP) is None and record.RUN_ID.fullmatch(STAMP) is None, "a new run or request takes the stamp form")
