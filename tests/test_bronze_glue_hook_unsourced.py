"""The bronze Glue mirror hook next to the source-less table work: a table the load left out for lack of a source object is left out of the hook's
registration exactly as ``glue_mirror register --layer bronze`` leaves it out, on the same evidence, and the live omission rules stay where they were.

Live finding of Nov 18 2026: the load leaves ``hcai_util_workbook_sheet_rows`` out until its workbooks arrive, and the by-hand register created 334
tables and left that one out. The exclusion stays upstream of the hook (the stage names the table, ``bronze.check_unsourced`` checks it against the
pinned inventory). The hook adds one read: the version history of the table's own source collection, through the same ``source_evidence`` the register
uses, so a delete-marked manifest still counts as history. A table with history is required, and the load committed no snapshot for it: that is a
failed mirror, never a quiet omission."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

import pytest

from scripts.lakehouse import bronze, bronze_mirror, bronze_source_history, bronze_unsourced, glue_mirror
from scripts.lakehouse import run_glue_mirror_e2e as stand_in
from tests.support import check
from tests.test_bronze_glue_mirror_hook import SIDE_SNAPSHOTS, Source, enabled, reader
from tests.test_bronze_unsourced_required import BUCKET, RUN_A, TABLE, Bucket, Glue, key_of, record, versioned

RUN = "bronze_0a1b2c3d"
DONE = ("created", "updated", "unchanged")


def table_map() -> dict[str, Any]:
    return bronze.load_table_map()


# Every mapped table plus the four loader tables: what the Glue mirror registers when no table is left out. The count follows the table map.
ALL_TABLES = len(table_map()["tables"]) + 4


def loaded_names() -> list[str]:
    """Every mapped table except the source-less one: what a bootstrap that leaves ``TABLE`` out requests."""
    return [str(entry["table"]) for entry in table_map()["tables"] if entry["table"] != TABLE]


def history_reader(root: str = "", *, deleted: bool = False) -> Any:
    """The real ``S3Storage`` over a versioned stand-in bucket. With ``deleted`` the table's stored workbook and its manifest carry a delete marker."""
    if deleted:
        return versioned(root, manifests=True, payloads=False)[0]
    return bronze.S3Storage.from_client(stand_in.VersionedClient(stand_in.FakeSources(), stand_in.FakeS3()))


def run_hook(glue: Any, history: Any, unsourced: list[str], names: list[str] | None = None, **keywords: Any) -> dict[str, Any]:
    names = loaded_names() if names is None else names
    keywords.setdefault("s3", Bucket({}))
    snapshots = {name: 100 + number for number, name in enumerate(names)}
    keywords.setdefault("read_loader", reader(SIDE_SNAPSHOTS))
    return bronze_mirror.mirror_after_load(
        None,
        "bronze",
        names,
        snapshots,
        RUN,
        source=Source(),
        glue=glue,
        config=enabled(),
        unsourced=unsourced,
        history=history,
        bucket=BUCKET,
        table_map=table_map(),
        **keywords,
    )


def by_hand(
    history: Any, skipped: str = TABLE, metadata: tuple[str, ...] = (), glue_tables: tuple[str, ...] = ()
) -> tuple[list[str], list[str], dict[str, str]]:
    """What ``glue_mirror register --layer bronze`` would register, leave out and keep required when the run records skip ``skipped``, ``metadata`` names tables
    that have a metadata file in S3 and ``glue_tables`` names tables that Glue already holds."""
    files = {f"lakehouse/bronze/{name}/metadata/00001-0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f.metadata.json": b"{}" for name in metadata}
    records = Bucket({key_of(RUN_A): record(RUN_A, ["alpha"], [skipped]), **files})
    return glue_mirror.bronze_register_tables(records, BUCKET, Glue(set(glue_tables)), "bronze", history)


def metadata_bucket(*tables: str) -> Bucket:
    """The bucket as the hook lists it: a metadata file for each of ``tables``."""
    return Bucket({f"lakehouse/bronze/{name}/metadata/00001-0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f.metadata.json": b"{}" for name in tables})


def registered_in(report: dict[str, Any]) -> list[str]:
    return sorted(name for name, outcome in report["tables"].items() if outcome in DONE)


def test_the_hook_registers_exactly_the_tables_the_by_hand_register_would_and_leaves_the_sourceless_one_out() -> None:
    """The by-hand register on Nov 18 2026 created every mapped and loader table but one and left one out (``hcai_util_workbook_sheet_rows``)."""
    glue = stand_in.FakeGlue()
    report = run_hook(glue, history_reader(), [TABLE])
    registered = sorted(name for name, outcome in report["tables"].items() if outcome in DONE)
    wanted, left_out, kept = by_hand(history_reader())
    check(report["status"] == "ok", f"{report['status']}: {report.get('reason')}")
    check(registered == sorted(wanted), f"hook only: {sorted(set(registered) - set(wanted))}, by hand only: {sorted(set(wanted) - set(registered))}")
    check(left_out == [TABLE] and kept == {}, f"the by-hand register changed its decision: {left_out} {kept}")
    check(len(registered) == ALL_TABLES - 1 and TABLE not in registered, f"{len(registered)} tables registered")
    check(report["tables"][TABLE].startswith("left out: ") and "no source object" in report["tables"][TABLE], f"{report['tables'].get(TABLE)}")
    check(("bronze", TABLE) not in glue.tables, "the left-out table was registered in Glue")


@pytest.mark.parametrize("root", ["", "raw/"])
def test_a_delete_marked_manifest_still_counts_as_history_for_the_hook_as_for_the_by_hand_register(root: str) -> None:
    history = history_reader(root, deleted=True)
    glue = stand_in.FakeGlue()
    report = run_hook(glue, history, [TABLE])
    _, left_out, kept = by_hand(history_reader(root, deleted=True))
    check(left_out == [] and kept == {TABLE: bronze_source_history.HISTORY_REASON}, f"the by-hand register left it out: {left_out} {kept}")
    outcome = report["tables"][TABLE]
    check(outcome.startswith("failed: required") and bronze_source_history.HISTORY_REASON in outcome, f"{outcome}")
    check(report["status"] == "failed" and TABLE in report["reason"], f"{report['status']} {report.get('reason')}")
    registered = [name for name, text in report["tables"].items() if text in DONE]
    check(len(registered) == ALL_TABLES - 1, f"the other tables were not registered: {len(registered)}")
    check(("bronze", TABLE) not in glue.tables, "a table with no snapshot was registered")


def test_the_loader_never_reads_the_history_for_a_table_that_is_not_left_out_and_reads_it_once_for_those_that_are() -> None:
    class Counting:
        def __init__(self) -> None:
            self.asked: list[str] = []

        def list_history(self, bucket: str, prefix: str) -> list[tuple[str, str, bool]]:
            self.asked.append(prefix)
            return []

    none = Counting()
    run_hook(stand_in.FakeGlue(), none, [], names=loaded_names()[:2])
    check(none.asked == [], f"the history was read with nothing left out: {none.asked}")
    some = Counting()
    run_hook(stand_in.FakeGlue(), some, [TABLE])
    check(0 < len(some.asked) <= 4 and len(set(some.asked)) == len(some.asked), f"each history prefix is listed once: {some.asked}")


def test_a_history_listing_that_fails_is_a_reported_failure_and_the_committed_tables_are_still_registered() -> None:
    class Broken:
        def list_history(self, bucket: str, prefix: str) -> list[tuple[str, str, bool]]:
            raise RuntimeError("history stand-in down")

    glue = stand_in.FakeGlue()
    report = run_hook(glue, Broken(), [TABLE])
    check(report["status"] == "failed" and TABLE in report["reason"] and "history stand-in down" in report["reason"], f"{report}")
    check(report["tables"][TABLE].startswith("failed: ") and "history" in report["tables"][TABLE], f"{report['tables'].get(TABLE)}")
    check(sum(text in DONE for text in report["tables"].values()) == ALL_TABLES - 1, "a failed history read stopped the registration of the committed tables")


def test_a_reader_that_cannot_list_history_cannot_prove_absence_and_fails_the_mirror() -> None:
    report = run_hook(stand_in.FakeGlue(), object(), [TABLE])
    check(report["status"] == "failed" and report["tables"][TABLE].startswith("failed: "), f"{report}")


@pytest.mark.parametrize(("namespace", "enabled_flag"), [("bronze", False), ("bronze_e2e", True), ("bronze_dictionary", True)])
def test_nothing_reads_the_history_while_the_mirror_is_off_or_in_a_test_namespace(namespace: str, enabled_flag: bool) -> None:
    class Forbidden:
        def list_history(self, *_arguments: Any) -> Any:
            raise AssertionError("the history was read")

    config = enabled() if enabled_flag else replace(glue_mirror.load_config(), enabled=False)
    report = bronze_mirror.mirror_after_load(
        None,
        namespace,
        loaded_names(),
        {},
        RUN,
        source=Source(),
        glue=stand_in.FakeGlue(),
        config=config,
        unsourced=[TABLE],
        history=Forbidden(),
        bucket=BUCKET,
    )
    check(report["status"] == "skipped", f"{report}")


def test_the_hook_and_the_register_share_one_evidence_function_and_one_reason() -> None:
    check(bronze_unsourced.source_evidence is bronze_source_history.source_evidence, "the register and the hook read different evidence functions")
    check(bronze_unsourced.UnsourcedError is bronze_source_history.UnsourcedError, "the omission error was duplicated")
    import inspect

    rule = inspect.getsource(glue_mirror.omission_decision)
    check("bronze_source_history.source_evidence" in rule, "the shared omission rule does not read the shared evidence")
    check("glue_mirror.omission_decision" in inspect.getsource(bronze_mirror.mirror_after_load), "the hook does not use the shared omission rule")
    check("omission_decision(" in inspect.getsource(glue_mirror.bronze_register_tables), "the register does not use the shared omission rule")
    check(json.dumps(bronze_source_history.READ_ROOTS) == '["", "raw/"]', f"{bronze_source_history.READ_ROOTS}")


SILVER_TABLE = "acs_b16005"


def names_without(*left_out: str) -> list[str]:
    return [str(entry["table"]) for entry in table_map()["tables"] if entry["table"] not in left_out]


def test_the_silver_readers_file_the_hook_reads_equals_the_dbt_bronze_sources() -> None:
    """The Spark image has no dbt or yaml, so the list of tables a silver model reads reaches it as a small file that the host derives from the dbt sources."""
    derived = sorted(glue_mirror.silver_bronze_sources())
    check(sorted(glue_mirror.silver_readers_file()) == derived, "config/lakehouse/bronze_silver_readers.json differs from the dbt bronze sources")
    check(SILVER_TABLE in derived and TABLE not in derived, "the cases below need one silver-read table and one that silver does not read")


def test_a_left_out_table_that_a_silver_model_reads_is_required_and_fails_the_mirror_by_name_as_in_the_register() -> None:
    glue = stand_in.FakeGlue()
    report = run_hook(glue, history_reader(), [SILVER_TABLE], names=names_without(SILVER_TABLE))
    wanted, left_out, kept = by_hand(history_reader(), skipped=SILVER_TABLE)
    check(left_out == [] and kept == {SILVER_TABLE: glue_mirror.SILVER_REASON} and SILVER_TABLE in wanted, f"the register changed: {left_out} {kept}")
    check(
        report["status"] == "failed" and SILVER_TABLE in report["reason"] and glue_mirror.SILVER_REASON in report["reason"],
        f"{report['status']} {report.get('reason')}",
    )
    check(report["tables"][SILVER_TABLE].startswith("failed: required"), f"{report['tables'].get(SILVER_TABLE)}")
    check(
        len(registered_in(report)) == ALL_TABLES - 1 and ("bronze", SILVER_TABLE) not in glue.tables,
        "the other tables were not registered, or the silver table was",
    )


def test_a_left_out_table_with_s3_metadata_is_required_and_registers_from_its_committed_snapshot_as_the_register_does() -> None:
    wanted, left_out, kept = by_hand(history_reader(), metadata=(TABLE,))
    check(left_out == [] and kept == {TABLE: "its table metadata exists in S3"} and TABLE in wanted, f"the register changed: {left_out} {kept}")
    glue = stand_in.FakeGlue()
    held = reader({**SIDE_SNAPSHOTS, TABLE: 555})
    report = run_hook(glue, history_reader(), [TABLE], s3=metadata_bucket(TABLE), read_loader=held)
    check(report["status"] == "ok" and report["tables"][TABLE] == "created" and ("bronze", TABLE) in glue.tables, f"{report}")
    check(len(registered_in(report)) == ALL_TABLES, f"{len(registered_in(report))} tables registered")
    gone = run_hook(stand_in.FakeGlue(), history_reader(), [TABLE], s3=metadata_bucket(TABLE))
    check(
        gone["status"] == "failed" and TABLE in gone["reason"] and "metadata exists in S3" in gone["reason"],
        f"a required table with no snapshot passed: {gone}",
    )


def test_a_left_out_table_that_glue_already_holds_is_required_and_fails_the_mirror_when_nothing_is_committed_as_in_the_register() -> None:
    wanted, left_out, kept = by_hand(history_reader(), glue_tables=(TABLE,))
    check(left_out == [] and kept == {TABLE: "a Glue table of that name exists"} and TABLE in wanted, f"the register changed: {left_out} {kept}")
    glue = stand_in.FakeGlue()
    glue.tables[("bronze", TABLE)] = {"Name": TABLE, "Parameters": {}, "VersionId": "1"}
    report = run_hook(glue, history_reader(), [TABLE])
    check(report["status"] == "failed" and TABLE in report["reason"] and "a Glue table of that name exists" in report["reason"], f"{report}")
    check(report["tables"][TABLE].startswith("failed: required") and len(registered_in(report)) == ALL_TABLES - 1, f"{report['tables'].get(TABLE)}")


def test_an_existence_check_that_cannot_be_read_never_proves_absence_and_the_other_tables_are_still_registered() -> None:
    class Flaky(stand_in.FakeGlue):
        def get_table(self, DatabaseName: str, Name: str) -> dict[str, Any]:
            if Name == TABLE:
                raise stand_in.ClientError({"Error": {"Code": "InternalServiceException", "Message": "stand-in"}}, "GetTable")
            return super().get_table(DatabaseName, Name)

    report = run_hook(Flaky(), history_reader(), [TABLE])
    check(report["status"] == "failed" and TABLE in report["reason"] and "InternalServiceException" in report["tables"][TABLE], f"{report}")
    check(report["tables"][TABLE].startswith("failed: ") and len(registered_in(report)) == ALL_TABLES - 1, f"{report['tables'].get(TABLE)}")


def test_the_hook_and_the_register_leave_out_exactly_the_same_tables_for_each_condition() -> None:
    """One rule, the same inputs: a table is left out only with no source history, no S3 metadata, no Glue entry and no silver reader."""
    cases = [
        ("nothing found", TABLE, {}, "", ()),
        ("source history", TABLE, {}, "history", ()),
        ("s3 metadata", TABLE, {"metadata": (TABLE,)}, "", ()),
        ("glue entry", TABLE, {"glue_tables": (TABLE,)}, "", (TABLE,)),
        ("silver reader", SILVER_TABLE, {}, "", ()),
    ]
    for label, skipped, hand, history, held in cases:
        reader_for = history_reader(deleted=True) if history else history_reader()
        _, left_out, kept = by_hand(reader_for, skipped=skipped, **hand)
        glue = stand_in.FakeGlue()
        for name in held:
            glue.tables[("bronze", name)] = {"Name": name, "Parameters": {}, "VersionId": "1"}
        report = run_hook(
            glue,
            history_reader(deleted=True) if history else history_reader(),
            [skipped],
            names=names_without(skipped),
            s3=metadata_bucket(*hand.get("metadata", ())),
        )
        hook_left_out = sorted(name for name, text in report["tables"].items() if text.startswith("left out"))
        hook_required = sorted(name for name, text in report["tables"].items() if text.startswith("failed: required"))
        check(hook_left_out == sorted(left_out), f"{label}: the hook left out {hook_left_out}, the register {left_out}")
        check(hook_required == sorted(kept), f"{label}: the hook requires {hook_required}, the register {sorted(kept)}")


@pytest.mark.parametrize("code", ["AccessDeniedException"])
def test_a_glue_that_refuses_every_call_makes_the_mirror_unavailable_not_failed(code: str, capsys: pytest.CaptureFixture[str]) -> None:
    """Before the IAM grant exists the mirror is unavailable: Glue's refusal is named, nothing is registered and nothing is reported as ok."""
    glue = stand_in.FakeGlue(fail={"get_table": code})
    report = run_hook(glue, history_reader(), [TABLE])
    check(report["status"] == "unavailable" and code in report["reason"], f"{report['status']} {report.get('reason')}")
    check(not registered_in(report) and glue.writes() == [], "something was registered or written while Glue refused every call")
    err = capsys.readouterr().err
    check("UNAVAILABLE" in err and "FAILED" not in err and "IAM" in err, err)


def test_a_refusal_next_to_another_failure_is_still_a_failure() -> None:
    class Mixed(stand_in.FakeGlue):
        def get_table(self, DatabaseName: str, Name: str) -> dict[str, Any]:
            code = "AccessDeniedException" if Name < "b" else "InternalServiceException"
            raise stand_in.ClientError({"Error": {"Code": code, "Message": "stand-in"}}, "GetTable")

    report = run_hook(Mixed(), history_reader(), [])
    check(report["status"] == "failed", f"{report['status']}: a refusal hid another failure")
