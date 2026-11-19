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


def by_hand(history: Any) -> tuple[list[str], list[str], dict[str, str]]:
    """What ``glue_mirror register --layer bronze`` would register, leave out and keep required when the run records skip ``TABLE``."""
    records = Bucket({key_of(RUN_A): record(RUN_A, ["alpha"], [TABLE])})
    return glue_mirror.bronze_register_tables(records, BUCKET, Glue(), "bronze", history)


def test_the_hook_registers_exactly_the_tables_the_by_hand_register_would_and_leaves_the_sourceless_one_out() -> None:
    """Tonight's live by-hand register: 334 created, 1 left out (``hcai_util_workbook_sheet_rows``)."""
    glue = stand_in.FakeGlue()
    report = run_hook(glue, history_reader(), [TABLE])
    registered = sorted(name for name, outcome in report["tables"].items() if outcome in DONE)
    wanted, left_out, kept = by_hand(history_reader())
    check(report["status"] == "ok", f"{report['status']}: {report.get('reason')}")
    check(registered == sorted(wanted), f"hook only: {sorted(set(registered) - set(wanted))}, by hand only: {sorted(set(wanted) - set(registered))}")
    check(left_out == [TABLE] and kept == {}, f"the by-hand register changed its decision: {left_out} {kept}")
    check(len(registered) == 334 and TABLE not in registered, f"{len(registered)} tables registered")
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
    check(len(registered) == 335 - 1, f"the other tables were not registered: {len(registered)}")
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
    check(sum(text in DONE for text in report["tables"].values()) == 334, "a failed history read stopped the registration of the committed tables")


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
    check(bronze_mirror.source_history is bronze_source_history, "the hook does not use the shared evidence module")
    check(json.dumps(bronze_source_history.READ_ROOTS) == '["", "raw/"]', f"{bronze_source_history.READ_ROOTS}")
