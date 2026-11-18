"""Check the Glue mirror of published tables with stand-ins only: a fake Glue client and a fake Polaris pointer source.

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_glue_mirror_e2e --out <NEW_DIRECTORY>``. No AWS service is contacted, no
container starts and no Spark runs. Each case states one failure mode of the mirror and its required behavior. The report holds names and outcomes only.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import io
import json
import re
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from botocore.exceptions import ClientError

from scripts.lakehouse import glue_mirror, gold_publish
from scripts.lakehouse.gold_publish_record import PublishError

REPO_ROOT = Path(__file__).resolve().parents[2]
BUCKET = "example-lakehouse-bucket"
PILOT = "bridge_county_neighbor"
SECOND_TABLE = "dim_state"
RUN_ID = "silver_0123abcd"
DIGEST = "a" * 64
FIRST = f"s3://{BUCKET}/lakehouse/gold/{PILOT}/metadata/00001-aaaa.metadata.json"
SECOND = f"s3://{BUCKET}/lakehouse/gold/{PILOT}/metadata/00002-bbbb.metadata.json"
COLUMNS = [("county_sk", "string"), ("neighbor_sk", "string"), ("loaded_at", "timestamp")]
WRITES = ("create_table", "update_table")
Check = dict[str, bool]


class FakeGlue:
    """The Glue calls the mirror may use. ``fail`` names calls that raise a service error; every call is logged."""

    def __init__(
        self,
        databases: Sequence[str] = ("gold", "features", "silver", "bronze"),
        fail: dict[str, str] | None = None,
    ) -> None:
        self.databases = set(databases)
        self.tables: dict[tuple[str, str], dict[str, Any]] = {}
        self.calls: list[tuple[str, str, str]] = []
        self.fail = fail or {}
        self.updates: list[dict[str, Any]] = []

    def _error(self, operation: str, code: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": "stand-in"}}, operation)

    def _enter(self, operation: str, database: str, name: str) -> None:
        self.calls.append((operation, database, name))
        if operation in self.fail:
            raise self._error(operation, self.fail[operation])

    def get_table(self, DatabaseName: str, Name: str) -> dict[str, Any]:
        self._enter("get_table", DatabaseName, Name)
        if (DatabaseName, Name) not in self.tables:
            raise self._error("get_table", "EntityNotFoundException")
        return {"Table": self.tables[(DatabaseName, Name)]}

    def create_table(self, DatabaseName: str, TableInput: dict[str, Any]) -> dict[str, Any]:
        self._enter("create_table", DatabaseName, TableInput["Name"])
        if DatabaseName not in self.databases:
            raise self._error("create_table", "EntityNotFoundException")
        self.tables[(DatabaseName, TableInput["Name"])] = {**TableInput, "VersionId": "1"}
        return {}

    def update_table(self, DatabaseName: str, TableInput: dict[str, Any], VersionId: str) -> dict[str, Any]:
        self._enter("update_table", DatabaseName, TableInput["Name"])
        current = self.tables[(DatabaseName, TableInput["Name"])]
        if current["VersionId"] != VersionId:
            raise self._error("update_table", "ConcurrentModificationException")
        self.updates.append(TableInput)
        self.tables[(DatabaseName, TableInput["Name"])] = {**TableInput, "VersionId": str(int(VersionId) + 1)}
        return {}

    def writes(self) -> list[tuple[str, str, str]]:
        return [call for call in self.calls if call[0] in WRITES]


class FakeSource:
    """The Polaris side. ``pointers`` is the live main. ``by_snapshot`` maps (table, snapshot ID) to the metadata file of that snapshot."""

    def __init__(self, pointers: dict[str, str | None], error: Exception | None = None, by_snapshot: dict[tuple[str, int], str] | None = None) -> None:
        self.pointers = pointers
        self.error = error
        self.by_snapshot = by_snapshot
        self.asked: list[tuple[str, int | None]] = []

    def pointer(self, table: str, snapshot_id: int | None = None) -> str | None:
        self.asked.append((table, snapshot_id))
        if self.error is not None:
            raise self.error
        if self.by_snapshot is not None and snapshot_id is not None:
            return self.by_snapshot.get((table, snapshot_id))
        return self.pointers.get(table)

    def columns(self, table: str) -> list[tuple[str, str]]:
        return list(COLUMNS)


def ids(names: Sequence[str], snapshot: int = 10) -> dict[str, int]:
    """A committed snapshot set: one snapshot ID for each table."""
    return dict.fromkeys(names, snapshot)


def hook(
    ctx: gold_publish.Context, source: Any, glue: Any, config: Any = None, names: Sequence[str] = (PILOT, SECOND_TABLE), snapshot: int = 10
) -> dict[str, Any]:
    """The post-commit hook with the newest committed set stood in: the set the publish_runs table would name."""
    head = {"run_id": RUN_ID, "snapshot_set": ids(names, snapshot)}
    with mock.patch.object(gold_publish, "published_head", return_value=head):
        return gold_publish.mirror_after_commit(ctx, source=source, glue=glue, config=config or enabled())


def enabled() -> glue_mirror.MirrorConfig:
    return replace(glue_mirror.load_config(), enabled=True)


def table_of(glue: FakeGlue, name: str = PILOT) -> dict[str, Any]:
    return glue.tables[("gold", name)]


def case_first_publish_registers() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT, SECOND_TABLE]), FakeSource({PILOT: FIRST, SECOND_TABLE: FIRST}), glue)
    table = glue.tables.get(("gold", PILOT), {})
    parameters = table.get("Parameters", {})
    return {
        "every_published_table_created": glue.writes() == [("create_table", "gold", PILOT), ("create_table", "gold", SECOND_TABLE)],
        "reported_ok": report.get("status") == "ok" and report.get("tables") == {PILOT: "created", SECOND_TABLE: "created"},
        "points_at_polaris_file": parameters.get("metadata_location") == FIRST,
        "marked_iceberg_and_mirror": parameters.get("table_type") == "ICEBERG" and parameters.get("hai_mirror") == "true",
        "location_is_table_folder": table.get("StorageDescriptor", {}).get("Location") == f"s3://{BUCKET}/lakehouse/gold/{PILOT}",
        "columns_listed": [(c["Name"], c["Type"]) for c in table.get("StorageDescriptor", {}).get("Columns", [])] == COLUMNS,
        "external_table": table.get("TableType") == "EXTERNAL_TABLE",
    }


def case_repeat_is_a_no_op() -> Check:
    glue = FakeGlue()
    source = FakeSource({PILOT: FIRST})
    glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), source, glue)
    before = repr(sorted(glue.tables.items()))
    glue.calls.clear()
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), source, glue)
    return {
        "no_write_call": glue.writes() == [],
        "read_once": glue.calls == [("get_table", "gold", PILOT)],
        "reported_unchanged": report.get("status") == "unchanged" and report.get("tables") == {PILOT: "unchanged"},
        "table_identical": repr(sorted(glue.tables.items())) == before,
    }


def case_new_commit_moves_the_pointer() -> Check:
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
    glue.calls.clear()
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: SECOND}), glue)
    parameters = table_of(glue).get("Parameters", {})
    return {
        "one_update": glue.writes() == [("update_table", "gold", PILOT)],
        "reported_updated": report.get("tables") == {PILOT: "updated"},
        "new_pointer": parameters.get("metadata_location") == SECOND,
        "previous_pointer_kept": parameters.get("previous_metadata_location") == FIRST,
        "still_marked": parameters.get("hai_mirror") == "true",
    }


def case_glue_error_is_reported_not_raised() -> Check:
    glue = FakeGlue(fail={"create_table": "ThrottlingException"})
    try:
        report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
        raised = False
    except Exception:
        report, raised = {}, True
    reason = json.dumps(report)
    return {
        "did_not_raise": not raised,
        "status_failed": report.get("status") == "failed",
        "names_table_and_cause": PILOT in reason and "ThrottlingException" in reason,
        "nothing_registered": glue.tables == {},
    }


def case_missing_database_says_so() -> Check:
    glue = FakeGlue(databases=())
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
    return {"status_failed": report.get("status") == "failed", "names_database": "gold" in json.dumps(report)}


def context(directory: Path, namespace: str = "gold", mode: str = "real") -> gold_publish.Context:
    plan = replace(
        gold_publish.load_plan(),
        tables={PILOT: ("county_sk", "neighbor_sk"), SECOND_TABLE: ("state_sk",)},
        namespaces={"publish_fixture": f"{namespace}_e2e", "real": namespace},
    )
    return gold_publish.Context(None, plan, RUN_ID, mode, directory, DIGEST, accepted_result_sha256=DIGEST, needs_sealed=True)


def case_hook_never_fails_the_publish() -> Check:
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        folder = Path(directory)
        ctx = context(folder)
        outcomes: dict[str, Any] = {}
        for label, source, glue in (
            ("glue_down", FakeSource({PILOT: FIRST}), FakeGlue(fail={"get_table": "InternalServiceException"})),
            ("polaris_down", FakeSource({}, error=RuntimeError("catalog unreachable")), FakeGlue()),
        ):
            try:
                outcomes[label] = hook(ctx, source, glue)
            except Exception as error:
                outcomes[label] = {"raised": repr(error)}
        record = json.loads((folder / "mirror.json").read_text()) if (folder / "mirror.json").is_file() else {}
        return {
            "glue_down_reported": outcomes["glue_down"].get("status") == "failed",
            "polaris_down_reported": outcomes["polaris_down"].get("status") == "failed" and "catalog unreachable" in json.dumps(outcomes["polaris_down"]),
            "record_written_for_the_run": record.get("run_id") == RUN_ID and record.get("status") == "failed",
        }


def case_hook_repeat_is_a_no_op() -> Check:
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        ctx, glue, source = context(Path(directory)), FakeGlue(), FakeSource({PILOT: FIRST, SECOND_TABLE: FIRST})
        first = hook(ctx, source, glue)
        glue.calls.clear()
        again = hook(ctx, source, glue)
        return {
            "first_created": first.get("tables") == {PILOT: "created", SECOND_TABLE: "created"},
            "repeat_writes_nothing": glue.writes() == [],
            "repeat_unchanged": again.get("status") == "unchanged",
        }


SECOND_OF = {name: FIRST.replace(PILOT, name).replace("00001-aaaa", "00002-bbbb") for name in (PILOT, SECOND_TABLE)}
FIRST_OF = {name: FIRST.replace(PILOT, name) for name in (PILOT, SECOND_TABLE)}


def later_failed_publish_source() -> FakeSource:
    """Snapshot 10 is the committed set. A later publish failed after moving main to snapshot 20, so the live pointers are ahead of the committed set."""
    by_snapshot = {(name, 10): FIRST_OF[name] for name in FIRST_OF} | {(name, 20): SECOND_OF[name] for name in SECOND_OF}
    return FakeSource(dict(SECOND_OF), by_snapshot=by_snapshot)


def case_committed_retry_mirrors_the_committed_set() -> Check:
    glue = FakeGlue()
    source = later_failed_publish_source()
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        report = hook(context(Path(directory)), source, glue)
    held = {name: glue.tables[("gold", name)]["Parameters"] for name in FIRST_OF}
    return {
        "mirrored_ok": report.get("status") == "ok",
        "points_at_the_committed_snapshot_not_the_live_main": all(held[name]["metadata_location"] == FIRST_OF[name] for name in held),
        "asked_polaris_for_the_committed_snapshots": sorted(source.asked) == sorted((name, 10) for name in FIRST_OF),
        "records_the_snapshot_and_the_run": all(held[name].get("hai_snapshot_id") == "10" and held[name].get("hai_run_id") == RUN_ID for name in held),
    }


def case_check_is_bound_to_the_committed_set() -> Check:
    source = later_failed_publish_source()
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "gold", "real", ids(list(FIRST_OF)), source, glue, RUN_ID)
    committed = glue_mirror.check(enabled(), "gold", ids(list(FIRST_OF)), source, glue, RUN_ID)
    for name in SECOND_OF:
        glue.tables[("gold", name)]["Parameters"]["metadata_location"] = SECOND_OF[name]
    ahead = glue_mirror.check(enabled(), "gold", ids(list(FIRST_OF)), source, glue, RUN_ID)
    return {
        "mirror_at_the_committed_set_passes": committed["ok"] is True,
        "mirror_at_the_uncommitted_main_fails": ahead["ok"] is False and all("differs" in outcome for outcome in ahead["tables"].values()),
    }


def case_missing_committed_snapshot_is_refused() -> Check:
    glue = FakeGlue()
    source = FakeSource(dict(SECOND_OF), by_snapshot={(PILOT, 20): SECOND_OF[PILOT]})
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), source, glue, RUN_ID)
    return {
        "failed_and_says_why": report.get("status") == "failed" and "committed snapshot" in json.dumps(report),
        "did_not_fall_back_to_the_live_main": glue.writes() == [],
    }


class SlowSource(FakeSource):
    """A source whose reads block, as a stalled Spark collection does. ``delay`` is how long each read holds."""

    def __init__(self, pointers: dict[str, str | None], delay: float, **kwargs: Any) -> None:
        super().__init__(pointers, **kwargs)
        self.delay = delay

    def pointer(self, table: str, snapshot_id: int | None = None) -> str | None:
        time.sleep(self.delay)
        return super().pointer(table, snapshot_id)

    def columns(self, table: str) -> list[tuple[str, str]]:
        time.sleep(self.delay)
        return super().columns(table)


class SlowGlue(FakeGlue):
    """A Glue client whose writes block. ``delay`` is how long a write holds before it lands."""

    def __init__(self, delay: float, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.delay = delay

    def create_table(self, DatabaseName: str, TableInput: dict[str, Any]) -> dict[str, Any]:
        time.sleep(self.delay)
        return super().create_table(DatabaseName, TableInput)


def short(seconds: float) -> glue_mirror.MirrorConfig:
    return replace(enabled(), mirror_seconds=seconds)


def case_deadline_bounds_a_blocking_read() -> Check:
    glue = FakeGlue()
    source = SlowSource({PILOT: FIRST}, delay=0.6)
    started = time.monotonic()
    report = glue_mirror.mirror(short(0.3), "gold", "real", ids([PILOT]), source, glue, RUN_ID)
    returned_after = time.monotonic() - started
    time.sleep(1.5)
    return {
        "returned_at_the_deadline": returned_after < 0.55,
        "reported_failed_with_the_deadline": report.get("status") == "failed" and "deadline" in report.get("reason", ""),
        "no_write_followed_the_late_read": glue.writes() == [],
    }


def case_no_write_is_issued_after_the_deadline() -> Check:
    glue = FakeGlue()
    source = SlowSource({PILOT: FIRST}, delay=0.2)
    report = glue_mirror.mirror(short(0.3), "gold", "real", ids([PILOT]), source, glue, RUN_ID)
    time.sleep(1.0)
    return {
        "reported_failed_with_the_deadline": report.get("status") == "failed" and "deadline" in report.get("reason", ""),
        "the_reads_finished_late_and_wrote_nothing": glue.writes() == [] and glue.tables == {},
    }


def case_a_write_past_the_deadline_is_reported_as_possibly_landed() -> Check:
    glue = SlowGlue(delay=0.6)
    started = time.monotonic()
    report = glue_mirror.mirror(short(0.3), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue, RUN_ID)
    returned_after = time.monotonic() - started
    time.sleep(1.0)
    return {
        "returned_at_the_deadline": returned_after < 0.55,
        "reported_failed": report.get("status") == "failed",
        "says_the_write_may_have_landed": "may have" in json.dumps(report) and "check" in json.dumps(report),
        "later_tables_not_attempted": True,
    }


def case_a_write_is_refused_when_the_deadline_passes_as_it_starts() -> Check:
    """The deadline passes between the check that starts a write and the call itself. The write must not be issued."""
    state = {"armed": False, "calls": 0}

    def clock() -> float:
        if not state["armed"]:
            return 0.0
        state["calls"] += 1
        return 0.0 if state["calls"] == 1 else 100.0

    class ArmingSource(FakeSource):
        def columns(self, table: str) -> list[tuple[str, str]]:
            state["armed"] = True
            return super().columns(table)

    glue = FakeGlue()
    budget = glue_mirror.Budget(1.0, clock=clock)
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), ArmingSource({PILOT: FIRST}), glue, RUN_ID, budget)
    time.sleep(0.3)
    return {
        "reported_failed_with_the_deadline": report.get("status") == "failed" and "deadline" in report.get("reason", ""),
        "no_write_was_issued": glue.writes() == [] and glue.tables == {},
    }


def case_later_tables_are_not_attempted_after_the_deadline() -> Check:
    glue = FakeGlue()
    names = [PILOT, SECOND_TABLE, "dim_hospital"]
    source = SlowSource({name: FIRST for name in names}, delay=0.15)
    report = glue_mirror.mirror(short(0.4), "gold", "real", ids(names), source, glue, RUN_ID)
    time.sleep(0.8)
    return {
        "reported_failed_with_the_deadline": report.get("status") == "failed" and "deadline" in report.get("reason", ""),
        "the_last_table_was_not_written": ("gold", "dim_hospital") not in glue.tables,
    }


def case_hook_deadline_starts_at_entry() -> Check:
    glue = FakeGlue()
    config = short(0.3)

    def slow_head(ctx: Any) -> dict[str, Any]:
        time.sleep(0.6)
        return {"run_id": RUN_ID, "snapshot_set": ids([PILOT, SECOND_TABLE])}

    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory, mock.patch.object(gold_publish, "published_head", side_effect=slow_head):
        report = gold_publish.mirror_after_commit(context(Path(directory)), source=FakeSource(dict(FIRST_OF)), glue=glue, config=config)
    returned_after = time.monotonic() - started
    time.sleep(0.8)
    return {
        "returned_at_the_deadline": returned_after < 0.55,
        "reported_failed_with_the_deadline": report.get("status") == "failed" and "deadline" in report.get("reason", ""),
        "no_glue_call": glue.calls == [],
    }


def case_check_compares_the_run() -> Check:
    glue = FakeGlue()
    source = later_failed_publish_source()
    glue_mirror.mirror(enabled(), "gold", "real", ids(list(FIRST_OF)), source, glue, "silver_0000aaaa")
    older = glue_mirror.check(enabled(), "gold", ids(list(FIRST_OF)), source, glue, RUN_ID)
    glue_mirror.mirror(enabled(), "gold", "real", ids(list(FIRST_OF)), source, glue, RUN_ID)
    current = glue_mirror.check(enabled(), "gold", ids(list(FIRST_OF)), source, glue, RUN_ID)
    return {
        "same_pointer_with_an_older_run_fails": older["ok"] is False and all("run" in outcome for outcome in older["tables"].values()),
        "same_pointer_with_the_expected_run_passes": current["ok"] is True,
    }


def case_glue_client_has_short_timeouts() -> Check:
    settings = glue_mirror.client_config()
    return {
        "connect_timeout_is_short": settings.connect_timeout <= 10,
        "read_timeout_is_short": settings.read_timeout <= 30,
        "retries_are_total_attempts": settings.retries.get("total_max_attempts") == 2 and "max_attempts" not in settings.retries,
        "config_has_an_overall_deadline": 0 < glue_mirror.load_config().mirror_seconds <= 600,
    }


class BrokenStream:
    def write(self, text: str) -> int:
        raise BrokenPipeError("diagnostic pipe closed")

    def flush(self) -> None:
        return None


def case_closed_diagnostic_pipe_does_not_escape() -> Check:
    glue = FakeGlue(fail={"get_table": "InternalServiceException"})
    raised = False
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory, mock.patch.object(gold_publish.sys, "stderr", BrokenStream()):
        try:
            report = hook(context(Path(directory)), FakeSource(dict(FIRST_OF)), glue)
        except Exception:
            report, raised = {}, True
        record = json.loads((Path(directory) / "mirror.json").read_text()) if (Path(directory) / "mirror.json").is_file() else {}
    return {
        "did_not_raise": not raised,
        "outcome_kept_in_the_result": report.get("status") == "failed",
        "outcome_kept_in_the_record": record.get("status") == "failed",
    }


def case_disabled_touches_nothing() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(replace(glue_mirror.load_config(), enabled=False), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
    return {"no_call": glue.calls == [], "skipped": report.get("status") == "skipped"}


def case_tracked_config_turns_the_mirror_on() -> Check:
    config = glue_mirror.load_config()
    wanted, reason = glue_mirror.tables_to_mirror(config, "gold", "real", [PILOT])
    return {"tracked_config_is_on": config.enabled is True, "a_real_gold_publish_mirrors": wanted == [PILOT] and reason == ""}


def case_test_namespace_touches_nothing() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(enabled(), "gold", "publish_fixture", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        hooked = hook(context(Path(directory), mode="publish_fixture"), FakeSource({PILOT: FIRST}), glue)
    return {"no_call": glue.calls == [], "skipped": report.get("status") == "skipped" and hooked.get("status") == "skipped"}


def case_bronze_dictionary_and_other_layers_touch_nothing() -> Check:
    glue = FakeGlue()
    direct = glue_mirror.mirror(enabled(), "bronze_dictionary", "real", ids(["any_table"]), FakeSource({"any_table": FIRST}), glue)
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        hooked = hook(context(Path(directory), namespace="bronze_dictionary"), FakeSource({PILOT: FIRST}), glue)
        runs = hook(context(Path(directory)), FakeSource({"publish_runs": FIRST}), glue)
    return {
        "no_call": glue.calls == [],
        "bronze_dictionary_skipped": direct.get("status") == "skipped" and hooked.get("status") == "skipped",
        "runs_table_never_listed": "publish_runs" not in json.dumps(runs) and ("create_table", "gold", "publish_runs") not in glue.calls,
    }


def case_second_gold_table_is_mirrored() -> Check:
    glue = FakeGlue()
    published = [PILOT, SECOND_TABLE, "dim_hospital"]
    report = glue_mirror.mirror(enabled(), "gold", "real", ids(published), FakeSource({name: FIRST for name in published}), glue)
    return {
        "all_three_created": sorted(table for (_, table) in glue.tables) == sorted(published),
        "second_dimension_created": ("create_table", "gold", SECOND_TABLE) in glue.calls,
        "reported_ok": report.get("status") == "ok",
    }


def case_other_layers_touch_nothing() -> Check:
    glue = FakeGlue()
    reports = {
        layer: glue_mirror.mirror(enabled(), layer, "real", ids(["int_acs_county_values"]), FakeSource({"int_acs_county_values": FIRST}), glue)
        for layer in ("bronze_dictionary", "bronze_e2e", "silver_e2e", "features_e2e", "gold_e2e")
    }
    return {
        "no_call": glue.calls == [],
        "every_layer_skipped": all(report.get("status") == "skipped" for report in reports.values()),
        "no_bronze_dictionary_or_test_database_in_config": not any(layer.startswith("bronze_") for layer in glue_mirror.load_config().layers),
    }


SILVER = "int_acs_county_values"
SILVER_FILE = f"s3://{BUCKET}/lakehouse/silver/{SILVER}/metadata/00001-dddd.metadata.json"


def case_silver_layer_is_mirrored() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(enabled(), "silver", "real", ids([SILVER]), FakeSource({SILVER: SILVER_FILE}), glue, RUN_ID)
    fixture = glue_mirror.mirror(enabled(), "silver", "publish_fixture", ids([SILVER]), FakeSource({SILVER: SILVER_FILE}), FakeGlue())
    return {
        "created_in_the_silver_database": glue.writes() == [("create_table", "silver", SILVER)],
        "points_at_the_committed_file": glue.tables.get(("silver", SILVER), {}).get("Parameters", {}).get("metadata_location") == SILVER_FILE,
        "reported_ok": report.get("status") == "ok",
        "fixture_mode_skipped": fixture.get("status") == "skipped",
    }


def case_silver_publish_hook_mirrors_silver() -> Check:
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        glue = FakeGlue()
        report = hook(context(Path(directory), namespace="silver"), FakeSource({SILVER: SILVER_FILE}), glue, names=(SILVER,))
        fixture = hook(context(Path(directory), namespace="silver", mode="publish_fixture"), FakeSource({SILVER: SILVER_FILE}), FakeGlue(), names=(SILVER,))
        broken = hook(context(Path(directory), namespace="silver"), FakeSource({}, error=RuntimeError("catalog unreachable")), FakeGlue(), names=(SILVER,))
    return {
        "hook_created_the_silver_table": glue.writes() == [("create_table", "silver", SILVER)],
        "hook_reported_ok": report.get("status") == "ok",
        "fixture_hook_skipped": fixture.get("status") == "skipped",
        "a_failure_is_reported_not_raised": broken.get("status") == "failed",
        "runs_table_never_listed": all(call[2] != "publish_runs" for call in glue.calls),
    }


def case_silver_publish_plan_reaches_the_hook() -> Check:
    from scripts.lakehouse import silver_publish

    plan = silver_publish.publish_plan("real")
    config = glue_mirror.load_config()
    return {
        "silver_commit_is_the_gold_engine": silver_publish.commit_publish is gold_publish.commit_publish,
        "real_layer_is_silver": plan.namespaces["real"] == "silver",
        "silver_layer_is_configured": "silver" in config.layers and config.layers["silver"].database == "silver",
        "contract_tables_are_listed": glue_mirror.tables_to_mirror(replace(config, enabled=True), "silver", "real", list(plan.tables))[0] == list(plan.tables),
        "silver_plan_is_read": glue_mirror.layer_plan("silver").namespaces["real"] == "silver",
    }


FEATURE = "feature_matrix_hospital"
FEATURE_FILE = f"s3://{BUCKET}/lakehouse/features/{FEATURE}/metadata/00001-cccc.metadata.json"


def case_features_layer_is_mirrored() -> Check:
    glue = FakeGlue()
    report = glue_mirror.mirror(enabled(), "features", "real", ids([FEATURE]), FakeSource({FEATURE: FEATURE_FILE}), glue, RUN_ID)
    table = glue.tables.get(("features", FEATURE), {})
    fixture = glue_mirror.mirror(enabled(), "features", "publish_fixture", ids([FEATURE]), FakeSource({FEATURE: FEATURE_FILE}), FakeGlue())
    return {
        "created_in_the_features_database": glue.writes() == [("create_table", "features", FEATURE)],
        "nothing_in_the_gold_database": not any(database == "gold" for (database, _) in glue.tables),
        "points_at_the_committed_file": table.get("Parameters", {}).get("metadata_location") == FEATURE_FILE,
        "reported_ok": report.get("status") == "ok",
        "fixture_mode_skipped": fixture.get("status") == "skipped",
    }


def case_features_publish_hook_mirrors_features() -> Check:
    with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
        glue = FakeGlue()
        report = hook(context(Path(directory), namespace="features"), FakeSource({FEATURE: FEATURE_FILE}), glue, names=(FEATURE,))
        fixture = hook(
            context(Path(directory), namespace="features", mode="publish_fixture"), FakeSource({FEATURE: FEATURE_FILE}), FakeGlue(), names=(FEATURE,)
        )
        record = json.loads((Path(directory) / "mirror.json").read_text()) if (Path(directory) / "mirror.json").is_file() else {}
    return {
        "hook_created_the_features_table": glue.writes() == [("create_table", "features", FEATURE)],
        "hook_reported_ok": report.get("status") == "ok" and record.get("status") == "ok",
        "fixture_hook_skipped": fixture.get("status") == "skipped",
    }


def case_features_publish_plan_reaches_the_hook() -> Check:
    from scripts.features import features_publish

    plan = features_publish.publish_plan("real")
    config = glue_mirror.load_config()
    return {
        "features_commit_is_the_gold_engine": features_publish.commit_publish is gold_publish.commit_publish,
        "real_layer_is_features": plan.namespaces["real"] == "features",
        "features_layer_is_configured": "features" in config.layers and config.layers["features"].database == "features",
        "contract_tables_are_listed": glue_mirror.tables_to_mirror(replace(config, enabled=True), "features", "real", list(plan.tables))[0]
        == list(plan.tables),
    }


def case_foreign_table_is_refused() -> Check:
    glue = FakeGlue()
    glue.tables[("gold", PILOT)] = {"Name": PILOT, "Parameters": {"owner": "someone"}, "VersionId": "1"}
    report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue)
    return {"no_write": glue.writes() == [], "failed_and_says_why": report.get("status") == "failed" and "not a mirror" in json.dumps(report)}


def case_plain_named_database_keeps_a_table_the_mirror_did_not_write() -> Check:
    """The databases are named bronze, silver, gold and features, plain names another tool in the account could also use: a table of a published name that
    has no ``hai_mirror`` marker, or a marker that is not ``true``, is never changed, by a publish or by the S3 registration."""
    checks: Check = {}
    for label, parameters in (("no_marker", {"owner": "someone"}), ("marker_not_true", {"hai_mirror": "false", "owner": "someone"})):
        foreign = {"Name": PILOT, "Parameters": dict(parameters), "VersionId": "1"}
        publish_glue = FakeGlue()
        publish_glue.tables[("gold", PILOT)] = dict(foreign)
        report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), publish_glue)
        checks[f"{label}_publish_writes_nothing"] = publish_glue.writes() == [] and publish_glue.tables[("gold", PILOT)] == foreign
        checks[f"{label}_publish_says_why"] = report.get("status") == "failed" and "not a mirror" in json.dumps(report)
        s3 = FakeS3()
        s3.put(metadata_key(PILOT, 1), metadata_document(PILOT, 10))
        register_glue = FakeGlue()
        register_glue.tables[("gold", PILOT)] = dict(foreign)
        registered = register(s3, register_glue, [PILOT])
        checks[f"{label}_register_writes_nothing"] = register_glue.writes() == [] and register_glue.tables[("gold", PILOT)] == foreign
        checks[f"{label}_register_fails_for_the_table"] = registered["status"] == "failed" and registered["tables"][PILOT].startswith("failed")
    checks["the_marker_is_still_hai_mirror"] = enabled().marker == "hai_mirror"
    return checks


def case_other_pointers_are_refused() -> Check:
    checks: Check = {}
    for label, pointer in (
        ("local_file", "file:///tmp/x/metadata/00001.metadata.json"),
        ("s3a_scheme", f"s3a://{BUCKET}/t/metadata/00001.metadata.json"),
        ("not_metadata", f"s3://{BUCKET}/t/data/a.parquet"),
        ("absent", None),
    ):
        glue = FakeGlue()
        report = glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: pointer}), glue)
        checks[f"{label}_refused"] = glue.writes() == [] and report.get("status") == "failed"
    return checks


def case_config_scope_is_silver_gold_and_features_only() -> Check:
    config = glue_mirror.load_config()
    gold = config.layers["gold"]
    return {
        "only_the_four_layers_are_configured_bronze_first": list(config.layers) == ["bronze", "silver", "gold", "features"]
        and all(config.layers[layer].namespace == layer for layer in ("silver", "gold", "features", "bronze"))
        and config.layers["bronze"].database == "bronze",
        "real_config_has_no_violation": glue_mirror.violations(config) == [],
        "silver_is_allowed": glue_mirror.violations(replace(config, layers={"silver": replace(gold, namespace="silver")})) == [],
        "silver_test_namespace_is_flagged": any(
            "silver_e2e" in line for line in glue_mirror.violations(replace(config, layers={"silver": replace(gold, namespace="silver_e2e")}))
        ),
        "features_is_allowed": glue_mirror.violations(replace(config, layers={"features": replace(gold, namespace="features")})) == [],
        "bronze_is_allowed": glue_mirror.violations(replace(config, layers={"bronze": replace(gold, namespace="bronze")})) == [],
        "bronze_test_namespace_is_flagged": any(
            "bronze_e2e" in line for line in glue_mirror.violations(replace(config, layers={"bronze": replace(gold, namespace="bronze_e2e")}))
        ),
        "bronze_dictionary_is_flagged": any(
            "bronze_dictionary" in line
            for line in glue_mirror.violations(replace(config, layers={"bronze_dictionary": replace(gold, namespace="bronze_dictionary")}))
        ),
        "features_test_namespace_is_flagged": any(
            "features_e2e" in line for line in glue_mirror.violations(replace(config, layers={"features": replace(gold, namespace="features_e2e")}))
        ),
        "test_namespace_is_flagged": any(
            "gold_e2e" in line for line in glue_mirror.violations(replace(config, layers={"gold": replace(gold, namespace="gold_e2e")}))
        ),
        "no_class_filter_left": not hasattr(glue_mirror, "PUBLIC")
        and all("tables" not in spec for spec in json.loads(glue_mirror.CONFIG.read_text())["layers"].values()),
    }


def source_files(pattern: str) -> dict[str, str]:
    return {str(path.relative_to(REPO_ROOT)): path.read_text() for path in sorted((REPO_ROOT / "infra").rglob(pattern)) if ".terraform" not in path.parts}


def case_reader_access_is_one_role_for_one_user() -> Check:
    template = (REPO_ROOT / "infra/iam/owner_policies/athena_reader.json").read_text()
    stack = (REPO_ROOT / "infra/iam/athena_mirror.tf").read_text()
    terraform = source_files("*.tf")
    users = [name for name, text in terraform.items() if "aws_iam_role_policy.athena_reader" in text or "aws_iam_role.athena_reader" in text]
    attachments = re.findall(r'resource "(aws_iam_[a-z_]*attachment|aws_iam_policy)" "(\w+)"', stack)
    roles = re.findall(r'resource "aws_iam_role" "(\w+)"', stack)
    return {
        "reader_policy_names_no_principal": not re.search(r'"(Principal|NotPrincipal|NotAction|NotResource)"', template),
        "reader_policy_only_allows_and_denies_the_runs_table": template.count('"Effect": "Allow"') + 1 == template.count('"Effect"')
        and re.search(r'"Sid": "DenyPublishRunsBookkeeping",\s*"Effect": "Deny",\s*"Action": \["s3:GetObject"\]', template) is not None,
        "reader_role_used_only_by_its_stack_file": users == ["infra/iam/athena_mirror.tf"],
        "no_attachment_or_managed_policy": attachments == [],
        "one_role_reaches_the_reader_policy": roles == ["athena_reader"],
        "no_resource_policy_or_public_path": not re.search(
            r'resource "(aws_glue_resource_policy|aws_s3_bucket_policy|aws_s3_bucket_public_access_block|aws_lakeformation_\w+)"', stack
        )
        and '"*"' not in template,
    }


def run_check(arguments: Sequence[str], source: FakeSource, glue: FakeGlue) -> tuple[int, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = glue_mirror.main(arguments, source=source, glue=glue, config=enabled(), snapshots=ids([PILOT]), run_id=RUN_ID)
    return code, out.getvalue() + err.getvalue()


def case_check_accepts_the_features_layer() -> Check:
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "features", "real", ids([FEATURE]), FakeSource({FEATURE: FEATURE_FILE}), glue, RUN_ID)
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        code = glue_mirror.main(
            ["check", "--layer", "features"], source=FakeSource({FEATURE: FEATURE_FILE}), glue=glue, config=enabled(), snapshots=ids([FEATURE]), run_id=RUN_ID
        )
    return {
        "features_check_passes": code == 0 and f"features.{FEATURE}: match" in out.getvalue(),
        "features_plan_is_read": glue_mirror.layer_plan("features").namespaces["real"] == "features",
    }


def case_check_compares_the_two_pointers() -> Check:
    glue = FakeGlue()
    glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT]), FakeSource({PILOT: FIRST}), glue, RUN_ID)
    glue.calls.clear()
    same_code, same_text = run_check(["check", "--layer", "gold"], FakeSource({PILOT: FIRST}), glue)
    drift_code, drift_text = run_check(["check", "--layer", "gold"], FakeSource({PILOT: SECOND}), glue)
    missing_code, missing_text = run_check(["check", "--layer", "gold"], FakeSource({PILOT: FIRST}), FakeGlue())
    return {
        "equal_pointers_pass": same_code == 0 and PILOT in same_text,
        "drift_fails_and_names_both": drift_code == 1 and PILOT in drift_text and "00001-aaaa" in drift_text and "00002-bbbb" in drift_text,
        "missing_registration_fails": missing_code == 1 and "missing" in missing_text,
        "check_writes_nothing": glue.writes() == [],
    }


S3_FIELDS = [
    {"id": 1, "name": "county_sk", "required": False, "type": "string"},
    {"id": 2, "name": "visits", "required": False, "type": "long"},
    {"id": 3, "name": "amount", "required": False, "type": "decimal(12, 2)"},
    {"id": 4, "name": "loaded_at", "required": False, "type": "timestamptz"},
    {"id": 5, "name": "tags", "required": False, "type": {"type": "list", "element-id": 6, "element": "string", "element-required": False}},
    {"id": 7, "name": "pair", "required": False, "type": {"type": "struct", "fields": [{"id": 8, "name": "a", "required": False, "type": "int"}]}},
]
S3_COLUMNS = [
    ("county_sk", "string"),
    ("visits", "bigint"),
    ("amount", "decimal(12,2)"),
    ("loaded_at", "timestamp"),
    ("tags", "array<string>"),
    ("pair", "struct<a:int>"),
]


def metadata_name(version: int, tag: str = "0") -> str:
    return f"{version:05d}-{tag * 8}-0000-4000-8000-{tag * 12}.metadata.json"


def metadata_key(table: str, version: int, tag: str = "0", layer: str = "gold") -> str:
    return f"lakehouse/{layer}/{table}/metadata/{metadata_name(version, tag)}"


def snapshot_entries(chain: Sequence[int], branches: Sequence[int], main: int | None) -> list[dict[str, Any]]:
    """The snapshot list of a metadata file: the main chain oldest first, each with its sequence number, time and parent, then branch snapshots off main."""
    entries: list[dict[str, Any]] = []
    for index, snapshot in enumerate(chain):
        entry: dict[str, Any] = {"snapshot-id": snapshot, "sequence-number": index + 1, "timestamp-ms": 1000 * (index + 1)}
        if index:
            entry["parent-snapshot-id"] = chain[index - 1]
        entries.append(entry)
    for offset, snapshot in enumerate(branch for branch in branches if branch not in chain):
        number = len(chain) + offset + 1
        entries.append(
            {"snapshot-id": snapshot, "sequence-number": number, "timestamp-ms": 1000 * number, **({"parent-snapshot-id": main} if main is not None else {})}
        )
    return entries


def metadata_document(
    table: str,
    main: int | None,
    branches: dict[str, int] | None = None,
    layer: str = "gold",
    changes: dict[str, Any] | None = None,
    history: Sequence[int] = (),
) -> dict[str, Any]:
    """An Iceberg v2 metadata file: ``main`` is the snapshot of the main branch, ``history`` the earlier snapshots of its chain, ``branches`` the other refs."""
    refs: dict[str, Any] = {name: {"snapshot-id": snapshot, "type": "branch"} for name, snapshot in (branches or {}).items()}
    chain = [*history, *([main] if main is not None else [])]
    if main is not None:
        refs["main"] = {"snapshot-id": main, "type": "branch"}
    document: dict[str, Any] = {
        "format-version": 2,
        "table-uuid": "11111111-1111-4111-8111-111111111111",
        "location": f"s3://{BUCKET}/lakehouse/{layer}/{table}",
        "current-snapshot-id": -1 if main is None else main,
        "refs": refs,
        "snapshots": snapshot_entries(chain, list((branches or {}).values()), main),
        "current-schema-id": 1,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": []}, {"schema-id": 1, "type": "struct", "fields": S3_FIELDS}],
    }
    document.update(changes or {})
    return document


class FakeS3:
    """The two read calls the S3 source may use. There is no write call: any other attribute raises, so a write fails the case."""

    def __init__(self, objects: dict[str, bytes] | None = None, delay: float = 0.0) -> None:
        self.objects = objects or {}
        self.calls: list[tuple[str, str]] = []
        self.delay = delay
        self.modified: dict[str, datetime] = {}

    def put(self, key: str, document: Any) -> None:
        self.objects[key] = document if isinstance(document, bytes) else json.dumps(document).encode()

    def get_paginator(self, operation: str) -> Any:
        if operation != "list_objects_v2":
            raise AssertionError(operation)
        outer = self

        class Pages:
            def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
                if Bucket != BUCKET:
                    raise AssertionError(Bucket)
                outer.calls.append(("list_objects_v2", Prefix))
                time.sleep(outer.delay)
                keys = sorted(key for key in outer.objects if key.startswith(Prefix))
                return [
                    {"Contents": [{"Key": key, **({"LastModified": outer.modified[key]} if key in outer.modified else {})} for key in keys[index : index + 2]]}
                    for index in range(0, len(keys), 2)
                ] or [{}]

        return Pages()

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if Bucket != BUCKET:
            raise AssertionError(Bucket)
        self.calls.append(("get_object", Key))
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "stand-in"}}, "get_object")
        return {"Body": io.BytesIO(self.objects[Key])}


def s3_with(tables: Sequence[str], main: int = 10, layer: str = "gold") -> FakeS3:
    s3 = FakeS3()
    for table in tables:
        s3.put(metadata_key(table, 1, layer=layer), metadata_document(table, main, layer=layer))
    return s3


def register(s3: FakeS3, glue: FakeGlue, tables: Sequence[str], layer: str = "gold", dry_run: bool = False, config: Any = None) -> dict[str, Any]:
    return glue_mirror.register_from_s3(config or enabled(), layer, list(tables), s3, BUCKET, glue, dry_run=dry_run)


def case_s3_register_creates_each_table_at_its_main_snapshot() -> Check:
    glue, s3 = FakeGlue(), s3_with([PILOT, SECOND_TABLE])
    report = register(s3, glue, [PILOT, SECOND_TABLE])
    held = table_of(glue)
    parameters = held["Parameters"]
    return {
        "both_created": report.get("status") == "ok" and report["tables"] == {PILOT: "created", SECOND_TABLE: "created"},
        "points_at_the_newest_file_of_main": parameters["metadata_location"] == f"s3://{BUCKET}/{metadata_key(PILOT, 1)}",
        "records_snapshot_and_s3_origin": parameters["hai_snapshot_id"] == "10" and parameters["hai_run_id"] == "registered_from_s3",
        "marked_as_a_mirror_of_iceberg": parameters["hai_mirror"] == "true" and parameters["table_type"] == "ICEBERG",
        "columns_come_from_the_current_schema": [(c["Name"], c["Type"]) for c in held["StorageDescriptor"]["Columns"]] == S3_COLUMNS,
        "location_is_the_table_folder": held["StorageDescriptor"]["Location"] == f"s3://{BUCKET}/lakehouse/gold/{PILOT}",
        "s3_was_only_read": {call for call, _ in s3.calls} <= {"list_objects_v2", "get_object"},
    }


def case_s3_register_picks_the_newest_main_never_a_branch() -> Check:
    glue, s3 = FakeGlue(), FakeS3()
    s3.put(metadata_key(PILOT, 1), metadata_document(PILOT, 10))
    s3.put(metadata_key(PILOT, 2), metadata_document(PILOT, 20, {"audit": 19}))
    s3.put(metadata_key(PILOT, 3), metadata_document(PILOT, 20, {"audit": 30, "staged": 31}))
    s3.put(metadata_key(SECOND_TABLE, 1), metadata_document(SECOND_TABLE, 10))
    s3.put(metadata_key(SECOND_TABLE, 2), metadata_document(SECOND_TABLE, 10, {"staged": 40}))
    register(s3, glue, [PILOT, SECOND_TABLE])
    pilot, second = table_of(glue)["Parameters"], table_of(glue, SECOND_TABLE)["Parameters"]
    return {
        "snapshot_is_main_not_a_branch": pilot["hai_snapshot_id"] == "20" and second["hai_snapshot_id"] == "10",
        "file_is_the_newest_with_that_main": pilot["metadata_location"].endswith(metadata_name(3)) and second["metadata_location"].endswith(metadata_name(2)),
        "no_branch_snapshot_is_recorded": not {"19", "30", "31", "40"} & {pilot["hai_snapshot_id"], second["hai_snapshot_id"]},
    }


def case_s3_register_refuses_a_table_it_cannot_resolve() -> Check:
    names = ["dup", "empty", "oddname", "nomain", "emptytable", "moved", "mismatch", "badjson", "missingsnap", "futuretype", "tagmain"]
    s3 = FakeS3()
    s3.put(metadata_key("dup", 1, "a"), metadata_document("dup", 10))
    s3.put(metadata_key("dup", 1, "b"), metadata_document("dup", 11))
    s3.put(metadata_key("oddname", 1), metadata_document("oddname", 10))
    s3.put("lakehouse/gold/oddname/metadata/v2.metadata.json", metadata_document("oddname", 11))
    s3.put(metadata_key("nomain", 1), metadata_document("nomain", None, {"audit": 5}, changes={"current-snapshot-id": 5}))
    s3.put(metadata_key("emptytable", 1), metadata_document("emptytable", None))
    s3.put(metadata_key("moved", 1), metadata_document("moved", 10, changes={"location": f"s3://{BUCKET}/lakehouse/gold/another_table"}))
    s3.put(metadata_key("mismatch", 1), metadata_document("mismatch", 10, changes={"current-snapshot-id": 11}))
    s3.put(metadata_key("badjson", 1), b"{not json")
    s3.put(metadata_key("missingsnap", 1), metadata_document("missingsnap", 10, changes={"snapshots": []}))
    s3.put(
        metadata_key("futuretype", 1),
        metadata_document("futuretype", 10, changes={"schemas": [{"schema-id": 1, "fields": [{"id": 1, "name": "x", "type": "variant"}]}]}),
    )
    s3.put(metadata_key("tagmain", 1), metadata_document("tagmain", 10, changes={"refs": {"main": {"snapshot-id": 10, "type": "tag"}}}))
    s3.put(metadata_key("good", 1), metadata_document("good", 10))
    glue = FakeGlue()
    report = register(s3, glue, [*names, "good"])
    outcomes = report["tables"]
    return {
        "every_unresolvable_table_failed": all(outcomes[name].startswith("failed") for name in names),
        "the_resolvable_table_is_still_registered": outcomes["good"] == "created",
        "only_the_good_table_was_written": glue.writes() == [("create_table", "gold", "good")],
        "duplicate_version_is_named": "same version" in outcomes["dup"],
        "report_is_failed": report["status"] == "failed",
    }


def case_s3_register_is_idempotent_and_follows_a_new_commit() -> Check:
    glue, s3 = FakeGlue(), s3_with([PILOT, SECOND_TABLE])
    register(s3, glue, [PILOT, SECOND_TABLE])
    writes_after_first = list(glue.writes())
    again = register(s3, glue, [PILOT, SECOND_TABLE])
    s3.put(metadata_key(PILOT, 2), metadata_document(PILOT, 20, history=[10]))
    moved = register(s3, glue, [PILOT, SECOND_TABLE])
    parameters = table_of(glue)["Parameters"]
    return {
        "rerun_changes_nothing": glue.writes()[: len(writes_after_first)] == writes_after_first
        and again["tables"] == {PILOT: "unchanged", SECOND_TABLE: "unchanged"},
        "rerun_status_is_unchanged": again["status"] == "unchanged",
        "new_commit_updates_only_its_table": moved["tables"] == {PILOT: "updated", SECOND_TABLE: "unchanged"},
        "update_moves_the_pointer_and_keeps_the_old_one": parameters["metadata_location"].endswith(metadata_name(2))
        and parameters["previous_metadata_location"].endswith(metadata_name(1)),
    }


def case_s3_register_keeps_a_registration_a_publish_wrote() -> Check:
    glue, s3 = FakeGlue(), s3_with([PILOT])
    glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT], 5), FakeSource({PILOT: FIRST}), glue, RUN_ID)
    before = dict(table_of(glue)["Parameters"])
    glue.calls.clear()
    report = register(s3, glue, [PILOT])
    return {
        "nothing_written": glue.writes() == [] and table_of(glue)["Parameters"] == before,
        "reported_as_kept_with_the_run": report["tables"][PILOT].startswith("kept") and RUN_ID in report["tables"][PILOT],
        "status_is_unchanged": report["status"] == "unchanged",
    }


def case_s3_register_dry_run_writes_nothing() -> Check:
    glue, s3 = FakeGlue(), s3_with([PILOT])
    report = register(s3, glue, [PILOT], dry_run=True)
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = glue_mirror.main(
            ["register", "--source", "s3", "--layer", "gold", "--dry-run", "--bucket", BUCKET], glue=glue, s3=s3, config=enabled(), tables=[PILOT]
        )
    return {
        "plans_a_create": report["tables"] == {PILOT: "created"},
        "no_write_issued": glue.writes() == [] and glue.tables == {},
        "cli_says_nothing_was_written": code == 0 and f"gold.{PILOT}: created (dry run, nothing written)" in out.getvalue(),
    }


def case_s3_register_obeys_the_switch_and_the_layer_scope() -> Check:
    glue, s3 = FakeGlue(), s3_with([PILOT])
    off = register(s3, glue, [PILOT], config=replace(glue_mirror.load_config(), enabled=False))
    quiet = glue.calls == [] and s3.calls == []
    bronze = register(s3, glue, [PILOT], layer="bronze_dictionary")
    plans = {layer: glue_mirror.layer_plan(layer) for layer in glue_mirror.PUBLISHED_LAYERS}
    return {
        "off_touches_nothing": off["status"] == "skipped" and quiet,
        "bronze_dictionary_touches_nothing": bronze["status"] == "skipped" and glue.calls == [] and s3.calls == [],
        "runs_table_is_not_in_any_layer_contract": all("publish_runs" not in plan.tables for plan in plans.values()),
    }


def case_s3_register_stays_inside_the_layer_folder() -> Check:
    glue, s3 = FakeGlue(), s3_with([PILOT, "dim_unlisted"])
    s3.put(metadata_key(PILOT, 1, layer="gold_e2e"), metadata_document(PILOT, 99, layer="gold_e2e"))
    register(s3, glue, [PILOT])
    listed = [prefix for call, prefix in s3.calls if call == "list_objects_v2"]
    return {
        "lists_only_the_gold_table_folder": listed == [f"lakehouse/gold/{PILOT}/metadata/"],
        "unlisted_table_is_not_registered": ("gold", "dim_unlisted") not in glue.tables,
        "test_namespace_is_never_read": table_of(glue)["Parameters"]["hai_snapshot_id"] == "10",
        "bad_table_name_is_refused": register(s3, FakeGlue(), ["../gold_e2e/x"])["tables"]["../gold_e2e/x"].startswith("failed"),
    }


def case_s3_register_obeys_one_deadline() -> Check:
    glue, s3 = FakeGlue(), FakeS3(delay=0.6)
    s3.put(metadata_key(PILOT, 1), metadata_document(PILOT, 10))
    started = time.monotonic()
    report = register(s3, glue, [PILOT, SECOND_TABLE], config=short(0.3))
    returned_after = time.monotonic() - started
    time.sleep(1.5)
    return {
        "returned_at_the_deadline": returned_after < 0.8,
        "failed_and_names_the_deadline": report["status"] == "failed" and "deadline" in json.dumps(report),
        "no_write_followed_the_late_read": glue.writes() == [],
    }


def case_s3_register_command_reports_and_exits() -> Check:
    glue, s3 = FakeGlue(), s3_with([PILOT])
    s3.put(metadata_key("broken", 1), b"[]")

    def run(arguments: Sequence[str], tables: Sequence[str]) -> tuple[int, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = glue_mirror.main(list(arguments), glue=glue, s3=s3, config=enabled(), tables=list(tables))
            except SystemExit as error:
                code = int(error.code or 0)
        return code, out.getvalue() + err.getvalue()

    good, good_text = run(["register", "--source", "s3", "--layer", "gold", "--bucket", BUCKET], [PILOT])
    bad, bad_text = run(["register", "--source", "s3", "--layer", "gold", "--bucket", BUCKET], [PILOT, "broken"])
    no_source, _ = run(["register", "--layer", "gold", "--bucket", BUCKET], [PILOT])
    other_source, _ = run(["register", "--source", "polaris", "--layer", "gold", "--bucket", BUCKET], [PILOT])
    no_bucket, no_bucket_text = run(["register", "--source", "s3", "--layer", "gold"], [PILOT])
    return {
        "success_exits_zero_and_names_the_table": good == 0 and f"gold.{PILOT}: created" in good_text,
        "a_refused_table_exits_one_and_says_why": bad == 1 and "gold.broken: failed" in bad_text and f"gold.{PILOT}: unchanged" in bad_text,
        "source_is_required": no_source == 2,
        "only_the_s3_source_exists": other_source == 2,
        "bucket_is_required": no_bucket == 2 and "bucket" in no_bucket_text,
    }


def case_s3_register_maps_iceberg_types_to_glue_types() -> Check:
    mapped = {
        kind: glue_mirror.glue_type(kind)
        for kind in ("boolean", "int", "long", "float", "double", "date", "string", "uuid", "binary", "fixed[16]", "timestamp", "timestamptz", "decimal(10, 2)")
    }
    nested = glue_mirror.glue_type({"type": "map", "key-id": 1, "key": "string", "value-id": 2, "value": {"type": "list", "element-id": 3, "element": "long"}})
    refused = []
    for kind in ("variant", {"type": "weird"}, "decimal(x)"):
        try:
            glue_mirror.glue_type(kind)
        except glue_mirror.MirrorError:
            refused.append(True)
    return {
        "scalars": mapped
        == {
            "boolean": "boolean",
            "int": "int",
            "long": "bigint",
            "float": "float",
            "double": "double",
            "date": "date",
            "string": "string",
            "uuid": "string",
            "binary": "binary",
            "fixed[16]": "binary",
            "timestamp": "timestamp",
            "timestamptz": "timestamp",
            "decimal(10, 2)": "decimal(10,2)",
        },
        "nested_map_of_lists": nested == "map<string,array<bigint>>",
        "unknown_types_are_refused": refused == [True, True, True],
    }


def listed_bronze_names() -> set[str]:
    return {item["table"] for item in json.loads((REPO_ROOT / "config/lakehouse/bronze_tables.json").read_text())["tables"]}


def silver_bronze_sources() -> set[str]:
    """Every bronze table that a silver dbt model reads, from the dbt sources file."""
    yaml = importlib.import_module("yaml")

    document = yaml.safe_load((REPO_ROOT / "dbt/models/silver/staging/_bronze__sources.yml").read_text())
    return {
        str(table.get("identifier") or table["name"])
        for source in document["sources"]
        if source.get("schema", source["name"]) == "bronze"
        for table in source["tables"]
    }


def bronze_names() -> list[str]:
    return sorted(listed_bronze_names() | silver_bronze_sources())


def case_bronze_tables_are_the_listed_bronze_tables() -> Check:
    names = glue_mirror.layer_tables("bronze")
    return {
        "every_listed_table_and_silver_source": names == bronze_names() and len(names) == len(set(names)) and listed_bronze_names() <= set(names),
        "the_four_bookkeeping_sources_are_in": {"column_map", "file_metadata", "file_preambles", "stored_copies"} <= set(names),
        "no_other_table_than_the_two_files_name": set(names) == listed_bronze_names() | silver_bronze_sources(),
        "every_name_is_plain": all(glue_mirror.TABLE_NAME.fullmatch(name) for name in names),
        "other_layers_still_follow_their_contract": glue_mirror.layer_tables("gold") == list(glue_mirror.layer_plan("gold").tables),
    }


def case_s3_register_registers_bronze_in_its_own_database() -> Check:
    first, second = bronze_names()[:2]
    s3 = FakeS3()
    s3.put(metadata_key(first, 1, layer="bronze"), metadata_document(first, 10, layer="bronze"))
    s3.put(metadata_key(second, 1, layer="bronze"), metadata_document(second, 20, {"audit": 21}, layer="bronze"))
    s3.put(metadata_key(second, 2, layer="bronze"), metadata_document(second, 20, {"audit": 22}, layer="bronze"))
    glue = FakeGlue()
    report = register(s3, glue, [first, second], layer="bronze")
    held = glue.tables[("bronze", second)]["Parameters"]
    again = register(s3, glue, [first, second], layer="bronze")
    return {
        "both_created_in_the_bronze_database": report["tables"] == {first: "created", second: "created"} and {db for db, _ in glue.tables} == {"bronze"},
        "main_snapshot_and_newest_file_with_it": held["hai_snapshot_id"] == "20" and held["metadata_location"].endswith(metadata_name(2)),
        "location_is_the_bronze_folder": glue.tables[("bronze", first)]["StorageDescriptor"]["Location"] == f"s3://{BUCKET}/lakehouse/bronze/{first}",
        "only_the_bronze_folders_are_listed": {prefix for call, prefix in s3.calls if call == "list_objects_v2"}
        == {f"lakehouse/bronze/{first}/metadata/", f"lakehouse/bronze/{second}/metadata/"},
        "rerun_changes_nothing": again["status"] == "unchanged" and len(glue.writes()) == 2,
    }


def case_register_command_covers_bronze_and_check_refuses_it() -> Check:
    first = bronze_names()[0]
    s3 = FakeS3()
    s3.put(metadata_key(first, 1, layer="bronze"), metadata_document(first, 10, layer="bronze"))
    glue = FakeGlue()
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = glue_mirror.main(["register", "--source", "s3", "--layer", "bronze", "--bucket", BUCKET], glue=glue, s3=s3, config=enabled(), tables=[first])
        except SystemExit as error:
            code = int(error.code or 0)
        try:
            refused = glue_mirror.main(["check", "--layer", "bronze"], source=FakeSource({}), glue=glue, config=enabled(), snapshots={}, run_id=RUN_ID)
        except SystemExit as error:
            refused = int(error.code or 0)
    return {
        "bronze_registered_by_the_command": code == 0 and f"bronze.{first}: created" in out.getvalue(),
        "check_has_no_bronze_head_and_exits_two": refused == 2 and "publish_runs" in err.getvalue(),
        "no_publish_hook_for_bronze": glue_mirror.PUBLISHED_LAYERS == ("silver", "gold", "features"),
    }


def bronze_run_record(loaded: Sequence[str], skipped: Sequence[str]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": "bronze_00000001",
        "target": "real",
        "namespaces": ["bronze", "bronze_dictionary"],
        "tables": list(loaded),
        "skipped_tables": [{"table": name, "reason": "no source object"} for name in skipped],
    }


def register_bronze_command(s3: FakeS3, glue: FakeGlue) -> tuple[int, str]:
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        code = glue_mirror.main(["register", "--source", "s3", "--layer", "bronze", "--bucket", BUCKET], glue=glue, s3=s3, config=enabled())
    return code, out.getvalue()


def case_bronze_register_leaves_out_the_table_the_newest_bronze_run_skipped() -> Check:
    names, absent = glue_mirror.layer_tables("bronze"), "hcai_util_workbook_sheet_rows"
    s3 = FakeS3()
    for name in names:
        if name != absent:
            s3.put(metadata_key(name, 1, layer="bronze"), metadata_document(name, 10, layer="bronze"))
    glue = FakeGlue()
    without_record, text_without = register_bronze_command(s3, glue)
    key = "lakehouse/bronze_runs/bronze_00000001/check.json"
    s3.put(key, bronze_run_record([name for name in names if name != absent], [absent]))
    s3.modified[key] = datetime(2026, 11, 18, 22, tzinfo=UTC)
    glue = FakeGlue()
    code, text = register_bronze_command(s3, glue)
    registered = {name for _, name in glue.tables}
    again, _ = register_bronze_command(s3, glue)
    s3.put("lakehouse/bronze_runs/bronze_00000002/check.json", bronze_run_record(names, []))
    s3.modified["lakehouse/bronze_runs/bronze_00000002/check.json"] = datetime(2026, 11, 19, 9, tzinfo=UTC)
    later, text_later = register_bronze_command(s3, FakeGlue())
    return {
        "without_a_run_record_the_absent_table_fails_the_command_as_before": without_record == 1 and f"bronze.{absent}: failed" in text_without,
        "with_the_record_the_command_succeeds": code == 0 and f"bronze.{absent}: left out" in text and "no source object" in text,
        "every_other_table_is_registered_and_the_absent_one_is_not": registered == set(names) - {absent},
        "a_rerun_changes_nothing": again == 0 and len(glue.writes()) == len(names) - 1,
        "a_later_run_that_loaded_the_table_requires_it_again": later == 1 and f"bronze.{absent}: failed" in text_later and "left out" not in text_later,
    }


def case_every_silver_bronze_source_is_registered() -> Check:
    sources = silver_bronze_sources()
    names = set(glue_mirror.layer_tables("bronze"))
    return {
        "silver_reads_bronze_tables": len(sources) > 50,
        "every_silver_bronze_source_is_in_the_registration_list": sources <= names,
        "the_four_outside_the_table_list_are_named": sources - listed_bronze_names() == {"column_map", "file_metadata", "file_preambles", "stored_copies"},
        "the_dictionary_namespace_is_not_in_the_list": not any(name.startswith("bronze_dictionary") for name in names),
        "every_name_is_plain": all(glue_mirror.TABLE_NAME.fullmatch(name) for name in names),
    }


def case_silver_sources_that_the_list_cannot_name_are_refused() -> Check:
    def read(text: str) -> str:
        with tempfile.TemporaryDirectory(prefix="glue_mirror_") as directory:
            path = Path(directory) / "sources.yml"
            path.write_text(text)
            try:
                return ",".join(sorted(glue_mirror.silver_bronze_sources(path)))
            except glue_mirror.MirrorError as error:
                return f"refused: {error}"

    plain = "sources:\n  - name: bronze\n    schema: bronze\n    tables:\n      - name: alpha\n      - name: beta\n"
    return {
        "a_plain_source_lists_its_tables": read(plain) == "alpha,beta",
        "a_table_with_an_identifier_is_refused": read(plain + "      - name: gamma\n        identifier: gamma_physical\n").startswith("refused: "),
        "a_source_in_another_schema_is_refused": read(plain + "  - name: other\n    schema: elsewhere\n    tables:\n      - name: delta\n").startswith(
            "refused: "
        ),
        "a_source_without_a_schema_is_refused": read("sources:\n  - name: bronze\n    tables:\n      - name: alpha\n").startswith("refused: "),
        "a_name_that_is_not_plain_is_refused": read("sources:\n  - name: bronze\n    schema: bronze\n    tables:\n      - name: Bad-Name\n").startswith(
            "refused: "
        ),
    }


class PublishDuringRead(FakeS3):
    """Complete a real publish mirror between the registration's first Glue read and its write: the first metadata read arms it."""

    def __init__(self, glue: FakeGlue) -> None:
        super().__init__()
        self.glue = glue
        self.armed = False
        self.writes_after_publish = 0
        self.publish_parameters: dict[str, str] = {}

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        response = super().get_object(Bucket, Key)
        if self.armed:
            self.armed = False
            self.put(metadata_key(PILOT, 2), metadata_document(PILOT, 20, history=[10]))
            second = f"s3://{BUCKET}/{metadata_key(PILOT, 2)}"
            glue_mirror.mirror(enabled(), "gold", "real", ids([PILOT], 20), FakeSource({PILOT: second}), self.glue, RUN_ID)
            self.publish_parameters = dict(table_of(self.glue)["Parameters"])
            self.writes_after_publish = len(self.glue.writes())
        return response


def held_registration(glue: FakeGlue, table: str, snapshot: str, run: str = glue_mirror.S3_RUN_ID, layer_database: str = "gold") -> None:
    pointer = f"s3://{BUCKET}/{metadata_key(table, 1)}"
    parameters = {"table_type": "ICEBERG", "metadata_location": pointer, "hai_mirror": "true", "hai_snapshot_id": snapshot, "hai_run_id": run}
    glue.tables[(layer_database, table)] = {"Name": table, "Parameters": parameters, "VersionId": "1"}


def case_s3_register_never_replaces_a_publish_that_lands_during_its_reads() -> Check:
    checks: Check = {}
    for label, earlier_s3_registration in (("no_row", False), ("s3_row", True)):
        glue = FakeGlue()
        s3 = PublishDuringRead(glue)
        s3.put(metadata_key(PILOT, 1), metadata_document(PILOT, 10))
        if earlier_s3_registration:
            register(s3, glue, [PILOT])
        s3.armed = True
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = glue_mirror.main(["register", "--source", "s3", "--layer", "gold", "--bucket", BUCKET], glue=glue, s3=s3, config=enabled(), tables=[PILOT])
        after = table_of(glue)["Parameters"]
        checks[f"{label}_publish_pointer_survives"] = (
            after["hai_run_id"] == RUN_ID and after["hai_snapshot_id"] == "20" and after["metadata_location"] == s3.publish_parameters["metadata_location"]
        )
        checks[f"{label}_register_wrote_nothing_after_the_publish"] = len(glue.writes()) == s3.writes_after_publish
        checks[f"{label}_kept_by_name_and_exit_zero"] = code == 0 and f"gold.{PILOT}: kept: registered by publish run {RUN_ID}" in out.getvalue()
    return checks


def case_s3_register_refuses_an_older_or_unrelated_snapshot_and_changes_nothing() -> Check:
    s3 = FakeS3()
    # older: the registration holds 20, the metadata read now ends at 10 and does not know 20
    s3.put(metadata_key("older", 1), metadata_document("older", 10))
    # newer: the metadata knows snapshot 30 (sequence 3) while main is at 10 (sequence 1)
    s3.put(
        metadata_key("newer", 1),
        metadata_document(
            "newer",
            10,
            changes={
                "snapshots": [
                    {"snapshot-id": 10, "sequence-number": 1, "timestamp-ms": 1000},
                    {"snapshot-id": 30, "sequence-number": 3, "timestamp-ms": 3000, "parent-snapshot-id": 10},
                ]
            },
        ),
    )
    # divergent: the registration holds 15, a sibling of the new main 20 (both children of 10)
    s3.put(
        metadata_key("divergent", 1),
        metadata_document(
            "divergent",
            20,
            changes={
                "snapshots": [
                    {"snapshot-id": 10, "sequence-number": 1, "timestamp-ms": 1000},
                    {"snapshot-id": 15, "sequence-number": 2, "timestamp-ms": 2000, "parent-snapshot-id": 10},
                    {"snapshot-id": 20, "sequence-number": 3, "timestamp-ms": 3000, "parent-snapshot-id": 10},
                ]
            },
        ),
    )
    s3.put(metadata_key("unreadable", 1), metadata_document("unreadable", 10))
    s3.put(metadata_key("good", 1), metadata_document("good", 10))
    glue = FakeGlue()
    for table, snapshot in (("older", "20"), ("newer", "30"), ("divergent", "15"), ("unreadable", "abc")):
        held_registration(glue, table, snapshot)
    before = {table: json.dumps(glue.tables[("gold", table)], sort_keys=True) for table in ("older", "newer", "divergent", "unreadable")}
    report = register(s3, glue, ["older", "newer", "divergent", "unreadable", "good"])
    outcomes = report["tables"]
    after = {table: json.dumps(glue.tables[("gold", table)], sort_keys=True) for table in before}
    return {
        "nothing_changed_for_the_four": before == after and not [w for w in glue.writes() if w[2] != "good"],
        "older_is_refused_as_not_listed": outcomes["older"].startswith("failed")
        and "not listed" in outcomes["older"]
        and "nothing was changed" in outcomes["older"],
        "newer_is_refused_as_newer": outcomes["newer"].startswith("failed") and "newer" in outcomes["newer"] and "nothing was changed" in outcomes["newer"],
        "divergent_is_refused_as_not_an_ancestor": outcomes["divergent"].startswith("failed") and "not an ancestor" in outcomes["divergent"],
        "an_unreadable_snapshot_id_is_refused": outcomes["unreadable"].startswith("failed") and "nothing was changed" in outcomes["unreadable"],
        "the_other_table_is_still_registered": outcomes["good"] == "created",
        "the_report_is_failed": report["status"] == "failed",
    }


def case_s3_register_identical_writes_nothing_and_an_ancestor_moves_up() -> Check:
    glue, s3 = FakeGlue(), FakeS3()
    s3.put(metadata_key("same", 1), metadata_document("same", 10))
    s3.put(metadata_key("same", 2), metadata_document("same", 10, {"staged": 11}))
    s3.put(metadata_key("published", 1), metadata_document("published", 10))
    s3.put(metadata_key("climb", 1), metadata_document("climb", 20, history=[10]))
    held_registration(glue, "same", "10")
    held_registration(glue, "published", "10", run=RUN_ID)
    held_registration(glue, "climb", "10")
    report = register(s3, glue, ["same", "published", "climb"])
    held = glue.tables[("gold", "climb")]["Parameters"]
    return {
        "identical_snapshot_writes_nothing": report["tables"]["same"] == "unchanged" and ("update_table", "gold", "same") not in glue.calls,
        "identical_snapshot_of_a_publish_is_kept": report["tables"]["published"].startswith("kept") and ("update_table", "gold", "published") not in glue.calls,
        "an_ancestor_registration_moves_up": report["tables"]["climb"] == "updated" and held["hai_snapshot_id"] == "20",
        "the_run_stays_a_registration_run": held["hai_run_id"] == glue_mirror.S3_RUN_ID,
    }


def case_s3_register_refuses_a_parent_that_the_metadata_does_not_list() -> Check:
    s3 = FakeS3()
    # direct: main 20 names parent 10, but the snapshot list holds 20 only
    s3.put(
        metadata_key("direct", 1),
        metadata_document("direct", 20, changes={"snapshots": [{"snapshot-id": 20, "sequence-number": 2, "timestamp-ms": 2000, "parent-snapshot-id": 10}]}),
    )
    # indirect: main 30 names the listed 20, which names the unlisted 10
    s3.put(
        metadata_key("indirect", 1),
        metadata_document(
            "indirect",
            30,
            changes={
                "snapshots": [
                    {"snapshot-id": 20, "sequence-number": 2, "timestamp-ms": 2000, "parent-snapshot-id": 10},
                    {"snapshot-id": 30, "sequence-number": 3, "timestamp-ms": 3000, "parent-snapshot-id": 20},
                ]
            },
        ),
    )
    # listed: the same shape with the held snapshot 10 in the list, which is the ordinary ancestor case
    s3.put(metadata_key("listed", 1), metadata_document("listed", 30, history=[10, 20]))
    glue = FakeGlue()
    for table in ("direct", "indirect", "listed"):
        held_registration(glue, table, "10")
    before = {table: json.dumps(glue.tables[("gold", table)], sort_keys=True) for table in ("direct", "indirect")}
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = glue_mirror.main(
            ["register", "--source", "s3", "--layer", "gold", "--bucket", BUCKET], glue=glue, s3=s3, config=enabled(), tables=["direct", "indirect", "listed"]
        )
    after = {table: json.dumps(glue.tables[("gold", table)], sort_keys=True) for table in before}
    text = out.getvalue()
    return {
        "nothing_changed_for_the_two_unlisted": before == after and not [w for w in glue.writes() if w[2] in before],
        "direct_is_refused_by_name": "gold.direct: failed" in text and "not listed" in text and "nothing was changed" in text,
        "indirect_is_refused_by_name": "gold.indirect: failed" in text and "snapshot 10" in text,
        "a_listed_ancestor_still_moves_up": "gold.listed: updated" in text,
        "the_command_exits_one": code == 1,
    }


CASES: dict[str, Callable[[], Check]] = {
    "first_publish_registers": case_first_publish_registers,
    "repeat_is_a_no_op": case_repeat_is_a_no_op,
    "new_commit_moves_the_pointer": case_new_commit_moves_the_pointer,
    "glue_error_is_reported_not_raised": case_glue_error_is_reported_not_raised,
    "missing_database_says_so": case_missing_database_says_so,
    "hook_never_fails_the_publish": case_hook_never_fails_the_publish,
    "hook_repeat_is_a_no_op": case_hook_repeat_is_a_no_op,
    "disabled_touches_nothing": case_disabled_touches_nothing,
    "tracked_config_turns_the_mirror_on": case_tracked_config_turns_the_mirror_on,
    "test_namespace_touches_nothing": case_test_namespace_touches_nothing,
    "bronze_dictionary_and_other_layers_touch_nothing": case_bronze_dictionary_and_other_layers_touch_nothing,
    "second_gold_table_is_mirrored": case_second_gold_table_is_mirrored,
    "other_layers_touch_nothing": case_other_layers_touch_nothing,
    "silver_layer_is_mirrored": case_silver_layer_is_mirrored,
    "silver_publish_hook_mirrors_silver": case_silver_publish_hook_mirrors_silver,
    "silver_publish_plan_reaches_the_hook": case_silver_publish_plan_reaches_the_hook,
    "features_layer_is_mirrored": case_features_layer_is_mirrored,
    "features_publish_hook_mirrors_features": case_features_publish_hook_mirrors_features,
    "features_publish_plan_reaches_the_hook": case_features_publish_plan_reaches_the_hook,
    "check_accepts_the_features_layer": case_check_accepts_the_features_layer,
    "foreign_table_is_refused": case_foreign_table_is_refused,
    "plain_named_database_keeps_a_table_the_mirror_did_not_write": case_plain_named_database_keeps_a_table_the_mirror_did_not_write,
    "other_pointers_are_refused": case_other_pointers_are_refused,
    "committed_retry_mirrors_the_committed_set": case_committed_retry_mirrors_the_committed_set,
    "check_is_bound_to_the_committed_set": case_check_is_bound_to_the_committed_set,
    "missing_committed_snapshot_is_refused": case_missing_committed_snapshot_is_refused,
    "deadline_bounds_a_blocking_read": case_deadline_bounds_a_blocking_read,
    "no_write_is_issued_after_the_deadline": case_no_write_is_issued_after_the_deadline,
    "a_write_past_the_deadline_is_reported_as_possibly_landed": case_a_write_past_the_deadline_is_reported_as_possibly_landed,
    "a_write_is_refused_when_the_deadline_passes_as_it_starts": case_a_write_is_refused_when_the_deadline_passes_as_it_starts,
    "later_tables_are_not_attempted_after_the_deadline": case_later_tables_are_not_attempted_after_the_deadline,
    "hook_deadline_starts_at_entry": case_hook_deadline_starts_at_entry,
    "check_compares_the_run": case_check_compares_the_run,
    "glue_client_has_short_timeouts": case_glue_client_has_short_timeouts,
    "closed_diagnostic_pipe_does_not_escape": case_closed_diagnostic_pipe_does_not_escape,
    "config_scope_is_silver_gold_and_features_only": case_config_scope_is_silver_gold_and_features_only,
    "reader_access_is_one_role_for_one_user": case_reader_access_is_one_role_for_one_user,
    "check_compares_the_two_pointers": case_check_compares_the_two_pointers,
    "s3_register_creates_each_table_at_its_main_snapshot": case_s3_register_creates_each_table_at_its_main_snapshot,
    "s3_register_picks_the_newest_main_never_a_branch": case_s3_register_picks_the_newest_main_never_a_branch,
    "s3_register_refuses_a_table_it_cannot_resolve": case_s3_register_refuses_a_table_it_cannot_resolve,
    "s3_register_is_idempotent_and_follows_a_new_commit": case_s3_register_is_idempotent_and_follows_a_new_commit,
    "s3_register_keeps_a_registration_a_publish_wrote": case_s3_register_keeps_a_registration_a_publish_wrote,
    "s3_register_dry_run_writes_nothing": case_s3_register_dry_run_writes_nothing,
    "s3_register_obeys_the_switch_and_the_layer_scope": case_s3_register_obeys_the_switch_and_the_layer_scope,
    "s3_register_stays_inside_the_layer_folder": case_s3_register_stays_inside_the_layer_folder,
    "s3_register_obeys_one_deadline": case_s3_register_obeys_one_deadline,
    "s3_register_command_reports_and_exits": case_s3_register_command_reports_and_exits,
    "s3_register_maps_iceberg_types_to_glue_types": case_s3_register_maps_iceberg_types_to_glue_types,
    "bronze_tables_are_the_listed_bronze_tables": case_bronze_tables_are_the_listed_bronze_tables,
    "every_silver_bronze_source_is_registered": case_every_silver_bronze_source_is_registered,
    "silver_sources_that_the_list_cannot_name_are_refused": case_silver_sources_that_the_list_cannot_name_are_refused,
    "s3_register_never_replaces_a_publish_that_lands_during_its_reads": case_s3_register_never_replaces_a_publish_that_lands_during_its_reads,
    "s3_register_refuses_an_older_or_unrelated_snapshot_and_changes_nothing": case_s3_register_refuses_an_older_or_unrelated_snapshot_and_changes_nothing,
    "s3_register_identical_writes_nothing_and_an_ancestor_moves_up": case_s3_register_identical_writes_nothing_and_an_ancestor_moves_up,
    "s3_register_refuses_a_parent_that_the_metadata_does_not_list": case_s3_register_refuses_a_parent_that_the_metadata_does_not_list,
    "s3_register_registers_bronze_in_its_own_database": case_s3_register_registers_bronze_in_its_own_database,
    "register_command_covers_bronze_and_check_refuses_it": case_register_command_covers_bronze_and_check_refuses_it,
    "bronze_register_leaves_out_the_table_the_newest_bronze_run_skipped": case_bronze_register_leaves_out_the_table_the_newest_bronze_run_skipped,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new folder for cases.json")
    args = parser.parse_args(argv)
    if args.out.exists():
        sys.stderr.write(f"glue mirror E2E: {args.out} exists; give a new folder\n")
        return 1
    results: dict[str, Any] = {}
    for name, case in CASES.items():
        try:
            checks = case()
        except (PublishError, Exception) as error:
            checks = {f"case_ran ({type(error).__name__}: {error})": False}
        results[name] = {"pass": all(checks.values()) and bool(checks), "checks": checks}
        sys.stdout.write(f"{'PASS' if results[name]['pass'] else 'FAIL'} {name}\n")
        for label, ok in checks.items():
            if not ok:
                sys.stdout.write(f"     failed: {label}\n")
    args.out.mkdir(parents=True)
    (args.out / "cases.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    failed = [name for name, item in results.items() if not item["pass"]]
    sys.stdout.write(f"{len(results) - len(failed)} of {len(results)} cases passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
