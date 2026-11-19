"""The bronze load registers its tables in the bronze Glue mirror database once the load and its metadata generation are committed.

Owner decision of Nov 18 2026 ("add the hook for bronze"). The hook reuses ``glue_mirror`` (the switch, the time budget and the registration), so these
cases pin only what the bronze load adds: when it runs, what it registers, what it reports and that it never undoes the committed load. No AWS service and
no Spark are used: a stand-in Glue client and a stand-in Polaris source, and the real ``bronze.main`` with its service boundaries replaced.
"""

from __future__ import annotations

import contextlib
import io
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from scripts.lakehouse import bronze, bronze_mirror, glue_mirror
from scripts.lakehouse.run_glue_mirror_e2e import COLUMNS, FakeGlue
from tests.support import check

RUN = "bronze_0a1b2c3d"
TABLES = ["alpha_data", "beta_data"]
SNAPSHOTS = {"alpha_data": 41, "beta_data": 42}


def metadata_file(table: str, snapshot: int) -> str:
    return f"s3://example-bucket/lakehouse/bronze/{table}/metadata/00001-aaaaaaaa-0000-0000-0000-{snapshot:012d}.metadata.json"


class Source:
    """The Polaris side: the metadata file of each committed snapshot."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error, self.asked = error, []

    def pointer(self, table: str, snapshot_id: int) -> str | None:
        self.asked.append((table, snapshot_id))
        if self.error is not None:
            raise self.error
        return metadata_file(table, snapshot_id)

    def columns(self, table: str) -> list[tuple[str, str]]:
        return list(COLUMNS)


def enabled(**changes: Any) -> glue_mirror.MirrorConfig:
    return replace(glue_mirror.load_config(), enabled=True, **changes)


def hook(
    namespace: str = "bronze", config: glue_mirror.MirrorConfig | None = None, glue: Any = None, source: Any = None, run_id: str = RUN, **keywords: Any
) -> dict[str, Any]:
    keywords.setdefault("read_loader", lambda *_: ({}, []))
    return bronze_mirror.mirror_after_load(
        None,
        namespace,
        TABLES,
        keywords.pop("snapshots", SNAPSHOTS),
        run_id,
        source=source or Source(),
        glue=glue or FakeGlue(),
        config=config or enabled(),
        **keywords,
    )


def test_the_tracked_config_mirrors_bronze_and_the_qualification_twin_stays_out() -> None:
    config = glue_mirror.load_config()
    check(config.enabled and config.layers["bronze"].database == "bronze", "the tracked config does not mirror bronze")
    check("bronze_e2e" not in config.layers and "bronze_dictionary" not in config.layers, "a test or dictionary namespace is mirrored")


def test_a_committed_load_registers_each_loaded_table_in_the_bronze_database_with_the_run() -> None:
    glue = FakeGlue()
    report = hook(glue=glue)
    check(report["status"] == "ok" and report["tables"] == {"alpha_data": "created", "beta_data": "created"}, f"{report}")
    for table, snapshot in SNAPSHOTS.items():
        held = glue.tables[("bronze", table)]
        check(held["Parameters"]["metadata_location"] == metadata_file(table, snapshot), f"{table}: {held['Parameters']}")
        check(held["Parameters"]["hai_snapshot_id"] == str(snapshot) and held["Parameters"]["hai_run_id"] == RUN, f"{table}: {held['Parameters']}")
        check(held["Parameters"]["hai_mirror"] == "true", f"{table}: the registration is not marked as a mirror")
    check({database for database, _ in glue.tables} == {"bronze"}, "a table was registered outside the bronze database")


def test_only_the_tables_of_the_committed_generation_are_registered() -> None:
    glue = FakeGlue()
    report = hook(glue=glue, snapshots={"alpha_data": 41})
    check(
        report["tables"] == {"alpha_data": "created"} and set(glue.tables) == {("bronze", "alpha_data")}, f"a table without a snapshot was registered: {report}"
    )


def test_a_repeat_writes_nothing_and_a_new_snapshot_moves_the_registration() -> None:
    glue = FakeGlue()
    hook(glue=glue)
    glue.calls.clear()
    again = hook(glue=glue)
    check(again["status"] == "unchanged" and glue.writes() == [], f"a repeat wrote to Glue: {again} {glue.writes()}")
    moved = hook(glue=glue, snapshots={"alpha_data": 43, "beta_data": 42}, run_id="bronze_0a1b2c3e")
    check(moved["tables"] == {"alpha_data": "updated", "beta_data": "updated"}, f"{moved}")
    check(
        glue.tables[("bronze", "alpha_data")]["Parameters"]["metadata_location"] == metadata_file("alpha_data", 43),
        "the registration did not follow the new snapshot",
    )


@pytest.mark.parametrize(
    ("label", "namespace", "config"),
    [
        ("the mirror is off in the config", "bronze", replace(glue_mirror.load_config(), enabled=False)),
        ("the qualification namespace", "bronze_e2e", None),
        ("the dictionary namespace", "bronze_dictionary", None),
        ("another test namespace", "smoke_check_e2e", None),
    ],
)
def test_nothing_runs_when_the_mirror_is_off_or_the_namespace_is_not_the_real_bronze_one(
    label: str, namespace: str, config: glue_mirror.MirrorConfig | None
) -> None:
    glue, source = FakeGlue(), Source()
    with mock.patch.object(glue_mirror, "glue_client", side_effect=AssertionError("a Glue client was built")):
        report = bronze_mirror.mirror_after_load(None, namespace, TABLES, SNAPSHOTS, RUN, source=source, glue=None, config=config or enabled())
    check(report["status"] == "skipped" and report["tables"] == {} and report["reason"], f"{label}: {report}")
    check(glue.calls == [] and source.asked == [], f"{label}: the hook touched Glue or Polaris")


@pytest.mark.parametrize(
    ("label", "glue", "source"),
    [
        ("glue is down", FakeGlue(fail={"get_table": "InternalServiceException"}), None),
        ("the database is missing", FakeGlue(databases=()), None),
        ("polaris is down", None, Source(error=RuntimeError("catalog unreachable"))),
    ],
)
def test_a_failure_never_raises_and_is_reported_with_its_cause(label: str, glue: Any, source: Any, capsys: pytest.CaptureFixture[str]) -> None:
    report = hook(glue=glue, source=source)
    check(report["status"] == "failed" and report["reason"], f"{label}: {report}")
    check("FAILED" in capsys.readouterr().err, f"{label}: a failed mirror is not named on standard error")


def test_a_missing_glue_client_setting_is_a_reported_failure_not_a_crash(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_REGION", raising=False)
    report = bronze_mirror.mirror_after_load(
        None, "bronze", TABLES, SNAPSHOTS, RUN, source=Source(), glue=None, config=enabled(), read_loader=lambda *_: ({}, [])
    )
    check(report["status"] == "failed" and "AWS_REGION" in report["reason"], f"{report}")


def test_the_deadline_starts_when_the_hook_is_entered() -> None:
    import threading

    release = threading.Event()

    class Stuck(Source):
        def pointer(self, table: str, snapshot_id: int) -> str | None:
            release.wait(5)
            return super().pointer(table, snapshot_id)

    glue = FakeGlue()
    try:
        report = hook(glue=glue, source=Stuck(), config=enabled(mirror_seconds=0.3))
    finally:
        release.set()
    check(report["status"] == "failed" and "deadline" in report["reason"] and glue.writes() == [], f"{report}")


# The real ``bronze.main`` with its service boundaries replaced, to pin where the hook sits in the load.


HOOK_KEYWORDS: list[dict[str, Any]] = []


def run_main(
    tmp_path: Path,
    *,
    passed: bool = True,
    namespace: str | None = None,
    hook_result: Any = None,
    extra: list[str] | None = None,
    left_out: tuple[str, ...] = (),
) -> tuple[int, dict[str, Any], list[str]]:
    steps: list[str] = []
    HOOK_KEYWORDS.clear()
    table_map = {"namespace": "bronze", "tables": [{"table": name, "group": "g"} for name in (*TABLES, *left_out)]}
    checks = {name: {"passed": passed} for name in TABLES}

    def step(name: str, value: Any = None) -> Any:
        def call(*_arguments: Any, **_keywords: Any) -> Any:
            steps.append(name)
            return value

        return call

    def mirror(spark: Any, space: str, names: list[str], snapshots: dict[str, int], run_id: str | None = None, **_keywords: Any) -> dict[str, Any]:
        steps.append(f"mirror {space} {sorted(snapshots)} {run_id}")
        HOOK_KEYWORDS.append(_keywords)
        return hook_result or {"status": "ok", "tables": dict.fromkeys(snapshots, "created")}

    metadata = [{"generation": "g1"}]
    spark = SimpleNamespace(stop=lambda: steps.append("stop"))
    arguments = ["--tables", ",".join(TABLES), "--bucket", "example-bucket", "--task-role"] + (["--namespace", namespace] if namespace else []) + (extra or [])
    arguments += ["--unsourced-tables", ",".join(left_out)] if left_out else []
    with (
        mock.patch.dict("sys.modules", {"scripts.lakehouse.session": SimpleNamespace(spark_session=lambda *_a, **_k: spark, job_memory=lambda: "1g")}),
        mock.patch.object(bronze, "load_table_map", return_value=table_map),
        mock.patch.object(bronze, "S3Storage", return_value=object()),
        mock.patch.object(bronze, "load_retired", return_value=None),
        mock.patch.object(bronze, "discover", return_value=([], [])),
        mock.patch.object(bronze, "load_objects", step("load", {})),
        mock.patch.object(bronze, "prune", step("prune", {})),
        mock.patch.object(bronze, "write_copies", step("copies", 0)),
        mock.patch.object(bronze, "verify", return_value=checks),
        mock.patch.object(bronze, "table_snapshots", return_value=dict(SNAPSHOTS)),
        mock.patch.object(bronze, "REPORT_ROOT", tmp_path),
        mock.patch("scripts.lakehouse.bronze_metadata.read", return_value=[]),
        mock.patch("scripts.lakehouse.bronze_metadata.build", return_value=metadata),
        mock.patch("scripts.lakehouse.bronze_metadata.validate_successor"),
        mock.patch("scripts.lakehouse.bronze_metadata.publish", step("publish", "g1")),
        mock.patch("scripts.lakehouse.bronze_metadata.write_bootstrap", return_value="a" * 64),
        mock.patch("scripts.lakehouse.dictionary.build", step("dictionary", {})),
        mock.patch.object(bronze_mirror, "mirror_after_load", mirror),
        contextlib.redirect_stdout(io.StringIO()),
        contextlib.redirect_stderr(io.StringIO()),
    ):
        status = bronze.main(arguments)
    reports = sorted(tmp_path.glob("report_*.json"))
    check(len(reports) == 1, f"the loader wrote {len(reports)} reports")
    return status, json.loads(reports[0].read_text()), steps


def test_the_loader_runs_the_hook_after_the_metadata_generation_is_published_and_reports_it(tmp_path: Path) -> None:
    status, report, steps = run_main(tmp_path, extra=["--run-id", RUN])
    check(status == 0 and report["passed"] is True, f"{status} {report}")
    check(
        steps.index("publish") < next(i for i, item in enumerate(steps) if item.startswith("mirror")),
        f"the hook ran before the metadata generation was published: {steps}",
    )
    check(steps[-2:] == [f"mirror bronze {sorted(TABLES)} {RUN}", "stop"], f"the hook ran after the session stopped or not last: {steps}")
    check(report["mirror"] == {"status": "ok", "tables": {"alpha_data": "created", "beta_data": "created"}}, f"{report.get('mirror')}")


def test_the_loader_tells_the_hook_the_tables_it_left_out_and_the_reader_that_lists_the_source_history(tmp_path: Path) -> None:
    """The live branch's exclusion stays upstream of the hook (``check_unsourced`` runs first); the hook is told the same list, the loader's own S3 reader
    (taken before the pinned replay wrapper, which has no history), the bucket and the table map."""
    status, report, _ = run_main(tmp_path, extra=["--run-id", RUN], left_out=("gamma_data",))
    check(status == 0 and report["passed"] is True and report["unsourced_tables"] == ["gamma_data"], f"{status} {report}")
    given = HOOK_KEYWORDS[0]
    check(given["unsourced"] == ["gamma_data"] and given["bucket"] == "example-bucket", f"{given}")
    check({entry["table"] for entry in given["table_map"]["tables"]} == {*TABLES, "gamma_data"}, f"{given['table_map']}")
    check(given["history"] is not None, "the hook was given no source reader")


def test_a_failed_mirror_leaves_the_load_committed_and_passing_and_is_in_the_report(tmp_path: Path) -> None:
    failed = {"status": "failed", "reason": "alpha_data failed: AccessDeniedException from Glue", "tables": {"alpha_data": "failed: x", "beta_data": "created"}}
    status, report, steps = run_main(tmp_path, hook_result=failed)
    check(status == 0 and report["passed"] is True and report["metadata_index"]["generation"] == "g1" and "publish" in steps, f"{status} {report}")
    check(report["mirror"] == failed, f"{report.get('mirror')}")


def test_a_load_that_did_not_pass_its_checks_publishes_nothing_and_mirrors_nothing(tmp_path: Path) -> None:
    status, report, steps = run_main(tmp_path, passed=False)
    check(status == 1 and report["passed"] is False, f"{status} {report}")
    check(not any(item.startswith("mirror") or item == "publish" for item in steps), f"a load that failed its checks published or mirrored: {steps}")
    check(report["mirror"]["status"] == "skipped" and report["mirror"]["tables"] == {}, f"{report.get('mirror')}")


def test_the_qualification_namespace_reaches_the_hook_as_its_own_namespace(tmp_path: Path) -> None:
    _, _, steps = run_main(tmp_path, namespace="bronze_e2e", extra=["--selected-metadata"])
    check(any(item.startswith("mirror bronze_e2e") for item in steps), f"the hook was not told the qualification namespace: {steps}")


def test_the_run_id_must_be_a_bronze_run_id(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        run_main(tmp_path, extra=["--run-id", "silver_0a1b2c3d"])


# Review round 2: the side tables that a load commits beside the mapped tables.

SIDE = ["column_map", "file_metadata", "file_preambles", "stored_copies"]
SIDE_SNAPSHOTS = {"column_map": 71, "file_metadata": 72, "file_preambles": 73, "stored_copies": 74}


class Reader:
    """The read of the loader tables' committed snapshots: those with a snapshot and those that exist without one. An absent table is in neither."""

    def __init__(self, present: dict[str, int], empty: tuple[str, ...] = ()) -> None:
        self.present, self.empty, self.asked = present, empty, []

    def __call__(self, _spark: Any, _namespace: str, tables: Any) -> tuple[dict[str, int], list[str]]:
        self.asked.append(list(tables))
        return {name: self.present[name] for name in tables if name in self.present}, [name for name in tables if name in self.empty]


def reader(present: dict[str, int], empty: tuple[str, ...] = ()) -> Reader:
    return Reader(present, empty)


def test_the_hook_and_the_by_hand_register_name_the_same_bronze_tables() -> None:
    listed = {str(item["table"]) for item in json.loads(glue_mirror.BRONZE_TABLES.read_text())["tables"]}
    by_hand = set(glue_mirror.layer_tables("bronze"))
    from_the_hook = set(glue_mirror.bronze_table_set(sorted(listed)))
    check(from_the_hook == by_hand, f"hook only: {sorted(from_the_hook - by_hand)}, by hand only: {sorted(by_hand - from_the_hook)}")
    check(set(SIDE) <= from_the_hook and set(SIDE) == set(glue_mirror.bronze_loader_tables()), f"{glue_mirror.bronze_loader_tables()}")


def test_the_side_tables_are_the_ones_the_table_map_and_silver_declare() -> None:
    declared = set(json.loads(glue_mirror.BRONZE_TABLES.read_text())["classification"]["loader_tables"])
    check(set(glue_mirror.bronze_loader_tables()) == declared, "the shared side table list is not the table map's loader tables")
    check(declared <= glue_mirror.silver_bronze_sources(), "a loader table is not a source of silver, so silver would not resolve it")


def test_a_load_into_an_empty_mirror_registers_the_side_tables_with_the_mapped_ones() -> None:
    glue, read = FakeGlue(), reader(SIDE_SNAPSHOTS)
    report = hook(glue=glue, read_loader=read)
    check(report["status"] == "ok" and set(report["tables"]) == {*TABLES, *SIDE}, f"{report}")
    check(set(glue.tables) == {("bronze", name) for name in [*TABLES, *SIDE]}, f"registered: {sorted(glue.tables)}")
    for name, snapshot in SIDE_SNAPSHOTS.items():
        held = glue.tables[("bronze", name)]["Parameters"]
        check(held["metadata_location"] == metadata_file(name, snapshot) and held["hai_snapshot_id"] == str(snapshot), f"{name}: {held}")
    check(read.asked == [SIDE], f"the loader tables were read as {read.asked}")


def test_a_second_load_advances_the_side_table_pointers() -> None:
    glue = FakeGlue()
    hook(glue=glue, read_loader=reader(SIDE_SNAPSHOTS))
    later = {**SIDE_SNAPSHOTS, "file_metadata": 82, "stored_copies": 84}
    report = hook(glue=glue, read_loader=reader(later), snapshots={"alpha_data": 43, "beta_data": 42})
    check(report["tables"]["file_metadata"] == "updated" and report["tables"]["stored_copies"] == "updated", f"{report}")
    check(report["tables"]["column_map"] == "unchanged" and report["tables"]["file_preambles"] == "unchanged", f"{report}")
    check(glue.tables[("bronze", "file_metadata")]["Parameters"]["metadata_location"] == metadata_file("file_metadata", 82), "file_metadata did not advance")


def test_a_side_table_the_catalog_does_not_hold_yet_is_not_mirrored_and_is_no_failure() -> None:
    glue = FakeGlue()
    report = hook(glue=glue, read_loader=reader({"file_metadata": 72, "column_map": 71}))
    check(report["status"] == "ok" and set(report["tables"]) == {*TABLES, "file_metadata", "column_map"}, f"{report}")


def test_a_partial_load_mirrors_its_tables_and_the_side_tables_and_no_other_mapped_table() -> None:
    glue = FakeGlue()
    report = hook(glue=glue, snapshots={"alpha_data": 41}, read_loader=reader(SIDE_SNAPSHOTS))
    check(set(report["tables"]) == {"alpha_data", *SIDE}, f"{report}")


def test_the_side_table_reads_run_inside_the_deadline_and_a_failed_read_is_a_reported_failure() -> None:
    def broken(_spark: Any, _namespace: str, _tables: Any) -> dict[str, int]:
        raise RuntimeError("catalog unreachable")

    glue = FakeGlue()
    report = hook(glue=glue, read_loader=broken)
    check(report["status"] == "failed" and "catalog unreachable" in report["reason"] and glue.writes() == [], f"{report}")


def test_the_side_tables_are_not_read_while_the_mirror_is_off_or_in_a_test_namespace() -> None:
    for namespace, config in (("bronze", replace(glue_mirror.load_config(), enabled=False)), ("bronze_e2e", None)):
        read = reader(SIDE_SNAPSHOTS)
        report = hook(namespace=namespace, config=config, read_loader=read)
        check(report["status"] == "skipped" and read.asked == [], f"{namespace}: {report} {read.asked}")


class Catalog:
    """A catalog for the default side read: which tables exist and the snapshot rows of each (newest first). The real ``bronze.latest_snapshot`` runs on it."""

    def __init__(self, snapshots: dict[str, list[int]]) -> None:
        self.snapshots = snapshots
        self.catalog = SimpleNamespace(tableExists=lambda name: name.rsplit(".", 1)[-1] in snapshots)

    def table(self, name: str) -> Any:
        table = name.removesuffix(".snapshots").rsplit(".", 1)[-1]
        rows = [{"snapshot_id": snapshot} for snapshot in self.snapshots[table]]
        frame = SimpleNamespace(orderBy=lambda _column: frame, limit=lambda count: SimpleNamespace(collect=lambda: rows[:count]))
        return frame


@contextlib.contextmanager
def pyspark_functions() -> Any:
    """The one Spark function the snapshot read names, so the real read runs where Spark is not installed."""
    import sys
    import types

    module, sql = types.ModuleType("pyspark"), types.ModuleType("pyspark.sql")
    sql.__dict__["functions"] = SimpleNamespace(desc=lambda column: column)
    with mock.patch.dict(sys.modules, {"pyspark": module, "pyspark.sql": sql}):
        yield


def test_the_default_read_takes_the_exact_snapshots_of_the_side_tables_that_exist() -> None:
    spark = Catalog({"column_map": [71], "file_metadata": [72, 60], "stored_copies": [74]})
    with pyspark_functions():
        found, waiting = bronze_mirror.loader_snapshots(spark, "bronze", SIDE)
    check(found == {"column_map": 71, "file_metadata": 72, "stored_copies": 74} and waiting == [], f"{found} {waiting}")


@pytest.mark.parametrize("empty_table", SIDE)
def test_a_side_table_that_exists_without_a_snapshot_is_waiting_and_blocks_nothing(empty_table: str) -> None:
    """A first load of headerless formats creates ``column_map`` but writes no row to it, so the table exists with no snapshot. Any side table can be in that
    state, so the rule is the same for all four: it is reported as not mirrored yet, and the committed tables are registered."""
    snapshots = {name: [SIDE_SNAPSHOTS[name]] for name in SIDE}
    snapshots[empty_table] = []
    glue = FakeGlue()
    with pyspark_functions():
        report = bronze_mirror.mirror_after_load(Catalog(snapshots), "bronze", TABLES, SNAPSHOTS, RUN, source=Source(), glue=glue, config=enabled())
    check(report["status"] == "ok", f"an empty {empty_table} failed the mirror: {report}")
    check(set(report["tables"]) == {*TABLES, *SIDE}, f"{report}")
    check(report["tables"][empty_table].startswith("not mirrored: no committed snapshot"), f"{report['tables'][empty_table]}")
    check(set(glue.tables) == {("bronze", name) for name in [*TABLES, *SIDE] if name != empty_table}, f"registered: {sorted(glue.tables)}")


def test_when_every_side_table_is_empty_the_loaded_tables_are_still_registered() -> None:
    glue = FakeGlue()
    with pyspark_functions():
        report = bronze_mirror.mirror_after_load(
            Catalog(dict.fromkeys(SIDE, [])), "bronze", TABLES, SNAPSHOTS, RUN, source=Source(), glue=glue, config=enabled()
        )
    check(report["status"] == "ok" and {name for _, name in glue.tables} == set(TABLES), f"{report}")


def test_an_empty_side_table_is_registered_once_its_first_snapshot_commits() -> None:
    glue = FakeGlue()
    with pyspark_functions():
        first = bronze_mirror.mirror_after_load(
            Catalog({"column_map": [], "file_metadata": [72]}), "bronze", TABLES, SNAPSHOTS, RUN, source=Source(), glue=glue, config=enabled()
        )
        later = bronze_mirror.mirror_after_load(
            Catalog({"column_map": [75], "file_metadata": [72]}), "bronze", TABLES, SNAPSHOTS, RUN, source=Source(), glue=glue, config=enabled()
        )
    check(first["tables"]["column_map"].startswith("not mirrored") and later["tables"]["column_map"] == "created", f"{first} {later}")


def test_an_unexpected_failure_of_the_snapshot_read_is_still_a_failed_mirror() -> None:
    class Broken(Catalog):
        def table(self, name: str) -> Any:
            raise RuntimeError("catalog unreachable")

    glue = FakeGlue()
    with pyspark_functions():
        report = bronze_mirror.mirror_after_load(Broken({"column_map": [71]}), "bronze", TABLES, SNAPSHOTS, RUN, source=Source(), glue=glue, config=enabled())
    check(report["status"] == "failed" and "catalog unreachable" in report["reason"] and glue.writes() == [], f"{report}")
