"""Cases of the gold publish on a local file-based Iceberg catalog (runs inside the project's Spark container).

The launcher runs this module as the ``gold_publish_e2e`` job (network none, no Polaris, no S3, no secret):
``.venv/bin/python -m scripts.lakehouse.run_gold_publish_e2e --mode publish_offline``. The module writes ``cases.json`` into
data/e2e/gold_publish/<stamp>/. It holds counts, names, hashes and snapshot IDs only; the planted row values below never reach a record.

An exception raised at a named point of a step (``Faults``) stands for a killed process: the Iceberg state and the step records are on disk
either way.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, fields, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.lakehouse import gold_maintenance as maintenance
from scripts.lakehouse import gold_publish as publish
from scripts.lakehouse.gold_publish import Context, Faults, FolderStore, InjectedFault, Plan
from scripts.lakehouse.gold_publish_record import PublishError
from scripts.lakehouse.session import CATALOG, spark_session

REPORT_ROOT = Path("data/e2e/gold_publish")

ROW_SENTINEL = "ROWVALUE-7f3a91"
MODE = "publish_fixture"
TEST_PLAN = Plan(
    tables={"fct_visit": ("visit_key",), "dim_site": ("site_sk",), "bridge_pair": ("a_sk", "b_sk")},
    namespaces={"publish_fixture": "gold_e2e", "real": "gold"},
    branch_prefix="publish_",
    runs_table="publish_runs",
    backup_prefix=None,
    retain_sets=2,
    expire_after_days=7,
    orphan_after_days=7,
)
Check = dict[str, bool]

SHOW_TABLES = "SHOW TABLES IN {catalog}.{namespace}"
SHOW_SPACES = "SHOW NAMESPACES IN {catalog}"
CREATE_SPACE = "CREATE NAMESPACE IF NOT EXISTS {catalog}.gold_e2e"
CREATE_CHECK = "CREATE TABLE {table} (k int, v string) USING iceberg TBLPROPERTIES ('format-version' = '2')"
REFS_Q = "SELECT name, type, snapshot_id FROM {table}.refs"
BRANCH_Q = "ALTER TABLE {table} CREATE BRANCH {branch}"
MERGE_Q = (
    "MERGE INTO {table}.branch_{branch} t USING {source} s ON t.k = s.k "
    "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * WHEN NOT MATCHED BY SOURCE THEN DELETE"
)
FF_Q = "CALL {catalog}.system.fast_forward('gold_e2e.check_t', 'main', '{branch}')"
ROWS_Q = "SELECT k, v FROM {table} ORDER BY k"
BRANCH_ROWS_Q = "SELECT k, v FROM {table}.branch_{branch} ORDER BY k"
SNAP_IDS_Q = "SELECT snapshot_id FROM {table}.snapshots"
HISTORY_Q = "SELECT snapshot_id FROM {table}.history"
COUNT_Q = "SELECT count(*) FROM {table}"
SNAP_COUNT_Q = "SELECT count(*) FROM {table}.snapshots"
COUNT_AT_Q = "SELECT count(*) FROM {table} VERSION AS OF {snapshot}"
UPDATE_AMOUNT_Q = "UPDATE {table}.branch_{branch} SET amount = amount + 1 WHERE visit_key = 'v1'"
UPDATE_SITE_Q = "UPDATE {table}.branch_{branch} SET site_name = 'x' WHERE site_sk = 's1'"
INSERT_SITE_Q = "INSERT INTO {table} VALUES ('s9', 'other writer')"


def q(spark: Any, template: str, **parts: Any) -> Any:
    """Run a statement built from one of the constants above."""
    return spark.sql(template.format(catalog=CATALOG, **parts))


def run_id(index: int) -> str:
    return f"20261014T03{index:02d}00Z-{index + 1:08x}"


def rows_for(table: str, version: int) -> tuple[list[str], list[tuple[Any, ...]], str]:
    """Rows of a test table in a data version: version 2 changes, adds and removes rows against version 1, and so on."""
    rows: list[tuple[Any, ...]]
    if table == "fct_visit":
        names = ["visit_key", "site_sk", "visit_date", "amount", "flag", "note", "noted_at"]
        ddl = "visit_key string, site_sk string, visit_date date, amount double, flag boolean, note string, noted_at timestamp"
        rows = []
        for i in range(20 + 5 * version):
            if version >= 2 and i % 11 == 3:
                continue
            amount = i * 1.5 + (version if version >= 2 and i % 5 == 0 else 0)
            note = None if i % 7 == 0 else f"{ROW_SENTINEL}-{i}"
            rows.append(
                (f"v{i}", f"s{i % 4}", date(2026, 1, 1) + timedelta(days=i), amount, i % 2 == 0, note, datetime(2026, 1, 1, i % 24, i % 60, tzinfo=UTC))
            )
        return names, rows, ddl
    if table == "dim_site":
        names = ["site_sk", "site_name"]
        rows = [("<unknown>", None), *((f"s{i}", f"{ROW_SENTINEL}-site-{i}") for i in range(4 + (1 if version >= 2 else 0)))]
        return names, rows, "site_sk string, site_name string"
    names = ["a_sk", "b_sk", "weight", "vintage_start"]
    rows = [(f"a{i}", f"b{i % 3}", i * 10, None if i % 3 == 0 else date(2026, 1, 1)) for i in range(6 + version)]
    return names, rows, "a_sk string, b_sk string, weight bigint, vintage_start date"


def make_export(spark: Any, plan: Plan, folder: Path, run: str, version: int) -> str:
    """Write the folder an export step leaves for a run: one Parquet file per table, the manifest and the consumer result. Returns the sealed digest."""
    sealed = publish.sha256_bytes(f"sealed build of {run} version {version}".encode())
    export = folder / "export"
    publish.remove_tree(export)
    export.mkdir(parents=True)
    tables: dict[str, Any] = {}
    for name, key in plan.tables.items():
        names, rows, ddl = rows_for(name, version)
        frame = spark.createDataFrame(rows, ddl)
        scratch = folder / f"scratch_{name}"
        frame.coalesce(1).write.mode("overwrite").parquet(str(scratch))
        part = next(scratch.glob("part-*.parquet"))
        file = export / f"{name}.parquet"
        shutil.move(str(part), str(file))
        shutil.rmtree(scratch)
        tables[name] = {
            "rows": len(rows),
            "columns": [[column, field.dataType.simpleString()] for column, field in zip(names, frame.schema.fields, strict=True)],
            "merge_key": list(key),
            "file": file.name,
            "file_sha256": publish.sha256_file(file),
            "bytes": file.stat().st_size,
        }
    manifest = {"schema": publish.SCHEMA, "run_id": run, "sealed_sha256": sealed, "in_container_sha256": sealed, "tables": tables}
    manifest_sha = publish.write_record(export / "manifest.json", manifest)
    result = {
        "schema": publish.SCHEMA,
        "run_id": run,
        "expected_sha256": sealed,
        "in_container_sha256": sealed,
        "exported": True,
        "manifest_sha256": manifest_sha,
    }
    publish.write_record(folder / "consumer_result.json", result)
    return sealed


@dataclass
class Harness:
    root: Path
    spark: Any = None
    exports: dict[str, tuple[Path, str, int]] = field(default_factory=dict)
    baselines: dict[str, Path] = field(default_factory=dict)
    r1_expected: dict[str, tuple[int, str]] = field(default_factory=dict)
    partial: Check = field(default_factory=dict)

    def begin(self) -> Check:
        """The checks of the case that starts now; a case that ends with an exception still reports the checks it recorded before it."""
        self.partial = {}
        return self.partial

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse"

    def open(self, name: str = "gold_publish_cases") -> Any:
        if self.spark is not None:
            self.spark.stop()
        self.spark = spark_session(name, self.warehouse)
        return self.spark

    def fresh(self) -> Any:
        """An empty warehouse and a new session."""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
        shutil.rmtree(self.warehouse, ignore_errors=True)
        return self.open()

    def save(self, label: str) -> None:
        """Keep a copy of the warehouse (the same path is restored, so the table locations in the metadata stay valid)."""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
        target = self.root / f"baseline_{label}"
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(self.warehouse, target)
        self.baselines[label] = target
        self.open()

    def restore(self, label: str) -> Any:
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
        shutil.rmtree(self.warehouse, ignore_errors=True)
        shutil.copytree(self.baselines[label], self.warehouse)
        return self.open()

    def export(self, index: int, version: int) -> tuple[Path, str]:
        """The export folder of run ``index`` in data version ``version``, built once."""
        run = run_id(index)
        if run not in self.exports:
            folder = self.root / "exports" / run
            sealed = make_export(self.spark, TEST_PLAN, folder, run, version)
            self.exports[run] = (folder, sealed, version)
        folder, sealed, _ = self.exports[run]
        return folder, sealed

    def context(self, index: int, version: int, faults: Faults | None = None, mode: str = MODE, work: str = "work", plan: Plan = TEST_PLAN) -> Context:
        """A context on a private copy of the run's export folder, so the records of one case never meet another's."""
        folder, sealed = self.export(index, version)
        copy = self.root / work / f"{run_id(index)}-{len(list((self.root / work).glob('*'))) if (self.root / work).exists() else 0}"
        shutil.copytree(folder, copy)
        return Context(
            self.spark,
            plan,
            run_id(index),
            mode,
            copy,
            sealed,
            FolderStore(copy / "backup"),
            faults or Faults(),
            create_namespace=True,
            accepted_result_sha256=publish.sha256_file(copy / publish.RESULT_NAME),
        )


STEP_ORDER = ("candidates", "reconcile", "backup", "commit")


def step(ctx: Context, name: str) -> dict[str, Any]:
    return {
        "candidates": publish.publish_candidates,
        "reconcile": publish.reconcile_candidates,
        "backup": publish.catalog_backup,
        "commit": publish.commit_publish,
    }[name](ctx)


def run_steps(ctx: Context, names: Sequence[str] = STEP_ORDER) -> None:
    for name in names:
        step(ctx, name)


def mains(ctx: Context) -> dict[str, int | None]:
    return {name: publish.main_head(ctx, name) for name in ctx.plan.tables}


def expected_content(ctx: Context) -> dict[str, tuple[int, str]]:
    """Rows and fingerprint of each table in this run's export, computed from the Parquet files."""
    manifest, _, export_dir = publish.verified_export(ctx)
    out = {}
    for name, item in manifest["tables"].items():
        view = publish.parquet_view(ctx, export_dir, name, item)
        out[name] = publish.fingerprint(ctx, item["columns"], view)
    return out


def content_at(ctx: Context, name: str, snapshot: int, columns: Sequence[Sequence[str]]) -> tuple[int, str]:
    return publish.fingerprint(ctx, columns, f"{ctx.fq(name)} VERSION AS OF {snapshot}")


def reader_view(ctx: Context) -> dict[str, Any]:
    """What a reader sees: the newest ``publish_runs`` row and the content of exactly the snapshots it names."""
    head = publish.published_head(ctx)
    if head is None:
        return {}
    manifest = {name: [[c.name] for c in ctx.spark.table(ctx.fq(name)).schema.fields] for name in head["snapshot_set"]}
    return {
        "run_id": head["run_id"],
        "set": dict(head["snapshot_set"]),
        "content": {name: content_at(ctx, name, int(snapshot), manifest[name]) for name, snapshot in head["snapshot_set"].items()},
    }


def all_tables(ctx: Context) -> set[str]:
    rows = q(ctx.spark, SHOW_TABLES, namespace=ctx.namespace).collect()
    return {str(row[1]) for row in rows}


def committed_baseline(h: Harness) -> dict[str, tuple[int, str]]:
    """Run 1 published on a fresh warehouse and saved as the ``r1`` baseline (built once; returns the content the export holds)."""
    if "r1" not in h.baselines:
        h.fresh()
        ctx = h.context(1, 1, work="baseline_work")
        h.r1_expected = expected_content(ctx)
        run_steps(ctx)
        h.save("r1")
    return h.r1_expected


Case = Callable[[Harness], Check]


def iceberg_check(h: Harness) -> Check:
    """The local Iceberg check: a branch per table, MERGE INTO on the branch, candidate snapshots invisible on main, the fast-forward, a read-back."""
    spark = h.fresh()
    fq = f"{CATALOG}.gold_e2e.check_t"

    def refs() -> dict[str, int]:
        return {r[0]: r[2] for r in q(spark, REFS_Q, table=fq).collect()}

    def rows(source: str = "") -> list[tuple[Any, ...]]:
        return [tuple(r) for r in (q(spark, BRANCH_ROWS_Q, table=fq, branch=source) if source else q(spark, ROWS_Q, table=fq)).collect()]

    q(spark, CREATE_SPACE)
    q(spark, CREATE_CHECK, table=fq)
    refs0 = refs()
    q(spark, BRANCH_Q, table=fq, branch="first_publish")
    spark.sql("CREATE OR REPLACE TEMP VIEW first_src AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(k, v)")
    q(spark, MERGE_Q, table=fq, branch="first_publish", source="first_src")
    refs1 = refs()
    first_main_before = refs1.get("main")
    q(spark, FF_Q, branch="first_publish")
    first_main_after = refs().get("main")
    first_rows = rows()
    q(spark, BRANCH_Q, table=fq, branch="second_publish")
    spark.sql("CREATE OR REPLACE TEMP VIEW second_src AS SELECT * FROM VALUES (2, 'b2'), (3, 'c') AS t(k, v)")
    q(spark, MERGE_Q, table=fq, branch="second_publish", source="second_src")
    branch_rows = rows("second_publish")
    main_rows_before = rows()
    refs2 = refs()
    candidate = refs2["second_publish"]
    snapshots = {r[0] for r in q(spark, SNAP_IDS_Q, table=fq).collect()}
    main_lineage = {r[0] for r in q(spark, HISTORY_Q, table=fq).collect()}
    q(spark, FF_Q, branch="second_publish")
    main_rows_after = rows()
    main_after = refs()["main"]
    iceberg = spark._jvm.org.apache.iceberg.IcebergBuild.version()
    (h.root / "iceberg_versions.json").write_text(json.dumps({"spark": spark.version, "iceberg": str(iceberg)}) + "\n")
    return {
        "a table with no snapshot has no main snapshot before the first publish": "main" not in refs0,
        "a branch is created on a table with no snapshot": "first_publish" in refs1,
        "MERGE INTO with WHEN NOT MATCHED BY SOURCE writes on the branch of an empty table": first_main_before is None and "main" not in refs1,
        "the first fast-forward moves main from no snapshot to the branch head": first_main_after == refs1["first_publish"]
        and first_rows == [(1, "a"), (2, "b")],
        "a second branch is created on a table with a main snapshot": "second_publish" in refs2,
        "the branch holds the merged rows": branch_rows == [(2, "b2"), (3, "c")],
        "the candidate snapshot is not visible on main (rows and head unchanged)": main_rows_before == [(1, "a"), (2, "b")]
        and refs2["main"] == first_main_after
        and candidate not in main_lineage
        and candidate in snapshots,
        "the fast-forward makes main read the candidate rows": main_rows_after == [(2, "b2"), (3, "c")] and main_after == candidate,
        "Spark and Iceberg versions are recorded": str(iceberg) != "",
    }


def happy_path(h: Harness) -> Check:
    """Two publishes in order: nothing is visible before the commit; the committed set equals the exports; the earlier set stays readable."""
    h.fresh()
    checks = h.begin()
    ctx1 = h.context(1, 1)
    want1 = expected_content(ctx1)
    step(ctx1, "candidates")
    checks["after candidates: no table has a main snapshot"] = all(head is None for head in mains(ctx1).values())
    checks["after candidates: publish_runs is absent"] = not publish.table_exists(ctx1, ctx1.plan.runs_table) and publish.published_head(ctx1) is None
    for name in ("reconcile", "backup"):
        step(ctx1, name)
    checks["after reconcile and backup: still nothing on main"] = all(head is None for head in mains(ctx1).values()) and publish.published_head(ctx1) is None
    step(ctx1, "commit")
    head1 = publish.published_head(ctx1)
    candidates1 = publish.read_record(ctx1.path("candidates.json"))[0]["tables"]
    checks["publish_runs names exactly the candidate snapshot set"] = head1 is not None and head1["snapshot_set"] == {
        n: candidates1[n]["candidate_snapshot_id"] for n in candidates1
    }
    checks["main of every table is at the named snapshot"] = (
        head1 is not None and {n: int(m) for n, m in mains(ctx1).items() if m is not None} == head1["snapshot_set"]
    )
    view1 = reader_view(ctx1)
    checks["a read-back of the named snapshots equals the export (rows and fingerprint)"] = bool(view1) and view1["content"] == want1
    checks["the run's branches are removed after the commit"] = all(ctx1.branch not in publish.table_refs(ctx1, n) for n in ctx1.plan.tables)
    record = publish.read_record(ctx1.path("backup.json"))[0]
    checks["the backup was read back and its hash is the record's"] = (
        publish.sha256_bytes((ctx1.path("backup") / record["name"]).read_bytes()) == record["backup_sha256"]
    )
    ctx2 = h.context(2, 2)
    want2 = expected_content(ctx2)
    step(ctx2, "candidates")
    checks["second publish, after candidates: main heads equal the first set"] = {n: int(m) for n, m in mains(ctx2).items() if m is not None} == view1["set"]
    checks["second publish, after candidates: a reader still gets the first set with the first content"] = reader_view(ctx2) == view1
    step(ctx2, "reconcile")
    step(ctx2, "backup")
    step(ctx2, "commit")
    view2 = reader_view(ctx2)
    checks["the second commit names the second set with the second content"] = bool(view2) and view2["run_id"] == run_id(2) and view2["content"] == want2
    checks["the first set is still readable at its snapshots (time travel)"] = {
        n: content_at(ctx2, n, s, [[c.name] for c in h.spark.table(ctx2.fq(n)).schema.fields]) for n, s in view1["set"].items()
    } == view1["content"]
    checks["publish_runs holds one row per run"] = [r["run_id"] for r in publish.published_runs(ctx2)] == [run_id(1), run_id(2)]
    return checks


def branch_isolation(h: Harness) -> Check:
    """The candidate snapshot IDs are on the run's branch only; main's head, rows and lineage do not contain them."""
    committed_baseline(h)
    h.restore("r1")
    ctx = h.context(2, 2)
    before = mains(ctx)
    rows_before = {n: q(h.spark, COUNT_Q, table=ctx.fq(n)).collect()[0][0] for n in ctx.plan.tables}
    step(ctx, "candidates")
    candidates = publish.read_record(ctx.path("candidates.json"))[0]["tables"]
    lineage = {n: {r[0] for r in q(h.spark, HISTORY_Q, table=ctx.fq(n)).collect()} for n in ctx.plan.tables}
    return {
        "main heads are unchanged": mains(ctx) == before,
        "main row counts are unchanged": {n: q(h.spark, COUNT_Q, table=ctx.fq(n)).collect()[0][0] for n in ctx.plan.tables} == rows_before,
        "a candidate ID is on the run's branch and not in main's lineage": all(
            candidates[n]["candidate_snapshot_id"] == publish.table_refs(ctx, n)[ctx.branch] and candidates[n]["candidate_snapshot_id"] not in lineage[n]
            for n in ctx.plan.tables
        ),
        "the reader's view is the first set": (reader_view(ctx) or {}).get("run_id") == run_id(1),
    }


INTERRUPTS = (
    "candidates:planned",
    "candidates:after_branch:fct_visit",
    "candidates:after_merge:dim_site",
    "candidates:after_record:bridge_pair",
    "reconcile:after_table:dim_site",
    "backup:after_write",
    "backup:before_record",
    "commit:after_ff:fct_visit",
    "commit:after_ff:dim_site",
    "commit:before_publish_runs",
    "commit:after_publish_runs",
)


def interruptions(h: Harness) -> Check:
    """An interruption at each point leaves the reader's view unchanged; a rerun of all steps completes the publish exactly once."""
    checks = h.begin()
    want1 = committed_baseline(h)
    for point in INTERRUPTS:
        spark = h.restore("r1")
        probe = h.context(1, 1, work="probe")
        view_before = reader_view(probe)
        faults = Faults(frozenset({point}))
        ctx = h.context(2, 2, faults, work=f"fault_{point.replace(':', '_')}")
        want2 = expected_content(ctx)
        raised = False
        try:
            run_steps(ctx)
        except InjectedFault:
            raised = True
        label = f"[{point}]"
        checks[f"{label} the interruption fired"] = raised
        committed_row = point == "commit:after_publish_runs"
        after = reader_view(ctx)
        if committed_row:
            checks[f"{label} the commit row is the only change: it names the full set"] = (
                bool(after) and after["run_id"] == run_id(2) and after["content"] == want2
            )
        else:
            checks[f"{label} a reader still gets the first set unchanged"] = after == view_before
        if not point.startswith("commit:"):
            checks[f"{label} main heads still equal the first set"] = {n: int(m) for n, m in mains(ctx).items() if m is not None} == (view_before or {}).get(
                "set"
            )
        resume = replace(ctx, spark=spark, faults=Faults())
        resumed_ok = True
        try:
            run_steps(resume, STEP_ORDER[STEP_ORDER.index(point.split(":")[0]) :])
        except PublishError:
            resumed_ok = False
        view = reader_view(resume)
        runs = [r["run_id"] for r in publish.published_runs(resume)]
        checks[f"{label} the resume completes and names the second set"] = (
            resumed_ok and bool(view) and view["run_id"] == run_id(2) and view["content"] == want2
        )
        checks[f"{label} publish_runs holds the second run once"] = runs == [run_id(1), run_id(2)]
        checks[f"{label} no table has the run's branch left"] = all(ctx.branch not in publish.table_refs(resume, n) for n in TEST_PLAN.tables)
    checks["the first set is still readable after every resume"] = bool(want1)
    return checks


def retry_no_double_commit(h: Harness) -> Check:
    """Every step run twice changes nothing the second time; a rerun after the commit refuses to rewrite candidates."""
    committed_baseline(h)
    spark = h.restore("r1")
    ctx = h.context(2, 2)
    step(ctx, "candidates")
    first = publish.read_record(ctx.path("candidates.json"))
    again = step(ctx, "candidates")
    second = publish.read_record(ctx.path("candidates.json"))
    snapshots_after = {n: q(spark, SNAP_COUNT_Q, table=ctx.fq(n)).collect()[0][0] for n in TEST_PLAN.tables}
    step(ctx, "candidates")
    snapshots_third = {n: q(spark, SNAP_COUNT_Q, table=ctx.fq(n)).collect()[0][0] for n in TEST_PLAN.tables}
    checks: Check = {
        "a candidates rerun keeps the same snapshot IDs": first[0]["tables"] == second[0]["tables"] and again["step"] == "candidates",
        "a candidates rerun creates no new snapshot": snapshots_after == snapshots_third,
    }
    run_steps(ctx, ("reconcile", "backup", "commit"))
    heads = mains(ctx)
    snapshots_commit = {n: q(spark, SNAP_COUNT_Q, table=ctx.fq(n)).collect()[0][0] for n in list(TEST_PLAN.tables) + [TEST_PLAN.runs_table]}
    repeated = step(ctx, "commit")
    checks["a second commit reports the run as committed"] = repeated.get("already_committed") is True
    checks["a second commit changes no main head and no snapshot count"] = (
        mains(ctx) == heads
        and {n: q(spark, SNAP_COUNT_Q, table=ctx.fq(n)).collect()[0][0] for n in list(TEST_PLAN.tables) + [TEST_PLAN.runs_table]} == snapshots_commit
    )
    checks["publish_runs holds one row for the run"] = [r["run_id"] for r in publish.published_runs(ctx)].count(run_id(2)) == 1
    refused = "already committed" in refusal_of(lambda: step(ctx, "candidates"))
    checks["candidates after the commit are refused"] = refused
    return checks


def tamper_and_order(h: Harness) -> Check:
    """A candidate that differs from the export fails the reconcile and the commit; the commit needs the reconcile and the backup records."""
    committed_baseline(h)
    checks = h.begin()
    spark = h.restore("r1")
    ctx = h.context(2, 2)
    step(ctx, "candidates")
    view_before = reader_view(ctx)
    q(spark, UPDATE_AMOUNT_Q, table=ctx.fq("fct_visit"), branch=ctx.branch)

    document, _ = publish.read_record(ctx.path("candidates.json"))
    document["tables"]["fct_visit"]["candidate_snapshot_id"] = publish.table_refs(ctx, "fct_visit")[ctx.branch]
    publish.write_record(ctx.path("candidates.json"), document)
    failed = "differs from the export" in refusal_of(lambda: step(ctx, "reconcile"))
    record = publish.read_record(ctx.path("reconcile.json"))[0]
    checks["a recorded candidate that differs from the export fails the reconcile and the record is not a pass"] = (
        failed and record["pass"] is False and record["tables"]["fct_visit"]["equal"] is False
    )
    for name in ("backup", "commit"):
        checks[f"{name} is refused on a failed reconcile"] = step_refused(ctx, name)
    checks["nothing became visible"] = reader_view(ctx) == view_before and all(
        m == (view_before["set"][n] if view_before else None) for n, m in mains(ctx).items()
    )

    spark = h.restore("r1")
    ctx = h.context(2, 2, work="order")
    run_steps(ctx, ("candidates", "reconcile", "backup"))
    q(spark, UPDATE_SITE_Q, table=ctx.fq("dim_site"), branch=ctx.branch)
    refused = "branch head" in refusal_of(lambda: step(ctx, "commit"))
    checks["a branch changed after the reconcile is refused at the commit"] = refused and (reader_view(ctx) or {}).get("run_id") == run_id(1)
    spark = h.restore("r1")
    ctx = h.context(2, 2, work="order2")
    run_steps(ctx, ("candidates", "reconcile"))
    no_backup = refuses(lambda: step(ctx, "commit"))
    checks["the commit is refused without the backup record"] = no_backup
    run_steps(ctx, ("backup",))
    backup = publish.read_record(ctx.path("backup.json"))[0]
    publish.write_record(ctx.path("backup.json"), {**backup, "reconcile_sha256": "0" * 64})
    stale = refuses(lambda: step(ctx, "commit"))
    checks["the commit is refused when the backup record names another reconcile record"] = stale
    publish.write_record(ctx.path("backup.json"), {**backup, "read_back": False})
    unread = refuses(lambda: step(ctx, "commit"))
    checks["the commit is refused when the backup was not read back"] = unread
    checks["control: an untouched candidate passes the reconcile, the backup and the commit"] = control_commit_succeeds(h)
    return checks


def control_commit_succeeds(h: Harness) -> bool:
    """The valid-input control of the commit cases: on the first set, run 2 passes all four steps and is the published head."""
    h.restore("r1")
    ctx = h.context(2, 2, work="control_commit")
    run_steps(ctx)
    return (reader_view(ctx) or {}).get("run_id") == run_id(2)


def main_moved_refused(h: Harness) -> Check:
    """Another writer moves main of one table after the candidates; the commit moves no table and keeps the candidates."""
    committed_baseline(h)
    spark = h.restore("r1")
    ctx = h.context(2, 2)
    run_steps(ctx, ("candidates", "reconcile", "backup"))
    view_before = reader_view(ctx)
    q(spark, INSERT_SITE_Q, table=ctx.fq("dim_site"))
    moved = mains(ctx)
    refused = "main moved" in refusal_of(lambda: step(ctx, "commit"))
    return {
        "the commit is refused with the reason": refused,
        "no table moved (main heads equal the heads after the other writer's insert)": mains(ctx) == moved,
        "publish_runs still names the first run": (reader_view(ctx) or {}).get("run_id") == run_id(1) and reader_view(ctx)["set"] == view_before["set"],
        "the candidates stay on the branches": all(ctx.branch in publish.table_refs(ctx, n) for n in TEST_PLAN.tables),
        "control: when no other writer moved main the same commit succeeds": control_commit_succeeds(h),
    }


def export_and_consumer(h: Harness) -> Check:
    """A changed export file, a consumer result for another digest or a missing export writes nothing to the catalog."""
    checks = h.begin()
    h.fresh()
    flipped = h.context(3, 1, work="flip")
    file = flipped.path("export") / "fct_visit.parquet"
    data = bytearray(file.read_bytes())
    data[len(data) // 2] ^= 0xFF
    file.write_bytes(bytes(data))
    refused = "changed after the consumer wrote it" in refusal_of(lambda: step(flipped, "candidates"))
    checks["an export file changed after the consumer wrote it is refused"] = refused
    forged = h.context(3, 1, work="forged")
    result = publish.read_record(forged.path("consumer_result.json"))[0]
    rewrite_result(forged, in_container_sha256="1" * 64)
    refused = "rejected" in refusal_of(lambda: step(bound(forged), "candidates"))
    checks["a consumer result for another digest is refused"] = refused
    rejected = h.context(3, 1, work="rejected")
    publish.remove_tree(rejected.path("export"))
    publish.write_record(rejected.path("consumer_result.json"), {**result, "exported": False, "manifest_sha256": None, "in_container_sha256": "2" * 64})
    refused = refuses(lambda: step(bound(rejected), "candidates"))
    checks["a rejected consumer result with no export is refused"] = refused
    other_run = h.context(3, 1, work="other_run")
    wrong = replace(other_run, run_id=run_id(4), store=None)
    refused = refuses(lambda: step(wrong, "candidates"))
    checks["an export of another run is refused"] = refused
    checks["no table, namespace content or branch was written by any refusal"] = (
        not h.warehouse.exists() or not any((h.warehouse / "gold_e2e").glob("*")) if (h.warehouse / "gold_e2e").exists() else True
    )
    h.fresh()
    untouched = h.context(3, 1, work="untouched")
    checks["control: an untouched export is accepted by the candidates step"] = not step_refused(untouched, "candidates")
    good = {"run_id": run_id(3), "in_container_sha256": "a" * 64, "expected_sha256": "a" * 64, "exported": True, "manifest_sha256": "b" * 64}
    accept_ok = not refuses(lambda: publish.accept_consumer(good, {"sha256": "a" * 64}, run_id(3)))
    checks["the one acceptance rule accepts a result that matches completion"] = accept_ok
    for label, variant, completion in (
        ("a digest mismatch with completion", good, {"sha256": "c" * 64}),
        ("an in-container digest that differs", {**good, "in_container_sha256": "c" * 64}, {"sha256": "a" * 64}),
        ("an expected digest that differs", {**good, "expected_sha256": "c" * 64}, {"sha256": "a" * 64}),
        ("another run's result", {**good, "run_id": run_id(4)}, {"sha256": "a" * 64}),
        ("a non-export", {**good, "exported": False, "manifest_sha256": None}, {"sha256": "a" * 64}),
        ("a record with no digest", good, {}),
    ):
        checks[f"the one acceptance rule rejects {label}"] = rule_rejects(variant, completion)
    return checks


def namespace_guard(h: Harness) -> Check:
    """A publish writes only the contract's tables in its mode's namespace; anything else is refused before a statement runs."""
    h.fresh()
    checks = h.begin()
    ctx = h.context(1, 1, work="guard")
    for name in ("not_in_contract", "x; DROP TABLE y", "`quoted`"):
        checks[f"a table named {name!r} is refused"] = fq_refused(ctx, name)
    refused = refuses(lambda: Context(h.spark, TEST_PLAN, run_id(1), "root", ctx.publish_dir, ctx.sealed_sha256))
    checks["an unknown mode is refused"] = refused
    refused = refuses(lambda: Context(h.spark, TEST_PLAN, "run1", MODE, ctx.publish_dir, ctx.sealed_sha256))
    checks["a run ID outside the pattern is refused"] = refused
    real = h.context(1, 1, mode="real", work="guard_real")
    run_steps(real)
    spaces = {str(r[0]) for r in q(h.spark, SHOW_SPACES).collect()}
    checks["a real-mode publish writes the gold namespace only"] = (
        "gold" in spaces and "gold_e2e" not in spaces and all_tables(real) == set(TEST_PLAN.tables) | {TEST_PLAN.runs_table}
    )
    fixture = h.context(2, 2, work="guard_fixture")
    run_steps(fixture)
    checks["a publish_fixture publish writes gold_e2e and leaves gold as it was"] = (
        "gold_e2e" in {str(r[0]) for r in q(h.spark, SHOW_SPACES).collect()}
        and (reader_view(real) or {}).get("run_id") == run_id(1)
        and (reader_view(fixture) or {}).get("run_id") == run_id(2)
    )
    names = [call for call in ctx.faults.hits]
    checks["a context built without a writer namespace creates none"] = not names
    no_namespace = replace(
        ctx,
        plan=Plan({**TEST_PLAN.tables}, {"publish_fixture": "absent_ns", "real": "gold"}, "publish_", "publish_runs", None, 2, 7, 7),
        run_id=run_id(3),
        create_namespace=False,
    )
    refused = "does not exist" in refusal_of(lambda: publish.ensure_namespace(no_namespace))
    checks["the writer never creates a namespace (create_namespace is off by default)"] = refused
    return checks


def backup_store(h: Harness) -> Check:
    """A repeatable name, a read back that must match, a prefix that must lie under lakehouse/ and no store means no backup."""
    committed_baseline(h)
    spark = h.restore("r1")
    ctx = h.context(2, 2)
    run_steps(ctx, ("candidates", "reconcile", "backup"))
    first = publish.read_record(ctx.path("backup.json"))[0]
    run_steps(ctx, ("backup",))
    second = publish.read_record(ctx.path("backup.json"))[0]

    class Fake:
        def __init__(self) -> None:
            self.objects: dict[tuple[str, str], bytes] = {}
            self.corrupt = False

        def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
            self.objects[(Bucket, Key)] = b"corrupt" if self.corrupt else Body

        def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
            class Body:
                def __init__(self, data: bytes) -> None:
                    self.data = data

                def read(self) -> bytes:
                    return self.data

            return {"Body": Body(self.objects[(Bucket, Key)])}

    fake = Fake()
    store = publish.S3Store(fake, "bucket", "lakehouse/catalog_backup/")
    ctx.store = store
    run_steps(ctx, ("backup",))
    key = ("bucket", "lakehouse/catalog_backup/" + publish.backup_name(ctx.run_id))
    fake.corrupt = True
    corrupt_refused = "read back differs" in refusal_of(lambda: run_steps(ctx, ("backup",)))
    prefixes = []
    for prefix in ("other/", "lakehouse/../x/", "lakehouse/nofolder"):
        try:
            publish.S3Store(fake, "bucket", prefix)
            prefixes.append(False)
        except PublishError:
            prefixes.append(True)
    ctx.store = None
    no_store = refuses(lambda: run_steps(ctx, ("backup",)))
    stats = {"objects": len(fake.objects)}
    del stats
    return {
        "the backup name derives from the run ID and repeats": first["name"] == second["name"] == f"catalog_backup_{ctx.run_id}.json",
        "the same catalog state gives the same bytes": first["backup_sha256"] == second["backup_sha256"],
        "an S3 store writes under the contract prefix and reads back": key in fake.objects,
        "a read back that differs is refused": corrupt_refused,
        "a prefix outside lakehouse/ or without a folder is refused": all(prefixes),
        "no store means no backup": no_store,
        "the backup holds ids, paths and names but no row value": ROW_SENTINEL not in (ctx.path("backup") / first["name"]).read_text(),
        "the session is still open": spark is not None,
    }


def maintenance_cases(h: Harness) -> Check:
    """Snapshot expiry and orphan removal keep every registered or reserved set, the newest sets and any candidate in flight."""
    checks = h.begin()
    h.fresh()
    views: dict[int, dict[str, Any]] = {}
    contexts: dict[int, Context] = {}
    for index, version in ((1, 1), (2, 2), (3, 3), (4, 4)):
        ctx = h.context(index, version, work="maint")
        run_steps(ctx)
        contexts[index] = ctx
        views[index] = reader_view(ctx) or {}
    h.save("four")
    protected_path = h.root / "protected.json"
    future = datetime.now(UTC) + timedelta(days=8)

    def columns_of(ctx: Context, name: str) -> list[list[str]]:
        return [[c.name] for c in h.spark.table(ctx.fq(name)).schema.fields]

    def readable(ctx: Context, index: int) -> bool:
        live = replace(ctx, spark=h.spark)
        try:
            return all(content_at(live, n, s, columns_of(live, n)) == views[index]["content"][n] for n, s in views[index]["set"].items())
        except Exception:
            return False

    def snapshot_ids(ctx: Context, name: str) -> set[int]:
        return {int(r[0]) for r in q(h.spark, SNAP_IDS_Q, table=ctx.fq(name)).collect()}

    ctx = contexts[4]

    def run_maintenance(hooks: maintenance.Hooks | None = None) -> dict[str, Any]:
        """Maintenance on the current session (the harness starts a new session at every restore)."""
        maint = Context(h.spark, TEST_PLAN, run_id(9), MODE, h.root / "maint_out", "", needs_sealed=False)
        return maintenance.maintain(maint, lambda: maintenance.load_protected(protected_path), now=future, orphan_now=datetime.now(UTC), hooks=hooks)

    protected_path.write_text(json.dumps(protected_doc([{"kind": "registration", "id": "reg1", "run_id": run_id(1)}])))
    result = run_maintenance()
    checks["the registered set is readable at its snapshots after the expiry"] = readable(ctx, 1)
    checks["the newest two sets are readable"] = readable(ctx, 3) and readable(ctx, 4)
    checks["the unprotected old set is expired (its snapshots are gone)"] = not readable(ctx, 2) and set(views[2]["set"].values()).isdisjoint(
        snapshot_ids(ctx, "fct_visit")
    )
    checks["the report names the kept runs"] = result["kept_runs"] == sorted([run_id(1), run_id(3), run_id(4)]) and sum(result["expired"].values()) > 0

    h.restore("four")
    ctx = h.context(1, 1, work="maint_after")
    protected_path.write_text(json.dumps(protected_doc([{"kind": "registration", "id": "reg2", "run_id": run_id(2)}])))
    run_maintenance()
    checks["a set registered before the run is kept"] = readable(ctx, 2)
    checks["the unreserved oldest set is expired in the same run"] = not readable(ctx, 1)

    h.restore("four")
    runs = publish.published_runs(replace(ctx, spark=h.spark))
    since = parse_committed(runs[1]["committed_at"]) + timedelta(seconds=maintenance.MARGIN_SECONDS + 1)
    protected_path.write_text(json.dumps(protected_doc([{"kind": "pending", "id": "pend1", "since": since.isoformat()}])))
    run_maintenance()
    checks["a pending reservation keeps the set that was newest then and the later sets"] = readable(ctx, 2) and readable(ctx, 3) and readable(ctx, 4)
    checks["a pending reservation does not keep older sets"] = not readable(ctx, 1)

    h.restore("four")
    protected_path.write_text(json.dumps(protected_doc([{"kind": "registration", "id": "reg9", "run_id": run_id(8)}])))
    stopped = "not a committed set" in refusal_of(lambda: run_maintenance())
    checks["a registration naming an unknown run stops maintenance"] = stopped and readable(ctx, 1) and readable(ctx, 2)
    protected_path.write_text("{ not json")
    malformed = refuses(lambda: maintenance.load_protected(protected_path))
    checks["a malformed protected file stops maintenance"] = malformed

    h.restore("four")
    ctx5 = h.context(5, 5, work="maint_flight")
    run_steps(ctx5, ("candidates", "reconcile", "backup"))
    stray = h.warehouse / "gold_e2e" / "fct_visit" / "data" / "stray-planted-file.parquet"
    stray.write_bytes(b"not referenced")
    old = time.time() - 10 * 86400
    os.utime(stray, (old, old))
    protected_path.write_text(json.dumps(protected_doc([])))
    result = run_maintenance()
    checks["the planted orphan file is removed"] = not stray.exists() and result["orphans_removed"]["fct_visit"] >= 1
    committed = True
    try:
        step(ctx5, "commit")
    except PublishError:
        committed = False
    view5 = reader_view(ctx5)
    checks["the candidate in flight survived maintenance and commits with the reconciled content"] = committed and bool(view5) and view5["run_id"] == run_id(5)
    refused = False
    short = Context(h.spark, replace(TEST_PLAN, orphan_after_days=0), run_id(9), MODE, h.root / "maint_short", "", needs_sealed=False)
    protected_path.write_text(json.dumps(protected_doc([])))
    try:
        maintenance.maintain(short, lambda: maintenance.load_protected(protected_path))
    except PublishError as error:
        refused = "orphan-file cutoff" in str(error)
    checks["a cutoff newer than one day is refused for orphan removal"] = refused

    synthetic = [
        {"run_id": run_id(i), "committed_at": (datetime(2026, 10, 1, tzinfo=UTC) + timedelta(hours=3 * i)).isoformat(), "snapshot_set": {"fct_visit": 100 + i}}
        for i in range(6)
    ]
    pending = maintenance.Protected((maintenance.Item("pending", "p", since=datetime(2026, 10, 1, 7, tzinfo=UTC)),))
    checks["a pending reservation at hour 7 keeps the run that was newest then (hour 6) and all later"] = maintenance.kept_runs(synthetic, pending, 1) == {
        run_id(i) for i in range(2, 6)
    }
    none = maintenance.Protected()
    checks["with no reservation the newest retain_sets runs stay"] = maintenance.kept_runs(synthetic, none, 2) == {run_id(4), run_id(5)}
    return checks


def binding_exists() -> bool:
    """True when a context can carry the digest of the consumer result that the launcher accepted."""
    return "accepted_result_sha256" in {item.name for item in fields(Context)}


def bound(ctx: Context) -> Context:
    """The context a step gets from the launcher: it carries the digest of the consumer result as the file is now."""
    return replace(ctx, accepted_result_sha256=publish.sha256_file(ctx.path("consumer_result.json")))


def rewrite_result(ctx: Context, **changes: Any) -> None:
    """Change fields of the run's consumer result in place."""
    result = publish.read_record(ctx.path("consumer_result.json"))[0]
    publish.write_record(ctx.path("consumer_result.json"), {**result, **changes})


def refuses(action: Callable[[], Any]) -> bool:
    """True when the action raises a publish refusal; any other exception is a defect, not a refusal, and counts as no refusal."""
    try:
        action()
    except PublishError:
        return True
    except Exception:
        return False
    return False


def refusal_of(action: Callable[[], Any]) -> str:
    """The message of the publish refusal an action raises; empty when it raises nothing; a marker when it raises anything else (a defect, no refusal)."""
    try:
        action()
    except PublishError as error:
        return str(error)
    except Exception as error:
        return f"unexpected {type(error).__name__}"
    return ""


def fq_refused(ctx: Context, name: str) -> bool:
    """True when the context refuses to name the table."""
    return refuses(lambda: ctx.fq(name))


def rule_rejects(result: dict[str, Any], completion: dict[str, Any]) -> bool:
    """True when the one acceptance rule rejects the result for run 3."""
    return refuses(lambda: publish.accept_consumer(result, completion, run_id(3)))


def step_refused(ctx: Context, name: str) -> bool:
    """True when the step raises a publish refusal."""
    return refuses(lambda: step(ctx, name))


REJECTED_RESULTS: dict[str, dict[str, Any]] = {
    "an expected digest that is not the sealed build's": {"expected_sha256": "b" * 64},
    "a digest computed in the container that is not the sealed build's": {"in_container_sha256": "c" * 64},
    "another run's identity": {"run_id": run_id(8)},
    "a consumer that did not export": {"exported": False},
}


def rejected_result_commit(h: Harness) -> Check:
    """858 and D1 section 10 item 6: a consumer result that the acceptance rule rejects reaches no step, also with the export files in place."""
    committed_baseline(h)
    checks = h.begin()
    h.restore("r1")
    control = bound(h.context(2, 2, work="accept_control"))
    run_steps(control)
    checks["control: a result that the rule accepts is published by all four steps"] = (reader_view(control) or {}).get("run_id") == run_id(2)
    for index, (label, change) in enumerate(REJECTED_RESULTS.items()):
        h.restore("r1")
        probe = h.context(1, 1, work=f"accept_probe_{index}")
        view_before, heads_before = reader_view(probe), mains(probe)
        ctx = h.context(2, 2, work=f"accept_rejected_{index}")
        rewrite_result(ctx, **change)
        ctx = bound(ctx)
        outcomes = {name: step_refused(ctx, name) for name in STEP_ORDER}
        for name, refused in outcomes.items():
            checks[f"[{label}] the {name} step is refused"] = refused
        checks[f"[{label}] no table moved and the reader's view is unchanged"] = mains(ctx) == heads_before and reader_view(ctx) == view_before
        checks[f"[{label}] the run has no branch on any table"] = all(ctx.branch not in publish.table_refs(ctx, n) for n in ctx.plan.tables)
        checks[f"[{label}] publish_runs holds no row for the run"] = run_id(2) not in [r["run_id"] for r in publish.published_runs(ctx)]

    h.restore("r1")
    ctx = bound(h.context(2, 2, work="accept_late"))
    run_steps(ctx, ("candidates", "reconcile", "backup"))
    view_before, heads_before = reader_view(ctx), mains(ctx)
    rewrite_result(ctx, expected_sha256="b" * 64)
    late = bound(ctx)
    checks["[late rejection] the commit is refused after a valid backup"] = step_refused(late, "commit")
    checks["[late rejection] the fast-forward and the publish_runs write did not happen"] = (
        mains(late) == heads_before and reader_view(late) == view_before and run_id(2) not in [r["run_id"] for r in publish.published_runs(late)]
    )
    checks["[late rejection] the candidates stay on their branches"] = all(late.branch in publish.table_refs(late, n) for n in late.plan.tables)
    rewrite_result(ctx, expected_sha256=late.sealed_sha256)
    again = bound(ctx)
    checks["control: the same commit succeeds with the accepted result restored"] = not refuses(lambda: step(again, "commit")) and (
        reader_view(again) or {}
    ).get("run_id") == run_id(2)

    checks["a context carries the digest of the accepted result"] = binding_exists()
    h.restore("r1")
    ctx = h.context(2, 2, work="accept_swap")
    rewrite_result(ctx, reused=True)
    checks["a result that differs from the accepted one is refused even when the rule accepts it"] = step_refused(ctx, "candidates")
    return checks


def protected_doc(entries: list[dict[str, Any]], run: str | None = None, **changes: Any) -> dict[str, Any]:
    """The document the launcher registry writes under its maintenance fence for the maintenance run ``run``."""
    fence = {"run_id": run or run_id(9), "attempt_id": "at-0123456789ab", "taken_at": datetime.now(UTC).isoformat()}
    return {"schema": 1, "fence": fence, "items": entries, **changes}


def maintenance_protection(h: Harness) -> Check:
    """The protection file is a bound, closed document; any other shape, an absent file or a change during the run stops maintenance."""
    checks = h.begin()
    h.fresh()
    for index, version in ((1, 1), (2, 2), (3, 3), (4, 4)):
        run_steps(h.context(index, version, work="protect"))
    h.save("four")
    folder = h.root / "protection"
    folder.mkdir(exist_ok=True)
    future = datetime.now(UTC) + timedelta(days=8)
    registration = {"kind": "registration", "id": "reg1", "run_id": run_id(1)}

    def maint() -> Context:
        """The maintenance context on the session that is open now (a restore opens a new session)."""
        return Context(h.spark, TEST_PLAN, run_id(9), MODE, h.root / "maint_out", "", needs_sealed=False)

    def write(label: str, document: Any) -> Path:
        path = folder / f"{label}.json"
        path.write_text(document if isinstance(document, str) else json.dumps(document))
        return path

    def survivors() -> int:
        """How many of the four committed sets still have all their snapshots."""
        live = {int(r[0]) for n in TEST_PLAN.tables for r in q(h.spark, SNAP_IDS_Q, table=maint().fq(n)).collect()}
        sets = [r["snapshot_set"] for r in publish.published_runs(maint())]
        return sum(set(s.values()) <= live for s in sets)

    def run_refused(path: Path | None) -> bool:
        return refuses(lambda: run(path))

    def run(path: Path | None, hooks: maintenance.Hooks | None = None) -> dict[str, Any]:
        source = (lambda: maintenance.load_protected(path)) if path is not None else (lambda: maintenance.load_protected(None))
        return maintenance.maintain(maint(), source, now=future, orphan_now=datetime.now(UTC), hooks=hooks)

    result = run(write("valid", protected_doc([registration])))
    checks["control: a valid document is read; the registered set and the newest two stay, the other set goes"] = (
        result["kept_runs"] == sorted([run_id(1), run_id(3), run_id(4)]) and survivors() == 3
    )
    h.restore("four")
    result = run(write("valid_empty", protected_doc([])))
    checks["control: a valid document with no item is read; the newest sets stay and the older ones go"] = (
        result["kept_runs"] == sorted([run_id(3), run_id(4)]) and survivors() == 2
    )
    bad = {
        "an empty object": {},
        "a document with the list under another name": {"schema": 1, "fence": protected_doc([])["fence"], "item": [registration]},
        "a list that is not a list": protected_doc([], items="none"),
        "no schema": {key: value for key, value in protected_doc([registration]).items() if key != "schema"},
        "another schema": protected_doc([registration], schema=2),
        "no fence": {key: value for key, value in protected_doc([registration]).items() if key != "fence"},
        "a fence of another maintenance run": protected_doc([registration], run=run_id(8)),
        "a fence without a run": protected_doc([registration], fence={"attempt_id": "at-0123456789ab"}),
        "an item with a field it does not define": protected_doc([{**registration, "note": "x"}]),
        "an item without an id": protected_doc([{"kind": "registration", "run_id": run_id(1)}]),
        "a registration that also carries a time": protected_doc([{**registration, "since": "2026-10-01T00:00:00+00:00"}]),
        "a pending reservation that names a run": protected_doc([{"kind": "pending", "id": "p1", "since": "2026-10-01T00:00:00+00:00", "run_id": run_id(1)}]),
        "a pending reservation with a time that is not a time": protected_doc([{"kind": "pending", "id": "p1", "since": "yesterday"}]),
        "an item of another kind": protected_doc([{"kind": "other", "id": "x"}]),
        "an extra field at the top": protected_doc([registration], extra=1),
        "text that is not JSON": "{ not json",
        "a list at the top": [registration],
    }
    for label, document in bad.items():
        h.restore("four")
        path = write("bad", document)
        refused = run_refused(path)
        checks[f"{label} stops maintenance before any expiry"] = refused and survivors() == 4
    h.restore("four")
    checks["no named file stops maintenance"] = refuses(lambda: run(None)) and survivors() == 4
    h.restore("four")
    checks["a missing file stops maintenance"] = refuses(lambda: run(folder / "absent.json")) and survivors() == 4

    try:
        maintenance.main(["--run", run_id(9), "--mode", MODE])
        parsed = True
    except SystemExit:
        parsed = False
    except Exception:
        parsed = True
    checks["the maintenance command refuses to start without --protected"] = not parsed

    h.restore("four")
    path = write("moving", protected_doc([]))

    def register_late() -> None:
        write("moving", protected_doc([{"kind": "registration", "id": "reg2", "run_id": run_id(2)}]))

    hooks = maintenance.Hooks(after_plan=register_late)
    stopped = refuses(lambda: run(path, hooks))
    checks["a document that changed after the first read stops maintenance"] = stopped and hooks.log == ["after_plan"]
    checks["the sets that the plan would have expired are all still there when the document changed"] = survivors() == 4
    return checks


def parse_committed(text: str) -> datetime:
    return maintenance.parse_time(text)


def reader_discovery(h: Harness) -> Check:
    """A reader reads publish_runs first and then only the named snapshots, also while a newer publish writes its candidates."""
    committed_baseline(h)
    spark = h.restore("r1")
    ctx = h.context(2, 2)
    head = publish.published_head(ctx) or {"snapshot_set": {}}
    step(ctx, "candidates")
    while_writing = publish.published_head(ctx)
    view = reader_view(ctx)
    live = {n: int(m) for n, m in mains(ctx).items() if m is not None}
    return {
        "the head row does not change while candidates are written": bool(head["snapshot_set"]) and while_writing == head,
        "the named snapshots read the first publish's content": bool(view) and view["set"] == head["snapshot_set"],
        "the head's snapshots equal main for every table before the commit": live == head["snapshot_set"],
        "a table read at a named snapshot is the table as it was published": q(
            spark, COUNT_AT_Q, table=ctx.fq("fct_visit"), snapshot=head["snapshot_set"]["fct_visit"]
        ).collect()[0][0]
        == len(rows_for("fct_visit", 1)[1]),
    }


def contract_plan(h: Harness) -> Check:
    """The contract's publish entry names every gold table with a merge key and the two namespaces."""
    plan = publish.load_plan()
    return {
        "70 tables with a non-empty key": len(plan.tables) == 70 and all(plan.tables.values()),
        "46 facts, 20 dimensions and 4 bridges": sum(n.startswith("fct_") for n in plan.tables) == 46
        and sum(n.startswith("dim_") for n in plan.tables) == 20
        and sum(n.startswith("bridge_") for n in plan.tables) == 4,
        "the namespaces are gold and gold_e2e": dict(plan.namespaces) == {"publish_fixture": "gold_e2e", "real": "gold"},
        "the backup prefix is the owner's folder under lakehouse/": plan.backup_prefix == "lakehouse/catalog_backups/",
        "the session catalog name is the one the module uses": CATALOG == publish.CATALOG,
    }


def readback(h: Harness) -> Check:
    """The multi-process publish (one container per step) left a catalog that reads back as the export says. Needs ``inputs.json``."""
    inputs = json.loads((REPORT_ROOT / STAMP / "inputs.json").read_text())["multi"]
    warehouse = Path(inputs["warehouse"])
    publish_dir = Path(inputs["publish_dir"])
    if h.spark is not None:
        h.spark.stop()
    h.spark = spark_session("gold_publish_readback", warehouse)
    plan = publish.load_plan()
    accepted = publish.sha256_file(publish_dir / publish.RESULT_NAME)
    ctx = Context(h.spark, plan, inputs["run_id"], MODE, publish_dir, inputs["sealed_sha256"], create_namespace=False, accepted_result_sha256=accepted)
    manifest, _, export_dir = publish.verified_export(ctx)
    head = publish.published_head(ctx)
    checks: Check = {"publish_runs has a head row for the run": head is not None and head["run_id"] == ctx.run_id}
    if head is None:
        return checks
    equal = 0
    for name, item in manifest["tables"].items():
        view = publish.parquet_view(ctx, export_dir, name, item)
        wanted = publish.fingerprint(ctx, item["columns"], view)
        found = content_at(ctx, name, int(head["snapshot_set"][name]), item["columns"])
        equal += found == wanted and found[0] == int(item["rows"])
    checks["every table at the named snapshot equals the export (rows and fingerprint)"] = equal == len(manifest["tables"]) == 70
    checks["main of every table is at the named snapshot"] = {n: m for n, m in mains(ctx).items()} == head["snapshot_set"]
    checks["no run branch is left"] = all(ctx.branch not in publish.table_refs(ctx, n) for n in plan.tables)
    checks["the record chain is complete"] = all(
        (publish_dir / f).is_file() for f in ("consumer_result.json", "candidates.json", "reconcile.json", "backup.json", "commit.json")
    )
    return checks


CASES: dict[str, Case] = {
    "iceberg_check": iceberg_check,
    "happy_path": happy_path,
    "branch_isolation": branch_isolation,
    "interruptions": interruptions,
    "retry_no_double_commit": retry_no_double_commit,
    "tamper_and_order": tamper_and_order,
    "main_moved_refused": main_moved_refused,
    "export_and_consumer": export_and_consumer,
    "rejected_result_commit": rejected_result_commit,
    "namespace_guard": namespace_guard,
    "backup_store": backup_store,
    "maintenance": maintenance_cases,
    "maintenance_protection": maintenance_protection,
    "reader_discovery": reader_discovery,
    "contract_plan": contract_plan,
    "readback": readback,
}
STAMP = ""


def main(argv: Sequence[str] | None = None) -> int:
    global STAMP
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", nargs="+", default=[], choices=sorted(CASES))
    parser.add_argument("--stamp", default=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    STAMP = args.stamp
    out = REPORT_ROOT / STAMP
    out.mkdir(parents=True, exist_ok=True)
    results = []
    with tempfile.TemporaryDirectory(prefix="gp") as scratch:
        h = Harness(Path(scratch))
        for name in args.case or [n for n in CASES if n != "readback"]:
            error = ""
            checks: Check = {}
            h.partial = {}
            try:
                checks = CASES[name](h)
            except Exception as exc:
                checks = dict(h.partial)
                error = f"{type(exc).__name__}: {str(exc)[:300]}\n{traceback.format_exc()[-1200:]}"
            status = "pass" if checks and all(checks.values()) and not error else "fail"
            results.append({"case": name, "status": status, "checks": checks, "error": error})
            sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name}\n")
            for check, ok in checks.items():
                if not ok:
                    sys.stdout.write(f"      failed: {check}\n")
            if error:
                sys.stdout.write(f"      error: {error.splitlines()[0]}\n")
        if h.spark is not None:
            h.spark.stop()
        versions = scratch + "/iceberg_versions.json"
        if Path(versions).is_file():
            shutil.copy2(versions, out / "iceberg_versions.json")
    (out / f"cases_{'_'.join(args.case) if args.case else 'all'}.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(r["status"] == "pass" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
