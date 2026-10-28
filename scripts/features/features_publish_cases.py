"""Cases of the features publish on a local file-based Iceberg catalog (runs inside the project's Spark container, like ``run_gold_publish_cases``).

The launcher runs this module as the ``features_publish_e2e`` job (template ``spark_e2e``: network none, no Polaris, no S3, no secret):
``.venv/bin/python -m scripts.features.run_feature_store_e2e --mode publish_offline``. The host side of that mode builds the synthetic fixture with
``features_build`` in three runs (A, B with the same pin and data as A, and V2 with changed data), exports each through the shared consumer and writes
``inputs.json`` in the report folder with the export folder, run ID and sealed digest of each. The cases publish those exports with the gold publish
engine and the features plan (``features_publish.publish_plan``). The module writes ``cases_all.json`` into data/e2e/feature_store/<stamp>/ and holds
counts, names, hashes and snapshot IDs only, never a row value.

An exception raised at a named point of a step (``Faults``) stands for a killed process: the Iceberg state and the step records are on disk either
way.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import traceback
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.features import features_publish
from scripts.lakehouse import gold_publish as publish
from scripts.lakehouse.gold_publish import Context, Faults, FolderStore, InjectedFault, Plan
from scripts.lakehouse.run_gold_publish_cases import STEP_ORDER, Harness, mains, reader_view, refusal_of, run_steps, step

REPORT_ROOT = Path("data/e2e/feature_store")
MODE = "publish_fixture"
WIDE = "feature_matrix_hospital"
WINDOW = "feature_matrix_hospital_window"
EXACT = 120.12345678901234567
Check = dict[str, bool]
Case = Callable[["Features"], Check]
STAMP = ""
COLUMN_TYPES = "DESCRIBE {table}"
SELECT_VALUE = "SELECT c001__value__number FROM {table} VERSION AS OF {snapshot} WHERE spine_key = '990001:2022'"
COUNT_NULLS = "SELECT count(*) FROM {table} VERSION AS OF {snapshot} WHERE {column} IS NULL"
HAS_KEY = "SELECT count(*) FROM {table} VERSION AS OF {snapshot} WHERE spine_key = '{key}'"
INPUT_ROWS = "SELECT features_run_id, gold_run_id, feature_schema_sha256, hai_outcome_controls_sha256 FROM {table} VERSION AS OF {snapshot}"


class Features(Harness):
    """The gold harness (warehouse save and restore, sessions) with the features plan and the exports the host built."""

    inputs: dict[str, Any]

    def plan(self) -> Plan:
        return features_publish.publish_plan(MODE)

    def features_context(self, name: str, faults: Faults | None = None, work: str = "work") -> Context:
        """A context on a private copy of one export folder (``a``, ``b`` or ``v2``)."""
        entry = self.inputs["runs"][name]
        copy = self.root / work / f"{name}-{len(list((self.root / work).glob('*'))) if (self.root / work).exists() else 0}"
        shutil.copytree(entry["publish_dir"], copy)
        return Context(
            self.spark,
            self.plan(),
            entry["run_id"],
            MODE,
            copy,
            entry["sealed_sha256"],
            FolderStore(copy / "backup"),
            faults or Faults(),
            create_namespace=True,
            accepted_result_sha256=publish.sha256_file(copy / publish.RESULT_NAME),
        )

    def committed(self, name: str, label: str) -> Context:
        """Publish export ``name`` on a fresh warehouse and save the warehouse as ``label``."""
        self.fresh()
        ctx = self.features_context(name, work=f"{label}_work")
        run_steps(ctx)
        self.save(label)
        return ctx


def count(spark: Any, template: str, **parts: Any) -> int:
    return int(spark.sql(template.format(**parts)).collect()[0][0])


def head_matches(ctx: Context, row: dict[str, Any]) -> list[str]:
    """Per-table head verification of design 3.5: the tables whose ``main`` differs from the row's snapshot."""
    return [name for name, snapshot in sorted(row["snapshot_set"].items()) if publish.main_head(ctx, name) != int(snapshot)]


def happy_and_types(h: Features) -> Check:
    """E10: the whole sequence publishes every contract table; rows and fingerprints equal the export; types, nulls and the 17-digit double survive."""
    ctx = h.committed("a", "a")
    manifest, _, export_dir = publish.verified_export(ctx)
    head = publish.published_head(ctx)
    checks = h.begin()
    checks["publish_runs has the head row of the run"] = head is not None and head["run_id"] == ctx.run_id and set(head["snapshot_set"]) == set(ctx.plan.tables)
    if head is None:
        return checks
    equal = 0
    for name, item in manifest["tables"].items():
        view = publish.parquet_view(ctx, export_dir, name, item)
        wanted = publish.fingerprint(ctx, item["columns"], view)
        found = publish.fingerprint(ctx, item["columns"], f"{ctx.fq(name)} VERSION AS OF {head['snapshot_set'][name]}")
        equal += found == wanted and found[0] == int(item["rows"])
    checks["every table at the named snapshot equals the export (rows and fingerprint)"] = equal == len(manifest["tables"]) == len(ctx.plan.tables)
    types = {row[0]: row[1] for row in h.spark.sql(COLUMN_TYPES.format(table=ctx.fq(WINDOW))).collect()}
    checks["the window table keeps event_timestamp as a timestamp and bigint, double, string and boolean attributes"] = (
        types.get("event_timestamp") == "timestamp"
        and types.get("bed_count") == "bigint"
        and types.get("cmi") == "double"
        and types.get("state_code") == "string"
        and types.get("has_pos_snapshot") == "boolean"
    )
    wide = {row[0]: row[1] for row in h.spark.sql(COLUMN_TYPES.format(table=ctx.fq(WIDE))).collect()}
    checks["no wide column is a date or a 32-bit integer in the Iceberg table"] = not {"date", "int", "smallint", "tinyint"} & set(wide.values())
    snapshot = int(head["snapshot_set"][WIDE])
    value = h.spark.sql(SELECT_VALUE.format(table=ctx.fq(WIDE), snapshot=snapshot)).collect()
    checks["the 17-significant-digit double reads back exactly"] = len(value) == 1 and value[0][0] == EXACT
    exported = h.spark.read.parquet(str(export_dir / manifest["tables"][WIDE]["file"]))
    nulls = {column: exported.filter(exported[column].isNull()).count() for column in ("c001__value__number", "c002__value__category")}
    stored = {column: count(h.spark, COUNT_NULLS, table=ctx.fq(WIDE), snapshot=snapshot, column=column) for column in nulls}
    checks["null cells stay null (the null count of two columns equals the export's)"] = nulls == stored and nulls["c001__value__number"] > 0
    checks["main of every table is at the named snapshot and no run branch is left"] = mains(ctx) == head["snapshot_set"] and all(
        ctx.branch not in publish.table_refs(ctx, name) for name in ctx.plan.tables
    )
    return checks


def retry_and_new_publication(h: Features) -> Check:
    """E06a and E06b: a retry of one run changes nothing; a new run ID with the same data adds a row, equal fingerprints and a different provenance row only."""
    h.committed("a", "a")
    spark = h.restore("a")
    checks = h.begin()
    ctx = h.features_context("a", work="retry")
    before = {name: count(spark, "SELECT count(*) FROM {table}.snapshots", table=ctx.fq(name)) for name in [*ctx.plan.tables, ctx.plan.runs_table]}
    run_steps(ctx)
    again = step(ctx, "commit")
    after = {name: count(spark, "SELECT count(*) FROM {table}.snapshots", table=ctx.fq(name)) for name in [*ctx.plan.tables, ctx.plan.runs_table]}
    checks["a retry after the commit reports the run as committed"] = again.get("already_committed") is True
    checks["a retry after the commit creates no snapshot in any table"] = before == after
    checks["publish_runs holds the run once"] = [row["run_id"] for row in publish.published_runs(ctx)] == [ctx.run_id]
    first = publish.published_head(ctx)
    other = h.features_context("b", work="new")
    run_steps(other)
    second = publish.published_head(other)
    runs = [row["run_id"] for row in publish.published_runs(other)]
    checks["a new run ID adds a second publish_runs row"] = first is not None and second is not None and runs == [ctx.run_id, other.run_id]
    if first is None or second is None:
        return checks
    same = []
    for name in sorted(other.plan.tables):
        columns = [[field.name] for field in spark.table(other.fq(name)).schema.fields]
        old = publish.fingerprint(other, columns, f"{other.fq(name)} VERSION AS OF {first['snapshot_set'][name]}")
        new = publish.fingerprint(other, columns, f"{other.fq(name)} VERSION AS OF {second['snapshot_set'][name]}")
        if old == new:
            same.append(name)
    checks["every table except the inputs table has equal rows and fingerprint in both publications"] = set(other.plan.tables) - set(same) == {
        "feature_build_inputs"
    }
    old_inputs = spark.sql(INPUT_ROWS.format(table=other.fq("feature_build_inputs"), snapshot=first["snapshot_set"]["feature_build_inputs"])).collect()[0]
    new_inputs = spark.sql(INPUT_ROWS.format(table=other.fq("feature_build_inputs"), snapshot=second["snapshot_set"]["feature_build_inputs"])).collect()[0]
    checks["the provenance rows differ only in the features run ID"] = old_inputs[0] != new_inputs[0] and old_inputs[1:] == new_inputs[1:]
    return checks


def version_two(h: Features) -> Check:
    """E07: data version 2 changes, adds and removes rows; the old key is gone from the new set and still readable in the old one."""
    h.committed("a", "a")
    h.restore("a")
    checks = h.begin()
    old = h.features_context("a", work="old")
    first = publish.published_head(old)
    ctx = h.features_context("v2", work="v2")
    run_steps(ctx)
    second = publish.published_head(ctx)
    if first is None or second is None:
        return checks
    key = "990003:2023"
    table = ctx.fq(WINDOW)
    checks["the removed key is in the first set"] = count(h.spark, HAS_KEY, table=table, snapshot=first["snapshot_set"][WINDOW], key=key) == 1
    checks["the removed key is gone from the second set"] = count(h.spark, HAS_KEY, table=table, snapshot=second["snapshot_set"][WINDOW], key=key) == 0
    checks["the added key is in the second set only"] = (
        count(h.spark, HAS_KEY, table=table, snapshot=second["snapshot_set"][WINDOW], key="990006:2023") == 1
        and count(h.spark, HAS_KEY, table=table, snapshot=first["snapshot_set"][WINDOW], key="990006:2023") == 0
    )
    return checks


def interruption_between_fast_forwards(h: Features) -> Check:
    """E08 and E17's publisher half: a stop after the first fast-forward leaves the tables at different publications; the head verification names it, and the
    resume completes with every table at the new set."""
    h.committed("a", "a")
    spark = h.restore("a")
    checks = h.begin()
    probe = h.features_context("a", work="probe")
    before = publish.published_head(probe)
    first_table = sorted(probe.plan.tables)[0]
    faults = Faults(frozenset({f"commit:after_ff:{first_table}"}))
    ctx = h.features_context("v2", faults, work="fault")
    raised = False
    try:
        run_steps(ctx)
    except InjectedFault:
        raised = True
    head = publish.published_head(ctx)
    checks["the interruption fired between fast-forwards"] = raised
    checks["publish_runs still names the first publication"] = before is not None and head is not None and head["run_id"] == before["run_id"]
    checks["the head verification of the first publication names the table that moved"] = head is not None and head_matches(ctx, head) == [first_table]
    resume = replace(ctx, spark=spark, faults=Faults())
    run_steps(resume, STEP_ORDER[STEP_ORDER.index("commit") :])
    new = publish.published_head(resume)
    checks["the resume names the second publication and every table is at it"] = (
        new is not None and new["run_id"] == ctx.run_id and head_matches(resume, new) == []
    )
    return checks


def tamper(h: Features) -> Check:
    """E09: an export file changed after the consumer wrote it, and a candidate changed after the merge, are refused."""
    h.committed("a", "a")
    h.restore("a")
    checks = h.begin()
    ctx = h.features_context("v2", work="tamper_export")
    victim = next((ctx.path("export")).glob("*.parquet"))
    victim.write_bytes(victim.read_bytes() + b"x")
    checks["a changed export file is refused by the candidates step"] = "changed after the consumer wrote it" in refusal_of(lambda: step(ctx, "candidates"))
    probe = h.features_context("a", work="probe")
    checks["the first publication is unchanged"] = publish.published_head(probe) is not None
    checks["the reader's view is the first set"] = reader_view(probe) != {}
    return checks


CASES: dict[str, Case] = {
    "happy_and_types": happy_and_types,
    "retry_and_new_publication": retry_and_new_publication,
    "version_two": version_two,
    "interruption_between_fast_forwards": interruption_between_fast_forwards,
    "tamper": tamper,
}


def main(argv: Sequence[str] | None = None) -> int:
    global STAMP
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", nargs="+", default=[], choices=sorted(CASES))
    parser.add_argument("--stamp", default=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    STAMP = args.stamp
    out = REPORT_ROOT / STAMP
    out.mkdir(parents=True, exist_ok=True)
    inputs = json.loads((out / "inputs.json").read_text())
    results = []
    with tempfile.TemporaryDirectory(prefix="fp") as scratch:
        h = Features(Path(scratch))
        h.inputs = inputs
        for name in args.case or list(CASES):
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
    (out / f"cases_{'_'.join(args.case) if args.case else 'all'}.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(r["status"] == "pass" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
