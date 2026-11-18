"""Cases of the post-commit recovery snapshot that every publish writes, on a stand-in Spark session and stores (no Spark, Docker, AWS or Polaris).

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_committed_snapshot_cases``. The functions under test are the ones ``commit_publish``
calls (``committed_document``, ``write_committed_snapshot`` and ``ensure_committed_snapshot`` of ``scripts.lakehouse.gold_publish``) and the stores
(``FolderStore`` and ``S3Store``). The stand-in session answers the four statements those functions send (table existence, refs, metadata log and the run
row) from a dictionary, so each case sets the catalog state exactly: ``main`` of each table, the newest metadata file and the ``publish_runs`` rows. The cases
show what is written, when it is refused and that the snapshot is the one ``catalog_restore`` reads and resolves to the committed set. The order inside
``commit_publish`` (fast-forward, run row, snapshot, ``commit.json``) needs Spark: ``run_gold_publish_cases`` covers it (its ``recovery_snapshot`` case).
"""

from __future__ import annotations

import ast
import hashlib
import json
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from scripts.infrastructure.postgres_backup_fakes import StandInS3
from scripts.lakehouse import catalog_restore
from scripts.lakehouse import gold_publish as publish
from scripts.lakehouse.gold_publish import Context, FolderStore, Plan, PublishError, S3Store
from scripts.lakehouse.run_catalog_restore_e2e import ACCESS, BUCKET, MAIN, StandInPolaris

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT = REPO_ROOT / "data" / "e2e" / "committed_snapshot"
RUN = "gold_aaaaaaaa"
OTHER_RUN = "gold_bbbbbbbb"
SHA = "a" * 64
PLAN = Plan(
    tables={"fct_visit": ("visit_key",), "dim_site": ("site_sk",)},
    namespaces={"publish_fixture": "gold_fixture", "real": "gold"},
    branch_prefix="run_",
    runs_table="publish_runs",
    backup_prefix="lakehouse/catalog_backups/",
    retain_sets=2,
    expire_after_days=7,
    orphan_after_days=3,
)
SNAPSHOT_SET = {"fct_visit": 1101, "dim_site": 1202}
RUNS_MAIN = 9001


class Rows(list[Any]):
    def collect(self) -> list[Any]:
        return list(self)


class StandInSpark:
    """The catalog as a dictionary: ``tables`` maps a table to its refs and newest metadata file, ``runs`` holds the publish_runs rows."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[str, Any]] = {}
        self.runs: list[tuple[Any, ...]] = []
        self.catalog = SimpleNamespace(tableExists=lambda fq: fq in self.tables)

    def table(self, fq: str) -> Any:
        return SimpleNamespace(schema=SimpleNamespace(fields=[SimpleNamespace(name=name) for name, _ in publish.RUNS_COLUMNS]))

    def sql(self, text: str) -> Rows:
        source = text.split(" FROM ", 1)[1]
        if source.endswith(".refs"):
            return Rows((name, "BRANCH", snapshot) for name, snapshot in self.tables[source.removesuffix(".refs")]["refs"].items())
        if ".metadata_log_entries" in source:
            found = self.tables[source.split(".metadata_log_entries")[0]].get("file")
            return Rows([(found,)] if found else [])
        if " WHERE run_id = " in source:
            run = source.split("run_id = '", 1)[1].rstrip("'")
            return Rows(row for row in self.runs if row[0] == run)
        raise AssertionError(f"the stand-in does not know: {text}")


def metadata_file(namespace: str, table: str, sequence: int) -> str:
    name = hashlib.sha256(f"{namespace}.{table}.{sequence}".encode()).hexdigest()
    uuid = f"{name[:8]}-{name[8:12]}-{name[12:16]}-{name[16:20]}-{name[20:32]}"
    found = f"s3://{BUCKET}/lakehouse/{namespace}/{table}/metadata/{sequence:05d}-{uuid}.metadata.json"
    return found


def committed_catalog(run: str = RUN, snapshot_set: dict[str, int] | None = None) -> StandInSpark:
    """A catalog right after the commit of ``run``: main of each table is its snapshot, the branch is dropped and the run's row is merged."""
    spark = StandInSpark()
    wanted = snapshot_set or SNAPSHOT_SET
    for index, (name, snapshot) in enumerate(sorted(wanted.items()), start=5):
        spark.tables[f"hai_lakehouse.gold.{name}"] = {"refs": {"main": snapshot}, "file": metadata_file("gold", name, index)}
    spark.tables["hai_lakehouse.gold.publish_runs"] = {"refs": {"main": RUNS_MAIN}, "file": metadata_file("gold", "publish_runs", 11)}
    spark.runs = [
        (run, "real", datetime(2026, 11, 17, 12, tzinfo=UTC), SHA, "b" * 64, f"catalog_backup_{run}.json", "c" * 64, json.dumps(wanted, sort_keys=True), None)
    ]
    return spark


def context(spark: StandInSpark, store: Any, run: str = RUN) -> Context:
    return Context(spark, PLAN, run, "real", Path(tempfile.mkdtemp(prefix="committed_publish_")), SHA, store, accepted_result_sha256="d" * 64)


def new_store() -> FolderStore:
    return FolderStore(Path(tempfile.mkdtemp(prefix="committed_store_")))


def refused(action: Callable[[], Any]) -> str | None:
    try:
        action()
    except PublishError as error:
        return str(error)
    return None


def listing(store: FolderStore) -> list[str]:
    return sorted(path.name for path in store.root.iterdir() if path.is_file() and not path.name.endswith(".tmp")) if store.root.exists() else []


def case_writes_the_snapshot_once_from_the_committed_state() -> dict[str, bool]:
    store = new_store()
    ctx = context(committed_catalog(), store)
    result = publish.write_committed_snapshot(ctx, SNAPSHOT_SET)
    document = json.loads(store.get(publish.committed_name(RUN)))
    return {
        "one object named for the run is created": listing(store) == [f"catalog_committed_{RUN}.json"] and result["created"] is True,
        "it is a committed schema 2 snapshot of this run and namespace": (document["schema"], document["kind"], document["run_id"], document["namespace"])
        == (2, "committed", RUN, "gold"),
        "it holds the snapshot set and the main snapshot of each table": document["snapshot_set"] == SNAPSHOT_SET
        and {name: document["tables"][name]["main_snapshot_id"] for name in SNAPSHOT_SET} == SNAPSHOT_SET,
        "it holds the newest metadata file of each table and of the runs table": document["tables"]["fct_visit"]["metadata_file"]
        == metadata_file("gold", "fct_visit", 6)
        and document["tables"]["publish_runs"] == {"metadata_file": metadata_file("gold", "publish_runs", 11), "main_snapshot_id": RUNS_MAIN},
        "it names the run row's digests and no time": set(document["publish_run"]) == {"sealed_sha256", "export_manifest_sha256", "backup_sha256"}
        and "committed_at" not in json.dumps(document),
        "the result names the snapshot's digest": result["snapshot_sha256"] == hashlib.sha256(store.get(publish.committed_name(RUN))).hexdigest(),
    }


def case_it_refuses_a_table_whose_main_is_not_the_committed_snapshot() -> dict[str, bool]:
    store = new_store()
    spark = committed_catalog()
    spark.tables["hai_lakehouse.gold.dim_site"]["refs"] = {"main": 1, "run_aaaaaaaa": 1202}
    message = refused(lambda: publish.write_committed_snapshot(context(spark, store), SNAPSHOT_SET))
    return {
        "the step stops and names the table": message is not None and "dim_site" in message and "main" in message,
        "nothing is written": listing(store) == [],
    }


def case_it_refuses_when_the_run_row_is_missing_or_names_another_set() -> dict[str, bool]:
    checks = {}
    missing = committed_catalog()
    missing.runs = []
    other = committed_catalog()
    other.runs = [(*other.runs[0][:7], json.dumps({"fct_visit": 1, "dim_site": 2}), None)]
    for name, spark in (("no row", missing), ("a row with another set", other)):
        store = new_store()
        message = refused(lambda spark=spark, store=store: publish.write_committed_snapshot(context(spark, store), SNAPSHOT_SET))
        checks[f"{name}: refused, nothing written"] = message is not None and "publish_runs row" in message and listing(store) == []
    return checks


def case_it_refuses_a_table_without_a_metadata_file() -> dict[str, bool]:
    store = new_store()
    spark = committed_catalog()
    spark.tables["hai_lakehouse.gold.fct_visit"]["file"] = None
    message = refused(lambda: publish.write_committed_snapshot(context(spark, store), SNAPSHOT_SET))
    return {"refused": message is not None and "metadata file" in message, "nothing is written": listing(store) == []}


def case_an_identical_rerun_leaves_the_snapshot_alone() -> dict[str, bool]:
    store = new_store()
    ctx = context(committed_catalog(), store)
    publish.write_committed_snapshot(ctx, SNAPSHOT_SET)
    path = store.root / publish.committed_name(RUN)
    before = (path.read_bytes(), path.stat().st_mtime_ns, path.stat().st_ino)
    again = publish.ensure_committed_snapshot(ctx, SNAPSHOT_SET)
    return {
        "the rerun reports it was not created": again.get("created") is False and "skipped" not in again,
        "the object is the same file with the same bytes": (path.read_bytes(), path.stat().st_mtime_ns, path.stat().st_ino) == before,
        "no other object appeared": listing(store) == [path.name],
    }


def case_a_different_snapshot_of_the_run_is_refused_and_kept() -> dict[str, bool]:
    store = new_store()
    first = context(committed_catalog(), store)
    publish.write_committed_snapshot(first, SNAPSHOT_SET)
    path = store.root / publish.committed_name(RUN)
    kept = path.read_bytes()
    moved = committed_catalog()
    moved.tables["hai_lakehouse.gold.fct_visit"]["file"] = metadata_file("gold", "fct_visit", 99)
    message = refused(lambda: publish.ensure_committed_snapshot(context(moved, store), SNAPSHOT_SET))
    return {
        "the rerun stops and says it never replaces": message is not None and "never replaced" in message,
        "the existing snapshot is unchanged": path.read_bytes() == kept,
    }


def case_a_rerun_after_a_later_commit_writes_nothing() -> dict[str, bool]:
    store = new_store()
    later = committed_catalog(OTHER_RUN, {"fct_visit": 2101, "dim_site": 2202})
    ctx = context(later, store, run=RUN)
    answer = publish.ensure_committed_snapshot(ctx, SNAPSHOT_SET)
    return {"it reports that main moved": "main moved" in str(answer.get("skipped")), "nothing is written": listing(store) == []}


def case_a_rerun_after_an_interruption_writes_the_missing_snapshot() -> dict[str, bool]:
    store = new_store()
    answer = publish.ensure_committed_snapshot(context(committed_catalog(), store), SNAPSHOT_SET)
    return {"the snapshot is created by the rerun": answer.get("created") is True and listing(store) == [f"catalog_committed_{RUN}.json"]}


def case_the_stores_create_an_object_only_once() -> dict[str, bool]:
    folder = new_store()
    first, second = folder.put_new("one.json", b"first"), folder.put_new("one.json", b"second")
    client = StandInS3()
    s3 = S3Store(client, BUCKET, "lakehouse/silver/catalog_backups/")
    created, repeated = s3.put_new("catalog_committed_x.json", b"first"), s3.put_new("catalog_committed_x.json", b"second")
    failing = StandInS3()
    failing.put_error = ConnectionError("lost")
    try:
        S3Store(failing, BUCKET, "lakehouse/silver/catalog_backups/").put_new("x.json", b"1")
        raised = False
    except ConnectionError:
        raised = True
    return {
        "a folder creates once and keeps the first bytes": (first, second) == (True, False)
        and folder.get("one.json") == b"first"
        and listing(folder) == ["one.json"],
        "S3 creates once with a conditional write and keeps the first bytes": (created, repeated) == (True, False)
        and client.objects["lakehouse/silver/catalog_backups/catalog_committed_x.json"] == b"first"
        and all(put["if_none_match"] == "*" for put in client.puts),
        "any other S3 failure is raised, not read as 'exists'": raised,
    }


def case_the_commit_step_gets_a_backup_store_in_all_three_publish_modules() -> dict[str, bool]:
    checks = {}
    for path in ("scripts/lakehouse/gold_publish.py", "scripts/lakehouse/silver_publish.py", "scripts/features/features_publish.py"):
        tree = ast.parse((REPO_ROOT / path).read_text(encoding="utf-8"))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.IfExp) and isinstance(node.body, ast.Call) and getattr(node.body.func, "id", "") == "build_store"
        ]
        checks[f"{path}: the store is built for backup and commit"] = bool(calls) and all(
            isinstance(call.test, ast.Compare) and isinstance(call.test.ops[0], ast.In) and ast.literal_eval(call.test.comparators[0]) == ("backup", "commit")
            for call in calls
        )
    return checks


class Adapter:
    """A folder store seen as the bucket folders ``catalog_restore`` lists."""

    def __init__(self, store: FolderStore, prefix: str) -> None:
        self.store, self.prefix = store, prefix

    def names(self, prefix: str) -> list[str]:
        return [self.prefix + name for name in listing(self.store)] if prefix == self.prefix else []

    def read(self, key: str) -> bytes:
        return self.store.get(key.removeprefix(self.prefix))


def case_catalog_restore_reads_the_snapshot_and_a_reader_sees_the_committed_set() -> dict[str, bool]:
    store = new_store()
    spark = committed_catalog()
    publish.write_committed_snapshot(context(spark, store), SNAPSHOT_SET)
    for entry in json.loads(store.get(publish.committed_name(RUN)))["tables"].values():
        MAIN[entry["metadata_file"]] = entry["main_snapshot_id"]
    items = catalog_restore.read_items(Adapter(store, "lakehouse/catalog_backups/"), BUCKET)
    polaris = StandInPolaris(("gold",))
    rows = catalog_restore.restore_items(polaris, ACCESS, items, apply=True, deadline=100.0, clock=lambda: 0.0)
    resolved = {
        name: catalog_restore.main_of(polaris("GET", f"/api/catalog/v1/hai_lakehouse/namespaces/gold/tables/{name}", ACCESS)[1])
        for name in (*SNAPSHOT_SET, "publish_runs")
    }
    return {
        "the restore reads exactly the tables of the snapshot": sorted(item.table for item in items) == ["dim_site", "fct_visit", "publish_runs"],
        "every table is registered": {row["outcome"] for row in rows} == {"registered"},
        "every table resolves to its committed main snapshot": resolved == {**SNAPSHOT_SET, "publish_runs": RUNS_MAIN},
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    name.removeprefix("case_"): function for name, function in sorted(globals().items()) if name.startswith("case_")
}
CASE_ERRORS = (AssertionError, LookupError, OSError, TypeError, ValueError, AttributeError, NameError, ImportError, PublishError, catalog_restore.RestoreError)


def main() -> int:
    """Run every case, save the outcomes and return 1 when any check failed."""
    results: dict[str, dict[str, bool]] = {}
    for name, function in CASES.items():
        try:
            results[name] = function()
        except CASE_ERRORS as error:
            results[name] = {f"the case ended with {type(error).__name__}: {error}": False}
    passed = all(all(checks.values()) for checks in results.values())
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / f"report_{stamp}.json").write_text(json.dumps({"run_at_utc": stamp, "results": results, "passed": passed}, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        failed = [check for check, ok in checks.items() if not ok]
        sys.stdout.write(f"{'PASS' if not failed else 'FAIL'} {name}{': ' + '; '.join(failed) if failed else ''}\n")
    sys.stdout.write(f"{sum(all(checks.values()) for checks in results.values())} of {len(results)} cases passed\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
