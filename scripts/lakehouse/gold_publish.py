"""Publish the sealed gold build to the Iceberg gold namespace.

Every step is one launcher job. The steps share records in the run's publish folder; each record names the hash of the record before it, so a
step runs only on the exact output of the step before:

- the export (``scripts/lakehouse/gold_export.py``, the consumer): it hashes the mounted sealed copy inside the container, refuses when the digest
  differs from the expected one and otherwise writes one Parquet file per gold table and a manifest (the quality image, network none). The launcher
  accepts its result by ``accept_consumer`` and records the digest of that exact result; every step below runs only against that digest.
- ``candidates``: one Iceberg branch per table and run; ``MERGE INTO`` the export on the branch. Nothing changes on ``main``.
- ``reconcile``: rows and a content fingerprint of exactly the candidate snapshot IDs against the export.
- ``backup``: the catalog state written under a repeatable name and read back.
- ``commit``: fast-forward each table to its reconciled candidate, then write the ``publish_runs`` row last. Readers read that row first and
  read only the snapshot IDs it names.

Spark runs inside the project's Spark image; this module imports no Spark package at load time. It records counts, names, hashes and snapshot
IDs only, never row values or credentials.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from scripts.lakehouse.gold_publish_record import IDENT_MAX, RESULT_NAME, RUN_ID, SHA256, PublishError, accept_consumer, read_record, sha256_bytes

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT = REPO_ROOT / "data_contracts/lakehouse/gold_contract.json"

CATALOG = "hai_lakehouse"
MODES = ("publish_fixture", "real")
STEPS = ("candidates", "reconcile", "backup", "commit")
IDENT = re.compile(rf"[A-Za-z_][A-Za-z0-9_]{{0,{IDENT_MAX - 1}}}")
BACKUP_NAME = re.compile(r"catalog_backup_[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}\.json")
SCHEMA = 1

DEFAULT_PUBLISH_DIR = "/workspace/out/publish"


ABSENT_NAMESPACE = ("SCHEMA_NOT_FOUND", "NAMESPACE_NOT_FOUND", "does not exist")
NAMESPACE_EXISTS = "DESCRIBE NAMESPACE {catalog}.{namespace}"
CREATE_NAMESPACE = "CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}"
CREATE_TABLE = "CREATE TABLE IF NOT EXISTS {table} ({columns}) USING iceberg TBLPROPERTIES ('format-version' = '2')"
REFS = "SELECT name, type, snapshot_id FROM {table}.refs"
SNAPSHOTS = "SELECT snapshot_id, parent_id, operation, committed_at FROM {table}.snapshots"
METADATA_LOG = "SELECT file FROM {table}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1"
CREATE_BRANCH = "ALTER TABLE {table} CREATE BRANCH {branch}"
DROP_BRANCH = "ALTER TABLE {table} DROP BRANCH IF EXISTS {branch}"
FAST_FORWARD = "CALL {catalog}.system.fast_forward('{namespace}.{name}', 'main', '{branch}')"
FINGERPRINT = "SELECT count(*) AS n, CAST(coalesce(sum(CAST(xxhash64({columns}, {mask}) AS DECIMAL(38, 0))), 0) AS STRING) AS fp FROM {source}"
MERGE = "MERGE INTO {table}.branch_{branch} AS t USING {view} AS s ON {on} {matched}WHEN NOT MATCHED THEN INSERT * WHEN NOT MATCHED BY SOURCE THEN DELETE"
MERGE_MATCHED = "WHEN MATCHED AND NOT ({same}) THEN UPDATE SET * "
RUNS_COLUMNS = (
    ("run_id", "STRING"),
    ("mode", "STRING"),
    ("committed_at", "TIMESTAMP"),
    ("sealed_sha256", "STRING"),
    ("export_manifest_sha256", "STRING"),
    ("backup_name", "STRING"),
    ("backup_sha256", "STRING"),
    ("snapshot_set", "STRING"),
)
MERGE_RUN = "MERGE INTO {table} AS t USING {view} AS s ON t.run_id = s.run_id WHEN NOT MATCHED THEN INSERT *"
SELECT_RUN = "SELECT {columns} FROM {table} WHERE run_id = '{run_id}'"
SELECT_HEAD = "SELECT {columns} FROM {table} ORDER BY committed_at DESC, run_id DESC LIMIT 1"
SELECT_ALL_RUNS = "SELECT {columns} FROM {table} ORDER BY committed_at ASC, run_id ASC"


class InjectedFault(Exception):
    """An interruption that a test asked for at a named point of a step."""


@dataclass
class Faults:
    """Interruption points for the end-to-end cases. The command line never builds one; only a case passes it to a step."""

    points: frozenset[str] = frozenset()
    hits: list[str] = field(default_factory=list)

    def hit(self, point: str) -> None:
        """Raise at a named point when a case asked for it."""
        self.hits.append(point)
        if point in self.points:
            raise InjectedFault(point)


def ident(name: str) -> str:
    """A name that is safe to place in a statement."""
    if not IDENT.fullmatch(name):
        raise PublishError(f"{name!r} is not a plain identifier")
    return name


def quote(name: str) -> str:
    """A column name quoted for Spark."""
    return "`" + ident(name) + "`"


def sha256_file(path: Path) -> str:
    """SHA-256 of a file, read in blocks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical(document: Mapping[str, Any]) -> bytes:
    """The bytes of a record: sorted keys, two-space indent, a final newline."""
    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()


def write_record(path: Path, document: Mapping[str, Any]) -> str:
    """Write a record atomically (a temporary sibling, flushed, then renamed); returns its SHA-256."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = canonical(document)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise
    return sha256_bytes(data)


def branch_name(prefix: str, run_id: str) -> str:
    """The run's branch name: the plan's prefix and the run ID in lower case with underscores."""
    if not RUN_ID.fullmatch(run_id):
        raise PublishError(f"{run_id!r} is not a run ID")
    return ident(prefix + run_id.lower().replace("-", "_"))


def backup_name(run_id: str) -> str:
    """The repeatable name of the run's catalog backup."""
    name = f"catalog_backup_{run_id}.json"
    if not BACKUP_NAME.fullmatch(name):
        raise PublishError(f"{run_id!r} is not a run ID")
    return name


@dataclass(frozen=True)
class Plan:
    """What the gold contract's ``publish`` entry says: the tables with their merge keys, the namespaces by mode and the settings."""

    tables: Mapping[str, tuple[str, ...]]
    namespaces: Mapping[str, str]
    branch_prefix: str
    runs_table: str
    backup_prefix: str | None
    retain_sets: int
    expire_after_days: int
    orphan_after_days: int

    @property
    def writable(self) -> frozenset[str]:
        """The tables the writer may touch: the contract's tables and the runs table."""
        return frozenset([*self.tables, self.runs_table])


def load_plan(path: Path = CONTRACT) -> Plan:
    """Read the ``publish`` entry of the gold contract."""
    document = json.loads(path.read_text())
    entry = document["publish"]
    tables = {name: tuple(ident(column) for column in item["merge_key"]) for name, item in sorted(entry["tables"].items())}
    for name in tables:
        ident(name)
    namespaces = {mode: ident(namespace) for mode, namespace in entry["namespaces"].items()}
    if set(namespaces) != set(MODES):
        raise PublishError(f"the contract names namespaces for {sorted(namespaces)}, not for {list(MODES)}")
    maintenance = entry["maintenance"]
    return Plan(
        tables=tables,
        namespaces=namespaces,
        branch_prefix=ident(entry["branch_prefix"]),
        runs_table=ident(entry["runs_table"]),
        backup_prefix=entry.get("backup_prefix"),
        retain_sets=int(maintenance["retain_sets"]),
        expire_after_days=int(maintenance["expire_after_days"]),
        orphan_after_days=int(maintenance["orphan_after_days"]),
    )


class BackupStore(Protocol):
    """Where the catalog backup goes: write a named object and read it back."""

    def put(self, name: str, data: bytes) -> None: ...

    def get(self, name: str) -> bytes: ...


@dataclass
class FolderStore:
    """A folder on a mounted path (the offline runs)."""

    root: Path

    def put(self, name: str, data: bytes) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(dir=self.root, prefix=name + ".", suffix=".tmp")
        with os.fdopen(handle, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.root / name)

    def get(self, name: str) -> bytes:
        return (self.root / name).read_bytes()


@dataclass
class S3Store:
    """An S3 prefix under ``lakehouse/`` through a boto3 client the caller built from the job's AWS credentials."""

    client: Any
    bucket: str
    prefix: str

    def __post_init__(self) -> None:
        if not self.prefix.startswith("lakehouse/") or not self.prefix.endswith("/") or ".." in self.prefix:
            raise PublishError("the backup prefix must be a folder under lakehouse/")

    def put(self, name: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self.prefix + name, Body=data)

    def get(self, name: str) -> bytes:
        return bytes(self.client.get_object(Bucket=self.bucket, Key=self.prefix + name)["Body"].read())


def remove_tree(path: Path) -> None:
    """Remove a folder this module made (files and folders only; a link is removed as a link)."""
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    for child in path.iterdir():
        remove_tree(child)
    path.rmdir()


def export_files_match(export_dir: Path, manifest: Mapping[str, Any]) -> bool:
    """True when exactly the manifest's regular export files are present with their recorded hashes."""
    expected = {"manifest.json", *(str(item["file"]) for item in manifest.get("tables", {}).values())}
    if not export_dir.is_dir() or export_dir.is_symlink() or {path.name for path in export_dir.iterdir()} != expected:
        return False
    for item in manifest.get("tables", {}).values():
        file = export_dir / str(item["file"])
        if file.is_symlink() or not file.is_file() or sha256_file(file) != item["file_sha256"]:
            return False
    return True


@dataclass
class Context:
    """One step's inputs. ``spark`` is a session on the target catalog; the namespace is fixed by the mode.

    ``accepted_result_sha256`` is the digest of the consumer result as the launcher accepted it; a step refuses a result file with any other bytes.
    """

    spark: Any
    plan: Plan
    run_id: str
    mode: str
    publish_dir: Path
    sealed_sha256: str
    store: BackupStore | None = None
    faults: Faults = field(default_factory=Faults)
    create_namespace: bool = False
    needs_sealed: bool = True
    accepted_result_sha256: str = ""

    def __post_init__(self) -> None:
        if self.mode not in self.plan.namespaces:
            raise PublishError(f"mode {self.mode!r} is not one of {list(MODES)}")
        if not RUN_ID.fullmatch(self.run_id) or (self.needs_sealed and not SHA256.fullmatch(self.sealed_sha256)):
            raise PublishError("a publish needs a run ID and the SHA-256 of the sealed copy")
        if self.needs_sealed and not SHA256.fullmatch(self.accepted_result_sha256):
            raise PublishError("a publish step needs the SHA-256 of the consumer result that the launcher accepted")

    @property
    def namespace(self) -> str:
        return self.plan.namespaces[self.mode]

    @property
    def branch(self) -> str:
        return branch_name(self.plan.branch_prefix, self.run_id)

    def fq(self, name: str) -> str:
        """The fully qualified table, only for a table the contract names and only in this mode's namespace."""
        if name not in self.plan.writable:
            raise PublishError(f"{name!r} is not a gold table of the contract")
        return f"{CATALOG}.{self.namespace}.{name}"

    def sql(self, template: str, **parts: str) -> Any:
        return self.spark.sql(template.format(catalog=CATALOG, namespace=self.namespace, **parts))

    def path(self, name: str) -> Path:
        return self.publish_dir / name


def table_refs(ctx: Context, name: str) -> dict[str, int]:
    """Branch and tag heads of a table by name."""
    rows = ctx.sql(REFS, table=ctx.fq(name)).collect()
    return {str(row[0]): int(row[2]) for row in rows}


def table_exists(ctx: Context, name: str) -> bool:
    return bool(ctx.spark.catalog.tableExists(ctx.fq(name)))


def main_head(ctx: Context, name: str) -> int | None:
    """The snapshot ID of ``main``, or None for a table that has no snapshot on main or does not exist."""
    if not table_exists(ctx, name):
        return None
    return table_refs(ctx, name).get("main")


def fingerprint_sql(columns: Sequence[Sequence[str]], source: str) -> str:
    """Rows and an order-independent content hash over every column. A null mask joins the hash so that a null in one column and a null in
    another are different rows."""
    names = [str(column[0]) for column in columns]
    mask = "concat_ws('', " + ", ".join(f"CAST(CAST({quote(name)} IS NULL AS INT) AS STRING)" for name in names) + ")"
    return FINGERPRINT.format(columns=", ".join(quote(name) for name in names), mask=mask, source=source)


def fingerprint(ctx: Context, columns: Sequence[Sequence[str]], source: str) -> tuple[int, str]:
    row = ctx.spark.sql(fingerprint_sql(columns, source)).collect()[0]
    return int(row[0]), str(row[1])


def parquet_view(ctx: Context, export_dir: Path, name: str, item: Mapping[str, Any]) -> str:
    """Register the exported file as a temporary view and return its name."""
    view = "export_" + ident(name)
    ctx.spark.read.parquet(str(export_dir / item["file"])).createOrReplaceTempView(view)
    return view


def table_ddl(ctx: Context, view: str) -> str:
    """Column definitions for a new Iceberg table: the view's columns and Spark types."""
    return ", ".join(f"{quote(field.name)} {field.dataType.simpleString()}" for field in ctx.spark.table(view).schema.fields)


def verified_export(ctx: Context) -> tuple[dict[str, Any], str, Path]:
    """The export manifest of this run, checked against the accepted consumer result, the expected sealed digest and the files on disk."""
    export_dir = ctx.path("export")
    result_path = ctx.path(RESULT_NAME)
    result, _ = read_record(result_path)
    if sha256_file(result_path) != ctx.accepted_result_sha256:
        raise PublishError("the consumer result is not the one the launcher accepted")
    accept_consumer(result, {"sha256": ctx.sealed_sha256}, ctx.run_id)
    manifest, manifest_sha = read_record(export_dir / "manifest.json")
    if manifest.get("run_id") != ctx.run_id or manifest.get("sealed_sha256") != ctx.sealed_sha256 or manifest.get("in_container_sha256") != ctx.sealed_sha256:
        raise PublishError("the export is not the export of this run's sealed copy")
    if result.get("manifest_sha256") != manifest_sha:
        raise PublishError("the consumer result does not match the export manifest")
    if set(manifest["tables"]) != set(ctx.plan.tables):
        raise PublishError("the export does not hold exactly the contract's tables")
    if not export_files_match(export_dir, manifest):
        raise PublishError("an exported file changed after the consumer wrote it")
    return manifest, manifest_sha, export_dir


def committed_run(ctx: Context) -> dict[str, Any] | None:
    """This run's ``publish_runs`` row, or None."""
    if not table_exists(ctx, ctx.plan.runs_table):
        return None
    columns = ", ".join(quote(name) for name, _ in RUNS_COLUMNS)
    rows = ctx.sql(SELECT_RUN, columns=columns, table=ctx.fq(ctx.plan.runs_table), run_id=ctx.run_id).collect()
    if not rows:
        return None
    return run_row(rows[0])


def run_row(row: Any) -> dict[str, Any]:
    document = {name: row[index] for index, (name, _) in enumerate(RUNS_COLUMNS)}
    document["committed_at"] = document["committed_at"].astimezone(UTC).isoformat() if document["committed_at"] is not None else None
    document["snapshot_set"] = json.loads(document["snapshot_set"])
    return document


def ensure_namespace(ctx: Context) -> None:
    """The namespace must exist. A local catalog creates it; the writer principal never creates one (its grants stop at the tables)."""
    try:
        ctx.sql(NAMESPACE_EXISTS).collect()
        return
    except Exception as error:
        if not any(marker in str(error) for marker in ABSENT_NAMESPACE):
            raise
    if not ctx.create_namespace:
        raise PublishError(f"namespace {ctx.namespace} does not exist")
    ctx.sql(CREATE_NAMESPACE)


def same_columns(columns: Sequence[Sequence[str]], key: Sequence[str]) -> str:
    return " AND ".join(f"t.{quote(str(c[0]))} <=> s.{quote(str(c[0]))}" for c in columns if c[0] not in key)


def merge_statement(ctx: Context, name: str, view: str, columns: Sequence[Sequence[str]], key: Sequence[str]) -> str:
    on = " AND ".join(f"t.{quote(column)} <=> s.{quote(column)}" for column in key)
    same = same_columns(columns, key)
    matched = MERGE_MATCHED.format(same=same) if same else ""
    return MERGE.format(table=ctx.fq(name), branch=ctx.branch, view=view, on=on, matched=matched)


def publish_candidates(ctx: Context) -> dict[str, Any]:
    """Write every table to the run's branch and record the candidate snapshot IDs. Nothing becomes visible on ``main``.

    A rerun republishes into the same branches: a table whose branch already holds the export's rows and fingerprint is left as it is, so a
    retry creates no second snapshot. A run that is already committed refuses.
    """
    manifest, manifest_sha, export_dir = verified_export(ctx)
    ensure_namespace(ctx)
    if committed_run(ctx) is not None:
        raise PublishError("this run is already committed; its candidates are not rewritten")
    record_path = ctx.path("candidates.json")
    previous = read_record(record_path)[0] if record_path.is_file() else None
    if previous is not None and previous.get("export_manifest_sha256") != manifest_sha:
        raise PublishError("candidates.json belongs to a different export of this run")
    bases = dict(previous["bases"]) if previous is not None else {}
    if previous is None:
        bases = {name: main_head(ctx, name) for name in ctx.plan.tables}
    candidates: dict[str, Any] = dict(previous["tables"]) if previous is not None else {}
    document = {
        "schema": SCHEMA,
        "run_id": ctx.run_id,
        "mode": ctx.mode,
        "namespace": ctx.namespace,
        "branch": ctx.branch,
        "sealed_sha256": ctx.sealed_sha256,
        "export_manifest_sha256": manifest_sha,
        "bases": bases,
        "tables": candidates,
        "complete": False,
    }
    write_record(record_path, document)
    ctx.faults.hit("candidates:planned")
    for name, item in manifest["tables"].items():
        view = parquet_view(ctx, export_dir, name, item)
        fq = ctx.fq(name)
        if not table_exists(ctx, name):
            ctx.sql(CREATE_TABLE, table=fq, columns=table_ddl(ctx, view))
        if ctx.branch not in table_refs(ctx, name):
            ctx.sql(CREATE_BRANCH, table=fq, branch=ctx.branch)
        ctx.faults.hit(f"candidates:after_branch:{name}")
        columns = item["columns"]
        wanted = fingerprint(ctx, columns, view)
        if fingerprint(ctx, columns, f"{fq} VERSION AS OF {table_refs(ctx, name)[ctx.branch]}") != wanted:
            ctx.spark.sql(merge_statement(ctx, name, view, columns, item["merge_key"]))
        ctx.faults.hit(f"candidates:after_merge:{name}")
        head = table_refs(ctx, name)[ctx.branch]
        candidates[name] = {"candidate_snapshot_id": head, "rows": int(item["rows"]), "base_snapshot_id": bases[name]}
        write_record(record_path, document)
        ctx.faults.hit(f"candidates:after_record:{name}")
    document["complete"] = True
    sha = write_record(record_path, document)
    return {"step": "candidates", "tables": len(candidates), "record_sha256": sha}


def verified_candidates(ctx: Context, manifest_sha: str) -> tuple[dict[str, Any], str]:
    document, sha = read_record(ctx.path("candidates.json"))
    if document.get("run_id") != ctx.run_id or document.get("export_manifest_sha256") != manifest_sha or document.get("sealed_sha256") != ctx.sealed_sha256:
        raise PublishError("candidates.json does not belong to this run's export")
    if document.get("complete") is not True or set(document["tables"]) != set(ctx.plan.tables):
        raise PublishError("the candidates are not complete")
    return document, sha


def reconcile_candidates(ctx: Context) -> dict[str, Any]:
    """Compare rows and content fingerprints of exactly the candidate snapshot IDs with the export, and check that ``main`` is unchanged."""
    manifest, manifest_sha, export_dir = verified_export(ctx)
    candidates, candidates_sha = verified_candidates(ctx, manifest_sha)
    tables: dict[str, Any] = {}
    problems: list[str] = []
    for name, item in manifest["tables"].items():
        candidate = candidates["tables"][name]
        columns = item["columns"]
        view = parquet_view(ctx, export_dir, name, item)
        wanted_rows, wanted = fingerprint(ctx, columns, view)
        snapshot = int(candidate["candidate_snapshot_id"])
        rows, found = fingerprint(ctx, columns, f"{ctx.fq(name)} VERSION AS OF {snapshot}")
        head = main_head(ctx, name)
        entry = {
            "candidate_snapshot_id": snapshot,
            "rows": rows,
            "export_rows": wanted_rows,
            "manifest_rows": int(item["rows"]),
            "fingerprint": found,
            "export_fingerprint": wanted,
            "main_snapshot_id": head,
            "main_unchanged": head == candidate["base_snapshot_id"],
        }
        entry["equal"] = rows == wanted_rows == int(item["rows"]) and found == wanted
        if not entry["equal"]:
            problems.append(f"{name}: the candidate snapshot differs from the export")
        if not entry["main_unchanged"] and head != snapshot:
            problems.append(f"{name}: main moved since the run started")
        tables[name] = entry
        ctx.faults.hit(f"reconcile:after_table:{name}")
    document = {"schema": SCHEMA, "run_id": ctx.run_id, "candidates_sha256": candidates_sha, "tables": tables, "pass": not problems}
    sha = write_record(ctx.path("reconcile.json"), document)
    if problems:
        raise PublishError("; ".join(problems[:5]))
    return {"step": "reconcile", "tables": len(tables), "record_sha256": sha}


def catalog_state(ctx: Context) -> dict[str, Any]:
    """The catalog pointers of the namespace's gold tables: metadata file, refs and snapshot lineage. No row values."""
    state: dict[str, Any] = {}
    for name in [*ctx.plan.tables, ctx.plan.runs_table]:
        if not table_exists(ctx, name):
            continue
        fq = ctx.fq(name)
        log = ctx.sql(METADATA_LOG, table=fq).collect()
        snapshots = ctx.sql(SNAPSHOTS, table=fq).collect()
        state[name] = {
            "metadata_file": str(log[0][0]) if log else None,
            "refs": dict(sorted(table_refs(ctx, name).items())),
            "snapshots": sorted([int(row[0]), None if row[1] is None else int(row[1]), str(row[2])] for row in snapshots),
        }
    return state


def catalog_backup(ctx: Context) -> dict[str, Any]:
    """Write the catalog state under the run's repeatable name and read it back."""
    if ctx.store is None:
        raise PublishError("no backup location is configured")
    manifest, manifest_sha, _ = verified_export(ctx)
    reconcile, reconcile_sha = read_record(ctx.path("reconcile.json"))
    if reconcile.get("pass") is not True or reconcile.get("run_id") != ctx.run_id:
        raise PublishError("the reconcile record is not a pass for this run")
    document = {"schema": SCHEMA, "run_id": ctx.run_id, "namespace": ctx.namespace, "tables": catalog_state(ctx)}
    data = canonical(document)
    name = backup_name(ctx.run_id)
    ctx.store.put(name, data)
    ctx.faults.hit("backup:after_write")
    stored = ctx.store.get(name)
    if sha256_bytes(stored) != sha256_bytes(data):
        raise PublishError("the backup read back differs from what was written")
    record = {"schema": SCHEMA, "run_id": ctx.run_id, "reconcile_sha256": reconcile_sha, "name": name, "backup_sha256": sha256_bytes(data), "read_back": True}
    ctx.faults.hit("backup:before_record")
    sha = write_record(ctx.path("backup.json"), record)
    return {"step": "backup", "name": name, "record_sha256": sha}


def runs_view(ctx: Context, row: Mapping[str, Any]) -> str:
    """A one-row temporary view holding the ``publish_runs`` row."""
    committed = datetime.fromisoformat(str(row["committed_at"]))
    values = {**row, "committed_at": committed, "snapshot_set": json.dumps(row["snapshot_set"], sort_keys=True)}
    schema = ", ".join(f"{name} {kind}" for name, kind in RUNS_COLUMNS)
    view = "publish_run_row"
    ctx.spark.createDataFrame([tuple(values[name] for name, _ in RUNS_COLUMNS)], schema).createOrReplaceTempView(view)
    return view


def ensure_runs_table(ctx: Context) -> None:
    if not table_exists(ctx, ctx.plan.runs_table):
        columns = ", ".join(f"{quote(name)} {kind}" for name, kind in RUNS_COLUMNS)
        ctx.sql(CREATE_TABLE, table=ctx.fq(ctx.plan.runs_table), columns=columns)


def commit_receipt(ctx: Context, backup_sha: str, snapshot_set: Mapping[str, int]) -> dict[str, Any]:
    """The ``commit.json`` record of a committed run: the one shape both the first commit and a retry write."""
    return {"schema": SCHEMA, "run_id": ctx.run_id, "backup_sha256": backup_sha, "snapshot_set": dict(snapshot_set), "state": "committed"}


def restore_receipt(path: Path, receipt: Mapping[str, Any]) -> str:
    """Write the receipt of a run whose ``publish_runs`` row exists, or confirm that the receipt on disk is identical; returns its SHA-256.

    An interruption after the row and before the receipt leaves no ``commit.json``. A receipt that records anything else is refused and kept.
    """
    if path.is_file():
        on_disk, sha = read_record(path)
        if on_disk != dict(receipt):
            raise PublishError("commit.json records another commit of this run")
        return sha
    return write_record(path, receipt)


def commit_publish(ctx: Context) -> dict[str, Any]:
    """Fast-forward every table to its reconciled candidate and write the ``publish_runs`` row last.

    A rerun after a success changes nothing. A rerun after an interruption fast-forwards the tables still behind. A table whose ``main`` is
    neither the recorded base nor the candidate stops the commit before any table moves. A rerun that finds the run's row checks the row's mode,
    sealed digest, export manifest digest, backup name and digest and snapshot set against the run's records, then restores a missing ``commit.json``.
    """
    manifest, manifest_sha, _ = verified_export(ctx)
    candidates, candidates_sha = verified_candidates(ctx, manifest_sha)
    reconcile, reconcile_sha = read_record(ctx.path("reconcile.json"))
    backup, backup_sha = read_record(ctx.path("backup.json"))
    if reconcile.get("pass") is not True or reconcile.get("candidates_sha256") != candidates_sha:
        raise PublishError("the reconcile record is not a pass for these candidates")
    if backup.get("read_back") is not True or backup.get("reconcile_sha256") != reconcile_sha or backup.get("run_id") != ctx.run_id:
        raise PublishError("the backup record was not read back for this reconcile record")
    snapshot_set = {name: int(candidates["tables"][name]["candidate_snapshot_id"]) for name in sorted(ctx.plan.tables)}
    existing = committed_run(ctx)
    if existing is not None:
        identity = {
            "mode": ctx.mode,
            "sealed_sha256": ctx.sealed_sha256,
            "export_manifest_sha256": manifest_sha,
            "backup_name": backup["name"],
            "backup_sha256": backup["backup_sha256"],
            "snapshot_set": snapshot_set,
        }
        if any(existing[key] != value for key, value in identity.items()):
            raise PublishError("this run is committed with a different identity, predecessor hash or snapshot set")
        sha = restore_receipt(ctx.path("commit.json"), commit_receipt(ctx, backup_sha, snapshot_set))
        return {"step": "commit", "already_committed": True, "tables": len(snapshot_set), "record_sha256": sha}
    heads = {name: main_head(ctx, name) for name in snapshot_set}
    for name, candidate in snapshot_set.items():
        base = candidates["tables"][name]["base_snapshot_id"]
        if heads[name] not in (base, candidate):
            raise PublishError(f"{name}: main moved since the run started; nothing is committed")
        if ctx.branch not in table_refs(ctx, name) and heads[name] != candidate:
            raise PublishError(f"{name}: the run's branch is gone and main is not at the candidate")
        if ctx.branch in table_refs(ctx, name) and table_refs(ctx, name)[ctx.branch] != candidate:
            raise PublishError(f"{name}: the branch head is not the reconciled candidate")
    for name, candidate in snapshot_set.items():
        if heads[name] != candidate:
            ctx.sql(FAST_FORWARD, name=name, branch=ctx.branch)
        ctx.faults.hit(f"commit:after_ff:{name}")
    for name, candidate in snapshot_set.items():
        if main_head(ctx, name) != candidate:
            raise PublishError(f"{name}: main is not at the reconciled candidate after the fast-forward")
    for name in snapshot_set:
        if ctx.branch in table_refs(ctx, name):
            ctx.sql(DROP_BRANCH, table=ctx.fq(name), branch=ctx.branch)
    ctx.faults.hit("commit:before_publish_runs")
    ensure_runs_table(ctx)
    row = {
        "run_id": ctx.run_id,
        "mode": ctx.mode,
        "committed_at": datetime.now(UTC).isoformat(),
        "sealed_sha256": ctx.sealed_sha256,
        "export_manifest_sha256": manifest_sha,
        "backup_name": backup["name"],
        "backup_sha256": backup["backup_sha256"],
        "snapshot_set": snapshot_set,
    }
    ctx.spark.sql(MERGE_RUN.format(table=ctx.fq(ctx.plan.runs_table), view=runs_view(ctx, row)))
    ctx.faults.hit("commit:after_publish_runs")
    sha = write_record(ctx.path("commit.json"), commit_receipt(ctx, backup_sha, snapshot_set))
    return {"step": "commit", "already_committed": False, "tables": len(snapshot_set), "record_sha256": sha}


def published_head(ctx: Context) -> dict[str, Any] | None:
    """The newest ``publish_runs`` row: a reader reads this first and then reads only the snapshot IDs it names."""
    if not table_exists(ctx, ctx.plan.runs_table):
        return None
    columns = ", ".join(quote(name) for name, _ in RUNS_COLUMNS)
    rows = ctx.sql(SELECT_HEAD, columns=columns, table=ctx.fq(ctx.plan.runs_table)).collect()
    return run_row(rows[0]) if rows else None


def published_runs(ctx: Context) -> list[dict[str, Any]]:
    """Every committed set, oldest first."""
    if not table_exists(ctx, ctx.plan.runs_table):
        return []
    columns = ", ".join(quote(name) for name, _ in RUNS_COLUMNS)
    return [run_row(row) for row in ctx.sql(SELECT_ALL_RUNS, columns=columns, table=ctx.fq(ctx.plan.runs_table)).collect()]


def build_store(plan: Plan, local: Path | None) -> BackupStore | None:
    """The backup store of this run: a folder next to the local warehouse offline, the S3 prefix from the contract otherwise."""
    if local is not None:
        return FolderStore(local.parent / "catalog_backup")
    if plan.backup_prefix is None:
        return None
    import boto3

    deployment = json.loads(Path("/workspace/infra/deployment.auto.tfvars.json").read_text())
    session = boto3.Session(region_name=os.environ["AWS_REGION"])
    return S3Store(session.client("s3"), str(deployment["data_bucket_name"]), plan.backup_prefix)


def parse(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=STEPS)
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", required=True, choices=MODES, help="the run's publish mode")
    parser.add_argument("--sealed-sha256", required=True)
    parser.add_argument("--accepted-result-sha256", required=True, help="the digest of the consumer result that the launcher accepted")
    parser.add_argument("--publish-dir", default=DEFAULT_PUBLISH_DIR)
    parser.add_argument("--local-warehouse", help="a throwaway file catalog for the offline runs; refused when a catalog URI is set")
    return parser.parse_args(list(argv))


def main(argv: Sequence[str] | None = None) -> int:
    """Run one step. Prints one JSON line with counts and hashes; a refusal prints its reason to standard error and exits 1."""
    args = parse(sys.argv[1:] if argv is None else argv)
    local = Path(args.local_warehouse) if args.local_warehouse else None
    if local is not None and os.environ.get("POLARIS_URI"):
        sys.stderr.write("gold publish: a local warehouse is refused when a catalog URI is set\n")
        return 1
    plan = load_plan()
    publish_dir = Path(args.publish_dir)
    try:
        from scripts.lakehouse.session import spark_session

        spark = spark_session(f"gold_publish_{args.step}", local)
        try:
            ctx = Context(
                spark,
                plan,
                args.run,
                args.mode,
                publish_dir,
                args.sealed_sha256,
                build_store(plan, local) if args.step == "backup" else None,
                create_namespace=local is not None,
                accepted_result_sha256=args.accepted_result_sha256,
            )
            steps = {"candidates": publish_candidates, "reconcile": reconcile_candidates, "backup": catalog_backup, "commit": commit_publish}
            sys.stdout.write(json.dumps(steps[args.step](ctx)) + "\n")
        finally:
            spark.stop()
    except PublishError as error:
        sys.stderr.write(f"gold publish: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
