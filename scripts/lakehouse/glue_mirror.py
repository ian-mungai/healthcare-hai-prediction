"""Mirror published Iceberg tables into read-only Glue registrations for Athena. Polaris stays the catalog and the source of truth.

After a publish commits, ``gold_publish.mirror_after_commit`` calls ``mirror``; the silver and features publishes commit through the same engine
(``gold_publish.commit_publish``), so they call it too.
For every table that the publish contract names, all Public research data, the mirror makes one Glue table. Its ``metadata_location`` is the metadata
file of the table's committed snapshot, taken from the newest ``publish_runs`` row. A repeat with the same file, snapshot and run writes nothing. Only mode
``real`` and the layers of ``config/lakehouse/athena_mirror.json`` are mirrored, so test namespaces and the runs
table never reach Glue (``violations``). The Glue registrations are also what the OpenMetadata Glue connector reads (owner decision of Nov 7 2026), because
OpenMetadata 2.0.7 has no Iceberg connector. The mirror never raises and stops at ``mirror_seconds``: a failure comes back in the report.

``python -m scripts.lakehouse.glue_mirror check --layer silver|gold|features`` compares each registration with Polaris and exits 1 on any difference.
It writes nothing.

``python -m scripts.lakehouse.glue_mirror register --source s3 --layer silver|gold|features|bronze|all [--dry-run] [--bucket NAME]`` registers the tables
that are already published, without Spark and without Polaris. The bronze layer has no publish step, so it has no publish-time registration and no
``check``: ``register`` is its only route, for the tables that ``config/lakehouse/bronze_tables.json`` lists and the bronze tables that silver's dbt
sources name (so every silver input resolves; not the ``bronze_dictionary`` namespace). It reads each table's metadata files in S3 and registers the
snapshot that the main branch of the newest metadata file points to, never a staged or audit branch snapshot. A table whose metadata cannot be resolved
without doubt is refused and named: no file, two files with one version number, a file name that is not numbered, a location that is not the table folder,
a main that is missing or not a branch, or a type the mirror cannot map. The registration records the run ``registered_from_s3``. A later publish replaces
it, and a registration that a publish wrote is kept. A rerun with unchanged metadata writes nothing. ``--dry-run`` plans and writes nothing. The caller
needs Glue write access (the administrator profile; the project user has none) and read access to the bucket.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "config/lakehouse/athena_mirror.json"
# The layers with a publish contract and a publish_runs table: their publishes register tables and ``check`` compares them with Polaris.
PUBLISHED_LAYERS = ("silver", "gold", "features")
# Bronze has no publish step, so no hook and no check. Its tables are the ones that ``config/lakehouse/bronze_tables.json`` lists and the ones that silver's
# dbt sources read, registered by ``register``.
MIRRORED_LAYERS = ("bronze", *PUBLISHED_LAYERS)
BRONZE_TABLES = REPO_ROOT / "config/lakehouse/bronze_tables.json"
SILVER_BRONZE_SOURCES = REPO_ROOT / "dbt/models/silver/staging/_bronze__sources.yml"
# The publish contract of each mirrored layer: the features publish is the gold engine with its own contract (scripts/features/features_publish.py).
LAYER_CONTRACTS = {
    "silver": REPO_ROOT / "data_contracts/lakehouse/silver_publish.json",
    "gold": REPO_ROOT / "data_contracts/lakehouse/gold_contract.json",
    "features": REPO_ROOT / "data_contracts/lakehouse/features_publish.json",
}
POINTER = re.compile(r"s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/(?:[^/]+/)*metadata/[^/]+\.metadata\.json")
# The newest metadata file whose current snapshot is the committed one. A later publish that failed halfway moves main, but not this answer.
COMMITTED_METADATA_FILE = "SELECT file FROM {table}.metadata_log_entries WHERE latest_snapshot_id = {snapshot} ORDER BY timestamp DESC LIMIT 1"
CONNECT_SECONDS = 5
READ_SECONDS = 20
CALL_ATTEMPTS = 2
TABLE_NAME = re.compile(r"[a-z][a-z0-9_]{0,127}")
LOCATION_CUT = "/metadata/"
S3_RUN_ID = "registered_from_s3"
METADATA_FILE = re.compile(r"(?P<version>[0-9]{5,})-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json")
SCALAR_TYPES = {
    "boolean": "boolean",
    "int": "int",
    "long": "bigint",
    "float": "float",
    "double": "double",
    "date": "date",
    "time": "string",
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "timestamp_ns": "timestamp",
    "timestamptz_ns": "timestamp",
    "string": "string",
    "uuid": "string",
    "binary": "binary",
}
DECIMAL_TYPE = re.compile(r"decimal\((?P<precision>[0-9]+),\s*(?P<scale>[0-9]+)\)")
FIXED_TYPE = re.compile(r"fixed\[[0-9]+\]")


class MirrorError(RuntimeError):
    """One table cannot be mirrored; the message says why in plain words."""


class MirrorDeadline(MirrorError):
    """The one absolute deadline of a mirror run passed."""


class Budget:
    """One absolute deadline, started when the hook is entered. Every blocking step runs through ``run``, so none can hold the publish job past it.

    A step that is still running at the deadline is abandoned: its result is discarded and no later step starts. A step cannot be interrupted once it
    is inside a Spark or Glue call, so ``run`` reports that and a write that was already issued is reported as possibly landed.
    """

    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic, started: float | None = None) -> None:
        self.seconds = seconds
        self.clock = clock
        self.end = (clock() if started is None else started) + seconds
        self.stopped = threading.Event()

    def remaining(self) -> float:
        return self.end - self.clock()

    def run(self, label: str, work: Callable[..., Any], *arguments: Any, write: bool = False, **keywords: Any) -> Any:
        """Run ``work`` in a helper thread and wait for it until the deadline. A write checks the deadline again just before it is issued."""
        left = self.remaining()
        if left <= 0 or self.stopped.is_set():
            raise MirrorDeadline(f"the mirror deadline of {self.seconds:g} seconds passed before {label}")
        box: dict[str, Any] = {}

        def target() -> None:
            try:
                if write and (self.stopped.is_set() or self.remaining() <= 0):
                    raise MirrorDeadline(f"the mirror deadline of {self.seconds:g} seconds passed before {label}")
                box["value"] = work(*arguments, **keywords)
            except BaseException as error:
                box["error"] = error

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(left)
        if thread.is_alive():
            self.stopped.set()
            if write:
                raise MirrorDeadline(
                    f"the mirror deadline of {self.seconds:g} seconds passed during {label}: the write may have landed, run check and rerun the step"
                )
            raise MirrorDeadline(f"the mirror deadline of {self.seconds:g} seconds passed during {label}")
        if "error" in box:
            raise box["error"]
        return box.get("value")


@dataclass(frozen=True)
class Layer:
    namespace: str
    database: str


@dataclass(frozen=True)
class MirrorConfig:
    enabled: bool
    marker: str
    layers: Mapping[str, Layer]
    mirror_seconds: float = 300


class Source(Protocol):
    """The Polaris side: the current metadata file and the columns of a published table."""

    def pointer(self, table: str, snapshot_id: int) -> str | None: ...

    def columns(self, table: str) -> list[tuple[str, str]]: ...


class SparkSource:
    """Polaris read through the publish step's Spark session."""

    def __init__(self, spark: Any, catalog: str, namespace: str) -> None:
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace

    def fq(self, table: str) -> str:
        if not TABLE_NAME.fullmatch(table):
            raise MirrorError(f"{table!r} is not a plain table name")
        return f"{self.catalog}.{self.namespace}.{table}"

    def pointer(self, table: str, snapshot_id: int) -> str | None:
        rows = self.spark.sql(COMMITTED_METADATA_FILE.format(table=self.fq(table), snapshot=int(snapshot_id))).collect()
        return str(rows[0][0]) if rows else None

    def columns(self, table: str) -> list[tuple[str, str]]:
        return [(str(name), "timestamp" if kind == "timestamp_ntz" else str(kind)) for name, kind in self.spark.table(self.fq(table)).dtypes]


def glue_type(kind: Any) -> str:
    """The Glue column type of an Iceberg type; a type the mirror cannot map is refused."""
    if isinstance(kind, str):
        if kind in SCALAR_TYPES:
            return SCALAR_TYPES[kind]
        decimal = DECIMAL_TYPE.fullmatch(kind)
        if decimal:
            return f"decimal({decimal['precision']},{decimal['scale']})"
        if FIXED_TYPE.fullmatch(kind):
            return "binary"
    elif isinstance(kind, dict):
        try:
            if kind["type"] == "list":
                return f"array<{glue_type(kind['element'])}>"
            if kind["type"] == "map":
                return f"map<{glue_type(kind['key'])},{glue_type(kind['value'])}>"
            if kind["type"] == "struct":
                return "struct<" + ",".join(f"{field['name']}:{glue_type(field['type'])}" for field in kind["fields"]) + ">"
        except KeyError as error:
            raise MirrorError(f"an Iceberg type lacks its {error} entry") from error
    raise MirrorError(f"the Iceberg type {kind!r} has no Glue type")


class S3Source:
    """The table side read from the metadata files in S3, with no Spark and no Polaris. It only reads.

    The folder of a table is exactly ``lakehouse/<namespace>/<table>``. Its metadata files are the numbered ``<version>-<uuid>.metadata.json`` files in
    ``metadata/``; the highest version is the newest. The committed snapshot is the one that the main branch of the newest file points to. The file for a
    snapshot is the newest one whose main branch points to it, the same rule as the Polaris query. Anything that leaves the answer open is a ``MirrorError``.
    """

    def __init__(self, s3: Any, bucket: str, namespace: str) -> None:
        self.s3 = s3
        self.bucket = bucket
        self.namespace = namespace
        self.listed: dict[str, list[tuple[int, str]]] = {}
        self.documents: dict[str, dict[str, Any]] = {}

    def folder(self, table: str) -> str:
        if not TABLE_NAME.fullmatch(table):
            raise MirrorError(f"{table!r} is not a plain table name")
        return f"lakehouse/{self.namespace}/{table}"

    def files(self, table: str) -> list[tuple[int, str]]:
        """The metadata files of the table, newest first."""
        if table not in self.listed:
            prefix = f"{self.folder(table)}/metadata/"
            found: list[tuple[int, str]] = []
            for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
                for item in page.get("Contents", []):
                    name = item["Key"][len(prefix) :]
                    if not name.endswith(".metadata.json"):
                        continue
                    match = METADATA_FILE.fullmatch(name)
                    if match is None:
                        raise MirrorError(f"{table}: {name} is not a numbered metadata file name")
                    found.append((int(match["version"]), item["Key"]))
            if not found:
                raise MirrorError(f"{table}: no metadata file under {prefix}")
            if len({version for version, _ in found}) != len(found):
                raise MirrorError(f"{table}: two metadata files carry the same version number")
            self.listed[table] = sorted(found, reverse=True)
        return self.listed[table]

    def document(self, key: str) -> dict[str, Any]:
        if key not in self.documents:
            try:
                document = json.loads(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())
            except ValueError as error:
                raise MirrorError(f"{key.rsplit('/', 1)[-1]} is not JSON") from error
            if not isinstance(document, dict):
                raise MirrorError(f"{key.rsplit('/', 1)[-1]} is not a metadata document")
            self.documents[key] = document
        return self.documents[key]

    @staticmethod
    def main_snapshot(document: Mapping[str, Any]) -> int | None:
        """The snapshot of the main branch, or None when the document has no branch named main."""
        main = (document.get("refs") or {}).get("main")
        if not isinstance(main, dict) or main.get("type", "branch") != "branch" or not isinstance(main.get("snapshot-id"), int):
            return None
        return int(main["snapshot-id"])

    def committed(self, table: str) -> int:
        """The snapshot of the main branch in the newest metadata file, after the checks that tie the file to this table."""
        document = self.document(self.files(table)[0][1])
        if document.get("location") != f"s3://{self.bucket}/{self.folder(table)}":
            raise MirrorError(f"{table}: the metadata names another table location")
        snapshot = self.main_snapshot(document)
        if snapshot is None:
            raise MirrorError(f"{table}: the newest metadata file has no main branch")
        if document.get("current-snapshot-id") != snapshot:
            raise MirrorError(f"{table}: the current snapshot is not the snapshot of main")
        if snapshot not in [item.get("snapshot-id") for item in document.get("snapshots") or [] if isinstance(item, dict)]:
            raise MirrorError(f"{table}: the snapshot of main is not in the snapshot list")
        return snapshot

    def relation(self, table: str, held: int, wanted: int) -> str:
        """How the snapshot ``held`` stands to ``wanted`` in the newest metadata file: same, ancestor, newer, unlisted or unrelated.

        An ancestor is a snapshot that the file lists and that is on the parent chain of ``wanted``. A snapshot that the file does not list is unlisted, even
        when a parent pointer names it: it cannot be shown to be older. Newer is a listed snapshot with a higher sequence number than ``wanted``. Any other
        listed snapshot is unrelated.
        """
        if held == wanted:
            return "same"
        entries = {item.get("snapshot-id"): item for item in self.document(self.files(table)[0][1]).get("snapshots") or [] if isinstance(item, dict)}
        if held not in entries:
            return "unlisted"
        step: Any = wanted
        seen: set[Any] = set()
        while step is not None and step not in seen:
            seen.add(step)
            step = entries.get(step, {}).get("parent-snapshot-id")
            if step == held:
                return "ancestor"
        if entries[held].get("sequence-number", 0) > entries.get(wanted, {}).get("sequence-number", 0):
            return "newer"
        return "unrelated"

    def pointer(self, table: str, snapshot_id: int) -> str | None:
        for _, key in self.files(table):
            if self.main_snapshot(self.document(key)) == snapshot_id:
                return f"s3://{self.bucket}/{key}"
        return None

    def columns(self, table: str) -> list[tuple[str, str]]:
        document = self.document(self.files(table)[0][1])
        schemas = {schema.get("schema-id"): schema for schema in document.get("schemas") or [] if isinstance(schema, dict)}
        schema = schemas.get(document.get("current-schema-id")) or document.get("schema")
        if not isinstance(schema, dict) or not schema.get("fields"):
            raise MirrorError(f"{table}: the metadata has no current schema")
        return [(str(field["name"]), glue_type(field["type"])) for field in schema["fields"]]


class DryRunGlue:
    """The Glue client of a dry run: reads go through, a write is dropped. The mirror then reports what it would have done."""

    def __init__(self, glue: Any) -> None:
        self.glue = glue

    def get_table(self, **arguments: Any) -> Any:
        return self.glue.get_table(**arguments)

    def create_table(self, **arguments: Any) -> dict[str, Any]:
        return {}

    def update_table(self, **arguments: Any) -> dict[str, Any]:
        return {}


def load_config(path: Path = CONFIG) -> MirrorConfig:
    document = json.loads(path.read_text())
    layers = {name: Layer(item["namespace"], item["database"]) for name, item in document["layers"].items()}
    return MirrorConfig(document["enabled"] is True, document["mirror_marker"], layers, float(document["mirror_seconds"]))


def violations(config: MirrorConfig) -> list[str]:
    """Why the config may not be used: a layer other than silver, gold, features or bronze, or a namespace other than the layer's own (a test one)."""
    found: list[str] = []
    for layer, spec in config.layers.items():
        if layer not in MIRRORED_LAYERS:
            found.append(f"layer {layer} may not be mirrored: only {', '.join(MIRRORED_LAYERS)}")
        elif spec.namespace != layer:
            found.append(f"layer {layer} names namespace {spec.namespace}, not {layer}")
    return found


def tables_to_mirror(config: MirrorConfig, layer: str, mode: str, published: Sequence[str]) -> tuple[list[str], str]:
    """The tables this publish mirrors, or none with the reason."""
    spec = config.layers.get(layer)
    if not config.enabled:
        return [], "the mirror is off in config/lakehouse/athena_mirror.json"
    if mode != "real":
        return [], f"mode {mode} is a test mode"
    if spec is None or spec.namespace != layer:
        return [], f"namespace {layer} is not mirrored"
    wanted = list(dict.fromkeys(published))
    return wanted, "" if wanted else f"the {layer} publish named no table"


def explain(error: Exception) -> str:
    code = getattr(error, "response", {}).get("Error", {}).get("Code") if hasattr(error, "response") else None
    return f"{code} from Glue" if code else f"{type(error).__name__}: {error}"


def glue_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    return response.get("Error", {}).get("Code") if isinstance(response, dict) else None


def current(glue: Any, database: str, table: str) -> dict[str, Any] | None:
    try:
        table_document: dict[str, Any] = glue.get_table(DatabaseName=database, Name=table)["Table"]
        return table_document
    except Exception as error:
        if glue_error_code(error) == "EntityNotFoundException":
            return None
        raise


def client_config() -> Any:
    """Short timeouts and few retries for every Glue call, so a missing network path fails fast instead of holding the publish job."""
    from botocore.config import Config

    return Config(connect_timeout=CONNECT_SECONDS, read_timeout=READ_SECONDS, retries={"total_max_attempts": CALL_ATTEMPTS, "mode": "standard"})


def glue_client() -> Any:
    import boto3

    return boto3.Session(region_name=os.environ["AWS_REGION"]).client("glue", config=client_config())


def s3_client() -> Any:
    import boto3

    return boto3.Session(region_name=os.environ["AWS_REGION"]).client("s3", config=client_config())


# Decides, on the registration just read and before any write, whether to leave it: an outcome string keeps it, None goes on and a MirrorError refuses.
Guard = Callable[[str, Mapping[str, Any], int], str | None]


def register(
    config: MirrorConfig, spec: Layer, table: str, snapshot_id: int, run_id: str, source: Source, glue: Any, budget: Budget, guard: Guard | None = None
) -> str:
    """Create or move one registration to the committed snapshot's metadata file; returns created, updated or unchanged.

    ``guard`` sees the registration that this call just read, the one it would replace, so a registration that lands after an earlier check is still seen.
    """
    if not TABLE_NAME.fullmatch(table):
        raise MirrorError("the table name is not a plain lower-case name")
    pointer = budget.run(f"reading the metadata file of {table}", source.pointer, table, snapshot_id)
    if pointer is None:
        raise MirrorError(f"Polaris holds no metadata file for the committed snapshot {snapshot_id}: the registration was not changed")
    if not POINTER.fullmatch(pointer):
        raise MirrorError("the metadata file is not an s3:// path that ends in .metadata.json")
    existing = budget.run(f"reading the registration of {table}", current, glue, spec.database, table)
    if existing is not None:
        held = existing.get("Parameters", {})
        if held.get(config.marker) != "true":
            raise MirrorError(f"Glue already holds {spec.database}.{table} and it is not a mirror: nothing was changed")
        kept = guard(table, existing, snapshot_id) if guard is not None else None
        if kept is not None:
            return kept
        if held.get("metadata_location") == pointer and held.get("hai_snapshot_id") == str(snapshot_id) and held.get("hai_run_id") == run_id:
            return "unchanged"
    columns = [{"Name": name, "Type": kind} for name, kind in budget.run(f"reading the columns of {table}", source.columns, table)]
    parameters = {"table_type": "ICEBERG", "metadata_location": pointer, config.marker: "true", "hai_snapshot_id": str(snapshot_id), "hai_run_id": run_id}
    if existing is not None and existing["Parameters"].get("metadata_location") != pointer:
        parameters["previous_metadata_location"] = existing["Parameters"]["metadata_location"]
    document = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": parameters,
        "StorageDescriptor": {"Columns": columns, "Location": pointer.rsplit(LOCATION_CUT, 1)[0]},
    }
    if existing is None:
        try:
            budget.run(f"creating the registration of {table}", glue.create_table, write=True, DatabaseName=spec.database, TableInput=document)
        except Exception as error:
            if glue_error_code(error) == "EntityNotFoundException":
                raise MirrorError(f"database {spec.database} does not exist: apply the Athena mirror stack first") from error
            raise
        return "created"
    budget.run(
        f"updating the registration of {table}", glue.update_table, write=True, DatabaseName=spec.database, TableInput=document, VersionId=existing["VersionId"]
    )
    return "updated"


def mirror(
    config: MirrorConfig,
    layer: str,
    mode: str,
    snapshots: Mapping[str, int],
    source: Source,
    glue: Any,
    run_id: str = "",
    budget: Budget | None = None,
    guard: Guard | None = None,
) -> dict[str, Any]:
    """Mirror the newest committed snapshot set of one layer. Never raises: each table's outcome and any failure are in the report.

    ``snapshots`` maps each table to its committed snapshot ID. Each registration points at the metadata file of that snapshot, not at the live main,
    which a later publish that failed halfway may have moved. Every Spark read and Glue call runs under one absolute deadline (``budget``, which the hook
    starts when it is entered). After it passes, no further step starts and the tables not reached are reported failed.
    """
    wanted, reason = tables_to_mirror(config, layer, mode, list(snapshots))
    if not wanted:
        return {"status": "skipped", "reason": reason, "tables": {}}
    budget = budget or Budget(config.mirror_seconds)
    outcomes: dict[str, str] = {}
    for table in wanted:
        try:
            outcomes[table] = register(config, config.layers[layer], table, snapshots[table], run_id, source, glue, budget, guard)
        except Exception as error:
            outcomes[table] = "failed: " + (str(error) if isinstance(error, MirrorError) else explain(error))
    failed = [table for table, outcome in outcomes.items() if outcome.startswith("failed")]
    status = "failed" if failed else "unchanged" if set(outcomes.values()) == {"unchanged"} else "ok"
    report: dict[str, Any] = {"status": status, "tables": outcomes}
    if failed:
        report["reason"] = "; ".join(f"{table} {outcomes[table]}" for table in failed)
    return report


def check(config: MirrorConfig, layer: str, snapshots: Mapping[str, int], source: Source, glue: Any, run_id: str) -> dict[str, Any]:
    """Compare each table's Glue registration with the committed snapshot set's metadata file, snapshot and run. Reads only."""
    spec = config.layers[layer]
    outcomes: dict[str, str] = {}
    for table, snapshot_id in snapshots.items():
        try:
            wanted = source.pointer(table, snapshot_id)
            found = current(glue, spec.database, table)
            if found is None:
                outcomes[table] = "missing in Glue"
            elif found.get("Parameters", {}).get(config.marker) != "true":
                outcomes[table] = "in Glue but not a mirror"
            elif wanted is None:
                outcomes[table] = f"no Polaris metadata file for the committed snapshot {snapshot_id}"
            else:
                held = found["Parameters"].get("metadata_location")
                same_snapshot = found["Parameters"].get("hai_snapshot_id") == str(snapshot_id)
                held_run = found["Parameters"].get("hai_run_id")
                if held == wanted and same_snapshot and held_run == run_id:
                    outcomes[table] = "match"
                elif held == wanted and same_snapshot:
                    outcomes[table] = f"stale run: Glue records run {held_run}, the committed run is {run_id}"
                else:
                    outcomes[table] = f"differs: committed {wanted} (snapshot {snapshot_id}), Glue {held}"
        except Exception as error:
            outcomes[table] = "unreadable: " + (str(error) if isinstance(error, MirrorError) else explain(error))
    return {"ok": all(outcome == "match" for outcome in outcomes.values()), "tables": outcomes}


def register_from_s3(config: MirrorConfig, layer: str, tables: Sequence[str], s3: Any, bucket: str, glue: Any, dry_run: bool = False) -> dict[str, Any]:
    """Register the published tables of one layer from their metadata files in S3. Never raises: each table's outcome is in the report.

    The tables are the ones the layer's publish contract names. One absolute deadline bounds every S3 read and Glue call. A table that cannot be resolved
    is reported failed and the others go on. A registration that a publish wrote stays as it is, also when the publish lands while the metadata is read.
    A registration is replaced only when its snapshot is an ancestor of the one to write. With ``dry_run`` nothing is written.
    """
    wanted, reason = tables_to_mirror(config, layer, "real", tables)
    if not wanted:
        return {"status": "skipped", "reason": reason, "tables": {}}
    spec = config.layers[layer]
    budget = Budget(config.mirror_seconds)
    source = S3Source(s3, bucket, spec.namespace)
    outcomes: dict[str, str] = {}
    snapshots: dict[str, int] = {}
    for table in wanted:
        try:
            held = budget.run(f"reading the registration of {table}", current, glue, spec.database, table) if TABLE_NAME.fullmatch(table) else None
            run = (held or {}).get("Parameters", {}).get("hai_run_id")
            if held is not None and held.get("Parameters", {}).get(config.marker) == "true" and run != S3_RUN_ID:
                outcomes[table] = f"kept: registered by publish run {run}"
                continue
            snapshots[table] = budget.run(f"reading the metadata of {table}", source.committed, table)
        except Exception as error:
            outcomes[table] = "failed: " + (str(error) if isinstance(error, MirrorError) else explain(error))

    def keep_or_refuse(table: str, existing: Mapping[str, Any], snapshot_id: int) -> str | None:
        """Leave a publish's registration, an identical snapshot and anything that is not provably older; only an ancestor registration is replaced."""
        held = existing.get("Parameters", {})
        run = held.get("hai_run_id")
        if run != S3_RUN_ID:
            return f"kept: registered by publish run {run}"
        theirs = str(held.get("hai_snapshot_id", ""))
        if not theirs.isdigit():
            raise MirrorError(
                f"the registration holds no readable snapshot ID ({theirs!r}), and cannot be shown older than snapshot {snapshot_id}: nothing was changed"
            )
        relation = source.relation(table, int(theirs), snapshot_id)
        if relation == "same":
            return "unchanged"
        if relation != "ancestor":
            why = {"newer": "is newer than", "unlisted": "is not listed in the newest metadata, so it cannot be shown to be an ancestor of"}.get(
                relation, "is not an ancestor of"
            )
            raise MirrorError(f"the registration holds snapshot {theirs}, which {why} snapshot {snapshot_id} that register would write: nothing was changed")
        return None

    if snapshots:
        report = mirror(config, layer, "real", snapshots, source, DryRunGlue(glue) if dry_run else glue, S3_RUN_ID, budget, keep_or_refuse)
        outcomes.update(report["tables"])
    failed = any(outcome.startswith("failed") for outcome in outcomes.values())
    changed = any(outcome in ("created", "updated") for outcome in outcomes.values())
    return {"status": "failed" if failed else "ok" if changed else "unchanged", "tables": outcomes}


def silver_bronze_sources(path: Path = SILVER_BRONZE_SOURCES) -> set[str]:
    """Every bronze table that a silver dbt model reads, from the dbt sources file. A source that the mirror cannot name by its table name is refused."""
    yaml = importlib.import_module("yaml")

    tables: set[str] = set()
    for source in yaml.safe_load(path.read_text())["sources"]:
        if source.get("schema") != "bronze":
            raise MirrorError(f"the dbt source {source['name']} is not in the bronze schema: the mirror lists bronze tables only")
        for table in source["tables"]:
            if "identifier" in table or not TABLE_NAME.fullmatch(str(table["name"])):
                raise MirrorError(f"the dbt source table {table['name']} has an identifier or a name that is not a plain table name: register cannot name it")
            tables.add(str(table["name"]))
    return tables


def layer_tables(layer: str) -> list[str]:
    """The tables a layer mirrors: the tables of its publish contract, and for bronze the listed bronze tables plus the bronze sources of silver."""
    if layer == "bronze":
        listed = {str(item["table"]) for item in json.loads(BRONZE_TABLES.read_text())["tables"]}
        return sorted(listed | silver_bronze_sources())
    return list(layer_plan(layer).tables)


def layer_plan(layer: str) -> Any:
    """The publish plan of a mirrored layer, read from its own contract."""
    from scripts.lakehouse import gold_publish

    return gold_publish.load_plan(LAYER_CONTRACTS[layer])


def committed_set(spark: Any, layer: str) -> tuple[dict[str, int], str]:
    """The newest committed snapshot set of the layer: the publish_runs row that a reader reads first."""
    from scripts.lakehouse import gold_publish

    ctx = gold_publish.Context(spark, layer_plan(layer), "00000000T000000Z-00000000", "real", Path("/nonexistent"), "", needs_sealed=False)
    head = gold_publish.published_head(ctx)
    if head is None:
        raise MirrorError(f"no committed {layer} snapshot set exists")
    return {str(name): int(snapshot) for name, snapshot in dict(head["snapshot_set"]).items()}, str(head["run_id"])


def register_main(args: argparse.Namespace, parser: argparse.ArgumentParser, glue: Any, s3: Any, config: MirrorConfig, tables: Sequence[str] | None) -> int:
    """The ``register`` command: one report line per table, exit 1 when any table failed."""
    bucket = args.bucket or os.environ.get("DATA_BUCKET_NAME", "")
    if not bucket:
        parser.error("register needs the data bucket: pass --bucket or set DATA_BUCKET_NAME")
    layers = list(MIRRORED_LAYERS) if args.layer == "all" else [args.layer]
    s3 = s3 or s3_client()
    glue = glue or glue_client()
    failed = False
    for layer in layers:
        report = register_from_s3(config, layer, tables if tables is not None else layer_tables(layer), s3, bucket, glue, dry_run=args.dry_run)
        if report["status"] == "skipped":
            sys.stdout.write(f"{layer}: skipped: {report['reason']}\n")
        for table, outcome in report["tables"].items():
            note = " (dry run, nothing written)" if args.dry_run and outcome in ("created", "updated") else ""
            sys.stdout.write(f"{layer}.{table}: {outcome}{note}\n")
        failed = failed or report["status"] == "failed"
    if failed:
        sys.stderr.write("glue mirror register: at least one table was refused or failed; the others are registered, rerun after the cause is fixed\n")
    return 1 if failed else 0


def main(
    argv: Sequence[str] | None = None,
    source: Source | None = None,
    glue: Any = None,
    config: MirrorConfig | None = None,
    snapshots: Mapping[str, int] | None = None,
    run_id: str | None = None,
    s3: Any = None,
    tables: Sequence[str] | None = None,
) -> int:
    """The ``check`` and ``register`` commands. ``check`` without a source and a Glue client builds them: Spark on the Polaris catalog and boto3."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check", "register"))
    parser.add_argument("--layer", required=True, choices=(*MIRRORED_LAYERS, "all"))
    parser.add_argument("--source", choices=("s3",), help="register reads the table metadata from this source: the metadata files in S3")
    parser.add_argument("--bucket", help="register: the data bucket (default: DATA_BUCKET_NAME)")
    parser.add_argument("--dry-run", action="store_true", help="register: plan and write nothing")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    config = config or load_config()
    if args.command == "register":
        if args.source is None:
            parser.error("register needs --source s3")
        return register_main(args, parser, glue, s3, config, tables)
    if args.layer not in PUBLISHED_LAYERS or args.source or args.dry_run or args.bucket:
        parser.error("check takes one layer with a publish_runs table (silver, gold or features) and no --source, --bucket or --dry-run")
    spark = None
    try:
        if source is None or snapshots is None or run_id is None:
            from scripts.lakehouse.session import CATALOG, spark_session

            spark = spark_session(f"glue_mirror_{args.command}", None)
            source = source or SparkSource(spark, CATALOG, config.layers[args.layer].namespace)
            if snapshots is None or run_id is None:
                snapshots, run_id = committed_set(spark, args.layer)
        glue = glue or glue_client()
        result = check(config, args.layer, snapshots, source, glue, run_id)
    finally:
        if spark is not None:
            spark.stop()
    for table, outcome in result["tables"].items():
        sys.stdout.write(f"{args.layer}.{table}: {outcome}\n")
    if not result["ok"]:
        sys.stderr.write(f"glue mirror check: {args.layer} differs from the committed snapshot set\n")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
