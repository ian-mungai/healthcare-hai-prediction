"""Mirror published Iceberg tables into read-only Glue registrations for Athena. Polaris stays the catalog and the source of truth.

After a publish commits, ``gold_publish.mirror_after_commit`` calls ``mirror``. For every table that the publish contract names, Public and
Confidential alike, the mirror makes one Glue table. Its ``metadata_location`` is the metadata file of the table's committed snapshot, taken from the
newest ``publish_runs`` row. A repeat with the same file, snapshot and run writes nothing. Only mode ``real`` and the gold layer of
``config/lakehouse/athena_mirror.json`` are mirrored, so silver, features, bronze, test namespaces and the runs table never reach Glue
(``violations``). The mirror never raises and stops at ``mirror_seconds``: a failure comes back in the report.

``python -m scripts.lakehouse.glue_mirror check --layer gold`` compares each registration with Polaris and exits 1 on any difference. It writes nothing.
"""

from __future__ import annotations

import argparse
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
MIRRORED_LAYERS = ("gold",)
POINTER = re.compile(r"s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/(?:[^/]+/)*metadata/[^/]+\.metadata\.json")
# The newest metadata file whose current snapshot is the committed one. A later publish that failed halfway moves main, but not this answer.
COMMITTED_METADATA_FILE = "SELECT file FROM {table}.metadata_log_entries WHERE latest_snapshot_id = {snapshot} ORDER BY timestamp DESC LIMIT 1"
CONNECT_SECONDS = 5
READ_SECONDS = 20
CALL_ATTEMPTS = 2
TABLE_NAME = re.compile(r"[a-z][a-z0-9_]{0,127}")
LOCATION_CUT = "/metadata/"


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


def load_config(path: Path = CONFIG) -> MirrorConfig:
    document = json.loads(path.read_text())
    layers = {name: Layer(item["namespace"], item["database"]) for name, item in document["layers"].items()}
    return MirrorConfig(document["enabled"] is True, document["mirror_marker"], layers, float(document["mirror_seconds"]))


def violations(config: MirrorConfig) -> list[str]:
    """Why the config may not be used: a layer other than gold, or a namespace other than the layer's own (a test namespace, for example)."""
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


def register(config: MirrorConfig, spec: Layer, table: str, snapshot_id: int, run_id: str, source: Source, glue: Any, budget: Budget) -> str:
    """Create or move one registration to the committed snapshot's metadata file; returns created, updated or unchanged."""
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
            outcomes[table] = register(config, config.layers[layer], table, snapshots[table], run_id, source, glue, budget)
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


def committed_set(spark: Any, layer: str) -> tuple[dict[str, int], str]:
    """The newest committed snapshot set of the layer: the publish_runs row that a reader reads first."""
    from scripts.lakehouse import gold_publish

    ctx = gold_publish.Context(spark, gold_publish.load_plan(), "00000000T000000Z-00000000", "real", Path("/nonexistent"), "", needs_sealed=False)
    head = gold_publish.published_head(ctx)
    if head is None:
        raise MirrorError(f"no committed {layer} snapshot set exists")
    return {str(name): int(snapshot) for name, snapshot in dict(head["snapshot_set"]).items()}, str(head["run_id"])


def main(
    argv: Sequence[str] | None = None,
    source: Source | None = None,
    glue: Any = None,
    config: MirrorConfig | None = None,
    snapshots: Mapping[str, int] | None = None,
    run_id: str | None = None,
) -> int:
    """The ``check`` command. Without a source and a Glue client it builds them: Spark on the Polaris catalog and boto3 under the task's own role."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check",))
    parser.add_argument("--layer", required=True, choices=MIRRORED_LAYERS)
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    config = config or load_config()
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
