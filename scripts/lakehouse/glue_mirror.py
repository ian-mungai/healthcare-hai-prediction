"""Mirror published Iceberg tables into read-only Glue registrations for Athena. Polaris stays the catalog and the source of truth.

After a publish commits, ``gold_publish.mirror_after_commit`` calls ``mirror``. For each table that ``config/lakehouse/athena_mirror.json`` lists, the
mirror makes one Glue table whose ``metadata_location`` is the metadata file that Polaris reports as current. A repeat with the same file writes
nothing. Only mode ``real`` and the configured layers are mirrored, so bronze, test namespaces and the runs table never reach Glue. The configured
tables must be Public in the dbt model metadata (``violations``). The mirror never raises: a failure comes back in the report.

``python -m scripts.lakehouse.glue_mirror check --layer gold`` compares each registration with Polaris and exits 1 on any difference. It writes nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "config/lakehouse/athena_mirror.json"
POINTER = re.compile(r"s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/(?:[^/]+/)*metadata/[^/]+\.metadata\.json")
CURRENT_METADATA_FILE = "SELECT file FROM {table}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1"
TABLE_NAME = re.compile(r"[a-z][a-z0-9_]{0,127}")
PUBLIC = "Public"
LOCATION_CUT = "/metadata/"


class MirrorError(RuntimeError):
    """One table cannot be mirrored; the message says why in plain words."""


@dataclass(frozen=True)
class Layer:
    namespace: str
    database: str
    tables: tuple[str, ...]


@dataclass(frozen=True)
class MirrorConfig:
    enabled: bool
    marker: str
    layers: Mapping[str, Layer]


class Source(Protocol):
    """The Polaris side: the current metadata file and the columns of a published table."""

    def pointer(self, table: str) -> str | None: ...

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

    def pointer(self, table: str) -> str | None:
        rows = self.spark.sql(CURRENT_METADATA_FILE.format(table=self.fq(table))).collect()
        return str(rows[0][0]) if rows else None

    def columns(self, table: str) -> list[tuple[str, str]]:
        return [(str(name), "timestamp" if kind == "timestamp_ntz" else str(kind)) for name, kind in self.spark.table(self.fq(table)).dtypes]


def load_config(path: Path = CONFIG) -> MirrorConfig:
    document = json.loads(path.read_text())
    layers = {name: Layer(item["namespace"], item["database"], tuple(item["tables"])) for name, item in document["layers"].items()}
    return MirrorConfig(document["enabled"] is True, document["mirror_marker"], layers)


def violations(config: MirrorConfig, classes: Mapping[str, str], contract: Mapping[str, set[str]]) -> list[str]:
    """Why the configured tables may not be mirrored: a layer outside silver and gold, a table outside the publish contract or one that is not Public."""
    found: list[str] = []
    for layer, spec in config.layers.items():
        if layer not in ("silver", "gold") or spec.namespace != layer:
            found.append(f"layer {layer} may not be mirrored: only the silver and gold namespaces")
            continue
        for table in spec.tables:
            if not TABLE_NAME.fullmatch(table):
                found.append(f"{layer}.{table} is not a plain table name")
            elif table not in contract.get(layer, set()):
                found.append(f"{layer}.{table} is not a table of the {layer} publish contract")
            elif classes.get(table) != PUBLIC:
                found.append(f"{layer}.{table} is classified {classes.get(table)}, not {PUBLIC}")
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
    wanted = [table for table in spec.tables if table in published]
    return wanted, "" if wanted else f"no configured table of {layer} was published"


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


def register(config: MirrorConfig, spec: Layer, table: str, source: Source, glue: Any) -> str:
    """Create or move one registration; returns created, updated or unchanged."""
    pointer = source.pointer(table)
    if pointer is None:
        raise MirrorError("Polaris reports no metadata file for the table")
    if not POINTER.fullmatch(pointer):
        raise MirrorError("the metadata file is not an s3:// path that ends in .metadata.json")
    existing = current(glue, spec.database, table)
    if existing is not None:
        parameters = existing.get("Parameters", {})
        if parameters.get(config.marker) != "true":
            raise MirrorError(f"Glue already holds {spec.database}.{table} and it is not a mirror: nothing was changed")
        if parameters.get("metadata_location") == pointer:
            return "unchanged"
    columns = [{"Name": name, "Type": kind} for name, kind in source.columns(table)]
    parameters = {"table_type": "ICEBERG", "metadata_location": pointer, config.marker: "true"}
    if existing is not None:
        parameters["previous_metadata_location"] = existing["Parameters"]["metadata_location"]
    document = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": parameters,
        "StorageDescriptor": {"Columns": columns, "Location": pointer.rsplit(LOCATION_CUT, 1)[0]},
    }
    if existing is None:
        try:
            glue.create_table(DatabaseName=spec.database, TableInput=document)
        except Exception as error:
            if glue_error_code(error) == "EntityNotFoundException":
                raise MirrorError(f"database {spec.database} does not exist: apply the Athena mirror stack first") from error
            raise
        return "created"
    glue.update_table(DatabaseName=spec.database, TableInput=document, VersionId=existing["VersionId"])
    return "updated"


def mirror(config: MirrorConfig, layer: str, mode: str, published: Sequence[str], source: Source, glue: Any) -> dict[str, Any]:
    """Mirror the configured tables of one published layer. Never raises: each table's outcome and any failure are in the report."""
    wanted, reason = tables_to_mirror(config, layer, mode, published)
    if not wanted:
        return {"status": "skipped", "reason": reason, "tables": {}}
    outcomes: dict[str, str] = {}
    for table in wanted:
        try:
            outcomes[table] = register(config, config.layers[layer], table, source, glue)
        except Exception as error:
            outcomes[table] = "failed: " + (str(error) if isinstance(error, MirrorError) else explain(error))
    failed = [table for table, outcome in outcomes.items() if outcome.startswith("failed")]
    status = "failed" if failed else "unchanged" if set(outcomes.values()) == {"unchanged"} else "ok"
    report: dict[str, Any] = {"status": status, "tables": outcomes}
    if failed:
        report["reason"] = "; ".join(f"{table} {outcomes[table]}" for table in failed)
    return report


def check(config: MirrorConfig, layer: str, source: Source, glue: Any) -> dict[str, Any]:
    """Compare each configured table's Glue pointer with Polaris's. Reads only."""
    spec = config.layers[layer]
    outcomes: dict[str, str] = {}
    for table in spec.tables:
        try:
            wanted = source.pointer(table)
            found = current(glue, spec.database, table)
            if found is None:
                outcomes[table] = "missing in Glue"
            elif found.get("Parameters", {}).get(config.marker) != "true":
                outcomes[table] = "in Glue but not a mirror"
            elif wanted is None:
                outcomes[table] = "no Polaris metadata file"
            else:
                held = found["Parameters"].get("metadata_location")
                outcomes[table] = "match" if held == wanted else f"differs: Polaris {wanted}, Glue {held}"
        except Exception as error:
            outcomes[table] = "unreadable: " + (str(error) if isinstance(error, MirrorError) else explain(error))
    return {"ok": all(outcome == "match" for outcome in outcomes.values()), "tables": outcomes}


def main(argv: Sequence[str] | None = None, source: Source | None = None, glue: Any = None, config: MirrorConfig | None = None) -> int:
    """The ``check`` command. Without a source and a Glue client it builds them: Spark on the Polaris catalog and boto3 under the task's own role."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check",))
    parser.add_argument("--layer", required=True, choices=("silver", "gold"))
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    config = config or load_config()
    spark = None
    try:
        if source is None:
            from scripts.lakehouse.session import CATALOG, spark_session

            spark = spark_session(f"glue_mirror_{args.command}", None)
            source = SparkSource(spark, CATALOG, config.layers[args.layer].namespace)
        if glue is None:
            import boto3

            glue = boto3.Session(region_name=os.environ["AWS_REGION"]).client("glue")
        result = check(config, args.layer, source, glue)
    finally:
        if spark is not None:
            spark.stop()
    for table, outcome in result["tables"].items():
        sys.stdout.write(f"{args.layer}.{table}: {outcome}\n")
    if not result["ok"]:
        sys.stderr.write(f"glue mirror check: {args.layer} differs from Polaris\n")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
