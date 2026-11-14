"""Rewrite a layer build's dbt artifacts so OpenMetadata's dbt connector links every published model to its table of the Glue mirror.

A layer build writes every model into a DuckDB build file as ``<build database>.main.<model>``: ``staging`` for silver and gold, ``features`` for the base
feature build (``features_work/features.duckdb``) and ``model_set`` for a model set (``features_work/model_set.duckdb``), the names dbt-duckdb derives from
the file names. The publish step writes each published model to its Iceberg table, which the read-only Glue mirror registers in the Glue database of its layer
(``config/lakehouse/athena_mirror.json``). OpenMetadata's Glue connector creates that table as ``glue_mirror.hai_lakehouse.hai_mirror_<layer>.<model>``. The
pinned OpenMetadata 2.0.7 dbt connector links a manifest node to the table named by the node's database, schema and alias (or name), and parses the model's
compiled SQL for table and column lineage. Without this rewrite no model is linked, and no dbt lineage reaches the catalog.

The rewrite moves each published model to that table, using the table sets the publish steps use (``silver_publish``, ``gold_publish`` and
``features_publish``): the node's ``database``, ``schema`` and ``relation_name``, every ``<build database>.main.<model>`` relation of a published model in
compiled SQL (quoted or not), the ``catalog.json`` metadata and the ``run_results.json`` relation names. A manifest holds every enabled model of the project,
selected or not, so a model with no published table (the model-set matrices, the cost-report domain check) stays exactly as dbt built it and is named by
``unpublished``; it is never a reason to refuse the file. A published model built anywhere but ``<build database>.main`` is refused by name. The places are the
same names whether the mirror is switched on or not. The source nodes stay as dbt wrote them, and the ingestion places them in ``hai_mirror_bronze``; the
compiled SQL of every node that reads a declared source names its mirror table (``rewrite_source_sql``), so the query lineage of OpenMetadata, which looks
an explicit database and schema up first, finds it. The ingestion (``openmetadata_ingest.place_models``) puts the models in the same places too, so a copy
that this rewrite made is not moved again. The output is canonical JSON, so the same input gives the same bytes.
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from functools import cache
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MIRROR_FILE = Path("config/lakehouse/athena_mirror.json")
GLUE_FILE = Path("config/metadata/ingestion/glue.json")
BUILD_DATABASES = ("staging", "features", "model_set")  # the databases of the DuckDB build files of the layer jobs, each with the schema main
BUILD_SCHEMA = "main"
ANY_RELATION = re.compile(r'(?<![\w."])"?([A-Za-z0-9_]+)"?\s*\.\s*"?([A-Za-z0-9_]+)"?\s*\.\s*"?([A-Za-z0-9_]+)"?')
BUILD_RELATION = re.compile(r'(?<![\w."])"?(staging|features|model_set)"?\s*\.\s*"?main"?\s*\.\s*"?([A-Za-z0-9_]+)"?')


class ArtifactError(ValueError):
    """A published model is built where no layer job builds."""


@cache
def mirror_schemas() -> dict[str, str]:
    """The Glue database of each mirrored layer, which is the schema OpenMetadata's Glue connector gives its tables."""
    layers = json.loads((ROOT / MIRROR_FILE).read_text())["layers"]
    return {str(spec["namespace"]): str(spec["database"]) for spec in layers.values()}


@cache
def database() -> str:
    """The OpenMetadata database of the Glue connector."""
    return str(json.loads((ROOT / GLUE_FILE).read_text())["source"]["serviceConnection"]["config"]["databaseName"])


def layer_tables() -> dict[str, str]:
    """Each published table and its layer, from the publish plans of the real mode."""
    from scripts.features import features_publish
    from scripts.lakehouse import gold_publish, silver_publish

    found: dict[str, str] = {}
    for layer, tables in (
        ("silver", silver_publish.publish_plan("real").tables),
        ("gold", gold_publish.load_plan().tables),
        ("features", features_publish.publish_plan("real").tables),
    ):
        for table in tables:
            if table in found:
                raise ArtifactError(f"{table} is published by both {found[table]} and {layer}")
            found[table] = layer
    return found


def relation(layer: str, name: str) -> str:
    return f'"{database()}"."{mirror_schemas()[layer]}"."{name}"'


def build_place(database_name: Any, schema: Any) -> bool:
    return database_name in BUILD_DATABASES and schema == BUILD_SCHEMA


def rewrite_sql(sql: str, tables: Mapping[str, str]) -> str:
    """Every ``<build database>.main.<model>`` relation of a published model in compiled SQL as its published table; a relation of another model stays."""

    def move(match: re.Match[str]) -> str:
        name = match.group(2)
        return relation(tables[name], name) if name in tables else match.group(0)

    return BUILD_RELATION.sub(move, sql)


def source_moves(manifest: Mapping[str, Any]) -> dict[tuple[str, str, str], str]:
    """The relation of each declared source whose schema is a mirrored namespace (bronze) as the dbt build names it, with the mirror table it becomes."""
    schemas = mirror_schemas()
    moves: dict[tuple[str, str, str], str] = {}
    for node in manifest.get("sources", {}).values():
        if node.get("schema") in schemas and node.get("database"):
            table = str(node.get("identifier") or node["name"])
            moves[(str(node["database"]), str(node["schema"]), table)] = relation(str(node["schema"]), table)
    return moves


def rewrite_source_sql(sql: str, moves: Mapping[tuple[str, str, str], str]) -> str:
    """Every relation of compiled SQL that names a declared source (``lakehouse.bronze.<table>``, quoted or not) as the mirror table of that source.
    OpenMetadata's query lineage resolves an explicit database and schema before anything else, so SQL that kept the build's name would look for a
    table that does not exist."""
    if not moves:
        return sql
    return ANY_RELATION.sub(lambda match: moves.get((match.group(1), match.group(2), match.group(3)), match.group(0)), sql)


def model_name(node: Mapping[str, Any]) -> str:
    return str(node.get("alias") or node["name"])


def unpublished(manifest: Mapping[str, Any], tables: Mapping[str, str]) -> list[str]:
    """The models of a manifest that have no published table, sorted: they stay as dbt built them."""
    nodes = manifest.get("nodes", {})
    return sorted({model_name(node) for node in nodes.values() if node.get("resource_type") == "model" and model_name(node) not in tables})


def rewrite_manifest(manifest: Mapping[str, Any], tables: Mapping[str, str]) -> dict[str, Any]:
    """The manifest with each published model at its published table and its compiled SQL naming published tables. A model with no published table is left
    as built. The input is not changed."""
    result = copy.deepcopy(dict(manifest))
    nodes = result.get("nodes", {})
    moves = source_moves(result)
    for key in sorted(nodes):
        node = nodes[key]
        if node.get("resource_type") == "model":
            name = model_name(node)
            if name not in tables:
                continue
            if not build_place(node.get("database"), node.get("schema")):
                raise ArtifactError(f"{key}: built in {node.get('database')}.{node.get('schema')}, not in a build database of the layer jobs")
            layer = tables[name]
            node["database"], node["schema"], node["relation_name"] = database(), mirror_schemas()[layer], relation(layer, name)
        for field in ("compiled_code", "compiled_sql"):
            if isinstance(node.get(field), str):
                node[field] = rewrite_source_sql(rewrite_sql(node[field], tables), moves)
    return result


def table_inputs(manifest: Mapping[str, Any], tables: Mapping[str, str], layer: str, run_id: str) -> dict[str, Any]:
    """The record ``table_inputs.json`` of a layer run: each published table of the layer with the published tables and bronze sources its model reads.

    The inputs of a table are the dependencies of its manifest node. A published model is an input of its own layer; a source is an input of its schema (the
    bronze namespace); a model that is not published is passed through to what it reads; a seed, a test or any other node adds nothing. The orchestration
    reads this record to send one lineage event per table, so that each table is linked to exactly what it reads."""
    nodes, sources = manifest.get("nodes", {}), manifest.get("sources", {})

    def reads(key: str, seen: set[str]) -> set[tuple[str, str]]:
        found: set[tuple[str, str]] = set()
        for dependency in nodes.get(key, {}).get("depends_on", {}).get("nodes", []):
            if dependency in seen:
                continue
            seen.add(dependency)
            if dependency.startswith("source.") and dependency in sources:
                source = sources[dependency]
                found.add((str(source["schema"]), str(source.get("identifier") or source["name"])))
            elif dependency.startswith("model.") and dependency in nodes:
                name = model_name(nodes[dependency])
                if name in tables:
                    found.add((tables[name], name))
                else:
                    found |= reads(dependency, seen)
        return found

    record: dict[str, dict[str, Any]] = {}
    for key in sorted(nodes):
        node = nodes[key]
        name = model_name(node) if node.get("resource_type") == "model" else ""
        if tables.get(name) == layer:
            record[name] = [{"layer": below, "table": table} for below, table in sorted(reads(key, {key}) - {(layer, name)})]
    return {"schema": 1, "layer": layer, "run_id": run_id, "tables": record}


def rewrite_catalog(catalog: Mapping[str, Any], tables: Mapping[str, str]) -> dict[str, Any]:
    """catalog.json with each published model's metadata at its published table; an entry of another model or of a seed stays as built."""
    result = copy.deepcopy(dict(catalog))
    for key in sorted(result.get("nodes", {})):
        metadata = result["nodes"][key]["metadata"]
        if not key.startswith("model.") or metadata.get("name") not in tables:
            continue
        if not build_place(metadata.get("database"), metadata.get("schema")):
            raise ArtifactError(
                f"{key}: the catalog places it in {metadata.get('database')}.{metadata.get('schema')}, not in a build database of the layer jobs"
            )
        metadata["database"], metadata["schema"] = database(), mirror_schemas()[tables[metadata["name"]]]
    return result


def rewrite_run_results(results: Mapping[str, Any], tables: Mapping[str, str]) -> dict[str, Any]:
    """run_results.json with each result's relation name at its published table."""
    result = copy.deepcopy(dict(results))
    for item in result.get("results", []):
        if isinstance(item.get("relation_name"), str):
            item["relation_name"] = rewrite_sql(item["relation_name"], tables)
    return result


def encoded(document: Mapping[str, Any]) -> bytes:
    """Canonical bytes of a rewritten artifact."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


REWRITERS = {"manifest.json": rewrite_manifest, "catalog.json": rewrite_catalog, "run_results.json": rewrite_run_results}
