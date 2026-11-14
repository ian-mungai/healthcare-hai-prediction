"""Rewrite a layer build's dbt artifacts so OpenMetadata's dbt connector links every model to its table of the Glue mirror.

A layer build writes every model into the DuckDB build file as ``staging.main.<model>``, and the publish step writes it to its Iceberg table, which the
read-only Glue mirror registers in the Glue database of its layer (``config/lakehouse/athena_mirror.json``). OpenMetadata's Glue connector creates that table
as ``glue_mirror.hai_lakehouse.hai_mirror_<layer>.<model>``. The pinned OpenMetadata 2.0.7 dbt connector links a manifest node to the table named by the node's
database, schema and alias (or name), and parses the model's compiled SQL for table and column lineage. Without this rewrite no model is linked, and no dbt
lineage reaches the catalog.

The rewrite moves each model to that table, using the table sets the publish steps use (``silver_publish``, ``gold_publish`` and ``features_publish``):
the node's ``database``, ``schema`` and ``relation_name``, every ``staging.main.<model>`` relation in compiled SQL (quoted or not), the ``catalog.json``
metadata and the ``run_results.json`` relation names. The places are the same names whether the mirror is switched on or not. A model with no published
table, a model built anywhere but ``staging.main`` and compiled SQL that names an unpublished relation are refused by name. The bronze sources keep the dbt
catalog ``lakehouse`` and schema ``bronze``: bronze has no Glue database, so they stay unresolved in the catalog. The ingestion
(``openmetadata_ingest.place_models``) puts the models in the same places, so a copy that this rewrite made is not moved again. The output is canonical JSON,
so the same input gives the same bytes.
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
BUILD = ("staging", "main")
BUILD_RELATION = re.compile(r'(?<![\w."])"?staging"?\s*\.\s*"?main"?\s*\.\s*"?([A-Za-z0-9_]+)"?')


class ArtifactError(ValueError):
    """A dbt artifact names a model or relation that has no published table."""


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


def published(name: str, tables: Mapping[str, str], where: str) -> str:
    layer = tables.get(name)
    if layer is None:
        raise ArtifactError(f"{where}: model {name} has no published table")
    return layer


def rewrite_sql(sql: str, tables: Mapping[str, str], where: str) -> str:
    """Every ``staging.main.<model>`` relation of compiled SQL as its published table; a relation with no published table is refused."""

    def move(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in tables:
            raise ArtifactError(f"{where}: compiled SQL names staging.main.{name}, which has no published table")
        return relation(tables[name], name)

    return BUILD_RELATION.sub(move, sql)


def rewrite_manifest(manifest: Mapping[str, Any], tables: Mapping[str, str]) -> dict[str, Any]:
    """The manifest with each model at its published table and its compiled SQL naming published tables. The input is not changed."""
    result = copy.deepcopy(dict(manifest))
    nodes = result.get("nodes", {})
    for key in sorted(nodes):
        node = nodes[key]
        if node.get("resource_type") == "model":
            name = node.get("alias") or node["name"]
            if (node.get("database"), node.get("schema")) != BUILD:
                raise ArtifactError(f"{key}: built in {node.get('database')}.{node.get('schema')}, not staging.main")
            layer = published(name, tables, key)
            node["database"], node["schema"], node["relation_name"] = database(), mirror_schemas()[layer], relation(layer, name)
        for field in ("compiled_code", "compiled_sql"):
            if isinstance(node.get(field), str):
                node[field] = rewrite_sql(node[field], tables, key)
    return result


def rewrite_catalog(catalog: Mapping[str, Any], tables: Mapping[str, str]) -> dict[str, Any]:
    """catalog.json with each model node's metadata at its published table."""
    result = copy.deepcopy(dict(catalog))
    for key in sorted(result.get("nodes", {})):
        metadata = result["nodes"][key]["metadata"]
        if (metadata.get("database"), metadata.get("schema")) != BUILD:
            raise ArtifactError(f"{key}: the catalog places it in {metadata.get('database')}.{metadata.get('schema')}, not staging.main")
        metadata["database"], metadata["schema"] = database(), mirror_schemas()[published(metadata["name"], tables, key)]
    return result


def rewrite_run_results(results: Mapping[str, Any], tables: Mapping[str, str]) -> dict[str, Any]:
    """run_results.json with each result's relation name at its published table."""
    result = copy.deepcopy(dict(results))
    for item in result.get("results", []):
        if isinstance(item.get("relation_name"), str):
            item["relation_name"] = rewrite_sql(item["relation_name"], tables, str(item.get("unique_id")))
    return result


def encoded(document: Mapping[str, Any]) -> bytes:
    """Canonical bytes of a rewritten artifact."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


REWRITERS = {"manifest.json": rewrite_manifest, "catalog.json": rewrite_catalog, "run_results.json": rewrite_run_results}
