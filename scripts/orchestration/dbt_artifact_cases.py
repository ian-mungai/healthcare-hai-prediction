"""Cases of the rewrite of the dbt artifacts for OpenMetadata's dbt connector, so it links every model to its table of the Glue mirror.

The pinned OpenMetadata 2.0.7 connector (``ingestion/src/metadata/ingestion/source/database/dbt/metadata.py``) links a manifest node to the table
``<service>.<database>.<schema>.<alias or name>`` and parses each model's compiled SQL for column lineage. A layer build writes every model to
``staging.main`` (the DuckDB build file), while OpenMetadata's Glue connector creates the published tables as
``glue_mirror.hai_lakehouse.hai_mirror_<layer>.<model>``. The rewrite moves each model to that table, using the table sets the publish steps use. The manifests
here are stand-ins with the fields the connector and the rewrite read; no dbt runs.
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.lakehouse import dbt_catalog_artifacts as artifacts
from scripts.orchestration.ecs_launch_cases import refuses, require

ROOT_CONFIG = Path(__file__).resolve().parents[2] / "config/metadata/ingestion/dbt.json"
LAYER_MODELS = {"silver": ("int_hospital_windows", "int_county_values"), "gold": ("fact_hai_measure", "dim_hospital"), "features": ("feature_matrix_hospital",)}
TABLES = {model: layer for layer, models in LAYER_MODELS.items() for model in models}
# The Glue database of each layer, written out here and not read from config/lakehouse/athena_mirror.json, so a changed config or module fails a case.
MIRROR = {"silver": "hai_mirror_silver", "gold": "hai_mirror_gold", "features": "hai_mirror_features"}
PLACES = {**MIRROR, "bronze": "hai_mirror_bronze"}  # the ingestion also places the bronze sources, which have no model
RELATION = re.compile(r'"?([A-Za-z0-9_]+)"?\s*\.\s*"?([A-Za-z0-9_]+)"?\s*\.\s*"?([A-Za-z0-9_]+)"?')


def model(name: str, sql: str, *, alias: str | None = None, layer: str = "layer") -> dict[str, Any]:
    node: dict[str, Any] = {
        "resource_type": "model",
        "name": name,
        "fqn": ["hai", layer, name],
        "database": "staging",
        "schema": "main",
        "relation_name": f'"staging"."main"."{name}"',
        "compiled_code": sql,
        "depends_on": {"nodes": []},
    }
    if alias is not None:
        node["alias"] = alias
    return node


def manifest(layer: str) -> dict[str, Any]:
    """A manifest of one layer's build: its models read a bronze source or a model of the layer below, quoted the way dbt-duckdb renders relations."""
    nodes: dict[str, Any] = {}
    below = {"silver": None, "gold": "int_hospital_windows", "features": "fact_hai_measure"}[layer]
    for name in LAYER_MODELS[layer]:
        source = '"lakehouse"."bronze"."cms_hai_hospital"' if below is None else f'"staging"."main"."{below}"'
        nodes[f"model.hai.{name}"] = model(
            name, " ".join(("select h.facility_id, h.score from", source, "as h join", f"staging.main.{LAYER_MODELS[layer][0]}", "as s on true")), layer=layer
        )
    nodes["test.hai.not_null_x"] = {
        "resource_type": "test",
        "name": "not_null_x",
        "compiled_code": " ".join(("select * from", f'"staging"."main"."{LAYER_MODELS[layer][0]}"')),
    }
    sources = {"source.hai.bronze.cms_hai_hospital": {"resource_type": "source", "name": "cms_hai_hospital", "database": "lakehouse", "schema": "bronze"}}
    return {"metadata": {"dbt_version": "1.10.0"}, "nodes": nodes, "sources": sources}


def catalog(layer: str) -> dict[str, Any]:
    return {
        "nodes": {f"model.hai.{name}": {"metadata": {"database": "staging", "schema": "main", "name": name}, "columns": {}} for name in LAYER_MODELS[layer]},
        "sources": {},
    }


def linked(rewritten: dict[str, Any]) -> list[str]:
    """Every node that the 2.0.7 linking rule would not find: a model whose database, schema and name are not a table of the Glue mirror, or a bronze source
    that the rewrite changed (bronze has no Glue database, so a source keeps the catalog dbt gave it)."""
    problems = []
    for key, node in rewritten["nodes"].items():
        if node["resource_type"] != "model":
            continue
        name = node.get("alias") or node["name"]
        if (node["database"], node["schema"]) != ("hai_lakehouse", MIRROR.get(TABLES.get(name, ""))):
            problems.append(f"{key} -> {node['database']}.{node['schema']}.{name}")
    for key, node in rewritten["sources"].items():
        if (node["database"], node["schema"]) != ("lakehouse", "bronze"):
            problems.append(f"{key} source moved to {node['database']}.{node['schema']}")
    return problems


def every_layer_maps_every_model() -> None:
    """For each layer's stand-in manifest every model moves to its published table, and every node passes the connector's linking rule."""
    for layer in LAYER_MODELS:
        rewritten = artifacts.rewrite_manifest(manifest(layer), TABLES)
        require(not linked(rewritten), f"{layer}: nodes the connector would not link: {linked(rewritten)}")
        for name in LAYER_MODELS[layer]:
            node = rewritten["nodes"][f"model.hai.{name}"]
            require(node["relation_name"] == f'"hai_lakehouse"."{MIRROR[layer]}"."{name}"', f"{layer} {name}: relation {node['relation_name']}")


def rewritten_sql_names_only_published_tables() -> None:
    """After the rewrite every three-part relation in compiled SQL is a table of the Glue mirror or a bronze source, quoted or not."""
    for layer in LAYER_MODELS:
        rewritten = artifacts.rewrite_manifest(manifest(layer), TABLES)
        for key, node in rewritten["nodes"].items():
            for database, schema, table in RELATION.findall(node.get("compiled_code", "")):
                published = (database, schema) == ("hai_lakehouse", MIRROR.get(TABLES.get(table, "")))
                source = (database, schema) == ("lakehouse", "bronze")
                require(published or source, f"{layer} {key}: compiled SQL names {database}.{schema}.{table}")


def unmapped_model_is_refused() -> None:
    """A model with no published table, a model built elsewhere than the build file, or SQL that names an unpublished relation is refused by name."""
    broken = manifest("silver")
    broken["nodes"]["model.hai.stg_unpublished"] = model("stg_unpublished", "select 1")
    error = refuses(artifacts.ArtifactError, lambda: artifacts.rewrite_manifest(broken, TABLES))
    require("model.hai.stg_unpublished" in str(error) and "no published table" in str(error), f"unmapped model: {error}")
    elsewhere = manifest("gold")
    elsewhere["nodes"]["model.hai.dim_hospital"]["schema"] = "other"
    error = refuses(artifacts.ArtifactError, lambda: artifacts.rewrite_manifest(elsewhere, TABLES))
    require("model.hai.dim_hospital" in str(error) and "staging.main" in str(error), f"model built elsewhere: {error}")
    reads = manifest("gold")
    reads["nodes"]["model.hai.dim_hospital"]["compiled_code"] = 'select * from "staging"."main"."stg_unpublished"'
    error = refuses(artifacts.ArtifactError, lambda: artifacts.rewrite_manifest(reads, TABLES))
    require("model.hai.dim_hospital" in str(error) and "stg_unpublished" in str(error), f"SQL that names an unpublished relation: {error}")
    aliased = manifest("features")
    aliased["nodes"]["model.hai.feature_matrix_hospital"] = model("feature_matrix_hospital", "select 1", alias="other_name")
    error = refuses(artifacts.ArtifactError, lambda: artifacts.rewrite_manifest(aliased, TABLES))
    require("other_name" in str(error), f"an alias with no published table: {error}")


def output_is_deterministic() -> None:
    """The same input gives the same bytes; node order in the input does not matter; the input is not changed."""
    original = manifest("gold")
    before = copy.deepcopy(original)
    reordered = {**original, "nodes": dict(reversed(list(original["nodes"].items())))}
    first, second = artifacts.encoded(artifacts.rewrite_manifest(original, TABLES)), artifacts.encoded(artifacts.rewrite_manifest(reordered, TABLES))
    require(first == second, "the rewrite gives the same bytes for the same nodes")
    require(original == before, "the input manifest is not changed")


def catalog_and_results_follow() -> None:
    """catalog.json nodes move with the models; run_results relation names move too; an unknown catalog node is refused."""
    rewritten = artifacts.rewrite_catalog(catalog("silver"), TABLES)
    for name in LAYER_MODELS["silver"]:
        metadata = rewritten["nodes"][f"model.hai.{name}"]["metadata"]
        require((metadata["database"], metadata["schema"], metadata["name"]) == ("hai_lakehouse", "hai_mirror_silver", name), f"catalog {metadata}")
    results = {"results": [{"unique_id": "model.hai.dim_hospital", "relation_name": '"staging"."main"."dim_hospital"', "status": "success"}]}
    moved = artifacts.rewrite_run_results(results, TABLES)
    require(moved["results"][0]["relation_name"] == '"hai_lakehouse"."hai_mirror_gold"."dim_hospital"', f"run results {moved}")
    unknown = catalog("silver")
    unknown["nodes"]["model.hai.stg_unpublished"] = {"metadata": {"database": "staging", "schema": "main", "name": "stg_unpublished"}, "columns": {}}
    refuses(artifacts.ArtifactError, lambda: artifacts.rewrite_catalog(unknown, TABLES))


def table_sets_are_the_publish_sets() -> None:
    """The real table map comes from the publish plans: every silver, gold and features table maps to its own layer, and no table is in two layers."""
    tables = artifacts.layer_tables()
    from scripts.features import features_publish
    from scripts.lakehouse import gold_publish, silver_publish

    plans = (("silver", silver_publish.publish_plan("real")), ("gold", gold_publish.load_plan()), ("features", features_publish.publish_plan("real")))
    for layer, plan in plans:
        require(all(tables[name] == layer for name in plan.tables), f"{layer} tables map elsewhere")
    require(len(tables) == sum(len(plan.tables) for _, plan in plans), "a table is in two layers or a table is missing")


def connector_config_places_the_models() -> None:
    """dbt.json names the Glue mirror as the place of the models (the database and the Glue database of each layer, as the cases state them), attaches the
    models to the Glue connector's service, has no catalog mapping for the bronze sources, and still passes the ingestion's own checks."""
    from scripts.infrastructure.openmetadata_ingest import MAPPING_KEY, violations

    config = json.loads(ROOT_CONFIG.read_text())
    mapping = config.pop(MAPPING_KEY)
    require(mapping == {"database": "hai_lakehouse", "schemas": PLACES}, f"the models and bronze sources are not placed in the Glue mirror: {mapping}")
    require(config["source"]["serviceName"] == "glue_mirror", f"the dbt models attach to {config['source']['serviceName']}")
    require("catalogMapping" not in config["source"]["sourceConfig"]["config"], "bronze is not mirrored, so no catalog maps to a Glue database")
    require(not violations(config), f"dbt.json is refused: {violations(config)}")


def rewrite_and_placement_agree() -> None:
    """The copy a layer uploads (the rewrite) and the placement the ingestion applies (``place_models``) put each model in the same Glue table, so a copy
    made by one and read by the other is not moved a second time."""
    import tempfile

    from scripts.infrastructure.openmetadata_ingest import MAPPING_KEY, place_models

    mapping = json.loads(ROOT_CONFIG.read_text())[MAPPING_KEY]
    for layer in LAYER_MODELS:
        rewritten = artifacts.rewrite_manifest(manifest(layer), TABLES)
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "manifest.json").write_text(json.dumps(rewritten))
            moved = place_models(Path(folder), mapping)
            placed = json.loads((Path(folder) / "manifest.json").read_text())
        require(moved == len(LAYER_MODELS[layer]) + 1, f"{layer}: the placement moved {moved} nodes, the models and the one bronze source")
        for key, node in rewritten["nodes"].items():
            if node["resource_type"] == "model":
                require(
                    (placed["nodes"][key]["database"], placed["nodes"][key]["schema"]) == (node["database"], node["schema"]), f"{layer} {key}: placed apart"
                )
        source = placed["sources"]["source.hai.bronze.cms_hai_hospital"]
        require(
            (source["database"], source["schema"]) == ("hai_lakehouse", "hai_mirror_bronze"),
            f"{layer}: a bronze source is placed in hai_mirror_bronze: {source}",
        )


RUNS = {"silver": "20261104T100000Z-5111e201", "gold": "20261104T110000Z-601d0001", "features": "20261104T120000Z-feed0001"}


class WriteOnceBucket:
    """A bucket stand-in with the write-once put the layer writers use, a read for the comparison and the downloads the ingestion makes."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts: list[str] = []
        self.failures: dict[str, Exception] = {}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, IfNoneMatch: str) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        self.puts.append(Key)
        if IfNoneMatch != "*":
            raise AssertionError("every dbt copy is a write-once put")
        if Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Key] = bytes(Body)
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        import io

        return {"Body": io.BytesIO(self.objects[Key])}

    def download_file(self, bucket: str, key: str, path: str) -> None:
        from botocore.exceptions import ClientError

        if key in self.failures:
            raise self.failures[key]
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")  # what boto3's download_file raises for a missing key
        Path(path).write_bytes(self.objects[key])


def layer_upload_is_write_once() -> None:
    """Each layer copies its dbt files once to metadata/dbt/<layer>/<run>/; a repeat finds the same bytes, a different copy is reported and never replaced."""
    import contextlib
    import io
    import tempfile

    from scripts.lakehouse import layer_run

    for layer, run in RUNS.items():
        with tempfile.TemporaryDirectory() as folder:
            job = layer_run.Job(layer=layer, run=run, mode="real", code=Path(folder))
            target = job.folder / layer_run.DBT_TARGET.get(layer, Path("target"))  # features builds in features_work/build/target
            target.mkdir(parents=True)
            for name in ("manifest.json", "run_results.json"):
                (target / name).write_text(f'{{"layer": "{layer}", "file": "{name}"}}')
            bucket = WriteOnceBucket()
            sent = layer_run.upload_dbt_run_artifacts(job, "example-private-bucket", bucket)
            require(sent == ["manifest.json", "run_results.json"], f"{layer}: sent {sent}")
            require(sorted(bucket.objects) == [f"metadata/dbt/{layer}/{run}/manifest.json", f"metadata/dbt/{layer}/{run}/run_results.json"], f"{layer} keys")
            require(layer_run.upload_dbt_run_artifacts(job, "example-private-bucket", bucket) == sent, f"{layer}: a repeat with the same bytes passes")
            (target / "manifest.json").write_text("{}")
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                again = layer_run.upload_dbt_run_artifacts(job, "example-private-bucket", bucket)
            require(again == ["run_results.json"] and "differs" in errors.getvalue(), f"{layer}: a different copy is reported, not sent: {again}")
            require(bucket.objects[f"metadata/dbt/{layer}/{run}/manifest.json"] != b"{}", f"{layer}: a stored copy is never replaced")


class FileUploads:
    """A stand-in of the client's upload_file that keeps the bytes of each uploaded file, as gold's fixed-key copy sends them."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def upload_file(self, path: str, bucket: str, key: str) -> None:
        self.objects[key] = Path(path).read_bytes()


def copies_name_the_published_tables() -> None:
    """The per-run copy of every layer and gold's fixed-key copy hold the rewritten artifacts, so the catalog reads each model at its published table and
    no staging.main relation. A model with no published table is reported and not copied, never sent as built."""
    import contextlib
    import io
    import tempfile

    from scripts.lakehouse import layer_run

    for layer, run in RUNS.items():
        with tempfile.TemporaryDirectory() as folder:
            job = layer_run.Job(layer=layer, run=run, mode="real", code=Path(folder))
            target = job.folder / layer_run.DBT_TARGET.get(layer, Path("target"))
            target.mkdir(parents=True)
            (target / "manifest.json").write_text(json.dumps(manifest(layer)))
            (target / "catalog.json").write_text(json.dumps(catalog(layer)))
            bucket = WriteOnceBucket()
            sent = layer_run.upload_dbt_run_artifacts(job, "example-private-bucket", bucket, TABLES)
            require(sent == ["manifest.json", "catalog.json"], f"{layer}: sent {sent}")
            for name in sent:
                stored = bucket.objects[f"metadata/dbt/{layer}/{run}/{name}"]
                wanted = artifacts.encoded(artifacts.REWRITERS[name](json.loads((target / name).read_text()), TABLES))
                require(stored == wanted and b'"main"' not in stored, f"{layer} {name}: the per-run copy is the rewrite")
            if layer == "gold":
                uploads = FileUploads()
                require(layer_run.upload_dbt_artifacts(job, "example-private-bucket", uploads, TABLES) == ["manifest.json", "catalog.json"], "gold fixed keys")
                stored = uploads.objects["metadata/dbt/manifest.json"]
                require(
                    b'"main"' not in stored and json.loads(stored)["nodes"]["model.hai.fact_hai_measure"]["schema"] == "hai_mirror_gold",
                    "the fixed-key copy is rewritten",
                )
            unmapped = {name: value for name, value in TABLES.items() if name != LAYER_MODELS[layer][0]}
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                left = layer_run.upload_dbt_run_artifacts(job, "example-private-bucket", WriteOnceBucket(), unmapped)
            require(
                "manifest.json" not in left and "has no published table" in errors.getvalue(), f"{layer}: an unmapped model is reported, not copied: {left}"
            )


def ingestion_runs_once_per_layer() -> None:
    """Given layer runs, the dbt ingestion fetches each layer's copy and runs the workflow once per layer; without them it reads the fixed keys as before."""
    import tempfile

    from scripts.infrastructure import openmetadata_ingest

    config_dir = ROOT_CONFIG.parent
    bucket = WriteOnceBucket()
    for layer, run in RUNS.items():
        bucket.objects[f"metadata/dbt/{layer}/{run}/manifest.json"] = json.dumps({"layer": layer}).encode()
    bucket.objects["metadata/dbt/manifest.json"] = b'{"layer": "fixed"}'
    environment = {"OM_TOKEN": "synthetic-ingestion-credential", "DATA_BUCKET_NAME": "example-private-bucket", "AWS_REGION": "us-west-2"}
    with tempfile.TemporaryDirectory() as folder:
        seen: list[dict[str, Any]] = []
        layers = [item for layer, run in RUNS.items() for item in ("--dbt-run", f"{layer}={run}")]
        arguments = ["dbt", "--config-dir", str(config_dir), "--token-env", "OM_TOKEN", "--work-dir", folder, *layers]
        code = openmetadata_ingest.main(arguments, environment, seen.append, s3_client=bucket)
        require(code == 0 and len(seen) == 3, f"one workflow per layer: code {code}, {len(seen)} runs")
        for config, layer in zip(seen, RUNS, strict=True):
            path = Path(config["source"]["sourceConfig"]["config"]["dbtConfigSource"]["dbtManifestFilePath"])
            require(json.loads(path.read_text()) == {"layer": layer}, f"{layer}: the workflow read {path}")
        seen.clear()
        code = openmetadata_ingest.main(
            ["dbt", "--config-dir", str(config_dir), "--token-env", "OM_TOKEN", "--work-dir", folder], environment, seen.append, s3_client=bucket
        )
        path = Path(seen[0]["source"]["sourceConfig"]["config"]["dbtConfigSource"]["dbtManifestFilePath"]) if seen else None
        require(
            code == 0 and len(seen) == 1 and path is not None and json.loads(path.read_text()) == {"layer": "fixed"},
            "without layer runs the fixed keys are read",
        )
        for bad in (
            ["--dbt-run", "bronze=" + RUNS["gold"]],
            ["--dbt-run", "gold=latest"],
            ["--dbt-run", f"gold={RUNS['gold']}", "--dbt-run", f"gold={RUNS['gold']}"],
        ):
            seen.clear()
            code = openmetadata_ingest.main(
                ["dbt", "--config-dir", str(config_dir), "--token-env", "OM_TOKEN", "--work-dir", folder, *bad], environment, seen.append, s3_client=bucket
            )
            require(code == 2 and not seen, f"{bad} must be refused before any workflow")
        missing = WriteOnceBucket()
        seen.clear()
        code = openmetadata_ingest.main(arguments, environment, seen.append, s3_client=missing)
        require(code == 1 and not seen, "a layer run without its manifest fails before any workflow runs")


def ingestion_fails_on_an_unreadable_artifact() -> None:
    """Only a missing object is absent. A refused or timed-out download of any dbt file fails the ingestion before any workflow runs."""
    import tempfile

    from botocore.exceptions import ClientError, ReadTimeoutError

    from scripts.infrastructure import openmetadata_ingest

    environment = {"OM_TOKEN": "synthetic-ingestion-credential", "DATA_BUCKET_NAME": "example-private-bucket", "AWS_REGION": "us-west-2"}
    arguments = ["dbt", "--config-dir", str(ROOT_CONFIG.parent), "--token-env", "OM_TOKEN"]
    failures = {
        "access denied": ClientError({"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "HeadObject"),
        "forbidden": ClientError({"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"),
        "timeout": ReadTimeoutError(endpoint_url="https://s3.example.invalid"),
    }
    for label, failure in failures.items():
        bucket = WriteOnceBucket()
        bucket.objects["metadata/dbt/manifest.json"] = b"{}"
        bucket.failures["metadata/dbt/catalog.json"] = failure
        seen: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as folder:
            code = openmetadata_ingest.main([*arguments, "--work-dir", folder], environment, seen.append, s3_client=bucket)
        require(code == 1 and not seen, f"{label}: an unreadable catalog.json fails the ingestion (code {code}, {len(seen)} workflows)")
    bucket = WriteOnceBucket()
    bucket.objects["metadata/dbt/manifest.json"] = b"{}"
    seen = []
    with tempfile.TemporaryDirectory() as folder:
        code = openmetadata_ingest.main([*arguments, "--work-dir", folder], environment, seen.append, s3_client=bucket)
    settings = seen[0]["source"]["sourceConfig"]["config"]["dbtConfigSource"] if seen else {}
    require(code == 0 and "dbtCatalogFilePath" not in settings and "dbtManifestFilePath" in settings, "a missing catalog.json is left out and the run goes on")


CASES: dict[str, Callable[[], None]] = {
    "every_layer_maps_every_model": every_layer_maps_every_model,
    "rewritten_sql_names_only_published_tables": rewritten_sql_names_only_published_tables,
    "unmapped_model_is_refused": unmapped_model_is_refused,
    "output_is_deterministic": output_is_deterministic,
    "catalog_and_results_follow": catalog_and_results_follow,
    "table_sets_are_the_publish_sets": table_sets_are_the_publish_sets,
    "connector_config_places_the_models": connector_config_places_the_models,
    "rewrite_and_placement_agree": rewrite_and_placement_agree,
    "layer_upload_is_write_once": layer_upload_is_write_once,
    "copies_name_the_published_tables": copies_name_the_published_tables,
    "ingestion_runs_once_per_layer": ingestion_runs_once_per_layer,
    "ingestion_fails_on_an_unreadable_artifact": ingestion_fails_on_an_unreadable_artifact,
}
