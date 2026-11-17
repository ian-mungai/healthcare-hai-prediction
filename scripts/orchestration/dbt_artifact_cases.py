"""Cases of the rewrite of the dbt artifacts for OpenMetadata's dbt connector, so it links every model to its table of the Glue mirror.

The pinned OpenMetadata 2.0.7 connector (``ingestion/src/metadata/ingestion/source/database/dbt/metadata.py``) links a manifest node to the table
``<service>.<database>.<schema>.<alias or name>`` and parses each model's compiled SQL for column lineage. A layer build writes every model to
``staging.main`` (the DuckDB build file), while OpenMetadata's Glue connector creates the published tables as
``glue_mirror.hai_lakehouse.<layer>.<model>``. The rewrite moves each model to that table, using the table sets the publish steps use. The manifests
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
MIRROR = {"silver": "silver", "gold": "gold", "features": "features"}
PLACES = {**MIRROR, "bronze": "bronze"}  # the ingestion also places the bronze sources, which have no model
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
    """After the rewrite every three-part relation in compiled SQL is a table of the Glue mirror, quoted or not: a bronze source too, since the mirror has a
    bronze database."""
    for layer in LAYER_MODELS:
        rewritten = artifacts.rewrite_manifest(manifest(layer), TABLES)
        for key, node in rewritten["nodes"].items():
            for database, schema, table in RELATION.findall(node.get("compiled_code", "")):
                published = (database, schema) == ("hai_lakehouse", MIRROR.get(TABLES.get(table, "")))
                source = (database, schema, table) == ("hai_lakehouse", "bronze", "cms_hai_hospital")
                require(published or source, f"{layer} {key}: compiled SQL names {database}.{schema}.{table}")


def bronze_source_sql_follows_its_source() -> None:
    """The compiled SQL of a silver model reads its bronze source as ``lakehouse.bronze.<table>``, quoted or not. OpenMetadata's query lineage resolves an
    explicit database and schema first, so the SQL must name the mirror table the manifest places the source at, through the artifact rewrite, through the
    ingestion placement alone (a copy that was never rewritten) and through both."""
    import re
    import tempfile

    from scripts.infrastructure.openmetadata_ingest import MAPPING_KEY, place_models

    macro = (
        (ROOT_CONFIG.parents[3] / "dbt/macros/canonical_rows.sql").read_text().replace("{% macro canonical_rows(table) %}", "").replace("{% endmacro %}", "")
    )
    expanded = macro.replace("{{ source('bronze', table) }}", '"lakehouse"."bronze"."cms_hai_hospital"').replace("{{ table }}", "cms_hai_hospital")
    expanded = expanded.replace("{{ ref('stg_bronze__files') }}", '"staging"."main"."stg_bronze__files"')
    unquoted = "select a.x from lakehouse.bronze.cms_hai_state as a join lakehouse . bronze . cms_hai_state as b on true"
    base = manifest("silver")
    base["nodes"]["model.hai.stg_cms_hai_hospital"] = {
        **model("stg_cms_hai_hospital", expanded, layer="silver"),
        "depends_on": {"nodes": ["source.hai.bronze.cms_hai_hospital"]},
    }
    base["nodes"]["model.hai.stg_cms_hai_state"] = {
        **model("stg_cms_hai_state", unquoted, layer="silver"),
        "depends_on": {"nodes": ["source.hai.bronze.cms_hai_state"]},
    }
    base["sources"]["source.hai.bronze.cms_hai_state"] = {"resource_type": "source", "name": "cms_hai_state", "database": "lakehouse", "schema": "bronze"}
    tables = {**TABLES, "stg_cms_hai_hospital": "silver", "stg_cms_hai_state": "silver", "stg_bronze__files": "silver"}
    mapping = json.loads(ROOT_CONFIG.read_text())[MAPPING_KEY]
    old = re.compile(r'(?<!\w)"?lakehouse"?\s*\.\s*"?bronze"?\s*\.')  # the dbt build's lakehouse catalog, not hai_lakehouse

    def after_placement(document: dict[str, Any]) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "manifest.json").write_text(json.dumps(document))
            place_models(Path(folder), mapping)
            return dict(json.loads((Path(folder) / "manifest.json").read_text()))

    rewritten = artifacts.rewrite_manifest(base, tables)
    for label, document in (("rewrite", rewritten), ("placement alone", after_placement(copy.deepcopy(base))), ("both", after_placement(rewritten))):
        for name in ("stg_cms_hai_hospital", "stg_cms_hai_state"):
            sql = document["nodes"][f"model.hai.{name}"]["compiled_code"]
            require(not old.search(sql), f"{label}: {name} still reads the old bronze identity: {sql[:200]}")
            require(
                "hai_lakehouse" in sql and "bronze" in sql and name.removeprefix("stg_") in sql,
                f"{label}: {name} does not read the mirror table: {sql[:200]}",
            )
        if label != "placement alone":
            require('"hai_lakehouse"."silver"."stg_bronze__files"' in document["nodes"]["model.hai.stg_cms_hai_hospital"]["compiled_code"], label)
    require(
        rewritten["sources"]["source.hai.bronze.cms_hai_hospital"] == base["sources"]["source.hai.bronze.cms_hai_hospital"], "the rewrite moves models only"
    )
    undeclared = copy.deepcopy(base)
    undeclared["nodes"]["model.hai.stg_cms_hai_hospital"]["compiled_code"] = 'select * from "lakehouse"."bronze"."not_declared"'
    require(
        '"lakehouse"."bronze"."not_declared"' in artifacts.rewrite_manifest(undeclared, tables)["nodes"]["model.hai.stg_cms_hai_hospital"]["compiled_code"],
        "an undeclared relation stays",
    )


def unpublished_models_stay_as_built() -> None:
    """A manifest holds every enabled model, selected or not. A model with no published table (the six model-set matrices, the cost-report domain check)
    stays as dbt built it, with its compiled SQL, and never stops the rewrite of the models that are published."""
    full = manifest("silver")
    for name in ("feature_matrix_m1", "feature_matrix_m6", "int_cost_report_domain_checks"):
        full["nodes"][f"model.hai.{name}"] = model(name, 'select * from "staging"."main"."int_hospital_windows"')
    rewritten = artifacts.rewrite_manifest(full, TABLES)
    for name in ("feature_matrix_m1", "feature_matrix_m6", "int_cost_report_domain_checks"):
        require(rewritten["nodes"][f"model.hai.{name}"] == full["nodes"][f"model.hai.{name}"], f"{name}: an unpublished model changed")
    published = {"nodes": {key: node for key, node in rewritten["nodes"].items() if key.rsplit(".", 1)[1] in TABLES}, "sources": {}}
    require(len(published["nodes"]) == len(LAYER_MODELS["silver"]) and not linked(published), f"the published models are not placed: {linked(published)}")
    require(
        artifacts.unpublished(full, TABLES) == ["feature_matrix_m1", "feature_matrix_m6", "int_cost_report_domain_checks"],
        "the unpublished models are not named",
    )


def unexpected_build_place_is_refused() -> None:
    """A published model built anywhere but a build database of the layer jobs, or outside its main schema, is refused by name."""
    for database, schema in (("other", "main"), ("staging", "other")):
        elsewhere = manifest("gold")
        elsewhere["nodes"]["model.hai.dim_hospital"].update(database=database, schema=schema)
        error = refuses(artifacts.ArtifactError, lambda elsewhere=elsewhere: artifacts.rewrite_manifest(elsewhere, TABLES))
        require("model.hai.dim_hospital" in str(error) and f"{database}.{schema}" in str(error), f"model built in {database}.{schema}: {error}")


def repository_models() -> list[str]:
    """The name of every model file of the dbt project: what a full-project manifest holds."""
    return sorted({path.stem for path in (ROOT_CONFIG.parents[3] / "dbt/models").rglob("*.sql")})


def full_project_manifest_is_copied() -> None:
    """The manifest of a layer build holds every model of the project. With the real publish map the copy is made, the published models are placed, and the
    unselected or unpublished ones stay as built, for the staging build of silver and gold and for both feature builds."""
    import contextlib
    import io
    import tempfile

    from scripts.lakehouse import layer_run

    tables = artifacts.layer_tables()
    names = repository_models()
    require(set(names) - set(tables) >= {"feature_matrix_m1", "int_cost_report_domain_checks"}, "the project has no unpublished model to cover")
    for database in ("staging", "features", "model_set"):
        full = {"metadata": {}, "sources": {}, "nodes": {f"model.hai.{name}": model(name, "select 1") for name in names}}
        for node in full["nodes"].values():
            node.update(database=database, relation_name=f'"{database}"."main"."{node["name"]}"')
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.json"
            path.write_text(json.dumps(full))
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                copied = layer_run.catalog_copy("manifest.json", path, None)
        if copied is None:
            raise AssertionError(f"{database}: the full-project manifest was not copied: {errors.getvalue()}")
        nodes = json.loads(copied)["nodes"]
        placed = {key.rsplit(".", 1)[1] for key, node in nodes.items() if node["database"] == "hai_lakehouse"}
        require(
            placed == set(tables) & set(names), f"{database}: the placed models are not the published ones: {sorted(placed ^ (set(tables) & set(names)))[:5]}"
        )
        require(all(nodes[f"model.hai.{name}"]["database"] == database for name in set(names) - set(tables)), f"{database}: an unpublished model moved")


def feature_targets_are_placed() -> None:
    """The feature builds run in ``features_work/features.duckdb`` (database ``features``) and ``features_work/model_set.duckdb`` (database ``model_set``),
    not in staging. A published feature model built there moves to its Glue table with its relation, its compiled SQL, its catalog entry and its run result."""
    for build, target in (("features", "features_work/features.duckdb"), ("model_set", "features_work/model_set.duckdb")):
        require(Path(target).stem == build, f"{target} does not give the dbt-duckdb database name {build}")
        name = "feature_matrix_hospital"
        node = model(name, " ".join(("select * from", f'"{build}"."main"."int_hospital_spine"')))
        node.update(database=build, relation_name=f'"{build}"."main"."{name}"')
        spine = model("int_hospital_spine", "select 1")
        spine.update(database=build, relation_name=f'"{build}"."main"."int_hospital_spine"')
        built = {"nodes": {f"model.hai.{name}": node, "model.hai.int_hospital_spine": spine}, "sources": {}}
        tables = {**TABLES, "int_hospital_spine": "features"}
        rewritten = artifacts.rewrite_manifest(built, tables)
        moved = rewritten["nodes"][f"model.hai.{name}"]
        require((moved["database"], moved["schema"]) == ("hai_lakehouse", "features"), f"{build}: manifest node {moved['database']}.{moved['schema']}")
        require(moved["relation_name"] == '"hai_lakehouse"."features"."feature_matrix_hospital"', f"{build}: relation {moved['relation_name']}")
        require(
            f'"{build}"."main"' not in moved["compiled_code"] and '"hai_lakehouse"."features"."int_hospital_spine"' in moved["compiled_code"],
            f"{build}: compiled SQL {moved['compiled_code']}",
        )
        tracked = {"nodes": {f"model.hai.{name}": {"metadata": {"database": build, "schema": "main", "name": name}, "columns": {}}}, "sources": {}}
        metadata = artifacts.rewrite_catalog(tracked, tables)["nodes"][f"model.hai.{name}"]["metadata"]
        require((metadata["database"], metadata["schema"]) == ("hai_lakehouse", "features"), f"{build}: catalog {metadata}")
        results = {"results": [{"unique_id": f"model.hai.{name}", "relation_name": f'"{build}"."main"."{name}"', "status": "success"}]}
        require(
            artifacts.rewrite_run_results(results, tables)["results"][0]["relation_name"] == '"hai_lakehouse"."features"."feature_matrix_hospital"',
            f"{build}: run result",
        )


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
        require((metadata["database"], metadata["schema"], metadata["name"]) == ("hai_lakehouse", "silver", name), f"catalog {metadata}")
    results = {"results": [{"unique_id": "model.hai.dim_hospital", "relation_name": '"staging"."main"."dim_hospital"', "status": "success"}]}
    moved = artifacts.rewrite_run_results(results, TABLES)
    require(moved["results"][0]["relation_name"] == '"hai_lakehouse"."gold"."dim_hospital"', f"run results {moved}")
    unknown = catalog("silver")
    unknown["nodes"]["model.hai.stg_unpublished"] = {"metadata": {"database": "staging", "schema": "main", "name": "stg_unpublished"}, "columns": {}}
    require(
        artifacts.rewrite_catalog(unknown, TABLES)["nodes"]["model.hai.stg_unpublished"] == unknown["nodes"]["model.hai.stg_unpublished"],
        "a catalog entry with no published table stays as built",
    )


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
            (source["database"], source["schema"]) == ("hai_lakehouse", "bronze"),
            f"{layer}: a bronze source is placed in bronze: {source}",
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
            require(sent == ["manifest.json", "run_results.json", "table_inputs.json"], f"{layer}: sent {sent}")
            require(
                sorted(bucket.objects) == [f"metadata/dbt/{layer}/{run}/{name}" for name in ("manifest.json", "run_results.json", "table_inputs.json")],
                f"{layer} keys",
            )
            require(layer_run.upload_dbt_run_artifacts(job, "example-private-bucket", bucket) == sent, f"{layer}: a repeat with the same bytes passes")
            (target / "manifest.json").write_text("{}")
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                again = layer_run.upload_dbt_run_artifacts(job, "example-private-bucket", bucket)
            require(
                again == ["run_results.json", "table_inputs.json"] and "differs" in errors.getvalue(),
                f"{layer}: a different copy is reported, not sent: {again}",
            )
            require(bucket.objects[f"metadata/dbt/{layer}/{run}/manifest.json"] != b"{}", f"{layer}: a stored copy is never replaced")


class FileUploads:
    """A stand-in of the client's upload_file that keeps the bytes of each uploaded file, as gold's fixed-key copy sends them."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def upload_file(self, path: str, bucket: str, key: str) -> None:
        self.objects[key] = Path(path).read_bytes()


def copies_name_the_published_tables() -> None:
    """The per-run copy of every layer and gold's fixed-key copy hold the rewritten artifacts, so the catalog reads each model at its published table and
    no staging.main relation of a published model. A model with no published table is reported and left as built."""
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
            require(sent == ["manifest.json", "catalog.json", "table_inputs.json"], f"{layer}: sent {sent}")
            wanted_inputs = artifacts.encoded(artifacts.table_inputs(json.loads((target / "manifest.json").read_text()), TABLES, layer, run))
            require(
                bucket.objects[f"metadata/dbt/{layer}/{run}/table_inputs.json"] == wanted_inputs, f"{layer}: the inputs record is the manifest's dependencies"
            )
            for name in (item for item in sent if item in artifacts.REWRITERS):
                stored = bucket.objects[f"metadata/dbt/{layer}/{run}/{name}"]
                wanted = artifacts.encoded(artifacts.REWRITERS[name](json.loads((target / name).read_text()), TABLES))
                require(stored == wanted and b'"main"' not in stored, f"{layer} {name}: the per-run copy is the rewrite")
            if layer == "gold":
                uploads = FileUploads()
                require(layer_run.upload_dbt_artifacts(job, "example-private-bucket", uploads, TABLES) == ["manifest.json", "catalog.json"], "gold fixed keys")
                stored = uploads.objects["metadata/dbt/manifest.json"]
                require(
                    b'"main"' not in stored and json.loads(stored)["nodes"]["model.hai.fact_hai_measure"]["schema"] == "gold",
                    "the fixed-key copy is rewritten",
                )
            unmapped = {name: value for name, value in TABLES.items() if name != LAYER_MODELS[layer][0]}
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                left = layer_run.upload_dbt_run_artifacts(job, "example-private-bucket", WriteOnceBucket(), unmapped)
            require(
                "manifest.json" in left and "no published table and stay as built" in errors.getvalue(),
                f"{layer}: a model with no published table is reported and left as built, and the copy is made: {left}",
            )


def table_inputs_follow_manifest_dependencies() -> None:
    """Each published table of the layer gets its own inputs from the manifest's dependencies: a published model it reads (of any layer), a bronze source,
    and, through a model that is not published, what that model reads. A seed and a test add nothing, and a model of another layer gets no entry."""
    tables = {"dim_a": "gold", "dim_b": "gold", "int_x": "silver", "fact_c": "features"}

    def node(name: str, *dependencies: str, kind: str = "model") -> dict[str, Any]:
        return {"resource_type": kind, "name": name, "alias": name, "depends_on": {"nodes": list(dependencies)}}

    built = {
        "nodes": {
            "model.hai.dim_a": node("dim_a", "model.hai.int_x", "source.hai.bronze.cms_hai_hospital", "seed.hai.codes", "model.hai.helper"),
            "model.hai.dim_b": node("dim_b", "model.hai.dim_a"),
            "model.hai.int_x": node("int_x", "source.hai.bronze.file_metadata"),
            "model.hai.helper": node("helper", "model.hai.int_x", "source.hai.bronze.cms_hai_state"),
            "model.hai.fact_c": node("fact_c", "model.hai.dim_b"),
            "seed.hai.codes": node("codes", kind="seed"),
            "test.hai.t": node("t", "model.hai.dim_a", kind="test"),
        },
        "sources": {
            "source.hai.bronze.cms_hai_hospital": {"resource_type": "source", "name": "cms_hai_hospital", "schema": "bronze", "database": "lakehouse"},
            "source.hai.bronze.cms_hai_state": {"resource_type": "source", "name": "cms_hai_state", "schema": "bronze", "database": "lakehouse"},
            "source.hai.bronze.file_metadata": {"resource_type": "source", "name": "file_metadata", "schema": "bronze", "database": "lakehouse"},
        },
    }
    record = artifacts.table_inputs(built, tables, "gold", RUNS["gold"])
    require(
        set(record) == {"schema", "layer", "run_id", "tables"} and (record["schema"], record["layer"], record["run_id"]) == (1, "gold", RUNS["gold"]),
        f"{record}",
    )
    expected = {
        "dim_a": [
            {"layer": "bronze", "table": "cms_hai_hospital"},
            {"layer": "bronze", "table": "cms_hai_state"},
            {"layer": "silver", "table": "int_x"},
        ],
        "dim_b": [{"layer": "gold", "table": "dim_a"}],
    }
    require(record["tables"] == expected, f"the inputs of the gold tables: {record['tables']}")
    require(artifacts.encoded(record) == artifacts.encoded(artifacts.table_inputs(copy.deepcopy(built), tables, "gold", RUNS["gold"])), "same bytes twice")


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
        fixed = Path(seen[0]["source"]["sourceConfig"]["config"]["dbtConfigSource"]["dbtManifestFilePath"]) if seen else None
        require(
            code == 0 and len(seen) == 1 and fixed is not None and json.loads(fixed.read_text()) == {"layer": "fixed"},
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


def ingest_saved_runs(bucket: WriteOnceBucket) -> tuple[int, str, list[dict[str, Any]]]:
    """Run the dbt ingestion over the three layer runs in ``bucket`` with the repository config; return its exit code, stderr and workflows."""
    import contextlib
    import io
    import tempfile

    from scripts.infrastructure import openmetadata_ingest

    environment = {"OM_TOKEN": "synthetic-ingestion-credential", "DATA_BUCKET_NAME": "example-private-bucket", "AWS_REGION": "us-west-2"}
    seen: list[dict[str, Any]] = []
    errors = io.StringIO()
    with tempfile.TemporaryDirectory() as folder:
        layers = [item for layer, run in RUNS.items() for item in ("--dbt-run", f"{layer}={run}")]
        arguments = ["dbt", "--config-dir", str(ROOT_CONFIG.parent), "--token-env", "OM_TOKEN", "--work-dir", folder, *layers]
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(errors):
            code = openmetadata_ingest.main(arguments, environment, seen.append, s3_client=bucket)
    return code, errors.getvalue(), seen


def saved_runs(old_names: bool) -> WriteOnceBucket:
    """A bucket that holds the three layer runs as a build wrote them: before the mirror databases took plain layer names (``hai_mirror_<layer>``) or after."""
    import contextlib
    from unittest.mock import patch

    bucket = WriteOnceBucket()
    old = {layer: f"hai_mirror_{layer}" for layer in ("bronze", "silver", "gold", "features")}
    for layer, run in RUNS.items():
        with patch.object(artifacts, "mirror_schemas", return_value=old) if old_names else contextlib.nullcontext():
            prefix = f"metadata/dbt/{layer}/{run}/"
            bucket.objects[prefix + "manifest.json"] = artifacts.encoded(artifacts.rewrite_manifest(manifest(layer), TABLES))
            bucket.objects[prefix + "catalog.json"] = artifacts.encoded(artifacts.rewrite_catalog(catalog(layer), TABLES))
    return bucket


def ingestion_refuses_artifacts_that_name_the_old_mirror_schemas() -> None:
    """A saved run from before the rename names ``hai_mirror_<layer>`` in its SQL and relation names. The ingestion refuses it, with a failure exit and before
    any workflow starts, and names the run and its first node; a run built after the rename still ingests. The stored objects stay as they were."""
    fresh = saved_runs(old_names=False)
    before = dict(fresh.objects)
    code, err, seen = ingest_saved_runs(fresh)
    require(code == 0 and len(seen) == 3 and fresh.objects == before and not fresh.puts, f"a run built after the rename ingests: code {code}, {err}")
    old = saved_runs(old_names=True)
    before = dict(old.objects)
    code, err, seen = ingest_saved_runs(old)
    first = f"metadata/dbt/silver/{RUNS['silver']}/"
    require(code == 1 and not seen, f"a run built before the rename is refused before any workflow: code {code}, {len(seen)} workflows")
    require(
        first in err and "model.hai.int_county_values" in err and "hai_mirror_silver" in err,
        f"the refusal names the run, the first node and the schema: {err!r}",
    )
    require(old.objects == before and not old.puts, "the stored objects are never changed")
    stale = {
        "node schema": lambda node: node.update(schema="hai_mirror_silver"),
        "relation name": lambda node: node.update(relation_name='"hai_lakehouse"."hai_mirror_silver"."int_county_values"'),
        "compiled code": lambda node: node.update(compiled_code='select * from "hai_lakehouse"."hai_mirror_silver"."int_hospital_windows"'),
        "compiled sql": lambda node: node.update(compiled_sql="select * from hai_lakehouse.hai_mirror_gold.fact_hai_measure"),
        "unknown schema of the database": lambda node: node.update(compiled_code="select * from hai_lakehouse.old_silver.int_hospital_windows"),
    }
    for label, damage in stale.items():
        bucket = saved_runs(old_names=False)
        key = f"metadata/dbt/gold/{RUNS['gold']}/manifest.json"
        document = json.loads(bucket.objects[key])
        damage(document["nodes"]["model.hai.dim_hospital"])
        bucket.objects[key] = json.dumps(document).encode()
        code, err, seen = ingest_saved_runs(bucket)
        require(
            code == 1 and not seen and "model.hai.dim_hospital" in err and RUNS["gold"] in err,
            f"{label}: refused and named: code {code}, {len(seen)} workflows, {err!r}",
        )
    bucket = saved_runs(old_names=False)
    key = f"metadata/dbt/features/{RUNS['features']}/catalog.json"
    document = json.loads(bucket.objects[key])
    document["nodes"]["model.hai.feature_matrix_hospital"]["metadata"]["schema"] = "hai_mirror_features"
    bucket.objects[key] = json.dumps(document).encode()
    code, err, seen = ingest_saved_runs(bucket)
    require(code == 1 and not seen and "model.hai.feature_matrix_hospital" in err, f"a stale catalog.json is refused too: code {code}, {err!r}")


CASES: dict[str, Callable[[], None]] = {
    "every_layer_maps_every_model": every_layer_maps_every_model,
    "rewritten_sql_names_only_published_tables": rewritten_sql_names_only_published_tables,
    "bronze_source_sql_follows_its_source": bronze_source_sql_follows_its_source,
    "unpublished_models_stay_as_built": unpublished_models_stay_as_built,
    "unexpected_build_place_is_refused": unexpected_build_place_is_refused,
    "full_project_manifest_is_copied": full_project_manifest_is_copied,
    "feature_targets_are_placed": feature_targets_are_placed,
    "output_is_deterministic": output_is_deterministic,
    "catalog_and_results_follow": catalog_and_results_follow,
    "table_sets_are_the_publish_sets": table_sets_are_the_publish_sets,
    "connector_config_places_the_models": connector_config_places_the_models,
    "rewrite_and_placement_agree": rewrite_and_placement_agree,
    "layer_upload_is_write_once": layer_upload_is_write_once,
    "copies_name_the_published_tables": copies_name_the_published_tables,
    "table_inputs_follow_manifest_dependencies": table_inputs_follow_manifest_dependencies,
    "ingestion_runs_once_per_layer": ingestion_runs_once_per_layer,
    "ingestion_fails_on_an_unreadable_artifact": ingestion_fails_on_an_unreadable_artifact,
    "ingestion_refuses_artifacts_that_name_the_old_mirror_schemas": ingestion_refuses_artifacts_that_name_the_old_mirror_schemas,
}
