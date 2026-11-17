"""Cases of the lineage of the direct ECS jobs: the facets the launch operator returns and the events the replay command rebuilds from S3 evidence.

Isolation is justified because the evidence must be wrong on cue (a pin of another run, a record that names an undeclared table, an unreadable object) and
the receiver must be slow on cue. Every case drives the real ``orchestration.ecs_lineage`` and ``orchestration.snapshots``. The launch records come from a
real ``Launcher`` run over the fake ECS API (``ecs_launch_cases.Rig``). The bucket is the in-memory write-once bucket of ``ecs_launch_fake``, and the
receiver is a local HTTP stand-in of the OpenMetadata lineage route. The events are checked against the OpenLineage 2-0-2 schema copied into
``data_contracts/openlineage`` by this file's own validator, not by the module's.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import json
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from orchestration import ecs_lineage
from orchestration.ecs_launch import JobSpec, S3LaunchStore
from orchestration.ecs_launch_operator import LaunchGuarantees
from scripts.orchestration.ecs_launch_cases import BOUND, BUCKET, CLUSTER, PREFIX, RUN, Rig, refuses, require, spec
from scripts.orchestration.ecs_launch_standin import StandInEcsRunTaskOperator

ROOT = Path(__file__).resolve().parents[2]
SPEC_FOLDER = ROOT / "data_contracts" / "openlineage"
FACET_SCHEMA = ROOT / "data_contracts" / "orchestration" / "launch_lineage_facet.schema.json"
JOBS = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())
GOLD_RUN = RUN  # the run of the gold job the launch rig runs
GOLD_INPUT = BOUND["input_run"]  # the silver run the rig's gold launch is bound to
FEATURES_RUN = "20261104T120000Z-feed0001"
CREDENTIAL = "synthetic-lineage-credential"
START = "2026-11-04T10:00:00+00:00"
END = "2026-11-04T10:30:00+00:00"


# The OpenMetadata table of each lakehouse namespace: glue_mirror.hai_lakehouse.<Glue database>.<table> (config/metadata/ingestion/glue.json). Written out here,
# not read from the config, so a changed config or module fails a case. Every layer, bronze included, has a Glue database in athena_mirror.json.
SERVICE = "glue_mirror"
MIRROR_SCHEMA = {"bronze": "bronze", "silver": "silver", "gold": "gold", "features": "features"}


def glue(schema: str, table: str) -> dict[str, str]:
    return {"namespace": f"iceberg://{SERVICE}", "name": f"hai_lakehouse.{MIRROR_SCHEMA[schema]}.{table}"}


DECLARED = {
    "silver": {"inputs": [glue("bronze", "t1"), glue("bronze", "t2")], "outputs": [glue("silver", "s1"), glue("silver", "s2")]},
    "gold": {"inputs": [glue("silver", "s1"), glue("silver", "s2")], "outputs": [glue("gold", "g1")]},
    "features": {"inputs": [glue("gold", "g1")], "outputs": [glue("features", "f1")]},
}
OTHER_DECLARED = {"inputs": [glue("features", "f1")], "outputs": [glue("gold", "model_scores")]}
TEMPLATE = {"silver": "silver_build", "gold": "gold_build", "features": "features_build"}
DAG = {"silver": "silver_transform_dag", "gold": "gold_transform_dag", "features": "features_base_dag"}
RUN_OF = {"silver": GOLD_INPUT, "gold": GOLD_RUN, "features": FEATURES_RUN}


def put(bucket: Any, key: str, value: Any) -> None:
    bucket.objects[key] = value if isinstance(value, bytes) else (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def put_table_inputs(bucket: Any, layer: str, edges: dict[str, list[tuple[str, str]]]) -> None:
    """The record the layer job's upload phase writes beside its dbt copy: each published table with the tables its model reads."""
    run = RUN_OF[layer]
    body = {table: [{"layer": below, "table": name} for below, name in inputs] for table, inputs in edges.items()}
    put(bucket, f"metadata/dbt/{layer}/{run}/table_inputs.json", {"schema": 1, "layer": layer, "run_id": run, "tables": body})


EDGES = {
    "silver": {"s1": [("bronze", "t1")], "s2": [("bronze", "t1"), ("bronze", "t2")]},
    "gold": {"g1": [("silver", "s1")]},
    "features": {"f1": [("gold", "g1")]},
}


def evidence_bucket(rig: Rig | None = None) -> Any:
    """The project bucket with the S3 evidence of a silver, a gold and a features run that chain into each other."""
    bucket = (rig or Rig()).bucket
    bucket.allowed_prefix = ""  # the stand-in holds the launch records and the layer evidence, as the one project bucket does
    layer = "lakehouse/layer_runs"
    silver, gold, features = RUN_OF["silver"], RUN_OF["gold"], RUN_OF["features"]
    put(bucket, f"{layer}/silver/{silver}/state/pinned.json", {"layer": "silver", "mode": "real", "run_id": silver})
    put(bucket, f"{layer}/silver/{silver}/pins.json", {"t1": "11", "t2": "12"})
    put(
        bucket,
        f"{layer}/silver/{silver}/silver_snapshot.json",
        {"schema": 1, "run_id": silver, "mode": "real", "namespace": "silver", "snapshot_set": {"s1": 21, "s2": 22}},
    )
    put(bucket, f"{layer}/gold/{gold}/state/pinned.json", {"layer": "gold", "mode": "real", "run_id": gold})
    put(
        bucket,
        f"{layer}/gold/{gold}/silver_pins.json",
        {"schema": 1, "run_id": gold, "mode": "real", "namespace": "silver", "source_run_id": silver, "snapshot_set": {"s1": 21, "s2": 22}},
    )
    put(
        bucket,
        f"{layer}/gold/{gold}/publish/commit.json",
        {"schema": 1, "run_id": gold, "backup_sha256": "c" * 64, "snapshot_set": {"g1": 31}, "state": "committed"},
    )
    put(bucket, f"{layer}/features/{features}/state/pinned.json", {"layer": "features", "mode": "real", "run_id": features})
    put(
        bucket,
        f"{layer}/features/{features}/gold_pins.json",
        {"schema": 1, "run_id": features, "mode": "real", "namespace": "gold", "source_run_id": gold, "snapshot_set": {"g1": 31}},
    )
    put(
        bucket,
        f"{layer}/features/{features}/features_snapshot.json",
        {"schema": 1, "run_id": features, "mode": "real", "namespace": "features", "snapshot_set": {"f1": 41}},
    )
    for layer, edges in EDGES.items():
        put_table_inputs(bucket, layer, edges)
    return bucket


def facts(job: str, outcome: str = "succeeded", input_run: str | None = None) -> ecs_lineage.LaunchFacts:
    bound = input_run or {"silver": "latest", "gold": RUN_OF["silver"], "features": RUN_OF["gold"]}.get(job, "latest")
    return ecs_lineage.LaunchFacts(run_id=RUN_OF.get(job, GOLD_RUN), job=job, attempt=1, started_at=START, ended_at=END, outcome=outcome, input_run=bound)


def invocation(job: str, template: str | None = None) -> dict[str, Any]:
    template = template or TEMPLATE.get(job) or next(name for name, owner in JOBS["steps"].items() if owner == job)
    dags = json.loads((ROOT / "config/orchestration/host_jobs.json").read_text())["dags"]
    dag = DAG.get(job) or next((name for name, family in dags.items() if family == job), "bronze_chain_dag")  # the bronze job runs in the chain DAG
    request: dict[str, Any] = {
        "request_version": 2,
        "template": template,
        "run_id": RUN_OF.get(job, GOLD_RUN),
        "mode": "real",
        "attempt": 1,
        "dag_id": dag,
        "task_id": template,
        "dag_run_id": f"manual__{RUN_OF.get(job, GOLD_RUN)}",
    }
    if dag == "training_dag":
        request["training"] = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "aws"}
    if dag == "model_promotion_dag":
        request["training"] = {"hai_type": "HAI_1", "model_version": "3", "promotion_mode": "promote", "mlflow": "aws"}
    return request


def validator() -> Callable[[dict[str, Any]], list[str]]:
    """The case's own OpenLineage 2-0-2 RunEvent check, over the copied spec files, plus the closed schema of the project's run facet."""
    resources = []
    for path in sorted(SPEC_FOLDER.glob("*.json")):
        document = json.loads(path.read_text())
        resources.append((document["$id"], Resource.from_contents(document)))
    registry = Registry().with_resources(resources)
    core = json.loads((SPEC_FOLDER / "OpenLineage-2-0-2.json").read_text())
    event_check = Draft202012Validator({"$ref": core["$id"] + "#/$defs/RunEvent"}, registry=registry)
    version_check = Draft202012Validator(
        {"$ref": "https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet"}, registry=registry
    )
    facet_check = Draft202012Validator(json.loads(FACET_SCHEMA.read_text()))

    def check(event: dict[str, Any]) -> list[str]:
        problems = [error.message for error in event_check.iter_errors(event)]
        facet = event.get("run", {}).get("facets", {}).get("haiLaunch")
        problems += ["haiLaunch: " + error.message for error in facet_check.iter_errors(facet)] if facet is not None else ["haiLaunch facet missing"]
        for item in [*event.get("inputs", []), *event.get("outputs", [])]:
            version = item.get("facets", {}).get("version")
            if version is not None:
                problems += [f"{item['name']} version: " + error.message for error in version_check.iter_errors(version)]
        return problems

    return check


def built(job: str, declared: dict[str, Any] | None = None, outcome: str = "succeeded", bucket: Any = None, **change: Any) -> list[dict[str, Any]]:
    bucket = bucket if bucket is not None else evidence_bucket()
    source = ecs_lineage.S3Evidence(bucket, BUCKET)
    lineage = ecs_lineage.job_lineage(job, invocation(job), declared if declared is not None else DECLARED[job], source, facts(job, outcome, **change))
    return ecs_lineage.events(lineage, invocation(job), facts(job, outcome, **change))


def snapshot_id(item: dict[str, str]) -> int:
    return int(hashlib.sha256(item["name"].encode()).hexdigest()[:15], 16) + 1


def tables_of(items: list[dict[str, str]]) -> dict[str, int]:
    return {item["name"].split(".")[-1]: snapshot_id(item) for item in items}


def real_edges(declared: dict[str, Any]) -> dict[str, dict[str, list[tuple[str, str]]]]:
    """Edges over the real declared tables: each output reads two inputs of the layer below (and, in gold and features, the output before it)."""
    found: dict[str, dict[str, list[tuple[str, str]]]] = {}
    for layer in ("silver", "gold", "features"):
        outputs = [item["name"].split(".")[-1] for item in declared[layer]["outputs"]]
        inputs = [item["name"].split(".")[-1] for item in declared[layer]["inputs"]]
        below = {"silver": "bronze", "gold": "silver", "features": "gold"}[layer]
        found[layer] = {}
        for index, table in enumerate(outputs):
            reads = {(below, inputs[index % len(inputs)]), (below, inputs[(index + 1) % len(inputs)])}
            if layer != "silver" and index:
                reads.add((layer, outputs[index - 1]))
            found[layer][table] = sorted(reads)
    return found


def template_bucket(rig: Rig | None = None) -> Any:
    """Evidence of a silver, gold and features run that pins and publishes exactly the tables the contract files declare for each job."""
    bucket = (rig or Rig()).bucket
    bucket.allowed_prefix = ""
    layer = "lakehouse/layer_runs"
    silver, gold, features = RUN_OF["silver"], RUN_OF["gold"], RUN_OF["features"]
    declared = {job: ecs_lineage.declared_lineage(job) for job in ("silver", "gold", "features")}
    put(bucket, f"{layer}/silver/{silver}/state/pinned.json", {"layer": "silver", "mode": "real", "run_id": silver})
    put(bucket, f"{layer}/silver/{silver}/pins.json", {table: str(value) for table, value in tables_of(declared["silver"]["inputs"]).items()})
    silver_set = tables_of(declared["silver"]["outputs"])
    put(
        bucket,
        f"{layer}/silver/{silver}/silver_snapshot.json",
        {"schema": 1, "run_id": silver, "mode": "real", "namespace": "silver", "snapshot_set": silver_set},
    )
    put(
        bucket,
        f"{layer}/gold/{gold}/silver_pins.json",
        {"schema": 1, "run_id": gold, "mode": "real", "namespace": "silver", "source_run_id": silver, "snapshot_set": tables_of(declared["gold"]["inputs"])},
    )
    gold_set = tables_of(declared["gold"]["outputs"])
    put(
        bucket,
        f"{layer}/gold/{gold}/publish/commit.json",
        {"schema": 1, "run_id": gold, "backup_sha256": "c" * 64, "snapshot_set": gold_set, "state": "committed"},
    )
    put(
        bucket,
        f"{layer}/features/{features}/gold_pins.json",
        {
            "schema": 1,
            "run_id": features,
            "mode": "real",
            "namespace": "gold",
            "source_run_id": gold,
            "snapshot_set": tables_of(declared["features"]["inputs"]),
        },
    )
    put(
        bucket,
        f"{layer}/features/{features}/features_snapshot.json",
        {"schema": 1, "run_id": features, "mode": "real", "namespace": "features", "snapshot_set": tables_of(declared["features"]["outputs"])},
    )
    for layer, wanted in real_edges(declared).items():
        put_table_inputs(bucket, layer, wanted)
    return bucket


NO_TABLES = {
    "promote": "it moves a model version in the MLflow registry and reads or writes no table",
    "serve": "it deploys a model version to the model API and reads or writes no table",
    "metadata": "it ingests catalog metadata into OpenMetadata and reads or writes no table data",
    "train": "its DAG runs a task per step with no lineage hooks, so no event of it carries datasets (recorded limit of orchestration/ecs_lineage.py)",
    "drift": "its DAG runs a task per step with no lineage hooks, so no event of it carries datasets (recorded limit of orchestration/ecs_lineage.py)",
}


def glue_items(schema: str, tables: Any) -> list[dict[str, str]]:
    return [glue(schema, table) for table in sorted(tables)]


def code_derived_lineage() -> dict[str, dict[str, list[dict[str, str]]]]:
    """Each lineage job's tables from the code it runs: the table map the bronze job loads, the silver pin and the publish modules of the layers."""
    from scripts.features import features_publish
    from scripts.lakehouse import bronze, gold_publish, pins, silver_publish

    silver, gold, features = silver_publish.publish_plan("real").tables, gold_publish.load_plan().tables, features_publish.publish_plan("real").tables
    loaded = [table["table"] for table in bronze.load_table_map()["tables"]]
    return {
        "bronze": {"inputs": [], "outputs": glue_items("bronze", loaded)},
        "silver": {"inputs": glue_items("bronze", pins.mapped_tables()), "outputs": glue_items("silver", silver)},
        "gold": {"inputs": glue_items("silver", silver), "outputs": glue_items("gold", gold)},
        "features": {"inputs": glue_items("gold", gold), "outputs": glue_items("features", features)},
    }


def declared_lineage_follows_the_code() -> None:
    """Every job that reads or writes tables declares exactly the tables its code pins, loads and publishes, named as the Glue mirror names them; the
    others are named with their reason."""
    derived = code_derived_lineage()
    require(set(derived) | set(NO_TABLES) == set(JOBS["jobs"]), "every job is either derived from its code or named as having no declared tables")
    for job, wanted in derived.items():
        found = ecs_lineage.declared_lineage(job)
        require(bool(found["inputs"] or found["outputs"]), f"{job} reads or writes tables but declares none")
        for side in ("inputs", "outputs"):
            got = sorted(found[side], key=lambda item: item["name"])
            require(
                got == sorted(wanted[side], key=lambda item: item["name"]),
                f"{job} {side}: declared {len(got)} datasets, the code gives {len(wanted[side])}: {[i['name'] for i in got if i not in wanted[side]][:3]}",
            )
    for job in NO_TABLES:
        found = ecs_lineage.declared_lineage(job)
        require(found == {"inputs": [], "outputs": []}, f"{job} declares tables but {NO_TABLES[job]}")
    refuses(ecs_lineage.LineageRefused, lambda: ecs_lineage.declared_lineage("job_without_a_declaration"))


def declared_names_are_the_mirror_names() -> None:
    """A layer's datasets name its Glue database (bronze, silver, gold, features), whatever the mirror's
    ``enabled`` setting says: the names come from the config files, not from a live mirror. Bronze names resolve like the other layers' names."""
    mirror = json.loads((ROOT / "config/lakehouse/athena_mirror.json").read_text())
    for layer, expected in MIRROR_SCHEMA.items():
        require(
            mirror["layers"][layer] == {"namespace": layer, "database": expected},
            f"{layer}: the mirror config names {mirror['layers'].get(layer)}, the case {expected}",
        )
    silver = ecs_lineage.declared_lineage("silver")
    require(all(item["name"].startswith("hai_lakehouse.silver.") for item in silver["outputs"]), "silver outputs are not in silver")
    require(all(item["name"].startswith("hai_lakehouse.bronze.") for item in silver["inputs"]), "silver inputs are not in bronze")
    bronze = ecs_lineage.declared_lineage("bronze")["outputs"]
    require(bool(bronze) and all(item["name"].startswith("hai_lakehouse.bronze.") for item in bronze), "bronze outputs are not in bronze")
    require({item["namespace"] for item in [*silver["inputs"], *silver["outputs"]]} == {f"iceberg://{SERVICE}"}, "every dataset names the glue_mirror service")
    gold_inputs = ecs_lineage.declared_lineage("gold")["inputs"]
    require(all(item["name"].startswith("hai_lakehouse.") and "hai_mirror_" not in item["name"] for item in gold_inputs), "a Polaris or old mirror name")


def every_job_from_the_contract_files() -> None:
    """Each of the nine jobs, with the lineage its contract files declare, gives valid START and terminal events for success and failure. A layer job with
    evidence that pins and publishes exactly its declared tables gets a version on every dataset."""
    check = validator()
    require(len(JOBS["jobs"]) == 9, f"the job table names {len(JOBS['jobs'])} jobs, not nine")
    bucket = template_bucket()
    for job in sorted(JOBS["jobs"]):
        declared = ecs_lineage.declared_lineage(job)
        for outcome in ("succeeded", "failed"):
            events = built(job, declared, outcome, bucket=bucket)
            require([event["eventType"] for event in events] == ["START", "COMPLETE" if outcome == "succeeded" else "FAIL"], f"{job}: event types {events}")
            for event in events:
                problems = check(event)
                require(not problems, f"{job} {event['eventType']}: {problems[:3]}")
            if outcome == "succeeded" and job in ("silver", "gold", "features"):
                complete = events[1]
                require(complete["run"]["facets"]["haiLaunch"]["snapshotState"] == "complete", f"{job}: {complete['run']['facets']['haiLaunch']}")
                require(all("version" in item.get("facets", {}) for item in [*complete["inputs"], *complete["outputs"]]), f"{job}: a dataset has no version")


def every_job_with_declared_datasets() -> None:
    """With Iceberg datasets declared for every job, each event is valid, and a COMPLETE event carries the versions of the S3 evidence."""
    check = validator()
    for job in sorted(JOBS["jobs"]):
        declared = DECLARED.get(job, OTHER_DECLARED)
        events = built(job, declared)
        for event in events:
            problems = check(event)
            require(not problems, f"{job} {event['eventType']}: {problems[:3]}")
        start, complete = events
        require(start["outputs"] == [], f"{job}: a START event names no output")
        facet = complete["run"]["facets"]["haiLaunch"]
        if job in DECLARED:
            require(facet["snapshotState"] == "complete", f"{job}: snapshot state {facet}")
            versions = {item["name"]: item["facets"]["version"]["datasetVersion"] for item in [*complete["inputs"], *complete["outputs"]]}
            wanted = {
                "silver": {
                    "hai_lakehouse.bronze.t1": "11",
                    "hai_lakehouse.bronze.t2": "12",
                    "hai_lakehouse.silver.s1": "21",
                    "hai_lakehouse.silver.s2": "22",
                },
                "gold": {"hai_lakehouse.silver.s1": "21", "hai_lakehouse.silver.s2": "22", "hai_lakehouse.gold.g1": "31"},
                "features": {"hai_lakehouse.gold.g1": "31", "hai_lakehouse.features.f1": "41"},
            }[job]
            require(versions == wanted, f"{job}: versions {versions}")
        else:
            require(
                facet["snapshotState"] == "missing" and facet["missingOutputs"] == OTHER_DECLARED["outputs"], f"{job}: no evidence must be missing: {facet}"
            )


def edges_equal_the_declared_lineage() -> None:
    """The datasets of a COMPLETE event are exactly the declared ones, in their declared order; a FAIL event names the inputs and no output."""
    for job, declared in DECLARED.items():
        complete = built(job)[1]
        named = (
            [{"namespace": item["namespace"], "name": item["name"]} for item in complete["inputs"]],
            [{"namespace": item["namespace"], "name": item["name"]} for item in complete["outputs"]],
        )
        require(named == (declared["inputs"], declared["outputs"]), f"{job}: edges {named}")
        failed = built(job, outcome="failed")[1]
        require(failed["outputs"] == [] and [item["name"] for item in failed["inputs"]] == [item["name"] for item in declared["inputs"]], f"{job}: {failed}")
        require(all("facets" not in item for item in failed["inputs"]), f"{job}: a failed job claims no pinned version")


def names_equal_the_connector_names() -> None:
    """A dataset name is the table FQN the Glue connector creates for the mirror, without its service name; the dbt connector writes to the same service and
    the same Glue databases (the models and the bronze sources are moved to them at ingestion). Every layer of athena_mirror.json is in the connector's
    schema filter and in the dbt mapping, so no dataset names a table the catalog does not read."""
    glue_source = json.loads((ROOT / "config/metadata/ingestion/glue.json").read_text())["source"]
    dbt_config = json.loads((ROOT / "config/metadata/ingestion/dbt.json").read_text())
    mirror = json.loads((ROOT / "config/lakehouse/athena_mirror.json").read_text())
    service, database = glue_source["serviceName"], glue_source["serviceConnection"]["config"]["databaseName"]
    require((service, database) == (SERVICE, "hai_lakehouse"), f"the Glue connector names {service}.{database}")
    require(dbt_config["source"]["serviceName"] == service, "the dbt connector must attach its models to the Glue mirror service")
    require(dbt_config["hai_glue_mapping"]["database"] == database, "the dbt mapping names another OpenMetadata database than the Glue connector")
    databases = {layer: spec["database"] for layer, spec in mirror["layers"].items()}
    require(
        set(databases) == set(MIRROR_SCHEMA) and all(databases[layer] == MIRROR_SCHEMA[layer] for layer in databases), f"the mirror config names {databases}"
    )
    places = dbt_config["hai_glue_mapping"]["schemas"]
    require(places == databases, f"the dbt mapping must name every mirror layer, the models of silver, gold and features and the bronze sources: {places}")
    includes = set(glue_source["sourceConfig"]["config"]["schemaFilterPattern"]["includes"])
    require({f"^{name}$" for name in databases.values()} <= includes, f"the Glue connector's schema filter lacks a mirror database: {sorted(includes)}")
    item = ecs_lineage.dataset("gold", "fact_hai_measure")
    require(item == {"namespace": f"iceberg://{service}", "name": f"{database}.gold.fact_hai_measure"}, f"dataset {item}")
    require(ecs_lineage.connector_fqn(item) == f"{service}.{database}.gold.fact_hai_measure", f"FQN {ecs_lineage.connector_fqn(item)}")
    bronze = ecs_lineage.dataset("bronze", "cms_hai_hospital")
    require(bronze["name"] == f"{database}.bronze.cms_hai_hospital", f"bronze dataset {bronze}")
    require(ecs_lineage.connector_fqn(bronze) == f"{service}.{database}.bronze.cms_hai_hospital", "a bronze table resolves like any other layer's table")
    refuses(ecs_lineage.LineageRefused, lambda: ecs_lineage.dataset("Gold", "x;y"))
    refuses(ecs_lineage.LineageRefused, lambda: ecs_lineage.dataset("polaris_extra", "x"))
    refuses(
        ecs_lineage.LineageRefused,
        lambda: ecs_lineage.connector_fqn({"namespace": "iceberg://polaris", "name": f"{database}.gold.fact_hai_measure"}),
    )
    refuses(ecs_lineage.LineageRefused, lambda: ecs_lineage.connector_fqn({"namespace": f"iceberg://{service}", "name": "lakehouse.gold.fact_hai_measure"}))
    refuses(ecs_lineage.LineageRefused, lambda: ecs_lineage.connector_fqn({"namespace": f"iceberg://{service}", "name": f"{database}.lake.fact_hai_measure"}))
    for old_name in ("hai_mirror_gold.fact_hai_measure", "hai_mirror_bronze.cms_hai_hospital"):
        refuses(
            ecs_lineage.LineageRefused,
            lambda old_name=old_name: ecs_lineage.connector_fqn({"namespace": f"iceberg://{service}", "name": f"{database}.{old_name}"}),
        )
    for job in JOBS["jobs"]:
        for entry in [side for found in ecs_lineage.declared_lineage(job).values() for side in found]:
            require(entry["namespace"] == f"iceberg://{service}", f"{job}: declared namespace {entry['namespace']}")
            ecs_lineage.connector_fqn(entry)  # refuses a declared dataset the connector would name differently


def table_pairs(events: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """The input to output name pairs that OpenMetadata 2.0.7 makes of these events: every input of an event with every output of the same event."""
    return {(i["name"], o["name"]) for event in events if event["eventType"] == "COMPLETE" for i in event["inputs"] for o in event["outputs"]}


def layer_table_events_carry_each_tables_own_edges() -> None:
    """A layer job's datasets travel in one event per published table, with the tables its own model reads in and itself out, so the receiver's pairing
    gives exactly the manifest's edges. Versions: an output has its snapshot, an input its pin."""
    check = validator()
    pins = {"t1": "11", "t2": "12", "s1": "21", "s2": "22", "g1": "31", "f1": "41"}
    for job, edges in EDGES.items():
        found = ecs_lineage.layer_table_events(ecs_lineage.S3Evidence(evidence_bucket(), BUCKET), DECLARED[job], invocation(job), facts(job))
        for event in found:
            require(not check(event), f"{job} {event['eventType']}: {check(event)[:3]}")
        want = {(glue(below, name)["name"], glue(job, table)["name"]) for table, reads in edges.items() for below, name in reads}
        require(table_pairs(found) == want, f"{job}: pairs {sorted(table_pairs(found) ^ want)}")
        require([event["eventType"] for event in found] == ["START", "COMPLETE"] * len(edges), f"{job}: event types")
        for event in (item for item in found if item["eventType"] == "COMPLETE"):
            require(len(event["outputs"]) == 1, f"{job}: one table per event: {event['outputs']}")
            for item in [*event["inputs"], *event["outputs"]]:
                require(item["facets"]["version"]["datasetVersion"] == pins[item["name"].split(".")[-1]], f"{job}: version of {item['name']}")
        started = [item for item in found if item["eventType"] == "START"]
        require(all(item["outputs"] == [] and "facets" not in item["inputs"][0] for item in started), f"{job}: START names no output and no version")
    failed = ecs_lineage.layer_table_events(ecs_lineage.S3Evidence(evidence_bucket(), BUCKET), DECLARED["gold"], invocation("gold"), facts("gold", "failed"))
    require(failed == [], "a job that did not succeed claims no table")


def real_gold_edges_are_not_a_cross_product() -> None:
    """With the real declared gold tables (137 silver inputs, 71 outputs), the events describe the recorded edges only, not the 9,727 pairs of one grouped
    event, and an input a table does not read is not paired with it."""
    declared = {job: ecs_lineage.declared_lineage(job) for job in ("silver", "gold", "features")}
    found = ecs_lineage.layer_table_events(ecs_lineage.S3Evidence(template_bucket(), BUCKET), declared["gold"], invocation("gold"), facts("gold"))
    wanted = real_edges(declared)["gold"]
    pairs = table_pairs(found)
    require(
        len(pairs) == sum(len(reads) for reads in wanted.values()) and len(pairs) < len(declared["gold"]["inputs"]) * len(declared["gold"]["outputs"]),
        f"{len(pairs)} table pairs",
    )
    unrelated = (glue("silver", "int_wonder_county_deaths")["name"], glue("gold", "dim_change_type")["name"])
    require(unrelated not in pairs, "a silver table the gold table does not read is paired with it")
    require(len([event for event in found if event["eventType"] == "COMPLETE"]) == len(declared["gold"]["outputs"]), "one event per published gold table")


def layer_task_event_names_no_dataset() -> None:
    """The task's own event and the facets the provider gets carry the run facet and no dataset, so the receiver cannot pair the layer's inputs with its
    outputs; the replay adds the per-table events."""
    rig = Rig()
    evidence_bucket(rig)
    operator(rig, None).execute(context())
    found = source(rig, spec(), DECLARED["gold"])
    for complete in (True, False):
        facets = found.facets(complete)
        require(facets["inputs"] == [] and facets["outputs"] == [], f"facets name datasets (complete={complete}): {facets}")
    require(found.facets(True)["run_facets"]["haiLaunch"]["snapshotState"] == "complete", "the facet still says the versions are complete")
    events = found.events()
    task, tables = events[:2], events[2:]
    require(all(item["inputs"] == [] and item["outputs"] == [] for item in task), "the task event names no dataset")
    require(task[1]["run"]["facets"]["haiLaunch"]["snapshotState"] == "complete" and len(tables) == 2, f"{len(tables)} table events")
    require(table_pairs(events) == {(glue("silver", "s1")["name"], glue("gold", "g1")["name"])}, f"pairs {table_pairs(events)}")
    broken = Rig()
    evidence_bucket(broken)
    operator(broken, None).execute(context())
    pin = f"lakehouse/layer_runs/gold/{RUN_OF['gold']}/silver_pins.json"
    broken.bucket.objects.pop(pin)
    refused = source(broken, spec(), DECLARED["gold"]).facets(True)
    require(refused["inputs"] == [] and refused["outputs"] == [], f"a refused record leaves no declared dataset: {refused}")
    require(pin in refused["run_facets"]["haiLaunch"]["refusal"], "the facet names the record")


def table_inputs_record_refusals() -> None:
    """A per-table inputs record that is missing, unreadable or wrong is refused by its key and the rule, and a refusal sends no table event."""
    key = "metadata/dbt/gold/" + RUN_OF["gold"] + "/table_inputs.json"

    def change(edit: Callable[[dict[str, Any]], None]) -> Callable[[Any], None]:
        return lambda bucket: edit_record(bucket, key, edit)

    cases: list[tuple[str, Callable[[Any], None], str]] = [
        ("missing", lambda b: b.objects.pop(key), "missing"),
        ("another run", change(lambda d: d.update(run_id=RUN_OF["features"])), "another run"),
        ("another layer", change(lambda d: d.update(layer="silver")), "layer"),
        ("a table nobody published", change(lambda d: d["tables"].update(g9=[])), "not a published table"),
        ("an input nobody pinned or published", change(lambda d: d["tables"].update(g1=[{"layer": "silver", "table": "s9"}])), "not declared"),
        ("an extra key", change(lambda d: d.update(extra=1)), "keys"),
        ("a duplicate key", lambda b: put(b, key, b'{"schema": 1, "schema": 1}'), "duplicate"),
        ("not JSON", lambda b: put(b, key, b"[nope"), "not JSON"),
        ("an input that reads itself", change(lambda d: d["tables"].update(g1=[{"layer": "gold", "table": "g1"}])), "itself"),
    ]
    for label, edit, phrase in cases:
        bucket = evidence_bucket()
        edit(bucket)
        error = refuses(
            ecs_lineage.LineageRefused,
            lambda bucket=bucket: ecs_lineage.layer_table_events(ecs_lineage.S3Evidence(bucket, BUCKET), DECLARED["gold"], invocation("gold"), facts("gold")),
        )
        require(key in str(error) and phrase in str(error), f"{label}: the refusal must name the record and '{phrase}': {error}")
    bucket = evidence_bucket()
    bucket.deny_keys.add(key)
    error = refuses(
        ecs_lineage.EvidenceUnavailable,
        lambda: ecs_lineage.layer_table_events(ecs_lineage.S3Evidence(bucket, BUCKET), DECLARED["gold"], invocation("gold"), facts("gold")),
    )
    require(key in str(error) and "AccessDenied" in str(error), f"an unreadable record is never read as missing: {error}")


def mutated(change: Callable[[Any], None]) -> Any:
    bucket = evidence_bucket()
    change(bucket)
    return bucket


def edit(bucket: Any, key: str, change: Callable[[dict[str, Any]], None]) -> None:
    document = json.loads(bucket.objects[key])
    change(document)
    put(bucket, key, document)


edit_record = edit


def refused_records() -> None:
    """Every invalid or mismatched record is refused, and the refusal names the evidence object and the rule."""
    layer = "lakehouse/layer_runs"
    silver, gold, features = RUN_OF["silver"], RUN_OF["gold"], RUN_OF["features"]
    gold_pin, silver_out = f"{layer}/gold/{gold}/silver_pins.json", f"{layer}/silver/{silver}/silver_snapshot.json"
    cases: list[tuple[str, str, Callable[[Any], None], str]] = [
        ("silver", f"{silver}/pins.json", lambda b: edit(b, f"{layer}/silver/{silver}/pins.json", lambda d: d.update(t3="13")), "not declared"),
        ("silver", f"{silver}/pins.json", lambda b: edit(b, f"{layer}/silver/{silver}/pins.json", lambda d: d.pop("t2")), "every declared input"),
        ("silver", f"{silver}/pins.json", lambda b: edit(b, f"{layer}/silver/{silver}/pins.json", lambda d: d.update(t1="abc")), "closed schema"),
        ("silver", f"{silver}/state/pinned.json", lambda b: b.objects.pop(f"{layer}/silver/{silver}/state/pinned.json"), "claim"),
        ("silver", f"{silver}/silver_snapshot.json", lambda b: edit(b, silver_out, lambda d: d["snapshot_set"].update(s9=29)), "not declared"),
        ("silver", f"{silver}/silver_snapshot.json", lambda b: edit(b, silver_out, lambda d: d.update(run_id=features)), "another run"),
        ("silver", f"{silver}/silver_snapshot.json", lambda b: edit(b, silver_out, lambda d: d.update(mode="publish_fixture")), "real mode"),
        ("gold", f"{gold}/silver_pins.json", lambda b: edit(b, gold_pin, lambda d: d.update(source_run_id=features)), "bound at launch"),
        ("gold", f"{gold}/silver_pins.json", lambda b: edit(b, gold_pin, lambda d: d["snapshot_set"].update(s1=99)), "frozen"),
        ("gold", f"{gold}/silver_pins.json", lambda b: put(b, gold_pin, b'{"schema": 1, "schema": 2}'), "duplicate"),
        ("gold", f"{gold}/silver_pins.json", lambda b: put(b, gold_pin, b"[not json"), "not JSON"),
        (
            "gold",
            f"{gold}/publish/commit.json",
            lambda b: edit(b, f"{layer}/gold/{gold}/publish/commit.json", lambda d: d.update(state="pending")),
            "committed",
        ),
        (
            "features",
            f"{features}/gold_pins.json",
            lambda b: edit(b, f"{layer}/features/{features}/gold_pins.json", lambda d: d.update(namespace="silver")),
            "gold",
        ),
    ]
    for job, key, change, phrase in cases:
        bucket = mutated(change)
        error = refuses(ecs_lineage.LineageRefused, lambda job=job, bucket=bucket: built(job, bucket=bucket))
        require(key in str(error) and phrase in str(error), f"{job} {key}: the refusal must name the object and '{phrase}': {error}")
    bucket = mutated(lambda b: b.deny_keys.add(gold_pin))
    error = refuses(ecs_lineage.EvidenceUnavailable, lambda: built("gold", bucket=bucket))
    require(gold_pin in str(error) and "AccessDenied" in str(error), f"an unreadable pin is never read as missing: {error}")
    bucket = mutated(lambda b: b.objects.pop(gold_pin))
    error = refuses(ecs_lineage.LineageRefused, lambda: built("gold", bucket=bucket))
    require(gold_pin in str(error) and "missing" in str(error), f"a succeeded job without its pin is refused: {error}")
    bucket = mutated(lambda b: b.objects.pop(silver_out))
    complete = built("silver", bucket=bucket)[1]
    facet = complete["run"]["facets"]["haiLaunch"]
    require(facet["snapshotState"] == "missing" and facet["missingOutputs"] == DECLARED["silver"]["outputs"], f"a missing output record: {facet}")
    require(all("facets" not in item for item in complete["outputs"]), "no output version without its record")


def operator(rig: Rig, lineage: Callable[[bool], Any] | None) -> Any:
    class Operator(LaunchGuarantees, StandInEcsRunTaskOperator):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)  # the stand-in metaclass wraps __init__ only when the subclass has its own, as Airflow does

    built_operator = Operator(job=spec(), launcher=rig.launcher, lineage=lineage, task_definition="stand-in", cluster=CLUSTER, overrides={})
    built_operator.client = rig.ecs
    return built_operator


def context() -> dict[str, Any]:
    return {"ti": SimpleNamespace(dag_id="gold_transform_dag", task_id="gold_build", run_id=f"manual__{RUN}", map_index=-1, try_number=1)}


def source(rig: Rig, job: JobSpec, declared: dict[str, Any]) -> ecs_lineage.LineageSource:
    return ecs_lineage.LineageSource(
        job=job.job,
        run_id=job.run_id,
        attempt=job.attempt,
        invocation=invocation("gold"),
        declared=declared,
        evidence=ecs_lineage.S3Evidence(rig.bucket, BUCKET),
        store=S3LaunchStore(rig.bucket, BUCKET, PREFIX),
    )


def operator_returns_the_facets() -> None:
    """After a real launch the operator returns the run facet with the versions' state and no dataset (a layer's datasets travel in per-table events); a
    refused record never fails the task and is named in the facet."""
    rig = Rig()
    evidence_bucket(rig)
    built_operator = operator(rig, source(rig, spec(), DECLARED["gold"]).facets)
    built_operator.execute(context())
    found = built_operator.get_openlineage_facets_on_complete(None)
    require(found["inputs"] == [] and found["outputs"] == [], f"a layer job's facets name no dataset: {found}")
    require(found["run_facets"]["haiLaunch"]["snapshotState"] == "complete", f"facet {found['run_facets']}")
    require(found["run_facets"]["haiLaunch"]["boundInputRun"] == BOUND["input_run"], "the facet names the input run bound at launch")
    failed = built_operator.get_openlineage_facets_on_failure(None)
    require(failed["outputs"] == [] and failed["inputs"] == [], f"on failure: {failed}")
    edit(rig.bucket, f"lakehouse/layer_runs/gold/{RUN}/silver_pins.json", lambda d: d["snapshot_set"].update(s2=98))
    found = built_operator.get_openlineage_facets_on_complete(None)
    facet = found["run_facets"]["haiLaunch"]
    require(facet["snapshotState"] == "refused" and "silver_pins.json" in facet["refusal"], f"a refused record shows in the facet: {facet}")
    require(found["inputs"] == [] and found["outputs"] == [], "a refused record leaves no dataset")

    def broken(complete: bool) -> Any:
        raise RuntimeError("synthetic failure of the lineage source")

    plain = operator(Rig(), broken)
    require(plain.get_openlineage_facets_on_complete(None) is None, "a broken lineage source gives no lineage and raises nothing")
    require(operator(Rig(), None).get_openlineage_facets_on_complete(None) is None, "an operator without a lineage source gives none")


def unreadable_output_record() -> None:
    """A role that may not read the one optional record, a succeeded job's output record, gives the declared outputs without versions and a facet that says
    the record was unreadable, not missing. Every other read still refuses on AccessDenied, and a readable record still adds its versions."""
    layer = "lakehouse/layer_runs"
    silver, gold = RUN_OF["silver"], RUN_OF["gold"]
    commit = f"{layer}/gold/{gold}/publish/commit.json"
    check = validator()
    versions = built("gold")[1]
    require(all(item["facets"]["version"]["datasetVersion"] == "31" for item in versions["outputs"]), "a readable output record adds its versions")
    require(versions["run"]["facets"]["haiLaunch"]["snapshotState"] == "complete", "a readable output record is complete")
    complete = built("gold", bucket=mutated(lambda b: b.deny_keys.add(commit)))[1]
    facet = complete["run"]["facets"]["haiLaunch"]
    require(facet["snapshotState"] == "unreadable", f"a denied output record is unreadable, not missing: {facet}")
    require(facet["missingOutputs"] == DECLARED["gold"]["outputs"], f"every declared output lacks its version: {facet}")
    require(commit in facet["refusal"] and "AccessDenied" in facet["refusal"], f"the facet names the record and the denial: {facet}")
    require([{key: item[key] for key in ("namespace", "name")} for item in complete["outputs"]] == DECLARED["gold"]["outputs"], "the declared outputs are sent")
    require(all("facets" not in item for item in complete["outputs"]), "no output version without its record")
    require(all("version" in item["facets"] for item in complete["inputs"]), "the input versions stay")
    require(not check(complete), f"the unreadable event is valid: {check(complete)[:3]}")
    absent = built("gold", bucket=mutated(lambda b: b.objects.pop(commit)))[1]["run"]["facets"]["haiLaunch"]
    require(absent["snapshotState"] == "missing" and "refusal" not in absent, f"an absent output record is still missing: {absent}")
    denied = [
        ("gold", f"{layer}/gold/{gold}/silver_pins.json"),
        ("gold", f"{layer}/silver/{silver}/silver_snapshot.json"),  # the input run's output record, a required read
        ("silver", f"{layer}/silver/{silver}/pins.json"),
        ("silver", f"{layer}/silver/{silver}/state/pinned.json"),
    ]
    for job, key in denied:
        error = refuses(ecs_lineage.EvidenceUnavailable, lambda job=job, key=key: built(job, bucket=mutated(lambda b: b.deny_keys.add(key))))
        require(key in str(error) and "AccessDenied" in str(error), f"{job}: a denied required record {key} is never read as missing: {error}")
    unready = Rig()
    source_bucket = evidence_bucket(unready)
    source_bucket.deny_keys.add(commit)
    operator(unready, source(unready, spec(), DECLARED["gold"]).facets).execute(context())
    found = source(unready, spec(), DECLARED["gold"]).facets()
    require(found["run_facets"]["haiLaunch"]["snapshotState"] == "unreadable", f"the live facets say unreadable: {found['run_facets']}")
    require(found["inputs"] == [] and found["outputs"] == [], f"the live facets name no dataset: {found}")


class Receiver:
    """A local HTTP stand-in of OpenMetadata's lineage route. ``delay`` holds every reply that long, as a slow or hung receiver does."""

    def __init__(self, delay: float = 0.0) -> None:
        self.requests: list[dict[str, Any]] = []
        self.delay = delay
        self.release = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                owner.requests.append({"path": self.path, "authorization": self.headers.get("Authorization"), "body": body})
                if owner.delay:
                    owner.release.wait(owner.delay)
                reply = json.dumps({"status": "success", "lineageEdgesCreated": 1, "unresolvedDatasets": []}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

            def log_message(self, format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)


@contextmanager
def receiving(delay: float = 0.0) -> Iterator[Receiver]:
    receiver = Receiver(delay)
    receiver.thread.start()
    try:
        yield receiver
    finally:
        receiver.release.set()
        receiver.server.shutdown()
        receiver.server.server_close()
        receiver.thread.join(10)


def receiver_gets_the_bearer_and_the_timeout() -> None:
    """The events reach the stand-in at the lineage route with the bearer credential; a receiver that does not answer is left after the 5 s limit."""
    require(ecs_lineage.ROUTE == "/api/v1/openlineage/lineage" and ecs_lineage.DELIVERY_SECONDS == 5.0, "the route and the 5 second limit are fixed")
    events = built("gold")
    with receiving() as receiver:
        for event in events:
            status = ecs_lineage.post_event(receiver.url, ecs_lineage.canonical(event).encode(), CREDENTIAL)
            require(status == 200, f"status {status}")
    require([item["path"] for item in receiver.requests] == [ecs_lineage.ROUTE] * 2, f"paths {receiver.requests}")
    require(all(item["authorization"] == f"Bearer {CREDENTIAL}" for item in receiver.requests), "every request carries the bearer credential")
    require([json.loads(item["body"]) for item in receiver.requests] == events, "the stand-in received the events byte for byte")
    with receiving(delay=30.0) as slow:
        began = time.monotonic()
        error = refuses(ecs_lineage.DeliveryFailed, lambda: ecs_lineage.post_event(slow.url, b"{}", CREDENTIAL))
        spent = time.monotonic() - began
    require(4.5 <= spent < 7.0, f"a slow receiver is left after the limit, not before and not much later: {spent:.2f} s ({error})")
    require(CREDENTIAL not in str(error), "a delivery error never shows the credential")


def replay_rebuilds_the_same_events() -> None:
    """The replay command rebuilds the events from the launch records and the evidence, byte for byte, any number of times, and writes nothing."""
    rig = Rig()
    evidence_bucket(rig)
    operator(rig, None).execute(context())
    writes = rig.bucket.count("put_object")
    arguments = ["--bucket", BUCKET, "--job", "gold", "--run-id", RUN, "--attempt", "1", "--template", "gold_build"]
    arguments += ["--dag-id", "gold_transform_dag", "--task-id", "gold_build", "--dag-run-id", f"manual__{RUN}"]
    first, second = (ecs_lineage.replay(rig.bucket, arguments, declared=DECLARED["gold"]) for _ in range(2))
    require(first == second and len(first) == 4, "two replays give the same bytes: the task's START and COMPLETE and the table's")
    expected = source(rig, spec(), DECLARED["gold"])
    require([json.loads(line) for line in first] == expected.events(), "the replay equals the events the operator's source builds")
    require(rig.bucket.count("put_object") == writes, "a replay writes nothing to the bucket")
    check = validator()
    require(all(not check(json.loads(line)) for line in first), "replayed events are valid")
    with receiving() as receiver:
        for _ in range(2):
            ecs_lineage.replay(rig.bucket, arguments, declared=DECLARED["gold"], send=(receiver.url, CREDENTIAL))
    bodies = [item["body"] for item in receiver.requests]
    require(len(bodies) == 8 and bodies[:4] == bodies[4:] and [body.decode() for body in bodies[:4]] == first, "a second send repeats the same events")
    unended = Rig()
    evidence_bucket(unended)
    error = refuses(ecs_lineage.LineageRefused, lambda: ecs_lineage.replay(unended.bucket, arguments, declared=DECLARED["gold"]))
    require("nothing to rebuild yet" in str(error), f"a launch that has not ended has nothing to replay: {error}")


def adopted_retry_keeps_its_lineage() -> None:
    """A retry that adopts an earlier successful attempt replays with the versions of that launch, under its own attempt's identity."""
    rig = Rig()
    evidence_bucket(rig)
    operator(rig, None).execute(context())
    retry = operator(rig, None)
    retry.job = spec(2)
    retry.execute({"ti": SimpleNamespace(dag_id="gold_transform_dag", task_id="gold_build", run_id=f"manual__{RUN}", map_index=-1, try_number=2)})
    second = {**invocation("gold"), "attempt": 2}
    found = ecs_lineage.LineageSource(
        "gold", RUN, 2, second, DECLARED["gold"], ecs_lineage.S3Evidence(rig.bucket, BUCKET), S3LaunchStore(rig.bucket, BUCKET, PREFIX)
    )
    events = found.events()
    complete = events[1]
    require(complete["eventType"] == "COMPLETE", f"the adopted success is a COMPLETE event: {complete['eventType']}")
    table = events[3]
    require(table["eventType"] == "COMPLETE" and table["outputs"][0]["name"].endswith(".g1"), f"the table's event follows the task's: {table['eventType']}")
    require(all("version" in item.get("facets", {}) for item in [*table["inputs"], *table["outputs"]]), "the adopted retry keeps the versions")
    require(complete["run"]["facets"]["haiLaunch"]["attempt"] == 2, "the identity is the retry's own attempt")
    first = source(rig, spec(), DECLARED["gold"]).events()
    require(first[1]["run"]["runId"] != complete["run"]["runId"], "the retry is its own run")
    require((first[0]["eventTime"], first[1]["eventTime"]) == (events[0]["eventTime"], events[1]["eventTime"]), "the times are those of the adopted launch")
    arguments = ["--bucket", BUCKET, "--job", "gold", "--run-id", RUN, "--attempt", "2", "--template", "gold_build"]
    arguments += ["--dag-id", "gold_transform_dag", "--task-id", "gold_build", "--dag-run-id", f"manual__{RUN}"]
    require(
        [json.loads(line) for line in ecs_lineage.replay(rig.bucket, arguments, declared=DECLARED["gold"])] == events,
        "the replay of the retry equals its events",
    )
    facets = found.facets()
    require(facets["run_facets"]["haiLaunch"]["snapshotState"] == "complete", f"the live facets of the retry keep the versions: {facets['run_facets']}")


def spec_copies_are_pinned() -> None:
    """The copied OpenLineage files are the published ones: their digests are pinned in the module."""
    found = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(SPEC_FOLDER.glob("*.json"))}
    require(found == ecs_lineage.SPEC_FILES, f"spec copies {found}")


def module_check_agrees() -> None:
    """The module's own event check accepts what the case's validator accepts and refuses an event with an unknown launch facet field."""
    event = built("gold")[1]
    ecs_lineage.check_event(event)
    bad = copy.deepcopy(event)
    bad["run"]["facets"]["haiLaunch"]["extra"] = 1
    refuses(ecs_lineage.LineageRefused, lambda: ecs_lineage.check_event(bad))
    bad = copy.deepcopy(event)
    bad["eventTime"] = 5
    refuses(ecs_lineage.LineageRefused, lambda: ecs_lineage.check_event(bad))


def imports_stay_in_the_image() -> None:
    """The module runs in the Airflow image: it imports nothing from ``scripts`` and nothing from Airflow."""
    tree = ast.parse((ROOT / "orchestration/ecs_lineage.py").read_text())
    names = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    names += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
    require(not [name for name in names if name.split(".")[0] in ("scripts", "airflow")], f"imports {names}")


def lineage_is_built_inside_the_guard() -> None:
    """A lineage source that cannot be built (a job without a declaration) never stops the launch: the step's factory only builds it when the provider
    asks, inside the operator's guard, so the task runs and the provider gets no lineage."""
    from dataclasses import replace

    from orchestration import airflow_steps
    from orchestration.interface import SubmissionRequest

    rig = Rig()
    evidence_bucket(rig)
    factory = airflow_steps.lineage_factory(ecs_lineage.S3Evidence(rig.bucket, BUCKET), S3LaunchStore(rig.bucket, BUCKET, PREFIX))
    request = SubmissionRequest("gold_build", RUN, "real", 1, "gold_transform_dag", "gold_build", f"manual__{RUN}")
    deferred = factory(request, replace(spec(), job="job_without_a_declaration"))
    built_operator = operator(rig, deferred)
    built_operator.execute(context())
    require(built_operator.get_openlineage_facets_on_complete(None) is None, "an undeclared job gives no lineage and raises nothing")
    require(built_operator.get_openlineage_facets_on_failure(None) is None, "nor on failure")


def model_set_families_carry_the_features_layer() -> None:
    """A model set's host family (``features_m1`` to ``features_m6``) keeps its own launch records and has the lineage of the features layer: a source that
    sends the layer's table events, the domain matrices of the feature schema as its inputs and the one table of its set as its output. No other family is
    mapped."""
    schema = json.loads((ROOT / ecs_lineage.FEATURE_SCHEMA_FILE).read_text())
    domain = sorted(name for name, table in schema["tables"].items() if table["role"] == "predictor")
    host = json.loads((ROOT / ecs_lineage.HOST_FILE).read_text())["jobs"]
    sets = sorted(family for family, entry in host.items() if entry.get("like") == "features")
    require(sets == [f"features_m{number}" for number in range(1, 7)], f"the host table names the model sets {sets}")
    for family in sets:
        require(ecs_lineage.lineage_layer(family) == "features", f"{family} is not mapped to the features layer")
        table = schema["model_sets"][family.removeprefix("features_")]["table"]
        expected = {"inputs": [glue("features", name) for name in domain], "outputs": [glue("features", table)]}
        require(ecs_lineage.declared_lineage(family) == expected, f"{family} declares {ecs_lineage.declared_lineage(family)}, not its set's tables")
        require(ecs_lineage.declared_lineage("features") != expected, "the base features run declares the contract tables, not a set's")
        rig = Rig()
        found = ecs_lineage.LineageSource(
            family,
            RUN_OF["features"],
            1,
            invocation("features"),
            DECLARED["features"],
            ecs_lineage.S3Evidence(rig.bucket, BUCKET),
            S3LaunchStore(rig.bucket, BUCKET, PREFIX),
        )
        require(found.job == family and found.layer == "features" and found.layer in ecs_lineage.TABLE_EVENT_JOBS, f"{family} is not a table event source")
    for other in ("bronze", "silver", "gold", "features", "train", "metadata"):
        require(ecs_lineage.lineage_layer(other) == other, f"{other} must be its own layer")


MODEL_RUN = "20261104T130000Z-feed0002"
BASE_COMMITTED_AT = "2026-11-04T10:20:00+00:00"


def base_row_digest_of(run: str, committed_at: str, snapshot_set: dict[str, int]) -> str:
    """The digest of a base ledger row in the writer's canonical form (scripts/features/feature_sets.py ``base_row_digest``), written out here."""
    document = {"run_id": run, "committed_at": committed_at, "snapshot_set": snapshot_set, "base_run": None}
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def model_set_evidence(**change: Any) -> tuple[Any, dict[str, Any], ecs_lineage.LaunchFacts]:
    """Evidence of the model set m1's run over a base features run: the base run's record and its own Feast apply record (the publication it committed, with
    its commit time), the model run's pin with the digest of the base row, its one scoped table and its facts. ``change`` replaces one part of the evidence:
    ``pin_digest``, ``pin_time`` (the commit time the pin's digest was made from), ``apply_time``, ``apply_run``, ``drift`` (a snapshot of the base record that
    changes after the pin) or ``apply=False`` (no apply record)."""
    bucket = template_bucket()
    base, layer = RUN_OF["features"], "lakehouse/layer_runs/features"
    declared = ecs_lineage.declared_lineage("features_m1")
    domain = tables_of(declared["inputs"])
    base_set = {**tables_of(ecs_lineage.declared_lineage("features")["outputs"]), **domain}
    drifted = {**base_set, "feature_bridge_geography_measures": 999} if change.get("drift") else base_set
    put(bucket, f"{layer}/{base}/features_snapshot.json", {"schema": 1, "run_id": base, "mode": "real", "namespace": "features", "snapshot_set": drifted})
    if change.get("apply", True):
        publication = {
            "run_id": change.get("apply_run", base),
            "committed_at": change.get("apply_time", BASE_COMMITTED_AT),
            "tables": len(base_set),
            "snapshot_set_sha256": hashlib.sha256(json.dumps(dict(sorted(drifted.items())), separators=(",", ":")).encode()).hexdigest(),
        }
        put(bucket, f"{layer}/{base}/feast_apply.json", {"schema": 1, "step": "apply", "run_id": base, "mode": "real", "publication": publication})
    digest = change.get("pin_digest") or base_row_digest_of(base, change.get("pin_time", BASE_COMMITTED_AT), base_set)
    pin = {"schema": 1, "run_id": MODEL_RUN, "mode": "real", "namespace": "features", "source_run_id": base, "snapshot_set": domain, "base_row_sha256": digest}
    put(bucket, f"{layer}/{MODEL_RUN}/base_pins.json", pin)
    scoped = tables_of(declared["outputs"])
    record = {"schema": 1, "run_id": MODEL_RUN, "mode": "real", "namespace": "features", "snapshot_set": scoped, "feature_set": "m1", "base_run": base}
    put(bucket, f"{layer}/{MODEL_RUN}/features_snapshot.json", record)
    facts_of = ecs_lineage.LaunchFacts(MODEL_RUN, "features", 1, START, END, "succeeded", base, "m1")
    return bucket, declared, facts_of


def model_set_digest_is_bound_to_its_base_row() -> None:
    """The base-row digest of a model set's pin is compared with the digest of the row that the base run's own evidence gives (its snapshot set, its
    publication's run and commit time): a wrong digest, a digest of another commit time, a changed commit time, a drifted base record, a missing or foreign
    apply record each make the lineage refused, never complete."""
    bucket, declared, found = model_set_evidence()
    lineage = ecs_lineage.job_lineage("features", invocation("features"), declared, ecs_lineage.S3Evidence(bucket, BUCKET), found)
    require(lineage.launch["snapshotState"] == "complete", f"the evidence of a model set with its true digest is {lineage.launch}")
    mutations: dict[str, tuple[dict[str, str | bool], str]] = {
        "a wrong digest": ({"pin_digest": "0" * 64}, "base_row_sha256"),
        "a digest of another commit time": ({"pin_time": "2026-11-04T10:21:00+00:00"}, "base_row_sha256"),
        "a changed commit time": ({"apply_time": "2026-11-04T10:21:00+00:00"}, "base_row_sha256"),
        "a drifted base record": ({"drift": True}, "base_row_sha256"),
        "no apply record": ({"apply": False}, "feast_apply.json"),
        "an apply record of another run": ({"apply_run": MODEL_RUN}, "feast_apply.json"),
    }
    for name, (change, word) in mutations.items():
        bucket, declared, found = model_set_evidence(**change)
        read = partial(ecs_lineage.job_lineage, "features", invocation("features"), declared, ecs_lineage.S3Evidence(bucket, BUCKET), found)
        error = refuses(ecs_lineage.LineageRefused, read)
        require(word in str(error), f"{name} is refused with {error}")


CASES: dict[str, Callable[[], None]] = {
    "model_set_digest_is_bound_to_its_base_row": model_set_digest_is_bound_to_its_base_row,
    "model_set_families_carry_the_features_layer": model_set_families_carry_the_features_layer,
    "declared_lineage_follows_the_code": declared_lineage_follows_the_code,
    "declared_names_are_the_mirror_names": declared_names_are_the_mirror_names,
    "every_job_from_the_contract_files": every_job_from_the_contract_files,
    "every_job_with_declared_datasets": every_job_with_declared_datasets,
    "edges_equal_the_declared_lineage": edges_equal_the_declared_lineage,
    "names_equal_the_connector_names": names_equal_the_connector_names,
    "layer_table_events_carry_each_tables_own_edges": layer_table_events_carry_each_tables_own_edges,
    "real_gold_edges_are_not_a_cross_product": real_gold_edges_are_not_a_cross_product,
    "layer_task_event_names_no_dataset": layer_task_event_names_no_dataset,
    "table_inputs_record_refusals": table_inputs_record_refusals,
    "refused_records": refused_records,
    "operator_returns_the_facets": operator_returns_the_facets,
    "unreadable_output_record": unreadable_output_record,
    "receiver_gets_the_bearer_and_the_timeout": receiver_gets_the_bearer_and_the_timeout,
    "replay_rebuilds_the_same_events": replay_rebuilds_the_same_events,
    "adopted_retry_keeps_its_lineage": adopted_retry_keeps_its_lineage,
    "spec_copies_are_pinned": spec_copies_are_pinned,
    "module_check_agrees": module_check_agrees,
    "imports_stay_in_the_image": imports_stay_in_the_image,
    "lineage_is_built_inside_the_guard": lineage_is_built_inside_the_guard,
}
