"""Cases for the MlModel step of the metadata job: the MLflow source and the table-to-model edges, against stand-ins shaped on the live probe of Nov 17 2026.

The probe ran the pinned OpenMetadata 2.0.7 server, the MLflow connector and a throwaway MLflow 3.17.2 server (probe exit 0). The stand-ins answer as it did:
the connector ingests a registered model with its features; a PATCH of ``mlFeatures.featureSources`` is refused with HTTP 400 (so no code here sends one);
``PUT /api/v1/lineage`` from a table to an ``mlmodel`` answers 200 with an empty body, the edge reads back as the table's downstream and the model's upstream,
and it survives a second ingest of the model. MLflow answers 401 without a login and 403 ``Permission denied`` to a reader without grants, for an existing and
a missing model alike. Nothing here reaches a network: every request goes through an in-memory stand-in, so the cases prove this code's own behavior and not
the real servers'. Each case raises ``AssertionError`` on a defect. ``run_openmetadata_e2e`` runs them.
"""

from __future__ import annotations

import base64
import json
import tempfile
import urllib.parse
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from unittest import mock

OM_HOST = "http://10.0.1.20:8585/api"
MLFLOW_URI = "http://10.0.1.20:5100"
READER = ("metadata_reader", "Synthetic-Reader-1!")
SERVICE = "mlflow_registry"
PROBE_400 = "Invalid value for field 'mlFeatures.featureSources.dataType' on MlModel. Allowed values are: array, boolean, date, integer, number, object"
LOGIN_KEY = "MLFLOW_TRACKING_PASSWORD"
WRONG_LOGIN = "Synthetic-Wrong-1!"


def base() -> Any:
    from scripts.infrastructure import run_openmetadata_e2e

    return run_openmetadata_e2e


def edges() -> Any:
    from scripts.infrastructure import openmetadata_mlmodel_edges

    return openmetadata_mlmodel_edges


def contract_models() -> list[str]:
    root = Path(__file__).resolve().parents[2]
    return sorted(json.loads((root / "data_contracts/training/model_contract.json").read_text())["registered_models"].values())


def feature_tables() -> list[str]:
    """The feature_matrix tables the registrable slots' feature lists name, read straight from the files, so the case does not use the code under test."""
    root = Path(__file__).resolve().parents[2]
    contract = json.loads((root / "data_contracts/training/model_contract.json").read_text())
    slots = json.loads((root / "config/training/models.json").read_text())["slots"]
    found: set[str] = set()
    for slot in contract["registrable_slots"]:
        references = json.loads((root / "config/training/feature_lists" / slots[slot]["feature_list"]).read_text())["references"]
        found.update(reference.split(":")[0] for reference in references)
    return sorted(found)


class World:
    """One MLflow server and one OpenMetadata server in memory. A call is ``world(method, url, headers, body)``; every call is logged."""

    def __init__(self, models: tuple[str, ...] = (), ingested: tuple[str, ...] | None = None, grants: str = "reader") -> None:
        self.mlflow_models = set(models)
        self.grants = grants  # reader: wildcard READ on models, experiments and logged models; none: no grant at all; model_only: one model name only
        self.mlmodels = {f"{SERVICE}.{name}": f"model-{index}" for index, name in enumerate(sorted(self.mlflow_models if ingested is None else ingested))}
        self.tables: dict[str, str] = {}
        self.edge_set: set[tuple[str, str]] = set()
        self.calls: list[tuple[str, str]] = []
        self.fail_put = False
        self.hide_edges = False
        self.token = base().BEARER_VALUE

    def catalog_all_tables(self) -> None:
        from orchestration import ecs_lineage

        for table in feature_tables():
            self.tables[ecs_lineage.connector_fqn(ecs_lineage.dataset("features", table))] = f"table-{table}"

    def reingest(self) -> None:
        """What the connector does on a second run: the MlModel is written again from the registry. Its edges are separate entity relationships."""
        self.calls.append(("INGEST", "mlflow"))

    def __call__(self, method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> tuple[int, Any]:
        self.calls.append((method, url))
        parts = urllib.parse.urlsplit(url)
        path = urllib.parse.unquote(parts.path)
        query = urllib.parse.parse_qs(parts.query)
        if url.startswith(MLFLOW_URI):
            return self.mlflow(method, path, query, headers)
        if headers.get("Authorization") != f"Bearer {self.token}":
            return 401, {"code": 401, "message": "Unauthorized"}
        if method == "PATCH":
            return 400, {"code": 400, "message": PROBE_400}
        if method == "GET" and path.startswith("/api/v1/mlmodels/name/"):
            fqn = path.rsplit("/", 1)[1]
            return (
                (200, {"id": self.mlmodels[fqn], "name": fqn.split(".", 1)[1], "fullyQualifiedName": fqn})
                if fqn in self.mlmodels
                else (404, {"code": 404, "message": f"mlModel instance for {fqn} not found"})
            )
        if method == "GET" and path.startswith("/api/v1/tables/name/"):
            fqn = path.rsplit("/", 1)[1]
            return (
                (200, {"id": self.tables[fqn], "fullyQualifiedName": fqn})
                if fqn in self.tables
                else (404, {"code": 404, "message": f"table instance for {fqn} not found"})
            )
        if method == "PUT" and path == "/api/v1/lineage":
            edge = json.loads(body or b"{}")["edge"]
            if self.fail_put:
                return 500, {"code": 500, "message": "boom"}
            self.edge_set.add((edge["fromEntity"]["id"], edge["toEntity"]["id"]))
            return 200, {}
        if method == "GET" and path.startswith("/api/v1/lineage/mlmodel/name/"):
            fqn = path.rsplit("/", 1)[1]
            model = self.mlmodels[fqn]
            upstream = [] if self.hide_edges else [{"fromEntity": source, "toEntity": target} for source, target in sorted(self.edge_set) if target == model]
            return 200, {"entity": {"id": model, "type": "mlmodel", "fullyQualifiedName": fqn}, "nodes": [], "upstreamEdges": upstream, "downstreamEdges": []}
        return 404, {"code": 404, "message": f"no route {method} {path}"}

    def mlflow(self, method: str, path: str, query: dict[str, list[str]], headers: Mapping[str, str]) -> tuple[int, Any]:
        if headers.get("Authorization") != "Basic " + base64.b64encode(":".join(READER).encode()).decode():
            return 401, None
        if method == "GET" and path == "/api/3.0/mlflow/users/current/permissions":
            wildcard = [
                {"permission": "READ", "resource_pattern": "*", "resource_type": kind, "role_id": 2, "role_name": "__user_2__", "workspace": "default"}
                for kind in ("registered_model", "experiment", "logged_model")
            ]
            named = [
                {
                    "permission": "READ",
                    "resource_pattern": "synthetic",
                    "resource_type": "registered_model",
                    "role_id": 2,
                    "role_name": "__user_2__",
                    "workspace": "default",
                }
            ]
            grants = {"reader": wildcard, "none": [], "model_only": named, "no_experiment": wildcard[:1] + wildcard[2:]}[self.grants]
            return 200, {"is_admin": False, "permissions": grants}
        if method == "GET" and path == "/api/2.0/mlflow/registered-models/get":
            name = query["name"][0]
            if name in self.mlflow_models:
                return 200, {"registered_model": {"name": name}}
            return 404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": f"Registered Model with name={name} not found"}
        return 404, {"error_code": "ENDPOINT_NOT_FOUND", "message": path}


def environment(**changes: str | None) -> dict[str, str]:
    env = {
        "OM_API": OM_HOST,
        "OM_INGESTION_TOKEN": base().BEARER_VALUE,
        "MLFLOW_TRACKING_URI": MLFLOW_URI,
        "MLFLOW_TRACKING_USERNAME": READER[0],
        "MLFLOW_TRACKING_PASSWORD": READER[1],
    }
    for name, value in changes.items():
        if value is None:
            env.pop(name, None)
        else:
            env[name] = value
    return env


def run(world: Callable[..., tuple[int, Any]], **changes: str | None) -> tuple[int, str, str]:
    with base().quiet() as (out, err):
        code = edges().main([], env=environment(**changes), http=world)
    return code, out.getvalue(), err.getvalue()


def secrets_absent(*texts: str) -> None:
    for text in texts:
        base().require(not any(secret in text for secret in (base().BEARER_VALUE, READER[1], WRONG_LOGIN)), "a secret reached the output")


def tables_are_named_by_the_mirror_config() -> None:
    from orchestration import ecs_lineage

    require = base().require
    schema = ecs_lineage.connector().schemas["features"]
    world = World(models=("hai_model_HAI_1",))
    world.catalog_all_tables()
    code, out, err = run(world)
    require(code == 0, (code, out, err))
    asked = sorted(url.rsplit("/", 1)[1] for method, url in world.calls if method == "GET" and "/api/v1/tables/name/" in url)
    expected = sorted(f"glue_mirror.hai_lakehouse.{schema}.{table}" for table in feature_tables())
    require(asked == expected, f"table names do not follow the mirror config: {asked}")
    # A renamed mirror database (the plain-names change) is followed with no code change: the names come from the mapping function only.
    plain = ecs_lineage.Connector("glue_mirror", "hai_lakehouse", {**ecs_lineage.connector().schemas, "features": "features"})
    renamed = World(models=("hai_model_HAI_1",))
    with mock.patch.object(ecs_lineage, "connector", return_value=plain):
        renamed.catalog_all_tables()
        code, out, err = run(renamed)
    require(code == 0, (code, out, err))
    require(
        all(f"/glue_mirror.hai_lakehouse.features.{table}" in " ".join(url for _, url in renamed.calls) for table in feature_tables()),
        "a renamed mirror database is not followed",
    )
    source = (Path(__file__).resolve().parent / "openmetadata_mlmodel_edges.py").read_text()
    require("hai_mirror" not in source and "hai_lakehouse" not in source, "the edge step must not spell a mirror database or table prefix")


def one_edge_is_drawn_from_each_feature_table_to_each_registered_model() -> None:
    require = base().require
    models = ("hai_model_HAI_1", "hai_model_HAI_4")
    world = World(models=models)
    world.catalog_all_tables()
    code, out, err = run(world)
    require(code == 0, (code, out, err))
    tables = feature_tables()
    require(len(tables) >= 2 and "feature_matrix_hospital_window" in tables, tables)
    require(len(world.edge_set) == len(models) * len(tables), f"edges: {len(world.edge_set)}")
    require(all(source.startswith("table-") and target.startswith("model-") for source, target in world.edge_set), world.edge_set)
    require(f"{len(models) * len(tables)} edges" in out, out)
    require("hai_model_HAI_2" in out and "no registered model yet" in out, f"the types without a registered model are reported: {out}")
    secrets_absent(out, err)


def the_edge_request_is_the_probed_one_and_never_a_patch() -> None:
    require = base().require
    sent: list[tuple[str, str, dict[str, Any]]] = []
    world = World(models=("hai_model_HAI_1",))
    world.catalog_all_tables()

    def spy(method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> tuple[int, Any]:
        if body:
            sent.append((method, url, json.loads(body)))
        return world(method, url, headers, body)

    code, out, err = run(spy)
    require(code == 0, (code, out, err))
    require(
        sent and all(method == "PUT" and url == f"{OM_HOST}/v1/lineage" for method, url, _ in sent),
        f"only PUT /v1/lineage may carry a body: {[(m, u) for m, u, _ in sent]}",
    )
    for _, _, document in sent:
        edge = document["edge"]
        require(set(document) == {"edge"} and edge["fromEntity"]["type"] == "table" and edge["toEntity"]["type"] == "mlmodel", document)
        require(set(edge["fromEntity"]) == {"id", "type"} and set(edge["toEntity"]) == {"id", "type"}, edge)
    require(
        not any(method == "PATCH" for method, _ in world.calls) and "featureSources" not in json.dumps(sent),
        "featureSources must never be sent (the server refuses it with 400)",
    )


def a_rerun_and_a_second_ingest_keep_the_same_edges() -> None:
    require = base().require
    world = World(models=("hai_model_HAI_1",))
    world.catalog_all_tables()
    code, _, err = run(world)
    require(code == 0, err)
    first = set(world.edge_set)
    world.reingest()
    code, out, err = run(world)
    require(code == 0 and world.edge_set == first and len(first) == len(feature_tables()), (code, out, err))
    world.hide_edges = False
    require(all(call[0] != "PATCH" for call in world.calls), "a repeat must not patch")


def a_reader_without_the_wildcard_grants_fails_visibly() -> None:
    require = base().require
    for grants in ("none", "model_only", "no_experiment"):
        world = World(models=("hai_model_HAI_1",), grants=grants)
        world.catalog_all_tables()
        code, out, err = run(world)
        require(code == 1 and "metadata_reader" in err and "READ" in err, (grants, code, err))
        require(not world.edge_set and not any(call[0] == "PUT" for call in world.calls), "no edge may be drawn on an unreadable registry")
        secrets_absent(out, err)
    admin = World(models=("hai_model_HAI_1",))

    def as_admin(method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> tuple[int, Any]:
        if url.endswith("/api/3.0/mlflow/users/current/permissions"):
            return 200, {"is_admin": True, "permissions": []}
        return admin(method, url, headers, body)

    admin.catalog_all_tables()
    code, out, err = run(as_admin)
    require(code == 0, f"an administrator login reads everything: {(code, err)}")


def a_missing_login_or_a_server_refusal_fails_without_edges() -> None:
    require = base().require
    world = World(models=("hai_model_HAI_1",))
    world.catalog_all_tables()
    code, out, err = run(world, **{LOGIN_KEY: WRONG_LOGIN})
    require(code == 1 and "401" in err and not world.edge_set, (code, err))
    secrets_absent(out, err)
    for name in ("MLFLOW_TRACKING_USERNAME", LOGIN_KEY, "MLFLOW_TRACKING_URI", "OM_INGESTION_TOKEN", "OM_API"):
        fresh = World(models=("hai_model_HAI_1",))
        code, out, err = run(fresh, **{name: None})
        require(code == 2 and name in err and not fresh.calls, (name, code, err))


def a_registered_model_that_the_catalog_lacks_stops_the_run() -> None:
    """The registry holds the model but the connector left no MlModel (zero models read): the run fails and names it. This is the rule for a first champion."""
    require = base().require
    world = World(models=("hai_model_HAI_1", "hai_model_HAI_2"), ingested=("hai_model_HAI_1",))
    world.catalog_all_tables()
    code, out, err = run(world)
    require(code == 1 and "hai_model_HAI_2" in err and "no MlModel" in err, (code, err))
    require(not world.edge_set, "a run that finds a model missing from the catalog draws nothing")
    empty = World(models=("hai_model_HAI_1",), ingested=())
    empty.catalog_all_tables()
    code, out, err = run(empty)
    require(code == 1 and "hai_model_HAI_1" in err, (code, err))


def no_registered_model_yet_is_reported_and_draws_nothing() -> None:
    require = base().require
    world = World()
    world.catalog_all_tables()
    code, out, err = run(world)
    require(code == 0 and "no registered model yet" in out and not world.edge_set and not any(call[0] == "PUT" for call in world.calls), (code, out, err))


def a_table_the_catalog_lacks_stops_the_run_before_any_edge() -> None:
    require = base().require
    world = World(models=("hai_model_HAI_1",))
    world.catalog_all_tables()
    gone = sorted(world.tables)[0]
    del world.tables[gone]
    code, out, err = run(world)
    require(code == 1 and gone in err and not world.edge_set, (code, err))
    require(not any(call[0] == "PUT" for call in world.calls), "an edge was drawn although a table is absent")


def a_refused_edge_or_a_missing_read_back_fails_without_secrets() -> None:
    require = base().require
    world = World(models=("hai_model_HAI_1",))
    world.catalog_all_tables()
    world.fail_put = True
    code, out, err = run(world)
    require(code == 1 and "500" in err, (code, err))
    secrets_absent(out, err)
    quiet_world = World(models=("hai_model_HAI_1",))
    quiet_world.catalog_all_tables()
    quiet_world.hide_edges = True
    code, out, err = run(quiet_world)
    require(code == 1 and "read back" in err, f"an edge that the lineage read does not show is a failure: {(code, err)}")


def the_catalog_and_registry_addresses_are_checked_before_any_call() -> None:
    require = base().require
    for label, changes in (
        ("public catalog", {"OM_API": "http://203.0.113.5:8585/api"}),
        ("loopback catalog", {"OM_API": "http://127.0.0.1:8585/api"}),
        ("public registry", {"MLFLOW_TRACKING_URI": "http://203.0.113.5:5100"}),
        ("https registry", {"MLFLOW_TRACKING_URI": "https://10.0.1.20:5100"}),
        ("wrong registry port", {"MLFLOW_TRACKING_URI": "http://10.0.1.20:5000"}),
    ):
        world = World(models=("hai_model_HAI_1",))
        code, out, err = run(world, **changes)
        require(code == 2 and not world.calls, (label, code, err))
        secrets_absent(out, err)


def the_mlflow_source_config_is_metadata_only_and_holds_no_login() -> None:
    from scripts.infrastructure import openmetadata_ingest as ingest

    require = base().require
    config = base().load("mlflow")
    require(ingest.violations(config) == [], ingest.violations(config))
    connection = config["source"]["serviceConnection"]["config"]
    require(config["source"]["type"] == "mlflow" and connection["type"] == "Mlflow", config["source"])
    require(connection["trackingUri"] == "${MLFLOW_TRACKING_URI}" and connection["registryUri"] == "${MLFLOW_TRACKING_URI}", connection)
    require(config["source"]["sourceConfig"]["config"]["type"] == "MlModelMetadata", "the pipeline type must be the probed MlModelMetadata")
    text = json.dumps(config).lower()
    require("password" not in text and "username" not in text and "token" in text, "the login travels as environment variables, never in the file")
    require(config["source"]["serviceName"] == SERVICE == edges().MODEL_SERVICE, "the edge step and the source must name one service")
    for label, mutate in (
        ("sampling on", lambda c: c["source"]["sourceConfig"]["config"].update(generateSampleData=True)),
        ("profiler type", lambda c: c["source"]["sourceConfig"]["config"].update(type="Profiler")),
        ("processor", lambda c: c.update(processor={"type": "orm-profiler", "config": {}})),
    ):
        broken = json.loads(json.dumps(config))
        mutate(broken)
        require(ingest.violations(broken), f"mutant not refused: {label}")


def the_host_ingest_runs_the_mlflow_source_and_then_the_edges() -> None:
    from scripts.infrastructure import openmetadata_host_ingest as host_ingest

    require = base().require
    seen: list[dict[str, Any]] = []
    world = World(models=("hai_model_HAI_1",))
    world.catalog_all_tables()
    with tempfile.TemporaryDirectory() as folder:
        args, env = base().host_ingest_environment(Path(folder))
        env.update(environment())
        args = [*args[:-2], "mlflow", "mlmodel_edges"]
        with base().quiet() as (out, err):
            code = host_ingest.main(args, env=env, workflow=seen.append, s3_client=base().FakeS3(base().chain_keys()), http=world)
    require(code == 0, (code, err.getvalue()))
    require(len(seen) == 1 and seen[0]["source"]["type"] == "mlflow", f"one MLflow workflow expected: {seen}")
    shown = seen[0]["source"]["serviceConnection"]["config"]
    require(shown["trackingUri"] == MLFLOW_URI == shown["registryUri"] and "${" not in json.dumps(seen[0]), shown)
    require(seen[0]["workflowConfig"]["openMetadataServerConfig"]["hostPort"] == base().HOST_API, "the workflow must aim at the persistent catalog")
    require(len(world.edge_set) == len(feature_tables()), "the edge step must follow the source")
    secrets_absent(out.getvalue(), err.getvalue())
    failing = World(models=("hai_model_HAI_1",), grants="none")
    with tempfile.TemporaryDirectory() as folder:
        args, env = base().host_ingest_environment(Path(folder))
        env.update(environment())
        with base().quiet():
            code = host_ingest.main(
                [*args[:-2], "mlflow", "mlmodel_edges"], env=env, workflow=lambda config: None, s3_client=base().FakeS3(base().chain_keys()), http=failing
            )
    require(code == 1, f"a failing edge step fails the job: {code}")


CASES = [
    tables_are_named_by_the_mirror_config,
    one_edge_is_drawn_from_each_feature_table_to_each_registered_model,
    the_edge_request_is_the_probed_one_and_never_a_patch,
    a_rerun_and_a_second_ingest_keep_the_same_edges,
    a_reader_without_the_wildcard_grants_fails_visibly,
    a_missing_login_or_a_server_refusal_fails_without_edges,
    a_registered_model_that_the_catalog_lacks_stops_the_run,
    no_registered_model_yet_is_reported_and_draws_nothing,
    a_table_the_catalog_lacks_stops_the_run_before_any_edge,
    a_refused_edge_or_a_missing_read_back_fails_without_secrets,
    the_catalog_and_registry_addresses_are_checked_before_any_call,
    the_mlflow_source_config_is_metadata_only_and_holds_no_login,
    the_host_ingest_runs_the_mlflow_source_and_then_the_edges,
]
