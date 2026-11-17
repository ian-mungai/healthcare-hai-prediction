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


ADMIN = ("admin", "Synthetic-Admin-2!")
ADMIN_PARAMETER = "/example/mlflow_admin_password"
READER_PARAMETER = "/example/mlflow_metadata_reader_login"
LOOPBACK = "http://127.0.0.1:5100"


class FakeParameters:
    """The two Parameter Store calls of the setup, in memory. A put without Overwrite refuses an existing name, as the service does."""

    def __init__(self, **values: str) -> None:
        self.values = {ADMIN_PARAMETER: ADMIN[1], **{name: value for name, value in values.items()}}
        self.puts: list[dict[str, Any]] = []

    def get_parameter(self, **kwargs: Any) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        base().require(kwargs.get("WithDecryption") is True, "a SecureString was read without decryption")
        if kwargs["Name"] not in self.values:
            raise ClientError({"Error": {"Code": "ParameterNotFound", "Message": "missing"}}, "GetParameter")
        return {"Parameter": {"Name": kwargs["Name"], "Value": self.values[kwargs["Name"]]}}

    def put_parameter(self, **kwargs: Any) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        self.puts.append({key: value for key, value in kwargs.items() if key != "Value"})
        if kwargs["Name"] in self.values and not kwargs.get("Overwrite"):
            raise ClientError({"Error": {"Code": "ParameterAlreadyExists", "Message": "exists"}}, "PutParameter")
        self.values[kwargs["Name"]] = kwargs["Value"]
        return {"Version": 1}


class Registry:
    """The user and permission calls of MLflow's basic-auth app that the setup makes, shaped on MLflow 3.17.2 (checked live in the probe)."""

    def __init__(self, reader_login: str | None = None, grants: tuple[str, ...] = ()) -> None:
        self.reader_login = reader_login
        self.granted = set(grants)
        self.calls: list[tuple[str, str]] = []
        self.fail_grant = False

    def __call__(self, method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> tuple[int, Any]:
        parts = urllib.parse.urlsplit(url)
        path = parts.path
        self.calls.append((method, path))
        document = json.loads(body) if body else {}
        sent = headers.get("Authorization", "")
        as_admin = sent == "Basic " + base64.b64encode(":".join(ADMIN).encode()).decode()
        as_reader = self.reader_login is not None and sent == "Basic " + base64.b64encode(f"{READER[0]}:{self.reader_login}".encode()).decode()
        if not (as_admin or as_reader):
            return 401, None
        if method == "GET" and path == "/api/3.0/mlflow/users/current/permissions":
            items = [{"permission": "READ", "resource_pattern": "*", "resource_type": kind} for kind in sorted(self.granted)]
            return 200, {"is_admin": as_admin, "permissions": [] if as_admin else items}
        if not as_admin:
            return 403, None
        if method == "GET" and path == "/api/2.0/mlflow/users/get":
            found = parse_user(parts.query) == READER[0] and self.reader_login is not None
            return (
                (200, {"user": {"username": READER[0], "is_admin": False}}) if found else (404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "no user"})
            )
        if method == "POST" and path == "/api/2.0/mlflow/users/create":
            self.reader_login = document["password"]
            return 200, {"user": {"username": document["username"]}}
        if method == "PATCH" and path == "/api/2.0/mlflow/users/update-password":
            self.reader_login = document["password"]
            return 200, {}
        if method == "POST" and path == "/api/3.0/mlflow/users/permissions/grant":
            if self.fail_grant:
                return 500, {"error_code": "INTERNAL_ERROR", "message": "boom"}
            if (document["permission"], document["resource_id"]) != ("READ", "*"):
                return 400, {"error_code": "INVALID_PARAMETER_VALUE", "message": "the setup grants READ on every name only"}
            self.granted.add(document["resource_type"])
            return 200, {}
        return 404, {"error_code": "ENDPOINT_NOT_FOUND", "message": path}


def parse_user(query: str) -> str:
    return urllib.parse.parse_qs(query).get("username", [""])[0]


def setup() -> Any:
    from scripts.infrastructure import mlflow_reader_setup

    return mlflow_reader_setup


def run_setup(registry: Callable[..., tuple[int, Any]], ssm: FakeParameters, **changes: str | None) -> tuple[int, str, str]:
    env = {"MLFLOW_TRACKING_URI": LOOPBACK, "READER_PARAMETER": READER_PARAMETER, "ADMIN_PARAMETER": ADMIN_PARAMETER}
    for name, value in changes.items():
        env.pop(name, None) if value is None else env.update({name: value})
    with base().quiet() as (out, err):
        code = setup().main([], env=env, http=registry, ssm=ssm)
    return code, out.getvalue(), err.getvalue()


def writes(registry: Registry) -> list[tuple[str, str]]:
    return [call for call in registry.calls if call[0] in ("POST", "PATCH")]


def values_absent(ssm: FakeParameters, *texts: str) -> None:
    for text in texts:
        base().require(not any(value in text for value in (ADMIN[1], ssm.values.get(READER_PARAMETER, "\0"))), "a password reached the output")


def the_setup_creates_the_reader_once_with_a_stored_password_and_wildcard_grants() -> None:
    require = base().require
    registry, ssm = Registry(), FakeParameters()
    code, out, err = run_setup(registry, ssm)
    require(code == 0, (code, out, err))
    stored = ssm.values[READER_PARAMETER]
    require(len(stored) >= 32 and registry.reader_login == stored, "the MLflow user must carry the stored value")
    require(
        ssm.puts == [{"Name": READER_PARAMETER, "Type": "SecureString", "Overwrite": False}], f"the parameter is created once and never overwritten: {ssm.puts}"
    )
    require(registry.granted == {"registered_model", "experiment", "logged_model"}, registry.granted)
    values_absent(ssm, out, err)


def a_second_setup_changes_nothing() -> None:
    require = base().require
    registry, ssm = Registry(), FakeParameters()
    require(run_setup(registry, ssm)[0] == 0, "first run")
    before, calls = dict(ssm.values), len(registry.calls)
    code, out, err = run_setup(registry, ssm)
    require(code == 0 and "unchanged" in out, (code, out, err))
    require(ssm.values == before and len(ssm.puts) == 1, "a rerun must not touch the parameter")
    require(len(writes(registry)) == 4 and len(registry.calls) > calls, f"only reads on the rerun: {registry.calls[calls:]}")


def a_user_with_another_password_is_set_to_the_stored_value() -> None:
    require = base().require
    registry, ssm = Registry(reader_login="Synthetic-Old-1!", grants=("registered_model",)), FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"})
    code, out, err = run_setup(registry, ssm)
    require(code == 0, (code, err))
    require(
        registry.reader_login == "Synthetic-Stored-1!" and ssm.values[READER_PARAMETER] == "Synthetic-Stored-1!" and not ssm.puts,
        "the parameter is the source of truth",
    )
    require(registry.granted == {"registered_model", "experiment", "logged_model"}, "only the missing grants are added")
    require(sum(1 for call in registry.calls if call[1].endswith("/permissions/grant")) == 2, registry.calls)
    values_absent(ssm, out, err)


def a_stored_value_without_a_user_creates_the_user_after_an_interrupted_run() -> None:
    require = base().require
    registry, ssm = Registry(), FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"})
    code, out, err = run_setup(registry, ssm)
    require(code == 0 and registry.reader_login == "Synthetic-Stored-1!" and not ssm.puts, (code, err))


def a_user_without_a_stored_value_is_given_a_new_stored_value() -> None:
    require = base().require
    registry, ssm = Registry(reader_login="Synthetic-Old-1!"), FakeParameters()
    code, out, err = run_setup(registry, ssm)
    require(code == 0 and registry.reader_login == ssm.values[READER_PARAMETER] != "Synthetic-Old-1!", (code, err))
    require(any(call[0] == "PATCH" for call in registry.calls), "an existing user's password is replaced with the new stored value")


def an_unreadable_admin_login_or_a_refused_grant_stops_with_a_cause() -> None:
    require = base().require
    registry, ssm = Registry(), FakeParameters()
    del ssm.values[ADMIN_PARAMETER]
    code, out, err = run_setup(registry, ssm)
    require(code == 1 and "administrator password" in err and not registry.calls and not ssm.puts, (code, err))
    wrong = Registry()
    code, out, err = run_setup(wrong, FakeParameters(**{ADMIN_PARAMETER: "Synthetic-Wrong-1!"}))
    require(code == 1 and "401" in err and not writes(wrong), (code, err))
    refused = Registry()
    refused.fail_grant = True
    code, out, err = run_setup(refused, FakeParameters())
    require(code == 1 and "500" in err, (code, err))


def the_setup_talks_only_to_a_loopback_registry() -> None:
    require = base().require
    for uri in ("http://10.0.1.20:5100", "https://127.0.0.1:5100", "http://203.0.113.5:5100", "http://127.0.0.1"):
        registry, ssm = Registry(), FakeParameters()
        code, out, err = run_setup(registry, ssm, MLFLOW_TRACKING_URI=uri)
        require(code == 2 and not registry.calls and not ssm.puts, (uri, code, err))
    code, out, err = run_setup(Registry(), FakeParameters(), MLFLOW_TRACKING_URI="http://localhost:5100")
    require(code == 0, f"the owner's port forward answers on localhost: {(code, err)}")


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
    the_setup_creates_the_reader_once_with_a_stored_password_and_wildcard_grants,
    a_second_setup_changes_nothing,
    a_user_with_another_password_is_set_to_the_stored_value,
    a_stored_value_without_a_user_creates_the_user_after_an_interrupted_run,
    a_user_without_a_stored_value_is_given_a_new_stored_value,
    an_unreadable_admin_login_or_a_refused_grant_stops_with_a_cause,
    the_setup_talks_only_to_a_loopback_registry,
]
