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
import re
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
PRIORITY = {"DENY": -1, "NO_PERMISSIONS": 0, "READ": 1, "USE": 2, "EDIT": 3, "MANAGE": 4}
DISABLED = "Endpoint: /api/3.0/mlflow/workspaces disabled because the server is running without workspaces support."


def base() -> Any:
    from scripts.infrastructure import run_openmetadata_e2e

    return run_openmetadata_e2e


def edges() -> Any:
    from scripts.infrastructure import openmetadata_mlmodel_edges

    return openmetadata_mlmodel_edges


def model_sets() -> dict[str, dict[str, str]]:
    """The model sets of the feature schema (set key to its slot list and its table), read straight from the file, not through the code under test."""
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "data_contracts/features/feature_schema.json").read_text())["model_sets"]
    return {key: {"slot": spec["list"], "table": spec["table"]} for key, spec in schema.items()}


def set_tags(key: str) -> dict[str, str]:
    """The version tags a registered version carries for a model set: the set key and the slot (``model``)."""
    return {"feature_set": key, "model": model_sets()[key]["slot"]}


class World:
    """One MLflow server and one OpenMetadata server in memory. A call is ``world(method, url, headers, body)``; every call is logged."""

    def __init__(
        self,
        models: tuple[str, ...] | Mapping[str, list[dict[str, str]]] = (),
        ingested: tuple[str, ...] | None = None,
        grants: str = "reader",
        others: tuple[str, ...] = (),
        default_permission: str = "NO_PERMISSIONS",
        workspaces: bool = True,
    ) -> None:
        # Each registered model with its versions in order (version 1 first), each version as its tags. A plain name list gives one version of set m2.
        self.versions: dict[str, list[dict[str, str]]] = dict(models) if isinstance(models, Mapping) else {name: [set_tags("m2")] for name in models}
        self.mlflow_models = set(self.versions)
        self.others = set(others)  # registered models outside the model contract
        self.default_permission = default_permission  # the server's default_permission, which floors every positive grant
        self.workspaces = workspaces  # false: the workspace list answers 503, and any login can create experiments and registered models
        self.page_size = 200  # versions and models per page of the registry's search answers
        self.indirect_edges: set[tuple[str, str]] = set()  # edges between other entities, which a deeper lineage read shows beside the model's own
        # reader: wildcard READ on models, experiments and logged models; none: no grant at all; model_only: one model name only; use: USE on all three;
        # edit, manage_named, admin, workspace_use: an elevated login (EDIT on every model, MANAGE on one experiment, an administrator, USE on the workspace)
        self.grants = grants
        self.mlmodels = {f"{SERVICE}.{name}": f"model-{index}" for index, name in enumerate(sorted(self.mlflow_models if ingested is None else ingested))}
        self.tables: dict[str, str] = {}
        self.edge_set: set[tuple[str, str]] = set()
        self.calls: list[tuple[str, str]] = []
        self.fail_put = False
        self.fail_delete = False
        self.foreign_edges: set[tuple[str, str]] = set()
        self.hide_edges = False
        self.token = base().BEARER_VALUE

    def catalog_all_tables(self) -> None:
        for spec in model_sets().values():
            self.tables[self.fqn(spec["table"])] = f"table-{spec['table']}"

    @staticmethod
    def fqn(table: str) -> str:
        from orchestration import ecs_lineage

        return ecs_lineage.connector_fqn(ecs_lineage.dataset("features", table))

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
        if method == "DELETE" and path.startswith("/api/v1/lineage/table/"):
            _, _, _, _, _, source, _, target = path.split("/")
            if self.fail_delete:
                return 500, {"code": 500, "message": "boom"}
            self.edge_set.discard((source, target))
            return 200, {}
        if method == "GET" and path.startswith("/api/v1/lineage/mlmodel/name/"):
            fqn = path.rsplit("/", 1)[1]
            model = self.mlmodels[fqn]
            shown = self.edge_set | self.foreign_edges
            upstream = (
                []
                if self.hide_edges
                else [{"fromEntity": source, "toEntity": target} for source, target in sorted(shown) if target == model]
                + [{"fromEntity": source, "toEntity": target} for source, target in sorted(self.indirect_edges)]
            )
            return 200, {"entity": {"id": model, "type": "mlmodel", "fullyQualifiedName": fqn}, "nodes": [], "upstreamEdges": upstream, "downstreamEdges": []}
        return 404, {"code": 404, "message": f"no route {method} {path}"}

    def mlflow(self, method: str, path: str, query: dict[str, list[str]], headers: Mapping[str, str]) -> tuple[int, Any]:
        if headers.get("Authorization") != "Basic " + base64.b64encode(":".join(READER).encode()).decode():
            return 401, None
        if method == "GET" and path == "/api/3.0/mlflow/users/current/permissions":

            def item(kind: str, permission: str = "READ", pattern: str = "*") -> dict[str, Any]:
                return {
                    "permission": permission,
                    "resource_pattern": pattern,
                    "resource_type": kind,
                    "role_id": 2,
                    "role_name": "__user_2__",
                    "workspace": "default",
                }

            kinds = ("registered_model", "experiment", "logged_model")
            wildcard = [item(kind) for kind in kinds]
            grants = {
                "reader": wildcard,
                "none": [],
                "model_only": [item("registered_model", pattern="synthetic")],
                "no_experiment": wildcard[:1] + wildcard[2:],
                "use": [item(kind, "USE") for kind in kinds],
                "workspace_use": [*wildcard, item("workspace", "USE")],
                "edit": [item("registered_model", "EDIT"), *wildcard[1:]],
                "manage_named": [*wildcard, item("experiment", "MANAGE", "1")],
                "admin": [],
            }[self.grants]
            return 200, {"is_admin": self.grants == "admin", "permissions": grants}
        if method == "GET" and path == "/api/3.0/mlflow/users/permissions/get":
            kind = query["resource_type"][0]
            granted = {"registered_model", "experiment", "logged_model"} if self.grants in ("reader", "use", "edit", "manage_named", "workspace_use") else set()
            floor = self.default_permission
            level = "READ" if kind in granted and PRIORITY[floor] <= PRIORITY["READ"] else floor
            return 200, {"allowed": PRIORITY[level] >= PRIORITY["USE"], "permission": level}
        if method == "GET" and path == "/api/3.0/mlflow/workspaces":
            return (200, {}) if self.workspaces else (503, DISABLED)
        if method == "GET" and path == "/api/2.0/mlflow/registered-models/search":
            names = sorted(self.mlflow_models | self.others)
            start = int(query.get("page_token", ["0"])[0])
            page = names[start : start + self.page_size]
            more = start + self.page_size < len(names)
            return 200, {"registered_models": [{"name": name} for name in page], **({"next_page_token": str(start + self.page_size)} if more else {})}
        if method == "GET" and path == "/api/2.0/mlflow/registered-models/get":
            name = query["name"][0]
            if name in self.mlflow_models:
                return 200, {"registered_model": {"name": name}}
            return 404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": f"Registered Model with name={name} not found"}
        if method == "GET" and path == "/api/2.0/mlflow/model-versions/search":
            name = query["filter"][0].removeprefix("name='").removesuffix("'")
            ordered = sorted(enumerate(self.versions.get(name, []), start=1))  # oldest first: the newest version of many is on the last page
            start = int(query.get("page_token", ["0"])[0])
            found = [
                {
                    "name": name,
                    "version": str(number),
                    "run_id": f"run-{number}",
                    "status": "READY",
                    **({"tags": [{"key": k, "value": v} for k, v in tags.items()]} if tags else {}),
                }
                for number, tags in ordered[start : start + self.page_size]
            ]
            more = start + self.page_size < len(ordered)
            if more:
                return 200, {"model_versions": found, "next_page_token": str(start + self.page_size)}
            return 200, ({"model_versions": found} if found else {})
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
    asked = {url.rsplit("/", 1)[1] for method, url in world.calls if method == "GET" and "/api/v1/tables/name/" in url}
    wanted = f"glue_mirror.hai_lakehouse.{schema}.{model_sets()['m2']['table']}"
    require(
        wanted in asked and all(name.startswith(f"glue_mirror.hai_lakehouse.{schema}.") for name in asked),
        f"table names do not follow the mirror config: {asked}",
    )
    # A renamed mirror database (the plain-names change) is followed with no code change: the names come from the mapping function only.
    plain = ecs_lineage.Connector("glue_mirror", "hai_lakehouse", {**ecs_lineage.connector().schemas, "features": "features"})
    renamed = World(models=("hai_model_HAI_1",))
    with mock.patch.object(ecs_lineage, "connector", return_value=plain):
        renamed.catalog_all_tables()
        code, out, err = run(renamed)
    require(code == 0, (code, out, err))
    require(
        f"/glue_mirror.hai_lakehouse.features.{model_sets()['m2']['table']}" in " ".join(url for _, url in renamed.calls),
        "a renamed mirror database is not followed",
    )
    source = (Path(__file__).resolve().parent / "openmetadata_mlmodel_edges.py").read_text()
    require("hai_mirror" not in source and "hai_lakehouse" not in source, "the edge step must not spell a mirror database or table prefix")


def each_model_gets_edges_only_from_the_table_of_its_own_model_set() -> None:
    """The union of the six matrix tables is gone: a model trained on set m2 has one edge, from the m2 table, and no other model's table reaches it."""
    require = base().require
    trained = {"hai_model_HAI_1": "m1", "hai_model_HAI_2": "m2", "hai_model_HAI_4": "m4"}
    world = World(models={name: [set_tags(key)] for name, key in trained.items()})
    world.catalog_all_tables()
    code, out, err = run(world)
    require(code == 0, (code, out, err))
    expected = {(f"table-{model_sets()[key]['table']}", world.mlmodels[f"{SERVICE}.{name}"]) for name, key in trained.items()}
    require(world.edge_set == expected, f"edges differ from the one table of each model's set: {sorted(world.edge_set)}")
    require(len(world.edge_set) == len(trained), "one edge per model: no union of tables")
    require("3 edges" in out, out)
    require("hai_model_HAI_3" in out and "no registered model yet" in out, f"the types without a registered model are reported: {out}")
    secrets_absent(out, err)


def a_model_whose_set_cannot_be_resolved_fails_naming_it() -> None:
    require = base().require
    good = {"hai_model_HAI_1": [set_tags("m2")]}
    unresolved: dict[str, list[dict[str, str]]] = {
        "no tags": [{}],
        "no versions": [],
        "unknown set": [{"feature_set": "m9", "model": "m9_other"}],
        "unknown slot only": [{"model": "m9_other"}],
        "set and slot disagree": [{"feature_set": "m2", "model": model_sets()["m4"]["slot"]}],
        "newest version untagged": [set_tags("m2"), {}],
    }
    for label, versions in unresolved.items():
        world = World(models={**good, "hai_model_HAI_2": versions})
        world.catalog_all_tables()
        code, out, err = run(world)
        require(code == 1 and "hai_model_HAI_2" in err and "hai_model_HAI_1" not in err, (label, code, err))
        require(not world.edge_set and not any(call[0] in ("PUT", "DELETE") for call in world.calls), f"{label}: a run that cannot resolve a set draws nothing")
        secrets_absent(out, err)
    only_slot = World(models={"hai_model_HAI_1": [{"model": model_sets()["m4"]["slot"]}]})
    only_slot.catalog_all_tables()
    code, out, err = run(only_slot)
    require(code == 0 and only_slot.edge_set == {(f"table-{model_sets()['m4']['table']}", "model-0")}, f"a slot alone resolves its set: {(code, err)}")


def the_newest_version_decides_the_set_and_a_changed_set_replaces_the_edge() -> None:
    require = base().require
    world = World(models={"hai_model_HAI_1": [set_tags("m2"), set_tags("m4")]})
    world.catalog_all_tables()
    foreign = ("table-another-source", "model-0")
    world.foreign_edges.add(foreign)
    code, out, err = run(world)
    require(code == 0 and world.edge_set == {(f"table-{model_sets()['m4']['table']}", "model-0")}, (code, err, world.edge_set))
    world.versions["hai_model_HAI_1"].append(set_tags("m1"))  # a newer version trained on another set
    code, out, err = run(world)
    require(code == 0 and world.edge_set == {(f"table-{model_sets()['m1']['table']}", "model-0")}, f"the old edge must go: {(code, err, world.edge_set)}")
    require(foreign in world.foreign_edges and any(call[0] == "DELETE" for call in world.calls), "only the model-set tables' stale edges are removed")
    world.versions["hai_model_HAI_1"].append(set_tags("m2"))
    world.fail_delete = True
    code, out, err = run(world)
    require(code == 1 and "500" in err and "feature_matrix_m1" in err, f"a refused removal fails and names the table: {(code, err)}")
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
    require(code == 0 and world.edge_set == first and len(first) == 1, (code, out, err))
    world.hide_edges = False
    require(all(call[0] != "PATCH" for call in world.calls), "a repeat must not patch")


def an_indirect_upstream_edge_is_not_a_stale_edge_of_the_model() -> None:
    """With ``m1 -> foreign table -> model`` beside ``m2 -> model``, a lineage read also shows ``m1 -> foreign``, which is not an edge into the model."""
    require = base().require
    world = World(models=("hai_model_HAI_1",))
    world.catalog_all_tables()
    first = model_sets()["m1"]["table"]
    world.foreign_edges.add(("table-foreign", "model-0"))
    world.indirect_edges.add((f"table-{first}", "table-foreign"))
    code, out, err = run(world)
    require(code == 0, (code, out, err))
    require(not any(call[0] == "DELETE" for call in world.calls), f"an edge that does not exist was deleted: {[c for c in world.calls if c[0] == 'DELETE']}")
    require(world.edge_set == {(f"table-{model_sets()['m2']['table']}", "model-0")} and ("table-foreign", "model-0") in world.foreign_edges, world.edge_set)
    lineage_reads = [url for method, url in world.calls if method == "GET" and "/api/v1/lineage/" in url]
    require(lineage_reads and all("upstreamDepth=1&" in url for url in lineage_reads), f"the step asks for the direct edges only: {lineage_reads}")
    # The same holds when a model's own stale edge and an indirect edge are both there.
    world.versions["hai_model_HAI_1"].append(set_tags("m1"))
    code, out, err = run(world)
    require(
        code == 0 and world.edge_set == {(f"table-{first}", "model-0")}, f"the stale edge of the model goes and nothing else: {(code, err, world.edge_set)}"
    )


def the_newest_of_many_versions_decides_across_pages_and_past_nine() -> None:
    """Versions 1 to 8 are of set m2, version 9 of m4 and version 10 of m1, on pages of four. The newest is 10 (not "9" by text order) on the last page."""
    require = base().require
    versions = [set_tags("m2")] * 8 + [set_tags("m4"), set_tags("m1")]
    world = World(models={"hai_model_HAI_1": versions})
    world.page_size = 4
    world.catalog_all_tables()
    code, out, err = run(world)
    require(code == 0, (code, out, err))
    require(
        world.edge_set == {(f"table-{model_sets()['m1']['table']}", "model-0")}, f"the newest version, number 10 on the third page, decides: {world.edge_set}"
    )
    pages = [url for method, url in world.calls if method == "GET" and "model-versions/search" in url]
    require(len(pages) == 3, f"every page of versions is read: {pages}")
    short = World(models={"hai_model_HAI_1": [set_tags("m2"), set_tags("m4")]})
    short.catalog_all_tables()
    code, out, err = run(short)
    require(code == 0 and short.edge_set == {(f"table-{model_sets()['m4']['table']}", "model-0")}, (code, err))
    endless = World(models={"hai_model_HAI_1": [set_tags("m2")] * 30})
    endless.page_size = 1
    endless.catalog_all_tables()
    code, out, err = run(endless)
    require(code == 1 and "pages" in err and not endless.edge_set, f"a listing that never ends fails at its bound: {(code, err)}")


def models_outside_the_contract_get_no_edges_and_are_reported_by_name() -> None:
    require = base().require
    world = World(models=("hai_model_HAI_1",), others=("scratch_model_b", "scratch_model_a", "scratch_model_c"))
    world.page_size = 2
    world.catalog_all_tables()
    code, out, err = run(world)
    require(code == 0 and len(world.edge_set) == 1, (code, out, err))
    require(
        "outside the model contract, no lineage: scratch_model_a, scratch_model_b, scratch_model_c" in out, f"the models outside the contract are named: {out}"
    )
    require(
        "no registered model yet" in out and "scratch_model" not in out.split("outside the model contract")[0],
        f"they are not reported as 'no registered model yet': {out}",
    )
    touched = [url for _, url in world.calls if "scratch_model" in urllib.parse.unquote(url)]
    require(not touched, f"nothing is read or drawn for them: {touched}")
    only = World(others=("scratch_model_a",))
    only.catalog_all_tables()
    code, out, err = run(only)
    require(code == 0 and not only.edge_set and "outside the model contract, no lineage: scratch_model_a" in out, (code, out, err))
    require(not any(call[0] in ("PUT", "DELETE") for call in only.calls), "no edge for a registry that holds only other models")


def a_reader_without_the_wildcard_grants_or_with_write_privilege_fails_visibly() -> None:
    require = base().require
    for grants in ("none", "model_only", "no_experiment"):
        world = World(models=("hai_model_HAI_1",), grants=grants)
        world.catalog_all_tables()
        code, out, err = run(world)
        require(code == 1 and "metadata_reader" in err and "READ" in err, (grants, code, err))
        require(not world.edge_set and not any(call[0] == "PUT" for call in world.calls), "no edge may be drawn on an unreadable registry")
        secrets_absent(out, err)
    for grants, named in (
        ("edit", "EDIT"),
        ("manage_named", "MANAGE"),
        ("admin", "administrator"),
        ("use", "USE on registered_model *"),
        ("workspace_use", "USE on workspace *"),
    ):
        elevated = World(models=("hai_model_HAI_1",), grants=grants)
        elevated.catalog_all_tables()
        code, out, err = run(elevated)
        require(code == 1 and "metadata_reader" in err and named in err, (grants, code, err))
        require(len(elevated.calls) == 1 and not elevated.edge_set, f"an elevated login stops the run before any other call: {elevated.calls}")
        secrets_absent(out, err)


def a_default_permission_above_read_or_a_registry_without_workspaces_is_refused() -> None:
    """The server's ``default_permission`` floors every grant, so a default above READ makes the reader more than a reader. With workspaces off any login can
    create experiments and registered models (MLflow 3.17.2 ``_can_create_in_workspace``), which no grant can prevent."""
    require = base().require
    for default in ("USE", "EDIT", "MANAGE"):
        world = World(models=("hai_model_HAI_1",), default_permission=default)
        world.catalog_all_tables()
        code, out, err = run(world)
        require(code == 1 and "default_permission" in err and default in err and "metadata_reader" in err, (default, code, err))
        require(
            not world.edge_set and not any(call[0] in ("PUT", "DELETE") or "registered-models" in call[1] for call in world.calls),
            f"{default}: nothing is read or drawn",
        )
        secrets_absent(out, err)
    for default in ("NO_PERMISSIONS", "READ"):
        world = World(models=("hai_model_HAI_1",), default_permission=default)
        world.catalog_all_tables()
        code, out, err = run(world)
        require(code == 0 and len(world.edge_set) == 1, f"a default of {default} gives no more than READ: {(code, err)}")
    open_registry = World(models=("hai_model_HAI_1",), workspaces=False)
    open_registry.catalog_all_tables()
    code, out, err = run(open_registry)
    require(
        code == 1 and "MLFLOW_ENABLE_WORKSPACES" in err and "create" in err,
        f"a registry that lets any login create is refused with the exact step: {(code, err)}",
    )
    require(not open_registry.edge_set and not any(call[0] == "PUT" for call in open_registry.calls), "nothing is drawn")


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
    gone = world.fqn(model_sets()["m2"]["table"])
    del world.tables[gone]
    code, out, err = run(world)
    require(code == 1 and gone in err and not world.edge_set, (code, err))
    require(not any(call[0] == "PUT" for call in world.calls), "an edge was drawn although a table is absent")
    other = World(models=("hai_model_HAI_1",))
    other.catalog_all_tables()
    del other.tables[other.fqn(model_sets()["m3"]["table"])]
    code, out, err = run(other)
    require(code == 0 and len(other.edge_set) == 1, f"a table of a set that no model uses may be absent: {(code, err)}")


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
    require(len(world.edge_set) == 1, "the edge step must follow the source")
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
    """The user, role and permission calls of MLflow's basic-auth app that the setup makes, shaped on MLflow 3.17.2 (checked live, see the probe evidence).

    A grant is ``(resource type, pattern)`` to ``(level, role name)``. A grant row that exists is refused on a second grant, as MLflow does. The per-user grant
    and revoke calls refuse the ``workspace`` type with 400 and the role call removes any row, as the real server does. ``default_permission`` floors every
    positive grant and is the effective level of a type with none; ``workspaces`` false answers the workspace list with 503."""

    def __init__(
        self,
        reader_login: str | None = None,
        grants: tuple[str, ...] = (),
        extra: Mapping[tuple[str, str], tuple[str, str]] | None = None,
        administrator: bool = False,
        default_permission: str = "NO_PERMISSIONS",
        workspaces: bool = True,
        reader_id: int = 2,
        other_users: Mapping[int, str] | None = None,
        shared_with: Mapping[str, tuple[int, ...]] | None = None,
    ) -> None:
        self.reader_login = reader_login
        self.reader_id = reader_id
        self.other_users = dict(other_users or {})  # user ID to name of the other users of the registry
        self.shared_with = dict(shared_with or {})  # role name to the IDs of the other users that hold it
        self.rows: dict[tuple[str, str], tuple[str, str]] = {(kind, "*"): ("READ", f"__user_{reader_id}__") for kind in grants}
        self.rows.update(extra or {})
        self.row_ids: dict[tuple[str, str], int] = {}
        self.administrator = administrator
        self.default_permission = default_permission
        self.workspaces = workspaces
        self.calls: list[tuple[str, str]] = []
        self.fail_grant = False
        self.fail_remove = False
        self.ignore_remove = False  # answers 200 and keeps the grant
        self.fail_holders = False  # the list of the holders of a role answers 500
        self.hide_user_id = False  # the user read answers without an ID

    @property
    def granted(self) -> set[str]:
        return {kind for (kind, pattern), (level, _) in self.rows.items() if pattern == "*" and level == "READ"}

    def identifier(self, key: tuple[str, str]) -> int:
        return self.row_ids.setdefault(key, 3 + len(self.row_ids))

    def role_id(self, role: str) -> int:
        """A user's own role has the ID of the user, as in the live server (role 3 is ``__user_3__``); another role has an ID from 10 up."""
        found = re.fullmatch(r"__user_([0-9]+)__", role)
        return int(found.group(1)) if found else 10 + sorted({name for _, name in self.rows.values() if not name.startswith("__user_")}).index(role)

    def holders(self, role: str) -> list[int]:
        """The IDs of the users that hold a role: the owner of a synthetic role, the reader (it holds each role of its rows) and the users it is shared with."""
        found = re.fullmatch(r"__user_([0-9]+)__", role)
        return sorted({self.reader_id, *([int(found.group(1))] if found else []), *self.shared_with.get(role, ())})

    def effective(self, kind: str) -> str:
        granted = [level for (row_kind, pattern), (level, _) in self.rows.items() if row_kind == kind and pattern == "*"]
        best = max(granted, key=PRIORITY.__getitem__, default=None)
        floor = self.default_permission
        return best if best is not None and PRIORITY[best] >= PRIORITY[floor] else floor

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
            items = [
                {"permission": level, "resource_pattern": pattern, "resource_type": kind, "role_name": role}
                for (kind, pattern), (level, role) in sorted(self.rows.items())
            ]
            return 200, {"is_admin": as_admin or self.administrator, "permissions": [] if as_admin else items}
        if method == "GET" and path == "/api/3.0/mlflow/users/permissions/get":
            kind = urllib.parse.parse_qs(parts.query)["resource_type"][0]
            if kind == "workspace":
                return 400, {
                    "error_code": "INVALID_PARAMETER_VALUE",
                    "message": "resource_type 'workspace' is not supported by the per-user permission convenience APIs.",
                }
            level = self.effective(kind)
            return 200, {"allowed": PRIORITY[level] >= PRIORITY["USE"], "permission": level}
        if method == "GET" and path == "/api/3.0/mlflow/workspaces":
            return (200, {}) if self.workspaces else (503, DISABLED)
        if not as_admin:
            return 403, None
        if method == "GET" and path == "/api/2.0/mlflow/users/get":
            found = parse_user(parts.query) == READER[0] and self.reader_login is not None
            return (
                (200, {"user": {**({} if self.hide_user_id else {"id": self.reader_id}), "username": READER[0], "is_admin": self.administrator}})
                if found
                else (404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "no user"})
            )
        if method == "GET" and path == "/api/3.0/mlflow/roles/users/list":
            if self.fail_holders:
                return 500, {"error_code": "INTERNAL_ERROR", "message": "boom"}
            role_id = int(urllib.parse.parse_qs(parts.query)["role_id"][0])
            role = next((name for _, name in self.rows.values() if self.role_id(name) == role_id), None)
            holders = self.holders(role) if role else []
            return 200, {"assignments": [{"id": 100 + user, "role_id": role_id, "user_id": user} for user in holders]}
        if method == "GET" and path == "/api/2.0/mlflow/users/list":
            users = {self.reader_id: READER[0], **self.other_users}
            return 200, {"users": [{"id": user, "username": name, "is_admin": False} for user, name in sorted(users.items())]}
        if method == "GET" and path == "/api/3.0/mlflow/users/roles/list":
            roles: dict[str, list[dict[str, Any]]] = {}
            for (kind, pattern), (level, role) in sorted(self.rows.items()):
                roles.setdefault(role, []).append(
                    {
                        "id": self.identifier((kind, pattern)),
                        "permission": level,
                        "resource_pattern": pattern,
                        "resource_type": kind,
                        "role_id": self.role_id(role),
                    }
                )
            return 200, {"roles": [{"id": self.role_id(role), "name": role, "permissions": rows, "workspace": "default"} for role, rows in roles.items()]}
        if method == "DELETE" and path == "/api/3.0/mlflow/roles/permissions/remove":
            if self.fail_remove:
                return 500, {"error_code": "INTERNAL_ERROR", "message": "boom"}
            if not self.ignore_remove:
                for key in list(self.rows):
                    if self.identifier(key) == document["role_permission_id"]:
                        del self.rows[key]
            return 200, {}
        if method == "POST" and path == "/api/2.0/mlflow/users/create":
            self.reader_login = document["password"]
            return 200, {"user": {"username": document["username"]}}
        if method == "PATCH" and path == "/api/2.0/mlflow/users/update-password":
            self.reader_login = document["password"]
            return 200, {}
        if method == "PATCH" and path == "/api/2.0/mlflow/users/update-admin":
            self.administrator = bool(document["is_admin"])
            return 200, {}
        if (
            method == "POST"
            and path in ("/api/3.0/mlflow/users/permissions/revoke", "/api/3.0/mlflow/users/permissions/grant")
            and document["resource_type"] == "workspace"
        ):
            return 400, {
                "error_code": "INVALID_PARAMETER_VALUE",
                "message": "resource_type 'workspace' is not supported by the per-user permission convenience APIs.",
            }
        if method == "POST" and path == "/api/3.0/mlflow/users/permissions/revoke":
            self.rows.pop((document["resource_type"], document["resource_id"]), None)
            return 200, {}
        if method == "POST" and path == "/api/3.0/mlflow/users/permissions/grant":
            if self.fail_grant:
                return 500, {"error_code": "INTERNAL_ERROR", "message": "boom"}
            if (document["permission"], document["resource_id"]) != ("READ", "*"):
                return 400, {"error_code": "INVALID_PARAMETER_VALUE", "message": "the setup grants READ on every name only"}
            if (document["resource_type"], "*") in self.rows:
                return 409, {"error_code": "RESOURCE_ALREADY_EXISTS", "message": "Permission already exists."}
            self.rows[(document["resource_type"], "*")] = ("READ", "__user_2__")
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


def the_setup_downgrades_an_elevated_reader_or_fails_naming_why() -> None:
    require = base().require
    every = ("registered_model", "experiment", "logged_model")
    for label, kwargs, said in (
        (
            "edit on every model",
            {"extra": {("registered_model", "*"): ("EDIT", "__user_2__")}, "grants": ("experiment", "logged_model")},
            "EDIT on registered_model *",
        ),
        (
            "use on every model",
            {"extra": {("registered_model", "*"): ("USE", "__user_2__")}, "grants": ("experiment", "logged_model")},
            "USE on registered_model *",
        ),
        ("manage on one experiment", {"extra": {("experiment", "1"): ("MANAGE", "__user_2__")}, "grants": every}, "MANAGE on experiment 1"),
        ("manage on the workspace", {"extra": {("workspace", "*"): ("MANAGE", "__user_2__")}, "grants": every}, "MANAGE on workspace *"),
        ("use on the workspace", {"extra": {("workspace", "*"): ("USE", "__user_2__")}, "grants": every}, "USE on workspace *"),
        ("administrator", {"administrator": True, "grants": every}, "administrator rights removed"),
    ):
        registry, ssm = Registry(reader_login="Synthetic-Stored-1!", **kwargs), FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"})
        code, out, err = run_setup(registry, ssm)
        require(code == 0 and said in out, (label, code, out, err))
        require(
            {level for level, _ in registry.rows.values()} == {"READ"} and not registry.administrator and registry.granted == set(every),
            (label, registry.rows, registry.administrator),
        )
        require(
            not any(call[1].endswith("/permissions/revoke") for call in registry.calls),
            f"{label}: grants are removed through the role API, which accepts every type",
        )
        again = len(writes(registry))
        code, out, err = run_setup(registry, ssm)
        require(code == 0 and "unchanged" in out and len(writes(registry)) == again, f"{label}: a rerun changes nothing: {(code, out)}")
        values_absent(ssm, out, err)
    shared = Registry(reader_login="Synthetic-Stored-1!", grants=("experiment", "logged_model"), extra={("workspace", "*"): ("USE", "team_editors")})
    code, out, err = run_setup(shared, FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"}))
    require(code == 1 and "team_editors" in err and "roles/unassign" in err, f"a shared role is refused with the unassign call: {(code, err)}")
    require(
        not any(call[0] in ("DELETE", "PATCH") for call in shared.calls[-6:]) and shared.rows[("workspace", "*")] == ("USE", "team_editors"),
        "nothing was removed",
    )
    silent = Registry(reader_login="Synthetic-Stored-1!", grants=every, extra={("experiment", "1"): ("MANAGE", "__user_2__")})
    silent.ignore_remove = True
    code, out, err = run_setup(silent, FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"}))
    require(code == 1 and "not read-only after the setup" in err, f"a removal that changed nothing is caught by the final check: {(code, err)}")
    refused = Registry(reader_login="Synthetic-Stored-1!", grants=("experiment", "logged_model"), extra={("registered_model", "*"): ("MANAGE", "__user_2__")})
    refused.fail_remove = True
    code, out, err = run_setup(refused, FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"}))
    require(code == 1 and "500" in err and refused.rows[("registered_model", "*")][0] == "MANAGE", (code, err))


def the_setup_removes_only_grants_of_the_readers_own_role_and_refuses_any_other_role_first() -> None:
    """MLflow 3.17.2 lets an administrator assign another user's synthetic role, or a shared role, to the reader. A row of such a role belongs to its other
    holders too, so the setup refuses before it changes anything, names the role and gives the unassign call. The reader's own role is ``__user_<its ID>__``."""
    require = base().require
    every = ("registered_model", "experiment", "logged_model")
    login = FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"})

    def refused(label: str, registry: Registry, *said: str, announced: bool = True, parameters: FakeParameters | None = None) -> None:
        """The setup stops with exit 1 and has written nothing: no registry mutation of any kind (user, password, administrator flag, grant, role), no
        Parameter Store put, the reader's password and the parameter as they were."""
        ssm = FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"}) if parameters is None else parameters
        before = (dict(registry.rows), registry.reader_login, registry.administrator, dict(ssm.values))
        code, out, err = run_setup(registry, ssm)
        require(code == 1 and all(text in err for text in said), (label, code, err))
        require(registry.rows == before[0], f"{label}: the grant of the other holder is intact")
        require(
            not any(call[0] in ("POST", "PATCH", "DELETE") for call in registry.calls),
            f"{label}: no write of any kind reached the registry: {[call for call in registry.calls if call[0] != 'GET']}",
        )
        require(registry.reader_login == before[1] and registry.administrator == before[2], f"{label}: the reader's password and rights are as they were")
        require(not ssm.puts and ssm.values == before[3], f"{label}: nothing was written to Parameter Store")
        require(not announced or "Nothing was changed" in err, f"{label}: the owner is told nothing was changed: {err}")
        values_absent(ssm, out, err)

    # Another user's synthetic role, borrowed by an assignment, with an EDIT grant of that user, while the reader is also an administrator.
    borrowed = Registry(
        reader_login="Synthetic-Stored-1!",
        grants=every,
        extra={("experiment", "7"): ("EDIT", "__user_3__")},
        administrator=True,
        other_users={3: "other_user"},
    )
    refused("borrowed synthetic role", borrowed, "__user_3__", 'DELETE /api/3.0/mlflow/roles/unassign {"username": "metadata_reader", "role_id": 3}')
    require(borrowed.administrator, "the administrator flag is untouched while the setup refuses")
    # The same borrowed role with the two repairs that used to run first: a password that differs from the stored one, and no stored parameter at all.
    mismatch = Registry(reader_login="Synthetic-Old-1!", grants=every, extra={("experiment", "7"): ("EDIT", "__user_3__")}, other_users={3: "other_user"})
    refused("borrowed role and a password mismatch", mismatch, "__user_3__", "roles/unassign")
    require(mismatch.reader_login == "Synthetic-Old-1!", "the old login still works after the refusal")
    absent = Registry(reader_login="Synthetic-Old-1!", grants=every, extra={("experiment", "7"): ("EDIT", "__user_3__")}, other_users={3: "other_user"})
    no_parameter = FakeParameters()
    refused("borrowed role and no stored parameter", absent, "__user_3__", "roles/unassign", parameters=no_parameter)
    require(READER_PARAMETER not in no_parameter.values and not no_parameter.puts, "the reader parameter was not created")
    # A shared role, with a read-only grant: any other role of the reader is refused, whatever it grants.
    refused(
        "shared role with READ only",
        Registry(reader_login="Synthetic-Stored-1!", grants=every, extra={("run", "*"): ("READ", "team_readers")}),
        "team_readers",
        "roles/unassign",
    )
    refused(
        "shared role with EDIT",
        Registry(reader_login="Synthetic-Stored-1!", grants=every, extra={("run", "*"): ("EDIT", "team_editors")}),
        "team_editors",
        "roles/unassign",
    )
    # The reader's own role, also held by another user: its rows are not the reader's alone.
    shared_own = Registry(
        reader_login="Synthetic-Stored-1!",
        grants=every,
        extra={("experiment", "7"): ("MANAGE", "__user_2__")},
        other_users={5: "other_user"},
        shared_with={"__user_2__": (5,)},
    )
    refused(
        "the readers own role held by another user", shared_own, "__user_2__", 'DELETE /api/3.0/mlflow/roles/unassign {"username": "other_user", "role_id": 2}'
    )
    # The binding is to the reader's user ID, not to the name pattern: with ID 7 the role ``__user_2__`` is another user's.
    foreign_pattern = Registry(
        reader_login="Synthetic-Stored-1!", reader_id=7, grants=(), extra={("experiment", "7"): ("EDIT", "__user_2__")}, other_users={2: "other_user"}
    )
    refused("a synthetic role of another ID", foreign_pattern, "__user_2__", '"role_id": 2')
    # A holders list or a user read that fails stops the setup with nothing changed, and a role with other holders cannot slip through.
    for label, flag in (("holders list", "fail_holders"), ("user without an ID", "hide_user_id")):
        broken = Registry(reader_login="Synthetic-Stored-1!", grants=every, extra={("experiment", "7"): ("EDIT", "__user_2__")})
        setattr(broken, flag, True)
        refused(label, broken, "HTTP", announced=label == "holders list")
    # The reader's own role, with ID 7, is still cleaned and the rerun changes nothing.
    own = Registry(reader_login="Synthetic-Stored-1!", reader_id=7, grants=every, extra={("experiment", "7"): ("EDIT", "__user_7__")})
    code, out, err = run_setup(own, login)
    require(code == 0 and "EDIT on experiment 7 removed" in out and {level for level, _ in own.rows.values()} == {"READ"}, (code, out, err))
    again = len(writes(own))
    code, out, err = run_setup(own, login)
    require(code == 0 and "unchanged" in out and len(writes(own)) == again, (code, out, err))


def the_setup_refuses_a_default_permission_above_read_and_a_registry_that_lets_any_login_create() -> None:
    require = base().require
    for default in ("USE", "EDIT"):
        registry = Registry(reader_login="Synthetic-Stored-1!", grants=("registered_model", "experiment", "logged_model"), default_permission=default)
        code, out, err = run_setup(registry, FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"}))
        require(code == 1 and "default_permission" in err and default in err, (default, code, err))
        require("default_permission = NO_PERMISSIONS" in err, f"the owner is told the setting to change: {err}")
    open_registry = Registry(reader_login="Synthetic-Stored-1!", grants=("registered_model", "experiment", "logged_model"), workspaces=False)
    code, out, err = run_setup(open_registry, FakeParameters(**{READER_PARAMETER: "Synthetic-Stored-1!"}))
    require(code == 1 and "MLFLOW_ENABLE_WORKSPACES" in err and "create" in err, (code, err))


CASES = [
    tables_are_named_by_the_mirror_config,
    each_model_gets_edges_only_from_the_table_of_its_own_model_set,
    a_model_whose_set_cannot_be_resolved_fails_naming_it,
    the_newest_version_decides_the_set_and_a_changed_set_replaces_the_edge,
    an_indirect_upstream_edge_is_not_a_stale_edge_of_the_model,
    the_newest_of_many_versions_decides_across_pages_and_past_nine,
    models_outside_the_contract_get_no_edges_and_are_reported_by_name,
    the_edge_request_is_the_probed_one_and_never_a_patch,
    a_rerun_and_a_second_ingest_keep_the_same_edges,
    a_reader_without_the_wildcard_grants_or_with_write_privilege_fails_visibly,
    a_default_permission_above_read_or_a_registry_without_workspaces_is_refused,
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
    the_setup_downgrades_an_elevated_reader_or_fails_naming_why,
    the_setup_removes_only_grants_of_the_readers_own_role_and_refuses_any_other_role_first,
    the_setup_refuses_a_default_permission_above_read_and_a_registry_that_lets_any_login_create,
]
