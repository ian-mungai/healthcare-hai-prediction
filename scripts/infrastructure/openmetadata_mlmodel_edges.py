"""Draw the lineage edges from the feature tables to the registered models in the persistent OpenMetadata catalog, after the MLflow source ingested the models.

Runs in the ingestion image as the last source of ``openmetadata_host_ingest`` (``mlmodel_edges``). The connector ingests each registered model as an MlModel
entity named ``<service>.<registered model>`` but sets no feature sources, and the catalog refuses a patch of them (HTTP 400, probe of Nov 17 2026). The edge is
therefore drawn directly: ``PUT /api/v1/lineage`` from a table to the ``mlmodel``, which the probe showed is accepted, read back and kept by a second ingest.

Which edges: a registered model is trained on its own model set, so it gets an edge only from the table of that set (``feature_matrix_m1`` to
``feature_matrix_m6``, the ``table`` of each entry of ``model_sets`` in ``data_contracts/features/feature_schema.json``), never from a union of tables. The set
comes from the tags of the version the connector ingests (the highest version number): ``feature_set`` (the set key, written by ``promotion_check``) and
``model`` (the slot, which the same file's ``list`` names for each set). Either one resolves the set and, when both are present, they must agree. A model whose
set cannot be resolved stops the run and is named. When the newest version moves to another set, the edge from the old set's table is deleted, so the lineage
always says what the ingested version was trained on; an edge from any other table is left alone. A table is named as OpenMetadata's Glue connector names it, by
the one mapping function of ``orchestration.ecs_lineage`` (the Glue database of the features layer comes from ``config/lakehouse/athena_mirror.json``).

The login: the registry asks for a login on every request and grants nothing by default, so the step reads it as ``MLFLOW_TRACKING_USERNAME`` and
``MLFLOW_TRACKING_PASSWORD`` from the task's secrets and holds it in memory only. MLflow answers a reader without grants with an empty registry, which looks
like no model yet. The step therefore reads the reader's own permissions first and fails unless it may read every registered model, experiment and logged
model. Then the rules:

- a model that the registry holds but the catalog lacks (the connector read none of them) stops the run and is named;
- a model whose set cannot be resolved stops the run before any edge is drawn;
- the table of a set that a model uses must be in the catalog, or the run stops before any edge is drawn;
- no registered model yet (no champion has been registered) is reported and is not a failure;
- every edge drawn is read back from the lineage of its model.

Exit codes: 0 done, 1 a rule or a request failed, 2 the variables or addresses were refused.
Output names models, tables and status codes, never a login or token.
"""

from __future__ import annotations

import base64
import ipaddress
import json
import os
import re
import sys
import time
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.infrastructure import openmetadata_host_ingest as host_ingest
from scripts.infrastructure.openmetadata_raw_files import Http, WriteFailed, http_request

ROOT = Path(__file__).resolve().parents[2]
SOURCE_CONFIG = Path("config/metadata/ingestion/mlflow.json")
CONTRACT = Path("data_contracts/training/model_contract.json")
FEATURE_SCHEMA = Path("data_contracts/features/feature_schema.json")
SET_TAG = "feature_set"  # the version tag that promotion_check writes
SLOT_TAG = "model"  # the version tag of the slot (data_contracts/training/model_contract.json version_tags)
MAX_PAGES = 20
MODEL_SERVICE = str(json.loads((ROOT / SOURCE_CONFIG).read_text())["source"]["serviceName"])
MLFLOW_ADDRESS = re.compile(r"http://([0-9]{1,3}(?:\.[0-9]{1,3}){3}):5100")
READER = "metadata_reader"
GRANT_KINDS = (
    "registered_model",
    "experiment",
    "logged_model",
)  # what the connector reads: the model list, the run of a version and the logged model's signature
READ_LEVELS = frozenset({"READ"})  # the only level the reader may hold, on any resource
# USE on the workspace is the right to create, USE on a resource the right to use it, EDIT and MANAGE the right to write.
ELEVATED_LEVELS = frozenset({"USE", "EDIT", "MANAGE"})
# The kinds whose effective permission is read through the registry, which adds the server's default_permission: the three the connector reads, then others.
EFFECTIVE_KINDS = (*GRANT_KINDS, "prompt", "scorer", "gateway_secret", "run", "trace")
QUIET_LEVELS = frozenset({"NO_PERMISSIONS", "READ", "DENY"})
DEADLINE_SECONDS = 600.0
EDGE_NOTE = "table of the model set that the newest version of the registered model is trained on"
TRACKING_LOGIN = ("MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD")


class StepFailed(RuntimeError):
    """A rule or a request failed. The message names models, tables and status codes and holds no login."""


def registered_models() -> list[str]:
    """The registered model name of each HAI type, from the model contract."""
    return sorted(json.loads((ROOT / CONTRACT).read_text())["registered_models"].values())


def model_sets() -> dict[str, dict[str, str]]:
    """Each model set key with its slot (``list``) and its table, from the feature schema."""
    sets = json.loads((ROOT / FEATURE_SCHEMA).read_text())["model_sets"]
    return {str(key): {"slot": str(spec["list"]), "table": str(spec["table"])} for key, spec in sets.items()}


def table_fqn(table: str) -> str:
    """The catalog name of a features table, as the Glue connector creates it."""
    from orchestration import ecs_lineage

    return ecs_lineage.connector_fqn(ecs_lineage.dataset("features", table))


def resolve_set(tags: Mapping[str, str]) -> str:
    """The model set a version was trained on, from its tags. Raises ``StepFailed`` with the reason when no set resolves or the tags disagree."""
    sets = model_sets()
    found: dict[str, str] = {}
    if SET_TAG in tags:
        if tags[SET_TAG] not in sets:
            raise StepFailed(f"its {SET_TAG} tag {tags[SET_TAG]!r} is not a model set")
        found[SET_TAG] = tags[SET_TAG]
    if SLOT_TAG in tags:
        owners = [key for key, spec in sets.items() if spec["slot"] == tags[SLOT_TAG]]
        if not owners:
            raise StepFailed(f"its {SLOT_TAG} tag {tags[SLOT_TAG]!r} is not the slot of a model set")
        found[SLOT_TAG] = owners[0]
    if not found:
        raise StepFailed(f"its newest version has neither a {SET_TAG} nor a {SLOT_TAG} tag")
    if len(set(found.values())) != 1:
        raise StepFailed(f"its {SET_TAG} tag names {found[SET_TAG]} but its {SLOT_TAG} tag names {found[SLOT_TAG]}")
    return next(iter(found.values()))


def registry_address_is_valid(uri: str) -> bool:
    """True only for http://<RFC 1918 IPv4 address>:5100, so the login can only go to the host's own registry."""
    match = MLFLOW_ADDRESS.fullmatch(uri)
    if match is None:
        return False
    try:
        host = ipaddress.IPv4Address(match.group(1))
    except ValueError:
        return False
    return any(host in network for network in host_ingest.PRIVATE_NETWORKS)


class Client:
    """Bounded requests of one run: every call checks the run's deadline and turns a transport failure into ``StepFailed`` by its class."""

    def __init__(self, http: Http, deadline: float) -> None:
        self.http = http
        self.deadline = deadline

    def call(self, method: str, url: str, headers: Mapping[str, str], body: Any = None) -> tuple[int, Any]:
        if time.monotonic() > self.deadline:
            raise StepFailed(f"the step passed its {DEADLINE_SECONDS:.0f} second deadline")
        data = None if body is None else json.dumps(body).encode()
        try:
            return self.http(method, url, {**headers, **({"Content-Type": "application/json"} if data else {})}, data)
        except WriteFailed as error:
            raise StepFailed(str(error)) from None


def elevated_grants(permissions: Mapping[str, Any]) -> list[str]:
    """Every grant of the login above READ, on any kind of resource and any name (the workspace included), worded as ``EDIT on registered_model *``."""
    return [
        f"{item.get('permission')} on {item.get('resource_type')} {item.get('resource_pattern')}"
        for item in permissions.get("permissions") or []
        if isinstance(item, dict) and item.get("permission") in ELEVATED_LEVELS
    ]


def reader_problem(permissions: Any) -> str | None:
    """Why the login is not the read-only reader, or None when it is: no administrator, nothing above READ anywhere, READ on every kind the connector reads."""
    if not isinstance(permissions, dict):
        return "its permissions cannot be read"
    if permissions.get("is_admin") is True:
        return "it is an administrator"
    elevated = elevated_grants(permissions)
    if elevated:
        return "it holds more than READ: " + ", ".join(elevated)
    granted = {
        item.get("resource_type")
        for item in permissions.get("permissions") or []
        if isinstance(item, dict) and item.get("resource_pattern") == "*" and item.get("permission") in READ_LEVELS
    }
    missing = [kind for kind in GRANT_KINDS if kind not in granted]
    return f"it lacks READ on every {', '.join(missing)} (resource_pattern *)" if missing else None


def server_problem(client: Client, registry: str, headers: Mapping[str, str]) -> str | None:
    """Why the registry itself does not keep the login read-only, or None. Grants are not the whole story: the server's ``default_permission`` floors every
    grant, and with workspaces off (the default of MLflow 3.17.2) every login may create experiments and registered models, which no grant can refuse."""
    for kind in EFFECTIVE_KINDS:
        query = urllib.parse.urlencode({"username": READER, "resource_type": kind, "resource_id": "*"})
        status, answer = client.call("GET", f"{registry}/api/3.0/mlflow/users/permissions/get?{query}", headers)
        if status != 200 or not isinstance(answer, dict):
            raise StepFailed(f"the registry answered HTTP {status} when the effective permission of {READER} on {kind} was read")
        level = str(answer.get("permission"))
        if level not in QUIET_LEVELS:
            return (
                f"its effective permission on {kind} is {level}: the registry's default_permission (or a grant) gives more than READ. "
                "Set default_permission = NO_PERMISSIONS in the registry's auth file (services/mlflow/start_server.py writes it) and restart the server"
            )
    status, _ = client.call("GET", f"{registry}/api/3.0/mlflow/workspaces", headers)
    if status == 503:
        return (
            "the registry runs without workspaces, so any login can create experiments and registered models whatever its grants (MLflow 3.17.2). "
            "Start the MLflow server with MLFLOW_ENABLE_WORKSPACES=true (the environment of the mlflow task in infra/services/mlflow.tf) and rerun"
        )
    if status != 200:
        raise StepFailed(f"the registry answered HTTP {status} when its workspace support was read")
    return None


def existing_models(client: Client, registry: str, headers: Mapping[str, str]) -> dict[str, str]:
    """The contract's registered models that the registry holds, each with its model set key. A missing one is not an error: its type has no champion yet."""
    status, permissions = client.call("GET", f"{registry}/api/3.0/mlflow/users/current/permissions", headers)
    if status != 200:
        raise StepFailed(f"the registry refused the login: HTTP {status} on the permissions of the reader")
    problem = reader_problem(permissions)
    if problem:
        raise StepFailed(
            f"the registry login is not the read-only {READER} ({problem}). A login without READ on everything sees an empty registry, which would read as "
            "no model yet, and the metadata job takes no right to change the registry"
        )
    problem = server_problem(client, registry, headers)
    if problem:
        raise StepFailed(f"the registry does not keep {READER} read-only: {problem}")
    found = {}
    unresolved = []
    for name in registered_models():
        status, _ = client.call("GET", f"{registry}/api/2.0/mlflow/registered-models/get?{urllib.parse.urlencode({'name': name})}", headers)
        if status == 404:
            continue
        if status != 200:
            raise StepFailed(f"the registry answered HTTP {status} for the registered model {name}")
        try:
            found[name] = resolve_set(newest_tags(client, registry, headers, name))
        except StepFailed as error:
            unresolved.append(f"{name}: {error}")
    if unresolved:
        raise StepFailed(f"the model set of {len(unresolved)} registered models cannot be resolved, so no edge was drawn: " + "; ".join(unresolved))
    return found


def outside_models(client: Client, registry: str, headers: Mapping[str, str]) -> list[str]:
    """The registered models of the registry that are not in the model contract. They get no edge and no failure, and the report names them."""
    contract = set(registered_models())
    outside: set[str] = set()
    token = ""
    for _ in range(MAX_PAGES):
        query = {"max_results": "100", **({"page_token": token} if token else {})}
        status, page = client.call("GET", f"{registry}/api/2.0/mlflow/registered-models/search?{urllib.parse.urlencode(query)}", headers)
        if status != 200 or not isinstance(page, dict):
            raise StepFailed(f"the registry answered HTTP {status} when its registered models were listed")
        outside.update(str(item["name"]) for item in page.get("registered_models") or [] if str(item["name"]) not in contract)
        token = str(page.get("next_page_token") or "")
        if not token:
            return sorted(outside)
    raise StepFailed(f"more than {MAX_PAGES} pages of registered models")


def newest_tags(client: Client, registry: str, headers: Mapping[str, str], name: str) -> dict[str, str]:
    """The tags of the highest-numbered version of a registered model, which is the version the MLflow source ingests."""
    newest: tuple[int, dict[str, str]] | None = None
    token = ""
    for _ in range(MAX_PAGES):
        query = {"filter": f"name='{name}'", "max_results": "200", **({"page_token": token} if token else {})}
        status, page = client.call("GET", f"{registry}/api/2.0/mlflow/model-versions/search?{urllib.parse.urlencode(query)}", headers)
        if status != 200 or not isinstance(page, dict):
            raise StepFailed(f"the registry answered HTTP {status} when the versions were listed")
        for version in page.get("model_versions") or []:
            number = int(version["version"])
            if newest is None or number > newest[0]:
                newest = (number, {str(tag["key"]): str(tag["value"]) for tag in version.get("tags") or []})
        token = str(page.get("next_page_token") or "")
        if not token:
            break
    else:
        raise StepFailed(f"more than {MAX_PAGES} pages of versions")
    if newest is None:
        raise StepFailed("it has no version")
    return newest[1]


def catalog_id(client: Client, api: str, headers: Mapping[str, str], kind: str, fqn: str) -> str | None:
    status, entity = client.call("GET", f"{api}/v1/{kind}/name/{urllib.parse.quote(fqn, safe='')}", headers)
    if status == 404:
        return None
    if status != 200 or not isinstance(entity, dict) or not entity.get("id"):
        raise StepFailed(f"the catalog answered HTTP {status} for the {kind} entity {fqn}")
    return str(entity["id"])


def lineage_sources(client: Client, api: str, headers: Mapping[str, str], model: str, model_id: str) -> set[str]:
    """The IDs of the entities with an edge into the model: the direct upstream edges only, whatever else the lineage answer holds."""
    fqn = urllib.parse.quote(f"{MODEL_SERVICE}.{model}", safe="")
    status, lineage = client.call("GET", f"{api}/v1/lineage/mlmodel/name/{fqn}?upstreamDepth=1&downstreamDepth=0", headers)
    if status != 200 or not isinstance(lineage, dict):
        raise StepFailed(f"the lineage of {model} cannot be read: HTTP {status}")
    return {str(edge.get("fromEntity")) for edge in lineage.get("upstreamEdges", []) if str(edge.get("toEntity")) == model_id}


def draw(client: Client, api: str, headers: Mapping[str, str], models: Mapping[str, str]) -> tuple[int, int]:
    """Check every model and table first, then draw each model's one edge, remove the edges of its other sets' tables and read the lineage back.

    Returns the number of edges drawn and the number of stale edges removed."""
    found_ids = {name: catalog_id(client, api, headers, "mlmodels", f"{MODEL_SERVICE}.{name}") for name in models}
    lacking = [name for name, found in found_ids.items() if found is None]
    if lacking:
        raise StepFailed(f"the registry holds {', '.join(lacking)} but the catalog has no MlModel of it: the MLflow source read none of them")
    model_ids = {name: str(found) for name, found in found_ids.items()}
    sets = model_sets()
    names = {key: table_fqn(spec["table"]) for key, spec in sets.items()}
    table_ids = {key: catalog_id(client, api, headers, "tables", fqn) for key, fqn in names.items()}
    absent = sorted(names[key] for key in set(models.values()) if table_ids[key] is None)
    if absent:
        raise StepFailed(f"the catalog has no table {', '.join(absent)}: no edge was drawn")
    count = stale = 0
    for name, key in models.items():
        edge = {"fromEntity": {"id": table_ids[key], "type": "table"}, "toEntity": {"id": model_ids[name], "type": "mlmodel"}, "description": EDGE_NOTE}
        status, _ = client.call("PUT", f"{api}/v1/lineage", headers, {"edge": edge})
        if status != 200:
            raise StepFailed(f"the catalog refused the edge from {names[key]} to {name}: HTTP {status}")
        count += 1
        shown = lineage_sources(client, api, headers, name, model_ids[name])
        if table_ids[key] not in shown:
            raise StepFailed(f"the lineage of {name} does not show the edge from {names[key]} when it is read back")
        removed = 0
        for other, other_id in table_ids.items():
            if other != key and other_id in shown:
                status, _ = client.call("DELETE", f"{api}/v1/lineage/table/{other_id}/mlmodel/{model_ids[name]}", headers)
                if status != 200:
                    raise StepFailed(f"the catalog refused to remove the stale edge from {names[other]} to {name}: HTTP {status}")
                removed += 1
        stale += removed
        if removed and any(other_id in lineage_sources(client, api, headers, name, model_ids[name]) for other, other_id in table_ids.items() if other != key):
            raise StepFailed(f"the lineage of {name} still shows an edge from a table of another model set after the removal")
    return count, stale


def run(api: str, token: str, environment: Mapping[str, str], http: Http = http_request) -> int:
    """The step against the catalog at ``api``. The registry address and login come from ``environment``."""
    missing = [name for name in ("MLFLOW_TRACKING_URI", *TRACKING_LOGIN) if not environment.get(name)]
    if missing:
        sys.stderr.write(f"openmetadata_mlmodel_edges: these variables are not set: {', '.join(missing)}\n")
        return 2
    registry = environment["MLFLOW_TRACKING_URI"]
    if not registry_address_is_valid(registry):
        sys.stderr.write(
            "openmetadata_mlmodel_edges: MLFLOW_TRACKING_URI must be http://<private IPv4 address>:5100, with a 10., 172.16 to 172.31 or 192.168. address\n"
        )
        return 2
    login = base64.b64encode(f"{environment[TRACKING_LOGIN[0]]}:{environment[TRACKING_LOGIN[1]]}".encode()).decode()
    client = Client(http, time.monotonic() + DEADLINE_SECONDS)
    try:
        models = existing_models(client, registry, {"Authorization": f"Basic {login}"})
        outside = outside_models(client, registry, {"Authorization": f"Basic {login}"})
        left = f"; outside the model contract, no lineage: {', '.join(outside)}" if outside else ""
        if not models:
            sys.stdout.write(f"openmetadata_mlmodel_edges: no registered model yet, so no edge is drawn{left}\n")
            return 0
        count, stale = draw(client, api, {"Authorization": f"Bearer {token}"}, models)
    except StepFailed as error:
        sys.stderr.write(f"openmetadata_mlmodel_edges: {error}\n")
        return 1
    waiting = [name for name in registered_models() if name not in models]
    note = f"; no registered model yet: {', '.join(waiting)}" if waiting else ""
    removed = f", {stale} stale edges removed" if stale else ""
    sys.stdout.write(f"openmetadata_mlmodel_edges: {len(models)} models, {count} edges drawn and read back{removed}{note}{left}\n")
    return 0


def main(argv: list[str], env: Mapping[str, str] | None = None, http: Http = http_request) -> int:
    environment = dict(os.environ if env is None else env)
    api, token = environment.get("OM_API", ""), environment.get("OM_INGESTION_TOKEN", "")
    if not host_ingest.catalog_address_is_valid(api) or not token or argv:
        sys.stderr.write(
            "openmetadata_mlmodel_edges: OM_API must be http://<private IPv4 address>:8585/api, OM_INGESTION_TOKEN must be set and no argument is taken\n"
        )
        return 2
    return run(api, token, environment, http)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
