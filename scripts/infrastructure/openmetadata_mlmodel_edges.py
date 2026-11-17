"""Draw the lineage edges from the feature tables to the registered models in the persistent OpenMetadata catalog, after the MLflow source ingested the models.

Runs in the ingestion image as the last source of ``openmetadata_host_ingest`` (``mlmodel_edges``). The connector ingests each registered model as an MlModel
entity named ``<service>.<registered model>`` but sets no feature sources, and the catalog refuses a patch of them (HTTP 400, probe of Nov 17 2026). The edge is
therefore drawn directly: ``PUT /api/v1/lineage`` from a table to the ``mlmodel``, which the probe showed is accepted, read back and kept by a second ingest.

Which edges: every registered model of the model contract (``data_contracts/training/model_contract.json``) is trained on the feature_matrix tables that the
feature lists of the registrable slots name (``config/training/feature_lists``). A table is named as OpenMetadata's Glue connector names it, by the one mapping
function of ``orchestration.ecs_lineage`` (the Glue database of the features layer comes from ``config/lakehouse/athena_mirror.json``, never from this file).

The login: the registry asks for a login on every request and grants nothing by default, so the step reads it as ``MLFLOW_TRACKING_USERNAME`` and
``MLFLOW_TRACKING_PASSWORD`` from the task's secrets and holds it in memory only. MLflow answers a reader without grants with an empty registry, which looks
like no model yet. The step therefore reads the reader's own permissions first and fails unless it may read every registered model, experiment and logged
model. Then the rules:

- a model that the registry holds but the catalog lacks (the connector read none of them) stops the run and is named;
- a table that the catalog lacks stops the run before any edge is drawn;
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
TRAINING = Path("config/training/models.json")
FEATURE_LISTS = Path("config/training/feature_lists")
MODEL_SERVICE = str(json.loads((ROOT / SOURCE_CONFIG).read_text())["source"]["serviceName"])
MLFLOW_ADDRESS = re.compile(r"http://([0-9]{1,3}(?:\.[0-9]{1,3}){3}):5100")
READER = "metadata_reader"
GRANT_KINDS = (
    "registered_model",
    "experiment",
    "logged_model",
)  # what the connector reads: the model list, the run of a version and the logged model's signature
READ_OR_MORE = frozenset({"READ", "USE", "EDIT", "MANAGE"})
DEADLINE_SECONDS = 600.0
EDGE_NOTE = "feature table that the registered model is trained on"
TRACKING_LOGIN = ("MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD")


class StepFailed(RuntimeError):
    """A rule or a request failed. The message names models, tables and status codes and holds no login."""


def registered_models() -> list[str]:
    """The registered model name of each HAI type, from the model contract."""
    return sorted(json.loads((ROOT / CONTRACT).read_text())["registered_models"].values())


def feature_tables() -> list[str]:
    """The feature tables of the feature lists of the registrable slots: each reference names ``<table>:<column>``."""
    slots = json.loads((ROOT / TRAINING).read_text())["slots"]
    tables: set[str] = set()
    for slot in json.loads((ROOT / CONTRACT).read_text())["registrable_slots"]:
        references = json.loads((ROOT / FEATURE_LISTS / slots[slot]["feature_list"]).read_text())["references"]
        tables.update(str(reference).split(":", 1)[0] for reference in references)
    return sorted(tables)


def table_fqns() -> dict[str, str]:
    """The catalog name of each feature table, as the Glue connector creates it."""
    from orchestration import ecs_lineage

    return {table: ecs_lineage.connector_fqn(ecs_lineage.dataset("features", table)) for table in feature_tables()}


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


def reader_may_read(permissions: Any) -> bool:
    """True when the user is an administrator or holds READ or more on every kind the connector reads, for every name (the ``*`` pattern)."""
    if not isinstance(permissions, dict):
        return False
    if permissions.get("is_admin") is True:
        return True
    granted = {
        item.get("resource_type")
        for item in permissions.get("permissions") or []
        if isinstance(item, dict) and item.get("resource_pattern") == "*" and item.get("permission") in READ_OR_MORE
    }
    return all(kind in granted for kind in GRANT_KINDS)


def existing_models(client: Client, registry: str, headers: Mapping[str, str]) -> list[str]:
    """The contract's registered models that the registry holds. A missing one is not an error: its type has no champion yet."""
    status, permissions = client.call("GET", f"{registry}/api/3.0/mlflow/users/current/permissions", headers)
    if status != 200:
        raise StepFailed(f"the registry refused the login: HTTP {status} on the permissions of the reader")
    if not reader_may_read(permissions):
        raise StepFailed(
            f"the registry login is not {READER} with READ on every registered model, experiment and logged model (resource_pattern *): "
            "such a reader sees an empty registry, so the step stops instead of reading that as no model yet"
        )
    found = []
    for name in registered_models():
        status, _ = client.call("GET", f"{registry}/api/2.0/mlflow/registered-models/get?{urllib.parse.urlencode({'name': name})}", headers)
        if status == 200:
            found.append(name)
        elif status != 404:
            raise StepFailed(f"the registry answered HTTP {status} for the registered model {name}")
    return found


def catalog_id(client: Client, api: str, headers: Mapping[str, str], kind: str, fqn: str) -> str | None:
    status, entity = client.call("GET", f"{api}/v1/{kind}/name/{urllib.parse.quote(fqn, safe='')}", headers)
    if status == 404:
        return None
    if status != 200 or not isinstance(entity, dict) or not entity.get("id"):
        raise StepFailed(f"the catalog answered HTTP {status} for the {kind} entity {fqn}")
    return str(entity["id"])


def draw(client: Client, api: str, headers: Mapping[str, str], models: list[str]) -> int:
    """Check every model and table first, then draw each edge and read it back. Returns the number of edges."""
    model_ids = {name: catalog_id(client, api, headers, "mlmodels", f"{MODEL_SERVICE}.{name}") for name in models}
    lacking = [name for name, found in model_ids.items() if found is None]
    if lacking:
        raise StepFailed(f"the registry holds {', '.join(lacking)} but the catalog has no MlModel of it: the MLflow source read none of them")
    names = table_fqns()
    table_ids = {table: catalog_id(client, api, headers, "tables", fqn) for table, fqn in names.items()}
    absent = [names[table] for table, found in table_ids.items() if found is None]
    if absent:
        raise StepFailed(f"the catalog has no table {', '.join(absent)}: no edge was drawn")
    count = 0
    for name in models:
        for table, table_id in table_ids.items():
            edge = {"fromEntity": {"id": table_id, "type": "table"}, "toEntity": {"id": model_ids[name], "type": "mlmodel"}, "description": EDGE_NOTE}
            status, _ = client.call("PUT", f"{api}/v1/lineage", headers, {"edge": edge})
            if status != 200:
                raise StepFailed(f"the catalog refused the edge from {names[table]} to {name}: HTTP {status}")
            count += 1
        status, lineage = client.call(
            "GET", f"{api}/v1/lineage/mlmodel/name/{urllib.parse.quote(f'{MODEL_SERVICE}.{name}', safe='')}?upstreamDepth=1&downstreamDepth=0", headers
        )
        shown = {edge.get("fromEntity") for edge in (lineage or {}).get("upstreamEdges", [])} if status == 200 and isinstance(lineage, dict) else set()
        if not set(table_ids.values()) <= shown:
            raise StepFailed(f"the lineage of {name} does not show all {len(table_ids)} edges when it is read back (HTTP {status})")
    return count


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
        if not models:
            sys.stdout.write("openmetadata_mlmodel_edges: no registered model yet, so no edge is drawn\n")
            return 0
        count = draw(client, api, {"Authorization": f"Bearer {token}"}, models)
    except StepFailed as error:
        sys.stderr.write(f"openmetadata_mlmodel_edges: {error}\n")
        return 1
    waiting = [name for name in registered_models() if name not in models]
    note = f"; no registered model yet: {', '.join(waiting)}" if waiting else ""
    sys.stdout.write(f"openmetadata_mlmodel_edges: {len(models)} models, {len(table_fqns())} tables, {count} edges drawn and read back{note}\n")
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
