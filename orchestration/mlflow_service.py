"""Closed local MLflow settings and network identities, with no service side effects."""

from __future__ import annotations

import base64
import hmac
import ipaddress
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

STACK = "mlflow"
OWNER = "stack:mlflow"
TEMPLATE = "mlflow_service"
COMPOSE = "services/mlflow/docker-compose.mlflow.yaml"
TRAINING_COMPOSE = "services/training/docker-compose.training.yaml"
SERVICES = ("mlflow_db", "mlflow")
PORT = 5100
START_SECONDS = 300.0
HEALTH_SECONDS = 120.0
RELEASE_SECONDS = 120.0
AUTH_ENV = "PGPASSWORD"
BACKEND_URI = "postgresql+psycopg2://mlflow@mlflow-db:5432/mlflow"
SETTINGS = frozenset({"HAI_MLFLOW_STATE", "HAI_MLFLOW_DB_MEMORY", "HAI_MLFLOW_MEMORY"})
COMMAND = [
    "--host",
    str(ipaddress.IPv4Address(0)),
    "--port",
    str(PORT),
    "--workers",
    "2",
    "--serve-artifacts",
    "--artifacts-destination",
    "/mlflow/artifacts",
    "--allowed-hosts",
    "mlflow:5100,127.0.0.1:5100,localhost:5100",
    "--cors-allowed-origins",
    "http://127.0.0.1:5100",
]


def network_names(registry_id: str) -> dict[str, str]:
    """Name the shared server's database and UI networks within one registry."""
    return {role: f"hai-{registry_id[:8]}-mlflow-{role}" for role in ("backend", "ui")}


def run_network(registry_id: str, run_id: str) -> str:
    """Name the isolated network of one run's MLflow clients."""
    return f"hai-{registry_id[:8]}-{run_id.lower()}-mlflow"


def settings_for(values: Mapping[str, str]) -> dict[str, str]:
    """Require closed state and memory settings without reading ambient variables."""
    selected = {name: value for name, value in values.items() if name.startswith("HAI_MLFLOW_")}
    if set(selected) != SETTINGS or not Path(selected["HAI_MLFLOW_STATE"]).is_absolute():
        raise ValueError("MLflow settings require an absolute state folder and both memory limits")
    for name in SETTINGS - {"HAI_MLFLOW_STATE"}:
        value = selected[name]
        if not re.fullmatch(r"[1-9][0-9]{0,18}", value) or int(value) > 2**63 - 1:
            raise ValueError("MLflow memory must be a bounded positive decimal integer")
    if int(selected["HAI_MLFLOW_MEMORY"]) < 1024**3 or int(selected["HAI_MLFLOW_DB_MEMORY"]) < 256 * 1024**2:
        raise ValueError("MLflow memory is below its declared floor")
    return selected


def fixture_password(key: bytes, registry_id: str) -> str:
    """Derive a fixture-only password from the launcher's private key so a restart retains database access."""
    return base64.urlsafe_b64encode(hmac.digest(key, f"mlflow:{registry_id}".encode(), "sha256")).decode().rstrip("=")


def validate_bindings(service: str, body: Mapping[str, Any]) -> None:
    """Refuse a database host port or a UI binding other than loopback port 5100."""
    host = body.get("HostConfig", {})
    bindings = host.get("PortBindings", {})
    expected = {"5100/tcp": [{"HostIp": "127.0.0.1", "HostPort": "5100"}]} if service == "mlflow" else {}
    if service not in SERVICES or bindings != expected:
        raise ValueError("MLflow publishes only its UI on 127.0.0.1:5100")
    if service == "mlflow":
        if body.get("User") != "10005:10005" or not host.get("ReadonlyRootfs") or host.get("CapDrop") != ["ALL"]:
            raise ValueError("MLflow server hardening differs from its closed contract")
        environment = dict(item.split("=", 1) for item in body.get("Env", []))
        if environment.get("MLFLOW_BACKEND_STORE_URI") != BACKEND_URI or not environment.get(AUTH_ENV):
            raise ValueError("MLflow requires PostgreSQL with its password only in the environment")
        if body.get("Entrypoint") != ["mlflow", "server"] or body.get("Cmd") != COMMAND:
            raise ValueError("MLflow startup command differs from its closed contract")
        if any(environment.get(name) != "true" for name in ("MLFLOW_DISABLE_TELEMETRY", "DO_NOT_TRACK")):
            raise ValueError("MLflow telemetry must be disabled")
