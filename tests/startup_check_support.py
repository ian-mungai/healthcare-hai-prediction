"""Stand-ins and environments shared by the host job startup check tests."""

from __future__ import annotations

import socket
import time
from pathlib import Path

import pytest

from scripts.infrastructure import startup_check

REGION = "us-west-2"
POLARIS_HOST = "192.0.2.10"
POLARIS_URI = f"http://{POLARIS_HOST}:8181/api/catalog"
MLFLOW_URI = "http://192.0.2.10:5000"
SERVICE_HOSTS = (f"s3.{REGION}.amazonaws.com", f"api.ecr.{REGION}.amazonaws.com", f"dkr.ecr.{REGION}.amazonaws.com")
FORBIDDEN = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
)


class Network:
    """Stands in for the socket connect. A host in ``refuse`` fails with its exception, loopback uses a real socket and every other host answers."""

    def __init__(self, refuse: dict[str, Exception] | None = None, delay: float = 0.0) -> None:
        self.refuse = refuse or {}
        self.delay = delay
        self.seen: list[tuple[str, int]] = []

    def __call__(self, host: str, port: int, timeout: float) -> None:
        self.seen.append((host, port))
        if self.delay:
            time.sleep(self.delay)
        if host in self.refuse:
            raise self.refuse[host]
        if host.startswith("127."):
            socket.create_connection((host, port), timeout=timeout).close()


def host_environment(monkeypatch: pytest.MonkeyPatch, execution: str | None = "AWS_ECS_EC2", polaris: str = POLARIS_URI, mlflow: str = "") -> None:
    """The environment of a job container on the host: ECS markers, the task credential provider, the region and the service addresses."""
    for name in FORBIDDEN:
        monkeypatch.delenv(name, raising=False)
    if execution is None:
        monkeypatch.delenv("AWS_EXECUTION_ENV", raising=False)
    else:
        monkeypatch.setenv("AWS_EXECUTION_ENV", execution)
    monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/00000000-0000-0000-0000-000000000000")
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", "http://169.254.170.2/v4/00000000")
    monkeypatch.setenv("AWS_REGION", REGION)
    monkeypatch.setenv("POLARIS_URI", polaris)
    monkeypatch.setenv("MLFLOW_TRACKING_URI", mlflow)
    monkeypatch.setenv("HAI_DUCKDB_TEMP_DIRECTORY", "/workspace/duckdb_spill")
    monkeypatch.setenv("HAI_DUCKDB_MAX_TEMP_BYTES", "1048576")


def install_network(monkeypatch: pytest.MonkeyPatch, network: Network) -> Network:
    monkeypatch.setattr(startup_check, "open_connection", network)
    return network


def profile_with_extensions(source: Path, target: Path, extensions: Path, names: tuple[str, ...] = ("iceberg", "httpfs", "aws")) -> Path:
    """Copy the tracked dbt profiles with the task extension directory moved under ``extensions``, and fill it with one file per extension."""
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.read_text().replace("/opt/duckdb/extensions", str(extensions)))
    folder = extensions / "v0.0.0" / "linux_arm64"
    folder.mkdir(parents=True, exist_ok=True)
    for name in names:
        (folder / f"{name}.duckdb_extension").write_bytes(b"x")
    return target
