"""Read native task credentials and explicit engine storage settings without loading an engine."""

from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlsplit


class TemporaryStorageError(RuntimeError):
    """An incomplete task spill configuration, independent of controller modules."""


def validate_task_credentials(*, require_catalog: bool = True) -> bool:
    """Reject profile and static credentials when a native task provider is required."""
    if os.environ.get("AWS_EXECUTION_ENV") != "AWS_ECS_FARGATE":
        return False
    forbidden = (
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
    if any(os.environ.get(name) for name in forbidden):
        raise RuntimeError("task_credential_source_refused")
    if re.fullmatch(r"/v2/credentials/[A-Za-z0-9_-]+", os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "")) is None:
        raise RuntimeError("task_credential_provider_missing")
    if require_catalog:
        endpoint = urlsplit(os.environ.get("POLARIS_URI", ""))
        if (
            endpoint.scheme not in ("http", "https")
            or not endpoint.hostname
            or endpoint.username
            or endpoint.password
            or endpoint.query
            or endpoint.fragment
            or endpoint.path != "/api/catalog"
        ):
            raise RuntimeError("task_catalog_endpoint_refused")
    return True


def duckdb_temp_settings() -> dict[str, str]:
    """Read an explicit task spill directory and byte cap, preserving unset local defaults."""
    directory = os.environ.get("HAI_DUCKDB_TEMP_DIRECTORY", "")
    maximum = os.environ.get("HAI_DUCKDB_MAX_TEMP_BYTES", "")
    if not directory and not maximum and os.environ.get("AWS_EXECUTION_ENV") != "AWS_ECS_FARGATE":
        return {}
    if not directory or not maximum.isdecimal() or int(maximum) <= 0 or not Path(directory).is_absolute() or ".." in Path(directory).parts:
        raise TemporaryStorageError("temporary_storage_settings_incomplete")
    return {"temp_directory": directory, "max_temp_directory_size": maximum + "B"}
