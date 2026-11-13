"""Read native task credentials and explicit engine storage settings without loading an engine."""

from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlsplit

# ECS sets this in the containers of a job on the EC2 host, which read their credentials from the task provider. Any other value is no task.
TASK_ENVIRONMENTS = frozenset({"AWS_ECS_EC2"})
# A task reads its credentials from the native provider only, so any of these in the environment is refused.
FORBIDDEN_CREDENTIAL_VARIABLES = (
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


def refusal_code(error: BaseException, fallback: str) -> str:
    """The code of a refusal, read from its one plain string argument. The text conversion of the error is never called, because a subclass can write
    to a log or a stream when it converts, so only an argument that is a built-in string is taken, and anything else gives ``fallback``."""
    arguments = BaseException.__dict__["args"].__get__(error)
    code = arguments[0] if len(arguments) == 1 else None
    return code if type(code) is str and re.fullmatch(r"[a-z_]{1,64}", code) else fallback


def in_task() -> bool:
    """Whether this process runs in an ECS task on the EC2 host."""
    return os.environ.get("AWS_EXECUTION_ENV") in TASK_ENVIRONMENTS


class TemporaryStorageError(RuntimeError):
    """An incomplete task spill configuration, independent of controller modules."""


def validate_task_credentials(*, require_catalog: bool = True) -> bool:
    """Reject profile and static credentials when a native task provider is required."""
    if not in_task():
        return False
    if any(os.environ.get(name) for name in FORBIDDEN_CREDENTIAL_VARIABLES):
        raise RuntimeError("task_credential_source_refused")
    if re.fullmatch(r"/v2/credentials/[A-Za-z0-9_-]+", os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "")) is None:
        raise RuntimeError("task_credential_provider_missing")
    if require_catalog:
        try:
            endpoint = urlsplit(os.environ.get("POLARIS_URI", ""))
            parts = (endpoint.scheme, endpoint.hostname, endpoint.username, endpoint.password, endpoint.query, endpoint.fragment, endpoint.path)
        except ValueError:
            # The parser's message repeats the address, so the refusal carries its fixed reason only.
            raise RuntimeError("task_catalog_endpoint_refused") from None
        scheme, hostname, username, password, query, fragment, path = parts
        if scheme not in ("http", "https") or not hostname or username or password or query or fragment or path != "/api/catalog":
            raise RuntimeError("task_catalog_endpoint_refused")
    return True


def duckdb_temp_settings() -> dict[str, str]:
    """Read an explicit task spill directory and byte cap, preserving unset local defaults."""
    directory = os.environ.get("HAI_DUCKDB_TEMP_DIRECTORY", "")
    maximum = os.environ.get("HAI_DUCKDB_MAX_TEMP_BYTES", "")
    if not directory and not maximum and not in_task():
        return {}
    if not directory or not maximum.isdecimal() or int(maximum) <= 0 or not Path(directory).is_absolute() or ".." in Path(directory).parts:
        raise TemporaryStorageError("temporary_storage_settings_incomplete")
    return {"temp_directory": directory, "max_temp_directory_size": maximum + "B"}
