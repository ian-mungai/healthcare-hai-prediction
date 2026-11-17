"""Owner step: create the read-only MLflow login ``metadata_reader`` that the metadata job's MLflow source uses, and keep its password in Parameter Store.

The registry grants nothing by default (``default_permission`` NO_PERMISSIONS), so the OpenMetadata MLflow connector needs a login of its own. This step makes
one: the user ``metadata_reader`` with READ on every registered model, experiment and logged model (the ``*`` pattern, so a model registered later needs no new
grant, which MLflow 3.17.2 accepts). Its password is a generated SecureString parameter that only this step writes, with ``Overwrite`` off, so a rerun never
replaces it. The parameter is the source of truth: a user that carries another password is set back to the stored value.
No password is printed or written to a file.

Run by the owner with the administrator profile, after the IAM apply that lets the metadata job's execution role inject the parameter and with the registry
reached through the Session Manager port forward (the same one ``promote_owner`` uses)::

    MLFLOW_TRACKING_URI=http://localhost:5100 python -m scripts.infrastructure.mlflow_reader_setup

It reads the administrator password from its parameter in memory. A rerun that finds everything in place changes nothing. Exit codes: 0 done (or unchanged), 1 a
request or a parameter failed, 2 the address was refused.
"""

from __future__ import annotations

import base64
import os
import re
import secrets
import sys
import time
import urllib.parse
from collections.abc import Mapping
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from scripts.infrastructure.openmetadata_mlmodel_edges import DEADLINE_SECONDS, GRANT_KINDS, READER, Client, StepFailed, reader_may_read
from scripts.infrastructure.openmetadata_raw_files import Http, http_request

READER_PARAMETER = "/healthcare_hai_prediction/mlflow_metadata_reader_password"
ADMIN_PARAMETER = "/healthcare_hai_prediction/mlflow_admin_password"
LOOPBACK_REGISTRY = re.compile(r"http://(?:127\.0\.0\.1|localhost):[0-9]{2,5}")
ADMIN_USER = "admin"


def basic(user: str, secret: str) -> dict[str, str]:
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{secret}".encode()).decode()}


def read_parameter(ssm: Any, name: str) -> str | None:
    """The decrypted value, or None when the parameter does not exist. Any other failure is raised by its code only."""
    try:
        return str(ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"])
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code == "ParameterNotFound":
            return None
        raise StepFailed(f"the parameter {name} cannot be read: {code or 'ClientError'}") from None
    except BotoCoreError as error:
        raise StepFailed(f"the parameter {name} cannot be read: {type(error).__name__}") from None


def stored_reader_login(ssm: Any, name: str) -> tuple[str, bool]:
    """The reader's stored password and whether this call created it. A concurrent create is read back, never overwritten."""
    found = read_parameter(ssm, name)
    if found:
        return found, False
    value = secrets.token_urlsafe(32)
    try:
        ssm.put_parameter(Name=name, Value=value, Type="SecureString", Overwrite=False)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        again = read_parameter(ssm, name) if code == "ParameterAlreadyExists" else None
        if not again:
            raise StepFailed(f"the parameter {name} cannot be created: {code or 'ClientError'}") from None
        return again, False
    except BotoCoreError as error:
        raise StepFailed(f"the parameter {name} cannot be created: {type(error).__name__}") from None
    return value, True


def reader_grants(client: Client, registry: str, reader: Mapping[str, str]) -> tuple[int, set[str]]:
    status, permissions = client.call("GET", f"{registry}/api/3.0/mlflow/users/current/permissions", reader)
    granted: set[str] = set()
    if status == 200 and isinstance(permissions, dict):
        granted = {
            str(item.get("resource_type"))
            for item in permissions.get("permissions") or []
            if isinstance(item, dict) and item.get("resource_pattern") == "*" and item.get("permission") in ("READ", "USE", "EDIT", "MANAGE")
        }
    return status, granted


def ensure_reader(client: Client, registry: str, admin: Mapping[str, str], stored: str) -> list[str]:
    """Bring the user and its grants to the wanted state and return what changed."""
    changed: list[str] = []
    reader = basic(READER, stored)
    status, _ = client.call("GET", f"{registry}/api/2.0/mlflow/users/get?{urllib.parse.urlencode({'username': READER})}", admin)
    body = {"username": READER, "password": stored}
    if status == 404:
        status, _ = client.call("POST", f"{registry}/api/2.0/mlflow/users/create", admin, body)
        changed.append("user created")
    elif status == 200:
        status, _ = reader_grants(client, registry, reader)
        if status == 401:
            status, _ = client.call("PATCH", f"{registry}/api/2.0/mlflow/users/update-password", admin, body)
            changed.append("password set to the stored value")
        else:
            status = 200
    if status != 200:
        raise StepFailed(f"the registry answered HTTP {status} while the user {READER} was read, created or updated (an administrator login is needed)")
    status, granted = reader_grants(client, registry, reader)
    if status != 200:
        raise StepFailed(f"the registry answered HTTP {status} to the login of {READER}")
    for kind in GRANT_KINDS:
        if kind not in granted:
            status, _ = client.call(
                "POST",
                f"{registry}/api/3.0/mlflow/users/permissions/grant",
                admin,
                {"username": READER, "resource_type": kind, "resource_id": "*", "permission": "READ"},
            )
            if status != 200:
                raise StepFailed(f"the registry answered HTTP {status} when READ on every {kind} was granted")
            changed.append(f"READ on every {kind}")
    status, final = client.call("GET", f"{registry}/api/3.0/mlflow/users/current/permissions", reader)
    if status != 200 or not reader_may_read(final):
        raise StepFailed(f"the login of {READER} does not read every registered model, experiment and logged model after the setup (HTTP {status})")
    return changed


def main(argv: list[str], env: Mapping[str, str] | None = None, http: Http = http_request, ssm: Any = None) -> int:
    environment = dict(os.environ if env is None else env)
    registry = environment.get("MLFLOW_TRACKING_URI", "")
    if argv or not LOOPBACK_REGISTRY.fullmatch(registry):
        sys.stderr.write(
            "mlflow_reader_setup: MLFLOW_TRACKING_URI must be the port forward, http://localhost:<port> or http://127.0.0.1:<port>, and no argument is taken\n"
        )
        return 2
    if ssm is None:
        import boto3

        ssm = boto3.client("ssm", region_name=environment.get("AWS_REGION"))
    client = Client(http, time.monotonic() + DEADLINE_SECONDS)
    try:
        administrator = read_parameter(ssm, environment.get("ADMIN_PARAMETER", ADMIN_PARAMETER))
        if not administrator:
            raise StepFailed("the administrator password of the registry cannot be read from its parameter")
        stored, created = stored_reader_login(ssm, environment.get("READER_PARAMETER", READER_PARAMETER))
        changed = ([f"parameter {environment.get('READER_PARAMETER', READER_PARAMETER)} created"] if created else []) + ensure_reader(
            client, registry, basic(ADMIN_USER, administrator), stored
        )
    except StepFailed as error:
        sys.stderr.write(f"mlflow_reader_setup: {error}\n")
        return 1
    sys.stdout.write(f"mlflow_reader_setup: {'; '.join(changed) if changed else 'unchanged'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
