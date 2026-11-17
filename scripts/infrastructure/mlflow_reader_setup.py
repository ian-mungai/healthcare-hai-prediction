"""Owner step: create the read-only MLflow login ``metadata_reader`` that the metadata job's MLflow source uses, and keep its password in Parameter Store.

The registry grants nothing by default (``default_permission`` NO_PERMISSIONS), so the OpenMetadata MLflow connector needs a login of its own. This step makes
one: the user ``metadata_reader`` with READ on every registered model, experiment and logged model (the ``*`` pattern, so a model registered later needs no new
grant, which MLflow 3.17.2 accepts) and nothing more. A user that is an administrator or holds an EDIT or MANAGE grant of its own is brought back to READ.
A write grant from a shared role stops the step with the role named. The password is a generated SecureString parameter that only this step writes, with
``Overwrite`` off, so a rerun never replaces it. The parameter is the source of truth: a user that carries another password is set back to the stored value.
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

from scripts.infrastructure.openmetadata_mlmodel_edges import (
    DEADLINE_SECONDS,
    ELEVATED_LEVELS,
    GRANT_KINDS,
    READ_LEVELS,
    READER,
    Client,
    StepFailed,
    reader_problem,
    server_problem,
)
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


def reader_permissions(client: Client, registry: str, reader: Mapping[str, str]) -> Mapping[str, Any]:
    status, permissions = client.call("GET", f"{registry}/api/3.0/mlflow/users/current/permissions", reader)
    if status != 200 or not isinstance(permissions, dict):
        raise StepFailed(f"the registry answered HTTP {status} to the login of {READER}")
    return permissions


def unassign(username: str, role_id: Any) -> str:
    """The administrator call that takes a role away from a user."""
    return f'DELETE /api/3.0/mlflow/roles/unassign {{"username": "{username}", "role_id": {role_id}}}'


def reader_identity(client: Client, registry: str, admin: Mapping[str, str]) -> int:
    """The user ID of the reader, from the registry. MLflow names the reader's own role ``__user_<this ID>__``."""
    status, answer = client.call("GET", f"{registry}/api/2.0/mlflow/users/get?{urllib.parse.urlencode({'username': READER})}", admin)
    user = answer.get("user") if isinstance(answer, dict) else None
    if status != 200 or not isinstance(user, dict) or not isinstance(user.get("id"), int):
        raise StepFailed(f"the registry answered HTTP {status} without a user ID when {READER} was read")
    return int(user["id"])


def user_names(client: Client, registry: str, admin: Mapping[str, str]) -> dict[int, str]:
    """The names of the users by ID, for a message. An answer that cannot be read leaves the IDs to stand for the names."""
    status, answer = client.call("GET", f"{registry}/api/2.0/mlflow/users/list", admin)
    users = answer.get("users") if status == 200 and isinstance(answer, dict) else None
    return {int(user["id"]): str(user["username"]) for user in users or [] if isinstance(user, dict) and "id" in user and "username" in user}


def refuse_roles_other_than_its_own(client: Client, registry: str, admin: Mapping[str, str], roles: list[dict[str, Any]], user_id: int) -> None:
    """Stop, with nothing changed, unless every role of the reader is its own synthetic role and no other user holds that role.

    A role row is removed by its ID for every holder of the role. MLflow 3.17.2 lets an administrator assign another user's synthetic role
    (``__user_<their ID>__``) or a shared role to the reader, and a name that fits the synthetic pattern does not show whose role it is. Only the role named for
    the reader's own ID counts as its own, and only while the reader alone holds it. Any other role is refused with the administrator call that unassigns it."""
    own = f"__user_{user_id}__"
    for role in roles:
        if role.get("name") != own:
            raise StepFailed(
                f"the role {role.get('name')} (id {role.get('id')}) is assigned to {READER} but is not its own role {own}, so its grants may be other users'. "
                f"As the administrator unassign it: {unassign(READER, role.get('id'))} then rerun. Nothing was changed"
            )
        query = urllib.parse.urlencode({"role_id": role["id"]})
        status, holders = client.call("GET", f"{registry}/api/3.0/mlflow/roles/users/list?{query}", admin)
        assigned = {item.get("user_id") for item in holders.get("assignments") or []} if status == 200 and isinstance(holders, dict) else None
        if assigned is None:
            raise StepFailed(f"the registry answered HTTP {status} when the holders of the role {own} were listed. Nothing was changed")
        others = sorted(user for user in assigned if user != user_id)
        if others:
            names = user_names(client, registry, admin)
            calls = "; ".join(unassign(names.get(user, str(user)), role["id"]) for user in others)
            raise StepFailed(
                f"the role {own} of {READER} is also held by other users, so its grants are not the reader's alone. As the administrator: {calls}, then rerun. "
                "Nothing was changed"
            )


def downgrade(client: Client, registry: str, admin: Mapping[str, str], permissions: Mapping[str, Any], user_id: int) -> list[str]:
    """Take administrator rights and every grant above READ away from the reader, on every kind of resource and on the workspace.

    The roles of the reader are checked first (``refuse_roles_other_than_its_own``), before any change. The grants are read from them with their IDs and
    removed by ID through the role API, which is the one path that accepts the ``workspace`` type (the per-user revoke call refuses it with 400)."""
    changed: list[str] = []
    status, listing = client.call("GET", f"{registry}/api/3.0/mlflow/users/roles/list?{urllib.parse.urlencode({'username': READER})}", admin)
    if status != 200 or not isinstance(listing, dict):
        raise StepFailed(f"the registry answered HTTP {status} when the roles of {READER} were listed")
    roles = [role for role in listing.get("roles") or [] if isinstance(role, dict)]
    refuse_roles_other_than_its_own(client, registry, admin, roles, user_id)
    if permissions.get("is_admin") is True:
        status, _ = client.call("PATCH", f"{registry}/api/2.0/mlflow/users/update-admin", admin, {"username": READER, "is_admin": False})
        if status != 200:
            raise StepFailed(f"the registry answered HTTP {status} when the administrator rights of {READER} were removed")
        changed.append("administrator rights removed")
    for role in roles:
        for item in role.get("permissions") or []:
            if item.get("permission") not in ELEVATED_LEVELS:
                continue
            label = f"{item['permission']} on {item['resource_type']} {item['resource_pattern']}"
            status, _ = client.call("DELETE", f"{registry}/api/3.0/mlflow/roles/permissions/remove", admin, {"role_permission_id": item["id"]})
            if status != 200:
                raise StepFailed(f"the registry answered HTTP {status} when the grant {label} of {READER} was removed")
            changed.append(f"{label} removed")
    return changed


def ensure_reader(client: Client, registry: str, admin: Mapping[str, str], stored: str) -> list[str]:
    """Bring the user to the read-only state (no administrator, no write grant, READ on all models, experiments and logged models); return what changed."""
    changed: list[str] = []
    reader = basic(READER, stored)
    status, _ = client.call("GET", f"{registry}/api/2.0/mlflow/users/get?{urllib.parse.urlencode({'username': READER})}", admin)
    body = {"username": READER, "password": stored}
    if status == 404:
        status, _ = client.call("POST", f"{registry}/api/2.0/mlflow/users/create", admin, body)
        changed.append("user created")
    elif status == 200:
        status, _ = client.call("GET", f"{registry}/api/3.0/mlflow/users/current/permissions", reader)
        if status == 401:
            status, _ = client.call("PATCH", f"{registry}/api/2.0/mlflow/users/update-password", admin, body)
            changed.append("password set to the stored value")
        else:
            status = 200
    if status != 200:
        raise StepFailed(f"the registry answered HTTP {status} while the user {READER} was read, created or updated (an administrator login is needed)")
    permissions = reader_permissions(client, registry, reader)
    lowered = downgrade(client, registry, admin, permissions, reader_identity(client, registry, admin))
    if lowered:
        changed.extend(lowered)
        permissions = reader_permissions(client, registry, reader)
    granted = {
        item.get("resource_type")
        for item in permissions.get("permissions") or []
        if isinstance(item, dict) and item.get("resource_pattern") == "*" and item.get("permission") in READ_LEVELS
    }
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
    problem = reader_problem(reader_permissions(client, registry, reader)) or server_problem(client, registry, reader)
    if problem:
        raise StepFailed(f"the login of {READER} is not read-only after the setup: {problem}")
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
