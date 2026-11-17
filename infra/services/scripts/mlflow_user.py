"""MLflow user one-shot of the host: make sure the model API's MLflow user exists with its stored login and may read the served models.

It is the command of the ``mlflow_user`` task (MLflow image, ``python -I -c``, host network, its own task role). The task reads three Parameter Store
parameters through its role: the MLflow administrator password and the model API user name and credential. Then, as the administrator, it creates the
user when it does not exist, checks that the stored credential logs in (an existing user with another password is never overwritten and the step fails by
name), and grants the user READ, and only READ, on the registered model ``hai_model_<type>`` and the experiment ``hai_training_<type>`` of each served type.
A grant of another level is reported and kept. A model or experiment that does not exist yet is reported as pending: run the step again after a training
run. A login that is not stored yet skips the step. Every call has a timeout. Output names steps, users and models and never a password.
The routes are the MLflow 3.17 auth routes as the owner's secret block knew them and are unproven against the live server.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import re
import sys
import urllib.parse
from collections.abc import Mapping
from typing import Any

SECONDS = 30
TYPE = re.compile(r"HAI_[1-6]")
USER = re.compile(r"[a-z][a-z0-9_]{2,31}")
URL = re.compile(r"http://127\.0\.0\.1:[0-9]{4,5}")


class Refused(Exception):
    """A step failed; the message names it and never holds a password."""


def call(url: str, method: str, path: str, login: tuple[str, str], body: Mapping[str, Any] | None = None) -> tuple[int, Any]:
    """One bounded request with HTTP basic authentication; returns the status and the parsed JSON body (empty when there is none)."""
    parts = urllib.parse.urlsplit(url)
    token = base64.b64encode(f"{login[0]}:{login[1]}".encode()).decode()
    headers = {"Authorization": f"Basic {token}", "Content-Type": "application/json"}
    connection = http.client.HTTPConnection(parts.hostname or "", parts.port, timeout=SECONDS)
    try:
        connection.request(method, path, None if body is None else json.dumps(body), headers)
        reply = connection.getresponse()
        raw, status = reply.read(), reply.status
    except (OSError, http.client.HTTPException) as error:
        raise Refused(f"MLflow did not answer ({type(error).__name__})") from None
    finally:
        connection.close()
    try:
        return status, json.loads(raw) if raw else {}
    except ValueError:
        return status, {}


def parameter(ssm: Any, name: str) -> str | None:
    """The decrypted value of a parameter, or None when it does not exist."""
    try:
        return str(ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"])
    except Exception as error:
        if type(error).__name__ == "ParameterNotFound":
            return None
        raise


def ensure_user(url: str, admin: tuple[str, str], user: str, credential: str) -> None:
    """Create the user or check that it logs in with the stored credential."""
    status, _ = call(url, "GET", f"/api/2.0/mlflow/users/get?{urllib.parse.urlencode({'username': user})}", admin)
    if status == 404:
        status, _ = call(url, "POST", "/api/2.0/mlflow/users/create", admin, {"username": user, "password": credential})
        if status != 200:
            raise Refused(f"creating the user {user} failed (HTTP {status})")
        sys.stdout.write(f"mlflow_user: user created {user}\n")
    elif status != 200:
        raise Refused(f"reading the user {user} failed (HTTP {status})")
    else:
        sys.stdout.write(f"mlflow_user: user current {user}\n")
    status, _ = call(url, "GET", "/api/2.0/mlflow/registered-models/search?max_results=1", (user, credential))
    if status != 200:
        raise Refused(f"the login of {user} with the stored credential was refused (HTTP {status}); the password is not overwritten")


def ensure_read(url: str, admin: tuple[str, str], user: str, kind: str, identifier: str, label: str) -> None:
    """READ on one resource for the user: granted when there is none, kept when it is READ or stronger."""
    query = urllib.parse.urlencode({"username": user, "resource_type": kind, "resource_id": identifier})
    status, held = call(url, "GET", f"/api/3.0/mlflow/users/permissions/get?{query}", admin)
    if status != 200:
        raise Refused(f"reading the permission on {label} failed (HTTP {status})")
    have = str(held.get("permission", "")) if isinstance(held, dict) else ""
    if have == "READ":
        sys.stdout.write(f"mlflow_user: READ on {label} already\n")
    elif have not in ("", "NO_PERMISSIONS"):
        sys.stdout.write(f"mlflow_user: {have} on {label} kept\n")
    else:
        status, _ = call(
            url,
            "POST",
            "/api/3.0/mlflow/users/permissions/grant",
            admin,
            {"username": user, "resource_type": kind, "resource_id": identifier, "permission": "READ"},
        )
        if status != 200:
            raise Refused(f"granting READ on {label} failed (HTTP {status})")
        sys.stdout.write(f"mlflow_user: READ on {label} granted\n")


def run(environment: Mapping[str, str], ssm: Any) -> int:
    """Run the step once; returns 0 (also when it skips or something is pending) or 1 with the reason on standard error."""
    url = environment.get("MLFLOW_URL", "")
    types = environment.get("MODEL_API_HAI_TYPES", "HAI_1").split(",")
    if not URL.fullmatch(url) or not all(TYPE.fullmatch(item) for item in types):
        sys.stderr.write("mlflow_user refused: MLFLOW_URL or MODEL_API_HAI_TYPES has another form\n")
        return 1
    try:
        user = parameter(ssm, environment["USER_PARAMETER"])
        credential = parameter(ssm, environment["CREDENTIAL_PARAMETER"])
        if user is None or credential is None:
            sys.stdout.write("mlflow_user: the model API login is not stored yet; nothing to do\n")
            return 0
        password = parameter(ssm, environment["ADMIN_PASSWORD_PARAMETER"])
        if password is None or not USER.fullmatch(user):
            raise Refused("the administrator password is missing or the stored user name has another form")
        admin = (environment.get("MLFLOW_ADMIN_USERNAME", "admin"), password)
        status, _ = call(url, "GET", "/api/2.0/mlflow/experiments/search?max_results=1", admin)
        if status != 200:
            raise Refused(f"the administrator login failed (HTTP {status})")
        ensure_user(url, admin, user, credential)
        for item in types:
            model, experiment = f"hai_model_{item}", f"hai_training_{item}"
            status, _ = call(url, "GET", f"/api/2.0/mlflow/registered-models/get?{urllib.parse.urlencode({'name': model})}", admin)
            if status == 200:
                ensure_read(url, admin, user, "registered_model", model, f"registered model {model}")
            elif status == 404:
                sys.stdout.write(f"mlflow_user: registered model {model} pending: not registered yet\n")
            else:
                raise Refused(f"reading the registered model {model} failed (HTTP {status})")
            status, found = call(url, "GET", f"/api/2.0/mlflow/experiments/get-by-name?{urllib.parse.urlencode({'experiment_name': experiment})}", admin)
            identifier = str(((found or {}).get("experiment") or {}).get("experiment_id", "")) if status == 200 else ""
            if status == 200 and re.fullmatch(r"[0-9]+", identifier):
                ensure_read(url, admin, user, "experiment", identifier, f"experiment {experiment}")
            elif status == 404:
                sys.stdout.write(f"mlflow_user: experiment {experiment} pending: not created yet\n")
            else:
                raise Refused(f"reading the experiment {experiment} failed (HTTP {status})")
    except Refused as refusal:
        sys.stderr.write(f"mlflow_user refused: {refusal}\n")
        return 1
    sys.stdout.write("mlflow_user: ready\n")
    return 0


def main() -> int:
    import boto3

    return run(os.environ, boto3.client("ssm", region_name=os.environ.get("AWS_REGION")))


if __name__ == "__main__":
    sys.exit(main())
