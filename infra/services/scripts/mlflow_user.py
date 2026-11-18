"""The one check of the model API's MLflow account: its stored login and its READ grants. The ``mlflow_user`` task and the serving secrets tool both call it.

The ``mlflow_user`` task (MLflow image, ``python -I -c``, host network, its own task role) runs ``run`` after every host start. It reads three
Parameter Store parameters through its role: the MLflow administrator password and the model API user name and credential. The serving secrets tool
(``scripts/infrastructure/create_serving_secrets.py``) loads this file and calls ``ensure_account`` once it has created the secrets. Nothing else in
the project creates the user, checks its login or grants it a permission.

``ensure_account``, as the administrator, creates the user when it does not exist, then logs in as the user with the stored credential and refuses
when that fails (an existing user with another password is never overwritten and the step fails by name). It grants the user READ, and only READ, on
the registered model ``hai_model_<type>`` and the experiment ``hai_training_<type>`` of each served type. A grant of USE, EDIT or MANAGE is reported
and kept. A DENY, or a NO_PERMISSIONS row that a grant cannot replace, is a refusal: the account is never reported ready while it cannot read a served
object. A model or experiment that does not exist yet, or one in the trash, is reported as pending: run the step again after a training run. A login
that is not stored yet skips the step.

Every HTTP call, its connection, its headers and the reading of its body end at one absolute run deadline (``RUN_SECONDS`` from the start of the run):
a watchdog closes the socket at the deadline and every read checks the clock. Output names steps, users and models and never a password.

MLflow 3.17.2 routes, read from the wheel (``mlflow/server/auth/routes.py`` and ``__init__.py``): ``users/get`` and ``users/create``
(``/api/2.0/mlflow``, the create call needs a JSON body with ``username`` and ``password``), ``users/permissions/get`` and ``users/permissions/grant``
(``/api/3.0/mlflow``, the grant call inserts one row and answers ``RESOURCE_ALREADY_EXISTS`` for a user that already has a row, so it never changes a
permission), the registered model get, the experiment get-by-name (an experiment is named by its ID in a grant) and the registered model and
experiment searches that any logged-in user may call. The per-object permission routes under ``registered-models/permissions`` and
``experiments/permissions`` were removed in 3.17.2 and are never called. The routes are proven against the loopback stand-in
``scripts/infrastructure/mlflow_auth_stand_in.py`` only, not against a live server.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
from collections.abc import Mapping
from typing import Any

SECONDS = 30
RUN_SECONDS = 120
MAX_BYTES = 1024 * 1024
CHUNK_BYTES = 8192
TYPE = re.compile(r"HAI_[1-6]")
USER = re.compile(r"[a-z][a-z0-9_]{2,31}")
URL = re.compile(r"http://127\.0\.0\.1:[0-9]{4,5}")


class Refused(Exception):
    """A step failed; the message names it and never holds a password."""


def remaining(deadline: float, what: str) -> float:
    """The seconds left until the absolute deadline; a call past it is refused by name."""
    left = deadline - time.monotonic()
    if left <= 0:
        raise Refused(f"{what} did not finish before the run deadline")
    return left


def call(url: str, method: str, path: str, login: tuple[str, str], deadline: float, body: Mapping[str, Any] | None = None) -> tuple[int, Any]:
    """One request with HTTP basic authentication; returns the status and the parsed JSON body (empty when there is none).

    The connection, the request, the headers and every chunk of the body end at ``deadline`` (a ``time.monotonic`` value): the socket timeout is the time
    left, a watchdog shuts the socket at the deadline and the clock is checked between chunks. A reply of more than ``MAX_BYTES`` is refused.
    """
    what = f"{method} {path.partition('?')[0]}"
    parts = urllib.parse.urlsplit(url)
    token = base64.b64encode(f"{login[0]}:{login[1]}".encode()).decode()
    headers = {"Authorization": f"Basic {token}", "Content-Type": "application/json"}
    connection = http.client.HTTPConnection(parts.hostname or "", parts.port, timeout=min(SECONDS, remaining(deadline, what)))
    expired = threading.Event()
    watchdog = threading.Timer(remaining(deadline, what), shut, (connection, expired))
    watchdog.daemon = True
    watchdog.start()
    try:
        connection.connect()
        remaining(deadline, what)
        connection.request(method, path, None if body is None else json.dumps(body), headers)
        reply = connection.getresponse()
        status, chunks, size = reply.status, [], 0
        while True:
            reply_timeout(connection, deadline, what)
            piece = reply.read1(CHUNK_BYTES)
            if not piece:
                break
            chunks.append(piece)
            size += len(piece)
            if size > MAX_BYTES:
                raise Refused(f"{what} answered more than {MAX_BYTES} bytes")
        raw = b"".join(chunks)
    except (OSError, http.client.HTTPException) as error:
        if expired.is_set() or time.monotonic() >= deadline:
            raise Refused(f"{what} did not finish before the run deadline") from None
        raise Refused(f"MLflow did not answer ({type(error).__name__})") from None
    finally:
        watchdog.cancel()
        connection.close()
    try:
        return status, json.loads(raw) if raw else {}
    except ValueError:
        return status, {}


def reply_timeout(connection: http.client.HTTPConnection, deadline: float, what: str) -> None:
    """Limit the next read of the reply to the time left; a deadline already passed is refused."""
    left = remaining(deadline, what)
    if connection.sock is not None:
        connection.sock.settimeout(min(SECONDS, left))


def shut(connection: http.client.HTTPConnection, expired: threading.Event) -> None:
    """The watchdog: at the deadline close both directions of the socket so that a blocked read returns."""
    expired.set()
    sock = connection.sock
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            return


def parameter(ssm: Any, name: str) -> str | None:
    """The decrypted value of a parameter, or None when it does not exist."""
    try:
        return str(ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"])
    except Exception as error:
        if type(error).__name__ == "ParameterNotFound":
            return None
        raise


def ensure_user(url: str, admin: tuple[str, str], user: str, credential: str, deadline: float) -> None:
    """Create the user or check that it logs in with the stored credential."""
    status, _ = call(url, "GET", f"/api/2.0/mlflow/users/get?{urllib.parse.urlencode({'username': user})}", admin, deadline)
    if status == 404:
        status, _ = call(url, "POST", "/api/2.0/mlflow/users/create", admin, deadline, {"username": user, "password": credential})
        if status != 200:
            raise Refused(f"creating the user {user} failed (HTTP {status})")
        sys.stdout.write(f"mlflow_user: user created {user}\n")
    elif status != 200:
        raise Refused(f"reading the user {user} failed (HTTP {status})")
    else:
        sys.stdout.write(f"mlflow_user: user current {user}\n")
    status, _ = call(url, "GET", "/api/2.0/mlflow/registered-models/search?max_results=1", (user, credential), deadline)
    if status != 200:
        raise Refused(f"the login of {user} with the stored credential was refused (HTTP {status}); the password is not overwritten")


def ensure_read(url: str, admin: tuple[str, str], user: str, kind: str, identifier: str, label: str, deadline: float) -> None:
    """READ on one resource for the user: granted when there is none, kept when it is READ or stronger, refused when it is DENY."""
    query = urllib.parse.urlencode({"username": user, "resource_type": kind, "resource_id": identifier})
    status, held = call(url, "GET", f"/api/3.0/mlflow/users/permissions/get?{query}", admin, deadline)
    have = held.get("permission") if status == 200 and isinstance(held, dict) else None
    if not isinstance(have, str) or not have:
        raise Refused(f"reading the permission on {label} failed (HTTP {status})")
    if have == "READ":
        sys.stdout.write(f"mlflow_user: READ on {label} already\n")
    elif have == "DENY":
        raise Refused(f"{user} holds DENY on {label}, which blocks the model API; left unchanged")
    elif have != "NO_PERMISSIONS":
        sys.stdout.write(f"mlflow_user: {have} on {label} kept\n")
    else:
        body = {"username": user, "resource_type": kind, "resource_id": identifier, "permission": "READ"}
        status, _ = call(url, "POST", "/api/3.0/mlflow/users/permissions/grant", admin, deadline, body)
        if status != 200:
            raise Refused(f"granting READ on {label} failed (HTTP {status}); {user} holds NO_PERMISSIONS there")
        sys.stdout.write(f"mlflow_user: READ on {label} granted\n")


def ensure_account(url: str, admin: tuple[str, str], user: str, credential: str, types: tuple[str, ...], deadline: float) -> None:
    """Check the administrator login, the user and its stored login, then READ on each served model and experiment; refuse by name, else print ready.

    ``deadline`` is an absolute ``time.monotonic`` value that ends every request and every read of a body. Raises ``Refused``; a ready account is
    reported only after every check passed.
    """
    for item in types:
        if not TYPE.fullmatch(item):
            raise Refused(f"{item} is not a served HAI type (HAI_1 to HAI_6)")
    status, _ = call(url, "GET", "/api/2.0/mlflow/experiments/search?max_results=1", admin, deadline)
    if status != 200:
        raise Refused(f"the administrator login failed (HTTP {status})")
    ensure_user(url, admin, user, credential, deadline)
    for item in types:
        model, experiment = f"hai_model_{item}", f"hai_training_{item}"
        status, _ = call(url, "GET", f"/api/2.0/mlflow/registered-models/get?{urllib.parse.urlencode({'name': model})}", admin, deadline)
        if status == 200:
            ensure_read(url, admin, user, "registered_model", model, f"registered model {model}", deadline)
        elif status == 404:
            sys.stdout.write(f"mlflow_user: registered model {model} pending: not registered yet\n")
        else:
            raise Refused(f"reading the registered model {model} failed (HTTP {status})")
        status, found = call(url, "GET", f"/api/2.0/mlflow/experiments/get-by-name?{urllib.parse.urlencode({'experiment_name': experiment})}", admin, deadline)
        record = ((found or {}).get("experiment") or {}) if status == 200 and isinstance(found, dict) else {}
        identifier = str(record.get("experiment_id", ""))
        if status == 200 and re.fullmatch(r"[0-9]+", identifier) and record.get("lifecycle_stage") == "deleted":
            sys.stdout.write(f"mlflow_user: experiment {experiment} pending: in the trash\n")
        elif status == 200 and re.fullmatch(r"[0-9]+", identifier):
            ensure_read(url, admin, user, "experiment", identifier, f"experiment {experiment}", deadline)
        elif status == 404:
            sys.stdout.write(f"mlflow_user: experiment {experiment} pending: not created yet\n")
        else:
            raise Refused(f"reading the experiment {experiment} failed (HTTP {status})")
    sys.stdout.write("mlflow_user: ready\n")


def run(environment: Mapping[str, str], ssm: Any, run_seconds: float = RUN_SECONDS) -> int:
    """Run the step once; returns 0 (also when it skips or something is pending) or 1 with the reason on standard error."""
    deadline = time.monotonic() + run_seconds
    url = environment.get("MLFLOW_URL", "")
    types = tuple(environment.get("MODEL_API_HAI_TYPES", "HAI_1").split(","))
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
        ensure_account(url, (environment.get("MLFLOW_ADMIN_USERNAME", "admin"), password), user, credential, types, deadline)
    except Refused as refusal:
        sys.stderr.write(f"mlflow_user refused: {refusal}\n")
        return 1
    return 0


def main() -> int:
    import boto3

    return run(os.environ, boto3.client("ssm", region_name=os.environ.get("AWS_REGION")))


if __name__ == "__main__":
    sys.exit(main())
