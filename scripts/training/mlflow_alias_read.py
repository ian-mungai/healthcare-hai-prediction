"""Read the version that a registered model alias names from the MLflow REST API, with the Python standard library only.

The owner command ``promote_owner`` runs in the host environment, which has no MLflow package. The pinned server (MLflow 3.17.0) answers

``GET {tracking_uri}/api/2.0/mlflow/registered-models/alias?name=<model>&alias=<alias>``

with 200 and ``{"model_version": {"name": ..., "version": "<number>", ...}}``, and with 404 and ``{"error_code": "RESOURCE_DOES_NOT_EXIST", ...}`` when the
registered model does not exist. The server with the basic-auth app also needs an HTTP Basic login. Only that 404 means that no version holds the alias. Every
other answer is a failure: a network error, a missing answer within the deadline, a redirect (the login is never sent to another address), 401, 403, 5xx,
another 404 (a wrong address) and an answer that is not the JSON of a version of the model. The login never enters a message.
"""

from __future__ import annotations

import base64
import json
import re
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlencode, urlsplit

ENDPOINT = "/api/2.0/mlflow/registered-models/alias"
TIMEOUT_SECONDS = 30.0
MAX_BYTES = 1_048_576
CHUNK_BYTES = 65_536
ABSENT = "RESOURCE_DOES_NOT_EXIST"
VERSION = re.compile(r"[0-9]+")


class AliasReadError(Exception):
    """The alias could not be read. The message names the cause and holds no login."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect, so the login goes only to the address the owner set."""

    def redirect_request(self, *arguments: Any, **options: Any) -> None:
        return None


class AliasRequest(urllib.request.Request):
    """The one GET of the alias endpoint, built only for an http or https tracking address without a login in it."""

    def __init__(self, uri: str, model: str, alias: str, username: str, password: str) -> None:
        parts = urlsplit(uri)
        if parts.scheme not in {"http", "https"} or not parts.netloc or parts.query or parts.fragment or parts.username or parts.password:
            raise AliasReadError("MLFLOW_TRACKING_URI must be an http or https address without a login, query or fragment")
        login = base64.b64encode(f"{username}:{password}".encode()).decode()
        super().__init__(
            f"{uri.rstrip('/')}{ENDPOINT}?{urlencode({'name': model, 'alias': alias})}",
            headers={"Authorization": f"Basic {login}", "Accept": "application/json"},
        )


def body_of(response: Any, deadline: float, timeout: float) -> bytes:
    """Read at most ``MAX_BYTES`` of the answer, and fail when it is not complete at the deadline."""
    data = b""
    while chunk := response.read(CHUNK_BYTES):
        data += chunk
        if len(data) > MAX_BYTES:
            raise AliasReadError(f"the answer is longer than {MAX_BYTES} bytes")
        if time.monotonic() > deadline:
            raise AliasReadError(f"MLflow did not answer within {timeout:g} seconds")
    return data


def read_alias(uri: str, model: str, alias: str, username: str, password: str, timeout: float | None = None) -> str | None:
    """Return the version number the alias names, or None when MLflow answers that the registered model does not exist. Raise AliasReadError otherwise."""
    limit = TIMEOUT_SECONDS if timeout is None else timeout
    request = AliasRequest(uri, model, alias, username, password)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect)
    deadline = time.monotonic() + limit
    try:
        with opener.open(request, timeout=limit) as response:
            raw = body_of(response, deadline, limit)
    except urllib.error.HTTPError as error:
        check_missing_model(error, body_of(error, deadline, limit) if error.code == 404 else b"")
        return None
    except TimeoutError:
        raise AliasReadError(f"MLflow did not answer within {limit:g} seconds") from None
    except urllib.error.URLError as error:
        if isinstance(error.reason, TimeoutError):
            raise AliasReadError(f"MLflow did not answer within {limit:g} seconds") from None
        raise AliasReadError(f"MLflow cannot be reached at the tracking address: {type(error.reason).__name__}") from None
    except OSError as error:
        raise AliasReadError(f"the answer of MLflow cannot be read: {type(error).__name__}") from None
    return version_in(raw, model)


def check_missing_model(error: urllib.error.HTTPError, body: bytes) -> None:
    """Return when the answer is the 404 that names a missing registered model. Raise AliasReadError for every other status."""
    if error.code == 404:
        try:
            code = json.loads(body).get("error_code")
        except (ValueError, AttributeError):
            code = None
        if code == ABSENT:
            return
    hint = " Check MLFLOW_TRACKING_USERNAME and MLFLOW_TRACKING_PASSWORD." if error.code in {401, 403} else ""
    raise AliasReadError(f"MLflow answered HTTP {error.code}.{hint}")


def version_in(raw: bytes, model: str) -> str:
    """Return the version number in the 200 answer, or raise AliasReadError when the answer is not the JSON of a version of the model."""
    try:
        answer = json.loads(raw)
    except ValueError:
        raise AliasReadError("the answer of MLflow is not JSON") from None
    found = answer.get("model_version") if isinstance(answer, dict) else None
    version = found.get("version") if isinstance(found, dict) else None
    if not isinstance(found, dict) or not isinstance(version, str) or not VERSION.fullmatch(version) or found.get("name") not in {None, model}:
        raise AliasReadError(f"the answer of MLflow holds no version of {model}")
    return version
