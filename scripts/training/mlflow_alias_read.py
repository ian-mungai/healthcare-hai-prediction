"""Read the version that a registered model alias names from the MLflow REST API, with the Python standard library only.

The owner command ``promote_owner`` runs in the host environment, which has no MLflow package. The pinned server (MLflow 3.17.0) answers

``GET {tracking_uri}/api/2.0/mlflow/registered-models/alias?name=<model>&alias=<alias>``

with 200 and ``{"model_version": {"name": ..., "version": "<number>", ...}}``. The server with the basic-auth app also needs an HTTP Basic login. Two answers
mean that no version holds the alias: 404 and ``{"error_code": "RESOURCE_DOES_NOT_EXIST", ...}`` when the registered model does not exist, and 400 and
``{"error_code": "INVALID_PARAMETER_VALUE", "message": "Registered model alias <alias> not found."}`` when the model exists and the alias is not set
(``get_model_version_by_alias`` of the SQL store, passed through ``catch_mlflow_exception`` unchanged). Every other answer is a failure: a network error, a
missing answer within the deadline, a redirect (the login is never sent to another address), 401, 403, 5xx, another 400 (another code or message) or 404 (a
wrong address) and an answer that is not the JSON of a version of the model. The login never enters a message.
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
INVALID = "INVALID_PARAMETER_VALUE"
VERSION = re.compile(r"[0-9]+")


def missing_alias_message(alias: str) -> str:
    """The message MLflow 3.17.0 gives with the code ``INVALID_PARAMETER_VALUE`` when the registered model exists and the alias is not set."""
    return f"Registered model alias {alias} not found."


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
    """Return the version number the alias names, or None when MLflow answers that the model or the alias does not exist. Raise AliasReadError otherwise."""
    limit = TIMEOUT_SECONDS if timeout is None else timeout
    request = AliasRequest(uri, model, alias, username, password)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect)
    deadline = time.monotonic() + limit
    try:
        with opener.open(request, timeout=limit) as response:
            raw = body_of(response, deadline, limit)
    except urllib.error.HTTPError as error:
        check_missing(error, body_of(error, deadline, limit) if error.code in {400, 404} else b"", alias)
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


def check_missing(error: urllib.error.HTTPError, body: bytes, alias: str) -> None:
    """Return when the answer is the 404 of a missing registered model or the 400 of an alias that is not set. Raise AliasReadError for every other answer."""
    try:
        answer = json.loads(body)
    except ValueError:
        answer = None
    fields = answer if isinstance(answer, dict) else {}
    seen = (error.code, fields.get("error_code"), fields.get("message"))
    if seen[:2] == (404, ABSENT) or seen == (400, INVALID, missing_alias_message(alias)):
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
