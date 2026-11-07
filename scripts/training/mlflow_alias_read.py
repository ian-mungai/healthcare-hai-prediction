"""Read the version that a registered model alias names from the MLflow REST API, with the Python standard library only.

The owner command ``promote_owner`` runs in the host environment, which has no MLflow package. The pinned server (MLflow 3.17.0) answers

``GET {tracking_uri}/api/2.0/mlflow/registered-models/alias?name=<model>&alias=<alias>``

with 200 and ``{"model_version": {"name": ..., "version": "<number>", ...}}``. The server with the basic-auth app also needs an HTTP Basic login. Two answers
mean that no version holds the alias: 404 and ``{"error_code": "RESOURCE_DOES_NOT_EXIST", ...}`` when the registered model does not exist, and 400 and
``{"error_code": "INVALID_PARAMETER_VALUE", "message": "Registered model alias <alias> not found."}`` when the model exists and the alias is not set
(``get_model_version_by_alias`` of the SQL store, passed through ``catch_mlflow_exception`` unchanged). Every other answer is a failure: a network error, a
missing answer within the deadline, a redirect (the login is never sent to another address), 401, 403, 5xx, another 400 (another code or message) or 404 (a
wrong address) and an answer that is not the JSON of a version of the model. The login never enters a message.

One absolute deadline of ``TIMEOUT_SECONDS`` covers the status line and headers and every byte of the body, the body of a 400 or 404 answer included.
Every receive on the socket gets the time that is left as its timeout, so a server that sends one byte at a time cannot keep the call open. Three steps are
outside that deadline: the name lookup of the address has no timeout of its own (the system resolver's limits apply), the TCP connection to each address the
lookup returns can use the full limit again, and so can the TLS handshake of an https address. The documented owner address, http://localhost:5100 through
the Session Manager port forward, needs neither a name server nor TLS.
"""

from __future__ import annotations

import base64
import functools
import http.client
import io
import json
import re
import socket
import time
import urllib.error
import urllib.request
from typing import Any, cast
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
        self.deadline = 0.0


def time_left(deadline: float) -> float:
    """The seconds until the deadline. Raise TimeoutError when it has passed."""
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError("the deadline has passed")
    return left


class DeadlineReader(io.RawIOBase):
    """The raw reader of one socket that gives every receive the time left until the deadline as its timeout."""

    def __init__(self, sock: Any, deadline: float) -> None:
        super().__init__()
        self.sock, self.deadline = sock, deadline
        self.raw = sock.makefile("rb", buffering=0)

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int | None:
        self.sock.settimeout(time_left(self.deadline))
        return self.raw.readinto(buffer)

    def close(self) -> None:
        self.raw.close()
        super().close()


class DeadlineSocket:
    """The socket as ``http.client.HTTPResponse`` sees it: its only use is ``makefile``, which here reads under the deadline."""

    def __init__(self, sock: Any, deadline: float) -> None:
        self.sock, self.deadline = sock, deadline

    def makefile(self, mode: str) -> io.BufferedReader:
        return io.BufferedReader(DeadlineReader(self.sock, self.deadline))


def deadline_response(deadline: float) -> type[http.client.HTTPResponse]:
    """The response class that reads the status line, the headers and the body under the deadline."""

    class Response(http.client.HTTPResponse):
        def __init__(self, sock: socket.socket, *arguments: Any, **options: Any) -> None:
            super().__init__(cast(socket.socket, DeadlineSocket(sock, deadline)), *arguments, **options)

    return Response


class DeadlineHTTPConnection(http.client.HTTPConnection):
    """An http connection that connects within the time left and reads under the deadline."""

    def __init__(self, *arguments: Any, deadline: float, **options: Any) -> None:
        super().__init__(*arguments, **options)
        self.deadline = deadline
        self.response_class = deadline_response(deadline)

    def connect(self) -> None:
        self.timeout = time_left(self.deadline)
        super().connect()
        self.sock.settimeout(time_left(self.deadline))


class DeadlineHTTPSConnection(http.client.HTTPSConnection):
    """An https connection that connects within the time left and reads under the deadline."""

    def __init__(self, *arguments: Any, deadline: float, **options: Any) -> None:
        super().__init__(*arguments, **options)
        self.deadline = deadline
        self.response_class = deadline_response(deadline)

    def connect(self) -> None:
        self.timeout = time_left(self.deadline)
        super().connect()
        self.sock.settimeout(time_left(self.deadline))


class DeadlineHTTPHandler(urllib.request.HTTPHandler):
    """The http handler whose connection works under the deadline of the request."""

    def http_open(self, req: Any) -> Any:
        return self.do_open(functools.partial(DeadlineHTTPConnection, deadline=req.deadline), req)


class DeadlineHTTPSHandler(urllib.request.HTTPSHandler):
    """The https handler whose connection works under the deadline of the request."""

    def https_open(self, req: Any) -> Any:
        return self.do_open(functools.partial(DeadlineHTTPSConnection, deadline=req.deadline), req)


def body_of(response: Any) -> bytes:
    """Read at most ``MAX_BYTES`` of the answer. A read that outlasts the deadline raises TimeoutError."""
    data = b""
    while chunk := response.read(CHUNK_BYTES):
        data += chunk
        if len(data) > MAX_BYTES:
            raise AliasReadError(f"the answer is longer than {MAX_BYTES} bytes")
    return data


def fetch(request: AliasRequest, limit: float) -> tuple[urllib.error.HTTPError | None, bytes]:
    """Send the request and read the whole answer under the deadline: the body of a 200, or the error and its body when the status is 400 or 404."""
    request.deadline = time.monotonic() + limit
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect, DeadlineHTTPHandler, DeadlineHTTPSHandler)
    try:
        with opener.open(request, timeout=limit) as response:
            return None, body_of(response)
    except urllib.error.HTTPError as error:
        with error:
            return error, body_of(error) if error.code in {400, 404} else b""


def read_alias(uri: str, model: str, alias: str, username: str, password: str, timeout: float | None = None) -> str | None:
    """Return the version number the alias names, or None when MLflow answers that the model or the alias does not exist. Raise AliasReadError otherwise."""
    limit = TIMEOUT_SECONDS if timeout is None else timeout
    try:
        error, body = fetch(AliasRequest(uri, model, alias, username, password), limit)
    except TimeoutError:
        raise AliasReadError(f"MLflow did not answer within {limit:g} seconds") from None
    except urllib.error.URLError as error:
        if isinstance(error.reason, TimeoutError):
            raise AliasReadError(f"MLflow did not answer within {limit:g} seconds") from None
        raise AliasReadError(f"MLflow cannot be reached at the tracking address: {type(error.reason).__name__}") from None
    except OSError as error:
        raise AliasReadError(f"the answer of MLflow cannot be read: {type(error).__name__}") from None
    if error is None:
        return version_in(body, model)
    check_missing(error, body, alias)
    return None


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
