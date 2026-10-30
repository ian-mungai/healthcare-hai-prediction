"""The client of the model API for the Streamlit page. It uses the standard library only and imports nothing from Streamlit.

The page gets every prediction through ``predict``. Each call has one deadline for the whole exchange, enforced by a watchdog that closes the socket, so a slow
or dripped answer cannot hold the page. The key lives in a ``Credential`` that prints as hidden, cannot be pickled and never reaches a log line or a message.
The client follows no redirect and retries nothing: the owner of a 429 or 503 decides when to ask again.
"""

from __future__ import annotations

import contextlib
import datetime
import http.client
import json
import logging
import math
import re
import socket
import threading
import time
import urllib.parse
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, NoReturn

LOG = logging.getLogger("streamlit_app.client")

PREDICT_PATH = "/v1/predict"
KEY_HEADER = "X-API-Key"
REQUEST_HEADER = "X-Request-ID"
MAX_ANSWER_BYTES = 16 * 1024
MAX_WAIT_SECONDS = 3600.0
DEADLINE_DEFAULT, DEADLINE_MIN, DEADLINE_MAX = 30.0, 0.1, 60.0
DEFAULT_URL = "http://127.0.0.1:8000"
LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")

KEY_PATTERN = re.compile(r"[\x21-\x7e]{1,256}")
HAI_PATTERN = re.compile(r"HAI_[1-6]")
REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9_.:-]{1,64}")
MODEL_VERSION_PATTERN = re.compile(r"[1-9][0-9]{0,18}")
AS_OF_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
SCHEMA_HASH_PATTERN = re.compile(r"[0-9a-f]{64}")
CCN_PATTERN = re.compile(r"[A-Za-z0-9]{6}")
YEAR_MIN, YEAR_MAX = 1990, 2100


class ConfigError(ValueError):
    """The settings are unusable. The message never holds the key."""


class Credential:
    """The API key. It prints as hidden, formats as hidden and cannot be pickled or copied into a session store."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Credential(<hidden>)"

    __str__ = __repr__

    def __reduce__(self) -> NoReturn:
        raise TypeError("the API key cannot be pickled or stored")


@dataclass(frozen=True)
class Settings:
    api_url: str
    api_key: Credential = field(repr=False)
    deadline_seconds: float
    hai_types: tuple[str, ...]


@dataclass(frozen=True)
class Prediction:
    request_id: str
    hai_type: str
    ccn: str
    window_year: int
    value: float
    model_version: str
    served_by: str
    fallback: bool
    features_as_of: str
    feature_schema_sha256: str


@dataclass(frozen=True)
class Failure:
    """A refusal or a failure. ``kind`` names the cause, ``status`` is the HTTP status when one arrived and ``retry_after`` is the wait the API asked for."""

    kind: str
    status: int | None
    retry_after: float | None
    request_id: str
    deadline_seconds: float


def load_settings(env: Mapping[str, str]) -> Settings:
    """Read and check the settings. The key must be set. A plain http address must be a loopback address, so the key never crosses a network unencrypted."""
    key = env.get("MODEL_API_KEY", "")
    if not key:
        raise ConfigError("MODEL_API_KEY is not set")
    if not KEY_PATTERN.fullmatch(key):
        raise ConfigError("MODEL_API_KEY must be 1 to 256 printable characters without spaces")
    url = env.get("MODEL_API_URL", DEFAULT_URL).strip()
    try:
        parts = urllib.parse.urlsplit(url)
        port = parts.port
    except ValueError:
        raise ConfigError("MODEL_API_URL is not a valid address") from None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ConfigError("MODEL_API_URL must be an http or https address")
    if parts.username is not None or parts.password is not None or parts.path not in ("", "/") or parts.query or parts.fragment:
        raise ConfigError("MODEL_API_URL must hold only a scheme, a host and a port")
    if parts.scheme == "http" and parts.hostname not in LOOPBACK_HOSTS:
        raise ConfigError("MODEL_API_URL may use plain http only for a loopback address")
    host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
    api_url = f"{parts.scheme}://{host}" + (f":{port}" if port else "")
    try:
        deadline = float(env.get("MODEL_API_DEADLINE_SECONDS", str(DEADLINE_DEFAULT)))
    except ValueError:
        raise ConfigError("MODEL_API_DEADLINE_SECONDS must be a number") from None
    if not math.isfinite(deadline) or not DEADLINE_MIN <= deadline <= DEADLINE_MAX:
        raise ConfigError(f"MODEL_API_DEADLINE_SECONDS must be from {DEADLINE_MIN:g} to {DEADLINE_MAX:g}")
    types = tuple(item.strip() for item in env.get("STREAMLIT_HAI_TYPES", "HAI_1").split(","))
    if not all(HAI_PATTERN.fullmatch(item) for item in types) or len(set(types)) != len(types):
        raise ConfigError("STREAMLIT_HAI_TYPES must list distinct types from HAI_1 to HAI_6")
    return Settings(api_url=api_url, api_key=Credential(key), deadline_seconds=deadline, hai_types=types)


def parse_wait(value: str | None) -> float | None:
    """The seconds a Retry-After header asks for, or None when it is absent, negative, not finite or not a number of seconds."""
    if value is None:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    return min(seconds, MAX_WAIT_SECONDS) if math.isfinite(seconds) and seconds >= 0 else None


STATUS_KINDS = {
    401: "unauthorized",
    413: "too_large",
    429: "rate_limited",
    503: "unavailable",
    404: "not_found",
    400: "invalid_request",
    422: "invalid_request",
}


def refuse_constant(name: str) -> NoReturn:
    raise ValueError("not a finite JSON number")


def read_answer(raw: bytes, hai_type: str, ccn: str, window_year: int, request_id: str, key: str) -> Prediction | None:
    """The prediction in a 200 answer, or None when the answer is not what the API documents. Nothing in a refused answer reaches the page.

    The metadata must have the formats the API writes and the request id must be the one sent. An answer whose shown fields hold the key is refused, so a server
    that echoes the key cannot put it on the page.
    """
    if len(raw) > MAX_ANSWER_BYTES:
        return None
    try:
        data: Any = json.loads(raw, parse_constant=refuse_constant)
    except (ValueError, RecursionError):
        return None
    if not isinstance(data, dict):
        return None
    value, year, fallback = data.get("prediction"), data.get("window_year"), data.get("fallback")
    answered_id, answered_hai, answered_ccn = data.get("request_id"), data.get("hai_type"), data.get("ccn")
    model_version, served_by, as_of, schema = data.get("model_version"), data.get("served_by"), data.get("features_as_of"), data.get("feature_schema_sha256")
    if not (isinstance(answered_id, str) and isinstance(answered_hai, str) and isinstance(answered_ccn, str) and isinstance(model_version, str)):
        return None
    if not (isinstance(served_by, str) and isinstance(as_of, str) and isinstance(schema, str) and isinstance(fallback, bool)):
        return None
    if any(key in shown for shown in (answered_id, answered_hai, answered_ccn, model_version, served_by, as_of, schema)):
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    if not math.isfinite(number) or isinstance(year, bool) or year != window_year or answered_hai != hai_type or answered_ccn.upper() != ccn.upper():
        return None
    if served_by not in ("stable", "canary") or answered_id != request_id or not REQUEST_ID_PATTERN.fullmatch(answered_id):
        return None
    if not MODEL_VERSION_PATTERN.fullmatch(model_version) or not SCHEMA_HASH_PATTERN.fullmatch(schema) or not AS_OF_PATTERN.fullmatch(as_of):
        return None
    try:
        datetime.date.fromisoformat(as_of)
    except ValueError:
        return None
    return Prediction(answered_id, hai_type, ccn, window_year, number, model_version, served_by, fallback, as_of, schema)


def exchange(connection: http.client.HTTPConnection, payload: bytes, headers: dict[str, str], abandoned: threading.Event, box: dict[str, Any]) -> None:
    """The whole exchange, run in a worker thread so the caller can stop waiting at the deadline in any phase, name lookup included."""
    try:
        connection.connect()
        if abandoned.is_set():
            connection.close()
            return
        connection.request("POST", PREDICT_PATH, body=payload, headers=headers)
        response = connection.getresponse()
        box["status"] = response.status
        box["wait"] = parse_wait(response.getheader("Retry-After"))
        if response.status == 200:
            box["raw"] = response.read(MAX_ANSWER_BYTES + 1)
    except (OSError, http.client.HTTPException) as error:
        box["error"] = error


def predict(settings: Settings, hai_type: str, ccn: str, window_year: int) -> Prediction | Failure:
    """Ask the API for one prediction. Every outcome returns: a prediction or a failure that names its cause.

    One absolute deadline covers name lookup, connection, send, headers and body. At the deadline the caller returns a timeout, closes the socket if one exists
    and tells the worker not to send if it has not yet connected.
    """
    request_id = uuid.uuid4().hex
    parts = urllib.parse.urlsplit(settings.api_url)
    connection_class = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    connection = connection_class(parts.hostname or "", parts.port, timeout=settings.deadline_seconds)
    payload = json.dumps({"hai_type": hai_type, "ccn": ccn, "window_year": window_year}).encode()
    headers = {"Content-Type": "application/json", KEY_HEADER: settings.api_key.reveal(), REQUEST_HEADER: request_id}
    abandoned = threading.Event()
    box: dict[str, Any] = {}
    started = time.monotonic()
    worker = threading.Thread(target=exchange, args=(connection, payload, headers, abandoned, box), daemon=True)
    worker.start()
    worker.join(settings.deadline_seconds)
    kind = ""
    if worker.is_alive():
        abandoned.set()
        sock = connection.sock
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
        kind = "timeout"
    connection.close()
    error = box.get("error")
    status: int | None = box.get("status")
    if error is not None:
        kind = "timeout" if isinstance(error, TimeoutError) else "unreachable"
    outcome: Prediction | Failure
    if not kind and status == 200:
        answer = read_answer(box.get("raw", b""), hai_type, ccn, window_year, request_id, settings.api_key.reveal())
        outcome = answer if answer is not None else Failure("bad_answer", status, None, request_id, settings.deadline_seconds)
    else:
        kind = kind or STATUS_KINDS.get(status or 0, "unexpected_status")
        wait = box.get("wait") if kind in ("rate_limited", "unavailable") else None
        outcome = Failure(kind, None if kind in ("timeout", "unreachable") else status, wait, request_id, settings.deadline_seconds)
    LOG.info(
        "model API call finished: outcome=%s status=%s request_id=%s ms=%d",
        "prediction" if isinstance(outcome, Prediction) else outcome.kind,
        status,
        request_id,
        (time.monotonic() - started) * 1000,
    )
    return outcome
