"""A local HTTP stand-in for the model alias endpoint of the MLflow 3.17.0 tracking server, for offline end-to-end cases of the owner command.

``GET /api/2.0/mlflow/registered-models/alias?name=<model>&alias=<alias>`` answers as the pinned server does with the basic-auth app:

- 200 and ``{"model_version": {"name": ..., "version": "<number>", "aliases": [...], ...}}`` for a set alias.
- 404 and ``{"error_code": "RESOURCE_DOES_NOT_EXIST", "message": ...}`` for a missing registered model.
- 400 and ``{"error_code": "INVALID_PARAMETER_VALUE", "message": "Registered model alias <alias> not found."}`` for a registered model that exists (``exists``
  returns True for it) and has no such alias. Without ``exists`` no model exists.
- 401 and a plain-text body when the request carries no valid HTTP Basic login.

``lookup(model, alias)`` returns the version number the alias names, or None. ``fault`` names what goes wrong instead: ``server_error`` (500),
``other_404`` (404 with the code ENDPOINT_NOT_FOUND, as a wrong address gives), ``invalid_alias_400`` (400 with the code INVALID_PARAMETER_VALUE and the message
of an invalid alias name), ``other_code_400`` (400 with the alias message and another code), ``other_alias_400`` (400 with the alias message for another
alias), ``malformed`` (200 with text that is not JSON), ``wrong_shape`` (200 with JSON that has no version), ``redirect`` (302 to another path of the same
server) and ``stall`` (no answer until the server stops). Four faults deliver a correct answer too slowly. ``drip_headers`` sends the 200 answer, its status
line and headers included, one byte every ``DRIP_SECONDS`` for its first ``DRIP_BYTES`` bytes. ``drip_body`` does the same for the body of the 200 answer
only, and ``drip_error_body`` for the body of the 400 answer for an alias that is not set. ``withhold_400`` and ``withhold_404`` send the headers of that answer
with its Content-Length and never the body. ``requests`` records what each call asked for and whether its login was right, never the login itself. Start and
stop it as a context manager; stopping waits at most ``STOP_SECONDS``.
"""

from __future__ import annotations

import base64
import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import TracebackType
from typing import Any
from urllib.parse import parse_qs, urlsplit

ENDPOINT = "/api/2.0/mlflow/registered-models/alias"
STOP_SECONDS = 10.0
STALL_SECONDS = 30.0
DRIP_SECONDS = 0.25
DRIP_BYTES = 12


class AliasServer:
    """One local server for the alias endpoint, bound to an unused port of 127.0.0.1."""

    def __init__(self, lookup: Callable[[str, str], str | None], username: str, password: str, exists: Callable[[str], bool] = lambda model: False) -> None:
        self.lookup, self.exists, self.fault = lookup, exists, ""
        self.expected = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
        self.requests: list[dict[str, Any]] = []
        self.release = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                owner.answer(self)

            def log_message(self, format: str, *arguments: Any) -> None:
                return None

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, name="alias-stand-in", daemon=True)

    @property
    def uri(self) -> str:
        """The tracking URI that reaches this server."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def answer(self, handler: BaseHTTPRequestHandler) -> None:
        """Answer one GET and record it."""
        parts = urlsplit(handler.path)
        query = {key: values[0] for key, values in parse_qs(parts.query).items()}
        self.requests.append({"path": parts.path, "query": query, "authorized": handler.headers.get("Authorization") == self.expected})
        if self.fault == "stall":
            self.release.wait(STALL_SECONDS)
            return
        if parts.path != ENDPOINT:
            self.send(handler, 404, {"error_code": "ENDPOINT_NOT_FOUND", "message": f"no endpoint {parts.path}"})
        elif self.fault == "redirect":
            handler.send_response(302)
            handler.send_header("Location", "/elsewhere")
            handler.send_header("Content-Length", "0")
            handler.end_headers()
        elif not self.requests[-1]["authorized"]:
            self.send(
                handler,
                401,
                "You are not authenticated. Please see the MLflow authentication documentation.",
                "text/plain",
                {"WWW-Authenticate": 'Basic realm="mlflow"'},
            )
        elif self.fault == "server_error":
            self.send(handler, 500, {"error_code": "INTERNAL_ERROR", "message": "the stand-in failed"})
        elif self.fault == "other_404":
            self.send(handler, 404, {"error_code": "ENDPOINT_NOT_FOUND", "message": "no such endpoint"})
        elif self.fault == "invalid_alias_400":
            self.send(
                handler,
                400,
                {"error_code": "INVALID_PARAMETER_VALUE", "message": f"Invalid alias name: '{query.get('alias')}'. Aliases may not contain spaces."},
            )
        elif self.fault == "other_code_400":
            self.send(handler, 400, {"error_code": "BAD_REQUEST", "message": f"Registered model alias {query.get('alias')} not found."})
        elif self.fault == "other_alias_400":
            self.send(handler, 400, {"error_code": "INVALID_PARAMETER_VALUE", "message": "Registered model alias challenger not found."})
        elif self.fault in {"drip_headers", "drip_body", "drip_error_body"}:
            self.drip(handler, query)
        elif self.fault in {"withhold_400", "withhold_404"}:
            handler.send_response(int(self.fault[-3:]))
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", "100")
            handler.end_headers()
            self.release.wait(STALL_SECONDS)
        elif self.fault == "malformed":
            self.send(handler, 200, "<html>not json", "text/html")
        elif self.fault == "wrong_shape":
            self.send(handler, 200, {"model_version": {"name": query.get("name"), "aliases": [query.get("alias")]}})
        else:
            version = self.lookup(query.get("name", ""), query.get("alias", ""))
            if version is None and self.exists(query.get("name", "")):
                self.send(handler, 400, {"error_code": "INVALID_PARAMETER_VALUE", "message": f"Registered model alias {query.get('alias')} not found."})
            elif version is None:
                self.send(handler, 404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": f"Registered Model with name={query.get('name')} not found"})
            else:
                self.send(handler, 200, {"model_version": {"name": query["name"], "version": version, "aliases": [query["alias"]], "tags": []}})

    def drip(self, handler: BaseHTTPRequestHandler, query: dict[str, str]) -> None:
        """Send the answer of the fault slowly: one byte every ``DRIP_SECONDS`` for ``DRIP_BYTES`` bytes, then the rest at once. Stop when the server stops."""
        alias = query.get("alias")
        if self.fault == "drip_error_body":
            status, text = "400 Bad Request", json.dumps({"error_code": "INVALID_PARAMETER_VALUE", "message": f"Registered model alias {alias} not found."})
        else:
            status, text = "200 OK", json.dumps({"model_version": {"name": query.get("name"), "version": "2", "aliases": [alias], "tags": []}})
        body = text.encode()
        head = f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
        slow = head + body if self.fault == "drip_headers" else body
        if self.fault != "drip_headers":
            handler.wfile.write(head)
        try:
            for index in range(min(DRIP_BYTES, len(slow))):
                handler.wfile.write(slow[index : index + 1])
                if self.release.wait(DRIP_SECONDS):
                    return
            handler.wfile.write(slow[DRIP_BYTES:])
        except OSError:
            return

    @staticmethod
    def send(handler: BaseHTTPRequestHandler, status: int, body: Any, content_type: str = "application/json", headers: dict[str, str] | None = None) -> None:
        data = (json.dumps(body) if content_type == "application/json" else str(body)).encode()
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            handler.send_header(name, value)
        handler.end_headers()
        handler.wfile.write(data)

    def __enter__(self) -> AliasServer:
        self.thread.start()
        return self

    def __exit__(self, kind: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None) -> None:
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(STOP_SECONDS)
        if self.thread.is_alive():
            raise RuntimeError(f"the alias stand-in server did not stop within {STOP_SECONDS} seconds")
