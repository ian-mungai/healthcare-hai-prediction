"""A loopback stand-in for the routes of the MLflow 3.17.2 basic-auth server that the serving secrets tool calls, for offline end-to-end cases.

It answers the routes read from the 3.17.2 wheel (``mlflow/server/auth/routes.py`` and ``__init__.py``; the services ``mlflow_user.py`` cites each line):
user get and create, user permission get and grant, registered model get and experiment get-by-name, and the registered model and experiment searches that
any logged-in user may call (the login check). ``slow_body_seconds`` makes every answer of a logged-in user arrive one byte at a time with that pause
between bytes, so a case can show that a reader stops at its absolute deadline. It answers the per-object permission routes that 3.17.2
removed (``/api/2.0/mlflow/registered-models/permissions/*`` and ``/api/2.0/mlflow/experiments/permissions/*``) with a 404 HTML page, as the real server
does, and counts every hit on them. A request needs HTTP Basic credentials of a known user. It keeps the request line of every call, never a body, and keeps
each body's content type and the keys of its JSON object so a case can show that a credential travels in a body and never in the address.
"""

from __future__ import annotations

import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

GET_USER = "/api/2.0/mlflow/users/get"
CREATE_USER = "/api/2.0/mlflow/users/create"
SEARCH_PATHS = ("/api/2.0/mlflow/registered-models/search", "/api/2.0/mlflow/experiments/search")
GET_PERMISSION = "/api/3.0/mlflow/users/permissions/get"
GRANT_PERMISSION = "/api/3.0/mlflow/users/permissions/grant"
GET_MODEL = "/api/2.0/mlflow/registered-models/get"
GET_EXPERIMENT = "/api/2.0/mlflow/experiments/get-by-name"
REMOVED_PREFIXES = ("/api/2.0/mlflow/registered-models/permissions/", "/api/2.0/mlflow/experiments/permissions/")
RESOURCE_TYPES = ("registered_model", "experiment")
NOT_FOUND_PAGE = b"<!doctype html><title>404 Not Found</title><h1>Not Found</h1>"


class StandInServer(ThreadingHTTPServer):
    """The HTTP server; it carries the stand-in whose state the handler reads and changes."""

    stand_in: MlflowAuthStandIn


class MlflowAuthHandler(BaseHTTPRequestHandler):
    """Answers the 3.17.2 routes with the documented status codes and error codes."""

    def log_message(self, format: str, *args: Any) -> None:
        return

    def send_json(self, status: int, body: dict[str, Any]) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        pause = cast(StandInServer, self.server).stand_in.slow_body_seconds
        if not pause:
            self.wfile.write(payload)
            return
        try:
            for index in range(len(payload)):
                self.wfile.write(payload[index : index + 1])
                self.wfile.flush()
                time.sleep(pause)
        except OSError:
            return

    def send_error_code(self, status: int, code: str) -> None:
        self.send_json(status, {"error_code": code, "message": code.lower()})

    def send_not_found_page(self) -> None:
        self.send_response(404)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(NOT_FOUND_PAGE)))
        self.end_headers()
        self.wfile.write(NOT_FOUND_PAGE)

    def caller(self, stand_in: MlflowAuthStandIn) -> str | None:
        """The user the Basic header names when its password is right, else ``None``."""
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return None
        try:
            name, _, password = base64.b64decode(header[6:]).decode().partition(":")
        except ValueError:
            return None
        return name if stand_in.users.get(name) == password else None

    def handle_any(self, method: str) -> None:
        stand_in = cast(StandInServer, self.server).stand_in
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        query = {key: values[0] for key, values in parse_qs(parsed.query).items()}
        body: dict[str, Any] = {}
        if raw:
            try:
                loaded = json.loads(raw)
                body = loaded if isinstance(loaded, dict) else {}
            except ValueError:
                body = {}
        with stand_in.lock:
            stand_in.log.append((method, parsed.path + ("?" + parsed.query if parsed.query else "")))
            stand_in.bodies.append((parsed.path, self.headers.get("Content-Type", ""), tuple(sorted(body))))
            if parsed.path.startswith(REMOVED_PREFIXES):
                stand_in.removed_hits.append((method, parsed.path))
                self.send_not_found_page()
                return
            caller = self.caller(stand_in)
            if caller is None:
                self.send_error_code(401, "UNAUTHENTICATED")
            elif method == "GET" and parsed.path in SEARCH_PATHS:
                self.send_json(200, {})
            elif caller != "admin":
                self.send_error_code(403, "PERMISSION_DENIED")
            else:
                self.route(stand_in, method, parsed.path, query, body)

    def route(self, stand_in: MlflowAuthStandIn, method: str, path: str, query: dict[str, str], body: dict[str, Any]) -> None:
        """The known routes; the caller holds the lock and has been authenticated as the administrator."""
        if (method, path) == ("GET", GET_USER):
            if query.get("username") in stand_in.users:
                self.send_json(200, {"user": {"username": query["username"], "is_admin": query["username"] == "admin"}})
            else:
                self.send_error_code(404, "RESOURCE_DOES_NOT_EXIST")
        elif (method, path) == ("POST", CREATE_USER):
            username, password = body.get("username"), body.get("password")
            if "json" not in self.headers.get("Content-Type", ""):
                self.send_error_code(400, "BAD_REQUEST")
            elif not username or not password:
                self.send_error_code(400, "INVALID_PARAMETER_VALUE")
            elif username in stand_in.users:
                self.send_error_code(400, "RESOURCE_ALREADY_EXISTS")
            else:
                stand_in.users[username] = password
                stand_in.created_users.append(username)
                self.send_json(200, {"user": {"id": len(stand_in.users), "username": username, "is_admin": False}})
        elif (method, path) == ("GET", GET_MODEL):
            if query.get("name") in stand_in.models:
                self.send_json(200, {"registered_model": {"name": query["name"]}})
            else:
                self.send_error_code(404, "RESOURCE_DOES_NOT_EXIST")
        elif (method, path) == ("GET", GET_EXPERIMENT):
            found = stand_in.experiments.get(query.get("experiment_name", ""))
            if found is None:
                self.send_error_code(404, "RESOURCE_DOES_NOT_EXIST")
            else:
                self.send_json(200, {"experiment": {"experiment_id": found[0], "name": query["experiment_name"], "lifecycle_stage": found[1]}})
        elif (method, path) == ("GET", GET_PERMISSION):
            self.get_permission(stand_in, query)
        elif (method, path) == ("POST", GRANT_PERMISSION):
            self.grant_permission(stand_in, body)
        else:
            self.send_not_found_page()

    def get_permission(self, stand_in: MlflowAuthStandIn, query: dict[str, str]) -> None:
        """The effective permission: the user's grant, else the server default. An unknown resource answers the default, as the real server does."""
        username = query.get("username", "")
        if username not in stand_in.users:
            self.send_error_code(404, "RESOURCE_DOES_NOT_EXIST")
        elif query.get("resource_type") not in RESOURCE_TYPES:
            self.send_error_code(400, "INVALID_PARAMETER_VALUE")
        else:
            permission = stand_in.grants.get((username, query["resource_type"], query.get("resource_id", "")), stand_in.default_permission)
            self.send_json(200, {"allowed": permission in ("USE", "EDIT", "MANAGE"), "permission": permission})

    def grant_permission(self, stand_in: MlflowAuthStandIn, body: dict[str, Any]) -> None:
        """One new grant row; an existing row refuses with ``RESOURCE_ALREADY_EXISTS``, as ``grant_user_resource_permission`` does."""
        key = (str(body.get("username")), str(body.get("resource_type")), str(body.get("resource_id")))
        if key[0] not in stand_in.users:
            self.send_error_code(404, "RESOURCE_DOES_NOT_EXIST")
        elif key[1] not in RESOURCE_TYPES or body.get("permission") not in ("READ", "USE", "EDIT", "MANAGE", "NO_PERMISSIONS", "DENY"):
            self.send_error_code(400, "INVALID_PARAMETER_VALUE")
        elif key in stand_in.grants:
            self.send_error_code(400, "RESOURCE_ALREADY_EXISTS")
        else:
            stand_in.grants[key] = str(body["permission"])
            stand_in.granted.append(key)
            self.send_json(200, {})

    def do_GET(self) -> None:
        self.handle_any("GET")

    def do_POST(self) -> None:
        self.handle_any("POST")


class MlflowAuthStandIn:
    """Holds users, registered models, experiments (name to id and lifecycle stage) and grants in memory.

    ``users`` maps a user name to its password and starts with the administrator. ``log`` keeps method and address (path and query) of every request; ``bodies``
    keeps path, content type and the sorted keys of the JSON object; ``removed_hits`` keeps each call of a removed route. ``default_permission`` is what a user
    without a grant gets. ``created_users`` and ``granted`` list what succeeded, in order. ``slow_body_seconds`` is the pause between the bytes of an
    answer (zero sends the answer at once).
    """

    def __init__(self, admin_password: str, models: set[str] | None = None, experiments: dict[str, tuple[str, str]] | None = None) -> None:
        self.users: dict[str, str] = {"admin": admin_password}
        self.models: set[str] = set(models or ())
        self.experiments: dict[str, tuple[str, str]] = dict(experiments or {})
        self.grants: dict[tuple[str, str, str], str] = {}
        self.default_permission = "READ"
        self.slow_body_seconds = 0.0
        self.log: list[tuple[str, str]] = []
        self.bodies: list[tuple[str, str, tuple[str, ...]]] = []
        self.removed_hits: list[tuple[str, str]] = []
        self.created_users: list[str] = []
        self.granted: list[tuple[str, str, str]] = []
        self.lock = threading.Lock()
        self.server = StandInServer(("127.0.0.1", 0), MlflowAuthHandler)
        self.server.stand_in = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        """The base URL a client passes as the tracking address."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self) -> MlflowAuthStandIn:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
