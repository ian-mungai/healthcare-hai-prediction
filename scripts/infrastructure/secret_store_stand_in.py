"""A loopback stand-in for the Parameter Store calls the project makes and for reading a shared key from Secrets Manager, for offline end-to-end cases.

It never leaves the machine. It enforces the rules the service documents, so a call the service would refuse fails here too: a hierarchy name starts
with a slash, a value is not empty and at most 4096 bytes, only the standard tier and the SecureString type are accepted, a create without
``Overwrite`` refuses an existing name, and a read without ``WithDecryption`` returns an opaque ciphertext marker instead of the value.
"""

from __future__ import annotations

import json
import re
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

from scripts.infrastructure import secret_store

PARAMETER_ACTIONS = ("GetParameter", "PutParameter")
SECRET_ACTIONS = ("GetSecretValue",)
STALL_LIMIT_SECONDS = 60.0
CIPHERTEXT_MARKER = "AQECAHg-ciphertext-"
PARAMETER_NAME = re.compile(r"^/[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+){0,14}$")


def stack_seeds() -> dict[str, str]:
    """Synthetic values for the five secrets the orchestration and lineage stacks read; the Fernet key has the format Airflow requires."""
    names = (
        secret_store.AIRFLOW_DB_PASSWORD,
        secret_store.AIRFLOW_JWT_SECRET,
        secret_store.AIRFLOW_API_SECRET_KEY,
        secret_store.MARQUEZ_DB_PASSWORD,
    )
    seeds = {name: "stack-" + secrets.token_hex(12) for name in names}
    seeds[secret_store.AIRFLOW_FERNET_KEY] = secret_store.new_fernet_key()
    if not secret_store.is_fernet_key(seeds[secret_store.AIRFLOW_FERNET_KEY]):
        raise ValueError(f"the stand-in value for {secret_store.AIRFLOW_FERNET_KEY} is not a Fernet key")
    return seeds


class StandInServer(ThreadingHTTPServer):
    """The HTTP server; it carries the stand-in whose state the handler reads and changes."""

    stand_in: StandIn


class StandInHandler(BaseHTTPRequestHandler):
    """Answers the Parameter Store and Secrets Manager JSON protocols for the calls the project makes."""

    def log_message(self, format: str, *args: Any) -> None:
        return

    def reply(self, status: int, body: dict[str, Any], error: str = "") -> None:
        """Send a JSON answer with the error type header the AWS client reads."""
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/x-amz-json-1.1")
        self.send_header("Content-Length", str(len(payload)))
        if error:
            self.send_header("x-amzn-ErrorType", error)
        self.end_headers()
        self.wfile.write(payload)

    def refuse(self, error: str, message: str) -> None:
        """Send a 400 answer with the given error code."""
        self.reply(400, {"__type": error, "message": message}, error)

    def do_POST(self) -> None:
        stand_in = cast(StandInServer, self.server).stand_in
        target = self.headers.get("X-Amz-Target", "")
        service, _, action = target.rpartition(".")
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
        name = str(request.get("Name") or request.get("SecretId") or "")
        overwrite = bool(request.get("Overwrite"))
        logical = action
        if action == "PutParameter":
            logical = "OverwriteParameter" if overwrite else "CreateParameter"
        if action in stand_in.stalled or logical in stand_in.stalled:
            stand_in.released.wait(STALL_LIMIT_SECONDS)
        with stand_in.lock:
            stand_in.log.append((action, name))
            known = (service == "AmazonSSM" and action in PARAMETER_ACTIONS) or (service == "secretsmanager" and action in SECRET_ACTIONS)
            if not known or action in stand_in.denied or logical in stand_in.denied:
                self.refuse("AccessDeniedException", f"not allowed: {action}")
            elif action == "GetSecretValue":
                if name in stand_in.shared:
                    self.reply(200, {"Name": name, "VersionId": "v1", "SecretString": stand_in.shared[name], "VersionStages": ["AWSCURRENT"]})
                else:
                    self.refuse("ResourceNotFoundException", "missing")
            elif action == "GetParameter":
                if name not in stand_in.values:
                    self.refuse("ParameterNotFound", "missing")
                else:
                    value = stand_in.values[name] if request.get("WithDecryption") is True else CIPHERTEXT_MARKER + secrets.token_hex(8)
                    self.reply(200, {"Parameter": {"Name": name, "Type": "SecureString", "Value": value, "Version": 1, "ARN": parameter_arn(name)}})
            else:
                self.put_parameter(stand_in, name, request, overwrite)

    def put_parameter(self, stand_in: StandIn, name: str, request: dict[str, Any], overwrite: bool) -> None:
        """The documented ``PutParameter`` rules, then the change; the caller holds the lock."""
        value = request.get("Value")
        if not PARAMETER_NAME.match(name) or name.lower().startswith(("/aws", "/ssm")):
            self.refuse("ValidationException", "invalid parameter name")
        elif not isinstance(value, str) or not value or len(value.encode()) > secret_store.MAX_VALUE_BYTES:
            self.refuse("ValidationException", "invalid parameter value")
        elif request.get("Type") != "SecureString" or request.get("Tier", "Standard") != "Standard" or "KeyId" in request:
            self.refuse("ValidationException", "only a standard SecureString under the default key is accepted")
        elif name in stand_in.values and not overwrite:
            self.refuse("ParameterAlreadyExists", "exists")
        else:
            stand_in.changes.append(("OverwriteParameter" if overwrite else "CreateParameter", name))
            stand_in.writes_seen.append({"name": name, "type": request.get("Type"), "tier": request.get("Tier", "Standard")})
            stand_in.values[name] = value
            self.reply(200, {"Version": 1, "Tier": "Standard"})


def parameter_arn(name: str) -> str:
    """The parameter ARN: the name without its leading slash after ``parameter/``."""
    return f"arn:aws:ssm:us-west-2:stand-in:parameter/{name.lstrip('/')}"


class StandIn:
    """Holds parameters (and shared secrets) in memory, counts calls and refuses the actions named in ``denied``; ``log`` keeps action and name only.

    ``denied`` and ``stalled`` take an API action (``GetParameter``, ``PutParameter``, ``GetSecretValue``) or a logical one: ``CreateParameter``
    (a put without ``Overwrite``) and ``OverwriteParameter``. A stalled action waits until ``released`` is set (at most 60 s). ``values`` holds the
    parameters by full name; ``shared`` holds the Secrets Manager secrets by name.
    """

    def __init__(self, values: dict[str, str] | None = None, denied: set[str] | None = None, shared: dict[str, str] | None = None) -> None:
        self.values: dict[str, str] = dict(values or {})
        self.shared: dict[str, str] = dict(shared or {})
        self.denied: set[str] = set(denied or ())
        self.stalled: set[str] = set()
        self.released = threading.Event()
        self.log: list[tuple[str, str]] = []
        self.changes: list[tuple[str, str]] = []
        self.writes_seen: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        self.server = StandInServer(("127.0.0.1", 0), StandInHandler)
        self.server.stand_in = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        """The base URL clients use."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def writes(self) -> int:
        """How many put calls arrived, successful or not."""
        with self.lock:
            return sum(1 for action, _ in self.log if action == "PutParameter")

    def changed(self) -> list[tuple[str, str]]:
        """The creates and overwrites that succeeded, as ``CreateParameter`` or ``OverwriteParameter`` and name, in order."""
        with self.lock:
            return list(self.changes)

    def requests(self) -> list[tuple[str, str]]:
        """Every request that arrived, as action and name, in order."""
        with self.lock:
            return list(self.log)

    def __enter__(self) -> StandIn:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.released.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
