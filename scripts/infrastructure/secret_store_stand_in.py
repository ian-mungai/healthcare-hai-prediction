"""A loopback stand-in for the four Secrets Manager calls the project uses, for offline end-to-end cases; it never leaves the machine."""

from __future__ import annotations

import json
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

from scripts.infrastructure import secret_store

ACTIONS = ("GetSecretValue", "CreateSecret", "PutSecretValue", "DescribeSecret")
STALL_LIMIT_SECONDS = 60.0


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
    """Answers the Secrets Manager JSON protocol for the four actions."""

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

    def do_POST(self) -> None:
        stand_in = cast(StandInServer, self.server).stand_in
        action = self.headers.get("X-Amz-Target", "").rpartition(".")[2]
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
        name = str(request.get("SecretId") or request.get("Name") or "")
        if action in stand_in.stalled:
            stand_in.released.wait(STALL_LIMIT_SECONDS)
        with stand_in.lock:
            stand_in.log.append((action, name))
            if action not in ACTIONS or action in stand_in.denied:
                self.reply(400, {"__type": "AccessDeniedException", "Message": f"not allowed: {action}"}, "AccessDeniedException")
                return
            valueless = name in stand_in.valueless
            present = name in stand_in.values or valueless
            arn = f"arn:aws:secretsmanager:us-west-2:stand-in:secret:{name}-AbCdEf"
            if action == "CreateSecret" and present:
                self.reply(400, {"__type": "ResourceExistsException", "Message": "exists"}, "ResourceExistsException")
            elif action != "CreateSecret" and not present:
                self.reply(400, {"__type": "ResourceNotFoundException", "Message": "missing"}, "ResourceNotFoundException")
            elif action == "GetSecretValue" and valueless:
                self.reply(400, {"__type": "ResourceNotFoundException", "Message": "no value"}, "ResourceNotFoundException")
            elif action == "GetSecretValue":
                self.reply(200, {"ARN": arn, "Name": name, "VersionId": "v1", "SecretString": stand_in.values[name], "VersionStages": ["AWSCURRENT"]})
            elif action == "DescribeSecret":
                self.reply(200, {"ARN": arn, "Name": name})
            else:
                stand_in.changes.append((action, name))
                stand_in.valueless.discard(name)
                stand_in.values[name] = str(request["SecretString"])
                self.reply(200, {"ARN": arn, "Name": name, "VersionId": "v1"})


class StandIn:
    """Holds secrets in memory, counts calls and refuses the actions named in ``denied``; ``log`` keeps action and secret name only.

    A request for an action named in ``stalled`` waits until ``released`` is set (at most 60 s). A name in ``valueless`` exists but has no value.
    """

    def __init__(self, values: dict[str, str] | None = None, denied: set[str] | None = None) -> None:
        self.values: dict[str, str] = dict(values or {})
        self.denied: set[str] = set(denied or ())
        self.stalled: set[str] = set()
        self.valueless: set[str] = set()
        self.released = threading.Event()
        self.log: list[tuple[str, str]] = []
        self.changes: list[tuple[str, str]] = []
        self.lock = threading.Lock()
        self.server = StandInServer(("127.0.0.1", 0), StandInHandler)
        self.server.stand_in = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        """The base URL clients use."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def writes(self) -> int:
        """How many create and put calls arrived, successful or not."""
        with self.lock:
            return sum(1 for action, _ in self.log if action in ("CreateSecret", "PutSecretValue"))

    def changed(self) -> list[tuple[str, str]]:
        """The creates and puts that succeeded, as action and secret name, in order."""
        with self.lock:
            return list(self.changes)

    def requests(self) -> list[tuple[str, str]]:
        """Every request that arrived, as action and secret name, in order."""
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
