"""The HTTP client a task uses to reach the host job broker; it implements orchestration.interface.Submissions.

DAG code imports this module and the interface only (failure modes 771 and 823): it imports nothing but the standard
library and the interface, so it carries no launcher, Docker or credential code. Inside the Airflow scheduler the
supervisor mounts the session folder at ``/opt/hai/broker``: ``client.json`` names the broker URL and
``broker_token`` holds this session's token, which is sent only in the ``Authorization`` header and never logged.
"""

from __future__ import annotations

import http.client
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from orchestration.interface import Handle, State, Status, SubmissionRefused, SubmissionRequest

SESSION_FOLDER = Path("/opt/hai/broker")
SESSION_KEY_FILE = "broker_token"
CLIENT_FILE = "client.json"


class BrokerUnavailable(Exception):
    """The broker could not be reached or refused the caller's token; nothing was submitted."""


class BrokerClient:
    """Submissions over the broker's loopback HTTP listener."""

    def __init__(self, url: str, token: str, timeout: float = 120.0) -> None:
        """``url`` is the broker's base URL, for example ``http://host.docker.internal:18790``."""
        parts = urlsplit(url)
        if parts.scheme != "http" or not parts.hostname or parts.port is None or parts.path not in ("", "/") or not token:
            raise BrokerUnavailable("the broker URL must be http://<host>:<port> and the token must not be empty")
        self._host, self._port = parts.hostname, parts.port
        self._token = token
        self._timeout = timeout

    @classmethod
    def from_session(cls, folder: Path = SESSION_FOLDER) -> BrokerClient:
        """A client from the session folder the supervisor mounts read-only."""
        try:
            settings = json.loads((folder / CLIENT_FILE).read_text())
            token = (folder / SESSION_KEY_FILE).read_text().strip()
        except (OSError, ValueError) as exc:
            raise BrokerUnavailable(f"broker session files unreadable in {folder}: {type(exc).__name__}") from None
        return cls(str(settings["url"]), token)

    def __repr__(self) -> str:
        """Never show the token."""
        return f"BrokerClient(http://{self._host}:{self._port})"

    def _call(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST one operation; a refusal raises SubmissionRefused, a transport or token failure BrokerUnavailable."""
        connection = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
        headers = {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}
        try:
            connection.request("POST", f"/v1/{operation}", body=json.dumps(payload).encode(), headers=headers)
            response = connection.getresponse()
            status, raw = response.status, response.read()
        except (OSError, http.client.HTTPException) as exc:
            raise BrokerUnavailable(f"broker unreachable for {operation}: {type(exc).__name__}") from None
        finally:
            connection.close()
        try:
            body: dict[str, Any] = json.loads(raw)
        except ValueError:
            raise BrokerUnavailable(f"broker answered {status} to {operation} with a body that is not JSON") from None
        if status == 200:
            return body
        message = str(body.get("message", body.get("error", "")))
        if status == 400:
            raise SubmissionRefused(message)
        raise BrokerUnavailable(f"broker answered {status} to {operation}: {message}")

    def health(self) -> dict[str, Any]:
        """The broker's liveness answer; proves the listener is reachable and the token accepted."""
        return self._call("health", {})

    def submit(self, request: SubmissionRequest) -> Handle:
        """Write the intent, then launch once; a repeated request returns the existing handle."""
        return _handle(self._call("submit", request.as_dict()))

    def status(self, handle: Handle) -> Status:
        """The submission's current state from the ledger."""
        return _status(self._call("status", {"submission_id": handle.submission_id}))

    def cancel(self, handle: Handle) -> Status:
        """Stop the submission's container with its template's signal and grace period."""
        return _status(self._call("cancel", {"submission_id": handle.submission_id}))

    def logs(self, handle: Handle, tail: int = 200) -> str:
        """The last ``tail`` lines of the container's output, with secrets redacted by the broker."""
        return str(self._call("logs", {"submission_id": handle.submission_id, "tail": tail})["logs"])

    def record(self, handle: Handle) -> dict[str, Any]:
        """The submission's finished ledger line; refused while it has not finished."""
        line: dict[str, Any] = self._call("record", {"submission_id": handle.submission_id})["record"]
        return line


def _handle(body: dict[str, Any]) -> Handle:
    """A handle from the broker's answer."""
    return Handle(str(body["submission_id"]), str(body["container_name"]))


def _status(body: dict[str, Any]) -> Status:
    """A status from the broker's answer."""
    exit_code = body["exit_code"]
    return Status(_handle(body["handle"]), State(body["state"]), None if exit_code is None else int(exit_code), body["record_path"])
