"""A small client of the Airflow 3 REST API for the host, used by the supervisor's reconciliation and the transform DAG's E2E runner.

The host holds the session's JWT secret (the supervisor wrote it under the ignored ``data/orchestration/`` folder), so it signs its own short-lived token for
the API server, which listens on the loopback only. No password file is read: the generated simple-auth passwords live inside the container. The token is
never logged and never leaves this process except in the ``Authorization`` header of a request to the loopback API port.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import http.client
import json
import socket
import threading
import time
from typing import Any

TOKEN_SECONDS = 600
AUDIENCE = "apache-airflow"
TERMINAL_RUN_STATES = ("success", "failed")
PAGE_SIZE = 100
MAX_PAGES = 50
TASK_LIMIT = 200


class AirflowApiError(Exception):
    """The API answered with a status the caller did not expect, or could not be reached."""


def abort_connection(connection: socket.socket, expired: threading.Event) -> None:
    expired.set()
    with contextlib.suppress(OSError):
        connection.shutdown(socket.SHUT_RDWR)


def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def sign_token(secret: str, user: str = "admin", role: str = "ADMIN", seconds: int = TOKEN_SECONDS) -> str:
    now = int(time.time())
    header = b64(json.dumps({"alg": "HS512", "typ": "JWT"}, separators=(",", ":")).encode())
    claims = b64(
        json.dumps({"sub": user, "role": role, "teams": [], "aud": AUDIENCE, "iat": now, "nbf": now - 5, "exp": now + seconds}, separators=(",", ":")).encode()
    )
    signature = hmac.new(secret.encode(), f"{header}.{claims}".encode(), hashlib.sha512).digest()
    return f"{header}.{claims}.{b64(signature)}"


class AirflowApi:
    """Requests to ``http://127.0.0.1:<port>/api/v2`` with a token signed by the session's JWT secret.

    ``deadline`` is an optional ``time.monotonic()`` time that bounds the whole read. A request that would start after it raises ``AirflowApiError``.
    A request that starts before it ends at the deadline whatever its state: a timer shuts the socket down, so a connect, a header or a body that keeps
    arriving in small groups cannot outlast it, and the request's socket is closed. A response that completes after the deadline is an error too.
    """

    def __init__(self, port: int, jwt_secret: str, timeout: float = 30.0, deadline: float | None = None) -> None:
        self.port = port
        self._secret = jwt_secret
        self.timeout = timeout
        self.deadline = deadline

    def __repr__(self) -> str:
        return f"AirflowApi(127.0.0.1:{self.port})"

    def request(self, method: str, path: str, body: dict[str, Any] | None = None, expect: tuple[int, ...] = (200,)) -> Any:
        timeout = self.timeout
        if self.deadline is not None:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise AirflowApiError(f"{method} {path}: the deadline of the read passed")
            timeout = min(timeout, remaining)
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        expired = threading.Event()
        watchdog = None
        headers = {"Authorization": f"Bearer {sign_token(self._secret)}", "Content-Type": "application/json"}
        try:
            connection.connect()
            if self.deadline is not None and connection.sock is not None:
                watchdog = threading.Timer(max(self.deadline - time.monotonic(), 0.0), abort_connection, (connection.sock, expired))
                watchdog.daemon = True
                watchdog.start()
            connection.request(method, f"/api/v2{path}", body=None if body is None else json.dumps(body).encode(), headers=headers)
            response = connection.getresponse()
            status, raw = response.status, response.read()
        except (OSError, http.client.HTTPException) as error:
            late = expired.is_set() or (self.deadline is not None and time.monotonic() >= self.deadline)
            raise AirflowApiError(f"{method} {path}: {'the deadline of the read passed' if late else type(error).__name__}") from None
        finally:
            if watchdog is not None:
                watchdog.cancel()
            connection.close()
        if expired.is_set() or (self.deadline is not None and time.monotonic() >= self.deadline):
            raise AirflowApiError(f"{method} {path}: the deadline of the read passed")
        if status not in expect:
            raise AirflowApiError(f"{method} {path}: status {status}: {raw.decode(errors='replace')[:300]}")
        return json.loads(raw) if raw else None

    def dag(self, dag_id: str) -> dict[str, Any] | None:
        """The DAG's record, or None while the DAG processor has not parsed it."""
        try:
            found: dict[str, Any] = self.request("GET", f"/dags/{dag_id}")
        except AirflowApiError as error:
            if "status 404" in str(error):
                return None
            raise
        return found

    def import_errors(self) -> list[dict[str, Any]]:
        """The DAG files the DAG processor could not import."""
        found: list[dict[str, Any]] = self.request("GET", "/importErrors")["import_errors"]
        return found

    def unpause(self, dag_id: str) -> None:
        """Let the scheduler run the DAG's runs (DAGs start paused)."""
        self.request("PATCH", f"/dags/{dag_id}", {"is_paused": False})

    def trigger(self, dag_id: str, dag_run_id: str, conf: dict[str, Any]) -> dict[str, Any]:
        """Start a manual run with a run ID and a configuration."""
        started: dict[str, Any] = self.request("POST", f"/dags/{dag_id}/dagRuns", {"dag_run_id": dag_run_id, "logical_date": None, "conf": conf})
        return started

    def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """A DAG run's record; ``state`` is queued, running, success or failed."""
        found: dict[str, Any] = self.request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        return found

    def task_states(self, dag_id: str, dag_run_id: str) -> dict[str, str | None]:
        """The state of every task instance of a run, by task ID. ``AirflowApiError`` when the answer does not list every task instance."""
        page = self.request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances?limit={TASK_LIMIT}")
        found = page["task_instances"]
        if page.get("total_entries") != len(found):
            raise AirflowApiError(f"the task list of run {dag_run_id} is incomplete")
        return {item["task_id"]: item["state"] for item in found}

    def mark_failed(self, dag_id: str, dag_run_id: str) -> None:
        """Mark a DAG run failed by hand."""
        self.request("PATCH", f"/dags/{dag_id}/dagRuns/{dag_run_id}", {"state": "failed"})

    def clear(self, dag_id: str, dag_run_id: str, task_ids: list[str]) -> None:
        """Clear task instances of a run so the scheduler runs them again (a new try)."""
        self.request(
            "POST",
            f"/dags/{dag_id}/clearTaskInstances",
            {"dry_run": False, "dag_run_id": dag_run_id, "task_ids": task_ids, "only_failed": False, "reset_dag_runs": True},
        )

    def runs(self, dag_id: str, states: tuple[str, ...] = ()) -> list[dict[str, Any]]:
        """The DAG's runs, newest first, optionally only those in the given states, read from every page of the listing.

        ``AirflowApiError`` when the listing cannot be shown to be complete: a page without its run list or total, a total that changes while the pages
        are read, a page that is empty before the total is reached, a run listed twice or more than ``MAX_PAGES`` pages.
        """
        found: list[dict[str, Any]] = []
        total: int | None = None
        for _ in range(MAX_PAGES):
            query = "&".join([f"limit={PAGE_SIZE}", f"offset={len(found)}", "order_by=-run_after", *[f"state={state}" for state in states]])
            page = self.request("GET", f"/dags/{dag_id}/dagRuns?{query}")
            listed, entries = page.get("dag_runs"), page.get("total_entries")
            if not isinstance(listed, list) or not isinstance(entries, int) or isinstance(entries, bool):
                raise AirflowApiError(f"the DAG run listing of {dag_id} has no run list or no total")
            if total is not None and entries != total:
                raise AirflowApiError(f"the number of runs of {dag_id} changed while its pages were read")
            total = entries
            found.extend(listed)
            if len(found) >= total:
                break
            if not listed:
                raise AirflowApiError(f"the DAG run listing of {dag_id} ended at {len(found)} of {total} runs")
        else:
            raise AirflowApiError(f"the DAG run listing of {dag_id} has more than {MAX_PAGES} pages")
        if len(found) != total or len({item.get("dag_run_id") for item in found}) != len(found):
            raise AirflowApiError(f"the DAG run listing of {dag_id} is not a list of {total} distinct runs")
        return found

    def wait_for_run(self, dag_id: str, dag_run_id: str, seconds: float) -> dict[str, Any]:
        """Poll until the DAG run is in a terminal state; ``AirflowApiError`` when the time runs out."""
        deadline = time.monotonic() + seconds
        while True:
            record = self.run(dag_id, dag_run_id)
            if record["state"] in TERMINAL_RUN_STATES:
                return record
            if time.monotonic() > deadline:
                raise AirflowApiError(f"run {dag_run_id} is still {record['state']} after {seconds:.0f} s")
            time.sleep(2.0)
