"""A stand-in for the model API that speaks real HTTP on the loopback address, for the cases of the Streamlit page.

It checks the key header the way the real API does (a wrong or missing key answers 401 first) and then plays a scripted behavior, or answers a fixed
prediction. It records every request it saw. It is a stand-in: it proves how the page reacts to each answer the API documents, not how the real API behaves.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

KEY = "sk-test-streamlit-0123456789abcdef-do-not-log"
MODEL_VERSION = "7"
SENT = "<sent>"


@dataclass
class Behavior:
    """One scripted answer. ``delay`` waits before the answer, ``drip`` sends the answer one byte at a time with that pause between bytes."""

    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | dict[str, Any] | None = None
    delay: float = 0.0
    drip: float = 0.0
    hang: bool = False


@dataclass(frozen=True)
class Seen:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


def good_answer(payload: dict[str, Any], request_id: str) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "hai_type": payload["hai_type"],
        "ccn": payload["ccn"],
        "window_year": payload["window_year"],
        "prediction": 1.25,
        "model_version": MODEL_VERSION,
        "served_by": "stable",
        "fallback": False,
        "features_as_of": "2026-10-01",
        "feature_schema_sha256": "ab" * 32,
    }


class StandInApi:
    """Start with ``with StandInApi() as api``; queue behaviors with ``api.script(...)``; read ``api.seen`` afterwards."""

    def __init__(self, key: str = KEY) -> None:
        self.key = key
        self.seen: list[Seen] = []
        self._script: deque[Behavior] = deque()
        self._lock = threading.Lock()
        self._release = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:
                return

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                owner.respond(self, raw)

            def do_GET(self) -> None:
                owner.respond(self, b"")

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> StandInApi:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._release.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def script(self, *behaviors: Behavior) -> None:
        with self._lock:
            self._script.extend(behaviors)

    def respond(self, handler: BaseHTTPRequestHandler, raw: bytes) -> None:
        headers = {name.lower(): value for name, value in handler.headers.items()}
        with self._lock:
            self.seen.append(Seen(handler.command, handler.path, headers, raw))
            behavior = self._script.popleft() if self._script else None
        if headers.get("x-api-key") != self.key:
            self.send(handler, Behavior(status=401, body={"error": "unauthorized"}))
            return
        if behavior is None:
            try:
                payload = json.loads(raw)
                behavior = Behavior(body=good_answer(payload, headers.get("x-request-id", "none")))
            except (ValueError, KeyError, TypeError):
                behavior = Behavior(status=422, body={"error": "invalid"})
        self.send(handler, behavior, headers.get("x-request-id", ""))

    def send(self, handler: BaseHTTPRequestHandler, behavior: Behavior, sent_id: str = "") -> None:
        if behavior.hang:
            self._release.wait(60)
            return
        if behavior.delay:
            time.sleep(behavior.delay)
        body = behavior.body
        if isinstance(body, dict) and body.get("request_id") == SENT:
            body = body | {"request_id": sent_id}
        raw = body if isinstance(body, bytes) else json.dumps(body if body is not None else {}).encode()
        head = f"HTTP/1.1 {behavior.status} Stand-in\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n"
        head += "".join(f"{name}: {value}\r\n" for name, value in behavior.headers.items()) + "\r\n"
        data = head.encode() + raw
        try:
            if behavior.drip:
                for index in range(len(data)):
                    if self._release.is_set():
                        break
                    handler.wfile.write(data[index : index + 1])
                    handler.wfile.flush()
                    time.sleep(behavior.drip)
            else:
                handler.wfile.write(data)
                handler.wfile.flush()
        except OSError:
            pass
        handler.close_connection = True


def closed_port() -> int:
    """A loopback port that nothing listens on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])
