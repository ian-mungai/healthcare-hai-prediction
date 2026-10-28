"""Bounded loopback HTTP faults for SDK worker verification. No AWS client or credentials are used."""

from __future__ import annotations

import contextlib
import json
import select
import socket
import struct
import threading
import time
from dataclasses import dataclass
from types import TracebackType

MODES = frozenset({"normal", "delay_headers", "drip_headers", "hang_body", "drip_body", "reset", "throttle", "server_error"})
MAX_REQUEST = 16 * 1024 * 1024


@dataclass(frozen=True)
class Scenario:
    """A synthetic response and a fixed fault. Payload bytes are never included in event records."""

    operation: str
    mode: str
    body: bytes = b"{}"
    lifetime_s: float = 60.0
    drip_interval_s: float = 2.0

    def __post_init__(self) -> None:
        if self.mode not in MODES or not 0 < self.lifetime_s <= 180 or not 0 < self.drip_interval_s <= 2:
            raise ValueError("Use a supported fault, a lifetime up to 180 seconds and a drip interval up to two seconds.")
        if len(self.body) > MAX_REQUEST:
            raise ValueError("Use a synthetic response no larger than 16 MiB.")


@dataclass(frozen=True)
class Event:
    """Independent request and socket evidence without request or response contents."""

    kind: str
    connection: int
    elapsed_s: float


class WorkerStandin:
    """Own a loopback listener and its bounded threads. Constructing this object opens no socket."""

    def __init__(self, scenario: Scenario) -> None:
        self.scenario = scenario
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._events: list[Event] = []
        self._connections: set[socket.socket] = set()
        self._threads: list[threading.Thread] = []
        self._listener: socket.socket | None = None
        self._started = 0.0
        self._deadline = 0.0
        self.endpoint = ""

    @property
    def events(self) -> tuple[Event, ...]:
        """Return a snapshot suitable for a sanitized verification artifact."""
        with self._lock:
            return tuple(self._events)

    def _record(self, kind: str, connection: int) -> None:
        with self._lock:
            self._events.append(Event(kind, connection, time.monotonic() - self._started))

    def __enter__(self) -> WorkerStandin:
        if self._listener is not None or self._stop.is_set():
            raise RuntimeError("Create a new stand-in for each bounded run.")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("127.0.0.1", 0))
            listener.listen(8)
            listener.settimeout(0.05)
        except BaseException:
            listener.close()
            raise
        self._listener = listener
        self._started = time.monotonic()
        self._deadline = self._started + self.scenario.lifetime_s
        self.endpoint = f"http://127.0.0.1:{listener.getsockname()[1]}"
        thread = threading.Thread(target=self._accept, name="sdk_standin_listener")
        self._threads.append(thread)
        try:
            thread.start()
        except BaseException:
            listener.close()
            self._listener = None
            self._threads.clear()
            raise
        return self

    def _accept(self) -> None:
        listener = self._listener
        if listener is None:
            return
        connection_id = 0
        while not self._stop.is_set() and time.monotonic() < self._deadline:
            try:
                peer, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            connection_id += 1
            with self._lock:
                self._connections.add(peer)
            thread = threading.Thread(target=self._serve, args=(peer, connection_id), name=f"sdk_standin_peer_{connection_id}")
            self._threads.append(thread)
            try:
                thread.start()
            except RuntimeError:
                self._record("handler_start_failed", connection_id)
                self._threads.remove(thread)
                with self._lock:
                    self._connections.discard(peer)
                peer.close()

    def _read_request(self, peer: socket.socket) -> bool:
        until = min(self._deadline, time.monotonic() + 2)
        pending = bytearray()
        required: int | None = None
        while not self._stop.is_set() and time.monotonic() < until:
            try:
                chunk = peer.recv(65536)
            except TimeoutError:
                continue
            if not chunk:
                return False
            pending.extend(chunk)
            if len(pending) > MAX_REQUEST:
                return False
            if required is None and b"\r\n\r\n" in pending:
                headers, body = pending.split(b"\r\n\r\n", 1)
                lengths = [line.split(b":", 1)[1].strip() for line in headers.split(b"\r\n")[1:] if line.lower().startswith(b"content-length:")]
                if len(lengths) > 1 or any(not value.isdigit() for value in lengths):
                    return False
                size = int(lengths[0]) if lengths else 0
                required = len(headers) + 4 + size
                if required > MAX_REQUEST or len(body) > size:
                    return False
                expect = [line.split(b":", 1)[1].strip().lower() for line in headers.split(b"\r\n")[1:] if line.lower().startswith(b"expect:")]
                if b"100-continue" in expect:
                    peer.sendall(b"HTTP/1.1 100 Continue\r\n\r\n")
            if required is not None and len(pending) >= required:
                return True
        return False

    def _wait(self, peer: socket.socket, connection: int, until: float) -> bool:
        while not self._stop.is_set() and time.monotonic() < min(until, self._deadline):
            readable, _, _ = select.select([peer], [], [], min(0.05, max(0, until - time.monotonic())))
            if readable:
                try:
                    closed = not peer.recv(1, socket.MSG_PEEK)
                except ConnectionResetError:
                    closed = True
                if closed:
                    self._record("peer_closed", connection)
                    return False
                raise ValueError("The fault connection received another request before its response finished.")
        return not self._stop.is_set() and time.monotonic() < self._deadline

    def _drip(self, peer: socket.socket, connection: int, payload: bytes) -> None:
        for value in payload:
            if not self._wait(peer, connection, time.monotonic()):
                return
            peer.sendall(bytes([value]))
            if not self._wait(peer, connection, time.monotonic() + self.scenario.drip_interval_s):
                return
        self._wait(peer, connection, self._deadline)

    def _serve(self, peer: socket.socket, connection: int) -> None:
        try:
            peer.settimeout(0.05)
            if not self._read_request(peer):
                self._record("request_incomplete", connection)
                return
            self._record("request", connection)
            mode = self.scenario.mode
            if mode == "reset":
                peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                self._record("reset", connection)
                return
            if mode == "delay_headers":
                self._wait(peer, connection, self._deadline)
                return
            status = 429 if mode == "throttle" else 500 if mode == "server_error" else 200
            body = self.scenario.body
            content_type = "application/x-amz-json-1.1"
            if mode == "drip_body":
                body += b" " * (int(self.scenario.lifetime_s / self.scenario.drip_interval_s) + 1)
            if mode in {"throttle", "server_error"}:
                error_code = "ThrottlingException" if mode == "throttle" else "InternalFailure"
                operation = self.scenario.operation
                if operation.startswith("s3_"):
                    content_type = "application/xml"
                    status = 503 if mode == "throttle" else 500
                    error_code = "SlowDown" if mode == "throttle" else "InternalError"
                    body = f"<Error><Code>{error_code}</Code><Message>Synthetic service fault</Message><RequestId>synthetic</RequestId></Error>".encode()
                elif operation in {"assume_role", "caller_identity"}:
                    content_type = "text/xml"
                    body = (
                        '<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
                        f"<Error><Type>Sender</Type><Code>{error_code}</Code><Message>Synthetic service fault</Message></Error>"
                        "<RequestId>synthetic</RequestId></ErrorResponse>"
                    ).encode()
                else:
                    body = json.dumps({"__type": error_code, "message": "Synthetic service fault"}).encode()
            object_headers = (
                'ETag: "synthetic_etag"\r\nx-amz-version-id: synthetic_version\r\nx-amz-meta-sha256: ' + "1" * 64 + "\r\n"
                if self.scenario.operation.startswith("s3_") and status == 200
                else ""
            )
            headers = (
                f"HTTP/1.1 {status} Synthetic\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\n{object_headers}Connection: close\r\n\r\n"
            ).encode()
            if mode == "drip_headers":
                self._drip(peer, connection, headers)
                return
            peer.sendall(headers)
            if mode == "hang_body":
                self._wait(peer, connection, self._deadline)
            elif mode == "drip_body":
                self._drip(peer, connection, body)
            else:
                peer.sendall(body)
                self._record("response_sent", connection)
        except (ConnectionError, OSError):
            self._record("peer_disconnected", connection)
        except ValueError:
            self._record("unexpected_request", connection)
        finally:
            peer.close()
            with self._lock:
                self._connections.discard(peer)

    def __exit__(self, kind: type[BaseException] | None, value: BaseException | None, _trace: TracebackType | None) -> None:
        self._stop.set()
        until = time.monotonic() + 2
        listener = self._listener
        if listener is not None:
            listener.close()
        self._threads[0].join(max(0, until - time.monotonic()))
        with self._lock:
            peers = tuple(self._connections)
        for peer in peers:
            with contextlib.suppress(OSError):
                peer.shutdown(socket.SHUT_RDWR)
        for thread in self._threads[1:]:
            thread.join(max(0, until - time.monotonic()))
        if any(thread.is_alive() for thread in self._threads):
            raise RuntimeError("Stand-in cleanup exceeded two seconds. Keep its evidence and start no further work.")
        self._listener = None
