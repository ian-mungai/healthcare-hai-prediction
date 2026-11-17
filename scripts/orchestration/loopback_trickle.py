"""A loopback HTTP server that answers one byte at a time, for the deadline cases of the launch and the catalog setup.

A peer that keeps sending a few bytes inside every read timeout defeats a socket timeout, so a case that needs it must use a real HTTP exchange and not a
stand-in that sleeps exactly the read timeout. The server listens on 127.0.0.1 only, sends ``pad`` space bytes at ``interval`` seconds each and then ``body``,
with a Content-Length that names all of them, so a client that reads the whole body waits ``pad * interval`` seconds. Leading spaces are valid JSON.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


@dataclass
class Trickle:
    """The server: its base URL, an event set when a request arrives and the count of requests answered."""

    url: str
    started: threading.Event = field(default_factory=threading.Event)
    stop: threading.Event = field(default_factory=threading.Event)
    requests: list[str] = field(default_factory=list)


@contextlib.contextmanager
def trickling_server(body: bytes, *, pad: int, interval: float) -> Iterator[Trickle]:
    """Serve ``body`` after ``pad`` bytes sent ``interval`` seconds apart; the server is stopped and joined on exit."""
    holder: list[Trickle] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            state = holder[0]
            state.requests.append(self.path)
            state.started.set()
            self.send_response(200)
            self.send_header("Content-Length", str(pad + len(body)))
            self.end_headers()
            try:
                for _ in range(pad):
                    if state.stop.wait(interval):
                        return
                    self.wfile.write(b" ")
                    self.wfile.flush()
                self.wfile.write(body)
                self.wfile.flush()
            except OSError:
                return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    state = Trickle(f"http://127.0.0.1:{server.server_address[1]}")
    holder.append(state)
    serving = threading.Thread(target=server.serve_forever, name="loopback-trickle", daemon=True)
    serving.start()
    try:
        yield state
    finally:
        state.stop.set()
        server.shutdown()
        server.server_close()
        serving.join(timeout=5)
