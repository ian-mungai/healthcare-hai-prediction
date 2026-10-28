"""Serve a synthetic Session Manager route without importing an AWS client or reading credentials."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any


def _event(path: Path, kind: str, **fields: Any) -> None:
    with path.open("a") as stream:
        stream.write(json.dumps({"kind": kind, **fields}, separators=(",", ":")) + "\n")


def main() -> int:
    """Run a bounded local plugin or tunnel executable selected by a case-owned configuration."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--role", choices=("plugin", "tunnel", "sleeper"), required=True)
    args, command = parser.parse_known_args()
    if command[:1] == ["--"]:
        command = command[1:]
    config = json.loads(args.config.read_text())
    events = Path(config["events"])
    if args.role == "sleeper":
        _event(events, "sleeper_ready", pid=os.getpid(), pgid=os.getpgrp())
        time.sleep(15)
        return 0
    if args.role == "plugin":
        _event(events, "plugin", arguments=command)
        if command != ["--version"]:
            return 64
        sys.stdout.write(config["version"] + "\n")
        return int(config["plugin_exit"])
    _event(events, "tunnel", arguments=command)
    if (
        command[:2] != ["ssm", "start-session"]
        or "--document-name" not in command
        or command[command.index("--document-name") + 1] != "AWS-StartPortForwardingSession"
    ):
        return 64
    parameters = json.loads(command[command.index("--parameters") + 1])
    port = int(parameters["localPortNumber"][0])
    if parameters["portNumber"] != ["8181"]:
        return 64
    deadline = time.monotonic() + 15
    if config["mode"] == "not_ready":
        while time.monotonic() < deadline:
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        return 0
    body = b'{"catalog":"synthetic"}'
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", port))
        listener.listen(8)
        listener.settimeout(0.05)
        _event(events, "listening", port=port)
        while time.monotonic() < deadline:
            try:
                peer, _ = listener.accept()
            except TimeoutError:
                continue
            with peer:
                peer.settimeout(min(0.25, max(0.01, deadline - time.monotonic())))
                request = bytearray()
                try:
                    while b"\r\n\r\n" not in request and len(request) < 65536 and time.monotonic() < deadline:
                        chunk = peer.recv(4096)
                        if not chunk:
                            break
                        request.extend(chunk)
                except (TimeoutError, ConnectionError):
                    continue
                if not request:
                    continue
                _event(events, "request", method=bytes(request).split(b" ", 1)[0].decode("ascii"))
                if config["mode"] == "tunnel_loss":
                    return 0
                if config["mode"] == "unknown_writer":
                    continue
                try:
                    peer.sendall(b"HTTP/1.1 200 Synthetic\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)
                except ConnectionError:
                    continue
                if config["mode"] == "tunnel_body_exit":
                    _event(events, "tunnel_body_exit", pid=os.getpid(), pgid=os.getpgrp())
                    return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
