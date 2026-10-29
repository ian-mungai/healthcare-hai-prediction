"""Run one owned catalog tunnel or relay after its parent records ownership durably."""

from __future__ import annotations

import argparse
import contextlib
import errno
import importlib.util
import json
import os
import resource
import select
import selectors
import shutil
import signal
import socket
import threading
import time
from pathlib import Path
from typing import Any


def _protocol() -> Any:
    spec = importlib.util.spec_from_file_location("hai_catalog_frames", Path(__file__).with_name("worker_protocol.py"))
    if spec is None or spec.loader is None:
        raise RuntimeError("catalog_protocol_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


protocol = _protocol()


def _monitor(deadline: float) -> None:
    while time.monotonic() < deadline:
        ready, _, _ = select.select([0], [], [], min(0.25, max(0.0, deadline - time.monotonic())))
        if ready:
            break
    os.killpg(os.getpgrp(), signal.SIGKILL)


def _relay(target_port: int, listen_port: int, deadline: float) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener, selectors.DefaultSelector() as selector:
        try:
            listener.bind(("127.0.0.1", listen_port))
        except OSError as error:
            if error.errno == errno.EADDRINUSE:
                os.write(2, b"catalog_relay_bind_in_use\n")
            raise
        listener.listen(16)
        listener.setblocking(False)
        selector.register(listener, selectors.EVENT_READ)
        protocol.write_frame(1, {"ready": True, "role": "relay", "port": listener.getsockname()[1]}, min(deadline, time.monotonic() + 2))
        peers: dict[socket.socket, socket.socket] = {}
        buffers: dict[socket.socket, bytearray] = {}
        closing: set[socket.socket] = set()

        def close_pair(peer: socket.socket) -> None:
            other = peers.pop(peer, None)
            for entry in (peer, other):
                if entry is None:
                    continue
                peers.pop(entry, None)
                buffers.pop(entry, None)
                closing.discard(entry)
                with contextlib.suppress(KeyError):
                    selector.unregister(entry)
                entry.close()

        def refresh(peer: socket.socket) -> None:
            if peer not in peers:
                return
            other = peers[peer]
            events = (selectors.EVENT_READ if peer not in closing and len(buffers[other]) < 1024 * 1024 else 0) | (
                selectors.EVENT_WRITE if buffers[peer] else 0
            )
            try:
                key = selector.get_key(peer)
            except KeyError:
                if events:
                    selector.register(peer, events)
            else:
                if events:
                    selector.modify(peer, events, key.data)
                else:
                    selector.unregister(peer)

        try:
            while time.monotonic() < deadline:
                for key, mask in selector.select(timeout=min(0.1, max(0.0, deadline - time.monotonic()))):
                    peer = key.fileobj
                    if peer is listener:
                        incoming, _ = listener.accept()
                        try:
                            target = socket.create_connection(("127.0.0.1", target_port), timeout=min(2.0, max(0.01, deadline - time.monotonic())))
                        except OSError:
                            incoming.close()
                            raise
                        incoming.setblocking(False)
                        target.setblocking(False)
                        peers[incoming], peers[target] = target, incoming
                        buffers[incoming], buffers[target] = bytearray(), bytearray()
                        selector.register(incoming, selectors.EVENT_READ)
                        selector.register(target, selectors.EVENT_READ)
                        continue
                    if not isinstance(peer, socket.socket) or peer not in peers:
                        continue
                    other = peers[peer]
                    try:
                        if mask & selectors.EVENT_READ:
                            chunk = peer.recv(65536)
                            if not chunk:
                                closing.update((peer, other))
                            else:
                                buffers[other].extend(chunk)
                        if mask & selectors.EVENT_WRITE and buffers[peer]:
                            sent = peer.send(buffers[peer])
                            del buffers[peer][:sent]
                    except BlockingIOError:
                        pass
                    except OSError:
                        close_pair(peer)
                        continue
                    if peer in closing and not buffers[peer] and not buffers[other]:
                        close_pair(peer)
                    else:
                        refresh(peer)
                        refresh(other)
        finally:
            for peer in tuple(peers):
                close_pair(peer)


def _tunnel(args: argparse.Namespace, deadline: float) -> None:
    allowed = shutil.which("aws")
    if not isinstance(args.aws_executable, str) or not Path(args.aws_executable).is_absolute():
        raise ValueError("catalog_tunnel_executable_not_allowed")
    if allowed is None or Path(args.aws_executable).resolve() != Path(allowed).resolve():
        raise ValueError("catalog_tunnel_executable_not_allowed")
    command = [
        args.aws_executable,
        "ssm",
        "start-session",
        "--target",
        args.instance,
        "--document-name",
        "AWS-StartPortForwardingSession",
        "--parameters",
        json.dumps({"portNumber": ["8181"], "localPortNumber": [str(args.target_port)]}),
        "--profile",
        args.profile,
        "--region",
        args.region,
        "--no-cli-pager",
    ]
    actions = [(os.POSIX_SPAWN_OPEN, number, os.devnull, os.O_RDONLY if number == 0 else os.O_WRONLY, 0) for number in (0, 1)]
    child = os.posix_spawn(
        args.aws_executable, command, dict(os.environ), file_actions=actions, setpgroup=os.getpgrp(), setsigdef=(signal.SIGCHLD, signal.SIGINT, signal.SIGTERM)
    )
    protocol.write_frame(1, {"ready": True, "role": "tunnel", "port": args.target_port}, min(deadline, time.monotonic() + 2))
    while time.monotonic() < deadline:
        if os.waitpid(child, os.WNOHANG)[0]:
            return
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))


def main() -> int:
    """Refuse invalid startup with status 70. Stop the group when an authorized body ends."""
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", choices=("relay", "tunnel"), required=True)
    parser.add_argument("--hai-worker", required=True)
    parser.add_argument("--target-port", type=int, required=True)
    parser.add_argument("--listen-port", type=int, default=0)
    parser.add_argument("--aws-executable")
    parser.add_argument("--instance")
    parser.add_argument("--profile")
    parser.add_argument("--region")
    args = parser.parse_args()
    start = protocol.read_frame(0, time.monotonic() + 5)
    if (
        set(start) != {"action", "deadline"}
        or start["action"] != "start"
        or not isinstance(start["deadline"], (int, float))
        or start["deadline"] <= time.monotonic()
        or (args.role == "tunnel" and not (args.aws_executable and args.instance and args.profile and args.region))
    ):
        return 70
    deadline = float(start["deadline"])
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    monitor = threading.Thread(target=_monitor, args=(deadline,), name="catalog_parent_monitor", daemon=True)
    monitor.start()
    try:
        if args.role == "relay":
            _relay(args.target_port, args.listen_port, deadline)
        else:
            _tunnel(args, deadline)
        return 0
    finally:
        os.killpg(os.getpgrp(), signal.SIGKILL)


if __name__ == "__main__":
    try:
        os._exit(main())
    except (OSError, ValueError, EOFError, TimeoutError):
        os._exit(70)
