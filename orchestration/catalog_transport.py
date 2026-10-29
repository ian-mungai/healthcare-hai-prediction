"""Own the Mac's private catalog tunnel and loopback relay without replaying dependent operations."""

from __future__ import annotations

import contextlib
import ipaddress
import os
import re
import secrets
import signal
import socket
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

from orchestration.worker_protocol import read_frame, write_frame
from orchestration.worker_recovery import WorkerLedger
from orchestration.worker_stderr import StderrReader
from scripts.process import TimeoutExpired, find_program, run_command
from scripts.process_group import OwnedProcess, group_state, prepare_parent, process_snapshot, record_leader_start, spawn_owned, stop_owned

PLUGIN_VERSION = "1.2.835.0"
T = TypeVar("T")


@dataclass(frozen=True)
class TransportSpec:
    """Approved nonsecret host and port inputs. Class-network changes require separate approval."""

    instance: str
    profile: str
    region: str
    tunnel_port: int
    relay_port: int

    def __post_init__(self) -> None:
        if not re.fullmatch(r"i-[a-f0-9]{8,17}", self.instance) or not self.profile or not self.region:
            raise ValueError("catalog_transport_requires_verified_host_and_profile")
        if not 1 <= self.tunnel_port <= 65535 or not 1 <= self.relay_port <= 65535 or self.tunnel_port == self.relay_port:
            raise ValueError("catalog_transport_requires_distinct_local_ports")


class CatalogTransport:
    """Keep exact process ownership through readiness, loss, expiry and confirmed cleanup."""

    def __init__(self, registry: Path, *, session: str | None = None) -> None:
        prepare_parent()
        self.ledger = WorkerLedger(registry)
        self.session = session or secrets.token_hex(16)
        self.ledger.register_session(self.session)
        self.handles: list[OwnedProcess] = []
        self.deadline = 0.0
        self.endpoint = ""
        self.failed = False
        self._cleanup_failed = False
        self._closing = False
        self._lifecycle = threading.Lock()

    @property
    def survivors(self) -> tuple[OwnedProcess, ...]:
        """Expose retained handles whose process-group absence remains unconfirmed."""
        return tuple(handle for handle in self.handles if not handle.confirmed)

    @property
    def dispatch_paused(self) -> bool:
        """Pause new dispatch after transport failure or unconfirmed cleanup."""
        return (
            self.failed
            or self._cleanup_failed
            or (self._closing and bool(self.survivors))
            or any(handle.stderr_reader is not None and handle.stderr_reader.failure is not None for handle in self.handles)
        )

    def _spawn(self, role: str, arguments: list[str], deadline: float) -> OwnedProcess:
        if not self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise TimeoutError("catalog_startup_lock_deadline")
        try:
            if self._closing:
                raise RuntimeError("catalog_transport_closing")
            nonce = secrets.token_hex(16)
            witness = secrets.token_hex(32)
            spawn_utc = datetime.now(UTC).isoformat()
            environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR") if name in os.environ}
            environment.update(PYTHONDONTWRITEBYTECODE="1", HAI_WORKER_WITNESS=witness)
            command = [
                sys.executable,
                "-I",
                "-B",
                str(Path(__file__).with_name("catalog_transport_child.py")),
                "--hai-worker",
                nonce,
                "--role",
                role,
                *arguments,
            ]
            previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
            try:
                with self.ledger.spawn_guard(deadline):
                    self.ledger.append_intent(
                        {
                            "nonce": nonce,
                            "spawn_utc": spawn_utc,
                            "session": self.session,
                            "lane": "catalog_transport",
                            "owner": self.session,
                            "witness": witness,
                        },
                        deadline=deadline,
                    )
                    try:
                        handle = spawn_owned(
                            command,
                            environment,
                            witness=witness,
                            nonce=nonce,
                            session=self.session,
                            lane="catalog_transport",
                            owner=self.session,
                            deadline=deadline,
                            spawn_utc=spawn_utc,
                        )
                    except BaseException:
                        with contextlib.suppress(OSError, ValueError, TimeoutError):
                            self.ledger.close_intent(nonce, "spawn_failed", deadline=max(deadline, time.monotonic() + 1))
                        raise
                    self.handles.append(handle)
                    record_leader_start(handle, deadline)
                    self.ledger.append_spawn(handle.spawn_record(), deadline=deadline)
                handle.stderr_reader = StderrReader(
                    handle.stderr,
                    self.ledger.registry / f"catalog_stderr_{nonce}.log",
                    deadline=self.deadline + 10,
                    values=(witness,),
                    withhold=role == "tunnel",
                )
                handle.stderr_reader.start()
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous)
            write_frame(handle.stdin, {"action": "start", "deadline": self.deadline}, deadline)
            reply = read_frame(handle.stdout, deadline)
            if set(reply) != {"ready", "role", "port"} or reply["ready"] is not True or reply["role"] != role or type(reply["port"]) is not int:
                raise RuntimeError("catalog_child_readiness_refused")
            if role == "relay":
                self.endpoint = f"http://127.0.0.1:{reply['port']}"
            return handle
        finally:
            self._lifecycle.release()

    def _owned_listener(self, handle: OwnedProcess, port: int, deadline: float) -> bool:
        remaining = min(1.0, deadline - time.monotonic())
        if remaining <= 0:
            return False
        try:
            result = run_command("lsof", ["-nP", "-a", "-g", str(handle.pgid), f"-iTCP:{port}", "-sTCP:LISTEN", "-Fpn"], timeout=remaining)
        except (OSError, TimeoutExpired):
            return False
        members = {member.pid for member in process_snapshot(deadline).group(handle.pgid)}
        pid: int | None = None
        owned = False
        for line in result.stdout.splitlines():
            if line.startswith("p") and line[1:].isdigit():
                pid = int(line[1:])
            if line.startswith("n") and pid in members:
                address = line[1:].rsplit(":", 1)[0].strip("[]")
                try:
                    if not ipaddress.ip_address(address).is_loopback:
                        raise RuntimeError("catalog_listener_is_not_loopback")
                except ValueError:
                    raise RuntimeError("catalog_listener_address_unproven") from None
                owned = True
        return result.returncode == 0 and owned

    def _ready(self, handle: OwnedProcess, port: int, deadline: float) -> None:
        while time.monotonic() < deadline:
            snapshot = process_snapshot(deadline)
            if time.monotonic() >= deadline:
                break
            leader = next((member for member in snapshot.group(handle.pgid) if member.pid == handle.pid), None)
            if snapshot.status != "seen" or leader is None or leader.state.startswith("Z"):
                raise RuntimeError("catalog_transport_child_died")
            owned = self._owned_listener(handle, port, deadline)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if owned:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=min(0.5, remaining)):
                        if time.monotonic() < deadline:
                            return
                except OSError:
                    if time.monotonic() < deadline:
                        raise
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        raise TimeoutError("catalog_transport_readiness_deadline")

    def start(self, spec: TransportSpec, *, deadline: float, ready_deadline: float) -> str:
        """Start the exact approved Session Manager route. No network rule or relay image is changed."""
        if self.handles or self._closing or ready_deadline > deadline or time.monotonic() >= ready_deadline:
            raise ValueError("catalog_transport_start_refused")
        self.deadline = deadline
        try:
            remaining = min(2.0, ready_deadline - time.monotonic())
            version = run_command("session-manager-plugin", ["--version"], timeout=remaining)
            if version.returncode or version.stdout.strip() != PLUGIN_VERSION:
                raise RuntimeError("session_manager_plugin_version_requires_review")
            executable = find_program("aws")
            if executable is None:
                raise RuntimeError("session_manager_aws_cli_unavailable")
            tunnel = self._spawn(
                "tunnel",
                [
                    "--target-port",
                    str(spec.tunnel_port),
                    "--aws-executable",
                    executable,
                    "--instance",
                    spec.instance,
                    "--profile",
                    spec.profile,
                    "--region",
                    spec.region,
                ],
                ready_deadline,
            )
            self._ready(tunnel, spec.tunnel_port, ready_deadline)
            relay = self._spawn("relay", ["--target-port", str(spec.tunnel_port), "--listen-port", str(spec.relay_port)], ready_deadline)
            self._ready(relay, spec.relay_port, ready_deadline)
            return self.endpoint
        except BaseException:
            self.failed = True
            self.close(deadline=time.monotonic() + 10)
            raise

    def start_relay(self, target_port: int, *, deadline: float) -> str:
        """Exercise the real owned relay against a supplied loopback peer without starting an AWS session."""
        if self.handles or self._closing or not 1 <= target_port <= 65535 or time.monotonic() >= deadline:
            raise ValueError("catalog_relay_start_refused")
        self.deadline = deadline
        try:
            relay = self._spawn("relay", ["--target-port", str(target_port), "--listen-port", "0"], deadline)
            port = int(self.endpoint.rsplit(":", 1)[1])
            self._ready(relay, port, deadline)
            return self.endpoint
        except BaseException:
            self.failed = True
            self.close(deadline=time.monotonic() + 10)
            raise

    def check(self, *, deadline: float) -> None:
        """Fail visibly on tunnel loss or lifetime expiry. Never reconnect or repeat a request."""
        snapshot = process_snapshot(min(deadline, self.deadline))
        if (
            self._closing
            or self.dispatch_paused
            or not self.endpoint
            or time.monotonic() >= min(deadline, self.deadline)
            or snapshot.status != "seen"
            or any(not any(member.pid == handle.pid and not member.state.startswith("Z") for member in snapshot.group(handle.pgid)) for handle in self.handles)
        ):
            self.failed = True
            self.close(deadline=time.monotonic() + 10)
            raise RuntimeError("catalog_transport_unavailable")

    def execute(self, operation: Callable[[], T], *, deadline: float) -> T:
        """Run once. The caller bounds the callback. Keep late success while route health and lifetime remain valid.

        A callback failure retires the route without replay. The caller reconciles an unknown writer outcome.
        """
        self.check(deadline=deadline)
        try:
            result = operation()
        except BaseException:
            self.failed = True
            self.close(deadline=time.monotonic() + 10)
            raise
        self.check(deadline=self.deadline)
        return result

    def close(self, *, deadline: float) -> bool:
        """Stop only this transport's groups and retain every unresolved ownership record."""
        previous_interrupts = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        try:
            self._closing = True
            if not self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic())):
                self._cleanup_failed = True
                return False
            try:
                confirmed = True
                for handle in reversed(self.handles):
                    result = stop_owned(handle, deadline)
                    drained = handle.stderr_reader is None or handle.stderr_reader.finish(deadline=deadline)
                    if not drained:
                        confirmed = False
                    if result.confirmed and group_state(handle.pgid) == "gone":
                        from datetime import UTC, datetime

                        evidence: dict[str, Any] = {"group": "absent", "token": "not_seen", "members": [], "scan_utc": datetime.now(UTC).isoformat()}
                        try:
                            self.ledger.close(handle.nonce, handle.pgid, evidence, event="gone", deadline=deadline)
                        except (OSError, ValueError, TimeoutError):
                            confirmed = False
                    else:
                        confirmed = False
                self._cleanup_failed = not confirmed
                if confirmed:
                    self.endpoint = ""
                    self.ledger.unregister_session()
                return confirmed
            finally:
                self._lifecycle.release()
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_interrupts)
