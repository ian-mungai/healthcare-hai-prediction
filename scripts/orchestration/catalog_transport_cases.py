"""Exercise bounded local relay failures. Class-network and Session Manager acceptance stay separate."""

from __future__ import annotations

import argparse
import contextlib
import errno
import hashlib
import http.client
import json
import os
import shlex
import socket
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import urlsplit

from orchestration.catalog_transport import PLUGIN_VERSION, CatalogTransport, TransportSpec
from orchestration.worker_recovery import Recovery, WorkerLedger
from scripts.orchestration.sdk_standins import Scenario, WorkerStandin
from scripts.process import CompletedProcess, run_command
from scripts.process_group import Snapshot, group_state, process_snapshot, stop_owned

ROUTE_CASES = (
    "full_normal",
    "plugin_mismatch",
    "plugin_failure",
    "readiness_deadline",
    "readiness_scan_deadline",
    "relay_start_failure",
    "tunnel_loss",
    "tunnel_loss_after_reply",
    "unknown_writer",
    "request_deadline",
    "full_lifetime",
    "parent_crash",
    "tunnel_body_exit",
    "cleanup_retry",
    "failed_cleanup_retry",
    "late_callback_success",
)
CASES = ("normal", "relay_death", "lifetime_expiry", "dead_target", "stderr_flood", *ROUTE_CASES)
BODY = b'{"catalog":"synthetic"}'


class TransportCleanupUnconfirmed(RuntimeError):
    """Stop the local case queue and retain all payloads after unconfirmed cleanup."""

    def __init__(self, row: dict[str, Any]) -> None:
        super().__init__("catalog_case_cleanup_unconfirmed")
        self.row = row


def _request(endpoint: str) -> bytes:
    connection = http.client.HTTPConnection("127.0.0.1", urlsplit(endpoint).port, timeout=1)
    try:
        connection.request("GET", "/", headers={"X-Synthetic-Client": "catalog"})
        return connection.getresponse().read()
    finally:
        connection.close()


def _unavailable(transport: CatalogTransport) -> bool:
    try:
        transport.check(deadline=time.monotonic() + 5)
    except RuntimeError as error:
        return str(error) == "catalog_transport_unavailable"
    return False


def _stderr_route(transport: CatalogTransport, folder: Path) -> bytes:
    """Use a local executable in the real tunnel group without starting Session Manager."""
    executable = folder / "aws"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json, os, socket, sys, time\n"
        "port = int(json.loads(sys.argv[sys.argv.index('--parameters') + 1])['localPortNumber'][0])\n"
        f"with open({str(transport.ledger.path)!r}) as ledger: rows = [json.loads(line) for line in ledger]\n"
        "witness = next(row['spawn']['witness'] for row in rows if row['event'] == 'spawn' and row['spawn']['pid'] == os.getppid())\n"
        "body = b'x' * (3 * 65536) + b'synthetic_transport_secret' + witness.encode()\n"
        "while body:\n    size = os.write(2, body[:4096])\n    body = body[size:]\n"
        "until = time.monotonic() + 10\n"
        "with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:\n"
        "    listener.bind(('127.0.0.1', port))\n    listener.listen(8)\n    listener.settimeout(0.05)\n"
        "    while time.monotonic() < until:\n"
        "        try: peer, _ = listener.accept()\n        except TimeoutError: continue\n"
        "        with peer:\n            peer.settimeout(0.1)\n"
        "            try: request = peer.recv(4096)\n            except TimeoutError: continue\n"
        f"            body = {BODY!r}\n"
        "            if request: peer.sendall(b'HTTP/1.1 200 Synthetic\\r\\nContent-Length: ' + str(len(body)).encode() + b'"
        "\\r\\nConnection: close\\r\\n\\r\\n' + body)\n"
    )
    executable.chmod(0o700)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as target, socket.socket(socket.AF_INET, socket.SOCK_STREAM) as relay:
        target.bind(("127.0.0.1", 0))
        relay.bind(("127.0.0.1", 0))
        target_port, relay_port = target.getsockname()[1], relay.getsockname()[1]

    def command(program: str, arguments: Any, **options: Any) -> CompletedProcess[str]:
        if program == "session-manager-plugin":
            return CompletedProcess([program, *arguments], 0, PLUGIN_VERSION + "\n", "")
        if program != "lsof":
            raise ValueError("unexpected_catalog_case_program")
        return run_command("lsof", arguments, **options)

    lifetime = time.monotonic() + 10
    with (
        patch.dict(os.environ, {"PATH": f"{folder}{os.pathsep}{os.environ.get('PATH', os.defpath)}"}),
        patch("orchestration.catalog_transport.find_program", return_value=str(executable)),
        patch("orchestration.catalog_transport.run_command", command),
    ):
        endpoint = transport.start(
            TransportSpec("i-00000000", "synthetic", "synthetic", target_port, relay_port), deadline=lifetime, ready_deadline=lifetime - 5
        )
        return transport.execute(lambda: _request(endpoint), deadline=lifetime)


def run_case(name: str, folder: Path) -> dict[str, Any]:
    """Exercise one real relay with an absolute lifetime and independent group-absence checks."""
    if name in ROUTE_CASES:
        return _route_case(name, folder)
    scratch = Path(tempfile.mkdtemp(prefix=f"catalog_{name}_", dir=folder))
    started = time.monotonic()
    checks: dict[str, bool] = {}
    failure_code = None
    transport = CatalogTransport(scratch / "registry")
    with WorkerStandin(Scenario("catalog", "reset" if name == "dead_target" else "normal", BODY, lifetime_s=12, counted_header="x-synthetic-client")) as peer:
        try:
            if name == "stderr_flood":
                checks["stderr_tunnel_flood_does_not_block_route"] = _stderr_route(transport, scratch) == BODY
            elif name == "dead_target":
                failed = False
                count = 0
                until = time.monotonic() + 5
                endpoint = transport.start_relay(int(urlsplit(peer.endpoint).port or 0), deadline=until)

                def failed_operation() -> bytes:
                    nonlocal count
                    count += 1
                    return _request(endpoint)

                try:
                    transport.execute(failed_operation, deadline=until)
                except (ConnectionError, TimeoutError, http.client.RemoteDisconnected):
                    failed = True
                except RuntimeError as error:
                    failed = str(error) == "catalog_transport_unavailable"
                checks["dead_target_fails_visibly"] = failed
                checks["dead_target_operation_runs_once"] = count == 1
                checks["dead_target_receives_one_request"] = sum(event.kind == "request" for event in peer.events) == 1
                checks["dead_target_failure_is_bounded"] = time.monotonic() - started < 8
            else:
                lifetime = time.monotonic() + (1.5 if name == "lifetime_expiry" else 8)
                endpoint = transport.start_relay(int(urlsplit(peer.endpoint).port or 0), deadline=lifetime)
                if name == "normal":
                    count = 0

                    def operation() -> bytes:
                        nonlocal count
                        count += 1
                        return _request(endpoint)

                    body = transport.execute(operation, deadline=lifetime)
                    checks.update(
                        real_relay_reply=body == BODY,
                        execute_runs_once=count == 1,
                        single_request=sum(event.kind == "request" for event in peer.events) == 1,
                    )
                elif name == "relay_death":
                    killed = stop_owned(transport.handles[0], min(lifetime, time.monotonic() + 5))
                    checks["relay_group_was_killed"] = killed.confirmed and group_state(transport.handles[0].pgid) == "gone"
                    checks["check_refuses_dead_relay"] = _unavailable(transport)
                elif name == "lifetime_expiry":
                    while time.monotonic() < lifetime:
                        time.sleep(min(0.05, max(0.0, lifetime - time.monotonic())))
                    checks["check_refuses_expired_transport"] = _unavailable(transport)
                else:
                    raise ValueError("unsupported_local_catalog_case")
        except (OSError, ValueError, EOFError, TimeoutError, RuntimeError, http.client.HTTPException) as error:
            checks[f"unexpected_{type(error).__name__}"] = False
            code = str(error)
            failure_code = (
                code
                if code in {"catalog_transport_readiness_deadline", "catalog_transport_child_died", "catalog_transport_unavailable"}
                else type(error).__name__
            )
        finally:
            confirmed = transport.close(deadline=time.monotonic() + 5)
            absent = all(group_state(handle.pgid) == "gone" for handle in transport.handles)
            checks.update(cleanup_confirmed=confirmed, groups_absent=absent)
            readers = [handle.stderr_reader for handle in transport.handles]
            checks["stderr_readers_owned_and_complete"] = bool(readers) and all(reader is not None and reader.complete for reader in readers)
            if name == "stderr_flood":
                tunnel_reader = readers[0] if readers else None
                checks["stderr_tunnel_bytes_drained"] = (
                    tunnel_reader is not None and tunnel_reader.bytes_read == 3 * 65536 + len(b"synthetic_transport_secret") + 64
                )
                checks["stderr_tunnel_unknown_values_withheld"] = tunnel_reader is not None and tunnel_reader.path.read_bytes() == b"[redacted stderr]\n"
    row = {
        "case": name,
        "failure_code": failure_code,
        "checks": checks,
        "elapsed_s": time.monotonic() - started,
        "groups": [{"pid": handle.pid, "pgid": handle.pgid, "state": group_state(handle.pgid)} for handle in transport.handles],
        "registry": str(scratch / "registry"),
        "cleanup": "done" if confirmed and absent else "unconfirmed",
    }
    if row["cleanup"] != "done":
        raise TransportCleanupUnconfirmed(row)
    return row


def mutation_checks(folder: Path) -> list[dict[str, Any]]:
    """Require each harmless planted behavioral defect to fail its intended local case."""
    rows = []
    for case in ("relay_death", "lifetime_expiry"):
        with patch.object(CatalogTransport, "check", return_value=None):
            observed = run_case(case, folder)
        assertion = "check_refuses_dead_relay" if case == "relay_death" else "check_refuses_expired_transport"
        rows.append({"mutation": "check_never_refuses", "case": case, "caught": observed["checks"].get(assertion) is False, "observed": observed})

    def repeat(transport: CatalogTransport, operation: Callable[[], Any], *, deadline: float) -> Any:
        operation()
        return operation()

    with patch.object(CatalogTransport, "execute", repeat):
        observed = run_case("normal", folder)
    rows.append(
        {"mutation": "execute_repeats_operation", "case": "normal", "caught": observed["checks"].get("execute_runs_once") is False, "observed": observed}
    )

    def retry_failure(transport: CatalogTransport, operation: Callable[[], Any], *, deadline: float) -> Any:
        try:
            return operation()
        except (ConnectionError, TimeoutError, http.client.RemoteDisconnected):
            return operation()

    with patch.object(CatalogTransport, "execute", retry_failure):
        observed = run_case("dead_target", folder)
    rows.append(
        {
            "mutation": "execute_retries_after_failure",
            "case": "dead_target",
            "caught": observed["checks"].get("dead_target_operation_runs_once") is False,
            "observed": observed,
        }
    )
    return rows


def _route_fixture(folder: Path, name: str) -> tuple[TransportSpec, Path]:
    events = folder / "events.jsonl"
    config = folder / "standin.json"
    config.write_text(
        json.dumps(
            {
                "events": str(events),
                "version": "1.2.834.0" if name == "plugin_mismatch" else "1.2.835.0",
                "plugin_exit": 69 if name == "plugin_failure" else 0,
                "mode": "not_ready" if name.startswith("readiness_") else name,
            }
        )
        + "\n"
    )
    standin = Path(__file__).with_name("catalog_transport_standin.py").resolve()
    for executable, role in (("session-manager-plugin", "plugin"), ("aws", "tunnel")):
        wrapper = folder / executable
        command = [sys.executable, "-I", "-B", str(standin), "--config", str(config), "--role", role, "--"]
        sleeper = ""
        if executable == "aws" and name == "tunnel_body_exit":
            sleeper_command = [sys.executable, "-I", "-B", str(standin), "--config", str(config), "--role", "sleeper"]
            sleeper = shlex.join(sleeper_command) + " &\n"
        wrapper.write_text("#!/bin/sh\n" + sleeper + "exec " + shlex.join(command) + ' "$@"\n')
        wrapper.chmod(0o700)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as target, socket.socket(socket.AF_INET, socket.SOCK_STREAM) as relay:
        target.bind(("127.0.0.1", 0))
        relay.bind(("127.0.0.1", 0))
        ports = target.getsockname()[1], relay.getsockname()[1]
    return TransportSpec("i-00000000", "synthetic", "synthetic", *ports), events


def _events(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _writer(endpoint: str) -> bytes:
    connection = http.client.HTTPConnection("127.0.0.1", urlsplit(endpoint).port, timeout=1)
    try:
        connection.request("POST", "/synthetic_writer", body=b"")
        return connection.getresponse().read()
    finally:
        connection.close()


def _all_gone(transport: CatalogTransport) -> bool:
    return all(group_state(handle.pgid) == "gone" for handle in transport.handles)


def _crashed_owner(folder: Path, spec: TransportSpec, environment: dict[str, str]) -> dict[str, bool]:
    state = folder / "owner_ready.json"
    registry = folder / "crash_registry"
    code = """
import json, os, sys, time
from pathlib import Path
from orchestration.catalog_transport import CatalogTransport, TransportSpec
from scripts.orchestration.catalog_transport_cases import _request
registry, state = map(Path, sys.argv[1:3])
spec = TransportSpec(**json.loads(sys.argv[3]))
transport = CatalogTransport(registry, session="synthetic_crash_owner")
until = time.monotonic() + 7
endpoint = transport.start(spec, deadline=until, ready_deadline=until - 3)
body = transport.execute(lambda: _request(endpoint), deadline=until)
state.write_text(json.dumps({"entries": transport.ledger.open_entries(deadline=until), "reply": body.decode(), "lifetime": until}))
os._exit(73)
"""
    fields = {key: getattr(spec, key) for key in ("instance", "profile", "region", "tunnel_port", "relay_port")}
    result = run_command(sys.executable, ["-B", "-c", code, str(registry), str(state), json.dumps(fields)], env=environment, timeout=12)
    checks = {"owner_exits_at_planted_crash": result.returncode == 73, "owner_was_ready_before_crash": state.exists()}
    if not state.exists():
        records = WorkerLedger(registry).open_entries(deadline=time.monotonic() + 2)
        lifetime = time.monotonic() + 8
    else:
        saved = json.loads(state.read_text())
        records = saved["entries"]
        lifetime = saved["lifetime"]
        checks["valid_route_reply_precedes_crash"] = saved["reply"].encode() == BODY
        checks["crash_saved_both_owned_groups"] = len(records) == 2
    until = min(lifetime - 1, time.monotonic() + 2)
    while any(group_state(row["pgid"]) != "gone" for row in records) and time.monotonic() < until:
        time.sleep(min(0.05, max(0.0, until - time.monotonic())))
    checks["crash_ends_children_before_lifetime"] = bool(records) and all(group_state(row["pgid"]) == "gone" for row in records)
    until = max(time.monotonic(), lifetime) + 3
    while any(group_state(row["pgid"]) != "gone" for row in records) and time.monotonic() < until:
        time.sleep(min(0.05, max(0.0, until - time.monotonic())))
    checks["crash_groups_absent"] = all(group_state(row["pgid"]) == "gone" for row in records)
    ledger = WorkerLedger(registry)
    recovery = Recovery(ledger, "synthetic_crash_recovery")
    _, observations = recovery.inspect(deadline=time.monotonic() + 2)
    first = ledger.observe(observations, deadline=time.monotonic() + 2)
    checks["crash_first_recovery_stays_read_only"] = len(first) == 2 and all(row["state"] == "read_only" for row in first)
    _, observations = recovery.inspect(deadline=time.monotonic() + 2)
    checks["crash_later_current_absence_closes_records"] = not ledger.observe(observations, deadline=time.monotonic() + 2)
    return checks


def _startup_refusal(
    name: str, transport: CatalogTransport, spec: TransportSpec, events: Path, lifetime: float, readiness: float, started: float
) -> tuple[dict[str, bool], str | None]:
    checks: dict[str, bool] = {}
    with contextlib.ExitStack() as resources:
        if name == "readiness_scan_deadline":

            def expired_snapshot(deadline: float) -> Snapshot:
                while time.monotonic() < deadline:
                    time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
                return process_snapshot(deadline)

            resources.enter_context(patch("orchestration.catalog_transport.process_snapshot", side_effect=expired_snapshot))
        occupied = None
        if name == "relay_start_failure":
            occupied = resources.enter_context(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            occupied.bind(("127.0.0.1", spec.relay_port))
            occupied.listen(1)
        refusal = None
        try:
            transport.start(spec, deadline=lifetime, ready_deadline=readiness)
        except (OSError, EOFError, RuntimeError, TimeoutError) as error:
            refusal = str(error) if isinstance(error, (RuntimeError, TimeoutError)) else type(error).__name__
        if name.startswith("plugin_"):
            checks["plugin_refuses_exact_version_gate"] = refusal == "session_manager_plugin_version_requires_review"
            checks["plugin_refusal_starts_no_owned_child"] = not transport.handles
            checks["plugin_refusal_runs_only_version_command"] = _events(events) == [{"kind": "plugin", "arguments": ["--version"]}]
        elif name.startswith("readiness_"):
            checks["readiness_has_intended_deadline_error"] = refusal == "catalog_transport_readiness_deadline"
            checks["readiness_starts_only_tunnel"] = len(transport.handles) == 1
            checks["readiness_cleanup_finishes_before_return"] = bool(transport.handles) and _all_gone(transport)
            checks["readiness_return_is_bounded"] = time.monotonic() - started < 7
        else:
            checks["relay_bind_failure_is_visible"] = refusal == "EOFError"
            checks["relay_failure_owns_both_children"] = len(transport.handles) == 2
            checks["relay_failure_cleanup_finishes_before_return"] = bool(transport.handles) and _all_gone(transport)
            checks["occupied_socket_remains_listener"] = False
            if occupied is not None:
                occupied.settimeout(0.5)
                with socket.create_connection(("127.0.0.1", spec.relay_port), timeout=0.5) as probe:
                    accepted, address = occupied.accept()
                    with accepted:
                        checks["occupied_socket_remains_listener"] = address == probe.getsockname() and accepted.getsockname() == ("127.0.0.1", spec.relay_port)
            bind_error = None
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender:
                try:
                    contender.bind(("127.0.0.1", spec.relay_port))
                except OSError as error:
                    bind_error = error.errno
            reader = transport.handles[1].stderr_reader if len(transport.handles) == 2 else None
            checks["relay_bind_has_exact_occupied_port_cause"] = (
                bind_error == errno.EADDRINUSE
                and reader is not None
                and reader.complete
                and reader.path.read_bytes() == b"catalog_relay_bind_in_use\n"
                and not transport.endpoint
                and len(transport.handles) == 2
                and not transport._owned_listener(transport.handles[1], spec.relay_port, time.monotonic() + 1)
            )
        checks["startup_failure_keeps_dispatch_paused"] = transport.failed and transport.dispatch_paused
    return checks, refusal


def _normal_route(transport: CatalogTransport, endpoint: str, lifetime: float) -> dict[str, bool]:
    checks = {"full_route_reply": transport.execute(lambda: _request(endpoint), deadline=lifetime) == BODY}
    checks["normal_close_confirmed"] = transport.close(deadline=time.monotonic() + 5)
    before = transport.ledger.redacted(deadline=time.monotonic() + 2)
    checks["normal_closes_both_ledger_entries"] = len(transport.ledger.closed_events(deadline=time.monotonic() + 2)) == 2
    checks["normal_second_close_confirmed"] = transport.close(deadline=time.monotonic() + 5)
    checks["normal_second_close_has_no_extra_ledger_effect"] = transport.ledger.redacted(deadline=time.monotonic() + 2) == before
    return checks


def _lifetime_route(transport: CatalogTransport, lifetime: float) -> dict[str, bool]:
    while time.monotonic() < lifetime + 0.5:
        time.sleep(min(0.05, max(0.0, lifetime + 0.5 - time.monotonic())))
    snapshot = process_snapshot(time.monotonic() + 2)
    checks: dict[str, bool] = {}
    checks["lifetime_children_end_without_owner_close"] = snapshot.status == "seen" and all(
        all(member.state.startswith("Z") for member in snapshot.group(handle.pgid)) for handle in transport.handles
    )
    checks["lifetime_refuses_later_request"] = _unavailable(transport)
    checks["lifetime_cleanup_finishes_before_return"] = _all_gone(transport)
    checks["lifetime_failure_keeps_dispatch_paused"] = transport.failed and transport.dispatch_paused
    return checks


def _request_failure(name: str, transport: CatalogTransport, endpoint: str, lifetime: float) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    count = 0

    def operation() -> bytes:
        nonlocal count
        count += 1
        if name == "tunnel_loss_after_reply":
            reply = _request(endpoint)
            stopped = stop_owned(transport.handles[0], time.monotonic() + 5)
            checks["tunnel_stopped_after_valid_reply"] = reply == BODY and stopped.confirmed
            return reply
        return _writer(endpoint)

    request_error = None
    try:
        transport.execute(operation, deadline=time.monotonic() - 0.01 if name == "request_deadline" else lifetime)
    except (OSError, RuntimeError, http.client.HTTPException) as error:
        request_error = str(error) if isinstance(error, RuntimeError) else type(error).__name__
    checks["dependent_request_fails_visibly"] = request_error is not None
    expected_errors = (
        {"catalog_transport_unavailable"} if name in {"request_deadline", "tunnel_loss_after_reply"} else {"RemoteDisconnected", "ConnectionResetError"}
    )
    checks["dependent_failure_has_intended_cause"] = request_error in expected_errors
    checks["failed_request_cleanup_finishes_before_return"] = _all_gone(transport)
    checks["failed_request_keeps_dispatch_paused"] = transport.failed and transport.dispatch_paused
    expected = 0 if name == "request_deadline" else 1
    checks["dependent_operation_runs_expected_times"] = count == expected
    second_error = None
    try:
        transport.execute(operation, deadline=time.monotonic() + 2)
    except (OSError, RuntimeError, http.client.HTTPException) as error:
        second_error = str(error) if isinstance(error, RuntimeError) else type(error).__name__
    checks["second_execute_refuses_retired_transport"] = second_error == "catalog_transport_unavailable"
    checks["unknown_operation_is_never_replayed"] = count == expected
    return checks


def _cleanup_retry(name: str, transport: CatalogTransport, endpoint: str, lifetime: float) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    reader = transport.handles[-1].stderr_reader
    if reader is None:
        raise RuntimeError("cleanup_case_requires_real_stderr_reader")
    finish = reader.finish
    finish_calls = 0

    def incomplete_once(*, deadline: float) -> bool:
        nonlocal finish_calls
        finish_calls += 1
        completed = finish(deadline=deadline)
        checks["real_drain_finishes_before_substituted_failure"] = completed
        return False

    operational_failure = name == "failed_cleanup_retry"
    original = RuntimeError("synthetic_unknown_writer_outcome")
    received = None
    with patch.object(reader, "finish", side_effect=incomplete_once):
        if operational_failure:

            def operation() -> bytes:
                checks["writer_reaches_route_before_failure"] = _writer(endpoint) == BODY
                raise original

            try:
                transport.execute(operation, deadline=lifetime)
            except RuntimeError as error:
                received = error
            checks["callback_error_is_preserved_during_failed_cleanup"] = received is original
        else:
            checks["cleanup_route_serves_before_close"] = transport.execute(lambda: _request(endpoint), deadline=lifetime) == BODY
            checks["first_close_reports_unconfirmed_drain"] = transport.close(deadline=time.monotonic() + 5) is False
    checks["cleanup_failure_reaches_exact_drain_boundary"] = (
        finish_calls == 1
        and _all_gone(transport)
        and not transport.survivors
        and all(handle.stderr_reader is not None and handle.stderr_reader.complete for handle in transport.handles)
    )
    checks["cleanup_failure_latches_pause"] = transport.dispatch_paused
    checks["cleanup_preserves_operational_failure_state"] = transport.failed is operational_failure
    checks["cleanup_retry_confirms"] = transport.close(deadline=time.monotonic() + 5)
    checks["cleanup_retry_clears_only_cleanup_pause"] = transport.dispatch_paused is operational_failure
    checks["cleanup_retry_preserves_operational_failure"] = transport.failed is operational_failure
    return checks


def _late_callback(transport: CatalogTransport, endpoint: str, lifetime: float) -> dict[str, bool]:
    request_deadline = time.monotonic() + 0.5
    checks: dict[str, bool] = {}

    def operation() -> bytes:
        reply = _request(endpoint)
        while time.monotonic() < request_deadline + 0.05:
            time.sleep(min(0.02, max(0.0, request_deadline + 0.05 - time.monotonic())))
        checks["successful_callback_finishes_after_request_deadline"] = request_deadline < time.monotonic() < lifetime
        return reply

    reply = None
    try:
        reply = transport.execute(operation, deadline=request_deadline)
    except RuntimeError as error:
        checks["late_callback_error_is_deadline_refusal"] = str(error) == "catalog_transport_unavailable"
    checks["late_success_keeps_callback_result"] = reply == BODY
    checks["late_success_keeps_healthy_route"] = not transport.failed and not transport.dispatch_paused and bool(transport.endpoint)
    return checks


def _tunnel_body_exit(transport: CatalogTransport, endpoint: str, events: Path, lifetime: float) -> dict[str, bool]:
    tunnel = transport.handles[0]
    until = min(lifetime - 5, time.monotonic() + 2)
    sleepers = [row for row in _events(events) if row["kind"] == "sleeper_ready"]
    while not sleepers and time.monotonic() < until:
        time.sleep(min(0.02, max(0.0, until - time.monotonic())))
        sleepers = [row for row in _events(events) if row["kind"] == "sleeper_ready"]
    before = process_snapshot(until)
    checks = {
        "same_group_sleeper_is_live_before_tunnel_exit": bool(sleepers)
        and before.status == "seen"
        and any(
            row["pgid"] == tunnel.pgid and any(member.pid == row["pid"] and not member.state.startswith("Z") for member in before.group(tunnel.pgid))
            for row in sleepers
        )
    }
    checks["tunnel_exit_follows_real_reply"] = _request(endpoint) == BODY
    until = min(lifetime - 3, time.monotonic() + 2)
    while time.monotonic() < until:
        snapshot = process_snapshot(until)
        if snapshot.status == "seen" and all(member.state.startswith("Z") for member in snapshot.group(tunnel.pgid)):
            break
        time.sleep(min(0.02, max(0.0, until - time.monotonic())))
    after = process_snapshot(time.monotonic() + 1)
    checks["tunnel_body_exited_before_lifetime"] = any(row["kind"] == "tunnel_body_exit" for row in _events(events)) and time.monotonic() < lifetime - 2
    checks["body_exit_stops_sleeper_without_owner_close"] = after.status == "seen" and all(member.state.startswith("Z") for member in after.group(tunnel.pgid))
    checks["owner_close_has_not_run"] = not transport._closing and all(handle.state == "signalling" for handle in transport.handles)
    return checks


def _route_case(name: str, folder: Path) -> dict[str, Any]:
    scratch = Path(tempfile.mkdtemp(prefix=f"route_{name}_", dir=folder))
    started = time.monotonic()
    checks: dict[str, bool] = {}
    spec, events = _route_fixture(scratch, name)
    environment = {key: os.environ[key] for key in ("PATH", "HOME", "TMPDIR") if key in os.environ}
    environment.update(PATH=f"{scratch}{os.pathsep}{os.environ.get('PATH', os.defpath)}", PYTHONDONTWRITEBYTECODE="1")
    transport = CatalogTransport(scratch / "registry")
    error_code = None
    with patch.dict(os.environ, environment):
        try:
            if name == "parent_crash":
                checks.update(_crashed_owner(scratch, spec, environment))
            else:
                lifetime = started + (1.5 if name.startswith("readiness_") else 4 if name == "full_lifetime" else 12)
                readiness = lifetime if name.startswith("readiness_") else min(lifetime - 1, time.monotonic() + 5)
                if name in {"plugin_mismatch", "plugin_failure", "readiness_deadline", "readiness_scan_deadline", "relay_start_failure"}:
                    checks, error_code = _startup_refusal(name, transport, spec, events, lifetime, readiness, started)
                else:
                    endpoint = transport.start(spec, deadline=lifetime, ready_deadline=readiness)
                    checks["both_route_children_owned"] = len(transport.handles) == 2
                    if name == "full_normal":
                        checks.update(_normal_route(transport, endpoint, lifetime))
                    elif name == "full_lifetime":
                        checks.update(_lifetime_route(transport, lifetime))
                    elif name == "tunnel_body_exit":
                        checks.update(_tunnel_body_exit(transport, endpoint, events, lifetime))
                    elif name in {"cleanup_retry", "failed_cleanup_retry"}:
                        checks.update(_cleanup_retry(name, transport, endpoint, lifetime))
                    elif name == "late_callback_success":
                        checks.update(_late_callback(transport, endpoint, lifetime))
                    else:
                        checks.update(_request_failure(name, transport, endpoint, lifetime))
        except (OSError, ValueError, EOFError, RuntimeError, TimeoutError, http.client.HTTPException) as error:
            error_code = type(error).__name__
            checks["unexpected_error"] = False
        finally:
            confirmed = transport.close(deadline=time.monotonic() + 5)
            absent = _all_gone(transport) and checks.get("crash_groups_absent", True)
            checks.update(cleanup_confirmed=confirmed, groups_absent=absent)
            checks["stderr_readers_complete"] = all(handle.stderr_reader is not None and handle.stderr_reader.complete for handle in transport.handles)
    observed = _events(events)
    checks["plugin_version_read_once"] = sum(row["kind"] == "plugin" for row in observed) == 1
    if name in {
        "full_normal",
        "unknown_writer",
        "tunnel_loss",
        "tunnel_loss_after_reply",
        "parent_crash",
        "tunnel_body_exit",
        "cleanup_retry",
        "failed_cleanup_retry",
        "late_callback_success",
    }:
        checks["peer_receives_exactly_one_request"] = sum(row["kind"] == "request" for row in observed) == 1
    if name == "request_deadline":
        checks["expired_deadline_sends_no_request"] = not any(row["kind"] == "request" for row in observed)
    row = {
        "case": name,
        "checks": checks,
        "failure_code": error_code,
        "elapsed_s": time.monotonic() - started,
        "events": observed,
        "groups": [{"pid": handle.pid, "pgid": handle.pgid, "state": group_state(handle.pgid)} for handle in transport.handles],
        "registry": str(scratch / "registry"),
        "cleanup": "done" if confirmed and absent else "unconfirmed",
    }
    if row["cleanup"] != "done":
        raise TransportCleanupUnconfirmed(row)
    return row


def main() -> int:
    """Save focused loopback lifecycle results without AWS, containers or a launcher suite."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=(*CASES, "all"), default="all")
    parser.add_argument("--mutations", action="store_true")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    folder = Path(tempfile.mkdtemp(prefix="catalog_cases_", dir=args.output.parent))
    (folder / "retention.json").write_text(
        json.dumps(
            {
                "class": 3,
                "purpose": "Synthetic local relay lifecycle payload",
                "created": datetime.now(UTC).isoformat(),
                "end_condition": "The relay lifecycle evidence and its review are accepted",
            },
            indent=2,
        )
        + "\n"
    )
    sources = (
        Path(__file__),
        Path(__file__).with_name("catalog_transport_standin.py"),
        Path("orchestration/catalog_transport.py"),
        Path("orchestration/catalog_transport_child.py"),
        Path("scripts/process_group.py"),
        Path("scripts/process.py"),
        Path("data_contracts/orchestration/sdk_worker.schema.json"),
        Path("orchestration/worker_recovery.py"),
        Path("orchestration/worker_protocol.py"),
        Path("orchestration/worker_stderr.py"),
        Path("services/hai/deadline_run.py"),
    )
    frozen = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    results = []
    mutations: list[dict[str, Any]] = []
    try:
        for case in CASES if args.case == "all" else (args.case,):
            results.append(run_case(case, folder))
        if args.mutations:
            mutations = mutation_checks(folder)
    except TransportCleanupUnconfirmed as error:
        results.append(error.row)
    unchanged = all(hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest for path, digest in frozen.items())
    checks = {f"{row['case']}:{name}": ok for row in results for name, ok in row["checks"].items()}
    checks.update({f"mutation:{row['mutation']}:{row['case']}": row["caught"] for row in mutations})
    checks["sources_unchanged"] = unchanged
    cleanup = all(row["cleanup"] == "done" for row in results) and all(row["observed"]["cleanup"] == "done" for row in mutations)
    report = {
        "classification": "Synthetic",
        "written_utc": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-B", "-m", "scripts.orchestration.catalog_transport_cases", *sys.argv[1:]],
        "source_sha256": frozen,
        "checks": checks,
        "cases": results,
        "mutations": mutations,
        "cleanup": "done" if cleanup else "unconfirmed",
        "limits": ["Focused local relay lifecycle only", "No Session Manager session, class-network or integrated launcher proof"],
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    failed = [name for name, ok in checks.items() if not ok]
    sys.stdout.write(json.dumps({"failed": failed, "report": str(args.output), "cleanup": report["cleanup"]}) + "\n")
    return int(bool(failed) or not cleanup)


if __name__ == "__main__":
    sys.exit(main())
