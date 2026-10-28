"""Exercise bounded local relay failures. Class-network and Session Manager acceptance stay separate."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
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
from scripts.orchestration.sdk_standins import Scenario, WorkerStandin
from scripts.process import CompletedProcess, run_command
from scripts.process_group import group_state, stop_owned

CASES = ("normal", "relay_death", "lifetime_expiry", "dead_target", "stderr_flood")
BODY = b'{"catalog":"synthetic"}'


class TransportCleanupUnconfirmed(RuntimeError):
    """Stop the local case queue and retain all payloads after unconfirmed cleanup."""

    def __init__(self, row: dict[str, Any]) -> None:
        super().__init__("catalog_case_cleanup_unconfirmed")
        self.row = row


def _request(endpoint: str) -> bytes:
    connection = http.client.HTTPConnection("127.0.0.1", urlsplit(endpoint).port, timeout=1)
    try:
        connection.request("GET", "/")
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
    scratch = Path(tempfile.mkdtemp(prefix=f"catalog_{name}_", dir=folder))
    started = time.monotonic()
    checks: dict[str, bool] = {}
    failure_code = None
    transport = CatalogTransport(scratch / "registry")
    with WorkerStandin(Scenario("catalog", "reset" if name == "dead_target" else "normal", BODY, lifetime_s=12)) as peer:
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
    sources = (Path(__file__), Path("orchestration/catalog_transport.py"), Path("orchestration/catalog_transport_child.py"), Path("scripts/process_group.py"))
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
