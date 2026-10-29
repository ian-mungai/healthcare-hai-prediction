"""Exercise the real worker facade against synthetic HTTP faults and native process ownership."""

from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import http.client
import json
import os
import secrets
import sys
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import urlsplit

from scripts.orchestration.sdk_standins import Scenario, WorkerStandin
from scripts.process_group import Member, Snapshot, process_snapshot

ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATHS = (
    "orchestration/ecs_api.py",
    "orchestration/worker_pool.py",
    "orchestration/worker_protocol.py",
    "orchestration/worker_stderr.py",
    "orchestration/worker_recovery.py",
    "orchestration/sdk_worker.py",
    "orchestration/catalog_transport.py",
    "orchestration/catalog_transport_child.py",
    "scripts/process_group.py",
    "services/hai/bootstrap/supervisor.py",
)
REAPERS = frozenset({"wait", "waitpid", "waitid", "wait3", "wait4"})
SIGNAL_FUNCTIONS = frozenset({"kill", "killpg", "signal_group", "signal_owned", "waitpid"})
SLOW_MODES = ("delay_headers", "drip_headers", "hang_body", "drip_body")


def _without_foreign_workers(snapshot: Snapshot) -> Snapshot:
    """Drop workers whose marker names another spawner, because a recovery sweep adopts each unrecorded one as an unproven group and pauses dispatch."""
    return replace(snapshot, members=tuple(member for member in snapshot.members if member.marker_parent in {None, os.getpid()}))


def _own_workers_snapshot(deadline: float, witness: str | None = None, witnesses: Mapping[str, tuple[int, str]] | None = None) -> Snapshot:
    """Read the real process table as this run's recovery sweeps see it: its own workers only, never a neighbor run's live ones."""
    return _without_foreign_workers(process_snapshot(deadline, witness, witnesses))


def _call_name(call: ast.Call, aliases: dict[str, str]) -> str:
    if isinstance(call.func, ast.Name):
        return aliases.get(call.func.id, call.func.id)
    return call.func.attr if isinstance(call.func, ast.Attribute) else ""


def _function_name(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str | None:
    parent = parents.get(node)
    while parent is not None:
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return parent.name
        parent = parents.get(parent)
    return None


def source_checks(root: Path = ROOT) -> dict[str, bool]:
    """Check source contracts across every orchestration module in the staged worker increment."""
    checks: dict[str, bool] = {}
    trees = {path: ast.parse((root / path).read_text()) for path in SOURCE_PATHS}
    api = trees["orchestration/ecs_api.py"]
    imports = {node.module or "" for node in ast.walk(api) if isinstance(node, ast.ImportFrom)}
    imports |= {item.name for node in ast.walk(api) if isinstance(node, ast.Import) for item in node.names}
    checks["facade_has_no_sdk_or_async_call_path"] = not any(
        name == forbidden or name.startswith(forbidden + ".") for name in imports for forbidden in ("boto3", "threading", "concurrent.futures", "asyncio")
    )
    for path, tree in trees.items():
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        aliases = {alias.asname or alias.name: alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names}
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
        allowed_reaps = True
        for call in calls:
            if _call_name(call, aliases) not in REAPERS:
                continue
            name = ast.unparse(call.func)
            enclosing_function = _function_name(call, parents)
            condition_wait = path == "orchestration/worker_pool.py" and name == "self.condition.wait"
            owned_reap = (
                path in {"scripts/process_group.py", "services/hai/bootstrap/supervisor.py"}
                and enclosing_function == "stop_owned"
                and name == "os.waitpid"
                and len(call.args) == 2
                and ast.unparse(call.args[0]) == "handle.pid"
                and ast.unparse(call.args[1]) == "os.WNOHANG"
            )
            tunnel_child_reap = (
                path == "orchestration/catalog_transport_child.py"
                and enclosing_function == "_tunnel"
                and name == "os.waitpid"
                and len(call.args) == 2
                and ast.unparse(call.args[0]) == "child"
                and ast.unparse(call.args[1]) == "os.WNOHANG"
            )
            allowed_reaps = allowed_reaps and (condition_wait or owned_reap or tunnel_child_reap)
        checks[f"{path}:no_catch_all_reaper"] = allowed_reaps
        checks[f"{path}:signal_imports_closed"] = not any(
            alias.name in SIGNAL_FUNCTIONS
            and not (path in {"scripts/process_group.py", "services/hai/bootstrap/supervisor.py"} and _function_name(node, parents) == "stop_owned")
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        )
        signals_allowed = True
        for call in calls:
            name = _call_name(call, aliases)
            if name not in SIGNAL_FUNCTIONS - {"waitpid"}:
                continue
            signal_enclosing = _function_name(call, parents)
            owned_stop = path in {"scripts/process_group.py", "services/hai/bootstrap/supervisor.py"} and signal_enclosing == "stop_owned"
            self_monitor = (
                (path == "orchestration/sdk_worker.py" and signal_enclosing in {"run", "_monitor"})
                or (path == "orchestration/catalog_transport_child.py" and signal_enclosing in {"run", "_monitor", "main"})
            ) and ast.unparse(call) == "os.killpg(os.getpgrp(), signal.SIGKILL)"
            signals_allowed = signals_allowed and (owned_stop or self_monitor)
        checks[f"{path}:signals_only_in_owned_stop_or_self_monitor"] = signals_allowed
    for path in ("scripts/process_group.py", "services/hai/bootstrap/supervisor.py"):
        tree = trees[path]
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        guarded = True
        aliases = {alias.asname or alias.name: alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names}
        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call) and _call_name(node, aliases) in SIGNAL_FUNCTIONS):
            chain = []
            ancestor = parents.get(call)
            while ancestor is not None:
                chain.append(ancestor)
                ancestor = parents.get(ancestor)
            guarded = guarded and any(
                isinstance(node, ast.With) and any(ast.unparse(item.context_expr) == "handle.lock" for item in node.items) for node in chain
            )
            guarded = guarded and next((node.name for node in chain if isinstance(node, ast.FunctionDef)), None) == "stop_owned"
        checks[f"{path}:signals_and_reaps_hold_handle_lock"] = guarded
        stop = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "stop_owned")
        checks[f"{path}:reaped_state_refuses_signals"] = any(
            isinstance(node, ast.If) and ast.unparse(node.test) == "handle.state == 'read_only'" and isinstance(node.body[0], ast.Return)
            for node in ast.walk(stop)
        )
        reapers = []
        for function in (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)):
            if any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "waitpid" for call in ast.walk(function)):
                reapers.append(function.name)
        checks[f"{path}:only_guarded_stop_reaps"] = set(reapers) <= {"stop_owned"}
    group = trees["scripts/process_group.py"]
    helpers = {node.attr for node in ast.walk(group) if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "_portable"}
    checks["portable_helper_names_closed"] = helpers == {"group_state", "signal_group", "wait_group_gone"}
    recovery = trees["orchestration/worker_recovery.py"]
    recovery_aliases = {alias.asname or alias.name: alias.name for node in ast.walk(recovery) if isinstance(node, ast.ImportFrom) for alias in node.names}
    checks["recovery:signal_imports_closed"] = not any(
        alias.name in SIGNAL_FUNCTIONS for node in ast.walk(recovery) if isinstance(node, ast.ImportFrom) for alias in node.names
    )
    for function in (node for node in ast.walk(recovery) if isinstance(node, ast.FunctionDef)):
        checks[f"recovery:{function.name}:no_signal_or_reap"] = not any(
            isinstance(node, ast.Call) and _call_name(node, recovery_aliases) in SIGNAL_FUNCTIONS for node in ast.walk(function)
        )
    return checks


class NativeCleanupUnconfirmed(RuntimeError):
    """Stop the native queue and retain its owned files after an unresolved cleanup."""

    def __init__(self, registry: Path, cleanup: dict[str, Any]) -> None:
        super().__init__("native_worker_cleanup_unconfirmed")
        self.record = {"registry": str(registry), "cleanup": cleanup}


@contextlib.contextmanager
def _owned_api(config: Any, registry: Path) -> Iterator[Any]:
    from orchestration.ecs_api import EcsApi

    api = EcsApi(config, registry)
    try:
        yield api
    finally:
        cleanup = api.close(deadline=time.monotonic() + 10)
        if not cleanup["confirmed"]:
            raise NativeCleanupUnconfirmed(registry, cleanup)


def _request_fixture(root: Path, operation: str) -> dict[str, Any]:
    examples = json.loads((root / "data_contracts/orchestration/examples/sdk_worker.json").read_text())
    params: dict[str, Any] = next(row["request"]["params"] for row in examples["positive"] if row["op"] == operation)
    if operation == "assume_role":
        params["role_arn"] = "arn:aws:iam::" + "0" * 12 + ":role/synthetic_worker"
    if operation == "s3_put_small":
        params["sha256"] = hashlib.sha256(params["body_utf8"].encode()).hexdigest()
    if "session" in params:
        params["session"]["expiration_utc"] = "2099-01-01T00:00:00Z"
    return params


def _check_absence(api: Any) -> bool:
    from scripts.process_group import group_state

    return all(handle.state == "read_only" and group_state(handle.pgid) == "gone" for handle in api.handles)


def _deadline_params(operation: str, path: Path) -> dict[str, Any]:
    params = _request_fixture(ROOT, operation)
    if operation == "s3_download":
        params["temp_path"] = str(path / "download.partial")
    if operation == "s3_upload_part":
        payload = path / "upload.bin"
        payload.write_bytes(b"synthetic_part")
        status = payload.stat()
        params.update(
            path=str(payload),
            offset=0,
            length=status.st_size,
            file_identity={"size": status.st_size, "mtime_ns": status.st_mtime_ns, "inode": status.st_ino},
        )
    return params


def _startup_probe_s(folder: Path, operation: str) -> float:
    """Measure one unfaulted call, whose spawn, connect and reply time is the cost any deadline budget must cover before a stall can begin."""
    from orchestration.ecs_api import WorkerConfig

    path = Path(tempfile.mkdtemp(prefix="sdk_probe_", dir=folder))
    params = _deadline_params(operation, path)
    with (
        WorkerStandin(Scenario(operation, "normal", lifetime_s=15)) as standin,
        _owned_api(WorkerConfig(region="us-west-2", test_endpoint=standin.endpoint), path / "registry") as api,
    ):
        started = time.monotonic()
        api.call(operation, params, deadline=started + 30)
        return time.monotonic() - started


def deadline_cases(folder: Path, operations: tuple[str, ...]) -> list[dict[str, Any]]:
    """Keep absolute deadlines armed through all HTTP response stages.

    Each budget is a 0.8 second stall window plus triple the slowest unfaulted call measured so far, so the request reaches the stand-in first.
    """
    from orchestration.ecs_api import WorkerConfig

    results: list[dict[str, Any]] = []
    startup_s = 0.0
    for operation in operations:
        startup_s = max(startup_s, _startup_probe_s(folder, operation))
        for mode in SLOW_MODES:
            scratch = tempfile.mkdtemp(prefix="sdk_deadline_", dir=folder)
            path = Path(scratch)
            params = _deadline_params(operation, path)
            with (
                WorkerStandin(Scenario(operation, mode, lifetime_s=15, drip_interval_s=0.05)) as standin,
                _owned_api(WorkerConfig(region="us-west-2", test_endpoint=standin.endpoint), path / "registry") as api,
            ):
                budget_s = 0.8 + 3 * startup_s
                started = time.monotonic()
                deadline = started + budget_s
                reply = api.call(operation, params, deadline=deadline)
                stopped = time.monotonic()
                shutdown = api.close(deadline=time.monotonic() + 10)
                until = time.monotonic() + 1
                while not any(event.kind == "peer_closed" for event in standin.events) and time.monotonic() < until:
                    time.sleep(0.02)
                header_only = operation == "s3_head" and mode in {"hang_body", "drip_body"}
                checks = {
                    "head_finishes_without_body" if header_only else "typed_deadline": reply.get("ok") is True
                    if header_only
                    else reply.get("error", {}).get("kind") == "deadline",
                    "absolute_bound": stopped <= deadline + 5.2,
                    "request_reached_http": sum(event.kind == "request" for event in standin.events) == 1,
                    "peer_closed": any(event.kind == "peer_closed" for event in standin.events),
                    "groups_absent": _check_absence(api),
                    "shutdown_confirmed": shutdown["confirmed"],
                    "partial_removed": not (path / "download.partial").exists(),
                }
                results.append(
                    {
                        "case": "absolute_call_deadline",
                        "operation": operation,
                        "mode": mode,
                        "checks": checks,
                        "budget_s": budget_s,
                        "reply_error": reply.get("error"),
                        "elapsed_s": stopped - started,
                        "events": [{"kind": event.kind, "connection": event.connection, "elapsed_s": event.elapsed_s} for event in standin.events],
                    }
                )
    return results


def retry_cases(folder: Path, operations: tuple[str, ...]) -> list[dict[str, Any]]:
    """Count real SDK HTTP requests without substituting the retry implementation."""
    from orchestration.ecs_api import WorkerConfig

    results = []
    for operation in operations:
        for fault in ("throttle", "server_error"):
            for pooled in (True, False):
                scratch = tempfile.mkdtemp(prefix="sdk_retry_", dir=folder)
                path = Path(scratch)
                params = _request_fixture(ROOT, operation)
                if operation == "s3_upload_part":
                    payload = path / "upload.bin"
                    payload.write_bytes(b"synthetic_part")
                    status = payload.stat()
                    params.update(
                        path=str(payload),
                        offset=0,
                        length=status.st_size,
                        file_identity={"size": status.st_size, "mtime_ns": status.st_mtime_ns, "inode": status.st_ino},
                    )
                if operation == "s3_download":
                    params["temp_path"] = str(path / "download.partial")
                with (
                    WorkerStandin(Scenario(operation, fault, lifetime_s=15)) as standin,
                    _owned_api(WorkerConfig(region="us-west-2", test_endpoint=standin.endpoint, pooled=pooled), path / "registry") as api,
                ):
                    reply = api.call(operation, params, deadline=time.monotonic() + 8)
                    shutdown = api.close(deadline=time.monotonic() + 10)
                    results.append(
                        {
                            "case": "single_http_attempt",
                            "operation": operation,
                            "fault": fault,
                            "pooled": pooled,
                            "checks": {
                                "service_error": reply.get("error", {}).get("kind") in {"throttle", "service"},
                                "one_request": sum(event.kind == "request" for event in standin.events) == 1,
                                "groups_absent": _check_absence(api),
                                "shutdown_confirmed": shutdown["confirmed"],
                            },
                            "reply_error": reply.get("error"),
                            "events": [{"kind": event.kind, "connection": event.connection, "elapsed_s": event.elapsed_s} for event in standin.events],
                        }
                    )
    return results


def pool_cases(folder: Path) -> list[dict[str, Any]]:
    """Verify reuse, one-shot parameter reads and deadline admission through the facade."""
    from orchestration.ecs_api import WorkerConfig

    results = []
    with (
        WorkerStandin(Scenario("describe_tasks", "normal", b'{"tasks":[],"failures":[]}')) as standin,
        _owned_api(WorkerConfig(region="us-west-2", test_endpoint=standin.endpoint), folder / "pool_registry") as api,
    ):
        try:
            first = api.call("describe_tasks", _request_fixture(ROOT, "describe_tasks"), deadline=time.monotonic() + 8)
            identities = [handle.pid for handle in api.handles]
            second = api.call("describe_tasks", _request_fixture(ROOT, "describe_tasks"), deadline=time.monotonic() + 8)
            results.append(
                {
                    "case": "normal_control_reuse",
                    "checks": {"success": first["ok"] and second["ok"], "same_worker": identities == [handle.pid for handle in api.handles]},
                }
            )
        finally:
            shutdown = api.close(deadline=time.monotonic() + 10)
        results.append({"case": "normal_pool_shutdown", "checks": {"confirmed": shutdown["confirmed"], "groups_absent": _check_absence(api)}})
    with (
        WorkerStandin(
            Scenario("get_parameter", "normal", b'{"Parameter":{"Name":"/synthetic/reader","Version":2,"Value":"synthetic_secret_value"}}')
        ) as standin,
        _owned_api(WorkerConfig(region="us-west-2", test_endpoint=standin.endpoint), folder / "parameter_registry") as api,
    ):
        try:
            first = api.call("get_parameter", {"name": "/synthetic/reader:2", "with_decryption": True}, deadline=time.monotonic() + 8)
            first_pid = api.handles[-1].pid
            second = api.call("get_parameter", {"name": "/synthetic/reader:2", "with_decryption": True}, deadline=time.monotonic() + 8)
            results.append(
                {
                    "case": "parameter_memory_only_one_shot",
                    "checks": {
                        "success": first["ok"] and second["ok"],
                        "fresh_worker": api.handles[-1].pid != first_pid,
                        "first_gone": api.handles[0].state == "read_only",
                        "no_saved_value": all(
                            b"synthetic_secret_value" not in item.read_bytes() for item in (folder / "parameter_registry").rglob("*") if item.is_file()
                        ),
                    },
                }
            )
        finally:
            api.close(deadline=time.monotonic() + 10)
    return results


def concurrency_cases(folder: Path) -> list[dict[str, Any]]:
    """Overlap three blocked data calls, an independent poll and aggregate shutdown."""
    from orchestration.ecs_api import WorkerConfig

    results = []
    with (
        WorkerStandin(Scenario("s3_get_small", "hang_body", lifetime_s=20)) as slow,
        WorkerStandin(Scenario("describe_tasks", "normal", b'{"tasks":[],"failures":[]}')) as control,
        _owned_api(
            WorkerConfig(region="us-west-2", test_endpoint=slow.endpoint, test_control_endpoint=control.endpoint), folder / "concurrent_registry"
        ) as api,
        ThreadPoolExecutor(max_workers=3) as executor,
    ):
        calls = [
            executor.submit(api.call, "s3_get_small", _request_fixture(ROOT, "s3_get_small"), deadline=time.monotonic() + 12, file_key=f"file_{index}")
            for index in range(3)
        ]
        until = time.monotonic() + 5
        while sum(event.kind == "request" for event in slow.events) < 3 and time.monotonic() < until:
            time.sleep(0.02)
        started = time.monotonic()
        reply = api.call("describe_tasks", _request_fixture(ROOT, "describe_tasks"), deadline=started + 4)
        results.append(
            {
                "case": "separate_control_capacity",
                "checks": {
                    "three_data_requests": sum(event.kind == "request" for event in slow.events) == 3,
                    "poll_succeeded": reply["ok"],
                    "poll_within_own_deadline": time.monotonic() < started + 4,
                    "live_cap": len(api.handles) <= 6,
                },
            }
        )
        started = time.monotonic()
        cleanup = api.close(deadline=started + 10)
        replies = [call.result(timeout=max(0.01, started + 10 - time.monotonic())) for call in calls]
        rejected = api.call("describe_tasks", _request_fixture(ROOT, "describe_tasks"), deadline=time.monotonic() + 1)
        results.append(
            {
                "case": "concurrent_cleanup",
                "checks": {
                    "within_ten_seconds": time.monotonic() <= started + 10.2,
                    "confirmed": cleanup["confirmed"],
                    "groups_absent": _check_absence(api),
                    "calls_interrupted": all(not item["ok"] for item in replies),
                    "new_calls_shutdown": rejected.get("error", {}).get("kind") == "shutdown",
                },
            }
        )
    return results


def recovery_cases(folder: Path) -> list[dict[str, Any]]:
    """Keep all missing evidence open and close only a later current absence observation."""
    from orchestration.worker_recovery import WorkerLedger

    results = []
    for group in ("present", "absent", "denied", "timed_out", "not_probed"):
        for token in ("seen", "not_seen", "denied", "timed_out"):
            ledger = WorkerLedger(folder / f"recovery_{group}_{token}")
            entry: dict[str, Any] = {
                "nonce": "synthetic_nonce",
                "pid": 2147483000,
                "pgid": 2147483000,
                "leader_start": "synthetic_start",
                "spawn_utc": "2026-10-23T00:00:00Z",
                "lane": "data",
                "session": "old_session",
                "witness": "synthetic_witness",
                "kind": "ledger",
            }
            ledger.append_spawn(entry, deadline=time.monotonic() + 1)
            evidence: dict[str, Any] = {"group": group, "token": token, "members": [], "scan_utc": "2026-10-23T00:00:01Z"}
            first = ledger.observe({entry["nonce"]: evidence}, deadline=time.monotonic() + 1)
            second = ledger.observe({entry["nonce"]: evidence}, deadline=time.monotonic() + 1)
            third_evidence = dict(evidence, group="denied")
            third = ledger.observe({entry["nonce"]: third_evidence}, deadline=time.monotonic() + 1)
            results.append(
                {
                    "case": "recovery_evidence",
                    "group": group,
                    "token": token,
                    "checks": {
                        "first_read_open": len(first) == 1,
                        "later_absence_only": len(second) == (0 if group == "absent" else 1),
                        "prior_absence_not_reused": len(third) == (0 if group == "absent" else 1),
                        "saved_copy_redacts_witness": "synthetic_witness" not in json.dumps(ledger.redacted(deadline=time.monotonic() + 1)),
                    },
                }
            )
    return results


def _probe_events(counted_header: str | None) -> tuple[int, list[str]]:
    """Send one plain GET from a non-SDK client and return the reply status and the stand-in's event kinds."""
    with WorkerStandin(Scenario("describe_tasks", "throttle", lifetime_s=15, counted_header=counted_header)) as standin:
        connection = http.client.HTTPConnection("127.0.0.1", urlsplit(standin.endpoint).port, timeout=5)
        try:
            connection.request("GET", "/", headers={"User-Agent": "node"})
            status = connection.getresponse().status
        finally:
            connection.close()
        return status, [event.kind for event in standin.events]


def stray_client_cases() -> list[dict[str, Any]]:
    """Count only the SDK client's requests, not a probe from another local program."""
    stray_status, stray_kinds = _probe_events("amz-sdk-invocation-id")
    counted_status, counted_kinds = _probe_events(None)
    return [
        {
            "case": "stray_client_not_counted",
            "checks": {
                "stray_recorded_apart": stray_kinds == ["stray_request"],
                "stray_refused": stray_status == 404,
                "unfiltered_stand_in_counts_it": counted_kinds[:1] == ["request"] and counted_status == 429,
            },
        }
    ]


def isolation_cases(folder: Path) -> list[dict[str, Any]]:
    """Keep a fresh session's recovery sweep from adopting a live worker that another process spawned."""
    from orchestration.ecs_api import EcsApi, WorkerConfig
    from scripts.process_group import spawn_owned, stop_owned

    scratch = Path(tempfile.mkdtemp(prefix="sdk_foreign_", dir=folder))
    script = scratch / "foreign_worker.py"
    script.write_text("import time\nuntil = time.monotonic() + 30\nwhile time.monotonic() < until:\n    time.sleep(0.1)\n")
    nonce, witness = secrets.token_hex(16), secrets.token_hex(32)
    command = [sys.executable, "-I", "-B", str(script), "--hai-worker", f"{os.getppid()}:{nonce}"]
    foreign = spawn_owned(
        command, {"HAI_WORKER_WITNESS": witness}, witness=witness, nonce=nonce, session="foreign", lane="data", owner="foreign", deadline=time.monotonic() + 5
    )
    try:
        with WorkerStandin(Scenario("run_task", "normal", lifetime_s=15)) as standin:
            api = EcsApi(WorkerConfig(region="us-west-2", test_endpoint=standin.endpoint), scratch / "registry")
            paused, alert = api.dispatch_paused, api.recovery.alert
            cleanup = api.close(deadline=time.monotonic() + 10)
    finally:
        stopped = stop_owned(foreign, time.monotonic() + 10)
    own, other = os.getpid(), os.getpid() + 1
    synthetic = Snapshot(
        "seen",
        (
            Member(10, 10, "S", "python", marker_parent=own, marker_nonce="a" * 32),
            Member(11, 11, "S", "python", marker_parent=other, marker_nonce="b" * 32),
            Member(12, 12, "S", "ps"),
        ),
    )
    return [
        {
            "case": "foreign_marker_filter",
            "checks": {"keeps_own_and_unmarked_members": [member.pid for member in _without_foreign_workers(synthetic).members] == [10, 12]},
        },
        {
            "case": "foreign_worker_isolation",
            "checks": {"fresh_session_dispatch_open": not paused, "own_cleanup_confirmed": cleanup["confirmed"], "foreign_stop_confirmed": stopped.confirmed},
            "alert_kind": alert["kind"] if alert else None,
        },
    ]


def parameter_cases(folder: Path) -> list[dict[str, Any]]:
    """Read every selected version through one-shot workers and reject a mismatched reply."""
    from orchestration.ecs_api import WorkerConfig

    with WorkerStandin(
        Scenario("get_parameter", "normal", b'{"Parameter":{"Name":"/synthetic/reader","Version":3,"Value":"synthetic_secret_value"}}')
    ) as standin:
        with _owned_api(WorkerConfig(region="us-west-2", test_endpoint=standin.endpoint), folder / "mismatched_parameter_registry") as api:
            try:
                reply = api.call("get_parameter", {"name": "/synthetic/reader:2", "with_decryption": True}, deadline=time.monotonic() + 8)
            finally:
                api.close(deadline=time.monotonic() + 10)
        return [
            {
                "case": "parameter_version_mismatch",
                "checks": {"refused": reply.get("error", {}).get("kind") == "protocol", "groups_absent": _check_absence(api)},
            }
        ]


def stderr_cases(folder: Path) -> list[dict[str, Any]]:
    """Drain actual worker pipes before a response and preserve the final sanitized byte."""
    from orchestration.ecs_api import WorkerConfig
    from scripts.process_group import spawn_owned

    rows = []
    for operation in ("describe_tasks", "get_parameter"):
        scratch = Path(tempfile.mkdtemp(prefix="sdk_stderr_", dir=folder))
        script = scratch / "worker.py"
        protocol_path = ROOT / "orchestration/worker_protocol.py"
        script.write_text(
            "import importlib.util, json, os, time\n"
            f"spec = importlib.util.spec_from_file_location('frames', {str(protocol_path)!r})\n"
            "protocol = importlib.util.module_from_spec(spec)\nspec.loader.exec_module(protocol)\n"
            "request = protocol.read_frame(0, time.monotonic() + 5)\n"
            f"with open({str(scratch / 'registry/workers.jsonl')!r}) as ledger: rows = [json.loads(line) for line in ledger]\n"
            "witness = next(row['spawn']['witness'] for row in rows if row['event'] == 'spawn' and row['spawn']['pid'] == os.getpid())\n"
            "body = b'BEGIN\\n' + b'x' * (3 * 65536) + witness.encode() + b':END'\n"
            "if request['op'] == 'get_parameter': body += b'synthetic_unknown_secret'\n"
            "while body:\n    size = os.write(2, body[:4096])\n    body = body[size:]\n"
            "result = {'tasks': [], 'failures': []} if request['op'] == 'describe_tasks' else "
            "{'name': '/synthetic/reader', 'version': 2, 'value': 'synthetic_unknown_secret'}\n"
            "protocol.write_frame(1, {'id': request['id'], 'ok': True, 'result': result}, time.monotonic() + 5)\n"
        )
        params = (
            {"cluster": "synthetic", "tasks": ["synthetic_task"], "include": ["TAGS"]}
            if operation == "describe_tasks"
            else {"name": "/synthetic/reader:2", "with_decryption": True}
        )
        with (
            patch("orchestration.worker_pool.spawn_owned", wraps=spawn_owned) as spawned,
            _owned_api(WorkerConfig("synthetic", worker=script.resolve(), test_endpoint="http://127.0.0.1:1"), scratch / "registry") as api,
        ):
            reply = api.call(operation, params, deadline=time.monotonic() + 4)
            cleaned = api.close(deadline=time.monotonic() + 5)
        environment = spawned.call_args.args[1] if spawned.call_args is not None else {}
        readers = [getattr(handle, "stderr_reader", None) for handle in api.handles]
        checks = {
            "stderr_flood_does_not_block_reply": reply.get("ok") is True,
            "stderr_cleanup_confirmed": cleaned["confirmed"] and _check_absence(api),
            "stderr_reader_owned": len(readers) == 1 and readers[0] is not None,
            "worker_aws_config_ignored": environment.get("AWS_CONFIG_FILE") == os.devnull,
            "worker_shared_credentials_ignored": environment.get("AWS_SHARED_CREDENTIALS_FILE") == os.devnull,
            "worker_metadata_disabled": environment.get("AWS_EC2_METADATA_DISABLED") == "true",
        }
        reader = readers[0] if readers else None
        evidence = reader.snapshot() if reader is not None else {}
        if reader is not None:
            data = reader.path.read_bytes()
            checks["stderr_every_byte_drained"] = evidence["bytes_read"] == len(b"BEGIN\n") + 3 * 65536 + 64 + len(b":END") + (
                len(b"synthetic_unknown_secret") if operation == "get_parameter" else 0
            )
            checks["stderr_reader_stopped"] = evidence["complete"] and not reader.is_alive
            checks["stderr_memory_bounded"] = evidence["max_buffer_bytes"] <= 2 * 65536
            checks["stderr_witness_absent"] = all(handle.witness.encode() not in data for handle in api.handles)
            if operation == "describe_tasks":
                checks["stderr_full_sanitized_output_preserved"] = data == b"BEGIN\n" + b"x" * (3 * 65536) + b"[redacted]:END"
            else:
                checks["stderr_unknown_secret_withheld"] = data == b"[redacted stderr]\n"
        rows.append({"case": f"stderr_{operation}", "checks": checks, "reader": evidence, "error_kind": reply.get("error", {}).get("kind")})
    return rows


def main() -> int:
    """Run a bounded native matrix, or source-only checks without starting any worker."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("static", "native"), required=True)
    parser.add_argument(
        "--cases",
        choices=(
            "all",
            "deadlines",
            "retries",
            "pool",
            "concurrency",
            "recovery",
            "stray_clients",
            "isolation",
            "parameters",
            "stderr",
            "reader_failures",
            "reader_idle",
            "stderr_cap",
            "executables",
        ),
        default="all",
    )
    parser.add_argument("--operations", nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    sources = (
        *SOURCE_PATHS,
        "scripts/process.py",
        "services/hai/deadline_run.py",
        "scripts/orchestration/sdk_standins.py",
        "scripts/orchestration/sdk_stderr_cases.py",
        __file__,
    )
    hashes = {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in sources}
    rows: list[dict[str, Any]] = [{"case": "source_contracts", "checks": source_checks()}]
    operations = tuple(
        args.operations or [row["op"] for row in json.loads((ROOT / "data_contracts/orchestration/examples/sdk_worker.json").read_text())["positive"]]
    )
    baseline_threads = {thread.ident for thread in threading.enumerate()}
    if args.mode == "native":
        args.output.parent.mkdir(parents=True, exist_ok=True)
        scratch = tempfile.mkdtemp(prefix="worker_cases_", dir=args.output.parent)
        folder = Path(scratch)
        (folder / "retention.json").write_text(
            json.dumps(
                {
                    "class": 3,
                    "purpose": "Native worker payload and private ownership evidence",
                    "created": datetime.now(UTC).isoformat(),
                    "end_condition": "Native acceptance and the required review complete",
                },
                indent=2,
            )
            + "\n"
        )
        isolated_sweeps = patch("orchestration.worker_recovery.process_snapshot", new=_own_workers_snapshot)
        isolated_sweeps.start()
        try:
            for name, function in (("deadlines", deadline_cases), ("retries", retry_cases)):
                if args.cases in {"all", name}:
                    rows.extend(function(folder, operations))
            if args.cases in {"all", "stray_clients"}:
                rows.extend(stray_client_cases())
            for name, single_function in (
                ("pool", pool_cases),
                ("concurrency", concurrency_cases),
                ("recovery", recovery_cases),
                ("isolation", isolation_cases),
                ("parameters", parameter_cases),
                ("stderr", stderr_cases),
            ):
                if args.cases in {"all", name}:
                    rows.extend(single_function(folder))
            from scripts.orchestration.sdk_stderr_cases import executable_cases, failure_cases, file_cap_case, idle_case

            if args.cases in {"all", "reader_failures"}:
                rows.extend(failure_cases(folder))
            if args.cases in {"all", "reader_idle"}:
                rows.append(idle_case(folder))
            if args.cases in {"all", "stderr_cap"}:
                rows.append(file_cap_case(folder))
            if args.cases in {"all", "executables"}:
                rows.extend(executable_cases(folder))
            rows.append({"case": "thread_cleanup", "checks": {"no_new_threads": {thread.ident for thread in threading.enumerate()} <= baseline_threads}})
        except NativeCleanupUnconfirmed as error:
            rows.append({"case": "cleanup_unconfirmed", "checks": {"confirmed": False}, "evidence": error.record})
        except (OSError, ValueError, RuntimeError, TimeoutError, EOFError, KeyError, TypeError, KeyboardInterrupt) as error:
            rows.append({"case": "native_case_error", "checks": {"completed": False}, "error_type": type(error).__name__})
        finally:
            isolated_sweeps.stop()
    rows.append(
        {"case": "frozen_sources", "checks": {"unchanged": hashes == {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in sources}}}
    )
    failed = [row["case"] for row in rows if not all(row["checks"].values())]
    report = {
        "classification": "Synthetic",
        "written_utc": datetime.now(UTC).isoformat(),
        "mode": args.mode,
        "command": [sys.executable, *sys.argv],
        "source_sha256": hashes,
        "cases": rows,
        "failed": failed,
        "elapsed_s": time.monotonic() - started,
        "cleanup": "done" if not failed else "inspect failed cases",
        "limits": [
            "Shared launcher wiring, owner terminal harness, descendants, native omission and Linux evidence require the lead's integrated runs",
            "No cloud requests or source datasets",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"report": str(args.output), "failed": failed}) + "\n")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
