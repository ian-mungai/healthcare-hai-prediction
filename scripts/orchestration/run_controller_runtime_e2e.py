"""Exercise controller startup, socket requests and owner commands with synthetic credentials and ECS observations.

Run with --output pointing to a fresh evidence folder. No AWS or Docker service is used.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import http.client
import io
import json
import os
import socket
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import ecs_target, launcher
from orchestration.interface import State, SubmissionRequest
from scripts.lakehouse import run_lock
from scripts.orchestration.broker import Broker, BrokerError, BrokerPaths, LauncherUnavailable, Listener, load_templates
from scripts.orchestration.ecs_fake_api import FakeEcs

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261025T000000Z-01234567"
SOURCES = (
    "orchestration/controller_backend.py",
    "orchestration/launcher.py",
    "scripts/lakehouse/run_lock.py",
    "scripts/orchestration/broker.py",
    "scripts/orchestration/controller_runtime.py",
    "scripts/orchestration/controller_owner.py",
    "scripts/orchestration/run_controller_runtime_e2e.py",
)


def require(condition: bool, message: str) -> None:
    """Fail a case with its independently specified expectation."""
    if not condition:
        raise AssertionError(message)


def refused(operation: Callable[[], Any], text: str) -> None:
    """Accept only the named refusal, preserving unrelated failures."""
    try:
        operation()
    except (launcher.LauncherError, launcher.ClientError, run_lock.RegistryError, LauncherUnavailable) as error:
        require(text in str(error), f"expected refusal containing {text!r}, got {error}")
    else:
        raise AssertionError(f"expected refusal containing {text!r}")


class MemoryTokens:
    """Synthetic provider. Real durable secure-token publication belongs to its own runtime slice."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def signing_key(self) -> bytes:
        return b"synthetic-controller-signing-key-00"

    def read(self, run_id: str) -> str:
        if run_id not in self.values:
            raise launcher.LauncherError("secure run token is absent", "token_missing")
        return self.values[run_id]

    def absent(self, run_id: str) -> bool:
        return run_id not in self.values

    def acquire(self, server: launcher.Launcher, ctx: launcher.Ctx) -> dict[str, Any]:
        params = ctx.params
        token = run_lock.new_run(
            ctx.state,
            str(params["domain"]),
            str(params["run_id"]),
            str(params["mode"]),
            str(params.get("holder", "manual")),
            ctx.conn.peer.pid,
            str(ctx.conn.peer.start),
            list(params.get("write_prefixes", [])),
            str(server.cfg.root),
        )
        self.values[str(params["run_id"])] = token
        return {"run_id": params["run_id"], "token_file": None}

    def start(self, server: launcher.Launcher) -> None:
        pass

    def tick(self, server: launcher.Launcher) -> None:
        pass

    def release(self, server: launcher.Launcher, ctx: launcher.Ctx) -> None:
        self.values.pop(str(ctx.principal["run_id"]), None)

    def recover(self, server: launcher.Launcher, ctx: launcher.Ctx) -> dict[str, Any]:
        raise launcher.LauncherError("synthetic provider has no durable token recovery", "refused")

    def close(self, *, deadline: float) -> None:
        self.values.clear()


class CompleteEcs(FakeEcs):
    """Synthetic complete-listing witness and terminal application through the existing runtime seam."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.clear = True

    def run_clear(self, state: Mapping[str, Any], run_id: str) -> bool:
        return self.clear

    def advance(self, state: dict[str, Any], attempt: dict[str, Any], now: float) -> None:
        if attempt.get("cancel_intent"):
            live = attempt["execution"]["live_try"]
            placement = next(row for row in attempt["execution"]["placements"] if row["try_id"] == live)
            ecs_target.fenced(attempt, live, now, no_start=not placement["started"])
            attempt.update(phase="ended", reason="cancelled", exit_code=None)
        else:
            super().advance(state, attempt, now)


def settings(folder: Path) -> tuple[Path, run_lock.Registry]:
    """Write explicit nonsecret roots for a disposable test registry."""
    registry = run_lock.Registry(folder / "registry", "test")
    path = folder / "controller.json"
    path.write_text(
        json.dumps(
            {
                "local_backend": "disabled",
                "registry_root": str(registry.path),
                "runs_root": str(folder / "runs"),
                "artifacts_root": str(folder / "artifacts"),
                "controller_uid": os.getuid(),
            }
        )
    )
    return path, registry


def configured(folder: Path) -> launcher.Config:
    from scripts.orchestration import controller_runtime

    path, registry = settings(folder)
    config = controller_runtime.load_config(path, test_registry=str(registry.path))
    tokens = MemoryTokens()
    runtime = CompleteEcs(registry.state_file)
    return replace(config, token_provider=tokens, ecs_runtime=runtime, ecs_run_clear=runtime.run_clear)


@contextlib.contextmanager
def running(config: launcher.Config, *, broker: bool = False) -> Iterator[Any]:
    from scripts.orchestration.controller_runtime import Controller

    controller = Controller(config)
    if config.ecs_runtime is not None:
        controller.server.ecs_jobs = {**controller.server.ecs_jobs, "enabled": True}
    try:
        controller.start(broker_token="synthetic-broker-credential" if broker else None, port=0)
        yield controller
    finally:
        controller.close()
        require(not controller.thread.is_alive(), "controller thread survived cleanup")
        require(not config.socket_path.exists(), "launcher socket survived cleanup")


def configuration(folder: Path) -> None:
    from scripts.orchestration import controller_runtime

    path, registry = settings(folder)
    with (
        mock.patch.object(launcher, "docker_socket_path", side_effect=AssertionError("Docker socket discovery")),
        mock.patch.object(launcher, "real_token_root", side_effect=AssertionError("Mac token root")),
    ):
        config = controller_runtime.load_config(path, test_registry=str(registry.path))
        require(config.local_backend == "disabled", "explicit disabled mode was lost")
        require(config.runs_root == folder / "runs", "run root changed")
        require(config.artifacts_root == folder / "artifacts", "artifact root changed")
        require(config.registry.path == registry.path, "registry root changed")
        for field, bad, reason in (
            ("local_backend", "invalid", "local_backend"),
            ("registry_root", "relative", "absolute"),
            ("runs_root", "relative", "absolute"),
            ("artifacts_root", "", "absolute"),
        ):
            document = json.loads(path.read_text())
            original = dict(document)
            document[field] = bad
            path.write_text(json.dumps(document))
            refused(lambda: controller_runtime.load_config(path, test_registry=str(registry.path)), reason)
            path.write_text(json.dumps(original))


def identity(folder: Path) -> None:
    config = configured(folder)

    def startup_refused(reason: str) -> None:
        server = launcher.Launcher(config)
        try:
            refused(server.serve, reason)
        finally:
            server.pool.shutdown(wait=False, cancel_futures=True)
            server.selector.close()
            os.close(server.wake_read)
            os.close(server.wake_write)

    with mock.patch.object(launcher.pwd, "getpwuid", side_effect=KeyError("absent")):
        refused(lambda: launcher.real_token_root("synthetic"), "passwd")
        startup_refused("passwd")
    with mock.patch.object(run_lock, "process_start", return_value=None):
        startup_refused("procps")


def startup(folder: Path) -> None:
    config = configured(folder)
    with (
        mock.patch.object(launcher, "DockerApi", side_effect=AssertionError("Docker client constructed")),
        mock.patch.object(launcher.Launcher, "connect_docker", side_effect=AssertionError("Docker connected")),
        mock.patch.object(launcher.Launcher, "try_reconnect", side_effect=AssertionError("Docker retried")),
    ):
        with running(config, broker=True) as controller:
            view = controller.client.call("status", {"what": "launcher"})
            require(view["status"] == "serving" and view["local_backend"] == "disabled", "Docker absence degraded AWS startup")
            require(view["controller_ready"] and view["storage_ready"] and view["ecs_ready"], "independent readiness is missing")
            require(controller.server.stream is None and controller.server.probe is None, "a Docker stream or probe was created")
            controller.server.timers()
            controller.server.mark_degraded("synthetic prerequisite unavailable")
            controller.server.next_retry = 0
            controller.server.timers()
            require(controller.listener is not None, "broker listener was not started")
            connection = http.client.HTTPConnection("127.0.0.1", controller.listener.port, timeout=3)
            try:
                connection.request("POST", "/v1/health", "{}", {"Authorization": "Bearer synthetic-broker-credential"})
                reply = connection.getresponse()
                body = json.loads(reply.read())
                require(reply.status == 200 and body["broker"] == "ok", "authenticated broker health failed")
                require(body["local_backend"] == "disabled", "broker health omitted controller readiness")
            finally:
                connection.close()
        with running(config) as replacement:
            require(replacement.client.call("ping")["status"] == "serving", "restart did not serve")
    require(not config.token_root.exists(), "disabled mode wrote token files")


def local_refusal(folder: Path) -> None:
    config = configured(folder)
    allocation_id = "historical-allocation"
    with run_lock.transaction(config.registry, create_schema=run_lock.LAUNCHER_SCHEMA) as state:
        state["allocations"][allocation_id] = {
            "id": allocation_id,
            "state": "released",
            "holder_pid": os.getpid(),
            "holder_start": run_lock.process_start(os.getpid()),
        }
    with mock.patch.object(launcher, "DockerApi", side_effect=AssertionError("Docker client constructed")), running(config) as controller:
        for method in (
            "allocate",
            "plan",
            "image",
            "catalog_up",
            "catalog_down",
            "orchestration_up",
            "orchestration_down",
            "lineage_up",
            "lineage_down",
            "pull",
            "docker_restart",
            "session",
            "snapshot",
            "pipeline_network_up",
            "pipeline_catalog_up",
        ):

            def request_local(method: str = method) -> Any:
                return controller.client.call(method)

            refused(request_local, "local backend is disabled")
        refused(lambda: controller.client.call("submit", {"target": "docker"}), "local backend is disabled")
        refused(lambda: controller.client.call("submit", {"template": "fixture_dbt"}), "local backend is disabled")
        refused(
            lambda: controller.client.call("status", {"what": "launcher"}, {"kind": "alloc", "alloc_id": allocation_id}),
            "local backend is disabled",
        )
        state = controller.client.call("status", {"what": "registry"})
        require(set(state["allocations"]) == {allocation_id}, "refused local work took an allocation")


def prerequisites(folder: Path) -> None:
    config = replace(configured(folder), token_provider=None, ecs_runtime=None, ecs_run_clear=None)
    with running(config) as controller:
        view = controller.client.call("status", {"what": "launcher"})
        require(view["controller_ready"] and not view["storage_ready"] and not view["ecs_ready"], "missing prerequisites were reported ready")
        refused(lambda: controller.client.call("acquire", {"domain": "transform", "run_id": RUN, "mode": "fixture"}), "secure token provider")
        broker = Broker(BrokerPaths(config.registry, folder / "broker_state"), load_templates(), controller.client)
        try:
            require(broker.token_absent(RUN) is False, "no provider was treated as confirmed token absence")
        finally:
            broker.close()
    ecs_folder = folder / "ecs_unready"
    ecs_folder.mkdir()
    config = replace(configured(ecs_folder), ecs_runtime=None, ecs_run_clear=None)
    with running(config) as controller:
        view = controller.client.call("status", {"what": "launcher"})
        require(view["storage_ready"] and not view["ecs_ready"], "ECS-only prerequisite setup is invalid")
        broker = Broker(BrokerPaths(config.registry, ecs_folder / "broker_state"), load_templates(), controller.client)
        try:
            require(broker.token_absent(RUN) is True, "provider-confirmed absence was not reported")
            refused(lambda: broker.require_serving(allow_cloud=True), "dispatch prerequisites")
        finally:
            broker.close()


def imported_work(folder: Path) -> None:
    payload = folder / "copy_payload"
    payload.mkdir()
    (payload / "sentinel").write_text("synthetic copied bytes")
    examples = (
        ("allocation", "allocations", "imported", {"state": "held"}),
        ("copy", "copies", "imported", {"phase": "copying", "guard": "local-copy", "folder": str(folder / "copy_payload")}),
        ("seal", "runs", RUN, {"seal": {"id": "local-seal"}}),
        ("registration", "registrations", "imported", {"allocation": "local-allocation"}),
        ("network", "runs", RUN, {"pipeline_network": {"phase": "up"}}),
        ("container", "containers", "imported", {"name": "synthetic-local-container"}),
        ("catalog", "launcher", "catalog", {"phase": "up"}),
        ("orchestration", "launcher", "orchestration", {"phase": "up"}),
        ("lineage", "launcher", "lineage", {"phase": "up"}),
        ("operation", "operations", "imported", {"state": "open", "kind": "create", "op_id": "imported", "attempt": "absent"}),
        ("restart", "launcher", "restart", {"phase": "running"}),
    )
    for name, collection, identity, record in examples:
        case_folder = folder / name
        case_folder.mkdir()
        config = configured(case_folder)
        with run_lock.transaction(config.registry, create_schema=run_lock.LAUNCHER_SCHEMA) as state:
            state[collection][identity] = record
        server = launcher.Launcher(config)
        try:
            with mock.patch.object(server, "loop", side_effect=AssertionError(f"undrained imported {name} reached serve loop")):
                require(server.serve() == launcher.REFUSED_EXIT, f"startup accepted imported local {name}")
            require(not config.socket_path.exists(), "refused import opened the socket")
            with run_lock.transaction(config.registry, write=False) as state:
                require(state[collection][identity] == record, f"startup changed imported {name} before refusal")
            require((payload / "sentinel").exists(), "startup removed imported copy bytes")
        finally:
            server.selector.close()
            os.close(server.wake_read)
            os.close(server.wake_write)


def cloud_flow(folder: Path) -> None:
    config = configured(folder)
    runtime = config.ecs_runtime
    if not isinstance(runtime, CompleteEcs):
        raise AssertionError("synthetic ECS facade was not installed")
    with mock.patch.object(launcher, "account_home", side_effect=AssertionError("Mac home resolution in AWS mode")), running(config) as controller:
        controller.server.ecs_jobs = {**controller.server.ecs_jobs, "enabled": True}
        acquired = controller.client.call("acquire", {"domain": "transform", "run_id": RUN, "mode": "publish_fixture"}, request_id="stable-acquire")
        require(acquired["token_file"] is None, "secure token acquire returned a local file")
        principal = controller.client.principal_run(RUN)
        require(
            controller.client.call("acquire", {"domain": "transform", "run_id": RUN, "mode": "publish_fixture"}, request_id="stable-acquire") == acquired,
            "acquire replay changed",
        )
        run_folder = config.runs_root / RUN
        run_folder.mkdir(parents=True)
        (run_folder / "staging.duckdb").write_bytes(b"synthetic database placeholder")
        submission = controller.client.call(
            "submit",
            {
                "template": "pipeline_validate_gold",
                "launch_id": "synthetic-validation",
                "params": {
                    "database": str(run_folder / "staging.duckdb"),
                    "build_root": str(run_folder),
                    "suites_host": str(ROOT / "data_contracts/great_expectations/gold"),
                    "baselines_host": str(ROOT / "data_contracts/great_expectations/fixture_baselines.json"),
                    "out_host": str(run_folder / "quality_gold"),
                },
            },
            principal,
        )
        deadline = time.monotonic() + 5
        attempt: dict[str, Any] = {}
        while time.monotonic() < deadline:
            attempt = controller.client.call("status", {"what": "attempt", "id": submission["attempt_id"]}, principal)
            if attempt.get("execution", {}).get("live_try"):
                break
            time.sleep(0.02)
        require(attempt["execution"]["requested_target"] == "ecs", "automatic AWS work retained Docker fallback")
        require(attempt["execution"]["placements"][0]["target"] == "ecs", "automatic AWS work did not dispatch to ECS")
        runtime.clear = False
        controller.client.call("recover", {"owner": True}, {**principal, "owner": True})
        time.sleep(0.1)
        held = controller.client.call("status", {"what": "run", "id": RUN}, principal)
        require(held["state"] == "recovering", "unconfirmed ECS reconciliation finished recovery")
        runtime.clear = True
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            held = controller.client.call("status", {"what": "run", "id": RUN}, principal)
            if held["state"] == "failed_retained":
                break
            time.sleep(0.02)
        require(held["state"] == "failed_retained", "confirmed ECS recovery did not finish")
        runtime.clear = False
        refused(lambda: controller.client.call("release", {"owner": True}, {**principal, "owner": True}), "ECS reconciliation")
        runtime.clear = True
        result = controller.client.call("release", {"owner": True}, {**principal, "owner": True})
        require(result["state"] == "released", "confirmed release failed")


def peer_access(folder: Path) -> None:
    config = configured(folder)
    with running(config) as controller:
        require(controller.client.call("status")["status"] == "serving", "same-UID owner status failed")
        controller.server.cfg = replace(config, expected_uid=os.getuid() + 1)
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(3)
            connection.connect(str(config.socket_path))
            connection.sendall(b'{"id":"foreign","method":"live"}\n')
            try:
                reply = connection.recv(4096)
            except ConnectionResetError:
                reply = b""
            require(not reply, "foreign UID received a launcher reply")
        controller.server.cfg = config


def owner_wrapper(folder: Path) -> None:
    from scripts.orchestration import controller_owner

    config = configured(folder)
    with running(config) as controller:
        calls: list[tuple[str, Any]] = []
        with (
            mock.patch.object(controller_owner.os, "geteuid", side_effect=[0, config.expected_uid]),
            mock.patch.object(controller_owner.os, "getuid", return_value=config.expected_uid),
            mock.patch.object(controller_owner.os, "getgid", return_value=0),
            mock.patch.object(controller_owner.os, "getegid", return_value=0),
            mock.patch.object(controller_owner.os, "getgroups", return_value=[]),
            mock.patch.object(controller_owner.os, "setgroups", side_effect=lambda value: calls.append(("groups", value))),
            mock.patch.object(controller_owner.os, "setgid", side_effect=lambda value: calls.append(("gid", value))),
            mock.patch.object(controller_owner.os, "setuid", side_effect=lambda value: calls.append(("uid", value))),
        ):
            controller_owner.drop_identity(config)
        require(calls == [("groups", []), ("gid", 0), ("uid", os.getuid())], "owner identity drop order or groups changed")
        with (
            mock.patch.object(controller_owner.os, "getuid", return_value=config.expected_uid),
            mock.patch.object(controller_owner.os, "geteuid", return_value=config.expected_uid),
            mock.patch.object(controller_owner.os, "getgid", return_value=0),
            mock.patch.object(controller_owner.os, "getegid", return_value=0),
            mock.patch.object(controller_owner.os, "getgroups", return_value=[]),
        ):
            controller_owner.verify_identity(config)
            with mock.patch.object(controller_owner.os, "getgroups", return_value=[123]):
                refused(lambda: controller_owner.verify_identity(config), "cleared groups")
        with mock.patch.object(controller_owner, "drop_identity"), contextlib.redirect_stdout(io.StringIO()):
            require(controller_owner.execute(config, ["status"]) == 0, "existing owner status command failed")
            controller.client.call("acquire", {"domain": "transform", "run_id": RUN, "mode": "fixture"})
            for input_tty, output_tty, answer, reason in (
                (False, False, "", "terminal"),
                (True, False, f"recover {RUN}", "terminal"),
                (True, True, "wrong", "confirmation"),
            ):
                with (
                    mock.patch.object(controller_owner.sys.stdin, "isatty", return_value=input_tty),
                    mock.patch.object(controller_owner.sys.stdout, "isatty", return_value=output_tty),
                    mock.patch("builtins.input", return_value=answer),
                ):
                    refused(lambda: controller_owner.execute(config, ["recover", "--run", RUN]), reason)
            with (
                mock.patch.object(controller_owner.sys.stdin, "isatty", return_value=True),
                mock.patch.object(controller_owner.sys.stdout, "isatty", return_value=True),
                mock.patch("builtins.input", return_value=f"recover {RUN}"),
            ):
                require(controller_owner.execute(config, ["recover", "--run", RUN]) == 0, "confirmed owner recovery failed")
            principal = controller.client.principal_run(RUN)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                held = controller.client.call("status", {"what": "run", "id": RUN}, principal)
                if held["state"] == "failed_retained":
                    break
                time.sleep(0.02)
            with (
                mock.patch.object(controller_owner.sys.stdin, "isatty", return_value=True),
                mock.patch.object(controller_owner.sys.stdout, "isatty", return_value=True),
                mock.patch("builtins.input", return_value=f"release {RUN}"),
            ):
                require(controller_owner.execute(config, ["release", "--run", RUN, "--owner"]) == 0, "confirmed owner release failed")
            require(controller.client.call("status", {"what": "run", "id": RUN}, principal)["state"] == "released", "owner release did not persist")
            controller.client.call("reserve", {"reader": "synthetic-reader", "domain": "transform", "publish_run": RUN})
            with (
                mock.patch.object(controller_owner.sys.stdin, "isatty", return_value=True),
                mock.patch.object(controller_owner.sys.stdout, "isatty", return_value=True),
                mock.patch("builtins.input", return_value="deregister-reader synthetic-reader"),
            ):
                refused(lambda: controller_owner.execute(config, ["deregister-reader", "--reader", "synthetic-reader"]), "--owner-confirmed")
                require(
                    controller_owner.execute(config, ["deregister-reader", "--reader", "synthetic-reader", "--owner-confirmed"]) == 0,
                    "confirmed reader removal failed",
                )
            require(not controller.client.call("status", {"what": "registry"})["readers"], "reader removal did not persist")
        with contextlib.redirect_stderr(io.StringIO()), mock.patch.object(controller_owner, "drop_identity"):
            try:
                controller_owner.execute(config, ["recover", "--run", RUN, "--pid", "1"])
            except SystemExit as error:
                require(error.code == 2, "arbitrary PID failed for an unrelated reason")
            else:
                raise AssertionError("owner wrapper accepted a caller PID")


def history_compaction(folder: Path) -> None:
    config = configured(folder)
    config = replace(config, timings=replace(config.timings, compact_threshold=1, compact_every=0.1, compact_floor=60))
    old_at = "2000-01-01T00:00:00+00:00"
    with run_lock.transaction(config.registry, create_schema=run_lock.LAUNCHER_SCHEMA) as state:
        for name, status, at in (
            ("old-done", "done", old_at),
            ("old-pending", "pending", old_at),
            ("fresh-done", "done", datetime.now(UTC).isoformat()),
        ):
            state["requests"][name] = {"state": status, "at": at, "method": "submit", "payload_sha256": "0" * 64, "result": {"synthetic": name}}
    probe = launcher.Launcher(config)
    try:
        with mock.patch.object(probe, "registry_snapshot", side_effect=AssertionError("maintenance ran during startup")):
            probe.maintenance(time.monotonic() + 100)
    finally:
        probe.pool.shutdown(wait=False, cancel_futures=True)
        probe.selector.close()
        os.close(probe.wake_read)
        os.close(probe.wake_write)
    with (
        mock.patch.object(launcher.Launcher, "start_scan", side_effect=AssertionError("AWS maintenance started a Docker scan")) as scans,
        mock.patch.object(launcher.Launcher, "docker_dispose", side_effect=AssertionError("AWS maintenance disposed a Docker orphan")) as disposal,
        running(config) as controller,
    ):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            state = json.loads(config.registry.state_file.read_text())
            if "old-done" not in state["requests"]:
                break
            time.sleep(0.02)
        require("old-done" not in state["requests"], "completed history was not compacted in AWS mode")
        require("old-pending" in state["requests"] and "fresh-done" in state["requests"], "compaction removed pending or recent requests")
        records = [json.loads(line) for path in (config.registry.path / "history").glob("*.jsonl") for line in path.read_text().splitlines()]
        archived = [row for row in records if row["collection"] == "requests" and row["key"] == "old-done"]
        require(len(archived) == 1 and archived[0]["record"]["result"] == {"synthetic": "old-done"}, "archived request bytes changed")
        previous = controller.server.last_maintenance
        controller.server.create_deadlines = [("synthetic-expired", 0)]
        controller.server.orphan_retries = [("synthetic-orphan", "synthetic-container")]
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and controller.server.last_maintenance <= previous:
            time.sleep(0.02)
        controller.client.call("ping")
        require(controller.server.last_maintenance > previous, "maintenance did not run again within its deadline")
        require(scans.call_count == 0 and disposal.call_count == 0, "AWS maintenance touched the local backend")
    with running(config) as restarted:
        state = json.loads(config.registry.state_file.read_text())
        require("old-done" not in state["requests"], "restart restored compacted history")
        require(restarted.client.call("ping")["status"] == "serving", "restart after compaction failed")


def forwarded_access(folder: Path) -> None:
    config = configured(folder)
    runtime = config.ecs_runtime
    if not isinstance(runtime, CompleteEcs):
        raise AssertionError("synthetic ECS facade was not installed")
    run_folder = config.runs_root / RUN
    run_folder.mkdir(parents=True)
    (run_folder / "staging.duckdb").write_bytes(b"synthetic unused database")
    request = SubmissionRequest("gold_validate", RUN, "publish_fixture", 1, "gold_dag", "validate", "manual")
    with running(config, broker=True) as controller:
        broker = controller.broker
        if broker is None:
            raise AssertionError("broker was not started")
        handle = broker.submit(request)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            status = broker.status(handle)
            if status.execution and status.execution["placements"] and status.execution["placements"][-1].get("task_arn"):
                break
            time.sleep(0.02)
        require(len(runtime.calls) == 1, "synthetic ECS task was not dispatched exactly once")
        forward = broker.read_forward(request)
        require(forward is not None, "accepted submission did not persist a forward")
        intent_hash = broker.ledger.read(RUN)[0]["spec_sha256"]
        original_call = broker.call

        def unavailable_dispatch(
            method: str, params: dict[str, Any], principal: dict[str, Any] | None = None, request_id: str | None = None, timeout: float | None = None
        ) -> Any:
            result = original_call(method, params, principal, request_id, timeout)
            if method == "status" and params.get("what") == "launcher":
                return {**result, "storage_ready": False, "ecs_ready": False}
            return result

        with mock.patch.object(broker, "call", side_effect=unavailable_dispatch):
            status = broker.status(handle)
            require(status.state in (State.PENDING, State.RUNNING), "forwarded status was unavailable when dispatch prerequisites failed")
            require(handle.submission_id in broker.reconcile()["updated"], "forwarded reconciliation was blocked")
            broker.cancel(handle)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                status = broker.status(handle)
                if status.state == State.CANCELLED:
                    break
                time.sleep(0.02)
            require(status.state == State.CANCELLED, "forwarded cancellation was blocked by dispatch prerequisites")
        require(len(runtime.calls) == 1, "status or cancellation dispatched another task")
        after = broker.read_forward(request)
        require(after is not None and forward is not None and after["attempt_id"] == forward["attempt_id"], "forwarded attempt identity changed")
        require(broker.ledger.read(RUN)[0]["spec_sha256"] == intent_hash, "forwarded intent identity changed")


def mac_degradation(folder: Path) -> None:
    config = launcher.Config(run_lock.Registry(folder / "registry", "test"), ROOT, str(folder / "missing.sock"), folder / "tokens", folder / "runs")
    server = launcher.Launcher(config)
    try:
        server.migration_gate()
        server.connect_docker()
        require(server.status == "degraded" and server.next_retry > time.monotonic(), "Mac Docker absence lost degradation or retry")
    finally:
        server.shutdown_resources()
        server.selector.close()
        os.close(server.wake_read)
        os.close(server.wake_write)
    client = launcher.LauncherClient(config, start=False)
    broker = Broker(BrokerPaths(config.registry, folder / "broker_state"), load_templates(), client)
    listener = Listener(broker, "127.0.0.1", 0, "synthetic-broker-credential")
    connection = http.client.HTTPConnection("127.0.0.1", listener.port, timeout=3)
    try:
        broker.start()
        listener.start()
        with mock.patch.object(client, "call", side_effect=AssertionError("Mac health probed launcher")):
            connection.request("POST", "/v1/health", "{}", {"Authorization": "Bearer synthetic-broker-credential"})
            reply = connection.getresponse()
            body = json.loads(reply.read())
        require(reply.status == 200 and body == {"broker": "ok"}, "Mac health changed or probed launcher")
    finally:
        connection.close()
        listener.stop()
        broker.close()


CASES = {
    case.__name__: case
    for case in (
        configuration,
        identity,
        startup,
        local_refusal,
        prerequisites,
        imported_work,
        cloud_flow,
        peer_access,
        owner_wrapper,
        history_compaction,
        forwarded_access,
        mac_degradation,
    )
}


def hashes() -> dict[str, str]:
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else "absent" for name in SOURCES}


def main() -> int:
    """Run bounded stand-in cases and preserve exact failures and source hashes."""
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--output", type=Path, required=True)
    cli.add_argument("--case", action="append", choices=tuple(CASES))
    args = cli.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    started, before = datetime.now(UTC), hashes()
    results: dict[str, Any] = {}
    initial_threads = set(threading.enumerate())
    for name in args.case or CASES:
        with tempfile.TemporaryDirectory(prefix="ctrl-", dir="/private/tmp") as temporary:
            start = time.monotonic()
            try:
                CASES[name](Path(temporary).resolve())
                results[name] = {"status": "pass"}
            except (
                AssertionError,
                ecs_target.PlacementError,
                launcher.LauncherError,
                launcher.ClientError,
                run_lock.RegistryError,
                run_lock.Refused,
                BrokerError,
                LauncherUnavailable,
                OSError,
                ValueError,
                KeyError,
                TypeError,
            ) as error:
                results[name] = {"status": "fail", "error_type": type(error).__name__, "message": str(error)}
            results[name]["seconds"] = round(time.monotonic() - start, 3)
    changed = before != hashes()
    remaining_threads = [thread.name for thread in threading.enumerate() if thread not in initial_threads and thread.is_alive()]
    report = {
        "started_utc": started.isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.run_controller_runtime_e2e", *sys.argv[1:]],
        "classification": "Synthetic",
        "scope": "real launcher loop and Unix socket with synthetic secure tokens and ECS observations",
        "source_sha256": before,
        "cases": results,
        "source_changed": changed,
        "cleanup": "failed" if remaining_threads else "done",
        "remaining_threads": remaining_threads,
        "python_version": sys.version.split()[0],
        "limits": ["No AWS, Docker, native image, real UID drop or ECS Exec", "Durable secure-token publication is supplied by another runtime slice"],
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "retention.json").write_text(
        json.dumps({"class": 4, "purpose": report["scope"], "created": started.isoformat(), "end_condition": "Review accepted and no record cites the run"})
        + "\n"
    )
    failed = [name for name, result in results.items() if result["status"] != "pass"]
    sys.stdout.write(json.dumps({"report": str(args.output / "report.json"), "failed": failed, "source_changed": changed}) + "\n")
    return int(bool(failed) or changed or bool(remaining_threads))


if __name__ == "__main__":
    sys.exit(main())
