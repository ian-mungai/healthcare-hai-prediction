"""Exercise durable controller tokens through real socket requests and a synthetic memory-only Parameter Store."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import launcher, worker_protocol
from orchestration.controller_tokens import SecureRunTokens
from orchestration.interface import SubmissionRequest
from scripts.lakehouse import run_lock
from scripts.orchestration.broker import LauncherUnavailable
from scripts.orchestration.run_controller_runtime_e2e import RUN, require, running
from scripts.orchestration.run_controller_runtime_e2e import configured as base_configured

ROOT = Path(__file__).resolve().parents[2]
SIGNING = "/healthcare_hai_prediction/controller_signing_key"
SYNTHETIC_TOKEN = bytes(range(32, 64)).hex()
SOURCES = (
    "orchestration/controller_tokens.py",
    "scripts/lakehouse/run_lock.py",
    "orchestration/controller_backend.py",
    "orchestration/launcher.py",
    "orchestration/sdk_worker.py",
    "orchestration/worker_pool.py",
    "orchestration/worker_protocol.py",
    "orchestration/ecs_api.py",
    "data_contracts/orchestration/sdk_worker.schema.json",
    "scripts/orchestration/controller_runtime.py",
    "scripts/orchestration/run_controller_runtime_e2e.py",
    "scripts/orchestration/run_controller_tokens_e2e.py",
)


def failure(code: str, kind: str = "service") -> dict[str, Any]:
    return worker_protocol.error_reply("synthetic", "controller_put_parameter", kind, code)


class Parameters:
    """Synthetic remote state survives controller and client replacement without writing token values to disk."""

    def __init__(self) -> None:
        self.values: dict[str, dict[str, Any]] = {SIGNING: {"name": SIGNING, "version": 1, "value": bytes(range(32)).hex()}}
        self.calls: list[dict[str, Any]] = []
        self.before: Callable[[str, Mapping[str, Any]], dict[str, Any] | None] | None = None
        self.after: Callable[[str, Mapping[str, Any]], dict[str, Any] | None] | None = None
        self.registry: run_lock.Registry | None = None

    def client(self) -> FakeClient:
        return FakeClient(self)


class FakeClient:
    """Inject the same closed request interface as the isolated SDK client, including absolute deadlines."""

    def __init__(self, remote: Parameters) -> None:
        self.remote = remote
        self.stopped = threading.Event()

    def call(self, operation: str, params: Mapping[str, Any], *, deadline: float) -> dict[str, Any]:
        require(time.monotonic() < deadline, "network call started after its deadline")
        remote = self.remote
        name = str(params["name"])
        remote.calls.append({"op": operation, "name": name, "deadline": deadline})
        if remote.registry is not None:
            with (remote.registry.path / "registry.lock").open("a") as lock:
                limit = time.monotonic() + 0.1
                while True:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= limit:
                            raise AssertionError("network call holds the registry lock") from None
                        time.sleep(0.001)
                fcntl.flock(lock, fcntl.LOCK_UN)
            if operation == "controller_put_parameter":
                saved = json.loads(remote.registry.state_file.read_text())
                entry = saved["runs"].get(RUN, {})
                require(entry.get("state") == "acquire_pending", "parameter write preceded durable pending state")
                require(entry.get("token_sha256") == run_lock.digest(str(params["value"])), "pending token hash differs")
        if self.stopped.is_set():
            return failure("shutdown", "shutdown")
        if remote.before is not None and (answer := remote.before(operation, params)) is not None:
            return answer
        if operation == "controller_get_parameter":
            answer = {"ok": True, "result": dict(remote.values[name])} if name in remote.values else failure("ParameterNotFound")
        elif operation == "controller_put_parameter":
            require(set(params) == {"name", "value"}, "parameter write contains undeclared controls")
            if name in remote.values:
                answer = failure("ParameterAlreadyExists")
            else:
                remote.values[name] = {"name": name, "version": 1, "value": params["value"]}
                answer = {"ok": True, "result": {"version": 1}}
        elif operation == "controller_delete_parameter":
            require(set(params) == {"name"}, "parameter delete contains undeclared controls")
            remote.values.pop(name, None)
            answer = {"ok": True, "result": {"deleted": True}}
        else:
            raise AssertionError("unknown synthetic parameter operation")
        if remote.after is not None and (override := remote.after(operation, params)) is not None:
            return override
        return answer

    def close(self, *, deadline: float) -> dict[str, Any]:
        require(time.monotonic() < deadline, "client cleanup has no budget")
        self.stopped.set()
        return {"confirmed": True, "survivors": []}


def configured(folder: Path, remote: Parameters, *, seconds: float = 1.0) -> launcher.Config:
    config = base_configured(folder)
    remote.registry = config.registry
    provider = SecureRunTokens(
        config.registry, remote.client(), seconds=seconds, call_seconds=min(0.2, seconds), retry_delay=0.01, token_factory=lambda: SYNTHETIC_TOKEN
    )
    return replace(config, token_provider=provider, timings=replace(config.timings, tick=0.01))


def secure_provider(config: launcher.Config) -> SecureRunTokens:
    provider = config.token_provider
    if not isinstance(provider, SecureRunTokens):
        raise AssertionError("secure provider is absent")
    return provider


def snapshot(config: launcher.Config) -> dict[str, Any]:
    with run_lock.transaction(config.registry, write=False) as state:
        return json.loads(json.dumps(state))


def parameter(config: launcher.Config) -> str:
    return f"/healthcare_hai_prediction/controller_run_tokens/{snapshot(config)['registry_id']}/{RUN}"


def acquire(controller: Any, *, request_id: str = "stable-acquire") -> dict[str, Any]:
    return controller.client.call("acquire", {"domain": "transform", "run_id": RUN, "mode": "publish_fixture"}, request_id=request_id, timeout=2, budget=3)


def rejects(operation: Callable[[], Any], code: str) -> None:
    try:
        operation()
    except (launcher.ClientError, launcher.LauncherError) as error:
        require(error.code == code, f"expected {code}, got {error.code}")
    else:
        raise AssertionError(f"expected {code}")


def until(predicate: Callable[[], bool], message: str, seconds: float = 2) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(message)
        time.sleep(min(0.01, max(0, deadline - time.monotonic())))


@contextlib.contextmanager
def pending_call(controller: Any) -> Iterator[list[Any]]:
    result: list[Any] = []

    def send() -> None:
        try:
            result.append(acquire(controller))
        except launcher.ClientError as error:
            result.append(error)

    thread = threading.Thread(target=send, name="synthetic-acquire-client")
    thread.start()
    try:
        yield result
    finally:
        thread.join(timeout=4)
        require(not thread.is_alive(), "acquire client survived cleanup")


def no_plaintext(folder: Path, remote: Parameters) -> None:
    values = [str(row["value"]).encode() for row in remote.values.values()]
    values.append(SYNTHETIC_TOKEN.encode())
    for path in folder.rglob("*"):
        if path.is_file() and not path.is_symlink():
            data = path.read_bytes()
            require(not any(value and value in data for value in values), "a token value reached controller files")


def acquire_replay(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    with running(config) as controller:
        first = acquire(controller)
        require(first == {"run_id": RUN, "token_file": None}, "secure acquire response changed")
        require(acquire(controller) == first, "same acquire request changed its answer")
        principal = controller.client.principal_run(RUN)
        require(principal["token"] == SYNTHETIC_TOKEN, "broker principal changed the secure token")
        require(controller.client.call("enter", {}, principal)["run_id"] == RUN, "held token cannot authenticate")
        entry = snapshot(config)["runs"][RUN]
        require(entry["state"] == "held" and entry["generation"] == 1, "readback did not publish the held generation")
        require(entry["secure_token"]["version"] == 1, "parameter version was not retained")
        require(sum(row["op"] == "controller_put_parameter" for row in remote.calls) == 1, "acquire replay wrote again")
        rejects(lambda: acquire(controller, request_id="different-acquire"), "refused")
        changed = {"domain": "transform", "run_id": RUN, "mode": "e2e"}
        rejects(lambda: controller.client.call("acquire", changed, request_id="stable-acquire"), "replay")
        no_plaintext(folder, remote)


def broker_restart(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    request = SubmissionRequest("gold_validate", RUN, "publish_fixture", 1, "gold_dag", "validate", "manual")
    with running(config, broker=True) as controller:
        require(controller.broker is not None, "real broker was not started")
        principal = controller.broker.ensure_run(request)
        require(principal["token"] == SYNTHETIC_TOKEN, "broker acquire changed the run token")
        before = snapshot(config)["runs"][RUN]
    replacement = configured(folder, remote)
    with running(replacement, broker=True) as controller:
        require(controller.broker is not None, "replacement broker was not started")
        principal = controller.broker.ensure_run(request)
        require(principal["token"] == SYNTHETIC_TOKEN, "broker restart changed run authentication")
        after = snapshot(replacement)["runs"][RUN]
        require(after["token_sha256"] == before["token_sha256"] and after["generation"] == before["generation"], "broker restart changed run identity")
        release_run(controller)
        try:
            controller.broker.ensure_run(request)
        except LauncherUnavailable:
            pass
        else:
            raise AssertionError("broker cache authenticated a released run")
        require(sum(row["op"] == "controller_put_parameter" for row in remote.calls) == 1, "broker release or restart reacquired the run")
        no_plaintext(folder, remote)


def runtime_factory(folder: Path) -> None:
    from scripts.orchestration.controller_runtime import secure_config

    remote = Parameters()
    original = base_configured(folder)
    remote.registry = original.registry
    config = secure_config(original, region="us-west-2", parameter_client=remote.client())
    require(isinstance(config.token_provider, SecureRunTokens), "AWS runtime factory did not connect the secure provider")
    with running(config) as controller:
        acquire(controller)
        require(controller.client.principal_run(RUN)["token"], "connected runtime provider cannot read its acquired token")


def retry_recorded(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    remote.after = lambda op, p: failure("AccessDeniedException") if op == "controller_get_parameter" and p["name"] != SIGNING else None
    with running(config) as controller:
        rejects(lambda: acquire(controller), "acquire_unconfirmed")
        before = snapshot(config)["runs"][RUN]
        require(not secure_provider(config).pending_values, "acknowledged put retained retry plaintext")
        remote.after = None
        controller.client.call("token_recover", {"run_id": RUN, "action": "retry", "owner": True})
        until(lambda: snapshot(config)["runs"][RUN]["state"] == "held", "explicit retry did not reconcile the original token")
        after = snapshot(config)["runs"][RUN]
        require(before["generation"] == after["generation"] and before["token_sha256"] == after["token_sha256"], "explicit retry changed acquire identity")
        remote.before = lambda op, _p: failure("AccessDeniedException") if op == "controller_delete_parameter" else None
        release_run(controller)
        until(lambda: bool(snapshot(config)["runs"][RUN]["secure_token"].get("last_error")), "cleanup failure was not retained")
        remote.before = None
        controller.client.call("token_recover", {"run_id": RUN, "action": "retry", "owner": True})
        until(lambda: snapshot(config)["runs"][RUN]["secure_token"]["status"] == "retired", "explicit retry did not retire the token")


def retry_refused(folder: Path, *, quota: bool) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    provider = secure_provider(config)
    code, kind = ("ParameterLimitExceeded", "service") if quota else ("ThrottlingException", "throttle")
    attempted_hashes: list[str] = []
    refusing = True

    def refuse(operation: str, params: Mapping[str, Any]) -> dict[str, Any] | None:
        if operation == "controller_put_parameter":
            attempted_hashes.append(run_lock.digest(str(params["value"])))
            if refusing:
                return failure(code, kind)
        return None

    remote.before = refuse
    with mock.patch.object(provider, "token_factory", wraps=provider.token_factory) as generate, running(config) as controller:
        rejects(lambda: acquire(controller), "token_quota" if quota else "acquire_unconfirmed")
        before = snapshot(config)
        entry = before["runs"][RUN]
        key = entry["secure_token"]["request_key"]
        require(len(attempted_hashes) == (1 if quota else 3), "refused put attempt count changed")
        require(parameter(config) not in remote.values, "refused put created a parameter")
        refusing = False
        controller.client.call("token_recover", {"run_id": RUN, "action": "retry", "owner": True})
        until(lambda: snapshot(config)["runs"][RUN]["state"] == "held", "retry after refused put did not publish the original token")
        after = snapshot(config)
        held = after["runs"][RUN]
        require(
            (held["token_sha256"], held["generation"], held["secure_token"]["request_key"]) == (entry["token_sha256"], entry["generation"], key),
            "retry after refusal changed token or request identity",
        )
        require(after["requests"][key]["payload_sha256"] == before["requests"][key]["payload_sha256"], "retry changed the acquire payload")
        require(len(attempted_hashes) == (2 if quota else 4) and set(attempted_hashes) == {entry["token_sha256"]}, "retry did not reuse the original value")
        require(generate.call_count == 1, "retry generated a replacement token")
        require("re-puts the same token from memory" in before["requests"][key]["result"]["error"], "refusal omitted the available in-memory retry")
        require(acquire(controller)["run_id"] == RUN, "retried acquire cannot replay")
        require(controller.client.principal_run(RUN)["token"] == SYNTHETIC_TOKEN, "retried token cannot authenticate")
        require(RUN not in provider.pending_values, "successful put retained the pending plaintext")
        require(len([name for name in remote.values if name != SIGNING]) == 1, "retry published duplicate parameters")
        no_plaintext(folder, remote)
    require(not provider.pending_values and not provider.cache, "confirmed close retained token values")


def retry_throttled(folder: Path) -> None:
    retry_refused(folder, quota=False)


def retry_quota(folder: Path) -> None:
    retry_refused(folder, quota=True)


def retry_restart(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    remote.before = lambda op, _p: failure("ParameterLimitExceeded") if op == "controller_put_parameter" else None
    with running(config) as controller:
        rejects(lambda: acquire(controller), "token_quota")
        before = snapshot(config)["runs"][RUN]
    require(not secure_provider(config).pending_values, "shutdown retained refused token plaintext")
    remote.before = None
    replacement = configured(folder, remote)
    with (
        mock.patch.object(replacement.token_provider, "token_factory", side_effect=AssertionError("restart generated a replacement token")) as generate,
        running(replacement) as controller,
    ):
        until(lambda: snapshot(replacement)["runs"][RUN]["state"] == "acquire_unconfirmed", "restart did not finish readback")
        state = snapshot(replacement)
        record = state["runs"][RUN]["secure_token"]
        message = state["requests"][record["request_key"]]["result"]["error"]
        require("Retry is readback only because no token remains in memory" in message, "restart retry message promises a new put")
        require("readback only" in record["recovery_action"], "restart recovery status does not describe readback only")
        controller.client.call("token_recover", {"run_id": RUN, "action": "retry", "owner": True})
        until(lambda: snapshot(replacement)["runs"][RUN]["state"] == "acquire_unconfirmed", "restart retry did not finish readback")
        require(sum(row["op"] == "controller_put_parameter" for row in remote.calls) == 1, "restart retry wrote a replacement token")
        name = parameter(replacement)
        remote.values[name] = {"name": name, "version": 1, "value": SYNTHETIC_TOKEN}
        controller.client.call("token_recover", {"run_id": RUN, "action": "retry", "owner": True})
        until(lambda: snapshot(replacement)["runs"][RUN]["state"] == "held", "restart retry did not read the restored original token")
        after = snapshot(replacement)["runs"][RUN]
        require(
            (after["token_sha256"], after["generation"], after["secure_token"]["request_key"])
            == (before["token_sha256"], before["generation"], before["secure_token"]["request_key"]),
            "restart retry changed the pending identity",
        )
        require(generate.call_count == 0, "restart retry invoked the token generator")
        require(sum(row["op"] == "controller_put_parameter" for row in remote.calls) == 1, "restart reconciliation rewrote the parameter")
        no_plaintext(folder, remote)


def cleanup_timeout_shutdown(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    provider = secure_provider(config)
    with running(config) as controller:
        handle = controller.server.lock_handle
        if handle is None:
            raise AssertionError("running controller has no lock handle")
        original_close = provider.client.close

        def unconfirmed(*, deadline: float) -> dict[str, Any]:
            original_close(deadline=deadline)
            return {"confirmed": False, "survivors": []}

        try:
            with (
                mock.patch.object(provider.client, "close", side_effect=unconfirmed),
                mock.patch.object(controller.server, "drain_seals", wraps=controller.server.drain_seals) as drain,
                mock.patch.object(controller.server.pool, "shutdown", wraps=controller.server.pool.shutdown) as stop_pool,
            ):
                controller.close()
                require(drain.call_count == 1, "token cleanup timeout skipped seal drain")
                require(stop_pool.call_count == 1, "token cleanup timeout skipped pool shutdown")
                require(handle.closed, "token cleanup timeout kept the launcher lock open")
                require(snapshot(config)["launcher"]["status"] == "stopped", "token cleanup timeout skipped stopped status")
                require(isinstance(controller.failure, TimeoutError), "token cleanup timeout was hidden")
                require(str(controller.failure) == "controller token worker cleanup is unconfirmed", "shutdown changed the cleanup failure")
        finally:
            provider.close(deadline=time.monotonic() + 1)
            controller.server.pool.shutdown(wait=False, cancel_futures=True)
            handle.close()


def signing_encoding(folder: Path) -> None:
    for index, value in enumerate((bytes(range(32)).hex(), bytes(range(32)).hex().upper())):
        root = folder / f"valid_{index}"
        root.mkdir()
        remote = Parameters()
        remote.values[SIGNING]["value"] = value
        with running(configured(root, remote)):
            pass
    base = bytes(range(32)).hex()
    for index, value in enumerate((base + "\n", " " + base, base[:32] + " " + base[32:], base[:32] + "\t" + base[32:], base[:-1], base + "0", "g" * 64)):
        root = folder / f"invalid_{index}"
        root.mkdir()
        remote = Parameters()
        remote.values[SIGNING]["value"] = value
        config = configured(root, remote)
        rejects(partial(start_once, config), "token_identity")


def pooled_delete_timeouts(folder: Path) -> None:
    from botocore.exceptions import ClientError

    from orchestration.sdk_worker import Sdk

    remote = Parameters()
    name = "/healthcare_hai_prediction/controller_run_tokens/synthetic_registry/20261025T000000Z-01234567"
    budgets = (8.0, 4.0, 4.0, *(4.0 - index / 1000 for index in range(1, 21)))
    with parameter_http(remote) as endpoint:
        sdk = Sdk(argparse.Namespace(region="us-west-2", profile=None, test_endpoint=endpoint, test_control_endpoint=None))
        selected_clients: list[tuple[Any, Any]] = []
        original_client = sdk._client

        def observed_client(operation: str, params: Mapping[str, Any]) -> Any:
            client = original_client(operation, params)
            closed = mock.Mock(wraps=client.close)
            client.close = closed
            selected_clients.append((client, closed))
            return client

        try:
            with mock.patch.object(sdk, "_client", side_effect=observed_client), mock.patch.object(sdk, "_execute", wraps=sdk._execute) as execute:
                for budget in budgets:
                    require(sdk.call("controller_delete_parameter", {"name": name}, budget_s=budget)["deleted"], "pooled delete did not reach the service")
                    selected = execute.call_args.args[0].meta.config
                    expected = (min(3.0, budget / 2), min(5.0, budget / 2))
                    require((selected.connect_timeout, selected.read_timeout) == expected, "pooled delete reused stale SDK timeouts")
                require(not sdk.clients, "controller calls accumulated SDK clients")
                require(all(closed.call_count == 1 for _, closed in selected_clients), "controller call left its SDK client open")
                require(sum(row["op"] == "DeleteParameter" for row in remote.calls) == len(budgets), "pooled timeout case omitted a real SDK delete")
                try:
                    sdk.call("controller_get_parameter", {"name": name}, budget_s=1.99)
                except ClientError as error:
                    require(error.response["Error"]["Code"] == "ParameterNotFound", "SDK refusal has an unrelated cause")
                else:
                    raise AssertionError("absent parameter did not reach the SDK refusal")
                require(selected_clients[-1][1].call_count == 1, "refused controller call left its SDK client open")
                require(not sdk.clients, "refused controller call accumulated an SDK client")
        finally:
            for client, _ in selected_clients:
                client.close()


def cleanup_timeout_memory(folder: Path) -> None:
    for status in ("pending", "cached"):
        root = folder / status
        root.mkdir()
        remote = Parameters()
        config = configured(root, remote)
        provider = secure_provider(config)
        if status == "pending":
            remote.before = lambda operation, _params: failure("ParameterLimitExceeded") if operation == "controller_put_parameter" else None
        with running(config) as controller:
            if status == "pending":
                rejects(lambda: acquire(controller), "token_quota")
                require(RUN in provider.pending_values, "cleanup case did not populate pending token memory")
            else:
                acquire(controller)
                require(RUN in provider.cache, "cleanup case did not populate cached token memory")
            original_close = provider.client.close

            def unconfirmed(*, deadline: float, close: Callable[..., Any] = original_close) -> dict[str, Any]:
                close(deadline=deadline)
                return {"confirmed": False, "survivors": []}

            try:
                with mock.patch.object(provider.client, "close", side_effect=unconfirmed):
                    controller.close()
                require(isinstance(controller.failure, TimeoutError), "unconfirmed cleanup did not report its failure")
                require(not provider.pending_values, "unconfirmed cleanup retained pending token plaintext")
                require(not provider.cache, "unconfirmed cleanup retained cached token plaintext")
                require(not provider.closed, "unconfirmed cleanup was marked confirmed")
            finally:
                provider.close(deadline=time.monotonic() + 1)
            require(provider.closed, "confirmed cleanup retry did not close the provider")


def readback_required(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    remote.after = lambda op, p: failure("AccessDeniedException") if op == "controller_get_parameter" and p["name"] != SIGNING else None
    with running(config) as controller:
        rejects(lambda: acquire(controller), "acquire_unconfirmed")
        entry = snapshot(config)["runs"][RUN]
        require(entry["state"] == "acquire_unconfirmed", "failed readback published a held run")
        require(entry["secure_token"]["last_error"] == "AccessDeniedException", "readback refusal is not visible")
        principal = {"kind": "run", "run_id": RUN, "token": SYNTHETIC_TOKEN}
        rejects(lambda: controller.client.call("finalize", {}, principal), "principal")


def conflicting_parameter(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    with running(config) as controller:
        name = parameter(config)
        remote.values[name] = {"name": name, "version": 7, "value": "synthetic_foreign_token"}
        rejects(lambda: acquire(controller), "token_conflict")
        require(snapshot(config)["runs"][RUN]["state"] == "acquire_unconfirmed", "conflicting token became held")
        require(remote.values[name]["version"] == 7, "existing parameter was overwritten")
        provider = secure_provider(config)
        require(RUN not in provider.pending_values, "hash conflict retained pending plaintext")
        state = snapshot(config)
        record = state["runs"][RUN]["secure_token"]
        message = state["requests"][record["request_key"]]["result"]["error"]
        require("Abort the recorded acquire." in message and "Retry" not in message, "hash conflict response is not abort-only")
        require(
            "Abort the recorded acquire." in record["recovery_action"] and "Retry" not in record["recovery_action"], "hash conflict status is not abort-only"
        )
        no_plaintext(folder, remote)


def interrupted(folder: Path, *, after_write: bool) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    reached, release = threading.Event(), threading.Event()

    def pause(operation: str, _params: Mapping[str, Any]) -> dict[str, Any] | None:
        if operation == "controller_put_parameter":
            reached.set()
            release.wait(timeout=2)
            return failure("shutdown", "shutdown")
        return None

    if after_write:
        remote.after = pause
    else:
        remote.before = pause
    with running(config) as controller, pending_call(controller):
        require(reached.wait(timeout=1), "write boundary was not reached")
        before = snapshot(config)["runs"][RUN]
        require(before["state"] == "acquire_pending", "interrupted request lost pending state")
        require(controller.client.call("status", {"what": "launcher"})["status"] == "serving", "pending write blocked status")
        controller.server.running = False
        os.write(controller.server.wake_write, b"x")
        release.set()
    remote.before = remote.after = None
    replacement = configured(folder, remote)
    with running(replacement) as controller:
        if after_write:
            until(lambda: snapshot(replacement)["runs"][RUN]["state"] == "held", "restart did not reconcile the written token")
            require(acquire(controller)["run_id"] == RUN, "reconciled acquire replay changed")
            require(controller.client.principal_run(RUN)["token"] == SYNTHETIC_TOKEN, "restart generated another token")
        else:
            until(lambda: snapshot(replacement)["runs"][RUN]["state"] == "acquire_unconfirmed", "missing pending parameter became dispatchable")
            rejects(lambda: acquire(controller), "acquire_unconfirmed")
        after = snapshot(replacement)["runs"][RUN]
        require(after["generation"] == before["generation"] and after["token_sha256"] == before["token_sha256"], "restart changed token identity")
        require(after["secure_token"]["request_key"] == before["secure_token"]["request_key"], "restart changed request identity")
        require(sum(row["op"] == "controller_put_parameter" for row in remote.calls) == 1, "restart rewrote the parameter")
        no_plaintext(folder, remote)


def restart_before_write(folder: Path) -> None:
    interrupted(folder, after_write=False)


def restart_after_write(folder: Path) -> None:
    interrupted(folder, after_write=True)


def lost_response(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    remote.after = lambda op, _p: failure("sdk_network_error", "network") if op == "controller_put_parameter" else None
    with running(config) as controller:
        require(acquire(controller)["run_id"] == RUN, "lost write response was not reconciled")
        require(sum(row["op"] == "controller_put_parameter" for row in remote.calls) == 1, "lost response duplicated the write")


def release_run(controller: Any) -> dict[str, Any]:
    principal = controller.client.principal_run(RUN)
    controller.client.call("mark", {"state": "failed_retained"}, principal, request_id="stable-mark")
    result = controller.client.call("release", {"owner": True}, principal, request_id="stable-release")
    require(result["state"] == "released", "release did not commit terminal state")
    return principal


def release_replay(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    with running(config) as controller:
        acquire(controller)
        principal = release_run(controller)
        rejects(lambda: controller.client.principal_run(RUN), "token_missing")
        rejects(lambda: controller.client.call("mark", {"state": "failed_retained"}, principal, request_id="fresh-mark"), "principal")
        replay = controller.client.call("mark", {"state": "failed_retained"}, principal, request_id="stable-mark")
        require(replay["state"] == "failed_retained", "exact completed replay was lost after release")
        key = f"run:{RUN}|mark|stable-mark"
        with run_lock.transaction(config.registry) as state:
            state["requests"][key]["token_sha256"] = "0" * 64
        rejects(lambda: controller.client.call("mark", {"state": "failed_retained"}, principal, request_id="stable-mark"), "principal")
        with run_lock.transaction(config.registry) as state:
            state["requests"][key]["token_sha256"] = run_lock.digest(SYNTHETIC_TOKEN)
        require(controller.client.call("release", {"owner": True}, principal, request_id="stable-release")["state"] == "released", "release replay changed")
        rejects(lambda: controller.client.call("mark", {"state": "finalizing"}, principal, request_id="stable-mark"), "replay")
        until(lambda: parameter(config) not in remote.values, "released parameter accumulated")
        until(lambda: snapshot(config)["runs"][RUN]["secure_token"]["status"] == "retired", "absence did not retire cleanup")
        rejects(lambda: acquire(controller, request_id="reused-run"), "refused")
        no_plaintext(folder, remote)


def pending_history(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    config = replace(config, timings=replace(config.timings, compact_every=0.05, history_retain=0.1))
    remote.after = lambda op, p: failure("AccessDeniedException") if op == "controller_get_parameter" and p["name"] != SIGNING else None
    with running(config) as controller:
        rejects(lambda: acquire(controller), "acquire_unconfirmed")
        previous = controller.server.last_compact
        with run_lock.transaction(config.registry) as state:
            record = state["runs"][RUN]["secure_token"]
            state["requests"][record["request_key"]]["at"] = "2000-01-01T00:00:00+00:00"
        until(lambda: controller.server.last_compact > previous + 0.02, "maintenance did not compact the token request")
        saved = snapshot(config)
        key = saved["runs"][RUN]["secure_token"]["request_key"]
        require(key in saved["requests"], "compaction removed the unconfirmed acquire identity")
    remote.after = None
    replacement = configured(folder, remote)
    with running(replacement):
        until(lambda: snapshot(replacement)["runs"][RUN]["state"] == "held", "compacted pending acquire could not reconcile")


def cleanup_restart(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    remote.before = lambda op, _p: failure("AccessDeniedException") if op == "controller_delete_parameter" else None
    with running(config) as controller:
        acquire(controller)
        release_run(controller)
        until(lambda: bool(snapshot(config)["runs"][RUN]["secure_token"].get("last_error")), "cleanup refusal is not visible")
        require(snapshot(config)["runs"][RUN]["state"] == "released", "delete failure revived the run")
        require(parameter(config) in remote.values, "failed deletion removed the stand-in value")
    remote.before = None
    replacement = configured(folder, remote)
    with running(replacement):
        until(lambda: snapshot(replacement)["runs"][RUN]["secure_token"]["status"] == "retired", "restart did not drain cleanup")
        require(parameter(replacement) not in remote.values, "retired parameter remains present")


def cleanup_readback(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    with running(config) as controller:
        acquire(controller)

        def retain(operation: str, _params: Mapping[str, Any]) -> dict[str, Any] | None:
            return {"ok": True, "result": {"deleted": True}} if operation == "controller_delete_parameter" else None

        remote.before = retain
        release_run(controller)
        until(lambda: bool(snapshot(config)["runs"][RUN]["secure_token"].get("last_error")), "unconfirmed delete was accepted")
        require(snapshot(config)["runs"][RUN]["secure_token"]["status"] == "token_cleanup_pending", "delete without absence retired the token")


def abort_pending(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    remote.after = lambda op, p: failure("AccessDeniedException") if op == "controller_get_parameter" and p["name"] != SIGNING else None
    with running(config) as controller:
        rejects(lambda: acquire(controller), "acquire_unconfirmed")
        rejects(lambda: controller.client.call("token_recover", {"run_id": RUN, "action": "abort"}), "principal")
        remote.after = None
        result = controller.client.call("token_recover", {"run_id": RUN, "action": "abort", "owner": True})
        require(result["state"] == "released", "explicit abort did not commit terminal state")
        require(not secure_provider(config).pending_values, "abort retained the pending plaintext")
        until(lambda: snapshot(config)["runs"][RUN]["secure_token"]["status"] == "retired", "aborted parameter was not retired")
        rejects(lambda: acquire(controller, request_id="abort-reuse"), "refused")

    refused_folder = folder / "refused"
    refused_folder.mkdir()
    refused_remote = Parameters()
    config = configured(refused_folder, refused_remote)
    refused_remote.before = lambda op, _p: failure("ParameterLimitExceeded") if op == "controller_put_parameter" else None
    with running(config) as controller:
        rejects(lambda: acquire(controller), "token_quota")
        require(bool(secure_provider(config).pending_values), "refused acquire lost its retry value before abort")
        controller.client.call("token_recover", {"run_id": RUN, "action": "abort", "owner": True})
        require(not secure_provider(config).pending_values, "abort retained the refused token plaintext")
        until(lambda: snapshot(config)["runs"][RUN]["secure_token"]["status"] == "retired", "aborted refused acquire did not retire")
        no_plaintext(refused_folder, refused_remote)


def throttled(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    attempts = 0

    def throttle(operation: str, _params: Mapping[str, Any]) -> dict[str, Any] | None:
        nonlocal attempts
        if operation == "controller_put_parameter":
            attempts += 1
            if attempts < 3:
                return failure("ThrottlingException", "throttle")
        return None

    remote.before = throttle
    with running(config) as controller:
        acquire(controller)
        require(attempts == 3, "throttled write did not use its bounded retries")
        deadlines = [row["deadline"] for row in remote.calls if row["op"] == "controller_put_parameter"]
        require(max(deadlines) - min(deadlines) < 0.1, "retry reset the operation deadline")
    exhausted = Parameters()
    second = folder / "exhausted"
    second.mkdir()
    config = configured(second, exhausted)
    exhausted.before = lambda op, _p: failure("ThrottlingException", "throttle") if op == "controller_put_parameter" else None
    with running(config) as controller:
        rejects(lambda: acquire(controller), "acquire_unconfirmed")
        require(sum(row["op"] == "controller_put_parameter" for row in exhausted.calls) == 3, "throttled write exceeded three attempts")


def quota_exhausted(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    remote.before = lambda op, _p: failure("ParameterLimitExceeded") if op == "controller_put_parameter" else None
    with running(config) as controller:
        rejects(lambda: acquire(controller), "token_quota")
        token = snapshot(config)["runs"][RUN]["secure_token"]
        require(token["status"] == "acquire_unconfirmed" and token["last_error"] == "ParameterLimitExceeded", "quota outcome was hidden")
        require(sum(row["op"] == "controller_put_parameter" for row in remote.calls) == 1, "quota failure was retried")
        saved = snapshot(config)
        result = saved["requests"][token["request_key"]]["result"]
        require("Pending cleanup intents: 0" in result["error"], "quota refusal did not report pending cleanup")


def deadline_status(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote, seconds=0.15)
    entered, proceed = threading.Event(), threading.Event()

    def slow(operation: str, _params: Mapping[str, Any]) -> dict[str, Any] | None:
        if operation == "controller_put_parameter":
            entered.set()
            require(proceed.wait(timeout=2), "deadline fixture did not release the worker")
        return None

    remote.before = slow
    with running(config) as controller, pending_call(controller) as result:
        try:
            require(entered.wait(timeout=1), "slow write was not entered")
            start = time.monotonic()
            controller.client.call("status", {"what": "launcher"}, timeout=0.1, budget=0.15)
            require(time.monotonic() - start < 0.15, "slow parameter call blocked the launcher loop")
            until(lambda: bool(result), "acquire deadline did not answer the client", seconds=0.6)
            require(isinstance(result[0], launcher.ClientError) and result[0].code == "acquire_unconfirmed", "deadline has no named outcome")
            require(parameter(config) not in remote.values, "deadline response waited for the parameter write")
        finally:
            proceed.set()
        until(lambda: not secure_provider(config).jobs, "late worker did not finish")
        require(snapshot(config)["runs"][RUN]["state"] == "acquire_unconfirmed", "late success published an expired acquire")
        remote.before = None
        controller.client.call("token_recover", {"run_id": RUN, "action": "retry", "owner": True})
        until(lambda: snapshot(config)["runs"][RUN]["state"] == "held", "retry after timeout lost the original token")
        require(controller.client.principal_run(RUN)["token"] == SYNTHETIC_TOKEN, "timeout retry changed the token")
        require(sum(row["op"] == "controller_put_parameter" for row in remote.calls) == 2, "timeout retry discarded its in-memory value")
        no_plaintext(folder, remote)


def stale_generation(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    entered, proceed = threading.Event(), threading.Event()

    def delay(operation: str, _params: Mapping[str, Any]) -> dict[str, Any] | None:
        if operation == "controller_put_parameter":
            entered.set()
            proceed.wait(timeout=1)
        return None

    remote.after = delay
    with running(config) as controller, pending_call(controller):
        require(entered.wait(timeout=1), "stale-response write was not reached")
        with run_lock.transaction(config.registry) as state:
            state["runs"][RUN]["generation"] += 1
        proceed.set()
        until(lambda: snapshot(config)["runs"][RUN]["state"] == "acquire_unconfirmed", "stale generation was published")


def retained_and_missing(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    with running(config) as controller:
        acquire(controller)
        principal = controller.client.principal_run(RUN)
        controller.client.call("mark", {"state": "failed_retained"}, principal)
    replacement = configured(folder, remote)
    with running(replacement) as controller:
        require(controller.client.principal_run(RUN)["token"] == SYNTHETIC_TOKEN, "restart lost retained token identity")
        require(parameter(replacement) in remote.values, "restart deleted failed-retained authentication")
    remote.values.pop(parameter(replacement))
    replacement = configured(folder, remote)
    with running(replacement) as controller:
        rejects(lambda: controller.client.principal_run(RUN), "token_missing")
        require(snapshot(replacement)["runs"][RUN]["state"] == "failed_retained", "missing token reset retained state")


def signing_identity(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    with running(config):
        pinned = snapshot(config)["launcher"].get("controller_signing_key")
        require(pinned is not None and pinned["version"] == 1, "signing-key identity was not pinned")
    remote.values[SIGNING]["version"] = 2
    config = configured(folder, remote)
    rejects(lambda: start_once(config), "token_identity")
    remote.values.pop(SIGNING)
    config = configured(folder, remote)
    rejects(lambda: start_once(config), "token_missing")


def start_once(config: launcher.Config) -> None:
    with running(config):
        pass


def closed_operations(folder: Path) -> None:
    remote = Parameters()
    config = configured(folder, remote)
    with running(config):
        name = parameter(config)
        for operation, params in (
            ("controller_get_parameter", {"name": name}),
            ("controller_get_parameter", {"name": SIGNING}),
            ("controller_put_parameter", {"name": name, "value": SYNTHETIC_TOKEN}),
            ("controller_delete_parameter", {"name": name}),
        ):
            worker_protocol.validate({"id": "case", "op": operation, "params": params, "budget_s": 1})
            for invalid in ("/other_project/token", SIGNING if operation != "controller_get_parameter" else SIGNING + "/extra"):
                bad = {"id": "case", "op": operation, "params": {**params, "name": invalid}, "budget_s": 1}
                try:
                    worker_protocol.validate(bad)
                except worker_protocol.ProtocolError:
                    pass
                else:
                    raise AssertionError("closed controller operation accepted another parameter scope")
        for operation in ("controller_put_parameter", "controller_delete_parameter"):
            require(operation in worker_protocol.MUTATING, "parameter mutation has no uncertain-effect classification")
        require(worker_protocol.secret_bearing("controller_put_parameter", {"name": name}), "token write can reuse a worker")


def container_credentials(folder: Path) -> None:
    from orchestration.worker_pool import WorkerConfig, WorkerPool
    from orchestration.worker_recovery import WorkerLedger

    environment = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(folder),
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/v2/credentials/synthetic",
        "AWS_PROFILE": "synthetic_forbidden_profile",
        "AWS_ACCESS_KEY_ID": "synthetic_forbidden_access",
        "AWS_SECRET_ACCESS_KEY": "synthetic_forbidden_secret",
    }
    with mock.patch.dict(os.environ, environment, clear=True):
        pool = WorkerPool(WorkerConfig("us-west-2", container_credentials=True), WorkerLedger(folder / "registry"), "synthetic-session")
        observed = pool._environment("synthetic-witness")
        require(observed.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") == environment["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"], "ECS credential URI was lost")
        require(
            all(name not in observed for name in ("AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")), "static credentials reached controller worker"
        )
        require(
            observed.get("AWS_CONFIG_FILE") == os.devnull and observed.get("AWS_SHARED_CREDENTIALS_FILE") == os.devnull, "worker can fall back to profile files"
        )
        require(pool.close(time.monotonic() + 1)["confirmed"], "empty credential-test pool did not close")


@contextlib.contextmanager
def parameter_http(remote: Parameters) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            operation = self.headers["X-Amz-Target"].rsplit(".", 1)[-1]
            name = payload["Name"]
            remote.calls.append({"op": operation, "name": name})
            status = 200
            body: dict[str, Any]
            if operation == "PutParameter":
                require(payload["Type"] == "SecureString", "isolated token write is plaintext")
                require(payload["Tier"] == "Standard" and payload["Overwrite"] is False, "isolated token write can overwrite or charge")
                if name in remote.values:
                    status, body = 400, {"__type": "ParameterAlreadyExists", "message": "synthetic conflict"}
                else:
                    remote.values[name] = {"name": name, "version": 1, "value": payload["Value"]}
                    body = {"Version": 1, "Tier": "Standard"}
            elif operation == "GetParameter":
                require(payload["WithDecryption"] is True, "isolated parameter read omitted decryption")
                row = remote.values.get(name)
                if row is None:
                    status, body = 400, {"__type": "ParameterNotFound", "message": "synthetic absent"}
                else:
                    body = {"Parameter": {"Name": name, "Type": row.get("type", "SecureString"), "Value": row["value"], "Version": row["version"]}}
            elif operation == "DeleteParameter":
                remote.values.pop(name, None)
                body = {}
            else:
                status, body = 400, {"__type": "ValidationException", "message": "synthetic operation refused"}
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/x-amz-json-1.1")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, _format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, name="synthetic-parameter-http")
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        require(not thread.is_alive(), "parameter HTTP server survived cleanup")


def isolated_store(folder: Path) -> None:
    from orchestration.ecs_api import EcsApi, WorkerConfig

    remote = Parameters()
    config = configured(folder, remote)
    with running(config):
        name = parameter(config)
    with parameter_http(remote) as endpoint:
        api = EcsApi(WorkerConfig("us-west-2", test_endpoint=endpoint, pooled=False, container_credentials=True), config.registry.path)
        try:
            put = api.call("controller_put_parameter", {"name": name, "value": SYNTHETIC_TOKEN}, deadline=time.monotonic() + 4)
            require(put["ok"] and put["result"]["version"] == 1, "isolated SecureString creation failed")
            duplicate = api.call("controller_put_parameter", {"name": name, "value": SYNTHETIC_TOKEN}, deadline=time.monotonic() + 4)
            require(not duplicate["ok"] and duplicate["error"]["code"] == "ParameterAlreadyExists", "isolated create lost conflict identity")
            read = api.call("controller_get_parameter", {"name": name}, deadline=time.monotonic() + 4)
            require(read["ok"] and read["result"] == {"name": name, "version": 1, "value": SYNTHETIC_TOKEN}, "isolated readback changed identity")
            remote.values[name]["type"] = "String"
            invalid = api.call("controller_get_parameter", {"name": name}, deadline=time.monotonic() + 4)
            require(not invalid["ok"] and invalid["error"]["kind"] == "protocol", "isolated read accepted a plaintext parameter")
            deleted = api.call("controller_delete_parameter", {"name": name}, deadline=time.monotonic() + 4)
            require(deleted["ok"], "isolated parameter deletion failed")
            absent = api.call("controller_get_parameter", {"name": name}, deadline=time.monotonic() + 4)
            require(not absent["ok"] and absent["error"]["code"] == "ParameterNotFound", "isolated absence identity changed")
        finally:
            require(api.close(deadline=time.monotonic() + 5)["confirmed"], "isolated worker cleanup is unconfirmed")
            require(all(handle.confirmed for handle in api.handles), "isolated parameter worker survived cleanup")
    no_plaintext(folder, remote)


CASES = {
    case.__name__: case
    for case in (
        acquire_replay,
        broker_restart,
        runtime_factory,
        retry_recorded,
        retry_throttled,
        retry_quota,
        retry_restart,
        cleanup_timeout_shutdown,
        cleanup_timeout_memory,
        signing_encoding,
        pooled_delete_timeouts,
        readback_required,
        conflicting_parameter,
        restart_before_write,
        restart_after_write,
        lost_response,
        release_replay,
        cleanup_restart,
        pending_history,
        cleanup_readback,
        abort_pending,
        throttled,
        quota_exhausted,
        deadline_status,
        stale_generation,
        retained_and_missing,
        signing_identity,
        closed_operations,
        container_credentials,
        isolated_store,
    )
}


def hashes() -> dict[str, str]:
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).is_file() else "absent" for name in SOURCES}


def main() -> int:
    """Freeze sources, run the selected stand-in cases and retain sanitized repeatable evidence."""
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--output", type=Path, required=True)
    cli.add_argument("--case", action="append", choices=tuple(CASES))
    args = cli.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    started, before = datetime.now(UTC), hashes()
    results: dict[str, Any] = {}
    initial_threads = set(threading.enumerate())
    for name in args.case or CASES:
        with tempfile.TemporaryDirectory(prefix="ctok-", dir="/private/tmp") as temporary:
            start = time.monotonic()
            try:
                CASES[name](Path(temporary).resolve())
                results[name] = {"status": "pass"}
            except (
                AssertionError,
                launcher.ClientError,
                launcher.LauncherError,
                run_lock.RegistryError,
                LauncherUnavailable,
                OSError,
                ValueError,
                TypeError,
                KeyError,
                ImportError,
                AttributeError,
            ) as error:
                results[name] = {"status": "fail", "error_type": type(error).__name__, "message": str(error)}
            results[name]["seconds"] = round(time.monotonic() - start, 3)
    remaining = [thread.name for thread in threading.enumerate() if thread not in initial_threads and thread.is_alive()]
    changed = before != hashes()
    report = {
        "started_utc": started.isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.run_controller_tokens_e2e", *sys.argv[1:]],
        "classification": "Synthetic",
        "scope": "real launcher loop, Unix socket and durable registry with an injected memory-only Parameter Store",
        "source_sha256": before,
        "source_changed": changed,
        "cases": results,
        "cleanup": "failed" if remaining else "done",
        "remaining_threads": remaining,
        "python_version": sys.version.split()[0],
        "limits": ["No AWS, Docker, real task-role identity or native image", "Parameter Store calls use a synthetic closed client"],
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "retention.json").write_text(
        json.dumps({"class": 4, "purpose": report["scope"], "created": started.isoformat(), "end_condition": "Review accepted and no record cites the run"})
        + "\n"
    )
    failed = [name for name, row in results.items() if row["status"] != "pass"]
    sys.stdout.write(json.dumps({"report": str(args.output / "report.json"), "failed": failed, "source_changed": changed, "cleanup": report["cleanup"]}) + "\n")
    return int(bool(failed or remaining or changed))


if __name__ == "__main__":
    sys.exit(main())
