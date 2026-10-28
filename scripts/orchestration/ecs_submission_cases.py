"""Exercise real broker and launcher handlers with synthetic registry and task transport."""

from __future__ import annotations

import copy
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import ecs_target, launcher, pipeline_protocol
from orchestration.interface import State, SubmissionRefused, SubmissionRequest
from scripts.lakehouse import run_lock
from scripts.orchestration.broker import Broker, BrokerPaths, LauncherUnavailable, load_templates
from scripts.orchestration.ecs_fake_api import FakeEcs
from scripts.orchestration.ecs_target_cases import NOW


class InProcessClient(launcher.LauncherClient):
    """Replace socket transport while retaining authentication, dispatch and registry transactions."""

    def __init__(self, server: launcher.Launcher) -> None:
        super().__init__(server.cfg, start=False)
        self.server = server
        self.connection = mock.Mock(spec=launcher.Conn)
        self.connection.peer = launcher.Peer(os.getuid(), os.getpid(), run_lock.process_start(os.getpid()))

    def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        principal: dict[str, Any] | None = None,
        request_id: str | None = None,
        timeout: float = 120,
        budget: float | None = None,
    ) -> Any:
        try:
            return self.server.dispatch(
                self.connection,
                {"v": launcher.PROTOCOL, "params": params or {}, "principal": principal or {"kind": "any"}},
                request_id or f"read-{method}",
                method,
            )
        except launcher.LauncherError as error:
            raise launcher.ClientError(str(error), error.code) from None


def dispatch_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Submit through the broker, retain replay identity and persist admission before the fake accepts it."""
    registry = run_lock.Registry(folder / "registry", "test")
    runtime = FakeEcs(registry.state_file)
    runs = folder / "runs"
    config = launcher.Config(
        registry,
        root,
        str(folder / "unavailable.sock"),
        folder / "tokens",
        runs,
        ecs_runtime=runtime,
        secrets_endpoint="http://127.0.0.1:1",
        aws_profile="synthetic",
        aws_region="us-west-2",
    )
    server = launcher.Launcher(config)
    server.ecs_jobs = {**server.ecs_jobs, "enabled": True}
    server.status, server.docker_ok = "degraded", False
    client = InProcessClient(server)
    broker = Broker(BrokerPaths(registry, folder / "broker"), load_templates(), client, repo_root=root, runs_root=runs)
    run_id = "20261023T000000Z-01234567"
    run_folder = runs / run_id
    run_folder.mkdir(parents=True)
    (run_folder / "staging.duckdb").write_bytes(b"synthetic unused database")
    request = SubmissionRequest("gold_validate", run_id, "publish_fixture", 1, "gold_dag", "validate", "manual")
    checks: dict[str, bool] = {}
    try:
        with mock.patch("orchestration.launcher.time.time", return_value=NOW):
            handle = broker.submit(request)
            with server.tx(write=False) as state:
                before = copy.deepcopy(next(iter(state["attempts"].values())))
            checks["broker_omission_selects_automatic_placement"] = before["execution"]["requested_target"] == "auto"
            checks["submission_does_not_need_docker"] = before["phase"] == "queued" and not server.docker_ok
            server.advance_all()
            checks["dispatch_follows_durable_intent"] = len(runtime.calls) == 1
            checks["aws_dispatch_takes_no_mac_slot"] = not json.loads(registry.state_file.read_text())["allocations"]
            checks["broker_replay_returns_original_handle"] = broker.submit(request) == handle
            server.advance_all()
            checks["replay_does_not_dispatch_again"] = len(runtime.calls) == 1
            status = broker.status(handle)
            checks["broker_exposes_live_task"] = bool(status.execution and status.execution["placements"][0]["task_arn"])
            runtime.states[runtime.calls[0]["try_id"]] = "running"
            server.advance_all()
            with mock.patch.object(broker, "attempt_of", wraps=broker.attempt_of) as attempt_read:
                running = broker.status(handle)
            checks["broker_cloud_poll_uses_fresh_execution_from_one_attempt"] = (
                attempt_read.call_count == 1
                and running.execution is not None
                and running.execution["placements"][0]["state"] == "running"
                and running.execution["placements"][0]["started"] is True
            )
            lines = broker.ledger.read(run_id)
            checks["cloud_identity_uses_separate_ledger_fields"] = any("ecs_task_arn" in line and "container_id" not in line for line in lines)
            try:
                broker.submit(SubmissionRequest(**{**request.__dict__, "target": "docker"}))
            except SubmissionRefused as error:
                checks["changed_target_replay_refused"] = "target differs" in str(error)
            else:
                checks["changed_target_replay_refused"] = False
            runtime.states[runtime.calls[0]["try_id"]] = "unknown"
            with mock.patch("orchestration.launcher.time.time", return_value=NOW + 3601):
                server.advance_all()
            state = json.loads(registry.state_file.read_text())
            live = next(iter(state["attempts"].values()))
            checks["unknown_task_stays_counted_at_deadline"] = ecs_target.active_usage(state["attempts"].values()).tasks == 1 and live["phase"] != "ended"
            checks["attempt_deadline_is_unchanged"] = live["deadline_utc"] == before["deadline_utc"]
    finally:
        broker.close()
        server.pool.shutdown(wait=True, cancel_futures=True)
        server.selector.close()
        os.close(server.wake_read)
        os.close(server.wake_write)
    return checks


@contextmanager
def submission_environment(root: Path, folder: Path, *, enabled: bool = True) -> Iterator[tuple[launcher.Launcher, Broker, SubmissionRequest]]:
    """Own a synthetic registry and transport for a bounded handler scenario, without worker or service startup."""
    registry = run_lock.Registry(folder / "registry", "test")
    runtime = FakeEcs(registry.state_file)
    runs = folder / "runs"
    config = launcher.Config(
        registry,
        root,
        str(folder / "unavailable.sock"),
        folder / "tokens",
        runs,
        ecs_runtime=runtime,
        secrets_endpoint="http://127.0.0.1:1",
        aws_profile="synthetic",
        aws_region="us-west-2",
    )
    server = launcher.Launcher(config)
    server.ecs_jobs = {**server.ecs_jobs, "enabled": enabled}
    server.status, server.docker_ok = "serving", True
    broker = Broker(BrokerPaths(registry, folder / "broker"), load_templates(), InProcessClient(server), repo_root=root, runs_root=runs)
    request = SubmissionRequest("gold_validate", "20261023T000000Z-01234567", "publish_fixture", 1, "gold_dag", "validate", "manual")
    run_folder = runs / request.run_id
    run_folder.mkdir(parents=True)
    (run_folder / "staging.duckdb").write_bytes(b"synthetic unused database")
    try:
        with mock.patch("orchestration.launcher.time.time", return_value=NOW):
            yield server, broker, request
    finally:
        broker.close()
        server.pool.shutdown(wait=True, cancel_futures=True)
        server.selector.close()
        os.close(server.wake_read)
        os.close(server.wake_write)


def registry_state(server: launcher.Launcher) -> dict[str, Any]:
    """Read only the synthetic registry after a real handler transaction has committed."""
    with server.tx(write=False) as state:
        return copy.deepcopy(state)


def fallback_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Drive real admission, preparation completion and local reservation up to the Docker creation boundary."""
    checks: dict[str, bool] = {}
    with submission_environment(root, folder / "occupied") as (server, broker, request):
        runtime = server.ecs_runtime
        if not isinstance(runtime, FakeEcs):
            raise RuntimeError("the fixture runtime is missing")
        runtime.view = ecs_target.Availability(reason="breaker", observed_at=NOW, portable=True)
        broker.submit(request)
        with server.tx() as state:
            state["allocations"]["mac-existing"] = {
                "id": "mac-existing",
                "kind": "shared",
                "state": "held",
                "holder_pid": os.getpid(),
                "holder_start": run_lock.process_start(os.getpid()),
            }
        with mock.patch.object(server, "spawn") as spawn, mock.patch.object(server, "start_create") as create:
            server.advance_all()
            state = registry_state(server)
            attempt = next(iter(state["attempts"].values()))
            checks["occupied_mac_waits_without_allocation"] = (
                attempt["phase"] == "queued"
                and attempt["waiting_on"] == "breaker"
                and "placement_decision" not in attempt
                and attempt["execution"]["live_try"] is None
                and list(state["allocations"]) == ["mac-existing"]
                and not spawn.called
                and not create.called
            )
            deadline = attempt["deadline_utc"]
            with mock.patch("orchestration.launcher.time.time", return_value=deadline):
                server.advance_all()
            ended = next(iter(registry_state(server)["attempts"].values()))
            checks["occupied_mac_expires_at_original_deadline"] = (
                ended["phase"] == "ended"
                and ended["reason"] == "timeout"
                and ended["timeout_intent"]["reason"] == "start_after_deadline"
                and ended["deadline_utc"] == deadline
                and ended["execution"]["queued_at"] == NOW
                and not runtime.calls
            )
    for label, memory_gib in (("free", 4), ("insufficient", 1)):
        with submission_environment(root, folder / label) as (server, broker, request):
            runtime = server.ecs_runtime
            if not isinstance(runtime, FakeEcs):
                raise RuntimeError("the fixture runtime is missing")
            runtime.view = ecs_target.Availability(reason="breaker", observed_at=NOW, portable=True)
            broker.submit(request)
            with mock.patch.object(server, "spawn") as spawn, mock.patch.object(server, "start_create", return_value=False) as create:
                server.advance_all()
                preparing = next(iter(registry_state(server)["attempts"].values()))
                checks[label + "_fallback_reaches_prepare"] = preparing["preparing"] and spawn.call_count == 1 and spawn.call_args.args[1] == "prepare"
                server.on_prepare(
                    "prepare:" + preparing["id"], "ok", {"image_error": None, "plan": {"environment": {"JOB_MEMORY_LIMIT": memory_gib * 1024**3}, "record": {}}}
                )
                server.advance_all()
                state = registry_state(server)
                attempt = state["attempts"][preparing["id"]]
                placements = attempt["execution"]["placements"]
                if label == "free":
                    checks["free_mac_reserves_one_docker_try"] = (
                        len(placements) == 1
                        and placements[0]["target"] == "docker"
                        and placements[0]["state"] == "intent"
                        and attempt["execution"]["live_try"] == placements[0]["try_id"]
                        and create.call_count == 1
                        and state["allocations"][attempt["attempt_alloc"]]["state"] == "held"
                        and not runtime.calls
                    )
                    server.advance_all()
                    repeated = registry_state(server)["attempts"][preparing["id"]]
                    checks["free_mac_repeat_keeps_single_try"] = (
                        repeated["execution"]["placements"] == placements and len(registry_state(server)["allocations"]) == 1
                    )
                else:
                    checks["insufficient_mac_plan_releases_allocation"] = (
                        attempt["phase"] == "queued"
                        and attempt["waiting_on"] == "the fresh Mac plan cannot fit the job"
                        and attempt["attempt_alloc"] is None
                        and attempt["plan_env"] is None
                        and not attempt["prepared"]
                        and not placements
                        and not create.called
                        and all(item["state"] == "released" for item in state["allocations"].values())
                    )
                    server.advance_all()
                    waiting = registry_state(server)["attempts"][preparing["id"]]
                    checks["insufficient_mac_plan_waits_before_retry"] = not waiting["preparing"] and spawn.call_count == 1 and not create.called
                    with mock.patch("orchestration.launcher.time.time", return_value=NOW + server.CAPACITY_RETRY):
                        server.advance_all()
                    retrying = registry_state(server)["attempts"][preparing["id"]]
                    checks["insufficient_mac_plan_retries_after_capacity_interval"] = retrying["preparing"] and spawn.call_count == 2 and not create.called
                    with mock.patch("orchestration.launcher.time.time", return_value=attempt["deadline_utc"]):
                        server.advance_all()
                    ended = registry_state(server)["attempts"][preparing["id"]]
                    checks["insufficient_mac_plan_expires_without_dispatch"] = (
                        ended["phase"] == "ended"
                        and ended["reason"] == "timeout"
                        and ended["timeout_intent"]["reason"] == "start_after_deadline"
                        and not create.called
                        and not runtime.calls
                    )
    with submission_environment(root, folder / "recovery") as (server, broker, request):
        runtime = server.ecs_runtime
        if not isinstance(runtime, FakeEcs):
            raise RuntimeError("the fixture runtime is missing")
        runtime.view = ecs_target.Availability(reason="breaker", observed_at=NOW, portable=True)
        broker.submit(request)
        with mock.patch.object(server, "spawn") as spawn, mock.patch.object(server, "start_create", return_value=False):
            server.advance_all()
            attempt = next(iter(registry_state(server)["attempts"].values()))
            server.on_prepare("prepare:" + attempt["id"], "ok", {"image_error": None, "plan": {"environment": {"JOB_MEMORY_LIMIT": 1024**3}, "record": {}}})
            server.advance_all()
            runtime.view = ecs_target.Availability(portable=True)
            with mock.patch("orchestration.launcher.time.time", return_value=NOW + 1):
                server.advance_all()
            recovered = registry_state(server)["attempts"][attempt["id"]]
            checks["aws_recovery_precedes_mac_plan_retry"] = (
                len(runtime.calls) == 1
                and runtime.calls[0]["target"] == "ecs"
                and spawn.call_count == 1
                and recovered["deadline_utc"] == attempt["deadline_utc"]
                and recovered["attempt_alloc"] is None
            )
    with submission_environment(root, folder / "prepared_recovery") as (server, broker, request):
        runtime = server.ecs_runtime
        if not isinstance(runtime, FakeEcs):
            raise RuntimeError("the fixture runtime is missing")
        runtime.view = ecs_target.Availability(reason="breaker", observed_at=NOW, portable=True)
        broker.submit(request)
        with mock.patch.object(server, "spawn") as spawn, mock.patch.object(server, "start_create", return_value=False) as create:
            server.advance_all()
            preparing = next(iter(registry_state(server)["attempts"].values()))
            server.on_prepare(
                "prepare:" + preparing["id"], "ok", {"image_error": None, "plan": {"environment": {"JOB_MEMORY_LIMIT": 4 * 1024**3}, "record": {}}}
            )
            prepared = registry_state(server)
            attempt = prepared["attempts"][preparing["id"]]
            allocation_id = attempt["attempt_alloc"]
            checks["aws_recovery_starts_with_held_mac_allocation"] = (
                attempt["prepared"]
                and not attempt["preparing"]
                and attempt["execution"]["live_try"] is None
                and prepared["allocations"][allocation_id]["state"] == "held"
                and not create.called
                and not runtime.calls
            )
            observed: list[bool] = []
            original_dispatch = runtime.dispatch

            def observe_dispatch(saved: dict[str, Any], placement: dict[str, Any]) -> Any:
                state = registry_state(server)
                observed.append(state["allocations"][allocation_id]["state"] == "released" and state["attempts"][preparing["id"]]["attempt_alloc"] is None)
                return original_dispatch(saved, placement)

            runtime.view = ecs_target.Availability(portable=True)
            with mock.patch.object(runtime, "dispatch", side_effect=observe_dispatch):
                server.advance_all()
            recovered = registry_state(server)
            attempt = recovered["attempts"][preparing["id"]]
            checks["aws_dispatch_releases_prepared_mac_allocation"] = (
                observed == [True]
                and len(runtime.calls) == 1
                and runtime.calls[0]["target"] == "ecs"
                and recovered["allocations"][allocation_id]["state"] == "released"
                and attempt["attempt_alloc"] is None
                and attempt["plan_env"] is None
                and not attempt["prepared"]
                and attempt["deadline_utc"] == preparing["deadline_utc"]
                and spawn.call_count == 1
                and not create.called
            )
    return checks


def refusal_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Verify exact client-visible errors and compatibility through authenticated submission."""
    checks: dict[str, bool] = {}
    for enabled in (False, True):
        with submission_environment(root, folder / ("enabled" if enabled else "disabled"), enabled=enabled) as (server, broker, request):
            broker.ensure_run(request)
            principal = broker.principal(request.run_id)
            try:
                broker.client.call(
                    "submit", {"template": "pipeline_probe", "launch_id": "bad-class", "params": {"class": "unknown", "case": "traffic"}}, principal
                )
            except launcher.ClientError as error:
                checks[f"invalid_probe_class_is_argument_error_{enabled}"] = error.code == "args" and str(error) == "the probe class is unknown"
            except pipeline_protocol.PipelineError:
                checks[f"invalid_probe_class_is_argument_error_{enabled}"] = False
            else:
                checks[f"invalid_probe_class_is_argument_error_{enabled}"] = False
            with mock.patch.object(launcher.pipeline_render, "job_class", wraps=launcher.pipeline_render.job_class) as classify:
                with suppress(launcher.ClientError):
                    broker.client.call("submit", {"template": "fixture_dbt", "launch_id": "local-class", "args": ["--version"]}, principal)
                checks[f"non_pipeline_submit_skips_classifier_{enabled}"] = all(call.args[0] != "fixture_dbt" for call in classify.call_args_list)
            if not enabled:
                broker.submit(SubmissionRequest(**{**request.__dict__, "target": "docker"}))
                attempts = list(registry_state(server)["attempts"].values())
                checks["disabled_switch_preserves_explicit_docker"] = len(attempts) == 1 and attempts[0]["phase"] == "queued" and "execution" not in attempts[0]
                continue
            with server.tx() as state:
                state["runs"][request.run_id]["mode"] = "real"
                state["launcher"]["catalog_authority"] = {"epoch": 7, "target": "aws", "frozen": False}
            try:
                broker.client.call(
                    "submit", {"template": "pipeline_publish", "launch_id": "writer", "params": {"layer": "gold", "step": "candidates"}}, principal, "writer"
                )
            except launcher.ClientError as error:
                checks["writer_submission_refused"] = error.code == "refused" and str(error) == "AWS writers are not enabled in the read increment"
            else:
                checks["writer_submission_refused"] = False
            with server.tx() as state:
                state["launcher"]["catalog_authority"]["frozen"] = True
            try:
                broker.client.call(
                    "submit", {"template": "pipeline_spark", "launch_id": "frozen-read", "params": {"layer": "silver", "job": "pins"}}, principal, "frozen-read"
                )
            except launcher.ClientError as error:
                checks["frozen_catalog_submission_refused"] = (
                    error.code == "refused" and str(error) == "AWS lakehouse execution requires an unfrozen authoritative AWS catalog"
                )
            else:
                checks["frozen_catalog_submission_refused"] = False
    return checks


def authority_and_cap_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Check recovered authority records, dispatch fencing and real registry resource accounting."""
    checks: dict[str, bool] = {}
    with submission_environment(root, folder / "offline") as (server, broker, request):
        with server.tx() as state:
            state["launcher"]["catalog_authority"] = {"epoch": 7, "target": "aws", "frozen": False}
        handle = broker.submit(request)
        attempt = next(iter(registry_state(server)["attempts"].values()))
        checks["offline_submit_has_no_catalog_epoch"] = attempt["execution"]["catalog_epoch"] is None
        with server.tx() as state:
            state["launcher"]["catalog_authority"]["frozen"] = True
        server.advance_all()
        checks["offline_submit_survives_catalog_freeze"] = len(getattr(server.ecs_runtime, "calls", [])) == 1
        try:
            replay = broker.submit(SubmissionRequest(**{**request.__dict__, "target": "auto"}))
        except SubmissionRefused:
            checks["broker_omitted_and_auto_replay_match"] = False
        else:
            checks["broker_omitted_and_auto_replay_match"] = replay == handle and len(getattr(server.ecs_runtime, "calls", [])) == 1
    for change in ("stale", "freeze", "cancel", "deadline"):
        with submission_environment(root, folder / change) as (server, broker, request):
            broker.submit(request)
            actions: list[Any] = []
            with server.tx() as state:
                attempt = next(iter(state["attempts"].values()))
                attempt["template"], attempt["params"] = "pipeline_spark", {"layer": "silver", "job": "pins"}
                attempt["execution"]["catalog_epoch"] = 7
                state["launcher"]["catalog_authority"] = {"epoch": 7, "target": "aws", "frozen": False}
                server.advance_target(state, attempt, actions)
            with server.tx() as state:
                attempt = next(iter(state["attempts"].values()))
                if change == "stale":
                    state["launcher"]["catalog_authority"]["epoch"] = 8
                elif change == "freeze":
                    state["launcher"]["catalog_authority"]["frozen"] = True
                elif change == "cancel":
                    attempt["cancel_intent"] = {"reason": "cancelled", "at": "2026-10-23T00:00:00Z"}
            at = attempt["deadline_utc"] if change == "deadline" else NOW
            with mock.patch("orchestration.launcher.time.time", return_value=at):
                for action in actions:
                    action()
            attempt = next(iter(registry_state(server)["attempts"].values()))
            checks[f"dispatch_fences_{change}_before_effect"] = (
                attempt["phase"] == "ended"
                and attempt["reason"] == "fenced"
                and attempt["execution"]["live_try"] is None
                and attempt["execution"]["placements"][0]["state"] == "fenced"
                and attempt["execution"]["placements"][0]["no_start"] is True
                and not getattr(server.ecs_runtime, "calls", [])
            )
    with submission_environment(root, folder / "cap") as (server, broker, request):
        server.status, server.docker_ok = "degraded", False
        runtime = server.ecs_runtime
        if not isinstance(runtime, FakeEcs):
            raise RuntimeError("the fixture runtime is missing")
        for number in range(1, 5):
            broker.submit(SubmissionRequest(**{**request.__dict__, "attempt": number}))
            server.advance_all()
            if number <= 3:
                runtime.states[runtime.calls[-1]["try_id"]] = ("pending", "unknown", "stopping")[number - 1]
        state = registry_state(server)
        queued = [item for item in state["attempts"].values() if item["execution"]["live_try"] is None]
        checks["registry_cap_blocks_fourth_dispatch"] = (
            len(runtime.calls) == 3 and len(queued) == 1 and queued[0]["waiting_on"] == "cloud_cap" and not state["allocations"]
        )
        checks["registry_cap_counts_pending_unknown_stopping"] = ecs_target.active_usage(state["attempts"].values()) == ecs_target.Usage(3, 3072, 12288)
        checks["registry_cap_snapshot_precedes_each_dispatch"] = [item["cap_before"]["tasks"] for item in runtime.calls] == [0, 1, 2]
    return checks


def history_and_local_status_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Recover fenced placements through the real queue handler and keep degraded local status from looking live."""
    checks: dict[str, bool] = {}
    with submission_environment(root, folder / "history") as (server, broker, request):
        runtime = server.ecs_runtime
        if not isinstance(runtime, FakeEcs):
            raise RuntimeError("the fixture runtime is missing")
        broker.submit(request)
        server.advance_all()
        with server.tx() as state:
            attempt = next(iter(state["attempts"].values()))
            deadline = attempt["deadline_utc"]
            ecs_target.fenced(attempt, attempt["execution"]["live_try"], NOW, no_start=True)
        runtime.view = ecs_target.Availability(reason="capacity", observed_at=NOW, portable=True)
        server.advance_all()
        checks["queue_uses_saved_spot_no_start"] = len(runtime.calls) == 2 and runtime.calls[-1]["capacity_provider"] == "FARGATE"
        with server.tx() as state:
            attempt = next(iter(state["attempts"].values()))
            if attempt["execution"]["live_try"] is not None:
                ecs_target.fenced(attempt, attempt["execution"]["live_try"], NOW, no_start=True)
        with mock.patch.object(server, "spawn") as spawn, mock.patch.object(server, "start_create", return_value=False):
            server.advance_all()
            attempt = next(iter(registry_state(server)["attempts"].values()))
            if attempt.get("preparing"):
                server.on_prepare(
                    "prepare:" + attempt["id"], "ok", {"image_error": None, "plan": {"environment": {"JOB_MEMORY_LIMIT": 4 * 1024**3}, "record": {}}}
                )
                server.advance_all()
            attempt = next(iter(registry_state(server)["attempts"].values()))
            history = attempt["execution"]["placements"]
            checks["queue_recovery_keeps_one_on_demand_then_mac"] = (
                len(runtime.calls) == 2
                and spawn.call_count == 1
                and len(history) == 3
                and [item["capacity_provider"] for item in history] == ["FARGATE_SPOT", "FARGATE", None]
                and [item["state"] for item in history] == ["fenced", "fenced", "intent"]
                and history[-1]["target"] == "docker"
                and attempt["deadline_utc"] == deadline
                and attempt["execution"]["queued_at"] == NOW
            )
    with submission_environment(root, folder / "local_status", enabled=False) as (server, broker, request):
        handle = broker.submit(request)
        server.ecs_jobs = {**server.ecs_jobs, "enabled": True}
        server.status, server.docker_ok = "degraded", False
        try:
            broker.status(handle)
        except LauncherUnavailable as error:
            checks["degraded_local_status_stays_unavailable"] = "the launcher is degraded" in str(error)
        else:
            checks["degraded_local_status_stays_unavailable"] = False
    with submission_environment(root, folder / "forced_docker") as (server, broker, request):
        handle = broker.submit(SubmissionRequest(**{**request.__dict__, "target": "docker"}))
        server.status, server.docker_ok = "degraded", False
        try:
            broker.status(handle)
        except LauncherUnavailable as error:
            checks["forced_docker_status_requires_local_serving"] = "the launcher is degraded" in str(error)
        else:
            checks["forced_docker_status_requires_local_serving"] = False
    with submission_environment(root, folder / "metadata") as (server, broker, request):
        server.status, server.docker_ok = "degraded", False
        broker.ensure_run(request)
        state = registry_state(server)
        checks["degraded_acquire_adds_no_compute"] = len(state["runs"]) == 1 and not state["attempts"] and not state["allocations"] and not state["operations"]
        try:
            broker.client.call(
                "submit", {"template": "fixture_dbt", "launch_id": "degraded-local", "args": ["--version"]}, broker.principal(request.run_id), "degraded-local"
            )
        except launcher.ClientError as error:
            checks["degraded_unlisted_submit_stays_refused"] = error.code == "degraded"
        else:
            checks["degraded_unlisted_submit_stays_refused"] = False
    return checks


def broker_local_poll_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Read local attempts once through the real broker with the rollout switch disabled."""
    checks: dict[str, bool] = {}
    for phase, expected in (("queued", State.PENDING), ("started", State.RUNNING)):
        with submission_environment(root, folder / phase, enabled=False) as (server, broker, request):
            handle = broker.submit(request)
            with server.tx() as state:
                attempt = next(iter(state["attempts"].values()))
                attempt["phase"] = phase
                attempt["container_id"] = "a" * 64 if phase == "started" else None
                checks["switch_off_" + phase + "_has_no_execution"] = "execution" not in attempt and not server.ecs_jobs["enabled"]
            original = broker.call
            calls: list[tuple[str, str | None]] = []

            def bounded_call(
                method: str,
                params: dict[str, Any] | None = None,
                *args: Any,
                calls: list[tuple[str, str | None]] = calls,
                original: Any = original,
                **kwargs: Any,
            ) -> Any:
                calls.append((method, (params or {}).get("what")))
                if len(calls) > 2:
                    raise LauncherUnavailable("synthetic launcher unavailable after the attempt reply")
                return original(method, params, *args, **kwargs)

            status = None
            with mock.patch.object(broker, "call", side_effect=bounded_call), suppress(LauncherUnavailable):
                status = broker.status(handle)
            checks["switch_off_" + phase + "_poll_reads_attempt_once"] = (
                status is not None and status.state == expected and status.execution is None and calls == [("status", "launcher"), ("status", "attempt")]
            )
            calls.clear()
            forward = broker.read_forward(request)
            if forward is None:
                raise RuntimeError("the local submission has no forward record")

            def attempt_only(
                method: str,
                params: dict[str, Any] | None = None,
                *args: Any,
                calls: list[tuple[str, str | None]] = calls,
                original: Any = original,
                **kwargs: Any,
            ) -> Any:
                calls.append((method, (params or {}).get("what")))
                if calls != [("status", "attempt")]:
                    raise LauncherUnavailable("synthetic launcher unavailable after the attempt reply")
                return original(method, params, *args, **kwargs)

            error = None
            with mock.patch.object(broker, "call", side_effect=attempt_only):
                try:
                    broker.apply_attempt(request, forward)
                except LauncherUnavailable as unavailable:
                    error = unavailable
            checks["switch_off_" + phase + "_apply_needs_no_health_read"] = error is None and calls == [("status", "attempt")]
    return checks


def submission_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Exercise the actual broker and launcher handlers with bounded synthetic transport and no backend process."""
    return {
        **dispatch_cases(root, folder / "dispatch"),
        **fallback_cases(root, folder / "fallback"),
        **refusal_cases(root, folder / "refusals"),
        **authority_and_cap_cases(root, folder / "authority"),
        **history_and_local_status_cases(root, folder / "history"),
        **broker_local_poll_cases(root, folder / "local_poll"),
    }
