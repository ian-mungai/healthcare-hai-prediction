"""Exercise owned-worker stop and restart contracts with synthetic OS observations and real persistence."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import json
import os
import signal
import stat
import sys
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

from orchestration.ecs_api import EcsApi
from orchestration.worker_pool import WorkerConfig
from orchestration.worker_recovery import WorkerLedger
from scripts import process_group

ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATHS = (
    "scripts/process_group.py",
    "services/hai/bootstrap/supervisor.py",
    "orchestration/worker_recovery.py",
    "orchestration/worker_pool.py",
    "orchestration/ecs_api.py",
    "orchestration/sdk_worker.py",
    "orchestration/worker_protocol.py",
    "scripts/orchestration/sdk_lifecycle_cases.py",
    "scripts/orchestration/sdk_owned_cases.py",
)


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.001, seconds)


class _ObservedLock:
    def __init__(self) -> None:
        self.inner = threading.RLock()
        self.depth = 0
        self.denied = False
        self.timeouts: list[float] = []

    def acquire(self, *, timeout: float = -1) -> bool:
        self.timeouts.append(timeout)
        if self.denied or not self.inner.acquire(timeout=timeout):
            return False
        self.depth += 1
        return True

    def release(self) -> None:
        self.depth -= 1
        self.inner.release()

    def __enter__(self) -> _ObservedLock:
        self.acquire()
        return self

    def __exit__(self, *args: object) -> None:
        self.release()


class _World:
    def __init__(self, module: ModuleType, *, child: bool = True) -> None:
        self.module = module
        self.clock = _Clock()
        self.lock = _ObservedLock()
        self.pid = 1000101
        self.handle: Any
        if module is process_group:
            self.handle = process_group.OwnedProcess(
                self.pid, self.pid, -1, -1, -1, "synthetic_witness", "synthetic_nonce", "current", "control", "current", "synthetic", "synthetic"
            )
        else:
            self.handle = module.Handle(self.pid, self.pid, -1, -1, -1)
        self.handle.lock = self.lock
        self.members = {self.pid: "R", **({self.pid + 1: "R"} if child else {})}
        self.events: list[tuple[str, bool, bool, int]] = []
        self.scans = 0
        self.waits = 0
        self.reaped = False
        self.denied_signals = False
        self.group_after_reap = "gone"
        self.failed_scan: str | None = None
        self.missing_leader = False
        self.lost_witness = False
        self.first_scan_unwitnessed = False
        self.closed: list[int] = []

    def _record(self, kind: str, number: int = 0) -> None:
        self.events.append((kind, self.lock.depth > 0, self.reaped, number))

    def killpg(self, group: int, number: int) -> None:
        self._record("probe" if number == 0 else "signal", number)
        if number == 0:
            if self.reaped and self.group_after_reap == "gone":
                raise ProcessLookupError
            if self.group_after_reap == "denied":
                raise PermissionError
            return
        if self.denied_signals:
            raise PermissionError
        states: dict[int, str] = {signal.SIGSTOP: "T", signal.SIGCONT: "R", signal.SIGKILL: "Z"}
        state = states[number]
        self.members = {pid: previous if previous == "Z" else state for pid, previous in self.members.items()}

    def waitpid(self, pid: int, options: int) -> tuple[int, int]:
        self.waits += 1
        if pid != self.pid or options != os.WNOHANG:
            raise ValueError("synthetic_wait_identity_mismatch")
        if self.reaped:
            raise ChildProcessError
        if self.members.get(pid) == "Z":
            self._record("reap")
            self.reaped = True
            return pid, 0
        return 0, 0

    def snapshot(self, deadline: float, witness: str | None = None) -> process_group.Snapshot:
        self.scans += 1
        if self.failed_scan is not None:
            return process_group.Snapshot(self.failed_scan)
        members = tuple(
            process_group.Member(
                pid,
                self.pid,
                state,
                "synthetic_worker",
                not (self.first_scan_unwitnessed and self.scans == 1) and not (self.lost_witness and self.scans >= 2),
            )
            for pid, state in self.members.items()
            if not (self.missing_leader and pid == self.pid)
        )
        return process_group.Snapshot("seen", members)

    def member_states(self, group: int, deadline: float) -> dict[int, str] | None:
        snapshot = self.snapshot(deadline)
        return {member.pid: member.state for member in snapshot.members} if snapshot.status == "seen" else None

    def stop(self, budget: float = 0.3) -> dict[str, Any]:
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(self.module.time, "monotonic", side_effect=self.clock.monotonic))
            stack.enter_context(patch.object(self.module.time, "sleep", side_effect=self.clock.sleep))
            stack.enter_context(patch.object(self.module.os, "killpg", side_effect=self.killpg))
            stack.enter_context(patch.object(self.module.os, "waitpid", side_effect=self.waitpid))
            stack.enter_context(patch.object(self.module.os, "close", side_effect=self.closed.append))
            if self.module is process_group:
                stack.enter_context(patch.object(self.module, "process_snapshot", side_effect=self.snapshot))
            else:
                stack.enter_context(patch.object(self.module, "_members", side_effect=self.member_states))
            result = self.module.stop_owned(self.handle, self.clock.now + budget)
        return {"status": result.status, "confirmed": result.confirmed} if self.module is process_group else dict(result)


def _supervisor() -> ModuleType:
    spec = importlib.util.spec_from_file_location("hai_owned_supervisor_cases", ROOT / "services/hai/bootstrap/supervisor.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("synthetic_supervisor_loader_unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _stop_cases(module: ModuleType, prefix: str) -> dict[str, bool]:
    checks = {}
    world = _World(module)
    result = world.stop()
    signals = [event[3] for event in world.events if event[0] == "signal"]
    checks[f"{prefix}_last_signal_precedes_reap"] = signals == [signal.SIGSTOP, signal.SIGKILL] and result["confirmed"] and world.reaped
    checks[f"{prefix}_signals_and_reap_hold_same_lock"] = all(event[1] for event in world.events if event[0] in {"signal", "reap"})
    checks[f"{prefix}_no_signal_after_reap"] = all(not event[2] for event in world.events if event[0] == "signal")
    before_counts = len(world.events), world.scans, world.waits
    world.handle.pgid = 1000201
    late = world.stop()
    checks[f"{prefix}_reaped_handle_refuses_reused_number"] = (
        late["status"] == "refused_reaped" and world.handle.state == "read_only" and before_counts == (len(world.events), world.scans, world.waits)
    )
    world = _World(module, child=False)
    world.members[world.pid] = "Z"
    result = world.stop()
    checks[f"{prefix}_lone_zombie_reaps_without_signal"] = result["confirmed"] and [event[0] for event in world.events] == ["reap", "probe"]
    world = _World(module)
    world.members = {world.pid: "Z", world.pid + 1: "R"}
    result = world.stop()
    kill_positions = [index for index, event in enumerate(world.events) if event[0] == "signal" and event[3] == signal.SIGKILL]
    reap_positions = [index for index, event in enumerate(world.events) if event[0] == "reap"]
    checks[f"{prefix}_zombie_leader_waits_for_descendant_kill"] = (
        result["confirmed"]
        and world.handle.state == "read_only"
        and bool(kill_positions)
        and len(reap_positions) == 1
        and max(kill_positions) < reap_positions[0]
    )
    for outcome in ("alive", "denied"):
        world = _World(module, child=False)
        world.members[world.pid] = "Z"
        world.group_after_reap = outcome
        result = world.stop()
        before = len(world.events)
        late = world.stop()
        checks[f"{prefix}_post_reap_{outcome}_stays_unproven"] = (
            result["status"] == "worker_unproven"
            and not result["confirmed"]
            and world.handle.state == "read_only"
            and late["status"] == "refused_reaped"
            and len(world.events) == before
        )
    world = _World(module)
    world.denied_signals = True
    result = world.stop()
    checks[f"{prefix}_denied_signals_keep_leader_until_deadline"] = (
        result["status"] == "survivor" and not result["confirmed"] and not world.reaped and world.waits == 0 and world.clock.now <= 100.301
    )
    world.denied_signals = False
    retry = world.stop()
    checks[f"{prefix}_survivor_retry_reaps_after_last_signal"] = retry["confirmed"] and all(
        not event[2] and event[1] for event in world.events if event[0] == "signal"
    )
    world = _World(module)
    world.lock.denied = True
    result = world.stop(budget=0.2)
    checks[f"{prefix}_lock_wait_uses_remaining_deadline"] = (
        result["status"] == "survivor" and not world.events and world.scans == 0 and len(world.lock.timeouts) == 1 and 0 <= world.lock.timeouts[0] <= 0.201
    )
    world = _World(module)
    result = world.stop(budget=-1)
    checks[f"{prefix}_expired_deadline_has_no_effect"] = not result["confirmed"] and world.scans == world.waits == len(world.events) == 0
    world = _World(module)
    world.handle.state = "reap_only"
    first = world.stop()
    first_scans = world.scans
    world.members[world.pid] = "Z"
    second = world.stop()
    checks[f"{prefix}_reap_only_live_has_no_scan_or_signal"] = first["status"] == "refused_unproven" and first_scans == 0
    checks[f"{prefix}_reap_only_eventual_reap_needs_no_scan"] = (
        second["confirmed"]
        and world.reaped
        and world.handle.state == "read_only"
        and world.scans == 0
        and not any(event[0] == "signal" for event in world.events)
    )
    return checks


def _witness_cases() -> dict[str, bool]:
    checks = {}
    world = _World(process_group)
    world.first_scan_unwitnessed = True
    first = world.stop()
    first_state = world.handle.state
    scans = world.scans
    checks["first_scan_unwitnessed_revokes_signals_but_keeps_reap_duty"] = (
        first["status"] == "refused_unproven" and first_state == "reap_only" and not any(event[0] == "signal" for event in world.events)
    )
    world.members[world.pid] = "Z"
    world.group_after_reap = "alive"
    second = world.stop()
    checks["first_scan_unwitnessed_eventual_reap_needs_no_scan"] = (
        world.reaped
        and world.handle.state == "read_only"
        and second["status"] == "worker_unproven"
        and not second["confirmed"]
        and world.scans == scans
        and not any(event[0] == "signal" for event in world.events)
    )
    for failure in ("denied", "timed_out"):
        world = _World(process_group)
        world.failed_scan = failure
        first = world.stop()
        count = world.scans
        first_state = world.handle.state
        world.members[world.pid] = "Z"
        second = world.stop()
        checks[f"witness_{failure}_revokes_signals_but_keeps_reap_duty"] = first_state == "reap_only" and not first["confirmed"]
        checks[f"witness_{failure}_eventual_reap_ignores_scan_failure"] = (
            second["confirmed"] and world.scans == count and world.reaped and not any(event[0] == "signal" for event in world.events)
        )
    world = _World(process_group)
    world.lost_witness = True
    first = world.stop()
    first_state = world.handle.state
    scans = world.scans
    world.members[world.pid] = "Z"
    world.group_after_reap = "alive"
    second = world.stop()
    checks["lost_rescan_witness_resumes_without_kill"] = (
        not first["confirmed"]
        and first_state == "reap_only"
        and [event[3] for event in world.events if event[0] == "signal"] == [signal.SIGSTOP, signal.SIGCONT]
    )
    checks["lost_witness_reaps_with_member_left_without_more_scans"] = (
        world.reaped and not second["confirmed"] and second["status"] == "worker_unproven" and world.scans == scans and world.handle.state == "read_only"
    )
    world = _World(process_group)
    world.missing_leader = True
    result = world.stop()
    checks["missing_leader_row_does_not_claim_actual_reap"] = (
        not result["confirmed"] and world.handle.state == "reap_only" and not world.reaped and not any(event[0] == "signal" for event in world.events)
    )
    return checks


def _disposition_cases(supervisor: ModuleType, folder: Path) -> dict[str, bool]:
    checks = {}
    for parent in ("host", "task"):
        for setter_works in (True, False):
            disposition = [signal.SIG_IGN]
            spawns: list[bool] = []
            handles: list[Any] = []

            def set_signal(number: int, value: Any, *, works: bool = setter_works, state: list[Any] = disposition) -> Any:
                if works:
                    state[0] = value
                return signal.SIG_IGN

            def spawn(*args: Any, state: list[Any] = disposition, calls: list[bool] = spawns, **kwargs: Any) -> int:
                calls.append(state[0] == signal.SIG_DFL)
                return 1000301

            try:
                with (
                    patch.object(signal, "signal", side_effect=set_signal),
                    patch.object(signal, "getsignal", side_effect=lambda number, state=disposition: state[0]),
                    patch.object(os, "posix_spawn", side_effect=spawn),
                    patch.object(supervisor._stderr, "StderrReader", return_value=_Reader()),
                ):
                    if parent == "host":
                        handle = process_group.spawn_owned(
                            [sys.executable, "-I", "-B"],
                            {},
                            witness="synthetic",
                            nonce="synthetic",
                            session="current",
                            lane="control",
                            owner="current",
                            deadline=time.monotonic() + 1,
                        )
                        handles.append(handle)
                    else:
                        owner = supervisor.Supervisor("synthetic", stderr_folder=folder)
                        handles = owner.handles
                        owner._spawn(time.monotonic() + 1, withhold=False)
            except RuntimeError as error:
                checks[f"{parent}_failed_sigchld_check_prevents_spawn"] = (
                    not setter_works
                    and not spawns
                    and str(error) == ("worker_parent_requires_default_sigchld" if parent == "host" else "supervisor_requires_default_sigchld")
                )
            finally:
                for handle in handles:
                    for fd in (handle.stdin, handle.stdout, handle.stderr):
                        os.close(fd)
            if setter_works:
                checks[f"{parent}_sets_and_checks_sigchld_before_spawn"] = spawns == [True]
            else:
                checks.setdefault(f"{parent}_failed_sigchld_check_prevents_spawn", False)
    return checks


def _changed_disposition_case(supervisor: ModuleType, folder: Path) -> dict[str, bool]:
    disposition: list[Any] = [signal.SIG_IGN]
    refused = False
    with (
        patch.object(signal, "signal", side_effect=lambda number, value: disposition.__setitem__(0, value)),
        patch.object(signal, "getsignal", side_effect=lambda number: disposition[0]),
        patch.object(os, "posix_spawn", return_value=1000302) as spawn,
        patch.object(supervisor._stderr, "StderrReader", return_value=_Reader()),
    ):
        owner = supervisor.Supervisor("synthetic", stderr_folder=folder)
        constructed_with_default = disposition[0] == signal.SIG_DFL
        disposition[0] = signal.SIG_IGN
        try:
            owner._spawn(time.monotonic() + 1, withhold=False)
        except RuntimeError as error:
            refused = str(error) == "supervisor_spawn_refused"
        finally:
            for handle in owner.handles:
                for fd in (handle.stdin, handle.stdout, handle.stderr):
                    os.close(fd)
        return {"task_changed_sigchld_before_spawn_is_refused": constructed_with_default and refused and spawn.call_count == 0}


class _Reader:
    failure = None

    def start(self) -> None:
        pass

    def finish(self, *, deadline: float) -> bool:
        return True


def _first_frame_case(folder: Path) -> dict[str, bool]:
    checks = {}
    handle = process_group.OwnedProcess(1000401, 1000401, -1, -1, -1, "", "", "", "control", "", "synthetic", "synthetic")
    frames: list[dict[str, Any]] = []

    def spawn(command: Any, environment: Any, **kwargs: Any) -> process_group.OwnedProcess:
        for name in ("witness", "nonce", "session", "lane", "owner"):
            setattr(handle, name, kwargs[name])
        checks["child_environment_matches_private_witness"] = environment["HAI_WORKER_WITNESS"] == handle.witness
        return handle

    def write(fd: int, frame: dict[str, Any], deadline: float, cancelled: Any) -> None:
        frames.append(frame)
        records = api.ledger.private_open(deadline=deadline)
        checks["private_witness_is_durable_before_first_frame"] = (
            len(records) == 1 and records[0] == handle.spawn_record() and stat.S_IMODE(api.ledger.path.stat().st_mode) == 0o600
        )

    def read(fd: int, deadline: float, cancelled: Any) -> dict[str, Any]:
        return {"id": frames[-1]["id"], "ok": True, "result": {"tasks": [], "failures": []}}

    def stop(handle: process_group.OwnedProcess, deadline: float) -> process_group.StopResult:
        handle.state = "read_only"
        handle.confirmed = True
        return process_group.StopResult("confirmed", handle.pid, handle.pgid, True)

    with (
        patch("orchestration.worker_recovery.process_snapshot", return_value=process_group.Snapshot("seen")),
        patch("orchestration.worker_pool.spawn_owned", side_effect=spawn),
        patch("orchestration.worker_pool.record_leader_start", return_value=None),
        patch("orchestration.worker_pool.StderrReader", return_value=_Reader()),
        patch("orchestration.worker_pool.stop_owned", side_effect=stop),
        patch("orchestration.worker_pool.group_state", return_value="gone"),
        patch("orchestration.ecs_api.write_frame", side_effect=write),
        patch("orchestration.ecs_api.read_frame", side_effect=read),
    ):
        api = EcsApi(WorkerConfig("synthetic", pooled=False), folder)
        try:
            reply = api.call("describe_tasks", {"cluster": "synthetic", "tasks": ["synthetic"], "include": ["TAGS"]}, deadline=time.monotonic() + 2)
            checks["first_frame_path_returns_valid_reply"] = reply["ok"] and len(frames) == 1
            checks["saved_ledger_redacts_private_witness"] = handle.witness not in json.dumps(api.ledger.redacted(deadline=time.monotonic() + 1))
        finally:
            api.close(deadline=time.monotonic() + 1)
    return checks


def _recovery_cases(folder: Path) -> dict[str, bool]:
    checks = {}
    ledger = WorkerLedger(folder / "restart")
    for number in (1000501, 1000502):
        handle = process_group.OwnedProcess(
            number, number, -1, -1, -1, "synthetic_private_witness", f"synthetic_{number}", "previous", "data", "previous", "synthetic", "synthetic"
        )
        ledger.append_spawn(handle.spawn_record(), deadline=time.monotonic() + 1)
    probes: list[int] = []
    state = ["gone"]

    def probe(group: int) -> str:
        probes.append(group)
        return state[0]

    with (
        patch("orchestration.worker_recovery.process_snapshot", return_value=process_group.Snapshot("seen")),
        patch("orchestration.worker_recovery.group_state", side_effect=probe),
        patch.object(os, "killpg") as signals,
        patch.object(os, "waitpid") as reaps,
    ):
        api = EcsApi(WorkerConfig("synthetic"), ledger.registry)
        alert = api.recovery.alert
        checks["restart_first_absence_records_one_alert_and_pauses_dispatch"] = (
            api.dispatch_paused and alert is not None and alert["kind"] == "worker_unproven" and len(alert["entries"]) == 2 and len(probes) == 2
        )
        checks["recovered_entries_are_explicitly_read_only"] = alert is not None and all(entry.get("state") == "read_only" for entry in alert["entries"])
        checks["recovery_alert_excludes_private_witness"] = "synthetic_private_witness" not in json.dumps(alert)
        if not checks["restart_first_absence_records_one_alert_and_pauses_dispatch"]:
            return checks
        with patch.object(api.pool, "acquire", side_effect=RuntimeError("synthetic_admission")) as acquire:
            dispatch = api.call("run_task", {}, deadline=time.monotonic() + 1)
            poll = api.call("describe_tasks", {"cluster": "synthetic", "tasks": ["synthetic"], "include": ["TAGS"]}, deadline=time.monotonic() + 1)
            checks["recovery_blocks_dispatch_but_allows_reconciliation"] = (
                dispatch["error"]["code"] == "worker_recovery_unproven" and poll["error"]["code"] == "worker_admission_refused" and acquire.call_count == 1
            )
        state[0] = "alive"
        present = api.recovery.sweep(deadline=time.monotonic() + 1)
        checks["later_present_probe_does_not_reuse_prior_absence"] = len(present) == 2 and api.dispatch_paused and len(probes) == 4
        for evidence in ("denied", "timed_out"):
            state[0] = evidence
            for token in ("seen", "denied", "timed_out"):
                with patch("orchestration.worker_recovery.process_snapshot", return_value=process_group.Snapshot(token)):
                    entries = api.recovery.sweep(deadline=time.monotonic() + 1)
                checks[f"recovery_{evidence}_{token}_stays_open"] = len(entries) == 2 and api.dispatch_paused
        state[0] = "gone"
        before = len(probes)
        absent = api.recovery.sweep(deadline=time.monotonic() + 1)
        rows = api.ledger.redacted(deadline=time.monotonic() + 1)
        closures = [row for row in rows if row["event"] == "closed_absent"]
        checks["later_current_absence_appends_exactly_one_closure_per_entry"] = (
            absent == []
            and not api.dispatch_paused
            and len(probes) == before + 2
            and len(closures) == 2
            and all(row["evidence"]["group"] == "absent" for row in closures)
        )
        before = len(rows)
        api.recovery.sweep(deadline=time.monotonic() + 1)
        checks["repeated_absence_has_no_duplicate_closure"] = len(api.ledger.redacted(deadline=time.monotonic() + 1)) == before
        checks["restart_and_sweeps_never_signal_or_reap"] = signals.call_count == reaps.call_count == 0
        api.close(deadline=time.monotonic() + 1)
    return checks


def source_hashes() -> dict[str, str]:
    """Freeze the exact source bytes exercised by the isolated ownership cases."""
    return {relative: hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() for relative in SOURCE_PATHS}


def owned_cases(folder: Path) -> dict[str, bool]:
    """Run real lifecycle and persistence paths without a worker process, listener or AWS request."""
    supervisor = _supervisor()
    return {
        **_stop_cases(process_group, "host"),
        **_stop_cases(supervisor, "task"),
        **_witness_cases(),
        **_disposition_cases(supervisor, folder / "stderr"),
        **_changed_disposition_case(supervisor, folder / "changed_disposition"),
        **_first_frame_case(folder / "frame"),
        **_recovery_cases(folder / "recovery"),
    }
