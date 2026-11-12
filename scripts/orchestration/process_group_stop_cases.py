"""Exercise the host stop of scripts/process_group.py against synthetic kernel observations: no process starts and no signal is sent."""

from __future__ import annotations

import contextlib
import os
import signal
import threading
from collections.abc import Iterator
from types import ModuleType
from typing import Any
from unittest.mock import patch

from scripts import process_group


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
        self.handle = process_group.OwnedProcess(
            self.pid, self.pid, -1, -1, -1, "synthetic_witness", "synthetic_nonce", "current", "control", "current", "synthetic", "synthetic"
        )
        self.handle.lock = self.lock
        self.members = {self.pid: "R", **({self.pid + 1: "R"} if child else {})}
        self.events: list[tuple[str, bool, bool, int]] = []
        self.scans = 0
        self.scan_seconds = 0.0
        self.scan_deadlines: list[float] = []
        self.scan_witnesses: list[bool] = []
        self.waits = 0
        self.reaped = False
        self.denied_signals = False
        self.group_after_reap = "gone"
        self.failed_scan: str | None = None
        self.scan_failures = 0
        self.scripted_scans: list[dict[int, str]] = []
        self.unreapable_waits = 0
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
        if self.members.get(pid) in {"Z", "?E"} and self.unreapable_waits > 0:
            # A dying leader is not reapable yet (a zombie whose other threads are still exiting, or an exiting process that is not a zombie yet):
            # the kernel answers a no-hang wait with nothing.
            self.unreapable_waits -= 1
            return 0, 0
        if self.members.get(pid) in {"Z", "?E"}:
            self._record("reap")
            self.reaped = True
            return pid, 0
        return 0, 0

    def snapshot(self, deadline: float, witness: str | None = None) -> process_group.Snapshot:
        self.scans += 1
        self.scan_deadlines.append(deadline)
        self.scan_witnesses.append(witness is not None)
        self.clock.now += min(self.scan_seconds, max(0.0, deadline - self.clock.now))
        if self.scan_failures > 0:
            self.scan_failures -= 1
            return process_group.Snapshot("unreadable")
        if self.failed_scan is not None:
            return process_group.Snapshot(self.failed_scan)
        members = tuple(
            process_group.Member(
                pid,
                self.pid,
                state,
                "synthetic_worker",
                not (self.first_scan_unwitnessed and self.scans == 1) and not (self.lost_witness and self.scans >= 2) and "E" not in state[1:],
            )
            for pid, state in (self.scripted_scans[self.scans - 1] if self.scans <= len(self.scripted_scans) else self.members).items()
            if not (self.missing_leader and pid == self.pid)
        )
        return process_group.Snapshot("seen", members)

    @contextlib.contextmanager
    def observations(self) -> Iterator[None]:
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(self.module.time, "monotonic", side_effect=self.clock.monotonic))
            stack.enter_context(patch.object(self.module.time, "sleep", side_effect=self.clock.sleep))
            stack.enter_context(patch.object(self.module.os, "killpg", side_effect=self.killpg))
            stack.enter_context(patch.object(self.module.os, "waitpid", side_effect=self.waitpid))
            stack.enter_context(patch.object(self.module.os, "close", side_effect=self.closed.append))
            stack.enter_context(patch.object(self.module, "process_snapshot", side_effect=self.snapshot))
            yield

    def stop(self, budget: float = 0.3) -> dict[str, Any]:
        with self.observations():
            result = self.module.stop_owned(self.handle, self.clock.now + budget)
        return {"status": result.status, "confirmed": result.confirmed}


def exit_cases(module: ModuleType = process_group) -> dict[str, bool]:
    """A leader that the kernel reports as exiting is dying, not live without a witness: the host stop keeps it signalling and gives its reap a short grace."""
    checks = {}
    world = _World(module, child=False)
    world.members[world.pid] = "?E"
    world.unreapable_waits = 3
    result = world.stop(budget=4.0)
    checks["host_exiting_leader_is_reaped_within_the_exit_grace_without_a_demotion_or_a_signal"] = (
        result["confirmed"]
        and world.reaped
        and world.handle.state == "read_only"
        and not any(event[0] == "signal" for event in world.events)
        and world.clock.now <= 100.3
    )
    world = _World(module, child=False)
    world.members[world.pid] = "?E"
    world.unreapable_waits = 10**6
    result = world.stop(budget=4.0)
    checks["host_exiting_leader_that_stays_unreapable_is_unconfirmed_after_the_grace_and_stays_signalling"] = (
        result["status"] == "survivor"
        and not result["confirmed"]
        and world.handle.state == "signalling"
        and not any(event[0] == "signal" for event in world.events)
        and not world.reaped
        and 100.2 <= world.clock.now <= 100.4
    )
    world = _World(module, child=False)
    world.members[world.pid] = "?E"
    world.unreapable_waits = 10**6
    first = world.stop(budget=4.0)
    world.unreapable_waits = 0
    second = world.stop(budget=4.0)
    checks["host_a_later_attempt_reaps_an_exiting_leader_that_was_not_reapable_before"] = (
        not first["confirmed"] and second["confirmed"] and world.reaped and not any(event[0] == "signal" for event in world.events)
    )
    world = _World(module)
    world.first_scan_unwitnessed = True
    result = world.stop(budget=4.0)
    checks["host_live_leader_without_a_witness_is_still_demoted_and_never_waited_for"] = (
        not result["confirmed"] and world.handle.state == "reap_only" and not any(event[0] == "signal" for event in world.events) and world.clock.now <= 100.05
    )
    world = _World(module)
    world.members[world.pid] = "?E"
    world.members[world.pid + 1] = "R"
    world.first_scan_unwitnessed = True
    world.unreapable_waits = 10**6
    result = world.stop(budget=4.0)
    checks["host_exiting_leader_with_a_live_unwitnessed_member_is_still_demoted"] = (
        not result["confirmed"] and world.handle.state == "reap_only" and not any(event[0] == "signal" for event in world.events)
    )
    world = _World(module, child=False)
    world.members[world.pid] = "?"
    result = world.stop(budget=4.0)
    signals = [event[3] for event in world.events if event[0] == "signal"]
    checks["host_unknown_state_with_a_witness_is_stopped_and_reaped_like_a_live_member"] = (
        result["confirmed"] and world.reaped and signals == [signal.SIGSTOP, signal.SIGKILL]
    )
    world = _World(module, child=False)
    world.scripted_scans = [{world.pid: "?"}, {world.pid: "R"}]
    world.first_scan_unwitnessed = True
    world.lost_witness = True
    result = world.stop(budget=4.0)
    checks["host_unknown_state_without_a_witness_is_refused_and_never_signalled"] = (
        not result["confirmed"] and world.handle.state == "reap_only" and not any(event[0] == "signal" for event in world.events)
    )
    stuck = _World(module, child=False)
    stuck.members[stuck.pid] = "Z"
    stuck.unreapable_waits = 10**6
    later = _World(module)
    later.clock = stuck.clock
    deadline = stuck.clock.now + 0.2
    one = stuck.stop(budget=0.2)
    two = later.stop(budget=deadline - later.clock.now)
    checks["host_a_dying_unreapable_leader_leaves_a_short_shared_budget_to_the_next_live_worker"] = (
        not one["confirmed"] and two["confirmed"] and later.reaped and later.handle.state == "read_only"
    )
    handles = [_World(module, child=False) for _ in range(2)]
    for handle in handles:
        handle.clock = handles[0].clock
        handle.members[handle.pid] = "Z"
        handle.unreapable_waits = 10**6
    start = handles[0].clock.now
    for handle in handles:
        handle.stop(budget=start + 0.2 - handle.clock.now)
    checks["host_two_unreapable_leaders_leave_more_than_half_of_a_short_budget"] = handles[0].clock.now - start <= 0.12
    handles = [_World(module, child=False) for _ in range(4)]
    for handle in handles:
        handle.clock = handles[0].clock
        handle.members[handle.pid] = "?E"
        handle.unreapable_waits = 10**6
    start = handles[0].clock.now
    for handle in handles:
        handle.stop(budget=start + 1.0 - handle.clock.now)
    checks["host_four_unreapable_leaders_leave_a_quarter_of_the_budget_for_recovery"] = start + 1.0 - handles[0].clock.now >= 0.25
    dying = _World(module, child=False)
    dying.members[dying.pid] = "?E"
    dying.unreapable_waits = 10**6
    waiting = _World(module, child=False)
    waiting.clock = dying.clock
    waiting.members[waiting.pid] = "Z"
    deadline = dying.clock.now + 0.6
    first = dying.stop(budget=0.6)
    second = waiting.stop(budget=deadline - waiting.clock.now)
    checks["host_serial_handles_a_stuck_exiting_leader_leaves_time_for_the_next_handle"] = not first["confirmed"] and second["confirmed"] and waiting.reaped
    return checks
