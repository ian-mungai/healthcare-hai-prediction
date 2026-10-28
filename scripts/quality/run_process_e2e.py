"""E2E for the process launcher's deadline (scripts/process.py ``run_command``): the whole process group stops, not only the child (WV3-027).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_process_e2e``. Each case starts real shell programs through ``run_command``. A
child starts a grandchild (one that ignores TERM in some cases) and the case checks, by process ID, that no member of the group is left once the call
returns or raises. Cases: a grandchild stopped at the deadline, a grandchild and a child that ignore TERM (KILL after the grace period), a grandchild
that holds the output pipe after the child exited, partial output kept in the ``TimeoutExpired``, a caller interrupt that stops the group, a child in
its own group (the caller is never signalled), a survivor that fails visibly, and unchanged results for a program that ends in time (exit code, text,
``check``, a missing program, working folder, environment and a closed stdin). Nested cases start a Python program that itself calls ``run_command``
(one and two levels deep): the outer deadline must stop the nested groups too, through the private registry of owned groups that the launcher passes to
its descendants in ``PROCESS_GROUP_REGISTRIES``; a nested launch keeps its own earlier deadline; a recorded group that has already ended counts as stopped;
the registry is private (mode 600 in a folder with mode 700) and removed, and a write to a removed registry is skipped.
Planted defects run the same cases with a broken stop (the direct child only, TERM only, no confirmation, the own group only). Each plant has the real
parameters of ``stop_process_group`` and counts as caught only when the named check of each named case is false and the case raised nothing.
A member that starts its own session is outside the group; that limit is documented in scripts/process.py and not tested here.
The report goes to data/e2e/process/<UTC>_<pid>/report.json with the revision and the SHA-256 of the files under test.
"""

from __future__ import annotations

import contextlib
import hashlib
import inspect
import json
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from scripts import process
from scripts.process import CompletedProcess, ProcessGroupSurvived, SubprocessError, TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
WATCHED = ("scripts/process.py", "scripts/quality/run_process_e2e.py")
# The grace periods of the cases (the real defaults are checked by a separate case); short, so the cases stay quick.
CASE_GRACE_SECONDS = 1.0
CASE_TIMEOUT_SECONDS = 1.0
# The outer deadline of the nested cases: long enough for a nested Python launcher to start its program on a loaded machine.
NESTED_OUTER_TIMEOUT_SECONDS = 3.0
# The child is one literal bash program; the folder and the modes reach it and its grandchild through the environment. A program that ignores TERM
# does so for its children too (an ignored signal survives exec).
GRANDCHILD = """
# $1 pid file, $2 "term" to ignore TERM
if [ "$2" = term ]; then trap '' TERM; fi
echo $$ >"$1"
while :; do sleep 1; done
"""
CHILD = """
# CHILD_MODE: wait, ignore_term (also ignore TERM) or leave (exit at once); GRANDCHILD_MODE: respect or term (ignore TERM)
if [ "$CHILD_MODE" = ignore_term ]; then trap '' TERM; fi
echo $$ >"$CASE_DIR/child.pid"
echo "pgid $(ps -o pgid= -p $$ | tr -d ' ')" >"$CASE_DIR/child.group"
printf '%s' "${PROCESS_GROUP_REGISTRIES:-}" >"$CASE_DIR/child.registries"
printf 'partial output\\n'
printf 'partial error\\n' >&2
bash -c "$GRANDCHILD_CODE" grandchild "$CASE_DIR/grandchild.pid" "$GRANDCHILD_MODE" &
if [ "$CHILD_MODE" = leave ]; then exit 0; fi
while :; do sleep 1; done
"""
# The nested launcher, written to the case folder as nested.py. Arguments: repository root, case folder, levels, inner timeout, inner grace period, and
# "exit" or "wait" (what it does after the inner timeout). With levels above 1 it starts itself again through run_command; at level 1 it starts CHILD.
# It records its pid, its group and, for each registry it inherited, the registry's mode, its folder's mode and its owner.
NESTED = """
import json, os, stat, sys, time
root, folder, levels, timeout, grace, after = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]), sys.argv[6]
sys.path.insert(0, root)
from scripts.process import TimeoutExpired, run_command
with open(os.path.join(folder, f"level{levels}.pid"), "w") as file:
    file.write(str(os.getpid()))
seen = [path for path in os.environ.get("PROCESS_GROUP_REGISTRIES", "").split(":") if path]
details = []
for path in seen:
    item = {"path": path, "exists": os.path.exists(path)}
    if item["exists"]:
        item.update(mode=stat.S_IMODE(os.stat(path).st_mode), folder_mode=stat.S_IMODE(os.stat(os.path.dirname(path)).st_mode), owner=os.stat(path).st_uid)
    details.append(item)
with open(os.path.join(folder, f"level{levels}.json"), "w") as file:
    json.dump({"group": os.getpgrp(), "registries": details}, file)
try:
    if levels > 1:
        run_command(sys.executable, ["-B", __file__, root, folder, str(levels - 1), str(timeout), str(grace), after], timeout=60)
    else:
        run_command("bash", ["-c", os.environ.get("NESTED_CHILD_CODE", "")], timeout=timeout, grace_seconds=grace)
except TimeoutExpired:
    print("inner timeout", flush=True)
    if after == "wait":
        while True:
            time.sleep(1)
"""


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class Scratch:
    """One case folder for the pid files; every pid found is killed at the end so no case leaves a process."""

    def __init__(self, base: Path, name: str) -> None:
        self.folder = base / name
        self.folder.mkdir(parents=True)

    def environment(self, grandchild_mode: str, child_mode: str) -> dict[str, str]:
        return {
            **os.environ,
            "CASE_DIR": str(self.folder),
            "GRANDCHILD_CODE": GRANDCHILD,
            "GRANDCHILD_MODE": grandchild_mode,
            "CHILD_MODE": child_mode,
            "NESTED_CHILD_CODE": CHILD,
        }

    def run(self, grandchild_mode: str, child_mode: str, timeout: float) -> CompletedProcess[str]:
        """Start the child through the launcher; its environment names the folder and the modes."""
        return run_command("bash", ["-c", CHILD], cwd=ROOT, env=self.environment(grandchild_mode, child_mode), timeout=timeout)

    def run_nested(self, levels: int, inner_timeout: float, after: str, timeout: float) -> CompletedProcess[str]:
        """Start the nested launcher through the launcher; its innermost program is CHILD with a grandchild that ignores TERM."""
        script = self.folder / "nested.py"
        script.write_text(NESTED, encoding="utf-8")
        arguments = ["-B", str(script), str(ROOT), str(self.folder), str(levels), str(inner_timeout), str(CASE_GRACE_SECONDS), after]
        return run_command(sys.executable, arguments, cwd=ROOT, env=self.environment("term", "wait"), timeout=timeout)

    def pids(self) -> dict[str, int]:
        found: dict[str, int] = {}
        for file in self.folder.glob("*.pid"):
            if file.read_text().strip():
                found[file.stem] = int(file.read_text().strip())
        return found

    def registries(self) -> list[str]:
        """Every registry path a nested launcher or the child saw."""
        seen: list[str] = []
        for file in self.folder.glob("level*.json"):
            seen.extend(item["path"] for item in json.loads(file.read_text())["registries"])
        child = self.folder / "child.registries"
        if child.exists():
            seen.extend(path for path in child.read_text().split(":") if path)
        return sorted(set(seen))

    def cleanup(self) -> None:
        for pid in self.pids().values():
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(pid, signal.SIGKILL)
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(pid, signal.SIGKILL)


@contextlib.contextmanager
def short_grace(survivor_probe: Callable[[int], bool] | None = None) -> Iterator[None]:
    saved = (process.STOP_GRACE_SECONDS, process.CONFIRM_SECONDS)
    process.STOP_GRACE_SECONDS, process.CONFIRM_SECONDS = CASE_GRACE_SECONDS, CASE_GRACE_SECONDS
    try:
        if survivor_probe is None:
            yield
        else:
            with mock.patch("scripts.process.group_alive", survivor_probe):
                yield
    finally:
        process.STOP_GRACE_SECONDS, process.CONFIRM_SECONDS = saved


def timed_out(scratch: Scratch, grandchild_mode: str, child_mode: str) -> tuple[BaseException | None, float]:
    started = time.monotonic()
    caught: BaseException | None = None
    try:
        scratch.run(grandchild_mode, child_mode, CASE_TIMEOUT_SECONDS)
    except TimeoutExpired as error:
        caught = error
    return caught, time.monotonic() - started


def case(name: str, base: Path, body: Callable[[Scratch], dict[str, Any]]) -> dict[str, Any]:
    scratch = Scratch(base, name)
    started = time.monotonic()
    try:
        with short_grace():
            checks = body(scratch)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, SubprocessError) as error:
        checks = {"raised": f"{type(error).__name__}: {error}"}
    finally:
        scratch.cleanup()
    flags = {key: value for key, value in checks.items() if isinstance(value, bool)}
    return {
        "case": name,
        "passed": bool(flags) and all(flags.values()) and "raised" not in checks,
        "seconds": round(time.monotonic() - started, 1),
        "checks": checks,
    }


def stopped(scratch: Scratch, caught: BaseException | None, elapsed: float) -> dict[str, Any]:
    pids = scratch.pids()
    return {
        "raised_timeout": isinstance(caught, TimeoutExpired),
        "child_started": "child" in pids,
        "grandchild_started": "grandchild" in pids,
        "child_gone": "child" in pids and not alive(pids["child"]),
        "grandchild_gone": "grandchild" in pids and not alive(pids["grandchild"]),
        "group_gone": "child" in pids and not process.group_alive(pids["child"]),
        "seconds": round(elapsed, 2),
    }


def grandchild_stopped(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught, elapsed = timed_out(scratch, "respect", "wait")
        return stopped(scratch, caught, elapsed)

    return case("grandchild_stopped_at_deadline", base, body)


def term_ignoring_grandchild(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught, elapsed = timed_out(scratch, "term", "wait")
        return {**stopped(scratch, caught, elapsed), "waited_for_the_grace_period": elapsed >= CASE_TIMEOUT_SECONDS + CASE_GRACE_SECONDS * 0.9}

    return case("term_ignoring_grandchild_killed", base, body)


def term_ignoring_child(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught, elapsed = timed_out(scratch, "term", "ignore_term")
        return stopped(scratch, caught, elapsed)

    return case("term_ignoring_child_killed", base, body)


def grandchild_holds_pipe(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught, elapsed = timed_out(scratch, "respect", "leave")
        return {**stopped(scratch, caught, elapsed), "deadline_fired_not_the_exit": elapsed >= CASE_TIMEOUT_SECONDS * 0.9}

    return case("grandchild_holding_pipe_stopped", base, body)


def partial_output(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught, elapsed = timed_out(scratch, "respect", "wait")
        output = caught.stdout if isinstance(caught, TimeoutExpired) else None
        error = caught.stderr if isinstance(caught, TimeoutExpired) else None
        text = (output.decode() if isinstance(output, bytes) else output) or ""
        error_text = (error.decode() if isinstance(error, bytes) else error) or ""
        return {
            "stdout_kept": "partial output" in text,
            "stderr_kept": "partial error" in error_text,
            "timeout_value_kept": isinstance(caught, TimeoutExpired) and caught.timeout == CASE_TIMEOUT_SECONDS,
            "seconds": round(elapsed, 2),
        }

    return case("timeout_keeps_partial_output", base, body)


def interrupt_stops_group(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        main = threading.get_ident()
        ready = threading.Event()
        done = threading.Event()

        def interrupt_when_ready() -> None:
            # A job started in the background by a shell without job control inherits SIGINT as ignored, so the case installs Python's handler and
            # interrupts only after the grandchild is up.
            end = time.monotonic() + 30
            while not done.is_set() and time.monotonic() < end:
                if "grandchild" in scratch.pids():
                    ready.set()
                    signal.pthread_kill(main, signal.SIGINT)
                    return
                done.wait(0.05)

        earlier = signal.signal(signal.SIGINT, signal.default_int_handler)
        sender = threading.Thread(target=interrupt_when_ready, daemon=True)
        sender.start()
        caught: BaseException | None = None
        try:
            scratch.run("term", "wait", 60)
        except KeyboardInterrupt as error:
            caught = error
        finally:
            done.set()
            sender.join(5)
            signal.signal(signal.SIGINT, earlier)
        pids = scratch.pids()
        return {
            "interrupt_sent_after_the_grandchild_started": ready.is_set(),
            "interrupt_reached_the_caller": isinstance(caught, KeyboardInterrupt),
            "grandchild_started": "grandchild" in pids,
            "grandchild_gone": "grandchild" in pids and not alive(pids["grandchild"]),
            "group_gone": "child" in pids and not process.group_alive(pids["child"]),
        }

    return case("interrupt_stops_group", base, body)


def caller_not_signalled(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught, _ = timed_out(scratch, "respect", "wait")
        pids = scratch.pids()
        group = (scratch.folder / "child.group").read_text().split()[1] if (scratch.folder / "child.group").exists() else ""
        return {
            "raised_timeout": isinstance(caught, TimeoutExpired),
            "child_group_is_not_the_callers": bool(group) and int(group) != os.getpgrp(),
            "child_leads_its_group": bool(group) and "child" in pids and int(group) == pids["child"],
            "caller_still_running": True,
        }

    return case("caller_not_signalled", base, body)


def survivor_fails_visibly(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught: BaseException | None = None
        with short_grace(survivor_probe=lambda _group: True):
            try:
                scratch.run("respect", "wait", CASE_TIMEOUT_SECONDS)
            except TimeoutExpired as error:
                caught = error
        return {
            "raised_survivor_error": isinstance(caught, ProcessGroupSurvived),
            "is_still_a_timeout": isinstance(caught, TimeoutExpired),
            "message_names_the_group": caught is not None and "process group" in str(caught) and "still had members" in str(caught),
        }

    return case("survivor_fails_visibly", base, body)


def normal_run_unchanged(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        done = run_command("bash", ["-c", "printf out; printf err >&2; exit 3"], cwd=ROOT, timeout=30)
        checked: BaseException | None = None
        try:
            run_command("bash", ["-c", "printf out; exit 4"], cwd=ROOT, timeout=30, check=True)
        except process.subprocess.CalledProcessError as error:
            checked = error
        missing: BaseException | None = None
        try:
            run_command("no_such_program_wv3_027", [], timeout=30)
        except FileNotFoundError as error:
            missing = error
        where = run_command("bash", ["-c", 'pwd; printf %s "$WV3_027_PROBE"'], cwd=scratch.folder, env={**os.environ, "WV3_027_PROBE": "seen"}, timeout=30)
        silent = run_command("bash", ["-c", "cat; printf done"], cwd=ROOT, timeout=30)
        return {
            "result_type": isinstance(done, process.CompletedProcess),
            "exit_code_kept": done.returncode == 3,
            "text_output": done.stdout == "out" and done.stderr == "err",
            "args_kept": done.args[0].endswith("bash"),
            "check_raises_called_process_error": isinstance(checked, process.subprocess.CalledProcessError)
            and checked.returncode == 4
            and checked.stdout == "out",
            "missing_program_raises_file_not_found": isinstance(missing, FileNotFoundError),
            "cwd_and_env_applied": where.stdout.splitlines()[0].endswith(scratch.folder.name) and where.stdout.endswith("seen"),
            "stdin_is_closed": silent.stdout == "done",
        }

    return case("normal_run_unchanged", base, body)


def nested_stopped(scratch: Scratch, levels: int) -> dict[str, Any]:
    started = time.monotonic()
    caught: BaseException | None = None
    try:
        scratch.run_nested(levels, 60, "exit", NESTED_OUTER_TIMEOUT_SECONDS)
    except TimeoutExpired as error:
        caught = error
    elapsed = time.monotonic() - started
    pids = scratch.pids()
    group = (scratch.folder / "child.group").read_text().split()[1] if (scratch.folder / "child.group").exists() else ""
    innermost = scratch.folder / "level1.json"
    seen = len(json.loads(innermost.read_text())["registries"]) if innermost.exists() else 0
    return {
        "raised_timeout": isinstance(caught, TimeoutExpired),
        "not_a_survivor_error": not isinstance(caught, ProcessGroupSurvived),
        "nested_launchers_started": all(f"level{level}" in pids for level in range(1, levels + 1)),
        "nested_child_started": "child" in pids and "grandchild" in pids,
        "nested_child_in_another_group": bool(group) and "level1" in pids and int(group) != pids["level1"] and int(group) != os.getpgrp(),
        "innermost_launcher_saw_every_ancestor_registry": seen == levels,
        "nested_launchers_gone": all(not alive(pid) for name, pid in pids.items() if name.startswith("level")),
        "child_gone": "child" in pids and not alive(pids["child"]),
        "grandchild_that_ignores_term_gone": "grandchild" in pids and not alive(pids["grandchild"]),
        "nested_group_gone": "child" in pids and not process.group_alive(pids["child"]),
        "registries_removed": bool(scratch.registries()) and not any(Path(path).exists() for path in scratch.registries()),
        "seconds": round(elapsed, 2),
    }


def nested_launch(base: Path) -> dict[str, Any]:
    return case("nested_launch_stopped_at_the_outer_deadline", base, lambda scratch: nested_stopped(scratch, 1))


def two_level_nested_launch(base: Path) -> dict[str, Any]:
    return case("two_level_nested_launch_stopped_at_the_outer_deadline", base, lambda scratch: nested_stopped(scratch, 2))


def nested_own_deadline(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        started = time.monotonic()
        done = scratch.run_nested(1, CASE_TIMEOUT_SECONDS, "exit", 30)
        elapsed = time.monotonic() - started
        pids = scratch.pids()
        level = scratch.folder / "level1.json"
        details = json.loads(level.read_text())["registries"] if level.exists() else []
        child_saw = (scratch.folder / "child.registries").read_text().split(":") if (scratch.folder / "child.registries").exists() else []
        return {
            "outer_call_completed": done.returncode == 0,
            "inner_deadline_reported": "inner timeout" in done.stdout,
            "child_gone": "child" in pids and not alive(pids["child"]),
            "grandchild_that_ignores_term_gone": "grandchild" in pids and not alive(pids["grandchild"]),
            "inner_deadline_not_the_outer": elapsed < 15,
            "child_saw_the_outer_and_the_nested_registry": len([path for path in child_saw if path]) == 2,
            "registry_is_private": bool(details)
            and all(item["exists"] and item["mode"] == 0o600 and item["folder_mode"] == 0o700 and item["owner"] == os.getuid() for item in details),
            "registries_removed": bool(scratch.registries()) and not any(Path(path).exists() for path in scratch.registries()),
            "seconds": round(elapsed, 2),
        }

    return case("nested_launch_keeps_its_own_deadline", base, body)


def ended_recorded_group(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught: BaseException | None = None
        try:
            scratch.run_nested(1, CASE_TIMEOUT_SECONDS, "wait", NESTED_OUTER_TIMEOUT_SECONDS + CASE_TIMEOUT_SECONDS)
        except TimeoutExpired as error:
            caught = error
        pids = scratch.pids()
        output = caught.stdout if isinstance(caught, TimeoutExpired) else None
        return {
            "inner_deadline_passed_first": "inner timeout" in ((output.decode() if isinstance(output, bytes) else output) or ""),
            "raised_timeout": isinstance(caught, TimeoutExpired),
            "not_a_survivor_error": not isinstance(caught, ProcessGroupSurvived),
            "nested_launcher_gone": "level1" in pids and not alive(pids["level1"]),
            "child_gone": "child" in pids and not alive(pids["child"]),
        }

    return case("ended_recorded_group_counts_as_stopped", base, body)


def late_registry_write(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        removed = scratch.folder / "removed_owner" / "groups"
        saved = os.environ.get(process.REGISTRY_VARIABLE)
        os.environ[process.REGISTRY_VARIABLE] = str(removed)
        try:
            done = run_command("bash", ["-c", "printf ok"], cwd=ROOT, timeout=30)
        finally:
            if saved is None:
                os.environ.pop(process.REGISTRY_VARIABLE, None)
            else:
                os.environ[process.REGISTRY_VARIABLE] = saved
        return {"launch_ran": done.stdout == "ok", "removed_registry_not_created_again": not removed.parent.exists()}

    return case("write_to_a_removed_registry_is_skipped", base, body)


def default_periods(base: Path) -> dict[str, Any]:
    return {
        "case": "default_grace_periods_are_set",
        "passed": process.STOP_GRACE_SECONDS >= 2.0 and process.CONFIRM_SECONDS >= 2.0 and process.DEFAULT_TIMEOUT_SECONDS > 0,
        "checks": {"grace": process.STOP_GRACE_SECONDS, "confirm": process.CONFIRM_SECONDS},
    }


CASES: dict[str, Callable[[Path], dict[str, Any]]] = {
    "grandchild_stopped_at_deadline": grandchild_stopped,
    "term_ignoring_grandchild_killed": term_ignoring_grandchild,
    "term_ignoring_child_killed": term_ignoring_child,
    "grandchild_holding_pipe_stopped": grandchild_holds_pipe,
    "timeout_keeps_partial_output": partial_output,
    "interrupt_stops_group": interrupt_stops_group,
    "caller_not_signalled": caller_not_signalled,
    "survivor_fails_visibly": survivor_fails_visibly,
    "normal_run_unchanged": normal_run_unchanged,
    "nested_launch_stopped_at_the_outer_deadline": nested_launch,
    "two_level_nested_launch_stopped_at_the_outer_deadline": two_level_nested_launch,
    "nested_launch_keeps_its_own_deadline": nested_own_deadline,
    "ended_recorded_group_counts_as_stopped": ended_recorded_group,
    "write_to_a_removed_registry_is_skipped": late_registry_write,
    "default_grace_periods_are_set": default_periods,
}
REAL_STOP = process.stop_process_group


def plant_direct_child_only(process_group: int, leader: Any = None, grace_seconds: float | None = None, registry: Path | None = None) -> bool:
    """The defect WV3-027 describes: only the direct child is killed."""
    if leader is not None:
        leader.kill()
        leader.wait()
    return False


def plant_term_only(process_group: int, leader: Any = None, grace_seconds: float | None = None, registry: Path | None = None) -> bool:
    """TERM to the group, a wait for the grace period, no KILL."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process_group, signal.SIGTERM)
    time.sleep(process.STOP_GRACE_SECONDS if grace_seconds is None else grace_seconds)
    if leader is not None:
        leader.poll()
    return False


def plant_no_confirmation(process_group: int, leader: Any = None, grace_seconds: float | None = None, registry: Path | None = None) -> bool:
    """TERM and KILL, no check that the group is gone: a survivor is never reported."""
    for number in (signal.SIGTERM, signal.SIGKILL):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process_group, number)
    if leader is not None:
        leader.poll()
    return False


def plant_own_group_only(process_group: int, leader: Any = None, grace_seconds: float | None = None, registry: Path | None = None) -> bool:
    """The defect of review finding 1: the real stop of the own group, without the groups that descendants recorded."""
    return REAL_STOP(process_group, leader, grace_seconds)


# plant name -> (replacement for stop_process_group, {case that must fail: the check that must be false in it})
PLANTS: dict[str, tuple[Callable[..., bool], dict[str, str]]] = {
    "direct_child_only": (
        plant_direct_child_only,
        {"grandchild_stopped_at_deadline": "grandchild_gone", "grandchild_holding_pipe_stopped": "grandchild_gone"},
    ),
    "term_only": (plant_term_only, {"term_ignoring_grandchild_killed": "grandchild_gone", "term_ignoring_child_killed": "child_gone"}),
    "no_confirmation": (plant_no_confirmation, {"survivor_fails_visibly": "raised_survivor_error"}),
    "own_group_only": (
        plant_own_group_only,
        {"nested_launch_stopped_at_the_outer_deadline": "child_gone", "two_level_nested_launch_stopped_at_the_outer_deadline": "child_gone"},
    ),
}


def caught_on(result: dict[str, Any], check: str) -> bool:
    """A plant is caught only by the intended failed check: a case that raised (a setup error) is no evidence."""
    checks = result["checks"]
    return "raised" not in checks and checks.get(check) is False


def planted_cases(base: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    real = list(inspect.signature(REAL_STOP).parameters)
    for name, (replacement, wanted) in PLANTS.items():
        if list(inspect.signature(replacement).parameters) != real:
            results.append({"case": f"planted_{name}_is_caught", "passed": False, "checks": {"plant_takes_the_real_arguments": False, "real": real}})
            continue
        saved = process.stop_process_group
        process.stop_process_group = replacement
        try:
            ran = {case_name: CASES[case_name](base / f"plant_{name}") for case_name in wanted}
        finally:
            process.stop_process_group = saved
        caught = {case_name: caught_on(ran[case_name], check) for case_name, check in wanted.items()}
        raised = {case_name: result["checks"]["raised"] for case_name, result in ran.items() if "raised" in result["checks"]}
        results.append({"case": f"planted_{name}_is_caught", "passed": all(caught.values()), "checks": {**caught, **({"raised": raised} if raised else {})}})
    return results


def main() -> int:
    started = datetime.now(UTC)
    before = {name: sha256_of(ROOT / name) for name in WATCHED}
    base = Path(tempfile.mkdtemp(prefix="process_e2e."))
    try:
        results = [case_function(base) for case_function in CASES.values()]
        results.extend(planted_cases(base))
    finally:
        shutil.rmtree(base, ignore_errors=True)
    after = {name: sha256_of(ROOT / name) for name in WATCHED}
    if before != after:
        results.append({"case": "code_unchanged_during_run", "passed": False, "changed": sorted(name for name in before if before[name] != after[name])})
    failed = [result["case"] for result in results if not result["passed"]]
    out = ROOT / "data" / "e2e" / "process" / f"{started:%Y%m%dT%H%M%SZ}_{os.getpid()}"
    out.mkdir(parents=True)
    revision = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip()
    report = {
        "started": started.isoformat(),
        "revision": revision,
        "code_sha256": before,
        "python": sys.version.split()[0],
        "cases": results,
        "passed": len(results) - len(failed),
        "total": len(results),
        "failed": failed,
    }
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for result in results:
        sys.stdout.write(("PASS " if result["passed"] else "FAIL ") + result["case"] + "\n")
    sys.stdout.write(f"{report['passed']}/{report['total']} passed; report {out / 'report.json'}\n")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
