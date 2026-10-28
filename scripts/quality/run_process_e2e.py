"""E2E for the process launcher's deadline (scripts/process.py ``run_command``): the whole process group stops, not only the child (WV3-027).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_process_e2e``. Each case starts real shell programs through ``run_command``. A
child starts a grandchild (one that ignores TERM in some cases) and the case checks, by process ID, that no member of the group is left once the call
returns or raises. Cases: a grandchild stopped at the deadline, a grandchild and a child that ignore TERM (KILL after the grace period), a grandchild
that holds the output pipe after the child exited, partial output kept in the ``TimeoutExpired``, a caller interrupt that stops the group, a child in
its own group (the caller is never signalled), a survivor that fails visibly, and unchanged results for a program that ends in time (exit code, text,
``check``, a missing program, working folder, environment and a closed stdin).
Planted defects run the same cases with a broken stop (the direct child only, TERM only, no confirmation) and each must be caught by a named case.
A member that starts its own session is outside the group; that limit is documented in scripts/process.py and not tested here.
The report goes to data/e2e/process/<UTC>_<pid>/report.json with the revision and the SHA-256 of the files under test.
"""

from __future__ import annotations

import contextlib
import hashlib
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
printf 'partial output\\n'
printf 'partial error\\n' >&2
bash -c "$GRANDCHILD_CODE" grandchild "$CASE_DIR/grandchild.pid" "$GRANDCHILD_MODE" &
if [ "$CHILD_MODE" = leave ]; then exit 0; fi
while :; do sleep 1; done
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

    def run(self, grandchild_mode: str, child_mode: str, timeout: float) -> CompletedProcess[str]:
        """Start the child through the launcher; its environment names the folder and the modes."""
        environment = {**os.environ, "CASE_DIR": str(self.folder), "GRANDCHILD_CODE": GRANDCHILD, "GRANDCHILD_MODE": grandchild_mode, "CHILD_MODE": child_mode}
        return run_command("bash", ["-c", CHILD], cwd=ROOT, env=environment, timeout=timeout)

    def pids(self) -> dict[str, int]:
        found: dict[str, int] = {}
        for name in ("child", "grandchild"):
            file = self.folder / f"{name}.pid"
            if file.exists() and file.read_text().strip():
                found[name] = int(file.read_text().strip())
        return found

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
    "default_grace_periods_are_set": default_periods,
}


def plant_direct_child_only(group: int, leader: Any = None) -> bool:
    """The defect WV3-027 describes: only the direct child is killed."""
    if leader is not None:
        leader.kill()
        leader.wait()
    return False


def plant_term_only(group: int, leader: Any = None) -> bool:
    """TERM to the group, a wait for the grace period, no KILL."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(group, signal.SIGTERM)
    time.sleep(process.STOP_GRACE_SECONDS)
    if leader is not None:
        leader.poll()
    return False


def plant_no_confirmation(group: int, leader: Any = None) -> bool:
    """TERM and KILL, no check that the group is gone: a survivor is never reported."""
    for number in (signal.SIGTERM, signal.SIGKILL):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(group, number)
    if leader is not None:
        leader.poll()
    return False


# plant name -> (replacement for stop_process_group, cases that must fail)
PLANTS: dict[str, tuple[Callable[..., bool], tuple[str, ...]]] = {
    "direct_child_only": (plant_direct_child_only, ("grandchild_stopped_at_deadline", "grandchild_holding_pipe_stopped")),
    "term_only": (plant_term_only, ("term_ignoring_grandchild_killed", "term_ignoring_child_killed")),
    "no_confirmation": (plant_no_confirmation, ("survivor_fails_visibly",)),
}


def planted_cases(base: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for name, (replacement, wanted) in PLANTS.items():
        saved = process.stop_process_group
        process.stop_process_group = replacement
        try:
            ran = {case_name: CASES[case_name](base / f"plant_{name}") for case_name in wanted}
        finally:
            process.stop_process_group = saved
        caught = {case_name: not result["passed"] for case_name, result in ran.items()}
        results.append({"case": f"planted_{name}_is_caught", "passed": all(caught.values()), "checks": caught})
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
