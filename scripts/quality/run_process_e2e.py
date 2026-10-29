"""E2E for the process launcher's deadline (scripts/process.py ``run_command``): the whole process group stops, not only the child (WV3-027).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_process_e2e``. Each case starts real shell programs through ``run_command``. A
child starts a grandchild (one that ignores TERM in some cases) and the case checks, by process ID, that no member of the group is left once the call
returns or raises. Cases: a grandchild stopped at the deadline, a grandchild and a child that ignore TERM (KILL after the grace period), a grandchild
that holds the output pipe after the child exited, partial output kept in the ``TimeoutExpired``, a caller interrupt that stops the group, a child in
its own group (the caller is never signalled), a survivor that fails visibly, and unchanged results for a program that ends in time (exit code, text,
``check``, a missing program, working folder, environment and a closed stdin). The program leads its own session. Nested cases start a Python program
that itself calls ``run_command`` (one and two levels deep): a nested launch stays in the outer session in a group of its own, the outer deadline stops
every member of the session, and a nested launch keeps its own earlier deadline. A descendant that moves to a new process group is stopped; a
descendant that tampers with the old registry variable cannot hide (the review reproductions); an unrelated command in another session is never
signalled; a failed session listing still stops the leader's group and fails visibly, also when the lister cannot start or stalls with a child that
holds its pipes; a second interrupt during the stop is held until the stop is confirmed; a member that leaves the session after the listing is not
signalled (the review reproductions of fix round 4).
Planted defects run the same cases with a broken stop (the direct child only, TERM only, no confirmation, the leader's group only). Each plant has the
real parameters of ``stop_launch`` and counts as caught only when the named check of each named case is false and the case raised nothing.
Fix round 6 (unreported cleanup failures): an unconfirmed deadline stop that overlaps an interrupt, two futures on one worker, an unconfirmed stop after an
interrupt without a pool, a stop that raises, a caller that swallows the timeout, a pool step with an unconfirmed deadline stop and an alarm during the listing,
with six plants. Fix round 7 (the recheck of round 6): an alarm that raises ``SystemExit`` during the stop, a failed session listing in the CI worker's
own session, a member left after a normal end (also in the credential-process shape) and a finalization that cannot be confirmed, with four plants.
A member that starts its own session is outside the launch's session; that limit is documented in scripts/process.py and not tested here.
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
from scripts.process import CompletedProcess, ProcessGroupSurvived, StopOutcome, SubprocessError, TimeoutExpired, run_command

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
printf 'partial output\\n'
printf 'partial error\\n' >&2
bash -c "$GRANDCHILD_CODE" grandchild "$CASE_DIR/grandchild.pid" "$GRANDCHILD_MODE" &
if [ "$CHILD_MODE" = leave ]; then exit 0; fi
while :; do sleep 1; done
"""
# The nested launcher, written to the case folder as nested.py. Arguments: repository root, case folder, levels, inner timeout, inner grace period, and
# "exit" or "wait" (what it does after the inner timeout). With levels above 1 it starts itself again through run_command; at level 1 it starts CHILD.
# It records its pid, its group and its session.
NESTED = """
import json, os, sys, time
root, folder, levels, timeout, grace, after = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]), sys.argv[6]
sys.path.insert(0, root)
from scripts.process import TimeoutExpired, run_command
with open(os.path.join(folder, f"level{levels}.pid"), "w") as file:
    file.write(str(os.getpid()))
with open(os.path.join(folder, f"level{levels}.json"), "w") as file:
    json.dump({"pid": os.getpid(), "group": os.getpgrp(), "session": os.getsid(0)}, file)
try:
    if levels > 1:
        run_command(sys.executable, ["-B", __file__, root, folder, str(levels - 1), str(timeout), str(grace), after], timeout=60)
    else:
        run_command(os.environ.get("NESTED_PROGRAM", "bash"), ["-c", os.environ.get("NESTED_CHILD_CODE", ""), folder], timeout=timeout, grace_seconds=grace)
except TimeoutExpired:
    print("inner timeout", flush=True)
    if after == "wait":
        while True:
            time.sleep(1)
"""

# A program that tries to hide from its launcher's stop (argv: case folder, mode). It records its pid, then makes the first registry of the old
# launcher's PROCESS_GROUP_REGISTRIES unreadable ("unreadable") or removes it (any other mode), and waits. Without that variable it only waits.
HIDING = """
import os, sys, time
folder = sys.argv[1]
with open(os.path.join(folder, "hiding.pid"), "w") as file:
    file.write(str(os.getpid()))
paths = [path for path in os.environ.get("PROCESS_GROUP_REGISTRIES", "").split(":") if path]
if paths:
    if sys.argv[2:] == ["unreadable"]:
        os.chmod(paths[0], 0)
    else:
        os.unlink(paths[0])
while True:
    time.sleep(1)
"""
# A child that forks a grandchild which moves to a new process group of its own (argv: case folder); both wait.
NEW_GROUP = """
import os, sys, time
folder = sys.argv[1]
with open(os.path.join(folder, "child.pid"), "w") as file:
    file.write(str(os.getpid()))
if os.fork() == 0:
    os.setpgid(0, 0)
    with open(os.path.join(folder, "grandchild.group"), "w") as file:
        file.write("moved" if os.getpgrp() == os.getpid() else "same")
    with open(os.path.join(folder, "grandchild.pid"), "w") as file:
        file.write(str(os.getpid()))
while True:
    time.sleep(1)
"""
# Another command (argv: pid file): it records its pid and waits.
OTHER = """
import os, sys, time
with open(sys.argv[1], "w") as file:
    file.write(str(os.getpid()))
while True:
    time.sleep(1)
"""
# The subject (argv: case folder, the other command's group): it appends that group to the old launcher's registry when the variable names one, and waits.
CLAIMING = """
import os, sys, time
with open(os.path.join(sys.argv[1], "subject.pid"), "w") as file:
    file.write(str(os.getpid()))
paths = [path for path in os.environ.get("PROCESS_GROUP_REGISTRIES", "").split(":") if path]
if paths:
    with open(paths[-1], "a") as file:
        file.write(sys.argv[2] + "\\n")
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

    def run_nested(self, levels: int, inner_timeout: float, after: str, timeout: float, **innermost: str) -> CompletedProcess[str]:
        """Start the nested launcher through the launcher; its innermost program is CHILD with a grandchild that ignores TERM.

        ``innermost`` may set NESTED_PROGRAM and NESTED_CHILD_CODE to run another innermost program (it gets the case folder as its first argument).
        """
        script = self.folder / "nested.py"
        script.write_text(NESTED, encoding="utf-8")
        arguments = ["-B", str(script), str(ROOT), str(self.folder), str(levels), str(inner_timeout), str(CASE_GRACE_SECONDS), after]
        return run_command(sys.executable, arguments, cwd=ROOT, env={**self.environment("term", "wait"), **innermost}, timeout=timeout)

    def pids(self) -> dict[str, int]:
        found: dict[str, int] = {}
        for file in self.folder.glob("*.pid"):
            if file.read_text().strip():
                found[file.stem] = int(file.read_text().strip())
        return found

    def level(self, number: int) -> dict[str, int]:
        """The pid, group and session a nested launcher recorded, or an empty record when it did not start."""
        file = self.folder / f"level{number}.json"
        return json.loads(file.read_text()) if file.exists() else {}

    def cleanup(self) -> None:
        for pid in self.pids().values():
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(pid, signal.SIGKILL)
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(pid, signal.SIGKILL)


@contextlib.contextmanager
def short_grace(survivor_probe: Callable[[Any], bool] | None = None) -> Iterator[None]:
    saved = (process.STOP_GRACE_SECONDS, process.CONFIRM_SECONDS)
    process.STOP_GRACE_SECONDS, process.CONFIRM_SECONDS = CASE_GRACE_SECONDS, CASE_GRACE_SECONDS
    try:
        if survivor_probe is None:
            yield
        else:
            with mock.patch("scripts.process.group_alive", survivor_probe), mock.patch("scripts.process.session_left", survivor_probe):
                yield
    finally:
        process.STOP_GRACE_SECONDS, process.CONFIRM_SECONDS = saved


def timed_out_with(call: Callable[[], object]) -> tuple[BaseException | None, float]:
    """Run a launch that must reach its deadline; return the exception it raised (None when it returned) and the seconds it took."""
    started = time.monotonic()
    try:
        call()
    except (TimeoutExpired, OSError) as error:
        return error, time.monotonic() - started
    return None, time.monotonic() - started


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
        # A case that forces a survivor has checked the launcher's record itself; clearing it keeps this runner's own exit status clean.
        acknowledge_unconfirmed()
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
    sessions = [scratch.level(level).get("session") for level in range(1, levels + 1)]
    return {
        "raised_timeout": isinstance(caught, TimeoutExpired),
        "not_a_survivor_error": not isinstance(caught, ProcessGroupSurvived),
        "nested_launchers_started": all(f"level{level}" in pids for level in range(1, levels + 1)),
        "nested_child_started": "child" in pids and "grandchild" in pids,
        "nested_child_in_another_group": bool(group) and "level1" in pids and int(group) != pids["level1"] and int(group) != os.getpgrp(),
        # The outermost program leads the session; every nested launch stays in it, in a group of its own.
        "nested_launches_in_the_outer_session": f"level{levels}" in pids and sessions == [pids[f"level{levels}"]] * levels,
        "nested_launchers_gone": all(not alive(pid) for name, pid in pids.items() if name.startswith("level")),
        "child_gone": "child" in pids and not alive(pids["child"]),
        "grandchild_that_ignores_term_gone": "grandchild" in pids and not alive(pids["grandchild"]),
        "nested_group_gone": "child" in pids and not process.group_alive(pids["child"]),
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
        level = scratch.level(1)
        group = (scratch.folder / "child.group").read_text().split()[1] if (scratch.folder / "child.group").exists() else ""
        return {
            "outer_call_completed": done.returncode == 0,
            "inner_deadline_reported": "inner timeout" in done.stdout,
            "child_gone": "child" in pids and not alive(pids["child"]),
            "grandchild_that_ignores_term_gone": "grandchild" in pids and not alive(pids["grandchild"]),
            "inner_deadline_not_the_outer": elapsed < 15,
            "launcher_leads_its_session": bool(level) and level["session"] == level["pid"] == level["group"],
            "nested_child_in_a_group_of_its_own": bool(group) and bool(level) and int(group) != level["group"],
            "seconds": round(elapsed, 2),
        }

    return case("nested_launch_keeps_its_own_deadline", base, body)


def ended_nested_group(base: Path) -> dict[str, Any]:
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

    return case("ended_nested_group_counts_as_stopped", base, body)


def hiding_descendant(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The review reproductions of the old registry: a program makes the first inherited registry unreadable, or a nested program removes it, and
        # then waits. Nothing it can change hides it from the stop of its session.
        unreadable, unreadable_seconds = timed_out_with(
            lambda: run_command(sys.executable, ["-B", "-c", HIDING, str(scratch.folder), "unreadable"], cwd=ROOT, timeout=CASE_TIMEOUT_SECONDS)
        )
        first = scratch.pids().get("hiding")
        if first is None:
            return {"unreadable_program_started": False}
        (scratch.folder / "hiding.pid").rename(scratch.folder / "hiding_first.pid")
        removed, removed_seconds = timed_out_with(
            lambda: scratch.run_nested(1, 60, "exit", NESTED_OUTER_TIMEOUT_SECONDS, NESTED_PROGRAM=sys.executable, NESTED_CHILD_CODE=HIDING)
        )
        second = scratch.pids().get("hiding")
        return {
            "unreadable_raised_timeout": isinstance(unreadable, TimeoutExpired) and not isinstance(unreadable, ProcessGroupSurvived),
            "unreadable_program_gone": first is not None and not alive(first),
            "removed_raised_timeout": isinstance(removed, TimeoutExpired) and not isinstance(removed, ProcessGroupSurvived),
            "removed_nested_program_gone": second is not None and not alive(second),
            "seconds": round(unreadable_seconds + removed_seconds, 2),
        }

    return case("descendant_cannot_hide_from_the_session_stop", base, body)


def new_group_descendant(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        caught, elapsed = timed_out_with(
            lambda: run_command(sys.executable, ["-B", "-c", NEW_GROUP, str(scratch.folder)], cwd=ROOT, timeout=CASE_TIMEOUT_SECONDS)
        )
        pids = scratch.pids()
        return {
            "raised_timeout": isinstance(caught, TimeoutExpired),
            "descendant_started": "grandchild" in pids,
            "descendant_left_the_group": (scratch.folder / "grandchild.group").exists() and (scratch.folder / "grandchild.group").read_text() == "moved",
            "grandchild_gone": "grandchild" in pids and not alive(pids["grandchild"]),
            "seconds": round(elapsed, 2),
        }

    return case("descendant_in_a_new_group_is_stopped", base, body)


def unrelated_command(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The review's stale-id reproduction: the subject names another command's group where the old launcher read owned groups. The other command runs
        # in a session of its own, so the subject's stop must not reach it.
        other_file = scratch.folder / "other.pid"
        other: dict[str, Any] = {}

        def run_other() -> None:
            try:
                other["exit"] = run_command(sys.executable, ["-B", "-c", OTHER, str(other_file)], cwd=ROOT, timeout=30, grace_seconds=0.1).returncode
            except SubprocessError as error:
                other["error"] = type(error).__name__

        thread = threading.Thread(target=run_other, daemon=True)
        thread.start()
        end = time.monotonic() + 10
        while not (other_file.exists() and other_file.read_text().strip()) and time.monotonic() < end:
            time.sleep(0.02)
        target = int(other_file.read_text()) if other_file.exists() and other_file.read_text().strip() else None
        caught, elapsed = timed_out_with(
            lambda: run_command(sys.executable, ["-B", "-c", CLAIMING, str(scratch.folder), str(target or 0)], cwd=ROOT, timeout=CASE_TIMEOUT_SECONDS)
        )
        other_untouched = target is not None and alive(target) and not other
        if target is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(target, signal.SIGKILL)
        thread.join(10)
        return {
            "other_command_started": target is not None,
            "subject_raised_timeout": isinstance(caught, TimeoutExpired),
            "other_command_not_signalled": other_untouched,
            "other_command_runner_finished": not thread.is_alive(),
            "seconds": round(elapsed, 2),
        }

    return case("unrelated_command_is_not_signalled", base, body)


def listing_failure(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        with mock.patch("scripts.process.SESSION_LISTING", ("no_such_ps_wv3_027", "-A")):
            caught, elapsed = timed_out_with(lambda: scratch.run("respect", "wait", CASE_TIMEOUT_SECONDS))
        pids = scratch.pids()
        return {
            "raised_survivor_error": isinstance(caught, ProcessGroupSurvived),
            "message_names_the_listing": caught is not None and "could not be listed" in str(caught),
            "leader_group_still_stopped": "child" in pids and not process.group_alive(pids["child"]),
            "grandchild_gone": "grandchild" in pids and not alive(pids["grandchild"]),
            "seconds": round(elapsed, 2),
        }

    return case("session_listing_failure_fails_visibly", base, body)


# A program that ignores TERM and records its pid (the review's subject program).
TERM_IGNORING = """
import os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
open(sys.argv[1], 'w').write(str(os.getpid()))
while True:
    time.sleep(1)
"""


def wait_for_file(path: Path, seconds: float = 10.0) -> bool:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if path.exists() and path.read_text().strip():
            return True
        time.sleep(0.02)
    return False


def listing_start_failure(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The review reproduction: an executable lister with invalid executable bytes fails to start with OSError (errno 8).
        lister = scratch.folder / "ps"
        lister.write_text("Not an executable format\n", encoding="utf-8")
        lister.chmod(0o700)
        subject = scratch.folder / "subject.pid"
        with mock.patch("scripts.process.SESSION_LISTING", (str(lister), "-A")):
            caught, elapsed = timed_out_with(
                lambda: run_command(sys.executable, ["-B", "-c", TERM_IGNORING, str(subject)], cwd=ROOT, timeout=CASE_TIMEOUT_SECONDS)
            )
        pids = scratch.pids()
        return {
            "raised_survivor_error": isinstance(caught, ProcessGroupSurvived),
            "message_names_the_listing": caught is not None and "could not be listed" in str(caught),
            "subject_started": "subject" in pids,
            "subject_gone": "subject" in pids and not alive(pids["subject"]),
            "seconds": round(elapsed, 2),
        }

    return case("listing_start_failure_fails_visibly", base, body)


def stalled_listing(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The review reproduction: the lister forks a process that holds its pipes and then sleeps, so the listing times out and a reap that waits for
        # the pipes to close never returns.
        holder = scratch.folder / "holder.pid"
        subject = scratch.folder / "subject.pid"
        lister_code = f"import os, time; os.fork() == 0 and open({str(holder)!r}, 'w').write(str(os.getpid())); time.sleep(60)"
        outcome: dict[str, Any] = {}

        def invoke() -> None:
            caught, elapsed = timed_out_with(
                lambda: run_command(sys.executable, ["-B", "-c", TERM_IGNORING, str(subject)], cwd=ROOT, timeout=CASE_TIMEOUT_SECONDS)
            )
            outcome.update(caught=caught, seconds=elapsed)

        calling = threading.Thread(target=invoke, daemon=True)
        with mock.patch("scripts.process.SESSION_LISTING", (sys.executable, "-c", lister_code)), mock.patch("scripts.process.LISTING_SECONDS", 0.5):
            calling.start()
            # Bound: the deadline, one listing, the fallback group stop's confirmation and slack for a loaded machine.
            calling.join(CASE_TIMEOUT_SECONDS + 0.5 + CASE_GRACE_SECONDS + 6)
            finished = not calling.is_alive()
            if not finished and holder.exists():
                with contextlib.suppress(ProcessLookupError):
                    os.kill(int(holder.read_text()), signal.SIGKILL)
                calling.join(5)
        pids = scratch.pids()
        return {
            "call_finished_within_its_bound": finished,
            "raised_survivor_error": isinstance(outcome.get("caught"), ProcessGroupSurvived),
            "holder_started": "holder" in pids,
            "holder_gone": "holder" in pids and not alive(pids["holder"]),
            "subject_gone": "subject" in pids and not alive(pids["subject"]),
            "seconds": round(float(outcome.get("seconds", -1.0)), 2),
        }

    return case("stalled_listing_is_bounded", base, body)


def second_interrupt(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The review reproduction: SIGINT while the program runs, then a second SIGINT 0.2 s into the stop of a member that ignores TERM.
        main = threading.get_ident()
        subject = scratch.folder / "subject.pid"
        cleaning, finished = threading.Event(), threading.Event()
        sent: list[str] = []
        real_stop = process.stop_launch

        def observed_stop(leader_pid: int, leader: Any = None, grace_seconds: float | None = None, session: bool = False) -> StopOutcome:
            cleaning.set()
            survived = real_stop(leader_pid, leader, grace_seconds, session)
            finished.set()
            return survived

        def sender() -> None:
            if not wait_for_file(subject):
                return
            signal.pthread_kill(main, signal.SIGINT)
            sent.append("first")
            if cleaning.wait(5) and not finished.wait(0.2):
                signal.pthread_kill(main, signal.SIGINT)
                sent.append("second")

        earlier = signal.signal(signal.SIGINT, signal.default_int_handler)
        sending = threading.Thread(target=sender, daemon=True)
        caught: BaseException | None = None
        stop_finished_before_return = False
        try:
            with mock.patch("scripts.process.stop_launch", observed_stop):
                sending.start()
                try:
                    run_command(sys.executable, ["-B", "-c", TERM_IGNORING, str(subject)], cwd=ROOT, timeout=60)
                except KeyboardInterrupt as error:
                    caught = error
                    stop_finished_before_return = finished.is_set()
            sending.join(5)
        finally:
            signal.signal(signal.SIGINT, earlier)
        pids = scratch.pids()
        return {
            "both_interrupts_sent": sent == ["first", "second"],
            "interrupt_reached_the_caller": isinstance(caught, KeyboardInterrupt),
            "stop_finished_before_the_interrupt_returned": stop_finished_before_return,
            "subject_started": "subject" in pids,
            "subject_gone": "subject" in pids and not alive(pids["subject"]),
        }

    return case("second_interrupt_during_cleanup_is_deferred", base, body)


# A leader whose forked member leaves the session (setsid) when the file "escape" appears, and records the session in which TERM reached it.
ESCAPING = """
import os, signal, sys, time
from pathlib import Path
folder = Path(sys.argv[1])
if os.fork() == 0:
    (folder / 'escaper.pid').write_text(str(os.getpid()))
    while not (folder / 'escape').exists():
        time.sleep(0.005)
    os.setsid()
    def term(_number, _frame):
        (folder / 'term_session').write_text(str(os.getsid(0)))
        os._exit(0)
    signal.signal(signal.SIGTERM, term)
    (folder / 'escaped').write_text(str(os.getsid(0)))
    while True:
        time.sleep(1)
while True:
    time.sleep(1)
"""


def member_leaves_before_signal(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The review reproduction: the member leaves the session after it was listed and before its signal.
        leader = process.start([sys.executable, "-B", "-c", ESCAPING, str(scratch.folder)], ROOT, None, new_session=True)
        (scratch.folder / "leader.pid").write_text(str(leader.pid))
        real_members = process.session_members
        synchronized: list[bool] = []

        def members_then_escape(sessions: Any, parent: int) -> set[int]:
            members = real_members(sessions, parent)
            escaper = int((scratch.folder / "escaper.pid").read_text())
            if escaper in members and not synchronized:
                synchronized.append(True)
                (scratch.folder / "escape").write_text("go")
                wait_for_file(scratch.folder / "escaped", 2)
            return members

        try:
            if not wait_for_file(scratch.folder / "escaper.pid"):
                return {"escaper_started": False}
            with mock.patch("scripts.process.session_members", members_then_escape):
                survived = process.stop_sessions({leader.pid}, grace_seconds=0.2, leader=leader)
            time.sleep(0.3)
            return {
                "member_left_after_the_listing": bool(synchronized) and (scratch.folder / "escaped").exists(),
                "member_outside_the_session_not_signalled": not (scratch.folder / "term_session").exists(),
                "stop_confirmed": not survived,
                "leader_gone": leader.poll() is not None,
            }
        finally:
            # The escaped member holds the leader's pipes, so it is killed before the leader is reaped.
            scratch.cleanup()
            process.close_pipes(leader)
            leader.wait(timeout=CASE_GRACE_SECONDS + 5)

    return case("member_that_leaves_before_the_signal_is_skipped", base, body)


# A program that prints its first argument.
PRINTER = "import sys; print(sys.argv[1])"
# The reviewer's threaded reproduction through the real call site (fix round 5): ``pre_push.run_steps`` runs two steps at once and a serial pair, each
# step starting a program through ``run_command`` (``launch`` is replaced by a launch of this subject). Arguments: repository root, case folder,
# mode ("interrupt" or "survivor": a session stop that is never confirmed) and plant ("" or the name of a planted defect). The runner is interrupted
# twice (the parent sends SIGINT, then again 0.3 s later) and records, at the moment the interrupt returns to it, which subjects are still alive.
THREADED_RUNNER = """
import json, os, signal, sys, threading
from pathlib import Path
from unittest import mock
root, folder, mode, plant = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4]
sys.path.insert(0, str(root))
signal.signal(signal.SIGINT, signal.default_int_handler)
from scripts import process
from scripts.quality import pre_push
SUBJECT = (
    "import os, signal, sys, time\\n"
    "if sys.argv[2] == 'two':\\n"
    "    signal.signal(signal.SIGTERM, signal.SIG_IGN)\\n"
    "open(sys.argv[1], 'w').write(str(os.getpid()))\\n"
    "while True:\\n"
    "    time.sleep(1)\\n"
)
SURVIVOR = threading.local()
def survivor_session_left(members):
    return bool(members) or getattr(SURVIVOR, "on", False)
def launch(_root, step, _environment):
    SURVIVOR.on = mode == "survivor"
    return process.run_command(sys.executable, ["-B", "-c", SUBJECT, str(folder / (step["name"] + ".pid")), step["name"]], timeout=60, grace_seconds=0.5)
def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
steps = [
    {"name": name, "kind": kind, "env": {}, "arguments": [], "timeout_seconds": 60}
    for name, kind in (("one", "repo_checks"), ("two", "repo_checks"), ("three", "pre_commit"), ("four", "pre_commit"))
]
patches = [mock.patch.object(pre_push, "launch", launch), mock.patch("scripts.process.CANCEL_WAIT_SECONDS", 3.0, create=True)]
if mode == "survivor":
    patches += [mock.patch("scripts.process.session_left", survivor_session_left), mock.patch("scripts.process.CONFIRM_SECONDS", 0.5)]
if plant == "no_cancellation_flag":
    patches.append(mock.patch.object(process.Cancellation, "cancelled", lambda self: False))
if plant == "no_session_stop":
    patches.append(mock.patch("scripts.process.stop_launch", lambda *_arguments, **_keywords: False))
if plant == "pool_does_not_wait":
    patches.append(mock.patch.object(process.LaunchPool, "wait_for_jobs", lambda self: []))
output = folder / "output"
output.mkdir()
result = {}
for patch in patches:
    patch.start()
try:
    pre_push.run_steps(root, output, steps)
    result["raised"] = "nothing"
except BaseException as error:
    result["raised"] = type(error).__name__
    result["message"] = str(error)[:300]
    for name in ("one", "two", "three"):
        file = folder / (name + ".pid")
        result[name + "_alive_at_return"] = file.exists() and alive(int(file.read_text()))
    result["four_started"] = (folder / "four.pid").exists()
(folder / "result.json").write_text(json.dumps(result))
"""


def threaded_interrupt_result(scratch: Scratch, mode: str, plant: str = "") -> dict[str, Any]:
    """Run THREADED_RUNNER, interrupt it twice and return what it recorded when the interrupt came back to it (empty when it never did)."""
    script = scratch.folder / "runner.py"
    script.write_text(THREADED_RUNNER, encoding="utf-8")
    environment = {key: value for key, value in os.environ.items() if key != process.SESSION_VARIABLE}
    runner = process.start([sys.executable, "-B", str(script), str(ROOT), str(scratch.folder), mode, plant], ROOT, environment, new_session=True)
    try:
        names = ("one", "two", "three")
        if not all(wait_for_file(scratch.folder / f"{name}.pid") for name in names):
            return {"started": False}
        os.kill(runner.pid, signal.SIGINT)
        time.sleep(0.3)
        with contextlib.suppress(ProcessLookupError):
            os.kill(runner.pid, signal.SIGINT)
        finished = wait_for_file(scratch.folder / "result.json", 45)
        result: dict[str, Any] = json.loads((scratch.folder / "result.json").read_text()) if finished else {}
        pids = scratch.pids()
        result.update(started=True, finished=finished, alive_after={name: alive(pids[name]) for name in names if name in pids})
        return result
    finally:
        # A runner that never saw the interrupt through (the defect) waits for its pool at exit: its session and every subject are killed.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(runner.pid, signal.SIGKILL)
        scratch.cleanup()
        process.close_pipes(runner)
        runner.wait(timeout=CASE_GRACE_SECONDS + 5)


def threaded_interrupt(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        result = threaded_interrupt_result(scratch, "interrupt", plant)
        if not result.get("started"):
            return {"programs_started": False}
        return {
            "interrupt_came_back_to_the_caller": result.get("raised") == "KeyboardInterrupt",
            "every_running_program_gone_before_the_interrupt_returned": result.get("finished", False)
            and not any(result.get(f"{name}_alive_at_return", True) for name in ("one", "two", "three")),
            "every_running_program_gone_afterwards": bool(result.get("alive_after")) and not any(result["alive_after"].values()),
            "queued_step_never_started": result.get("four_started", True) is False,
            "exception_seen": result.get("raised"),
        }

    return case("threaded_interrupt_stops_the_launch", base, body)


def threaded_survivor(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        result = threaded_interrupt_result(scratch, "survivor")
        if not result.get("started"):
            return {"programs_started": False}
        return {
            "unconfirmed_stop_fails_visibly": result.get("raised") == "LaunchesNotStopped",
            "message_names_the_cause": "not confirmed" in result.get("message", "") or "still running" in result.get("message", ""),
            "exception_seen": result.get("raised"),
        }

    return case("threaded_interrupt_survivor_fails_visibly", base, body)


def threaded_normal_run(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        earlier = signal.getsignal(signal.SIGINT)

        def printing(number: int) -> str:
            if number == 9:
                raise ValueError("job failure")
            return run_command(sys.executable, ["-B", "-c", PRINTER, str(number)], cwd=ROOT, timeout=30).stdout.strip()

        with process.LaunchPool(max_workers=3) as pool:
            ordered = list(pool.map(printing, [3, 1, 2]))
            failing = pool.submit(printing, 9)
            expiring = pool.submit(scratch.run, "term", "wait", CASE_TIMEOUT_SECONDS)
            failing_error, expiring_error = failing.exception(), expiring.exception()
        pids = scratch.pids()
        return {
            "results_in_submission_order": ordered == ["3", "1", "2"],
            "job_exception_reaches_the_caller": isinstance(failing_error, ValueError),
            "job_deadline_raises_the_timeout": isinstance(expiring_error, TimeoutExpired),
            "job_deadline_stops_the_session": "grandchild" in pids and not alive(pids["grandchild"]) and not alive(pids.get("child", 0)),
            "interrupt_handler_restored": signal.getsignal(signal.SIGINT) is earlier,
        }

    return case("threaded_normal_run_unchanged", base, body)


def plant_patches(plant: str) -> list[Any]:
    """The patches of a planted defect of fix round 6 or 7 (an empty list for no plant); a runner process embeds this function's source."""
    table: dict[str, list[Any]] = {
        # The pool takes a job's end for cleanup and ignores the stops that were not confirmed.
        "pool_ignores_unconfirmed": [mock.patch.object(process.LaunchPool, "unconfirmed_problems", lambda self: [])],
        # The record of an unconfirmed stop is not kept (a caller that drops the exception drops the outcome).
        "no_record": [mock.patch("scripts.process.record_unconfirmed", lambda *_arguments, **_keywords: None)],
        # Cancelled queued futures are waited for as if they ran.
        "cancelled_futures_counted": [mock.patch.object(process.LaunchPool, "live_jobs", lambda self: list(self.jobs))],
        # The lister is left running when the listing is interrupted.
        "listing_not_unwound": [mock.patch("scripts.process.end_lister", lambda *_arguments, **_keywords: None)],
        # The interrupt path raises the interrupt and drops the outcome of the stop.
        "interrupt_drops_the_outcome": [mock.patch("scripts.process.named_failure", lambda _outcome, _command, cause: cause)],
        # Fix round 7: the fallback of a failed listing signals the caller's own group again.
        "own_group_signalled": [mock.patch("scripts.process.caller_group", lambda: -1)],
        # Fix round 7: a normal end leaves the session alone.
        "no_finalization": [mock.patch("scripts.process.finalize_session", lambda *_arguments, **_keywords: (StopOutcome(), None))],
    }
    return table.get(plant, [])


# A launch of a pool job that must end with an unconfirmed stop sets ``SURVIVOR.on`` in its thread: ``session_left`` then reports a member left, for that
# thread only (any other launch, such as the git calls of ``pre_push``, is confirmed as usual).
SURVIVOR_SOURCE = "SURVIVOR = threading.local()\ndef survivor_session_left(members):\n    return bool(members) or getattr(SURVIVOR, 'on', False)\n"
# The runners of fix round 6 start with the same lines: the repository on the path, Python's SIGINT handler, the planted patches.
ROUND6_PREFIX = (
    "import json, os, signal, sys, threading\n"
    "from pathlib import Path\n"
    "from unittest import mock\n"
    "root, folder, plant = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]\n"
    "sys.path.insert(0, str(root))\n"
    "signal.signal(signal.SIGINT, signal.default_int_handler)\n"
    "from scripts import process\n"
    "from scripts.quality import pre_push\n"
    "from typing import Any\n" + inspect.getsource(plant_patches) + SURVIVOR_SOURCE + "def living(pid):\n"
    "    try:\n"
    "        os.kill(pid, 0)\n"
    "    except ProcessLookupError:\n"
    "        return False\n"
    "    return True\n"
)
# A program with a member in a group of its own that ignores TERM (argv: case folder): it survives a stop that only reaches the leader's group.
MEMBER_IN_OTHER_GROUP = (
    "import os, signal, sys, time\\n"
    "from pathlib import Path\\n"
    "folder = Path(sys.argv[1])\\n"
    "(folder / 'leader.pid').write_text(str(os.getpid()))\\n"
    "if os.fork() == 0:\\n"
    "    os.setpgid(0, 0)\\n"
    "    signal.signal(signal.SIGTERM, signal.SIG_IGN)\\n"
    "    (folder / 'member.pid').write_text(str(os.getpid()))\\n"
    "while True:\\n"
    "    time.sleep(1)\\n"
)
# Gap 1 of the fix round 5 recheck: the real ``pre_push.run_steps`` with a command that reaches its deadline; its stop cannot list the session (the lister
# is missing) and SIGINT arrives while that stop runs. The pool must end with LaunchesNotStopped chained from the interrupt, not a plain KeyboardInterrupt.
TIMEOUT_OVERLAP_RUNNER = (
    ROUND6_PREFIX
    + 'SUBJECT = "'
    + MEMBER_IN_OTHER_GROUP
    + """"
began = threading.Event()
main_thread = threading.get_ident()
real_stop = process.stop_launch
def stop(*arguments, **keywords):
    if SUBJECT not in arguments[1].args:
        return real_stop(*arguments, **keywords)
    began.set()
    process.JOB.cancellation.event.wait(8)
    with mock.patch("scripts.process.SESSION_LISTING", ("/missing-lister",)):
        return real_stop(*arguments, **keywords)
def launch(_root, _step, _environment):
    return process.run_command(sys.executable, ["-B", "-c", SUBJECT, str(folder)], timeout=0.6, grace_seconds=0.1)
def send():
    if began.wait(8):
        signal.pthread_kill(main_thread, signal.SIGINT)
threading.Thread(target=send).start()
output = folder / "output"
output.mkdir()
step = {"name": "deadline", "kind": "repo_checks", "arguments": [], "env": {}, "timeout_seconds": 60}
patches = [mock.patch.object(pre_push, "launch", launch), mock.patch("scripts.process.stop_launch", stop), *plant_patches(plant)]
result = {}
for patch in patches:
    patch.start()
try:
    pre_push.run_steps(root, output, [step])
    result["raised"] = "nothing"
except BaseException as error:
    result["raised"] = type(error).__name__
    result["cause"] = type(error.__cause__).__name__
    result["message"] = str(error)[:300]
member = folder / "member.pid"
result["member_alive_at_return"] = member.exists() and living(int(member.read_text()))
result["stop_began"] = began.is_set()
(folder / "result.json").write_text(json.dumps(result))
"""
)
# A caller that treats ``ProcessGroupSurvived`` as a plain timeout and carries on (the shape of api_keys.py, s3_store.py and fetch_support.py).
SWALLOWING_RUNNER = (
    ROUND6_PREFIX
    + """
patches = [
    mock.patch("scripts.process.session_left", lambda _members: True),
    mock.patch("scripts.process.CONFIRM_SECONDS", 0.5),
    *plant_patches(plant),
]
for patch in patches:
    patch.start()
try:
    process.run_command(sys.executable, ["-B", "-c", "import time; time.sleep(30)"], timeout=0.5, grace_seconds=0.2)
except process.TimeoutExpired:
    pass
print("the caller carried on")
"""
)
# The real ``pre_push.run_steps`` with a step whose deadline stop is never confirmed and no interrupt: the step row and the pool must both say so.
POOL_TIMEOUT_RUNNER = (
    ROUND6_PREFIX
    + """
def launch(_root, _step, _environment):
    SURVIVOR.on = True
    return process.run_command(sys.executable, ["-B", "-c", "import time; time.sleep(30)"], timeout=0.5, grace_seconds=0.2)
output = folder / "output"
output.mkdir()
step = {"name": "survivor", "kind": "repo_checks", "arguments": [], "env": {}, "timeout_seconds": 60}
patches = [
    mock.patch.object(pre_push, "launch", launch),
    mock.patch("scripts.process.session_left", survivor_session_left),
    mock.patch("scripts.process.CONFIRM_SECONDS", 0.5),
    *plant_patches(plant),
]
for patch in patches:
    patch.start()
result = {}
try:
    rows = pre_push.run_steps(root, output, [step])
    result["raised"] = "nothing"
    result["rows"] = [{"name": row["name"], "exit": row["exit"], "error": row["error"]} for row in rows]
except BaseException as error:
    result["raised"] = type(error).__name__
    result["message"] = str(error)[:300]
(folder / "result.json").write_text(json.dumps(result))
"""
)


def run_round6_runner(scratch: Scratch, code: str, plant: str = "", seconds: float = 40.0) -> tuple[dict[str, Any], int | None, str]:
    """Run a fix round 6 runner in a session of its own; return what it recorded, its exit status and its standard error."""
    script = scratch.folder / "runner.py"
    script.write_text(code, encoding="utf-8")
    environment = {key: value for key, value in os.environ.items() if key != process.SESSION_VARIABLE}
    runner = process.start([sys.executable, "-B", str(script), str(ROOT), str(scratch.folder), plant], ROOT, environment, new_session=True)
    status: int | None = None
    error_text = ""
    try:
        try:
            _output, error_text = runner.communicate(timeout=seconds)
            status = runner.returncode
        except TimeoutExpired:
            status = None
        result_file = scratch.folder / "result.json"
        return (json.loads(result_file.read_text()) if result_file.exists() else {}), status, error_text
    finally:
        # A runner that never ended (the defect) and every program it started are killed.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(runner.pid, signal.SIGKILL)
        scratch.cleanup()
        process.close_pipes(runner)
        runner.wait(timeout=CASE_GRACE_SECONDS + 5)


def acknowledge_unconfirmed() -> list[str]:
    """Hand back what the launcher recorded as unconfirmed in this process (a case that forces a survivor checks it, then the record is cleared)."""
    take = getattr(process, "take_unconfirmed", None)
    return list(take()) if take is not None else []


def exit_status_marks_unconfirmed(status: int | None) -> bool:
    return status is not None and status == getattr(process, "UNCONFIRMED_EXIT_STATUS", -1)


def timeout_overlap(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        result, status, _error = run_round6_runner(scratch, TIMEOUT_OVERLAP_RUNNER, plant)
        return {
            "natural_timeout_cleanup_began": result.get("stop_began", False),
            "member_survived_the_stop": result.get("member_alive_at_return", False),
            "interrupt_became_a_named_failure": result.get("raised") == "LaunchesNotStopped",
            "interrupt_is_the_cause": result.get("cause") == "KeyboardInterrupt",
            "message_says_the_stop_is_not_confirmed": "not confirmed" in result.get("message", ""),
            "exit_status_marks_the_unconfirmed_stop": exit_status_marks_unconfirmed(status),
            "exception_seen": result.get("raised", "no result"),
        }

    return case("timeout_cleanup_failure_during_interrupt_is_named", base, body)


def cancelled_queued_future(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # Gap 2 of the fix round 5 recheck: one worker, one cooperative job, one queued job, SIGINT. The queued job is cancelled and never ran.
        ran: list[bool] = []
        raised = "nothing"
        started = time.monotonic()
        pool = process.LaunchPool(max_workers=1)
        with contextlib.ExitStack() as stack:
            for patch in [mock.patch("scripts.process.CANCEL_WAIT_SECONDS", 4.0), *plant_patches(plant)]:
                stack.enter_context(patch)
            try:
                with pool:
                    pool.submit(lambda: pool.cancellation.event.wait(10))
                    queued = pool.submit(lambda: ran.append(True))
                    signal.raise_signal(signal.SIGINT)
            except BaseException as error:
                raised = type(error).__name__
        seconds = time.monotonic() - started
        acknowledge_unconfirmed()
        return {
            "interrupt_reached_the_caller_unchanged": raised == "KeyboardInterrupt",
            "pool_did_not_wait_for_the_cancelled_future": seconds < 3.0,
            "queued_job_was_cancelled": queued.cancelled(),
            "queued_job_never_ran": not ran,
            "exception_seen": raised,
            "seconds": round(seconds, 2),
        }

    return case("cancelled_queued_future_is_not_waited_for", base, body)


def unconfirmed_on_interrupt(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # A caller without a pool, interrupted while its program runs; the stop of the session is never confirmed.
        main = threading.get_ident()
        subject = scratch.folder / "subject.pid"

        def sender() -> None:
            if wait_for_file(subject):
                signal.pthread_kill(main, signal.SIGINT)

        earlier = signal.signal(signal.SIGINT, signal.default_int_handler)
        sending = threading.Thread(target=sender, daemon=True)
        caught: BaseException | None = None
        try:
            with short_grace(survivor_probe=lambda _group: True), contextlib.ExitStack() as stack:
                for patch in plant_patches(plant):
                    stack.enter_context(patch)
                sending.start()
                try:
                    run_command(sys.executable, ["-B", "-c", TERM_IGNORING, str(subject)], cwd=ROOT, timeout=60)
                except BaseException as error:
                    caught = error
            sending.join(5)
        finally:
            signal.signal(signal.SIGINT, earlier)
        recorded = acknowledge_unconfirmed()
        pids = scratch.pids()
        return {
            "subject_started": "subject" in pids,
            "failure_is_named": type(caught).__name__ == "LaunchesNotStopped",
            "interrupt_is_the_cause": isinstance(getattr(caught, "__cause__", None), KeyboardInterrupt),
            "message_says_the_stop_is_not_confirmed": caught is not None and "not confirmed" in str(caught),
            "outcome_is_recorded": bool(recorded),
            "exception_seen": type(caught).__name__,
        }

    return case("unconfirmed_stop_on_interrupt_is_named", base, body)


def stop_that_raises(base: Path) -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The stop itself fails with an error; the call must still say that the program may be alive, with the error text.
        def broken(*_arguments: Any, **_keywords: Any) -> bool:
            raise OSError("stop failure for the case")

        subject = scratch.folder / "subject.pid"
        with mock.patch("scripts.process.stop_launch", broken):
            caught, _elapsed = timed_out_with(
                lambda: run_command(sys.executable, ["-B", "-c", TERM_IGNORING, str(subject)], cwd=ROOT, timeout=CASE_TIMEOUT_SECONDS)
            )
        recorded = acknowledge_unconfirmed()
        return {
            "subject_started": subject.exists(),
            "raised_survivor_error": isinstance(caught, ProcessGroupSurvived),
            "message_carries_the_stop_error": caught is not None and "stop failure for the case" in str(caught),
            "outcome_is_recorded": bool(recorded),
        }

    return case("stop_that_raises_is_unconfirmed", base, body)


def swallowing_caller(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        _result, status, error_text = run_round6_runner(scratch, SWALLOWING_RUNNER, plant, seconds=30)
        return {
            "caller_carried_on": status is not None,
            "stderr_names_the_unconfirmed_stop": "not confirmed" in error_text,
            "exit_status_marks_the_unconfirmed_stop": exit_status_marks_unconfirmed(status),
            "exit_status_seen": -1 if status is None else status,
        }

    return case("unconfirmed_stop_survives_a_swallowing_caller", base, body)


def pool_timeout_report(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        result, _status, _error = run_round6_runner(scratch, POOL_TIMEOUT_RUNNER, plant, seconds=30)
        rows = {row["name"]: row for row in result.get("rows", [])}
        return {
            "step_row_fails": rows.get("survivor", {}).get("exit") == 1,
            "step_row_says_the_stop_is_not_confirmed": "not confirmed" in rows.get("survivor", {}).get("error", ""),
            "step_row_does_not_claim_it_was_stopped": "and was stopped" not in rows.get("survivor", {}).get("error", "and was stopped"),
            "pool_report_row_fails": rows.get("stop_confirmation", {}).get("exit") == 1,
            "exception_seen": result.get("raised", "no result"),
        }

    return case("pool_reports_an_unconfirmed_timeout_stop", base, body)


# A stand-in lister (argv: case folder): it records itself, starts a child that holds its pipes and waits.
STALLED_LISTER = """
import os, subprocess, sys, time
folder = sys.argv[1]
open(os.path.join(folder, "lister.pid"), "w").write(str(os.getpid()))
holder = subprocess.Popen(["sleep", "60"])
open(os.path.join(folder, "holder.pid"), "w").write(str(holder.pid))
time.sleep(60)
"""


def interrupted_listing(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # An alarm (the exit trap's stop helper) ends the listing while the lister is stalled: the lister and the process that holds its pipes must go.
        def alarm(_number: int, _frame: Any) -> None:
            raise TimeoutError("alarm during the listing")

        earlier = signal.signal(signal.SIGALRM, alarm)
        caught: BaseException | None = None
        patches = [
            mock.patch("scripts.process.SESSION_LISTING", (sys.executable, "-B", "-c", STALLED_LISTER, str(scratch.folder))),
            mock.patch("scripts.process.LISTING_SECONDS", 30.0),
            *plant_patches(plant),
        ]
        try:
            with contextlib.ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                signal.setitimer(signal.ITIMER_REAL, 0.8)
                try:
                    process.list_processes()
                except TimeoutError as error:
                    caught = error
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
        finally:
            signal.signal(signal.SIGALRM, earlier)
        end = time.monotonic() + 3
        pids = scratch.pids()
        while time.monotonic() < end and any(alive(pid) for pid in pids.values()):
            time.sleep(0.05)
        return {
            "alarm_unwound_the_listing": isinstance(caught, TimeoutError),
            "lister_and_holder_started": "lister" in pids and "holder" in pids,
            "lister_gone": "lister" in pids and not alive(pids["lister"]),
            "holder_gone": "holder" in pids and not alive(pids["holder"]),
        }

    return case("interrupted_listing_leaves_no_lister", base, body)


# Fix round 7. The leader of this program (argv: case folder) forks a member that moves to a group of its own, ignores TERM, closes its output and waits,
# then ends at once with success: a normal end that leaves a member in the session.
LEFT_MEMBER = """
import os, signal, sys, time
folder = sys.argv[1]
open(os.path.join(folder, 'leader.pid'), 'w').write(str(os.getpid()))
if os.fork() == 0:
    os.setpgid(0, 0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    null = os.open(os.devnull, os.O_RDWR)
    for descriptor in (0, 1, 2):
        os.dup2(null, descriptor)
    open(os.path.join(folder, 'member.pid'), 'w').write(str(os.getpid()))
    time.sleep(90)
    os._exit(0)
end = time.monotonic() + 5
while not os.path.exists(os.path.join(folder, 'member.pid')) and time.monotonic() < end:
    time.sleep(0.01)
"""
# The shape of botocore's credential process (the inspected IAM ``Deadline``): the command starts a child that stays in its group with its output
# closed, then exits with status 2 at its own deadline before the outer one; the child lives on in the session.
CREDENTIAL_SHAPE = """
import os, sys, time
folder = sys.argv[1]
open(os.path.join(folder, 'leader.pid'), 'w').write(str(os.getpid()))
if os.fork() == 0:
    null = os.open(os.devnull, os.O_RDWR)
    for descriptor in (0, 1, 2):
        os.dup2(null, descriptor)
    open(os.path.join(folder, 'member.pid'), 'w').write(str(os.getpid()))
    time.sleep(90)
    os._exit(0)
end = time.monotonic() + 5
while not os.path.exists(os.path.join(folder, 'member.pid')) and time.monotonic() < end:
    time.sleep(0.01)
sys.exit(2)
"""
# A CI worker's leader (it stops its own session, allow_own) whose session cannot be listed (the lister is missing) and that has a member in a group of its own.
OWN_SESSION_RUNNER = (
    ROUND6_PREFIX
    + """
import time
if os.fork() == 0:
    os.setpgid(0, 0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    null = os.open(os.devnull, os.O_RDWR)
    for descriptor in (0, 1, 2):
        os.dup2(null, descriptor)
    (folder / "member.pid").write_text(str(os.getpid()))
    time.sleep(90)
    os._exit(0)
end = time.monotonic() + 5
while not (folder / "member.pid").exists() and time.monotonic() < end:
    time.sleep(0.01)
patches = [mock.patch("scripts.process.SESSION_LISTING", ("/missing-lister",)), *plant_patches(plant)]
for patch in patches:
    patch.start()
outcome = process.stop_sessions({os.getpid()}, 0.1, allow_own=True)
(folder / "result.json").write_text(json.dumps({"unconfirmed": bool(outcome), "reason": outcome.describe()}))
"""
)


def gone_within(pid: int | None, seconds: float = 3.0) -> bool:
    """Whether the process has ended within ``seconds`` (a killed member is reaped by init a moment after the stop)."""
    if pid is None:
        return False
    end = time.monotonic() + seconds
    while alive(pid):
        if time.monotonic() >= end:
            return False
        time.sleep(0.02)
    return True


def stop_recorded_exception_only(process_: Any, grace_seconds: float | None, nested: bool, cancellation: Any) -> tuple[StopOutcome, BaseException | None]:
    """The planted defect of fix round 7: the stop catches only ``Exception``, as before, so a ``SystemExit`` of an alarm handler escapes unrecorded."""
    outcome = StopOutcome()
    try:
        outcome = process.as_outcome(process.stop_launch(process_.pid, process_, grace_seconds, session=not nested), process_.pid)
    except Exception as failure:
        outcome = StopOutcome(True, frozenset({process_.pid}), f"the stop failed with {type(failure).__name__}: {failure}")
    process.close_pipes(process_)
    if outcome.unconfirmed:
        process.record_unconfirmed(process_.args, process_.pid, outcome, cancellation)
    return outcome, None


# A program with a member in a group of its own that ignores TERM (argv: case folder), which waits: the deadline stop of the alarm case.
WAITING_MEMBER = """
import os, signal, sys, time
folder = sys.argv[1]
open(os.path.join(folder, 'leader.pid'), 'w').write(str(os.getpid()))
if os.fork() == 0:
    os.setpgid(0, 0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    open(os.path.join(folder, 'member.pid'), 'w').write(str(os.getpid()))
while True:
    time.sleep(1)
"""


def alarm_during_the_stop(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The reviewer's probe: the program reaches its deadline, the stop lists the session with a stalled lister, and the real alarm handler of the scratch
        # worktree runner (``stop_run``: ``SystemExit``) fires during that listing.
        from scripts.quality.scratch_worktree import stop_run

        caught: BaseException | None = None
        lister_script = "import os, sys, time\nopen(os.path.join(sys.argv[1], 'lister.pid'), 'w').write(str(os.getpid()))\ntime.sleep(60)\n"
        patches: list[Any] = [
            mock.patch("scripts.process.SESSION_LISTING", (sys.executable, "-B", "-c", lister_script, str(scratch.folder))),
            mock.patch("scripts.process.LISTING_SECONDS", 10.0),
        ]
        if plant == "stop_catches_exception_only":
            patches.append(mock.patch("scripts.process.stop_recorded", stop_recorded_exception_only))
        earlier = signal.signal(signal.SIGALRM, stop_run)
        try:
            with contextlib.ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                signal.setitimer(signal.ITIMER_REAL, 0.8)
                try:
                    run_command(sys.executable, ["-B", "-c", WAITING_MEMBER, str(scratch.folder)], cwd=ROOT, timeout=0.2, grace_seconds=0.1)
                except BaseException as error:
                    caught = error
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
        finally:
            signal.signal(signal.SIGALRM, earlier)
        recorded = acknowledge_unconfirmed()
        pids = scratch.pids()
        return {
            "subject_and_lister_started": "leader" in pids and "member" in pids and "lister" in pids,
            "failure_is_named": isinstance(caught, ProcessGroupSurvived),
            "system_exit_is_the_cause": isinstance(getattr(caught, "__cause__", None), SystemExit),
            "message_says_the_stop_is_not_confirmed": caught is not None and "not confirmed" in str(caught),
            "outcome_is_recorded": bool(recorded),
            "lister_gone": gone_within(pids.get("lister")),
            "leader_stopped_by_the_last_kill": gone_within(pids.get("leader")),
            "exception_seen": type(caught).__name__,
        }

    return case("alarm_during_the_stop_is_named", base, body)


def leader_with_a_missing_lister(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        result, status, error_text = run_round6_runner(scratch, OWN_SESSION_RUNNER, plant, seconds=30)
        return {
            "caller_survived_the_fallback_to_report": "unconfirmed" in result,
            "outcome_is_unconfirmed": result.get("unconfirmed", False),
            "outcome_says_the_own_group_was_not_signalled": "own group" in result.get("reason", ""),
            "listing_failure_written_to_standard_error": "could not be listed" in error_text,
            "exit_status_marks_the_unconfirmed_stop": exit_status_marks_unconfirmed(status),
            "exit_status_seen": -1 if status is None else status,
        }

    return case("missing_lister_fallback_spares_the_confirming_caller", base, body)


def normal_end_finalizes(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # The reviewer's probes: a pool job and a plain call end normally (status 0, and status 2 in the credential-process shape) and leave a member in the
        # session; the launch must stop it and record nothing, because the stop is confirmed.
        shape = Scratch(scratch.folder, "shape")
        try:
            with contextlib.ExitStack() as stack:
                for patch in plant_patches(plant):
                    stack.enter_context(patch)
                with process.LaunchPool(max_workers=1) as pool:
                    left = ["-B", "-c", LEFT_MEMBER, str(scratch.folder)]
                    ended = pool.submit(lambda: run_command(sys.executable, left, timeout=20, grace_seconds=0.2)).result(30)
                credential = run_command(sys.executable, ["-B", "-c", CREDENTIAL_SHAPE, str(shape.folder)], cwd=ROOT, timeout=20, grace_seconds=0.2)
            recorded = acknowledge_unconfirmed()
            pids, shape_pids = scratch.pids(), shape.pids()
            return {
                "member_started": "member" in pids and "member" in shape_pids,
                "status_of_the_pool_job_kept": ended.returncode == 0,
                "status_of_the_credential_shape_kept": credential.returncode == 2,
                "member_gone_after_the_pool_job_ended": gone_within(pids.get("member")),
                "credential_shape_child_gone_after_exit_2": gone_within(shape_pids.get("member")),
                "nothing_recorded_for_a_confirmed_stop": not recorded,
            }
        finally:
            shape.cleanup()

    return case("normal_end_finalizes_the_session", base, body)


def unconfirmed_finalization(base: Path, plant: str = "") -> dict[str, Any]:
    def body(scratch: Scratch) -> dict[str, Any]:
        # A program ends normally and its session cannot be confirmed empty (a member survives KILL): the call is named, not a success.
        caught: BaseException | None = None
        with short_grace(survivor_probe=lambda _group: True), contextlib.ExitStack() as stack:
            for patch in plant_patches(plant):
                stack.enter_context(patch)
            try:
                run_command(sys.executable, ["-B", "-c", "pass"], cwd=ROOT, timeout=20)
            except BaseException as error:
                caught = error
        recorded = acknowledge_unconfirmed()
        return {
            "failure_is_named": type(caught).__name__ == "LaunchesNotStopped",
            "message_says_the_stop_is_not_confirmed": caught is not None and "not confirmed" in str(caught),
            "outcome_is_recorded": bool(recorded),
            "exception_seen": type(caught).__name__,
        }

    return case("unconfirmed_finalization_is_named", base, body)


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
    "ended_nested_group_counts_as_stopped": ended_nested_group,
    "descendant_cannot_hide_from_the_session_stop": hiding_descendant,
    "descendant_in_a_new_group_is_stopped": new_group_descendant,
    "unrelated_command_is_not_signalled": unrelated_command,
    "session_listing_failure_fails_visibly": listing_failure,
    "listing_start_failure_fails_visibly": listing_start_failure,
    "stalled_listing_is_bounded": stalled_listing,
    "second_interrupt_during_cleanup_is_deferred": second_interrupt,
    "member_that_leaves_before_the_signal_is_skipped": member_leaves_before_signal,
    "threaded_interrupt_stops_the_launch": threaded_interrupt,
    "threaded_interrupt_survivor_fails_visibly": threaded_survivor,
    "threaded_normal_run_unchanged": threaded_normal_run,
    "timeout_cleanup_failure_during_interrupt_is_named": timeout_overlap,
    "cancelled_queued_future_is_not_waited_for": cancelled_queued_future,
    "unconfirmed_stop_on_interrupt_is_named": unconfirmed_on_interrupt,
    "stop_that_raises_is_unconfirmed": stop_that_raises,
    "unconfirmed_stop_survives_a_swallowing_caller": swallowing_caller,
    "pool_reports_an_unconfirmed_timeout_stop": pool_timeout_report,
    "interrupted_listing_leaves_no_lister": interrupted_listing,
    "alarm_during_the_stop_is_named": alarm_during_the_stop,
    "missing_lister_fallback_spares_the_confirming_caller": leader_with_a_missing_lister,
    "normal_end_finalizes_the_session": normal_end_finalizes,
    "unconfirmed_finalization_is_named": unconfirmed_finalization,
    "default_grace_periods_are_set": default_periods,
}
REAL_STOP = process.stop_launch


def plant_direct_child_only(leader_pid: int, leader: Any = None, grace_seconds: float | None = None, session: bool = False) -> bool:
    """The defect WV3-027 describes: only the direct child is killed."""
    if leader is not None:
        leader.kill()
        leader.wait()
    return False


def plant_term_only(leader_pid: int, leader: Any = None, grace_seconds: float | None = None, session: bool = False) -> bool:
    """TERM to the leader's group, a wait for the grace period, no KILL."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(leader_pid, signal.SIGTERM)
    time.sleep(process.STOP_GRACE_SECONDS if grace_seconds is None else grace_seconds)
    if leader is not None:
        leader.poll()
    return False


def plant_no_confirmation(leader_pid: int, leader: Any = None, grace_seconds: float | None = None, session: bool = False) -> bool:
    """TERM and KILL to the leader's group, no check that anything is gone: a survivor is never reported."""
    for number in (signal.SIGTERM, signal.SIGKILL):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(leader_pid, number)
    if leader is not None:
        leader.poll()
    return False


def plant_leader_group_only(leader_pid: int, leader: Any = None, grace_seconds: float | None = None, session: bool = False) -> StopOutcome:
    """The real stop of the leader's process group, without the other members of the session (nested groups, a moved descendant)."""
    return process.stop_process_group(leader_pid, leader, grace_seconds)


# plant name -> (replacement for stop_launch, {case that must fail: the check that must be false in it})
PLANTS: dict[str, tuple[Callable[..., StopOutcome | bool], dict[str, str]]] = {
    "direct_child_only": (
        plant_direct_child_only,
        {"grandchild_stopped_at_deadline": "grandchild_gone", "grandchild_holding_pipe_stopped": "grandchild_gone"},
    ),
    "term_only": (plant_term_only, {"term_ignoring_grandchild_killed": "grandchild_gone", "term_ignoring_child_killed": "child_gone"}),
    "no_confirmation": (plant_no_confirmation, {"survivor_fails_visibly": "raised_survivor_error"}),
    "leader_group_only": (
        plant_leader_group_only,
        {
            "nested_launch_stopped_at_the_outer_deadline": "child_gone",
            "two_level_nested_launch_stopped_at_the_outer_deadline": "child_gone",
            "descendant_in_a_new_group_is_stopped": "grandchild_gone",
        },
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
        with mock.patch("scripts.process.stop_launch", replacement):
            ran = {case_name: CASES[case_name](base / f"plant_{name}") for case_name in wanted}
        caught = {case_name: caught_on(ran[case_name], check) for case_name, check in wanted.items()}
        raised = {case_name: result["checks"]["raised"] for case_name, result in ran.items() if "raised" in result["checks"]}
        results.append({"case": f"planted_{name}_is_caught", "passed": all(caught.values()), "checks": {**caught, **({"raised": raised} if raised else {})}})
    return results


# threaded plant name -> the check of threaded_interrupt_stops_the_launch that must be false
THREADED_PLANTS = {
    "no_cancellation_flag": "every_running_program_gone_before_the_interrupt_returned",
    "no_session_stop": "every_running_program_gone_before_the_interrupt_returned",
    "pool_does_not_wait": "every_running_program_gone_before_the_interrupt_returned",
}


def planted_threaded_cases(base: Path) -> list[dict[str, Any]]:
    """Each plant breaks one part of the pool's interrupt cleanup inside the runner; the threaded case must fail on the named check."""
    results: list[dict[str, Any]] = []
    for name, check in THREADED_PLANTS.items():
        result = threaded_interrupt(base / f"plant_{name}", name)
        results.append(
            {
                "case": f"planted_{name}_is_caught",
                "passed": caught_on(result, check),
                "checks": {check: result["checks"].get(check), "exception_seen": result["checks"].get("exception_seen")},
            }
        )
    return results


# fix round 6 plant name -> (case function, {case name: the check that must be false in it})
ROUND6_PLANTS: dict[str, tuple[Callable[..., dict[str, Any]], dict[str, str]]] = {
    "pool_ignores_unconfirmed": (
        timeout_overlap,
        {"timeout_cleanup_failure_during_interrupt_is_named": "interrupt_became_a_named_failure"},
    ),
    "pool_ignores_unconfirmed_report": (pool_timeout_report, {"pool_reports_an_unconfirmed_timeout_stop": "pool_report_row_fails"}),
    "no_record": (swallowing_caller, {"unconfirmed_stop_survives_a_swallowing_caller": "exit_status_marks_the_unconfirmed_stop"}),
    "cancelled_futures_counted": (cancelled_queued_future, {"cancelled_queued_future_is_not_waited_for": "interrupt_reached_the_caller_unchanged"}),
    "listing_not_unwound": (interrupted_listing, {"interrupted_listing_leaves_no_lister": "lister_gone"}),
    "interrupt_drops_the_outcome": (unconfirmed_on_interrupt, {"unconfirmed_stop_on_interrupt_is_named": "failure_is_named"}),
}


def planted_round6_cases(base: Path) -> list[dict[str, Any]]:
    """Each plant breaks one part of the shared outcome mechanism; its case must fail on the named check and raise nothing."""
    results: list[dict[str, Any]] = []
    for name, (function, wanted) in ROUND6_PLANTS.items():
        plant = "pool_ignores_unconfirmed" if name.startswith("pool_ignores_unconfirmed") else name
        result = function(base / f"plant_{name}", plant)
        ((case_name, check),) = wanted.items()
        results.append(
            {
                "case": f"planted_{name}_is_caught",
                "passed": result["case"] == case_name and caught_on(result, check),
                "checks": {check: result["checks"].get(check), "exception_seen": result["checks"].get("exception_seen")},
            }
        )
    return results


# fix round 7 plant name -> (case function, the plant label the case passes on, {case name: the check that must be false in it})
ROUND7_PLANTS: dict[str, tuple[Callable[..., dict[str, Any]], str, dict[str, str]]] = {
    "stop_catches_exception_only": (
        alarm_during_the_stop,
        "stop_catches_exception_only",
        {"alarm_during_the_stop_is_named": "failure_is_named"},
    ),
    "own_group_signalled": (
        leader_with_a_missing_lister,
        "own_group_signalled",
        {"missing_lister_fallback_spares_the_confirming_caller": "caller_survived_the_fallback_to_report"},
    ),
    "no_finalization": (normal_end_finalizes, "no_finalization", {"normal_end_finalizes_the_session": "member_gone_after_the_pool_job_ended"}),
    "no_finalization_report": (unconfirmed_finalization, "no_finalization", {"unconfirmed_finalization_is_named": "failure_is_named"}),
}


def planted_round7_cases(base: Path) -> list[dict[str, Any]]:
    """Each plant breaks one part of the fix round 7 mechanism; its case must fail on the named check and raise nothing."""
    results: list[dict[str, Any]] = []
    for name, (function, plant, wanted) in ROUND7_PLANTS.items():
        result = function(base / f"plant_{name}", plant)
        ((case_name, check),) = wanted.items()
        results.append(
            {
                "case": f"planted_{name}_is_caught",
                "passed": result["case"] == case_name and caught_on(result, check),
                "checks": {check: result["checks"].get(check), "exception_seen": result["checks"].get("exception_seen")},
            }
        )
    return results


def main() -> int:
    # The cases are outer launches: a run_ci.sh worker sets the session marker for its own launches, which would make every case a nested one.
    os.environ.pop(process.SESSION_VARIABLE, None)
    started = datetime.now(UTC)
    before = {name: sha256_of(ROOT / name) for name in WATCHED}
    base = Path(tempfile.mkdtemp(prefix="process_e2e."))
    # Case names on the command line run only those cases, without the plants (a quick check while a change is made).
    selected = sys.argv[1:]
    try:
        results = [CASES[name](base) for name in selected] if selected else [case_function(base) for case_function in CASES.values()]
        if not selected:
            results.extend(planted_cases(base))
            results.extend(planted_threaded_cases(base))
            results.extend(planted_round6_cases(base))
            results.extend(planted_round7_cases(base))
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
