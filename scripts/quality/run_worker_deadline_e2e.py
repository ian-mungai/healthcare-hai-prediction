"""E2E for the deadline on the workers of scripts/run_ci.sh (WV3-032): a worker that never ends is stopped with its session and fails by name.

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_worker_deadline_e2e``. Each case copies the real dispatcher,
scripts/quality/timing.sh, scripts/process.py (the session stop) and config/quality/ci_groups.json into a scratch folder (a new Git repository, no
worktree) with stand-in check groups and a stand-in Python launcher that answers the plan, so the dispatcher's own gates, parallel set, deadline wait
and exit trap run unchanged. One stand-in
worker never ends and starts a child that ignores TERM.
Cases: the run fails with the group's name and the deadline; the other worker keeps its result; no process of the stopped group is left (checked by
process ID); a worker that also ignores TERM is killed after the grace period; a single worker (the streaming path) fails the same way; the deadline
comes from ``worker_limits`` in the config when ``CI_WORKER_LIMIT_SECONDS`` is unset; a kind of worker without a limit stops the run before any
worker starts; workers that end in time pass; a stop of the dispatcher itself through ``run_command`` (the pre-push deadline) leaves no worker; a
worker that starts its program through ``scripts/process.py`` ``run_command`` (a nested launch: a new process group in the worker's session) leaves no
process at the worker deadline or at a stop of the dispatcher; and a worker is never stopped before its limit (the start is aligned late in a
wall-clock second, where whole-second timestamps fire up to a second early: a worker that ends 0.25 s before the limit passes and a worker that never
ends gets TERM only after its limit). The review reproductions of fix round 4: a KILL of the dispatcher leaves no worker (the worker's leader stops its
session), a worker that ends with a background process left in its session leaves no process, and the exit trap ends within its bound when every
listing stalls.
Fix round 6: a worker's leader stops a left descendant while the dispatcher is paused, an alarm of the exit trap's stop step leaves no stalled
lister or pipe holder, and a deadline stop that cannot be confirmed is named.
Fix round 7: the stop helper and the worker's leader (whose fallback no longer signals its own group) exit with status 70 when the session cannot be
listed, and the dispatcher keeps that status through its exit trap.
Planted defects run the cases against a changed copy of the dispatcher (no deadline, which is the plain wait; a stop of the direct process only; the
old whole-second comparison; a stop of the worker's process group without the rest of its session). Each counts as caught only when the named
check of its case is false and the case raised nothing.
The report goes to data/e2e/worker_deadline/<UTC>_<pid>/report.json with the revision and the SHA-256 of the files under test.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import SESSION_VARIABLE, SubprocessError, TimeoutExpired, run_command, start

ROOT = Path(__file__).resolve().parents[2]
PYTHON = str(ROOT / ".venv" / "bin" / "python")
WATCHED = (
    "scripts/run_ci.sh",
    "scripts/process.py",
    "scripts/quality/timing.sh",
    "config/quality/ci_groups.json",
    "scripts/quality/run_worker_deadline_e2e.py",
)
LIMIT_SECONDS = 3
GRACE_SECONDS = 1
RUN_TIMEOUT_SECONDS = 20
# The stand-in Python launcher: the plan lists the groups of STUB_GROUPS, every other module succeeds, and -c code (the dispatcher's JSON reads) runs for real.
STUB_PYTHON = f"""#!/bin/bash
if [ -n "${{STUB_ALIGN_FRACTION:-}}" ] && [ "$1" = -c ] && [[ "$2" == *worker_limits* ]]; then
  {PYTHON} -c "import time; time.sleep(($STUB_ALIGN_FRACTION - time.time() % 1) % 1)"
fi
if [ "$1" = -m ]; then
  if [ "$2" = scripts.quality.ci_groups ]; then
    for group in $STUB_GROUPS; do echo "run $group"; done
  fi
  exit 0
fi
exec {PYTHON} "$@"
"""
NEVER_ENDS = """#!/bin/bash
# A worker that never ends: it starts a child that ignores TERM; with STUB_IGNORE_TERM=1 it ignores TERM too.
if [ "${STUB_IGNORE_TERM:-0}" = 1 ]; then trap '' TERM; fi
echo $$ >"$STUB_DIR/worker.pid"
bash "$STUB_DIR/ignore_term.sh" &
while :; do sleep 1; done
"""
IGNORE_TERM = """#!/bin/bash
trap '' TERM
echo $$ >"$STUB_DIR/child.pid"
while :; do sleep 1; done
"""
QUICK = """#!/bin/bash
sleep 1
echo done >"$STUB_DIR/$1.done"
"""
# A worker that ends 0.25 s before the limit of LIMIT_SECONDS.
NEAR = f"""#!/bin/bash
sleep {LIMIT_SECONDS - 0.25}
echo done >"$STUB_DIR/$1.done"
"""
# A worker that starts never_ends.sh through the shared launcher (a nested process group) and records its own pid.
LAUNCHER = f"""#!/bin/bash
exec {PYTHON} -B -c '
import os, sys
sys.path.insert(0, sys.argv[1])
from scripts.process import run_command
open(sys.argv[2] + "/launcher.pid", "w").write(str(os.getpid()))
run_command("bash", [sys.argv[2] + "/never_ends.sh"], timeout=600)
' {ROOT} "$STUB_DIR"
"""
# The launcher worker, which first removes every registry that the old launcher named in PROCESS_GROUP_REGISTRIES (the review reproduction).
HIDING_LAUNCHER = LAUNCHER.replace(
    "from scripts.process import run_command\n",
    "from scripts.process import run_command\n"
    'for path in [path for path in os.environ.get("PROCESS_GROUP_REGISTRIES", "").split(":") if path]:\n'
    "    os.path.exists(path) and os.unlink(path)\n",
)
# A worker that never ends and records, when TERM arrives, how long after its own start (monotonic) and at which wall-clock fraction it started.
TERM_TIME = f"""#!/bin/bash
exec {PYTHON} -B -c '
import json, os, signal, sys, time
started, fraction = time.monotonic(), time.time() % 1
def stop(_number, _frame):
    open(sys.argv[1] + "/term.json", "w").write(json.dumps({{"elapsed": time.monotonic() - started, "start_fraction": fraction}}))
    os._exit(0)
signal.signal(signal.SIGTERM, stop)
open(sys.argv[1] + "/worker.pid", "w").write(str(os.getpid()))
while True:
    time.sleep(1)
' "$STUB_DIR"
"""
# A worker that leaves a descendant in its session with closed output and ends at once with success (the review reproduction).
BACKGROUND = """#!/bin/bash
sleep 60 </dev/null >/dev/null 2>&1 &
echo $! >"$STUB_DIR/orphan.pid"
"""
# A worker that leaves a descendant in its session, then waits for the file "release" and ends with success (fix round 6: the worker's leader must stop the
# rest of its session before it exits, also while the dispatcher cannot act).
LINGERING = """#!/bin/bash
sleep 60 </dev/null >/dev/null 2>&1 &
echo $! >"$STUB_DIR/orphan.pid"
while [ ! -e "$STUB_DIR/release" ]; do sleep 0.1; done
"""
# A stand-in ps that never ends and keeps a record of every stalled listing (lister, then its holder, one pid per line in listers.txt).
LOGGING_STALLED_PS = """#!/bin/bash
echo $$ >>"$STUB_DIR/listers.txt"
sleep 60 &
echo $! >>"$STUB_DIR/listers.txt"
wait
"""
# A stand-in ps that never ends: it records itself and a child that holds its pipes (the review reproduction of a stalled listing).
STALLED_PS = """#!/bin/bash
echo $$ >"$STUB_DIR/lister.pid"
sleep 60 &
echo $! >"$STUB_DIR/lister_child.pid"
wait
"""
# Appended to the sandbox copy of scripts/process.py: the session lister does not exist, so every listing fails (the fix round 7 reproductions).
MISSING_LISTER = '\nSESSION_LISTING = ("/missing-review-lister",)\n'
# A worker command (argv: the stub folder) that leaves a member in a group of its own, ignoring TERM with output closed, and ends once the member is recorded.
LEFT_MEMBER = """
import os, signal, sys, time
folder = sys.argv[1]
open(os.path.join(folder, 'worker.pid'), 'w').write(str(os.getpid()))
if os.fork() == 0:
    os.setpgid(0, 0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    null = os.open(os.devnull, os.O_RDWR)
    for descriptor in (0, 1, 2):
        os.dup2(null, descriptor)
    open(os.path.join(folder, 'orphan.pid'), 'w').write(str(os.getpid()))
    time.sleep(90)
    os._exit(0)
end = time.monotonic() + 5
while not os.path.exists(os.path.join(folder, 'orphan.pid')) and time.monotonic() < end:
    time.sleep(0.01)
"""
# Seconds a worker may outlive the death of its dispatcher: the leader's poll, the grace period, the 5 s confirmation and slack for a loaded machine.
WATCHDOG_BOUND_SECONDS = 0.5 + GRACE_SECONDS + 5 + 3
# Seconds the exit trap may take when the listing stalls: one 5 s listing, the 5 s confirmation of the fallback group stop and slack.
TRAP_BOUND_SECONDS = 5 + 5 + 4
# The body files of the stand-in groups; STUB_PYTHON_LINT picks the script the first group runs.
GROUP_BODIES = {
    "python_lint": 'python_lint() { timed "stub: python_lint" bash "$STUB_DIR/$STUB_PYTHON_LINT.sh" python_lint; }\n',
    "orchestration_checks": (
        'orchestration_checks() { timed "stub: orchestration_checks" bash "$STUB_DIR/${STUB_ORCHESTRATION:-quick}.sh" orchestration_checks; }\n'
    ),
}


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


class Sandbox:
    """A scratch repository that holds the real dispatcher and stand-in groups; the stand-in processes are killed at the end of the case."""

    # A planted defect sets this to a changed copy of the dispatcher; every sandbox then holds that copy instead of the real file.
    dispatcher_override: str | None = None

    def __init__(self, base: Path, name: str, config: dict[str, Any] | None = None) -> None:
        dispatcher = Sandbox.dispatcher_override
        self.root = base / name
        self.stub = self.root / "stub"
        for folder in (
            "scripts/quality",
            "scripts/infrastructure",
            "scripts/ci/groups",
            "config/quality",
            ".tools/markdownlint-cli2/node_modules/.bin",
            "stub",
            "out",
            "bin",
        ):
            (self.root / folder).mkdir(parents=True)
        (self.root / "scripts/run_ci.sh").write_text(
            dispatcher if dispatcher is not None else (ROOT / "scripts/run_ci.sh").read_text(encoding="utf-8"), encoding="utf-8"
        )
        shutil.copy2(ROOT / "scripts/quality/timing.sh", self.root / "scripts/quality/timing.sh")
        shutil.copy2(ROOT / "scripts/process.py", self.root / "scripts/process.py")
        (self.root / "scripts/infrastructure/placeholder.sh").write_text("# empty\n", encoding="utf-8")
        for group, body in GROUP_BODIES.items():
            (self.root / f"scripts/ci/groups/{group}.sh").write_text(body, encoding="utf-8")
        document = config if config is not None else json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))
        (self.root / "config/quality/ci_groups.json").write_text(json.dumps(document), encoding="utf-8")
        (self.root / "config/data_paths.json").write_text('{"moved": []}\n', encoding="utf-8")
        for path, text in (
            ("bin_python", STUB_PYTHON),
            (".tools/markdownlint-cli2/node_modules/.bin/markdownlint-cli2", "#!/bin/bash\nexit 0\n"),
            ("stub/never_ends.sh", NEVER_ENDS),
            ("stub/ignore_term.sh", IGNORE_TERM),
            ("stub/quick.sh", QUICK),
            ("stub/near.sh", NEAR),
            ("stub/launcher.sh", LAUNCHER),
            ("stub/hiding_launcher.sh", HIDING_LAUNCHER),
            ("stub/term_time.sh", TERM_TIME),
            ("stub/background.sh", BACKGROUND),
            ("stub/lingering.sh", LINGERING),
            ("bin/ps", STALLED_PS),
        ):
            (self.root / path).write_text(text, encoding="utf-8")
            (self.root / path).chmod(0o755)
        run_command("git", ["init", "-q"], cwd=self.root, check=True)

    def environment(self, groups: str, extra: dict[str, str]) -> dict[str, str]:
        base = {key: value for key, value in os.environ.items() if not key.startswith(("CI_", "GIT_", "GITHUB_", "STUB_"))}
        return {
            **base,
            "PYTHON_BIN": str(self.root / "bin_python"),
            "STUB_GROUPS": groups,
            "STUB_DIR": str(self.stub),
            "STUB_PYTHON_LINT": "never_ends",
            "CI_PART": "core",
            "CI_SCOPE": "full",
            "GITHUB_STEP_SUMMARY": str(self.root / "out/summary.md"),
            "E2E_ARTIFACT_DIR": str(self.root / "out/e2e"),
            "QUALITY_ARTIFACT_DIR": str(self.root / "out/quality"),
            "CI_WORKER_GRACE_SECONDS": str(GRACE_SECONDS),
            **extra,
        }

    def run(self, groups: str = "python_lint orchestration_checks", timeout: float = RUN_TIMEOUT_SECONDS, **extra: str) -> dict[str, Any]:
        started = time.monotonic()
        result: dict[str, Any] = {"timed_out": False, "exit": None, "text": ""}
        try:
            done = run_command("/bin/bash", ["scripts/run_ci.sh"], cwd=self.root, env=self.environment(groups, extra), timeout=timeout)
            result.update(exit=done.returncode, text=done.stdout + done.stderr)
        except TimeoutExpired as expired:
            result["timed_out"] = True
            result["text"] = str(expired)
        result["seconds"] = round(time.monotonic() - started, 1)
        return result

    def pid(self, name: str) -> int | None:
        file = self.stub / f"{name}.pid"
        return int(file.read_text().strip()) if file.exists() and file.read_text().strip() else None

    def gone(self, name: str) -> bool:
        pid = self.pid(name)
        return pid is not None and not alive(pid)

    def cleanup(self) -> None:
        for name in ("worker", "child", "launcher", "orphan", "lister", "lister_child"):
            pid = self.pid(name)
            if pid is not None:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(pid, signal.SIGKILL)
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.kill(pid, signal.SIGKILL)


def case(name: str, base: Path, body: Callable[[Sandbox], dict[str, bool | str | float]], config: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    holder: list[Sandbox] = []
    try:
        sandbox = Sandbox(base, name, config)
        holder.append(sandbox)
        checks = body(sandbox)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, SubprocessError) as error:
        checks = {"raised": f"{type(error).__name__}: {error}"}
    finally:
        for sandbox in holder:
            sandbox.cleanup()
    flags = {key: value for key, value in checks.items() if isinstance(value, bool)}
    return {
        "case": name,
        "passed": bool(flags) and all(flags.values()) and "raised" not in checks,
        "seconds": round(time.monotonic() - started, 1),
        "checks": checks,
    }


def stopped_checks(sandbox: Sandbox, run: dict[str, Any], failed_line: str) -> dict[str, bool | str | float]:
    text = run["text"]
    return {
        "run_did_not_hang": not run["timed_out"],
        "exit_nonzero": run["exit"] not in (None, 0),
        "failure_line_names_the_group": f"Local CI failed in: {failed_line}" in text,
        "deadline_message_names_the_group_and_limit": f"(check group python_lint) ran past its deadline of {LIMIT_SECONDS} s" in text,
        "stop_confirmed": "Stopped: no process of the session" in text,
        "worker_gone": sandbox.gone("worker"),
        "child_that_ignores_term_gone": sandbox.gone("child"),
        "no_pass_line": "All local CI checks passed" not in text,
        "seconds": run["seconds"],
    }


def never_ends_two_workers(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(CI_WORKER_LIMIT_SECONDS=str(LIMIT_SECONDS))
        return {
            **stopped_checks(sandbox, run, f"python_lint (deadline {LIMIT_SECONDS} s)"),
            "other_worker_finished": (sandbox.stub / "orchestration_checks.done").exists(),
            "other_worker_not_named_failed": "orchestration_checks (deadline" not in run["text"]
            and "Local CI failed in: orchestration_checks" not in run["text"],
            "log_of_the_stopped_worker_is_printed": "== python_lint ==" in run["text"],
        }

    return case("worker_that_never_ends_fails_with_the_group_name", base, body)


def term_ignoring_worker(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(CI_WORKER_LIMIT_SECONDS=str(LIMIT_SECONDS), STUB_IGNORE_TERM="1")
        # The worker and its child ignore TERM, so they are gone only if KILL followed the grace period.
        return stopped_checks(sandbox, run, f"python_lint (deadline {LIMIT_SECONDS} s)")

    return case("worker_that_ignores_term_is_killed_after_the_grace_period", base, body)


def single_worker(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(groups="python_lint", CI_WORKER_LIMIT_SECONDS=str(LIMIT_SECONDS))
        return stopped_checks(sandbox, run, f"python_lint (deadline {LIMIT_SECONDS} s)")

    return case("single_streaming_worker_that_never_ends_fails", base, body)


def limit_from_config(base: Path) -> dict[str, Any]:
    document = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))
    document["worker_limits"]["python_lint"]["minutes"] = LIMIT_SECONDS / 60

    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run()
        return stopped_checks(sandbox, run, f"python_lint (deadline {LIMIT_SECONDS} s)")

    return case("deadline_comes_from_the_recorded_job_limit", base, body, config=document)


def missing_limit(base: Path) -> dict[str, Any]:
    document = json.loads((ROOT / "config/quality/ci_groups.json").read_text(encoding="utf-8"))
    del document["worker_limits"]["python_lint"]

    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(CI_WORKER_LIMIT_SECONDS=str(LIMIT_SECONDS))
        return {
            "run_did_not_hang": not run["timed_out"],
            "exit_nonzero": run["exit"] not in (None, 0),
            "names_the_worker": "The worker python_lint has no limit" in run["text"],
            "no_worker_started": not (sandbox.stub / "orchestration_checks.done").exists() and sandbox.pid("worker") is None,
        }

    return case("worker_without_a_limit_stops_the_run_before_any_worker", base, body, config=document)


def workers_in_time(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(STUB_PYTHON_LINT="quick", CI_WORKER_LIMIT_SECONDS="60")
        return {
            "exit_zero": run["exit"] == 0,
            "pass_line": "All local CI checks passed" in run["text"],
            "no_deadline_message": "ran past its deadline" not in run["text"],
            "both_workers_finished": (sandbox.stub / "python_lint.done").exists() and (sandbox.stub / "orchestration_checks.done").exists(),
            "seconds": run["seconds"],
        }

    return case("workers_that_end_in_time_pass", base, body)


def worker_failure_keeps_its_name(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        (sandbox.root / "stub/quick.sh").write_text("#!/bin/bash\nexit 3\n", encoding="utf-8")
        run = sandbox.run(STUB_PYTHON_LINT="quick", CI_WORKER_LIMIT_SECONDS="60")
        return {
            "exit_nonzero": run["exit"] not in (None, 0),
            "both_named": "Local CI failed in: python_lint orchestration_checks" in run["text"],
            "no_deadline_message": "ran past its deadline" not in run["text"],
        }

    return case("worker_that_fails_keeps_its_name", base, body)


def dispatcher_stop(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(timeout=8, CI_WORKER_LIMIT_SECONDS="600")
        return {
            "outer_deadline_fired": run["timed_out"],
            "workers_had_started": sandbox.pid("worker") is not None and sandbox.pid("child") is not None,
            "worker_gone": sandbox.gone("worker"),
            "child_that_ignores_term_gone": sandbox.gone("child"),
            "seconds": run["seconds"],
        }

    return case("stop_of_the_dispatcher_leaves_no_worker", base, body)


def launcher_worker(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(STUB_PYTHON_LINT="launcher", CI_WORKER_LIMIT_SECONDS=str(LIMIT_SECONDS))
        return {
            **stopped_checks(sandbox, run, f"python_lint (deadline {LIMIT_SECONDS} s)"),
            "launcher_started_the_nested_group": sandbox.pid("launcher") is not None and sandbox.pid("worker") is not None,
            "launcher_gone": sandbox.gone("launcher"),
        }

    return case("worker_using_the_launcher_leaves_no_nested_group", base, body)


def hiding_launcher_worker(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(STUB_PYTHON_LINT="hiding_launcher", CI_WORKER_LIMIT_SECONDS=str(LIMIT_SECONDS))
        return {
            **stopped_checks(sandbox, run, f"python_lint (deadline {LIMIT_SECONDS} s)"),
            "launcher_started_the_nested_group": sandbox.pid("launcher") is not None and sandbox.pid("worker") is not None,
            "nested_worker_gone": sandbox.gone("worker"),
        }

    return case("worker_that_removes_the_old_registry_leaves_no_process", base, body)


def launcher_worker_dispatcher_stop(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(timeout=8, STUB_PYTHON_LINT="launcher", CI_WORKER_LIMIT_SECONDS="600")
        return {
            "outer_deadline_fired": run["timed_out"],
            "workers_had_started": sandbox.pid("launcher") is not None and sandbox.pid("worker") is not None and sandbox.pid("child") is not None,
            "launcher_gone": sandbox.gone("launcher"),
            "nested_worker_gone": sandbox.gone("worker"),
            "child_that_ignores_term_gone": sandbox.gone("child"),
            "seconds": run["seconds"],
        }

    return case("stop_of_the_dispatcher_leaves_no_nested_group", base, body)


def wait_for_pids(sandbox: Sandbox, names: tuple[str, ...], seconds: float = 20.0) -> bool:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if all(sandbox.pid(name) is not None for name in names):
            return True
        time.sleep(0.05)
    return False


def all_gone_within(sandbox: Sandbox, names: tuple[str, ...], seconds: float) -> float | None:
    """Seconds until every named process has ended, or None when one is still alive after ``seconds``."""
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        if all(sandbox.gone(name) for name in names):
            return round(time.monotonic() - started, 2)
        time.sleep(0.1)
    return None


def stop_dispatcher(dispatcher: Any) -> None:
    """Kill what is left of a dispatcher that a case started directly (its group holds its exit trap's stop step) and reap it."""
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(dispatcher.pid, signal.SIGKILL)
    for pipe in (dispatcher.stdout, dispatcher.stderr):
        if pipe is not None:
            pipe.close()
    with contextlib.suppress(TimeoutExpired):
        dispatcher.wait(timeout=5)


def killed_dispatcher(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        # The review reproduction: the dispatcher dies by KILL, so its exit trap never runs; the worker's session must still stop.
        environment = sandbox.environment("python_lint", {"STUB_PYTHON_LINT": "launcher", "CI_WORKER_LIMIT_SECONDS": "600"})
        dispatcher = start(["/bin/bash", "scripts/run_ci.sh"], sandbox.root, environment, new_session=True)
        try:
            started = wait_for_pids(sandbox, ("launcher", "worker", "child"))
            os.kill(dispatcher.pid, signal.SIGKILL)
            dispatcher.wait(timeout=5)
            seconds = all_gone_within(sandbox, ("launcher", "worker", "child"), WATCHDOG_BOUND_SECONDS)
        finally:
            stop_dispatcher(dispatcher)
        return {
            "workers_had_started": started,
            "dispatcher_killed": dispatcher.returncode == -signal.SIGKILL,
            "worker_session_gone_within_the_bound": seconds is not None,
            "seconds_after_the_kill": -1.0 if seconds is None else seconds,
        }

    return case("killed_dispatcher_leaves_no_worker", base, body)


def completed_worker(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(groups="python_lint", STUB_PYTHON_LINT="background", CI_WORKER_LIMIT_SECONDS="60")
        return {
            "run_did_not_hang": not run["timed_out"],
            "exit_zero": run["exit"] == 0,
            "pass_line": "All local CI checks passed" in run["text"],
            "descendant_started": sandbox.pid("orphan") is not None,
            "descendant_gone_when_the_run_returns": sandbox.gone("orphan"),
            "seconds": run["seconds"],
        }

    return case("completed_worker_leaves_no_descendant", base, body)


def trap_with_stalled_listing(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        # The review reproduction: TERM to the dispatcher while every listing stalls; the exit trap must still end within its bound.
        path = f"{sandbox.root / 'bin'}{os.pathsep}{os.environ['PATH']}"
        environment = sandbox.environment("python_lint", {"CI_WORKER_LIMIT_SECONDS": "600", "PATH": path})
        dispatcher = start(["/bin/bash", "scripts/run_ci.sh"], sandbox.root, environment, new_session=True)
        try:
            started = wait_for_pids(sandbox, ("worker", "child"))
            began = time.monotonic()
            os.kill(dispatcher.pid, signal.SIGTERM)
            with contextlib.suppress(TimeoutExpired):
                dispatcher.wait(timeout=TRAP_BOUND_SECONDS)
            seconds = round(time.monotonic() - began, 2)
            exited = dispatcher.returncode is not None
            listing_stalled = sandbox.pid("lister") is not None
            workers_gone = all_gone_within(sandbox, ("worker", "child"), 2) is not None
        finally:
            stop_dispatcher(dispatcher)
        return {
            "workers_had_started": started,
            "listing_stalled": listing_stalled,
            "dispatcher_exited_within_the_bound": exited,
            "exit_nonzero": exited and dispatcher.returncode not in (None, 0),
            "worker_and_child_gone": workers_gone,
            "seconds_after_term": seconds,
        }

    return case("exit_trap_with_a_stalled_listing_is_bounded", base, body)


def ended_worker_leader(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        # Residual 1 (accepted limit A1): the worker ends with a descendant in its session while the dispatcher cannot act (it is paused). The worker's leader
        # must stop and confirm the rest of its session before it exits, so the descendant is gone with the dispatcher still paused.
        environment = sandbox.environment("python_lint", {"STUB_PYTHON_LINT": "lingering", "CI_WORKER_LIMIT_SECONDS": "600"})
        dispatcher = start(["/bin/bash", "scripts/run_ci.sh"], sandbox.root, environment, new_session=True)
        paused = False
        try:
            started = wait_for_pids(sandbox, ("orphan",))
            os.kill(dispatcher.pid, signal.SIGSTOP)
            paused = True
            (sandbox.stub / "release").write_text("go")
            seconds = all_gone_within(sandbox, ("orphan",), 8)
            gone_while_paused = seconds is not None
            os.kill(dispatcher.pid, signal.SIGCONT)
            paused = False
            with contextlib.suppress(TimeoutExpired):
                dispatcher.wait(timeout=15)
        finally:
            if paused:
                os.kill(dispatcher.pid, signal.SIGCONT)
            stop_dispatcher(dispatcher)
        return {
            "descendant_started": started,
            "descendant_gone_while_the_dispatcher_is_paused": gone_while_paused,
            "run_passed_after_the_dispatcher_resumed": dispatcher.returncode == 0,
            "seconds_until_the_descendant_was_gone": -1.0 if seconds is None else seconds,
        }

    return case("ended_worker_session_is_stopped_before_the_leader_exits", base, body)


def alarm_expiry(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        # Residual 2 (accepted limit A2): the exit trap's stop helper is ended by its alarm while its lister is stalled. The sandbox copy of the dispatcher
        # gets an alarm of the grace period alone (2 s) instead of 27 s, so the case takes seconds; the code under test is otherwise the real file.
        script = sandbox.root / "scripts/run_ci.sh"
        text = script.read_text(encoding="utf-8")
        if "+ 25)" not in text:
            return {"alarm_text_found": False}
        script.write_text(text.replace("+ 25)", "+ 0)"), encoding="utf-8")
        (sandbox.root / "bin/ps").write_text(LOGGING_STALLED_PS, encoding="utf-8")
        path = f"{sandbox.root / 'bin'}{os.pathsep}{os.environ['PATH']}"
        environment = sandbox.environment("python_lint", {"CI_WORKER_LIMIT_SECONDS": "600", "PATH": path})
        dispatcher = start(["/bin/bash", "scripts/run_ci.sh"], sandbox.root, environment, new_session=True)
        listers = sandbox.stub / "listers.txt"
        try:
            started = wait_for_pids(sandbox, ("worker", "child"))
            os.kill(dispatcher.pid, signal.SIGTERM)
            end = time.monotonic() + 10
            while time.monotonic() < end and (not listers.exists() or len(listers.read_text().split()) < 2):
                time.sleep(0.05)
            first = [int(field) for field in listers.read_text().split()[:2]] if listers.exists() else []
            # The helper's alarm fires 2 s after its start; the lister and the process that holds its pipes must be gone a few seconds later.
            end = time.monotonic() + 8
            while time.monotonic() < end and any(alive(pid) for pid in first):
                time.sleep(0.1)
            lister_gone = bool(first) and not alive(first[0])
            holder_gone = len(first) == 2 and not alive(first[1])
            with contextlib.suppress(TimeoutExpired):
                dispatcher.wait(timeout=TRAP_BOUND_SECONDS)
        finally:
            stop_dispatcher(dispatcher)
            if listers.exists():
                for field in listers.read_text().split():
                    with contextlib.suppress(ProcessLookupError, PermissionError):
                        os.killpg(int(field), signal.SIGKILL)
                    with contextlib.suppress(ProcessLookupError, PermissionError):
                        os.kill(int(field), signal.SIGKILL)
        return {
            "workers_had_started": started,
            "stop_helper_listing_stalled": len(first) == 2,
            "stalled_lister_gone_after_the_alarm": lister_gone,
            "process_holding_its_pipes_gone_after_the_alarm": holder_gone,
        }

    return case("alarm_expiry_leaves_no_lister_or_holder", base, body)


def missing_lister_files(sandbox: Sandbox) -> None:
    """Make every session listing of the sandbox copy of scripts/process.py fail: the lister is not on PATH."""
    path = sandbox.root / "scripts/process.py"
    path.write_text(path.read_text(encoding="utf-8") + MISSING_LISTER, encoding="utf-8")


def sandbox_function(sandbox: Sandbox, pattern: str) -> str:
    """The text a pattern selects in the sandbox copy of the dispatcher (the planted copy in a plant run)."""
    match = re.search(pattern, (sandbox.root / "scripts/run_ci.sh").read_text(encoding="utf-8"), re.MULTILINE | re.DOTALL)
    if match is None:
        raise ValueError(f"the dispatcher has no text for {pattern}")
    return match.group(1) if match.groups() else match.group(0)


def stop_helper_status(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        # The reviewer's probe: the exact stop helper of the dispatcher, run on a session whose listing fails. It prints that the stop is not confirmed and must
        # exit with status 70, not 1.
        missing_lister_files(sandbox)
        helper = sandbox_function(sandbox, r"^stop_sessions\(\) \{\n.*?^\}")
        subject = start([sys.executable, "-B", "-c", "import time; time.sleep(60)"], sandbox.root, None, new_session=True)
        # The script holds the helper and its call; the call's values are quoted into it, so the program that runs is a literal script path.
        call = f"repo_root={shlex.quote(str(sandbox.root))}\npython_bin={shlex.quote(sys.executable)}\nstop_sessions 1 {subject.pid}\n"
        (sandbox.root / "stop_helper.sh").write_text(f"{helper}\n{call}", encoding="utf-8")
        try:
            done = run_command("/bin/bash", ["stop_helper.sh"], cwd=sandbox.root, timeout=30)
            end = time.monotonic() + 3
            while subject.poll() is None and time.monotonic() < end:
                time.sleep(0.05)
            stopped = subject.poll() is not None
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(subject.pid, signal.SIGKILL)
            subject.wait(timeout=5)
            for pipe in (subject.stdout, subject.stderr):
                if pipe is not None:
                    pipe.close()
        return {
            "helper_exit_status_is_70": done.returncode == 70,
            "helper_says_the_stop_is_not_confirmed": "not confirmed" in done.stderr,
            "leader_group_still_stopped_by_the_fallback": stopped,
            "exit_status_seen": done.returncode,
        }

    return case("stop_helper_keeps_status_70", base, body)


def leader_status(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        # The reviewer's probe: the worker ends and leaves a member in another group of its session while the session cannot be listed. The leader runs the
        # real worker_session_step. It must not kill itself in the fallback: it reports that the stop is not confirmed and exits with status 70.
        missing_lister_files(sandbox)
        step = sandbox_function(sandbox, r"worker_session_step='(.*?)'\n")
        # The dispatcher starts the step as a background job (not a group leader, so its setsid works): this script does the same.
        arguments = [sys.executable, "-I", "-c", step, str(sandbox.root), "0.1", sys.executable, "-B", "-c", LEFT_MEMBER, str(sandbox.stub)]
        (sandbox.root / "start_step.sh").write_text(f"{shlex.join(arguments)} </dev/null &\nwait $!\n", encoding="utf-8")
        done = run_command("/bin/bash", ["start_step.sh"], cwd=sandbox.root, timeout=30)
        return {
            "member_started": sandbox.pid("orphan") is not None,
            "leader_was_not_killed_by_its_own_fallback": done.returncode < 128,
            "leader_says_the_stop_is_not_confirmed": "not confirmed" in done.stderr,
            "leader_exit_status_is_70": done.returncode == 70,
            "exit_status_seen": done.returncode,
        }

    return case("leader_with_a_missing_lister_reports_status_70", base, body)


def unconfirmed_deadline_stop(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        # The deadline stop cannot list the session (every listing stalls). The failure entry must say that the stop is not confirmed, and the dispatcher
        # must not wait for the worker without a bound.
        path = f"{sandbox.root / 'bin'}{os.pathsep}{os.environ['PATH']}"
        run = sandbox.run(groups="python_lint", timeout=45, CI_WORKER_LIMIT_SECONDS=str(LIMIT_SECONDS), PATH=path)
        return {
            "run_did_not_hang": not run["timed_out"],
            "exit_nonzero": run["exit"] not in (None, 0),
            "exit_status_is_70": run["exit"] == 70,
            "failure_names_the_group_and_the_unconfirmed_stop": f"Local CI failed in: python_lint (deadline {LIMIT_SECONDS} s, stop not confirmed)"
            in run["text"],
            "no_pass_line": "All local CI checks passed" not in run["text"],
            "seconds": run["seconds"],
        }

    return case("deadline_stop_that_cannot_be_confirmed_is_named", base, body)


def not_before_the_limit(base: Path) -> dict[str, Any]:
    def body(sandbox: Sandbox) -> dict[str, bool | str | float]:
        run = sandbox.run(STUB_PYTHON_LINT="term_time", STUB_ORCHESTRATION="near", STUB_ALIGN_FRACTION="0.75", CI_WORKER_LIMIT_SECONDS=str(LIMIT_SECONDS))
        term = sandbox.stub / "term.json"
        record = json.loads(term.read_text()) if term.exists() else {}
        elapsed = float(record.get("elapsed", 0.0))
        return {
            "run_did_not_hang": not run["timed_out"],
            "only_the_endless_worker_failed": f"Local CI failed in: python_lint (deadline {LIMIT_SECONDS} s)\n" in run["text"],
            "worker_ending_before_the_limit_passed": (sandbox.stub / "orchestration_checks.done").exists(),
            "term_reached_the_endless_worker": term.exists(),
            # The worker measures from its own start, a little after the dispatcher's, so 0.2 s is allowed for that start.
            "term_not_before_the_limit": term.exists() and elapsed >= LIMIT_SECONDS - 0.2,
            # Whole-second timestamps fire early only for a start late in a second; a start that drifted past the next second tests nothing.
            "start_late_in_a_wall_clock_second": 0.6 <= float(record.get("start_fraction", -1.0)) < 1.0,
            "term_seconds_after_worker_start": round(elapsed, 3),
            "worker_start_wall_fraction": round(float(record.get("start_fraction", -1.0)), 3),
            "seconds": run["seconds"],
        }

    return case("worker_is_not_stopped_before_its_limit", base, body)


CASES: dict[str, Callable[[Path], dict[str, Any]]] = {
    "worker_that_never_ends_fails_with_the_group_name": never_ends_two_workers,
    "worker_that_ignores_term_is_killed_after_the_grace_period": term_ignoring_worker,
    "single_streaming_worker_that_never_ends_fails": single_worker,
    "deadline_comes_from_the_recorded_job_limit": limit_from_config,
    "worker_without_a_limit_stops_the_run_before_any_worker": missing_limit,
    "workers_that_end_in_time_pass": workers_in_time,
    "worker_that_fails_keeps_its_name": worker_failure_keeps_its_name,
    "stop_of_the_dispatcher_leaves_no_worker": dispatcher_stop,
    "worker_using_the_launcher_leaves_no_nested_group": launcher_worker,
    "stop_of_the_dispatcher_leaves_no_nested_group": launcher_worker_dispatcher_stop,
    "worker_that_removes_the_old_registry_leaves_no_process": hiding_launcher_worker,
    "worker_is_not_stopped_before_its_limit": not_before_the_limit,
    "killed_dispatcher_leaves_no_worker": killed_dispatcher,
    "completed_worker_leaves_no_descendant": completed_worker,
    "exit_trap_with_a_stalled_listing_is_bounded": trap_with_stalled_listing,
    "ended_worker_session_is_stopped_before_the_leader_exits": ended_worker_leader,
    "alarm_expiry_leaves_no_lister_or_holder": alarm_expiry,
    "deadline_stop_that_cannot_be_confirmed_is_named": unconfirmed_deadline_stop,
    "stop_helper_keeps_status_70": stop_helper_status,
    "leader_with_a_missing_lister_reports_status_70": leader_status,
}

# plant name -> (pairs of text replaced in the dispatcher, {case that must fail: the check that must be false in it}). The first plant is the plain wait
# of WV3-032: no worker is ever overdue.
# The stop plants end in "|| true": a stop that reports success, so the dispatcher forgets the worker (an unconfirmed stop keeps it for the exit trap).
PLANTS: dict[str, tuple[tuple[tuple[str, str], ...], dict[str, str]]] = {
    "no_deadline": (
        (("worker_overdue() { ((SECONDS - $1 > $2)); }", "worker_overdue() { return 1; }"),),
        {"worker_that_never_ends_fails_with_the_group_name": "run_did_not_hang"},
    ),
    "direct_process_only": (
        (('stop_sessions "$worker_grace" "$pid"', '{ kill -TERM "$pid"; sleep 1; kill -KILL "$pid"; } 2>/dev/null || true'),),
        {"worker_that_ignores_term_is_killed_after_the_grace_period": "child_that_ignores_term_gone"},
    ),
    "deadline_one_second_early": (
        (("worker_overdue() { ((SECONDS - $1 > $2)); }", "worker_overdue() { ((SECONDS - $1 >= $2)); }"),),
        {"worker_is_not_stopped_before_its_limit": "term_not_before_the_limit"},
    ),
    # Fix round 6: the worker's leader exits with the worker without stopping the rest of its session (accepted limit A1 before).
    "leader_exits_at_once": (
        (
            (
                "outcome = stop_sessions({os.getpid()}, float(sys.argv[2]), allow_own=True)",
                "outcome = None if status is not None else stop_sessions({os.getpid()}, float(sys.argv[2]), allow_own=True)",
            ),
        ),
        {"ended_worker_session_is_stopped_before_the_leader_exits": "descendant_gone_while_the_dispatcher_is_paused"},
    ),
    # Fix round 6: the stop helper's alarm has no handler, so it ends the helper without the unwinding (accepted limit A2 before).
    "alarm_without_unwinding": (
        (("signal.signal(signal.SIGALRM, expired)\n", ""),),
        {"alarm_expiry_leaves_no_lister_or_holder": "stalled_lister_gone_after_the_alarm"},
    ),
    # Fix round 6: a deadline stop that is not confirmed is written like a confirmed one.
    "unconfirmed_stop_not_named": (
        (('s, stop not confirmed)")', 's)")'),),
        {"deadline_stop_that_cannot_be_confirmed_is_named": "failure_names_the_group_and_the_unconfirmed_stop"},
    ),
    # Fix round 7: the stop helper, the worker's leader and the exit trap end with status 1 again instead of 70. The helper and the leader end with os._exit,
    # because the unconfirmed stop on record (scripts/process.py) would make the interpreter exit with 70 at exit anyway.
    "helper_exits_1": (
        (("sys.exit(UNCONFIRMED_EXIT_STATUS if outcome else 0)'", "import os; sys.stderr.flush(); os._exit(1 if outcome else 0)'"),),
        {"stop_helper_keeps_status_70": "helper_exit_status_is_70"},
    ),
    "leader_exits_1": (
        (
            (
                "sys.exit(UNCONFIRMED_EXIT_STATUS if outcome else (0 if status is None else status))'",
                "sys.stderr.flush(); os._exit(1 if outcome else (0 if status is None else status))'",
            ),
        ),
        {"leader_with_a_missing_lister_reports_status_70": "leader_exit_status_is_70"},
    ),
    "trap_exits_1": (
        (('exit "$unconfirmed_status"; fi; }', "exit 1; fi; }"),),
        {"deadline_stop_that_cannot_be_confirmed_is_named": "exit_status_is_70"},
    ),
    "worker_group_only": (
        (('stop_sessions "$worker_grace" "$pid"', '{ kill -TERM -- "-$pid"; sleep 1; kill -KILL -- "-$pid"; } 2>/dev/null || true'),),
        {"worker_using_the_launcher_leaves_no_nested_group": "child_that_ignores_term_gone"},
    ),
}


def caught_on(result: dict[str, Any], check: str) -> bool:
    """A plant is caught only by the intended failed check: a case that raised (a setup error) is no evidence."""
    checks = result["checks"]
    return "raised" not in checks and checks.get(check) is False


def planted_cases(base: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    dispatcher = (ROOT / "scripts/run_ci.sh").read_text(encoding="utf-8")
    for name, (replacements, wanted) in PLANTS.items():
        if any(old not in dispatcher for old, _ in replacements):
            results.append({"case": f"planted_{name}_is_caught", "passed": False, "checks": {"plant_text_found": False}})
            continue
        planted = dispatcher
        for old, new in replacements:
            planted = planted.replace(old, new)
        ran = {case_name: run_planted(base / f"plant_{name}", case_name, planted) for case_name in wanted}
        caught = {case_name: caught_on(ran[case_name], check) for case_name, check in wanted.items()}
        raised = {case_name: result["checks"]["raised"] for case_name, result in ran.items() if "raised" in result["checks"]}
        results.append({"case": f"planted_{name}_is_caught", "passed": all(caught.values()), "checks": {**caught, **({"raised": raised} if raised else {})}})
    return results


def run_planted(base: Path, case_name: str, dispatcher: str) -> dict[str, Any]:
    """Run one case with a changed dispatcher in its sandbox."""
    Sandbox.dispatcher_override = dispatcher
    try:
        return CASES[case_name](base)
    finally:
        Sandbox.dispatcher_override = None


def main() -> int:
    # The cases are outer launches: a run_ci.sh worker sets the session marker for its own launches, which would make every case a nested one.
    os.environ.pop(SESSION_VARIABLE, None)
    started = datetime.now(UTC)
    before = {name: sha256_of(ROOT / name) for name in WATCHED}
    base = Path(tempfile.mkdtemp(prefix="worker_deadline_e2e."))
    # Case names on the command line run only those cases, without the plants (a quick check while a change is made).
    selected = sys.argv[1:]
    try:
        results = [CASES[name](base) for name in selected] if selected else [case_function(base) for case_function in CASES.values()]
        if not selected:
            results.extend(planted_cases(base))
    finally:
        shutil.rmtree(base, ignore_errors=True)
    after = {name: sha256_of(ROOT / name) for name in WATCHED}
    if before != after:
        results.append({"case": "code_unchanged_during_run", "passed": False, "changed": sorted(name for name in before if before[name] != after[name])})
    failed = [result["case"] for result in results if not result["passed"]]
    out = ROOT / "data" / "e2e" / "worker_deadline" / f"{started:%Y%m%dT%H%M%SZ}_{os.getpid()}"
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
