"""E2E for the deadline on the workers of scripts/run_ci.sh (WV3-032): a worker that never ends is stopped with its process group and fails by name.

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_worker_deadline_e2e``. Each case copies the real dispatcher,
scripts/quality/timing.sh and config/quality/ci_groups.json into a scratch folder (a new Git repository, no worktree) with stand-in check groups and a
stand-in Python launcher that answers the plan, so the dispatcher's own gates, parallel set, deadline wait and exit trap run unchanged. One stand-in
worker never ends and starts a child that ignores TERM.
Cases: the run fails with the group's name and the deadline; the other worker keeps its result; no process of the stopped group is left (checked by
process ID); a worker that also ignores TERM is killed after the grace period; a single worker (the streaming path) fails the same way; the deadline
comes from ``worker_limits`` in the config when ``CI_WORKER_LIMIT_SECONDS`` is unset; a kind of worker without a limit stops the run before any
worker starts; workers that end in time pass; and a stop of the dispatcher itself through ``run_command`` (the pre-push deadline) leaves no worker.
Planted defects run the never-ending cases against a changed copy of the dispatcher (no deadline, which is the plain wait; a stop of the direct
process only) and each must be caught.
The report goes to data/e2e/worker_deadline/<UTC>_<pid>/report.json with the revision and the SHA-256 of the files under test.
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
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import SubprocessError, TimeoutExpired, run_command

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
# The body files of the stand-in groups; STUB_PYTHON_LINT picks the script the first group runs.
GROUP_BODIES = {
    "python_lint": 'python_lint() { timed "stub: python_lint" bash "$STUB_DIR/$STUB_PYTHON_LINT.sh" python_lint; }\n',
    "orchestration_checks": 'orchestration_checks() { timed "stub: orchestration_checks" bash "$STUB_DIR/quick.sh" orchestration_checks; }\n',
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
        ):
            (self.root / folder).mkdir(parents=True)
        (self.root / "scripts/run_ci.sh").write_text(
            dispatcher if dispatcher is not None else (ROOT / "scripts/run_ci.sh").read_text(encoding="utf-8"), encoding="utf-8"
        )
        shutil.copy2(ROOT / "scripts/quality/timing.sh", self.root / "scripts/quality/timing.sh")
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
        for name in ("worker", "child"):
            pid = self.pid(name)
            if pid is not None:
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
        "stop_confirmed": "Stopped: no process of the group" in text,
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


CASES: dict[str, Callable[[Path], dict[str, Any]]] = {
    "worker_that_never_ends_fails_with_the_group_name": never_ends_two_workers,
    "worker_that_ignores_term_is_killed_after_the_grace_period": term_ignoring_worker,
    "single_streaming_worker_that_never_ends_fails": single_worker,
    "deadline_comes_from_the_recorded_job_limit": limit_from_config,
    "worker_without_a_limit_stops_the_run_before_any_worker": missing_limit,
    "workers_that_end_in_time_pass": workers_in_time,
    "worker_that_fails_keeps_its_name": worker_failure_keeps_its_name,
    "stop_of_the_dispatcher_leaves_no_worker": dispatcher_stop,
}

# plant name -> (pairs of text replaced in the dispatcher, cases that must fail). The first plant is the plain wait of WV3-032: no worker is ever overdue.
PLANTS: dict[str, tuple[tuple[tuple[str, str], ...], tuple[str, ...]]] = {
    "no_deadline": (
        (("worker_overdue() { ((SECONDS - $1 >= $2)); }", "worker_overdue() { return 1; }"),),
        ("worker_that_never_ends_fails_with_the_group_name",),
    ),
    "direct_process_only": (
        (
            ('for pgid in "$@"; do kill -TERM -- "-$pgid" 2>/dev/null || true; done', 'for pgid in "$@"; do kill -TERM "$pgid" 2>/dev/null || true; done'),
            ('for pgid in "$@"; do kill -KILL -- "-$pgid" 2>/dev/null || true; done', 'for pgid in "$@"; do kill -KILL "$pgid" 2>/dev/null || true; done'),
        ),
        ("worker_that_ignores_term_is_killed_after_the_grace_period",),
    ),
}


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
        caught = {case_name: not result["passed"] for case_name, result in ran.items()}
        results.append({"case": f"planted_{name}_is_caught", "passed": all(caught.values()), "checks": caught})
    return results


def run_planted(base: Path, case_name: str, dispatcher: str) -> dict[str, Any]:
    """Run one case with a changed dispatcher in its sandbox."""
    Sandbox.dispatcher_override = dispatcher
    try:
        return CASES[case_name](base)
    finally:
        Sandbox.dispatcher_override = None


def main() -> int:
    started = datetime.now(UTC)
    before = {name: sha256_of(ROOT / name) for name in WATCHED}
    base = Path(tempfile.mkdtemp(prefix="worker_deadline_e2e."))
    try:
        results = [case_function(base) for case_function in CASES.values()]
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
