"""Synthetic cases for the training job module: dispatch, named failures, deadlines and the snapshot-mount smoke run."""

from __future__ import annotations

import contextlib
import io
import json
import os
import signal
import sys
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import training_contract
from scripts.process import run_command
from scripts.training import guards, job, job_failure
from scripts.training.lane_b_cases import RUN, Cases, require
from scripts.training.step_records import verify_done

DIGEST, COMMIT = "a" * 64, "b" * 40


def call(arguments: Sequence[str]) -> tuple[int, str, str]:
    """Run the job entry point in this process and return its exit code, standard output and standard error."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = job.main(list(arguments))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def failure_of(error: str) -> str:
    """Return the failure name of the job's one-line failure record."""
    return str(json.loads(error.strip().splitlines()[-1])["failure"])


def arguments_for(action: str, folder: Path, record: Path | None = None) -> list[str]:
    """Build the option set the training renderer always passes."""
    return [
        action,
        "--run",
        RUN,
        "--mode",
        "fixture",
        "--run-dir",
        str(folder / "out"),
        "--parameters",
        str(folder / "training_parameters.json"),
        "--snapshot-record",
        str(record or folder / "snapshot_training.json"),
    ]


def prepare(folder: Path, **changes: Any) -> Path:
    """Create a run folder with a snapshot record, a parameter file and an empty output folder."""
    folder.mkdir(parents=True)
    (folder / "out").mkdir()
    (folder / "training_parameters.json").write_text("{}\n")
    record = {"schema_version": 1, "run_id": RUN, "code_digest": DIGEST, "git_commit": COMMIT, "git_dirty": False, "files": []} | changes
    (folder / "snapshot_training.json").write_text(json.dumps(record) + "\n")
    return folder


def listing(folder: Path) -> list[str]:
    """List every file under the output folder."""
    return sorted(path.relative_to(folder).as_posix() for path in folder.rglob("*") if path.is_file())


def dispatch_cases(cases: Cases, folder: Path) -> None:
    """Require one dispatch per action to its own callable, and a named refusal where no implementation exists."""
    cases.check("job_actions_match_contract", lambda: require(set(job.ACTIONS) == set(training_contract.PREREQUISITES) - {"extract"}, "action lists differ"))
    cases.check("job_unknown_action_refused", lambda: require(call(["nonsense"])[0] == 2, "an unknown action was accepted"))
    for action in job.ACTIONS:

        def dispatched(action: str = action) -> None:
            seen: list[job.Context] = []
            root = prepare(folder / f"dispatch_{action}")
            with mock.patch.dict(job.HANDLERS, {action: seen.append}):
                code, _, error = call(arguments_for(action, root))
            require(code == 0 and len(seen) == 1 and seen[0].action == action, f"{action} did not reach its callable once: {code} {error}")

        cases.check(f"job_dispatch_{action}", dispatched)
    for action in (name for name in job.ACTIONS if name not in job.HANDLERS):

        def refused(action: str = action) -> None:
            root = prepare(folder / f"absent_{action}")
            code, _, error = call(arguments_for(action, root))
            require(code == job.EXIT_NOT_IMPLEMENTED and failure_of(error) == "action_not_implemented", f"{action}: {code} {error}")
            require(listing(root / "out") == [], f"{action} wrote output without an implementation")

        cases.check(f"job_{action}_not_implemented", refused)


def argument_cases(cases: Cases, folder: Path) -> None:
    """Check the named refusals before any action runs."""

    def missing() -> None:
        code, _, error = call(["guard"])
        require(code == job.EXIT_FAILED and failure_of(error) == "missing_argument", f"{code} {error}")

    def record() -> None:
        changes: tuple[dict[str, Any], ...] = ({"schema_version": 2}, {"git_commit": "main"}, {"code_digest": "short"})
        for index, change in enumerate(changes):
            root = prepare(folder / f"record_{index}", **change)
            code, _, error = call(arguments_for("guard", root))
            require(code == job.EXIT_FAILED and failure_of(error) == "snapshot_record" and listing(root / "out") == [], f"{change}: {code} {error}")
        root = prepare(folder / "record_unreadable")
        (root / "snapshot_training.json").write_text("{")
        require(failure_of(call(arguments_for("guard", root))[2]) == "snapshot_record", "an unreadable record was accepted")

    def mismatch() -> None:
        root = prepare(folder / "record_run", run_id="20261024T000000Z-ffffffff")
        code, _, error = call(arguments_for("guard", root))
        require(code == job.EXIT_FAILED and failure_of(error) == "run_mismatch" and listing(root / "out") == [], f"{code} {error}")

    def run_dir() -> None:
        root = prepare(folder / "run_dir")
        (root / "out").rmdir()
        code, _, error = call(arguments_for("guard", root))
        require(code == job.EXIT_FAILED and failure_of(error) == "run_dir", f"{code} {error}")

    def environment() -> None:
        root = prepare(folder / "environment", run_id="other")
        with mock.patch.dict(os.environ, {"AWS_SECRET_ACCESS_KEY": "synthetic-sentinel-value"}):
            _, out, error = call(arguments_for("guard", root))
        require("synthetic-sentinel-value" not in out + error, "a failure line carried an environment value")

    cases.check("job_missing_argument_named", missing)
    cases.check("job_snapshot_record_refused", record)
    cases.check("job_run_mismatch_refused", mismatch)
    cases.check("job_run_dir_missing_named", run_dir)
    cases.check("job_failure_line_has_no_environment", environment)


def guard_cases(cases: Cases, folder: Path) -> None:
    """Run the real static guard on this checkout, then its refusal, retry and conflict paths."""

    def written() -> None:
        root = prepare(folder / "guard")
        code, out, error = call(arguments_for("guard", root))
        require(code == 0, f"the guard refused this checkout: {error}")
        text = (root / "out/guard.json").read_text()
        require(out == text and json.loads(text)["git_commit"] == COMMIT, "guard.json is not the printed record")
        require(listing(root / "out") == ["guard.done.json", "guard.json"], "the guard left other files")
        verify_done(root / "out", "guard", DIGEST)
        require(call(arguments_for("guard", root))[0] == 0 and (root / "out/guard.json").read_text() == text, "a repeat changed the completed step")

    def refused() -> None:
        root = prepare(folder / "guard_refused")
        with mock.patch.object(guards, "check", side_effect=guards.GuardError("synthetic refusal")):
            code, _, error = call(arguments_for("guard", root))
        require(code == job.EXIT_FAILED and failure_of(error) == "guard_refused" and "synthetic refusal" in error, f"{code} {error}")
        require(listing(root / "out") == [], "a refused guard wrote output")

    def interrupted() -> None:
        root = prepare(folder / "guard_interrupted")
        with mock.patch.object(guards, "check", return_value={"schema": 1}):
            require(call(arguments_for("guard", root))[0] == 0, "the first guard run failed")
            (root / "out/guard.done.json").unlink()
            code, _, error = call(arguments_for("guard", root))
        require(code == 0 and verify_done(root / "out", "guard", DIGEST)["step"] == "guard", f"the retry did not complete: {error}")

    def conflict() -> None:
        root = prepare(folder / "guard_conflict")
        with mock.patch.object(guards, "check", return_value={"schema": 1}):
            require(call(arguments_for("guard", root))[0] == 0, "the first guard run failed")
        before = (root / "out/guard.json").read_bytes()
        with mock.patch.object(guards, "check", return_value={"schema": 2}):
            code, _, error = call(arguments_for("guard", root))
        require(code == job.EXIT_FAILED and failure_of(error) == "done_conflict", f"{code} {error}")
        require((root / "out/guard.json").read_bytes() == before, "a completed output was replaced")

    cases.check("job_guard_writes_record_and_done", written)
    cases.check("job_guard_refusal_writes_nothing", refused)
    cases.check("job_guard_retry_after_interrupt", interrupted)
    cases.check("job_guard_done_conflict", conflict)


def deadline_case(cases: Cases, folder: Path) -> None:
    """Require the named deadline failure and a clean timer and handler afterwards."""

    def expired() -> None:
        root = prepare(folder / "deadline")
        previous = signal.getsignal(signal.SIGALRM)
        with mock.patch.object(job, "budget", return_value=0.2), mock.patch.dict(job.HANDLERS, {"guard": lambda context: time.sleep(5)}):
            code, _, error = call(arguments_for("guard", root))
        require(code == job.EXIT_DEADLINE and failure_of(error) == "deadline", f"{code} {error}")
        require(signal.getitimer(signal.ITIMER_REAL)[0] == 0 and signal.getsignal(signal.SIGALRM) == previous, "the timer or handler was left behind")

    cases.check("job_deadline_named", expired)


def smoke_cases(cases: Cases, folder: Path) -> None:
    """Run the rendered smoke command as a child process from a snapshot-like tree whose action modules are stand-ins."""
    names = sorted({name for modules in job.MODULES.values() for name in modules})

    def tree(name: str) -> Path:
        root = folder / name
        for package in ("scripts", "scripts/training", "scripts/training/models", "orchestration"):
            (root / package).mkdir(parents=True, exist_ok=True)
            (root / package / "__init__.py").write_text("")
        for source in (job.__file__, job_failure.__file__):
            (root / "scripts/training" / Path(source).name).write_bytes(Path(source).read_bytes())
        for module in names:
            (root / (module.replace(".", "/") + ".py")).write_text("VALUE = 1\n")
        return root

    def smoke(root: Path) -> tuple[int, str, str]:
        environment = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"} | {"PYTHONDONTWRITEBYTECODE": "1"}
        result = run_command(sys.executable, ["-m", "scripts.training.job", "smoke"], cwd=root, env=environment, timeout=60)
        return result.returncode, result.stdout, result.stderr

    def mounted() -> None:
        code, out, error = smoke(tree("mount"))
        require(code == 0, f"smoke failed: {error}")
        require(sorted(json.loads(out)["modules"]) == names, "smoke did not import every action module")

    def missing() -> None:
        root = tree("missing")
        (root / "scripts/training/mediation.py").unlink()
        code, _, error = smoke(root)
        require(code == job.EXIT_FAILED and failure_of(error) == "import_failed" and "scripts.training.mediation" in error, f"{code} {error}")

    def outside() -> None:
        root = tree("outside")
        elsewhere = folder / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / "mediation.py").write_text("VALUE = 1\n")
        (root / "scripts/training/mediation.py").unlink()
        (root / "scripts/training/__init__.py").write_text(f"__path__.append({str(elsewhere)!r})\n")
        code, _, error = smoke(root)
        require(code == job.EXIT_FAILED and failure_of(error) == "import_outside_snapshot" and "scripts.training.mediation" in error, f"{code} {error}")

    cases.check("job_smoke_from_snapshot_mount", mounted)
    cases.check("job_smoke_missing_module", missing)
    cases.check("job_smoke_outside_root", outside)


def run() -> list[dict[str, Any]]:
    """Run the job module cases without a launcher, an image or a fit."""
    cases = Cases([])
    with tempfile.TemporaryDirectory(prefix="hai_training_job_") as temporary:
        folder = Path(temporary)
        dispatch_cases(cases, folder / "dispatch")
        argument_cases(cases, folder / "arguments")
        guard_cases(cases, folder / "guard")
        deadline_case(cases, folder / "deadline")
        smoke_cases(cases, folder / "smoke")
    return cases.results
