"""Run one fixed training action from the mounted snapshot: the entry point of every training job container."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import FrameType, ModuleType
from typing import Any

from scripts.training.job_failure import EXIT_DEADLINE, EXIT_NOT_IMPLEMENTED, JobFailure
from scripts.training.job_failure import EXIT_FAILED as EXIT_FAILED

ROOT = Path(__file__).resolve().parents[2]
ACTIONS = ("guard", "version_extract", "fit", "mediation", "hac_validate", "log_run", "promotion_check", "promote", "smoke")
MODULES: dict[str, tuple[str, ...]] = {
    "guard": ("scripts.training.guards", "scripts.training.targets", "scripts.training.step_records"),
    "version_extract": ("scripts.training.dvc_baseline", "scripts.training.step_records"),
    "fit": ("scripts.training.models.suite", "scripts.training.job_actions", "scripts.training.job_inputs"),
    "mediation": ("scripts.training.mediation", "scripts.training.job_actions", "scripts.training.job_inputs"),
    "hac_validate": ("scripts.training.hac_validation", "scripts.training.job_actions", "scripts.training.job_inputs"),
    "log_run": ("scripts.training.run_tracking", "scripts.training.dvc_baseline", "scripts.training.job_actions", "scripts.training.job_inputs"),
    "promotion_check": (),
    "promote": (),
    "smoke": (),
}
SNAPSHOT_PACKAGES = ("scripts", "orchestration", "feature_store")
STATIC_SECONDS = {"guard": 300.0, "smoke": 120.0}
REQUIRED = ("run", "mode", "run_dir", "parameters", "snapshot_record")
OPTIONS = {name: "--" + name.replace("_", "-") for name in (*REQUIRED, "approval")}


@dataclass(frozen=True)
class Context:
    """The validated command line of one action."""

    action: str
    run: str
    mode: str
    run_dir: Path
    parameters: Path
    snapshot_record: Path
    approval: Path | None


def parser() -> argparse.ArgumentParser:
    """Describe the rendered command line: the action and the options the training renderer always passes."""
    result = argparse.ArgumentParser(prog="python -m scripts.training.job", description=__doc__)
    result.add_argument("action", choices=ACTIONS)
    result.add_argument(OPTIONS["run"])
    result.add_argument(OPTIONS["mode"], choices=("fixture", "real"))
    for name in ("run_dir", "parameters", "snapshot_record", "approval"):
        result.add_argument(OPTIONS[name], type=Path)
    return result


def context_of(values: argparse.Namespace) -> Context:
    """Require every run option for an action that writes outputs."""
    missing = [OPTIONS[name] for name in REQUIRED if getattr(values, name) is None]
    if missing:
        raise JobFailure("missing_argument", f"{values.action} needs {', '.join(missing)}")
    return Context(values.action, values.run, values.mode, values.run_dir, values.parameters, values.snapshot_record, values.approval)


def read_record(context: Context) -> dict[str, Any]:
    """Read the launcher-verified snapshot record and require the run, the code digest and the Git commit this job records."""
    try:
        record = json.loads(context.snapshot_record.read_bytes())
    except (OSError, ValueError) as error:
        raise JobFailure("snapshot_record", f"cannot read the snapshot record: {error}") from None
    if not isinstance(record, dict) or record.get("schema_version") != 1:
        raise JobFailure("snapshot_record", "the snapshot record has another schema")
    if not re.fullmatch(r"[a-f0-9]{64}", str(record.get("code_digest", ""))) or not re.fullmatch(r"[a-f0-9]{40}", str(record.get("git_commit", ""))):
        raise JobFailure("snapshot_record", "the snapshot record has no valid code digest or Git commit")
    if record.get("run_id") != context.run:
        raise JobFailure("run_mismatch", "the snapshot record names another run")
    return record


def run_guard(context: Context) -> None:
    """Validate the static snapshot inputs before any data step and write guard.json with its completion record."""
    from scripts.training import guards, step_records

    record = read_record(context)
    if not context.run_dir.is_dir():
        raise JobFailure("run_dir", "the run folder does not exist")
    try:
        result = guards.check(ROOT, git_commit=str(record["git_commit"]))
    except guards.GuardError as error:
        raise JobFailure("guard_refused", str(error)) from None
    text = json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n"
    target = context.run_dir / "guard.json"
    try:
        if (context.run_dir / "guard.done.json").exists():
            step_records.verify_done(context.run_dir, "guard", str(record["code_digest"]))
            if target.read_text(encoding="utf-8") != text:
                raise ValueError("a completed guard cannot be replaced by another record")
        else:
            partial = context.run_dir / ".guard.json.partial"
            with partial.open("w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(partial, target)
            step_records.write_done(context.run_dir, "guard", str(record["code_digest"]), ["guard.json"])
    except ValueError as error:
        raise JobFailure("done_conflict", str(error)) from None
    except OSError as error:
        raise JobFailure("run_dir", str(error)) from None
    sys.stdout.write(text)


def action_modules() -> tuple[ModuleType, ...]:
    """Import every action module by a static statement, so the mount check and the image smoke see the same imports."""
    import scripts.training.dvc_baseline as dvc_baseline
    import scripts.training.guards as guards
    import scripts.training.hac_validation as hac_validation
    import scripts.training.job_actions as job_actions
    import scripts.training.job_inputs as job_inputs
    import scripts.training.mediation as mediation
    import scripts.training.models.suite as suite
    import scripts.training.run_tracking as run_tracking
    import scripts.training.step_records as step_records
    import scripts.training.targets as targets

    return (guards, targets, step_records, dvc_baseline, suite, mediation, hac_validation, job_actions, job_inputs, run_tracking)


def run_smoke(context: Context) -> None:
    """Import every action module from the snapshot and require each repository module to come from this tree."""
    try:
        modules = action_modules()
    except ImportError as error:
        raise JobFailure("import_failed", f"{error.name}: {error}") from None
    names = [module.__name__ for module in modules]
    for module in modules:
        location = Path(str(getattr(module, "__file__", ""))).resolve()
        if module.__name__.split(".")[0] in SNAPSHOT_PACKAGES and ROOT not in location.parents:
            raise JobFailure("import_outside_snapshot", f"{module.__name__} resolves to {location}")
    sys.stdout.write(json.dumps({"action": "smoke", "modules": names, "unimplemented": [name for name in ACTIONS if name not in HANDLERS]}) + "\n")


def run_fit(context: Context) -> None:
    """Fit the six-model suite for every requested HAI type from the mounted extract."""
    from scripts.training import job_actions

    job_actions.run_fit(context, read_record(context))


def run_mediation(context: Context) -> None:
    """Estimate the PSI-90 and PSI-13 mediation for every requested HAI type."""
    from scripts.training import job_actions

    job_actions.run_mediation(context, read_record(context))


def run_hac_validate(context: Context) -> None:
    """Rank each fitted holdout against the HAC Reduction Program flags."""
    from scripts.training import job_actions

    job_actions.run_hac_validate(context, read_record(context))


def run_log_run(context: Context) -> None:
    """Log one MLflow run per requested HAI type from the mounted outputs of the earlier steps."""
    from scripts.training import job_actions

    job_actions.run_log_run(context, read_record(context))


HANDLERS: dict[str, Callable[[Context], None]] = {
    "guard": run_guard,
    "fit": run_fit,
    "mediation": run_mediation,
    "hac_validate": run_hac_validate,
    "log_run": run_log_run,
    "smoke": run_smoke,
}


def budget(action: str) -> float:
    """Return the action's absolute budget in seconds."""
    if action in STATIC_SECONDS:
        return STATIC_SECONDS[action]
    from orchestration import training_contract

    return float(training_contract.ACTION_SECONDS[action])


def expire(signum: int, frame: FrameType | None) -> None:
    """Turn the timer signal into the named deadline failure."""
    raise JobFailure("deadline", "the action exceeded its budget", EXIT_DEADLINE)


def execute(context: Context) -> None:
    """Dispatch the action under its deadline, restoring the timer and signal handler on every exit."""
    handler = HANDLERS.get(context.action)
    if handler is None:
        raise JobFailure("action_not_implemented", f"{context.action} has no implementation in this snapshot", EXIT_NOT_IMPLEMENTED)
    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, budget(context.action))
    try:
        handler(context)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one action and report a failure as one JSON line on standard error."""
    values = parser().parse_args(argv)
    try:
        if values.action == "smoke":
            context = Context("smoke", values.run or "", values.mode or "", Path("."), Path("."), Path("."), None)
        else:
            context = context_of(values)
        execute(context)
    except JobFailure as failure:
        sys.stderr.write(json.dumps({"action": values.action, "failure": failure.name, "detail": failure.detail}, sort_keys=True) + "\n")
        return failure.code
    return 0


if __name__ == "__main__":
    sys.exit(main())
