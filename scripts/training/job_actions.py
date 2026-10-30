"""The data actions of the training job: fit, mediation and HAC validation. Each writes write-once outputs with a completion record."""

from __future__ import annotations

import contextlib
import json
import os
import pickle
import sys
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

from scripts.training import job_inputs, step_records, targets
from scripts.training.job_failure import EXIT_DEADLINE, JobFailure
from scripts.training.job_inputs import InputError

HAC_SECONDS = 600.0
HAC_MAPPING = "config/training/hac_year_mapping.json"
Plan = Callable[[], dict[str, bytes]]


def text(document: Mapping[str, Any]) -> bytes:
    """Return the canonical bytes of a JSON output."""
    return (json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


@contextlib.contextmanager
def guarded(action: str) -> Iterator[None]:
    """Turn an input, model or dependency failure into the action's named failure."""
    from scripts.training.models.common import DeadlineExceeded, ModelError

    try:
        yield
    except InputError as error:
        raise JobFailure(error.name, error.detail) from None
    except DeadlineExceeded:
        raise JobFailure("deadline", f"{action} used its model budget", EXIT_DEADLINE) from None
    except ModelError as error:
        raise JobFailure(f"{action}_refused", str(error)) from None
    except ImportError as error:
        raise JobFailure("dependency_missing", f"{error.name}: {error}") from None


def write_atomic(folder: Path, name: str, data: bytes) -> None:
    """Write one output beside its final name, flush it and rename it, so a crash never leaves a half file under the final name."""
    partial = folder / f".{name}.partial"
    with partial.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, folder / name)


def run_step(context: Any, record: Mapping[str, Any], step: str, allowed: set[str], plan: Plan) -> None:
    """Resume a completed step unchanged, or run the plan, write its outputs and the completion record, and print the outputs."""
    folder, digest = context.run_dir, str(record["code_digest"])
    if not folder.is_dir():
        raise JobFailure("run_dir", "the run folder does not exist")
    try:
        completed = (folder / f"{step}.done.json").exists()
        done = step_records.verify_done(folder, step, digest) if completed else {}
        if not completed:
            for leftover in folder.glob(".*.partial"):
                leftover.unlink()
            present = {path.relative_to(folder).as_posix() for path in folder.rglob("*") if path.is_file()}
            if present - allowed:
                raise JobFailure("run_dir_not_clean", f"the run folder holds files this step does not write: {sorted(present - allowed)[0]}")
            for stale in present:
                (folder / stale).unlink()
    except ValueError as error:
        raise JobFailure("done_conflict", str(error)) from None
    except OSError as error:
        raise JobFailure("run_dir", str(error)) from None
    if completed:
        summary = {"action": step, "resumed": True, "outputs": sorted(done["outputs"])}
    else:
        outputs = plan()
        try:
            for name, data in outputs.items():
                write_atomic(folder, name, data)
            step_records.write_done(folder, step, digest, outputs)
        except ValueError as error:
            raise JobFailure("done_conflict", str(error)) from None
        except OSError as error:
            raise JobFailure("run_dir", str(error)) from None
        summary = {"action": step, "resumed": False, "outputs": sorted(outputs)}
    sys.stdout.write(json.dumps(summary, sort_keys=True) + "\n")


def check_targets(root: Path) -> None:
    """Check the target and earlier-outcome lists against the gold control bridge, as the guard does (project decision 1)."""
    try:
        problems = targets.check(root / "config/training/targets.json", root / "dbt/seeds/gold/hai_outcome_measures_bridge.csv")
    except OSError as error:
        raise JobFailure("targets_refused", f"cannot read the target lists or the bridge: {type(error).__name__}") from None
    if problems:
        raise JobFailure("targets_refused", "; ".join(problems))


def fit_type(context: Any, config: Mapping[str, Any], hai_type: str, parameters: Mapping[str, Any]) -> dict[str, bytes]:
    """Fit the six-model suite on one type's verified extract and return its outputs."""
    from scripts.training.models import suite

    extract = job_inputs.input_root(context.parameters) / "extract"
    manifest, rows = job_inputs.extract_of(extract, hai_type, context.run)
    adjacency = job_inputs.auxiliary_of(extract, manifest, job_inputs.ADJACENCY)
    request = job_inputs.model_request(hai_type, rows, adjacency, config, float(config["budgets"]["fit_seconds"]), job_inputs.ROOT)
    result = suite.fit_models(request)
    payload = result.to_result()
    auxiliary = manifest["auxiliary_files"]
    payload["hai_type"] = hai_type
    payload["inputs"] = {
        "extract_sha256": manifest["sha256"],
        "snapshot_set": manifest["snapshot_set"],
        "auxiliary_sha256": {name: entry["sha256"] for name, entry in sorted(auxiliary.items())},
        "auxiliary_rows": {name: entry["row_count"] for name, entry in sorted(auxiliary.items())},
    }
    _, holdout = request.data.split()
    chosen = result.selected
    predictions: list[dict[str, Any]] = []
    if chosen is not None:
        slot = result.slots[chosen]
        scored = sorted((holdout.keys[position], value) for position, value in zip(slot.holdout_positions, slot.predictions, strict=True))
        predictions = [{"spine_key": key, "predicted": value} for key, value in scored]
    outputs = {
        f"fit_{hai_type}.json": text(payload),
        f"holdout_{hai_type}.json": text({"hai_type": hai_type, "model": chosen, "predictions": predictions}),
    }
    if result.candidate is not None:
        outputs[f"model_{hai_type}.pkl"] = pickle.dumps(result.slots[result.candidate].pipeline, protocol=4)
    return outputs


def fit_names(hai_types: list[str]) -> set[str]:
    """Return every output name a fit may write for the run's types."""
    return {name for hai_type in hai_types for name in (f"fit_{hai_type}.json", f"holdout_{hai_type}.json", f"model_{hai_type}.pkl")}


def run_fit(context: Any, record: Mapping[str, Any]) -> None:
    """Fit the six-model suite for every requested type from the mounted extract, after the target-list check."""
    with guarded("fit"):
        parameters = job_inputs.parameters_of(context.parameters)
        names = fit_names(parameters["hai_types"])

        def plan() -> dict[str, bytes]:
            check_targets(job_inputs.ROOT)
            config = job_inputs.config_of(job_inputs.ROOT)
            outputs: dict[str, bytes] = {}
            for hai_type in parameters["hai_types"]:
                outputs |= fit_type(context, config, hai_type, parameters)
            return outputs

        run_step(context, record, "fit", names, plan)
