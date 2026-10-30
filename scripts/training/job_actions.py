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

from scripts.training import job_inputs, run_tracking, step_records, targets
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


def mediation_type(context: Any, config: Mapping[str, Any], hai_type: str) -> dict[str, bytes]:
    """Estimate the PSI-90 and PSI-13 mediation of one type from its verified extract and mediator table."""
    from scripts.training import mediation

    extract = job_inputs.input_root(context.parameters) / "extract"
    manifest, rows = job_inputs.extract_of(extract, hai_type, context.run)
    mediators = job_inputs.auxiliary_of(extract, manifest, "mediator.parquet")
    budget = 2 * float(config["budgets"]["bootstrap_seconds"])
    request = job_inputs.model_request(hai_type, rows, (), config, budget, job_inputs.ROOT)
    result = mediation.analyze_mediation(request.data, mediators, request.process_references, (request.cmi_reference,), config, request.deadline)
    return {f"mediation_{hai_type}.json": text(result)}


def run_mediation(context: Any, record: Mapping[str, Any]) -> None:
    """Run the separate associational mediation analyses for every requested type."""
    with guarded("mediation"):
        parameters = job_inputs.parameters_of(context.parameters)
        names = {f"mediation_{hai_type}.json" for hai_type in parameters["hai_types"]}

        def plan() -> dict[str, bytes]:
            config = job_inputs.config_of(job_inputs.ROOT)
            outputs: dict[str, bytes] = {}
            for hai_type in parameters["hai_types"]:
                outputs |= mediation_type(context, config, hai_type)
            return outputs

        run_step(context, record, "mediation", names, plan)


def year_mapping(root: Path) -> dict[int, int]:
    """Return the reviewed outcome-year to HAC program-year map committed with the code; no offset is inferred."""
    path = root / HAC_MAPPING
    if not path.is_file():
        raise InputError("hac_year_mapping_missing", f"{HAC_MAPPING} is not in the code snapshot; the reviewed year map must be committed")
    document = job_inputs.read_json(path, "hac_year_mapping")
    try:
        return {int(year): int(program) for year, program in document["mapping"].items()}
    except (KeyError, AttributeError, TypeError, ValueError):
        raise InputError("hac_year_mapping", f"{HAC_MAPPING} needs a mapping object of whole years") from None


def hac_type(context: Any, record: Mapping[str, Any], mapping: Mapping[int, int], hai_type: str) -> dict[str, bytes]:
    """Rank one type's selected-model holdout against its HAC flags; the flag table is checked against the hash the fit recorded."""
    from scripts.training import hac_validation
    from scripts.training.models.common import Deadline

    inputs = job_inputs.input_root(context.parameters)
    fit_folder = verified_step(inputs, "fit", record)
    fit = job_inputs.read_json(fit_folder / f"fit_{hai_type}.json", "fit_changed")
    holdout = job_inputs.read_json(fit_folder / f"holdout_{hai_type}.json", "fit_changed")
    entry = {"file": "hac.parquet", "sha256": fit["inputs"]["auxiliary_sha256"]["hac.parquet"], "row_count": fit["inputs"]["auxiliary_rows"]["hac.parquet"]}
    flags = job_inputs.table_of(inputs / "extract", entry)
    result = hac_validation.validate_hac(holdout["predictions"], flags, mapping, Deadline.after(HAC_SECONDS))
    return {f"hac_{hai_type}.json": text({"hai_type": hai_type, "model": holdout["model"], **result})}


def run_hac_validate(context: Any, record: Mapping[str, Any]) -> None:
    """Validate every requested type's fitted holdout against the HAC Reduction Program flags."""
    with guarded("hac_validate"):
        parameters = job_inputs.parameters_of(context.parameters)
        names = {f"hac_{hai_type}.json" for hai_type in parameters["hai_types"]}

        def plan() -> dict[str, bytes]:
            mapping = year_mapping(job_inputs.ROOT)
            outputs: dict[str, bytes] = {}
            for hai_type in parameters["hai_types"]:
                outputs |= hac_type(context, record, mapping, hai_type)
            return outputs

        run_step(context, record, "hac_validate", names, plan)


def verified_step(inputs: Path, step: str, record: Mapping[str, Any]) -> Path:
    """Require a mounted earlier step to match its completion record and return its folder."""
    try:
        step_records.verify_done(inputs / step, step, str(record["code_digest"]))
    except (ValueError, OSError) as error:
        raise JobFailure(f"{step}_changed", f"the mounted {step} outputs differ from their completion record: {error}") from None
    return inputs / step


def run_plan(context: Any, record: Mapping[str, Any], config: Mapping[str, Any], parameters: Mapping[str, Any], hai_type: str) -> run_tracking.RunPlan:
    """Check every mounted output of one type against its completion record and the extract it came from, then describe its MLflow run."""
    inputs = job_inputs.input_root(context.parameters)
    manifest = job_inputs.manifest_of(inputs / "extract", hai_type, context.run)
    job_inputs.table_of(inputs / "extract", manifest, read=False)
    folders = {step: verified_step(inputs, step, record) for step in ("version_extract", "fit", "mediation", "hac_validate")}
    fit = job_inputs.read_json(folders["fit"] / f"fit_{hai_type}.json", "fit_changed")
    if fit["inputs"]["extract_sha256"] != manifest["sha256"]:
        raise JobFailure("fit_inputs_differ", f"the fit of {hai_type} read another extract than the one mounted")
    baseline = job_inputs.read_json(folders["version_extract"] / f"baseline_{hai_type}.json", "version_extract_changed")
    metafile = folders["version_extract"] / baseline["metafile"]
    mediation_result = job_inputs.read_json(folders["mediation"] / f"mediation_{hai_type}.json", "mediation_changed")
    hac_result = job_inputs.read_json(folders["hac_validate"] / f"hac_{hai_type}.json", "hac_validate_changed")
    chosen, candidate = fit["selection"]["selected"], fit["selection"]["registry_candidate"]
    params = {
        "hai_type": hai_type,
        "run_id": context.run,
        "population": manifest["population"],
        "features_run": parameters["features_run"],
        "trigger": parameters["trigger"],
        "code_digest": str(record["code_digest"]),
        "git_commit": str(record["git_commit"]),
        "master_seed": config["master_seed"],
        "selected_model": chosen or "none",
        "registry_candidate": candidate or "none",
        "extract_sha256": manifest["sha256"],
        "feature_lists_sha256": fit["input_hashes"]["feature_lists_sha256"],
        **{f"seed.{purpose}": seed for purpose, seed in fit["seeds"].items()},
        **{
            f"selected_parameters.{slot}": json.dumps(details["selected_parameters"], sort_keys=True)
            for slot, details in fit["fitted_parameters"].items()
            if "selected_parameters" in details
        },
    }
    metrics: dict[str, float] = {}
    for slot, scores in fit["holdout_metrics"].items():
        for name, value in scores.items():
            metrics |= run_tracking.numbers(f"holdout_{name}.{slot}", value)
    metrics |= run_tracking.numbers("comparison_rmse", fit["selection"]["comparison_rmse"])
    metrics |= run_tracking.numbers("comparison_holdout_rows", fit["selection"]["comparison_holdout_rows"])
    metrics |= run_tracking.numbers("counts", fit["counts"])
    metrics |= run_tracking.numbers("hac", hac_result)
    metrics |= run_tracking.numbers("mediation", mediation_result["mediators"])
    artifacts = [(folders["fit"] / f"fit_{hai_type}.json", "fit"), (folders["fit"] / f"holdout_{hai_type}.json", "fit")]
    if (folders["fit"] / f"model_{hai_type}.pkl").is_file():
        artifacts.append((folders["fit"] / f"model_{hai_type}.pkl", "model"))
    artifacts += [(folders["mediation"] / f"mediation_{hai_type}.json", "mediation"), (folders["hac_validate"] / f"hac_{hai_type}.json", "hac")]
    tags = {
        run_tracking.RUN_TAG: context.run,
        run_tracking.TYPE_TAG: hai_type,
        "population": manifest["population"],
        "features_run": parameters["features_run"],
        "trigger": parameters["trigger"],
        "code_digest": str(record["code_digest"]),
        "git_commit": str(record["git_commit"]),
    }
    snapshots = {table: str(snapshot) for table, snapshot in manifest["snapshot_set"].items()}
    return run_tracking.RunPlan(
        hai_type, tags, {key: str(value) for key, value in params.items()}, metrics, artifacts, metafile, Path(baseline["matrix_path"]), snapshots
    )


def run_log_run(context: Any, record: Mapping[str, Any]) -> None:
    """Log one MLflow run per requested type to the tracking URI the job is given, after checking every mounted output."""
    with guarded("log_run"):
        parameters = job_inputs.parameters_of(context.parameters)

        def plan() -> dict[str, bytes]:
            uri = os.environ.get("MLFLOW_TRACKING_URI", "")
            if not uri:
                raise JobFailure("mlflow_uri_missing", "MLFLOW_TRACKING_URI is not set")
            config = job_inputs.config_of(job_inputs.ROOT)
            plans = [run_plan(context, record, config, parameters, hai_type) for hai_type in parameters["hai_types"]]
            try:
                runs = run_tracking.log_all(run_tracking.open_store(uri), plans, context.run)
            except (JobFailure, ImportError):
                raise
            except run_tracking.TrackingError as error:
                raise JobFailure(error.name, error.detail) from None
            except Exception as error:
                raise JobFailure("mlflow_failed", f"{type(error).__name__}: {error}") from None
            return {"log_run.json": text({"experiment": run_tracking.EXPERIMENT, "runs": runs})}

        run_step(context, record, "log_run", {"log_run.json"}, plan)
