"""The data actions of the training job: fit, mediation and HAC validation. Each writes write-once outputs with a completion record."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

from scripts.training import dvc_baseline, job_inputs, run_tracking, step_records, targets
from scripts.training.job_failure import EXIT_DEADLINE, JobFailure
from scripts.training.job_inputs import InputError

HAC_SECONDS = 600.0
HAC_MAPPING = "config/training/hac_year_mapping.json"
DVC_CONFIG = "[core]\n    analytics = false\n    autostage = false\n    no_scm = true\n"
DVC_DEADLINES = dvc_baseline.Deadlines(add=240, push=480)
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
    target = folder / name
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(f".{target.name}.partial")
    with partial.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, target)


def run_step(context: Any, record: Mapping[str, Any], step: str, allowed: Callable[[str], bool], plan: Plan) -> None:
    """Resume a completed step unchanged, or run the plan, write its outputs and the completion record, and print the outputs."""
    folder, digest = context.run_dir, str(record["code_digest"])
    if not folder.is_dir():
        raise JobFailure("run_dir", "the run folder does not exist")
    try:
        completed = (folder / f"{step}.done.json").exists()
        done = step_records.verify_done(folder, step, digest) if completed else {}
        if not completed:
            for leftover in folder.rglob(".*.partial"):
                leftover.unlink()
            present = {path.relative_to(folder).as_posix() for path in folder.rglob("*") if path.is_file()}
            foreign = sorted(name for name in present if not allowed(name))
            if foreign:
                raise JobFailure("run_dir_not_clean", f"the run folder holds files this step does not write: {foreign[0]}")
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


def fit_type(context: Any, record: Mapping[str, Any], config: Mapping[str, Any], hai_type: str) -> dict[str, bytes]:
    """Fit the six-model suite on one type's verified extract and return the files of its bundle."""
    from scripts.training import bundle
    from scripts.training.models import suite

    started = time.monotonic()
    extract = job_inputs.input_root(context.parameters) / "extract"
    manifest, rows = job_inputs.extract_of(extract, hai_type, context.run)
    adjacency = job_inputs.auxiliary_of(extract, manifest, job_inputs.ADJACENCY)
    request = job_inputs.model_request(hai_type, rows, adjacency, config, float(config["budgets"]["fit_seconds"]), job_inputs.ROOT)
    contract = job_inputs.read_json(job_inputs.ROOT / "data_contracts/training/model_contract.json", "model_contract")
    inputs = {
        "hashes": {"extract_sha256": manifest["sha256"], "manifest_sha256": step_records.file_sha256(extract / f"manifest_{hai_type}.json")},
        "records": {
            "snapshot_set": manifest["snapshot_set"],
            "auxiliary_files": {
                name: {"sha256": entry["sha256"], "row_count": entry["row_count"]} for name, entry in sorted(manifest["auxiliary_files"].items())
            },
        },
    }
    return bundle.build(suite.fit_models(request), hai_type, inputs, record, context.run, contract, job_inputs.ROOT, started)


def fit_allowed(hai_types: list[str]) -> Callable[[str], bool]:
    """Return the test for the names a fit may write: any file under a requested type's bundle folder."""
    from scripts.training import bundle

    prefixes = tuple(bundle.prefix(hai_type) + "/" for hai_type in hai_types)
    return lambda name: name.startswith(prefixes)


def run_fit(context: Any, record: Mapping[str, Any]) -> None:
    """Fit the six-model suite for every requested type from the mounted extract, after the target-list check."""
    with guarded("fit"):
        parameters = job_inputs.parameters_of(context.parameters)

        def plan() -> dict[str, bytes]:
            check_targets(job_inputs.ROOT)
            config = job_inputs.config_of(job_inputs.ROOT)
            outputs: dict[str, bytes] = {}
            for hai_type in parameters["hai_types"]:
                outputs |= fit_type(context, record, config, hai_type)
            return outputs

        run_step(context, record, "fit", fit_allowed(parameters["hai_types"]), plan)


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

        run_step(context, record, "mediation", names.__contains__, plan)


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
    from scripts.training import bundle, hac_validation
    from scripts.training.models.common import Deadline

    inputs = job_inputs.input_root(context.parameters)
    folder = verified_step(inputs, "fit", record) / bundle.prefix(hai_type)
    fitted = job_inputs.read_json(folder / "result.json", "fit_changed")
    chosen = fitted["selection"]["selected"]
    table = job_inputs.read_rows(folder / "holdout_predictions.parquet") if chosen is not None else []
    predictions = [{"spine_key": row["spine_key"], "predicted": row[f"predicted_{chosen}"]} for row in table if row.get(f"predicted_{chosen}") is not None]
    recorded = job_inputs.read_json(folder / "provenance.json", "fit_changed")["inputs"]["auxiliary_files"]["hac.parquet"]
    flags = job_inputs.table_of(inputs / "extract", {"file": "hac.parquet", **recorded})
    result = hac_validation.validate_hac(predictions, flags, mapping, Deadline.after(HAC_SECONDS))
    return {f"hac_{hai_type}.json": text({"hai_type": hai_type, "model": chosen, **result})}


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

        run_step(context, record, "hac_validate", names.__contains__, plan)


def verified_step(inputs: Path, step: str, record: Mapping[str, Any]) -> Path:
    """Require a mounted earlier step to match its completion record and return its folder."""
    try:
        step_records.verify_done(inputs / step, step, str(record["code_digest"]))
    except (ValueError, OSError) as error:
        raise JobFailure(f"{step}_changed", f"the mounted {step} outputs differ from their completion record: {error}") from None
    return inputs / step


def bundle_files(folder: Path, hai_type: str) -> Path:
    """Return the mounted bundle folder of a type."""
    from scripts.training import bundle

    return folder / bundle.prefix(hai_type)


def run_tags(
    context: Any, record: Mapping[str, Any], config: Mapping[str, Any], parameters: Mapping[str, Any], manifest: Mapping[str, Any], digests: Mapping[str, str]
) -> dict[str, str]:
    """Return the required run tags of the model contract, all but the completion mark, which the logger writes last."""
    from scripts.training import guards

    hai_type = manifest["hai_type"]
    try:
        guard = guards.check(job_inputs.ROOT, git_commit=str(record["git_commit"]))
    except guards.GuardError as error:
        raise JobFailure("guard_refused", str(error)) from None
    listed = job_inputs.read_json(job_inputs.ROOT / "config/training/targets.json", "targets_refused")["target_candidates"]
    image = os.environ.get("TRAINING_IMAGE_DIGEST", "unrecorded")
    key = {
        "analysis_plan_sha256": guard["analysis_plan_sha256"],
        "targets_sha256": guard["targets_sha256"],
        "training_config_sha256": guard["training_config_sha256"],
        "code_digest": str(record["code_digest"]),
        "features_run_id": manifest["publication_run_id"],
        "features_snapshot_set_sha256": manifest["features_snapshot_set_sha256"],
        "feature_schema_sha256": manifest["feature_schema_sha256"],
        "training_image_digest": image,
        "master_seed": config["master_seed"],
        "population": manifest["population"],
        "hai_types": parameters["hai_types"],
    }
    return {
        "run_id": context.run,
        "step_key": run_tracking.PARENT_STEP,
        "hai_type": hai_type,
        "target_control": next(entry["control"] for entry in listed if entry["hai_type"] == hai_type),
        "population": manifest["population"],
        "analysis_plan_sha256": guard["analysis_plan_sha256"],
        "targets_sha256": guard["targets_sha256"],
        "training_config_sha256": guard["training_config_sha256"],
        "code_digest": str(record["code_digest"]),
        "git_commit": str(record["git_commit"]),
        "git_dirty": "true" if record.get("git_dirty") else "false",
        "image_digest": image,
        "master_seed": str(config["master_seed"]),
        "features_run_id": manifest["publication_run_id"],
        "features_snapshot_set_sha256": manifest["features_snapshot_set_sha256"],
        "feature_schema_sha256": manifest["feature_schema_sha256"],
        "gold_run_id": manifest["gold_run_id"],
        "dvc_md5": digests["dvc_md5"],
        "extract_sha256": manifest["sha256"],
        "manifest_sha256": digests["manifest_sha256"],
        "newest_window": str(manifest["newest_window_year"]),
        "feature_list_sha256": manifest["feature_list_sha256"],
        "training_key": hashlib.sha256(text(key)).hexdigest(),
        "reproducibility_sha256": digests["reproducibility_sha256"],
    }


def child_spec(
    context: Any, manifest: Mapping[str, Any], step: str, result: Mapping[str, Any], artifacts: list[tuple[Path, str]] | None = None
) -> run_tracking.RunSpec:
    """Describe one child run: a model slot, a research question or a validation, with its status and every finite number of its result."""
    tags = {"run_id": context.run, "step_key": step, "hai_type": manifest["hai_type"], "population": manifest["population"]}
    params = {"status": str(result.get("status", "fitted"))} | ({"reason": str(result["reason"])} if result.get("reason") else {})
    return run_tracking.RunSpec(
        step, tags, params, run_tracking.numbers(step, {key: value for key, value in result.items() if key != "details"}), artifacts or ()
    )


RESEARCH = {
    "rq1_joint": ("joint_process", "process_vs_intercept_cmi"),
    "rq4_nested": ("nested",),
    "rq5_moran": ("spatial",),
    "rq6_hausman": ("hospital_effects",),
}


def run_plan(context: Any, record: Mapping[str, Any], config: Mapping[str, Any], parameters: Mapping[str, Any], hai_type: str) -> run_tracking.TypePlan:
    """Check every mounted output of one type against its completion records and the extract it came from, then describe its MLflow runs."""
    from scripts.training import bundle
    from scripts.training.models.common import SLOTS

    inputs = job_inputs.input_root(context.parameters)
    manifest = job_inputs.manifest_of(inputs / "extract", hai_type, context.run)
    job_inputs.table_of(inputs / "extract", manifest, read=False)
    folders = {step: verified_step(inputs, step, record) for step in ("version_extract", "fit", "mediation", "hac_validate")}
    base = bundle_files(folders["fit"], hai_type)
    result = job_inputs.read_json(base / "result.json", "fit_changed")
    if result["input_hashes"]["extract_sha256"] != manifest["sha256"]:
        raise JobFailure("fit_inputs_differ", f"the fit of {hai_type} read another extract than the one mounted")
    baseline = job_inputs.read_json(folders["version_extract"] / f"baseline_{hai_type}.json", "version_extract_changed")
    metafile = folders["version_extract"] / baseline["metafile"]
    mediation_path, hac_path = folders["mediation"] / f"mediation_{hai_type}.json", folders["hac_validate"] / f"hac_{hai_type}.json"
    mediation_result, hac_result = job_inputs.read_json(mediation_path, "mediation_changed"), job_inputs.read_json(hac_path, "hac_validate_changed")
    done = step_records.verify_done(folders["fit"], "fit", str(record["code_digest"]))
    manifest_hash = step_records.file_sha256(inputs / "extract" / f"manifest_{hai_type}.json")
    digests = {
        "dvc_md5": dvc_baseline.metafile_md5(metafile),
        "manifest_sha256": manifest_hash,
        "reproducibility_sha256": step_records.file_sha256(base / "result.json"),
    }
    bundle_hash = bundle.sha256_of(done["outputs"], hai_type)
    chosen, candidate = result["selection"]["selected"], result["selection"]["registry_candidate"]
    params = {
        "hai_type": hai_type,
        "population": manifest["population"],
        "features_run": parameters["features_run"],
        "trigger": parameters["trigger"],
        "selected_model": chosen or "none",
        "registry_candidate": candidate or "none",
        **{f"seed.{purpose}": seed for purpose, seed in result["seeds"].items()},
    }
    metrics: dict[str, float] = run_tracking.numbers("counts", result["counts"]) | run_tracking.numbers(
        "comparison_rmse", result["selection"]["comparison_rmse"]
    )
    metrics |= run_tracking.numbers("comparison_holdout_rows", result["selection"]["comparison_holdout_rows"])
    parent_files = [(base / name, "bundle") for name in ("result.json", "provenance.json", "holdout_predictions.parquet", "feature_list.json", "shap.json")]
    parent_files.append((inputs / "extract" / f"manifest_{hai_type}.json", "extract"))
    model = base / "model"
    tags = run_tags(context, record, config, parameters, manifest, digests)
    parent = run_tracking.RunSpec(
        run_tracking.PARENT_STEP, tags, {key: str(value) for key, value in params.items()}, metrics, parent_files, [(model, "model")] if model.is_dir() else []
    )
    children = []
    for slot in SLOTS:
        details = {key: value for key, value in result["fitted_parameters"][slot].items() if isinstance(value, int | float) and not isinstance(value, bool)}
        slot_result = {**result["statuses"][slot], "metrics": result["holdout_metrics"][slot], "details": details, **details}
        spec = child_spec(context, manifest, slot, slot_result)
        extra = {"selected_parameters": json.dumps(result["fitted_parameters"][slot].get("selected_parameters", {}), sort_keys=True)}
        children.append(run_tracking.RunSpec(spec.step_key, spec.tags, {**spec.params, **extra}, spec.metrics))
    for step, names in RESEARCH.items():
        merged = {name: result["test_statistics"].get(name, {}) for name in names}
        statuses = {str(item.get("status", "")) for item in merged.values() if isinstance(item, Mapping)}
        children.append(child_spec(context, manifest, step, {"status": ",".join(sorted(statuses)) or "none", **merged}))
    children.append(child_spec(context, manifest, "rq3_mediation", {"status": "fitted", **mediation_result["mediators"]}, [(mediation_path, "result")]))
    children.append(child_spec(context, manifest, "hac_validation", hac_result, [(hac_path, "result")]))
    snapshots = {table: str(snapshot) for table, snapshot in manifest["snapshot_set"].items()}
    experiment = job_inputs.read_json(job_inputs.ROOT / "data_contracts/training/model_contract.json", "model_contract")["experiments"][hai_type]
    return run_tracking.TypePlan(hai_type, experiment, bundle_hash, parent, children, metafile, Path(baseline["matrix_path"]), snapshots)


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
            return {"log_run.json": text({"runs": runs})}

        run_step(context, record, "log_run", {"log_run.json"}.__contains__, plan)


def version_names(hai_types: list[str]) -> set[str]:
    """Return every output name version_extract writes for the run's types."""
    return {name for hai_type in hai_types for name in (f"baseline_{hai_type}.json", f"extract_{hai_type}.parquet.dvc")}


def run_version_extract(context: Any, record: Mapping[str, Any]) -> None:
    """Version each exported matrix with DVC in a scratch project, push it to the run's remote and record its metafile and path."""
    with guarded("version_extract"):
        parameters = job_inputs.parameters_of(context.parameters)

        def plan() -> dict[str, bytes]:
            remote = os.environ.get("DVC_REMOTE_URL", "")
            if not remote:
                raise JobFailure("dvc_remote_missing", "DVC_REMOTE_URL is not set")
            extract = job_inputs.input_root(context.parameters) / "extract"
            manifests = {hai_type: job_inputs.manifest_of(extract, hai_type, context.run) for hai_type in parameters["hai_types"]}
            for manifest in manifests.values():
                job_inputs.table_of(extract, manifest, read=False)
            outputs: dict[str, bytes] = {}
            with tempfile.TemporaryDirectory(prefix="dvc_version_") as scratch:
                project = Path(scratch)
                try:
                    (project / ".dvc").mkdir()
                    (project / ".dvc/config").write_text(DVC_CONFIG)
                    dvc_baseline.run_dvc(["remote", "add", "--local", "--default", "store", remote], project, 120)
                    for hai_type, manifest in manifests.items():
                        matrix = project / "data/training" / context.run / manifest["file"]
                        matrix.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(extract / manifest["file"], matrix)
                        relative, metafile = dvc_baseline.add_and_push(matrix, project=project, deadlines=DVC_DEADLINES)
                        content = metafile.read_bytes()
                        outputs[metafile.name] = content
                        baseline = {
                            "hai_type": hai_type,
                            "matrix_path": relative.as_posix(),
                            "metafile": metafile.name,
                            "metafile_sha256": hashlib.sha256(content).hexdigest(),
                        }
                        outputs[f"baseline_{hai_type}.json"] = text(baseline)
                except dvc_baseline.BaselineError as error:
                    raise JobFailure("dvc_failed", str(error)) from None
            return outputs

        run_step(context, record, "version_extract", version_names(parameters["hai_types"]).__contains__, plan)
