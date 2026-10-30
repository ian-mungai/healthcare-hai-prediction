"""The promotion_check action: register each type's candidate, score it against the champion on the same holdout and record the verdict.

It never moves the ``production`` alias. A pass sets the alias ``challenger`` on the new version. Promotion to production is the ``promote`` action, which needs
the owner's approval record. The comparison files are written and checked before any version tag or alias. The check reads the written Parquet file back and
repeats the rule on it.
"""

from __future__ import annotations

import hashlib
import importlib
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np

from scripts.training import bundle, job_actions, job_inputs, promotion, registry, run_tracking, step_records
from scripts.training.job_failure import JobFailure
from scripts.training.models.common import Deadline, seed_for

PROMOTION_SECONDS = 600.0


def load_pipeline(uri: str) -> Any:
    """Load a saved scikit-learn pipeline from a models URI or a folder with the training image's MLflow."""
    return importlib.import_module("mlflow.sklearn").load_model(uri)


def predict(pipeline: Any, rows: list[dict[str, Any]]) -> dict[str, float]:
    """Score holdout rows with one pipeline and return the prediction of each hospital-window."""
    values = np.asarray(pipeline.predict(rows), dtype=np.float64).reshape(-1)
    if len(values) != len(rows):
        raise JobFailure("promotion_refused", "a pipeline returned another number of predictions than holdout rows")
    return {row["spine_key"]: float(value) for row, value in zip(rows, values, strict=True)}


def names_for(hai_type: str) -> tuple[str, str]:
    """Return the comparison Parquet and JSON file names of a type."""
    return f"comparison_{hai_type}.parquet", f"comparison_{hai_type}.json"


def champion_predictions(
    store: Any, version: Any, name: str, manifest: Mapping[str, Any], rows: list[dict[str, Any]], year: int, scratch: Path
) -> dict[str, float]:
    """Load the champion and score the holdout, after the checks that it needs only extract columns and never trained on the holdout year."""
    try:
        listing = store.download_artifacts(version.run_id, "bundle/feature_list.json", str(scratch))
        columns = set(job_inputs.read_json(Path(listing), "champion_features")["columns"])
    except JobFailure:
        raise
    except Exception as error:
        raise JobFailure("champion_features", f"the champion's feature list cannot be read: {type(error).__name__}: {error}") from None
    if not columns <= set(manifest["columns"]):
        raise JobFailure("champion_features", f"the champion needs columns the extract lacks: {sorted(columns - set(manifest['columns']))[0]}")
    window = store.get_run(version.run_id).data.tags.get("newest_window", "")
    if not window.isdigit() or int(window) > year:
        raise JobFailure("holdout_leak", f"the champion's newest window is {window or 'unknown'}, so its training years may include the holdout year {year}")
    try:
        pipeline = load_pipeline(f"models:/{name}@{registry.CHAMPION_ALIAS}")
    except JobFailure:
        raise
    except Exception as error:
        raise JobFailure("champion_unloadable", f"{type(error).__name__}: {error}") from None
    return predict(pipeline, rows)


def promotion_type(context: Any, record: Mapping[str, Any], parameters: Mapping[str, Any], hai_type: str, store: Any, scratch: Path) -> dict[str, bytes]:
    """Run the whole check for one type and return its comparison files."""
    inputs = job_inputs.input_root(context.parameters)
    root = job_inputs.ROOT
    folders = {step: job_actions.verified_step(inputs, step, record) for step in ("version_extract", "fit", "log_run")}
    base = folders["fit"] / bundle.prefix(hai_type)
    result = job_inputs.read_json(base / "result.json", "fit_changed")
    contract = job_inputs.read_json(root / "data_contracts/training/model_contract.json", "model_contract")
    manifest, extract_rows = job_inputs.extract_of(inputs / "extract", hai_type, context.run)
    parent = job_inputs.read_json(folders["log_run"] / "log_run.json", "log_run_changed")["runs"][hai_type]
    candidate = result["selection"]["registry_candidate"]
    json_name = names_for(hai_type)[1]
    if candidate is None:
        return {json_name: job_actions.text({"schema": 1, "hai_type": hai_type, "verdict": {"verdict": "no_candidate", "failed": []}})}
    experiment = contract["experiments"][hai_type]
    found = store.get_experiment_by_name(experiment)
    if found is None or not str(found.artifact_location).startswith("mlflow-artifacts:/"):
        raise JobFailure("artifact_location", f"the experiment {experiment} must keep its artifacts on the proxied location mlflow-artifacts:/")
    done = step_records.verify_done(folders["fit"], "fit", str(record["code_digest"]))
    bundle_hash = bundle.sha256_of(done["outputs"], hai_type)
    run_tags = store.get_run(parent).data.tags
    if run_tags.get(run_tracking.COMPLETE_TAG) != bundle_hash:
        raise JobFailure("run_incomplete", f"the logged run {parent} does not carry the bundle hash of the mounted fit")
    name = contract["registered_models"][hai_type]
    version = registry.register(
        store,
        name,
        parent,
        f"{context.run}:{hai_type}:promotion",
        {key: run_tags[key] for key in ("hai_type", "target_control", "feature_schema_sha256", "feature_list_sha256")}
        | {"source_run_id": parent, "bundle_sha256": bundle_hash},
    )
    year = max(row["window_year"] for row in extract_rows)
    holdout = [row for row in extract_rows if row["window_year"] == year]
    challenger = predict(load_pipeline(str(base / "model")), holdout)
    current = registry.champion(store, name)
    rival = None if current is None else champion_predictions(store, current, name, manifest, holdout, year, scratch)
    rules = job_inputs.read_json(root / "config/training/promotion.json", "promotion_config")
    regions = job_inputs.read_json(root / "config/training/census_regions.json", "promotion_config")["states"]
    seed = seed_for(job_inputs.config_of(root)["master_seed"], hai_type, "promotion_bootstrap")
    rows = promotion.comparison_rows(holdout, manifest["label_column"], challenger, rival, rules, regions)
    verdict = promotion.evaluate(rows, rules, seed=seed, deadline=Deadline.after(PROMOTION_SECONDS))
    missing = [tag for tag in contract["required_run_tags"] if tag not in run_tags]
    provenance = {"passed": not missing and run_tags.get("git_dirty") != "true", "missing_tags": missing, "git_dirty": run_tags.get("git_dirty")}
    parquet_name = names_for(hai_type)[0]
    table = scratch / parquet_name
    bundle.write_table(table, rows, promotion.COLUMNS)
    comparison = {
        "schema": 1,
        "hai_type": hai_type,
        "registered_model": name,
        "champion_version": None if current is None else str(current.version),
        "challenger_version": str(version.version),
        "extract_sha256": manifest["sha256"],
        "manifest_sha256": step_records.file_sha256(inputs / "extract" / f"manifest_{hai_type}.json"),
        "holdout_year": year,
        "rows": len(rows),
        "comparison_parquet_sha256": step_records.file_sha256(table),
        "thresholds": {key: rules[key] for key in ("minimum_rmse_improvement", "maximum_group_rmse_increase", "minimum_group_size")},
        "bootstrap": {**rules["bootstrap"], "seed": seed},
        "census_regions_sha256": step_records.file_sha256(root / "config/training/census_regions.json"),
        "verdict": verdict,
        "provenance": provenance,
    }
    again = promotion.evaluate(job_inputs.read_rows(table), rules, seed=seed, deadline=Deadline.after(PROMOTION_SECONDS))
    if {key: again[key] for key in ("verdict", "conditions")} != {key: verdict[key] for key in ("verdict", "conditions")}:
        raise JobFailure("comparison_mismatch", "the rule gives another verdict on the written comparison file than in memory")
    document = job_actions.text(comparison)
    (scratch / json_name).write_bytes(document)
    failed = [*verdict["failed"], *([] if provenance["passed"] else ["provenance"])]
    label = "fail:" + ",".join(failed) if failed else verdict["verdict"]
    spec = run_tracking.RunSpec(
        "promotion",
        {"run_id": context.run, "step_key": "promotion", "hai_type": hai_type, "population": manifest["population"]},
        {"verdict": label, "registered_model": name, "challenger_version": str(version.version), "champion_version": str(comparison["champion_version"])},
        run_tracking.numbers("promotion", verdict["conditions"]),
        [(table, "comparison"), (scratch / json_name, "comparison")],
    )
    run_tracking.log_child(store, experiment, parent, spec, context.run, bundle_hash)
    store.set_model_version_tag(name, str(version.version), "promotion_verdict", label)
    store.set_model_version_tag(name, str(version.version), "comparison_sha256", hashlib.sha256(document).hexdigest())
    if label == "pass":
        store.set_registered_model_alias(name, registry.CHALLENGER_ALIAS, str(version.version))
    return {parquet_name: table.read_bytes(), json_name: document}


def run(context: Any, record: Mapping[str, Any]) -> None:
    """Check every requested type against the tracking URI the job is given; never change the production alias."""
    with job_actions.guarded("promotion_check"):
        parameters = job_inputs.parameters_of(context.parameters)
        allowed = {name for hai_type in parameters["hai_types"] for name in names_for(hai_type)}

        def plan() -> dict[str, bytes]:
            uri = os.environ.get("MLFLOW_TRACKING_URI", "")
            if not uri:
                raise JobFailure("mlflow_uri_missing", "MLFLOW_TRACKING_URI is not set")
            outputs: dict[str, bytes] = {}
            try:
                store = run_tracking.open_store(uri)
                for hai_type in parameters["hai_types"]:
                    with tempfile.TemporaryDirectory(prefix="promotion_") as scratch:
                        outputs |= promotion_type(context, record, parameters, hai_type, store, Path(scratch))
            except (JobFailure, ImportError):
                raise
            except job_inputs.InputError as error:
                raise JobFailure(error.name, error.detail) from None
            except run_tracking.TrackingError as error:
                raise JobFailure(error.name, error.detail) from None
            except promotion.PromotionError as error:
                raise JobFailure("promotion_refused", str(error)) from None
            except Exception as error:
                raise JobFailure("mlflow_failed", f"{type(error).__name__}: {error}") from None
            return outputs

        job_actions.run_step(context, record, "promotion_check", allowed.__contains__, plan)
