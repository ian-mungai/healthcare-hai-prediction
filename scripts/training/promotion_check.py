"""The promotion_check action: register the run's candidate, score it against the champion on the same holdout and record the verdict.

With no champion, the candidate is scored against the intercept + CMI baseline instead, fitted on the same training years as the fit step's RQ1 test; a pass
is ``pass_baseline``. The comparison record names the reference it used. It never moves the ``production`` alias and sets no alias: the research action
chooses the one challenger of a type among the versions of m1, m2 and m4 that passed. A champion whose columns this run's extract lacks cannot be scored, and
the version gets the verdict ``not_comparable``. A champion whose feature list has no usable ``columns``, or whose list file cannot be downloaded, read or
parsed, is never loaded or scored: the check fails with ``champion_feature_list_invalid``. The comparison files are written and checked before any version
tag. The check reads the written Parquet file back and repeats the rule on it.

A model is read as data only, never through ``mlflow.sklearn.load_model`` (which reads ``MLmodel``, imports the artifact's code folder and takes trusted types
from the artifact): the challenger from its own ``model.skops`` and the champion from the four fixed files of its run, fetched one by one through the bounded
fetch of ``scripts/model_code.py`` (1 MiB for a JSON file, 64 MiB for the model, one deadline each). Both are read with skops under the trusted types of this
code root's own model contract. Before its model file is fetched, the champion must record the model code closure of this job's own code and must have a
self-check that passes the data rules of the model API: a champion that does not is refused by name (``model_code_closure_missing`` for one trained before the
closure was recorded). With no champion nothing of this applies and the baseline is the reference, as before.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from scripts import model_code
from scripts.training import bundle, job_actions, job_inputs, promote, promotion, registry, research_tests, run_tracking, step_records
from scripts.training.job_failure import JobFailure
from scripts.training.models.common import Deadline, ModelError, seed_for

PROMOTION_SECONDS = 600.0


# The bounded single-file fetch of one run's file (a name here so that a case can replace the transport).
fetch_file = model_code.mlflow_fetch


def load_pipeline(path: str, trusted: Sequence[str]) -> Any:
    """Read one saved model file as data: skops under the trusted types of this code root's own model contract, never a list from the artifact."""
    return model_code.load_data_only(path, trusted)


def predict(pipeline: Any, rows: list[dict[str, Any]]) -> dict[str, float]:
    """Score holdout rows with one pipeline and return the prediction of each hospital-window."""
    values = np.asarray(pipeline.predict(rows), dtype=np.float64).reshape(-1)
    if len(values) != len(rows):
        raise JobFailure("promotion_refused", "a pipeline returned another number of predictions than holdout rows")
    return {row["spine_key"]: float(value) for row, value in zip(rows, values, strict=True)}


def names_for(hai_type: str) -> tuple[str, str]:
    """Return the comparison Parquet and JSON file names of a type."""
    return f"comparison_{hai_type}.parquet", f"comparison_{hai_type}.json"


def champion_columns(document: Any) -> list[str]:
    """Return the ``columns`` of a champion's feature list, or refuse a list whose ``columns`` is not a non-empty list of distinct, non-empty names.

    The refusal is ``champion_feature_list_invalid``, a failure of the check and not the verdict ``not_comparable``: the champion's own artifact is damaged,
    and no extract can repair that. The detail names the key and what was found, never a value.
    """
    kinds = {str: "text", bool: "a boolean", int: "a number", float: "a number", dict: "an object", list: "a list", type(None): "null"}
    if not isinstance(document, Mapping):
        raise JobFailure("champion_feature_list_invalid", f"the champion's feature list is {kinds.get(type(document), 'not JSON')}, not an object")

    def refuse(found: str) -> JobFailure:
        return JobFailure("champion_feature_list_invalid", f"the champion's feature list holds a bad columns key: {found}")

    if "columns" not in document:
        raise refuse("absent")
    columns = document["columns"]
    if not isinstance(columns, list):
        raise refuse(f"{kinds.get(type(columns), 'not JSON')}, not a list")
    if not columns:
        raise refuse("an empty list")
    seen: set[str] = set()
    for index, name in enumerate(columns):
        if not isinstance(name, str):
            raise refuse(f"element {index} is {kinds.get(type(name), 'not JSON')}, not text")
        if not name:
            raise refuse(f"element {index} is an empty name")
        if name in seen:
            raise refuse(f"element {index} repeats an earlier name")
        seen.add(name)
    return columns


# For each file of a champion: its name in a refusal, then the reason for a file that cannot be fetched or read, for one over its cap and for one that is not
# JSON.
CHAMPION_FILES = {
    model_code.PROVENANCE: ("provenance", "model_provenance_unreadable", "model_provenance_unreadable", "model_provenance_unreadable"),
    model_code.FEATURE_LIST: ("feature list", "champion_feature_list_invalid", "champion_feature_list_invalid", "champion_feature_list_invalid"),
    model_code.SELF_CHECK: ("self-check", "self_check_missing", "self_check_too_large", "self_check_not_json"),
    model_code.MODEL_FILE: ("model file", "champion_unloadable", "champion_unloadable", "champion_unloadable"),
}


def champion_file(version: Any, path: str, folder: str) -> str:
    """Fetch one file of the champion's run to its fixed name in ``folder`` through the bounded fetch, and require one regular file. A failure is a named
    refusal that says which step failed and never holds the text of an error (a deadline, or the one worker slot of the shared fetch staying taken by an
    earlier cancelled call, is ``champion_fetch_deadline``)."""
    label, unreadable, oversize, _ = CHAMPION_FILES[path]
    cap = model_code.MODEL_CAP if path == model_code.MODEL_FILE else model_code.JSON_CAP
    try:
        return model_code.fetch_regular_file(fetch_file, str(version.run_id), path, os.path.join(folder, os.path.basename(path)), cap)
    except JobFailure:
        raise
    except model_code.DataRuleError as error:
        if error.rule in ("deadline", "busy"):
            raise JobFailure("champion_fetch_deadline", f"the champion's {label} was not fetched in time: {error}") from None
        if error.rule == "too_large":
            raise JobFailure(oversize, f"the champion's {label} is over its byte cap") from None
        raise JobFailure(unreadable, f"the champion's {label} was not fetched as one regular file") from None
    except Exception as error:
        raise JobFailure(unreadable, f"the champion's {label} could not be downloaded: {type(error).__name__}") from None


def champion_document(version: Any, path: str, folder: str) -> Any:
    """Fetch a champion's JSON file and parse it with the data rules of the model API (one UTF-8 document, no repeated key, no NaN or Infinity), or refuse by
    the reason of the file. Every detail ends with the class of the error and never holds its text. Nothing here tells a transport fault from a damaged file, so
    both refuse; the job does not retry."""
    label, unreadable, oversize, not_json = CHAMPION_FILES[path]
    local = champion_file(version, path, folder)
    try:
        return model_code.read_json_file(local, path)
    except model_code.DataRuleError as error:
        cause = type(error.__cause__).__name__
        if error.rule == "not_json":
            raise JobFailure(not_json, f"the champion's {label} is not JSON: {cause}") from None
        if error.rule == "too_large":
            raise JobFailure(oversize, f"the champion's {label} is over its byte cap") from None
        raise JobFailure(unreadable, f"the champion's {label} file is unreadable: {cause}") from None


def champion_predictions(
    store: Any, version: Any, name: str, manifest: Mapping[str, Any], rows: list[dict[str, Any]], year: int, root: Path, trusted: Sequence[str]
) -> dict[str, float]:
    """Admit the champion, then read it as data and score the holdout.

    The admission comes first and reads no model: its provenance must hold the closure digest of this job's own code root (``promote.check_closure``), its
    feature list must hold the bounds of the model API and its self-check must pass the model API's shape rules (``promote.check_self_check``). Then the checks
    that it needs only extract columns and never trained on the holdout year. Only then is its model file fetched and read with skops under ``trusted``.
    """
    with tempfile.TemporaryDirectory(prefix="champion_") as folder:
        promote.check_closure(champion_document(version, model_code.PROVENANCE, folder), root)
        document = champion_document(version, model_code.FEATURE_LIST, folder)
        columns = set(champion_columns(document))
        try:
            references = list(model_code.check_feature_list(document)[0])
        except model_code.DataRuleError as error:
            raise JobFailure("champion_feature_list_invalid", f"the champion's feature list breaks a bound of the model API: {error}") from None
        promote.check_self_check(champion_document(version, model_code.SELF_CHECK, folder), references)
        if not columns <= set(manifest["columns"]):
            raise JobFailure("champion_features", f"the champion needs columns the extract lacks: {sorted(columns - set(manifest['columns']))[0]}")
        window = store.get_run(version.run_id).data.tags.get("newest_window", "")
        if not window.isdigit() or int(window) > year:
            raise JobFailure(
                "holdout_leak", f"the champion's newest window is {window or 'unknown'}, so its training years may include the holdout year {year}"
            )
        try:
            pipeline = load_pipeline(champion_file(version, model_code.MODEL_FILE, folder), trusted)
        except JobFailure:
            raise
        except Exception as error:
            raise JobFailure("champion_unloadable", f"{type(error).__name__}: {error}") from None
    return predict(pipeline, rows)


def baseline_predictions(
    hai_type: str, rows: list[dict[str, Any]], config: Mapping[str, Any], root: Path, model: str, cmi: Mapping[str, float | None] | None = None
) -> tuple[dict[str, float] | None, dict[str, Any]]:
    """With no champion, fit the intercept + CMI baseline the fit step's RQ1 test fits, on the same training years, and predict the holdout.

    A model whose own list leaves CMI out (m1) gets CMI from ``cmi``, the extract's separate table, for this fit only; its own specification is unchanged.

    A baseline that cannot be fitted leaves the type without a reference, so its verdict stays no_champion and nothing becomes promotable.
    """
    if cmi is not None and any("cmi" not in row for row in rows):
        if any(row["spine_key"] not in cmi for row in rows):
            return None, {"model": "intercept_cmi", "status": "not_estimable", "reason": "the extract's CMI table misses a hospital-window"}
        rows = [{**row, "cmi": cmi[row["spine_key"]]} for row in rows]
    try:
        request = job_inputs.model_request(hai_type, rows, (), config, PROMOTION_SECONDS, root, model, with_cmi=True)
        values = research_tests.intercept_cmi_predictions(request)
    except (job_inputs.InputError, ModelError) as error:
        return None, {"model": "intercept_cmi", "status": "not_estimable", "reason": str(error)}
    keys = request.data.split()[1].keys
    return dict(zip(keys, (float(value) for value in values), strict=True)), {
        "model": "intercept_cmi",
        "status": "fitted",
        "cmi_reference": request.cmi_reference,
    }


def not_comparable(store: Any, name: str, version: Any, current: Any, hai_type: str, reason: str, json_name: str) -> dict[str, bytes]:
    """Record that this run's data cannot score the champion, so the version is not promotable on this base, and tag the version."""
    store.set_model_version_tag(name, str(version.version), "promotion_verdict", "not_comparable")
    document = {
        "schema": 1,
        "hai_type": hai_type,
        "registered_model": name,
        "champion_version": str(current.version),
        "challenger_version": str(version.version),
        "verdict": {"verdict": "not_comparable", "failed": ["champion_features"]},
        "reason": reason,
    }
    return {json_name: job_actions.text(document)}


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
    rules = job_inputs.read_json(root / "config/training/promotion.json", "promotion_config")
    population = run_tags.get("population")
    if population != rules["population"] or manifest["population"] != rules["population"]:
        raise JobFailure(
            "population_not_primary",
            f"only the {rules['population']} population can become a challenger; the run is {population} and the extract {manifest['population']}",
        )
    name = contract["registered_models"][hai_type]
    version = registry.register(
        store,
        name,
        parent,
        f"{context.run}:{hai_type}:promotion",
        {
            key: run_tags[key]
            for key in (
                "hai_type",
                "model",
                "base_run",
                "target_control",
                "feature_schema_sha256",
                "feature_list_sha256",
                "feature_set",
                "feature_set_schema_sha256",
            )
        }
        | {"source_run_id": parent, "bundle_sha256": bundle_hash},
    )
    year = max(row["window_year"] for row in extract_rows)
    holdout = [row for row in extract_rows if row["window_year"] == year]
    trusted = contract["serialization"]["trusted_types"]
    challenger = predict(load_pipeline(str(base / model_code.MODEL_FILE), trusted), holdout)
    current = registry.champion(store, name)
    config = job_inputs.config_of(root)
    baseline: dict[str, Any] | None = None
    if current is None:
        rival, baseline = baseline_predictions(hai_type, extract_rows, config, root, parameters["model"], job_inputs.cmi_of(inputs / "extract", manifest))
    else:
        try:
            rival = champion_predictions(store, current, name, manifest, holdout, year, root, trusted)
        except JobFailure as failure:
            if failure.name != "champion_features":
                raise
            return not_comparable(store, name, version, current, hai_type, failure.detail, json_name)
    reference = "baseline" if current is None and rival is not None else "champion"
    regions = job_inputs.read_json(root / "config/training/census_regions.json", "promotion_config")["states"]
    seed = seed_for(config["master_seed"], hai_type, "promotion_bootstrap")
    rows = promotion.comparison_rows(holdout, manifest["label_column"], challenger, rival, rules, regions)
    verdict = promotion.evaluate(rows, rules, seed=seed, deadline=Deadline.after(PROMOTION_SECONDS), reference=reference)
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
        "reference": reference,
        "baseline": baseline,
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
    again = promotion.evaluate(job_inputs.read_rows(table), rules, seed=seed, deadline=Deadline.after(PROMOTION_SECONDS), reference=reference)
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
