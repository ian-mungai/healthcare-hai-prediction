"""The research action: the tests that need two models and the choice of one challenger per type, from the finished runs of one base publication.

The action reads MLflow only. For each requested type it needs a finished run of m1, m2 and m4 with the same ``base_run`` and the requested population, and
refuses by name when one is missing (``research_models_missing``) or changed (``research_holdouts_differ``, ``research_rows_differ``). It then

- assembles RQ1 from the joint test that the m1 run logged and the Elastic Net comparison that the m2 run logged,
- runs RQ4 on the reduced fit of the m1 run and the full fit of the m2 run (``research_tests.rq4_compare``),
- reads the verdict that each promotion-eligible run gave its own version under the owner's promotion rule and sets the ``challenger`` alias on the
  eligible version with the lowest holdout RMSE, one challenger per type.

It never moves the ``production`` alias. The record of each type is ``research_<type>.json``, and a ``research`` run of the type's experiment holds its numbers.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from orchestration import training_contract
from scripts.training import job_actions, job_inputs, promotion, registry, research_tests, run_tracking
from scripts.training.job_failure import JobFailure
from scripts.training.models.common import MODEL_SLOT, Deadline, ModelError, seed_for

NEEDED = training_contract.PROMOTION_MODELS
STEP = "research"
SIDE = "bundle/rq4_side.json"


def parameters_of(path: Path) -> dict[str, Any]:
    """Return the validated parameters of a research run."""
    raw = job_inputs.read_json(path, "parameters")
    try:
        return training_contract.parameters_for("research_dag", raw, training_contract.JOB_LANES)
    except (ValueError, TypeError) as error:
        raise JobFailure("parameters", str(error)) from None


def names_for(hai_type: str) -> str:
    """Return the record name of a type."""
    return f"research_{hai_type}.json"


def parents_of(store: Any, experiment_id: str, hai_type: str, parameters: Mapping[str, Any]) -> dict[str, Any]:
    """Return the finished parent run of each needed model on the base run; of two runs of one model the later training run ID counts."""
    found = store.search_runs([experiment_id], f"tags.base_run = '{parameters['base_run']}' and tags.step_key = '{run_tracking.PARENT_STEP}'", max_results=1000)
    chosen: dict[str, Any] = {}
    for item in found:
        tags = item.data.tags
        model = tags.get("model")
        if model not in NEEDED or run_tracking.COMPLETE_TAG not in tags or tags.get("population") != parameters["population"]:
            continue
        if model not in chosen or tags["run_id"] > chosen[model].data.tags["run_id"]:
            chosen[model] = item
    missing = [model for model in NEEDED if model not in chosen]
    if missing:
        raise JobFailure(
            "research_models_missing",
            f"{hai_type} has no finished {parameters['population']} run of {', '.join(missing)} on the base run {parameters['base_run']}",
        )
    return chosen


def child_result(store: Any, experiment_id: str, parent: Any, step: str, prefix: str) -> dict[str, Any]:
    """Return the status and the numbers of one test a run logged as a child, or a missing result."""
    run = parent.data.tags["run_id"]
    children = store.search_runs([experiment_id], f"tags.run_id = '{run}' and tags.step_key = '{step}'", max_results=2)
    if len(children) != 1:
        return {"status": "missing", "reason": f"the run {run} logged {len(children)} {step} runs"}
    data = children[0].data
    return {
        "status": data.params.get("status", "unknown"),
        **{key.removeprefix(prefix): value for key, value in sorted(data.metrics.items()) if key.startswith(prefix)},
    }


def side_of(store: Any, parent: Any, name: str) -> Mapping[str, Any]:
    """Download the RQ4 side file of a run and return the side it holds under ``name``."""
    with tempfile.TemporaryDirectory(prefix="research_") as scratch:
        try:
            document = json.loads(Path(store.download_artifacts(parent.info.run_id, SIDE, scratch)).read_bytes())
        except (OSError, ValueError) as error:
            raise JobFailure("research_side_missing", f"the run {parent.data.tags['run_id']} has no readable RQ4 side file: {type(error).__name__}") from None
    if name not in document:
        raise JobFailure("research_side_missing", f"the RQ4 side file of the run {parent.data.tags['run_id']} holds no {name}")
    side: Mapping[str, Any] = document[name]
    return side


def candidate_of(store: Any, name: str, model: str, parent: Any) -> dict[str, Any]:
    """Describe the version a model's run registered: the verdict of its promotion check and its holdout RMSE."""
    versions = [item for item in registry.versions_of(store, name) if item.run_id == parent.info.run_id]
    rmse = parent.data.metrics.get(f"comparison_rmse.{MODEL_SLOT[model]}")
    if len(versions) != 1:
        return {"model": model, "version": None, "verdict": "missing_version", "rmse": rmse, "eligible": False}
    verdict = str(versions[0].tags.get("promotion_verdict", "none"))
    return {
        "model": model,
        "version": str(versions[0].version),
        "verdict": verdict,
        "rmse": rmse,
        "eligible": verdict in promotion.REFERENCES.values() and rmse is not None,
    }


def research_type(store: Any, parameters: Mapping[str, Any], hai_type: str, run: str) -> bytes:
    """Run the cross-model tests of one type and set its one challenger; return the record of the type."""
    contract = job_inputs.read_json(job_inputs.ROOT / "data_contracts/training/model_contract.json", "model_contract")
    found = store.get_experiment_by_name(contract["experiments"][hai_type])
    if found is None:
        raise JobFailure("research_models_missing", f"the experiment of {hai_type} does not exist, so no model has run")
    experiment_id = str(found.experiment_id)
    parents = parents_of(store, experiment_id, hai_type, parameters)
    config = job_inputs.config_of(job_inputs.ROOT)
    rq1 = {
        "joint_process": child_result(store, experiment_id, parents["m1"], "rq1_joint", "rq1_joint.joint_process."),
        "process_vs_intercept_cmi": child_result(store, experiment_id, parents["m2"], "rq1_joint", "rq1_joint.process_vs_intercept_cmi."),
    }
    budget = Deadline.after(float(config["budgets"]["bootstrap_seconds"]))
    seed = seed_for(config["master_seed"], hai_type, "nested_bootstrap")
    reduced, full = side_of(store, parents["m1"], "rq4_reduced"), side_of(store, parents["m2"], "rq4_full")
    try:
        rq4 = research_tests.rq4_compare(reduced, full, config["bootstrap"]["nested_comparison"], seed, budget)
    except ModelError as error:
        raise JobFailure("research_rows_differ", str(error)) from None
    name = contract["registered_models"][hai_type]
    candidates = [candidate_of(store, name, model, parents[model]) for model in NEEDED]
    if len({parents[model].data.metrics.get("comparison_holdout_rows") for model in NEEDED}) != 1:
        raise JobFailure("research_holdouts_differ", f"the {hai_type} runs of {', '.join(NEEDED)} scored holdouts of other sizes")
    eligible = sorted((item for item in candidates if item["eligible"]), key=lambda item: (item["rmse"], NEEDED.index(item["model"])))
    winner = eligible[0] if eligible else None
    if winner is not None:
        store.set_registered_model_alias(name, registry.CHALLENGER_ALIAS, winner["version"])
    document = {
        "schema": 1,
        "hai_type": hai_type,
        "base_run": parameters["base_run"],
        "population": parameters["population"],
        "runs": {model: parents[model].data.tags["run_id"] for model in NEEDED},
        "rq1": rq1,
        "rq4": rq4,
        "promotion": {
            "registered_model": name,
            "candidates": candidates,
            "challenger": None if winner is None else {"model": winner["model"], "version": winner["version"], "verdict": winner["verdict"]},
        },
    }
    body = job_actions.text(document)
    digest = hashlib.sha256(body).hexdigest()
    numbers = (
        run_tracking.numbers("rq1", rq1)
        | run_tracking.numbers("rq4", rq4)
        | run_tracking.numbers("promotion", {item["model"]: item["rmse"] for item in candidates})
    )
    tags = {"run_id": run, "step_key": STEP, "hai_type": hai_type, "base_run": parameters["base_run"], "population": parameters["population"]}
    with tempfile.TemporaryDirectory(prefix="research_") as scratch:
        path = Path(scratch) / names_for(hai_type)
        path.write_bytes(body)
        spec = run_tracking.RunSpec(STEP, tags, {"challenger": json.dumps(document["promotion"]["challenger"], sort_keys=True)}, numbers, [(path, "research")])
        run_tracking.log_child(store, contract["experiments"][hai_type], None, spec, run, digest)
    return body


def run(context: Any, record: Mapping[str, Any]) -> None:
    """Run the research of every requested type against the tracking URI the job is given."""
    with job_actions.guarded("research"):
        parameters = parameters_of(context.parameters)
        allowed = {names_for(hai_type) for hai_type in parameters["hai_types"]}

        def plan() -> dict[str, bytes]:
            uri = os.environ.get("MLFLOW_TRACKING_URI", "")
            if not uri:
                raise JobFailure("mlflow_uri_missing", "MLFLOW_TRACKING_URI is not set")
            outputs: dict[str, bytes] = {}
            try:
                store = run_tracking.open_store(uri)
                for hai_type in parameters["hai_types"]:
                    outputs[names_for(hai_type)] = research_type(store, parameters, hai_type, context.run)
            except (JobFailure, ImportError):
                raise
            except run_tracking.TrackingError as error:
                raise JobFailure(error.name, error.detail) from None
            except Exception as error:
                raise JobFailure("mlflow_failed", f"{type(error).__name__}: {error}") from None
            return outputs

        job_actions.run_step(context, record, STEP, allowed.__contains__, plan)
