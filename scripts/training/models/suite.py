"""Fit and compare six offline model bodies on one hospital-window holdout."""

from __future__ import annotations

import hashlib
import importlib
import itertools
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any

import numpy as np

from scripts.training import research_tests
from scripts.training.models.common import (
    REGISTRABLE,
    SLOTS,
    Array,
    Dataset,
    DeadlineExceeded,
    ModelError,
    ModelRequest,
    NotConverged,
    NotEstimable,
    Prepared,
    SlotResult,
    metrics,
    seed_for,
)
from scripts.training.models.elastic import ElasticRegressor
from scripts.training.models.linear import OLSRegressor
from scripts.training.models.mixed import MixedRegressor
from scripts.training.models.panel import PanelRegressor, repeated_hospital_positions
from scripts.training.models.preprocessing import FoldPreprocessor, missing
from scripts.training.models.spatial import SpatialRegressor, build_weights, moran_test
from scripts.training.models.trees import TreeRegressor, shap_values


def folds_for(data: Dataset, config: Mapping[str, Any], seed: int) -> list[tuple[Any, Any]]:
    """Create seeded, shuffled folds that keep all windows of one hospital together."""
    groups = np.asarray([row["ccn"] for row in data.rows])
    count = config["cross_validation"]["folds"]
    if len(set(groups)) < count:
        raise NotEstimable("fewer training hospitals than configured cross-validation folds")
    splitter = importlib.import_module("sklearn.model_selection").GroupKFold(n_splits=count, shuffle=True, random_state=seed)
    return list(splitter.split(np.zeros(len(groups)), data.y, groups))


def time_folds(data: Dataset) -> list[tuple[Any, Any]]:
    """Reserve each later training year after at least two years for panel fitting."""
    years = np.asarray([row["window_year"] for row in data.rows])
    return [(np.flatnonzero(years < year), np.flatnonzero(years == year)) for year in sorted(set(years))[2:]]


def _pipeline(request: ModelRequest, slot: str, parameters: Mapping[str, Any], forest: bool) -> Any:
    features = request.features_for(slot)
    if slot == "m3_mixed":
        features = tuple(feature for feature in features if feature.name != "control_type_code")
    forced = () if slot == "m1_ols" else (request.cmi_reference,)
    if forced and not set(forced).issubset(feature.reference for feature in features):
        raise NotEstimable("forced covariate CMI is absent from the model specification")
    prepare = FoldPreprocessor(features, forced=forced, native_missing=slot == "m4_boosting")
    seed = seed_for(request.config["master_seed"], request.data.hai_type, "forest" if forest else slot)
    if slot == "m1_ols":
        estimator: Any = OLSRegressor()
    elif slot == "m2_elastic_net":
        estimator = ElasticRegressor(float(parameters["alpha"]), float(parameters["l1_ratio"]), forced, seed)
    elif slot == "m3_mixed":
        estimator = MixedRegressor()
    elif slot == "m4_boosting":
        estimator = TreeRegressor("random_forest" if forest else "xgboost", seed, request.threads, parameters)
    elif slot == "m5_spatial":
        estimator = SpatialRegressor(request.adjacency)
    elif slot == "m6_fixed_effects":
        estimator = PanelRegressor()
    else:
        raise ModelError("unknown model slot")
    return importlib.import_module("sklearn.pipeline").Pipeline([("prepare", prepare), ("model", estimator)])


def _grid(grid: Mapping[str, list[Any]]) -> list[dict[str, Any]]:
    names = sorted(grid)
    if any(not grid[name] for name in names):
        raise ModelError("tuning grid contains no candidate values")
    return [dict(zip(names, values, strict=True)) for values in itertools.product(*(grid[name] for name in names))]


def _fit_pipeline(request: ModelRequest, slot: str, parameters: Mapping[str, Any], forest: bool, training: Dataset) -> tuple[Any, list[str]]:
    dropped: list[str] = []
    if slot == "m6_fixed_effects":
        positions, dropped = repeated_hospital_positions(training.rows)
        training = training.subset(positions)
        if not training.rows:
            raise NotEstimable("panel has no repeated training hospitals")
    request.deadline.check()
    pipeline = _pipeline(request, slot, parameters, forest)
    pipeline.fit(training.rows, training.y)
    request.deadline.check()
    return pipeline, dropped


def _tune(request: ModelRequest, slot: str, forest: bool, training: Dataset) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    config = request.config["slots"][slot]
    options = _grid(config["check"]["grid"] if forest else config["grid"])
    if slot not in ("m2_elastic_net", "m4_boosting", "m6_fixed_effects"):
        return options[0], []
    fold_seed = seed_for(request.config["master_seed"], request.data.hai_type, "folds")
    folds = time_folds(training) if slot == "m6_fixed_effects" else folds_for(training, request.config, fold_seed)
    if not folds:
        return options[0], [{"status": "not_estimable", "reason": "no time-ordered validation fold after two fitting years"}]
    cached: list[tuple[Prepared, Array, Prepared, Array, dict[str, Any]]] = []
    for left, right in folds:
        request.deadline.check()
        fitting, validation = training.subset(left), training.subset(right)
        if slot == "m6_fixed_effects":
            positions, _ = repeated_hospital_positions(fitting.rows)
            fitting = fitting.subset(positions)
            if not fitting.rows:
                raise NotEstimable("panel has no repeated training hospitals")
        prepare = _pipeline(request, slot, options[0], forest).named_steps["prepare"]
        fitting_design = prepare.fit_transform(fitting.rows)
        validation_design = prepare.transform(validation.rows)
        record = {
            "fitting_rows": len(fitting.rows),
            "validation_rows": len(validation.rows),
            "selected_references": list(prepare.kept_references_),
            "indicator_aliases": dict(prepare.indicator_aliases_),
            "constant_columns": dict(prepare.constant_columns_),
        }
        cached.append((fitting_design, fitting.y, validation_design, validation.y, record))
        request.deadline.check()
    audit: list[dict[str, Any]] = []
    best: tuple[float, int] | None = None
    for index, parameters in enumerate(options):
        sum_squared_error, row_count = 0.0, 0
        fold_records: list[dict[str, Any]] = []
        try:
            for fitting_design, fitting_y, validation_design, validation_y, record in cached:
                request.deadline.check()
                estimator = _pipeline(request, slot, parameters, forest).named_steps["model"]
                estimator.fit(fitting_design, fitting_y)
                request.deadline.check()
                prediction = np.asarray(estimator.predict(validation_design), dtype=np.float64).reshape(-1)
                metrics(validation_y, prediction)
                request.deadline.check()
                sum_squared_error += float(np.sum((validation_y - prediction) ** 2))
                row_count += len(validation_y)
                fold_records.append(record)
        except (NotEstimable, NotConverged, np.linalg.LinAlgError) as error:
            audit.append(
                {
                    "parameters": parameters,
                    "status": "not_converged" if isinstance(error, NotConverged) else "not_estimable",
                    "reason": str(error),
                    "folds": fold_records,
                }
            )
            continue
        score = math.sqrt(sum_squared_error / row_count)
        audit.append({"parameters": parameters, "status": "fitted", "rmse": score, "folds": fold_records})
        if best is None or (score, index) < best:
            best = score, index
    if best is None:
        reason = audit[0]["reason"]
        if all(record["status"] == "not_converged" for record in audit):
            raise NotConverged(f"no tuning configuration converged: {reason}")
        raise NotEstimable(f"no tuning configuration completed every fitting fold: {reason}")
    return options[best[1]], audit


def _fit_slot(request: ModelRequest, slot: str, forest: bool) -> SlotResult:
    training, holdout = request.data.split()
    positions = tuple(range(len(holdout.rows)))
    if slot == "m3_mixed":
        training = training.subset([index for index, row in enumerate(training.rows) if not missing(row.get("control_type_code"))])
        positions = tuple(index for index, row in enumerate(holdout.rows) if not missing(row.get("control_type_code")))
        holdout = holdout.subset(positions)
        if not training.rows or not holdout.rows:
            raise NotEstimable("mixed model has no ownership-eligible training or holdout windows")
    parameters, audit = _tune(request, slot, forest, training)
    pipeline, dropped = _fit_pipeline(request, slot, parameters, forest, training)
    prediction = np.asarray(pipeline.predict(holdout.rows), dtype=np.float64).reshape(-1)
    scored = metrics(holdout.y, prediction)
    request.deadline.check()
    prepare, model = pipeline.named_steps["prepare"], pipeline.named_steps["model"]
    details = {
        **model.details_,
        "training_rows": len(training.rows),
        "holdout_rows": len(holdout.rows),
        "selected_references": list(prepare.kept_references_),
        "training_missing_fraction": prepare.missing_rates_,
        "indicator_aliases": dict(prepare.indicator_aliases_),
        "constant_columns": dict(prepare.constant_columns_),
        "unknown_holdout_categories": prepare.transform(holdout.rows).unknown_categories,
        "tuning": audit,
        "selected_parameters": parameters,
    }
    if slot == "m2_elastic_net":
        grid = request.config["slots"][slot]["grid"]
        details["grid_edge"] = any(parameters[name] in (grid[name][0], grid[name][-1]) for name in grid)
    if slot == "m6_fixed_effects":
        details["dropped_singleton_hospital_count"] = len(dropped)
        details["unseen_holdout_hospitals"] = len({row["ccn"] for row in holdout.rows if row["ccn"] not in model.effects_})
    return SlotResult(
        slot,
        "fitted",
        predictions=tuple(prediction.tolist()),
        holdout_positions=positions,
        metrics={"rmse": scored["rmse"], "mae": scored["mae"], "r2": scored["r2"]},
        details=details,
        pipeline=pipeline,
    )


def fit_slot(request: ModelRequest, slot: str, *, forest: bool = False) -> SlotResult:
    """Return an explicit model status while preserving contract and dependency failures."""
    if slot not in SLOTS:
        raise ModelError("unknown model slot")
    operation = replace(request, deadline=request.deadline.within(request.config["slots"][slot]["budget_seconds"]))
    try:
        result = _fit_slot(operation, slot, forest)
        if slot == "m4_boosting" and not forest:
            check = fit_slot(operation, slot, forest=True)
            result.details["forest_check"] = {
                "status": check.status,
                "reason": check.reason,
                "metrics": check.metrics,
                "details": check.details,
                "registrable": False,
            }
    except NotConverged as error:
        result = SlotResult(slot, "not_converged", str(error))
    except (NotEstimable, np.linalg.LinAlgError) as error:
        result = SlotResult(slot, "not_estimable", str(error))
    except DeadlineExceeded:
        result = SlotResult(slot, "not_run", "deadline")
    if slot == "m3_mixed":
        training, holdout = request.data.split()
        excluded = {
            "training": sum(missing(row.get("control_type_code")) for row in training.rows),
            "holdout": sum(missing(row.get("control_type_code")) for row in holdout.rows),
        }
        result.details["excluded_missing_ownership"] = {**excluded, "total": sum(excluded.values())}
        result.details["excluded_fixed_references"] = [feature.reference for feature in request.features_for(slot) if feature.name == "control_type_code"]
    return result


def comparison_scores(results: Mapping[str, SlotResult], observed: Array) -> tuple[dict[str, float], int]:
    """Score fitted alternatives on the intersection of their declared holdout positions."""
    fitted = {slot: result for slot, result in results.items() if result.status == "fitted"}
    if not fitted:
        return {}, 0
    positions = sorted(set.intersection(*(set(result.holdout_positions) for result in fitted.values())))
    if not positions:
        raise ModelError("fitted models have no shared scored holdout windows")
    scores = {}
    for slot, result in fitted.items():
        predictions = dict(zip(result.holdout_positions, result.predictions, strict=True))
        scores[slot] = metrics(observed[positions], np.asarray([predictions[index] for index in positions]))["rmse"]
    return scores, len(positions)


def select(results: Mapping[str, SlotResult], *, registrable: bool = False, non_spatial: bool = False, observed: Array | None = None) -> str | None:
    """Choose the lowest matched-holdout RMSE and break ties by the declared slot order."""
    allowed = REGISTRABLE if registrable else SLOTS
    eligible = {
        slot: result for slot, result in results.items() if slot in allowed and not (non_spatial and slot == "m5_spatial") and result.status == "fitted"
    }
    scores = comparison_scores(eligible, observed)[0] if observed is not None else {slot: result.metrics.get("rmse") for slot, result in eligible.items()}
    choices = []
    for index, slot in enumerate(SLOTS):
        if slot not in eligible:
            continue
        score = scores.get(slot)
        if score is None or not math.isfinite(score):
            raise ModelError("fitted model lacks a finite common-holdout RMSE")
        choices.append((score, index, slot))
    return min(choices)[2] if choices else None


def _p_values(results: Mapping[str, Any], prefix: str = "") -> dict[str, float]:
    values: dict[str, float] = {}
    for name, result in results.items():
        key = f"{prefix}.{name}" if prefix else name
        if isinstance(result, Mapping):
            values.update(_p_values(result, key))
        elif name == "p_value" or name.endswith("_p_value"):
            value = float(result)
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ModelError("research p-value is not finite or outside zero to one")
            values[key] = value
    return values


@dataclass(frozen=True)
class ModelSuite:
    """Six slot results, stable research outputs and the separate registry candidate."""

    request: ModelRequest
    slots: Mapping[str, SlotResult]
    selected: str | None
    candidate: str | None
    research: Mapping[str, Any]
    shap: Mapping[str, Any]

    def to_result(self) -> dict[str, Any]:
        """Return stable bundle fields without pipelines, run IDs, timing or source row values."""
        training, holdout = self.request.data.split()
        comparison, comparison_count = comparison_scores(self.slots, holdout.y)
        registry = {slot: self.slots[slot] for slot in REGISTRABLE}
        _, registry_count = comparison_scores(registry, holdout.y)
        predictions = {
            slot: dict(zip(result.holdout_positions, result.predictions, strict=True)) for slot, result in self.slots.items() if result.status == "fitted"
        }
        digest = hashlib.sha256()
        for index in sorted(range(len(holdout.rows)), key=lambda index: holdout.keys[index]):
            digest.update(holdout.keys[index].encode() + b"\0")
            values = [holdout.y[index], *(predictions[slot].get(index) for slot in SLOTS if slot in predictions)]
            digest.update(json.dumps(values, allow_nan=False, separators=(",", ":")).encode() + b"\0")
        payload = {
            "statuses": {slot: {"status": self.slots[slot].status, "reason": self.slots[slot].reason} for slot in SLOTS},
            "selection": {
                "selected": self.selected,
                "registry_candidate": self.candidate,
                "comparison_holdout_rows": comparison_count,
                "comparison_rmse": comparison,
                "registry_holdout_rows": registry_count,
            },
            "fitted_parameters": {slot: self.slots[slot].details for slot in SLOTS},
            "holdout_metrics": {slot: self.slots[slot].metrics for slot in SLOTS},
            "test_statistics": dict(self.research),
            "p_values": _p_values(self.research),
            "counts": {"training": len(training.rows), "holdout": len(holdout.rows), "training_hospitals": len({row["ccn"] for row in training.rows})},
            "seeds": {
                purpose: seed_for(self.request.config["master_seed"], self.request.data.hai_type, purpose)
                for purpose in (*SLOTS, "folds", "forest", "moran", "shap", "nested_bootstrap")
            },
            "input_hashes": {"feature_lists_sha256": hashlib.sha256(json.dumps(dict(self.request.feature_lists), sort_keys=True).encode()).hexdigest()},
            "holdout_predictions_content_sha256": digest.hexdigest(),
        }
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return payload


def _optional_analysis(call: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return call()
    except DeadlineExceeded:
        raise
    except (ModelError, np.linalg.LinAlgError) as error:
        return {"status": "not_estimable", "reason": str(error)}


def fit_models(request: ModelRequest) -> ModelSuite:
    """Run the six model bodies and research tests without auxiliary validation inputs.

    Parameters
    ----------
    request : ModelRequest
        A verified single-type extract, reviewed feature lists and one operation deadline.

    Returns
    -------
    ModelSuite
        Fitted pipelines, six explicit statuses and deterministic research results.

    Notes
    -----
    The owning fit job must enforce the native-call deadline through its owned process or image deadline entrypoint.
    This callable checks that same absolute deadline between numerical calls and never owns a service or process.
    """
    request.deadline.check()
    training, holdout = request.data.split()
    minimum = request.config["minimum_rows"]
    if len(training.rows) < minimum["training"] or len(holdout.rows) < minimum["holdout"]:
        raise ModelError(f"insufficient split rows: training={len(training.rows)}, holdout={len(holdout.rows)}")
    if request.threads < 1:
        raise ModelError("training thread count must be positive")
    result = {slot: fit_slot(request, slot) for slot in SLOTS if slot != "m5_spatial"}
    research: dict[str, Any] = {}
    best = select(result, non_spatial=True, observed=holdout.y)
    if best is None:
        result["m5_spatial"] = SlotResult("m5_spatial", "not_run", "no fitted non-spatial model")
        research["spatial"] = {"status": "not_run", "reason": "no fitted non-spatial model"}
    else:
        residual_sample = holdout.subset(result[best].holdout_positions)
        graph = build_weights(residual_sample.rows, request.adjacency)
        spatial_test = moran_test(
            residual_sample.y - np.asarray(result[best].predictions),
            residual_sample.keys,
            graph,
            seed_for(request.config["master_seed"], request.data.hai_type, "moran"),
            request.config["moran"]["permutations"],
            request.deadline,
        )
        research["spatial"] = {**spatial_test, "residual_model": best}
        if spatial_test["status"] == "fitted" and spatial_test["p_value"] < 0.05:
            result["m5_spatial"] = fit_slot(request, "m5_spatial")
        else:
            result["m5_spatial"] = SlotResult(
                "m5_spatial", "not_run", "Moran's I not significant" if spatial_test["status"] == "fitted" else spatial_test["reason"]
            )
    if result["m1_ols"].status == "fitted":
        research["joint_process"] = _optional_analysis(lambda: research_tests.joint_significance(result["m1_ols"].pipeline, request.process_references))
    else:
        research["joint_process"] = {"status": "not_estimable", "reason": result["m1_ols"].reason}
    if result["m2_elastic_net"].status == "fitted":
        research["process_vs_intercept_cmi"] = _optional_analysis(
            lambda: research_tests.intercept_cmi_comparison(request, result["m2_elastic_net"].predictions)
        )
    else:
        research["process_vs_intercept_cmi"] = {"status": "not_estimable", "reason": result["m2_elastic_net"].reason}
    research["ownership"] = {"status": result["m3_mixed"].status, "reason": result["m3_mixed"].reason, **result["m3_mixed"].details}
    research["hospital_effects"] = result["m6_fixed_effects"].details.get(
        "hausman", {"status": result["m6_fixed_effects"].status, "reason": result["m6_fixed_effects"].reason}
    )
    research["nested"] = _optional_analysis(lambda: research_tests.nested_comparison(request))
    shap: dict[str, Any] = {"status": "not_run", "reason": "gradient boosting was not fitted"}
    if result["m4_boosting"].status == "fitted":
        shap = _optional_analysis(
            lambda: shap_values(
                result["m4_boosting"].pipeline,
                holdout.rows,
                seed_for(request.config["master_seed"], request.data.hai_type, "shap"),
                request.config["shap"]["sample_cap"],
                request.deadline.within(request.config["budgets"]["shap_seconds"]),
            )
        )
    request.deadline.check()
    return ModelSuite(request, result, select(result, observed=holdout.y), select(result, registrable=True, observed=holdout.y), research, shap)
