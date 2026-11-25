"""Fit and compare six offline model bodies on one hospital-window holdout."""

from __future__ import annotations

import hashlib
import importlib
import itertools
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import numpy as np

from scripts.training import research_tests
from scripts.training.models.common import (
    MODEL_SLOT,
    REGISTRABLE,
    SLOTS,
    Array,
    Dataset,
    DeadlineExceeded,
    Feature,
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
from scripts.training.models.panel import MINIMUM_FITTING_YEARS, PanelRegressor, complete_case_positions, repeated_hospital_positions
from scripts.training.models.preprocessing import MAX_MISSING_FRACTION, FoldPreprocessor, missing, missing_selection
from scripts.training.models.spatial import SpatialRegressor, build_weights, moran_test
from scripts.training.models.trees import TreeRegressor, shap_values

# The likelihood models need a full-rank design, so their preprocessing drops the fitting-row columns that are exact combinations of others.
LIKELIHOOD_SLOTS = frozenset(("m1_ols", "m3_mixed", "m5_spatial", "m6_fixed_effects"))
# The slots that tune on time-ordered folds, with the fewest fitting years a fold may train on. The Elastic Net needs one year to fit a cross-section.
# The panel needs three, by its own estimability rule.
TIME_ORDERED_MINIMUM = {"m2_elastic_net": 1, "m6_fixed_effects": MINIMUM_FITTING_YEARS}


def folds_for(data: Dataset, config: Mapping[str, Any], seed: int) -> list[tuple[Any, Any]]:
    """Create seeded, shuffled folds that keep all windows of one hospital together."""
    groups = np.asarray([row["ccn"] for row in data.rows])
    count = config["cross_validation"]["folds"]
    if len(set(groups)) < count:
        raise NotEstimable("fewer training hospitals than configured cross-validation folds")
    splitter = importlib.import_module("sklearn.model_selection").GroupKFold(n_splits=count, shuffle=True, random_state=seed)
    return list(splitter.split(np.zeros(len(groups)), data.y, groups))


def time_folds(data: Dataset, minimum_years: int = MINIMUM_FITTING_YEARS) -> list[tuple[Any, Any]]:
    """Validate each training year on every earlier year, once at least ``minimum_years`` earlier years exist."""
    years = np.asarray([row["window_year"] for row in data.rows])
    return [(np.flatnonzero(years < year), np.flatnonzero(years == year)) for year in sorted(set(years))[minimum_years:]]


def _pipeline(request: ModelRequest, slot: str, parameters: Mapping[str, Any], forest: bool, features: tuple[Feature, ...] | None = None) -> Any:
    features = request.features_for(slot) if features is None else features
    if slot == "m3_mixed":
        features = tuple(feature for feature in features if feature.name != "control_type_code")
    forced = () if slot == "m1_ols" else (request.cmi_reference,)
    if forced and not set(forced).issubset(feature.reference for feature in features):
        raise NotEstimable("forced covariate CMI is absent from the model specification")
    prepare = FoldPreprocessor(
        features, forced=forced, native_missing=slot == "m4_boosting", drop_dependent=slot in LIKELIHOOD_SLOTS, winsorize=slot == "m2_elastic_net"
    )
    seed = seed_for(request.config["master_seed"], request.data.hai_type, "forest" if forest else slot)
    if slot == "m1_ols":
        estimator: Any = OLSRegressor()
    elif slot == "m2_elastic_net":
        estimator = ElasticRegressor(float(parameters["alpha"]), float(parameters["l1_ratio"]), forced, seed)
    elif slot == "m3_mixed":
        estimator = MixedRegressor(request.deadline)
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


@dataclass(frozen=True)
class PanelSample:
    """The fixed-effects rows of one fitting set with the predictors that were learned from it before any row was dropped."""

    data: Dataset
    features: tuple[Feature, ...]
    selection: Mapping[str, Any]
    incomplete: int
    singletons: list[str]


def _panel_rows(request: ModelRequest, data: Dataset) -> PanelSample:
    """Learn the missing-predictor selection on these fitting rows, then keep complete cases on the kept predictors, then only hospitals with repeated windows.

    Rows outside ``data`` never enter the selection, so validation and holdout rows cannot change which predictors the fit keeps.
    """
    declared = request.features_for("m6_fixed_effects")
    if not data.rows:
        raise NotEstimable("panel has no complete repeated training hospitals")
    kept, rates = missing_selection(data.rows, declared, (request.cmi_reference,))
    features = tuple(feature for feature in declared if feature.reference in kept)
    complete, incomplete = complete_case_positions(data.rows, tuple(feature.name for feature in features))
    selection = {
        "learned_on": "fitting_rows",
        "selection_rows": len(data.rows),
        "maximum_missing_fraction": MAX_MISSING_FRACTION,
        "declared_count": len(declared),
        "kept_references": list(kept),
        "kept": [{"reference": reference, "missing_fraction": rates[reference]} for reference in kept],
        "dropped": [{"reference": feature.reference, "missing_fraction": rates[feature.reference]} for feature in declared if feature.reference not in kept],
    }
    data = data.subset(complete)
    positions, singletons = repeated_hospital_positions(data.rows)
    data = data.subset(positions)
    if not data.rows:
        raise NotEstimable("panel has no complete repeated training hospitals")
    return PanelSample(data, features, selection, incomplete, singletons)


def _fit_pipeline(request: ModelRequest, slot: str, parameters: Mapping[str, Any], forest: bool, training: Dataset) -> tuple[Any, PanelSample | None]:
    sample = None
    features = None
    if slot == "m6_fixed_effects":
        sample = _panel_rows(request, training)
        training, features = sample.data, sample.features
    request.deadline.check()
    pipeline = _pipeline(request, slot, parameters, forest, features)
    pipeline.fit(training.rows, training.y)
    request.deadline.check()
    return pipeline, sample


def _tune(request: ModelRequest, slot: str, forest: bool, training: Dataset) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    config = request.config["slots"][slot]
    options = _grid(config["check"]["grid"] if forest else config["grid"])
    if slot not in ("m2_elastic_net", "m4_boosting", "m6_fixed_effects"):
        return options[0], []
    folds = (
        time_folds(training, TIME_ORDERED_MINIMUM[slot])
        if slot in TIME_ORDERED_MINIMUM
        else folds_for(training, request.config, seed_for(request.config["master_seed"], request.data.hai_type, "folds"))
    )
    if not folds and slot == "m2_elastic_net":
        raise NotEstimable("no time-ordered validation fold: the training years leave no year to validate after the first")
    if not folds:
        return options[0], [{"status": "not_estimable", "reason": "no time-ordered validation fold after three fitting years"}]
    cached: list[tuple[Prepared, Array, Prepared, Array, dict[str, Any]]] = []
    dropped: list[dict[str, Any]] = []
    for left, right in folds:
        request.deadline.check()
        fitting, validation = training.subset(left), training.subset(right)
        features = None
        if slot == "m6_fixed_effects":
            sample = _panel_rows(request, fitting)
            fitting, features = sample.data, sample.features
            years = len({row["window_year"] for row in fitting.rows})
            if years < MINIMUM_FITTING_YEARS:
                reason = f"panel requires at least {MINIMUM_FITTING_YEARS} fitting years after the complete-case and repeated-hospital filters, found {years}"
                dropped.append(
                    {"status": "fold_dropped", "validation_year": max(row["window_year"] for row in validation.rows), "fitting_years": years, "reason": reason}
                )
                continue
        prepare = _pipeline(request, slot, options[0], forest, features).named_steps["prepare"]
        fitting_design = prepare.fit_transform(fitting.rows)
        validation_design = prepare.transform(validation.rows)
        record = {
            "fitting_rows": len(fitting.rows),
            "validation_rows": len(validation.rows),
            "selected_references": list(prepare.kept_references_),
            "indicator_aliases": dict(prepare.indicator_aliases_),
            "constant_columns": dict(prepare.constant_columns_),
            "dependent_columns": list(prepare.dependent_records_),
        }
        cached.append((fitting_design, fitting.y, validation_design, validation.y, record))
        request.deadline.check()
    if not cached:
        return options[0], [
            {"status": "not_estimable", "reason": "no time-ordered validation fold keeps three fitting years after the panel filters"},
            *dropped,
        ]
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
    return options[best[1]], [*audit, *dropped]


def _fit_slot(request: ModelRequest, slot: str, forest: bool, parts: tuple[Dataset, Dataset] | None = None) -> SlotResult:
    training, holdout = request.data.split() if parts is None else parts
    positions = tuple(range(len(holdout.rows)))
    if slot == "m3_mixed":
        training = training.subset([index for index, row in enumerate(training.rows) if not missing(row.get("control_type_code"))])
        positions = tuple(index for index, row in enumerate(holdout.rows) if not missing(row.get("control_type_code")))
        holdout = holdout.subset(positions)
        if not training.rows or not holdout.rows:
            raise NotEstimable("mixed model has no ownership-eligible training or holdout windows")
    parameters, audit = _tune(request, slot, forest, training)
    pipeline, sample = _fit_pipeline(request, slot, parameters, forest, training)
    prediction = np.asarray(pipeline.predict(holdout.rows), dtype=np.float64).reshape(-1)
    scored = metrics(holdout.y, prediction)
    request.deadline.check()
    prepare, model = pipeline.named_steps["prepare"], pipeline.named_steps["model"]
    panel_columns = model.kept_indices_ if slot == "m6_fixed_effects" else None
    details = {
        **model.details_,
        "training_rows": len(training.rows),
        "holdout_rows": len(holdout.rows),
        "selected_references": list(prepare.kept_references_),
        "used_references": list(prepare.used_references(training.rows[:1], panel_columns)),
        "training_missing_fraction": prepare.missing_rates_,
        "indicator_aliases": dict(prepare.indicator_aliases_),
        "constant_columns": dict(prepare.constant_columns_),
        "dependent_columns": list(prepare.dependent_records_),
        "dependent_column_count": len(prepare.dependent_records_),
        "unknown_holdout_categories": prepare.transform(holdout.rows).unknown_categories,
        "tuning": audit,
        "selected_parameters": parameters,
    }
    if slot == "m2_elastic_net":
        grid = request.config["slots"][slot]["grid"]
        details["grid_edge"] = any(parameters[name] in (grid[name][0], grid[name][-1]) for name in grid)
    if sample is not None:
        details["panel_selection"] = dict(sample.selection)
        details["dropped_incomplete_row_count"] = sample.incomplete
        details["panel_fitting_rows"] = len(sample.data.rows)
        details["dropped_singleton_hospital_count"] = len(sample.singletons)
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


def process_cmi_references(request: ModelRequest) -> tuple[str, ...]:
    """The columns of the RQ1 Elastic Net comparison: the Care Compare process measures of the joint test of RQ1, then CMI. Nothing else."""
    if not request.process_references:
        raise ModelError("the RQ1 comparison needs at least one process measure")
    return (*request.process_references, request.cmi_reference)


def _without_bounds(status: str, reason: str) -> dict[str, Any]:
    """An RQ1 result that has no CMI bounds: its status and reason, and a clipping record that claims no verified bounds and says why there are none."""
    clipping = {"both_sides": False, "learned_on": None, "lower": None, "upper": None, "reason": f"no matching bounds were established: {reason}"}
    return {"status": status, "reason": reason, "cmi_clipping": clipping}


def process_cmi_comparison(request: ModelRequest) -> dict[str, Any]:
    """Fit the Elastic Net on the process measures and CMI alone, with the tuning and winsorizing of m2, and score it against the intercept-plus-CMI model.

    Both sides clip CMI at the same training-row bounds. Every result, fitted or not, carries the ``cmi_clipping`` record that states them or says why
    there are none.
    """
    try:
        references = process_cmi_references(request)
        fitted = fit_slot(replace(request, feature_lists={**request.feature_lists, "m2_elastic_net": references}), "m2_elastic_net")
        if fitted.status != "fitted":
            return _without_bounds(fitted.status, fitted.reason)
        bounds = fitted.pipeline.named_steps["prepare"].bounds_
        return research_tests.intercept_cmi_comparison(request, fitted.predictions, references, fitted.details["selected_parameters"], bounds)
    except DeadlineExceeded:
        raise
    except (ModelError, np.linalg.LinAlgError) as error:
        return _without_bounds("not_estimable", str(error))


def validation_split(data: Dataset, last_year: int, years: Sequence[int]) -> tuple[Dataset, Dataset]:
    """Return the windows through last_year as training data and the named later windows as the rows to predict."""
    rows = data.rows
    return data.subset([i for i, row in enumerate(rows) if row["window_year"] <= last_year]), data.subset(
        [i for i, row in enumerate(rows) if row["window_year"] in years]
    )


def fit_validation(request: ModelRequest, slot: str, last_year: int, years: Sequence[int]) -> SlotResult:
    """Refit one slot's specification on the windows through last_year and predict the named later windows, for validation only.

    The slot tunes inside the shorter training set with its own grid, so no label of a predicted window reaches the fit.
    The fitted pipeline is dropped: nothing here is saved, compared, promoted or counted in a research result.
    """
    if slot not in SLOTS:
        raise ModelError("unknown model slot")
    parts = validation_split(request.data, last_year, years)
    minimum = request.config["minimum_rows"]
    if len(parts[0].rows) < minimum["training"] or len(parts[1].rows) < minimum["holdout"]:
        raise ModelError(f"insufficient validation rows: training={len(parts[0].rows)}, predicted={len(parts[1].rows)}")
    operation = replace(request, deadline=request.deadline.within(request.config["slots"][slot]["budget_seconds"]))
    try:
        result = _fit_slot(operation, slot, False, parts)
    except NotConverged as error:
        result = SlotResult(slot, "not_converged", str(error))
    except (NotEstimable, np.linalg.LinAlgError) as error:
        result = SlotResult(slot, "not_estimable", str(error))
    except DeadlineExceeded:
        result = SlotResult(slot, "not_run", "deadline")
    result.pipeline = None
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
    sides: Mapping[str, Any] = field(default_factory=dict)
    model_slot: str | None = None

    def fitted_slot(self) -> str | None:
        """The slot of the model this run fitted, registrable or not, or None when the run fitted none. A per-model run names its slot (``model_slot``); a
        suite of all six slots has the registry candidate."""
        slot = self.model_slot or self.candidate
        return slot if slot is not None and self.slots[slot].status == "fitted" else None

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
    research["process_vs_intercept_cmi"] = _optional_analysis(lambda: process_cmi_comparison(request))
    research["ownership"] = {"status": result["m3_mixed"].status, "reason": result["m3_mixed"].reason, **result["m3_mixed"].details}
    research["hospital_effects"] = result["m6_fixed_effects"].details.get(
        "mundlak", {"status": result["m6_fixed_effects"].status, "reason": result["m6_fixed_effects"].reason}
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


NOT_IN_RUN = "not part of this model's run"


def _spatial(request: ModelRequest) -> tuple[SlotResult, dict[str, Any]]:
    """Test the holdout residuals of a non-spatial fit for spatial autocorrelation, then fit the spatial model if it is significant.

    The reference fit is an OLS on the spatial list, because a model run holds no other model's residuals.
    """
    _, holdout = request.data.split()
    reference = fit_slot(replace(request, feature_lists={**request.feature_lists, "m1_ols": request.feature_lists["m5_spatial"]}), "m1_ols")
    if reference.status != "fitted":
        reason = f"the non-spatial reference fit ended {reference.status}: {reference.reason}"
        return SlotResult("m5_spatial", "not_run", reason), {"status": "not_run", "reason": reason}
    sample = holdout.subset(reference.holdout_positions)
    graph = build_weights(sample.rows, request.adjacency)
    test = moran_test(
        sample.y - np.asarray(reference.predictions),
        sample.keys,
        graph,
        seed_for(request.config["master_seed"], request.data.hai_type, "moran"),
        request.config["moran"]["permutations"],
        request.deadline,
    )
    result = {**test, "residual_model": "ols_on_the_spatial_list"}
    if test["status"] == "fitted" and test["p_value"] < 0.05:
        return fit_slot(request, "m5_spatial"), result
    return SlotResult("m5_spatial", "not_run", "Moran's I not significant" if test["status"] == "fitted" else test["reason"]), result


def fit_model(request: ModelRequest, model: str) -> ModelSuite:
    """Fit one model of the per-model design and run only the tests it owns.

    m1 runs the joint test of the process measures and fits the reduced side of RQ4. m2 compares an Elastic Net over the process measures and CMI with the
    intercept and CMI model and fits the full side of RQ4. m3 reports its ownership test, m4 its SHAP values, m5 the RQ5 test and m6 the RQ6 test.
    Every other slot stays ``not_run``.
    """
    if model not in MODEL_SLOT:
        raise ModelError("unknown model")
    request.deadline.check()
    training, holdout = request.data.split()
    minimum = request.config["minimum_rows"]
    if len(training.rows) < minimum["training"] or len(holdout.rows) < minimum["holdout"]:
        raise ModelError(f"insufficient split rows: training={len(training.rows)}, holdout={len(holdout.rows)}")
    if request.threads < 1:
        raise ModelError("training thread count must be positive")
    slot = MODEL_SLOT[model]
    slots = {name: SlotResult(name, "not_run", NOT_IN_RUN) for name in SLOTS}
    research: dict[str, Any] = {}
    sides: dict[str, Any] = {}
    shap: dict[str, Any] = {"status": "not_run", "reason": NOT_IN_RUN}
    if model == "m5":
        slots[slot], research["spatial"] = _spatial(request)
    else:
        slots[slot] = fit_slot(request, slot)
    fitted = slots[slot]
    unfitted = {"status": "not_estimable", "reason": fitted.reason}
    if model == "m1":
        research["joint_process"] = (
            _optional_analysis(lambda: research_tests.joint_significance(fitted.pipeline, request.process_references))
            if fitted.status == "fitted"
            else unfitted
        )
        sides["rq4_reduced"] = _optional_analysis(lambda: research_tests.rq4_side(request, "reduced"))
    elif model == "m2":
        research["process_vs_intercept_cmi"] = _optional_analysis(lambda: process_cmi_comparison(request))
        sides["rq4_full"] = _optional_analysis(lambda: research_tests.rq4_side(request, "full"))
    elif model == "m3":
        research["ownership"] = {"status": fitted.status, "reason": fitted.reason, **fitted.details}
    elif model == "m4" and fitted.status == "fitted":
        shap = _optional_analysis(
            lambda: shap_values(
                fitted.pipeline,
                holdout.rows,
                seed_for(request.config["master_seed"], request.data.hai_type, "shap"),
                request.config["shap"]["sample_cap"],
                request.deadline.within(request.config["budgets"]["shap_seconds"]),
            )
        )
    elif model == "m6":
        research["hospital_effects"] = fitted.details.get("mundlak", {"status": fitted.status, "reason": fitted.reason})
    request.deadline.check()
    return ModelSuite(request, slots, select(slots, observed=holdout.y), select(slots, registrable=True, observed=holdout.y), research, shap, sides, slot)
