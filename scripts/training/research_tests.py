"""Joint, nested and panel research statistics on explicitly matched samples."""

from __future__ import annotations

import importlib
import math
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from scripts.training.guards import CMI_COLUMNS
from scripts.training.models.common import Array, Deadline, ModelError, ModelRequest, NotEstimable, metrics, seed_for
from scripts.training.models.linear import OLSRegressor
from scripts.training.models.preprocessing import FoldPreprocessor


def hospital_resamples(entities: Sequence[str], draws: int, seed: int) -> Any:
    """Yield paired row positions from fixed-count seeded hospital bootstrap draws."""
    if draws < 1 or not entities:
        raise ModelError("hospital bootstrap requires rows and positive draws")
    positions: dict[str, list[int]] = {}
    for index, entity in enumerate(entities):
        positions.setdefault(entity, []).append(index)
    groups = {entity: np.asarray(values, dtype=np.intp) for entity, values in sorted(positions.items())}
    names = sorted(groups)
    rng = np.random.default_rng(seed)
    for _ in range(draws):
        yield np.concatenate([groups[names[int(index)]] for index in rng.integers(0, len(names), size=len(names))])


def improvement_interval(
    observed: Array, reduced: Array, full: Array, entities: Sequence[str], draws: int, seed: int, confidence: float, deadline: Deadline
) -> dict[str, Any]:
    """Compare paired holdout RMSEs with a hospital-level percentile bootstrap interval."""
    if not 0 < confidence < 1 or len(entities) != len(observed):
        raise ModelError("paired bootstrap rows or confidence level differ")
    metrics(observed, reduced)
    metrics(observed, full)
    values = []
    for positions in hospital_resamples(entities, draws, seed):
        deadline.check()
        reduced_error = observed[positions] - reduced[positions]
        full_error = observed[positions] - full[positions]
        values.append(float(np.sqrt(np.mean(reduced_error**2)) - np.sqrt(np.mean(full_error**2))))
    tail = (1 - confidence) / 2
    interval = np.quantile(values, [tail, 1 - tail], method="linear").tolist()
    return {
        "interval": interval,
        "bootstrap": {
            "unit": "hospital",
            "paired": True,
            "draws": draws,
            "seed": seed,
            "generator": "numpy.random.default_rng",
            "interval_method": "percentile",
            "confidence_level": confidence,
        },
    }


def joint_significance(pipeline: Any, process_references: Sequence[str]) -> dict[str, Any]:
    """Test all retained process and structure columns jointly in the fitted baseline OLS."""
    prepare, model = pipeline.named_steps["prepare"], pipeline.named_steps["model"]
    indices = [index + 1 for index, references in enumerate(model.column_references_) if set(references).intersection(process_references)]
    if not indices:
        return {"status": "not_estimable", "reason": "no retained process or structure coefficients"}
    restrictions = np.zeros((len(indices), len(prepare.output_names_) + 1))
    restrictions[np.arange(len(indices)), indices] = 1.0
    covariance = restrictions @ model.covariance_ @ restrictions.T
    if np.linalg.matrix_rank(covariance) != len(indices):
        raise NotEstimable("joint F test covariance is singular")
    delta = restrictions @ model.coefficients_
    statistic = float(delta @ np.linalg.solve(covariance, delta) / len(indices))
    p_value = float(importlib.import_module("scipy.stats").f.sf(statistic, len(indices), model.residual_df_))
    if not math.isfinite(statistic) or not math.isfinite(p_value):
        raise NotEstimable("joint F test is not finite")
    return {
        "status": "fitted",
        "f_statistic": statistic,
        "p_value": p_value,
        "numerator_degrees_of_freedom": len(indices),
        "denominator_degrees_of_freedom": model.residual_df_,
    }


def intercept_cmi_predictions(request: ModelRequest) -> Array:
    """Fit the intercept-plus-CMI model on the training years and predict the newest-year holdout rows, in their extract order."""
    training, holdout = request.data.split()
    features = tuple(feature for feature in request.data.features if feature.reference == request.cmi_reference)
    if len(features) != 1:
        raise ModelError("intercept comparison requires one registered CMI reference")
    transform = FoldPreprocessor(features, forced=(request.cmi_reference,))
    design = transform.fit_transform(training.rows)
    baseline = OLSRegressor().fit(design, training.y)
    request.deadline.check()
    return np.asarray(baseline.predict(transform.transform(holdout.rows)), dtype=np.float64).reshape(-1)


def intercept_cmi_comparison(request: ModelRequest, elastic_predictions: tuple[float, ...]) -> dict[str, Any]:
    """Score the Elastic Net against an intercept-plus-CMI model on the same holdout rows."""
    holdout = request.data.split()[1]
    baseline_metric = metrics(holdout.y, intercept_cmi_predictions(request))["rmse"]
    expanded_metric = metrics(holdout.y, np.asarray(elastic_predictions))["rmse"]
    return {
        "status": "fitted",
        "intercept_cmi_rmse": baseline_metric,
        "elastic_net_rmse": expanded_metric,
        "rmse_improvement": float(baseline_metric) - float(expanded_metric),
        "holdout_rows": len(holdout.rows),
    }


def nested_comparison(request: ModelRequest) -> dict[str, Any]:
    """Fit nested OLS designs from one fold-fitted matrix and compare matched holdout predictions."""
    request.deadline.check()
    reduced = request.feature_lists["rq4_reduced"]
    full = request.feature_lists["m2_elastic_net"]
    if not set(reduced).issubset(full):
        raise ModelError("reduced predictor list is not nested in the full predictor list")
    if request.cmi_reference not in full:
        raise ModelError("nested full list omits CMI")
    if any(
        feature.reference == request.cmi_reference
        or feature.family not in ("care_compare", "hospital_window")
        or feature.family == "hospital_window"
        and feature.name in CMI_COLUMNS
        for feature in request.features_for("rq4_reduced")
    ):
        raise ModelError("nested reduced list must be CMS-only without CMI or other full-model additions")
    training, holdout = request.data.split()
    transform = FoldPreprocessor(request.features_for("m2_elastic_net"), forced=(request.cmi_reference,), drop_dependent=True, protected=tuple(reduced))
    full_design = transform.fit_transform(training.rows)
    full_holdout = transform.transform(holdout.rows)
    selected = list(full_design.columns_for(reduced))
    reduced_design = type(full_design)(
        full_design.matrix[:, selected],
        tuple(full_design.names[index] for index in selected),
        tuple(full_design.references[index] for index in selected),
        full_design.rows,
    )
    reduced_holdout = type(full_holdout)(full_holdout.matrix[:, selected], reduced_design.names, reduced_design.references, full_holdout.rows)
    left, right = OLSRegressor().fit(reduced_design, training.y), OLSRegressor().fit(full_design, training.y)
    request.deadline.check()
    difference = len(full_design.names) - len(reduced_design.names)
    if difference <= 0 or right.ssr_ <= 0:
        raise NotEstimable("nested models have no extra estimable coefficients or positive residual variance")
    f_statistic = max(0.0, float((left.ssr_ - right.ssr_) / difference / (right.ssr_ / right.residual_df_)))
    lr_statistic = max(0.0, 2 * float(right.log_likelihood_ - left.log_likelihood_))
    distribution = importlib.import_module("scipy.stats")
    left_prediction, right_prediction = np.asarray(left.predict(reduced_holdout)), np.asarray(right.predict(full_holdout))
    left_metrics, right_metrics = metrics(holdout.y, left_prediction), metrics(holdout.y, right_prediction)
    settings = request.config["bootstrap"]["nested_comparison"]
    interval = improvement_interval(
        holdout.y,
        left_prediction,
        right_prediction,
        tuple(row["ccn"] for row in holdout.rows),
        settings["draws"],
        seed_for(request.config["master_seed"], request.data.hai_type, "nested_bootstrap"),
        settings["confidence_level"],
        request.deadline.within(request.config["budgets"]["bootstrap_seconds"]),
    )
    return {
        "status": "fitted",
        "f_statistic": f_statistic,
        "f_p_value": float(distribution.f.sf(f_statistic, difference, right.residual_df_)),
        "likelihood_ratio": lr_statistic,
        "likelihood_ratio_p_value": float(distribution.chi2.sf(lr_statistic, difference)),
        "extra_degrees_of_freedom": difference,
        "training_rows": len(training.rows),
        "holdout_rows": len(holdout.rows),
        "reduced_references": list(reduced),
        "full_references": list(full),
        "reduced_references_fitted": sorted(set(reduced_design.references)),
        "dependent_columns": list(transform.dependent_records_),
        "dependent_column_count": len(transform.dependent_records_),
        "cmi_in_reduced": False,
        "reduced_rmse": left_metrics["rmse"],
        "full_rmse": right_metrics["rmse"],
        "rmse_improvement": float(left_metrics["rmse"]) - float(right_metrics["rmse"]),
        **interval,
    }


def rq4_side(request: ModelRequest, side: str) -> dict[str, Any]:
    """Fit one side of the nested RQ4 comparison as an OLS on the training years and keep what the research job needs to compare it with the other side.

    The reduced side takes the CMS-only list. The full side takes the Elastic Net list with CMI forced in and the reduced columns protected from the
    dependent-column drop. Both fit the same training rows, so the two sides run in different model runs and still compare.
    """
    request.deadline.check()
    reduced = request.feature_lists["rq4_reduced"]
    if side == "reduced":
        transform = FoldPreprocessor(request.features_for("rq4_reduced"), drop_dependent=True)
    elif side == "full":
        full = request.feature_lists["m2_elastic_net"]
        if not set(reduced).issubset(full) or request.cmi_reference not in full:
            raise ModelError("the full list must contain the reduced list and CMI")
        transform = FoldPreprocessor(request.features_for("m2_elastic_net"), forced=(request.cmi_reference,), drop_dependent=True, protected=tuple(reduced))
    else:
        raise ModelError("unknown RQ4 side")
    training, holdout = request.data.split()
    design = transform.fit_transform(training.rows)
    fitted = OLSRegressor().fit(design, training.y)
    request.deadline.check()
    prediction = np.asarray(fitted.predict(transform.transform(holdout.rows)), dtype=np.float64)
    return {
        "status": "fitted",
        "side": side,
        "training_rows": len(training.rows),
        "holdout_rows": len(holdout.rows),
        "columns": len(design.names),
        "ssr": fitted.ssr_,
        "residual_degrees_of_freedom": fitted.residual_df_,
        "log_likelihood": fitted.log_likelihood_,
        "references_fitted": sorted(set(design.references)),
        "dependent_column_count": len(transform.dependent_records_),
        "holdout": [
            {"spine_key": key, "ccn": row["ccn"], "observed": float(observed), "predicted": float(value)}
            for key, row, observed, value in zip(holdout.keys, holdout.rows, holdout.y, prediction, strict=True)
        ],
    }


def rq4_compare(reduced: Mapping[str, Any], full: Mapping[str, Any], settings: Mapping[str, Any], seed: int, deadline: Deadline) -> dict[str, Any]:
    """Compare the reduced and the full RQ4 fit of two model runs: in-sample F and likelihood-ratio tests and the holdout RMSE difference with its interval.

    Both sides must hold the same training rows and the same holdout windows, and the reduced columns must be among the full columns.
    """
    deadline.check()
    if reduced.get("status") != "fitted" or full.get("status") != "fitted":
        return {"status": "not_estimable", "reason": f"reduced side {reduced.get('status')}, full side {full.get('status')}"}
    keys = [row["spine_key"] for row in reduced["holdout"]]
    if reduced["training_rows"] != full["training_rows"] or keys != [row["spine_key"] for row in full["holdout"]]:
        raise ModelError("the two sides of RQ4 hold other training rows or other holdout windows")
    if not set(reduced["references_fitted"]).issubset(full["references_fitted"]):
        return {"status": "not_estimable", "reason": "the reduced columns are not among the full columns"}
    difference = full["columns"] - reduced["columns"]
    if difference <= 0 or full["ssr"] <= 0:
        return {"status": "not_estimable", "reason": "nested models have no extra estimable coefficients or positive residual variance"}
    f_statistic = max(0.0, float((reduced["ssr"] - full["ssr"]) / difference / (full["ssr"] / full["residual_degrees_of_freedom"])))
    lr_statistic = max(0.0, 2 * float(full["log_likelihood"] - reduced["log_likelihood"]))
    distribution = importlib.import_module("scipy.stats")
    observed = np.asarray([row["observed"] for row in reduced["holdout"]], dtype=np.float64)
    left, right = (np.asarray([row["predicted"] for row in side["holdout"]], dtype=np.float64) for side in (reduced, full))
    left_metrics, right_metrics = metrics(observed, left), metrics(observed, right)
    interval = improvement_interval(
        observed, left, right, tuple(row["ccn"] for row in reduced["holdout"]), settings["draws"], seed, settings["confidence_level"], deadline
    )
    return {
        "status": "fitted",
        "f_statistic": f_statistic,
        "f_p_value": float(distribution.f.sf(f_statistic, difference, full["residual_degrees_of_freedom"])),
        "likelihood_ratio": lr_statistic,
        "likelihood_ratio_p_value": float(distribution.chi2.sf(lr_statistic, difference)),
        "extra_degrees_of_freedom": difference,
        "training_rows": reduced["training_rows"],
        "holdout_rows": len(keys),
        "reduced_rmse": left_metrics["rmse"],
        "full_rmse": right_metrics["rmse"],
        "rmse_improvement": float(left_metrics["rmse"]) - float(right_metrics["rmse"]),
        **interval,
    }


def hausman(fixed: Array, fixed_covariance: Array, random: Array, random_covariance: Array, names: tuple[str, ...]) -> dict[str, Any]:
    """Compare ordered slope estimates without inverting singular or indefinite covariance differences."""
    fixed, random = np.asarray(fixed, dtype=np.float64), np.asarray(random, dtype=np.float64)
    covariance = np.asarray(fixed_covariance) - np.asarray(random_covariance)
    if fixed.shape != random.shape or fixed.shape != (len(names),) or covariance.shape != (len(names), len(names)) or not names:
        raise ModelError("Hausman estimates differ in coefficient identity or covariance shape")
    if not np.isfinite(covariance).all() or not np.isfinite(fixed).all() or not np.isfinite(random).all():
        return {"status": "not_estimable", "reason": "Hausman estimates are not finite"}
    if not np.allclose(covariance, covariance.T, rtol=1e-8, atol=1e-10) or np.min(np.linalg.eigvalsh(covariance)) <= 1e-12:
        return {"status": "not_estimable", "reason": "Hausman covariance difference is singular or indefinite", "coefficients": list(names)}
    delta = fixed - random
    statistic = float(delta @ np.linalg.solve(covariance, delta))
    return {
        "status": "fitted",
        "statistic": statistic,
        "p_value": float(importlib.import_module("scipy.stats").chi2.sf(statistic, len(names))),
        "degrees_of_freedom": len(names),
        "coefficients": list(names),
    }
