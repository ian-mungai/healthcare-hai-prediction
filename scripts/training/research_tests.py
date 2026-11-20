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


def intercept_cmi_fit(request: ModelRequest, winsorize: bool = False) -> tuple[Array, FoldPreprocessor]:
    """Fit the intercept-plus-CMI model on the training years and predict the newest-year holdout rows, in their extract order.

    With ``winsorize`` the CMI is clipped at the 1st and 99th percentiles of the training rows, as the Elastic Net does, and the same bounds clip the holdout
    rows. Returns the predictions and the fitted preprocessor, which holds the bounds.
    """
    training, holdout = request.data.split()
    features = tuple(feature for feature in request.data.features if feature.reference == request.cmi_reference)
    if len(features) != 1:
        raise ModelError("intercept comparison requires one registered CMI reference")
    transform = FoldPreprocessor(features, forced=(request.cmi_reference,), winsorize=winsorize)
    design = transform.fit_transform(training.rows)
    baseline = OLSRegressor().fit(design, training.y)
    request.deadline.check()
    return np.asarray(baseline.predict(transform.transform(holdout.rows)), dtype=np.float64).reshape(-1), transform


def intercept_cmi_predictions(request: ModelRequest) -> Array:
    """The intercept-plus-CMI predictions on the raw CMI, as the promotion check scores them."""
    return intercept_cmi_fit(request)[0]


def intercept_cmi_comparison(
    request: ModelRequest,
    elastic_predictions: tuple[float, ...],
    references: Sequence[str],
    parameters: Mapping[str, Any],
    elastic_bounds: Mapping[str, tuple[float, float]],
) -> dict[str, Any]:
    """Score an Elastic Net over the process measures and CMI alone against an intercept-plus-CMI model on the same holdout rows.

    ``elastic_predictions`` come from the fit on the declared ``references`` with the tuned ``parameters``. The record lists the declared inputs, and the fit's
    missing-value selection can still drop some. Both sides clip CMI at the same bounds, learned on the training rows: ``elastic_bounds`` are the cut points
    of the Elastic Net's final fit, and the baseline learns its own from the same rows with the same code. The record states them.
    """
    holdout = request.data.split()[1]
    baseline, transform = intercept_cmi_fit(request, winsorize=True)
    bounds = transform.bounds_.get(request.cmi_reference)
    if bounds != elastic_bounds.get(request.cmi_reference):
        raise ModelError("the two sides of the RQ1 comparison clip CMI at different bounds")
    baseline_metric = metrics(holdout.y, baseline)["rmse"]
    expanded_metric = metrics(holdout.y, np.asarray(elastic_predictions))["rmse"]
    return {
        "status": "fitted",
        "intercept_cmi_rmse": baseline_metric,
        "elastic_net_rmse": expanded_metric,
        "rmse_improvement": float(baseline_metric) - float(expanded_metric),
        "holdout_rows": len(holdout.rows),
        "elastic_net_references": list(references),
        "elastic_net_alpha": float(parameters["alpha"]),
        "elastic_net_l1_ratio": float(parameters["l1_ratio"]),
        "cmi_clipping": {
            "both_sides": True,
            "learned_on": "training_rows",
            "lower": None if bounds is None else bounds[0],
            "upper": None if bounds is None else bounds[1],
        },
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


MUNDLAK_MINIMUM_CLUSTERS = 50
YEAR_EFFECT_PREFIX = "window_year__"


def cluster_covariances(y: Array, x: Array, groups: Array | Sequence[str]) -> tuple[Array, Array, Array]:
    """OLS coefficients with the hospital-clustered (CR1) covariance and, beside it, the unadjusted covariance of the same fit.

    The clustered covariance is (X'X)^-1 (sum over hospitals of X_g'u_g u_g'X_g) (X'X)^-1 times G/(G-1) * (N-1)/(N-K).
    It lets the windows of one hospital be correlated.
    """
    y, x = np.asarray(y, dtype=np.float64), np.asarray(x, dtype=np.float64)
    rows, columns = x.shape
    labels, positions = np.unique(np.asarray(groups), return_inverse=True)
    if y.shape != (rows,) or len(positions) != rows or len(labels) < 2 or rows <= columns:
        raise ModelError("clustered covariance needs matching rows, two hospitals and residual degrees of freedom")
    inverse = np.linalg.inv(x.T @ x)
    beta = inverse @ x.T @ y
    residual = y - x @ beta
    scores = np.zeros((len(labels), columns))
    np.add.at(scores, positions, x * residual[:, None])
    correction = len(labels) / (len(labels) - 1) * (rows - 1) / (rows - columns)
    clustered = correction * inverse @ (scores.T @ scores) @ inverse
    unadjusted = float(residual @ residual) / (rows - columns) * inverse
    return beta, clustered, unadjusted


def mundlak(y: Array, slopes: Array, names: Sequence[str], entities: Sequence[str], years: Sequence[int]) -> dict[str, Any]:
    """Mundlak test: do the hospital means of the time-varying slopes explain the outcome beyond the slopes themselves?

    The outcome is regressed on a constant, the time-varying predictors, their hospital means and a dummy for every window year after the earliest.
    The test is the joint Wald test W that the coefficients of the hospital means are zero, with hospital-clustered errors.
    Its reference is F = W / q on q and G - 1 degrees of freedom, with q the number of tested slopes and G the number of hospitals.
    A hospital effect correlated with a regressor makes it reject.
    Year-effect columns of the design are replaced by the dummies built here.
    """
    y, entity_labels, year_values = np.asarray(y, dtype=np.float64), np.asarray(entities), np.asarray(years)
    keep = [position for position, name in enumerate(names) if not name.startswith(YEAR_EFFECT_PREFIX)]
    slopes = np.asarray(slopes, dtype=np.float64)[:, keep]
    tested = [names[position] for position in keep]
    clusters = len(set(entity_labels.tolist()))
    if not tested or slopes.shape[0] != len(y) or len(entity_labels) != len(y) or len(year_values) != len(y):
        raise ModelError("Mundlak test needs time-varying slopes with matching rows, hospitals and years")
    base = {"clusters": clusters, "tested_columns": tested, "numerator_degrees_of_freedom": len(tested)}
    if clusters < MUNDLAK_MINIMUM_CLUSTERS:
        return {
            "status": "not_estimable",
            "reason": f"Mundlak test needs at least {MUNDLAK_MINIMUM_CLUSTERS} hospital clusters, found {clusters}",
            **base,
        }
    means = np.zeros_like(slopes)
    for label in sorted(set(entity_labels.tolist())):
        mask = entity_labels == label
        means[mask] = slopes[mask].mean(axis=0)
    later_years = sorted(set(year_values.tolist()))[1:]
    dummies = np.column_stack([(year_values == year).astype(np.float64) for year in later_years]) if later_years else np.empty((len(y), 0))
    design = np.column_stack((np.ones(len(y)), slopes, means, dummies))
    block = list(range(1 + len(tested), 1 + 2 * len(tested)))
    if not np.isfinite(design).all() or not np.isfinite(y).all():
        return {"status": "not_estimable", "reason": "Mundlak design contains a nonfinite value", **base}
    if design.shape[0] <= design.shape[1] or np.linalg.matrix_rank(design) != design.shape[1]:
        return {"status": "not_estimable", "reason": "Mundlak design is rank deficient or has no residual degrees of freedom", **base}
    beta, covariance, _ = cluster_covariances(y, design, entity_labels)
    gamma, variance = beta[block], covariance[np.ix_(block, block)]
    eigenvalues = np.linalg.eigvalsh((variance + variance.T) / 2)
    if not np.isfinite(eigenvalues).all() or eigenvalues[0] <= 1e-10 * max(eigenvalues[-1], 1e-300):
        return {"status": "not_estimable", "reason": "Mundlak clustered covariance of the hospital means is singular or indefinite", **base}
    statistic = float(gamma @ np.linalg.solve(variance, gamma))
    f_statistic = statistic / len(tested)
    return {
        "status": "fitted",
        "test": "mundlak",
        "statistic": statistic,
        "f_statistic": f_statistic,
        "numerator_degrees_of_freedom": len(tested),
        "denominator_degrees_of_freedom": clusters - 1,
        "p_value": float(importlib.import_module("scipy.stats").f.sf(f_statistic, len(tested), clusters - 1)),
        "clusters": clusters,
        "rows": len(y),
        "tested_columns": tested,
        "year_effect_years": later_years,
        "covariance": "cluster_robust_cr1",
        "reference_distribution": "f",
    }
