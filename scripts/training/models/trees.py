"""Seeded gradient boosting, the forest check and bounded SHAP attribution."""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from scripts.training.models.common import Array, Deadline, ModelError, NotEstimable, Prepared, Regressor


class TreeRegressor(Regressor):
    """Keep native tree models behind the same fold-fitted prediction Pipeline."""

    def __init__(self, kind: str, seed: int, threads: int, parameters: Mapping[str, Any]) -> None:
        self.kind, self.seed, self.threads, self.parameters = kind, seed, threads, parameters

    def fit(self, design: Prepared, y: Array) -> TreeRegressor:
        """Fit only the chosen approved estimator with explicit random seed and thread count."""
        if self.threads < 1:
            raise ModelError("tree thread count must be positive")
        if self.kind == "xgboost":
            self.native_ = importlib.import_module("xgboost").XGBRegressor(
                **self.parameters,
                random_state=self.seed,
                n_jobs=self.threads,
                objective="reg:squarederror",
                tree_method="hist",
                verbosity=0,
            )
        elif self.kind == "random_forest":
            self.native_ = importlib.import_module("sklearn.ensemble").RandomForestRegressor(**self.parameters, random_state=self.seed, n_jobs=self.threads)
        else:
            raise ModelError("tree estimator is outside the approved model family")
        self.native_.fit(design.matrix, y)
        self.details_ = {"estimator": self.kind, "parameters": dict(self.parameters), "seed": self.seed, "threads": self.threads}
        return self

    def predict(self, design: Prepared) -> Any:
        """Score the same supplied rows with native numeric missing-value handling."""
        return self.native_.predict(design.matrix)


def shap_values(pipeline: Any, rows: Sequence[Mapping[str, Any]], seed: int, sample_cap: int, deadline: Deadline) -> dict[str, Any]:
    """Explain transformed holdout inputs and reject a failed prediction additivity check.

    Returns derived attributions, base values and feature names. It returns no source feature values or hospital identifiers.
    """
    deadline.check()
    if sample_cap < 1 or not rows:
        raise ModelError("SHAP requires a positive sample cap and holdout rows")
    positions = sorted(np.random.default_rng(seed).choice(len(rows), size=min(sample_cap, len(rows)), replace=False).tolist())
    sample = tuple(rows[index] for index in positions)
    design = pipeline.named_steps["prepare"].transform(sample)
    tree = pipeline.named_steps["model"].native_
    shap = importlib.import_module("shap")
    explainer_error = importlib.import_module("shap.utils._exceptions").ExplainerError
    try:
        explainer = shap.TreeExplainer(tree, feature_perturbation="tree_path_dependent", model_output="raw")
        deadline.check()
        values = np.asarray(explainer.shap_values(design.matrix, check_additivity=True), dtype=np.float64)
    except explainer_error as error:
        raise NotEstimable("SHAP explainer failed its native explanation contract") from error
    base = np.asarray(explainer.expected_value, dtype=np.float64)
    predicted = np.asarray(pipeline.predict(sample), dtype=np.float64)
    deadline.check()
    if values.shape != design.matrix.shape or base.size != 1 or not np.isfinite(values).all():
        raise ModelError("SHAP additivity requires a finite single-output attribution matrix")
    reconstructed = float(base.reshape(-1)[0]) + np.sum(values, axis=1)
    if not np.allclose(reconstructed, predicted, rtol=1e-5, atol=1e-6):
        raise ModelError("SHAP additivity differs from the fitted pipeline predictions")
    return {
        "status": "fitted",
        "seed": seed,
        "sample_count": len(sample),
        "sample_cap": sample_cap,
        "feature_names": list(design.names),
        "values": values.tolist(),
        "base_value": float(base.reshape(-1)[0]),
        "mean_absolute": np.mean(np.abs(values), axis=0).tolist(),
        "maximum_additivity_error": float(np.max(np.abs(reconstructed - predicted))),
        "relative_tolerance": 1e-5,
        "absolute_tolerance": 1e-6,
    }
