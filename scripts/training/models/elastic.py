"""Elastic Net with unpenalized forced covariates."""

from __future__ import annotations

import importlib
import warnings
from typing import Any

import numpy as np

from scripts.training.models.common import Array, NotConverged, Prepared, Regressor, full_rank, with_intercept


class ElasticRegressor(Regressor):
    """Penalize optional features after partialling out the intercept and forced CMI columns."""

    def __init__(self, alpha: float, l1_ratio: float, forced: tuple[str, ...], seed: int) -> None:
        self.alpha, self.l1_ratio, self.forced, self.seed = alpha, l1_ratio, forced, seed

    def fit(self, design: Prepared, y: Array) -> ElasticRegressor:
        """Fit unpenalized forced effects and a seeded Elastic Net on their orthogonal residuals."""
        self.forced_indices_ = design.columns_for(self.forced)
        self.optional_indices_ = tuple(i for i in range(design.matrix.shape[1]) if i not in self.forced_indices_)
        forced = with_intercept(design.matrix[:, self.forced_indices_])
        full_rank(forced)
        optional = design.matrix[:, self.optional_indices_]
        projection_y = np.linalg.lstsq(forced, y, rcond=None)[0]
        projection_x = np.linalg.lstsq(forced, optional, rcond=None)[0]
        residual_y, residual_x = y - forced @ projection_y, optional - forced @ projection_x
        beta = np.zeros(optional.shape[1])
        if optional.shape[1]:
            estimator = importlib.import_module("sklearn.linear_model").ElasticNet(
                alpha=self.alpha,
                l1_ratio=self.l1_ratio,
                fit_intercept=False,
                random_state=self.seed,
                max_iter=10000,
                tol=1e-7,
                selection="cyclic",
            )
            with warnings.catch_warnings(record=True) as observed:
                estimator.fit(residual_x, residual_y)
            if any(item.category.__name__ == "ConvergenceWarning" for item in observed) or np.max(estimator.n_iter_) >= 10000:
                raise NotConverged("Elastic Net did not converge inside its fixed iteration budget")
            beta = np.asarray(estimator.coef_, dtype=np.float64)
        self.optional_coefficients_ = beta
        self.forced_coefficients_ = np.linalg.lstsq(forced, y - optional @ beta, rcond=None)[0]
        coefficients = np.zeros(design.matrix.shape[1])
        coefficients[list(self.forced_indices_)] = self.forced_coefficients_[1:]
        coefficients[list(self.optional_indices_)] = beta
        self.details_ = {
            "coefficients": dict(zip(("intercept", *design.names), [float(self.forced_coefficients_[0]), *coefficients.tolist()], strict=True)),
            "forced_unpenalized": list(self.forced),
            "alpha": self.alpha,
            "l1_ratio": self.l1_ratio,
            "seed": self.seed,
        }
        return self

    def predict(self, design: Prepared) -> Any:
        """Combine forced and penalized coefficients without fitting anything on scoring rows."""
        return (
            with_intercept(design.matrix[:, self.forced_indices_]) @ self.forced_coefficients_
            + design.matrix[:, self.optional_indices_] @ self.optional_coefficients_
        )
