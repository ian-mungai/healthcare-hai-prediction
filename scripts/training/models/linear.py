"""Full-rank ordinary least squares with likelihood diagnostics."""

from __future__ import annotations

import importlib
from typing import Any

import numpy as np

from scripts.training.models.common import Array, Prepared, Regressor, full_rank, with_intercept


class OLSRegressor(Regressor):
    """Fit a QR-based likelihood model after checking rank and residual degrees of freedom."""

    def __init__(self) -> None:
        pass

    def fit(self, design: Prepared, y: Array) -> OLSRegressor:
        """Fit the supplied fold design and retain coefficient and information criteria."""
        matrix = with_intercept(design.matrix)
        full_rank(matrix)
        fitted = importlib.import_module("statsmodels.api").OLS(y, matrix).fit(method="qr")
        self.coefficients_ = np.asarray(fitted.params, dtype=np.float64)
        self.column_references_ = tuple(
            (reference, *design.reference_aliases.get(name, ())) for name, reference in zip(design.names, design.references, strict=True)
        )
        self.covariance_ = np.linalg.inv(matrix.T @ matrix) * float(fitted.scale)
        self.ssr_, self.log_likelihood_, self.residual_df_ = float(fitted.ssr), float(fitted.llf), float(fitted.df_resid)
        self.details_ = {
            "coefficients": dict(zip(("intercept", *design.names), self.coefficients_.tolist(), strict=True)),
            "aic": float(fitted.aic),
            "bic": float(fitted.bic),
            "log_likelihood": self.log_likelihood_,
            "residual_degrees_of_freedom": self.residual_df_,
        }
        return self

    def predict(self, design: Prepared) -> Any:
        """Predict every requested row with the fitted coefficient order."""
        return with_intercept(design.matrix) @ self.coefficients_
