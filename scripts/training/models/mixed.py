"""Ownership random-intercept models fitted by maximum likelihood."""

from __future__ import annotations

import importlib
import math
import warnings
from collections import defaultdict
from typing import Any

import numpy as np

from scripts.training.models.common import Array, NotConverged, Prepared, Regressor, full_rank, with_intercept
from scripts.training.models.preprocessing import missing


class MixedRegressor(Regressor):
    """Estimate ownership intercept variance and compare it with the same fixed-only design."""

    def __init__(self) -> None:
        pass

    def fit(self, design: Prepared, y: Array) -> MixedRegressor:
        """Fit ML, require usable ownership groups and expose ICC with a boundary-aware likelihood test."""
        hospitals: dict[str, set[str]] = defaultdict(set)
        for row in design.rows:
            if missing(row.get("control_type_code")):
                raise NotConverged("ownership group is missing for a training hospital")
            hospitals[str(row["control_type_code"])].add(row["ccn"])
        if len(hospitals) < 2 or any(len(values) < 2 for values in hospitals.values()):
            raise NotConverged("ownership needs at least two groups and two hospitals in every group")
        matrix = with_intercept(design.matrix)
        full_rank(matrix)
        groups = np.asarray([str(row["control_type_code"]) for row in design.rows])
        group_columns = np.column_stack([groups == group for group in sorted(hospitals)])
        if np.linalg.matrix_rank(np.column_stack((matrix, group_columns))) == matrix.shape[1]:
            raise NotConverged("ownership random intercepts are fully represented by fixed-effect columns")
        api = importlib.import_module("statsmodels.api")
        with warnings.catch_warnings(record=True) as observed:
            model = api.MixedLM(y, matrix, groups=groups).fit(reml=False, method="lbfgs", disp=False)
        variance, residual = float(np.asarray(model.cov_re)[0, 0]), float(model.scale)
        if not model.converged or any(item.category.__name__ == "ConvergenceWarning" for item in observed):
            raise NotConverged("ownership mixed model did not converge")
        if not math.isfinite(variance) or not math.isfinite(residual) or variance <= 1e-10 or residual <= 0:
            raise NotConverged("ownership mixed model has singular variance")
        pooled = api.OLS(y, matrix).fit(method="qr")
        statistic = max(0.0, 2 * (float(model.llf) - float(pooled.llf)))
        p_value = 1.0 if statistic == 0 else 0.5 * float(importlib.import_module("scipy.stats").chi2.sf(statistic, 1))
        self.coefficients_ = np.asarray(model.fe_params, dtype=np.float64)
        self.effects_ = {str(key): float(np.asarray(value).reshape(-1)[0]) for key, value in model.random_effects.items()}
        self.details_ = {
            "coefficients": dict(zip(("intercept", *design.names), self.coefficients_.tolist(), strict=True)),
            "ownership_variance": variance,
            "residual_variance": residual,
            "icc": variance / (variance + residual),
            "likelihood_ratio": statistic,
            "p_value": p_value,
            "null_distribution": "half_point_mass_zero_half_chi_squared_1",
            "reml": False,
            "ownership_groups": len(hospitals),
            "aic": float(model.aic),
            "bic": float(model.bic),
        }
        return self

    def predict(self, design: Prepared) -> Any:
        """Use a known ownership intercept or its zero population mean for an unseen group."""
        return with_intercept(design.matrix) @ self.coefficients_ + np.asarray(
            [self.effects_.get(str(row.get("control_type_code")), 0.0) for row in design.rows]
        )
