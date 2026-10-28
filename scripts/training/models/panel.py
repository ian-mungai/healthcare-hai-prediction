"""Hospital fixed effects with explicit unbalanced-panel and unseen-hospital handling."""

from __future__ import annotations

import importlib
import math
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np

from scripts.training.models.common import Array, ModelError, NotEstimable, Prepared, Regressor, full_rank, with_intercept


@dataclass(frozen=True)
class PanelFits:
    """Comparable fixed and random estimates on exactly the same panel design and rows."""

    fixed: Mapping[str, float]
    fixed_covariance: Array
    random: Mapping[str, float]
    random_covariance: Array
    effects: Mapping[str, float]
    log_likelihood: float | None = None


def native_fits(y: Array, matrix: Array, entities: tuple[str, ...], years: tuple[int, ...], names: tuple[str, ...]) -> PanelFits:
    """Fit the two pinned linearmodels estimators on one explicit entity-time MultiIndex."""
    pandas = importlib.import_module("pandas")
    api = importlib.import_module("linearmodels.panel")
    index = pandas.MultiIndex.from_arrays([entities, years], names=["ccn", "window_year"])
    x = pandas.DataFrame(matrix, index=index, columns=list(names))
    outcome = pandas.Series(y, index=index, name="sir")
    fixed = api.PanelOLS(outcome, x, entity_effects=True, drop_absorbed=True, check_rank=True).fit(cov_type="unadjusted", debiased=True)
    random = api.RandomEffects(outcome, x, check_rank=True).fit(cov_type="unadjusted", debiased=True)
    if fixed.nobs != len(y) or random.nobs != len(y) or tuple(fixed.params.index) != names or tuple(random.params.index) != names:
        raise ModelError("panel comparison changed its sample or coefficient identity")
    coefficients = np.asarray([fixed.params.to_dict()[name] for name in names], dtype=np.float64)
    residuals = y - matrix @ coefficients
    entity_values = np.asarray(entities)
    effects = {entity: float(np.mean(residuals[entity_values == entity])) for entity in sorted(set(entities))}
    return PanelFits(
        fixed.params.to_dict(),
        fixed.cov.loc[list(names), list(names)].to_numpy(),
        random.params.to_dict(),
        random.cov.loc[list(names), list(names)].to_numpy(),
        effects,
        float(fixed.loglik),
    )


def repeated_hospital_positions(rows: tuple[Mapping[str, Any], ...]) -> tuple[list[int], list[str]]:
    """Drop hospitals with only one training window before fitting their preprocessing."""
    counts = Counter(row["ccn"] for row in rows)
    return [index for index, row in enumerate(rows) if counts[row["ccn"]] > 1], sorted(key for key, count in counts.items() if count == 1)


class PanelRegressor(Regressor):
    """Fit time-varying hospital effects and retain a comparable random-effects estimate."""

    def __init__(self) -> None:
        pass

    def fit(self, design: Prepared, y: Array) -> PanelRegressor:
        """Identify absorbed columns and estimate the panel with explicit covariance settings."""
        from scripts.training.research_tests import hausman

        entities = tuple(row["ccn"] for row in design.rows)
        years = tuple(row["window_year"] for row in design.rows)
        unique = sorted(set(entities))
        if len(unique) < 2 or any(count < 2 for count in Counter(entities).values()):
            raise NotEstimable("panel requires repeated windows for at least two training hospitals")
        centered = design.matrix.copy()
        for entity in unique:
            mask = np.asarray(entities) == entity
            centered[mask] -= np.mean(centered[mask], axis=0)
        self.kept_indices_ = tuple(index for index in range(centered.shape[1]) if np.any(np.abs(centered[:, index]) > 1e-12))
        absorbed = [name for index, name in enumerate(design.names) if index not in self.kept_indices_]
        if not self.kept_indices_:
            raise NotEstimable("all panel predictors are absorbed by hospital effects: " + ", ".join(absorbed))
        within = centered[:, self.kept_indices_]
        if np.linalg.matrix_rank(within) != len(self.kept_indices_):
            raise NotEstimable("within-hospital panel design has deficient rank")
        matrix = with_intercept(design.matrix[:, self.kept_indices_])
        full_rank(matrix)
        if len(y) <= len(unique) + len(self.kept_indices_):
            raise NotEstimable("panel residual degrees of freedom are exhausted")
        names = ("intercept", *(design.names[index] for index in self.kept_indices_))
        result = native_fits(y, matrix, entities, years, names)
        if set(result.fixed) != set(names) or set(result.random) != set(names) or set(result.effects) != set(unique):
            raise ModelError("panel result changed its coefficient or hospital identity")
        self.coefficients_ = np.asarray([result.fixed[name] for name in names], dtype=np.float64)
        self.effects_ = dict(result.effects)
        self.mean_effect_ = float(np.mean(list(self.effects_.values())))
        if not np.isfinite(self.coefficients_).all() or not math.isfinite(self.mean_effect_):
            raise NotEstimable("panel estimates are not finite")
        comparison = hausman(
            np.asarray([result.fixed[name] for name in names[1:]]),
            result.fixed_covariance[1:, 1:],
            np.asarray([result.random[name] for name in names[1:]]),
            result.random_covariance[1:, 1:],
            names[1:],
        )
        self.details_ = {
            "coefficients": {name: float(result.fixed[name]) for name in names},
            "absorbed_columns": absorbed,
            "training_hospitals": len(unique),
            "training_hospitals_by_window_count": {str(windows): count for windows, count in sorted(Counter(Counter(entities).values()).items())},
            "hausman": comparison,
            "covariance": "unadjusted",
            "debiased": True,
            "unseen_effect": "mean_estimated_hospital_effect",
            "effect_definition": "mean_outcome_minus_intercept_and_slopes",
        }
        if result.log_likelihood is not None:
            parameter_count = len(names) + len(unique) - 1
            self.details_.update(aic=-2 * result.log_likelihood + 2 * parameter_count, bic=-2 * result.log_likelihood + math.log(len(y)) * parameter_count)
        return self

    def predict(self, design: Prepared) -> Any:
        """Score every row, including hospitals absent from the fitted panel."""
        effects = np.asarray([self.effects_.get(row["ccn"], self.mean_effect_) for row in design.rows], dtype=np.float64)
        return with_intercept(design.matrix[:, self.kept_indices_]) @ self.coefficients_ + effects
