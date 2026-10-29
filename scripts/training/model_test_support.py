"""Explicit numerical stand-ins for synthetic model-caller checks, without native-library acceptance."""

from __future__ import annotations

import math
import sys
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import numpy as np


class Pipeline:
    """Run the supplied production transformers and estimator in their declared order."""

    def __init__(self, steps: list[tuple[str, Any]]) -> None:
        self.steps = steps
        self.named_steps = dict(steps)

    def fit(self, rows: Any, y: Any) -> Pipeline:
        for _, step in self.steps[:-1]:
            rows = step.fit_transform(rows, y)
        self.steps[-1][1].fit(rows, y)
        return self

    def predict(self, rows: Any) -> Any:
        for _, step in self.steps[:-1]:
            rows = step.transform(rows)
        return self.steps[-1][1].predict(rows)


class OLS:
    """Supply a full-rank synthetic likelihood fit using NumPy, with explicit native limits."""

    def __init__(self, y: Any, x: Any) -> None:
        self.y, self.x = np.asarray(y), np.asarray(x)

    def fit(self, method: str = "qr") -> Any:
        if method != "qr":
            raise AssertionError("OLS must request QR after its rank check")
        beta = np.linalg.solve(self.x.T @ self.x, self.x.T @ self.y)
        residual = self.y - self.x @ beta
        n, k = self.x.shape
        sse = max(float(residual @ residual), 1e-25)
        ll = -n / 2 * (math.log(2 * math.pi) + 1 + math.log(sse / n))

        def f_test(restrictions: Any) -> Any:
            r = np.asarray(restrictions)
            delta = r @ beta
            covariance = r @ np.linalg.inv(self.x.T @ self.x) @ r.T * (sse / (n - k))
            statistic = float(delta @ np.linalg.solve(covariance, delta) / len(r))
            return SimpleNamespace(fvalue=statistic, pvalue=math.exp(-max(0.0, statistic)), df_num=len(r), df_denom=n - k)

        return SimpleNamespace(
            params=beta,
            llf=ll,
            aic=-2 * ll + 2 * k,
            bic=-2 * ll + k * math.log(n),
            df_resid=n - k,
            ssr=sse,
            nobs=n,
            scale=sse / (n - k),
            f_test=f_test,
            predict=lambda values: np.asarray(values) @ beta,
        )


class ElasticNet:
    """Use ridge as an explicit tuning stand-in, never as an Elastic Net result."""

    def __init__(self, **kwargs: Any) -> None:
        self.parameters = kwargs

    def fit(self, x: Any, y: Any) -> ElasticNet:
        x = np.asarray(x)
        penalty = float(self.parameters["alpha"]) * len(x)
        self.coef_ = np.linalg.solve(x.T @ x + np.eye(x.shape[1]) * penalty, x.T @ y)
        self.n_iter_ = 2
        return self


class Tree:
    """Use a deterministic linear predictor to check tree wiring and attribution contracts."""

    def __init__(self, **kwargs: Any) -> None:
        self.parameters = kwargs

    def fit(self, x: Any, y: Any) -> Tree:
        x = np.nan_to_num(x)
        augmented = np.column_stack((np.ones(len(x)), x))
        beta = np.linalg.lstsq(augmented, y, rcond=None)[0]
        self.intercept_, self.coef_ = float(beta[0]), beta[1:]
        return self

    def predict(self, x: Any) -> Any:
        return self.intercept_ + np.nan_to_num(x) @ self.coef_


class GroupKFold:
    """Expose seeded hospital separation for the production caller."""

    def __init__(self, *, n_splits: int, shuffle: bool, random_state: int) -> None:
        if not shuffle:
            raise AssertionError("hospital folds must shuffle")
        self.folds, self.seed = n_splits, random_state

    def split(self, x: Any, y: Any, groups: Any) -> Any:
        unique = np.asarray(sorted(set(groups)))
        np.random.default_rng(self.seed).shuffle(unique)
        for batch in np.array_split(unique, self.folds):
            mask = np.isin(groups, batch)
            yield np.flatnonzero(~mask), np.flatnonzero(mask)


class W:
    """Keep the supplied weight IDs visible and construct row-standardized synthetic weights."""

    def __init__(self, neighbors: dict[str, list[str]], *, id_order: list[str], silence_warnings: bool) -> None:
        if not silence_warnings:
            raise AssertionError("spatial weight construction must silence warnings")
        self.neighbors, self.id_order = neighbors, id_order
        self.transform = "r"

    def full(self) -> tuple[Any, list[str]]:
        positions = {key: index for index, key in enumerate(self.id_order)}
        matrix = np.zeros((len(positions), len(positions)))
        for key, adjacent in self.neighbors.items():
            for neighbor in adjacent:
                matrix[positions[key], positions[neighbor]] = 1 / len(adjacent)
        return matrix, self.id_order

    @property
    def sparse(self) -> Any:
        """Expose a dense synthetic substitute for the native sparse weight matrix."""
        return self.full()[0]


class SpatialOLS:
    """Supply explicit LM alternatives so the caller can exercise both prediction paths."""

    def __init__(self, y: Any, x: Any, **kwargs: Any) -> None:
        self.lm_lag, self.lm_error = (12.0, 0.001), (1.0, 0.4)
        self.rlm_lag, self.rlm_error = (10.0, 0.002), (0.2, 0.7)


class SpatialLag:
    """Return a known coefficient order without claiming spatial likelihood estimation."""

    def __init__(self, y: Any, x: Any, weights: W, **kwargs: Any) -> None:
        matrix = np.column_stack((np.ones(len(x)), x))
        beta = np.linalg.lstsq(matrix, np.asarray(y).reshape(-1), rcond=None)[0]
        self.betas = np.r_[beta, 0.15].reshape(-1, 1)
        self.aic, self.schwarz = 10.0, 12.0


def _inverse_product(weights: Any, values: Any, parameter: float, **kwargs: Any) -> Any:
    return np.linalg.solve(np.eye(len(weights)) - parameter * weights, values)


def _moran(y: Any, weights: W, *, transformation: str, permutations: int, two_tailed: bool) -> Any:
    if transformation != "r" or not two_tailed:
        raise AssertionError("Moran caller changed row standardization or tail settings")
    y = np.asarray(y)
    matrix = weights.full()[0]
    centered = y - np.mean(y)

    def value(vector: Any) -> float:
        return float(len(y) / matrix.sum() * (vector @ matrix @ vector) / (vector @ vector))

    observed = value(centered)
    draws = np.asarray([value(np.random.permutation(centered)) for _ in range(permutations)])
    extreme = min(int(np.sum(draws >= observed)), int(np.sum(draws <= observed)))
    return SimpleNamespace(I=observed, p_sim=(extreme + 1) / (permutations + 1), sim=draws)


class MixedLM:
    """Expose ML fitting and ownership effects without claiming native convergence."""

    def __init__(self, y: Any, x: Any, *, groups: Any) -> None:
        self.y, self.x, self.groups = np.asarray(y), np.asarray(x), np.asarray(groups)

    def fit(self, *, reml: bool, method: str, disp: bool) -> Any:
        if reml or method != "lbfgs" or disp:
            raise AssertionError("mixed comparison must use maximum likelihood without verbose output")
        pooled = OLS(self.y, self.x).fit()
        residual = self.y - self.x @ pooled.params
        effects = {group: np.asarray([np.mean(residual[self.groups == group])]) for group in sorted(set(self.groups))}
        variance = max(float(np.var([value[0] for value in effects.values()])), 0.01)
        return SimpleNamespace(
            converged=True,
            fe_params=pooled.params,
            cov_re=np.asarray([[variance]]),
            scale=pooled.scale,
            random_effects=effects,
            llf=pooled.llf + 3,
            aic=pooled.aic - 4,
            bic=pooled.bic - 2,
        )


class ExplainerError(Exception):
    """Represent the native SHAP explanation exception at the library boundary."""


class TreeExplainer:
    """Return additive synthetic attributions for the stand-in tree only."""

    def __init__(self, model: Tree, **kwargs: Any) -> None:
        self.model = model
        self.expected_value = model.intercept_

    def shap_values(self, x: Any, *, check_additivity: bool) -> Any:
        return np.nan_to_num(x) * self.model.coef_


class Tail:
    """Record distribution arguments while supplying an explicit non-native probability."""

    def __init__(self) -> None:
        self.calls: list[tuple[float, ...]] = []

    def sf(self, statistic: float, *degrees: float) -> float:
        self.calls.append((float(statistic), *degrees))
        return math.exp(-max(0.0, float(statistic)))


@contextmanager
def libraries() -> Any:
    """Substitute only absent numerical libraries and retain every production model wrapper."""
    replacements = {
        "sklearn.pipeline": SimpleNamespace(Pipeline=Pipeline),
        "sklearn.linear_model": SimpleNamespace(ElasticNet=ElasticNet),
        "sklearn.model_selection": SimpleNamespace(GroupKFold=GroupKFold),
        "sklearn.ensemble": SimpleNamespace(RandomForestRegressor=Tree),
        "xgboost": SimpleNamespace(XGBRegressor=Tree),
        "statsmodels.api": SimpleNamespace(OLS=OLS, MixedLM=MixedLM),
        "scipy.stats": SimpleNamespace(chi2=Tail(), f=Tail()),
        "libpysal.weights": SimpleNamespace(W=W),
        "esda": SimpleNamespace(Moran=_moran),
        "shap": SimpleNamespace(TreeExplainer=TreeExplainer),
        "shap.utils._exceptions": SimpleNamespace(ExplainerError=ExplainerError),
        "spreg": SimpleNamespace(OLS=SpatialOLS, ML_Lag=SpatialLag, ML_Error=SpatialLag),
        "spreg.sputils": SimpleNamespace(inverse_prod=_inverse_product),
    }

    with patch.dict(sys.modules, replacements):
        yield replacements
