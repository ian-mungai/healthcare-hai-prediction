"""Explicit numerical stand-ins for synthetic model-caller checks, without native-library acceptance."""

from __future__ import annotations

import math
import sys
import warnings
from collections.abc import Mapping
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
        if reml or method not in ("lbfgs", "bfgs", "powell", "nm") or disp:
            raise AssertionError("mixed comparison must use maximum likelihood with a named optimizer and without verbose output")
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


class ConvergenceWarning(UserWarning):
    """Stand in for statsmodels' own convergence warning class, which the production code tells apart by identity."""


class SingularMatrixWarning(UserWarning):
    """Stand in for statsmodels' own singular-matrix warning class."""


ON_BOUNDARY = (ConvergenceWarning, "The MLE may be on the boundary of the parameter space.")
_UNCONVERGED = (ConvergenceWarning, "Maximum Likelihood optimization failed to converge. Check mle_retvals")
# A class of another library that has statsmodels' class name and says the same words.
_LOOKALIKE = type("ConvergenceWarning", (UserWarning,), {})
# One outcome of one optimizer: whether it reports convergence, the ownership variance over the residual variance, the log likelihood minus that of the
# fixed-only fit and the warnings it raises. A ratio of None puts the attempt at the profile peak of the data it is given, and its likelihood is then measured
# from that peak. The other outcomes copy the kinds statsmodels 0.15.0 returned on synthetic data near the zero-variance boundary.
Outcome = tuple[bool, float | None, float, tuple[tuple[type[Warning], str], ...]]
SINGULAR_ATTEMPT = (SingularMatrixWarning, "Random effects covariance is singular")
NEAR_BOUNDARY: dict[str, Outcome] = {
    "broken": (True, 0.0, math.inf, (SINGULAR_ATTEMPT,)),
    "boundary": (True, 1e-9, -1e-6, (ON_BOUNDARY,)),
    "boundary_unconverged": (False, 1e-9, -1e-6, (_UNCONVERGED,)),
    "clean_zero": (True, 0.0, 0.0, ()),
    "clean_positive": (True, 0.1, 3.0, ()),
    "below": (False, 0.1, -15.0, (_UNCONVERGED,)),
    "above": (True, 0.002, 7.0, (ON_BOUNDARY,)),
    "above_unconverged": (False, 0.002, 7.0, (ON_BOUNDARY,)),
    # A converged positive variance that only the boundary warning flags, at the profile peak, short of it and claiming more than it.
    "peak": (True, None, 0.0, (ON_BOUNDARY,)),
    "short_of_peak": (True, None, -0.1, (ON_BOUNDARY,)),
    "past_peak": (True, None, 0.5, (ON_BOUNDARY,)),
    # The peak, or more than the peak, with something that keeps the attempt from counting.
    "past_peak_unconverged": (False, None, 0.5, (ON_BOUNDARY,)),
    "past_peak_and_hessian": (True, None, 0.5, (ON_BOUNDARY, (ConvergenceWarning, "The Hessian matrix at the estimated parameter values is not positive."))),
    "peak_unconverged": (False, None, 0.0, (ON_BOUNDARY,)),
    "peak_and_hessian": (True, None, 0.0, (ON_BOUNDARY, (ConvergenceWarning, "The Hessian matrix at the estimated parameter values is not positive."))),
    "peak_and_singular": (True, None, 0.0, (ON_BOUNDARY, (SingularMatrixWarning, "The random effects covariance matrix is singular."))),
    "peak_and_numeric": (True, None, 0.0, (ON_BOUNDARY, (RuntimeWarning, "overflow in the likelihood"))),
    "peak_other_text": (True, None, 0.0, ((ConvergenceWarning, "The MLE may be on the boundary of the parameter space"),)),
    "peak_other_class": (True, None, 0.0, ((_LOOKALIKE, ON_BOUNDARY[1]),)),
    "peak_singular_class": (True, None, 0.0, ((SingularMatrixWarning, ON_BOUNDARY[1]),)),
    # Boundary warnings on estimates that are not a positive variance above the fixed-only fit. The stall claims a gain that the profile does not have at its
    # ratio: a variance ratio of 1e-13 moves the log likelihood by less than the rounding allowed.
    "stalled_at_zero": (True, 1e-13, 1e-6, (ON_BOUNDARY,)),
    "flat_away_from_zero": (True, 0.002, -1e-6, (ON_BOUNDARY,)),
    "level_at_zero": (True, 1e-9, 0.0, (ON_BOUNDARY,)),
    "zero_above": (True, 0.0, 0.0014, (ON_BOUNDARY,)),
}


def dense_profile_peak(y: Any, x: Any, groups: Any) -> tuple[float, float]:
    """Find from the full covariance matrix of the rows the variance ratio with the highest ML log likelihood and its gain over a zero ratio."""
    y, x, groups = np.asarray(y), np.asarray(x), np.asarray(groups)
    together = (groups[:, None] == groups[None, :]).astype(float)

    def likelihood(ratio: float) -> float:
        covariance = np.eye(len(y)) + ratio * together
        weighted = np.linalg.solve(covariance, np.column_stack((x, y)))
        residual = y - x @ np.linalg.solve(x.T @ weighted[:, :-1], x.T @ weighted[:, -1])
        quadratic = float(residual @ np.linalg.solve(covariance, residual))
        return -0.5 * (len(y) * (math.log(2 * math.pi * quadratic / len(y)) + 1) + float(np.linalg.slogdet(covariance)[1]))

    # Some numpy builds raise floating-point warnings inside these products; inside a scripted fit they would count against the attempt.
    with np.errstate(all="ignore"):
        powers = np.linspace(-8.0, 3.0, 45)
        for _ in range(5):
            values = [likelihood(10.0**power) for power in powers]
            best = int(np.argmax(values))
            powers = np.linspace(powers[max(best - 1, 0)], powers[min(best + 1, len(powers) - 1)], 11)
        peak = float(10.0 ** powers[len(powers) // 2])
        return peak, likelihood(peak) - likelihood(0.0)


def near_boundary_fit(script: Mapping[str, str | Outcome], calls: list[str]) -> Any:
    """A ``MixedLM.fit`` replacement that gives each optimizer its scripted result, by name or in full; ``linalg`` raises the singular-matrix error."""

    def fit(self: MixedLM, *, reml: bool, method: str, disp: bool) -> Any:
        if reml or disp:
            raise AssertionError("mixed comparison must use maximum likelihood without verbose output")
        calls.append(method)
        outcome = script[method]
        if outcome == "linalg":
            raise np.linalg.LinAlgError("Singular matrix")
        converged, ratio, gain, raised = NEAR_BOUNDARY[outcome] if isinstance(outcome, str) else outcome
        if ratio is None:
            ratio, peak_gain = dense_profile_peak(self.y, self.x, self.groups)
            gain += peak_gain
        for category, message in raised:
            warnings.warn(message, category, stacklevel=2)
        pooled = OLS(self.y, self.x).fit()
        scale = pooled.ssr / len(self.y)
        # An exactly zero covariance leaves statsmodels with fixed effects that are not the fixed-only ones.
        shift = 0.5 if math.isinf(gain) else 0.0
        return SimpleNamespace(
            converged=converged,
            fe_params=pooled.params + shift,
            cov_re=np.asarray([[ratio * scale]]),
            scale=scale,
            random_effects={group: np.asarray([ratio * number]) for number, group in enumerate(sorted(set(self.groups)), start=1)},
            llf=pooled.llf + gain,
            aic=pooled.aic - 2 * gain,
            bic=pooled.bic - 2 * gain,
        )

    return fit


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


def regularized_incomplete_beta(x: float, a: float, b: float) -> float:
    """Regularized incomplete beta function I_x(a, b), by the continued fraction on the faster-converging side."""
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    if x > (a + 1) / (a + b + 2):
        return 1.0 - regularized_incomplete_beta(1.0 - x, b, a)
    prefactor = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log1p(-x)) / a
    tiny = 1e-300
    c, d = 1.0, 1.0 - (a + b) * x / (a + 1)
    d = 1 / (tiny if abs(d) < tiny else d)
    fraction = d
    for step in range(1, 10000):
        for numerator in (
            step * (b - step) * x / ((a + 2 * step - 1) * (a + 2 * step)),
            -(a + step) * (a + b + step) * x / ((a + 2 * step) * (a + 2 * step + 1)),
        ):
            d = 1 + numerator * d
            d = 1 / (tiny if abs(d) < tiny else d)
            c = 1 + numerator / c
            c = tiny if abs(c) < tiny else c
            fraction *= d * c
        if abs(d * c - 1) < 1e-15:
            break
    return prefactor * fraction


def upper_f_tail(statistic: float, numerator: float, denominator: float) -> float:
    """Exact upper tail of the F distribution with the given degrees of freedom, from the incomplete beta function."""
    if statistic <= 0:
        return 1.0
    return regularized_incomplete_beta(denominator / (denominator + numerator * statistic), denominator / 2, numerator / 2)


class ExactF:
    """Exact F upper tail computed with the standard library, recording every request."""

    def __init__(self) -> None:
        self.calls: list[tuple[float, float, float]] = []

    def sf(self, statistic: float, numerator: float, denominator: float) -> float:
        self.calls.append((float(statistic), float(numerator), float(denominator)))
        return upper_f_tail(float(statistic), float(numerator), float(denominator))


class ForbiddenTail:
    """A tail that fails the case when read, for a statistic that must not use this reference distribution."""

    def sf(self, statistic: float, *degrees: float) -> float:
        raise AssertionError(f"a chi-square tail was read with statistic {statistic} and degrees {degrees}")


@contextmanager
def exact_f() -> Any:
    """Replace only the scipy tail with the exact standard-library F tail, for statistics whose size and power are checked."""
    tail = ExactF()
    with patch.dict(sys.modules, {"scipy.stats": SimpleNamespace(f=tail, chi2=ForbiddenTail())}):
        yield tail


def _pivoted_qr(matrix: Any, mode: str = "r", pivoting: bool = False) -> Any:
    """QR with column pivoting (Businger and Golub) in NumPy, for the small synthetic designs; it returns ``(R, pivots)`` as SciPy does."""
    if mode != "r" or not pivoting:
        raise ValueError("the stand-in supports mode='r' with pivoting only")
    work = np.array(matrix, dtype=np.float64, copy=True)
    rows, columns = work.shape
    pivots = np.arange(columns)
    for step in range(min(rows, columns)):
        norms = np.linalg.norm(work[step:, step:], axis=0)
        best = step + int(np.argmax(norms))
        work[:, [step, best]] = work[:, [best, step]]
        pivots[[step, best]] = pivots[[best, step]]
        vector = work[step:, step].copy()
        length = np.linalg.norm(vector)
        if length == 0:
            continue
        vector[0] += math.copysign(length, vector[0] or 1.0)
        vector /= np.linalg.norm(vector)
        work[step:, step:] -= 2.0 * np.outer(vector, vector @ work[step:, step:])
    return np.triu(work[: min(rows, columns)]), pivots


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
        "statsmodels.tools.sm_exceptions": SimpleNamespace(ConvergenceWarning=ConvergenceWarning),
        "scipy.stats": SimpleNamespace(chi2=Tail(), f=Tail()),
        "scipy.linalg": SimpleNamespace(qr=_pivoted_qr),
        "libpysal.weights": SimpleNamespace(W=W),
        "esda": SimpleNamespace(Moran=_moran),
        "shap": SimpleNamespace(TreeExplainer=TreeExplainer),
        "shap.utils._exceptions": SimpleNamespace(ExplainerError=ExplainerError),
        "spreg": SimpleNamespace(OLS=SpatialOLS, ML_Lag=SpatialLag, ML_Error=SpatialLag),
        "spreg.sputils": SimpleNamespace(inverse_prod=_inverse_product),
    }

    with patch.dict(sys.modules, replacements):
        yield replacements
