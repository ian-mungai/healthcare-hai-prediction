"""Ownership random-intercept models fitted by maximum likelihood.

Each optimizer is tried in order, and the first that converges with no warning from statsmodels or the numerics to a positive variance is the fit. Without one,
a search of the profile likelihood over the ownership to residual variance ratio decides between a zero and a positive variance:

1. Positive, when a positive ratio has a likelihood above the zero ratio by more than rounding error. The fit counts only if an optimizer attempt converged,
   raised no warning of any kind besides the statsmodels boundary warning, stopped at a positive variance and has a likelihood above the fixed-only one that
   no other attempt's finite likelihood exceeds. For a stop within the zero-boundary tolerance, the profile at its own ratio must also be above zero by more
   than numerical error. It is reported like any other positive-variance fit. statsmodels raises that warning for every variance estimate below 0.01, so the
   warning alone does not show that the optimum is zero.
2. Zero, when no positive ratio is better, no attempt is above the fixed-only likelihood, the likelihood does not rise as the variance leaves zero and an
   optimizer converged there (``_boundary_witness``). The fit is the fixed-only fit, and its details say that it is a boundary fit.

Otherwise the model is not fitted. That includes a profile that cannot be evaluated or leaves the comparison unresolved: its highest value is at an end of the
ratios searched, or no positive ratio is better and it still rises at the largest one or its evaluations are too ill-conditioned."""

from __future__ import annotations

import importlib
import math
import warnings
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import PurePath
from typing import Any

import numpy as np

from scripts.training.models.common import Array, Deadline, NotConverged, Prepared, Regressor, full_rank, with_intercept
from scripts.training.models.preprocessing import missing

# Tried in this order; each attempt starts from the default start values, and the first that converges without a warning is kept.
OPTIMIZERS = ("lbfgs", "bfgs", "powell", "nm")
# The text of the one warning that does not disqualify a positive-variance fit. statsmodels 0.15.0 (MixedLM.fit) raises it with its ConvergenceWarning class.
BOUNDARY_WARNING = "The MLE may be on the boundary of the parameter space."
# statsmodels' own warning classes (all derive from ModelWarning) are told apart by module or, for a stand-in, by name.
STATSMODELS_WARNINGS = ("ModelWarning", "ConvergenceWarning", "SingularMatrixWarning")
# The folders that hold installed packages; a statsmodels folder directly inside one is the installed package.
INSTALL_FOLDERS = frozenset({"site-packages", "dist-packages"})
# With statsmodels 0.15.0 on synthetic data, powell and nm stopped at an ownership to residual variance ratio near 4e-8 or below at a zero optimum. A converged
# stop at or below this ratio can show a zero optimum. A positive optimum can lie below it too, so it does not rule out a positive-variance fit.
BOUNDARY_VARIANCE_RATIO = 1e-6
# In log-likelihood units: how far below the fixed-only likelihood a converged stop at the zero boundary may be (those seen at that ratio were within 0.001).
LIKELIHOOD_TOLERANCE = 0.01
# The rounding error allowed in a log-likelihood difference, per unit of the larger of the likelihood's size and the row count.
GAIN_ROUNDING = 1e-12
# The score at a zero variance is the sum of squared group residual totals over the residual sum of squares, minus one. Up to this value it is not told apart
# from zero, and the profile likelihood alone decides.
SCORE_ROUNDING = 1e-9
# The variance ratios at which the profile likelihood is first evaluated (ten for each power of ten) and the number of times the search narrows around each
# sampled local maximum.
PROFILE_RATIOS = np.logspace(-10, 8, 181)
PROFILE_NARROWINGS = 4
# A clean fit's estimated ownership variance at or below this is singular, not positive.
SINGULAR_VARIANCE = 1e-10
# One optimizer's estimate: its name, whether it reported convergence, the ownership variance, the residual variance and the log likelihood.
Estimate = tuple[str, bool, float, float, float]


def _makes_attempt_unclean(item: warnings.WarningMessage) -> bool:
    """Tell a warning that spoils an optimizer attempt (a RuntimeWarning, a statsmodels class or a UserWarning from statsmodels) from one that does not."""
    category = item.category
    return (
        issubclass(category, RuntimeWarning)
        or any(kind.__module__.startswith("statsmodels.") or kind.__name__ in STATSMODELS_WARNINGS for kind in category.__mro__)
        or (issubclass(category, UserWarning) and _from_installed_statsmodels(str(item.filename)))
    )


def _from_installed_statsmodels(filename: str) -> bool:
    """True for a file of the installed statsmodels package (site-packages/statsmodels/...), not for a project folder that happens to be named statsmodels."""
    parts = PurePath(filename).parts
    return any(part in INSTALL_FOLDERS and following == "statsmodels" for part, following in zip(parts, parts[1:], strict=False))


def _only_boundary_warning(observed: Sequence[warnings.WarningMessage]) -> bool:
    """True when an optimizer call warned and every warning it raised, from any library, is the statsmodels boundary warning: its own convergence warning class
    with its exact text."""
    category = importlib.import_module("statsmodels.tools.sm_exceptions").ConvergenceWarning
    return bool(observed) and all(item.category is category and str(item.message) == BOUNDARY_WARNING for item in observed)


def _profile_peak(
    matrix: Array, residuals: Array, groups: Any, rounding: float, probes: Sequence[float] = (), evaluated: dict[float, tuple[float, float]] | None = None
) -> tuple[float, float, str]:
    """Search the ML profile likelihood over positive variance ratios and give the best (ratio, gain over the zero ratio, why the comparison is unresolved).

    With t the ownership variance over the residual variance and n_g rows in group g, the gain is -(sum over g of log(1 + n_g t) + n log(Q(t) / Q(0))) / 2,
    where Q(t) is the generalized least-squares residual sum of squares at t. Q(t) - Q(0) is computed from the fixed-only residuals and an orthonormal basis of
    the design, so a small gain is not lost in the subtraction of two large likelihoods. The ratios searched are ``PROFILE_RATIOS`` and the ``probes``, here the
    optimizers' own ratios. The search narrows around every sampled local maximum, whatever its gain, because a peak above zero can be sampled below it. A
    last sample that is not below the one before it counts as one, so a maximum inside the last step is found. ``evaluated``, when given, receives the gain
    and its error bound at every ratio evaluated.

    The last value is empty when the search shows which wins: a gain above ``rounding`` beyond its own error bound, or no gain above it within any error bound.
    The error bound of a gain is the machine precision times the condition number of the solved system, carried into the log likelihood. A highest value at an
    end of the ratios searched leaves the comparison unresolved. When no gain is reliably above zero, so does a profile still rising at the largest ratio of
    ``PROFILE_RATIOS``, measured over the last step the narrowing reaches: zero would then depend on ratios that were not searched."""
    members = [groups == group for group in np.unique(groups)]
    sizes = np.asarray([member.sum() for member in members], dtype=np.float64)
    totals = np.asarray([residuals[member].sum() for member in members])
    basis = np.linalg.qr(matrix)[0]
    columns = np.asarray([basis[member].sum(axis=0) for member in members])
    squares, identity = float(residuals @ residuals), np.eye(basis.shape[1])
    found: dict[float, tuple[float, float]] = {} if evaluated is None else evaluated

    def gain(ratio: float) -> float:
        weights = ratio / (1 + sizes * ratio)
        cross = columns.T @ (weights * totals)
        information = identity - columns.T @ (weights[:, None] * columns)
        quadratic = float(cross @ np.linalg.solve(information, cross))
        change = -float(weights @ totals**2) - quadratic
        value = float(-0.5 * (np.log1p(sizes * ratio).sum() + len(residuals) * np.log1p(change / squares)))
        error = float(np.finfo(np.float64).eps * np.linalg.cond(information) * abs(quadratic) * len(residuals) / (2 * (squares + change)))
        found[ratio] = (value, error)
        return value

    unevaluated = "the profile likelihood could not be evaluated, so zero and positive are not told apart"
    ratios = np.unique(np.concatenate((PROFILE_RATIOS, np.asarray(probes, dtype=np.float64))))
    top = float(PROFILE_RATIOS[-1])
    # One narrowed step below the largest ratio of the grid: the change from there to that ratio shows the profile's direction where the grid ends.
    near_top = top * float(PROFILE_RATIOS[-2] / top) ** (1 / 20**PROFILE_NARROWINGS)
    try:
        # A value that cannot be computed is caught below as one that is not finite, so numpy's floating-point warnings add nothing.
        with np.errstate(all="ignore"):
            gains = [gain(float(ratio)) for ratio in ratios]
            for index in range(1, len(ratios)):
                following = min(index + 1, len(ratios) - 1)
                if not gains[index - 1] <= gains[index] >= gains[following]:
                    continue
                low, high = ratios[index - 1], ratios[following]
                for _ in range(PROFILE_NARROWINGS):
                    finer = np.geomspace(low, high, 21)
                    best = int(np.argmax([gain(float(ratio)) for ratio in finer]))
                    low, high = finer[max(best - 1, 0)], finer[min(best + 1, len(finer) - 1)]
            gain(near_top)
    except np.linalg.LinAlgError:
        return math.nan, math.nan, unevaluated
    if not all(math.isfinite(value) for value, _error in found.values()):
        return math.nan, math.nan, unevaluated
    ratio, (best_gain, error) = max(found.items(), key=lambda item: item[1][0])
    if ratio == max(found) or (not best_gain - error > rounding and found[top][0] >= found[near_top][0]):
        doubt = "the profile likelihood is still rising at the largest variance ratio searched, so zero and positive are not told apart"
    elif ratio == min(found) and best_gain > rounding:
        doubt = "the profile likelihood is highest at the smallest variance ratio searched, so zero and positive are not told apart"
    elif best_gain - error <= rounding < max(value + bound for value, bound in found.values()):
        doubt = "the profile likelihood is too ill-conditioned to tell zero and a positive variance apart"
    else:
        doubt = ""
    return ratio, best_gain, doubt


def _flagged_positive_fit(
    flagged: Sequence[tuple[str, Any]], estimates: Sequence[Estimate], fixed_only_llf: float, rounding: float, profile: Mapping[float, tuple[float, float]]
) -> tuple[str, Any]:
    """Pick the attempt flagged only by the boundary warning that stands as the positive-variance fit, or ("", None).

    It needs a finite, positive ownership and residual variance, and a finite likelihood that is above the fixed-only likelihood and that no attempt's finite
    likelihood exceeds (both by more than rounding error). A likelihood reported by a stop within the zero-boundary tolerance is checked as well: ``profile``,
    the gain and error bound at each ratio the profile search evaluated, must be above zero at the stop's own ratio by more than the rounding and that bound.
    These tests are unchanged by the outcome's scale. The profile likelihood does not measure how close an attempt came to its peak."""
    highest = max((item[4] for item in estimates if math.isfinite(item[4])), default=-math.inf)
    for name, attempt in flagged:
        variance, residual, llf = float(np.asarray(attempt.cov_re)[0, 0]), float(attempt.scale), float(attempt.llf)
        if not (math.isfinite(variance) and math.isfinite(residual) and math.isfinite(llf) and residual > 0):
            continue
        if not (variance > 0 and llf - fixed_only_llf > rounding and highest - llf <= rounding):
            continue
        if variance <= BOUNDARY_VARIANCE_RATIO * residual:
            # The ratio was one of the search's probes; one too small to be told from zero was not, and has no gain to show.
            gain, error = profile.get(variance / residual, (0.0, 0.0))
            if not gain - error > rounding:
                continue
        return name, attempt
    return "", None


def _boundary_witness(estimates: Sequence[Estimate], residuals: Array, groups: Any, fixed_only_llf: float, rounding: float) -> tuple[str, str]:
    """Name the optimizer that shows the ML optimum is the zero ownership variance, or say why that optimum is not shown.

    The caller has found no positive ratio in the profile likelihood that is better than zero. All of these must hold as well. No estimate with a finite
    likelihood is above the fixed-only likelihood by more than rounding error. The likelihood does not rise as the variance leaves zero: its slope there has
    the sign of the squared sums of the fixed-only residuals in each group, added up, minus the residual sum of squares. At least one optimizer converged at
    the boundary with a finite likelihood equal to the fixed-only one. Warnings are not evidence, because statsmodels also warns about a singular or boundary
    covariance when the optimum is positive."""
    finite = [item for item in estimates if math.isfinite(item[4])]
    highest = max(finite, key=lambda item: item[4], default=None)
    if highest is not None and highest[4] - fixed_only_llf > rounding:
        return "", f"{highest[0]} reached a likelihood {highest[4] - fixed_only_llf:.3g} above the fixed-only likelihood"
    between = sum(float(residuals[groups == group].sum()) ** 2 for group in np.unique(groups))
    if between / float(residuals @ residuals) - 1 > SCORE_ROUNDING:
        return "", "the likelihood rises as the ownership variance leaves zero"
    for name, converged, variance, residual, llf in finite:
        if converged and abs(llf - fixed_only_llf) <= LIKELIHOOD_TOLERANCE and residual > 0 and 0 <= variance <= BOUNDARY_VARIANCE_RATIO * residual:
            return name, ""
    return "", "no attempt converged at the zero-variance boundary"


class MixedRegressor(Regressor):
    """Estimate ownership intercept variance and compare it with the same fixed-only design."""

    def __init__(self, deadline: Deadline | None = None) -> None:
        self.deadline = deadline

    def fit(self, design: Prepared, y: Array) -> MixedRegressor:
        """Fit ML, require usable ownership groups and expose ICC with a boundary-aware likelihood test.

        Without a clean positive fit, the profile likelihood decides (``_profile_peak``). A better positive variance stands only through an attempt flagged only
        by the boundary warning that qualifies (``_flagged_positive_fit``). When the ML optimum is the zero ownership variance (``_boundary_witness``), the
        model is the fixed-only fit with every ownership effect at zero, and its details say that it is a boundary fit."""
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
        model, optimizer, outcomes = None, "", []
        estimates: list[Estimate] = []
        flagged: list[tuple[str, Any]] = []
        for optimizer in OPTIMIZERS:
            self._check_deadline()
            failure = ""
            with warnings.catch_warnings(record=True) as observed:
                # A caller's ignore, default or once filter must not hide a warning from the classifier, so each becomes always while a caller's error
                # filter stays and still raises. The two filters go last: they only make a warning that no filter covers show up on every attempt.
                warnings.filters = [
                    ("error" if action == "error" else "always", text, kind, module, line) for action, text, kind, module, line in warnings.filters
                ]
                warnings.simplefilter("always", UserWarning, append=True)
                warnings.simplefilter("always", RuntimeWarning, append=True)
                try:
                    candidate = api.MixedLM(y, matrix, groups=groups)
                    self._check_deadline()
                    attempt = candidate.fit(reml=False, method=optimizer, disp=False)
                except (np.linalg.LinAlgError, FloatingPointError) as error:
                    attempt, failure = None, f"{type(error).__name__}: {error}"
            self._check_deadline()
            spoiling = [item for item in observed if _makes_attempt_unclean(item)]
            warned = [str(item.message) for item in spoiling]
            if attempt is not None:
                variance, residual = float(np.asarray(attempt.cov_re)[0, 0]), float(attempt.scale)
                estimates.append((optimizer, bool(attempt.converged), variance, residual, float(attempt.llf)))
            if attempt is not None and attempt.converged and not warned:
                model = attempt
                break
            if attempt is not None and attempt.converged and spoiling and _only_boundary_warning(observed):
                flagged.append((optimizer, attempt))
            # The cause of each failed attempt goes into the reason, so a fit on the boundary is told apart from one that never converged.
            causes = [cause[:120] for cause in (warned[:1] + ([failure] if failure else []))]
            outcomes.append(f"{optimizer}: {' then '.join(causes) or 'not converged'}")
        failed = f"ownership mixed model did not converge with any of the optimizers ({'; '.join(outcomes)})"
        if model is not None:
            _name, _converged, variance, residual, _llf = estimates[-1]
            if not math.isfinite(variance) or not math.isfinite(residual) or residual <= 0:
                raise NotConverged("ownership mixed model has singular variance")
            if variance <= SINGULAR_VARIANCE:
                failed, model = "ownership mixed model has singular variance", None
                outcomes.append(f"{optimizer}: converged with zero ownership variance")
        pooled = api.OLS(y, matrix).fit(method="qr")
        if model is None:
            fixed, fixed_llf = np.asarray(pooled.params, dtype=np.float64), float(pooled.llf)
            residuals = y - matrix @ fixed
            pooled_residual = float(residuals @ residuals) / len(y)
            if not math.isfinite(fixed_llf) or not math.isfinite(pooled_residual) or pooled_residual <= 0 or not np.isfinite(fixed).all():
                raise NotConverged(f"{failed}; not a zero-boundary fit: the fixed-only fit has no finite likelihood and positive residual variance")
            rounding = GAIN_ROUNDING * max(abs(fixed_llf), len(y))
            probes = [ratio for item in estimates if item[3] > 0 and 0 < (ratio := item[2] / item[3]) < math.inf]
            profile: dict[float, tuple[float, float]] = {}
            peak_ratio, peak_gain, doubt = _profile_peak(matrix, residuals, groups, rounding, probes, profile)
            if doubt:
                raise NotConverged(f"{failed}; not a zero-boundary fit: {doubt}")
            if peak_gain > rounding:
                name, model = _flagged_positive_fit(flagged, estimates, fixed_llf, rounding, profile)
                if model is None:
                    raise NotConverged(
                        f"{failed}; not a zero-boundary fit: a positive ownership variance is better (the profile likelihood is {peak_gain:.3g} above the "
                        f"fixed-only likelihood at a variance ratio of {peak_ratio:.3g}) and no attempt that converged with only the boundary warning has a "
                        "positive variance with a likelihood above the fixed-only one that no other attempt exceeds"
                    )
                optimizer, variance, residual = name, float(np.asarray(model.cov_re)[0, 0]), float(model.scale)
        boundary: dict[str, Any] = {}
        if model is not None:
            coefficients, llf = np.asarray(model.fe_params, dtype=np.float64), float(model.llf)
            effects = {str(key): float(np.asarray(value).reshape(-1)[0]) for key, value in model.random_effects.items()}
            statistic = max(0.0, 2 * (llf - float(pooled.llf)))
            aic, bic = float(model.aic), float(model.bic)
        else:
            coefficients, llf = fixed, fixed_llf
            variance, residual = 0.0, pooled_residual
            optimizer, refusal = _boundary_witness(estimates, residuals, groups, llf, rounding)
            if refusal:
                raise NotConverged(f"{failed}; not a zero-boundary fit: {refusal}")
            effects, statistic = dict.fromkeys(sorted(hospitals), 0.0), 0.0
            # As statsmodels counts them for an ML fit: the fixed coefficients, the ownership variance and the residual variance.
            parameter_count = matrix.shape[1] + 2
            aic, bic = -2 * llf + 2 * parameter_count, -2 * llf + math.log(len(y)) * parameter_count
            reason = (
                f"{optimizer} converged at an ownership variance of zero with the fixed-only likelihood; no attempt and no positive variance ratio in the "
                "profile likelihood exceeded it, and it does not rise as the variance leaves zero"
            )
            boundary = {"boundary": True, "boundary_reason": reason, "optimizer_outcomes": outcomes}
        p_value = 1.0 if statistic == 0 else 0.5 * float(importlib.import_module("scipy.stats").chi2.sf(statistic, 1))
        self.coefficients_ = coefficients
        self.effects_ = effects
        self.details_ = {
            "coefficients": dict(zip(("intercept", *design.names), self.coefficients_.tolist(), strict=True)),
            "ownership_variance": variance,
            "residual_variance": residual,
            "icc": variance / (variance + residual),
            "likelihood_ratio": statistic,
            "p_value": p_value,
            "null_distribution": "half_point_mass_zero_half_chi_squared_1",
            "reml": False,
            "optimizer": optimizer,
            "ownership_groups": len(hospitals),
            "aic": aic,
            "bic": bic,
            **boundary,
        }
        return self

    def _check_deadline(self) -> None:
        """Refuse to start or accept an optimizer attempt after the inherited operation deadline."""
        if self.deadline is not None:
            self.deadline.check()

    def predict(self, design: Prepared) -> Any:
        """Use a known ownership intercept or its zero population mean for an unseen group."""
        return with_intercept(design.matrix) @ self.coefficients_ + np.asarray(
            [self.effects_.get(str(row.get("control_type_code")), 0.0) for row in design.rows]
        )
