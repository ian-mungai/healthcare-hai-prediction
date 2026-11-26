"""Ownership random-intercept models fitted by maximum likelihood."""

from __future__ import annotations

import importlib
import math
import warnings
from collections import defaultdict
from collections.abc import Sequence
from pathlib import PurePath
from typing import Any

import numpy as np

from scripts.training.models.common import Array, Deadline, NotConverged, Prepared, Regressor, full_rank, with_intercept
from scripts.training.models.preprocessing import missing

# Tried in this order; each attempt starts from the default start values, and the first that converges without a warning is kept.
OPTIMIZERS = ("lbfgs", "bfgs", "powell", "nm")
# statsmodels' own warning classes (all derive from ModelWarning) are told apart by module or, for a stand-in, by name.
STATSMODELS_WARNINGS = ("ModelWarning", "ConvergenceWarning", "SingularMatrixWarning")
# The folders that hold installed packages; a statsmodels folder directly inside one is the installed package.
INSTALL_FOLDERS = frozenset({"site-packages", "dist-packages"})
# With statsmodels 0.15.0 on synthetic data, powell and nm stopped at an ownership to residual variance ratio near 4e-8 or below at a zero optimum.
BOUNDARY_VARIANCE_RATIO = 1e-6
# In log-likelihood units. Converged stops at that ratio were within 0.001 of the fixed-only fit, and a gain this small cannot make the test significant.
LIKELIHOOD_TOLERANCE = 0.01
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


def _boundary_witness(estimates: Sequence[Estimate], residuals: Array, groups: Any, fixed_only_llf: float) -> tuple[str, str]:
    """Name the optimizer that shows the ML optimum is the zero ownership variance, or say why that optimum is not shown.

    All of these must hold. No estimate with a finite likelihood is above the fixed-only likelihood. The profile likelihood does not rise as the variance
    leaves zero: its slope there is positive exactly when the squared sums of the fixed-only residuals in each group add up to more than the residual sum of
    squares. At least one optimizer converged at the boundary with a finite likelihood equal to the fixed-only one. Warnings are not evidence, because
    statsmodels also warns about a singular or boundary covariance when the optimum is positive."""
    finite = [item for item in estimates if math.isfinite(item[4])]
    highest = max(finite, key=lambda item: item[4], default=None)
    if highest is not None and highest[4] - fixed_only_llf > LIKELIHOOD_TOLERANCE:
        return "", f"{highest[0]} reached a likelihood {highest[4] - fixed_only_llf:.3g} above the fixed-only likelihood"
    if sum(float(residuals[groups == group].sum()) ** 2 for group in np.unique(groups)) > float(residuals @ residuals):
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

        When the ML optimum is the zero ownership variance (``_boundary_witness``), the model is the fixed-only fit with every ownership effect at zero, and its
        details say that it is a boundary fit."""
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
            warned = [str(item.message) for item in observed if _makes_attempt_unclean(item)]
            if attempt is not None:
                variance, residual = float(np.asarray(attempt.cov_re)[0, 0]), float(attempt.scale)
                estimates.append((optimizer, bool(attempt.converged), variance, residual, float(attempt.llf)))
            if attempt is not None and attempt.converged and not warned:
                model = attempt
                break
            # The cause of each failed attempt goes into the reason, so a fit on the boundary is told apart from one that never converged.
            causes = [cause[:120] for cause in (warned[:1] + ([failure] if failure else []))]
            outcomes.append(f"{optimizer}: {' then '.join(causes) or 'not converged'}")
        failed = f"ownership mixed model did not converge with any of the optimizers ({'; '.join(outcomes)})"
        if model is not None:
            _name, _converged, variance, residual, _llf = estimates[-1]
            if not math.isfinite(variance) or not math.isfinite(residual) or residual <= 0:
                raise NotConverged("ownership mixed model has singular variance")
            if variance <= 1e-10:
                failed, model = "ownership mixed model has singular variance", None
                outcomes.append(f"{optimizer}: converged with zero ownership variance")
        pooled = api.OLS(y, matrix).fit(method="qr")
        boundary: dict[str, Any] = {}
        if model is not None:
            coefficients, llf = np.asarray(model.fe_params, dtype=np.float64), float(model.llf)
            effects = {str(key): float(np.asarray(value).reshape(-1)[0]) for key, value in model.random_effects.items()}
            statistic = max(0.0, 2 * (llf - float(pooled.llf)))
            aic, bic = float(model.aic), float(model.bic)
        else:
            coefficients, llf = np.asarray(pooled.params, dtype=np.float64), float(pooled.llf)
            residuals = y - matrix @ coefficients
            variance, residual = 0.0, float(residuals @ residuals) / len(y)
            if not math.isfinite(llf) or not math.isfinite(residual) or residual <= 0 or not np.isfinite(coefficients).all():
                raise NotConverged(f"{failed}; not a zero-boundary fit: the fixed-only fit has no finite likelihood and positive residual variance")
            optimizer, refusal = _boundary_witness(estimates, residuals, groups, llf)
            if refusal:
                raise NotConverged(f"{failed}; not a zero-boundary fit: {refusal}")
            effects, statistic = dict.fromkeys(sorted(hospitals), 0.0), 0.0
            # As statsmodels counts them for an ML fit: the fixed coefficients, the ownership variance and the residual variance.
            parameter_count = matrix.shape[1] + 2
            aic, bic = -2 * llf + 2 * parameter_count, -2 * llf + math.log(len(y)) * parameter_count
            reason = (
                f"{optimizer} converged at an ownership variance of zero with the fixed-only likelihood; no attempt exceeded that likelihood and it does not "
                "rise as the variance leaves zero"
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
