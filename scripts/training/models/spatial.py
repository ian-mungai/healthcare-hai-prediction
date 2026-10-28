"""Hospital-neighbor weights, seeded Moran inference and spatial likelihood models."""

from __future__ import annotations

import hashlib
import importlib
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np

from scripts.training.models.common import Array, Deadline, ModelError, NotEstimable, Prepared, Regressor, full_rank, with_intercept


@dataclass(frozen=True)
class Weights:
    """An ordered hospital-window graph with explicit islands and no cross-year edges."""

    keys: tuple[str, ...]
    neighbors: Mapping[str, tuple[str, ...]]

    @property
    def islands(self) -> tuple[str, ...]:
        """Return hospitals with no same-window county neighbor."""
        return tuple(key for key in self.keys if not self.neighbors[key])

    def native(self, keys: tuple[str, ...] | None = None) -> Any:
        """Construct row-standardized native weights and verify the exact row-ID order."""
        selected = self.keys if keys is None else keys
        selected_set = set(selected)
        neighbors = {key: [value for value in self.neighbors[key] if value in selected_set] for key in selected}
        result = importlib.import_module("libpysal.weights").W(neighbors, id_order=list(selected), silence_warnings=True)
        result.transform = "r"
        if tuple(result.id_order) != selected:
            raise ModelError("native spatial weights changed their row order")
        return result


def build_weights(rows: Sequence[Mapping[str, Any]], adjacency: Sequence[Mapping[str, Any]]) -> Weights:
    """Join same or adjacent counties within each year, preserving every supplied row key."""
    keys = tuple(row["spine_key"] for row in rows)
    if len(keys) != len(set(keys)):
        raise ModelError("duplicate spatial row key")
    counties: dict[tuple[int, str], list[str]] = defaultdict(list)
    borders: dict[str, set[str]] = defaultdict(set)
    for edge in adjacency:
        left, right = edge.get("county_fips"), edge.get("neighbor_fips")
        if isinstance(left, str) and isinstance(right, str) and left != right:
            borders[left].add(right)
            borders[right].add(left)
    for row in rows:
        if row.get("county_fips") is not None:
            counties[(row["window_year"], str(row["county_fips"]))].append(row["spine_key"])
    neighbors = {}
    for row in rows:
        year, county = row["window_year"], row.get("county_fips")
        adjacent: set[str] = set()
        if county is not None:
            for other in {str(county), *borders[str(county)]}:
                adjacent.update(counties.get((year, other), ()))
        adjacent.discard(row["spine_key"])
        neighbors[row["spine_key"]] = tuple(sorted(adjacent))
    return Weights(keys, neighbors)


def moran_test(residuals: Array, keys: tuple[str, ...], graph: Weights, seed: int, permutations: int, deadline: Deadline) -> dict[str, Any]:
    """Compute Moran's I with the immediately seeded global stream and the permutation p-value."""
    deadline.check()
    if keys != graph.keys or len(residuals) != len(keys):
        raise ModelError("Moran residual and weight row order differs")
    islands = set(graph.islands)
    active = tuple(key for key in keys if key not in islands)
    if len(active) < 3:
        return {"status": "not_estimable", "reason": "fewer than three non-island hospitals", "islands": len(islands)}
    positions = {key: index for index, key in enumerate(keys)}
    values = np.asarray([residuals[positions[key]] for key in active], dtype=np.float64)
    if not np.isfinite(values).all() or np.var(values) <= 0:
        return {"status": "not_estimable", "reason": "residuals are nonfinite or constant", "islands": len(islands)}
    weights = graph.native(active)
    moran = importlib.import_module("esda").Moran
    np.random.seed(seed)
    result = moran(values, weights, transformation="r", permutations=permutations, two_tailed=True)
    deadline.check()
    statistic, p_value = float(result.I), float(result.p_sim)
    if not math.isfinite(statistic) or not math.isfinite(p_value) or not 0 <= p_value <= 1:
        raise NotEstimable("Moran statistic or permutation p-value is not finite")
    return {
        "status": "fitted",
        "statistic": statistic,
        "p_value": p_value,
        "p_value_name": "p_sim",
        "seed": seed,
        "permutations": permutations,
        "islands": len(islands),
        "included_hospitals": len(active),
        "row_standardized": True,
        "permutations_sha256": hashlib.sha256(np.asarray(result.sim, dtype="<f8").tobytes()).hexdigest(),
    }


def choose_spatial_model(diagnostics: Mapping[str, Sequence[float]], significance: float = 0.05) -> tuple[str, str]:
    """Choose a significant robust LM alternative and return the rule that selected it."""
    supported = []
    for kind, name in (("lag", "rlm_lag"), ("error", "rlm_error")):
        statistic, p_value = map(float, diagnostics[name])
        if not math.isfinite(statistic) or not math.isfinite(p_value) or not 0 <= p_value <= 1:
            raise NotEstimable("LM diagnostic is not finite")
        if p_value < significance:
            supported.append((statistic, kind))
    if not supported:
        raise NotEstimable("neither robust LM alternative is significant")
    if len(supported) == 1:
        return supported[0][1], "one_significant_robust_alternative"
    if supported[0][0] == supported[1][0]:
        raise NotEstimable("significant robust LM alternatives have equal statistics")
    return max(supported)[1], "both_significant_larger_robust_statistic"


class SpatialRegressor(Regressor):
    """Fit a training-only LM-selected likelihood model and predict without holdout outcomes."""

    def __init__(self, adjacency: tuple[Mapping[str, Any], ...]) -> None:
        self.adjacency = adjacency

    def fit(self, design: Prepared, y: Array) -> SpatialRegressor:
        """Fit the spatial alternative selected by robust training-sample LM diagnostics."""
        full_rank(with_intercept(design.matrix))
        graph = build_weights(design.rows, self.adjacency)
        if len(graph.islands) == len(graph.keys):
            raise NotEstimable("training spatial graph has no neighbors")
        weights = graph.native()
        api = importlib.import_module("spreg")
        base = api.OLS(y.reshape(-1, 1), design.matrix, w=weights, spat_diag=True, name_x=list(design.names))
        diagnostics = {
            "lm_lag": [float(value) for value in base.lm_lag],
            "lm_error": [float(value) for value in base.lm_error],
            "rlm_lag": [float(value) for value in base.rlm_lag],
            "rlm_error": [float(value) for value in base.rlm_error],
        }
        self.kind_, selection_rule = choose_spatial_model(diagnostics)
        estimator = api.ML_Lag if self.kind_ == "lag" else api.ML_Error
        native = estimator(y.reshape(-1, 1), design.matrix, weights, method="LU", epsilon=1e-7, name_x=list(design.names))
        beta = np.asarray(native.betas, dtype=np.float64).reshape(-1)
        if len(beta) != design.matrix.shape[1] + 2 or not np.isfinite(beta).all():
            raise NotEstimable("spatial model changed coefficient identity or returned nonfinite estimates")
        self.coefficients_, self.spatial_parameter_ = beta[:-1], float(beta[-1])
        if abs(self.spatial_parameter_) >= 1:
            raise NotEstimable("spatial dependence estimate is outside the stable prediction range")
        self.details_ = {
            "kind": self.kind_,
            "lm_diagnostics": diagnostics,
            "lm_selection": selection_rule,
            "coefficients": dict(zip(("intercept", *design.names), self.coefficients_.tolist(), strict=True)),
            "spatial_parameter": self.spatial_parameter_,
            "training_islands": len(graph.islands),
            "method": "LU",
            "epsilon": 1e-7,
            "aic": float(native.aic),
            "bic": float(native.schwarz),
        }
        return self

    def predict(self, design: Prepared) -> Any:
        """Use structural error predictions or reduced-form lag predictions on the supplied graph."""
        fixed = with_intercept(design.matrix) @ self.coefficients_
        if self.kind_ == "error":
            return fixed
        weights = build_weights(design.rows, self.adjacency).native()
        return np.asarray(
            importlib.import_module("spreg.sputils").inverse_prod(
                weights.sparse, fixed.reshape(-1, 1), self.spatial_parameter_, inv_method="power_exp", threshold=1e-7
            )
        ).reshape(-1)
