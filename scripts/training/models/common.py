"""Validated numerical inputs, stable model results and operation deadlines."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import math
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypedDict

import numpy as np
from numpy.typing import NDArray

SLOTS = ("m1_ols", "m2_elastic_net", "m3_mixed", "m4_boosting", "m5_spatial", "m6_fixed_effects")
REGISTRABLE = ("m1_ols", "m2_elastic_net", "m4_boosting")
GROUP_FIELDS = frozenset(("control_type_code", "provider_subtype_code", "state_code", "bed_count", "county_fips"))
Array = NDArray[np.float64]
Status = Literal["fitted", "not_converged", "not_estimable", "not_run"]


class Scores(TypedDict):
    """The common-holdout regression metrics, with undefined constant-outcome R squared explicit."""

    rmse: float
    mae: float
    r2: float | None


class ModelError(ValueError):
    """An input or operation violates the analysis contract."""


class NotEstimable(ModelError):
    """The supplied analysis sample cannot identify the requested parameters."""


class NotConverged(ModelError):
    """A numerical fit did not reach an acceptable solution."""


class DeadlineExceeded(ModelError):
    """The whole operation exhausted its fixed deadline."""


@dataclass(frozen=True)
class Deadline:
    """Carry one absolute monotonic deadline through folds and analysis calls."""

    expires: float

    @classmethod
    def after(cls, seconds: float) -> Deadline:
        """Create an operation deadline from a positive finite budget in seconds."""
        if not math.isfinite(seconds) or seconds <= 0:
            raise ModelError("operation budget must be positive and finite")
        return cls(time.monotonic() + seconds)

    def check(self) -> None:
        """Refuse further work when the inherited operation deadline expires."""
        if time.monotonic() >= self.expires:
            raise DeadlineExceeded("operation deadline expired")

    def within(self, seconds: float) -> Deadline:
        """Limit a child operation without extending its parent's absolute deadline."""
        self.check()
        return Deadline(min(self.expires, time.monotonic() + seconds))


@dataclass(frozen=True)
class Feature:
    """A registered predictor reference and its declared numerical or categorical type."""

    reference: str
    kind: Literal["number", "category"]
    control: str = ""
    family: str = ""

    @property
    def name(self) -> str:
        """Return the bare column name used by extracts and prediction signatures."""
        return self.reference.split(":", 1)[-1]


def _excluded_controls() -> frozenset[str]:
    path = Path(__file__).resolve().parents[3] / "config/training/excluded_controls.json"
    return frozenset(json.loads(path.read_bytes())["controls"])


@dataclass(frozen=True)
class Dataset:
    """One type's already verified extract, retaining the exact hospital-window row identity."""

    rows: tuple[Mapping[str, Any], ...]
    features: tuple[Feature, ...]
    hai_type: str
    excluded_controls: frozenset[str] = field(default_factory=_excluded_controls)

    def __post_init__(self) -> None:
        if self.hai_type not in tuple(f"HAI_{number}" for number in range(1, 7)):
            raise ModelError("unknown HAI type")
        references = [feature.reference for feature in self.features]
        names = [feature.name for feature in self.features]
        if not self.features or len(set(references)) != len(references) or len(set(names)) != len(names):
            raise ModelError("duplicate or empty predictor definitions")
        for feature in self.features:
            control = feature.control.split(".", 1)[0]
            match = re.match(r"([a-zA-Z][0-9]+)(?:[._]|$)", feature.name)
            named_control = match[1].upper() if match else ""
            if (
                feature.reference.startswith("label_")
                or feature.control in self.excluded_controls
                or control in self.excluded_controls
                or named_control in self.excluded_controls
            ):
                raise ModelError("excluded label, outcome component or validation control cannot be a predictor")
            if feature.kind not in ("number", "category") or feature.reference.count(":") != 1:
                raise ModelError("predictor reference or type is invalid")
        allowed = set(names) | set(GROUP_FIELDS) | {"spine_key", "ccn", "window_year", self.label}
        keys: set[str] = set()
        for row in self.rows:
            if set(row) - allowed:
                raise ModelError("unexpected extract column outside registered predictors, grouping attributes and the label")
            if not set(names).issubset(row):
                raise ModelError("extract is missing a registered predictor column")
            key = row.get("spine_key")
            if not isinstance(key, str) or key in keys:
                raise ModelError("duplicate or missing hospital-window key")
            match = re.fullmatch(r"([0-9]{6}):([12][0-9]{3})", key)
            if match is None or row.get("ccn") != match[1] or row.get("window_year") != int(match[2]):
                raise ModelError("hospital-window key differs from CCN or year")
            value = row.get(self.label)
            if not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(value):
                raise ModelError("each analysis row requires a finite label")
            keys.add(key)

    @property
    def label(self) -> str:
        """Return the type-specific published SIR column, without deriving an outcome."""
        return f"hai_{self.hai_type.split('_')[1]}__sir__number"

    @property
    def y(self) -> Array:
        """Return labels in the unchanged extract row order."""
        return np.asarray([row[self.label] for row in self.rows], dtype=np.float64)

    @property
    def keys(self) -> tuple[str, ...]:
        """Return stable row keys in prediction order."""
        return tuple(row["spine_key"] for row in self.rows)

    def subset(self, indices: Sequence[int]) -> Dataset:
        """Keep specified positions with their original metadata and predictor definitions."""
        return Dataset(tuple(self.rows[int(index)] for index in indices), self.features, self.hai_type, self.excluded_controls)

    def split(self) -> tuple[Dataset, Dataset]:
        """Reserve every row of the newest outcome year as the common holdout."""
        if not self.rows:
            raise ModelError("extract has no analysis rows")
        newest = max(row["window_year"] for row in self.rows)
        return self.subset([i for i, row in enumerate(self.rows) if row["window_year"] < newest]), self.subset(
            [i for i, row in enumerate(self.rows) if row["window_year"] == newest]
        )


@dataclass(frozen=True)
class Prepared:
    """A fold-fitted design matrix, its source references and unmodeled row metadata."""

    matrix: Array
    names: tuple[str, ...]
    references: tuple[str, ...]
    rows: tuple[Mapping[str, Any], ...]
    unknown_categories: int = 0
    reference_aliases: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def columns_for(self, references: Sequence[str]) -> tuple[int, ...]:
        """Resolve every retained column owned by a requested source, including shared indicators."""
        requested = set(references)
        return tuple(
            index
            for index, (name, reference) in enumerate(zip(self.names, self.references, strict=True))
            if reference in requested or requested.intersection(self.reference_aliases.get(name, ()))
        )


@dataclass(frozen=True)
class ModelRequest:
    """Inputs shared by the six model bodies, without mediator or HAC inputs."""

    data: Dataset
    feature_lists: Mapping[str, tuple[str, ...]]
    config: Mapping[str, Any]
    adjacency: tuple[Mapping[str, Any], ...]
    process_references: tuple[str, ...]
    cmi_reference: str
    threads: int
    deadline: Deadline

    def features_for(self, slot: str) -> tuple[Feature, ...]:
        """Resolve a requested slot against the validated extract's predictor inventory."""
        catalog = {feature.reference: feature for feature in self.data.features}
        references = self.feature_lists[slot]
        if len(set(references)) != len(references) or not set(references).issubset(catalog):
            raise ModelError("slot contains duplicate or unknown predictor references")
        return tuple(catalog[reference] for reference in references)


@dataclass
class SlotResult:
    """Exactly one model status, one holdout score set and optional fitted pipeline."""

    slot: str
    status: Status
    reason: str = ""
    predictions: tuple[float, ...] = ()
    holdout_positions: tuple[int, ...] = ()
    metrics: dict[str, float | None] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    pipeline: Any = field(default=None, repr=False, compare=False)


class EstimatorParameters:
    """Expose the estimator parameter protocol required by scikit-learn Pipelines."""

    def get_params(self, deep: bool = True) -> dict[str, Any]:
        """Return constructor parameters without fitted numerical state."""
        return {name: getattr(self, name) for name in inspect.signature(type(self).__init__).parameters if name != "self"}

    def set_params(self, **parameters: Any) -> Any:
        """Set declared constructor parameters and reject unknown names."""
        known = self.get_params()
        if not set(parameters).issubset(known):
            raise ModelError("unknown estimator parameter")
        for name, value in parameters.items():
            setattr(self, name, value)
        return self

    def __sklearn_is_fitted__(self) -> bool:
        return any(name.endswith("_") for name in vars(self))

    def __sklearn_tags__(self) -> Any:
        tags = importlib.import_module("sklearn.base").BaseEstimator().__sklearn_tags__()
        tags.estimator_type = getattr(self, "_estimator_type", None)
        tags.target_tags.required = tags.estimator_type == "regressor"
        return tags


class Regressor(EstimatorParameters):
    """Declare the scikit-learn regressor tag for custom offline estimator adapters."""

    _estimator_type = "regressor"


def seed_for(master: int, hai_type: str, purpose: str) -> int:
    """Derive the accepted first-32-bit SHA-256 seed for one type and purpose."""
    return int.from_bytes(hashlib.sha256(f"{master}|{hai_type}|{purpose}".encode()).digest()[:4], "big")


def full_rank(matrix: Array) -> None:
    """Refuse a likelihood design with no residual degrees of freedom or deficient rank."""
    if not np.isfinite(matrix).all() or matrix.shape[0] <= matrix.shape[1] or np.linalg.matrix_rank(matrix) != matrix.shape[1]:
        raise NotEstimable("design rank is deficient or residual degrees of freedom are exhausted")


def with_intercept(matrix: Array) -> Array:
    """Prepend a constant without asking a library to infer one."""
    return np.column_stack((np.ones(len(matrix)), matrix))


def metrics(observed: Array, predicted: Array) -> Scores:
    """Score finite predictions on every row of the common holdout."""
    predicted = np.asarray(predicted, dtype=np.float64).reshape(-1)
    if observed.shape != predicted.shape or not len(observed) or not np.isfinite(predicted).all():
        raise ModelError("predictions differ from the finite common holdout row set")
    error = observed - predicted
    denominator = float(np.sum((observed - np.mean(observed)) ** 2))
    rmse = float(np.sqrt(np.mean(error**2)))
    mae = float(np.mean(np.abs(error)))
    r2 = None if denominator == 0 else float(1 - np.sum(error**2) / denominator)
    return {"rmse": rmse, "mae": mae, "r2": r2}
