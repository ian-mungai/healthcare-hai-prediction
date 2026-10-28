"""Fold-local selection, imputation, scaling and categorical encoding."""

from __future__ import annotations

import math
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from scripts.training.models.common import EstimatorParameters, Feature, ModelError, NotEstimable, Prepared


def records(values: Any) -> tuple[Mapping[str, Any], ...]:
    """Accept extract records or a prediction DataFrame without changing their row order."""
    result = tuple(values) if isinstance(values, Sequence) and not isinstance(values, str | bytes) else tuple(values.to_dict(orient="records"))
    if not result or any(not isinstance(row, Mapping) for row in result):
        raise ModelError("preprocessing requires nonempty feature records")
    return result


def missing(value: Any) -> bool:
    """Recognize source nulls and floating missing values without zero filling."""
    return value is None or value is getattr(sys.modules.get("pandas"), "NA", None) or isinstance(value, float | np.floating) and math.isnan(value)


def _numbers(values: Sequence[Any]) -> Any:
    raw = np.asarray(values)
    if raw.dtype.kind not in "biuf" and (
        raw.dtype.kind != "O" or any(value is not None and not isinstance(value, int | float | np.number | bool) for value in values)
    ):
        raise ModelError("numeric predictor contains a nonfinite or nonnumeric value")
    numbers = np.asarray(values, dtype=np.float64)
    if np.isinf(numbers).any():
        raise ModelError("numeric predictor contains a nonfinite or nonnumeric value")
    return numbers


@dataclass(frozen=True)
class FeatureBatch:
    """Validated source columns that can be indexed without repeating record conversion."""

    rows: tuple[Mapping[str, Any], ...]
    columns: Mapping[str, Any]
    encoded_categories: Mapping[str, tuple[tuple[str | None, ...], Any]] = field(default_factory=dict)

    @classmethod
    def from_records(cls, values: Any, features: tuple[Feature, ...]) -> FeatureBatch:
        """Convert numeric values and categorical nulls once at the record boundary."""
        if isinstance(values, cls):
            return values
        rows = records(values)
        columns = {}
        for feature in features:
            name = feature.name
            source = [row[name] for row in rows]
            columns[feature.reference] = (
                _numbers(source) if feature.kind == "number" else np.asarray([None if missing(value) else str(value) for value in source], dtype=object)
            )
        return cls(rows, columns)

    def encode_categories(self, levels: Mapping[str, tuple[str | None, ...]]) -> FeatureBatch:
        """Cache category codes for one frozen analysis encoding without learning numeric statistics."""
        encoded = {}
        for reference, categories in levels.items():
            lookup = {value: index for index, value in enumerate(categories)}
            codes = np.fromiter((lookup.get(value, -1) for value in self.columns[reference]), dtype=np.intp, count=len(self.rows))
            encoded[reference] = categories, codes
        return FeatureBatch(self.rows, self.columns, encoded)

    def subset(self, positions: Any) -> FeatureBatch:
        """Retain complete sampled hospital blocks with their cached source columns."""
        return FeatureBatch(
            tuple(self.rows[int(index)] for index in positions),
            {name: values[positions] for name, values in self.columns.items()},
            {name: (levels, codes[positions]) for name, (levels, codes) in self.encoded_categories.items()},
        )


class FoldPreprocessor(EstimatorParameters):
    """Fit every learned preprocessing choice on this pipeline's fitting rows only."""

    def __init__(
        self,
        features: tuple[Feature, ...],
        forced: tuple[str, ...] = (),
        native_missing: bool = False,
        category_levels: Mapping[str, tuple[str | None, ...]] | None = None,
        indicator_aliases: Mapping[str, str] | None = None,
    ) -> None:
        self.features, self.forced, self.native_missing = features, forced, native_missing
        self.category_levels, self.indicator_aliases = category_levels, indicator_aliases

    def fit(self, values: Any, y: Any = None) -> FoldPreprocessor:
        """Learn selectors, numeric statistics, exact indicator aliases and categories from fitting rows."""
        batch = FeatureBatch.from_records(values, self.features)
        masks = {
            feature.reference: np.isnan(batch.columns[feature.reference])
            if feature.kind == "number"
            else np.equal(batch.columns[feature.reference], np.asarray(None, dtype=object))
            for feature in self.features
        }
        self.missing_rates_ = {reference: float(np.mean(mask)) for reference, mask in masks.items()}
        self.kept_references_ = tuple(feature.reference for feature in self.features if self.missing_rates_[feature.reference] <= 0.5)
        if not set(self.forced).issubset(self.kept_references_):
            raise NotEstimable("forced covariate exceeds the half-missing threshold or is absent")
        self.medians_: dict[str, float] = {}
        self.scales_: dict[str, float] = {}
        self.centers_: dict[str, float] = {}
        self.categories_: dict[str, tuple[str | None, ...]] = {}
        self.indicator_aliases_: dict[str, str] = dict(self.indicator_aliases or {})
        self.constant_columns_: dict[str, float] = {}
        indicators: dict[bytes, str] = {}
        for feature in self.features:
            if feature.reference not in self.kept_references_:
                continue
            column = batch.columns[feature.reference]
            absent = masks[feature.reference]
            if feature.kind == "number":
                median = float(np.nanmedian(column))
                filled = np.where(absent, median, column)
                self.medians_[feature.reference] = median
                self.centers_[feature.reference] = float(np.mean(filled))
                self.scales_[feature.reference] = float(np.std(filled)) or 1.0
                if np.ptp(filled) == 0 and not (self.native_missing and absent.any()):
                    if feature.reference in self.forced:
                        raise NotEstimable("forced covariate is constant in the fitting rows")
                    self.constant_columns_[feature.name] = median
            else:
                if self.category_levels is not None:
                    categories = self.category_levels[feature.reference]
                else:
                    categories = tuple(sorted(set(column), key=lambda value: (value is not None, value or "")))
                    if absent.any():
                        counts = Counter(value for value in column if value is not None)
                        reference = min(counts, key=lambda value: (-counts[value], value))
                        categories = (reference, *(value for value in categories if value != reference))
                self.categories_[feature.reference] = categories
            if self.indicator_aliases is None and (feature.kind == "category" or not self.native_missing) and self.missing_rates_[feature.reference] > 0:
                signature = np.packbits(absent).tobytes()
                indicator = feature.name + "__missing"
                if signature in indicators:
                    self.indicator_aliases_[indicator] = indicators[signature]
                else:
                    indicators[signature] = indicator
        return self

    def transform(self, values: Any) -> Prepared:
        """Apply frozen fitting-row choices and count unknown categorical values."""
        batch = FeatureBatch.from_records(values, self.features)
        columns, names, references = [], [], []
        unknown = 0
        for feature in self.features:
            if feature.reference not in self.kept_references_:
                continue
            column = batch.columns[feature.reference]
            if feature.kind == "number":
                absent = np.isnan(column)
                numbers = (
                    column
                    if self.native_missing
                    else (np.where(absent, self.medians_[feature.reference], column) - self.centers_[feature.reference]) / self.scales_[feature.reference]
                )
                if feature.name not in self.constant_columns_:
                    columns.append(numbers)
                    names.append(feature.name)
                    references.append(feature.reference)
                indicator = feature.name + "__missing"
                if (
                    not self.native_missing
                    and (self.missing_rates_[feature.reference] > 0 or indicator in self.indicator_aliases_.values())
                    and indicator not in self.indicator_aliases_
                ):
                    columns.append(absent.astype(np.float64))
                    names.append(indicator)
                    references.append(feature.reference)
            else:
                categories = self.categories_[feature.reference]
                cached = batch.encoded_categories.get(feature.reference)
                codes = cached[1] if cached is not None and cached[0] == categories else None
                unknown += int(np.count_nonzero(~np.isin(column, np.asarray(categories, dtype=object)) if codes is None else codes < 0))
                for index, category in enumerate(categories[1:], 1):
                    name = feature.name + "__missing" if category is None else f"{feature.name}__category_{index}"
                    if name in self.indicator_aliases_:
                        continue
                    columns.append(np.asarray(column == category if codes is None else codes == index, dtype=np.float64))
                    names.append(name)
                    references.append(feature.reference)
        matrix = np.column_stack(columns) if columns else np.empty((len(batch.rows), 0))
        indicator_references = {feature.name + "__missing": feature.reference for feature in self.features}
        alias_references: dict[str, tuple[str, ...]] = {}
        for alias, canonical in self.indicator_aliases_.items():
            alias_references[canonical] = (*alias_references.get(canonical, ()), indicator_references[alias])
        return Prepared(matrix, tuple(names), tuple(references), batch.rows, unknown, alias_references)

    def fit_transform(self, values: Any, y: Any = None) -> Prepared:
        """Fit and transform exactly the same fitting rows for a Pipeline step."""
        batch = FeatureBatch.from_records(values, self.features)
        result = self.fit(batch, y).transform(batch)
        self.output_names_ = result.names
        return result

    def get_feature_names_out(self, input_features: Any = None) -> Any:
        """Expose the source feature names for serialized prediction pipelines."""
        del input_features
        return np.asarray(self.output_names_, dtype=object)
