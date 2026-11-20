"""Fold-local selection, imputation, scaling and categorical encoding."""

from __future__ import annotations

import math
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from scripts.training.models.common import EstimatorParameters, Feature, ModelError, NotEstimable, Prepared, dependent_positions


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


MAX_MISSING_FRACTION = 0.5
WINSOR_PERCENTILES = (1.0, 99.0)


def missing_masks(batch: FeatureBatch, features: tuple[Feature, ...]) -> dict[str, Any]:
    """Return the missing-value mask of every predictor, keyed by reference."""
    return {
        feature.reference: np.isnan(batch.columns[feature.reference])
        if feature.kind == "number"
        else np.equal(batch.columns[feature.reference], np.asarray(None, dtype=object))
        for feature in features
    }


def select_by_missing(masks: Mapping[str, Any], features: tuple[Feature, ...], forced: tuple[str, ...] = ()) -> tuple[tuple[str, ...], dict[str, float]]:
    """Apply the missing-predictor rule: keep the predictors missing in at most half of the rows and refuse a forced covariate that fails it."""
    rates = {reference: float(np.mean(mask)) for reference, mask in masks.items()}
    kept = tuple(feature.reference for feature in features if rates[feature.reference] <= MAX_MISSING_FRACTION)
    if not set(forced).issubset(kept):
        raise NotEstimable("forced covariate exceeds the half-missing threshold or is absent")
    return kept, rates


def missing_selection(values: Any, features: tuple[Feature, ...], forced: tuple[str, ...] = ()) -> tuple[tuple[str, ...], dict[str, float]]:
    """Learn only the missing-predictor selection of the given rows, without any other fold statistic."""
    return select_by_missing(missing_masks(FeatureBatch.from_records(values, features), features), features, forced)


def column_kind(name: str) -> str:
    """The kind of a design column from its name: a missing-value flag, a category level or a value."""
    if name.endswith("__missing"):
        return "missing_flag"
    if name.rsplit("__", 1)[-1].startswith("category_"):
        return "category_level"
    return "value"


class FoldPreprocessor(EstimatorParameters):
    """Fit every learned preprocessing choice on this pipeline's fitting rows only.

    With ``winsorize`` each numeric predictor with more than two distinct observed (not missing) values among the fitting rows is clipped to the 1st and 99th
    percentiles of those values before its median, mean and spread are learned; the same cut points clip every later row. A numeric predictor with at most
    two distinct observed values, categories and the missing-value flags are never clipped."""

    def __init__(
        self,
        features: tuple[Feature, ...],
        forced: tuple[str, ...] = (),
        native_missing: bool = False,
        category_levels: Mapping[str, tuple[str | None, ...]] | None = None,
        indicator_aliases: Mapping[str, str] | None = None,
        drop_dependent: bool = False,
        protected: tuple[str, ...] = (),
        winsorize: bool = False,
    ) -> None:
        self.features, self.forced, self.native_missing = features, forced, native_missing
        self.category_levels, self.indicator_aliases = category_levels, indicator_aliases
        self.drop_dependent, self.protected, self.winsorize = drop_dependent, protected, winsorize

    def fit(self, values: Any, y: Any = None) -> FoldPreprocessor:
        """Learn selectors, cut points, numeric statistics, exact indicator aliases and categories from fitting rows."""
        batch = FeatureBatch.from_records(values, self.features)
        masks = missing_masks(batch, self.features)
        self.kept_references_, self.missing_rates_ = select_by_missing(masks, self.features, self.forced)
        self.medians_: dict[str, float] = {}
        self.bounds_: dict[str, tuple[float, float]] = {}
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
                if self.winsorize and len(np.unique(column[~absent])) > 2:
                    low, high = np.percentile(column[~absent], WINSOR_PERCENTILES)
                    self.bounds_[feature.reference] = float(low), float(high)
                    column = np.clip(column, low, high)
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
        self.dependent_records_: list[dict[str, str]] = []
        if self.drop_dependent:
            self._find_dependent(batch)
        return self

    def _find_dependent(self, batch: FeatureBatch) -> None:
        """Drop the fitting-row columns that are exact combinations of others: forced, protected and value columns are kept before flags and levels."""
        design = self.transform(batch)
        forced, protected = set(self.forced), set(self.protected)

        def rank(position: int) -> tuple[int, int]:
            reference, kind = design.references[position], column_kind(design.names[position])
            group = 0 if reference in forced else 1 if reference in protected else 3
            return (group + (kind != "value"), position)

        dropped = dependent_positions(design.matrix, sorted(range(len(design.names)), key=rank))
        if any(design.references[position] in forced for position in dropped):
            raise NotEstimable("a forced covariate is an exact combination of other fitting-row columns")
        self.dependent_records_ = [
            {"name": design.names[position], "reference": design.references[position], "kind": column_kind(design.names[position])} for position in dropped
        ]

    def transform(self, values: Any) -> Prepared:
        """Apply frozen fitting-row choices, the cut points included, and count unknown categorical values."""
        batch = FeatureBatch.from_records(values, self.features)
        columns, names, references = [], [], []
        unknown = 0
        for feature in self.features:
            if feature.reference not in self.kept_references_:
                continue
            column = batch.columns[feature.reference]
            if feature.kind == "number":
                absent = np.isnan(column)
                if feature.reference in self.bounds_:
                    column = np.clip(column, *self.bounds_[feature.reference])
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
        dropped = {record["name"] for record in getattr(self, "dependent_records_", [])}
        if dropped:
            kept = [index for index, name in enumerate(names) if name not in dropped]
            columns, names, references = [columns[index] for index in kept], [names[index] for index in kept], [references[index] for index in kept]
        matrix = np.column_stack(columns) if columns else np.empty((len(batch.rows), 0))
        indicator_references = {feature.name + "__missing": feature.reference for feature in self.features}
        alias_references: dict[str, tuple[str, ...]] = {}
        for alias, canonical in self.indicator_aliases_.items():
            alias_references[canonical] = (*alias_references.get(canonical, ()), indicator_references[alias])
        return Prepared(matrix, tuple(names), tuple(references), batch.rows, unknown, alias_references)

    def used_references(self, values: Any, columns: Sequence[int] | None = None) -> tuple[str, ...]:
        """The references of the sources that still contribute a column to the fitted design, in the order of the feature list.

        Stage one is the preprocessing alone (``columns`` is None): a source counts when a value or category column of it survives or when its missing
        indicator does, including an indicator that several sources share. A source that the half-missing selector, the constant check or the dependent-column
        removal left without a column is not listed. Stage two is a model that drops columns of the design afterwards, as the fixed-effects panel does for the
        columns that the hospital effects absorb or that depend on others within hospital: it passes the positions it kept, and only a source with a column
        among them counts. The layout does not depend on the values, so any rows of the fitting set (one is enough) give the answer."""
        design = self.transform(values)
        kept = None if columns is None else set(columns)
        return tuple(
            feature.reference
            for feature in self.features
            if (found := design.columns_for([feature.reference])) and (kept is None or not kept.isdisjoint(found))
        )

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
