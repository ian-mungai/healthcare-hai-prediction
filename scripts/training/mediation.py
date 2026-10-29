"""Separate associational mediation analyses for PSI-90 and PSI-13."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime
from typing import Any

import numpy as np

from scripts.training.models.common import Dataset, Deadline, Feature, ModelError, NotEstimable, seed_for
from scripts.training.models.preprocessing import FeatureBatch, FoldPreprocessor
from scripts.training.research_tests import hospital_resamples

MEDIATORS = ("PSI_90", "PSI_13")


def _period_end(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ModelError("mediator period end is not a calendar date") from error
    raise ModelError("mediator period end is missing")


def _products(
    batch: FeatureBatch,
    features: tuple[Feature, ...],
    processes: tuple[str, ...],
    covariates: tuple[str, ...],
    outcome: np.ndarray[Any, Any],
    mediator: np.ndarray[Any, Any],
    positions: np.ndarray[Any, Any] | None = None,
    encoding: FoldPreprocessor | None = None,
    dropped_covariates: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    prepare = FoldPreprocessor(
        features,
        forced=covariates,
        category_levels=None if encoding is None else encoding.categories_,
        indicator_aliases=None if encoding is None else encoding.indicator_aliases_,
    )
    design = prepare.fit_transform(batch)
    if not set(processes).issubset(prepare.kept_references_):
        raise NotEstimable("a process exposure exceeds the half-missing threshold in the mediation sample")
    retained = tuple(index for index, reference in enumerate(design.references) if reference in processes or np.any(design.matrix[:, index]))
    if len(retained) != len(design.names):
        if dropped_covariates is not None:
            dropped_covariates.extend(name for index, name in enumerate(design.names) if index not in retained)
        design = replace(
            design,
            matrix=design.matrix[:, retained],
            names=tuple(design.names[index] for index in retained),
            references=tuple(design.references[index] for index in retained),
        )
    matrix = np.column_stack((np.ones(len(outcome)), design.matrix))
    if len(outcome) <= matrix.shape[1] + 1:
        raise NotEstimable("mediation residual degrees of freedom are exhausted")
    responses = np.column_stack((mediator, outcome))
    sample_shape = matrix.shape
    if positions is not None:
        _, first, counts = np.unique(positions, return_index=True, return_counts=True)
        weights = np.sqrt(counts)[:, None]
        matrix, responses = matrix[first] * weights, responses[first] * weights
    coefficients, _, rank, singular = np.linalg.lstsq(matrix, responses, rcond=np.finfo(np.float64).eps * max(sample_shape))
    if rank != matrix.shape[1]:
        raise NotEstimable("mediation design rank is deficient")
    residuals = responses - matrix @ coefficients
    mediator_residual = residuals[:, 0]
    tolerance = np.finfo(np.float64).eps * max(sample_shape) * max(float(singular[0]), float(np.linalg.norm(mediator)))
    if float(np.linalg.norm(mediator_residual)) <= tolerance:
        raise NotEstimable("mediator is fully represented by the process and covariate design")
    b = float(mediator_residual @ residuals[:, 1] / (mediator_residual @ mediator_residual))
    effects = {}
    for index, (name, reference) in enumerate(zip(design.names, design.references, strict=True)):
        if reference not in processes or name.endswith("__missing"):
            continue
        scale = prepare.scales_.get(reference, 1.0)
        indirect = float(coefficients[index + 1, 0]) / scale * b
        total = float(coefficients[index + 1, 1]) / scale
        effects[name] = {"indirect_effect": indirect, "direct_effect": total - indirect, "total_effect": total}
    if not effects:
        raise NotEstimable("mediation has no retained process exposure contrasts")
    return effects


def analyze_mediation(
    data: Dataset,
    mediator_rows: Sequence[Mapping[str, Any]],
    process_references: tuple[str, ...],
    covariate_references: tuple[str, ...],
    config: Mapping[str, Any],
    deadline: Deadline,
) -> dict[str, Any]:
    """Estimate separate product-of-coefficients associations using only aligned prior-period mediators.

    Each bootstrap draw resamples whole hospitals and refits numeric preprocessing with the full-sample categorical encoding.
    This function neither imports the model selector nor returns predictors.
    """
    deadline.check()
    catalog = {feature.reference: feature for feature in data.features}
    references = (*process_references, *covariate_references)
    if not process_references or len(set(references)) != len(references) or not set(references).issubset(catalog):
        raise ModelError("mediation requires distinct registered process and covariate references")
    features = tuple(catalog[reference] for reference in references)
    outcomes = {row["spine_key"]: row for row in data.rows}
    aligned: dict[str, dict[str, float]] = {name: {} for name in MEDIATORS}
    missing = dict.fromkeys(MEDIATORS, 0)
    seen: set[tuple[str, str]] = set()
    for row in mediator_rows:
        deadline.check()
        name, key = row.get("measure"), row.get("spine_key")
        if name not in MEDIATORS or not isinstance(key, str):
            raise ModelError("mediator input contains another measure or a missing key")
        identity = key, name
        if identity in seen:
            raise ModelError("duplicate mediator hospital-window and measure")
        seen.add(identity)
        if key not in outcomes:
            continue
        value = row.get("value_number")
        if row.get("alignment_status") != "aligned" or not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(value):
            missing[name] += 1
            continue
        if _period_end(row.get("period_end")) >= date(outcomes[key]["window_year"], 1, 1):
            raise ModelError("mediator period must end before the outcome window")
        aligned[name][key] = float(value)
    results: dict[str, Any] = {}
    settings = config["bootstrap"]["mediation"]
    for name in MEDIATORS:
        deadline.check()
        selected = tuple(row for row in data.rows if row["spine_key"] in aligned[name])
        excluded = len(data.rows) - len(selected)
        if not selected:
            results[name] = {"status": "not_run", "reason": "no aligned finite mediator values", "included_rows": 0, "excluded_rows": excluded}
            continue
        mediator = np.asarray([aligned[name][row["spine_key"]] for row in selected], dtype=np.float64)
        batch = FeatureBatch.from_records(selected, features)
        outcome = np.asarray([row[data.label] for row in selected], dtype=np.float64)
        try:
            encoding = FoldPreprocessor(features, forced=covariate_references).fit(batch)
            batch = batch.encode_categories(encoding.categories_)
            effects = _products(batch, features, process_references, covariate_references, outcome, mediator, None, encoding)
        except (NotEstimable, np.linalg.LinAlgError) as error:
            results[name] = {"status": "not_estimable", "reason": str(error), "included_rows": len(selected), "excluded_rows": excluded}
            continue
        seed = seed_for(config["master_seed"], data.hai_type, "mediation_" + name)
        bootstraps: dict[str, list[float]] = {key: [] for key in effects}
        invalid = 0
        draws_with_dropped_covariate_columns = 0
        budget = deadline.within(config["budgets"]["bootstrap_seconds"])
        for positions in hospital_resamples(tuple(row["ccn"] for row in selected), settings["draws"], seed):
            budget.check()
            sample = batch.subset(positions)
            dropped_columns: list[str] = []
            try:
                replicate = _products(
                    sample, features, process_references, covariate_references, outcome[positions], mediator[positions], positions, encoding, dropped_columns
                )
                if set(replicate) != set(effects):
                    raise NotEstimable("bootstrap process contrast identity changed")
            except (NotEstimable, np.linalg.LinAlgError):
                invalid += 1
                continue
            finally:
                draws_with_dropped_covariate_columns += bool(dropped_columns)
            for key in effects:
                bootstraps[key].append(replicate[key]["indirect_effect"])
        budget.check()
        confidence = float(settings["confidence_level"])
        tail = (1 - confidence) / 2
        for key in effects:
            effects[key]["interval"] = np.quantile(bootstraps[key], [tail, 1 - tail], method="linear").tolist() if len(bootstraps[key]) >= 2 else None
        results[name] = {
            "status": "fitted" if settings["draws"] - invalid >= 2 else "not_estimable",
            "reason": "" if settings["draws"] - invalid >= 2 else "fewer than two estimable bootstrap resamples",
            "interpretation": "associational",
            "effect_unit": "original_numeric_exposure_unit_or_categorical_contrast",
            "included_rows": len(selected),
            "excluded_rows": excluded,
            "invalid_mediator_rows": missing[name],
            "effects": effects,
            "bootstrap": {
                "draws": settings["draws"],
                "valid_draws": settings["draws"] - invalid,
                "invalid_draws": invalid,
                "draws_with_dropped_covariate_columns": draws_with_dropped_covariate_columns,
                "seed": seed,
                "unit": "hospital",
                "paired": True,
                "generator": "numpy.random.default_rng",
                "interval_method": "percentile",
                "confidence_level": confidence,
            },
        }
    deadline.check()
    return {"hai_type": data.hai_type, "mediators": results}
