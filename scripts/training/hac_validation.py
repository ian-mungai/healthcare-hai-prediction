"""Post-fit HAC validation with explicit outcome-year to program-year mapping."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

from scripts.training.models.common import Deadline, ModelError


def _key(value: Any) -> tuple[str, int]:
    if not isinstance(value, str) or (match := re.fullmatch(r"([0-9]{6}):([12][0-9]{3})", value)) is None:
        raise ModelError("HAC validation requires a hospital-window key")
    return match[1], int(match[2])


def _flag(row: Mapping[str, Any]) -> int | None:
    if row.get("alignment_status") != "aligned":
        return None
    number, text = row.get("value_number"), row.get("value_text")
    numeric = int(number) if isinstance(number, int | float) and number in (0, 1) else None
    published = {"yes": 1, "no": 0, "1": 1, "0": 0}.get(text.strip().lower()) if isinstance(text, str) else None
    if numeric is not None and published is not None and numeric != published:
        raise ModelError("HAC published and numeric flags disagree")
    return numeric if numeric is not None else published


def _auc(rows: Sequence[tuple[str, float, int]]) -> float | None:
    positives = sum(flag for _, _, flag in rows)
    negatives = len(rows) - positives
    if not positives or not negatives:
        return None
    ordered = sorted(rows, key=lambda row: row[1])
    rank_sum, index = 0.0, 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        average_rank = (index + 1 + end) / 2
        rank_sum += average_rank * sum(flag for _, _, flag in ordered[index:end])
        index = end
    return (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)


def validate_hac(
    predictions: Sequence[Mapping[str, Any]], hac_rows: Sequence[Mapping[str, Any]], year_mapping: Mapping[int, int], deadline: Deadline
) -> dict[str, Any]:
    """Rank selected-model holdout predictions against matched HAC flags without fitting a model.

    The caller supplies the reviewed committed year map. No fixed fiscal-year offset is inferred here.
    Missing flags remain missing. Repeated source windows may name the same consistent hospital-program flag.
    """
    deadline.check()
    if not predictions or not hac_rows:
        return {"status": "not_run", "reason": "no holdout predictions or HAC source", "matched_hospitals": 0, "missing_flags": len(predictions)}
    scores: dict[str, float] = {}
    years: set[int] = set()
    for row in predictions:
        ccn, year = _key(row.get("spine_key"))
        value = row.get("predicted")
        if ccn in scores or not isinstance(value, int | float) or not math.isfinite(value):
            raise ModelError("HAC holdout has a repeated hospital or nonfinite prediction")
        scores[ccn] = float(value)
        years.add(year)
    if len(years) != 1:
        raise ModelError("HAC validation requires the single newest-window holdout")
    year = next(iter(years))
    if year not in year_mapping or type(year_mapping[year]) is not int:
        raise ModelError("HAC year mapping is missing for the outcome window")
    program_year = year_mapping[year]
    flags: dict[tuple[str, int], int | None] = {}
    seen: set[tuple[str, int]] = set()
    for row in hac_rows:
        deadline.check()
        ccn, _ = _key(row.get("spine_key"))
        fiscal_year = row.get("fiscal_year")
        if type(fiscal_year) is not int:
            raise ModelError("HAC source lacks a program fiscal year")
        source_identity = row["spine_key"], fiscal_year
        if source_identity in seen:
            raise ModelError("duplicate HAC hospital-window and program year")
        seen.add(source_identity)
        identity, value = (ccn, fiscal_year), _flag(row)
        if identity in flags and flags[identity] != value:
            raise ModelError("conflicting HAC flags for one hospital and program year")
        flags[identity] = value
    matched: list[tuple[str, float, int]] = []
    for ccn, score in sorted(scores.items()):
        flag = flags.get((ccn, program_year))
        if flag is not None:
            matched.append((ccn, score, flag))
    missing = len(scores) - len(matched)
    if not matched:
        return {
            "status": "not_run",
            "reason": "no flags match the reviewed program year",
            "matched_hospitals": 0,
            "missing_flags": missing,
            "outcome_year": year,
            "program_year": program_year,
        }
    ranked = matched
    top = sorted(ranked, key=lambda row: (-row[1], row[0]))[: math.ceil(len(ranked) / 4)]
    auc = _auc(ranked)
    deadline.check()
    return {
        "status": "fitted",
        "outcome_year": year,
        "program_year": program_year,
        "matched_hospitals": len(ranked),
        "missing_flags": missing,
        "flagged_hospitals": sum(flag for _, _, flag in ranked),
        "auc": auc,
        "auc_reason": "" if auc is not None else "only one flag class",
        "top_quartile_hospitals": len(top),
        "top_quartile_flagged_share": sum(flag for _, _, flag in top) / len(top),
        "score_tie_order": "ccn_ascending",
        "year_mapping": {str(year): program_year},
    }
