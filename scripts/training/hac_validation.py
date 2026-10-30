"""Validation of the selected specification against the HAC Reduction Program flags, on the two-year period that the flag covers.

The newest window with published SIRs is Y. Its program year comes from the reviewed year map and its period covers windows Y-1 and Y. The
specification is refit on the windows through Y-2 only, so no window of the flag's period is in the training set. The result validates that
refit specification, not the final fitted model.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
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


@dataclass(frozen=True)
class HacPlan:
    """The windows of one validation: the outcome window Y, its program year, the last training window and the windows the flag's period covers."""

    outcome_year: int
    program_year: int
    training_last_year: int
    windows: tuple[int, ...]

    @property
    def one_year_period(self) -> bool:
        """Return whether the flag's period covers a single window."""
        return len(self.windows) == 1

    def record(self) -> dict[str, Any]:
        """Return the plan as a JSON document."""
        return {
            "outcome_year": self.outcome_year,
            "program_year": self.program_year,
            "training_last_year": self.training_last_year,
            "windows": list(self.windows),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> HacPlan:
        """Rebuild a plan from its JSON document."""
        try:
            numbers = (record["outcome_year"], record["program_year"], record["training_last_year"], *record["windows"])
            if not record["windows"] or any(type(number) is not int for number in numbers):
                raise TypeError
            return cls(record["outcome_year"], record["program_year"], record["training_last_year"], tuple(record["windows"]))
        except (KeyError, TypeError):
            raise ModelError("HAC validation plan is incomplete") from None


def training_cutoff(outcome_year: int) -> int:
    """Return the last window that the validation-only fit may train on: two windows before the outcome window."""
    return outcome_year - 2


def check_no_overlap(plan: HacPlan) -> None:
    """Refuse a plan whose training windows reach into the windows of the flag's period."""
    if plan.training_last_year >= min(plan.windows):
        raise ModelError(f"HAC training through window {plan.training_last_year} would overlap the flag period that starts at window {min(plan.windows)}")


def plan_validation(window_rows: Mapping[int, int], year_mapping: Mapping[int, int], periods: Mapping[int, tuple[int, int]]) -> HacPlan:
    """Plan one validation from the row count of each window: refuse a missing map entry, a period that does not fit, or windows without rows."""
    if not window_rows:
        raise ModelError("HAC validation requires analysis rows")
    year = max(window_rows)
    if year not in year_mapping or type(year_mapping[year]) is not int:
        raise ModelError(f"HAC year mapping is missing for the outcome window {year}")
    program_year = year_mapping[year]
    if program_year not in periods:
        raise ModelError(f"HAC year mapping file has no period for program year {program_year}")
    first, last = periods[program_year]
    if last != year or first not in (year - 1, year):
        raise ModelError(
            f"HAC period of program year {program_year} runs from {first} to {last}, but it must be window {year} or windows {year - 1} and {year}"
        )
    plan = HacPlan(year, program_year, training_cutoff(year), tuple(range(first, last + 1)))
    for window in plan.windows:
        if not window_rows.get(window):
            raise ModelError(f"HAC validation window {window} has no rows in the extract")
    if not any(rows for window, rows in window_rows.items() if window <= plan.training_last_year):
        raise ModelError(f"HAC validation has no windows through {plan.training_last_year} to train on")
    check_no_overlap(plan)
    return plan


def period_note(plan: HacPlan) -> str:
    """Say which windows the program year's flag is compared with."""
    if plan.one_year_period:
        return f"FY {plan.program_year} covers one year only, so only window {plan.outcome_year} is compared."
    return f"FY {plan.program_year} covers windows {plan.windows[0]} and {plan.windows[-1]}, and both are compared as one average per hospital."


def validate_hac(predictions: Sequence[Mapping[str, Any]], hac_rows: Sequence[Mapping[str, Any]], plan: HacPlan, deadline: Deadline) -> dict[str, Any]:
    """Rank the validation-only predictions against the HAC flags of the plan's program year without fitting a model.

    Each hospital's score is the mean of its predictions over the windows of the flag's period. Missing flags remain missing.
    Repeated source windows may name the same consistent hospital-program flag.
    """
    deadline.check()
    by_hospital: dict[str, dict[int, float]] = {}
    for row in predictions:
        ccn, window = _key(row.get("spine_key"))
        value = row.get("predicted")
        if window not in plan.windows or window in by_hospital.get(ccn, {}) or not isinstance(value, int | float) or not math.isfinite(value):
            raise ModelError("HAC predictions have a window outside the period, a repeated hospital-window or a nonfinite value")
        by_hospital.setdefault(ccn, {})[window] = float(value)
    for window in plan.windows:
        if not any(window in scores for scores in by_hospital.values()):
            raise ModelError(f"HAC validation has no predictions for window {window}")
    scores = {ccn: sum(values.values()) / len(values) for ccn, values in by_hospital.items()}
    facts = {
        "outcome_year": plan.outcome_year,
        "program_year": plan.program_year,
        "training_last_year": plan.training_last_year,
        "windows_compared": list(plan.windows),
        "one_year_period": plan.one_year_period,
        "period_note": period_note(plan),
        "scope": f"This validates the selected specification refit on the windows through {plan.training_last_year}, not the final fitted model.",
        "prediction_rows": len(predictions),
        "hospitals_one_window": sum(len(values) < len(plan.windows) for values in by_hospital.values()),
    }
    if not hac_rows:
        return {"status": "not_run", "reason": "no HAC source", "matched_hospitals": 0, "missing_flags": len(scores), **facts}
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
        flag = flags.get((ccn, plan.program_year))
        if flag is not None:
            matched.append((ccn, score, flag))
    missing = len(scores) - len(matched)
    if not matched:
        return {"status": "not_run", "reason": "no flags match the reviewed program year", "matched_hospitals": 0, "missing_flags": missing, **facts}
    top = sorted(matched, key=lambda row: (-row[1], row[0]))[: math.ceil(len(matched) / 4)]
    auc = _auc(matched)
    deadline.check()
    return {
        "status": "fitted",
        "matched_hospitals": len(matched),
        "missing_flags": missing,
        "flagged_hospitals": sum(flag for _, _, flag in matched),
        "auc": auc,
        "auc_reason": "" if auc is not None else "only one flag class",
        "top_quartile_hospitals": len(top),
        "top_quartile_flagged_share": sum(flag for _, _, flag in top) / len(top),
        "score_tie_order": "ccn_ascending",
        "year_mapping": {str(plan.outcome_year): plan.program_year},
        **facts,
    }
