"""Synthetic cases for the promotion rule and the promotion_check action."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import numpy as np

from scripts.training import promotion
from scripts.training.lane_b_cases import require
from scripts.training.models.common import Deadline

REAL = Path(__file__).resolve().parents[2]
CONFIG = json.loads((REAL / "config/training/promotion.json").read_text())
REGIONS = json.loads((REAL / "config/training/census_regions.json").read_text())["states"]


def rows_for(
    count: int,
    champion_error: float,
    challenger_error: float,
    *,
    seed: int = 5,
    noise: float = 0.0,
    owners: int = 3,
    states: tuple[str, ...] = ("AK", "AL", "CT", "IA"),
) -> list[dict[str, Any]]:
    """Build holdout comparison rows whose champion and challenger errors have fixed size, with small noise so bootstrap draws vary."""
    rng = np.random.default_rng(seed)
    rows = []
    for index in range(count):
        observed = 1.0 + 0.1 * (index % 7)
        jitter = float(rng.normal(scale=noise)) if noise else 0.0
        rows.append(
            {
                "spine_key": f"{900000 + index}:2023",
                "observed_sir": observed,
                "champion_prediction": observed + champion_error + jitter,
                "challenger_prediction": observed + challenger_error + jitter,
                "control_type_code": str(index % owners),
                "provider_subtype_code": "11" if index % 2 else "12",
                "census_region": REGIONS[states[index % len(states)]],
                "bed_size_band": ("under_100", "100_to_299", "300_and_over")[index % 3],
            }
        )
    return rows


def evaluate(rows: list[dict[str, Any]], **changes: Any) -> dict[str, Any]:
    """Run the rule with a small bootstrap, so the cases stay light, and any configuration change a case names."""
    config = copy.deepcopy(CONFIG)
    config["bootstrap"]["draws"] = 200
    config.update(changes)
    return promotion.evaluate(rows, config, seed=11, deadline=Deadline.after(60))


def gate_cases(cases: Any) -> None:
    """Check each condition of the owner's rule at, above and below its boundary."""

    def passes() -> None:
        verdict = evaluate(rows_for(600, 1.0, 0.5, noise=0.05))
        require(verdict["verdict"] == "pass" and verdict["failed"] == [], f"{verdict}")
        require(verdict["conditions"]["interval"]["lower"] > 0 and verdict["rows"] == 600, "the interval or row count is wrong")

    def small_gain() -> None:
        verdict = evaluate(rows_for(600, 1.0, 0.99, noise=0.05))
        require(verdict["verdict"] == "fail" and "rmse_improvement" in verdict["failed"], f"{verdict['failed']}")

    def exact_boundary() -> None:
        verdict = evaluate(rows_for(600, 1.0, 0.98))
        require(verdict["conditions"]["rmse_improvement"]["passed"], f"exactly 2 percent must pass: {verdict['conditions']['rmse_improvement']}")

    def touching_zero() -> None:
        rows = rows_for(600, 1.0, 1.0)
        verdict = evaluate(rows)
        interval = verdict["conditions"]["interval"]
        require(interval["lower"] == 0.0 and not interval["passed"] and "interval" in verdict["failed"], f"an interval that touches zero passed: {interval}")

    def worse_group() -> None:
        rows = rows_for(600, 1.0, 0.5, noise=0.05)
        for row in rows:
            if row["control_type_code"] == "0":
                row["challenger_prediction"] = row["observed_sir"] + 1.06
        verdict = evaluate(rows)
        require("group_regression" in verdict["failed"], f"a group 6 percent worse passed: {verdict['failed']}")
        require(
            any(item["dimension"] == "ownership_type" and item["group"] == "0" for item in verdict["conditions"]["groups"]["failed"]),
            "the failed group is not named",
        )

    def exact_group_limit() -> None:
        rows = rows_for(600, 1.0, 0.5, noise=0.0)
        for row in rows:
            if row["control_type_code"] == "0":
                row["challenger_prediction"] = row["observed_sir"] + 1.05
        verdict = evaluate(rows)
        require(verdict["conditions"]["groups"]["passed"], f"a group exactly 5 percent worse must pass: {verdict['conditions']['groups']}")

    def small_group_ignored() -> None:
        rows = rows_for(600, 1.0, 0.5, noise=0.05)
        for row in rows[:99]:
            row.update(
                control_type_code="7", provider_subtype_code="99", census_region=None, bed_size_band=None, challenger_prediction=row["observed_sir"] + 5.0
            )
        verdict = evaluate(rows)
        groups = verdict["conditions"]["groups"]
        require("group_regression" not in verdict["failed"], f"a group of 99 was judged: {groups['failed']}")
        require({item["group"] for item in groups["ignored_small"]} >= {"7", "99"}, f"the small groups were not listed: {groups['ignored_small']}")

    def small_group_limit() -> None:
        rows = rows_for(600, 1.0, 0.5, noise=0.05)
        for index, row in enumerate(rows):
            if index < 100:
                row["provider_subtype_code"] = "98"
                row["challenger_prediction"] = row["observed_sir"] + 5.0
        verdict = evaluate(rows)
        require("group_regression" in verdict["failed"], "a group of exactly 100 was ignored")

    def null_groups() -> None:
        rows = rows_for(600, 1.0, 0.5, noise=0.05)
        for index, row in enumerate(rows):
            if index % 10 == 0:
                row["census_region"] = None
        verdict = evaluate(rows)
        require(verdict["conditions"]["groups"]["null_counts"]["census_region"] == 60, f"{verdict['conditions']['groups']['null_counts']}")

    def no_champion() -> None:
        rows = rows_for(600, 1.0, 0.5, noise=0.05)
        for row in rows:
            row["champion_prediction"] = None
        verdict = evaluate(rows)
        require(verdict["verdict"] == "no_champion" and verdict["failed"] == [], f"{verdict}")

    def mixed_champion() -> None:
        rows = rows_for(600, 1.0, 0.5, noise=0.05)
        rows[3]["champion_prediction"] = None
        try:
            evaluate(rows)
        except promotion.PromotionError as error:
            require("some rows" in str(error), f"{error}")
        else:
            raise ValueError("a champion with predictions for only some rows was accepted")

    def bands() -> None:
        edges = {99: "under_100", 100: "100_to_299", 299: "100_to_299", 300: "300_and_over"}
        require(all(promotion.bed_band(count, CONFIG["groups"]["bed_size_band"]) == band for count, band in edges.items()), "a bed-size band edge is wrong")
        require(promotion.bed_band(None, CONFIG["groups"]["bed_size_band"]) is None, "a missing bed count got a band")

    def deterministic() -> None:
        rows = rows_for(600, 1.0, 0.5, noise=0.05)
        require(evaluate(rows) == evaluate(rows), "the same rows gave different verdicts")

    cases.check("promotion_rule_passes_clear_improvement", passes)
    cases.check("promotion_rule_fails_small_gain", small_gain)
    cases.check("promotion_rule_accepts_exact_two_percent", exact_boundary)
    cases.check("promotion_rule_fails_interval_touching_zero", touching_zero)
    cases.check("promotion_rule_fails_group_over_five_percent", worse_group)
    cases.check("promotion_rule_accepts_group_exactly_five_percent", exact_group_limit)
    cases.check("promotion_rule_ignores_group_of_99", small_group_ignored)
    cases.check("promotion_rule_judges_group_of_100", small_group_limit)
    cases.check("promotion_rule_counts_null_group_values", null_groups)
    cases.check("promotion_rule_reports_no_champion", no_champion)
    cases.check("promotion_rule_refuses_partial_champion", mixed_champion)
    cases.check("promotion_rule_bed_bands", bands)
    cases.check("promotion_rule_is_deterministic", deterministic)
