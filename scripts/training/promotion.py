"""The owner's promotion rule: a challenger replaces the champion only when all three conditions hold on the same newest-period holdout.

1. The challenger RMSE is at least ``minimum_rmse_improvement`` lower than the champion's. The comparison rounds to 12 places, so exactly 2 percent passes.
2. The percentile interval of the relative RMSE improvement, from seeded hospital-level bootstrap resamples, excludes zero. A lower bound of zero fails.
3. In every group of at least ``minimum_group_size`` holdout hospitals, the challenger RMSE is at most ``maximum_group_rmse_increase`` above the champion's.
   The groups are ownership type, hospital subtype, census region and bed-size band.
   A hospital with no value in a dimension is in no group of that dimension and is counted.

The rule reads comparison rows only. The promotion check and the promote action call the same function on the same written rows.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from scripts.training.models.common import Deadline
from scripts.training.research_tests import hospital_resamples

DIMENSIONS = {
    "ownership_type": "control_type_code",
    "hospital_subtype": "provider_subtype_code",
    "census_region": "census_region",
    "bed_size_band": "bed_size_band",
}
COLUMNS = {
    "spine_key": "varchar",
    "observed_sir": "double",
    "champion_prediction": "double",
    "challenger_prediction": "double",
    "control_type_code": "varchar",
    "provider_subtype_code": "varchar",
    "census_region": "varchar",
    "bed_size_band": "varchar",
}
PLACES = 12


class PromotionError(ValueError):
    """The comparison rows cannot be judged."""


def bed_band(count: Any, spec: Mapping[str, Any]) -> str | None:
    """Return the bed-size band of a bed count, with each cut point in the band it opens, or None when the count is missing."""
    if isinstance(count, bool) or not isinstance(count, int | float) or not math.isfinite(count):
        return None
    return str(spec["bands"][sum(count >= cut for cut in spec["cut_points"])])


def comparison_rows(
    holdout: Sequence[Mapping[str, Any]],
    label: str,
    challenger: Mapping[str, float],
    champion: Mapping[str, float] | None,
    config: Mapping[str, Any],
    regions: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Build one comparison row per holdout hospital-window, sorted by key, with the four group inputs."""
    groups = config["groups"]
    rows = []
    for row in sorted(holdout, key=lambda item: item["spine_key"]):
        key = row["spine_key"]
        state = row.get(groups["census_region"]["column"])
        rows.append(
            {
                "spine_key": key,
                "observed_sir": float(row[label]),
                "champion_prediction": None if champion is None else float(champion[key]),
                "challenger_prediction": float(challenger[key]),
                "control_type_code": _text(row.get(groups["ownership_type"]["column"])),
                "provider_subtype_code": _text(row.get(groups["hospital_subtype"]["column"])),
                "census_region": regions.get(state) if isinstance(state, str) else None,
                "bed_size_band": bed_band(row.get(groups["bed_size_band"]["column"]), groups["bed_size_band"]),
            }
        )
    return rows


def _text(value: Any) -> str | None:
    return None if value is None or (isinstance(value, float) and math.isnan(value)) else str(value)


def _rmse(observed: np.ndarray[Any, Any], predicted: np.ndarray[Any, Any]) -> float:
    return float(np.sqrt(np.mean((observed - predicted) ** 2)))


def _finite(values: Sequence[Any], name: str) -> np.ndarray[Any, Any]:
    array = np.asarray(values, dtype=np.float64)
    if not array.size or not np.isfinite(array).all():
        raise PromotionError(f"{name} must be finite for every holdout row")
    return array


def evaluate(rows: Sequence[Mapping[str, Any]], config: Mapping[str, Any], *, seed: int, deadline: Deadline) -> dict[str, Any]:
    """Judge comparison rows by the three conditions and return the verdict with each condition's value."""
    if not rows:
        raise PromotionError("there are no holdout comparison rows")
    observed = _finite([row["observed_sir"] for row in rows], "the observed SIR")
    challenger = _finite([row["challenger_prediction"] for row in rows], "the challenger prediction")
    present = [row["champion_prediction"] is not None for row in rows]
    summary: dict[str, Any] = {"rows": len(rows), "hospitals": len({str(row["spine_key"]).split(":")[0] for row in rows})}
    if not any(present):
        return {**summary, "verdict": "no_champion", "failed": [], "conditions": {}}
    if not all(present):
        raise PromotionError("the champion predicts some rows only")
    champion = _finite([row["champion_prediction"] for row in rows], "the champion prediction")
    champion_rmse, challenger_rmse = _rmse(observed, champion), _rmse(observed, challenger)
    if champion_rmse <= 0:
        raise PromotionError("the champion RMSE is zero, so a relative improvement is undefined")
    improvement = (champion_rmse - challenger_rmse) / champion_rmse
    minimum = float(config["minimum_rmse_improvement"])
    settings = config["bootstrap"]
    entities = [str(row["spine_key"]).split(":")[0] for row in rows]
    draws = []
    for positions in hospital_resamples(entities, int(settings["draws"]), seed):
        deadline.check()
        base, other = _rmse(observed[positions], champion[positions]), _rmse(observed[positions], challenger[positions])
        draws.append((base - other) / base if base > 0 else 0.0)
    tail = (1 - float(settings["confidence_level"])) / 2
    lower, upper = (float(value) for value in np.quantile(draws, [tail, 1 - tail], method="linear"))
    groups = _groups(rows, observed, champion, challenger, config, champion_rmse, challenger_rmse)
    conditions = {
        "rmse_improvement": {"value": improvement, "threshold": minimum, "passed": round(improvement, PLACES) >= round(minimum, PLACES)},
        "interval": {
            "lower": lower,
            "upper": upper,
            "confidence_level": settings["confidence_level"],
            "draws": int(settings["draws"]),
            "seed": seed,
            "passed": lower > 0,
        },
        "groups": groups,
    }
    failed = [
        name
        for name, key in (("rmse_improvement", "rmse_improvement"), ("interval", "interval"), ("group_regression", "groups"))
        if not conditions[key]["passed"]
    ]
    return {
        **summary,
        "verdict": "fail" if failed else "pass",
        "failed": failed,
        "champion_rmse": champion_rmse,
        "challenger_rmse": challenger_rmse,
        "conditions": conditions,
    }


def _groups(
    rows: Sequence[Mapping[str, Any]],
    observed: np.ndarray[Any, Any],
    champion: np.ndarray[Any, Any],
    challenger: np.ndarray[Any, Any],
    config: Mapping[str, Any],
    champion_rmse: float,
    challenger_rmse: float,
) -> dict[str, Any]:
    minimum = int(config["minimum_group_size"])
    limit = round(1 + float(config["maximum_group_rmse_increase"]), PLACES)
    checked: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    small: list[dict[str, Any]] = []
    nulls: dict[str, int] = {}
    for dimension, column in DIMENSIONS.items():
        members: dict[str, list[int]] = defaultdict(list)
        nulls[column] = 0
        for index, row in enumerate(rows):
            if row[column] is None:
                nulls[column] += 1
            else:
                members[str(row[column])].append(index)
        for group, positions in sorted(members.items()):
            if len(positions) < minimum:
                small.append({"dimension": dimension, "group": group, "hospitals": len(positions)})
                continue
            base, other = _rmse(observed[positions], champion[positions]), _rmse(observed[positions], challenger[positions])
            entry = {"dimension": dimension, "group": group, "hospitals": len(positions), "champion_rmse": base, "challenger_rmse": other}
            ratio = other / base if base > 0 else (1.0 if other == 0 else math.inf)
            checked.append(entry | {"ratio": ratio})
            if round(ratio, PLACES) > limit:
                failed.append(entry | {"ratio": ratio})
    return {"passed": not failed, "limit": limit, "checked": checked, "failed": failed, "ignored_small": small, "null_counts": nulls}
