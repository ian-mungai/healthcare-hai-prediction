"""Compare the predictions of the stable and the canary model over the same replayed requests.

The comparison reports counts, the mean, three quantiles and the two-sample Kolmogorov-Smirnov distance of the two prediction sets. It judges nothing.
Whether the canary may become the production model is the owner's decision. A side with fewer than ``MIN_ROWS`` rows gets no comparison.
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from typing import Any

MIN_ROWS = 30


def quantile(ordered: Sequence[float], share: float) -> float:
    """The linear-interpolation quantile of an ordered, non-empty list."""
    position = share * (len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def summary(values: Sequence[float]) -> dict[str, Any]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "mean": statistics.fmean(ordered),
        "p10": quantile(ordered, 0.1),
        "p50": quantile(ordered, 0.5),
        "p90": quantile(ordered, 0.9),
    }


def ks_statistic(first: Sequence[float], second: Sequence[float]) -> float:
    """The largest gap between the two empirical distribution functions."""
    a, b = sorted(first), sorted(second)
    i = j = 0
    gap = 0.0
    while i < len(a) and j < len(b):
        value = min(a[i], b[j])
        while i < len(a) and a[i] <= value:
            i += 1
        while j < len(b) and b[j] <= value:
            j += 1
        gap = max(gap, abs(i / len(a) - j / len(b)))
    return gap


def compare(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Compare the predictions served by the stable and by the canary model. Rows carry ``served_by`` and ``prediction``."""
    stable = [float(row["prediction"]) for row in rows if row["served_by"] == "stable"]
    canary = [float(row["prediction"]) for row in rows if row["served_by"] == "canary"]
    report: dict[str, Any] = {"stable": summary(stable), "canary": summary(canary), "minimum_rows": MIN_ROWS}
    if len(stable) < MIN_ROWS or len(canary) < MIN_ROWS:
        return report | {"status": "insufficient"}
    return report | {"status": "ok", "ks_statistic": ks_statistic(stable, canary), "mean_shift": report["canary"]["mean"] - report["stable"]["mean"]}
