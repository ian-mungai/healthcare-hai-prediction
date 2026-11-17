"""Hand the results of the training, research, canary and promotion phases to their exit push, as numbers and counts with closed labels.

Each function takes the object its phase already holds: the selection of the fit, the research record of a type, the canary decision, the parameters of an
executed promotion and the verdict of the owner notice. The labels are the HAI type, the model key (m1 to m6), the model slot, the promotion action, the canary
decision and the notice verdict, all closed. A run ID, a registered model name or free text never becomes a label. Anything unexpected adds nothing.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from scripts import metrics_push

MODELS = ("m1", "m2", "m3", "m4", "m5", "m6")
HAI_TYPES = tuple(f"HAI_{index}" for index in range(1, 7))
VERDICTS = ("promoted", "promoted_not_rolled_out", "held", "rolled_back")


def number(value: Any) -> float | None:
    """A finite number or None; a bool is no number here."""
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        return None
    return float(value)


def version(value: Any) -> float | None:
    """A registry version as a number, or None when the text is not a whole number."""
    return float(int(value)) if isinstance(value, str | int) and not isinstance(value, bool) and str(value).isdecimal() else None


def fit_selection(model: str, hai_type: str, selection: Mapping[str, Any]) -> None:
    """The holdout RMSE of each model slot of one type's fit, the selected slot and whether a registry candidate was chosen."""
    try:
        if model not in MODELS or hai_type not in HAI_TYPES:
            return
        rmse = selection.get("comparison_rmse")
        for slot, value in rmse.items() if isinstance(rmse, Mapping) else []:
            if (found := number(value)) is not None and metrics_push.SLOT.fullmatch(str(slot)):
                metrics_push.add("hai_training_holdout_rmse", found, hai_type=hai_type, model=model, slot=str(slot))
        chosen = selection.get("selected")
        if isinstance(chosen, str) and metrics_push.SLOT.fullmatch(chosen):
            metrics_push.add("hai_training_selected", 1, hai_type=hai_type, model=model, slot=chosen)
        metrics_push.add("hai_training_challenger_registered", 1 if selection.get("registry_candidate") else 0, hai_type=hai_type, model=model)
    except Exception:
        return


def research_record(document: Mapping[str, Any]) -> None:
    """The candidates, the one challenger and the Production version of one type, from the research record the phase stores."""
    try:
        hai_type = document.get("hai_type")
        promotion = document.get("promotion")
        if hai_type not in HAI_TYPES or not isinstance(promotion, Mapping):
            return
        candidates = [item for item in promotion.get("candidates", []) if isinstance(item, Mapping) and item.get("model") in MODELS]
        for item in candidates:
            if (found := number(item.get("rmse"))) is not None:
                metrics_push.add("hai_research_candidate_rmse", found, hai_type=hai_type, model=str(item["model"]))
        metrics_push.add("hai_research_eligible_candidates", sum(1 for item in candidates if item.get("eligible") is True), hai_type=hai_type)
        challenger = promotion.get("challenger")
        metrics_push.add("hai_research_challenger", 1 if isinstance(challenger, Mapping) else 0, hai_type=hai_type)
        if isinstance(challenger, Mapping) and challenger.get("model") in MODELS:
            metrics_push.add("hai_research_challenger_model", 1, hai_type=hai_type, model=str(challenger["model"]))
        if (found := version(promotion.get("production_version"))) is not None:
            metrics_push.add("hai_research_production_version", found, hai_type=hai_type)
    except Exception:
        return


def canary_decision(hai_type: str, decision: Mapping[str, Any]) -> None:
    """The decision of the canary of one type: promote or hold."""
    try:
        word = decision.get("decision")
        if hai_type in HAI_TYPES and word in ("promote", "hold"):
            metrics_push.add("hai_canary_result", 1, hai_type=hai_type, decision=str(word))
    except Exception:
        return


def notice_verdict(hai_type: str, verdict: str) -> None:
    """What became of the type's challenger, as the owner is told."""
    try:
        if hai_type in HAI_TYPES and verdict in VERDICTS:
            metrics_push.add("hai_promotion_notice", 1, hai_type=hai_type, verdict=verdict)
    except Exception:
        return


def promotion_executed(parameters: Mapping[str, Any]) -> None:
    """A promotion or rollback that moved the Production alias: its mode and the version that is now Production."""
    try:
        hai_type, mode = parameters.get("hai_type"), parameters.get("promotion_mode")
        if hai_type not in HAI_TYPES or mode not in ("promote", "rollback"):
            return
        metrics_push.add("hai_promotion_executed", 1, hai_type=str(hai_type), action=str(mode))
        if (found := version(parameters.get("model_version"))) is not None:
            metrics_push.add("hai_promotion_version", found, hai_type=str(hai_type))
    except Exception:
        return
