"""The drift gate of one model: after its feature set is published, train when the model drifted, its feature schema changed or it has no Production model.

The gate reads what the drift task already produced for the model: which types the baseline phase could monitor and why it could not monitor the others
(``baseline.fetch``), and the drift record of each monitored type. A type trains when

- the baseline phase found no Production model of this model for it (``no_production_model``, ``production_model_of_another_model``, ``no_features``),
- the feature schema of the model differs from the one its Production model trained on (``feature_schema_changed``), or
- its drift record says ``triggered`` (at least half of the model's features drifted, the rule of ``config/monitoring/drift.json``).

Every other type records a skip with the reason. A feature with no current values and a current window under the minimum rows refuse earlier, in
``drift_check``, and so refuse the whole decision by name. The decision carries the types to train and, when there are none, the reason for each skip.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from orchestration.training_contract import HAI_TYPES, MODELS

TRAIN_WITHOUT_CHECK = ("no_production_model", "production_model_of_another_model", "feature_schema_changed", "no_features")


class GateRefused(ValueError):
    """The records do not support a decision; the message names what is wrong."""


def decide(model: str, baseline: Mapping[str, Any], records: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Return the decision of the gate for ``model`` from the baseline phase's record and the drift record of each monitored type."""
    if model not in MODELS:
        raise GateRefused(f"{model} is not one of {', '.join(MODELS)}")
    monitored, unmonitored = list(baseline["monitored"]), dict(baseline["unmonitored"])
    if set(monitored) & set(unmonitored) or any(hai_type not in HAI_TYPES for hai_type in (*monitored, *unmonitored)):
        raise GateRefused("the baseline record names a type twice or an unknown type")
    decisions: dict[str, dict[str, Any]] = {}
    for hai_type, reason in sorted(unmonitored.items()):
        if reason not in TRAIN_WITHOUT_CHECK:
            raise GateRefused(f"{hai_type} was not monitored for the reason {reason}, which does not allow a decision")
        decisions[hai_type] = {"train": True, "reason": reason}
    for hai_type in sorted(monitored):
        record = records.get(hai_type)
        if record is None or not isinstance(record.get("triggered"), bool):
            raise GateRefused(f"{hai_type} is monitored and has no drift record with a triggered flag")
        if record["triggered"]:
            decisions[hai_type] = {"train": True, "reason": "drifted"}
        else:
            decisions[hai_type] = {"train": False, "reason": str(record.get("skip_reason") or "not_drifted")}
    train = [hai_type for hai_type in HAI_TYPES if decisions.get(hai_type, {}).get("train")]
    skipped = {hai_type: item["reason"] for hai_type, item in sorted(decisions.items()) if not item["train"]}
    return {
        "schema": 1,
        "kind": "drift_gate",
        "model": model,
        "decisions": decisions,
        "train_types": train,
        "train": bool(train),
        "skip_reasons": {} if train else skipped,
    }
