"""E2E cases for the drift gate of one model: which types train, which skip and why, from the baseline and the drift records."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from monitoring import aws_cases, drift_gate, reader_cases
from monitoring.drift_check import CheckError

Problems = list[str]
SHA_A, SHA_B = "a" * 64, "b" * 64


def record(triggered: bool, skip_reason: str | None = None, excluded: tuple[str, ...] = ()) -> dict[str, Any]:
    return {
        "kind": "drift_record",
        "triggered": triggered,
        "skip_reason": skip_reason,
        "excluded_features": [{"name": name, "reason": "all_missing_in_reference"} for name in excluded],
    }


def refused(call: Callable[[], Any], text: str) -> Problems:
    try:
        call()
    except (drift_gate.GateRefused, CheckError) as error:
        return [] if text in str(error) else [f"the refusal does not name {text}: {error}"]
    return [f"no refusal naming {text}"]


def drifted_model_trains(folder: Path) -> Problems:
    decision = drift_gate.decide("m2", {"monitored": ["HAI_1", "HAI_2"], "unmonitored": {}}, {"HAI_1": record(True), "HAI_2": record(False)})
    expected = {"HAI_1": {"train": True, "reason": "drifted"}, "HAI_2": {"train": False, "reason": "not_drifted"}}
    return [] if decision["decisions"] == expected and decision["train_types"] == ["HAI_1"] and decision["train"] else [f"decision {decision}"]


def quiet_model_skips_with_its_reasons(folder: Path) -> Problems:
    records = {"HAI_1": record(False), "HAI_2": record(False, "no_new_window")}
    decision = drift_gate.decide("m4", {"monitored": ["HAI_1", "HAI_2"], "unmonitored": {}}, records)
    expected = {"HAI_1": "not_drifted", "HAI_2": "no_new_window"}
    return [] if not decision["train"] and decision["train_types"] == [] and decision["skip_reasons"] == expected else [f"decision {decision}"]


def excluded_features_do_not_change_the_decision(folder: Path) -> Problems:
    """The gate reads ``triggered`` and the skip reason; a record that names excluded features gives the decision of the same record without them."""
    problems = []
    for triggered, skip_reason in ((True, None), (False, None), (False, "no_new_window")):
        plain = drift_gate.decide("m2", {"monitored": ["HAI_1"], "unmonitored": {}}, {"HAI_1": record(triggered, skip_reason)})
        named = drift_gate.decide("m2", {"monitored": ["HAI_1"], "unmonitored": {}}, {"HAI_1": record(triggered, skip_reason, ("c142__value__number",))})
        if plain != named:
            problems.append(f"an excluded feature changed the decision for triggered={triggered} {skip_reason}: {plain} against {named}")
    return problems


def model_without_a_production_model_trains(folder: Path) -> Problems:
    for reason in ("no_production_model", "production_model_of_another_model", "feature_schema_changed", "no_features"):
        decision = drift_gate.decide("m1", {"monitored": [], "unmonitored": {"HAI_3": reason}}, {})
        if decision["decisions"] != {"HAI_3": {"train": True, "reason": reason}} or decision["train_types"] != ["HAI_3"]:
            return [f"{reason} did not train: {decision}"]
    return []


def unknown_reason_or_model_is_refused(folder: Path) -> Problems:
    return (
        refused(lambda: drift_gate.decide("m2", {"monitored": [], "unmonitored": {"HAI_1": "artifact_missing"}}, {}), "does not allow a decision")
        + refused(lambda: drift_gate.decide("m7", {"monitored": [], "unmonitored": {}}, {}), "m7")
        + refused(lambda: drift_gate.decide("m2", {"monitored": ["HAI_1"], "unmonitored": {}}, {}), "no drift record")
    )


def baseline_names_a_production_model_of_another_model(folder: Path) -> Problems:
    baseline = reader_cases.Baseline(folder, manifest=reader_cases.manifest_document(model="m2", feature_list_sha256=SHA_A))
    return refused(lambda: fetch_for(baseline, model="m4"), "production_model_of_another_model")


def baseline_names_a_changed_feature_schema(folder: Path) -> Problems:
    baseline = reader_cases.Baseline(folder, manifest=reader_cases.manifest_document(model="m2", feature_list_sha256=SHA_A))
    return refused(lambda: fetch_for(baseline, model="m2", feature_list_sha=SHA_B), "feature_schema_changed")


def fetch_for(baseline: reader_cases.Baseline, **gate: Any) -> Any:
    from monitoring import baseline as module

    return module.fetch(baseline.store, "HAI_1", baseline.out, project=baseline.work.project, **gate)


def baseline_of_the_same_model_and_schema_is_fetched(folder: Path) -> Problems:
    baseline = reader_cases.Baseline(folder, manifest=reader_cases.manifest_document(model="m2", feature_list_sha256=SHA_A))
    fetched = fetch_for(baseline, model="m2", feature_list_sha=SHA_A)
    return [] if fetched.hai_type == "HAI_1" and (baseline.out / "reference.parquet").is_file() else ["the matching baseline was not fetched"]


def published_marker_carries_the_decision(folder: Path) -> Problems:
    s3 = aws_cases.MemoryS3()
    work, _ = aws_cases.publish_records(folder, s3, gate_model="m2")
    marker = json.loads(s3.objects[("bucket-for-tests", f"drift/{aws_cases.RUN}/_complete.json")])
    gate = marker.get("gate", {})
    problems = [] if marker.get("model") == "m2" and gate.get("model") == "m2" else [f"the marker is {marker}"]
    if gate.get("decisions") != {"HAI_1": {"train": False, "reason": "no_new_window"}} or gate.get("train") is not False:
        problems.append(f"the gate decision is {gate}")
    return problems


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    return [
        ("gate_trains_a_drifted_type_and_skips_a_quiet_one", drifted_model_trains),
        ("gate_skips_a_quiet_model_with_a_reason_per_type", quiet_model_skips_with_its_reasons),
        ("gate_ignores_excluded_features", excluded_features_do_not_change_the_decision),
        ("gate_trains_without_a_production_model_of_the_model_or_after_a_schema_change", model_without_a_production_model_trains),
        ("gate_refuses_an_unknown_reason_an_unknown_model_and_a_missing_record", unknown_reason_or_model_is_refused),
        ("gate_baseline_names_a_production_model_of_another_model", baseline_names_a_production_model_of_another_model),
        ("gate_baseline_names_a_changed_feature_schema", baseline_names_a_changed_feature_schema),
        ("gate_baseline_of_the_same_model_and_schema_is_fetched", baseline_of_the_same_model_and_schema_is_fetched),
        ("gate_marker_carries_the_decision", published_marker_carries_the_decision),
    ]
