"""Cases for the training, research and promotion metrics that the training job phases push at their exit.

Each function of ``scripts.training.training_metrics`` turns the result object that its phase already holds into numbers and counts with closed labels (HAI
type, model key, model slot, action, decision, verdict). A run ID, a registered model name and free text never become labels. The cases feed the functions the
shapes the code produces (the fit selection, the research record, the canary decision, the promotion parameters). Nothing here can fail a job, and the last case
checks that each phase calls its function. No AWS service is contacted.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from scripts import metrics_push
from scripts.training import training_metrics

RUN = "training_elastic_net_0a1b2c3d"


def pending() -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return {(sample.name, tuple(sorted(sample.labels.items()))): sample.value for sample in metrics_push.drain()}


def key(name: str, **labels: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    return name, tuple(sorted(labels.items()))


def a_fit_selection_becomes_rmse_per_slot_and_a_selection(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    selection = {
        "comparison_rmse": {"m1_ols": 0.31, "m2_elastic_net": 0.27, "m4_boosting": 0.25, "bad slot!": 1.0, "m3_mixed": float("nan")},
        "selected": "m4_boosting",
        "registry_candidate": "m4_boosting",
    }
    training_metrics.fit_selection("m4", "HAI_2", selection)
    training_metrics.fit_selection("m9", "HAI_2", selection)
    training_metrics.fit_selection("m1", "HAI_2", {"comparison_rmse": "x", "selected": None, "registry_candidate": None})
    found = pending()
    return {
        "each slot has its holdout RMSE": found.get(key("hai_training_holdout_rmse", hai_type="HAI_2", model="m4", slot="m1_ols")) == 0.31
        and found.get(key("hai_training_holdout_rmse", hai_type="HAI_2", model="m4", slot="m4_boosting")) == 0.25,
        "the selected slot is flagged": found.get(key("hai_training_selected", hai_type="HAI_2", model="m4", slot="m4_boosting")) == 1,
        "a registry candidate counts as a challenger registered": found.get(key("hai_training_challenger_registered", hai_type="HAI_2", model="m4")) == 1,
        "a slot name outside the closed form and a NaN are dropped": key("hai_training_holdout_rmse", hai_type="HAI_2", model="m4", slot="bad slot!")
        not in found
        and key("hai_training_holdout_rmse", hai_type="HAI_2", model="m4", slot="m3_mixed") not in found,
        "an unknown model key adds nothing": not any(dict(labels).get("model") == "m9" for _, labels in found),
        "no candidate means no challenger series for that model": key("hai_training_challenger_registered", hai_type="HAI_2", model="m1") in found
        and found[key("hai_training_challenger_registered", hai_type="HAI_2", model="m1")] == 0,
    }


def research_record(challenger: str | None, production: str | None = "7") -> dict[str, object]:
    candidates = [
        {"model": "m1", "version": "3", "verdict": "ok", "rmse": 0.31, "eligible": True, "source_run": RUN},
        {"model": "m2", "version": "4", "verdict": "ok", "rmse": 0.27, "eligible": True, "source_run": RUN},
        {"model": "m4", "version": None, "verdict": "missing_version", "rmse": None, "eligible": False, "source_run": RUN},
    ]
    won = None if challenger is None else {"model": challenger, "version": "4", "verdict": "ok", "source_run": RUN}
    return {
        "hai_type": "HAI_3",
        "promotion": {"registered_model": "hai_3_model", "production_version": production, "candidates": candidates, "challenger": won},
    }


def a_research_record_becomes_candidates_and_a_challenger(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    training_metrics.research_record(research_record("m2"))
    found = pending()
    metrics_push.drain()
    training_metrics.research_record(research_record(None))
    none = pending()
    return {
        "each candidate with a holdout RMSE has one": found.get(key("hai_research_candidate_rmse", hai_type="HAI_3", model="m1")) == 0.31
        and found.get(key("hai_research_candidate_rmse", hai_type="HAI_3", model="m2")) == 0.27
        and key("hai_research_candidate_rmse", hai_type="HAI_3", model="m4") not in found,
        "the eligible candidates are counted": found.get(key("hai_research_eligible_candidates", hai_type="HAI_3")) == 2,
        "a challenger is flagged with its model": found.get(key("hai_research_challenger", hai_type="HAI_3")) == 1
        and found.get(key("hai_research_challenger_model", hai_type="HAI_3", model="m2")) == 1,
        "the Production version of the type is a number": found.get(key("hai_research_production_version", hai_type="HAI_3")) == 7,
        "no challenger is a zero and no model series": none.get(key("hai_research_challenger", hai_type="HAI_3")) == 0
        and not any(name == "hai_research_challenger_model" for name, _ in none),
        "no run ID or registered model name is a label": all(
            "2026" not in value and "hai_3_model" not in value for _, labels in {**found, **none} for _, value in labels
        ),
    }


def a_research_record_without_a_production_version_adds_no_version(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    training_metrics.research_record(research_record("m1", None))
    training_metrics.research_record({"promotion": "broken"})
    training_metrics.research_record({})
    found = pending()
    return {
        "no Production model means no version series": not any(name == "hai_research_production_version" for name, _ in found),
        "a broken record raises nothing": True,
    }


def the_canary_decision_and_the_notice_verdict_are_closed_words(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    training_metrics.canary_decision("HAI_1", {"decision": "promote", "failed": []})
    training_metrics.canary_decision("HAI_2", {"decision": "hold", "failed": ["replay_report_missing"]})
    training_metrics.canary_decision("HAI_9", {"decision": "hold"})
    training_metrics.canary_decision("HAI_3", {"decision": "<script>"})
    for verdict in ("promoted", "promoted_not_rolled_out", "held", "rolled_back", "strange"):
        training_metrics.notice_verdict("HAI_4", verdict)
    found = pending()
    return {
        "a canary that passes and one that holds": found.get(key("hai_canary_result", hai_type="HAI_1", decision="promote")) == 1
        and found.get(key("hai_canary_result", hai_type="HAI_2", decision="hold")) == 1,
        "an unknown HAI type or decision word adds nothing": not any(
            dict(labels).get("hai_type") in {"HAI_9", "HAI_3"} for name, labels in found if name == "hai_canary_result"
        ),
        "the four notice verdicts are series, a fifth is not": {dict(labels)["verdict"] for name, labels in found if name == "hai_promotion_notice"}
        == {"promoted", "promoted_not_rolled_out", "held", "rolled_back"},
    }


def an_executed_promotion_or_rollback_names_its_mode_and_the_new_version(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    training_metrics.promotion_executed({"hai_type": "HAI_5", "model_version": "12", "promotion_mode": "promote", "mlflow": "aws"})
    training_metrics.promotion_executed({"hai_type": "HAI_6", "model_version": "3", "promotion_mode": "rollback", "mlflow": "aws"})
    training_metrics.promotion_executed({"hai_type": "HAI_6", "model_version": "x", "promotion_mode": "weird"})
    found = pending()
    return {
        "a promotion is counted with its action": found.get(key("hai_promotion_executed", hai_type="HAI_5", action="promote")) == 1,
        "a rollback is its own series": found.get(key("hai_promotion_executed", hai_type="HAI_6", action="rollback")) == 1,
        "the version made Production is a number": found.get(key("hai_promotion_version", hai_type="HAI_5")) == 12
        and found.get(key("hai_promotion_version", hai_type="HAI_6")) == 3,
        "an unknown mode or a version that is not a number adds nothing": not any(dict(labels).get("action") == "weird" for _, labels in found),
    }


def every_new_metric_is_in_the_push_catalog_with_closed_labels(root: Path) -> dict[str, bool]:
    names = (
        "hai_training_holdout_rmse",
        "hai_training_selected",
        "hai_training_challenger_registered",
        "hai_research_candidate_rmse",
        "hai_research_challenger",
        "hai_research_challenger_model",
        "hai_research_eligible_candidates",
        "hai_research_production_version",
        "hai_canary_result",
        "hai_promotion_executed",
        "hai_promotion_version",
        "hai_promotion_notice",
    )
    labels = {label for name in names for label in metrics_push.CATALOG[name][1]}
    return {
        "all twelve are in the catalog": all(name in metrics_push.CATALOG for name in names),
        "the only label names are hai_type, model, slot, action, decision and verdict (never outcome, which is a grouping key)": labels
        <= {"hai_type", "model", "slot", "action", "decision", "verdict"},
    }


def each_phase_calls_its_function(root: Path) -> dict[str, bool]:
    source = {
        name: (root / name).read_text(encoding="utf-8")
        for name in ("scripts/training/aws_run.py", "scripts/training/research.py", "scripts/training/job_actions.py")
    }
    return {
        "the canary phase hands over its decision": "training_metrics.canary_decision(" in source["scripts/training/aws_run.py"],
        "the notify phase hands over the verdict": "training_metrics.notice_verdict(" in source["scripts/training/aws_run.py"],
        "the promote phase hands over the executed promotion": "training_metrics.promotion_executed(" in source["scripts/training/aws_run.py"],
        "the research action hands over its record": "training_metrics.research_record(" in source["scripts/training/research.py"],
        "the log_run plan hands over the fit selection": "training_metrics.fit_selection(" in source["scripts/training/job_actions.py"],
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "training_a_fit_selection_becomes_rmse_per_slot_and_a_selection": a_fit_selection_becomes_rmse_per_slot_and_a_selection,
    "training_a_research_record_becomes_candidates_and_a_challenger": a_research_record_becomes_candidates_and_a_challenger,
    "training_a_research_record_without_a_production_version_adds_no_version": a_research_record_without_a_production_version_adds_no_version,
    "training_the_canary_decision_and_the_notice_verdict_are_closed_words": the_canary_decision_and_the_notice_verdict_are_closed_words,
    "training_an_executed_promotion_or_rollback_names_its_mode_and_the_new_version": an_executed_promotion_or_rollback_names_its_mode_and_the_new_version,
    "training_every_new_metric_is_in_the_push_catalog_with_closed_labels": every_new_metric_is_in_the_push_catalog_with_closed_labels,
    "training_each_phase_calls_its_function": each_phase_calls_its_function,
}
