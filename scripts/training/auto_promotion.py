"""Automatic promotion: a challenger that passed the promotion rule goes to the canary, the canary checks decide the full rollout, the owner is told.

    python -m scripts.training.auto_promotion plan --research research_HAI_1.json --percent 10
    python -m scripts.training.auto_promotion verdict --report replay_report.json --research research_HAI_1.json
    python -m scripts.training.auto_promotion record --report replay_report.json --research research_HAI_1.json
    python -m scripts.training.auto_promotion release --model m2 --features-run <run ID>
    python -m scripts.training.auto_promotion notify --hai-type HAI_1 --outcome promoted --detail <text>

``plan`` reads the research record of a type and prints the parameters of the canary deployment and of the promotion, or ``{"challenger": null}``. ``verdict``
judges the replay report of the canary with the owner's pass rule and the offline predictions of the research record, and prints ``promote`` or ``hold``
with the failed checks. ``record`` does the same and, on a pass, tags the challenger's version with the result, bound to the version and its model run
(``MLFLOW_TRACKING_URI``); the ``promote`` action refuses a version without it. ``release`` loads the held online
data of the model's feature set, which only a model with another feature schema than the online one needs (``schema_changed``). ``notify`` publishes the
outcome to the project's alerts topic named by ``ALERTS_TOPIC_ARN``. No step needs the owner's approval, and none moves an alias: the ``promote`` action does.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from orchestration import serving_contract, training_contract
from scripts.process import run_command
from scripts.serving import compare, replay
from scripts.training import promote, registry, run_tracking

RELEASE_SECONDS = 1800.0
MAX_RELATIVE_DIFFERENCE = 1e-6
OUTCOMES = ("promoted", "held", "rolled_back")


class PlanRefused(ValueError):
    """The research record cannot start a promotion."""


def plan(record: Mapping[str, Any], percent: int) -> dict[str, Any]:
    """Return the canary deployment (at ``percent`` of the hospital-windows) and the promotion that follow from a research record, or no challenger."""
    challenger = record["promotion"]["challenger"]
    if record.get("schema") != 1 or record.get("hai_type") not in training_contract.HAI_TYPES:
        raise PlanRefused("the research record has another schema or no known HAI type")
    if challenger is None:
        return {"hai_type": record["hai_type"], "challenger": None}
    if challenger["model"] not in training_contract.PROMOTION_MODELS or challenger["verdict"] not in ("pass", "pass_baseline"):
        raise PlanRefused(f"the challenger {challenger} is not a promotion-eligible model with a passing verdict")
    return {
        "hai_type": record["hai_type"],
        "challenger": challenger,
        "canary": serving_contract.parameters_for(
            "serving_deploy_dag", {"hai_type": record["hai_type"], "candidate_version": challenger["version"], "percent": percent, "deploy_mode": "canary"}
        ),
        "promotion": {"hai_type": record["hai_type"], "model_version": challenger["version"], "promotion_mode": "promote"},
    }


def same_within_rounding(canary: float, offline: float) -> bool:
    """True when two predictions differ by at most ``MAX_RELATIVE_DIFFERENCE`` of the larger one."""
    return abs(canary - offline) <= MAX_RELATIVE_DIFFERENCE * max(abs(canary), abs(offline))


def canary_checks(report: Mapping[str, Any], offline: Mapping[str, float], candidate_version: str) -> dict[str, Any]:
    """Judge the replay report of a canary against the owner's pass rule.

    Every request answered 200, each within the API's time limit (``replay.REQUEST_SECONDS``). Both sides hold at least ``compare.MIN_ROWS`` rows. The canary
    side was served by the candidate version, and each canary prediction matches the one the research job scored offline for the same version and
    hospital-window (``offline`` maps ``<CCN>:<year>`` to it) within ``MAX_RELATIVE_DIFFERENCE``. A canary row with no offline prediction fails.
    """
    failed = []
    statuses = report.get("statuses", {})
    rows = [row for row in report.get("rows", []) if isinstance(row, Mapping)]
    canary = [row for row in rows if row.get("served_by") == "canary"]
    stable = [row for row in rows if row.get("served_by") == "stable"]
    if not report.get("requests") or statuses != {"200": report["requests"]}:
        failed.append("requests_answered")
    if not rows or any(not isinstance(row.get("seconds"), int | float) or not 0 <= row["seconds"] <= replay.REQUEST_SECONDS for row in rows):
        failed.append("within_time_limit")
    comparison = report.get("comparison", {})
    counts = (comparison.get("stable", {}).get("count", 0), comparison.get("canary", {}).get("count", 0), len(stable), len(canary))
    if comparison.get("status") != "ok" or min(counts) < compare.MIN_ROWS:
        failed.append("both_sides_compared")
    if any(str(row.get("model_version")) != candidate_version for row in canary):
        failed.append("canary_version")
    if not canary or any(
        f"{row.get('ccn')}:{row.get('window_year')}" not in offline
        or not same_within_rounding(float(row["prediction"]), float(offline[f"{row.get('ccn')}:{row.get('window_year')}"]))
        for row in canary
    ):
        failed.append("predictions_match_offline")
    return {"decision": "hold" if failed else "promote", "failed": failed}


def record_canary(store: Any, name: str, number: str, report: Mapping[str, Any], offline: Mapping[str, float]) -> dict[str, Any]:
    """Judge the canary of one version and, only on a pass, tag the version with the result bound to its number and its model run."""
    version = next((item for item in registry.versions_of(store, name) if str(item.version) == number), None)
    if version is None:
        raise ValueError(f"{name} has no version {number}")
    result = canary_checks(report, offline, number)
    if result["decision"] == "promote":
        for key, value in promote.canary_tags(number, str(version.run_id)).items():
            store.set_model_version_tag(name, number, key, value)
    return result


def schema_changed(promoted_tags: Mapping[str, str], online_tags: Mapping[str, str] | None) -> bool:
    """True when the promoted version was trained on another feature schema than the version the online store serves, or when none is served."""
    return online_tags is None or promoted_tags["feature_list_sha256"] != online_tags["feature_list_sha256"]


def release(model: str, features_run: str) -> None:
    """Load the held online data of the model's feature set for the run that trained the promoted model."""
    if model not in training_contract.MODELS or not re.fullmatch(training_contract.RUN_ID, features_run):
        raise ValueError("release needs a model m1 to m6 and a feature run ID")
    done = run_command(sys.executable, ["-m", "scripts.features.feast_materialize", "--release", model, features_run], timeout=RELEASE_SECONDS)
    if done.returncode != 0:
        raise RuntimeError(f"the release of {model} for {features_run} failed with exit code {done.returncode}: {done.stderr.strip()[-300:]}")


def notify(client: Any, topic_arn: str, hai_type: str, outcome: str, detail: str) -> str:
    """Tell the owner what happened to a type's challenger through the alerts topic and return the message ID."""
    if outcome not in OUTCOMES or hai_type not in training_contract.HAI_TYPES:
        raise ValueError(f"the notice needs an outcome in {', '.join(OUTCOMES)} and an HAI type")
    answer = client.publish(TopicArn=topic_arn, Subject=f"model {outcome}: {hai_type}"[:100], Message=f"{hai_type}: {outcome}. {detail}"[:1000])
    return str(answer["MessageId"])


def main(argv: Sequence[str] | None = None) -> int:
    """Run one step and print its result as one JSON line; a refusal prints one JSON line to standard error and exits 2."""
    parser = argparse.ArgumentParser(prog="python -m scripts.training.auto_promotion", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    planned = commands.add_parser("plan")
    planned.add_argument("--research", type=Path, required=True)
    planned.add_argument("--percent", type=int, required=True, help="the share of hospital-windows the canary receives, 1 to 50")
    for command in ("verdict", "record"):
        judged = commands.add_parser(command)
        judged.add_argument("--report", type=Path, required=True)
        judged.add_argument("--research", type=Path, required=True)
    released = commands.add_parser("release")
    released.add_argument("--model", required=True)
    released.add_argument("--features-run", required=True)
    told = commands.add_parser("notify")
    told.add_argument("--hai-type", required=True)
    told.add_argument("--outcome", required=True)
    told.add_argument("--detail", required=True)
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "plan":
            result: Any = plan(json.loads(arguments.research.read_bytes()), arguments.percent)
        elif arguments.command in ("verdict", "record"):
            record = json.loads(arguments.research.read_bytes())
            challenger = record["promotion"]["challenger"]
            if challenger is None or not record["promotion"].get("challenger_predictions"):
                raise ValueError("the research record has no challenger with offline predictions")
            report = json.loads(arguments.report.read_bytes())
            offline = record["promotion"]["challenger_predictions"]
            if arguments.command == "verdict":
                result = canary_checks(report, offline, str(challenger["version"]))
            else:
                uri = os.environ.get("MLFLOW_TRACKING_URI", "")
                if not uri:
                    raise ValueError("MLFLOW_TRACKING_URI is not set")
                result = record_canary(run_tracking.open_store(uri), record["promotion"]["registered_model"], str(challenger["version"]), report, offline)
        elif arguments.command == "release":
            release(arguments.model, arguments.features_run)
            result = {"released": arguments.model, "features_run": arguments.features_run}
        else:
            import boto3

            topic = os.environ.get("ALERTS_TOPIC_ARN", "")
            if not topic:
                raise ValueError("ALERTS_TOPIC_ARN is not set")
            result = {"message_id": notify(boto3.client("sns"), topic, arguments.hai_type, arguments.outcome, arguments.detail)}
    except (PlanRefused, ValueError, RuntimeError, OSError, KeyError) as error:
        sys.stderr.write(json.dumps({"command": arguments.command, "failure": type(error).__name__, "detail": str(error)}) + "\n")
        return 2
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
