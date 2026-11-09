"""Automatic promotion: a challenger that passed the promotion rule goes to the canary, the canary checks decide the full rollout, the owner is told.

    python -m scripts.training.auto_promotion plan --research research_HAI_1.json --percent 10
    python -m scripts.training.auto_promotion verdict --report replay_report.json
    python -m scripts.training.auto_promotion release --model m2 --features-run <run ID>
    python -m scripts.training.auto_promotion notify --hai-type HAI_1 --outcome promoted --detail <text>

``plan`` reads the research record of a type and prints the parameters of the canary deployment and of the promotion, or ``{"challenger": null}``. ``verdict``
judges the replay report of the canary with the canary checks and prints ``promote`` or ``hold`` with the failed checks. ``release`` loads the held online
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
from scripts.serving import compare

RELEASE_SECONDS = 1800.0
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


def canary_checks(report: Mapping[str, Any]) -> dict[str, Any]:
    """Judge the replay report of a canary: every request answered, and both sides large enough to compare."""
    failed = []
    statuses = report.get("statuses", {})
    if not report.get("requests") or statuses != {"200": report["requests"]}:
        failed.append("requests_answered")
    comparison = report.get("comparison", {})
    if comparison.get("status") != "ok" or min(comparison.get("stable", {}).get("count", 0), comparison.get("canary", {}).get("count", 0)) < compare.MIN_ROWS:
        failed.append("both_sides_compared")
    return {"decision": "hold" if failed else "promote", "failed": failed}


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
    commands.add_parser("verdict").add_argument("--report", type=Path, required=True)
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
        elif arguments.command == "verdict":
            result = canary_checks(json.loads(arguments.report.read_bytes()))
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
