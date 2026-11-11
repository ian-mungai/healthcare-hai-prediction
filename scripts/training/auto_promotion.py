"""Automatic promotion: a challenger that passed the promotion rule goes to the canary, the canary checks decide the full rollout, the owner is told.

    python -m scripts.training.auto_promotion plan --research research_HAI_1.json --percent 10
    python -m scripts.training.auto_promotion verdict --report replay_report.json --research research_HAI_1.json
    python -m scripts.training.auto_promotion record --report replay_report.json --research research_HAI_1.json
    python -m scripts.training.auto_promotion release --model m2 --features-run <run ID>
    python -m scripts.training.auto_promotion notify --hai-type HAI_1 --outcome promoted --detail <text>

``plan`` reads the research record of a type and prints the parameters of the canary deployment and of the promotion, or ``{"challenger": null}``. ``verdict``
judges the replay report of the canary with the owner's pass rule and the offline predictions of the research record, and prints ``promote`` or ``hold``
with the failed checks. ``record`` does the same and, on a pass, tags the challenger's version with the result, bound to the version and its model run
(``MLFLOW_TRACKING_URI``); the ``promote`` action refuses a version without it. Both refuse, by the name in ``code``, a replay of another infection type than
the record's; ``record`` also refuses a registry version that is not the record's challenger (other registered model, source run, model, type, base run or
selected training run), so a record and replay from another tracking store certify nothing. ``release`` loads the held online
data of the model's feature set, which only a model with another feature schema than the online one needs (``schema_changed``); a set that has no hold
releases nothing (``scripts.features.online_release``). ``notify`` publishes the
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
from scripts.training import job_inputs, promote, registry, run_tracking

RELEASE_SECONDS = 1800.0
MAX_RELATIVE_DIFFERENCE = 1e-6
OUTCOMES = ("promoted", "held", "rolled_back")


class PlanRefused(ValueError):
    """The research record cannot start a promotion."""


class CandidateRefused(ValueError):
    """The research record, the replay report and the registry version do not name one candidate; ``code`` names the check that failed."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


def check_replay(record: Mapping[str, Any], report: Mapping[str, Any]) -> None:
    """Refuse a replay of another infection type than the research record's, and a record whose challenger names no source run."""
    hai_type = record.get("hai_type")
    if hai_type not in training_contract.HAI_TYPES or report.get("hai_type") != hai_type:
        raise CandidateRefused("replay_type_differs", f"the replay is of {report.get('hai_type')!r}, the research record of {hai_type!r}")
    source_run = (record["promotion"]["challenger"] or {}).get("source_run")
    if not isinstance(source_run, str) or not source_run:
        raise CandidateRefused("research_source_run_missing", "the research record's challenger names no source run, so no version can be bound to it")


def check_candidate(store: Any, record: Mapping[str, Any], name: str, version: Any) -> None:
    """Refuse a registry version that is not the research record's challenger.

    The registered model is the one the model contract names for the record's type, and the version came from the challenger's source run, was
    registered for the challenger's model, type and base run, and its source run is the training run the record selected for that model.
    """
    challenger, hai_type = record["promotion"]["challenger"], record["hai_type"]
    contract = job_inputs.read_json(job_inputs.ROOT / "data_contracts/training/model_contract.json", "model_contract")
    if contract["registered_models"].get(hai_type) != name:
        raise CandidateRefused("candidate_registered_model_differs", f"{name} is not the registered model of {hai_type}")
    tags = dict(getattr(version, "tags", {}) or {})
    source_run = challenger["source_run"]
    if str(version.run_id) != source_run or tags.get("source_run_id") != source_run:
        raise CandidateRefused(
            "candidate_source_run_differs",
            f"version {version.version} of {name} came from {version.run_id}, the research record's challenger from {source_run}",
        )
    for key, expected in (("model", challenger["model"]), ("hai_type", hai_type), ("base_run", record.get("base_run"))):
        if tags.get(key) != expected:
            raise CandidateRefused(
                f"candidate_{key}_differs", f"version {version.version} of {name} has {key} {tags.get(key)!r}, the research record {expected!r}"
            )
    training_run = store.get_run(source_run).data.tags.get("run_id")
    if training_run != record.get("runs", {}).get(challenger["model"]):
        raise CandidateRefused(
            "candidate_training_run_differs", f"the source run {source_run} belongs to the training run {training_run!r}, not to the one the record selected"
        )


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

    Every request answered 200 on every attempt, so a request that met a rate limit or an error first fails (``requests_answered``), and each request's
    total time from its first send to its answer, retries and waits included, is within the API's time limit (``replay.REQUEST_SECONDS``). This reads the
    owner's rule as the lead recorded it for the owner. Both sides hold at least ``compare.MIN_ROWS`` rows. The canary
    side was served by the candidate version, and each canary prediction matches the one the research job scored offline for the same version and
    hospital-window (``offline`` maps ``<CCN>:<year>`` to it) within ``MAX_RELATIVE_DIFFERENCE``. A canary row with no offline prediction fails.
    """
    failed = []
    statuses = report.get("statuses", {})
    rows = [row for row in report.get("rows", []) if isinstance(row, Mapping)]
    canary = [row for row in rows if row.get("served_by") == "canary"]
    stable = [row for row in rows if row.get("served_by") == "stable"]
    answered = {"200": report.get("requests")}
    if (
        not report.get("requests")
        or statuses != answered
        or report.get("attempt_statuses") != answered
        or len(rows) != report["requests"]
        or any(not row.get("attempts") or any(attempt.get("status") != 200 for attempt in row["attempts"]) for row in rows)
    ):
        failed.append("requests_answered")
    if not rows or any(not isinstance(row.get("elapsed_seconds"), int | float) or not 0 <= row["elapsed_seconds"] <= replay.REQUEST_SECONDS for row in rows):
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


def record_canary(store: Any, record: Mapping[str, Any], report: Mapping[str, Any]) -> dict[str, Any]:
    """Judge the canary of the research record's challenger and, only on a pass, tag its version with the result bound to its number and its model run.

    The replay, the research record and the registry version must name one candidate (``check_replay``, ``check_candidate``), or nothing is tagged.
    """
    check_replay(record, report)
    name, number = record["promotion"]["registered_model"], str(record["promotion"]["challenger"]["version"])
    version = next((item for item in registry.versions_of(store, name) if str(item.version) == number), None)
    if version is None:
        raise ValueError(f"{name} has no version {number}")
    check_candidate(store, record, name, version)
    result = canary_checks(report, record["promotion"]["challenger_predictions"], number)
    if result["decision"] == "promote":
        for key, value in promote.canary_tags(number, str(version.run_id)).items():
            store.set_model_version_tag(name, number, key, value)
    return result


def schema_changed(promoted_tags: Mapping[str, str], online_tags: Mapping[str, str] | None) -> bool:
    """True when the promoted version was trained on another feature schema than the version the online store serves, or when none is served."""
    return online_tags is None or promoted_tags["feature_list_sha256"] != online_tags["feature_list_sha256"]


def release(model: str, features_run: str) -> str:
    """Load the held online data of the model's feature set for the run that trained the promoted model, after the canary's full rollout.

    Returns ``released``, ``already_released`` or ``not_held`` (the run loaded online by itself): ``scripts.features.online_release`` explains each."""
    if model not in training_contract.MODELS or not re.fullmatch(training_contract.RUN_ID, features_run):
        raise ValueError("release needs a model m1 to m6 and a feature run ID")
    done = run_command(sys.executable, ["-m", "scripts.features.online_release", "--model", model, "--features-run", features_run], timeout=RELEASE_SECONDS)
    if done.returncode != 0:
        raise RuntimeError(f"the release of {model} for {features_run} failed with exit code {done.returncode}: {done.stderr.strip()[-300:]}")
    return str(json.loads(done.stdout)["outcome"])


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
            check_replay(record, report)
            if arguments.command == "verdict":
                result = canary_checks(report, offline, str(challenger["version"]))
            else:
                uri = os.environ.get("MLFLOW_TRACKING_URI", "")
                if not uri:
                    raise ValueError("MLFLOW_TRACKING_URI is not set")
                result = record_canary(run_tracking.open_store(uri), record, report)
        elif arguments.command == "release":
            outcome = release(arguments.model, arguments.features_run)
            result = {"released": arguments.model, "features_run": arguments.features_run, "outcome": outcome}
        else:
            import boto3

            topic = os.environ.get("ALERTS_TOPIC_ARN", "")
            if not topic:
                raise ValueError("ALERTS_TOPIC_ARN is not set")
            result = {"message_id": notify(boto3.client("sns"), topic, arguments.hai_type, arguments.outcome, arguments.detail)}
    except (PlanRefused, ValueError, RuntimeError, OSError, KeyError) as error:
        failure = {"command": arguments.command, "failure": type(error).__name__, "detail": str(error)}
        if isinstance(error, CandidateRefused):
            failure["code"] = error.code
        sys.stderr.write(json.dumps(failure) + "\n")
        return 2
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
