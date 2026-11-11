"""The alert address reaches the alerts root from .env, the topic from the platform output, and no CI path or other stack ever holds the subscription."""

import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import render_platform_config as renderer
from scripts.infrastructure import render_project_config
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "owner@example.org"
TOPIC = f"arn:aws:sns:eu-west-1:{'1' * 12}:example_project_alerts_dev"
ENVIRONMENT = {
    "AWS_ACCOUNT_ID": "1" * 12,
    "AWS_PROFILE": "example_project_dev",
    "AWS_REGION": "eu-west-1",
    "ENVIRONMENT": "dev",
    "PROJECT_NAME": "example_project",
    "S3_BUCKET": "example-project-ci-bucket",
}
PROJECT_KEYS = {"aws_profile", "aws_region", "environment", "expected_account_id", "project_name"}


def env_file(tmp_path: Path, **extra: str) -> Path:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in {**ENVIRONMENT, **extra}.items()) + "\n", encoding="utf-8")
    return path


def platform_outputs(monkeypatch: pytest.MonkeyPatch, **changes: Any) -> None:
    """Stand in for ``terraform output -json`` of the platform stack."""
    outputs: dict[str, Any] = {"alerts_topic_arn": {"value": TOPIC, "type": "string"}}
    outputs.update(changes)
    monkeypatch.setattr(renderer, "terraform_document", lambda *arguments, **keywords: outputs)


def render(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stack: str, **extra: str) -> dict[str, Any]:
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", stack, "--env-file", str(env_file(tmp_path, **extra))])
    renderer.main()
    return json.loads((tmp_path / "infra" / stack / "deployment.auto.tfvars.json").read_text(encoding="utf-8"))


def test_a_set_alert_address_is_rendered_for_the_alerts_root_with_the_platform_topic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    platform_outputs(monkeypatch)
    values = render(tmp_path, monkeypatch, "alerts", ALERT_EMAIL=ADDRESS)
    check(set(values) == PROJECT_KEYS | {"alerts_topic_arn", "alert_email"}, "the root gets the five project keys, the topic and the address, and no other key")
    check(values["alert_email"] == ADDRESS and values["alerts_topic_arn"] == TOPIC, "the address comes from .env and the topic from the platform output")


@pytest.mark.parametrize("extra", [{}, {"ALERT_EMAIL": ""}])
def test_an_unset_or_empty_alert_address_renders_no_address_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra: dict[str, str]) -> None:
    platform_outputs(monkeypatch)
    values = render(tmp_path, monkeypatch, "alerts", **extra)
    check(set(values) == PROJECT_KEYS | {"alerts_topic_arn"}, "no address input is rendered, so the root keeps its empty default")


@pytest.mark.parametrize("stack", ["platform", "services"])
def test_no_other_stack_receives_the_alert_address(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stack: str) -> None:
    outputs = {name: {"value": "x"} for name in renderer.SERVICES_PLATFORM_KEYS}
    outputs["log_group_names"] = {"value": {"jobs": "x"}}
    outputs["ecr_repository_urls"] = {"value": {"airflow": "x"}}
    platform_outputs(monkeypatch, **outputs)
    values = render(tmp_path, monkeypatch, stack, ALERT_EMAIL=ADDRESS)
    check("alert_email" not in values and "alerts_topic_arn" not in values, f"the {stack} inputs hold no alert address and no alerts topic")
    check(ADDRESS not in json.dumps(values), f"the {stack} inputs never hold the address")


@pytest.mark.parametrize("output", [{}, {"alerts_topic_arn": {"value": None}}, {"alerts_topic_arn": {"value": ""}}, {"alerts_topic_arn": {"value": 3}}])
def test_a_missing_or_invalid_platform_topic_output_renders_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], output: dict[str, Any]
) -> None:
    monkeypatch.setattr(renderer, "terraform_document", lambda *arguments, **keywords: output)
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "alerts", "--env-file", str(env_file(tmp_path, ALERT_EMAIL=ADDRESS))])
    with pytest.raises(SystemExit):
        renderer.main()
    error = capsys.readouterr().err
    check("alerts_topic_arn" in error and "platform" in error, "the error names the output and the stack that provides it")
    check(ADDRESS not in error, "the address is not echoed")
    check(not (tmp_path / "infra").exists(), "nothing is rendered")


def test_the_rendered_file_is_owner_only_and_check_mode_detects_a_changed_address(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    platform_outputs(monkeypatch)
    render(tmp_path, monkeypatch, "alerts", ALERT_EMAIL=ADDRESS)
    target = tmp_path / "infra" / "alerts" / "deployment.auto.tfvars.json"
    check(target.stat().st_mode & 0o777 == 0o600, "the rendered file is owner-only")
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "alerts", "--env-file", str(env_file(tmp_path, ALERT_EMAIL=ADDRESS)), "--check"])
    renderer.main()
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "alerts", "--env-file", str(env_file(tmp_path, ALERT_EMAIL="other@example.org")), "--check"])
    with pytest.raises(SystemExit):
        renderer.main()


def test_an_invalid_alert_address_renders_nothing_and_is_not_echoed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    platform_outputs(monkeypatch)
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["renderer", "--stack", "alerts", "--env-file", str(env_file(tmp_path, ALERT_EMAIL="owner@example.org,other@example.org"))]
    )
    with pytest.raises(SystemExit):
        renderer.main()
    check("owner@example.org" not in capsys.readouterr().err, "the rejected value is not echoed")
    check(not (tmp_path / "infra").exists(), "nothing is rendered")


def test_the_alerts_root_takes_no_preparation_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    platform_outputs(monkeypatch)
    inputs = tmp_path / "inputs.json"
    inputs.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["renderer", "--stack", "alerts", "--env-file", str(env_file(tmp_path)), "--preparation-inputs", str(inputs), "--preparation-sha256", "0" * 64],
    )
    with pytest.raises(SystemExit):
        renderer.main()
    check("no preparation inputs" in capsys.readouterr().err, "the error says the root takes no preparation inputs")
    check(not (tmp_path / "infra").exists(), "nothing is rendered")


def test_a_saved_alerts_plan_with_the_rendered_inputs_is_accepted_and_a_different_address_is_not() -> None:
    declared = {"alert_email": {"default": "", "sensitive": True}, "alerts_topic_arn": {}, "project_name": {}}
    plan = {"complete": True, "errored": False, "configuration": {"root_module": {"variables": declared}}}
    expected = {"project_name": "example_project", "alerts_topic_arn": TOPIC, "alert_email": ADDRESS}
    shown = {"project_name": {"value": "example_project"}, "alerts_topic_arn": {"value": TOPIC}}
    renderer.verify_platform_plan({**plan, "variables": {**shown, "alert_email": {"value": ADDRESS}}}, expected, "alerts")
    with pytest.raises(render_project_config.ConfigurationError):
        renderer.verify_platform_plan({**plan, "variables": {**shown, "alert_email": {"value": "other@example.org"}}}, expected, "alerts")
    with pytest.raises(render_project_config.ConfigurationError):
        renderer.verify_platform_plan(
            {**plan, "variables": {**shown, "alerts_topic_arn": {"value": TOPIC + "x"}, "alert_email": {"value": ADDRESS}}}, expected, "alerts"
        )
    renderer.verify_platform_plan(
        {**plan, "variables": {**shown, "alert_email": {"value": ""}}}, {"project_name": "example_project", "alerts_topic_arn": TOPIC}, "alerts"
    )


def test_the_platform_stack_declares_no_alert_subscription_and_no_alert_address() -> None:
    declared = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "infra/platform").glob("*.tf"))
    check("aws_sns_topic_subscription" not in declared, "the platform stack holds no subscription")
    check("alert_email" not in declared, "the platform stack declares no alert address")
    tests = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "infra/platform/tests").glob("*.hcl"))
    check("alert_email" not in tests and "aws_sns_topic_subscription" not in tests, "no platform test mentions the subscription")


def test_no_ci_path_plans_the_alerts_root() -> None:
    """The plan role never reads the subscription, so no workflow, saved-plan script or state-read list may name the alerts root."""
    for path in sorted((ROOT / ".github").rglob("*")):
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            check(not re.search(r"infra/alerts|\balerts\b.*stack|stack.*\balerts\b", text), f"{path.relative_to(ROOT)} names the alerts root")
    workflow = (ROOT / ".github/workflows/terraform_plan.yml").read_text(encoding="utf-8")
    check(re.search(r"stack: \[platform, services, jobs\]", workflow) is not None, "the plan matrix keeps its three stacks")
    oidc = (ROOT / "infra/iam/github_oidc.tf").read_text(encoding="utf-8")
    check('github_state_stacks = ["platform", "services", "jobs"]' in oidc, "the plan role's stack list keeps its three stacks")


def test_the_budget_topic_name_is_the_one_the_iam_stack_creates() -> None:
    budget = (ROOT / "infra/iam/budget.tf").read_text(encoding="utf-8")
    check('name = format(local.platform_name, "budget_alerts")' in budget, "the IAM stack names the budget alerts topic budget_alerts")
    check(
        'platform_name        = "${var.project_name}_%s_${var.environment}"' in (ROOT / "infra/iam/roles.tf").read_text(encoding="utf-8"),
        "the name pattern is unchanged",
    )
    alerts = (ROOT / "infra/alerts/main.tf").read_text(encoding="utf-8")
    check("${var.project_name}_budget_alerts_${var.environment}" in alerts, "the alerts root builds the same name")
