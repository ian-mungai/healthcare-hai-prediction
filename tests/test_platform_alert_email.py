"""The alert address reaches the platform stack from .env as one input, and only the platform stack."""

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import render_platform_config as renderer
from scripts.infrastructure import render_project_config
from tests.support import check

ADDRESS = "owner@example.org"
ENVIRONMENT = {
    "AWS_ACCOUNT_ID": "1" * 12,
    "AWS_PROFILE": "example_project_dev",
    "AWS_REGION": "eu-west-1",
    "ENVIRONMENT": "dev",
    "PROJECT_NAME": "example_project",
    "S3_BUCKET": "example-project-ci-bucket",
}
PLATFORM_KEYS = {"aws_profile", "aws_region", "environment", "expected_account_id", "project_name"}


def env_file(tmp_path: Path, **extra: str) -> Path:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in {**ENVIRONMENT, **extra}.items()) + "\n", encoding="utf-8")
    return path


def render(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *arguments: str, **extra: str) -> dict[str, Any]:
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--env-file", str(env_file(tmp_path, **extra)), *arguments])
    renderer.main()
    return json.loads((tmp_path / "infra" / "platform" / "deployment.auto.tfvars.json").read_text(encoding="utf-8"))


def test_a_set_alert_address_is_rendered_as_the_platform_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    values = render(tmp_path, monkeypatch, ALERT_EMAIL=ADDRESS)
    check(values == {**{key: values[key] for key in PLATFORM_KEYS}, "alert_email": ADDRESS}, "the platform inputs hold the five project keys and the address")
    check(set(values) == PLATFORM_KEYS | {"alert_email"}, "no other key is rendered")


@pytest.mark.parametrize("extra", [{}, {"ALERT_EMAIL": ""}])
def test_an_unset_or_empty_alert_address_renders_no_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra: dict[str, str]) -> None:
    values = render(tmp_path, monkeypatch, **extra)
    check(set(values) == PLATFORM_KEYS, "no alert input is rendered, so the stack keeps its empty default")


def test_the_rendered_file_is_owner_only_and_check_mode_detects_a_changed_address(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    render(tmp_path, monkeypatch, ALERT_EMAIL=ADDRESS)
    target = tmp_path / "infra" / "platform" / "deployment.auto.tfvars.json"
    check(target.stat().st_mode & 0o777 == 0o600, "the rendered file is owner-only")
    monkeypatch.setattr(sys, "argv", ["renderer", "--env-file", str(env_file(tmp_path, ALERT_EMAIL=ADDRESS)), "--check"])
    renderer.main()
    monkeypatch.setattr(sys, "argv", ["renderer", "--env-file", str(env_file(tmp_path, ALERT_EMAIL="other@example.org")), "--check"])
    with pytest.raises(SystemExit):
        renderer.main()


def test_an_invalid_alert_address_renders_nothing_and_is_not_echoed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--env-file", str(env_file(tmp_path, ALERT_EMAIL="owner@example.org,other@example.org"))])
    with pytest.raises(SystemExit):
        renderer.main()
    check("owner@example.org" not in capsys.readouterr().err, "the rejected value is not echoed")
    check(not (tmp_path / "infra").exists(), "nothing is rendered")


def test_the_services_stack_does_not_receive_the_alert_address(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage, _ = render_project_config.load_configuration(env_file(tmp_path, ALERT_EMAIL=ADDRESS))
    check("alert_email" not in renderer.platform_values(storage), "platform_values holds only the five project keys")
    outputs = {name: {"value": "x"} for name in renderer.SERVICES_PLATFORM_KEYS}
    outputs["log_group_names"] = {"value": {"jobs": "x"}}
    outputs["ecr_repository_urls"] = {"value": {"airflow": "x"}}
    monkeypatch.setattr(renderer, "terraform_document", lambda *arguments, **keywords: outputs)
    check("alert_email" not in renderer.services_values(storage), "the services inputs have no alert address")


def test_a_saved_plan_with_the_rendered_address_is_accepted_and_a_different_one_is_not() -> None:
    declared = {"alert_email": {"default": "", "sensitive": True}, "project_name": {}}
    plan = {"complete": True, "errored": False, "configuration": {"root_module": {"variables": declared}}}
    expected = {"project_name": "example_project", "alert_email": ADDRESS}
    renderer.verify_platform_plan({**plan, "variables": {"project_name": {"value": "example_project"}, "alert_email": {"value": ADDRESS}}}, expected)
    with pytest.raises(render_project_config.ConfigurationError):
        renderer.verify_platform_plan(
            {**plan, "variables": {"project_name": {"value": "example_project"}, "alert_email": {"value": "other@example.org"}}}, expected
        )
    renderer.verify_platform_plan(
        {**plan, "variables": {"project_name": {"value": "example_project"}, "alert_email": {"value": ""}}}, {"project_name": "example_project"}
    )
