"""Retained configuration safeguards using generic synthetic inputs only."""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import render_project_config as renderer
from scripts.infrastructure.render_project_config import ConfigurationError, build_configuration, verify_plan_variables

TEST_PROJECT_NAME = "example_project"
TEST_PROJECT_PROFILE = f"{TEST_PROJECT_NAME}_user"


@pytest.fixture
def environment() -> dict[str, Any]:
    return {
        "AWS_ACCOUNT_ID": "111111111111",
        "AWS_PROFILE": TEST_PROJECT_PROFILE,
        "AWS_REGION": "eu-west-1",
        "PROJECT_NAME": TEST_PROJECT_NAME,
        "S3_BUCKET": "example-project-ci-bucket",
    }


@pytest.fixture
def caller_identity(environment: dict[str, Any]) -> dict[str, Any]:
    return {"Account": environment["AWS_ACCOUNT_ID"], "Arn": f"arn:aws:iam::{environment['AWS_ACCOUNT_ID']}:user/{environment['AWS_PROFILE']}"}


def test_configuration_has_one_source_and_no_credentials(environment: dict[str, Any]) -> None:
    storage, iam = build_configuration(environment)
    assert storage["expected_account_id"] == iam["expected_account_id"] == environment["AWS_ACCOUNT_ID"]
    assert storage["data_bucket_name"] == iam["data_bucket_name"] == environment["S3_BUCKET"]
    assert storage["project_name"] == iam["project_name"] == environment["PROJECT_NAME"]
    assert storage == iam
    assert storage["aws_profile"] == environment["AWS_PROFILE"]
    assert set(storage) == {"expected_account_id", "data_bucket_name", "project_name", "aws_profile", "aws_region"}


def test_admin_selection_does_not_change_project_profile(environment: dict[str, Any]) -> None:
    storage, iam = build_configuration(environment, "example_admin")
    assert storage["aws_profile"] == TEST_PROJECT_PROFILE
    assert iam["admin_profile"] == "example_admin"
    assert iam["aws_profile"] == TEST_PROJECT_PROFILE
    assert environment["AWS_PROFILE"] == TEST_PROJECT_PROFILE


@pytest.mark.parametrize("name", ["AWS_ACCOUNT_ID", "AWS_REGION", "PROJECT_NAME", "S3_BUCKET", "AWS_PROFILE"])
def test_missing_values_are_rejected(environment: dict[str, Any], name: str) -> None:
    del environment[name]
    with pytest.raises(ConfigurationError):
        build_configuration(environment)


@pytest.mark.parametrize("name,value", [("AWS_ACCOUNT_ID", "bad"), ("PROJECT_NAME", "wrong-name"), ("S3_BUCKET", "*"), ("AWS_PROFILE", "example_admin")])
def test_invalid_values_are_rejected_without_echoing_values(environment: dict[str, Any], name: str, value: str) -> None:
    environment[name] = value
    with pytest.raises(ConfigurationError) as error:
        build_configuration(environment)
    assert value not in str(error.value)


@pytest.mark.parametrize("value", [TEST_PROJECT_PROFILE, "example_different_user", ""])
def test_unsupported_configuration_is_rejected(environment: dict[str, Any], value: str) -> None:
    environment["EXAMPLE_UNUSED_SETTING"] = value
    with pytest.raises(ConfigurationError, match="Unsupported .env settings"):
        build_configuration(environment)


@pytest.mark.parametrize("profile", ["", " ", TEST_PROJECT_PROFILE])
def test_project_profile_cannot_administer_itself(environment: dict[str, Any], profile: str) -> None:
    with pytest.raises(ConfigurationError):
        build_configuration(environment, profile)


@pytest.mark.parametrize("user_path", ["", "example_team/"])
def test_identity_check_uses_explicit_profile_and_ignores_ambient_credentials(
    environment: dict[str, Any], caller_identity: dict[str, Any], monkeypatch: pytest.MonkeyPatch, user_path: str
) -> None:
    storage, _ = build_configuration(environment)
    caller_identity["Arn"] = f"arn:aws:iam::{environment['AWS_ACCOUNT_ID']}:user/{user_path}{environment['AWS_PROFILE']}"
    monkeypatch.setenv("AWS_PROFILE", "example_admin")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "example-not-a-real-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "example-not-a-real-secret")
    calls = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, json.dumps(caller_identity), "")

    monkeypatch.setattr(renderer.subprocess, "run", run)
    renderer.verify_project_identity(storage)
    command, options = calls[0]
    assert len(calls) == 1 and command[:3] == ["aws", "sts", "get-caller-identity"]
    assert command[command.index("--profile") + 1] == environment["AWS_PROFILE"]
    assert command[command.index("--region") + 1] == environment["AWS_REGION"]
    assert options["capture_output"] and options["timeout"] == 30
    assert not {"AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}.intersection(options["env"])


@pytest.mark.parametrize("change", ["account", "arn_account", "user", "role", "assumed_role", "root", "missing_arn"])
def test_wrong_aws_identity_is_rejected(environment: dict[str, Any], caller_identity: dict[str, Any], monkeypatch: pytest.MonkeyPatch, change: str) -> None:
    storage, _ = build_configuration(environment)
    account = environment["AWS_ACCOUNT_ID"]
    profile = environment["AWS_PROFILE"]
    replacements: dict[str, dict[str, str | None]] = {
        "account": {"Account": "222222222222"},
        "arn_account": {"Arn": f"arn:aws:iam::222222222222:user/{profile}"},
        "user": {"Arn": f"arn:aws:iam::{account}:user/example_different_user"},
        "role": {"Arn": f"arn:aws:iam::{account}:role/{profile}"},
        "assumed_role": {"Arn": f"arn:aws:sts::{account}:assumed-role/{profile}/example_session"},
        "root": {"Arn": f"arn:aws:iam::{account}:root"},
        "missing_arn": {"Arn": None},
    }
    caller_identity.update(replacements[change])
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, json.dumps(caller_identity), ""))
    with pytest.raises(ConfigurationError, match="same-named IAM user") as error:
        renderer.verify_project_identity(storage)
    assert account not in str(error.value) and profile not in str(error.value)


@pytest.mark.parametrize("response", ["not-json", "[]", "null"])
def test_malformed_identity_responses_are_rejected(environment: dict[str, Any], monkeypatch: pytest.MonkeyPatch, response: str) -> None:
    storage, _ = build_configuration(environment)
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, response, ""))
    with pytest.raises(ConfigurationError, match="invalid response"):
        renderer.verify_project_identity(storage)


def test_failed_identity_request_does_not_echo_diagnostics(environment: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    storage, _ = build_configuration(environment)
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 1, "", "example private diagnostic"))
    with pytest.raises(ConfigurationError, match="No plan or apply") as error:
        renderer.verify_project_identity(storage)
    assert "example private diagnostic" not in str(error.value)


@pytest.mark.parametrize("failure", [FileNotFoundError("example private path"), subprocess.TimeoutExpired("example private command", 30)])
def test_identity_check_handles_missing_cli_and_timeouts(environment: dict[str, Any], monkeypatch: pytest.MonkeyPatch, failure: Exception) -> None:
    storage, _ = build_configuration(environment)

    def run(*args: Any, **kwargs: Any) -> None:
        raise failure

    monkeypatch.setattr(renderer.subprocess, "run", run)
    with pytest.raises(ConfigurationError, match="Could not verify") as error:
        renderer.verify_project_identity(storage)
    assert "example private" not in str(error.value)


def test_saved_plan_must_match_current_dotenv(environment: dict[str, Any]) -> None:
    storage, _ = build_configuration(environment)
    plan = {"complete": True, "errored": False, "variables": {key: {"value": value} for key, value in storage.items()}}
    verify_plan_variables(plan, storage)
    with pytest.raises(ConfigurationError):
        verify_plan_variables(plan, {**storage, "data_bucket_name": "different-bucket"})


@pytest.mark.parametrize("complete,errored", [(False, False), (True, True)])
def test_incomplete_or_errored_plan_is_rejected(environment: dict[str, Any], complete: bool, errored: bool) -> None:
    storage, _ = build_configuration(environment)
    with pytest.raises(ConfigurationError):
        verify_plan_variables({"complete": complete, "errored": errored}, storage)


@pytest.fixture
def wrapper_environment(tmp_path: Path) -> dict[str, str]:
    return {"PATH": "/usr/bin:/bin", "HOME": str(tmp_path), "AWS_EC2_METADATA_DISABLED": "true"}


@pytest.mark.parametrize("arguments", [["plan"], ["apply", "example.tfplan"], ["validate"]])
@pytest.mark.parametrize("name", ["TF_CLI_ARGS", "TF_CLI_ARGS_plan", "TF_CLI_ARGS_apply", "TF_CLI_ARGS_show", "TF_CLI_ARGS_validate", "TF_CLI_ARGS_future"])
def test_wrapper_rejects_inherited_arguments_before_configuration_checks(
    arguments: list[str], name: str, tmp_path: Path, wrapper_environment: dict[str, str]
) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "infrastructure" / "terraform.sh"
    value = "-var-file=example_private.tfvars" if name == "TF_CLI_ARGS" else "-var=project_name=unexpected_project"
    wrapper_environment.update({"PYTHON_BIN": str(tmp_path / "must_not_run"), name: value})
    result = subprocess.run(["/bin/bash", str(script), *arguments], capture_output=True, text=True, env=wrapper_environment)  # noqa: S603 - fixed wrapper, synthetic arguments.
    assert result.returncode == 2
    assert f"Unset {name};" in result.stderr
    assert value not in result.stderr and "must_not_run" not in result.stderr
    assert not result.stdout


@pytest.mark.parametrize("empty_exports", [False, True])
@pytest.mark.parametrize(
    "arguments",
    [
        ["plan", "-out=example.tfplan", "-no-color"],
        ["validate", "-no-color"],
        ["apply", "example.tfplan"],
        ["--iam", "--admin-profile", "example_admin", "plan", "-out=example.tfplan"],
        ["--iam", "--admin-profile", "example_admin", "apply", "example.tfplan"],
    ],
)
def test_wrapper_preserves_explicit_options_without_inherited_arguments(
    arguments: list[str], empty_exports: bool, tmp_path: Path, wrapper_environment: dict[str, str]
) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "infrastructure" / "terraform.sh"
    for name in ("example_python", "terraform"):
        stub = tmp_path / name
        stub.write_text(f'#!/usr/bin/env bash\nprintf "{name}\\n"\nprintf "%s\\n" "$@"\n', encoding="utf-8")
        stub.chmod(0o700)
    wrapper_environment.update({"PYTHON_BIN": str(tmp_path / "example_python"), "PATH": f"{tmp_path}{os.pathsep}{wrapper_environment['PATH']}"})
    if empty_exports:
        wrapper_environment.update({"TF_CLI_ARGS": "", "TF_CLI_ARGS_plan": "", "TF_CLI_ARGS_show": ""})
    result = subprocess.run(["/bin/bash", str(script), *arguments], capture_output=True, text=True, env=wrapper_environment)  # noqa: S603 - fixed wrapper, synthetic arguments.
    assert result.returncode == 0, result.stderr
    output = result.stdout.splitlines()
    terraform_arguments = output[output.index("terraform") + 1 :]
    iam = arguments[0] == "--iam"
    action, *options = arguments[3:] if iam else arguments
    expected = [action, *([] if action == "validate" else ["-input=false"])]
    if iam and action == "plan":
        expected.append("-var=admin_profile=example_admin")
    expected.extend(options)
    assert terraform_arguments[1:] == expected
    assert terraform_arguments[0].startswith("-chdir=")
    if action == "apply":
        assert "--check-plan" in output[: output.index("terraform")]


@pytest.mark.parametrize("arguments", [["plan"], ["apply", "example.tfplan"], ["--iam", "--admin-profile", "example_admin", "plan"], ["validate"]])
def test_wrapper_requests_identity_verification_for_plans_and_applies(arguments: list[str], tmp_path: Path, wrapper_environment: dict[str, str]) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "infrastructure" / "terraform.sh"
    stub = tmp_path / "example_python"
    stub.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@"\nexit 1\n', encoding="utf-8")
    stub.chmod(0o700)
    wrapper_environment["PYTHON_BIN"] = str(stub)
    result = subprocess.run(["/bin/bash", str(script), *arguments], capture_output=True, text=True, env=wrapper_environment)  # noqa: S603 - fixed wrapper, synthetic arguments.
    assert result.returncode == 1
    assert ("--verify-identity" in result.stdout.splitlines()) == (arguments != ["validate"])


@pytest.mark.parametrize("matches", [True, False])
def test_cli_checks_identity_before_writing_inputs(
    environment: dict[str, Any], caller_identity: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, matches: bool
) -> None:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--verify-identity"])
    if not matches:
        caller_identity["Account"] = "222222222222"
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, json.dumps(caller_identity), ""))
    if matches:
        renderer.main()
        assert (tmp_path / "infra" / "deployment.auto.tfvars.json").is_file()
    else:
        with pytest.raises(SystemExit):
            renderer.main()
        assert not (tmp_path / "infra").exists()


@pytest.mark.parametrize("stack", ["storage", "iam"])
@pytest.mark.parametrize("change", [None, "aws_profile", "admin_profile", "example_retired_input"])
def test_cli_checks_selected_saved_plan(environment: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stack: str, change: str | None) -> None:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    storage, iam = build_configuration(environment, "example_admin")
    values = storage if stack == "storage" else iam
    plan: dict[str, Any] = {"complete": True, "variables": {key: {"value": value} for key, value in values.items()}}
    if change:
        plan["variables"][change] = {"value": "example_unapproved"}
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", stack, "--admin-profile", "example_admin", "--check-plan", str(tmp_path / "reviewed.tfplan")])
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, json.dumps(plan), ""))
    if change:
        with pytest.raises(SystemExit):
            renderer.main()
        assert not (tmp_path / "infra").exists()
    else:
        renderer.main()


@pytest.mark.parametrize("arguments", [["--check-plan", "example.tfplan"], ["--env-file", "missing.env"]])
def test_cli_rejects_invalid_requests(environment: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arguments: list[str]) -> None:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", *arguments])
    with pytest.raises(SystemExit) as error:
        renderer.main()
    assert error.value.code == 2


def test_cli_rejects_unreadable_plan_without_echoing_output(
    environment: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "storage", "--check-plan", str(tmp_path / "broken.tfplan")])
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 1, "", "example private diagnostic"))
    with pytest.raises(SystemExit):
        renderer.main()
    assert "example private diagnostic" not in capsys.readouterr().err
