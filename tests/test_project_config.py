"""Retained configuration safeguards using generic synthetic inputs only."""

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import render_project_config as renderer
from scripts.infrastructure.render_project_config import ConfigurationError, alert_email_input, build_configuration, verify_plan_variables
from scripts.process import CompletedProcess, TimeoutExpired, run_command
from tests.support import check

# The committed defaults in the shape the renderer reads; config/aws/iam_preparation.json is the reviewed file: flags and non-environment values only.
DEFAULT_PREPARATION = Path(__file__).parent / "fixtures" / "iam_preparation_default.json"
# The live IAM apply inputs, keys and shapes only: the account, region, bucket and project are placeholders. They hold the bronze and bronze_stage launch
# families and the six prepared flags of the live roles, and the publisher secret ARNs in their wildcard form.
LIVE_INPUTS = Path(__file__).parent / "fixtures" / "iam_live_inputs_sanitized.json"

TEST_PROJECT_NAME = "example_project"
TEST_ENVIRONMENT = "dev"
# The project profile and its IAM user are named <project>_<environment>.
TEST_PROJECT_PROFILE = f"{TEST_PROJECT_NAME}_{TEST_ENVIRONMENT}"


@pytest.fixture
def environment() -> dict[str, Any]:
    return {
        "AWS_ACCOUNT_ID": "111111111111",
        "AWS_PROFILE": TEST_PROJECT_PROFILE,
        "AWS_REGION": "eu-west-1",
        "ENVIRONMENT": TEST_ENVIRONMENT,
        "PROJECT_NAME": TEST_PROJECT_NAME,
        "S3_BUCKET": "example-project-ci-bucket",
    }


@pytest.fixture
def caller_identity(environment: dict[str, Any]) -> dict[str, Any]:
    return {"Account": environment["AWS_ACCOUNT_ID"], "Arn": f"arn:aws:iam::{environment['AWS_ACCOUNT_ID']}:user/{environment['AWS_PROFILE']}"}


def test_configuration_has_one_source_and_no_credentials(environment: dict[str, Any]) -> None:
    storage, iam = build_configuration(environment)
    check(
        storage["expected_account_id"] == iam["expected_account_id"] == environment["AWS_ACCOUNT_ID"],
        'storage["expected_account_id"] == iam["expected_account_id"] == environment["AWS_ACCOUNT_ID"]',
    )
    check(
        storage["data_bucket_name"] == iam["data_bucket_name"] == environment["S3_BUCKET"],
        'storage["data_bucket_name"] == iam["data_bucket_name"] == environment["S3_BUCKET"]',
    )
    check(
        storage["project_name"] == iam["project_name"] == environment["PROJECT_NAME"],
        'storage["project_name"] == iam["project_name"] == environment["PROJECT_NAME"]',
    )
    check(storage == iam, "storage == iam")
    check(storage["aws_profile"] == environment["AWS_PROFILE"], 'storage["aws_profile"] == environment["AWS_PROFILE"]')
    check(storage["environment"] == iam["environment"] == TEST_ENVIRONMENT, 'storage["environment"] == iam["environment"] == TEST_ENVIRONMENT')
    check(
        set(storage) == {"expected_account_id", "data_bucket_name", "project_name", "aws_profile", "aws_region", "environment"},
        'set(storage) == {"expected_account_id", "data_bucket_name", "project_name", "aws_profile", "aws_region", "environment"}',
    )


def test_admin_selection_does_not_change_project_profile(environment: dict[str, Any]) -> None:
    storage, iam = build_configuration(environment, "example_admin")
    check(storage["aws_profile"] == TEST_PROJECT_PROFILE, 'storage["aws_profile"] == TEST_PROJECT_PROFILE')
    check(iam["admin_profile"] == "example_admin", 'iam["admin_profile"] == "example_admin"')
    check(iam["aws_profile"] == TEST_PROJECT_PROFILE, 'iam["aws_profile"] == TEST_PROJECT_PROFILE')
    check(environment["AWS_PROFILE"] == TEST_PROJECT_PROFILE, 'environment["AWS_PROFILE"] == TEST_PROJECT_PROFILE')


@pytest.mark.parametrize("name", ["AWS_ACCOUNT_ID", "AWS_REGION", "PROJECT_NAME", "S3_BUCKET", "AWS_PROFILE", "ENVIRONMENT"])
def test_missing_values_are_rejected(environment: dict[str, Any], name: str) -> None:
    del environment[name]
    with pytest.raises(ConfigurationError):
        build_configuration(environment)


@pytest.mark.parametrize(
    "name,value",
    [
        ("AWS_ACCOUNT_ID", "bad"),
        ("PROJECT_NAME", "wrong-name"),
        ("S3_BUCKET", "*"),
        ("AWS_PROFILE", "example_admin"),
        ("AWS_PROFILE", "example_project_user"),
        ("ENVIRONMENT", "development"),
    ],
)
def test_invalid_values_are_rejected_without_echoing_values(environment: dict[str, Any], name: str, value: str) -> None:
    environment[name] = value
    with pytest.raises(ConfigurationError) as error:
        build_configuration(environment)
    check(value not in str(error.value), "value not in str(error.value)")


@pytest.mark.parametrize("value", [TEST_PROJECT_PROFILE, "example_different_user", ""])
def test_unsupported_configuration_is_rejected(environment: dict[str, Any], value: str) -> None:
    environment["EXAMPLE_UNUSED_SETTING"] = value
    with pytest.raises(ConfigurationError, match="Unsupported .env settings"):
        build_configuration(environment)


@pytest.mark.parametrize("value", ["owner@example.org", "first.last+alerts@mail.example.org", ""])
def test_alert_email_is_an_optional_key_that_stays_out_of_the_storage_and_iam_inputs(environment: dict[str, Any], value: str) -> None:
    plain_storage, plain_iam = build_configuration(environment)
    environment["ALERT_EMAIL"] = value
    storage, iam = build_configuration(environment)
    check(storage == plain_storage and iam == plain_iam, "the alert address reaches neither the storage nor the IAM inputs")
    check(alert_email_input(environment) == ({"alert_email": value} if value else {}), "only a set address becomes the alerts root input")


@pytest.mark.parametrize("value", [None, ""])
def test_unset_or_empty_alert_email_renders_no_input(environment: dict[str, Any], value: str | None) -> None:
    environment["ALERT_EMAIL"] = value
    build_configuration(environment)
    check(alert_email_input(environment) == {}, "an unset or empty alert address renders no input")
    check(alert_email_input({}) == {}, "a missing key renders no input")


@pytest.mark.parametrize(
    "value",
    [
        "owner@example.org,other@example.org",
        "owner@example.org other@example.org",
        "owner@example.org;other@example.org",
        "Owner <owner@example.org>",
        "mailto:owner@example.org",
        " owner@example.org",
        "owner@example.org ",
        "owner@example",
        "owner@",
        "@example.org",
        "owner",
        "owner@@example.org",
        "owner@example..org",
        "own er@example.org",
        "owner@example.org\n",
        "a" * 250 + "@example.org",
        "your-alert-email",
    ],
)
def test_alert_email_must_be_one_plain_address_and_is_never_echoed(environment: dict[str, Any], value: str) -> None:
    environment["ALERT_EMAIL"] = value
    with pytest.raises(ConfigurationError) as error:
        build_configuration(environment)
    check(value.strip() not in str(error.value), "the rejected value is not echoed")
    check("ALERT_EMAIL" in str(error.value), "the error names the key")
    with pytest.raises(ConfigurationError):
        alert_email_input(environment)


def test_profile_follows_project_and_environment(environment: dict[str, Any]) -> None:
    environment["ENVIRONMENT"] = "prod"
    with pytest.raises(ConfigurationError):
        build_configuration(environment)
    environment["AWS_PROFILE"] = f"{TEST_PROJECT_NAME}_prod"
    storage, _ = build_configuration(environment)
    check(
        storage["aws_profile"] == f"{TEST_PROJECT_NAME}_prod" and storage["environment"] == "prod",
        'storage["aws_profile"] == f"{TEST_PROJECT_NAME}_prod" and storage["environment"] == "prod"',
    )


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

    def run(program: str, args: list[str], **kwargs: Any) -> CompletedProcess[str]:
        calls.append(([program, *args], kwargs))
        return CompletedProcess([program, *args], 0, json.dumps(caller_identity), "")

    monkeypatch.setattr(renderer, "run_command", run)
    renderer.verify_project_identity(storage)
    command, options = calls[0]
    check(len(calls) == 1 and command[:3] == ["aws", "sts", "get-caller-identity"], 'len(calls) == 1 and command[:3] == ["aws", "sts", "get-caller-identity"]')
    check(command[command.index("--profile") + 1] == environment["AWS_PROFILE"], 'command[command.index("--profile") + 1] == environment["AWS_PROFILE"]')
    check(command[command.index("--region") + 1] == environment["AWS_REGION"], 'command[command.index("--region") + 1] == environment["AWS_REGION"]')
    check(options["timeout"] == 30, 'options["timeout"] == 30')
    check(
        not {"AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}.intersection(options["env"]),
        'not {"AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}.intersection(options["env"])',
    )


@pytest.mark.parametrize("change", ["account", "arn_account", "user", "role", "assumed_role", "root", "missing_arn"])
def test_wrong_aws_identity_is_rejected(environment: dict[str, Any], caller_identity: dict[str, Any], monkeypatch: pytest.MonkeyPatch, change: str) -> None:
    storage, _ = build_configuration(environment)
    account = environment["AWS_ACCOUNT_ID"]
    profile = environment["AWS_PROFILE"]
    other_account = "2" * 12
    replacements: dict[str, dict[str, str | None]] = {
        "account": {"Account": other_account},
        "arn_account": {"Arn": f"arn:aws:iam::{other_account}:user/{profile}"},
        "user": {"Arn": f"arn:aws:iam::{account}:user/example_different_user"},
        "role": {"Arn": f"arn:aws:iam::{account}:role/{profile}"},
        "assumed_role": {"Arn": f"arn:aws:sts::{account}:assumed-role/{profile}/example_session"},
        "root": {"Arn": f"arn:aws:iam::{account}:root"},
        "missing_arn": {"Arn": None},
    }
    caller_identity.update(replacements[change])
    monkeypatch.setattr(renderer, "run_command", lambda program, args, **kwargs: CompletedProcess([program, *args], 0, json.dumps(caller_identity), ""))
    with pytest.raises(ConfigurationError, match="same-named IAM user") as error:
        renderer.verify_project_identity(storage)
    check(account not in str(error.value) and profile not in str(error.value), "account not in str(error.value) and profile not in str(error.value)")


@pytest.mark.parametrize("response", ["not-json", "[]", "null"])
def test_malformed_identity_responses_are_rejected(environment: dict[str, Any], monkeypatch: pytest.MonkeyPatch, response: str) -> None:
    storage, _ = build_configuration(environment)
    monkeypatch.setattr(renderer, "run_command", lambda program, args, **kwargs: CompletedProcess([program, *args], 0, response, ""))
    with pytest.raises(ConfigurationError, match="invalid response"):
        renderer.verify_project_identity(storage)


def test_failed_identity_request_does_not_echo_diagnostics(environment: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    storage, _ = build_configuration(environment)
    monkeypatch.setattr(renderer, "run_command", lambda program, args, **kwargs: CompletedProcess([program, *args], 1, "", "example private diagnostic"))
    with pytest.raises(ConfigurationError, match="No plan or apply") as error:
        renderer.verify_project_identity(storage)
    check("example private diagnostic" not in str(error.value), '"example private diagnostic" not in str(error.value)')


@pytest.mark.parametrize("failure", [FileNotFoundError("example private path"), TimeoutExpired("example private command", 30)])
def test_identity_check_handles_missing_cli_and_timeouts(environment: dict[str, Any], monkeypatch: pytest.MonkeyPatch, failure: Exception) -> None:
    storage, _ = build_configuration(environment)

    def run(*args: Any, **kwargs: Any) -> None:
        raise failure

    monkeypatch.setattr(renderer, "run_command", run)
    with pytest.raises(ConfigurationError, match="Could not verify") as error:
        renderer.verify_project_identity(storage)
    check("example private" not in str(error.value), '"example private" not in str(error.value)')


def test_saved_plan_must_match_current_dotenv(environment: dict[str, Any]) -> None:
    storage, _ = build_configuration(environment)
    plan = {"complete": True, "errored": False, "variables": {key: {"value": value} for key, value in storage.items()}}
    verify_plan_variables(plan, storage)
    with pytest.raises(ConfigurationError):
        verify_plan_variables(plan, {**storage, "data_bucket_name": "different-bucket"})


def test_saved_plan_accepts_other_inputs_only_at_their_committed_default(environment: dict[str, Any]) -> None:
    """The IAM stack declares inputs with defaults (the owner addresses, the budget): a saved plan must keep each default."""
    storage, _ = build_configuration(environment)
    variables = {key: {"value": value} for key, value in storage.items()}
    declared = {"owner_ip_cidrs": {"default": []}, "budget_limit_usd": {"default": 150}, "no_default": {}}
    base = {"complete": True, "errored": False, "configuration": {"root_module": {"variables": declared}}}
    verify_plan_variables({**base, "variables": {**variables, "owner_ip_cidrs": {"value": []}, "budget_limit_usd": {"value": 150}}}, storage)
    for name, value in (("owner_ip_cidrs", ["192.0.2.10/32"]), ("budget_limit_usd", 1000), ("no_default", "x"), ("undeclared", 1)):
        with pytest.raises(ConfigurationError) as error:
            verify_plan_variables({**base, "variables": {**variables, name: {"value": value}}}, storage)
        check(name in str(error.value), f"the refusal must name {name}")


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
    result = run_command("/bin/bash", [str(script), *arguments], env=wrapper_environment, timeout=60)
    check(result.returncode == 2, "result.returncode == 2")
    check(f"Unset {name};" in result.stderr, 'f"Unset {name};" in result.stderr')
    check(value not in result.stderr and "must_not_run" not in result.stderr, 'value not in result.stderr and "must_not_run" not in result.stderr')
    check(not result.stdout, "not result.stdout")


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
    result = run_command("/bin/bash", [str(script), *arguments], env=wrapper_environment, timeout=60)
    check(result.returncode == 0, result.stderr)
    output = result.stdout.splitlines()
    terraform_arguments = output[output.index("terraform") + 1 :]
    iam = arguments[0] == "--iam"
    action, *options = arguments[3:] if iam else arguments
    expected = [action, *([] if action == "validate" else ["-input=false"])]
    if iam and action == "plan":
        expected.append("-var=admin_profile=example_admin")
    expected.extend(options)
    check(terraform_arguments[1:] == expected, "terraform_arguments[1:] == expected")
    check(terraform_arguments[0].startswith("-chdir="), 'terraform_arguments[0].startswith("-chdir=")')
    if action == "apply":
        check("--check-plan" in output[: output.index("terraform")], '"--check-plan" in output[: output.index("terraform")]')


@pytest.mark.parametrize("arguments", [["plan"], ["apply", "example.tfplan"], ["--iam", "--admin-profile", "example_admin", "plan"], ["validate"]])
def test_wrapper_requests_identity_verification_for_plans_and_applies(arguments: list[str], tmp_path: Path, wrapper_environment: dict[str, str]) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "infrastructure" / "terraform.sh"
    stub = tmp_path / "example_python"
    stub.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@"\nexit 1\n', encoding="utf-8")
    stub.chmod(0o700)
    wrapper_environment["PYTHON_BIN"] = str(stub)
    result = run_command("/bin/bash", [str(script), *arguments], env=wrapper_environment, timeout=60)
    check(result.returncode == 1, "result.returncode == 1")
    check(
        ("--verify-identity" in result.stdout.splitlines()) == (arguments != ["validate"]),
        '("--verify-identity" in result.stdout.splitlines()) == (arguments != ["validate"])',
    )


@pytest.mark.parametrize("matches", [True, False])
def test_cli_checks_identity_before_writing_inputs(
    environment: dict[str, Any], caller_identity: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, matches: bool
) -> None:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    committed = renderer.load_preparation
    monkeypatch.setattr(renderer, "load_preparation", lambda bucket: committed(bucket, DEFAULT_PREPARATION))
    monkeypatch.setattr(sys, "argv", ["renderer", "--verify-identity"])
    if not matches:
        caller_identity["Account"] = "2" * 12
    monkeypatch.setattr(renderer, "run_command", lambda program, args, **kwargs: CompletedProcess([program, *args], 0, json.dumps(caller_identity), ""))
    if matches:
        renderer.main()
        check((tmp_path / "infra" / "deployment.auto.tfvars.json").is_file(), '(tmp_path / "infra" / "deployment.auto.tfvars.json").is_file()')
    else:
        with pytest.raises(SystemExit):
            renderer.main()
        check(not (tmp_path / "infra").exists(), 'not (tmp_path / "infra").exists()')


@pytest.mark.parametrize("stack", ["storage", "iam"])
@pytest.mark.parametrize("change", [None, "aws_profile", "admin_profile", "example_retired_input"])
def test_cli_checks_selected_saved_plan(environment: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stack: str, change: str | None) -> None:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    storage, iam = build_configuration(environment, "example_admin")
    values = storage if stack == "storage" else iam
    # The IAM stack also carries the four committed preparation inputs.
    committed = renderer.load_preparation
    preparation = committed(storage["data_bucket_name"], DEFAULT_PREPARATION) if stack == "iam" else {}
    monkeypatch.setattr(renderer, "load_preparation", lambda bucket: committed(bucket, DEFAULT_PREPARATION))
    plan: dict[str, Any] = {"complete": True, "variables": {key: {"value": value} for key, value in {**values, **preparation}.items()}}
    if change:
        plan["variables"][change] = {"value": "example_unapproved"}
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", stack, "--admin-profile", "example_admin", "--check-plan", str(tmp_path / "reviewed.tfplan")])
    monkeypatch.setattr(renderer, "run_command", lambda program, args, **kwargs: CompletedProcess([program, *args], 0, json.dumps(plan), ""))
    if change:
        with pytest.raises(SystemExit):
            renderer.main()
        check(not (tmp_path / "infra").exists(), 'not (tmp_path / "infra").exists()')
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
    check(error.value.code == 2, "error.value.code == 2")


def test_cli_rejects_unreadable_plan_without_echoing_output(
    environment: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "storage", "--check-plan", str(tmp_path / "broken.tfplan")])
    monkeypatch.setattr(renderer, "run_command", lambda program, args, **kwargs: CompletedProcess([program, *args], 1, "", "example private diagnostic"))
    with pytest.raises(SystemExit):
        renderer.main()
    check("example private diagnostic" not in capsys.readouterr().err, '"example private diagnostic" not in capsys.readouterr().err')


def preparation_file(tmp_path: Path, **extra: Any) -> Path:
    document = json.loads(DEFAULT_PREPARATION.read_text(encoding="utf-8")) | extra
    path = tmp_path / "iam_preparation.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_optional_preparation_flags_are_absent_by_default(tmp_path: Path) -> None:
    document = renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path))
    check(
        not {"job_endpoints_enabled", "training_prepared"} & document.keys(),
        "a flag that is not set stays out of the rendered inputs, so the stack default applies",
    )


def test_optional_preparation_flags_are_read_when_set(tmp_path: Path) -> None:
    document = renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, job_endpoints_enabled=True, training_prepared=True))
    check(document["job_endpoints_enabled"] is True and document["training_prepared"] is True, "both flags are rendered as set")


@pytest.mark.parametrize("name", ["job_endpoints_enabled", "training_prepared"])
def test_optional_preparation_flag_must_be_a_boolean_and_is_not_echoed(tmp_path: Path, name: str) -> None:
    with pytest.raises(ConfigurationError) as refusal:
        renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, **{name: "synthetic-value"}))
    check(name in str(refusal.value) and "synthetic-value" not in str(refusal.value), "the message names the input and never echoes the value")


ALERTS_TOPIC = "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev"


def test_alerts_topic_is_absent_by_default_and_read_when_set(tmp_path: Path) -> None:
    default = renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path))
    check("alerts_topic_arn" not in default, "an unset topic stays out of the rendered inputs, so the stack default (no publish grant) applies")
    document = renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, training_prepared=True, alerts_topic_arn=ALERTS_TOPIC))
    check(document["alerts_topic_arn"] == ALERTS_TOPIC, "the platform alerts topic is rendered as typed")


@pytest.mark.parametrize(
    "value",
    [
        "",
        5,
        None,
        "not an arn",
        "arn:aws:sns:eu-west-1:111111111111:*",
        "arn:aws:sns:eu-west-1:111111111111:example_project_alerts_dev:01234567-89ab-cdef-0123-456789abcdef",
        "arn:aws:sqs:eu-west-1:111111111111:example_project_alerts_dev",
        "arn:aws:sns:eu-west-1:11111111111:example_project_alerts_dev",
    ],
)
def test_alerts_topic_must_be_one_topic_arn_and_is_not_echoed(tmp_path: Path, value: Any) -> None:
    with pytest.raises(ConfigurationError) as refusal:
        renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, alerts_topic_arn=value))
    message = str(refusal.value)
    echoed = isinstance(value, str) and bool(value) and value in message
    check("Set alerts_topic_arn to" in message and not echoed, "the message names the input and never echoes the value")


def test_unknown_preparation_input_is_still_refused(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError):
        renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, example_extra_input=True))


def render_committed_inputs(environment: dict[str, Any]) -> dict[str, object]:
    """The IAM preparation inputs a fresh render writes from the committed file and the .env values, as the CLI writes them."""
    _, iam = build_configuration(environment)
    return renderer.derive_environment_inputs(renderer.load_preparation(environment["S3_BUCKET"]), iam)


def sanitize(document: dict[str, object], environment: dict[str, Any]) -> dict[str, object]:
    """Replace the .env values with the placeholders of the live fixture."""
    text = json.dumps(document, sort_keys=True)
    for placeholder, name in (("<bucket>", "S3_BUCKET"), ("<account>", "AWS_ACCOUNT_ID"), ("<region>", "AWS_REGION")):
        text = text.replace(environment[name], placeholder)
    text = text.replace(f"{environment['PROJECT_NAME']}_alerts_{environment['ENVIRONMENT']}", "<project>_alerts_<environment>")
    text = text.replace(f'"{environment["PROJECT_NAME"]}_{environment["ENVIRONMENT"]}"', '"<project>_<environment>"')
    return json.loads(text)


def test_committed_iam_inputs_render_every_live_key(environment: dict[str, Any]) -> None:
    live = json.loads(LIVE_INPUTS.read_text(encoding="utf-8"))
    rendered = sanitize(render_committed_inputs(environment), environment)
    check(sorted(rendered) == sorted(live), "a fresh render from the committed file writes exactly the live key set")
    check(rendered == live, "a fresh render from the committed file writes the live values, with the .env values as placeholders")


def test_live_inputs_fixture_holds_no_account_number_or_bucket() -> None:
    text = LIVE_INPUTS.read_text(encoding="utf-8")
    check(not re.search(r"[0-9]{12}", text) and "imungai" not in text, "the live fixture holds no account number and no bucket name")


def derived_inputs(tmp_path: Path, environment: dict[str, Any], **extra: Any) -> dict[str, object]:
    _, iam = build_configuration(environment)
    return renderer.derive_environment_inputs(renderer.load_preparation(environment["S3_BUCKET"], preparation_file(tmp_path, **extra)), iam)


def test_derived_inputs_are_built_from_env_and_their_flags_are_not_stack_inputs(tmp_path: Path, environment: dict[str, Any]) -> None:
    inputs = derived_inputs(
        tmp_path, environment, alerts_topic_enabled=True, athena_owner_is_project_user=True, publisher_secret_arns_enabled=True, training_prepared=True
    )
    account, region = environment["AWS_ACCOUNT_ID"], environment["AWS_REGION"]
    check(
        inputs["alerts_topic_arn"] == f"arn:aws:sns:{region}:{account}:{TEST_PROJECT_NAME}_alerts_{TEST_ENVIRONMENT}", "the topic is the platform alerts topic"
    )
    check(inputs["athena_owner_user"] == f"{TEST_PROJECT_NAME}_{TEST_ENVIRONMENT}", "the owner is the project policy user")
    check(
        inputs["publisher_secret_arns"]
        == {name: f"arn:aws:secretsmanager:{region}:{account}:secret:{name}-??????" for name in ("bls_api_key", "census_api_key", "hud_api_key")},
        "each secret ARN holds the .env account and region and the six-character wildcard, never a suffix",
    )
    check(inputs["training_prepared"] is True, "an input that holds no .env value passes through")
    check(not {"alerts_topic_enabled", "athena_owner_is_project_user", "publisher_secret_arns_enabled"} & inputs.keys(), "no derived flag reaches the stack")


def test_derived_inputs_stay_out_when_their_flags_are_off(tmp_path: Path, environment: dict[str, Any]) -> None:
    inputs = derived_inputs(tmp_path, environment, alerts_topic_enabled=False, athena_owner_is_project_user=False)
    check(not {"alerts_topic_arn", "athena_owner_user", "publisher_secret_arns"} & inputs.keys(), "off flags build no input")


@pytest.mark.parametrize("name", ["alerts_topic_enabled", "athena_owner_is_project_user", "publisher_secret_arns_enabled"])
def test_derived_flag_must_be_a_boolean_and_is_not_echoed(tmp_path: Path, name: str) -> None:
    with pytest.raises(ConfigurationError) as refusal:
        renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, **{name: "synthetic-value"}))
    check(f"Set {name} to true or false" in str(refusal.value) and "synthetic-value" not in str(refusal.value), "the message names the input, not the value")


PUBLISHER_NAMES = ("bls_api_key", "census_api_key", "hud_api_key")


def secret_arns(suffix: str = "??????", account: str = "1" * 12, **replace: str) -> dict[str, str]:
    arns = {name: f"arn:aws:secretsmanager:eu-west-1:{account}:secret:{name}-{suffix}" for name in PUBLISHER_NAMES}
    return {**arns, **replace}


def test_a_literal_publisher_secret_arn_may_only_be_the_six_character_wildcard_form(tmp_path: Path) -> None:
    document = renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, publisher_secret_arns=secret_arns()))
    check(document["publisher_secret_arns"] == secret_arns(), "the exact wildcard form of the three names is kept as written")


@pytest.mark.parametrize(
    "arns",
    [
        secret_arns("*"),
        secret_arns("AbCdEf"),
        secret_arns("?????"),
        secret_arns("???????"),
        secret_arns("*??????"),
        secret_arns(account="1" * 11),
        secret_arns(account="*"),
        secret_arns(census_api_key="arn:aws:secretsmanager:eu-west-1:" + "1" * 12 + ":secret:*-??????"),
        secret_arns(census_api_key="arn:aws:secretsmanager:eu-west-1:" + "1" * 12 + ":secret:other_secret-??????"),
        {name: arn for name, arn in secret_arns().items() if name != "hud_api_key"},
    ],
)
def test_a_literal_publisher_secret_arn_outside_the_wildcard_form_is_refused(tmp_path: Path, arns: dict[str, str]) -> None:
    with pytest.raises(ConfigurationError) as refusal:
        renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, publisher_secret_arns=arns))
    check("Set publisher_secret_arns to empty or" in str(refusal.value) and "1" * 11 not in str(refusal.value), "the message names the input, not a value")


@pytest.mark.parametrize(
    "extra",
    [
        {"alerts_topic_enabled": True, "alerts_topic_arn": ALERTS_TOPIC},
        {"athena_owner_is_project_user": True, "athena_owner_user": "example_owner"},
        {"publisher_secret_arns_enabled": True, "publisher_secret_arns": secret_arns()},
    ],
)
def test_a_derived_input_and_its_literal_are_refused_together(tmp_path: Path, extra: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError, match="not both"):
        renderer.load_preparation("example-project-ci-bucket", preparation_file(tmp_path, **extra))


def iam_pattern_matches(pattern: str, arn: str) -> bool:
    """The IAM Resource match rule: a question mark is one character, an asterisk is any run and every other character is literal."""
    return re.fullmatch("".join(".*" if char == "*" else "." if char == "?" else re.escape(char) for char in pattern), arn) is not None


def test_a_recreated_secret_with_a_new_suffix_still_matches_and_another_name_does_not(tmp_path: Path, environment: dict[str, Any]) -> None:
    value = derived_inputs(tmp_path, environment, publisher_secret_arns_enabled=True)["publisher_secret_arns"]
    rendered: dict[str, str] = value if isinstance(value, dict) else {}
    check(set(rendered) == set(PUBLISHER_NAMES), "the render names the three secrets")
    account, region = environment["AWS_ACCOUNT_ID"], environment["AWS_REGION"]
    for name, pattern in rendered.items():
        for suffix in ("Xy9Q1z", "a1B2c3", "000000"):
            check(
                iam_pattern_matches(pattern, f"arn:aws:secretsmanager:{region}:{account}:secret:{name}-{suffix}"),
                f"{name} matches a recreated secret with suffix {suffix}",
            )
        others = [other for other in PUBLISHER_NAMES if other != name]
        wrong = [
            *(f"arn:aws:secretsmanager:{region}:{account}:secret:{other}-Xy9Q1z" for other in others),
            f"arn:aws:secretsmanager:{region}:{account}:secret:{name}_extra-Xy9Q1z",
            f"arn:aws:secretsmanager:{region}:{account}:secret:{name}-Xy9Q1",
            f"arn:aws:secretsmanager:{region}:{account}:secret:{name}-Xy9Q1zz",
            f"arn:aws:secretsmanager:{region}:{'2' * 12}:secret:{name}-Xy9Q1z",
            f"arn:aws:secretsmanager:us-east-1:{account}:secret:{name}-Xy9Q1z",
        ]
        check(not any(iam_pattern_matches(pattern, arn) for arn in wrong), f"{name} matches no other name, length, account or region")


def render_iam_cli(environment: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *extra: str) -> Path:
    """Run the render command line for the IAM stack in a synthetic workspace and return the preparation file it writes."""
    (tmp_path / ".env").write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "iam", "--admin-profile", "example_admin", *extra])
    renderer.main()
    return tmp_path / "infra" / "iam" / "preparation.auto.tfvars.json"


def test_cli_writes_the_derived_iam_inputs_and_none_of_the_preparation_only_settings(
    environment: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    written = json.loads(render_iam_cli(environment, tmp_path, monkeypatch).read_text(encoding="utf-8"))
    account, region = environment["AWS_ACCOUNT_ID"], environment["AWS_REGION"]
    check(
        written.get("alerts_topic_arn") == f"arn:aws:sns:{region}:{account}:{TEST_PROJECT_NAME}_alerts_{TEST_ENVIRONMENT}",
        "the written file holds the alerts topic",
    )
    check(written.get("athena_owner_user") == f"{TEST_PROJECT_NAME}_{TEST_ENVIRONMENT}", "the written file holds the project policy user")
    check(
        written.get("publisher_secret_arns") == {name: f"arn:aws:secretsmanager:{region}:{account}:secret:{name}-??????" for name in PUBLISHER_NAMES},
        "the written file holds the three wildcard secret ARNs",
    )
    settings = {"alerts_topic_enabled", "athena_owner_is_project_user", "publisher_secret_arns_enabled"}
    check(not settings & written.keys(), "no preparation-only setting is forwarded to Terraform")
    check(sanitize(written, environment) == json.loads(LIVE_INPUTS.read_text(encoding="utf-8")), "the written file equals the sanitized live inputs")


@pytest.mark.parametrize("change", [None, "alerts_topic_arn"])
def test_cli_saved_plan_check_uses_the_derived_iam_inputs(
    environment: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str | None
) -> None:
    render_iam_cli(environment, tmp_path, monkeypatch)
    _, iam = build_configuration(environment, "example_admin")
    # The saved plan holds the inputs the reviewed render must write, built here without the command line.
    reviewed = renderer.derive_environment_inputs(renderer.load_preparation(environment["S3_BUCKET"]), iam)
    variables = {key: {"value": value} for key, value in {**iam, **reviewed}.items()}
    if change:
        variables[change] = {"value": "arn:aws:sns:eu-west-1:" + "2" * 12 + ":example_project_alerts_dev"}
    plan = {"complete": True, "variables": variables}
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "iam", "--admin-profile", "example_admin", "--check-plan", str(tmp_path / "reviewed.tfplan")])
    monkeypatch.setattr(renderer, "run_command", lambda program, args, **kwargs: CompletedProcess([program, *args], 0, json.dumps(plan), ""))
    if change:
        with pytest.raises(SystemExit):
            renderer.main()
    else:
        renderer.main()
