import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.infrastructure import render_project_config as renderer
from scripts.infrastructure.render_project_config import ConfigurationError, build_configuration, verify_plan_variables

TEST_PROJECT_PROFILE = "example_project_profile"

@pytest.fixture
def environment(monkeypatch):
    monkeypatch.setattr(renderer, "PROJECT_PROFILE", TEST_PROJECT_PROFILE)
    return {
        "AWS_ACCOUNT_ID": "111111111111",
        "AWS_PROFILE": TEST_PROJECT_PROFILE,
        "AWS_REGION": "us-west-2",
        "PROJECT_NAME": "example_project",
        "S3_BUCKET": "example-project-ci-bucket",
    }

@pytest.fixture
def caller_identity(environment):
    return {"Account": environment["AWS_ACCOUNT_ID"], "Arn": f"arn:aws:iam::{environment['AWS_ACCOUNT_ID']}:user/{environment['AWS_PROFILE']}"}

def test_configuration_has_one_source_and_no_credentials(environment):
    environment["MODEL_API_KEY"] = "example-not-a-real-secret"
    storage, iam = build_configuration(environment)
    assert storage["expected_account_id"] == iam["expected_account_id"] == environment["AWS_ACCOUNT_ID"]
    assert storage["data_bucket_name"] == iam["data_bucket_name"] == environment["S3_BUCKET"]
    assert storage["project_name"] == iam["project_name"] == environment["PROJECT_NAME"]
    assert storage["aws_profile"] == iam["deployment_user_name"] == environment["AWS_PROFILE"]
    assert "aws_profile" not in iam
    assert "MODEL_API_KEY" not in storage and "MODEL_API_KEY" not in iam

def test_admin_selection_does_not_change_project_profile(environment):
    storage, iam = build_configuration(environment, "example_admin")
    assert storage["aws_profile"] == TEST_PROJECT_PROFILE
    assert iam["aws_profile"] == "example_admin"
    assert iam["deployment_user_name"] == TEST_PROJECT_PROFILE
    assert environment["AWS_PROFILE"] == TEST_PROJECT_PROFILE

@pytest.mark.parametrize("name", ["AWS_ACCOUNT_ID", "AWS_REGION", "PROJECT_NAME", "S3_BUCKET", "AWS_PROFILE"])
def test_missing_values_are_rejected(environment, name):
    del environment[name]
    with pytest.raises(ConfigurationError):
        build_configuration(environment)

@pytest.mark.parametrize("name,value", [("AWS_ACCOUNT_ID", "bad"), ("PROJECT_NAME", "wrong-name"), ("S3_BUCKET", "*"), ("AWS_PROFILE", "example_admin")])
def test_invalid_values_are_rejected_without_echoing_values(environment, name, value):
    environment[name] = value
    with pytest.raises(ConfigurationError) as error:
        build_configuration(environment)
    assert value not in str(error.value)

@pytest.mark.parametrize("user", [TEST_PROJECT_PROFILE, "example_different_user", ""])
def test_duplicate_user_configuration_is_rejected(environment, user):
    environment["IAM_DEPLOYMENT_USER"] = user
    with pytest.raises(ConfigurationError, match="derived from AWS_PROFILE"):
        build_configuration(environment)

@pytest.mark.parametrize("profile", ["", " ", TEST_PROJECT_PROFILE])
def test_project_profile_cannot_administer_itself(environment, profile):
    with pytest.raises(ConfigurationError):
        build_configuration(environment, profile)

@pytest.mark.parametrize("user_path", ["", "example_team/"])
def test_identity_check_uses_explicit_profile_and_ignores_ambient_credentials(environment, caller_identity, monkeypatch, user_path):
    storage, _ = build_configuration(environment)
    caller_identity["Arn"] = f"arn:aws:iam::{environment['AWS_ACCOUNT_ID']}:user/{user_path}{environment['AWS_PROFILE']}"
    monkeypatch.setenv("AWS_PROFILE", "example_admin")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "example-not-a-real-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "example-not-a-real-secret")
    calls = []
    def run(command, **kwargs):
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
def test_wrong_aws_identity_is_rejected(environment, caller_identity, monkeypatch, change):
    storage, _ = build_configuration(environment)
    account = environment["AWS_ACCOUNT_ID"]
    profile = environment["AWS_PROFILE"]
    replacements = {
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
def test_malformed_identity_responses_are_rejected(environment, monkeypatch, response):
    storage, _ = build_configuration(environment)
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, response, ""))
    with pytest.raises(ConfigurationError, match="invalid response"):
        renderer.verify_project_identity(storage)

def test_failed_identity_request_does_not_echo_diagnostics(environment, monkeypatch):
    storage, _ = build_configuration(environment)
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 1, "", "example private diagnostic"))
    with pytest.raises(ConfigurationError, match="No plan or apply") as error:
        renderer.verify_project_identity(storage)
    assert "example private diagnostic" not in str(error.value)

@pytest.mark.parametrize("failure", [FileNotFoundError("example private path"), subprocess.TimeoutExpired("example private command", 30)])
def test_identity_check_handles_missing_cli_and_timeouts(environment, monkeypatch, failure):
    storage, _ = build_configuration(environment)
    def run(*args, **kwargs):
        raise failure
    monkeypatch.setattr(renderer.subprocess, "run", run)
    with pytest.raises(ConfigurationError, match="Could not verify") as error:
        renderer.verify_project_identity(storage)
    assert "example private" not in str(error.value)

def test_saved_plan_must_match_current_dotenv(environment):
    storage, _ = build_configuration(environment)
    plan = {"complete": True, "errored": False, "variables": {key: {"value": value} for key, value in storage.items()}}
    verify_plan_variables(plan, storage)
    with pytest.raises(ConfigurationError):
        verify_plan_variables(plan, {**storage, "data_bucket_name": "different-bucket"})

@pytest.mark.parametrize("complete,errored", [(False, False), (True, True)])
def test_incomplete_or_errored_plan_is_rejected(environment, complete, errored):
    storage, _ = build_configuration(environment)
    with pytest.raises(ConfigurationError):
        verify_plan_variables({"complete": complete, "errored": errored}, storage)

@pytest.fixture
def wrapper_environment():
    return {name: value for name, value in os.environ.items() if name != "TF_CLI_ARGS" and not name.startswith("TF_CLI_ARGS_")}

@pytest.mark.parametrize("arguments", [["plan"], ["apply", "example.tfplan"], ["validate"]])
@pytest.mark.parametrize("name", ["TF_CLI_ARGS", "TF_CLI_ARGS_plan", "TF_CLI_ARGS_apply", "TF_CLI_ARGS_show", "TF_CLI_ARGS_validate", "TF_CLI_ARGS_future"])
def test_wrapper_rejects_inherited_arguments_before_configuration_checks(arguments, name, tmp_path, wrapper_environment):
    script = Path(__file__).resolve().parents[1] / "scripts" / "infrastructure" / "terraform.sh"
    value = "-var-file=example_private.tfvars" if name == "TF_CLI_ARGS" else "-var=project_name=unexpected_project"
    wrapper_environment.update({"PYTHON_BIN": str(tmp_path / "must_not_run"), name: value})
    result = subprocess.run(["bash", str(script), *arguments], capture_output=True, text=True, env=wrapper_environment)
    assert result.returncode == 2
    assert f"Unset {name};" in result.stderr
    assert value not in result.stderr and "must_not_run" not in result.stderr
    assert not result.stdout

@pytest.mark.parametrize("empty_exports", [False, True])
@pytest.mark.parametrize("arguments", [["plan", "-out=example.tfplan", "-no-color"], ["validate", "-no-color"], ["apply", "example.tfplan"]])
def test_wrapper_preserves_explicit_options_without_inherited_arguments(arguments, empty_exports, tmp_path, wrapper_environment):
    script = Path(__file__).resolve().parents[1] / "scripts" / "infrastructure" / "terraform.sh"
    for name in ("example_python", "terraform"):
        stub = tmp_path / name
        stub.write_text(f'#!/usr/bin/env bash\nprintf "{name}\\n"\nprintf "%s\\n" "$@"\n', encoding="utf-8")
        stub.chmod(0o700)
    wrapper_environment.update({"PYTHON_BIN": str(tmp_path / "example_python"), "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"})
    if empty_exports:
        wrapper_environment.update({"TF_CLI_ARGS": "", "TF_CLI_ARGS_plan": "", "TF_CLI_ARGS_show": ""})
    result = subprocess.run(["bash", str(script), *arguments], capture_output=True, text=True, env=wrapper_environment)
    assert result.returncode == 0, result.stderr
    output = result.stdout.splitlines()
    terraform_arguments = output[output.index("terraform") + 1:]
    expected = [arguments[0], *([] if arguments[0] == "validate" else ["-input=false"]), *arguments[1:]]
    assert terraform_arguments[1:] == expected
    assert terraform_arguments[0].startswith("-chdir=")
    if arguments[0] == "apply":
        assert "--check-plan" in output[:output.index("terraform")]

@pytest.mark.parametrize("arguments", [["plan"], ["apply", "example.tfplan"], ["--iam", "--admin-profile", "example_admin", "plan"], ["validate"]])
def test_wrapper_requests_identity_verification_for_plans_and_applies(arguments, tmp_path, wrapper_environment):
    script = Path(__file__).resolve().parents[1] / "scripts" / "infrastructure" / "terraform.sh"
    stub = tmp_path / "example_python"
    stub.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@"\nexit 1\n', encoding="utf-8")
    stub.chmod(0o700)
    wrapper_environment["PYTHON_BIN"] = str(stub)
    result = subprocess.run(["bash", str(script), *arguments], capture_output=True, text=True, env=wrapper_environment)
    assert result.returncode == 1
    assert ("--verify-identity" in result.stdout.splitlines()) == (arguments != ["validate"])

@pytest.mark.parametrize("matches", [True, False])
def test_cli_checks_identity_before_writing_inputs(environment, caller_identity, tmp_path, monkeypatch, matches):
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
def test_cli_checks_selected_saved_plan(environment, tmp_path, monkeypatch, stack):
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    storage, iam = build_configuration(environment, "example_admin")
    values = storage if stack == "storage" else iam
    plan = {"complete": True, "variables": {key: {"value": value} for key, value in values.items()}}
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", stack, "--admin-profile", "example_admin", "--check-plan", str(tmp_path / "reviewed.tfplan")])
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, json.dumps(plan), ""))
    renderer.main()

@pytest.mark.parametrize("arguments", [["--check-plan", "example.tfplan"], ["--env-file", "missing.env"]])
def test_cli_rejects_invalid_requests(environment, tmp_path, monkeypatch, arguments):
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", *arguments])
    with pytest.raises(SystemExit) as error:
        renderer.main()
    assert error.value.code == 2

def test_cli_rejects_unreadable_plan_without_echoing_output(environment, tmp_path, monkeypatch, capsys):
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in environment.items()), encoding="utf-8")
    monkeypatch.setattr(renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "storage", "--check-plan", str(tmp_path / "broken.tfplan")])
    monkeypatch.setattr(renderer.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 1, "", "example private diagnostic"))
    with pytest.raises(SystemExit):
        renderer.main()
    assert "example private diagnostic" not in capsys.readouterr().err

def assert_test_identifiers_are_abstract(root):
    # The configured identity follows <project_name>_user; checkout names are machine-specific.
    project_name = renderer.PROJECT_PROFILE.removesuffix("_user")
    identifiers = {renderer.PROJECT_PROFILE, project_name, project_name.replace("_", "-")}
    sources = [path for folder in ("tests", "infra/tests", "infra/iam/tests") for path in (root / folder).rglob("*") if path.suffix in {".py", ".hcl", ".json"}]
    offending = [str(path.relative_to(root)) for path in sources if any(identifier in path.read_text(encoding="utf-8") for identifier in identifiers)]
    assert not offending, f"Use synthetic fixtures or injected configuration instead of project identifiers in: {offending}"

def test_test_sources_do_not_embed_project_identifiers():
    assert_test_identifiers_are_abstract(Path(__file__).resolve().parents[1])

@pytest.mark.parametrize("checkout_name", ["project", "app", "renamed_checkout"])
def test_identifier_check_is_independent_of_checkout_name(tmp_path, checkout_name):
    source_root = Path(__file__).resolve().parents[1]
    root = tmp_path / checkout_name
    for folder in ("tests", "infra/tests", "infra/iam/tests"):
        shutil.copytree(source_root / folder, root / folder, ignore=shutil.ignore_patterns("__pycache__"))
    assert_test_identifiers_are_abstract(root)

@pytest.mark.parametrize("folder,suffix", [("tests", ".py"), ("infra/tests", ".hcl"), ("infra/iam/tests", ".json")])
@pytest.mark.parametrize("spelling", ["profile", "underscores", "dashes"])
def test_identifier_check_still_rejects_configured_identifiers(tmp_path, folder, suffix, spelling):
    root = tmp_path / "project"
    source = root / folder / f"example{suffix}"
    source.parent.mkdir(parents=True)
    identifier = renderer.PROJECT_PROFILE
    if spelling != "profile":
        identifier = identifier.removesuffix("_user")
    if spelling == "dashes":
        identifier = identifier.replace("_", "-")
    source.write_text(json.dumps({"example": identifier}), encoding="utf-8")
    with pytest.raises(AssertionError, match="Use synthetic fixtures"):
        assert_test_identifiers_are_abstract(root)
