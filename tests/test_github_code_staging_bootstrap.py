"""The code-staging bootstrap: the GitHub environment and variables the staging workflow needs, set from .env and Terraform and read back.

Every case runs the script in this process against a stand-in of the GitHub API and of Terraform. No real ``gh`` or ``terraform`` call is made.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import github_code_staging_bootstrap as bootstrap
from tests.support import check

PROJECT, REGION, REPOSITORY = "example_project", "us-west-2", "example-owner/example-repo"
ENV_TEXT = f'PROJECT_NAME={PROJECT}\nAWS_REGION="{REGION}"\nAWS_PROFILE=example_profile\nS3_BUCKET=example-bucket\n'
CONTRACT = {"enabled": True, "repository": REPOSITORY, "roles": {"code_staging": "example_role"}, "subjects": {}, "state_stack": {}}
WANTED = {"CODE_RUN_PARAMETER": f"/{PROJECT}/current_code_run", "AWS_REGION": REGION, "OIDC_DELIVERY_ENABLED": "true"}


class Github:
    """A stand-in of GitHub's environment, branch policy and variable calls, and of ``terraform output``. It records every call in order."""

    def __init__(self, contract: Any = CONTRACT, signed_in: bool = True) -> None:
        self.contract, self.signed_in, self.calls = contract, signed_in, []
        self.environment: dict[str, Any] | None = None
        self.policies: list[dict[str, Any]] = []
        self.variables: dict[str, str] = {}
        self.next_id = 1
        self.ignore_variable_writes = False
        self.keep_reviewers = False

    def __call__(self, program: str, args: list[str]) -> tuple[int, str]:
        self.calls.append([program, *args])
        if program == "terraform":
            return (0, json.dumps(self.contract)) if self.contract is not None else (1, "")
        if args[:2] == ["auth", "status"]:
            return (0, "") if self.signed_in else (1, "")
        if args[0] == "variable":
            return self.variable(args)
        return self.api(args)

    def variable(self, args: list[str]) -> tuple[int, str]:
        name = args[2]
        if args[1] == "set":
            if not self.ignore_variable_writes:
                self.variables[name] = args[args.index("--body") + 1]
            return 0, ""
        return (0, self.variables[name]) if name in self.variables else (1, "")

    def api(self, args: list[str]) -> tuple[int, str]:
        method = args[args.index("-X") + 1] if "-X" in args else "GET"
        path = next(item for item in args[1:] if item.startswith("repos/"))
        body = json.loads(Path(args[args.index("--input") + 1]).read_text()) if "--input" in args else None
        if path.endswith("/deployment-branch-policies"):
            if method == "POST":
                self.policies.append({"id": self.next_id, "name": body["name"], "type": body["type"]})
                self.next_id += 1
                return 0, "{}"
            return 0, json.dumps({"total_count": len(self.policies), "branch_policies": self.policies})
        if "/deployment-branch-policies/" in path:
            self.policies = [item for item in self.policies if str(item["id"]) != path.rsplit("/", 1)[1]]
            return 0, ""
        if method == "PUT":
            reviewers = self.environment["reviewers"] if self.environment and self.keep_reviewers else body["reviewers"]
            self.environment = {"wait_timer": body["wait_timer"], "reviewers": reviewers, "policy": body["deployment_branch_policy"]}
            return 0, "{}"
        if self.environment is None:
            return 1, ""
        rules = [{"type": "wait_timer", "wait_timer": self.environment["wait_timer"]}] if self.environment["wait_timer"] else []
        rules += [{"type": "required_reviewers", "reviewers": self.environment["reviewers"]}] if self.environment["reviewers"] else []
        return 0, json.dumps({"name": "code-staging", "protection_rules": rules, "deployment_branch_policy": self.environment["policy"]})

    def writes(self) -> list[list[str]]:
        return [call for call in self.calls if call[0] == "gh" and (call[1] == "variable" and call[2] == "set" or "-X" in call)]


def run(tmp_path: Path, github: Github, env_text: str = ENV_TEXT) -> tuple[int, str]:
    env_file = tmp_path / ".env"
    env_file.write_text(env_text)
    lines: list[str] = []
    code = bootstrap.main(["--env-file", str(env_file)], github, lines.append)
    return code, "\n".join(lines)


def test_a_new_environment_gets_no_reviewers_no_wait_and_main_only_and_the_three_variables_from_env_and_terraform(tmp_path: Path) -> None:
    github = Github()
    code, shown = run(tmp_path, github)
    check(code == 0, f"the bootstrap failed: {shown}")
    check(
        github.environment == {"wait_timer": 0, "reviewers": [], "policy": {"protected_branches": False, "custom_branch_policies": True}},
        str(github.environment),
    )
    check([(item["name"], item["type"]) for item in github.policies] == [("main", "branch")], f"policies: {github.policies}")
    check(github.variables == WANTED, f"variables: {github.variables}")


def test_the_output_names_what_it_did_and_holds_no_value(tmp_path: Path) -> None:
    code, shown = run(tmp_path, Github())
    check(code == 0 and "code-staging" in shown and all(name in shown for name in WANTED), shown)
    check(
        all(secret not in shown for secret in (PROJECT, REGION, REPOSITORY, "example_profile", "example-bucket", "example_role")), f"a value is shown: {shown}"
    )


def test_a_second_run_changes_nothing_it_already_has(tmp_path: Path) -> None:
    github = Github()
    run(tmp_path, github)
    before = (json.dumps(github.environment), list(github.policies), dict(github.variables))
    seen = len(github.calls)
    code, shown = run(tmp_path, github)
    check(code == 0, shown)
    check((json.dumps(github.environment), github.policies, github.variables) == before, "the second run changed the state")
    check(not [call for call in github.calls[seen:] if "POST" in call or "DELETE" in call], "the main policy was created or deleted again")


def test_an_environment_with_reviewers_a_wait_timer_other_branches_and_wrong_variables_is_brought_to_the_wanted_state(tmp_path: Path) -> None:
    github = Github()
    github.environment = {"wait_timer": 30, "reviewers": [{"type": "User", "id": 1}], "policy": {"protected_branches": True, "custom_branch_policies": False}}
    github.policies = [{"id": 7, "name": "release/*", "type": "branch"}, {"id": 8, "name": "main", "type": "branch"}]
    github.next_id = 9
    github.variables = {"AWS_REGION": "eu-west-1", "OIDC_DELIVERY_ENABLED": "false", "OTHER": "keep"}
    code, shown = run(tmp_path, github)
    check(code == 0, shown)
    check(github.environment["wait_timer"] == 0 and github.environment["reviewers"] == [], f"{github.environment}")
    check([item["name"] for item in github.policies] == ["main"], f"policies: {github.policies}")
    check(github.variables == WANTED | {"OTHER": "keep"}, f"variables: {github.variables}")


def test_the_enabled_flag_of_terraform_gives_the_delivery_variable_and_nothing_is_typed(tmp_path: Path) -> None:
    github = Github(CONTRACT | {"enabled": False})
    code, _ = run(tmp_path, github)
    check(code == 0 and github.variables["OIDC_DELIVERY_ENABLED"] == "false", f"{github.variables}")


def test_a_difference_at_read_back_fails_names_the_variable_and_shows_no_value(tmp_path: Path) -> None:
    github = Github()
    github.ignore_variable_writes = True
    github.variables = {"AWS_REGION": "eu-west-1"}
    code, shown = run(tmp_path, github)
    check(code != 0 and "AWS_REGION" in shown and "differs" in shown and "eu-west-1" not in shown and REGION not in shown, f"{code}: {shown}")


def test_a_reviewer_that_stays_after_the_update_fails_the_read_back(tmp_path: Path) -> None:
    github = Github()
    github.environment = {"wait_timer": 0, "reviewers": [{"type": "User", "id": 1}], "policy": {"protected_branches": False, "custom_branch_policies": True}}
    github.keep_reviewers = True
    code, shown = run(tmp_path, github)
    check(code != 0 and "reviewers" in shown, f"{code}: {shown}")


@pytest.mark.parametrize(
    ("github", "env_text", "fragment"),
    [
        (Github(signed_in=False), ENV_TEXT, "gh"),
        (Github(contract=None), ENV_TEXT, "terraform"),
        (Github(contract={"enabled": "yes", "repository": REPOSITORY}), ENV_TEXT, "enabled"),
        (Github(contract={"enabled": True, "repository": "not a repository"}), ENV_TEXT, "repository"),
        (Github(contract=[1]), ENV_TEXT, "terraform"),
        (Github(), "AWS_REGION=us-west-2\n", "PROJECT_NAME"),
        (Github(), "PROJECT_NAME=has space\nAWS_REGION=us-west-2\n", "PROJECT_NAME"),
        (Github(), "PROJECT_NAME=example_project\nAWS_REGION=../x\n", "AWS_REGION"),
    ],
)
def test_a_missing_login_output_or_env_value_stops_before_any_write(tmp_path: Path, github: Github, env_text: str, fragment: str) -> None:
    code, shown = run(tmp_path, github, env_text)
    check(code != 0 and fragment in shown, f"{code}: {shown}")
    check(github.writes() == [] and github.environment is None, "something was written")


def test_the_environment_file_is_read_for_its_names_only_and_a_missing_file_stops_the_run(tmp_path: Path) -> None:
    github = Github()
    lines: list[str] = []
    check(bootstrap.main(["--env-file", str(tmp_path / "absent.env")], github, lines.append) != 0 and github.calls == [], f"{lines}: {github.calls}")


def test_the_default_runner_reports_a_program_it_does_not_run_as_a_failure() -> None:
    check(bootstrap.external("a-program-that-is-not-installed", [])[0] != 0, "an unknown program is not a failure")
