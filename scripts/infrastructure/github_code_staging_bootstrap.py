"""Set up the GitHub side of the code-staging workflow with the owner's signed-in ``gh``: the ``code-staging`` environment and three repository
variables.

The environment has no reviewers and no wait timer and may deploy from ``main`` only (one branch policy, ``main``; every other policy is removed). The
variables are ``CODE_RUN_PARAMETER`` (the parameter that names the current code run), ``AWS_REGION`` and ``OIDC_DELIVERY_ENABLED``. Nothing is typed:
the project name and the region come from ``.env`` and the repository and the delivery flag come from the output ``github_oidc_contract`` of the IAM
stack. The script sets each thing and then reads all of it back, and fails when any part differs. Running it again changes nothing it already has. The
output names what it did and never shows a value.

    .venv/bin/python -m scripts.infrastructure.github_code_staging_bootstrap

"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.process import run_command

ENVIRONMENT = "code-staging"
BRANCH = "main"
ENV_FILE = Path(".env")
NAME = re.compile(r"[A-Za-z0-9_]+")
REGION = re.compile(r"[a-z]{2}(?:-[a-z]+)+-[0-9]")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
Runner = Callable[[str, list[str]], tuple[int, str]]
Say = Callable[[str], None]


class Stop(Exception):
    """The bootstrap cannot go on. The message names the part and never a value."""


def external(program: str, args: list[str]) -> tuple[int, str]:
    """Run ``gh`` or ``terraform`` with the caller's environment and return its exit code and standard output. Its error output is not kept, because it can hold
    a value. A program that is not installed is a failure."""
    try:
        if program == "gh":
            result = run_command("gh", args, timeout=120.0)
        elif program == "terraform":
            result = run_command("terraform", args, timeout=120.0)
        else:
            return 2, ""
    except FileNotFoundError:
        return 127, ""
    return result.returncode, result.stdout


def read_env(path: Path) -> dict[str, str]:
    """The project name and the region of ``.env``. Any other line is not read."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        raise Stop("the environment file is not readable") from None
    found: dict[str, str] = {}
    for line in lines:
        name, _, value = line.partition("=")
        if name in {"PROJECT_NAME", "AWS_REGION"}:
            found[name] = value.strip().strip('"').strip("'")
    if not NAME.fullmatch(found.get("PROJECT_NAME", "")):
        raise Stop("PROJECT_NAME in .env is missing or not letters, digits and underscores")
    if not REGION.fullmatch(found.get("AWS_REGION", "")):
        raise Stop("AWS_REGION in .env is missing or not a region name")
    return found


def terraform_contract(runner: Runner) -> dict[str, Any]:
    code, text = runner("terraform", ["-chdir=infra/iam", "output", "-json", "github_oidc_contract"])
    try:
        contract = json.loads(text) if code == 0 else None
    except ValueError:
        contract = None
    if not isinstance(contract, dict):
        raise Stop("terraform did not give the output github_oidc_contract of the IAM stack")
    if not isinstance(contract.get("enabled"), bool):
        raise Stop("the output github_oidc_contract has no true or false value for enabled")
    if not isinstance(contract.get("repository"), str) or not REPOSITORY.fullmatch(contract["repository"]):
        raise Stop("the output github_oidc_contract has no repository in the form owner/name")
    return contract


def wanted_variables(env: dict[str, str], contract: dict[str, Any]) -> dict[str, str]:
    return {
        "CODE_RUN_PARAMETER": f"/{env['PROJECT_NAME']}/current_code_run",
        "AWS_REGION": env["AWS_REGION"],
        "OIDC_DELIVERY_ENABLED": "true" if contract["enabled"] else "false",
    }


def gh(runner: Runner, args: list[str]) -> str:
    code, text = runner("gh", args)
    if code != 0:
        raise Stop(f"gh {' '.join(args[:2])} failed")
    return text


def api(runner: Runner, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    """One GitHub REST call through ``gh api``. A body goes in a file, so no value is on a command line that others can read."""
    args = ["api", "-X", method, path]
    if body is None:
        text = gh(runner, args)
    else:
        with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8") as handle:
            handle.write(json.dumps(body))
            handle.flush()
            text = gh(runner, [*args, "--input", handle.name])
    return json.loads(text) if text.strip() else {}


def apply(runner: Runner, repository: str, variables: dict[str, str], say: Say) -> None:
    base = f"repos/{repository}/environments/{ENVIRONMENT}"
    api(
        runner,
        "PUT",
        base,
        {"wait_timer": 0, "reviewers": [], "deployment_branch_policy": {"protected_branches": False, "custom_branch_policies": True}},
    )
    say(f"environment {ENVIRONMENT}: set with no reviewers, no wait timer and custom branch policies")
    policies = api(runner, "GET", f"{base}/deployment-branch-policies").get("branch_policies", [])
    for policy in policies:
        if (policy.get("name"), policy.get("type")) != (BRANCH, "branch"):
            api(runner, "DELETE", f"{base}/deployment-branch-policies/{policy['id']}")
    if not any((policy.get("name"), policy.get("type")) == (BRANCH, "branch") for policy in policies):
        api(runner, "POST", f"{base}/deployment-branch-policies", {"name": BRANCH, "type": "branch"})
    say(f"environment {ENVIRONMENT}: the one branch policy is {BRANCH}")
    for name, value in variables.items():
        gh(runner, ["variable", "set", name, "--body", value, "--repo", repository])
        say(f"variable {name}: set")


def verify(runner: Runner, repository: str, variables: dict[str, str]) -> list[str]:
    """Everything the bootstrap set, read back. Each difference is named by its part, never by its value."""
    base = f"repos/{repository}/environments/{ENVIRONMENT}"
    problems = []
    environment = api(runner, "GET", base)
    rules = environment.get("protection_rules", [])
    if any(rule.get("type") == "required_reviewers" for rule in rules):
        problems.append("reviewers differs: the environment still has required reviewers")
    if any(rule.get("type") == "wait_timer" and rule.get("wait_timer") for rule in rules):
        problems.append("wait timer differs: the environment still has a wait timer")
    if environment.get("deployment_branch_policy") != {"protected_branches": False, "custom_branch_policies": True}:
        problems.append("deployment branch policy differs: it does not take custom branch policies only")
    policies = api(runner, "GET", f"{base}/deployment-branch-policies").get("branch_policies", [])
    if [(policy.get("name"), policy.get("type")) for policy in policies] != [(BRANCH, "branch")]:
        problems.append("branch policies differ: they are not main alone")
    for name, value in variables.items():
        code, text = runner("gh", ["variable", "get", name, "--repo", repository])
        if code != 0 or text.strip() != value:
            problems.append(f"variable {name} differs")
    return problems


def main(argv: list[str] | None = None, runner: Runner | None = None, say: Say = print) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, default=ENV_FILE)
    args = parser.parse_args(argv)
    if runner is None:

        def direct(program: str, arguments: list[str]) -> tuple[int, str]:
            return external(program, arguments)

        runner = direct
    try:
        env = read_env(args.env_file)
        contract = terraform_contract(runner)
        variables = wanted_variables(env, contract)
        code, _ = runner("gh", ["auth", "status"])
        if code != 0:
            raise Stop("gh is not signed in: run gh auth login first")
        apply(runner, contract["repository"], variables, say)
        problems = verify(runner, contract["repository"], variables)
    except Stop as error:
        say(f"stopped: {error}. Nothing further was changed.")
        return 1
    for problem in problems:
        say(f"read back: {problem}")
    say("read back: every part matches" if not problems else "read back: FAILED")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
