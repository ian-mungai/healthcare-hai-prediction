"""Defects planted in copies of the owner's IAM policy scripts and the script tests that must catch each one.

``run_script_defects`` copies the scripts, the policy templates and ``tests/test_iam_policy_scripts.py`` into a scratch workspace, records the passing
baseline, then applies one text change at a time and runs the tests there. A defect counts as caught when a named test fails.
"""

from __future__ import annotations

import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
COPIED = (
    "infra/iam/scripts/render_policy.py",
    "infra/iam/scripts/manage_policies.py",
    "infra/iam/scripts/export_policies.py",
    "scripts/process.py",
    "scripts/infrastructure/render_project_config.py",
    "scripts/infrastructure/secret_store.py",
    "infra/iam/variables.tf",
    "tests/test_iam_policy_scripts.py",
    "tests/support.py",
    "pyproject.toml",
)
MANAGE, RENDER, EXPORT = COPIED[1], COPIED[0], COPIED[2]
SECRET_STORE, VARIABLES = COPIED[5], COPIED[6]
CONFIRMATION_CHECK = 'if arguments.action == "apply" and environment.get(CONFIRMATION_VARIABLE) != CONFIRMATION:'
CONFIRMATION_REFUSAL = 'raise ConfigurationError(f"Set {CONFIRMATION_VARIABLE}={CONFIRMATION} to apply; nothing was read or changed.")'
REFUSE_UNCONFIRMED = "test_apply_refuses_without_the_confirmation_word_before_any_aws_call"
REFUSE_PROJECT_USER = "test_the_wrong_account_or_the_project_user_is_refused_before_any_read"
PLAN_CHANGES_NOTHING = "test_plan_in_an_empty_account_lists_every_creation_and_changes_nothing"
RENDERER_REFUSALS = "test_renderer_refuses_unresolved_unknown_malformed_and_oversized_templates"
NAME_SOURCE = "test_the_names_come_from_the_terraform_variable_defaults_and_nowhere_else"


@dataclass(frozen=True)
class ScriptDefect:
    """A text change to one script and the test that must fail because of it."""

    name: str
    path: str
    old: str
    new: str
    test: str


DEFECTS = (
    ScriptDefect(
        "apply runs without the confirmation word",
        MANAGE,
        CONFIRMATION_CHECK,
        "if False:",
        REFUSE_UNCONFIRMED,
    ),
    ScriptDefect(
        "a near miss of the confirmation word is accepted",
        MANAGE,
        "environment.get(CONFIRMATION_VARIABLE) != CONFIRMATION",
        'CONFIRMATION not in (environment.get(CONFIRMATION_VARIABLE) or "")',
        REFUSE_UNCONFIRMED,
    ),
    ScriptDefect(
        "apply connects before it checks the confirmation word",
        MANAGE,
        f"    {CONFIRMATION_CHECK}\n        {CONFIRMATION_REFUSAL}\n",
        '    session_factory(profile_name=arguments.admin_profile, region_name=storage["aws_region"])\n',
        REFUSE_UNCONFIRMED,
    ),
    ScriptDefect(
        "the project profile may act as the administrator",
        MANAGE,
        'if arguments.admin_profile == storage["aws_profile"] or not arguments.admin_profile.strip():',
        "if False:",
        "test_the_administrator_profile_is_required_and_is_not_the_project_profile",
    ),
    ScriptDefect(
        "a profile in another account is accepted",
        MANAGE,
        'identity["Account"] != account',
        "False",
        REFUSE_PROJECT_USER,
    ),
    ScriptDefect(
        "the plan writes to IAM",
        MANAGE,
        '    if arguments.action == "plan":\n        return 0\n',
        "",
        PLAN_CHANGES_NOTHING,
    ),
    ScriptDefect(
        "an update skips the version pruning",
        MANAGE,
        "        remove_oldest_nondefault_version(iam, arn)\n",
        "",
        "test_an_update_frees_a_version_slot_first_and_makes_the_new_version_the_default",
    ),
    ScriptDefect(
        "a failed step stops the others",
        MANAGE,
        'results.append(StepResult(step, "failed", f"{type(error).__name__}: {error}"))',
        "raise",
        "test_a_failed_step_is_reported_and_the_others_still_run",
    ),
    ScriptDefect(
        "stored policies are compared without IAM's normal form",
        MANAGE,
        'return json.dumps(normalize_iam_value(dict(document)), sort_keys=True, separators=(",", ":"))',
        'return json.dumps(dict(document), separators=(",", ":"))',
        "test_stored_documents_in_the_form_iam_returns_count_as_unchanged",
    ),
    ScriptDefect(
        "a retired policy is deleted while a role holds it",
        MANAGE,
        "    if others:\n",
        "    if False:\n",
        "test_a_retired_policy_held_by_a_role_is_kept_and_reported",
    ),
    ScriptDefect(
        "thirteen policies go into one group",
        MANAGE,
        '"platform": ("autoscaling", "cloudwatch", "dlm", "ec2", "ecr", "ecs", "logs", "sns"),',
        '"platform": (),',
        "test_every_shipped_template_belongs_to_one_group_of_at_most_ten",
    ),
    ScriptDefect(
        "a group member besides the project user is ignored",
        MANAGE,
        "    if state.members - {names.user}:\n",
        "    if False:\n",
        "test_a_group_with_another_member_is_refused_before_any_change",
    ),
    ScriptDefect(
        "an extra managed policy on a group is ignored",
        MANAGE,
        "    if extra:\n",
        "    if False:\n",
        "test_a_group_with_an_extra_managed_policy_is_refused_before_any_change",
    ),
    ScriptDefect(
        "an extra managed policy is matched by name instead of ARN",
        MANAGE,
        "extra = sorted(arn for arn in state.attached.values() if arn not in intended)",
        "extra = sorted(arn for name, arn in state.attached.items() if policy_arn(account, name) not in intended)",
        "test_a_group_with_an_extra_managed_policy_is_refused_before_any_change",
    ),
    ScriptDefect(
        "a retired policy on a group is allowed",
        MANAGE,
        "extra = sorted(arn for arn in state.attached.values() if arn not in intended)",
        "extra = sorted(arn for arn in state.attached.values() if arn not in intended | {policy_arn(account, names.policy(s)) for s in RETIRED_SERVICES})",
        "test_a_group_with_a_retired_policy_is_refused_before_any_change",
    ),
    ScriptDefect(
        "a missing group membership is not planned",
        MANAGE,
        '            steps.append(Step("add_user", names.user, group))\n',
        "            pass\n",
        "test_a_group_without_the_project_user_is_repaired_not_refused",
    ),
    ScriptDefect(
        "an inline policy on a group is ignored",
        MANAGE,
        "    if state.inline:\n",
        "    if False:\n",
        "test_a_group_with_an_inline_policy_is_refused_before_any_change",
    ),
    ScriptDefect(
        "a group problem is listed but the run goes on",
        MANAGE,
        "    if problems:\n",
        "    if False:\n",
        "test_a_group_with_another_member_is_refused_before_any_change",
    ),
    ScriptDefect(
        "the other grants of the project user are not reported",
        MANAGE,
        "        if items:\n",
        "        if False:\n",
        "test_the_other_grants_of_the_project_user_are_reported",
    ),
    ScriptDefect(
        "the renderer leaves an unresolved placeholder",
        RENDER,
        "    if missing:\n",
        "    if False:\n",
        RENDERER_REFUSALS,
    ),
    ScriptDefect(
        "the renderer accepts an oversized policy",
        RENDER,
        "MAX_POLICY_CHARACTERS = 6144",
        "MAX_POLICY_CHARACTERS = 10**9",
        RENDERER_REFUSALS,
    ),
    ScriptDefect(
        "the renderer repeats only the first name of a list",
        RENDER,
        'expanded += [item.replace("${" + used[0] + "}", name) for name in names[used[0]]]',
        'expanded += [item.replace("${" + used[0] + "}", name) for name in names[used[0]][:1]]',
        NAME_SOURCE,
    ),
    ScriptDefect(
        "the renderer ignores the names it is given and reads the defaults",
        RENDER,
        "expand_names(document, names)",
        "expand_names(document, terraform_names())",
        NAME_SOURCE,
    ),
    ScriptDefect(
        "the read-only Airflow administrator password is also written",
        SECRET_STORE,
        "    managed = [value for name, value in defaults.items() if value.startswith(PREFIX) and name not in (*READ_ONLY_VARIABLES, *AWS_POLARIS_VARIABLES)]",
        "    managed = [value for name, value in defaults.items() if value.startswith(PREFIX)]",
        NAME_SOURCE,
    ),
    ScriptDefect(
        "a list stands for a single name",
        RENDER,
        "        if not isinstance(value, str):",
        "        if False:",
        NAME_SOURCE,
    ),
    ScriptDefect(
        "the lakehouse role names are not read from the variables",
        SECRET_STORE,
        "if name in (*ROLE_VARIABLES, *AWS_POLARIS_VARIABLES) or value.startswith(PREFIX)]",
        "if value.startswith(PREFIX)]",
        NAME_SOURCE,
    ),
    ScriptDefect(
        "a name placeholder outside a list is accepted",
        RENDER,
        'raise PolicyRenderError("a name placeholder must be an element of a list")',
        "return node",
        RENDERER_REFUSALS,
    ),
    ScriptDefect(
        "the exporter writes the account ID",
        EXPORT,
        'for name in ("DATA_BUCKET_NAME", "AWS_ACCOUNT_ID", "AWS_REGION", "PROJECT_NAME"):',
        'for name in ("DATA_BUCKET_NAME", "AWS_REGION", "PROJECT_NAME"):',
        "test_the_exporter_replaces_deployment_values_with_placeholders",
    ),
)


def run_tests(workspace: Path) -> tuple[int, str]:
    """Run the script tests in the workspace and return the exit code and the output."""
    try:
        result = run_command(
            sys.executable,
            ["-m", "pytest", "tests/test_iam_policy_scripts.py", "-q", "-rf", "--no-header", "-p", "no:cacheprovider"],
            cwd=workspace,
            env={"PATH": "/usr/bin:/bin", "HOME": str(workspace), "PYTHONDONTWRITEBYTECODE": "1", "AWS_EC2_METADATA_DISABLED": "true"},
            timeout=240,
        )
    except TimeoutExpired:
        return 124, "timed out"
    return result.returncode, result.stdout + result.stderr


def run_script_defects(scratch: Path) -> dict[str, Any]:
    """Record the baseline, then plant each defect in a fresh copy and name the tests that fail."""
    baseline = scratch / "baseline"
    for relative in COPIED:
        (baseline / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(REPO_ROOT / relative, baseline / relative)
    shutil.copytree(REPO_ROOT / "infra/iam/policies", baseline / "infra/iam/policies")
    shutil.copytree(REPO_ROOT / "infra/iam/owner_policies", baseline / "infra/iam/owner_policies")
    code, output = run_tests(baseline)
    report: dict[str, Any] = {"baseline_passed": code == 0, "baseline_summary": output.strip().splitlines()[-1] if output.strip() else "", "defects": []}
    for index, defect in enumerate(DEFECTS):
        workspace = scratch / f"defect_{index:02d}"
        shutil.copytree(baseline, workspace)
        target = workspace / defect.path
        text = target.read_text(encoding="utf-8")
        if text.count(defect.old) != 1:
            report["defects"].append({"defect": defect.name, "caught": False, "problem": "the planted text must occur once in the script"})
            continue
        target.write_text(text.replace(defect.old, defect.new), encoding="utf-8")
        code, output = run_tests(workspace)
        failed = sorted(set(re.findall(r"^FAILED tests/test_iam_policy_scripts\.py::(\w+)", output, flags=re.MULTILINE)))
        report["defects"].append({"defect": defect.name, "expected_test": defect.test, "failed_tests": failed, "caught": defect.test in failed})
    return report
