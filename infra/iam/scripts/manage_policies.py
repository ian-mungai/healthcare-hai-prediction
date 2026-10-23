"""Plan or apply the per-service IAM policies of the project profile with the administrator profile.

``plan`` is the dry run: it renders the templates, reads IAM and prints what ``apply`` would do, and changes nothing. ``apply`` needs
``CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies`` and checks it before any AWS call. Policies are customer-managed, one per service. They
attach to two IAM groups (the managed policy quota per user is 10), and the project user joins both. The retired grouped policies are detached and deleted.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from infra.iam.scripts.render_policy import PolicyRenderError, load_documents, placeholder_values
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration

CONFIRMATION = "apply-healthcare-hai-prediction-policies"
CONFIRMATION_VARIABLE = "CONFIRM_IAM_POLICIES"
IAM_SCALAR_OR_LIST_FIELDS = {"Action", "NotAction", "Resource", "NotResource", "Principal", "NotPrincipal"}
MAX_POLICY_VERSIONS = 5
# The services of each group. Each group holds at most 10 policies. A policy file whose service is in no group stops the run.
GROUP_SERVICES = {
    "data": ("iam", "s3", "secretsmanager", "ssm", "sts"),
    "platform": ("autoscaling", "cloudwatch", "dlm", "ec2", "ecr", "ecs", "logs", "sns"),
}
MAX_GROUP_POLICIES = 10
# Earlier grouped policies, replaced by the per-service files. Their old Terraform resources are removed from state without a destroy.
RETIRED_SERVICES = ("lakehouse", "platform_network", "platform_services", "platform_tags", "secrets")
SessionFactory = Callable[..., Any]


@dataclass(frozen=True)
class Step:
    """One change. ``kind`` is create_policy, update_policy, keep_policy, create_group, attach_policy, add_user, detach_user or retire_policy."""

    kind: str
    name: str
    target: str = ""

    def text(self) -> str:
        label = self.kind.replace("_", " ").upper()
        return f"{label:16} {self.name}" + (f" -> {self.target}" if self.target else "")


@dataclass(frozen=True)
class StepResult:
    step: Step
    status: str
    detail: str


@dataclass(frozen=True)
class Names:
    """The names derived from ``.env``: the project user, its groups and its policies."""

    project: str
    environment: str

    @property
    def user(self) -> str:
        return f"{self.project}_{self.environment}"

    def group(self, suffix: str) -> str:
        return f"{self.project}_{suffix}_{self.environment}"

    def policy(self, service: str) -> str:
        return f"{self.project}_{service}_policy"

    def service(self, policy_name: str) -> str:
        return policy_name.removeprefix(f"{self.project}_").removesuffix("_policy")


def group_of(names: Names, service: str) -> str:
    """The group a service's policy belongs to."""
    for suffix, services in GROUP_SERVICES.items():
        if service in services:
            return names.group(suffix)
    raise PolicyRenderError(f"the {service} policy belongs to no group; add it to GROUP_SERVICES in manage_policies.py")


def normalize_iam_value(value: Any, normalize_lists: bool = False) -> Any:
    """The form IAM stores: sorted keys, and one-item lists of actions, resources and principals as scalars, in a fixed order."""
    if isinstance(value, dict):
        return {key: normalize_iam_value(child, normalize_lists or key in IAM_SCALAR_OR_LIST_FIELDS) for key, child in sorted(value.items())}
    if isinstance(value, list):
        normalized = [normalize_iam_value(child, normalize_lists) for child in value]
        if not normalize_lists:
            return normalized
        normalized.sort(key=lambda child: json.dumps(child, sort_keys=True, separators=(",", ":")))
        return normalized[0] if len(normalized) == 1 else normalized
    return value


def canonical(document: Mapping[str, Any]) -> str:
    return json.dumps(normalize_iam_value(dict(document)), sort_keys=True, separators=(",", ":"))


def paged(iam: Any, operation: str, key: str, **arguments: Any) -> list[Any]:
    """Every item of a paginated list call."""
    return [item for page in iam.get_paginator(operation).paginate(**arguments) for item in page.get(key, [])]


def is_missing(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") == "NoSuchEntity"


def local_policies(iam: Any) -> dict[str, dict[str, Any]]:
    return {policy["PolicyName"]: policy for policy in paged(iam, "list_policies", "Policies", Scope="Local")}


def stored_document(iam: Any, policy: Mapping[str, Any]) -> dict[str, Any]:
    version = iam.get_policy_version(PolicyArn=policy["Arn"], VersionId=policy["DefaultVersionId"])
    document: dict[str, Any] = version["PolicyVersion"]["Document"]
    return document


@dataclass(frozen=True)
class GroupState:
    """What a group holds in IAM: its members, its attached managed policies by name with their ARNs and its inline policies."""

    members: frozenset[str]
    attached: Mapping[str, str]
    inline: frozenset[str]


def read_group(iam: Any, group: str) -> GroupState | None:
    """The state of a group, or None when the group does not exist."""
    try:
        members = frozenset(item["UserName"] for item in paged(iam, "get_group", "Users", GroupName=group))
    except ClientError as error:
        if is_missing(error):
            return None
        raise
    attached = {item["PolicyName"]: item["PolicyArn"] for item in paged(iam, "list_attached_group_policies", "AttachedPolicies", GroupName=group)}
    return GroupState(members, attached, frozenset(paged(iam, "list_group_policies", "PolicyNames", GroupName=group)))


def policy_arn(account: str, name: str) -> str:
    return f"arn:aws:iam::{account}:policy/{name}"


def exclusivity_problems(names: Names, account: str, suffix: str, state: GroupState) -> list[str]:
    """What a group holds beyond its intended members, managed policies and inline policies.

    A retired policy on a group counts as extra: the apply attaches the new policies before it retires the old ones, so a retired policy left on a
    group could push it past the ten-policy quota in the middle of a run. This script never attaches a retired policy to a group. Missing members and
    attachments are not problems: they are planned as changes, so a run can complete one that stopped part way.
    """
    group = names.group(suffix)
    intended = {policy_arn(account, names.policy(service)) for service in GROUP_SERVICES[suffix]}
    problems = []
    if state.members - {names.user}:
        problems.append(f"{group} has members besides {names.user}: {', '.join(sorted(state.members - {names.user}))}")
    extra = sorted(arn for arn in state.attached.values() if arn not in intended)
    if extra:
        problems.append(f"{group} has managed policies outside its intended set: {', '.join(extra)}")
    if state.inline:
        problems.append(f"{group} has inline policies: {', '.join(sorted(state.inline))}")
    return problems


def user_other_grants(iam: Any, names: Names) -> list[str]:
    """The grants of the project user that this script does not manage: other managed policies, inline policies and other groups."""
    managed = {names.policy(service) for services in GROUP_SERVICES.values() for service in services} | {names.policy(service) for service in RETIRED_SERVICES}
    direct = sorted({item["PolicyName"] for item in paged(iam, "list_attached_user_policies", "AttachedPolicies", UserName=names.user)} - managed)
    inline = sorted(paged(iam, "list_user_policies", "PolicyNames", UserName=names.user))
    own_groups = {names.group(suffix) for suffix in GROUP_SERVICES}
    other_groups = sorted({item["GroupName"] for item in paged(iam, "list_groups_for_user", "Groups", UserName=names.user)} - own_groups)
    notes = []
    for label, items in (("managed policies", direct), ("inline policies", inline), ("groups", other_groups)):
        if items:
            notes.append(f"the project user {names.user} also holds these {label}: {', '.join(items)}")
    return notes


def plan_steps(iam: Any, names: Names, account: str, documents: Mapping[str, Mapping[str, Any]], full: bool) -> list[Step]:
    """Compare the rendered policies with IAM and list the changes in the order ``apply`` runs them. Read-only.

    It stops before listing any change when a group holds a member, a managed policy or an inline policy beyond the intended ones.
    """
    try:
        iam.get_user(UserName=names.user)
    except ClientError as error:
        if is_missing(error):
            raise RuntimeError(f"the project user {names.user} does not exist in this account") from None
        raise
    states = {suffix: read_group(iam, names.group(suffix)) for suffix in GROUP_SERVICES}
    problems = [problem for suffix, state in states.items() if state is not None for problem in exclusivity_problems(names, account, suffix, state)]
    if problems:
        raise RuntimeError("a group of the project user holds more than intended; nothing was changed:\n  - " + "\n  - ".join(problems))
    existing = local_policies(iam)
    steps: list[Step] = []
    for name in sorted(documents):
        policy = existing.get(name)
        if policy is None:
            steps.append(Step("create_policy", name))
        elif canonical(stored_document(iam, policy)) == canonical(documents[name]):
            steps.append(Step("keep_policy", name))
        else:
            steps.append(Step("update_policy", name))
    wanted: dict[str, list[str]] = {}
    for name in sorted(documents):
        wanted.setdefault(group_of(names, names.service(name)), []).append(name)
    member_of = {item["GroupName"] for item in paged(iam, "list_groups_for_user", "Groups", UserName=names.user)}
    for suffix in GROUP_SERVICES:
        group = names.group(suffix)
        if group not in wanted:
            continue
        state = states[suffix]
        if state is None:
            steps.append(Step("create_group", group))
        attached = set(state.attached) if state else set()
        steps += [Step("attach_policy", name, group) for name in wanted[group] if name not in attached]
        if group not in member_of:
            steps.append(Step("add_user", names.user, group))
    if full:
        direct = {item["PolicyName"] for item in paged(iam, "list_attached_user_policies", "AttachedPolicies", UserName=names.user)}
        steps += [Step("detach_user", name, names.user) for name in sorted(direct & set(documents))]
        retired = {names.policy(service) for service in RETIRED_SERVICES}
        steps += [Step("retire_policy", name) for name in sorted(existing) if name in retired]
    return steps


def remove_oldest_nondefault_version(iam: Any, policy_arn: str) -> None:
    """IAM keeps five versions, so free one before creating the sixth."""
    versions = iam.list_policy_versions(PolicyArn=policy_arn)["Versions"]
    if len(versions) < MAX_POLICY_VERSIONS:
        return
    candidates = sorted((version for version in versions if not version["IsDefaultVersion"]), key=lambda version: version["CreateDate"])
    if not candidates:
        raise RuntimeError("the policy has five versions but no removable nondefault version")
    iam.delete_policy_version(PolicyArn=policy_arn, VersionId=candidates[0]["VersionId"])


def retire(iam: Any, names: Names, policy: Mapping[str, Any]) -> str:
    """Detach a retired policy from the project user and its groups, delete its old versions and the policy. Any other holder stops the delete."""
    arn = policy["Arn"]
    holders = iam.list_entities_for_policy(PolicyArn=arn)
    own_groups = {names.group(suffix) for suffix in GROUP_SERVICES}
    others = [item["UserName"] for item in holders.get("PolicyUsers", []) if item["UserName"] != names.user]
    others += [item["GroupName"] for item in holders.get("PolicyGroups", []) if item["GroupName"] not in own_groups]
    others += [item["RoleName"] for item in holders.get("PolicyRoles", [])]
    if others:
        raise RuntimeError(f"still attached to {', '.join(sorted(others))}; detach it there first")
    for item in holders.get("PolicyUsers", []):
        iam.detach_user_policy(UserName=item["UserName"], PolicyArn=arn)
    for item in holders.get("PolicyGroups", []):
        iam.detach_group_policy(GroupName=item["GroupName"], PolicyArn=arn)
    for version in iam.list_policy_versions(PolicyArn=arn)["Versions"]:
        if not version["IsDefaultVersion"]:
            iam.delete_policy_version(PolicyArn=arn, VersionId=version["VersionId"])
    iam.delete_policy(PolicyArn=arn)
    return "detached and deleted"


def apply_step(iam: Any, names: Names, account: str, documents: Mapping[str, Mapping[str, Any]], step: Step, existing: Mapping[str, Mapping[str, Any]]) -> str:
    """Run one step; every step is safe to run again."""
    arn = policy_arn(account, step.name)
    if step.kind == "create_policy":
        iam.create_policy(
            PolicyName=step.name,
            PolicyDocument=json.dumps(documents[step.name], separators=(",", ":")),
            Description=f"Project profile permissions: {names.service(step.name)}",
        )
    elif step.kind == "update_policy":
        remove_oldest_nondefault_version(iam, arn)
        iam.create_policy_version(PolicyArn=arn, PolicyDocument=json.dumps(documents[step.name], separators=(",", ":")), SetAsDefault=True)
    elif step.kind == "create_group":
        iam.create_group(GroupName=step.name)
    elif step.kind == "attach_policy":
        iam.attach_group_policy(GroupName=step.target, PolicyArn=arn)
    elif step.kind == "add_user":
        iam.add_user_to_group(GroupName=step.target, UserName=step.name)
    elif step.kind == "detach_user":
        iam.detach_user_policy(UserName=step.target, PolicyArn=arn)
    elif step.kind == "retire_policy":
        return retire(iam, names, existing[step.name])
    else:
        raise ValueError(f"unsupported step: {step.kind}")
    return "completed"


def apply_steps(iam: Any, names: Names, account: str, documents: Mapping[str, Mapping[str, Any]], steps: list[Step]) -> list[StepResult]:
    """Run the steps in order; a failed step is recorded and the others still run."""
    existing = local_policies(iam)
    results = []
    for step in steps:
        if step.kind == "keep_policy":
            results.append(StepResult(step, "unchanged", "the policy matches the template"))
            continue
        try:
            results.append(StepResult(step, "applied", apply_step(iam, names, account, documents, step, existing)))
        except (ClientError, RuntimeError, ValueError) as error:
            results.append(StepResult(step, "failed", f"{type(error).__name__}: {error}"))
    return results


def connect(session_factory: SessionFactory, admin_profile: str, region: str, account: str, names: Names) -> tuple[Any, Any]:
    """The administrator's IAM client, after checking the profile is in the configured account and is not the project user."""
    session = session_factory(profile_name=admin_profile, region_name=region)
    identity = session.client("sts").get_caller_identity()
    if identity["Account"] != account:
        raise RuntimeError("the administrator profile is in another account than .env names; no policy was read or changed")
    if identity["Arn"].endswith(f":user/{names.user}"):
        raise RuntimeError("the administrator profile authenticates as the project user; use the administrator profile")
    return session.client("iam"), identity


def run(arguments: argparse.Namespace, environment: Mapping[str, str], session_factory: SessionFactory = boto3.Session) -> int:
    """Plan or apply. The confirmation word is checked before any AWS client exists."""
    storage, _ = load_configuration(arguments.env_file)
    if arguments.admin_profile == storage["aws_profile"] or not arguments.admin_profile.strip():
        raise ConfigurationError("Pass an administrator profile that is not the project profile.")
    if arguments.action == "apply" and environment.get(CONFIRMATION_VARIABLE) != CONFIRMATION:
        raise ConfigurationError(f"Set {CONFIRMATION_VARIABLE}={CONFIRMATION} to apply; nothing was read or changed.")
    names = Names(storage["project_name"], storage["environment"])
    selected = set(arguments.policies) if arguments.policies else None
    rendered = load_documents(placeholder_values(storage), selected)
    documents = {names.policy(service): document for service, document in rendered.items()}
    for service in rendered:
        group_of(names, service)
    iam, _ = connect(session_factory, arguments.admin_profile, storage["aws_region"], storage["expected_account_id"], names)
    steps = plan_steps(iam, names, storage["expected_account_id"], documents, full=selected is None)
    for note in user_other_grants(iam, names):
        sys.stdout.write(f"NOTE {note}\n")
    for step in steps:
        sys.stdout.write(step.text() + "\n")
    changes = [step for step in steps if step.kind != "keep_policy"]
    sys.stdout.write(f"Summary: {len(changes)} change(s), {len(steps) - len(changes)} unchanged.\n")
    if arguments.action == "plan":
        return 0
    results = apply_steps(iam, names, storage["expected_account_id"], documents, steps)
    for result in results:
        sys.stdout.write(f"{result.status.upper():9} {result.step.text()}: {result.detail}\n")
    failures = [result for result in results if result.status == "failed"]
    if failures:
        sys.stderr.write(f"{len(failures)} step(s) failed; run apply again after fixing the cause.\n")
        return 1
    sys.stdout.write(f"Applied {sum(result.status == 'applied' for result in results)} step(s).\n")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "apply"))
    parser.add_argument("--admin-profile", required=True, help="The administrator AWS profile; it is never read from .env.")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--policy", action="append", dest="policies", help="Limit the run to one service, for example sns. Repeat as needed.")
    arguments = parser.parse_args()
    try:
        raise SystemExit(run(arguments, os.environ))
    except (ConfigurationError, PolicyRenderError, RuntimeError, ClientError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
