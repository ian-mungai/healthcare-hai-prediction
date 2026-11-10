"""Plan, apply or export the per-service IAM policies of the project profile with the administrator profile.

``plan`` is the dry run: it renders the templates, reads IAM and prints what ``apply`` would do and one digest of it, and changes nothing. The digest
covers the steps, the rendered documents and the live policy versions they were compared with. ``apply`` needs
``CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies`` and ``--digest`` with the digest the reviewed plan printed, and checks both before any AWS
call. It then plans again and changes nothing unless the new digest matches, so it applies only what the owner reviewed. Each live policy in the digest
carries its whole version inventory, and each step names the versions it deletes: IAM keeps five versions, so an update first deletes the oldest
version that is not the default, and the plan names that version. Before the first write, apply reads the live state again and stops if it differs
from the plan. A retired policy's holders are in the digest too, and a holder other than the project user and its groups refuses the plan before any
step, so a run never writes the new policies and then stops at a retirement.

Policies are customer-managed, one per service. They attach to two IAM groups (the managed policy quota per user is 10), and the project user joins
both. The retired grouped policies are detached and deleted.

``export`` is read-only: it writes the live policies as templates with placeholders, so drift from the tracked templates shows in a comparison. It
replaces the account, region, bucket, project and environment values with ``${AWS_ACCOUNT_ID}``, ``${AWS_REGION}``, ``${DATA_BUCKET_NAME}``,
``${PROJECT_NAME}`` and ``${ENVIRONMENT}``. Export to a scratch directory and compare.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from infra.iam.scripts.render_policy import POLICIES_DIRECTORY, TEMPLATE_PREFIX, PolicyRenderError, load_documents, placeholder_values
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration

CONFIRMATION = "apply-healthcare-hai-prediction-policies"
CONFIRMATION_VARIABLE = "CONFIRM_IAM_POLICIES"
DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
IAM_SCALAR_OR_LIST_FIELDS = {"Action", "NotAction", "Resource", "NotResource", "Principal", "NotPrincipal"}
MAX_POLICY_VERSIONS = 5
# The services of each group. Each group holds at most 10 policies. A policy file whose service is in no group stops the run.
GROUP_SERVICES = {
    "data": ("iam", "s3", "secretsmanager", "ssm", "ssmcap", "sts"),
    "platform": ("autoscaling", "cloudwatch", "dlm", "ec2", "ecr", "ecs", "logs", "sns"),
}
MAX_GROUP_POLICIES = 10
# Earlier grouped policies, replaced by the per-service files. Their old Terraform resources are removed from state without a destroy.
RETIRED_SERVICES = ("lakehouse", "platform_network", "platform_services", "platform_tags", "secrets")
SessionFactory = Callable[..., Any]


@dataclass(frozen=True)
class Step:
    """One change. ``kind`` is create_policy, update_policy, keep_policy, create_group, attach_policy, add_user, detach_user or retire_policy.

    ``removes`` names the policy versions the step deletes, chosen by the plan; apply deletes exactly these.
    """

    kind: str
    name: str
    target: str = ""
    removes: tuple[str, ...] = ()

    def text(self) -> str:
        label = self.kind.replace("_", " ").upper()
        text = f"{label:16} {self.name}" + (f" -> {self.target}" if self.target else "")
        if self.removes:
            text += f" (deletes version{'s' if len(self.removes) > 1 else ''} {', '.join(self.removes)})"
        return text


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


# The live state a plan compared with, by policy name, or None when the policy does not exist: the default version, the version inventory and, for
# a rendered policy, the SHA-256 of its stored document or, for a retired policy, its ARN and holders.
Compared = dict[str, dict[str, Any] | None]


def version_inventory(iam: Any, arn: str) -> list[dict[str, Any]]:
    """Every version of a policy, oldest first."""
    versions: list[dict[str, Any]] = iam.list_policy_versions(PolicyArn=arn)["Versions"]
    return sorted(versions, key=lambda version: (version["CreateDate"], version["VersionId"]))


def live_state(iam: Any, names: Names, documents: Mapping[str, Mapping[str, Any]], existing: Mapping[str, Mapping[str, Any]], full: bool) -> Compared:
    """The live state of every policy a plan covers: the rendered policies and, in a full run, the retired policies that exist. Read-only."""
    state: Compared = {}
    for name in sorted(documents):
        policy = existing.get(name)
        if policy is None:
            state[name] = None
            continue
        stored = canonical(stored_document(iam, policy))
        state[name] = {
            "version": policy["DefaultVersionId"],
            "sha256": hashlib.sha256(stored.encode()).hexdigest(),
            "versions": [version["VersionId"] for version in version_inventory(iam, policy["Arn"])],
        }
    if full:
        retired = {names.policy(service) for service in RETIRED_SERVICES}
        for name in sorted(name for name in existing if name in retired):
            arn = existing[name]["Arn"]
            state[name] = {
                "arn": arn,
                "version": existing[name]["DefaultVersionId"],
                "versions": [version["VersionId"] for version in version_inventory(iam, arn)],
                "holders": policy_holders(iam, arn),
            }
    return state


def policy_holders(iam: Any, arn: str) -> dict[str, list[str]]:
    """The users, groups and roles a policy is attached to, each sorted. Read-only."""
    holders = iam.list_entities_for_policy(PolicyArn=arn)
    return {
        "users": sorted(item["UserName"] for item in holders.get("PolicyUsers", [])),
        "groups": sorted(item["GroupName"] for item in holders.get("PolicyGroups", [])),
        "roles": sorted(item["RoleName"] for item in holders.get("PolicyRoles", [])),
    }


def other_holders(names: Names, holders: Mapping[str, list[str]]) -> list[str]:
    """The holders a retirement must not detach: users besides the project user, groups besides its own and every role."""
    own_groups = {names.group(suffix) for suffix in GROUP_SERVICES}
    others = [user for user in holders["users"] if user != names.user] + [group for group in holders["groups"] if group not in own_groups]
    return sorted(others + holders["roles"])


def pruned_version(iam: Any, policy: Mapping[str, Any]) -> tuple[str, ...]:
    """The version an update deletes first: IAM keeps five, so with five the oldest version that is not the default goes. Read-only."""
    versions = version_inventory(iam, policy["Arn"])
    if len(versions) < MAX_POLICY_VERSIONS:
        return ()
    candidates = [version["VersionId"] for version in versions if not version["IsDefaultVersion"]]
    if not candidates:
        raise RuntimeError(f"{policy['PolicyName']} has five versions but no version that is not the default; nothing was changed")
    return (candidates[0],)


def plan_steps(iam: Any, names: Names, account: str, documents: Mapping[str, Mapping[str, Any]], full: bool) -> tuple[list[Step], Compared]:
    """Compare the rendered policies with IAM and list the changes in the order ``apply`` runs them, with the live state compared. Read-only.

    It stops before listing any change when a group holds a member, a managed policy or an inline policy beyond the intended ones, when an update
    has no version it can delete, or when a retired policy has a holder besides the project user and its groups.
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
    compared = live_state(iam, names, documents, existing, full)
    held = []
    for name, live in compared.items():
        others = other_holders(names, live["holders"]) if live and "holders" in live else []
        if others:
            held.append(f"{name} is still attached to {', '.join(others)}")
    if held:
        raise RuntimeError("a retired policy has other holders; detach it there first. Nothing was changed:\n  - " + "\n  - ".join(held))
    steps: list[Step] = []
    for name in sorted(documents):
        policy, live = existing.get(name), compared[name]
        if policy is None or live is None:
            steps.append(Step("create_policy", name))
        elif live["sha256"] == hashlib.sha256(canonical(documents[name]).encode()).hexdigest():
            steps.append(Step("keep_policy", name))
        else:
            steps.append(Step("update_policy", name, removes=pruned_version(iam, policy)))
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
        for name in sorted(name for name in existing if name in retired):
            versions = version_inventory(iam, existing[name]["Arn"])
            steps.append(Step("retire_policy", name, removes=tuple(version["VersionId"] for version in versions if not version["IsDefaultVersion"])))
    return steps, compared


def sent_document(document: Mapping[str, Any]) -> str:
    """The exact text ``apply`` sends to IAM for a rendered document."""
    return json.dumps(document, separators=(",", ":"))


def plan_digest(steps: list[Step], documents: Mapping[str, Mapping[str, Any]], compared: Compared) -> str:
    """The SHA-256 of what a plan shows: its steps with the versions they delete, the exact rendered documents and the live state compared."""
    payload = {
        "steps": [[step.kind, step.name, step.target, list(step.removes)] for step in steps],
        "documents": {name: sent_document(documents[name]) for name in sorted(documents)},
        "live": compared,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def check_unchanged(iam: Any, names: Names, documents: Mapping[str, Mapping[str, Any]], compared: Compared, full: bool) -> None:
    """Stop before the first write when the live state of any planned policy differs from the plan. Read-only."""
    current = live_state(iam, names, documents, local_policies(iam), full)
    changed = sorted(name for name in {*current, *compared} if current.get(name) != compared.get(name))
    if changed:
        raise RuntimeError(f"these policies changed after the plan was read: {', '.join(changed)}; nothing was changed. Run plan again, review it and apply.")


def retire(iam: Any, arn: str, holders: Mapping[str, list[str]], step: Step) -> str:
    """Detach a retired policy from the planned holders, delete the planned versions and the policy. The plan refused any other holder."""
    for user in holders["users"]:
        iam.detach_user_policy(UserName=user, PolicyArn=arn)
    for group in holders["groups"]:
        iam.detach_group_policy(GroupName=group, PolicyArn=arn)
    for version in step.removes:
        iam.delete_policy_version(PolicyArn=arn, VersionId=version)
    iam.delete_policy(PolicyArn=arn)
    return "detached and deleted"


def apply_step(iam: Any, names: Names, account: str, documents: Mapping[str, Mapping[str, Any]], step: Step, compared: Compared) -> str:
    """Run one step; every step is safe to run again."""
    arn = policy_arn(account, step.name)
    if step.kind == "create_policy":
        iam.create_policy(
            PolicyName=step.name,
            PolicyDocument=sent_document(documents[step.name]),
            Description=f"Project profile permissions: {names.service(step.name)}",
        )
    elif step.kind == "update_policy":
        for version in step.removes:
            iam.delete_policy_version(PolicyArn=arn, VersionId=version)
        iam.create_policy_version(PolicyArn=arn, PolicyDocument=sent_document(documents[step.name]), SetAsDefault=True)
    elif step.kind == "create_group":
        iam.create_group(GroupName=step.name)
    elif step.kind == "attach_policy":
        iam.attach_group_policy(GroupName=step.target, PolicyArn=arn)
    elif step.kind == "add_user":
        iam.add_user_to_group(GroupName=step.target, UserName=step.name)
    elif step.kind == "detach_user":
        iam.detach_user_policy(UserName=step.target, PolicyArn=arn)
    elif step.kind == "retire_policy":
        live = compared[step.name]
        if live is None:
            raise ValueError(f"the plan holds no live state for {step.name}")
        return retire(iam, live["arn"], live["holders"], step)
    else:
        raise ValueError(f"unsupported step: {step.kind}")
    return "completed"


def apply_steps(
    iam: Any, names: Names, account: str, documents: Mapping[str, Mapping[str, Any]], steps: list[Step], compared: Compared, full: bool
) -> list[StepResult]:
    """Check that the live state still matches the plan, then run the steps in order; a failed step is recorded and the others still run."""
    check_unchanged(iam, names, documents, compared, full)
    results = []
    for step in steps:
        if step.kind == "keep_policy":
            results.append(StepResult(step, "unchanged", "the policy matches the template"))
            continue
        try:
            results.append(StepResult(step, "applied", apply_step(iam, names, account, documents, step, compared)))
        except (ClientError, RuntimeError, ValueError) as error:
            results.append(StepResult(step, "failed", f"{type(error).__name__}: {error}"))
    return results


def sanitize_string(value: str, values: Mapping[str, str]) -> str:
    """Replace the deployment values in one string with their placeholders, longest and most specific first."""
    for name in ("DATA_BUCKET_NAME", "AWS_ACCOUNT_ID", "AWS_REGION", "PROJECT_NAME"):
        value = value.replace(values[name], f"${{{name}}}")
    return re.sub(rf"(?<=[_}}])({re.escape(values['ENVIRONMENT'])})(?![A-Za-z0-9])", "${ENVIRONMENT}", value)


def sanitize(value: Any, values: Mapping[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: sanitize(item, values) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize(item, values) for item in value]
    if isinstance(value, str):
        return sanitize_string(value, values)
    return value


def export_templates(iam: Any, names: Names, values: Mapping[str, str], output: Path) -> list[Path]:
    """Write one template per service from the live default versions; a missing policy stops the export. Read-only against IAM."""
    existing = local_policies(iam)
    written = []
    for service in sorted(service for services in GROUP_SERVICES.values() for service in services):
        name = names.policy(service)
        if name not in existing:
            raise RuntimeError(f"IAM holds no customer-managed policy named {name}")
        target = output / f"{TEMPLATE_PREFIX}{service}_policy.json"
        target.write_text(json.dumps(sanitize(stored_document(iam, existing[name]), values), indent=2) + "\n", encoding="utf-8")
        written.append(target)
    return written


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
    """Plan, apply or export. The confirmation word and the form of the digest are checked before any AWS client exists."""
    storage, _ = load_configuration(arguments.env_file)
    if arguments.admin_profile == storage["aws_profile"] or not arguments.admin_profile.strip():
        raise ConfigurationError("Pass an administrator profile that is not the project profile.")
    if arguments.action == "apply" and environment.get(CONFIRMATION_VARIABLE) != CONFIRMATION:
        raise ConfigurationError(f"Set {CONFIRMATION_VARIABLE}={CONFIRMATION} to apply; nothing was read or changed.")
    if arguments.action == "apply" and not DIGEST_PATTERN.fullmatch(arguments.digest or ""):
        raise ConfigurationError("Pass --digest with the 64-character digest that the reviewed plan printed; nothing was read or changed.")
    names = Names(storage["project_name"], storage["environment"])
    if arguments.action == "export":
        iam, _ = connect(session_factory, arguments.admin_profile, storage["aws_region"], storage["expected_account_id"], names)
        arguments.output_directory.mkdir(parents=True, exist_ok=True)
        for path in export_templates(iam, names, placeholder_values(storage), arguments.output_directory):
            sys.stdout.write(f"Exported {path.name}\n")
        return 0
    selected = set(arguments.policies) if arguments.policies else None
    rendered = load_documents(placeholder_values(storage), selected)
    documents = {names.policy(service): document for service, document in rendered.items()}
    for service in rendered:
        group_of(names, service)
    iam, _ = connect(session_factory, arguments.admin_profile, storage["aws_region"], storage["expected_account_id"], names)
    steps, compared = plan_steps(iam, names, storage["expected_account_id"], documents, full=selected is None)
    for note in user_other_grants(iam, names):
        sys.stdout.write(f"NOTE {note}\n")
    for step in steps:
        sys.stdout.write(step.text() + "\n")
    changes = [step for step in steps if step.kind != "keep_policy"]
    sys.stdout.write(f"Summary: {len(changes)} change(s), {len(steps) - len(changes)} unchanged.\n")
    digest = plan_digest(steps, documents, compared)
    sys.stdout.write(f"Plan digest: {digest}\n")
    if arguments.action == "plan":
        return 0
    if digest != arguments.digest:
        raise RuntimeError(
            f"the reviewed digest {arguments.digest} does not match this plan's digest {digest}: the templates or the live policies changed since the "
            "reviewed plan, and nothing was changed. Run plan again, review it and apply with its digest."
        )
    results = apply_steps(iam, names, storage["expected_account_id"], documents, steps, compared, full=selected is None)
    for result in results:
        sys.stdout.write(f"{result.status.upper():9} {result.step.text()}: {result.detail}\n")
    failures = [result for result in results if result.status == "failed"]
    if failures:
        sys.stderr.write(f"{len(failures)} step(s) failed; run apply again after fixing the cause.\n")
        return 1
    sys.stdout.write(f"Applied {sum(result.status == 'applied' for result in results)} step(s).\n")
    return 0


def main() -> None:
    profile = argparse.ArgumentParser(add_help=False)
    profile.add_argument("--admin-profile", required=True, help="The administrator AWS profile; it is never read from .env.")
    profile.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    common = argparse.ArgumentParser(add_help=False, parents=[profile])
    common.add_argument("--policy", action="append", dest="policies", help="Limit the run to one service, for example sns. Repeat as needed.")
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("plan", parents=[common], help="Print the steps and their digest; changes nothing.").set_defaults(digest=None)
    apply = commands.add_parser("apply", parents=[common], help="Plan again and run the steps if the digest matches the reviewed plan.")
    apply.add_argument("--digest", required=True, help="The digest that the reviewed plan printed; apply changes nothing unless the new plan has it.")
    export = commands.add_parser("export", parents=[profile], help="Write the live policies as templates with placeholders; read-only.")
    export.add_argument("--output-directory", type=Path, default=POLICIES_DIRECTORY, help="Where to write the templates; use a scratch directory to compare.")
    export.set_defaults(policies=None, digest=None)
    arguments = parser.parse_args()
    try:
        raise SystemExit(run(arguments, os.environ))
    except (ConfigurationError, PolicyRenderError, RuntimeError, ClientError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
