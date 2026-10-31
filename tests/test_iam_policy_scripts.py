"""The owner's IAM policy scripts, run against an in-memory IAM with synthetic values: no AWS call is possible."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from infra.iam.scripts import export_policies, manage_policies, render_policy
from scripts.infrastructure.render_project_config import ConfigurationError
from scripts.process import run_command
from tests.support import check

REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT = "example_project"
ACCOUNT = "111111111111"
USER = f"{PROJECT}_dev"
ENV = {
    "AWS_ACCOUNT_ID": ACCOUNT,
    "AWS_PROFILE": USER,
    "AWS_REGION": "eu-west-1",
    "ENVIRONMENT": "dev",
    "PROJECT_NAME": PROJECT,
    "S3_BUCKET": "example-project-ci-bucket",
}
TEMPLATE_SERVICES = sorted(render_policy.template_service(path.stem) for path in (REPO_ROOT / "infra/iam/policies").glob("*_policy.json"))


def no_such_entity(operation: str) -> ClientError:
    return ClientError({"Error": {"Code": "NoSuchEntity", "Message": "x"}}, operation)


class Paginator:
    def __init__(self, fetch: Callable[..., list[dict[str, Any]]], key: str) -> None:
        self.fetch, self.key = fetch, key

    def paginate(self, **arguments: Any) -> list[dict[str, Any]]:
        items = self.fetch(**arguments)
        return [{self.key: items[:2]}, {self.key: items[2:]}]


class FakeIam:
    """A small in-memory IAM: policies with versions, groups with attachments and one user."""

    def __init__(self, user_exists: bool = True) -> None:
        self.user_exists = user_exists
        self.policies: dict[str, dict[str, Any]] = {}
        self.groups: dict[str, set[str]] = {}
        self.members: dict[str, set[str]] = {USER: set()}
        self.group_users: dict[str, set[str]] = {}
        self.group_inline: dict[str, set[str]] = {}
        self.aws_managed: set[str] = set()
        self.user_policies: dict[str, set[str]] = {USER: set()}
        self.user_inline: dict[str, set[str]] = {USER: set()}
        self.role_policies: dict[str, set[str]] = {}
        self.calls: list[str] = []
        self.fail_on: set[str] = set()

    def arn(self, name: str) -> str:
        return f"arn:aws:iam::{ACCOUNT}:policy/{name}"

    def name_of(self, arn: str) -> str:
        return arn.rsplit("/", 1)[-1]

    def add_policy(self, name: str, document: dict[str, Any], versions: int = 1) -> None:
        now = datetime(2026, 1, 1, tzinfo=UTC)
        self.policies[name] = {
            "versions": [
                {"VersionId": f"v{index + 1}", "IsDefaultVersion": index == versions - 1, "CreateDate": now + timedelta(days=index)}
                for index in range(versions)
            ]
        }
        self.policies[name]["document"] = document

    def get_paginator(self, name: str) -> Paginator:
        listing: dict[str, tuple[str, Callable[..., list[Any]]]] = {
            "list_policies": ("Policies", self.local_policies),
            "get_group": ("Users", self.users_of_group),
            "list_attached_group_policies": ("AttachedPolicies", lambda GroupName: [self.attachment(n) for n in sorted(self.groups[GroupName])]),
            "list_group_policies": ("PolicyNames", lambda GroupName: sorted(self.group_inline.get(GroupName, set()))),
            "list_groups_for_user": ("Groups", lambda UserName: [{"GroupName": g} for g in sorted(self.members[UserName])]),
            "list_attached_user_policies": ("AttachedPolicies", lambda UserName: [self.attachment(n) for n in sorted(self.user_policies[UserName])]),
            "list_user_policies": ("PolicyNames", lambda UserName: sorted(self.user_inline[UserName])),
        }
        key, fetch = listing[name]
        return Paginator(fetch, key)

    def attachment(self, name: str) -> dict[str, str]:
        return {"PolicyName": name, "PolicyArn": f"arn:aws:iam::aws:policy/{name}" if name in self.aws_managed else self.arn(name)}

    def users_of_group(self, GroupName: str) -> list[dict[str, str]]:
        if GroupName not in self.groups:
            raise no_such_entity("GetGroup")
        return [{"UserName": user} for user in sorted(self.group_users.get(GroupName, set()))]

    def local_policies(self, Scope: str) -> list[dict[str, Any]]:
        return [{"PolicyName": n, "Arn": self.arn(n), "DefaultVersionId": p["versions"][-1]["VersionId"]} for n, p in sorted(self.policies.items())]

    def get_user(self, **_: Any) -> dict[str, Any]:
        if not self.user_exists:
            raise no_such_entity("GetUser")
        return {}

    def get_policy_version(self, PolicyArn: str, VersionId: str) -> dict[str, Any]:
        return {"PolicyVersion": {"Document": self.policies[self.name_of(PolicyArn)]["document"]}}

    def list_policy_versions(self, PolicyArn: str) -> dict[str, Any]:
        return {"Versions": list(self.policies[self.name_of(PolicyArn)]["versions"])}

    def list_entities_for_policy(self, PolicyArn: str) -> dict[str, Any]:
        name = self.name_of(PolicyArn)
        return {
            "PolicyUsers": [{"UserName": USER} for held in [self.user_policies[USER]] if name in held],
            "PolicyGroups": [{"GroupName": g} for g, held in sorted(self.groups.items()) if name in held],
            "PolicyRoles": [{"RoleName": r} for r, held in sorted(self.role_policies.items()) if name in held],
        }

    def record(self, operation: str, name: str) -> None:
        self.calls.append(f"{operation} {name}")
        if f"{operation} {name}" in self.fail_on:
            raise ClientError({"Error": {"Code": "LimitExceeded", "Message": "simulated"}}, operation)

    def create_policy(self, PolicyName: str, PolicyDocument: str, **_: Any) -> None:
        self.record("create_policy", PolicyName)
        self.add_policy(PolicyName, json.loads(PolicyDocument))

    def create_policy_version(self, PolicyArn: str, PolicyDocument: str, **_: Any) -> None:
        name = self.name_of(PolicyArn)
        self.record("create_policy_version", name)
        versions = self.policies[name]["versions"]
        if len(versions) >= manage_policies.MAX_POLICY_VERSIONS:
            raise ClientError({"Error": {"Code": "LimitExceeded", "Message": "five versions"}}, "CreatePolicyVersion")
        for version in versions:
            version["IsDefaultVersion"] = False
        versions.append({"VersionId": f"v{len(versions) + 1}", "IsDefaultVersion": True, "CreateDate": datetime(2026, 2, 1, tzinfo=UTC)})
        self.policies[name]["document"] = json.loads(PolicyDocument)

    def delete_policy_version(self, PolicyArn: str, VersionId: str) -> None:
        name = self.name_of(PolicyArn)
        self.record("delete_policy_version", f"{name} {VersionId}")
        self.policies[name]["versions"] = [v for v in self.policies[name]["versions"] if v["VersionId"] != VersionId]

    def delete_policy(self, PolicyArn: str) -> None:
        self.record("delete_policy", self.name_of(PolicyArn))
        del self.policies[self.name_of(PolicyArn)]

    def create_group(self, GroupName: str) -> None:
        self.record("create_group", GroupName)
        self.groups[GroupName] = set()

    def attach_group_policy(self, GroupName: str, PolicyArn: str) -> None:
        self.record("attach_group_policy", f"{GroupName} {self.name_of(PolicyArn)}")
        if len(self.groups[GroupName]) >= manage_policies.MAX_GROUP_POLICIES:
            raise ClientError({"Error": {"Code": "LimitExceeded", "Message": "ten policies"}}, "AttachGroupPolicy")
        self.groups[GroupName].add(self.name_of(PolicyArn))

    def detach_group_policy(self, GroupName: str, PolicyArn: str) -> None:
        self.record("detach_group_policy", f"{GroupName} {self.name_of(PolicyArn)}")
        self.groups[GroupName].discard(self.name_of(PolicyArn))

    def detach_user_policy(self, UserName: str, PolicyArn: str) -> None:
        self.record("detach_user_policy", f"{UserName} {self.name_of(PolicyArn)}")
        self.user_policies[UserName].discard(self.name_of(PolicyArn))

    def add_user_to_group(self, GroupName: str, UserName: str) -> None:
        self.record("add_user_to_group", GroupName)
        self.members[UserName].add(GroupName)
        self.group_users.setdefault(GroupName, set()).add(UserName)

    def writes(self) -> list[str]:
        return list(self.calls)


class FakeSession:
    def __init__(self, iam: FakeIam, account: str = ACCOUNT, arn: str = "arn:aws:iam::111111111111:user/example_administrator") -> None:
        self.iam, self.account, self.arn = iam, account, arn
        self.profiles: list[str] = []

    def __call__(self, profile_name: str, **_: Any) -> FakeSession:
        self.profiles.append(profile_name)
        return self

    def client(self, service: str) -> Any:
        if service == "iam":
            return self.iam
        return type("Sts", (), {"get_caller_identity": lambda _self: {"Account": self.account, "Arn": self.arn}})()


def forbidden_session(**_: Any) -> Any:
    raise AssertionError("an AWS session was created")


@pytest.fixture
def env_file(tmp_path: Path) -> Path:
    path = tmp_path / ".env"
    path.write_text("".join(f"{key}={value}\n" for key, value in ENV.items()), encoding="utf-8")
    return path


def arguments(env_file: Path, action: str, admin: str = "example_administrator", policies: list[str] | None = None) -> argparse.Namespace:
    return argparse.Namespace(action=action, admin_profile=admin, env_file=env_file, policies=policies)


CONFIRMED = {manage_policies.CONFIRMATION_VARIABLE: manage_policies.CONFIRMATION}


def test_renderer_fills_every_placeholder_and_keeps_policy_variables() -> None:
    values = render_policy.placeholder_values(
        {"aws_region": "eu-west-1", "expected_account_id": ACCOUNT, "project_name": PROJECT, "environment": "dev", "data_bucket_name": "example-data"}
    )
    documents = render_policy.load_documents(values)
    text = json.dumps(documents)
    check("${" not in text.replace("${aws:username}", ""), "an unresolved placeholder remains")
    check("${aws:username}" in text, "the session policy variable was replaced")
    check(
        "arn:aws:s3:::example-data/lakehouse/*" in text and "role/healthcare_hai_prediction_lakehouse_reader" in text and f"{PROJECT}_lakehouse" not in text,
        "the bucket or the project name was not substituted",
    )
    check(len(documents) == 13, "the renderer must produce the 13 approved policies")


def statement_resources(documents: dict[str, dict[str, Any]], service: str, sid: str) -> list[str]:
    return [resource for statement in documents[service]["Statement"] if statement["Sid"] == sid for resource in statement["Resource"]]


def test_the_names_come_from_the_terraform_variable_defaults_and_nowhere_else() -> None:
    values = {"AWS_REGION": "eu-west-1", "AWS_ACCOUNT_ID": ACCOUNT, "PROJECT_NAME": PROJECT, "ENVIRONMENT": "dev", "DATA_BUCKET_NAME": "example-data"}
    prefix = f"arn:aws:ssm:eu-west-1:{ACCOUNT}:parameter"
    documents = render_policy.load_documents(values)
    declared = render_policy.terraform_names()
    check(
        statement_resources(documents, "ssm", "ManageProjectParameters") == [prefix + name for name in declared["MANAGED_PARAMETER_NAME"]],
        "the read and write set must be the managed variable defaults, each name once",
    )
    check(
        statement_resources(documents, "ssm", "ReadAirflowAdminParameter") == [prefix + declared["AIRFLOW_ADMIN_SECRET_NAME"]],
        "the read only set must be the Airflow administrator password default",
    )
    check(
        {*statement_resources(documents, "ssm", "ManageProjectParameters"), *statement_resources(documents, "ssm", "ReadAirflowAdminParameter")}
        == {prefix + name for name in declared["READ_PARAMETER_NAME"]},
        "the two statements together must name exactly the parameters that are read",
    )
    check(len(declared["READ_PARAMETER_NAME"]) == 18 and len(declared["MANAGED_PARAMETER_NAME"]) == 17, "18 parameters are read and 17 are written")
    check(
        "/healthcare_hai_prediction/airflow_admin" in declared["READ_PARAMETER_NAME"]
        and "/healthcare_hai_prediction/airflow_admin" not in declared["MANAGED_PARAMETER_NAME"],
        "the Airflow administrator password is read only",
    )
    check(len(declared["SHARED_SECRET_NAME"]) == 4, "four shared API keys")
    roles = [f"arn:aws:iam::{ACCOUNT}:role/{declared[key]}" for key in ("LAKEHOUSE_READER_ROLE_NAME", "LAKEHOUSE_WRITER_ROLE_NAME")]
    check(statement_resources(documents, "sts", "AssumeLakehouseRolesOnly") == roles, "the sts roles must be the role-name variable defaults, not PROJECT_NAME")
    owner = render_policy.render_template((REPO_ROOT / "infra/iam/owner_policies/task_execution.json").read_text(encoding="utf-8"), values)
    monitoring = render_policy.render_template((REPO_ROOT / "infra/iam/owner_policies/task_execution_monitoring.json").read_text(encoding="utf-8"), values)
    injected_names = [item for item in owner["Statement"] if item["Sid"] == "InjectNamedParameters"][0]["Resource"]
    check(len(injected_names) == 8 and not any("grafana" in item for item in injected_names), "the base task execution policy names no Grafana parameter")
    injected_names = [*injected_names, *monitoring["Statement"][0]["Resource"]]
    check(
        len(injected_names) == 9 and all(item.removeprefix(prefix) in declared["MANAGED_PARAMETER_NAME"] for item in injected_names),
        "the task execution parameters must be managed parameters named by their variables, not PROJECT_NAME",
    )
    injected = {
        **declared,
        "SHARED_SECRET_NAME": ["only_key"],
        "READ_PARAMETER_NAME": ["/only_folder/read"],
        "MANAGED_PARAMETER_NAME": ["/only_folder/write"],
    }
    with pytest.raises(render_policy.PolicyRenderError):
        render_policy.load_documents(values, names={**injected, "LAKEHOUSE_READER_ROLE_NAME": ["a", "b"]})
    documents = render_policy.load_documents(values, names={**injected, "LAKEHOUSE_READER_ROLE_NAME": "only_role"})
    check(
        f"arn:aws:iam::{ACCOUNT}:role/only_role" in statement_resources(documents, "sts", "AssumeLakehouseRolesOnly"),
        "an injected single name must replace the role default",
    )
    check(
        statement_resources(documents, "ssm", "ManageProjectParameters") == [prefix + "/only_folder/write"],
        "an injected managed name must replace the defaults",
    )
    check(
        statement_resources(documents, "secretsmanager", "ReadSharedApiKeys") == [f"arn:aws:secretsmanager:eu-west-1:{ACCOUNT}:secret:only_key-??????"],
        "an injected shared key must replace the defaults",
    )


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ('{"Version": "2012-10-17", "Statement": [{"Resource": "${AWS_REGION}"}]}', "missing values for: AWS_REGION"),
        ('{"Version": "2012-10-17", "Statement": [{"Resource": "${EXAMPLE_OTHER}"}]}', "unknown placeholders: EXAMPLE_OTHER"),
        ('["not", "a", "policy"]', "must be a JSON object with a Statement list"),
        ('{"Version": "2012-10-17", "Statement": [{"Resource": "${READ_PARAMETER_NAME}"}]}', "a name placeholder must be an element of a list"),
        ('{"Version": "2012-10-17", "Statement": [{"Resource": ["${READ_PARAMETER_NAME}${SHARED_SECRET_NAME}"]}]}', "more than one name placeholder"),
        ('{"Version": "2012-10-17", "Statement": ["' + "x" * 6200 + '"]}', "IAM allows 6144"),
    ],
)
def test_renderer_refuses_unresolved_unknown_malformed_and_oversized_templates(text: str, message: str) -> None:
    with pytest.raises(render_policy.PolicyRenderError) as error:
        render_policy.render_template(text, {"AWS_ACCOUNT_ID": ACCOUNT})
    check(message in str(error.value), f"the refusal must cite its cause: {error.value}")


def test_every_shipped_template_belongs_to_one_group_of_at_most_ten() -> None:
    services = TEMPLATE_SERVICES
    grouped = [service for members in manage_policies.GROUP_SERVICES.values() for service in members]
    check(sorted(grouped) == sorted(services), f"templates and groups differ: {sorted(set(grouped) ^ set(services))}")
    check(
        all(len(members) <= manage_policies.MAX_GROUP_POLICIES for members in manage_policies.GROUP_SERVICES.values()), "a group holds more than ten policies"
    )


def test_plan_in_an_empty_account_lists_every_creation_and_changes_nothing(env_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    iam = FakeIam()
    session = FakeSession(iam)
    code = manage_policies.run(arguments(env_file, "plan"), {}, session)
    output = capsys.readouterr().out
    check(code == 0, "a plan must succeed")
    check(output.count("CREATE POLICY") == 13 and output.count("CREATE GROUP") == 2 and output.count("ATTACH POLICY") == 13, f"unexpected plan:\n{output}")
    check(output.count("ADD USER") == 2, "the user must join both groups")
    check(iam.writes() == [] and session.profiles == ["example_administrator"], "a plan must not write and must use the administrator profile")


def test_apply_creates_everything_and_a_second_run_changes_nothing(env_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    iam = FakeIam()
    check(manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam)) == 0, "the first apply must succeed")
    check(len(iam.policies) == 13 and sorted(iam.groups) == [f"{PROJECT}_data_dev", f"{PROJECT}_platform_dev"], "policies or groups are missing")
    check(sum(len(held) for held in iam.groups.values()) == 13 and iam.members[USER] == set(iam.groups), "attachments or membership are missing")
    capsys.readouterr()
    iam.calls.clear()
    check(manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam)) == 0, "the second apply must succeed")
    check(iam.writes() == [], f"a second apply must write nothing: {iam.writes()}")
    check("Summary: 0 change(s), 13 unchanged." in capsys.readouterr().out, "the second plan must show no change")


def applied_iam(env_file: Path) -> FakeIam:
    iam = FakeIam()
    check(manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam)) == 0, "the first apply must succeed")
    iam.calls.clear()
    return iam


def refused_plan(env_file: Path, iam: FakeIam, action: str = "plan") -> str:
    """Run the action and return the refusal text; nothing may have been written."""
    with pytest.raises(RuntimeError) as error:
        manage_policies.run(arguments(env_file, action), CONFIRMED, FakeSession(iam))
    check(iam.writes() == [], f"a refused run must write nothing: {iam.writes()}")
    return str(error.value)


def test_a_group_with_another_member_is_refused_before_any_change(env_file: Path) -> None:
    iam = applied_iam(env_file)
    iam.group_users[f"{PROJECT}_data_dev"].add("example_other_user")
    message = refused_plan(env_file, iam, "apply")
    check("example_other_user" in message and f"{PROJECT}_data_dev" in message, f"the extra member must be named: {message}")


def test_a_group_with_an_extra_managed_policy_is_refused_before_any_change(env_file: Path) -> None:
    iam = applied_iam(env_file)
    iam.groups[f"{PROJECT}_platform_dev"].add("AdministratorAccess")
    iam.aws_managed.add("AdministratorAccess")
    message = refused_plan(env_file, iam, "apply")
    check("arn:aws:iam::aws:policy/AdministratorAccess" in message, f"the extra policy must be named: {message}")
    iam.aws_managed.clear()
    iam.groups[f"{PROJECT}_platform_dev"].discard("AdministratorAccess")
    iam.groups[f"{PROJECT}_data_dev"].add(f"{PROJECT}_s3_policy_copy")
    check(f"{PROJECT}_s3_policy_copy" in refused_plan(env_file, iam), "a local policy outside the intended set must be refused too")
    iam.groups[f"{PROJECT}_data_dev"].discard(f"{PROJECT}_s3_policy_copy")
    iam.aws_managed.add(f"{PROJECT}_sns_policy")
    check("arn:aws:iam::aws:policy/" in refused_plan(env_file, iam), "a policy with an intended name but another ARN must be refused")


def test_a_group_with_an_inline_policy_is_refused_before_any_change(env_file: Path) -> None:
    iam = applied_iam(env_file)
    iam.group_inline[f"{PROJECT}_data_dev"] = {"example_inline_grant"}
    message = refused_plan(env_file, iam, "apply")
    check("example_inline_grant" in message and "inline" in message, f"the inline policy must be named: {message}")


@pytest.mark.parametrize("action", ["plan", "apply"])
def test_a_group_with_a_retired_policy_is_refused_before_any_change(env_file: Path, action: str) -> None:
    # The apply attaches the new policies before it retires the old ones, so a retired policy left on a group can push it past the
    # ten-policy quota in the middle of a run.
    iam = applied_iam(env_file)
    group = f"{PROJECT}_platform_dev"
    for service in ("ecs", "logs", "sns"):
        iam.groups[group].discard(f"{PROJECT}_{service}_policy")
    for service in manage_policies.RETIRED_SERVICES:
        iam.groups[group].add(f"{PROJECT}_{service}_policy")
    message = refused_plan(env_file, iam, action)
    for service in manage_policies.RETIRED_SERVICES:
        check(f"{PROJECT}_{service}_policy" in message, f"every retired policy on the group must be named: {message}")


def test_a_group_without_the_project_user_is_repaired_not_refused(env_file: Path) -> None:
    iam = applied_iam(env_file)
    group = f"{PROJECT}_data_dev"
    iam.group_users[group].discard(USER)
    iam.members[USER].discard(group)
    check(manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam)) == 0, "a missing membership is a change, not a refusal")
    check(group in iam.members[USER] and USER in iam.group_users[group], "the missing membership must be restored")


def test_a_group_with_a_missing_intended_policy_is_repaired_not_refused(env_file: Path) -> None:
    iam = applied_iam(env_file)
    iam.groups[f"{PROJECT}_data_dev"].discard(f"{PROJECT}_s3_policy")
    check(manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam)) == 0, "a missing attachment is a change, not a refusal")
    check(f"{PROJECT}_s3_policy" in iam.groups[f"{PROJECT}_data_dev"], "the missing attachment must be restored")


def test_the_other_grants_of_the_project_user_are_reported(env_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    iam = applied_iam(env_file)
    capsys.readouterr()
    check(manage_policies.run(arguments(env_file, "plan"), {}, FakeSession(iam)) == 0, "the plan must succeed")
    check("NOTE" not in capsys.readouterr().out, "a user with only its two groups has nothing to report")
    iam.user_policies[USER].add("PowerUserAccess")
    iam.user_inline[USER].add("example_user_inline")
    iam.members[USER].add("example_other_group")
    check(manage_policies.run(arguments(env_file, "plan"), {}, FakeSession(iam)) == 0, "the user's other grants are reported, not refused")
    output = capsys.readouterr().out
    check("PowerUserAccess" in output and "example_user_inline" in output and "example_other_group" in output, f"the other grants must be named:\n{output}")


def test_apply_refuses_without_the_confirmation_word_before_any_aws_call(env_file: Path) -> None:
    for environment in ({}, {manage_policies.CONFIRMATION_VARIABLE: "yes"}, {manage_policies.CONFIRMATION_VARIABLE: manage_policies.CONFIRMATION + " "}):
        with pytest.raises(ConfigurationError) as error:
            manage_policies.run(arguments(env_file, "apply"), environment, forbidden_session)
        check(manage_policies.CONFIRMATION in str(error.value), "the refusal must name the confirmation word")


def test_the_administrator_profile_is_required_and_is_not_the_project_profile(env_file: Path) -> None:
    for admin in (USER, " ", ""):
        with pytest.raises(ConfigurationError) as error:
            manage_policies.run(arguments(env_file, "plan", admin), {}, forbidden_session)
        check("administrator profile" in str(error.value), "the refusal must name the administrator profile")


def test_the_wrong_account_or_the_project_user_is_refused_before_any_read(env_file: Path) -> None:
    iam = FakeIam()
    with pytest.raises(RuntimeError) as account:
        manage_policies.run(arguments(env_file, "plan"), {}, FakeSession(iam, account="222222222222"))
    check("another account" in str(account.value), "a profile in another account must be refused")
    with pytest.raises(RuntimeError) as user:
        manage_policies.run(arguments(env_file, "plan"), {}, FakeSession(iam, arn=f"arn:aws:iam::{ACCOUNT}:user/{USER}"))
    check("project user" in str(user.value), "the project user as the administrator must be refused")
    check(iam.calls == [], "nothing may be read or written")


def test_a_missing_project_user_stops_the_plan(env_file: Path) -> None:
    with pytest.raises(RuntimeError) as error:
        manage_policies.run(arguments(env_file, "plan"), {}, FakeSession(FakeIam(user_exists=False)))
    check(USER in str(error.value) and "does not exist" in str(error.value), "the refusal must name the user")


def test_stored_documents_in_the_form_iam_returns_count_as_unchanged() -> None:
    template = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"], "Resource": ["arn:aws:s3:::x/*"], "Condition": {"StringEquals": {"a": ["b"]}}}
        ],
    }
    stored = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": "arn:aws:s3:::x/*", "Condition": {"StringEquals": {"a": ["b"]}}}
        ],
    }
    changed = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::x/*", "Condition": {"StringEquals": {"a": ["b"]}}}],
    }
    check(manage_policies.canonical(template) == manage_policies.canonical(stored), "AWS's scalar form and order must compare equal")
    check(manage_policies.canonical(template) != manage_policies.canonical(changed), "a removed action must compare unequal")


def test_an_update_frees_a_version_slot_first_and_makes_the_new_version_the_default(env_file: Path) -> None:
    iam = FakeIam()
    manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam))
    name = f"{PROJECT}_sns_policy"
    iam.add_policy(name, {"Version": "2012-10-17", "Statement": []}, versions=5)
    iam.calls.clear()
    check(manage_policies.run(arguments(env_file, "apply", policies=["sns"]), CONFIRMED, FakeSession(iam)) == 0, "the update must succeed")
    check(
        iam.calls[0] == f"delete_policy_version {name} v1" and iam.calls[1] == f"create_policy_version {name}", f"the oldest version must go first: {iam.calls}"
    )
    check(len(iam.policies[name]["versions"]) == 5 and iam.policies[name]["versions"][-1]["IsDefaultVersion"], "the new version must be the default")


def test_a_failed_step_is_reported_and_the_others_still_run(env_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    iam = FakeIam()
    iam.fail_on = {f"create_policy {PROJECT}_ecr_policy"}
    code = manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam))
    output = capsys.readouterr()
    check(code == 1, "a failed step must give a non-zero exit code")
    check(f"{PROJECT}_ecr_policy" not in iam.policies and len(iam.policies) == 12, "only the failed policy may be missing")
    check("FAILED" in output.out and "LimitExceeded" in output.out, "the failure must be named")
    iam.fail_on = set()
    check(manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam)) == 0 and len(iam.policies) == 13, "a rerun must finish the work")


def test_old_grouped_policies_are_detached_and_deleted_and_direct_attachments_move_to_the_groups(env_file: Path) -> None:
    iam = FakeIam()
    for service in ("lakehouse", "platform_network", "platform_services", "platform_tags", "secrets"):
        iam.add_policy(f"{PROJECT}_{service}_policy", {"Version": "2012-10-17", "Statement": []}, versions=2)
        iam.user_policies[USER].add(f"{PROJECT}_{service}_policy")
    iam.add_policy(f"{PROJECT}_s3_policy", {"Version": "2012-10-17", "Statement": []})
    iam.user_policies[USER].add(f"{PROJECT}_s3_policy")
    check(manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam)) == 0, "the apply must succeed")
    check(
        not any("platform_" in name or name.endswith(("lakehouse_policy", "secrets_policy")) for name in iam.policies),
        f"retired policies remain: {sorted(iam.policies)}",
    )
    check(iam.user_policies[USER] == set(), f"no direct attachment may stay on the user: {iam.user_policies[USER]}")
    check(f"{PROJECT}_s3_policy" in iam.groups[f"{PROJECT}_data_dev"], "the S3 policy must reach the user through the data group")


def test_a_retired_policy_held_by_a_role_is_kept_and_reported(env_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    iam = FakeIam()
    name = f"{PROJECT}_secrets_policy"
    iam.add_policy(name, {"Version": "2012-10-17", "Statement": []})
    iam.role_policies["example_other_role"] = {name}
    code = manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam))
    check(code == 1 and name in iam.policies, "a policy that a role holds must not be deleted")
    check("example_other_role" in capsys.readouterr().out, "the other holder must be named")


def test_a_selected_policy_limits_the_run_and_skips_the_retirements(env_file: Path) -> None:
    iam = FakeIam()
    iam.add_policy(f"{PROJECT}_secrets_policy", {"Version": "2012-10-17", "Statement": []})
    manage_policies.run(arguments(env_file, "apply", policies=["sns"]), CONFIRMED, FakeSession(iam))
    check(sorted(iam.policies) == [f"{PROJECT}_secrets_policy", f"{PROJECT}_sns_policy"], f"only the selected policy may be created: {sorted(iam.policies)}")
    with pytest.raises(render_policy.PolicyRenderError):
        manage_policies.run(arguments(env_file, "plan", policies=["unknown"]), {}, FakeSession(iam))


def test_the_exporter_replaces_deployment_values_with_placeholders() -> None:
    values = {
        "AWS_REGION": "eu-west-1",
        "AWS_ACCOUNT_ID": ACCOUNT,
        "PROJECT_NAME": PROJECT,
        "ENVIRONMENT": "dev",
        "DATA_BUCKET_NAME": "example-project-ci-bucket",
    }
    resources = [f"arn:aws:ecs:eu-west-1:{ACCOUNT}:cluster/{PROJECT}_cluster_dev", "arn:aws:s3:::example-project-ci-bucket/lakehouse/*"]
    resources.append(f"arn:aws:ssm:eu-west-1:{ACCOUNT}:session/${{aws:username}}-*")
    document = {"Resource": resources, "Condition": {"StringEquals": {"aws:RequestedRegion": "eu-west-1", "aws:RequestTag/Project": PROJECT}}}
    exported = json.dumps(export_policies.sanitize(document, values))
    for private in (ACCOUNT, "eu-west-1", PROJECT, "example-project-ci-bucket", "_dev"):
        check(private not in exported, f"{private} must not survive the export")
    check(
        "${AWS_ACCOUNT_ID}" in exported and "${PROJECT_NAME}_cluster_${ENVIRONMENT}" in exported and "${aws:username}" in exported,
        f"placeholders are missing: {exported}",
    )


def test_export_writes_one_template_per_service_and_stops_on_a_missing_policy(env_file: Path, tmp_path: Path) -> None:
    iam = FakeIam()
    manage_policies.run(arguments(env_file, "apply"), CONFIRMED, FakeSession(iam))
    exported = export_policies.run(
        argparse.Namespace(admin_profile="example_administrator", env_file=env_file, output_directory=tmp_path / "out"), FakeSession(iam)
    )
    check(len(exported) == 13 and all(path.read_text(encoding="utf-8").endswith("}\n") for path in exported), "one template per service must be written")
    check(ACCOUNT not in "".join(path.read_text(encoding="utf-8") for path in exported), "the account must not appear in an exported template")
    del iam.policies[f"{PROJECT}_ssm_policy"]
    with pytest.raises(RuntimeError) as error:
        export_policies.run(argparse.Namespace(admin_profile="example_administrator", env_file=env_file, output_directory=tmp_path / "again"), FakeSession(iam))
    check(f"{PROJECT}_ssm_policy" in str(error.value), "the missing policy must be named")


def run_cli(module: str, env_file: Path, *extra: str) -> Any:
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(env_file.parent),
        "AWS_CONFIG_FILE": str(env_file.parent / "absent"),
        "AWS_SHARED_CREDENTIALS_FILE": str(env_file.parent / "absent"),
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    return run_command(sys.executable, ["-m", module, "--env-file", str(env_file), *extra], cwd=REPO_ROOT, env=environment, timeout=60)


def test_the_command_line_refuses_an_apply_without_the_word_and_renders_offline(env_file: Path, tmp_path: Path) -> None:
    refused = run_cli("infra.iam.scripts.manage_policies", env_file, "apply", "--admin-profile", "example_administrator")
    check(refused.returncode == 2 and manage_policies.CONFIRMATION in refused.stderr, f"apply must refuse: {refused.stderr}")
    check("Credentials" not in refused.stderr and "botocore" not in refused.stderr, "the refusal must come before any AWS client")
    rendered = run_cli("infra.iam.scripts.render_policy", env_file, "--output-directory", str(tmp_path / "rendered"))
    files = sorted((tmp_path / "rendered").glob("*.json"))
    check(rendered.returncode == 0 and len(files) == 13, f"the offline render must write 13 files: {rendered.stderr}")
    check(all(oct(path.stat().st_mode & 0o777) == "0o600" for path in files), "rendered files must be owner-only")
    check(
        f":{ACCOUNT}:" in files[0].read_text(encoding="utf-8") or ACCOUNT in "".join(path.read_text(encoding="utf-8") for path in files),
        "the account must be substituted",
    )
