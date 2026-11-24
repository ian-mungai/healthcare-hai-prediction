"""Move the Terraform state of each stack from its local file into the project bucket, once, and prove the move.

The owner runs one stack at a time (sheet: ``scripts/infrastructure/owner/state_migration.sh``). The state of a stack ends up at
``terraform_state/<stack>/terraform.tfstate`` in the project bucket with its lock file beside it (the S3 backend's own ``use_lockfile``, no DynamoDB
table). The bucket policy of the storage stack (``infra/main.tf``, switch ``terraform_state_enabled``) lets only the project user touch the state of
the storage, platform, services, jobs and alerts stacks, and only the administrator touch the state of the iam stack. This script keeps that split:
the iam stack uses the administrator profile (``--admin-profile``, required there and refused anywhere else), every other stack the project profile of
``.env``. Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.state_migration <command> --stack <stack> [--admin-profile
NAME]``.

Commands, in the order the sheet runs them:

- ``backend`` writes ``infra/<stack>/backend.tf`` (partial: the key, the lock file and encryption; the bucket, the region and the profile come from
  the init options, so nothing account-specific is committed). A different file already there is refused, never overwritten.
- ``check`` reads only: the identity of the profile, the local state (only the default workspace moves: a named local workspace is refused), the
  version history of the bucket, each state statement of its policy (effect, principal, actions, resources and deny conditions), the lifecycle rule of
  replaced state versions (enabled, state prefix, version expiry, no expiry of the current state) and the remote key and its lock.
- ``plan --phase before|after`` runs the project wrapper's plan, keeps the sorted change list (resources and outputs) in the record folder and deletes the
  plan file (it holds values). The storage plan carries the approved state administrator (``plans/state_migration/state_protection.json``, kept by the
  prefix apply) and is refused when it does not. A run that stopped after the copy keeps its plan before.
- ``approve-plan`` reads a saved plan's JSON and passes only when its resource changes are exactly the named ``--update`` addresses, each as an update in
  place (the prefix sheet runs it before its question).
- ``migrate`` needs the plan before, runs the check, backs up the local state, copies it with a write that refuses an existing key, reads it back and
  compares the bytes, switches the backend with ``terraform init -reconfigure`` (equal states, so Terraform moves nothing itself and empties the local
  file) and reads the state back through the backend. The serial, the lineage and the resource count must equal the local ones at every step. A stop
  before the switch leaves the local state and the stack folder as they were. A failed read puts back the backend file, the local file and the backend
  settings Terraform cached, and reports recovery only after a local command read the original state. A run that stopped after the switch resumes from
  its record when the backend and the bucket still hold the recorded state.
- ``verify`` reads the state through the backend again and needs the exact recorded fingerprint and an object whose SHA-256 equals the record, then
  compares the plan after with the plan before.

A stack that finished is never migrated again: its record says ``done`` and its folder holds the switched backend. A remote key that holds other bytes
than the local state is refused. The bytes of the same state are adopted, which resumes a run that stopped between the copy and the record. Output is
``<subject>: <status>`` lines only. Exit code 0 means the step is done, 1 means it stopped and 2 means a bad argument. Rollback: see
``docs/terraform_state_migration.md``.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from scripts.infrastructure.render_project_config import ConfigurationError, load_configuration
from scripts.infrastructure.state_protection import LIFECYCLE_RULE, STATE_SIDS, Refused, deployment_digest
from scripts.infrastructure.state_protection import RECORD as STATE_PROTECTION_RECORD
from scripts.infrastructure.state_protection import read as read_state_protection
from scripts.process import TimeoutExpired, run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
RECORD_FOLDER = Path("plans/state_migration")
STATE_PREFIX = "terraform_state"
TERRAFORM_SECONDS = 600
WRAPPER_SECONDS = 1800
SCRUBBED = ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN")
ENDPOINT_VARIABLES = ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL_STS")
LOOPBACK = {"127.0.0.1", "localhost", "::1"}
EXIT_STOPPED = 1
EXIT_USAGE = 2
EXIT_NOT_RESTORED = 3
WRAPPER_SCRIPT = (
    'options=(); [ -z "$WRAPPER_STACK" ] || options+=("$WRAPPER_STACK"); [ -z "$WRAPPER_ADMIN" ] || options+=(--admin-profile "$WRAPPER_ADMIN"); '
    '[ -z "$WRAPPER_STATE_ADMIN" ] || options+=(--state-admin "$WRAPPER_STATE_ADMIN"); '
    'exec bash scripts/infrastructure/terraform.sh "${options[@]}" plan -detailed-exitcode -out "$WRAPPER_PLAN"'
)
STATE_RULE_PREFIX = f"{STATE_PREFIX}/"
STATE_RULE_DAYS = 90
OBJECT_STATEMENTS = STATE_SIDS[:2]
OBJECT_ACTIONS = ({"s3:GetObject", "s3:PutObject"}, {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"})
DENY_STATEMENTS = STATE_SIDS[2:]
SAVED_PLAN_STATEMENTS = (
    "ProjectUserReadsSavedPlans",
    "DenySavedPlansToEveryoneElse",
    "DenySavedPlanWritesExceptThePlanRole",
    "DenyThePlanRoleAnythingButSavingPlans",
)
PREFIX_POLICY = "aws_s3_bucket_policy.tls"
PREFIX_LIFECYCLE = "aws_s3_bucket_lifecycle_configuration.data"
PREFIX_RULES = (LIFECYCLE_RULE, "expire_saved_plans")
NOT_KEYS = ("NotAction", "NotResource", "NotPrincipal")
PHASES = ("before", "after")
CLIENT_CONFIG = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=60)
LOOPBACK_CONFIG = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=60, s3={"addressing_style": "path"})


@dataclass(frozen=True)
class Stack:
    """One Terraform root: its folder, the options of the project wrapper that select it, and whether the administrator profile applies it."""

    folder: str
    wrapper: tuple[str, ...]
    administrator: bool = False


STACKS = {
    "storage": Stack("infra", ()),
    "iam": Stack("infra/iam", ("--iam",), administrator=True),
    "platform": Stack("infra/platform", ("--platform",)),
    "services": Stack("infra/services", ("--services",)),
    "jobs": Stack("infra/jobs", ("--jobs",)),
    "alerts": Stack("infra/alerts", ("--alerts",)),
}


class Stopped(Exception):
    """A step stops with this status line; the exit code is 1 (2 for a bad argument)."""

    def __init__(self, line: str, code: int = EXIT_STOPPED) -> None:
        super().__init__(line)
        self.line = line
        self.code = code


def say(subject: str, status: str) -> None:
    sys.stdout.write(f"{subject}: {status}\n")
    sys.stdout.flush()


def state_key(stack: str) -> str:
    return f"{STATE_PREFIX}/{stack}/terraform.tfstate"


def backend_text(stack: str) -> str:
    """The backend block of one stack. The location (bucket, region, profile) is passed to ``terraform init``, so no account value is committed."""
    return (
        f"# Remote state of the {stack} stack, written by scripts/infrastructure/state_migration.py. The location and the sign-in come from the init options.\n"
        "terraform {\n"
        '  backend "s3" {\n'
        f'    key                  = "{state_key(stack)}"\n'
        f'    workspace_key_prefix = "{STATE_PREFIX}/{stack}/workspaces"\n'
        "    use_lockfile         = true\n"
        "    encrypt              = true\n"
        "  }\n"
        "}\n"
    )


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Fingerprint:
    """What must stay equal through the move: the serial, the lineage and the resource count of a state."""

    serial: int
    lineage: str
    resources: int
    instances: int

    def text(self) -> str:
        return f"serial {self.serial}, {self.resources} resources, {self.instances} instances"


def fingerprint(data: bytes) -> Fingerprint | None:
    """The fingerprint of a Terraform state document, or None when it is not one."""
    try:
        state = json.loads(data)
        resources = state["resources"]
        serial, lineage = state["serial"], state["lineage"]
        instances = sum(len(resource.get("instances", [])) for resource in resources)
    except (ValueError, KeyError, TypeError, AttributeError):
        return None
    if not isinstance(serial, int) or isinstance(serial, bool) or not isinstance(lineage, str) or not isinstance(resources, list) or state.get("version") != 4:
        return None
    return Fingerprint(serial, lineage, len(resources), instances)


@dataclass(frozen=True)
class Settings:
    """The inputs of one stack: the profile that applies it and the bucket, region and account from ``.env``."""

    stack: str
    profile: str
    region: str
    bucket: str
    account: str
    project_user: str
    root: Path

    @property
    def folder(self) -> Path:
        return self.root / STACKS[self.stack].folder

    @property
    def local_state(self) -> Path:
        return self.folder / "terraform.tfstate"

    @property
    def record_dir(self) -> Path:
        return self.root / RECORD_FOLDER

    @property
    def record(self) -> Path:
        return self.record_dir / f"{self.stack}.json"

    @property
    def backup(self) -> Path:
        return self.record_dir / f"{self.stack}.local_state.json"

    def changes(self, phase: str) -> Path:
        return self.record_dir / f"{self.stack}_{phase}.changes.txt"


def load_settings(root: Path, stack: str, admin_profile: str | None) -> Settings:
    """Read ``.env`` and choose the profile: the administrator for the iam stack only, the project profile for every other stack."""
    if STACKS[stack].administrator and not admin_profile:
        raise Stopped("administrator profile: the iam stack needs --admin-profile", EXIT_USAGE)
    if not STACKS[stack].administrator and admin_profile:
        raise Stopped("administrator profile: only the iam stack takes --admin-profile", EXIT_USAGE)
    try:
        storage, iam = load_configuration(root / ".env", admin_profile if STACKS[stack].administrator else None)
    except ConfigurationError as error:
        text = str(error)
        raise Stopped(f"administrator profile: {text}" if "administrator" in text else f"settings: {text}") from None
    profile = iam["admin_profile"] if STACKS[stack].administrator else storage["aws_profile"]
    return Settings(
        stack=stack,
        profile=profile,
        region=storage["aws_region"],
        bucket=storage["data_bucket_name"],
        account=storage["expected_account_id"],
        project_user=f"arn:aws:iam::{storage['expected_account_id']}:user/{storage['aws_profile']}",
        root=root,
    )


def endpoint_override(environment: Mapping[str, str]) -> bool:
    """True when a loopback endpoint is set (the E2E stand-in). Any other endpoint override is refused: state goes only where the project named."""
    found = False
    for name in ENDPOINT_VARIABLES:
        value = environment.get(name)
        if value:
            if urlsplit(value).hostname not in LOOPBACK:
                raise Stopped(f"endpoint: {name} names an address that is not the machine itself")
            found = True
    return found


def terraform_environment() -> dict[str, str]:
    """The caller's environment without the AWS identity variables (the profile is an option), refusing injected Terraform arguments."""
    environment = {name: value for name, value in os.environ.items() if name not in SCRUBBED}
    for name in environment:
        if name == "TF_CLI_ARGS" or name.startswith("TF_CLI_ARGS_"):
            raise Stopped(f"terraform: unset {name}; pass Terraform options explicitly")
    return environment


class Terraform:
    """The Terraform calls of one stack folder, each with a deadline."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.environment = terraform_environment()

    def call(self, arguments: list[str], seconds: int = TERRAFORM_SECONDS) -> tuple[int, str]:
        try:
            result = run_command("terraform", [f"-chdir={self.settings.folder}", *arguments], env=self.environment, timeout=seconds)
        except (OSError, TimeoutExpired):
            return 1, ""
        return result.returncode, result.stdout

    def init(self, remote: bool, path_style: bool) -> bool:
        options = ["init", "-reconfigure", "-input=false", "-no-color", "-lockfile=readonly"]
        if remote:
            settings = self.settings
            options += [f"-backend-config=bucket={settings.bucket}", f"-backend-config=region={settings.region}", f"-backend-config=profile={settings.profile}"]
            if path_style:
                options.append("-backend-config=use_path_style=true")
        return self.call(options)[0] == 0

    def pull(self) -> bytes | None:
        code, output = self.call(["state", "pull"])
        return output.encode() if code == 0 else None


def listed(value: object) -> list[str]:
    """The strings of a policy field that may be one string or a list of them."""
    if isinstance(value, str):
        return [value]
    return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []


def invalid_policy(sid: str, reason: str) -> Stopped:
    return Stopped(f"bucket policy: state statements invalid ({sid}: {reason})")


def principal_of(arn: str, account: str) -> str:
    """The ARN that ``aws:PrincipalArn`` shows for a caller: the role for an assumed-role session, the caller itself otherwise."""
    parts = arn.split(":", 5)
    if len(parts) == 6 and parts[2] == "sts" and parts[5].startswith("assumed-role/"):
        return f"arn:aws:iam::{account}:role/{parts[5].split('/')[1]}"
    return arn


def check_allow(statement: dict[str, Any], sid: str, actions: set[str], user: str, wanted: str) -> None:
    """One Allow statement: effect, principal, actions and resources are exactly the ones of ``infra/main.tf`` and the stack's own key is named."""
    principal = statement.get("Principal")
    if statement.get("Effect") != "Allow" or any(name in statement for name in (*NOT_KEYS, "Condition")):
        raise invalid_policy(sid, "it is not a plain allow")
    if not isinstance(principal, dict) or listed(principal.get("AWS")) != [user]:
        raise invalid_policy(sid, "its principal is not the project user")
    if set(listed(statement.get("Action"))) != actions:
        raise invalid_policy(sid, "its actions differ")
    resources = listed(statement.get("Resource"))
    if any("*" in resource or f"/{STATE_PREFIX}/iam/" in resource for resource in resources):
        raise invalid_policy(sid, "a resource is a wildcard or the iam stack")
    if wanted not in resources:
        raise Stopped("bucket policy: state statements missing (the state key of this stack is not allowed)")


def check_deny(statement: dict[str, Any], sid: str, resource: str, operator: str, names: list[str], absent: list[str]) -> None:
    """One Deny statement: it covers everyone, all S3 actions and exactly this prefix, and its condition lists the principals that must stay in or out."""
    if statement.get("Effect") != "Deny" or any(name in statement for name in NOT_KEYS):
        raise invalid_policy(sid, "it is not a plain deny")
    principal = statement.get("Principal")
    if principal != "*" and not (isinstance(principal, dict) and listed(principal.get("AWS")) == ["*"]):
        raise invalid_policy(sid, "its principal is not everyone")
    if listed(statement.get("Action")) != ["s3:*"] or listed(statement.get("Resource")) != [resource]:
        raise invalid_policy(sid, "its actions or its resource differ")
    condition = statement.get("Condition")
    if (
        not isinstance(condition, dict)
        or list(condition) != [operator]
        or not isinstance(condition[operator], dict)
        or list(condition[operator]) != ["aws:PrincipalArn"]
    ):
        raise invalid_policy(sid, "its condition is not the principal list")
    found = listed(condition[operator]["aws:PrincipalArn"])
    if any(name not in found for name in names) or any(name in found for name in absent):
        raise invalid_policy(sid, "its principal list differs")


def rule_prefix(rule: dict[str, Any]) -> str | None:
    """The prefix of a lifecycle rule's filter, an empty string for a rule of the whole bucket and None for a filter that is not a plain prefix."""
    scope = rule.get("Filter")
    if isinstance(scope, dict):
        return str(scope["Prefix"]) if "Prefix" in scope else ("" if not scope else None)
    return str(rule.get("Prefix", ""))


def check_state_rule(rule: dict[str, Any]) -> str | None:
    """Why the rule of replaced state versions differs from ``infra/main.tf``, or None when it is the same: enabled, this prefix, replaced versions expire."""
    expiration = rule.get("Expiration", {})
    if rule.get("Status") != "Enabled":
        return "it is not enabled"
    if rule_prefix(rule) != STATE_RULE_PREFIX:
        return "it does not cover the state prefix"
    if rule.get("NoncurrentVersionExpiration", {}).get("NoncurrentDays") != STATE_RULE_DAYS:
        return f"replaced versions do not expire after {STATE_RULE_DAYS} days"
    if "Days" in expiration or "Date" in expiration:
        return "it expires the current state"
    return None


def state_objects() -> list[str]:
    """The state key and the lock key of every stack: the objects that a rule expiring current objects must not reach."""
    return [name for stack in STACKS for name in (state_key(stack), f"{state_key(stack)}.tflock")]


def can_reach(rule: dict[str, Any], keys: list[str]) -> bool:
    """False only when the rule's filter provably misses every key, which is a prefix that no key starts with. A tag or an object size condition cannot be
    proved to miss, so a rule that names only those, or one that names a prefix some key has next to them, still counts as reaching."""
    scope = rule.get("Filter")
    if not isinstance(scope, dict):
        scope = {"Prefix": rule.get("Prefix", "")}
    combined = scope.get("And")
    prefix = (combined if isinstance(combined, dict) else scope).get("Prefix")
    return prefix is None or any(name.startswith(str(prefix)) for name in keys)


def expires_state(rule: dict[str, Any]) -> bool:
    """True when another enabled rule can expire the current state or lock object of any stack."""
    expiration = rule.get("Expiration", {})
    return rule.get("Status") == "Enabled" and ("Days" in expiration or "Date" in expiration) and can_reach(rule, state_objects())


class Bucket:
    """The S3 and STS calls of one stack, signed by its profile. A failed call raises Stopped with a status word and never prints the AWS answer."""

    def __init__(self, settings: Settings, loopback: bool) -> None:
        self.settings = settings
        self.loopback = loopback
        session = boto3.Session(profile_name=settings.profile, region_name=settings.region)
        self.principal = settings.project_user
        self.s3 = session.client("s3", config=LOOPBACK_CONFIG if loopback else CLIENT_CONFIG)
        self.sts = session.client("sts", config=CLIENT_CONFIG)

    def identity(self) -> None:
        try:
            answer = self.sts.get_caller_identity()
        except (BotoCoreError, ClientError):
            raise Stopped("identity: refused (the profile could not sign in)") from None
        arn = answer.get("Arn", "")
        settings = self.settings
        if STACKS[settings.stack].administrator:
            fits = arn.startswith((f"arn:aws:iam::{settings.account}:", f"arn:aws:sts::{settings.account}:")) and arn != settings.project_user
        else:
            fits = arn == settings.project_user
        if answer.get("Account") != settings.account or not fits:
            raise Stopped("identity: refused (not the expected principal of this stack in the configured account)")
        self.principal = principal_of(arn, settings.account)
        say("identity", "ok")

    def versioning(self) -> None:
        try:
            status = self.s3.get_bucket_versioning(Bucket=self.settings.bucket).get("Status")
        except (BotoCoreError, ClientError):
            raise Stopped("bucket versioning: could not be read") from None
        if status != "Enabled":
            raise Stopped("bucket versioning: not enabled")
        say("bucket versioning", "on")

    def policy(self) -> None:
        """Read the bucket policy and check each state statement for what it does: its effect, principal, actions, resources and deny conditions."""
        settings = self.settings
        try:
            document = json.loads(self.s3.get_bucket_policy(Bucket=settings.bucket)["Policy"])
        except (BotoCoreError, ClientError, ValueError, KeyError):
            raise Stopped("bucket policy: state statements missing (the policy could not be read)") from None
        raw = document.get("Statement", []) if isinstance(document, dict) else []
        statements = [statement for statement in raw if isinstance(statement, dict)] if isinstance(raw, list) else []
        administrator = STACKS[settings.stack].administrator
        required = DENY_STATEMENTS if administrator else (*OBJECT_STATEMENTS, *DENY_STATEMENTS)
        found = {sid: [statement for statement in statements if statement.get("Sid") == sid] for sid in required}
        if any(not matching for matching in found.values()):
            raise Stopped("bucket policy: state statements missing")
        for sid, matching in found.items():
            if len(matching) > 1:
                raise invalid_policy(sid, "the Sid appears more than once")
        prefix = f"arn:aws:s3:::{settings.bucket}/{STATE_PREFIX}"
        user, principal = settings.project_user, self.principal
        if not administrator:
            key = f"{prefix}/{settings.stack}/terraform.tfstate"
            for sid, actions, wanted in zip(OBJECT_STATEMENTS, OBJECT_ACTIONS, (key, f"{key}.tflock"), strict=True):
                check_allow(found[sid][0], sid, actions, user, wanted)
        check_deny(found[DENY_STATEMENTS[0]][0], DENY_STATEMENTS[0], f"{prefix}/*", "ArnNotLike", [principal], [])
        check_deny(found[DENY_STATEMENTS[1]][0], DENY_STATEMENTS[1], f"{prefix}/iam/*", "ArnLike", [user], [principal] if administrator else [])
        reached = [f"{prefix}/{settings.stack}/terraform.tfstate", f"{prefix}/{settings.stack}/terraform.tfstate.tflock"]
        for statement in statements:
            unconditional = statement.get("Effect") == "Deny" and "Condition" not in statement and statement.get("Sid") not in required
            if unconditional and any(fnmatch.fnmatchcase(key, pattern) for key in reached for pattern in listed(statement.get("Resource"))):
                raise invalid_policy(str(statement.get("Sid", "?")), "an unconditional deny reaches this stack's state key")
        say("bucket policy", "state statements present")

    def lifecycle(self) -> None:
        try:
            rules = [rule for rule in self.s3.get_bucket_lifecycle_configuration(Bucket=self.settings.bucket).get("Rules", []) if isinstance(rule, dict)]
        except (BotoCoreError, ClientError):
            raise Stopped("lifecycle: rule for replaced state versions missing (the rules could not be read)") from None
        mine = [rule for rule in rules if rule.get("ID") == LIFECYCLE_RULE]
        if not mine:
            raise Stopped("lifecycle: rule for replaced state versions missing")
        problem = "the rule appears more than once" if len(mine) > 1 else check_state_rule(mine[0])
        if problem:
            raise Stopped(f"lifecycle: rule for replaced state versions invalid ({problem})")
        for rule in rules:
            if rule.get("ID") != LIFECYCLE_RULE and expires_state(rule):
                raise Stopped(f"lifecycle: another rule can expire the current state or lock object of a stack ({rule.get('ID', '?')})")
        say("lifecycle", "replaced state versions expire")

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.settings.bucket, Key=key)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise Stopped("remote key: could not be read") from None
        except BotoCoreError:
            raise Stopped("remote key: could not be read") from None
        return True

    def read(self, key: str) -> bytes:
        try:
            return self.s3.get_object(Bucket=self.settings.bucket, Key=key)["Body"].read()
        except (BotoCoreError, ClientError, OSError):
            raise Stopped("remote key: could not be read") from None

    def create(self, key: str, data: bytes) -> None:
        """Write the object only when the key does not exist (a conditional write), encrypted with the bucket's own key type."""
        self.s3.put_object(Bucket=self.settings.bucket, Key=key, Body=data, IfNoneMatch="*", ServerSideEncryption="AES256")


def marker_path(settings: Settings) -> Path:
    return settings.folder / ".terraform" / "terraform.tfstate"


def backend_switched(settings: Settings) -> bool:
    """True when Terraform's cached backend settings of the stack folder name the S3 backend."""
    marker = marker_path(settings)
    if not marker.is_file():
        return False
    try:
        return json.loads(marker.read_text(encoding="utf-8")).get("backend", {}).get("type") == "s3"
    except (ValueError, AttributeError, OSError):
        raise Stopped("local state: the backend marker cannot be read") from None


def refuse_named_workspaces(settings: Settings) -> None:
    """Only the default workspace moves: Terraform finds named local workspaces at init and would start a state migration of its own."""
    environment = settings.folder / ".terraform" / "environment"
    try:
        selected = environment.read_text(encoding="utf-8").strip() if environment.is_file() else ""
        named = (
            [path.name for path in (settings.folder / "terraform.tfstate.d").iterdir() if path.is_dir()]
            if (settings.folder / "terraform.tfstate.d").is_dir()
            else []
        )
    except OSError:
        raise Stopped("local state: the workspaces cannot be read") from None
    if selected not in ("", "default"):
        raise Stopped("local state: a named workspace is selected (select the default workspace; only it moves)")
    if named:
        raise Stopped("local state: named workspaces exist in terraform.tfstate.d (only the default workspace moves: move or delete them first)")


def read_local(settings: Settings) -> tuple[bytes, Fingerprint]:
    """The local state of the stack as bytes, or a stop. A switched backend, a named workspace, a missing, empty or invalid file all stop here."""
    if backend_switched(settings):
        raise Stopped("local state: backend already switched")
    refuse_named_workspaces(settings)
    if not settings.local_state.is_file():
        raise Stopped("local state: missing")
    data = settings.local_state.read_bytes()
    if not data.strip():
        raise Stopped("local state: empty")
    found = fingerprint(data)
    if found is None:
        raise Stopped("local state: invalid")
    if (settings.folder / ".terraform.tfstate.lock.info").exists():
        raise Stopped("local state: a local lock is held")
    say("local state", f"ok ({found.text()})")
    return data, found


def preflight(settings: Settings, bucket: Bucket, data: bytes) -> str:
    """The read-only checks. Returns what the remote key holds: ``absent`` or ``identical``; any other answer stops."""
    bucket.identity()
    bucket.versioning()
    bucket.policy()
    bucket.lifecycle()
    key = state_key(settings.stack)
    if bucket.exists(f"{key}.tflock"):
        raise Stopped("remote key: locked")
    if not bucket.exists(key):
        say("remote key", "absent")
        return "absent"
    if bucket.read(key) != data:
        raise Stopped("remote key: differs from the local state")
    say("remote key", "identical")
    return "identical"


def write_private(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.writing")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
    os.replace(temporary, path)


def write_record(settings: Settings, stage: str, found: Fingerprint, digest: str) -> None:
    document = {
        "stack": settings.stack,
        "stage": stage,
        "serial": found.serial,
        "lineage": found.lineage,
        "resources": found.resources,
        "instances": found.instances,
        "sha256": digest,
        "at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    write_private(settings.record, (json.dumps(document, indent=2) + "\n").encode())


def read_record(settings: Settings) -> dict[str, Any] | None:
    if not settings.record.is_file():
        return None
    try:
        document = json.loads(settings.record.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        raise Stopped("record: cannot be read") from None
    return document if isinstance(document, dict) else None


def command_backend(settings: Settings) -> None:
    path = settings.folder / "backend.tf"
    wanted = backend_text(settings.stack)
    if path.exists():
        if path.read_text(encoding="utf-8") != wanted:
            raise Stopped("backend: differs from the generated text; no file was changed")
        say("backend", "unchanged")
        return
    path.write_text(wanted, encoding="utf-8")
    say("backend", "written")


def command_check(settings: Settings, loopback: bool) -> None:
    record = read_record(settings)
    if record is not None and record.get("stage") == "copied" and backend_switched(settings):
        # A run that stopped after the switch: the local state is empty by design and ``migrate`` resumes from the record, so only the identity is checked here.
        Bucket(settings, loopback).identity()
        say("check", "resume (the backend is switched and the record is copied; run the plan and migrate steps again to finish)")
        return
    data, _ = read_local(settings)
    preflight(settings, Bucket(settings, loopback), data)
    say("check", "ok")


def changes_of(plan: dict[str, Any]) -> list[str]:
    """The sorted change list of a saved plan: every resource change and every output change that is not a no-op or a read."""
    lines = []
    for change in plan.get("resource_changes", []):
        actions = change.get("change", {}).get("actions", [])
        if actions not in (["no-op"], ["read"]):
            lines.append(f"{change.get('address', '?')}: {'+'.join(actions)}")
    for name, change in plan.get("output_changes", {}).items():
        actions = change.get("actions", [])
        if actions not in (["no-op"], ["read"]):
            lines.append(f"output.{name}: {'+'.join(actions)}")
    return sorted(lines)


def approved_state_admin(settings: Settings, phase: str) -> str:
    """The state administrator the owner approved for the storage stack, or a stop: every storage plan carries it once the protection is on."""
    try:
        principal = read_state_protection(settings.root / STATE_PROTECTION_RECORD, deployment_digest(settings.bucket, settings.account))
    except Refused:
        raise Stopped(f"plan {phase}: the approved state protection inputs are not valid (repair {STATE_PROTECTION_RECORD})") from None
    if principal is None:
        raise Stopped(f"plan {phase}: the approved state protection inputs are not recorded as verified (run state_prefix_apply.sh first)")
    return principal


def carries_state_inputs(plan: dict[str, Any], principal: str) -> bool:
    """True when the plan was made with the state prefix on and the approved administrator: a plan without them would remove the bucket's state statements."""
    variables = plan.get("variables", {})
    enabled = variables.get("terraform_state_enabled", {}).get("value")
    names = variables.get("terraform_state_admin_principals", {}).get("value")
    return enabled is True and names == [principal]


def command_plan(settings: Settings, phase: str) -> None:
    """Plan through the project wrapper (it renders the inputs and verifies the identity) and keep only the sorted change list."""
    record = read_record(settings)
    if phase == "before" and record is not None and record.get("stage") == "copied" and settings.changes("before").is_file():
        say("plan before", "kept (the run stopped after the copy, so the list from before the move stands)")
        return
    principal = approved_state_admin(settings, phase) if settings.stack == "storage" else ""
    plan_file = settings.record_dir / f"{settings.stack}_{phase}.tfplan"
    settings.record_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    environment = {
        **os.environ,
        "WRAPPER_STACK": next(iter(STACKS[settings.stack].wrapper), ""),
        "WRAPPER_ADMIN": settings.profile if STACKS[settings.stack].administrator else "",
        "WRAPPER_STATE_ADMIN": principal,
        "WRAPPER_PLAN": str(plan_file),
    }
    try:
        result = run_command("bash", ["-c", WRAPPER_SCRIPT], cwd=settings.root, env=environment, timeout=WRAPPER_SECONDS)
    except (OSError, TimeoutExpired):
        raise Stopped(f"plan {phase}: the plan did not finish") from None
    try:
        if result.returncode not in (0, 2):
            raise Stopped(f"plan {phase}: failed")
        code, output = Terraform(settings).call(["show", "-json", str(plan_file)])
        try:
            plan = json.loads(output) if code == 0 else None
        except ValueError:
            plan = None
        if not isinstance(plan, dict):
            raise Stopped(f"plan {phase}: the plan could not be read")
        if principal and not carries_state_inputs(plan, principal):
            raise Stopped(f"plan {phase}: the plan does not carry the approved state protection inputs (it would remove the state grants of the bucket)")
        lines = changes_of(plan)
        if result.returncode == 2 and not lines:
            raise Stopped(f"plan {phase}: reported changes that the change list does not show")
    finally:
        plan_file.unlink(missing_ok=True)
    write_private(settings.changes(phase), "".join(f"{line}\n" for line in lines).encode())
    say(f"plan {phase}", "no change" if not lines else f"{len(lines)} change(s) (list in {settings.changes(phase).relative_to(settings.root)})")


def has_unknown(value: object) -> bool:
    """True when a plan's ``after_unknown`` structure marks any value as not yet known."""
    if isinstance(value, dict):
        return any(has_unknown(item) for item in value.values())
    if isinstance(value, list):
        return any(has_unknown(item) for item in value)
    return value is True


def delta_policy(change: dict[str, Any]) -> str | None:
    """Why the bucket policy update is more than the state statements, or None: every statement but those of the state switch and every other attribute stay."""
    before, after = change.get("before"), change.get("after")
    if not isinstance(before, dict) or not isinstance(after, dict) or has_unknown(change.get("after_unknown")):
        return "its before and after cannot be compared"
    try:
        old, new = json.loads(before["policy"]), json.loads(after["policy"])
        old_rest, new_rest = old["Statement"], new["Statement"]
        ids = [statement.get("Sid") for statement in new_rest]
    except (KeyError, TypeError, ValueError, AttributeError):
        return "its policy cannot be read"
    allowed = (*STATE_SIDS, *SAVED_PLAN_STATEMENTS)
    kept_old = [statement for statement in old_rest if statement.get("Sid") not in allowed]
    kept_new = [statement for statement in new_rest if statement.get("Sid") not in allowed]
    if kept_old != kept_new or {**old, "Statement": 0} != {**new, "Statement": 0}:
        return "a statement other than the state statements changes"
    if len({sid for sid in ids if sid in allowed}) != len([sid for sid in ids if sid in allowed]):
        return "a state statement appears more than once"
    if {key: value for key, value in before.items() if key != "policy"} != {key: value for key, value in after.items() if key != "policy"}:
        return "an attribute other than the policy changes"
    return None


def delta_lifecycle(change: dict[str, Any]) -> str | None:
    """Why the lifecycle update is more than the rules of the state switch, or None: every other rule and every other attribute stay."""
    before, after, unknown = change.get("before"), change.get("after"), change.get("after_unknown")
    if not isinstance(before, dict) or not isinstance(after, dict) or not isinstance(unknown, dict):
        return "its before and after cannot be compared"
    old, new, marks = before.get("rule"), after.get("rule"), unknown.get("rule")
    if not isinstance(old, list) or not isinstance(new, list) or not isinstance(marks, list) or len(marks) != len(new):
        return "its rules cannot be read"
    if any(not isinstance(rule, dict) for rule in (*old, *new)):
        return "its rules cannot be read"
    kept_old = [rule for rule in old if rule.get("id") not in PREFIX_RULES]
    if kept_old != [rule for rule in new if rule.get("id") not in PREFIX_RULES]:
        return "a rule other than the state rules changes"
    if any(has_unknown(mark) for rule, mark in zip(new, marks, strict=True) if rule.get("id") not in PREFIX_RULES):
        return "a rule other than the state rules has values not known yet"
    if has_unknown({key: value for key, value in unknown.items() if key != "rule"}):
        return "an attribute other than the rules is not known yet"
    if {key: value for key, value in before.items() if key != "rule"} != {key: value for key, value in after.items() if key != "rule"}:
        return "an attribute other than the rules changes"
    ids = [rule.get("id") for rule in new if rule.get("id") in PREFIX_RULES]
    return "a state rule appears more than once" if len(set(ids)) != len(ids) else None


DELTAS: dict[str, Callable[[dict[str, Any]], str | None]] = {PREFIX_POLICY: delta_policy, PREFIX_LIFECYCLE: delta_lifecycle}


def plan_shape_problems(plan: dict[str, Any], updates: list[str]) -> list[str]:
    """Why a saved plan is not the prefix change, or an empty list: only the named updates in place, no move, no import, no output change, and for each
    update only the state statements of the bucket policy and the state rules of the lifecycle configuration differ."""
    found: dict[str, dict[str, Any]] = {}
    problems = []
    for entry in plan.get("resource_changes", []):
        address = str(entry.get("address", "?"))
        body = entry.get("change", {})
        if entry.get("previous_address"):
            problems.append(f"{address}: moved")
        if body.get("importing"):
            problems.append(f"{address}: imported")
        if body.get("actions", []) not in (["no-op"], ["read"]):
            found[address] = body
    for name, change in plan.get("output_changes", {}).items():
        if change.get("actions", []) not in (["no-op"], ["read"]):
            problems.append(f"output.{name}: {'+'.join(change.get('actions', []))}")
    wanted = set(updates)
    problems += [
        f"{address}: {'+'.join(body.get('actions', []))}"
        for address, body in sorted(found.items())
        if address not in wanted or body.get("actions") != ["update"]
    ]
    problems += [f"{address}: missing" for address in sorted(wanted - found.keys())]
    for address in sorted(wanted & found.keys()):
        if found[address].get("actions") != ["update"]:
            continue
        rule = DELTAS.get(address)
        reason = rule(found[address]) if rule is not None else "no rule says what may change in it"
        if reason:
            problems.append(f"{address}: {reason}")
    return problems


def file_sha256(path: Path) -> str:
    try:
        return sha256(path.read_bytes())
    except OSError:
        raise Stopped("plan shape: the plan could not be read") from None


def command_approve_plan(settings: Settings, plan_file: Path, updates: list[str]) -> None:
    """Accept a saved plan only when it is the prefix change and nothing else, and print the SHA-256 of that file for the sheet to bind its confirmation to."""
    path = plan_file.resolve()
    digest = file_sha256(path)
    code, output = Terraform(settings).call(["show", "-json", str(path)])
    try:
        plan = json.loads(output) if code == 0 else None
    except ValueError:
        plan = None
    if not isinstance(plan, dict) or plan.get("errored") or file_sha256(path) != digest:
        raise Stopped("plan shape: the plan could not be read")
    problems = plan_shape_problems(plan, updates)
    if problems:
        raise Stopped(f"plan shape: refused ({'; '.join(problems)})")
    say("plan shape", f"ok ({len(set(updates))} updates in place and nothing else)")
    say("plan sha256", digest)


def restore(settings: Settings, terraform: Terraform, data: bytes, marker_before: bytes | None) -> bool:
    """Put the stack back as it was: no backend file, the local state with its original bytes, the backend settings Terraform cached before the switch, and a
    local command that reads the state again. Recovery is reported, and True returned, only after that command read the original state."""
    marker = marker_path(settings)
    (settings.folder / "backend.tf").unlink(missing_ok=True)
    try:
        if not settings.local_state.is_file() or settings.local_state.read_bytes() != data:
            settings.local_state.write_bytes(data)
        terraform.init(remote=False, path_style=False)
        if marker_before is None:
            marker.unlink(missing_ok=True)
        else:
            marker.write_bytes(marker_before)
    except OSError:
        say("backend", "restore failed: copy the state back from the record folder")
        return False
    raw = terraform.pull()
    if raw is None or fingerprint(raw) != fingerprint(data):
        say(
            "backend",
            "restore failed: a local command does not read the original state; copy it back from the record folder and run terraform init -reconfigure",
        )
        return False
    say("backend", "restored")
    return True


def record_fingerprint(record: dict[str, Any]) -> Fingerprint:
    try:
        return Fingerprint(record["serial"], record["lineage"], record["resources"], record["instances"])
    except KeyError:
        raise Stopped("record: cannot be read") from None


def resume_switched(settings: Settings, record: dict[str, Any], loopback: bool) -> None:
    """A run that stopped after the switch and before its last record: finish it only when the backend and the bucket still hold the recorded state."""
    expected = record_fingerprint(record)
    bucket = Bucket(settings, loopback)
    bucket.identity()
    raw = Terraform(settings).pull()
    key = state_key(settings.stack)
    if raw is None or fingerprint(raw) != expected or not bucket.exists(key) or sha256(bucket.read(key)) != record.get("sha256"):
        raise Stopped("backend: switched, but the state read through it is not the recorded one; follow the rollback in docs/terraform_state_migration.md")
    write_record(settings, "done", expected, str(record["sha256"]))
    say("backend", "switched (resumed from the record)")
    say("migration", f"done (serial {expected.serial}, lineage {expected.lineage}, {expected.resources} resources)")


def command_migrate(settings: Settings, loopback: bool) -> None:
    record = read_record(settings)
    if record is not None and record.get("stage") == "done":
        raise Stopped("migration: already done for this stack")
    if record is not None and record.get("stage") == "copied" and backend_switched(settings):
        resume_switched(settings, record, loopback)
        return
    data, local = read_local(settings)
    bucket = Bucket(settings, loopback)
    remote = preflight(settings, bucket, data)
    if not settings.changes("before").is_file():
        raise Stopped("plan before: missing (run the plan step first)")
    digest = sha256(data)
    write_private(settings.backup, data)
    if sha256(settings.backup.read_bytes()) != digest:
        raise Stopped("local state: the backup in the record folder differs")
    key = state_key(settings.stack)
    if remote == "absent":
        try:
            bucket.create(key, data)
        except (BotoCoreError, ClientError, OSError):
            raise Stopped("copy: failed (nothing was written; the local state is unchanged)") from None
        copied = bucket.read(key)
        if sha256(copied) != digest or fingerprint(copied) != local:
            raise Stopped(
                "copy: mismatch (the remote object does not read back as the local state; it was left in place and an administrator removes it before a rerun)"
            )
        say("copy", f"done ({local.text()})")
    else:
        say("copy", f"adopted (the remote object is the same state; {local.text()})")
    write_record(settings, "copied", local, digest)
    command_backend(settings)
    terraform = Terraform(settings)
    marker = marker_path(settings)
    marker_before = marker.read_bytes() if marker.is_file() else None
    pulled = None
    if terraform.init(remote=True, path_style=loopback):
        raw = terraform.pull()
        pulled = fingerprint(raw) if raw is not None else None
    if pulled != local:
        if not restore(settings, terraform, data, marker_before):
            raise Stopped(
                "backend: the state read through the backend does not equal the local state and the stack is NOT back on its local state; "
                "follow the rollback in docs/terraform_state_migration.md",
                EXIT_NOT_RESTORED,
            )
        raise Stopped("backend: the state read through the backend does not equal the local state; the stack is back on its local state")
    write_record(settings, "done", local, digest)
    say("backend", "switched")
    say("migration", f"done (serial {local.serial}, lineage {local.lineage}, {local.resources} resources)")


def command_verify(settings: Settings, loopback: bool) -> None:
    """Compare the state read through the backend and the object in the bucket with the record: serial, lineage, counts and the digest of the bytes."""
    record = read_record(settings)
    if record is None or record.get("stage") != "done":
        raise Stopped("verify: no finished migration record for this stack")
    pulled = Terraform(settings).pull()
    found = fingerprint(pulled) if pulled is not None else None
    if found != record_fingerprint(record):
        raise Stopped("verify: the state read through the backend differs from the record (serial, lineage or counts)")
    bucket = Bucket(settings, loopback)
    key = state_key(settings.stack)
    if not bucket.exists(key):
        raise Stopped("verify: the remote key is missing")
    if sha256(bucket.read(key)) != record.get("sha256"):
        raise Stopped("verify: the remote object differs from the migrated state (its digest does not equal the record)")
    if bucket.exists(f"{key}.tflock"):
        raise Stopped("verify: a lock file is left in the bucket")
    say("verify", f"equal (serial {found.serial}, {found.resources} resources)")
    before, after = settings.changes("before"), settings.changes("after")
    if not before.is_file() or not after.is_file():
        raise Stopped("plans: the plan before or the plan after is missing")
    if before.read_bytes() != after.read_bytes():
        raise Stopped("plans: differ (compare the two change lists in the record folder)")
    say("plans", "equal")


def parse(arguments: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="The repository root (the E2E uses a synthetic one).")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("backend", "check", "plan", "migrate", "verify", "approve-plan"):
        command = commands.add_parser(name)
        command.add_argument("--stack", choices=sorted(STACKS), required=True)
        command.add_argument("--admin-profile", help="The administrator profile; required for the iam stack and refused for every other stack.")
        if name == "plan":
            command.add_argument("--phase", choices=PHASES, required=True)
        if name == "approve-plan":
            command.add_argument("--plan", type=Path, required=True, help="The saved plan to inspect.")
            command.add_argument(
                "--update", action="append", required=True, metavar="ADDRESS", help="A resource address that the plan must update in place; give each one."
            )
    return parser.parse_args(arguments)


def run(arguments: list[str]) -> int:
    parsed = parse(arguments)
    try:
        settings = load_settings(parsed.root.resolve(), parsed.stack, parsed.admin_profile)
        loopback = endpoint_override(os.environ)
        if parsed.command == "backend":
            command_backend(settings)
        elif parsed.command == "check":
            command_check(settings, loopback)
        elif parsed.command == "plan":
            command_plan(settings, parsed.phase)
        elif parsed.command == "migrate":
            command_migrate(settings, loopback)
        elif parsed.command == "approve-plan":
            command_approve_plan(settings, parsed.plan, parsed.update)
        else:
            command_verify(settings, loopback)
    except Stopped as stop:
        sys.stdout.write(f"{stop.line}\n")
        return stop.code
    return 0


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
