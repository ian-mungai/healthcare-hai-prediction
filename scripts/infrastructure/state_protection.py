"""The state administrator the owner approved for the storage stack, kept so that every later storage plan and apply carries it.

The storage stack turns the state prefix of the project bucket on with two inputs, ``terraform_state_enabled`` and ``terraform_state_admin_principals``.
A storage plan rendered without them removes the bucket's state statements and lifecycle rules, so the wrapper (``scripts/infrastructure/terraform.sh``)
reads the approved administrator from ``plans/state_migration/state_protection.json`` (ignored, owner only) and passes it to the renderer on every storage
render. The record holds the administrator, a digest of the bucket and the account it was approved for (taken from ``.env``) and a status. Run from the
repository root:

    .venv/bin/python -m scripts.infrastructure.state_protection resolve [--state-admin role/NAME] [--live] [--disable]
    .venv/bin/python -m scripts.infrastructure.state_protection remember --state-admin role/NAME
    .venv/bin/python -m scripts.infrastructure.state_protection verify --state-admin role/NAME

``resolve`` prints the administrator the render must carry (empty when none was approved and none was given). It refuses a different administrator than
the approved one and a record that belongs to another bucket or account. With ``--live`` it also reads the live bucket policy: when the bucket already
holds the state statements and no verified record is on hand (a checkout without the record, or a record whose apply failed), a render without the
administrator would remove them, so it refuses; a policy that cannot be read refuses too. ``--disable`` skips the record and the live read, which is the
explicit way to turn the protection off on purpose (the wrapper's ``--disable-state-protection``). ``remember`` keeps the administrator as ``pending``
before the apply; a pending record is no approval. ``verify`` runs after the apply exited 0: it reads the live bucket policy and the lifecycle rule back,
and only when both carry the protection for that administrator does it mark the record ``verified``. To turn the protection off on purpose, apply with
``--disable-state-protection`` as its own owner step, then remove the record by hand. Exit code 0 is done, 2 is a refusal (the reason is on stderr).
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from scripts.infrastructure.render_project_config import ConfigurationError, load_configuration

REPO_ROOT = Path(__file__).resolve().parents[2]
RECORD = Path("plans/state_migration/state_protection.json")
# The same rule as the terraform_state_admin_principals validation in infra/variables.tf and the --state-admin option of the renderer.
STATE_ADMIN = re.compile(r"(user|role)/[A-Za-z0-9_+=,.@/-]+")
STATE_SIDS = ("ProjectUserUsesItsStateKeys", "ProjectUserHoldsItsStateLocks", "DenyStatePrefixToEveryoneElse", "DenyIamStateToProjectIdentities")
LIFECYCLE_RULE = "expire_replaced_state_versions"
STATE_PREFIX = "terraform_state"
STATE_STACKS = ("storage", "iam", "platform", "services", "jobs", "alerts")
STATE_RULE_PREFIX = f"{STATE_PREFIX}/"
STATE_RULE_DAYS = 90
OBJECT_STATEMENTS = STATE_SIDS[:2]
OBJECT_ACTIONS = ({"s3:GetObject", "s3:PutObject"}, {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"})
DENY_STATEMENTS = STATE_SIDS[2:]
NOT_KEYS = ("NotAction", "NotResource", "NotPrincipal")
PENDING, VERIFIED = "pending", "verified"
NO_POLICY = ("NoSuchBucketPolicy", "NoSuchBucket")
CLIENT_CONFIG = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=60)
LOOPBACK_CONFIG = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=60, s3={"addressing_style": "path"})


class Refused(Exception):
    """A refusal with a reason that is safe to print."""


@dataclass(frozen=True)
class Deployment:
    """The deployment the storage stack belongs to, from ``.env``: the profile that reads its bucket, the region, the bucket and the account."""

    profile: str
    region: str
    bucket: str
    account: str

    @property
    def digest(self) -> str:
        return deployment_digest(self.bucket, self.account)


def deployment_digest(bucket: str, account: str) -> str:
    """What binds a record to its deployment, without keeping the bucket name or the account ID in it."""
    return hashlib.sha256(f"{account}/{bucket}".encode()).hexdigest()


def load_deployment(root: Path) -> Deployment:
    try:
        storage, _ = load_configuration(root / ".env", None)
    except ConfigurationError as error:
        raise Refused(f"settings: {error}") from None
    return Deployment(storage["aws_profile"], storage["aws_region"], storage["data_bucket_name"], storage["expected_account_id"])


def valid(name: object) -> bool:
    """True for role/NAME or user/NAME with no wildcard, no full ARN and no 12-digit account ID."""
    return isinstance(name, str) and STATE_ADMIN.fullmatch(name) is not None and re.search(r"[0-9]{12}", name) is None


def load(path: Path, digest: str) -> tuple[str, str] | None:
    """The administrator and the status of the record, or None when no record exists. An unreadable, invalid or foreign record is a refusal."""
    if not path.is_file():
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        names, status, recorded = document["terraform_state_admin_principals"], document["status"], document["deployment"]
    except (OSError, ValueError, KeyError, TypeError):
        raise Refused(f"the approved state protection inputs cannot be read ({path}); repair or remove the record") from None
    if not isinstance(names, list) or len(names) != 1 or not valid(names[0]) or status not in (PENDING, VERIFIED) or not isinstance(recorded, str):
        raise Refused(f"the approved state protection inputs are not valid ({path}): one role/NAME or user/NAME, a status and a deployment are expected")
    if recorded != digest:
        raise Refused(f"the approved state protection inputs belong to another bucket or account ({path}); repair or remove the record")
    return str(names[0]), str(status)


def read(path: Path, digest: str) -> str | None:
    """The approved administrator: that of a verified record. No record and a pending record (an apply that was not read back) give None."""
    found = load(path, digest)
    return found[0] if found is not None and found[1] == VERIFIED else None


def write(path: Path, principal: str, digest: str, status: str) -> None:
    """Keep the administrator, owner only. The write replaces the file in one step."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.writing")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"terraform_state_admin_principals": [principal], "deployment": digest, "status": status}, indent=2) + "\n")
    os.replace(temporary, path)


def s3_client(target: Deployment) -> Any:
    session = boto3.Session(profile_name=target.profile, region_name=target.region)
    return session.client("s3", config=LOOPBACK_CONFIG if os.environ.get("AWS_ENDPOINT_URL_S3") else CLIENT_CONFIG)


def live_statements(target: Deployment) -> list[dict[str, Any]]:
    """The statements of the live bucket policy. A bucket without a policy has none; any other failure to read it is a refusal."""
    try:
        document = json.loads(s3_client(target).get_bucket_policy(Bucket=target.bucket)["Policy"])
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in NO_POLICY:
            return []
        raise Refused("the live bucket policy could not be read, so the state protection cannot be checked") from None
    except (BotoCoreError, ValueError, KeyError):
        raise Refused("the live bucket policy could not be read, so the state protection cannot be checked") from None
    raw = document.get("Statement", []) if isinstance(document, dict) else []
    return [statement for statement in raw if isinstance(statement, dict)] if isinstance(raw, list) else []


def protected(target: Deployment) -> bool:
    """True when the live bucket policy holds any state statement of the storage stack."""
    return any(statement.get("Sid") in STATE_SIDS for statement in live_statements(target))


def state_key(stack: str) -> str:
    return f"{STATE_PREFIX}/{stack}/terraform.tfstate"


def listed(value: object) -> list[str]:
    """The strings of a policy field that may be one string or a list of them."""
    if isinstance(value, str):
        return [value]
    return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []


def invalid_policy(sid: str, reason: str) -> Refused:
    return Refused(f"bucket policy: state statements invalid ({sid}: {reason})")


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
        raise Refused("bucket policy: state statements missing (the state key of this stack is not allowed)")


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


def check_policy(statements: list[dict[str, Any]], bucket: str, stack: str, administrator: bool, user: str, principal: str) -> None:
    """Check each state statement of a bucket policy for what it does (its effect, principal, actions, resources and deny conditions) for one stack, whose
    caller is ``principal``. ``administrator`` is True for the iam stack, which has no allow statements of its own."""
    required = DENY_STATEMENTS if administrator else (*OBJECT_STATEMENTS, *DENY_STATEMENTS)
    found = {sid: [statement for statement in statements if statement.get("Sid") == sid] for sid in required}
    if any(not matching for matching in found.values()):
        raise Refused("bucket policy: state statements missing")
    for sid, matching in found.items():
        if len(matching) > 1:
            raise invalid_policy(sid, "the Sid appears more than once")
    prefix = f"arn:aws:s3:::{bucket}/{STATE_PREFIX}"
    if not administrator:
        key = f"{prefix}/{stack}/terraform.tfstate"
        for sid, actions, wanted in zip(OBJECT_STATEMENTS, OBJECT_ACTIONS, (key, f"{key}.tflock"), strict=True):
            check_allow(found[sid][0], sid, actions, user, wanted)
    check_deny(found[DENY_STATEMENTS[0]][0], DENY_STATEMENTS[0], f"{prefix}/*", "ArnNotLike", [principal], [])
    check_deny(found[DENY_STATEMENTS[1]][0], DENY_STATEMENTS[1], f"{prefix}/iam/*", "ArnLike", [user], [principal] if administrator else [])
    reached = [f"{prefix}/{stack}/terraform.tfstate", f"{prefix}/{stack}/terraform.tfstate.tflock"]
    for statement in statements:
        unconditional = statement.get("Effect") == "Deny" and "Condition" not in statement and statement.get("Sid") not in required
        if unconditional and any(fnmatch.fnmatchcase(key, pattern) for key in reached for pattern in listed(statement.get("Resource"))):
            raise invalid_policy(str(statement.get("Sid", "?")), "an unconditional deny reaches this stack's state key")


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
    return [name for stack in STATE_STACKS for name in (state_key(stack), f"{state_key(stack)}.tflock")]


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


def check_lifecycle(rules: list[dict[str, Any]]) -> None:
    """Check the lifecycle rules for what they do: the rule of replaced state versions is the one of ``infra/main.tf`` and no other rule expires state."""
    mine = [rule for rule in rules if rule.get("ID") == LIFECYCLE_RULE]
    if not mine:
        raise Refused("lifecycle: rule for replaced state versions missing")
    problem = "the rule appears more than once" if len(mine) > 1 else check_state_rule(mine[0])
    if problem:
        raise Refused(f"lifecycle: rule for replaced state versions invalid ({problem})")
    for rule in rules:
        if rule.get("ID") != LIFECYCLE_RULE and expires_state(rule):
            raise Refused(f"lifecycle: another rule can expire the current state or lock object of a stack ({rule.get('ID', '?')})")


def policy_back(target: Deployment, principal: str) -> None:
    """Refuse unless the live bucket policy passes the same detailed check as the sheet's final check for the storage stack and names the administrator in the
    prefix deny."""
    statements = live_statements(target)
    user = f"arn:aws:iam::{target.account}:user/{target.profile}"
    check_policy(statements, target.bucket, "storage", False, user, user)
    found = {statement.get("Sid"): statement for statement in statements}
    condition = found["DenyStatePrefixToEveryoneElse"].get("Condition", {})
    names = condition.get("ArnNotLike", {}).get("aws:PrincipalArn", []) if isinstance(condition, dict) else []
    names = [names] if isinstance(names, str) else names
    if f"arn:aws:iam::{target.account}:{principal}" not in names:
        raise Refused("the live bucket policy does not except the approved state administrator after the apply")


def lifecycle_back(target: Deployment) -> None:
    """Refuse unless the live lifecycle rules pass the same detailed check as the sheet's final check."""
    try:
        rules = s3_client(target).get_bucket_lifecycle_configuration(Bucket=target.bucket).get("Rules", [])
    except (BotoCoreError, ClientError):
        raise Refused("the live lifecycle rules could not be read, so the state protection cannot be confirmed") from None
    check_lifecycle([rule for rule in rules if isinstance(rule, dict)])


def read_back(target: Deployment, principal: str) -> None:
    """Refuse unless the live policy and lifecycle rules pass the detailed checks of the sheet's final check and the prefix deny names the administrator."""
    policy_back(target, principal)
    lifecycle_back(target)


def resolve(root: Path, given: str | None, live: bool = False, disable: bool = False) -> str:
    """The administrator a storage render must carry: the approved one, or the given one when none is approved yet. With ``live`` a render without one is
    refused when the live bucket already holds the protection, unless ``disable`` asks for it to go."""
    if given is not None and not valid(given):
        raise Refused("--state-admin takes role/NAME or user/NAME with no wildcard, no full ARN and no 12-digit account ID")
    if disable:
        if given is not None:
            raise Refused("--disable-state-protection cannot be combined with --state-admin")
        return ""
    path = root / RECORD
    target = load_deployment(root) if live or path.is_file() else None
    approved = read(path, target.digest) if target is not None else None
    if approved is not None and given is not None and given != approved:
        raise Refused("--state-admin differs from the approved state administrator on record; the state protection is not changed here")
    chosen = given or approved or ""
    if not chosen and live and target is not None and protected(target):
        raise Refused(
            "the live bucket already holds the state protection and no verified state administrator is on record here, so this plan or apply would remove it. "
            "Plan with --state-admin role/NAME, or pass --disable-state-protection to remove the protection on purpose"
        )
    return chosen


def remember(root: Path, given: str) -> None:
    """Keep the administrator as pending before the apply. A verified record of the same administrator stays verified; a pending one is no approval."""
    chosen = resolve(root, given)
    target = load_deployment(root)
    found = load(root / RECORD, target.digest)
    if found is not None and found == (chosen, VERIFIED):
        return
    write(root / RECORD, chosen, target.digest, PENDING)


def verify(root: Path, given: str) -> None:
    """After an apply that exited 0: mark the record verified only when the live bucket carries the protection for the administrator."""
    if not valid(given):
        raise Refused("--state-admin takes role/NAME or user/NAME with no wildcard, no full ARN and no 12-digit account ID")
    target = load_deployment(root)
    found = load(root / RECORD, target.digest)
    if found is None or found[0] != given:
        raise Refused("no state protection record of this administrator was kept before the apply")
    read_back(target, given)
    write(root / RECORD, given, target.digest, VERIFIED)


def main(arguments: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("resolve", "remember", "verify"))
    parser.add_argument("--state-admin")
    parser.add_argument("--live", action="store_true", help="resolve: also read the live bucket policy and refuse a render that would remove the protection.")
    parser.add_argument("--disable", action="store_true", help="resolve: the owner turns the protection off on purpose.")
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="The repository root (the E2E uses a synthetic one).")
    parsed = parser.parse_args(arguments)
    try:
        if parsed.command in ("remember", "verify"):
            if parsed.state_admin is None:
                raise Refused(f"{parsed.command} needs --state-admin")
            (remember if parsed.command == "remember" else verify)(parsed.root, parsed.state_admin)
        else:
            sys.stdout.write(f"{resolve(parsed.root, parsed.state_admin, parsed.live, parsed.disable)}\n")
    except Refused as refusal:
        sys.stderr.write(f"state protection: {refusal}\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
