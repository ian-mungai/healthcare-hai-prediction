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
- ``check`` reads only: the identity of the profile, the local state, the version history of the bucket, the state statements of its policy, the
  lifecycle rule of replaced state versions, and the remote key and its lock.
- ``plan --phase before|after`` runs the project wrapper's plan, keeps the sorted change list in the record folder and deletes the plan file (it holds
  values).
- ``migrate`` needs the plan before, runs the check, backs up the local state, copies it with a write that refuses an existing key, reads it back and
  compares the bytes, switches the backend with ``terraform init -reconfigure`` (equal states, so Terraform moves nothing itself and empties the local
  file) and reads the state back through the backend. The serial, the lineage and the resource count must equal the local ones at every step. A stop
  before the switch leaves the local state and the stack folder as they were. A stop after it puts the backend and the local file back.
- ``verify`` reads the state through the backend again, compares it with the record and compares the plan after with the plan before.

A stack that finished is never migrated again: its record says ``done`` and its folder holds the switched backend. A remote key that holds other bytes
than the local state is refused. The bytes of the same state are adopted, which resumes a run that stopped between the copy and the record. Output is
``<subject>: <status>`` lines only. Exit code 0 means the step is done, 1 means it stopped and 2 means a bad argument. Rollback: see
``docs/terraform_state_migration.md``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from scripts.infrastructure.render_project_config import ConfigurationError, load_configuration
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
WRAPPER_SCRIPT = (
    'options=(); [ -z "$WRAPPER_STACK" ] || options+=("$WRAPPER_STACK"); [ -z "$WRAPPER_ADMIN" ] || options+=(--admin-profile "$WRAPPER_ADMIN"); '
    'exec bash scripts/infrastructure/terraform.sh "${options[@]}" plan -detailed-exitcode -out "$WRAPPER_PLAN"'
)
LIFECYCLE_RULE = "expire_replaced_state_versions"
OBJECT_STATEMENTS = ("ProjectUserUsesItsStateKeys", "ProjectUserHoldsItsStateLocks")
DENY_STATEMENTS = ("DenyStatePrefixToEveryoneElse", "DenyIamStateToProjectIdentities")
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


class Bucket:
    """The S3 and STS calls of one stack, signed by its profile. A failed call raises Stopped with a status word and never prints the AWS answer."""

    def __init__(self, settings: Settings, loopback: bool) -> None:
        self.settings = settings
        self.loopback = loopback
        session = boto3.Session(profile_name=settings.profile, region_name=settings.region)
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
        settings = self.settings
        try:
            document = json.loads(self.s3.get_bucket_policy(Bucket=settings.bucket)["Policy"])
        except (BotoCoreError, ClientError, ValueError, KeyError):
            raise Stopped("bucket policy: state statements missing (the policy could not be read)") from None
        statements = {statement.get("Sid"): statement for statement in document.get("Statement", []) if isinstance(statement, dict)}
        missing = [sid for sid in DENY_STATEMENTS if sid not in statements]
        if missing:
            raise Stopped("bucket policy: state statements missing")
        if not STACKS[settings.stack].administrator:
            key = f"arn:aws:s3:::{settings.bucket}/{state_key(settings.stack)}"
            for sid, wanted in zip(OBJECT_STATEMENTS, (key, f"{key}.tflock"), strict=True):
                resources = statements.get(sid, {}).get("Resource", [])
                if wanted not in ([resources] if isinstance(resources, str) else resources):
                    raise Stopped("bucket policy: state statements missing (the state key of this stack is not allowed)")
        say("bucket policy", "state statements present")

    def lifecycle(self) -> None:
        try:
            rules = self.s3.get_bucket_lifecycle_configuration(Bucket=self.settings.bucket).get("Rules", [])
        except (BotoCoreError, ClientError):
            raise Stopped("lifecycle: rule for replaced state versions missing (the rules could not be read)") from None
        if LIFECYCLE_RULE not in {rule.get("ID") for rule in rules}:
            raise Stopped("lifecycle: rule for replaced state versions missing")
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


def read_local(settings: Settings) -> tuple[bytes, Fingerprint]:
    """The local state of the stack as bytes, or a stop. A switched backend, a missing, empty or invalid file all stop here."""
    marker = settings.folder / ".terraform" / "terraform.tfstate"
    if marker.is_file():
        try:
            if json.loads(marker.read_text(encoding="utf-8")).get("backend", {}).get("type") == "s3":
                raise Stopped("local state: backend already switched")
        except (ValueError, AttributeError, OSError):
            raise Stopped("local state: the backend marker cannot be read") from None
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
    data, _ = read_local(settings)
    preflight(settings, Bucket(settings, loopback), data)
    say("check", "ok")


def changes_of(plan: dict[str, Any]) -> list[str]:
    lines = []
    for change in plan.get("resource_changes", []):
        actions = change.get("change", {}).get("actions", [])
        if actions not in (["no-op"], ["read"]):
            lines.append(f"{change.get('address', '?')}: {'+'.join(actions)}")
    return sorted(lines)


def command_plan(settings: Settings, phase: str) -> None:
    """Plan through the project wrapper (it renders the inputs and verifies the identity) and keep only the sorted change list."""
    plan_file = settings.record_dir / f"{settings.stack}_{phase}.tfplan"
    settings.record_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    environment = {
        **os.environ,
        "WRAPPER_STACK": next(iter(STACKS[settings.stack].wrapper), ""),
        "WRAPPER_ADMIN": settings.profile if STACKS[settings.stack].administrator else "",
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
            lines = changes_of(json.loads(output)) if code == 0 else None
        except ValueError:
            lines = None
        if lines is None:
            raise Stopped(f"plan {phase}: the plan could not be read")
    finally:
        plan_file.unlink(missing_ok=True)
    write_private(settings.changes(phase), "".join(f"{line}\n" for line in lines).encode())
    say(f"plan {phase}", "no change" if not lines else f"{len(lines)} change(s) (list in {settings.changes(phase).relative_to(settings.root)})")


def restore(settings: Settings, terraform: Terraform, data: bytes) -> None:
    """Put the stack back as it was: no backend file, the local state with its original bytes and a local backend."""
    (settings.folder / "backend.tf").unlink(missing_ok=True)
    try:
        if not settings.local_state.is_file() or settings.local_state.read_bytes() != data:
            settings.local_state.write_bytes(data)
    except OSError:
        say("backend", "restore failed: copy the state back from the record folder")
        return
    say(
        "backend", "restored" if terraform.init(remote=False, path_style=False) else "restored (the local init did not finish: run terraform init -reconfigure)"
    )


def command_migrate(settings: Settings, loopback: bool) -> None:
    record = read_record(settings)
    if record is not None and record.get("stage") == "done":
        raise Stopped("migration: already done for this stack")
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
    pulled = None
    if terraform.init(remote=True, path_style=loopback):
        raw = terraform.pull()
        pulled = fingerprint(raw) if raw is not None else None
    if pulled != local:
        restore(settings, terraform, data)
        raise Stopped("backend: the state read through the backend does not equal the local state; the stack is back on its local state")
    write_record(settings, "done", local, digest)
    say("backend", "switched")
    say("migration", f"done (serial {local.serial}, lineage {local.lineage}, {local.resources} resources)")


def command_verify(settings: Settings, loopback: bool) -> None:
    record = read_record(settings)
    if record is None or record.get("stage") != "done":
        raise Stopped("verify: no finished migration record for this stack")
    terraform = Terraform(settings)
    pulled = terraform.pull()
    found = fingerprint(pulled) if pulled is not None else None
    expected = Fingerprint(record["serial"], record["lineage"], record["resources"], record["instances"])
    if found is None or found.lineage != expected.lineage or found.resources != expected.resources or found.instances != expected.instances:
        raise Stopped("verify: the state read through the backend differs from the record")
    if found.serial < expected.serial:
        raise Stopped("verify: the state read through the backend is older than the record")
    bucket = Bucket(settings, loopback)
    if not bucket.exists(state_key(settings.stack)):
        raise Stopped("verify: the remote key is missing")
    if bucket.exists(f"{state_key(settings.stack)}.tflock"):
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
    for name in ("backend", "check", "plan", "migrate", "verify"):
        command = commands.add_parser(name)
        command.add_argument("--stack", choices=sorted(STACKS), required=True)
        command.add_argument("--admin-profile", help="The administrator profile; required for the iam stack and refused for every other stack.")
        if name == "plan":
            command.add_argument("--phase", choices=PHASES, required=True)
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
        else:
            command_verify(settings, loopback)
    except Stopped as stop:
        sys.stdout.write(f"{stop.line}\n")
        return stop.code
    return 0


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
