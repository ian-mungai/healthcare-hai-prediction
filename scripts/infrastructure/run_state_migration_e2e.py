"""E2E of the Terraform state migration (``scripts.infrastructure.state_migration``) against real Terraform and a loopback stand-in of the project bucket.

    .venv/bin/python -m scripts.infrastructure.run_state_migration_e2e --out <new folder>

Each case builds a synthetic repository root with the six stacks (storage, iam, platform, services, jobs, alerts). Every stack holds one or more built-in
``terraform_data`` resources and a real local state that Terraform wrote. The stand-in answers S3 and STS on the loopback address
(``state_migration_s3_stand_in``): it tells the administrator key from the project key and answers the iam stack's objects to the administrator key only and
every other stack's objects to the project key only, as the bucket policy does. Nothing leaves the machine, no real AWS file is read and no state of the
project is touched. The cases are:

- a full run of all six stacks: the written backend, the preflight, the plan before, the copy, the switch, the plan after and the verification, with the lock
  file written next to the state and every request signed by the key of the profile that applies the stack (the iam stack's by the administrator only);
- the second run of a finished stack refused, also after its record is lost, and no second write;
- a copy that is interrupted (the connection drops) and a copy that reads back wrong (the stack stops, writes no backend and a retry then adopts the
  identical copy);
- a remote object that differs, a lock held, a suspended version history, a missing state statement, a missing lifecycle rule, a missing, empty or invalid
  local state and a wrong account, each refused with nothing changed;
- the profile split: the iam stack refuses a missing administrator profile and the project profile, every other stack refuses an administrator profile;
- a local state file that is a symbolic link (the run tree links to the state of another checkout);
- the backend text: valid, no bucket, account, region or profile in it, and the same state keys as the bucket policy of ``infra/main.tf``;
- the state protection of the storage stack fails closed: no record and a protected live bucket refuse a render unless the disable flag is given, a pending
  record is no approval until the read-back marks it verified, and a record of another bucket or account is refused;
- a lifecycle rule that can expire the state or lock object of any stack is refused (prefix and tag filters), and one that cannot is accepted;
- the shape check of the prefix plan: moves, imports, output changes and any change inside the two resources besides the state statements and rules;
- a stop after the switch resumes from the sheet's first step, and a failed restore ends with its own exit code and no success line;
- the record says ``switched`` after the move and ``done`` only after the verification, so a stop between them resumes at the plan after in both move sheets
  (the project profile sheet and the iam sheet, run as delivered) without copying the state again;
- the prefix plan as Terraform writes it (``state_migration_native_plans.json``: the plans of a first apply, of a retry after the policy was applied and of
  a retry after both updates, from ``terraform show -json`` of the two resources of ``infra/main.tf`` with Terraform 1.16.4, provider 6.66.0, offline and
  synthetic credentials) and the recovery of a pending record: an update that a partial apply already made must be live in the bucket, the rest is checked
  as before.

``--cases NAME[,NAME]`` runs only the named cases (a name is the function name); the default is all of them.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from unittest import mock

from scripts.infrastructure import state_migration
from scripts.infrastructure.state_migration import RECORD_FOLDER, STACKS, backend_text, plan_shape_problems
from scripts.infrastructure.state_migration_s3_stand_in import ACCOUNT, BUCKET, STATE_RULE, StateBucket, state_policy
from scripts.infrastructure.state_protection import PENDING, VERIFIED, deployment_digest, load_deployment
from scripts.infrastructure.state_protection import RECORD as STATE_PROTECTION_RECORD
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
ORDER = ("storage", "jobs", "platform", "services", "alerts", "iam")
PROJECT_PROFILE = "example_project_dev"
ADMIN_PROFILE = "example_admin"
KEYS = {"AKIAPROJECTSTANDIN": "project", "AKIAADMINSTANDIN": "administrator"}
IDENTITIES = {
    "project": (ACCOUNT, f"arn:aws:iam::{ACCOUNT}:user/{PROJECT_PROFILE}"),
    "administrator": (ACCOUNT, f"arn:aws:iam::{ACCOUNT}:user/{ADMIN_PROFILE}"),
}
RESOURCES = {"storage": 2, "iam": 3, "platform": 1, "services": 2, "jobs": 1, "alerts": 1}
STEP_SECONDS = 180
FOLDERS = {stack: STACKS[stack].folder for stack in STACKS}
WRAPPER = """#!/usr/bin/env bash
# Stand-in of scripts/infrastructure/terraform.sh: the same stack options, the plan of the stack folder, and a log of the options it was given.
set -euo pipefail
cd "$(dirname "$0")/../.."
stack=infra
admin=""
state_admin=""
log="$PWD/wrapper_calls.log"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --iam | --platform | --services | --jobs | --alerts) stack="infra/${1#--}"; shift ;;
    --admin-profile) admin="$2"; shift 2 ;;
    --state-admin) state_admin="$2"; shift 2 ;;
    *) break ;;
  esac
done
[[ "$stack" != infra/iam || -n "$admin" ]] || { echo "IAM updates require --admin-profile." >&2; exit 2; }
printf '%s admin=%s state_admin=%s\\n' "$stack" "${admin:-none}" "${state_admin:-none}" >> "$log"
if [[ "$stack" == infra ]]; then
  if [[ -n "$state_admin" ]]; then
    printf '{"terraform_state_enabled": true, "terraform_state_admin_principals": ["%s"]}\\n' "$state_admin" > "$stack/deployment.auto.tfvars.json"
  else
    rm -f "$stack/deployment.auto.tfvars.json"
  fi
fi
exec terraform -chdir="$stack" "$@" -input=false
"""


def text_of(path: Path) -> str | None:
    return path.read_text(encoding="utf-8") if path.is_file() else None


STATE_VARIABLES = (
    'variable "terraform_state_enabled" {\n  type    = bool\n  default = false\n}\n\n'
    'variable "terraform_state_admin_principals" {\n  type    = list(string)\n  default = []\n}\n\n'
)


def main_tf(count: int, storage: bool = False) -> str:
    resources = "\n".join(f'resource "terraform_data" "r{index}" {{\n  input = "value{index}"\n}}\n' for index in range(count))
    return f'terraform {{\n  required_version = "~> 1.16.1"\n}}\n\n{STATE_VARIABLES if storage else ""}{resources}'


class Scene:
    """One synthetic repository root and its stand-in bucket."""

    def __init__(self, root: Path, bucket: StateBucket | None) -> None:
        self.root = root
        self.stand_in = bucket

    @property
    def bucket(self) -> StateBucket:
        if self.stand_in is None:
            raise RuntimeError("the case has not started its stand-in bucket")
        return self.stand_in

    @bucket.setter
    def bucket(self, value: StateBucket) -> None:
        self.stand_in = value

    @staticmethod
    def template(path: Path) -> None:
        """Build the root once: .env, AWS files, the wrapper stand-in and six stacks with a state that Terraform wrote."""
        (path / "scripts/infrastructure").mkdir(parents=True)
        (path / "plans").mkdir()
        (path / ".env").write_text(
            f"AWS_REGION=us-west-2\nAWS_PROFILE={PROJECT_PROFILE}\nAWS_ACCOUNT_ID={ACCOUNT}\nPROJECT_NAME=example_project\nS3_BUCKET={BUCKET}\nENVIRONMENT=dev\n",
            encoding="utf-8",
        )
        (path / "home").mkdir()
        (path / "home/credentials").write_text(
            "".join(
                f"[{profile}]\naws_access_key_id={key}\naws_secret_access_key=stand-in\n"
                for key, profile in zip(KEYS, (PROJECT_PROFILE, ADMIN_PROFILE), strict=True)
            ),
            encoding="utf-8",
        )
        (path / "home/config").write_text("".join(f"[profile {profile}]\nregion=us-west-2\n" for profile in (PROJECT_PROFILE, ADMIN_PROFILE)), encoding="utf-8")
        wrapper = path / "scripts/infrastructure/terraform.sh"
        wrapper.write_text(WRAPPER, encoding="utf-8")
        wrapper.chmod(0o755)
        for stack, count in RESOURCES.items():
            folder = path / FOLDERS[stack]
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "main.tf").write_text(main_tf(count, storage=stack == "storage"), encoding="utf-8")
            for arguments in (["init", "-input=false"], ["apply", "-auto-approve", "-input=false"]):
                result = run_command("terraform", [f"-chdir={folder}", *arguments], timeout=STEP_SECONDS)
                if result.returncode:
                    raise RuntimeError(f"building the {stack} stack failed: {result.stdout[-400:]}{result.stderr[-400:]}")
            for leftover in folder.glob("terraform.tfstate.backup"):
                leftover.unlink()

    def environment(self) -> dict[str, str]:
        endpoints = {"AWS_ENDPOINT_URL_S3": self.stand_in.endpoint, "AWS_ENDPOINT_URL_STS": self.stand_in.endpoint} if self.stand_in is not None else {}
        return {
            **endpoints,
            "PATH": os.environ["PATH"],
            "HOME": str(self.root / "home"),
            "AWS_CONFIG_FILE": str(self.root / "home/config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(self.root / "home/credentials"),
            "AWS_PROFILE": "no_such_profile",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE": "IPv4",
            "PYTHONDONTWRITEBYTECODE": "1",
        }

    def run(self, command: str, stack: str, *extra: str, environment: dict[str, str] | None = None) -> tuple[int, str]:
        arguments = ["-m", "scripts.infrastructure.state_migration", "--root", str(self.root), command, "--stack", stack, *extra]
        result = run_command(sys.executable, arguments, cwd=ROOT, env={**self.environment(), **(environment or {})}, timeout=STEP_SECONDS)
        return result.returncode, result.stdout + result.stderr

    def admin(self, stack: str) -> list[str]:
        return ["--admin-profile", ADMIN_PROFILE] if stack == "iam" else []

    def state_file(self, stack: str) -> Path:
        return self.root / FOLDERS[stack] / "terraform.tfstate"

    def key(self, stack: str) -> str:
        return f"terraform_state/{stack}/terraform.tfstate"

    def record(self, stack: str) -> Path:
        return self.root / RECORD_FOLDER / f"{stack}.json"

    def approve_state_admin(self, principal: str = "role/example_admin", status: str = "verified", digest: str | None = None) -> None:
        """Record the state administrator the owner approved for the storage stack, as the apply sheet does."""
        path = self.root / STATE_PROTECTION_RECORD
        path.parent.mkdir(parents=True, exist_ok=True)
        document = {"terraform_state_admin_principals": [principal], "deployment": digest or load_deployment(self.root).digest, "status": status}
        path.write_text(json.dumps(document) + "\n", encoding="utf-8")

    def protection(self, command: str, *extra: str) -> tuple[int, str]:
        """The state protection tool of the wrapper, run against this scene's root and stand-in bucket."""
        arguments = ["-m", "scripts.infrastructure.state_protection", command, "--root", str(self.root), *extra]
        result = run_command(sys.executable, arguments, cwd=ROOT, env=self.environment(), timeout=STEP_SECONDS)
        return result.returncode, result.stdout + result.stderr

    def protect(self) -> None:
        """Give the stand-in bucket the state protection the migration requires: the version history, the lifecycle rule and the state statements of the bucket
        policy of ``infra/main.tf`` (effects, principals, actions, resources and deny conditions)."""
        self.bucket.versioning = "Enabled"
        self.bucket.lifecycle = ["abort_incomplete_uploads", STATE_RULE]
        self.bucket.policy = state_policy(
            BUCKET,
            f"arn:aws:iam::{ACCOUNT}:user/{PROJECT_PROFILE}",
            f"arn:aws:iam::{ACCOUNT}:user/{ADMIN_PROFILE}",
            f"arn:aws:iam::{ACCOUNT}:role/example_project_gh_terraform_plan_dev",
        )

    def statements(self) -> list[dict[str, Any]]:
        """The statements of the stand-in's policy, for a case to change in place."""
        if self.bucket.policy is None:
            raise RuntimeError("the case has not protected its bucket")
        return cast(list[dict[str, Any]], self.bucket.policy["Statement"])

    def statement(self, sid: str) -> dict[str, Any]:
        return next(statement for statement in self.statements() if statement["Sid"] == sid)


def fingerprint(path: Path) -> tuple[int, str, int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data["serial"], data["lineage"], len(data["resources"])


def expect(problems: list[str], condition: bool, text: str) -> None:
    if not condition:
        problems.append(text)


def fresh(template: Path, work: Path, name: str) -> Scene:
    destination = work / name
    shutil.copytree(template, destination, symlinks=True)
    return Scene(destination, None)


def full_run(scene: Scene, problems: list[str], report: dict[str, object]) -> None:
    """All six stacks, in the order of the owner sheet."""
    originals = {stack: scene.state_file(stack).read_bytes() for stack in ORDER}
    scene.approve_state_admin()
    for stack in ORDER:
        label = f"[{stack}]"
        code, output = scene.run("backend", stack, *scene.admin(stack))
        expect(problems, code == 0 and text_of(scene.root / FOLDERS[stack] / "backend.tf") == backend_text(stack), f"{label} backend: exit {code}: {output}")
        scene.root.joinpath(FOLDERS[stack], "backend.tf").unlink(missing_ok=True)
        code, output = scene.run("check", stack, *scene.admin(stack))
        expect(problems, code == 0 and "remote key: absent" in output, f"{label} check: exit {code}: {output}")
        code, output = scene.run("migrate", stack, *scene.admin(stack))
        expect(problems, code == 1 and "plan before: missing" in output, f"{label} migrate without a plan before must stop: exit {code}: {output}")
        expect(problems, scene.key(stack) not in scene.bucket.objects, f"{label} migrate without a plan before wrote the remote state")
        code, output = scene.run("plan", stack, "--phase", "before", *scene.admin(stack))
        expect(problems, code == 0 and "plan before: no change" in output, f"{label} plan before: exit {code}: {output}")
        code, output = scene.run("migrate", stack, *scene.admin(stack))
        expect(
            problems,
            code == 0 and "copy: done" in output and "backend: switched" in output and "migration: done" in output,
            f"{label} migrate: exit {code}: {output}",
        )
        expect(problems, scene.bucket.objects.get(scene.key(stack)) == originals[stack], f"{label} the remote state differs from the local state it came from")
        backup = scene.root / RECORD_FOLDER / f"{stack}.local_state.json"
        expect(problems, backup.is_file() and backup.read_bytes() == originals[stack], f"{label} the local backup in the record folder is missing or differs")
        expect(problems, text_of(scene.root / FOLDERS[stack] / "backend.tf") == backend_text(stack), f"{label} backend.tf is not the generated text")
        code, output = scene.run("plan", stack, "--phase", "after", *scene.admin(stack))
        expect(problems, code == 0 and "plan after: no change" in output, f"{label} plan after: exit {code}: {output}")
        code, output = scene.run("verify", stack, *scene.admin(stack))
        expect(problems, code == 0 and "verify: equal" in output and "plans: equal" in output, f"{label} verify: exit {code}: {output}")
        expect(problems, scene.record(stack).is_file() and json.loads(scene.record(stack).read_text())["stage"] == "done", f"{label} no done record")
    expected_role = {stack: "administrator" if stack == "iam" else "project" for stack in ORDER}
    for stack in ORDER:
        roles = {seen.role for seen in scene.bucket.requests() if seen.key.startswith(f"terraform_state/{stack}/")}
        expect(
            problems, roles == {expected_role[stack]}, f"{stack}: the state objects were reached by {sorted(roles)}, not by the {expected_role[stack]} key only"
        )
    puts = [seen.key for seen in scene.bucket.requests("PUT")]
    expect(problems, any(key.endswith(".tflock") for key in puts), "no lock file was written next to a state: use_lockfile did not take effect")
    expect(
        problems,
        not [seen for seen in scene.bucket.requests() if seen.status in (401, 403)],
        "a request was refused: a stack was reached with the wrong profile",
    )
    expect(
        problems,
        all(not key.endswith(".tflock") for key in scene.bucket.objects),
        "a lock file stays in the bucket after the run",
    )
    report["requests"] = len(scene.bucket.requests())
    report["lock_writes"] = sum(1 for key in puts if key.endswith(".tflock"))


def second_run(scene: Scene, problems: list[str]) -> None:
    stack = "platform"
    snapshot = dict(scene.bucket.objects)
    puts_before = len(scene.bucket.requests("PUT"))
    code, output = scene.run("migrate", stack)
    expect(problems, code == 1 and "migration: already done" in output, f"second run: exit {code}: {output}")
    scene.record(stack).unlink(missing_ok=True)
    code, output = scene.run("migrate", stack)
    expect(
        problems,
        code == 1 and ("backend already switched" in output or "migration: refused" in output),
        f"second run without the record: exit {code}: {output}",
    )
    expect(problems, scene.bucket.objects == snapshot, "a refused second run changed the bucket")
    expect(problems, len(scene.bucket.requests("PUT")) == puts_before, "a refused second run wrote an object")


def interrupted_and_mismatch(template: Path, work: Path, problems: list[str]) -> None:
    stack = "jobs"
    scene = fresh(template, work, "interrupted")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        scene.run("plan", stack, "--phase", "before")
        before = scene.state_file(stack).read_bytes()
        bucket.faults.drop_put = {scene.key(stack)}
        code, output = scene.run("migrate", stack)
        expect(problems, code == 1 and "copy: failed" in output, f"interrupted copy: exit {code}: {output}")
        expect(problems, scene.key(stack) not in bucket.objects, "an interrupted copy left an object")
        expect(
            problems,
            not (scene.root / FOLDERS[stack] / "backend.tf").exists() and not scene.record(stack).exists(),
            "an interrupted copy wrote a backend or a record",
        )
        expect(problems, scene.state_file(stack).read_bytes() == before, "an interrupted copy changed the local state")
        bucket.faults.drop_put = set()
        code, output = scene.run("migrate", stack)
        expect(problems, code == 0 and "migration: done" in output, f"retry after the interruption: exit {code}: {output}")
    scene = fresh(template, work, "race")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        scene.run("plan", stack, "--phase", "before")
        other = b'{"version": 4, "serial": 7, "lineage": "another-writer", "resources": []}'
        bucket.faults.race_put = {scene.key(stack): other}
        code, output = scene.run("migrate", stack)
        expect(problems, code == 1 and "copy: failed" in output, f"a key created between the check and the write: exit {code}: {output}")
        expect(problems, bucket.objects.get(scene.key(stack)) == other, "the write replaced an object that another writer created")
        expect(
            problems, not (scene.root / FOLDERS[stack] / "backend.tf").exists() and not scene.record(stack).exists(), "a lost race wrote a backend or a record"
        )
    scene = fresh(template, work, "mismatch")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        scene.run("plan", stack, "--phase", "before")
        bucket.faults.corrupt_get = {scene.key(stack)}
        code, output = scene.run("migrate", stack)
        expect(problems, code == 1 and "copy: mismatch" in output, f"read-back mismatch: exit {code}: {output}")
        expect(
            problems, not (scene.root / FOLDERS[stack] / "backend.tf").exists() and not scene.record(stack).exists(), "a mismatch wrote a backend or a record"
        )
        expect(problems, fingerprint(scene.state_file(stack))[2] == RESOURCES[stack], "a mismatch changed the local state")
        puts = len(bucket.requests("PUT"))
        bucket.faults.corrupt_get = set()
        code, output = scene.run("migrate", stack)
        expect(problems, code == 0 and "copy: adopted" in output, f"retry after a mismatch must adopt the identical copy: exit {code}: {output}")
        expect(problems, len(bucket.requests("PUT")) == puts, "the retry wrote the state again instead of adopting it")


def mutate(scene: Scene, sid: str, **changes: object) -> None:
    scene.statement(sid).update(changes)


def drop_project_user(scene: Scene) -> None:
    names = scene.statement("DenyStatePrefixToEveryoneElse")["Condition"]["ArnNotLike"]
    names["aws:PrincipalArn"] = [value for value in names["aws:PrincipalArn"] if "gh_terraform_plan" in value]


def duplicate_sid(scene: Scene) -> None:
    scene.statements().append({**scene.statement("ProjectUserUsesItsStateKeys"), "Effect": "Deny"})


def other_deny(scene: Scene) -> None:
    scene.statements().append(
        {
            "Sid": "DenyEverythingUnderTheStatePrefix",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [f"arn:aws:s3:::{BUCKET}/terraform_state/*"],
        }
    )


POLICY_MUTATIONS = {
    "deny_prefix": lambda scene: mutate(scene, "DenyStatePrefixToEveryoneElse", Resource=[f"arn:aws:s3:::{BUCKET}/terraform_other/*"]),
    "allow_effect": lambda scene: mutate(scene, "ProjectUserUsesItsStateKeys", Effect="Deny"),
    "lock_effect": lambda scene: mutate(scene, "ProjectUserHoldsItsStateLocks", Effect="Deny"),
    "allow_principal": lambda scene: mutate(scene, "ProjectUserUsesItsStateKeys", Principal={"AWS": f"arn:aws:iam::{ACCOUNT}:user/someone_else"}),
    "allow_actions": lambda scene: mutate(scene, "ProjectUserUsesItsStateKeys", Action=["s3:GetObject"]),
    "deny_exception": drop_project_user,
    "duplicate_sid": duplicate_sid,
    "other_deny": other_deny,
}
STATE_RULE_FIELDS = {"id": STATE_RULE, "prefix": "terraform_state/", "noncurrent_days": 90, "delete_marker": True}
LIFECYCLE_MUTATIONS: dict[str, list[str | dict[str, object]]] = {
    "lifecycle_off": [{**STATE_RULE_FIELDS, "status": "Disabled"}],
    "lifecycle_prefix": [{**STATE_RULE_FIELDS, "prefix": "lakehouse/"}],
    "lifecycle_no_expiry": [{**STATE_RULE_FIELDS, "noncurrent_days": None}],
    "lifecycle_current": [{**STATE_RULE_FIELDS, "days": 30}],
    "lifecycle_other": [STATE_RULE, {"id": "expire_everything", "prefix": None, "days": 30}],
    "lifecycle_stack": [STATE_RULE, {"id": "expire_jobs_state", "prefix": "terraform_state/jobs/", "days": 30}],
    "lifecycle_lock": [STATE_RULE, {"id": "expire_locks", "prefix": "terraform_state/services/terraform.tfstate.tflock", "days": 30}],
    "lifecycle_tag": [STATE_RULE, {"id": "expire_tagged", "prefix": "terraform_state/", "tag": ("keep", "no"), "days": 30}],
    "lifecycle_tag_only": [STATE_RULE, {"id": "expire_tag_only", "prefix": None, "tag": ("keep", "no"), "days": 30}],
}


def refusals(template: Path, work: Path, problems: list[str]) -> None:
    stack = "services"
    cases: dict[str, tuple[str, str]] = {
        "a remote object that differs": ("remote key: differs", "differs"),
        "a lock held": ("remote key: locked", "locked"),
        "a suspended version history": ("bucket versioning: not enabled", "versioning"),
        "no state statements in the bucket policy": ("bucket policy: state statements missing", "policy"),
        "no lifecycle rule for replaced state versions": ("lifecycle: rule for replaced state versions missing", "lifecycle"),
        "no local state": ("local state: missing", "no_local"),
        "an empty local state": ("local state: empty", "empty_local"),
        "a local state that is not state": ("local state: invalid", "bad_local"),
        "a wrong account": ("identity: refused", "identity"),
        "a policy that does not allow this stack's state key": ("the state key of this stack is not allowed", "key_missing"),
        "deny statements that name another prefix": ("bucket policy: state statements invalid", "deny_prefix"),
        "a state allow that became a deny": ("bucket policy: state statements invalid", "allow_effect"),
        "a lock statement that became a deny": ("bucket policy: state statements invalid", "lock_effect"),
        "a state allow for another principal": ("bucket policy: state statements invalid", "allow_principal"),
        "a state allow with other actions": ("bucket policy: state statements invalid", "allow_actions"),
        "a deny that does not except the project user": ("bucket policy: state statements invalid", "deny_exception"),
        "a second statement with a state Sid": ("bucket policy: state statements invalid", "duplicate_sid"),
        "a deny of this stack's key without a condition": ("bucket policy: state statements invalid", "other_deny"),
        "a lifecycle rule that is disabled": ("lifecycle: rule for replaced state versions invalid", "lifecycle_off"),
        "a lifecycle rule on another prefix": ("lifecycle: rule for replaced state versions invalid", "lifecycle_prefix"),
        "a lifecycle rule without version expiry": ("lifecycle: rule for replaced state versions invalid", "lifecycle_no_expiry"),
        "a lifecycle rule that expires the current state": ("lifecycle: rule for replaced state versions invalid", "lifecycle_current"),
        "another rule that expires current objects of the whole bucket": ("lifecycle: another rule can expire", "lifecycle_other"),
        "another rule that expires the objects of one other stack": ("lifecycle: another rule can expire", "lifecycle_stack"),
        "another rule that expires the lock object of this stack": ("lifecycle: another rule can expire", "lifecycle_lock"),
        "another rule on the state prefix with a tag filter": ("lifecycle: another rule can expire", "lifecycle_tag"),
        "another rule with only a tag filter": ("lifecycle: another rule can expire", "lifecycle_tag_only"),
        "named workspaces left in the stack folder": ("local state: named workspaces", "workspace_dir"),
        "a named workspace selected": ("local state: a named workspace is selected", "workspace_selected"),
    }
    for name, (status, tag) in cases.items():
        scene = fresh(template, work, f"refuse_{tag}")
        with StateBucket(KEYS, dict(IDENTITIES)) as bucket:
            scene.bucket = bucket
            scene.protect()
            local = scene.state_file(stack)
            if tag == "differs":
                bucket.objects[scene.key(stack)] = b'{"version": 4, "serial": 99, "lineage": "other", "resources": []}'
            elif tag == "locked":
                bucket.objects[scene.key(stack) + ".tflock"] = b"{}"
            elif tag == "versioning":
                bucket.versioning = "Suspended"
            elif tag == "policy":
                bucket.policy = {"Version": "2012-10-17", "Statement": []}
            elif tag == "lifecycle":
                bucket.lifecycle = ["abort_incomplete_uploads"]
            elif tag == "no_local":
                local.unlink()
            elif tag == "empty_local":
                local.write_bytes(b"")
            elif tag == "bad_local":
                local.write_text("not json\n")
            elif tag == "key_missing":
                bucket.policy = json.loads(json.dumps(bucket.policy).replace("terraform_state/services/", "terraform_state/other/"))
            elif tag == "identity":
                bucket.identities = {"project": ("222222222222", IDENTITIES["project"][1])}
            elif tag in POLICY_MUTATIONS:
                POLICY_MUTATIONS[tag](scene)
            elif tag.startswith("lifecycle_"):
                bucket.lifecycle = ["abort_incomplete_uploads", *LIFECYCLE_MUTATIONS[tag]]
            elif tag.startswith("workspace_"):
                folder = scene.root / FOLDERS[stack]
                terraform = [f"-chdir={folder}", "workspace"]
                created = run_command("terraform", [*terraform, "new", "dev"], timeout=STEP_SECONDS)
                if tag == "workspace_dir":
                    created = run_command("terraform", [*terraform, "select", "default"], timeout=STEP_SECONDS)
                expect(problems, created.returncode == 0, f"{name}: the workspace could not be prepared: {created.stdout[-300:]}")
            before = dict(bucket.objects)
            local_before = local.read_bytes() if local.exists() else None
            record = scene.root / RECORD_FOLDER
            record.mkdir(exist_ok=True)
            (record / f"{stack}_before.changes.txt").write_text("", encoding="utf-8")
            code, output = scene.run("migrate", stack)
            expect(problems, code == 1 and status in output, f"{name}: exit {code}: {output}")
            expect(problems, bucket.objects == before, f"{name}: the bucket changed")
            expect(
                problems,
                not (scene.root / FOLDERS[stack] / "backend.tf").exists() and not scene.record(stack).exists(),
                f"{name}: a backend or a record was written",
            )
            expect(problems, (local.read_bytes() if local.exists() else None) == local_before, f"{name}: the local state changed")


def profile_split(template: Path, work: Path, problems: list[str]) -> None:
    scene = fresh(template, work, "split")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        checks = [
            ("iam", [], "administrator profile"),
            ("iam", ["--admin-profile", PROJECT_PROFILE], "administrator profile"),
            ("platform", ["--admin-profile", ADMIN_PROFILE], "administrator profile"),
            ("storage", ["--admin-profile", ADMIN_PROFILE], "administrator profile"),
        ]
        for stack, extra, status in checks:
            for command in ("check", "plan", "migrate", "verify"):
                arguments = [*extra, "--phase", "before"] if command == "plan" else extra
                code, output = scene.run(command, stack, *arguments)
                expect(problems, code in (1, 2) and status in output, f"profile split {stack} {command} {extra}: exit {code}: {output}")
        expect(problems, not bucket.requests("PUT") and not bucket.objects, "a refused profile choice reached the bucket")
        bucket.identities = {"administrator": IDENTITIES["project"], "project": IDENTITIES["project"]}
        code, output = scene.run("check", "iam", *scene.admin("iam"))
        expect(
            problems,
            code == 1 and "identity: refused" in output,
            f"the administrator profile signing in as the project user must be refused: exit {code}: {output}",
        )


def symlinked_state(template: Path, work: Path, problems: list[str]) -> None:
    stack = "alerts"
    scene = fresh(template, work, "symlink")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        owner = work / "state_of_another_checkout"
        owner.mkdir()
        target = owner / "terraform.tfstate"
        local = scene.state_file(stack)
        shutil.move(local, target)
        local.symlink_to(target)
        original = target.read_bytes()
        scene.run("plan", stack, "--phase", "before")
        code, output = scene.run("migrate", stack)
        expect(problems, code == 0 and "migration: done" in output, f"symbolic link: exit {code}: {output}")
        backup = scene.root / RECORD_FOLDER / f"{stack}.local_state.json"
        expect(problems, backup.is_file() and backup.read_bytes() == original, "the backup of a linked state differs from the state it came from")
        expect(problems, bucket.objects.get(scene.key(stack)) == original, "the remote copy of a linked state differs")


def backend_contract(template: Path, work: Path, problems: list[str]) -> None:
    stack = "platform"
    scene = fresh(template, work, "backend_text")
    folder = scene.root / FOLDERS[stack]
    code, output = scene.run("backend", stack, environment={"AWS_ENDPOINT_URL_S3": "http://127.0.0.1:1", "AWS_ENDPOINT_URL_STS": "http://127.0.0.1:1"})
    expect(problems, code == 0, f"backend command: exit {code}: {output}")
    text = (folder / "backend.tf").read_text()
    for forbidden in (BUCKET, ACCOUNT, "us-west-2", PROJECT_PROFILE, ADMIN_PROFILE, "bucket", "region", "profile", "dynamodb"):
        expect(problems, forbidden not in text, f"backend.tf holds {forbidden!r}")
    expect(
        problems,
        'key                  = "terraform_state/platform/terraform.tfstate"' in text and "use_lockfile         = true" in text,
        "backend.tf lacks the key or the lock file",
    )
    result = run_command("terraform", [f"-chdir={folder}", "init", "-backend=false", "-input=false"], timeout=STEP_SECONDS)
    expect(problems, result.returncode == 0, f"terraform init -backend=false: {result.stdout[-300:]}{result.stderr[-300:]}")
    result = run_command("terraform", [f"-chdir={folder}", "fmt", "-check", "-diff"], timeout=STEP_SECONDS)
    expect(problems, result.returncode == 0, f"terraform fmt -check: {result.stdout[-400:]}")
    result = run_command("terraform", [f"-chdir={folder}", "validate"], timeout=STEP_SECONDS)
    expect(problems, result.returncode == 0, f"terraform validate: {result.stdout[-300:]}{result.stderr[-300:]}")
    code, output = scene.run("backend", stack)
    expect(problems, code == 0, f"the backend command must be idempotent: exit {code}: {output}")
    (folder / "backend.tf").write_text(text.replace("use_lockfile         = true", "use_lockfile         = false"))
    code, output = scene.run("backend", stack)
    expect(problems, code == 1 and "backend: differs" in output, f"a changed backend.tf must be refused, not overwritten: exit {code}: {output}")
    main = (ROOT / "infra/main.tf").read_text()
    allowed = re.search(r"state_user_stacks\s*=\s*\[([^\]]*)\]", main)
    reach = re.search(r"reach_stacks\s*=\s*\[([^\]]*)\]", (ROOT / "infra/iam/state_prefix_reach.tf").read_text())
    names = {name for name in STACKS if name != "iam"}
    expect(
        problems,
        allowed is not None and set(re.findall(r'"(\w+)"', allowed.group(1))) == names,
        "state_user_stacks in infra/main.tf is not the five stacks the project user applies",
    )
    expect(
        problems,
        reach is not None and set(re.findall(r'"(\w+)"', reach.group(1))) == set(STACKS),
        "reach_stacks in infra/iam/state_prefix_reach.tf is not the six stacks",
    )
    for stack in STACKS:
        expect(problems, f'terraform_state/{stack}/terraform.tfstate"' in backend_text(stack), f"the backend of {stack} names another key")


def migrate_fully(scene: Scene, stack: str, problems: list[str]) -> None:
    """The sheet's steps for one stack, each expected to succeed."""
    for command, extra, text in (
        ("plan", ["--phase", "before"], "plan before"),
        ("migrate", [], "migration: done"),
        ("plan", ["--phase", "after"], "plan after"),
    ):
        code, output = scene.run(command, stack, *extra, *scene.admin(stack))
        expect(problems, code == 0 and text in output, f"[{stack}] {command} {extra}: exit {code}: {output}")


def tampered_verification(template: Path, work: Path, problems: list[str]) -> None:
    """A state read through the backend must equal the record: the same serial and the same bytes in the bucket."""
    stack = "jobs"
    scene = fresh(template, work, "tampered")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        migrate_fully(scene, stack, problems)
        code, output = scene.run("verify", stack)
        expect(problems, code == 0 and "plans: equal" in output, f"verify before any change: exit {code}: {output}")
        original = bucket.objects[scene.key(stack)]
        document = json.loads(original)
        newer = {**document, "serial": document["serial"] + 1}
        changed = json.loads(original)
        changed["resources"][0]["instances"][0]["attributes"]["input"] = "another_value"
        for label, data in (("a newer serial with the same counts", newer), ("the same serial with another attribute", changed)):
            bucket.objects[scene.key(stack)] = (json.dumps(data, indent=2) + "\n").encode()
            code, output = scene.run("verify", stack)
            expect(problems, code == 1 and "verify:" in output and "plans:" not in output, f"verify must refuse {label}: exit {code}: {output}")
        bucket.objects[scene.key(stack)] = original
        code, output = scene.run("verify", stack)
        expect(problems, code == 0, f"verify after the object was put back: exit {code}: {output}")


def output_only_plan(template: Path, work: Path, problems: list[str]) -> None:
    """A plan that changes only an output is a change: it is kept in the list, and an exit code 2 with an empty list is refused."""
    stack = "alerts"
    scene = fresh(template, work, "output_plan")
    folder = scene.root / FOLDERS[stack]
    (folder / "main.tf").write_text(main_tf(RESOURCES[stack]) + '\noutput "stamp" {\n  value = "new"\n}\n', encoding="utf-8")
    code, output = scene.run("plan", stack, "--phase", "before")
    listed = text_of(scene.root / RECORD_FOLDER / f"{stack}_before.changes.txt")
    expect(problems, code == 0 and "1 change(s)" in output and listed == "output.stamp: create\n", f"an output-only plan: exit {code}: {output}: {listed!r}")
    wrapper = scene.root / "scripts/infrastructure/terraform.sh"
    wrapper.write_text(
        WRAPPER.replace('exec terraform -chdir="$stack" "$@" -input=false', 'terraform -chdir="$stack" "$@" -input=false || true\nexit 2'), encoding="utf-8"
    )
    (folder / "main.tf").write_text(main_tf(RESOURCES[stack]), encoding="utf-8")
    code, output = scene.run("plan", stack, "--phase", "after")
    expect(problems, code == 1 and "plan after: reported changes" in output, f"exit code 2 with no listed change: exit {code}: {output}")


def resumed_runs(template: Path, work: Path, problems: list[str]) -> None:
    """A run that stopped at any stage is resumed from its record: after the backend file, after the switch, and after a failed read through the backend."""
    stack = "jobs"
    scene = fresh(template, work, "resume_backend")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        code, output = scene.run("plan", stack, "--phase", "before")
        expect(problems, code == 0, f"resume: plan before: exit {code}: {output}")
        data = scene.state_file(stack).read_bytes()
        document = json.loads(data)
        bucket.objects[scene.key(stack)] = data
        record = {"stack": stack, "stage": "copied", "serial": document["serial"], "lineage": document["lineage"], "resources": len(document["resources"])}
        record |= {"instances": sum(len(item.get("instances", [])) for item in document["resources"]), "sha256": hashlib.sha256(data).hexdigest(), "at": "x"}
        scene.record(stack).write_text(json.dumps(record), encoding="utf-8")
        code, output = scene.run("backend", stack)
        expect(problems, code == 0, f"resume: backend: exit {code}: {output}")
        code, output = scene.run("plan", stack, "--phase", "before")
        expect(problems, code == 0 and "plan before: kept" in output, f"a stop after the backend file must resume at the plan before: exit {code}: {output}")
        code, output = scene.run("migrate", stack)
        expect(problems, code == 0 and "migration: done" in output, f"resume: migrate after the backend file: exit {code}: {output}")
        for command, extra in (("plan", ["--phase", "after"]), ("verify", [])):
            code, output = scene.run(command, stack, *extra)
            expect(problems, code == 0, f"resume: {command}: exit {code}: {output}")
    scene = fresh(template, work, "resume_switched")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        migrate_fully(scene, stack, problems)
        record = json.loads(scene.record(stack).read_text())
        scene.record(stack).write_text(json.dumps({**record, "stage": "copied"}), encoding="utf-8")
        puts = len(bucket.requests("PUT"))
        code, output = scene.run("check", stack)
        expect(
            problems, code == 0 and "check: resume" in output, f"the first sheet step must resume a run that stopped after the switch: exit {code}: {output}"
        )
        code, output = scene.run("plan", stack, "--phase", "before")
        expect(problems, code == 0 and "plan before: kept" in output, f"the plan step must keep its list after the switch: exit {code}: {output}")
        code, output = scene.run("migrate", stack)
        expect(problems, code == 0 and "backend: switched (resumed" in output, f"a stop after the switch must resume from the record: exit {code}: {output}")
        expect(problems, json.loads(scene.record(stack).read_text())["stage"] == "switched", "the resumed run did not record the switch")
        expect(problems, len(bucket.requests("PUT")) == puts, "the resumed run wrote to the bucket")
        code, output = scene.run("plan", stack, "--phase", "after")
        expect(problems, code == 0, f"resume after the switch: plan after: exit {code}: {output}")
        code, output = scene.run("verify", stack)
        expect(problems, code == 0, f"resume after the switch: verify: exit {code}: {output}")
        expect(problems, json.loads(scene.record(stack).read_text())["stage"] == "done", "the verification did not record done")
    scene = fresh(template, work, "resume_pull")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        code, output = scene.run("plan", stack, "--phase", "before")
        original = scene.state_file(stack).read_bytes()
        bucket.faults.corrupt_get_after = {scene.key(stack): 1}
        code, output = scene.run("migrate", stack)
        expect(problems, code == 1 and "backend: restored" in output, f"a failed read through the backend must restore the stack: exit {code}: {output}")
        expect(problems, scene.state_file(stack).read_bytes() == original, "a failed read through the backend changed the local state")
        marker = scene.root / FOLDERS[stack] / ".terraform" / "terraform.tfstate"
        expect(problems, not marker.is_file() or '"type": "s3"' not in marker.read_text(), "a failed read left the cached S3 backend marker")
        code, output = scene.run("plan", stack, "--phase", "after")
        expect(problems, code == 0, f"an ordinary plan after the restore: exit {code}: {output}")
        bucket.faults.corrupt_get_after = {}
        code, output = scene.run("migrate", stack)
        expect(problems, code == 0 and "migration: done" in output, f"migrate must retry after a restore: exit {code}: {output}")


def storage_protection(template: Path, work: Path, problems: list[str]) -> None:
    """The storage plans carry the approved state inputs; a plan without them is refused before it is kept."""
    stack = "storage"
    scene = fresh(template, work, "storage_inputs")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        code, output = scene.run("plan", stack, "--phase", "before")
        expect(problems, code == 1 and "state protection inputs are not recorded" in output, f"no approved inputs on record: exit {code}: {output}")
        expect(problems, not (scene.root / RECORD_FOLDER / "storage_before.changes.txt").exists(), "a refused plan wrote its change list")
        scene.approve_state_admin()
        code, output = scene.run("plan", stack, "--phase", "before")
        calls = text_of(scene.root / "wrapper_calls.log") or ""
        expect(
            problems,
            code == 0 and "state_admin=role/example_admin" in calls,
            f"the storage plan must pass the approved administrator: exit {code}: {output}: {calls}",
        )
        wrapper = scene.root / "scripts/infrastructure/terraform.sh"
        wrapper.write_text(WRAPPER.replace('state_admin="$2"; shift 2', "shift 2"), encoding="utf-8")
        code, output = scene.run("plan", stack, "--phase", "after")
        expect(
            problems,
            code == 1 and "does not carry the approved state protection inputs" in output,
            f"a plan without the inputs must be refused: exit {code}: {output}",
        )
        scene.approve_state_admin("arn:aws:iam::x:role/y")
        code, output = scene.run("plan", stack, "--phase", "after")
        expect(problems, code == 1 and "state protection inputs are not valid" in output, f"an invalid record must be refused: exit {code}: {output}")


def approved_plan_shape(template: Path, work: Path, problems: list[str]) -> None:
    """The prefix sheet applies a saved plan only when it holds exactly the two approved updates in place and nothing else."""
    scene = fresh(template, work, "plan_shape")
    folder = scene.root / FOLDERS["storage"]

    def write(inputs: dict[str, str], extra: str = "", tail: str = "") -> None:
        body = "\n".join(f'resource "terraform_data" "{name}" {{\n  input = "{value}"\n{extra if name == "r1" else ""}}}\n' for name, value in inputs.items())
        (folder / "main.tf").write_text(f'terraform {{\n  required_version = "~> 1.16.1"\n}}\n\n{STATE_VARIABLES}{body}{tail}', encoding="utf-8")

    def plan() -> Path:
        path = scene.root / "storage_shape.tfplan"
        result = run_command("terraform", [f"-chdir={folder}", "plan", "-input=false", "-no-color", "-out", str(path)], timeout=STEP_SECONDS)
        if result.returncode:
            raise RuntimeError(f"planning the storage shape failed: {result.stdout[-300:]}")
        return path

    write({"r0": "value0", "r1": "value1", "r2": "value2"})
    result = run_command("terraform", [f"-chdir={folder}", "apply", "-auto-approve", "-input=false"], timeout=STEP_SECONDS)
    expect(problems, result.returncode == 0, f"preparing the storage shape failed: {result.stdout[-300:]}")
    approved = ["--update", "terraform_data.r0", "--update", "terraform_data.r1"]
    moved = "moved {\n  from = terraform_data.r2\n  to   = terraform_data.r2b\n}\n"
    output = 'output "stamp" {\n  value = "new"\n}\n'
    cases = (
        ("two updates and a destroy", {"r0": "a", "r1": "b"}, "", "", "terraform_data.r2: delete"),
        ("two updates and a create", {"r0": "a", "r1": "b", "r2": "value2", "r3": "c"}, "", "", "terraform_data.r3: create"),
        ("one update only", {"r0": "a", "r1": "value1", "r2": "value2"}, "", "", "terraform_data.r1: missing"),
        (
            "a replacement instead of an update",
            {"r0": "a", "r1": "value1", "r2": "value2"},
            '  triggers_replace = "x"\n',
            "",
            "terraform_data.r1: delete+create",
        ),
        ("two updates and a moved block", {"r0": "a", "r1": "b", "r2b": "value2"}, "", moved, "terraform_data.r2b: moved"),
        ("two updates and an output change", {"r0": "a", "r1": "b", "r2": "value2"}, "", output, "output.stamp: create"),
        ("two updates that no rule describes", {"r0": "a", "r1": "b", "r2": "value2"}, "", "", "terraform_data.r0: no rule says what may change in it"),
    )
    for label, inputs, extra, tail, text in cases:
        write(inputs, extra, tail)
        saved = plan()
        code, printed = scene.run("approve-plan", "storage", "--plan", str(saved), *approved)
        expect(problems, code == 1 and text in printed and "plan sha256" not in printed, f"{label}: exit {code}: {printed}")
        saved.unlink(missing_ok=True)
    write({"r0": "a", "r1": "b", "r2": "value2"})
    saved = plan()
    original = dict(state_migration.DELTAS)
    state_migration.DELTAS.update({"terraform_data.r0": lambda _: None, "terraform_data.r1": lambda _: None})
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            state_migration.command_approve_plan(state_migration.load_settings(scene.root, "storage", None), saved, ["terraform_data.r0", "terraform_data.r1"])
    finally:
        state_migration.DELTAS.clear()
        state_migration.DELTAS.update(original)
    digest = hashlib.sha256(saved.read_bytes()).hexdigest()
    expect(
        problems,
        "plan shape: ok" in buffer.getvalue() and f"plan sha256: {digest}\n" in buffer.getvalue(),
        f"an approved plan must print its file digest: {buffer.getvalue()!r}",
    )
    saved.unlink(missing_ok=True)
    prefix_plan_shape(problems)
    native_prefix_plans(problems)


def iam_deny_exception(template: Path, work: Path, problems: list[str]) -> None:
    """The iam stack needs the administrator among the principals that the prefix deny excepts."""
    stack = "iam"
    scene = fresh(template, work, "refuse_iam_deny")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        names = scene.statement("DenyStatePrefixToEveryoneElse")["Condition"]["ArnNotLike"]
        names["aws:PrincipalArn"] = [value for value in names["aws:PrincipalArn"] if ADMIN_PROFILE not in value]
        code, output = scene.run("check", stack, *scene.admin(stack))
        expect(
            problems,
            code == 1 and "bucket policy: state statements invalid" in output,
            f"the iam stack must refuse a deny that does not except the administrator: exit {code}: {output}",
        )
        names["aws:PrincipalArn"].append(f"arn:aws:iam::{ACCOUNT}:user/{ADMIN_PROFILE}")
        code, output = scene.run("check", stack, *scene.admin(stack))
        expect(problems, code == 0, f"the iam stack must accept the policy with the administrator excepted: exit {code}: {output}")


STATE_ADMIN = f"user/{ADMIN_PROFILE}"


def protection_fails_closed(template: Path, work: Path, problems: list[str]) -> None:
    """A checkout without the record cannot plan or apply the protection away, a pending record is no approval and a foreign record is refused."""
    scene = fresh(template, work, "fail_closed")
    record = scene.root / STATE_PROTECTION_RECORD
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        code, output = scene.protection("resolve", "--live")
        expect(
            problems,
            code == 2 and "already holds the state protection" in output,
            f"no record and a protected bucket must refuse a render: exit {code}: {output}",
        )
        code, output = scene.protection("resolve")
        expect(problems, code == 0 and output.strip() == "", f"a render that reads no bucket (validate) needs no record: exit {code}: {output}")
        code, output = scene.protection("resolve", "--live", "--disable")
        expect(problems, code == 0 and output.strip() == "", f"the disable flag must let the protection go on purpose: exit {code}: {output}")
        code, output = scene.protection("resolve", "--live", "--disable", "--state-admin", STATE_ADMIN)
        expect(problems, code == 2 and "cannot be combined" in output, f"the disable flag with an administrator must be refused: exit {code}: {output}")
        code, output = scene.protection("resolve", "--live", "--state-admin", STATE_ADMIN)
        expect(problems, code == 0 and output.strip() == STATE_ADMIN, f"a render that carries an administrator is not refused: exit {code}: {output}")
        bucket.faults.deny_all = True
        code, output = scene.protection("resolve", "--live")
        expect(problems, code == 2 and "could not be read" in output, f"an unreadable live policy must refuse, not pass: exit {code}: {output}")
        bucket.faults.deny_all = False
        policy = bucket.policy
        bucket.policy = None
        code, output = scene.protection("resolve", "--live")
        expect(problems, code == 0 and output.strip() == "", f"a bucket with no policy has no protection to remove: exit {code}: {output}")
        code, output = scene.protection("remember", "--state-admin", STATE_ADMIN)
        document = json.loads(record.read_text(encoding="utf-8")) if record.is_file() else {}
        expect(problems, code == 0 and document.get("status") == "pending", f"remember must keep the administrator as pending: exit {code}: {document}")
        code, output = scene.protection("verify", "--state-admin", STATE_ADMIN)
        expect(
            problems,
            code == 2 and json.loads(record.read_text())["status"] == "pending",
            f"a read-back without the protection must stay pending: {code}: {output}",
        )
        bucket.policy = policy
        code, output = scene.protection("resolve", "--live")
        expect(problems, code == 2 and "already holds the state protection" in output, f"a pending record must count as no approval: exit {code}: {output}")
        bucket.lifecycle = ["abort_incomplete_uploads"]
        code, output = scene.protection("verify", "--state-admin", STATE_ADMIN)
        expect(
            problems,
            code == 2 and "lifecycle" in output and json.loads(record.read_text())["status"] == "pending",
            f"no lifecycle rule must stay pending: {code}: {output}",
        )
        scene.protect()
        code, output = scene.protection("verify", "--state-admin", STATE_ADMIN)
        expect(
            problems,
            code == 0 and json.loads(record.read_text())["status"] == "verified",
            f"the read-back with the protection must verify: exit {code}: {output}",
        )
        code, output = scene.protection("resolve", "--live")
        expect(problems, code == 0 and output.strip() == STATE_ADMIN, f"a verified record carries its administrator: exit {code}: {output}")
        code, output = scene.protection("remember", "--state-admin", STATE_ADMIN)
        expect(
            problems,
            code == 0 and json.loads(record.read_text())["status"] == "verified",
            f"a verified record must not fall back to pending: exit {code}: {output}",
        )
        code, output = scene.protection("remember", "--state-admin", "user/someone_else")
        expect(problems, code == 2 and "differs from the approved" in output, f"another administrator must be refused: exit {code}: {output}")
        scene.approve_state_admin(STATE_ADMIN, digest=deployment_digest("another-bucket", ACCOUNT))
        for command, extra in (("resolve", ["--live"]), ("remember", ["--state-admin", STATE_ADMIN])):
            code, output = scene.protection(command, *extra)
            expect(
                problems,
                code == 2 and "another bucket or account" in output,
                f"a record of another deployment must be refused ({command}): exit {code}: {output}",
            )
        record.write_text(json.dumps({"terraform_state_admin_principals": [STATE_ADMIN]}) + "\n", encoding="utf-8")
        code, output = scene.protection("resolve", "--live")
        expect(problems, code == 2 and "cannot be read" in output, f"a record without a deployment must be refused: exit {code}: {output}")
        scene.approve_state_admin(STATE_ADMIN, status="pending")
        code, output = scene.run("plan", "storage", "--phase", "before")
        expect(problems, code == 1 and "not recorded as verified" in output, f"a pending record must not carry a storage plan: exit {code}: {output}")


def lifecycle_other_prefixes_accepted(template: Path, work: Path, problems: list[str]) -> None:
    """A rule that expires current objects elsewhere is no problem: only a rule that can reach a state or lock object of a stack is refused."""
    scene = fresh(template, work, "lifecycle_ok")
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        bucket.lifecycle = [
            "abort_incomplete_uploads",
            STATE_RULE,
            {"id": "expire_saved_plans", "prefix": "terraform_plans/", "days": 7},
            {"id": "expire_other_state", "prefix": "terraform_state/other/", "days": 30},
        ]
        code, output = scene.run("check", "services")
        expect(problems, code == 0 and "check: ok" in output, f"rules that cannot reach a state or lock object must be accepted: exit {code}: {output}")


PLAN_BASE_RULES: list[dict[str, Any]] = [{"id": "abort_incomplete_uploads", "status": "Enabled", "filter": [{"prefix": ""}]}]
PLAN_STATE_RULES: list[dict[str, Any]] = [{"id": rule, "status": "Enabled", "filter": [{"prefix": "x"}]} for rule in state_migration.PREFIX_RULES]
PLAN_TRANSPORT: dict[str, Any] = {"Sid": "DenyInsecureTransport", "Effect": "Deny", "Principal": "*", "Action": "s3:*", "Resource": "*"}
PLAN_UPDATES = [state_migration.PREFIX_LIFECYCLE, state_migration.PREFIX_POLICY]


def prefix_plan() -> dict[str, Any]:
    """A saved plan as ``terraform show -json`` writes it for the prefix change: the bucket policy gains the state statements and the lifecycle configuration
    gains the two state rules, which carry values that are not known yet."""
    sids = (*state_migration.STATE_SIDS, *state_migration.SAVED_PLAN_STATEMENTS)
    before = json.dumps({"Version": "2012-10-17", "Statement": [PLAN_TRANSPORT]})
    after = json.dumps({"Version": "2012-10-17", "Statement": [PLAN_TRANSPORT, *({"Sid": sid, "Effect": "Allow"} for sid in sids)]})
    rules = copy.deepcopy([*PLAN_BASE_RULES, *PLAN_STATE_RULES])
    return {
        "resource_changes": [
            {
                "address": state_migration.PREFIX_POLICY,
                "change": {"actions": ["update"], "before": {"bucket": "b", "policy": before}, "after": {"bucket": "b", "policy": after}, "after_unknown": {}},
            },
            {
                "address": state_migration.PREFIX_LIFECYCLE,
                "change": {
                    "actions": ["update"],
                    "before": {"bucket": "b", "rule": copy.deepcopy(PLAN_BASE_RULES)},
                    "after": {"bucket": "b", "rule": rules},
                    "after_unknown": {"rule": [{}, {"filter": [{"object_size_greater_than": True}]}, {"filter": [{"and": True}]}]},
                },
            },
            {"address": "aws_s3_bucket.data", "change": {"actions": ["no-op"]}},
        ],
        "output_changes": {"bucket": {"actions": ["no-op"]}},
    }


def edit_policy(plan: dict[str, Any], edit: Any) -> None:
    after = plan["resource_changes"][0]["change"]["after"]
    document = json.loads(after["policy"])
    edit(document)
    after["policy"] = json.dumps(document)


def edit_change(plan: dict[str, Any], index: int, edit: Any) -> None:
    edit(plan["resource_changes"][index]["change"])


def prefix_plan_mutants() -> dict[str, tuple[Any, str]]:
    """Each way a plan can carry more than the prefix change, with the words the refusal must hold."""
    state_statement = {"Sid": "ProjectUserUsesItsStateKeys", "Effect": "Allow"}
    return {
        "a moved resource": (
            lambda plan: plan["resource_changes"].append(
                {"address": "aws_s3_bucket.data", "previous_address": "aws_s3_bucket.old", "change": {"actions": ["no-op"]}}
            ),
            "aws_s3_bucket.data: moved",
        ),
        "an imported resource": (
            lambda plan: plan["resource_changes"].append({"address": "aws_s3_bucket.other", "change": {"actions": ["no-op"], "importing": {"id": "x"}}}),
            "aws_s3_bucket.other: imported",
        ),
        "an output change": (lambda plan: plan["output_changes"].update({"bucket": {"actions": ["update"]}}), "output.bucket: update"),
        "a transport statement that became an allow": (
            lambda plan: edit_policy(plan, lambda document: document["Statement"][0].update({"Effect": "Allow"})),
            "a statement other than the state statements changes",
        ),
        "a transport statement that was dropped": (
            lambda plan: edit_policy(plan, lambda document: document["Statement"].pop(0)),
            "a statement other than the state statements",
        ),
        "a statement of another Sid": (
            lambda plan: edit_policy(plan, lambda document: document["Statement"].append({"Sid": "OpenToEveryone", "Effect": "Allow"})),
            "a statement other than the state statements changes",
        ),
        "a policy version change": (
            lambda plan: edit_policy(plan, lambda document: document.update({"Version": "2008-10-17"})),
            "a statement other than the state statements",
        ),
        "another policy attribute": (
            lambda plan: edit_change(plan, 0, lambda change: change["after"].update({"bucket": "other"})),
            "an attribute other than the policy changes",
        ),
        "a policy that is not known yet": (
            lambda plan: edit_change(plan, 0, lambda change: change["after_unknown"].update({"policy": True})),
            "cannot be compared",
        ),
        "a state statement twice": (
            lambda plan: edit_policy(plan, lambda document: document["Statement"].append(state_statement)),
            "a state statement appears more than once",
        ),
        "an existing rule that changed": (
            lambda plan: edit_change(plan, 1, lambda change: change["after"]["rule"][0].update({"status": "Disabled"})),
            "a rule other than the state rules changes",
        ),
        "an existing rule that was dropped": (
            lambda plan: edit_change(plan, 1, lambda change: (change["after"]["rule"].pop(0), change["after_unknown"]["rule"].pop(0))),
            "a rule other than the state rules changes",
        ),
        "an existing rule with values not known yet": (
            lambda plan: edit_change(plan, 1, lambda change: change["after_unknown"]["rule"][0].update({"status": True})),
            "values not known yet",
        ),
        "another lifecycle attribute": (
            lambda plan: edit_change(plan, 1, lambda change: change["after"].update({"expected_bucket_owner": "x"})),
            "an attribute other than the rules changes",
        ),
        "a state rule twice": (
            lambda plan: edit_change(plan, 1, lambda change: (change["after"]["rule"].append(PLAN_STATE_RULES[0]), change["after_unknown"]["rule"].append({}))),
            "a state rule appears more than once",
        ),
    }


def prefix_plan_shape(problems: list[str]) -> None:
    """The prefix plan passes only as the exact change: the delta of each of the two resources is checked, not only their actions."""
    expect(problems, plan_shape_problems(prefix_plan(), PLAN_UPDATES) == [], f"the prefix plan must pass: {plan_shape_problems(prefix_plan(), PLAN_UPDATES)}")
    for label, (mutate_plan, words) in prefix_plan_mutants().items():
        plan = prefix_plan()
        mutate_plan(plan)
        found = "; ".join(plan_shape_problems(plan, PLAN_UPDATES))
        expect(problems, words in found, f"{label} must be refused with {words!r}: {found!r}")
    main_tf_text = (ROOT / "infra/main.tf").read_text(encoding="utf-8")
    statements = main_tf_text[main_tf_text.index("state_statements = ") : main_tf_text.index('resource "aws_s3_bucket_policy" "tls"')]
    sids = {*state_migration.STATE_SIDS, *state_migration.SAVED_PLAN_STATEMENTS}
    expect(problems, set(re.findall(r'Sid\s*=\s*"(\w+)"', statements)) == sids, "the statements that the prefix plan may add are not the Sids of infra/main.tf")
    for rule in state_migration.PREFIX_RULES:
        expect(
            problems, re.search(rf'id\s*=\s*"{rule}"', main_tf_text) is not None, f"the state rule {rule} that the prefix plan may add is not in infra/main.tf"
        )


NATIVE_PLANS = Path(__file__).with_name("state_migration_native_plans.json")
RECOVERY_ADMIN = f"user/{ADMIN_PROFILE}"
SHEET_PYTHON = """#!/bin/bash
# Stand-in of .venv/bin/python for the owner sheets: the state tool of this checkout, run against the scratch root.
[[ "$1" == -m && "$2" == scripts.infrastructure.state_migration ]] || { echo "unexpected command: $*" >&2; exit 99; }
shift 2
cd "$REPO_ROOT"
exec "$REAL_PYTHON" -m scripts.infrastructure.state_migration --root "$SCENE_ROOT" "$@"
"""


def native_plan(name: str) -> dict[str, Any]:
    """A saved plan of the prefix change as ``terraform show -json`` wrote it (the two resource changes of ``state_migration_native_plans.json``)."""
    return {"resource_changes": json.loads(NATIVE_PLANS.read_text(encoding="utf-8"))[name], "output_changes": {}}


def native_change(plan: dict[str, Any], address: str) -> dict[str, Any]:
    return cast(dict[str, Any], next(entry["change"] for entry in plan["resource_changes"] if entry["address"] == address))


def native_prefix_plans(problems: list[str]) -> None:
    """The plans that Terraform writes for the prefix change pass the shape check: a first apply (after_unknown has no ``rule`` key: all rule values are
    known), a retry after the policy was applied and a retry after both were. Values that are not known yet in another rule or attribute are still refused."""
    first = native_plan("enable")
    found = plan_shape_problems(first, PLAN_UPDATES)
    expect(problems, found == [], f"the plan of a first prefix apply, as Terraform writes it, must pass: {found}")
    rules = native_change(first, state_migration.PREFIX_LIFECYCLE)["after"]["rule"]
    other = next(index for index, rule in enumerate(rules) if rule["id"] not in state_migration.PREFIX_RULES)
    unknown_rule = native_plan("enable")
    marks: list[dict[str, bool]] = [{} for _ in rules]
    marks[other] = {"status": True}
    native_change(unknown_rule, state_migration.PREFIX_LIFECYCLE)["after_unknown"] = {"rule": marks}
    text = "; ".join(plan_shape_problems(unknown_rule, PLAN_UPDATES))
    expect(problems, "values not known yet" in text, f"a rule other than the state rules with values not known yet must be refused: {text!r}")
    unknown_attribute = native_plan("enable")
    native_change(unknown_attribute, state_migration.PREFIX_LIFECYCLE)["after_unknown"] = {"expected_bucket_owner": True}
    text = "; ".join(plan_shape_problems(unknown_attribute, PLAN_UPDATES))
    expect(problems, "an attribute other than the rules is not known yet" in text, f"another attribute not known yet must be refused: {text!r}")
    policy, lifecycle = frozenset({state_migration.PREFIX_POLICY}), frozenset({state_migration.PREFIX_LIFECYCLE})
    partial = native_plan("policy_applied")
    found = plan_shape_problems(partial, PLAN_UPDATES)
    expect(
        problems, found == [f"{state_migration.PREFIX_POLICY}: missing"], f"a retry after the policy update, with no live read, must still be refused: {found}"
    )
    found = plan_shape_problems(partial, PLAN_UPDATES, policy)
    expect(problems, found == [], f"a retry after the policy update must pass when the policy is live: {found}")
    text = "; ".join(plan_shape_problems(partial, PLAN_UPDATES, policy | lifecycle))
    expect(
        problems,
        "aws_s3_bucket_lifecycle_configuration.data: not in the plan as a no-op" in text,
        f"an update that the plan still holds cannot be declared live: {text!r}",
    )
    complete = native_plan("both_applied")
    found = plan_shape_problems(complete, PLAN_UPDATES, policy | lifecycle)
    expect(problems, found == [], f"a retry after both updates must pass when both are live: {found}")
    found = plan_shape_problems(complete, PLAN_UPDATES)
    expect(problems, len(found) == 2 and all(line.endswith(": missing") for line in found), f"no live read, no recovery: {found}")


def storage_shape_folder(scene: Scene, inputs: dict[str, str], problems: list[str]) -> Path:
    """Write the three-resource storage folder for the recovery cases and apply it once."""
    folder = scene.root / FOLDERS["storage"]
    write_storage_shape(folder, inputs)
    result = run_command("terraform", [f"-chdir={folder}", "apply", "-auto-approve", "-input=false"], timeout=STEP_SECONDS)
    expect(problems, result.returncode == 0, f"preparing the recovery folder failed: {result.stdout[-300:]}")
    return folder


def write_storage_shape(folder: Path, inputs: dict[str, str]) -> None:
    body = "\n".join(f'resource "terraform_data" "{name}" {{\n  input = "{value}"\n}}\n' for name, value in inputs.items())
    (folder / "main.tf").write_text(f'terraform {{\n  required_version = "~> 1.16.1"\n}}\n\n{STATE_VARIABLES}{body}', encoding="utf-8")


def save_storage_plan(scene: Scene, folder: Path, name: str) -> Path:
    path = scene.root / f"{name}.tfplan"
    result = run_command("terraform", [f"-chdir={folder}", "plan", "-input=false", "-no-color", "-out", str(path)], timeout=STEP_SECONDS)
    if result.returncode:
        raise RuntimeError(f"planning {name} failed: {result.stdout[-300:]}")
    return path


def approve_recovery(scene: Scene, plan: Path, *extra: str) -> tuple[int, str]:
    """``approve-plan`` as the prefix sheet runs it, in this process, against the stand-in bucket and with two stand-in resources."""
    arguments = ["--root", str(scene.root), "approve-plan", "--stack", "storage", "--plan", str(plan)]
    arguments += ["--update", "terraform_data.r0", "--update", "terraform_data.r1", *extra]
    names = {name: value for name, value in scene.environment().items() if name.startswith("AWS_")}
    buffer = io.StringIO()
    with mock.patch.dict(os.environ, names), contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
        try:
            code = state_migration.run(arguments)
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, buffer.getvalue()


@contextlib.contextmanager
def stand_in_updates() -> Iterator[None]:
    """Let the two stand-in resources of the recovery folder take the place of the policy and the lifecycle configuration: the same shape check and the same
    live read of the real update."""
    original = (dict(state_migration.DELTAS), dict(state_migration.LIVE_PARTS))
    state_migration.DELTAS.update({"terraform_data.r0": lambda _: None, "terraform_data.r1": lambda _: None})
    state_migration.LIVE_PARTS.update(
        {"terraform_data.r0": original[1][state_migration.PREFIX_POLICY], "terraform_data.r1": original[1][state_migration.PREFIX_LIFECYCLE]}
    )
    try:
        yield
    finally:
        state_migration.DELTAS.clear()
        state_migration.DELTAS.update(original[0])
        state_migration.LIVE_PARTS.clear()
        state_migration.LIVE_PARTS.update(original[1])


def approve_plan_recovery(template: Path, work: Path, problems: list[str]) -> None:
    """A pending record after a partial apply: the next plan holds only the remaining update. It passes only when the live bucket shows the update that the
    plan no longer holds, and only for the administrator of the pending record. Nothing left to apply passes too (the sheet then reads the bucket back)."""
    scene = fresh(template, work, "approve_recovery")
    admin = ["--state-admin", RECOVERY_ADMIN]
    with stand_in_updates(), StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        folder = storage_shape_folder(scene, {"r0": "value0", "r1": "value1", "r2": "value2"}, problems)
        write_storage_shape(folder, {"r0": "value0", "r1": "b", "r2": "value2"})
        one_left = save_storage_plan(scene, folder, "one_left")
        code, output = approve_recovery(scene, one_left)
        expect(
            problems,
            code == 1 and "terraform_data.r0: missing" in output,
            f"without the recovery option the partial plan is refused as before: {code}: {output}",
        )
        code, output = approve_recovery(scene, one_left, *admin)
        expect(problems, code == 1 and "pending state protection record" in output, f"no record, no recovery: exit {code}: {output}")
        scene.approve_state_admin(RECOVERY_ADMIN, status=VERIFIED)
        code, output = approve_recovery(scene, one_left, *admin)
        expect(problems, code == 1 and "pending state protection record" in output, f"a verified record needs no recovery: exit {code}: {output}")
        scene.approve_state_admin("role/another_admin", status=PENDING)
        code, output = approve_recovery(scene, one_left, *admin)
        expect(
            problems,
            code == 1 and "pending state protection record" in output,
            f"the record of another administrator gives no recovery: exit {code}: {output}",
        )
        scene.approve_state_admin(RECOVERY_ADMIN, status=PENDING)
        code, output = approve_recovery(scene, one_left, *admin)
        digest = hashlib.sha256(one_left.read_bytes()).hexdigest()
        expect(
            problems,
            code == 0 and "plan changes: 1\n" in output and f"plan sha256: {digest}\n" in output and "1 already live" in output,
            f"the plan of the remaining update must pass with its digest when the live bucket holds the other: exit {code}: {output}",
        )
        policy = bucket.policy
        bucket.policy = None
        code, output = approve_recovery(scene, one_left, *admin)
        expect(
            problems,
            code == 1 and "terraform_data.r0" in output and "does not show it" in output and "plan sha256" not in output,
            f"an update that is not live must not be skipped: exit {code}: {output}",
        )
        bucket.policy = policy
        write_storage_shape(folder, {"r0": "value0", "r1": "b"})
        wrong = save_storage_plan(scene, folder, "wrong_rest")
        code, output = approve_recovery(scene, wrong, *admin)
        expect(problems, code == 1 and "terraform_data.r2: delete" in output, f"the remaining changes are checked as before: exit {code}: {output}")
        write_storage_shape(folder, {"r0": "a", "r1": "value1", "r2": "value2"})
        other_left = save_storage_plan(scene, folder, "other_left")
        bucket.lifecycle = ["abort_incomplete_uploads"]
        code, output = approve_recovery(scene, other_left, *admin)
        expect(
            problems,
            code == 1 and "terraform_data.r1" in output and "does not show it" in output,
            f"a lifecycle update that is not live must not be skipped: exit {code}: {output}",
        )
        scene.protect()
        code, output = approve_recovery(scene, other_left, *admin)
        expect(problems, code == 0 and "plan changes: 1\n" in output, f"the policy update left, with the lifecycle rule live, must pass: exit {code}: {output}")
        write_storage_shape(folder, {"r0": "value0", "r1": "value1", "r2": "value2"})
        result = run_command("terraform", [f"-chdir={folder}", "apply", "-auto-approve", "-input=false"], timeout=STEP_SECONDS)
        expect(problems, result.returncode == 0, f"resetting the recovery folder failed: {result.stdout[-300:]}")
        nothing_left = save_storage_plan(scene, folder, "nothing_left")
        code, output = approve_recovery(scene, nothing_left, *admin)
        expect(problems, code == 0 and "plan changes: 0\n" in output, f"a plan with nothing left passes while both parts are live: exit {code}: {output}")
        bucket.lifecycle = ["abort_incomplete_uploads"]
        code, output = approve_recovery(scene, nothing_left, *admin)
        expect(problems, code == 1 and "does not show it" in output, f"a plan with nothing left must not pass without the live parts: exit {code}: {output}")


def first_prefix_apply_needs_no_record(template: Path, work: Path, problems: list[str]) -> None:
    """The first prefix apply has no pending record: the sheet passes the administrator and the plan holds both updates, so the plan is approved with no
    record and no live read. A plan that omits an update still needs the pending record of that administrator."""
    scene = fresh(template, work, "first_apply")
    admin = ["--state-admin", RECOVERY_ADMIN]
    with stand_in_updates(), StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        folder = storage_shape_folder(scene, {"r0": "value0", "r1": "value1", "r2": "value2"}, problems)
        write_storage_shape(folder, {"r0": "a", "r1": "b", "r2": "value2"})
        both = save_storage_plan(scene, folder, "both_left")
        write_storage_shape(folder, {"r0": "value0", "r1": "b", "r2": "value2"})
        one = save_storage_plan(scene, folder, "one_left")
        digest = hashlib.sha256(both.read_bytes()).hexdigest()
        expected = f"plan changes: 2\nplan sha256: {digest}\n"
        for record, label in ((None, "no record"), (VERIFIED, "a verified record")):
            if record is not None:
                scene.approve_state_admin(RECOVERY_ADMIN, status=record)
            code, output = approve_recovery(scene, both, *admin)
            expect(
                problems,
                code == 0 and "0 already live" in output and output.endswith(expected),
                f"the first apply, whose plan holds both updates, must pass with {label}: exit {code}: {output}",
            )
        code, output = approve_recovery(scene, one, *admin)
        expect(
            problems,
            code == 1 and "pending state protection record" in output,
            f"a plan that omits an update must still need the pending record: exit {code}: {output}",
        )


def retry_checks_the_live_protection(template: Path, work: Path, problems: list[str]) -> None:
    """A retry that omits an update, and the read-back that marks the record verified, accept only a live bucket that passes the detailed checks of the
    sheet's final check: a deny aimed at another prefix, a rule that expires current state and another rule that does so are all refused."""
    scene = fresh(template, work, "retry_live")
    admin = ["--state-admin", RECOVERY_ADMIN]
    record = scene.root / STATE_PROTECTION_RECORD
    with stand_in_updates(), StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        folder = storage_shape_folder(scene, {"r0": "value0", "r1": "value1", "r2": "value2"}, problems)
        write_storage_shape(folder, {"r0": "value0", "r1": "b", "r2": "value2"})
        policy_left = save_storage_plan(scene, folder, "policy_omitted")
        write_storage_shape(folder, {"r0": "value0", "r1": "value1", "r2": "value2"})
        nothing_left = save_storage_plan(scene, folder, "nothing_left")
        scene.protect()
        scene.approve_state_admin(RECOVERY_ADMIN, status=PENDING)
        code, output = approve_recovery(scene, nothing_left, *admin)
        expect(problems, code == 0 and "plan changes: 0\n" in output, f"a bucket with the whole protection must pass: exit {code}: {output}")
        code, output = scene.protection("verify", "--state-admin", RECOVERY_ADMIN)
        expect(problems, code == 0 and json.loads(record.read_text())["status"] == VERIFIED, f"the read-back of a valid bucket must verify: {code}: {output}")
        for label, plan, damage in (
            ("a deny aimed at another prefix", policy_left, POLICY_MUTATIONS["deny_prefix"]),
            ("an allow that is not plain", policy_left, POLICY_MUTATIONS["allow_effect"]),
            ("an unconditional deny that reaches the state key", policy_left, POLICY_MUTATIONS["other_deny"]),
            (
                "a state rule that expires the current state",
                nothing_left,
                lambda scene: setattr(scene.bucket, "lifecycle", LIFECYCLE_MUTATIONS["lifecycle_current"]),
            ),
            ("another rule that expires every object", nothing_left, lambda scene: setattr(scene.bucket, "lifecycle", LIFECYCLE_MUTATIONS["lifecycle_other"])),
        ):
            scene.protect()
            damage(scene)
            scene.approve_state_admin(RECOVERY_ADMIN, status=PENDING)
            code, output = approve_recovery(scene, plan, *admin)
            expect(
                problems,
                code == 1 and "does not show it" in output and "plan sha256" not in output,
                f"a retry must not be approved with {label}: exit {code}: {output}",
            )
            code, output = scene.protection("verify", "--state-admin", RECOVERY_ADMIN)
            expect(
                problems,
                code == 2 and json.loads(record.read_text())["status"] == PENDING,
                f"the read-back must not verify a bucket with {label}: exit {code}: {output}",
            )


def switched_resume_needs_the_saved_list(template: Path, work: Path, problems: list[str]) -> None:
    """After the switch the plan before is not generated again: with the saved list missing, the plan step and both move sheets stop, the record stays
    ``switched`` and no second copy is made."""
    for sheet, stack, arguments, answers in (
        ("state_migration.sh", "jobs", ["jobs"], "migrate\n"),
        ("state_migration_iam.sh", "iam", [], f"{ADMIN_PROFILE}\nmigrate\n"),
    ):
        label = f"{sheet} with the saved list missing"
        scene = fresh(template, work, f"missing_list_{stack}")
        with StateBucket(KEYS, IDENTITIES) as bucket:
            scene.bucket = bucket
            scene.protect()
            for command, extra in (("plan", ["--phase", "before"]), ("migrate", [])):
                code, output = scene.run(command, stack, *extra, *scene.admin(stack))
                expect(problems, code == 0, f"{label}: {command}: exit {code}: {output}")
            (scene.root / RECORD_FOLDER / f"{stack}_before.changes.txt").unlink()
            state_puts = len([seen for seen in bucket.requests("PUT") if seen.key == scene.key(stack)])
            code, output = scene.run("plan", stack, "--phase", "before", *scene.admin(stack))
            expect(problems, code == 1 and "saved list from before the move is missing" in output, f"{label}: the plan step must stop: exit {code}: {output}")
            code, output = run_move_sheet(scene, sheet, arguments, answers)
            expect(
                problems,
                code != 0 and "saved list from before the move is missing" in output and "plans: equal" not in output,
                f"{label}: the sheet must stop at the plan before: exit {code}: {output}",
            )
            expect(problems, json.loads(scene.record(stack).read_text())["stage"] == "switched", f"{label}: the record must stay switched")
            expect(
                problems,
                len([seen for seen in bucket.requests("PUT") if seen.key == scene.key(stack)]) == state_puts,
                f"{label}: the state was copied again",
            )


def run_move_sheet(scene: Scene, sheet: str, arguments: list[str], answers: str) -> tuple[int, str]:
    """One move sheet as delivered, in the scratch root, with the state tool of this checkout behind it and the typed answers on its input."""
    (scene.root / "scripts/infrastructure/owner").mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / "scripts/infrastructure/owner" / sheet, scene.root / "scripts/infrastructure/owner" / sheet)
    shim = scene.root / ".venv/bin/python"
    shim.parent.mkdir(parents=True, exist_ok=True)
    shim.write_text(SHEET_PYTHON, encoding="utf-8")
    shim.chmod(0o755)
    if not (scene.root / ".git").exists():
        run_command("git", ["init", "-q"], cwd=scene.root, timeout=STEP_SECONDS)
    (scene.root / "answers.txt").write_text(answers, encoding="utf-8")
    environment = {**scene.environment(), "REPO_ROOT": str(ROOT), "SCENE_ROOT": str(scene.root), "REAL_PYTHON": sys.executable, "SHEET": sheet}
    environment["SHEET_ARGUMENTS"] = " ".join(arguments)
    result = run_command(
        "bash",
        ["-c", 'bash "scripts/infrastructure/owner/$SHEET" $SHEET_ARGUMENTS < answers.txt'],
        cwd=scene.root,
        env=environment,
        timeout=STEP_SECONDS * 4,
        own_session=True,
    )
    return result.returncode, result.stdout + result.stderr


def resumed_move_sheets(template: Path, work: Path, problems: list[str]) -> None:
    """A stop after the switch and before the plan after: the project profile sheet and the iam sheet start again, resume at the plan after without any
    copy, and only the verification marks the stack done. A second run of a finished stack is still refused at the first step."""
    for sheet, stack, arguments, answers in (
        ("state_migration.sh", "jobs", ["jobs"], "migrate\n"),
        ("state_migration_iam.sh", "iam", [], f"{ADMIN_PROFILE}\nmigrate\n"),
    ):
        label = f"{sheet} after a stop at the switch"
        scene = fresh(template, work, f"sheet_{stack}")
        with StateBucket(KEYS, IDENTITIES) as bucket:
            scene.bucket = bucket
            scene.protect()
            for command, extra in (("plan", ["--phase", "before"]), ("migrate", [])):
                code, output = scene.run(command, stack, *extra, *scene.admin(stack))
                expect(problems, code == 0, f"{label}: {command}: exit {code}: {output}")
            expect(problems, json.loads(scene.record(stack).read_text())["stage"] == "switched", f"{label}: the move was not recorded as switched only")
            state_puts = len([seen for seen in bucket.requests("PUT") if seen.key == scene.key(stack)])
            code, output = run_move_sheet(scene, sheet, arguments, answers)
            expect(problems, code == 0 and "check: resume" in output, f"{label}: the sheet must resume at its first step: exit {code}: {output}")
            expect(
                problems,
                "plan after: no change" in output and "verify: equal" in output,
                f"{label}: the sheet did not reach the plan after and the verification: {output}",
            )
            expect(
                problems,
                len([seen for seen in bucket.requests("PUT") if seen.key == scene.key(stack)]) == state_puts,
                f"{label}: the resumed sheet copied the state again",
            )
            expect(problems, json.loads(scene.record(stack).read_text())["stage"] == "done", f"{label}: the verification did not record done")
            code, output = run_move_sheet(scene, sheet, arguments, answers)
            expect(
                problems,
                code != 0 and "backend already switched" in output,
                f"{label}: a finished stack must be refused at the first step: exit {code}: {output}",
            )


def failed_restore(template: Path, work: Path, problems: list[str]) -> None:
    """A failed read through the backend whose restore also fails ends with its own exit code and message, never the line that says the stack is back."""
    stack = "jobs"
    scene = fresh(template, work, "restore_fails")
    shim = scene.root / "shim"
    shim.mkdir()
    program = shim / "terraform"
    program.write_text(
        '#!/bin/bash\ncase "$1" in -chdir=*) folder="${1#-chdir=}" ;; esac\n'
        'if [[ " $* " == *" state pull "* ]] && ! grep -q \'"type": "s3"\' "$folder/.terraform/terraform.tfstate" 2>/dev/null; then exit 1; fi\n'
        f'exec "{shutil.which("terraform")}" "$@"\n',
        encoding="utf-8",
    )
    program.chmod(0o755)
    with StateBucket(KEYS, IDENTITIES) as bucket:
        scene.bucket = bucket
        scene.protect()
        scene.run("plan", stack, "--phase", "before")
        bucket.faults.corrupt_get_after = {scene.key(stack): 1}
        code, output = scene.run("migrate", stack, environment={"PATH": f"{shim}:{os.environ['PATH']}"})
        expect(problems, code == 3 and "restore failed" in output, f"a failed restore must exit with its own code 3: exit {code}: {output}")
        expect(
            problems,
            "NOT back on its local state" in output and "; the stack is back on its local state" not in output,
            f"a failed restore must not claim recovery: {output}",
        )
        expect(problems, "backend: restored" not in output and "migration: done" not in output, f"a failed restore printed a success line: {output}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True, help="A new folder for the report.")
    parser.add_argument("--cases", default="", help="Comma-separated case names to run (the function names); all when omitted.")
    arguments = parser.parse_args()
    chosen = {name for name in arguments.cases.split(",") if name}
    arguments.out.mkdir(parents=True, exist_ok=False)
    problems: list[str] = []
    report: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="state_migration_e2e_") as temporary:
        work = Path(temporary)
        template = work / "template"
        template.mkdir()
        Scene.template(template)
        if not chosen or "full_run" in chosen:
            scene = fresh(template, work, "full")
            with StateBucket(KEYS, IDENTITIES) as bucket:
                scene.bucket = bucket
                scene.protect()
                full_run(scene, problems, report)
                second_run(scene, problems)
        cases = (
            interrupted_and_mismatch,
            refusals,
            iam_deny_exception,
            tampered_verification,
            output_only_plan,
            resumed_runs,
            storage_protection,
            approved_plan_shape,
            profile_split,
            symlinked_state,
            backend_contract,
            protection_fails_closed,
            lifecycle_other_prefixes_accepted,
            failed_restore,
            approve_plan_recovery,
            first_prefix_apply_needs_no_record,
            retry_checks_the_live_protection,
            resumed_move_sheets,
            switched_resume_needs_the_saved_list,
        )
        for case in cases:
            if not chosen or case.__name__ in chosen:
                case(template, work, problems)
    report["problems"] = problems
    (arguments.out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for problem in problems:
        sys.stdout.write(f"FAIL: {problem}\n")
    sys.stdout.write(f"state migration E2E: {'FAILED' if problems else 'passed'} ({len(problems)} problem(s)); report {arguments.out / 'report.json'}\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
