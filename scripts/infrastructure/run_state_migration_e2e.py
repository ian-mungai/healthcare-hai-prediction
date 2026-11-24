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
- the backend text: valid, no bucket, account, region or profile in it, and the same state keys as the bucket policy of ``infra/main.tf``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

from scripts.infrastructure.state_migration import RECORD_FOLDER, STACKS, backend_text
from scripts.infrastructure.state_migration_s3_stand_in import ACCOUNT, BUCKET, StateBucket
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
log="$PWD/wrapper_calls.log"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --iam | --platform | --services | --jobs | --alerts) stack="infra/${1#--}"; shift ;;
    --admin-profile) admin="$2"; shift 2 ;;
    *) break ;;
  esac
done
[[ "$stack" != infra/iam || -n "$admin" ]] || { echo "IAM updates require --admin-profile." >&2; exit 2; }
printf '%s admin=%s\\n' "$stack" "${admin:-none}" >> "$log"
exec terraform -chdir="$stack" "$@" -input=false
"""


def text_of(path: Path) -> str | None:
    return path.read_text(encoding="utf-8") if path.is_file() else None


def main_tf(count: int) -> str:
    resources = "\n".join(f'resource "terraform_data" "r{index}" {{\n  input = "value{index}"\n}}\n' for index in range(count))
    return f'terraform {{\n  required_version = "~> 1.16.1"\n}}\n\n{resources}'


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
            (folder / "main.tf").write_text(main_tf(count), encoding="utf-8")
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

    def protect(self) -> None:
        """Give the stand-in bucket the state protection the migration requires: versioning, the lifecycle rule and the bucket policy statements."""
        self.bucket.versioning = "Enabled"
        self.bucket.lifecycle = ["abort_incomplete_uploads", "expire_replaced_state_versions"]
        keys = [f"arn:aws:s3:::{BUCKET}/terraform_state/{stack}/terraform.tfstate" for stack in STACKS if stack != "iam"]
        self.bucket.policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Sid": "ProjectUserUsesItsStateKeys", "Effect": "Allow", "Resource": keys},
                {"Sid": "ProjectUserHoldsItsStateLocks", "Effect": "Allow", "Resource": [f"{key}.tflock" for key in keys]},
                {"Sid": "DenyStatePrefixToEveryoneElse", "Effect": "Deny", "Resource": [f"arn:aws:s3:::{BUCKET}/terraform_state/*"]},
                {"Sid": "DenyIamStateToProjectIdentities", "Effect": "Deny", "Resource": [f"arn:aws:s3:::{BUCKET}/terraform_state/iam/*"]},
            ],
        }


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True, help="A new folder for the report.")
    arguments = parser.parse_args()
    arguments.out.mkdir(parents=True, exist_ok=False)
    problems: list[str] = []
    report: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="state_migration_e2e_") as temporary:
        work = Path(temporary)
        template = work / "template"
        template.mkdir()
        Scene.template(template)
        scene = fresh(template, work, "full")
        with StateBucket(KEYS, IDENTITIES) as bucket:
            scene.bucket = bucket
            scene.protect()
            full_run(scene, problems, report)
            second_run(scene, problems)
        interrupted_and_mismatch(template, work, problems)
        refusals(template, work, problems)
        profile_split(template, work, problems)
        symlinked_state(template, work, problems)
        backend_contract(template, work, problems)
    report["problems"] = problems
    (arguments.out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for problem in problems:
        sys.stdout.write(f"FAIL: {problem}\n")
    sys.stdout.write(f"state migration E2E: {'FAILED' if problems else 'passed'} ({len(problems)} problem(s)); report {arguments.out / 'report.json'}\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
