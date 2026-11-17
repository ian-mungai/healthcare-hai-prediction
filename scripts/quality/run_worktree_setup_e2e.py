"""E2E for the worktree setup command: every Git-ignored input of a new worktree is made or checked from the main checkout, and nothing is overwritten.

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_worktree_setup_e2e``. Every case uses a throwaway Git repository with real
worktrees in a temporary folder, never the project repository. A stand-in ``terraform`` on the PATH checks its whole argument vector and its working
folder, records its arguments and makes the folders a real ``terraform init -backend=false`` would make from the lock file and the plugin folder, and a
stand-in file list replaces the dependency lock export (which needs ``uv``), so the cases run with no network. The cases cover links, tracked aliases of
a destination (found through the filesystem, so a case-insensitive volume such as APFS gets the refusals and a case-sensitive one does not), races,
interruptions and recovery. Results go to ``data/e2e/worktree_setup/<UTC>/report.json``.
"""

from __future__ import annotations

import errno
import io
import json
import os
import shutil
import stat
import sys
import tempfile
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import clear_git_environment, run_command
from scripts.quality import worktree_setup

ROOT = Path(__file__).resolve().parents[2]
PIN_FILE = "services/orchestration/constraints-3.12.txt"
PINS = "apache-airflow==3.1.0\n"
STACKS = ("infra", "infra/iam", "infra/services", "infra/platform")
TREES = ("data/analytics/dbt/dbt_packages", "data/analytics/dbt/package_archive", *(f"{stack}/.terraform" for stack in STACKS))
PROVIDER_FOLDER = "providers/registry.terraform.io/hashicorp/aws/6.66.0/linux_amd64"
PROVIDER_FILE = "terraform-provider-aws_v6.66.0_x5"
JOBS_PROVIDER = f"infra/jobs/.terraform/{PROVIDER_FOLDER}/{PROVIDER_FILE}"
BINARY = bytes(range(256)) * 1200 + b"\0tail"
GITIGNORE = ".env\n*.tfvars.json\n.terraform/\n/data/\n/.worktrees/\n/services/orchestration/constraints-3.12.txt\n"
INFO_EXCLUDE = "/.venv\n/.tools\n"
LOCK = 'provider "registry.terraform.io/hashicorp/aws" {\n  version     = "6.66.0"\n  constraints = "~> 6.0"\n}\n'
TERRAFORM = r"""#!/bin/sh
printf '%s\n' "$*" >> "$TERRAFORM_LOG"
[ "$*" = "$TERRAFORM_EXPECTED" ] || { echo "unexpected terraform arguments: $*" >&2; exit 64; }
[ "$(pwd -P)" = "$TERRAFORM_CWD" ] || { echo "unexpected working folder: $(pwd -P)" >&2; exit 65; }
[ "$TERRAFORM_FAIL" = before ] && { echo "Error: the provider cannot be installed" >&2; exit 1; }
cd "${1#-chdir=}" || exit 66
mkdir -p .terraform/modules .terraform/providers
modules=$(awk '
/^module "/ { n = $2; gsub(/"/, "", n) }
/^ *source *=/ && n != "" { split($0, a, "\""); printf ",{\"Key\":\"%s\",\"Source\":\"%s\",\"Dir\":\"../services/tests/flow\"}", n, a[2]; n = "" }
' *.tf)
manifest='{"Modules":[{"Key":"","Source":"","Dir":"."}'"$modules"']}'
[ -z "$TERRAFORM_MANIFEST" ] || manifest=$TERRAFORM_MANIFEST
printf '%s\n' "$manifest" > .terraform/modules/modules.json
[ "$TERRAFORM_FAIL" = after_modules ] && { echo "Error: provider installation failed" >&2; exit 1; }
version=$(sed -n 's/^ *version *= *"\(.*\)"/\1/p' .terraform.lock.hcl | head -n 1)
plugin=${5#-plugin-dir=}
for platform in "$plugin"/registry.terraform.io/hashicorp/aws/"$version"/*/; do
  platform=${platform%/}
  target=.terraform/providers/registry.terraform.io/hashicorp/aws/$version/$(basename "$platform")
  mkdir -p "$(dirname "$target")"
  rm -rf "$target"
  ln -s "$platform" "$target"
done
"""

Case = Callable[["Scene"], list[str]]


class Interrupted(BaseException):
    """A stand-in for a kill or an interrupt that no handler of the command may swallow."""


def git(folder: Path, *arguments: str) -> str:
    return run_command("git", ["-C", str(folder), *arguments], timeout=120, check=True).stdout.strip()


def write(path: Path, text: str | bytes, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode() if isinstance(text, str) else text)
    path.chmod(mode)


def fs_equates(folder: Path, first: str, second: str) -> bool:
    """True when the filesystem of the folder treats the two names as one entry (case or normalisation insensitive volume)."""
    folder.mkdir(parents=True, exist_ok=True)
    probe = folder / first
    probe.write_text("probe")
    try:
        return (folder / second).exists() and (folder / second).samefile(probe)
    finally:
        probe.unlink()


class Scene:
    """A throwaway repository with the ignored inputs of a main checkout and one fresh worktree under ``.worktrees``."""

    def __init__(self, top: Path, *, ignore: str = GITIGNORE) -> None:
        self.top = top
        self.main = top / "main"
        self.main.mkdir()
        git(self.main, "init", "--quiet", "-b", "main")
        git(self.main, "config", "user.email", "case@example.invalid")
        git(self.main, "config", "user.name", "case")
        write(self.main / ".gitignore", ignore)
        write(self.main / "README.md", "tracked\n")
        write(self.main / "infra/jobs/.terraform.lock.hcl", LOCK)
        write(self.main / "infra/jobs/main.tf", 'module "flow" {\n  source = "../services/tests/flow"\n}\n')
        write(self.main / "infra/services/tests/flow/main.tf", "# flow\n")
        git(self.main, "add", ".gitignore", "README.md", "infra")
        git(self.main, "commit", "--quiet", "-m", "chore: start")
        with (self.main / ".git/info/exclude").open("a") as handle:
            handle.write(INFO_EXCLUDE)
        for name in (".venv", ".tools"):
            write(self.main / name / "marker.txt", f"{name}\n")
        write(self.main / "data/analytics/dbt/dbt_packages/dbt_utils/dbt_project.yml", "name: dbt_utils\n")
        write(self.main / "data/analytics/dbt/dbt_packages/dbt_utils/macros/binary.bin", BINARY, 0o755)
        write(self.main / "data/analytics/dbt/package_archive/95b9/archive.tgz", "archive\n")
        for stack in STACKS:
            write(self.main / stack / ".terraform/terraform.tfstate", "backend marker that holds state\n")
        for stack in ("infra", "infra/platform"):
            write(self.main / stack / ".terraform" / PROVIDER_FOLDER / PROVIDER_FILE, BINARY + stack.encode(), 0o755)
            write(self.main / stack / ".terraform" / PROVIDER_FOLDER / "LICENSE.txt", f"license of {stack}\n")
        for stack in ("infra/iam", "infra/services"):
            link = self.main / stack / ".terraform" / PROVIDER_FOLDER
            link.parent.mkdir(parents=True)
            link.symlink_to(self.main / "infra/.terraform" / PROVIDER_FOLDER)
        write(self.main / ".env", "AWS_REGION=us-fake-1\n")
        write(self.main / "infra/deployment.auto.tfvars.json", '{"environment": "fake"}\n', 0o600)
        self.worktree = self.main / ".worktrees" / "feat-case"
        git(self.main, "worktree", "add", "--quiet", "-b", "feat/case", str(self.worktree))
        self.victim = top / "victim"
        self.victim.mkdir()
        self.bin = top / "bin"
        self.log = top / "terraform.log"
        self.bin.mkdir()
        write(self.bin / "terraform", TERRAFORM, 0o755)
        self.generated = {PIN_FILE: PINS}

    def scratch(self) -> Path:
        """The command's private folder inside the worktree's Git directory."""
        return Path(git(self.worktree, "rev-parse", "--absolute-git-dir")) / "worktree_setup"

    def track_in_index(self, path: str, text: str = "tracked elsewhere\n") -> None:
        """Add an index entry with no file on disk, the way a tracked path of another spelling or an unchecked-out path exists."""
        blob = self.top / "blob.txt"
        blob.write_text(text)
        digest = git(self.worktree, "hash-object", "-w", str(blob))
        git(self.worktree, "update-index", "--add", "--replace", "--cacheinfo", f"100644,{digest},{path}")

    def run(
        self,
        *extra: str,
        path: Path | None = None,
        terraform_mode: str = "",
        around: Any = None,
        interrupted_ok: bool = False,
        expected: str | None = None,
        manifest: str = "",
    ) -> tuple[int, str]:
        """Run the command on the worktree (or ``path``); returns its exit code (-1 after a simulated interruption) and everything it printed."""
        worktree = (path or self.worktree).resolve()
        environment = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "TERRAFORM_LOG": str(self.log),
            "TERRAFORM_FAIL": terraform_mode,
            "TERRAFORM_CWD": str(worktree),
            "TERRAFORM_EXPECTED": expected or self.vector(worktree),
            "TERRAFORM_MANIFEST": manifest,
        }
        printed = io.StringIO()
        generated = mock.patch.object(worktree_setup, "generated_files", lambda root: self.generated)
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, environment))
            stack.enter_context(generated)
            stack.enter_context(redirect_stdout(printed))
            stack.enter_context(redirect_stderr(printed))
            if around is not None:
                stack.enter_context(around)
            try:
                code = worktree_setup.main([str(path or self.worktree), *extra])
            except Interrupted:
                if not interrupted_ok:
                    raise
                code = -1
        return code, printed.getvalue()

    def vector(self, worktree: Path | None = None) -> str:
        """The full argument vector a correct jobs init passes to terraform."""
        worktree = (worktree or self.worktree).resolve()
        return f"-chdir={worktree}/infra/jobs init -backend=false -input=false -plugin-dir={worktree}/infra/.terraform/providers"

    def terraform_calls(self) -> list[str]:
        return self.log.read_text().splitlines() if self.log.exists() else []


def expect(problems: list[str], condition: bool, text: str) -> None:
    if not condition:
        problems.append(text)


def snapshot(folder: Path) -> dict[str, tuple[Any, float]]:
    """Every file and link below the folder with its bytes or target and its modification time."""
    found: dict[str, tuple[Any, float]] = {}
    for path in sorted(folder.rglob("*")):
        key = path.relative_to(folder).as_posix()
        if ".git" in path.relative_to(folder).parts[:1] or key.startswith(".worktrees"):
            continue
        if path.is_symlink():
            found[key] = (f"-> {os.readlink(path)}", 0.0)
        elif path.is_file():
            found[key] = (path.read_bytes(), path.stat().st_mtime_ns)
    return found


def files_below(folder: Path) -> list[str]:
    return sorted(path.relative_to(folder).as_posix() for path in folder.rglob("*") if path.is_file() or path.is_symlink()) if folder.is_dir() else []


@contextmanager
def before_publish(action: Callable[[str], None]) -> Iterator[dict[str, int]]:
    """Run ``action(name)`` just before each publication of a file by the command (its ``os.link`` call); the real call still runs afterwards."""
    real = os.link
    seen = {"calls": 0}

    def wrapper(source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        seen["calls"] += 1
        action(str(destination))
        real(source, destination, *args, **kwargs)

    with mock.patch.object(os, "link", wrapper):
        yield seen


def fresh_setup(scene: Scene) -> list[str]:
    problems: list[str] = []
    code, output = scene.run()
    wt = scene.worktree
    expect(problems, code == 0, f"exit {code}: {output}")
    for name in (".venv", ".tools"):
        expect(
            problems, (wt / name).is_symlink() and (wt / name).resolve() == (scene.main / name).resolve(), f"{name} is not a link to the main checkout's folder"
        )
    for tree in TREES:
        for path in sorted((scene.main / tree).rglob("*")):
            relative = path.relative_to(scene.main)
            target = wt / relative
            if path.suffix == ".tfstate" or path.is_dir() and not path.is_symlink():
                continue
            if path.is_symlink():
                expected = str(wt / Path(os.readlink(path)).relative_to(scene.main))
                expect(problems, target.is_symlink() and os.readlink(target) == expected, f"{relative} is not a link to the worktree's own copy ({expected})")
                expect(problems, target.is_dir(), f"{relative} is a dangling link")
            else:
                same_bytes = target.is_file() and not target.is_symlink() and target.read_bytes() == path.read_bytes()
                expect(problems, same_bytes, f"{relative} differs from the main checkout's file or is missing")
                expect(problems, target.is_file() and stat.S_IMODE(target.stat().st_mode) == stat.S_IMODE(path.stat().st_mode), f"{relative} lost its mode")
    for stack in STACKS:
        expect(problems, not list((wt / stack / ".terraform").rglob("*.tfstate*")), f"{stack}/.terraform holds a state file")
    expect(problems, (wt / ".env").read_bytes() == (scene.main / ".env").read_bytes(), ".env was not copied byte for byte")
    expect(problems, stat.S_IMODE((wt / ".env").stat().st_mode) == 0o600, ".env is not mode 0600")
    expect(problems, (wt / "infra/deployment.auto.tfvars.json").read_bytes() == (scene.main / "infra/deployment.auto.tfvars.json").read_bytes(), "tfvars bytes")
    expect(problems, stat.S_IMODE((wt / "infra/deployment.auto.tfvars.json").stat().st_mode) == 0o600, "the tfvars file is not mode 0600")
    expect(problems, (wt / PIN_FILE).read_bytes() == PINS.encode(), "the pin file was not generated byte for byte")
    calls = scene.terraform_calls()
    expect(problems, calls == [scene.vector()], f"terraform calls: {calls}")
    provider = wt / JOBS_PROVIDER
    expect(
        problems,
        provider.is_file() and provider.read_bytes() == (scene.main / "infra/.terraform" / PROVIDER_FOLDER / PROVIDER_FILE).read_bytes(),
        "jobs provider",
    )
    expect(problems, (wt / "infra/jobs/.terraform/modules/modules.json").is_file(), "the jobs stack was not initialised")
    expect(problems, git(wt, "status", "--porcelain") == "", "the setup left a change Git can see")
    expect(problems, not list(scene.scratch().rglob("*.part")), "a staging file was left behind")
    for item in (".venv", ".tools", PIN_FILE, "infra/jobs", ".env"):
        expect(problems, item in output, f"the output does not name {item}")
    return problems


def second_run_changes_nothing(scene: Scene) -> list[str]:
    problems: list[str] = []
    first, output = scene.run()
    before = snapshot(scene.worktree)
    code, output = scene.run()
    expect(problems, first == 0 and code == 0, f"exit {first} then {code}: {output}")
    expect(problems, snapshot(scene.worktree) == before, "a file changed or was added on the second run")
    expect(problems, len(scene.terraform_calls()) == 1, "terraform ran again")
    expect(problems, "created" not in output and "copied" not in output, f"the second run reports a change: {output}")
    return problems


def differing_file_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.run()
    target = scene.worktree / "data/analytics/dbt/dbt_packages/dbt_utils/dbt_project.yml"
    target.write_text("name: edited\n")
    code, output = scene.run()
    expect(problems, code == 1, f"exit {code}")
    expect(problems, "data/analytics/dbt/dbt_packages/dbt_utils/dbt_project.yml" in output and "differs" in output, f"the file is not named: {output}")
    expect(problems, target.read_text() == "name: edited\n", "the differing file was overwritten")
    return problems


def missing_source_named(scene: Scene) -> list[str]:
    problems: list[str] = []
    shutil.rmtree(scene.main / "data/analytics/dbt/package_archive")
    code, output = scene.run()
    expect(problems, code == 1, f"exit {code}")
    expect(problems, "data/analytics/dbt/package_archive" in output and "main checkout" in output, f"the missing item is not named: {output}")
    expect(problems, (scene.worktree / PIN_FILE).is_file() and (scene.worktree / ".env").is_file(), "the other items were not done")
    return problems


def empty_folder_filled(scene: Scene) -> list[str]:
    problems: list[str] = []
    (scene.worktree / "data/analytics/dbt/dbt_packages").mkdir(parents=True)
    code, output = scene.run()
    expect(problems, code == 0, f"exit {code}: {output}")
    expect(problems, (scene.worktree / "data/analytics/dbt/dbt_packages/dbt_utils/dbt_project.yml").is_file(), "the empty folder was not filled")
    return problems


def differing_env_kept(scene: Scene) -> list[str]:
    problems: list[str] = []
    write(scene.worktree / ".env", "AWS_REGION=us-other-1\n", 0o600)
    code, output = scene.run()
    expect(problems, code == 0, f"exit {code}: {output}")
    expect(problems, (scene.worktree / ".env").read_text() == "AWS_REGION=us-other-1\n", "the worktree's own .env was overwritten")
    expect(problems, ".env" in output and "kept" in output, f"the kept file is not reported: {output}")
    return problems


def link_to_another_folder_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    (scene.worktree / ".venv").symlink_to(scene.victim)
    code, output = scene.run()
    expect(problems, code == 1 and ".venv" in output, f"exit {code}: {output}")
    expect(problems, (scene.worktree / ".venv").resolve() == scene.victim.resolve(), "the link was replaced")
    return problems


def stale_pin_file_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    write(scene.worktree / PIN_FILE, "apache-airflow==2.0.0\n")
    code, output = scene.run()
    expect(problems, code == 1 and PIN_FILE in output and "differs" in output, f"exit {code}: {output}")
    expect(problems, (scene.worktree / PIN_FILE).read_text() == "apache-airflow==2.0.0\n", "the differing pin file was overwritten")
    return problems


def not_a_worktree_of_the_folder_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    before = snapshot(scene.main)
    elsewhere = scene.main.parent / "other_worktree"
    git(scene.main, "worktree", "add", "--quiet", "-b", "feat/other", str(elsewhere))
    for label, path in (("the main checkout", scene.main), ("a worktree outside .worktrees", elsewhere), ("a folder that is no worktree", scene.main / "data")):
        code, output = scene.run(path=path)
        expect(problems, code == 2 and "worktree" in output, f"{label}: exit {code}: {output}")
    expect(problems, snapshot(scene.main) == before, "a refused run changed the main checkout")
    expect(problems, not snapshot(elsewhere).keys() - {".git"} - set(git(elsewhere, "ls-files").splitlines()), "a refused run wrote into the other worktree")
    return problems


def terraform_failure_named(scene: Scene) -> list[str]:
    problems: list[str] = []
    code, output = scene.run(terraform_mode="before")
    expect(problems, code == 1 and "infra/jobs" in output and "provider cannot be installed" in output, f"exit {code}: {output}")
    expect(problems, (scene.worktree / PIN_FILE).is_file(), "the other items were not done")
    return problems


def destination_not_ignored_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    code, output = scene.run()
    expect(problems, code == 1 and "infra/deployment.auto.tfvars.json" in output and "not ignored" in output, f"exit {code}: {output}")
    expect(problems, not (scene.worktree / "infra/deployment.auto.tfvars.json").exists(), "a file was copied to a path Git does not ignore")
    return problems


def nested_link_in_a_copy_tree_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    folder = scene.worktree / "data/analytics/dbt/dbt_packages"
    folder.mkdir(parents=True)
    (folder / "dbt_utils").symlink_to(scene.victim)
    code, output = scene.run()
    expect(problems, code == 1 and "dbt_packages/dbt_utils" in output, f"exit {code}: {output}")
    expect(problems, files_below(scene.victim) == [], f"a write went through the link: {files_below(scene.victim)}")
    expect(problems, (folder / "dbt_utils").is_symlink() and (folder / "dbt_utils").resolve() == scene.victim.resolve(), "the link was replaced")
    expect(problems, (scene.worktree / PIN_FILE).is_file() and (scene.worktree / ".env").is_file(), "the other items were not done")
    return problems


def dangling_generated_link_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    outside = scene.victim / "pins.txt"
    (scene.worktree / PIN_FILE).parent.mkdir(parents=True)
    (scene.worktree / PIN_FILE).symlink_to(outside)
    code, output = scene.run()
    expect(problems, code == 1 and PIN_FILE in output and "link" in output, f"exit {code}: {output}")
    expect(problems, not outside.exists(), "the generated file was written through a dangling link")
    expect(problems, (scene.worktree / PIN_FILE).is_symlink(), "the link was replaced")
    return problems


def dangling_env_link_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    outside = scene.victim / "env_target"
    (scene.worktree / ".env").symlink_to(outside)
    code, output = scene.run()
    expect(problems, code == 1 and ".env" in output and "link" in output, f"exit {code}: {output}")
    expect(problems, not outside.exists(), ".env was written through a dangling link")
    return problems


def link_in_a_destination_parent_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    (scene.worktree / "infra/iam").symlink_to(scene.victim)
    shutil.rmtree(scene.main / "infra/iam/.terraform")
    write(scene.main / "infra/iam/.terraform/providers/x.bin", "x\n")
    code, output = scene.run()
    expect(problems, code == 1 and "infra/iam" in output, f"exit {code}: {output}")
    expect(problems, files_below(scene.victim) == [], f"a write went through the link: {files_below(scene.victim)}")
    expect(problems, (scene.worktree / "infra/platform/.terraform").is_dir(), "the other items were not done")
    return problems


def swap_during_the_run_cannot_redirect(scene: Scene) -> list[str]:
    problems: list[str] = []
    folder = scene.worktree / "data/analytics/dbt/dbt_packages"

    def swap(name: str) -> None:
        if folder.is_symlink() or not folder.is_dir():
            return
        folder.rename(folder.with_name("dbt_packages_moved"))
        folder.symlink_to(scene.victim)

    code, output = scene.run(around=before_publish(swap))
    expect(problems, files_below(scene.victim) == [], f"a write went through the swapped folder: {files_below(scene.victim)} (exit {code}: {output})")
    expect(problems, folder.is_symlink(), "the swap did not happen, so the case proved nothing")
    again, output = scene.run()
    expect(problems, again == 1 and "dbt_packages" in output, f"the next run did not refuse the link: exit {again}: {output}")
    expect(problems, files_below(scene.victim) == [], "the next run wrote through the link")
    return problems


def file_created_before_publish_not_replaced(scene: Scene) -> list[str]:
    problems: list[str] = []

    def rival(name: str) -> None:
        if name == ".env" and not (scene.worktree / ".env").exists():
            write(scene.worktree / ".env", "AWS_REGION=owner-wrote-this\n", 0o600)

    code, output = scene.run(around=before_publish(rival))
    expect(problems, (scene.worktree / ".env").read_text() == "AWS_REGION=owner-wrote-this\n", f".env was replaced (exit {code}: {output})")
    expect(problems, code == 1 and ".env" in output, f"the lost race is not reported: exit {code}: {output}")
    expect(problems, not list(scene.scratch().rglob("*.part")), "a staging file was left behind")
    return problems


def tracked_alias_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    probe = scene.top / "fs_probe"
    for index_name, destination in ((".toolſ", ".tools"), (".TOOLS", ".tools"), (".ENV", ".env")):
        aliased = fs_equates(probe, destination, index_name)
        shutil.rmtree(scene.worktree)
        git(scene.main, "worktree", "prune")
        git(scene.main, "worktree", "add", "--quiet", "-f", "-B", "feat/case", str(scene.worktree))
        scene.track_in_index(index_name)
        before = (scene.worktree / destination).exists() or (scene.worktree / destination).is_symlink()
        code, output = scene.run()
        made = (scene.worktree / destination).exists() or (scene.worktree / destination).is_symlink()
        label = f"{index_name} against {destination} (aliased on this volume: {aliased})"
        expect(problems, not before, f"{label}: the fixture already had the destination")
        if aliased:
            expect(problems, not made, f"{label}: {destination} was created through the alias of a tracked path")
            expect(problems, code == 1 and index_name in output and "tracked" in output, f"{label}: exit {code}: {output}")
        else:
            expect(problems, made and code == 0, f"{label}: a different name blocked the setup: exit {code}: {output}")
    return problems


def tracked_descendants_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    probe = scene.top / "fs_probe"
    tracked = (
        ("data/analytics/dbt/dbt_packages/dbt_utils/dbt_project.yml", "data/analytics/dbt/dbt_packages", True),
        ("Data/Analytics/dbt/dbt_packages/dbt_utils/dbt_project.yml", "data/analytics/dbt/dbt_packages", fs_equates(probe, "data", "Data")),
        (".tools/inner.txt", ".tools", True),
        ("infra", "infra/.terraform", True),
    )
    for index_name, destination, blocked in tracked:
        shutil.rmtree(scene.worktree)
        git(scene.main, "worktree", "prune")
        git(scene.main, "worktree", "add", "--quiet", "-f", "-B", "feat/case", str(scene.worktree))
        scene.track_in_index(index_name, "bytes that the main checkout does not have\n")
        index_before = git(scene.worktree, "ls-files", "-s")
        code, output = scene.run()
        made = (scene.worktree / destination).exists() or (scene.worktree / destination).is_symlink()
        label = f"tracked {index_name} against {destination}"
        if blocked:
            expect(problems, not made, f"{label}: the destination was written")
            expect(problems, code == 1 and "tracked" in output, f"{label}: exit {code}: {output}")
        else:
            expect(problems, made, f"{label}: a different name blocked the copy: {output}")
        expect(problems, git(scene.worktree, "ls-files", "-s") == index_before, f"{label}: the index changed")
    return problems


def tracked_other_names_do_not_block(scene: Scene) -> list[str]:
    problems: list[str] = []
    for name in (".toolz", ".tools_extra/x.txt", "data/analytics/dbt/dbt_packages_extra/x.txt", ".env_template"):
        scene.track_in_index(name)
    code, output = scene.run()
    expect(problems, code == 0, f"exit {code}: {output}")
    expect(
        problems, (scene.worktree / ".tools").is_symlink() and (scene.worktree / "data/analytics/dbt/dbt_packages").is_dir(), "a different name blocked a write"
    )
    return problems


def failed_init_is_retried(scene: Scene) -> list[str]:
    problems: list[str] = []
    first, output = scene.run(terraform_mode="after_modules")
    expect(problems, first == 1 and "infra/jobs" in output, f"the failed init is not reported: exit {first}: {output}")
    second, output = scene.run()
    expect(problems, second == 0, f"the retry did not finish: exit {second}: {output}")
    expect(problems, len(scene.terraform_calls()) == 2, f"the retry did not run terraform init again: {scene.terraform_calls()}")
    expect(problems, (scene.worktree / JOBS_PROVIDER).is_file(), "the provider is missing after the retry")
    third, output = scene.run()
    expect(problems, third == 0 and len(scene.terraform_calls()) == 2 and "verified infra/jobs" in output, f"a complete cache was not accepted: {output}")
    return problems


def incomplete_provider_reinitialised(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.run()
    platform = (scene.worktree / JOBS_PROVIDER).parent
    expect(problems, platform.is_symlink(), "the stand-in did not link the provider folder the way terraform does")

    def empty_copy() -> None:
        platform.unlink()
        platform.mkdir()
        (platform / PROVIDER_FILE).write_bytes(b"")

    for label, damage in (("an empty provider file", empty_copy), ("a missing provider folder", lambda: platform.unlink())):
        damage()
        code, output = scene.run()
        provider = scene.worktree / JOBS_PROVIDER
        expect(problems, code == 0 and provider.is_file() and provider.stat().st_size > 0, f"{label}: exit {code}: {output}")
    expect(problems, len(scene.terraform_calls()) == 3, f"terraform calls: {scene.terraform_calls()}")
    return problems


def empty_env_without_marker_not_ready(scene: Scene) -> list[str]:
    problems: list[str] = []
    write(scene.worktree / ".env", "", 0o600)
    code, output = scene.run()
    expect(problems, code == 1 and ".env" in output and "incomplete" in output, f"exit {code}: {output}")
    expect(problems, (scene.worktree / ".env").read_bytes() == b"", "the empty .env was overwritten")
    expect(problems, not output.strip().endswith(": ready"), f"an empty .env was called ready: {output}")
    return problems


def truncated_env_without_marker_not_ready(scene: Scene) -> list[str]:
    problems: list[str] = []
    write(scene.worktree / ".env", "AWS_REG", 0o600)
    code, output = scene.run()
    expect(problems, code == 1 and "incomplete" in output, f"exit {code}: {output}")
    expect(problems, (scene.worktree / ".env").read_text() == "AWS_REG", "the truncated .env was overwritten")
    return problems


def owner_changes_after_setup_are_kept(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.run()
    for text in ("", "AWS_REGION=us-fake-1\nEXTRA=1\n", "AWS_REGION=us-fake-1\n"):
        (scene.worktree / ".env").write_text(text)
        code, output = scene.run()
        expect(problems, code == 0 and (scene.worktree / ".env").read_text() == text, f"{text!r}: exit {code}: {output}")
    return problems


def interrupted_publish_leaves_nothing_partial(scene: Scene) -> list[str]:
    problems: list[str] = []
    fired = {"count": 0}

    def interrupt(name: str) -> None:
        fired["count"] += 1
        if fired["count"] == 3:
            raise Interrupted

    code, output = scene.run(around=before_publish(interrupt), interrupted_ok=True)
    expect(problems, code == -1 and fired["count"] == 3, f"the interruption did not fire: exit {code}: {output}")
    for path in sorted(scene.worktree.rglob("*")):
        relative = path.relative_to(scene.worktree)
        if relative.parts[0] in (".git", ".venv", ".tools") or path.is_symlink() or not path.is_file() or (scene.main / relative).suffix == ".tfstate":
            continue
        source = scene.main / relative
        expect(problems, not source.is_file() or path.read_bytes() == source.read_bytes(), f"a partial file stayed at {relative}")
    code, output = scene.run()
    expect(problems, code == 0, f"the retry did not finish: exit {code}: {output}")
    expect(problems, (scene.worktree / ".env").read_bytes() == (scene.main / ".env").read_bytes(), ".env after the retry")
    expect(problems, not list(scene.scratch().rglob("*.part")), "a staging file was left behind")
    return problems


def crash_before_marker_adopted(scene: Scene) -> list[str]:
    problems: list[str] = []

    def crash(*args: Any, **kwargs: Any) -> None:
        raise Interrupted

    code, output = scene.run(around=mock.patch.object(worktree_setup, "write_marker", crash), interrupted_ok=True)
    expect(problems, code == -1, f"the crash did not fire: exit {code}: {output}")
    expect(problems, (scene.worktree / ".env").read_bytes() == (scene.main / ".env").read_bytes(), ".env was not complete when the marker failed")
    code, output = scene.run()
    expect(problems, code == 0 and "created .env" not in output and "verified .env" in output, f"exit {code}: {output}")
    expect(problems, (scene.worktree / ".env").read_bytes() == (scene.main / ".env").read_bytes(), ".env changed on the retry")
    expect(problems, (scene.scratch() / "published.json").is_file(), "the marker was not written by the retry")
    return problems


def leftover_staging_removed(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.run()
    stage = scene.scratch() / "stage"
    write(stage / "deadbeef.part", "half a file")
    code, output = scene.run()
    expect(problems, code == 0, f"exit {code}: {output}")
    expect(problems, not list(scene.scratch().rglob("*.part")), "the leftover staging file stayed")
    return problems


def source_link_outside_main_not_copied(scene: Scene) -> list[str]:
    problems: list[str] = []
    link = scene.main / "infra/platform/.terraform/providers/outside"
    link.symlink_to(scene.victim)
    code, output = scene.run()
    expect(problems, code == 1 and "infra/platform/.terraform/providers/outside" in output, f"exit {code}: {output}")
    expect(
        problems, not (scene.worktree / "infra/platform/.terraform/providers/outside").is_symlink(), "a link to a folder outside the main checkout was copied"
    )
    expect(problems, files_below(scene.victim) == [], "something was written through the link")
    return problems


def pipe_in_place_of_a_file_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    os.mkfifo(scene.worktree / ".env")
    folder = scene.worktree / "data/analytics/dbt/dbt_packages/dbt_utils"
    folder.mkdir(parents=True)
    os.mkfifo(folder / "dbt_project.yml")
    code, output = scene.run()
    expect(problems, code == 1 and ".env" in output and "dbt_project.yml" in output, f"exit {code}: {output}")
    expect(
        problems,
        stat.S_ISFIFO((scene.worktree / ".env").lstat().st_mode) and stat.S_ISFIFO((folder / "dbt_project.yml").lstat().st_mode),
        "a pipe was replaced",
    )
    return problems


def tracked_path_below_the_jobs_cache_blocks_init(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.track_in_index("infra/jobs/.terraform/modules/modules.json", "{}\n")
    code, output = scene.run()
    expect(problems, code == 1 and "infra/jobs/.terraform" in output and "tracked" in output, f"exit {code}: {output}")
    expect(problems, scene.terraform_calls() == [] and not (scene.worktree / "infra/jobs/.terraform").exists(), "terraform ran or wrote below a tracked path")
    return problems


def link_leaving_the_jobs_cache_blocks_the_init(scene: Scene) -> list[str]:
    problems: list[str] = []
    cache = scene.worktree / "infra/jobs/.terraform"
    cache.mkdir(parents=True)
    (cache / "providers").symlink_to(scene.victim)
    code, output = scene.run()
    expect(problems, code == 1 and "infra/jobs" in output and "providers" in output and "link" in output, f"exit {code}: {output}")
    expect(problems, scene.terraform_calls() == [] and files_below(scene.victim) == [], "terraform ran or wrote through the link")
    return problems


def marker_file_with_wrong_shapes_is_ignored(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.run()
    marker = scene.scratch() / "published.json"
    for text in ('{"files": {".env": "text"}}', '["list"]', "not json"):
        marker.write_text(text)
        (scene.worktree / ".env").write_text("AWS_REGION=owner\n")
        code, output = scene.run()
        expect(problems, code == 0 and (scene.worktree / ".env").read_text() == "AWS_REGION=owner\n", f"{text!r}: exit {code}: {output}")
    return problems


def write_error_is_named_and_leaves_nothing_partial(scene: Scene) -> list[str]:
    problems: list[str] = []

    def full_disk(descriptor: int) -> None:
        raise OSError(errno.ENOSPC, "No space left on device")

    with mock.patch.object(os, "fsync", full_disk):
        code, output = scene.run()
    expect(problems, code == 1 and "No space left on device" in output and "Traceback" not in output, f"exit {code}: {output}")
    expect(problems, ".env" in output and "no partial file" in output, f"the item is not named: {output}")
    for path in sorted(scene.worktree.rglob("*")):
        relative = path.relative_to(scene.worktree)
        if relative.parts[0] not in (".git", ".venv", ".tools") and (scene.main / relative).is_file() and not path.is_dir():
            expect(problems, path.read_bytes() == (scene.main / relative).read_bytes(), f"a partial file was published at {relative}")
    expect(problems, not (scene.worktree / ".env").exists() and not list(scene.scratch().rglob("*.part")), ".env or a staging file was left")
    code, output = scene.run()
    expect(
        problems, code == 0 and (scene.worktree / ".env").read_bytes() == (scene.main / ".env").read_bytes(), f"the retry did not finish: exit {code}: {output}"
    )
    return problems


def jobs_cache(scene: Scene) -> Path:
    cache = scene.worktree / "infra/jobs/.terraform"
    (cache / "modules").mkdir(parents=True)
    return cache


def symlinked_manifest_blocks_the_init(scene: Scene) -> list[str]:
    problems: list[str] = []
    cache = jobs_cache(scene)
    readme = scene.worktree / "README.md"
    before = readme.read_bytes()
    (cache / "modules/modules.json").symlink_to(readme)
    code, output = scene.run()
    expect(problems, code == 1 and "infra/jobs" in output and "modules.json" in output and "link" in output, f"exit {code}: {output}")
    expect(problems, readme.read_bytes() == before, "terraform init wrote through the link into a tracked file")
    expect(problems, scene.terraform_calls() == [], "terraform ran")
    return problems


def linked_modules_folder_blocks_the_init(scene: Scene) -> list[str]:
    problems: list[str] = []
    cache = scene.worktree / "infra/jobs/.terraform"
    cache.mkdir(parents=True)
    protected = scene.victim / "protected.json"
    protected.write_text("{}\n")
    (scene.worktree / "aux").mkdir()
    (scene.worktree / "aux/modules.json").symlink_to(protected)
    (cache / "modules").symlink_to(scene.worktree / "aux")
    code, output = scene.run()
    expect(problems, code == 1 and "infra/jobs" in output and "modules" in output and "link" in output, f"exit {code}: {output}")
    expect(problems, protected.read_text() == "{}\n", "terraform init wrote outside the worktree through two links")
    expect(problems, scene.terraform_calls() == [], "terraform ran")
    return problems


def hard_linked_manifest_blocks_the_init(scene: Scene) -> list[str]:
    problems: list[str] = []
    cache = jobs_cache(scene)
    protected = scene.victim / "protected.json"
    protected.write_text("{}\n")
    os.link(protected, cache / "modules/modules.json")
    code, output = scene.run()
    expect(problems, code == 1 and "infra/jobs" in output and "modules.json" in output and "hard link" in output, f"exit {code}: {output}")
    expect(problems, protected.read_text() == "{}\n", "terraform init wrote through the hard link")
    expect(problems, scene.terraform_calls() == [], "terraform ran")
    return problems


def tracked_lock_file_link_blocks_the_init(scene: Scene) -> list[str]:
    problems: list[str] = []
    lock = scene.worktree / "infra/jobs/.terraform.lock.hcl"
    elsewhere = scene.victim / "lock.hcl"
    elsewhere.write_text(LOCK)
    lock.unlink()
    lock.symlink_to(elsewhere)
    code, output = scene.run()
    expect(problems, code == 1 and ".terraform.lock.hcl" in output and "link" in output, f"exit {code}: {output}")
    expect(problems, scene.terraform_calls() == [] and elsewhere.read_text() == LOCK, "terraform ran or wrote through the lock file link")
    return problems


def native_provider_link_is_accepted_and_others_are_not(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.run()
    cache = scene.worktree / "infra/jobs/.terraform"
    platform = (scene.worktree / JOBS_PROVIDER).parent
    (cache / "modules/modules.json").unlink()
    code, output = scene.run()
    expect(
        problems,
        code == 0 and platform.is_symlink() and (cache / "modules/modules.json").is_file(),
        f"a terraform-made provider link blocked the retry: exit {code}: {output}",
    )
    expect(problems, len(scene.terraform_calls()) == 2, f"terraform calls: {scene.terraform_calls()}")
    for label, target in (("a folder of the worktree", scene.worktree / "infra/iam"), ("a folder outside it", scene.victim)):
        (cache / "modules/modules.json").unlink(missing_ok=True)
        platform.unlink()
        platform.symlink_to(target)
        code, output = scene.run()
        expect(problems, code == 1 and "link" in output and "infra/jobs" in output, f"a provider link to {label} was accepted: exit {code}: {output}")
        expect(problems, len(scene.terraform_calls()) == 2 and files_below(scene.victim) == [], f"terraform ran or wrote through a link to {label}")
    return problems


def incomplete_manifest_runs_init_again(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.run()
    manifest = scene.worktree / "infra/jobs/.terraform/modules/modules.json"
    wrong = '{"Modules":[{"Key":"","Source":"","Dir":"."},{"Key":"flow","Source":"../elsewhere","Dir":"../services/tests/flow"}]}'
    for label, text in (
        ("no module", '{"Modules":[]}'),
        ("only the root", '{"Modules":[{"Key":"","Source":"","Dir":"."}]}'),
        ("a module of another source", wrong),
    ):
        manifest.write_text(text)
        before = len(scene.terraform_calls())
        code, output = scene.run()
        expect(problems, code == 0 and len(scene.terraform_calls()) == before + 1, f"a manifest with {label} was accepted: exit {code}: {output}")
        expect(problems, '"flow"' in manifest.read_text(), f"the manifest was not made again after {label}")
    return problems


def changed_configuration_runs_init_again(scene: Scene) -> list[str]:
    problems: list[str] = []
    scene.run()
    with (scene.worktree / "infra/jobs/main.tf").open("a") as handle:
        handle.write('module "extra" {\n  source = "../services/tests/flow"\n}\n')
    code, output = scene.run()
    expect(problems, code == 0 and len(scene.terraform_calls()) == 2, f"a module added to the configuration was not installed: exit {code}: {output}")
    expect(problems, '"extra"' in (scene.worktree / "infra/jobs/.terraform/modules/modules.json").read_text(), "the manifest lacks the new module")
    code, output = scene.run()
    expect(
        problems, code == 0 and len(scene.terraform_calls()) == 2 and "verified infra/jobs/.terraform" in output, f"a complete cache was not accepted: {output}"
    )
    return problems


TRICKY = r"""# module "ghost_comment" {
/* module "ghost_block" {
   source = "../services/tests/flow"
} */
locals {
  braces = "} module \"ghost_string\" { ${lower("}")}"
  note   = <<-EOT
    } module "ghost_heredoc" {
      source = "../services/tests/flow"
  EOT
}

module "flow" {
  source = "../services/tests/flow"
}

module "late" {
  source   = "../services/tests/flow"
  for_each = { a = 1 }
  inputs   = { source = "nested" }
}

module "dynamic" {
  source = "${var.where}"
}
"""


def parser_reads_module_calls_only(scene: Scene) -> list[str]:
    problems: list[str] = []
    (scene.worktree / "infra/jobs/main.tf").write_text(TRICKY)
    scene.run()
    manifest = scene.worktree / "infra/jobs/.terraform/modules/modules.json"
    keys = sorted(item["Key"] for item in json.loads(manifest.read_text())["Modules"])
    expect(problems, keys == ["", "dynamic", "flow", "late"], f"the stand-in made keys {keys}")
    code, output = scene.run()
    expect(
        problems,
        code == 0 and len(scene.terraform_calls()) == 1 and "verified infra/jobs/.terraform" in output,
        f"text of a comment, a string or a heredoc was read as a module: {output}",
    )
    for missing in ("late", "dynamic", "flow"):
        data = json.loads(manifest.read_text())
        data["Modules"] = [item for item in data["Modules"] if item["Key"] != missing]
        manifest.write_text(json.dumps(data))
        before = len(scene.terraform_calls())
        code, output = scene.run()
        expect(problems, code == 0 and len(scene.terraform_calls()) == before + 1, f"the missing module {missing} was not noticed: exit {code}: {output}")
    return problems


def nested_local_module_is_required(scene: Scene) -> list[str]:
    problems: list[str] = []
    write(scene.worktree / "infra/services/tests/flow/main.tf", 'module "inner" {\n  source = "./inner"\n}\n')
    write(scene.worktree / "infra/services/tests/flow/inner/main.tf", "# inner\n")
    root = '{"Key":"","Source":"","Dir":"."}'
    flow = '{"Key":"flow","Source":"../services/tests/flow","Dir":"../services/tests/flow"}'
    inner = '{"Key":"flow.inner","Source":"./inner","Dir":"../services/tests/flow/inner"}'
    code, output = scene.run(manifest=f'{{"Modules":[{root},{flow}]}}')
    expect(problems, code == 1 and "flow.inner" in output, f"a missing nested module was not noticed: exit {code}: {output}")
    code, output = scene.run(manifest=f'{{"Modules":[{root},{flow},{inner}]}}')
    expect(problems, code == 0 and len(scene.terraform_calls()) == 2, f"the complete manifest was not accepted: exit {code}: {output}")
    return problems


def terraform_arguments_are_checked(scene: Scene) -> list[str]:
    problems: list[str] = []
    code, output = scene.run()
    expect(problems, code == 0 and scene.terraform_calls() == [scene.vector()], f"exit {code}: {output}: {scene.terraform_calls()}")
    wt = scene.worktree
    for label, wrong in (
        ("no -chdir", f"init -backend=false -input=false -plugin-dir={wt}/infra/.terraform/providers"),
        ("another -chdir", scene.vector().replace("/infra/jobs", "/infra")),
        ("no -backend=false", scene.vector().replace(" -backend=false", "")),
        ("another plugin folder", scene.vector().replace("/infra/.terraform/providers", "/infra/jobs")),
    ):
        shutil.rmtree(wt / "infra/jobs/.terraform", ignore_errors=True)
        code, output = scene.run(expected=wrong)
        expect(problems, code == 1 and "unexpected terraform arguments" in output, f"the stand-in accepted {label}: exit {code}: {output}")
    return problems


CASES: dict[str, tuple[Case, dict[str, Any]]] = {
    "fresh_worktree_gets_every_ignored_input": (fresh_setup, {}),
    "second_run_changes_nothing": (second_run_changes_nothing, {}),
    "a_differing_file_is_refused_and_kept": (differing_file_refused, {}),
    "a_missing_source_is_named_and_the_rest_is_done": (missing_source_named, {}),
    "an_empty_package_folder_is_filled": (empty_folder_filled, {}),
    "a_differing_env_file_is_kept": (differing_env_kept, {}),
    "a_link_to_another_folder_is_refused": (link_to_another_folder_refused, {}),
    "a_stale_pin_file_is_refused": (stale_pin_file_refused, {}),
    "a_path_outside_the_worktrees_folder_is_refused": (not_a_worktree_of_the_folder_refused, {}),
    "a_failed_terraform_init_is_named": (terraform_failure_named, {}),
    "a_destination_git_does_not_ignore_is_refused": (destination_not_ignored_refused, {"ignore": GITIGNORE.replace("*.tfvars.json\n", "")}),
    "a_nested_link_in_a_copy_tree_is_refused": (nested_link_in_a_copy_tree_refused, {}),
    "a_dangling_link_for_a_generated_file_is_refused": (dangling_generated_link_refused, {}),
    "a_dangling_link_for_env_is_refused": (dangling_env_link_refused, {}),
    "a_link_in_a_destination_parent_is_refused": (link_in_a_destination_parent_refused, {}),
    "a_folder_swapped_for_a_link_during_the_run_cannot_redirect_a_write": (swap_during_the_run_cannot_redirect, {}),
    "a_file_created_before_publication_is_not_replaced": (file_created_before_publish_not_replaced, {}),
    "a_tracked_alias_of_a_destination_is_refused": (tracked_alias_refused, {}),
    "a_tracked_descendant_or_ancestor_of_a_destination_is_refused": (tracked_descendants_refused, {}),
    "other_tracked_names_do_not_block": (tracked_other_names_do_not_block, {}),
    "a_failed_init_is_retried_and_a_complete_cache_is_accepted": (failed_init_is_retried, {}),
    "an_incomplete_provider_cache_is_initialised_again": (incomplete_provider_reinitialised, {}),
    "an_empty_env_without_a_marker_is_not_ready": (empty_env_without_marker_not_ready, {}),
    "a_truncated_env_without_a_marker_is_not_ready": (truncated_env_without_marker_not_ready, {}),
    "owner_changes_to_env_after_setup_are_kept": (owner_changes_after_setup_are_kept, {}),
    "an_interrupted_publication_leaves_nothing_partial": (interrupted_publish_leaves_nothing_partial, {}),
    "a_crash_before_the_marker_is_adopted_on_retry": (crash_before_marker_adopted, {}),
    "a_leftover_staging_file_is_removed": (leftover_staging_removed, {}),
    "a_source_link_outside_the_main_checkout_is_not_copied": (source_link_outside_main_not_copied, {}),
    "a_pipe_in_place_of_a_file_is_refused_and_does_not_hang_the_run": (pipe_in_place_of_a_file_refused, {}),
    "a_tracked_path_below_the_jobs_cache_blocks_the_init": (tracked_path_below_the_jobs_cache_blocks_init, {}),
    "a_link_leaving_the_jobs_cache_blocks_the_init": (link_leaving_the_jobs_cache_blocks_the_init, {}),
    "a_marker_file_of_a_wrong_shape_is_ignored": (marker_file_with_wrong_shapes_is_ignored, {}),
    "a_symlinked_manifest_in_the_jobs_cache_blocks_the_init": (symlinked_manifest_blocks_the_init, {}),
    "a_linked_modules_folder_in_the_jobs_cache_blocks_the_init": (linked_modules_folder_blocks_the_init, {}),
    "a_hard_linked_manifest_in_the_jobs_cache_blocks_the_init": (hard_linked_manifest_blocks_the_init, {}),
    "a_linked_lock_file_blocks_the_init": (tracked_lock_file_link_blocks_the_init, {}),
    "a_terraform_made_provider_link_is_accepted_and_other_links_are_not": (native_provider_link_is_accepted_and_others_are_not, {}),
    "an_empty_or_root_only_or_foreign_manifest_runs_the_init_again": (incomplete_manifest_runs_init_again, {}),
    "a_module_added_to_the_configuration_runs_the_init_again": (changed_configuration_runs_init_again, {}),
    "text_in_comments_strings_and_heredocs_is_not_a_module_call": (parser_reads_module_calls_only, {}),
    "a_nested_local_module_is_required_in_the_manifest": (nested_local_module_is_required, {}),
    "a_write_error_is_named_and_leaves_nothing_partial": (write_error_is_named_and_leaves_nothing_partial, {}),
    "the_terraform_stand_in_checks_the_whole_vector": (terraform_arguments_are_checked, {}),
}


def main(argv: list[str] | None = None) -> int:
    wanted = set(sys.argv[1:] if argv is None else argv)
    clear_git_environment()
    out = ROOT / "data" / "e2e" / "worktree_setup" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    out.mkdir(parents=True)
    results: list[dict[str, Any]] = []
    for name, (case, settings) in CASES.items():
        if wanted and name not in wanted:
            continue
        top = Path(tempfile.mkdtemp(prefix="worktree_setup_e2e.")).resolve()
        try:
            problems = case(Scene(top, **settings))
        except Exception as error:
            problems = [f"{type(error).__name__}: {error}"]
        finally:
            shutil.rmtree(top, ignore_errors=True)
        results.append({"case": name, "passed": not problems, "problems": problems})
    passed = sum(result["passed"] for result in results)
    report = out / "report.json"
    report.write_text(json.dumps({"pass": passed == len(results), "cases": results}, indent=2) + "\n")
    for result in results:
        sys.stdout.write(f"{'PASS' if result['passed'] else 'FAIL'} {result['case']}\n")
        for problem in result["problems"]:
            sys.stdout.write(f"  {problem}\n")
    sys.stdout.write(f"{passed}/{len(results)} passed; report {report}\n")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
