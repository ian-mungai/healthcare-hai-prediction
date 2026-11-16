"""Set up the Git-ignored inputs of a new worktree from the main checkout, so no check or run starts from a hand-made worktree.

Run from any checkout, with the worktree under ``<main checkout>/.worktrees/``:

    .venv/bin/python -m scripts.quality.worktree_setup .worktrees/<name>

It makes or verifies, in this order, each item that Git ignores:

1. ``.venv`` and ``.tools`` as links to the folders of the main checkout.
2. The dbt ``dbt_packages`` and ``package_archive`` folders and the ``.terraform`` folder of the stacks ``infra``, ``infra/iam``, ``infra/services`` and
   ``infra/platform``, copied from the main checkout (no state file is ever copied).
3. ``.env`` and ``infra/deployment.auto.tfvars.json``, copied only when absent, in mode 0600. They hold the account, bucket and profile names and no credential.
4. The ``.terraform`` folder of ``infra/jobs``, made by ``terraform init -backend=false`` from the copied providers (the main checkout holds none to copy).
5. The generated requirement and pin files of ``lock_env images``, which include ``services/orchestration/constraints-3.12.txt``, the file that the
   ``airflow-check`` service bind-mounts.

A second run changes nothing. A file that exists and differs from its source is never overwritten: the run names it and exits 1. ``.env`` and the tfvars
file are the exception, because a worktree may keep its own values: a differing one is kept and reported. Every destination must be ignored by Git,
so no file of the main checkout can land in a tracked path. A source the main checkout lacks is named, and the other items are still done. The exit code is
0 when every item is made or verified, 1 when an item is missing or refused and 2 when the path is not a worktree under ``.worktrees/`` of its repository.
"""

from __future__ import annotations

import argparse
import filecmp
import os
import shutil
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from scripts.process import SubprocessError, run_command
from scripts.quality import lock_env

LINKS = (".venv", ".tools")
STACKS = ("infra", "infra/iam", "infra/services", "infra/platform")
TREES = ("data/analytics/dbt/dbt_packages", "data/analytics/dbt/package_archive", *(f"{stack}/.terraform" for stack in STACKS))
COPIED_FILES = (".env", "infra/deployment.auto.tfvars.json")
JOBS = "infra/jobs"
PROVIDERS = "infra/.terraform/providers"
SKIPPED_NAMES = (".DS_Store",)
STATE_MARKER = ".tfstate"
GIT_DEADLINE = 60
TERRAFORM_DEADLINE = 600


class SetupRefused(ValueError):
    """The path is not a worktree under ``.worktrees/`` of its repository."""


@dataclass
class Report:
    """What one run did, in order, and what stopped it."""

    lines: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    def note(self, text: str) -> None:
        self.lines.append(text)

    def problem(self, item: str, text: str) -> None:
        self.problems.append(f"{item}: {text}")


def generated_files(root: Path) -> dict[str, str]:
    """Repository path to expected text of every file ``lock_env images`` writes; a seam for the cases, which have no ``uv``."""
    return lock_env.image_files(root)


def git(folder: Path, *arguments: str) -> tuple[int, str]:
    result = run_command("git", ["-C", str(folder), *arguments], timeout=GIT_DEADLINE)
    return result.returncode, result.stdout.strip()


def locate(worktree: Path) -> tuple[Path, Path]:
    """The resolved worktree and the main checkout, after checking that the worktree is a folder directly under ``<main>/.worktrees/``."""
    path = worktree.resolve()
    if not path.is_dir():
        raise SetupRefused(f"{worktree} is not a folder")
    code, top = git(path, "rev-parse", "--show-toplevel")
    if code or Path(top).resolve() != path:
        raise SetupRefused(f"{worktree} is not the top folder of a Git worktree")
    code, common = git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    if code:
        raise SetupRefused(f"{worktree}: Git cannot name its repository")
    main = Path(common).resolve().parent
    if path.parent != main / ".worktrees":
        raise SetupRefused(f"{worktree} is not a worktree directly under {main / '.worktrees'} (a worktree is a folder of .worktrees/ in the main checkout)")
    return path, main


def ignored(worktree: Path, relative: str, folder: bool) -> bool:
    """True when Git ignores the path; a folder is tested through a name inside it, because Git matches ``name/`` patterns only against what it can see."""
    probe = f"{relative}/.setup_probe" if folder else relative
    code, _ = git(worktree, "check-ignore", "-q", "--", probe)
    return code == 0


def refuse_unignored(report: Report, worktree: Path, relative: str, folder: bool) -> bool:
    if ignored(worktree, relative, folder):
        return False
    report.problem(relative, "is not ignored by Git, so nothing is written there")
    return True


def link_item(report: Report, worktree: Path, main: Path, name: str) -> None:
    source, target = main / name, worktree / name
    if not source.exists():
        report.problem(name, f"the main checkout has no {name} to link to")
    elif target.is_symlink():
        if target.resolve() == source.resolve():
            report.note(f"verified {name} (link to the main checkout)")
        else:
            report.problem(name, f"is a link to {os.readlink(target)}, not to {source}; it was left as it is")
    elif target.exists():
        report.problem(name, "exists and is not a link to the main checkout; it was left as it is")
    elif not refuse_unignored(report, worktree, name, folder=False):
        target.symlink_to(source)
        report.note(f"created {name} -> {source}")


def skipped(name: str) -> bool:
    return name in SKIPPED_NAMES or STATE_MARKER in name


def tree_files(folder: Path) -> list[Path]:
    """Every file and link below the folder that a copy carries; a state file is never one."""
    return [path for path in sorted(folder.rglob("*")) if (path.is_symlink() or path.is_file()) and not skipped(path.name)]


def same(source: Path, target: Path) -> bool:
    if source.is_symlink() or target.is_symlink():
        return source.is_symlink() and target.is_symlink() and os.readlink(source) == os.readlink(target)
    return target.is_file() and filecmp.cmp(source, target, shallow=False)


def copy_tree(report: Report, worktree: Path, main: Path, relative: str) -> None:
    """Copy every file of the main checkout's folder that the worktree's folder lacks; a file that exists and differs is named and left."""
    source, target = main / relative, worktree / relative
    files = tree_files(source) if source.is_dir() else []
    if not files:
        report.problem(relative, "the main checkout has no files there to copy")
        return
    if target.is_symlink():
        report.problem(relative, "is a link; it was left as it is")
        return
    if refuse_unignored(report, worktree, relative, folder=True):
        return
    copied = verified = 0
    differing = []
    for path in files:
        name = path.relative_to(source)
        destination = target / name
        if destination.exists() or destination.is_symlink():
            if same(path, destination):
                verified += 1
            else:
                differing.append(name.as_posix())
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            destination.symlink_to(os.readlink(path))
        else:
            shutil.copy2(path, destination)
        copied += 1
    for item in differing:
        report.problem(f"{relative}/{item}", "differs from the main checkout's file; it was not overwritten")
    report.note(f"copied {relative} ({copied} files added, {verified} already there)" if copied else f"verified {relative} ({verified} files)")


def copy_file(report: Report, worktree: Path, main: Path, relative: str) -> None:
    """Copy a file in mode 0600 when the worktree has none; a file that is there is kept, and a different one is reported."""
    source, target = main / relative, worktree / relative
    if not source.is_file():
        report.problem(relative, "the main checkout has no such file to copy")
    elif target.exists():
        report.note(
            f"verified {relative}" if same(source, target) else f"kept {relative} (it differs from the main checkout's file, which is not copied over it)"
        )
    elif not refuse_unignored(report, worktree, relative, folder=False):
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(source.read_bytes())
        report.note(f"created {relative} (mode 0600)")


def initialise_jobs(report: Report, worktree: Path) -> None:
    """Make the jobs stack's ``.terraform`` from the copied providers, with no backend and no network."""
    target = worktree / JOBS / ".terraform"
    if (target / "modules/modules.json").is_file():
        report.note(f"verified {JOBS}/.terraform")
        return
    if not (worktree / PROVIDERS).is_dir():
        report.problem(JOBS, f"cannot be initialised: {PROVIDERS} is missing")
        return
    if refuse_unignored(report, worktree, f"{JOBS}/.terraform", folder=True):
        return
    arguments = [f"-chdir={worktree / JOBS}", "init", "-backend=false", "-input=false", f"-plugin-dir={worktree / PROVIDERS}"]
    try:
        result = run_command("terraform", arguments, cwd=worktree, timeout=TERRAFORM_DEADLINE)
    except (OSError, SubprocessError) as error:
        report.problem(JOBS, f"terraform init could not run: {type(error).__name__}")
        return
    if result.returncode:
        report.problem(JOBS, f"terraform init -backend=false failed ({result.returncode}): {(result.stderr or result.stdout).strip()[-600:]}")
    else:
        report.note(f"created {JOBS}/.terraform (terraform init -backend=false)")


def generate(report: Report, worktree: Path) -> None:
    """Write each generated file the worktree lacks from the dependency lock; a file that exists and differs from the lock is named and left."""
    try:
        expected = generated_files(worktree)
    except (lock_env.LockError, OSError) as error:
        report.problem("generated files", f"cannot be made from the lock: {error}")
        return
    written = 0
    for relative, text in expected.items():
        target = worktree / relative
        if target.exists():
            if target.read_text(encoding="utf-8") != text:
                report.problem(
                    relative, "differs from the lock; it was not overwritten (run `.venv/bin/python -m scripts.quality.lock_env images` to replace it)"
                )
        elif not refuse_unignored(report, worktree, relative, folder=False):
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
            written += 1
    pins = lock_env.PIN_FILES["orchestration"]
    report.note(
        f"created {written} of the {len(expected)} generated files of lock_env images, including {pins}"
        if written
        else f"verified the generated files of lock_env images, including {pins}"
    )


def setup(worktree: Path, main: Path) -> Report:
    """Make or verify every item for the worktree, collecting every problem instead of stopping at the first."""
    report = Report()
    for name in LINKS:
        link_item(report, worktree, main, name)
    for relative in TREES:
        copy_tree(report, worktree, main, relative)
    for relative in COPIED_FILES:
        copy_file(report, worktree, main, relative)
    initialise_jobs(report, worktree)
    generate(report, worktree)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("worktree", type=Path, help="a worktree folder directly under .worktrees/ of the main checkout")
    arguments = parser.parse_args(argv)
    try:
        worktree, main_checkout = locate(arguments.worktree)
    except (SetupRefused, OSError, SubprocessError) as error:
        sys.stderr.write(f"worktree_setup: {error}\n")
        return 2
    report = setup(worktree, main_checkout)
    for line in report.lines:
        sys.stdout.write(f"worktree_setup: {line}\n")
    for problem in report.problems:
        sys.stderr.write(f"worktree_setup: {problem}\n")
    sys.stdout.write(f"worktree_setup: {worktree}: {'ready' if not report.problems else f'{len(report.problems)} problem(s) above'}\n")
    return 1 if report.problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
