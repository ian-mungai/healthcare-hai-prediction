"""E2E test of the head check and the pinned-tools check (``check-head`` and ``check-tools``, the bundle's commit-from-a-checked-head procedure).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_worktree_checks_e2e``. Each case builds a scratch repository with this repository's hook
setup and scripts (``run_checks_e2e.scratch_repo``), gives it a bare ``origin`` that holds ``main``, and runs the check as its own command and through
``pre-commit`` itself, so a hook that is not wired in cannot pass. The head cases cover a good branch, ``main``, a detached head, a rebase, a merge and a
cherry-pick in progress, a branch behind ``origin/main`` and an unreachable ``origin``. The tools cases use stand-in executables for the pinned tools.
They cover a good main checkout and a good linked worktree, links that point elsewhere or are folders, a pinned tool without an executable, a missing
and a damaged package archive and a missing push gate.
Every blocked case must print its own message, so a failure for another reason cannot pass. The report is the run's output.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
from collections.abc import Callable
from pathlib import Path

from scripts.lakehouse.dbt_package_archive import ARCHIVES, RECORD, pack, sha
from scripts.process import clear_git_environment, run_command
from scripts.quality.check_tools import OTHER_EXECUTABLES
from scripts.quality.hook_stage import hook_stage
from scripts.quality.run_checks_e2e import Case, git, scratch_repo

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT / ".venv" / "bin" / "python"
PRE_COMMIT = ROOT / ".venv" / "bin" / "pre-commit"
ARCHIVE_FILES = (RECORD, "dbt/package-lock.yml")
ARCHIVE_FOLDER = str(ARCHIVES)


def head_repo(scratch: Path) -> Path:
    """A repository on the branch ``topic`` that holds ``main`` of a bare ``origin``."""
    repo = scratch_repo(scratch, Case("head setup", "check-head", True))
    remote = scratch / "origin.git"
    git(repo, "init", "-q", "--bare", "-b", "main", str(remote))
    git(repo, "remote", "add", "origin", str(remote))
    git(repo, "push", "-q", "origin", "main")
    git(repo, "fetch", "-q", "origin")
    git(repo, "checkout", "-q", "-b", "topic")
    return repo


def advance_origin(scratch: Path, repo: Path) -> None:
    """Add a commit to ``main`` of ``origin`` that the repository has not fetched."""
    other = scratch / "other"
    git(scratch, "clone", "-q", str(scratch / "origin.git"), str(other))
    git(other, "config", "user.name", "check test")
    git(other, "config", "user.email", "test@example.invalid")
    (other / "newer.txt").write_text("newer\n")
    git(other, "add", "newer.txt")
    git(other, "commit", "-q", "-m", "chore: newer main")
    git(other, "push", "-q", "origin", "main")


def run_check(repo: Path, module: str) -> tuple[int, str]:
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    if module == "check_head":
        result = run_command(str(PYTHON), ["-m", "scripts.quality.check_head"], cwd=repo, env=environment, timeout=240)
    else:
        result = run_command(str(PYTHON), ["-m", "scripts.quality.check_tools"], cwd=repo, env=environment, timeout=240)
    return result.returncode, result.stdout + result.stderr


def run_hook(repo: Path, hook: str) -> int:
    environment = {**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools" / "pre_commit_cache")}
    result = run_command(
        str(PRE_COMMIT), ["run", hook, "--hook-stage", hook_stage(repo / ".pre-commit-config.yaml", hook)], cwd=repo, env=environment, timeout=240
    )
    return result.returncode


def head_cases() -> list[tuple[str, bool, str]]:
    """(case, passed, output): each head case in a scratch repository of its own."""
    rows: list[tuple[str, bool, str]] = []

    def case(name: str, expect: str, setup: Callable[[Path, Path], None] | None = None, hook: bool = False) -> None:
        with tempfile.TemporaryDirectory(prefix="hai_head_check_") as folder:
            scratch = Path(folder)
            repo = head_repo(scratch)
            if setup:
                setup(scratch, repo)
            code, output = run_check(repo, "check_head")
            ok = (code == 0) if not expect else (code == 1 and expect in output and "Traceback" not in output)
            ok = ok and "Traceback" not in output
            if hook:
                ok = ok and (run_hook(repo, "check-head") == 0) == (not expect)
            rows.append((name, ok, output.strip()[-300:]))

    def on_main(_: Path, repo: Path) -> None:
        git(repo, "checkout", "-q", "main")

    def detached(_: Path, repo: Path) -> None:
        git(repo, "checkout", "-q", "--detach")

    def marker(name: str) -> Callable[[Path, Path], None]:
        def make(_: Path, repo: Path) -> None:
            path = repo / git(repo, "rev-parse", "--git-path", name).strip()
            path.mkdir() if name.startswith("rebase") else path.write_text("0" * 40 + "\n")

        return make

    def behind(scratch: Path, repo: Path) -> None:
        advance_origin(scratch, repo)

    def unreachable(scratch: Path, repo: Path) -> None:
        git(repo, "remote", "set-url", "origin", str(scratch / "missing.git"))

    case("head_on_a_branch_that_contains_origin_main", "", hook=True)
    case("head_on_main_is_refused", "the head is on main", on_main, hook=True)
    case("detached_head_is_refused", "the head is detached", detached)
    case("rebase_in_progress_is_refused", "a rebase in progress", marker("rebase-merge"))
    case("merge_in_progress_is_refused", "a merge in progress", marker("MERGE_HEAD"))
    case("cherry_pick_in_progress_is_refused", "a cherry-pick in progress", marker("CHERRY_PICK_HEAD"))
    case("branch_behind_origin_main_is_refused", "commits behind origin/main", behind)
    case("unreachable_origin_is_refused", "git fetch origin main failed", unreachable)
    return rows


def synthetic_archive(repo: Path) -> None:
    """A one-package dbt lock, its pinned record and the aggregate archive they name, built the way scripts.lakehouse.dbt_package_archive packs it."""
    lock = "packages:\n  - name: sample_package\n    package: sample/sample_package\n    version: 1.0.0\nsha1_hash: sample\n"
    tree = io.BytesIO()
    with tarfile.open(fileobj=tree, mode="w:gz") as package_archive:
        content = b"name: sample_package\nversion: '1.0.0'\n"
        member = tarfile.TarInfo("sample_package-1.0.0/dbt_project.yml")
        member.size = len(content)
        package_archive.addfile(member, io.BytesIO(content))
    package = tree.getvalue()
    filename = "sample_package-1.0.0.tar.gz"
    data = pack({filename: package})
    relative = ARCHIVES / sha(lock.encode()) / f"packages_{sha(data)}.tar"
    row = {"name": "sample_package", "package": "sample/sample_package", "version": "1.0.0", "filename": filename, "url": "https://example.invalid/sample"}
    record = {
        "schema_version": 1,
        "lock_sha256": sha(lock.encode()),
        "packages": [{**row, "size": len(package), "sha256": sha(package)}],
        "archive": {"path": str(relative), "size": len(data), "sha256": sha(data)},
    }
    for path, text in (("dbt/package-lock.yml", lock), (RECORD, json.dumps(record, indent=2) + "\n")):
        (repo / path).parent.mkdir(parents=True, exist_ok=True)
        (repo / path).write_text(text)
    (repo / relative).parent.mkdir(parents=True, exist_ok=True)
    (repo / relative).write_bytes(data)


def synthetic_tools(repo: Path) -> None:
    """A tools folder of stand-in executables for every pinned tool, with receipts that name the pinned versions, in place of the link to this checkout's
    folder. The cases then test the check and not the tools that the machine running them happens to have installed (a hosted hook job installs only some).
    """
    (repo / ".tools").unlink()
    tools = json.loads((repo / "config/quality_tools.json").read_text(encoding="utf-8"))["tools"]
    places = [Path(".tools/bin") / name for name in tools] + [Path(path) for path in OTHER_EXECUTABLES]
    for relative in places:
        (repo / relative).parent.mkdir(parents=True, exist_ok=True)
        (repo / relative).write_text("#!/bin/sh\nexit 0\n")
        (repo / relative).chmod(0o755)
    (repo / ".tools/receipts").mkdir(parents=True)
    for name, entry in tools.items():
        (repo / ".tools/receipts" / f"{name}.json").write_text(json.dumps({"version": entry["version"]}))


def tools_repo(scratch: Path) -> Path:
    """A main checkout with stand-in pinned tools, a synthetic package archive and its record, and the push gate installed."""
    repo = scratch_repo(scratch, Case("tools setup", "check-tools", True))
    synthetic_tools(repo)
    synthetic_archive(repo)
    run_command(str(PYTHON), ["-m", "scripts.quality.pre_push", "install-shim"], cwd=repo, timeout=120, check=True)
    return repo


def tools_cases() -> list[tuple[str, bool, str]]:
    rows: list[tuple[str, bool, str]] = []

    def case(name: str, expect: str, setup: Callable[[Path, Path], Path | None] | None = None, hook: bool = False) -> None:
        with tempfile.TemporaryDirectory(prefix="hai_tools_check_") as folder:
            scratch = Path(folder)
            repo = tools_repo(scratch)
            where = (setup(scratch, repo) if setup else None) or repo
            code, output = run_check(where, "check_tools")
            ok = (code == 0) if not expect else (code == 1 and expect in output)
            ok = ok and "Traceback" not in output
            if hook:
                ok = ok and (run_hook(where, "check-tools") == 0) == (not expect)
            rows.append((name, ok, output.strip()[-300:]))

    def worktree(scratch: Path, repo: Path) -> Path:
        linked = scratch / "linked"
        git(repo, "worktree", "add", "-q", "-b", "side", str(linked))
        for name in (".venv", ".tools"):
            (linked / name).symlink_to(repo / name)
        for relative in ARCHIVE_FILES:
            (linked / relative).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(repo / relative, linked / relative)
        shutil.copytree(repo / ARCHIVE_FOLDER, linked / ARCHIVE_FOLDER)
        return linked

    def elsewhere(scratch: Path, repo: Path) -> Path:
        linked = worktree(scratch, repo)
        (linked / ".venv").unlink()
        (linked / ".venv").symlink_to(ROOT / ".venv")
        return linked

    def folder(scratch: Path, repo: Path) -> Path:
        linked = worktree(scratch, repo)
        (linked / ".tools").unlink()
        (linked / ".tools").mkdir()
        return linked

    def phantom(_: Path, repo: Path) -> None:
        manifest = repo / "config/quality_tools.json"
        document = json.loads(manifest.read_text())
        document["tools"]["phantom_tool"] = {"version": "1.0.0"}
        manifest.write_text(json.dumps(document))

    def no_archive(_: Path, repo: Path) -> None:
        shutil.rmtree(repo / ARCHIVE_FOLDER)

    def damaged_archive(_: Path, repo: Path) -> None:
        archive = next((repo / ARCHIVE_FOLDER).rglob("*.tar"))
        archive.write_bytes(archive.read_bytes() + b"x")

    def no_gate(_: Path, repo: Path) -> None:
        git(repo, "config", "--local", "--unset-all", "hook.hai-push-input.command")

    case("main_checkout_with_everything_in_place", "", hook=True)
    case("linked_worktree_linked_to_the_main_checkout", "", worktree)
    case("link_to_another_folder_is_refused", ".venv must be a link to", elsewhere)
    case("folder_instead_of_a_link_is_refused", ".tools must be a link to", folder, hook=True)
    case("pinned_tool_without_an_executable_is_refused", "pinned tool phantom_tool", phantom)
    case("missing_package_archive_is_refused", "the dbt package archive does not verify", no_archive)
    case("damaged_package_archive_is_refused", "the dbt package archive does not verify", damaged_archive)
    case("missing_push_gate_is_refused", "The Git config hook hai-push-input", no_gate)
    return rows


def main() -> int:
    clear_git_environment()
    rows = [*head_cases(), *tools_cases()]
    for name, ok, output in rows:
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
        if not ok:
            sys.stdout.write("".join(f"     {line}\n" for line in output.splitlines()[-6:]))
    failed = [name for name, ok, _ in rows if not ok]
    sys.stdout.write(f"{len(rows) - len(failed)}/{len(rows)} passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
