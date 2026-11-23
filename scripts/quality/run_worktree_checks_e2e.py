"""E2E test of the head check and the pinned-tools check (``check-head`` and ``check-tools``, the bundle's commit-from-a-checked-head procedure).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_worktree_checks_e2e``. Each case builds a scratch repository with this repository's hook
setup and scripts (``run_checks_e2e.scratch_repo``), gives it a bare ``origin`` that holds ``main``, and runs the check as its own command and through
``pre-commit`` itself, so a hook that is not wired in cannot pass. The head cases cover a good branch, ``main``, a detached head, a rebase, a merge and a
cherry-pick in progress, a branch behind ``origin/main`` and an unreachable ``origin``. The tools cases use stand-in executables for the pinned tools.
They cover a good main checkout and a good linked worktree, links that point elsewhere or are folders, a pinned tool without an executable, a missing and a
damaged package archive and a missing push gate. The lock cases compare the requirements copies that the installers saved in ``.tools/<tool>`` (DVC saves two,
its requirements and its build requirements) with the export of the tool's group of ``uv.lock`` now; no installed package is inspected. They cover copies
that match (the good cases), a copy that differs (the build copy of DVC alone too), a missing copy and one that cannot be read (each names the installer
command to rerun), a lock that cannot be read, a receipt that cannot be looked up (its folder cannot be entered)
and a lint tool whose manifest entry names no group. Each of these ends in a named problem, exit status 1 and no traceback.
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


SHORT_EXPORT = (
    "import runpy; from scripts.quality import lock_env; lock_env.EXPORT_DEADLINE = 1; "
    "runpy.run_module('scripts.quality.check_tools', run_name='__main__', alter_sys=True)"
)


def run_check(repo: Path, module: str) -> tuple[int, str]:
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    if module == "check_head":
        result = run_command(str(PYTHON), ["-m", "scripts.quality.check_head"], cwd=repo, env=environment, timeout=240)
    elif module == "check_tools_short_export":
        # The real check with a one-second export timeout, set in this child process only, so a stalled export times out without a 120 s wait.
        result = run_command(str(PYTHON), ["-c", SHORT_EXPORT], cwd=repo, env=environment, timeout=240)
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
    lock_stand_in(repo, tools)


def lock_stand_in(repo: Path, tools: dict[str, dict[str, str]]) -> None:
    """A ``uv`` that prints a fixed export for each group, and the requirements file that each installed environment copied from that export."""
    (repo / ".tools/lock_stub").mkdir()
    (repo / ".tools/bin/uv").write_text(
        '#!/bin/sh\n[ "$1" = "--version" ] && echo "uv ' + tools["uv"]["version"] + '" && exit 0\n'
        'while [ $# -gt 0 ]; do [ "$1" = "--only-group" ] && group="$2"; shift; done\ncat "$(dirname "$0")/../lock_stub/$group.txt"\n'
    )
    environments = {"sqlfluff": ("sqlfluff", "sqlfluff"), "dvc": ("dvc", "dvc")}
    environments.update({name: (name, entry["group"]) for name, entry in tools.items() if entry.get("installer") == "scripts/quality/install_lint_tools.py"})
    for folder, group in environments.values():
        text = f"{group}-package==1.0.0 \\\n    --hash=sha256:{'0' * 64}\n"
        build = f"setuptools==70.0.0 \\\n    --hash=sha256:{'2' * 64}\n" if group == "dvc" else ""
        (repo / ".tools/lock_stub" / f"{group}.txt").write_text(text + build)
        (repo / ".tools" / folder).mkdir(exist_ok=True)
        (repo / ".tools" / folder / "requirements.txt").write_text(text)
        if build:
            (repo / ".tools" / folder / "build_requirements.txt").write_text(build)


def tools_repo(scratch: Path) -> Path:
    """A main checkout with stand-in pinned tools, a synthetic package archive and its record, and the push gate installed."""
    repo = scratch_repo(scratch, Case("tools setup", "check-tools", True))
    synthetic_tools(repo)
    synthetic_archive(repo)
    run_command(str(PYTHON), ["-m", "scripts.quality.pre_push", "install-shim"], cwd=repo, timeout=120, check=True)
    return repo


def tools_cases() -> list[tuple[str, bool, str]]:
    rows: list[tuple[str, bool, str]] = []

    def case(
        name: str,
        expect: str | tuple[str, ...],
        setup: Callable[[Path, Path], Path | None] | None = None,
        hook: bool = False,
        module: str = "check_tools",
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="hai_tools_check_") as folder:
            scratch = Path(folder)
            repo = tools_repo(scratch)
            try:
                where = (setup(scratch, repo) if setup else None) or repo
                code, output = run_check(where, module)
            finally:
                (repo / ".tools/receipts").chmod(0o755)  # a case may close the folder; the scratch folder cannot be removed while it is closed
            expected = tuple(filter(None, (expect,) if isinstance(expect, str) else expect))
            ok = (code == 0) if not expected else (code == 1 and all(text in output for text in expected))
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

    def project_yamllint(reports: str | None, interpreter: bool = True) -> Callable[[Path, Path], None]:
        """A fresh install puts yamllint in .venv (the host lock) and nowhere under .tools. The stand-in .venv holds that executable and, unless
        ``interpreter`` is false, a stand-in python whose ``-m yamllint --version`` prints ``reports`` and exits 0."""

        def setup(_: Path, repo: Path) -> None:
            (repo / ".tools/bin/yamllint").unlink()
            (repo / ".tools/receipts/yamllint.json").unlink()
            (repo / ".venv").unlink()
            (repo / ".venv/bin").mkdir(parents=True)
            (repo / ".venv/bin/yamllint").write_text("#!/bin/sh\nexit 0\n")
            (repo / ".venv/bin/yamllint").chmod(0o755)
            if interpreter:
                (repo / ".venv/bin/python").write_text(f"#!/bin/sh\n[ \"$*\" = '-m yamllint --version' ] && echo '{reports}'\nexit 0\n")
                (repo / ".venv/bin/python").chmod(0o755)

        return setup

    def no_archive(_: Path, repo: Path) -> None:
        shutil.rmtree(repo / ARCHIVE_FOLDER)

    def damaged_archive(_: Path, repo: Path) -> None:
        archive = next((repo / ARCHIVE_FOLDER).rglob("*.tar"))
        archive.write_bytes(archive.read_bytes() + b"x")

    def no_gate(_: Path, repo: Path) -> None:
        git(repo, "config", "--local", "--unset-all", "hook.hai-push-input.command")

    def installed(folder: str) -> Callable[[Path, Path], None]:
        def change(_: Path, repo: Path) -> None:
            (repo / ".tools" / folder / "requirements.txt").write_text("another-package==2.0.0 \\\n    --hash=sha256:" + "1" * 64 + "\n")

        return change

    def not_recorded(_: Path, repo: Path) -> None:
        (repo / ".tools/import-linter/requirements.txt").unlink()

    def build_changed(_: Path, repo: Path) -> None:
        (repo / ".tools/dvc/build_requirements.txt").write_text("setuptools==71.0.0 \\\n    --hash=sha256:" + "3" * 64 + "\n")

    def build_not_recorded(_: Path, repo: Path) -> None:
        (repo / ".tools/dvc/build_requirements.txt").unlink()

    def record_forbidden(_: Path, repo: Path) -> None:
        (repo / ".tools/dvc/requirements.txt").chmod(0)

    def record_not_text(_: Path, repo: Path) -> None:
        (repo / ".tools/sqlfluff/requirements.txt").write_bytes(b"\xff\xfe\x00 not text")

    def lock_unreadable(_: Path, repo: Path) -> None:
        (repo / ".tools/lock_stub/dvc.txt").unlink()

    def export_stalls(_: Path, repo: Path) -> None:
        uv = repo / ".tools/bin/uv"
        uv.write_text(uv.read_text().replace('cat "$(dirname', '[ "$group" = dvc ] && sleep 30\ncat "$(dirname'))

    def receipts_closed(_: Path, repo: Path) -> None:
        (repo / ".tools/receipts").chmod(0)

    def group_unnamed(_: Path, repo: Path) -> None:
        manifest = repo / "config/quality_tools.json"
        document = json.loads(manifest.read_text())
        del document["tools"]["check-jsonschema"]["group"]
        manifest.write_text(json.dumps(document))

    case("main_checkout_with_everything_in_place", "", hook=True)
    case("linked_worktree_linked_to_the_main_checkout", "", worktree)
    case("link_to_another_folder_is_refused", ".venv must be a link to", elsewhere)
    case("folder_instead_of_a_link_is_refused", ".tools must be a link to", folder, hook=True)
    case("pinned_tool_without_an_executable_is_refused", "pinned tool phantom_tool", phantom)
    sqlfluff_command = "`.venv/bin/python -m scripts.quality.install_sqlfluff`"
    dvc_command = "`.venv/bin/python -m scripts.quality.install_dvc`"
    case(
        "installed_sqlfluff_that_differs_from_the_lock_is_refused",
        f"installed sqlfluff: the requirements.txt its installer saved differs from the sqlfluff group of uv.lock now; run {sqlfluff_command}",
        installed("sqlfluff"),
        hook=True,
    )
    case(
        "installed_dvc_that_differs_from_the_lock_is_refused",
        f"installed dvc: the requirements.txt its installer saved differs from the dvc group of uv.lock now; run {dvc_command}",
        installed("dvc"),
    )
    case(
        "installed_dvc_whose_build_requirements_differ_from_the_lock_is_refused",
        f"installed dvc: the build_requirements.txt its installer saved differs from the dvc group of uv.lock now; run {dvc_command}",
        build_changed,
        hook=True,
    )
    case(
        "installed_dvc_without_a_build_requirements_record_is_refused",
        f"installed dvc: its installer saved no build_requirements.txt; run {dvc_command}",
        build_not_recorded,
    )
    case(
        "installed_lint_tool_that_differs_from_the_lock_is_refused",
        "installed check-jsonschema: the requirements.txt its installer saved differs from the check_jsonschema group of uv.lock now; "
        "run `.venv/bin/python -m scripts.quality.install_lint_tools --tool check-jsonschema`",
        installed("check-jsonschema"),
    )
    case(
        "installed_lint_tool_without_a_record_is_refused",
        "installed import-linter: its installer saved no requirements.txt; run `.venv/bin/python -m scripts.quality.install_lint_tools --tool import-linter`",
        not_recorded,
    )
    case(
        "record_that_cannot_be_opened_is_refused",
        ("installed dvc: the requirements.txt its installer saved cannot be read (PermissionError", f"run {dvc_command}"),
        record_forbidden,
    )
    case(
        "record_that_is_not_text_is_refused",
        ("installed sqlfluff: the requirements.txt its installer saved cannot be read (UnicodeDecodeError", f"run {sqlfluff_command}"),
        record_not_text,
    )
    case(
        "lock_that_cannot_be_read_is_refused",
        ("the dvc group of uv.lock cannot be read (LockError", "run `.venv/bin/python -m scripts.quality.lock_env check`"),
        lock_unreadable,
    )
    case(
        "lock_export_that_does_not_finish_is_refused",
        ("uv, reading the dvc group of uv.lock for dvc, did not finish in 1 s", "run `.venv/bin/python -m scripts.quality.lock_env check`"),
        export_stalls,
        module="check_tools_short_export",
    )
    case(
        "receipt_that_cannot_be_looked_up_is_refused",
        ("the receipt of ", " cannot be read (PermissionError", "run scripts/quality/install_tools.py"),
        receipts_closed,
    )
    case(
        "lint_tool_without_a_group_in_the_manifest_is_refused",
        'config/quality_tools.json names no lock group for check-jsonschema, so installed check-jsonschema cannot be checked; add a "group" to its entry',
        group_unnamed,
    )
    pinned = json.loads((ROOT / "config/quality_tools.json").read_text(encoding="utf-8"))["tools"]["yamllint"]["version"]
    fix = "run `bash scripts/install.sh`"
    case("yamllint_from_the_project_environment_passes", "", project_yamllint(f"yamllint {pinned}"))
    case(
        "stale_yamllint_in_the_project_environment_is_refused",
        f"yamllint in .venv reports yamllint 1.0.0, not the pinned {pinned}; {fix}",
        project_yamllint("yamllint 1.0.0"),
    )
    case(
        "dummy_yamllint_in_the_project_environment_is_refused",
        f"yamllint in .venv reports dummy tool, not the pinned {pinned}; {fix}",
        project_yamllint("dummy tool"),
    )
    case(
        "yamllint_without_its_interpreter_is_refused",
        f"yamllint in .venv reports no version, not the pinned {pinned}; {fix}",
        project_yamllint(None, interpreter=False),
    )
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
