"""Check that the worktree has its pinned inputs before a commit or a push (the bundle's commit-from-a-checked-head procedure).

Run from the worktree's root: ``.venv/bin/python -m scripts.quality.check_tools``. The ``check-tools`` pre-commit hook runs it before every commit and
every push.
It checks five things and names each problem with its fix:

1. In a linked worktree, ``.venv`` and ``.tools`` are links that point at the main checkout's own folders (the main checkout holds real folders).
2. Every pinned tool executable exists: each tool of ``config/quality_tools.json`` (its release binary in ``.tools/bin``, its pinned
   executable or, for a tool of installer ``requirements.txt``, ``.venv/bin`` or ``.tools/<tool>/bin/python``), the
   executables the hooks start from ``.tools``, the receipt of each release binary names the pinned version, and the yamllint that the lint command runs
   reports the pinned version (an executable that only runs is not enough).
3. The requirements copy that the installer of each tool that installs from ``uv.lock`` (SQLFluff, DVC, import-linter and check-jsonschema) saved in
   ``.tools/<tool>`` equals the export of the tool's group of the lock now (``.tools/<tool>/requirements.txt``; DVC also saves ``build_requirements.txt``). The
   check compares those saved files with a fresh ``uv`` export. It does not inspect the installed packages. A copy that differs, is missing or cannot be read,
   a lock that cannot be read and a uv command of the export that does not finish within its own timeout are each named with the command
   to run.
4. The dbt package archive that the model gates read is present and verifies against its record (``scripts.lakehouse.dbt_package_archive``).
5. The push gate is installed (``scripts.quality.pre_push check-shim``).

Exit 0 when everything is in place; 1 with each problem named. E2E: ``.venv/bin/python -m scripts.quality.run_checks_e2e`` (the worktree checks).
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
from pathlib import Path
from typing import Any

from scripts.lakehouse.dbt_package_archive import ArchiveError, verify
from scripts.process import TimeoutExpired, run_command
from scripts.quality import lock_env
from scripts.quality.lint_sources import yamllint_version
from scripts.quality.lock_env import LockError
from scripts.quality.pre_push import check_shim

GIT_SECONDS = 60
LOCK_CHECK = "`.venv/bin/python -m scripts.quality.lock_env check`"
LINKED = (".venv", ".tools")
# Pinned tools that other installers place under .tools (SQLFluff, markdownlint and DVC have their own pins).
OTHER_EXECUTABLES = (".tools/sqlfluff/bin/sqlfluff", ".tools/markdownlint-cli2/node_modules/.bin/markdownlint-cli2", ".tools/dvc/bin/dvc")


def git(root: Path, *arguments: str) -> tuple[int, str]:
    try:
        result = run_command("git", ["-C", str(root), *arguments], timeout=GIT_SECONDS)
    except TimeoutExpired:
        return 1, f"no answer within {GIT_SECONDS} s"
    return result.returncode, (result.stdout if result.returncode == 0 else result.stderr).strip()


def main_checkout(root: Path) -> Path | None:
    """The main checkout's root when ``root`` is a linked worktree; None for the main checkout itself or when Git cannot say."""
    code, directory = git(root, "rev-parse", "--path-format=absolute", "--git-dir")
    common_code, common = git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
    if code or common_code or Path(directory).resolve() == Path(common).resolve():
        return None
    return Path(common).resolve().parent


def points_at(path: Path, target: Path) -> bool:
    """Whether ``path`` is a link whose own target is ``target`` (the last link is not followed, so a link to another link to ``target`` fails)."""
    if not path.is_symlink():
        return False
    link = Path(os.path.abspath(path.parent / os.readlink(path)))
    return os.path.realpath(link.parent) == os.path.realpath(target.parent) and link.name == target.name


def link_problems(root: Path) -> list[str]:
    main = main_checkout(root)
    found = []
    for name in LINKED:
        path = root / name
        if not path.exists():
            found.append(f"{name} is missing; in a linked worktree link it to the main checkout with `ln -sfn <main checkout>/{name} {name}`")
        elif main is not None and not points_at(path, main / name):
            found.append(
                f"{name} must be a link to {main / name}; it is {'a link to ' + os.readlink(path) if path.is_symlink() else 'a folder in this worktree'}"
            )
    return found


def executable(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def manifest_executables(root: Path, tools: dict[str, Any]) -> dict[str, list[Path]]:
    """Per pinned tool, the places its executable can be: the release binary first, then the pinned executable, then, for a tool of installer
    ``requirements.txt``, the project environment (the host lock installs it there) or the older private environment in ``.tools``."""
    options: dict[str, list[Path]] = {}
    for name, entry in tools.items():
        places = [root / ".tools/bin" / name]
        if "executable" in entry:
            pinned = Path(entry["executable"])
            places.append(pinned if pinned.is_absolute() else root / pinned)
        if entry.get("installer") == "requirements.txt":
            places += [root / ".venv/bin" / name, root / ".tools" / name / "bin/python"]
        options[name] = places
    return options


def yamllint_problems(root: Path, pinned: str) -> list[str]:
    """The problem when the yamllint that the lint command runs (the private ``.tools/yamllint`` python, else the ``.venv`` python) is not the pinned one.

    An executable that runs proves nothing, since a stale or stand-in file passes the existence check, so its reported version must equal the pin."""
    private = root / ".tools/yamllint/bin/python"
    python = private if executable(private) else root / ".venv/bin/python"
    label = ".tools/yamllint" if python == private else ".venv"
    try:
        result = run_command(str(python), ["-m", "yamllint", "--version"], cwd=root, timeout=30)
        reported, _ = yamllint_version(result.returncode, result.stdout, root)
    except (OSError, TimeoutExpired, ValueError, KeyError):
        reported = None
    if reported == f"yamllint {pinned}":
        return []
    return [f"yamllint in {label} reports {reported or 'no version'}, not the pinned {pinned}; run `bash scripts/install.sh`"]


def tool_problems(root: Path) -> list[str]:
    manifest = root / "config/quality_tools.json"
    try:
        tools = json.loads(manifest.read_text(encoding="utf-8"))["tools"]
    except (OSError, ValueError, KeyError):
        return [f"{manifest.relative_to(root)} cannot be read, so the pinned tools are unknown"]
    found = []
    for name, places in manifest_executables(root, tools).items():
        if not any(executable(place) for place in places):
            where = ", ".join(str(place) for place in places)
            found.append(f"pinned tool {name} {tools[name]['version']} has no executable ({where}); run scripts/quality/install_tools.py")
            continue
        if name == "yamllint" and not executable(root / ".tools/bin" / name):
            found += yamllint_problems(root, tools[name]["version"])
        receipt = root / ".tools/receipts" / f"{name}.json"
        if executable(root / ".tools/bin" / name):
            try:
                if not receipt.is_file():
                    continue
                version = json.loads(receipt.read_text(encoding="utf-8")).get("version")
            except OSError as error:
                found.append(f"the receipt of {name} cannot be read ({type(error).__name__}: {error}); run scripts/quality/install_tools.py")
                continue
            except ValueError:
                version = None
            if version != tools[name]["version"]:
                found.append(f"the receipt of {name} names version {version}, not the pinned {tools[name]['version']}; run scripts/quality/install_tools.py")
    found += [f"pinned tool executable {path} is missing" for path in OTHER_EXECUTABLES if not executable(root / path)]
    return found


def lock_environments(tools: dict[str, Any]) -> dict[str, tuple[str | None, str]]:
    """Per tool that installs from ``uv.lock``, its group of the lock (None when the manifest names none) and its installer command."""
    found: dict[str, tuple[str | None, str]] = {
        "sqlfluff": ("sqlfluff", ".venv/bin/python -m scripts.quality.install_sqlfluff"),
        "dvc": ("dvc", ".venv/bin/python -m scripts.quality.install_dvc"),
    }
    found.update(
        {
            name: (entry.get("group"), f".venv/bin/python -m scripts.quality.install_lint_tools --tool {name}")
            for name, entry in tools.items()
            if entry.get("installer") == "scripts/quality/install_lint_tools.py"
        }
    )
    return found


def record_problems(root: Path, name: str, group: str, installer: str, expected: dict[str, str]) -> list[str]:
    """Each file that the installer of ``name`` saved that is missing, cannot be read or differs from the export of ``group`` now."""
    found = []
    for filename, text in expected.items():
        recorded = root / ".tools" / name / filename
        if not recorded.is_file():
            found.append(f"installed {name}: its installer saved no {filename}; run `{installer}`")
            continue
        try:
            saved = recorded.read_text(encoding="utf-8")
        except (OSError, ValueError) as error:
            found.append(f"installed {name}: the {filename} its installer saved cannot be read ({type(error).__name__}: {error}); run `{installer}`")
            continue
        if saved != text:
            found.append(f"installed {name}: the {filename} its installer saved differs from the {group} group of uv.lock now; run `{installer}`")
    return found


def lock_problems(root: Path) -> list[str]:
    """Each lock-installed tool whose saved requirements copies differ from the export of its group of ``uv.lock`` now, and each check that could not finish."""
    try:
        tools = json.loads((root / "config/quality_tools.json").read_text(encoding="utf-8"))["tools"]
    except (OSError, ValueError, KeyError):
        return []
    found = []
    for name, (group, installer) in lock_environments(tools).items():
        if not (root / ".tools" / name).is_dir():
            continue
        if group is None:
            found.append(f'config/quality_tools.json names no lock group for {name}, so installed {name} cannot be checked; add a "group" to its entry')
            continue
        try:
            expected = lock_env.export_files(group, root)
        except TimeoutExpired as error:
            program = Path(str(error.cmd[0] if isinstance(error.cmd, (list, tuple)) and error.cmd else error.cmd)).name
            found.append(
                f"{program}, reading the {group} group of uv.lock for {name}, did not finish in {error.timeout:g} s; run {LOCK_CHECK}, then repeat the check"
            )
            continue
        except (LockError, OSError, ValueError, KeyError) as error:
            found.append(
                f"the {group} group of uv.lock cannot be read ({type(error).__name__}: {str(error)[:200]}), "
                f"so installed {name} cannot be checked; run {LOCK_CHECK}"
            )
            continue
        found += record_problems(root, name, group, installer, expected)
    return found


def archive_problems(root: Path) -> list[str]:
    try:
        verify(root)
    except (ArchiveError, OSError, ValueError, KeyError) as error:
        return [f"the dbt package archive does not verify ({type(error).__name__}: {error}); copy data/analytics/dbt/package_archive from the main checkout"]
    return []


def gate_problems(root: Path) -> list[str]:
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        code = check_shim(root)
    return [] if code == 0 else [captured.getvalue().strip() or "the push gate is not installed"]


def problems(root: Path) -> list[str]:
    found = link_problems(root)
    if found:
        return found
    return [*tool_problems(root), *lock_problems(root), *archive_problems(root), *gate_problems(root)]


def main() -> int:
    found = problems(Path.cwd())
    for problem in found:
        sys.stderr.write(f"FAIL: {problem}\n")
    if found:
        return 1
    sys.stdout.write("verified   the worktree links, the pinned tools, the lock environments, the dbt package archive and the push gate are in place\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
