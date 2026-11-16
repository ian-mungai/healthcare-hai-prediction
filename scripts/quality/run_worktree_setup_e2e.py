"""E2E for the worktree setup command: every Git-ignored input of a new worktree is made or checked from the main checkout, and nothing is overwritten.

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_worktree_setup_e2e``. Every case uses a throwaway Git repository with real
worktrees in a temporary folder, never the project repository. A stand-in ``terraform`` on the PATH records its arguments and makes the folders a real
``terraform init -backend=false`` would make, and a stand-in file list replaces the dependency lock export (which needs ``uv``), so the cases run with no
network. Results go to ``data/e2e/worktree_setup/<UTC>/report.json``.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import stat
import sys
import tempfile
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
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
GITIGNORE = ".env\n*.tfvars.json\n.terraform/\n/data/\n/.worktrees/\n/services/orchestration/constraints-3.12.txt\n"
INFO_EXCLUDE = "/.venv\n/.tools\n"
TERRAFORM = """#!/bin/sh
printf '%s\\n' "$*" >> "$TERRAFORM_LOG"
[ -z "$TERRAFORM_FAIL" ] || { echo "Error: the provider cannot be installed" >&2; exit 1; }
mkdir -p infra/jobs/.terraform/modules infra/jobs/.terraform/providers
echo '{}' > infra/jobs/.terraform/modules/modules.json
"""

Case = Callable[["Scene"], list[str]]


def git(folder: Path, *arguments: str) -> str:
    return run_command("git", ["-C", str(folder), *arguments], timeout=120, check=True).stdout.strip()


def write(path: Path, text: str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(mode)


class Scene:
    """A throwaway repository with the ignored inputs of a main checkout and one fresh worktree under ``.worktrees``."""

    def __init__(self, top: Path, *, ignore: str = GITIGNORE) -> None:
        self.main = top / "main"
        self.main.mkdir()
        git(self.main, "init", "--quiet", "-b", "main")
        git(self.main, "config", "user.email", "case@example.invalid")
        git(self.main, "config", "user.name", "case")
        write(self.main / ".gitignore", ignore)
        write(self.main / "README.md", "tracked\n")
        git(self.main, "add", ".gitignore", "README.md")
        git(self.main, "commit", "--quiet", "-m", "chore: start")
        with (self.main / ".git/info/exclude").open("a") as handle:
            handle.write(INFO_EXCLUDE)
        for name in (".venv", ".tools"):
            write(self.main / name / "marker.txt", f"{name}\n")
        write(self.main / "data/analytics/dbt/dbt_packages/dbt_utils/dbt_project.yml", "name: dbt_utils\n")
        write(self.main / "data/analytics/dbt/package_archive/95b9/archive.tgz", "archive\n")
        for stack in STACKS:
            write(self.main / stack / ".terraform/providers/registry/aws/provider.bin", f"provider of {stack}\n")
            write(self.main / stack / ".terraform/terraform.tfstate", "backend marker that holds state\n")
        write(self.main / ".env", "AWS_REGION=us-fake-1\n")
        write(self.main / "infra/deployment.auto.tfvars.json", '{"environment": "fake"}\n', 0o600)
        self.worktree = self.main / ".worktrees" / "feat-case"
        git(self.main, "worktree", "add", "--quiet", "-b", "feat/case", str(self.worktree))
        self.bin = top / "bin"
        self.log = top / "terraform.log"
        self.bin.mkdir()
        write(self.bin / "terraform", TERRAFORM, 0o755)
        self.generated = {PIN_FILE: PINS}

    def run(self, *extra: str, path: Path | None = None, fail_terraform: bool = False) -> tuple[int, str]:
        """Run the command on the worktree (or ``path``); returns its exit code and everything it printed."""
        environment = {"PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}", "TERRAFORM_LOG": str(self.log), "TERRAFORM_FAIL": "1" if fail_terraform else ""}
        printed = io.StringIO()
        generated = mock.patch.object(worktree_setup, "generated_files", lambda root: self.generated)
        with mock.patch.dict(os.environ, environment), generated, redirect_stdout(printed), redirect_stderr(printed):
            code = worktree_setup.main([str(path or self.worktree), *extra])
        return code, printed.getvalue()

    def terraform_calls(self) -> list[str]:
        return self.log.read_text().splitlines() if self.log.exists() else []


def expect(problems: list[str], condition: bool, text: str) -> None:
    if not condition:
        problems.append(text)


def snapshot(folder: Path) -> dict[str, tuple[Any, float]]:
    """Every file and link below the folder with its text or target and its modification time."""
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
        expect(
            problems,
            (wt / tree).is_dir() and snapshot(scene.main / tree).keys() - {"terraform.tfstate"} <= snapshot(wt / tree).keys(),
            f"{tree} was not copied",
        )
    for stack in STACKS:
        expect(problems, not list((wt / stack / ".terraform").rglob("*.tfstate*")), f"{stack}/.terraform holds a state file")
    expect(problems, (wt / ".env").read_text() == (scene.main / ".env").read_text(), ".env was not copied")
    expect(problems, stat.S_IMODE((wt / ".env").stat().st_mode) == 0o600, ".env is not mode 0600")
    expect(problems, stat.S_IMODE((wt / "infra/deployment.auto.tfvars.json").stat().st_mode) == 0o600, "the tfvars file is not mode 0600")
    expect(problems, (wt / PIN_FILE).read_text() == PINS, "the pin file was not generated")
    calls = scene.terraform_calls()
    expect(problems, len(calls) == 1 and "-backend=false" in calls[0], f"terraform calls: {calls}")
    expect(problems, f"-plugin-dir={wt}/infra/.terraform/providers" in " ".join(calls), "the jobs init did not use the copied providers")
    expect(problems, (wt / "infra/jobs/.terraform/modules/modules.json").is_file(), "the jobs stack was not initialised")
    expect(problems, git(wt, "status", "--porcelain") == "", "the setup left a change Git can see")
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
    elsewhere = scene.main.parent / "elsewhere"
    elsewhere.mkdir()
    (scene.worktree / ".venv").symlink_to(elsewhere)
    code, output = scene.run()
    expect(problems, code == 1 and ".venv" in output, f"exit {code}: {output}")
    expect(problems, (scene.worktree / ".venv").resolve() == elsewhere.resolve(), "the link was replaced")
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
    expect(problems, not snapshot(elsewhere).keys() - {".git"} - {"README.md", ".gitignore"}, "a refused run wrote into the other worktree")
    return problems


def terraform_failure_named(scene: Scene) -> list[str]:
    problems: list[str] = []
    code, output = scene.run(fail_terraform=True)
    expect(problems, code == 1 and "infra/jobs" in output and "provider cannot be installed" in output, f"exit {code}: {output}")
    expect(problems, (scene.worktree / PIN_FILE).is_file(), "the other items were not done")
    return problems


def destination_not_ignored_refused(scene: Scene) -> list[str]:
    problems: list[str] = []
    code, output = scene.run()
    expect(problems, code == 1 and "infra/deployment.auto.tfvars.json" in output and "not ignored" in output, f"exit {code}: {output}")
    expect(problems, not (scene.worktree / "infra/deployment.auto.tfvars.json").exists(), "a file was copied to a path Git does not ignore")
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
}


def main() -> int:
    clear_git_environment()
    out = ROOT / "data" / "e2e" / "worktree_setup" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    out.mkdir(parents=True)
    results: list[dict[str, Any]] = []
    for name, (case, settings) in CASES.items():
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
