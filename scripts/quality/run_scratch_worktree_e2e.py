"""E2E for the scratch worktree check: a run fails only on Git's own answers about the worktrees it added, never on other runs' worktrees.

Run from the repository root: ``python -m scripts.quality.run_scratch_worktree_e2e``. Every case uses real Git worktrees of a throwaway repository in a
temporary folder, never the project repository, so the cases cannot disturb other agents' runs. The Python cases drive ``ScratchWorktrees``
(``scripts/quality/scratch_worktree.py``); the shell cases run the real ``cleanup`` of ``scripts/acquisition/run_checks_ci_e2e.sh``, taken from the
file. A leak is planted with Git itself: ``git worktree remove`` refuses a locked worktree and a read-only parent folder blocks the removal. A stand-in
``git`` fails one named subcommand (``SHIM_FAIL``) and passes every other command to the real Git.
Results go to ``data/e2e/scratch_worktree/<UTC>/report.json``.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import SubprocessError, clear_git_environment, find_program, run_command
from scripts.quality import scratch_worktree
from scripts.quality.scratch_worktree import ScratchWorktrees

ROOT = Path(__file__).resolve().parents[2]
SHELL_RUNNER = ROOT / "scripts" / "acquisition" / "run_checks_ci_e2e.sh"
CHANGED = "changed during the run"
# Takes the lines from the scratch variables through the end of the cleanup function from the shell runner, adds its worktree, applies one action and
# runs the cleanup, whose exit status is the harness's exit status.
SHELL_HARNESS = r"""
set -euo pipefail
script=$1 repo=$2 base=$3 out=$4 action=$5
cd "$repo"
results="$out/results.txt"
export TMPDIR="$base"
eval "$(awk '/^scratch=/{f=1} f{print} /^cleanup\(\) \{/{c=1} c&&/^}$/{exit}' "$script")"
GIT_LFS_SKIP_SMUDGE=1 git worktree add --quiet --detach "$work" HEAD
case "$action" in
  clean) ;;
  foreign_removed) git worktree remove --force "$base/foreign_before" ;;
  foreign_added) git worktree add --quiet --detach "$base/foreign_new" HEAD ;;
  own_locked) git worktree lock "$work" ;;
  drop_alias) rm "$base" ;;
  listing_fails) export PATH="$SHIM:$PATH" SHIM_FAIL=list ;;
  prune_fails) export PATH="$SHIM:$PATH" SHIM_FAIL=prune ;;
  freeze_own) chmod 555 "$scratch" ;;
  *) echo "unknown action $action" >&2; exit 2 ;;
esac
true
cleanup
"""


def git(repo: Path, *args: str) -> str:
    return run_command("git", ["-C", str(repo), *args], timeout=120, check=True).stdout


def new_repository(base: Path) -> Path:
    repo = base / "repo"
    repo.mkdir()
    git(repo, "init", "--quiet")
    git(repo, "-c", "user.name=e2e", "-c", "user.email=e2e@example.invalid", "commit", "--quiet", "--allow-empty", "-m", "start")
    return repo


def make_shim(base: Path) -> Path:
    """A folder holding a stand-in ``git`` that exits 91 for ``git ... worktree <SHIM_FAIL>`` and runs the real Git for everything else."""
    shim = base / "shim"
    shim.mkdir()
    (shim / "git").write_text(
        "#!/bin/bash\n"
        'previous=""\n'
        'for argument in "$@"; do\n'
        '  if [[ "$previous" == worktree && "$argument" == "${SHIM_FAIL:-}" ]]; then printf \'fatal: planted failure \\303\\251\\n\' >&2; exit 91; fi\n'
        "  previous=$argument\n"
        "done\n"
        f'exec {find_program("git")} "$@"\n'
    )
    (shim / "git").chmod(0o755)
    return shim


def python_case(
    name: str,
    base: Path,
    act: Callable[[Path, Path, ScratchWorktrees], None],
    expect: Callable[[dict[str, Any], Path], list[str]],
    *,
    alias: bool = False,
) -> dict[str, Any]:
    """Add one worktree through ``ScratchWorktrees``, apply ``act`` and check the record of ``close`` with ``expect`` (it returns the problems).

    With ``alias`` the worktree is added through ``base/alias``, a symbolic link to ``base/real``, which ``act`` may remove before ``close``.
    """
    repo = new_repository(base)
    parent = base
    if alias:
        (base / "real").mkdir()
        (base / "alias").symlink_to(base / "real")
        parent = base / "alias"
    own = parent / "scratch" / "worktree"
    own.parent.mkdir()
    trees = ScratchWorktrees(repo)
    original = scratch_worktree.listed_worktrees
    saved = {name: os.environ.get(name) for name in ("PATH", "SHIM_FAIL")}
    try:
        trees.add(own)
        act(repo, own, trees)
        record = trees.close()
    finally:
        scratch_worktree.listed_worktrees = original
        for variable, value in saved.items():
            if value is None:
                os.environ.pop(variable, None)
            else:
                os.environ[variable] = value
        for folder in (own.parent, base):
            if folder.exists():
                folder.chmod(0o755)
    problems = expect(record, own)
    return {"case": name, "passed": not problems, "problems": problems, "record": record}


def shell_case(name: str, base: Path, action: str, expect_status: int, expect_lines: list[str], out: Path, *, alias: bool = False) -> dict[str, Any]:
    """Run the shell runner's cleanup after ``action`` and compare its exit status and result lines.

    With ``alias`` the scratch folder is made through ``base/alias``, a symbolic link to ``base/real``; the action ``drop_alias`` removes the link.
    """
    repo = new_repository(base)
    temporary = base
    if alias:
        (base / "real").mkdir()
        (base / "alias").symlink_to(base / "real")
        temporary = base / "alias"
    if action == "foreign_removed":
        git(repo, "worktree", "add", "--quiet", "--detach", str(base / "foreign_before"), "HEAD")
    case_out = out / name
    case_out.mkdir(parents=True)
    shim = make_shim(base)
    done = run_command(
        "bash",
        ["-c", SHELL_HARNESS, "harness", str(SHELL_RUNNER), str(repo), str(temporary), str(case_out), action],
        env={**os.environ, "SHIM": str(shim)},
        timeout=300,
    )
    results = (case_out / "results.txt").read_text() if (case_out / "results.txt").exists() else ""
    problems = []
    if done.returncode != expect_status:
        problems.append(f"exit status {done.returncode}, expected {expect_status}; stderr: {done.stderr.strip()[-400:]}")
    for line in expect_lines:
        if not any(result.startswith(line) for result in results.splitlines()):
            problems.append(f"no result line starting with {line!r} in {results!r}")
    return {"case": name, "passed": not problems, "problems": problems, "results": results.splitlines()}


def no_problems_no_notes(record: dict[str, Any], own: Path) -> list[str]:
    return [f"expected no problems and no notes, got {record['problems']} and {record['notes']}"] if record["problems"] or record["notes"] else []


def note_only(fragment: str) -> Callable[[dict[str, Any], Path], list[str]]:
    def expect(record: dict[str, Any], own: Path) -> list[str]:
        if record["problems"]:
            return [f"another run's change failed this run: {record['problems']}"]
        if not any(fragment in note for note in record["notes"]):
            return [f"no note names {fragment}: {record['notes']}"]
        return []

    return expect


def problem_naming(text: str, *, names_path: bool = True) -> Callable[[dict[str, Any], Path], list[str]]:
    def expect(record: dict[str, Any], own: Path) -> list[str]:
        named = [problem for problem in record["problems"] if text in problem and (not names_path or own.name in problem)]
        return [] if named else [f"no problem with {text!r} naming {own}: {record['problems']}"]

    return expect


def remove_foreign(repo: Path, own: Path, trees: ScratchWorktrees) -> None:
    git(repo, "worktree", "remove", "--force", str(own.parent.parent / "foreign_before"))


def add_foreign(repo: Path, own: Path, trees: ScratchWorktrees) -> None:
    git(repo, "worktree", "add", "--quiet", "--detach", str(own.parent.parent / "foreign_new"), "HEAD")


def lock_own(repo: Path, own: Path, trees: ScratchWorktrees) -> None:
    git(repo, "worktree", "lock", str(own))


def lock_own_and_drop_alias(repo: Path, own: Path, trees: ScratchWorktrees) -> None:
    """The run's worktree leaks and the symbolic link it was added through disappears before ``close``."""
    lock_own(repo, own, trees)
    (own.parent.parent).unlink()


def drop_alias(repo: Path, own: Path, trees: ScratchWorktrees) -> None:
    """The symbolic link the worktree was added through disappears before ``close``; the run must still remove its worktree."""
    (own.parent.parent).unlink()


def freeze_own(repo: Path, own: Path, trees: ScratchWorktrees) -> None:
    own.parent.chmod(0o555)


def break_listing(repo: Path, own: Path, trees: ScratchWorktrees) -> None:
    def failing(root: Path) -> list[str]:
        raise OSError("planted listing failure")

    scratch_worktree.listed_worktrees = failing


def fail_prune(repo: Path, own: Path, trees: ScratchWorktrees) -> None:
    os.environ["PATH"] = f"{make_shim(own.parent.parent)}{os.pathsep}{os.environ['PATH']}"
    os.environ["SHIM_FAIL"] = "prune"


def foreign_before_case(base: Path) -> dict[str, Any]:
    """S1: another run's worktree, registered before this run, is removed during it."""
    repo = new_repository(base)
    git(repo, "worktree", "add", "--quiet", "--detach", str(base / "foreign_before"), "HEAD")
    own = base / "scratch" / "worktree"
    own.parent.mkdir()
    trees = ScratchWorktrees(repo)
    trees.add(own)
    remove_foreign(repo, own, trees)
    record = trees.close()
    problems = note_only(CHANGED)(record, own)
    return {"case": "foreign_worktree_removed_meanwhile_is_a_note", "passed": not problems, "problems": problems, "record": record}


# Runs ScratchWorktrees in a child process under the C locale with Python's UTF-8 mode off, so Git's listing of a worktree whose name is not
# ASCII cannot be decoded; the listing is report-only, so the run must still pass with a note.
LOCALE_CHILD = r"""
import json, sys
from pathlib import Path
from scripts.quality.scratch_worktree import ScratchWorktrees
trees = ScratchWorktrees(Path(sys.argv[1]))
trees.add(Path(sys.argv[2]))
sys.stdout.write(json.dumps(trees.close()))
"""


def undecodable_listing_case(base: Path) -> dict[str, Any]:
    """Another run's worktree has a non-ASCII name and the locale cannot decode it: the listings before and after fail, the run still passes."""
    repo = new_repository(base)
    git(repo, "worktree", "add", "--quiet", "--detach", str(base / "foreign_\u00e9"), "HEAD")
    own = base / "scratch" / "worktree"
    own.parent.mkdir()
    env = {**os.environ, "LC_ALL": "C", "LANG": "C", "PYTHONUTF8": "0", "PYTHONCOERCECLOCALE": "0", "PYTHONPATH": str(ROOT)}
    done = run_command(sys.executable, ["-c", LOCALE_CHILD, str(repo), str(own)], env=env, timeout=300)
    problems = []
    if done.returncode != 0:
        problems.append(f"the run failed (exit {done.returncode}): {done.stderr.strip()[-400:]}")
        return {"case": "undecodable_listing_is_a_note", "passed": False, "problems": problems}
    record = json.loads(done.stdout)
    if record["problems"]:
        problems.append(f"a report-only listing failed the run: {record['problems']}")
    for when in ("before", "after"):
        if not any(f"git worktree list failed {when}" in note for note in record["notes"]):
            problems.append(f"no note for the failed listing {when} the run: {record['notes']}")
    return {"case": "undecodable_listing_is_a_note", "passed": not problems, "problems": problems, "record": record}


def undecodable_removal_error_case(base: Path) -> dict[str, Any]:
    """git worktree remove fails with a non-ASCII message under the C locale (a stand-in git; real Git escapes non-ASCII paths in its messages):
    an unknown answer about the run's own worktree is a problem, never a crash."""
    repo = new_repository(base)
    own = base / "scratch" / "worktree"
    own.parent.mkdir()
    shim = make_shim(base)
    env = {**os.environ, "LC_ALL": "C", "LANG": "C", "PYTHONUTF8": "0", "PYTHONCOERCECLOCALE": "0", "PYTHONPATH": str(ROOT)}
    env |= {"PATH": f"{shim}{os.pathsep}{os.environ['PATH']}", "SHIM_FAIL": "remove"}
    done = run_command(sys.executable, ["-c", LOCALE_CHILD, str(repo), str(own)], env=env, timeout=300)
    if done.returncode != 0:
        return {
            "case": "undecodable_removal_error_fails",
            "passed": False,
            "problems": [f"the run crashed (exit {done.returncode}): {done.stderr.strip()[-400:]}"],
        }
    record = json.loads(done.stdout)
    named = [problem for problem in record["problems"] if "remove failed" in problem]
    problems = [] if named else [f"the undecodable refusal did not fail the run: {record['problems']}"]
    return {"case": "undecodable_removal_error_fails", "passed": not problems, "problems": problems, "record": record}


def undecodable_prune_error_case(base: Path) -> dict[str, Any]:
    """git worktree prune fails with a non-ASCII message under the C locale: an unknown answer is a problem, never a crash."""
    repo = new_repository(base)
    own = base / "scratch" / "worktree"
    own.parent.mkdir()
    shim = make_shim(base)
    env = {**os.environ, "LC_ALL": "C", "LANG": "C", "PYTHONUTF8": "0", "PYTHONCOERCECLOCALE": "0", "PYTHONPATH": str(ROOT)}
    env |= {"PATH": f"{shim}{os.pathsep}{os.environ['PATH']}", "SHIM_FAIL": "prune"}
    done = run_command(sys.executable, ["-c", LOCALE_CHILD, str(repo), str(own)], env=env, timeout=300)
    if done.returncode != 0:
        return {
            "case": "undecodable_prune_error_fails",
            "passed": False,
            "problems": [f"the run crashed (exit {done.returncode}): {done.stderr.strip()[-400:]}"],
        }
    record = json.loads(done.stdout)
    named = [problem for problem in record["problems"] if "prune failed" in problem]
    problems = [] if named else [f"the undecodable prune failure did not fail the run: {record['problems']}"]
    return {"case": "undecodable_prune_error_fails", "passed": not problems, "problems": problems, "record": record}


def main() -> int:
    clear_git_environment()
    out = ROOT / "data" / "e2e" / "scratch_worktree" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    out.mkdir(parents=True)
    top = Path(tempfile.mkdtemp(prefix="scratch_worktree_e2e."))
    link_target = top / "real"
    link_target.mkdir()
    (top / "link").symlink_to(link_target)
    results: list[dict[str, Any]] = []
    counter = iter(range(1000))

    def fresh(linked: bool = False, newline: bool = False, carriage: bool = False) -> Path:
        suffix = "\nline" if newline else "\rcarriage" if carriage else ""
        folder = (top / "link" if linked else top) / f"case{next(counter)}{suffix}"
        folder.mkdir()
        return folder

    try:
        results.append(python_case("clean_run_passes_without_notes", fresh(), lambda repo, own, trees: None, no_problems_no_notes))
        results.append(foreign_before_case(fresh()))
        results.append(python_case("foreign_worktree_added_meanwhile_is_a_note", fresh(), add_foreign, note_only(CHANGED)))
        results.append(python_case("own_locked_worktree_fails", fresh(), lock_own, problem_naming("remove failed")))
        results.append(python_case("own_folder_still_present_fails", fresh(), freeze_own, problem_naming("still exist")))
        results.append(python_case("own_locked_worktree_under_a_link_fails", fresh(linked=True), lock_own, problem_naming("remove failed")))
        results.append(python_case("clean_run_under_a_link_passes", fresh(linked=True), lambda repo, own, trees: None, no_problems_no_notes))
        results.append(python_case("listing_failure_is_a_note", fresh(), break_listing, note_only("git worktree list failed after")))
        results.append(undecodable_listing_case(fresh()))
        results.append(undecodable_removal_error_case(fresh()))
        results.append(undecodable_prune_error_case(fresh()))
        results.append(python_case("prune_failure_fails", fresh(), fail_prune, problem_naming("prune failed", names_path=False)))
        results.append(
            python_case("own_locked_worktree_whose_alias_disappears_fails", fresh(), lock_own_and_drop_alias, problem_naming("remove failed"), alias=True)
        )
        results.append(python_case("clean_run_whose_alias_disappears_passes", fresh(), drop_alias, no_problems_no_notes, alias=True))
        results.append(python_case("clean_run_with_a_newline_in_the_path_passes", fresh(newline=True), lambda repo, own, trees: None, no_problems_no_notes))
        results.append(python_case("own_locked_worktree_with_a_newline_in_the_path_fails", fresh(newline=True), lock_own, problem_naming("remove failed")))
        results.append(
            python_case("clean_run_with_a_carriage_return_in_the_path_passes", fresh(carriage=True), lambda repo, own, trees: None, no_problems_no_notes)
        )
        results.append(
            python_case("own_locked_worktree_with_a_carriage_return_in_the_path_fails", fresh(carriage=True), lock_own, problem_naming("remove failed"))
        )
        shell_out = out / "shell"
        results.append(shell_case("shell_clean_run_passes", fresh(), "clean", 0, ["PASS scratch_worktree_removed"], shell_out))
        results.append(
            shell_case(
                "shell_foreign_removed_is_a_note",
                fresh(),
                "foreign_removed",
                0,
                ["PASS scratch_worktree_removed", "NOTE the registered worktrees changed"],
                shell_out,
            )
        )
        results.append(
            shell_case(
                "shell_foreign_added_is_a_note",
                fresh(),
                "foreign_added",
                0,
                ["PASS scratch_worktree_removed", "NOTE the registered worktrees changed"],
                shell_out,
            )
        )
        results.append(shell_case("shell_own_worktree_locked_fails", fresh(), "own_locked", 1, ["FAIL scratch_worktree_removed"], shell_out))
        results.append(
            shell_case("shell_own_worktree_under_a_link_locked_fails", fresh(linked=True), "own_locked", 1, ["FAIL scratch_worktree_removed"], shell_out)
        )
        results.append(
            shell_case("shell_clean_run_whose_alias_disappears_passes", fresh(), "drop_alias", 0, ["PASS scratch_worktree_removed"], shell_out, alias=True)
        )
        results.append(
            shell_case("shell_clean_run_with_a_newline_in_the_path_passes", fresh(newline=True), "clean", 0, ["PASS scratch_worktree_removed"], shell_out)
        )
        results.append(
            shell_case(
                "shell_own_worktree_with_a_newline_in_the_path_fails", fresh(newline=True), "own_locked", 1, ["FAIL scratch_worktree_removed"], shell_out
            )
        )
        results.append(
            shell_case(
                "shell_clean_run_with_a_carriage_return_in_the_path_passes", fresh(carriage=True), "clean", 0, ["PASS scratch_worktree_removed"], shell_out
            )
        )
        results.append(
            shell_case(
                "shell_own_worktree_with_a_carriage_return_in_the_path_fails",
                fresh(carriage=True),
                "own_locked",
                1,
                ["FAIL scratch_worktree_removed"],
                shell_out,
            )
        )
        results.append(
            shell_case(
                "shell_listing_failure_is_a_note",
                fresh(),
                "listing_fails",
                0,
                ["PASS scratch_worktree_removed", "NOTE the registered worktrees changed"],
                shell_out,
            )
        )
        results.append(
            shell_case(
                "shell_own_folder_still_present_fails",
                fresh(),
                "freeze_own",
                1,
                ["FAIL scratch_worktree_removed: git worktree remove failed", "FAIL scratch_worktree_removed: the folder of this run's worktree"],
                shell_out,
            )
        )
        results.append(
            shell_case("shell_prune_failure_fails", fresh(), "prune_fails", 1, ["FAIL scratch_worktree_removed: git worktree prune failed"], shell_out)
        )
    except (SubprocessError, OSError) as error:
        results.append({"case": "runner_error", "passed": False, "problems": [str(error)]})
    finally:
        for folder in top.rglob("*"):
            if folder.is_dir() and not folder.is_symlink():
                folder.chmod(0o755)
        shutil.rmtree(top, ignore_errors=True)
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
    os.umask(0o022)
    raise SystemExit(main())
