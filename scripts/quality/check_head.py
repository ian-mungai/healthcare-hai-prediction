"""Check that the worktree's head is a current named branch before a commit or a push (the bundle's commit-from-a-checked-head procedure).

Run from the worktree's root: ``.venv/bin/python -m scripts.quality.check_head``. The ``check-head`` pre-commit hook runs it before every commit and every push.
The head must be a named branch other than main or master, with no rebase, merge or cherry-pick in progress, and it must contain the current
``origin/main``: the check fetches ``main`` from ``origin`` first, so a branch on an old base is caught before its commit runs old hooks. Rebase a branch that
is behind ``origin/main`` first, then rebind the reviewed lists and refresh the documentation review. Exit 0 when the head passes; 1 with each problem named.
E2E: ``.venv/bin/python -m scripts.quality.run_checks_e2e`` (the worktree checks).
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.process import TimeoutExpired, run_command

REMOTE, BASE = "origin", "main"
PROTECTED = ("main", "master")
GIT_SECONDS = 60
FETCH_SECONDS = 120
IN_PROGRESS = {"rebase-merge": "rebase", "rebase-apply": "rebase", "MERGE_HEAD": "merge", "CHERRY_PICK_HEAD": "cherry-pick"}


def git(root: Path, *arguments: str, timeout: float = GIT_SECONDS) -> tuple[int, str]:
    """Run Git in ``root``; the text is the output on success and the error text otherwise."""
    result = run_command("git", ["-C", str(root), *arguments], timeout=timeout)
    return result.returncode, (result.stdout if result.returncode == 0 else result.stderr).strip()


def problems(root: Path) -> tuple[str, list[str]]:
    """The branch name (empty when detached) and every problem found."""
    code, text = git(root, "rev-parse", "--git-dir")
    if code:
        return "", [f"not in a Git repository: {text}"]
    code, branch = git(root, "symbolic-ref", "-q", "--short", "HEAD")
    found: list[str] = []
    if code:
        branch = ""
        found.append("the head is detached; check out a named branch")
    elif branch in PROTECTED:
        found.append(f"the head is on {branch}; commit on a named branch, not {branch}")
    for marker, operation in IN_PROGRESS.items():
        code, path = git(root, "rev-parse", "--git-path", marker)
        if code or not path:
            found.append(f"git rev-parse --git-path {marker} failed, so a {operation} in progress cannot be ruled out: {path}")
        elif (root / path).exists():
            found.append(f"a {operation} in progress; finish or abort it first")
    try:
        code, text = git(root, "fetch", "--quiet", REMOTE, BASE, timeout=FETCH_SECONDS)
    except TimeoutExpired:
        code, text = 1, f"no answer within {FETCH_SECONDS} s"
    if code:
        found.append(f"git fetch {REMOTE} {BASE} failed, so {REMOTE}/{BASE} may be stale: {text.splitlines()[-1] if text else 'no message'}")
        return branch, sorted(set(found))
    code, count = git(root, "rev-list", "--count", f"HEAD..{REMOTE}/{BASE}")
    if code:
        found.append(f"cannot compare the head with {REMOTE}/{BASE}: {count}")
    elif int(count):
        found.append(f"the branch is {count} commits behind {REMOTE}/{BASE}; rebase it onto {REMOTE}/{BASE} first")
    return branch, sorted(set(found))


def main() -> int:
    branch, found = problems(Path.cwd())
    for problem in found:
        sys.stderr.write(f"FAIL: {problem}\n")
    if found:
        return 1
    sys.stdout.write(f"verified   {branch} is a named branch that contains the current {REMOTE}/{BASE}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
