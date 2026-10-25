"""Scratch Git worktrees for end-to-end runners, removed on every exit path.

A runner that needs a copy of HEAD adds it through ``ScratchWorktrees`` instead of calling ``git worktree add`` itself::

    trees = ScratchWorktrees(ROOT)
    try:
        work = trees.add(scratch / "worktree")
        ...
    finally:
        record = trees.close()

``close`` runs for a pass, a failed case, an exception, an interrupt and a deadline alike. It removes every added worktree with
``git worktree remove --force`` and runs ``git worktree prune``. SIGTERM, SIGHUP and SIGALRM (the deadline wrapper's signal) end the run
with ``SystemExit`` from the moment ``ScratchWorktrees`` is made, so the ``finally`` block runs. A removal that fails falls back to deleting the folder.
While ``close`` removes the worktrees and checks the registered ones, those three signals and SIGINT (Ctrl-C) are blocked, also for the Git processes it
starts. One that arrives then is delivered when ``close`` has finished, through the handler the process had before ``ScratchWorktrees`` was made.
``close`` returns the record for the report and prints every problem to standard error. Only Git's own answers about this run's worktrees count:
``git worktree remove`` failing for one of them, ``git worktree prune`` failing or a folder of one of them still existing. The run never parses
Git's registry to decide, because several agents add and remove their own worktrees in the same repository at once. The registered worktrees before
and after are kept for the report only; a difference or a failed listing is a note. The runner fails its run on a record with problems.
SIGKILL cannot be handled.
"""

from __future__ import annotations

import os
import shutil
import signal
import sys
from pathlib import Path
from types import FrameType
from typing import Any

from scripts.process import SubprocessError, run_command

GIT_TIMEOUT_SECONDS = 600
STOP_SIGNALS = (signal.SIGTERM, signal.SIGHUP, signal.SIGALRM)
DEFERRED_SIGNALS = {*STOP_SIGNALS, signal.SIGINT}


def listed_worktrees(root: Path) -> list[str]:
    """The paths in ``git worktree list --porcelain``, sorted, for the report only: nothing decides on them, so a path Git prints oddly is harmless."""
    listing = run_command("git", ["-C", str(root), "worktree", "list", "--porcelain"], timeout=60, check=True).stdout
    return sorted(line.removeprefix("worktree ") for line in listing.splitlines() if line.startswith("worktree "))


def stop_run(signum: int, frame: FrameType | None) -> None:
    raise SystemExit(128 + signum)


class ScratchWorktrees:
    """The scratch worktrees one run added to the repository at ``root``, and what removing them did."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.paths: list[Path] = []
        self.notes: list[str] = []
        self.problems: list[str] = []
        self.before = self.listing("before")
        self.after: list[str] = []
        self.previous = {number: signal.signal(number, stop_run) for number in STOP_SIGNALS}

    def add(self, path: Path) -> Path:
        """Add a detached worktree of HEAD at ``path``, a new or empty folder, and return its resolved path.

        The path is resolved and recorded first, so a half-made worktree is removed too and a symbolic link in ``path`` that disappears before
        ``close`` cannot hide the worktree from the removal or the check.
        """
        if path.exists() and any(path.iterdir()):
            raise FileExistsError(f"{path} is not empty; the fallback removal would delete it")
        resolved = path.resolve()
        self.paths.append(resolved)
        run_command(
            "git",
            ["-C", str(self.root), "worktree", "add", "--quiet", "--detach", str(resolved), "HEAD"],
            env={**os.environ, "GIT_LFS_SKIP_SMUDGE": "1"},
            timeout=GIT_TIMEOUT_SECONDS,
            check=True,
        )
        return resolved

    def listing(self, when: str) -> list[str]:
        """The registered worktrees for the report; a failed listing is a note, because nothing decides on it.

        ``ValueError`` covers ``UnicodeDecodeError``: under a locale that cannot decode another run's worktree path, the listing fails to decode.
        In the removal and the prune the same error means Git's answer is unknown, so there it is a problem.
        """
        try:
            return listed_worktrees(self.root)
        except (SubprocessError, OSError, ValueError) as error:
            self.notes.append(f"git worktree list failed {when} the run (report only): {error}")
            return []

    def remove_one(self, path: Path) -> None:
        """Remove one of this run's worktrees. Git's failure is a problem; the folder is then deleted so the run still cleans up after itself."""
        try:
            done = run_command("git", ["-C", str(self.root), "worktree", "remove", "--force", str(path)], timeout=GIT_TIMEOUT_SECONDS)
            failure = done.stderr.strip() or f"exit status {done.returncode}" if done.returncode != 0 else ""
        except (SubprocessError, OSError, ValueError) as error:
            failure = str(error)
        if failure:
            self.problems.append(f"git worktree remove failed for {path}: {failure}")
            if path.exists():
                shutil.rmtree(path, ignore_errors=True)

    def remove_all(self) -> None:
        """Remove every added worktree and prune; then check this run's own folders. Changes other runs made meanwhile are notes, not problems."""
        for path in reversed(self.paths):
            self.remove_one(path)
        try:
            run_command("git", ["-C", str(self.root), "worktree", "prune"], timeout=GIT_TIMEOUT_SECONDS, check=True)
        except (SubprocessError, OSError, ValueError) as error:
            self.problems.append(f"git worktree prune failed: {error}")
        kept = sorted(str(path) for path in self.paths if path.exists())
        if kept:
            self.problems.append(f"folders of worktrees this run added still exist: {kept}")
        self.after = self.listing("after")
        if self.after != self.before:
            self.notes.append("the registered worktrees changed during the run (other runs add and remove their own; report only)")

    def close(self) -> dict[str, Any]:
        """Remove everything this run added, restore the signal handlers and return the record; problems are also printed to standard error.

        The stop signals and SIGINT are blocked until the removal and the checks are done; one that arrives meanwhile is delivered after the handlers are
        restored.
        """
        held = signal.pthread_sigmask(signal.SIG_BLOCK, DEFERRED_SIGNALS)
        try:
            self.remove_all()
            for problem in self.problems:
                sys.stderr.write(f"scratch worktree: {problem}\n")
            record = self.record()
        finally:
            for number, handler in self.previous.items():
                signal.signal(number, handler)
            signal.pthread_sigmask(signal.SIG_SETMASK, held)
        return record

    def record(self) -> dict[str, Any]:
        """What the report keeps: the registered worktrees before and after, whether they match, and any fallback or problem."""
        return {
            "before": self.before,
            "after": self.after,
            "unchanged": self.after == self.before,
            "notes": self.notes,
            "problems": self.problems,
        }
