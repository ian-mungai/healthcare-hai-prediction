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
``close`` returns the record for the report and prints every problem to standard error: a worktree still registered, or registered worktrees
(``git worktree list --porcelain``) that differ from those before the run. The runner fails its run on a record with problems. SIGKILL cannot be handled.
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


def registered_worktrees(root: Path) -> list[str]:
    """The paths in ``git worktree list --porcelain``, sorted; the HEAD lines are left out because a commit may move them during a long run."""
    listing = run_command("git", ["-C", str(root), "worktree", "list", "--porcelain"], timeout=60, check=True).stdout
    return sorted(line.removeprefix("worktree ") for line in listing.splitlines() if line.startswith("worktree "))


def stop_run(signum: int, frame: FrameType | None) -> None:
    raise SystemExit(128 + signum)


class ScratchWorktrees:
    """The scratch worktrees one run added to the repository at ``root``, and what removing them did."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.paths: list[Path] = []
        self.before = registered_worktrees(root)
        self.after: list[str] = []
        self.notes: list[str] = []
        self.problems: list[str] = []
        self.previous = {number: signal.signal(number, stop_run) for number in STOP_SIGNALS}

    def add(self, path: Path) -> Path:
        """Add a detached worktree of HEAD at ``path``, a new or empty folder. The path is recorded first, so a half-made worktree is removed too."""
        if path.exists() and any(path.iterdir()):
            raise FileExistsError(f"{path} is not empty; the fallback removal would delete it")
        self.paths.append(path)
        run_command(
            "git",
            ["-C", str(self.root), "worktree", "add", "--quiet", "--detach", str(path), "HEAD"],
            env={**os.environ, "GIT_LFS_SKIP_SMUDGE": "1"},
            timeout=GIT_TIMEOUT_SECONDS,
            check=True,
        )
        return path

    def remove_one(self, path: Path) -> None:
        try:
            done = run_command("git", ["-C", str(self.root), "worktree", "remove", "--force", str(path)], timeout=GIT_TIMEOUT_SECONDS)
            failure = done.stderr.strip() if done.returncode != 0 else ""
        except (SubprocessError, OSError) as error:
            failure = str(error)
        if failure and path.exists():
            self.notes.append(f"git worktree remove failed for {path} ({failure}); deleted the folder instead")
            shutil.rmtree(path, ignore_errors=True)

    def remove_all(self) -> None:
        """Remove every added worktree, prune and compare the registered worktrees with those before the run."""
        for path in reversed(self.paths):
            self.remove_one(path)
        try:
            run_command("git", ["-C", str(self.root), "worktree", "prune"], timeout=GIT_TIMEOUT_SECONDS, check=True)
            self.after = registered_worktrees(self.root)
        except (SubprocessError, OSError) as error:
            self.problems.append(f"git worktree prune or list failed: {error}")
            return
        if self.after != self.before:
            added = sorted(set(self.after) - set(self.before))
            gone = sorted(set(self.before) - set(self.after))
            self.problems.append(f"registered worktrees changed during the run: still registered {added}; no longer registered {gone}")

    def close(self) -> dict[str, Any]:
        """Remove everything this run added, restore the signal handlers and return the record; problems are also printed to standard error.

        The stop signals and SIGINT are blocked until the removal and the check of the registered worktrees are done; one that arrives meanwhile is delivered
        after the handlers are restored.
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
