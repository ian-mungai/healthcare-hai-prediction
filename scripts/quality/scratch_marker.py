"""Marker of the Git folders that an end-to-end runner made itself, so that a helper which resets or cleans a folder refuses any other.

A runner that makes a scratch Git folder (``git worktree add`` through ``ScratchWorktrees``, or ``git init``) calls ``mark`` on it. A helper that runs
``git reset --hard`` or ``git clean`` in a folder it is given calls ``require`` first. ``mark`` writes ``hai_scratch_folder.json`` into the folder's own
Git directory (``git rev-parse --absolute-git-dir``: the ``.git`` folder of a repository, ``.git/worktrees/<name>`` of a linked worktree) with the runner
and the folder's resolved path. ``require`` accepts a folder only when that file exists, can be read and names exactly that folder, so the main checkout,
any other repository and a folder that holds a copy of another folder's marker are all refused. A refusal raises ``NotScratchFolder`` with the folder
named and changes nothing. The marker lives and ends with the Git directory.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from scripts.process import run_command

MARKER = "hai_scratch_folder.json"
GIT_SECONDS = 60


class NotScratchFolder(RuntimeError):
    """A helper was asked to reset or clean a folder that no end-to-end runner made."""


def text_form(path: Path) -> str:
    """The path as the captured text output of a program shows it: each carriage return, with or without its newline, becomes one newline."""
    return str(path).replace("\r\n", "\n").replace("\r", "\n")


def discovered_git_directory(folder: Path) -> Path | None:
    """The Git directory that the first ``.git`` above ``folder`` names, read from the file system so that a carriage return in a path stays one."""
    for directory in (folder, *folder.parents):
        entry = directory / ".git"
        if entry.is_dir():
            return entry.resolve()
        if entry.is_file():
            with entry.open(encoding="utf-8", errors="surrogateescape", newline="") as handle:
                line = handle.read().split("\n", 1)[0]
            target = line.removeprefix("gitdir: ")
            return (directory / target).resolve() if line.startswith("gitdir: ") else None
    return None


def git_directory(folder: Path) -> Path | None:
    """The absolute Git directory of ``folder``, or None when it is not inside a Git repository.

    Git decides whether there is one. Its answer reaches this process as text in which a carriage return became a newline, so a path with a newline in
    it is taken from the file system instead when that path reads back exactly as Git's answer does.
    """
    result = run_command("git", ["-C", str(folder), "rev-parse", "--absolute-git-dir"], timeout=GIT_SECONDS)
    text = result.stdout.strip()
    if result.returncode != 0 or not text:
        return None
    if "\n" in text:
        found = discovered_git_directory(folder.resolve())
        if found is not None and text_form(found) == text:
            return found
    return Path(text)


def mark(folder: Path, runner: str) -> None:
    """Record that ``runner`` made the Git folder ``folder``."""
    directory = git_directory(folder)
    if directory is None:
        raise NotScratchFolder(f"{folder} is not a Git folder, so it cannot be marked as made by {runner}")
    (directory / MARKER).write_text(json.dumps({"runner": runner, "folder": str(folder.resolve()), "pid": os.getpid()}), encoding="utf-8")


def require(folder: Path, action: str) -> None:
    """Raise ``NotScratchFolder`` unless an end-to-end runner made ``folder``; ``action`` names what was refused."""
    directory = git_directory(folder)
    problem = "it is not a Git folder"
    if directory is not None:
        try:
            record = json.loads((directory / MARKER).read_text(encoding="utf-8"))
            ours = isinstance(record, dict) and bool(record.get("runner")) and record.get("folder") == str(folder.resolve())
            problem = "" if ours else "its marker names another folder"
        except (OSError, ValueError):
            problem = "it has no readable scratch marker in its Git directory"
    if problem:
        raise NotScratchFolder(f"{action} refused for {folder}: no end-to-end runner made it ({problem}); nothing was changed")
