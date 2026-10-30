"""Reuse the pass of a check group that a full local CI run recorded on a commit of ``origin/main``, when the group's inputs are byte-identical.

Only full local runs of ``scripts/run_ci.sh`` use this. The pre-push gate never does (it sets ``HAI_CI_REUSE=off`` and ``HAI_DOCKER_RELEASE=outer``),
and hosted CI has no records. The inputs of a group are the files of the working tree (tracked, or untracked and not ignored) whose change would select
the group: the paths for which ``scripts/quality/ci_groups.py`` selects it in the ``paths`` scope. A path that no group lists, or that is ``global``,
belongs to every group. A ``full_only`` group has no path list, so every file is its input. The input hash is the SHA-256 of the sorted ``path`` and Git
blob hash lines of those files.

A record holds the group, the input hash, the commit and the time. It is written only when every one of these holds:

1. the run was the full scope of every part (``scripts/run_ci.sh`` decides this) and every group of it passed;
2. the checked-out commit is in ``origin/main`` (is an ancestor of it or equal to it);
3. the working tree has no change (no staged, unstaged or untracked file that Git does not ignore).

A group is reused only when its record has the same input hash and the commit of the record is still in ``origin/main``. A group in ``reuse.never`` of
config/quality/ci_groups.json (``repository_hooks``: it checks the pushed range) is never recorded or reused. The records are files in the Git common folder
(``hai_ci_main_passes/<group>.json``), so every worktree of the repository shares them and no commit carries them. ``HAI_CI_PASSES_DIR`` and
``HAI_CI_MAIN_REF`` replace the folder and the main reference for tests of this script only.

    .venv/bin/python -m scripts.quality.ci_main_passes filter --groups <group>...     prints ``reuse <group> <commit>`` or ``run <group>`` per group
    .venv/bin/python -m scripts.quality.ci_main_passes record --groups <group>... [--test-any-state]

E2E: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e --selection-only``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.quality import ci_groups

GIT_SECONDS = 300
CHUNK = 400
MAIN_REF = "origin/main"
FOLDER = "hai_ci_main_passes"


def git(root: Path, *args: str) -> tuple[int, str]:
    result = run_command("git", ["-C", str(root), *args], timeout=GIT_SECONDS)
    return result.returncode, result.stdout.strip()


def passes_folder(root: Path) -> Path:
    override = os.environ.get("HAI_CI_PASSES_DIR")
    if override:
        return Path(override)
    code, common = git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
    if code != 0 or not common:
        raise RuntimeError("the Git common folder cannot be found")
    return Path(common) / FOLDER


def main_ref() -> str:
    return os.environ.get("HAI_CI_MAIN_REF") or MAIN_REF


def working_files(root: Path) -> list[str]:
    code, listing = git(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    if code != 0:
        raise RuntimeError("the file list cannot be read from Git")
    return sorted({path for path in listing.split("\0") if path and (root / path).is_file()})


def blob_hashes(root: Path, paths: list[str]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for start in range(0, len(paths), CHUNK):
        chunk = paths[start : start + CHUNK]
        code, listing = git(root, "hash-object", "--", *chunk)
        lines = listing.splitlines()
        if code != 0 or len(lines) != len(chunk):
            raise RuntimeError("the file hashes cannot be read from Git")
        hashes.update(zip(chunk, lines, strict=True))
    return hashes


def input_hashes(root: Path, groups: list[str]) -> dict[str, str]:
    """The input hash of each group: the files whose change selects it, with their blob hashes."""
    config = ci_groups.load()
    paths = working_files(root)
    blobs = blob_hashes(root, paths)
    lines: dict[str, list[str]] = {group: [] for group in groups}
    for path in paths:
        selected = set(ci_groups.plan("paths", "all", paths=[path], config=config)["run"])
        for group in groups:
            if group in selected or group in config["full_only"]:
                lines[group].append(f"{path}\0{blobs[path]}\n")
    return {group: hashlib.sha256(("".join(lines[group])).encode()).hexdigest() for group in groups}


def never(root: Path) -> set[str]:
    config = json.loads((root / ci_groups.CONFIG).read_text(encoding="utf-8"))
    return set(config.get("reuse", {}).get("never", []))


def in_main(root: Path, commit: str) -> bool:
    return git(root, "merge-base", "--is-ancestor", commit, main_ref())[0] == 0


def read_record(folder: Path, group: str) -> dict[str, Any] | None:
    try:
        record = json.loads((folder / f"{group}.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return record if isinstance(record, dict) and record.get("group") == group else None


def reusable(root: Path, groups: list[str]) -> dict[str, str]:
    """The groups whose inputs match a record from a commit still in ``origin/main``, with that commit."""
    wanted = [group for group in groups if group not in never(root)]
    folder = passes_folder(root)
    records = {group: read_record(folder, group) for group in wanted}
    candidates = [group for group in wanted if records[group] is not None]
    if not candidates:
        return {}
    hashes = input_hashes(root, candidates)
    found: dict[str, str] = {}
    for group in candidates:
        record = records[group]
        if record and record.get("input_hash") == hashes[group] and isinstance(record.get("commit"), str) and in_main(root, record["commit"]):
            found[group] = record["commit"]
    return found


def refusal(root: Path) -> str:
    """Why a pass cannot be recorded now, or an empty string."""
    code, head = git(root, "rev-parse", "HEAD")
    if code != 0 or not head:
        return "HEAD is not a commit"
    if not in_main(root, head):
        return f"the checked-out commit is not in {main_ref()}"
    code, dirty = git(root, "status", "--porcelain", "--untracked-files=normal")
    if code != 0 or dirty:
        return "the working tree has changes (staged, unstaged or untracked files)"
    return ""


def record(root: Path, groups: list[str], any_state: bool = False) -> tuple[list[str], str]:
    """Write the records of the groups that passed; return the groups written and the reason when none was."""
    reason = "" if any_state else refusal(root)
    if reason:
        return [], reason
    chosen = [group for group in groups if group not in never(root)]
    hashes = input_hashes(root, chosen)
    commit = git(root, "rev-parse", "HEAD")[1]
    folder = passes_folder(root)
    folder.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    for group in chosen:
        target = folder / f"{group}.json"
        temporary = folder / f".{group}.{os.getpid()}.tmp"
        temporary.write_text(
            json.dumps({"group": group, "input_hash": hashes[group], "commit": commit, "recorded_utc": now}, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(target)
    return chosen, ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("filter", "record"):
        command = commands.add_parser(name)
        command.add_argument("--root", type=Path, default=Path.cwd())
        command.add_argument("--groups", nargs="*", default=[])
        if name == "record":
            command.add_argument("--test-any-state", action="store_true", help="skip the main-commit and clean-tree conditions (for tests of this script only)")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        if args.command == "filter":
            found = reusable(root, args.groups)
            for group in args.groups:
                sys.stdout.write(f"reuse {group} {found[group][:12]}\n" if group in found else f"run {group}\n")
            return 0
        written, reason = record(root, args.groups, args.test_any_state)
    except (RuntimeError, OSError, ValueError) as error:
        # A failure to read or write records never fails the run: every group then runs, and nothing is recorded.
        sys.stdout.write("".join(f"run {group}\n" for group in args.groups) if args.command == "filter" else f"not recorded: {error}\n")
        return 0
    sys.stdout.write(f"recorded {' '.join(written)}\n" if written else f"not recorded: {reason}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
