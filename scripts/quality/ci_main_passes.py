"""Reuse the pass of a check group that an earlier local CI run recorded, when the group's inputs are byte-identical.

Only full local runs of ``scripts/run_ci.sh`` use this, in one of two modes: ``main`` (the default of a local run) and ``gate`` (the pre-push gate,
``HAI_CI_REUSE=gate``, set only by scripts/quality/pre_push.py). Hosted CI has no records and sets neither mode. The inputs of a group are the files
of the working tree (tracked, or untracked and not ignored) whose change would select the group: the paths for which ``scripts/quality/ci_groups.py``
selects it in the ``paths`` scope. A path that no group lists, or that is ``global``, belongs to every group. A ``full_only`` group has no path list,
so every file is its input. The input hash is the SHA-256 of the sorted ``path`` and Git blob hash lines of those files.

## Main mode

A record holds the group, the input hash, the commit and the time. It is written only when every one of these holds:

1. the run was the full scope of every part (``scripts/run_ci.sh`` decides this) and every group of it passed;
2. the checked-out commit is in ``origin/main`` (is an ancestor of it or equal to it);
3. the working tree has no change (no staged, unstaged or untracked file that Git does not ignore).

A group is reused only when its record has the same input hash and the commit of the record is still in ``origin/main``. A group in ``reuse.never`` of
config/quality/ci_groups.json (``repository_hooks``: it checks the pushed range) is never recorded or reused. The records are files in the Git common
folder (``hai_ci_main_passes/<group>.json``), so every worktree of the repository shares them and no commit carries them. ``HAI_CI_PASSES_DIR`` and
``HAI_CI_MAIN_REF`` replace the folder and the main reference for tests of this script only.

## Gate mode

The gate reuses a pass of its own, on any commit and any tree, never one recorded for main. Only a group named in ``reuse.closed`` of
config/quality/ci_groups.json is eligible, and only when this script knows its closure check (the launcher: ``python -m scripts.quality.launcher_inputs
--check``), because a path list alone cannot show that a group reads no other file. Any other group always runs.

The key of a group is its input hash with the Python and Bash versions and the tool pins (``config/quality_tools.json``). A record
(``hai_ci_gate_passes/<group>.json`` in the Git common folder, or ``gate/`` below ``HAI_CI_PASSES_DIR``) holds the group, the key, the commit, the
closure check and the time. It is written only for a group that passed in a run that passed and whose closure check passes. It is reused only for an
equal key and a closure check that passes at that moment. A run that failed drops the records of the groups that ran (``drop``), so a failure is never
reused and an older pass on the same key cannot return.

    .venv/bin/python -m scripts.quality.ci_main_passes filter --groups <group>... [--mode gate]   prints ``reuse <group> <commit>`` or ``run <group>``
    .venv/bin/python -m scripts.quality.ci_main_passes record --groups <group>... [--mode gate] [--test-any-state]
    .venv/bin/python -m scripts.quality.ci_main_passes drop --groups <group>...                  removes the gate records of the groups

E2E: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e --reuse-only``.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import pwd
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command
from scripts.quality import ci_groups

GIT_SECONDS = 300
CHUNK = 400
MAIN_REF = "origin/main"
FOLDER = "hai_ci_main_passes"
GATE_FOLDER = "hai_ci_gate_passes"
MODES = ("main", "gate")
CLOSURE_SECONDS = 900
KEY_FILES = ("config/quality_tools.json",)


def git(root: Path, *args: str) -> tuple[int, str]:
    result = run_command("git", ["-C", str(root), *args], timeout=GIT_SECONDS)
    return result.returncode, result.stdout.strip()


def passes_folder(root: Path, mode: str = "main") -> Path:
    override = os.environ.get("HAI_CI_PASSES_DIR")
    if override:
        return Path(override) / "gate" if mode == "gate" else Path(override)
    code, common = git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
    if code != 0 or not common:
        raise RuntimeError("the Git common folder cannot be found")
    return Path(common) / (GATE_FOLDER if mode == "gate" else FOLDER)


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


def closed(root: Path) -> list[str]:
    """The groups that gate mode may reuse: those that ``reuse.closed`` names."""
    config = json.loads((root / ci_groups.CONFIG).read_text(encoding="utf-8"))
    return [str(group) for group in config.get("reuse", {}).get("closed", [])]


def closure_problem(root: Path, group: str) -> str:
    """Why the closure check of a group does not pass now, or an empty string. Only the launcher has one: its input list is computed from the code it runs."""
    if group not in closed(root) or group != "launcher":
        return f"{group} has no closure check"
    try:
        result = run_command(sys.executable, ["-m", "scripts.quality.launcher_inputs", "--check"], cwd=root, timeout=CLOSURE_SECONDS)
    except (OSError, TimeoutExpired, ValueError) as error:
        return f"the launcher input check could not run ({type(error).__name__})"
    return "" if result.returncode == 0 else f"the launcher input check failed ({result.returncode})"


class KeyUnavailable(Exception):
    """An input that the key of a group needs cannot be read or does not verify: the group is never reused and never recorded."""


def archive_identity(root: Path) -> str:
    """The dbt package archive that the launcher E2E verifies and reads (it is Git-ignored): its verification must pass, and its identity is the SHA-256 of
    the archive file that the verification names, which the committed record config/lakehouse/dbt_package_archive.json pins."""
    try:
        result = run_command(sys.executable, ["-m", "scripts.lakehouse.dbt_package_archive", "verify", "--root", str(root)], cwd=root, timeout=300)
    except (OSError, TimeoutExpired, ValueError) as error:
        raise KeyUnavailable(f"the dbt package archive check could not run ({type(error).__name__})") from error
    if result.returncode != 0:
        raise KeyUnavailable("the dbt package archive is missing or fails its verification")
    try:
        path = Path(json.loads(result.stdout))
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except (OSError, ValueError, TypeError) as error:
        raise KeyUnavailable("the verified dbt package archive cannot be read") from error


def account_home() -> Path:
    """The home folder of the account in the user database (the launcher ignores HOME for the same reason)."""
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def aws_account_identity(root: Path) -> str:
    """The SHA-256 of the AWS config and credentials files of the account (``absent`` for a missing one): the launcher E2E opens them (traced Nov 22 2026), so a
    pass under another account state is not reused. Only the digests enter the key."""
    parts = []
    for name in ("config", "credentials"):
        path = account_home() / ".aws" / name
        try:
            parts.append(f"{name}={hashlib.sha256(path.read_bytes()).hexdigest()}")
        except FileNotFoundError:
            parts.append(f"{name}=absent")
        except OSError as error:
            raise KeyUnavailable(f"the AWS {name} file cannot be read ({type(error).__name__})") from error
    return "\0".join(parts)


# The Git-ignored files that a group reads at run time, by content: (label, function that returns the identity or raises KeyUnavailable).
# A group with an ignored input that is not listed here must not be in reuse.closed.
IGNORED_INPUTS = {"launcher": (("dbt package archive", archive_identity), ("AWS account files", aws_account_identity))}


# The interpreter variables that change what it imports, loads or computes (not PYTHON_BIN of run_ci.sh, nor the bytecode and buffering switches).
PYTHON_VARIABLES = (
    "PYTHONHOME",
    "PYTHONPATH",
    "PYTHONSTARTUP",
    "PYTHONUSERBASE",
    "PYTHONNOUSERSITE",
    "PYTHONSAFEPATH",
    "PYTHONPLATLIBDIR",
    "PYTHONOPTIMIZE",
    "PYTHONPYCACHEPREFIX",
    "PYTHONHASHSEED",
    "PYTHONUTF8",
    "PYTHONIOENCODING",
    "PYTHONWARNINGS",
)


def environment_identity() -> str:
    """The effective Python environment of this process as the interpreter sees it: its path, real file and version, the name and version of every installed
    distribution, the interpreter's PYTHON variables, the search path and every .pth, sitecustomize and usercustomize file on it."""
    distributions = sorted(
        (re.sub(r"[-_.]+", "-", str(dist.metadata["Name"])).lower(), dist.version) for dist in importlib.metadata.distributions() if dist.metadata["Name"]
    )
    real = Path(os.path.realpath(sys.executable))
    try:
        interpreter = hashlib.sha256(real.read_bytes()).hexdigest()
    except OSError:
        interpreter = "unreadable"
    startup: list[str] = []
    for entry in sys.path:
        folder = Path(entry or ".")
        if folder.is_dir():
            for name in sorted(path.name for path in folder.iterdir() if path.name.endswith(".pth") or path.name in {"sitecustomize.py", "usercustomize.py"}):
                startup.append(f"{folder / name}={hashlib.sha256((folder / name).read_bytes()).hexdigest()}")
    variables = sorted((name, value) for name, value in os.environ.items() if name in PYTHON_VARIABLES)
    parts = [sys.executable, str(real), interpreter, sys.version, sys.prefix, sys.base_prefix, *sys.path, *startup, *(f"{n}={v}" for n, v in variables)]
    parts += [f"{name}=={version}" for name, version in distributions]
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()


def tool_versions() -> list[str]:
    """The versions of the programs outside the repository that the launcher E2E starts: git and the AWS command (``absent`` when a program is missing)."""
    found = []
    for program in ("git", "aws"):
        try:
            result = run_command(program, ["--version"], timeout=60)
            found.append(f"{program}: {(result.stdout + result.stderr).strip()[:200]}")
        except (OSError, TimeoutExpired, ValueError):
            found.append(f"{program}: absent")
    return found


def gate_keys(root: Path, groups: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """The gate key of each group, and why a group has none. The key is the input hash of the group with the environment identity, the Python and Bash
    versions, the tool pins, the content of every ignored input the group reads and the worktree root, so another worktree never inherits a pass."""
    hashes = input_hashes(root, groups)
    pins = [path for path in KEY_FILES if (root / path).is_file()]
    blobs = blob_hashes(root, pins) if pins else {}
    bash = run_command("bash", ["--version"], timeout=60).stdout.splitlines()[:1]
    extras = "\0".join(
        [
            sys.version,
            platform.platform(),
            platform.machine(),
            *bash,
            *tool_versions(),
            *(f"{path}={blobs[path]}" for path in pins),
            environment_identity(),
            str(root.resolve()),
        ]
    )
    keys: dict[str, str] = {}
    unavailable: dict[str, str] = {}
    for group in groups:
        try:
            ignored = [f"{label}={identity(root)}" for label, identity in IGNORED_INPUTS.get(group, ())]
        except KeyUnavailable as error:
            unavailable[group] = str(error)
            continue
        keys[group] = hashlib.sha256("\0".join([hashes[group], extras, *ignored]).encode()).hexdigest()
    return keys, unavailable


def gate_reusable(root: Path, groups: list[str], identity: Path | None = None) -> dict[str, str]:
    """The groups whose gate record has the key of the working tree now and whose closure check passes now, with the commit of the record. The keys of every
    closed group are also written to ``identity``: the record at the end of the run certifies that state, and only when it is unchanged."""
    wanted = [group for group in groups if group in closed(root)]
    if not wanted:
        return {}
    keys, _ = gate_keys(root, wanted)
    if identity is not None:
        identity.write_text(json.dumps(keys, indent=2) + "\n", encoding="utf-8")
    folder = passes_folder(root, "gate")
    found: dict[str, str] = {}
    for group in keys:
        record = read_record(folder, group)
        if record and record.get("key") == keys[group] and isinstance(record.get("commit"), str) and not closure_problem(root, group):
            found[group] = record["commit"]
    return found


def gate_record(root: Path, groups: list[str], identity: Path | None = None) -> tuple[list[str], str]:
    """Write the gate records of the groups that have a closure check that passes now and whose inputs are what the run started with; return the groups
    written and the reason when none was. The key written is the one taken before the run."""
    chosen = [group for group in groups if group in closed(root)]
    if not chosen:
        return [], "no group has a closure check"
    if identity is None or not identity.is_file():
        return [], "there is no identity of the inputs from before the run"
    before = json.loads(identity.read_text(encoding="utf-8"))
    problems = {group: closure_problem(root, group) for group in chosen}
    keys, unavailable = gate_keys(root, [group for group in chosen if not problems[group]])
    reasons = [problem for problem in problems.values() if problem] + list(unavailable.values())
    written: list[str] = []
    code, commit = git(root, "rev-parse", "HEAD")
    folder = passes_folder(root, "gate")
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    for group in keys:
        if before.get(group) != keys[group]:
            reasons.append(f"the inputs of {group} are not what the run started with (changed during the run, or no identity was taken)")
            continue
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / f"{group}.json"
        temporary = folder / f".{group}.{os.getpid()}.tmp"
        body = {
            "group": group,
            "key": before[group],
            "commit": commit if code == 0 else "unknown",
            "closure": "scripts.quality.launcher_inputs",
            "recorded_utc": now,
        }
        temporary.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
        temporary.replace(target)
        written.append(group)
    return written, "; ".join(reasons)


def gate_drop(root: Path, groups: list[str]) -> list[str]:
    """Remove the gate records of the groups (a run that failed): a failure is never reused and an older pass on the same key cannot return."""
    folder = passes_folder(root, "gate")
    dropped = []
    for group in groups:
        target = folder / f"{group}.json"
        if target.exists():
            target.unlink()
            dropped.append(group)
    return dropped


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
    for name in ("filter", "record", "drop"):
        command = commands.add_parser(name)
        command.add_argument("--root", type=Path, default=Path.cwd())
        command.add_argument("--groups", nargs="*", default=[])
        if name != "drop":
            command.add_argument("--mode", choices=MODES, default="main")
            command.add_argument(
                "--identity", type=Path, help="gate mode: filter writes the keys of the inputs before the run here, record certifies only that state"
            )
        if name == "record":
            command.add_argument("--test-any-state", action="store_true", help="skip the main-commit and clean-tree conditions (for tests of this script only)")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    mode = getattr(args, "mode", "gate")
    try:
        if args.command == "drop":
            dropped = gate_drop(root, args.groups)
            sys.stdout.write(f"dropped {' '.join(dropped)}\n" if dropped else "dropped nothing\n")
            return 0
        if args.command == "filter":
            found = gate_reusable(root, args.groups, args.identity) if mode == "gate" else reusable(root, args.groups)
            for group in args.groups:
                sys.stdout.write(f"reuse {group} {found[group][:12]}\n" if group in found else f"run {group}\n")
            return 0
        written, reason = gate_record(root, args.groups, args.identity) if mode == "gate" else record(root, args.groups, args.test_any_state)
    except (RuntimeError, OSError, ValueError) as error:
        # A failure to read or write records never fails the run: every group then runs, and nothing is recorded.
        sys.stdout.write("".join(f"run {group}\n" for group in args.groups) if args.command == "filter" else f"not recorded: {error}\n")
        return 0
    sys.stdout.write(f"recorded {' '.join(written)}\n" if written else f"not recorded: {reason}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
