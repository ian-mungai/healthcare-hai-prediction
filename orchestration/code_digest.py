"""Copy and verify the declared training code with a portable file digest."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

TRAINING_ROOTS = (
    "scripts",
    "orchestration",
    "feature_store",
    "config/training",
    "data_contracts/features",
    "data_contracts/lakehouse",
    "data_contracts/training",
    "docs/analysis_plan.md",
    "dbt/seeds/gold/*_bridge.csv",
    "dbt/seeds/features/frozen_hai_outcome_controls.csv",
)
RECORD = "snapshot_training.json"
PENDING = "snapshot_training.pending.json"
COMPONENT = re.compile(r"[A-Za-z0-9_.-]+")
GIT_SECONDS = 10.0
SNAPSHOT_SECONDS = 120.0
GitReader = Callable[[Path, Sequence[str], float], dict[str, Any]]


class SnapshotError(ValueError):
    """The snapshot is incomplete, changed or outside its declared file contract."""


def remaining(deadline: float) -> float:
    """Return the shared operation's remaining seconds or fail visibly."""
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError("training snapshot exceeded its absolute deadline")
    return left


def _regular(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise SnapshotError("snapshot inputs must be regular files without links")
    return info


def file_entries(root: Path, roots: Sequence[str] = TRAINING_ROOTS, *, deadline: float | None = None) -> list[dict[str, Any]]:
    """List regular files under declared roots, excluding bytecode, with portable hashes and executable classes."""
    stop = deadline if deadline is not None else time.monotonic() + SNAPSHOT_SECONDS
    selected: set[Path] = set()

    def visit(path: Path) -> None:
        remaining(stop)
        relative = path.relative_to(root)
        if "__pycache__" in relative.parts or path.suffix == ".pyc":
            return
        if any(part in (".", "..") or not COMPONENT.fullmatch(part) for part in relative.parts):
            raise SnapshotError("snapshot input has a nonportable path component")
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            for child in sorted(path.iterdir(), key=lambda item: os.fsencode(item.name)):
                visit(child)
        elif stat.S_ISREG(mode):
            selected.add(path)
        else:
            raise SnapshotError("snapshot inputs contain a link or special file")

    for declared in roots:
        matches = [root] if declared == "." else sorted(root.glob(declared))
        if not matches:
            raise SnapshotError(f"snapshot root is missing: {declared}")
        for path in matches:
            for parent in path.relative_to(root).parents:
                if (root / parent).is_symlink():
                    raise SnapshotError("snapshot inputs have a linked parent")
            visit(path)
    entries = []
    for path in sorted(selected, key=lambda item: os.fsencode(item.relative_to(root).as_posix())):
        remaining(stop)
        info = _regular(path)
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while block := handle.read(1 << 20):
                remaining(stop)
                digest.update(block)
        entries.append(
            {"path": path.relative_to(root).as_posix(), "mode": "x" if info.st_mode & 0o111 else "r", "size": info.st_size, "sha256": digest.hexdigest()}
        )
    return entries


def entries_digest(entries: Sequence[dict[str, Any]]) -> str:
    """Hash sorted path, mode class, byte count and SHA-256 lines using the run-path contract."""
    digest = hashlib.sha256()
    for entry in sorted(entries, key=lambda item: str(item["path"]).encode()):
        digest.update(f"{entry['path']}\0{entry['mode']}\0{entry['size']}\0{entry['sha256']}\n".encode())
    return digest.hexdigest()


def tree_digest(root: Path, roots: Sequence[str] = TRAINING_ROOTS, *, deadline: float | None = None) -> str:
    """Return the portable digest of every regular file under the declared roots."""
    return entries_digest(file_entries(root, roots, deadline=deadline))


def git_state(root: Path, files: Sequence[str], deadline: float) -> dict[str, Any]:
    """Record bounded Git identity and dirtiness, including every copied file absent from Git's index."""

    def git(arguments: list[str]) -> str:
        try:
            result = run_command("git", arguments, cwd=root, timeout=min(GIT_SECONDS, remaining(deadline)))
        except TimeoutExpired as error:
            raise SnapshotError("snapshot Git call exceeded its deadline") from error
        if result.returncode:
            raise SnapshotError("snapshot Git identity could not be recorded")
        return result.stdout

    commit = git(["rev-parse", "HEAD"]).strip()
    if not re.fullmatch(r"[a-f0-9]{40,64}", commit):
        raise SnapshotError("snapshot Git commit is malformed")
    dirty = git(["status", "--porcelain", "--untracked-files=no", "--", *TRAINING_ROOTS])
    tracked = set(git(["ls-files", "-z", "--", *TRAINING_ROOTS]).split("\0"))
    untracked = sorted(set(files) - tracked)
    return {"git_commit": commit, "git_dirty": bool(dirty or untracked), "untracked_files": untracked}


def _write(path: Path, record: dict[str, Any]) -> None:
    with tempfile.TemporaryDirectory(prefix=f".{path.name}_", dir=path.parent) as temporary:
        pending = Path(temporary) / "record.json"
        with pending.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        pending.chmod(0o444)
        os.replace(pending, path)


def verify_snapshot(folder: Path, run_id: str, expected_digest: str | None = None, *, deadline: float | None = None) -> dict[str, Any]:
    """Verify the recorded run identity and every code byte before reuse or execution."""
    path = folder / RECORD
    _regular(path)
    record = json.loads(path.read_bytes())
    if not isinstance(record, dict) or record.get("schema_version") != 1 or record.get("run_id") != run_id:
        raise SnapshotError("snapshot record names another run or schema")
    entries = file_entries(folder / "code", (".",), deadline=deadline)
    digest = entries_digest(entries)
    if entries != record.get("files") or digest != record.get("code_digest") or expected_digest is not None and digest != expected_digest:
        raise SnapshotError("snapshot code or completion record changed")
    if not re.fullmatch(r"[a-f0-9]{40,64}", str(record.get("git_commit", ""))) or type(record.get("git_dirty")) is not bool:
        raise SnapshotError("snapshot record has no valid Git provenance")
    return record


def snapshot_training(checkout: Path, folder: Path, run_id: str, *, git_reader: GitReader = git_state, deadline: float | None = None) -> dict[str, Any]:
    """Copy code once for a run, synchronously publish its record and verify retries without consulting the live checkout."""
    stop = deadline if deadline is not None else time.monotonic() + SNAPSHOT_SECONDS
    folder.mkdir(parents=True, exist_ok=True)
    if (folder / RECORD).exists() or (folder / RECORD).is_symlink():
        return verify_snapshot(folder, run_id, deadline=stop)
    code, pending = folder / "code", folder / PENDING
    if code.exists():
        _regular(pending)
        record = json.loads(pending.read_bytes())
        if record.get("run_id") != run_id or file_entries(code, deadline=stop) != record.get("files"):
            raise SnapshotError("interrupted snapshot does not match its pending record")
        os.replace(pending, folder / RECORD)
        return verify_snapshot(folder, run_id, deadline=stop)
    entries = file_entries(checkout, deadline=stop)
    provenance = git_reader(checkout, [str(entry["path"]) for entry in entries], stop)
    with tempfile.TemporaryDirectory(prefix=".training_code_", dir=folder) as temporary:
        destination = Path(temporary) / "code"
        destination.mkdir()
        for entry in entries:
            remaining(stop)
            target = destination / entry["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(checkout / entry["path"], target, follow_symlinks=False)
            target.chmod(0o555 if entry["mode"] == "x" else 0o444)
            with target.open("rb") as handle:
                os.fsync(handle.fileno())
        copied = file_entries(destination, deadline=stop)
        if copied != entries or file_entries(checkout, deadline=stop) != entries:
            raise SnapshotError("snapshot inputs changed while copying")
        plan = destination / "docs/analysis_plan.md"
        if not plan.stat().st_size:
            raise SnapshotError("snapshot analysis plan is empty")
        record = {"schema_version": 1, "run_id": run_id, "code_digest": entries_digest(copied), "files": copied, **provenance}
        _write(pending, record)
        os.replace(destination, code)
    os.replace(pending, folder / RECORD)
    descriptor = os.open(folder, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return verify_snapshot(folder, run_id, deadline=stop)
