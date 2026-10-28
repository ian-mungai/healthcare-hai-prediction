"""Verify immutable files and manifest-driven archives without importing staged code."""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
import stat
import tarfile
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

CHUNK = 1 << 20
MAX_RECORD = 32 << 20
MAX_MEMBERS = 200_000
NAME = re.compile(r"[A-Za-z0-9_.-]+")
SHA256 = re.compile(r"[a-f0-9]{64}")
DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


class StageError(RuntimeError):
    """A stable refusal code and exit status with no external error text."""

    def __init__(self, code: str, exit_code: int = 70) -> None:
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


def check_deadline(deadline: float) -> None:
    """Refuse work after the caller's absolute monotonic deadline."""
    if not math.isfinite(deadline) or time.monotonic() >= deadline:
        raise StageError("stage_deadline", 72)


def safe_path(value: str) -> tuple[str, ...]:
    """Parse a relative logical path with plain, nonempty components."""
    parts = tuple(value.split("/"))
    if not value or any(part in (".", "..") or NAME.fullmatch(part) is None for part in parts):
        raise StageError("unsafe_path")
    return parts


def run_prefix(run_id: str, generation: int) -> str:
    """Construct the checked stage prefix for exactly one run generation."""
    if re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", run_id) is None or type(generation) is not int or generation < 1:
        raise StageError("unsafe_run_prefix")
    return f"lakehouse/run_stage/{run_id}/g{generation}/"


def try_prefix(run_id: str, generation: int, try_id: str) -> str:
    """Construct a try prefix using the accepted startedBy identifier syntax."""
    if re.fullmatch(r"[A-Za-z0-9_-]{1,100}", try_id) is None:
        raise StageError("unsafe_try_id")
    return run_prefix(run_id, generation) + "tries/" + try_id + "/"


def protected_path(parts: Sequence[str]) -> bool:
    """Recognize forbidden source names before opening their contents."""
    lowered = tuple(part.casefold() for part in parts)
    return any(
        part == ".env" or part == ".aws" or part == "source_registry.json" or ".tfstate" in part or part.endswith(".tfvars.json") for part in lowered
    ) or ("lakehouse", "secrets") in zip(lowered, lowered[1:], strict=False)


def canonical(value: Any) -> bytes:
    """Serialize a JSON contract with a deterministic key order and no nonfinite numbers."""
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError):
        raise StageError("invalid_record") from None


def parse_json(raw: bytes) -> Any:
    """Reject duplicate keys and nonfinite constants at a JSON boundary."""

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise StageError("duplicate_record_key")
            result[key] = value
        return result

    def constant(value: str) -> Any:
        raise StageError("invalid_record")

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except (UnicodeDecodeError, ValueError):
        raise StageError("invalid_record") from None


@contextmanager
def held_directory(path: Path) -> Iterator[int]:
    """Open every component of a directory path without following links."""
    absolute = path.absolute()
    descriptor: int | None = None
    try:
        try:
            descriptor = os.open(absolute.anchor, DIRECTORY_FLAGS)
            for part in absolute.parts[1:]:
                if part in (".", ".."):
                    raise StageError("unsafe_source")
                following = os.open(part, DIRECTORY_FLAGS, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = following
        except OSError:
            raise StageError("unsafe_source") from None
        yield descriptor
    finally:
        if descriptor is not None:
            os.close(descriptor)


@contextmanager
def held_file(root: Path, name: str) -> Iterator[int]:
    """Hold a regular file beneath an anchored directory without following links."""
    parts = safe_path(name)
    if protected_path(parts):
        raise StageError("unsafe_source")
    with held_directory(root) as top:
        directory: int | None = None
        descriptor: int | None = None
        try:
            try:
                directory = os.dup(top)
                for part in parts[:-1]:
                    following = os.open(part, DIRECTORY_FLAGS, dir_fd=directory)
                    os.close(directory)
                    directory = following
                descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise StageError("unsafe_source")
            except OSError:
                raise StageError("unsafe_source") from None
            yield descriptor
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if directory is not None:
                os.close(directory)


def fingerprint(info: os.stat_result) -> tuple[int, int, int, int, int, int]:
    """Identify an open file and the metadata that must remain stable during transfer."""
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, stat.S_IMODE(info.st_mode)


def digest_descriptor(descriptor: int, deadline: float) -> tuple[str, int]:
    """Hash an anchored regular file under one deadline and detect concurrent changes."""
    before = os.fstat(descriptor)
    digest = hashlib.sha256()
    offset = 0
    while True:
        check_deadline(deadline)
        block = os.pread(descriptor, CHUNK, offset)
        if not block:
            break
        digest.update(block)
        offset += len(block)
    if fingerprint(before) != fingerprint(os.fstat(descriptor)) or offset != before.st_size:
        raise StageError("source_changed", 71)
    return digest.hexdigest(), offset


@dataclass(frozen=True)
class Member:
    """One archive member's logical path, whole-file digest, size and execution class."""

    path: str
    sha256: str
    size: int
    executable: bool

    def __post_init__(self) -> None:
        safe_path(self.path)
        if protected_path(safe_path(self.path)):
            raise StageError("unsafe_source")
        if not SHA256.fullmatch(self.sha256) or type(self.size) is not int or self.size < 0 or type(self.executable) is not bool:
            raise StageError("invalid_manifest")

    def document(self) -> dict[str, Any]:
        """Return the closed archive-member wire representation."""
        return asdict(self)


def inventory(root: Path, roots: Sequence[str], deadline: float, *, include_bytecode: bool = False) -> tuple[Member, ...]:
    """Inventory declared files and directories, refusing every link or protected name."""
    members: dict[str, Member] = {}
    visits = 0

    def walk(directory: int, name: str) -> None:
        nonlocal visits
        check_deadline(deadline)
        visits += 1
        if visits > MAX_MEMBERS or protected_path(safe_path(name)):
            raise StageError("unsafe_source")
        parts = safe_path(name)
        if not include_bytecode and (name.endswith(".pyc") or "__pycache__" in parts):
            return
        parent = os.dup(directory)
        try:
            for part in parts[:-1]:
                following = os.open(part, DIRECTORY_FLAGS, dir_fd=parent)
                os.close(parent)
                parent = following
            info = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child = os.open(parts[-1], DIRECTORY_FLAGS, dir_fd=parent)
                try:
                    with os.scandir(child) as entries:
                        names = sorted(entry.name for entry in entries)
                    for entry in names:
                        walk(directory, name + "/" + entry)
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode):
                with held_file(root, name) as descriptor:
                    digest, size = digest_descriptor(descriptor, deadline)
                    actual = os.fstat(descriptor)
                    if fingerprint(info) != fingerprint(actual):
                        raise StageError("source_changed", 71)
                if name in members:
                    raise StageError("duplicate_manifest")
                members[name] = Member(name, digest, size, bool(info.st_mode & 0o111))
            else:
                raise StageError("unsafe_source")
        except OSError:
            raise StageError("unsafe_source") from None
        finally:
            os.close(parent)

    with held_directory(root) as directory:
        for name in roots:
            walk(directory, name)
    return tuple(members[name] for name in sorted(members))


def checked_members(members: Sequence[Member]) -> dict[str, Member]:
    """Index a bounded manifest and reject duplicate or nested file names."""
    result = {member.path: member for member in members}
    if not result or len(result) != len(members) or len(result) > MAX_MEMBERS:
        raise StageError("invalid_manifest")
    for name in result:
        parts = safe_path(name)
        if any("/".join(parts[:index]) in result for index in range(1, len(parts))):
            raise StageError("invalid_manifest")
    return result


def code_digest(root: Path, members: Sequence[Member], deadline: float) -> str:
    """Bind logical names, execution classes, sizes and content using the generic code framing."""
    digest = hashlib.sha256()
    for name, member in sorted(checked_members(members).items()):
        with held_file(root, name) as descriptor:
            content, size = digest_descriptor(descriptor, deadline)
            executable = bool(os.fstat(descriptor).st_mode & 0o111)
        if (content, size, executable) != (member.sha256, member.size, member.executable):
            raise StageError("source_changed", 71)
        digest.update(name.encode() + b"\0" + (b"x" if executable else b"r") + b"\0" + str(size).encode() + b"\0" + content.encode() + b"\n")
    return digest.hexdigest()


def dbt_digest(root: Path, deadline: float) -> str:
    """Reproduce the existing project-and-packages digest using raw content digests."""
    digest = hashlib.sha256()
    for label in ("project", "packages"):
        for member in inventory(root, (label,), deadline, include_bytecode=True):
            digest.update(member.path.encode() + b"\0" + bytes.fromhex(member.sha256))
    return digest.hexdigest()


def pack_archive(root: Path, members: Sequence[Member], destination: Path, deadline: float) -> None:
    """Create a deterministic archive from a previously frozen, rechecked manifest."""
    code_digest(root, members, deadline)
    with destination.open("xb") as output, tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, member in sorted(checked_members(members).items()):
            check_deadline(deadline)
            with held_file(root, name) as descriptor, os.fdopen(os.dup(descriptor), "rb") as reader:
                before = fingerprint(os.fstat(descriptor))
                entry = tarfile.TarInfo(name)
                entry.size = member.size
                entry.mode = 0o555 if member.executable else 0o444
                archive.addfile(entry, reader)
                digest, size = digest_descriptor(descriptor, deadline)
                if before != fingerprint(os.fstat(descriptor)) or (digest, size) != (member.sha256, member.size):
                    raise StageError("source_changed", 71)
    code_digest(root, members, deadline)


def extract_archive(source: Path, members: Sequence[Member], destination: Path, deadline: float, maximum_bytes: int = 2 << 30) -> None:
    """Extract only exact regular manifest members into a fresh, empty directory."""
    expected = checked_members(members)
    if sum(member.size for member in members) > maximum_bytes:
        raise StageError("archive_size")
    parents = {"/".join(safe_path(name)[:index]) for name in expected for index in range(1, len(safe_path(name)))}
    seen: set[str] = set()
    try:
        destination.mkdir(mode=0o700)
        with (
            held_file(source.parent, source.name) as descriptor,
            os.fdopen(os.dup(descriptor), "rb") as reader,
            tarfile.open(fileobj=reader, mode="r:") as archive,
        ):
            for entry in archive:
                check_deadline(deadline)
                name = entry.name.removesuffix("/") if entry.isdir() else entry.name
                try:
                    safe_path(name)
                except StageError:
                    raise StageError("unsafe_archive") from None
                if name in seen or len(seen) >= len(expected) + len(parents) or not (entry.isfile() or entry.isdir()):
                    raise StageError("unsafe_archive")
                tarfile.data_filter(entry, str(destination))
                seen.add(name)
                if entry.isdir():
                    if name not in parents:
                        raise StageError("unsafe_archive")
                    (destination / name).mkdir(mode=0o700, parents=True, exist_ok=True)
                    continue
                member = expected.get(name)
                if member is None or entry.size != member.size or bool(entry.mode & 0o111) != member.executable:
                    raise StageError("unsafe_archive")
                target = destination / name
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                body = archive.extractfile(entry)
                if body is None:
                    raise StageError("unsafe_archive")
                digest = hashlib.sha256()
                remaining = member.size
                with body, target.open("xb") as writer:
                    while remaining:
                        check_deadline(deadline)
                        block = body.read(min(CHUNK, remaining))
                        if not block:
                            raise StageError("unsafe_archive")
                        writer.write(block)
                        digest.update(block)
                        remaining -= len(block)
                if digest.hexdigest() != member.sha256:
                    raise StageError("archive_digest", 71)
                target.chmod(0o555 if member.executable else 0o444)
    except OSError as error:
        if error.errno in (errno.ENOSPC, errno.EDQUOT):
            raise StageError("workspace_full", 72) from None
        raise StageError("unsafe_archive") from None
    except tarfile.TarError:
        raise StageError("unsafe_archive") from None
    if set(expected) != seen - parents:
        raise StageError("archive_members")


def make_read_only(root: Path, deadline: float) -> None:
    """Remove write permission after the complete tree passed verification."""
    for folder, directories, files in os.walk(root, followlinks=False):
        check_deadline(deadline)
        for name in (*directories, *files):
            path = Path(folder) / name
            info = path.lstat()
            if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                raise StageError("unsafe_source")
            path.chmod(0o555 if stat.S_ISDIR(info.st_mode) or info.st_mode & 0o111 else 0o444)
    root.chmod(0o555)
