"""Verify write-once step outputs against their recorded code identity."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any

SHA256 = re.compile(r"[a-f0-9]{64}")


def file_sha256(path: Path) -> str:
    """Hash a regular file without accepting a linked path."""
    if path.is_symlink() or not path.is_file():
        raise ValueError("step output must be a regular file without a link")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()


def output_path(folder: Path, name: str) -> Path:
    """Resolve one relative output name without links or traversal."""
    relative = PurePosixPath(name)
    if relative.is_absolute() or not relative.parts or any(part in (".", "..") for part in relative.parts) or relative.as_posix() != name:
        raise ValueError("step output name is not a canonical relative path")
    path = folder / name
    if any(parent.is_symlink() for parent in (folder, path, *path.parents) if parent == folder or folder in parent.parents):
        raise ValueError("step output has a linked parent")
    return path


def verify_done(folder: Path, step: str, code_digest: str) -> dict[str, Any]:
    """Require a completed step whose complete output set still matches its code and bytes."""
    path = folder / f"{step}.done.json"
    file_sha256(path)
    record = json.loads(path.read_bytes())
    expected = {"schema_version", "step", "code_digest", "outputs"}
    if not isinstance(record, dict) or set(record) != expected or record.get("schema_version") != 1 or record.get("step") != step:
        raise ValueError("step completion record has another step or schema")
    if record["code_digest"] != code_digest or not SHA256.fullmatch(code_digest):
        raise ValueError("step completion code digest differs from the run snapshot")
    outputs = record["outputs"]
    if not isinstance(outputs, dict) or not outputs:
        raise ValueError("step completion must name every output")
    actual = {path.relative_to(folder).as_posix() for path in folder.rglob("*") if not path.is_dir() and path != folder / f"{step}.done.json"}
    if actual != set(outputs):
        raise ValueError("step completion does not name the exact output set")
    for name, digest in outputs.items():
        if not isinstance(name, str) or not isinstance(digest, str) or not SHA256.fullmatch(digest) or file_sha256(output_path(folder, name)) != digest:
            raise ValueError("step output differs from its completion record")
    return record


def write_done(folder: Path, step: str, code_digest: str, names: Iterable[str]) -> dict[str, Any]:
    """Synchronously write the completion mark only after every named output exists and hashes."""
    if not SHA256.fullmatch(code_digest):
        raise ValueError("step completion requires the snapshot code digest")
    record = {
        "schema_version": 1,
        "step": step,
        "code_digest": code_digest,
        "outputs": {name: file_sha256(output_path(folder, name)) for name in sorted(names)},
    }
    final = folder / f"{step}.done.json"
    if final.exists() or final.is_symlink():
        previous = verify_done(folder, step, code_digest)
        if previous != record:
            raise ValueError("completed step cannot be replaced by other outputs")
        return previous
    temporary = folder / f".{step}.done.partial"
    if temporary.is_symlink():
        raise ValueError("step completion temporary path is linked")
    with temporary.open("w") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, final)
    descriptor = os.open(folder, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return verify_done(folder, step, code_digest)
