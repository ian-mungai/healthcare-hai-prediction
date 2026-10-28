"""Hash one validated immutable build and write its declared seal evidence."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any

from services.hai.bootstrap.integrity import (
    MAX_RECORD,
    SHA256,
    StageError,
    canonical,
    check_deadline,
    digest_descriptor,
    held_directory,
    held_file,
    parse_json,
    run_prefix,
)
from services.hai.bootstrap.outputs import Declaration, Output
from services.hai.bootstrap.transfer import FileRef

REQUEST_KEYS = {"run_id", "layer", "generation", "validation_attempt", "sha256", "source"}


def declaration() -> Declaration:
    """Return the seal role's complete result contract, excluding wrapper-owned logs."""
    return Declaration((Output("seal.json", "controller_record", MAX_RECORD),), (), (), 0)


def evidence(request: Mapping[str, Any]) -> tuple[FileRef, dict[str, Any]]:
    """Validate a controller-pinned seal request and derive its exact evidence fields."""
    if (
        set(request) != REQUEST_KEYS
        or request.get("layer") not in ("silver", "gold")
        or not isinstance(request.get("sha256"), str)
        or SHA256.fullmatch(request["sha256"]) is None
        or not isinstance(request.get("validation_attempt"), str)
        or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", request["validation_attempt"]) is None
        or not isinstance(request.get("source"), dict)
    ):
        raise StageError("seal_binding", 71)
    try:
        prefix = run_prefix(request["run_id"], request["generation"])
        source = FileRef(**request["source"])
    except (TypeError, KeyError, AttributeError):
        raise StageError("seal_binding", 71) from None
    if source.path != "staging.duckdb" or source.sha256 != request["sha256"] or not source.key.startswith(prefix + "tries/"):
        raise StageError("seal_binding", 71)
    return source, {
        "run_id": request["run_id"],
        "layer": request["layer"],
        "generation": request["generation"],
        "validation_attempt": request["validation_attempt"],
        "sha256": source.sha256,
        "size": source.size,
        "source_key": source.key,
        "source_version_id": source.version_id,
    }


def _existing(root: Path, raw: bytes, deadline: float) -> None:
    with held_file(root, "seal.json") as descriptor:
        digest, size = digest_descriptor(descriptor, deadline)
    if size != len(raw) or digest != hashlib.sha256(raw).hexdigest():
        raise StageError("seal_conflict", 71)


def seal(inputs: Path, outputs: Path, request: Mapping[str, Any], deadline: float) -> dict[str, Any]:
    """Stream the validated input and publish identical, write-once seal evidence.

    The verified wrapper downloads the source by version before this operation.
    The launcher separately authenticates the returned evidence and owns layer
    completion. This operation never copies the database or writes a completion.
    """
    check_deadline(deadline)
    source, record = evidence(request)
    wal = inputs / (source.path + ".wal")
    if wal.exists() or wal.is_symlink():
        raise StageError("wal_present", 73)
    with held_file(inputs, source.path) as descriptor:
        digest, size = digest_descriptor(descriptor, deadline)
    if (digest, size) != (request["sha256"], source.size):
        raise StageError("seal_digest", 71)
    raw = canonical(record) + b"\n"
    with held_directory(outputs) as directory:
        check_deadline(deadline)
        if (outputs / "seal.json").exists() or (outputs / "seal.json").is_symlink():
            _existing(outputs, raw, deadline)
            return record
        handle, temporary = tempfile.mkstemp(prefix=".seal_", suffix=".partial", dir=outputs)
        try:
            with os.fdopen(handle, "wb") as writer:
                writer.write(raw)
                writer.flush()
                os.fsync(writer.fileno())
            check_deadline(deadline)
            try:
                os.link(temporary, "seal.json", dst_dir_fd=directory, follow_symlinks=False)
            except FileExistsError:
                _existing(outputs, raw, deadline)
            os.fsync(directory)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary)
    return record


def main(argv: Sequence[str] | None = None) -> int:
    """Read a pinned request, hash its staged input and emit an independent evidence receipt."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, default=Path("/workspace/inputs"))
    parser.add_argument("--out", type=Path, default=Path("/workspace/out"))
    parser.add_argument("--timeout-seconds", type=float, required=True)
    args = parser.parse_args(argv)
    deadline = time.monotonic() + args.timeout_seconds
    try:
        check_deadline(deadline)
        with held_file(args.request.parent, args.request.name) as descriptor:
            raw = os.pread(descriptor, MAX_RECORD + 1, 0)
        if len(raw) > MAX_RECORD:
            raise StageError("record_bound")
        request = parse_json(raw)
        if not isinstance(request, dict):
            raise StageError("seal_binding", 71)
        seal(args.inputs, args.out, request, deadline)
        with held_file(args.out, "seal.json") as descriptor:
            digest, _ = digest_descriptor(descriptor, deadline)
        sys.stdout.write(canonical({"seal_sha256": digest}).decode() + "\n")
    except StageError as error:
        sys.stderr.write(canonical({"error": error.code}).decode() + "\n")
        return error.exit_code
    except OSError:
        sys.stderr.write('{"error":"seal_io"}\n')
        return 71
    return 0


if __name__ == "__main__":
    sys.exit(main())
