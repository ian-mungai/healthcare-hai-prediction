"""Transfer immutable files through an injected, deadline-enforcing storage transport."""

from __future__ import annotations

import base64
import hashlib
import os
import tempfile
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from .integrity import SHA256, StageError, check_deadline, digest_descriptor, fingerprint, held_file, safe_path

PART_BYTES = 8 << 20
PUT_BYTES = 5 << 20


class StorageError(RuntimeError):
    """A sanitized transport error, never an SDK response body."""

    def __init__(self, code: str, status: int) -> None:
        super().__init__("storage_call_failed")
        self.code = code
        self.status = status


class Storage(Protocol):
    """A storage adapter whose calls and streamed file bodies obey the supplied deadline.

    File bodies travel through the named file slice or destination, never a reply
    frame. A production adapter must use owned workers rather than blocking SDK
    calls in the controller. This interface grants no identity or credential scope.
    """

    def invoke(self, operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]: ...


@dataclass(frozen=True)
class FileRef:
    """An immutable logical file reference with the exact storage version and content."""

    path: str
    key: str
    version_id: str
    sha256: str
    size: int

    def __post_init__(self) -> None:
        safe_path(self.path)
        safe_path(self.key)
        if not self.version_id or self.version_id.casefold() == "null" or not SHA256.fullmatch(self.sha256):
            raise StageError("invalid_reference")
        if type(self.size) is not int or self.size < 0:
            raise StageError("invalid_reference")

    def document(self) -> dict[str, Any]:
        """Return the canonical input-map entry."""
        return asdict(self)


def object_identity(reply: Mapping[str, Any], size: int, sha256: str) -> str:
    """Check a storage version, byte count and whole-file digest metadata."""
    version = reply.get("version_id")
    if not isinstance(version, str) or not version or version.casefold() == "null":
        raise StageError("unversioned_object", 71)
    metadata = reply.get("metadata")
    if not isinstance(metadata, Mapping) or reply.get("size") != size or metadata.get("sha256") != sha256:
        raise StageError("stage_conflict", 71)
    return version


class Transfer:
    """Write once, restart multipart conflicts and verify downloaded bytes.

    The writer and reader can use distinct scoped identities. Abort failures stay
    in ``pending_aborts`` for the owning release sweep. The caller retains that
    ledger until its bounded cleanup confirms the uploads are absent.
    """

    def __init__(self, storage: Storage, reader: Storage | None = None, measured_bytes_per_second: float = 1 << 20) -> None:
        if measured_bytes_per_second <= 0:
            raise ValueError("the transfer rate must be positive")
        self.storage = storage
        self.reader = storage if reader is None else reader
        self.rate = measured_bytes_per_second
        self.pending_aborts: list[dict[str, str]] = []

    @staticmethod
    def _call(storage: Storage, operation: str, request: dict[str, Any], deadline: float, cap: float = 20.0) -> dict[str, Any]:
        check_deadline(deadline)
        call_deadline = min(deadline, time.monotonic() + cap)
        result = storage.invoke(operation, request, call_deadline)
        check_deadline(call_deadline)
        return result

    def verify_reference(self, reference: FileRef, deadline: float) -> None:
        """Check an exact version's size and digest metadata without downloading it."""
        reply = self._call(self.reader, "head", {"key": reference.key, "version_id": reference.version_id}, deadline)
        if object_identity(reply, reference.size, reference.sha256) != reference.version_id:
            raise StageError("stage_conflict", 71)

    def _written(self, reply: Mapping[str, Any], key: str, size: int, digest: str, deadline: float) -> str:
        version = reply.get("version_id")
        if not isinstance(version, str) or not version or version.casefold() == "null":
            raise StageError("unversioned_object", 71)
        try:
            head = self._call(self.reader, "head", {"key": key, "version_id": version}, deadline)
        except StorageError:
            raise StageError("stage_head_failed", 72) from None
        if head.get("version_id") != version:
            raise StageError("stage_conflict", 71)
        return object_identity(head, size, digest)

    def _existing(self, key: str, size: int, digest: str, deadline: float) -> str | None:
        try:
            reply = self._call(self.reader, "head", {"key": key}, deadline)
        except StorageError as error:
            if error.status in (403, 404):
                return None
            raise StageError("stage_head_failed", 72) from None
        return object_identity(reply, size, digest)

    def _abort(self, key: str, identifier: str, deadline: float) -> None:
        try:
            self._call(self.storage, "abort", {"key": key, "upload_id": identifier}, deadline, 10.0)
        except (StorageError, StageError):
            self.pending_aborts.append({"key": key, "upload_id": identifier})

    def _parts(self, path: Path, descriptor: int, key: str, identifier: str, size: int, deadline: float) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        offset = 0
        while offset < size:
            check_deadline(deadline)
            length = min(PART_BYTES, size - offset)
            chunk = os.pread(descriptor, length, offset)
            if len(chunk) != length:
                raise StageError("source_changed", 71)
            checksum = base64.b64encode(hashlib.sha256(chunk).digest()).decode("ascii")
            request = {
                "key": key,
                "upload_id": identifier,
                "number": len(result) + 1,
                "source": str(path),
                "offset": offset,
                "length": length,
                "checksum": checksum,
            }
            reply = self._call(self.storage, "part", request, deadline, 40.0)
            if reply.get("checksum") != checksum or not isinstance(reply.get("etag"), str) or not reply["etag"]:
                raise StageError("part_checksum", 71)
            result.append({"number": len(result) + 1, "checksum": checksum, "etag": reply["etag"]})
            offset += length
        return result

    def _multipart(self, path: Path, descriptor: int, key: str, size: int, digest: str, deadline: float) -> str:
        for restart in range(3):
            if restart and deadline - time.monotonic() <= size / self.rate * 1.2 + 30:
                raise StageError("multipart_budget", 72)
            try:
                reply = self._call(self.storage, "create", {"key": key, "metadata": {"sha256": digest}, "checksum_algorithm": "SHA256"}, deadline)
            except StorageError:
                raise StageError("stage_upload_failed", 72) from None
            identifier = reply.get("upload_id")
            if not isinstance(identifier, str) or not identifier:
                raise StageError("invalid_upload_identity", 72)
            completed = False
            try:
                parts = self._parts(path, descriptor, key, identifier, size, deadline)
                current_digest, current_size = digest_descriptor(descriptor, deadline)
                if (current_digest, current_size) != (digest, size):
                    raise StageError("source_changed", 71)
                request = {"key": key, "upload_id": identifier, "parts": parts, "if_none_match": "*"}
                reply = self._call(self.storage, "complete", request, deadline)
                completed = True
                return self._written(reply, key, size, digest, deadline)
            except StorageError as error:
                self._abort(key, identifier, deadline)
                completed = True
                if error.status not in (409, 412):
                    raise StageError("stage_upload_failed", 72) from None
                existing = self._existing(key, size, digest, deadline)
                if existing is not None:
                    return existing
                if error.status == 412:
                    raise StageError("stage_conflict", 71) from None
                if restart == 2:
                    raise StageError("multipart_restarts_exhausted", 72) from None
            finally:
                if not completed:
                    self._abort(key, identifier, deadline)
        raise StageError("multipart_restarts_exhausted", 72)

    def upload(self, path: Path, key: str, logical_path: str, deadline: float, *, multipart: bool = False, expected: tuple[str, int] | None = None) -> FileRef:
        """Upload a stable regular file, verifying conditional conflicts before reuse."""
        check_deadline(deadline)
        safe_path(key)
        safe_path(logical_path)
        if path.name.endswith(".duckdb") and path.with_name(path.name + ".wal").exists():
            raise StageError("wal_present", 73)
        with held_file(path.parent, path.name) as descriptor:
            initial = fingerprint(os.fstat(descriptor))
            digest, size = digest_descriptor(descriptor, deadline)
            if expected is not None and (digest, size) != expected:
                raise StageError("source_changed", 71)
            if multipart or size > PUT_BYTES:
                version = self._multipart(path, descriptor, key, size, digest, deadline)
            else:
                request = {
                    "key": key,
                    "source": str(path),
                    "offset": 0,
                    "length": size,
                    "checksum": base64.b64encode(bytes.fromhex(digest)).decode("ascii"),
                    "metadata": {"sha256": digest},
                    "if_none_match": "*",
                }
                try:
                    reply = self._call(self.storage, "put", request, deadline)
                    version = self._written(reply, key, size, digest, deadline)
                except StorageError as error:
                    if error.status != 412:
                        raise StageError("stage_upload_failed", 72) from None
                    existing = self._existing(key, size, digest, deadline)
                    if existing is None:
                        raise StageError("stage_conflict", 71) from None
                    version = existing
            final_digest, final_size = digest_descriptor(descriptor, deadline)
            with held_file(path.parent, path.name) as current:
                stable = initial == fingerprint(os.fstat(descriptor)) == fingerprint(os.fstat(current))
            if not stable or (final_digest, final_size) != (digest, size):
                raise StageError("source_changed", 71)
        return FileRef(logical_path, key, version, digest, size)

    def download(self, reference: FileRef, destination: Path, deadline: float) -> None:
        """Expose bytes only after the exact version, size and whole-file hash match."""
        check_deadline(deadline)
        if destination.exists() or destination.is_symlink():
            raise StageError("destination_exists")
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="stage_", dir=destination.parent) as temporary:
            target = Path(temporary.name)
            offset = 0
            while offset < reference.size or not offset and not reference.size:
                length = min(PART_BYTES, reference.size - offset)
                request = {
                    "key": reference.key,
                    "version_id": reference.version_id,
                    "destination": str(target),
                    "maximum_bytes": reference.size,
                    "offset": offset,
                    "length": length,
                }
                try:
                    reply = self._call(self.reader, "download", request, deadline, 40.0)
                except StorageError:
                    raise StageError("stage_download_failed", 71) from None
                if reply.get("version_id") != reference.version_id or reply.get("size") != reference.size:
                    raise StageError("input_identity", 71)
                offset += length
                if not length:
                    break
            with held_file(target.parent, target.name) as descriptor:
                digest, size = digest_descriptor(descriptor, deadline)
                if (digest, size) != (reference.sha256, reference.size):
                    raise StageError("input_digest", 71)
            os.link(target, destination, follow_symlinks=False)
