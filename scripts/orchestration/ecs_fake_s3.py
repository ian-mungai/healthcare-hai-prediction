"""Deterministic in-memory staging API model for synthetic contract preparation.

The harness supplies trusted principals and explicit time. This model implements
no HTTP, authentication, IAM interpreter, AWS client or grant-delivery algorithm.
"""

from __future__ import annotations

import base64
import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal


class S3Refusal(RuntimeError):
    """A sanitized model refusal containing only its stable code and HTTP status."""

    def __init__(self, code: str, status: int) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


@dataclass(frozen=True)
class Principal:
    """A harness-supplied role, identity, optional writer scope and expiry."""

    role: Literal["project", "task", "writer"]
    user_id: str
    session_prefix: str | None = None
    expires_at: float | None = None


@dataclass(frozen=True)
class ObjectHead:
    """Object identity and client metadata without object content."""

    version_id: str
    size: int
    metadata: Mapping[str, str]


@dataclass(frozen=True)
class _Version:
    version_id: str
    body: bytes | None
    metadata: dict[str, str]


@dataclass
class _Upload:
    key: str
    metadata: dict[str, str]
    parts: dict[int, tuple[bytes, str]] = field(default_factory=dict)
    conflict: bool = False
    usable: bool = True


class FakeS3:
    """Model immutable stage writes, explicit role scopes and versioned multipart state.

    Parameters
    ----------
    prefix : str
        One canonical synthetic run and generation prefix. Every operation stays
        within that prefix. Source-bucket provider tests require another adapter.
    """

    def __init__(self, prefix: str) -> None:
        if not re.fullmatch(r"lakehouse/run_stage/[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}/g[1-9][0-9]*/", prefix):
            raise ValueError("the model needs a canonical run and generation prefix")
        self.prefix = prefix
        self._objects: dict[str, list[_Version]] = {}
        self._uploads: dict[str, _Upload] = {}
        self._sequence = 0

    @staticmethod
    def checksum(body: bytes) -> str:
        """Return the S3-shaped base64 SHA-256 checksum of synthetic bytes."""
        return base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")

    def _key(self, key: str) -> None:
        if not key.startswith(self.prefix) or not re.fullmatch(r"[A-Za-z0-9_.:-]+(?:/[A-Za-z0-9_.:-]+)*", key):
            raise S3Refusal("AccessDenied", 403)
        if any(part in (".", "..") for part in key.split("/")):
            raise S3Refusal("AccessDenied", 403)

    def _task_allowed(self, principal: Principal, key: str, action: str) -> bool:
        if action != "read":
            return False
        if key.startswith((self.prefix + "inputs/", self.prefix + "tries/")):
            return True
        return key == self.prefix + "grants/" + principal.user_id + ".json"

    def _writer_allowed(self, principal: Principal, key: str, action: str) -> bool:
        if action not in ("write", "multipart") or principal.session_prefix is None or not key.startswith(principal.session_prefix):
            return False
        role_prefix = self.prefix + "tries/"
        if not key.startswith(role_prefix):
            return False
        parts = key[len(role_prefix) :].split("/", 1)
        return len(parts) == 2 and (parts[1] == "outputs.json" or parts[1].startswith("out/"))

    def _authorize(self, principal: Principal, key: str, action: str, now: float) -> None:
        self._key(key)
        if principal.role == "project":
            return
        if principal.role == "task" and self._task_allowed(principal, key, action):
            return
        if principal.role == "writer":
            if principal.expires_at is None or now >= principal.expires_at:
                raise S3Refusal("ExpiredToken", 403)
            if self._writer_allowed(principal, key, action):
                return
        raise S3Refusal("AccessDenied", 403)

    def _conditional(self, key: str, if_none_match: str | None) -> None:
        if if_none_match != "*":
            raise S3Refusal("AccessDenied", 403)
        if key in self._objects and self._objects[key][-1].body is not None:
            raise S3Refusal("PreconditionFailed", 412)

    def _append(self, key: str, body: bytes | None, metadata: Mapping[str, str]) -> ObjectHead:
        self._sequence += 1
        version = _Version(f"version_{self._sequence:08d}", body, dict(metadata))
        self._objects.setdefault(key, []).append(version)
        return self._head(version)

    @staticmethod
    def _head(version: _Version) -> ObjectHead:
        return ObjectHead(version.version_id, len(version.body or b""), dict(version.metadata))

    def seed(self, key: str, body: bytes, metadata: Mapping[str, str]) -> ObjectHead:
        """Plant a synthetic version through trusted harness setup, outside the client API."""
        self._key(key)
        return self._append(key, body, metadata)

    def put(self, principal: Principal, key: str, body: bytes, if_none_match: str | None, metadata: Mapping[str, str], *, now: float = 0.0) -> ObjectHead:
        """Write one conditional version after checking role and session scope."""
        self._authorize(principal, key, "write", now)
        self._conditional(key, if_none_match)
        return self._append(key, body, metadata)

    def _find_version(self, key: str, version_id: str | None) -> _Version:
        versions = self._objects.get(key, [])
        if version_id is None:
            if not versions or versions[-1].body is None:
                raise S3Refusal("NoSuchKey", 404)
            return versions[-1]
        for version in versions:
            if version.version_id == version_id:
                return version
        raise S3Refusal("NoSuchVersion", 404)

    def head(self, principal: Principal, key: str, version_id: str | None = None, *, now: float = 0.0) -> ObjectHead:
        """Read explicit version metadata, including retained deletion markers."""
        self._authorize(principal, key, "read", now)
        try:
            return self._head(self._find_version(key, version_id))
        except S3Refusal as error:
            if principal.role == "task" and error.status == 404:
                raise S3Refusal("AccessDenied", 403) from None
            raise

    def get(self, principal: Principal, key: str, version_id: str | None = None, *, now: float = 0.0) -> bytes:
        """Read synthetic bytes at the requested immutable version."""
        self.head(principal, key, version_id, now=now)
        result = self._find_version(key, version_id)
        if result.body is None:
            raise S3Refusal("NoSuchKey", 404)
        return result.body

    def versions(self, principal: Principal, key: str) -> list[ObjectHead]:
        """List retained synthetic versions through the project's harness identity."""
        self._authorize(principal, key, "list", 0.0)
        return [self._head(version) for version in self._objects.get(key, [])]

    def delete_current(self, principal: Principal, key: str) -> None:
        """Append a current deletion marker without removing prior versions."""
        self._authorize(principal, key, "delete", 0.0)
        self._append(key, None, {})

    def create_upload(self, principal: Principal, key: str, metadata: Mapping[str, str], *, now: float = 0.0) -> str:
        """Create an empty multipart upload without applying the completion condition."""
        self._authorize(principal, key, "multipart", now)
        self._sequence += 1
        upload_id = f"upload_{self._sequence:08d}"
        self._uploads[upload_id] = _Upload(key, dict(metadata))
        return upload_id

    def _upload(self, principal: Principal, upload_id: str, now: float) -> _Upload:
        if upload_id not in self._uploads:
            raise S3Refusal("NoSuchUpload", 404)
        upload = self._uploads[upload_id]
        self._authorize(principal, upload.key, "multipart", now)
        return upload

    def _verify_checksum(self, body: bytes, checksum: str) -> None:
        if self.checksum(body) != checksum:
            raise S3Refusal("BadDigest", 400)

    def upload_part(self, principal: Principal, upload_id: str, number: int, body: bytes, checksum: str, *, now: float = 0.0) -> str:
        """Store only a correctly checksummed numbered part of a usable upload."""
        upload = self._upload(principal, upload_id, now)
        if not upload.usable:
            raise S3Refusal("NoSuchUpload", 404)
        if not 1 <= number <= 10000:
            raise S3Refusal("InvalidPart", 400)
        self._verify_checksum(body, checksum)
        upload.parts[number] = (body, checksum)
        return checksum

    def list_parts(self, principal: Principal, upload_id: str, *, now: float = 0.0) -> list[tuple[int, str]]:
        """List the checksums the synthetic API actually stored for an upload."""
        upload = self._upload(principal, upload_id, now)
        return [(number, checksum) for number, (_, checksum) in sorted(upload.parts.items())]

    def conflict_on_complete(self, upload_id: str) -> None:
        """Plant one completion conflict through trusted harness setup."""
        self._uploads[upload_id].conflict = True

    def _completion_fault(self, upload: _Upload) -> None:
        if upload.conflict:
            upload.usable = False
            raise S3Refusal("ConditionalRequestConflict", 409)

    def complete(self, principal: Principal, upload_id: str, parts: Sequence[tuple[int, str]], if_none_match: str | None, *, now: float = 0.0) -> ObjectHead:
        """Commit submitted verified parts, or preserve incomplete state on refusal."""
        upload = self._upload(principal, upload_id, now)
        if not upload.usable:
            raise S3Refusal("NoSuchUpload", 404)
        self._conditional(upload.key, if_none_match)
        self._completion_fault(upload)
        numbers = [number for number, _ in parts]
        if not numbers or len(numbers) > 10000 or numbers != sorted(set(numbers)):
            raise S3Refusal("InvalidPartOrder", 400)
        bodies = []
        for number, checksum in parts:
            if number not in upload.parts or upload.parts[number][1] != checksum:
                raise S3Refusal("InvalidPart", 400)
            bodies.append(upload.parts[number][0])
        if any(len(body) < 5 << 20 for body in bodies[:-1]):
            raise S3Refusal("EntityTooSmall", 400)
        head = self._append(upload.key, b"".join(bodies), upload.metadata)
        del self._uploads[upload_id]
        return head

    def abort(self, principal: Principal, upload_id: str, *, now: float = 0.0) -> None:
        """Remove the named incomplete upload after checking its actual key scope."""
        self._upload(principal, upload_id, now)
        del self._uploads[upload_id]

    def list_uploads(self, principal: Principal, prefix: str, cursor: str | None = None, page_size: int = 100) -> tuple[list[str], str | None]:
        """List a checked prefix with deterministic pages and a harness continuation cursor."""
        if principal.role != "project" or not prefix.endswith("/"):
            raise S3Refusal("AccessDenied", 403)
        self._key(prefix + "listing")
        if not 1 <= page_size <= 100:
            raise ValueError("the model page size must be between 1 and 100")
        matching = sorted(
            identifier for identifier, upload in self._uploads.items() if upload.key.startswith(prefix) and (cursor is None or identifier > cursor)
        )
        page = matching[:page_size]
        return page, page[-1] if len(matching) > page_size else None
