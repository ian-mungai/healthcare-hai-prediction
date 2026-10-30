"""Where the active deployment of each HAI type lives, and the stores that keep it.

``active/<HAI type>.json`` is the record the service reads. ``history/<HAI type>/<deployment id>.json`` is written once per deployment before the
active record moves, so a deploy that stops between the two writes finishes on its retry. The active record is replaced only when its current token is
the one the writer read (a compare and swap). The file store serves the local lane and the cases. The S3 store is the AWS one and runs only in the
serving image.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


class ManifestConflict(Exception):
    """A write that would replace a record the writer did not read, or change a history record."""


@dataclass(frozen=True)
class Stored:
    token: str
    body: bytes


class ManifestStore(Protocol):
    def read_active(self, hai_type: str) -> Stored | None: ...

    def read_history(self, hai_type: str, deployment_id: str) -> bytes | None: ...

    def put_history(self, hai_type: str, deployment_id: str, body: bytes) -> None: ...

    def replace_active(self, hai_type: str, body: bytes, expected_token: str | None) -> None: ...


def token_of(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class FileManifestStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.writes = 0
        root.mkdir(parents=True, exist_ok=True)

    def _write(self, path: Path, body: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, name = tempfile.mkstemp(dir=path.parent, prefix=".tmp_")
        with os.fdopen(handle, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        self.writes += 1

    def read_active(self, hai_type: str) -> Stored | None:
        path = self.root / "active" / f"{hai_type}.json"
        if not path.exists():
            return None
        body = path.read_bytes()
        return Stored(token_of(body), body)

    def read_history(self, hai_type: str, deployment_id: str) -> bytes | None:
        path = self.root / "history" / hai_type / f"{deployment_id}.json"
        return path.read_bytes() if path.exists() else None

    def put_history(self, hai_type: str, deployment_id: str, body: bytes) -> None:
        path = self.root / "history" / hai_type / f"{deployment_id}.json"
        if path.exists():
            if path.read_bytes() != body:
                raise ManifestConflict("the history record of this deployment holds other content")
            return
        self._write(path, body)

    def replace_active(self, hai_type: str, body: bytes, expected_token: str | None) -> None:
        (self.root / "active").mkdir(parents=True, exist_ok=True)
        with (self.root / "active" / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            current = self.read_active(hai_type)
            if (current.token if current else None) != expected_token:
                raise ManifestConflict("the active deployment changed since it was read")
            self._write(self.root / "active" / f"{hai_type}.json", body)


class S3ManifestStore:
    """The AWS store: conditional writes by ETag. Not run offline. The check before a replace leaves a short window, closed by the DAG's single active run."""

    def __init__(self, client: Any, bucket: str, prefix: str) -> None:
        self.client, self.bucket, self.prefix = client, bucket, prefix.strip("/")

    def _key(self, *parts: str) -> str:
        return "/".join([self.prefix, *parts])

    def _get(self, key: str) -> tuple[str, bytes] | None:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except self.client.exceptions.NoSuchKey:
            return None
        return str(response["ETag"]), bytes(response["Body"].read())

    def read_active(self, hai_type: str) -> Stored | None:
        found = self._get(self._key("active", f"{hai_type}.json"))
        return None if found is None else Stored(found[0], found[1])

    def read_history(self, hai_type: str, deployment_id: str) -> bytes | None:
        found = self._get(self._key("history", hai_type, f"{deployment_id}.json"))
        return None if found is None else found[1]

    def put_history(self, hai_type: str, deployment_id: str, body: bytes) -> None:
        key = self._key("history", hai_type, f"{deployment_id}.json")
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body, IfNoneMatch="*", ContentType="application/json")
        except self.client.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") not in ("PreconditionFailed", "ConditionalRequestConflict"):
                raise
            existing = self._get(key)
            if existing is None or existing[1] != body:
                raise ManifestConflict("the history record of this deployment holds other content") from None

    def replace_active(self, hai_type: str, body: bytes, expected_token: str | None) -> None:
        key = self._key("active", f"{hai_type}.json")
        condition = {"IfNoneMatch": "*"} if expected_token is None else {"IfMatch": expected_token}
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json", **condition)
        except self.client.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") in ("PreconditionFailed", "ConditionalRequestConflict"):
                raise ManifestConflict("the active deployment changed since it was read") from None
            raise
