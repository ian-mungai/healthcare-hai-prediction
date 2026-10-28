"""Confirm an immutable first-dispatch latch through isolated SDK calls outside registry transactions."""

from __future__ import annotations

import hashlib
import math
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Protocol

from scripts.lakehouse.catalog_journal import encoded, identifier
from services.hai.bootstrap.integrity import controller_binding


def latch_key(transfer_id: str) -> str:
    """Return the immutable first-dispatch object key for the validated transfer identity."""
    return f"lakehouse/catalog_migration/controller_transfers/{identifier(transfer_id)}/dispatch_started.json"


class ObjectClient(Protocol):
    """The existing isolated, bounded SDK transport for small conditional objects."""

    def call(self, operation: str, params: Mapping[str, Any], *, deadline: float) -> dict[str, Any]:
        """Return a closed SDK result within the absolute operation deadline."""
        ...

    def close(self, *, deadline: float) -> dict[str, Any]:
        """Confirm owned workers stopped before returning."""
        ...


class LatchError(RuntimeError):
    """An unconfirmed or conflicting latch keeps dispatch and simple rollback closed."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class DispatchLatch:
    """Create one latch per transfer and require exact readback, including after a lost write response."""

    def __init__(self, client: ObjectClient, bucket: str, *, seconds: float = 20.0) -> None:
        if not bucket or not math.isfinite(seconds) or not 0 < seconds <= 30:
            raise ValueError("controller_latch_config")
        self.client, self.bucket, self.seconds = client, bucket, seconds
        self.closing = threading.Event()
        self.condition = threading.Condition()
        self.active = 0
        self.closed = False

    @contextmanager
    def _operation(self) -> Iterator[None]:
        with self.condition:
            if self.closing.is_set():
                raise LatchError("controller_latch_unconfirmed")
            self.active += 1
        try:
            yield
        finally:
            with self.condition:
                self.active -= 1
                self.condition.notify_all()

    def _get(self, key: str, deadline: float) -> dict[str, Any] | None:
        if self.closing.is_set() or time.monotonic() >= deadline:
            raise LatchError("controller_latch_unconfirmed")
        reply = self.client.call("s3_get_small", {"bucket": self.bucket, "key": key}, deadline=deadline)
        if self.closing.is_set() or time.monotonic() >= deadline:
            raise LatchError("controller_latch_unconfirmed")
        if reply.get("ok") is True:
            return dict(reply["result"])
        if reply.get("error", {}).get("code") in ("NoSuchKey", "NotFound"):
            return None
        raise LatchError("controller_latch_unconfirmed")

    def confirm(self, binding: Mapping[str, Any], deadline: float) -> dict[str, Any]:
        """Confirm exact immutable bytes under one deadline. Every possible write is reconciled by readback."""
        with self._operation():
            return self._confirm(binding, deadline)

    def _confirm(self, binding: Mapping[str, Any], deadline: float) -> dict[str, Any]:
        binding = controller_binding(dict(binding))
        if binding["controller_owner"] != "aws":
            raise LatchError("controller_latch_owner")
        key = latch_key(binding["controller_transfer_id"])
        raw = encoded({"version": 1, **binding})
        deadline = min(deadline, time.monotonic() + self.seconds)
        if self.closing.is_set() or time.monotonic() >= deadline:
            raise LatchError("controller_latch_unconfirmed")
        self.client.call(
            "s3_put_small",
            {"bucket": self.bucket, "key": key, "body_utf8": raw.decode(), "sha256": hashlib.sha256(raw).hexdigest(), "if_none_match": "*"},
            deadline=deadline,
        )
        observed = self._get(key, deadline)
        if observed is None:
            raise LatchError("controller_latch_unconfirmed")
        if observed.get("body_utf8") != raw.decode() or observed.get("sha256") != hashlib.sha256(raw).hexdigest() or observed.get("size") != len(raw):
            raise LatchError("controller_latch_conflict")
        if not isinstance(observed.get("version_id"), str) or not observed["version_id"] or time.monotonic() >= deadline:
            raise LatchError("controller_latch_unconfirmed")
        return {"binding": dict(binding), "key": key, "version_id": observed["version_id"], "sha256": observed["sha256"]}

    def absence(self, transfer_id: str, deadline: float) -> dict[str, Any]:
        """Confirm absence through the transfer key prefix while admission stays closed."""
        key = latch_key(transfer_id)
        with self._operation():
            deadline = min(deadline, time.monotonic() + self.seconds)
            if self.closing.is_set() or time.monotonic() >= deadline:
                raise LatchError("controller_latch_unconfirmed")
            reply = self.client.call("s3_list", {"bucket": self.bucket, "prefix": key}, deadline=deadline)
            if reply.get("ok") is not True or self.closing.is_set() or time.monotonic() >= deadline:
                raise LatchError("controller_latch_unconfirmed")
            result = reply["result"]
            if not isinstance(result.get("objects"), list) or result.get("next_token"):
                raise LatchError("controller_latch_unconfirmed")
            if result["objects"]:
                raise LatchError("controller_latch_exists")
            return {"latch_absent": True}

    def close(self, *, deadline: float) -> None:
        """Stop this latch transport's owned SDK workers within the controller stop budget."""
        if self.closed:
            return
        self.closing.set()
        confirmed = self.client.close(deadline=deadline).get("confirmed")
        with self.condition:
            while self.active and time.monotonic() < deadline:
                self.condition.wait(timeout=max(0, deadline - time.monotonic()))
        if not confirmed or self.active:
            raise TimeoutError("controller_latch_cleanup_unconfirmed")
        self.closed = True
