"""Opt-in durable request and byte controls for fresh redownloads; ordinary collectors stay opt-out."""

import fcntl
import hashlib
import json
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class Pause(ValueError):
    """A rate/quota limit needs a recorded source resume before another attempt."""


class BudgetExceeded(ValueError):
    """An exhausted durable attempt/byte budget cannot be reset by restarting."""


def validate_path(path: Path, root: Path) -> None:
    """Refuse traversal and symlinks before any harness-owned write."""
    path, root = path.absolute(), root.absolute()
    if ".." in path.parts or not path.is_relative_to(root):
        raise ValueError("Write outside the redownload state root")
    if any(item.is_symlink() for item in [path, *path.parents] if item.is_relative_to(root)):
        raise ValueError("Symlink in redownload staging path")


def record(path: Path, value: dict) -> None:
    """Control records are append-only, fsynced and intentionally outside their own byte accounting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True)
        handle.flush()
        import os

        os.fsync(handle.fileno())


def charged(root: Path) -> int:
    """Unsettled allocations remain fully charged after a crash."""
    total = 0
    for path in sorted((root / ".budgets").glob("*.json")):
        if path.name.endswith(".settled.json"):
            continue
        item = json.loads(path.read_text())
        settled = path.with_name(path.stem + ".settled.json")
        total += json.loads(settled.read_text())["bytes"] if settled.exists() else item["bytes"]
    return total


@dataclass
class Boundary:
    """One queue unit's persistent boundaries, active only while its handler runs."""

    root: Path
    base: Path
    identity: str
    byte_cap: int
    used: int | None = None
    sequence: int | None = field(default=None, init=False)

    def attempt(self, key: str) -> None:
        path = self.root / ".attempts" / self.identity / hashlib.sha256(key.encode()).hexdigest()
        validate_path(path, self.root)
        count = len(list(path.glob("*.json")))
        if count >= 2:
            raise BudgetExceeded("Attempt budget exhausted")
        record(path / f"{count + 1:03}.json", {"reserved_at_utc": datetime.now(UTC).isoformat()})

    def allocate(self, size: int, kind: str) -> Path:
        if self.used is None:
            self.used = charged(self.root)
        if size < 0 or self.used + size > self.byte_cap:
            raise BudgetExceeded("Byte cap would be exceeded")
        self.root.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(self.root).free <= size + 2**30:
            raise BudgetExceeded("Not enough free disk space")
        folder = self.root / ".budgets"
        validate_path(folder, self.root)
        if self.sequence is None:
            self.sequence = max((int(path.stem) for path in folder.glob("*.json") if not path.name.endswith(".settled.json")), default=0)
        self.sequence += 1
        path = folder / f"{self.sequence:09}.json"
        record(path, {"bytes": size, "kind": kind, "unit": self.identity})
        self.used += size
        return path

    def read(self, response: Any, size: int) -> bytes:
        if self.used is None:
            self.used = charged(self.root)
        available = self.byte_cap - self.used
        if available <= 0:
            raise BudgetExceeded("Byte cap would be exceeded")
        size = min(size, available)
        reservation = self.allocate(size, "received_bytes")
        # On any interrupted read the full reservation remains charged, including unknown partial bytes.
        body = response.read(size)
        if len(body) > size:
            raise BudgetExceeded("Response exceeded its reserved read")
        record(reservation.with_name(reservation.stem + ".settled.json"), {"bytes": len(body)})
        self.used -= size - len(body)
        return body


CURRENT: ContextVar[Boundary | None] = ContextVar("redownload_boundary", default=None)


@contextmanager
def active(boundary: Boundary) -> Iterator[None]:
    token = CURRENT.set(boundary)
    try:
        yield
    finally:
        CURRENT.reset(token)


def begin(key: str) -> None:
    if (boundary := CURRENT.get()) is not None:
        boundary.attempt(key)


def response_status(response: Any) -> None:
    if CURRENT.get() is not None and response.status == 429:
        header = response.getheader("Retry-After", "") if hasattr(response, "getheader") else response.headers.get("Retry-After", "")
        delay = min(int(header), 86400) if str(header).isdigit() else 0
        raise Pause(f"publisher_rate_limit; retry_after_seconds={delay}")


def read(response: Any, size: int) -> bytes:
    boundary = CURRENT.get()
    return boundary.read(response, size) if boundary is not None else response.read(size)


def returned(action: Any, *arguments: Any) -> bytes:
    """Injected API requests also consume the real budget, without double-counting a live read."""
    boundary = CURRENT.get()
    before = boundary.sequence if boundary is not None else None
    body = action(*arguments)
    if boundary is not None and boundary.sequence == before:
        boundary.allocate(len(body), "injected_received_bytes")
    return body


def writing(path: Path, size: int) -> None:
    if (boundary := CURRENT.get()) is not None:
        validate_path(path, boundary.root)
        boundary.allocate(size, "persisted_copy_bytes")


@contextmanager
def quota_lock(root: Path, production_base: Path) -> Iterator[list[Path]]:
    """All production BLS collectors share this lock, including old roots and fresh harnesses."""
    boundary = CURRENT.get()
    scope = boundary.base if boundary is not None else production_base if root.is_relative_to(production_base) else root
    folder = scope / "data/redownload_checks" if scope == production_base else scope
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / ".bls_quota.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            # Only BLS ledgers carry batch_id/reserved_at_utc; callers filter the metadata, never raw data.
            yield sorted(set(scope.glob("**/requests/*.json")) | set(root.glob("requests/*.json")))
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def recent(paths: list[Path], now: datetime) -> int:
    total = 0
    for path in paths:
        item = json.loads(path.read_text())
        if "reserved_at_utc" not in item:
            continue
        stamp = datetime.fromisoformat(item["reserved_at_utc"])
        if stamp.tzinfo is None or stamp > now:
            raise ValueError("BLS quota ledger clock changed")
        total += stamp > now - timedelta(hours=24)
    return total
