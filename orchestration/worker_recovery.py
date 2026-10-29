"""Keep worker recovery read-only and require typed owner confirmation for every non-absence closure."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import stat
import time
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from weakref import WeakKeyDictionary

from orchestration.worker_protocol import decode
from scripts.process_group import group_state, process_snapshot

_ACTIVE_SESSIONS: WeakKeyDictionary[WorkerLedger, str] = WeakKeyDictionary()

MAX_LEDGER_BYTES = 32 * 1024 * 1024
WARNING = "The launcher cannot prove this group is the worker's; token not seen does not mean the group is not yours, and token seen does not mean it is."
EVIDENCE_KEYS = frozenset({"group", "token", "members", "scan_utc"})
SPAWN_KEYS = frozenset({"nonce", "pid", "pgid", "leader_start", "spawn_utc", "lane", "session", "witness", "kind", "owner"})


def _utc() -> str:
    return datetime.now(UTC).isoformat()


def _evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    if (
        set(value) != EVIDENCE_KEYS
        or value["group"] not in {"present", "absent", "denied", "timed_out", "not_probed"}
        or value["token"] not in {"seen", "not_seen", "denied", "timed_out"}
    ):
        raise ValueError("invalid_worker_evidence")
    if not isinstance(value["scan_utc"], str) or len(value["scan_utc"]) > 64 or not isinstance(value["members"], list):
        raise ValueError("invalid_worker_evidence")
    for member in value["members"]:
        if set(member) != {"pid", "name", "state"} or type(member["pid"]) is not int or member["pid"] < 1:
            raise ValueError("invalid_worker_member")
        if any(not isinstance(member[key], str) or len(member[key]) > 128 or any(ord(char) < 32 for char in member[key]) for key in ("name", "state")):
            raise ValueError("invalid_worker_member")
    return dict(value)


class WorkerLedger:
    """Serialize private append-only ownership records with a bounded file lock."""

    def __init__(self, registry: Path) -> None:
        self.registry = registry
        self.path = registry / "workers.jsonl"

    def register_session(self, session: str) -> None:
        """Register a live in-process transport session until its confirmed close or collection."""
        _ACTIVE_SESSIONS[self] = session

    def unregister_session(self) -> None:
        """Remove this transport's session after confirmed cleanup."""
        _ACTIVE_SESSIONS.pop(self, None)

    def session_active(self, session: str) -> bool:
        """Identify live in-process transports that use this same registry."""
        return any(value == session and ledger.registry.resolve() == self.registry.resolve() for ledger, value in tuple(_ACTIVE_SESSIONS.items()))

    @contextlib.contextmanager
    def _locked(self, deadline: float, *, create: bool = False) -> Iterator[int | None]:
        if create:
            self.registry.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not self.registry.exists():
            yield None
            return
        status = self.registry.lstat()
        if not stat.S_ISDIR(status.st_mode) or status.st_uid != os.getuid() or status.st_mode & 0o077:
            raise PermissionError("worker_registry_must_be_private")
        try:
            fd = os.open(self.path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | (os.O_CREAT if create else 0), 0o600)
        except FileNotFoundError:
            if create:
                raise
            yield None
            return
        try:
            status = os.fstat(fd)
            if not stat.S_ISREG(status.st_mode) or status.st_uid != os.getuid() or status.st_mode & 0o077:
                raise PermissionError("worker_ledger_must_be_private")
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError("worker_ledger_lock_deadline")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
            yield fd
        finally:
            os.close(fd)

    def _read(self, fd: int | None, deadline: float) -> list[dict[str, Any]]:
        if fd is None:
            return []
        if os.fstat(fd).st_size > MAX_LEDGER_BYTES:
            raise ValueError("worker_ledger_limit")
        os.lseek(fd, 0, os.SEEK_SET)
        chunks = bytearray()
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError("worker_ledger_read_deadline")
            chunk = os.read(fd, min(65536, MAX_LEDGER_BYTES + 1 - len(chunks)))
            if not chunk:
                break
            chunks.extend(chunk)
            if len(chunks) > MAX_LEDGER_BYTES:
                raise ValueError("worker_ledger_limit")
        if chunks and not chunks.endswith(b"\n"):
            raise ValueError("worker_ledger_partial_record")
        rows = [decode(bytes(line)) for line in chunks.splitlines()]
        self._state(rows)
        return rows

    def _state(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        entries: dict[str, dict[str, Any]] = {}
        for row in rows:
            event = row.get("event")
            if event == "spawn":
                spawn = row.get("spawn", {})
                if set(row) != {"event", "utc", "spawn"} or set(spawn) - SPAWN_KEYS or not (SPAWN_KEYS - {"owner"}) <= set(spawn):
                    raise ValueError("worker_ledger_invalid_spawn")
                if spawn["nonce"] in entries or type(spawn["pid"]) is not int or type(spawn["pgid"]) is not int or spawn["pid"] < 1 or spawn["pgid"] < 1:
                    raise ValueError("worker_ledger_invalid_identity")
                if spawn["kind"] not in {"ledger", "marker"} or not isinstance(spawn["witness"], str):
                    raise ValueError("worker_ledger_invalid_spawn")
                entries[spawn["nonce"]] = {"spawn": spawn, "observed": False, "closed": False, "closed_by": None, "evidence": None}
                continue
            if set(row) != {"event", "utc", "nonce", "pgid", "evidence"} or event not in {
                "observed",
                "closed_absent",
                "closed_by_owner",
                "gone",
                "stop_unconfirmed",
            }:
                raise ValueError("worker_ledger_invalid_event")
            entry = entries.get(row["nonce"])
            if entry is None or entry["closed"] or row["pgid"] != entry["spawn"]["pgid"]:
                raise ValueError("worker_ledger_invalid_transition")
            evidence = _evidence(row["evidence"])
            if event in {"closed_absent", "gone"} and evidence["group"] != "absent":
                raise ValueError("worker_ledger_closure_requires_absence")
            if event == "closed_absent" and not entry["observed"]:
                raise ValueError("worker_ledger_first_read_cannot_close")
            entry["evidence"] = evidence
            entry["observed"] = entry["observed"] or event == "observed"
            entry["closed"] = event in {"closed_absent", "closed_by_owner", "gone"}
            entry["closed_by"] = event if entry["closed"] else None
        return entries

    def _append(self, fd: int, row: Mapping[str, Any], deadline: float) -> None:
        payload = json.dumps(row, allow_nan=False, separators=(",", ":")).encode() + b"\n"
        if os.fstat(fd).st_size + len(payload) > MAX_LEDGER_BYTES:
            raise ValueError("worker_ledger_limit")
        os.lseek(fd, 0, os.SEEK_END)
        pending = memoryview(payload)
        while pending:
            if time.monotonic() >= deadline:
                raise TimeoutError("worker_ledger_append_deadline")
            pending = pending[os.write(fd, pending) :]
        os.fsync(fd)

    def append_spawn(self, spawn: Mapping[str, Any], *, deadline: float) -> None:
        """Persist ownership before a first frame. Repeating the exact spawn has no extra effect."""
        row = {"event": "spawn", "utc": _utc(), "spawn": dict(spawn)}
        self._state([row])
        with self._locked(deadline, create=True) as fd:
            if fd is None:
                raise RuntimeError("worker_ledger_not_open")
            entries = self._state(self._read(fd, deadline))
            if spawn["nonce"] in entries:
                if entries[spawn["nonce"]]["spawn"] != dict(spawn):
                    raise ValueError("worker_spawn_identity_conflict")
                return
            self._append(fd, row, deadline)

    def private_open(self, *, deadline: float) -> list[dict[str, Any]]:
        """Return private in-memory records for the read-only evidence collector."""
        with self._locked(deadline) as fd:
            return [entry["spawn"] for entry in self._state(self._read(fd, deadline)).values() if not entry["closed"]]

    def closed_events(self, *, deadline: float) -> dict[str, str]:
        """Return closure reasons without exposing private spawn fields."""
        with self._locked(deadline) as fd:
            return {nonce: entry["closed_by"] for nonce, entry in self._state(self._read(fd, deadline)).items() if entry["closed"]}

    def known_markers(self, *, deadline: float) -> set[tuple[int, str]]:
        """Remember recorded marker identities so an owner closure stays closed on later scans."""
        with self._locked(deadline) as fd:
            records = [entry["spawn"] for entry in self._state(self._read(fd, deadline)).values()]
        return {(row["pid"], row["nonce"].removeprefix(f"marker_{row['pid']}_") if row["kind"] == "marker" else row["nonce"]) for row in records}

    def _public(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        spawn = {key: value for key, value in entry["spawn"].items() if key != "witness"}
        return {**spawn, "state": "read_only", "evidence": entry["evidence"], "warning": WARNING}

    def open_entries(self, *, deadline: float) -> list[dict[str, Any]]:
        """Return sanitized open records without writing or signalling anything."""
        with self._locked(deadline) as fd:
            return [self._public(entry) for entry in self._state(self._read(fd, deadline)).values() if not entry["closed"]]

    def observe(self, evidence: Mapping[str, Mapping[str, Any]], *, deadline: float) -> list[dict[str, Any]]:
        """Record first reads and close only a later read with its own current ESRCH evidence."""
        with self._locked(deadline) as fd:
            entries = self._state(self._read(fd, deadline))
            for nonce, observation in evidence.items():
                entry = entries.get(nonce)
                if entry is None or entry["closed"]:
                    continue
                current = _evidence(observation)
                event = "closed_absent" if entry["observed"] and current["group"] == "absent" else "observed"
                if fd is None:
                    raise RuntimeError("worker_ledger_not_open")
                self._append(fd, {"event": event, "utc": _utc(), "nonce": nonce, "pgid": entry["spawn"]["pgid"], "evidence": current}, deadline)
                entry.update(evidence=current, observed=True, closed=event == "closed_absent")
            return [self._public(entry) for entry in entries.values() if not entry["closed"]]

    def close(self, nonce: str, pgid: int, evidence: Mapping[str, Any], *, event: str, deadline: float) -> bool:
        """Append an idempotent confirmed closure for one exact recorded identity."""
        if event not in {"closed_by_owner", "gone"}:
            raise ValueError("invalid_worker_closure")
        current = _evidence(evidence)
        if event == "gone" and current["group"] != "absent":
            raise ValueError("worker_closure_requires_absence")
        with self._locked(deadline) as fd:
            entries = self._state(self._read(fd, deadline))
            entry = entries.get(nonce)
            if entry is None or entry["closed"]:
                return False
            if fd is None or entry["spawn"]["pgid"] != pgid:
                raise ValueError("worker_closure_identity_conflict")
            self._append(fd, {"event": event, "utc": _utc(), "nonce": nonce, "pgid": pgid, "evidence": current}, deadline)
            return True

    def redacted(self, *, deadline: float) -> list[dict[str, Any]]:
        """Return a saved-evidence copy with every private spawn witness removed."""
        with self._locked(deadline) as fd:
            rows = self._read(fd, deadline)
        return [{**row, "spawn": {**row["spawn"], "witness": "[redacted]"}} if row["event"] == "spawn" else row for row in rows]


class Recovery:
    """Read prior-session entries and expose one sanitized alert while dispatch is paused."""

    def __init__(self, ledger: WorkerLedger, session: str) -> None:
        self.ledger = ledger
        self.session = session
        self.alert: dict[str, Any] | None = None
        self.next_sweep = 0.0
        self.failed = False

    def _recovered(self, record: Mapping[str, Any]) -> bool:
        return record["session"] != self.session and not self.ledger.session_active(record["session"])

    def inspect(self, *, deadline: float, discover: bool = False) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        """Read current evidence. Discovery records markers only during a recovery sweep."""
        records = [row for row in self.ledger.private_open(deadline=deadline) if self._recovered(row)]
        snapshot = process_snapshot(min(deadline, time.monotonic() + 2), witnesses={row["nonce"]: (row["pgid"], row["witness"]) for row in records})
        if discover and snapshot.status == "seen":
            known = self.ledger.known_markers(deadline=deadline)
            for member in snapshot.members:
                if member.pid != member.pgid or member.marker_parent in {None, os.getpid()} or (member.pid, member.marker_nonce) in known:
                    continue
                nonce = f"marker_{member.pid}_{member.marker_nonce}"
                self.ledger.append_spawn(
                    {
                        "nonce": nonce,
                        "pid": member.pid,
                        "pgid": member.pgid,
                        "leader_start": "unverified",
                        "spawn_utc": _utc(),
                        "lane": "unknown",
                        "session": "recovered_marker",
                        "witness": "",
                        "kind": "marker",
                        "owner": "unknown",
                    },
                    deadline=deadline,
                )
                records = [row for row in self.ledger.private_open(deadline=deadline) if self._recovered(row)]
                known.add((member.pid, member.marker_nonce or ""))
        observations = {}
        probe_deadline = min(deadline, time.monotonic() + 1)
        for record in records:
            pgid = record["pgid"]
            state = group_state(pgid) if time.monotonic() < probe_deadline else "timed_out"
            group = {"gone": "absent", "alive": "present"}.get(state, state)
            token = ("seen" if record["nonce"] in snapshot.seen_nonces else "not_seen") if snapshot.status == "seen" else snapshot.status
            observations[record["nonce"]] = {
                "group": group,
                "token": token,
                "members": [{"pid": member.pid, "name": member.name, "state": member.state} for member in snapshot.group(pgid)],
                "scan_utc": _utc(),
            }
        return records, observations

    def sweep(self, *, deadline: float) -> list[dict[str, Any]]:
        """Refresh recovery evidence without any signal or reap of a recovered process."""
        try:
            _, observations = self.inspect(deadline=deadline, discover=True)
            entries = self.ledger.observe(observations, deadline=deadline)
            recovered = [entry for entry in entries if self._recovered(entry)]
            self.alert = {"kind": "worker_unproven", "entries": recovered, "command": "python -m orchestration.launcher workers"} if recovered else None
            self.failed = False
            self.next_sweep = time.monotonic() + 30
            return recovered
        except (OSError, ValueError, TimeoutError):
            self.failed = True
            self.alert = {"kind": "worker_recovery_unavailable", "entries": [], "command": "python -m orchestration.launcher workers"}
            raise

    def refresh_closures(self, *, deadline: float) -> bool:
        """Observe owner closures on each tick and request an immediate first evidence scan."""
        try:
            entries = [entry for entry in self.ledger.open_entries(deadline=deadline) if self._recovered(entry)]
            self.alert = {"kind": "worker_unproven", "entries": entries, "command": "python -m orchestration.launcher workers"} if entries else None
            return any(entry["evidence"] is None for entry in entries)
        except (OSError, ValueError, TimeoutError):
            self.failed = True
            self.alert = {"kind": "worker_recovery_unavailable", "entries": [], "command": "python -m orchestration.launcher workers"}
            raise

    @property
    def dispatch_paused(self) -> bool:
        """Keep dispatch paused whenever recovery is incomplete or has an open alert."""
        return self.failed or self.alert is not None
