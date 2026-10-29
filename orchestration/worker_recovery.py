"""Keep worker recovery read-only, trust only this registry's own records and require typed owner confirmation for every non-absence closure."""

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
from scripts.process_group import group_state, normalize_start, process_snapshot, start_epoch

_ACTIVE_SESSIONS: WeakKeyDictionary[WorkerLedger, str] = WeakKeyDictionary()

MAX_LEDGER_BYTES = 32 * 1024 * 1024
WARNING = "The launcher cannot prove this group is the worker's; token not seen does not mean the group is not yours, and token seen does not mean it is."
EVIDENCE_KEYS = frozenset({"group", "token", "members", "scan_utc"})
LEADER_STATES = frozenset({"match", "replaced", "absent", "unknown"})
INTENT_KEYS = frozenset({"nonce", "spawn_utc", "session", "lane", "owner", "witness"})
INTENT_GRACE_SECONDS = 10.0
GUARD_WAIT_SECONDS = 2.0
SPAWN_LOCK = "spawn.lock"
SPAWN_KEYS = frozenset({"nonce", "pid", "pgid", "leader_start", "spawn_utc", "lane", "session", "witness", "kind", "owner"})


def _utc() -> str:
    return datetime.now(UTC).isoformat()


def _evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    if (
        set(value) - {"leader"} != EVIDENCE_KEYS
        or value.get("leader", "unknown") not in LEADER_STATES
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

    def _check_registry(self) -> None:
        status = self.registry.lstat()
        if not stat.S_ISDIR(status.st_mode) or status.st_uid != os.getuid() or status.st_mode & 0o077:
            raise PermissionError("worker_registry_must_be_private")

    @staticmethod
    def _flock(fd: int, deadline: float, reason: str) -> None:
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError(reason)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))

    @contextlib.contextmanager
    def spawn_guard(self, deadline: float, *, create: bool = True) -> Iterator[None]:
        """Hold the spawn lock: a spawner from before its intent row until its spawn row is written, a sweep while it decides expiry."""
        if create:
            self.registry.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._check_registry()
        fd = os.open(self.registry / SPAWN_LOCK, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_CREAT, 0o600)
        try:
            status = os.fstat(fd)
            if not stat.S_ISREG(status.st_mode) or status.st_uid != os.getuid() or status.st_mode & 0o077:
                raise PermissionError("worker_spawn_lock_must_be_private")
            self._flock(fd, deadline, "worker_spawn_lock_deadline")
            yield
        finally:
            os.close(fd)

    @contextlib.contextmanager
    def _locked(self, deadline: float, *, create: bool = False) -> Iterator[int | None]:
        if create:
            self.registry.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not self.registry.exists():
            yield None
            return
        self._check_registry()
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
            self._flock(fd, deadline, "worker_ledger_lock_deadline")
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
        self._intent_state(rows)
        return rows

    def _intent_state(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Fold intent rows. An intent is resolved by a later spawn row for its nonce or by an explicit closure."""
        intents: dict[str, dict[str, Any]] = {}
        spawned = {row["spawn"]["nonce"] for row in rows if row.get("event") == "spawn"}
        for row in rows:
            event = row.get("event")
            if event == "intent":
                intent = row.get("intent", {})
                if set(row) != {"event", "utc", "intent"} or set(intent) != INTENT_KEYS or any(not isinstance(intent[key], str) for key in INTENT_KEYS):
                    raise ValueError("worker_ledger_invalid_intent")
                if not intent["nonce"] or intent["nonce"] in intents:
                    raise ValueError("worker_ledger_invalid_intent")
                try:
                    datetime.fromisoformat(intent["spawn_utc"])
                except ValueError as error:
                    raise ValueError("worker_ledger_invalid_intent") from error
                intents[intent["nonce"]] = {"intent": intent, "resolved": intent["nonce"] in spawned}
            elif event == "intent_closed":
                item = intents.get(row.get("nonce", ""))
                if set(row) != {"event", "utc", "nonce", "reason"} or item is None or row["reason"] not in {"no_worker", "spawn_failed"}:
                    raise ValueError("worker_ledger_invalid_intent_closure")
                item["resolved"] = True
        return intents

    def _state(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        entries: dict[str, dict[str, Any]] = {}
        for row in rows:
            event = row.get("event")
            if event in {"intent", "intent_closed"}:
                continue
            if event == "spawn":
                spawn = row.get("spawn", {})
                if set(row) != {"event", "utc", "spawn"} or set(spawn) - SPAWN_KEYS or not (SPAWN_KEYS - {"owner"}) <= set(spawn):
                    raise ValueError("worker_ledger_invalid_spawn")
                if spawn["nonce"] in entries or type(spawn["pid"]) is not int or type(spawn["pgid"]) is not int or spawn["pid"] < 1 or spawn["pgid"] < 1:
                    raise ValueError("worker_ledger_invalid_identity")
                if spawn["kind"] != "ledger" or not isinstance(spawn["witness"], str):
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

    def append_intent(self, intent: Mapping[str, Any], *, deadline: float) -> None:
        """Persist the intent to spawn before any process exists. Repeating the exact intent has no extra effect."""
        row = {"event": "intent", "utc": _utc(), "intent": dict(intent)}
        self._intent_state([row])
        with self._locked(deadline, create=True) as fd:
            if fd is None:
                raise RuntimeError("worker_ledger_not_open")
            known = self._intent_state(self._read(fd, deadline))
            if intent["nonce"] in known:
                if known[intent["nonce"]]["intent"] != dict(intent):
                    raise ValueError("worker_intent_identity_conflict")
                return
            self._append(fd, row, deadline)

    def close_intent(self, nonce: str, reason: str, *, deadline: float) -> bool:
        """Close one unresolved intent that never produced a recorded worker. Returns False when nothing was open."""
        with self._locked(deadline) as fd:
            item = self._intent_state(self._read(fd, deadline)).get(nonce)
            if fd is None or item is None or item["resolved"]:
                return False
            self._append(fd, {"event": "intent_closed", "utc": _utc(), "nonce": nonce, "reason": reason}, deadline)
            return True

    def open_intents(self, *, deadline: float) -> list[dict[str, Any]]:
        """Return private intent records that no spawn row or closure has resolved."""
        with self._locked(deadline) as fd:
            return [item["intent"] for item in self._intent_state(self._read(fd, deadline)).values() if not item["resolved"]]

    def adopt_intent(self, nonce: str, pid: int, pgid: int, leader_start: str, *, deadline: float) -> bool:
        """Turn an unresolved intent into the spawn row its crashed owner never wrote, from the OS-read pid, group and start time."""
        with self._locked(deadline) as fd:
            item = self._intent_state(self._read(fd, deadline)).get(nonce)
            if fd is None or item is None or item["resolved"]:
                return False
            intent = item["intent"]
            spawn = {
                "nonce": nonce,
                "pid": pid,
                "pgid": pgid,
                "leader_start": leader_start,
                "spawn_utc": intent["spawn_utc"],
                "lane": intent["lane"],
                "session": intent["session"],
                "witness": intent["witness"],
                "kind": "ledger",
                "owner": intent["owner"],
            }
            self._state([*self._read(fd, deadline), {"event": "spawn", "utc": _utc(), "spawn": spawn}])
            self._append(fd, {"event": "spawn", "utc": _utc(), "spawn": spawn}, deadline)
            return True

    def private_open(self, *, deadline: float) -> list[dict[str, Any]]:
        """Return private in-memory records for the read-only evidence collector."""
        with self._locked(deadline) as fd:
            return [entry["spawn"] for entry in self._state(self._read(fd, deadline)).values() if not entry["closed"]]

    def closed_events(self, *, deadline: float) -> dict[str, str]:
        """Return closure reasons without exposing private spawn fields."""
        with self._locked(deadline) as fd:
            return {nonce: entry["closed_by"] for nonce, entry in self._state(self._read(fd, deadline)).items() if entry["closed"]}

    def known_identities(self, *, deadline: float) -> set[tuple[int, str]]:
        """Return the pid and start time of every worker this registry ever recorded, closed ones included."""
        with self._locked(deadline) as fd:
            return {(entry["spawn"]["pid"], normalize_start(entry["spawn"]["leader_start"])) for entry in self._state(self._read(fd, deadline)).values()}

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
        return [
            {**row, key: {**row[key], "witness": "[redacted]"}} if (key := {"spawn": "spawn", "intent": "intent"}.get(row["event"])) else row for row in rows
        ]


class Recovery:
    """Read prior-session entries and expose one sanitized alert while dispatch is paused. Only this registry's own records pause it."""

    def __init__(self, ledger: WorkerLedger, session: str, *, intent_grace: float = INTENT_GRACE_SECONDS, guard_wait: float = GUARD_WAIT_SECONDS) -> None:
        self.ledger = ledger
        self.session = session
        self.intent_grace = intent_grace
        self.guard_wait = guard_wait
        self.alert: dict[str, Any] | None = None
        self.next_sweep = 0.0
        self.failed = False
        self.unresolved: list[dict[str, Any]] = []
        self.pending_intents: list[dict[str, Any]] = []
        self.not_matching: list[dict[str, Any]] = []

    def _recovered(self, record: Mapping[str, Any]) -> bool:
        return record["session"] != self.session and not self.ledger.session_active(record["session"])

    @staticmethod
    def _pauses(entry: Mapping[str, Any]) -> bool:
        """A record pauses unless its pid now has another start time and its group shows none of its witness."""
        evidence = entry["evidence"]
        return evidence is None or not (evidence.get("leader") == "replaced" and (evidence["group"], evidence["token"]) == ("present", "not_seen"))

    @staticmethod
    def _carriers(snapshot: Any, intent: Mapping[str, Any]) -> list[Any]:
        """Group leaders that carry the intent's nonce and started no earlier than the intent's second."""
        began = int(datetime.fromisoformat(intent["spawn_utc"]).timestamp())
        return [
            member
            for member in snapshot.members
            if member.pid == member.pgid and member.marker_nonce == intent["nonce"] and member.started and start_epoch(member.started) >= began
        ]

    def _adopt(self, intent: Mapping[str, Any], member: Any, deadline: float) -> None:
        self.ledger.adopt_intent(intent["nonce"], member.pid, member.pgid, normalize_start(member.started), deadline=deadline)

    def _pending(self, intent: Mapping[str, Any]) -> None:
        self.pending_intents.append({key: value for key, value in intent.items() if key != "witness"})

    def _resolve_intents(self, snapshot: Any, deadline: float) -> None:
        """Adopt the group leader that carries a recovered intent's nonce. Expiry is decided only under the spawn lock on a fresh snapshot."""
        self.pending_intents = []
        expired = []
        for intent in (row for row in self.ledger.open_intents(deadline=deadline) if self._recovered(row)):
            matches = self._carriers(snapshot, intent)
            if len(matches) == 1:
                self._adopt(intent, matches[0], deadline)
            elif not matches and time.time() - datetime.fromisoformat(intent["spawn_utc"]).timestamp() >= self.intent_grace:
                expired.append(intent)
            else:
                self._pending(intent)
        if expired:
            self._expire(expired, deadline)

    def _expire(self, intents: list[dict[str, Any]], deadline: float) -> None:
        """Close an intent only when a snapshot taken inside the spawn lock still shows no carrier. A held lock or an unreadable snapshot keeps it pending."""
        with contextlib.ExitStack() as stack:
            try:
                stack.enter_context(self.ledger.spawn_guard(min(deadline, time.monotonic() + self.guard_wait), create=False))
            except TimeoutError:
                for intent in intents:
                    self._pending(intent)
                return
            fresh = process_snapshot(min(deadline, time.monotonic() + 2))
            for intent in intents:
                matches = self._carriers(fresh, intent) if fresh.status == "seen" else None
                if matches is not None and len(matches) == 1:
                    self._adopt(intent, matches[0], deadline)
                elif matches is not None and not matches:
                    self.ledger.close_intent(intent["nonce"], "no_worker", deadline=deadline)
                else:
                    self._pending(intent)

    def _list_unresolved(self, snapshot: Any, deadline: float) -> list[dict[str, Any]]:
        """List marked group leaders that no spawn record of this registry names by pid and start time. Nothing pauses on them."""
        known = self.ledger.known_identities(deadline=deadline)
        return [
            {"pid": member.pid, "pgid": member.pgid, "name": member.name, "state": member.state, "started": member.started, "reason": "no_registry_record"}
            for member in snapshot.members
            if member.marker_nonce is not None and member.pid == member.pgid and (member.pid, normalize_start(member.started)) not in known
        ]

    def inspect(self, *, deadline: float, discover: bool = False) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        """Read current evidence. Discovery resolves intents and lists unrecorded workers only during a recovery sweep."""
        records = [row for row in self.ledger.private_open(deadline=deadline) if self._recovered(row)]
        snapshot = process_snapshot(min(deadline, time.monotonic() + 2), witnesses={row["nonce"]: (row["pgid"], row["witness"]) for row in records})
        if discover and snapshot.status == "seen":
            self._resolve_intents(snapshot, deadline)
            records = [row for row in self.ledger.private_open(deadline=deadline) if self._recovered(row)]
            self.unresolved = self._list_unresolved(snapshot, deadline)
        observations = {}
        probe_deadline = min(deadline, time.monotonic() + 1)
        for record in records:
            pgid = record["pgid"]
            state = group_state(pgid) if time.monotonic() < probe_deadline else "timed_out"
            group = {"gone": "absent", "alive": "present"}.get(state, state)
            token = ("seen" if record["nonce"] in snapshot.seen_nonces else "not_seen") if snapshot.status == "seen" else snapshot.status
            leader = next((member for member in snapshot.members if member.pid == record["pid"]), None)
            if snapshot.status != "seen":
                identity = "unknown"
            elif leader is None:
                identity = "absent"
            else:
                identity = "match" if normalize_start(leader.started) == normalize_start(record["leader_start"]) else "replaced"
            observations[record["nonce"]] = {
                "group": group,
                "token": token,
                "leader": identity,
                "members": [{"pid": member.pid, "name": member.name, "state": member.state} for member in snapshot.group(pgid)],
                "scan_utc": _utc(),
            }
        return records, observations

    def sweep(self, *, deadline: float) -> list[dict[str, Any]]:
        """Refresh recovery evidence without any signal or reap of a recovered process."""
        try:
            _, observations = self.inspect(deadline=deadline, discover=True)
            recovered = [entry for entry in self.ledger.observe(observations, deadline=deadline) if self._recovered(entry)]
            self.not_matching = [entry for entry in recovered if not self._pauses(entry)]
            paused = [entry for entry in recovered if self._pauses(entry)]
            self.alert = {"kind": "worker_unproven", "entries": paused, "command": "python -m orchestration.launcher workers"} if paused else None
            self.failed = False
            self.next_sweep = time.monotonic() + 30
            return paused
        except (OSError, ValueError, TimeoutError):
            self.failed = True
            self.alert = {"kind": "worker_recovery_unavailable", "entries": [], "command": "python -m orchestration.launcher workers"}
            raise

    def refresh_closures(self, *, deadline: float) -> bool:
        """Observe owner closures on each tick and request an immediate first evidence scan."""
        try:
            entries = [entry for entry in self.ledger.open_entries(deadline=deadline) if self._recovered(entry) and self._pauses(entry)]
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

    def report(self) -> dict[str, Any]:
        """Return the read-only recovery result: paused entries, unresolved marked workers, records that match nothing and pending intents."""
        return {
            "paused": self.dispatch_paused,
            "entries": list(self.alert["entries"]) if self.alert else [],
            "unresolved": list(self.unresolved),
            "records_not_matching": list(self.not_matching),
            "pending_intents": list(self.pending_intents),
        }
