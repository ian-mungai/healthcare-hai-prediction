"""Append-only, fsynced submission ledgers: ``<ledger folder>/<run_id>.jsonl`` (Airflow plan section 7).

Each line is validated against ``data_contracts/orchestration/ledger_line.schema.json`` and appended with an fsync before
the action it announces; a new ledger file also publishes its folder (``publish_folder``). Lines are never rewritten. A ledger with an
unparseable, invalid or out-of-order line stops every operation on that run with the line number; nothing repairs or
truncates it (failure mode 820). One exclusive ``flock`` per run serializes the duplicate check, the intent and the
container create of a submission (failure mode 819).
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

RUN_ID = re.compile(r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$")
# The transitions of one submission, as the contract E2E checks them (scripts/orchestration/run_contracts_e2e.py).
NEXT_EVENTS: dict[str | None, set[str]] = {
    None: {"intent"},
    "intent": {"started", "adopted", "finished", "failed_closed"},
    "started": {"finished", "adopted", "failed_closed"},
    "adopted": {"finished", "failed_closed"},
}
LOCK_WAIT_SECONDS = 600
CONTRACTS = Path(__file__).resolve().parents[2] / "data_contracts" / "orchestration"


def schema_validator(name: str) -> Draft202012Validator:
    """A Draft 2020-12 validator for one orchestration contract schema."""
    schema = json.loads((CONTRACTS / name).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def fsync_folder(folder: Path) -> None:
    """Flush a folder's entries (the names of the files and folders created in it) to disk."""
    descriptor = os.open(folder, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_folder(folder: Path, root: Path) -> None:
    """Make every directory entry on the path from ``root`` down to ``folder`` durable: flush ``folder``, each folder above it up to ``root`` and the
    folder that holds ``root``. Flushing a file does not publish the entry of the folder that holds it, so a folder created for a file is published
    here, before the file's own flush is relied on. It is safe to repeat: a retry after a failed flush flushes the whole path again."""
    if root != folder and root not in folder.parents:
        raise ValueError(f"{folder} is not inside {root}")
    folder.mkdir(parents=True, exist_ok=True)
    path = [folder]
    while path[-1] != root:
        path.append(path[-1].parent)
    for item in [*path, root.parent]:
        fsync_folder(item)


class LedgerError(Exception):
    """A ledger line is unreadable, invalid or out of order; the run's operations stop for the owner."""


class LedgerUnavailable(Exception):
    """The ledger's first line is written, but the directories of its new file could not be made durable; the line stays and the call can be repeated."""


def now() -> str:
    """Current UTC time in ISO format."""
    return datetime.now(UTC).isoformat()


class Ledger:
    """The ledger files of one state folder."""

    def __init__(self, folder: Path) -> None:
        """``folder`` holds one ``<run_id>.jsonl`` per run and its ``.lock`` file."""
        self.folder = folder
        self._schema = schema_validator("ledger_line.schema.json")

    def path(self, run_id: str) -> Path:
        """The ledger file of one run; refuses a malformed run ID before it reaches a path."""
        if not RUN_ID.fullmatch(run_id):
            raise LedgerError(f"malformed run ID {run_id!r}")
        return self.folder / f"{run_id}.jsonl"

    @contextmanager
    def locked(self, run_id: str) -> Iterator[None]:
        """Hold the run's exclusive ledger lock."""
        self.folder.mkdir(parents=True, exist_ok=True)
        with (self.folder / f"{self.path(run_id).stem}.lock").open("a") as lock:
            deadline = time.monotonic() + LOCK_WAIT_SECONDS
            while True:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() > deadline:
                        raise LedgerError(f"ledger lock of {run_id} busy for {LOCK_WAIT_SECONDS} s") from None
                    time.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def read(self, run_id: str) -> list[dict[str, Any]]:
        """Every line of the run's ledger, checked for schema and order; an empty list when none exists."""
        path = self.path(run_id)
        if not path.exists():
            return []
        lines: list[dict[str, Any]] = []
        last: dict[str, str] = {}
        text = path.read_text()
        if text and not text.endswith("\n"):
            raise LedgerError(f"{path.name}:{text.count(chr(10)) + 1}: last line is incomplete (torn write); the owner decides")
        for number, raw in enumerate(text.splitlines(), 1):
            try:
                line = json.loads(raw)
            except ValueError:
                raise LedgerError(f"{path.name}:{number}: not JSON") from None
            errors = sorted(self._schema.iter_errors(line), key=str)
            if errors:
                raise LedgerError(f"{path.name}:{number}: {errors[0].message}")
            if line["run_id"] != run_id:
                raise LedgerError(f"{path.name}:{number}: names run {line['run_id']}")
            previous = last.get(line["submission_id"])
            if line["event"] not in NEXT_EVENTS.get(previous, set()):
                raise LedgerError(f"{path.name}:{number}: {line['event']} cannot follow {previous}")
            last[line["submission_id"]] = line["event"]
            lines.append(line)
        return lines

    def append(self, line: dict[str, Any]) -> dict[str, Any]:
        """Validate ``line``, append it and fsync before returning; the caller holds the run's lock."""
        line = {"schema_version": 1, "at": now(), **line}
        errors = sorted(self._schema.iter_errors(line), key=str)
        if errors:
            raise LedgerError(f"refusing to write an invalid ledger line: {errors[0].message}")
        path = self.path(line["run_id"])
        created = not path.exists()
        self.folder.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(descriptor, (json.dumps(line, sort_keys=True) + "\n").encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if created:
            try:
                publish_folder(self.folder, self.folder.parent)
            except OSError as error:
                raise LedgerUnavailable(f"the ledger's folders cannot be made durable: {type(error).__name__}") from None
        return line

    def runs(self) -> list[str]:
        """Run IDs with a ledger file."""
        return sorted(path.stem for path in self.folder.glob("*.jsonl") if RUN_ID.fullmatch(path.stem))

    def submissions(self, run_id: str) -> dict[str, list[dict[str, Any]]]:
        """The run's lines grouped by submission ID, in ledger order."""
        grouped: dict[str, list[dict[str, Any]]] = {}
        for line in self.read(run_id):
            grouped.setdefault(line["submission_id"], []).append(line)
        return grouped
