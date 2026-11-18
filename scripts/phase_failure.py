"""The one line that a host job writes when its phase fails.

A host job writes ``phase_failure: {"phase": ..., "reason": ...}`` as the last line of a failed phase (``emit``; ``resource_peaks.recorded`` writes it for a job
that wrote none, from the job's last stderr line). The launcher reads the tail of the failing container's log and keeps its summary in the end record of the
launch (``orchestration.ecs_adapter.failure_summary``, the reader, which the Airflow image holds and this module is not part of), so the evidence and gate tasks
see why the phase failed without the task log. Both sides clean the text: no ARN, account number, access key, credential assignment, URL credential or long
token, one line and at most ``LIMIT`` characters, and ``tests/test_phase_failure.py`` keeps the two sides equal."""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TextIO

MARKER = "phase_failure: "
LIMIT = 300
ARN = re.compile(r"arn:[^\s,;]+")
ACCOUNT = re.compile(r"\b[0-9]{12}\b")
ACCESS_KEY = re.compile(r"\b(?:AKIA|ASIA|AIDA|AROA)[A-Z0-9]{16}\b")
CREDENTIAL = re.compile(r"(?i)\b(password|passwd|secret|token|api[_-]?key|authorization|credential)s?\b\s*[=:]\s*\S+")
URL_USER = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s/@]+@")
LONG_TOKEN = re.compile(r"\b[A-Za-z0-9+/_=-]{32,}\b")
PHASE_NAME = re.compile(r"[a-z][a-z0-9_]{0,39}")
CONTROL = re.compile(r"\s+")


def clean(text: object) -> str:
    """One line of at most ``LIMIT`` characters, with every ARN, account number, access key, credential assignment, URL credential and long token replaced."""
    found = str(text)
    found = ARN.sub("[arn]", found)
    found = ACCOUNT.sub("[account]", found)
    found = ACCESS_KEY.sub("[key]", found)
    found = CREDENTIAL.sub(lambda match: f"{match.group(1)}=[redacted]", found)
    found = URL_USER.sub(lambda match: f"{match.group(1)}[redacted]@", found)
    found = LONG_TOKEN.sub("[token]", found)
    return CONTROL.sub(" ", found).strip()[:LIMIT]


def line(phase: str, reason: object) -> str:
    """The failure line of a phase, without its line end."""
    name = phase if PHASE_NAME.fullmatch(phase) else "unknown"
    return MARKER + json.dumps({"phase": name, "reason": clean(reason)}, sort_keys=True, separators=(",", ":"))


def emit(phase: str, reason: object, stream: TextIO | None = None) -> None:
    """Write the failure line of a phase on standard error, as the last line of the phase."""
    target = sys.stderr if stream is None else stream
    target.write(line(phase, reason) + "\n")
    target.flush()


def phase_of(argv: Sequence[str]) -> str:
    """The phase an entry point runs: its first argument when that is a phase name (``build``), else the name of its script."""
    if len(argv) > 1 and PHASE_NAME.fullmatch(argv[1]):
        return argv[1]
    stem = Path(argv[0]).stem if argv and argv[0] else ""
    return stem if PHASE_NAME.fullmatch(stem) else "unknown"


class StderrTail:
    """Stands in for standard error while a job runs: every write goes through, and the last non-empty line and whether the job wrote its own failure line
    are kept. The job entry points print their failure as their last line, so the recorder can write the failure line for the ones that print none."""

    def __init__(self, stream: TextIO) -> None:
        self.stream = stream
        self.last = ""
        self.marked = False
        self.pending = ""

    def write(self, text: str) -> int:
        written = self.stream.write(text)
        *lines, self.pending = (self.pending + text).split("\n")
        self.pending = self.pending[-2000:]
        for item in lines:
            self.note(item)
        return written

    def note(self, item: str) -> None:
        found = item.strip()
        if found.startswith(MARKER):
            self.marked = True
        elif found:
            self.last = found

    def finish(self) -> None:
        """Count the text after the last line end as a line."""
        self.note(self.pending)
        self.pending = ""

    def __getattr__(self, name: str) -> Any:
        return getattr(self.stream, name)
