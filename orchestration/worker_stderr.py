"""Drain owned child stderr with finite buffers, private sanitized files and explicit completion."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import re
import select
import threading
import time
from collections.abc import Collection
from pathlib import Path
from typing import Any
from urllib.parse import quote, quote_plus

CHUNK_BYTES = 65536
MAX_VALUE_BYTES = 4096
MAX_VALUES = 64
MAX_FILE_BYTES = 8 * 1024 * 1024
TRUNCATED = b"\n[stderr truncated]\n"
REDACTED = b"[redacted]"
WITHHELD = b"[redacted stderr]\n"


class StreamRedactor:
    """Replace known values and their encoded variants without emitting a partial match."""

    def __init__(self, values: Collection[str]) -> None:
        if len(values) > MAX_VALUES:
            raise ValueError("stderr_redaction_set_too_large")
        variants: set[bytes] = set()
        for value in values:
            raw = value.encode()
            if not raw or len(raw) > MAX_VALUE_BYTES:
                raise ValueError("stderr_redaction_value_out_of_bounds")
            variants.update((raw, json.dumps(value, ensure_ascii=True)[1:-1].encode(), base64.b64encode(raw)))
            for encoded in (quote(value, safe="").encode(), quote_plus(value, safe="").encode()):
                variants.add(encoded)
                variants.add(re.sub(rb"%[0-9A-F]{2}", lambda match: match[0].lower(), encoded))
            for alignment in range(3):
                encoded = base64.b64encode(b"\0" * alignment + raw)
                variants.add(encoded[(alignment * 8 + 5) // 6 : ((alignment + len(raw)) * 8) // 6])
        variants.discard(b"")
        self.pattern = re.compile(b"|".join(re.escape(value) for value in sorted(variants, key=lambda value: (-len(value), value)))) if variants else None
        self.overlap = max((len(value) - 1 for value in variants), default=0)
        self.pending = b""

    def feed(self, chunk: bytes, *, final: bool = False) -> bytes:
        """Emit safe bytes and retain only the longest possible incomplete match."""
        data = self.pending + chunk
        cutoff = len(data) if final else max(0, len(data) - self.overlap)
        pieces: list[bytes] = []
        position = 0
        if self.pattern is not None:
            for match in self.pattern.finditer(data):
                if match.start() >= cutoff:
                    break
                pieces.extend((data[position : match.start()], REDACTED))
                position = match.end()
        cutoff = max(cutoff, position)
        pieces.append(data[position:cutoff])
        self.pending = data[cutoff:]
        return b"".join(pieces)

    def finish(self) -> bytes:
        """Flush the finite carry only after the owner proves stream EOF."""
        return self.feed(b"", final=True)


class StderrReader:
    """Own a duplicate pipe and reader thread until EOF or a visible absolute-deadline failure."""

    def __init__(self, fd: int, path: Path, *, deadline: float, values: Collection[str] = (), withhold: bool = False) -> None:
        self.path = path
        self.redactor = StreamRedactor(values)
        self.withhold = withhold
        self.deadline = deadline
        self.bytes_read = 0
        self.bytes_written = 0
        self.truncated = False
        self.max_buffer_bytes = 0
        self.failure: str | None = None
        self.eof = False
        self._started = False
        self._marked = False
        self._done = threading.Event()
        self._input = os.dup(fd)
        try:
            os.set_blocking(self._input, False)
            self._output = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except BaseException:
            os.close(self._input)
            raise
        self._thread = threading.Thread(target=self._drain, name=f"worker_stderr_{self._input}", daemon=True)

    def start(self) -> None:
        """Start only after the caller has retained this reader with its process handle."""
        if self._started or self._done.is_set():
            raise RuntimeError("stderr_reader_start_refused")
        try:
            self._thread.start()
            self._started = True
        except (RuntimeError, OSError):
            self.failure = "worker_stderr_start_failed"
            for fd in (self._input, self._output):
                with contextlib.suppress(OSError):
                    os.close(fd)
            self._done.set()
            raise

    def _emit(self, data: bytes) -> None:
        if self.failure is not None or self.truncated:
            return
        remaining = max(0, MAX_FILE_BYTES - len(TRUNCATED) - self.bytes_written)
        if len(data) > remaining:
            data = data[:remaining] + TRUNCATED
            self.truncated = True
        payload = memoryview(data)
        try:
            while payload:
                written = os.write(self._output, payload)
                if written <= 0:
                    raise OSError("stderr_destination_closed")
                self.bytes_written += written
                payload = payload[written:]
        except OSError:
            self.failure = "worker_stderr_write_failed"

    def _drain(self) -> None:
        try:
            while time.monotonic() < self.deadline:
                ready, _, _ = select.select([self._input], [], [], min(0.05, max(0.0, self.deadline - time.monotonic())))
                if not ready:
                    continue
                try:
                    chunk = os.read(self._input, CHUNK_BYTES)
                except BlockingIOError:
                    continue
                if not chunk:
                    self.eof = True
                    break
                self.bytes_read += len(chunk)
                self.max_buffer_bytes = max(self.max_buffer_bytes, len(chunk) + len(self.redactor.pending))
                if self.withhold:
                    if not self._marked:
                        self._emit(WITHHELD)
                        self._marked = True
                else:
                    self._emit(self.redactor.feed(chunk))
            if self.eof:
                self._emit(self.redactor.finish())
            elif self.failure is None:
                self.failure = "worker_stderr_deadline"
        except (OSError, ValueError):
            self.failure = "worker_stderr_read_failed"
        finally:
            self.redactor.pending = b""
            for fd in (self._input, self._output):
                with contextlib.suppress(OSError):
                    os.close(fd)
            self._done.set()

    @property
    def is_alive(self) -> bool:
        """Report the reader's retained thread duty without exposing diagnostic text."""
        return self._thread.is_alive()

    @property
    def complete(self) -> bool:
        """Require EOF, flushed output and a stopped thread for successful completion."""
        return self._done.is_set() and not self.is_alive and self.eof and self.failure is None

    def finish(self, *, deadline: float) -> bool:
        """Join after group stop within the caller's remaining absolute cleanup budget."""
        self.deadline = min(self.deadline, deadline)
        if self._started:
            self._thread.join(timeout=max(0.0, deadline - time.monotonic()))
        return self.complete

    def snapshot(self) -> dict[str, Any]:
        """Return counters and lifecycle evidence without raw stderr or redaction values."""
        return {
            "path": str(self.path),
            "bytes_read": self.bytes_read,
            "bytes_written": self.bytes_written,
            "truncated": self.truncated,
            "max_buffer_bytes": self.max_buffer_bytes,
            "withheld": self.withhold,
            "eof": self.eof,
            "complete": self.complete,
            "reader_alive": self.is_alive,
            "failure": self.failure,
        }
