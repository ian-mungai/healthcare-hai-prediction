"""Redact supported secret spellings before emitting streams or accepting files."""

from __future__ import annotations

import base64
import json
import re
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from urllib.parse import quote, quote_plus

from .integrity import CHUNK, StageError, check_deadline, fingerprint, held_file


def spellings(values: Mapping[str, str]) -> tuple[bytes, ...]:
    """Build plain, URL, JSON and alignment-independent base64 secret spellings."""
    variants: set[bytes] = set()
    for value in values.values():
        if not isinstance(value, str) or not 8 <= len(value) <= 4096:
            raise StageError("secret_length", 75)
        raw = value.encode("utf-8")
        variants.update((raw, quote(value, safe="").encode(), quote_plus(value, safe="").encode()))
        variants.update(json.dumps(value, ensure_ascii=ascii_only)[1:-1].encode() for ascii_only in (False, True))
        variants.add(base64.b64encode(raw))
        variants.add(base64.b64encode(raw).rstrip(b"="))
        for alignment in range(3):
            encoded = base64.b64encode(b"\0" * alignment + raw)
            start = (alignment * 8 + 5) // 6
            end = ((alignment + len(raw)) * 8) // 6
            variants.add(encoded[start:end])
    return tuple(sorted(variants, key=lambda value: (-len(value), value)))


class Redactor:
    """Keep enough original bytes to redact matches split across arbitrary reads."""

    def __init__(self, values: Mapping[str, str]) -> None:
        variants = self.variants = spellings(values)
        self.longest = max(map(len, variants), default=1)
        self.expression = re.compile(b"(?=(" + b"|".join(re.escape(value) for value in variants) + b"))") if variants else None
        self.pending = b""
        self.matched = False

    def _emit(self, final: bool) -> bytes:
        cutoff = len(self.pending) if final else max(0, len(self.pending) - self.longest + 1)
        if not cutoff:
            return b""
        spans: list[tuple[int, int]] = []
        if self.expression is not None and any(variant in self.pending for variant in self.variants):
            for match in self.expression.finditer(self.pending):
                start, end = match.span(1)
                if start >= cutoff and (not spans or start > spans[-1][1]):
                    break
                if spans and start <= spans[-1][1]:
                    spans[-1] = (spans[-1][0], max(spans[-1][1], end))
                else:
                    spans.append((start, end))
        pieces: list[bytes] = []
        cursor = 0
        for start, end in spans:
            pieces.extend((self.pending[cursor:start], b"[REDACTED]"))
            cursor = end
            self.matched = True
        consumed = max(cutoff, cursor)
        pieces.append(self.pending[cursor:consumed])
        self.pending = self.pending[consumed:]
        return b"".join(pieces)

    def feed(self, block: bytes) -> bytes:
        """Return only bytes whose future input cannot change their redaction."""
        self.pending += block
        return self._emit(False)

    def finish(self) -> bytes:
        """Redact and release the final retained bytes when the stream ends."""
        return self._emit(True)


def scan_file(path: Path, values: Mapping[str, str], deadline: float) -> None:
    """Refuse a secret-bearing or concurrently modified file without emitting bytes."""
    import os

    redactor = Redactor(values)
    with held_file(path.parent, path.name) as descriptor:
        before = fingerprint(os.fstat(descriptor))
        offset = 0
        while True:
            check_deadline(deadline)
            block = os.pread(descriptor, CHUNK, offset)
            if not block:
                break
            redactor.feed(block)
            if redactor.matched:
                raise StageError("secret_in_output", 75)
            offset += len(block)
        redactor.finish()
        if redactor.matched:
            raise StageError("secret_in_output", 75)
        if before != fingerprint(os.fstat(descriptor)):
            raise StageError("source_changed", 71)


def rotation_values(
    dispatch: Mapping[str, int], latest: Mapping[str, int], read: Callable[[str, int], str | None], deadline: float | None = None
) -> dict[str, str]:
    """Collect every pinned parameter version or refuse the entire affected scan.

    The supplied reader must keep values memory-only and bound each underlying
    secret-bearing call through the one-shot worker. No response is logged here.
    """
    deadline = time.monotonic() + 120 if deadline is None else deadline
    if set(dispatch) != set(latest):
        raise StageError("secret_scan_incomplete", 75)
    result: dict[str, str] = {}
    for name, first in sorted(dispatch.items()):
        last = latest[name]
        if type(first) is not int or type(last) is not int or first < 1 or last < first:
            raise StageError("secret_scan_incomplete", 75)
        for version in range(first, last + 1):
            check_deadline(deadline)
            value = read(name, version)
            check_deadline(deadline)
            if value is None:
                raise StageError("secret_scan_incomplete", 75)
            spellings({name: value})
            result[f"{name}:{version}"] = value
    return result
