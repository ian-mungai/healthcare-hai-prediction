"""Redact supported secret spellings before emitting streams or accepting files."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from urllib.parse import quote, quote_plus

from .integrity import StageError


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
