"""API keys by hash. The service holds the SHA-256 of each key under a short identifier and never the key itself.

The identifier names the caller in the rate limit and in each prediction row. A key is never written to a log, an answer or a row, and a missing,
empty or unknown key is one outcome.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping

KEY_ID = re.compile(r"[a-z][a-z0-9_]{0,31}")
HASH = re.compile(r"[0-9a-f]{64}")
MAX_KEY_LENGTH = 256


class KeyRingInvalid(ValueError):
    """The key configuration is malformed (the message names the identifier, never a hash)."""


def hash_key(presented: str) -> str:
    return hashlib.sha256(presented.encode()).hexdigest()


class KeyRing:
    def __init__(self, hashes: Mapping[str, str]) -> None:
        if not hashes:
            raise KeyRingInvalid("the key ring holds no key")
        for key_id, digest in hashes.items():
            if not KEY_ID.fullmatch(key_id) or not HASH.fullmatch(digest):
                raise KeyRingInvalid(f"the key entry {key_id[:32]!r} is malformed")
        self._hashes = dict(hashes)

    @classmethod
    def from_hashes(cls, hashes: Mapping[str, str]) -> KeyRing:
        return cls(hashes)

    def __repr__(self) -> str:
        return f"KeyRing({len(self._hashes)} keys)"

    def identify(self, presented: str | None) -> str | None:
        """The identifier of the key, or None. Every stored hash is compared in constant time so the answer time does not name the identifier."""
        if not presented or len(presented) > MAX_KEY_LENGTH:
            return None
        digest = hash_key(presented)
        found = None
        for key_id, stored in self._hashes.items():
            if hmac.compare_digest(digest, stored):
                found = key_id
        return found
