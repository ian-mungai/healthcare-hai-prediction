"""API keys by hash. The service holds the SHA-256 of each key under a short identifier and never the key itself.

The identifier names the caller in the rate limit and in each prediction row. A key is never written to a log, an answer or a row, and a missing,
empty or unknown key is one outcome.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

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


def new_key(key_id: str, folder: Path) -> dict[str, str]:
    """Write a new random key to ``api_key`` in the folder, with no trailing newline and a private mode, and return the hash entry for the key ring.

    The hash is taken from the same text that was written, which is the text a caller presents. An existing key file is never overwritten.
    """
    if not KEY_ID.fullmatch(key_id):
        raise KeyRingInvalid("the key identifier must start with a lowercase letter and use lowercase letters, digits and underscores, at most 32 characters")
    key = secrets.token_hex(24)
    descriptor = os.open(folder / "api_key", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(key)
    return {key_id: hash_key(key)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Make an API key for the model API. The key goes to a file and is never printed.")
    commands = parser.add_subparsers(dest="command", required=True)
    new = commands.add_parser("new", help="write a new key file and print its hash entry")
    new.add_argument("--id", required=True, help="the key identifier, which names the caller in the rate limit and in the rows")
    new.add_argument("--output", type=Path, required=True, help="an existing private folder for the key file")
    args = parser.parse_args(argv)
    try:
        entry = new_key(args.id, args.output)
    except (KeyRingInvalid, OSError) as error:
        sys.stderr.write(f"no key was made: {type(error).__name__}: {error}\n")
        return 2
    sys.stderr.write(f"key written to {args.output / 'api_key'}\n")
    sys.stdout.write(json.dumps(entry) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
