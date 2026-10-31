"""The digest of a version listing, the one rule for the layer driver and the owner's acceptance report.

An entry is a version or a delete marker as ``(kind, key, version ID, entity tag, size)``, with ``V`` for a version and ``D`` for a delete marker. The digest
takes the entries in key order and sorts the lines of one key, so it does not depend on how the bucket paged the listing or on how a key's versions and
markers interleave. It counts every entry. A key out of order is refused, because the digest would then differ between two readings of one listing."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any

Entry = tuple[str, str, str, str, int]


def canonical_digest(prefix: str, entries: Iterable[Entry]) -> dict[str, Any]:
    """The prefix, the number of entries and the SHA-256 of the sorted lines of each key, key after key. The entries are read once and never held whole."""
    digest, count, current, group = hashlib.sha256(), 0, "", []
    for kind, key, version, etag, size in entries:
        if key < current:
            raise ValueError("the listing is not in key order")
        if key != current:
            digest.update("".join(sorted(group)).encode())
            current, group = key, []
        group.append(f"{kind}\t{key}\t{version}\t{etag}\t{size}\n")
        count += 1
    digest.update("".join(sorted(group)).encode())
    return {"prefix": prefix, "versions": count, "sha256": digest.hexdigest()}
