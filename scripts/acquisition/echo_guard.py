"""One check of a publisher's JSON reply, shared by the BLS, Census and HUD transports, so that no reply that echoes the API key is returned or cached.

``checked_json`` runs before a transport returns the reply bytes, and therefore before any cache write. It refuses a reply when:

- the literal key bytes appear anywhere in it;
- it is not valid JSON, in any encoding the JSON parser detects (UTF-8, UTF-16 or UTF-32); the constants ``NaN``, ``Infinity`` and ``-Infinity``
  are not JSON, so they count as invalid although Python's parser accepts them;
- any object, at any depth, repeats a name. The parser keeps only the last value of a repeated name, so an earlier value (and a key hidden in it with
  JSON escapes) would survive in the cached bytes but never reach a check of the parsed result;
- any decoded object name or decoded string value, at any depth, contains the key. A key made only of characters that a number can hold is also
  compared with every number token (that scan runs only then, because it slows the parser). The shared key reader already refuses such a key, so
  this comparison is a second line of defence for a key from another source; it catches only a token that spells the key the same way.

Every refusal is fixed text with the source label. It never holds the key, a name, a value, an offset or parser text, and it has no exception context.
The check never changes the reply: it returns only the parsed value for the caller's shape checks.

This module imports only the standard library, so the collector code fingerprints that hash the package also cover it.
"""

from __future__ import annotations

import json
from typing import Any

NUMBER_CHARACTERS = frozenset("0123456789+-.eE")


class _RepeatedName(Exception):
    """Raised inside the parser hook; never leaves this module."""


class _NumberEcho(Exception):
    """Raised inside the parser hook; never leaves this module."""


def checked_json(raw: bytes, credential: str, source: str) -> Any:
    """The parsed JSON of ``raw`` once it passed every check; otherwise a ``ValueError`` with fixed text naming only ``source``."""
    echoed = f"{source} response echoed credential; nothing persisted"
    if not credential or credential.encode() in raw:
        raise ValueError(echoed)

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        names = {name for name, _ in items}
        if len(names) != len(items):
            raise _RepeatedName
        return dict(items)

    def integer(token: str) -> int:
        if credential in token:
            raise _NumberEcho
        return int(token)

    def real(token: str) -> float:
        if credential in token:
            raise _NumberEcho
        return float(token)

    def constant(_token: str) -> Any:
        raise ValueError

    spellable = set(credential) <= NUMBER_CHARACTERS
    try:
        parsed = json.loads(
            raw, object_pairs_hook=pairs, parse_int=integer if spellable else None, parse_float=real if spellable else None, parse_constant=constant
        )
    except _RepeatedName:
        raise ValueError(f"{source} response repeats a JSON name; nothing persisted") from None
    except _NumberEcho:
        raise ValueError(echoed) from None
    except (ValueError, RecursionError):
        raise ValueError(f"{source} invalid JSON; response details withheld") from None
    pending: list[Any] = [parsed]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if credential in item:
                raise ValueError(echoed)
        elif isinstance(item, dict):
            for name, value in item.items():
                if credential in name:
                    raise ValueError(echoed)
                pending.append(value)
        elif isinstance(item, list):
            pending.extend(item)
    return parsed
