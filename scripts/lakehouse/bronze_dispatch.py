"""The two values of a bronze run that change at dispatch, and the one check that both the broker and the job driver apply to them."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

NAMESPACES = {"real": ("bronze", "bronze_dictionary"), "qualify": ("bronze_e2e", "bronze_dictionary_e2e")}
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")


class DispatchError(ValueError):
    """The tables or the target of a bronze run are not allowed."""


def check_dispatch(tables: str | Sequence[str], target: str, collections: Mapping[str, str]) -> tuple[str, ...]:
    """Accept the target only when it is named, every table only when it is in the table map (``collections`` maps table to collection).

    A qualification run takes the tables of one collection.
    """
    if target not in NAMESPACES:
        raise DispatchError(f"the target must be one of {', '.join(NAMESPACES)}, not {target!r}")
    names = (tables.split(",") if tables else []) if isinstance(tables, str) else list(tables)
    if not names:
        raise DispatchError("at least one table is required")
    if any(not isinstance(name, str) or not IDENTIFIER.fullmatch(name) for name in names):
        raise DispatchError("table names are lowercase identifiers")
    if len(set(names)) != len(names):
        raise DispatchError("the table list repeats a table")
    unknown = [name for name in names if name not in collections]
    if unknown:
        raise DispatchError(f"{unknown[0]} is not a table of the table map")
    if target == "qualify" and len({collections[name] for name in names}) != 1:
        raise DispatchError("a qualification run loads the tables of one collection")
    return tuple(names)
