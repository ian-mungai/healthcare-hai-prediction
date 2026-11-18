"""The two values of a bronze run that change at dispatch, and the one check that both the launch preflight and the job driver apply to them."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

NAMESPACES = {"real": ("bronze", "bronze_dictionary"), "qualify": ("bronze_e2e", "bronze_dictionary_e2e")}
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")
SKIPPED_REASON = "no source object"


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


def check_skipped(skipped: object, tables: Sequence[str], target: str, collections: Mapping[str, str]) -> tuple[str, ...]:
    """Accept the tables a run leaves out (``skipped_tables`` of the staged request) only as distinct names of the table map that the run does not request.

    The stage leaves a table out when no source object matches it, and only a real run does that: a qualification run takes the tables of one collection
    and leaves none out.
    """
    if not isinstance(skipped, list) or not all(isinstance(name, str) for name in skipped):
        raise DispatchError("the skipped tables must be a list of table names")
    if any(not IDENTIFIER.fullmatch(name) for name in skipped):
        raise DispatchError("skipped table names are lowercase identifiers")
    if len(set(skipped)) != len(skipped):
        raise DispatchError("the skipped table list repeats a table")
    unknown = [name for name in skipped if name not in collections]
    if unknown:
        raise DispatchError(f"{unknown[0]} is not a table of the table map, so it cannot be skipped")
    requested = [name for name in skipped if name in tables]
    if requested:
        raise DispatchError(f"{requested[0]} is both requested and skipped")
    if skipped and target != "real":
        raise DispatchError("only a real run skips tables")
    return tuple(skipped)
