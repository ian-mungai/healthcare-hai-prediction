"""Validate namespace inputs shared by the bronze job entry points."""

from __future__ import annotations

import re

BRONZE_NAMESPACE = "bronze"
DICTIONARY_NAMESPACE = "bronze_dictionary"
SMOKE_NAMESPACE = "smoke_check"
FIXTURE_NAMESPACE = "duckdb_view_check"


def namespace_name(value: str) -> str:
    """Return one lowercase namespace identifier or refuse it before a catalog call."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError("namespace names must be lowercase identifiers")
    return value
