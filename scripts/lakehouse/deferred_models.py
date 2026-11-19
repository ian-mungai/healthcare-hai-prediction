"""The deferred silver models of the dbt project and the staging tables that follow from them, read from one place: ``vars.deferred_models`` in dbt_project.yml.

``copy_tables`` lists every bronze table whose stored copies silver tracks. A table whose staging view is deferred stays in that list for its lineage readers
(``stg_bronze__file_copies``, the release observations and the copy test) and leaves every list that expects a staging view. The dbt side of that rule is the
macro ``staged_copy_tables``; this module is the same rule for Python code, and ``run_dbt_disabled_nodes_e2e`` proves the two agree on the real project.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

STAGING_PREFIX = "stg_"


def deferred_names(project: Mapping[str, Any]) -> frozenset[str]:
    """The names of the models the deferral record lists. No record, a null record and an empty list mean no deferral."""
    record = (project.get("vars") or {}).get("deferred_models") or []
    return frozenset(name for entry in record for name in entry["models"])


def staged_tables(project: Mapping[str, Any]) -> list[str]:
    """The ``copy_tables`` entries that have a staging view: every one whose ``stg_`` model is not deferred."""
    deferred = deferred_names(project)
    return [table for table in project["vars"]["copy_tables"] if f"{STAGING_PREFIX}{table}" not in deferred]


def staging_models(project: Mapping[str, Any]) -> frozenset[str]:
    """The staging model names a silver build must have: one for each staged table."""
    return frozenset(f"{STAGING_PREFIX}{table}" for table in staged_tables(project))
