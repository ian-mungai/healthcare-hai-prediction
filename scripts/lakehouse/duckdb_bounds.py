"""The spill directory and the byte cap of a layer task, applied to every DuckDB connection of its phases.

DuckDB treats the two settings differently, and both differences cost the task its disk if they are ignored. ``temp_directory`` must be given when the
connection is opened: setting it later fails once a query has spilled, even to the same path. ``max_temp_directory_size`` must be set after the connection is
open: given at connect it is read back as set but is not enforced. A connection that gets neither spills beside its database file and without a cap.

``connect`` opens a connection with the directory and sets the cap at once. ``shell_profile`` gives the same two settings to the dbt profile that a phase
writes. The environment variables are those of the task definition (``HAI_DUCKDB_TEMP_DIRECTORY`` and ``HAI_DUCKDB_MAX_TEMP_BYTES``), read by
``ecs_engine.duckdb_temp_settings``: with neither set outside a task nothing is bounded, and inside a task a missing one is an error."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.lakehouse.ecs_engine import duckdb_temp_settings

CAP_SENTENCE = "max_temp_directory_size"


def options(extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """The connect options of a phase: the memory limit and threads of the allocation, the spill directory and then ``extra``."""
    found: dict[str, Any] = {
        name: value for name, value in (("memory_limit", os.environ.get("DUCKDB_MEMORY_LIMIT")), ("threads", os.environ.get("JOB_THREADS"))) if value
    }
    spill = duckdb_temp_settings().get("temp_directory")
    if spill:
        found["temp_directory"] = spill
    found.update(extra or {})
    return found


def apply_cap(connection: Any) -> None:
    """Enforce the byte cap on an open connection. Every cursor of the connection shares it."""
    cap = duckdb_temp_settings().get("max_temp_directory_size")
    if cap:
        connection.execute("SET max_temp_directory_size = ?", [cap])


def connect(duckdb: Any, database: str | Path | None = None, *, read_only: bool = False, config: Mapping[str, Any] | None = None) -> Any:
    """Open a DuckDB connection with the task's spill directory and byte cap. ``config`` is added to the options of the allocation and wins over them.

    The directory goes in before an option that locks external access, because DuckDB refuses to change it afterwards."""
    given = dict(config or {})
    locked = {name: given.pop(name) for name in ("enable_external_access",) if name in given}
    connection = duckdb.connect(str(database) if database is not None else ":memory:", read_only=read_only, config={**locked, **options(given)})
    apply_cap(connection)
    return connection


def shell_profile(database: Path, locked: bool) -> str:
    """The text of the ``profiles.yml`` that a phase writes for its own dbt run: one DuckDB target ``shell`` on a file.

    ``locked`` turns external access off, and it comes first among the connect options because DuckDB refuses to change the spill directory once access is
    locked. In a task the directory is a connect option and the byte cap is a setting that the adapter runs on every new cursor (the cap is not enforced at
    connect, and a directory set later fails after a spill). The memory limit and threads of the job's allocation are settings too when the job sets them.
    Outside a task nothing is bounded."""
    found = duckdb_temp_settings()
    connect_options = (["enable_external_access: false"] if locked else []) + ([f"temp_directory: {json.dumps(found['temp_directory'])}"] if found else [])
    settings = []
    if os.environ.get("DUCKDB_MEMORY_LIMIT"):
        settings.append(f"memory_limit: {json.dumps(os.environ['DUCKDB_MEMORY_LIMIT'])}")
    if os.environ.get("JOB_THREADS"):
        settings.append(f"threads: {int(os.environ['JOB_THREADS'])}")
    if found:
        settings.append(f"max_temp_directory_size: {json.dumps(found['max_temp_directory_size'])}")
    text = f"hai:\n  target: shell\n  outputs:\n    shell:\n      type: duckdb\n      path: {json.dumps(str(database))}\n      threads: 1\n"
    for key, entries in (("config_options", connect_options), ("settings", settings)):
        if entries:
            text += f"      {key}:\n" + "".join(f"        {entry}\n" for entry in entries)
    return text


def cap_reached(text: str) -> bool:
    """Whether the output of a phase says that DuckDB stopped at the byte cap. DuckDB names the setting only when the cap is what stopped the query."""
    return "This limit was set by the" in text and CAP_SENTENCE in text
