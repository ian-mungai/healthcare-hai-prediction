"""Validate the silver build file with Great Expectations before it is published (silver step 6).

Runs inside the quality image (``hai-quality``), started by ``scripts/lakehouse/silver.py validate``. It opens the build
file read-only, checks the build marker, compiles the committed JSON suites into Great Expectations objects in an
ephemeral context, validates every table in the database (SQL pushdown) and writes one result with counts only: no
data value leaves the build file. Failure modes 639 to 651 and 713 in plans/silver_processed_zone_20261009/plan.md.

    python -m scripts.lakehouse.silver_quality --database BUILD --suites DIR --baselines FILE --output FILE
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.execution_engine import sqlalchemy_execution_engine as engine_module

# The connection fix below patches a private constant, so it is applied only to the version the spike proved [713].
PINNED_GX = "1.23.2"
MARKER = "int_silver_build_marker"
SEVERITIES = ("structural", "distribution")
NUMERIC_RESULT_KEYS = ("element_count", "unexpected_count", "unexpected_percent", "missing_count", "observed_value")
# A baseline value is written <<name>> in a suite; braces would collide with Great Expectations' {batch} formatting.
PLACEHOLDER = re.compile(r"<<([a-z0-9_.]+)>>")


def engine_config() -> dict[str, str]:
    """DuckDB's memory and thread limits from the launch plan, the same settings a dbt build gets [533]."""
    settings = {"memory_limit": os.environ.get("DUCKDB_MEMORY_LIMIT", ""), "threads": os.environ.get("JOB_THREADS", "")}
    return {name: value for name, value in settings.items() if value}


class QualityError(RuntimeError):
    """A run that cannot validate: a wrong version, a missing marker or an unreadable suite."""


class DuckDBDialect(str):
    """A dialect name Great Expectations reads as its enum member, so it keeps one connection for DuckDB [713]."""

    value = "duckdb"


def keep_connection() -> None:
    """Make Great Expectations fetch DuckDB results before it closes the connection (spike finding 1) [713]."""
    if gx.__version__ != PINNED_GX or not hasattr(engine_module, "_PERSISTED_CONNECTION_DIALECTS"):
        raise QualityError(f"GX version or engine changed ({gx.__version__}): re-run the step 6.2 spike before validating")
    dialects = engine_module._PERSISTED_CONNECTION_DIALECTS
    if "duckdb" not in dialects:
        engine_module._PERSISTED_CONNECTION_DIALECTS = (*dialects, DuckDBDialect("duckdb"))


def read_marker(database: Path) -> dict[str, str]:
    """Return the build marker, read-only; without it there is no finished build to validate [647]."""
    with duckdb.connect(str(database), read_only=True, config=engine_config()) as con:
        tables = {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        if MARKER not in tables:
            raise QualityError(f"no {MARKER} table: the build did not finish, so it is not validated")
        rows = con.table(MARKER).project("dbt_invocation_id, git_revision, duckdb_version").fetchall()
        version = con.execute("SELECT version()").fetchone()
    if len(rows) != 1:
        raise QualityError(f"{MARKER} must hold exactly one row")
    marker = dict(zip(("dbt_invocation_id", "git_revision", "duckdb_version"), rows[0], strict=True))
    if version is None or marker["duckdb_version"] != version[0]:
        raise QualityError(f"the build was written by DuckDB {marker['duckdb_version']}, this image reads with {version}")  # [646]
    return marker


def fingerprints(database: Path, tables: list[str]) -> dict[str, dict[str, Any]]:
    """Row count and an order-independent content hash per table, so a result binds the exact build it read [639]."""
    out = {}
    with duckdb.connect(str(database), read_only=True, config=engine_config()) as con:
        for table in tables:
            count, digest = con.table(table).set_alias("t").aggregate("count(*), bit_xor(hash(t))").fetchone() or (0, None)
            out[table] = {"rows": count, "fingerprint": str(digest)}
    return out


def resolve(value: Any, baselines: dict[str, Any]) -> Any:
    """Replace <<name>> placeholders with baseline values; an unknown name stops the run [644]."""
    if isinstance(value, dict):
        return {key: resolve(item, baselines) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve(item, baselines) for item in value]
    if not isinstance(value, str):
        return value
    whole = PLACEHOLDER.fullmatch(value)
    if whole:
        if whole[1] not in baselines:
            raise QualityError(f"baseline {whole[1]} is not in baselines.json")
        return baselines[whole[1]]

    def text(match: re.Match[str]) -> str:
        if match[1] not in baselines:
            raise QualityError(f"baseline {match[1]} is not in baselines.json")
        return str(baselines[match[1]])

    return PLACEHOLDER.sub(text, value)


def load_suites(folder: Path, baselines: dict[str, Any]) -> list[dict[str, Any]]:
    """Read every suite file; each expectation names its table, Great Expectations class, arguments and severity."""
    entries = []
    for path in sorted(folder.glob("*.json")):
        suite = json.loads(path.read_text())
        for item in suite["expectations"]:
            if set(item) != {"id", "table", "type", "kwargs", "severity"} or item["severity"] not in SEVERITIES:
                raise QualityError(f"{path.name}: {item.get('id')} needs exactly id, table, type, kwargs and a known severity")
            if not hasattr(gxe, item["type"]):
                raise QualityError(f"{path.name}: unknown expectation type {item['type']}")
            entries.append({**item, "suite": suite["name"], "kwargs": resolve(item["kwargs"], baselines)})
    ids = [entry["id"] for entry in entries]
    if len(ids) != len(set(ids)):
        raise QualityError("expectation ids repeat across suites")
    return entries


def counts_only(result: dict[str, Any]) -> dict[str, Any]:
    """Keep numbers only; lists of values and samples never leave the build file [643]."""
    kept = {}
    for key in NUMERIC_RESULT_KEYS:
        value = result.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        kept[key] = value
    return kept


def validate(database: Path, suites: Path, baselines_path: Path) -> dict[str, Any]:
    """Validate every expectation and return the run record."""
    keep_connection()
    marker = read_marker(database)
    baselines_doc = json.loads(baselines_path.read_text())
    entries = load_suites(suites, baselines_doc["values"])
    tables = sorted({entry["table"] for entry in entries})
    # Taken before Great Expectations opens the file: it keeps its own connection for the whole run.
    read = fingerprints(database, tables)
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_sql(
        name="silver_build", connection_string=f"duckdb:///{database}", kwargs={"connect_args": {"read_only": True, "config": engine_config()}}
    )
    batches = {table: source.add_table_asset(name=table, table_name=table).add_batch_definition_whole_table("all").get_batch() for table in tables}
    approved = baselines_doc.get("approved") is True
    results = []
    for entry in entries:
        expectation = getattr(gxe, entry["type"])(**entry["kwargs"])
        outcome = batches[entry["table"]].validate(expectation)
        raised = bool(outcome.exception_info and outcome.exception_info.get("raised_exception")) or any(
            isinstance(info, dict) and info.get("raised_exception") for info in (outcome.exception_info or {}).values()
        )
        # A distribution band only warns until the owner approves the baselines (decision D6) [645].
        level = "pass" if outcome.success and not raised else ("warn" if entry["severity"] == "distribution" and not approved else "fail")
        results.append(
            {
                "id": entry["id"],
                "suite": entry["suite"],
                "table": entry["table"],
                "type": entry["type"],
                "severity": entry["severity"],
                "level": level,
                "raised_exception": raised,
                **counts_only(outcome.result),
            }
        )
    return {
        "validated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "great_expectations": gx.__version__,
        "duckdb": duckdb.__version__,
        "marker": marker,
        "baselines": {"revision": baselines_doc.get("revision"), "approved": approved},
        "tables": read,
        "expectations": results,
        "counts": {level: sum(1 for item in results if item["level"] == level) for level in ("pass", "warn", "fail")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--suites", type=Path, required=True)
    parser.add_argument("--baselines", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        record = validate(args.database, args.suites, args.baselines)
    except QualityError as error:
        args.output.write_text(json.dumps({"error": str(error)}, indent=2) + "\n")
        sys.stderr.write(f"silver quality: stopped: {error}\n")
        return 2
    args.output.write_text(json.dumps(record, indent=2) + "\n")
    counts = record["counts"]
    sys.stdout.write(f"silver quality: {counts['pass']} passed, {counts['warn']} warned, {counts['fail']} failed\n")
    # Any structural failure stops the publish [645].
    return 1 if counts["fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
