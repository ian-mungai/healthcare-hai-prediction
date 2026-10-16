"""Validate the silver build file with Great Expectations before it is published (silver step 6).

Runs inside the quality image (``hai-quality``), started by ``scripts/lakehouse/silver.py validate``. It copies the build
file to a private temporary folder, reports the copy's SHA-256 in the result and runs every suite on that copy. It opens the
copy read-only, checks the build marker, compiles the committed JSON suites of every folder it is given (the silver
atomic suites and the gold suites) into Great Expectations objects in one ephemeral context, validates every table the
suites name (SQL pushdown) and writes one result with counts only: no data value leaves the build file. A table a suite
names but the build lacks is a structural failure of its expectations, not a crash. A band under ``proposed_values`` in the
baselines file only warns until the owner moves it to ``values``. Failure modes 639 to 651 and 713 in
plans/silver_processed_zone_20261009/plan.md and 853 to 857 in plans/wp/r3_failure_modes.md.

    python -m scripts.lakehouse.silver_quality --database BUILD --suites DIR [DIR ...] --baselines FILE --output FILE
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
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


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


@contextmanager
def private_copy(database: Path) -> Iterator[tuple[Path, str]]:
    """Copy the build file into a folder that only this container can reach and yield the copy with its SHA-256.

    Every suite reads the copy, so the SHA-256 names the bytes that were validated whatever happens to the original afterwards. The copy is hashed again
    when the block ends and a copy that changed raises ``QualityError``.
    """
    with tempfile.TemporaryDirectory(prefix="build_") as folder:
        copy = Path(folder) / database.name
        try:
            shutil.copyfile(database, copy)
            copy.chmod(0o400)
            digest = sha256_of(copy)
        except OSError as error:
            raise QualityError(f"the build file cannot be copied and hashed: {type(error).__name__}") from error
        yield copy, digest
        try:
            unchanged = sha256_of(copy) == digest
        except OSError as error:
            raise QualityError(f"the private copy of the build file cannot be read again: {type(error).__name__}") from error
        if not unchanged:
            raise QualityError("the private copy of the build file changed while it was validated")


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


def resolve(value: Any, baselines: dict[str, Any], used: set[str]) -> Any:
    """Replace <<name>> placeholders with baseline values and record each name used; an unknown name stops the run [644]."""
    if isinstance(value, dict):
        return {key: resolve(item, baselines, used) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve(item, baselines, used) for item in value]
    if not isinstance(value, str):
        return value

    def lookup(name: str) -> Any:
        if name not in baselines:
            raise QualityError(f"baseline {name} is not in baselines.json")
        used.add(name)
        return baselines[name]

    whole = PLACEHOLDER.fullmatch(value)
    if whole:
        return lookup(whole[1])
    return PLACEHOLDER.sub(lambda match: str(lookup(match[1])), value)


def read_baselines(document: dict[str, Any]) -> tuple[dict[str, Any], frozenset[str]]:
    """The approved ``values`` and the ``proposed_values`` awaiting the owner as one lookup, plus the proposed names [856]."""
    approved, proposed = document["values"], document.get("proposed_values", {})
    if both := sorted(set(approved) & set(proposed)):
        raise QualityError(f"baseline names are both approved and proposed: {', '.join(both)}")
    return {**approved, **proposed}, frozenset(proposed)


def load_suites(folders: list[Path], baselines: dict[str, Any], proposed: frozenset[str]) -> list[dict[str, Any]]:
    """Read every suite file of every folder; each expectation names its table, Great Expectations class, arguments and severity.

    ``bands`` lists the proposed baseline names an expectation uses. A proposal can only warn, so a structural expectation may not use one [856].
    """
    entries = []
    for folder in folders:
        files = sorted(folder.glob("*.json"))
        if not files:
            raise QualityError(f"suites folder {folder.name} holds no suite file")
        for path in files:
            suite = json.loads(path.read_text())
            for item in suite["expectations"]:
                if set(item) != {"id", "table", "type", "kwargs", "severity"} or item["severity"] not in SEVERITIES:
                    raise QualityError(f"{path.name}: {item.get('id')} needs exactly id, table, type, kwargs and a known severity")
                if not hasattr(gxe, item["type"]):
                    raise QualityError(f"{path.name}: unknown expectation type {item['type']}")
                used: set[str] = set()
                kwargs = resolve(item["kwargs"], baselines, used)
                bands = sorted(used & proposed)
                if bands and item["severity"] != "distribution":
                    raise QualityError(f"{path.name}: {item['id']} is structural and uses the proposed baseline {bands[0]}")
                entries.append({**item, "suite": suite["name"], "kwargs": kwargs, "bands": bands})
    ids = [entry["id"] for entry in entries]
    if len(ids) != len(set(ids)):
        raise QualityError("expectation ids repeat across suites")
    return entries


def existing_tables(database: Path) -> set[str]:
    """Names of the tables and views in the build file's main schema, read-only."""
    with duckdb.connect(str(database), read_only=True, config=engine_config()) as con:
        return {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()}


def counts_only(result: dict[str, Any]) -> dict[str, Any]:
    """Keep numbers only; lists of values and samples never leave the build file [643]."""
    kept = {}
    for key in NUMERIC_RESULT_KEYS:
        value = result.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        kept[key] = value
    return kept


def table_missing(entry: dict[str, Any]) -> dict[str, Any]:
    """The result of an expectation whose table the build lacks: a structural failure that names the table [853]."""
    return {
        "id": entry["id"],
        "suite": entry["suite"],
        "table": entry["table"],
        "type": entry["type"],
        "severity": entry["severity"],
        "level": "fail",
        "raised_exception": False,
        "table_missing": True,
    }


def validate(database: Path, suites: list[Path], baselines_path: Path) -> dict[str, Any]:
    """Validate every expectation of every suite folder and return the run record."""
    keep_connection()
    marker = read_marker(database)
    baselines_doc = json.loads(baselines_path.read_text())
    values, proposed = read_baselines(baselines_doc)
    entries = load_suites(suites, values, proposed)
    named = sorted({entry["table"] for entry in entries})
    present = existing_tables(database)
    tables = [table for table in named if table in present]
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
        if entry["table"] not in batches:
            results.append(table_missing(entry))
            continue
        try:
            expectation = getattr(gxe, entry["type"])(**entry["kwargs"])
        except ValueError as error:
            # A band whose minimum is above its maximum is a baseline fault, not a failed build [856].
            raise QualityError(f"{entry['id']}: invalid arguments: {str(error).splitlines()[0]}") from error
        outcome = batches[entry["table"]].validate(expectation)
        raised = bool(outcome.exception_info and outcome.exception_info.get("raised_exception")) or any(
            isinstance(info, dict) and info.get("raised_exception") for info in (outcome.exception_info or {}).values()
        )
        # A distribution band warns until the owner approves the baselines (decision D6); a proposed band warns until it joins ``values`` [645] [856].
        warns = entry["severity"] == "distribution" and (not approved or bool(entry["bands"]))
        level = "pass" if outcome.success and not raised else ("warn" if warns else "fail")
        results.append(
            {
                "id": entry["id"],
                "suite": entry["suite"],
                "table": entry["table"],
                "type": entry["type"],
                "severity": entry["severity"],
                "level": level,
                "raised_exception": raised,
                **({"proposed_bands": entry["bands"]} if entry["bands"] else {}),
                **counts_only(outcome.result),
            }
        )
    return {
        "validated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "great_expectations": gx.__version__,
        "duckdb": duckdb.__version__,
        "marker": marker,
        "baselines": {"revision": baselines_doc.get("revision"), "approved": approved, "proposed": len(proposed)},
        "suites": {name: sum(1 for item in results if item["suite"] == name) for name in sorted({item["suite"] for item in results})},
        "missing_tables": [table for table in named if table not in present],
        "tables": read,
        "expectations": results,
        "counts": {level: sum(1 for item in results if item["level"] == level) for level in ("pass", "warn", "fail")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--suites", type=Path, nargs="+", required=True, help="one or more suite folders (silver atomic, gold)")
    parser.add_argument("--baselines", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        with private_copy(args.database) as (copy, digest):
            record = validate(copy, args.suites, args.baselines)
        record["build_file_sha256"] = digest
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
