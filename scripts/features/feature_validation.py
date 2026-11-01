"""Validate the features build before any catalog write (the quality image: DuckDB on a private copy of the build file, no network).

    python -m scripts.features.feature_validation --database <staging.duckdb> --output <result.json> [--schema <feature_schema.json>] [--contract <json>]

It is reached as ``pipeline_quality.py --kind features`` and takes the arguments the launcher's validate renderer gives every kind (``--suites`` names
the suite folder, whose unexpected-rows expectations run as checks; ``--baselines`` is accepted and ignored). The checks of design 1.3, each an expectation of
the result record:

1. receipt: every model and test node of the build ended ``success`` or ``pass``, and every published model was built;
2. columns: the names and types of each wide table equal ``feature_schema.json`` in order; the inputs table has its fixed columns;
2b. event time: every wide table's ``event_timestamp`` equals the window start of its spine row at 00:00 UTC, null included;
3. pivot: unpivoting each wide table with the schema's mapping gives exactly the long table's value after the projection's masks, compared null-safe
   (``is not distinct from``), and the long table's key set equals the schema's keys times the spine. This catches a zero fill, a dropped control and a
   value under the wrong column;
4. grain: each wide table has exactly one row per spine key, no key missing or extra, none null;
5. kinds: a category-only column whose long rows carry a number fails; text under a number kind is counted, not failed;
6. types: no ``date`` column and no 32-bit integer in a table Feast reads;
7. masks: no attribute and no county or ZIP value survives without proven pre-window provenance, recomputed from the own POS snapshot and independent of the
   ``*_source`` labels the projection uses.

Also: merge keys are unique and not null in every published table, and the inputs table names the schema file that this validation read. The record holds
table and check names and counts only, never a row value. The result carries ``build_file_sha256`` of the private copy; the launcher accepts the result only
when that digest is the sealed copy's.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib
import json
import shutil
import sys
import tempfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from scripts.features import feature_schema
from scripts.lakehouse import duckdb_bounds

DEFAULT_SCHEMA = "/workspace/data_contracts/features/feature_schema.json"
DEFAULT_CONTRACT = "/workspace/data_contracts/lakehouse/features_publish.json"
SPINE = "int_hospital_spine"
WINDOW_TABLE = feature_schema.WINDOW_TABLE
INPUTS = "feature_build_inputs"
RECEIPT = "feature_build_receipt"
OWN = "validation_own_snapshot"
INPUT_COLUMNS = (
    ("build_key", "varchar"),
    ("features_run_id", "varchar"),
    ("gold_run_id", "varchar"),
    ("gold_snapshot_set", "varchar"),
    ("gold_snapshot_set_sha256", "varchar"),
    ("source_sha256", "varchar"),
    ("feature_schema_sha256", "varchar"),
    ("hai_outcome_controls_sha256", "varchar"),
)
FORBIDDEN_TYPES = ("date", "integer", "int", "smallint", "tinyint")
MASK_OK = {
    "none": "true",
    "county_family": "(s.county_source = 'pos_own' and s.planning_region_fips is null) is true",
    "ruca_zip": "(s.zip_code is not null and s.zip_code = o.zip5) is true",
    "own_snapshot_state": "s.state_source = 'pos_own'",
    "own_snapshot_county": "s.county_source = 'pos_own'",
    "own_snapshot_classification": "s.classification_source = 'pos_own'",
}
OWN_COLUMNS = ("state_code", "county_fips", "provider_subtype_code", "control_type_code")
Record = list[dict[str, Any]]


class QualityError(Exception):
    """The build file cannot be validated at all (missing, unreadable or changed while read)."""


def quote(name: str) -> str:
    """A DuckDB identifier; the names come from the committed schema, and a quote in one is refused."""
    if '"' in name or "\x00" in name:
        raise QualityError("a schema name holds a quote")
    return f'"{name}"'


def literal(text: str) -> str:
    """A DuckDB string literal."""
    return "'" + text.replace("'", "''") + "'"


def sha256_of(path: Path) -> str:
    """SHA-256 of a file, in blocks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


@contextlib.contextmanager
def private_copy(database: Path) -> Iterator[tuple[Path, str]]:
    """Copy the build file into a folder that only this container can reach and yield the copy with its SHA-256; a copy that changed raises."""
    with tempfile.TemporaryDirectory(prefix="features_build_") as folder:
        copy = Path(folder) / database.name
        try:
            shutil.copyfile(database, copy)
            copy.chmod(0o400)
            digest = sha256_of(copy)
        except OSError as error:
            raise QualityError(f"the build file cannot be copied and hashed: {type(error).__name__}") from error
        yield copy, digest
        if sha256_of(copy) != digest:
            raise QualityError("the private copy of the build file changed while it was validated")


COUNT_ROWS = "select count(*) from {table}"
COUNT_NULL_KEYS = "select count(*) from {table} where {nulls}"
COUNT_DISTINCT = "select count(*) from (select distinct {keys} from {table})"
COUNT_NULL_SPINE = "select count(*) from {table} where spine_key is null"
COUNT_DISTINCT_SPINE = "select count(distinct spine_key) from {table}"
COUNT_EVENT_TIME = (
    "select count(*) from {table} as w inner join int_hospital_spine as s on w.spine_key = s.spine_key "
    "where w.event_timestamp is distinct from (s.window_start::timestamp) at time zone 'UTC'"
)
COUNT_UNMATCHED = (
    "select count(*) from {table} as w full outer join (select spine_key from int_hospital_spine) as s on w.spine_key = s.spine_key "
    "where w.spine_key is null or s.spine_key is null"
)
COUNT_WINDOW_MISMATCH = (
    "select count(*) from {table} as w inner join int_hospital_spine as s on w.spine_key = s.spine_key where w.{column} is distinct from {expected}"
)
DISTINCT_KEYS = "select distinct {keys} from {table}"
KIND_COUNTS = "select {keys}, count({number}), count({text}) from {table} group by {keys}"
OWN_MISMATCH = (
    "select count(*) from {table} as w left join validation_own_snapshot as o on w.spine_key = o.spine_key "
    "where w.{column} is not null and w.{column} is distinct from o.{own}"
)
COUNTY_PROVENANCE = (
    "select count(*) from {table} as w inner join int_hospital_spine as s on w.spine_key = s.spine_key "
    "left join validation_own_snapshot as o on w.spine_key = o.spine_key "
    "where ({present}) and (o.county_fips is not null and s.county_fips = o.county_fips and s.planning_region_fips is null) is not true"
)
ZIP_PROVENANCE = (
    "select count(*) from {table} as w inner join int_hospital_spine as s on w.spine_key = s.spine_key "
    "left join validation_own_snapshot as o on w.spine_key = o.spine_key where ({present}) and (o.zip5 is not null and s.zip_code = o.zip5) is not true"
)
PIVOT_COMPARE = (
    "with expected as (select l.spine_key, m.col, case when {masks} then {chosen} end as value from {long_table} as l "
    "inner join pivot_map as m on {joined} inner join int_hospital_spine as s on l.spine_key = s.spine_key "
    "left join validation_own_snapshot as o on l.spine_key = o.spine_key), "
    "actual as (select * from (select spine_key, {names} from {table}) unpivot include nulls (value for col in ({names}))) "
    "select count(*) from expected as e full outer join actual as a on e.spine_key = a.spine_key and e.col = a.col "
    "where e.spine_key is null or a.spine_key is null or e.value is distinct from a.value"
)
RECEIPT_ROWS = "select resource_type, status, node from feature_build_receipt"
INPUTS_ROWS = "select count(*) from feature_build_inputs"
INPUTS_SAME = "select count(*) from feature_build_inputs where build_key = 'current' and feature_schema_sha256 = ?"
INPUTS_CONTROL_DIGEST = "select count(*) from feature_build_inputs where regexp_full_match(hai_outcome_controls_sha256, '[a-f0-9]{64}')"
SUITE_COUNT = "select count(*) from ({query})"
CREATE_MAP = "create temp table pivot_map (col varchar, mask varchar, value_column varchar, {columns})"
INSERT_MAP = "insert into pivot_map values (?, ?, ?, {marks})"


class Checker:
    """One validation: a read-only connection, the schema and the record being written."""

    def __init__(self, connection: Any, schema: dict[str, Any], contract: dict[str, Any], schema_sha: str) -> None:
        self.con = connection
        self.schema = schema
        self.contract = contract
        self.schema_sha = schema_sha
        self.record: Record = []
        self.tables = {row[0] for row in connection.execute("select table_name from information_schema.tables where table_schema = 'main'").fetchall()}

    def expect(self, check: str, table: str, ok: bool, **detail: Any) -> None:
        """One expectation: ``pass`` or ``fail``, with counts only."""
        self.record.append({"id": check, "table": table, "level": "pass" if ok else "fail", "detail": detail})

    def scalar(self, sql: str, parameters: list[Any] | None = None) -> int:
        """One integer from a query."""
        row = self.con.execute(sql, parameters or []).fetchone()
        return int(row[0])

    def count(self, template: str, **parts: str) -> int:
        """One integer from a query template whose identifiers were quoted by the caller."""
        return self.scalar(template.format(**parts))

    def columns_of(self, table: str) -> list[tuple[str, str]]:
        """The ordered (name, lower-case type) pairs of a table."""
        rows = self.con.execute(
            "select column_name, lower(data_type) from information_schema.columns where table_schema = 'main' and table_name = ? order by ordinal_position",
            [table],
        ).fetchall()
        return [(str(name), str(kind)) for name, kind in rows]

    def receipt(self) -> None:
        """Check 1: the build receipt table."""
        if RECEIPT not in self.tables:
            self.expect("receipt", RECEIPT, False, reason="no receipt table")
            return
        rows = self.con.execute(RECEIPT_ROWS).fetchall()
        bad = [row for row in rows if row[1] not in ("success", "pass")]
        built = {str(row[2]).split(".")[-1] for row in rows if row[0] == "model"}
        wanted = {name for name in self.contract["publish"]["tables"] if name != INPUTS}
        missing = sorted(wanted - built)
        self.expect("receipt", RECEIPT, bool(rows) and not bad and not missing, nodes=len(rows), nonpassing=len(bad), models_missing=len(missing))

    def inputs(self) -> None:
        """Require the fixed input columns, one row, the matching schema digest and a SHA-256 outcome identity."""
        present = INPUTS in self.tables
        rows = self.scalar(INPUTS_ROWS) if present else 0
        columns_ok = self.columns_of(INPUTS) == list(INPUT_COLUMNS)
        same = self.scalar(INPUTS_SAME, [self.schema_sha]) if columns_ok else 0
        controls = self.scalar(INPUTS_CONTROL_DIGEST) if columns_ok else 0
        self.expect("inputs_columns", INPUTS, columns_ok)
        self.expect("inputs_hai_outcome_digest", INPUTS, rows == 1 and controls == 1, valid_digests=controls)
        self.expect("inputs_one_row_with_schema_digest", INPUTS, rows == 1 and same == 1, rows=rows, schema_digest_equal=same)

    def merge_keys(self) -> None:
        """Every published table has its merge key columns, not null and unique."""
        for name, entry in sorted(self.contract["publish"]["tables"].items()):
            if name not in self.tables:
                self.expect("merge_key", name, False, reason="table missing")
                continue
            key = entry["merge_key"]
            if not set(key) <= {column for column, _ in self.columns_of(name)}:
                self.expect("merge_key", name, False, reason="key column missing")
                continue
            table = quote(name)
            rows = self.count(COUNT_ROWS, table=table)
            nulls = self.count(COUNT_NULL_KEYS, table=table, nulls=" or ".join(f"{quote(column)} is null" for column in key))
            distinct = self.count(COUNT_DISTINCT, table=table, keys=", ".join(quote(column) for column in key))
            self.expect("merge_key", name, nulls == 0 and distinct == rows, rows=rows, null_keys=nulls, repeated=rows - distinct)

    def suites(self, folders: list[Path]) -> None:
        """Run the unexpected-rows expectations of the suite folders (``data_contracts/great_expectations/features``): a query that returns a row fails."""
        for folder in folders:
            for path in sorted(folder.glob("*.json")):
                for entry in json.loads(path.read_text())["expectations"]:
                    name, table = str(entry["id"]), str(entry["table"])
                    if entry["type"] != "UnexpectedRowsExpectation":
                        self.expect(f"suite:{name}", table, False, reason="unsupported expectation type")
                    elif table not in self.tables:
                        self.expect(f"suite:{name}", table, False, reason="table missing")
                    else:
                        query = str(entry["kwargs"]["unexpected_rows_query"]).replace("{batch}", f"main.{quote(table)}")
                        rows = self.count(SUITE_COUNT, query=query)
                        self.expect(f"suite:{name}", table, rows == 0, unexpected_rows=rows)

    def wide_columns(self, name: str, table: dict[str, Any]) -> bool:
        """Checks 2 and 6 for one wide table."""
        built = self.columns_of(name)
        wanted = [(column["name"], column["type"]) for column in table["columns"]]
        forbidden = [column for column, kind in built if kind in FORBIDDEN_TYPES]
        same = built == wanted
        self.expect("columns_equal_schema", name, same, built=len(built), schema=len(wanted), different=len(set(built) ^ set(wanted)))
        self.expect("no_date_or_32bit_integer", name, not forbidden, columns=len(forbidden))
        return same

    def grain(self, name: str) -> None:
        """Check 4: one row per spine key."""
        quoted = quote(name)
        rows = self.count(COUNT_ROWS, table=quoted)
        nulls = self.count(COUNT_NULL_SPINE, table=quoted)
        distinct = self.count(COUNT_DISTINCT_SPINE, table=quoted)
        unmatched = self.count(COUNT_UNMATCHED, table=quoted)
        self.expect(
            "one_row_per_spine_key",
            name,
            nulls == 0 and distinct == rows and unmatched == 0,
            rows=rows,
            null_keys=nulls,
            repeated=rows - distinct,
            unmatched=unmatched,
        )

    def event_times(self, name: str) -> None:
        """Every wide table's event time is its window start at 00:00 UTC, null included: Feast joins on it, so a shifted or missing time loses the row."""
        wrong = self.count(COUNT_EVENT_TIME, table=quote(name))
        self.expect("event_time_equals_window_start", name, wrong == 0, rows_with_other_event_time=wrong)

    def window_table(self, name: str, table: dict[str, Any]) -> None:
        """The hospital-window table: each column equals the spine column after its mask and cast."""
        wrong = 0
        for column in table["columns"]:
            if column["role"] == "key":
                continue
            if column["role"] == "event_time":
                expected = "(s.window_start::timestamp) at time zone 'UTC'"
            else:
                expected = f"s.{quote(column['pivot']['copy'])}"
                if column["type"] == "bigint":
                    expected += "::bigint"
                if column["mask"] != "none":
                    expected = f"case when {MASK_OK[column['mask']]} then {expected} end"
            wrong += self.count(COUNT_WINDOW_MISMATCH, table=quote(name), column=quote(column["name"]), expected=expected)
        self.expect("pivot_round_trip", name, wrong == 0, mismatching_values=wrong)

    def key_set(self, name: str, table: dict[str, Any]) -> None:
        """The long table's key set equals the schema's keys, and each key has one row per spine key."""
        match = table["match_columns"]
        long_table = quote(table["long_table"])
        rows = self.con.execute(DISTINCT_KEYS.format(keys=", ".join(quote(c) for c in match), table=long_table)).fetchall()
        found = {tuple(str(value) for value in row) for row in rows}
        wanted = {tuple(column["pivot"]["match"][c] for c in match) for column in table["columns"] if column["pivot"] is not None}
        spine_rows = self.count(COUNT_ROWS, table=SPINE)
        long_rows = self.count(COUNT_ROWS, table=long_table)
        self.expect(
            "long_key_set_equals_schema",
            name,
            found == wanted and long_rows == spine_rows * len(wanted),
            long_keys=len(found),
            schema_keys=len(wanted),
            extra=len(found - wanted),
            missing=len(wanted - found),
            long_rows=long_rows,
            expected_rows=spine_rows * len(wanted),
        )

    def pivot(self, name: str, table: dict[str, Any]) -> None:
        """Check 3: unpivot the wide table and compare it null-safe with the long table's values after the masks."""
        match = table["match_columns"]
        wrong = 0
        for kind, sql_type in (("number", "double"), ("category", "varchar")):
            mapped = [column for column in table["columns"] if column["pivot"] is not None and column["pivot"]["kind"] == kind]
            if not mapped:
                continue
            self.con.execute("drop table if exists pivot_map")
            self.con.execute(CREATE_MAP.format(columns=", ".join(f"m{i} varchar" for i in range(len(match)))))
            self.con.executemany(
                INSERT_MAP.format(marks=", ".join("?" for _ in match)),
                [[c["name"], c["mask"], c["pivot"]["value_column"], *[c["pivot"]["match"][m] for m in match]] for c in mapped],
            )
            values = sorted({c["pivot"]["value_column"] for c in mapped})
            chosen = "case m.value_column " + " ".join(f"when {literal(v)} then l.{quote(v)}::{sql_type}" for v in values) + " end"
            masks = "case m.mask " + " ".join(f"when {literal(mask)} then {MASK_OK[mask]}" for mask in sorted({c["mask"] for c in mapped})) + " end"
            joined = " and ".join(f"l.{quote(c)} = m.m{i}" for i, c in enumerate(match))
            wrong += self.count(
                PIVOT_COMPARE,
                masks=masks,
                chosen=chosen,
                long_table=quote(table["long_table"]),
                joined=joined,
                names=", ".join(quote(c["name"]) for c in mapped),
                table=quote(name),
            )
        self.expect("pivot_round_trip", name, wrong == 0, mismatching_values=wrong)

    def kinds(self, name: str, table: dict[str, Any]) -> None:
        """Check 5: kind against the long table's data. A category-only key whose rows carry a number fails; text under a number-only key is counted."""
        values = table["value_columns"]
        if values is None:
            return
        match = table["match_columns"]
        wanted: dict[tuple[str, ...], set[str]] = {}
        for column in table["columns"]:
            pivot = column["pivot"]
            if pivot is not None:
                wanted.setdefault(tuple(pivot["match"][c] for c in match), set()).add(pivot["kind"])
        keys = ", ".join(quote(c) for c in match)
        rows = self.con.execute(
            KIND_COUNTS.format(keys=keys, number=quote(values["number"]), text=quote(values["category"]), table=quote(table["long_table"]))
        ).fetchall()
        width = len(match)
        failed = counted = 0
        for row in rows:
            kinds = wanted.get(tuple(str(value) for value in row[:width]), set())
            has_number, has_text = int(row[width]) > 0, int(row[width + 1]) > 0
            failed += kinds == {"category"} and has_number
            counted += kinds == {"number"} and has_text and not has_number
        self.expect("category_has_no_number", name, failed == 0, category_keys_with_numbers=failed, text_under_number_kind=counted)

    def masks(self, name: str, table: dict[str, Any]) -> None:
        """Check 7: provenance recomputed from the own POS snapshot, not from the source labels."""
        quoted = quote(name)
        if name == WINDOW_TABLE:
            wrong = sum(self.count(OWN_MISMATCH, table=quoted, column=column, own=column) for column in OWN_COLUMNS)
            self.expect("masks_have_own_snapshot_provenance", name, wrong == 0, attributes_without_provenance=wrong)
            return
        if table["family"] not in ("county_measures", "county_context"):
            return
        wrong = 0
        for mask, template in (("county_family", COUNTY_PROVENANCE), ("ruca_zip", ZIP_PROVENANCE)):
            masked = [c["name"] for c in table["columns"] if c["pivot"] is not None and c["mask"] == mask]
            if masked:
                wrong += self.count(template, table=quoted, present=" or ".join(f"w.{quote(c)} is not null" for c in masked))
        self.expect("masks_have_own_snapshot_provenance", name, wrong == 0, rows_without_provenance=wrong)


def validate(database: Path, schema_path: Path, contract_path: Path, suites: list[Path] | None = None) -> Record:
    """Run every check on a build file (already a private copy) and return the expectations."""
    duckdb: Any = importlib.import_module("duckdb")
    schema = json.loads(schema_path.read_text())
    contract = json.loads(contract_path.read_text())
    connection = duckdb_bounds.connect(duckdb, database, read_only=True)
    try:
        checker = Checker(connection, schema, contract, sha256_of(schema_path))
        checker.receipt()
        checker.inputs()
        checker.merge_keys()
        checker.suites(suites or [])
        needed = {SPINE, OWN, *schema["tables"]}
        for missing in sorted(needed - checker.tables):
            checker.expect("table_exists", missing, False)
        if needed <= checker.tables:
            for name, table in schema["tables"].items():
                if not checker.wide_columns(name, table):
                    continue
                checker.grain(name)
                checker.event_times(name)
                if name == WINDOW_TABLE:
                    checker.window_table(name, table)
                else:
                    checker.key_set(name, table)
                    checker.pivot(name, table)
                    checker.kinds(name, table)
                checker.masks(name, table)
        return checker.record
    finally:
        connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the build file and write the result record; returns 1 when an expectation fails, 2 when the file cannot be validated."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--schema", type=Path, default=Path(DEFAULT_SCHEMA))
    parser.add_argument("--contract", type=Path, default=Path(DEFAULT_CONTRACT))
    parser.add_argument(
        "--suites", type=Path, nargs="*", default=[], help="suite folders of unexpected-rows expectations (data_contracts/great_expectations/features)"
    )
    parser.add_argument("--baselines", help="ignored: the features gate has no baselines")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        with private_copy(args.database) as (copy, digest):
            expectations = validate(copy, args.schema, args.contract, args.suites)
        record: dict[str, Any] = {
            "expectations": expectations,
            "counts": {"pass": sum(e["level"] == "pass" for e in expectations), "warn": 0, "fail": sum(e["level"] == "fail" for e in expectations)},
            "build_file_sha256": digest,
            "feature_schema_sha256": sha256_of(args.schema),
        }
    except (QualityError, OSError, ValueError) as error:
        args.output.write_text(json.dumps({"error": str(error)}, indent=2) + "\n")
        sys.stderr.write(f"features validation: stopped: {error}\n")
        return 2
    args.output.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    counts = record["counts"]
    sys.stdout.write(f"features validation: {counts['pass']} passed, {counts['fail']} failed\n")
    return 1 if counts["fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
