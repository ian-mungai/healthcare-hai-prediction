"""Contract-driven gold checks: SQL builders, verdicts and the Great Expectations gold suites (failure modes 838 to 842).

Everything here reads ``data_contracts/lakehouse/gold_contract.json`` (facts, grains, columns, surrogate keys, source dates,
text columns, dimensions and the expression allowlist) and ``silver_columns.json`` (the kept silver models). No fact list is
copied: a new fact in the contract is covered without a change here. The functions build SQL text or judge rows that a
caller fetched; ``scripts/lakehouse/run_gold_e2e.py`` runs the SQL against a build file and reports the verdicts.

    .venv/bin/python -m scripts.lakehouse.gold_checks write-suites [--contract FILE] [--out DIR]
    .venv/bin/python -m scripts.lakehouse.gold_checks check-suites [--contract FILE] [--suites DIR]

The suites run through ``scripts/lakehouse/silver.py validate --suites data_contracts/great_expectations/gold`` (the quality
image, read-only, counts only). The suites use no dbt-equivalent expectation type, so they repeat no dbt test (silver.py's
repeated-test check stays empty).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Collection, Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from string import Template
from typing import Any, NamedTuple

from scripts.lakehouse import gold_sql

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT = REPO_ROOT / "data_contracts/lakehouse/gold_contract.json"
SILVER_COLUMNS = REPO_ROOT / "data_contracts/lakehouse/silver_columns.json"
SILVER_PUBLISH = REPO_ROOT / "data_contracts/lakehouse/silver_publish.json"
SUITES = REPO_ROOT / "data_contracts/great_expectations/gold"
DBT = REPO_ROOT / "dbt"
UNKNOWN = "<unknown>"
NUMERIC_TYPES = frozenset({"DOUBLE", "FLOAT", "BIGINT", "INTEGER", "SMALLINT", "HUGEINT", "TINYINT"})
# One SQL text per query keeps the container's argument length small.
CHUNK_CHARACTERS = 60_000


@dataclass(frozen=True)
class Column:
    """One gold column: its name, the silver column it carries and its type."""

    name: str
    silver_column: str
    type: str


@dataclass(frozen=True)
class SurrogateKey:
    """A surrogate-key column of a fact: the dimension it points at and the natural key it is computed from."""

    name: str
    dimension: str
    natural_key: str


@dataclass(frozen=True)
class Fact:
    """A gold fact as the contract states it."""

    name: str
    silver_model: str
    grain: str
    columns: tuple[Column, ...]
    keys: tuple[SurrogateKey, ...]
    source_dates: tuple[str, ...]
    text_columns: tuple[str, ...]

    @property
    def is_hold(self) -> bool:
        """The contract names hold facts ``fct_*_hold``; they reconcile row for row (failure mode 749)."""
        return self.name.endswith("_hold")

    @property
    def expected_columns(self) -> list[tuple[str, str]]:
        """The gold relation's ordered columns: the contract's columns, then one surrogate key each."""
        return [(column.name, column.type) for column in self.columns] + [(key.name, "VARCHAR") for key in self.keys]


@dataclass(frozen=True)
class Table:
    """A gold dimension or bridge as the contract states it. A bridge always has columns, a grain and its references (a dimension has only its key)."""

    name: str
    kind: str
    grain: str
    columns: tuple[Column, ...]
    inputs: tuple[str, ...] = ()
    grain_columns: tuple[str, ...] = ()
    references: tuple[tuple[str, str], ...] = ()
    nullable_columns: tuple[str, ...] = ()

    @property
    def required_columns(self) -> tuple[str, ...]:
        """Columns that must never be null: a bridge's references where it has them (its vintage columns may be empty), else its grain; a column the
        contract lists as nullable is left out."""
        required = tuple(column for column, _dimension in self.references) or self.grain_columns
        return tuple(column for column in required if column not in self.nullable_columns)

    @property
    def key(self) -> str:
        """A dimension's surrogate-key column: its name without ``dim_``, plus ``_sk`` (for example ``hospital_sk``)."""
        return dimension_key(self.name)


@dataclass(frozen=True)
class Contract:
    """The gold contract with the kept silver models' ordered columns."""

    facts: tuple[Fact, ...]
    tables: tuple[Table, ...]
    allowlist: tuple[str, ...]
    silver: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    expression_policy: dict[str, Any] = field(default_factory=lambda: gold_sql.A36_A39_A43_A48)

    @property
    def dimensions(self) -> tuple[Table, ...]:
        return tuple(table for table in self.tables if table.kind == "dimension")

    @property
    def bridges(self) -> tuple[Table, ...]:
        return tuple(table for table in self.tables if table.kind == "bridge")

    @property
    def model_names(self) -> list[str]:
        return [fact.name for fact in self.facts] + [table.name for table in self.tables]

    @property
    def date_pairs(self) -> frozenset[tuple[str, str]]:
        """(silver model, column) of every DATE source date of a fact: the only inputs of the dimensions' date aggregates (amendment A36 rule 2)."""
        return frozenset(
            (fact.silver_model, column.name) for fact in self.facts for column in fact.columns if column.name in fact.source_dates and column.type == "DATE"
        )

    @property
    def value_columns(self) -> frozenset[str]:
        """Numeric contract columns that are not keys or source dates: the columns arithmetic must never touch (746)."""
        names: set[str] = set()
        for fact in self.facts:
            skip = {fact.grain, *fact.source_dates, *(key.natural_key for key in fact.keys)}
            names |= {column.name for column in fact.columns if column.type in NUMERIC_TYPES and column.name not in skip}
        for table in self.bridges:
            names |= {column.name for column in table.columns if column.type in NUMERIC_TYPES}
        return frozenset(names)

    def kind_of(self, model: str) -> str:
        """``fact``, ``dimension`` or ``bridge``; a model outside the contract is treated as a fact (the strictest)."""
        for table in self.tables:
            if table.name == model:
                return table.kind
        return "fact"


class ContractError(ValueError):
    """The contract is incomplete: a table definition with no columns is an error, never an empty schema (amendment A39)."""


class BridgeSchema(NamedTuple):
    columns: tuple[tuple[str, str], ...]
    grain: tuple[str, ...]
    references: tuple[tuple[str, str], ...]


# Amendment A39 item 1: the schema of bridge_measure_control as G2 built it, until the coordinating agent writes it into gold_contract.json.
A39_BRIDGE_MEASURE_CONTROL = BridgeSchema(
    (
        ("measure_sk", "VARCHAR"),
        ("registry_control_sk", "VARCHAR"),
        ("measure_control", "VARCHAR"),
        ("vintage_start", "VARCHAR"),
        ("vintage_end", "VARCHAR"),
        ("source_seed", "VARCHAR"),
        ("source_row", "INTEGER"),
    ),
    ("measure_sk", "registry_control_sk", "vintage_start", "vintage_end"),
    (("measure_sk", "dim_measure"), ("registry_control_sk", "dim_registry_control")),
)


def bridge_table(name: str, entry: dict[str, Any]) -> Table:
    """A bridge from the contract's columns, or from the A39 constant where the contract still gives only a prose grain."""
    inputs = tuple(entry.get("inputs", []))
    columns = tuple(Column(item["name"], item["silver_column"], item["type"]) for item in entry.get("columns", []))
    if name == "bridge_measure_control":
        schema = A39_BRIDGE_MEASURE_CONTROL
        columns = columns or tuple(Column(column, column, kind) for column, kind in schema.columns)
        grain = tuple(entry.get("grain_columns", schema.grain))
        references = tuple((item["column"], item["dimension"]) for item in entry.get("references", [])) or schema.references
        return Table(name, "bridge", str(entry["grain"]), columns, inputs, grain, references)
    if not columns:
        raise ContractError(f"{name}: the contract lists no columns for this bridge")
    listed = {column.name for column in columns}
    grain = tuple(entry["grain_columns"]) if "grain_columns" in entry else (str(entry["grain"]),)
    if not set(grain) <= listed:
        raise ContractError(f"{name}: grain columns {sorted(set(grain) - listed)} are not among its columns")
    nullable = tuple(entry.get("nullable_columns", ()))
    if not set(nullable) <= listed:
        raise ContractError(f"{name}: nullable columns {sorted(set(nullable) - listed)} are not among its columns")
    references = tuple((item["column"], item["dimension"]) for item in entry.get("references", []))
    return Table(name, "bridge", str(entry["grain"]), columns, inputs, grain, references, nullable)


def dimension_key(dimension: str) -> str:
    return dimension.removeprefix("dim_") + "_sk"


def load_contract(contract_path: Path = CONTRACT, silver_path: Path = SILVER_COLUMNS) -> Contract:
    """Read the gold contract and the silver column contract."""
    document = json.loads(contract_path.read_text())
    silver = {name: [(column, kind) for column, kind in entry["columns"]] for name, entry in json.loads(silver_path.read_text())["models"].items()}
    facts = []
    for name, entry in sorted(document["facts"].items()):
        columns = tuple(Column(item["name"], item["silver_column"], item["type"]) for item in entry["columns"])
        keys = tuple(SurrogateKey(key, item["dimension"], item["natural_key"]) for key, item in entry["surrogate_keys"].items())
        facts.append(Fact(name, entry["silver_model"], entry["grain"], columns, keys, tuple(entry["source_dates"]), tuple(entry["published_text_columns"])))
    tables = []
    for name, entry in sorted(document["dimensions_and_bridges"].items()):
        if name.startswith("dim_"):
            tables.append(Table(name, "dimension", str(entry["grain"]), (), tuple(entry.get("inputs", []))))
        else:
            tables.append(bridge_table(name, entry))
    return Contract(tuple(facts), tuple(tables), tuple(document["expression_allowlist"]), silver, document.get("expression_policy", gold_sql.A36_A39_A43_A48))


def contract_problems(contract: Contract) -> list[str]:
    """Static reading of the contract against silver: each silver value column lands in one gold column (failure mode 744)."""
    problems: list[str] = []
    dimensions = {table.name for table in contract.dimensions}
    for fact in contract.facts:
        silver = contract.silver.get(fact.silver_model)
        if silver is None:
            problems.append(f"{fact.name}: silver model {fact.silver_model} is not a kept silver model")
            continue
        mapped = [(column.silver_column, column.type) for column in fact.columns]
        # Column order in silver_columns.json is not binding (owner, A52 item 2): names and types are compared.
        if sorted(mapped) != sorted(silver):
            problems.append(f"{fact.name}: contract columns differ from {fact.silver_model} in silver_columns.json")
        names = [column.name for column in fact.columns] + [key.name for key in fact.keys]
        if len(set(names)) != len(names):
            problems.append(f"{fact.name}: a gold column name repeats")
        if len({column.silver_column for column in fact.columns}) != len(fact.columns):
            problems.append(f"{fact.name}: a silver column lands in two gold columns")
        known = {column.name for column in fact.columns}
        wanted = {fact.grain, *fact.source_dates, *fact.text_columns, *(key.natural_key for key in fact.keys)}
        problems += [f"{fact.name}: {name} is not a contract column" for name in sorted(wanted - known)]
        problems += [f"{fact.name}: {key.name} points at {key.dimension}, which is not in the contract" for key in fact.keys if key.dimension not in dimensions]
    for table in contract.bridges:
        listed = {column.name for column in table.columns}
        problems += [f"{table.name}: grain column {name} is not a column" for name in table.grain_columns if name not in listed]
        problems += [
            f"{table.name}: {column} points at {dimension}, which is not in the contract"
            for column, dimension in table.references
            if dimension not in dimensions
        ]
    return problems


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def relation(name: str) -> str:
    return f"main.{ident(name)}"


def build(template: str, **parts: str) -> str:
    """Fill ``$name`` slots in a SQL template with identifiers and literals that ``ident`` and ``literal`` quoted."""
    return Template(template).substitute(parts)


def chunks(statements: Iterable[str], limit: int = CHUNK_CHARACTERS) -> Iterator[str]:
    """Group statements so no query text passes ``limit`` characters (a single longer statement stands alone)."""
    current: list[str] = []
    size = 0
    for statement in statements:
        if current and size + len(statement) > limit:
            yield "\n".join(current)
            current, size = [], 0
        current.append(statement)
        size += len(statement)
    if current:
        yield "\n".join(current)


Relations = dict[str, list[tuple[str, str]]]

COLUMNS_SQL = "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' ORDER BY table_name, ordinal_position;"


def parse_relations(rows: list[list[str]]) -> Relations:
    """Group ``COLUMNS_SQL`` rows into each relation's ordered (column, type) pairs."""
    found: Relations = {}
    for table, column, kind in rows:
        found.setdefault(table, []).append((column, kind))
    return found


def structure_problems(contract: Contract, relations: Relations) -> dict[str, list[str]]:
    """Relations, column map, source dates and published text, from the build's column list (failure modes 838 and 841)."""
    problems: dict[str, list[str]] = {name: [] for name in ("gold_relations_present", "column_map", "source_dates_typed", "published_text_kept")}
    for fact in contract.facts:
        found = relations.get(fact.name)
        if found is None:
            problems["gold_relations_present"].append(f"{fact.name}: no relation in the build")
            continue
        if not columns_match(fact, found):
            problems["column_map"].append(f"{fact.name}: {describe_difference(fact.expected_columns, found)}")
        actual = dict(found)
        for name in fact.source_dates:
            expected = next(column.type for column in fact.columns if column.name == name)
            if actual.get(name) != expected:
                problems["source_dates_typed"].append(f"{fact.name}.{name}: expected {expected}, found {actual.get(name)}")
        # Amendment A41: a listed column may be non-text (a BOOLEAN flag); the check is its presence and its contract type.
        for name in fact.text_columns:
            expected = next(column.type for column in fact.columns if column.name == name)
            if actual.get(name) != expected:
                problems["published_text_kept"].append(f"{fact.name}.{name}: expected {expected}, found {actual.get(name)}")
    for table in contract.tables:
        found = relations.get(table.name)
        if found is None:
            problems["gold_relations_present"].append(f"{table.name}: no relation in the build")
            continue
        actual = dict(found)
        if table.kind == "dimension" and actual.get(table.key) != "VARCHAR":
            problems["column_map"].append(f"{table.name}: key column {table.key} expected VARCHAR, found {actual.get(table.key)}")
        for column in table.columns:
            if actual.get(column.name) != column.type:
                problems["column_map"].append(f"{table.name}.{column.name}: expected {column.type}, found {actual.get(column.name)}")
        if table.kind == "bridge":
            extra = [name for name in actual if name not in {column.name for column in table.columns} and not name.endswith("_sk")]
            if extra:
                problems["column_map"].append(f"{table.name}: columns beyond the contract and surrogate keys: {sorted(extra)}")
    return problems


def columns_match(fact: Fact, found: list[tuple[str, str]]) -> bool:
    """The silver columns with their types in any order (a built silver model may order them differently from the column
    contract), then the surrogate keys last in the contract's order."""
    count = len(fact.keys)
    split = len(found) - count
    return (
        split >= 0
        and sorted(found[:split]) == sorted(fact.expected_columns[: len(fact.columns)])
        and found[split:] == fact.expected_columns[len(fact.columns) :]
    )


def describe_difference(expected: list[tuple[str, str]], found: list[tuple[str, str]]) -> str:
    """Name the first difference between two ordered column lists, without values."""
    missing = [name for name, _ in expected if name not in dict(found)]
    extra = [name for name, _ in found if name not in dict(expected)]
    retyped = [name for name, kind in expected if name in dict(found) and dict(found)[name] != kind]
    if missing or extra or retyped:
        return f"missing {missing}, extra {extra}, retyped {retyped}"
    return "surrogate keys are not last, in the contract's order"


def reconcilable(contract: Contract, relations: Relations) -> tuple[list[Fact], list[str]]:
    """Facts whose gold and silver relations hold every contract column, and a problem for each fact that cannot be compared."""
    ready: list[Fact] = []
    problems: list[str] = []
    for fact in contract.facts:
        gold = dict(relations.get(fact.name, []))
        silver = dict(relations.get(fact.silver_model, []))
        lacking = [column.name for column in fact.columns if column.name not in gold] + [
            column.silver_column for column in fact.columns if column.silver_column not in silver
        ]
        if fact.silver_model not in relations:
            problems.append(f"{fact.name}: silver model {fact.silver_model} is not in the build")
        elif fact.name not in relations:
            problems.append(f"{fact.name}: no gold relation to compare")
        elif lacking:
            problems.append(f"{fact.name}: cannot compare, columns absent: {sorted(set(lacking))}")
        else:
            ready.append(fact)
    return ready, problems


FINGERPRINT = (
    "SELECT $label, $side, count(*)::VARCHAR, coalesce(sum(md5_number_lower(CAST(r AS VARCHAR))::HUGEINT), 0)::VARCHAR, "
    "coalesce(bit_xor(md5_number(CAST(r AS VARCHAR))), 0)::VARCHAR FROM (SELECT $row AS r FROM $source);"
)
GRAIN = "SELECT $label, count(*)::VARCHAR, count(DISTINCT $grain)::VARCHAR, (count(*) FILTER (WHERE $grain IS NULL))::VARCHAR FROM $source;"
HOLD = "SELECT $label, (SELECT count(*) FROM ($a EXCEPT ALL $b))::VARCHAR, (SELECT count(*) FROM ($b EXCEPT ALL $a))::VARCHAR;"
DIMENSION = (
    "SELECT $label, count(*)::VARCHAR, count(DISTINCT $key)::VARCHAR, (count(*) FILTER (WHERE $key IS NULL))::VARCHAR, "
    "(count(*) FILTER (WHERE $key = $unknown))::VARCHAR, "
    "(count(*) FILTER (WHERE length($key) <> 32 OR regexp_matches($key, '[^0-9a-f]')))::VARCHAR FROM $source;"
)
BRIDGE = (
    "SELECT $label, count(*)::VARCHAR, count(DISTINCT struct_pack($pack))::VARCHAR, (count(*) FILTER (WHERE $nulls))::VARCHAR$unresolved "
    "FROM $source AS f $joins;"
)
ROWS = "SELECT $label, count(*)::VARCHAR FROM $source;"
KEY_SIDE = "(SELECT DISTINCT $key AS k FROM $source)"
KEY_ROW = "SELECT $label, $counts FROM $source AS f $joins;"
PARSE = "SELECT $label, json_serialize_sql(rtrim(trim(content), ';')) FROM read_text($path);"
TYPES = (
    "SELECT e.column_name, e.data_type AS expected, c.data_type AS found FROM (VALUES $values) AS e(column_name, data_type) "
    "LEFT JOIN information_schema.columns AS c ON c.table_schema = 'main' AND c.table_name = $table AND c.column_name = e.column_name "
    "CROSS JOIN (SELECT count(*) AS batch_rows FROM {batch}) AS b WHERE c.data_type IS DISTINCT FROM e.data_type"
)
EQUAL = (
    "SELECT coalesce(g.$grain, s.$silver_grain) AS grain_key FROM {batch} AS g FULL JOIN $silver AS s ON g.$grain = s.$silver_grain "
    "WHERE g.$grain IS NULL OR s.$silver_grain IS NULL OR $differs"
)
DUPLICATE_KEY = "SELECT $key FROM {batch} GROUP BY $key HAVING count(*) > 1 OR $key IS NULL"
UNKNOWN_ONCE = "SELECT count(*) AS unknown_members FROM {batch} HAVING count(*) FILTER (WHERE $key = $unknown) <> 1"
BRIDGE_DUPLICATES = "SELECT $grain FROM {batch} GROUP BY $grain HAVING count(*) > 1"
BRIDGE_REFS = "SELECT f.* FROM {batch} AS f $joins WHERE $conditions"
KEYS_NOT_NULL = "SELECT * FROM {batch} WHERE $nulls"
KEYS_JOINED = "SELECT f.$grain FROM {batch} AS f $joins WHERE $conditions"


def pack(pairs: Iterable[tuple[str, str]]) -> str:
    """A struct over (name, column) pairs: both sides use the silver names so their text is the same."""
    return ", ".join(f"{ident(name)} := {ident(column)}" for name, column in pairs)


def fingerprint_statement(label: str, side: str, source: str, pairs: Iterable[tuple[str, str]]) -> str:
    return build(FINGERPRINT, label=literal(label), side=literal(side), row=f"struct_pack({pack(pairs)})", source=relation(source))


def reconcile_statements(facts: Iterable[Fact]) -> list[str]:
    """Rows and content fingerprint of each fact and of its silver model over the silver columns (743, 744)."""
    statements = []
    for fact in facts:
        gold = [(column.silver_column, column.name) for column in fact.columns]
        silver = [(column.silver_column, column.silver_column) for column in fact.columns]
        statements.append(fingerprint_statement(fact.name, "gold", fact.name, gold))
        statements.append(fingerprint_statement(fact.name, "silver", fact.silver_model, silver))
    return statements


def reconcile_problems(facts: Iterable[Fact], rows: list[list[str]]) -> dict[str, list[str]]:
    seen: dict[tuple[str, str], list[str]] = {(label, side): values for label, side, *values in rows}
    problems: dict[str, list[str]] = {"reconcile_rows": [], "reconcile_content": []}
    for fact in facts:
        gold, silver = seen.get((fact.name, "gold")), seen.get((fact.name, "silver"))
        if gold is None or silver is None:
            problems["reconcile_rows"].append(f"{fact.name}: no fingerprint returned")
            continue
        if gold[0] != silver[0]:
            problems["reconcile_rows"].append(f"{fact.name}: {gold[0]} gold rows against {silver[0]} silver rows")
        if gold != silver:
            problems["reconcile_content"].append(f"{fact.name}: content fingerprint differs from {fact.silver_model}")
    return problems


def grain_statements(facts: Iterable[Fact]) -> list[str]:
    return [build(GRAIN, label=literal(fact.name), grain=ident(fact.grain), source=relation(fact.name)) for fact in facts]


def grain_problems(rows: list[list[str]], expected: Iterable[str]) -> list[str]:
    seen = {name: values for name, *values in rows}
    problems = []
    for name in expected:
        values = seen.get(name)
        if values is None:
            problems.append(f"{name}: no grain count returned")
        elif values[0] != values[1] or values[2] != "0":
            problems.append(f"{name}: {values[0]} rows, {values[1]} distinct grain keys, {values[2]} null")
    return problems


def hold_statements(facts: Iterable[Fact]) -> list[str]:
    """Both multiset differences between a hold fact and its silver hold model (failure mode 749)."""
    statements = []
    for fact in facts:
        if not fact.is_hold:
            continue
        silver = ", ".join(ident(column.silver_column) for column in fact.columns)
        gold = ", ".join(f"{ident(column.name)} AS {ident(column.silver_column)}" for column in fact.columns)
        a = build("SELECT $columns FROM $source", columns=silver, source=relation(fact.silver_model))
        b = build("SELECT $columns FROM $source", columns=gold, source=relation(fact.name))
        statements.append(build(HOLD, label=literal(fact.name), a=a, b=b))
    return statements


def hold_problems(rows: list[list[str]], expected: Iterable[str]) -> list[str]:
    seen = {name: values for name, *values in rows}
    problems = []
    for name in expected:
        values = seen.get(name)
        if values is None:
            problems.append(f"{name}: no hold comparison returned")
        elif values != ["0", "0"]:
            problems.append(f"{name}: {values[0]} silver rows missing from gold, {values[1]} gold rows not in silver")
    return problems


def key_statements(facts: Iterable[Fact]) -> list[str]:
    """Per fact and surrogate key: null keys, keys with no dimension row, keys off the contract rule, and unknown-member rows (745)."""
    statements = []
    for fact in facts:
        if not fact.keys:
            continue
        joins, counts = [], []
        for index, key in enumerate(fact.keys):
            dimension = dimension_key(key.dimension)
            side = build(KEY_SIDE, key=ident(dimension), source=relation(key.dimension))
            sk, natural = f"f.{ident(key.name)}", f"f.{ident(key.natural_key)}"
            joins.append(f"LEFT JOIN {side} AS r{index} ON r{index}.k = {sk} LEFT JOIN {side} AS m{index} ON m{index}.k = md5(CAST({natural} AS VARCHAR))")
            unknown = f"md5({literal(UNKNOWN)})"
            counts += [
                f"(count(*) FILTER (WHERE {sk} IS NULL))::VARCHAR",
                f"(count(*) FILTER (WHERE {sk} IS NOT NULL AND r{index}.k IS NULL))::VARCHAR",
                f"(count(*) FILTER (WHERE {sk} IS DISTINCT FROM coalesce(m{index}.k, {unknown})))::VARCHAR",
                f"(count(*) FILTER (WHERE {natural} IS NOT NULL AND {sk} = {unknown}))::VARCHAR",
            ]
        statements.append(build(KEY_ROW, label=literal(fact.name), counts=", ".join(counts), source=relation(fact.name), joins=" ".join(joins)))
    return statements


def key_problems(facts: Iterable[Fact], rows: list[list[str]]) -> tuple[dict[str, list[str]], dict[str, dict[str, int]]]:
    """Verdicts for the three fact-key checks and, per fact, how many rows took the unknown member despite a natural key."""
    seen = {name: values for name, *values in rows}
    problems: dict[str, list[str]] = {"fact_keys_not_null": [], "fact_keys_resolve": [], "fact_keys_follow_rule": []}
    unknown_rows: dict[str, dict[str, int]] = {}
    for fact in facts:
        if not fact.keys:
            continue
        values = seen.get(fact.name)
        if values is None:
            for name in problems:
                problems[name].append(f"{fact.name}: no key counts returned")
            continue
        unknown_rows[fact.name] = {}
        for index, key in enumerate(fact.keys):
            null, unresolved, off_rule, unknown = (int(value) for value in values[index * 4 : index * 4 + 4])
            unknown_rows[fact.name][key.name] = unknown
            if null:
                problems["fact_keys_not_null"].append(f"{fact.name}.{key.name}: {null} null")
            if unresolved:
                problems["fact_keys_resolve"].append(f"{fact.name}.{key.name}: {unresolved} keys with no row in {key.dimension}")
            if off_rule:
                problems["fact_keys_follow_rule"].append(f"{fact.name}.{key.name}: {off_rule} rows off the contract rule")
    return problems, unknown_rows


def dimension_statements(dimensions: Iterable[Table]) -> list[str]:
    statements = []
    for table in dimensions:
        key = ident(table.key)
        unknown = f"md5({literal(UNKNOWN)})"
        statements.append(build(DIMENSION, label=literal(table.name), key=key, unknown=unknown, source=relation(table.name)))
    return statements


def dimension_problems(rows: list[list[str]], expected: Iterable[str]) -> list[str]:
    seen = {name: values for name, *values in rows}
    problems = []
    for name in expected:
        values = seen.get(name)
        if values is None:
            problems.append(f"{name}: no key counts returned")
            continue
        total, distinct, null, unknown, malformed = (int(value) for value in values)
        if total != distinct or null:
            problems.append(f"{name}: {total} rows, {distinct} distinct keys, {null} null")
        if unknown != 1:
            problems.append(f"{name}: the unknown member occurs {unknown} times")
        if malformed:
            problems.append(f"{name}: {malformed} keys are not 32 lowercase hexadecimal characters")
    return problems


def bridge_statements(bridges: Iterable[Table]) -> list[str]:
    """Per bridge: rows, distinct grain combinations, null grain columns and references with no row in their dimension (failure mode 744)."""
    statements = []
    for table in bridges:
        pack_sql = ", ".join(f"{ident(column)} := f.{ident(column)}" for column in table.grain_columns)
        nulls = " OR ".join(f"f.{ident(column)} IS NULL" for column in table.required_columns)
        joins, unresolved = [], ""
        for index, (column, dimension) in enumerate(table.references):
            side = build(KEY_SIDE, key=ident(dimension_key(dimension)), source=relation(dimension))
            joins.append(f"LEFT JOIN {side} AS r{index} ON r{index}.k = f.{ident(column)}")
            unresolved += f", (count(*) FILTER (WHERE f.{ident(column)} IS NOT NULL AND r{index}.k IS NULL))::VARCHAR"
        statements.append(
            build(BRIDGE, label=literal(table.name), pack=pack_sql, nulls=nulls, unresolved=unresolved, source=relation(table.name), joins=" ".join(joins))
        )
    return statements


def bridge_problems(bridges: Iterable[Table], rows: list[list[str]]) -> dict[str, list[str]]:
    seen = {name: values for name, *values in rows}
    problems: dict[str, list[str]] = {"bridge_grain": [], "bridge_keys_resolve": []}
    for table in bridges:
        values = seen.get(table.name)
        if values is None:
            problems["bridge_grain"].append(f"{table.name}: no grain count returned")
            continue
        total, distinct, null = values[:3]
        if total == "0":
            problems["bridge_grain"].append(f"{table.name}: no rows")
        if total != distinct or null != "0":
            problems["bridge_grain"].append(f"{table.name}: {total} rows, {distinct} distinct grain combinations, {null} with a null key column")
        for (column, dimension), count in zip(table.references, values[3:], strict=True):
            if count != "0":
                problems["bridge_keys_resolve"].append(f"{table.name}.{column}: {count} keys with no row in {dimension}")
    return problems


def silver_row_statements(contract: Contract, relations: Relations) -> list[str]:
    """Row count of each fact's silver model, whether or not its gold fact exists (amendment A39 item 2)."""
    return [build(ROWS, label=literal(fact.name), source=relation(fact.silver_model)) for fact in contract.facts if fact.silver_model in relations]


def coverage_problems(
    contract: Contract, silver_rows: dict[str, int], gold_rows: dict[str, int], allowed_empty: dict[str, str]
) -> tuple[list[str], dict[str, Any]]:
    """Every fact needs a silver and a gold row, unless the caller names it with a reason; one non-empty hold fact proves row preservation (A39)."""
    problems: list[str] = []
    detail: dict[str, Any] = {}
    for fact in contract.facts:
        silver, gold = silver_rows.get(fact.name), gold_rows.get(fact.name)
        reason = allowed_empty.get(fact.name)
        detail[fact.name] = {"silver_rows": silver, "gold_rows": gold, **({"allowed_empty": reason} if reason else {})}
        if silver is None:
            problems.append(f"{fact.name}: no silver row count (silver model {fact.silver_model} is not in the build)")
        elif silver == 0 and reason is None:
            problems.append(f"{fact.name}: no silver rows")
        elif gold == 0 and reason is None:
            problems.append(f"{fact.name}: no gold rows")
    holds = [fact for fact in contract.facts if fact.is_hold]
    if holds and not any(silver_rows.get(fact.name, 0) > 0 and gold_rows.get(fact.name) == silver_rows.get(fact.name) for fact in holds):
        problems.append("no non-empty hold fact proves row preservation")
    return problems, detail


def rebuild_statements(contract: Contract, relations: Relations) -> list[str]:
    """Rows and content fingerprint of every gold relation over all its columns, to compare two builds (failure mode 750)."""
    statements = []
    for name in sorted(contract.model_names):
        columns = relations.get(name)
        if columns:
            statements.append(fingerprint_statement(name, "gold", name, [(column, column) for column, _ in columns]))
    return statements


def rebuild_problems(first: list[list[str]], second: list[list[str]], expected: Iterable[str]) -> list[str]:
    """Models whose fingerprint differs between two builds or is absent from either."""
    one = {name: values for name, _side, *values in first}
    two = {name: values for name, _side, *values in second}
    return [f"{name}: fingerprint differs between the two builds" for name in expected if name not in one or one.get(name) != two.get(name)]


# Lineage (failure mode 746): gold reads kept silver models, seeds and gold models only.
def gold_nodes(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The manifest's models under ``models/gold``, by name."""
    return {node["name"]: node for node in manifest["nodes"].values() if node["resource_type"] == "model" and len(node["fqn"]) > 2 and node["fqn"][1] == "gold"}


def published_silver(path: Path = SILVER_PUBLISH) -> frozenset[str]:
    """The silver models the silver publish contract publishes."""
    return frozenset(json.loads(path.read_text())["publish"]["tables"])


def unpublished_silver_problems(manifest: dict[str, Any], published: Collection[str]) -> list[str]:
    """Gold models whose dbt dependencies include a silver model that is not published, and published names that no silver model has."""
    silver = {
        key: node["name"] for key, node in manifest["nodes"].items() if node["resource_type"] == "model" and len(node["fqn"]) > 2 and node["fqn"][1] == "silver"
    }
    if not silver:
        return ["the manifest holds no silver model, so no read was checked"]
    problems = [
        f"{name} reads the silver model {silver[key]}, which the silver publish contract does not publish"
        for name, node in sorted(gold_nodes(manifest).items())
        for key in node["depends_on"]["nodes"]
        if key in silver and silver[key] not in published
    ]
    return problems + [f"{name} is published but no silver model has that name" for name in sorted(set(published) - set(silver.values()))]


QUOTED_NAME = re.compile(r"""['"]([A-Za-z0-9_]+)['"]""")
TEST_DEFINITION = re.compile(r"{%-?\s*test\s+([A-Za-z0-9_]+)")
BUILT_IN_TESTS = frozenset({"unique", "not_null", "accepted_values", "relationships"})


def gold_read_problems(gold_sql: dict[str, str], silver: Collection[str], published: Collection[str]) -> list[str]:
    """Gold models whose text names a silver model that the silver publish contract does not publish.

    This is the text form of ``unpublished_silver_problems`` for a check without a dbt manifest: a ``ref()`` argument or a quoted name in a model's list of
    sources is a read. The manifest check in the gold E2E stays the decisive one.
    """
    return [
        f"{name} reads the silver model {model}, which the silver publish contract does not publish"
        for name, text in sorted(gold_sql.items())
        for model in sorted(set(QUOTED_NAME.findall(text)) & set(silver) - set(published))
    ]


def listed_tests(document: Any) -> Iterator[str]:
    """The name of each test a model YAML lists under ``data_tests`` or ``tests``, at model and column level."""
    if isinstance(document, dict):
        for key, value in document.items():
            if key in {"data_tests", "tests"} and isinstance(value, list):
                for item in value:
                    yield from [item] if isinstance(item, str) else [name for name in item if isinstance(name, str)] if isinstance(item, dict) else []
            else:
                yield from listed_tests(value)
    elif isinstance(document, list):
        for item in document:
            yield from listed_tests(item)


def test_binding_problems(documents: dict[str, Any], generic: Collection[str], singular: Collection[str]) -> list[str]:
    """Test names in model YAML that dbt cannot bind. A singular test binds by its file in ``dbt/tests`` and is never listed; a listed name must be a built-in
    test, a package test or a ``{% test %}`` macro."""
    problems = []
    for path, document in sorted(documents.items()):
        for name in listed_tests(document):
            if name in BUILT_IN_TESTS or "." in name or name in generic:
                continue
            reason = "a singular test binds by its file in dbt/tests and is not listed in YAML" if name in singular else "no generic test has that name"
            problems.append(f"{path}: {name}: {reason}" if name not in singular else f"{path}: {name} is a singular test: {reason}")
    return problems


def model_problems(dbt: Path = DBT, published: Collection[str] | None = None) -> list[str]:
    """Offline reads of the dbt tree: gold models name only published silver models, and model YAML lists only tests dbt can bind."""
    import yaml  # the host checks need it; the in-container readers of this module do not

    silver = {path.stem for path in (dbt / "models/silver").rglob("*.sql")}
    gold = {path.stem: path.read_text() for path in (dbt / "models/gold").rglob("*.sql")}
    problems = gold_read_problems(gold, silver, published_silver() if published is None else published)
    generic = {name for path in (dbt / "macros").rglob("*.sql") for name in TEST_DEFINITION.findall(path.read_text())}
    singular = {path.stem for path in (dbt / "tests").rglob("*.sql")}
    documents = {str(path.relative_to(dbt)): yaml.safe_load(path.read_text()) for path in sorted((dbt / "models").rglob("*.yml"))}
    return problems + test_binding_problems(documents, generic, singular)


def lineage_problems(contract: Contract, manifest: dict[str, Any], published: Collection[str]) -> list[str]:
    nodes = gold_nodes(manifest)
    problems = unpublished_silver_problems(manifest, published)
    dimension_names = {table.name for table in contract.dimensions}
    for name, node in sorted(nodes.items()):
        fact = next((item for item in contract.facts if item.name == name), None)
        parents = [(parent.split(".", 1)[0], parent.rsplit(".", 1)[-1]) for parent in node["depends_on"]["nodes"]]
        for kind, parent in parents:
            if kind == "seed":
                continue
            if kind != "model":
                problems.append(f"{name} reads {kind} {parent}")
            elif fact is not None:
                if parent != fact.silver_model and parent not in {key.dimension for key in fact.keys}:
                    problems.append(f"{name} reads {parent}: a fact reads its silver model and its dimensions only")
            elif parent not in contract.silver and parent not in nodes and parent not in listed_inputs(contract, name):
                problems.append(f"{name} reads {parent}, which is not a kept silver model, a seed, a gold model or an input the contract lists for it")
        if fact is not None and fact.silver_model not in {parent for _, parent in parents}:
            problems.append(f"{name} does not read its silver model {fact.silver_model}")
        if fact is not None and not {key.dimension for key in fact.keys} <= dimension_names:
            problems.append(f"{name} names a dimension outside the contract")
    return problems


def listed_inputs(contract: Contract, model: str) -> tuple[str, ...]:
    """The inputs the contract itself lists for a dimension or bridge (``dim_release`` lists ``stg_bronze__file_copies``)."""
    return next((table.inputs for table in contract.tables if table.name == model), ())


def contract_models_problems(contract: Contract, manifest: dict[str, Any]) -> list[str]:
    """Every contract model has a gold node and every gold node is in the contract."""
    nodes = set(gold_nodes(manifest))
    wanted = set(contract.model_names)
    return [f"{name}: no gold model" for name in sorted(wanted - nodes)] + [f"{name}: gold model outside the contract" for name in sorted(nodes - wanted)]


# Expression check (failure modes 746 and 840), over DuckDB's parse of the compiled SQL (``json_serialize_sql``); the analysis is in gold_sql.py.
AGGREGATE_ROWS = "SELECT '__aggregate__', lower(function_name) FROM duckdb_functions() WHERE function_type = 'aggregate' GROUP BY 2;"
CHECK_OF_FINDING = {"arithmetic": "expression_arithmetic", "case": "expression_case", "functions": "expression_functions", "lineage": "lineage"}


def parse_query(paths: dict[str, str]) -> str:
    """One ``json_serialize_sql`` row per model, from the compiled files at the given container paths, then DuckDB's aggregate function names."""
    return "\n".join([*(build(PARSE, label=literal(model), path=literal(path)) for model, path in sorted(paths.items())), AGGREGATE_ROWS])


def split_parse_rows(rows: list[list[str]], models: list[str]) -> tuple[dict[str, str | None], frozenset[str]]:
    """The parse of each model (None where none came back) and the aggregate names DuckDB reported."""
    parsed: dict[str, str | None] = dict.fromkeys(models)
    aggregates = set()
    for label, text in rows:
        if label == "__aggregate__":
            aggregates.add(text)
        else:
            parsed[label] = text
    return parsed, frozenset(aggregates)


def policy_for(contract: Contract, aggregates: frozenset[str]) -> gold_sql.Policy:
    """The expression policy: the contract's allowlist, its value columns, source dates and dimension keys, and DuckDB's aggregates."""
    scopes = dict(contract.expression_policy)
    scopes["source_dates"] = {
        "dim_date": sorted(contract.date_pairs),
        "dim_hospital": sorted(
            (fact.silver_model, column.name)
            for fact in contract.facts
            if any(key.dimension == "dim_hospital" for key in fact.keys)
            for column in fact.columns
            if column.name in fact.source_dates and column.type == "DATE"
        ),
    }
    return gold_sql.Policy(
        gold_sql.capabilities(contract.allowlist),
        contract.value_columns,
        contract.date_pairs,
        aggregates,
        scopes,
    )


def declared_relations(manifest: dict[str, Any]) -> dict[str, set[str]]:
    """For each gold model, the relation names of the models and seeds its dbt dependencies declare."""
    declared: dict[str, set[str]] = {}
    for name, node in gold_nodes(manifest).items():
        declared[name] = set()
        for unique_id in node["depends_on"]["nodes"]:
            parent = manifest["nodes"].get(unique_id)
            if parent is not None:
                declared[name].add(parent.get("alias") or parent["name"])
    return declared


def rendered_facts(contract: Contract, manifest: dict[str, Any], macro: Path = REPO_ROOT / "dbt/macros/gold_facts.sql") -> dict[str, str]:
    """Render facts from contract arguments using the actual macro and manifest relation bindings."""
    relations = {
        node.get("name", ""): node["relation_name"]
        for node in manifest["nodes"].values()
        if node.get("resource_type") in ("model", "seed") and node.get("relation_name")
    }
    text = macro.read_text()
    return {
        fact.name: gold_sql.render_fact(
            text, fact.silver_model, fact.grain, {key.name: {"dimension": key.dimension, "natural_key": key.natural_key} for key in fact.keys}, relations
        )
        for fact in contract.facts
    }


def expression_problems(
    contract: Contract,
    parsed: dict[str, str | None],
    expected: Iterable[str],
    aggregates: frozenset[str],
    declared: dict[str, set[str]],
    compiled: dict[str, str] | None = None,
    expected_fact_sql: dict[str, str] | None = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Verdicts for arithmetic, case, other expressions and the physical relations a model reads, and the relations each model reads."""
    if not aggregates:
        raise ContractError("DuckDB reported no aggregate functions, so aggregates cannot be classified")
    policy = policy_for(contract, aggregates)
    problems: dict[str, list[str]] = {"expression_arithmetic": [], "expression_case": [], "expression_functions": [], "lineage": []}
    reads: dict[str, list[str]] = {}
    facts = {fact.name: fact for fact in contract.facts}
    for model in expected:
        text = parsed.get(model)
        if text is None:
            for key in ("expression_arithmetic", "expression_case", "expression_functions"):
                problems[key].append(f"{model}: no compiled SQL to check")
            continue
        document = json.loads(text)
        if document.get("error"):
            for key in ("expression_arithmetic", "expression_case", "expression_functions"):
                problems[key].append(f"{model}: compiled SQL does not parse: {str(document.get('error_message', ''))[:120]}")
            continue
        fact = facts.get(model)
        fact_map = {key.name: (key.dimension, key.natural_key) for key in fact.keys} if fact else {}
        if fact is not None:
            analysis = gold_sql.analyze_fact(model, document, policy, fact_map, (compiled or {}).get(model), (expected_fact_sql or {}).get(model))
        else:
            analysis = gold_sql.analyze_model(model, contract.kind_of(model), document, policy, fact_map)
        for finding in analysis.findings:
            problems[CHECK_OF_FINDING[finding.check]].append(finding.message)
        reads[model] = sorted(analysis.reads)
        problems["lineage"] += [
            f"{model} reads relation {name}, which its dbt dependencies do not declare" for name in sorted(analysis.reads - declared.get(model, set()))
        ]
    return problems, reads


# Great Expectations suites (failure modes 747, 748 and the key rules).
def suite(name: str, description: str, expectations: list[dict[str, Any]]) -> dict[str, Any]:
    return {"name": name, "description": description, "expectations": expectations}


def expectation(table: str, tag: str, kind: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    return {"id": f"{table}.{tag}", "table": table, "type": kind, "kwargs": kwargs, "severity": "structural"}


def rows_query(sql: str) -> dict[str, Any]:
    return {"unexpected_rows_query": sql}


def types_query(table: str, columns: list[tuple[str, str]]) -> str:
    """Rows for every column that is absent from the relation or has another type (one query per relation)."""
    values = ", ".join(f"({literal(name)}, {literal(kind)})" for name, kind in columns)
    return build(TYPES, values=values, table=literal(table))


def equal_query(fact: Fact, names: tuple[str, ...]) -> str:
    """Rows where the fact and its silver model differ on the named columns, matched on the grain key (747, 748)."""
    by_name = {column.name: column.silver_column for column in fact.columns}
    grain = by_name[fact.grain]
    differs = " OR ".join(f"g.{ident(name)} IS DISTINCT FROM s.{ident(by_name[name])}" for name in names)
    return build(EQUAL, grain=ident(fact.grain), silver_grain=ident(grain), silver=relation(fact.silver_model), differs=differs)


def unexpected(table: str, tag: str, sql: str) -> dict[str, Any]:
    return expectation(table, tag, "UnexpectedRowsExpectation", rows_query(sql))


def key_joins(fact: Fact, prefix: str, on: str) -> str:
    """One left join per surrogate key to its dimension's keys; ``on`` is the join condition with ``$k`` and ``$key`` slots."""
    joins = []
    for index, key in enumerate(fact.keys):
        side = build(KEY_SIDE, key=ident(dimension_key(key.dimension)), source=relation(key.dimension))
        condition = build(on, alias=f"{prefix}{index}", sk=f"f.{ident(key.name)}", natural=f"f.{ident(key.natural_key)}")
        joins.append(f"LEFT JOIN {side} AS {prefix}{index} ON {condition}")
    return " ".join(joins)


def fact_expectations(fact: Fact) -> dict[str, list[dict[str, Any]]]:
    """The expectations of one fact, by suite file stem."""
    expected = fact.expected_columns
    unknown = f"md5({literal(UNKNOWN)})"
    grain = ident(fact.grain)
    out: dict[str, list[dict[str, Any]]] = {"gold_contract": [], "gold_keys": [], "gold_source_dates": [], "gold_published_text": []}
    out["gold_contract"].append(expectation(fact.name, "columns", "ExpectTableColumnsToMatchSet", {"column_set": [name for name, _ in expected]}))
    out["gold_contract"].append(unexpected(fact.name, "types", types_query(fact.name, expected)))
    out["gold_keys"].append(unexpected(fact.name, "grain", build(DUPLICATE_KEY, key=grain)))
    if fact.keys:
        nulls = " OR ".join(f"{ident(key.name)} IS NULL" for key in fact.keys)
        out["gold_keys"].append(unexpected(fact.name, "keys_not_null", build(KEYS_NOT_NULL, nulls=nulls)))
        unresolved = " OR ".join(f"(f.{ident(key.name)} IS NOT NULL AND r{index}.k IS NULL)" for index, key in enumerate(fact.keys))
        joins = key_joins(fact, "r", "$alias.k = $sk")
        out["gold_keys"].append(unexpected(fact.name, "keys_resolve", build(KEYS_JOINED, grain=grain, joins=joins, conditions=unresolved)))
        off_rule = " OR ".join(f"f.{ident(key.name)} IS DISTINCT FROM coalesce(m{index}.k, {unknown})" for index, key in enumerate(fact.keys))
        joins = key_joins(fact, "m", "$alias.k = md5(CAST($natural AS VARCHAR))")
        out["gold_keys"].append(unexpected(fact.name, "keys_follow_rule", build(KEYS_JOINED, grain=grain, joins=joins, conditions=off_rule)))
    if fact.source_dates:
        out["gold_source_dates"].append(unexpected(fact.name, "source_dates_equal_silver", equal_query(fact, fact.source_dates)))
    if fact.text_columns:
        out["gold_published_text"].append(unexpected(fact.name, "published_text_equal_silver", equal_query(fact, fact.text_columns)))
    return out


def table_expectations(table: Table) -> dict[str, list[dict[str, Any]]]:
    """The expectations of one dimension or bridge, by suite file stem."""
    out: dict[str, list[dict[str, Any]]] = {"gold_contract": [], "gold_keys": []}
    if table.kind == "dimension":
        key = ident(table.key)
        out["gold_contract"].append(unexpected(table.name, "key_type", types_query(table.name, [(table.key, "VARCHAR")])))
        out["gold_keys"].append(unexpected(table.name, "key_unique", build(DUPLICATE_KEY, key=key)))
        out["gold_keys"].append(unexpected(table.name, "unknown_member_once", build(UNKNOWN_ONCE, key=key, unknown=f"md5({literal(UNKNOWN)})")))
    else:
        out["gold_contract"].append(unexpected(table.name, "types", types_query(table.name, [(column.name, column.type) for column in table.columns])))
        grain = ", ".join(ident(column) for column in table.grain_columns)
        nulls = " OR ".join(f"{ident(column)} IS NULL" for column in table.required_columns)
        out["gold_keys"].append(unexpected(table.name, "grain", build(BRIDGE_DUPLICATES, grain=grain)))
        out["gold_keys"].append(unexpected(table.name, "key_not_null", build(KEYS_NOT_NULL, nulls=nulls)))
        if table.references:
            conditions = " OR ".join(f"(f.{ident(column)} IS NOT NULL AND r{index}.k IS NULL)" for index, (column, _dimension) in enumerate(table.references))
            joins = " ".join(
                f"LEFT JOIN {build(KEY_SIDE, key=ident(dimension_key(dimension)), source=relation(dimension))} AS r{index} ON r{index}.k = f.{ident(column)}"
                for index, (column, dimension) in enumerate(table.references)
            )
            out["gold_keys"].append(unexpected(table.name, "refs_resolve", build(BRIDGE_REFS, joins=joins, conditions=conditions)))
    return out


SUITE_TEXT = {
    "gold_contract": "Every gold fact has its contract columns and surrogate keys with their types, and every dimension its key column",
    "gold_keys": "Grain keys are unique; surrogate keys are not null, resolve and follow the contract rule; each dimension holds its unknown member once",
    "gold_source_dates": "Each fact's source dates equal its silver model's on the grain key (failure mode 747)",
    "gold_published_text": "Each fact's published text columns equal its silver model's on the grain key (failure mode 748)",
}


def build_suites(contract: Contract) -> dict[str, dict[str, Any]]:
    """The four gold suites, generated from the contract; file name stem to suite."""
    collected: dict[str, list[dict[str, Any]]] = {stem: [] for stem in SUITE_TEXT}
    for part in [fact_expectations(fact) for fact in contract.facts] + [table_expectations(table) for table in contract.tables]:
        for stem, items in part.items():
            collected[stem] += items
    return {stem: suite(stem, SUITE_TEXT[stem], collected[stem]) for stem in SUITE_TEXT}


def render(document: dict[str, Any]) -> str:
    return json.dumps(document, indent=2) + "\n"


def write_suites(contract: Contract, out: Path) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for stem, document in build_suites(contract).items():
        path = out / f"{stem}.json"
        path.write_text(render(document))
        written.append(path)
    return written


def suites_problems(contract: Contract, folder: Path) -> list[str]:
    """Differences between the suite files in ``folder`` and the suites the contract generates."""
    wanted = build_suites(contract)
    found = {path.stem for path in folder.glob("*.json")} if folder.is_dir() else set()
    problems = [f"suite {stem} is missing" for stem in sorted(set(wanted) - found)] + [
        f"suite {stem} is not generated from the contract" for stem in sorted(found - set(wanted))
    ]
    for stem, document in wanted.items():
        path = folder / f"{stem}.json"
        if path.is_file() and path.read_text() != render(document):
            problems.append(f"suite {stem} differs from what the contract generates")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    write = commands.add_parser("write-suites", help="write the gold suites generated from the contract")
    write.add_argument("--contract", type=Path, default=CONTRACT)
    write.add_argument("--out", type=Path, default=SUITES)
    check = commands.add_parser("check-suites", help="fail when the suite files differ from the contract")
    check.add_argument("--contract", type=Path, default=CONTRACT)
    check.add_argument("--suites", type=Path, default=SUITES)
    commands.add_parser("check-models", help="fail when a gold model reads an unpublished silver model or model YAML lists a test dbt cannot bind")
    args = parser.parse_args()
    if args.command == "check-models":
        found = model_problems()
        for problem in found:
            sys.stderr.write(f"{problem}\n")
        sys.stdout.write(f"dbt models: {len(found)} problems\n")
        return 1 if found else 0
    contract = load_contract(args.contract)
    if args.command == "write-suites":
        written = write_suites(contract, args.out)
        sys.stdout.write(f"wrote {len(written)} suites to {args.out}\n")
        return 0
    problems = suites_problems(contract, args.suites)
    for problem in problems:
        sys.stderr.write(f"{problem}\n")
    sys.stdout.write(f"gold suites: {len(problems)} differences\n")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
