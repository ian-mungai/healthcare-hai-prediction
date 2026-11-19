"""The columns that compiled dbt models read from each bronze table, found by parsing the compiled SQL (no macro text is trusted).

The compiled models of the real project (lint target) are parsed with DuckDB's own SQL parser (``json_serialize_sql``), so quoting, comments and string
literals are read the way the engine reads them: a quoted identifier is a column, text inside a literal is not. The analysis follows each query's scopes:

- a named column is a read of the relation it belongs to (a qualified name by its alias; an unqualified name by the only relation that can hold it);
- ``select *`` (or ``alias.*``) passes every column of a relation on to the query's own output, so the relation needs whatever the readers of that output
  need (through common table expressions, derived tables and whole models);
- a flag or any other expression reads its input columns like any other column, and an output alias is never a read.

Anything the analysis cannot place exactly is never guessed: the relation is marked as read through ``any`` column, with the reason and the model, and the
declaration says so by table name. A model that the parser refuses does the same for every relation it names. The result for one bronze table is either the
columns read, each with the models that read it, or the reason the table is read through any column.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BRONZE_SCHEMA = "bronze"
# Models in these folders are read outside the dbt project (publishes, features), so a column passed through them has an unknown reader.
EXTERNAL_FOLDERS = ("gold", "features")

Relation = tuple[str, ...]
Parser = Callable[[str], "dict[str, Any] | None"]


class LineageError(ValueError):
    """The compiled SQL cannot be analyzed."""


@dataclass(frozen=True)
class Model:
    """One compiled statement: the model or test name, whether it is a test, whether the project's consumers lie outside dbt and the SQL text."""

    name: str
    is_test: bool
    external: bool
    sql: str


@dataclass
class Reads:
    """Named reads (with their models), flows of every column into another relation, the reasons a relation is read whole and the output names known."""

    named: dict[Relation, dict[str, set[str]]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(set)))
    flows: dict[Relation, list[tuple[Relation, frozenset[str]]]] = field(default_factory=lambda: defaultdict(list))
    whole: dict[Relation, str] = field(default_factory=dict)
    used: set[Relation] = field(default_factory=set)
    readers: dict[Relation, set[str]] = field(default_factory=lambda: defaultdict(set))
    known: dict[Relation, frozenset[str] | None] = field(default_factory=dict)
    counter: int = 0

    def fresh(self, kind: str) -> Relation:
        """A new relation identity for a query output."""
        self.counter += 1
        return (kind, str(self.counter))


@dataclass(frozen=True)
class Source:
    """One relation in a FROM clause: its alias, the relation it stands for and the output names it is known to have (None when unknown)."""

    alias: str
    relation: Relation
    known: frozenset[str] | None


@dataclass(frozen=True)
class QueryResult:
    """The output of an analyzed query: its relation, its explicit output names and whether it also passes columns through."""

    relation: Relation
    names: frozenset[str]
    stars: bool


def parse(sql: str) -> dict[str, Any] | None:
    """The parse tree of one statement, or None when the parser refuses it."""
    import duckdb

    connection = duckdb.connect()
    try:
        row = connection.execute("select json_serialize_sql(?)", [sql]).fetchone()
    except duckdb.Error:
        return None
    finally:
        connection.close()
    tree: dict[str, Any] = json.loads(row[0]) if row else {"error": True}
    if tree.get("error"):
        return None
    node: dict[str, Any] = tree["statements"][0]["node"]
    return node


def lower(name: str) -> str:
    """Identifiers are case-insensitive in the engine."""
    return name.lower()


def names_in(item: Any) -> list[str]:
    """Every column name written inside an expression (the parameter list of a lambda)."""
    found: list[str] = []
    stack = [item]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if current.get("class") == "COLUMN_REF":
                found.append(current["column_names"][-1])
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return found


class Analyzer:
    """Walk one model's parse tree and record its reads into ``Reads``."""

    def __init__(self, reads: Reads, model: Model) -> None:
        self.reads = reads
        self.model = model

    def table_relation(self, table: Mapping[str, Any], env: Mapping[str, Relation]) -> Relation:
        """The relation a table reference names: a common table expression in scope, a bronze source table or another model."""
        name = lower(table["table_name"])
        schema = lower(table.get("schema_name") or "")
        if not schema and name in env:
            return env[name]
        relation: Relation = ("bronze", name) if schema == BRONZE_SCHEMA else ("table", name)
        self.reads.used.add(relation)
        self.reads.readers[relation].add(self.model.name)
        return relation

    def read(self, relation: Relation, name: str) -> None:
        """A named read of one column of a relation by the current model."""
        self.reads.named[relation][lower(name)].add(self.model.name)

    def whole(self, relation: Relation, reason: str) -> None:
        """Mark a relation as read through any column, keeping the first reason."""
        self.reads.whole.setdefault(relation, f"{self.model.name}: {reason}")

    def query(self, node: Mapping[str, Any], env: Mapping[str, Relation], outer: list[list[Source]]) -> QueryResult:
        """Analyze a SELECT or set-operation node and return its output relation."""
        scope_env = dict(env)
        for entry in (node.get("cte_map") or {}).get("map", []):
            inner = self.query(entry["value"]["query"]["node"], scope_env, outer)
            listed = [lower(name) for name in entry["value"].get("aliases") or []]
            if listed:
                self.reads.known[inner.relation] = frozenset(listed)
                if inner.stars:
                    self.whole(inner.relation, "a common table expression renames its columns and passes columns through")
            scope_env[lower(entry["key"])] = inner.relation
        if node["type"] == "SET_OPERATION_NODE":
            return self.set_operation(node, scope_env, outer)
        if node["type"] != "SELECT_NODE":
            raise LineageError(f"unsupported query node {node['type']}")
        return self.select(node, scope_env, outer)

    def set_operation(self, node: Mapping[str, Any], env: Mapping[str, Relation], outer: list[list[Source]]) -> QueryResult:
        """A set operation matches branch columns by position, so a branch that passes columns through cannot be placed and its relations are read whole."""
        relation = self.reads.fresh("union")
        first: QueryResult | None = None
        for key in ("left", "right"):
            branch = self.query(node[key], env, outer)
            first = first or branch
            if branch.stars:
                self.whole(branch.relation, "a branch of a set operation passes every column through, matched by position")
        names = first.names if first else frozenset()
        self.reads.known[relation] = None if first and first.stars else names
        return QueryResult(relation, names, bool(first and first.stars))

    def sources(self, table: Mapping[str, Any], env: Mapping[str, Relation], outer: list[list[Source]], deferred: list[Any]) -> list[Source]:
        """The relations of a FROM clause. Join conditions and unplaceable table expressions are collected for resolution once every source is known."""
        kind = table["type"]
        if kind == "BASE_TABLE":
            relation = self.table_relation(table, env)
            return [Source(lower(table["alias"] or table["table_name"]), relation, self.reads.known.get(relation))]
        if kind == "JOIN":
            left = self.sources(table["left"], env, outer, deferred)
            right = self.sources(table["right"], env, outer, deferred)
            if table.get("condition"):
                deferred.append(table["condition"])
            for name in table.get("using_columns") or []:
                holders = [s for s in [*left, *right] if s.known is None or lower(name) in s.known]
                for source in holders:
                    self.read(source.relation, name)
            return [*left, *right]
        if kind == "SUBQUERY":
            result = self.query(table["subquery"]["node"], env, outer)
            known = None if result.stars else result.names
            self.reads.known[result.relation] = known
            return [Source(lower(table["alias"] or ""), result.relation, known)]
        for entry in self.base_tables(table):
            self.whole(self.table_relation(entry, env), f"it is used inside a {kind.lower()} table expression")
        deferred.append(table)
        return [Source(lower(table.get("alias") or ""), self.reads.fresh("opaque"), None)]

    def base_tables(self, table: Any) -> list[Mapping[str, Any]]:
        """Every table reference inside an unplaceable table expression."""
        found: list[Mapping[str, Any]] = []
        stack = [table]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                if item.get("type") == "BASE_TABLE":
                    found.append(item)
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
        return found

    def select(self, node: Mapping[str, Any], env: Mapping[str, Relation], outer: list[list[Source]]) -> QueryResult:
        """Analyze one SELECT: its sources, its select list and every expression."""
        deferred: list[Any] = []
        from_table = node.get("from_table")
        sources = self.sources(from_table, env, outer, deferred) if from_table and from_table["type"] != "EMPTY" else []
        scope = [sources, *outer]
        relation = self.reads.fresh("query")
        listed = node["select_list"]
        names = {lower(item["alias"] or item["column_names"][-1]) for item in listed if item["alias"] or item["class"] == "COLUMN_REF"}
        computed = {
            lower(item["alias"])
            for item in listed
            if item["alias"] and (item["class"] != "COLUMN_REF" or lower(item["alias"]) != lower(item["column_names"][-1]))
        }
        stars = False
        for item in listed:
            if item["class"] == "STAR":
                stars = True
                self.star(item, sources, relation, names, env, scope)
            else:
                self.expression(item, scope, env, frozenset(), frozenset())
        for entry in deferred:
            self.expression(entry, scope, env, frozenset(), frozenset())
        for key in ("where_clause", "group_expressions", "having", "qualify", "modifiers", "sample", "group_sets"):
            if node.get(key):
                self.expression(node[key], scope, env, frozenset(computed), frozenset())
        known = None if stars else frozenset(names)
        self.reads.known[relation] = known
        return QueryResult(relation, frozenset(names), stars)

    def star(
        self, item: Mapping[str, Any], sources: list[Source], relation: Relation, names: set[str], env: Mapping[str, Relation], scope: list[list[Source]]
    ) -> None:
        """``*`` or ``alias.*``: every column of the relations flows into this query's output, except those named elsewhere in the select list."""
        chosen = [s for s in sources if not item["relation_name"] or s.alias == lower(item["relation_name"])]
        if item.get("columns") or item.get("rename_list") or item.get("qualified_exclude_list"):
            for source in chosen:
                self.whole(source.relation, "it uses a COLUMNS expression or renames star columns")
            return
        excluded = frozenset(lower(name) for name in item.get("exclude_list") or []) | frozenset(names)
        for source in chosen:
            self.reads.flows[source.relation].append((relation, excluded))
        self.expression(item.get("replace_list") or [], scope, env, frozenset(), frozenset())

    def expression(self, item: Any, scope: list[list[Source]], env: Mapping[str, Relation], aliases: frozenset[str], bound: frozenset[str]) -> None:
        """Walk an expression: column references are reads, subqueries are new scopes, lambda parameters and output aliases are not columns."""
        if isinstance(item, list):
            for entry in item:
                self.expression(entry, scope, env, aliases, bound)
            return
        if not isinstance(item, dict):
            return
        kind = item.get("class")
        if kind == "COLUMN_REF":
            self.column(item["column_names"], scope, aliases, bound)
        elif kind == "LAMBDA":
            parameters = frozenset(lower(name) for name in names_in(item["lhs"]))
            self.expression(item["expr"], scope, env, aliases, bound | parameters)
        elif kind == "SUBQUERY":
            inner = self.query(item["subquery"]["node"], env, scope)
            if item.get("subquery_type") != "EXISTS" and inner.stars:
                self.whole(inner.relation, "a value subquery passes every column through, matched by position")
            self.expression(item.get("child"), scope, env, aliases, bound)
        elif kind != "STAR":
            for value in item.values():
                if isinstance(value, dict | list):
                    self.expression(value, scope, env, aliases, bound)

    def column(self, parts: list[str], scope: list[list[Source]], aliases: frozenset[str], bound: frozenset[str]) -> None:
        """Place one column reference on a relation of the nearest scope that can hold it."""
        name = lower(parts[-1])
        qualifier = lower(parts[-2]) if len(parts) >= 2 else None
        if qualifier is None and (name in bound or name in aliases):
            return
        for sources in scope:
            if qualifier is not None:
                for source in sources:
                    if source.alias == qualifier:
                        self.read(source.relation, name)
                        return
                continue
            candidates = [s for s in sources if s.known is None or name in s.known]
            if len(candidates) == 1:
                self.read(candidates[0].relation, name)
                return
            if len(candidates) > 1:
                for source in candidates:
                    self.whole(source.relation, f"the unqualified column {name} could belong to any of {len(candidates)} relations of one query")
                return


def analyze(models: Iterable[Model], parser: Parser = parse) -> tuple[Reads, dict[str, str]]:
    """Analyze every model. Returns the reads and the models that could not be analyzed (name to reason)."""
    reads = Reads()
    unparsed: dict[str, str] = {}
    texts: dict[str, str] = {}
    for model in models:
        tree = parser(model.sql)
        if tree is None:
            unparsed[model.name] = "the SQL parser refuses it"
            texts[model.name] = model.sql
            continue
        try:
            result = Analyzer(reads, model).query(tree, {}, [])
        except (LineageError, KeyError, IndexError, TypeError) as error:
            unparsed[model.name] = f"the analysis cannot place it ({error.__class__.__name__}: {error})"
            texts[model.name] = model.sql
            continue
        if model.is_test:
            continue
        reads.flows[result.relation].append((("table", model.name), frozenset()))
        if model.external:
            reads.whole.setdefault(("table", model.name), f"{model.name} is read outside the dbt project")
        reads.known.setdefault(("table", model.name), None if result.stars else result.names)
    for model in models:
        if not model.is_test and not model.external and ("table", model.name) not in reads.used:
            reads.whole.setdefault(("table", model.name), f"{model.name} has no reader inside the dbt project")
    for name, text in texts.items():
        tokens = set(re.findall(r"[a-z_][a-z0-9_]*", text.lower()))
        for relation in [*reads.used, *(r for r in reads.flows if r[0] == "table")]:
            if relation[1] in tokens:
                reads.whole.setdefault(relation, f"{name}: {unparsed[name]}")
    return reads, unparsed


def need(reads: Reads, relation: Relation, memo: dict[Relation, dict[str, set[str]] | str], path: tuple[Relation, ...] = ()) -> dict[str, set[str]] | str:
    """The columns a relation must hold, each with the models that read it; the reason string when the relation is read through any column."""
    if relation in memo:
        return memo[relation]
    if relation in reads.whole:
        memo[relation] = reads.whole[relation]
        return memo[relation]
    if relation in path:
        raise LineageError(f"a read cycle through {relation}")
    result: dict[str, set[str]] = defaultdict(set)
    for name, models in reads.named.get(relation, {}).items():
        result[name] |= models
    for target, excluded in reads.flows.get(relation, []):
        inner = need(reads, target, memo, (*path, relation))
        if isinstance(inner, str):
            memo[relation] = inner
            return inner
        for name, models in inner.items():
            if name not in excluded:
                result[name] |= models
    memo[relation] = result
    return result


def read_models(compiled: Path) -> list[Model]:
    """Every compiled model and test of the project under the compiled folder."""
    models = []
    for path in sorted(compiled.rglob("*.sql")):
        parts = path.relative_to(compiled).parts
        if "tests" in parts:
            models.append(Model(path.stem, True, False, path.read_text()))
        elif "models" in parts:
            models.append(Model(path.stem, False, any(folder in parts for folder in EXTERNAL_FOLDERS), path.read_text()))
    return models


def bronze_needs(models: list[Model], parser: Parser = parse) -> tuple[dict[str, dict[str, set[str]] | str], dict[str, str]]:
    """The columns read from each bronze table (table to columns with their models, or the reason it is read through any column) and the unanalyzed models."""
    reads, unparsed = analyze(models, parser)
    memo: dict[Relation, dict[str, set[str]] | str] = {}
    tables = sorted({relation[1] for relation in [*reads.named, *reads.flows, *reads.whole, *reads.used] if relation[0] == "bronze"})
    return {table: need(reads, ("bronze", table), memo) for table in tables}, unparsed
