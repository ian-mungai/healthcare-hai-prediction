"""Model metadata check: sensitivity classes, gold fact types and measure additivity in the dbt project.

Run from the repository root: ``.venv/bin/python -m scripts.quality.model_metadata``. The check reads the dbt project (YAML, SQL, macros,
``dbt_project.yml``), the gold contract and the bronze table map. The installed SQLFluff dbt templater compiles models using an in-memory lint profile.
Model references come from a static evaluation of the
Jinja in each model and in the macros it calls. SQLFluff parses compiled SQL to trace named confidential source columns into outputs.
An unresolved reference blocks the check. Unsupported SQL retains every input column. The scan-personal-data subcommand scans explicit local exports. See
``docs/quality_checks.md#model-metadata``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import json
import operator
import os
import re
import sys
import tempfile
from collections import Counter
from collections.abc import Generator, Iterator
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NamedTuple

from jinja2 import Environment, TemplateSyntaxError, nodes, select_autoescape

from scripts.process import TimeoutExpired, run_command

yaml: Any = importlib.import_module("yaml")

# The lineage worker compiles and parses every model. The whole check took 49 s on the development Mac, and the worker passed 90 s on a hosted runner.
LINEAGE_WORKER_SECONDS = 300
ROOT = Path(__file__).resolve().parents[2]
DOCUMENT = "docs/quality_checks.md#model-metadata"
DBT_DIRECTORY = "dbt"
PROJECT_FILE = "dbt/dbt_project.yml"
CONTRACT = "data_contracts/lakehouse/gold_contract.json"
BRONZE_MAP = "config/lakehouse/bronze_tables.json"
BRONZE_SOURCE = "bronze"
CLASSES = ("Public", "Confidential")
FACT_TYPES = ("transaction", "periodic_snapshot", "accumulating_snapshot")
ADDITIVITY = ("additive", "semi_additive", "non_additive")
ONC_SOURCES = frozenset({"bronze.onc_pi_attestations_csv", "bronze.onc_pi_chpl_linkage_csv"})
LOADER_LINEAGE_COLUMNS = frozenset({"_object_key", "_member_sha256"})
COST_REPORT_NOTE = "sum across hospitals only after choosing one report and period per hospital"
DBT_PATHS = (
    ("model-paths", "models"),
    ("seed-paths", "seeds"),
    ("analysis-paths", "analyses"),
    ("test-paths", "tests"),
    ("macro-paths", "macros"),
    ("snapshot-paths", "snapshots"),
)
DISABLED_MODEL = re.compile(r"\{\{-?\s*config\([^)]*\benabled\s*=\s*false\b")
NUMERIC_TYPES = frozenset({"TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "HUGEINT", "FLOAT", "REAL", "DOUBLE", "DECIMAL"})
OTHER_TYPES = frozenset({"VARCHAR", "BOOLEAN", "DATE", "TIMESTAMP", "TIME"})
DATE_PARTS = frozenset(
    {
        "data_year",
        "release_year",
        "rule_fiscal_year",
        "rule_fiscal_years",
        "data_fiscal_year",
        "file_fiscal_year",
        "fiscal_year",
        "program_year",
        "payment_year",
        "attestation_year",
        "attestation_month",
        "month_number",
        "file_year",
        "estimate_year",
    }
)
LINEAGE_COUNTS = frozenset(
    {
        "source_row_number",
        "line_number",
        "row_count",
        "hold_rows",
        "release_file_count",
        "releases_listed",
        "rows_for_provider_period",
        "reports_in_fiscal_year",
    }
)
NOT_MEASURES = DATE_PARTS | LINEAGE_COUNTS
BUILTIN_CALLS = frozenset({"config", "var", "env_var", "namespace", "range", "dict", "list", "zip", "run_query", "log", "tojson", "fromjson", "print"})
CONTEXT_NAMES = frozenset(
    {"exceptions", "adapter", "modules", "dbt", "context", "builtins", "target", "this", "model", "flags", "graph", "loop", "caller", "varargs", "kwargs"}
)
PACKAGE_MACROS = frozenset({"dbt_utils.get_column_values"})
MUTATORS = frozenset({"append", "extend", "update", "pop", "clear", "insert", "remove", "setdefault", "sort", "reverse", "add", "discard"})
STRING_METHODS = frozenset({"startswith", "endswith", "lower", "upper", "strip", "split", "replace"})
STEP_LIMIT = 200000
HARMLESS_CONTEXT_CALLS = frozenset({"adapter.quote", "exceptions.raise_compiler_error", "exceptions.warn", "modules.re.fullmatch"})
BINARY = {"+": operator.add, "-": operator.sub, "*": operator.mul, "/": operator.truediv, "//": operator.floordiv, "%": operator.mod}
COMPARISONS = {
    "eq": operator.eq,
    "ne": operator.ne,
    "lt": operator.lt,
    "lteq": operator.le,
    "gt": operator.gt,
    "gteq": operator.ge,
    "in": lambda left, right: left in right,
    "notin": lambda left, right: left not in right,
}
UNSUPPORTED_STATEMENTS = (nodes.Extends, nodes.Include, nodes.Import, nodes.FromImport)
SCOPED_STATEMENTS = (nodes.Scope, nodes.ScopedEvalContextModifier, nodes.FilterBlock, nodes.With, nodes.Block)
HELP = f"Fix the metadata in the YAML file. If the rule seems wrong for this case, ask the owner; never skip the hook. Rules: {DOCUMENT}"
ESCALATION = f"If the file is valid and the check is wrong, ask the owner; never skip the hook. Rules: {DOCUMENT}"


class Unknown:
    def __init__(self, reason: str = "") -> None:
        self.reason = reason

    def __repr__(self) -> str:
        return "?"


UNKNOWN: Any = Unknown()


class Unresolved(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class StepLimit(Exception):
    pass


class CannotRun(Exception):
    def __init__(self, path: str, line: int, cause: str, repair: str) -> None:
        super().__init__(cause)
        self.path, self.line, self.cause, self.repair = path, line, cause, repair


class Ref(NamedTuple):
    kind: str
    name: str


class Where(NamedTuple):
    path: str
    model: str


class Item(NamedTuple):
    data: dict[str, Any]
    node: Any
    line: int


@dataclass(frozen=True)
class Entry:
    name: str
    path: str
    line: int
    meta: dict[str, Any]
    columns: dict[str, dict[str, Any]] = field(default_factory=dict)
    column_lines: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Model:
    name: str
    path: str
    text: str
    is_fact: bool
    entry: Entry | None


@dataclass(frozen=True)
class Problem:
    rule: str
    path: str
    line: int
    message: str


@dataclass(frozen=True)
class Rule:
    class_name: Any
    reason: str
    exception: str
    line: int


@dataclass(frozen=True)
class Bronze:
    collections: dict[str, Rule]
    loaders: dict[str, Rule]
    overrides: dict[str, Rule]
    table_collection: dict[str, str]
    lines: dict[str, int]


@dataclass(frozen=True)
class MacroDef:
    node: nodes.Macro
    path: str


@dataclass(frozen=True)
class Context:
    project: str
    variables: dict[str, Any]
    models: frozenset[str]
    seeds: frozenset[str]
    sources: frozenset[str]
    macros: dict[str, MacroDef]
    macro_directories: str


@dataclass(frozen=True)
class Project:
    sources: dict[str, Entry]
    models: dict[str, Model]
    inputs: dict[str, frozenset[Ref]]
    output_columns: dict[str, dict[str, frozenset[str]]]
    conservative: dict[str, str]
    compiled_hashes: dict[str, str]
    analysis_errors: dict[str, str]
    measures: dict[str, tuple[str, ...]]
    contract_facts: frozenset[str]
    contract_lines: dict[str, int]
    bronze: Bronze
    problems: tuple[Problem, ...]


def read_text(root: Path, relative: str) -> str:
    try:
        return (root / relative).read_text()
    except (OSError, UnicodeDecodeError) as error:
        raise CannotRun(relative, 1, f"cannot read the file ({error.__class__.__name__})", "restore the file or correct its path") from error


def load_yaml(path: str, text: str) -> tuple[Any, Any]:
    try:
        return yaml.safe_load(text), yaml.compose(text)
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        raise CannotRun(path, mark.line + 1 if mark else 1, str(getattr(error, "problem", error)), "repair the YAML syntax in that file") from error


def load_json(path: str, text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise CannotRun(path, error.lineno, error.msg, "repair the JSON syntax in that file") from error


def parse_jinja(env: Environment, path: str, text: str) -> nodes.Template:
    try:
        return env.parse(text, filename=path)
    except TemplateSyntaxError as error:
        raise CannotRun(path, error.lineno, error.message or "invalid Jinja", "repair the Jinja syntax in that file") from error


def environment() -> Environment:
    return Environment(extensions=["jinja2.ext.do"], autoescape=select_autoescape(default_for_string=False))


def tests_as_macros(text: str) -> str:
    text = re.sub(r"\{%-?\s*test\s+", "{% macro test_", text)
    return re.sub(r"\{%-?\s*endtest\s*-?%\}", "{% endmacro %}", text)


def relative(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def json_line(text: str, needle: str) -> int:
    return next((number for number, line in enumerate(text.splitlines(), start=1) if f'"{needle}"' in line), 1)


def meta_of(node: Any) -> dict[str, Any]:
    meta = node.get("meta") if isinstance(node, dict) else None
    return meta if isinstance(meta, dict) else {}


def mapping_value(node: Any, key: str) -> Any:
    if isinstance(node, yaml.MappingNode):
        return next((value for key_node, value in node.value if key_node.value == key), None)
    return None


def items_of(data: Any, node: Any, key: str, path: str) -> list[Item]:
    if data is not None and not isinstance(data, dict):
        raise CannotRun(path, 1, "the file is not a YAML mapping", "repair the structure of that file")
    values = (data or {}).get(key) or []
    if not isinstance(values, list):
        raise CannotRun(path, node.start_mark.line + 1 if node is not None else 1, f"{key} is not a list", "repair the structure of that file")
    sequence = mapping_value(node, key)
    nodes_ = list(sequence.value) if isinstance(sequence, yaml.SequenceNode) else []
    found = []
    for index, value in enumerate(values):
        item_node = nodes_[index] if index < len(nodes_) else None
        line = item_node.start_mark.line + 1 if item_node is not None else 1
        if not isinstance(value, dict) or not isinstance(value.get("name"), str):
            raise CannotRun(path, line, f"an entry of {key} has no name", "give every entry a name")
        found.append(Item(value, item_node, line))
    return found


def project_directories(root: Path, project: dict[str, Any]) -> dict[str, list[Path]]:
    found: dict[str, list[Path]] = {}
    for key, default in DBT_PATHS:
        value = project.get(key, [default])
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise CannotRun(PROJECT_FILE, 1, f"{key} is not a list of folders", "repair the structure of that file")
        found[key] = [root / DBT_DIRECTORY / item for item in value]
    return found


def files_under(directories: list[Path], *suffixes: str) -> list[Path]:
    return sorted({path for directory in directories for suffix in suffixes for path in directory.rglob(f"*{suffix}") if path.is_file()})


@dataclass
class Properties:
    sources: dict[str, Entry] = field(default_factory=dict)
    models: dict[str, Entry] = field(default_factory=dict)
    seeds: set[str] = field(default_factory=set)
    problems: list[Problem] = field(default_factory=list)


def read_properties(root: Path, directories: dict[str, list[Path]]) -> Properties:
    found = Properties()
    scope = [directory for key, _ in DBT_PATHS for directory in directories[key]]
    for path in files_under(scope, ".yml", ".yaml"):
        where = relative(root, path)
        document, composed = load_yaml(where, path.read_text())
        for source in items_of(document, composed, "sources", where):
            for table in items_of(source.data, source.node, "tables", where):
                key = f"{source.data['name']}.{table.data['name']}"
                if key in found.sources:
                    found.problems.append(Problem("duplicate-entry", where, table.line, f"source {key} is defined more than once"))
                else:
                    columns = items_of(table.data, table.node, "columns", where)
                    found.sources[key] = Entry(
                        table.data["name"], where, table.line, meta_of(table.data), {column.data["name"]: meta_of(column.data) for column in columns}
                    )
        for model in items_of(document, composed, "models", where):
            columns = items_of(model.data, model.node, "columns", where)
            name = model.data["name"]
            if name in found.models:
                found.problems.append(Problem("duplicate-entry", where, model.line, f"model {name} is defined more than once in YAML"))
            else:
                column_meta = {column.data["name"]: meta_of(column.data) for column in columns}
                found.models[name] = Entry(name, where, model.line, meta_of(model.data), column_meta, {c.data["name"]: c.line for c in columns})
        found.seeds |= {seed.data["name"] for seed in items_of(document, composed, "seeds", where)}
    return found


def read_macros(root: Path, env: Environment, directories: list[Path]) -> tuple[dict[str, MacroDef], list[Problem]]:
    macros: dict[str, MacroDef] = {}
    problems: list[Problem] = []
    for path in files_under(directories, ".sql"):
        where = relative(root, path)
        for macro in parse_jinja(env, where, tests_as_macros(path.read_text())).find_all(nodes.Macro):
            if macro.name in macros:
                problems.append(Problem("duplicate-entry", where, macro.lineno, f"macro {macro.name} is also defined in {macros[macro.name].path}"))
            else:
                macros[macro.name] = MacroDef(macro, where)
    return macros, problems


def show(node: Any) -> str:
    if isinstance(node, nodes.Const):
        return repr(node.value)
    if isinstance(node, nodes.Name):
        return str(node.name)
    if isinstance(node, nodes.Getattr):
        return f"{show(node.node)}.{node.attr}"
    if isinstance(node, nodes.Getitem):
        return f"{show(node.node)}[{show(node.arg)}]"
    if isinstance(node, nodes.Concat):
        return " ~ ".join(show(item) for item in node.nodes)
    if isinstance(node, nodes.Call):
        return f"{show(node.node)}({', '.join(show(argument) for argument in node.args)})"
    return type(node).__name__


class Walker:
    def __init__(self, ctx: Context, model: Model, macros: dict[str, MacroDef]) -> None:
        self.ctx, self.model, self.macros = ctx, model, macros
        self.refs: set[Ref] = set()
        self.problems: dict[tuple[str, int, str], Problem] = {}
        self.seen: set[tuple[str, str]] = set()
        self.cache: dict[tuple[str, str], Any] = {}
        self.active: set[tuple[str, str]] = set()
        self.returns: list[list[Any]] = []
        self.steps = 0

    def fail(self, node: nodes.Node, where: Where, subject: str, reason: str) -> None:
        line = node.lineno or 1
        message = f"{subject} cannot be resolved ({reason}), reached from model {where.model}"
        self.problems.setdefault((where.path, line, subject), Problem("unresolved-reference", where.path, line, message))

    def value(self, node: nodes.Node, env: dict[str, Any]) -> Any:
        try:
            return self.evaluate(node, env)
        except (TypeError, ValueError, KeyError, IndexError, AttributeError, ZeroDivisionError) as error:
            raise Unresolved(f"the expression cannot be evaluated ({error.__class__.__name__})") from error

    def try_value(self, node: nodes.Node, env: dict[str, Any]) -> Any:
        try:
            return self.value(node, env)
        except Unresolved as error:
            return Unknown(error.reason)

    def truth(self, node: nodes.Node, env: dict[str, Any]) -> bool | None:
        try:
            return bool(self.value(node, env))
        except Unresolved:
            return None

    def text(self, item: Any) -> str:
        if isinstance(item, (str, int, float, bool)):
            return str(item)
        raise Unresolved("a list or mapping cannot be joined into text")

    def evaluate(self, node: nodes.Node, env: dict[str, Any]) -> Any:
        if isinstance(node, nodes.Const):
            return node.value
        if isinstance(node, nodes.TemplateData):
            return node.data
        if isinstance(node, nodes.Name):
            found = env.get(node.name, UNKNOWN)
            if isinstance(found, Unknown):
                raise Unresolved(found.reason or f"name '{node.name}' has no value the check can read")
            return found
        if isinstance(node, nodes.Tuple):
            return tuple(self.value(item, env) for item in node.items)
        if isinstance(node, nodes.List):
            return [self.value(item, env) for item in node.items]
        if isinstance(node, nodes.Dict):
            return {self.value(pair.key, env): self.value(pair.value, env) for pair in node.items}
        if isinstance(node, nodes.Concat):
            return "".join(self.text(self.value(item, env)) for item in node.nodes)
        if isinstance(node, nodes.And):
            left = self.value(node.left, env)
            return self.value(node.right, env) if left else left
        if isinstance(node, nodes.Or):
            left = self.value(node.left, env)
            return left if left else self.value(node.right, env)
        if isinstance(node, nodes.Not):
            return not self.value(node.node, env)
        if isinstance(node, nodes.BinExpr):
            return BINARY[node.operator](self.value(node.left, env), self.value(node.right, env))
        if isinstance(node, nodes.Compare):
            left = self.value(node.expr, env)
            for operand in node.ops:
                right = self.value(operand.expr, env)
                if not COMPARISONS[operand.op](left, right):
                    return False
                left = right
            return True
        if isinstance(node, nodes.CondExpr):
            chosen = node.expr1 if self.value(node.test, env) else node.expr2
            if chosen is None:
                raise Unresolved("a conditional expression has no else value")
            return self.value(chosen, env)
        if isinstance(node, nodes.Getattr):
            base = self.value(node.node, env)
            if isinstance(base, dict) and node.attr in base:
                return base[node.attr]
            raise Unresolved(f"attribute '{node.attr}' has no value the check can read")
        if isinstance(node, nodes.Getitem):
            base = self.value(node.node, env)
            argument = node.arg
            if isinstance(argument, nodes.Slice):
                bounds = [self.value(part, env) if part is not None else None for part in (argument.start, argument.stop, argument.step)]
                return base[slice(*bounds)]
            return base[self.value(argument, env)]
        if isinstance(node, nodes.Filter):
            return self.apply_filter(node, env)
        if isinstance(node, nodes.Call):
            return self.call_value(node, env)
        raise Unresolved(f"{type(node).__name__} expressions are not supported")

    def apply_filter(self, node: nodes.Filter, env: dict[str, Any]) -> Any:
        if node.node is None:
            raise Unresolved("a filter block is not supported")
        base = self.value(node.node, env)
        args = [self.value(argument, env) for argument in node.args]
        name = node.name
        if name == "unique":
            return [item for index, item in enumerate(base) if item not in base[:index]]
        if name == "list":
            return list(base)
        if name == "sort":
            return sorted(base)
        if name == "reverse":
            return list(reversed(base))
        if name == "join":
            return str(args[0] if args else "").join(self.text(item) for item in base)
        if name in ("length", "count"):
            return len(base)
        if name in ("first", "last"):
            return base[0 if name == "first" else -1]
        if name == "string":
            return self.text(base)
        if name == "lower":
            return self.text(base).lower()
        if name == "upper":
            return self.text(base).upper()
        if name in ("default", "d"):
            return base
        raise Unresolved(f"filter '{name}' is not supported")

    def call_value(self, node: nodes.Call, env: dict[str, Any]) -> Any:
        callee = node.node
        if isinstance(callee, nodes.Name):
            if callee.name == "var":
                return self.variable([self.value(argument, env) for argument in node.args])
            if callee.name == "range":
                return list(range(*[self.value(argument, env) for argument in node.args]))
            if callee.name in self.macros:
                return self.macro_value(callee.name, node, env)
            raise Unresolved(f"{callee.name}() is not a static value")
        if isinstance(callee, nodes.Getattr):
            if isinstance(callee.node, nodes.Name) and callee.node.name == self.ctx.project and callee.attr in self.macros:
                return self.macro_value(callee.attr, node, env)
            return self.method(self.value(callee.node, env), callee.attr, [self.value(argument, env) for argument in node.args])
        raise Unresolved("the call is not a static value")

    def variable(self, args: list[Any]) -> Any:
        if args[0] in self.ctx.variables:
            return self.ctx.variables[args[0]]
        if len(args) > 1:
            return args[1]
        raise Unresolved(f"var '{args[0]}' is not set in {PROJECT_FILE}")

    def method(self, base: Any, attr: str, args: list[Any]) -> Any:
        if isinstance(base, dict):
            if attr in ("items", "keys", "values"):
                return list(getattr(base, attr)())
            if attr == "get":
                return base.get(args[0], args[1] if len(args) > 1 else None)
        if isinstance(base, str) and attr in STRING_METHODS:
            return getattr(base, attr)(*args)
        raise Unresolved(f"method '{attr}' is not a static value")

    def bind_arguments(self, macro: nodes.Macro, args: list[Any], kwargs: dict[str, Any]) -> dict[str, Any]:
        names = [argument.name for argument in macro.args]
        offset = len(names) - len(macro.defaults)
        bound: dict[str, Any] = {}
        for index, name in enumerate(names):
            if index < len(args):
                bound[name] = args[index]
            elif name in kwargs:
                bound[name] = kwargs[name]
            else:
                bound[name] = self.try_value(macro.defaults[index - offset], {}) if index >= offset else UNKNOWN
        return bound

    def macro_arguments(self, node: nodes.Call, env: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
        return [self.try_value(argument, env) for argument in node.args], {keyword.key: self.try_value(keyword.value, env) for keyword in node.kwargs}

    def macro_value(self, name: str, node: nodes.Call, env: dict[str, Any]) -> Any:
        macro = self.macros[name]
        bound = self.bind_arguments(macro.node, *self.macro_arguments(node, env))
        key = (name, repr(bound))
        if key in self.cache:
            return self.cache[key]
        if key in self.active:
            raise Unresolved(f"macro '{name}' calls itself")
        self.active.add(key)
        self.returns.append([])
        try:
            self.walk(macro.node.body, bound, Where(macro.path, self.model.name))
        finally:
            returned = self.returns.pop()
            self.active.discard(key)
        distinct = {repr(item): item for item in returned}
        if len(distinct) != 1 or any(isinstance(item, Unknown) for item in distinct.values()):
            raise Unresolved(f"macro '{name}' does not return one static value")
        self.cache[key] = next(iter(distinct.values()))
        return self.cache[key]

    def walk(self, body: list[nodes.Node], env: dict[str, Any], where: Where) -> None:
        for node in body:
            self.statement(node, env, where)

    def statement(self, node: nodes.Node, env: dict[str, Any], where: Where) -> None:
        self.steps += 1
        if self.steps > STEP_LIMIT:
            raise StepLimit
        if isinstance(node, nodes.Assign):
            self.expression(node.node, env, where)
            self.bind(node.target, self.try_value(node.node, env), env)
        elif isinstance(node, nodes.For):
            self.loop(node, env, where)
        elif isinstance(node, nodes.If):
            self.branch(node, env, where)
        elif isinstance(node, nodes.Output):
            for child in node.nodes:
                self.expression(child, env, where)
        elif isinstance(node, nodes.ExprStmt):
            self.expression(node.node, env, where)
        elif isinstance(node, nodes.CallBlock):
            self.expression(node.call, env, where)
            self.walk(node.body, dict(env), where)
        elif isinstance(node, nodes.AssignBlock):
            self.walk(node.body, dict(env), where)
            self.bind(node.target, UNKNOWN, env)
        elif isinstance(node, UNSUPPORTED_STATEMENTS):
            name = type(node).__name__.lower()
            self.fail(node, where, f"{{% {name} %}}", f"Jinja statement '{name}' is not supported")
        elif isinstance(node, SCOPED_STATEMENTS):
            inner = dict(env)
            for target in getattr(node, "targets", []):
                self.bind(target, UNKNOWN, inner)
            self.walk(node.body, inner, where)
        elif not isinstance(node, (nodes.Macro, nodes.EvalContextModifier, nodes.TemplateData)):
            for part in node.iter_child_nodes():
                if isinstance(part, nodes.Stmt):
                    self.statement(part, env, where)
                else:
                    self.expression(part, env, where)

    def bind(self, target: nodes.Node, value: Any, env: dict[str, Any]) -> None:
        if isinstance(target, nodes.Name):
            env[target.name] = value
        elif isinstance(target, nodes.Tuple):
            exact = isinstance(value, (list, tuple)) and len(value) == len(target.items)
            for index, item in enumerate(target.items):
                self.bind(item, value[index] if exact else UNKNOWN, env)

    def loop(self, node: nodes.For, env: dict[str, Any], where: Where) -> None:
        self.expression(node.iter, env, where)
        iterable = self.try_value(node.iter, env)
        items = list(iterable) if isinstance(iterable, (dict, list, tuple)) else [UNKNOWN]
        for item in items:
            inner = dict(env)
            self.bind(node.target, item, inner)
            inner["loop"] = UNKNOWN
            if node.test is not None and self.truth(node.test, inner) is False:
                continue
            self.walk(node.body, inner, where)
        self.walk(node.else_, dict(env), where)

    def isolated(self, body: list[nodes.Node], env: dict[str, Any], where: Where) -> None:
        inner = dict(env)
        self.walk(body, inner, where)
        for name, value in inner.items():
            if name not in env or env[name] is not value:
                env[name] = UNKNOWN

    def branch(self, node: nodes.If, env: dict[str, Any], where: Where) -> None:
        undecided = False
        for test, body in [(node.test, node.body), *((arm.test, arm.body) for arm in node.elif_)]:
            self.expression(test, env, where)
            decision = self.truth(test, env)
            if decision is True and not undecided:
                self.walk(body, env, where)
                return
            if decision is None or undecided:
                undecided = True
                self.isolated(body, env, where)
        if undecided:
            self.isolated(node.else_, env, where)
        else:
            self.walk(node.else_, env, where)

    def expression(self, node: nodes.Node, env: dict[str, Any], where: Where) -> None:
        if not isinstance(node, nodes.Call):
            for child in node.iter_child_nodes():
                self.expression(child, env, where)
            return
        category, name = self.classify(node.node, env)
        if category in ("ref", "source"):
            self.reference(node, category, env, where)
        elif category == "macro":
            self.invoke(node, name, env, where)
        elif category == "return" and self.returns:
            self.returns[-1].append(self.try_value(node.args[0], env) if node.args else None)
        elif category == "mutate":
            env[name] = UNKNOWN
        elif category == "unresolved":
            self.fail(node, where, f"{show(node.node)}(...)", name)
        for child in (*node.args, *(keyword.value for keyword in node.kwargs)):
            self.expression(child, env, where)
        if not isinstance(node.node, nodes.Name):
            self.expression(node.node, env, where)

    def classify(self, callee: nodes.Node, env: dict[str, Any]) -> tuple[str, str]:
        directories = self.ctx.macro_directories
        if show(callee) in HARMLESS_CONTEXT_CALLS | PACKAGE_MACROS:
            return "builtin", show(callee)
        if isinstance(callee, nodes.Name):
            name = callee.name
            if name in ("ref", "source", "return"):
                return name, name
            if name in self.macros:
                return "macro", name
            if name in BUILTIN_CALLS:
                return "builtin", name
            return "unresolved", f"macro '{name}' is not defined in {directories} or is an unresolved callable alias"
        if isinstance(callee, nodes.Getattr) and isinstance(callee.node, nodes.Name):
            base = callee.node.name
            if base == "builtins" and callee.attr in ("ref", "source"):
                return callee.attr, callee.attr
            if base == self.ctx.project:
                known = callee.attr in self.macros
                return ("macro", callee.attr) if known else ("unresolved", f"macro '{base}.{callee.attr}' is not defined in {directories}")
            if f"{base}.{callee.attr}" in HARMLESS_CONTEXT_CALLS | PACKAGE_MACROS:
                return "builtin", base
            if base in env and callee.attr in MUTATORS:
                return "mutate", base
            if base not in env and base not in CONTEXT_NAMES:
                return "unresolved", f"macro '{base}.{callee.attr}' belongs to a package the check cannot read"
        if isinstance(callee, nodes.Getattr):
            value = self.try_value(callee.node, env)
            if (isinstance(value, dict) and callee.attr in {"items", "keys", "values", "get"}) or (isinstance(value, str) and callee.attr in STRING_METHODS):
                return "builtin", ""
        return "unresolved", f"callable '{show(callee)}' has no proven dependency-free meaning"

    def reference(self, node: nodes.Call, kind: str, env: dict[str, Any], where: Where) -> None:
        subject = f"{kind}({', '.join(show(argument) for argument in node.args)})"
        try:
            if node.kwargs or node.dyn_args or node.dyn_kwargs or not 1 <= len(node.args) <= 2:
                raise Unresolved("only one or two positional arguments are supported")
            args = [self.value(argument, env) for argument in node.args]
            if not all(isinstance(argument, str) for argument in args):
                raise Unresolved("the arguments are not text")
            found = self.resolve(kind, args)
        except Unresolved as error:
            self.fail(node, where, subject, error.reason)
            return
        self.refs |= found

    def resolve(self, kind: str, args: list[str]) -> set[Ref]:
        if kind == "source":
            if len(args) != 2:
                raise Unresolved("a source needs a source name and a table name")
            key = ".".join(args)
            if key not in self.ctx.sources:
                raise Unresolved(f"no source table '{key}' is declared")
            return {Ref("source", key)}
        if len(args) == 2 and args[0] != self.ctx.project:
            raise Unresolved(f"package '{args[0]}' is not this project")
        name = args[-1]
        if name in self.ctx.models:
            return {Ref("model", name)}
        if name in self.ctx.seeds:
            return set()
        raise Unresolved(f"no model or seed named '{name}'")

    def invoke(self, node: nodes.Call, name: str, env: dict[str, Any], where: Where) -> None:
        macro = self.macros[name]
        bound = self.bind_arguments(macro.node, *self.macro_arguments(node, env))
        key = (name, repr(bound))
        if key not in self.seen:
            self.seen.add(key)
            self.walk(macro.node.body, bound, Where(macro.path, where.model))


def inputs_of(ctx: Context, env: Environment, model: Model) -> Walker:
    tree = parse_jinja(env, model.path, model.text)
    local = {macro.name: MacroDef(macro, model.path) for macro in tree.find_all(nodes.Macro)}
    walker = Walker(ctx, model, {**ctx.macros, **local})
    try:
        walker.walk(tree.body, {}, Where(model.path, model.name))
    except StepLimit:
        walker.fail(tree, Where(model.path, model.name), "the template", "the evaluation did not finish within the step limit")
    return walker


def valid_confidential_columns(value: Any) -> bool:
    if value == "all":
        return True
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(column, str) and re.fullmatch(r"[a-z_][a-z0-9_]*", column) for column in value)
        and len(value) == len(set(value))
    )


def source_columns(entry: Entry, key: str) -> dict[str, frozenset[str]]:
    known: dict[str, frozenset[str]] = {column.lower(): frozenset() for column in entry.columns}
    if class_of(entry) != "Confidential":
        return known
    columns = entry.meta.get("confidential_columns")
    if columns == "all" or not isinstance(columns, list) or not valid_confidential_columns(columns):
        protected = frozenset({f"{key}.*"})
        known = {column: protected for column in known}
        if columns == "all" and key in ONC_SOURCES:
            known.update({column: frozenset() for column in LOADER_LINEAGE_COLUMNS})
        return {**known, "*": protected}
    return {**known, **{column: frozenset({f"{key}.{column}"}) for column in columns}}


def fallback_outputs(sources: dict[str, Entry], inputs: dict[str, frozenset[Ref]]) -> dict[str, dict[str, frozenset[str]]]:
    found: dict[str, dict[str, frozenset[str]]] = {name: {} for name in inputs}
    for _ in range(len(inputs) + 1):
        changed = False
        for name, refs in inputs.items():
            values = set().union(
                *(
                    set().union(*source_columns(sources[ref.name], ref.name).values()) if ref.kind == "source" else set().union(*found[ref.name].values())
                    for ref in refs
                )
            )
            result = {"*": frozenset(values)} if values else {}
            if result != found[name]:
                found[name] = result
                changed = True
        if not changed:
            break
    return found


def compiled_lineage(
    root: Path, project: dict[str, Any], sources: dict[str, Entry], models: dict[str, Model], inputs: dict[str, frozenset[Ref]], invalid: bool
) -> tuple[dict[str, dict[str, frozenset[str]]], dict[str, str], dict[str, str], dict[str, str]]:
    if invalid:
        return fallback_outputs(sources, inputs), {name: "metadata or reference errors prevent compilation" for name in models}, {}, {}
    runtime = root / ".tools" / "sqlfluff" / "bin" / "python"
    if not runtime.is_file():
        raise CannotRun(".tools/sqlfluff", 1, "the installed SQLFluff runtime is missing", "restore the project tool environment")
    payload = {
        "root": str(root),
        "profile": project.get("profile", project["name"]),
        "models": {name: model.path for name, model in models.items()},
        "sources": {key: {column: sorted(origins) for column, origins in source_columns(entry, key).items()} for key, entry in sources.items()},
        "inputs": {name: [list(ref) for ref in refs] for name, refs in inputs.items()},
    }
    with tempfile.TemporaryDirectory(prefix="hai_metadata_sql_") as directory:
        scratch = Path(directory)
        request = scratch / "request.json"
        request.write_text(json.dumps(payload))
        env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "DBT_LOG_PATH": str(scratch / "logs"), "DBT_TARGET_PATH": str(scratch / "target")}
        packages = (root / ".tools").resolve().parent / "data" / "analytics" / "dbt" / "dbt_packages"
        if packages.is_dir():
            env["DBT_PACKAGES_INSTALL_PATH"] = str(packages)
        try:
            result = run_command(
                str(runtime), ["-m", "scripts.quality.model_metadata", "--lineage-worker", str(request)], cwd=root, env=env, timeout=LINEAGE_WORKER_SECONDS
            )
        except TimeoutExpired as error:
            raise CannotRun(
                ".tools/sqlfluff",
                1,
                f"SQL lineage worker exceeded {error.timeout} seconds while compiling and parsing models",
                "inspect the dbt templater and retry this check",
            ) from error
        except OSError as error:
            raise CannotRun(
                ".tools/sqlfluff", 1, f"SQL lineage worker failed ({type(error).__name__})", "check the installed SQLFluff runtime and retry"
            ) from error
        if result.returncode != 0 or not (scratch / "result.json").is_file():
            raise CannotRun(".tools/sqlfluff", 1, "SQL lineage worker did not produce a result", "check the installed dbt templater and retry")
        response = load_json("SQL lineage result", (scratch / "result.json").read_text())
        outputs = {name: {column: frozenset(origins) for column, origins in values.items()} for name, values in response["outputs"].items()}
        return outputs, response["conservative"], response["compiled_hashes"], response["analysis_errors"]


class UnsupportedSQL(Exception):
    pass


class RelationBinding(NamedTuple):
    columns: dict[str, set[str]]
    unknown: set[str]


class ColumnLineage:
    def __init__(self, query_type: Any, relations: dict[str, tuple[str, str]], sources: dict[str, dict[str, set[str]]]) -> None:
        self.query_type, self.relations, self.sources = query_type, relations, sources
        self.queries: dict[str, Any] = {}
        self.outputs: dict[str, dict[str, set[str]]] = {}
        self.active_models: set[str] = set()
        self.active_queries: set[int] = set()
        self.conservative: dict[str, str] = {}
        self.analysis_errors: dict[str, str] = {}
        self.inputs: dict[str, list[list[str]]] = {}

    def all_source_origins(self) -> set[str]:
        return {origin for columns in self.sources.values() for values in columns.values() for origin in values}

    def analysis_failed(self, name: str, error: Exception) -> None:
        self.queries.pop(name, None)
        self.conservative[name] = "SQL analysis failed"
        self.analysis_errors[name] = f"{type(error).__name__}: {error}"

    def relation(self, raw: str) -> RelationBinding:
        key = re.sub(r'["`\s]', "", raw).lower()
        found = self.relations.get(key)
        if found is None:
            raise UnsupportedSQL("unresolved relation in compiled SQL")
        kind, name = found
        columns = self.sources.get(name, {}) if kind == "source" else self.model_outputs(name) if kind == "model" else {}
        unknown = columns.get("*", set()) if kind == "source" else {origin for values in columns.values() for origin in values}
        return RelationBinding(columns, unknown)

    def dependencies(self, query: Any, seen: set[int]) -> tuple[set[tuple[str, str]], bool]:
        if id(query) in seen:
            return set(), False
        seen.add(id(query))
        found: set[tuple[str, str]] = set()
        unresolved = False
        for selectable in query.selectables:
            if selectable.selectable.is_type("values_clause"):
                continue
            info = selectable.select_info
            if info is None:
                continue
            for alias in info.table_aliases:
                if alias.object_reference is not None:
                    raw = alias.object_reference.raw
                    cte = query.lookup_cte(raw.strip('"').upper(), pop=False)
                    if cte is not None:
                        extra, unknown = self.dependencies(cte, seen)
                        found.update(extra)
                        unresolved |= unknown
                    else:
                        relation = self.relations.get(re.sub(r'["`\s]', "", raw).lower())
                        if relation is None:
                            unresolved = True
                        elif relation[0] in {"source", "model"}:
                            found.add(relation)
                else:
                    element = alias.from_expression_element
                    local = next(element.recursive_crawl("select_statement", "with_compound_statement", "set_expression", "values_clause"), None)
                    functions = {function.raw.lower() for function in element.recursive_crawl("function_name")}
                    if local is None and not (functions and functions <= {"unnest", "range", "generate_series"}):
                        unresolved = True
        for child in query.children:
            extra, unknown = self.dependencies(child, seen)
            found.update(extra)
            unresolved |= unknown
        return found, unresolved

    def reachable_sources(self, name: str) -> set[str]:
        found: set[str] = set()
        pending = [name]
        visited = set()
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            for kind, parent in self.inputs.get(current, []):
                if kind == "source":
                    found.update(origin for origins in self.sources.get(parent, {}).values() for origin in origins)
                else:
                    pending.append(parent)
        return found

    def all_inputs(self, name: str) -> set[str]:
        found: set[str] = set()
        for kind, parent in self.inputs.get(name, []):
            if kind == "source":
                row = self.sources.get(parent, {})
            elif parent in self.active_models:
                row = {"*": self.reachable_sources(parent)}
            else:
                row = self.model_outputs(parent)
            found.update(origin for origins in row.values() for origin in origins)
        return found

    def model_outputs(self, name: str) -> dict[str, set[str]]:
        if name in self.outputs:
            return self.outputs[name]
        if name in self.active_models:
            raise UnsupportedSQL("recursive model dependency")
        self.active_models.add(name)
        try:
            query = self.queries.get(name)
            if query is None:
                raise UnsupportedSQL(self.conservative.get(name, "compiled SQL has no supported query"))
            result = self.query_outputs(query)
        except UnsupportedSQL as error:
            self.conservative[name] = str(error)
            if str(error) in {"dbt template could not be compiled", "compiled SQL could not be parsed", "dbt compilation failed", "SQL analysis failed"}:
                result = {"*": self.all_source_origins()}
            else:
                result = {"*": self.all_inputs(name)}
        except Exception as error:
            self.analysis_failed(name, error)
            result = {"*": self.all_source_origins()}
        finally:
            self.active_models.remove(name)
        self.outputs[name] = result
        return result

    def query_outputs(self, query: Any) -> dict[str, set[str]]:
        if query.cte_definition_segment is not None and query.cte_definition_segment.get_child("cte_column_list") is not None:
            raise UnsupportedSQL("CTE column alias list")
        identity = id(query)
        if identity in self.active_queries:
            raise UnsupportedSQL("recursive CTE")
        self.active_queries.add(identity)
        try:
            branches = [self.select_outputs(selectable, query) for selectable in query.selectables]
            for child in query.children:
                self.query_outputs(child)
            if not branches:
                raise UnsupportedSQL("query has no supported select list")
            if len(branches) == 1:
                return branches[0]
            if any("*" in branch for branch in branches) or len({len(branch) for branch in branches}) != 1:
                raise UnsupportedSQL("union output width or wildcard cannot be resolved")
            names = list(branches[0])
            return {name: set().union(*(list(branch.values())[index] for branch in branches)) for index, name in enumerate(names)}
        finally:
            self.active_queries.remove(identity)

    def select_outputs(self, selectable: Any, query: Any) -> dict[str, set[str]]:
        info = selectable.select_info
        if info is None or not info.select_targets:
            raise UnsupportedSQL("unsupported selectable")
        from_clause = selectable.selectable.get_child("from_clause")
        if from_clause is not None:
            for element in from_clause.recursive_crawl(
                "from_expression_element", no_recursive_seg_type=["select_statement", "with_compound_statement", "set_expression"]
            ):
                table = element.get_child("table_expression")
                if table is not None and table.get_child("function") is not None:
                    raise UnsupportedSQL("table function source")
        bindings: dict[str, list[RelationBinding]] = {}
        relations = []
        outer_names, outer_columns = self.outer_scope(query)
        for alias in info.table_aliases:
            if any(keyword.raw_upper == "LATERAL" for keyword in alias.from_expression_element.recursive_crawl("keyword")):
                raise UnsupportedSQL("lateral source")
            if alias.alias_expression is not None and next(alias.alias_expression.recursive_crawl("bracketed"), None) is not None:
                raise UnsupportedSQL("relation column alias list")
            if alias.object_reference is not None:
                raw = alias.object_reference.raw
                cte = query.lookup_cte(raw.strip('"').upper(), pop=False)
                if cte is not None:
                    columns = self.query_outputs(cte)
                    row = RelationBinding(columns, {origin for values in columns.values() for origin in values})
                else:
                    row = self.relation(raw)
                table_name = list(alias.object_reference.iter_raw_references())[-1].part.strip('"').lower()
                names = {table_name, alias.ref_str.lower()}
            else:
                nested = next(
                    alias.from_expression_element.recursive_crawl("with_compound_statement", "set_expression", "select_statement", recurse_into=False), None
                )
                if nested is None:
                    raise UnsupportedSQL("table function or lateral source")
                columns = self.query_outputs(self.query_type.from_segment(nested, query.dialect, parent=query))
                row = RelationBinding(columns, {origin for values in columns.values() for origin in values})
                names = {alias.ref_str.lower()}
            relations.append(row)
            for name in names:
                bindings.setdefault(name, []).append(row)
        result: dict[str, set[str]] = {}
        for index, element in enumerate(info.select_targets):
            if any(function.get_child("dot") is not None for function in element.recursive_crawl("function_name")):
                raise UnsupportedSQL("dotted function call")
            if next(element.recursive_crawl("column_index"), None) is not None:
                raise UnsupportedSQL("positional column reference")
            wildcard = element.get_child("wildcard_expression")
            if wildcard is not None:
                if any(child.is_code and child.get_type() not in {"wildcard_identifier", "wildcard_exclude"} for child in wildcard.segments):
                    raise UnsupportedSQL("wildcard replacement or rename")
                identifier = wildcard.get_child("wildcard_identifier")
                qualifier = identifier.raw.rsplit(".", 1)[0].strip('"').lower() if identifier is not None and "." in identifier.raw else None
                if qualifier is not None and qualifier not in bindings:
                    raise UnsupportedSQL("unresolved or struct wildcard")
                rows = bindings.get(qualifier or "", relations)
                excluded = {
                    ref.raw.strip('"').lower() for child in wildcard.get_children("wildcard_exclude") for ref in child.recursive_crawl("column_reference")
                }
                for row in rows:
                    for column, upstream in row.columns.items():
                        if column not in excluded:
                            result.setdefault(column, set()).update(upstream)
                continue
            if next(element.recursive_crawl("select_statement", "set_expression", "with_compound_statement"), None) is not None:
                raise UnsupportedSQL("subquery in output expression")
            if any(function.raw.lower() == "columns" for function in element.recursive_crawl("function_name")):
                raise UnsupportedSQL("dynamic COLUMNS expression")
            alias = element.get_child("alias_expression")
            if alias is not None and alias.get_child("naked_identifier", "quoted_identifier") is None:
                raise UnsupportedSQL("unsupported output alias")
            references = list(element.recursive_crawl("column_reference"))
            output_name = (
                alias.get_child("naked_identifier", "quoted_identifier").raw.strip('"').lower()
                if alias is not None
                else references[0].raw.rsplit(".", 1)[-1].strip('"').lower()
                if len(references) == 1 and element.get_child("column_reference") is not None
                else f"__expression_{index}"
            )
            origins: set[str] = set()
            for reference in references:
                origins.update(self.reference_origins(reference, bindings, relations, result, outer_names, outer_columns))
            result.setdefault(output_name, set()).update(origins)
            if output_name.startswith("__expression_") and origins:
                result.setdefault("*", set()).update(origins)
        predicate_refs = {
            id(reference)
            for predicate in selectable.selectable.recursive_crawl(
                "where_clause", "having_clause", "join_on_condition", no_recursive_seg_type=["select_statement", "with_compound_statement", "set_expression"]
            )
            for reference in predicate.recursive_crawl(
                "column_reference", no_recursive_seg_type=["select_statement", "with_compound_statement", "set_expression"]
            )
        }
        predicate_correlation = not self.is_correlated_source(query)
        for reference in info.reference_buffer:
            if not reference.is_type("wildcard_identifier"):
                self.reference_origins(
                    reference,
                    bindings,
                    relations,
                    result,
                    outer_names,
                    outer_columns,
                    allow_correlation=predicate_correlation and id(reference) in predicate_refs,
                )
        return result

    @staticmethod
    def scope_ancestors(query: Any) -> Iterator[tuple[Any, Any]]:
        """Cross CTE bodies through their WITH owner before visiting the enclosing query scopes."""
        while query.parent is not None:
            while query.cte_definition_segment is not None and query.parent is not None:
                query = query.parent
            parent = query.parent
            if parent is None:
                return
            yield query, parent
            query = parent

    @staticmethod
    def is_correlated_source(query: Any) -> bool:
        """Keep FROM-derived queries conservative even when their outer reference is in a predicate."""
        for scope, parent in ColumnLineage.scope_ancestors(query):
            for selectable in parent.selectables:
                info = selectable.select_info
                if info is not None and any(
                    child is scope.selectables[0].selectable
                    for alias in info.table_aliases
                    for child in alias.from_expression_element.recursive_crawl("select_statement", "with_compound_statement", "set_expression")
                ):
                    return True
        return False

    def outer_scope(self, query: Any) -> tuple[set[str], set[str]]:
        names: set[str] = set()
        columns: set[str] = set()
        for scope, parent in self.scope_ancestors(query):
            for selectable in parent.selectables:
                if not any(child is scope.selectables[0].selectable for child in selectable.selectable.recursive_crawl("select_statement")):
                    continue
                info = selectable.select_info
                if info is None:
                    continue
                for alias in info.table_aliases:
                    if any(child is scope.selectables[0].selectable for child in alias.from_expression_element.recursive_crawl("select_statement")):
                        break
                    names.add(alias.ref_str.lower())
                    if alias.object_reference is not None:
                        raw = alias.object_reference.raw
                        names.add(list(alias.object_reference.iter_raw_references())[-1].part.strip('"').lower())
                        cte = parent.lookup_cte(raw.strip('"').upper(), pop=False)
                        values = self.query_outputs(cte) if cte is not None else self.relation(raw).columns
                        columns.update(values)
                    else:
                        nested = next(
                            alias.from_expression_element.recursive_crawl("with_compound_statement", "set_expression", "select_statement", recurse_into=False),
                            None,
                        )
                        if nested is not None:
                            columns.update(self.query_outputs(self.query_type.from_segment(nested, query.dialect, parent=parent)))
        return names, columns

    def column_origins(
        self, name: str, relations: list[RelationBinding], aliases: dict[str, set[str]], outer_columns: set[str], *, allow_correlation: bool = False
    ) -> set[str]:
        if any(name in row.columns for row in relations):
            return {origin for row in relations for origin in row.columns.get(name, row.columns.get("*", set()))}
        if name in aliases:
            unknown = relations[0].unknown if len(relations) == 1 else {origin for row in relations for values in row.columns.values() for origin in values}
            return aliases[name] | unknown
        if name in outer_columns or "*" in outer_columns or not relations:
            if allow_correlation:
                return set()
            raise UnsupportedSQL("correlated source or column without a local relation")
        if len(relations) == 1:
            return relations[0].unknown
        return {origin for row in relations for values in row.columns.values() for origin in values}

    def reference_origins(
        self,
        reference: Any,
        bindings: dict[str, list[RelationBinding]],
        relations: list[RelationBinding],
        aliases: dict[str, set[str]],
        outer_names: set[str],
        outer_columns: set[str],
        *,
        allow_correlation: bool = False,
    ) -> set[str]:
        """Attribute candidate relations, allowing predicate-only outer references outside FROM sources."""
        parts = [part.part.strip('"').lower() for part in reference.iter_raw_references()]
        if not parts or len(parts) > 2:
            raise UnsupportedSQL("struct or multipart column reference")
        if len(parts) == 1 and parts[0] in bindings:
            return {origin for row in bindings[parts[0]] for values in row.columns.values() for origin in values}
        if len(parts) == 2:
            if parts[0] in bindings:
                return self.column_origins(parts[1], bindings[parts[0]], {}, set())
            if parts[0] in outer_names:
                if allow_correlation:
                    return set()
                raise UnsupportedSQL("correlated source")
            return self.column_origins(parts[0], relations, aliases, outer_columns, allow_correlation=allow_correlation)
        if parts[0] in outer_names and parts[0] not in aliases and not any(parts[0] in row.columns for row in relations):
            if allow_correlation:
                return set()
            raise UnsupportedSQL("correlated source")
        return self.column_origins(parts[0], relations, aliases, outer_columns, allow_correlation=allow_correlation)


def lineage_worker(request: Path) -> int:
    sqlfluff = importlib.import_module("sqlfluff.core")
    templater_module = importlib.import_module("sqlfluff_templater_dbt.templater")
    query_module = importlib.import_module("sqlfluff.utils.analysis.query")
    errors_module = importlib.import_module("sqlfluff.core.errors")
    payload = json.loads(request.read_text())
    root, scratch = Path(payload["root"]), request.parent
    (scratch / "profiles.yml").write_text(
        yaml.safe_dump({payload["profile"]: {"target": "lint", "outputs": {"lint": {"type": "duckdb", "path": ":memory:", "threads": 1}}}})
    )
    config = sqlfluff.FluffConfig(
        configs={
            "core": {"dialect": "duckdb", "templater": "dbt"},
            "templater": {
                "dbt": {
                    "project_dir": str(root / "dbt"),
                    "profiles_dir": str(scratch),
                    "profile": payload["profile"],
                    "target": "lint",
                    "target_path": str(scratch / "target"),
                }
            },
        },
        overrides={"dialect": "duckdb", "templater": "dbt"},
    )
    templater = templater_module.DbtTemplater()
    linter = sqlfluff.Linter(dialect="duckdb")
    sources = {name: {column: set(origins) for column, origins in columns.items()} for name, columns in payload["sources"].items()}
    lineage = ColumnLineage(query_module.Query, {}, sources)
    lineage.inputs = payload["inputs"]
    compiled_hashes = {}
    dependency_queries = {}
    compile_errors = (errors_module.SQLFluffUserError, errors_module.SQLTemplaterError)
    for name, path in payload["models"].items():
        try:
            compiled, violations = templater.process(in_str=(root / path).read_text(), fname=str(root / path), config=config)
            if compiled is None or violations:
                raise UnsupportedSQL("dbt template could not be compiled")
            compiled_hashes[name] = hashlib.sha256(compiled.templated_str.encode()).hexdigest()
            parsed = linter.parse_string(compiled.templated_str)
            if parsed.tree is None or parsed.violations or next(parsed.tree.recursive_crawl("unparsable"), None) is not None:
                raise UnsupportedSQL("compiled SQL could not be parsed")
            dependency_queries[name] = query_module.Query.from_root(parsed.tree, linter.config.get("dialect_obj"))
            if any({"BY", "NAME"} <= set(operator.raw.upper().split()) for operator in parsed.tree.recursive_crawl("set_operator")):
                raise UnsupportedSQL("union by name")
            lineage.queries[name] = query_module.Query.from_root(parsed.tree, linter.config.get("dialect_obj"))
        except (*compile_errors, UnsupportedSQL) as error:
            lineage.conservative[name] = str(error) if isinstance(error, UnsupportedSQL) else "dbt compilation failed"
        except Exception as error:
            dependency_queries.pop(name, None)
            lineage.analysis_failed(name, error)
    for node in [*templater.dbt_manifest.nodes.values(), *templater.dbt_manifest.sources.values()]:
        if node.resource_type not in {"model", "source", "seed"} or not node.relation_name:
            continue
        name = f"{node.source_name}.{node.name}" if node.resource_type == "source" else node.name
        lineage.relations[re.sub(r'["`\s]', "", node.relation_name).lower()] = (node.resource_type, name)
    unresolved = set()
    for name, query in dependency_queries.items():
        try:
            dependencies, unknown = lineage.dependencies(query, set())
        except Exception as error:
            lineage.analysis_failed(name, error)
            continue
        dependencies.update((ref[0], ref[1]) for ref in lineage.inputs[name])
        dependencies.discard(("model", name))
        lineage.inputs[name] = [list(ref) for ref in sorted(dependencies)]
        if unknown:
            unresolved.add(name)
    for name in payload["models"]:
        lineage.model_outputs(name)
    for name in unresolved:
        lineage.conservative[name] = "unresolved relation in compiled SQL"
    response = {
        "outputs": {name: {column: sorted(origins) for column, origins in columns.items()} for name, columns in lineage.outputs.items()},
        "conservative": lineage.conservative,
        "analysis_errors": lineage.analysis_errors,
        "compiled_hashes": compiled_hashes,
    }
    (scratch / "result.json").write_text(json.dumps(response))
    return 0


def read_bronze(root: Path) -> Bronze:
    text = read_text(root, BRONZE_MAP)
    document = load_json(BRONZE_MAP, text)
    if not isinstance(document, dict) or not isinstance(document.get("tables"), list):
        raise CannotRun(BRONZE_MAP, 1, "the table map has no tables list", "repair the structure of that file")
    classification: dict[str, Any] = document["classification"] if isinstance(document.get("classification"), dict) else {}

    def rules(section: str) -> dict[str, Rule]:
        raw = classification.get(section) or {}
        if not isinstance(raw, dict):
            raise CannotRun(BRONZE_MAP, json_line(text, section), f"classification.{section} is not an object", "repair the structure of that file")
        return {name: make_rule(name, value) for name, value in raw.items()}

    def make_rule(name: str, value: Any) -> Rule:
        if not isinstance(value, dict):
            raise CannotRun(BRONZE_MAP, json_line(text, name), f"the class of {name} is not an object", "repair the structure of that file")
        return Rule(value.get("class"), str(value.get("reason") or ""), str(value.get("exception") or ""), json_line(text, name))

    overrides, table_collection = {}, {}
    for table in document["tables"]:
        if not isinstance(table, dict) or not isinstance(table.get("table"), str) or not isinstance(table.get("collection"), str):
            raise CannotRun(BRONZE_MAP, 1, "a table entry has no table or collection name", "repair the structure of that file")
        table_collection[table["table"]] = table["collection"]
        if "classification" in table:
            overrides[table["table"]] = make_rule(table["table"], table["classification"])
    lines = {name: json_line(text, name) for name in table_collection}
    return Bronze(rules("collections"), rules("loader_tables"), overrides, table_collection, lines)


def read_contract(root: Path) -> tuple[dict[str, list[tuple[str, str]]], dict[str, int]]:
    text = read_text(root, CONTRACT)
    document = load_json(CONTRACT, text)
    facts = document.get("facts") if isinstance(document, dict) else None
    if not isinstance(facts, dict):
        raise CannotRun(CONTRACT, 1, "the contract has no facts object", "repair the structure of that file")
    columns: dict[str, list[tuple[str, str]]] = {}
    for fact, spec in facts.items():
        listed = spec.get("columns") if isinstance(spec, dict) else None
        if not isinstance(listed, list) or not all(
            isinstance(item, dict) and isinstance(item.get("name"), str) and isinstance(item.get("type"), str) for item in listed
        ):
            raise CannotRun(CONTRACT, json_line(text, fact), f"{fact} has no list of columns with a name and a type", "repair the structure of that file")
        columns[fact] = [(item["name"], item["type"]) for item in listed]
    return columns, {fact: json_line(text, fact) for fact in facts}


def measure_inventory(columns: dict[str, list[tuple[str, str]]], lines: dict[str, int]) -> tuple[dict[str, tuple[str, ...]], list[Problem]]:
    measures: dict[str, tuple[str, ...]] = {}
    problems: list[Problem] = []
    for fact, listed in columns.items():
        found = []
        for name, declared in listed:
            base = re.sub(r"\(.*\)|\[\]", "", declared).strip().upper()
            if base in OTHER_TYPES or (base in NUMERIC_TYPES and name in NOT_MEASURES):
                continue
            if base in NUMERIC_TYPES:
                found.append(name)
            else:
                problems.append(
                    Problem("fact-contract", CONTRACT, lines[fact], f"contract column {fact}.{name} has type '{declared}' that the check does not know")
                )
        measures[fact] = tuple(found)
    return measures, problems


def load(root: Path) -> Project:
    """Return project metadata, dependencies and compiled column origins for the repository at root; raise CannotRun for unreadable inputs."""
    env = environment()
    project_data, _ = load_yaml(PROJECT_FILE, read_text(root, PROJECT_FILE))
    if not isinstance(project_data, dict) or not isinstance(project_data.get("name"), str):
        raise CannotRun(PROJECT_FILE, 1, "the project file has no name", "repair the structure of that file")
    directories = project_directories(root, project_data)
    properties = read_properties(root, directories)
    macros, problems = read_macros(root, env, directories["macro-paths"])
    problems = [*properties.problems, *problems]
    models: dict[str, Model] = {}
    for path in files_under(directories["model-paths"], ".sql"):
        where = relative(root, path)
        if path.stem in models:
            problems.append(Problem("duplicate-entry", where, 1, f"model {path.stem} has more than one SQL file ({models[path.stem].path})"))
            continue
        is_fact = path.stem.startswith("fct_") and "gold" in path.relative_to(root / DBT_DIRECTORY).parts
        text = path.read_text()
        if DISABLED_MODEL.search(text):
            continue
        models[path.stem] = Model(path.stem, where, text, is_fact, properties.models.get(path.stem))
    variables = dict(project_data.get("vars") or {})
    scoped = variables.get(project_data["name"])
    variables.update(scoped if isinstance(scoped, dict) else {})
    seeds = properties.seeds | {path.stem for path in files_under(directories["seed-paths"], ".csv")}
    ctx = Context(
        project_data["name"],
        variables,
        frozenset(models),
        frozenset(seeds),
        frozenset(properties.sources),
        macros,
        ", ".join(relative(root, directory) for directory in directories["macro-paths"]),
    )
    inputs = {}
    for name, model in models.items():
        walker = inputs_of(ctx, env, model)
        inputs[name] = frozenset(walker.refs - {Ref("model", name)})
        problems.extend(walker.problems.values())
    columns, lines = read_contract(root)
    measures, measure_problems = measure_inventory(columns, lines)
    outputs, conservative, compiled_hashes, analysis_errors = compiled_lineage(root, project_data, properties.sources, models, inputs, bool(problems))
    return Project(
        properties.sources,
        models,
        inputs,
        outputs,
        conservative,
        compiled_hashes,
        analysis_errors,
        measures,
        frozenset(columns),
        lines,
        read_bronze(root),
        tuple([*problems, *measure_problems]),
    )


def class_of(entry: Entry | None) -> str | None:
    value = entry.meta.get("classification") if entry else None
    return value if value in CLASSES else None


def rank(class_name: str | None) -> int:
    return CLASSES.index(class_name) if class_name in CLASSES else -1


def has_exception(meta: dict[str, Any]) -> bool:
    return isinstance(meta.get("exception"), str) and bool(meta["exception"].strip())


def lists_axes(value: Any) -> bool:
    return isinstance(value, list) and bool(value) and all(isinstance(axis, str) and axis.strip() for axis in value)


def bronze_class(bronze: Bronze, table: str) -> str | None:
    if table in bronze.table_collection:
        rule = bronze.overrides.get(table) or bronze.collections.get(bronze.table_collection[table])
    else:
        rule = bronze.loaders.get(table)
    return rule.class_name if rule and rule.class_name in CLASSES else None


def check_bronze(project: Project) -> list[Problem]:
    bronze, problems = project.bronze, []
    for kind, rules in (("collection", bronze.collections), ("table", bronze.overrides), ("loader table", bronze.loaders)):
        for name, rule in rules.items():
            if rule.class_name not in CLASSES:
                problems.append(
                    Problem("allowed-values", BRONZE_MAP, rule.line, f"bronze {kind} {name}: class {rule.class_name!r} is not one of {', '.join(CLASSES)}")
                )
            elif rule.class_name == "Confidential" and not rule.exception.strip():
                problems.append(Problem("exception", BRONZE_MAP, rule.line, f"bronze {kind} {name} is Confidential but has no exception"))
    for table, collection in bronze.table_collection.items():
        if table not in bronze.overrides and collection not in bronze.collections:
            problems.append(Problem("bronze-class", BRONZE_MAP, bronze.lines[table], f"bronze table {table}: collection {collection} has no class"))
    return problems


def check(project: Project) -> list[Problem]:
    problems: list[Problem] = list(project.problems)

    def add(rule: str, entry: Entry, message: str, line: int | None = None) -> None:
        problems.append(Problem(rule, entry.path, line or entry.line, message))

    def not_allowed(entry: Entry, label: str, value: Any, allowed: tuple[str, ...], line: int | None = None) -> None:
        add("allowed-values", entry, f"{label} {value!r} is not one of {', '.join(allowed)}", line)

    for declared_entry in [*project.sources.values(), *(model.entry for model in project.models.values() if model.entry is not None)]:
        for key in ("classification_override", "lineage_columns"):
            if key in declared_entry.meta:
                add("lineage-override", declared_entry, f"{declared_entry.name}: {key} is obsolete. Classification follows parsed output lineage")
    for key, source in project.sources.items():
        namespace, _, table = key.partition(".")
        own = class_of(source)
        if "classification" not in source.meta:
            add("source-class", source, f"source table {key} has no meta.classification")
        elif own is None:
            not_allowed(source, f"{key}: classification", source.meta["classification"], CLASSES)
        else:
            declared_columns = source.meta.get("confidential_columns")
            if own == "Confidential" and not valid_confidential_columns(declared_columns):
                add("confidential-columns", source, f"source {key} needs meta.confidential_columns as a nonempty list of explicit column names or all")
            reason = source.meta.get("confidential_columns_reason")
            if own == "Confidential" and declared_columns == "all" and (not isinstance(reason, str) or not reason.strip()):
                add("confidential-columns", source, f"source {key}: all needs a nonempty meta.confidential_columns_reason")
            if own == "Public" and declared_columns:
                add("confidential-columns", source, f"source {key} declares confidential_columns but is Public")
            if own == "Confidential" and not has_exception(source.meta):
                add("exception", source, f"source {key} is Confidential but has no meta.exception")
            label = table if namespace == BRONZE_SOURCE else key
            expected = bronze_class(project.bronze, table) if namespace == BRONZE_SOURCE else None
            known = namespace == BRONZE_SOURCE and (table in project.bronze.table_collection or table in project.bronze.loaders)
            if not known:
                add("bronze-class", source, f"source {label} has no bronze table")
            elif expected is not None and rank(own) < rank(expected):
                add("bronze-class", source, f"source {label} is {own} but its bronze table is {expected}")
    problems.extend(check_bronze(project))
    facts = {name for name, model in project.models.items() if model.is_fact}
    for name in sorted(facts - project.contract_facts):
        problems.append(Problem("fact-contract", project.models[name].path, 1, f"gold fact {name} is not in {CONTRACT}"))
    for name in sorted(project.contract_facts - facts):
        problems.append(Problem("fact-contract", CONTRACT, project.contract_lines[name], f"contract fact {name} has no model"))
    for model in project.models.values():
        entry = model.entry
        if entry is None:
            problems.append(Problem("model-class", model.path, 1, f"model {model.name} has no YAML entry, so no meta.classification"))
            continue
        if "classification" not in entry.meta:
            add("model-class", entry, f"model {model.name} has no meta.classification")
        elif (own := class_of(entry)) is None:
            not_allowed(entry, f"{model.name}: classification", entry.meta["classification"], CLASSES)
        else:
            outputs = project.output_columns[model.name]
            carried = set().union(*outputs.values()) if outputs else set()
            if rank(own) < rank("Confidential") and carried:
                attributed = False
                reason = project.conservative.get(model.name)
                explanation = f" (conservative: {reason})" if reason else ""
                for ref in sorted(project.inputs[model.name]):
                    parent_outputs = source_columns(project.sources[ref.name], ref.name) if ref.kind == "source" else project.output_columns[ref.name]
                    parent_carried = set().union(*parent_outputs.values()) if parent_outputs else set()
                    if parent_carried & carried:
                        attributed = True
                        label = ", ".join(sorted(carried & parent_carried))
                        add("class-inheritance", entry, f"{model.name} is {own} but its input {ref.name} is Confidential; output carries {label}{explanation}")
                if not attributed:
                    add("class-inheritance", entry, f"{model.name} is {own} but output carries {', '.join(sorted(carried))}{explanation}")
        if "confidential_columns" in entry.meta:
            add("confidential-columns", entry, f"{model.name}: confidential_columns is source-only; a model cannot authorize its own output columns")
        if project.conservative.get(model.name) == "unresolved relation in compiled SQL":
            add("unresolved-reference", entry, f"{model.name}: unresolved relation in compiled SQL")
        if not model.is_fact:
            continue
        if "fact_type" not in entry.meta:
            add("fact-type", entry, f"gold fact {model.name} has no meta.fact_type")
        elif entry.meta["fact_type"] not in FACT_TYPES:
            not_allowed(entry, f"{model.name}: fact_type", entry.meta["fact_type"], FACT_TYPES)
        for column, declared in entry.columns.items():
            if "additivity" in declared and declared["additivity"] not in ADDITIVITY:
                not_allowed(entry, f"{model.name}.{column}: additivity", declared["additivity"], ADDITIVITY, entry.column_lines.get(column))
            axes = declared.get("additive_across")
            if declared.get("additivity") == "semi_additive" and not lists_axes(axes):
                message = f"{model.name}.{column} is semi_additive but meta.additive_across is {axes!r}; it must list the axes the measure sums across"
                add("additive-across", entry, message, entry.column_lines.get(column))
            if model.name == "fct_cost_report" and declared.get("additivity") == "semi_additive" and declared.get("additivity_note") != COST_REPORT_NOTE:
                message = f"{model.name}.{column} is semi_additive but meta.additivity_note must be {COST_REPORT_NOTE!r}"
                add("additivity-note", entry, message, entry.column_lines.get(column))
        for column in project.measures.get(model.name, ()):
            if "additivity" not in entry.columns.get(column, {}):
                add("additivity", entry, f"measure column {model.name}.{column} has no meta.additivity", entry.column_lines.get(column))
    return sorted(set(problems), key=lambda problem: (problem.path, problem.line, problem.rule, problem.message))


SCHEMA_PATTERNS = {
    "name": re.compile(r"(?:^|_)(?:name|full_name|first_name|last_name|middle_name|surname|forename|contact_name|owner_name|person_name)(?:_|$)"),
    "address": re.compile(r"(?:^|_)(?:address|street|postal_address|mailing_address)(?:_|$)"),
    "date_of_birth": re.compile(r"(?:^|_)(?:dob|date_of_birth|birth_date|birthdate)(?:_|$)"),
    "identifier": re.compile(r"(?:^|_)(?:ssn|social_security|patient_id|member_id|person_id|passport|npi|associate_id|driver_license)(?:_|$)"),
    "email": re.compile(r"(?:^|_)(?:email|e_mail)(?:_|$)"),
    "phone": re.compile(r"(?:^|_)(?:phone|telephone|mobile|fax)(?:_|$)"),
}
TEXT_PATTERNS = {
    "name": re.compile(
        r"\b(?:[A-Z][a-z]{1,30}[ -]){1,3}[A-Z][a-z]{1,30}\b|"
        r"(?i:\b(?:name|contact|author|prepared\s+by|reviewed\s+by)\s*[:=-]\s*[a-z][a-z'-]*(?:\s+[a-z][a-z'-]*){0,3})"
    ),
    "address": re.compile(
        r"\b\d{1,6}\s+(?:[A-Za-z][A-Za-z0-9.-]*\s+){1,5}(?:street|st|avenue|ave|road|rd|lane|ln|drive|dr|boulevard|blvd|court|ct|way)\b", re.IGNORECASE
    ),
    "date_of_birth": re.compile(r"\b(?:dob|date\s+of\s+birth|born)\s*[:=-]?\s*(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b", re.IGNORECASE),
    "identifier": re.compile(r"\b\d{3}-\d{2}-\d{4}\b|\b(?:ssn|patient\s*id|member\s*id|passport|npi)\s*[:=-]\s*[A-Za-z0-9-]{3,}\b", re.IGNORECASE),
    "email": re.compile(r"\b[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "phone": re.compile(r"(?<!\d)(?:\+?1[ .-]?)?\(?\d{3}\)?[ .-]\d{3}[ .-]\d{4}(?!\d)"),
}
MAX_JSON_BYTES = 64 * 1024 * 1024


class ScanError(Exception):
    pass


class OutputConflict(ScanError):
    pass


def rows(path: Path) -> Generator[tuple[bool, Any], None, None]:
    if path.is_symlink() or not path.is_file():
        raise ScanError("input must be an existing regular file, not a symlink")
    if any(part.lower() in {"secrets", "private_original"} for part in path.parts) or path.name in {"registry.json", ".env"}:
        raise ScanError("input path is excluded from this export scanner")
    with path.open(encoding="utf-8-sig", newline="") as handle:
        match path.suffix.lower():
            case ".csv":
                reader = csv.DictReader(handle, strict=True)
                if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
                    raise ScanError("CSV must have unique column names")
                yield False, dict.fromkeys(reader.fieldnames)
                for row in reader:
                    if None in row or any(value is None for value in row.values()):
                        raise ScanError("CSV row has the wrong number of columns")
                    yield True, row
            case ".jsonl" | ".ndjson":
                for line in handle:
                    if line.strip():
                        yield True, json.loads(line)
            case ".json":
                if path.stat().st_size > MAX_JSON_BYTES:
                    raise ScanError("JSON exceeds the bounded-reader limit; export it as JSONL")
                value = json.load(handle)
                for row in value if isinstance(value, list) else [value]:
                    yield True, row
            case ".txt":
                for line in handle:
                    yield True, {"line_text": line}
            case _:
                raise ScanError("unsupported export format")


def cells(value: Any, columns: set[str], path: str = "") -> Iterator[tuple[str, str, str]]:
    if isinstance(value, dict):
        for index, (key, child) in enumerate(value.items()):
            schema_name = re.sub(r"[^a-z0-9]+", "_", str(key).lower()).strip("_")
            column = str(key) if str(key) in columns else f"field_{index}"
            nested = f"{path}.{column}" if path else column
            yield nested, schema_name, ""
            yield from cells(child, columns, nested)
    elif isinstance(value, list):
        for child in value:
            yield from cells(child, columns, f"{path}[]")
    elif value is not None:
        yield path or "value", "", str(value)


def scan_table(spec: dict[str, Any], base: Path) -> dict[str, Any]:
    counts: Counter[tuple[str, str, str]] = Counter()
    seen_columns: set[str] = set()
    row_count = 0
    declared = spec.get("columns")
    if "columns" in spec and (
        not isinstance(declared, list)
        or not declared
        or not all(isinstance(column, str) and column for column in declared)
        or len(declared) != len(set(declared))
    ):
        raise ScanError("declared schema needs a nonempty list of unique column names")
    columns = set(declared) if isinstance(declared, list) else set()
    if declared is None:
        first_csv = next((Path(file) for file in spec["files"] if Path(file).suffix.lower() == ".csv"), None)
        if first_csv is not None:
            first_csv = first_csv if first_csv.is_absolute() else base / first_csv
            with closing(rows(first_csv)) as header_rows:
                _, header = next(header_rows)
                columns = set(header)
    for file in spec["files"]:
        path = Path(file)
        path = path if path.is_absolute() else base / path
        for is_row, row in rows(path):
            row_count += int(is_row)
            for column, schema_name, text in cells(row, columns | {"line_text"} if path.suffix.lower() == ".txt" else columns):
                if schema_name and column not in seen_columns:
                    seen_columns.add(column)
                    for kind, pattern in SCHEMA_PATTERNS.items():
                        if pattern.search(schema_name):
                            counts[column, kind, "column_name"] += 1
                    if TEXT_PATTERNS["name"].search(schema_name.replace("_", " ").title()) and column.rsplit(".", 1)[-1].startswith("field_"):
                        counts[column, "name", "column_name"] += 1
                for kind, pattern in TEXT_PATTERNS.items():
                    counts[column, kind, "text"] += sum(1 for _ in pattern.finditer(text))
    findings = [{"column_path": column, "kind": kind, "basis": basis, "count": count} for (column, kind, basis), count in sorted(counts.items()) if count]
    return {"collection": spec["collection"], "table": spec["table"], "rows": row_count, "findings": findings}


def table_identity(spec: Any, index: int) -> tuple[str, str]:
    if not isinstance(spec, dict) or not isinstance(spec.get("collection"), str) or not re.fullmatch(r"[a-z0-9_]+(?:/[a-z0-9_]+)*", spec["collection"]):
        raise ScanError(f"invalid collection descriptor at table index {index}")
    if not isinstance(spec.get("table"), str) or not re.fullmatch(r"[a-z_][a-z0-9_]*", spec["table"]):
        raise ScanError(f"invalid table descriptor at table index {index}")
    return spec["collection"], spec["table"]


def scan(manifest: Path, output: Path, inventory: Path | None = None) -> dict[str, Any]:
    manifests = [manifest, *([inventory] if inventory is not None else [])]
    if output.is_symlink() or any(output.resolve() == path.resolve() or (output.exists() and path.exists() and output.samefile(path)) for path in manifests):
        raise OutputConflict("report path overlaps an input")
    document = json.loads(manifest.read_text())
    specs = document.get("tables") if isinstance(document, dict) else None
    if not isinstance(specs, list) or not specs:
        raise ScanError("manifest needs a nonempty tables list")
    identities = set()
    for index, spec in enumerate(specs):
        identity = table_identity(spec, index)
        files = spec.get("files")
        if not isinstance(files, list) or not files or not all(isinstance(file, str) and file for file in files) or len(files) != len(set(files)):
            raise ScanError(f"invalid export list at table index {index}")
        for file in files:
            path = Path(file)
            path = path if path.is_absolute() else manifest.parent / path
            if path.resolve() == output.resolve() or (path.exists() and output.exists() and path.samefile(output)):
                raise OutputConflict("report path overlaps an input")
        if identity in identities:
            raise ScanError(f"duplicate table descriptor at table index {index}")
        identities.add(identity)
    if inventory is not None:
        document = json.loads(inventory.read_text())
        required = document.get("tables") if isinstance(document, dict) else None
        if not isinstance(required, list) or not required:
            raise ScanError("inventory needs a nonempty tables list")
        expected = {table_identity(spec, index) for index, spec in enumerate(required)}
        if len(expected) != len(required):
            raise ScanError("inventory has duplicate table descriptors")
        if missing := expected - identities:
            raise ScanError(f"input manifest is missing {len(missing)} inventoried tables")
    tables = [scan_table(spec, manifest.parent) for spec in specs]
    return {"scope": "explicit local exports", "tables": tables, "matched_tables": sum(bool(table["findings"]) for table in tables), "complete": True}


def scan_personal_data(argv: list[str]) -> int:
    """Write counts for local exports named by a manifest. Return 1 for possible personal data or 2 for incomplete scanning."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, help="require every collection/table pair in this JSON tables list")
    args = parser.parse_args(argv)
    try:
        report = scan(args.manifest, args.output, args.inventory)
    except OutputConflict:
        sys.stderr.write("personal-data scan refused: report path overlaps an input\n")
        return 2
    except (OSError, UnicodeError, json.JSONDecodeError, csv.Error, ScanError, RecursionError) as error:
        cause = str(error) if isinstance(error, ScanError) else type(error).__name__
        args.output.write_text(json.dumps({"complete": False, "error_kind": cause, "tables": []}, indent=2) + "\n")
        sys.stderr.write("personal-data scan incomplete; inspect the export manifest and supported formats\n")
        return 2
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(f"personal-data scan: {len(report['tables'])} tables, {report['matched_tables']} with possible personal data\n")
    return int(report["matched_tables"] > 0)


def main(argv: list[str] | None = None) -> int:
    """Check the repository (or ``--root``) and return 1 when any metadata problem is found, 2 when the check cannot run."""
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "scan-personal-data":
        return scan_personal_data(argv[1:])
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--root", type=Path, default=ROOT, help="repository root that holds the dbt folder")
    parser.add_argument("--lineage-report", type=Path, help="write the derived column lineage and conservative model list as JSON")
    parser.add_argument("--lineage-worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.lineage_worker is not None:
        return lineage_worker(args.lineage_worker)
    root = args.root.resolve()
    try:
        project = load(root)
    except CannotRun as error:
        sys.stderr.write(f"model metadata check cannot run: {error.path}:{error.line}: {error.cause}. Fix: {error.repair}. {ESCALATION}\n")
        return 2
    if args.lineage_report is not None:
        evidence = {
            "checker_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "compiled_sql_sha256": project.compiled_hashes,
            "conservative": project.conservative,
            "analysis_errors": project.analysis_errors,
            "outputs": {name: {column: sorted(origins) for column, origins in columns.items()} for name, columns in project.output_columns.items()},
        }
        args.lineage_report.write_text(json.dumps(evidence, indent=2) + "\n")
    problems = check(project)
    for problem in problems:
        sys.stderr.write(f"{problem.path}:{problem.line}: [{problem.rule}] {problem.message}\n")
    if problems:
        sys.stderr.write(f"{len(problems)} model metadata problems. {HELP}\n")
        return 1
    columns = sum(len(names) for names in project.measures.values())
    facts = sum(model.is_fact for model in project.models.values())
    sys.stdout.write(f"model metadata ok: {len(project.sources)} sources, {len(project.models)} models, {facts} gold facts, {columns} measure columns\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
