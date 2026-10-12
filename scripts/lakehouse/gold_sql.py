"""Semantic checks over DuckDB's parse of compiled gold SQL (failure modes 840, 746 and the lineage rule).

``json_serialize_sql`` gives each compiled model as a syntax tree. This module resolves column lineage through CTEs, subqueries and
projections (each base column keeps the relation it comes from), lists the physical relations a model reads (local CTEs excluded) and
applies the expression policy:

* The contract's ``expression_allowlist`` entries switch on the surrogate-key expressions (``md5``, the unknown-member ``coalesce``, the
  ``varchar`` cast and the key ``case``); with an entry missing, every expression that needs it fails.
* A key expression is accepted only at its contracted output role: a dimension's ``<name>_sk`` hashes that dimension's natural key
  (``DIMENSION_KEYS``), a bridge's key columns hash theirs (``BRIDGE_KEYS``), a fact's key column is ``coalesce(<dimension>.<dimension>_sk,
  md5('<unknown>'))``, and a fact's key join hashes one of the fact's own natural keys. An alias suffix, a CTE name or ``md5`` alone grants
  nothing, and a role matches by shape and by the names of the columns it reads.
* Amendment A36 rule 8, A37, A40 and A43 add scoped permissions for named models and output roles; ``DATE_ROLES`` and the other constants
  below name each one. Everything else that computes a value fails: arithmetic, ``case``, aggregates, window expressions, casts and any function.
* Every expression node in the parse is visited or recognized by a role; a node nothing visited is reported, so an unsupported shape cannot hide.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, NamedTuple

Json = dict[str, Any]
Base = tuple[str, str]
Matcher = Callable[[Json, "Scope"], "Value | None"]

UNKNOWN = "<unknown>"
ARITHMETIC = frozenset({"+", "-", "*", "/", "%", "//", "**", "add", "subtract", "multiply", "divide", "mod"})
BOOLEAN_TYPES = frozenset(
    {"OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "OPERATOR_NOT", "COMPARE_IN", "COMPARE_NOT_IN", "COMPARE_BETWEEN", "COMPARE_NOT_BETWEEN"}
)
QUERY_NODES = frozenset({"SELECT_NODE", "SET_OPERATION_NODE"})
# Contract allowlist text to the capability it switches on (gold_contract.json expression_allowlist).
CAPABILITIES = {
    "md5(<natural key text>) for a surrogate key": "md5_key",
    "coalesce(<dimension>.<dimension>_sk, md5('<unknown>')) for the unknown member": "unknown_coalesce",
    "cast(<natural key> as varchar) inside a surrogate key": "varchar_cast",
    "case only inside surrogate-key or unknown-member handling": "key_case",
}


class KeySpec(NamedTuple):
    """The columns whose text a key hashes, in order; several columns are joined with ``separator`` by ``concat_ws``."""

    columns: tuple[str, ...]
    separator: str = "|"


# The natural key of each dimension. Sources: the dimension grains of gold_contract.json, amendments A36 rules 1, 3 to 7 and A40 item 1, and
# the unknown_member_natural_key tests of G2's checkpoint revision 3. A code list keys on its code system and code (A36 rule 4).
CODE_LIST = KeySpec(("code_system", "code"))
DIMENSION_KEYS = {
    "dim_census_tract": KeySpec(("census_tract_geoid",)),
    "dim_change_type": CODE_LIST,
    "dim_condition": CODE_LIST,
    "dim_county": KeySpec(("county_fips",)),
    "dim_date": KeySpec(("calendar_date",)),
    "dim_demographic_group": CODE_LIST,
    "dim_drg": CODE_LIST,
    "dim_ehr_product": CODE_LIST,
    "dim_enrollment": KeySpec(("enrollment_id",)),
    "dim_footnote": CODE_LIST,
    "dim_hospital": KeySpec(("ccn",)),
    "dim_measure": KeySpec(("fact", "measure_key", "value_column")),
    "dim_owner_organization": KeySpec(("associate_id",)),
    "dim_owner_role": CODE_LIST,
    "dim_registry_control": KeySpec(("measure_control",)),
    "dim_release": KeySpec(("member_sha256",)),
    "dim_rurality_code": CODE_LIST,
    "dim_source": KeySpec(("dataset_id",)),
    "dim_state": KeySpec(("state_code",)),
    "dim_zip": KeySpec(("zip_code",)),
}
# Amendments A39 and A36 rule 3: the control bridge keys its measure on the measure identity and its control on the control ID.
BRIDGE_KEYS = {
    "bridge_measure_control": {
        "measure_sk": KeySpec(("fact", "measure_key", "value_column")),
        "registry_control_sk": KeySpec(("measure_control",)),
    }
}
# Amendment A36 rule 3: dim_measure also carries its identity text, the natural key its key hashes.
IDENTITY_OUTPUTS = {"dim_measure": ("measure_identity", KeySpec(("fact", "measure_key", "value_column")))}
# Amendment A36 rule 1 (names as renamed by A38 item 1): the date parts of dim_date and the function that makes each from calendar_date.
CODE_LABEL = "code_label"
DATE_ROLES = {
    "iso_year": frozenset({"isoyear"}),
    "iso_week": frozenset({"weekofyear", "week"}),
    "month": frozenset({"month"}),
    "month_number": frozenset({"month"}),
    "quarter": frozenset({"quarter"}),
    "quarter_number": frozenset({"quarter"}),
    "calendar_year": frozenset({"year"}),
}
CALENDAR_COLUMN = "calendar_date"
FISCAL_COLUMN = "federal_fiscal_year"
SOURCE_DATE_MODELS = frozenset({"dim_date", "dim_hospital"})
# Amendment A36 rule 5: dim_release reads the stored copies.
RELEASE_MODEL = "dim_release"
RELEASE_RELATION = "stg_bronze__file_copies"
PUBLICATION_COLUMN = "publication_release"
OBJECT_COLUMN = "object_key"
# Amendment A43: the only function calls accepted in a predicate, in WHERE only, on the model's natural-key column: model to (function, column, argument).
VALUE_SPACE = {
    "dim_hospital": ("length", "ccn", 6),
    "dim_county": ("length", "county_fips", 5),
    "dim_state": ("regexp_matches", "state_code", "^[A-Z]{2}$"),
    "dim_zip": ("regexp_matches", "zip_code", "^[0-9]{5}$"),
    "dim_census_tract": ("regexp_matches", "census_tract_geoid", "^[0-9]{11}$"),
    "dim_owner_organization": ("regexp_matches", "associate_id", "^[0-9]{10}$"),
}


class PolicyError(ValueError):
    """The contract's allowlist holds an entry this check does not know, or a dimension has no natural-key specification."""


def capabilities(allowlist: tuple[str, ...]) -> frozenset[str]:
    """The capabilities the contract allowlist switches on; an unknown entry is an error, not a silent grant."""
    unknown = [entry for entry in allowlist if entry not in CAPABILITIES]
    if unknown:
        raise PolicyError(f"expression_allowlist entries this check cannot read: {unknown}")
    return frozenset(CAPABILITIES[entry] for entry in allowlist)


@dataclass(frozen=True)
class Policy:
    """Everything the expression policy reads from the contract and from DuckDB."""

    capabilities: frozenset[str]
    value_columns: frozenset[str]
    date_pairs: frozenset[Base]
    aggregates: frozenset[str]


@dataclass(frozen=True)
class Finding:
    """One violation: ``check`` is arithmetic, case, functions or lineage."""

    check: str
    message: str


@dataclass(frozen=True)
class Col:
    """An output column: its name, the (relation, column) bases it derives from and what kind of expression made it."""

    name: str
    bases: frozenset[Base]
    labels: frozenset[str]
    inputs: tuple[frozenset[Base], ...] = ()


@dataclass
class Out:
    """The output of a query: its columns and the physical relations whose unnamed columns (a star) also flow through."""

    columns: list[Col]
    star_relations: tuple[str, ...] = ()

    def get(self, name: str) -> Col | None:
        return next((column for column in self.columns if column.name == name), None)


@dataclass(frozen=True)
class Value:
    bases: frozenset[Base]
    labels: frozenset[str]
    inputs: tuple[frozenset[Base], ...] = ()


@dataclass
class Source:
    """A relation in a FROM clause; ``out`` is None for a physical relation (its columns are not known)."""

    alias: str
    out: Out | None
    relation: str = "?"


@dataclass
class Scope:
    sources: list[Source]
    ctes: dict[str, Out]


@dataclass
class Analysis:
    findings: list[Finding]
    reads: set[str]
    top: Out


COLUMN = frozenset({"column"})
CONSTANT = frozenset({"const"})
OTHER = frozenset({"other"})
EMPTY: frozenset[Base] = frozenset()


def dimension_key(dimension: str) -> str:
    """A dimension's surrogate-key column: its name without ``dim_`` plus ``_sk``."""
    return dimension.removeprefix("dim_") + "_sk"


def column_names(bases: frozenset[Base]) -> set[str]:
    return {column for _relation, column in bases}


def is_column(node: Json | None) -> bool:
    return node is not None and node["class"] == "COLUMN_REF"


def column_name(node: Json | None) -> str | None:
    return str(node["column_names"][-1]) if is_column(node) and node is not None else None


def is_text(node: Json | None, text: str | None = None) -> bool:
    """A VARCHAR constant, equal to ``text`` when one is given."""
    if node is None or node["class"] != "CONSTANT" or node["value"]["is_null"] or node["value"]["type"]["id"] != "VARCHAR":
        return False
    return text is None or node["value"]["value"] == text


def constant_cast(node: Json) -> bool:
    """A constant, or a cast of one (a typed null)."""
    return node["class"] == "CONSTANT" or (node["class"] == "CAST" and not node["try_cast"] and constant_cast(node["child"]))


def is_unknown_hash(node: Json | None) -> bool:
    return (
        node is not None
        and node["class"] == "FUNCTION"
        and node["function_name"] == "md5"
        and len(node["children"]) == 1
        and is_text(node["children"][0], UNKNOWN)
    )


def innermost_constant(node: Json) -> Any:
    """The number inside an interval built by ``to_days(cast(trunc(cast(3 as double)) as integer))``, or None for another shape."""
    if node["class"] == "CONSTANT":
        return node["value"]["value"]
    if node["class"] == "CAST" and not node["try_cast"]:
        return innermost_constant(node["child"])
    if node["class"] == "FUNCTION" and node["function_name"] == "trunc" and len(node["children"]) == 1:
        return innermost_constant(node["children"][0])
    return None


def interval(node: Json | None, unit: str, amount: int) -> bool:
    """``interval <amount> <unit>`` as DuckDB parses it: ``to_days`` or ``to_months`` of a constant."""
    if node is None or node["class"] != "FUNCTION" or node["function_name"] != unit or len(node["children"]) != 1:
        return False
    return innermost_constant(node["children"][0]) == amount


def merge_inputs(left: tuple[frozenset[Base], ...], right: tuple[frozenset[Base], ...]) -> tuple[frozenset[Base], ...]:
    """The hashed inputs of a key column across the arms of a set operation; arms that disagree cannot be bound to one natural key."""
    if not left or not right or left == right:
        return left or right
    return (frozenset({("!", "arms differ")}),)


def child_nodes(node: Any) -> list[Json]:
    """The expression nodes directly under a node, found through any containers between them (so no field is skipped)."""
    found: list[Json] = []
    values = list(node.values()) if isinstance(node, dict) else list(node) if isinstance(node, list) else []
    for value in values:
        if isinstance(value, dict):
            if "class" in value:
                found.append(value)
            elif value.get("type") not in QUERY_NODES:
                found += child_nodes(value)
        elif isinstance(value, list):
            found += child_nodes(value)
    return found


class Walker:
    """One pass over a model's parse: collects findings and physical reads, and resolves lineage."""

    def __init__(self, model: str, kind: str, policy: Policy, fact_map: dict[str, tuple[str, str]]) -> None:
        self.model = model
        self.kind = kind
        self.policy = policy
        self.fact_map = fact_map
        self.joined: dict[str, tuple[str, str]] = {}
        self.findings: list[Finding] = []
        self.reads: set[str] = set()
        self.seen: set[int] = set()

    def flag(self, check: str, message: str) -> None:
        self.findings.append(Finding(check, f"{self.model}: {message}"))

    def mark(self, node: Any) -> None:
        """Record that a whole subtree was recognized by a role."""
        if isinstance(node, dict):
            self.seen.add(id(node))
            for value in node.values():
                self.mark(value)
        elif isinstance(node, list):
            for value in node:
                self.mark(value)

    def sweep(self, node: Any) -> None:
        """Report any expression node that no handler visited and no role recognized."""
        if isinstance(node, dict):
            if "class" in node and id(node) not in self.seen:
                self.flag("functions", f"expression {node['class']} was not analyzed (unsupported shape)")
                return
            for value in node.values():
                self.sweep(value)
        elif isinstance(node, list):
            for value in node:
                self.sweep(value)

    # Queries and relations.
    def query(self, node: Json, ctes: dict[str, Out]) -> Out:
        local = dict(ctes)
        for entry in (node.get("cte_map") or {}).get("map", []):
            out = self.query(entry["value"]["query"]["node"], local)
            aliases = entry["value"].get("aliases") or []
            if aliases:
                out = Out([replace(column, name=alias) for alias, column in zip(aliases, out.columns, strict=False)], out.star_relations)
            local[entry["key"]] = out
        if node["type"] == "SELECT_NODE":
            out = self.select(node, local)
        elif node["type"] == "SET_OPERATION_NODE":
            left, right = self.query(node["left"], local), self.query(node["right"], local)
            merged = [
                Col(a.name, a.bases | b.bases, a.labels | b.labels, merge_inputs(a.inputs, b.inputs)) for a, b in zip(left.columns, right.columns, strict=False)
            ]
            out = Out(merged, tuple(dict.fromkeys(left.star_relations + right.star_relations)))
        else:
            self.flag("functions", f"query node {node['type']} is not supported")
            return Out([], ("?",))
        for modifier in node.get("modifiers", []):
            self.modifier(modifier, Scope([Source("", out)], local))
        return out

    def modifier(self, modifier: Json, scope: Scope) -> None:
        for order in modifier.get("orders", []):
            self.expr(order["expression"], scope, "predicate")
        for key in ("limit", "offset"):
            if modifier.get(key):
                self.expr(modifier[key], scope, "predicate")

    def select(self, node: Json, ctes: dict[str, Out]) -> Out:
        scope = Scope([], ctes)
        self.relations(node["from_table"], scope)
        columns: list[Col] = []
        stars: list[str] = []
        for item in node["select_list"]:
            found, relations = self.item(item, scope)
            columns += found
            stars += relations
        if node.get("where_clause"):
            self.where(node["where_clause"], scope)
        for clause in ("having", "qualify"):
            if node.get(clause):
                self.expr(node[clause], scope, "predicate")
        for group in node.get("group_expressions", []):
            self.expr(group, scope, "predicate")
        return Out(columns, tuple(dict.fromkeys(stars)))

    def relations(self, ref: Json, scope: Scope) -> None:
        kind = ref["type"]
        if kind == "BASE_TABLE":
            name = ref["table_name"]
            if not ref["schema_name"] and not ref["catalog_name"] and name in scope.ctes:
                scope.sources.append(Source(ref.get("alias") or name, scope.ctes[name]))
                return
            self.reads.add(name)
            if ref["catalog_name"] not in ("", "staging") or ref["schema_name"] not in ("", "main"):
                self.flag("lineage", f"reads {ref['catalog_name']}.{ref['schema_name']}.{name}, outside the build's main schema")
            scope.sources.append(Source(ref.get("alias") or name, None, name))
        elif kind == "JOIN":
            self.relations(ref["left"], scope)
            self.relations(ref["right"], scope)
            if ref.get("condition") and not self.key_join(ref["condition"], scope):
                self.expr(ref["condition"], scope, "predicate")
        elif kind == "SUBQUERY":
            out = self.query(ref["subquery"]["node"], scope.ctes)
            names = ref.get("column_name_alias") or []
            if names:
                out = Out([replace(column, name=alias) for alias, column in zip(names, out.columns, strict=False)], out.star_relations)
            scope.sources.append(Source(ref.get("alias") or "", out))
        elif kind == "TABLE_FUNCTION":
            name = str(ref["function"]["function_name"]).lower()
            self.flag("lineage", f"reads through table function {name}")
            self.expr(ref["function"], scope, "predicate")
            scope.sources.append(Source(ref.get("alias") or name, None, "?"))
        elif kind != "EMPTY":
            self.flag("lineage", f"relation kind {kind} is not supported")

    # WHERE clause: amendment A43 value-space filters, then the ordinary predicate rules.
    def where(self, node: Json, scope: Scope) -> None:
        parts = [node]
        while any(part["class"] == "CONJUNCTION" and part["type"] == "CONJUNCTION_AND" for part in parts):
            parts = [
                child for part in parts for child in (part["children"] if part["class"] == "CONJUNCTION" and part["type"] == "CONJUNCTION_AND" else [part])
            ]
        self.mark_conjunctions(node)
        for part in parts:
            if not self.value_space(part, scope):
                self.expr(part, scope, "predicate")

    def mark_conjunctions(self, node: Json) -> None:
        """The AND nodes that ``where`` took apart are visited through their parts."""
        if node["class"] == "CONJUNCTION" and node["type"] == "CONJUNCTION_AND":
            self.seen.add(id(node))
            for child in node["children"]:
                self.mark_conjunctions(child)

    def value_space(self, node: Json, scope: Scope) -> bool:
        """The one filter amendment A43 names for this model, on its natural-key column."""
        rule = VALUE_SPACE.get(self.model)
        if rule is None:
            return False
        function, column, argument = rule
        call = node
        if function == "length":
            if (
                node["class"] != "COMPARISON"
                or node["type"] != "COMPARE_EQUAL"
                or node["right"]["class"] != "CONSTANT"
                or node["right"]["value"]["value"] != argument
            ):
                return False
            call = node["left"]
        if call["class"] != "FUNCTION" or call["function_name"] != function or not call["children"] or column_name(call["children"][0]) != column:
            return False
        if function == "regexp_matches" and not (len(call["children"]) == 2 and is_text(call["children"][1], str(argument))):
            return False
        if function == "length" and len(call["children"]) != 1:
            return False
        self.expr(call["children"][0], scope, "predicate")
        self.mark(node)
        return True

    # Select-list items and their output roles.
    def item(self, node: Json, scope: Scope) -> tuple[list[Col], list[str]]:
        if node["class"] == "STAR":
            return self.star(node, scope)
        alias = node.get("alias") or column_name(node) or "expression"
        value = self.output(node, alias, scope)
        return [Col(alias, value.bases, value.labels, value.inputs)], []

    def star(self, node: Json, scope: Scope) -> tuple[list[Col], list[str]]:
        self.seen.add(id(node))
        replaced = {entry["key"]: self.expr(entry["value"], scope, "output") for entry in node.get("replace_list", [])}
        if node.get("expr"):
            self.expr(node["expr"], scope, "output")
        excluded = set(node.get("exclude_list", []))
        columns: list[Col] = []
        relations: list[str] = []
        for source in scope.sources:
            if node["relation_name"] and source.alias != node["relation_name"]:
                continue
            if source.out is None:
                relations.append(source.relation)
                continue
            relations += list(source.out.star_relations)
            for column in source.out.columns:
                if column.name in excluded:
                    continue
                if column.name in replaced:
                    columns.append(Col(column.name, replaced[column.name].bases, replaced[column.name].labels, replaced[column.name].inputs))
                else:
                    columns.append(column)
        return columns, relations

    def output(self, node: Json, alias: str, scope: Scope) -> Value:
        """An output expression. A computed value in a role the policy names (a key, an identity, a date part) must have that role's shape."""
        if is_column(node) or constant_cast(node):
            return self.expr(node, scope, "output")
        normalized = self.normalization(node, alias, scope)
        if normalized is not None:
            return normalized
        matcher = self.role(alias)
        if matcher is None:
            return self.expr(node, scope, "output")
        value = matcher(node, scope)
        if value is not None:
            self.mark(node)
            return value
        self.flag("functions", f"output column {alias} is not the contracted expression for that column")
        return self.expr(node, scope, "output")

    def role(self, alias: str) -> Matcher | None:
        """The matcher for a computed output with this name in this model, or None."""
        if self.kind == "fact":
            mapping = self.fact_map.get(alias)
            return (lambda node, scope: self.unknown_coalesce(node, scope, mapping)) if mapping else None
        if self.kind == "bridge":
            bridge_spec = BRIDGE_KEYS.get(self.model, {}).get(alias)
            return (lambda node, scope: self.key_hash(node, scope, bridge_spec)) if bridge_spec is not None else None
        spec = DIMENSION_KEYS.get(self.model)
        if spec is not None and alias == dimension_key(self.model):
            return lambda node, scope: self.key_hash(node, scope, spec)
        identity = IDENTITY_OUTPUTS.get(self.model)
        if identity and alias == identity[0]:
            return lambda node, scope: self.identity(node, scope, identity[1])
        if self.model == "dim_date":
            if alias == CALENDAR_COLUMN:
                return self.calendar_date
            if alias == FISCAL_COLUMN:
                return self.fiscal_year
            if alias in DATE_ROLES:
                return lambda node, scope: self.date_part(node, scope, DATE_ROLES[alias])
        return None

    # Roles.
    def key_hash(self, node: Json, scope: Scope, spec: KeySpec) -> Value | None:
        """``md5`` of the natural key's text, or ``md5('<unknown>')``. The value keeps the base columns of each hashed natural-key column."""
        if node["class"] != "FUNCTION" or node["function_name"] != "md5" or len(node["children"]) != 1 or "md5_key" not in self.policy.capabilities:
            return None
        argument = node["children"][0]
        if is_text(argument, UNKNOWN):
            return Value(EMPTY, frozenset({"md5_key"}))
        parts = self.key_text(argument, scope, spec)
        if parts is None:
            return None
        return Value(frozenset(base for part in parts for base in part), frozenset({"md5_key"}), parts)

    def key_text(self, node: Json, scope: Scope, spec: KeySpec) -> tuple[frozenset[Base], ...] | None:
        """Bases of each natural-key column in one of the contracted forms, or None."""
        caps = self.policy.capabilities
        if len(spec.columns) > 1:
            return self.concat(node, scope, spec)
        name = spec.columns[0]
        if is_column(node):
            return (self.column(node, scope).bases,) if column_name(node) == name else None
        if node["class"] == "CAST" and not node["try_cast"] and node["cast_type"]["id"] == "VARCHAR" and column_name(node["child"]) == name:
            return (self.column(node["child"], scope).bases,) if "varchar_cast" in caps else None
        if node["class"] == "CASE" and "key_case" in caps:
            return self.guard(node, scope, name)
        return None

    def concat(self, node: Json, scope: Scope, spec: KeySpec) -> tuple[frozenset[Base], ...] | None:
        """``concat_ws(separator, c1, c2, ...)`` over the spec's columns, each plain or cast to varchar."""
        parts = node["children"] if node["class"] == "FUNCTION" and node["function_name"] == "concat_ws" else []
        if len(parts) != len(spec.columns) + 1 or not is_text(parts[0], spec.separator):
            return None
        bases: list[frozenset[Base]] = []
        for part, name in zip(parts[1:], spec.columns, strict=True):
            if is_column(part) and column_name(part) == name:
                bases.append(self.column(part, scope).bases)
            elif (
                part["class"] == "CAST"
                and not part["try_cast"]
                and part["cast_type"]["id"] == "VARCHAR"
                and column_name(part["child"]) == name
                and "varchar_cast" in self.policy.capabilities
            ):
                bases.append(self.column(part["child"], scope).bases)
            else:
                return None
        return tuple(bases)

    def guard(self, node: Json, scope: Scope, name: str) -> tuple[frozenset[Base], ...] | None:
        """``case when c is null then '<unknown>' else cast(c as varchar) end`` on the key column; the cast needs its own allowlist entry."""
        checks = node["case_checks"]
        if len(checks) != 1 or not node.get("else_expr"):
            return None
        when, then, other = checks[0]["when_expr"], checks[0]["then_expr"], node["else_expr"]
        if not (when["class"] == "OPERATOR" and when["type"] == "OPERATOR_IS_NULL" and column_name(when["children"][0]) == name and is_text(then, UNKNOWN)):
            return None
        if is_column(other) and column_name(other) == name:
            return (self.column(other, scope).bases,)
        if other["class"] == "CAST" and not other["try_cast"] and other["cast_type"]["id"] == "VARCHAR" and column_name(other["child"]) == name:
            return (self.column(other["child"], scope).bases,) if "varchar_cast" in self.policy.capabilities else None
        return None

    def normalization(self, node: Json, alias: str, scope: Scope) -> Value | None:
        """``cast(<column> as varchar) as <natural-key or code column>`` in a dimension: the text normalization of a key or code, which the
        allowlist's varchar-cast entry covers; it is accepted for that output column only and fails when the entry is missing."""
        if self.kind != "dimension" or node["class"] != "CAST" or node["try_cast"] or node["cast_type"]["id"] != "VARCHAR" or not is_column(node["child"]):
            return None
        spec = DIMENSION_KEYS.get(self.model)
        if spec is None or alias not in spec.columns + (CODE_LABEL,) * (spec is CODE_LIST):
            return None
        if "varchar_cast" not in self.policy.capabilities:
            self.flag("functions", f"cast of {alias} to varchar needs the 'cast(<natural key> as varchar)' allowlist entry")
            return None
        value = self.column(node["child"], scope)
        if column_names(value.bases) & self.policy.value_columns:
            return None
        self.mark(node)
        return Value(value.bases, frozenset({"normalized"}))

    def key_join(self, node: Json, scope: Scope) -> bool:
        """A fact's key join: ``md5(<natural key text>) = <dimension alias>.<dimension>_sk`` for a (natural key, dimension) the contract maps."""
        if self.kind != "fact" or node["class"] != "COMPARISON" or node["type"] != "COMPARE_EQUAL" or not scope.sources:
            return False
        hashed, other = node["left"], node["right"]
        if is_column(hashed):
            hashed, other = other, hashed
        if hashed["class"] != "FUNCTION" or hashed["function_name"] != "md5" or len(hashed["children"]) != 1 or not is_column(other):
            return False
        right = scope.sources[-1]
        if right.out is not None or "md5_key" not in self.policy.capabilities or len(other["column_names"]) < 2 or other["column_names"][-2] != right.alias:
            return False
        for dimension, natural in sorted(set(self.fact_map.values())):
            if right.relation != dimension or column_name(other) != dimension_key(dimension):
                continue
            parts = self.key_text(hashed["children"][0], scope, KeySpec((natural,)))
            if parts is not None:
                self.joined[right.alias] = (natural, dimension)
                self.mark(node)
                return True
        return False

    def unknown_coalesce(self, node: Json, scope: Scope, mapping: tuple[str, str]) -> Value | None:
        """A fact's key: ``coalesce(<dimension>.<dimension>_sk, md5('<unknown>'))`` over the join that hashes the contract's natural key into that dimension."""
        caps = self.policy.capabilities
        if node["class"] != "OPERATOR" or node["type"] != "OPERATOR_COALESCE" or not {"unknown_coalesce", "md5_key"} <= caps:
            return None
        dimension, natural = mapping
        children = node["children"]
        if len(children) != 2 or not is_column(children[0]) or not is_unknown_hash(children[1]):
            return None
        names = children[0]["column_names"]
        if column_name(children[0]) != dimension_key(dimension) or len(names) < 2 or self.joined.get(names[-2]) != (natural, dimension):
            return None
        return Value(self.column(children[0], scope).bases, frozenset({"unknown_coalesce"}))

    def identity(self, node: Json, scope: Scope, spec: KeySpec) -> Value | None:
        """dim_measure's identity text: ``concat_ws('|', fact, measure_key, value_column)`` (amendment A36 rule 3)."""
        parts = self.concat(node, scope, spec)
        return None if parts is None else Value(frozenset(base for part in parts for base in part), frozenset({"identity"}), parts)

    def calendar_date(self, node: Json, scope: Scope) -> Value | None:
        """``cast(unnest(generate_series(<first day>, <last day>, interval 1 day)) as date)`` between source dates (amendment A36 rule 1)."""
        if node["class"] != "CAST" or node["try_cast"] or node["cast_type"]["id"] != "DATE":
            return None
        unnest = node["child"]
        if unnest["class"] != "FUNCTION" or unnest["function_name"] != "unnest" or len(unnest["children"]) != 1:
            return None
        series = unnest["children"][0]
        if series["class"] != "FUNCTION" or series["function_name"] != "generate_series" or len(series["children"]) != 3:
            return None
        first, last, step = series["children"]
        if not (is_column(first) and is_column(last) and interval(step, "to_days", 1)):
            return None
        bases = self.column(first, scope).bases | self.column(last, scope).bases
        if not bases or not bases <= self.policy.date_pairs:
            return None
        return Value(bases, frozenset({"calendar_date"}))

    def date_of(self, node: Json | None, scope: Scope) -> Value | None:
        """The calendar date column a date part reads."""
        if node is None or column_name(node) != CALENDAR_COLUMN:
            return None
        return self.column(node, scope)

    def date_part(self, node: Json, scope: Scope, functions: frozenset[str]) -> Value | None:
        if node["class"] != "FUNCTION" or str(node["function_name"]).lower() not in functions or len(node["children"]) != 1:
            return None
        value = self.date_of(node["children"][0], scope)
        return None if value is None else Value(value.bases, frozenset({"date_part"}))

    def part_of(self, node: Json | None, name: str, scope: Scope) -> Value | None:
        """``month(d)`` or ``year(d)`` of the calendar date, also written ``date_part('month', d)``."""
        if node is None or node["class"] != "FUNCTION":
            return None
        function, kids = str(node["function_name"]).lower(), node["children"]
        if function == name and len(kids) == 1:
            return self.date_of(kids[0], scope)
        if function == "date_part" and len(kids) == 2 and is_text(kids[0], name):
            return self.date_of(kids[1], scope)
        return None

    def fiscal_year(self, node: Json, scope: Scope) -> Value | None:
        """The federal fiscal year (October 1 starts the next one): ``year(date_add(d, interval 3 month))`` or the one-``case`` form."""
        if node["class"] == "FUNCTION" and str(node["function_name"]).lower() == "year" and len(node["children"]) == 1:
            shifted = node["children"][0]
            if (
                shifted["class"] == "FUNCTION"
                and shifted["function_name"] == "date_add"
                and len(shifted["children"]) == 2
                and interval(shifted["children"][1], "to_months", 3)
            ):
                value = self.date_of(shifted["children"][0], scope)
                return None if value is None else Value(value.bases, frozenset({"fiscal_year"}))
            return None
        if node["class"] != "CASE":
            return None
        checks = node["case_checks"]
        if len(checks) != 1 or not node.get("else_expr"):
            return None
        when, then = checks[0]["when_expr"], checks[0]["then_expr"]
        month = self.part_of(when.get("left"), "month", scope) if when["class"] == "COMPARISON" and when["type"] == "COMPARE_GREATERTHANOREQUALTO" else None
        threshold = when.get("right") or {}
        plus = then["class"] == "FUNCTION" and then["function_name"] == "+" and len(then["children"]) == 2
        year_then = self.part_of(then["children"][0], "year", scope) if plus else None
        one = plus and then["children"][1]["class"] == "CONSTANT" and then["children"][1]["value"]["value"] == 1
        year_else = self.part_of(node["else_expr"], "year", scope)
        if month and year_then and year_else and one and threshold.get("class") == "CONSTANT" and threshold["value"]["value"] == 10:
            return Value(month.bases, frozenset({"fiscal_year"}))
        return None

    # Expressions.
    def expr(self, node: Json, scope: Scope, position: str) -> Value:
        self.seen.add(id(node))
        kind = node["class"]
        if kind == "COLUMN_REF":
            return self.column(node, scope)
        if kind == "CONSTANT":
            return Value(EMPTY, CONSTANT)
        if kind == "FUNCTION":
            return self.function(node, scope, position)
        if kind == "CASE":
            self.flag("case", "case outside the contracted key and fiscal-year roles")
            return self.children(node, scope)
        if kind == "CAST":
            return self.cast(node, scope, position)
        if kind == "WINDOW":
            self.flag("functions", f"window expression {node.get('function_name')} is not allowed (de-duplicate with distinct, group by or any_value)")
            return self.children(node, scope)
        if kind == "OPERATOR" and node["type"] == "OPERATOR_COALESCE":
            self.flag("functions", "coalesce is allowed only as a fact key: coalesce(<dimension>.<dimension>_sk, md5('<unknown>'))")
            return self.children(node, scope)
        if kind in ("OPERATOR", "COMPARISON", "CONJUNCTION", "BETWEEN") and (
            node["type"] in BOOLEAN_TYPES or str(node["type"]).startswith(("COMPARE_", "CONJUNCTION_"))
        ):
            if position == "output":
                self.flag("functions", f"boolean expression {node['type']} in an output column")
            return self.children(node, scope)
        if kind == "SUBQUERY":
            out = self.query(node["subquery"]["node"], scope.ctes)
            if node.get("child"):
                self.expr(node["child"], scope, "predicate")
            bases = frozenset(base for column in out.columns for base in column.bases)
            return Value(bases, OTHER)
        self.flag("functions", f"expression kind {kind} is not supported")
        return self.children(node, scope)

    def children(self, node: Json, scope: Scope) -> Value:
        """Visit every expression directly under a node, whatever field holds it, as a predicate-position operand."""
        bases: set[Base] = set()
        for child in child_nodes(node):
            bases |= self.expr(child, scope, "predicate").bases
        return Value(frozenset(bases), OTHER)

    def column(self, node: Json, scope: Scope) -> Value:
        self.seen.add(id(node))
        names = node["column_names"]
        name = names[-1]
        candidates = [source for source in scope.sources if len(names) < 2 or source.alias == names[-2]]
        bases: set[Base] = set()
        labels: set[str] = set()
        inputs: tuple[frozenset[Base], ...] = ()
        found = False
        for source in candidates:
            if source.out is None:
                continue
            column = source.out.get(name)
            if column is not None:
                bases |= column.bases
                labels |= column.labels
                inputs = merge_inputs(inputs, column.inputs)
                found = True
            elif source.out.star_relations:
                bases |= {(relation, name) for relation in source.out.star_relations}
                labels |= COLUMN
                found = True
        if not found:
            relations = {source.relation for source in candidates if source.out is None}
            bases = {(next(iter(relations)) if len(relations) == 1 else "?", name)}
            labels = set(COLUMN)
        return Value(frozenset(bases), frozenset(labels), inputs)

    def cast(self, node: Json, scope: Scope, position: str) -> Value:
        target, child = node["cast_type"]["id"], node["child"]
        value = self.expr(child, scope, position)
        if node["try_cast"]:
            allowed = self.model == RELEASE_MODEL and target == "DATE" and value.bases == {(RELEASE_RELATION, PUBLICATION_COLUMN)}
            if not allowed:
                self.flag("functions", f"try_cast to {target} is allowed in {RELEASE_MODEL} on {RELEASE_RELATION}.{PUBLICATION_COLUMN} only")
            return Value(value.bases, OTHER)
        if constant_cast(child):
            return Value(EMPTY, CONSTANT)
        self.flag("functions", f"cast to {target} of a column is allowed only as the varchar normalization of a dimension's natural-key or code column")
        return Value(value.bases, OTHER)

    def function(self, node: Json, scope: Scope, position: str) -> Value:
        name = str(node["function_name"]).lower()
        if name in ARITHMETIC:
            self.flag("arithmetic", f"arithmetic {name!r}")
            return self.children(node, scope)
        if name == "md5":
            self.flag("functions", "md5 is allowed only for a surrogate key at its contracted output, or in a fact's key join to its dimension")
            return self.children(node, scope)
        if name in self.policy.aggregates or name == "count_star":
            return self.aggregate(name, node, scope)
        self.flag("functions", f"function {name} is not approved here")
        return self.children(node, scope)

    def aggregate(self, name: str, node: Json, scope: Scope) -> Value:
        kids = node["children"]
        value = self.children(node, scope)
        if name == "any_value" and len(kids) == 1 and is_column(kids[0]):
            return value
        policy = self.policy
        if name == "count_star" and self.model == RELEASE_MODEL:
            return Value(EMPTY, OTHER)
        if name in ("min", "max") and len(kids) == 1 and value.bases:
            if self.model in SOURCE_DATE_MODELS and value.bases <= policy.date_pairs:
                return Value(value.bases, OTHER)
            if self.model == RELEASE_MODEL and name == "min" and value.bases == {(RELEASE_RELATION, OBJECT_COLUMN)}:
                return Value(value.bases, OTHER)
        shown = ", ".join(f"{relation}.{column}" for relation, column in sorted(value.bases))
        self.flag("functions", f"aggregate {name}({shown}) is outside the A36 scopes (de-duplicate with distinct, group by or any_value)")
        return Value(value.bases, OTHER)


def analyze_model(model: str, kind: str, document: Json, policy: Policy, fact_map: dict[str, tuple[str, str]]) -> Analysis:
    """Parse one compiled model: findings, physical reads and the contracted key columns at the output positions.

    ``fact_map`` maps each of a fact's key columns to the (dimension model, natural-key column) the contract gives it.
    """
    walker = Walker(model, kind, policy, fact_map)
    if kind == "dimension" and model not in DIMENSION_KEYS:
        walker.flag("functions", "no natural-key specification for this dimension (add it to DIMENSION_KEYS with its contract amendment)")
    top = walker.query(document["statements"][0]["node"], {})
    walker.sweep(document["statements"][0]["node"])
    wanted: dict[str, str] = {}
    if kind == "dimension":
        wanted[dimension_key(model)] = "md5_key"
    wanted |= dict.fromkeys(BRIDGE_KEYS.get(model, {}), "md5_key")
    wanted |= dict.fromkeys(fact_map, "unknown_coalesce")
    for key, label in wanted.items():
        column = top.get(key)
        if column is None:
            walker.flag("functions", f"no output column {key}")
        elif column.labels != {label}:
            walker.flag("functions", f"output column {key} is not the contracted expression (found {sorted(column.labels)}, wanted {label})")
        elif label == "md5_key":
            bind_key(walker, model, key, column, top)
    if model == "dim_date":
        stray = [column.name for column in top.columns if "fiscal_year" in column.labels and column.name != FISCAL_COLUMN]
        if stray:
            walker.flag("case", f"the fiscal-year expression is allowed in {FISCAL_COLUMN} only (found in {stray})")
    return Analysis(walker.findings, walker.reads, top)


def bind_key(walker: Walker, model: str, key: str, column: Col, top: Out) -> None:
    """The columns a surrogate key hashes must be the model's own natural-key output columns, by resolved lineage through every CTE and alias."""
    spec = DIMENSION_KEYS.get(model) if model.startswith("dim_") else BRIDGE_KEYS.get(model, {}).get(key)
    if spec is None:
        return
    if len(column.inputs) != len(spec.columns):
        walker.flag("functions", f"output column {key} hashes no natural key (found {len(column.inputs)} inputs, wanted {len(spec.columns)})")
        return
    for name, hashed in zip(spec.columns, column.inputs, strict=True):
        natural = top.get(name)
        if natural is None:
            if model.startswith("dim_"):
                walker.flag("functions", f"no natural-key output column {name}")
        elif natural.bases != hashed:
            walker.flag("functions", f"{key} hashes a column whose lineage differs from the natural-key output {name}")
