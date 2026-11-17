"""A syntax and shape check for the PromQL queries of the tracked dashboards and alert rules. It evaluates nothing.

``validate(text)`` returns the kind of the result (``scalar``, ``vector`` or ``range``) or raises ``Unsupported``.
It reads the forms the tracked queries use and refuses the rest:
- instant and range selectors with label matchers and ``offset``;
- the aggregations and functions of ``AGGREGATIONS`` and ``FUNCTIONS``, with their argument counts and argument kinds;
- arithmetic and comparison operators with ``bool`` and the set operators;
- vector matching with ``on``, ``ignoring``, ``group_left`` and ``group_right``, and parentheses.

It does not know what a function returns for any series, so it makes no claim about alert behavior. Native ``promtool test rules`` over the fixtures in
``monitoring/promtool/`` is the authority for that. Grafana's interval variables are read as five minutes.
"""

from __future__ import annotations

import re

Kind = str
SCALAR, VECTOR, RANGE = "scalar", "vector", "range"
VARIABLE = re.compile(r"\$\{?[A-Za-z_]\w*\}?")
UNITS = ("ms", "s", "m", "h", "d")
TOKEN = re.compile(
    r'\s*(?:(?P<number>[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)(?P<unit>ms|s|m|h|d)?|(?P<ident>[a-zA-Z_:][a-zA-Z0-9_:]*)|(?P<string>"(?:[^"\\]|\\.)*")|(?P<op>=~|!~|>=|<=|==|!=|[-+*/%^><=(){}\[\],:]))'
)
COMPARISONS = (">", "<", ">=", "<=", "==", "!=")
# Aggregations: the argument kinds before the vector (a number for topk) and how many arguments the call takes in all.
AGGREGATIONS: dict[str, tuple[Kind, ...]] = {
    "sum": (VECTOR,),
    "min": (VECTOR,),
    "max": (VECTOR,),
    "avg": (VECTOR,),
    "count": (VECTOR,),
    "group": (VECTOR,),
    "topk": (SCALAR, VECTOR),
    "bottomk": (SCALAR, VECTOR),
}
# Functions: the argument kinds in order and the kind of the result.
FUNCTIONS: dict[str, tuple[tuple[Kind, ...], Kind]] = {
    "rate": ((RANGE,), VECTOR),
    "irate": ((RANGE,), VECTOR),
    "increase": ((RANGE,), VECTOR),
    "delta": ((RANGE,), VECTOR),
    "changes": ((RANGE,), VECTOR),
    "resets": ((RANGE,), VECTOR),
    "histogram_quantile": ((SCALAR, VECTOR), VECTOR),
    "vector": ((SCALAR,), VECTOR),
    "scalar": ((VECTOR,), SCALAR),
    "time": ((), SCALAR),
    "absent": ((VECTOR,), VECTOR),
    "clamp_min": ((VECTOR, SCALAR), VECTOR),
    "clamp_max": ((VECTOR, SCALAR), VECTOR),
}
KIND_WORDS = {SCALAR: "a number", VECTOR: "an instant vector", RANGE: "a range vector"}


class Unsupported(ValueError):
    """The text is not a PromQL form that this check reads, or a call in it has the wrong arguments."""


def tokens(text: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    position = 0
    while text[position:].strip():
        match = TOKEN.match(text, position)
        if match is None:
            raise Unsupported(f"cannot read {text[position : position + 20]!r}")
        position = match.end()
        if match.group("number") is not None:
            found.append(("duration" if match.group("unit") else "number", match.group("number") + (match.group("unit") or "")))
        elif match.group("ident") is not None:
            found.append(("ident", match.group("ident")))
        elif match.group("string") is not None:
            found.append(("string", match.group("string")[1:-1]))
        else:
            found.append(("op", match.group("op")))
    return found


def plural(count: int) -> str:
    return f"{count} argument" + ("" if count == 1 else "s")


class Parser:
    def __init__(self, text: str) -> None:
        self.items, self.at = tokens(VARIABLE.sub("5m", text)), 0

    def peek(self, ahead: int = 0) -> tuple[str, str]:
        return self.items[self.at + ahead] if self.at + ahead < len(self.items) else ("end", "")

    def take(self, kind: str, value: str | None = None) -> str:
        found = self.peek()
        if found[0] != kind or (value is not None and found[1] != value):
            raise Unsupported(f"expected {value or kind}, found {found[1] or 'the end of the text'}")
        self.at += 1
        return found[1]

    def word(self, *words: str) -> str | None:
        kind, value = self.peek()
        if kind == "ident" and value in words:
            self.at += 1
            return value
        return None

    def operator_of(self, *ops: str) -> str | None:
        kind, value = self.peek()
        if kind == "op" and value in ops:
            self.at += 1
            return value
        return None

    def query(self) -> Kind:
        kind = self.expression()
        if self.peek()[0] != "end":
            raise Unsupported(f"expected the end of the text, found {self.peek()[1]}")
        if kind == RANGE:
            raise Unsupported("the query ends in a range vector, which only a function can read")
        return kind

    def expression(self) -> Kind:
        left = self.and_level()
        while self.word("or") is not None:
            self.matching()
            left = self.set_operation(left, self.and_level())
        return left

    def and_level(self) -> Kind:
        left = self.compare_level()
        while self.word("and", "unless") is not None:
            self.matching()
            left = self.set_operation(left, self.compare_level())
        return left

    def compare_level(self) -> Kind:
        left = self.add_level()
        while self.operator_of(*COMPARISONS) is not None:
            as_bool = self.word("bool") is not None
            self.matching()
            right = self.add_level()
            if left == SCALAR and right == SCALAR and not as_bool:
                raise Unsupported("a comparison of two numbers needs bool")
            left = self.binary_kind(left, right)
        return left

    def add_level(self) -> Kind:
        left = self.multiply_level()
        while self.operator_of("+", "-") is not None:
            self.matching()
            left = self.binary_kind(left, self.multiply_level())
        return left

    def multiply_level(self) -> Kind:
        left = self.power_level()
        while self.operator_of("*", "/", "%") is not None:
            self.matching()
            left = self.binary_kind(left, self.power_level())
        return left

    def power_level(self) -> Kind:
        left = self.unary()
        if self.operator_of("^") is not None:
            self.matching()
            return self.binary_kind(left, self.power_level())
        return left

    def unary(self) -> Kind:
        if self.operator_of("-", "+") is not None:
            kind = self.unary()
            if kind == RANGE:
                raise Unsupported("a range vector cannot take a sign")
            return kind
        return self.atom()

    @staticmethod
    def binary_kind(left: Kind, right: Kind) -> Kind:
        if RANGE in (left, right):
            raise Unsupported("a range vector cannot be an operand of an operator")
        return SCALAR if left == right == SCALAR else VECTOR

    @staticmethod
    def set_operation(left: Kind, right: Kind) -> Kind:
        if left != VECTOR or right != VECTOR:
            raise Unsupported("set operators need vectors on both sides")
        return VECTOR

    def label_list(self) -> None:
        self.take("op", "(")
        while self.operator_of(")") is None:
            self.take("ident")
            self.operator_of(",")

    def matching(self) -> None:
        if self.word("on", "ignoring") is not None:
            self.label_list()
            if self.word("group_left", "group_right") is not None and self.peek() == ("op", "("):
                self.label_list()

    def atom(self) -> Kind:
        if self.operator_of("("):
            kind = self.expression()
            self.take("op", ")")
            return kind
        kind, value = self.peek()
        if kind == "number":
            self.at += 1
            return SCALAR
        if kind != "ident":
            raise Unsupported(f"expected a query, found {value or 'the end of the text'}")
        self.at += 1
        if value in AGGREGATIONS or value in FUNCTIONS:
            if self.peek() == ("op", "(") or (value in AGGREGATIONS and self.peek()[0] == "ident" and self.peek()[1] in ("by", "without")):
                return self.call(value)
        elif self.peek() == ("op", "("):
            raise Unsupported(f"the function {value} is not supported")
        return self.selector()

    def arguments(self) -> list[Kind]:
        self.take("op", "(")
        found: list[Kind] = []
        while self.operator_of(")") is None:
            found.append(self.expression())
            self.operator_of(",")
        return found

    def call(self, name: str) -> Kind:
        grouped = self.word("by", "without") is not None
        if grouped:
            self.label_list()
        found = self.arguments()
        if name in AGGREGATIONS:
            if self.word("by", "without") is not None:
                if grouped:
                    raise Unsupported(f"{name} has a grouping before and after its arguments")
                self.label_list()
            wanted = AGGREGATIONS[name]
            self.fit(name, wanted, found)
            return VECTOR
        if grouped:
            raise Unsupported(f"the function {name} takes no grouping")
        wanted, result = FUNCTIONS[name]
        self.fit(name, wanted, found)
        return result

    @staticmethod
    def fit(name: str, wanted: tuple[Kind, ...], found: list[Kind]) -> None:
        if len(found) != len(wanted):
            raise Unsupported(f"{name} takes {plural(len(wanted))}, not {len(found)}")
        for position, (need, got) in enumerate(zip(wanted, found, strict=True), start=1):
            if need != got:
                raise Unsupported(f"argument {position} of {name} needs {KIND_WORDS[need]}, not {KIND_WORDS[got]}")

    def selector(self) -> Kind:
        self.at -= 1
        self.take("ident")
        if self.operator_of("{"):
            while self.operator_of("}") is None:
                self.take("ident")
                if self.operator_of("=", "!=", "=~", "!~") is None:
                    raise Unsupported("a label matcher needs =, !=, =~ or !~")
                self.take("string")
                self.operator_of(",")
        kind = VECTOR
        if self.operator_of("["):
            self.take("duration")
            self.take("op", "]")
            kind = RANGE
        if self.word("offset") is not None:
            self.take("duration")
        return kind


def validate(text: str) -> Kind:
    """The kind of the result of one query: ``scalar``, ``vector`` or ``range``. A form that this check does not read raises ``Unsupported``."""
    return Parser(text).query()
