"""A small PromQL evaluator for the alert rule queries, so a case can show that a rule fires on one series set and stays silent on another.

It reads the subset that the tracked alert rules use and raises ``Unsupported`` for anything else:
- instant and range selectors with label matchers and ``offset``;
- the aggregations ``sum`` and ``count`` with ``by``, and the functions ``increase``, ``rate``, ``histogram_quantile`` and ``vector``;
- the arithmetic operators, the comparisons with and without ``bool`` and the set operators ``or``, ``and`` and ``unless``.

Two simplifications. An instant selector reads the newest sample of the last five minutes (no stale markers).
``increase`` and ``rate`` take the sample difference of the window with counter resets added, without Prometheus' extrapolation to the window edges.
Only the sign and the order of magnitude of the results matter to the cases. The metric name is dropped from every result, as Prometheus does after a
function or an operator.
"""

from __future__ import annotations

import math
import operator
import re
from collections.abc import Callable
from typing import Any

Labels = frozenset[tuple[str, str]]
Vector = dict[Labels, float]
Samples = list[tuple[float, float]]
Node = tuple[Any, ...]
LOOKBACK = 300.0
UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
TOKEN = re.compile(
    r'\s*(?:(?P<number>[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)(?P<unit>ms|s|m|h|d)?|(?P<ident>[a-zA-Z_:][a-zA-Z0-9_:]*)|(?P<string>"(?:[^"\\]|\\.)*")|(?P<op>=~|!~|>=|<=|==|!=|[-+*/><=(){}\[\],]))'
)
COMPARISONS: dict[str, Callable[[float, float], bool]] = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}
ARITHMETIC = ("+", "-", "*", "/")
SET_WORDS = ("or", "and", "unless")


class Unsupported(ValueError):
    """The text uses a PromQL form that this evaluator does not read."""


class Series:
    """One time series: a metric name, its labels and its samples as (seconds, value) pairs in time order."""

    def __init__(self, name: str, labels: dict[str, str], samples: Samples) -> None:
        self.name, self.labels, self.samples = name, labels, sorted(samples)


def tokens(text: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    position = 0
    while text[position:].strip():
        match = TOKEN.match(text, position)
        if match is None:
            raise Unsupported(f"cannot read {text[position : position + 20]!r}")
        position = match.end()
        if match.group("number") is not None:
            unit = match.group("unit")
            found.append(("duration", str(float(match.group("number")) * UNITS[unit])) if unit else ("number", match.group("number")))
        elif match.group("ident") is not None:
            found.append(("ident", match.group("ident")))
        elif match.group("string") is not None:
            found.append(("string", match.group("string")[1:-1]))
        else:
            found.append(("op", match.group("op")))
    return found


class Parser:
    def __init__(self, text: str) -> None:
        self.items, self.at = tokens(text), 0

    def peek(self) -> tuple[str, str]:
        return self.items[self.at] if self.at < len(self.items) else ("end", "")

    def take(self, kind: str, value: str | None = None) -> str:
        found = self.peek()
        if found[0] != kind or (value is not None and found[1] != value):
            raise Unsupported(f"expected {value or kind}, found {found[1] or 'the end'}")
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

    def expression(self) -> Node:
        left = self.and_level()
        while (word := self.word("or")) is not None:
            left = ("set", word, left, self.and_level())
        return left

    def and_level(self) -> Node:
        left = self.compare_level()
        while (word := self.word("and", "unless")) is not None:
            left = ("set", word, left, self.compare_level())
        return left

    def compare_level(self) -> Node:
        left = self.add_level()
        while (op := self.operator_of(*COMPARISONS)) is not None:
            as_bool = self.word("bool") is not None
            left = ("compare", op, as_bool, left, self.add_level())
        return left

    def add_level(self) -> Node:
        left = self.multiply_level()
        while (op := self.operator_of("+", "-")) is not None:
            left = ("arith", op, left, self.multiply_level())
        return left

    def multiply_level(self) -> Node:
        left = self.atom()
        while (op := self.operator_of("*", "/")) is not None:
            left = ("arith", op, left, self.atom())
        return left

    def labels(self) -> list[str]:
        self.take("op", "(")
        names: list[str] = []
        while self.operator_of(")") is None:
            names.append(self.take("ident"))
            self.operator_of(",")
        return names

    def duration(self) -> float:
        return float(self.take("duration"))

    def atom(self) -> Node:
        if self.operator_of("("):
            inner = self.expression()
            self.take("op", ")")
            return inner
        kind, value = self.peek()
        if kind == "number":
            self.at += 1
            return ("number", float(value))
        if kind != "ident":
            raise Unsupported(f"unexpected {value or 'end of text'}")
        self.at += 1
        if self.peek() == ("op", "(") or self.peek() == ("ident", "by"):
            return self.call(value)
        return self.selector(value)

    def call(self, name: str) -> Node:
        grouping: list[str] | None = None
        if self.word("by"):
            grouping = self.labels()
        self.take("op", "(")
        args = [self.expression()]
        while self.operator_of(","):
            args.append(self.expression())
        self.take("op", ")")
        if self.word("by"):
            grouping = self.labels()
        if name in ("sum", "count"):
            return ("aggregate", name, grouping, args[0])
        if grouping is not None or name not in ("increase", "rate", "histogram_quantile", "vector"):
            raise Unsupported(f"the function {name} is not supported")
        return ("call", name, args)

    def selector(self, name: str) -> Node:
        matchers: list[tuple[str, str, str]] = []
        if self.operator_of("{"):
            while self.operator_of("}") is None:
                label = self.take("ident")
                op = self.operator_of("=", "!=", "=~", "!~")
                if op is None:
                    raise Unsupported("a label matcher needs =, !=, =~ or !~")
                matchers.append((label, op, self.take("string")))
                self.operator_of(",")
        window = None
        if self.operator_of("["):
            window = self.duration()
            self.take("op", "]")
        shift = self.duration() if self.word("offset") else 0.0
        return ("selector", name, matchers, window, shift)


def matches(series: Series, name: str, matchers: list[tuple[str, str, str]]) -> bool:
    if series.name != name:
        return False
    for label, op, value in matchers:
        actual = series.labels.get(label, "")
        ok = {"=": actual == value, "!=": actual != value, "=~": re.fullmatch(value, actual) is not None, "!~": re.fullmatch(value, actual) is None}[op]
        if not ok:
            return False
    return True


def divide(left: float, right: float) -> float:
    if right == 0:
        return math.nan if left == 0 or math.isnan(left) else math.copysign(math.inf, left)
    return left / right


def arithmetic(op: str, left: float, right: float) -> float:
    return {"+": left + right, "-": left - right, "*": left * right}[op] if op != "/" else divide(left, right)


def increase(samples: Samples) -> float | None:
    if len(samples) < 2:
        return None
    total, previous = 0.0, samples[0][1]
    for _, value in samples[1:]:
        total += value - previous if value >= previous else value
        previous = value
    return total


def quantile(q: float, buckets: list[tuple[float, float]]) -> float:
    buckets = sorted(buckets)
    if len(buckets) < 2 or not math.isinf(buckets[-1][0]):
        return math.nan
    total = buckets[-1][1]
    if total == 0:
        return math.nan
    rank = q * total
    for index, (bound, count) in enumerate(buckets):
        if count >= rank:
            if math.isinf(bound):
                return buckets[-2][0]
            start, before = (0.0, 0.0) if index == 0 else buckets[index - 1]
            return bound if count == before else start + (bound - start) * (rank - before) / (count - before)
    return math.nan


class Engine:
    def __init__(self, store: list[Series]) -> None:
        self.store = store

    def run(self, text: str, at: float) -> Vector:
        parser = Parser(text)
        node = parser.expression()
        if parser.peek()[0] != "end":
            raise Unsupported(f"unexpected {parser.peek()[1]}")
        kind, value = self.evaluate(node, at)
        return {frozenset(): value} if kind == "scalar" else value

    def evaluate(self, node: Node, at: float) -> tuple[str, Any]:
        kind = node[0]
        if kind == "number":
            return "scalar", node[1]
        if kind == "selector":
            return self.select(node, at)
        if kind == "aggregate":
            return "vector", self.aggregate(node, at)
        if kind == "call":
            return self.function(node, at)
        if kind == "set":
            return "vector", self.sets(node, at)
        return self.binary(node, at)

    def select(self, node: Node, at: float) -> tuple[str, Any]:
        _, name, matchers, window, shift = node
        end = at - shift
        if window is not None:
            ranged: dict[Labels, Samples] = {}
            for series in self.store:
                if matches(series, name, matchers):
                    points = [(t, v) for t, v in series.samples if end - window < t <= end]
                    if points:
                        ranged[frozenset(series.labels.items())] = points
            return "range", ranged
        vector: Vector = {}
        for series in self.store:
            if matches(series, name, matchers):
                points = [(t, v) for t, v in series.samples if end - LOOKBACK < t <= end]
                if points:
                    vector[frozenset(series.labels.items())] = points[-1][1]
        return "vector", vector

    def vector_of(self, node: Node, at: float) -> Vector:
        kind, value = self.evaluate(node, at)
        if kind == "scalar":
            return {frozenset(): value}
        if kind != "vector":
            raise Unsupported("a range vector is only valid inside increase or rate")
        return dict(value)

    def aggregate(self, node: Node, at: float) -> Vector:
        _, name, grouping, argument = node
        groups: dict[Labels, list[float]] = {}
        for labels, value in self.vector_of(argument, at).items():
            key = frozenset(item for item in labels if grouping is not None and item[0] in grouping)
            groups.setdefault(key, []).append(value)
        return {key: sum(values) if name == "sum" else float(len(values)) for key, values in groups.items()}

    def function(self, node: Node, at: float) -> tuple[str, Any]:
        _, name, args = node
        if name == "vector":
            return "vector", {frozenset(): self.evaluate(args[0], at)[1]}
        if name in ("increase", "rate"):
            kind, ranged = self.evaluate(args[0], at)
            if kind != "range":
                raise Unsupported(f"{name} needs a range selector")
            window = args[0][3]
            result: Vector = {}
            for labels, samples in ranged.items():
                grown = increase(samples)
                if grown is not None:
                    result[labels] = grown if name == "increase" else grown / window
            return "vector", result
        scalar = self.evaluate(args[0], at)[1]
        grouped: dict[Labels, list[tuple[float, float]]] = {}
        for labels, value in self.vector_of(args[1], at).items():
            bound = next(float(v) for k, v in labels if k == "le")
            grouped.setdefault(frozenset(item for item in labels if item[0] != "le"), []).append((bound, value))
        return "vector", {key: quantile(scalar, buckets) for key, buckets in grouped.items()}

    def sets(self, node: Node, at: float) -> Vector:
        _, word, left_node, right_node = node
        left, right = self.vector_of(left_node, at), self.vector_of(right_node, at)
        if word == "or":
            return {**{key: value for key, value in right.items() if key not in left}, **left}
        if word == "and":
            return {key: value for key, value in left.items() if key in right}
        return {key: value for key, value in left.items() if key not in right}

    def binary(self, node: Node, at: float) -> tuple[str, Any]:
        if node[0] == "compare":
            _, op, as_bool, left_node, right_node = node
        else:
            _, op, left_node, right_node = node
            as_bool = False
        left_kind, left = self.evaluate(left_node, at)
        right_kind, right = self.evaluate(right_node, at)
        compare = node[0] == "compare"

        def apply(a: float, b: float) -> float | None:
            if not compare:
                return arithmetic(op, a, b)
            held = COMPARISONS[op](a, b)
            return float(held) if as_bool else (a if held else None)

        if left_kind == "scalar" and right_kind == "scalar":
            if compare and not as_bool:
                raise Unsupported("a comparison of two numbers needs bool")
            return "scalar", apply(left, right)
        if "range" in (left_kind, right_kind):
            raise Unsupported("a range vector is only valid inside increase or rate")
        result: Vector = {}
        if left_kind == "vector" and right_kind == "vector":
            for key, value in left.items():
                if key in right and (out := apply(value, right[key])) is not None:
                    result[key] = out
        elif left_kind == "vector":
            for key, value in left.items():
                if (out := apply(value, right)) is not None:
                    result[key] = out
        else:
            for key, value in right.items():
                if (out := apply(left, value)) is not None:
                    result[key] = out
        return "vector", result


def fires(text: str, store: list[Series], at: float) -> bool:
    """True when a Grafana rule on this query fires: the reduction keeps the last value, drops NaN and the threshold step asks for more than zero."""
    return any(value > 0 for value in Engine(store).run(text, at).values() if not math.isnan(value))
