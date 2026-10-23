"""Evaluate IAM identity policies the way IAM does, offline: an explicit Deny wins, else an Allow, else an implicit deny.

The evaluator renders the policy templates in ``infra/iam/policies/`` and answers whether a request is allowed. A request lists only
the condition keys that AWS sends for it. Key names match without regard to case, as in IAM, so ``aws:RequestTag/Project`` is satisfied
by a tag written ``project``. A negated operator matches a missing key. An operator, a statement element or a policy variable that the
evaluator does not model stops the run with ``UnsupportedPolicyElement`` instead of passing.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

from infra.iam.scripts.render_policy import TEMPLATE_PREFIX, load_documents

POLICY_VARIABLE = re.compile(r"\$\{([a-zA-Z0-9:/_.-]+)\}")


class UnsupportedPolicyElement(ValueError):
    """The policy uses an element that the evaluator does not model."""


@dataclass(frozen=True)
class Request:
    """One API request: the action, the resource ARN and the condition keys that AWS sends for it."""

    action: str
    resource: str
    context: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class Decision:
    """The result of an evaluation and the statement identifiers behind it."""

    effect: str
    statements: tuple[str, ...]

    @property
    def allowed(self) -> bool:
        return self.effect == "allow"


def build_request(
    action: str, resource: str, *, region: str | None = None, tags: Mapping[str, str] | None = None, extra: Mapping[str, str | tuple[str, ...]] | None = None
) -> Request:
    """Build a request. ``tags`` are the exact tag keys and values in the call; they become ``aws:RequestTag/<key>`` and ``aws:TagKeys``."""
    values: dict[str, tuple[str, ...]] = {}
    if region is not None:
        values["aws:RequestedRegion"] = (region,)
    if tags is not None:
        for key, value in tags.items():
            values[f"aws:RequestTag/{key}"] = (value,)
        values["aws:TagKeys"] = tuple(tags)
    for key, item in (extra or {}).items():
        values[key] = (item,) if isinstance(item, str) else tuple(item)
    return Request(action, resource, values)


def load_policies(directory: Path, values: Mapping[str, str], names: Mapping[str, Sequence[str]] | None = None) -> dict[str, dict[str, Any]]:
    """Render every policy template in the directory with the production renderer, keyed by template file stem.

    The secret and parameter names come from the Terraform variable defaults unless ``names`` replaces them.
    """
    return {f"{TEMPLATE_PREFIX}{service}_policy": document for service, document in load_documents(values, directory=directory, names=names).items()}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def substitute(pattern: str, context: Mapping[str, tuple[str, ...]]) -> str:
    """Replace policy variables such as ``${aws:username}`` with the first context value; an unknown variable stops the run."""

    def lookup(match: re.Match[str]) -> str:
        values = context.get(match.group(1).lower())
        if not values:
            raise UnsupportedPolicyElement(f"policy variable ${{{match.group(1)}}} has no value in the request context")
        return values[0]

    return POLICY_VARIABLE.sub(lookup, pattern)


@cache
def compiled_pattern(pattern: str, ignore_case: bool) -> re.Pattern[str]:
    """The regular expression of an IAM wildcard pattern: ``*`` is any run of characters (also ``/`` and ``:``) and ``?`` is one character."""
    escaped = "".join(".*" if character == "*" else "." if character == "?" else re.escape(character) for character in pattern)
    return re.compile(escaped, re.IGNORECASE | re.DOTALL if ignore_case else re.DOTALL)


def glob_match(pattern: str, value: str, *, ignore_case: bool = False) -> bool:
    """IAM wildcard match; nothing but ``*`` and ``?`` is special."""
    return compiled_pattern(pattern, ignore_case).fullmatch(value) is not None


BASE_OPERATORS = {
    "StringEquals": (lambda pattern, value: pattern == value, False),
    "StringNotEquals": (lambda pattern, value: pattern == value, True),
    "StringEqualsIgnoreCase": (lambda pattern, value: pattern.lower() == value.lower(), False),
    "StringNotEqualsIgnoreCase": (lambda pattern, value: pattern.lower() == value.lower(), True),
    "StringLike": (lambda pattern, value: glob_match(pattern, value), False),
    "StringNotLike": (lambda pattern, value: glob_match(pattern, value), True),
    "ArnEquals": (lambda pattern, value: glob_match(pattern, value), False),
    "ArnLike": (lambda pattern, value: glob_match(pattern, value), False),
    "ArnNotEquals": (lambda pattern, value: glob_match(pattern, value), True),
    "ArnNotLike": (lambda pattern, value: glob_match(pattern, value), True),
}


def condition_holds(operator: str, key: str, expected: Any, context: Mapping[str, tuple[str, ...]]) -> bool:
    """Evaluate one condition block entry against the request context."""
    set_operator = None
    base = operator
    for prefix in ("ForAnyValue:", "ForAllValues:"):
        if base.startswith(prefix):
            set_operator, base = prefix.removesuffix(":"), base.removeprefix(prefix)
    if_exists = base.endswith("IfExists")
    base = base.removesuffix("IfExists")
    present = context.get(key.lower())
    if base == "Null":
        return (present is None) == (str(as_list(expected)[0]).lower() == "true")
    if base == "Bool":
        if present is None:
            return if_exists
        return any(value.lower() == str(as_list(expected)[0]).lower() for value in present)
    if base not in BASE_OPERATORS:
        raise UnsupportedPolicyElement(f"condition operator {operator} is not modeled")
    matches, negated = BASE_OPERATORS[base]
    patterns = [substitute(str(item), context) for item in as_list(expected)]
    if present is None:
        if set_operator == "ForAllValues":
            return True
        if set_operator == "ForAnyValue":
            return False
        return negated or if_exists

    def single(value: str) -> bool:
        found = any(matches(pattern, value) for pattern in patterns)
        return not found if negated else found

    if set_operator == "ForAnyValue":
        return any(single(value) for value in present)
    if set_operator == "ForAllValues":
        return all(single(value) for value in present)
    return all(single(value) for value in present) if negated else any(single(value) for value in present)


def statement_matches(statement: Mapping[str, Any], request: Request) -> bool:
    """True when the statement's action, resource and every condition match the request."""
    unsupported = {"NotAction", "NotResource", "Principal", "NotPrincipal"} & statement.keys()
    if unsupported:
        raise UnsupportedPolicyElement(f"statement {statement.get('Sid')} uses {', '.join(sorted(unsupported))}")
    context = {key.lower(): value for key, value in request.context.items()}
    if not any(glob_match(action, request.action, ignore_case=True) for action in as_list(statement.get("Action", []))):
        return False
    if not any(glob_match(substitute(resource, context), request.resource) for resource in as_list(statement.get("Resource", []))):
        return False
    return all(condition_holds(operator, key, expected, context) for operator, keys in statement.get("Condition", {}).items() for key, expected in keys.items())


def decide(documents: Iterable[Mapping[str, Any]], request: Request) -> Decision:
    """Evaluate the request against all policy documents attached to one principal."""
    allows: list[str] = []
    for document in documents:
        for statement in document["Statement"]:
            if not statement_matches(statement, request):
                continue
            if statement["Effect"] == "Deny":
                return Decision("deny", (str(statement.get("Sid")),))
            allows.append(str(statement.get("Sid")))
    return Decision("allow", tuple(allows)) if allows else Decision("implicit_deny", ())


def allowed_actions(documents: Iterable[Mapping[str, Any]]) -> set[str]:
    """Every action named by an Allow statement."""
    return {
        action for document in documents for statement in document["Statement"] if statement["Effect"] == "Allow" for action in as_list(statement["Action"])
    }
