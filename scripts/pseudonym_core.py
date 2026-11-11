"""Keyed pseudonymization core shared by the acquisition derivative tool and the bronze loader.

The policy rules, the documented normalization, the keyed HMAC-SHA-256, the Parameter Store key reader and the row conversion live here, with no file
listing and no storage code, so a loader can import them without taking on the derivative tool. The key is read at use as text, held in memory and never
written, logged or printed. Errors carry one fixed code and never a value, a name or a key.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import re
import unicodedata
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from scripts.sdk_logs import quiet_sdk_logs

CONTRACT_VERSION = 1

NORMALIZATION_VERSION = "norm_v1"

CLASSIFICATION = "public_pseudonymized_derivative"

CONTRACT_NAME = "pseudonymization_contract.json"

DATA_NAME = "data.csv"

REPLACEMENT = "[REDACTED]"

PARAMETER_PREFIX = "/healthcare_hai_prediction/pseudonym_key_"

KEY_BYTES = 32

KEY_CHECK_LABEL = b"hai-pseudonym/key-check/" + NORMALIZATION_VERSION.encode()

DOMAIN = b"hai-pseudonym"

SEPARATOR = b"\x1f"

MAX_CELL_CHARACTERS = 4096

MAX_BYTES_LIMIT = 1024**3

KEY_VERSION = re.compile(r"v[1-9][0-9]{0,2}")

NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}")

PSEUDONYM_FORMAT = re.compile(r"v[1-9][0-9]{0,2}:[0-9a-f]{64}")

HEX64 = re.compile(r"[0-9a-f]{64}")

HEX16 = re.compile(r"[0-9a-f]{16}")

SOURCE_ID = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}")

MIN_IDENTIFIER_CHARACTERS = 4

MIN_NAME_CHARACTERS = 2

NO_IDENTITY = frozenset({"", "-", "n/a", "na", "none", "null", "unknown", "not available", "not applicable", "[redacted]"})

NAME_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv"})

APOSTROPHES = dict.fromkeys(map(ord, "'‘’ʼ`´."), None)

APOSTROPHE_FORMS = dict.fromkeys(map(ord, "‘’ʼ`´＇"), "'")

TEXT_TOKEN = re.compile(r"[^\W_]+(?:['‘’ʼ`´.]+[^\W_]+)*")

POSSESSIVE_END = re.compile(r"['‘’ʼ`´]s$")

MARKER = re.compile(re.escape(REPLACEMENT), re.IGNORECASE)

TREATMENTS = ("keep", "drop", "scrub_names")

KINDS = {"id": "authoritative_id", "name": "name_match_only"}

POLICY_FIELDS = {"policy_id", "policy_version", "source_id", "normalization_version", "headers", "treatments", "pseudonyms", "max_bytes", "min_rows"}

CLIENT_OPTIONS: dict[str, Any] = {
    "retries": {"total_max_attempts": 2, "mode": "standard"},
    "connect_timeout": 5,
    "read_timeout": 10,
    "ignore_configured_endpoint_urls": True,
}

KeyReader = Callable[[str], str | None]


class PseudonymizationError(ValueError):
    """Contains only a fixed code, never a source value, a name or a key."""


def require(condition: bool, code: str) -> None:
    """Raise the module error with the fixed code when the condition is false."""
    if not condition:
        raise PseudonymizationError(code)


def is_text_list(value: Any) -> bool:
    """True for a non-empty list of distinct non-empty strings."""
    return isinstance(value, list) and bool(value) and all(isinstance(v, str) and bool(v) for v in value) and len(set(value)) == len(value)


def validate_condition(condition: Any, headers: list[str]) -> None:
    """A condition names one header, the cell value that selects a row and every other approved value; any other value stops the run."""
    require(isinstance(condition, dict) and set(condition) == {"column", "equals", "others"}, "policy_invalid")
    require(isinstance(condition["column"], str) and condition["column"] in headers and isinstance(condition["equals"], str), "policy_invalid")
    require(isinstance(condition["others"], list) and all(isinstance(v, str) for v in condition["others"]), "policy_invalid")
    approved = [base_text(v) for v in [condition["equals"], *condition["others"]]]
    require(len(set(approved)) == len(approved), "policy_invalid")


def source_removed(treatment: dict, rule: dict) -> bool:
    """True when a pseudonym source column leaves the derivative: dropped, or blanked on exactly the rows the pseudonym covers."""
    return treatment["treatment"] == "drop" or (rule.get("when") is not None and treatment.get("blank_when") == rule["when"])


def check_policy(policy: Any) -> None:
    """Reject a policy that is not closed, complete and fail-closed: exact types first, then every source column needs a reviewed treatment."""
    require(isinstance(policy, dict) and set(policy) == POLICY_FIELDS, "policy_invalid")
    require(type(policy["policy_version"]) is int and policy["policy_version"] == 1, "policy_invalid")
    require(isinstance(policy["policy_id"], str) and NAME_PATTERN.fullmatch(policy["policy_id"]) is not None, "policy_invalid")
    require(isinstance(policy["source_id"], str) and NAME_PATTERN.fullmatch(policy["source_id"].lower()) is not None, "policy_invalid")
    require(isinstance(policy["normalization_version"], str), "policy_invalid")
    require(policy["normalization_version"] == NORMALIZATION_VERSION, "unsupported_normalization")
    headers = policy["headers"]
    require(is_text_list(headers), "policy_invalid")
    require(type(policy["max_bytes"]) is int and 0 < policy["max_bytes"] <= MAX_BYTES_LIMIT, "policy_invalid")
    require(type(policy["min_rows"]) is int and policy["min_rows"] >= 0, "policy_invalid")
    treatments = policy["treatments"]
    require(isinstance(treatments, dict) and set(treatments) == set(headers), "policy_invalid")
    for rule in treatments.values():
        require(isinstance(rule, dict) and {"treatment"} <= set(rule) <= {"treatment", "blank_when"}, "policy_invalid")
        require(isinstance(rule["treatment"], str) and rule["treatment"] in TREATMENTS, "policy_invalid")
        if "blank_when" in rule:
            validate_condition(rule["blank_when"], headers)
            require(rule["treatment"] != "drop", "policy_invalid")
    pseudonyms = policy["pseudonyms"]
    require(isinstance(pseudonyms, list) and bool(pseudonyms), "policy_invalid")
    outputs = set()
    for rule in pseudonyms:
        require(isinstance(rule, dict), "policy_invalid")
        require(
            {"output", "kind", "namespace", "columns"} <= set(rule) <= {"output", "kind", "namespace", "columns", "when", "middle_column"}, "policy_invalid"
        )
        require(isinstance(rule["output"], str) and NAME_PATTERN.fullmatch(rule["output"]) is not None, "policy_invalid")
        require(rule["output"] not in headers and rule["output"] not in outputs, "policy_invalid")
        outputs.add(rule["output"])
        require(isinstance(rule["kind"], str) and rule["kind"] in KINDS, "policy_invalid")
        require(isinstance(rule["namespace"], str) and NAME_PATTERN.fullmatch(rule["namespace"]) is not None, "policy_invalid")
        require(is_text_list(rule["columns"]) and set(rule["columns"]) <= set(headers), "policy_invalid")
        require(rule["kind"] == "name" or len(rule["columns"]) == 1, "policy_invalid")
        require(all(source_removed(treatments[c], rule) for c in rule["columns"]), "pseudonym_source_not_dropped")
        if "when" in rule:
            validate_condition(rule["when"], headers)
        if "middle_column" in rule:
            middle = rule["middle_column"]
            require(isinstance(middle, str) and rule["kind"] == "name" and middle in headers and source_removed(treatments[middle], rule), "policy_invalid")


def validate_policy(policy: Any) -> None:
    """Validate a policy; any unexpected shape is the named error, never an exception that could carry a value."""
    try:
        check_policy(policy)
    except (TypeError, KeyError, AttributeError):
        raise PseudonymizationError("policy_invalid") from None


def base_text(value: str) -> str:
    """NFKC, control and space characters as one space, trimmed and case-folded."""
    spaced = "".join(" " if unicodedata.category(c) in {"Cc", "Zs", "Zl", "Zp"} else c for c in unicodedata.normalize("NFKC", value))
    return " ".join(spaced.split()).casefold()


def normalize_id(value: str) -> str:
    """An authoritative identifier: base text without any space; leading zeros and every other character stay."""
    text = base_text(value)
    return "" if text in NO_IDENTITY else text.replace(" ", "")


def normalize_name(value: str) -> str:
    """A name part: base text without accents and apostrophes, other punctuation as a space, generational suffixes dropped when a word remains."""
    text = base_text(value)
    if text in NO_IDENTITY:
        return ""
    stripped = "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)).translate(APOSTROPHES)
    words = "".join(c if c.isalnum() else " " for c in stripped).split()
    while len(words) > 1 and words[-1] in NAME_SUFFIXES:
        words.pop()
    return " ".join(words)


def normalized_parts(kind: str, values: list[str]) -> list[str] | None:
    """The normalized parts of one identity, or None when any part is empty and the identity cannot match."""
    parts = [normalize_id(v) if kind == "id" else normalize_name(v) for v in values]
    return parts if all(parts) else None


def derive(key: bytes, key_version: str, namespace: str, kind: str, parts: list[str]) -> str:
    """The pseudonym: the key version and the HMAC-SHA-256 of the length-checked, domain-separated message."""
    fields = [DOMAIN, NORMALIZATION_VERSION.encode(), namespace.encode(), kind.encode(), str(len(parts)).encode(), *(p.encode() for p in parts)]
    return f"{key_version}:{hmac.new(key, SEPARATOR.join(fields), hashlib.sha256).hexdigest()}"


def key_check(key: bytes) -> str:
    """A short value that identifies the key without revealing it: the first 16 hex digits of the HMAC of a fixed label."""
    return hmac.new(key, KEY_CHECK_LABEL, hashlib.sha256).hexdigest()[:16]


def load_key(version: str, reader: KeyReader) -> bytes:
    """Read the key text for one version through the reader and decode it; the key stays in memory."""
    require(isinstance(version, str) and KEY_VERSION.fullmatch(version) is not None, "key_version_invalid")
    text = reader(version)
    stripped = (text or "").strip()
    require(bool(stripped), "key_missing")
    standard = stripped.replace("-", "+").replace("_", "/")
    try:
        key = base64.b64decode(standard + "=" * (-len(standard) % 4), validate=True)
    except (binascii.Error, ValueError):
        raise PseudonymizationError("key_malformed") from None
    require(len(key) == KEY_BYTES, "key_malformed")
    return key


def parameter_store_reader(client: Any) -> KeyReader:
    """A reader over a Parameter Store client: one SecureString per key version, read with decryption at use and checked for type and name."""

    def read(version: str) -> str | None:
        name = PARAMETER_PREFIX + version
        try:
            with quiet_sdk_logs():
                parameter = client.get_parameter(Name=name, WithDecryption=True)["Parameter"]
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ParameterNotFound":
                return None
            raise PseudonymizationError("key_read_failed") from None
        except (BotoCoreError, KeyError, TypeError):
            raise PseudonymizationError("key_read_failed") from None
        require(isinstance(parameter, dict), "key_read_failed")
        require(parameter.get("Type") == "SecureString" and parameter.get("Name") == name, "key_parameter_invalid")
        value = parameter.get("Value")
        return value if isinstance(value, str) else None

    return read


def open_parameter_client(profile: str, region: str) -> Any:
    """A bounded Parameter Store client for the project's own profile and region."""
    require(bool(profile) and bool(region) and "admin" not in profile.lower(), "aws_settings_invalid")
    import boto3

    try:
        return boto3.Session(profile_name=profile, region_name=region).client("ssm", config=Config(**CLIENT_OPTIONS))
    except BotoCoreError:
        raise PseudonymizationError("aws_settings_invalid") from None


@lru_cache(maxsize=4096)
def classify(value: str, equals: str, others: tuple[str, ...]) -> bool | None:
    """True for the selecting value, False for another approved value and None for any other value, compared after the base normalization."""
    text = base_text(value)
    if text == base_text(equals):
        return True
    return False if text in {base_text(v) for v in others} else None


def selects(condition: dict, value: str) -> bool | None:
    """The classification of one discriminator cell under a condition."""
    return classify(value, condition["equals"], tuple(condition["others"]))


def matches(condition: dict | None, row: dict[str, str]) -> bool:
    """True when no condition is set or the row cell is the selecting value; the run has already refused any unapproved value."""
    return condition is None or selects(condition, row[condition["column"]]) is True


def conditions(policy: dict) -> list[dict]:
    """Every distinct condition the policy uses, in policy order."""
    found = [r["blank_when"] for r in policy["treatments"].values() if "blank_when" in r] + [r["when"] for r in policy["pseudonyms"] if "when" in r]
    return [c for i, c in enumerate(found) if c not in found[:i]]


def scrub_tokens(value: str) -> list[str]:
    """The raw words of a value for scrub matching, shared by known names and free text: apostrophe forms folded first, then case and combining marks."""
    plain = "".join(c for c in unicodedata.normalize("NFKD", base_text(value.translate(APOSTROPHE_FORMS))) if not unicodedata.combining(c))
    return TEXT_TOKEN.findall(plain)


def scrub_name_words(value: str) -> tuple[str, ...]:
    """A known name as scrub words: the same word rules as free text, with a trailing generational suffix dropped when another word remains.

    The replacement marker is no part of a name: a cell an earlier pass already replaced collects no name, so a second pass cannot start scrubbing the word
    "redacted" wherever it appears.
    """
    words = [w for w in (text_word(token) for token in scrub_tokens(MARKER.sub(" ", value))) if w]
    while len(words) > 1 and words[-1] in NAME_SUFFIXES:
        words.pop()
    return tuple(words)


def person_words(policy: dict, row: dict[str, str]) -> Iterator[tuple[str, ...]]:
    """The name word sequences of the people one row names, for matching inside kept text cells; a single-word name counts.

    A name counts for scrubbing when at least one of its fields holds a real name word, even when identity normalization treats another field as
    empty (an empty marker such as ``Na``): scrub eligibility is separate from HMAC identity eligibility, which stays unchanged. Its words come
    from the shared scrub rules, never from the identity normalization, so the name list and the text can never be normalized differently.
    """
    for rule in policy["pseudonyms"]:
        if rule["kind"] != "name" or not matches(rule.get("when"), row):
            continue
        values = [row[c] for c in rule["columns"]]
        if any(normalize_name(value) for value in values):
            words = scrub_name_words(" ".join(values))
            if words:
                yield words
        if rule.get("middle_column") and scrub_name_words(row[rule["middle_column"]]):
            *first, last = values
            with_middle = [*first, row[rule["middle_column"]], last]
            # The middle variant is eligible on its own fields, so a real middle name with empty-marker first and last names is still indexed.
            if any(normalize_name(value) for value in with_middle):
                middle = scrub_name_words(" ".join(with_middle))
                if middle:
                    yield middle


def person_identifiers(policy: dict, row: dict[str, str]) -> Iterator[str]:
    """The identifiers one row holds, as lower-case letters and digits only, for matching inside kept text cells."""
    for rule in policy["pseudonyms"]:
        if rule["kind"] == "id" and matches(rule.get("when"), row):
            parts = normalized_parts("id", [row[c] for c in rule["columns"]])
            if parts is not None:
                yield "".join(c for c in parts[0] if c.isalnum())


def text_word(token: str) -> str:
    """One word of free text for name matching: connectors removed, every word kept (unlike identity normalization, which treats some words as empty)."""
    return token.translate(APOSTROPHES)


def index_names(names: set[tuple[str, ...]]) -> dict[str, list[tuple[str, ...]]]:
    """Known name word sequences grouped by their first word, built once per transformation."""
    index: dict[str, list[tuple[str, ...]]] = {}
    for name in names:
        index.setdefault(name[0], []).append(name)
    return index


def contains_name(text: str, index: dict[str, list[tuple[str, ...]]]) -> bool:
    """True when the text holds any known person's whole name, of one word or more, as consecutive words in order.

    Each word is matched as written or with one possessive ending removed, after combining marks are dropped and connectors deleted. Text words are
    never discarded, so a word that is an empty marker in an identity field (for example "na") still has to match.
    """
    options = [{text_word(token), text_word(POSSESSIVE_END.sub("", token))} - {""} for token in scrub_tokens(text)]
    for start, forms in enumerate(options):
        for first in forms:
            for name in index.get(first, ()):
                window = options[start : start + len(name)]
                if len(window) == len(name) and all(part in alternatives for part, alternatives in zip(name, window, strict=True)):
                    return True
    return False


def contains_identifier(text: str, identifiers: set[str], longest: int) -> bool:
    """True when consecutive letter and digit runs of the text, joined, equal a known identifier: it may be typed with spaces or dashes."""
    if not identifiers:
        return False
    runs = re.findall(r"[^\W_]+", base_text(text))
    for start in range(len(runs)):
        joined = ""
        for run in runs[start:]:
            joined += run
            if len(joined) > longest:
                break
            if joined in identifiers:
                return True
    return False


def refuse_short_values(names: set[tuple[str, ...]], identifiers: set[str]) -> None:
    """Fail closed: a name or identifier too short to match safely in text stops the run before any output."""
    require(all(len(i) >= MIN_IDENTIFIER_CHARACTERS for i in identifiers), "identifier_too_short")
    require(all(len(n) > 1 or len(n[0]) >= MIN_NAME_CHARACTERS for n in names), "name_too_short")


def output_headers(policy: dict) -> list[str]:
    """The derivative header: kept columns in source order, then one column per pseudonym rule."""
    return [*output_kept(policy), *(r["output"] for r in policy["pseudonyms"])]


@dataclass(frozen=True)
class RowScan:
    """What the first pass over all rows finds: the row count and the names and identifiers that kept text cells are scrubbed against."""

    count: int
    name_index: dict[str, list[tuple[str, ...]]]
    identifiers: set[str]
    longest: int


def scan_rows(policy: dict, source: Iterable[dict[str, str]]) -> RowScan:
    """The first pass: refuse an unapproved discriminator value, collect the people the rows name and apply the row minimum and the short-value refusal."""
    names: set[tuple[str, ...]] = set()
    identifiers: set[str] = set()
    count = 0
    approved = conditions(policy)
    for row in source:
        count += 1
        require(all(selects(c, row[c["column"]]) is not None for c in approved), "discriminator_unknown")
        names.update(person_words(policy, row))
        identifiers.update(person_identifiers(policy, row))
    require(count >= policy["min_rows"], "row_count_below_minimum")
    if any(t["treatment"] == "scrub_names" for t in policy["treatments"].values()):
        refuse_short_values(names, identifiers)
    return RowScan(count, index_names(names), identifiers, max((len(i) for i in identifiers), default=0))


def new_statistics(policy: dict) -> tuple[dict[str, Any], dict[str, set[str]]]:
    """Empty counters for one run and the sets that count each pseudonym's distinct values."""
    statistics: dict[str, Any] = {"rows": 0, "scrubbed": 0, "blanked": 0, "written": {r["output"]: 0 for r in policy["pseudonyms"]}, "distinct": {}}
    return statistics, {r["output"]: set() for r in policy["pseudonyms"]}


def convert_row(
    policy: dict, key: bytes, key_version: str, row: dict[str, str], scan: RowScan, statistics: dict[str, Any], seen: dict[str, set[str]]
) -> list[str]:
    """One output row: the kept cells in source order, blanked or scrubbed where the policy says, then one pseudonym cell per rule."""
    treatments = policy["treatments"]
    statistics["rows"] += 1
    values = []
    for column in output_kept(policy):
        rule, cell = treatments[column], row[column]
        if "blank_when" in rule and matches(rule["blank_when"], row) and cell:
            cell = ""
            statistics["blanked"] += 1
        elif (
            rule["treatment"] == "scrub_names" and cell and (contains_name(cell, scan.name_index) or contains_identifier(cell, scan.identifiers, scan.longest))
        ):
            cell = REPLACEMENT
            statistics["scrubbed"] += 1
        values.append(cell)
    for rule in policy["pseudonyms"]:
        parts = normalized_parts(rule["kind"], [row[c] for c in rule["columns"]]) if matches(rule.get("when"), row) else None
        pseudonym = "" if parts is None else derive(key, key_version, rule["namespace"], rule["kind"], parts)
        if pseudonym:
            statistics["written"][rule["output"]] += 1
            seen[rule["output"]].add(pseudonym)
        values.append(pseudonym)
    return values


def output_kept(policy: dict) -> list[str]:
    """The source columns that stay in the derivative, in source order."""
    return [h for h in policy["headers"] if policy["treatments"][h]["treatment"] != "drop"]


COMPARED = ("derivative_policy", "normalization_version", "key_version", "key_check")
