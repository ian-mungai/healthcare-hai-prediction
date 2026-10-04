"""Build, verify and upload redacted replacements for S3 text copies that name people.

Run from the repository root:

    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/redact_text_copies.py build --targets T --policy P --output DIR
    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/redact_text_copies.py upload --output DIR --record R

``build`` is local only: it classifies every target, writes redacted copies for the ones holding personal data, re-reads
each copy independently and writes a deterministic build report. ``upload`` stores each verified copy and its manifest with
the collectors' write-once ``store_file`` under the project profile. The rule and failure modes are in
``replacement_failure_modes.md`` and ``redaction_policy.json``. The name dictionary exists only in memory; reports hold
counts, column names, line codes and hashes, never values.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import tempfile
import unicodedata
from collections import Counter
from collections.abc import Callable, Iterator
from itertools import zip_longest
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, store_file, verify_version, write_once
from scripts.acquisition.source_registry import canonical_hash
from scripts.infrastructure.render_project_config import load_configuration, verify_project_identity

csv.field_size_limit(1 << 30)
LETTER = re.compile(r"[^\W\d_]")
WORD = re.compile(r"[^\W\d_]+")
EMAIL = re.compile(r"(?<![\w.%+-])[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9-]{1,63}(?:\.[A-Za-z0-9-]{1,63}){0,8}\.[A-Za-z]{2,24}(?![\w-])")
ATTRIBUTE = re.compile(r'(\sv=")([^"]*)(")')
TEXT_NODE = re.compile(r"(<t(?:\s[^>]*)?>)([^<]*)(</t>)")
ENTITY = re.compile(r"&(?:(amp|lt|gt|quot|apos)|#(\d{1,7})|#x([0-9A-Fa-f]{1,6}));")
PIVOT_RECORD = re.compile(r"<r>(.*?)</r>", re.S)
PIVOT_CHILD = re.compile(r'<([a-z]+)((?:\s+[\w:]+="[^"]*")*)\s*/>')
NAMED = {"amp": "&", "lt": "<", "gt": ">", "quot": '"', "apos": "'"}
XML_TOKEN = re.compile(
    r"<\?.*?\?>|<!--.*?-->|<!\[CDATA\[.*?\]\]>|<!.*?>|</([\w:.-]+)\s*>"
    r"|<([\w:.-]+)((?:\s+[\w:.-]+\s*=\s*(?:\"[^\"<]*\"|'[^'<]*'))*)\s*(/?)>|[^<]+",
    re.S,
)
XML_ATTRIBUTE = re.compile(r"([\w:.-]+)\s*=\s*(?:\"([^\"<]*)\"|'([^'<]*)')")


class RedactionError(ValueError):
    """Carries a static diagnostic code only, never a source value."""


def require(condition: object, code: str) -> None:
    if not condition:
        raise RedactionError(code)


def normalized(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).split()).casefold()


def trie_pattern(words: list[str]) -> str:
    """Return a regex alternation over ``words`` built as a trie, so each position follows one branch."""
    root: dict[str, Any] = {}
    for word in words:
        node = root
        for character in word:
            node = node.setdefault(character, {})
        node[""] = {}

    def emit(node: dict[str, Any]) -> str:
        branches = [(r"\s+" if key == " " else re.escape(key)) + emit(child) for key, child in sorted(node.items()) if key]
        if not branches:
            return ""
        body = branches[0] if len(branches) == 1 else "(?:" + "|".join(branches) + ")"
        # A word that ends here and also continues prefers the longer name first.
        return "(?:" + body + ")?" if "" in node else body

    return emit(root)


class Matcher:
    """Replace dictionary names and email addresses in text; cache results per distinct value."""

    def __init__(self, names: set[str], replacement: str) -> None:
        self.replacement = replacement
        self.size = len(names)
        pattern = trie_pattern(sorted(names)) if names else ""
        self.names = re.compile(r"(?<!\w)(?:" + pattern + r")(?!\w)", re.IGNORECASE) if names else None
        self.cache: dict[str, tuple[str, int, int]] = {}

    def scrub(self, value: str) -> tuple[str, int, int]:
        """Return the scrubbed text and the number of email and name replacements."""
        if not LETTER.search(value) and "@" not in value:
            return value, 0, 0
        if value in self.cache:
            return self.cache[value]
        text, emails, names = self.replace(value)
        if self.residual(text):
            # Rare Unicode case differences: redact the normalized text instead, as the earlier abstraction did.
            text, emails, names = self.replace(normalized(value))
        require(not self.residual(text), "residual_after_scrub")
        if len(self.cache) < 2_000_000:
            self.cache[value] = (text, emails, names)
        return text, emails, names

    def replace(self, value: str) -> tuple[str, int, int]:
        text, emails = EMAIL.subn(self.replacement, value) if "@" in value else (value, 0)
        text, names = self.names.subn(self.replacement, text) if self.names else (text, 0)
        return text, emails, names

    def residual(self, value: str) -> bool:
        if not LETTER.search(value) and "@" not in value:
            return False
        text = normalized(value)
        return bool(("@" in text and EMAIL.search(text)) or (self.names and self.names.search(text)))


def load_policy(path: Path) -> dict[str, Any]:
    policy = json.loads(path.read_text(encoding="utf-8"))
    require(policy.get("policy_version") == 1 and policy.get("replacement") == "[REDACTED]", "unapproved_policy")
    require(bool(policy.get("delimited_name_fields")) and bool(policy.get("finding")), "invalid_policy")
    long_format = policy["long_format"]
    require(long_format["code_field"] != long_format["value_field"], "invalid_long_format")
    require(policy["dataset_suffix"] == "_redacted", "invalid_dataset_suffix")
    return dict(policy)


def load_targets(path: Path) -> list[dict[str, Any]]:
    targets = json.loads(path.read_text(encoding="utf-8"))["objects"]
    identities = [(item["key"], item["version_id"]) for item in targets]
    require(len(identities) == len(set(identities)) and targets, "duplicate_or_empty_targets")
    for item in targets:
        require(re.fullmatch(r"[a-f0-9]{64}", item["sha256"]) and item["sha256"] in item["key"].split("/"), "invalid_target")
    return sorted(targets, key=lambda item: (item["key"], item["version_id"]))


def kind(target: dict[str, Any], policy: dict[str, Any]) -> str:
    """Classify a target by file name and header; unknown formats stop the run."""
    name = PurePosixPath(target["key"]).name
    suffix = PurePosixPath(name).suffix.lower()
    if name == policy["pivot_cache"]["records_part"]:
        return "pivot_records"
    if suffix in policy["xml_suffixes"]:
        return "xml"
    if suffix in policy["metadata_suffixes"]:
        return "metadata"
    require(suffix in policy["table_suffixes"], "unsupported_format")
    header = table_header(Path(target["local_path"]))[0]
    long_format = policy["long_format"]
    return "long" if {long_format["code_field"], long_format["value_field"]} <= set(header) else "table"


# Delimited tables ---------------------------------------------------------------------------------------------------


def records(path: Path) -> Iterator[tuple[bytes, str, bytes]]:
    """Yield (raw bytes, decoded body without its line ending, line ending) per CSV record, joined by quote parity."""
    with path.open("rb") as handle:
        pending: list[bytes] = []
        quotes = 0
        for line in handle:
            pending.append(line)
            quotes += line.count(b'"')
            if quotes % 2:
                continue
            raw = b"".join(pending)
            pending, quotes = [], 0
            ending = b"\r\n" if raw.endswith(b"\r\n") else b"\n" if raw.endswith(b"\n") else b""
            try:
                body = raw[: len(raw) - len(ending)].decode("utf-8")
            except UnicodeDecodeError:
                raise RedactionError("not_utf8") from None
            yield raw, body, ending
        require(not pending, "unbalanced_quotes")


def delimiter_of(header: str) -> str:
    return "\t" if header.count("\t") > header.count(",") else ","


def table_header(path: Path) -> tuple[list[str], str]:
    for _raw, body, _ending in records(path):
        text = body.removeprefix("\ufeff")
        delimiter = delimiter_of(text)
        return next(csv.reader([text], delimiter=delimiter)), delimiter
    raise RedactionError("empty_table")


def parse(body: str, delimiter: str) -> list[str]:
    rows = list(csv.reader([body], delimiter=delimiter))
    require(len(rows) == 1, "record_parse")
    return rows[0]


def raw_fields(body: str, delimiter: str) -> list[str]:
    """Split one record into its raw field texts, keeping each field's original quoting."""
    fields, start, quoted = [], 0, False
    for index, character in enumerate(body):
        if character == '"':
            quoted = not quoted
        elif character == delimiter and not quoted:
            fields.append(body[start:index])
            start = index + 1
    fields.append(body[start:])
    return fields


def unquote(raw: str) -> str:
    return raw[1:-1].replace('""', '"') if len(raw) >= 2 and raw[0] == raw[-1] == '"' else raw


def rebuild(body: str, row: list[str], delimiter: str, changes: dict[int, str]) -> str:
    """Rewrite only the changed cells of one record, keeping every other field's bytes and quoting."""
    fields = raw_fields(body, delimiter)
    require(len(fields) == len(row) and [unquote(field) for field in fields] == row, "field_split_mismatch")
    for index, value in changes.items():
        quote = len(fields[index]) >= 2 and fields[index][0] == '"' or any(mark in value for mark in (delimiter, '"', "\n", "\r"))
        fields[index] = '"' + value.replace('"', '""') + '"' if quote else value
    rebuilt = delimiter.join(fields)
    expected = list(row)
    for index, value in changes.items():
        expected[index] = value
    require(parse(rebuilt, delimiter) == expected, "rebuild_mismatch")
    return rebuilt


def long_code_is_named(code: str, policy: dict[str, Any]) -> bool:
    long_format = policy["long_format"]
    if code in long_format["name_codes"] or code in long_format["contact_codes"]:
        return True
    for block in long_format["name_code_ranges"]:
        line = code.removeprefix(block["prefix"])
        if code.startswith(block["prefix"]) and line.isdigit() and block["first_line"] <= int(line) <= block["last_line"]:
            return True
    return False


def named_selector(header: list[str], mode: str, policy: dict[str, Any]) -> Callable[[list[str]], dict[int, str]]:
    """Return a function mapping a row to {column index: label} for cells redacted whole."""
    if mode == "table":
        fixed = {index: name for index, name in enumerate(header) if name in policy["delimited_name_fields"]}
        return lambda row: fixed
    code_index, value_index = header.index(policy["long_format"]["code_field"]), header.index(policy["long_format"]["value_field"])

    def select(row: list[str]) -> dict[int, str]:
        code = row[code_index]
        return {value_index: code} if long_code_is_named(code, policy) else {}

    return select


def table_names(path: Path, mode: str, policy: dict[str, Any]) -> Iterator[str]:
    """Yield the values of every named cell, for the in-memory dictionary."""
    header, delimiter = table_header(path)
    select = named_selector(header, mode, policy)
    for position, (_raw, body, _ending) in enumerate(records(path)):
        row = parse(body, delimiter) if position else []
        if len(row) == len(header):
            for index, label in select(row).items():
                if not (mode == "long" and label in policy["long_format"]["contact_codes"]):
                    yield row[index]


def redact_table(path: Path, destination: Path, mode: str, matcher: Matcher, policy: dict[str, Any]) -> dict[str, Any]:
    header, delimiter = table_header(path)
    replacement = policy["replacement"]
    select = named_selector(header, mode, policy)
    code_index = header.index(policy["long_format"]["code_field"]) if mode == "long" else -1
    named: Counter[str] = Counter()
    emails: Counter[str] = Counter()
    names: Counter[str] = Counter()
    count = changed = 0
    with destination.open("wb") as output:
        for position, (raw, body, ending) in enumerate(records(path)):
            if position == 0:
                output.write(raw)
                continue
            count += 1
            row = parse(body, delimiter)
            if not row:
                output.write(raw)
                continue
            require(len(row) == len(header), "row_width_mismatch")
            whole = select(row)
            changes: dict[int, str] = {}
            for index, value in enumerate(row):
                label = header[index] if mode == "table" else f"{header[index]}@{row[code_index]}"
                if index in whole:
                    if value.strip() and value != replacement:
                        changes[index] = replacement
                        named[whole[index]] += 1
                    continue
                text, email_count, name_count = matcher.scrub(value)
                if text != value:
                    changes[index] = text
                    emails[label] += 1 if email_count else 0
                    names[label] += 1 if name_count else 0
            if changes:
                changed += 1
                output.write(rebuild(body, row, delimiter, changes).encode("utf-8") + ending)
            else:
                output.write(raw)
    return {
        "records": count,
        "records_changed": changed,
        "named_fields_present": sorted(set(policy["delimited_name_fields"]) & set(header)) if mode == "table" else [policy["long_format"]["value_field"]],
        "named_cells": dict(sorted(named.items())),
        "email_cells": dict(sorted((key, value) for key, value in emails.items() if value)),
        "dictionary_cells": dict(sorted((key, value) for key, value in names.items() if value)),
    }


def verify_table(original: Path, output: Path, mode: str, matcher: Matcher, policy: dict[str, Any]) -> dict[str, Any]:
    """Re-read both files with the standard CSV parser and check every cell independently of the build."""
    header, delimiter = table_header(original)
    select = named_selector(header, mode, policy)
    replacement = policy["replacement"]
    checked = 0
    with original.open(newline="", encoding="utf-8") as left, output.open(newline="", encoding="utf-8") as right:
        for position, (before, after) in enumerate(zip_longest(csv.reader(left, delimiter=delimiter), csv.reader(right, delimiter=delimiter))):
            require(before is not None and after is not None and len(before) == len(after), "record_structure_changed")
            if position == 0 or not before:
                require(before == after, "header_or_blank_changed")
                continue
            whole = select(before)
            for index, (value, result) in enumerate(zip(before, after, strict=True)):
                if index in whole:
                    require(result == (replacement if value.strip() else value), "named_cell_not_redacted")
                    continue
                expected = matcher.scrub(value)[0]
                require(result == expected, "unexpected_change")
                require(not matcher.residual(result), "residual_personal_data")
            checked += 1
    return {"records_checked": checked, "residual_matches": 0, "named_cells_all_redacted": True, "numeric_cells_changed": 0}


# XML string tables and pivot caches ---------------------------------------------------------------------------------


def xml_unescape(text: str) -> str:
    require("&" not in ENTITY.sub("", text), "unsupported_xml_entity")

    def value(match: re.Match[str]) -> str:
        name, decimal, hexadecimal = match.groups()
        return NAMED[name] if name else chr(int(decimal) if decimal else int(hexadecimal, 16))

    return ENTITY.sub(value, text)


def xml_escape(text: str, attribute: bool) -> str:
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return text.replace('"', "&quot;") if attribute else text


def read_xml(path: Path) -> str:
    try:
        # Decode the exact bytes; read_text would turn CRLF into LF and change unredacted lines.
        return path.read_bytes().decode("utf-8")
    except UnicodeDecodeError:
        raise RedactionError("not_utf8") from None


def xml_elements(text: str) -> list[dict[str, Any]]:
    """Tokenize XML strictly and return its elements in document order.

    A small parser of its own instead of an XML library: it rejects DOCTYPE declarations (so no entity expansion),
    requires balanced tags and one root, and decodes only the five XML entities and numeric references.
    """
    elements: list[dict[str, Any]] = []
    stack: list[int] = []
    position = roots = 0
    for match in XML_TOKEN.finditer(text):
        require(match.start() == position, "xml_not_well_formed")
        position = match.end()
        token, closing, opening, attributes, empty = match.group(0), *match.groups()
        if token.startswith(("<?", "<!--")):
            continue
        require(not token.startswith("<!") or token.startswith("<![CDATA["), "xml_doctype_not_allowed")
        if closing:
            require(stack and elements[stack[-1]]["tag"] == closing, "xml_not_well_formed")
            stack.pop()
        elif opening:
            values: dict[str, str] = {}
            for pair in XML_ATTRIBUTE.finditer(attributes):
                require(pair.group(1) not in values, "xml_duplicate_attribute")
                values[pair.group(1)] = xml_unescape(pair.group(2) if pair.group(2) is not None else pair.group(3))
            if stack:
                elements[stack[-1]]["children"].append(len(elements))
            else:
                roots += 1
            elements.append({"tag": opening, "attributes": values, "text": [], "children": []})
            if not empty:
                stack.append(len(elements) - 1)
        elif stack:
            elements[stack[-1]]["text"].append(token[9:-3] if token.startswith("<![CDATA[") else xml_unescape(token))
        else:
            require(not token.strip(), "xml_not_well_formed")
    require(position == len(text) and not stack and roots == 1, "xml_not_well_formed")
    return elements


def local_name(tag: str) -> str:
    return tag.rsplit(":", 1)[-1]


def pivot_field_index(records_path: Path, policy: dict[str, Any]) -> int:
    definition = records_path.with_name(policy["pivot_cache"]["definition_part"])
    require(definition.is_file(), "pivot_definition_missing")
    fields = [element["attributes"].get("name") for element in xml_elements(read_xml(definition)) if local_name(element["tag"]) == "cacheField"]
    matches = [index for index, name in enumerate(fields) if name in policy["pivot_cache"]["name_fields"]]
    require(len(matches) == 1, "pivot_name_field_not_unique")
    return matches[0]


def pivot_children(block: str) -> list[re.Match[str]]:
    children = list(PIVOT_CHILD.finditer(block))
    require("".join(match.group(0) for match in children) == block, "unsupported_pivot_record")
    return children


def pivot_names(path: Path, policy: dict[str, Any]) -> Iterator[str]:
    index = pivot_field_index(path, policy)
    for record in PIVOT_RECORD.finditer(read_xml(path)):
        child = pivot_children(record.group(1))[index]
        require(child.group(1) in {"s", "m"}, "pivot_name_field_not_inline")
        found = ATTRIBUTE.search(child.group(0))
        if child.group(1) == "s" and found:
            yield xml_unescape(found.group(2))


def redact_xml(path: Path, destination: Path, mode: str, matcher: Matcher, policy: dict[str, Any]) -> dict[str, Any]:
    text = read_xml(path)
    xml_elements(text)
    replacement = policy["replacement"]
    counts: Counter[str] = Counter()

    def scrub(match: re.Match[str], attribute: bool) -> str:
        value = xml_unescape(match.group(2))
        result, email_count, name_count = matcher.scrub(value)
        if result == value:
            return match.group(0)
        counts["email_values"] += 1 if email_count else 0
        counts["dictionary_values"] += 1 if name_count else 0
        return match.group(1) + xml_escape(result, attribute) + match.group(3)

    if mode == "pivot_records":
        index = pivot_field_index(path, policy)

        def record(match: re.Match[str]) -> str:
            children = pivot_children(match.group(1))
            parts = []
            for position, child in enumerate(children):
                piece = child.group(0)
                if position == index:
                    require(child.group(1) in {"s", "m"}, "pivot_name_field_not_inline")
                    found = ATTRIBUTE.search(piece)
                    if child.group(1) == "s" and found and xml_unescape(found.group(2)).strip() and xml_unescape(found.group(2)) != replacement:
                        counts["named_values"] += 1
                        piece = piece[: found.start(2)] + replacement + piece[found.end(2) :]
                else:
                    piece = ATTRIBUTE.sub(lambda found: scrub(found, True), piece)
                parts.append(piece)
            return "<r>" + "".join(parts) + "</r>"

        result = PIVOT_RECORD.sub(record, text)
    else:
        result = TEXT_NODE.sub(lambda match: scrub(match, False), ATTRIBUTE.sub(lambda match: scrub(match, True), text))
    destination.write_bytes(result.encode("utf-8"))
    return dict(sorted((key, value) for key, value in counts.items() if value))


def verify_xml(original: Path, output: Path, mode: str, matcher: Matcher, policy: dict[str, Any]) -> dict[str, Any]:
    """Re-parse both files with the strict tokenizer; the structure must match and no name or email may remain."""
    before, after = xml_elements(read_xml(original)), xml_elements(read_xml(output))
    shape = [(element["tag"], sorted(element["attributes"]), len(element["children"])) for element in before]
    require(shape == [(element["tag"], sorted(element["attributes"]), len(element["children"])) for element in after], "xml_structure_changed")
    named_indices: set[int] = set()
    if mode == "pivot_records":
        index = pivot_field_index(original, policy)
        for element in before:
            if local_name(element["tag"]) == "r":
                require(len(element["children"]) > index, "unsupported_pivot_record")
                named_indices.add(element["children"][index])
    for position, (old, new) in enumerate(zip(before, after, strict=True)):
        for attribute, value in old["attributes"].items():
            expected = policy["replacement"] if position in named_indices and attribute == "v" and value.strip() else matcher.scrub(value)[0]
            require(new["attributes"][attribute] == expected, "unexpected_change")
        require(new["text"] == [matcher.scrub(value)[0] for value in old["text"]], "unexpected_change")
    unchanged_tokens = re.compile(r"<\?.*?\?>|<!--.*?-->", re.S)
    require(unchanged_tokens.findall(read_xml(original)) == unchanged_tokens.findall(read_xml(output)), "unexpected_change")
    for element in after:
        require(not any(matcher.residual(value) for value in [*element["text"], *element["attributes"].values()]), "residual_personal_data")
    if mode == "pivot_records":
        index = pivot_field_index(original, policy)
        for record in (element for element in after if local_name(element["tag"]) == "r"):
            require(len(record["children"]) > index, "unsupported_pivot_record")
            child = after[record["children"][index]]
            require(local_name(child["tag"]) == "m" or child["attributes"].get("v") in {"", policy["replacement"]}, "named_cell_not_redacted")
    return {"elements_checked": len(after), "residual_matches": 0}


# Build ----------------------------------------------------------------------------------------------------------------


def verified_original(target: dict[str, Any]) -> Path:
    path = Path(target["local_path"])
    require(path.is_file() and not path.is_symlink(), "original_missing")
    require(fingerprint(path) == (target["sha256"], target["bytes"]), "original_changed")
    return path


def dictionary(targets: list[dict[str, Any]], kinds: dict[str, str], policy: dict[str, Any]) -> tuple[set[str], Counter[str]]:
    """Collect every named value from all targets into an in-memory set; return it with per-source counts only."""
    skip = set(policy["non_identity_tokens"])
    names: set[str] = set()
    sources: Counter[str] = Counter()
    done: set[str] = set()
    for target in targets:
        mode = kinds[target["sha256"]]
        if target["sha256"] in done or mode not in {"table", "long", "pivot_records"}:
            continue
        done.add(target["sha256"])
        path = verified_original(target)
        values = pivot_names(path, policy) if mode == "pivot_records" else table_names(path, mode, policy)
        for value in values:
            text = normalized(value)
            if text and text not in skip and LETTER.search(text):
                names.add(text)
                sources[mode] += 1
    return names, sources


def cross_field_names(names: set[str], policy: dict[str, Any]) -> set[str]:
    """Keep the entries that look like people for matching inside other fields.

    Named fields are redacted whole whatever they hold. Organization names and roles that turn up in those fields (a
    preparer firm, "Board of Directors") would otherwise erase owner, facility and organization names across the files.
    """
    excluded = set(policy["cross_field_exclusion_words"])
    return {name for name in names if len(name.split()) >= policy["cross_field_minimum_words"] and not excluded & set(WORD.findall(name))}


def build(targets_path: Path, policy_path: Path, output: Path) -> dict[str, Any]:
    policy, targets = load_policy(policy_path), load_targets(targets_path)
    kinds: dict[str, str] = {}
    for target in targets:
        verified_original(target)
        kinds.setdefault(target["sha256"], kind(target, policy))
    names, sources = dictionary(targets, kinds, policy)
    cross = cross_field_names(names, policy)
    matcher = Matcher(cross, policy["replacement"])
    copies = output / "copies"
    copies.mkdir(parents=True, exist_ok=True)
    results: dict[str, dict[str, Any]] = {}
    for target in targets:
        digest = target["sha256"]
        if digest in results:
            continue
        mode, path = kinds[digest], verified_original(target)
        if mode == "metadata":
            text = path.read_text(encoding="utf-8")
            json.loads(text)
            require(not (matcher.names and matcher.names.search(normalized(text))), "metadata_contains_names")
            results[digest] = {"format": mode, "status": "no_personal_data", "note": "publisher metadata; contact points kept (F4)"}
            continue
        folder = copies / digest
        folder.mkdir(exist_ok=True)
        final = folder / PurePosixPath(target["key"]).name
        with tempfile.TemporaryDirectory(prefix="redaction_", dir=folder) as temporary:
            staged = Path(temporary) / final.name
            if mode in {"table", "long"}:
                counts = redact_table(path, staged, mode, matcher, policy)
                verification = verify_table(path, staged, mode, matcher, policy)
            else:
                counts = redact_xml(path, staged, mode, matcher, policy)
                verification = verify_xml(path, staged, mode, matcher, policy)
            require(fingerprint(path) == (digest, target["bytes"]), "original_changed_during_build")
            redacted_digest, redacted_size = fingerprint(staged)
            if redacted_digest == digest:
                results[digest] = {"format": mode, "status": "no_personal_data", "verification": verification}
                continue
            if final.exists():
                require(fingerprint(final) == (redacted_digest, redacted_size), "conflicting_output")
            else:
                os.rename(staged, final)
        results[digest] = {
            "format": mode,
            "status": "replace",
            "redacted_sha256": redacted_digest,
            "redacted_bytes": redacted_size,
            "counts": counts,
            "verification": verification,
        }
    for digest, result in results.items():
        folder = copies / digest
        if result["status"] != "replace" and folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()
    objects = [{**{field: target[field] for field in ("key", "version_id", "sha256", "bytes")}, **results[target["sha256"]]} for target in targets]
    report = {
        "kind": "redacted_replacement_build",
        "policy_sha256": fingerprint(policy_path)[0],
        "targets_sha256": fingerprint(targets_path)[0],
        "dictionary": {
            "entries": len(names),
            "cross_field_entries": len(cross),
            "single_word_entries": sum(" " not in name for name in names),
            "named_values_by_source": dict(sorted(sources.items())),
        },
        "objects": objects,
        "summary": dict(sorted(Counter(item["status"] for item in objects).items())),
    }
    write_once(output / "build_report.json", encoded_json(report))
    return report


def verify_build(targets_path: Path, policy_path: Path, output: Path) -> dict[str, Any]:
    """Verify recorded copies against every original and the permitted transformation without changing files."""
    targets, policy = load_targets(targets_path), load_policy(policy_path)
    report = json.loads((output / "build_report.json").read_text(encoding="utf-8"))
    require(report["policy_sha256"] == fingerprint(policy_path)[0] and report["targets_sha256"] == fingerprint(targets_path)[0], "build_inputs_changed")
    kinds = {target["sha256"]: kind(target, policy) for target in targets}
    names, _sources = dictionary(targets, kinds, policy)
    matcher = Matcher(cross_field_names(names, policy), policy["replacement"])
    expected = {(entry["key"], entry["version_id"]): entry for entry in report["objects"]}
    require(len(expected) == len(targets) and set(expected) == {(item["key"], item["version_id"]) for item in targets}, "build_scope_changed")
    checked: set[str] = set()
    for target in targets:
        original = verified_original(target)
        item = expected[(target["key"], target["version_id"])]
        require(item["sha256"] == target["sha256"] and item["bytes"] == target["bytes"], "build_original_changed")
        if target["sha256"] in checked:
            continue
        checked.add(target["sha256"])
        mode = kinds[target["sha256"]]
        require(item["format"] == mode, "build_format_changed")
        if item["status"] == "no_personal_data":
            if mode == "metadata":
                require(not (matcher.names and matcher.names.search(normalized(original.read_text(encoding="utf-8")))), "metadata_contains_names")
            elif mode in {"table", "long"}:
                verify_table(original, original, mode, matcher, policy)
            else:
                verify_xml(original, original, mode, matcher, policy)
            continue
        require(item["status"] == "replace", "invalid_build_status")
        copies = list((output / "copies" / target["sha256"]).iterdir())
        require(len(copies) == 1, "copy_folder_not_single_file")
        copy = copies[0]
        if mode in {"table", "long"}:
            verify_table(original, copy, mode, matcher, policy)
        else:
            verify_xml(original, copy, mode, matcher, policy)
        require(fingerprint(copy) == (item["redacted_sha256"], item["redacted_bytes"]), "copy_changed_since_build")
    return {
        "status": "passed",
        "objects": len(targets),
        "copies_verified": report["summary"]["replace"],
        "build_report_sha256": fingerprint(output / "build_report.json")[0],
    }


# Upload ---------------------------------------------------------------------------------------------------------------


def redacted_key(key: str, digest: str, policy: dict[str, Any]) -> str:
    parts = key.split("/")
    require(len(parts) >= 7 and parts[2] == "datasets" and parts[1:].count(parts[-2]) == 1, "unexpected_key_layout")
    original = parts[-2]
    parts[3] = parts[3] + policy["dataset_suffix"]
    parts[-2] = digest
    result = "/".join(parts)
    require(result != key and original not in result.split("/"), "replacement_key_collision")
    return result


def manifest_key(key: str, digest: str) -> str:
    parts = key.split("/")
    require(parts[4].startswith(("capture_id=", "release_date=")), "unexpected_partition")
    return f"{parts[0]}/{parts[1]}/manifests/{parts[4]}/{digest}/redaction_manifest.json"


def upload(client: Any, output: Path, record_path: Path, policy_path: Path) -> dict[str, Any]:
    report = json.loads((output / "build_report.json").read_text(encoding="utf-8"))
    policy = load_policy(policy_path)
    require(report["policy_sha256"] == fingerprint(policy_path)[0], "policy_changed_since_build")
    require(client.call("s3api", "get-bucket-versioning").get("Status") == "Enabled", "versioning_not_enabled")
    replacements: list[dict[str, Any]] = []
    created: Counter[str] = Counter()
    for item in sorted((entry for entry in report["objects"] if entry["status"] == "replace"), key=lambda entry: (entry["key"], entry["version_id"])):
        # One copy per distinct original: identical bytes may be stored under several file names.
        copies = sorted((output / "copies" / item["sha256"]).iterdir())
        require(len(copies) == 1, "copy_folder_not_single_file")
        copy = copies[0]
        require(fingerprint(copy) == (item["redacted_sha256"], item["redacted_bytes"]), "copy_changed_since_build")
        data, made = store_file(client, copy, redacted_key(item["key"], item["redacted_sha256"], policy))
        created["copies"] += made
        manifest = {
            "kind": "redacted_replacement",
            "finding": policy["finding"],
            "policy_sha256": report["policy_sha256"],
            "original": {field: item[field] for field in ("key", "version_id", "sha256")} | {"byte_count": item["bytes"]},
            "redacted": {field: data[field] for field in ("key", "version_id", "sha256", "byte_count")},
            "format": item["format"],
            "counts": item["counts"],
            "verification": item["verification"],
            "replacement_marker": policy["replacement"],
            "kept_as_public": policy["kept_as_public"],
            "limitations": policy["limitations"],
            "original_retention": "The unredacted original is kept locally by the project and is not a shareable copy.",
            "model_eligible": False,
        }
        content = encoded_json(manifest)
        digest = hashlib.sha256(content).hexdigest()
        path = output / "manifests" / f"{digest}.json"
        write_once(path, content)
        stored, made = store_file(client, path, manifest_key(item["key"], digest))
        created["manifests"] += made
        replacements.append({"original": manifest["original"], "redacted": manifest["redacted"], "manifest": stored})
    record = {"kind": "redacted_replacements", "build_report_sha256": fingerprint(output / "build_report.json")[0], "replacements": replacements}
    write_once(record_path, encoded_json(record))
    return {"status": "passed", "replacements": len(replacements), "created": dict(created), "record_sha256": canonical_hash(record)}


def verify_replacements(output: Path, record_path: Path) -> dict[str, Any]:
    """Read back every recorded replacement and manifest by immutable version, without creating S3 objects."""
    record = json.loads(record_path.read_text(encoding="utf-8"))
    report = json.loads((output / "build_report.json").read_text(encoding="utf-8"))
    require(record["build_report_sha256"] == fingerprint(output / "build_report.json")[0], "replacement_build_changed")
    expected = {(entry["key"], entry["version_id"]): entry for entry in report["objects"] if entry["status"] == "replace"}
    identities = [(item["original"]["key"], item["original"]["version_id"]) for item in record["replacements"]]
    require(len(identities) == len(set(identities)) and set(identities) == set(expected), "replacement_scope_changed")
    settings, _ = load_configuration(Path(".env"))
    verify_project_identity(settings)
    client = AwsCli(settings)
    for item in record["replacements"]:
        original, redacted = item["original"], item["redacted"]
        built = expected[(original["key"], original["version_id"])]
        require(original["sha256"] == built["sha256"] and original["byte_count"] == built["bytes"], "replacement_original_changed")
        require(redacted["sha256"] == built["redacted_sha256"] and redacted["byte_count"] == built["redacted_bytes"], "replacement_copy_changed")
        manifest_path = output / "manifests" / f"{item['manifest']['sha256']}.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        require(manifest["original"] == original and manifest["redacted"] == redacted and manifest["model_eligible"] is False, "replacement_manifest_changed")
        require(fingerprint(manifest_path) == (item["manifest"]["sha256"], item["manifest"]["byte_count"]), "replacement_manifest_changed")
        for part in ("redacted", "manifest"):
            obj = item[part]
            verify_version(client, obj["key"], obj["version_id"], obj["sha256"], obj["byte_count"])
    return {
        "status": "passed",
        "copies": len(identities),
        "manifests": len(identities),
        "s3_objects_created": 0,
        "verification": "version_get_sha256_length_checksum_aes256",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("build", "verify"):
        make = commands.add_parser(command, help="build or read-only verify redacted copies")
        make.add_argument("--targets", required=True, type=Path)
        make.add_argument("--policy", required=True, type=Path)
        make.add_argument("--output", required=True, type=Path)
    send = commands.add_parser("upload", help="store verified copies and manifests in S3 (project profile)")
    send.add_argument("--output", required=True, type=Path)
    send.add_argument("--policy", required=True, type=Path)
    send.add_argument("--record", required=True, type=Path)
    live = commands.add_parser("verify-live", help="read back every saved replacement and manifest; no uploads")
    live.add_argument("--output", required=True, type=Path)
    live.add_argument("--record", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.command == "build":
            report = build(args.targets, args.policy, args.output)
            summary = {
                "status": "passed",
                "objects": len(report["objects"]),
                "by_status": report["summary"],
                "dictionary_entries": report["dictionary"]["entries"],
            }
        elif args.command == "verify":
            summary = verify_build(args.targets, args.policy, args.output)
        elif args.command == "verify-live":
            summary = verify_replacements(args.output, args.record)
        else:
            settings, _ = load_configuration(Path(".env"))
            verify_project_identity(settings)
            summary = upload(AwsCli(settings), args.output, args.record, args.policy)
    except (RedactionError, ValueError, OSError, KeyError, TypeError, csv.Error) as error:
        code = str(error) if isinstance(error, RedactionError) else "not_utf8" if isinstance(error, UnicodeDecodeError) else type(error).__name__
        sys.stdout.write(json.dumps({"status": "stopped", "reason": code}) + "\n")
        return 1
    sys.stdout.write(json.dumps(summary) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
