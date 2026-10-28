"""Load the frozen collector derivative policies and verify a stored derivative without its original.

run_derivative_policy_e2e.py exercises both entry points with synthetic inputs. build_derivative_policies.py regenerates the policy and its lock.

Every message names only fixed text, declared column names, policy configuration and counts. verify_derivative lets nothing else out and keeps no
payload in the frames of the error it raises.
"""

import csv
import hashlib
import importlib
import io
import json
import re
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal, InvalidOperation
from functools import cache
from pathlib import Path
from types import MappingProxyType
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from scripts.acquisition import wonder_export_contract as wonder
from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, canonical_hash

CONFIG_DIR = REPO_ROOT / "config/acquisition"
BRONZE_PATH = REPO_ROOT / "config/lakehouse/bronze_tables.json"
POLICY_NAME = "projection_policies.json"
LOCK_NAME = "projection_policies.lock.json"
SCHEMA_NAME = "projection_policies.schema.json"
NULL_MARKER = "\\N"
CONTROL = re.compile(r"[\x00-\x1f\x7f]")
SUM_TOLERANCE = Decimal("0.01")
RATIOS = ("res_ratio", "bus_ratio", "oth_ratio", "tot_ratio")
SHA256 = re.compile(r"[a-f0-9]{64}")
CONTEXT_KEYS = frozenset({"batch_id", "raw_sha256"})
API_FAMILIES = frozenset({"bls_laus", "census_profile", "census_detailed", "hud_crosswalk"})
SCHEMA_KEYWORDS = frozenset(
    {
        "type",
        "properties",
        "required",
        "additionalProperties",
        "patternProperties",
        "items",
        "maxItems",
        "maxLength",
        "pattern",
        "const",
        "enum",
        "minimum",
        "maximum",
    }
)
UNREADABLE = "Derivative verification stopped on an input it could not read"
DEADLINE = "Derivative verification stopped at its deadline"


class DerivativeDeadline(TimeoutError):
    """The verification stopped at its deadline, which says nothing about the derivative."""


class DerivativeRejected(RegistryError):
    """A defect named with fixed text; verify_derivative lets no other message out."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise DerivativeRejected(message)


def _read_json(directory: Path, name: str) -> dict:
    try:
        body = (directory / name).read_bytes()
    except OSError as error:
        raise DerivativeRejected(f"Cannot read {name}: {error}") from error
    try:
        document = json.loads(body)
    except ValueError:
        raise DerivativeRejected(f"{name} is not valid JSON") from None
    require(isinstance(document, dict), f"{name} is not a JSON object")
    return document


@cache
def _pattern(source: str) -> re.Pattern[str]:
    try:
        return re.compile(source)
    except re.error as error:
        raise DerivativeRejected(f"Policy pattern invalid: {source}") from error


def _specs(layout: dict) -> list[dict]:
    return layout["columns"] if "columns" in layout else layout["rules"]


def _locked_entries() -> dict[str, str]:
    entries = _read_json(CONFIG_DIR, LOCK_NAME).get("entries")
    if not isinstance(entries, dict):
        raise DerivativeRejected("Policy lock has no entries")
    return entries


def _require_locked(policy: dict) -> None:
    require(_locked_entries().get(policy.get("id", "")) == canonical_hash(policy), "Policy entry differs from the lock")


def _plan(path: str, sha: str) -> dict:
    target = REPO_ROOT / path
    body = _read_json(target.parent, target.name)
    require(canonical_hash(body) == sha, f"Policy plan hash differs: {path}")
    require(_read_json(target.parent, target.with_suffix(".lock.json").name).get("plan_sha256") == sha, f"Policy plan lock differs: {path}")
    return body


def _decision(path: str, sha: str) -> dict:
    target = REPO_ROOT / path
    body = _read_json(target.parent, target.name)
    require(canonical_hash(body) == sha, f"Policy decision hash differs: {path}")
    require(_read_json(target.parent, target.with_suffix(".lock.json").name).get("decision_sha256") == sha, f"Policy decision lock differs: {path}")
    return body


def _planned(entry: dict) -> list[dict]:
    return [batch for plan in entry["extraction_map"]["plans"] for batch in _plan(plan["path"], plan["plan_sha256"])["batches"]]


def _bound(batch: dict) -> dict:
    return {
        "id": batch["id"],
        "table": batch["table"],
        "headers_sha256": canonical_hash(batch["headers"]),
        **({"year": batch["year"]} if "year" in batch else {}),
    }


def _check_closed(node: Any) -> None:
    require(isinstance(node, dict) and set(node) <= SCHEMA_KEYWORDS, "Policy evidence schema uses an unsupported keyword")
    types = node.get("type", [])
    types = [types] if isinstance(types, str) else types
    if "const" in node or "enum" in node:
        require(all(not isinstance(v, dict | list) for v in [node.get("const"), *node.get("enum", [])]), "Policy evidence schema literal is not a scalar")
        return
    require(bool(types), "Policy evidence schema node has no type")
    if "object" in types:
        properties, patterns = node.get("properties", {}), node.get("patternProperties", {})
        require(node.get("additionalProperties") is False and bool(properties) != bool(patterns), "Policy evidence schema object is open")
        require(set(node.get("required", [])) <= set(properties), "Policy evidence schema requires an undeclared property")
        require(all(p.startswith("^") and p.endswith("$") for p in patterns), "Policy evidence schema pattern is not anchored")
        for child in [*properties.values(), *patterns.values()]:
            _check_closed(child)
    if "array" in types:
        require(isinstance(node.get("maxItems"), int) and "items" in node, "Policy evidence schema array is unbounded")
        _check_closed(node["items"])
    if "string" in types:
        require(isinstance(node.get("maxLength"), int), "Policy evidence schema string is unbounded")
    if "integer" in types:
        require(isinstance(node.get("maximum"), int), "Policy evidence schema integer is unbounded")


def _check_evidence_schema(entry: dict) -> None:
    evidence = entry["evidence"]
    schema = evidence["schema"]
    _check_closed(schema)
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError:
        raise DerivativeRejected("Policy evidence schema is invalid") from None
    properties = set(schema.get("properties", {}))
    require(
        schema["type"] == "object" and set(schema["required"]) == set(evidence["required"]), "Policy evidence schema required differs from the evidence keys"
    )
    require(
        {evidence["raw_sha256_field"], evidence["derivative_sha256_field"], "statistics"} <= properties,
        "Policy evidence schema lacks a hash or statistics property",
    )
    require(not properties & set(evidence["forbidden"]), "Policy evidence schema declares a forbidden key")
    api = entry["family_check"] in API_FAMILIES
    require(api == ("request_function" in evidence), "Policy request function belongs to the API recipes only")
    require(not api or "request" in properties, "Policy evidence schema lacks the request")
    if api:
        function = getattr(importlib.import_module(entry["parser"]["module"]), evidence["request_function"], None)
        require(callable(function), "Policy request function missing")


def _check_plans(entry: dict) -> list[dict]:
    shas = set()
    for plan in entry["extraction_map"]["plans"]:
        _plan(plan["path"], plan["plan_sha256"])
        shas.add(plan["plan_sha256"])
    decision = entry["extraction_map"].get("decision")
    if decision is not None:
        require(_decision(decision["path"], decision["decision_sha256"])["plan_sha256"] in shas, "Policy decision binds a plan the entry does not")
    return _planned(entry)


def _check_layout(layout: dict, table: dict, planned: list[dict]) -> None:
    specs = _specs(layout)
    for spec in specs:
        domain = spec["domain"]
        for source in (spec.get("name_pattern"), domain.get("pattern")):
            if source is not None:
                _pattern(source)
        require("*" not in spec.get("input", "") + spec.get("output", ""), "Policy selector is a wildcard")
    if "columns" in layout:
        outputs = [c["output"] for c in layout["columns"]]
        require(len(outputs) == len(set(outputs)), f"Policy duplicate output column in {layout['name']}")
        require(set(table["key"]) | set(table["order"]) <= set(outputs), f"Policy key or order column missing in {layout['name']}")
    else:
        names = [r["name"] for r in layout["rules"]]
        require(len(names) == len(set(names)), f"Policy duplicate rule name in {layout['name']}")
        require(len({b["id"] for b in layout["bound_batches"]}) == len(layout["bound_batches"]), f"Policy duplicate bound batch in {layout['name']}")
        require(layout["bound_batches"] == [_bound(b) for b in planned], f"Policy bound batches differ from the bound plans in {layout['name']}")
        for batch in planned:
            column_rules(layout, batch["headers"])
        for column in {*table["key"], *table["order"]}:
            require(any(_pattern(r["name_pattern"]).fullmatch(column) for r in layout["rules"]), f"Policy key or order column {column} matches no rule")


def _check_entry(entry: dict, bronze: dict[str, dict]) -> None:
    planned = _check_plans(entry)
    tables = [f for f in entry["files"] if f["role"] == "table"]
    require(len(tables) == 1, f"Policy {entry['id']} needs exactly one table file")
    require(len({f["path"] for f in entry["files"]}) == len(entry["files"]), f"Policy duplicate file path in {entry['id']}")
    table = tables[0]
    require(all(k in table for k in ("layouts", "key", "order", "max_rows")), f"Policy table file lacks a layout, key, order or row limit in {entry['id']}")
    for layout in table["layouts"]:
        _check_layout(layout, table, planned)
    parser = entry["parser"]
    function = getattr(importlib.import_module(parser["module"]), parser["function"], None)
    require(callable(function), f"Policy parser missing: {parser['module']}.{parser['function']}")
    consumers = entry["consumers"]
    for path in consumers["paths"]:
        require((REPO_ROOT / path).is_file(), f"Policy consumer file missing: {path}")
    row = bronze.get(consumers["bronze_table"])
    if row is None or row["format"] != "csv":
        raise DerivativeRejected(f"Policy bronze table missing or not csv: {consumers['bronze_table']}")
    require(re.search(row["member_pattern"], Path(table["path"]).name) is not None, f"Policy bronze member pattern does not match {table['path']}")
    for name in consumers["dropped_bronze_tables"]:
        require(name in bronze, f"Policy bronze table missing: {name}")
    evidence = entry["evidence"]
    require(
        {evidence["raw_sha256_field"], evidence["derivative_sha256_field"], "statistics"} <= set(evidence["required"]),
        "Policy evidence lacks its hash or statistics keys",
    )
    _check_evidence_schema(entry)
    require(entry["family_check"] in FAMILIES, "Policy family check unknown")
    require(("query" in entry) == (entry["family_check"] == "wonder_county_year"), "Policy query belongs to the WONDER recipe only")


def load_policies(directory: Path = CONFIG_DIR, bronze_path: Path = BRONZE_PATH) -> dict[str, dict]:
    """Return the validated policy entries by id; any drift from the schema, lock, parsers or consumers stops the load."""
    document, lock, schema = (_read_json(directory, n) for n in (POLICY_NAME, LOCK_NAME, SCHEMA_NAME))
    first = next(iter(Draft202012Validator(schema).iter_errors(document)), None)
    require(first is None, f"Policy schema violation at {list(first.absolute_path)[:6]}: {first.validator}" if first else "")
    ids = [e["id"] for e in document["entries"]]
    require(len(ids) == len(set(ids)), "Policy duplicate entry id")
    require(set(lock) == {"policies_sha256", "entries"} and lock["policies_sha256"] == canonical_hash(document), "Policy lock differs from the policy file")
    require(lock["entries"] == {e["id"]: canonical_hash(e) for e in document["entries"]}, "Policy lock differs for an entry")
    bronze = {t["table"]: t for t in _read_json(bronze_path.parent, bronze_path.name)["tables"]}
    for entry in document["entries"]:
        _check_entry(entry, bronze)
    return {e["id"]: e for e in document["entries"]}


def column_rules(layout: dict, headers: list[str]) -> list[dict]:
    """Return the rule that types each locked header; a header no rule covers stops the policy."""
    if "columns" in layout:
        return layout["columns"]
    rules = []
    for header in headers:
        rule = next((r for r in layout["rules"] if _pattern(r["name_pattern"]).fullmatch(header)), None)
        if rule is None:
            raise DerivativeRejected(f"Policy header {header} matches no rule")
        rules.append(rule)
    return rules


def _footnote_problem(value: str) -> str | None:
    try:
        notes = json.loads(value)
    except ValueError:
        return "not JSON"
    valid = isinstance(notes, list) and all(isinstance(n, dict) and set(n) <= {"code", "text"} and all(isinstance(v, str) for v in n.values()) for n in notes)
    if not valid or json.dumps(notes, sort_keys=True, separators=(",", ":")) != value:
        return "not canonical footnotes"
    return None


def _checker(spec: dict) -> Callable[[str], str | None]:
    token, domain = spec["null_token"], spec["domain"]
    kind = domain["kind"]

    def body(value: str) -> str | None:
        if kind == "enum":
            return None if value in domain["values"] else "not in the allowed values"
        if kind == "text":
            if value == "" and not domain["allow_empty"]:
                return "empty"
            return "too long or has control characters" if len(value) > domain["max_length"] or CONTROL.search(value) else None
        if kind == "footnotes":
            return _footnote_problem(value)
        if value in domain.get("tokens", ()):
            return None
        if not _pattern(domain["pattern"]).fullmatch(value):
            return "does not match its pattern"
        if "range" in domain:
            try:
                number = Decimal(value)
            except InvalidOperation:
                return "not a number"
            if not domain["range"][0] <= number <= domain["range"][1]:
                return "outside its range"
        if "refuse_range" in domain and value.isdigit() and domain["refuse_range"][0] <= int(value) <= domain["refuse_range"][1]:
            return f"refused range {domain['refuse_range'][0]}-{domain['refuse_range'][1]}"
        return None

    def check(value: str) -> str | None:
        if value == NULL_MARKER:
            return None if token == NULL_MARKER else "null token not permitted"
        return body(value)

    return check


def cell_problem(spec: dict, value: str) -> str | None:
    """Return why a cell breaks its selector's type and domain, or None when it is allowed."""
    return _checker(spec)(value)


def _select_layout(table: dict, header: list[str], context: dict) -> dict:
    for candidate in table["layouts"]:
        if "columns" in candidate:
            if header == [c["output"] for c in candidate["columns"]]:
                return candidate
            continue
        batch = context["batch"]
        bound = next((b for b in candidate["bound_batches"] if b["id"] == batch["id"]), None)
        if bound is None:
            raise DerivativeRejected("Bound plan batch is not bound by the policy")
        require(canonical_hash(batch["headers"]) == bound["headers_sha256"], "Bound plan headers hash differs from the bound batch")
        if header == batch["headers"]:
            return candidate
    raise DerivativeRejected("Derivative header differs from the policy layout")


def _parse_csv(body: bytes) -> tuple[str, list[list[str]]] | None:
    try:
        text = body.decode("utf-8")
        return text, list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error):
        return None


def _read_table(table: dict, body: bytes, context: dict) -> tuple[dict, list[str], list[list[str]]]:
    parsed = _parse_csv(body)
    require(parsed is not None, "Derivative serialization is not UTF-8 CSV")
    text, rows = parsed or ("", [])
    canonical = io.StringIO(newline="")
    csv.writer(canonical, lineterminator="\n").writerows(rows)
    clean = text.endswith("\n") and "\r" not in text and not text.startswith("﻿") and "\n\n" not in text and canonical.getvalue() == text
    require(clean, "Derivative serialization is not canonical UTF-8 CSV with newline endings")
    require(len(rows) >= 2, "Derivative has no data rows")
    header, data = rows[0], rows[1:]
    layout = _select_layout(table, header, context)
    require(table.get("min_rows", 1) <= len(data) <= table["max_rows"], f"Derivative row limit not met: {len(data)} rows")
    checks = [(spec["output"] if "columns" in layout else header[i], _checker(spec)) for i, spec in enumerate(column_rules(layout, header))]
    short = sum(len(row) != len(header) for row in data)
    require(short == 0, f"Derivative row width differs in {short} rows")
    rejected: Counter[tuple[str, str]] = Counter()
    for row in data:
        for (name, check), value in zip(checks, row, strict=True):
            problem = check(value)
            if problem is not None:
                rejected[(name, problem)] += 1
    if rejected:
        (name, problem), count = next(iter(rejected.items()))
        raise DerivativeRejected(f"Derivative column {name} has {count} rejected cells: {problem}")
    position = {name: i for i, name in enumerate(header)}
    keys = [tuple(r[position[c]] for c in table["key"]) for r in data]
    require(len(set(keys)) == len(keys), "Derivative duplicate key")
    order = [tuple(r[position[c]] for c in table["order"]) for r in data]
    require(all(a < b for a, b in zip(order, order[1:], strict=False)), "Derivative row order differs from the policy order")
    return layout, header, data


def _schema_where(schema: dict, path: Iterable[Any]) -> str:
    """Name an evidence location by the schema's declared properties; a key matched by a pattern prints as *."""
    names: list[str] = []
    node: dict = schema
    for part in path:
        if isinstance(part, int):
            names.append("[]")
            node = node.get("items", {})
        elif part in node.get("properties", {}):
            names.append(part)
            node = node["properties"][part]
        else:
            names.append("*")
            node = next(iter(node.get("patternProperties", {}).values()), {})
    return "/".join(names) or "root"


def _check_evidence(policy: dict, files: Mapping[str, bytes], evidence: dict, context: dict) -> None:
    spec = policy["evidence"]
    require(isinstance(evidence, dict), "Derivative evidence is not an object")
    for key in spec["forbidden"]:
        require(key not in evidence, f"Derivative evidence carries the forbidden key {key}")
    for key in spec["required"]:
        require(key in evidence, f"Derivative evidence lacks the key {key}")
    first = min(Draft202012Validator(spec["schema"]).iter_errors(evidence), key=lambda e: ([str(p) for p in e.absolute_path], e.validator), default=None)
    if first is not None:
        where = _schema_where(spec["schema"], first.absolute_path)
        raise DerivativeRejected(f"Derivative evidence schema violation at {where}: {first.validator}")
    require(
        isinstance(evidence["statistics"], dict) and evidence["statistics"].get("model_eligible", False) is False,
        "Evidence statistics model_eligible is not false",
    )
    require(SHA256.fullmatch(str(context.get("raw_sha256", ""))) is not None, "Expected raw hash is not a SHA-256")
    if "sha256" in context["batch"]:
        require(context["raw_sha256"] == context["batch"]["sha256"], "Expected raw hash differs from the bound batch")
    if "bytes" in spec["required"] and "bytes" in context["batch"]:
        require(evidence["bytes"] == context["batch"]["bytes"], "Evidence bytes differ from the bound batch")
    require(evidence[spec["raw_sha256_field"]] == context["raw_sha256"], "Pinned raw hash in the evidence differs from the expected raw hash")
    if spec["raw_sha256_field"] != "sha256" and "sha256" in evidence:
        require(evidence["sha256"] == context["raw_sha256"], "Evidence sha256 differs from the expected raw hash")
    if "request_function" in spec:
        parser = importlib.import_module(policy["parser"]["module"])
        require(evidence["request"] == getattr(parser, spec["request_function"])(context["batch"]), "Evidence request differs from the bound batch")
        route = policy["source"]["route"].replace("{year}", str(context["batch"].get("year", "")))
        require(evidence["endpoint"] == route, "Evidence endpoint differs from the bound route")
    table = next(f for f in policy["files"] if f["role"] == "table")
    require(hashlib.sha256(files[table["path"]]).hexdigest() == evidence[spec["derivative_sha256_field"]], "Stored derivative hash differs from the evidence")
    if "notes_sha256" in spec["required"]:
        notes = next(f for f in policy["files"] if f["role"] == "notes")
        require(hashlib.sha256(files[notes["path"]]).hexdigest() == evidence["notes_sha256"], "Stored notes hash differs from the evidence")
    if "origin_urls" in spec:
        database = context["batch"].get("database", "")
        require(evidence["origin_urls"] == [u.replace("{database}", database) for u in spec["origin_urls"]], "Evidence origin URLs differ from the policy")
    if "file_name" in spec["required"]:
        require(evidence["file_name"] == context["batch"]["file_name"], "Evidence file name differs from the batch")


def _bls(policy: dict, header: list[str], rows: list[list[str]], evidence: dict, context: dict, files: Mapping[str, bytes]) -> tuple[dict, set[str]]:
    batch, at = context["batch"], {name: i for i, name in enumerate(header)}
    sids, first, last = set(batch["series_ids"]), batch["start_year"], batch["end_year"]
    periods: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    missing = footnoted = 0
    for row in rows:
        sid, year, value = row[at["seriesID"]], row[at["year"]], row[at["value"]]
        require(sid in sids, "BLS series outside the batch")
        require(row[at["county_fips"]] == sid[5:10] and row[at["measure_code"]] == sid[-2:], "BLS county_fips or measure_code differs from the series id")
        require(first <= int(year) <= last, "BLS period grid has a year outside the batch window")
        notes = any(json.loads(row[at["footnotes"]]))
        require(value != "-" or notes, "BLS missing token '-' lacks a footnote")
        missing += value == "-"
        footnoted += notes
        periods[sid][year].add(row[at["period"]])
    holds = []
    incomplete = 0
    for sid in sorted(sids):
        if sid not in periods:
            holds.append({"series_id": sid, "status": "availability_hold", "message": f"Series does not exist for Series {sid}"})
            continue
        for calendar in range(first, last + 1):
            if str(calendar) not in periods[sid]:
                holds.append(
                    {"series_id": sid, "year": calendar, "status": "availability_hold", "message": f"No Data Available for Series {sid} Year: {calendar}"}
                )
            else:
                incomplete += len(periods[sid][str(calendar)]) != 13
    require(incomplete == 0, f"BLS period grid is incomplete in {incomplete} series-years")
    recorded = evidence["statistics"].get("availability_holds")
    require(
        isinstance(recorded, list) and sorted(map(json.dumps, recorded)) == sorted(map(json.dumps, holds)),
        "BLS availability hold evidence differs from the derivative",
    )
    expected = {
        "rows": len(rows),
        "requested_series": len(sids),
        "available_series": len(periods),
        "missing_value_rows": missing,
        "footnoted_rows": footnoted,
    }
    return expected, {"availability_holds", "model_eligible"}


def _census(policy: dict, header: list[str], rows: list[list[str]], evidence: dict, context: dict, files: Mapping[str, bytes]) -> tuple[dict, set[str]]:
    at = {name: i for i, name in enumerate(header)}
    identities = set()
    nulls = negative = annotations = 0
    notes = [i for i, name in enumerate(header) if name.endswith("A")]
    for row in rows:
        identity = row[at["state"]] + row[at["county"]]
        require(row[at["GEO_ID"]] == f"0500000US{identity}", "Census GEO_ID differs from state and county")
        identities.add(identity)
        nulls += row.count(NULL_MARKER)
        negative += sum(v.startswith("-") for v in row)
        annotations += sum(row[i] not in (NULL_MARKER, "") for i in notes)
    expected = {"rows": len(rows), "columns": len(header), "puerto_rico_counties": sum(i.startswith("72") for i in identities)}
    if policy["family_check"] == "census_profile":
        require(identities == set(context["counties"]), "Census county coverage differs from the pinned set")
        expected |= {"null_cells": nulls, "negative_tokens": negative, "nonempty_annotations": annotations}
    return expected, {"model_eligible"}


def _hud(policy: dict, header: list[str], rows: list[list[str]], evidence: dict, context: dict, files: Mapping[str, bytes]) -> tuple[dict, set[str]]:
    at = {name: i for i, name in enumerate(header)}
    sums: dict[str, dict[str, Decimal]] = {}
    states, zips, counties = set(), set(), set()
    for row in rows:
        zip_code, geoid = row[at["zip"]], row[at["geoid"]]
        states.add(geoid[:2])
        zips.add(zip_code)
        counties.update([geoid] if len(geoid) == 5 else [])
        totals = sums.setdefault(zip_code, dict.fromkeys(RATIOS, Decimal(0)))
        for name in RATIOS:
            totals[name] += Decimal(row[at[name]])
    require(set(context["required_state_fips"]) <= states, "HUD state coverage incomplete")
    profile = {}
    for name in RATIOS:
        values = [t[name] for t in sums.values()]
        profile[name] = {
            "zips_summing_to_one": sum(abs(v - 1) <= SUM_TOLERANCE for v in values),
            "zips_with_zero_weight": sum(v == 0 for v in values),
            "zips_outside_tolerance": sum(v != 0 and abs(v - 1) > SUM_TOLERANCE for v in values),
        }
    expected = {
        "rows": len(rows),
        "zips": len(zips),
        "counties": len(counties),
        "states_and_territories": sorted(states),
        "ratio_sums": profile,
        "direction": "zip_to_county",
    }
    extras = {"model_eligible"}
    if policy["family_check"] == "hud_workbook":
        approved = context.get("exact_repeat_rows")
        recorded = evidence["statistics"].get("exact_repeat_rows_dropped")
        require(recorded == approved, "Evidence exact repeat count differs from the approved decision")
        extras.add("exact_repeat_rows_dropped")
    return expected, extras


def _wonder(policy: dict, header: list[str], rows: list[list[str]], evidence: dict, context: dict, files: Mapping[str, bytes]) -> tuple[dict, set[str]]:
    batch, at = context["batch"], {name: i for i, name in enumerate(header)}
    planned = list(range(batch["years"][0], batch["years"][1] + 1))
    layout = next(f for f in policy["files"] if f["role"] == "table")["layouts"][0]
    flagged = [s["output"] for s in layout["columns"] if s["domain"]["kind"] == "pattern" and "tokens" in s["domain"]]
    seen: dict[str, set[str]] = {}
    flags: dict[str, Counter[str]] = {name: Counter() for name in flagged}
    for row in rows:
        year = row[at["Year Code"]]
        require(row[at["Year"]].strip() == year, "WONDER Year differs from Year Code")
        seen.setdefault(year, set()).add(row[at["County Code"]])
        for name in flagged:
            if not re.fullmatch(r"[0-9.]+", row[at[name]]):
                flags[name][row[at[name]]] += 1
    require(sorted(int(y) for y in seen) == planned, "WONDER years differ from the plan")
    per_year = {year: len(codes) for year, codes in sorted(seen.items())}
    require(min(per_year.values()) >= context["min_counties_per_year"], "WONDER county floor not met")
    require(all(set(context["required_state_fips"]) <= {c[:2] for c in codes} for codes in seen.values()), "WONDER state coverage incomplete")
    record = _query(policy, files[next(f for f in policy["files"] if f["role"] == "notes")["path"]], batch, planned)
    require(evidence["query"] == record, "WONDER query in the evidence differs from the notes")
    expected = {"rows": len(rows), "years": planned, "counties_per_year": per_year, "flags": {c: dict(sorted(f.items())) for c, f in flags.items() if f}}
    return expected, {"query", "model_eligible"}


def _utf8(body: bytes) -> str | None:
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _query(policy: dict, notes: bytes, batch: dict, planned: list[int]) -> dict:
    text = _utf8(notes)
    require(text is not None, "WONDER query notes are not UTF-8")
    text = text or ""
    try:
        record = wonder.query_record(text.splitlines())
    except (ValueError, IndexError):
        raise DerivativeRejected("WONDER query record is unreadable") from None
    require("\r" not in text and text.endswith("\n"), "WONDER query notes are not newline-terminated text")
    query = policy["query"]
    require(record["dataset"] == query["databases"].get(batch["database"]), "WONDER query dataset differs from the policy")
    parameters = dict(record["parameters"])
    if "Year/Month" in parameters:
        try:
            listed = wonder.listed_years(parameters.pop("Year/Month"))
        except ValueError:
            raise DerivativeRejected("WONDER query years are unreadable") from None
        require(listed == planned, "WONDER query years differ from the plan")
    require(parameters == query["fixed_parameters"], "WONDER query parameters differ from the policy")
    require(record["query_date"] is not None, "WONDER query date missing")
    return record


FAMILIES = MappingProxyType(
    {
        "bls_laus": _bls,
        "census_profile": _census,
        "census_detailed": _census,
        "hud_crosswalk": _hud,
        "hud_workbook": _hud,
        "wonder_county_year": _wonder,
    }
)


def _approved_repeats(policy: dict, batch: dict) -> int | None:
    decision = policy["extraction_map"].get("decision")
    if decision is None:
        return None
    quarters = [q for q in _decision(decision["path"], decision["decision_sha256"])["quarters"] if q["batch_id"] == batch["id"]]
    require(
        all((q["sha256"], q["year"], q["quarter"]) == (batch["sha256"], batch["year"], batch["quarter"]) for q in quarters),
        "Decision differs from the bound batch",
    )
    return quarters[0]["exact_repeat_rows"] if quarters else None


def frozen_inputs(policy: dict, batch_id: str) -> dict:
    """Resolve the bound plan batch and the family's coverage inputs from the plans and decision, read and hash-checked on this call.

    The policy must equal its lock entry. The result is new on every call, so changing it changes no later check.
    """
    _require_locked(policy)
    return _frozen(policy, batch_id)


def _frozen(policy: dict, batch_id: str) -> dict:
    found = [
        (plan, batch)
        for entry in policy["extraction_map"]["plans"]
        for plan in [_plan(entry["path"], entry["plan_sha256"])]
        for batch in plan["batches"]
        if batch["id"] == batch_id
    ]
    require(len(found) == 1, "Context batch is not in the bound plans")
    plan, batch = found[0]
    family = policy["family_check"]
    frozen: dict = {"batch": batch}
    if family == "census_profile":
        frozen["counties"] = plan["county_ids"]
    if family in ("hud_crosswalk", "hud_workbook", "wonder_county_year"):
        frozen["required_state_fips"] = plan["required_state_fips"]
    if family == "wonder_county_year":
        frozen["min_counties_per_year"] = plan["min_counties_per_year"]
    if family == "hud_workbook":
        frozen["exact_repeat_rows"] = _approved_repeats(policy, batch)
    return frozen


def verify_derivative(policy: dict, files: Mapping[str, bytes], evidence: dict, context: dict) -> dict:
    """Check a stored derivative against its frozen policy and evidence, using no original.

    ``files`` maps each policy file path to its stored bytes. ``context`` holds exactly ``batch_id`` and ``raw_sha256``, the pinned raw
    SHA-256. The batch and coverage inputs come from the bound plans. Returns the row accounting. On the first defect it raises
    DerivativeRejected with fixed text; a stop at the deadline raises DerivativeDeadline.
    The error has no cause or context, and its frames hold no row, cell, evidence or file.
    """
    try:
        return _verify(policy, files, evidence, context)
    except DerivativeRejected as rejection:
        message = str(rejection)
    except TimeoutError:
        message = DEADLINE
    except Exception:
        message = UNREADABLE
    del policy, files, evidence, context
    if message == DEADLINE:
        raise DerivativeDeadline(DEADLINE)
    raise DerivativeRejected(message)


def _verify(policy: dict, files: Mapping[str, bytes], evidence: dict, context: dict) -> dict:
    _require_locked(policy)
    require(
        isinstance(context, dict) and set(context) == CONTEXT_KEYS and isinstance(context["batch_id"], str), "Context keys differ from batch_id and raw_sha256"
    )
    context = {**_frozen(policy, context["batch_id"]), "raw_sha256": context["raw_sha256"]}
    require(set(files) == {f["path"] for f in policy["files"]}, "Derivative file set differs from the policy")
    for spec in policy["files"]:
        require(len(files[spec["path"]]) <= spec["max_bytes"], f"Derivative {spec['path']} exceeds its size limit")
    _check_evidence(policy, files, evidence, context)
    table = next(f for f in policy["files"] if f["role"] == "table")
    layout, header, rows = _read_table(table, files[table["path"]], context)
    expected, extras = FAMILIES[policy["family_check"]](policy, header, rows, evidence, context, files)
    statistics = evidence["statistics"]
    require(set(expected) <= set(statistics) <= set(expected) | extras, "Evidence statistics differ from the derivative: key set")
    for key, value in expected.items():
        require(statistics[key] == value, f"Evidence statistics differ from the derivative: {key}")
    return {
        "policy": policy["id"],
        "layout": layout["name"],
        "rows": len(rows),
        "columns": len(header),
        "derivative_sha256": evidence[policy["evidence"]["derivative_sha256_field"]],
        "raw_sha256": context["raw_sha256"],
        "statistics": expected,
    }
