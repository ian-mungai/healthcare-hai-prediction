"""Repeatable synthetic verification of the collector derivative policies and the derivative-only verifier, without real data, credentials or any original."""

import argparse
import ast
import contextlib
import copy
import csv
import hashlib
import importlib
import io
import json
import logging
import re
import sys
import tempfile
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_contract as bls
from scripts.acquisition import census_acs_api_contract as census_profile
from scripts.acquisition import census_acs_detailed_contract as census_detailed
from scripts.acquisition import derivative_policy as module
from scripts.acquisition import hud_api_contract as hud
from scripts.acquisition import hud_xlsx_contract as hud_workbook
from scripts.acquisition import wonder_export_contract as wonder
from scripts.acquisition.run_hud_xlsx_e2e import rows as workbook_rows
from scripts.acquisition.run_hud_xlsx_e2e import workbook
from scripts.acquisition.run_wonder_export_e2e import export_text
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json, require

CONFIG = REPO_ROOT / "config/acquisition"
NULL = "\\N"
CANARY = "ZQ7K-CANARY-4417"
Mutation = Callable[[dict], None]
CHANGED_BATCH = {"id": "0" * 64, "table": "UNPLANNED", "year": 1900, "headers": []}


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def parse(body: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(body.decode(), newline="")))


def emit(rows: list[list[str]]) -> bytes:
    out = io.StringIO(newline="")
    csv.writer(out, lineterminator="\n").writerows(rows)
    return out.getvalue().encode()


def seal(case: dict) -> None:
    """Re-bind the evidence to the case's current derivative so only the planted defect can fail."""
    policy = case["policy"]
    table = next(f for f in policy["files"] if f["role"] == "table")["path"]
    case["evidence"][policy["evidence"]["derivative_sha256_field"]] = sha(case["files"][table])
    if "notes_sha256" in case["evidence"]:
        case["evidence"]["notes_sha256"] = sha(case["files"]["derived/export_notes.txt"])


def make_case(recipe_id: str, policies: dict, raw: bytes, derived: bytes, statistics: dict, batch: dict, extra: dict, files: dict | None = None) -> dict:
    policy = policies[recipe_id]
    table = next(f for f in policy["files"] if f["role"] == "table")["path"]
    pinned = batch.get("sha256") or sha(raw)
    evidence = {policy["evidence"]["raw_sha256_field"]: pinned, "statistics": statistics, **extra}
    context = {"batch_id": batch["id"], "raw_sha256": pinned}
    case = {"policy": policy, "files": {table: derived, **(files or {})}, "evidence": evidence, "context": context, "table": table, "raw": raw}
    seal(case)
    return case


def api_evidence(batch: dict, raw: bytes, route: str, request: dict) -> dict:
    return {
        "endpoint": route.replace("{year}", str(batch.get("year", ""))),
        "request": request,
        "retrieved_at_utc": "2026-01-01T00:00:00+00:00",
        "sha256": sha(raw),
        "bytes": len(raw),
        "http_status": 200,
    }


def plan_batch(name: str, **match: Any) -> dict:
    return next(b for b in read_json(CONFIG / f"{name}.json")["batches"] if all(b.get(k) == v for k, v in match.items()))


def edit_rows(case: dict, change: Callable[[list[list[str]]], None], keep_hash: bool = False) -> None:
    rows = parse(case["files"][case["table"]])
    change(rows)
    case["files"][case["table"]] = emit(rows)
    if not keep_hash:
        seal(case)


BLS_PLAN = read_json(CONFIG / "bls_api_plan.json")
BLS_BATCH = min(BLS_PLAN["batches"], key=lambda b: (b["end_year"] - b["start_year"], len(b["series_ids"])))
BLS_SERIES = BLS_BATCH["series_ids"]
BLS_FIRST, BLS_LAST = BLS_BATCH["start_year"], BLS_BATCH["end_year"]


def bls_case(policies: dict) -> dict:
    batch = BLS_BATCH
    absent_series, short_series = BLS_SERIES[0], BLS_SERIES[1]
    series, messages = [], [f"Series does not exist for Series {absent_series}", f"No Data Available for Series {short_series} Year: {BLS_FIRST + 5}"]
    for sid in BLS_SERIES:
        data: list[dict[str, Any]] = [
            {
                "year": str(y),
                "period": f"M{m:02}",
                "periodName": "Synthetic period",
                "value": "-" if m == 10 else f"{y - 1980}.{m}",
                "footnotes": [{"code": "X", "text": "Synthetic missing value"}] if m == 10 else [{}],
            }
            for y in range(BLS_FIRST, BLS_LAST + 1)
            for m in range(1, 14)
            if sid != absent_series and not (sid == short_series and y == BLS_FIRST + 5)
        ]
        series.append({"seriesID": sid, "data": data})
    raw = encoded_json({"status": "REQUEST_SUCCEEDED", "message": messages, "Results": {"series": series}})
    derived, statistics = bls.validate_response(raw, batch)
    extra = api_evidence(batch, raw, BLS_PLAN["endpoint"], bls.request_for(batch))
    return make_case("r1_bls_laus_api", policies, raw, derived, statistics, batch, extra)


def census_rows(headers: list[str], counties: list[str]) -> bytes:
    body = []
    for n, identity in enumerate(counties):
        row: dict[str, str | None] = {}
        for h in headers:
            if h == "GEO_ID":
                row[h] = f"0500000US{identity}"
            elif h == "NAME":
                row[h] = f"Synthetic County {identity}"
            elif h in ("state", "county"):
                row[h] = identity[:2] if h == "state" else identity[2:]
            elif h.endswith(("EA", "MA")):
                row[h] = "*****" if n == 0 else None
            elif h.endswith(("E", "M")):
                row[h] = None if n == 1 and h.endswith("_0001E") else ("-555555555" if n == 0 else str(n + 10))
            else:
                row[h] = ""
        body.append([row[h] for h in reversed(headers)])
    return json.dumps([list(reversed(headers)), *body]).encode()


def profile_case(policies: dict) -> dict:
    plan = read_json(CONFIG / "census_acs_api_plan_dp02pr.json")
    batch, counties = plan["batches"][0], plan["county_ids"]
    raw = census_rows(batch["headers"], counties)
    derived, statistics = census_profile.validate_response(raw, batch, counties)
    extra = api_evidence(batch, raw, plan["endpoint"], census_profile.request_for(batch)) | {"response_sha256": sha(raw)}
    return make_case("r2_census_acs_profile_api", policies, raw, derived, statistics, batch, extra)


def county_universe(count: int) -> list[str]:
    return [f"{state:02d}{n:03d}" for state in [*range(1, 57), 72] for n in range(1, 61)][:count]


def detailed_case(policies: dict) -> dict:
    batch = read_json(CONFIG / "census_acs_detailed_plan.json")["batches"][0]
    counties = county_universe(3101)
    raw = census_rows(batch["headers"], counties)
    derived, statistics = census_detailed.validate_response(raw, batch)
    extra = api_evidence(batch, raw, policies["r3_census_acs_detailed_api"]["source"]["route"], census_detailed.request_for(batch))
    return make_case("r3_census_acs_detailed_api", policies, raw, derived, statistics, batch, extra)


def hud_results(fields: list[str]) -> list[dict]:
    out = []
    for i, state in enumerate(hud.STATE_FIPS, 1):
        for geoid, ratio in ((f"{state}001", 0.25), (f"{state}003", 0.75)):
            out.append({"zip": f"{i:05d}", "geoid": geoid, "res_ratio": ratio, "bus_ratio": ratio, "oth_ratio": ratio, "tot_ratio": ratio})
    out.append({"zip": "96799", "geoid": "60", "res_ratio": 0, "bus_ratio": 0, "oth_ratio": 0, "tot_ratio": 0})
    out.append({"zip": "00001", "geoid": "99999", "res_ratio": 7.2e-5, "bus_ratio": 0, "oth_ratio": 1, "tot_ratio": 7.2e-5})
    for row in out:
        row.update({"city": "Synthetic", "state": "SY"} if "city" in fields else {})
    return out


def hud_case(policies: dict, fields: list[str]) -> dict:
    plan = read_json(CONFIG / "hud_api_plan_2021q2_2025q4.json") | {"result_fields": fields}
    batch = plan_batch("hud_api_plan_2021q2_2025q4", year=2022, quarter=1)
    data = {"year": "2022", "quarter": "1", "input": "All", "crosswalk_type": "zip-county", "results": hud_results(fields)}
    raw = json.dumps({"data": data}).encode()
    derived, statistics = hud.validate_response(raw, batch, plan)
    extra = api_evidence(batch, raw, plan["endpoint"], hud.request_for(batch))
    return make_case("r4_hud_usps_crosswalk_api", policies, raw, derived, statistics, batch, extra)


XLSX_PLAN = "hud_xlsx_plan_2010q1_2020q4"


def proof_evidence(batch: dict, raw: bytes) -> dict:
    return {
        "file_name": batch["file_name"],
        "origin_urls": ["https://www.huduser.gov/"],
        "created_at_utc": "2026-01-01T00:00:00+00:00",
        "bytes": batch["bytes"],
        "saved_by": "user, Chrome",
    }


def workbook_case(policies: dict, root: Path) -> dict:
    table = workbook_rows(2012, 1)
    plan = {"header": hud_workbook.HEADER, "required_state_fips": hud.STATE_FIPS}
    batch = plan_batch(XLSX_PLAN, year=2012, quarter=1)
    raw = workbook(table)
    with patch.object(hud_workbook, "DECISION_PATH", root / "absent.json"):
        derived, statistics = hud_workbook.validate_workbook(raw, batch, plan)
    return make_case("r5_hud_zip_county_workbook", policies, raw, derived, statistics, batch, proof_evidence(batch, raw))


def repeat_case(policies: dict, root: Path, repeats: int) -> dict:
    base = workbook_rows(2014, 2)
    table = [base[0], *[r for row in base[1 : repeats + 1] for r in (row, list(row))], *base[repeats + 1 :]]
    batch = plan_batch(XLSX_PLAN, year=2014, quarter=2)
    plan = {"header": hud_workbook.HEADER, "required_state_fips": hud.STATE_FIPS, "batches": [batch]}
    decision = {
        "plan_sha256": canonical_hash(plan),
        "source_id": "HUD",
        "quarters": [{"batch_id": batch["id"], "sha256": batch["sha256"], "year": 2014, "quarter": 2, "exact_repeat_rows": repeats}],
    }
    path = root / "decision.json"
    path.write_bytes(encoded_json(decision))
    path.with_suffix(".lock.json").write_bytes(encoded_json({"decision_sha256": canonical_hash(decision)}))
    raw = workbook(table)
    with patch.object(hud_workbook, "DECISION_PATH", path):
        derived, statistics = hud_workbook.validate_workbook(raw, batch, plan)
    statistics["exact_repeat_rows_dropped"] = APPROVED_REPEATS
    return make_case("r5_hud_zip_county_workbook", policies, raw, derived, statistics, batch, proof_evidence(batch, raw))


APPROVED_REPEATS = read_json(CONFIG / "hud_xlsx_exact_repeats_2014q2.json")["quarters"][0]["exact_repeat_rows"]
WONDER_PLAN = read_json(CONFIG / "wonder_export_plan.json")


def wonder_rows(database: str, year: int) -> list[list[str]]:
    out = []
    label = f"{year} " if database == "D158" else str(year)
    for state in hud.STATE_FIPS:
        for n in range(1, 62):
            code = f"{state}{n:03d}"
            out.append(["", f"Synthetic {code}", code, label, str(year), "120", "10000", "1200.0", "990.1", "1409.9", "109.5"])
    out[-102][5:] = ["Suppressed", "800", "Suppressed", "Suppressed", "Suppressed", "Suppressed"]
    out[-101][7] = "Unreliable" if database == "D76" else "200.0"
    out[-100][6:] = ["Not Available"] * 5
    out[-99][5:] = ["Missing"] * 6
    return out


def wonder_case(policies: dict, database: str, year: int) -> dict:
    batch = next(b for b in WONDER_PLAN["batches"] if b["database"] == database and b["years"] == [year, year])
    raw = export_text(database, year, year, wonder_rows(database, year))
    derived, statistics = wonder.validate_export(raw, batch, WONDER_PLAN)
    notes = ("\n".join(wonder.split_export(raw)[1]) + "\n").encode()
    extra = proof_evidence(batch, raw) | {"origin_urls": [wonder.ORIGIN + database], "query": statistics["query"], "notes_sha256": sha(notes)}
    return make_case("r6_wonder_county_year", policies, raw, derived, statistics, batch, extra, {"derived/export_notes.txt": notes})


def set_cell_with(row: int, column: str, change: Callable[[str], str]) -> Mutation:
    def apply(case: dict) -> None:
        def edit(rows: list[list[str]]) -> None:
            index = rows[0].index(column)
            rows[row][index] = change(rows[row][index])

        edit_rows(case, edit)

    return apply


def set_cell(row: int, column: str, value: str) -> Mutation:
    return set_cell_with(row, column, lambda _old: value)


def oversize(case: dict) -> None:
    limit = next(f for f in case["policy"]["files"] if f["role"] == "table")["max_bytes"]
    case["files"][case["table"]] += b"#" * (limit - len(case["files"][case["table"]]) + 1)
    seal(case)


def on_rows(change: Callable[[list[list[str]]], None], keep_hash: bool = False) -> Mutation:
    return lambda case: edit_rows(case, change, keep_hash)


def on_case_evidence(change: Callable[[dict, bytes], None]) -> Mutation:
    return lambda case: change(case["evidence"], case["raw"])


def on_evidence(change: Callable[[dict], None]) -> Mutation:
    return lambda case: change(case["evidence"])


def on_statistics(change: Callable[[dict], None]) -> Mutation:
    return lambda case: change(case["evidence"]["statistics"])


def on_context(change: Callable[[dict], None]) -> Mutation:
    return lambda case: change(case["context"])


def on_notes(change: Callable[[str], str]) -> Mutation:
    def apply(case: dict) -> None:
        case["files"]["derived/export_notes.txt"] = change(case["files"]["derived/export_notes.txt"].decode()).encode()
        seal(case)

    return apply


def raw_bytes(change: Callable[[bytes], bytes]) -> Mutation:
    def apply(case: dict) -> None:
        case["files"][case["table"]] = change(case["files"][case["table"]])
        seal(case)

    return apply


def strip_first(column: str) -> Mutation:
    return set_cell_with(2, column, lambda value: value[1:])


def drop_row(index: int) -> Callable[[list[list[str]]], None]:
    def apply(rows: list[list[str]]) -> None:
        rows.pop(index)

    return apply


def swap_rows(rows: list[list[str]]) -> None:
    rows[2], rows[3] = rows[3], rows[2]


def duplicate_row(rows: list[list[str]]) -> None:
    rows.insert(3, list(rows[2]))


def drop_cell(rows: list[list[str]]) -> None:
    rows[2].pop()


def rename_header(rows: list[list[str]]) -> None:
    rows[0][1] += "x"


def drop_column(rows: list[list[str]]) -> None:
    for row in rows:
        row.pop(1)


def add_column(rows: list[list[str]]) -> None:
    for i, row in enumerate(rows):
        row.append("extra" if i == 0 else "1")


def reorder_columns(rows: list[list[str]]) -> None:
    for row in rows:
        row[0], row[1] = row[1], row[0]


def remove_where(predicate: Callable[[list[str], list[str]], bool]) -> Callable[[list[list[str]]], None]:
    def apply(rows: list[list[str]]) -> None:
        rows[1:] = [r for r in rows[1:] if not predicate(rows[0], r)]

    return apply


def common_defects(identifier: str, foreign: str) -> list[tuple[str, str, str, Mutation]]:
    """Defects any table can show; each targets a different check and keeps every other binding valid."""
    return [
        ("header_column_renamed", "second column name changed", "header differs", on_rows(rename_header)),
        ("header_column_dropped", "second column removed from every row", "header differs", on_rows(drop_column)),
        ("header_column_added", "extra column appended to every row", "header differs", on_rows(add_column)),
        ("header_columns_reordered", "first two columns swapped", "header differs", on_rows(reorder_columns)),
        ("row_cell_dropped", "one cell removed from one row", "row width", on_rows(drop_cell)),
        ("identifier_leading_zero_lost", "leading zero removed from an identifier", identifier, strip_first(identifier)),
        ("duplicate_key", "row repeated", "duplicate key", on_rows(duplicate_row)),
        ("rows_out_of_order", "two rows swapped", "row order", on_rows(swap_rows)),
        ("crlf_serialization", "CRLF line endings", "serialization", raw_bytes(lambda b: b.replace(b"\n", b"\r\n"))),
        ("bom_serialization", "UTF-8 byte order mark", "serialization", raw_bytes(lambda b: b"\xef\xbb\xbf" + b)),
        ("blank_trailing_line", "blank line after the last row", "serialization", raw_bytes(lambda b: b + b"\n")),
        ("header_only", "all data rows removed", "no data rows", on_rows(lambda rows: rows.__delitem__(slice(1, None)))),
        (
            "derivative_edited_after_hash",
            "a cell edited, evidence hash untouched",
            "derivative hash",
            on_rows(lambda rows: rows[2].__setitem__(-1, rows[2][-1] + "0"), True),
        ),
        ("raw_hash_differs", "expected raw hash differs from the pinned one", "raw hash", on_context(lambda c: c.update(raw_sha256="f" * 64))),
        ("evidence_carries_body", "evidence has a body_hex key", "evidence carries", on_evidence(lambda e: e.update(body_hex="00"))),
        ("evidence_key_missing", "statistics key removed", "evidence lacks", on_evidence(lambda e: e.pop("statistics"))),
        ("model_eligible_true", "statistics claim model eligibility", "model_eligible", on_statistics(lambda s: s.update(model_eligible=True))),
        ("derivative_oversize", "bytes above the policy limit", "size limit", oversize),
        ("evidence_row_count_wrong", "statistics rows differ from the table", "statistics differ", on_statistics(lambda s: s.update(rows=s["rows"] + 1))),
        (
            "evidence_retains_original_top_level",
            "the exact original response in a top-level field",
            "evidence schema",
            on_case_evidence(lambda e, raw: e.update(response_body=raw.decode("latin-1"))),
        ),
        (
            "evidence_retains_original_nested_hex",
            "the exact original response as hex in a nested transport object",
            "evidence schema",
            on_case_evidence(lambda e, raw: e.update(transport={"body_hex": raw.hex()})),
        ),
        ("evidence_extra_top_level_field", "a field no collector writes", "evidence schema", on_evidence(lambda e: e.update(note="x"))),
        ("evidence_extra_statistics_field", "a field inside statistics", "evidence schema", on_statistics(lambda s: s.update(note="x"))),
        (
            "context_carries_batch",
            "context supplies its own batch with year 1900 and an unplanned table",
            "Context keys",
            on_context(lambda c: c.update(batch=CHANGED_BATCH)),
        ),
        ("context_extra_key", "context carries a coverage input", "Context keys", on_context(lambda c: c.update(counties=[]))),
        (
            "policy_edited_after_locking",
            "policy schema widened in the caller's copy",
            "differs from the lock",
            widen_policy,
        ),
        ("context_batch_id_unknown", "batch id not in any plan", "bound plans", on_context(lambda c: c.update(batch_id="0" * 64))),
        ("context_batch_id_of_another_recipe", "batch id from another recipe's plan", "bound plans", on_context(lambda c: c.update(batch_id=foreign))),
    ]


def api_defects(other_request: dict, long_field: str = "endpoint") -> list[tuple[str, str, str, Mutation]]:
    return [
        ("request_empty", "request is an empty object", "evidence schema violation at request", on_evidence(lambda e: e.update(request={}))),
        ("request_for_another_batch", "request names another batch", "request differs", on_evidence(lambda e: e.update(request=other_request))),
        ("request_extra_field", "extra field inside the request", "evidence schema violation at request", on_evidence(lambda e: e["request"].update(note="x"))),
        ("endpoint_differs", "endpoint names another host", "endpoint", on_evidence(lambda e: e.update(endpoint="https://example.org/"))),
        ("endpoint_long_string", "5,000-character string in a declared field", "evidence schema", on_evidence(lambda e: e.update(endpoint="x" * 5000))),
    ]


def other_batch(plan_name: str, case_batch: str, **different: Any) -> str:
    return next(
        b["id"] for b in read_json(CONFIG / f"{plan_name}.json")["batches"] if b["id"] != case_batch and all(b.get(k) != v for k, v in different.items())
    )


def repin(case: dict) -> None:
    case["evidence"]["sha256"] = case["context"]["raw_sha256"] = "e" * 64


def widen_policy(case: dict) -> None:
    case["policy"] = copy.deepcopy(case["policy"])
    case["policy"]["evidence"]["schema"].update(additionalProperties=True)


def add_county(rows: list[list[str]]) -> None:
    at = {name: i for i, name in enumerate(rows[0])}
    extra = list(rows[-1])
    extra[at["county"]] = "999"
    extra[at["GEO_ID"]] = f"0500000US{extra[at['state']]}999"
    rows.append(extra)


def set_all_years(rows: list[list[str]]) -> None:
    for row in rows[1:]:
        row[3] = row[4] = "2019"


DETAILED_BATCH = read_json(CONFIG / "census_acs_detailed_plan.json")["batches"][0]
DETAILED_OTHER_TABLE = next(b["table"] for b in read_json(CONFIG / "census_acs_detailed_plan.json")["batches"] if b["table"] != DETAILED_BATCH["table"])
PROFILE_PLAN = read_json(CONFIG / "census_acs_api_plan_dp02pr.json")
HUD_PLAN = read_json(CONFIG / "hud_api_plan_2021q2_2025q4.json")

RECIPE_DEFECTS: dict[str, list[tuple[str, str, str, Mutation]]] = {
    "r1": [
        *api_defects(bls.request_for({**BLS_BATCH, "series_ids": BLS_SERIES[:-1]})),
        ("evidence_sha256_differs", "sha256 beside response_sha256 differs", "expected raw hash", on_evidence(lambda e: e.update(sha256="f" * 64))),
        ("bls_series_outside_batch", "series id not in the batch", "series", set_cell(-1, "seriesID", "LAUCN990990000000003")),
        ("bls_county_fips_differs_from_series", "county_fips disagrees with the series id", "county_fips", set_cell(2, "county_fips", "01999")),
        ("bls_period_outside_grid", "year after the batch end year", "period grid", set_cell(-1, "year", str(BLS_LAST + 1))),
        ("bls_partial_year", "one month removed", "period grid", on_rows(drop_row(2))),
        (
            "bls_year_missing_without_hold",
            "a full year removed for a series that has no hold",
            "availability hold",
            on_rows(remove_where(lambda h, r: r[0] == BLS_SERIES[2] and r[h.index("year")] == str(BLS_FIRST + 2))),
        ),
        (
            "bls_hold_for_present_year",
            "a hold names a year that has rows",
            "availability hold",
            on_statistics(
                lambda s: s["availability_holds"].append({"series_id": BLS_SERIES[3], "year": BLS_FIRST, "status": "availability_hold", "message": "x"})
            ),
        ),
        ("bls_missing_token_without_footnote", "'-' value with an empty footnote", "missing token", on_rows(lambda r: r[1 + 9].__setitem__(7, "[{}]"))),
        ("bls_value_token_unknown", "value written as 1e5", "value", set_cell(2, "value", "1e5")),
        ("bls_footnotes_not_json", "footnotes cell is not JSON", "footnotes", set_cell(2, "footnotes", "not json")),
        (
            "bls_footnote_count_differs",
            "footnoted_rows statistic edited",
            "statistics differ",
            on_statistics(lambda s: s.update(footnoted_rows=s["footnoted_rows"] + 1)),
        ),
    ],
    "r2": [
        *api_defects(census_profile.request_for({"table": "DP03"})),
        ("evidence_sha256_differs", "sha256 beside response_sha256 differs", "expected raw hash", on_evidence(lambda e: e.update(sha256="f" * 64))),
        (
            "context_batch_of_another_table",
            "context names the DP02 batch",
            "request differs",
            on_context(lambda c: c.update(batch_id=other_batch("census_acs_api_plan", PROFILE_PLAN["batches"][0]["id"], table="DP02PR"))),
        ),
        ("census_county_missing", "one pinned county removed", "county coverage", on_rows(drop_row(2))),
        ("census_county_extra", "county outside the pinned set", "county coverage", on_rows(add_county)),
        ("census_geo_id_differs", "GEO_ID does not match state and county", "GEO_ID", set_cell(2, "GEO_ID", "0500000US99999")),
        ("census_state_null", "null token in a geography key", "state", set_cell(2, "state", NULL)),
        ("census_null_count_differs", "null_cells statistic edited", "statistics differ", on_statistics(lambda s: s.update(null_cells=s["null_cells"] + 1))),
        (
            "census_annotation_count_differs",
            "nonempty_annotations statistic edited",
            "statistics differ",
            on_statistics(lambda s: s.update(nonempty_annotations=0)),
        ),
    ],
    "r3": [
        *api_defects(census_detailed.request_for({"table": DETAILED_OTHER_TABLE})),
        ("census_county_below_floor", "rows below the 3100 floor", "row limit", on_rows(lambda rows: rows.__delitem__(slice(3001, None)))),
        ("census_geo_id_differs", "GEO_ID does not match state and county", "GEO_ID", set_cell(2, "GEO_ID", "0500000US99999")),
        (
            "context_batch_of_another_year",
            "context names the same table in another year",
            "endpoint",
            on_context(lambda c: c.update(batch_id=other_batch("census_acs_detailed_plan", DETAILED_BATCH["id"], year=DETAILED_BATCH["year"], table=""))),
        ),
        (
            "context_batch_of_another_table",
            "context names another table",
            "request differs",
            on_context(lambda c: c.update(batch_id=other_batch("census_acs_detailed_plan", DETAILED_BATCH["id"], table=DETAILED_BATCH["table"]))),
        ),
    ],
    "r4": [
        *api_defects(hud.request_for({"type": 2, "query": "All", "year": 2023, "quarter": 1})),
        ("hud_ratio_above_one", "ratio 1.5", "res_ratio", set_cell(2, "res_ratio", "1.5")),
        ("hud_ratio_signed", "ratio -0.1", "res_ratio", set_cell(2, "res_ratio", "-0.1")),
        ("hud_state_missing", "all rows of one state removed", "state coverage", on_rows(remove_where(lambda h, r: r[h.index("geoid")].startswith("01")))),
        (
            "hud_ratio_sum_profile_differs",
            "ratio_sums statistic edited",
            "statistics differ",
            on_statistics(lambda s: s["ratio_sums"]["tot_ratio"].update(zips_summing_to_one=0)),
        ),
        ("hud_zip_short", "ZIP with four digits", "zip", set_cell(2, "zip", "1234")),
    ],
    "r5": [
        ("hud_ratio_above_one", "ratio 1.5", "tot_ratio", set_cell(2, "tot_ratio", "1.5")),
        ("hud_territory_code_refused", "two-digit area code in a workbook", "geoid", set_cell(2, "geoid", "60")),
        (
            "hud_repeat_count_differs",
            "exact_repeat_rows_dropped recorded where no repeat is approved",
            "exact repeat",
            on_statistics(lambda s: s.update(exact_repeat_rows_dropped=1)),
        ),
        ("hud_origin_differs", "origin URL differs", "origin", on_evidence(lambda e: e.update(origin_urls=["https://example.org/"]))),
        ("hud_file_name_differs", "file name differs from the batch", "file name", on_evidence(lambda e: e.update(file_name="other.xlsx"))),
        ("hud_pinned_hash_not_the_planned_hash", "evidence and context agree on a hash the plan does not hold", "bound batch", repin),
        ("hud_bytes_differ_from_batch", "evidence bytes differ from the planned size", "bytes differ", on_evidence(lambda e: e.update(bytes=1))),
        ("hud_file_name_long_string", "5,000-character string in a declared field", "evidence schema", on_evidence(lambda e: e.update(file_name="x" * 5000))),
    ],
    "r6": [
        ("wonder_deaths_one_to_nine", "deaths 5 shown", "refused range 1-9", set_cell(2, "Deaths", "5")),
        ("wonder_year_differs_from_code", "Year differs from Year Code", "Year", set_cell(2, "Year", "1999")),
        ("wonder_notes_cell_filled", "Notes cell filled", "Notes", set_cell(2, "Notes", "Total")),
        ("wonder_token_in_wrong_column", "Unreliable in Population", "Population", set_cell(2, "Population", "Unreliable")),
        ("wonder_years_differ_from_plan", "every row relabelled to a year the plan lacks", "years", on_rows(set_all_years)),
        ("wonder_county_floor", "rows removed below the county floor", "county floor", on_rows(lambda rows: rows.__delitem__(slice(1, 20)))),
        ("wonder_notes_divider_missing", "notes lines emptied", "query", on_notes(lambda t: "")),
        (
            "wonder_query_parameter_changed",
            "a fixed query parameter edited",
            "query",
            on_notes(lambda t: t.replace('"Show Totals: Disabled"', '"Show Totals: Enabled"')),
        ),
        ("wonder_evidence_query_differs", "evidence query differs from the notes", "query", on_evidence(lambda e: e["query"].update(dataset="Other"))),
        ("wonder_evidence_query_extra_field", "extra field inside the evidence query", "evidence schema", on_evidence(lambda e: e["query"].update(note="x"))),
        ("wonder_pinned_hash_not_the_planned_hash", "evidence and context agree on a hash the plan does not hold", "bound batch", repin),
        ("wonder_bytes_differ_from_batch", "evidence bytes differ from the planned size", "bytes differ", on_evidence(lambda e: e.update(bytes=1))),
        ("wonder_file_name_differs", "file name differs from the batch", "file name", on_evidence(lambda e: e.update(file_name="other.xls"))),
        (
            "wonder_notes_hash_differs",
            "notes edited, evidence hash untouched",
            "notes hash",
            lambda case: case["files"].update({"derived/export_notes.txt": case["files"]["derived/export_notes.txt"] + b"x\n"}),
        ),
    ],
}


def scenarios_for(
    case_factory: Callable[[], dict],
    recipe: str,
    prefix: str,
    extra: list[tuple[str, str, str, Mutation]],
    run: Callable[[dict], Any],
    record: Callable[..., None],
) -> None:
    identifier = {"r1": "seriesID", "r2": "state", "r3": "state", "r4": "zip", "r5": "zip", "r6": "County Code"}[recipe]
    record(f"{prefix}_real_producer_output_verifies", recipe, "none: derivative made by the real producer", None, lambda: run(case_factory()))
    foreign = BLS_BATCH["id"] if recipe != "r1" else DETAILED_BATCH["id"]
    for name, defect, error, mutate in [*common_defects(identifier, foreign), *extra]:

        def action(mutate: Mutation = mutate) -> None:
            case = case_factory()
            mutate(case)
            run(case)

        record(f"{prefix}_{name}", recipe, defect, error, action)


def policy_load_scenarios(root: Path, record: Callable[..., None]) -> None:
    source = {n: (CONFIG / n).read_bytes() for n in ("projection_policies.json", "projection_policies.lock.json", "projection_policies.schema.json")}
    bronze = REPO_ROOT / "config/lakehouse/bronze_tables.json"

    def staged(name: str, change: Callable[[dict], None] | None = None, relock: bool = True, lock_change: Callable[[dict], None] | None = None) -> Path:
        target = root / name
        target.mkdir()
        for n, body in source.items():
            (target / n).write_bytes(body)
        if change:
            policies = json.loads(source["projection_policies.json"])
            change(policies)
            (target / "projection_policies.json").write_bytes(encoded_json(policies))
            lock = {"policies_sha256": canonical_hash(policies), "entries": {e["id"]: canonical_hash(e) for e in policies["entries"]}}
            if relock:
                (target / "projection_policies.lock.json").write_bytes(encoded_json(lock))
        if lock_change:
            lock = json.loads(source["projection_policies.lock.json"])
            lock_change(lock)
            (target / "projection_policies.lock.json").write_bytes(encoded_json(lock))
        return target

    def load(directory: Path, bronze_path: Path = bronze) -> Callable[[], Any]:
        return lambda: module.load_policies(directory, bronze_path)

    def first(policies: dict, recipe: str = "R1") -> dict:
        return next(e for e in policies["entries"] if e["recipe"] == recipe)

    def table_columns(policies: dict, recipe: str = "R1") -> list[dict]:
        return first(policies, recipe)["files"][0]["layouts"][0]["columns"]

    def truncate(directory: Path) -> Path:
        path = directory / "projection_policies.json"
        path.write_bytes(path.read_bytes()[:-40])
        return directory

    record(
        "policy_load_committed_files",
        "all",
        "none: the committed policy, lock and schema",
        None,
        lambda: require(len(module.load_policies()) == 6, "Six entries expected"),
    )
    record("policy_truncated_file", "all", "policy bytes truncated", "not valid JSON", load(truncate(staged("truncated"))))
    unlocked = staged("no_lock")
    (unlocked / "projection_policies.lock.json").unlink()
    record("policy_lock_missing", "all", "lock file absent", "lock", load(unlocked))
    record(
        "policy_changed_domain_lock_untouched",
        "R1",
        "a value range edited without relocking",
        "lock differs",
        load(staged("drift", lambda p: table_columns(p)[3]["domain"].update(range=[1900, 2025]), relock=False)),
    )
    record(
        "policy_lock_line_removed",
        "R1",
        "one entry missing from the lock",
        "lock differs",
        load(staged("lock_line", lock_change=lambda lock: lock["entries"].pop("r1_bls_laus_api"))),
    )
    record("policy_wildcard_selector", "R1", "input name is '*'", "schema", load(staged("wildcard", lambda p: table_columns(p)[0].update(input="*"))))
    record("policy_empty_selector_list", "R1", "layout with no columns", "schema", load(staged("empty", lambda p: table_columns(p).clear())))
    record("policy_unknown_field", "R1", "entry with an extra field", "schema", load(staged("unknown", lambda p: first(p).update(select_all=True))))
    record(
        "policy_duplicate_output",
        "R1",
        "two columns share an output name",
        "duplicate",
        load(staged("dupe", lambda p: table_columns(p)[1].update(output="seriesID"))),
    )
    record("policy_duplicate_entry", "R1", "entry id repeated", "duplicate", load(staged("dupe_entry", lambda p: p["entries"].append(copy.deepcopy(first(p))))))
    record(
        "policy_parser_missing",
        "R1",
        "parser function renamed",
        "parser",
        load(staged("parser", lambda p: first(p)["parser"].update(function="validate_nothing"))),
    )
    record(
        "policy_consumer_file_missing",
        "R1",
        "consumer path does not exist",
        "consumer",
        load(staged("consumer", lambda p: first(p)["consumers"]["paths"].append("dbt/models/silver/staging/stg_absent.sql"))),
    )
    bad = json.loads(bronze.read_text())
    next(t for t in bad["tables"] if t["table"] == "bls_laus")["member_pattern"] = "^other\\.csv$"
    (root / "bronze_bad.json").write_text(json.dumps(bad))
    record(
        "policy_bronze_pattern_mismatch",
        "R1",
        "bronze member pattern does not match the derivative name",
        "bronze",
        load(CONFIG, root / "bronze_bad.json"),
    )
    record(
        "policy_dropped_table_unknown",
        "R5",
        "dropped bronze table does not exist",
        "bronze",
        load(staged("dropped", lambda p: first(p, "R5")["consumers"]["dropped_bronze_tables"].append("absent_table"))),
    )
    record(
        "policy_table_without_layout",
        "R1",
        "table file has no layout",
        "layout",
        load(staged("no_layout", lambda p: first(p)["files"][0].pop("layouts"))),
    )

    def bound(policies: dict) -> dict:
        return first(policies, "R3")["files"][0]["layouts"][0]["bound_batches"][0]

    def evidence_of(policies: dict, recipe: str = "R1") -> dict:
        return first(policies, recipe)["evidence"]

    record(
        "policy_bound_batch_year_changed", "R3", "bound batch year 1900, relocked", "bound batches", load(staged("year", lambda p: bound(p).update(year=1900)))
    )
    record(
        "policy_bound_batch_table_changed",
        "R3",
        "bound batch table unplanned, relocked",
        "bound batches",
        load(staged("table", lambda p: bound(p).update(table="X"))),
    )
    record(
        "policy_bound_batch_id_changed", "R3", "bound batch id changed, relocked", "bound batches", load(staged("bid", lambda p: bound(p).update(id="0" * 64)))
    )
    record(
        "policy_plan_hash_changed",
        "R1",
        "plan hash edited, relocked",
        "plan hash",
        load(staged("plan_hash", lambda p: first(p)["extraction_map"]["plans"][0].update(plan_sha256="0" * 64))),
    )
    record(
        "policy_decision_hash_changed",
        "R5",
        "decision hash edited, relocked",
        "decision hash",
        load(staged("decision_hash", lambda p: first(p, "R5")["extraction_map"]["decision"].update(decision_sha256="0" * 64))),
    )
    record(
        "policy_decision_binds_other_plan",
        "R4",
        "entry binds a decision for a plan it does not bind",
        "decision binds",
        load(staged("decision_plan", lambda p: first(p, "R4")["extraction_map"].update(decision=copy.deepcopy(first(p, "R5")["extraction_map"]["decision"])))),
    )
    schema_cases: list[tuple[str, str, str, Callable[[dict], None]]] = [
        ("top_object_open", "R1", "object is open", lambda p: evidence_of(p)["schema"].pop("additionalProperties")),
        ("request_object_open", "R1", "object is open", lambda p: evidence_of(p)["schema"]["properties"]["request"].update(additionalProperties=True)),
        ("string_unbounded", "R1", "string is unbounded", lambda p: evidence_of(p)["schema"]["properties"]["endpoint"].pop("maxLength")),
        ("array_unbounded", "R1", "array is unbounded", lambda p: evidence_of(p)["schema"]["properties"]["request"]["properties"]["seriesid"].pop("maxItems")),
        ("integer_unbounded", "R1", "integer is unbounded", lambda p: evidence_of(p)["schema"]["properties"]["bytes"].pop("maximum")),
        ("unsupported_keyword", "R1", "unsupported keyword", lambda p: evidence_of(p)["schema"].update(oneOf=[])),
        ("untyped_node", "R1", "no type", lambda p: evidence_of(p)["schema"]["properties"].update(note={"maxLength": 5})),
        ("required_differs", "R1", "required differs", lambda p: evidence_of(p)["schema"]["required"].remove("request")),
        ("forbidden_key_declared", "R1", "forbidden key", lambda p: evidence_of(p)["schema"]["properties"].update(body_hex={"type": "string", "maxLength": 5})),
        ("request_function_missing", "R1", "request function", lambda p: evidence_of(p).pop("request_function")),
        ("request_function_on_proof", "R5", "request function", lambda p: evidence_of(p, "R5").update(request_function="request_for")),
        ("schema_absent", "R1", "schema", lambda p: evidence_of(p).pop("schema")),
        (
            "pattern_unanchored",
            "R6",
            "not anchored",
            lambda p: evidence_of(p, "R6")["schema"]["properties"]["statistics"]["properties"]["counties_per_year"].update(
                patternProperties={"[0-9]{4}": {"type": "integer", "minimum": 0, "maximum": 9}}
            ),
        ),
    ]
    for name, recipe, error, change in schema_cases:
        record(f"policy_evidence_schema_{name}", recipe, f"evidence schema edited and relocked: {name}", error, load(staged(f"schema_{name}", change)))

    def lock_differs(loader: Callable[[str, str], Any], key: str) -> Callable[[], None]:
        def action() -> None:
            mini = root / f"mini_{key}"
            (mini / "config/acquisition").mkdir(parents=True)
            body: dict = {"batches": []}
            (mini / "config/acquisition/x.json").write_bytes(encoded_json(body))
            (mini / "config/acquisition/x.lock.json").write_bytes(encoded_json({key: "0" * 64}))
            with patch.object(module, "REPO_ROOT", mini):
                loader("config/acquisition/x.json", canonical_hash(body))

        return action

    record(
        "policy_plan_lock_differs",
        "R1",
        "plan file hash matches the policy but its lock differs",
        "plan lock",
        lock_differs(module._plan, "plan_sha256"),
    )
    record(
        "policy_decision_lock_differs",
        "R5",
        "decision hash matches the policy but its lock differs",
        "decision lock",
        lock_differs(module._decision, "decision_sha256"),
    )


def alignment_scenarios(policies: dict, record: Callable[..., None]) -> None:
    """Cross-check the frozen policy against tracked plans, locks and the reviewed producers; no data files are opened."""

    def plans_bound() -> None:
        for entry in policies.values():
            for plan in entry["extraction_map"]["plans"]:
                body = read_json(REPO_ROOT / plan["path"])
                require(
                    canonical_hash(body) == plan["plan_sha256"] == read_json((REPO_ROOT / plan["path"]).with_suffix(".lock.json"))["plan_sha256"], plan["path"]
                )
            decision = entry["extraction_map"].get("decision")
            if decision:
                body = read_json(REPO_ROOT / decision["path"])
                require(canonical_hash(body) == decision["decision_sha256"], decision["path"])

    def census_headers_bound() -> None:
        for recipe_id, names in (
            ("r2_census_acs_profile_api", ("census_acs_api_plan", "census_acs_api_plan_dp02pr")),
            ("r3_census_acs_detailed_api", ("census_acs_detailed_plan",)),
        ):
            layout = policies[recipe_id]["files"][0]["layouts"][0]
            planned = [b for n in names for b in read_json(CONFIG / f"{n}.json")["batches"]]
            require(
                layout["bound_batches"]
                == [
                    {"id": b["id"], "table": b["table"], "headers_sha256": canonical_hash(b["headers"]), **({"year": b["year"]} if "year" in b else {})}
                    for b in planned
                ],
                recipe_id,
            )
            for batch in planned:
                module.column_rules(layout, batch["headers"])

    def hud_layouts_match() -> None:
        plan = read_json(CONFIG / "hud_api_plan.json")
        layouts = [[c["output"] for c in layout["columns"]] for layout in policies["r4_hud_usps_crosswalk_api"]["files"][0]["layouts"]]
        require(layouts == hud.layouts(plan), "HUD API layouts differ from the locked plan")
        workbook_layout = [c["output"] for c in policies["r5_hud_zip_county_workbook"]["files"][0]["layouts"][0]["columns"]]
        require(workbook_layout == sorted(hud_workbook.HEADER), "HUD workbook layout differs from the contract header")

    def bls_columns_match() -> None:
        require([c["output"] for c in policies["r1_bls_laus_api"]["files"][0]["layouts"][0]["columns"]] == bls.FIELDS, "BLS columns differ")

    def wonder_domains_match() -> None:
        entry = policies["r6_wonder_county_year"]
        columns = entry["files"][0]["layouts"][0]["columns"]
        require([c["output"] for c in columns] == wonder.HEADER, "WONDER header differs")
        require(
            entry["query"] == {"databases": {k: v["title"] for k, v in wonder.DATABASES.items()}, "fixed_parameters": wonder.FIXED_PARAMETERS},
            "WONDER query differs",
        )
        grid = ["0", "5", "9", "10", "007", "Suppressed", "Unreliable", "Not Available", "Missing", "", "abc", "1.5", "-1", "1e3"]
        for selector in columns[5:]:
            for value in grid:
                try:
                    wonder.check_value(selector["output"], value)
                    producer = True
                except ValueError:
                    producer = False
                require(producer == (module.cell_problem(selector, value) is None), f"{selector['output']} {value!r}: producer and policy disagree")

    def endpoints_match() -> None:
        for recipe_id, names in (
            ("r1_bls_laus_api", ("bls_api_plan",)),
            ("r2_census_acs_profile_api", ("census_acs_api_plan", "census_acs_api_plan_dp02pr")),
            ("r4_hud_usps_crosswalk_api", ("hud_api_plan", "hud_api_plan_2021q2_2025q4")),
        ):
            for name in names:
                require(read_json(CONFIG / f"{name}.json")["endpoint"] == policies[recipe_id]["source"]["route"], f"{recipe_id} route differs from {name}")
        route = policies["r3_census_acs_detailed_api"]["source"]["route"]
        for batch in read_json(CONFIG / "census_acs_detailed_plan.json")["batches"]:
            require(census_detailed.endpoint(batch["year"]) == route.replace("{year}", str(batch["year"])), "Detailed endpoint differs from the route")

    def every_batch_resolves() -> None:
        total = 0
        for policy in policies.values():
            for plan in policy["extraction_map"]["plans"]:
                for batch in read_json(REPO_ROOT / plan["path"])["batches"]:
                    require(module.frozen_inputs(policy, batch["id"])["batch"] == batch, "Resolved batch differs from the plan")
                    total += 1
        require(total == 520 + 4 + 1 + 21 + 1 + 19 + 44 + 6, "Unexpected number of planned batches")

    record(
        "alignment_routes_equal_plan_endpoints", "R1-R4", "none: policy routes equal plan endpoints and the detailed endpoint function", None, endpoints_match
    )
    record("alignment_every_planned_batch_resolves", "all", "none: every planned batch resolves by id with its coverage inputs", None, every_batch_resolves)
    record("alignment_plans_and_decision_hashes", "all", "none: policy plan hashes against tracked plans and locks", None, plans_bound)
    record("alignment_census_headers_bound_and_ruled", "R2,R3", "none: every locked header bound and matched by a rule", None, census_headers_bound)
    record("alignment_hud_layouts", "R4,R5", "none: layouts equal the locked plan and contract fields", None, hud_layouts_match)
    record("alignment_bls_columns", "R1", "none: columns equal the producer fields", None, bls_columns_match)
    record("alignment_wonder_domains_equal_producer_check", "R6", "none: policy domains agree with the producer's check_value", None, wonder_domains_match)


def exercise(root: Path) -> list[dict]:
    results: list[dict] = []

    def record(name: str, recipe: str, defect: str, error: str | None, action: Callable[[], Any]) -> None:
        try:
            action()
            results.append(
                {"name": name, "recipe": recipe, "planted_defect": defect, "passed": error is None, "observed": "accepted", "expected": error or "accepted"}
            )
        except (ValueError, OSError, KeyError, TypeError, IndexError) as failure:
            results.append(
                {
                    "name": name,
                    "recipe": recipe,
                    "planted_defect": defect,
                    "passed": error is not None and error in str(failure),
                    "observed": str(failure)[:160],
                    "expected": error or "accepted",
                }
            )

    policies = module.load_policies()

    policy_load_scenarios(root, record)
    alignment_scenarios(policies, record)
    scenarios_for(lambda: bls_case(policies), "r1", "r1", RECIPE_DEFECTS["r1"], run, record)
    scenarios_for(lambda: profile_case(policies), "r2", "r2", RECIPE_DEFECTS["r2"], run, record)
    scenarios_for(lambda: detailed_case(policies), "r3", "r3", RECIPE_DEFECTS["r3"], run, record)
    scenarios_for(
        lambda: hud_case(policies, read_json(CONFIG / "hud_api_plan.json")["result_fields"]), "r4", "r4_eight_field", RECIPE_DEFECTS["r4"], run, record
    )
    record(
        "r4_six_field_layout_verifies",
        "R4",
        "none: 2022 Q1 layout without city and state",
        None,
        lambda: run(hud_case(policies, [f for f in hud.layouts(read_json(CONFIG / "hud_api_plan.json"))[1]])),
    )
    record(
        "r4_mixed_layout_rejected",
        "R4",
        "city column without state",
        "header differs",
        lambda: run(_mixed(hud_case(policies, hud.layouts(read_json(CONFIG / "hud_api_plan.json"))[0]))),
    )
    scenarios_for(lambda: workbook_case(policies, root), "r5", "r5", RECIPE_DEFECTS["r5"], run, record)
    record("r5_approved_exact_repeats_verify", "R5", "none: approved repeat count recorded", None, lambda: run(repeat_case(policies, root, 3)))
    record(
        "r5_repeat_unapproved_in_evidence",
        "R5",
        "repeat count recorded where none is approved",
        "exact repeat",
        lambda: run(_repeat_recorded(workbook_case(policies, root), 3)),
    )
    record(
        "r5_repeat_count_differs_from_decision",
        "R5",
        "evidence repeat count differs from the approved count",
        "exact repeat",
        lambda: run(_repeat_recorded(repeat_case(policies, root, 3), 3)),
    )
    scenarios_for(lambda: wonder_case(policies, "D76", 2020), "r6", "r6_d76", RECIPE_DEFECTS["r6"], run, record)
    record("r6_d158_year_label_verifies", "R6", "none: single-race export with a padded year label", None, lambda: run(wonder_case(policies, "D158", 2024)))
    canary_scenarios(policies, root, record)
    boundary_scenarios(policies, record)
    record("repeat_run_is_identical", "all", "none: same inputs verified twice", None, lambda: _same_twice(run, bls_case(policies)))
    record("verifier_takes_no_original", "all", "none: verifier signature has no original-file input", None, _no_original_input)
    authority_scenarios(policies, root, record)
    generator_scenarios(root, record)
    return results


class Collector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def verifier_locals(error: BaseException, watched: tuple[str, ...]) -> list[str]:
    """Return the repr of every local that a locals-capturing traceback would print for the watched files, across the error chain."""
    paths = {Path(name).resolve() for name in watched}
    texts: list[str] = []
    link: BaseException | None = error
    while link is not None:
        for frame in traceback.StackSummary.extract(traceback.walk_tb(link.__traceback__), capture_locals=True):
            if Path(frame.filename).resolve() in paths:
                texts += (frame.locals or {}).values()
        link = link.__cause__ or link.__context__
    return texts


def rejection_of(action: Callable[[], Any]) -> Exception:
    try:
        action()
    except Exception as failure:
        return failure
    raise ValueError("The case was accepted")


def leak_check(action: Callable[[], Any], token: str = CANARY, watched: tuple[str, ...] | None = None) -> None:
    """Run a case that must be rejected and require that the canary is absent from every error, every captured output and every watched frame local."""
    handler, out, err = Collector(), io.StringIO(), io.StringIO()
    root = logging.getLogger()
    previous = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    caught: Exception | None = None
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                action()
            except Exception as failure:
                caught = failure
    finally:
        root.removeHandler(handler)
        root.setLevel(previous)
    if caught is None:
        raise ValueError("The canary case was accepted")
    texts: list[str] = []
    link: BaseException | None = caught
    while link is not None:
        texts += [str(link), repr(link), *map(str, link.args)]
        link = link.__cause__ or link.__context__
    texts += ["".join(traceback.format_exception(caught)), out.getvalue(), err.getvalue(), *handler.messages]
    require(not any(token in text for text in texts), "A canary value reached an error message or captured output")
    require(not any(token in text for text in verifier_locals(caught, watched or (str(module.__file__),))), "A canary value survives in a traceback local")


def _leaking_error() -> None:
    raise ValueError(f"rejected value {CANARY}")


def _leaking_output() -> None:
    sys.stderr.write(CANARY)
    raise ValueError("fixed category")


def _leaking_cause() -> None:
    try:
        int(CANARY)
    except ValueError as failure:
        raise ValueError("fixed category") from failure


def _leaking_locals() -> None:
    held = CANARY
    raise ValueError(f"fixed category {len(held)}")


def short_row_with_canary(rows: list[list[str]]) -> None:
    rows[2][0] = CANARY
    rows[2].pop()


def canary_scenarios(policies: dict, root: Path, record: Callable[..., None]) -> None:
    def bad_csv(body: bytes) -> bytes:
        return b'"x"' + CANARY.encode() + b"\n" + body

    def case_factories() -> dict[str, Callable[[], dict]]:
        return {
            "r1": lambda: bls_case(policies),
            "r2": lambda: profile_case(policies),
            "r3": lambda: detailed_case(policies),
            "r4": lambda: hud_case(policies, read_json(CONFIG / "hud_api_plan.json")["result_fields"]),
            "r5": lambda: workbook_case(policies, root),
            "r6": lambda: wonder_case(policies, "D76", 2020),
        }

    tokens = {"r1_series_valid_shape": "LAUCN990990000000003", "r2_geo_id_valid_shape": "US99999"}
    mutations: list[tuple[str, str, Mutation]] = [
        ("r1_series_valid_shape", "r1", set_cell(-1, "seriesID", "LAUCN990990000000003")),
        ("r2_geo_id_valid_shape", "r2", set_cell(2, "GEO_ID", "0500000US99999")),
        ("r1_value_cell", "r1", set_cell(2, "value", CANARY)),
        ("r1_series_cell", "r1", set_cell(-1, "seriesID", CANARY)),
        ("r1_footnotes_cell", "r1", set_cell(2, "footnotes", CANARY)),
        ("r1_header_cell", "r1", on_rows(lambda rows: rows[0].__setitem__(1, CANARY))),
        ("r1_short_row", "r1", on_rows(short_row_with_canary)),
        ("r1_garbled_csv", "r1", raw_bytes(bad_csv)),
        ("r1_not_utf8", "r1", raw_bytes(lambda b: b"\xff" + CANARY.encode() + b)),
        ("r1_evidence_extra_name_and_value", "r1", on_evidence(lambda e: e.update({CANARY: CANARY}))),
        ("r1_evidence_nested_statistics", "r1", on_statistics(lambda s: s.update({CANARY: CANARY}))),
        ("r1_evidence_nested_request", "r1", on_evidence(lambda e: e["request"].update({CANARY: CANARY}))),
        ("r1_evidence_long_string", "r1", on_evidence(lambda e: e.update(endpoint=CANARY * 400))),
        ("r1_evidence_bad_hash_text", "r1", on_evidence(lambda e: e.update(sha256=CANARY))),
        ("r1_context_key", "r1", on_context(lambda c: c.update({CANARY: 1}))),
        ("r1_context_batch_id", "r1", on_context(lambda c: c.update(batch_id=CANARY))),
        ("r1_context_raw_hash", "r1", on_context(lambda c: c.update(raw_sha256=CANARY))),
        ("r2_name_cell", "r2", set_cell(2, "NAME", "\x00" + CANARY)),
        ("r2_geo_id_cell", "r2", set_cell(2, "GEO_ID", CANARY)),
        ("r3_state_cell", "r3", set_cell(2, "state", CANARY)),
        ("r4_ratio_cell", "r4", set_cell(2, "res_ratio", CANARY)),
        ("r4_zip_cell", "r4", set_cell(2, "zip", CANARY)),
        ("r5_ratio_cell", "r5", set_cell(2, "tot_ratio", CANARY)),
        ("r6_county_code_cell", "r6", set_cell(2, "County Code", CANARY)),
        ("r6_deaths_cell", "r6", set_cell(2, "Deaths", CANARY)),
        ("r6_notes_parameter", "r6", on_notes(lambda t: t.replace("Show Totals: Disabled", f"Show Totals: {CANARY}"))),
        ("r6_notes_not_utf8", "r6", lambda case: _notes_not_utf8(case)),
        ("r6_evidence_query", "r6", on_evidence(lambda e: e["query"].update(dataset=CANARY))),
        ("r6_evidence_file_name", "r6", on_evidence(lambda e: e.update(file_name=CANARY))),
    ]
    factories = case_factories()
    for name, recipe, mutate in mutations:

        def action(factory: Callable[[], dict] = factories[recipe], mutate: Mutation = mutate, token: str = tokens.get(name, CANARY)) -> None:
            case = factory()
            mutate(case)
            leak_check(lambda: run(case), token)

        record(f"canary_{name}", recipe.upper(), "canary planted in the rejected input", None, action)
    record(
        "canary_detector_flags_a_leaking_message", "all", "none: detector self-test, value in the message", "canary value", lambda: leak_check(_leaking_error)
    )
    record("canary_detector_flags_captured_output", "all", "none: detector self-test, value on stderr", "canary value", lambda: leak_check(_leaking_output))
    record("canary_detector_flags_a_chained_cause", "all", "none: detector self-test, value in the cause", "canary value", lambda: leak_check(_leaking_cause))
    record(
        "canary_detector_flags_a_traceback_local",
        "all",
        "none: detector self-test, value held in a frame local",
        "traceback local",
        lambda: leak_check(_leaking_locals, CANARY, (__file__,)),
    )


MESSAGE_FIELDS = {
    "name",
    "error",
    "source",
    "path",
    "key",
    "column",
    "header",
    "count",
    "problem",
    "short",
    "len(data)",
    "incomplete",
    "where",
    "layout['name']",
    "entry['id']",
    "spec['path']",
    "parser['module']",
    "parser['function']",
    "consumers['bronze_table']",
    "table['path']",
    "list(first.absolute_path)[:6]",
    "first.validator",
}
MESSAGE_CALLS = {"require", "ValueError", "RegistryError", "DerivativeRejected"}
CONSTANTS = ("CONTEXT_KEYS", "API_FAMILIES", "SCHEMA_KEYWORDS", "RATIOS", "FAMILIES")
GENERATOR = REPO_ROOT / "scripts/acquisition/build_derivative_policies.py"
GENERATED = ("projection_policies.json", "projection_policies.lock.json")


def message_expressions(source: str) -> set[str]:
    """Return every expression interpolated into the message argument of a raising call."""
    found: set[str] = set()
    for call in (n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Call)):
        name = getattr(call.func, "id", getattr(call.func, "attr", ""))
        if name not in MESSAGE_CALLS:
            continue
        for argument in call.args[1:] if name == "require" else call.args:
            for text in (n for n in ast.walk(argument) if isinstance(n, ast.JoinedStr)):
                found |= {ast.unparse(part.value) for part in text.values if isinstance(part, ast.FormattedValue)}
    return found


def boundary_scenarios(policies: dict, record: Callable[..., None]) -> None:
    def inner_error(kind: type[Exception]) -> Callable[[], None]:
        def action() -> None:
            case = bls_case(policies)

            def family(*_unused: Any) -> None:
                raise kind(f"inner {CANARY}")

            with patch.object(module, "FAMILIES", {**module.FAMILIES, "bls_laus": family}):
                leak_check(lambda: run(case))
                require(isinstance(rejection_of(lambda: run(case)), ValueError), "The boundary error is not a ValueError")

        return action

    def deadline_stays_a_deadline() -> None:
        case = bls_case(policies)

        def family(*_unused: Any) -> None:
            raise TimeoutError(f"deadline {CANARY}")

        with patch.object(module, "FAMILIES", {**module.FAMILIES, "bls_laus": family}):
            failure = rejection_of(lambda: run(case))
        require(isinstance(failure, TimeoutError), "A deadline stop surfaced as something other than a timeout")
        require(not isinstance(failure, module.DerivativeRejected), "A deadline stop surfaced as an input rejection")
        require(CANARY not in str(failure) and failure.__cause__ is None and failure.__context__ is None, "The timeout carries the inner text or a chain")

    def bls_month_names_nothing() -> None:
        case = bls_case(policies)
        row = parse(case["files"][case["table"]])[2]
        at = {name: i for i, name in enumerate(parse(case["files"][case["table"]])[0])}
        edit_rows(case, drop_row(2))
        message = str(rejection_of(lambda: run(case)))
        require("period grid" in message, "The grid defect was not the one reported")
        require(row[at["seriesID"]] not in message and re.search(rf"\b{row[at['year']]}\b", message) is None, "The message names a series or a year")

    def pattern_key_names_nothing() -> None:
        case = wonder_case(policies, "D76", 2020)
        case["evidence"]["statistics"]["counties_per_year"]["2020"] = 10**16
        message = str(rejection_of(lambda: run(case)))
        require("evidence schema violation" in message, "The schema defect was not the one reported")
        require("2020" not in message, "The message names a year")

    def message_fields_are_declared() -> None:
        found = message_expressions(Path(str(module.__file__)).read_text())
        require(not found - MESSAGE_FIELDS, f"Messages interpolate undeclared expressions: {sorted(found - MESSAGE_FIELDS)}")

    record("boundary_hides_a_value_error_from_inside", "R1", "family check raises ValueError holding the canary", None, inner_error(ValueError))
    record("boundary_hides_a_key_error_from_inside", "R1", "family check raises KeyError holding the canary", None, inner_error(KeyError))
    record("boundary_keeps_a_deadline_a_timeout", "R1", "family check raises TimeoutError during a valid check", None, deadline_stays_a_deadline)
    record("message_bls_missing_month_names_no_series_or_year", "R1", "one month removed", None, bls_month_names_nothing)
    record("message_schema_path_hides_pattern_key", "R6", "year key above its bound", None, pattern_key_names_nothing)
    record("message_fields_are_declared_names_and_counts", "all", "static scan of the module's messages", None, message_fields_are_declared)


def mini_repo(root: Path, name: str, policies: dict) -> Path:
    """Copy every bound plan, decision, lock and consumer file into a small repository so a test can edit them."""
    target = root / name
    for entry in policies.values():
        bound = [p["path"] for p in entry["extraction_map"]["plans"]] + [d["path"] for d in [entry["extraction_map"].get("decision")] if d]
        for path in [*bound, *(str(Path(p).with_suffix(".lock.json")) for p in bound), *entry["consumers"]["paths"]]:
            (target / path).parent.mkdir(parents=True, exist_ok=True)
            (target / path).write_bytes((REPO_ROOT / path).read_bytes())
    return target


def deep_change(node: Any) -> None:
    if isinstance(node, dict):
        for key, value in list(node.items()):
            if isinstance(value, dict | list):
                deep_change(value)
            else:
                node[key] = "changed"
        node["added"] = 1
    elif isinstance(node, list):
        for value in node:
            deep_change(value)
        node.append("added")


def authority_scenarios(policies: dict, root: Path, record: Callable[..., None]) -> None:
    bronze = REPO_ROOT / "config/lakehouse/bronze_tables.json"
    workbook_entry = policies["r5_hud_zip_county_workbook"]["extraction_map"]
    plan_path, decision_path = workbook_entry["plans"][0]["path"], workbook_entry["decision"]["path"]

    def returned_batch_changed() -> None:
        policy = policies["r3_census_acs_detailed_api"]
        case = detailed_case(policies)
        returned = module.frozen_inputs(policy, case["context"]["batch_id"])
        original = {key: returned["batch"][key] for key in ("year", "table")}
        returned["batch"].update(year=1900, table=DETAILED_OTHER_TABLE)
        case["evidence"].update(endpoint=policy["source"]["route"].replace("{year}", "1900"), request=census_detailed.request_for(returned["batch"]))
        try:
            run(case)
        finally:
            returned["batch"].update(original)

    def returned_inputs_detached() -> None:
        for policy in policies.values():
            for plan in policy["extraction_map"]["plans"]:
                batches = read_json(REPO_ROOT / plan["path"])["batches"]
                for batch in (batches[0], batches[-1]):
                    returned = module.frozen_inputs(policy, batch["id"])
                    pristine = copy.deepcopy(returned)
                    deep_change(returned)
                    try:
                        again = module.frozen_inputs(policy, batch["id"])
                    except ValueError:
                        again = None
                    require(again == pristine, "Changing a returned object changed the next resolution")

    def edit_json(path: Path, change: Callable[[dict], None]) -> None:
        body = read_json(path)
        change(body)
        path.write_bytes(encoded_json(body))

    def second_load_fails(name: str, edit: Callable[[Path], None]) -> Callable[[], None]:
        def action() -> None:
            mini = mini_repo(root, name, policies)
            with patch.object(module, "REPO_ROOT", mini):
                module.load_policies(CONFIG, bronze)
                edit(mini)
                module.load_policies(CONFIG, bronze)

        return action

    def lock_of(path: str) -> str:
        return str(Path(path).with_suffix(".lock.json"))

    def lock_changed_after_first_verify() -> None:
        lock_directory = root / "changed_lock"
        lock_directory.mkdir()
        edit = read_json(CONFIG / "projection_policies.lock.json")
        edit["entries"]["r1_bls_laus_api"] = "0" * 64
        (lock_directory / "projection_policies.lock.json").write_bytes(encoded_json(edit))
        case = bls_case(policies)
        run(case)
        with patch.object(module, "CONFIG_DIR", lock_directory):
            run(case)

    def frozen_inputs_of_edited_policy() -> None:
        case = bls_case(policies)
        widen_policy(case)
        module.frozen_inputs(case["policy"], case["context"]["batch_id"])

    def constants_are_immutable() -> None:
        mutable = [name for name in CONSTANTS if not isinstance(getattr(module, name), frozenset | tuple | MappingProxyType)]
        require(not mutable, f"Mutable module constants: {mutable}")

    def loads_are_independent() -> None:
        first = module.load_policies()
        first["r1_bls_laus_api"]["evidence"]["schema"]["additionalProperties"] = True
        require(
            canonical_hash(module.load_policies()["r1_bls_laus_api"]) == read_json(CONFIG / "projection_policies.lock.json")["entries"]["r1_bls_laus_api"],
            "Reload differs",
        )

    record(
        "authority_returned_batch_changed_then_verified",
        "R3",
        "year 1900 and another table written into the returned batch",
        "request differs",
        returned_batch_changed,
    )
    record("authority_returned_inputs_are_detached_copies", "all", "every level of a returned object changed", None, returned_inputs_detached)
    record(
        "authority_plan_edited_after_first_load",
        "R5",
        "plan file edited, lock untouched, between two loads",
        "plan hash",
        second_load_fails("plan_edit", lambda mini: edit_json(mini / plan_path, lambda body: body.update(scope="edited"))),
    )
    record(
        "authority_plan_lock_edited_after_first_load",
        "R5",
        "plan lock edited between two loads",
        "plan lock",
        second_load_fails("plan_lock_edit", lambda mini: edit_json(mini / lock_of(plan_path), lambda body: body.update(plan_sha256="0" * 64))),
    )
    record(
        "authority_decision_edited_after_first_load",
        "R5",
        "decision file edited between two loads",
        "decision hash",
        second_load_fails("decision_edit", lambda mini: edit_json(mini / decision_path, lambda body: body.update(source_id="edited"))),
    )
    record(
        "authority_decision_lock_edited_after_first_load",
        "R5",
        "decision lock edited between two loads",
        "decision lock",
        second_load_fails("decision_lock_edit", lambda mini: edit_json(mini / lock_of(decision_path), lambda body: body.update(decision_sha256="0" * 64))),
    )
    record(
        "authority_policy_lock_edited_after_first_verify",
        "R1",
        "policy lock edited between two verifications",
        "differs from the lock",
        lock_changed_after_first_verify,
    )
    record(
        "authority_frozen_inputs_of_edited_policy",
        "R1",
        "frozen_inputs called with a policy edited after locking",
        "differs from the lock",
        frozen_inputs_of_edited_policy,
    )
    record("authority_module_constants_are_immutable", "all", "none: constants are frozenset, tuple or read-only mapping", None, constants_are_immutable)
    record("authority_loaded_entries_are_independent", "all", "none: guard, an edited result does not reach the next load", None, loads_are_independent)


def generator_scenarios(root: Path, record: Callable[..., None]) -> None:
    def tracked() -> None:
        require(GENERATOR.is_file(), "Policy generator is not in the repository")

    def generator() -> Any:
        tracked()
        return importlib.import_module("scripts.acquisition.build_derivative_policies")

    def regenerates(directory: Path = CONFIG) -> None:
        for name, body in generator().render().items():
            require((directory / name).read_bytes() == body, f"{name} differs from its regeneration")

    def edited_copy(name: str) -> Callable[[], None]:
        def action() -> None:
            target = root / f"generated_{name}"
            target.mkdir()
            for each in GENERATED:
                (target / each).write_bytes((CONFIG / each).read_bytes())
            body = (target / name).read_bytes()
            (target / name).write_bytes(body[:-2] + b" " + body[-2:])
            regenerates(target)

        return action

    def renders_twice() -> None:
        require(generator().render() == generator().render(), "The generator output differs between runs")

    record("generator_is_in_the_repository", "all", "none: generator tracked next to the other builders", None, tracked)
    record("generator_regenerates_policy_and_lock_byte_for_byte", "all", "none: render() equals the committed policy and lock", None, regenerates)
    record("generator_output_is_repeatable", "all", "none: two renders are identical", None, renders_twice)
    record("generator_detects_policy_byte_changed", "all", "one byte added to a copy of the policy", "differs from its regeneration", edited_copy(GENERATED[0]))
    record("generator_detects_lock_byte_changed", "all", "one byte added to a copy of the lock", "differs from its regeneration", edited_copy(GENERATED[1]))


def _notes_not_utf8(case: dict) -> None:
    case["files"]["derived/export_notes.txt"] = b"\xff" + CANARY.encode()
    seal(case)


def run(case: dict) -> dict:
    return module.verify_derivative(case["policy"], case["files"], case["evidence"], case["context"])


def _mixed(case: dict) -> dict:
    def remove_state(rows: list[list[str]]) -> None:
        position = rows[0].index("state")
        for row in rows:
            row.pop(position)

    edit_rows(case, remove_state)
    return case


def _repeat_recorded(case: dict, count: int) -> dict:
    case["evidence"]["statistics"]["exact_repeat_rows_dropped"] = count
    return case


def _same_twice(run: Callable[[dict], Any], case: dict) -> None:
    require(run(case) == run(case) and run(copy.deepcopy(case)) == run(case), "Verification outcome differs between runs")


def _no_original_input() -> None:
    import inspect

    names = set(inspect.signature(module.verify_derivative).parameters)
    require(names == {"policy", "files", "evidence", "context"}, f"Unexpected verifier inputs {sorted(names)}")


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="derivative_policy_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "Collector derivative policies R1 to R6 and the derivative-only verifier",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "policy_sha256": canonical_hash(read_json(CONFIG / "projection_policies.json")),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_derivative_policy_e2e --output {args.output}",
        "boundary": "Synthetic inputs through the real producers. Real locked plans and policy files, no data files, registry, network or AWS.",
        "cleanup": "Temporary folder removed on exit.",
        "passed": sum(s["passed"] for s in scenarios),
        "total": len(scenarios),
        "scenarios": scenarios,
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(json.dumps({"status": report["status"], "passed": report["passed"], "total": report["total"], "artifact": str(args.output)}) + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
