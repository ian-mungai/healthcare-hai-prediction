"""Regenerate config/acquisition/projection_policies.json and its lock from the tracked locked plans.

Regeneration command, from the repository root: .venv/bin/python -m scripts.acquisition.build_derivative_policies
Reads only tracked config/acquisition plans and locks and the WONDER query constants. run_derivative_policy_e2e.py requires the output of render() to
equal the committed policy and lock byte for byte. projection_policies.schema.json is written by hand and is not generated.
"""

import json
import sys
from typing import Any

from scripts.acquisition import wonder_export_contract as wonder
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json, require

CONFIG = REPO_ROOT / "config/acquisition"
NULL = "\\N"


def plan_binding(name: str) -> dict:
    plan, lock = read_json(CONFIG / f"{name}.json"), read_json(CONFIG / f"{name}.lock.json")
    require(lock["plan_sha256"] == canonical_hash(plan), name)
    return {"path": f"config/acquisition/{name}.json", "plan_sha256": lock["plan_sha256"]}


def bound_batches(name: str) -> list[dict]:
    out = []
    for batch in read_json(CONFIG / f"{name}.json")["batches"]:
        item = {"id": batch["id"], "table": batch["table"], "headers_sha256": canonical_hash(batch["headers"])}
        if "year" in batch:
            item["year"] = batch["year"]
        out.append(item)
    return out


def pattern(regex: str, tokens: list[str] | None = None, value_range: list[float] | None = None, refuse: list[int] | None = None) -> dict:
    domain: dict = {"kind": "pattern", "pattern": regex}
    if tokens:
        domain["tokens"] = tokens
    if value_range:
        domain["range"] = value_range
    if refuse:
        domain["refuse_range"] = refuse
    return domain


def text(allow_empty: bool, max_length: int) -> dict:
    return {"kind": "text", "allow_empty": allow_empty, "max_length": max_length}


def column(name: str, kind: str, domain: dict, null_token: str | None = None, derivation: str | None = None, source: str | None = None) -> dict:
    item = {"input": source or name, "output": name, "type": kind, "null_token": null_token, "domain": domain}
    if derivation:
        item["derivation"] = derivation
    return item


def rule(name: str, name_pattern: str, kind: str, domain: dict, null_token: str | None) -> dict:
    return {"name": name, "name_pattern": name_pattern, "type": kind, "null_token": null_token, "domain": domain}


SHA = {"type": "string", "pattern": "^[a-f0-9]{64}$", "maxLength": 64}
COUNT = {"type": "integer", "minimum": 0, "maximum": 9007199254740991}


def short(limit: int) -> dict:
    return {"type": "string", "maxLength": limit}


def closed(properties: dict, required: list[str] | None = None) -> dict:
    return {"type": "object", "additionalProperties": False, "properties": properties, "required": sorted(properties if required is None else required)}


def counts(*names: str) -> dict:
    return dict.fromkeys(names, COUNT)


NOT_ELIGIBLE = {"model_eligible": {"const": False}}
BLS_STATISTICS = closed(
    {
        **counts("rows", "requested_series", "available_series", "missing_value_rows", "footnoted_rows"),
        "availability_holds": {
            "type": "array",
            "maxItems": 1000,
            "items": closed(
                {
                    "series_id": {"type": "string", "pattern": "^LAUCN[0-9]{15}$", "maxLength": 20},
                    "year": COUNT,
                    "status": {"const": "availability_hold"},
                    "message": short(200),
                },
                ["series_id", "status", "message"],
            ),
        },
        **NOT_ELIGIBLE,
    },
    ["rows", "requested_series", "available_series", "missing_value_rows", "footnoted_rows", "availability_holds"],
)
PROFILE_COUNTS = ("rows", "columns", "puerto_rico_counties", "null_cells", "negative_tokens", "nonempty_annotations")
PROFILE_STATISTICS = closed({**counts(*PROFILE_COUNTS), **NOT_ELIGIBLE}, list(PROFILE_COUNTS))
DETAILED_STATISTICS = closed({**counts("rows", "columns", "puerto_rico_counties"), **NOT_ELIGIBLE}, ["rows", "columns", "puerto_rico_counties"])
RATIO_SUMS = closed(
    {
        name: closed(counts("zips_summing_to_one", "zips_with_zero_weight", "zips_outside_tolerance"))
        for name in ("res_ratio", "bus_ratio", "oth_ratio", "tot_ratio")
    }
)
HUD_PROPERTIES = {
    **counts("rows", "zips", "counties"),
    "states_and_territories": {"type": "array", "maxItems": 100, "items": {"type": "string", "pattern": "^[0-9]{2}$", "maxLength": 2}},
    "ratio_sums": RATIO_SUMS,
    "direction": {"const": "zip_to_county"},
    **NOT_ELIGIBLE,
}
HUD_REQUIRED = ["rows", "zips", "counties", "states_and_territories", "ratio_sums", "direction"]
HUD_API_STATISTICS = closed(HUD_PROPERTIES, HUD_REQUIRED)
HUD_WORKBOOK_STATISTICS = closed({**HUD_PROPERTIES, "exact_repeat_rows_dropped": COUNT}, HUD_REQUIRED)
WONDER_TOKENS = ("Suppressed", "Unreliable", "Not Available", "Missing")
WONDER_FLAGGED = (
    "Deaths",
    "Population",
    "Crude Rate",
    "Crude Rate Lower 95% Confidence Interval",
    "Crude Rate Upper 95% Confidence Interval",
    "Crude Rate Standard Error",
)
WONDER_QUERY = closed(
    {
        "dataset": short(200),
        "parameters": closed({"Year/Month": short(400), **{k: short(200) for k in wonder.FIXED_PARAMETERS}}, sorted(wonder.FIXED_PARAMETERS)),
        "query_date": {"type": ["string", "null"], "maxLength": 64},
        "citation": {"type": ["string", "null"], "maxLength": 1000},
    }
)
WONDER_STATISTICS = closed(
    {
        "rows": COUNT,
        "years": {"type": "array", "maxItems": 100, "items": COUNT},
        "counties_per_year": {"type": "object", "additionalProperties": False, "patternProperties": {"^[0-9]{4}$": COUNT}},
        "flags": closed({name: closed(counts(*WONDER_TOKENS), []) for name in WONDER_FLAGGED}, []),
        "query": WONDER_QUERY,
        **NOT_ELIGIBLE,
    },
    ["rows", "years", "counties_per_year", "flags", "query"],
)
BLS_REQUEST = closed(
    {
        "seriesid": {"type": "array", "maxItems": 50, "items": {"type": "string", "pattern": "^LAUCN[0-9]{15}$", "maxLength": 20}},
        "startyear": {"type": "string", "pattern": "^[0-9]{4}$", "maxLength": 4},
        "endyear": {"type": "string", "pattern": "^[0-9]{4}$", "maxLength": 4},
        "annualaverage": {"const": True},
        "catalog": {"const": False},
        "calculations": {"const": False},
        "aspects": {"const": False},
    }
)
CENSUS_REQUEST = closed(
    {
        "get": {"type": "string", "pattern": "^group\\([A-Z0-9]{3,8}\\)$", "maxLength": 16},
        "for": {"const": "county:*"},
        "in": {"type": "string", "pattern": "^state:[0-9*]{1,3}$", "maxLength": 9},
    }
)
HUD_REQUEST = closed(
    {
        "type": {"type": "string", "pattern": "^[0-9]$", "maxLength": 1},
        "query": {"const": "All"},
        "year": {"type": "string", "pattern": "^[0-9]{4}$", "maxLength": 4},
        "quarter": {"type": "string", "pattern": "^[1-4]$", "maxLength": 1},
    }
)


def evidence(
    path: str,
    raw_field: str,
    required: list[str],
    schema_properties: dict,
    origin: list[str] | None = None,
    optional: dict | None = None,
    request_function: str | None = None,
) -> dict:
    names = sorted({raw_field, "statistics", "derivative_sha256", *required})
    item: dict[str, Any] = {
        "path": path,
        "raw_sha256_field": raw_field,
        "derivative_sha256_field": "derivative_sha256",
        "required": names,
        "forbidden": ["body", "body_hex", "raw"],
    }
    if origin:
        item["origin_urls"] = origin
    if request_function:
        item["request_function"] = request_function
    properties = {"derivative_sha256": SHA, raw_field: SHA, **schema_properties}
    require(set(names) <= set(properties), "Evidence schema lacks a required property")
    item["schema"] = closed({**properties, **(optional or {})}, names)
    return item


API_PROPERTIES = {"endpoint": short(200), "retrieved_at_utc": short(64), "sha256": SHA, "bytes": COUNT, "http_status": {"const": 200}}
API_REQUIRED = ["endpoint", "retrieved_at_utc", "sha256", "bytes", "http_status", "request"]
PROOF_PROPERTIES = {
    "file_name": short(256),
    "origin_urls": {"type": "array", "maxItems": 1, "items": short(200)},
    "created_at_utc": short(64),
    "sha256": SHA,
    "bytes": COUNT,
}
PROOF_REQUIRED = ["bytes", "created_at_utc", "file_name", "origin_urls"]


UNIT = pattern(r"[0-9]+(\.[0-9]+)?([Ee][-+]?[0-9]+)?", value_range=[0, 1])
CENSUS_RULES = [
    rule("geo_id", "GEO_ID", "identifier", pattern(r"0500000US[0-9]{5}"), None),
    rule("name", "NAME", "text", text(False, 256), None),
    rule("state", "state", "identifier", pattern(r"[0-9]{2}"), None),
    rule("county", "county", "identifier", pattern(r"[0-9]{3}"), None),
    rule("estimate", r"[A-Z0-9]+_[0-9]{3,4}(E|M|PE|PM)", "text", text(True, 64), NULL),
    rule("annotation", r"[A-Z0-9]+_[0-9]{3,4}(EA|MA|PEA|PMA)", "text", text(True, 64), NULL),
]
HUD_COLUMNS = {
    "bus_ratio": column("bus_ratio", "decimal", UNIT),
    "city": column("city", "text", text(True, 200)),
    "geoid": None,
    "oth_ratio": column("oth_ratio", "decimal", UNIT),
    "res_ratio": column("res_ratio", "decimal", UNIT),
    "state": column("state", "text", text(True, 64)),
    "tot_ratio": column("tot_ratio", "decimal", UNIT),
    "zip": column("zip", "identifier", pattern(r"[0-9]{5}")),
}


def hud_layout(name: str, fields: list[str], geoid: str) -> dict:
    columns = [column("geoid", "identifier", pattern(geoid)) if f == "geoid" else HUD_COLUMNS[f] for f in sorted(fields)]
    return {"name": name, "columns": columns}


def entry(**kwargs: object) -> dict:
    kwargs["disposition"] = "project"
    return kwargs


def wonder_columns() -> list[dict]:
    flags = ["Suppressed", "Unreliable", "Not Available", "Missing"]
    number = r"[0-9]+(\.[0-9]+)?"
    return [
        column("Notes", "text", {"kind": "enum", "values": [""]}),
        column("County", "text", text(False, 256)),
        column("County Code", "identifier", pattern(r"[0-9]{5}")),
        column("Year", "identifier", pattern(r"[0-9]{4} ?")),
        column("Year Code", "identifier", pattern(r"[0-9]{4}")),
        column("Deaths", "integer", pattern(r"[0-9]+", ["Suppressed", "Missing"], refuse=[1, 9])),
        column("Population", "integer", pattern(r"[0-9]+", ["Not Available", "Missing"])),
        column("Crude Rate", "decimal", pattern(number, flags)),
        column("Crude Rate Lower 95% Confidence Interval", "decimal", pattern(number, [f for f in flags if f != "Unreliable"])),
        column("Crude Rate Upper 95% Confidence Interval", "decimal", pattern(number, [f for f in flags if f != "Unreliable"])),
        column("Crude Rate Standard Error", "decimal", pattern(number, [f for f in flags if f != "Unreliable"])),
    ]


def build() -> dict:
    hud_plan = read_json(CONFIG / "hud_api_plan.json")
    require(hud_plan["result_fields"] == sorted(HUD_COLUMNS), "HUD result fields moved")
    six = [f for f in sorted(HUD_COLUMNS) if f not in ("city", "state")]
    entries = [
        entry(
            id="r1_bls_laus_api",
            recipe="R1",
            source={
                "family": "BLS Local Area Unemployment Statistics API v2",
                "layout": "county series LAUCN..03 to 06, monthly values and annual average",
                "route": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                "command": "collect_bls_api",
            },
            input={"format": "json", "encoding": "utf-8", "max_bytes": 33554432},
            extraction_map={"plans": [plan_binding("bls_api_plan")]},
            parser={"module": "scripts.acquisition.bls_api_contract", "function": "validate_response", "version": 1},
            consumers={
                "bronze_table": "bls_laus",
                "paths": ["dbt/models/silver/staging/stg_bls_laus.sql", "dbt/models/silver/intermediate/int_bls_county_series.sql"],
                "dropped_bronze_tables": [],
            },
            files=[
                {
                    "path": "derived/observations.csv",
                    "role": "table",
                    "max_bytes": 16777216,
                    "max_rows": 100000,
                    "key": ["seriesID", "year", "period"],
                    "order": ["seriesID", "year", "period"],
                    "layouts": [
                        {
                            "name": "bls_laus_v1",
                            "columns": [
                                column("seriesID", "identifier", pattern(r"LAUCN[0-9]{5}000000000[3-6]")),
                                column("county_fips", "identifier", pattern(r"[0-9]{5}"), derivation="seriesID[5:10]", source="seriesID"),
                                column("measure_code", "identifier", pattern(r"0[3-6]"), derivation="seriesID[-2:]", source="seriesID"),
                                column("year", "integer", pattern(r"[0-9]{4}", value_range=[1990, 2025])),
                                column("period", "identifier", pattern(r"M(0[1-9]|1[0-3])")),
                                column("periodName", "text", text(False, 64)),
                                column("value", "decimal", pattern(r"[0-9]+(\.[0-9]+)?", ["-"])),
                                column("footnotes", "json", {"kind": "footnotes"}),
                            ],
                        }
                    ],
                }
            ],
            evidence=evidence(
                "evidence/request_response.json",
                "response_sha256",
                API_REQUIRED,
                {**API_PROPERTIES, "request": BLS_REQUEST, "statistics": BLS_STATISTICS},
                request_function="request_for",
            ),
            family_check="bls_laus",
        ),
        entry(
            id="r2_census_acs_profile_api",
            recipe="R2",
            source={
                "family": "Census ACS 2005-2009 five-year profile API",
                "layout": "group(DP02..DP05) and group(DP02PR) county tables",
                "route": "https://api.census.gov/data/2009/acs/acs5/profile",
                "command": "collect_census_acs_api",
            },
            input={"format": "json", "encoding": "utf-8", "max_bytes": 67108864},
            extraction_map={"plans": [plan_binding("census_acs_api_plan"), plan_binding("census_acs_api_plan_dp02pr")]},
            parser={"module": "scripts.acquisition.census_acs_api_contract", "function": "validate_response", "version": 1},
            consumers={"bronze_table": "acs_api_profile", "paths": [], "dropped_bronze_tables": []},
            files=[
                {
                    "path": "derived/observations.csv",
                    "role": "table",
                    "max_bytes": 67108864,
                    "max_rows": 3300,
                    "key": ["state", "county"],
                    "order": ["state", "county"],
                    "layouts": [
                        {
                            "name": "census_profile_locked_headers",
                            "bound_batches": bound_batches("census_acs_api_plan") + bound_batches("census_acs_api_plan_dp02pr"),
                            "rules": CENSUS_RULES,
                        }
                    ],
                }
            ],
            evidence=evidence(
                "evidence/request_response.json",
                "response_sha256",
                API_REQUIRED,
                {**API_PROPERTIES, "request": CENSUS_REQUEST, "statistics": PROFILE_STATISTICS},
                request_function="request_for",
            ),
            family_check="census_profile",
        ),
        entry(
            id="r3_census_acs_detailed_api",
            recipe="R3",
            source={
                "family": "Census ACS five-year detailed tables API",
                "layout": "group(B17001, B16001, B16004) county tables, 21 table-year batches",
                "route": "https://api.census.gov/data/{year}/acs/acs5",
                "command": "collect_census_acs_detailed",
            },
            input={"format": "json", "encoding": "utf-8", "max_bytes": 67108864},
            extraction_map={"plans": [plan_binding("census_acs_detailed_plan")]},
            parser={"module": "scripts.acquisition.census_acs_detailed_contract", "function": "validate_response", "version": 1},
            consumers={"bronze_table": "acs_api_detailed", "paths": [], "dropped_bronze_tables": []},
            files=[
                {
                    "path": "derived/observations.csv",
                    "role": "table",
                    "max_bytes": 67108864,
                    "min_rows": 3100,
                    "max_rows": 3300,
                    "key": ["state", "county"],
                    "order": ["state", "county"],
                    "layouts": [{"name": "census_detailed_locked_headers", "bound_batches": bound_batches("census_acs_detailed_plan"), "rules": CENSUS_RULES}],
                }
            ],
            evidence=evidence(
                "evidence/request_response.json",
                "sha256",
                [n for n in API_REQUIRED if n != "sha256"],
                {**API_PROPERTIES, "request": CENSUS_REQUEST, "statistics": DETAILED_STATISTICS},
                request_function="request_for",
            ),
            family_check="census_detailed",
        ),
        entry(
            id="r4_hud_usps_crosswalk_api",
            recipe="R4",
            source={
                "family": "HUD USPS ZIP-to-county crosswalk API",
                "layout": "type 2 zip-county, query All, 2021 Q1 to 2025 Q4",
                "route": "https://www.huduser.gov/hudapi/public/usps",
                "command": "collect_hud_api",
            },
            input={"format": "json", "encoding": "utf-8", "max_bytes": 268435456},
            extraction_map={"plans": [plan_binding("hud_api_plan"), plan_binding("hud_api_plan_2021q2_2025q4")]},
            parser={"module": "scripts.acquisition.hud_api_contract", "function": "validate_response", "version": 1},
            consumers={
                "bronze_table": "hud_zip_county",
                "paths": [
                    "dbt/models/silver/staging/stg_hud_zip_county.sql",
                    "dbt/models/silver/intermediate/int_hud_zip_county_quarters.sql",
                    "dbt/models/silver/intermediate/int_hud_zip_county_holds.sql",
                ],
                "dropped_bronze_tables": [],
            },
            files=[
                {
                    "path": "derived/crosswalk.csv",
                    "role": "table",
                    "max_bytes": 67108864,
                    "max_rows": 250000,
                    "key": ["zip", "geoid"],
                    "order": ["zip", "geoid"],
                    "layouts": [
                        hud_layout("hud_api_eight_field", hud_plan["result_fields"], r"[0-9]{5}|60|64|68|70"),
                        hud_layout("hud_api_six_field", six, r"[0-9]{5}|60|64|68|70"),
                    ],
                }
            ],
            evidence=evidence(
                "evidence/request_response.json",
                "sha256",
                [n for n in API_REQUIRED if n != "sha256"],
                {**API_PROPERTIES, "request": HUD_REQUEST, "statistics": HUD_API_STATISTICS},
                request_function="request_for",
            ),
            family_check="hud_crosswalk",
        ),
        entry(
            id="r5_hud_zip_county_workbook",
            recipe="R5",
            source={
                "family": "HUD USPS ZIP-COUNTY crosswalk workbooks",
                "layout": "one worksheet, six columns, 2010 Q1 to 2020 Q4",
                "route": "https://www.huduser.gov/apps/public/uspscrosswalk/home",
                "command": "store_hud_xlsx",
            },
            input={"format": "xlsx", "encoding": "utf-8", "max_bytes": 16777216},
            extraction_map={
                "plans": [plan_binding("hud_xlsx_plan_2010q1_2020q4")],
                "decision": {
                    "path": "config/acquisition/hud_xlsx_exact_repeats_2014q2.json",
                    "decision_sha256": read_json(CONFIG / "hud_xlsx_exact_repeats_2014q2.lock.json")["decision_sha256"],
                },
            },
            parser={"module": "scripts.acquisition.hud_xlsx_contract", "function": "validate_workbook", "version": 1},
            consumers={
                "bronze_table": "hud_zip_county",
                "paths": [
                    "dbt/models/silver/staging/stg_hud_zip_county.sql",
                    "dbt/models/silver/intermediate/int_hud_zip_county_quarters.sql",
                    "dbt/models/silver/intermediate/int_hud_zip_county_holds.sql",
                ],
                "dropped_bronze_tables": ["hud_zip_county_sheet_rows"],
            },
            files=[
                {
                    "path": "derived/crosswalk.csv",
                    "role": "table",
                    "max_bytes": 16777216,
                    "max_rows": 250000,
                    "key": ["zip", "geoid"],
                    "order": ["zip", "geoid"],
                    "layouts": [hud_layout("hud_workbook_six_field", six, r"[0-9]{5}")],
                }
            ],
            evidence=evidence(
                "evidence/download_proof.json",
                "sha256",
                PROOF_REQUIRED,
                {**PROOF_PROPERTIES, "statistics": HUD_WORKBOOK_STATISTICS},
                origin=["https://www.huduser.gov/"],
                optional={"saved_by": short(64)},
            ),
            family_check="hud_workbook",
        ),
        entry(
            id="r6_wonder_county_year",
            recipe="R6",
            source={
                "family": "CDC WONDER underlying cause of death county exports",
                "layout": "county and year, deaths, population, crude rate with limits and standard error",
                "route": "https://wonder.cdc.gov/controller/datarequest/{database}",
                "command": "store_wonder_export",
            },
            input={"format": "tsv_with_notes", "encoding": "utf-8", "max_bytes": 67108864},
            extraction_map={"plans": [plan_binding("wonder_export_plan")]},
            parser={"module": "scripts.acquisition.wonder_export_contract", "function": "validate_export", "version": 1},
            consumers={
                "bronze_table": "wonder_county_mortality",
                "paths": ["dbt/models/silver/staging/stg_wonder_county_mortality.sql", "dbt/models/silver/intermediate/int_wonder_county_deaths.sql"],
                "dropped_bronze_tables": ["wonder_exports_text_lines"],
            },
            files=[
                {
                    "path": "derived/county_year.csv",
                    "role": "table",
                    "max_bytes": 67108864,
                    "max_rows": 250000,
                    "key": ["County Code", "Year Code"],
                    "order": ["County Code", "Year Code"],
                    "layouts": [{"name": "wonder_county_year_v1", "columns": wonder_columns()}],
                },
                {"path": "derived/export_notes.txt", "role": "notes", "max_bytes": 1048576},
            ],
            evidence=evidence(
                "evidence/download_proof.json",
                "sha256",
                [*PROOF_REQUIRED, "notes_sha256", "query"],
                {**PROOF_PROPERTIES, "notes_sha256": SHA, "query": WONDER_QUERY, "statistics": WONDER_STATISTICS},
                origin=["https://wonder.cdc.gov/controller/datarequest/{database}"],
                optional={"saved_by": short(64)},
            ),
            family_check="wonder_county_year",
            query={"databases": {k: v["title"] for k, v in wonder.DATABASES.items()}, "fixed_parameters": wonder.FIXED_PARAMETERS},
        ),
    ]
    return {"version": 1, "entries": entries}


def render() -> dict[str, bytes]:
    """Return the bytes of the policy file and its lock, keyed by file name."""
    policies = build()
    lock = {"policies_sha256": canonical_hash(policies), "entries": {e["id"]: canonical_hash(e) for e in policies["entries"]}}
    return {"projection_policies.json": encoded_json(policies), "projection_policies.lock.json": encoded_json(lock)}


def main() -> None:
    rendered = render()
    for name, body in rendered.items():
        (CONFIG / name).write_bytes(body)
    sys.stdout.write(json.dumps({name: canonical_hash(json.loads(body)) for name, body in rendered.items()}, indent=2) + "\n")


if __name__ == "__main__":
    main()
