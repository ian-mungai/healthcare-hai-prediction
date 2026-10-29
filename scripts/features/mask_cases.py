"""Execute the generated projections against synthetic source labels from the accepted spine contract."""

from __future__ import annotations

import importlib
import re
from datetime import date
from pathlib import Path
from typing import Any

from scripts.features import feature_schema, feature_validation

TABLES = ("feature_matrix_hospital_window", "feature_matrix_county_measures", "feature_matrix_county_context")
COUNTY = "c203__diabetes_ageadjprv__number"
CONTEXT = "c175__s1701_c01_001e__number"
RUCA = "c199__primary_ruca__number"
VALUES = {COUNTY: 9.5, CONTEXT: 12.5, RUCA: 1.0}
ATTRIBUTES = ("state_code", "county_fips", "provider_subtype_code", "control_type_code")
REF = re.compile(r"\{\{\s*ref\('([a-z][a-z0-9_]*)'\)\s*\}\}")
INSERT_SPINE = "insert into int_hospital_spine values ({marks})"
INSERT_LONG = "insert into {table} values (?, ?, ?, ?, ?, ?)"
COUNTY_SQL = "select spine_key, c203__diabetes_ageadjprv__number from feature_matrix_county_measures"
CONTEXT_SQL = "select spine_key, c175__s1701_c01_001e__number, c199__primary_ruca__number from feature_matrix_county_context"
CASES = {
    "own": ("pos_own", "pos_own", "pos_own", None, "00001"),
    "old_label": ("pos", "pos", "pos", None, None),
    "earlier": ("pos_earlier", "pos_earlier", "pos_earlier", None, None),
    "enrollment": ("enrollment", "hud_quarter", "pos_earlier", None, None),
    "hud": ("pos_earlier", "hud_quarter", "pos_earlier", None, None),
    "region": ("pos_own", "pos_own", "pos_own", "09110", "00001"),
    "other_zip": ("pos_own", "pos_own", "pos_own", None, "00002"),
}


def fixture(connection: Any, schema: dict[str, Any]) -> None:
    columns = {column["name"]: column["type"] for column in schema["tables"][TABLES[0]]["columns"] if column["role"] != "event_time"}
    columns.update(
        {
            "window_start": "date",
            "pos_period_end": "date",
            "state_source": "varchar",
            "county_source": "varchar",
            "classification_source": "varchar",
            "planning_region_fips": "varchar",
            "zip_code": "varchar",
        }
    )
    definitions = ", ".join(f"{feature_validation.quote(name)} {kind}" for name, kind in columns.items())
    connection.execute(f"create table int_hospital_spine ({definitions})")
    connection.execute("create table fct_pos_snapshot (ccn varchar, period_end date, zip_code varchar)")
    connection.execute(
        "create table validation_own_snapshot (spine_key varchar, state_code varchar, county_fips varchar, "
        "provider_subtype_code varchar, control_type_code varchar, zip5 varchar)"
    )
    marks = ", ".join("?" for _ in columns)
    for index, (key, (state, county, classification, region, own_zip)) in enumerate(CASES.items()):
        values = {
            "spine_key": key,
            "ccn": f"ZZ{index:04}",
            "window_year": 2023,
            "window_start": date(2023, 1, 1),
            "pos_period_end": date(2022, 12, 31),
            "state_code": "AL",
            "county_fips": "01001",
            "provider_subtype_code": "01",
            "control_type_code": "03",
            "bed_count": 100,
            "certified_bed_count": 90,
            "is_active": True,
            "has_pos_snapshot": True,
            "state_source": state,
            "county_source": county,
            "classification_source": classification,
            "planning_region_fips": region,
            "zip_code": "00001",
        }
        connection.execute(INSERT_SPINE.format(marks=marks), [values.get(name) for name in columns])
        connection.execute("insert into fct_pos_snapshot values (?, ?, ?)", [values["ccn"], values["pos_period_end"], own_zip])
        own = [values[name] if state == "pos_own" else None for name in ATTRIBUTES]
        connection.execute("insert into validation_own_snapshot values (?, ?, ?, ?, ?, ?)", [key, *own, own_zip])
    for name in TABLES[1:]:
        table = schema["tables"][name]
        long = feature_validation.quote(table["long_table"])
        connection.execute(
            f"create table {long} (spine_key varchar, measure_source varchar, measure_control varchar, field varchar, value_number double, value_text varchar)"
        )
        identities: dict[tuple[str, ...], float | None] = {}
        for column in table["columns"]:
            if pivot := column["pivot"]:
                identity = tuple(pivot["match"][key] for key in table["match_columns"])
                identities.setdefault(identity, None)
                if column["name"] in VALUES:
                    identities[identity] = VALUES[column["name"]]
        rows = [(key, *identity, value, None) for key in CASES for identity, value in identities.items()]
        connection.executemany(INSERT_LONG.format(table=long), rows)


def run(report: Any, root: Path) -> None:
    """Check the actual generated matrix queries and their existing validation on a complete synthetic key set."""
    duckdb = importlib.import_module("duckdb")
    schema = feature_schema.load_schema(root)
    connection = duckdb.connect()
    try:
        fixture(connection, schema)
        for name in TABLES:
            text = (root / feature_schema.MATRIX_FOLDER / f"{name}.sql").read_text()
            sql = REF.sub(lambda match: feature_validation.quote(match.group(1)), text)
            if "{{" in sql or "{%" in sql:
                raise RuntimeError("the generated projection contains an unsupported template")
            connection.execute(f"create table {feature_validation.quote(name)} as {sql}")
        own_keys = {"own", "region", "other_zip"}
        attributes = dict(
            (row[0], tuple(row[1:]))
            for row in connection.execute(
                "select spine_key, state_code, county_fips, provider_subtype_code, control_type_code from feature_matrix_hospital_window"
            ).fetchall()
        )
        wanted = {key: ("AL", "01001", "01", "03") if key in own_keys else (None,) * 4 for key in CASES}
        report.case("E14_own_snapshot", "DT10", "pos_own retains state, county and classification while other labels stay masked", attributes == wanted)
        county = dict(connection.execute(COUNTY_SQL).fetchall())
        eligible = {"own", "other_zip"}
        report.case(
            "E14_own_snapshot",
            "DT10",
            "pos_own retains county-family measures while planning regions and other labels stay masked",
            county == {key: 9.5 if key in eligible else None for key in CASES},
        )
        context = dict((row[0], tuple(row[1:])) for row in connection.execute(CONTEXT_SQL).fetchall())
        expected_context = {key: (12.5 if key in eligible else None, 1.0 if key in {"own", "region"} else None) for key in CASES}
        report.case("E14_own_snapshot", "DT10", "pos_own retains county context and preserves the planning-region and ZIP masks", context == expected_context)
        checker = feature_validation.Checker(connection, schema, {"publish": {"tables": {}}}, "projection fixture")
        for name in TABLES:
            table = schema["tables"][name]
            checker.wide_columns(name, table)
            checker.grain(name)
            checker.event_times(name)
            if name == TABLES[0]:
                checker.window_table(name, table)
            else:
                checker.key_set(name, table)
                checker.pivot(name, table)
                checker.kinds(name, table)
            checker.masks(name, table)
        failed = [f"{entry['table']}:{entry['id']}" for entry in checker.record if entry["level"] == "fail"]
        report.case(
            "E14_own_snapshot",
            "DT11",
            "the mask validation accepts the correctly projected pos_own rows",
            not failed,
            failed=failed,
            expectations=len(checker.record),
        )
    finally:
        connection.close()
