"""Synthetic gold rows for the features build, and the gold extract folder a run starts from. Synthetic only: fake six-character CCNs, no study data.

    <tool python> -m scripts.features.feature_fixture make-input <shell.duckdb> <folder> <run ID> <mode> <version>

The shell database is the one ``scripts.lakehouse.feature_columns.build_shell`` makes (empty gold tables, the control bridge and the seeds). ``make-input``
fills the gold facts the feature models read with the rows below, writes every gold table as Parquet under ``<folder>/gold_input/<table>/`` with a manifest of
file digests (the shape ``features_input read`` leaves), and writes ``<folder>/gold_pins.json`` with a fake committed gold run. DuckDB runs in the SQLFluff tool
environment, so the module imports it at call time.

The fixture plants the cases of design 6.2: per family a source period that ends on ``window_start`` and one that ends after it; a control with no data; a
split ZIP; the oldest and newest window; a 17-significant-digit double; and the future-attribute rows that the projection masks must null (a window with no
own snapshot whose later snapshot has another state, county and classification; an enrollment row that starts before the window and ends inside it; a
Connecticut window with a complete own snapshot and a HUD planning region; a ZIP that only a later snapshot gives). Version 2 changes, adds and removes rows.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

FIRST_YEAR = 2022
GOLD_RUN = "20261014T040000Z-0000ab01"
SNAPSHOT_BASE = 7_000_000_000_000_000_000
HAI_PARTS = ("SIR", "CIL", "CIU", "OBSERVED", "PREDICTED")
LONG_DIGITS = 0.12345678901234567
# The planted spine keys by scenario (version 1 and 2 share them unless a scenario is removed in version 2).
PLANTED = {
    "plain": "990001:2023",
    "later_snapshot": "990002:2023",
    "enrollment_only": "990003:2023",
    "connecticut_complete": "990004:2022",
    "zip_from_later_snapshot": "990005:2022",
    "period_on_window_start": "990001:2022",
    "oldest": "990001:2022",
    "newest": "990001:2024",
}
Rows = dict[str, list[dict[str, Any]]]
INSERT_SQL = 'insert into main."{table}" ({names}) values ({marks})'
COPY_SQL = "copy (select * from main.\"{table}\") to '{path}' (format parquet)"
PLACEHOLDER_SQL = 'create table main."{table}" (placeholder varchar)'


def window(ccn: str, year: int, version: int) -> list[dict[str, Any]]:
    """The HAI outcome rows of one calendar-year window; SIR varies with the data version."""
    scale = 1.0 if version == 1 else 1.1
    rows = []
    for part in HAI_PARTS:
        rows.append(
            {
                "entity_id": ccn,
                "measure_id": f"HAI_1_{part}",
                "window_start": date(year, 1, 1),
                "window_end": date(year, 12, 31),
                "measure_name": "Central line infections",
                "score": f"{(0.5 + (year - FIRST_YEAR) * 0.1) * scale:.3f}" if part == "SIR" else "1",
                "release_date": date(2026, 1, 15),
                "release_file_count": 1,
                "window_key": f"{ccn}:{year}:{part}",
            }
        )
    return rows


def snapshot(ccn: str, period_end: date, **values: Any) -> dict[str, Any]:
    """One POS snapshot row."""
    base = {
        "ccn": ccn,
        "provider_number": ccn,
        "period_start": date(period_end.year, 1, 1),
        "period_end": period_end,
        "member_sha256": f"{ccn}{period_end.isoformat()}",
        "snapshot_key": f"{ccn}:{period_end.isoformat()}",
        "state_code": "AL",
        "zip_code": "35005",
        "county_fips": "01001",
        "provider_subtype_code": "01",
        "control_type_code": "03",
        "bed_count": 100,
        "certified_bed_count": 90,
        "is_active": True,
        "county_source": "pos",
        "ssa_state_code": None,
    }
    base.update(values)
    return base


def gold_rows(version: int = 1) -> Rows:
    """The gold rows of one fixture version (1 or 2)."""
    rows: Rows = {name: [] for name in TABLES}
    hospitals: dict[str, list[int]] = {
        "990001": [2022, 2023, 2024],
        "990002": [2023],
        "990003": [2023],
        "990004": [2022, 2023],
        "990005": [2022],
    }
    if version == 2:
        hospitals["990006"] = [2023, 2024]
        del hospitals["990003"]
    for ccn, years in hospitals.items():
        for year in years:
            rows["fct_hai_hospital_window"] += window(ccn, year, version)
    # Own snapshots: one per year end for 990001, so every window has its own.
    for year in (2021, 2022, 2023):
        rows["fct_pos_snapshot"].append(snapshot("990001", date(year, 12, 31), bed_count=100 + year - 2021 + (10 if version == 2 else 0)))
    # 990002: only a later snapshot (ends inside its 2023 window) with another state, county, ZIP and classification.
    rows["fct_pos_snapshot"].append(
        snapshot("990002", date(2023, 12, 31), state_code="TX", zip_code="75001", county_fips="48001", provider_subtype_code="02", control_type_code="05")
    )
    # 990004: Connecticut, complete own snapshots, and a HUD planning region for its ZIP.
    for year in (2021, 2022):
        rows["fct_pos_snapshot"].append(snapshot("990004", date(year, 12, 31), state_code="CT", zip_code="06001", county_fips="09001"))
    rows["bridge_zip_county"].append(
        {
            "quarter_label": "2021Q1",
            "quarter_start_date": date(2021, 1, 1),
            "quarter_end_date": date(2021, 3, 31),
            "zip_code": "06001",
            "county_fips": "09110",
            "county_scope": "connecticut",
            "usps_state": "CT",
            "res_ratio": 1.0,
            "tot_ratio": 1.0,
            "hud_row_key": "ct-06001-09110-2021q1",
            "is_connecticut": True,
        }
    )
    # 990005: the own snapshot has no ZIP; only a later snapshot gives one.
    rows["fct_pos_snapshot"].append(snapshot("990005", date(2021, 12, 31), state_code="FL", zip_code=None, county_fips="12001"))
    rows["fct_pos_snapshot"].append(snapshot("990005", date(2023, 6, 30), state_code="FL", zip_code="33101", county_fips="12001"))
    # 990003: enrollment only; the row starts before the window and ends inside it.
    if version == 1:
        rows["fct_enrollment_release"].append(
            {
                "enrollment_id": "I20000000000001",
                "state": "GA",
                "zip_code": "30301",
                "ccn": "990003",
                "period_start": date(2022, 6, 1),
                "period_end": date(2023, 6, 1),
                "enrollment_key": "990003-e1",
                "release_id": "r1",
            }
        )
    # Care Compare overall rating (C284): one release before the 2022 window, one dated on its start.
    for released, rating in ((date(2021, 6, 1), 4), (date(2022, 1, 1), 5)):
        rows["fct_hgi_release"].append(
            {
                "ccn": "990001",
                "release_date": released,
                "overall_rating": rating,
                "overall_rating_text": str(rating),
                "release_file_count": 1,
                "release_key": f"990001-{released.isoformat()}",
                "member_sha256": f"hgi-{released.isoformat()}",
                "is_label_held": False,
            }
        )
    # Cost report (C001 beds as a 17-digit double, C002 type of control as a code): one report ends the day before 2022, one on the window start of 2022.
    for number, ending, beds in (("R1", date(2021, 12, 31), LONG_DIGITS + 120), ("R2", date(2022, 1, 1), 999.0)):
        rows["fct_cost_report"].append(
            {
                "rpt_rec_num": number,
                "ccn": "990001",
                "provider_ccn_raw": "990001",
                "period_begin": date(ending.year - 1, ending.month, min(ending.day, 28)),
                "period_end": ending,
                "file_fiscal_year": ending.year,
                "fiscal_year": ending.year,
                "reporting_days": 365,
                "number_of_beds": beds,
                "type_of_control": "2",
                "provider_type": "1",
                "member_sha256": f"cost-{number}",
            }
        )
    # ONC (C071 to C074).
    rows["fct_onc_ehr_product"].append(
        {
            "ccn": "990001",
            "facility_id": "F1",
            "meets_criteria_for_promoting_interoperability_of_ehrs": True,
            "start_date": date(2021, 1, 1),
            "end_date": date(2021, 12, 30),
            "program_year": 2021,
            "cehrt_id": "CEHRT1",
            "chpl_id": "CHP1",
            "product_database_id": "P1",
            "developer_name": "Vendor A",
            "linkage_key": "onc-1",
            "member_sha256": "onc-1",
        }
    )
    # County measures (PLACES C203) and county context (ACS C175, RUCA by ZIP): the counties of every scenario, two data years.
    for county in ("01001", "48001", "09001", "09110", "12001"):
        for data_year, value in ((2020, 9.5), (2022, 11.25)):
            rows["fct_places_county_value"].append(
                {
                    "edition": "2022",
                    "measureid": "DIABETES",
                    "datavaluetypeid": "AgeAdjPrv",
                    "data_value_published": str(value),
                    "data_year": data_year,
                    "county_fips": county,
                    "data_value": value,
                    "is_all_states": True,
                    "places_row_key": f"{county}-{data_year}",
                    "member_sha256": "places",
                }
            )
        rows["fct_acs_county_value"].append(
            {
                "bronze_table": "acs_s1701",
                "vintage": "2020",
                "concept_id": "S1701_C01_001E",
                "column_name": "S1701_C01_001E",
                "county_fips": county,
                "value_published": "12.5",
                "value_number": 12.5,
                "acs_value_key": f"acs-{county}",
                "member_sha256": "acs",
            }
        )
    for zip_code in ("35005", "33101", "75001", "06001"):
        rows["fct_ruca_code"].append(
            {
                "vintage": "2020",
                "geography_type": "zip",
                "geography_id": zip_code,
                "primary_ruca": "1",
                "secondary_ruca": "1.1",
                "ruca_row_key": f"ruca-{zip_code}",
                "member_sha256": "ruca",
            }
        )
    for vintage in ("2010", "2020"):
        for county, neighbor, isolated in (("01001", "01003", False), ("01003", "01001", False), ("01005", "01005", True)):
            rows["bridge_county_neighbor"].append(
                {
                    "member_sha256": "adjacency-" + vintage,
                    "source_row_number": len(rows["bridge_county_neighbor"]) + 1,
                    "vintage": vintage,
                    "county_fips": county,
                    "neighbor_fips": neighbor,
                    "shared_border_length_m": None if isolated else 12.5 * version,
                    "edge_key": f"{vintage}:{county}:{neighbor}",
                    "is_self_link": county == neighbor,
                    "is_isolated": isolated,
                    "is_connecticut": False,
                }
            )
    for measure in ("PSI_90", "PSI_13"):
        for year in range(2020, 2025):
            rows["fct_complications_deaths_window"].append(
                {
                    "entity_id": "990001",
                    "measure_id": measure,
                    "published_measure_id": measure,
                    "window_start": date(year, 1, 1),
                    "window_end": date(year, 12, 31),
                    "score": str(year - 2019.5),
                    "release_date": date(2026, 1, 15),
                    "window_key": f"psi:{measure}:{year}",
                }
            )
        for year in (2022, 2023, 2024):
            rows["fct_complications_deaths_window"].append(
                {
                    "entity_id": "990001",
                    "measure_id": measure,
                    "published_measure_id": measure,
                    "window_start": date(year - 1, 1, 1),
                    "window_end": date(year, 1, 1),
                    "score": "999",
                    "release_date": date(2026, 1, 15),
                    "window_key": f"psi-boundary:{measure}:{year}",
                }
            )
        rows["fct_complications_deaths_window"].append(
            {
                "entity_id": "990004",
                "measure_id": measure,
                "published_measure_id": measure,
                "window_start": date(2020, 1, 1),
                "window_end": date(2020, 12, 31),
                "score": "2.75",
                "release_date": date(2026, 1, 15),
                "window_key": f"psi-gap:{measure}",
            }
        )
        rows["fct_complications_deaths_window"].append(
            {
                "entity_id": "990002",
                "measure_id": measure,
                "published_measure_id": measure,
                "window_start": date(2023, 1, 1),
                "window_end": date(2023, 12, 31),
                "score": "999",
                "release_date": date(2026, 1, 15),
                "window_key": f"psi-future:{measure}",
            }
        )
        rows["fct_validation_window_hold"].append(
            {
                "entity_id": "990005",
                "measure_id": measure,
                "window_start": date(2021, 1, 1),
                "window_end": date(2021, 12, 31),
                "hold_reason": "synthetic hold",
                "row_count": 1,
            }
        )
    rows["fct_hac_program_year"].append(
        {
            "ccn": "990001",
            "fiscal_year": 2025,
            "payment_reduction_published": "Yes",
            "is_payment_reduced": True,
            "total_hac_score": "7.0",
            "total_hac_score_number": 7.0,
            "hai_measures_start_date": date(2022, 1, 1),
            "hai_measures_end_date": date(2023, 12, 31),
            "release_date": date(2026, 1, 15),
        }
    )
    return rows


TABLES = (
    "bridge_county_neighbor",
    "fct_complications_deaths_window",
    "fct_hac_program_year",
    "fct_validation_window_hold",
    "fct_hai_hospital_window",
    "fct_pos_snapshot",
    "bridge_zip_county",
    "fct_enrollment_release",
    "fct_hgi_release",
    "fct_cost_report",
    "fct_onc_ehr_product",
    "fct_places_county_value",
    "fct_acs_county_value",
    "fct_ruca_code",
)


def insert(connection: Any, table: str, rows: list[dict[str, Any]]) -> None:
    """Insert rows by column name; columns a row does not give stay null. A name the table lacks is an error."""
    if not rows:
        return
    names = [
        row[0]
        for row in connection.execute("select column_name from information_schema.columns where table_schema = 'main' and table_name = ?", [table]).fetchall()
    ]
    for row in rows:
        unknown = sorted(set(row) - set(names))
        if unknown:
            raise ValueError(f"{table} has no column {unknown[0]}")
    listed = sorted({name for row in rows for name in row})
    quoted = ", ".join(f'"{name}"' for name in listed)
    marks = ", ".join("?" for _ in listed)
    connection.executemany(INSERT_SQL.format(table=table, names=quoted, marks=marks), [[row.get(name) for name in listed] for row in rows])


def sha256_file(path: Path) -> str:
    """SHA-256 of a file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_input(shell: Path, folder: Path, run: str, mode: str, version: int, contract_tables: Sequence[str]) -> dict[str, Any]:
    """Fill the gold facts, write every named table as Parquet with the manifest, and the pin. Returns the pin record."""
    duckdb: Any = importlib.import_module("duckdb")
    connection = duckdb.connect(str(shell))
    try:
        for table, rows in gold_rows(version).items():
            insert(connection, table, rows)
        input_folder = folder / "gold_input"
        input_folder.mkdir(parents=True)
        existing = {row[0] for row in connection.execute("select table_name from information_schema.tables where table_schema = 'main'").fetchall()}
        files = {}
        snapshot_set = {}
        for index, name in enumerate(sorted(contract_tables)):
            target = input_folder / name
            target.mkdir()
            if name not in existing:
                connection.execute(PLACEHOLDER_SQL.format(table=name))
            connection.execute(COPY_SQL.format(table=name, path=target / "part-00000.parquet"))
            files[f"{name}/part-00000.parquet"] = sha256_file(target / "part-00000.parquet")
            snapshot_set[name] = SNAPSHOT_BASE + index + 1
    finally:
        connection.close()
    pin = {
        "schema": 1,
        "run_id": run,
        "mode": mode,
        "namespace": "gold_e2e" if mode == "publish_fixture" else "gold",
        "source_run_id": GOLD_RUN,
        "snapshot_set": snapshot_set,
    }
    pin_text = json.dumps(pin, indent=2, sort_keys=True) + "\n"
    (folder / "gold_pins.json").write_text(pin_text)
    manifest = {
        "schema": 1,
        "run_id": run,
        "mode": mode,
        "namespace": pin["namespace"],
        "pins_sha256": hashlib.sha256(pin_text.encode()).hexdigest(),
        "snapshot_set": snapshot_set,
        "files": files,
    }
    (input_folder / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return pin


def main(argv: Sequence[str] | None = None) -> int:
    """The worker entry: ``make-input`` with the shell database, the run folder, the run ID, the mode, the version and the gold table names."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) < 6 or arguments[0] != "make-input":
        sys.stderr.write("usage: make-input <shell.duckdb> <folder> <run ID> <mode> <version> <table>...\n")
        return 2
    pin = make_input(Path(arguments[1]), Path(arguments[2]), arguments[3], arguments[4], int(arguments[5]), arguments[6:])
    sys.stdout.write(json.dumps({"tables": len(pin["snapshot_set"])}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
