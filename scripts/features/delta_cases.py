"""Training support publication checks, called by the feature store build runner."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import shutil
from pathlib import Path
from typing import Any

SUPPORT = ("int_spine_validation_measures", "int_county_adjacency")


def built_support(report: Any, staging: Path, root: Path, run_folder: Path) -> None:
    """Check populated support tables and the seed identity from the real features build."""
    duckdb = importlib.import_module("duckdb")
    con = duckdb.connect(str(staging), read_only=True)
    try:
        tables = {row[0] for row in con.execute("select table_name from information_schema.tables").fetchall()}
        report.case("E01", "DT1", "the build materializes both non-Feast support tables", set(SUPPORT) <= tables)
        if "int_county_adjacency" in tables:
            actual = con.execute("select * from int_county_adjacency order by edge_key").fetchall()
            expected = con.execute(
                "select * from read_parquet(?) order by edge_key", [str(run_folder / "gold_input/bridge_county_neighbor/*.parquet")]
            ).fetchall()
            report.case(
                "E01", "DT3", "every pinned adjacency column and edge survives, including both vintages and the island", len(actual) == 6 and actual == expected
            )
        psi = con.execute(
            "select window_year, measure, value_number, period_end, overlap_days from int_spine_validation_measures "
            "where ccn = '990001' and measure in ('PSI_90', 'PSI_13') order by window_year, measure"
        ).fetchall()
        expected_psi = [(year, measure, year - 2020.5, f"{year - 1}-12-31", 0) for year in (2022, 2023, 2024) for measure in ("PSI_13", "PSI_90")]
        observed = [(year, measure, value, str(end), overlap) for year, measure, value, end, overlap in psi]
        report.case("E01", "DT6", "PSI selects the latest strictly prior period and excludes boundary and overlapping periods", observed == expected_psi)
        gap = con.execute(
            "select window_year, measure, value_number, overlap_days from int_spine_validation_measures "
            "where ccn = '990004' and measure in ('PSI_90', 'PSI_13') order by window_year, measure"
        ).fetchall()
        report.case(
            "E01",
            "DT6",
            "a PSI period separated from the window by a gap has zero overlap days",
            gap == [(year, measure, 2.75, 0) for year in (2022, 2023) for measure in ("PSI_13", "PSI_90")],
        )
        missing = con.execute(
            "select ccn, alignment_status, value_number from int_spine_validation_measures "
            "where spine_key in ('990002:2023', '990005:2022') and measure in ('PSI_90', 'PSI_13') order by ccn, measure"
        ).fetchall()
        report.case(
            "E01",
            "DT7",
            "PSI without an eligible period stays null and a prior hold stays held",
            missing == [("990002", "no_matching_period", None)] * 2 + [("990005", "held_in_staging", None)] * 2,
        )
        hac = con.execute(
            "select window_year, value_text, fiscal_year, alignment_status from int_spine_validation_measures "
            "where ccn = '990001' and measure = 'payment_reduction' order by window_year"
        ).fetchall()
        report.case(
            "E01",
            "DT6",
            "HAC payment flags retain outcome-window alignment",
            hac == [(2022, "Yes", 2025, "aligned"), (2023, "Yes", 2025, "aligned"), (2024, None, None, "no_matching_period")],
        )
        columns = {row[1] for row in con.execute("pragma table_info('feature_build_inputs')").fetchall()}
        expected_hash = hashlib.sha256((root / "dbt/seeds/features/frozen_hai_outcome_controls.csv").read_bytes()).hexdigest()
        hashes = con.execute("select hai_outcome_controls_sha256 from feature_build_inputs").fetchall() if "hai_outcome_controls_sha256" in columns else []
        report.case("E06a", "DT8", "the publication hash equals SHA-256 of the exact outcome identity seed bytes", hashes == [(expected_hash,)])
    finally:
        con.close()


def planted_support_rules(report: Any, staging: Path, root: Path, run_folder: Path, work: Path) -> None:
    """Run altered compiled model queries against the real build's pinned fixture database."""
    duckdb = importlib.import_module("duckdb")
    from scripts.features import features_build

    stage = run_folder / "features_work/features.duckdb"
    compiled = run_folder / "features_work/build/target/compiled/hai/models/features/int_spine_validation_measures.sql"
    sql = compiled.read_text().strip().rstrip(";")
    mutations = (
        ("PSI boundary admitted", "spine.window_start > periods.period_end", "spine.window_start >= periods.period_end"),
        ("PSI overlap admitted", "periods.measure not in ('PSI_90', 'PSI_13')", "true"),
        ("PSI oldest period selected", "periods.period_end desc,", "periods.period_end asc,"),
        ("PSI prior hold lost", "holds.window_end < spine.window_start", "holds.window_end >= spine.window_start"),
        (
            "PSI overlap days unclamped",
            "greatest(0, least(periods.period_end, spine.window_end) - greatest(periods.period_start, spine.window_start) + 1) as overlap_days",
            "least(periods.period_end, spine.window_end) - greatest(periods.period_start, spine.window_start) + 1 as overlap_days",
        ),
    )
    con = duckdb.connect(str(stage), read_only=True)
    try:
        for label, old, new in mutations:
            if old not in sql:
                raise RuntimeError(f"missing compiled SQL mutation target: {label}")
            cursor = con.execute(sql.replace(old, new))
            columns = [column[0] for column in cursor.description]
            result = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
            psi = sorted(
                (r["window_year"], r["measure"], r["value_number"], str(r["period_end"]), r["overlap_days"])
                for r in result
                if r["ccn"] == "990001" and r["measure"] in ("PSI_90", "PSI_13")
            )
            wanted = [(year, measure, year - 2020.5, f"{year - 1}-12-31", 0) for year in (2022, 2023, 2024) for measure in ("PSI_13", "PSI_90")]
            held = [(r["alignment_status"], r["value_number"]) for r in result if r["spine_key"] == "990005:2022" and r["measure"] in ("PSI_90", "PSI_13")]
            gap = sorted(
                (r["window_year"], r["measure"], r["value_number"], r["overlap_days"])
                for r in result
                if r["ccn"] == "990004" and r["measure"] in ("PSI_90", "PSI_13")
            )
            wanted_gap = [(year, measure, 2.75, 0) for year in (2022, 2023) for measure in ("PSI_13", "PSI_90")]
            report.case(
                "E01",
                "DT6 DT7",
                f"mutant ({label}): populated PSI case detects the defect",
                psi != wanted or held != [("held_in_staging", None)] * 2 or gap != wanted_gap,
            )
    finally:
        con.close()
    source = (root / "scripts/features/features_build.py").read_text()
    old = "source_sha, schema_sha, controls_sha],"
    if old not in source:
        raise RuntimeError("missing build hash mutation target")
    changed = work / "features_build_mutant.py"
    changed.write_text(source.replace(old, "source_sha, schema_sha, schema_sha],"))
    spec = importlib.util.spec_from_file_location("features_build_mutant", changed)
    if spec is None or spec.loader is None:
        raise RuntimeError("the build mutant cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    damaged = work / "wrong_outcome_identity.duckdb"
    receipt = json.loads((run_folder / "build_receipt.json").read_text())
    pin = json.loads((run_folder / "gold_pins.json").read_text())
    con = duckdb.connect(str(staging), read_only=True)
    try:
        nodes = con.execute("select node, resource_type, status from feature_build_receipt").fetchall()
    finally:
        con.close()
    module.materialize(
        stage,
        damaged,
        features_build.published_tables(root),
        pin,
        receipt["run_id"],
        receipt["source_sha256"],
        receipt["feature_schema_sha256"],
        receipt["hai_outcome_controls_sha256"],
        nodes,
    )
    probe = type(report)()
    built_support(probe, damaged, root, run_folder)
    report.case(
        "E06a",
        "DT8",
        "mutant (schema hash substituted for outcome identity): exact seed hash case detects the defect",
        any(item["failure_modes"] == "DT8" and item["verdict"] == "fail" for item in probe.steps),
    )


def snapshot_identity(report: Any, build: Any, first: Path, base: Path) -> None:
    """Build a snapshot with semantically equal but byte-different seed input and verify its identity."""
    snapshot = base / "identity_snapshot"
    root = Path(__file__).resolve().parents[2]
    for directory in ("dbt", "scripts", "orchestration", "data_contracts"):
        shutil.copytree(root / directory, snapshot / directory, ignore=shutil.ignore_patterns("__pycache__", "target", "logs", "dbt_packages"))
    seed = snapshot / "dbt/seeds/features/frozen_hai_outcome_controls.csv"
    seed.write_bytes(seed.read_bytes() + b"\n")
    expected = hashlib.sha256(seed.read_bytes()).hexdigest()
    folder = base / "identity_run"
    folder.mkdir()
    shutil.copytree(first / "gold_input", folder / "gold_input")
    shutil.copy2(first / "gold_pins.json", folder / "gold_pins.json")
    run_id = json.loads((folder / "gold_pins.json").read_text())["run_id"]
    result = build.build(folder, run_id, ["--project", str(snapshot / "dbt")])
    report.case(
        "E06a",
        "DT8 DT9",
        "a copied snapshot with a byte-different identity seed builds",
        result.returncode == 0,
        exit=result.returncode,
        error=(result.stdout + result.stderr)[-500:] if result.returncode else "",
    )
    if result.returncode:
        return
    duckdb = importlib.import_module("duckdb")
    con = duckdb.connect(str(folder / "staging.duckdb"), read_only=True)
    try:
        found = con.execute("select hai_outcome_controls_sha256 from feature_build_inputs").fetchall()
        live = hashlib.sha256((root / "dbt/seeds/features/frozen_hai_outcome_controls.csv").read_bytes()).hexdigest()
        report.case(
            "E06a",
            "DT8 DT9",
            "the built snapshot seed determines the publication hash independently of the live checkout",
            found == [(expected,)] and expected != live,
        )
    finally:
        con.close()
