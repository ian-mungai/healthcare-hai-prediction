"""Cases of the model set build, validation and export, run by ``run_feature_store_e2e --mode build_local`` on the fixture build.

A model set is built from the six domain matrices of a base run. The cases make that base input from the fixture build (the matrices written as Parquet files
with the pin and the manifest an extract leaves), build set m1 (online) and set m3 (offline only) with the real ``features_build``, validate and export them
as the job phases do, and plant a defect in each place a set can go wrong. Names, counts and hashes only.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

from scripts.features import feature_schema as schema_module
from scripts.features import features_input, features_publish
from scripts.lakehouse import feature_columns as columns
from scripts.lakehouse import gold_export
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
BASE_RUN = "20261109T010000Z-0000b001"
RUNS = {"m1": "20261109T020000Z-0000a001", "m3": "20261109T030000Z-0000a003"}
PLANTED = "20261109T040000Z-0000a099"
COPY_SQL = "copy (select * from main.\"{table}\") to '{path}' (format parquet)"
# Spark writes a timestamp as INT96, which DuckDB reads as a timestamp without a time zone. This copy writes the same read-side type: each time-zone column
# becomes a plain timestamp of the same UTC instant.
SPARK_COPY_SQL = "copy (select {columns} from main.\"{table}\") to '{path}' (format parquet)"
ZONED_COLUMNS = "select column_name, lower(data_type) from information_schema.columns where table_name = ? and table_schema = 'main' order by ordinal_position"
SPINE_COUNT = 'select count(*) from main."int_hospital_spine"'
TABLE_ROWS = 'select count(*) from main."{table}"'
CHANGE_VALUE = 'update main."{table}" set "{column}" = coalesce("{column}", 0) + 1 where rowid = 0'
DROP_COLUMN = 'alter table main."{table}" drop column "{column}"'
REPEAT_ROW = 'insert into main."{table}" select * from main."{table}" limit 1'
LOSE_ROW = 'delete from main."{table}" where rowid = 0'
MOVE_TIME = 'update main."{table}" set event_timestamp = event_timestamp + interval 1 day where rowid = 0'


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_tool(arguments: list[str], seconds: int) -> Any:
    """Run this interpreter (the tool environment's) in the repository root with a deadline."""
    return run_command(sys.executable, arguments, cwd=ROOT, env=columns.tool_environment(ROOT), timeout=seconds)


def write_json(path: Path, document: dict[str, Any]) -> str:
    text = json.dumps(document, indent=2, sort_keys=True) + "\n"
    path.write_text(text)
    return hashlib.sha256(text.encode()).hexdigest()


def spark_select(connection: Any, table: str) -> str:
    """The select list that writes every time-zone column of ``table`` as a plain UTC timestamp, the way the Spark extract leaves it for DuckDB."""
    items = []
    for name, kind in connection.execute(ZONED_COLUMNS, [table]).fetchall():
        items.append(f"cast(\"{name}\" at time zone 'UTC' as timestamp)" if kind == "timestamp with time zone" else f'"{name}"')
        items[-1] += f' as "{name}"'
    return ", ".join(items)


def base_input(first: Path, folder: Path, run: str, tables: dict[str, int] | None = None, spark_timestamps: bool = False) -> tuple[str, str]:
    """The base pin and the extract of the domain matrices of the fixture build, as the pin and read steps leave them. Returns the two digests.
    With ``spark_timestamps`` the time-zone columns are written as the Spark extract of a published base writes them (read by DuckDB without a time zone)."""
    duckdb: Any = importlib.import_module("duckdb")
    domain = features_input.base_tables()
    snapshot_set = tables if tables is not None else {name: 1000 + index for index, name in enumerate(domain)}
    pin = {"schema": 1, "run_id": run, "mode": "publish_fixture", "namespace": "features_e2e", "source_run_id": BASE_RUN, "snapshot_set": snapshot_set}
    pin_sha = write_json(folder / features_input.BASE_PIN_NAME, pin)
    files: dict[str, str] = {}
    connection = duckdb.connect(str(first / "staging.duckdb"), read_only=True)
    try:
        for name in domain:
            target = folder / features_input.BASE_INPUT_NAME / name
            target.mkdir(parents=True)
            if spark_timestamps:
                connection.execute(SPARK_COPY_SQL.format(columns=spark_select(connection, name), table=name, path=target / "part-0.parquet"))
            else:
                connection.execute(COPY_SQL.format(table=name, path=target / "part-0.parquet"))
            files[f"{name}/part-0.parquet"] = sha256(target / "part-0.parquet")
    finally:
        connection.close()
    manifest = {
        "schema": 1,
        "run_id": run,
        "mode": "publish_fixture",
        "namespace": "features_e2e",
        "pins_sha256": pin_sha,
        "snapshot_set": snapshot_set,
        "files": files,
    }
    return pin_sha, write_json(folder / features_input.BASE_INPUT_NAME / "manifest.json", manifest)


def build_set(folder: Path, run: str, feature_set: str, pin_sha: str, extract_sha: str) -> Any:
    return run_tool(
        [
            "-m",
            "scripts.features.features_build",
            "--mode",
            "publish_fixture",
            "--run",
            run,
            "--folder",
            str(folder),
            "--project",
            str(ROOT / "dbt"),
            "--source-sha256",
            "a" * 64,
            "--input-sha256",
            pin_sha,
            "--extract-sha256",
            extract_sha,
            "--feature-set",
            feature_set,
        ],
        1200,
    )


def validate_set(database: Path, output: Path, feature_set: str) -> tuple[int, dict[str, Any]]:
    """Run the validation module on a build file as the phase does; returns the exit code and the record (empty when none was written)."""
    done = run_tool(
        ["-m", "scripts.features.feature_validation", "--database", str(database), "--output", str(output), "--feature-set", feature_set],
        600,
    )
    return done.returncode, (json.loads(output.read_text()) if output.is_file() else {})


def failed(record: dict[str, Any]) -> set[tuple[str, str]]:
    return {(item["id"], item["table"]) for item in record.get("expectations", []) if item["level"] == "fail"}


def damaged(source: Path, target: Path, sql: str) -> Path:
    duckdb: Any = importlib.import_module("duckdb")
    shutil.copy2(source, target)
    connection = duckdb.connect(str(target))
    try:
        connection.execute(sql)
    finally:
        connection.close()
    return target


def model_set_build_cases(report: Any, first: Path, base: Path) -> None:
    """S01 to S16: build, validate and export two model sets from the domain matrices of the fixture build, and plant a defect in each place."""
    schema = schema_module.load_schema()
    built: dict[str, Path] = {}
    for feature_set, run in RUNS.items():
        folder = base / f"set_{feature_set}"
        folder.mkdir()
        pin_sha, extract_sha = base_input(first, folder, run)
        done = build_set(folder, run, feature_set, pin_sha, extract_sha)
        receipt = json.loads((folder / "build_receipt.json").read_text()) if (folder / "build_receipt.json").is_file() else {}
        wanted = schema_module.set_schema_sha256(feature_set, ROOT, schema)
        report.case(
            "S01",
            "FS1",
            f"set {feature_set} builds from the domain matrices of the base run and its receipt names the set, the base run and the set hash",
            done.returncode == 0 and (receipt.get("feature_set"), receipt.get("base_run"), receipt.get("set_schema_sha256")) == (feature_set, BASE_RUN, wanted),
            error=(done.stdout + done.stderr)[-300:] if done.returncode else "",
        )
        built[feature_set] = folder
    if len(built) != 2 or not all((folder / "staging.duckdb").is_file() for folder in built.values()):
        return
    duckdb: Any = importlib.import_module("duckdb")
    spine_rows = duckdb.connect(str(first / "staging.duckdb"), read_only=True).execute(SPINE_COUNT).fetchone()[0]
    for feature_set, folder in built.items():
        entry = schema["model_sets"][feature_set]
        connection = duckdb.connect(str(folder / "staging.duckdb"), read_only=True)
        try:
            found = connection.execute(
                "select column_name, lower(data_type) from information_schema.columns where table_name = ? and table_schema = 'main' order by ordinal_position",
                [entry["table"]],
            ).fetchall()
            rows = connection.execute(TABLE_ROWS.format(table=entry["table"])).fetchone()[0]
            tables = {row[0] for row in connection.execute("select table_name from information_schema.tables where table_schema = 'main'").fetchall()}
        finally:
            connection.close()
        report.case(
            "S02",
            "FS1",
            f"set {feature_set} holds the key, the event time and exactly the columns of its list with the schema's types, one row per window of the base",
            [(name, kind) for name, kind in found] == [(column["name"], column["type"]) for column in entry["columns"]] and rows == spine_rows > 0,
            columns=len(found),
            rows=rows,
        )
        report.case(
            "S03",
            "FS1",
            f"the build file of set {feature_set} holds the set's table, the six matrices it read, the inputs and the receipt, and no other model",
            tables == {entry["table"], *features_input.base_tables(), "feature_set_inputs", "feature_build_receipt"},
        )
    one, two = (schema["model_sets"][name]["columns"] for name in RUNS)
    report.case("S04", "FS1", "the two sets differ in their columns, so a list is not shared by accident", [c["name"] for c in one] != [c["name"] for c in two])
    for feature_set, folder in built.items():
        code, record = validate_set(folder / "staging.duckdb", folder / "quality.json", feature_set)
        report.case(
            "S05",
            "FS1",
            f"the validation passes set {feature_set}: receipt, columns, key, rows, event time and every value equal to its domain matrix",
            code == 0 and bool(record.get("expectations")) and record["counts"]["fail"] == 0,
            expectations=len(record.get("expectations", [])),
        )
    m1 = built["m1"] / "staging.duckdb"
    entry = schema["model_sets"]["m1"]
    victim = next(column["name"] for column in entry["columns"] if column["name"].startswith("c119"))
    table = entry["table"]
    damages = {
        "a changed value": (CHANGE_VALUE.format(table=table, column=victim), ("set_values_equal_source", table)),
        "a dropped column": (DROP_COLUMN.format(table=table, column=victim), ("columns_equal_schema", table)),
        "a repeated key": (REPEAT_ROW.format(table=table), ("merge_key", table)),
        "a lost row": (LOSE_ROW.format(table=table), ("set_rows_equal_window", table)),
        "a moved event time": (MOVE_TIME.format(table=table), ("set_event_time_equal_window", table)),
    }
    for index, (label, (sql, expected)) in enumerate(damages.items()):
        copy = damaged(m1, base / f"damaged_{index}.duckdb", sql)
        code, record = validate_set(copy, base / f"damaged_{index}.json", "m1")
        report.case("S06", "FS1", f"{label} in set m1 fails its own check", code == 1 and expected in failed(record), failed=sorted(failed(record))[:4])
    stale = damaged(m1, base / "stale_inputs.duckdb", "update main.feature_set_inputs set set_schema_sha256 = repeat('0', 64)")
    code, record = validate_set(stale, base / "stale_inputs.json", "m1")
    report.case(
        "S07",
        "FS6",
        "a build whose inputs record another set hash than the schema's fails",
        code == 1 and ("set_inputs", "feature_set_inputs") in failed(record),
    )
    code, _ = validate_set(built["m3"] / "staging.duckdb", base / "wrong_set.json", "m1")
    report.case("S08", "FS1", "a build of set m3 validated as set m1 fails", code == 1)
    wrong_plan = features_publish.publish_plan("publish_fixture", feature_set="m1")
    publish = base / "set_export"
    publish.mkdir()
    staging = built["m1"] / "staging.duckdb"
    result = gold_export.export_sealed(staging, sha256(staging), publish, wrong_plan, RUNS["m1"])
    manifest = json.loads((publish / "export/manifest.json").read_text())
    report.case(
        "S09",
        "FS1",
        "the export of set m1 holds exactly feature_matrix_m1 and none of the six matrices it read",
        result["exported"] is True and set(manifest["tables"]) == {"feature_matrix_m1"},
        tables=sorted(manifest["tables"]),
    )
    short = base / "set_short"
    short.mkdir()
    pin_sha, extract_sha = base_input(first, short, RUNS["m1"])
    pin = json.loads((short / features_input.BASE_PIN_NAME).read_text())
    pin["snapshot_set"].pop(next(iter(pin["snapshot_set"])))
    done = build_set(short, RUNS["m1"], "m1", write_json(short / features_input.BASE_PIN_NAME, pin), extract_sha)
    report.case(
        "S10", "FS3", "a base pin that misses a matrix is refused by the build", done.returncode == 1 and "base pin" in done.stderr, error=done.stderr[-200:]
    )
    tampered = base / "set_tampered"
    tampered.mkdir()
    pin_sha, extract_sha = base_input(first, tampered, PLANTED)
    target = next((tampered / features_input.BASE_INPUT_NAME).rglob("*.parquet"))
    target.chmod(0o644)
    with target.open("ab") as handle:
        handle.write(b"tampered")
    done = build_set(tampered, PLANTED, "m1", pin_sha, extract_sha)
    report.case(
        "S11",
        "FS3",
        "a base extract file changed after its manifest is refused by the build",
        done.returncode == 1 and "changed before import" in done.stderr,
        error=done.stderr[-200:],
    )
    other = base / "set_other_run"
    other.mkdir()
    pin_sha, extract_sha = base_input(first, other, RUNS["m3"])
    refused = build_set(other, RUNS["m1"], "m1", pin_sha, extract_sha)
    report.case(
        "S12",
        "FS3",
        "a pin of another run than the build's run is refused",
        refused.returncode == 1 and "base pin" in refused.stderr,
        error=refused.stderr[-200:],
    )
    unknown = build_set(other, RUNS["m1"], "m7", pin_sha, extract_sha)
    report.case(
        "S13",
        "FS2",
        "a set outside base and m1 to m6 is refused before any build",
        unknown.returncode != 0 and "m7" in unknown.stderr,
        error=unknown.stderr[-200:],
    )
    published = base / "set_published_base"
    published.mkdir()
    pin_sha, extract_sha = base_input(first, published, RUNS["m1"], spark_timestamps=True)
    done = build_set(published, RUNS["m1"], "m1", pin_sha, extract_sha)
    built_file = published / "staging.duckdb"
    types = []
    if done.returncode == 0 and built_file.is_file():
        connection = duckdb.connect(str(built_file), read_only=True)
        try:
            types = connection.execute(ZONED_COLUMNS, [schema["model_sets"]["m1"]["table"]]).fetchall()
        finally:
            connection.close()
    code, record = validate_set(built_file, published / "quality.json", "m1") if built_file.is_file() else (1, {})
    report.case(
        "S14",
        "FS1",
        "set m1 builds and validates from a base extract whose time-zone columns Spark wrote without a time zone, with the schema's types",
        done.returncode == 0
        and [(name, kind) for name, kind in types] == [(column["name"], column["type"]) for column in schema["model_sets"]["m1"]["columns"]]
        and code == 0
        and record.get("counts", {}).get("fail") == 0,
        error=(done.stdout + done.stderr)[-1200:] if done.returncode else "",
    )
