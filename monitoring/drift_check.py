"""Compare the current feature windows with the Production model's baseline and write one drift record (project decision 21).

``python -m monitoring.drift_check --reference-matrix P --reference-manifest P --current-matrix P --current-manifest P --features P --run-id ID --out DIR``
reads two Parquet matrices (the Production run's DVC baseline and the latest features), the manifests that name their HAI type and population, and the
JSON list of the model's feature columns. The current rows are those of a window year after the baseline's ``newest_window_year``. The check refuses mixed
HAI types and mixed populations, skips (and never triggers) for a population that is not monitored, with no newer window or with too few current rows, and
otherwise runs Evidently's dataset drift report. Retraining is triggered when the share of drifted features reaches the configured share. The record
``drift_<HAI type>.json`` in ``--out`` is written atomically and holds no clock value, so a rerun on the same inputs writes the same bytes.

Exit codes: 0 record written (checked or skipped), 2 an input is refused (the message names the failure and the column or file), 3 a package is missing.
Run it from the repository root as a module, in an environment with DuckDB and, for a real report, Evidently.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sys
import tempfile
from pathlib import Path
from typing import Any

import duckdb
from duckdb import DuckDBPyRelation
from jsonschema import Draft202012Validator

from monitoring import drift_config, evidently_report

ROOT = Path(__file__).resolve().parents[1]
RECORD_SCHEMA = ROOT / "data_contracts/monitoring/drift_record.schema.json"
NUMERIC_TYPES = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL")
CATEGORICAL_TYPES = ("VARCHAR", "BOOLEAN")
YEAR = "window_year"
HAI_TYPES = tuple(f"HAI_{index}" for index in range(1, 7))
POPULATIONS = ("primary", "sensitivity")


class CheckError(Exception):
    """An input is refused; the name is the failure, the detail says which file or column."""

    def __init__(self, name: str, detail: str, code: int = 2) -> None:
        super().__init__(f"{name}: {detail}")
        self.code = code


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path, what: str) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise CheckError(f"{what}_unreadable", f"{path}: {error}") from None


def read_manifest(path: Path, what: str) -> dict[str, Any]:
    manifest = read_json(path, what)
    if not isinstance(manifest, dict) or manifest.get("hai_type") not in HAI_TYPES or manifest.get("population") not in POPULATIONS:
        raise CheckError("manifest_invalid", f"{path} must name a hai_type HAI_1 to HAI_6 and a population primary or sensitivity")
    return manifest


def read_features(path: Path) -> list[str]:
    features = read_json(path, "features")
    if not isinstance(features, list) or not features:
        raise CheckError("features_empty", f"{path} must list at least one feature column")
    if any(not isinstance(name, str) or not name for name in features) or len(set(features)) != len(features):
        raise CheckError("features_invalid", f"{path} must list distinct column names")
    return features


def open_matrix(path: Path, what: str) -> DuckDBPyRelation:
    if not path.is_file():
        raise CheckError(f"{what}_matrix_missing", str(path))
    try:
        return duckdb.read_parquet(str(path))
    except duckdb.Error as error:
        raise CheckError(f"{what}_matrix_unreadable", f"{path}: {error}") from None


def kind_of(column_type: Any) -> str | None:
    text = str(column_type).upper()
    if text.startswith(NUMERIC_TYPES):
        return "numerical"
    return "categorical" if text.startswith(CATEGORICAL_TYPES) else None


def feature_kinds(reference: DuckDBPyRelation, current: DuckDBPyRelation, features: list[str]) -> dict[str, str]:
    """Name each feature's kind and refuse a feature that is absent, of another type or of a type Evidently's tests do not cover."""
    types = {"reference": dict(zip(reference.columns, reference.types, strict=True)), "current": dict(zip(current.columns, current.types, strict=True))}
    kinds: dict[str, str] = {}
    for name in features:
        for side, known in types.items():
            if name not in known:
                raise CheckError("feature_missing", f"{name} is not a column of the {side} matrix")
        kind = kind_of(types["reference"][name])
        if kind is None or kind != kind_of(types["current"][name]):
            raise CheckError(
                "feature_type_differs", f"{name} is {types['reference'][name]} in the reference and {types['current'][name]} in the current matrix"
            )
        kinds[name] = kind
    return kinds


def window_of(matrix: DuckDBPyRelation, newest: int | None) -> tuple[DuckDBPyRelation, int, list[int]]:
    """Return the rows after the baseline's newest window year (all rows when it is None), their count and their window years."""
    if YEAR not in matrix.columns:
        raise CheckError("column_missing", f"{YEAR} is not a column of the matrix")
    rows = matrix if newest is None else matrix.filter(f"{YEAR} > {int(newest)}")
    counted = rows.aggregate("count(*)").fetchone()
    count = int(counted[0]) if counted else 0
    years = [int(row[0]) for row in rows.project(YEAR).distinct().order(YEAR).fetchall()] if count else []
    return rows, count, years


def refuse_all_missing(rows: DuckDBPyRelation, features: list[str], side: str) -> None:
    counts = rows.aggregate(", ".join(f"count({quote(name)})" for name in features)).fetchone()
    for name, present in zip(features, counts or (), strict=True):
        if not present:
            raise CheckError("feature_all_missing", f"{name} has no value in any {side} row, so no test can compare it")


def write_record(out: Path, record: dict[str, Any]) -> Path:
    problems = [error.message for error in Draft202012Validator(json.loads(RECORD_SCHEMA.read_text())).iter_errors(record)]
    if problems:
        raise RuntimeError(f"the drift record breaks its schema: {problems[0]}")
    out.mkdir(parents=True, exist_ok=True)
    target = out / f"drift_{record['hai_type']}.json"
    handle, name = tempfile.mkstemp(dir=out, prefix=".drift_", suffix=".tmp")
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
        os.replace(name, target)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    return target


def run(arguments: argparse.Namespace) -> Path:
    config, config_sha = drift_config.load(arguments.config)
    reference_manifest = read_manifest(arguments.reference_manifest, "reference_manifest")
    current_manifest = read_manifest(arguments.current_manifest, "current_manifest")
    features = read_features(arguments.features)
    for key in ("hai_type", "population"):
        if reference_manifest[key] != current_manifest[key]:
            raise CheckError(f"{key}_mismatch", f"the reference is {reference_manifest[key]} and the current matrix is {current_manifest[key]}")
    newest = reference_manifest.get("newest_window_year")
    if not isinstance(newest, int):
        raise CheckError("manifest_invalid", f"{arguments.reference_manifest} must hold an integer newest_window_year")
    reference, current = open_matrix(arguments.reference_matrix, "reference"), open_matrix(arguments.current_matrix, "current")
    _, reference_rows, _ = window_of(reference, None)
    window, current_rows, years = window_of(current, newest)
    record: dict[str, Any] = {
        "schema_version": 1,
        "kind": "drift_record",
        "run_id": arguments.run_id,
        "hai_type": reference_manifest["hai_type"],
        "population": reference_manifest["population"],
        "status": "skipped",
        "skip_reason": None,
        "config_sha256": config_sha,
        "evidently_version": None,
        "reference": {"matrix_sha256": sha256_of(arguments.reference_matrix), "newest_window_year": newest, "rows": reference_rows},
        "current": {"matrix_sha256": sha256_of(arguments.current_matrix), "rows": current_rows, "window_years": years},
        "features": [],
        "feature_count": 0,
        "drifted_count": 0,
        "drift_share": None,
        "triggered": False,
    }
    if record["population"] not in config["trigger_populations"]:
        record["skip_reason"] = "population_not_monitored"
    elif current_rows == 0:
        record["skip_reason"] = "no_new_window"
    elif current_rows < config["minimum_current_rows"]:
        record["skip_reason"] = "too_few_current_rows"
    if record["skip_reason"] is None:
        kinds = feature_kinds(reference, current, features)
        refuse_all_missing(reference, features, "reference")
        refuse_all_missing(window, features, "current")
        try:
            evidently_report.ensure()
            frames = [rows.project(", ".join(quote(name) for name in features)).df() for rows in (reference, window)]
            version, found = evidently_report.run(
                frames[0],
                frames[1],
                [name for name in features if kinds[name] == "numerical"],
                [name for name in features if kinds[name] == "categorical"],
                config,
            )
        except evidently_report.DependencyMissing as error:
            raise CheckError("dependency_missing", f"the report needs Evidently and pandas: {error}", 3) from None
        listed = [{"name": name, **found[name]} for name in features]
        drifted = sum(item["drifted"] for item in listed)
        share = drifted / len(listed)
        record |= {
            "status": "checked",
            "evidently_version": version,
            "features": listed,
            "feature_count": len(listed),
            "drifted_count": drifted,
            "drift_share": share,
            "triggered": share >= config["drift_share"],
        }
    return write_record(arguments.out, record)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ("reference-matrix", "reference-manifest", "current-matrix", "current-manifest", "features"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--run-id", required=True, help="the drift run id, for example 20261030T120000Z-0a1b2c3d")
    parser.add_argument("--out", type=Path, required=True, help="the folder that receives drift_<HAI type>.json")
    parser.add_argument("--config", type=Path, default=drift_config.CONFIG)
    arguments = parser.parse_args(argv)
    try:
        config, _ = drift_config.load(arguments.config)

        def expired(_signal: int, _frame: Any) -> None:
            raise CheckError("deadline", f"the check ran longer than {config['deadline_seconds']} seconds")

        signal.signal(signal.SIGALRM, expired)
        signal.alarm(int(config["deadline_seconds"]))
        target = run(arguments)
    except (CheckError, drift_config.ConfigError) as error:
        sys.stderr.write(f"drift check failed: {error}\n")
        return error.code if isinstance(error, CheckError) else 2
    finally:
        signal.alarm(0)
    sys.stdout.write(f"drift record {target}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
