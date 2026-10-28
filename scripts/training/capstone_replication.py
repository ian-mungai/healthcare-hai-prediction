"""Rebuild the historical capstone dataset and repeat its notebook OLS procedure.

This standalone run has no connection to the study models or their predictor exclusions.
``--run-set`` names a JSON object with one Glue run per joined table.
``--enumerate-run-sets`` inspects the time-aligned choices without importing training libraries.
``--inspect-only`` verifies every part and reports join cardinality without materializing it.
``--prepare-only`` writes the reconstructed JSON lines without importing training libraries.

Image checks: ``python -m scripts.training.run_capstone_replication_cases`` and
``python -m scripts.training.capstone_replication_defects`` with their required output folders.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import itertools
import json
import math
import re
import sys
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

JOIN_TABLES = ("hai", "hospital_characteristics", "patient_experience", "patient_safety", "timely_effective_care")
TABLES = (*JOIN_TABLES, "infection_prevention")
CATEGORICAL_COLUMNS = ("emergency_services", "region", "hospital_ownership", "ed_volume")
CAP_COLUMNS = ("wound_dehiscence_rate", "postop_sepsis_rate", "pressure_ulcer_rate", "postop_respiratory_failure_rate")
METADATA_COLUMNS = ("hospital_type", "facility_id", "hospital_name", "state")
IMPUTATION_EXCLUSIONS = {"infection_index", "vte_prophylaxis_compliance"}
NOTEBOOK_COLUMNS = (
    "ed_length_of_stay_minutes",
    "sepsis_care_compliance",
    "emergency_services",
    "pulmonary_embolism_dvt_rate",
    "recommend_hospital",
    "hospital_name",
    "doctor_communication",
    "cleanliness_score",
    "ed_volume",
    "wound_dehiscence_rate",
    "state",
    "region",
    "discharge_information",
    "hospital_ownership",
    "infection_index",
    "postop_sepsis_rate",
    "pressure_ulcer_rate",
    "medication_communication",
    "healthcare_worker_flu_vaccination",
    "nurse_communication",
    "hospital_type",
    "facility_id",
    "opioid_safety_compliance",
    "postop_respiratory_failure_rate",
    "hospital_overall_rating",
    "vte_prophylaxis_compliance",
)
HISTORICAL_TERMS = (
    "ed_length_of_stay_minutes",
    "pulmonary_embolism_dvt_rate",
    "recommend_hospital",
    "healthcare_worker_flu_vaccination",
    "hospital_overall_rating",
    "region_Northeast",
    "region_South",
    "region_West",
    "hospital_ownership_Proprietary",
    "ed_volume_low",
    "ed_volume_medium",
)
TABLE_FIELDS = {
    "hai": {"hospital_name": "string", "facility_id": "string", "infection_index": "double"},
    "hospital_characteristics": {
        "facility_id": "string",
        "hospital_name": "string",
        "hospital_type": "string",
        "hospital_ownership": "string",
        "emergency_services": "string",
        "hospital_overall_rating": "double",
        "state": "string",
        "region": "string",
    },
    "patient_experience": {
        "facility_id": "string",
        "hospital_name": "string",
        "cleanliness_score": "double",
        "nurse_communication": "double",
        "doctor_communication": "double",
        "medication_communication": "double",
        "discharge_information": "double",
        "recommend_hospital": "double",
    },
    "patient_safety": {
        "facility_id": "string",
        "hospital_name": "string",
        "pressure_ulcer_rate": "double",
        "postop_respiratory_failure_rate": "double",
        "pulmonary_embolism_dvt_rate": "double",
        "postop_sepsis_rate": "double",
        "wound_dehiscence_rate": "double",
    },
    "timely_effective_care": {
        "facility_id": "string",
        "hospital_name": "string",
        "healthcare_worker_flu_vaccination": "double",
        "ed_length_of_stay_minutes": "double",
        "opioid_safety_compliance": "double",
        "sepsis_care_compliance": "double",
        "ed_volume": "string",
        "vte_prophylaxis_compliance": "double",
    },
    "infection_prevention": {"facility_id": "string", "hospital_name": "string", "flu_vaccination_rate": "double", "covid_vaccination_rate": "double"},
}
ROW_ORDER = (
    "Sorted selected timely-care part names and source lines determine facility order, followed by HAI source and right-side match order. "
    "This inferred rule reproduces the saved notebook head. Later file-local sample labels conflict, so exact historical split membership remains unverified."
)


def json_bytes(value: Any, *, sort_keys: bool = True) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=sort_keys, separators=(",", ":"), allow_nan=False) + "\n").encode()


def file_sha256(path: Path) -> str:
    """Hash a local file without loading the full reconstructed dataset."""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def write_checked(path: Path, chunks: Iterable[bytes]) -> dict[str, Any]:
    """Publish deterministic generated bytes, preserving a conflicting existing output."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".capstone_", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            digest = hashlib.sha256()
            for chunk in chunks:
                handle.write(chunk)
                digest.update(chunk)
            handle.flush()
            handle.close()
            expected = digest.hexdigest()
            if path.exists():
                if file_sha256(path) != expected:
                    raise ValueError(f"output conflict: {path.name}; use a new run folder")
            else:
                temporary.replace(path)
            return {"path": path.name, "sha256": expected, "bytes": path.stat().st_size}
        finally:
            if temporary.exists():
                temporary.unlink()


def manifest_entries(manifest: Path, expected_digest: str) -> tuple[dict[str, str], str]:
    data = manifest.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != expected_digest:
        raise ValueError("manifest hash mismatch")
    entries: dict[str, str] = {}
    for number, line in enumerate(data.decode().splitlines(), 1):
        match = re.fullmatch(r"([0-9a-f]{64}) [ *](.+)", line)
        if not match:
            raise ValueError(f"manifest format: line {number}")
        checksum, name = match.groups()
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"manifest path outside curated root: line {number}")
        for prefix in ("data/capstone_replication/curated/", "curated/"):
            if name.startswith(prefix):
                name = name.removeprefix(prefix)
                break
        relative = Path(name)
        if len(relative.parts) != 2 or relative.parts[0] not in TABLES:
            raise ValueError(f"manifest path outside curated tables: line {number}")
        if name in entries:
            raise ValueError(f"duplicate manifest path: line {number}")
        entries[name] = checksum
    if not entries or {Path(name).parts[0] for name in entries} != set(TABLES):
        raise ValueError("manifest inventory must name all six curated tables")
    return entries, digest


def glue_runs(entries: dict[str, str]) -> dict[str, list[str]]:
    runs: dict[str, set[str]] = {table: set() for table in JOIN_TABLES}
    for name in entries:
        table = Path(name).parts[0]
        if table in runs:
            match = re.fullmatch(r"(run-[0-9]+)-part-r-.+", Path(name).name)
            if match is None:
                raise ValueError(f"part has no Glue run identity: {name}")
            runs[table].add(match[1])
    return {table: sorted(values, key=lambda run: int(run.removeprefix("run-"))) for table, values in runs.items()}


def read_run_set(path: Path) -> dict[str, str]:
    value = json.loads(path.read_text())
    if (
        not isinstance(value, dict)
        or set(value) != set(JOIN_TABLES)
        or any(not isinstance(run, str) or re.fullmatch(r"run-[0-9]+", run) is None for run in value.values())
    ):
        raise ValueError("run set must choose exactly one Glue run per joined table")
    return value


def checked_tables(curated: Path, manifest: Path, expected_digest: str, run_set: dict[str, str]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Read only checksum-matching bytes, preserving duplicate rows and source order."""
    entries, digest = manifest_entries(manifest, expected_digest)
    available = glue_runs(entries)
    for table, run in run_set.items():
        if run not in available[table]:
            raise ValueError(f"selected Glue run not found: {table}={run}")
    paths = list(curated.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise ValueError("curated inventory contains a symbolic link")
    inventory = {path.relative_to(curated).as_posix() for path in paths if path.is_file()}
    if inventory != set(entries):
        raise ValueError(f"inventory mismatch: {len(set(entries) - inventory)} missing, {len(inventory - set(entries))} unlisted files")
    tables: dict[str, list[dict[str, Any]]] = {name: [] for name in TABLES}
    fields: dict[str, set[str]] = {name: set() for name in TABLES}
    selected_files = []
    for name, expected in sorted(entries.items()):
        data = (curated / name).read_bytes()
        if hashlib.sha256(data).hexdigest() != expected:
            raise ValueError(f"file hash mismatch: {name}")
        table = Path(name).parts[0]
        if table in JOIN_TABLES:
            if not Path(name).name.startswith(run_set[table] + "-part-r-"):
                continue
            selected_files.append(name)
        for number, line in enumerate(data.decode().splitlines(), 1):
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f"row must be a JSON object: {name}:{number}")
            for column, value in row.items():
                declared = TABLE_FIELDS[table].get(column)
                if declared is None:
                    raise ValueError(f"unknown column: {table}.{column}")
                valid = value is None or (isinstance(value, str) if declared == "string" else type(value) in (int, float) and math.isfinite(value))
                if not valid:
                    raise ValueError(f"field type: {table}.{column} must be {declared} or null")
                if declared == "double" and value is not None:
                    row[column] = float(value)
            fields[table].update(row)
            tables[table].append(row)
    schemas = {table: {column: TABLE_FIELDS[table][column] for column in sorted(columns)} for table, columns in fields.items()}
    audit: dict[str, Any] = {
        "manifest_sha256": digest,
        "run_set": run_set,
        "available_runs": available,
        "selected_files": selected_files,
        "files": entries,
        "table_rows": {name: len(rows) for name, rows in tables.items()},
        "table_schemas": schemas,
        "unused_table": "infection_prevention",
        "classification": "Public",
        "row_order": ROW_ORDER,
    }
    audit.update(join_plan(tables))
    return tables, audit


def join_plan(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    counts = {name: Counter(row.get("facility_id") for row in tables[name]) for name in JOIN_TABLES}
    left = {key: count for key, count in counts["hai"].items() if key is not None}
    joins = []
    for name in JOIN_TABLES[1:]:
        input_rows = sum(left.values())
        left = {key: count * counts[name][key] for key, count in left.items() if counts[name][key]}
        joins.append({"right_table": name, "type": "inner", "keys": ["facility_id"], "left_rows": input_rows, "output_rows": sum(left.values())})
    return {
        "joins": joins,
        "joined_rows": sum(left.values()),
        "joined_facilities": len(left),
        "duplicate_facilities": sum(count > 1 for count in left.values()),
        "excess_facility_rows": sum(count - 1 for count in left.values()),
        "historical_prepared_rows": 2396,
        "historical_row_count_equal": sum(left.values()) == 2396,
    }


def joined_rows(tables: dict[str, list[dict[str, Any]]]) -> Iterator[dict[str, Any]]:
    """Port the four Glue joins and final duplicate-field removal, without deduplicating rows."""
    indexes: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for table in JOIN_TABLES[1:]:
        index: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in tables[table]:
            if row.get("facility_id") is not None:
                index[row["facility_id"]].append(row)
        indexes[table] = index
    columns = {column for table in JOIN_TABLES for row in tables[table] for column in row}
    facility_order: dict[str | None, int] = {}
    for position, row in enumerate(tables["timely_effective_care"]):
        facility_order.setdefault(row.get("facility_id"), position)
    for hai in sorted(tables["hai"], key=lambda row: facility_order.get(row.get("facility_id"), len(tables["timely_effective_care"]))):
        key = hai.get("facility_id")
        if key is None:
            continue
        matches = [indexes[table].get(key, []) for table in JOIN_TABLES[1:]]
        if any(not group for group in matches):
            continue
        for combination in itertools.product(*matches):
            result = dict(hai)
            for right in combination:
                result.update({column: value for column, value in right.items() if column not in ("facility_id", "hospital_name", "facility_name")})
            yield {column: result.get(column) for column in NOTEBOOK_COLUMNS if column in columns}


def preparation_shape(tables: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    levels: dict[str, set[str]] = {column: set() for column in CATEGORICAL_COLUMNS}
    columns = {column for table in JOIN_TABLES for row in tables[table] for column in row}
    model_columns = [column for column in NOTEBOOK_COLUMNS if column in columns and column not in METADATA_COLUMNS]
    numerical_columns = set(model_columns) - set(CATEGORICAL_COLUMNS) - IMPUTATION_EXCLUSIONS
    observed_numeric: set[str] = set()
    complete_rows = 0
    for row in joined_rows(tables):
        for column in levels:
            if row.get(column) is not None:
                levels[column].add(row[column])
        observed_numeric.update(column for column in numerical_columns if row.get(column) is not None)
        complete_rows += all(row.get(column) is not None for column in IMPUTATION_EXCLUSIONS if column in columns)
    encoded_columns = len(model_columns) - len(CATEGORICAL_COLUMNS) + sum(max(0, len(values) - 1) for values in levels.values())
    return {
        "joined_columns": len(columns),
        "after_metadata_drop": len(model_columns),
        "categorical_levels": {column: sorted(values) for column, values in levels.items()},
        "encoded_columns": encoded_columns,
        "predictor_columns": encoded_columns - 1,
        "complete_rows": complete_rows,
        "all_null_imputed_columns": sorted(numerical_columns - observed_numeric),
        "limits": "Host shape inspection only. Imputation and clipping preserve column counts. Numerical transforms and fitting require the training image.",
    }


def enumerate_run_sets(curated: Path, manifest: Path, expected_digest: str, output: Path, classification: str) -> dict[str, Any]:
    entries, _ = manifest_entries(manifest, expected_digest)
    available = glue_runs(entries)
    counts = {len(runs) for runs in available.values()}
    if len(counts) != 1:
        raise ValueError(f"aligned run enumeration requires equal run counts per joined table: { {table: len(runs) for table, runs in available.items()} }")
    sets = []
    for ordinal in range(next(iter(counts))):
        run_set = {table: available[table][ordinal] for table in JOIN_TABLES}
        tables, audit = checked_tables(curated, manifest, expected_digest, run_set)
        audit["classification"] = classification
        record = {
            "ordinal": ordinal + 1,
            "run_set": run_set,
            "joined_rows": audit["joined_rows"],
            "joined_facilities": audit["joined_facilities"],
            "duplicate_facilities": audit["duplicate_facilities"],
            "excess_facility_rows": audit["excess_facility_rows"],
            "table_rows": audit["table_rows"],
            "prepared_shape": preparation_shape(tables),
            "candidate": audit["joined_rows"] == 2396 and audit["duplicate_facilities"] == 0,
        }
        record["argument_file"] = write_checked(output / "run_sets" / f"aligned_{ordinal + 1}.json", [json_bytes(run_set)])
        record["audit_file"] = write_checked(output / "run_sets" / f"aligned_{ordinal + 1}_audit.json", [json_bytes(audit)])
        sets.append(record)
    report = {
        "manifest_sha256": expected_digest,
        "implementation_sha256": file_sha256(Path(__file__)),
        "classification": classification,
        "strategy": "The k-th Glue run of each joined table, ordered by numeric run id. No other combinations searched.",
        "sets": sets,
        "candidates": [record["ordinal"] for record in sets if record["candidate"]],
        "model_executed": False,
        "limits": [ROW_ORDER, "Matching cohort size does not establish the historical split or metric."],
    }
    write_checked(output / "run_sets.json", [json_bytes(report)])
    return report


def training_libraries() -> tuple[Any, Any, Any, Any, Any, Any]:
    """Load optional image-only libraries at the model boundary, leaving input inspection available on the host."""
    try:
        pandas = importlib.import_module("pandas")
        statsmodels = importlib.import_module("statsmodels.api")
        impute = importlib.import_module("sklearn.impute")
        selection = importlib.import_module("sklearn.model_selection")
        numpy = importlib.import_module("numpy")
        metrics = importlib.import_module("sklearn.metrics")
    except ModuleNotFoundError as error:
        raise ValueError(f"training image required: missing {error.name}; no package is installed by this command") from error
    return pandas, statsmodels, impute.SimpleImputer, selection.train_test_split, numpy, metrics.mean_squared_error


def prepared_frame(frame: Any, pandas: Any, imputer_type: Any) -> tuple[Any, dict[str, Any]]:
    frame["facility_id"] = frame["facility_id"].astype("Int64").astype(str).str.zfill(6)
    numerical_columns = frame.select_dtypes(include=["number"]).columns.tolist()
    df_model = frame.drop(columns=list(METADATA_COLUMNS)).copy()
    numerical_cols = sorted(set(numerical_columns) - IMPUTATION_EXCLUSIONS)
    imputer = imputer_type(strategy="median")
    imputation_source = frame[numerical_cols]
    imputer.fit(imputation_source)
    df_model[numerical_cols] = imputer.transform(frame[numerical_cols])
    medians = dict(zip(numerical_cols, (float(value) for value in imputer.statistics_), strict=True))
    clipping = {}
    for column in CAP_COLUMNS:
        q1 = df_model[column].quantile(0.25)
        q3 = df_model[column].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df_model[column] = df_model[column].clip(lower=lower, upper=upper)
        clipping[column] = {"q1": float(q1), "q3": float(q3), "lower": float(lower), "upper": float(upper)}
    references = {column: sorted(df_model[column].dropna().unique())[0] if df_model[column].notna().any() else None for column in CATEGORICAL_COLUMNS}
    encoded = pandas.get_dummies(df_model, columns=list(CATEGORICAL_COLUMNS), drop_first=True, dtype=int)
    encoded.dropna(inplace=True)
    if len(encoded) < 5:
        raise ValueError("too few complete rows for the 80 to 20 split")
    trace = {
        "steps": [
            "six_character_ids",
            "drop_metadata",
            "median_before_split",
            "iqr_clipping",
            "first_level_indicators",
            "drop_incomplete",
            "split_80_20_seed_42",
            "intercept",
            "training_backward_elimination",
            "test_prediction",
        ],
        "medians": medians,
        "imputation_exclusions": sorted(IMPUTATION_EXCLUSIONS),
        "imputation_rows": len(frame),
        "clipping": clipping,
        "references": references,
        "predictors": encoded.drop(columns=["infection_index"]).columns.tolist(),
        "input_rows": len(frame),
        "complete_rows": len(encoded),
        "dropped_indices": [int(index) for index in frame.index if index not in encoded.index],
        "scaling": "none",
        "categorical_nulls": "all-zero indicators before incomplete-row removal",
    }
    return encoded, trace


def fit_model(encoded: Any, statsmodels: Any, splitter: Any, numpy: Any, mean_squared_error: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    predictors = encoded.drop("infection_index", axis=1)
    outcome = encoded["infection_index"]
    X_train, X_test, y_train, y_test = splitter(predictors, outcome, test_size=0.20, random_state=42)
    X_train_const = statsmodels.add_constant(X_train)
    if "const" not in X_train_const:
        raise ValueError("notebook add_constant produced no intercept; inspect constant candidate columns")
    X_opt = X_train_const.copy()
    elimination = []
    while True:
        model = statsmodels.OLS(y_train, X_opt).fit()
        p_values = model.pvalues.drop("const")
        max_p = p_values.max()
        if max_p > 0.05:
            variable = p_values.idxmax()
            elimination.append({"predictor": str(variable), "p_value": float(max_p)})
            X_opt = X_opt.drop(columns=[variable])
        else:
            break
    optimized = statsmodels.OLS(y_train, X_opt).fit()
    selected = X_opt.columns.drop("const").tolist()
    X_test_opt = statsmodels.add_constant(X_test[selected])
    predictions = optimized.predict(X_test_opt)
    training_r_squared = float(optimized.rsquared)
    test_rmse = float(numpy.sqrt(mean_squared_error(y_test, predictions)))
    if not math.isfinite(training_r_squared) or not math.isfinite(test_rmse):
        raise ValueError("fit cannot produce finite R squared and RMSE")
    denominator = float(((y_test - y_test.mean()) ** 2).sum())
    test_r_squared = 1.0 - float(((y_test - predictions) ** 2).sum()) / denominator if denominator else None
    split = {
        "seed": 42,
        "test_fraction": 0.20,
        "train_indices": [int(index) for index in X_train.index],
        "test_indices": [int(index) for index in X_test.index],
    }
    result = {
        "training_r_squared": training_r_squared,
        "training_adjusted_r_squared": float(optimized.rsquared_adj),
        "test_r_squared": test_r_squared,
        "test_rmse": test_rmse,
        "historical_training_r_squared_rounded": "0.102",
        "training_r_squared_rounded": f"{training_r_squared:.3f}",
        "rounds_to_0_102": f"{training_r_squared:.3f}" == "0.102",
        "historical_test_rmse": 0.334586774638517,
        "selected_predictors": selected,
        "historical_selected_predictors": list(HISTORICAL_TERMS),
        "selected_predictors_equal": set(selected) == set(HISTORICAL_TERMS),
        "coefficients": {str(name): float(value) for name, value in optimized.params.items()},
        "candidate_predictors": predictors.columns.tolist(),
        "elimination": elimination,
        "selection_rows": split["train_indices"],
        "training_rows": len(X_train),
        "test_rows": len(X_test),
        "inference": "nonrobust OLS",
        "p_value_threshold": 0.05,
        "scaling": "none",
        "study_inputs_or_outputs": "none",
        "runtime_versions": {name: importlib.metadata.version(name) for name in ("pandas", "statsmodels", "scikit-learn", "numpy", "scipy")},
    }
    return result, split


def model_artifacts(dataset: Path, output: Path) -> dict[str, Any]:
    pandas, statsmodels, imputer, splitter, numpy, mean_squared_error = training_libraries()
    frame = pandas.read_json(dataset, lines=True)
    if frame.empty:
        raise ValueError("empty regression dataset")
    encoded, trace = prepared_frame(frame, pandas, imputer)
    model, split = fit_model(encoded, statsmodels, splitter, numpy, mean_squared_error)
    rows = encoded.copy()
    rows.insert(0, "row_index", [int(index) for index in encoded.index])
    rows.insert(1, "facility_id", frame.loc[encoded.index, "facility_id"])
    artifacts = {"encoded_rows": write_checked(output / "encoded_rows.jsonl", (json_bytes(row) for row in rows.to_dict(orient="records")))}
    for name, value in (("preprocessing", trace), ("split", split), ("model", model)):
        artifacts[name] = write_checked(output / f"{name}.json", [json_bytes(value)])
    return {
        "artifacts": artifacts,
        "training_r_squared": model["training_r_squared"],
        "test_rmse": model["test_rmse"],
        "rounds_to_0_102": model["rounds_to_0_102"],
        "selected_predictors_equal": model["selected_predictors_equal"],
    }


def retention_label(output: Path) -> dict[str, Any]:
    expected = {"class": 3, "purpose": "Capstone replication run payload", "end_condition": "The result is accepted and its summary retained"}
    path = output / "retention.json"
    if path.exists():
        existing = json.loads(path.read_text())
        if not isinstance(existing, dict) or any(existing.get(key) != value for key, value in expected.items()) or "created" not in existing:
            raise ValueError("output conflict: retention.json; use a new run folder")
        return existing
    return {**expected, "created": datetime.now(UTC).isoformat()}


def main() -> int:
    """Validate immutable inputs, rebuild the capstone data and save actual model results without masking a mismatch."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--curated-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--run-set", type=Path, help="JSON object naming exactly one Glue run for each joined table")
    parser.add_argument("--classification", choices=("Public", "Synthetic"), default="Public")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--enumerate-run-sets", action="store_true")
    mode.add_argument("--inspect-only", action="store_true")
    mode.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    if not args.enumerate_run_sets and args.run_set is None:
        parser.error("--run-set is required unless --enumerate-run-sets is selected")
    if args.enumerate_run_sets and args.run_set is not None:
        parser.error("--run-set cannot be combined with --enumerate-run-sets")
    output = args.run_dir.resolve()
    audit_written = False
    try:
        curated = args.curated_root.resolve()
        if output == curated or curated in output.parents:
            raise ValueError("run folder must be outside the curated inputs")
        if args.enumerate_run_sets:
            write_checked(output / "retention.json", [json_bytes(retention_label(output))])
            report = enumerate_run_sets(curated, args.manifest, args.manifest_sha256, output, args.classification)
            sys.stdout.write(json.dumps({"aligned_sets": len(report["sets"]), "candidates": report["candidates"], "model_executed": False}) + "\n")
            return 0
        run_set = read_run_set(args.run_set)
        tables, audit = checked_tables(curated, args.manifest, args.manifest_sha256, run_set)
        if audit["joined_rows"] == 0 or audit["duplicate_facilities"]:
            raise ValueError(f"run set must give one row per facility: {audit['joined_rows']} rows, {audit['duplicate_facilities']} duplicate facilities")
        audit["classification"] = args.classification
        summary: dict[str, Any] = {
            "run_name": "m0_capstone_replication",
            "run_set": run_set,
            "implementation_sha256": file_sha256(Path(__file__)),
            "input_audit": audit,
            "mode": "inspect" if args.inspect_only else "prepare" if args.prepare_only else "model",
            "model_executed": False,
            "r_squared_match": "not evaluated",
            "limits": [ROW_ORDER, "The historical metric is training R squared. A rounded match alone does not establish exact cohort or split replication."],
        }
        write_checked(output / "retention.json", [json_bytes(retention_label(output))])
        summary["input_audit_artifact"] = write_checked(output / "input_audit.json", [json_bytes(audit)])
        audit_written = True
        if not args.inspect_only and not args.prepare_only:
            training_libraries()
        if not args.inspect_only:
            summary["dataset"] = write_checked(output / "regression_dataset.jsonl", (json_bytes(row, sort_keys=False) for row in joined_rows(tables)))
            del tables
            if not args.prepare_only:
                summary.update(model_artifacts(output / "regression_dataset.jsonl", output))
                summary["model_executed"] = True
                summary["r_squared_match"] = summary["rounds_to_0_102"]
        summary["status"] = "complete" if summary["model_executed"] else "inspected" if args.inspect_only else "prepared"
        write_checked(output / "summary.json", [json_bytes(summary)])
        sys.stdout.write(
            json.dumps(
                {
                    "mode": summary["mode"],
                    "joined_rows": audit["joined_rows"],
                    "model_executed": summary["model_executed"],
                    "r_squared_match": summary["r_squared_match"],
                },
                sort_keys=True,
            )
            + "\n"
        )
    except (ValueError, OSError, KeyError, TypeError) as error:
        if audit_written:
            failure = {
                "status": "blocked" if "training image required" in str(error) else "failed",
                "error": str(error),
                "implementation_sha256": file_sha256(Path(__file__)),
                "model_executed": False,
            }
            payload = json_bytes(failure)
            write_checked(output / "failures" / (hashlib.sha256(payload).hexdigest() + ".json"), [payload])
        sys.stderr.write(f"capstone replication: {error}\n")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
