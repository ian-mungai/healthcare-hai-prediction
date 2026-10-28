"""Exercise the replication CLI with synthetic curated parts and independent expectations.

Dataset cases run without the training libraries. Model cases require the pinned training image.
Use ``--implementation`` to run the same cases against an owned planted-defect copy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
IMPLEMENTATION = ROOT / "scripts/training/capstone_replication.py"
TABLES = ("hai", "hospital_characteristics", "patient_experience", "patient_safety", "timely_effective_care", "infection_prevention")
SYNTHETIC_RUN = "run-1000"
CAP_COLUMNS = ("wound_dehiscence_rate", "postop_sepsis_rate", "pressure_ulcer_rate", "postop_respiratory_failure_rate")
TEST_INDICES = [
    177,
    182,
    113,
    67,
    103,
    17,
    10,
    18,
    143,
    126,
    111,
    97,
    20,
    166,
    87,
    122,
    106,
    68,
    58,
    44,
    32,
    26,
    157,
    99,
    70,
    129,
    125,
    21,
    47,
    77,
    170,
    69,
    6,
    134,
    171,
    165,
    80,
    114,
]


def require(condition: bool, message: str) -> None:
    """Fail a synthetic case with its intended contract name."""
    if not condition:
        raise ValueError(message)


def save(path: Path, value: Any) -> None:
    """Write an owned synthetic fixture or local case record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n")


def fixture_rows(size: int = 192, prophylaxis: bool = True) -> dict[str, list[dict[str, Any]]]:
    """Generate synthetic public-schema rows with missing values, outliers and categorical contrasts."""
    rows: dict[str, list[dict[str, Any]]] = {name: [] for name in TABLES}
    for i in range(size):
        values = [int(hashlib.sha256(f"731|{i}|{j}".encode()).hexdigest()[:13], 16) / 16**13 for j in range(24)]
        identifier = str(49 if i == 0 else 100000 + i)
        rating = float(1 + int(5 * values[0]))
        embolism = 1 + 11 * values[1]
        length = 40 + 360 * values[2]
        outcome = 0.04 * length + 2.0 * embolism - 3.0 * rating + (values[3] - 0.5) * 0.8
        rows["hai"].append({"facility_id": identifier, "hospital_name": f"Synthetic HAI {i}", "infection_index": None if i == 5 else outcome})
        rows["hospital_characteristics"].append(
            {
                "facility_id": identifier,
                "hospital_name": "Different synthetic name",
                "hospital_type": None if i == 7 else "Acute Care Hospitals",
                "hospital_ownership": ["Government - Federal", "Proprietary", "Voluntary non-profit - Private"][int(3 * values[4])],
                "emergency_services": ["Yes", "No"][int(2 * values[5])],
                "hospital_overall_rating": rating,
                "state": "XX",
                "region": None if i == 13 else ["Midwest", "Northeast", "South", "West"][int(4 * values[6])],
            }
        )
        rows["patient_experience"].append(
            {
                "facility_id": identifier,
                "hospital_name": "Experience synthetic",
                **{
                    c: float(1 + int(5 * values[7 + j]))
                    for j, c in enumerate(
                        (
                            "cleanliness_score",
                            "nurse_communication",
                            "doctor_communication",
                            "medication_communication",
                            "discharge_information",
                            "recommend_hospital",
                        )
                    )
                },
            }
        )
        rows["patient_safety"].append(
            {
                "facility_id": identifier,
                "hospital_name": "Safety synthetic",
                "pulmonary_embolism_dvt_rate": embolism,
                **{c: 1000.0 if i == 7 else 10 * values[13 + j] for j, c in enumerate(CAP_COLUMNS)},
            }
        )
        care: dict[str, Any] = {
            "facility_id": identifier,
            "hospital_name": "Care synthetic",
            "healthcare_worker_flu_vaccination": 60 + 40 * values[17],
            "ed_length_of_stay_minutes": None if i % 7 == 0 else length,
            "opioid_safety_compliance": 100 * values[18],
            "sepsis_care_compliance": 100 * values[19],
            "ed_volume": ["high", "low", "medium", "very high"][int(4 * values[20])],
        }
        if prophylaxis:
            care["vte_prophylaxis_compliance"] = None if i == 11 else 100 * values[21]
        rows["timely_effective_care"].append(care)
    rows["infection_prevention"] = [
        {"facility_id": "unused", "hospital_name": "Unused synthetic", "flu_vaccination_rate": 70.0, "covid_vaccination_rate": 40.0}
    ]
    return rows


def write_fixture(folder: Path, rows: dict[str, list[dict[str, Any]]]) -> tuple[Path, Path, str]:
    """Write JSON lines and a manifest with the same prefix as the supplied curated copy."""
    curated = folder / "curated"
    lines = []
    for table in TABLES:
        directory = curated / table
        directory.mkdir(parents=True)
        midpoint = len(rows[table]) // 2
        for number, group in enumerate((rows[table][:midpoint], rows[table][midpoint:])):
            payload = "".join(json.dumps(row, sort_keys=True, allow_nan=False) + "\n" for row in group).encode()
            name = f"{SYNTHETIC_RUN}-part-r-{number:05d}"
            (directory / name).write_bytes(payload)
            lines.append(f"{hashlib.sha256(payload).hexdigest()}  data/capstone_replication/curated/{table}/{name}\n")
    manifest = folder / "curated_sha256.txt"
    manifest.write_text("".join(lines))
    return curated, manifest, hashlib.sha256(manifest.read_bytes()).hexdigest()


def invoke(
    implementation: Path,
    folder: Path,
    rows: dict[str, list[dict[str, Any]]],
    *,
    mode: str = "prepare",
    mutate: Callable[[Path, Path], None] | None = None,
    rehash_manifest: bool = False,
    run_set: dict[str, Any] | None = None,
    omit_run_set: bool = False,
) -> tuple[int, str, Path]:
    """Run the production file with a bounded process call and save its complete result."""
    curated, manifest, digest = write_fixture(folder, rows)
    if mutate:
        mutate(curated, manifest)
    if rehash_manifest:
        digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    subject = folder / "subject"
    package = subject / "scripts/training"
    package.mkdir(parents=True)
    (subject / "scripts/__init__.py").write_text("")
    (package / "__init__.py").write_text("")
    (package / "capstone_replication.py").write_bytes(implementation.read_bytes())
    output = folder / "run"
    arguments = [
        "-m",
        "scripts.training.capstone_replication",
        "--curated-root",
        str(curated),
        "--manifest",
        str(manifest),
        "--manifest-sha256",
        digest,
        "--run-dir",
        str(output),
        "--classification",
        "Synthetic",
    ]
    if mode == "enumerate":
        arguments.append("--enumerate-run-sets")
    elif not omit_run_set:
        selection = folder / "run_set.json"
        save(selection, run_set if run_set is not None else {table: SYNTHETIC_RUN for table in TABLES[:5]})
        arguments.extend(("--run-set", str(selection)))
    if mode not in ("model", "enumerate"):
        arguments.append("--inspect-only" if mode == "inspect" else "--prepare-only")
    result = run_command(sys.executable, arguments, cwd=subject, timeout=90)
    save(
        folder / "command.json",
        {"argv": [sys.executable, *arguments], "cwd": str(subject), "returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr},
    )
    return result.returncode, result.stdout + result.stderr, output


def inner_join_case(implementation: Path, folder: Path) -> None:
    rows = fixture_rows(4)
    for table in TABLES[:5]:
        rows[table] = rows[table][:2]
    rows["patient_safety"] = rows["patient_safety"][:1]
    rows["hai"].append({"facility_id": "000049", "hospital_name": "Unmatched padded key", "infection_index": 1.0})
    code, text, output = invoke(implementation, folder, rows)
    require(code == 0, "inner_join: command failed: " + text)
    actual = [json.loads(line) for line in (output / "regression_dataset.jsonl").read_text().splitlines()]
    require(len(actual) == 1, "inner_join: expected one matched facility and no unmatched key")
    require(all(r["facility_id"] == "49" for r in actual), "raw_keys: padding must follow joins")
    require(all(r["hospital_name"] == "Synthetic HAI 0" for r in actual), "name_precedence: HAI name must survive")
    require(all(not c.startswith(".") and c != "facility_name" for r in actual for c in r), "drop_fields: right duplicate fields survive")
    require(all("flu_vaccination_rate" not in r for r in actual), "unused_table: infection prevention entered the join")


def rejection_case(implementation: Path, folder: Path, mutation: str) -> None:
    rows = fixture_rows(4)
    if mutation == "wrong_type":
        rows["hai"][0]["infection_index"] = "wrong"
    elif mutation == "unknown_column":
        rows["hai"][0]["unreviewed_column"] = 7
    causes = {
        "changed_part": "file hash mismatch",
        "unused_hash": "file hash mismatch",
        "missing_part": "inventory mismatch",
        "extra_part": "inventory mismatch",
        "changed_manifest": "manifest hash mismatch",
        "wrong_type": "field type",
        "unknown_column": "unknown column",
        "duplicate_manifest": "duplicate manifest path",
        "path_traversal": "manifest path outside curated root",
        "missing_table": "manifest inventory",
        "malformed_manifest": "manifest format",
    }

    def change(curated: Path, manifest: Path) -> None:
        lines = manifest.read_text().splitlines(keepends=True)
        if mutation == "duplicate_manifest":
            manifest.write_text("".join([*lines, lines[0]]))
        elif mutation == "path_traversal":
            manifest.write_text(lines[0].replace("data/capstone_replication/curated/hai/run-1000-part-r-00000", "../outside") + "".join(lines[1:]))
        elif mutation == "missing_table":
            manifest.write_text("".join(lines[:-2]))
        elif mutation == "malformed_manifest":
            manifest.write_text("invalid checksum row\n")
        if mutation in {"changed_part", "unused_hash"}:
            table = "hai" if mutation == "changed_part" else "infection_prevention"
            with (curated / table / "run-1000-part-r-00000").open("a") as handle:
                handle.write("\n")
        elif mutation == "missing_part":
            (curated / "hai" / "run-1000-part-r-00000").unlink()
        elif mutation == "extra_part":
            (curated / "hai" / "extra").write_text("{}\n")
        elif mutation == "changed_manifest":
            with manifest.open("a") as handle:
                handle.write("\n")

    code, text, output = invoke(
        implementation, folder, rows, mutate=change, rehash_manifest=mutation in {"duplicate_manifest", "path_traversal", "missing_table", "malformed_manifest"}
    )
    require(code != 0 and causes[mutation] in text, f"{mutation}: expected rejection for {causes[mutation]}: {text}")
    require(not (output / "regression_dataset.jsonl").exists(), f"{mutation}: wrote a dataset before rejecting the input")


def repeat_case(implementation: Path, folder: Path) -> None:
    rows = fixture_rows(8)
    rows["timely_effective_care"].reverse()
    code, text, output = invoke(implementation, folder, rows)
    require(code == 0, "repeat: first command failed: " + text)
    actual = [json.loads(line) for line in (output / "regression_dataset.jsonl").read_text().splitlines()]
    require(
        [row["facility_id"] for row in actual] == ["100007", "100006", "100005", "100004", "100003", "100002", "100001", "49"],
        "row_order: timely-care part and line order must determine the emitted facility sequence",
    )
    before = {p.name: p.read_bytes() for p in output.iterdir() if p.is_file()}
    command = json.loads((folder / "command.json").read_text())
    arguments = command["argv"]
    result = run_command(sys.executable, ["-m", "scripts.training.capstone_replication", *arguments[3:]], cwd=Path(command["cwd"]), timeout=90)
    require(result.returncode == 0, "repeat: identical rerun failed: " + result.stderr)
    require(before == {p.name: p.read_bytes() for p in output.iterdir() if p.is_file()}, "repeat: rerun changed output bytes")
    (output / "regression_dataset.jsonl").write_text("unrelated existing content\n")
    result = run_command(sys.executable, ["-m", "scripts.training.capstone_replication", *arguments[3:]], cwd=Path(command["cwd"]), timeout=90)
    require(result.returncode != 0 and "output conflict" in result.stderr, "conflict: existing content was not protected")
    require((output / "regression_dataset.jsonl").read_text() == "unrelated existing content\n", "conflict: existing content was overwritten")


def inspect_case(implementation: Path, folder: Path) -> None:
    code, text, output = invoke(implementation, folder, fixture_rows(8), mode="inspect")
    require(code == 0, "inspect: command failed: " + text)
    plan = json.loads((output / "input_audit.json").read_text())
    require(plan["classification"] == "Synthetic", "inspect: generated data must retain its Synthetic label")
    require(plan["joined_rows"] == 8 and len(plan["joins"]) == 4, "inspect: incorrect cardinality or join count")
    require(not (output / "regression_dataset.jsonl").exists(), "inspect: materialized the dataset")


def sparse_case(implementation: Path, folder: Path) -> None:
    rows = fixture_rows(4)
    del rows["patient_safety"][0]["postop_sepsis_rate"]
    rows["hai"].append({"facility_id": None, "hospital_name": "Synthetic null key", "infection_index": 1.0})
    for table in TABLES[1:5]:
        rows[table].append({"facility_id": None, "hospital_name": "Synthetic null right key"})
    code, text, output = invoke(implementation, folder, rows)
    require(code == 0, "sparse: command failed: " + text)
    actual = [json.loads(line) for line in (output / "regression_dataset.jsonl").read_text().splitlines()]
    require(len(actual) == 4, "sparse: null join keys must not match")
    require(actual[0]["postop_sepsis_rate"] is None, "sparse: an omitted numeric field must stay null")
    require(isinstance(actual[1]["postop_sepsis_rate"], float), "sparse: source double type changed")
    require(list(actual[0])[:2] == ["ed_length_of_stay_minutes", "sepsis_care_compliance"], "column_order: notebook candidate order changed")


def run_selection_case(implementation: Path, folder: Path, selected_case: str) -> None:
    rows = fixture_rows(8)
    choice: dict[str, Any] = {table: SYNTHETIC_RUN for table in TABLES[:5]}
    if selected_case == "duplicate_facilities":
        rows["hospital_characteristics"].append(dict(rows["hospital_characteristics"][0]))
    elif selected_case == "mixed_runs":
        choice["hai"] = [SYNTHETIC_RUN, "run-2000"]
    elif selected_case == "incomplete_run_set":
        del choice["hai"]
    elif selected_case == "unknown_run":
        choice["hai"] = "run-9999"
    elif selected_case == "selected_run_and_record":
        choice = {table: "run-2000" for table in TABLES[:5]}

    def add_runs(curated: Path, manifest: Path) -> None:
        with manifest.open("a") as handle:
            for table in TABLES[:5]:
                for part in sorted((curated / table).iterdir()):
                    name = part.name.replace(SYNTHETIC_RUN, "run-2000")
                    records = [json.loads(line) for line in part.read_text().splitlines()]
                    if table == "hai":
                        for record in records:
                            record["infection_index"] = -100.0
                    payload = "".join(json.dumps(record) + "\n" for record in records).encode()
                    (part.parent / name).write_bytes(payload)
                    handle.write(f"{hashlib.sha256(payload).hexdigest()}  data/capstone_replication/curated/{table}/{name}\n")

    code, text, output = invoke(
        implementation,
        folder,
        rows,
        run_set=choice,
        omit_run_set=selected_case == "missing_run_set",
        mutate=add_runs if selected_case in ("mixed_runs", "selected_run_and_record") else None,
        rehash_manifest=selected_case in ("mixed_runs", "selected_run_and_record"),
    )
    if selected_case == "selected_run_and_record":
        require(code == 0, "selected_run: command failed: " + text)
        actual = [json.loads(line) for line in (output / "regression_dataset.jsonl").read_text().splitlines()]
        summary = json.loads((output / "summary.json").read_text())
        require(len(actual) == 8 and all(row["infection_index"] == -100.0 for row in actual), "selected_run: expected every part of only run-2000")
        require(summary["run_set"] == choice and summary["input_audit"]["run_set"] == choice, "selected_run: run set missing from record")
        require(len(summary["input_audit"]["selected_files"]) == 10, "selected_run: expected both parts of every joined table")
    else:
        causes = {
            "duplicate_facilities": "one row per facility",
            "mixed_runs": "exactly one Glue run per joined table",
            "incomplete_run_set": "exactly one Glue run per joined table",
            "missing_run_set": "--run-set is required",
            "unknown_run": "selected Glue run not found: hai=run-9999",
        }
        require(code != 0 and causes[selected_case] in text, f"{selected_case}: expected refusal for {causes[selected_case]}: {text}")
        require(not (output / "regression_dataset.jsonl").exists(), f"{selected_case}: wrote a dataset before refusal")


def enumeration_case(implementation: Path, folder: Path) -> None:
    rows = fixture_rows(4)
    for table in TABLES[:5]:
        for row in rows[table]:
            for column in ("emergency_services", "region", "hospital_ownership", "ed_volume"):
                if column in row:
                    row[column] = "Single synthetic level"
    code, text, output = invoke(implementation, folder, rows, mode="enumerate")
    require(code == 0, "enumeration: command failed: " + text)
    report = json.loads((output / "run_sets.json").read_text())
    require(len(report["sets"]) == 1, "enumeration: expected one aligned set")
    choice = report["sets"][0]
    require(choice["run_set"] == {table: SYNTHETIC_RUN for table in TABLES[:5]}, "enumeration: incorrect aligned run mapping")
    require(choice["joined_rows"] == 4 and choice["duplicate_facilities"] == 0, "enumeration: incorrect row or duplicate count")
    require(
        choice["prepared_shape"]["encoded_columns"] == 18 and choice["prepared_shape"]["predictor_columns"] == 17,
        "enumeration: incorrect notebook column count",
    )
    require(choice["candidate"] is False and choice["prepared_shape"]["complete_rows"] == 4, "enumeration: wrong cohort criterion")
    require(not (output / "regression_dataset.jsonl").exists(), "enumeration: wrote a dataset")


def quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def model_case(implementation: Path, folder: Path, selected_case: str) -> None:
    rows = fixture_rows(prophylaxis=selected_case != "absent_prophylaxis")
    if selected_case == "median_rounding":
        present = [row for row in rows["timely_effective_care"] if row["ed_length_of_stay_minutes"] is not None]
        middle = {(len(present) - 1) // 2, len(present) // 2}
        for index, row in enumerate(present):
            row["ed_length_of_stay_minutes"] = 1.2345678901234567 if index in middle else 0.5 + index / 1000 if index < min(middle) else 2 + index / 1000
    code, text, output = invoke(implementation, folder, rows, mode="model")
    require(code == 0, "model_runtime: " + text)
    preparation = json.loads((output / "preprocessing.json").read_text())
    encoded = [json.loads(line) for line in (output / "encoded_rows.jsonl").read_text().splitlines()]
    split = json.loads((output / "split.json").read_text())
    model = json.loads((output / "model.json").read_text())
    by_index = {row["row_index"]: row for row in encoded}
    if selected_case == "absent_prophylaxis":
        require(
            "vte_prophylaxis_compliance" not in preparation["predictors"] and len(encoded) == 191 and 11 in by_index,
            "absent_prophylaxis: an absent source column was manufactured or dropped valid rows",
        )
    if selected_case in {"all", "median_population", "median_rounding"}:
        expected = statistics.median(r["ed_length_of_stay_minutes"] for r in rows["timely_effective_care"] if r["ed_length_of_stay_minutes"] is not None)
        if selected_case == "median_rounding":
            require(preparation["medians"]["ed_length_of_stay_minutes"] != expected, "median_rounding: fixture must expose default parser rounding")
        require(
            math.isclose(preparation["medians"]["ed_length_of_stay_minutes"], expected, rel_tol=1e-12),
            "median_population: median must use every row before splitting",
        )
        require(math.isclose(by_index[0]["ed_length_of_stay_minutes"], expected, rel_tol=1e-12), "median_population: imputation value differs")
        require(
            "infection_index" not in preparation["medians"] and "vte_prophylaxis_compliance" not in preparation["medians"],
            "imputation_exclusions: excluded field was imputed",
        )
        require(by_index[0]["facility_id"] == "000049", "identifier: facility ID must have six characters")
        require(
            all(c not in preparation["predictors"] for c in ("hospital_type", "facility_id", "hospital_name", "state")), "metadata: a removed field entered OLS"
        )
        require(5 not in by_index and 11 not in by_index and 7 in by_index and 13 in by_index, "incomplete_rows: wrong drop order or categorical null rule")
    if selected_case in {"all", "clipping"}:
        for column in CAP_COLUMNS:
            values = [row[column] for row in rows["patient_safety"]]
            q1, q3 = quantile(values, 0.25), quantile(values, 0.75)
            upper = q3 + 1.5 * (q3 - q1)
            require(math.isclose(by_index[7][column], upper, abs_tol=1e-10), f"clipping: outlier in {column} was not capped")
            require(math.isclose(preparation["clipping"][column]["lower"], q1 - 1.5 * (q3 - q1), abs_tol=1e-10), "clipping: wrong lower bound")
    if selected_case in {"all", "reference_levels"}:
        references = {"emergency_services": "No", "region": "Midwest", "hospital_ownership": "Government - Federal", "ed_volume": "high"}
        require(preparation["references"] == references, "reference_levels: expected the first sorted category as reference")
        require("region_Midwest" not in by_index[0] and "region_West" in by_index[0], "reference_levels: wrong omitted indicator")
        require(
            all(by_index[13]["region_" + level] == 0 for level in ("Northeast", "South", "West")),
            "reference_levels: null categories must keep the notebook's all-zero indicators",
        )
    if selected_case in {"all", "split_membership"}:
        require(split["test_indices"] == TEST_INDICES, "split_membership: seed 42 or the 20 percent split changed")
        require(split["train_indices"][:10] == [53, 37, 120, 62, 163, 78, 71, 31, 130, 154], "split_membership: training row order differs")
    if selected_case in {"all", "training_selection"}:
        selected = model["selected_predictors"]
        require(
            "pulmonary_embolism_dvt_rate" in selected and "hospital_overall_rating" in selected, "candidate_matrix: study exclusions reached the replication"
        )
        require("const" in model["coefficients"], "intercept: intercept was removed")
        require(model["selection_rows"] == split["train_indices"], "training_selection: selection used another population")
        require(all(step["p_value"] > 0.05 and step["predictor"] != "const" for step in model["elimination"]), "training_selection: invalid removal")
        require(model["rounds_to_0_102"] == (f"{model['training_r_squared']:.3f}" == "0.102"), "mismatch: reported match disagrees with the actual fit")
        require(model["rounds_to_0_102"] is False, "mismatch: synthetic data unexpectedly claims the historical score")
        observed = []
        for index in split["test_indices"]:
            row = by_index[index]
            prediction = model["coefficients"]["const"] + sum(model["coefficients"][column] * row[column] for column in selected)
            observed.append((prediction - row["infection_index"]) ** 2)
        require(
            math.isclose(model["test_rmse"], math.sqrt(statistics.mean(observed)), abs_tol=1e-9),
            "rmse: coefficients do not reproduce test error in original units",
        )
        for index in TEST_INDICES:
            rows["hai"][index]["infection_index"] = 1000000.0 + index * 7000.0
        changed_code, changed_text, changed_output = invoke(implementation, folder / "test_outcome_change", rows, mode="model")
        require(changed_code == 0, "training_selection: changed-outcome command failed: " + changed_text)
        changed = json.loads((changed_output / "model.json").read_text())
        require(
            changed["coefficients"] == model["coefficients"] and changed["elimination"] == model["elimination"],
            "training_selection: changing test outcomes changed the trained model",
        )


def main() -> int:
    """Run named CLI cases and retain a repeatable report, including failures."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--implementation", type=Path, default=IMPLEMENTATION)
    parser.add_argument("--scope", choices=("dataset", "model", "all"), default="all")
    parser.add_argument("--case", default="all")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    save(
        output / "retention.json",
        {
            "class": 5,
            "purpose": "Synthetic replication regression evidence",
            "created": datetime.now(UTC).isoformat(),
            "end_condition": "A newer passing run replaces this run",
        },
    )
    implementation = args.implementation.resolve()
    digest = hashlib.sha256(implementation.read_bytes()).hexdigest()
    cases: list[tuple[str, Callable[[Path, Path], None]]] = []
    if args.scope != "model":
        cases.extend([("inner_join", inner_join_case), ("repeat_and_conflict", repeat_case), ("inspect", inspect_case), ("sparse_and_null_keys", sparse_case)])
        for name in ("duplicate_facilities", "mixed_runs", "incomplete_run_set", "missing_run_set", "unknown_run", "selected_run_and_record"):
            cases.append((name, partial(run_selection_case, selected_case=name)))
        cases.append(("enumeration_shape", enumeration_case))
        for mutation in (
            "changed_part",
            "unused_hash",
            "missing_part",
            "extra_part",
            "changed_manifest",
            "wrong_type",
            "unknown_column",
            "duplicate_manifest",
            "path_traversal",
            "missing_table",
            "malformed_manifest",
        ):
            cases.append((mutation, partial(rejection_case, mutation=mutation)))
    if args.scope != "dataset":
        for name in ("median_population", "median_rounding", "clipping", "reference_levels", "split_membership", "training_selection", "absent_prophylaxis"):
            cases.append((name, partial(model_case, selected_case=name)))
    cases = [(name, case) for name, case in cases if args.case in ("all", name)]
    if not cases:
        parser.error("no case selected")
    results = []
    for name, case in cases:
        folder = output / name
        folder.mkdir()
        try:
            case(implementation, folder)
            results.append({"case": name, "status": "passed"})
        except (ValueError, OSError, TimeoutExpired) as error:
            results.append({"case": name, "status": "blocked" if "training image required" in str(error) else "failed", "cause": str(error)})
    unchanged = hashlib.sha256(implementation.read_bytes()).hexdigest() == digest
    report = {
        "implementation_sha256": digest,
        "implementation_unchanged": unchanged,
        "classification": "Synthetic",
        "scope": args.scope,
        "cases": results,
        "cleanup": "done",
        "limits": "Dataset cases do not execute pandas, scikit-learn or statsmodels. Model cases use only synthetic data.",
    }
    save(output / "report.json", report)
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    if any(result["status"] == "blocked" for result in results):
        return 2
    return 0 if unchanged and all(result["status"] == "passed" for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
