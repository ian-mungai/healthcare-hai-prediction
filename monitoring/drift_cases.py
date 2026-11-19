"""E2E cases for the drift check on synthetic fixtures: no real patient, hospital or CMS record is read or written.

Every case writes small Parquet matrices with DuckDB from a fixed hash, starts ``python -m monitoring.drift_check`` as a child process and judges its exit
code, its message and the record file it leaves. The cases that need Evidently run only when ``.tools/evidently`` exists and are reported as blocked
otherwise, never as passed. The host environment has no Evidently by design, so a case that starts the check with it must see the dependency named.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
from jsonschema import Draft202012Validator

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[1]
PYTHON = str(ROOT / ".venv/bin/python")
EVIDENTLY_PYTHON = ROOT / ".tools/evidently/bin/python"
EVIDENTLY_PROGRAM = str(EVIDENTLY_PYTHON)
RECORD_SCHEMA = ROOT / "data_contracts/monitoring/drift_record.schema.json"
TIMEOUT = 600
RUN_ID = "drift_elastic_net_0a1b2c3d"
NUMERIC = ("f_num_a", "f_num_b", "f_num_c", "f_num_d")
FEATURES = (*NUMERIC, "f_cat")
Problems = list[str]


class Blocked(Exception):
    """The case needs a tool this machine does not have."""


@dataclass
class Fixture:
    """The knobs of one synthetic pair of matrices."""

    shifted: tuple[str, ...] = ()
    reference_rows: int = 1500
    current_rows: int = 1200
    reference_newest_year: int = 2021
    current_year: int = 2022
    population: str = "primary"
    current_population: str = "primary"
    hai_type: str = "HAI_1"
    current_hai_type: str = "HAI_1"
    features: tuple[str, ...] = FEATURES
    null_in_reference: tuple[str, ...] = ()
    null_in_current: tuple[str, ...] = ()
    mostly_null_in_reference: str | None = None
    drop_from_current: str | None = None
    drop_from_reference: str | None = None
    cat_as_number_in_current: bool = False
    corrupt_reference: str = ""
    corrupt_current: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def column_sql(name: str, shifted: bool) -> str:
    if name == "f_cat":
        draw = f"hash('{name}' || i::VARCHAR)"
        return f"CASE WHEN {draw} % 10 < 8 THEN 'x' ELSE 'y' END" if shifted else f"(['x', 'y', 'z'])[({draw} % 3)::BIGINT + 1]"
    unit = f"(hash('{name}' || i::VARCHAR) % 1000000) / 1000000.0"
    return f"{unit} + 0.5" if shifted else unit


def write_matrix(
    path: Path,
    rows: int,
    year_of: str,
    shifted: tuple[str, ...],
    null: tuple[str, ...],
    drop: str | None,
    numeric_cat: bool = False,
    mostly_null: str | None = None,
) -> None:
    columns = []
    for name in FEATURES:
        if name == drop:
            continue
        expression = ("NULL::VARCHAR" if name == "f_cat" else "NULL::DOUBLE") if name in null else column_sql(name, name in shifted)
        if name == mostly_null:
            expression = f"CASE WHEN i % 10 = 0 THEN {expression} ELSE NULL END"
        if numeric_cat and name == "f_cat":
            expression = "(hash('f_cat' || i::VARCHAR) % 3)::DOUBLE"
        columns.append(f"{expression} AS {name}")
    projection = f"'k' || i::VARCHAR AS spine_key, {year_of} AS window_year, (hash('label' || i::VARCHAR) % 1000) / 500.0 AS label_value, {', '.join(columns)}"
    duckdb.sql("SELECT range AS i FROM range(?)", params=[rows]).project(projection).write_parquet(str(path))


def corrupt(path: Path, mode: str) -> None:
    """Damage a written Parquet file the way a failed copy or a bad disk does."""
    raw = path.read_bytes()
    damaged = {"garbage": b"not a parquet file", "truncated": raw[:-20], "footer": raw[:-40] + b"\xff" * 30 + raw[-10:]}[mode]
    path.write_bytes(damaged)


def write_inputs(folder: Path, fixture: Fixture) -> dict[str, Path]:
    start = fixture.reference_newest_year - 2
    reference, current = folder / "reference.parquet", folder / "current.parquet"
    write_matrix(
        reference,
        fixture.reference_rows,
        f"{start} + i // ({fixture.reference_rows} // 3 + 1)",
        (),
        fixture.null_in_reference,
        fixture.drop_from_reference,
        mostly_null=fixture.mostly_null_in_reference,
    )
    write_matrix(
        current,
        fixture.current_rows,
        str(fixture.current_year),
        fixture.shifted,
        fixture.null_in_current,
        fixture.drop_from_current,
        fixture.cat_as_number_in_current,
    )
    for path, mode in ((reference, fixture.corrupt_reference), (current, fixture.corrupt_current)):
        if mode:
            corrupt(path, mode)
    paths = {"reference-matrix": reference, "current-matrix": current}
    for name, hai_type, population, extra in (
        ("reference-manifest", fixture.hai_type, fixture.population, {"newest_window_year": fixture.reference_newest_year}),
        ("current-manifest", fixture.current_hai_type, fixture.current_population, {}),
    ):
        paths[name] = folder / f"{name}.json"
        paths[name].write_text(json.dumps({"hai_type": hai_type, "population": population, **extra}) + "\n")
    paths["features"] = folder / "features.json"
    paths["features"].write_text(json.dumps(list(fixture.features)) + "\n")
    return paths


def run_check(folder: Path, fixture: Fixture, *, evidently: bool = False, config: Path | None = None, out: Path | None = None) -> tuple[int, str, Path]:
    """Write the fixture, start the check on it and return its exit code, text and output folder."""
    paths = write_inputs(folder, fixture)
    out = out or folder / "out"
    arguments = [f"--{name}={path}" for name, path in paths.items()] + [f"--run-id={RUN_ID}", f"--out={out}"]
    if config is not None:
        arguments.append(f"--config={config}")
    done = run_command(EVIDENTLY_PROGRAM if evidently else PYTHON, ["-m", "monitoring.drift_check", *arguments], cwd=ROOT, timeout=TIMEOUT)
    return done.returncode, done.stdout + done.stderr, out


def need_evidently() -> bool:
    if not EVIDENTLY_PYTHON.is_file():
        raise Blocked("needs .tools/evidently (an install the lead must approve)")
    return True


def read_record(out: Path, hai_type: str = "HAI_1") -> dict[str, Any]:
    return json.loads((out / f"drift_{hai_type}.json").read_text())


def schema_problems(record: dict[str, Any]) -> Problems:
    validator = Draft202012Validator(json.loads(RECORD_SCHEMA.read_text()))
    return [f"record breaks its schema: {error.message}" for error in validator.iter_errors(record)][:3]


def skipped(reason: str, fixture: Fixture) -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        code, text, out = run_check(folder, fixture)
        if code != 0:
            return [f"exit {code}: {text[:300]}"]
        record = read_record(out)
        problems = schema_problems(record)
        if record["schema_version"] != 2:
            problems.append(f"the record is schema version {record['schema_version']}, the writer's is 2")
        if record["status"] != "skipped" or record["skip_reason"] != reason:
            problems.append(f"expected a skipped record with reason {reason}, got {record['status']} {record['skip_reason']}")
        if record["triggered"]:
            problems.append("a skipped check triggered retraining")
        return problems

    return case


def refused(name: str, fixture: Fixture, detail: str = "") -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        code, text, out = run_check(folder, fixture)
        problems = [] if code == 2 else [f"expected exit 2, got {code}: {text[:300]}"]
        for needle in (name, detail):
            if needle not in text:
                problems.append(f"the message does not name {needle}: {text[:300]}")
        if list(out.glob("drift_*.json")):
            problems.append("a refused check left a record")
        return problems

    return case


def stalled_input_hits_the_deadline(folder: Path) -> Problems:
    """A manifest that never delivers (a named pipe, as a hung mount would) must end the check at the configured deadline."""
    config = folder / "drift.json"
    document = json.loads((ROOT / "config/monitoring/drift.json").read_text())
    document["deadline_seconds"] = 1
    config.write_text(json.dumps(document) + "\n")
    fixture = Fixture()
    paths = write_inputs(folder, fixture)
    paths["reference-manifest"].unlink()
    os.mkfifo(paths["reference-manifest"])
    arguments = [f"--{name}={path}" for name, path in paths.items()] + [f"--run-id={RUN_ID}", f"--out={folder / 'out'}", f"--config={config}"]
    done = run_command(PYTHON, ["-m", "monitoring.drift_check", *arguments], cwd=ROOT, timeout=60)
    text = done.stdout + done.stderr
    problems = [] if done.returncode == 2 else [f"expected exit 2, got {done.returncode}: {text[:300]}"]
    if "deadline" not in text or "1 seconds" not in text:
        problems.append(f"the message does not name the deadline: {text[:300]}")
    if list((folder / "out").glob("drift_*.json")):
        problems.append("a check that hit its deadline left a record")
    return problems


def evidently_missing(folder: Path) -> Problems:
    code, text, out = run_check(folder, Fixture(shifted=FEATURES))
    problems = [] if code == 3 else [f"expected exit 3, got {code}: {text[:300]}"]
    if "dependency_missing" not in text or "evidently" not in text.lower():
        problems.append(f"the message does not name the missing dependency: {text[:300]}")
    if list(out.glob("drift_*.json")):
        problems.append("a check without Evidently left a record")
    return problems


def checked(fixture: Fixture, expect_triggered: bool, expect_drifted: int | None = None, excluded: tuple[str, ...] = ()) -> Callable[[Path], Problems]:
    """Run the check on the fixture: the record must be checked, cover the compared features (the list minus ``excluded``) and name the excluded ones."""

    def case(folder: Path) -> Problems:
        code, text, out = run_check(folder, fixture, evidently=need_evidently())
        if code != 0:
            return [f"exit {code}: {text[:400]}"]
        record = read_record(out)
        problems = schema_problems(record)
        if record["schema_version"] != 2:
            problems.append(f"the record is schema version {record['schema_version']}, the writer's is 2")
        if record["status"] != "checked":
            problems.append(f"status {record['status']}")
        if record["triggered"] is not expect_triggered:
            problems.append(f"triggered is {record['triggered']}, expected {expect_triggered}, share {record['drift_share']}")
        if expect_drifted is not None and record["drifted_count"] != expect_drifted:
            problems.append(f"{record['drifted_count']} features drifted, expected {expect_drifted}")
        compared = [name for name in fixture.features if name not in excluded]
        names = {item["name"] for item in record["features"]}
        if names != set(compared):
            problems.append(f"the record covers {sorted(names)}, not the compared features {sorted(compared)}")
        if record["feature_count"] != len(compared):
            problems.append(f"feature_count is {record['feature_count']}, expected the {len(compared)} compared features")
        expected_excluded = [{"name": name, "reason": "all_missing_in_reference"} for name in fixture.features if name in excluded]
        if record["excluded_features"] != expected_excluded:
            problems.append(f"excluded_features is {record['excluded_features']}, expected {expected_excluded}")
        if record["drift_share"] is not None and record["drifted_count"] != round(record["drift_share"] * record["feature_count"]):
            problems.append(f"the drift share {record['drift_share']} is not drifted_count over feature_count")
        return problems

    return case


def tests_are_the_decided_ones(folder: Path) -> Problems:
    """Above 1000 reference rows Evidently's own choice is Wasserstein or Jensen-Shannon, so the configured tests must be named explicitly."""
    code, text, out = run_check(folder, Fixture(shifted=FEATURES), evidently=need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    problems = []
    for item in read_record(out)["features"]:
        expected, threshold = ("K-S p_value", 0.05) if item["column_type"] == "numerical" else ("chi-square p_value", 0.05)
        if item["test"] != expected or item["threshold"] != threshold:
            problems.append(f"{item['name']} used {item['test']} at {item['threshold']}, expected {expected} at {threshold}")
    return problems


def rerun_is_identical(folder: Path) -> Problems:
    return rerun_with(folder, Fixture(shifted=NUMERIC[:2]))


def rerun_with_an_excluded_feature_is_identical(folder: Path) -> Problems:
    problems = rerun_with(folder, Fixture(shifted=NUMERIC[:2], null_in_reference=("f_num_d",)))
    return problems or ([] if read_record(folder / "out")["excluded_features"] else ["the rerun fixture excluded no feature"])


def rerun_with(folder: Path, fixture: Fixture) -> Problems:
    evidently = need_evidently()
    code, text, out = run_check(folder, fixture, evidently=evidently)
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    first = (out / "drift_HAI_1.json").read_bytes()
    code, text, _ = run_check(folder, fixture, evidently=evidently, out=out)
    problems = [] if code == 0 else [f"the rerun exited {code}: {text[:300]}"]
    if (out / "drift_HAI_1.json").read_bytes() != first:
        problems.append("a rerun on the same inputs changed the record")
    if sorted(path.name for path in out.iterdir()) != ["drift_HAI_1.json"]:
        problems.append(f"the output folder holds more than the record: {sorted(path.name for path in out.iterdir())}")
    return problems


def excluded_feature_is_not_a_type_check(folder: Path) -> Problems:
    """A feature with no reference value is excluded before its type is judged: an all-missing text column against a numeric one is no type mismatch."""
    code, text, out = run_check(folder, Fixture(null_in_reference=("f_cat",), cat_as_number_in_current=True), evidently=need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    record = read_record(out)
    return [] if [item["name"] for item in record["excluded_features"]] == ["f_cat"] else [f"excluded_features is {record['excluded_features']}"]


def a_feature_the_list_leaves_out_does_not_appear(folder: Path) -> Problems:
    """A column that is all missing but not on the feature list (training dropped it) is neither compared nor excluded: the record does not name it at all."""
    code, text, out = run_check(
        folder, Fixture(null_in_reference=("f_num_d",), null_in_current=("f_num_d",), features=FEATURES[:3]), evidently=need_evidently()
    )
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    record = read_record(out)
    problems = schema_problems(record)
    if "f_num_d" in json.dumps(record):
        problems.append("the record names a feature that the list does not hold")
    if record["excluded_features"] != [] or record["feature_count"] != 3:
        problems.append(f"expected no exclusion and 3 compared features, got {record['excluded_features']} and {record['feature_count']}")
    return problems


def skipped_record_excludes_nothing(folder: Path) -> Problems:
    code, text, out = run_check(folder, Fixture(current_rows=10, null_in_reference=("f_num_a",)))
    if code != 0:
        return [f"exit {code}: {text[:300]}"]
    record = read_record(out)
    return schema_problems(record) + ([] if record["excluded_features"] == [] else [f"a skipped record names excluded features {record['excluded_features']}"])


def record_names_its_inputs(folder: Path) -> Problems:
    fixture = Fixture(shifted=("f_num_a",))
    code, text, out = run_check(folder, fixture, evidently=need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:300]}"]
    record = read_record(out)
    paths = write_inputs(folder, fixture)
    problems = []
    if record["reference"]["matrix_sha256"] != hashlib.sha256(paths["reference-matrix"].read_bytes()).hexdigest():
        problems.append("the reference hash is not the hash of the reference matrix")
    if record["current"]["matrix_sha256"] != hashlib.sha256(paths["current-matrix"].read_bytes()).hexdigest():
        problems.append("the current hash is not the hash of the current matrix")
    if record["current"]["window_years"] != [2022] or record["current"]["rows"] != 1200:
        problems.append("the current window is not 2022 with 1200 rows")
    return problems


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    return [
        ("skips_when_no_window_is_newer", skipped("no_new_window", Fixture(current_year=2021, shifted=FEATURES))),
        ("skips_when_current_rows_are_too_few", skipped("too_few_current_rows", Fixture(current_rows=10, shifted=FEATURES))),
        ("skips_just_below_the_minimum_of_100_rows", skipped("too_few_current_rows", Fixture(current_rows=99, shifted=FEATURES))),
        (
            "skips_the_sensitivity_population",
            skipped("population_not_monitored", Fixture(population="sensitivity", current_population="sensitivity", shifted=FEATURES)),
        ),
        ("refuses_mixed_populations", refused("population_mismatch", Fixture(population="primary", current_population="sensitivity"))),
        ("refuses_mixed_hai_types", refused("hai_type_mismatch", Fixture(hai_type="HAI_1", current_hai_type="HAI_2"))),
        ("refuses_a_feature_missing_from_current", refused("feature_missing", Fixture(drop_from_current="f_num_b"), "f_num_b")),
        ("refuses_a_feature_missing_from_reference", refused("feature_missing", Fixture(drop_from_reference="f_cat"), "f_cat")),
        ("refuses_a_feature_of_another_type", refused("feature_type_differs", Fixture(cat_as_number_in_current=True), "f_cat")),
        ("refuses_a_feature_missing_only_in_current", refused("feature_all_missing", Fixture(null_in_current=("f_num_a",)), "f_num_a")),
        ("refuses_when_every_feature_is_missing_in_reference", refused("no_comparable_feature", Fixture(null_in_reference=FEATURES))),
        (
            "refuses_a_kept_feature_that_loses_its_values_in_current",
            refused("feature_all_missing", Fixture(null_in_current=("f_num_b",), null_in_reference=("f_num_a",)), "f_num_b"),
        ),
        ("refuses_an_empty_feature_list", refused("features_empty", Fixture(features=()))),
        ("refuses_a_garbage_reference_matrix", refused("matrix_unreadable", Fixture(corrupt_reference="garbage"), "reference")),
        ("refuses_a_truncated_reference_matrix", refused("matrix_unreadable", Fixture(corrupt_reference="truncated"), "reference")),
        ("refuses_a_garbage_current_matrix", refused("matrix_unreadable", Fixture(corrupt_current="garbage"), "current")),
        ("refuses_a_current_matrix_with_a_damaged_footer", refused("matrix_unreadable", Fixture(corrupt_current="footer"), "current")),
        ("ends_at_the_deadline_on_a_stalled_input", stalled_input_hits_the_deadline),
        ("names_the_missing_dependency", evidently_missing),
        ("triggers_when_every_feature_drifts", checked(Fixture(shifted=FEATURES), True, len(FEATURES))),
        ("stays_quiet_without_drift", checked(Fixture(), False, 0)),
        ("triggers_at_exactly_half", checked(Fixture(shifted=NUMERIC[:2], features=NUMERIC), True, 2)),
        ("checks_at_exactly_the_minimum_of_100_rows", checked(Fixture(current_rows=100, shifted=FEATURES), True, len(FEATURES))),
        ("stays_quiet_below_half", checked(Fixture(shifted=NUMERIC[:1], features=NUMERIC), False, 1)),
        (
            "excludes_a_feature_missing_in_every_reference_row",
            checked(Fixture(null_in_reference=("f_num_a",)), False, 0, excluded=("f_num_a",)),
        ),
        (
            "excludes_a_feature_missing_in_the_whole_baseline",
            checked(Fixture(null_in_reference=("f_num_a",), null_in_current=("f_num_a",)), False, 0, excluded=("f_num_a",)),
        ),
        (
            "excludes_several_features_in_list_order",
            checked(Fixture(null_in_reference=("f_num_c", "f_num_a")), False, 0, excluded=("f_num_a", "f_num_c")),
        ),
        (
            "judges_the_share_on_the_compared_features",
            checked(Fixture(shifted=("f_num_a",), features=NUMERIC[:3], null_in_reference=("f_num_c",)), True, 1, excluded=("f_num_c",)),
        ),
        ("compares_a_mostly_missing_feature", checked(Fixture(mostly_null_in_reference="f_num_a"), False, None)),
        ("excluded_feature_is_not_a_type_check", excluded_feature_is_not_a_type_check),
        ("a_feature_the_list_leaves_out_does_not_appear", a_feature_the_list_leaves_out_does_not_appear),
        ("skipped_record_excludes_nothing", skipped_record_excludes_nothing),
        ("uses_the_decided_tests_above_1000_rows", tests_are_the_decided_ones),
        ("rerun_changes_nothing", rerun_is_identical),
        ("rerun_with_an_excluded_feature_changes_nothing", rerun_with_an_excluded_feature_is_identical),
        ("record_names_its_inputs", record_names_its_inputs),
    ]
