"""Run the static training commands on synthetic metadata and retain a report under data/e2e/training."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from importlib.metadata import version
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
SLOTS = ("m1_ols", "m2_elastic_net", "m3_mixed", "m4_boosting", "m5_spatial", "m6_fixed_effects", "rq4_reduced")
CMS = ["feature_matrix_care_compare:c119__value__number", "feature_matrix_hospital_window:bed_count"]
ALL = sorted([*CMS, "feature_matrix_hospital:c001__value__number", "feature_matrix_hospital_window:cmi"])
TRAINING_PINS = {
    "mlflow": "3.17.0",
    "scikit-learn": "1.9.1",
    "xgboost": "3.4.1",
    "shap": "0.52.0",
    "statsmodels": "0.15.0",
    "linearmodels": "7.0",
    "libpysal": "4.15.0",
    "esda": "2.10.0",
    "spreg": "1.9.1",
    "dvc": "3.67.1",
    "dvc-s3": "3.3.0",
}
SERVER_PINS = {"mlflow": "3.17.0", "boto3": "1.0.0", "psycopg2-binary": "2.0.0"}
API_PINS = {
    "mlflow": "3.17.0",
    "scikit-learn": "1.9.1",
    "xgboost": "3.4.1",
    "fastapi": "0.142.4",
    "uvicorn": "0.54.0",
    "feast": "0.66.0",
    "redis": "8.1.0",
    "prometheus-client": "0.26.0",
}
COMMIT = "a" * 40
FAILURE_MODES = {"E01": ["I1", "M10", "T4"], "E09": ["G1", "G2"], "E10": ["G5"], "E11": ["G6", "G7", "D6", "R4"], "E40": ["G6", "G2"]}


def _write(path: Path, document: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _edit(path: Path, change: Callable[[Any], None]) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    change(document)
    _write(path, document)


def _set_references(document: dict[str, Any], references: list[str]) -> None:
    document["references"] = references


def _write_plan(folder: Path, content: bytes) -> None:
    (folder / "docs/analysis_plan.md").write_bytes(content)


def _edit_config(folder: Path, filename: str, change: Callable[[Any], None]) -> None:
    _edit(folder / "config/training" / filename, change)


def _column(name: str, view: str, role: str = "predictor", control: str | None = None, source: str | None = None) -> dict[str, Any]:
    match: dict[str, str] = {}
    if control:
        match["measure_control"] = control
    if source:
        match["measure_source"] = source
    return {"name": name, "view": view, "role": role, "pivot": {"match": match}}


def _fixture(folder: Path) -> None:
    shutil.copytree(ROOT / "config/training", folder / "config/training")
    shutil.copytree(ROOT / "data_contracts/training", folder / "data_contracts/training")
    seed = folder / "dbt/seeds/gold"
    seed.mkdir(parents=True)
    for path in sorted((ROOT / "dbt/seeds/gold").glob("*_bridge.csv")):
        shutil.copyfile(path, seed / path.name)
    plan = folder / "docs/analysis_plan.md"
    plan.parent.mkdir()
    plan.write_bytes(b"# Synthetic analysis plan\n\nThis is a synthetic metadata fixture.\n")
    tables: dict[str, Any] = {
        "feature_matrix_care_compare": {
            "family": "care_compare",
            "role": "predictor",
            "columns": [_column("c119__value__number", "feature_matrix_care_compare", control="C119")],
        },
        "feature_matrix_hospital": {
            "family": "hospital",
            "role": "predictor",
            "columns": [_column("c001__value__number", "feature_matrix_hospital", control="C001", source="cost_report")],
        },
        "feature_matrix_hospital_window": {
            "family": "hospital_window",
            "role": "predictor",
            "columns": [_column(name, "feature_matrix_hospital_window") for name in ("bed_count", "cmi")],
        },
        "label_matrix_hai_outcomes": {"family": "hai_outcomes", "role": "label", "columns": [], "label_views": {}},
    }
    labels = tables["label_matrix_hai_outcomes"]
    for number in range(1, 7):
        name = f"hai_{number}__sir__number"
        column = _column(name, f"label_hai_{number}", role="label")
        column["pivot"] = {"kind": "number", "match": {"hai_type": f"HAI_{number}"}, "value_column": "sir"}
        labels["columns"].append(column)
        labels["label_views"][f"label_hai_{number}"] = [name]
    _write(folder / "data_contracts/features/feature_schema.json", {"schema": 1, "tables": tables, "servable": ALL})
    for slot in SLOTS:
        path = folder / f"config/training/feature_lists/{slot}.json"
        _edit(path, partial(_set_references, references=sorted(CMS if slot in ("m1_ols", "rq4_reduced") else ALL)))


def _lock_fixture(folder: Path) -> None:
    inventory: dict[str, Any] = {
        "schema": 1,
        "locked_at_utc": "2026-10-23T00:00:00Z",
        "release_history": {
            name: [{"version": pin, "uploaded_at_utc": "2026-10-01T00:00:00Z", "yanked": False}] for name, pin in SERVER_PINS.items() if name != "mlflow"
        },
        "images": {},
    }
    for image, direct in (("training", TRAINING_PINS), ("mlflow_server", SERVER_PINS), ("prediction_api", API_PINS)):
        pins = {**direct, "mlflow-skinny": "3.17.0"}
        source, lock = folder / image / "requirements.in", folder / image / "requirements.txt"
        source.parent.mkdir(parents=True)
        source.write_text("".join(f"{name}=={pin}\n" for name, pin in sorted(direct.items())), encoding="utf-8")
        lines = ["--only-binary=:all:\n"]
        wheels = []
        for name, pin in sorted(pins.items()):
            digest = hashlib.sha256(f"synthetic|{image}|{name}|{pin}".encode()).hexdigest()
            filename = f"{name.replace('-', '_')}-{pin}-py3-none-any.whl"
            wheels.append({"filename": filename, "sha256": digest})
            lines.append(f"{name}=={pin} --hash=sha256:{digest}\n")
        lock.write_text("".join(lines), encoding="utf-8")
        inventory["images"][image] = wheels
    _write(folder / "wheels.json", inventory)


def _source_digest() -> str:
    paths = [path for root in ("scripts/training", "config/training", "data_contracts/training") for path in (ROOT / root).rglob("*")]
    paths.extend(
        ROOT / path
        for path in (
            "scripts/process.py",
            "pyproject.toml",
            "docs/analysis_plan.md",
            "data_contracts/features/feature_schema.json",
        )
    )
    paths.extend((ROOT / "dbt/seeds/gold").glob("*_bridge.csv"))
    digest = hashlib.sha256()
    for path in sorted(path for path in paths if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"):
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0" + path.read_bytes())
    return digest.hexdigest()


class Report:
    def __init__(self) -> None:
        self.steps: list[dict[str, Any]] = []
        self.pending: list[dict[str, Any]] = []

    def command(self, name: str, design_case: str, module: str, arguments: list[str], expected: str | None = None) -> dict[str, Any] | None:
        started = time.monotonic()
        command = ["-m", module, "--check", *arguments]
        try:
            result = run_command(sys.executable, command, cwd=ROOT, timeout=30)
            ok = (result.returncode == 0) if expected is None else (result.returncode == 1 and expected in result.stderr and "Traceback" not in result.stderr)
            record = {
                "name": name,
                "design_case": design_case,
                "failure_modes": FAILURE_MODES[design_case],
                "verdict": "pass" if ok else "fail",
                "exit_code": result.returncode,
                "expected": expected or "exit 0",
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
                "seconds": time.monotonic() - started,
            }
            self.steps.append(record)
            if ok and expected is None:
                return json.loads(result.stdout)
        except (OSError, TimeoutExpired, json.JSONDecodeError) as error:
            self.steps.append({"name": name, "design_case": design_case, "verdict": "fail", "error": type(error).__name__})
        return None

    def assertion(self, name: str, ok: bool, **evidence: Any) -> None:
        self.steps.append({"name": name, "verdict": "pass" if ok else "fail", **evidence})


def _guard_cases(report: Report, scratch: Path) -> None:
    baseline = scratch / "guard_baseline"
    _fixture(baseline)

    def run(name: str, change: Callable[[Path], None] | None = None, expected: str | None = None, design_case: str = "E40") -> dict[str, Any] | None:
        folder = scratch / name
        shutil.copytree(baseline, folder)
        if change:
            change(folder)
        return report.command(name, design_case, "scripts.training.guards", ["--root", str(folder), "--git-commit", COMMIT], expected)

    first = run("valid_metadata")
    second = run("same_metadata")
    report.assertion("equal_inputs_give_equal_guard_records", first is not None and first == second)
    if first:
        report.assertion("snapshot_commit_is_recorded", first.get("git_commit") == COMMIT)
        plan_hash = hashlib.sha256((baseline / "docs/analysis_plan.md").read_bytes()).hexdigest()
        report.assertion("exact_plan_bytes_are_recorded", first.get("analysis_plan_sha256") == plan_hash)
        expected_hashes = {
            name: hashlib.sha256((baseline / "config/training" / f"{name}.json").read_bytes()).hexdigest()
            for name in ["models", "promotion", "census_regions", "excluded_controls", "targets", *(f"feature_lists/{slot}" for slot in SLOTS)]
        }
        for name in ("bundle", "model_contract"):
            expected_hashes[name] = hashlib.sha256((baseline / f"data_contracts/training/{name}.json").read_bytes()).hexdigest()
        report.assertion("all_static_input_hashes_are_recorded", first.get("file_sha256") == expected_hashes)
        feature_hash = hashlib.sha256((baseline / "data_contracts/features/feature_schema.json").read_bytes()).hexdigest()
        report.assertion("feature_schema_hash_is_recorded", first.get("feature_schema_sha256") == feature_hash)
        report.assertion("targets_hash_is_recorded", first.get("targets_sha256") == expected_hashes["targets"])
    changed_seed = run("changed_master_seed", partial(_edit_config, filename="models.json", change=lambda d: d.update(master_seed=43)))
    report.assertion(
        "configuration_change_changes_digest",
        first is not None
        and changed_seed is not None
        and first["training_config_sha256"] != changed_seed["training_config_sha256"]
        and changed_seed["master_seed"] == 43,
    )
    for name, content in (
        ("edited_plan", b"# Changed synthetic plan\n"),
        ("bom_plan", b"\xef\xbb\xbf# Synthetic plan\n"),
        ("crlf_plan", b"# Synthetic plan\r\n"),
    ):
        result = run(name, partial(_write_plan, content=content), design_case="E09")
        report.assertion(name + "_hash", result is not None and result.get("analysis_plan_sha256") == hashlib.sha256(content).hexdigest())
    run("missing_plan", lambda folder: (folder / "docs/analysis_plan.md").unlink(), "plan: missing", "E09")
    run("empty_plan", partial(_write_plan, content=b""), "plan: empty", "E09")

    def directory_plan(folder: Path) -> None:
        (folder / "docs/analysis_plan.md").unlink()
        (folder / "docs/analysis_plan.md").mkdir()

    def linked_plan(folder: Path) -> None:
        (folder / "docs/analysis_plan.md").unlink()
        (folder / "docs/analysis_plan.md").symlink_to(baseline / "docs/analysis_plan.md")

    run("directory_plan", directory_plan, "plan: regular", "E09")
    run("linked_plan", linked_plan, "plan: symbolic link", "E09")

    def linked_docs(folder: Path) -> None:
        (folder / "docs").rename(folder / "real_docs")
        (folder / "docs").symlink_to(folder / "real_docs", target_is_directory=True)

    run("linked_plan_parent", linked_docs, "plan: symbolic link", "E09")
    mutations: list[tuple[str, str, Callable[[Any], None], str]] = [
        ("unknown_configuration_key", "models.json", lambda d: d.update(unreviewed=True), "models.json"),
        ("negative_seed", "models.json", lambda d: d.update(master_seed=-1), "master_seed"),
        ("wrong_seed_type", "models.json", lambda d: d.update(master_seed=True), "master_seed"),
        ("missing_model_slot", "models.json", lambda d: d["slots"].pop("m3_mixed"), "slots"),
        ("widened_missingness", "models.json", lambda d: d["thresholds"].update(max_missing_fraction=0.51), "max_missing_fraction"),
        ("unshuffled_folds", "models.json", lambda d: d["cross_validation"].update(shuffle=False), "shuffle"),
        ("wrong_fold_group", "models.json", lambda d: d["cross_validation"].update(groups="county"), "groups"),
        ("analytic_moran_gate", "models.json", lambda d: d["moran"].update(p_value="p_norm"), "p_value"),
        ("unseeded_moran", "models.json", lambda d: d["moran"].update(global_seed_immediately_before_call=False), "global_seed"),
        ("widened_significance", "models.json", lambda d: d["thresholds"].update(significance_level=0.1), "significance_level"),
        ("removed_forced_cmi", "models.json", lambda d: d["slots"]["m2_elastic_net"].update(forced_references=[]), "forced_references"),
        ("registrable_mixed_model", "models.json", lambda d: d["slots"]["m3_mixed"].update(registrable=True), "registrable"),
        ("registrable_forest_check", "models.json", lambda d: d["slots"]["m4_boosting"]["check"].update(registrable=True), "registrable"),
        ("different_bootstrap_method", "models.json", lambda d: d["bootstrap"]["mediation"].update(interval_method="basic"), "interval_method"),
        ("unpaired_bootstrap", "models.json", lambda d: d["bootstrap"]["nested_comparison"].update(paired=False), "paired"),
        ("zero_budget", "models.json", lambda d: d["slots"]["m1_ols"].update(budget_seconds=0), "budget_seconds"),
        ("empty_tuning_grid", "models.json", lambda d: d["slots"]["m2_elastic_net"]["grid"].update(alpha=[]), "alpha"),
        ("lower_promotion_gain", "promotion.json", lambda d: d.update(minimum_rmse_improvement=0.01), "minimum_rmse_improvement"),
        ("higher_group_tolerance", "promotion.json", lambda d: d.update(maximum_group_rmse_increase=0.06), "maximum_group_rmse_increase"),
        ("lower_group_minimum", "promotion.json", lambda d: d.update(minimum_group_size=99), "minimum_group_size"),
        ("changed_bed_bands", "promotion.json", lambda d: d["groups"]["bed_size_band"].update(cut_points=[100, 299]), "cut_points"),
        ("missing_group_dimension", "promotion.json", lambda d: d["groups"].pop("ownership_type"), "groups"),
        ("changed_bootstrap_size", "promotion.json", lambda d: d["bootstrap"].update(draws=4999), "draws"),
        ("zero_touching_interval", "promotion.json", lambda d: d.update(interval_must_exclude_zero=False), "interval_must_exclude_zero"),
        ("exclusive_improvement_boundary", "promotion.json", lambda d: d.update(improvement_boundary_inclusive=False), "improvement_boundary"),
        ("exclusive_group_boundary", "promotion.json", lambda d: d.update(group_boundary_inclusive=False), "group_boundary"),
        ("different_group_column", "promotion.json", lambda d: d["groups"]["ownership_type"].update(column="state_code"), "ownership_type"),
        ("wrong_null_group_rule", "promotion.json", lambda d: d.update(null_group_values="zero"), "null_group_values"),
        ("missing_dc", "census_regions.json", lambda d: d["states"].pop("DC"), "states"),
        ("extra_territory", "census_regions.json", lambda d: d["states"].update(PR="South"), "states"),
        ("wrong_census_region", "census_regions.json", lambda d: d["states"].update(NY="West"), "NY"),
        ("removed_exclusion", "excluded_controls.json", lambda d: d["controls"].remove("E038"), "exclusions: bridge mismatch"),
        ("stale_exclusion", "excluded_controls.json", lambda d: d["controls"].append("C119"), "exclusions: bridge mismatch"),
    ]
    for name, filename, change, cause in mutations:
        run(name, partial(_edit_config, filename=filename, change=change), cause)

    def missing_validation_bridge(folder: Path) -> None:
        (folder / "dbt/seeds/gold/validation_measures_bridge.csv").rename(folder / "removed_validation_bridge.csv")

    run("missing_validation_bridge", missing_validation_bridge, "exclusions: missing control bridge")

    def missing_bridge_decision_column(folder: Path) -> None:
        path = folder / "dbt/seeds/gold/registry_measure_sources_bridge.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields, rows = [field for field in reader.fieldnames or [] if field != "review_decision"], list(reader)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    run("missing_bridge_decision_column", missing_bridge_decision_column, "missing control-bridge columns")

    def new_gold_bridge(folder: Path) -> None:
        path = folder / "dbt/seeds/gold/review_fixture_bridge.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["fact", "measure_key", "value_column", "measure_control", "review_decision"])
            writer.writeheader()
            for control, decision in (("C900", "exclude_primary"), ("C901", "comparison_only"), ("C902", "include_primary")):
                writer.writerow(dict(fact="synthetic", measure_key=control, value_column="value", measure_control=control, review_decision=decision))
        _edit_config(folder, "excluded_controls.json", lambda d: d["controls"].extend(["C900", "C901"]))

    run("new_gold_bridge_exclusions", new_gold_bridge)

    def validation_row_without_exclusion_decision(folder: Path) -> None:
        path = folder / "dbt/seeds/gold/validation_measures_bridge.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields, rows = reader.fieldnames, list(reader)
        for row in rows:
            if row["measure_control"] == "C285":
                row["review_decision"] = "include_primary"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields or [])
            writer.writeheader()
            writer.writerows(rows)

    run("all_validation_bridge_rows_excluded", validation_row_without_exclusion_decision)

    def malformed_config(folder: Path) -> None:
        (folder / "config/training/models.json").write_text("{", encoding="utf-8")

    run("malformed_json", malformed_config, "models.json: invalid JSON")
    run("missing_configuration", lambda folder: (folder / "config/training/models.json").unlink(), "models.json: cannot read")

    def refs(folder: Path, reference: str) -> None:
        _edit(folder / "config/training/feature_lists/m2_elastic_net.json", lambda d: d["references"].append(reference))

    run("unknown_reference", lambda f: refs(f, "unknown_view:unknown_column"), "reference: not registered", "E11")
    run("label_reference", lambda f: refs(f, "label_hai_1:hai_1__sir__number"), "reference: label", "E11")
    run("bare_reference", lambda f: refs(f, "c119__value__number"), "references", "E11")
    run("duplicate_reference", lambda f: refs(f, CMS[0]), "references", "E11")
    run(
        "curated_cms_subset",
        partial(_edit_config, filename="feature_lists/m1_ols.json", change=lambda d: d["references"].remove("feature_matrix_hospital_window:bed_count")),
    )
    run(
        "cost_report_in_cms_baseline",
        partial(_edit_config, filename="feature_lists/m1_ols.json", change=lambda d: d["references"].append("feature_matrix_hospital:c001__value__number")),
        "cms_only scope",
    )
    run(
        "omitted_eligible_reference",
        lambda f: _edit_config(f, "feature_lists/m2_elastic_net.json", lambda d: d["references"].remove(ALL[0])),
        "feature list differs",
        "E11",
    )
    run(
        "wrong_slot_in_feature_list",
        lambda f: _edit_config(f, "feature_lists/m1_ols.json", lambda d: d.update(slot="rq4_reduced")),
        "slot does not match",
        "E40",
    )

    def schema_edit(folder: Path, change: Callable[[Any], None]) -> None:
        _edit(folder / "data_contracts/features/feature_schema.json", change)

    run(
        "label_role_in_predictor_view",
        partial(schema_edit, change=lambda d: d["tables"]["feature_matrix_hospital"]["columns"][0].update(role="label")),
        "reference: label",
        "E11",
    )
    run(
        "label_table_with_predictor_column",
        partial(schema_edit, change=lambda d: d["tables"]["feature_matrix_hospital"].update(role="label")),
        "reference: label",
        "E11",
    )
    run(
        "nonpredictor_table",
        partial(schema_edit, change=lambda d: d["tables"]["feature_matrix_hospital"].update(role="identifier")),
        "reference: not a predictor",
        "E11",
    )

    def label_named_predictor(folder: Path) -> None:
        column = _column("x", "label_hai_1", control="C119")
        schema_edit(folder, lambda d: d["tables"]["feature_matrix_care_compare"]["columns"].append(column))
        schema_edit(folder, lambda d: d["servable"].append("label_hai_1:x"))
        for slot in SLOTS:
            _edit_config(folder, f"feature_lists/{slot}.json", lambda d: d["references"].append("label_hai_1:x"))

    run("label_named_predictor", label_named_predictor, "reference: label", "E11")
    run(
        "wrong_label_field",
        partial(schema_edit, change=lambda d: d["tables"]["label_matrix_hai_outcomes"]["columns"][0]["pivot"].update(value_column="numerator")),
        "target label: not registered",
        "E10",
    )
    run(
        "wrong_label_type",
        partial(schema_edit, change=lambda d: d["tables"]["label_matrix_hai_outcomes"]["columns"][0]["pivot"]["match"].update(hai_type="HAI_2")),
        "target label: not registered",
        "E10",
    )

    def missing_cmi(folder: Path) -> None:
        schema_edit(folder, lambda d: d["tables"]["feature_matrix_hospital_window"]["columns"].pop())
        schema_edit(folder, lambda d: d["servable"].remove("feature_matrix_hospital_window:cmi"))
        for slot in SLOTS:
            if slot not in ("m1_ols", "rq4_reduced"):
                _edit_config(folder, f"feature_lists/{slot}.json", lambda d: d["references"].remove("feature_matrix_hospital_window:cmi"))

    run("forced_cmi_absent_from_schema", missing_cmi, "forced covariate", "E40")

    def poison_control(folder: Path, control: str) -> None:
        _edit(
            folder / "data_contracts/features/feature_schema.json",
            lambda d: d["tables"]["feature_matrix_hospital"]["columns"][0]["pivot"]["match"].update(measure_control=control),
        )

    for name, control in (
        ("target", "C269"),
        ("earlier_outcome", "C275"),
        ("outcome_part", "C281"),
        ("overall_star_rating", "C284"),
        ("hospital_harm_c171", "C171"),
        ("hospital_harm_c172", "C172"),
        ("hospital_harm_c173", "C173"),
        ("hospital_harm_c174", "C174"),
        ("hac", "C285"),
        ("vbp", "C288"),
        ("psi90", "E038"),
        ("psi13", "E039"),
        ("readmissions", "C289.01"),
        ("complications", "C291"),
        ("exclusion_child", "C291.99"),
    ):
        run("forbidden_" + name, partial(poison_control, control=control), "reference: excluded control", "E11")

    def excluded_field(folder: Path) -> None:
        schema_edit(folder, lambda d: d.update(excluded_controls=["C001.field"]))
        poison_control(folder, "C001.field")

    run("exact_excluded_field", excluded_field, "reference: excluded control", "E11")
    run(
        "identifier_as_predictor",
        lambda f: _edit(
            f / "data_contracts/features/feature_schema.json", lambda d: d["tables"]["feature_matrix_hospital"]["columns"][0].update(role="identifier")
        ),
        "reference: not a predictor",
        "E11",
    )
    run(
        "unregistered_predictor",
        lambda f: _edit(f / "data_contracts/features/feature_schema.json", lambda d: d["servable"].remove(ALL[0])),
        "reference: not registered",
        "E11",
    )
    run(
        "missing_label",
        lambda f: _edit(
            f / "data_contracts/features/feature_schema.json", lambda d: d["tables"]["label_matrix_hai_outcomes"]["label_views"].pop("label_hai_6")
        ),
        "target label: not registered",
        "E10",
    )

    def changed_target_bridge(folder: Path) -> None:
        path = folder / "dbt/seeds/gold/hai_outcome_measures_bridge.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields, rows = reader.fieldnames, list(reader)
        for row in rows:
            if row["measure_control"] == "C269":
                row["measure_key"] = "HAI_2_SIR"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields or [])
            writer.writeheader()
            writer.writerows(rows)

    run("changed_target_bridge", changed_target_bridge, "targets:", "E10")
    for name in ("bundle", "model_contract"):

        def changed_contract(folder: Path, name: str) -> None:
            _edit(folder / f"data_contracts/training/{name}.json", lambda d: d.update(schema=2))

        run(f"changed_{name}_contract", partial(changed_contract, name=name), f"{name}.json")
    output = baseline / "guard.json"
    result = report.command(
        "persist_guard_record",
        "E09",
        "scripts.training.guards",
        ["--root", str(baseline), "--git-commit", COMMIT, "--output", str(output)],
    )
    report.assertion("saved_guard_equals_command_record", result is not None and output.exists() and json.loads(output.read_text()) == result)
    report.command("invalid_snapshot_commit", "E09", "scripts.training.guards", ["--root", str(baseline), "--git-commit", "invalid"], "git_commit:")
    trial = Report()
    trial.command("wrong_rejection_cause", "E09", "scripts.training.guards", ["--root", str(baseline), "--git-commit", "invalid"], "plan: missing")
    report.assertion(
        "wrong_rejection_reason_never_passes",
        len(trial.steps) == 1 and trial.steps[0]["verdict"] == "fail" and trial.steps[0].get("exit_code") == 1,
    )


def _lock_cases(report: Report, scratch: Path) -> None:
    baseline = scratch / "lock_baseline"
    _lock_fixture(baseline)

    def run(name: str, change: Callable[[Path], None] | None = None, expected: str | None = None) -> dict[str, Any] | None:
        folder = scratch / name
        shutil.copytree(baseline, folder)
        if change:
            change(folder)
        arguments = []
        for image in ("training", "mlflow_server", "prediction_api"):
            arguments.extend(["--image", image, str(folder / image / "requirements.in"), str(folder / image / "requirements.txt")])
        return report.command(name, "E01", "scripts.training.locks", [*arguments, "--wheel-inventory", str(folder / "wheels.json")], expected)

    def replace(folder: Path, image: str, filename: str, before: str, after: str) -> None:
        path = folder / image / filename
        path.write_text(path.read_text(encoding="utf-8").replace(before, after), encoding="utf-8")

    first = run("approved_synthetic_locks")
    second = run("repeated_synthetic_locks")
    report.assertion("equal_lock_inputs_give_equal_records", first is not None and first == second)
    run("unapproved_direct", lambda f: replace(f, "training", "requirements.in", "mlflow==3.17.0", "unapproved==1.0"), "is not on the approved version list")
    run("recent_direct_pin", lambda f: replace(f, "training", "requirements.in", "shap==0.52.0", "shap==0.54.1"), "is not on the approved version list")

    def unapproved_matched_pin(folder: Path) -> None:
        replace(folder, "training", "requirements.in", "shap==0.52.0", "shap==0.52.1")
        replace(folder, "training", "requirements.txt", "shap==0.52.0", "shap==0.52.1")

        def change_wheel(document: dict[str, Any]) -> None:
            for wheel in document["images"]["training"]:
                if wheel["filename"].startswith("shap-0.52.0-"):
                    wheel["filename"] = wheel["filename"].replace("0.52.0", "0.52.1")

        _edit(folder / "wheels.json", change_wheel)

    run("unapproved_matched_pin", unapproved_matched_pin, "is not on the approved version list")
    run("unpinned_direct", lambda f: replace(f, "training", "requirements.in", "mlflow==3.17.0", "mlflow>=3.17.0"), "exact pin")
    run("missing_direct", lambda f: replace(f, "training", "requirements.in", "shap==0.52.0\n", ""), "required packages")
    run("unhashed_lock", lambda f: replace(f, "training", "requirements.txt", "--hash=sha256:", "--hash=md5:"), "sha256")
    run("lock_without_binary_policy", lambda f: replace(f, "training", "requirements.txt", "--only-binary=:all:\n", ""), "only-binary")
    run(
        "duplicate_lock_package",
        lambda f: replace(f, "training", "requirements.txt", "--only-binary=:all:", "--only-binary=:all:\nmlflow==3.17.0 --hash=sha256:" + "a" * 64),
        "duplicate package",
    )
    run("unlocked_direct", lambda f: replace(f, "training", "requirements.txt", "xgboost==3.4.1", "xgboost==3.4.0"), "lock pin")
    for image, package in (
        ("mlflow_server", "mlflow"),
        ("mlflow_server", "mlflow-skinny"),
        ("prediction_api", "mlflow"),
        ("prediction_api", "scikit-learn"),
        ("prediction_api", "xgboost"),
    ):
        pin = "3.17.0" if package.startswith("mlflow") else TRAINING_PINS[package]
        run(
            f"mismatched_{image}_{package}",
            partial(replace, image=image, filename="requirements.txt", before=f"{package}=={pin}", after=f"{package}==0.0.1"),
            "version parity",
        )

    def artifact_filename(folder: Path, filename: str) -> None:
        _edit(folder / "wheels.json", lambda d: d["images"]["training"][5].update(filename=filename))

    for filename, cause in (
        ("mlflow-3.17.0-cp312-cp312-manylinux_2_17_x86_64.whl", "wheel platform"),
        ("mlflow-3.17.0-cp312-cp312-macosx_11_0_arm64.whl", "wheel platform"),
        ("mlflow-3.17.0-cp311-cp311-manylinux_2_17_aarch64.whl", "wheel Python"),
        ("mlflow-3.17.0.tar.gz", "wheel filename"),
    ):
        run(
            "wrong_artifact_" + filename,
            partial(artifact_filename, filename=filename),
            cause,
        )
    run("missing_wheel_evidence", lambda f: _edit(f / "wheels.json", lambda d: d["images"].pop("training")), "wheel inventory")
    run("missing_package_wheel", lambda f: _edit(f / "wheels.json", lambda d: d["images"]["training"].pop()), "wheel inventory")
    run("unbound_wheel_hash", lambda f: _edit(f / "wheels.json", lambda d: d["images"]["training"][0].update(sha256="0" * 64)), "wheel hash")
    run("native_arm64_wheel", partial(artifact_filename, filename="mlflow-3.17.0-cp312-cp312-manylinux_2_17_aarch64.whl"))
    run("older_stable_abi_wheel", partial(artifact_filename, filename="mlflow-3.17.0-cp39-abi3-manylinux_2_17_aarch64.whl"))
    run("wrong_wheel_version", partial(artifact_filename, filename="mlflow-0.0.1-py3-none-any.whl"), "wheel filename")

    def multiline_lock(folder: Path) -> None:
        replace(folder, "training", "requirements.txt", " --hash=", " " + chr(92) + "\n    --hash=")

    run("continued_lock_lines", multiline_lock)
    run("empty_lock", lambda f: replace(f, "training", "requirements.txt", (f / "training/requirements.txt").read_text(), ""), "no pinned packages")
    run("duplicate_direct", lambda f: replace(f, "training", "requirements.in", "mlflow==3.17.0", "mlflow==3.17.0\nmlflow==3.17.0"), "duplicate package")

    def recent_transitive(folder: Path) -> None:
        path = folder / "mlflow_server/requirements.txt"
        digest = "e" * 64
        path.write_text(path.read_text() + f"shap==0.54.1 --hash=sha256:{digest}\n")
        _edit(folder / "wheels.json", lambda d: d["images"]["mlflow_server"].append({"filename": "shap-0.54.1-py3-none-any.whl", "sha256": digest}))

    run("recent_transitive_pin", recent_transitive, "recent version")
    run(
        "exactly_fourteen_day_server_release",
        lambda f: _edit(f / "wheels.json", lambda d: d["release_history"]["boto3"][0].update(uploaded_at_utc="2026-10-09T00:00:00Z")),
    )
    run(
        "yanked_newer_server_release",
        lambda f: _edit(
            f / "wheels.json",
            lambda d: d["release_history"]["boto3"].append({"version": "1.1.0", "uploaded_at_utc": "2026-10-01T00:00:00Z", "yanked": True}),
        ),
    )
    run(
        "too_recent_server_release",
        lambda f: _edit(f / "wheels.json", lambda d: d["release_history"]["boto3"][0].update(uploaded_at_utc="2026-10-22T00:00:00Z")),
        "mature release",
    )
    run(
        "older_server_release_selected",
        lambda f: _edit(
            f / "wheels.json",
            lambda d: d["release_history"]["boto3"].append({"version": "1.0.1", "uploaded_at_utc": "2026-10-01T00:00:00Z", "yanked": False}),
        ),
        "newest mature",
    )
    run(
        "missing_server_release_history",
        lambda f: _edit(f / "wheels.json", lambda d: d["release_history"].pop("psycopg2-binary")),
        "release history",
    )


def _committed_feature_lists(report: Report, commit: str) -> None:
    if (ROOT / "data_contracts/features/feature_schema.json").is_file():
        report.command("committed_feature_lists", "E11", "scripts.training.guards", ["--root", str(ROOT), "--git-commit", commit])
    else:
        report.pending.append(
            {"name": "committed_feature_lists", "design_case": "E11", "verdict": "pending", "reason": "data_contracts/features/feature_schema.json is absent."}
        )


def main() -> int:
    """Run the implemented static subset. Pending lanes are reported separately from passes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("static",), required=True)
    args = parser.parse_args()
    started = datetime.now(UTC)
    before = _source_digest()
    report = Report()
    revision = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=10)
    try:
        with tempfile.TemporaryDirectory(prefix="training_static_") as scratch:
            _guard_cases(report, Path(scratch))
            _committed_feature_lists(report, revision.stdout.strip())
            _lock_cases(report, Path(scratch))
    except (OSError, ValueError, KeyError) as error:
        report.steps.append({"name": "fixture_setup", "verdict": "fail", "error": type(error).__name__, "detail": str(error)})
    report.assertion("source_unchanged_during_run", _source_digest() == before)
    record = {
        "feature": "training_static",
        "mode": args.mode,
        "started_at_utc": started.isoformat(),
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "code_revision": revision.stdout.strip(),
        "source_sha256": before,
        "environment": {"python": sys.version.split()[0], "jsonschema": version("jsonschema")},
        "command": f"{sys.executable} -m scripts.training.run_training_e2e --mode static",
        "fixtures": "Synthetic feature and lock metadata, all public gold bridges, and committed feature lists when the repository feature schema exists.",
        "steps": report.steps,
        "pending": [
            *report.pending,
            {"design_case": "E02", "reason": "Task definitions belong to the orchestration lane."},
            {"design_case": "E07", "reason": "Registry writer code is not part of the static guard implementation."},
            {"design_case": "E22", "reason": "Promotion calculations follow in the next implementation part. This part checks the policy configuration only."},
        ],
        "limits": ["No study data reads, image imports, package resolution, Docker, launcher, model fits, registry calls or cloud access."],
        "counts": {
            "passed": len([step for step in report.steps if step["verdict"] == "pass"]),
            "failed": len([step for step in report.steps if step["verdict"] == "fail"]),
        },
        "cleanup": "done",
    }
    failed = [step for step in report.steps if step["verdict"] == "fail"]
    record["exit_code"] = int(bool(failed))
    destination = ROOT / "data/e2e/training" / started.strftime("%Y%m%dT%H%M%S%fZ") / "report.json"
    _write(destination, record)
    sys.stdout.write(f"{len(report.steps) - len(failed)}/{len(report.steps)} static assertions passed; report: {destination}\n")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
