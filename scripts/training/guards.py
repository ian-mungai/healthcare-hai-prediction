"""Validate static training inputs before data access and record the plan used by the run."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from scripts.process import TimeoutExpired, run_command
from scripts.training import targets

ROOT = Path(__file__).resolve().parents[2]
SLOTS = ("m1_ols", "m2_elastic_net", "m3_mixed", "m4_boosting", "m5_spatial", "m6_fixed_effects", "rq4_reduced")
ALL_ROW_BRIDGES = frozenset(("hai_outcome_measures_bridge.csv", "validation_measures_bridge.csv"))
EXCLUDED_DECISIONS = frozenset(("exclude_primary", "comparison_only"))
CMI_COLUMNS = frozenset(("cmi", "cmi_cases", "has_cmi", "is_cmi_held"))


class GuardError(ValueError):
    """A named static input cannot be used for training."""


def _json(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise GuardError(f"{path.name}: invalid JSON at line {error.lineno}") from error
    except OSError as error:
        raise GuardError(f"{path.name}: cannot read ({type(error).__name__})") from error
    if not isinstance(document, dict):
        raise GuardError(f"{path.name}: expected a JSON object")
    return document


def _validate(path: Path, schema_path: Path) -> dict[str, Any]:
    document, schema = _json(path), _json(schema_path)
    try:
        Draft202012Validator.check_schema(schema)
        errors = sorted(Draft202012Validator(schema).iter_errors(document), key=lambda error: tuple(str(part) for part in error.absolute_path))
    except SchemaError as schema_error:
        raise GuardError(f"{schema_path.name}: invalid JSON schema") from schema_error
    if errors:
        error = errors[0]
        field = ".".join(str(part) for part in error.absolute_path) or "root"
        raise GuardError(f"{path.name}: {field}: schema {error.validator} failed. Restore a value allowed by {schema_path.name}.")
    return document


def _plan(root: Path) -> bytes:
    path = root / "docs/analysis_plan.md"
    for component in (root / "docs", path):
        if component.is_symlink():
            raise GuardError("plan: symbolic link is not allowed. Use a regular snapshot file at docs/analysis_plan.md.")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError as error:
        raise GuardError("plan: missing docs/analysis_plan.md") from error
    except OSError as error:
        raise GuardError(f"plan: cannot open docs/analysis_plan.md ({type(error).__name__})") from error
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise GuardError("plan: regular file required at docs/analysis_plan.md")
    with os.fdopen(descriptor, "rb") as handle:
        content = handle.read()
    if not content:
        raise GuardError("plan: empty docs/analysis_plan.md")
    return content


def _bridge_controls(root: Path) -> set[str]:
    controls: set[str] = set()
    paths = sorted((root / "dbt/seeds/gold").glob("*_bridge.csv"))
    missing = ALL_ROW_BRIDGES - {path.name for path in paths}
    if missing:
        raise GuardError(f"exclusions: missing control bridge {sorted(missing)[0]}")
    for path in paths:
        name = path.name
        try:
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                if not {"fact", "measure_key", "value_column", "measure_control", "review_decision"}.issubset(reader.fieldnames or []):
                    raise GuardError(f"{name}: missing control-bridge columns")
                for row in reader:
                    if name not in ALL_ROW_BRIDGES and row["review_decision"] not in EXCLUDED_DECISIONS:
                        continue
                    control = row["measure_control"]
                    if not control:
                        raise GuardError(f"{name}: a measure mapping has no control")
                    controls.add(control)
        except OSError as error:
            raise GuardError(f"{name}: cannot read control bridge ({type(error).__name__})") from error
    return controls


def _columns(schema: dict[str, Any]) -> tuple[dict[str, tuple[dict[str, Any], dict[str, Any]]], set[str]]:
    tables = schema.get("tables")
    servable = schema.get("servable")
    if schema.get("schema") != 1 or not isinstance(tables, dict) or not isinstance(servable, list) or not all(isinstance(item, str) for item in servable):
        raise GuardError("feature_schema.json: expected schema 1 with tables and servable references")
    columns: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for table in tables.values():
        if not isinstance(table, dict) or not isinstance(table.get("columns"), list):
            raise GuardError("feature_schema.json: table columns must be a list")
        for column in table["columns"]:
            if not isinstance(column, dict) or not isinstance(column.get("name"), str) or not isinstance(column.get("role"), str):
                raise GuardError("feature_schema.json: column needs a name and role")
            view = column.get("view")
            if view is None:
                continue
            if not isinstance(view, str) or not isinstance(column.get("pivot"), dict):
                raise GuardError("feature_schema.json: registered column needs a view and pivot")
            match = column["pivot"].get("match", {})
            if not isinstance(match, dict) or any(not isinstance(value, str) for value in match.values()):
                raise GuardError("feature_schema.json: pivot match values must be strings")
            reference = f"{view}:{column['name']}"
            if reference in columns:
                raise GuardError(f"feature_schema.json: duplicate reference {reference}")
            columns[reference] = (table, column)
    return columns, set(servable)


def _control(column: dict[str, Any]) -> str:
    return str(column["pivot"].get("match", {}).get("measure_control", ""))


def _is_excluded(control: str, excluded: set[str]) -> bool:
    return control in excluded or control.split(".", 1)[0] in excluded


def _references(document: dict[str, Any], columns: dict[str, tuple[dict[str, Any], dict[str, Any]]], servable: set[str], excluded: set[str]) -> None:
    references = document["references"]
    for reference in references:
        if reference.startswith("label_"):
            raise GuardError(f"reference: label {reference} cannot be a predictor")
        if reference not in columns:
            raise GuardError(f"reference: not registered: {reference}")
        table, column = columns[reference]
        if table.get("role") == "label" or column["role"] == "label":
            raise GuardError(f"reference: label {reference} cannot be a predictor")
        if table.get("role") != "predictor" or column["role"] != "predictor":
            raise GuardError(f"reference: not a predictor: {reference}")
        if reference not in servable:
            raise GuardError(f"reference: not registered in a predictor view: {reference}")
        if _is_excluded(_control(column), excluded):
            raise GuardError(f"reference: excluded control {_control(column)} in {reference}")
    expected = {
        reference
        for reference, (table, column) in columns.items()
        if table.get("role") == "predictor"
        and column["role"] == "predictor"
        and reference in servable
        and not _is_excluded(_control(column), excluded)
        and (
            document["scope"] == "all"
            or table.get("family") == "care_compare"
            or (table.get("family") == "hospital_window" and column["name"] not in CMI_COLUMNS)
        )
    }
    selected = set(references)
    if not selected.issubset(expected) or (document["scope"] == "all" and selected != expected):
        raise GuardError(f"{document['slot']}: feature list differs from its {document['scope']} scope. Regenerate the reviewed feature list.")


def _labels(schema: dict[str, Any], lists: dict[str, list[dict[str, str]]]) -> None:
    for target in lists["target_candidates"]:
        number = target["hai_type"].split("_")[1]
        view, name = f"label_hai_{number}", f"hai_{number}__sir__number"
        table = schema["tables"].get("label_matrix_hai_outcomes", {})
        registered = table.get("label_views", {}).get(view, [])
        found = [col for col in table.get("columns", []) if col.get("name") == name and col.get("view") == view and col.get("role") == "label"]
        if (
            table.get("role") != "label"
            or name not in registered
            or len(found) != 1
            or found[0]["pivot"].get("value_column") != "sir"
            or found[0]["pivot"].get("match", {}).get("hai_type") != target["hai_type"]
        ):
            raise GuardError(f"target label: not registered: {view}:{name}")


def check(root: Path = ROOT, *, feature_schema: Path | None = None, git_commit: str) -> dict[str, Any]:
    """Return a deterministic guard record after validating only static snapshot metadata."""
    if not re.fullmatch(r"[0-9a-f]{40}", git_commit):
        raise GuardError("git_commit: a 40-character lowercase Git commit is required")
    content = _plan(root)
    config, contracts = root / "config/training", root / "data_contracts/training"
    documents: dict[str, dict[str, Any]] = {}
    paths: dict[str, Path] = {}
    for name in ("models", "promotion", "census_regions", "excluded_controls", "targets"):
        paths[name] = config / f"{name}.json"
        documents[name] = _validate(paths[name], contracts / f"{name}.schema.json")
    for name in ("bundle", "model_contract"):
        paths[name] = contracts / f"{name}.json"
        documents[name] = _validate(paths[name], contracts / f"{name}.schema.json")
    target_problems = targets.check(config / "targets.json", root / "dbt/seeds/gold/hai_outcome_measures_bridge.csv")
    if target_problems:
        raise GuardError("targets: " + "; ".join(target_problems))
    excluded = set(documents["excluded_controls"]["controls"])
    bridge_controls = _bridge_controls(root)
    if excluded != bridge_controls:
        raise GuardError("exclusions: bridge mismatch. Regenerate excluded_controls.json from all gold control bridges and their review decisions.")
    schema_path = feature_schema or root / "data_contracts/features/feature_schema.json"
    feature = _json(schema_path)
    columns, servable = _columns(feature)
    lists, problems = targets.load_targets(config / "targets.json")
    if problems:
        raise GuardError("targets: " + "; ".join(problems))
    forbidden = excluded | {entry["control"] for entries in lists.values() for entry in entries} | set(feature.get("excluded_controls", []))
    _labels(feature, lists)
    for slot in SLOTS:
        key = f"feature_lists/{slot}"
        paths[key] = config / f"{key}.json"
        document = _validate(paths[key], contracts / "feature_list.schema.json")
        if document["slot"] != slot:
            raise GuardError(f"{slot}.json: slot does not match the filename")
        _references(document, columns, servable, forbidden)
        if slot != "rq4_reduced" and not set(documents["models"]["slots"][slot]["forced_references"]).issubset(document["references"]):
            raise GuardError(f"{slot}: a forced covariate is missing from the feature list")
    file_hashes = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in sorted(paths.items())}
    config_hashes = {name: value for name, value in file_hashes.items() if name not in ("bundle", "model_contract")}
    return {
        "schema": 1,
        "analysis_plan_sha256": hashlib.sha256(content).hexdigest(),
        "git_commit": git_commit,
        "targets_sha256": file_hashes["targets"],
        "training_config_sha256": hashlib.sha256(json.dumps(config_hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "feature_schema_sha256": hashlib.sha256(schema_path.read_bytes()).hexdigest(),
        "master_seed": documents["models"]["master_seed"],
        "file_sha256": file_hashes,
    }


def main() -> int:
    """Validate static snapshot files and optionally save guard.json without reading study data."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", required=True)
    parser.add_argument("--root", type=Path, default=ROOT, help="snapshot code root")
    parser.add_argument("--feature-schema", type=Path, help="feature-schema metadata input, otherwise the snapshot's schema")
    parser.add_argument("--git-commit", help="the commit recorded by snapshot_training, required for a copy without Git metadata")
    parser.add_argument("--output", type=Path, help="write the deterministic record, for example guard.json")
    args = parser.parse_args()
    try:
        commit = args.git_commit
        if commit is None:
            result = run_command("git", ["rev-parse", "HEAD"], cwd=args.root, timeout=10)
            if result.returncode:
                raise GuardError("git_commit: cannot read Git revision. Supply the snapshot's --git-commit.")
            commit = result.stdout.strip()
        record = check(args.root, feature_schema=args.feature_schema, git_commit=commit)
        text = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            temporary = args.output.with_name(args.output.name + ".tmp")
            temporary.write_text(text, encoding="utf-8")
            temporary.replace(args.output)
        sys.stdout.write(text)
    except (GuardError, OSError, TimeoutExpired) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
