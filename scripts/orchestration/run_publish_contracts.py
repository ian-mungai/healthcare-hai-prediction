"""Check shipped publication declarations and two host-only recovery and deadline scenarios, and record the evidence without starting services."""

from __future__ import annotations

import argparse
import ast
import dataclasses
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from orchestration.interface import SubmissionRequest
from scripts.orchestration import publish_recovery_cases

ROOT = Path(__file__).resolve().parents[2]
DAGS = ("silver_transform_dag", "gold_transform_dag", "gold_maintenance_dag")
ROWS = {
    "silver": (
        "catalog_up",
        "snapshot_project",
        "pin_bronze",
        "silver_build",
        "silver_validate",
        "verify_bronze",
        "seal_silver",
        "silver_export",
        "silver_candidates",
        "silver_reconcile",
        "silver_backup",
        "silver_commit",
        "record_silver_snapshot",
        "catalog_release",
    ),
    "gold": (
        "catalog_up",
        "snapshot_project",
        "reserve_silver",
        "pin_silver",
        "read_silver",
        "gold_build",
        "gold_validate",
        "cross_layer_checks",
        "seal_gold",
        "gold_export",
        "publish_candidates",
        "reconcile_candidates",
        "catalog_backup",
        "commit_publish",
        "catalog_release",
    ),
    "maintenance": ("catalog_up", "gold_maintenance", "catalog_release"),
}


def declarations(folder: Path) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Return literal DAG declarations and hashes after parsing every shipped Python file."""
    found: dict[str, dict[str, Any]] = {}
    hashes = {}
    for path in sorted(folder.glob("*.py")):
        raw = path.read_bytes()
        hashes[path.name] = hashlib.sha256(raw).hexdigest()
        tree = ast.parse(raw, filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id != "DAG":
                continue
            fields = {item.arg: item.value for item in node.keywords}
            identifier = fields.get("dag_id")
            if not isinstance(identifier, ast.Constant) or not isinstance(identifier.value, str):
                raise ValueError(f"{path.name}: DAG ID must be a literal")
            if identifier.value in found:
                raise ValueError(f"{path.name}: duplicate DAG ID")
            schedule = fields.get("schedule")
            found[identifier.value] = {"file": path.name, "manual": isinstance(schedule, ast.Constant) and schedule.value is None}
    return found, hashes


def collect(root: Path) -> dict[str, Any]:
    """Check the split declarations and complete executable rows against the shipped schema."""
    declared, hashes = declarations(root / "dags")
    document = json.loads((root / "config/orchestration/broker_templates.json").read_text())
    schema = json.loads((root / "config/orchestration/broker_templates.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    schema_errors = [error.message for error in Draft202012Validator(schema).iter_errors(document)]
    checks: dict[str, Any] = {
        "existing_declarations_parse": {"ok": bool(declared), "dag_ids": sorted(declared)},
        "template_schema": {"ok": not schema_errors, "errors": schema_errors},
        "combined_dag_retired": {"ok": "transform_dag" not in declared},
    }
    for name in DAGS:
        checks[name] = {"ok": name in declared and declared[name]["manual"], "observed": declared.get(name)}
    for mode in ("publish_fixture", "real"):
        missing = []
        for name in sorted({name for family in ROWS.values() for name in family}):
            row = document["templates"].get(name, {}).get("modes", {}).get(mode, {})
            if row.get("no_op") is not False or not (row.get("launcher") or row.get("control")):
                missing.append(name)
        checks[f"executable_rows_{mode}"] = {"ok": not missing, "missing": missing}
    request_schema = json.loads((root / "data_contracts/orchestration/submission_request.schema.json").read_text())
    validator = Draft202012Validator(request_schema)
    request = SubmissionRequest("pin_silver", "20261017T120000Z-00000001", "publish_fixture", 1, "silver_transform_dag", "pin_silver", "manual__contract")
    chosen = dataclasses.replace(request, input_run_id="20261017T110000Z-00000002")
    selected = chosen.as_dict()
    accepted = not list(validator.iter_errors(selected))
    wrong_step = {**selected, "template": "gold_build"}
    unsafe = {**selected, "input_run_id": "../../another-file"}
    checks["selected_silver_request"] = {"ok": accepted and bool(list(validator.iter_errors(wrong_step))) and bool(list(validator.iter_errors(unsafe)))}
    default = request.as_dict()
    checks["default_request_unchanged"] = {
        "ok": "input_run_id" not in default
        and default == {key: value for key, value in selected.items() if key != "input_run_id"}
        and not list(validator.iter_errors(default))
    }
    checks["selected_request_round_trip"] = {"ok": SubmissionRequest.from_record(selected) == chosen}
    for name, scenario in (
        ("silver_commit_receipt_recovery", publish_recovery_cases.receipt_recovery),
        ("pipeline_wait_deadline", publish_recovery_cases.deadline_cases),
    ):
        results = scenario()
        checks[name] = {"ok": bool(results) and all(results.values()), "results": results}
    return {
        "scope": "static declarations and schema, plus two host-only scenarios (in-memory catalog, controlled clock); no scheduler, launcher, Spark or network",
        "checks": checks,
        "dag_sha256": hashes,
    }


def main() -> int:
    """Write a timestamped contract report and return nonzero for any unmet contract."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    moment = datetime.now(UTC)
    destination = args.out or ROOT / "data/e2e/publish_contracts" / moment.strftime("%Y%m%dT%H%M%S_%fZ")
    destination.mkdir(parents=True, exist_ok=False)
    report = collect(args.root)
    report["source_sha256"] = {
        name: hashlib.sha256((args.root / name).read_bytes()).hexdigest()
        for name in (
            "config/orchestration/broker_templates.json",
            "config/orchestration/broker_templates.schema.json",
            "data_contracts/orchestration/submission_request.schema.json",
            "orchestration/interface.py",
            "scripts/lakehouse/gold_publish.py",
            "scripts/orchestration/pipeline_control.py",
            "scripts/orchestration/publish_recovery_cases.py",
        )
    }
    report.update({"at_utc": moment.isoformat(), "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    (destination / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (destination / "retention.json").write_text(
        json.dumps(
            {
                "class": 4,
                "purpose": "Static publication contract evidence",
                "created": moment.isoformat(),
                "end_condition": "No review or report cites this run",
            }
        )
        + "\n"
    )
    failed = [name for name, result in report["checks"].items() if not result["ok"]]
    sys.stdout.write(json.dumps({"report": str(destination / "report.json"), "failed": failed}) + "\n")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
