"""Prepare real-data AWS read checklists for the shared runner without dispatching a task."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from scripts.infrastructure.render_ecs_target import ROOT, TargetError, read_capture, read_capture_with_digest, validate_target, write_private_record

PROCEDURES = ROOT / "config/orchestration/ecs_real_runs.json"
REQUEST_SCHEMA = ROOT / "data_contracts/orchestration/ecs_real_run_request.schema.json"


def _source_inventory(case: str, request: dict[str, Any], target: dict[str, Any]) -> None:
    inventory = request["source_inventory"]
    canonical = (json.dumps(inventory, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    digest = hashlib.sha256(canonical).hexdigest()
    if request["source_inventory_sha256"] != digest or target["catalog"]["source_inventory_sha256"] != digest:
        raise TargetError("The source inventory differs from the registered target inventory.")
    publications = inventory["publications"]
    if {row["layer"] for row in publications} != {"silver", "gold"} or len({row["reservation"] for row in publications}) != 2:
        raise TargetError("The source inventory must retain both prerequisite publications.")
    tables = inventory["tables"]
    if len({(row["namespace"], row["name"]) for row in tables}) != len(tables):
        raise TargetError("The source inventory repeats a table.")
    for row in tables:
        prefix = f"s3://{target['bucket_name']}/lakehouse/{row['namespace']}/"
        if any(not row[key].startswith(prefix) or set(row[key].split("/")) & {".", ".."} for key in ("location", "metadata_location")):
            raise TargetError("The source inventory names another bucket or namespace path.")
    if case == "R3" and not any(row["namespace"] == "silver" for row in tables):
        raise TargetError("The source inventory needs existing published silver snapshots for the gold build.")


def prepare_run(case: str, target: dict[str, Any], target_sha256: str, request: dict[str, Any]) -> dict[str, Any]:
    """Bind an actual-data run checklist to its target, retained registration inventory and approved time window.

    This validates preparation records. The shared runner must execute and collect every observation before the coordinating agent audits cloud acceptance.
    """
    validate_target(target)
    if target["enabled"] is not True:
        raise TargetError("An enabled target is required for a real-run checklist.")
    if target["catalog"] is None:
        raise TargetError("A registered AWS catalog is required for real-data runs.")
    schema = read_capture(REQUEST_SCHEMA)
    if next(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(request), None) is not None:
        raise TargetError("Invalid real run request.")
    procedures, procedure_digest = read_capture_with_digest(PROCEDURES)
    if case not in procedures["runs"]:
        raise TargetError("Unknown real-run procedure.")
    procedure = procedures["runs"][case]
    start = datetime.fromisoformat(request["window_start_utc"])
    deadline = datetime.fromisoformat(request["window_deadline_utc"])
    finish_reserve = target["deadlines"]["stop_grace_seconds"] + target["deadlines"]["cleanup_seconds"]
    if (
        start.utcoffset() != timedelta(0)
        or deadline.utcoffset() != timedelta(0)
        or not finish_reserve < (deadline - start).total_seconds() <= procedure["outer_deadline_seconds"]
    ):
        raise TargetError("The run window must use UTC and reserve cleanup inside the procedure's fixed outer limit.")
    _source_inventory(case, request, target)
    return {
        "schema_version": 1,
        "proof": "A",
        "case": case,
        "name": procedure["name"],
        "preparation_only": True,
        "execution_verified": False,
        "requested_target": "ecs",
        "target_sha256": target_sha256,
        "procedure_sha256": procedure_digest,
        "request": request,
        "outer_deadline_seconds": procedure["outer_deadline_seconds"],
        "task_deadline_not_after_utc": (deadline - timedelta(seconds=finish_reserve)).isoformat().replace("+00:00", "Z"),
        "first_step_task_count": procedure["first_step_task_count"],
        "cloud_limits": target["cloud_limits"],
        "steps": [
            {"name": step["name"], "observations": [{"name": name, "status": "pending", "evidence_sha256": None} for name in step["observations"]]}
            for step in procedure["steps"]
        ],
        "cleanup_observations": procedures["cleanup_observations"],
        "observed_only": procedures["observed_only"],
        "forbidden_actions": procedures["forbidden_actions"],
        "mac_preparation_enabled": False,
        "result_file_max_bytes": procedures["result_file_max_bytes"],
    }


def main() -> int:
    """Write a private preparation card while leaving cloud execution and approval with the coordinating agent."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("R1", "R2", "R3"), required=True)
    parser.add_argument("--target", type=Path, default=ROOT / "data/orchestration/ecs_target.json")
    parser.add_argument("--run-inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        target, digest = read_capture_with_digest(arguments.target)
        card = prepare_run(arguments.case, target, digest, read_capture(arguments.run_inputs))
        write_private_record(arguments.output, card)
    except TargetError as error:
        parser.error(str(error))
    except (OSError, ValueError, TypeError, KeyError, AttributeError, RecursionError):
        parser.error("The real-run preparation inputs are invalid. No checklist was written.")
    sys.stdout.write("Prepared the private real-run checklist.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
