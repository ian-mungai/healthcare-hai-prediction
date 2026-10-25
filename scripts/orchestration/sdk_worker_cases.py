"""Check SDK preparation fixtures without starting workers, services or AWS requests."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from orchestration import worker_protocol
from scripts.orchestration.sdk_standins import MODES, Scenario, WorkerStandin

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "data_contracts/orchestration/sdk_worker.schema.json"
OWNED_CASES = {"61", "83", "85", "86", "93"}
SUBCASES = {f"61{suffix}" for suffix in "abcdefgx"}
SLOW_MODES = {"delay_headers", "drip_headers", "hang_body", "drip_body"}


def _validator(schema: Mapping[str, Any], definition: str) -> Draft202012Validator:
    return Draft202012Validator({"$schema": schema["$schema"], "$defs": schema["$defs"], "$ref": f"#/$defs/{definition}"})


def production_examples(schema: Mapping[str, Any], examples: Mapping[str, Any]) -> dict[str, bool]:
    """Run every independent positive and negative frame through the production validator."""
    frames = []
    for row in examples["positive"]:
        frames.append((row["request"], None, None, True))
        frames.append((row["reply"], row["op"], row["request"]["id"], True))
    error = examples["error_reply"]
    frames.append((error, None, error["id"], True))
    for row in examples["negative"]:
        frame = row["frame"]
        frames.append((frame, row.get("op"), None if row["target"] == "request" else frame.get("id", "synthetic"), False))
    checks = {}
    for index, (frame, operation, request_id, expected) in enumerate(frames):
        accepted = True
        try:
            worker_protocol.validate(frame, operation, request_id, schema=schema)
        except worker_protocol.ProtocolError:
            accepted = False
        checks[f"production_example_{index}"] = accepted == expected
    return checks


def check_preparation(fixtures: Path) -> dict[str, Any]:
    """Validate the closed contract and independent examples. Runtime cases remain pending."""
    matrix_path = fixtures / "case_matrix.json"
    mutation_path = fixtures / "mutation_cases.json"
    example_path = ROOT / "data_contracts/orchestration/examples/sdk_worker.json"
    schema = json.loads(SCHEMA.read_text())
    matrix = json.loads(matrix_path.read_text())
    mutations = json.loads(mutation_path.read_text())
    examples = json.loads(example_path.read_text())
    checks: dict[str, bool] = {}
    Draft202012Validator.check_schema(schema)
    operations = {row["op"] for row in matrix["operations"]}
    expected_operations = {name.removesuffix("_request") for name in schema["$defs"] if name.endswith("_request")}
    checks["all_closed_operations"] = operations == expected_operations and len(operations) == 19
    checks["common_scope_only"] = set(matrix["top_level_cases"]) == OWNED_CASES and set(matrix["deferred_cases"]) == {"107", "123"}
    checks["all_case_subrows"] = {row["case"] for row in matrix["cases"]} == SUBCASES | (OWNED_CASES - {"61"})
    checks["all_runtime_cases_pending"] = all(row["status"] == "pending_runtime" for key in ("cases", "deadline_matrix", "retry_matrix") for row in matrix[key])
    deadlines = [(row["op"], row["mode"]) for row in matrix["deadline_matrix"]]
    checks["complete_deadline_matrix"] = len(deadlines) == len(set(deadlines)) == 76 and set(deadlines) == {
        (op, mode) for op in operations for mode in SLOW_MODES
    }
    checks["eight_primary_operations"] = len(set(matrix["primary_deadline_operations"])) == 8 and set(matrix["primary_deadline_operations"]) <= operations
    retries = [(row["op"], row["fault"], row["pool"]) for row in matrix["retry_matrix"]]
    checks["complete_retry_matrix"] = len(retries) == len(set(retries)) == 76 and set(retries) == {
        (op, fault, pool) for op in operations for fault in ("throttle", "server_error") for pool in ("pooled", "per_call")
    }
    checks["single_request_expectations"] = all(row["expected_requests"] == 1 for row in matrix["retry_matrix"])
    schedule = matrix["repeated_timeout_schedule"]
    checks["twenty_five_fixed_deadlines"] = len(schedule) == 25 and [row["sequence"] for row in schedule] == list(range(1, 26))
    checks["timeout_schedule_both_lanes"] = {row["op"] for row in schedule} >= {"run_task", "s3_download", "s3_upload_part"} and all(
        row["original_deadline_must_not_reset"] for row in schedule
    )
    versions = matrix["parameter_version_matrix"]
    checks["inclusive_parameter_versions"] = all(row["must_read"] == list(range(row["dispatch_version"], row["collection_version"] + 1)) for row in versions)
    checks["incomplete_scans_withhold_results"] = all(
        row["scan_complete"] == (not row["unreadable"]) and (row["scan_complete"] or (row["logs_withheld"] and row["outputs_refused"])) for row in versions
    )
    recovery = matrix["recovery_evidence_matrix"]
    checks["complete_recovery_matrix"] = len(recovery) == 20 and {(row["group"], row["token"]) for row in recovery} == {
        (group, token) for group in ("present", "absent", "denied", "timed_out", "not_probed") for token in ("seen", "not_seen", "denied", "timed_out")
    }
    checks["read_only_recovery_contract"] = all(
        row["signals"] == 0 and row["first_read"] == "record_and_alert" and row["later_read"] == ("close_absent" if row["group"] == "absent" else "keep_open")
        for row in recovery
    )
    checks["all_owned_stop_variants"] = len(set(matrix["owned_stop_variants"])) == 7
    probes = mutations["mutations"]
    checks["all_twenty_four_intended_defects"] = len(probes) == 24 and {row["design_defect"] for row in probes} == set(range(1, 25))
    checks["mutations_pending_runtime"] = all(row["status"] == "pending_runtime" and row["cases"] and row["expected_failure"] for row in probes)
    checks["additional_mutations_pending"] = len(mutations["additional_mutations"]) == 7 and all(
        row["status"] == "pending_runtime" and row["case"] in OWNED_CASES for row in mutations["additional_mutations"]
    )
    witness = next(row for row in probes if row["design_defect"] == 18)
    checks["private_spawn_witness_exemption"] = witness["private_spawn_witness_exempt"] is True and witness["saved_ledger_witness"] == "redacted"
    checks["positive_operation_coverage"] = len(examples["positive"]) == 19 and {row["op"] for row in examples["positive"]} == operations
    for row in examples["positive"]:
        for target in ("request", "reply"):
            checks[f"positive_{row['op']}_{target}"] = _validator(schema, f"{row['op']}_{target}").is_valid(row[target])
    checks["positive_typed_error"] = _validator(schema, "error_reply").is_valid(examples["error_reply"])
    for row in examples["negative"]:
        definition = "error_reply" if row["target"] == "error_reply" else f"{row['op']}_{row['target']}"
        checks[f"reject_{row['name']}"] = not _validator(schema, definition).is_valid(row["frame"])
    production = production_examples(schema, examples)
    checks["production_validator_agrees_with_examples"] = all(production.values())
    for name, definition, owner, key in (
        ("open_parameter_object", "list_tasks_request", ["properties", "params"], "additionalProperties"),
        ("unversioned_parameter_name", "get_parameter_request", ["properties", "params", "properties", "name"], "pattern"),
        ("raw_service_error_body", "error_reply", ["properties", "error"], "additionalProperties"),
    ):
        mutant = copy.deepcopy(schema)
        selected = mutant["$defs"][definition]
        for field in owner:
            selected = selected[field]
        if key == "additionalProperties":
            selected[key] = True
        else:
            selected.pop(key)
        intended = {
            "open_parameter_object": "list_tasks_free_keyword",
            "unversioned_parameter_name": "parameter_latest",
            "raw_service_error_body": "raw_error_body",
        }[name]
        frame = next(row["frame"] for row in examples["negative"] if row["name"] == intended)
        checks[f"schema_mutation_caught_{name}"] = _validator(mutant, definition).is_valid(frame)
    for mode in MODES:
        standin = WorkerStandin(Scenario("synthetic_operation", mode))
        checks[f"standin_{mode}_construction_has_no_effect"] = standin.events == () and standin.endpoint == ""
    sources = (SCHEMA, matrix_path, mutation_path, example_path, Path(__file__), Path(__file__).with_name("sdk_standins.py"))
    return {
        "scope": "Local preparation record with tracked protocol examples and production-validator checks",
        "classification": "Synthetic",
        "checks": checks,
        "failed": [name for name, ok in checks.items() if not ok],
        "counts": {
            "operations": len(operations),
            "production_example_frames": len(production),
            "slow_variants": len(deadlines),
            "retry_variants": len(retries),
            "recovery_pairs": len(recovery),
            "intended_runtime_defects": len(probes),
        },
        "runtime_status": "pending_runtime",
        "runtime_limits": [
            "Case and mutation matrices are ignored local preparation records supplied through --fixtures",
            "No SDK worker or HTTP listener started",
            "No native process, descendant, peer-close or Linux proof",
            "No runtime mutation was executed",
        ],
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
        "cleanup": "done; no process, thread, listener or reservation started",
    }


def main() -> int:
    """Write a repeatable static report while keeping runtime acceptance explicitly pending."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-fixtures", required=True, action="store_true")
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    report = check_preparation(args.fixtures.resolve())
    report["code_revision"] = args.revision
    report["written_utc"] = datetime.now(UTC).isoformat()
    report["command"] = [sys.executable, "-B", "-m", "scripts.orchestration.sdk_worker_cases", *sys.argv[1:]]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"report": str(args.output), "failed": report["failed"], "runtime_status": report["runtime_status"]}) + "\n")
    return int(bool(report["failed"]))


if __name__ == "__main__":
    sys.exit(main())
