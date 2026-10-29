"""Run bounded training orchestration checks without services or real observations."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SOURCE_FILES = (
    "orchestration/code_digest.py",
    "orchestration/training_contract.py",
    "orchestration/training_approvals.py",
    "orchestration/training_render.py",
    "orchestration/mlflow_service.py",
    "orchestration/launcher.py",
    "orchestration/network_policy.py",
    "orchestration/pipeline_render.py",
    "orchestration/graph.py",
    "orchestration/interface.py",
    "orchestration/dag_runtime.py",
    "scripts/features/training_extract.py",
    "scripts/training/step_records.py",
    "scripts/training/lane_b_cases.py",
    "scripts/training/lane_b_dag_cases.py",
    "scripts/training/lane_b_fix_cases.py",
    "services/orchestration/docker-compose.orchestration.yaml",
    "scripts/orchestration/transform_fixtures.py",
    "scripts/orchestration/run_stack_e2e.py",
    "config/quality/dependency_runtimes.json",
    "scripts/training/run_lane_b_checks.py",
    "scripts/orchestration/broker.py",
    "scripts/orchestration/run_control.py",
    "scripts/orchestration/pipeline_control.py",
    "scripts/orchestration/supervisor.py",
    "scripts/orchestration/trigger_dag.py",
    "scripts/orchestration/offline_checks.py",
    "scripts/infrastructure/secret_store.py",
    "dags/training_dag.py",
    "dags/model_promotion_dag.py",
    "config/orchestration/broker_templates.json",
    "config/orchestration/broker_templates.schema.json",
    "data_contracts/orchestration/submission_request.schema.json",
    "data_contracts/training/extract_manifest.schema.json",
    "services/mlflow/docker-compose.mlflow.yaml",
    "services/training/docker-compose.training.yaml",
)


def source_hashes() -> dict[str, str]:
    """Record every file this lane's light cases exercise, without scanning unrelated inputs."""
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCE_FILES if (ROOT / name).is_file()}


def run_checks(output: Path) -> int:
    """Exercise the real orchestration boundaries and save a repeatable local report."""
    cases: list[dict[str, Any]] = []
    before = source_hashes()

    def case(name: str, passed: bool, detail: str = "") -> None:
        cases.append({"name": name, "passed": passed, "detail": detail})

    try:
        from orchestration import code_digest, mlflow_service, training_contract
    except ImportError as error:
        for name in ("code_digest", "training_contract", "mlflow_service"):
            case(f"module_{name}", False, str(error))
    else:
        for name in ("code_digest", "training_contract", "mlflow_service"):
            case(f"module_{name}", True)
    if all(item["passed"] for item in cases):
        from orchestration import graph
        from orchestration.interface import SubmissionRequest

        for dag in training_contract.DAGS:
            case(f"graph_{dag}", not graph.validate_transform_graph(graph.reference_graph(dag), dag))
        raw = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "local"}
        params = training_contract.parameters_for("training_dag", raw)
        first = SubmissionRequest("guard", "20261024T000000Z-1234abcd", "fixture", 1, "training_dag", "guard", "manual", training=params)
        second = SubmissionRequest(
            "guard", first.run_id, first.mode, 1, first.dag_id, first.task_id, first.dag_run_id, training={**params, "population": "sensitivity"}
        )
        case("request_parameters_persist", SubmissionRequest.from_record(first.as_dict()).training == params)
        case("request_parameter_identity", first.as_dict() != second.as_dict())
        try:
            training_contract.parameters_for("training_dag", {**raw, "mlflow": "aws"})
        except ValueError as error:
            case("aws_route_refused", "not built" in str(error))
        else:
            case("aws_route_refused", False)
        case("service_port", mlflow_service.PORT == 5100)
        with tempfile.TemporaryDirectory(prefix="hai_training_contract_") as temporary:
            source = Path(temporary) / "source"
            source.mkdir()
            for root in code_digest.TRAINING_ROOTS:
                if "*" in root:
                    path = source / root.replace("*", "sample")
                elif Path(root).suffix:
                    path = source / root
                else:
                    path = source / root / "sample.py"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("synthetic input\n")
            digest = code_digest.tree_digest(source)
            cache = source / "scripts/__pycache__"
            cache.mkdir()
            (cache / "sample.pyc").write_bytes(b"synthetic bytecode")
            case("bytecode_does_not_change_digest", code_digest.tree_digest(source) == digest)
            (source / "scripts/sample.py").write_text("changed synthetic input\n")
            case("source_edit_changes_digest", code_digest.tree_digest(source) != digest)
        from orchestration import launcher
        from scripts.infrastructure import secret_store
        from scripts.training import lane_b_cases, lane_b_dag_cases, lane_b_fix_cases

        with (
            mock.patch.object(launcher.LauncherClient, "call", side_effect=RuntimeError("light checks forbid launcher requests")),
            mock.patch("orchestration.launcher.DockerApi.request", side_effect=RuntimeError("light checks forbid engine requests")),
            mock.patch.object(secret_store, "open_store", side_effect=RuntimeError("light checks forbid secret-store access")),
            mock.patch.object(secret_store, "fetch_all", side_effect=RuntimeError("light checks forbid secret-store access")),
        ):
            cases.extend(lane_b_cases.run())
            cases.extend(lane_b_dag_cases.run())
            cases.extend(lane_b_fix_cases.run())
    output.mkdir(parents=True, exist_ok=True)
    case("candidate_did_not_change", before == source_hashes())
    source_identity = hashlib.sha256(json.dumps(before, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    report = {
        "schema_version": 1,
        "classification": "Synthetic",
        "boundary": "light checks with explicit stand-ins, no service or production acceptance",
        "source_sha256": source_identity,
        "source_files": before,
        "cases": cases,
        "passed": sum(bool(item["passed"]) for item in cases),
        "failed": sum(not item["passed"] for item in cases),
        "cleanup": "done",
    }
    (output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return int(bool(report["failed"]))


def main() -> int:
    """Run the light checks and write their report to the named local folder."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    return run_checks(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
