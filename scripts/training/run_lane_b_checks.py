"""Run bounded training orchestration checks without services or real observations."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
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
    "scripts/training/job.py",
    "scripts/training/job_cases.py",
    "scripts/training/job_failure.py",
    "scripts/training/job_inputs.py",
    "scripts/training/bundle.py",
    "scripts/training/job_actions.py",
    "scripts/training/run_tracking.py",
    "scripts/training/registry.py",
    "scripts/training/promotion.py",
    "scripts/training/promotion_check.py",
    "scripts/training/promotion_cases.py",
    "scripts/training/job_action_cases.py",
    "services/orchestration/docker-compose.orchestration.yaml",
    "scripts/orchestration/transform_fixtures.py",
    "config/quality/dependency_runtimes.json",
    "scripts/training/run_lane_b_checks.py",
    "scripts/orchestration/trigger_dag.py",
    "scripts/orchestration/offline_checks.py",
    "scripts/infrastructure/secret_store.py",
    "dags/training_dag.py",
    "dags/model_promotion_dag.py",
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
        raw = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "local", "model": "m2"}
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
        case("job_lanes_accept_aws", training_contract.parameters_for("training_dag", {**raw, "mlflow": "aws"}, training_contract.JOB_LANES)["mlflow"] == "aws")
        for chosen in training_contract.MODELS:
            case(f"model_{chosen}_accepted", training_contract.parameters_for("training_dag", {**raw, "model": chosen})["model"] == chosen)
        for name, changed in (
            ("missing", {key: value for key, value in raw.items() if key != "model"}),
            ("unknown", {**raw, "model": "m7"}),
            ("not_text", {**raw, "model": 2}),
        ):
            try:
                training_contract.parameters_for("training_dag", changed)
            except ValueError as error:
                case(f"model_{name}_refused", "model" in str(error))
            else:
                case(f"model_{name}_refused", False)
        drift = "20261109T010000Z-1234abcd"
        ids = {chosen: training_contract.model_training_run_id(drift, chosen) for chosen in training_contract.MODELS}
        case("model_run_ids_differ_per_model", len(set(ids.values())) == len(ids))
        case("model_run_id_is_stable_for_a_retry", ids["m3"] == training_contract.model_training_run_id(drift, "m3"))
        case("model_run_id_has_the_run_form", all(re.fullmatch(training_contract.RUN_ID, value) for value in ids.values()))
        base = "20261109T000000Z-b45e0001"
        research = training_contract.research_run_id(base)
        case("research_run_id_has_the_run_form", re.fullmatch(training_contract.RUN_ID, research) is not None and research != base)
        case("research_run_id_is_stable_for_a_retry", research == training_contract.research_run_id(base))
        case("research_run_id_differs_per_base", research != training_contract.research_run_id("20261109T000000Z-b45e0002"))
        attempts = {number: training_contract.research_run_id(base, number) for number in (1, 2, 3, 99)}
        case("research_attempt_one_is_the_default_run", attempts[1] == research)
        case("research_attempts_differ", len(set(attempts.values())) == len(attempts))
        case("research_attempt_ids_have_the_run_form", all(re.fullmatch(training_contract.RUN_ID, value) for value in attempts.values()))
        case("research_attempt_id_is_stable_for_a_retry", attempts[2] == training_contract.research_run_id(base, 2))
        case("research_attempt_id_differs_per_base", attempts[2] != training_contract.research_run_id("20261109T000000Z-b45e0002", 2))
        refused_attempts: list[tuple[str, Any]] = [("zero", 0), ("above_the_limit", 100), ("a_flag", True), ("text", "2"), ("a_fraction", 2.0)]
        for name, bad in refused_attempts:
            try:
                training_contract.research_run_id(base, bad)
            except ValueError as error:
                case(f"research_attempt_refuses_{name}", "attempt" in str(error))
            else:
                case(f"research_attempt_refuses_{name}", False)
        research_form = {"base_run": base, "population": "primary", "hai_types": ["HAI_1"], "mlflow": "aws"}
        launch = training_contract.LAUNCH_LANES
        case("research_form_without_an_attempt_is_accepted", training_contract.parameters_for("research_dag", research_form, launch) == research_form)
        case(
            "research_form_with_an_attempt_is_accepted",
            training_contract.parameters_for("research_dag", {**research_form, "research_attempt": 2}, launch)["research_attempt"] == 2,
        )
        refused_forms: list[tuple[str, Any]] = [("one", 1), ("zero", 0), ("above_the_limit", 100), ("a_flag", True), ("text", "2"), ("none", None)]
        for name, bad in refused_forms:
            try:
                training_contract.parameters_for("research_dag", {**research_form, "research_attempt": bad}, launch)
            except ValueError as error:
                case(f"research_form_refuses_the_attempt_{name}", "research_attempt" in str(error))
            else:
                case(f"research_form_refuses_the_attempt_{name}", False)
        second_chain = {step: training_contract.chain_run_id(attempts[2], "HAI_1", step) for step in training_contract.CHAIN_STEPS}
        first_chain = {step: training_contract.chain_run_id(research, "HAI_1", step) for step in training_contract.CHAIN_STEPS}
        case("chain_run_ids_differ_per_research_attempt", not set(second_chain.values()) & set(first_chain.values()))
        chain = {
            (hai_type, step): training_contract.chain_run_id(research, hai_type, step)
            for hai_type in training_contract.HAI_TYPES
            for step in training_contract.CHAIN_STEPS
        }
        case("chain_run_ids_differ_per_type_and_step", len(set(chain.values())) == len(chain) and research not in chain.values())
        case("chain_run_ids_have_the_run_form", all(re.fullmatch(training_contract.RUN_ID, value) for value in chain.values()))
        case("chain_run_id_is_stable_for_a_retry", chain["HAI_2", "canary"] == training_contract.chain_run_id(research, "HAI_2", "canary"))
        for name, arguments in (("step", (research, "HAI_1", "deploy")), ("type", (research, "HAI_9", "canary")), ("run", ("latest", "HAI_1", "canary"))):
            try:
                training_contract.chain_run_id(*arguments)
            except ValueError as error:
                case(f"chain_run_id_refuses_{name}", name in str(error))
            else:
                case(f"chain_run_id_refuses_{name}", False)
        case(
            "record_keys_sit_under_the_research_run",
            [training_contract.record_key(kind, research, "HAI_3") for kind in training_contract.RECORD_KINDS]
            == [f"research/{research}/{kind}_HAI_3.json" for kind in ("research", "replay", "canary", "notice")],
        )
        for name, arguments in (("kind", ("report", research, "HAI_1")), ("type", ("canary", research, "HAI_9")), ("run", ("canary", "latest", "HAI_1"))):
            try:
                training_contract.record_key(*arguments)
            except ValueError:
                case(f"record_key_refuses_{name}", True)
            else:
                case(f"record_key_refuses_{name}", False)
        feature_run = training_contract.model_features_run_id(base, "m2")
        drift_run = training_contract.model_drift_run_id(feature_run)
        case(
            "trained_run_id_is_the_run_chain_of_the_model",
            training_contract.trained_run_id(base, "m2") == training_contract.model_training_run_id(drift_run, "m2"),
        )
        case("trained_run_ids_differ_per_model", len({training_contract.trained_run_id(base, item) for item in training_contract.PROMOTION_MODELS}) == 3)
        canary = {"research_run": research, "hai_type": "HAI_1", "mlflow": "aws"}
        notice = {**canary, "outcome": "held"}
        promotion = {"hai_type": "HAI_1", "model_version": "3", "promotion_mode": "promote", "mlflow": "aws"}
        launch = training_contract.LAUNCH_LANES
        case(
            "chain_parameters_accept_each_step",
            all(
                training_contract.chain_parameters_for(step, raw, launch) == raw
                for step, raw in (("canary_check", canary), ("owner_notice", notice), ("promote", promotion))
            ),
        )
        for name, step, changed_parameters in (
            ("canary_unknown_key", "canary_check", {**canary, "outcome": "held"}),
            ("canary_bad_run", "canary_check", {**canary, "research_run": "latest"}),
            ("canary_bad_type", "canary_check", {**canary, "hai_type": "HAI_9"}),
            ("canary_local_lane", "canary_check", {**canary, "mlflow": "local"}),
            ("notice_missing_outcome", "owner_notice", canary),
            ("notice_bad_outcome", "owner_notice", {**notice, "outcome": "approved"}),
            ("promotion_rollback", "promote", {**promotion, "promotion_mode": "rollback"}),
            ("promotion_bad_version", "promote", {**promotion, "model_version": "0"}),
            ("research_step_is_not_a_chain_step", "research", canary),
        ):
            try:
                training_contract.chain_parameters_for(step, changed_parameters, launch)
            except ValueError:
                case(f"chain_parameters_refuse_{name}", True)
            else:
                case(f"chain_parameters_refuse_{name}", False)
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
        from scripts.training import job_cases, lane_b_cases, lane_b_dag_cases, lane_b_fix_cases

        with (
            mock.patch.object(launcher.LauncherClient, "call", side_effect=RuntimeError("light checks forbid launcher requests")),
            mock.patch("orchestration.launcher.DockerApi.request", side_effect=RuntimeError("light checks forbid engine requests")),
            mock.patch.object(secret_store, "open_store", side_effect=RuntimeError("light checks forbid secret-store access")),
            mock.patch.object(secret_store, "fetch_all", side_effect=RuntimeError("light checks forbid secret-store access")),
        ):
            cases.extend(lane_b_cases.run())
            cases.extend(lane_b_dag_cases.run())
            cases.extend(lane_b_fix_cases.run())
            cases.extend(job_cases.run())
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
