"""The real-image DAG runner starts the image itself on every run and judges only what that run returns, against the DAG folder's own inventory."""

from __future__ import annotations

import ast
import hashlib
import importlib
import json
import os
import re
import shutil
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.orchestration import offline_checks, real_image_dags
from scripts.orchestration import run_real_image_dags_e2e as runner
from scripts.quality.repo_checks import load_yaml
from tests.support import check

SOURCE_ROOT = Path(__file__).resolve().parents[1]
KNOWN_DAGS = {
    "bronze_dag",
    "drift_live_check_dag",
    "drift_monitoring_dag",
    "features_base_dag",
    "features_m1_dag",
    "features_m2_dag",
    "features_m3_dag",
    "features_m4_dag",
    "features_m5_dag",
    "features_m6_dag",
    "gold_transform_dag",
    "ingestion_dag",
    "ingestion_retry_dag",
    "metadata_ingest_dag",
    "model_promotion_dag",
    "pipeline_dag",
    "research_dag",
    "serving_deploy_dag",
    "silver_transform_dag",
    "training_dag",
}


def _step(dag_id: str, state: str = "success", expected: str | None = None) -> dict[str, Any]:
    return {"dag_id": dag_id, "run_state": state, "expected": expected or state, "tasks": {"task": state}, "run_task_calls": 0, "errors": []}


def _report(dag_ids: set[str], *, forms: bool = True) -> dict[str, Any]:
    scenarios: list[dict[str, Any]] = [{"name": "every_dag", "ok": True, "detail": None, "seconds": 1.0, "steps": [_step(name) for name in sorted(dag_ids)]}]
    if forms:
        scenarios.append({"name": "trigger_forms_validate_in_airflow", "ok": True, "detail": None, "seconds": 0.0, "steps": []})
    return {
        "ok": True,
        "dags": sorted(dag_ids),
        "dag_ids": sorted(KNOWN_DAGS),
        "import_errors": {},
        "scenarios": scenarios,
        "versions": {},
        "substitutions": [],
    }


class Image:
    """The image the runner starts: the launcher's ``in_image`` answer for each run, in order. ``started`` counts the runs it was asked to make. Like the
    launcher it adds the host's record of the attempt (``real_dags_run``) after the image ran, and the image's run time and inputs unless the answer sets them.
    """

    def __init__(self, *answers: dict[str, Any], during: Callable[[], None] | None = None) -> None:
        self.answers = list(answers)
        self.started = 0
        self.during = during

    def __call__(self, platform: dict[str, Any], test_registry: str | None) -> dict[str, Any]:
        self.started += 1
        began, before = datetime.now(UTC), real_image_dags.inventory(runner.ROOT, real_image_dags.IMAGE_SOURCES)
        if self.during is not None:
            self.during()
        section = self.answers.pop(0)
        section.setdefault("at", datetime.now(UTC).isoformat())
        section.setdefault("inputs", before)
        run = {
            "attempt_id": f"synthetic-attempt-{self.started}",
            "started": began.isoformat(),
            "ended": datetime.now(UTC).isoformat(),
            "inputs": before,
            "inputs_after": real_image_dags.inventory(runner.ROOT, real_image_dags.IMAGE_SOURCES),
        }
        return {"real_dags": section, "dags": {"dag_ids": sorted(KNOWN_DAGS), "import_errors": {}}, "real_dags_run": run}


@pytest.fixture
def checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A checkout with the real DAG folder, where the runner writes its records."""
    shutil.copytree(SOURCE_ROOT / "dags", tmp_path / "dags")
    for relative in runner.SOURCES:
        if (SOURCE_ROOT / relative).is_file():
            (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(SOURCE_ROOT / relative, tmp_path / relative)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "git", lambda *arguments: "")
    monkeypatch.setattr(sys, "argv", ["run_real_image_dags_e2e"])
    return tmp_path


def _records(checkout: Path) -> list[dict[str, Any]]:
    return [json.loads(path.read_text()) for path in sorted((checkout / "data/e2e/real_image_dags").glob("*.json"))]


def _run(checkout: Path, monkeypatch: pytest.MonkeyPatch, report: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    monkeypatch.setattr(offline_checks, "in_image", Image(report))
    code = runner.main()
    records = _records(checkout)
    check(len(records) == 1, "a run writes exactly one record")
    return code, records[0]


def _hashes(checkout: Path) -> dict[str, dict[str, str]]:
    return {
        "sources": {name: runner.digest(checkout / name) for name in runner.SOURCES if (checkout / name).exists()},
        "dag_files": {path.name: runner.digest(path) for path in sorted((checkout / "dags").glob("*.py"))},
    }


def test_the_rig_builds_and_the_launch_environment_sets_every_required_setting() -> None:
    steps = importlib.import_module("orchestration.airflow_steps")
    rig = real_image_dags.make_rig()
    environment = real_image_dags.launch_environment(rig, "http://127.0.0.1:1", "/example/scratch")
    check(all(name in environment for name in steps.SETTINGS), "every required launch setting is set")
    check(environment[steps.HOST_JOB_CAP_SETTING] == str(real_image_dags.rig_modules().cases.JOB_MEMORY), "the host job cap is the job memory")
    check("HAI_LAUNCH_HOST_CAP_PARAMETER" not in environment, "the retired cap parameter name is gone")


def test_the_inventory_comes_from_the_dag_folder() -> None:
    found = runner.discover_dag_ids(SOURCE_ROOT / "dags")
    check(set(found) == KNOWN_DAGS, f"the DAG folder names {sorted(found)}")


def _form(dag_id: str) -> dict[str, ast.Call]:
    """The ``Param`` call of each parameter in the trigger form of a shipped DAG file. A parameter named by a module variable takes the variable's value, and
    one that depends on the schedule takes its development form, because the image names no environment and so runs no schedule."""
    tree = ast.parse((SOURCE_ROOT / "dags" / f"{dag_id}.py").read_text())
    assigned = {target.id: stmt.value for stmt in tree.body if isinstance(stmt, ast.Assign) for target in stmt.targets if isinstance(target, ast.Name)}

    def parameter(value: ast.expr) -> ast.expr:
        if isinstance(value, ast.Name) and value.id in assigned:
            value = assigned[value.id]
        return value.orelse if isinstance(value, ast.IfExp) else value

    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "params" and isinstance(node.value, ast.Dict):
            calls: dict[str, ast.Call] = {}
            for key, value in zip(node.value.keys, node.value.values, strict=True):
                chosen = parameter(value)
                if isinstance(key, ast.Constant) and isinstance(chosen, ast.Call):
                    calls[str(key.value)] = chosen
            return calls
    raise AssertionError(f"{dag_id} declares no trigger form")


def _literal(call: ast.Call, name: str) -> object:
    """The value of a keyword of a ``Param`` call, or None when it is absent or not a literal."""
    for item in call.keywords:
        if item.arg == name:
            try:
                return ast.literal_eval(item.value)
            except ValueError:
                return None
    return None


def _steps() -> list[tuple[str, real_image_dags.Step]]:
    return [(scenario.name, step) for scenario in real_image_dags.cases_catalog().values() for step in scenario.steps]


def test_the_rig_runs_and_forms_every_dag_file() -> None:
    files = {path.stem for path in (SOURCE_ROOT / "dags").glob("*.py")}
    stepped, formed = {step.dag_id for _, step in _steps()}, set(real_image_dags.trigger_forms())
    check(stepped == files, f"the scenarios run {sorted(stepped ^ files)} differently from the DAG folder")
    check(formed == files, f"the form cases name {sorted(formed ^ files)} differently from the DAG folder")


def test_each_form_case_names_exactly_the_values_its_form_requires() -> None:
    for dag_id, values in real_image_dags.trigger_forms().items():
        required = {name for name, call in _form(dag_id).items() if not call.args}
        check(set(values) == required, f"{dag_id}: the form case holds {sorted(values)}, the form requires {sorted(required)}")


def test_every_step_triggers_its_dag_with_values_its_form_accepts() -> None:
    for scenario, step in _steps():
        form, where = _form(step.dag_id), f"{scenario} {step.dag_id}"
        unknown = set(step.params) - set(form)
        required = {name for name, call in form.items() if not call.args}
        check(
            not unknown and required <= set(step.params),
            f"{where}: the trigger holds {sorted(step.params)}, the form takes {sorted(form)} and requires {sorted(required)}",
        )
        for name, value in step.params.items():
            pattern, enum = _literal(form[name], "pattern"), _literal(form[name], "enum")
            check(not isinstance(pattern, str) or not isinstance(value, str) or re.search(pattern, value), f"{where}: {name}={value!r} breaks {pattern}")
            check(not isinstance(enum, list) or value in enum, f"{where}: {name}={value!r} is not one of {enum}")


def test_the_drift_and_training_runs_chain_from_the_model_features_run() -> None:
    contract = importlib.import_module("orchestration.training_contract")
    steps = [step for scenario, step in _steps() if scenario == "every_host_dag_runs_end_to_end"]
    by_dag = {step.dag_id: step for step in steps}
    base = by_dag["features_base_dag"].params["run_id"]
    features = contract.model_features_run_id(str(base), "m1")
    check(by_dag["features_m1_dag"].params["run_id"] == features, "the m1 features run is not derived from the base run")
    drift = by_dag["drift_monitoring_dag"].params
    check(drift["features_run"] == features and drift["run_id"] == contract.model_drift_run_id(features) and drift["model"] == "m1", f"the drift run: {drift}")
    training = by_dag["training_dag"].params
    check(training["model"] == "m1" and training["run_id"] == contract.trained_run_id(str(base), "m1"), f"the training run: {training}")


def test_the_research_scenario_trains_the_three_models_on_one_base_and_researches_it() -> None:
    contract = importlib.import_module("orchestration.training_contract")
    steps = [step for scenario, step in _steps() if scenario == "research_dag_promotes_a_challenger_with_a_recorded_release"]
    trained = [step for step in steps if step.dag_id == "training_dag"]
    research = [step for step in steps if step.dag_id == "research_dag"]
    check([step.params["model"] for step in trained] == list(contract.PROMOTION_MODELS), "the scenario does not train m1, m2 and m4 in order")
    base = str(research[0].params["base_run"])
    check(
        [step.params["run_id"] for step in trained] == [contract.trained_run_id(base, model) for model in contract.PROMOTION_MODELS],
        "a training run is not the run of its model on the base run",
    )
    check(research[0].params["run_id"] == contract.research_run_id(base) and len(research) == 2, f"the research steps: {[step.params for step in research]}")
    check(research[1].starts == 0, "the second research run for the base starts a task again")


def _chain_steps(objects: dict[str, bytes], research: str) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    """The failures, the launch parameters and the steps of the promotion chain over the staged records, with the launches recorded."""
    chain = importlib.import_module("orchestration.promotion_chain")
    launched: list[tuple[str, dict[str, Any]]] = []

    def launch(step: str, run: str, parameters: dict[str, Any]) -> None:
        launched.append((step, parameters))

    failures = chain.run_types(research, ["HAI_1", "HAI_2"], launch=launch, read=objects.get)
    return failures, [parameters for _, parameters in launched], [step for step, _ in launched]


def test_the_staged_research_records_drive_the_chain_through_the_promotion() -> None:
    cases = real_image_dags.rig_modules().cases
    challenger = {"model": "m2", "version": "4", "verdict": "pass", "source_run": "mlrun4"}
    for decision, expected in (
        ("promote", ["serve_deploy", "canary_check", "promote", "release_online", "serve_deploy", "owner_notice"]),
        ("hold", ["serve_deploy", "canary_check", "owner_notice"]),
    ):
        objects = real_image_dags.research_objects(cases.SERVE_BASE, cases.SERVE_RESEARCH, {"HAI_1": challenger, "HAI_2": None}, decision)
        failures, parameters, steps = _chain_steps(objects, cases.SERVE_RESEARCH)
        check(not failures and steps == expected, f"{decision}: failures {failures}, steps {steps}")
        check(parameters[-1]["outcome"] == ("promoted" if decision == "promote" else "held"), f"{decision}: the notice says {parameters[-1]}")


def test_a_held_canary_is_refused_by_the_serve_preflight() -> None:
    preflight, cases = importlib.import_module("orchestration.ecs_preflight"), real_image_dags.rig_modules().cases
    challenger = {"model": "m2", "version": "4", "verdict": "pass", "source_run": "mlrun4"}
    held = real_image_dags.research_objects(cases.SERVE_BASE, cases.SERVE_RESEARCH, {"HAI_1": challenger}, "hold")
    promoted = real_image_dags.research_objects(cases.SERVE_BASE, cases.SERVE_RESEARCH, {"HAI_1": challenger}, "promote")
    request = {"hai_type": "HAI_1", "deploy_mode": "canary"}
    with pytest.raises(Exception, match="serving_canary_held"):
        preflight.serving_values(request, cases.SERVE_RESEARCH, 10, held.get, 24)
    values = preflight.serving_values(request, cases.SERVE_RESEARCH, 10, promoted.get, 24)
    check(json.loads(values["SERVING_REQUEST"])["canary_ttl_hours"] == 24, f"the canary request has no lifetime: {values}")


BOUND_MODULES = ("orchestration/promotion_chain.py", "orchestration/ecs_preflight.py", "orchestration/training_contract.py")


def test_the_inventory_binds_the_promotion_preflight_and_training_code_the_image_mounts() -> None:
    found = real_image_dags.inventory(SOURCE_ROOT, real_image_dags.IMAGE_SOURCES)["sources"]
    for name in BOUND_MODULES:
        check(name in real_image_dags.SOURCES and name in real_image_dags.IMAGE_SOURCES and name in found, f"the inventory does not bind {name}")
    compose = (SOURCE_ROOT / "services/orchestration/docker-compose.orchestration.yaml").read_text()
    check(
        "${HAI_ORCH_REPO}/orchestration:/opt/hai/repo/orchestration:ro" in compose,
        "the airflow-check service does not mount the orchestration folder the image reads",
    )
    check(all(name.startswith("orchestration/") for name in BOUND_MODULES), "a bound module is outside the mounted folder")


def test_the_drift_launch_is_checked_for_its_exact_model_and_run_inputs() -> None:
    drift = next(step for scenario, step in _steps() if step.dag_id == "drift_monitoring_dag" and step.starts == 1)
    check(drift.sent is not None and drift.after is not None, "the drift step checks neither its container inputs nor its code bindings")
    environment = {
        "baseline": {"FEATURE_SET": "m1"},
        "current": {"INPUT_RUN_ID": "features_ols_feed5eed"},
        "fetch": {"CODE_RUN_ID": "bronze_b0b0b0b0"},
    }
    right = real_image_dags.drift_inputs_are("m1", "features_ols_feed5eed", "bronze_b0b0b0b0")
    right(environment)
    for container, name, value in (
        ("baseline", "FEATURE_SET", "m2"),
        ("current", "INPUT_RUN_ID", "latest"),
        ("current", "INPUT_RUN_ID", "features_ols_0f0f0f0f"),
        ("fetch", "CODE_RUN_ID", "20261104T100000Z-feed5eed"),
    ):
        changed = {**environment, container: {**environment[container], name: value}}
        with pytest.raises(real_image_dags.Expectation, match=name):
            right(changed)
    with pytest.raises(real_image_dags.Expectation, match="INPUT_RUN_ID"):
        right({key: value for key, value in environment.items() if key != "current"})


def test_a_first_champion_has_its_own_real_image_scenario() -> None:
    first = real_image_dags.cases_catalog()["research_dag_promotes_a_first_champion_without_a_canary"]
    research = [step for step in first.steps if step.dag_id == "research_dag"]
    check(len(research) == 1 and research[0].starts == 4 and research[0].run_state == "success", f"the first-champion research step: {research}")
    check(research[0].check is not None, "the first-champion step does not check the chain")
    check(not [step for step in first.steps if step.dag_id == "serving_deploy_dag"], "the first-champion scenario deploys a canary or a stable serve by form")


def test_the_release_phase_runs_for_real_and_a_refusal_fails_closed() -> None:
    catalog = real_image_dags.cases_catalog()
    released = catalog["research_dag_promotes_a_challenger_with_a_recorded_release"]
    refused = catalog["research_dag_fails_after_the_notice_when_the_release_is_refused"]
    chain = [step for step in released.steps if step.dag_id == "research_dag"]
    check([step.starts for step in chain] == [7, 0] and all(step.run_state == "success" for step in chain), f"the release scenario: {chain}")
    check(chain[0].check is not None, "the release scenario does not check the seven tasks of the chain")
    research = [step for step in refused.steps if step.dag_id == "research_dag"]
    check(len(research) == 1 and research[0].starts == 6 and research[0].run_state == "failed", f"the refused-release step: {research}")
    check(research[0].plan == {"feast": 1} and research[0].reason == "HAI_1 release:", f"the release is not refused by name: {research[0]}")
    check(research[0].states == {"ready": "success", "research": "success", "chain": "failed", "gate": "failed"}, f"its states: {research[0].states}")
    check(not [item for item in real_image_dags.SUBSTITUTIONS if "release" in item and "recorder" in item], "a substitution still replaces the release")
    check(not hasattr(real_image_dags.Step, "release_recorded"), "a step can still replace the release with a recorder")


def test_a_training_run_with_a_base_starts_the_research_through_the_mapped_trigger() -> None:
    contract = importlib.import_module("orchestration.training_contract")
    scenario = real_image_dags.cases_catalog()["training_dag_starts_the_research_for_its_base"]
    (step,) = scenario.steps
    base = str(step.params["base_run"])
    check(step.dag_id == "training_dag" and step.params["model"] in contract.PROMOTION_MODELS and not step.skipped, f"the step: {step}")
    check(step.params["run_id"] == contract.trained_run_id(base, str(step.params["model"])), "the training run is not the model's run on the base")
    check(step.after is not None, "the scenario does not check the research run the trigger created")
    request = contract.research_requests(str(step.params["model"]), str(step.params["run_id"]), base, "primary", list(step.params["hai_types"]), "aws")
    check(
        len(request) == 1 and request[0]["conf"] == real_image_dags.research_conf(base, list(step.params["hai_types"])),
        f"the expected trigger conf differs: {request}",
    )
    check(request[0]["trigger_run_id"] == real_image_dags.research_trigger_run(str(step.params["run_id"])), "the expected trigger run id differs")
    check(request[0]["conf"]["run_id"] == contract.research_run_id(base), "the research run is not derived from the base")


def _serve_rig(prefix: str, run: str, request_hash: str) -> Any:
    claim = {"event": "claim", "run_id": run, "job": "serve", "bound": {"code_digest": "d", "request_hash": request_hash, "input_run": "latest"}}
    key = f"{prefix}/{run}/serve/claim.json"
    return SimpleNamespace(launcher=SimpleNamespace(store=SimpleNamespace(prefix=prefix)), records=SimpleNamespace(objects={key: json.dumps(claim).encode()}))


def test_the_serving_request_digest_is_the_hash_the_claim_binds_not_a_container_variable() -> None:
    body = json.dumps({"hai_type": "HAI_1", "deploy_mode": "stable"}, sort_keys=True, separators=(",", ":"))
    environment = {"serve": {"SERVING_REQUEST": body}}
    digest = hashlib.sha256(body.encode()).hexdigest()
    real_image_dags.serving_request_is({"hai_type": "HAI_1", "deploy_mode": "stable"})(environment)
    real_image_dags.serving_request_is_bound(_serve_rig("records", "run-1", digest), "run-1", environment)
    with pytest.raises(real_image_dags.Expectation, match="binds the request hash"):
        real_image_dags.serving_request_is_bound(_serve_rig("records", "run-1", "0" * 64), "run-1", environment)
    with pytest.raises(real_image_dags.Expectation, match="no claim record"):
        real_image_dags.serving_request_is_bound(_serve_rig("records", "run-2", digest), "run-1", environment)


def test_each_wrong_bucket_plant_hits_only_the_store_its_scenario_names() -> None:
    cases = importlib.import_module("scripts.orchestration.airflow_mode_cases")
    catalog = real_image_dags.cases_catalog()
    (records,) = catalog["planted_wrong_bucket_for_the_launch_records_fails_closed"].steps
    (binding,) = catalog["planted_wrong_bucket_for_the_code_run_binding_fails_closed"].steps
    claim, bound = (
        {"Key": f"{cases.PREFIX}/ingestion_ab12cd34/publisher/claim.json"},
        {"Key": "lakehouse/run_stage/ingestion_ab12cd34/inputs/code_run.json"},
    )
    record_plant, binding_plant = records.plant, binding.plant
    if record_plant is None or binding_plant is None:
        raise AssertionError("a wrong bucket scenario has no plant")
    check(record_plant.when(claim) and not record_plant.when(bound), "the launch record plant must reach the claim write and leave the code run binding alone")
    check(binding_plant.when(bound) and not binding_plant.when(claim), "the binding plant must reach the code run binding and leave the launch records alone")
    check(binding.starts == 0 and records.starts == 0, "neither refused write may start a task")


def test_the_retry_dag_runs_under_the_run_its_original_and_day_give() -> None:
    orchestration = importlib.import_module("orchestration.ingestion_schedule")
    (step,) = [step for _, step in _steps() if step.dag_id == "ingestion_retry_dag"]
    original, day = str(step.params["original_run"]), int(step.params["day"])
    check(real_image_dags.launch_run_id(step) == orchestration.retry_run_id(original, day), "the retry step is not checked under its own run")
    check(real_image_dags.launch_run_id(step) != original, "the retry step is checked under the run it retries")


def test_the_rig_leaves_its_dags_unpaused_so_the_mapped_research_trigger_finds_research_dag_running(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(os, "environ", dict(os.environ))
    real_image_dags.configure_airflow(str(tmp_path))
    check(os.environ["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "false", "the rig creates DAGs paused, so the fail_when_dag_is_paused trigger fails")


def test_the_airflow_check_service_sets_the_creation_default_the_check_needs_before_it_imports_airflow() -> None:
    # in_image_checks imports Airflow before the rig runs, and Airflow reads the default once at import, so the rig's own setting comes too late in the image.
    compose: Any = load_yaml((SOURCE_ROOT / "services/orchestration/docker-compose.orchestration.yaml").read_text())
    environment = compose["services"]["airflow-check"]["environment"]
    check(environment.get("AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION") == "false", f"the check image creates DAGs paused: {environment}")


def test_the_airflow_check_service_loads_the_plugins_folder_the_timetable_scenario_reads_through() -> None:
    # Airflow reads the plugins folder once, at import, and in_image_checks imports it before the rig runs: a custom timetable is then not registered.
    compose: Any = load_yaml((SOURCE_ROOT / "services/orchestration/docker-compose.orchestration.yaml").read_text())
    service = compose["services"]["airflow-check"]
    folder = service["environment"].get("AIRFLOW__CORE__PLUGINS_FOLDER")
    mounted = [str(volume).split(":")[1] for volume in service["volumes"] if ":" in str(volume)]
    check(folder == "/opt/hai/repo/airflow_plugins", f"the check image reads the plugins from {folder}")
    check(folder in mounted, f"the plugins folder {folder} is not mounted in the check image: {mounted}")


def test_a_complete_run_is_certified(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    code, record = _run(checkout, monkeypatch, _report(KNOWN_DAGS))
    check(code == 0 and record["ok"] is True, f"a complete passing run must be certified: {record['error']}")
    check(record["dag_ids_in_image"] == sorted(KNOWN_DAGS) and record["uncovered"] == [], "the record names the inventory it was checked against")


def test_the_unchanged_input_hashes_are_recorded(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    expected = _hashes(checkout)
    code, record = _run(checkout, monkeypatch, _report(KNOWN_DAGS))
    check(code == 0 and record["inputs_unchanged"] is True and record["input_changes"] == [], "a run whose inputs stayed the same says so")
    check(record["sources"] == expected["sources"] and record["dag_files"] == expected["dag_files"], "the record holds the hashes taken before the image ran")
    check(len(record["sources"]) == len(runner.SOURCES) and len(record["dag_files"]) == len(KNOWN_DAGS), "the record holds the complete inventory")


def _edit_source(checkout: Path) -> None:
    with (checkout / "orchestration/ecs_launch.py").open("a") as handle:
        handle.write('\nraise RuntimeError("changed after the image consumed it")\n')


def _edit_module(name: str) -> Callable[[Path], None]:
    def edit(checkout: Path) -> None:
        with (checkout / name).open("a") as handle:
            handle.write('\nraise RuntimeError("changed after the image consumed it")\n')

    return edit


def _edit_dag(checkout: Path) -> None:
    with (checkout / "dags/ingestion_dag.py").open("a") as handle:
        handle.write('\nraise RuntimeError("changed after the image consumed it")\n')


def _add_dag(checkout: Path) -> None:
    (checkout / "dags/added_dag.py").write_text('from airflow import DAG\n\nDAG(dag_id="ingestion_dag")\n')


def _remove_dag(checkout: Path) -> None:
    (checkout / "dags/training_dag.py").unlink()


def _remove_source(checkout: Path) -> None:
    (checkout / "orchestration/ecs_launch.py").unlink()


def _add_source(checkout: Path) -> None:
    (checkout / "orchestration/ecs_launch.py").write_text("created while the image ran\n")


@pytest.mark.parametrize(
    ("change", "named", "before"),
    [
        (_edit_source, "orchestration/ecs_launch.py", None),
        (_edit_module("orchestration/promotion_chain.py"), "orchestration/promotion_chain.py", None),
        (_edit_module("orchestration/ecs_preflight.py"), "orchestration/ecs_preflight.py", None),
        (_edit_module("orchestration/training_contract.py"), "orchestration/training_contract.py", None),
        (_edit_dag, "ingestion_dag.py", None),
        (_add_dag, "added_dag.py", None),
        (_remove_dag, "training_dag.py", None),
        (_remove_source, "orchestration/ecs_launch.py", None),
        (_add_source, "orchestration/ecs_launch.py", _remove_source),
    ],
    ids=["edit_source", "edit_promotion_chain", "edit_preflight", "edit_training_contract", "edit_dag", "add_dag", "remove_dag", "remove_source", "add_source"],
)
def test_an_input_that_changes_while_the_image_runs_is_refused(
    checkout: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: Callable[[Path], None],
    named: str,
    before: Callable[[Path], None] | None,
) -> None:
    if before is not None:
        before(checkout)
    start = _hashes(checkout)
    monkeypatch.setattr(offline_checks, "in_image", Image(_report(KNOWN_DAGS), during=lambda: change(checkout)))
    code = runner.main()
    records = _records(checkout)
    check(len(records) == 1, "a refused run still writes its one record")
    record = records[0]
    check(code == 1 and record["ok"] is False, "a run whose inputs changed during the image run is not certified")
    check(record["inputs_unchanged"] is False and named in json.dumps(record["input_changes"]), f"the changed input is named: {record['input_changes']}")
    check(named in json.dumps(record["error"]), "the refusal names the changed input")
    check(record["sources"] == start["sources"] and record["dag_files"] == start["dag_files"], "the record keeps the hashes the image started from")


def test_a_certified_record_keeps_the_attempt_it_is_bound_to(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    code, record = _run(checkout, monkeypatch, _report(KNOWN_DAGS))
    check(code == 0 and record["attempt_id"] == "synthetic-attempt-1" and record["binding_problems"] == [], f"the record names its attempt: {record}")


def _foreign_inputs() -> dict[str, dict[str, str]]:
    zeros = "0" * 64
    return {"sources": {name: zeros for name in runner.SOURCES}, "dag_files": {f"{name}.py": zeros for name in KNOWN_DAGS}}


@pytest.mark.parametrize(
    ("damage", "named"),
    [
        ("old", "outside this attempt"),
        ("future", "outside this attempt"),
        ("naive", "time zone aware"),
        ("no_time", "time zone aware"),
        ("foreign", "other inputs"),
        ("no_inputs", "other inputs"),
    ],
)
def test_a_result_that_fails_the_attempt_binding_is_not_certified(checkout: Path, monkeypatch: pytest.MonkeyPatch, damage: str, named: str) -> None:
    report = _report(KNOWN_DAGS)
    if damage == "old":
        report["at"] = "2000-01-01T00:00:00+00:00"
    elif damage == "future":
        report["at"] = "2099-01-01T00:00:00+00:00"
    elif damage == "naive":
        report["at"] = "2026-11-11T10:00:00"
    elif damage == "no_time":
        report["at"] = None
    elif damage == "foreign":
        report["inputs"] = _foreign_inputs()
    else:
        report["inputs"] = None
    code, record = _run(checkout, monkeypatch, report)
    check(code == 1 and record["ok"] is False, f"a result that fails the binding ({damage}) must not be certified")
    check(any(named in item for item in record["binding_problems"]), f"the record keeps the binding problem: {record['binding_problems']}")
    check(named in json.dumps(record["error"]) and record["attempt_id"] == "synthetic-attempt-1", "the refusal names the problem and the record the attempt")


def test_a_result_without_the_host_record_of_its_attempt_is_not_certified(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    image = Image(_report(KNOWN_DAGS))
    monkeypatch.setattr(
        offline_checks, "in_image", lambda platform, registry: {key: value for key, value in image(platform, registry).items() if key != "real_dags_run"}
    )
    code = runner.main()
    record = _records(checkout)[0]
    check(code == 1 and record["ok"] is False and record["attempt_id"] is None, "a result with no attempt identity is not certified")
    check(any("attempt identity" in item for item in record["binding_problems"]), f"the missing identity is named: {record['binding_problems']}")


def test_there_is_no_saved_report_mode(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    check(not hasattr(runner, "read_report"), "the runner has a reader of reports made by another process")
    path = checkout / "old_report.out"
    path.write_text(real_image_dags.REPORT_PREFIX + json.dumps(_report(KNOWN_DAGS)) + "\n")
    monkeypatch.setattr(sys, "argv", ["run_real_image_dags_e2e", "--report", str(path)])
    monkeypatch.setattr(offline_checks, "in_image", Image(_report(KNOWN_DAGS)))
    with pytest.raises(SystemExit) as refused:
        runner.main()
    check(refused.value.code == 2, "the option for a saved report is not accepted")
    check(not _records(checkout), "a refused command line writes no record")


def test_every_run_starts_the_image_and_judges_only_its_own_result(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    image = Image(_report(KNOWN_DAGS - {"training_dag"}), _report(KNOWN_DAGS))
    monkeypatch.setattr(offline_checks, "in_image", image)
    check(runner.main() == 1, "the first image run lacked a DAG")
    check(runner.main() == 0, "the second image run was judged by its own result")
    check(image.started == 2, "each run starts the image")
    check(sorted(item["ok"] for item in _records(checkout)) == [False, True], "each run keeps its own verdict")


def test_a_form_only_report_is_refused(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    report = _report(set())
    report.update(dags=[], dag_ids=[])
    code, record = _run(checkout, monkeypatch, report)
    check(code == 1 and record["ok"] is False, "a result with no executed DAG must not be certified")
    check(record["uncovered"] == sorted(KNOWN_DAGS), "every DAG of the folder is named as not run")


def test_a_partial_run_is_refused(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    code, record = _run(checkout, monkeypatch, _report(KNOWN_DAGS - {"features_base_dag"}))
    check(code == 1 and record["uncovered"] == ["features_base_dag"], f"a missing DAG must be refused: {record['uncovered']}")


def test_a_dag_named_but_not_executed_is_refused(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    report = _report(KNOWN_DAGS)
    report["scenarios"][0]["steps"] = [step for step in report["scenarios"][0]["steps"] if step["dag_id"] != "training_dag"]
    report["scenarios"][0]["steps"].append({"dag_id": "training_dag", "error": "Expectation: broke"})
    code, record = _run(checkout, monkeypatch, report)
    check(code == 1 and "training_dag" in record["uncovered"], "a step that did not finish does not count as executed")


@pytest.mark.parametrize("state", ["running", "queued", "skipped", "scheduled", "deferred", "up_for_retry", "upstream_failed", "removed"])
def test_a_state_that_is_not_terminal_does_not_count(checkout: Path, monkeypatch: pytest.MonkeyPatch, state: str) -> None:
    report = _report(KNOWN_DAGS)
    report["scenarios"][0]["steps"] = [
        _step(step["dag_id"], state if step["dag_id"] == "training_dag" else "success") for step in report["scenarios"][0]["steps"]
    ]
    code, record = _run(checkout, monkeypatch, report)
    check(code == 1 and record["uncovered"] == ["training_dag"], f"a DAG run in state {state} is not a finished run: {record['uncovered']}")


def test_a_run_state_other_than_the_one_expected_does_not_count(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    report = _report(KNOWN_DAGS)
    report["scenarios"][0]["steps"] = [
        _step(step["dag_id"], "failed" if step["dag_id"] == "training_dag" else "success", "success") for step in report["scenarios"][0]["steps"]
    ]
    code, record = _run(checkout, monkeypatch, report)
    check(code == 1 and record["uncovered"] == ["training_dag"], "a DAG run that ended in a state its scenario did not expect is not coverage")


@pytest.mark.parametrize(
    ("state", "expected"),
    [("failed", "success"), ("success", "failed"), ("running", "running"), ("queued", "queued"), ("skipped", "skipped")],
)
def test_a_bad_extra_step_of_a_covered_dag_fails_the_run(checkout: Path, monkeypatch: pytest.MonkeyPatch, state: str, expected: str) -> None:
    report = _report(KNOWN_DAGS)
    report["scenarios"][0]["steps"].append(_step("training_dag", state, expected))
    code, record = _run(checkout, monkeypatch, report)
    check(record["uncovered"] == [], "the first successful step still covers the DAG")
    check(code == 1 and record["ok"] is False, f"an extra {state} step expected {expected} must fail the run even though another step covers the DAG")
    check("training_dag" in json.dumps(record["error"]) and record["step_problems"], f"the bad step is named: {record['error']}")


def test_a_scenario_with_no_dag_step_stays_valid() -> None:
    check(real_image_dags.step_problems([{"name": "forms", "ok": True, "steps": []}, {"name": "bridge", "ok": True}]) == [], "no step is no problem")


def test_a_step_that_is_not_a_finished_expected_run_is_a_problem() -> None:
    bad = [_step("a_dag", "failed", "success"), _step("b_dag", "running"), {"dag_id": "c_dag", "error": "broke"}, "not a step"]
    problems = real_image_dags.step_problems([{"name": "s", "ok": True, "steps": bad}, {"name": "t", "ok": True, "steps": "not a list"}])
    check(len(problems) == 5, f"each bad step and a malformed step list is a problem: {problems}")
    check(
        real_image_dags.step_problems([{"name": "s", "ok": True, "steps": [_step("a_dag"), _step("b_dag", "failed")]}]) == [],
        "success and an expected failure pass",
    )


def test_a_failed_run_that_the_scenario_expects_counts(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    report = _report(KNOWN_DAGS)
    report["scenarios"][0]["steps"] = [
        _step(step["dag_id"], "failed" if step["dag_id"] == "ingestion_dag" else "success", None) for step in report["scenarios"][0]["steps"]
    ]
    code, record = _run(checkout, monkeypatch, report)
    check(code == 0 and record["uncovered"] == [], "a refusal scenario finishes in failed, which is a finished run")


def test_a_failed_scenario_is_refused(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    report = _report(KNOWN_DAGS)
    report["scenarios"][0].update(ok=False, detail="synthetic refusal")
    code, record = _run(checkout, monkeypatch, report)
    check(code == 1 and record["ok"] is False, "a failed scenario must not be certified")
    check(sorted(record["uncovered"]) == sorted(KNOWN_DAGS), "the DAGs of a failed scenario are not executed passes")


@pytest.mark.parametrize("verdict", [False, None, "true", 1])
def test_the_report_must_declare_a_pass(checkout: Path, monkeypatch: pytest.MonkeyPatch, verdict: object) -> None:
    report = _report(KNOWN_DAGS)
    report["ok"] = verdict
    code, record = _run(checkout, monkeypatch, report)
    check(code == 1 and record["ok"] is False, f"a report whose verdict is {verdict!r} is not a pass")


def test_an_image_with_another_inventory_is_refused(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    image = Image(_report(KNOWN_DAGS))
    monkeypatch.setattr(
        offline_checks, "in_image", lambda platform, registry: {**image(platform, registry), "dags": {"dag_ids": [*sorted(KNOWN_DAGS), "synthetic_dag"]}}
    )
    code = runner.main()
    check(code == 1 and "synthetic_dag" in json.dumps(_records(checkout)[0]["error"]), "an image that holds a DAG the folder lacks is a refusal")


def test_a_crashed_image_run_is_a_failed_record(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    code, record = _run(checkout, monkeypatch, {"ok": False, "error": "synthetic driver crash"})
    check(code == 1 and "synthetic driver crash" in json.dumps(record["error"]), "the crash cause is recorded")


def test_two_runs_in_one_second_keep_both_records(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "now", lambda: datetime(2026, 11, 11, 22, 0, 0, tzinfo=UTC))
    monkeypatch.setattr(offline_checks, "in_image", Image(_report(KNOWN_DAGS - {"training_dag"}), _report(KNOWN_DAGS)))
    check(runner.main() == 1 and runner.main() == 0, "the first run fails and the second passes")
    records = _records(checkout)
    check(len(records) == 2 and [item["ok"] for item in records].count(False) == 1, "the failed result is still on disk after the later pass")


def test_a_record_is_never_overwritten(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "now", lambda: datetime(2026, 11, 11, 22, 0, 0, tzinfo=UTC))
    names = iter(["aaaaaaaa", "aaaaaaaa", "bbbbbbbb"])
    monkeypatch.setattr(runner.secrets, "token_hex", lambda size: next(names))
    folder = checkout / "data/e2e/real_image_dags"
    folder.mkdir(parents=True)
    existing = folder / "20261111T220000Z-aaaaaaaa.json"
    existing.write_text("earlier result")
    monkeypatch.setattr(offline_checks, "in_image", Image(_report(KNOWN_DAGS)))
    check(runner.main() == 0, "the run passes")
    check(existing.read_text() == "earlier result", "an existing record keeps its content")
    check((folder / "20261111T220000Z-bbbbbbbb.json").exists(), "the run took the next free name")


def test_only_finished_runs_in_passing_scenarios_are_counted() -> None:
    scenarios = [
        {
            "ok": True,
            "steps": [_step("a_dag"), _step("b_dag", "running"), _step("c_dag", "failed", "failed"), _step("d_dag", "failed", "success"), {"dag_id": "e_dag"}],
        },
        {"ok": False, "steps": [_step("f_dag")]},
    ]
    check(real_image_dags.finished_dags(scenarios) == ["a_dag", "c_dag"], f"finished runs: {real_image_dags.finished_dags(scenarios)}")


def test_the_driver_exits_non_zero_when_a_scenario_fails_or_none_ran() -> None:
    check(real_image_dags.exit_code({"ok": True, "scenarios": [{"ok": True}]}) == 0, "a passing run exits 0")
    check(real_image_dags.exit_code({"ok": False, "scenarios": [{"ok": False}]}) == 1, "a failing run exits 1")
    check(real_image_dags.exit_code({"ok": True, "scenarios": []}) == 1, "a run of no scenario exits 1")


def _marked_passed(source: str) -> list[str]:
    """Where a real-image DAG scenario would mark a task passed without running it: a ``mark_success_pattern`` argument, or a ``skip`` setting of a step."""
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.keyword) and node.arg in {"mark_success_pattern", "skip"}:
            found.append(f"line {node.value.lineno}: {node.arg}=")
        if isinstance(node, ast.Attribute) and node.attr in {"mark_success_pattern", "skip"}:
            found.append(f"line {node.lineno}: .{node.attr}")
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "skip":
            found.append(f"line {node.lineno}: a skip field")
    return found


def test_no_scenario_marks_a_task_passed_without_running_it() -> None:
    source = (SOURCE_ROOT / "scripts/orchestration/real_image_dags.py").read_text()
    check(_marked_passed(source) == [], f"a scenario marks tasks passed without running them: {_marked_passed(source)}")
    check("skip" not in {item.name for item in real_image_dags.Step.__dataclass_fields__.values()}, "a step can still skip tasks")
    for broken in (
        source + "\nrun = dag.test(run_conf={}, mark_success_pattern=step.skip)\n",
        source.replace("    api: Callable[[Any], Any] | None = None\n", "    skip: str | None = None\n", 1),
        source + "\nStep('pipeline_dag', {}, 0, skip='run_.*')\n",
    ):
        check(_marked_passed(broken) != [], "the guard missed a step that marks tasks passed without running them")


def test_the_first_dags_run_every_task_in_the_image_against_the_api_stand_in() -> None:
    catalog = real_image_dags.cases_catalog()
    first = [step for name, scenario in catalog.items() if "first_dag" in name or name == "every_host_dag_runs_end_to_end" for step in scenario.steps]
    pipeline = [step for step in first if step.dag_id == "pipeline_dag"]
    check(
        len(pipeline) == 39 and all(step.api is not None and step.api_after is not None for step in pipeline), "a first-DAG step runs without the API stand-in"
    )
    check(all(step.starts == 0 for step in pipeline), "the first DAG launches a host job itself")
    bronze = [step for step in first if step.dag_id == "bronze_dag"]
    check(len(bronze) == 1 and bronze[0].starts == 2 and bronze[0].check is not None, "the bronze DAG's two jobs are not run and checked")
    names = {name for name in catalog if name.startswith("the_first_dag_")}
    stages = ("ingestion", "bronze", "silver", "gold", "features")
    expected = {f"the_first_dag_starts_from_{stage}" for stage in stages[1:]} | {f"the_first_dag_stops_when_the_{stage}_run_fails" for stage in stages}
    check(expected <= names, f"missing first-DAG scenarios: {sorted(expected - names)}")
    check("scripts/orchestration/airflow_api_fake.py" in real_image_dags.SOURCES, "the API stand-in is not among the inputs the verdict binds to")
    check(
        any("airflow_api_fake.py" in text and "No task is marked passed without running" in text for text in real_image_dags.SUBSTITUTIONS),
        "the report does not name the API stand-in as a substitution",
    )


def test_every_step_runs_its_dag_under_a_run_id_of_the_run_name_the_dag_takes() -> None:
    """The forms of the layer DAGs build their run ID pattern from the contract, which the static form check above cannot read: a step that names another
    run name (a silver run for the catalog DAG) is refused by Airflow only in the image."""
    contract = importlib.import_module("orchestration.training_contract")
    names = {
        "ingestion_dag": "ingestion",
        "bronze_dag": "bronze",
        "silver_transform_dag": "silver",
        "gold_transform_dag": "gold",
        "features_base_dag": "features",
        "metadata_ingest_dag": "metadata",
        contract.PIPELINE_DAG: contract.PIPELINE_RUN,
    }
    seen = set()
    for scenario, step in _steps():
        if step.dag_id in names and "run_id" in step.params:
            seen.add(step.dag_id)
            run = str(step.params["run_id"])
            check(contract.run_id_parts(run).name == names[step.dag_id], f"{scenario}: {step.dag_id} runs under {run}, not a {names[step.dag_id]} run")
    check(seen == set(names), f"no step runs {sorted(set(names) - seen)}")
