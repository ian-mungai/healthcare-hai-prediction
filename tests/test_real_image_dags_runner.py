"""The real-image DAG runner starts the image itself on every run and judges only what that run returns, against the DAG folder's own inventory."""

from __future__ import annotations

import ast
import importlib
import json
import re
import shutil
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from scripts.orchestration import offline_checks, real_image_dags
from scripts.orchestration import run_real_image_dags_e2e as runner
from tests.support import check

SOURCE_ROOT = Path(__file__).resolve().parents[1]
KNOWN_DAGS = {
    "bronze_chain_dag",
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
    "metadata_ingest_dag",
    "model_promotion_dag",
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
    """The ``Param`` call of each parameter in the trigger form of a shipped DAG file."""
    for node in ast.walk(ast.parse((SOURCE_ROOT / "dags" / f"{dag_id}.py").read_text())):
        if isinstance(node, ast.keyword) and node.arg == "params" and isinstance(node.value, ast.Dict):
            return {
                str(key.value): value
                for key, value in zip(node.value.keys, node.value.values, strict=True)
                if isinstance(key, ast.Constant) and isinstance(value, ast.Call)
            }
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
    steps = [step for scenario, step in _steps() if scenario == "research_dag_promotes_a_challenger"]
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


def _chain_steps(objects: dict[str, bytes], base: str, research: str) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    """The failures, the launch parameters and the steps of the promotion chain over the staged records, with the launches and the release recorded."""
    chain = importlib.import_module("orchestration.promotion_chain")
    launched: list[tuple[str, dict[str, Any]]] = []

    def launch(step: str, run: str, parameters: dict[str, Any]) -> None:
        launched.append((step, parameters))

    failures = chain.run_types(base, research, ["HAI_1", "HAI_2"], launch=launch, read=objects.get, release=lambda model, features_run, run: None)
    return failures, [parameters for _, parameters in launched], [step for step, _ in launched]


def test_the_staged_research_records_drive_the_chain_through_the_promotion() -> None:
    cases = real_image_dags.rig_modules().cases
    challenger = {"model": "m2", "version": "4", "verdict": "pass", "source_run": "mlrun4"}
    for decision, expected in (
        ("promote", ["serve_deploy", "canary_check", "promote", "serve_deploy", "owner_notice"]),
        ("hold", ["serve_deploy", "canary_check", "owner_notice"]),
    ):
        objects = real_image_dags.research_objects(cases.SERVE_BASE, cases.SERVE_RESEARCH, {"HAI_1": challenger, "HAI_2": None}, decision)
        failures, parameters, steps = _chain_steps(objects, cases.SERVE_BASE, cases.SERVE_RESEARCH)
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


def test_the_rig_names_none_of_the_retired_inputs() -> None:
    text = Path(real_image_dags.__file__).read_text()
    for retired in ("serving_request.json", "promotion_approval", "SERVING_REQUEST ="):
        check(retired not in text, f"the rig still names {retired}")


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
        (_edit_dag, "ingestion_dag.py", None),
        (_add_dag, "added_dag.py", None),
        (_remove_dag, "training_dag.py", None),
        (_remove_source, "orchestration/ecs_launch.py", None),
        (_add_source, "orchestration/ecs_launch.py", _remove_source),
    ],
    ids=["edit_source", "edit_dag", "add_dag", "remove_dag", "remove_source", "add_source"],
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
