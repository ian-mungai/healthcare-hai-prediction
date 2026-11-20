"""The model display names: one source of truth beside the model lists, and every DAG and message that names a model shows it.

The DAG files run under the declared Airflow SDK stand-ins of ``scripts.training.lane_b_dag_cases``, so the cases read what each file really declares.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import logging
import re
import runpy
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from orchestration import ecs_launch, ecs_preflight, graph, training_contract
from orchestration.interface import SubmissionRequest
from scripts.training import lane_b_dag_cases as stand_ins
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
IN_IMAGE = ROOT / "scripts/orchestration/in_image_checks.py"
RUN = "20261117T000000Z-aaaaaaaa"
LIVE_DAG = "drift_live_check_dag"
OWNER_NAMES = {"m1": "OLS baseline", "m2": "Elastic Net", "m3": "Mixed effects", "m4": "Gradient boosting", "m5": "Spatial", "m6": "Fixed effects"}


class StandInTrigger(stand_ins.Node):
    """The operator of the standard provider, as far as a model DAG file uses it."""

    def __init__(self, **settings: Any) -> None:
        super().__init__(settings["task_id"], "all_success")
        self.settings = settings
        if stand_ins.Dag.current is not None:
            stand_ins.Dag.current.task_dict[self.task_id] = self

    @staticmethod
    def partial(**settings: Any) -> Any:
        return stand_ins.TriggerDagRunOperator.partial(**settings)


def recording_task(recorded: dict[str, Callable[..., Any]]) -> Callable[..., Any]:
    """The stand-in ``task`` decorator that also keeps each decorated function, so that a case can run the task body."""

    def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        decorate = stand_ins.task(**settings)

        def keep(function: Callable[..., Any]) -> Callable[..., Any]:
            recorded[settings["task_id"]] = function
            return decorate(function)

        return keep

    return task


def run_dag_file(
    name: str, folder: Path = ROOT / "dags", recorded: dict[str, Callable[..., Any]] | None = None, context: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Run ``<folder>/<name>.py`` with the SDK stand-ins and return its namespace. ``recorded`` keeps the task bodies, ``context`` is what tasks read."""
    sdk = ModuleType("airflow.sdk")
    for key, value in (
        ("DAG", stand_ins.Dag),
        ("Param", stand_ins.Param),
        ("TriggerRule", stand_ins.Rule),
        ("Asset", str),
        ("task", stand_ins.task if recorded is None else recording_task(recorded)),
        ("get_current_context", lambda: context or {}),
    ):
        setattr(sdk, key, value)
    trigger = ModuleType("airflow.providers.standard.operators.trigger_dagrun")
    trigger.__dict__["TriggerDagRunOperator"] = StandInTrigger
    providers = {part: ModuleType(part) for part in ("airflow.providers", "airflow.providers.standard", "airflow.providers.standard.operators")}
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, {"airflow": ModuleType("airflow"), "airflow.sdk": sdk, **providers, trigger.__name__: trigger}):
            return runpy.run_path(str(folder / f"{name}.py"))
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous


def as_airflow_dag(namespace: dict[str, Any]) -> SimpleNamespace:
    """Read a stand-in DAG the way the graph rules read an Airflow DAG: its ``dag_id``, names, docs, tags and ``params``."""
    settings = namespace["dag"].settings
    params = {key: SimpleNamespace(schema=param.settings) for key, param in settings["params"].items()}
    tasks = {
        task_id: SimpleNamespace(
            task_id=task_id,
            task_display_name=getattr(node, "settings", {}).get("task_display_name") or task_id,
            doc_md=getattr(node, "settings", {}).get("doc_md"),
        )
        for task_id, node in namespace["dag"].task_dict.items()
    }
    return SimpleNamespace(
        task_dict=tasks,
        dag_id=settings["dag_id"],
        dag_display_name=settings.get("dag_display_name", settings["dag_id"]),
        description=settings.get("description"),
        doc_md=settings.get("doc_md"),
        tags=set(settings.get("tags", ())),
        params=SimpleNamespace(get_param=params.__getitem__),
    )


@pytest.fixture(scope="module")
def loaded() -> Iterator[dict[str, dict[str, Any]]]:
    yield {name: run_dag_file(name) for name in {*graph.DOC_MODELS, LIVE_DAG}}


def test_names_are_the_owner_decision_and_cover_every_model() -> None:
    check(dict(training_contract.MODEL_LABELS) == OWNER_NAMES, "the names are not the owner's decision")
    check(tuple(training_contract.MODEL_LABELS) == training_contract.MODELS, "the names do not cover exactly the models")


@pytest.mark.parametrize(
    "names",
    [
        {key: value for key, value in OWNER_NAMES.items() if key != "m3"},
        {**OWNER_NAMES, "m7": "Another"},
        {**OWNER_NAMES, "m2": ""},
        {**OWNER_NAMES, "m2": " "},
        {**OWNER_NAMES, "m2": "OLS baseline"},
        {**OWNER_NAMES, "m2": "Elastic  Net"},
    ],
    ids=["missing key", "extra key", "empty name", "blank name", "duplicate name", "double space"],
)
def test_the_name_check_refuses_a_mismatch(names: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="model display names"):
        training_contract.check_model_names(names, training_contract.MODELS)


def test_the_name_check_accepts_the_shipped_names() -> None:
    training_contract.check_model_names(OWNER_NAMES, training_contract.MODELS)


def test_labels_keep_the_key_beside_the_name() -> None:
    check(training_contract.model_title("m2") == "m2 Elastic Net", "the title does not join key and name")
    check(training_contract.model_label("m2") == "m2 (Elastic Net)", "the label does not put the name in brackets")
    check(training_contract.features_dag_display_name("m4") == "Features: m4 Gradient boosting", "the model DAG name is wrong")
    with pytest.raises(ValueError, match="m7"):
        training_contract.model_label("m7")


def test_every_model_dag_shows_its_name_and_keeps_its_ids(loaded: dict[str, dict[str, Any]]) -> None:
    for dag_id, model in training_contract.FEATURE_DAGS.items():
        settings = loaded[dag_id]["dag"].settings
        check(settings["dag_id"] == dag_id, "a DAG ID changed")
        check(settings["dag_display_name"] == f"Features: {model} {OWNER_NAMES[model]}", "a model DAG shows another display name")
        check(training_contract.model_label(model) in settings["description"], "a model DAG description lacks the label")
        check(training_contract.model_label(model) in settings["doc_md"], "a model DAG doc lacks the label")
        check(training_contract.model_title(model) in settings["tags"], "a model DAG lacks its tag for the name")
        check({"orchestration", "features"} <= set(settings["tags"]), "the tags it had were lost")
        check(set(loaded[dag_id]["dag"].task_dict) == {"preflight", "job", "evidence", "gate", "trigger_drift"}, "the tasks of a model DAG changed")


@pytest.mark.parametrize("dag_id", ["training_dag", "drift_monitoring_dag", LIVE_DAG])
def test_the_model_choice_of_a_form_shows_every_name(loaded: dict[str, dict[str, Any]], dag_id: str) -> None:
    schema = loaded[dag_id]["dag"].settings["params"]["model"].settings
    check(schema["enum"] == list(training_contract.MODELS), "the model choice lost or changed its values")
    check(
        schema["values_display"] == {model: training_contract.model_title(model) for model in training_contract.MODELS},
        "the model choice does not show each name",
    )
    check(all(training_contract.model_label(model) in schema["description"] for model in training_contract.MODELS), "the model choice help lacks a label")


@pytest.mark.parametrize(
    ("dag_id", "models"),
    [
        ("training_dag", training_contract.MODELS),
        ("research_dag", ("m1", "m2", "m4")),
        ("drift_monitoring_dag", training_contract.MODELS),
        (LIVE_DAG, training_contract.MODELS),
        ("features_base_dag", training_contract.MODELS),
    ],
)
def test_the_doc_of_a_dag_that_names_models_shows_their_names(loaded: dict[str, dict[str, Any]], dag_id: str, models: tuple[str, ...]) -> None:
    doc = loaded[dag_id]["dag"].settings["doc_md"]
    check(all(training_contract.model_label(model) in doc for model in models), "a DAG doc lacks the label of a model it names")


def test_the_drift_log_line_names_the_model_with_its_name(loaded: dict[str, dict[str, Any]]) -> None:
    line = loaded["drift_monitoring_dag"]["skip_line"]("m2", "HAI_1", "no_features")
    check(line == "no training of m2 (Elastic Net) for HAI_1: no_features", "the drift log line is wrong")


def test_the_graph_rules_accept_every_shipped_dag(loaded: dict[str, dict[str, Any]]) -> None:
    for dag_id in graph.DOC_MODELS:
        check(graph.display_name_violations(as_airflow_dag(loaded[dag_id])) == [], dag_id)


def test_the_graph_rules_refuse_a_missing_or_mismatched_name(loaded: dict[str, dict[str, Any]]) -> None:
    shipped = as_airflow_dag(loaded["features_m2_dag"])

    def refused(**changes: Any) -> set[str]:
        return {violation.rule for violation in graph.display_name_violations(SimpleNamespace(**{**vars(shipped), **changes}))}

    check(refused(dag_display_name="features_m2_dag") == {"display_name"}, "a DAG that shows its ID as its name was accepted")
    check(refused(dag_display_name="Features: m3 Mixed effects") == {"display_name"}, "a DAG that shows another model's name was accepted")
    check(refused(description=None) == {"display_name"}, "a DAG with no description was accepted")
    check(refused(doc_md="") == {"display_name"}, "a DAG with an empty doc was accepted")
    check(refused(tags={"orchestration", "features"}) == {"display_name"}, "a DAG with no tag for the name was accepted")
    training = as_airflow_dag(loaded["training_dag"])
    without = SimpleNamespace(
        **{**vars(training), "params": SimpleNamespace(get_param=lambda key: SimpleNamespace(schema={"enum": list(training_contract.MODELS)}))}
    )
    check({violation.rule for violation in graph.display_name_violations(without)} == {"display_name"}, "a model choice with no display values was accepted")


def test_the_base_dag_shows_the_name_of_each_model_it_starts(loaded: dict[str, dict[str, Any]]) -> None:
    tasks = loaded["features_base_dag"]["dag"].task_dict
    check(
        set(tasks) == {"preflight", "job", "evidence", "gate", *(f"trigger_{model}" for model in training_contract.MODELS)}, "a task ID of the base DAG changed"
    )
    for model in training_contract.MODELS:
        settings = tasks[f"trigger_{model}"].settings
        check(settings["task_display_name"] == f"Trigger {training_contract.model_title(model)}", f"the trigger of {model} shows no name")
        check(training_contract.model_label(model) in settings["doc_md"], f"the trigger of {model} does not name its model in its doc")
        check(settings["trigger_dag_id"] == f"features_{model}_dag", "a trigger starts another DAG")


def test_the_graph_rules_refuse_a_trigger_task_without_its_model_name(loaded: dict[str, dict[str, Any]]) -> None:
    shipped = as_airflow_dag(loaded["features_base_dag"])

    def refused(task_id: str, **changes: Any) -> set[str]:
        tasks = {**shipped.task_dict, task_id: SimpleNamespace(**{**vars(shipped.task_dict[task_id]), **changes})}
        return {violation.rule for violation in graph.display_name_violations(SimpleNamespace(**{**vars(shipped), "task_dict": tasks}))}

    check(refused("trigger_m2", task_display_name="trigger_m2") == {"display_name"}, "a trigger that shows its task ID was accepted")
    check(refused("trigger_m2", task_display_name="Trigger m3 Mixed effects") == {"display_name"}, "a trigger that shows another model's name was accepted")
    check(refused("trigger_m2", doc_md=None) == {"display_name"}, "a trigger with no doc was accepted")
    gone = {key: value for key, value in shipped.task_dict.items() if key != "trigger_m2"}
    check(
        {v.rule for v in graph.display_name_violations(SimpleNamespace(**{**vars(shipped), "task_dict": gone}))} == {"display_name"},
        "a missing trigger was accepted",
    )


def execute_module(name: str, path: Path) -> ModuleType:
    """Run the file at ``path`` from its first line as a new module that ``sys.modules`` does not hold."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"{path} cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {name: module}):  # a dataclass in the file reads its module from sys.modules while the file runs
        spec.loader.exec_module(module)
    return module


def fresh_training_contract(folder: Path, old: str = "", new: str = "") -> ModuleType:
    """Run ``orchestration/training_contract.py`` from its first line as a new module, with ``old`` replaced by ``new`` in its source."""
    source = (ROOT / "orchestration/training_contract.py").read_text(encoding="utf-8")
    check(old in source, f"the module no longer holds {old!r}")
    path = folder / "training_contract_fresh.py"
    path.write_text(source.replace(old, new, 1), encoding="utf-8")
    return execute_module("training_contract_fresh", path)


def test_a_fresh_import_of_the_shipped_module_passes_its_own_name_check(tmp_path: Path) -> None:
    check(fresh_training_contract(tmp_path).MODEL_LABELS == OWNER_NAMES, "the fresh module changed the names")


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ('"m2": "Elastic Net"', '"m2": "OLS baseline"'),
        ('"m6": "Fixed effects"', '"m6": ""'),
        ('"m5": "Spatial", ', ""),
        ('"m2": "Elastic Net"', '"m2": "Elastic  Net"'),
    ],
    ids=["duplicate name", "empty name", "missing model", "double space"],
)
def test_importing_the_module_with_invalid_names_raises(tmp_path: Path, old: str, new: str) -> None:
    with pytest.raises(ValueError, match="model display names"):
        fresh_training_contract(tmp_path, old, new)


def drift_namespace(context: dict[str, Any] | None = None, recorded: dict[str, Callable[..., Any]] | None = None) -> dict[str, Any]:
    return run_dag_file("drift_monitoring_dag", recorded=recorded, context=context)


def test_the_drift_decision_logs_the_name_of_each_model_it_skips(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    params = {"run_id": RUN, "mlflow": "aws", "model": "m2", "features_run": RUN}
    recorded: dict[str, Callable[..., Any]] = {}
    drift_namespace({"params": params}, recorded)
    record = {"hai_type": "HAI_1", "triggered": False, "skip_reason": "no_features"}
    gate = training_contract.decide_drift_gate("m2", {"monitored": ["HAI_1"], "unmonitored": {}}, {"HAI_1": record})
    marker = {"model": "m2", "gate": gate, "monitored": ["HAI_1"], "unmonitored": {}}
    decide = recorded["decide"]
    monkeypatch.setenv("HAI_LAUNCH_BUCKET", "synthetic-bucket")
    with (
        mock.patch.dict(decide.__globals__, {"read_marker": lambda s3, bucket, run: (marker, [record]), "s3_client": lambda: None}),
        caplog.at_level(logging.INFO),
    ):
        requests = decide()
    check(requests == [], "a gate that trains no type started a training run")
    check("no training of m2 (Elastic Net) for HAI_1: no_features" in caplog.text, f"the decision logged no name beside the key: {caplog.text!r}")


def test_the_drift_reader_names_the_models_it_refuses() -> None:
    gate_types = drift_namespace()["gate_types"]
    gate = {"kind": "drift_gate", "model": "m3"}
    with pytest.raises(RuntimeError, match=r"another model, m3 \(Mixed effects\), not m2 \(Elastic Net\)"):
        gate_types({"model": "m3", "gate": gate}, "m2", [])
    with pytest.raises(RuntimeError, match=r"another model, m9, not m2 \(Elastic Net\)"):
        gate_types({"model": "m9", "gate": gate}, "m2", [])
    with pytest.raises(RuntimeError, match=r"no gate decision of m2 \(Elastic Net\)"):
        gate_types({}, "m2", [])


def test_the_form_check_names_the_models_in_its_refusal() -> None:
    form = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "aws", "model": "m9"}
    with pytest.raises(ValueError, match=r"model must be one of m1 \(OLS baseline\), m2 \(Elastic Net\)"):
        training_contract.parameters_for("training_dag", form, training_contract.LAUNCH_LANES)


def load_in_image(dags: dict[str, Any]) -> ModuleType:
    """Import ``in_image_checks.py`` with stand-ins for the Airflow modules it imports; its ``DagBag`` holds ``dags``."""

    class StandInBag:
        def __init__(self, **settings: Any) -> None:
            self.dags = dags
            self.import_errors: dict[str, str] = {}
            self.dag_ids = sorted(dags)

    stand_in_modules = {
        "airflow": ModuleType("airflow"),
        "airflow.dag_processing": ModuleType("airflow.dag_processing"),
        "airflow.dag_processing.dagbag": ModuleType("airflow.dag_processing.dagbag"),
        "airflow.providers": ModuleType("airflow.providers"),
        "airflow.providers.standard": ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": ModuleType("airflow.providers.standard.operators"),
        "airflow.providers.standard.operators.empty": ModuleType("airflow.providers.standard.operators.empty"),
        "airflow.sdk": ModuleType("airflow.sdk"),
        "airflow.sdk.bases": ModuleType("airflow.sdk.bases"),
        "airflow.sdk.bases.operator": ModuleType("airflow.sdk.bases.operator"),
    }
    for module_name, attribute, value in (
        ("airflow.dag_processing.dagbag", "DagBag", StandInBag),
        ("airflow.providers.standard.operators.empty", "EmptyOperator", object),
        ("airflow.sdk", "DAG", object),
        ("airflow.sdk.bases.operator", "BaseOperator", object),
    ):
        setattr(stand_in_modules[module_name], attribute, value)
    with mock.patch.dict(sys.modules, stand_in_modules):
        return execute_module("in_image_checks_under_test", IN_IMAGE)


def shipped_dags(loaded: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {dag_id: as_airflow_dag(namespace) for dag_id, namespace in loaded.items()}


def dag_without_names(dag_id: str, folder: Path) -> dict[str, Any]:
    """The DAG file with the model list of its doc and the display names of its model choice taken out."""
    source = (ROOT / f"dags/{dag_id}.py").read_text(encoding="utf-8")
    doc, choice = '    doc_md=f"{__doc__}\\n\\nModels: {model_choices()}.",\n', "**model_param_schema()"
    check(doc in source and choice in source, f"{dag_id} no longer holds the lines that this case takes out")
    (folder / f"{dag_id}.py").write_text(source.replace(doc, "").replace(choice, 'enum=["m1", "m2", "m3", "m4", "m5", "m6"]'), encoding="utf-8")
    return run_dag_file(dag_id, folder)


def drift_without_names(folder: Path) -> dict[str, Any]:
    return dag_without_names("drift_monitoring_dag", folder)


def test_the_in_image_check_runs_the_display_rules_on_every_dag_that_shows_a_model_key(loaded: dict[str, dict[str, Any]], tmp_path: Path) -> None:
    report = load_in_image(shipped_dags(loaded)).display_names()
    check(set(report) == set(graph.DOC_MODELS) and "drift_monitoring_dag" in report, "the in-image report leaves out a DAG that shows model keys")
    check(all(row == {"present": True, "violations": []} for row in report.values()), f"a shipped DAG breaks a display rule: {report}")
    broken = {**shipped_dags(loaded), "drift_monitoring_dag": as_airflow_dag(drift_without_names(tmp_path))}
    report = load_in_image(broken).display_names()
    rows = report["drift_monitoring_dag"]["violations"]
    check({row["rule"] for row in rows} == {"display_name"} and len(rows) >= 2, f"the in-image check accepts a drift DAG with no labels and no doc: {rows}")
    check(all(not row["violations"] for dag_id, row in report.items() if dag_id != "drift_monitoring_dag"), "another DAG was blamed for the drift DAG")
    gone = {key: value for key, value in shipped_dags(loaded).items() if key != "drift_monitoring_dag"}
    check(load_in_image(gone).display_names()["drift_monitoring_dag"]["present"] is False, "a missing drift DAG was reported as present")


def test_the_in_image_report_carries_the_display_names(
    loaded: dict[str, dict[str, Any]], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    module = load_in_image(shipped_dags(loaded))
    for name in ("dags", "graph_cases", "transform_graph", "pipeline_graphs", "constraints", "lineage_provider_off", "lineage_http_transport", "real_dags"):
        monkeypatch.setattr(module, name, lambda *arguments: {})
    monkeypatch.setattr(sys, "argv", ["in_image_checks.py"])
    check(module.main() == 0, "the in-image main failed")
    line = next(line for line in capsys.readouterr().out.splitlines() if line.startswith("IN_IMAGE "))
    report = json.loads(line.removeprefix("IN_IMAGE "))
    check(set(report.get("display_names", {})) == set(graph.DOC_MODELS), "the IN_IMAGE document does not carry the display name rows")


def test_the_drift_launch_refusal_names_the_models_it_accepts() -> None:
    request = SubmissionRequest("drift", RUN, "real", 1, "drift_monitoring_dag", "drift_run", "synthetic-dag-run", input_run_id=RUN, training={"model": "m9"})
    with pytest.raises(ecs_launch.LaunchRefused, match=r"'m9', which is not one of m1 \(OLS baseline\), m2 \(Elastic Net\)"):
        ecs_preflight.drift_model(request)


def test_the_in_image_check_fails_on_a_live_drift_dag_with_a_missing_display_value(loaded: dict[str, dict[str, Any]], tmp_path: Path) -> None:
    report = load_in_image(shipped_dags(loaded)).display_names()
    check(LIVE_DAG in report, "the in-image report leaves out the live drift check DAG")
    check(report[LIVE_DAG] == {"present": True, "violations": []}, f"the shipped live drift check DAG breaks a display rule: {report.get(LIVE_DAG)}")
    broken = {**shipped_dags(loaded), LIVE_DAG: as_airflow_dag(dag_without_names(LIVE_DAG, tmp_path))}
    rows = load_in_image(broken).display_names()[LIVE_DAG]["violations"]
    check(
        {row["rule"] for row in rows} == {"display_name"} and len(rows) >= 2, f"the in-image check accepts a live drift DAG with no labels and no doc: {rows}"
    )
    values = dict(loaded[LIVE_DAG]["dag"].settings["params"]["model"].settings)
    values["values_display"] = {**values["values_display"], "m4": "m4"}
    planted = shipped_dags(loaded)[LIVE_DAG]
    planted.params = SimpleNamespace(get_param=lambda key: SimpleNamespace(schema=values))
    names = {row["detail"] for row in load_in_image({**shipped_dags(loaded), LIVE_DAG: planted}).display_names()[LIVE_DAG]["violations"]}
    check(names == {"the model parameter does not show the display name of each model"}, f"one missing display value was not refused alone: {names}")


def model_param_dag_files() -> set[str]:
    """The DAG files whose ``params`` declare a ``model`` parameter, read from their source."""
    found = set()
    for path in sorted((ROOT / "dags").glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Dict) and any(
                isinstance(key, ast.Constant) and key.value == "model" and isinstance(value, ast.Call) and ast.unparse(value.func) == "Param"
                for key, value in zip(node.keys, node.values, strict=True)
            ):
                found.add(path.stem)
    return found


def test_every_dag_with_a_model_parameter_is_in_the_display_inventory() -> None:
    check(model_param_dag_files() == set(graph.MODEL_PARAM_DAGS), f"a DAG with a model parameter is missing from MODEL_PARAM_DAGS: {model_param_dag_files()}")
    check(set(graph.MODEL_PARAM_DAGS) <= set(graph.DOC_MODELS), "a model parameter DAG is missing from DOC_MODELS")


def test_the_live_drift_refusal_names_the_models_it_accepts() -> None:
    request = SubmissionRequest("drift_live_run", RUN, "real", 1, LIVE_DAG, "job", "synthetic-dag-run", None, training={"model": "m9", "hai_type": "HAI_1"})
    with pytest.raises(ecs_launch.LaunchRefused, match=r"'m9', which is not one of m1 \(OLS baseline\), m2 \(Elastic Net\)"):
        ecs_preflight.live_parameters(request)


def test_the_release_refusal_names_the_promotion_models_it_accepts() -> None:
    raw = {"hai_type": "HAI_1", "model": "m3", "features_run": RUN}
    with pytest.raises(ValueError, match=re.escape(f"model must be one of {training_contract.model_choices(training_contract.PROMOTION_MODELS)}")):
        training_contract.release_parameters_for(raw)
    check("m4 (Gradient boosting)" in training_contract.model_choices(training_contract.PROMOTION_MODELS), "the promotion models lose a label")


def keys_without_a_name(doc: str, model: str) -> list[str]:
    """The places in ``doc`` where ``model`` stands without its name after it."""
    shown = re.sub(r"(?<![A-Za-z0-9_])[_*]+(?=\S)|(?<=\S)[_*]+(?![A-Za-z0-9_])", "", doc)
    return re.findall(rf"\b{model}\b(?! \({re.escape(OWNER_NAMES[model])}\))", shown)


def test_a_model_dag_doc_shows_the_name_beside_every_key_it_repeats(loaded: dict[str, dict[str, Any]]) -> None:
    for dag_id, model in training_contract.FEATURE_DAGS.items():
        doc = loaded[dag_id]["dag"].settings["doc_md"]
        check(keys_without_a_name(doc, model) == [], f"the doc of {dag_id} shows {model} without its name: {doc[:160]!r}")


def test_the_graph_rules_refuse_a_model_dag_doc_that_repeats_a_bare_key(loaded: dict[str, dict[str, Any]]) -> None:
    shipped = as_airflow_dag(loaded["features_m2_dag"])
    plain = "Build the m2 model set.\n\nModel: m2 (Elastic Net)."
    rows = graph.display_name_violations(SimpleNamespace(**{**vars(shipped), "doc_md": plain}))
    check({row.rule for row in rows} == {"display_name"} and "m2 without" in rows[0].detail, f"a doc with a bare key was accepted: {rows}")
    named = "Build the m2 (Elastic Net) model set.\n\nModel: m2 (Elastic Net)."
    check(graph.display_name_violations(SimpleNamespace(**{**vars(shipped), "doc_md": named})) == [], "a doc with the name beside every key was refused")
    for emphasized in ("_m2_", "*m2*", "**m2**", "__m2__"):
        marked = f"Build the {emphasized} model set.\n\nModel: m2 (Elastic Net)."
        rows = graph.display_name_violations(SimpleNamespace(**{**vars(shipped), "doc_md": marked}))
        check(any("m2 without" in row.detail for row in rows), f"a doc with the bare key in emphasis ({emphasized}) was accepted: {rows}")
    identifier = "The DAG features_m2_dag builds the m2 (Elastic Net) model set.\n\nModel: m2 (Elastic Net)."
    check(graph.display_name_violations(SimpleNamespace(**{**vars(shipped), "doc_md": identifier})) == [], "a doc that names a code identifier was refused")


def test_no_doc_of_a_dag_that_names_models_shows_a_bare_key(loaded: dict[str, dict[str, Any]]) -> None:
    for dag_id in {*graph.DOC_MODELS, LIVE_DAG}:
        doc = loaded[dag_id]["dag"].settings["doc_md"]
        for model in training_contract.MODELS:
            check(keys_without_a_name(doc, model) == [], f"the doc of {dag_id} shows {model} without its name")
