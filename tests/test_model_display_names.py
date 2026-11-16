"""The model display names: one source of truth beside the model lists, and every DAG and message that names a model shows it.

The DAG files run under the declared Airflow SDK stand-ins of ``scripts.training.lane_b_dag_cases``, so the cases read what each file really declares.
"""

from __future__ import annotations

import runpy
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from orchestration import graph, training_contract
from scripts.training import lane_b_dag_cases as stand_ins
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
OWNER_NAMES = {"m1": "OLS baseline", "m2": "Elastic Net", "m3": "Mixed effects", "m4": "Gradient boosting", "m5": "Spatial", "m6": "Fixed effects"}


class StandInTrigger(stand_ins.Node):
    """The operator of the standard provider, as far as a model DAG file uses it."""

    def __init__(self, **settings: Any) -> None:
        super().__init__(settings["task_id"], "all_success")
        if stand_ins.Dag.current is not None:
            stand_ins.Dag.current.task_dict[self.task_id] = self

    @staticmethod
    def partial(**settings: Any) -> Any:
        return stand_ins.TriggerDagRunOperator.partial(**settings)


def run_dag_file(name: str) -> dict[str, Any]:
    """Run ``dags/<name>.py`` with the SDK stand-ins and return its module namespace."""
    sdk = ModuleType("airflow.sdk")
    for key, value in (
        ("DAG", stand_ins.Dag),
        ("Param", stand_ins.Param),
        ("TriggerRule", stand_ins.Rule),
        ("Asset", str),
        ("task", stand_ins.task),
        ("get_current_context", lambda: {}),
    ):
        setattr(sdk, key, value)
    trigger = ModuleType("airflow.providers.standard.operators.trigger_dagrun")
    trigger.__dict__["TriggerDagRunOperator"] = StandInTrigger
    providers = {part: ModuleType(part) for part in ("airflow.providers", "airflow.providers.standard", "airflow.providers.standard.operators")}
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, {"airflow": ModuleType("airflow"), "airflow.sdk": sdk, **providers, trigger.__name__: trigger}):
            return runpy.run_path(str(ROOT / "dags" / f"{name}.py"))
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous


def as_airflow_dag(namespace: dict[str, Any]) -> SimpleNamespace:
    """Read a stand-in DAG the way the graph rules read an Airflow DAG: its ``dag_id``, names, docs, tags and ``params``."""
    settings = namespace["dag"].settings
    params = {key: SimpleNamespace(schema=param.settings) for key, param in settings["params"].items()}
    return SimpleNamespace(
        dag_id=settings["dag_id"],
        dag_display_name=settings.get("dag_display_name", settings["dag_id"]),
        description=settings.get("description"),
        doc_md=settings.get("doc_md"),
        tags=set(settings.get("tags", ())),
        params=SimpleNamespace(get_param=params.__getitem__),
    )


@pytest.fixture(scope="module")
def loaded() -> Iterator[dict[str, dict[str, Any]]]:
    names = [*training_contract.FEATURE_DAGS, "training_dag", "drift_monitoring_dag", "research_dag"]
    yield {name: run_dag_file(name) for name in names}


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


@pytest.mark.parametrize("dag_id", ["training_dag", "drift_monitoring_dag"])
def test_the_model_choice_of_a_form_shows_every_name(loaded: dict[str, dict[str, Any]], dag_id: str) -> None:
    schema = loaded[dag_id]["dag"].settings["params"]["model"].settings
    check(schema["enum"] == list(training_contract.MODELS), "the model choice lost or changed its values")
    check(
        schema["values_display"] == {model: training_contract.model_title(model) for model in training_contract.MODELS},
        "the model choice does not show each name",
    )
    check(all(training_contract.model_label(model) in schema["description"] for model in training_contract.MODELS), "the model choice help lacks a label")


@pytest.mark.parametrize(
    ("dag_id", "models"), [("training_dag", training_contract.MODELS), ("research_dag", ("m1", "m2", "m4")), ("drift_monitoring_dag", training_contract.MODELS)]
)
def test_the_doc_of_a_dag_that_names_models_shows_their_names(loaded: dict[str, dict[str, Any]], dag_id: str, models: tuple[str, ...]) -> None:
    doc = loaded[dag_id]["dag"].settings["doc_md"]
    check(all(training_contract.model_label(model) in doc for model in models), "a DAG doc lacks the label of a model it names")


def test_the_drift_log_line_names_the_model_with_its_name(loaded: dict[str, dict[str, Any]]) -> None:
    line = loaded["drift_monitoring_dag"]["skip_line"]("m2", "HAI_1", "no_features")
    check(line == "no training of m2 (Elastic Net) for HAI_1: no_features", "the drift log line is wrong")


def test_the_graph_rules_accept_every_shipped_dag(loaded: dict[str, dict[str, Any]]) -> None:
    for dag_id in (*training_contract.FEATURE_DAGS, "training_dag", "drift_monitoring_dag", "research_dag"):
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
