"""Cases for the bronze-triggered chain DAG: derived run IDs, trigger order and the guard against starting a retried bronze snapshot again.

The airflow packages are not installed here. Small stand-ins record the DAG, its tasks and each trigger, and Jinja renders the templated trigger fields
with the DAG's own macros, so the run IDs and settings a trigger would send are the ones the cases compare. Nothing leaves the machine.
"""

from __future__ import annotations

import re
import runpy
import sys
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import jinja2

from scripts.orchestration.ecs_adapter_cases import require

ROOT = Path(__file__).resolve().parents[2]
BRONZE = "20261030T090000Z-b0b0b0b0"
OTHER_BRONZE = "20261031T090000Z-b1b1b1b1"
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")


class Operator:
    """A task node that records its upstream and downstream links and the settings it was built with."""

    def __init__(self, task_id: str, **settings: Any) -> None:
        self.task_id, self.settings = task_id, settings
        self.upstream: list[Operator] = []

    def __rshift__(self, other: Operator) -> Operator:
        other.upstream.append(self)
        return other


class Dag:
    def __init__(self, **settings: Any) -> None:
        self.settings = settings

    def __enter__(self) -> Dag:
        return self

    def __exit__(self, *_: object) -> None:
        return None


def load_dag() -> dict[str, Any]:
    """Run ``dags/bronze_chain_dag.py`` against the stand-ins and return its globals and the operators it built."""
    built: list[Operator] = []

    def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            def call() -> SimpleNamespace:
                node = Operator(**settings)
                built.append(node)
                return SimpleNamespace(operator=node)

            return call

        return decorate

    def trigger(**settings: Any) -> Operator:
        node = Operator(**settings)
        built.append(node)
        return node

    sdk = SimpleNamespace(
        Asset=str,
        TriggerRule=SimpleNamespace(ALL_SUCCESS="all_success", ALL_DONE="all_done"),
        DAG=Dag,
        Param=lambda *a, **k: (a, k),
        task=task,
        get_current_context=dict,
    )
    modules: dict[str, Any] = {
        "airflow": SimpleNamespace(),
        "airflow.sdk": sdk,
        "airflow.providers": SimpleNamespace(),
        "airflow.providers.standard": SimpleNamespace(),
        "airflow.providers.standard.operators": SimpleNamespace(),
        "airflow.providers.standard.operators.trigger_dagrun": SimpleNamespace(TriggerDagRunOperator=trigger),
    }
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, modules):
            loaded = runpy.run_path(str(ROOT / "dags/bronze_chain_dag.py"))
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous
    return {**loaded, "operators": built}


def render(loaded: dict[str, Any], template: str, bronze: str) -> str:
    """A templated field as Airflow renders it: the DAG's macros and its ``params``."""
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=True)
    return environment.from_string(template).render(params=SimpleNamespace(run_id=bronze, mode="real"), **loaded["dag"].settings["user_defined_macros"])


def derived_ids_are_valid_and_stable() -> None:
    loaded = load_dag()
    ids = [loaded["chain_run"](layer, BRONZE) for layer, _, _ in loaded["CHAIN"]]
    require(all(RUN_ID.fullmatch(run) for run in ids), f"a derived run ID has the wrong shape: {ids}")
    require(all(run.startswith(BRONZE[:16]) for run in ids), "a derived run ID lost the bronze time stamp")
    require(len(set(ids)) == 4 and BRONZE not in ids, "two layers share a run ID")
    require(ids == [loaded["chain_run"](layer, BRONZE) for layer, _, _ in loaded["CHAIN"]], "the run IDs are not stable")
    require(not set(ids) & {loaded["chain_run"](layer, OTHER_BRONZE) for layer, _, _ in loaded["CHAIN"]}, "two bronze runs share a layer run ID")
    require(loaded["chain_run"]("gold", BRONZE) == loaded["derived_run_id"](ids[0], "gold"), "gold does not derive from silver")


def chain_runs_in_order_after_the_gate() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node for node in loaded["operators"]}
    require(list(nodes) == ["chain_gate", "run_silver", "run_gold", "run_features", "run_drift"], f"wrong tasks: {list(nodes)}")
    order = ["chain_gate", "run_silver", "run_gold", "run_features", "run_drift"]
    for before, after in zip(order, order[1:], strict=False):
        require(nodes[after].upstream == [nodes[before]], f"{after} does not follow only {before}")
    require(loaded["dag"].settings["schedule"] is None and loaded["dag"].settings["catchup"] is False, "the chain has a calendar schedule")
    require(
        [nodes[name].settings["trigger_dag_id"] for name in order[1:]]
        == ["silver_transform_dag", "gold_transform_dag", "features_transform_dag", "drift_monitoring_dag"],
        "wrong DAGs",
    )
    for name in order[1:]:
        settings = nodes[name].settings
        require(
            settings["wait_for_completion"] is True and settings["allowed_states"] == ["success"] and "failed" in settings["failed_states"],
            f"{name} does not wait for success",
        )
        require(settings["skip_when_already_exists"] is True and settings["reset_dag_run"] is False, f"{name} may restart an existing run")


def triggers_send_derived_ids() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node.settings for node in loaded["operators"]}

    def run(layer: str) -> str:
        return str(loaded["chain_run"](layer, BRONZE))

    rendered = {name: render(loaded, nodes[name]["trigger_run_id"], BRONZE) for name in ("run_silver", "run_gold", "run_features", "run_drift")}
    require(rendered["run_gold"] == f"chain__gold_transform_dag__{run('gold')}", f"wrong DAG run ID for gold: {rendered['run_gold']}")
    conf = {name: {key: render(loaded, value, BRONZE) for key, value in nodes[name]["conf"].items()} for name in rendered}
    require(conf["run_silver"] == {"run_id": run("silver"), "mode": "real"}, f"silver settings: {conf['run_silver']}")
    require(conf["run_gold"] == {"run_id": run("gold"), "mode": "real", "silver_snapshot_run": run("silver")}, f"gold settings: {conf['run_gold']}")
    require(conf["run_features"] == {"run_id": run("features"), "mode": "real", "gold_snapshot_run": run("gold")}, f"features settings: {conf['run_features']}")
    require(conf["run_drift"] == {"run_id": run("drift"), "mode": "real", "mlflow": "local"}, f"drift settings: {conf['run_drift']}")


def a_retried_bronze_run_names_the_same_runs() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node.settings for node in loaded["operators"]}
    first = {name: render(loaded, nodes[name]["trigger_run_id"], BRONZE) for name in nodes if name.startswith("run_")}
    again = {name: render(loaded, nodes[name]["trigger_run_id"], BRONZE) for name in nodes if name.startswith("run_")}
    other = {name: render(loaded, nodes[name]["trigger_run_id"], OTHER_BRONZE) for name in nodes if name.startswith("run_")}
    require(first == again, "a second trigger for one bronze run names new DAG runs")
    require(not set(first.values()) & set(other.values()), "a new bronze run reuses an old DAG run ID")


def waits_have_explicit_deadlines() -> None:
    loaded = load_dag()
    triggers = [node.settings for node in loaded["operators"] if node.task_id.startswith("run_")]
    limits = [settings.get("execution_timeout") for settings in triggers]
    require(len(triggers) == 4 and all(isinstance(limit, timedelta) and limit > timedelta(0) for limit in limits), f"a layer wait has no deadline: {limits}")
    run_limit = loaded["dag"].settings.get("dagrun_timeout")
    require(isinstance(run_limit, timedelta) and run_limit >= sum(limits, timedelta(0)), f"the chain run has no deadline that covers its layers: {run_limit}")
    require(loaded["dag"].settings.get("max_active_runs") == 1, "the chain may run twice at once")


# Child DAGs that live on a branch that is not in this tree yet. When a branch is combined, the file appears and this case fails until the entry is removed,
# which makes the integration a deliberate step with the checks below.
PENDING_CHILDREN = {"drift_monitoring_dag": "feat/drift-monitoring"}


def named_child_dags_exist_and_accept_the_settings() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node.settings for node in loaded["operators"]}
    problems = []
    for layer, dag_id, _ in loaded["CHAIN"]:
        path = ROOT / "dags" / f"{dag_id}.py"
        if dag_id in PENDING_CHILDREN:
            if path.exists():
                problems.append(f"{dag_id} is now in the tree: remove it from PENDING_CHILDREN so its settings are checked")
            continue
        if not path.exists():
            problems.append(f"{dag_id} is not in the tree and is not marked as pending on a branch")
            continue
        text = path.read_text()
        if f'dag_id="{dag_id}"' not in text:
            problems.append(f"{path.name} does not declare {dag_id}")
        for key in nodes[f"run_{layer}"]["conf"]:
            if not re.search(rf'"{key}"\s*:\s*Param\(', text):
                problems.append(f"{dag_id} declares no parameter {key}")
    require(not problems, "; ".join(problems))


CASES: dict[str, Callable[[], None]] = {
    "derived_ids_are_valid_and_stable": derived_ids_are_valid_and_stable,
    "chain_runs_in_order_after_the_gate": chain_runs_in_order_after_the_gate,
    "triggers_send_derived_ids": triggers_send_derived_ids,
    "a_retried_bronze_run_names_the_same_runs": a_retried_bronze_run_names_the_same_runs,
    "waits_have_explicit_deadlines": waits_have_explicit_deadlines,
    "named_child_dags_exist_and_accept_the_settings": named_child_dags_exist_and_accept_the_settings,
}
