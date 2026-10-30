"""E2E cases for ``dags/drift_monitoring_dag.py`` on synthetic drift records, with a declared stand-in for the Airflow SDK.

The real DAG file runs against stand-ins for ``airflow.sdk``, the standard provider's ``TriggerDagRunOperator`` and ``AirflowSkipException``, as the other
DAG cases do. The stand-ins record the graph the file declares and keep each task's function, so the cases run the real task bodies with a chosen DAG run
context. The broker is stood in by a captured ``submit_and_wait``. What this proves: the declared graph, the trigger arguments, the decision from records,
the training configuration against the real validators, and the skip for nothing to monitor. What it does not prove: that Airflow accepts the graph, maps the
trigger over the list, or starts ``training_dag``. That needs ``airflow dags test`` in the orchestration image.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import re
import sys
from collections.abc import Callable
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar
from unittest import mock

from orchestration.interface import Handle, State, Status
from orchestration.training_contract import RUN_ID
from scripts.orchestration.trigger_dag import run_conf

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "dags/drift_monitoring_dag.py"
DRIFT_RUN = "20261030T120000Z-0a1b2c3d"
Problems = list[str]
CONTEXT: dict[str, Any] = {}


class Rule(StrEnum):
    ALL_SUCCESS = "all_success"
    ALL_DONE = "all_done"


class SkipSignal(Exception):
    """The stand-in for AirflowSkipException."""


class Node:
    """A declared task: its settings, its function and its upstream task ids."""

    def __init__(self, task_id: str, settings: dict[str, Any], function: Callable[..., Any] | None = None) -> None:
        self.task_id, self.settings, self.function = task_id, settings, function
        self.upstream_task_ids: set[str] = set()

    def __rshift__(self, after: Node) -> Node:
        after.upstream_task_ids.add(self.task_id)
        return after


class Dag:
    current: ClassVar[Dag | None] = None

    def __init__(self, **settings: Any) -> None:
        self.settings = settings
        self.task_dict: dict[str, Node] = {}

    def __enter__(self) -> Dag:
        Dag.current = self
        return self

    def __exit__(self, *arguments: Any) -> None:
        Dag.current = None


class Param:
    def __init__(self, *arguments: Any, **settings: Any) -> None:
        self.default = arguments[0] if arguments else None
        self.settings = settings


def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        def call(*arguments: Any, **keywords: Any) -> Any:
            if Dag.current is None:
                raise ValueError("a task was created outside its DAG")
            node = Node(settings["task_id"], settings, function)
            Dag.current.task_dict[node.task_id] = node
            return SimpleNamespace(operator=node)

        return call

    return decorate


class TriggerDagRunOperator:
    """Record ``partial(...).expand_kwargs(source)`` as one mapped task fed by the source task."""

    @staticmethod
    def partial(**settings: Any) -> SimpleNamespace:
        def expand_kwargs(source: Any) -> Node:
            if Dag.current is None:
                raise ValueError("an operator was created outside its DAG")
            node = Node(settings["task_id"], settings)
            node.upstream_task_ids.add(source.operator.task_id)
            Dag.current.task_dict[node.task_id] = node
            return node

        return SimpleNamespace(expand_kwargs=expand_kwargs)


def modules() -> dict[str, ModuleType]:
    sdk = ModuleType("airflow.sdk")
    for name, value in (
        ("DAG", Dag),
        ("Param", Param),
        ("TriggerRule", Rule),
        ("Asset", str),
        ("task", task),
        ("get_current_context", lambda: CONTEXT["context"]),
    ):
        setattr(sdk, name, value)
    exceptions, trigger = ModuleType("airflow.sdk.exceptions"), ModuleType("airflow.providers.standard.operators.trigger_dagrun")
    for module, name, value in ((exceptions, "AirflowSkipException", SkipSignal), (trigger, "TriggerDagRunOperator", TriggerDagRunOperator)):
        setattr(module, name, value)
    names = ("airflow", "airflow.providers", "airflow.providers.standard", "airflow.providers.standard.operators")
    return {
        **{name: ModuleType(name) for name in names},
        "airflow.sdk": sdk,
        "airflow.sdk.exceptions": exceptions,
        "airflow.providers.standard.operators.trigger_dagrun": trigger,
    }


@contextmanager
def loaded_dag() -> Any:
    """Load the real DAG file against the stand-ins and give back its module, restoring the real ``orchestration.dag_runtime`` afterwards."""
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, modules()):
            specification = importlib.util.spec_from_file_location("drift_dag_under_test", DAG_FILE)
            if specification is None or specification.loader is None:
                raise ValueError("the DAG file has no import specification")
            module = importlib.util.module_from_spec(specification)
            specification.loader.exec_module(module)
            yield module
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous


def record(hai_type: str, *, run: str = DRIFT_RUN, status: str = "checked", triggered: bool = False, population: str = "primary") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "drift_record",
        "run_id": run,
        "hai_type": hai_type,
        "population": population,
        "status": status,
        "skip_reason": "no_new_window" if status == "skipped" else None,
        "triggered": triggered,
    }


def write_records(folder: Path, records: list[dict[str, Any]]) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    for item in records:
        (folder / f"drift_{item['hai_type']}.json").write_text(json.dumps(item) + "\n")
    return folder


def decide(module: Any, folder: Path, *, mode: str = "fixture", mlflow: str = "local") -> list[dict[str, Any]]:
    """Run the real ``decide`` task body for the DRIFT_RUN against records in ``folder``."""
    CONTEXT["context"] = {"params": {"run_id": DRIFT_RUN, "mode": mode, "mlflow": mlflow}}
    with mock.patch.object(module, "RECORDS", folder.parent):
        result: list[dict[str, Any]] = module.dag.task_dict["decide"].function()
    return result


def graph_is_declared(folder: Path) -> Problems:
    with loaded_dag() as module:
        dag = module.dag
        nodes = dag.task_dict
        expected = {
            "fetch_baseline": set(),
            "fetch_current": {"fetch_baseline"},
            "check_drift": {"fetch_current"},
            "decide": {"check_drift"},
            "trigger_training": {"decide"},
        }
        problems = []
        if {name: node.upstream_task_ids for name, node in nodes.items()} != expected:
            problems.append(f"the graph is { ({name: sorted(node.upstream_task_ids) for name, node in nodes.items()}) }")
        wanted = {"schedule": None, "catchup": False, "max_active_runs": 1, "default_args": {"retries": 0}, "dag_id": "drift_monitoring_dag"}
        problems += [f"DAG setting {key} is {dag.settings.get(key)!r}, expected {value!r}" for key, value in wanted.items() if dag.settings.get(key) != value]
        trigger = nodes["trigger_training"].settings
        wanted_trigger = {"trigger_dag_id": "training_dag", "wait_for_completion": False, "reset_dag_run": False, "skip_when_already_exists": True}
        problems += [f"trigger setting {key} is {trigger.get(key)!r}, expected {value!r}" for key, value in wanted_trigger.items() if trigger.get(key) != value]
        if trigger.get("fail_when_dag_is_paused") is not True:
            problems.append("a trigger of a paused training_dag would pass silently")
        params = dag.settings["params"]
        if params["run_id"].settings.get("pattern") != f"^{RUN_ID}$" or params["mode"].settings.get("enum") != ["fixture", "real"]:
            problems.append("the run_id or mode parameter differs from the training DAG's")
        return problems


def one_drifted_type_starts_one_training_run(folder: Path) -> Problems:
    with loaded_dag() as module:
        records = [record("HAI_1", triggered=True), record("HAI_2"), record("HAI_3", status="skipped")]
        requests = decide(module, write_records(folder / DRIFT_RUN, records))
        if len(requests) != 1:
            return [f"expected one trigger request, got {len(requests)}"]
        request = requests[0]
        training = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": f"drift:{DRIFT_RUN}", "mlflow": "local"}
        problems = []
        if request["trigger_run_id"] != f"drift__{DRIFT_RUN}":
            problems.append(f"the trigger run id is {request['trigger_run_id']}")
        if request["conf"] != run_conf("training_dag", "fixture", request["conf"]["run_id"], None, training):
            problems.append(f"the training conf differs from the established builder's: {request['conf']}")
        if not re.fullmatch(RUN_ID, request["conf"]["run_id"]) or request["conf"]["run_id"] == DRIFT_RUN:
            problems.append("the training run id is malformed or equals the drift run id")
        return problems


def several_drifted_types_share_one_run(folder: Path) -> Problems:
    with loaded_dag() as module:
        records = [record("HAI_4", triggered=True), record("HAI_1", triggered=True), record("HAI_2")]
        requests = decide(module, write_records(folder / DRIFT_RUN, records))
        types = requests[0]["conf"]["hai_types"] if requests else None
        return [] if len(requests) == 1 and types == ["HAI_1", "HAI_4"] else [f"expected one run for HAI_1 and HAI_4, got {types} in {len(requests)} requests"]


def quiet_records_start_nothing(folder: Path) -> Problems:
    with loaded_dag() as module:
        requests = decide(module, write_records(folder / DRIFT_RUN, [record("HAI_1"), record("HAI_2", status="skipped"), record("HAI_3")]))
        return [] if requests == [] else [f"quiet records started {requests}"]


def real_mode_passes_through(folder: Path) -> Problems:
    with loaded_dag() as module:
        requests = decide(module, write_records(folder / DRIFT_RUN, [record("HAI_5", triggered=True)]), mode="real")
        return [] if requests and requests[0]["conf"]["mode"] == "real" else ["the training run did not take the drift run's mode"]


def decision_repeats(folder: Path) -> Problems:
    with loaded_dag() as module:
        written = write_records(folder / DRIFT_RUN, [record("HAI_1", triggered=True)])
        return [] if decide(module, written) == decide(module, written) else ["two decisions on the same records differ, so a retry could start a second run"]


def refuses(detail: str, records: list[dict[str, Any]], *, raw: dict[str, str] | None = None) -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        with loaded_dag() as module:
            written = write_records(folder / DRIFT_RUN, records)
            for name, text in (raw or {}).items():
                (written / name).write_text(text)
            try:
                requests = decide(module, written)
            except RuntimeError as error:
                return [] if detail in str(error) else [f"the error does not say {detail!r}: {error}"]
            return [f"records that must be refused started {requests}"]

    return case


def broker_steps(folder: Path) -> Problems:
    """Each step reaches the broker as its own template for this DAG, carries no training parameters and passes the DAG run's mode."""
    sent: list[Any] = []

    def wait(client: Any, request: Any) -> tuple[Handle, Status]:
        sent.append(request)
        handle = Handle(request.submission_id, request.container_name)
        return handle, Status(handle, State.SUCCEEDED, 0, None)

    with loaded_dag() as module:
        for name in ("fetch_baseline", "fetch_current", "check_drift"):
            instance = SimpleNamespace(try_number=1, dag_id="drift_monitoring_dag", task_id=name, run_id="manual")
            CONTEXT["context"] = {"params": {"run_id": DRIFT_RUN, "mode": "real", "mlflow": "local"}, "ti": instance}
            with (
                mock.patch.object(module.dag_runtime.BrokerClient, "from_session", return_value=SimpleNamespace()),
                mock.patch.object(module.dag_runtime, "submit_and_wait", side_effect=wait),
            ):
                module.dag.task_dict[name].function()
    shape = [(request.template, request.dag_id, request.mode, request.training) for request in sent]
    expected = [(name, "drift_monitoring_dag", "real", None) for name in ("fetch_baseline", "fetch_current", "check_drift")]
    return [] if shape == expected else [f"the broker received {shape}"]


def nothing_to_monitor_skips(exit_code: int, skips: bool) -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        with loaded_dag() as module:
            failure = RuntimeError(f"fetch_baseline ended failed with exit code {exit_code}")
            with mock.patch.object(module.dag_runtime, "run_step", side_effect=failure):
                try:
                    module.dag.task_dict["fetch_baseline"].function()
                except SkipSignal:
                    return [] if skips else [f"exit code {exit_code} skipped the run, it is a failure"]
                except RuntimeError:
                    return [] if not skips else ["exit code 4 failed the run, it means nothing to monitor"]
            return ["the step neither skipped nor failed"]

    return case


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    stale = [record("HAI_1", run="20261029T120000Z-ffffffff", triggered=True)]
    return [
        ("dag_declares_the_graph", graph_is_declared),
        ("dag_one_drifted_type_starts_one_training_run", one_drifted_type_starts_one_training_run),
        ("dag_drifted_types_share_one_training_run", several_drifted_types_share_one_run),
        ("dag_quiet_records_start_nothing", quiet_records_start_nothing),
        ("dag_real_mode_passes_through", real_mode_passes_through),
        ("dag_decision_repeats", decision_repeats),
        ("dag_refuses_a_record_of_another_run", refuses("another run", stale)),
        ("dag_refuses_missing_records", refuses("no drift record", [])),
        ("dag_refuses_an_unreadable_record", refuses("unreadable", [record("HAI_1")], raw={"drift_HAI_2.json": "{not json"})),
        ("dag_refuses_a_skipped_record_that_triggers", refuses("inconsistent", [record("HAI_1", status="skipped", triggered=True)])),
        ("dag_refuses_a_sensitivity_record_that_triggers", refuses("population", [record("HAI_1", triggered=True, population="sensitivity")])),
        ("dag_steps_reach_the_broker", broker_steps),
        ("dag_nothing_to_monitor_skips", nothing_to_monitor_skips(4, True)),
        ("dag_other_step_failures_fail", nothing_to_monitor_skips(1, False)),
    ]
