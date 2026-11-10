"""Exercise actual DAG builders with a declared Airflow SDK stand-in."""

from __future__ import annotations

import contextlib
import importlib
import runpy
import sys
from collections.abc import Callable
from enum import StrEnum
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar
from unittest import mock

from orchestration import graph, launcher
from orchestration.interface import SubmissionRequest
from scripts.lakehouse import memory_budget
from scripts.orchestration.trigger_dag import run_conf
from scripts.training.lane_b_cases import PARAMETERS, REGISTRY_ID, ROOT, RUN, Cases, require


class Rule(StrEnum):
    """The two trigger rules the builders use."""

    ALL_SUCCESS = "all_success"
    ALL_DONE = "all_done"


class Node:
    """An operator shape that records actual builder dependencies."""

    def __init__(self, task_id: str, trigger_rule: str) -> None:
        self.task_id = task_id
        self.trigger_rule = trigger_rule
        self.upstream_task_ids: set[str] = set()
        self.is_setup = False
        self.is_teardown = False

    def __rshift__(self, after: Node) -> Node:
        after.upstream_task_ids.add(self.task_id)
        return after


class Dag:
    """A context that accepts actual DAG declarations without importing Airflow."""

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
    """Retain parameter metadata for declaration checks."""

    def __init__(self, *arguments: Any, **settings: Any) -> None:
        self.default = arguments[0] if arguments else None
        self.settings = settings


def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Record a task when the real builder instantiates its decorated function."""

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        def call(*arguments: Any, **keywords: Any) -> Any:
            if Dag.current is None:
                raise ValueError("task was created outside its declared DAG")
            node = Node(settings["task_id"], settings.get("trigger_rule", Rule.ALL_SUCCESS))
            Dag.current.task_dict[node.task_id] = node
            return SimpleNamespace(operator=node)

        return call

    return decorate


def run() -> list[dict[str, Any]]:
    """Exercise actual declarations and task adapters while recording the substituted SDK boundary."""
    cases = Cases([])
    cases.check(
        "trigger_training_configuration",
        lambda: require(
            run_conf("training_dag", "real", RUN, None, {**PARAMETERS, "mlflow": "aws"})["hai_types"] == PARAMETERS["hai_types"],
            "trigger dropped the training selection",
        ),
    )
    promotion = {"hai_type": "HAI_2", "model_version": "3", "promotion_mode": "rollback", "mlflow": "aws"}
    cases.check(
        "trigger_rollback_configuration",
        lambda: require(run_conf("model_promotion_dag", "real", RUN, None, promotion)["mode"] == "rollback", "trigger changed rollback into forward promotion"),
    )
    cases.check(
        "service_shutdown_uses_owner_flag",
        lambda: require(
            launcher.METHODS["mlflow_down"].kinds == ("any",) and launcher.METHODS["mlflow_down"].owner_flag,
            "service shutdown uses an unsupported principal or lacks the owner flag",
        ),
    )
    sdk = ModuleType("airflow.sdk")
    for name, value in (
        ("DAG", Dag),
        ("Param", Param),
        ("TriggerRule", Rule),
        ("Asset", str),
        ("task", task),
        ("get_current_context", lambda: {}),
    ):
        setattr(sdk, name, value)
    package = ModuleType("airflow")
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, {"airflow": package, "airflow.sdk": sdk}):
            runtime = importlib.import_module("orchestration.dag_runtime")
            for dag_id in ("training_dag", "model_promotion_dag"):
                loaded = runpy.run_path(str(ROOT / "dags" / f"{dag_id}.py"))
                dag = loaded["dag"]

                def check_graph(dag: Dag = dag, dag_id: str = dag_id) -> None:
                    require(not graph.validate_transform_graph(graph.graph_from_dag(dag), dag_id), "actual DAG builder differs from the accepted graph")

                def check_limits(dag: Dag = dag) -> None:
                    require(
                        dag.settings["max_active_runs"] == 1
                        and dag.settings["schedule"] is None
                        and dag.settings["catchup"] is False
                        and dag.settings["default_args"]["retries"] == 0,
                        "DAG scheduling or retry limits changed",
                    )

                cases.check(f"dag_{dag_id}_actual_builder", check_graph)
                cases.check(f"dag_{dag_id}_run_limits", check_limits)
            captured: list[SubmissionRequest] = []
            launched = {**PARAMETERS, "mlflow": "aws"}  # a DAG step launches on AWS, so its MLflow lane is aws (mlflow=local is refused by name)

            def launch_step(request: SubmissionRequest, context: Any, selections: Any) -> str:
                captured.append(request)
                return "succeeded"

            library = SimpleNamespace(launch_step=launch_step)  # the launch library's fake: the DAGs launch through nothing else
            instance = SimpleNamespace(try_number=1, dag_id="training_dag", task_id="guard", run_id="manual")
            with (
                mock.patch.object(runtime, "get_current_context", return_value={"params": {"run_id": RUN, "mode": "real", **launched}, "ti": instance}),
                mock.patch.object(runtime, "launch_module", return_value=library),
            ):
                cases.check(
                    "dag_step_returns_the_launch_library_state", lambda: require(runtime.run_step("guard") == "succeeded", "the step lost the library's state")
                )
            cases.check(
                "dag_training_parameters_reach_launch_library",
                lambda: require(
                    len(captured) == 1 and captured[0].training == launched and captured[0].mode == "real", "task adapter dropped a training parameter"
                ),
            )
            instance = SimpleNamespace(try_number=1, dag_id="model_promotion_dag", task_id="promote", run_id="manual")
            params = {"run_id": RUN, "mode": "rollback", "hai_type": "HAI_2", "model_version": "3", "mlflow": "aws"}
            with (
                mock.patch.object(runtime, "get_current_context", return_value={"params": params, "ti": instance}),
                mock.patch.object(runtime, "launch_module", return_value=library),
            ):
                cases.check("dag_rollback_parameter_mapping", lambda: runtime.run_step("promote"))
            local = {**params, "mlflow": "local"}
            with (
                mock.patch.object(runtime, "get_current_context", return_value={"params": local, "ti": instance}),
                mock.patch.object(runtime, "launch_module", return_value=library),
            ):
                cases.refuses("dag_local_mlflow_refused_by_name", lambda: runtime.run_step("promote"), "mlflow=aws")
            cases.check(
                "dag_rollback_uses_real_execution_mode",
                lambda: require(
                    captured[-1].mode == "real"
                    and captured[-1].training == {"hai_type": "HAI_2", "model_version": "3", "promotion_mode": "rollback", "mlflow": "aws"},
                    "rollback mode was confused with data mode",
                ),
            )
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous
    fake: Any = object.__new__(launcher.Launcher)
    fake.reader = lambda: None
    fake.registry = SimpleNamespace(kind="test")
    fake.registry_id = REGISTRY_ID
    fake.cfg = SimpleNamespace(registry=SimpleNamespace(kind="test"))
    fake.docker = SimpleNamespace(networks=mock.Mock(side_effect=RuntimeError("network work began before memory admission")))
    low_memory = memory_budget.Budget(total=512 * 1024**2, used_by_containers=0, headroom=0)
    with (
        mock.patch.object(memory_budget, "current", return_value=low_memory),
        mock.patch.object(memory_budget, "launch_plan", return_value=SimpleNamespace(budget=low_memory)),
    ):
        cases.refuses("service_low_memory_starts_no_network", lambda: fake.prepare_mlflow(ROOT, ROOT / "data/e2e/training", 2**40), "memory")
    cases.check(
        "service_low_memory_has_no_side_effects",
        lambda: require(fake.docker.networks.call_count == 0, "network operations began before service memory admission"),
    )
    fake.docker_ok = False
    fake.status = "degraded"
    state: dict[str, Any] = {"launcher": {"mlflow": {"phase": "starting", "deadline": 0}}, "services": {"mlflow": 1024**3}}
    fake.tx = lambda **keywords: contextlib.nullcontext(state)
    cases.check("service_start_deadline_during_docker_outage", fake.expire_mlflow_startup)
    cases.check(
        "service_expiry_retains_ownership",
        lambda: require(
            state["launcher"]["mlflow"]["phase"] == "tearing_down" and state["services"]["mlflow"] == 1024**3, "expired startup dropped service ownership"
        ),
    )
    state["launcher"]["mlflow"]["stop_deadline"] = 0
    cases.check("service_stop_deadline_during_docker_outage", fake.expire_mlflow_startup)
    cases.check(
        "service_unconfirmed_cleanup_retains_reservation",
        lambda: require(
            state["launcher"]["mlflow"]["phase"] == "cleanup_failed" and state["services"]["mlflow"] == 1024**3, "unconfirmed cleanup released its reservation"
        ),
    )
    return cases.results
