"""The graph rules of ``transform_dag`` (Airflow plan section 5), checked offline on a plain graph.

The validator never imports Airflow: ``graph_from_dag`` reads any object shaped like an Airflow DAG (``task_dict``, and
per task ``upstream_task_ids``, ``trigger_rule``, ``is_setup`` and ``is_teardown``), so the same rules run on the host
against plain graphs and inside the pinned Airflow image against real DAG objects (failure modes 761, 766 and 823).
Rules: no setup or teardown task; ``release_run`` is the only leaf; no cycle; exactly the section 5 tasks; each work
task's upstream set follows the section 5 order with the default ``all_success`` rule; ``run_record``,
``catalog_release``, ``final_gate`` and ``release_run`` have the section 5 trigger rules and upstream sets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from orchestration.training_contract import PROMOTION_STEPS, TRAINING_STEPS

WORK_CHAIN = (
    "acquire_run",
    "catalog_up",
    "pin_bronze",
    "silver_build",
    "silver_validate",
    "gold_build",
    "gold_validate",
    "cross_layer_checks",
    "verify_bronze",
    "publish_branch",
    "publish_candidates",
    "reconcile_candidates",
    "catalog_backup",
    "commit_publish",
)
WORK_TASKS = (*WORK_CHAIN, "skip_publish", "run_record")
ALL_SUCCESS = "all_success"
EXPECTED_UPSTREAM: dict[str, frozenset[str]] = {
    WORK_CHAIN[0]: frozenset(),
    **{task: frozenset({WORK_CHAIN[index - 1]}) for index, task in enumerate(WORK_CHAIN) if index},
    "skip_publish": frozenset({"publish_branch"}),
    "run_record": frozenset({"commit_publish", "skip_publish"}),
    "catalog_release": frozenset(set(WORK_TASKS) - {"acquire_run"}),
    "final_gate": frozenset({*WORK_TASKS, "catalog_release"}),
    "release_run": frozenset({"final_gate"}),
}
EXPECTED_RULES = {"run_record": "none_failed_min_one_success", "catalog_release": "all_done", "final_gate": "all_done", "release_run": "all_done"}
PIPELINE_CHAINS = {
    "training_dag": TRAINING_STEPS,
    "model_promotion_dag": PROMOTION_STEPS,
    "silver_transform_dag": (
        "acquire_run",
        "catalog_up",
        "snapshot_project",
        "pin_bronze",
        "silver_build",
        "silver_validate",
        "verify_bronze",
        "seal_silver",
        "silver_export",
        "silver_candidates",
        "silver_reconcile",
        "silver_backup",
        "silver_commit",
        "record_silver_snapshot",
        "run_record",
    ),
    "gold_transform_dag": (
        "acquire_run",
        "catalog_up",
        "snapshot_project",
        "reserve_silver",
        "pin_silver",
        "read_silver",
        "gold_build",
        "gold_validate",
        "cross_layer_checks",
        "seal_gold",
        "gold_export",
        "publish_candidates",
        "reconcile_candidates",
        "catalog_backup",
        "commit_publish",
        "run_record",
    ),
    "gold_maintenance_dag": ("acquire_run", "catalog_up", "gold_maintenance", "run_record"),
}


def requirements(dag_id: str) -> tuple[dict[str, frozenset[str]], dict[str, str]]:
    """Return the fixed task dependencies and trigger rules for a supported pipeline."""
    if dag_id == "transform_dag":
        return EXPECTED_UPSTREAM, EXPECTED_RULES
    chain = PIPELINE_CHAINS[dag_id]
    upstream = {chain[0]: frozenset(), **{name: frozenset({chain[index - 1]}) for index, name in enumerate(chain) if index}}
    upstream.update({"catalog_release": frozenset(chain), "final_gate": frozenset((*chain, "catalog_release")), "release_run": frozenset({"final_gate"})})
    return upstream, {name: "all_done" for name in ("catalog_release", "final_gate", "release_run")}


@dataclass(frozen=True)
class TaskNode:
    """One task as the rules see it."""

    upstream: frozenset[str]
    trigger_rule: str = ALL_SUCCESS
    is_setup: bool = False
    is_teardown: bool = False


@dataclass(frozen=True)
class Violation:
    """A broken rule: its code, the task it concerns and what was found."""

    rule: str
    task: str
    detail: str


def reference_graph(dag_id: str = "transform_dag") -> dict[str, TaskNode]:
    """The section 5 graph itself; every rule accepts it."""
    expected, rules = requirements(dag_id)
    return {task: TaskNode(upstream, rules.get(task, ALL_SUCCESS)) for task, upstream in expected.items()}


def graph_from_dag(dag: Any) -> dict[str, TaskNode]:
    """The plain graph of an Airflow DAG object, read through its public attributes only."""
    graph = {}
    for task_id, task in dag.task_dict.items():
        rule = task.trigger_rule
        graph[str(task_id)] = TaskNode(
            frozenset(str(upstream) for upstream in task.upstream_task_ids),
            str(getattr(rule, "value", rule)),
            bool(getattr(task, "is_setup", False)),
            bool(getattr(task, "is_teardown", False)),
        )
    return graph


def cycle_members(graph: dict[str, TaskNode]) -> list[str]:
    """Tasks left after removing every task whose upstream tasks are all removed (Kahn); empty when acyclic."""
    remaining = {task: set(node.upstream) & set(graph) for task, node in graph.items()}
    while True:
        ready = [task for task, upstream in remaining.items() if not upstream]
        if not ready:
            return sorted(remaining)
        for task in ready:
            remaining.pop(task)
        for upstream in remaining.values():
            upstream.difference_update(ready)


def validate_transform_graph(graph: dict[str, TaskNode], dag_id: str = "transform_dag") -> list[Violation]:
    """Every section 5 rule ``graph`` breaks; an empty list means the graph is accepted."""
    expected_upstream, expected_rules = requirements(dag_id)
    violations = [
        Violation("setup_or_teardown", task, "setup task" if node.is_setup else "teardown task")
        for task, node in sorted(graph.items())
        if node.is_setup or node.is_teardown
    ]
    violations += [
        Violation("unknown_upstream", task, f"upstream {name} is not a task")
        for task, node in sorted(graph.items())
        for name in sorted(node.upstream - set(graph))
    ]
    cycle = cycle_members(graph)
    if cycle:
        violations.append(Violation("cycle", cycle[0], f"tasks in or after a cycle: {', '.join(cycle)}"))
    downstream = {upstream for node in graph.values() for upstream in node.upstream}
    leaves = sorted(set(graph) - downstream)
    if leaves != ["release_run"]:
        violations.append(Violation("leaf", ",".join(leaves) or "-", "release_run must be the only leaf"))
    violations += [Violation("missing_task", task, "required pipeline task absent") for task in expected_upstream if task not in graph]
    violations += [Violation("unexpected_task", task, "not a pipeline task") for task in sorted(set(graph) - set(expected_upstream))]
    for task, expected in expected_upstream.items():
        node = graph.get(task)
        if node is None:
            continue
        if node.upstream != expected:
            missing, extra = sorted(expected - node.upstream), sorted(node.upstream - expected)
            violations.append(Violation("upstream", task, f"missing {missing}, unexpected {extra}"))
        rule = expected_rules.get(task, ALL_SUCCESS)
        if node.trigger_rule != rule:
            violations.append(Violation("trigger_rule", task, f"{node.trigger_rule}, expected {rule}"))
    return violations
