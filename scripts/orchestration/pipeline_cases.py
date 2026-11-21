"""Cases for the first DAG (``pipeline_dag``): its stage selection, its join of every child run and its wait for the model routes after the features base.

The Airflow package is not installed here, so these cases model exactly two things. First, the Airflow API: ``Cluster`` is an in-memory stand-in of the four
Airflow 3 REST routes the DAG runtime calls (create a DAG run, read a DAG run, read a DAG, list the task instances of a run). It keeps each run's state,
moves it from queued to running to its end over fake clock ticks, never starts a run of a paused DAG and, when a run succeeds, creates the runs that the shipped
DAGs create after it (features base to the six model features runs, a model features run to its drift run, a drift run to its training run when its gate
trains, a promotion model's training run to its research run). Second, the scheduler: ``schedule`` runs the built tasks in order under the only two trigger
rules the DAG may use, ``all_success`` (a task runs when every upstream task succeeded; after a failed one it ends ``upstream_failed``) and ``all_done`` (it
always runs). The DAG uses no skip, no branch and no other rule (a case checks its file and its tasks), so no state other than success, failed and
upstream failed exists in a pipeline run and none of the rules that the real scheduler applies to a skipped task can change an outcome. The real scheduler,
the API server and the host pool are not run.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import socket
import threading
import time
from collections.abc import Callable
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import graph
from orchestration import training_contract as contract
from scripts.orchestration.airflow_api_fake import DAG_OF_MODEL, ApiServer, Cluster, token_problem
from scripts.orchestration.ecs_adapter_cases import require
from scripts.orchestration.ecs_chain_cases import SIGNING, load_dag

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = "pipeline_b0b0b0b0"
HEX = "b0b0b0b0"
STAGES = list(contract.PIPELINE_STAGES)
STAGE_DAGS = dict(contract.PIPELINE_STAGE_DAGS)
FORM = {"mode": "real", "run_kind": "bootstrap", "capture_root": "raw/", "start_from": "ingestion"}
BASE = f"features_{HEX}"


def stage_run(layer: str) -> str:
    return contract.chain_layer_run(layer, PIPELINE)


def runtime_of(loaded: dict[str, Any]) -> Any:
    return loaded["dag_runtime"]


def run_pipeline(
    cluster: Cluster, start: str = "ingestion", run_id: str = PIPELINE, extra_params: dict[str, Any] | None = None, bind: Callable[[], None] | None = None
) -> tuple[dict[str, str], dict[str, Any], dict[str, str]]:
    """Evaluate the first DAG's tasks under the scheduler model against ``cluster``: the task states, the task results and the run state of the DAG run.
    ``bind`` is what the binding task does (the cases log it; the image scenarios' check passes the real binding)."""
    loaded = load_dag("pipeline_dag.py")
    runtime = runtime_of(loaded)
    nodes = loaded["operators"]
    params = {**FORM, "run_id": run_id, "start_from": start, **(extra_params or {})}
    states: dict[str, str] = {}
    results: dict[str, Any] = {}
    steps: list[str] = []

    def run_step(template: str) -> str:
        steps.append(template)
        if bind is not None:
            bind()
        cluster.log.append(("step", template, cluster.sleeps))
        return template

    def task_states(names: tuple[str, ...]) -> dict[str, str | None]:
        return {name: states.get(name) for name in names}

    patched = {"get_current_context": lambda: {"params": params}, "run_step": run_step, "task_states": task_states}
    with (
        mock.patch.object(runtime, "airflow_call", cluster),
        mock.patch.object(runtime, "CLOCK", cluster.clock),
        mock.patch.object(runtime, "SLEEP", cluster.sleep),
        mock.patch.object(runtime, "run_step", run_step),
        mock.patch.object(runtime, "task_states", task_states),
    ):
        for node in nodes:
            function = node.function
            if function is None:
                continue
            before = [states[item.task_id] for item in node.upstream]
            rule = node.settings.get("trigger_rule") or "all_success"
            if rule != "all_done" and any(state != "success" for state in before):
                states[node.task_id] = "upstream_failed" if any(state in ("failed", "upstream_failed") for state in before) else "skipped"
                continue
            try:
                with mock.patch.dict(function.__globals__, patched):
                    results[node.task_id] = function()
                states[node.task_id] = "success"
            except Exception as error:
                results[node.task_id] = f"{type(error).__name__}: {error}"
                states[node.task_id] = "failed"
    leaves = [node.task_id for node in nodes if not any(node in other.upstream for other in nodes)]
    run_state = "failed" if any(states[leaf] in ("failed", "upstream_failed") for leaf in leaves) else "success"
    return states, results, {"run": run_state, "steps": ",".join(steps)}


def whole(cluster: Cluster, **options: Any) -> tuple[dict[str, str], dict[str, Any], dict[str, str]]:
    return run_pipeline(cluster, **options)


def first_log(cluster: Cluster, kind: str, *match: Any) -> int:
    for index, item in enumerate(cluster.log):
        if item[0] == kind and item[1 : 1 + len(match)] == match:
            return index
    return -1


OTHER_PIPELINE = "pipeline_a1a1a1a1"


def stage_conf_of(layer: str) -> dict[str, str]:
    """The settings the first DAG starts a stage's DAG with, written out here from the forms (not read from the DAG file)."""
    conf = {"run_id": stage_run(layer), "mode": "real"}
    if layer == "ingestion":
        conf |= {"run_kind": "bootstrap", "capture_root": "raw/"}
    if layer == "gold":
        conf["silver_snapshot_run"] = stage_run("silver")
    if layer == "features":
        conf["gold_snapshot_run"] = stage_run("gold")
    return conf


def seed(cluster: Cluster, layer: str, state: str, conf: Any = "right") -> tuple[str, str]:
    """A run of a stage's DAG that exists already, in ``state`` (``success``, ``failed`` or ``running``, which then ends as ``cluster`` says), started with
    ``conf`` (the settings the first DAG would use unless another value is given). Returns its key."""
    key = (STAGE_DAGS[layer], stage_run(layer))
    cluster.create(*key, "earlier", stage_conf_of(layer) if conf == "right" else conf)
    if state != "running":
        cluster.runs[key]["state"], cluster.runs[key]["left"] = state, 0
    if state == "success" and layer == "features":
        cluster.spawn(*key)
    return key


def wrong_confs(layer: str) -> dict[str, Any]:
    """Each way a run of this stage's DAG can have been started that the first DAG would not have started it: another logical run, another mode, another value
    of each other parameter of the form (the producer run of the stage before it, the ingestion form's run kind and capture root), a parameter the form of the
    first DAG does not set, a missing run ID and no settings at all."""
    right = stage_conf_of(layer)
    other = contract.chain_layer_run(layer, OTHER_PIPELINE)
    wrong: dict[str, Any] = {
        "run_id": {**right, "run_id": other},
        "mode": {**right, "mode": "fixture"},
        "run_id missing": {name: value for name, value in right.items() if name != "run_id"},
        "no settings": None,
        "extra parameter": {**right, "unexpected": "x"},
    }
    if layer == "ingestion":
        wrong |= {"run_kind": {**right, "run_kind": "monthly"}, "capture_root": {**right, "capture_root": f"ingestion_test/{stage_run('ingestion')}/"}}
    if layer in ("gold", "features"):
        producer = "silver_snapshot_run" if layer == "gold" else "gold_snapshot_run"
        before = "silver" if layer == "gold" else "gold"
        wrong[producer] = {**right, producer: contract.chain_layer_run(before, OTHER_PIPELINE)}
        wrong[f"{producer} missing"] = {name: value for name, value in right.items() if name != producer}
    return wrong


# --- the graph ---------------------------------------------------------------------------------------------------------------------------------------------


def the_pipeline_graph_has_no_skip_no_branch_and_only_the_two_exact_rules() -> None:
    loaded = load_dag("pipeline_dag.py")
    nodes = {node.task_id: node for node in loaded["operators"]}
    order = ["check_dags", "run_ingestion", "bind_code", "run_bronze", "run_silver", "run_gold", "run_features", "await_downstream", "gate"]
    require(list(nodes) == order, f"wrong tasks: {list(nodes)}")
    require(nodes["check_dags"].upstream == [], "the first task has an upstream task")
    for before, after in zip(order, order[1:], strict=False):
        require(nodes[after].upstream == [nodes[before]], f"{after} does not follow only {before}")
    rules = {name: node.settings.get("trigger_rule") or "all_success" for name, node in nodes.items()}
    require(rules.pop("gate") == "all_done" and set(rules.values()) == {"all_success"}, f"the trigger rules are {rules}: only the gate may run after a failure")
    require(all(node.settings.get("retries") == 0 for node in nodes.values()), "a task retries")
    text = (ROOT / "dags/pipeline_dag.py").read_text()
    for forbidden in ("none_failed", "NONE_FAILED", "Branch", "AirflowSkipException", "TriggerDagRunOperator", "skip_when_already_exists"):
        require(forbidden not in text, f"the first DAG file uses {forbidden}: a skipped task has rules of its own that a failure must never meet")
    settings = loaded["dag"].settings
    require(settings["dag_id"] == "pipeline_dag" == contract.PIPELINE_DAG and settings["schedule"] is None and settings["max_active_runs"] == 1, f"{settings}")
    limits = [nodes[name].settings.get("execution_timeout") for name in order if name != "gate"]
    require(all(isinstance(limit, timedelta) and limit > timedelta(0) for limit in limits), f"a task has no deadline: {limits}")
    run_limit = settings.get("dagrun_timeout")
    require(isinstance(run_limit, timedelta) and run_limit >= sum(limits, timedelta(0)), f"the run deadline does not cover its tasks: {run_limit}")


def the_pipeline_form_and_constants_match_the_contract() -> None:
    loaded = load_dag("pipeline_dag.py")
    params = loaded["dag"].settings["params"]
    require(tuple(layer for layer, _, _ in loaded["STAGES"]) == contract.PIPELINE_STAGES, "the DAG and the contract list different stages")
    require({layer: dag for layer, dag, _ in loaded["STAGES"]} == contract.PIPELINE_STAGE_DAGS, "the DAG and the contract name different stage DAGs")
    require(params["start_from"] == (("ingestion",), {"type": "string", "enum": list(contract.PIPELINE_STAGES)}), f"{params['start_from']}")
    require(params["run_id"][1]["pattern"] == f"^{contract.run_id_pattern(contract.PIPELINE_RUN)}$", f"{params['run_id']}")
    text = (ROOT / "dags/pipeline_dag.py").read_text()
    require(f'dag_id="{contract.PIPELINE_DAG}"' in text and text.count('"pipeline') == 1, "the first DAG file spells its name anywhere but its literal dag_id")
    active = contract.PIPELINE_ACTIVE_DAGS
    require(len(set(active)) == len(active) and contract.PIPELINE_DAG in active and "bronze_dag" in active, f"the activation inventory: {active}")
    shipped = {path.stem for path in (ROOT / "dags").glob("*.py")}
    require(set(active) <= shipped, f"the inventory names DAGs that are not shipped: {sorted(set(active) - shipped)}")


def graph_of(loaded: dict[str, Any]) -> dict[str, graph.TaskNode]:
    """The plain graph of a DAG file built under the stand-ins: each task's upstream set and trigger rule, as ``graph_from_dag`` reads them from Airflow."""
    return {
        node.task_id: graph.TaskNode(
            frozenset(item.task_id for item in node.upstream), str(getattr(rule := node.settings.get("trigger_rule") or "all_success", "value", rule))
        )
        for node in loaded["operators"]
    }


def the_first_dags_are_graph_checked_and_the_rules_refuse_a_broken_graph() -> None:
    """The graph rules name ``pipeline_dag`` (nine tasks in one chain, the gate the only leaf and the only task that runs after a failure) and ``bronze_dag``
    (the stage job then the load job, which is the only leaf), accept the shipped files and refuse each way of breaking either graph."""
    order = ["check_dags", "run_ingestion", "bind_code", "run_bronze", "run_silver", "run_gold", "run_features", "await_downstream", "gate"]
    for dag_id, tasks in ((contract.PIPELINE_DAG, order), ("bronze_dag", ["bronze_stage", "bronze_load"])):
        require(dag_id in graph.GRAPH_DAGS, f"{dag_id} is not graph-checked: {graph.GRAPH_DAGS}")
        reference = graph.reference_graph(dag_id)
        require(list(reference) == tasks, f"the reference graph of {dag_id} holds {list(reference)}")
        require(graph.validate_transform_graph(reference, dag_id) == [], f"the reference graph of {dag_id} is refused")
        shipped = graph_of(load_dag(f"{dag_id}.py"))
        require(
            graph.validate_transform_graph(shipped, dag_id) == [],
            f"the shipped {dag_id} breaks the graph rules: {graph.validate_transform_graph(shipped, dag_id)}",
        )
        require(shipped == reference, f"the shipped {dag_id} and its reference graph differ")

        def rules(broken: dict[str, graph.TaskNode], dag_id: str = dag_id) -> set[str]:
            return {violation.rule for violation in graph.validate_transform_graph(broken, dag_id)}

        require("missing_task" in rules({name: node for name, node in reference.items() if name != tasks[0]}), f"{dag_id} without {tasks[0]} was accepted")
        require("unexpected_task" in rules({**reference, "extra": graph.TaskNode(frozenset({tasks[-1]}))}), f"{dag_id} with an extra task was accepted")
        require("leaf" in rules({**reference, "tail": graph.TaskNode(frozenset({tasks[-1]}))}), f"{dag_id} with a tail task was accepted")
        require(
            "upstream" in rules({**reference, tasks[-1]: graph.TaskNode(frozenset({tasks[-3]} if len(tasks) > 2 else ()), reference[tasks[-1]].trigger_rule)}),
            f"{dag_id} with a skipped edge was accepted",
        )
        require("cycle" in rules({**reference, tasks[0]: graph.TaskNode(frozenset({tasks[-1]}))}), f"{dag_id} with a cycle was accepted")
    reference = graph.reference_graph(contract.PIPELINE_DAG)

    def pipeline_rules(broken: dict[str, graph.TaskNode]) -> set[str]:
        return {violation.rule for violation in graph.validate_transform_graph(broken, contract.PIPELINE_DAG)}

    require(
        "trigger_rule" in pipeline_rules({**reference, "gate": graph.TaskNode(reference["gate"].upstream, "all_success")}),
        "a gate that stops after a failure was accepted",
    )
    for name in order[:-1]:
        require(
            "trigger_rule" in pipeline_rules({**reference, name: graph.TaskNode(reference[name].upstream, "none_failed")}),
            f"{name} under none_failed was accepted: a stage must never run after a failed one",
        )
        require(
            "trigger_rule" in pipeline_rules({**reference, name: graph.TaskNode(reference[name].upstream, "all_done")}), f"{name} under all_done was accepted"
        )
    require(
        "upstream"
        in pipeline_rules(
            {**reference, "bind_code": graph.TaskNode(frozenset({"check_dags"}), "all_success"), "run_ingestion": graph.TaskNode(frozenset({"bind_code"}))}
        ),
        "the code binding before ingestion was accepted",
    )
    bronze = graph.reference_graph("bronze_dag")
    require(
        "upstream"
        in {
            violation.rule for violation in graph.validate_transform_graph({**bronze, "bronze_stage": graph.TaskNode(frozenset({"bronze_load"}))}, "bronze_dag")
        },
        "bronze jobs in the reverse order were accepted",
    )


# --- stage selection and the join --------------------------------------------------------------------------------------------------------------------------


def a_whole_run_creates_stages_in_order_and_binds_before_bronze() -> None:
    cluster = Cluster()
    states, results, summary = whole(cluster)
    require(summary["run"] == "success" and set(states.values()) == {"success"}, f"a whole run did not pass: {states} {results}")
    require(cluster.created_by("api") == [(STAGE_DAGS[layer], stage_run(layer)) for layer in STAGES], f"the stages created were {cluster.created_by('api')}")
    for before, after in zip(STAGES, STAGES[1:], strict=False):
        ended = first_log(cluster, "end", STAGE_DAGS[before], stage_run(before), "success")
        created = first_log(cluster, "create", STAGE_DAGS[after], stage_run(after))
        require(0 <= ended < created, f"{after} was created before {before} ended: {cluster.log}")
    bound, bronze = first_log(cluster, "step", "bind_code"), first_log(cluster, "create", "bronze_dag", stage_run("bronze"))
    ingestion = first_log(cluster, "end", "ingestion_dag", stage_run("ingestion"), "success")
    require(
        ingestion < bound < bronze and summary["steps"] == "bind_code", f"the binding ran at {bound}, ingestion ended at {ingestion}, bronze began at {bronze}"
    )
    conf = {item[1]: cluster.runs[(item[1], item[2])]["conf"] for item in cluster.log if item[0] == "create" and item[3] == "api"}
    require(
        conf["ingestion_dag"] == {"run_id": stage_run("ingestion"), "mode": "real", "run_kind": "bootstrap", "capture_root": "raw/"}
        and conf["bronze_dag"] == {"run_id": stage_run("bronze"), "mode": "real"}
        and conf["silver_transform_dag"] == {"run_id": stage_run("silver"), "mode": "real"}
        and conf["gold_transform_dag"] == {"run_id": stage_run("gold"), "mode": "real", "silver_snapshot_run": stage_run("silver")}
        and conf["features_base_dag"] == {"run_id": stage_run("features"), "mode": "real", "gold_snapshot_run": stage_run("gold")},
        f"a stage got the wrong settings: {conf}",
    )


def each_start_runs_exactly_the_selected_stages_and_binds_first() -> None:
    for index, start in enumerate(STAGES):
        cluster = Cluster()
        states, results, summary = whole(cluster, start=start)
        require(summary["run"] == "success" and set(states.values()) == {"success"}, f"start {start}: {states} {results}")
        selected = STAGES[index:]
        require(
            cluster.created_by("api") == [(STAGE_DAGS[layer], stage_run(layer)) for layer in selected], f"start {start}: created {cluster.created_by('api')}"
        )
        for layer in STAGES[:index]:
            require(
                "not run" in str(results[f"run_{layer}"]) and not any(item[1] == STAGE_DAGS[layer] for item in cluster.log if item[0] == "create"),
                f"start {start}: {layer} ran",
            )
        require(summary["steps"] == "bind_code", f"start {start}: the binding ran {summary['steps']!r}")
        bound = first_log(cluster, "step", "bind_code")
        first_child = first_log(
            cluster, "create", STAGE_DAGS[selected[0] if start != "ingestion" else "bronze"], stage_run(selected[0] if start != "ingestion" else "bronze")
        )
        require(0 <= bound < first_child, f"start {start}: the binding ran at {bound}, after the first selected stage began at {first_child}")
    refused = Cluster()
    for bad in ("metadata", "training", "", "Silver", "none"):
        states, results, _ = whole(refused, start=bad)
        require(
            states["check_dags"] == "failed" and "start_from" in str(results["check_dags"]) and states["run_ingestion"] == "upstream_failed",
            f"{bad!r}: {states} {results}",
        )
    require(not refused.created_by("api") and not refused.created_by("dag"), "an invalid start created a run")


def a_failed_stage_stops_the_run_and_no_later_child_exists() -> None:
    for index, layer in enumerate(STAGES):
        cluster = Cluster()
        cluster.failing.add((STAGE_DAGS[layer], stage_run(layer)))
        states, results, summary = whole(cluster)
        require(summary["run"] == "failed" and states[f"run_{layer}"] == "failed" and "failed" in str(results[f"run_{layer}"]), f"{layer}: {states} {results}")
        later = [*(["bind_code"] if index == 0 else []), *(f"run_{item}" for item in STAGES[index + 1 :]), "await_downstream"]
        for name in later:
            require(states[name] == "upstream_failed", f"{layer} failed and {name} is {states[name]}")
        require(index == 0 or states["bind_code"] == "success", f"{layer} failed and the binding is {states['bind_code']}")
        require(states["gate"] == "failed" and "run_" in str(results["gate"]), f"{layer} failed and the gate {states['gate']}: {results['gate']}")
        created = {dag for dag, _ in cluster.created_by("api")}
        require(created == {STAGE_DAGS[item] for item in STAGES[: index + 1]}, f"{layer} failed and the stages created were {sorted(created)}")
        require(not cluster.created_by("dag") or layer != "features", f"{layer} failed and runs were created after it: {cluster.created_by('dag')}")


def an_existing_child_is_joined_by_its_real_outcome_and_never_recreated() -> None:
    """The run starts at the stage whose child exists, so no earlier wait can end that child: a success is adopted, a running child is waited for until it
    ends (and the next stage begins only after that end), a failed child fails the stage and nothing after it is created."""
    for index, layer in enumerate(STAGES):
        later = {STAGE_DAGS[item] for item in STAGES[index + 1 :]}
        for existing in ("success", "running", "failed"):
            cluster = Cluster()
            key = seed(cluster, layer, existing)
            if existing == "failed":
                cluster.failing.add(key)
            states, results, summary = whole(cluster, start=layer)
            require(key not in set(cluster.created_by("api")), f"{layer}/{existing}: the existing child was created again")
            require(("POST", f"/api/v2/dags/{key[0]}/dagRuns") not in cluster.calls, f"{layer}/{existing}: the parent asked to create the run that exists")
            if existing == "failed":
                require(
                    summary["run"] == "failed" and states[f"run_{layer}"] == "failed" and "failed" in str(results[f"run_{layer}"]),
                    f"{layer}/failed: {states} {results}",
                )
                require(not later & {dag for dag, _ in cluster.created_by("api")}, f"{layer}/failed: a later child was created")
                again, _, after = whole(cluster, start=layer)
                require(
                    after["run"] == "failed" and again[f"run_{layer}"] == "failed" and not (later & {dag for dag, _ in cluster.created_by("api")}),
                    f"{layer}/failed: a repeat passed or started a later child",
                )
            else:
                require(summary["run"] == "success" and set(states.values()) == {"success"}, f"{layer}/{existing}: {states} {results}")
            if existing == "running":
                ended = first_log(cluster, "end", *key, "success")
                require(cluster.sleeps > 0 and ended >= 0, f"{layer}/running: the parent did not wait for the running child: {cluster.log}")
                if index + 1 < len(STAGES):
                    following = STAGES[index + 1]
                    created = first_log(cluster, "create", STAGE_DAGS[following], stage_run(following))
                    require(ended < created, f"{layer}/running: {following} was created before the running child ended: {cluster.log}")


def a_running_child_that_ends_failed_stops_the_run_before_the_next_stage() -> None:
    """The case that the wait for a running child has to pass: the child is running when the stage task first reads it and ends failed later, so a join that
    returns at the first read goes on to create the next stage and passes."""
    for index, layer in enumerate(STAGES):
        cluster = Cluster()
        key = seed(cluster, layer, "running")
        cluster.failing.add(key)
        states, results, summary = whole(cluster, start=layer)
        require(
            summary["run"] == "failed" and states[f"run_{layer}"] == "failed" and "failed" in str(results[f"run_{layer}"]),
            f"{layer}: a running child that failed did not fail the stage: {states} {results}",
        )
        require(cluster.runs[key]["state"] == "failed" and first_log(cluster, "end", *key, "failed") >= 0, f"{layer}: the child did not end failed")
        require(not cluster.created_by("api"), f"{layer}: a stage was created after a child that failed: {cluster.created_by('api')}")
        require(states["gate"] == "failed" and all(states[f"run_{item}"] == "upstream_failed" for item in STAGES[index + 1 :]), f"{layer}: {states}")


def an_existing_child_started_with_another_configuration_is_refused_by_name() -> None:
    """A run that exists under the stage's run ID is adopted only when it was started with the settings the first DAG would start it with. Each wrong
    configuration, for each stage and each state the child is in, fails the stage task with a message that names the DAG, the run, the parameter and the
    expected and found values, before any wait, creates no run and leaves the child as it was."""
    for index, layer in enumerate(STAGES):
        for existing in ("success", "running", "failed"):
            for name, conf in wrong_confs(layer).items():
                cluster = Cluster()
                key = seed(cluster, layer, existing, conf)
                before = dict(cluster.runs[key])
                states, results, summary = whole(cluster, start=layer)
                where = f"{layer}/{existing}/{name}"
                message = str(results[f"run_{layer}"])
                require(summary["run"] == "failed" and states[f"run_{layer}"] == "failed", f"{where}: the run was not refused: {states} {message}")
                parameter = (
                    name.removesuffix(" missing")
                    if name not in ("no settings", "extra parameter")
                    else {"no settings": "run_id", "extra parameter": "unexpected"}[name]
                )
                require(
                    key[0] in message and key[1] in message and parameter in message and "expected" in message and "found" in message,
                    f"{where}: the refusal does not name the child, the parameter and the values: {message}",
                )
                require(cluster.runs[key] == before and cluster.sleeps == 0, f"{where}: the refused child was changed or waited for")
                require(not cluster.created_by("api"), f"{where}: a run was created after the refusal: {cluster.created_by('api')}")
                require(
                    states["gate"] == "failed" and all(states[f"run_{item}"] == "upstream_failed" for item in STAGES[index + 1 :]),
                    f"{where}: the chain went on: {states}",
                )


def a_child_started_by_hand_with_the_defaults_left_out_is_adopted() -> None:
    """A run started in the Airflow form names only what differs from the defaults, and Airflow stores what was named: such a run is the same run."""
    for layer in STAGES:
        required = {name: value for name, value in stage_conf_of(layer).items() if name not in contract.DAG_PARAM_DEFAULTS[STAGE_DAGS[layer]]}
        cluster = Cluster()
        seed(cluster, layer, "success", required)
        states, results, summary = whole(cluster, start=layer)
        require(summary["run"] == "success" and set(states.values()) == {"success"}, f"{layer}: a run with the defaults left out was refused: {results}")


def the_defaults_table_is_the_forms_and_every_stage_parameter_is_set() -> None:
    """``DAG_PARAM_DEFAULTS`` holds exactly the defaults that each DAG file's form declares (read from the files), and the settings the first DAG gives each
    stage's DAG name exactly the parameters of that DAG's form: no parameter the child's behaviour depends on is left to a default or to chance."""
    for dag_id, defaults in contract.DAG_PARAM_DEFAULTS.items():
        form = load_dag(f"{dag_id}.py")["dag"].settings["params"]
        declared = {name: call[0][0] for name, call in form.items() if call[0]}
        require(declared == defaults, f"{dag_id}: the form declares the defaults {declared}, the table holds {defaults}")
    shipped = {path.stem for path in (ROOT / "dags").glob("*.py")}
    unlisted = shipped - set(contract.DAG_PARAM_DEFAULTS)
    require(
        unlisted == {"drift_live_check_dag", "model_promotion_dag", "serving_deploy_dag", "ingestion_retry_dag"},
        f"DAG files with no defaults: {sorted(unlisted)}",
    )
    for layer in STAGES:
        form = load_dag(f"{STAGE_DAGS[layer]}.py")["dag"].settings["params"]
        require(set(form) == set(stage_conf_of(layer)), f"{layer}: the form names {sorted(form)}, the first DAG sets {sorted(stage_conf_of(layer))}")
    cluster = Cluster()
    whole(cluster)
    for layer in STAGES:
        sent = cluster.runs[(STAGE_DAGS[layer], stage_run(layer))]["conf"]
        require(sent == stage_conf_of(layer), f"{layer}: the first DAG started the stage with {sent}, not {stage_conf_of(layer)}")


def a_repeated_run_of_finished_children_creates_nothing_new() -> None:
    cluster = Cluster()
    whole(cluster)
    created = len(cluster.log)
    states, _, summary = whole(cluster)
    require(summary["run"] == "success" and set(states.values()) == {"success"}, f"a repeat of a finished run: {states}")
    require(not [item for item in cluster.log[created:] if item[0] == "create"], "a repeat of a finished run created a run")


# --- the routes after the features base --------------------------------------------------------------------------------------------------------------------


def the_pipeline_waits_for_every_model_route_before_it_ends() -> None:
    cluster = Cluster()
    cluster.no_training.add("m3")
    cluster.slow[
        ("training_dag", contract.model_training_run_id(contract.model_drift_run_id(contract.model_features_run_id(stage_run("features"), "m4")), "m4"))
    ] = 9
    states, results, summary = whole(cluster)
    require(summary["run"] == "success" and set(states.values()) == {"success"}, f"{states} {results}")
    base = stage_run("features")
    for model in contract.MODELS:
        features = contract.model_features_run_id(base, model)
        drift = contract.model_drift_run_id(features)
        expected = [(DAG_OF_MODEL[model], features), ("drift_monitoring_dag", drift)]
        if model != "m3":
            training = contract.model_training_run_id(drift, model)
            expected.append(("training_dag", training))
            if model in contract.PROMOTION_MODELS:
                expected.append(("research_dag", contract.research_dag_run_id(training)))
        for dag, run in expected:
            require(cluster.runs[(dag, run)]["state"] == "success", f"{model}: {dag} {run} is {cluster.runs.get((dag, run))}")
        require(("training_dag", contract.model_training_run_id(drift, model)) in cluster.runs or model == "m3", f"{model}: no training run")
    require(
        ("training_dag", contract.model_training_run_id(contract.model_drift_run_id(contract.model_features_run_id(base, "m3")), "m3")) not in cluster.runs,
        "m3 trained",
    )
    last_end = max(index for index, item in enumerate(cluster.log) if item[0] == "end")
    require(results["await_downstream"] and first_log(cluster, "end", "research_dag") >= 0, "the route has no research end")
    parent_done = len(cluster.log)
    require(last_end < parent_done and all(run["state"] == "success" for run in cluster.runs.values()), "a child run is still open when the pipeline ended")
    require(
        not any(dag == "metadata_ingest_dag" for dag, _ in cluster.runs) and not any("metadata_ingest_dag" in path for _, path in cluster.calls),
        "the pipeline read or waited for the catalog",
    )
    require(results["gate"] == "accepted", f"the gate result is {results['gate']}")


def render_template(value: Any, params: dict[str, str]) -> Any:
    """A task setting of a DAG file with its Jinja expressions evaluated for ``params``: the three forms the DAG files use (``params.name`` and the two run ID
    functions of the contract)."""
    if not isinstance(value, str) or "{{" not in value:
        return value
    inner = re.fullmatch(r"\{\{ (.+) \}\}", value)
    if inner is None:
        raise AssertionError(f"a template with more than one expression: {value}")
    text = inner.group(1)
    if text.startswith("params."):
        return params[text.removeprefix("params.")]
    call = re.fullmatch(r"(model_features_run_id|model_drift_run_id)\(params\.run_id(?:, '(m[1-6])')?\)", text)
    if call is None:
        raise AssertionError(f"an expression the case cannot evaluate: {text}")
    if call.group(1) == "model_features_run_id":
        return contract.model_features_run_id(params["run_id"], call.group(2))
    return contract.model_drift_run_id(params["run_id"])


def the_route_settings_are_what_the_dags_send() -> None:
    """``training_contract.route_run_conf`` names the settings of each run of a model's route. It is held to the DAG files here: the base DAG's trigger of each
    model's features run, each model DAG's trigger of its drift run, the drift DAG's request for the training run and the research request of the contract."""
    base = stage_run("features")
    loaded = load_dag("features_base_dag.py")
    triggers = [node for node in loaded["operators"] if node.settings.get("trigger_dag_id")]
    require({node.settings["trigger_dag_id"] for node in triggers} == set(contract.FEATURE_DAGS), "the base DAG does not trigger each model's features DAG")
    for node in triggers:
        model = contract.FEATURE_DAGS[node.settings["trigger_dag_id"]]
        sent = {name: render_template(value, {"run_id": base, "mode": "real"}) for name, value in node.settings["conf"].items()}
        require(sent == contract.route_run_conf("features", base, model)[0], f"{model}: the base DAG sends {sent}")
        require(render_template(node.settings["trigger_run_id"], {"run_id": base}) == sent["run_id"], f"{model}: the trigger run ID is not the run's")
    for dag_id, model in contract.FEATURE_DAGS.items():
        features = contract.model_features_run_id(base, model)
        (node,) = [item for item in load_dag(f"{dag_id}.py")["operators"] if item.settings.get("trigger_dag_id")]
        sent = {name: render_template(value, {"run_id": features, "mode": "real"}) for name, value in node.settings["conf"].items()}
        require(sent == contract.route_run_conf("drift", base, model)[0], f"{model}: its features DAG sends {sent}")
    drift_dag = load_dag("drift_monitoring_dag.py")
    for model in contract.MODELS:
        features = contract.model_features_run_id(base, model)
        drift = contract.model_drift_run_id(features)
        types = ["HAI_1", "HAI_3"]
        (request,) = drift_dag["training_requests"](drift, "aws", model, features, types)
        fixed, free = contract.route_run_conf("training", base, model)
        require(request["conf"] == {**fixed, "hai_types": types} and free == ("hai_types",), f"{model}: the drift DAG requests {request['conf']}")
        if model in contract.PROMOTION_MODELS:
            (research,) = contract.research_requests(model, request["trigger_run_id"], base, "primary", contract.HAI_TYPES, "aws")
            fixed, free = contract.route_run_conf("research", base, model)
            sent = {**contract.DAG_PARAM_DEFAULTS["research_dag"], **research["conf"]}  # attempt 1 is left out of a request: the form's default is what runs
            require(all(sent[name] == value for name, value in fixed.items()), f"{model}: the research request sends {research['conf']}")
            require(set(research["conf"]) == (set(fixed) | set(free)) - {"research_attempt"}, f"{model}: the research request sets {sorted(research['conf'])}")
            require(free == ("hai_types",), f"{model}: the research settings the first DAG does not compare are {free}")


def a_route_run_started_with_other_settings_fails_the_pipeline_naming_it() -> None:
    """The first DAG waits for each model's route runs, so it reads them as it reads a stage: a run of a route that exists under the right ID but was started
    with another value of a setting that the base run and the model fix (the run it reads, the MLflow lane, the trigger, the base run) ends that route failed,
    naming the model, the step, the setting and both values, after every other route ended. A setting the DAG takes from data (the types) may differ."""
    base = stage_run("features")
    model = "m1"
    features = contract.model_features_run_id(base, model)
    drift = contract.model_drift_run_id(features)
    training = contract.model_training_run_id(drift, model)
    research = ("research_dag", contract.research_dag_run_id(training))
    keys = {"features": (DAG_OF_MODEL[model], features), "drift": ("drift_monitoring_dag", drift), "training": ("training_dag", training), "research": research}
    wrong = {
        "features": ("base_snapshot_run", contract.chain_layer_run("features", OTHER_PIPELINE)),
        "drift": ("mlflow", "local"),
        "training": ("trigger", "manual"),
        "research": ("base_run", contract.chain_layer_run("features", OTHER_PIPELINE)),
    }
    for step, (name, value) in wrong.items():
        cluster = Cluster()
        fixed, _ = contract.route_run_conf(step, base, model)
        cluster.create(*keys[step], "earlier", {**fixed, name: value})
        states, results, summary = whole(cluster, start="features")
        message = str(results["await_downstream"])
        require(
            summary["run"] == "failed" and states["await_downstream"] == "failed" and states["gate"] == "failed",
            f"{step}: the route run was reused: {states} {message}",
        )
        require(
            f"{model} {step}" in message
            and keys[step][1] in message
            and name in message
            and repr(value) in message
            and "expected" in message
            and "found" in message,
            f"{step}: the refusal names too little: {message}",
        )
        others = [item for item in contract.MODELS if item != model]
        require(
            all(cluster.runs[(DAG_OF_MODEL[item], contract.model_features_run_id(base, item))]["state"] == "success" for item in others),
            f"{step}: the other routes were not followed to their end",
        )
    for step, free in (
        ("training", {"hai_types": ["HAI_1"]}),
        ("research", {"hai_types": ["HAI_2"]}),
        ("research", {"hai_types": ["HAI_2"], "research_attempt": 1}),
    ):
        cluster = Cluster()
        fixed, _ = contract.route_run_conf(step, base, model)
        cluster.create(*keys[step], "earlier", {**fixed, **free})
        states, results, summary = whole(cluster, start="features")
        require(summary["run"] == "success", f"{step}: a setting the DAG takes from data stopped the run: {states} {results}")


def route_of(model: str = "m1") -> dict[str, tuple[str, str]]:
    """The run of each step of one model's route after the features base of the whole pipeline run: its DAG and its run ID."""
    base = stage_run("features")
    features = contract.model_features_run_id(base, model)
    drift = contract.model_drift_run_id(features)
    training = contract.model_training_run_id(drift, model)
    return {
        "features": (DAG_OF_MODEL[model], features),
        "drift": ("drift_monitoring_dag", drift),
        "training": ("training_dag", training),
        "research": ("research_dag", contract.research_dag_run_id(training)),
    }


TRIGGER_OF = {"training": ("drift", "trigger_training"), "research": ("training", "trigger_research")}


def child_conf(step: str, model: str = "m1") -> dict[str, Any]:
    """The settings the DAG before a route step sends with its trigger (the form's types included), as a child that the trigger made would hold them."""
    base = stage_run("features")
    if step == "training":
        fixed, _ = contract.route_run_conf("training", base, model)
        return {**fixed, "hai_types": list(contract.HAI_TYPES)}
    (request,) = contract.research_requests(model, route_of(model)["training"][1], base, "primary", contract.HAI_TYPES, "aws")
    return dict(request["conf"])


def duplicate_child(cluster: Cluster, step: str, state: str, conf: dict[str, Any] | None = None, ticks: int | None = None) -> tuple[str, str]:
    """A training or research run of ``m1`` that exists before the DAG before it tries to trigger it, so that the trigger finds a duplicate. ``state`` is
    ``failed``, or ``running`` (it then ends as ``cluster`` says after ``ticks`` polls)."""
    key = route_of()[step]
    if ticks is not None:
        cluster.slow[key] = ticks
    cluster.create(*key, "earlier", child_conf(step) if conf is None else conf)
    if state == "failed":
        cluster.runs[key]["state"], cluster.runs[key]["left"] = "failed", 0
    return key


def require_duplicate_untouched(cluster: Cluster, step: str, key: tuple[str, str], conf: dict[str, Any], label: str) -> None:
    """The trigger task of the DAG before the step ended skipped on a run that exists (a duplicate, one mapped instance with its map index 0: not a trigger
    that mapped no request), and the child was neither created again nor reset."""
    dag, run = route_of()[TRIGGER_OF[step][0]]
    task = TRIGGER_OF[step][1]
    require(cluster.task_states[(dag, run, task)] == ["skipped"] and (dag, run, task) not in cluster.unmapped, f"{label}: the trigger was not a duplicate")
    require(
        sum(1 for item in cluster.log if item[0] == "create" and item[1:3] == key) == 1 and cluster.runs[key]["conf"] == conf,
        f"{label}: the child was created again or changed",
    )


def each_step(check: Callable[[str], None]) -> None:
    """Run ``check`` for the training step and the research step, and fail with what each one found, so that neither step hides the other's failure."""
    problems = []
    for step in ("training", "research"):
        try:
            check(step)
        except AssertionError as error:
            problems.append(str(error)[:400])
    require(not problems, " | ".join(problems))


def a_skipped_duplicate_trigger_of_a_failed_child_fails_its_route() -> None:
    """A trigger that finds its child run already there ends skipped, as a trigger that has no request does. The pipeline tells them apart: a duplicate is
    followed to the child, and a child that failed fails its route (and so the pipeline) after every route ended, naming the model, the step and the run."""

    def check(step: str) -> None:
        cluster = Cluster()
        key = duplicate_child(cluster, step, "failed")
        states, results, summary = whole(cluster, start="features")
        message = str(results["await_downstream"])
        require(
            summary["run"] == "failed" and states["await_downstream"] == "failed" and states["gate"] == "failed",
            f"{step}: the pipeline passed over a failed child: {states} {message}",
        )
        require(f"m1 {step}" in message and key[1] in message and "failed" in message, f"{step}: the failure names too little: {message}")
        require_duplicate_untouched(cluster, step, key, child_conf(step), step)
        others = [item for item in contract.MODELS if item != "m1"]
        require(
            all(cluster.runs[(DAG_OF_MODEL[item], contract.model_features_run_id(stage_run("features"), item))]["state"] == "success" for item in others),
            f"{step}: the other routes were not followed to their end",
        )

    each_step(check)


def a_skipped_duplicate_trigger_of_a_running_child_waits_for_its_end() -> None:
    """A child that is still running when the DAG before it finds it is waited for: the pipeline succeeds only after the child succeeded and fails when it
    ends failed. A child that never ends fails the routes' wait at its limit (30 hours of polls), and the child is left as it is."""

    def check(step: str) -> None:
        cluster = Cluster()
        key = duplicate_child(cluster, step, "running", ticks=40)
        states, results, summary = whole(cluster, start="features")
        require(summary["run"] == "success" and set(states.values()) == {"success"}, f"{step}: the wait for a running child failed: {states} {results}")
        require(cluster.runs[key]["state"] == "success", f"{step}: the pipeline ended while its child was {cluster.runs[key]['state']}")
        require(first_log(cluster, "end", *key) >= 0 and cluster.sleeps >= 40, f"{step}: the pipeline did not wait for the child")
        require_duplicate_untouched(cluster, step, key, child_conf(step), step)

        cluster = Cluster()
        cluster.failing.add(route_of()[step])
        key = duplicate_child(cluster, step, "running", ticks=40)
        states, results, summary = whole(cluster, start="features")
        message = str(results["await_downstream"])
        require(summary["run"] == "failed" and states["await_downstream"] == "failed", f"{step}: a running child that failed passed: {states} {message}")
        require(f"m1 {step}" in message and key[1] in message and "failed" in message, f"{step}: the failure names too little: {message}")
        require(not [item for item in cluster.runs.values() if item["state"] in ("queued", "running")], f"{step}: the pipeline ended with an open run")

        cluster = Cluster()
        key = duplicate_child(cluster, step, "running", ticks=10**6)
        states, results, summary = whole(cluster, start="features")
        message = str(results["await_downstream"])
        require(summary["run"] == "failed" and states["await_downstream"] == "failed", f"{step}: a child that never ends passed: {states} {message}")
        require("still running after" in message and f"m1 ({step} run {key[1]}" in message, f"{step}: the wait failure names too little: {message}")
        require(cluster.now >= 30 * 3600 and cluster.runs[key]["state"] == "running", f"{step}: the wait ended before its limit or the child changed")
        require_duplicate_untouched(cluster, step, key, child_conf(step), step)

    each_step(check)


def a_duplicate_trigger_of_a_child_with_other_settings_fails_its_route() -> None:
    """The check of a route run's settings holds for a child that the trigger found instead of created: a training run of another trigger and a research run
    of another base run fail their route naming the model, the step, the setting and both values, and are not reused, reset or created again."""
    other = contract.chain_layer_run("features", OTHER_PIPELINE)
    wrong = {"training": ("trigger", "manual"), "research": ("base_run", other)}

    def check(step: str) -> None:
        name, value = wrong[step]
        cluster = Cluster()
        conf = {**child_conf(step), name: value}
        key = duplicate_child(cluster, step, "failed" if step == "research" else "running", conf)
        states, results, summary = whole(cluster, start="features")
        message = str(results["await_downstream"])
        require(
            summary["run"] == "failed" and states["await_downstream"] == "failed" and states["gate"] == "failed",
            f"{step}: the child with other settings was reused: {states} {message}",
        )
        require(
            f"m1 {step}" in message and key[1] in message and name in message and repr(value) in message and "expected" in message and "found" in message,
            f"{step}: the refusal names too little: {message}",
        )
        require_duplicate_untouched(cluster, step, key, conf, step)

    each_step(check)


def a_research_run_with_another_population_lane_or_attempt_is_refused() -> None:
    """The population, the MLflow lane and the attempt of a research run are fixed by the training route and the request (``primary``, ``aws``, attempt 1,
    which a form leaves out), so a research run that exists with another value of one fails its route by name. Only the types are the DAG's own choice."""
    problems = []
    for name, value in (("population", "sensitivity"), ("mlflow", "local"), ("research_attempt", 2)):
        cluster = Cluster()
        key = duplicate_child(cluster, "research", "running", {**child_conf("research"), name: value})
        states, results, summary = whole(cluster, start="features")
        message = str(results["await_downstream"])
        if summary["run"] != "failed" or states["await_downstream"] != "failed" or states["gate"] != "failed":
            problems.append(f"{name}: a research run with another value was reused")
        elif not ("m1 research" in message and key[1] in message and f"{name}: expected" in message and f"found {value!r}" in message):
            problems.append(f"{name}: the refusal names too little: {message[:300]}")
    require(not problems, " | ".join(problems))


def a_run_that_does_not_say_whether_it_started_the_next_run_fails_its_route() -> None:
    """A drift run says by its ``trigger_training`` task whether the gate trained, and a training run by its ``trigger_research`` task whether it started the
    research. A run whose task listing lacks that task cannot be told from one that started nothing, so its route fails and says so."""
    base = stage_run("features")
    features = contract.model_features_run_id(base, "m1")
    drift = contract.model_drift_run_id(features)
    training = contract.model_training_run_id(drift, "m1")
    for step, key in (("drift", ("drift_monitoring_dag", drift, "trigger_training")), ("training", ("training_dag", training, "trigger_research"))):
        cluster = Cluster()
        cluster.silent.add(key)
        states, results, summary = whole(cluster)
        message = str(results["await_downstream"])
        require(summary["run"] == "failed" and states["await_downstream"] == "failed", f"{step}: the pipeline passed: {states} {message}")
        require(f"m1 {step}" in message and "did not say whether" in message and key[2] in message, f"{step}: the failure names too little: {message}")


def a_failure_on_a_model_route_fails_the_pipeline_after_every_route_ended() -> None:
    base = stage_run("features")

    def run_of(model: str, step: str) -> tuple[str, str]:
        features = contract.model_features_run_id(base, model)
        drift = contract.model_drift_run_id(features)
        training = contract.model_training_run_id(drift, model)
        return {
            "features": (DAG_OF_MODEL[model], features),
            "drift": ("drift_monitoring_dag", drift),
            "training": ("training_dag", training),
            "research": ("research_dag", contract.research_dag_run_id(training)),
        }[step]

    for model, step in (("m2", "features"), ("m1", "drift"), ("m4", "training"), ("m2", "research")):
        cluster = Cluster()
        cluster.failing.add(run_of(model, step))
        states, results, summary = whole(cluster)
        require(summary["run"] == "failed" and states["await_downstream"] == "failed" and states["gate"] == "failed", f"{model}/{step}: {states}")
        require(
            model in str(results["await_downstream"]) and step in str(results["await_downstream"]),
            f"{model}/{step}: the failure does not name the route: {results['await_downstream']}",
        )
        other = [key for key in cluster.runs if cluster.runs[key]["state"] in ("queued", "running")]
        require(not other, f"{model}/{step}: the pipeline ended with open runs {other}")
        require(
            sum(1 for key in cluster.runs if key[0] == "drift_monitoring_dag") == 6 or step == "features", f"{model}/{step}: other routes were not followed"
        )
    cluster = Cluster()
    cluster.failing.add(run_of("m1", "research"))
    cluster.failing.add(run_of("m3", "drift"))
    states, results, _ = whole(cluster)
    require("m1" in str(results["await_downstream"]) and "m3" in str(results["await_downstream"]), f"two failed routes: {results['await_downstream']}")


def a_model_route_that_never_appears_or_never_ends_fails_visibly() -> None:
    base = stage_run("features")
    cluster = Cluster()
    model = "m5"
    features = contract.model_features_run_id(base, model)
    orig = cluster.spawn

    def withhold(dag: str, run: str) -> None:
        if dag == "features_base_dag":
            for item in contract.MODELS:
                if item != model:
                    cluster.create(DAG_OF_MODEL[item], contract.model_features_run_id(run, item), "dag")
            return
        orig(dag, run)

    with mock.patch.object(cluster, "spawn", withhold):
        states, results, summary = whole(cluster)
    require(summary["run"] == "failed" and "never created" in str(results["await_downstream"]) and features in str(results["await_downstream"]), f"{results}")
    slow = Cluster()
    slow.slow[(DAG_OF_MODEL["m6"], contract.model_features_run_id(base, "m6"))] = 10**6
    states, results, summary = whole(slow)
    require(summary["run"] == "failed" and "still running" in str(results["await_downstream"]) and "m6" in str(results["await_downstream"]), f"{results}")
    stuck = Cluster()
    stuck.slow[("silver_transform_dag", stage_run("silver"))] = 10**6
    states, results, summary = whole(stuck)
    require(states["run_silver"] == "failed" and "still" in str(results["run_silver"]) and stage_run("silver") in str(results["run_silver"]), f"{results}")
    require("gold_transform_dag" not in {dag for dag, _ in stuck.created_by("api")}, "gold was created after a silver that never ended")


# --- activation ---------------------------------------------------------------------------------------------------------------------------------------------


def a_paused_or_missing_dag_fails_the_pipeline_before_any_child_starts() -> None:
    inventory = [dag for dag in contract.PIPELINE_ACTIVE_DAGS if dag != contract.PIPELINE_DAG]
    cluster = Cluster()
    cluster.paused.update(inventory)
    states, results, summary = whole(cluster)
    require(summary["run"] == "failed" and states["check_dags"] == "failed" and states["run_ingestion"] == "upstream_failed", f"{states}")
    named = str(results["check_dags"])
    require(all(dag in named for dag in inventory if dag != "metadata_ingest_dag"), f"the failure does not name every paused DAG: {named}")
    require(not cluster.created_by("api"), "a child was created under a paused DAG")
    one = Cluster()
    one.paused.add("bronze_dag")
    states, results, _ = whole(one)
    require(
        states["check_dags"] == "failed"
        and "bronze_dag" in str(results["check_dags"])
        and "paused" in str(results["check_dags"])
        and not one.created_by("api"),
        f"{states} {results}",
    )
    gone = Cluster()
    gone.unregistered.add("silver_transform_dag")
    states, results, _ = whole(gone)
    require(
        states["check_dags"] == "failed" and "silver_transform_dag" in str(results["check_dags"]) and "not registered" in str(results["check_dags"]),
        f"{results}",
    )
    optional = Cluster()
    optional.paused.add("metadata_ingest_dag")
    states, _, summary = whole(optional)
    require(summary["run"] == "success", f"a paused catalog DAG stopped the pipeline: {states}")
    late = Cluster()
    late.paused.add("gold_transform_dag")
    runtime = runtime_of(load_dag("pipeline_dag.py"))
    with mock.patch.object(runtime, "airflow_call", late), mock.patch.object(runtime, "CLOCK", late.clock), mock.patch.object(runtime, "SLEEP", late.sleep):
        try:
            runtime.join_child("gold_transform_dag", stage_run("gold"), stage_conf_of("gold"), timedelta(hours=1))
        except runtime.ChildRunFailed as error:
            require("paused" in str(error), f"{error}")
        else:
            raise AssertionError("a child was created under a DAG that was paused after the check")
        late.paused.clear()
        late.create("gold_transform_dag", stage_run("gold"), "earlier", stage_conf_of("gold"))
        late.paused.add("gold_transform_dag")
        try:
            runtime.join_child("gold_transform_dag", stage_run("gold"), stage_conf_of("gold"), timedelta(hours=1))
        except runtime.ChildRunFailed as error:
            require("paused" in str(error) and late.sleeps == 0, f"{error}")
        else:
            raise AssertionError("a queued child of a paused DAG was waited for")
    require(not late.created_by("api"), "a run was created under a paused DAG")


def the_api_stand_in_serves_signed_requests_and_refuses_a_wrong_secret() -> None:
    """The loopback server of the real-image DAG runs: the runtime's own request code joins a stage and a model route through it, and a request signed with
    another secret, a token for another audience or an expired token is refused with 401 before the cluster sees it."""
    runtime = runtime_of(load_dag("pipeline_dag.py"))
    cluster = Cluster()
    with ApiServer(cluster, SIGNING) as api, mock.patch.dict("os.environ", {"AIRFLOW_API_PORT": str(api.port), "AIRFLOW__API_AUTH__JWT_SECRET": SIGNING}):
        with mock.patch.object(runtime, "CLOCK", cluster.clock), mock.patch.object(runtime, "SLEEP", cluster.sleep):
            conf = {"run_id": stage_run("features"), "mode": "real"}
            message = runtime.join_child("features_base_dag", stage_run("features"), conf, timedelta(hours=1))
            require("succeeded" in message and cluster.created_by("api") == [("features_base_dag", stage_run("features"))], f"{message} {cluster.log}")
            routes = runtime.join_model_routes(stage_run("features"), timedelta(hours=30))
            require("m1" in routes and not cluster.created_by("api")[1:], f"the routes were not joined: {routes}")
        served = [item for item in api.requests if item[2] == 401]
        require(not served and any(item[0] == "POST" and item[2] == 200 for item in api.requests), f"{api.requests}")
        before = len(cluster.calls)
        with mock.patch.dict("os.environ", {"AIRFLOW__API_AUTH__JWT_SECRET": "another-secret"}):
            status, _ = runtime.airflow_call("GET", "/api/v2/dags/bronze_dag")
        require(status == 401 and len(cluster.calls) == before, f"a request signed with another secret reached the cluster: {status}")
        for header in ("", "Bearer", "Basic abc", "Bearer a.b.c"):
            require(token_problem(header, SIGNING, time.time()) is not None, f"the header {header!r} was accepted")
        now = time.time()
        claims = {"sub": "admin", "aud": "apache-airflow", "iat": int(now), "nbf": int(now) - 5, "exp": int(now) + 120}

        def signed(body: dict[str, Any], algorithm: str = "HS512") -> str:
            def part(value: dict[str, Any]) -> str:
                return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()

            head, content = part({"alg": algorithm, "typ": "JWT"}), part(body)
            digest = hmac.new(SIGNING.encode(), f"{head}.{content}".encode(), hashlib.sha512).digest()
            return f"Bearer {head}.{content}.{base64.urlsafe_b64encode(digest).rstrip(b'=').decode()}"

        require(token_problem(signed(claims), SIGNING, now) is None, "a valid token was refused")
        require(token_problem(signed({**claims, "aud": "other"}), SIGNING, now) is not None, "a token for another audience was accepted")
        require(token_problem(signed({**claims, "exp": int(now) - 1}), SIGNING, now) is not None, "an expired token was accepted")
        require(token_problem(signed(claims, "HS256"), SIGNING, now) is not None, "another algorithm was accepted")


def the_bronze_request_the_image_run_stages_passes_the_launch_check() -> None:
    """The bronze DAG's load job launches only on a staged ``bronze_request.json`` that the launch check accepts (``ecs_preflight.bronze_request_reader``). The
    image run does not run the stage job, so its scenario stages that request itself: it must be the shape the stage phase writes, or the DAG fails in the image
    at ``bronze_load`` with a refusal that no pipeline case shows. The request of every bronze DAG step is read here by the launch check, and the key sets the
    stage phase writes and the launch accepts are the same."""
    from orchestration import ecs_preflight
    from scripts.lakehouse import bronze_run
    from scripts.orchestration import real_image_dags as image
    from scripts.orchestration.ecs_run_fixtures import collections_of_table_map

    require(
        bronze_run.REQUEST_KEYS == ecs_preflight.BRONZE_REQUEST_KEYS,
        f"the stage phase writes {bronze_run.REQUEST_KEYS}, the launch accepts {ecs_preflight.BRONZE_REQUEST_KEYS}",
    )
    modes = image.rig_modules().cases
    bucket = modes.BUCKET
    steps = [
        (name, step, step.prepare)
        for name, scenario in image.cases_catalog().items()
        for step in scenario.steps
        if step.dag_id == "bronze_dag" and step.prepare is not None
    ]
    require(bool(steps), "no image scenario stages a bronze request for the bronze DAG")
    for name, step, prepare in steps:
        rig = image.cases_catalog()[name].rig()
        prepare(rig)
        reader = ecs_preflight.bronze_request_reader(rig.stage, bucket, collections_of_table_map())
        try:
            values = reader(step.params["run_id"])
        except Exception as error:
            raise AssertionError(f"{name}: the launch check refused the bronze request that the image run stages: {type(error).__name__}: {error}") from None
        require(values["BRONZE_TARGET"] == "real" and not values["BRONZE_DRY_RUN"], f"{name}: the checked values are {values}")
        # The DAG's two tasks over the same rig, through the shipped step runner: the stage job and the load job each launch once.
        with modes.world(rig, "bronze_dag", dict(step.params)) as current:
            states = {task: current.step(task) for task in ("bronze_stage", "bronze_load")}
        sent = len([call for call in rig.ecs.calls if call[0] == "run_task"])
        require(
            states == {"bronze_stage": "succeeded", "bronze_load": "succeeded"} and sent == 2,
            f"{name}: the bronze DAG tasks ended {states} with {sent} RunTask calls",
        )
        if step.check is not None:
            step.check(rig, {"run_task_calls": sent})  # the launch records of both jobs, as the image run checks them


def the_image_scenarios_of_the_first_dag_hold_against_the_stand_in_run() -> None:
    """Every first-DAG step of the real-image scenarios (``real_image_dags.py``) is run here under the scheduler model of this file, against the rig's own
    code binding: the task states, the run state and the checks of the API must hold, and a step run with another start or another failing stage must break
    its checks. The image runs the same steps through Airflow's test runner, so a step that is vacuous or contradicts the DAG shows here first."""
    from scripts.orchestration import real_image_dags as image

    steps = [
        (name, step)
        for name, scenario in image.cases_catalog().items()
        if name.startswith("the_first_dag") or name == "every_host_dag_runs_end_to_end"
        for step in scenario.steps
        if step.dag_id == contract.PIPELINE_DAG
    ]
    require(len(steps) == 39, f"the image scenarios hold {len(steps)} first-DAG steps, expected 39: {[name for name, _ in steps]}")

    def play(name: str, step: Any, **changes: Any) -> None:
        scenario = image.cases_catalog()[name]
        rig = scenario.rig()
        if step.prepare is not None:
            step.prepare(rig)
        api = step.api(rig)
        run = step.params["run_id"]
        options = {"start": step.params["start_from"], **changes}
        if "failing" in options:
            api.failing = {key for key in options.pop("failing")}
        states, results, summary = run_pipeline(api, run_id=run, bind=lambda: rig.preflight.bind_pipeline("pipeline_gate", run), **options)
        wanted = dict(step.states) if step.states else dict.fromkeys(states, "success")
        require(states == wanted, f"{name}: task states {states}, expected {wanted}: {results}")
        require(summary["run"] == step.run_state, f"{name}: the run ended {summary['run']}, not {step.run_state}")
        step.api_after(rig, api)

    for name, step in steps:
        play(name, step)
    by_name = dict(steps)
    broken = (("the_first_dag_starts_from_silver", {"start": "bronze"}), ("the_first_dag_starts_from_gold", {"start": "silver"}))
    for name, options in broken:
        try:
            play(name, by_name[name], **options)
        except AssertionError:
            continue
        raise AssertionError(f"{name} passed with another start: its checks do not look at the stages created")
    try:
        play("every_host_dag_runs_end_to_end", by_name["every_host_dag_runs_end_to_end"], failing=[("silver_transform_dag", stage_run("silver"))])
    except AssertionError:
        return
    raise AssertionError("the whole run passed with a failing silver stage: its checks do not look at the outcome")


# --- the REST helpers ---------------------------------------------------------------------------------------------------------------------------------------


class Rest:
    """A loopback server that answers the four Airflow routes from a table and records each request."""

    def __init__(self, answers: dict[tuple[str, str], tuple[int, Any]]) -> None:
        self.answers = answers
        self.requests: list[dict[str, Any]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def handle_request(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"null")
                outer.requests.append({"method": self.command, "path": self.path, "authorization": self.headers.get("Authorization", ""), "body": body})
                status, payload = outer.answers.get((self.command, self.path), (404, {}))
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = handle_request

            def log_message(self, *_: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> Rest:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])


def the_runtime_reads_runs_pauses_and_task_states_through_the_signed_rest_api() -> None:
    runtime = runtime_of(load_dag("pipeline_dag.py"))
    answers: dict[tuple[str, str], tuple[int, Any]] = {
        ("GET", "/api/v2/dags/bronze_dag/dagRuns/bronze_b0b0b0b0"): (200, {"dag_run_id": "bronze_b0b0b0b0", "state": "failed"}),
        ("GET", "/api/v2/dags/bronze_dag/dagRuns/bronze_b1b1b1b1"): (404, {"detail": "not found"}),
        ("GET", "/api/v2/dags/bronze_dag"): (200, {"dag_id": "bronze_dag", "is_paused": True}),
        ("GET", "/api/v2/dags/gold_transform_dag"): (404, {}),
        ("GET", "/api/v2/dags/drift_monitoring_dag/dagRuns/drift_ols_b0b0b0b0/taskInstances?limit=100"): (
            200,
            {
                "task_instances": [
                    {"task_id": "trigger_training", "state": "skipped", "map_index": -1},
                    {"task_id": "decide", "state": "success", "map_index": -1},
                ],
                "total_entries": 2,
            },
        ),
        ("GET", "/api/v2/dags/training_dag/dagRuns/training_ols_b0b0b0b0/taskInstances?limit=100"): (
            200,
            {
                "task_instances": [
                    {"task_id": "trigger_research", "state": "success", "map_index": 0},
                    {"task_id": "trigger_research", "state": "skipped", "map_index": 1},
                ],
                "total_entries": 2,
            },
        ),
        ("GET", "/api/v2/dags/drift_monitoring_dag/dagRuns/drift_spatial_b0b0b0b0/taskInstances?limit=100"): (
            200,
            {"task_instances": [{"task_id": "trigger_training", "state": "skipped", "map_index": 0}], "total_entries": 1},
        ),
        ("GET", "/api/v2/dags/drift_monitoring_dag/dagRuns/drift_trend_b0b0b0b0/taskInstances?limit=100"): (
            200,
            {"task_instances": [{"task_id": "trigger_training", "state": "skipped"}], "total_entries": 1},
        ),
        ("GET", "/api/v2/dags/training_dag/dagRuns/training_spatial_b0b0b0b0/taskInstances?limit=100"): (200, {"task_instances": [], "total_entries": 5}),
        ("POST", "/api/v2/dags/bronze_dag/dagRuns"): (409, {}),
    }
    environment = {"AIRFLOW_API_PORT": "0", "AIRFLOW__API_AUTH__JWT_SECRET": SIGNING}
    with Rest(answers) as api, mock.patch.dict("os.environ", {**environment, "AIRFLOW_API_PORT": str(api.port)}):
        require(runtime.dag_run_state("bronze_dag", "bronze_b0b0b0b0") == "failed", "a failed run was not read as failed")
        require(runtime.dag_run_state("bronze_dag", "bronze_b1b1b1b1") is None, "an absent run was not read as absent")
        require(runtime.dag_is_paused("bronze_dag") is True, "a paused DAG was not read as paused")
        try:
            runtime.dag_is_paused("gold_transform_dag")
        except RuntimeError as error:
            require("not registered" in str(error), f"{error}")
        else:
            raise AssertionError("an unregistered DAG was read as active")
        require(runtime.task_outcome("drift_monitoring_dag", "drift_ols_b0b0b0b0", "trigger_training") == "skipped", "a skipped mapped task")
        require(
            runtime.task_outcome("training_dag", "training_ols_b0b0b0b0", "trigger_research") == "success",
            "a mapped task with a success and a skipped instance",
        )
        require(runtime.task_outcome("drift_monitoring_dag", "drift_spatial_b0b0b0b0", "trigger_training") == "duplicate", "a skipped mapped request")
        try:
            runtime.task_outcome("drift_monitoring_dag", "drift_trend_b0b0b0b0", "trigger_training")
        except RuntimeError as error:
            require("map index" in str(error), f"{error}")
        else:
            raise AssertionError("a skipped instance without its map index was read as an outcome")
        require(runtime.task_outcome("drift_monitoring_dag", "drift_ols_b0b0b0b0", "decide") == "success", "a plain task")
        require(runtime.task_outcome("drift_monitoring_dag", "drift_ols_b0b0b0b0", "absent_task") is None, "an absent task")
        try:
            runtime.task_outcome("training_dag", "training_spatial_b0b0b0b0", "trigger_research")
        except RuntimeError as error:
            require("incomplete" in str(error), f"{error}")
        else:
            raise AssertionError("an incomplete task list was accepted")
        require(
            runtime.create_dag_run("bronze_dag", "bronze_b0b0b0b0", {"run_id": "bronze_b0b0b0b0", "mode": "real"}) is False,
            "a 409 was not read as an existing run",
        )
    header, claims, signature = api.requests[0]["authorization"].removeprefix("Bearer ").split(".")
    payload = json.loads(base64.urlsafe_b64decode(claims + "=" * (-len(claims) % 4)))
    expected = base64.urlsafe_b64encode(hmac.new(SIGNING.encode(), f"{header}.{claims}".encode(), hashlib.sha512).digest()).rstrip(b"=").decode()
    require(signature == expected and payload.get("aud") == "apache-airflow", "the token is not signed with the Airflow JWT secret")
    require(all(item["authorization"].startswith("Bearer ") for item in api.requests), "a request went out unsigned")
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    free = sock.getsockname()[1]
    sock.close()
    with mock.patch.dict("os.environ", {**environment, "AIRFLOW_API_PORT": str(free)}):
        try:
            runtime.dag_run_state("bronze_dag", "bronze_b0b0b0b0")
        except OSError:
            pass
        else:
            raise AssertionError("an unreachable API was read as an absent run")
    with mock.patch.dict("os.environ", {"AIRFLOW_API_PORT": "1"}, clear=True):
        try:
            runtime.dag_run_state("bronze_dag", "bronze_b0b0b0b0")
        except RuntimeError as error:
            require("JWT secret" in str(error), f"{error}")
        else:
            raise AssertionError("a missing secret was accepted")


CASES: dict[str, Callable[[], None]] = {
    "a_run_that_does_not_say_whether_it_started_the_next_run_fails_its_route": a_run_that_does_not_say_whether_it_started_the_next_run_fails_its_route,
    "the_route_settings_are_what_the_dags_send": the_route_settings_are_what_the_dags_send,
    "a_skipped_duplicate_trigger_of_a_failed_child_fails_its_route": a_skipped_duplicate_trigger_of_a_failed_child_fails_its_route,
    "a_skipped_duplicate_trigger_of_a_running_child_waits_for_its_end": a_skipped_duplicate_trigger_of_a_running_child_waits_for_its_end,
    "a_duplicate_trigger_of_a_child_with_other_settings_fails_its_route": a_duplicate_trigger_of_a_child_with_other_settings_fails_its_route,
    "a_research_run_with_another_population_lane_or_attempt_is_refused": a_research_run_with_another_population_lane_or_attempt_is_refused,
    "a_route_run_started_with_other_settings_fails_the_pipeline_naming_it": a_route_run_started_with_other_settings_fails_the_pipeline_naming_it,
    "the_defaults_table_is_the_forms_and_every_stage_parameter_is_set": the_defaults_table_is_the_forms_and_every_stage_parameter_is_set,
    "a_child_started_by_hand_with_the_defaults_left_out_is_adopted": a_child_started_by_hand_with_the_defaults_left_out_is_adopted,
    "an_existing_child_started_with_another_configuration_is_refused_by_name": an_existing_child_started_with_another_configuration_is_refused_by_name,
    "a_running_child_that_ends_failed_stops_the_run_before_the_next_stage": a_running_child_that_ends_failed_stops_the_run_before_the_next_stage,
    "the_first_dags_are_graph_checked_and_the_rules_refuse_a_broken_graph": the_first_dags_are_graph_checked_and_the_rules_refuse_a_broken_graph,
    "the_pipeline_graph_has_no_skip_no_branch_and_only_the_two_exact_rules": the_pipeline_graph_has_no_skip_no_branch_and_only_the_two_exact_rules,
    "the_pipeline_form_and_constants_match_the_contract": the_pipeline_form_and_constants_match_the_contract,
    "a_whole_run_creates_stages_in_order_and_binds_before_bronze": a_whole_run_creates_stages_in_order_and_binds_before_bronze,
    "each_start_runs_exactly_the_selected_stages_and_binds_first": each_start_runs_exactly_the_selected_stages_and_binds_first,
    "a_failed_stage_stops_the_run_and_no_later_child_exists": a_failed_stage_stops_the_run_and_no_later_child_exists,
    "an_existing_child_is_joined_by_its_real_outcome_and_never_recreated": an_existing_child_is_joined_by_its_real_outcome_and_never_recreated,
    "a_repeated_run_of_finished_children_creates_nothing_new": a_repeated_run_of_finished_children_creates_nothing_new,
    "the_pipeline_waits_for_every_model_route_before_it_ends": the_pipeline_waits_for_every_model_route_before_it_ends,
    "a_failure_on_a_model_route_fails_the_pipeline_after_every_route_ended": a_failure_on_a_model_route_fails_the_pipeline_after_every_route_ended,
    "a_model_route_that_never_appears_or_never_ends_fails_visibly": a_model_route_that_never_appears_or_never_ends_fails_visibly,
    "a_paused_or_missing_dag_fails_the_pipeline_before_any_child_starts": a_paused_or_missing_dag_fails_the_pipeline_before_any_child_starts,
    "the_api_stand_in_serves_signed_requests_and_refuses_a_wrong_secret": the_api_stand_in_serves_signed_requests_and_refuses_a_wrong_secret,
    "the_bronze_request_the_image_run_stages_passes_the_launch_check": the_bronze_request_the_image_run_stages_passes_the_launch_check,
    "the_image_scenarios_of_the_first_dag_hold_against_the_stand_in_run": the_image_scenarios_of_the_first_dag_hold_against_the_stand_in_run,
    "the_runtime_reads_runs_pauses_and_task_states_through_the_signed_rest_api": the_runtime_reads_runs_pauses_and_task_states_through_the_signed_rest_api,
}
