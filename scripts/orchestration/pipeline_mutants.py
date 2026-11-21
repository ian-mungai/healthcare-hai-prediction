"""The first DAG's cases, held to what they claim: for each behaviour the suite claims, a change that removes it must make at least one case fail.

Each mutant is one change in memory to one source file (``source_mutants.mutated``) and the suites of cases that claim the behaviour. ``run`` returns, for each
mutant, the cases that failed with it on; a mutant with no failing case is a claim that nothing proves. ``unchanged`` runs the suites on the real code and must
return no failure, so that a failing case under a mutant is the mutant's doing.
"""

from __future__ import annotations

import importlib.util
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from orchestration.training_contract import PIPELINE_DAG
from scripts.orchestration import airflow_mode_cases, ecs_chain_cases, ingestion_schedule_cases, pipeline_cases, source_mutants, trigger_dag, trigger_dag_cases
from scripts.orchestration.ecs_adapter_cases import require

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = "orchestration/dag_runtime.py"
PIPELINE = "dags/pipeline_dag.py"
STAND_IN = "scripts/orchestration/airflow_api_fake.py"
GRAPH = "orchestration/graph.py"
METADATA_CASES = {name: case for name, case in ecs_chain_cases.CASES.items() if name.startswith("metadata_start")}
START_CASES = {name: case for name, case in ingestion_schedule_cases.CASES.items() if "pipeline" in name or "first_dag" in name or "retry_starts" in name}
BIND_CASES = {name: case for name, case in airflow_mode_cases.CASES.items() if "bind_code" in inspect.getsource(case)}
TRIGGER = "scripts/orchestration/trigger_dag.py"
PREFLIGHT = "orchestration/ecs_preflight.py"
CONTRACT = "orchestration/training_contract.py"


def trigger_group(name: str) -> Callable[[], None]:
    """A group of the host command's checks as one case: it fails when any check of the group is false."""

    def case() -> None:
        wrong = [check for check, passed in trigger_dag_cases.GROUPS[name]().items() if not passed]
        require(not wrong, f"{name}: {wrong}")

    return case


SUITES: dict[str, dict[str, Callable[[], Any]]] = {
    "pipeline": pipeline_cases.CASES,
    "metadata": METADATA_CASES,
    "start": START_CASES,
    "bind": BIND_CASES,
    "trigger": {name: trigger_group(name) for name in trigger_dag_cases.GROUPS},
}


@dataclass(frozen=True)
class Mutant:
    """One removed behaviour: what the suite claims, the one change that removes it and the suite that must notice."""

    behaviour: str
    path: str
    old: str
    new: str
    suite: str = "pipeline"


MUTANTS = (
    Mutant(
        "a child that is running is waited for until it ends",
        RUNTIME,
        '        if state == "success":\n            return f"{dag_id} run {run_id} succeeded"',
        '        if state in ("success", "running"):\n            return f"{dag_id} run {run_id} succeeded"',
    ),
    Mutant(
        "a child that is queued or running is never waited for",
        RUNTIME,
        '        if state == "success":\n            return f"{dag_id} run {run_id} succeeded"',
        '        if state in ("success", "running", "queued"):\n            return f"{dag_id} run {run_id} succeeded"',
    ),
    Mutant(
        "a child that failed fails the stage",
        RUNTIME,
        '        if state == "failed":\n            raise ChildRunFailed(f"{dag_id} run {run_id} failed")',
        '        if state == "failed":\n            return f"{dag_id} run {run_id} succeeded"',
    ),
    Mutant(
        "a child that exists is checked against the settings of the stage",
        RUNTIME,
        '            if problem is not None:\n                raise ChildRunFailed(f"{dag_id} run {run_id} exists but',
        '            if False:\n                raise ChildRunFailed(f"{dag_id} run {run_id} exists but',
    ),
    Mutant(
        "the defaults of a form fill in what a hand start left out",
        RUNTIME,
        "wanted, seen = {**(defaults or {}), **expected}, {**(defaults or {}), **named}",
        "wanted, seen = dict(expected), dict(named)",
    ),
    Mutant(
        "a setting the pipeline does not set is refused",
        RUNTIME,
        'problems.append(f"{name}: expected none (the pipeline sets no such parameter), found {seen[name]!r}")',
        "pass",
    ),
    Mutant("a setting the run lacks is refused", RUNTIME, 'problems.append(f"{name}: expected {wanted[name]!r}, found none")', "pass"),
    Mutant("a setting with another value is refused", RUNTIME, "        elif seen[name] != wanted[name]:\n", "        elif False:\n"),
    Mutant(
        "a child that exists is never asked for again",
        RUNTIME,
        "    if record is None:\n        if dag_is_paused(dag_id):\n",
        "    if True:\n        if dag_is_paused(dag_id):\n",
    ),
    Mutant(
        "a paused DAG fails a stage that has no run",
        RUNTIME,
        '        if dag_is_paused(dag_id):\n            raise ChildRunFailed(f"{dag_id} is paused, so its run {run_id} would',
        '        if False:\n            raise ChildRunFailed(f"{dag_id} is paused, so its run {run_id} would',
    ),
    Mutant(
        "a queued child of a paused DAG fails the stage",
        RUNTIME,
        '        if state == "queued" and dag_is_paused(dag_id):',
        "        if False:",
    ),
    Mutant(
        "a stage's wait ends at its limit",
        RUNTIME,
        '        if CLOCK() >= deadline:\n            raise ChildRunFailed(f"{dag_id} run {run_id} is still',
        '        if False:\n            raise ChildRunFailed(f"{dag_id} run {run_id} is still',
    ),
    Mutant(
        "the check of the pipeline's DAGs refuses a paused or missing DAG",
        RUNTIME,
        '    if problems:\n        raise ChildRunFailed("the pipeline cannot run: "',
        '    if False:\n        raise ChildRunFailed("the pipeline cannot run: "',
    ),
    Mutant(
        "the pipeline waits for every model route",
        RUNTIME,
        "    deadline = CLOCK() + limit.total_seconds()\n    while True:\n        for route in routes:",
        '    return "no wait"\n    deadline = CLOCK() + limit.total_seconds()\n    while True:\n        for route in routes:',
    ),
    Mutant(
        "a failed model route fails the pipeline",
        RUNTIME,
        '    if failed:\n        raise ChildRunFailed("a model route failed: "',
        '    if False:\n        raise ChildRunFailed("a model route failed: "',
    ),
    Mutant(
        "a route run is checked against the settings of its route",
        RUNTIME,
        '            if problem is not None:\n                self.finish("failed", f"run {self.run} of {self.dag} was started with other',
        '            if False:\n                self.finish("failed", f"run {self.run} of {self.dag} was started with other',
    ),
    Mutant(
        "a route run that never appears fails the route",
        RUNTIME,
        "            elif now - self.seen >= CREATION_GRACE.total_seconds():",
        "            elif False:",
    ),
    Mutant(
        "the routes' wait ends at its limit",
        RUNTIME,
        '        if CLOCK() >= deadline:\n            raise ChildRunFailed(\n                f"the model routes still running',
        '        if False:\n            raise ChildRunFailed(\n                f"the model routes still running',
    ),
    Mutant(
        "a drift run that trained no type ends its route in success",
        RUNTIME,
        '            elif started == "skipped":\n                self.finish("done", "the drift gate trained no type',
        '            elif started == "skipped":\n                self.finish("failed", "the drift gate trained no type',
    ),
    Mutant(
        "a drift run that does not say whether it trained fails the route",
        RUNTIME,
        '                self.finish("failed", f"drift run {self.run} did not say',
        '                self.finish("done", f"drift run {self.run} did not say',
    ),
    Mutant(
        "a mapped task with a success and a skipped instance has succeeded",
        RUNTIME,
        '    if "success" in states:\n        return "success"\n',
        '    if False:\n        return "success"\n',
    ),
    Mutant(
        "a trigger that found its child run already there is followed to the child",
        RUNTIME,
        'STARTED = ("success", "duplicate")',
        'STARTED = ("success",)',
    ),
    Mutant(
        "a skipped trigger with a mapped request is told from one that mapped none",
        RUNTIME,
        '    return "duplicate" if any(index >= 0 for index in indexes) else "skipped"',
        '    return "skipped"',
    ),
    Mutant(
        "a research run is checked for its population, MLflow lane and attempt",
        CONTRACT,
        '"research_attempt": 1}, ("hai_types",)',
        '"research_attempt": 1}, ("hai_types", "population", "mlflow", "research_attempt")',
    ),
    Mutant(
        "a pipeline run that exists is checked against the retry day's settings",
        RUNTIME,
        '        if problem is not None:\n            raise RuntimeError(f"the pipeline run {pipeline} exists but',
        '        if False:\n            raise RuntimeError(f"the pipeline run {pipeline} exists but',
        "start",
    ),
    Mutant(
        "a paused first DAG fails the retry day's start",
        RUNTIME,
        "        if dag_is_paused(PIPELINE_DAG):\n            raise RuntimeError(",
        "        if False:\n            raise RuntimeError(",
        "start",
    ),
    Mutant(
        "a catalog run that exists is checked against the training run's base",
        RUNTIME,
        '        if problem is not None:\n            LOG.error("the catalog run %s exists',
        '        if False:\n            LOG.error("the catalog run %s exists',
        "metadata",
    ),
    Mutant(
        "the first DAG's gate fails the run when a task failed",
        PIPELINE,
        "        if failures:\n            raise RuntimeError(",
        "        if False:\n            raise RuntimeError(",
    ),
    Mutant(
        "a stage before the start does not run",
        PIPELINE,
        "        if START_NAMES.index(layer) < START_NAMES.index(start):",
        "        if False:",
    ),
    Mutant(
        "the binding runs before ingestion's child, not after",
        PIPELINE,
        "        stage_task(0),\n        bind_code().operator,",
        "        bind_code().operator,\n        stage_task(0),",
    ),
    Mutant(
        "the check of the pipeline's DAGs runs",
        PIPELINE,
        "        return dag_runtime.require_active(REQUIRED_DAGS)",
        '        return "checked"',
    ),
    Mutant(
        "a stage gets the producer run of the stage before it",
        PIPELINE,
        "        conf[parent_key] = layer_run(STAGES[index - 1][0], run)",
        "        pass",
    ),
    Mutant(
        "an invalid start is refused",
        PIPELINE,
        '    if start not in START_NAMES:\n        raise ValueError(f"start_from must be one of',
        '    if False:\n        raise ValueError(f"start_from must be one of',
    ),
    Mutant(
        "the host command takes the bronze DAG",
        TRIGGER,
        "DAGS = (PIPELINE_DAG, BRONZE_DAG, ",
        "DAGS = (PIPELINE_DAG, ",
        "trigger",
    ),
    Mutant(
        "the host command refuses a bronze run that is not a bronze run ID",
        TRIGGER,
        '        if run_id_parts(run_id).name != "bronze":',
        "        if False:",
        "trigger",
    ),
    Mutant(
        "the host command refuses a first DAG run that is not a pipeline run ID",
        TRIGGER,
        "        if run_id_parts(run_id).name != PIPELINE_RUN:",
        "        if False:",
        "trigger",
    ),
    Mutant(
        "the host command's parser refuses a start stage that is not a stage",
        TRIGGER,
        '"--start-from", choices=PIPELINE_STAGES, help=',
        '"--start-from", help=',
        "trigger",
    ),
    Mutant(
        "the host command unpauses the DAGs of the pipeline when it starts the first DAG",
        TRIGGER,
        "ACTIVATION = {PIPELINE_DAG: (*(dag for dag in PIPELINE_ACTIVE_DAGS if dag != PIPELINE_DAG), PIPELINE_DAG)}",
        "ACTIVATION = {}",
        "trigger",
    ),
    Mutant(
        "the host command unpauses the children before the first DAG",
        TRIGGER,
        "ACTIVATION = {PIPELINE_DAG: (*(dag for dag in PIPELINE_ACTIVE_DAGS if dag != PIPELINE_DAG), PIPELINE_DAG)}",
        "ACTIVATION = {PIPELINE_DAG: (PIPELINE_DAG, *(dag for dag in PIPELINE_ACTIVE_DAGS if dag != PIPELINE_DAG))}",
        "trigger",
    ),
    Mutant(
        "the binding task binds the silver, gold and features runs",
        PREFLIGHT,
        '            self.code_runs.bind(run, code)\n            if layer == "features":',
        '            if layer == "features":',
        "bind",
    ),
    Mutant(
        "the binding task binds each model's features, drift and training run",
        PREFLIGHT,
        "                        self.code_runs.bind(chained, code)",
        "                        pass",
        "bind",
    ),
    Mutant(
        "the binding task refuses a run that is not a pipeline run",
        PREFLIGHT,
        '        if parts is None or parts.name != PIPELINE_RUN:\n            raise LaunchRefused(f"{template} binds',
        '        if False:\n            raise LaunchRefused(f"{template} binds',
        "bind",
    ),
    Mutant(
        "the binding task binds a bronze run that has no code of its own to its ingestion run's code",
        PREFLIGHT,
        "        if bind:\n            self.code_runs.bind(bronze_run, code)",
        "        if False:\n            self.code_runs.bind(bronze_run, code)",
        "bind",
    ),
    Mutant(
        "the API stand-in refuses a request with another signature",
        STAND_IN,
        "    if not hmac.compare_digest(signature, expected):",
        "    if False:",
    ),
    Mutant(
        "the first DAG's gate is the only task that runs after a failure",
        GRAPH,
        'FIRST_RULES = {"pipeline_dag": {"gate": "all_done"}, "bronze_dag": {}}',
        'FIRST_RULES = {"pipeline_dag": {}, "bronze_dag": {}}',
    ),
)


def unchanged() -> dict[str, list[str]]:
    """The cases that fail on the real code, by suite: none."""
    return {name: failed for name, cases in SUITES.items() if (failed := source_mutants.failing(cases))}


def run(only: tuple[str, ...] = ()) -> dict[str, list[str]]:
    """The cases that fail with each mutant on, by behaviour. A behaviour with an empty list is not proven by any case."""
    results = {}
    for mutant in MUTANTS:
        if only and mutant.behaviour not in only:
            continue
        with source_mutants.mutated(mutant.path, mutant.old, mutant.new):
            results[mutant.behaviour] = source_mutants.failing(SUITES[mutant.suite])
    return results


WAIT = "a child that is running is waited for until it ends"
WAIT_CASES = (
    "an_existing_child_is_joined_by_its_real_outcome_and_never_recreated",
    "a_running_child_that_ends_failed_stops_the_run_before_the_next_stage",
)


def the_removed_wait_for_a_running_child_is_noticed_and_the_real_code_passes() -> None:
    """The review's mutant: a join that returns success when it first reads a running child. The cases that claim the wait pass on the real code and fail with
    the mutant on, each of them."""
    chosen = {name: pipeline_cases.CASES[name] for name in WAIT_CASES}
    require(not source_mutants.failing(chosen), f"a wait case fails on the real code: {source_mutants.failing(chosen)}")
    (mutant,) = [item for item in MUTANTS if item.behaviour == WAIT]
    with source_mutants.mutated(mutant.path, mutant.old, mutant.new):
        noticed = source_mutants.failing(chosen)
    require(noticed == list(WAIT_CASES), f"with the wait removed only {noticed} failed")


def every_behaviour_the_suites_claim_has_a_mutant_that_a_case_catches() -> None:
    """The suites pass on the real code, and every mutant (one removed behaviour each) makes at least one case fail."""
    require(not unchanged(), f"a case fails on the real code: {unchanged()}")
    survivors = [behaviour for behaviour, broken in run().items() if not broken]
    require(not survivors, f"no case notices that these behaviours are removed: {survivors}")
    require(not unchanged(), "a mutant left the real code changed")


def a_mutant_leaves_no_saved_bytecode_of_its_change() -> None:
    """The mutant's text is compiled while it is on and the module is reloaded from the real file after: no bytecode of the change may be saved for a later
    import, or the next run, in this tree or in the process that runs the cases, reads the change as the real code."""
    (mutant,) = [item for item in MUTANTS if item.behaviour == "the host command unpauses the children before the first DAG"]
    cache = Path(importlib.util.cache_from_source(str(ROOT / mutant.path)))
    cache.unlink(missing_ok=True)
    with source_mutants.mutated(mutant.path, mutant.old, mutant.new):
        pass
    require(trigger_dag.ACTIVATION[PIPELINE_DAG][-1] == PIPELINE_DAG, "the module read after the mutant still holds its change")


CASES: dict[str, Callable[[], None]] = {
    "a_mutant_leaves_no_saved_bytecode_of_its_change": a_mutant_leaves_no_saved_bytecode_of_its_change,
    "the_removed_wait_for_a_running_child_is_noticed_and_the_real_code_passes": the_removed_wait_for_a_running_child_is_noticed_and_the_real_code_passes,
    "every_behaviour_the_suites_claim_has_a_mutant_that_a_case_catches": every_behaviour_the_suites_claim_has_a_mutant_that_a_case_catches,
}
