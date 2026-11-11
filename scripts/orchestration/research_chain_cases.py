"""Cases of the research DAG and the promotion chain it runs: every launching step runs in the promote family, told its phase by the PHASE variable.

The rig, the stand-in SDK and the launch services are those of ``airflow_mode_cases``: the real preflight, launcher and launch operator over a fake ECS and
fake buckets. The Airflow SDK and provider are stand-ins, so rerun the cases in the Airflow image after an upgrade.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import graph, promotion_chain, training_contract
from orchestration.ecs_launch import LaunchRefused
from orchestration.interface import SubmissionRequest
from scripts.orchestration.airflow_mode_cases import RUN_KEY, FormRefused, Rig, task_graph, world
from scripts.orchestration.ecs_adapter_cases import require
from scripts.orchestration.ecs_launch_cases import refuses

ROOT = Path(__file__).resolve().parents[2]
BASE = "20261109T000000Z-b45e0001"
RESEARCH = training_contract.research_run_id(BASE)
TYPE = "HAI_1"
RESEARCH_PARAMS = {"base_run": BASE, "population": "primary", "hai_types": ["HAI_1", "HAI_2"], "mlflow": "aws"}
CHALLENGER = {"model": "m2", "version": "4", "verdict": "pass", "source_run": "mlrun4"}
PHASES = {"research": "research", "canary_check": "canary", "promote": "promote", "owner_notice": "notify"}


def chain_parameters(template: str) -> dict[str, Any]:
    """The closed parameters of a launching step of the promotion chain."""
    return {
        "canary_check": {"research_run": RESEARCH, "hai_type": TYPE, "mlflow": "aws"},
        "owner_notice": {"research_run": RESEARCH, "hai_type": TYPE, "outcome": "promoted", "mlflow": "aws"},
        "promote": {"hai_type": TYPE, "model_version": "3", "promotion_mode": "promote", "mlflow": "aws"},
    }[template]


def request_for(template: str, run: str, training: dict[str, Any], attempt: int = 1) -> SubmissionRequest:
    return SubmissionRequest(template, run, "real", attempt, "research_dag", template, "manual__test", None, training=training)


def launched_phase(rig: Rig, template: str, run: str, training: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Launch one step of the promote family through the real preflight and launcher and return the variables its containers got."""
    rig.stage_runs(run, training_contract.trained_run_id(BASE, "m1"))
    spec = rig.preflight.prepare(request_for(template, run, training))
    require(spec is not None and spec.job == "promote", f"{template} did not name the promote job: {spec}")
    before = rig.tasks_started()
    rig.services.operator_class(
        task_id=template,
        job=spec,
        launcher=lambda: rig.launcher,
        task_definition=rig.services.definition("promote"),
        cluster=rig.services.cluster,
        overrides={},
    ).execute({"ti": request_context(template)})
    require(rig.tasks_started() == before + 1, f"{template} did not start exactly one task")
    return rig.environment(before)


def request_context(template: str) -> Any:
    return SimpleNamespace(dag_id="research_dag", task_id=template, run_id="manual__test", map_index=-1, try_number=1)


def every_promote_step_gets_its_phase() -> None:
    """The one task definition of the promote family runs a different phase for each step: research, canary check, promotion and notice name theirs in PHASE."""
    rig = Rig()
    runs = {
        template: training_contract.chain_run_id(RESEARCH, TYPE, step)
        for template, step in (("canary_check", "canary"), ("promote", "promote"), ("owner_notice", "notice"))
    }
    runs["research"] = RESEARCH
    parameters = {"research": RESEARCH_PARAMS, **{template: chain_parameters(template) for template in PHASES if template != "research"}}
    for template, phase in PHASES.items():
        environment = launched_phase(rig, template, runs[template], parameters[template])
        require(environment["promote"]["PHASE"] == phase, f"{template} told the container the phase {environment['promote'].get('PHASE')!r}, not {phase!r}")
        require(environment["promote"]["RUN_ID"] == runs[template], f"{template} ran another run: {environment['promote']['RUN_ID']}")
        require(json.loads(environment["promote"]["TRAINING_PARAMETERS"]) == parameters[template], f"{template} got other parameters")
        require("PHASE" not in environment["fetch"], "the fetch container was told a phase")


def no_other_job_is_told_a_phase() -> None:
    """Only the promote family's phase container takes PHASE: the job table names it nowhere else."""
    table = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())
    holders = {(job, container) for job, row in table["jobs"].items() for container, names in row["containers"].items() if "PHASE" in names}
    require(holders == {("promote", "promote")}, f"PHASE is held by {sorted(holders)}")
    require({template for template, job in table["steps"].items() if job == "promote"} == set(PHASES), "the promote family's steps are not the four phases")


def steps_refuse_the_parameters_of_another_step() -> None:
    """A step takes only the closed parameters of its own phase, checked before any cloud call or record."""
    rig = Rig()
    swapped = (
        ("research", chain_parameters("canary_check")),
        ("canary_check", RESEARCH_PARAMS),
        ("canary_check", chain_parameters("owner_notice")),
        ("owner_notice", chain_parameters("canary_check")),
        ("promote", chain_parameters("canary_check")),
        ("promote", {**chain_parameters("promote"), "promotion_mode": "rollback"}),
        ("owner_notice", {**chain_parameters("owner_notice"), "outcome": "approved"}),
    )
    for template, training in swapped:
        rig.stage_runs(RESEARCH)
        error = refuses(LaunchRefused, partial(rig.preflight.prepare, request_for(template, RESEARCH, training)))
        require("no ECS job" not in str(error), f"{template} is not a step of the table, so its parameters were never checked: {error}")
    require(not rig.ecs.calls and not rig.records.calls, "a step with another step's parameters reached the cloud or the launch records")


def research_dag_step_launches_with_its_form() -> None:
    """The research task of the DAG reads the form: the run is the research run, the parameters are the form's, and the lane is the AWS one."""
    rig = Rig()
    rig.stage_runs(training_contract.trained_run_id(BASE, "m1"))
    with world(rig, "research_dag", {"run_id": RESEARCH, "mode": "real", **RESEARCH_PARAMS}) as current:
        require(current.step("research") == "succeeded", "the research step did not pass")
    environment = rig.environment()
    require(environment["promote"]["PHASE"] == "research" and json.loads(environment["promote"]["TRAINING_PARAMETERS"]) == RESEARCH_PARAMS, f"{environment}")
    with world(rig, "research_dag", {"run_id": RESEARCH, "mode": "real", **RESEARCH_PARAMS, "mlflow": "local"}) as current:
        error = refuses(ValueError, lambda: current.step("research"))
    require("mlflow=aws" in str(error), f"the refusal of the local lane does not name its fix: {error}")


def research_record(hai_type: str, challenger: Mapping[str, Any] | None) -> bytes:
    document = {
        "schema": 1,
        "hai_type": hai_type,
        "base_run": BASE,
        "runs": {},
        "promotion": {"registered_model": f"hai_model_{hai_type}", "candidates": [], "challenger": challenger},
    }
    return json.dumps(document).encode()


def canary_record(
    hai_type: str, decision: str, failed: tuple[str, ...] = (), challenger: Mapping[str, Any] = CHALLENGER, changes: dict[str, Any] | None = None
) -> bytes:
    document = {
        "schema": 1,
        "research_run": RESEARCH,
        "hai_type": hai_type,
        "model": challenger["model"],
        "version": challenger["version"],
        "decision": decision,
        "failed": list(failed),
    } | (changes or {})
    return json.dumps(document).encode()


def stage_records(objects: dict[str, bytes], challengers: Mapping[str, Mapping[str, Any] | None], canary: str | None = "promote") -> None:
    """What the research and canary phases would have stored: a research record for each type and a canary result for each type with a challenger."""
    for hai_type, challenger in challengers.items():
        objects[training_contract.record_key("research", RESEARCH, hai_type)] = research_record(hai_type, challenger)
        if challenger is not None and canary is not None:
            objects[training_contract.record_key("canary", RESEARCH, hai_type)] = canary_record(hai_type, canary, challenger=challenger)


class Recorder:
    """Stand-ins for the three things the chain calls: a launch of a step, a read of a stored record and the release of a held online load."""

    def __init__(self, challengers: Mapping[str, Mapping[str, Any] | None], canary: str | None = "promote", failing: tuple[tuple[str, str], ...] = ()) -> None:
        self.objects: dict[str, bytes] = {}
        stage_records(self.objects, challengers, canary)
        self.calls: list[tuple[Any, ...]] = []
        self.types = tuple(challengers)
        self.failing = failing
        self.release_fails = False

    def launch(self, step: str, run: str, training: dict[str, Any]) -> None:
        self.calls.append((step, training["hai_type"], run, training))
        if (step, training["hai_type"]) in self.failing:
            raise promotion_chain.StepFailed(f"{step} of {training['hai_type']} failed")

    def read(self, key: str) -> bytes | None:
        return self.objects.get(key)

    def release(self, model: str, features_run: str, run: str) -> None:
        self.calls.append(("release", model, features_run, run))
        if self.release_fails:
            raise promotion_chain.StepFailed("the release step is not wired")

    def steps(self) -> list[tuple[str, str]]:
        return [(call[0], call[1]) for call in self.calls]

    def run(self, hai_types: tuple[str, ...] | None = None) -> list[str]:
        return promotion_chain.run_types(
            BASE, RESEARCH, self.types if hai_types is None else hai_types, launch=self.launch, read=self.read, release=self.release
        )


def a_challenger_is_canaried_promoted_released_and_announced() -> None:
    """A type with a challenger runs canary check, promotion, release and notice in that order, each with the run and the parameters of its own."""
    recorder = Recorder({"HAI_1": CHALLENGER, "HAI_2": None})
    require(recorder.run() == [], "a clean chain reported a failure")
    require(recorder.steps() == [("canary_check", "HAI_1"), ("promote", "HAI_1"), ("release", "m2"), ("owner_notice", "HAI_1")], f"steps: {recorder.steps()}")
    canary, promote, release, notice = recorder.calls
    require(canary[2] == training_contract.chain_run_id(RESEARCH, "HAI_1", "canary") and canary[3] == chain_parameters("canary_check"), f"canary: {canary}")
    require(promote[2] == training_contract.chain_run_id(RESEARCH, "HAI_1", "promote"), f"the promotion run: {promote[2]}")
    require(promote[3] == {"hai_type": "HAI_1", "model_version": "4", "promotion_mode": "promote", "mlflow": "aws"}, f"promotion: {promote[3]}")
    require(release[2] == training_contract.model_features_run_id(BASE, "m2"), f"the release names the run of the model's set: {release[2]}")
    require(notice[3] == {**chain_parameters("owner_notice"), "outcome": "promoted"}, f"notice: {notice[3]}")


def a_held_canary_promotes_nothing_and_tells_the_owner() -> None:
    recorder = Recorder({"HAI_1": CHALLENGER}, canary="hold")
    require(recorder.run() == [], "a held canary is a result, not a failure")
    require(recorder.steps() == [("canary_check", "HAI_1"), ("owner_notice", "HAI_1")], f"steps: {recorder.steps()}")
    require(recorder.calls[-1][3]["outcome"] == "held", f"notice: {recorder.calls[-1]}")


def a_canary_result_of_another_candidate_is_a_hold_and_a_failure() -> None:
    for changes in ({"version": "5"}, {"model": "m4"}, {"hai_type": "HAI_2"}, {"research_run": training_contract.research_run_id("20261108T000000Z-b45e0002")}):
        recorder = Recorder({"HAI_1": CHALLENGER})
        recorder.objects[training_contract.record_key("canary", RESEARCH, "HAI_1")] = canary_record("HAI_1", "promote", changes=changes)
        failures = recorder.run()
        require(len(failures) == 1 and "canary" in failures[0], f"a canary result of another candidate was not reported: {changes} {failures}")
        require(
            ("promote", "HAI_1") not in recorder.steps() and recorder.calls[-1][3]["outcome"] == "held",
            f"it promoted or did not tell the owner: {changes} {recorder.steps()}",
        )


def a_missing_or_unreadable_canary_result_holds() -> None:
    for name, value in (("missing", None), ("not_json", b"{"), ("not_an_object", b"[]"), ("unknown_decision", canary_record("HAI_1", "approve"))):
        recorder = Recorder({"HAI_1": CHALLENGER}, canary=None)
        if value is not None:
            recorder.objects[training_contract.record_key("canary", RESEARCH, "HAI_1")] = value
        failures = recorder.run()
        require(len(failures) == 1 and ("promote", "HAI_1") not in recorder.steps(), f"{name}: failures {failures}, steps {recorder.steps()}")
        require(recorder.calls[-1][3]["outcome"] == "held", f"{name}: the owner was not told the hold")


def one_type_failing_stops_only_its_chain() -> None:
    both = {"HAI_1": CHALLENGER, "HAI_2": {**CHALLENGER, "model": "m4", "version": "7"}}
    for step in ("canary_check", "promote", "owner_notice"):
        recorder = Recorder(both, failing=((step, "HAI_1"),))
        failures = recorder.run()
        require(len(failures) == 1 and "HAI_1" in failures[0] and step in failures[0], f"{step}: failures {failures}")
        other = [call[0] for call in recorder.calls if call[1] == "HAI_2" or call[0] == "release" and call[1] == "m4"]
        require(other == ["canary_check", "promote", "release", "owner_notice"], f"HAI_2 did not complete after HAI_1 failed at {step}: {other}")
    unresolved = Recorder(both, failing=(("promote", "HAI_1"),))
    unresolved.run()
    require(("owner_notice", "HAI_1") not in unresolved.steps() and ("release", "m2") not in unresolved.steps(), "a failed promotion was announced or released")
    unannounced = Recorder(both, failing=(("canary_check", "HAI_1"),))
    unannounced.run()
    require(
        ("promote", "HAI_1") not in unannounced.steps() and ("owner_notice", "HAI_1") in unannounced.steps(),
        "a failed canary was promoted or not announced as held",
    )


def a_failed_release_still_announces_the_promotion() -> None:
    recorder = Recorder({"HAI_1": CHALLENGER})
    recorder.release_fails = True
    failures = recorder.run()
    require(len(failures) == 1 and "release" in failures[0], f"a failed release was not reported: {failures}")
    require(
        recorder.calls[-1][0] == "owner_notice" and recorder.calls[-1][3]["outcome"] == "promoted", f"the promotion was not announced: {recorder.calls[-1]}"
    )


def bad_research_records_are_failures_and_launch_nothing() -> None:
    for name, mutate in (
        ("missing", lambda objects: objects.pop(training_contract.record_key("research", RESEARCH, "HAI_1"))),
        ("not_json", lambda objects: objects.__setitem__(training_contract.record_key("research", RESEARCH, "HAI_1"), b"{")),
        (
            "ineligible_model",
            lambda objects: objects.__setitem__(
                training_contract.record_key("research", RESEARCH, "HAI_1"), research_record("HAI_1", {**CHALLENGER, "model": "m3"})
            ),
        ),
        (
            "no_version",
            lambda objects: objects.__setitem__(training_contract.record_key("research", RESEARCH, "HAI_1"), research_record("HAI_1", {"model": "m2"})),
        ),
        (
            "another_type",
            lambda objects: objects.__setitem__(training_contract.record_key("research", RESEARCH, "HAI_1"), research_record("HAI_2", CHALLENGER)),
        ),
    ):
        recorder = Recorder({"HAI_1": CHALLENGER})
        mutate(recorder.objects)
        failures = recorder.run(("HAI_1",))
        require(len(failures) == 1 and not recorder.calls, f"{name}: failures {failures}, calls {recorder.calls}")


def the_chain_is_the_same_for_a_retry() -> None:
    first, second = Recorder({"HAI_1": CHALLENGER}), Recorder({"HAI_1": CHALLENGER})
    first.run()
    second.run()
    second.run()
    require(second.calls[: len(first.calls)] == first.calls == second.calls[len(first.calls) :], "a retry named other runs or parameters than the first pass")


OTHER_BASE = "20261108T000000Z-b45e0002"
TRAIN_FORM = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "aws"}


def launch_spec(rig: Rig, spec: Any, template: str) -> None:
    """Launch one job spec through the rig's operator and wait for it, as a task of the DAG does."""
    rig.services.operator_class(
        task_id=template,
        job=spec,
        launcher=lambda: rig.launcher,
        task_definition=rig.services.definition(spec.job),
        cluster=rig.services.cluster,
        overrides={},
    ).execute({"ti": request_context(template)})


def trained(rig: Rig, model: str, base: str = BASE) -> None:
    """The training run of ``model`` on ``base`` as the features, drift and training DAGs chain it, passed."""
    run = training_contract.trained_run_id(base, model)
    rig.stage_runs(run)
    training = training_contract.parameters_for("training_dag", {**TRAIN_FORM, "model": model}, training_contract.LAUNCH_LANES)
    spec = rig.preflight.prepare(SubmissionRequest("guard", run, "real", 1, "training_dag", "guard", "manual__test", None, training=training))
    launch_spec(rig, spec, "guard")


def dag_params(**changes: Any) -> dict[str, Any]:
    return {"run_id": RESEARCH, "mode": "real", **RESEARCH_PARAMS, **changes}


def staged_for_the_dag(rig: Rig, canary: str | None = "promote") -> None:
    stage_records(rig.stage.objects, {"HAI_1": CHALLENGER, "HAI_2": None}, canary)


def research_dag_form_and_graph() -> None:
    """The form needs the research run and the base run, the DAG is ready, research, chain and gate in that order, and only research takes the pool."""
    rig = Rig()
    with world(rig, "research_dag", dag_params()) as current:
        with mock.patch.dict("os.environ", {"HAI_LAUNCH_POOL": "host_jobs"}, clear=True):
            current.run_dag("research_dag.py")
        graph = {
            node.task_id: ({before.task_id for before in node.upstream}, node.settings.get("trigger_rule"), node.settings.get("pool")) for node in current.built
        }
    expected = {
        "ready": (set(), None, None),
        "research": ({"ready"}, None, "host_jobs"),
        "chain": ({"research"}, None, "host_jobs"),
        "gate": ({"chain"}, "all_done", None),
    }
    require(graph == expected, f"the research DAG is {graph}")
    for name, value in (("run_id", None), ("base_run", None), ("base_run", "latest"), ("run_id", "x")):
        params = {key: item for key, item in dag_params().items() if key != name}
        if value is not None:
            params[name] = value
        with world(Rig(), "research_dag", params) as current:
            refuses(FormRefused, partial(current.run_dag, "research_dag.py"))


def research_dag_skips_until_three_models_trained() -> None:
    """The research DAG ends skipped, not failed, until m1, m2 and m4 have training runs on its base run: nothing is launched and no record is written."""
    cases = [((), BASE), (("m1",), BASE), (("m1", "m2"), BASE), (("m1", "m4"), BASE), (("m2", "m4"), BASE), (("m1", "m2", "m4"), OTHER_BASE)]
    for models, base in cases:
        rig = Rig()
        for model in models:
            trained(rig, model, base)
        before, records = rig.tasks_started(), len(rig.records.objects)
        staged_for_the_dag(rig)
        with world(rig, "research_dag", dag_params()) as current:
            states = current.run_dag("research_dag.py")
        require(states == {"ready": "skipped", "research": "skipped", "chain": "skipped", "gate": "success"}, f"{models} on {base}: {states}")
        require(current.results["gate"] == "skipped", f"{models} on {base}: the gate said {current.results['gate']}")
        require(rig.tasks_started() == before and len(rig.records.objects) == records, f"{models} on {base}: the skipped DAG launched or recorded something")
        require(
            "m4" in str(current.results["ready"]) or "m2" in str(current.results["ready"]) or "m1" in str(current.results["ready"]), "the skip names no model"
        )


def research_dag_runs_the_chain_for_the_challenger() -> None:
    """All three models trained: research runs in the promote family, then the challenger's canary check, promotion and notice, with their phases and runs."""
    rig = Rig()
    for model in training_contract.PROMOTION_MODELS:
        trained(rig, model)
    staged_for_the_dag(rig)
    released: list[tuple[Any, ...]] = []
    before = rig.tasks_started()
    with (
        world(rig, "research_dag", dag_params()) as current,
        mock.patch.object(promotion_chain, "release_online", lambda *arguments: released.append(arguments[2:4])),
    ):
        states = current.run_dag("research_dag.py")
    require(set(states.values()) == {"success"} and current.results["gate"] == "accepted", f"the DAG did not finish: {states} {current.results}")
    require(rig.tasks_started() == before + 4, f"four tasks were expected: {rig.tasks_started() - before}")
    launched = [rig.environment(before + index)["promote"] for index in range(4)]
    require([item["PHASE"] for item in launched] == ["research", "canary", "promote", "notify"], f"phases: {[item['PHASE'] for item in launched]}")
    runs = [training_contract.chain_run_id(RESEARCH, "HAI_1", step) for step in ("canary", "promote", "notice")]
    require([item["RUN_ID"] for item in launched] == [RESEARCH, *runs], f"runs: {[item['RUN_ID'] for item in launched]}")
    require(json.loads(launched[3]["TRAINING_PARAMETERS"])["outcome"] == "promoted", "the owner was not told promoted")
    require(released == [("m2", training_contract.model_features_run_id(BASE, "m2"))], f"release: {released}")
    for run in (RESEARCH, *runs):
        bound = json.loads(rig.stage.objects[RUN_KEY.format(run=run, name="code_run.json")])
        require(bound == {"code_run": training_contract.trained_run_id(BASE, "m1")}, f"{run} is bound to {bound}, not to the code the models trained with")
    again = rig.tasks_started()
    with world(rig, "research_dag", dag_params()) as current, mock.patch.object(promotion_chain, "release_online", lambda *arguments: None):
        current.run_dag("research_dag.py")
    require(rig.tasks_started() == again, "a second run of the DAG for the same base launched again")


def research_dag_fails_after_the_notice_when_the_release_step_is_missing() -> None:
    """The held online load's release step is not in the job table yet: the promotion and the notice stand, and the DAG run fails by name."""
    rig = Rig()
    for model in training_contract.PROMOTION_MODELS:
        trained(rig, model)
    staged_for_the_dag(rig)
    before = rig.tasks_started()
    with world(rig, "research_dag", dag_params()) as current:
        states = current.run_dag("research_dag.py")
    require(states["chain"] == "failed" and states["gate"] == "failed", f"states: {states}")
    require("release_online" in str(current.results["chain"]), f"the failure does not name the release step: {current.results['chain']}")
    require(rig.tasks_started() == before + 4, "the promotion and the notice were not launched before the failure")


def research_dag_refuses_the_local_lane_before_anything_runs() -> None:
    rig = Rig()
    for model in training_contract.PROMOTION_MODELS:
        trained(rig, model)
    before = rig.tasks_started()
    with world(rig, "research_dag", dag_params(mlflow="local")) as current:
        states = current.run_dag("research_dag.py")
    require(states["ready"] == "failed" and "mlflow=aws" in str(current.results["ready"]), f"states {states}: {current.results['ready']}")
    require(rig.tasks_started() == before, "a refused research run launched a task")


def graph_rules_cover_the_research_dag() -> None:
    """The graph rules accept the shipped research DAG (ready, research, chain, then the gate that runs after a failure) and refuse a missing, extra or
    misplaced task, a gate that does not run after a failure and a tail after the gate."""
    require("research_dag" in graph.GRAPH_DAGS, "the research DAG is not graph-checked")
    reference = graph.reference_graph("research_dag")
    require(set(reference) == {"ready", "research", "chain", "gate"}, f"the reference graph holds {sorted(reference)}")
    require(graph.validate_transform_graph(reference, "research_dag") == [], "the reference graph of the research DAG is refused")

    def rules(broken: dict[str, Any]) -> set[str]:
        return {violation.rule for violation in graph.validate_transform_graph(broken, "research_dag")}

    require("missing_task" in rules({name: node for name, node in reference.items() if name != "ready"}), "a research DAG without ready was accepted")
    require("unexpected_task" in rules({**reference, "extra": graph.TaskNode(frozenset({"chain"}))}), "a research DAG with an extra task was accepted")
    require("upstream" in rules({**reference, "chain": graph.TaskNode(frozenset({"ready"}))}), "a chain that does not follow research was accepted")
    require(
        "trigger_rule" in rules({**reference, "gate": graph.TaskNode(frozenset({"chain"}), "all_success")}), "a gate that stops after a failure was accepted"
    )
    require("leaf" in rules({**reference, "tail": graph.TaskNode(frozenset({"gate"}))}), "a tail task after the gate was accepted")
    rig = Rig()
    with world(rig, "research_dag", dag_params()) as current:
        current.run_dag("research_dag.py")
        require(graph.validate_transform_graph(task_graph(current), "research_dag") == [], "the shipped research DAG breaks the graph rules")


def each_promotion_model_starts_the_research_on_its_base() -> None:
    """A training run of m1, m2 or m4 that names its base run starts the research DAG for that base, once per training run and under the one research run
    of the base. Any other model, and a run that names no base (a hand start), starts nothing."""
    training_run = training_contract.trained_run_id(BASE, "m2")
    for model in training_contract.MODELS:
        requests = promotion_chain.research_requests(model, training_run, BASE, "primary", ["HAI_1", "HAI_2"], "aws")
        if model not in training_contract.PROMOTION_MODELS:
            require(requests == [], f"{model} started the research: {requests}")
            continue
        require(
            requests == [{"trigger_run_id": f"training__{training_run}", "conf": {"run_id": RESEARCH, "mode": "real", **RESEARCH_PARAMS}}],
            f"{model} started another research: {requests}",
        )
    require(promotion_chain.research_requests("m2", training_run, "", "primary", ["HAI_1"], "aws") == [], "a run with no base started the research")
    first = promotion_chain.research_requests("m1", training_contract.trained_run_id(BASE, "m1"), BASE, "primary", ["HAI_1"], "aws")
    second = promotion_chain.research_requests("m4", training_contract.trained_run_id(BASE, "m4"), BASE, "primary", ["HAI_1"], "aws")
    require(
        first[0]["conf"]["run_id"] == second[0]["conf"]["run_id"] and first[0]["trigger_run_id"] != second[0]["trigger_run_id"],
        "the models' triggers share a run",
    )
    refuses(ValueError, partial(promotion_chain.research_requests, "m2", training_run, BASE, "primary", ["HAI_9"], "aws"))
    refuses(ValueError, partial(promotion_chain.research_requests, "m2", training_run, BASE, "primary", ["HAI_1"], "local"))


def training_dag_starts_the_research_after_its_release() -> None:
    """The shipped training DAG gains two tasks behind its release: one that builds the research request of the run (none for another model or a run with no
    base) and the mapped trigger of the research DAG that does not wait. A run that did not pass its release starts nothing."""
    expected = graph.reference_graph("training_dag")
    require(graph.validate_transform_graph(expected, "training_dag") == [], "the reference graph of the training DAG is refused")
    require(
        expected["research_request"].upstream == frozenset({"release_run"}) and expected["trigger_research"].upstream == frozenset({"research_request"}),
        "the reference graph does not put the trigger behind the release",
    )
    for model, base, count in (("m2", BASE, 1), ("m3", BASE, 0), ("m2", "", 0)):
        rig = Rig()
        training = {**TRAIN_FORM, "model": model, "mlflow": "aws", "base_run": base, "run_id": training_contract.trained_run_id(BASE, model), "mode": "real"}
        with world(rig, "training_dag", training) as current, mock.patch.object(current.runtime, "run_step", lambda name: "ok"):
            states = current.run_dag("training_dag.py")
            require(graph.validate_transform_graph(task_graph(current), "training_dag") == [], "the shipped training DAG breaks the graph rules")
        require(set(states.values()) == {"success"}, f"{model}: {states}")
        require(len(current.results["research_request"]) == count, f"{model} on {base!r}: the request is {current.results['research_request']}")
        trigger = next(node for node in current.built if node.task_id == "trigger_research")
        require(trigger.settings["trigger_dag_id"] == "research_dag" and trigger.settings["wait_for_completion"] is False, f"trigger: {trigger.settings}")
        require(trigger.settings["skip_when_already_exists"] is True and trigger.settings["reset_dag_run"] is False, f"trigger: {trigger.settings}")
    rig = Rig()
    training = {**TRAIN_FORM, "model": "m2", "mlflow": "aws", "base_run": BASE, "run_id": training_contract.trained_run_id(BASE, "m2"), "mode": "real"}

    def rejecting(name: str) -> str:
        if name == "training_final_gate_rejected" or name == "fit":
            raise RuntimeError("a step failed")
        return "ok"

    with world(rig, "training_dag", training) as current, mock.patch.object(current.runtime, "run_step", rejecting):
        states = current.run_dag("training_dag.py")
    require(
        states["research_request"] == "upstream_failed" and states["trigger_research"] == "upstream_failed",
        f"a failed training run started the research: {states}",
    )


CASES: dict[str, Callable[[], None]] = {
    "a_challenger_is_canaried_promoted_released_and_announced": a_challenger_is_canaried_promoted_released_and_announced,
    "a_held_canary_promotes_nothing_and_tells_the_owner": a_held_canary_promotes_nothing_and_tells_the_owner,
    "a_canary_result_of_another_candidate_is_a_hold_and_a_failure": a_canary_result_of_another_candidate_is_a_hold_and_a_failure,
    "a_missing_or_unreadable_canary_result_holds": a_missing_or_unreadable_canary_result_holds,
    "one_type_failing_stops_only_its_chain": one_type_failing_stops_only_its_chain,
    "a_failed_release_still_announces_the_promotion": a_failed_release_still_announces_the_promotion,
    "bad_research_records_are_failures_and_launch_nothing": bad_research_records_are_failures_and_launch_nothing,
    "the_chain_is_the_same_for_a_retry": the_chain_is_the_same_for_a_retry,
    "research_dag_form_and_graph": research_dag_form_and_graph,
    "graph_rules_cover_the_research_dag": graph_rules_cover_the_research_dag,
    "each_promotion_model_starts_the_research_on_its_base": each_promotion_model_starts_the_research_on_its_base,
    "training_dag_starts_the_research_after_its_release": training_dag_starts_the_research_after_its_release,
    "research_dag_skips_until_three_models_trained": research_dag_skips_until_three_models_trained,
    "research_dag_runs_the_chain_for_the_challenger": research_dag_runs_the_chain_for_the_challenger,
    "research_dag_fails_after_the_notice_when_the_release_step_is_missing": research_dag_fails_after_the_notice_when_the_release_step_is_missing,
    "research_dag_refuses_the_local_lane_before_anything_runs": research_dag_refuses_the_local_lane_before_anything_runs,
    "every_promote_step_gets_its_phase": every_promote_step_gets_its_phase,
    "no_other_job_is_told_a_phase": no_other_job_is_told_a_phase,
    "steps_refuse_the_parameters_of_another_step": steps_refuse_the_parameters_of_another_step,
    "research_dag_step_launches_with_its_form": research_dag_step_launches_with_its_form,
}
