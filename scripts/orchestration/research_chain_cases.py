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

from botocore.exceptions import ClientError

from orchestration import airflow_steps, graph, promotion_chain, training_contract
from orchestration.ecs_launch import LaunchRefused
from orchestration.interface import SubmissionRequest
from scripts.orchestration.airflow_mode_cases import RUN_KEY, FormRefused, Rig, task_graph, world
from scripts.orchestration.ecs_adapter_cases import require
from scripts.orchestration.ecs_launch_cases import refuses

ROOT = Path(__file__).resolve().parents[2]
BASE = "features_b45e0001"
RESEARCH = training_contract.research_run_id(BASE)
TYPE = "HAI_1"
RESEARCH_PARAMS = {"base_run": BASE, "population": "primary", "hai_types": ["HAI_1", "HAI_2"], "mlflow": "aws"}
CHALLENGER = {"model": "m2", "version": "4", "verdict": "pass", "source_run": "mlrun4"}
FEATURES_RUNS = {
    "m1": "features_ols_0a0a0a01",
    "m2": "features_elastic_net_0a0a0a02",
    "m4": "features_gradient_boosting_0a0a0a04",
}  # the runs the models trained on: not derived
PHASES = {"research": "research", "canary_check": "canary", "promote": "promote", "owner_notice": "notify"}


def chain_parameters(template: str) -> dict[str, Any]:
    """The closed parameters of a launching step of the promotion chain."""
    return {
        "canary_check": {"research_run": RESEARCH, "hai_type": TYPE, "mlflow": "aws"},
        "owner_notice": {"research_run": RESEARCH, "hai_type": TYPE, "outcome": "promoted", "mlflow": "aws"},
        "promote": {"hai_type": TYPE, "model_version": "3", "promotion_mode": "promote", "mlflow": "aws"},
    }[template]


def release_parameters(model: str = "m2", hai_type: str = TYPE) -> dict[str, Any]:
    """The closed parameters of the release step: the type, the model of its challenger and the features run that model trained on."""
    return {"hai_type": hai_type, "model": model, "features_run": FEATURES_RUNS[model]}


def request_for(template: str, run: str, training: dict[str, Any], attempt: int = 1, input_run: str | None = None) -> SubmissionRequest:
    return SubmissionRequest(template, run, "real", attempt, "research_dag", template, "manual__test", input_run, training=training)


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


def research_dag_runs_a_later_attempt_as_a_run_of_its_own() -> None:
    """A research run that failed cannot launch again, so the form's research attempt starts another run of the same base: the job gets the attempt in its
    parameters and the base's code, and the first attempt, the default, sends none. An attempt the contract refuses never launches."""
    second = training_contract.research_run_id(BASE, 2)
    require(second != RESEARCH, "the second attempt derives the first attempt's run")
    for attempt, run, expected in ((2, second, {**RESEARCH_PARAMS, "research_attempt": 2}), (1, RESEARCH, RESEARCH_PARAMS)):
        rig = Rig()
        rig.stage_runs(run if attempt > 1 else training_contract.trained_run_id(BASE, "m1"))  # a later attempt runs the code staged for its own run
        with world(rig, "research_dag", dag_params(run_id=run, research_attempt=attempt)) as current:
            require(current.step("research") == "succeeded", f"the research step of attempt {attempt} did not pass")
        promote = rig.environment()["promote"]
        require(promote["RUN_ID"] == run, f"attempt {attempt} ran {promote['RUN_ID']}, not {run}")
        require(json.loads(promote["TRAINING_PARAMETERS"]) == expected, f"attempt {attempt} sent {promote['TRAINING_PARAMETERS']}")
    rig = Rig()
    rig.stage_runs(training_contract.trained_run_id(BASE, "m1"))
    for attempt in (0, 100):
        with world(rig, "research_dag", dag_params(run_id=second, research_attempt=attempt)) as current:
            error = refuses(ValueError, lambda: current.step("research"))
        require("research attempt" in str(error), f"attempt {attempt} was refused for another cause: {error}")
    require(rig.tasks_started() == 0, "a refused attempt launched a task")
    rig.stage_runs(second)
    error = refuses(LaunchRefused, partial(rig.preflight.prepare, request_for("research", second, {**RESEARCH_PARAMS, "research_attempt": 1})))
    require("research_attempt" in str(error), f"the preflight took the attempt 1 spelled out: {error}")


def code_runs_of(spec: Any) -> set[str]:
    """The code runs a launch spec tells its containers."""
    return {values["CODE_RUN_ID"] for values in spec.environment.values() if "CODE_RUN_ID" in values}


def a_later_attempt_runs_the_code_staged_for_its_own_run() -> None:
    """Attempt 1 runs the code the first model trained with and binds to it. A later attempt exists to run fixed code, so it runs the code staged for its own
    research run, never the first model's: with only its own code staged it launches with that code and the first model's code is not read. A binding the owner
    wrote for the research run is honored."""
    second, first_model = training_contract.research_run_id(BASE, 2), training_contract.trained_run_id(BASE, "m1")
    later = {**RESEARCH_PARAMS, "research_attempt": 2}
    rig = Rig()
    rig.stage_runs(first_model)
    spec = rig.preflight.prepare(request_for("research", RESEARCH, RESEARCH_PARAMS))
    require(spec is not None and code_runs_of(spec) == {first_model}, f"attempt 1 did not run the first model's code: {spec}")
    require(rig.preflight.code_runs.stored(RESEARCH) == first_model, "attempt 1 is not bound to the first model's code")
    rig = Rig()
    rig.stage_runs(second)
    spec = rig.preflight.prepare(request_for("research", second, later))
    require(spec is not None and code_runs_of(spec) == {second}, f"attempt 2 did not run its own staged code: {spec}")
    require(spec.bound["code_digest"] == rig.stage.objects[RUN_KEY.format(run=second, name="code.sha256")].decode().strip(), "attempt 2 bound another digest")
    require(not [entry for entry in rig.stage.log if first_model in entry], f"attempt 2 read the first model's code: {rig.stage.log}")
    rig = Rig()
    rig.stage_runs(first_model)
    rig.stage.objects[RUN_KEY.format(run=second, name="code_run.json")] = json.dumps({"code_run": first_model}).encode()
    spec = rig.preflight.prepare(request_for("research", second, later))
    require(spec is not None and code_runs_of(spec) == {first_model}, f"a binding of attempt 2 was not honored: {spec}")


def a_later_attempt_without_staged_code_is_refused_by_name() -> None:
    """A later attempt with nothing staged for its research run is refused naming the run and the missing object, even when the first model's code is staged:
    it never falls back to that code. The refusal comes before any binding, launch record or task."""
    second, first_model = training_contract.research_run_id(BASE, 2), training_contract.trained_run_id(BASE, "m1")
    rig = Rig()
    rig.stage_runs(first_model, RESEARCH)
    error = refuses(LaunchRefused, partial(rig.preflight.prepare, request_for("research", second, {**RESEARCH_PARAMS, "research_attempt": 2})))
    require(second in str(error) and "code.sha256" in str(error) and "attempt 2" in str(error), f"the refusal does not name the run and the object: {error}")
    require(not rig.ecs.calls and not rig.records.calls, "a refused attempt reached the cloud or the launch records")
    require(RUN_KEY.format(run=second, name="code_run.json") not in rig.stage.objects, "a refused attempt was bound to a code run")


def the_chain_of_a_later_attempt_runs_the_code_of_its_research_run() -> None:
    """The whole DAG of a later attempt, with all three models trained and the first model's code staged: the research task and each launching step of the
    attempt's chain bind their runs to the code staged for the attempt's research run and launch with it, not with the first model's code."""
    second = training_contract.research_run_id(BASE, 2)
    rig = Rig()
    for model in training_contract.PROMOTION_MODELS:
        trained(rig, model)
    rig.stage_runs(second)
    staged_for_the_dag(rig, research=second)
    before = rig.tasks_started()
    with world(rig, "research_dag", dag_params(run_id=second, research_attempt=2)) as current:
        states = current.run_dag("research_dag.py")
    require(set(states.values()) == {"success"} and current.results["gate"] == "accepted", f"the DAG of attempt 2 did not finish: {states} {current.results}")
    expected = [
        ("", "research"),
        ("HAI_1", "serve_canary"),
        ("HAI_1", "canary"),
        ("HAI_1", "promote"),
        ("m2", "release"),
        ("HAI_1", "serve_stable"),
        ("HAI_1", "notify"),
    ]
    require(launched_tasks(rig, before) == expected, f"tasks: {launched_tasks(rig, before)}")
    for index in range(before, rig.tasks_started()):
        named = code_runs_of(SimpleNamespace(environment=rig.environment(index)))
        require(named == {second}, f"task {index - before} ran the code of {named}, not of its research run {second}")
    for step in ("canary_deploy", "canary", "promote", "release", "stable_deploy", "notice"):
        run = training_contract.chain_run_id(second, "HAI_1", step)
        bound = json.loads(rig.stage.objects[RUN_KEY.format(run=run, name="code_run.json")])
        require(bound == {"code_run": second}, f"{step} of attempt 2 is bound to {bound}, not to the code of its research run")


def research_record(hai_type: str, challenger: Mapping[str, Any] | None, production: Any = "1", drop: tuple[str, ...] = ()) -> bytes:
    """A research record: ``production`` is the version the Production alias named when the research ran (None for no Production model), ``drop`` names keys
    an older record lacks (``production_version``, ``features_runs``)."""
    promotion = {"registered_model": f"hai_model_{hai_type}", "candidates": [], "challenger": challenger, "production_version": production}
    document = {"schema": 1, "hai_type": hai_type, "base_run": BASE, "runs": {}, "features_runs": FEATURES_RUNS, "promotion": promotion}
    for name in drop:
        document.pop(name, None)
        promotion.pop(name, None)
    return json.dumps(document).encode()


def canary_record(
    hai_type: str,
    decision: str,
    failed: tuple[str, ...] = (),
    challenger: Mapping[str, Any] = CHALLENGER,
    changes: dict[str, Any] | None = None,
    research: str = RESEARCH,
) -> bytes:
    document = {
        "schema": 1,
        "research_run": research,
        "hai_type": hai_type,
        "model": challenger["model"],
        "version": challenger["version"],
        "decision": decision,
        "failed": list(failed),
    } | (changes or {})
    return json.dumps(document).encode()


def stage_records(
    objects: dict[str, bytes],
    challengers: Mapping[str, Mapping[str, Any] | None],
    canary: str | None = "promote",
    production: str | None = "1",
    research: str = RESEARCH,
) -> None:
    """What the research and canary phases would have stored: a research record for each type and a canary result for each type with a challenger (none for
    a first champion, which has no canary). The chain reads no hold record: the release phase of the features family reads its own."""
    for hai_type, challenger in challengers.items():
        objects[training_contract.record_key("research", research, hai_type)] = research_record(hai_type, challenger, production)
        if challenger is not None and canary is not None and production is not None:
            objects[training_contract.record_key("canary", research, hai_type)] = canary_record(hai_type, canary, challenger=challenger, research=research)


class Recorder:
    """Stand-ins for the two things the chain calls: a launch of a step (the release of a held online load is one, in the features family) and a read of a
    stored record."""

    def __init__(
        self,
        challengers: Mapping[str, Mapping[str, Any] | None],
        canary: str | None = "promote",
        failing: tuple[tuple[str, str], ...] = (),
        production: str | None = "1",
    ) -> None:
        self.objects: dict[str, bytes] = {}
        stage_records(self.objects, challengers, canary, production)
        self.reads: list[str] = []
        self.calls: list[tuple[Any, ...]] = []
        self.types = tuple(challengers)
        self.failing = failing
        self.pending: dict[str, bytes] = {}

    def launch(self, step: str, run: str, training: dict[str, Any]) -> None:
        self.calls.append((step, training["hai_type"], run, training))
        if (step, training["hai_type"]) in self.failing:
            raise promotion_chain.StepFailed(f"{step} of {training['hai_type']} failed")

    def read(self, key: str) -> bytes | None:
        self.reads.append(key)
        return self.objects.get(key)

    def steps(self) -> list[tuple[str, str]]:
        """What the chain called, in order: a serve deploy is named by its mode (``serve_canary``, ``serve_stable``)."""
        return [(f"serve_{call[3]['deploy_mode']}" if call[0] == "serve_deploy" else call[0], call[1]) for call in self.calls]

    def run(self, hai_types: tuple[str, ...] | None = None) -> list[str]:
        return promotion_chain.run_types(RESEARCH, self.types if hai_types is None else hai_types, launch=self.launch, read=self.read)


def a_challenger_is_deployed_promoted_rolled_out_and_announced() -> None:
    """A type with a challenger runs its canary deploy, canary check, promotion, release, stable-only rollout and notice in that order, each with the run
    and the parameters of its own. Both deploys are closed requests: the type and the mode, with no version and no share. The release is a launch of the
    features family's release phase for the model and the features run it trained on, and the chain reads no hold record."""
    recorder = Recorder({"HAI_1": CHALLENGER, "HAI_2": None})
    require(recorder.run() == [], "a clean chain reported a failure")
    expected = [
        ("serve_canary", "HAI_1"),
        ("canary_check", "HAI_1"),
        ("promote", "HAI_1"),
        ("release_online", "HAI_1"),
        ("serve_stable", "HAI_1"),
        ("owner_notice", "HAI_1"),
    ]
    require(recorder.steps() == expected, f"steps: {recorder.steps()}")
    deploy, canary, promote, release, rollout, notice = recorder.calls
    require(deploy[2] == training_contract.chain_run_id(RESEARCH, "HAI_1", "canary_deploy"), f"the canary deploy run: {deploy[2]}")
    require(deploy[3] == {"hai_type": "HAI_1", "deploy_mode": "canary"}, f"the canary deploy names more than the type and the mode: {deploy[3]}")
    require(canary[2] == training_contract.chain_run_id(RESEARCH, "HAI_1", "canary") and canary[3] == chain_parameters("canary_check"), f"canary: {canary}")
    require(promote[2] == training_contract.chain_run_id(RESEARCH, "HAI_1", "promote"), f"the promotion run: {promote[2]}")
    require(promote[3] == {"hai_type": "HAI_1", "model_version": "4", "promotion_mode": "promote", "mlflow": "aws"}, f"promotion: {promote[3]}")
    require(release[2] == training_contract.chain_run_id(RESEARCH, "HAI_1", "release"), f"the release run: {release[2]}")
    require(release[3] == release_parameters(), f"the release names more than the type, the model and the run it trained on: {release[3]}")
    require(rollout[2] == training_contract.chain_run_id(RESEARCH, "HAI_1", "stable_deploy"), f"the rollout run: {rollout[2]}")
    require(rollout[3] == {"hai_type": "HAI_1", "deploy_mode": "stable"}, f"the rollout names more than the type and the mode: {rollout[3]}")
    require(notice[3] == {**chain_parameters("owner_notice"), "outcome": "promoted"}, f"notice: {notice[3]}")
    require(not [key for key in recorder.reads if "online_hold" in key], f"the chain read a hold record: {recorder.reads}")


def a_held_canary_promotes_nothing_deploys_nothing_more_and_tells_the_owner() -> None:
    recorder = Recorder({"HAI_1": CHALLENGER}, canary="hold")
    require(recorder.run() == [], "a held canary is a result, not a failure")
    expected = [("serve_canary", "HAI_1"), ("canary_check", "HAI_1"), ("owner_notice", "HAI_1")]
    require(recorder.steps() == expected, f"steps: {recorder.steps()}")
    require(recorder.calls[-1][3]["outcome"] == "held", f"notice: {recorder.calls[-1]}")


def a_canary_result_of_another_candidate_is_a_hold_and_a_failure() -> None:
    for changes in ({"version": "5"}, {"model": "m4"}, {"hai_type": "HAI_2"}, {"research_run": training_contract.research_run_id("features_b45e0002")}):
        recorder = Recorder({"HAI_1": CHALLENGER})
        recorder.objects[training_contract.record_key("canary", RESEARCH, "HAI_1")] = canary_record("HAI_1", "promote", changes=changes)
        failures = recorder.run()
        require(len(failures) == 1 and "canary" in failures[0], f"a canary result of another candidate was not reported: {changes} {failures}")
        require(
            ("promote", "HAI_1") not in recorder.steps() and recorder.calls[-1][3]["outcome"] == "held" and ("serve_stable", "HAI_1") not in recorder.steps(),
            f"it promoted, rolled out or did not tell the owner: {changes} {recorder.steps()}",
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
    complete = ["serve_canary", "canary_check", "promote", "release_online", "serve_stable", "owner_notice"]
    for step in ("serve_canary", "canary_check", "promote", "owner_notice"):
        failing = (("serve_deploy", "HAI_1"),) if step.startswith("serve") else ((step, "HAI_1"),)
        recorder = Recorder(both, failing=failing)
        failures = recorder.run()
        require(bool(failures) and all("HAI_1" in failure for failure in failures), f"{step}: failures {failures}")
        other = [name for name, hai_type in recorder.steps() if hai_type == "HAI_2"]
        require(other == complete, f"HAI_2 did not complete after HAI_1 failed at {step}: {other}")
    unresolved = Recorder(both, failing=(("promote", "HAI_1"),))
    unresolved.run()
    steps = unresolved.steps()
    require(("owner_notice", "HAI_1") not in steps and ("release_online", "HAI_1") not in steps, "a failed promotion was announced or released")
    require(("serve_stable", "HAI_1") not in steps, "a failed promotion was rolled out")
    unannounced = Recorder(both, failing=(("canary_check", "HAI_1"),))
    unannounced.run()
    require(
        ("promote", "HAI_1") not in unannounced.steps() and ("owner_notice", "HAI_1") in unannounced.steps(),
        "a failed canary was promoted or not announced as held",
    )
    undeployed = Recorder(both, failing=(("serve_deploy", "HAI_1"),))
    failures = undeployed.run()
    require(
        ("canary_check", "HAI_1") not in undeployed.steps() and ("promote", "HAI_1") not in undeployed.steps(),
        "a canary that did not deploy was checked or promoted",
    )
    require(any("canary deploy" in failure for failure in failures), f"a failed canary deploy was not reported: {failures}")
    notice = [call for call in undeployed.calls if call[0] == "owner_notice" and call[1] == "HAI_1"]
    require(len(notice) == 1 and notice[0][3]["outcome"] == "held", f"a canary that did not deploy was not announced as held: {notice}")


def a_failed_release_or_rollout_is_announced_as_not_rolled_out() -> None:
    """A promotion that the API never serves is never announced as promoted: a failed release stops the rollout, a failed rollout is reported, and both
    notices say the promotion did not roll out."""
    recorder = Recorder({"HAI_1": CHALLENGER}, failing=(("release_online", "HAI_1"),))
    failures = recorder.run()
    require(
        len(failures) == 1 and "HAI_1 release:" in failures[0] and "release_online of HAI_1 failed" in failures[0],
        f"a failed release was not reported: {failures}",
    )
    require(("serve_stable", "HAI_1") not in recorder.steps(), "the rollout ran after a failed release")
    require(recorder.calls[-1][0] == "owner_notice" and recorder.calls[-1][3]["outcome"] == "promoted_not_rolled_out", f"the notice: {recorder.calls[-1]}")

    class Stopping(Recorder):
        def launch(self, step: str, run: str, training: dict[str, Any]) -> None:
            super().launch(step, run, training)
            if step == "serve_deploy" and training["deploy_mode"] == "stable":
                raise promotion_chain.StepFailed("the stable deployment failed")

    stopped = Stopping({"HAI_1": CHALLENGER})
    failures = stopped.run()
    require(len(failures) == 1 and "rollout" in failures[0], f"a failed rollout was not reported: {failures}")
    require(stopped.calls[-1][3]["outcome"] == "promoted_not_rolled_out", f"the notice after a failed rollout: {stopped.calls[-1]}")


def launched_tasks(rig: Rig, start: int = 0) -> list[tuple[str, str]]:
    """The tasks the rig started, in order, as (type, what it did): a serve deploy by its mode, a promote-family task by its phase and a features-family task
    that runs the release phase in its feast container as (its model, ``release``): the release names a model and a run, not a type."""
    found = []
    for index in range(start, rig.tasks_started()):
        environment = rig.environment(index)
        if "serve" in environment:
            request = json.loads(environment["serve"]["SERVING_REQUEST"])
            found.append((request["hai_type"], f"serve_{request['deploy_mode']}"))
        elif "feast" in environment:
            found.append((environment["feast"]["RELEASE_MODEL"], environment["feast"]["LAYER_PHASE"]))
        else:
            found.append((json.loads(environment["promote"]["TRAINING_PARAMETERS"]).get("hai_type", ""), environment["promote"]["PHASE"]))
    return found


def one_types_binding_failure_stops_only_its_chain() -> None:
    """A failure to bind one step's run to its code (a throttled or refused write of the binding record) ends that type's chain with a named failure and a hold
    notice, and the other type's chain still runs. A denied and a throttled write are both ordinary runtime boundaries, so both are tried."""
    both = {"HAI_1": CHALLENGER, "HAI_2": {**CHALLENGER, "model": "m4", "version": "7"}}
    first = training_contract.chain_run_id(RESEARCH, "HAI_1", "canary_deploy")
    for code in ("AccessDenied", "SlowDown"):
        rig = Rig()
        rig.stage_runs(RESEARCH)
        stage_records(rig.stage.objects, both)
        original = rig.stage.put_object
        key = rig.preflight.code_runs.key(first)

        def put(code: str = code, original: Callable[..., Any] = original, key: str = key, **arguments: Any) -> Any:
            if arguments["Key"] == key:
                raise ClientError({"Error": {"Code": code}}, "PutObject")
            return original(**arguments)

        with world(rig, "research_dag", dag_params()) as current, mock.patch.object(rig.stage, "put_object", side_effect=put):
            current.enter("chain")
            error = refuses(RuntimeError, lambda: airflow_steps.research_chain_step(request_for("research", RESEARCH, RESEARCH_PARAMS), current.context))
        require("HAI_1 canary deploy" in str(error) and code in str(error), f"{code}: the failure does not name the type, the step and the cause: {error}")
        phases = launched_tasks(rig)
        require(
            phases
            == [
                ("HAI_1", "notify"),
                ("HAI_2", "serve_canary"),
                ("HAI_2", "canary"),
                ("HAI_2", "promote"),
                ("m4", "release"),
                ("HAI_2", "serve_stable"),
                ("HAI_2", "notify"),
            ],
            f"{code}: HAI_1 was not held and announced, or HAI_2 did not run in full: {phases}",
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


def a_first_champion_skips_the_canary_and_is_promoted_and_rolled_out() -> None:
    """With no Production model in the research record, the chain launches no canary deploy and no canary check, reads no canary result, promotes the
    challenger, launches no release (a first deploy has no hold: owner decision B, Nov 15 2026), rolls out stable-only and announces it."""
    recorder = Recorder({"HAI_1": CHALLENGER, "HAI_2": None}, production=None)
    require(recorder.run() == [], "a clean first-champion chain reported a failure")
    expected = [("promote", "HAI_1"), ("serve_stable", "HAI_1"), ("owner_notice", "HAI_1")]
    require(recorder.steps() == expected, f"steps: {recorder.steps()}")
    require(not [key for key in recorder.reads if "canary_" in key], f"the first champion read a canary result: {recorder.reads}")
    require(not [key for key in recorder.reads if "online_hold" in key], f"the first champion read a hold key: {recorder.reads}")
    promote, rollout, notice = recorder.calls
    require(promote[3] == {"hai_type": "HAI_1", "model_version": "4", "promotion_mode": "promote", "mlflow": "aws"}, f"promotion: {promote[3]}")
    require(rollout[3] == {"hai_type": "HAI_1", "deploy_mode": "stable"}, f"rollout: {rollout[3]}")
    require(notice[3] == {**chain_parameters("owner_notice"), "outcome": "promoted"}, f"notice: {notice[3]}")
    both = Recorder({"HAI_1": CHALLENGER, "HAI_2": {**CHALLENGER, "model": "m4", "version": "7"}}, production=None)
    require(
        both.run() == [] and [name for name, hai_type in both.steps() if hai_type == "HAI_2"] == ["promote", "serve_stable", "owner_notice"], "HAI_2 differs"
    )


def a_first_champion_does_not_need_a_canary_result_a_later_promotion_does() -> None:
    """A research record that names a Production version, or none of the key at all, keeps the canary: only an explicit empty Production skips it."""
    later = Recorder({"HAI_1": CHALLENGER}, canary=None, production="1")
    failures = later.run()
    require(
        len(failures) == 1 and ("promote", "HAI_1") not in later.steps() and later.calls[-1][3]["outcome"] == "held",
        f"a later promotion went on: {later.steps()}",
    )
    unknown = Recorder({"HAI_1": CHALLENGER}, canary=None)
    unknown.objects[training_contract.record_key("research", RESEARCH, "HAI_1")] = research_record("HAI_1", CHALLENGER, drop=("production_version",))
    failures = unknown.run()
    require(
        ("serve_canary", "HAI_1") in unknown.steps() and ("promote", "HAI_1") not in unknown.steps(),
        f"a record without the key skipped the canary: {unknown.steps()}",
    )
    keyless = Recorder({"HAI_1": CHALLENGER})
    keyless.objects[training_contract.record_key("research", RESEARCH, "HAI_1")] = research_record("HAI_1", CHALLENGER, drop=("production_version",))
    require(keyless.run() == [] and ("release_online", "HAI_1") in keyless.steps(), f"a record without the key skipped the release: {keyless.steps()}")
    passed = Recorder({"HAI_1": CHALLENGER}, production="1")
    require(passed.run() == [], "a later promotion with a passed canary failed")
    expected = [
        ("serve_canary", "HAI_1"),
        ("canary_check", "HAI_1"),
        ("promote", "HAI_1"),
        ("release_online", "HAI_1"),
        ("serve_stable", "HAI_1"),
        ("owner_notice", "HAI_1"),
    ]
    require(passed.steps() == expected, f"a later promotion: {passed.steps()}")


def a_production_model_gets_a_release_launch_and_a_first_champion_none() -> None:
    """With a Production model the chain launches the release phase of the features family after the promotion and before the rollout, whatever the
    features run holds: the phase reads its own hold and decides released, already released or not held, so the chain reads no hold key. It names the
    features run that the research record gives for the model, never the run derived from the base run. A first champion launches no release."""
    derived = training_contract.model_features_run_id(BASE, "m2")
    require(derived != FEATURES_RUNS["m2"], "the fixture's real features run equals the derived run")
    real = Recorder({"HAI_1": CHALLENGER})
    real.objects[f"lakehouse/layer_runs/features/{derived}/online_hold.json"] = b"{}"
    require(real.run() == [] and ("release_online", "HAI_1") in real.steps(), f"the release was not launched: {real.steps()}")
    release = [call for call in real.calls if call[0] == "release_online"]
    require(len(release) == 1 and release[0][3]["features_run"] == FEATURES_RUNS["m2"], f"the release named another run: {release}")
    require(not [key for key in real.reads if "online_hold" in key], f"the chain read a hold key: {real.reads}")
    steps = real.steps()
    require(steps.index(("promote", "HAI_1")) < steps.index(("release_online", "HAI_1")) < steps.index(("serve_stable", "HAI_1")), f"release order: {steps}")
    first = Recorder({"HAI_1": CHALLENGER}, production=None, failing=(("release_online", "HAI_1"),))
    require(first.run() == [], "a first champion was stopped by a release it never launches")
    require(first.steps() == [("promote", "HAI_1"), ("serve_stable", "HAI_1"), ("owner_notice", "HAI_1")], f"steps: {first.steps()}")
    require(not [key for key in first.reads if "online_hold" in key], f"a first champion read a hold: {first.reads}")
    again = Recorder({"HAI_1": CHALLENGER})
    again.run()
    again.run()
    runs = [call[2] for call in again.calls if call[0] == "release_online"]
    require(len(runs) == 2 and runs[0] == runs[1] == training_contract.chain_run_id(RESEARCH, "HAI_1", "release"), f"a retry names another release run: {runs}")


def research_records_without_a_real_features_run_are_refused() -> None:
    for name, record in (
        ("no_map", research_record("HAI_1", CHALLENGER, drop=("features_runs",))),
        ("bad_run", research_record("HAI_1", CHALLENGER).replace(FEATURES_RUNS["m2"].encode(), b"not-a-run")),
        ("bad_production", research_record("HAI_1", CHALLENGER, production=4)),
    ):
        recorder = Recorder({"HAI_1": CHALLENGER})
        recorder.objects[training_contract.record_key("research", RESEARCH, "HAI_1")] = record
        failures = recorder.run(("HAI_1",))
        require(len(failures) == 1 and not recorder.calls, f"{name}: failures {failures}, calls {recorder.calls}")


OTHER_BASE = "features_b45e0002"
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


def staged_for_the_dag(rig: Rig, canary: str | None = "promote", production: str | None = "1", research: str = RESEARCH) -> None:
    stage_records(rig.stage.objects, {"HAI_1": CHALLENGER, "HAI_2": None}, canary, production, research)


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
    """All three models trained: research runs in the promote family, then the challenger's canary deploy in the serve family, its canary check, the
    promotion, the release phase of the features family, the stable-only rollout and the notice, with their phases, runs and derived serving requests. The
    release task runs only its own phase for the model and the features run the research record names, and the DAG reads no hold key."""
    rig = Rig()
    for model in training_contract.PROMOTION_MODELS:
        trained(rig, model)
    staged_for_the_dag(rig)
    before = rig.tasks_started()
    with world(rig, "research_dag", dag_params()) as current:
        states = current.run_dag("research_dag.py")
    require(set(states.values()) == {"success"} and current.results["gate"] == "accepted", f"the DAG did not finish: {states} {current.results}")
    require(rig.tasks_started() == before + 7, f"seven tasks were expected: {rig.tasks_started() - before}")
    expected = [
        ("", "research"),
        ("HAI_1", "serve_canary"),
        ("HAI_1", "canary"),
        ("HAI_1", "promote"),
        ("m2", "release"),
        ("HAI_1", "serve_stable"),
        ("HAI_1", "notify"),
    ]
    require(launched_tasks(rig, before) == expected, f"tasks: {launched_tasks(rig, before)}")
    steps = ("canary_deploy", "canary", "promote", "release", "stable_deploy", "notice")
    runs = [training_contract.chain_run_id(RESEARCH, "HAI_1", step) for step in steps]
    environments = [rig.environment(before + index) for index in range(7)]
    require(
        [item["serve" if "serve" in item else "feast" if "feast" in item else "promote"]["RUN_ID"] for item in environments] == [RESEARCH, *runs],
        f"runs: {environments}",
    )
    canary_request = json.loads(environments[1]["serve"]["SERVING_REQUEST"])
    require(
        canary_request
        == {"hai_type": "HAI_1", "deploy_mode": "canary", "candidate_version": "4", "percent": 10, "canary_ttl_hours": 24, "research_run": RESEARCH},
        f"the derived canary request: {canary_request}",
    )
    release = environments[4]
    for phase in ("pin", "build", "validate", "export", "publish", "upload", "feast"):
        require(release[phase]["LAYER_PHASE"] == "release", f"the {phase} container was not told the release phase: {release[phase]}")
    require("release" not in release, f"the features task has a release container: {sorted(release)}")
    require(release["feast"]["RELEASE_MODEL"] == "m2" and release["feast"]["FEATURES_RUN"] == FEATURES_RUNS["m2"], f"the release names: {release['feast']}")
    require(release["feast"]["RUN_ID"] == runs[3], f"the release ran another run: {release['feast']}")
    end = rig.end(runs[3], "features")
    require(end is not None and end["outcome"] == "succeeded", f"the release left {end}")
    stable_request = json.loads(environments[5]["serve"]["SERVING_REQUEST"])
    require(
        stable_request
        == {"hai_type": "HAI_1", "deploy_mode": "stable", "candidate_version": None, "percent": 0, "canary_ttl_hours": None, "research_run": None},
        f"the derived stable request: {stable_request}",
    )
    require(json.loads(environments[6]["promote"]["TRAINING_PARAMETERS"])["outcome"] == "promoted", "the owner was not told promoted")
    require(not [entry for entry in rig.stage.log if "online_hold" in entry], f"the DAG read a hold key: {rig.stage.log}")
    for run in (RESEARCH, *runs):
        bound = json.loads(rig.stage.objects[RUN_KEY.format(run=run, name="code_run.json")])
        require(bound == {"code_run": training_contract.trained_run_id(BASE, "m1")}, f"{run} is bound to {bound}, not to the code the models trained with")
    again = rig.tasks_started()
    with world(rig, "research_dag", dag_params()) as current:
        current.run_dag("research_dag.py")
    require(rig.tasks_started() == again, "a second run of the DAG for the same base launched again")


def research_dag_promotes_a_first_champion_without_a_canary() -> None:
    """No Production model: the DAG launches research, the promotion, the stable-only rollout and the notice, with no canary deploy or check and no release.
    It never reads a hold key (owner decision B, Nov 15 2026): a first deploy has no hold."""
    rig = Rig()
    for model in training_contract.PROMOTION_MODELS:
        trained(rig, model)
    staged_for_the_dag(rig, canary=None, production=None)
    before = rig.tasks_started()
    with world(rig, "research_dag", dag_params()) as current:
        states = current.run_dag("research_dag.py")
    require(set(states.values()) == {"success"} and current.results["gate"] == "accepted", f"the DAG did not finish: {states} {current.results}")
    expected = [("", "research"), ("HAI_1", "promote"), ("HAI_1", "serve_stable"), ("HAI_1", "notify")]
    require(launched_tasks(rig, before) == expected, f"tasks: {launched_tasks(rig, before)}")
    require(not [entry for entry in rig.stage.log if "online_hold" in entry], f"the first champion read a hold key: {rig.stage.log}")
    notice = json.loads(rig.environment(rig.tasks_started() - 1)["promote"]["TRAINING_PARAMETERS"])
    require(notice["outcome"] == "promoted", f"the owner was told {notice['outcome']}")


def research_dag_fails_closed_when_the_release_task_is_refused() -> None:
    """A Production model launches the release phase after the promotion. When the phase refuses (the feast container exits 1, as online_release does for a
    refused release), the promotion stands, no rollout runs, the notice says the promotion did not roll out and the DAG run fails naming the release."""
    rig = Rig()
    for model in training_contract.PROMOTION_MODELS:
        trained(rig, model)
    staged_for_the_dag(rig)
    rig.ecs.plan = {"feast": 1}
    before = rig.tasks_started()
    with world(rig, "research_dag", dag_params()) as current:
        states = current.run_dag("research_dag.py")
    require(states["chain"] == "failed" and states["gate"] == "failed", f"states: {states}")
    require("HAI_1 release" in str(current.results["chain"]), f"the failure does not name the release step: {current.results['chain']}")
    require(not [entry for entry in rig.stage.log if "online_hold" in entry], f"the DAG read a hold key: {rig.stage.log}")
    end = rig.end(training_contract.chain_run_id(RESEARCH, "HAI_1", "release"), "features")
    require(end is not None and end["outcome"] == "failed", f"the refused release left {end}")
    expected = [("", "research"), ("HAI_1", "serve_canary"), ("HAI_1", "canary"), ("HAI_1", "promote"), ("m2", "release"), ("HAI_1", "notify")]
    require(launched_tasks(rig, before) == expected, f"tasks: {launched_tasks(rig, before)}")
    notice = json.loads(rig.environment(rig.tasks_started() - 1)["promote"]["TRAINING_PARAMETERS"])
    require(notice["outcome"] == "promoted_not_rolled_out", f"the owner was told {notice['outcome']}")


def the_release_step_takes_only_its_closed_parameters() -> None:
    """The release step of the features family takes the infection type, the model of its challenger and the features run that model trained on, and nothing
    else: another model, a run that is not a run ID, a missing or extra name and another DAG are refused before any cloud call or record. The accepted request
    names the features job, runs only the release phase and takes no feature set."""
    rig = Rig()
    run = training_contract.chain_run_id(RESEARCH, TYPE, "release")
    good = release_parameters()
    rig.stage_runs(RESEARCH, run)
    refused = (
        {**good, "model": "m3"},
        {**good, "model": "m9"},
        {**good, "hai_type": "HAI_9"},
        {**good, "features_run": "latest"},
        {**good, "features_run": "x"},
        {**good, "mlflow": "aws"},
        {key: value for key, value in good.items() if key != "features_run"},
    )
    for training in refused:
        error = refuses(LaunchRefused, partial(rig.preflight.prepare, request_for("release_online", run, training, input_run=good["features_run"])))
        require("no ECS job" not in str(error), f"release_online is not a step of the table, so its parameters were never checked: {error}")
    error = refuses(LaunchRefused, partial(rig.preflight.prepare, request_for("release_online", run, good, input_run=None)))
    require("features run" in str(error), f"a release with no features run was not refused by name: {error}")
    error = refuses(LaunchRefused, partial(rig.preflight.prepare, request_for("release_online", run, good, input_run=FEATURES_RUNS["m4"])))
    require("features run" in str(error), f"a release whose input run differs from its parameters was not refused by name: {error}")
    require(not rig.ecs.calls and not rig.records.calls, "a release with other parameters reached the cloud or the launch records")
    spec = rig.preflight.prepare(request_for("release_online", run, good, input_run=good["features_run"]))
    require(spec is not None and spec.job == "features", f"the release did not name the features job: {spec}")
    require("release" not in spec.environment, f"the features task has a release container: {sorted(spec.environment)}")
    release = spec.environment["feast"]
    require(
        release["LAYER_PHASE"] == "release" and release["RELEASE_MODEL"] == "m2" and release["FEATURES_RUN"] == good["features_run"],
        f"the feast container: {release}",
    )
    for name, variables in spec.environment.items():
        require(
            variables.get("FEATURE_SET", "") == "" and variables.get("LAYER_PHASE", "release") == "release", f"{name} takes another set or phase: {variables}"
        )


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
        requests = training_contract.research_requests(model, training_run, BASE, "primary", ["HAI_1", "HAI_2"], "aws")
        if model not in training_contract.PROMOTION_MODELS:
            require(requests == [], f"{model} started the research: {requests}")
            continue
        require(
            requests
            == [{"trigger_run_id": training_contract.research_dag_run_id(training_run), "conf": {"run_id": RESEARCH, "mode": "real", **RESEARCH_PARAMS}}],
            f"{model} started another research: {requests}",
        )
    require(training_contract.research_requests("m2", training_run, "", "primary", ["HAI_1"], "aws") == [], "a run with no base started the research")
    first = training_contract.research_requests("m1", training_contract.trained_run_id(BASE, "m1"), BASE, "primary", ["HAI_1"], "aws")
    second = training_contract.research_requests("m4", training_contract.trained_run_id(BASE, "m4"), BASE, "primary", ["HAI_1"], "aws")
    require(
        first[0]["conf"]["run_id"] == second[0]["conf"]["run_id"] and first[0]["trigger_run_id"] != second[0]["trigger_run_id"],
        "the models' triggers share a run",
    )
    refuses(ValueError, partial(training_contract.research_requests, "m2", training_run, BASE, "primary", ["HAI_9"], "aws"))
    refuses(ValueError, partial(training_contract.research_requests, "m2", training_run, BASE, "primary", ["HAI_1"], "local"))


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
    "a_challenger_is_deployed_promoted_rolled_out_and_announced": a_challenger_is_deployed_promoted_rolled_out_and_announced,
    "a_held_canary_promotes_nothing_deploys_nothing_more_and_tells_the_owner": a_held_canary_promotes_nothing_deploys_nothing_more_and_tells_the_owner,
    "a_canary_result_of_another_candidate_is_a_hold_and_a_failure": a_canary_result_of_another_candidate_is_a_hold_and_a_failure,
    "a_missing_or_unreadable_canary_result_holds": a_missing_or_unreadable_canary_result_holds,
    "one_type_failing_stops_only_its_chain": one_type_failing_stops_only_its_chain,
    "a_failed_release_or_rollout_is_announced_as_not_rolled_out": a_failed_release_or_rollout_is_announced_as_not_rolled_out,
    "one_types_binding_failure_stops_only_its_chain": one_types_binding_failure_stops_only_its_chain,
    "bad_research_records_are_failures_and_launch_nothing": bad_research_records_are_failures_and_launch_nothing,
    "the_chain_is_the_same_for_a_retry": the_chain_is_the_same_for_a_retry,
    "research_dag_form_and_graph": research_dag_form_and_graph,
    "graph_rules_cover_the_research_dag": graph_rules_cover_the_research_dag,
    "each_promotion_model_starts_the_research_on_its_base": each_promotion_model_starts_the_research_on_its_base,
    "training_dag_starts_the_research_after_its_release": training_dag_starts_the_research_after_its_release,
    "research_dag_skips_until_three_models_trained": research_dag_skips_until_three_models_trained,
    "research_dag_runs_the_chain_for_the_challenger": research_dag_runs_the_chain_for_the_challenger,
    "research_dag_fails_closed_when_the_release_task_is_refused": research_dag_fails_closed_when_the_release_task_is_refused,
    "the_release_step_takes_only_its_closed_parameters": the_release_step_takes_only_its_closed_parameters,
    "research_dag_promotes_a_first_champion_without_a_canary": research_dag_promotes_a_first_champion_without_a_canary,
    "a_first_champion_skips_the_canary_and_is_promoted_and_rolled_out": a_first_champion_skips_the_canary_and_is_promoted_and_rolled_out,
    "a_first_champion_does_not_need_a_canary_result_a_later_promotion_does": a_first_champion_does_not_need_a_canary_result_a_later_promotion_does,
    "a_production_model_gets_a_release_launch_and_a_first_champion_none": a_production_model_gets_a_release_launch_and_a_first_champion_none,
    "research_records_without_a_real_features_run_are_refused": research_records_without_a_real_features_run_are_refused,
    "research_dag_refuses_the_local_lane_before_anything_runs": research_dag_refuses_the_local_lane_before_anything_runs,
    "every_promote_step_gets_its_phase": every_promote_step_gets_its_phase,
    "no_other_job_is_told_a_phase": no_other_job_is_told_a_phase,
    "steps_refuse_the_parameters_of_another_step": steps_refuse_the_parameters_of_another_step,
    "research_dag_step_launches_with_its_form": research_dag_step_launches_with_its_form,
    "research_dag_runs_a_later_attempt_as_a_run_of_its_own": research_dag_runs_a_later_attempt_as_a_run_of_its_own,
    "a_later_attempt_runs_the_code_staged_for_its_own_run": a_later_attempt_runs_the_code_staged_for_its_own_run,
    "a_later_attempt_without_staged_code_is_refused_by_name": a_later_attempt_without_staged_code_is_refused_by_name,
    "the_chain_of_a_later_attempt_runs_the_code_of_its_research_run": the_chain_of_a_later_attempt_runs_the_code_of_its_research_run,
}
