"""End-to-end cases for the lean ECS broker: a DAG task reaches the real HTTP listener, which reaches the fake ECS client.

The airflow SDK is replaced by a small stand-in (the package is not installed here), so ``orchestration.dag_runtime.run_step`` runs unchanged against
the real ``BrokerClient``. Cluster time is the fake's poll count and the broker's clock is a list the cases move by hand. Nothing leaves the machine.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import sys
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from orchestration.client import BrokerClient, BrokerUnavailable
from orchestration.ecs_adapter import EcsAdapter
from orchestration.interface import Handle, State, SubmissionRefused, SubmissionRequest
from scripts.orchestration.ecs_adapter_cases import SETTINGS, raises, require
from scripts.orchestration.ecs_broker import BrokerSettings, EcsBroker, Listener, derived_run_id, load_jobs, s3_digest_reader, s3_object_reader
from scripts.orchestration.ecs_broker_fake import FakeEcs, FakeLogs, FakeS3, client_error

ROOT = Path(__file__).resolve().parents[2]
SESSION_KEY = "stand-in-session-token-0123456789"
RUN = "20261030T100000Z-abcdef01"
SILVER_RUN = "20261030T100000Z-0badcafe"
GOLD_RUN = "20261030T100000Z-90d0c0de"
FEATURES_RUN = "20261030T100000Z-feed5eed"
BRONZE_RUN = "20261030T090000Z-b0b0b0b0"
PROMOTE_RUN = "20261030T110000Z-9a9a9a9a"
SIGNED = json.dumps({"silver": SILVER_RUN, "gold": GOLD_RUN})
DIGEST = "ab" * 32
PARAMETERS = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "local"}
JOB_FAMILIES = frozenset({"train", "silver", "gold", "features", "drift", "promote"})


class Rig:
    """One broker with its listener, a client and the fakes behind it."""

    def __init__(self, folder: Path, families: frozenset[str] = JOB_FAMILIES, signed: str | None = SIGNED) -> None:
        self.folder = folder
        self.signed = signed
        self.ecs, self.logs, self.s3 = FakeEcs(), FakeLogs(), FakeS3()
        self.s3.objects[f"lakehouse/run_stage/{RUN}/inputs/code.sha256"] = DIGEST.encode() + b"\n"
        for run in (SILVER_RUN, GOLD_RUN, FEATURES_RUN, PROMOTE_RUN):
            self.s3.objects[f"lakehouse/run_stage/{run}/inputs/code.sha256"] = DIGEST.encode() + b"\n"
        self.clock = [1_800_000_000.0]
        self.families = families
        self.broker = self.new_broker()
        self.listener = Listener(self.broker, "127.0.0.1", 0, SESSION_KEY)
        self.listener.start()
        self.client = BrokerClient(f"http://127.0.0.1:{self.listener.port}", SESSION_KEY, timeout=5)

    def new_broker(self) -> EcsBroker:
        settings = dataclasses.replace(SETTINGS, families=self.families)
        settings_broker = BrokerSettings(polaris_uri="http://10.0.0.5:8181/api/catalog", mlflow_uri="http://10.0.0.5:5100")
        digest = s3_digest_reader(self.s3, "example-bucket")
        approvals = s3_object_reader(self.s3, "example-bucket", "promotion_approval.json")
        return EcsBroker(
            self.folder / "state",
            load_jobs(ROOT),
            EcsAdapter(self.ecs, self.logs, settings),
            settings_broker,
            digest,
            lambda: self.signed,
            approvals,
            lambda: self.clock[0],
        )

    def close(self) -> None:
        self.listener.stop()


@contextmanager
def rig(families: frozenset[str] = JOB_FAMILIES, signed: str | None = SIGNED) -> Iterator[Rig]:
    with tempfile.TemporaryDirectory(prefix="ecs_broker_") as folder:
        current = Rig(Path(folder), families, signed)
        try:
            yield current
        finally:
            current.close()


def request(
    template: str, *, run: str = RUN, mode: str = "real", attempt: int = 1, dag: str = "training_dag", input_run: str | None = None
) -> SubmissionRequest:
    training = PARAMETERS if dag == "training_dag" else None
    return SubmissionRequest(template, run, mode, attempt, dag, template, "manual__test", input_run, None, training)


def wait_for(current: Rig, handle: Handle, limit: int = 20) -> Any:
    for _ in range(limit):
        status = current.client.status(handle)
        if status.state not in (State.PENDING, State.RUNNING):
            return status
    raise AssertionError("the submission never ended")


@contextmanager
def dag_runtime(current: Rig, dag_id: str, params: dict[str, Any]) -> Iterator[Callable[[str], str]]:
    """``run_step`` of the unchanged DAG runtime, with the SDK stand-in and the rig's client."""
    sdk = ModuleType("airflow.sdk")
    for name, value in (
        ("Asset", str),
        ("TriggerRule", SimpleNamespace(ALL_SUCCESS="all_success", ALL_DONE="all_done")),
        ("task", lambda **_: lambda f: f),
        ("get_current_context", dict),
    ):
        setattr(sdk, name, value)
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, {"airflow": ModuleType("airflow"), "airflow.sdk": sdk}):
            runtime = importlib.import_module("orchestration.dag_runtime")
            instance = SimpleNamespace(try_number=1, dag_id=dag_id, task_id="step", run_id="manual__test")
            with (
                mock.patch.object(runtime, "get_current_context", return_value={"params": params, "ti": instance}),
                mock.patch.object(runtime.BrokerClient, "from_session", return_value=current.client),
                mock.patch.object(runtime, "POLL_SECONDS", 0.01),
            ):
                yield runtime.run_step
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous


TRAIN_PARAMS = {"run_id": RUN, "mode": "real", **PARAMETERS}


def task_runs_through_the_broker() -> None:
    with rig() as current, dag_runtime(current, "training_dag", TRAIN_PARAMS) as run_step:
        require(run_step("guard") == "succeeded", "the first training step did not pass")
        require(len(current.ecs.tasks) == 1, "the first step did not start exactly one task")
        sent = next(arguments for name, arguments in current.ecs.calls if name == "run_task")
        values = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in sent["overrides"]["containerOverrides"]}
        require(sent["taskDefinition"] == "example_dev_job_train", "the train step ran another definition")
        require(values["fetch"] == {"RUN_ID": RUN, "CODE_SHA256": DIGEST}, f"fetch variables are wrong: {values['fetch']}")
        require(
            json.loads(values["prepare"]["TRAINING_PARAMETERS"]) == {**PARAMETERS, "mlflow": "aws"},
            "the training parameters did not reach the prepare container",
        )
        require(
            values["extract"]["POLARIS_URI"] == "http://10.0.0.5:8181/api/catalog" and values["train"]["MLFLOW_TRACKING_URI"] == "http://10.0.0.5:5100",
            "wrong URIs",
        )
        for step in ("extract", "fit", "log_run"):
            require(run_step(step) == "succeeded", f"{step} did not pass")
        require(len(current.ecs.tasks) == 1, "later steps of the job started more tasks")


def repeated_submit_one_task() -> None:
    with rig() as current:
        first, second = current.client.submit(request("guard")), current.client.submit(request("guard"))
        require(first == second and len(current.ecs.tasks) == 1 and current.ecs.count("run_task") == 1, "a retried submit started a second task")
        lines = [json.loads(line) for line in (current.folder / "state" / f"{RUN}.jsonl").read_text().splitlines()]
        require([line["event"] for line in lines] == ["intent", "started"], f"the retry changed the ledger: {[line['event'] for line in lines]}")


def finished_job_is_not_started_again() -> None:
    with rig() as current:
        wait_for(current, current.client.submit(request("guard")))
        current.ecs.tokens.clear()
        status = wait_for(current, current.client.submit(request("fit")))
        require(status.state == State.SUCCEEDED and len(current.ecs.tasks) == 1 and current.ecs.count("run_task") == 1, "a finished job was started again")
        new_attempt = current.client.submit(request("guard", attempt=2))
        wait_for(current, new_attempt)
        require(len(current.ecs.tasks) == 2, "a new attempt did not start its own task")


def container_failure_fails_the_job() -> None:
    with rig() as current:
        current.ecs.plan = {"init": 0, "fetch": 0, "prepare": 0, "extract": 0, "train": 3}
        status = wait_for(current, current.client.submit(request("fit")))
        require(status.state == State.FAILED and status.exit_code == 3, f"a failing container did not fail the job: {status}")
        joined = wait_for(current, current.client.submit(request("log_run")))
        require(joined.state == State.FAILED and joined.exit_code == 3 and len(current.ecs.tasks) == 1, "a later step did not report the same failure")
        with dag_runtime(current, "training_dag", TRAIN_PARAMS) as run_step:
            raises(RuntimeError, lambda: run_step("guard"))


def deadline_stops_the_task() -> None:
    with rig() as current:
        current.ecs.hold = True
        handle = current.client.submit(request("fit"))
        require(current.client.status(handle).state in (State.PENDING, State.RUNNING), "a held task was not running")
        current.clock[0] += 21600 + 1
        status = current.client.status(handle)
        require(status.state == State.FAILED and status.exit_code == 124, f"the deadline did not end the job: {status}")
        require(current.ecs.count("stop_task") == 1 and all(task["lastStatus"] == "STOPPED" for task in current.ecs.tasks.values()), "the task was not stopped")


def cancel_stops_the_task() -> None:
    with rig() as current:
        current.ecs.hold = True
        handle = current.client.submit(request("fit"))
        status = current.client.cancel(handle)
        require(status.state == State.CANCELLED and current.ecs.count("stop_task") == 1, f"cancel did not stop the task: {status}")
        require(current.client.cancel(handle).state == State.CANCELLED and current.ecs.count("stop_task") == 1, "a second cancel stopped again")


def control_step_starts_nothing() -> None:
    with rig() as current:
        handle = current.client.submit(request("acquire_run"))
        status = current.client.status(handle)
        require(status.state == State.SUCCEEDED and status.exit_code == 0 and not current.ecs.calls, f"a control step reached the cloud: {status}")
        require(current.client.record(handle)["outcome"] == "control", "the control record is missing")


def refusals() -> None:
    with rig(frozenset({"train"})) as current:
        for bad, why in (
            (request("fit", mode="fixture"), "a job step in fixture mode"),
            (request("promote"), "a step with no job"),
            (request("pin_bronze", dag="silver_transform_dag"), "a family that is not enabled"),
            (
                SubmissionRequest("fit", RUN, "real", 1, "training_dag", "fit", "manual__test", None, None, {**PARAMETERS, "hai_types": ["HAI_9"]}),
                "an unknown HAI type",
            ),
        ):
            raises(SubmissionRefused, partial(current.client.submit, bad))
            require(not current.ecs.calls, f"{why} reached the cloud")
        wrong = BrokerClient(f"http://127.0.0.1:{current.listener.port}", "another-token-value", timeout=5)
        raises(BrokerUnavailable, lambda: wrong.health())


def throttled_start_is_retried() -> None:
    with rig() as current:
        current.ecs.faults["run_task"] = [client_error("ThrottlingException", "RunTask")]
        try:
            current.client.submit(request("guard"))
        except BrokerUnavailable as error:
            require("answered 503" in str(error), f"a throttled start was not a 503: {error}")
        else:
            raise AssertionError("a throttled start was accepted")
        handle = current.client.submit(request("guard"))
        require(wait_for(current, handle).state == State.SUCCEEDED and len(current.ecs.tasks) == 1, "the retry after throttling did not run one task")
    with rig() as other, dag_runtime(other, "training_dag", TRAIN_PARAMS) as run_step:
        other.ecs.faults["run_task"] = [client_error("ThrottlingException", "RunTask")]
        require(
            run_step("guard") == "succeeded" and len(other.ecs.tasks) == 1 and other.ecs.count("run_task") == 2,
            "the DAG task did not ride out a throttled call",
        )


def lost_reply_and_restart() -> None:
    with rig() as current:
        current.ecs.lose_reply_once = True
        raises(BrokerUnavailable, lambda: current.client.submit(request("guard")))
        require(len(current.ecs.tasks) == 1, "the lost reply case created no task")
        current.listener.stop()
        current.broker = current.new_broker()
        current.listener = Listener(current.broker, "127.0.0.1", 0, SESSION_KEY)
        current.listener.start()
        current.client = BrokerClient(f"http://127.0.0.1:{current.listener.port}", SESSION_KEY, timeout=5)
        handle = current.client.submit(request("guard"))
        status = wait_for(current, handle)
        require(status.state == State.SUCCEEDED and len(current.ecs.tasks) == 1, "a restart after a lost reply started a second task")
        again = wait_for(current, current.client.submit(request("fit")))
        require(again.state == State.SUCCEEDED and len(current.ecs.tasks) == 1, "a step after the restart did not join the job")


def denied_start_closes_the_submission() -> None:
    with rig() as current:
        current.ecs.faults["run_task"] = [client_error("AccessDeniedException", "RunTask")]
        raises(SubmissionRefused, lambda: current.client.submit(request("guard")))
        status = wait_for(current, Handle(request("guard").submission_id, request("guard").container_name))
        require(status.state == State.FAILED_CLOSED and not current.ecs.tasks, f"a denied start did not close the submission: {status}")


def missing_code_digest_is_retryable() -> None:
    with rig() as current:
        del current.s3.objects[f"lakehouse/run_stage/{RUN}/inputs/code.sha256"]
        try:
            current.client.submit(request("guard"))
        except BrokerUnavailable as error:
            require("answered 503" in str(error) and not current.ecs.calls, f"a missing digest was not a retryable 503: {error}")
        else:
            raise AssertionError("a run without a staged digest started")


def silver_job_runs_its_family() -> None:
    params = {"run_id": SILVER_RUN, "mode": "real"}
    with rig() as current, dag_runtime(current, "silver_transform_dag", params) as run_step:
        require(run_step("acquire_run") == "succeeded" and not current.ecs.calls, "a control step reached the cloud")
        require(run_step("pin_bronze") == "succeeded", "the silver job did not pass")
        sent = next(arguments for name, arguments in current.ecs.calls if name == "run_task")
        names = [item["name"] for item in sent["overrides"]["containerOverrides"]]
        require(sent["taskDefinition"] == "example_dev_job_silver" and "publish" in names, f"the silver job ran the wrong definition: {names}")
        for step in ("silver_build", "silver_commit", "record_silver_snapshot"):
            require(run_step(step) == "succeeded", f"{step} did not pass")
        require(len(current.ecs.tasks) == 1, "silver steps started more than one task")


def silver_request(run: str = SILVER_RUN) -> SubmissionRequest:
    return request("pin_bronze", run=run, dag="silver_transform_dag")


def gold_request(input_run: str | None = SILVER_RUN, run: str = GOLD_RUN) -> SubmissionRequest:
    return request("pin_silver", run=run, dag="gold_transform_dag", input_run=input_run)


def gold_job_gets_its_input_run() -> None:
    with rig() as current:
        wait_for(current, current.client.submit(silver_request()))
        wait_for(current, current.client.submit(gold_request()))
        sent = [arguments for name, arguments in current.ecs.calls if name == "run_task"][1]
        values = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in sent["overrides"]["containerOverrides"]}
        require(sent["taskDefinition"] == "example_dev_job_gold" and values["pin"]["INPUT_RUN_ID"] == SILVER_RUN, "the gold job did not receive its silver run")


def chain_is_closed_until_the_first_publishes_are_signed() -> None:
    with rig(signed=None) as current:
        gate = request("chain_gate", run=SILVER_RUN, dag="bronze_chain_dag")
        for closed in (gate, silver_request(), gold_request(), request("pin_gold", run=FEATURES_RUN, dag="features_transform_dag", input_run=GOLD_RUN)):
            raises(SubmissionRefused, partial(current.client.submit, closed))
        require(not current.ecs.calls, "a chain job reached the cloud before the first publishes were signed")
        require(wait_for(current, current.client.submit(request("guard"))).state == State.SUCCEEDED, "training must not wait for the chain record")
        for raw in (
            "not json",
            json.dumps({"silver": SILVER_RUN}),
            json.dumps({"silver": SILVER_RUN, "gold": "latest"}),
            json.dumps({"silver": SILVER_RUN, "gold": GOLD_RUN, "x": 1}),
        ):
            current.signed = raw
            raises(SubmissionRefused, lambda: current.client.submit(gate))
        current.signed = SIGNED
        require(wait_for(current, current.client.submit(gate)).state == State.SUCCEEDED, "a signed record did not open the chain gate")
        require(wait_for(current, current.client.submit(silver_request())).state == State.SUCCEEDED, "a signed record did not open the silver job")


def acceptance_check_failure_stops_the_chain() -> None:
    with rig() as current:
        current.ecs.plan = {"validate": 1}
        status = wait_for(current, current.client.submit(silver_request()))
        require(status.state == State.FAILED and status.exit_code == 1, f"a failed acceptance check did not fail silver: {status}")
        raises(SubmissionRefused, lambda: current.client.submit(gold_request()))
        require(len(current.ecs.tasks) == 1, "gold started after silver failed its acceptance check")


def missing_acceptance_check_fails_the_job() -> None:
    with rig() as current:
        current.ecs.omit = {"verify"}
        status = wait_for(current, current.client.submit(silver_request()))
        require(status.state == State.FAILED and status.exit_code == 125, f"a task without its acceptance container passed: {status}")
        require("verify" in current.client.record(current.client.submit(silver_request()))["reason"], "the record does not name the missing check")


def each_chain_step_needs_the_one_before_to_have_passed() -> None:
    with rig() as current:
        for bad in (gold_request(None), gold_request(RUN)):
            raises(SubmissionRefused, partial(current.client.submit, bad))
        wait_for(current, current.client.submit(silver_request()))
        current.ecs.plan = {"validate": 1}
        require(wait_for(current, current.client.submit(gold_request())).state == State.FAILED, "gold did not fail its acceptance check")
        features = request("pin_gold", run=FEATURES_RUN, dag="features_transform_dag", input_run=GOLD_RUN)
        raises(SubmissionRefused, lambda: current.client.submit(features))
        require(len(current.ecs.tasks) == 2, "features started after gold failed")


def current_prerequisite_attempt_must_have_passed() -> None:
    with rig() as current:
        wait_for(current, current.client.submit(silver_request()))
        current.ecs.plan = {"validate": 1}
        again = SubmissionRequest("pin_bronze", SILVER_RUN, "real", 2, "silver_transform_dag", "pin_bronze", "manual__test", None, None, None)
        require(wait_for(current, current.client.submit(again)).state == State.FAILED, "the second silver attempt did not fail")
        raises(SubmissionRefused, partial(current.client.submit, gold_request()))
        require(len(current.ecs.tasks) == 2, "gold started after the current silver attempt failed")
        current.ecs.plan = {}
        third = SubmissionRequest("pin_bronze", SILVER_RUN, "real", 3, "silver_transform_dag", "pin_bronze", "manual__test", None, None, None)
        wait_for(current, current.client.submit(third))
        require(wait_for(current, current.client.submit(gold_request())).state == State.SUCCEEDED, "a passing current attempt did not reopen the way")


def later_gold_steps_recover_the_bound_input() -> None:
    params = {"run_id": GOLD_RUN, "mode": "real", "silver_snapshot_run": SILVER_RUN}
    with rig() as current, dag_runtime(current, "gold_transform_dag", params) as run_step:
        wait_for(current, current.client.submit(silver_request()))
        require(run_step("pin_silver") == "succeeded", "the pin step did not pass")
        require(run_step("read_silver") == "succeeded", "a later step without the silver run was refused")
        require(run_step("gold_build") == "succeeded" and len(current.ecs.tasks) == 2, "later steps did not join the gold job")
        wait_for(current, current.client.submit(silver_request(FEATURES_RUN)))
        other = gold_request(FEATURES_RUN)
        raises(SubmissionRefused, partial(current.client.submit, other))
        unbound = request("read_silver", run=SILVER_RUN, dag="gold_transform_dag")
        raises(SubmissionRefused, partial(current.client.submit, unbound))


def start_from_a_later_step_uses_the_bound_input() -> None:
    with rig() as current:
        wait_for(current, current.client.submit(silver_request()))
        current.ecs.faults["run_task"] = [client_error("ThrottlingException", "RunTask")]
        raises(BrokerUnavailable, partial(current.client.submit, gold_request()))
        later = request("read_silver", run=GOLD_RUN, dag="gold_transform_dag")
        wait_for(current, current.client.submit(later))
        sent = [arguments for name, arguments in current.ecs.calls if name == "run_task"][-1]
        values = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in sent["overrides"]["containerOverrides"]}
        require(values["pin"]["INPUT_RUN_ID"] == SILVER_RUN, f"a job started by a later step lost its silver run: {values['pin']}")


def refused_stop_is_not_recorded_as_a_timeout() -> None:
    with rig() as current:
        current.ecs.hold = True
        handle = current.client.submit(request("fit"))
        current.clock[0] += 21600 + 1
        current.ecs.faults["stop_task"] = [client_error("AccessDeniedException", "StopTask")]
        try:
            current.client.status(handle)
        except BrokerUnavailable as error:
            require("answered 502" in str(error) and "stop" in str(error), f"the stop failure is not reported: {error}")
        else:
            raise AssertionError("a refused stop was hidden")
        raises(SubmissionRefused, partial(current.client.record, handle))
        require(all(task["lastStatus"] != "STOPPED" for task in current.ecs.tasks.values()), "the task was stopped without a stop")
        status = current.client.status(handle)
        require(status.state == State.FAILED and status.exit_code == 124, f"the retried stop did not end the job: {status}")
        require(all(task["lastStatus"] == "STOPPED" for task in current.ecs.tasks.values()), "the retry did not stop the task")


def refused_stop_does_not_cancel() -> None:
    with rig() as current:
        current.ecs.hold = True
        handle = current.client.submit(request("fit"))
        current.ecs.faults["stop_task"] = [client_error("AccessDeniedException", "StopTask")]
        raises(BrokerUnavailable, partial(current.client.cancel, handle))
        require(current.client.status(handle).state in (State.PENDING, State.RUNNING), "a refused stop ended the submission")
        require(current.client.cancel(handle).state == State.CANCELLED, "the retried cancel did not end the submission")


def chain_layers_use_the_bronze_run_digest() -> None:
    with rig() as current:
        silver, gold = derived_run_id(BRONZE_RUN, "silver"), derived_run_id(silver_of(BRONZE_RUN), "gold")
        current.s3.objects[f"lakehouse/run_stage/{BRONZE_RUN}/inputs/code.sha256"] = b"cd" * 32 + b"\n"
        before = request("pin_bronze", run=silver, dag="silver_transform_dag")
        raises(BrokerUnavailable, partial(current.client.submit, before))
        require(not current.ecs.calls, "a layer run started before the chain gate named its bronze run")
        wait_for(current, current.client.submit(request("chain_gate", run=BRONZE_RUN, dag="bronze_chain_dag")))
        wait_for(current, current.client.submit(before))
        wait_for(current, current.client.submit(request("pin_silver", run=gold, dag="gold_transform_dag", input_run=silver)))
        sent = [arguments for name, arguments in current.ecs.calls if name == "run_task"]
        digests = {
            pair["value"] for call in sent for item in call["overrides"]["containerOverrides"] for pair in item["environment"] if pair["name"] == "CODE_SHA256"
        }
        require(digests == {"cd" * 32}, f"a layer used another digest than the bronze run's: {digests}")
        wait_for(current, current.client.submit(request("chain_gate", run=BRONZE_RUN, dag="bronze_chain_dag", attempt=2)))
        require((current.folder / "state" / "chains").is_dir(), "the chain record is missing")


def silver_of(bronze: str) -> str:
    return derived_run_id(bronze, "silver")


PROMOTION = {"hai_type": "HAI_1", "model_version": "3", "promotion_mode": "promote", "mlflow": "local"}


def promotion_request(template: str = "promote", run: str = PROMOTE_RUN, **changes: Any) -> SubmissionRequest:
    return SubmissionRequest(template, run, "real", 1, "model_promotion_dag", template, "manual__test", None, None, {**PROMOTION, **changes})


def approve(current: Rig, run: str = PROMOTE_RUN, **changes: Any) -> None:
    body = {
        "run_id": run,
        "dag_id": "model_promotion_dag",
        "scope": "promote",
        "approved_on": "2026-10-30",
        "hai_type": "HAI_1",
        "model_version": "3",
        **changes,
    }
    current.s3.objects[f"lakehouse/run_stage/{run}/inputs/promotion_approval.json"] = json.dumps(body).encode()


def promotion_needs_the_owners_approval() -> None:
    with rig() as current:
        raises(SubmissionRefused, partial(current.client.submit, promotion_request()))
        require(not current.ecs.calls, "a promotion started without approval")
        approve(current)
        status = wait_for(current, current.client.submit(promotion_request()))
        require(status.state == State.SUCCEEDED and len(current.ecs.tasks) == 1, f"an approved promotion did not run: {status}")
        sent = next(arguments for name, arguments in current.ecs.calls if name == "run_task")
        values = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in sent["overrides"]["containerOverrides"]}
        require(
            sent["taskDefinition"] == "example_dev_job_promote" and json.loads(values["promote"]["TRAINING_PARAMETERS"]) == {**PROMOTION, "mlflow": "aws"},
            "wrong job or parameters",
        )
        require(values["promote"]["MLFLOW_TRACKING_URI"] == "http://10.0.0.5:5100", "the promote container has no MLflow address")


def promotion_runs_through_the_dag_runtime() -> None:
    params = {"run_id": PROMOTE_RUN, "mode": "promote", "hai_type": "HAI_1", "model_version": "3", "mlflow": "local"}
    with rig() as current, dag_runtime(current, "model_promotion_dag", params) as run_step:
        approve(current)
        require(run_step("acquire_run") == "succeeded" and run_step("promote") == "succeeded", "the promotion DAG steps did not pass")
        require(len(current.ecs.tasks) == 1, "the promotion started more than one task")


def another_promotion_is_not_approved() -> None:
    wrong = (
        {"model_version": "4"},
        {"hai_type": "HAI_2"},
        {"scope": "rollback"},
        {"run_id": GOLD_RUN},
        {"dag_id": "training_dag"},
        {"approved_on": "2999-01-01"},
        {"approved_on": "yesterday"},
        {"extra": "field"},
    )
    with rig() as current:
        for changes in wrong:
            approve(current, **changes)
            raises(SubmissionRefused, partial(current.client.submit, promotion_request()))
        current.s3.objects[f"lakehouse/run_stage/{PROMOTE_RUN}/inputs/promotion_approval.json"] = b"not json"
        raises(SubmissionRefused, partial(current.client.submit, promotion_request()))
        approve(current)
        raises(SubmissionRefused, partial(current.client.submit, promotion_request(model_version="4")))
        raises(SubmissionRefused, partial(current.client.submit, promotion_request(promotion_mode="rollback")))
        require(not current.ecs.calls, "a promotion with another approval reached the cloud")
        approve(current, scope="rollback")
        rollback = promotion_request(promotion_mode="rollback")
        require(wait_for(current, current.client.submit(rollback)).state == State.SUCCEEDED, "an approved rollback did not run")


def training_needs_no_promotion_approval() -> None:
    with rig() as current:
        require(wait_for(current, current.client.submit(request("guard"))).state == State.SUCCEEDED, "training waited for a promotion approval")


def logs_hide_the_token() -> None:
    with rig() as current:
        handle = current.client.submit(request("guard"))
        arn = next(iter(current.ecs.tasks))
        current.logs.streams[f"train/train/{arn.rsplit('/', 1)[-1]}"] = ["step done", f"leaked {SESSION_KEY}"]
        text = current.client.logs(handle, 5)
        require("step done" in text and SESSION_KEY not in text and "[redacted]" in text, f"the log tail is wrong: {text!r}")


CASES: dict[str, Callable[[], None]] = {
    "task_runs_through_the_broker": task_runs_through_the_broker,
    "repeated_submit_one_task": repeated_submit_one_task,
    "finished_job_is_not_started_again": finished_job_is_not_started_again,
    "container_failure_fails_the_job": container_failure_fails_the_job,
    "deadline_stops_the_task": deadline_stops_the_task,
    "cancel_stops_the_task": cancel_stops_the_task,
    "control_step_starts_nothing": control_step_starts_nothing,
    "refusals": refusals,
    "throttled_start_is_retried": throttled_start_is_retried,
    "lost_reply_and_restart": lost_reply_and_restart,
    "denied_start_closes_the_submission": denied_start_closes_the_submission,
    "missing_code_digest_is_retryable": missing_code_digest_is_retryable,
    "silver_job_runs_its_family": silver_job_runs_its_family,
    "gold_job_gets_its_input_run": gold_job_gets_its_input_run,
    "chain_is_closed_until_the_first_publishes_are_signed": chain_is_closed_until_the_first_publishes_are_signed,
    "acceptance_check_failure_stops_the_chain": acceptance_check_failure_stops_the_chain,
    "missing_acceptance_check_fails_the_job": missing_acceptance_check_fails_the_job,
    "each_chain_step_needs_the_one_before_to_have_passed": each_chain_step_needs_the_one_before_to_have_passed,
    "current_prerequisite_attempt_must_have_passed": current_prerequisite_attempt_must_have_passed,
    "later_gold_steps_recover_the_bound_input": later_gold_steps_recover_the_bound_input,
    "start_from_a_later_step_uses_the_bound_input": start_from_a_later_step_uses_the_bound_input,
    "refused_stop_is_not_recorded_as_a_timeout": refused_stop_is_not_recorded_as_a_timeout,
    "refused_stop_does_not_cancel": refused_stop_does_not_cancel,
    "chain_layers_use_the_bronze_run_digest": chain_layers_use_the_bronze_run_digest,
    "promotion_needs_the_owners_approval": promotion_needs_the_owners_approval,
    "promotion_runs_through_the_dag_runtime": promotion_runs_through_the_dag_runtime,
    "another_promotion_is_not_approved": another_promotion_is_not_approved,
    "training_needs_no_promotion_approval": training_needs_no_promotion_approval,
    "logs_hide_the_token": logs_hide_the_token,
}
