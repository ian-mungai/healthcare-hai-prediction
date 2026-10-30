"""End-to-end cases for the lean ECS broker: a DAG task reaches the real HTTP listener, which reaches the fake ECS client.

The airflow SDK is replaced by a small stand-in (the package is not installed here), so ``orchestration.dag_runtime.run_step`` runs unchanged against
the real ``BrokerClient``. Cluster time is the fake's poll count and the broker's clock is a list the cases move by hand. Nothing leaves the machine.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import re
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
from scripts.orchestration.ecs_broker import (
    BrokerSettings,
    EcsBroker,
    Listener,
    bronze_request_reader,
    derived_run_id,
    load_jobs,
    s3_digest_reader,
    s3_object_reader,
)
from scripts.orchestration.ecs_broker_fake import FakeEcs, FakeLogs, FakeS3, client_error

ROOT = Path(__file__).resolve().parents[2]
SESSION_KEY = "stand-in-session-token-0123456789"
RUN = "20261030T100000Z-abcdef01"
SILVER_RUN = "20261030T100000Z-0badcafe"
GOLD_RUN = "20261030T100000Z-90d0c0de"
FEATURES_RUN = "20261030T100000Z-feed5eed"
BRONZE_RUN = "20261030T090000Z-b0b0b0b0"
PROMOTE_RUN = "20261030T110000Z-9a9a9a9a"
DRIFT_RUN = "20261030T120000Z-d71f70a1"
SIGNED = json.dumps({"silver": SILVER_RUN, "gold": GOLD_RUN})
DIGEST = "ab" * 32
PARAMETERS = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "local"}
JOB_FAMILIES = frozenset({"train", "silver", "gold", "features", "drift", "promote"})
BRONZE_RUN = "20261030T090000Z-b0b0b0b0"
BRONZE_FAMILIES = JOB_FAMILIES | {"bronze"}


def collections_of_table_map() -> dict[str, str]:
    """The table map's collection of each table, from the committed file."""
    return {entry["table"]: entry["collection"] for entry in json.loads((ROOT / "config/lakehouse/bronze_tables.json").read_text())["tables"]}


def table_pair() -> tuple[list[str], str]:
    """Two tables of one collection and one table of another, in table map order."""
    by_collection: dict[str, list[str]] = {}
    for table, collection in collections_of_table_map().items():
        by_collection.setdefault(collection, []).append(table)
    pair = next(tables[:2] for tables in by_collection.values() if len(tables) >= 2)
    other = next(tables[0] for tables in by_collection.values() if tables[0] not in pair)
    return pair, other


class Rig:
    """One broker with its listener, a client and the fakes behind it."""

    def __init__(self, folder: Path, families: frozenset[str] = JOB_FAMILIES, signed: str | None = SIGNED) -> None:
        self.folder = folder
        self.signed = signed
        self.ecs, self.logs, self.s3 = FakeEcs(), FakeLogs(), FakeS3()
        self.s3.objects[f"lakehouse/run_stage/{RUN}/inputs/code.sha256"] = DIGEST.encode() + b"\n"
        self.s3.objects[f"lakehouse/run_stage/{BRONZE_RUN}/inputs/code.sha256"] = DIGEST.encode() + b"\n"
        for run in (SILVER_RUN, GOLD_RUN, FEATURES_RUN, PROMOTE_RUN, DRIFT_RUN):
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
            bronze_request=bronze_request_reader(self.s3, "example-bucket", collections_of_table_map()),
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
        require(values["fetch"] == {"RUN_ID": RUN, "CODE_RUN_ID": RUN, "CODE_SHA256": DIGEST}, f"fetch variables are wrong: {values['fetch']}")
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


def cancel_of_an_unresolved_start_stops_the_task() -> None:
    """ECS accepted RunTask but the reply was lost: the submission holds only an intent. Cancel must find the task by its deterministic identity and stop it."""
    with rig() as current:
        current.ecs.hold = True
        current.ecs.lose_reply_once = True
        asked = request("fit")
        raises(BrokerUnavailable, partial(current.client.submit, asked))
        handle = Handle(asked.submission_id, asked.container_name)
        status = current.client.cancel(handle)
        task = next(iter(current.ecs.tasks.values()))
        require(task["lastStatus"] == "STOPPED", f"the cancel left the task {task['lastStatus']}")
        require(status.state == State.CANCELLED and current.ecs.count("stop_task") == 1, f"cancel did not end the submission after the stop: {status}")


def retry_beside_an_unresolved_start_is_refused() -> None:
    with rig() as current:
        current.ecs.hold = True
        current.ecs.lose_reply_once = True
        raises(BrokerUnavailable, partial(current.client.submit, request("fit")))
        raises(SubmissionRefused, partial(current.client.submit, request("fit", attempt=2)))
        require(len(current.ecs.tasks) == 1, "a later attempt started a second task beside the one that had no reply")


def failed_discovery_keeps_an_unresolved_start_open() -> None:
    with rig() as current:
        current.ecs.hold = True
        current.ecs.lose_reply_once = True
        asked = request("fit")
        raises(BrokerUnavailable, partial(current.client.submit, asked))
        handle = Handle(asked.submission_id, asked.container_name)
        current.ecs.faults["list_tasks"] = [client_error("ThrottlingException", "ListTasks")]
        raises(BrokerUnavailable, partial(current.client.cancel, handle))
        require(current.client.status(handle).state in (State.PENDING, State.RUNNING), "a cancel whose discovery failed ended the submission")
        require(current.client.cancel(handle).state == State.CANCELLED, "the repeated cancel did not end the submission")
        require(next(iter(current.ecs.tasks.values()))["lastStatus"] == "STOPPED", "the task was not stopped")


def code_digest_is_read_once_per_start() -> None:
    """The fetch and pin containers both receive the code digest. The broker reads the staged object once, so the value it checked is the value both get."""
    with rig() as current:
        handle = current.client.submit(silver_request())
        require(current.client.status(handle).state in (State.RUNNING, State.SUCCEEDED), "the silver job did not start")
        reads = [key for key in current.s3.reads if key.endswith("/inputs/code.sha256")]
        require(len(reads) == 1, f"the code digest object was read {len(reads)} times for one start")
        sent = next(arguments for name, arguments in current.ecs.calls if name == "run_task")
        digests = {item["value"] for override in sent["overrides"]["containerOverrides"] for item in override["environment"] if item["name"] == "CODE_SHA256"}
        require(len(digests) == 1, f"the containers received different code digests: {digests}")


def cancel_waits_for_the_task_to_stop() -> None:
    """StopTask only requests the stop. The submission may end as cancelled once the task is STOPPED, not before."""
    with rig() as current:
        current.ecs.hold = True
        handle = current.client.submit(request("fit"))
        current.ecs.stop_lag = 3
        status = current.client.cancel(handle)
        task = next(iter(current.ecs.tasks.values()))
        require(status.state == State.CANCELLED and task["lastStatus"] == "STOPPED", f"cancelled while the task was {task['lastStatus']}")


def stop_that_never_completes_blocks_the_retry() -> None:
    """A stop that the task never finishes fails visibly, ends nothing, and no later attempt starts beside the old task."""
    with rig() as current, mock.patch.object(EcsBroker, "STOP_WAIT_SECONDS", 0.3), mock.patch.object(EcsBroker, "STOP_POLL_SECONDS", 0.01):
        current.ecs.hold = True
        handle = current.client.submit(request("fit"))
        current.ecs.stop_lag = 10**9
        raises(BrokerUnavailable, partial(current.client.cancel, handle))
        require(current.client.status(handle).state in (State.PENDING, State.RUNNING), "an unconfirmed stop ended the submission")
        tasks = len(current.ecs.tasks)
        raises(SubmissionRefused, partial(current.client.submit, request("fit", attempt=2)))
        require(len(current.ecs.tasks) == tasks, "a retry started a second task beside the one that was still stopping")


def deadline_stop_waits_for_the_task_to_stop() -> None:
    with rig() as current:
        current.ecs.hold = True
        handle = current.client.submit(request("fit"))
        current.ecs.stop_lag = 3
        current.clock[0] += 10**7
        status = current.client.status(handle)
        task = next(iter(current.ecs.tasks.values()))
        require(status.state != State.RUNNING and task["lastStatus"] == "STOPPED", f"timeout recorded while the task was {task['lastStatus']}")


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
        code_runs = {
            (item["name"], pair["value"])
            for call in sent
            for item in call["overrides"]["containerOverrides"]
            for pair in item["environment"]
            if pair["name"] == "CODE_RUN_ID"
        }
        require(
            bool(code_runs) and {value for _, value in code_runs} == {BRONZE_RUN}, f"a chain layer was not told to fetch the bronze run's code: {code_runs}"
        )
        require({name for name, _ in code_runs} >= {"fetch", "pin"}, f"fetch and pin must both name the code run: {code_runs}")
        outputs = {
            pair["value"] for call in sent for item in call["overrides"]["containerOverrides"] for pair in item["environment"] if pair["name"] == "RUN_ID"
        }
        require(outputs == {silver, gold}, f"the job output run IDs changed: {outputs}")
        wait_for(current, current.client.submit(request("chain_gate", run=BRONZE_RUN, dag="bronze_chain_dag", attempt=2)))
        require((current.folder / "state" / "chains").is_dir(), "the chain record is missing")


def each_family_runs_in_its_approved_network() -> None:
    """The layer jobs reach the catalog and Redis through the lakehouse group, the training family through the group that also reaches MLflow. Each RunTask
    carries only the group of its own family's network class, and the table names a class for every family."""
    classes = {
        "train": "training",
        "silver": "lakehouse",
        "gold": "lakehouse",
        "features": "lakehouse",
        "drift": "training",
        "promote": "training",
        "metadata": "lakehouse",
        "bronze": "lakehouse",
    }
    table = load_jobs(ROOT)["jobs"]
    require(
        {name: job.get("network") for name, job in table.items()} == classes,
        f"the broker table binds the networks {[(n, j.get('network')) for n, j in table.items()]}",
    )
    groups = {"training": "sg-0train", "lakehouse": "sg-0lake"}
    for template, family, run, dag in (
        ("guard", "train", RUN, "training_dag"),
        ("pin_bronze", "silver", SILVER_RUN, "silver_transform_dag"),
        ("drift_run", "drift", DRIFT_RUN, "drift_monitoring_dag"),
    ):
        with rig() as current:
            current.client.submit(request(template, run=run, dag=dag))
            sent = [arguments for name, arguments in current.ecs.calls if name == "run_task"]
            require(bool(sent), f"the {family} family started no task")
            found = sent[0]["networkConfiguration"]["awsvpcConfiguration"]["securityGroups"]
            require(found == [groups[classes[family]]], f"the {family} family ran in {found}, not in {groups[classes[family]]}")


def broker_settings_name_the_networks_of_the_enabled_families() -> None:
    from scripts.orchestration import ecs_broker

    jobs = load_jobs(ROOT)
    both = ecs_broker.network_groups('{"lakehouse": "sg-0lake", "training": "sg-0train"}')
    require(both == {"lakehouse": ("sg-0lake",), "training": ("sg-0train",)}, f"{both}")
    ecs_broker.check_networks(dataclasses.replace(SETTINGS, networks=both, families=frozenset({"train", "silver"})), jobs)
    only_lakehouse = ecs_broker.network_groups('{"lakehouse": "sg-0lake"}')
    raises(
        ecs_broker.BrokerError, lambda: ecs_broker.check_networks(dataclasses.replace(SETTINGS, networks=only_lakehouse, families=frozenset({"train"})), jobs)
    )
    for bad in ("not json", '{"offline": "sg-0a"}', '{"lakehouse": "not-a-group"}', "{}"):
        raises(ecs_broker.BrokerError, partial(ecs_broker.network_groups, bad))


def network_classes_match_the_services_stack_and_the_iam_outputs() -> None:
    """The family to class map in the services variable equals the job table, and each class is an output of the IAM stack that the owner passes in."""
    table = {name: job["network"] for name, job in load_jobs(ROOT)["jobs"].items()}
    variables = (ROOT / "infra/services/variables.tf").read_text()
    literal = re.search(r"lookup\(\{([^}]*)\}, family", variables)
    require(literal is not None, "the services variable has no family to class map")
    in_variable = dict(re.findall(r'(\w+) = "(\w+)"', literal.group(1))) if literal else {}
    require(in_variable == table, f"the services variable maps {in_variable}, the job table {table}")
    job_outputs = (ROOT / "infra/iam/job_outputs.tf").read_text()
    classes = (ROOT / "infra/iam/job_network.tf").read_text()
    require(
        "for key in local.job_classes" in job_outputs and '"lakehouse"' in classes,
        "the IAM job_network output lacks the lakehouse class",
    )
    require('output "training_network"' in (ROOT / "infra/iam/training.tf").read_text(), "the IAM stack has no training_network output")


def drift_triggered_training_fetches_the_drift_runs_code() -> None:
    """The drift DAG derives the training run ID from the drift run ID. That run has no staged code of its own: it must fetch the code of the drift run."""
    from orchestration.training_contract import training_run_id

    training = training_run_id(DRIFT_RUN)
    with rig() as current, dag_runtime(current, "drift_monitoring_dag", {"run_id": DRIFT_RUN, "mode": "real", "mlflow": "local"}) as drift_step:
        drift_step("drift_run")
        with dag_runtime(current, "training_dag", {**TRAIN_PARAMS, "run_id": training}) as run_step:
            require(run_step("guard") == "succeeded", "the drift-triggered training step did not pass")
        sent = [arguments for name, arguments in current.ecs.calls if name == "run_task"][-1]
        values = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in sent["overrides"]["containerOverrides"]}
        require(values["fetch"]["CODE_RUN_ID"] == DRIFT_RUN and values["fetch"]["RUN_ID"] == training, f"the training fetch names {values['fetch']}")


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


def drift_dag_step_reaches_the_drift_job() -> None:
    """The step the drift DAG sends is ``drift_run``, the template of the broker table. The real selector must map it to the drift job."""
    params = {"run_id": DRIFT_RUN, "mode": "real", "mlflow": "local"}
    with rig() as current, dag_runtime(current, "drift_monitoring_dag", params) as run_step:
        require(run_step("drift_run") == "succeeded", "the drift DAG step did not pass")
        sent = next(arguments for name, arguments in current.ecs.calls if name == "run_task")
        names = [item["name"] for item in sent["overrides"]["containerOverrides"]]
        require(
            sent["taskDefinition"] == "example_dev_job_drift" and names == ["fetch", "baseline", "current", "check", "publish"],
            f"the drift step overrides containers that the task does not have: {names}",
        )
        require(len(current.ecs.tasks) == 1, "the drift step started more than one task")


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


def stage_bronze_request(current: Rig, document: object) -> None:
    current.s3.objects[f"lakehouse/run_stage/{BRONZE_RUN}/inputs/bronze_request.json"] = (
        document if isinstance(document, bytes) else json.dumps(document).encode()
    )


def bronze_request(attempt: int = 1) -> SubmissionRequest:
    return SubmissionRequest("bronze_load", BRONZE_RUN, "real", attempt, "ingestion_dag", "bronze_load", "manual__test", None, None, None)


def bronze_variables(current: Rig) -> dict[str, dict[str, str]]:
    sent = next(arguments for name, arguments in current.ecs.calls if name == "run_task")
    return {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in sent["overrides"]["containerOverrides"]}


def bronze_job_gets_its_checked_dispatch_values() -> None:
    pair, _ = table_pair()
    with rig(BRONZE_FAMILIES, signed=None) as current:
        stage_bronze_request(current, {"tables": pair, "target": "real", "dry_run": False})
        require(
            wait_for(current, current.client.submit(bronze_request())).state == State.SUCCEEDED,
            "the bronze job did not pass before the first publishes were signed",
        )
        sent = next(arguments for name, arguments in current.ecs.calls if name == "run_task")
        values = bronze_variables(current)
        require(
            sent["taskDefinition"] == "example_dev_job_bronze" and list(values) == ["fetch", "pin", "load", "check"],
            f"wrong definition or containers: {list(values)}",
        )
        require(values["fetch"] == {"RUN_ID": BRONZE_RUN, "CODE_SHA256": DIGEST}, values["fetch"])
        dispatch = {"BRONZE_TABLES": ",".join(pair), "BRONZE_TARGET": "real", "BRONZE_DRY_RUN": ""}
        for container in ("pin", "load", "check"):
            require(all(values[container][name] == value for name, value in dispatch.items()), (container, values[container]))
        require(values["load"]["POLARIS_URI"] == "http://10.0.0.5:8181/api/catalog" and "POLARIS_URI" not in values["check"], values["load"])
        stage_bronze_request(current, {"tables": pair[:1], "target": "qualify", "dry_run": True})
        wait_for(current, current.client.submit(bronze_request(2)))
        second = [arguments for name, arguments in current.ecs.calls if name == "run_task"][1]
        later = {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in second["overrides"]["containerOverrides"]}
        require(later["load"]["BRONZE_TARGET"] == "qualify" and later["load"]["BRONZE_DRY_RUN"] == "1", later["load"])


def bronze_job_is_off_unless_enabled() -> None:
    pair, _ = table_pair()
    with rig(JOB_FAMILIES) as current:
        stage_bronze_request(current, {"tables": pair, "target": "real", "dry_run": False})
        raises(SubmissionRefused, lambda: current.client.submit(bronze_request()))
        require(not current.ecs.calls, "a disabled bronze job reached the cloud")


def bronze_refuses_bad_dispatch_values() -> None:
    pair, other = table_pair()
    bad = {
        "unknown table": {"tables": ["not_a_table"], "target": "real", "dry_run": False},
        "no tables": {"tables": [], "target": "real", "dry_run": False},
        "repeated table": {"tables": [pair[0], pair[0]], "target": "real", "dry_run": False},
        "other target": {"tables": pair, "target": "production", "dry_run": False},
        "two collections in a qualification": {"tables": [pair[0], other], "target": "qualify", "dry_run": False},
        "dry run not a flag": {"tables": pair, "target": "real", "dry_run": "yes"},
        "extra key": {"tables": pair, "target": "real", "dry_run": False, "namespace": "bronze"},
        "missing key": {"tables": pair, "target": "real"},
        "tables not a list": {"tables": ",".join(pair), "target": "real", "dry_run": False},
        "not an object": [pair],
    }
    with rig(BRONZE_FAMILIES) as current:
        for label, document in bad.items():
            stage_bronze_request(current, document)
            raises(SubmissionRefused, lambda: current.client.submit(bronze_request()))
            require(not current.ecs.calls, f"{label} reached the cloud")
        stage_bronze_request(current, b"not json")
        raises(SubmissionRefused, lambda: current.client.submit(bronze_request()))
        stage_bronze_request(current, b"x" * 5000)
        raises(SubmissionRefused, lambda: current.client.submit(bronze_request()))
        require(not current.ecs.calls, "a malformed request reached the cloud")


def bronze_without_a_staged_request_is_retryable() -> None:
    with rig(BRONZE_FAMILIES) as current:
        try:
            current.client.submit(bronze_request())
        except BrokerUnavailable as error:
            require("answered 503" in str(error) and not current.ecs.calls, f"a missing request was not a retryable 503: {error}")
        else:
            raise AssertionError("a bronze run without a staged request started")


def bronze_failure_fails_the_job() -> None:
    pair, _ = table_pair()
    with rig(BRONZE_FAMILIES) as current:
        stage_bronze_request(current, {"tables": pair, "target": "real", "dry_run": False})
        current.ecs.plan = {"load": 1}
        status = wait_for(current, current.client.submit(bronze_request()))
        require(status.state == State.FAILED and status.exit_code == 1, f"a failed load did not fail the job: {status}")
        current.ecs.plan = {}
        current.ecs.omit = {"check"}
        status = wait_for(current, current.client.submit(bronze_request(2)))
        require(status.state == State.FAILED and status.exit_code == 125, f"a task without its check container passed: {status}")


def bronze_retry_starts_no_second_task() -> None:
    pair, _ = table_pair()
    with rig(BRONZE_FAMILIES) as current:
        stage_bronze_request(current, {"tables": pair, "target": "real", "dry_run": False})
        first, second = current.client.submit(bronze_request()), current.client.submit(bronze_request())
        require(first == second and current.ecs.count("run_task") == 1, "a retried bronze submit started a second task")
        wait_for(current, first)
        current.ecs.tokens.clear()
        again = wait_for(current, current.client.submit(bronze_request()))
        require(again.state == State.SUCCEEDED and current.ecs.count("run_task") == 1, "a finished bronze run started again")


def chain_stays_closed_while_bronze_runs() -> None:
    pair, _ = table_pair()
    with rig(BRONZE_FAMILIES, signed=None) as current:
        stage_bronze_request(current, {"tables": pair, "target": "real", "dry_run": False})
        wait_for(current, current.client.submit(bronze_request()))
        raises(SubmissionRefused, partial(current.client.submit, silver_request()))
        require(current.ecs.count("run_task") == 1, "silver started on a bronze run without the signed record")


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
    "cancel_of_an_unresolved_start_stops_the_task": cancel_of_an_unresolved_start_stops_the_task,
    "retry_beside_an_unresolved_start_is_refused": retry_beside_an_unresolved_start_is_refused,
    "failed_discovery_keeps_an_unresolved_start_open": failed_discovery_keeps_an_unresolved_start_open,
    "each_family_runs_in_its_approved_network": each_family_runs_in_its_approved_network,
    "broker_settings_name_the_networks_of_the_enabled_families": broker_settings_name_the_networks_of_the_enabled_families,
    "network_classes_match_the_services_stack_and_the_iam_outputs": network_classes_match_the_services_stack_and_the_iam_outputs,
    "drift_triggered_training_fetches_the_drift_runs_code": drift_triggered_training_fetches_the_drift_runs_code,
    "code_digest_is_read_once_per_start": code_digest_is_read_once_per_start,
    "cancel_waits_for_the_task_to_stop": cancel_waits_for_the_task_to_stop,
    "stop_that_never_completes_blocks_the_retry": stop_that_never_completes_blocks_the_retry,
    "deadline_stop_waits_for_the_task_to_stop": deadline_stop_waits_for_the_task_to_stop,
    "promotion_needs_the_owners_approval": promotion_needs_the_owners_approval,
    "drift_dag_step_reaches_the_drift_job": drift_dag_step_reaches_the_drift_job,
    "promotion_runs_through_the_dag_runtime": promotion_runs_through_the_dag_runtime,
    "another_promotion_is_not_approved": another_promotion_is_not_approved,
    "training_needs_no_promotion_approval": training_needs_no_promotion_approval,
    "logs_hide_the_token": logs_hide_the_token,
    "bronze_job_gets_its_checked_dispatch_values": bronze_job_gets_its_checked_dispatch_values,
    "bronze_job_is_off_unless_enabled": bronze_job_is_off_unless_enabled,
    "bronze_refuses_bad_dispatch_values": bronze_refuses_bad_dispatch_values,
    "bronze_without_a_staged_request_is_retryable": bronze_without_a_staged_request_is_retryable,
    "bronze_failure_fails_the_job": bronze_failure_fails_the_job,
    "bronze_retry_starts_no_second_task": bronze_retry_starts_no_second_task,
    "chain_stays_closed_while_bronze_runs": chain_stays_closed_while_bronze_runs,
}
