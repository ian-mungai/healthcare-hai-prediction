"""Run every DAG that launches a host job in the pinned Airflow image, with Airflow's own test runner and the real Amazon provider operator.

Every other launch-path test runs over stand-ins of the Airflow SDK and of the Amazon provider (``airflow_mode_cases.py``, ``ecs_launch_operator_cases.py``),
so a class of fault cannot show there: how Airflow's own classes treat ours (the operator metaclass, task parameters, the task SDK's context and state
calls, the DAG form). This module runs in the image with no network. It starts a local SQLite metadata database and runs each DAG with ``DAG.test()``,
which runs every task through the task SDK runner in process (``run_task_in_process``) against Airflow's execution API. A task runs the production code:
``orchestration/dag_runtime.py``, ``orchestration/airflow_steps.py`` with the real ``services()`` (real boto3 clients, session, configuration and
environment), the launch library and ``BrokerlessEcsRunTaskOperator`` on the real ``EcsRunTaskOperator``.

Only the cloud is replaced. ``aws_http_bridge.AwsBridge`` serves the existing fakes (``Rig`` of ``airflow_mode_cases``: the fake ECS with the host task
definition revisions, the Logs, SSM and S3 objects of the staged inputs and the launch records) on loopback, and ``AWS_ENDPOINT_URL`` points every boto3
client at it, with fake credentials. Three more substitutions, each named in the report: the free space of the scratch volume (the host volume is not in the
container), a staged input that is not there yet is waited for one second instead of ten minutes, and the fake task ends at its first status read.

``main`` prints one line ``REAL_IMAGE_DAGS <json>`` with one entry per scenario. A scenario fails with the first expectation it breaks, and the others still
run, so one run lists every fault. ``scripts/orchestration/run_real_image_dags_e2e.py`` records the report.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
REPORT_PREFIX = "REAL_IMAGE_DAGS "
SUBSTITUTIONS = (
    "the cloud: the fake ECS, Logs, SSM and S3 of the airflow_mode_cases rig behind a loopback bridge (aws_http_bridge.py), "
    "reached with AWS_ENDPOINT_URL and fake credentials",
    "the free space of the scratch volume: shutil.disk_usage answers 400 GiB, because the host's scratch volume is not in the container",
    "airflow_steps.UNAVAILABLE_SECONDS is 1 second: a staged input that is not there yet is waited for one second, not ten minutes",
    "the fake task ends at its first status read (finish_after 1) and its exit codes are planned by the scenario",
    "USER is set when the environment has none: the check runs python without the image entrypoint that adds the passwd entry for the user id",
    "the Airflow metadata database is SQLite in a temporary folder and DAG.test() runs the tasks in process (no scheduler, no API server)",
)
FAKE_CREDENTIAL = "stand-in"
SCRATCH_FREE = 400 * 2**30
DONE_STATE = "success"
BAG: dict[str, Any] = {}


@dataclass(frozen=True)
class Step:
    """One DAG run of a scenario: the DAG, its trigger values and what the run must do.

    ``states`` maps task ids to the states they must end in (``None`` means every task of the DAG ends in ``success``), ``starts`` is the number of RunTask
    calls the run sends, and ``family`` the host task definition family they name. ``reason`` is text each failing task's error must contain, ``skip`` the
    tasks that need the Airflow API server or other DAGs (they are marked as passed and not run), and ``plan`` the exit code per container of the task."""

    dag_id: str
    params: Mapping[str, Any]
    starts: int
    family: str | None = None
    states: Mapping[str, str] | None = None
    run_state: str = "success"
    reason: str | None = None
    skip: str | None = None
    plan: Mapping[str, int | None] | None = None
    end: str | None = None
    environment: Mapping[str, str] = field(default_factory=dict)
    prepare: Callable[[Any], None] | None = None


@dataclass(frozen=True)
class Scenario:
    """Steps that share one rig, so a later DAG finds the records of an earlier one. ``rig`` builds the rig of the scenario."""

    name: str
    steps: tuple[Step, ...]
    rig: Callable[[], Any]
    after: Callable[[Any], None] | None = None


class Expectation(AssertionError):
    """A scenario broke an expectation."""


def require(condition: object, message: str) -> None:
    if not condition:
        raise Expectation(message)


def configure_airflow(folder: str) -> None:
    """The Airflow settings of the run, set before Airflow is imported."""
    os.environ.update(
        {
            "AIRFLOW_HOME": folder,
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{folder}/airflow.db",
            "AIRFLOW__CORE__DAGS_FOLDER": str(ROOT / "dags"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
            "AIRFLOW__CORE__UNIT_TEST_MODE": "false",
            "AIRFLOW__OPENLINEAGE__DISABLED": "true",
        }
    )
    os.environ.setdefault(
        "USER", "airflow"
    )  # the check runs python without the image entrypoint, which adds the passwd entry that Airflow needs for any other user id


class Observer:
    """Records the error each launch step raises (the task SDK keeps only the task's state) and lets it go on."""

    NAMES = ("launch_step", "preflight_step", "job_step", "evidence_step", "settle_launch")

    def __init__(self) -> None:
        self.errors: list[str] = []

    @contextlib.contextmanager
    def watching(self) -> Iterator[None]:
        import importlib

        steps = importlib.import_module("orchestration.airflow_steps")
        patches = [mock.patch.object(steps, name, self.watch(name, getattr(steps, name))) for name in self.NAMES]
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            yield

    def watch(self, name: str, function: Callable[..., Any]) -> Callable[..., Any]:
        def watched(*arguments: Any, **keywords: Any) -> Any:
            try:
                return function(*arguments, **keywords)
            except Exception as error:
                self.errors.append(f"{name}: {type(error).__name__}: {error}")
                raise

        return watched


def rig_modules() -> SimpleNamespace:
    """The rig of the offline cases: its fakes, constants and staged inputs. ``orchestration`` may already be imported from a folder that holds only a few of
    its files (the in-image check mounts those), so the full package folder of this checkout is added to its path."""
    sys.path.insert(0, str(ROOT))
    import orchestration

    if str(ROOT / "orchestration") not in orchestration.__path__:
        orchestration.__path__.append(str(ROOT / "orchestration"))
    import scripts.orchestration.airflow_mode_cases as cases
    from scripts.orchestration import ecs_broker_cases, ecs_broker_fake

    return SimpleNamespace(cases=cases, broker=ecs_broker_cases, fake=ecs_broker_fake)


def make_rig(*, capacity: str | None = None, hold: str | Exception | None = None) -> Any:
    """A rig with the fake ECS finishing its task at the first status read, the real clock for task times, and the SSM answers of a services stack that
    is not applying (the hold clear), unless ``hold`` says otherwise."""
    cases = rig_modules().cases
    rig = cases.Rig(capacity=cases.CAPACITY if capacity is None else capacity)
    rig.ecs.clock = time.time
    rig.ecs.finish_after = 1
    answer: list[str | Exception] = [cases.CLEAR if hold is None else hold]
    rig.ssm = cases.LaunchSsm({cases.HOLD_PARAMETER: answer, cases.CAP_PARAMETER: [str(cases.JOB_MEMORY)], cases.LOCK_PARAMETER: cases.no_lock()})
    return rig


def launch_environment(rig: Any, endpoint: str, scratch: str) -> dict[str, str]:
    """The environment of the Airflow service, as the services stack renders it, with the endpoint of the bridge and fake credentials."""
    cases, fake = rig_modules().cases, rig_modules().fake
    return {
        "AWS_REGION": fake.REGION,
        "AWS_DEFAULT_REGION": fake.REGION,
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": FAKE_CREDENTIAL,
        "AWS_SECRET_ACCESS_KEY": FAKE_CREDENTIAL,
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_EC2_METADATA_DISABLED": "true",
        "HAI_LAUNCH_CLUSTER": fake.CLUSTER,
        "HAI_LAUNCH_FAMILY_PREFIX": "example_dev_job_",
        "HAI_LAUNCH_PROJECT_TAG": "example",
        "HAI_LAUNCH_LOG_GROUP": "example_jobs_dev",
        "HAI_LAUNCH_HOST_PROVIDER": "stand-in_host",
        "HAI_LAUNCH_BUCKET": cases.BUCKET,
        "HAI_LAUNCH_POLARIS_URI": cases.POLARIS,
        "HAI_LAUNCH_MLFLOW_URI": cases.MLFLOW,
        "HAI_LAUNCH_SCRATCH_PATH": scratch,
        "HAI_LAUNCH_HOST_CAPACITY": rig.preflight.host_capacity,
        "HAI_LAUNCH_HOST_CAP_PARAMETER": cases.CAP_PARAMETER,
        "HAI_LAUNCH_HOST_HOLD_PARAMETER": cases.HOLD_PARAMETER,
        "HAI_LAUNCH_APPLY_LOCK_PARAMETER": cases.LOCK_PARAMETER,
    }


def launch_records(rig: Any, run: str, job: str) -> dict[str, dict[str, Any]]:
    """The launch records of a run and job in the fake record bucket, by event."""
    prefix = rig.launcher.store.prefix if hasattr(rig.launcher.store, "prefix") else rig_modules().cases.PREFIX
    return {
        key.rsplit("/", 1)[-1].removesuffix(".json"): json.loads(body)
        for key, body in rig.records.objects.items()
        if key.startswith(f"{prefix}/") and f"/{run}/" in key and f"/{job}/" in key
    }


def states_of(run: Any) -> dict[str, str]:
    """The state each task instance of the DAG run ended in."""
    return {item.task_id: str(getattr(item.state, "value", item.state)) for item in run.get_task_instances()}


def run_step(bag: Any, rig: Any, step: Step, scratch: str) -> dict[str, Any]:
    """Run one DAG of a scenario in the image and check it against its step. Returns the facts of the run."""
    from orchestration import airflow_steps
    from scripts.orchestration.aws_http_bridge import Bridge

    cases = rig_modules().cases
    if step.prepare is not None:
        step.prepare(rig)
    rig.ecs.plan = dict(step.plan or {})
    started = len([name for name, _ in rig.ecs.calls if name == "run_task"])
    sent = len(rig.ecs.sent)
    observer = Observer()
    dag = bag.get_dag(step.dag_id)
    require(dag is not None, f"{step.dag_id} is not in the DAG folder")
    with Bridge({"ecs": rig.ecs, "ssm": rig.ssm, "logs": rig.logs}, rig.stage, rig.records, f"{cases.PREFIX}/") as bridge:
        environment = launch_environment(rig, bridge.endpoint, scratch) | dict(step.environment)
        with (
            mock.patch.dict(os.environ, environment),
            mock.patch.object(airflow_steps, "UNAVAILABLE_SECONDS", 1.0),
            mock.patch.object(shutil, "disk_usage", lambda _path: SimpleNamespace(total=SCRATCH_FREE, used=0, free=SCRATCH_FREE)),
            observer.watching(),
        ):
            run = dag.test(run_conf=dict(step.params), mark_success_pattern=step.skip)
        states = states_of(run)
        run_state = str(getattr(run.state, "value", run.state))
        requests = list(bridge.requests)
    calls = [arguments for name, arguments in rig.ecs.calls if name == "run_task"][started:]
    facts: dict[str, Any] = {
        "dag_id": step.dag_id,
        "run_state": run_state,
        "tasks": states,
        "run_task_calls": len(calls),
        "task_definitions": rig.ecs.sent[sent:],
        "errors": observer.errors,
        "bridge_requests": len(requests),
    }
    check_run(rig, step, facts, calls)
    return facts


def check_run(rig: Any, step: Step, facts: dict[str, Any], calls: list[dict[str, Any]]) -> None:
    """Break an expectation when the run is not the step's."""
    where = f"{step.dag_id} {dict(step.params).get('run_id', '')}"
    wanted = dict(step.states) if step.states is not None else {task: DONE_STATE for task in facts["tasks"]}
    require(facts["tasks"], f"{where}: no task ran: {facts}")
    require(facts["run_state"] == step.run_state, f"{where}: the DAG run ended {facts['run_state']}, not {step.run_state}: {facts['tasks']} {facts['errors']}")
    require(facts["tasks"] == wanted, f"{where}: task states {facts['tasks']}, expected {wanted}: {facts['errors']}")
    require(facts["run_task_calls"] == step.starts, f"{where}: {facts['run_task_calls']} RunTask calls, expected {step.starts}: {facts['errors']}")
    if step.reason is not None:
        text = " | ".join(facts["errors"])
        require(step.reason in text, f"{where}: no failing task named {step.reason!r}: {text or 'no error was raised'}")
    if step.starts == 0:
        require(step.run_state == "success" or not rig.ecs.count("describe_tasks"), f"{where}: a refused run read task state")
        return
    cases = rig_modules().cases
    family = step.family
    require(family is not None, f"{where}: the step names no family")
    require(
        all(sent == rig.bound[str(family)] for sent in facts["task_definitions"]),
        f"{where}: RunTask did not name the revision the preflight checked ({rig.bound[str(family)]}): {facts['task_definitions']}",
    )
    first = calls[0]
    require(first["cluster"] == rig_modules().fake.CLUSTER and first["count"] == 1, f"{where}: the RunTask call is not one task on the cluster: {first}")
    require(
        [item["capacityProvider"] for item in first["capacityProviderStrategy"]] == ["stand-in_host"] and "networkConfiguration" not in first,
        f"{where}: the RunTask call is not a host (bridge) launch: {first}",
    )
    names = {item["name"] for item in first["overrides"]["containerOverrides"]}
    planned = set(rig_modules().cases.host_planned_containers()[str(family).removeprefix("example_dev_job_").removesuffix("_host")]["containers"])
    require(bool(names) and names <= planned - {"init"}, f"{where}: overrides for containers {names} that the host definition {planned} does not run")
    run_id = str(dict(step.params)["run_id"])
    environment = {pair["name"]: pair["value"] for item in first["overrides"]["containerOverrides"] for pair in item["environment"]}
    require(environment.get("RUN_ID", run_id) == run_id or run_id in environment.values(), f"{where}: no override carries the run id: {environment}")
    job = str(family).removeprefix("example_dev_job_").removesuffix("_host")
    records = launch_records(rig, run_id, job)
    require({"claim", "intent", "started", "end"} <= set(records), f"{where}: the launch records are {sorted(records)}")
    outcome = step.end or "succeeded"
    require(records["end"]["outcome"] == outcome, f"{where}: the end record says {records['end']['outcome']}, not {outcome}: {records['end']}")
    arns = [task for task in rig.ecs.tasks]
    require(
        len(arns) >= 1 and records["started"].get("task_arn", records["end"].get("task_arn")) in (*arns, None),
        f"{where}: the record names no task of the cluster",
    )
    del cases


def execute(scenario: Scenario, bag: Any, scratch: str) -> dict[str, Any]:
    """Run a scenario's steps in order over one rig. Every step runs even when an earlier one broke an expectation, and the first break is the verdict."""
    started = time.monotonic()
    rig = scenario.rig()
    steps: list[dict[str, Any]] = []
    broken: str | None = None
    for step in scenario.steps:
        try:
            steps.append(run_step(bag, rig, step, scratch))
        except Exception as error:
            detail = f"{type(error).__name__}: {error}" if isinstance(error, Expectation) else "".join(traceback.format_exception(error))[-1500:]
            steps.append({"dag_id": step.dag_id, "error": detail[-1500:]})
            broken = broken or detail
    if scenario.after is not None and broken is None:
        try:
            scenario.after(rig)
        except Exception as error:
            broken = f"{type(error).__name__}: {error}"
    return {"name": scenario.name, "ok": broken is None, "detail": broken, "seconds": round(time.monotonic() - started, 1), "steps": steps}


def form_check(_rig: Any) -> None:
    """The trigger form of every shipped DAG, validated by Airflow's own ``Param`` code as the trigger does (the DAG's params updated with the trigger's
    values, then validated). Each DAG accepts a trigger with only its required values and fills the defaults. It refuses a missing run ID, a run ID that
    breaks its pattern, ``latest`` where a producer run is required and a value of another JSON type."""
    import copy

    c = rig_modules().cases
    run, other = c.SILVER_RUN, c.GOLD_RUN
    required: dict[str, dict[str, Any]] = {
        "ingestion_dag": {"run_id": c.PUBLISHER_RUN},
        "silver_transform_dag": {"run_id": run},
        "gold_transform_dag": {"run_id": other, "silver_snapshot_run": run},
        "features_transform_dag": {"run_id": other, "gold_snapshot_run": run},
        "feature_store_dag": {"run_id": other, "gold_snapshot_run": run},
        "drift_monitoring_dag": {"run_id": run},
        "training_dag": {"run_id": run},
        "model_promotion_dag": {"run_id": run, "hai_type": "HAI_1", "model_version": "3"},
        "serving_deploy_dag": {"run_id": run},
        "metadata_ingest_dag": {"run_id": run},
        "bronze_chain_dag": {"run_id": run},
        "launch_settle_dag": {"run_id": other, "launch_run": run, "job": "silver", "statement": c.SETTLE_STATEMENT},
    }
    wrong = ({}, {"run_id": "latest"}, {"run_id": "x" + run}, {"run_id": 20261104}, {"run_id": run, "mode": "mock"})

    def validated(dag_id: str, conf: Mapping[str, Any]) -> dict[str, Any]:
        dag = BAG["bag"].get_dag(dag_id)
        params = copy.deepcopy(dag.params)
        params.update(dict(conf))
        return dict(params.validate())

    def refused(dag_id: str, conf: Mapping[str, Any]) -> bool:
        try:
            validated(dag_id, conf)
        except Exception as error:
            return bool(str(error))
        return False

    problems = []
    for dag_id, conf in required.items():
        try:
            filled = validated(dag_id, conf)
            if "mode" in filled and filled["mode"] not in ("real", "promote"):
                problems.append(f"{dag_id}: the default mode is {filled['mode']!r}")
        except Exception as error:
            problems.append(f"{dag_id} refused its required values {conf}: {type(error).__name__}: {str(error)[:200]}")
        problems.extend(f"{dag_id} accepted {bad}" for bad in wrong if not refused(dag_id, {**conf, **bad} if bad else {}))
    for dag_id, name in (
        ("gold_transform_dag", "silver_snapshot_run"),
        ("features_transform_dag", "gold_snapshot_run"),
        ("feature_store_dag", "gold_snapshot_run"),
    ):
        if not refused(dag_id, {**required[dag_id], name: "latest"}):
            problems.append(f"{dag_id} accepted latest for {name}")
    missing = sorted(set(BAG["bag"].dag_ids) - set(required))
    require(not missing, f"DAGs without a form case: {missing}")
    require(not problems, "; ".join(problems))


def cases_catalog() -> dict[str, Scenario]:
    """Every scenario by name."""
    import importlib

    modules = rig_modules()
    c, b = modules.cases, modules.broker
    preflight = importlib.import_module("orchestration.ecs_preflight")
    training = importlib.import_module("orchestration.training_contract")
    derived = preflight.derived_run_id
    silver, gold = derived(c.BRONZE_RUN, "silver"), None
    gold = derived(silver, "gold")
    features = derived(gold, "features")
    drift = derived(features, "drift")
    train = training.training_run_id(drift)
    marker = json.dumps({"run_id": drift, "files": {}}).encode()
    approval = {
        "run_id": c.PROMOTE_RUN,
        "dag_id": "model_promotion_dag",
        "scope": "promote",
        "approved_on": "2026-10-30",
        "hai_type": "HAI_1",
        "model_version": "3",
    }
    chain = ("preflight", "job", "evidence", "gate")

    def job_states(*, failed: tuple[str, ...] = ()) -> dict[str, str]:
        return {name: "failed" if name in failed else DONE_STATE for name in chain}

    def stage_drift(rig: Any) -> None:
        rig.stage.objects[f"drift/{drift}/_complete.json"] = marker

    def stage_promotion(rig: Any) -> None:
        rig.stage.stage(c.PROMOTE_RUN, "promotion_approval.json", approval)
        rig.stage.stage(c.SERVE_RUN, "serving_request.json", c.SERVING_REQUEST)

    def stage_publisher(rig: Any, run: str = c.PUBLISHER_RUN) -> None:
        rig.stage.stage(run, "publisher_request.json", c.publisher_body(c.PUBLISHER_REQUEST))

    def stage_store(rig: Any) -> None:
        rig.stage_runs(c.STORE_RUN, c.OTHER_RUN)

    def family(job: str) -> str:
        return f"example_dev_job_{job}_host"

    pipeline = Scenario(
        "every_host_dag_runs_end_to_end",
        (
            Step("bronze_chain_dag", {"run_id": c.BRONZE_RUN, "mode": "real"}, 0, skip=r"run_.*|start_metadata"),
            Step("silver_transform_dag", {"run_id": silver, "mode": "real"}, 1, family("silver"), job_states()),
            Step("gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}, 1, family("gold"), job_states()),
            Step("features_transform_dag", {"run_id": features, "mode": "real", "gold_snapshot_run": gold}, 1, family("features")),
            Step("feature_store_dag", {"run_id": c.STORE_RUN, "mode": "real", "gold_snapshot_run": gold}, 1, family("features"), prepare=stage_store),
            Step(
                "drift_monitoring_dag",
                {"run_id": drift, "mode": "real", "mlflow": "aws"},
                1,
                family("drift"),
                {"drift_run": DONE_STATE, "decide": DONE_STATE, "trigger_training": "skipped"},  # a quiet run maps no trigger
                prepare=stage_drift,
            ),
            Step("training_dag", {**c.TRAIN_PARAMS, "run_id": train}, 1, family("train")),
            Step("model_promotion_dag", dict(c.PROMOTE_PARAMS), 1, family("promote"), job_states(), prepare=stage_promotion),
            Step("serving_deploy_dag", {"run_id": c.SERVE_RUN, "mode": "real"}, 1, family("serve"), job_states()),
            Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 1, family("publisher"), job_states(), prepare=stage_publisher),
            Step("metadata_ingest_dag", {"run_id": c.OTHER_RUN, "mode": "real"}, 1, family("metadata"), prepare=stage_store),
            Step("gold_transform_dag", {"run_id": gold, "mode": "real", "silver_snapshot_run": silver}, 0, family("gold"), job_states()),  # joins the launch
            Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 0, family("publisher"), job_states()),
        ),
        make_rig,
    )

    def ingestion(name: str, rig: Callable[[], Any], **changes: Any) -> Scenario:
        step = Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 0, family("publisher"), prepare=stage_publisher, **changes)
        return Scenario(name, (step,), rig, after=no_launch)

    def no_launch(rig: Any) -> None:
        require(not rig.ecs.tasks and not rig.records.objects, f"a refused run started a task or wrote a launch record: {sorted(rig.records.objects)}")

    def settled(rig: Any) -> None:
        end = rig.end(c.SILVER_RUN, "silver")
        require(end is not None and end["outcome"] in ("cancelled", "failed_closed"), f"the unfinished launch has no end record after the settle: {end}")

    refused = {"run_state": "failed", "states": {"preflight": "failed", "job": "upstream_failed", "evidence": "upstream_failed", "gate": "failed"}}

    def other_capacity() -> Any:
        return make_rig(capacity="usable_memory_mib=65536 system_reserve_mib=2048")

    def unbound() -> Any:
        rig = make_rig()
        rig.ecs.register("example_dev_job_publisher_host", 25600, None)
        return rig

    def no_digest() -> Any:
        rig = make_rig()
        del rig.stage.objects[c.RUN_KEY.format(run=c.PUBLISHER_RUN, name="code.sha256")]
        return rig

    refusals = (
        ingestion("hold_set_refuses_before_any_launch", lambda: make_rig(hold=c.HELD), reason="launch hold", **refused),
        ingestion(
            "hold_parameter_missing_refuses_before_any_launch",
            lambda: make_rig(hold=rig_modules().fake.client_error("ParameterNotFound", "GetParameter")),
            reason="launch hold",
            **refused,
        ),
        ingestion("capacity_mismatch_refuses_before_any_launch", other_capacity, reason="bound to", **refused),
        ingestion("unbound_revision_refuses_before_any_launch", unbound, reason="carries no host capacity binding", **refused),
        ingestion("missing_code_binding_refuses_before_any_launch", no_digest, reason="code_digest_not_staged", **refused),
        Scenario(
            "missing_request_refuses_before_any_launch",
            (Step("ingestion_dag", {"run_id": c.PUBLISHER_RUN, "mode": "real"}, 0, family("publisher"), reason="publisher_request", **refused),),
            make_rig,
            after=no_launch,
        ),
        Scenario(
            "a_task_that_exits_nonzero_fails_the_run",
            (
                Step(
                    "ingestion_dag",
                    {"run_id": c.PUBLISHER_RUN, "mode": "real"},
                    1,
                    family("publisher"),
                    {"preflight": DONE_STATE, "job": "failed", "evidence": "upstream_failed", "gate": "failed"},
                    run_state="failed",
                    reason="ended failed",
                    end="failed",
                    plan={"run": 1},
                    prepare=stage_publisher,
                ),
            ),
            make_rig,
        ),
    )
    settle = Scenario(
        "launch_settle_dag_settles_an_unfinished_launch",
        (Step("launch_settle_dag", c.settle_params(), 0, states={"settle": DONE_STATE}, end="settled"),),
        settle_rig,
        after=settled,
    )
    del b
    form = Scenario("trigger_forms_validate_in_airflow", (), make_rig, after=form_check)
    return {item.name: item for item in (pipeline, *refusals, settle, form)}


def settle_rig() -> Any:
    """A rig with a silver launch whose scheduler died after the task started, the task stopped and the send window of its claim over."""
    cases = rig_modules().cases
    rig = make_rig()
    rig.time.now = time.time() - 700.0
    rig.ecs.clock = lambda: rig.time.now
    cases.unfinished_silver_launch(rig)
    rig.ecs.clock = time.time
    rig.time.now = time.time()
    return rig


def versions() -> dict[str, str]:
    from importlib import metadata

    names = ("apache-airflow", "apache-airflow-providers-amazon", "apache-airflow-task-sdk", "boto3", "botocore")
    return {name: metadata.version(name) for name in names}


def run_all(only: str | None) -> dict[str, Any]:
    """Run the scenarios and return the report."""
    folder = tempfile.mkdtemp(prefix="real_image_dags_")
    scratch = tempfile.mkdtemp(prefix="scratch_", dir=folder)
    configure_airflow(folder)
    started = time.monotonic()
    from airflow.dag_processing.dagbag import DagBag
    from airflow.utils import db

    db.initdb()
    bag = DagBag(dag_folder=str(ROOT / "dags"))
    BAG["bag"] = bag
    imports = {Path(path).name: text[-300:] for path, text in bag.import_errors.items()}
    scenarios = cases_catalog()
    results = [execute(scenario, bag, scratch) for name, scenario in scenarios.items() if only in (None, name)]
    shutil.rmtree(folder, ignore_errors=True)
    covered = sorted({step["dag_id"] for item in results for step in item["steps"] if "dag_id" in step})
    return {
        "at": datetime.now(UTC).isoformat(),
        "versions": versions(),
        "substitutions": list(SUBSTITUTIONS),
        "import_errors": imports,
        "dags": covered,
        "ok": not imports and all(item["ok"] for item in results),
        "scenarios": results,
        "seconds": round(time.monotonic() - started, 1),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", help="run one scenario by name")
    arguments = parser.parse_args()
    report = run_all(arguments.case)
    sys.stdout.write(REPORT_PREFIX + json.dumps(report, default=str) + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
