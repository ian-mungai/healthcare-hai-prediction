"""The transform DAG on credential-free fixtures: acquire the run, build and validate silver and gold, record the run, then gate and release it.

Every task submits one request through the host job broker (``orchestration.client``) and waits for it to end. The broker maps the template to a launcher
template, a no-op or a run-control action, and the DAG never starts a container itself. The publish steps exist behind the ``publish_branch`` task and are
skipped in mode ``fixture``. The DAG has no schedule and no asset trigger: an owner triggers it with ``{"run_id": "<UTC start>-<8 hex>", "mode": "fixture"}``,
and the supervisor's reconciliation finds the DAG run of a held run by that ``run_id``. The finalization tasks ``catalog_release``, ``final_gate`` and
``release_run`` run after any failure. ``release_run`` is the only leaf, so its state is the DAG run's state. There is no setup or teardown task.
"""

from __future__ import annotations

import contextlib
import logging
import time
from datetime import timedelta
from typing import Any

from airflow.sdk import DAG, Param, TriggerRule, get_current_context, task

from orchestration.client import BrokerClient, BrokerUnavailable
from orchestration.interface import Handle, State, Status, SubmissionRefused, SubmissionRequest

LOG = logging.getLogger(__name__)
RUN_ID_PATTERN = r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"
MODES = ("fixture",)
POLL_SECONDS = 2.0
STEP_TIMEOUT = timedelta(hours=26)
TERMINAL = (State.SUCCEEDED, State.NO_OP, State.FAILED, State.CANCELLED, State.FAILED_CLOSED)
PASSING = (State.SUCCEEDED, State.NO_OP)
SKIPPED_STEPS = ("publish_candidates", "reconcile_candidates", "catalog_backup", "commit_publish")
WORK_STEPS = (
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
    "skip_publish",
    *SKIPPED_STEPS,
    "run_record",
)
GATE_PREDECESSORS = (*WORK_STEPS, "catalog_release")
EXPECTED_SKIPPED = {"fixture": SKIPPED_STEPS}


def unavailable(error: BrokerUnavailable) -> bool:
    return "answered 503" in str(error)


def transform_run() -> tuple[str, str, int]:
    context = get_current_context()
    params = context["params"]
    return str(params["run_id"]), str(params["mode"]), int(context["ti"].try_number)


def submit_and_wait(client: BrokerClient, request: SubmissionRequest) -> tuple[Handle, Status]:
    deadline = time.monotonic() + STEP_TIMEOUT.total_seconds()
    handle: Handle | None = None
    try:
        while handle is None:
            try:
                handle = client.submit(request)
            except BrokerUnavailable as error:
                if not unavailable(error) or time.monotonic() > deadline:
                    raise
                time.sleep(POLL_SECONDS)
        while True:
            try:
                status = client.status(handle)
            except BrokerUnavailable as error:
                if not unavailable(error):
                    raise
            else:
                if status.state in TERMINAL:
                    return handle, status
            if time.monotonic() > deadline:
                raise TimeoutError(f"{request.template} did not end within {STEP_TIMEOUT}")
            time.sleep(POLL_SECONDS)
    except BaseException:
        if handle is not None:
            with contextlib.suppress(BrokerUnavailable, SubmissionRefused):
                client.cancel(handle)
        raise


def run_step(template: str) -> str:
    run_id, mode, attempt = transform_run()
    _, status = submit_and_wait(BrokerClient.from_session(), SubmissionRequest(template, run_id, mode, attempt))
    if status.state not in PASSING:
        raise RuntimeError(f"{template} ended {status.state.value} with exit code {status.exit_code}")
    return status.state.value


def step(name: str, trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS) -> Any:

    @task(task_id=name, trigger_rule=trigger_rule, retries=0, execution_timeout=STEP_TIMEOUT)
    def run() -> str:
        return run_step(name)

    return run().operator


def task_states(*task_ids: str) -> dict[str, str | None]:
    ti = get_current_context()["ti"]
    found = ti.get_task_states(dag_id=ti.dag_id, task_ids=list(task_ids), run_ids=[ti.run_id])
    states: dict[str, str | None] = found.get(ti.run_id, {})
    return {name: states.get(name) for name in task_ids}


def gate_problems(mode: str, states: dict[str, str | None]) -> list[str]:
    skipped = EXPECTED_SKIPPED.get(mode, ("skip_publish",))
    problems = []
    for name, state in states.items():
        wanted = "skipped" if name in skipped else "success"
        if state != wanted:
            problems.append(f"{name} is {state}, expected {wanted}")
    return problems


with DAG(
    dag_id="transform_dag",
    schedule=None,
    catchup=False,
    tags=["orchestration", "transform"],
    description="Silver and gold transform on fixtures through the host job broker",
    params={
        "run_id": Param(
            type="string", pattern=RUN_ID_PATTERN, description="The transform run ID: the UTC start as YYYYMMDDTHHMMSSZ, a dash and 8 hex characters"
        ),
        "mode": Param("fixture", type="string", enum=list(MODES), description="fixture only until the publish steps have bodies"),
    },
    default_args={"retries": 0},
):
    acquire_run = step("acquire_run")
    catalog_up = step("catalog_up")
    pin_bronze = step("pin_bronze")
    silver_build = step("silver_build")
    silver_validate = step("silver_validate")
    gold_build = step("gold_build")
    gold_validate = step("gold_validate")
    cross_layer_checks = step("cross_layer_checks")
    verify_bronze = step("verify_bronze")

    @task.branch(task_id="publish_branch", retries=0)
    def publish_branch() -> str:
        run_step("publish_branch")
        return "skip_publish" if transform_run()[1] == "fixture" else "publish_candidates"

    branch = publish_branch().operator
    skip_publish = step("skip_publish")
    publish_candidates = step("publish_candidates")
    reconcile_candidates = step("reconcile_candidates")
    catalog_backup = step("catalog_backup")
    commit_publish = step("commit_publish")
    run_record = step("run_record", TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    catalog_release = step("catalog_release", TriggerRule.ALL_DONE)

    @task(task_id="final_gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=STEP_TIMEOUT)
    def final_gate() -> str:
        mode = transform_run()[1]
        problems = gate_problems(mode, task_states(*GATE_PREDECESSORS))
        verdict = "rejected" if problems else "accepted"
        run_step(f"final_gate_{verdict}")
        if problems:
            LOG.warning("final_gate rejected the run: %s", "; ".join(problems))
        return verdict

    @task(task_id="release_run", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=STEP_TIMEOUT)
    def release_run() -> str:
        gate_state = task_states("final_gate")["final_gate"]
        verdict = get_current_context()["ti"].xcom_pull(task_ids="final_gate") if gate_state == "success" else None
        accepted = gate_state == "success" and verdict == "accepted"
        run_step("release_accepted" if accepted else "release_rejected")
        if not accepted:
            raise RuntimeError(f"the run was not accepted (final_gate is {gate_state}, result {verdict}); the lock is retained and the build file kept")
        return "released"

    gate = final_gate().operator
    release = release_run().operator

    acquire_run >> catalog_up >> pin_bronze >> silver_build >> silver_validate >> gold_build >> gold_validate >> cross_layer_checks >> verify_bronze >> branch
    branch >> [skip_publish, publish_candidates]
    publish_candidates >> reconcile_candidates >> catalog_backup >> commit_publish
    [commit_publish, skip_publish] >> run_record
    [catalog_up, pin_bronze, silver_build, silver_validate, gold_build, gold_validate, cross_layer_checks, verify_bronze, branch] >> catalog_release
    [skip_publish, publish_candidates, reconcile_candidates, catalog_backup, commit_publish, run_record] >> catalog_release
    [acquire_run, catalog_up, pin_bronze, silver_build, silver_validate, gold_build, gold_validate, cross_layer_checks, verify_bronze, branch] >> gate
    [skip_publish, publish_candidates, reconcile_candidates, catalog_backup, commit_publish, run_record, catalog_release] >> gate
    gate >> release
