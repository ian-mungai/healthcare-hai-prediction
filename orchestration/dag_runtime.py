"""Build manual pipeline tasks that submit work through the broker and retain failed runs."""

from __future__ import annotations

import contextlib
import logging
import time
from datetime import timedelta
from typing import Any

from airflow.sdk import Asset, TriggerRule, get_current_context, task

from orchestration.client import BrokerClient, BrokerUnavailable
from orchestration.interface import Handle, State, Status, SubmissionRefused, SubmissionRequest

LOG = logging.getLogger(__name__)
STEP_TIMEOUT = timedelta(hours=26)
POLL_SECONDS = 2.0
TERMINAL = (State.SUCCEEDED, State.NO_OP, State.FAILED, State.CANCELLED, State.FAILED_CLOSED)
PASSING = (State.SUCCEEDED,)


def submit_and_wait(client: BrokerClient, request: SubmissionRequest) -> tuple[Handle, Status]:
    """Wait for a broker submission within the task deadline and cancel it if the caller fails."""
    deadline = time.monotonic() + STEP_TIMEOUT.total_seconds()
    handle: Handle | None = None
    try:
        while handle is None:
            try:
                handle = client.submit(request)
            except BrokerUnavailable as error:
                if "answered 503" not in str(error) or time.monotonic() >= deadline:
                    raise
                time.sleep(POLL_SECONDS)
        while time.monotonic() < deadline:
            try:
                status = client.status(handle)
            except BrokerUnavailable as error:
                if "answered 503" not in str(error):
                    raise
            else:
                if status.state in TERMINAL:
                    return handle, status
            time.sleep(POLL_SECONDS)
        raise TimeoutError(f"{request.template} did not end within {STEP_TIMEOUT}")
    except BaseException:
        if handle is not None:
            with contextlib.suppress(BrokerUnavailable, SubmissionRefused):
                client.cancel(handle)
        raise


def run_step(template: str) -> str:
    """Submit the current task's fixed template and return its successful terminal state."""
    context = get_current_context()
    params = context["params"]
    selected = params.get("silver_snapshot_run", "latest") if template == "pin_silver" else "latest"
    instance = context["ti"]
    request = SubmissionRequest(
        template,
        str(params["run_id"]),
        str(params["mode"]),
        int(instance.try_number),
        instance.dag_id,
        instance.task_id,
        instance.run_id,
        None if selected == "latest" else str(selected),
    )
    _, status = submit_and_wait(BrokerClient.from_session(), request)
    if status.state not in PASSING:
        raise RuntimeError(f"{template} ended {status.state.value} with exit code {status.exit_code}")
    return status.state.value


def task_states(task_ids: tuple[str, ...]) -> dict[str, str | None]:
    """Read this DAG run's named task states from Airflow's task-instance API."""
    ti = get_current_context()["ti"]
    found = ti.get_task_states(dag_id=ti.dag_id, task_ids=list(task_ids), run_ids=[ti.run_id])
    states = found.get(ti.run_id, {})
    return {name: states.get(name) for name in task_ids}


def build_tasks(work_steps: tuple[str, ...], family: str) -> None:
    """Create an ordered work chain, an always-run catalog release and a failure-preserving sole release leaf."""

    def step(name: str, rule: TriggerRule = TriggerRule.ALL_SUCCESS) -> Any:
        outlets = [Asset("gold")] if name == "commit_publish" else []

        @task(task_id=name, trigger_rule=rule, retries=0, execution_timeout=STEP_TIMEOUT, outlets=outlets)
        def run() -> str:
            return run_step(name)

        return run().operator

    work = [step(name) for name in work_steps]
    for before, after in zip(work, work[1:], strict=False):
        before >> after
    catalog_release = step("catalog_release", TriggerRule.ALL_DONE)
    for operation in work:
        operation >> catalog_release
    predecessors = (*work_steps, "catalog_release")

    @task(task_id="final_gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=STEP_TIMEOUT)
    def final_gate() -> str:
        states = task_states(predecessors)
        failures = [name for name, state in states.items() if state != "success"]
        verdict = "rejected" if failures else "accepted"
        run_step(f"{family}_final_gate_{verdict}")
        if failures:
            LOG.warning("Pipeline gate rejected tasks: %s", ", ".join(failures))
        return verdict

    @task(task_id="release_run", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=STEP_TIMEOUT)
    def release_run() -> str:
        state = task_states(("final_gate",))["final_gate"]
        verdict = get_current_context()["ti"].xcom_pull(task_ids="final_gate") if state == "success" else None
        accepted = state == "success" and verdict == "accepted"
        run_step("release_accepted" if accepted else "release_rejected")
        if not accepted:
            raise RuntimeError("The pipeline failed its final gate; its lock and recovery inputs are retained")
        return "released"

    gate = final_gate().operator
    release = release_run().operator
    for operation in [*work, catalog_release]:
        operation >> gate
    gate >> release
