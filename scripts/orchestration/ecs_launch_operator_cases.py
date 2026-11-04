"""Operator cases of the direct ECS launch core. They are stand-in based: the provider operator is a stand-in of apache-airflow-providers-amazon 9.36.0
(``ecs_launch_standin``), because the provider is not installed here. Rerun them against the real provider in the Airflow image.

Each case runs ``execute`` as Airflow does, over the fake ECS API and the in-memory bucket of ``ecs_launch_cases``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from orchestration.ecs_launch_operator import JobFailed, LaunchGuarantees
from scripts.orchestration.ecs_launch_cases import CLUSTER, RUN, Rig, refuses, require, spec
from scripts.orchestration.ecs_launch_standin import MIRRORED_PROVIDER_VERSION, StandInEcsRunTaskOperator

CONSTRAINTS = Path(__file__).resolve().parents[2] / "services" / "orchestration" / "constraints-3.12.txt"


class StandInLaunchOperator(LaunchGuarantees, StandInEcsRunTaskOperator):
    """The launch operator over the stand-in base: the same bases order as the provider subclass."""


class Interrupted(BaseException):
    """The scheduler process ends while the operator waits."""


def operator(rig: Rig, attempt: int = 1, stop_on_kill: bool = False, **changes: Any) -> StandInLaunchOperator:
    job = spec(attempt, **changes)
    built = StandInLaunchOperator(job=job, launcher=rig.launcher, stop_on_kill=stop_on_kill, task_definition="stand-in", cluster=CLUSTER, overrides={})
    built.client = rig.ecs
    return built


def context(attempt: int = 1) -> dict[str, Any]:
    return {"ti": SimpleNamespace(dag_id="gold_transform", task_id="gold", run_id=f"manual__{RUN}", map_index=-1, try_number=attempt)}


def success() -> None:
    rig = Rig()
    operator(rig).execute(context())
    require(rig.ecs.count("run_task") == 1 and rig.events(1) == ["intent", "started", "end"], f"execute launches once and records: {rig.events(1)}")
    require(rig.ecs.tasks[rig.task_arns()[0]]["request"]["clientToken"], "the launch carries a client token the provider alone never sends")


def failure_raises() -> None:
    rig = Rig()
    rig.ecs.plan = {"gold": 3}
    error = refuses(JobFailed, lambda: operator(rig).execute(context()))
    require("failed" in str(error) and "3" in str(error), f"the failure names outcome and exit code: {error}")
    rig = Rig()
    rig.ecs.omit = {"verify"}
    error = refuses(JobFailed, lambda: operator(rig).execute(context()))
    require("acceptance_check_missing:verify" in str(error), f"a missing acceptance container fails the task: {error}")


def refuses_deferrable_and_no_wait() -> None:
    rig = Rig()
    refuses(ValueError, lambda: StandInLaunchOperator(job=spec(), launcher=rig.launcher, task_definition="x", cluster=CLUSTER, overrides={}, deferrable=True))
    refuses(
        ValueError,
        lambda: StandInLaunchOperator(job=spec(), launcher=rig.launcher, task_definition="x", cluster=CLUSTER, overrides={}, wait_for_completion=False),
    )


def reattaches_after_interrupt() -> None:
    rig = Rig()
    rig.ecs.hold = True

    def interrupt(now: float) -> None:
        if len(rig.time.slept) >= 3:
            raise Interrupted

    rig.time.on_sleep = interrupt
    first = operator(rig)
    refuses(Interrupted, lambda: first.execute(context()))
    first.on_kill()
    require(rig.ecs.count("stop_task") == 0, "a kill of the scheduler leaves the task running so the next attempt can reattach")
    rig.time.on_sleep = None
    rig.ecs.hold = False
    operator(rig, 2).execute(context(2))
    require(rig.ecs.count("run_task") == 1 and len(rig.task_arns()) == 1, "the next attempt reattaches")


def stop_on_kill() -> None:
    rig = Rig()
    rig.ecs.hold = True

    def interrupt(now: float) -> None:
        if len(rig.time.slept) >= 2:
            raise Interrupted

    rig.time.on_sleep = interrupt
    first = operator(rig, stop_on_kill=True)
    refuses(Interrupted, lambda: first.execute(context()))
    rig.time.on_sleep = None
    first.on_kill()
    require(
        rig.ecs.tasks[rig.task_arns()[0]]["lastStatus"] == "STOPPED" and rig.record(1, "end")["outcome"] == "cancelled",
        "a requested stop is confirmed and recorded",
    )


def wait_is_bounded() -> None:
    rig = Rig()
    rig.ecs.hold = True
    begin = rig.time.now
    error = refuses(JobFailed, lambda: operator(rig, deadline_seconds=100).execute(context()))
    require("timeout" in str(error) and rig.time.now - begin < 130, f"the provider waits 1,000,000 attempts, this one ends at the deadline: {error}")


def provider_pin() -> None:
    match = re.search(r"^apache-airflow-providers-amazon==(\S+)$", CONSTRAINTS.read_text(), re.MULTILINE)
    require(
        match is not None and match.group(1) == MIRRORED_PROVIDER_VERSION,
        f"the stand-in mirrors {MIRRORED_PROVIDER_VERSION}, the image pins {match and match.group(1)}: re-read the operator source",
    )


CASES: dict[str, Callable[[], None]] = {
    "operator_success": success,
    "operator_failure_raises": failure_raises,
    "operator_refuses_deferrable_and_no_wait": refuses_deferrable_and_no_wait,
    "operator_reattaches_after_interrupt": reattaches_after_interrupt,
    "operator_stop_on_kill": stop_on_kill,
    "operator_wait_is_bounded": wait_is_bounded,
    "provider_pin": provider_pin,
}
