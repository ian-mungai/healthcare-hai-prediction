"""Cases of the monthly schedule of ``ingestion_dag``, over the Airflow SDK stand-in of ``airflow_mode_cases``.

Isolation is justified because the schedule and the logical date of a run belong to the Airflow scheduler, which is not installed here. Each case runs the
shipped DAG file with the environment a deployment names and a scheduled run's context, over the real preflight, the real launch library and the fake
ECS. The failure modes, each with the case that covers it:

1. The schedule is on where it must not be (development, or a deployment that names no environment), or off in production
   (``schedule_is_production_only``). A misspelt environment name turns the DAG file into an import error instead of silently turning the schedule off
   (``schedule_is_production_only``).
2. A scheduled run names its run from its logical date, runs the monthly kind and never the bootstrap kind, and the same logical date again launches
   nothing new (``scheduled_run_launches_the_monthly_kind_once_per_date``).
3. A hand trigger in production that names no run is refused before any cloud call, one that names its run keeps the kind it names, and a hand trigger in
   development still requires its run (``hand_trigger_in_production_must_name_its_run``, ``development_form_still_requires_its_run``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import ingestion_schedule as schedule
from scripts.orchestration.airflow_mode_cases import Rig, World, world
from scripts.orchestration.ecs_adapter_cases import require
from scripts.orchestration.ecs_chain_cases import Dag
from scripts.orchestration.ecs_launch_cases import refuses

LOGICAL_DATE = datetime(2026, 12, 1, tzinfo=UTC)
PRODUCTION = {schedule.ENVIRONMENT_SETTING: "prod"}
PASSED = {"preflight": "success", "job": "success", "evidence": "success", "gate": "success"}


@contextmanager
def scheduled(
    rig: Rig,
    environment: dict[str, str],
    params: dict[str, Any],
    run_type: str,
    logical_date: datetime = LOGICAL_DATE,
) -> Iterator[tuple[World, list[dict[str, Any]]]]:
    """The DAG runtime under ``environment`` with each task's context holding a DAG run of ``run_type`` and the logical date, and the DAG settings seen."""
    seen: list[dict[str, Any]] = []
    original = Dag.__init__

    def recording(self: Dag, **settings: Any) -> None:
        seen.append(settings)
        original(self, **settings)

    with world(rig, "ingestion_dag", params) as current:
        enter = current.enter

        def entering(task_id: str, attempt: int = 1) -> None:
            enter(task_id, attempt)
            current.context["dag_run"] = SimpleNamespace(run_type=run_type, logical_date=logical_date)

        with (
            mock.patch.dict("os.environ", environment, clear=True),
            mock.patch.object(Dag, "__init__", recording),
            mock.patch.object(current, "enter", entering),
        ):
            yield current, seen


def schedule_is_production_only() -> None:
    named = "20261110T120000Z-ab12cd34"
    for name, cron in {"prod": schedule.MONTHLY, "dev": None}.items():
        params = {"run_id": named, "mode": "real"}
        with scheduled(Rig(), {schedule.ENVIRONMENT_SETTING: name}, params, "manual") as (current, seen):
            current.run_dag("ingestion_dag.py")
        require(bool(seen) and seen[0]["schedule"] == cron, f"{name} got the schedule {seen[0]['schedule'] if seen else None}, not {cron}")
    with scheduled(Rig(), {}, {"run_id": named, "mode": "real"}, "manual") as (current, seen):
        current.run_dag("ingestion_dag.py")
    require(seen[0]["schedule"] is None, "a deployment that names no environment got a schedule")
    with scheduled(Rig(), {schedule.ENVIRONMENT_SETTING: "production"}, {"run_id": named, "mode": "real"}, "manual") as (current, _):
        refuses(ValueError, lambda: current.run_dag("ingestion_dag.py"))


def scheduled_run_launches_the_monthly_kind_once_per_date() -> None:
    rig = Rig()
    run = schedule.scheduled_run_id(LOGICAL_DATE)
    rig.stage_runs(run)
    with scheduled(rig, PRODUCTION, {"mode": "real"}, "scheduled") as (current, _):
        states = current.run_dag("ingestion_dag.py")
    require(states == PASSED, f"the scheduled chain did not pass: {states}")
    require(rig.tasks_started() == 1, f"the scheduled run did not launch the publisher once: {rig.tasks_started()}")
    environment = rig.environment()["run"]
    require(environment["RUN_ID"] == run, f"the publisher did not run under the run id of the logical date: {environment}")
    require(
        (environment["PUBLISHER_RUN_KIND"], environment["HAI_CAPTURE_ROOT"]) == ("monthly", "raw/"),
        f"the scheduled run did not carry the monthly kind under raw/: {environment}",
    )
    with scheduled(rig, PRODUCTION, {"mode": "real"}, "scheduled") as (again, _):
        again.run_dag("ingestion_dag.py")
    require(rig.tasks_started() == 1, "the same logical date started a second task")


def hand_trigger_in_production_must_name_its_run() -> None:
    rig = Rig()
    rig.stage_runs(schedule.scheduled_run_id(LOGICAL_DATE))
    with scheduled(rig, PRODUCTION, {"mode": "real"}, "manual") as (current, _):
        states = current.run_dag("ingestion_dag.py")
    require(states["preflight"] == "failed" and states["job"] == "upstream_failed", f"a hand trigger that names no run did not stop: {states}")
    require(rig.tasks_started() == 0 and not rig.records.objects and not rig.ecs.calls, "a hand trigger with no run reached the cloud")
    named = "20261110T120000Z-ab12cd34"
    rig.stage_runs(named)
    with scheduled(rig, PRODUCTION, {"run_id": named, "mode": "real"}, "manual") as (current, _):
        states = current.run_dag("ingestion_dag.py")
    require(states == PASSED, f"a hand trigger that names its run did not run it: {states}")
    environment = rig.environment()["run"]
    require(
        (environment["RUN_ID"], environment["PUBLISHER_RUN_KIND"]) == (named, "bootstrap"),
        f"a hand trigger in production changed the run or the bootstrap kind it was given: {environment}",
    )


def development_form_still_requires_its_run() -> None:
    with scheduled(Rig(), {schedule.ENVIRONMENT_SETTING: "dev"}, {"mode": "real"}, "manual") as (current, _):
        error = refuses(Exception, lambda: current.run_dag("ingestion_dag.py"))
    require("run_id is required" in str(error), f"the development form accepted a trigger without a run id: {error}")


CASES: dict[str, Callable[[], None]] = {
    "schedule_is_production_only": schedule_is_production_only,
    "scheduled_run_launches_the_monthly_kind_once_per_date": scheduled_run_launches_the_monthly_kind_once_per_date,
    "hand_trigger_in_production_must_name_its_run": hand_trigger_in_production_must_name_its_run,
    "development_form_still_requires_its_run": development_form_still_requires_its_run,
}
