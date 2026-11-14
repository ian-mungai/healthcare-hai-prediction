"""Cases of the monthly schedule of ``ingestion_dag``, over the Airflow SDK stand-in of ``airflow_mode_cases``.

Isolation is justified because the schedule and the logical date of a run belong to the Airflow scheduler, which is not installed here. Each case runs the
shipped DAG file with the environment a deployment names and a scheduled run's context, over the real preflight, the real launch library and the fake
ECS. The failure modes, each with the case that covers it:

1. The schedule is on where it must not be (development, or a deployment that names no environment), or off in production
   (``schedule_is_production_only``). A misspelt environment name turns the DAG file into an import error instead of silently turning the schedule off
   (``schedule_is_production_only``).
2. A scheduled run names its run from its logical date, runs the monthly kind and never the bootstrap kind, and the same logical date again launches
   nothing new (``scheduled_run_launches_the_monthly_kind_once_per_date``).
3. The daily retry runs on days 2 to 8 in production only, runs the retry kind under the run id of its original run and day, and refuses a day outside the
   seven or half a name before any cloud call (``retry_schedule_is_production_only``, ``a_scheduled_retry_runs_each_of_the_seven_days_once``,
   ``a_retry_outside_the_seven_days_is_refused_before_any_cloud_call``, ``a_hand_triggered_retry_names_its_run_and_day_or_is_refused``).
4. A scheduled run stages no code of its own, so it takes the code run that the current-code parameter names and is bound to it once: a run that has a
   binding keeps it when the parameter moves, an unreadable parameter waits and launches nothing, and a parameter that is not a run id is refused
   (``scheduled_run_takes_the_current_code_run_and_keeps_it``, ``current_code_run_that_is_unreadable_or_malformed_launches_nothing``). A deployment that
   names no code run keeps the run's own staged code (``without_a_current_code_run_the_run_uses_its_own_code``).
5. A hand trigger in production that names no run is refused before any cloud call, one that names its run keeps the kind it names, and a hand trigger in
   development still requires its run (``hand_trigger_in_production_must_name_its_run``, ``development_form_still_requires_its_run``).
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import ingestion_schedule as schedule
from orchestration.ecs_adapter import TaskRefused
from orchestration.ecs_preflight import TaskUnavailable, ssm_current_code_run_reader
from scripts.orchestration.airflow_mode_cases import (
    CURRENT_CODE_PARAMETER,
    HOLD_PARAMETER,
    LOCK_PARAMETER,
    LaunchSsm,
    Rig,
    World,
    launch_environment,
    no_lock,
    production_services,
    world,
)
from scripts.orchestration.ecs_adapter_cases import require
from scripts.orchestration.ecs_chain_cases import Dag
from scripts.orchestration.ecs_fake import client_error
from scripts.orchestration.ecs_launch_cases import refuses

LOGICAL_DATE = datetime(2026, 12, 1, tzinfo=UTC)
CURRENT_CODE = "20261110T130000Z-0a1b2c3d"
NEWER_CODE = "20261112T090000Z-1a2b3c4d"
PRODUCTION = {schedule.ENVIRONMENT_SETTING: "prod"}
RETRY_DAG = "ingestion_retry_dag"
ORIGINAL = schedule.scheduled_run_id(LOGICAL_DATE)
SCHEDULED_RETRY = {"original_run": schedule.SCHEDULED_RUN, "day": schedule.SCHEDULED_RETRY_DAY, "mode": "real"}
PASSED = {"preflight": "success", "job": "success", "evidence": "success", "gate": "success"}


@contextmanager
def scheduled(
    rig: Rig,
    environment: dict[str, str],
    params: dict[str, Any],
    run_type: str,
    dag: str = "ingestion_dag",
    logical_date: datetime = LOGICAL_DATE,
) -> Iterator[tuple[World, list[dict[str, Any]]]]:
    """The DAG runtime under ``environment`` with each task's context holding a DAG run of ``run_type`` and the logical date, and the DAG settings seen."""
    seen: list[dict[str, Any]] = []
    original = Dag.__init__

    def recording(self: Dag, **settings: Any) -> None:
        seen.append(settings)
        original(self, **settings)

    with world(rig, dag, params) as current:
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


def reader_of(value: str | None | Exception) -> Callable[[], str | None]:
    def read() -> str | None:
        if isinstance(value, Exception):
            raise value
        return value

    return read


def scheduled_run_takes_the_current_code_run_and_keeps_it() -> None:
    rig = Rig()
    run = schedule.scheduled_run_id(LOGICAL_DATE)
    rig.stage_runs(CURRENT_CODE, NEWER_CODE)
    rig.preflight.current_code_run = reader_of(CURRENT_CODE)
    with scheduled(rig, PRODUCTION, {"mode": "real"}, "scheduled") as (current, _):
        states = current.run_dag("ingestion_dag.py")
    require(states == PASSED, f"the scheduled run did not pass without code of its own: {states}")
    require(rig.environment()["fetch"]["CODE_RUN_ID"] == CURRENT_CODE, f"the publisher did not fetch the current code run: {rig.environment()['fetch']}")
    require(rig.preflight.code_runs.stored(run) == CURRENT_CODE, "the scheduled run was not bound to the code run it used")
    rig.preflight.current_code_run = reader_of(NEWER_CODE)
    with scheduled(rig, PRODUCTION, {"mode": "real"}, "scheduled") as (again, _):
        again.run_dag("ingestion_dag.py")
    require(rig.tasks_started() == 1, "the same logical date started a second task after the parameter moved")
    require(rig.preflight.code_runs.stored(run) == CURRENT_CODE, "the parameter moving changed the code run a bound run uses")


def current_code_run_that_is_unreadable_or_malformed_launches_nothing() -> None:
    run = schedule.scheduled_run_id(LOGICAL_DATE)
    for label, value in (
        ("an unreadable parameter", TaskUnavailable("current_code_run_unreadable")),
        ("a parameter that is not a run id", "latest"),
        ("a parameter with a trailing line break", CURRENT_CODE + "\n"),
    ):
        rig = Rig()
        rig.stage_runs(CURRENT_CODE, run)  # the run's own code is staged too, so a refusal can come only from the parameter
        rig.preflight.current_code_run = reader_of(value)
        with scheduled(rig, PRODUCTION, {"mode": "real"}, "scheduled") as (current, _):
            states = current.run_dag("ingestion_dag.py")
        require(states["preflight"] == "failed" and states["job"] == "upstream_failed", f"{label}: the scheduled run did not stop: {states}")
        require(rig.tasks_started() == 0, f"{label}: a task started")
        require(rig.preflight.code_runs.stored(run) is None, f"{label}: the run was bound to a code run")


def without_a_current_code_run_the_run_uses_its_own_code() -> None:
    """Outside production a run with no current code run uses the code staged under its own run id, is bound to it once, and keeps that code when the
    parameter appears later."""
    rig = Rig()
    run = schedule.scheduled_run_id(LOGICAL_DATE)
    rig.stage_runs(run, NEWER_CODE)
    rig.preflight.current_code_run = reader_of(None)
    with scheduled(rig, PRODUCTION, {"mode": "real"}, "scheduled") as (current, _):
        states = current.run_dag("ingestion_dag.py")
    require(states == PASSED, f"a run with code of its own and no current code run did not pass: {states}")
    require(rig.environment()["fetch"]["CODE_RUN_ID"] == run, f"the publisher did not fetch the code of its own run: {rig.environment()['fetch']}")
    require(rig.preflight.code_runs.stored(run) == run, "a run that used its own code was not bound to it")
    rig.preflight.current_code_run = reader_of(NEWER_CODE)
    with scheduled(rig, PRODUCTION, {"mode": "real"}, "scheduled") as (again, _):
        again.run_dag("ingestion_dag.py")
    require(rig.tasks_started() == 1, "the parameter appearing later started a second task")
    require(rig.preflight.code_runs.stored(run) == run, "the parameter appearing later changed the binding of a run")


def a_run_with_no_current_code_run_reader_is_bound_to_its_own_code() -> None:
    rig = Rig()
    run = schedule.scheduled_run_id(LOGICAL_DATE)
    rig.stage_runs(run)
    rig.preflight.current_code_run = None
    with scheduled(rig, PRODUCTION, {"mode": "real"}, "scheduled") as (current, _):
        states = current.run_dag("ingestion_dag.py")
    require(states == PASSED, f"a run with no parameter configured did not pass: {states}")
    require(rig.preflight.code_runs.stored(run) == run, "a run with no parameter configured was not bound to its own code")


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


def retry_rig() -> Rig:
    """A rig whose monthly run of day 1 is bound to ``CURRENT_CODE``, while the current code run parameter has moved on to ``NEWER_CODE``."""
    rig = Rig()
    rig.stage_runs(CURRENT_CODE, NEWER_CODE)
    rig.preflight.code_runs.bind(ORIGINAL, CURRENT_CODE)
    rig.preflight.current_code_run = reader_of(NEWER_CODE)
    return rig


def retry_schedule_is_production_only() -> None:
    for name, cron in {"prod": "0 0 2-8 * *", "dev": None}.items():
        params = SCHEDULED_RETRY | {"original_run": ORIGINAL, "day": 1}
        with scheduled(retry_rig(), {schedule.ENVIRONMENT_SETTING: name}, params, "manual", RETRY_DAG) as (current, seen):
            current.run_dag("ingestion_retry_dag.py")
        require(bool(seen) and seen[0]["schedule"] == cron, f"{name} got the retry schedule {seen[0]['schedule'] if seen else None}, not {cron}")
    with scheduled(retry_rig(), {}, SCHEDULED_RETRY | {"original_run": ORIGINAL, "day": 1}, "manual", RETRY_DAG) as (current, seen):
        current.run_dag("ingestion_retry_dag.py")
    require(seen[0]["schedule"] is None, "a deployment that names no environment got a retry schedule")


def a_scheduled_retry_runs_each_of_the_seven_days_once() -> None:
    rig = retry_rig()
    runs = []
    for day in range(1, 8):
        date = datetime(2026, 12, 1 + day, tzinfo=UTC)
        with scheduled(rig, PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, date) as (current, _):
            states = current.run_dag("ingestion_retry_dag.py")
        require(states == PASSED, f"retry day {day} did not pass: {states}")
        environment = rig.environment(day - 1)["run"]
        require(environment["PUBLISHER_RUN_KIND"] == "retry" and environment["HAI_CAPTURE_ROOT"] == "raw/", f"day {day} carried {environment}")
        require(environment["RUN_ID"] == schedule.retry_run_id(ORIGINAL, day), f"day {day} ran under {environment['RUN_ID']}")
        require(rig.environment(day - 1)["fetch"]["CODE_RUN_ID"] == CURRENT_CODE, f"day {day} did not run the code of its original run")
        require(rig.preflight.code_runs.stored(environment["RUN_ID"]) == CURRENT_CODE, f"day {day} was not bound to the code of its original run")
        runs.append(environment["RUN_ID"])
    require(len(set(runs)) == 7 and rig.tasks_started() == 7, f"seven distinct retry runs were expected: {runs}")
    with scheduled(rig, PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, datetime(2026, 12, 4, tzinfo=UTC)) as (again, _):
        again.run_dag("ingestion_retry_dag.py")
    require(rig.tasks_started() == 7, "the same retry day again started a task")


def a_retry_outside_the_seven_days_is_refused_before_any_cloud_call() -> None:
    rig = retry_rig()
    for date in (
        datetime(2026, 12, 1, tzinfo=UTC),
        datetime(2026, 12, 9, tzinfo=UTC),
        datetime(2026, 12, 15, tzinfo=UTC),
        datetime(2026, 12, 4, 6, tzinfo=UTC),
    ):
        with scheduled(rig, PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, date) as (current, _):
            states = current.run_dag("ingestion_retry_dag.py")
        require(states["preflight"] == "failed" and states["job"] == "upstream_failed", f"{date} did not stop: {states}")
    require(rig.tasks_started() == 0 and not rig.records.objects and not rig.ecs.calls, "a retry outside its window reached the cloud")


def a_hand_triggered_retry_names_its_run_and_day_or_is_refused() -> None:
    rig = retry_rig()
    for params in (SCHEDULED_RETRY | {"original_run": ORIGINAL}, SCHEDULED_RETRY | {"day": 2}):
        with scheduled(rig, PRODUCTION, dict(params), "manual", RETRY_DAG) as (current, _):
            states = current.run_dag("ingestion_retry_dag.py")
        require(states["preflight"] == "failed", f"a retry that names half its kind did not stop: {params}: {states}")
    require(rig.tasks_started() == 0 and not rig.ecs.calls, "a half-named retry reached the cloud")
    with scheduled(rig, PRODUCTION, {"original_run": ORIGINAL, "day": 2, "mode": "real"}, "manual", RETRY_DAG) as (current, _):
        require(current.run_dag("ingestion_retry_dag.py") == PASSED, "a hand trigger that names its run and day did not run")
    require(rig.environment()["run"]["RUN_ID"] == schedule.retry_run_id(ORIGINAL, 2), "the hand trigger did not run under the run id of its day")


def a_retry_of_a_run_with_its_own_code_uses_that_code() -> None:
    rig = Rig()
    rig.stage_runs(ORIGINAL, NEWER_CODE)
    rig.preflight.current_code_run = reader_of(NEWER_CODE)
    with scheduled(rig, PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, datetime(2026, 12, 3, tzinfo=UTC)) as (current, _):
        states = current.run_dag("ingestion_retry_dag.py")
    require(states == PASSED, f"a retry of a run that ran its own code did not pass: {states}")
    require(rig.environment()["fetch"]["CODE_RUN_ID"] == ORIGINAL, f"the retry did not fetch the code of its original run: {rig.environment()['fetch']}")
    require(rig.preflight.code_runs.stored(schedule.retry_run_id(ORIGINAL, 2)) == ORIGINAL, "the retry was not bound to the run whose code it used")


class MissingParameter:
    """The SSM client of a stack where nobody staged any code: the one parameter does not exist."""

    def get_parameter(self, **request: Any) -> dict[str, Any]:
        raise client_error("ParameterNotFound", "GetParameter")


def a_missing_current_code_run_refuses_in_production_and_names_the_parameter() -> None:
    name = CURRENT_CODE_PARAMETER
    error = refuses(TaskRefused, ssm_current_code_run_reader(MissingParameter(), name, required=True))
    require(name in str(error), f"the refusal does not name the parameter: {error}")
    require(ssm_current_code_run_reader(MissingParameter(), name, required=False)() is None, "a deployment that is not production refused a missing parameter")
    run = schedule.scheduled_run_id(LOGICAL_DATE)
    for label, blind in (("a scheduled run", "scheduled"), ("a hand trigger", "manual")):
        rig = Rig()
        rig.stage_runs(run, "20261110T120000Z-ab12cd34")  # code staged under the run's own id must not be a way around the refusal
        rig.preflight.current_code_run = ssm_current_code_run_reader(MissingParameter(), name, required=True)
        params = {"mode": "real"} if blind == "scheduled" else {"run_id": "20261110T120000Z-ab12cd34", "mode": "real"}
        with scheduled(rig, PRODUCTION, params, blind) as (current, _):
            states = current.run_dag("ingestion_dag.py")
            error_text = str(current.results.get("preflight"))
        require(states["preflight"] == "failed" and states["job"] == "upstream_failed", f"{label}: the run did not stop: {states}")
        require(name in error_text, f"{label}: the refusal does not name the parameter: {error_text}")
        require(rig.tasks_started() == 0, f"{label}: a task started")


def production_services_require_the_current_code_run_only_in_production() -> None:
    for environment, refuses_missing in (({schedule.ENVIRONMENT_SETTING: "prod"}, True), ({schedule.ENVIRONMENT_SETTING: "dev"}, False), ({}, False)):
        ssm = LaunchSsm({HOLD_PARAMETER: ["clear"], LOCK_PARAMETER: no_lock(), CURRENT_CODE_PARAMETER: no_lock()})
        with tempfile.TemporaryDirectory() as scratch:
            built = production_services(
                ssm,
                launch_environment(scratch, HAI_LAUNCH_CURRENT_CODE_RUN_PARAMETER=CURRENT_CODE_PARAMETER) | environment,
            )
        reader = built.preflight.current_code_run
        require(callable(reader), "the production preflight has no current code run reader")
        if refuses_missing:
            require(CURRENT_CODE_PARAMETER in str(refuses(TaskRefused, reader)), "production did not refuse a missing parameter by name")
        else:
            require(reader() is None, f"{environment} refused a missing parameter")


CASES: dict[str, Callable[[], None]] = {
    "schedule_is_production_only": schedule_is_production_only,
    "scheduled_run_launches_the_monthly_kind_once_per_date": scheduled_run_launches_the_monthly_kind_once_per_date,
    "scheduled_run_takes_the_current_code_run_and_keeps_it": scheduled_run_takes_the_current_code_run_and_keeps_it,
    "current_code_run_that_is_unreadable_or_malformed_launches_nothing": current_code_run_that_is_unreadable_or_malformed_launches_nothing,
    "without_a_current_code_run_the_run_uses_its_own_code": without_a_current_code_run_the_run_uses_its_own_code,
    "a_run_with_no_current_code_run_reader_is_bound_to_its_own_code": a_run_with_no_current_code_run_reader_is_bound_to_its_own_code,
    "a_missing_current_code_run_refuses_in_production_and_names_the_parameter": a_missing_current_code_run_refuses_in_production_and_names_the_parameter,
    "production_services_require_the_current_code_run_only_in_production": production_services_require_the_current_code_run_only_in_production,
    "hand_trigger_in_production_must_name_its_run": hand_trigger_in_production_must_name_its_run,
    "development_form_still_requires_its_run": development_form_still_requires_its_run,
    "retry_schedule_is_production_only": retry_schedule_is_production_only,
    "a_scheduled_retry_runs_each_of_the_seven_days_once": a_scheduled_retry_runs_each_of_the_seven_days_once,
    "a_retry_outside_the_seven_days_is_refused_before_any_cloud_call": a_retry_outside_the_seven_days_is_refused_before_any_cloud_call,
    "a_hand_triggered_retry_names_its_run_and_day_or_is_refused": a_hand_triggered_retry_names_its_run_and_day_or_is_refused,
    "a_retry_of_a_run_with_its_own_code_uses_that_code": a_retry_of_a_run_with_its_own_code_uses_that_code,
}
