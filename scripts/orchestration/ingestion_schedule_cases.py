"""Cases of the monthly schedule of ``ingestion_dag``, over the Airflow SDK stand-in of ``airflow_mode_cases``.

Isolation is justified because the schedule and the logical date of a run belong to the Airflow scheduler, which is not installed here. Each case runs the
shipped DAG file with the environment a deployment names and a scheduled run's context, over the real preflight, the real launch library and the fake
ECS. The failure modes, each with the case that covers it:

1. The schedule is on where it must not be (development, or a deployment that names no environment), or off in production
   (``schedule_is_production_only``). A misspelt environment name turns the DAG file into an import error instead of silently turning the schedule off
   (``schedule_is_production_only``).
2. A scheduled run names its run from its logical date, runs the monthly kind and never the bootstrap kind, and the same logical date again launches
   nothing new (``scheduled_run_launches_the_monthly_kind_once_per_date``).
3. The daily retry runs on days 2 and 3 in production only, runs the retry kind under the run id of its original run and day, and refuses a day outside the
   two or half a name before any cloud call (``retry_schedule_is_production_only``, ``a_scheduled_retry_runs_each_of_the_two_days_once``,
   ``a_retry_outside_the_two_days_is_refused_before_any_cloud_call``, ``a_hand_triggered_retry_names_its_run_and_day_or_is_refused``).
4. A scheduled run stages no code of its own, so it takes the code run that the current-code parameter names and is bound to it once: a run that has a
   binding keeps it when the parameter moves, an unreadable parameter waits and launches nothing, and a parameter that is not a run id is refused
   (``scheduled_run_takes_the_current_code_run_and_keeps_it``, ``current_code_run_that_is_unreadable_or_malformed_launches_nothing``). A deployment that
   names no code run keeps the run's own staged code (``without_a_current_code_run_the_run_uses_its_own_code``).
5. A hand trigger in production that names no run is refused before any cloud call, one that names its run keeps the kind it names, and a hand trigger in
   development still requires its run (``hand_trigger_in_production_must_name_its_run``, ``development_form_still_requires_its_run``).
"""

from __future__ import annotations

import importlib
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
from orchestration.training_contract import PIPELINE_DAG
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
WAITS = PASSED | {"start_pipeline": "skipped"}  # the pipeline start of a retry run that does not end a retry period
STARTS = PASSED | {"start_pipeline": "success"}  # the pipeline start of a retry run that does


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


def cron_of(setting: Any) -> str | None:
    """The cron string of a DAG's schedule: the string itself, or the cron of its timetable."""
    return setting if setting is None or isinstance(setting, str) else str(setting.cron)


def schedule_is_production_only() -> None:
    named = "ingestion_ab12cd34"
    for name, cron in {"prod": schedule.MONTHLY, "dev": None}.items():
        params = {"run_id": named, "mode": "real"}
        with scheduled(Rig(), {schedule.ENVIRONMENT_SETTING: name}, params, "manual") as (current, seen):
            current.run_dag("ingestion_dag.py")
        require(bool(seen) and cron_of(seen[0]["schedule"]) == cron, f"{name} got the schedule {seen[0]['schedule'] if seen else None}, not {cron}")
        if cron is not None:
            timetable = seen[0]["schedule"]
            require(
                type(timetable).__name__ == "IngestionMonthlyTimetable" and timetable.timezone == "UTC",
                f"the monthly schedule is {timetable!r}, not the timetable of the ingestion run IDs",
            )
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
    require(not current.dag_runs, "a scheduled ingestion run started a run of its own")
    require(rig.tasks_started() == 1, f"the scheduled run did not launch the publisher once: {rig.tasks_started()}")
    environment = rig.environment()["run"]
    require(environment["RUN_ID"] == run, f"the publisher did not run under the run id of the logical date: {environment}")
    require(
        (environment["PUBLISHER_RUN_KIND"], environment["HAI_CAPTURE_ROOT"]) == ("monthly", "raw/"),
        f"the scheduled run did not carry the monthly kind under raw/: {environment}",
    )
    require(
        "PUBLISHER_ORIGINAL_RUN" not in environment and "PUBLISHER_RETRY_DAY" not in environment,
        f"a monthly run carried a retry variable: {environment}",
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
    named = "ingestion_ab12cd34"
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
    for name, cron in {"prod": "0 0 2-3 * *", "dev": None}.items():
        params = SCHEDULED_RETRY | {"original_run": ORIGINAL, "day": schedule.RETRY_FIRST_DAY}
        with scheduled(retry_rig(), {schedule.ENVIRONMENT_SETTING: name}, params, "manual", RETRY_DAG) as (current, seen):
            current.run_dag("ingestion_retry_dag.py")
        require(bool(seen) and cron_of(seen[0]["schedule"]) == cron, f"{name} got the retry schedule {seen[0]['schedule'] if seen else None}, not {cron}")
        if cron is not None:
            timetable = seen[0]["schedule"]
            require(
                type(timetable).__name__ == "IngestionRetryTimetable" and timetable.timezone == "UTC",
                f"the retry schedule is {timetable!r}, not the retry timetable",
            )
    with scheduled(retry_rig(), {}, SCHEDULED_RETRY | {"original_run": ORIGINAL, "day": schedule.RETRY_FIRST_DAY}, "manual", RETRY_DAG) as (current, seen):
        current.run_dag("ingestion_retry_dag.py")
    require(seen[0]["schedule"] is None, "a deployment that names no environment got a retry schedule")


def a_scheduled_retry_runs_each_of_the_two_days_once() -> None:
    rig = retry_rig()
    runs = []
    for day in range(schedule.RETRY_FIRST_DAY, schedule.RETRY_LAST_DAY + 1):
        date = datetime(2026, 12, day, tzinfo=UTC)
        with scheduled(rig, PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, date) as (current, _):
            states = current.run_dag("ingestion_retry_dag.py")
        require(states == (STARTS if day == schedule.RETRY_LAST_DAY else WAITS), f"retry day {day} did not pass: {states}")
        environment = rig.environment(day - schedule.RETRY_FIRST_DAY)["run"]
        require(environment["PUBLISHER_RUN_KIND"] == "retry" and environment["HAI_CAPTURE_ROOT"] == "raw/", f"day {day} carried {environment}")
        require(environment["RUN_ID"] == schedule.retry_run_id(ORIGINAL, day), f"day {day} ran under {environment['RUN_ID']}")
        require(
            (environment["PUBLISHER_ORIGINAL_RUN"], environment["PUBLISHER_RETRY_DAY"]) == (ORIGINAL, str(day)),
            f"day {day} did not carry its original run and its day to the container: {environment}",
        )
        require(rig.environment(day - schedule.RETRY_FIRST_DAY)["fetch"]["CODE_RUN_ID"] == CURRENT_CODE, f"day {day} did not run the code of its original run")
        require(rig.preflight.code_runs.stored(environment["RUN_ID"]) == CURRENT_CODE, f"day {day} was not bound to the code of its original run")
        runs.append(environment["RUN_ID"])
    require(len(set(runs)) == 2 and rig.tasks_started() == 2, f"two distinct retry runs were expected: {runs}")
    with scheduled(rig, PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, datetime(2026, 12, 3, tzinfo=UTC)) as (again, _):
        again.run_dag("ingestion_retry_dag.py")
    require(rig.tasks_started() == 2, "the same retry day again started a task")


def a_retry_outside_the_two_days_is_refused_before_any_cloud_call() -> None:
    rig = retry_rig()
    for date in (
        datetime(2026, 12, 1, tzinfo=UTC),
        datetime(2026, 12, 4, tzinfo=UTC),
        datetime(2026, 12, 9, tzinfo=UTC),
        datetime(2026, 12, 15, tzinfo=UTC),
        datetime(2026, 12, 3, 6, tzinfo=UTC),
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
        require(current.run_dag("ingestion_retry_dag.py") == WAITS, "a hand trigger that names its run and day did not run, or started a pipeline run")
    require(rig.environment()["run"]["RUN_ID"] == schedule.retry_run_id(ORIGINAL, 2), "the hand trigger did not run under the run id of its day")


def a_retry_of_a_run_with_its_own_code_uses_that_code() -> None:
    rig = Rig()
    rig.stage_runs(ORIGINAL, NEWER_CODE)
    rig.preflight.current_code_run = reader_of(NEWER_CODE)
    with scheduled(rig, PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, datetime(2026, 12, 3, tzinfo=UTC)) as (current, _):
        states = current.run_dag("ingestion_retry_dag.py")
    require(states == STARTS, f"a retry of a run that ran its own code did not pass: {states}")
    require(rig.environment()["fetch"]["CODE_RUN_ID"] == ORIGINAL, f"the retry did not fetch the code of its original run: {rig.environment()['fetch']}")
    require(rig.preflight.code_runs.stored(schedule.retry_run_id(ORIGINAL, 3)) == ORIGINAL, "the retry was not bound to the run whose code it used")


PIPELINE_START = [(PIPELINE_DAG, "pipeline_ab12cd34", {"run_id": "pipeline_ab12cd34", "mode": "real", "start_from": "bronze"})]


def a_hand_started_ingestion_run_starts_nothing_after_it() -> None:
    """The first DAG runs the stages after ingestion, so ingestion alone ends at its gate: no start task, no DAG run, whatever the capture root."""
    for environment in (PRODUCTION, {}):
        rig = Rig()
        rig.stage_runs("ingestion_ab12cd34")
        with scheduled(rig, environment, {"run_id": "ingestion_ab12cd34", "mode": "real"}, "manual") as (current, _):
            states = current.run_dag("ingestion_dag.py")
        require(states == PASSED and not current.dag_runs, f"a hand-started ingestion run did not end at its gate: {states} {current.dag_runs}")
    rig = Rig()
    rig.stage_runs("ingestion_ab12cd34")
    test_root = "ingestion_test/ingestion_ab12cd34/"
    with scheduled(rig, {}, {"run_id": "ingestion_ab12cd34", "mode": "real", "capture_root": test_root}, "manual") as (current, _):
        states = current.run_dag("ingestion_dag.py")
    require(states == PASSED and not current.dag_runs, f"a test capture root started a run: {states} {current.dag_runs}")


def an_existing_pipeline_run_counts_and_an_api_refusal_fails() -> None:
    day = datetime(2026, 12, schedule.RETRY_LAST_DAY, tzinfo=UTC)
    with scheduled(retry_rig(), PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, day) as (current, _):
        current.dag_run_exists = True
        states = current.run_dag("ingestion_retry_dag.py")
    require(states == STARTS and "already exists" in str(current.results["start_pipeline"]), f"{states} {current.results.get('start_pipeline')}")
    with scheduled(retry_rig(), PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, day) as (current, _):
        current.dag_run_error = RuntimeError(f"the Airflow API answered 500 to the trigger of {PIPELINE_DAG}")
        states = current.run_dag("ingestion_retry_dag.py")
    require(states["gate"] == "success" and states["start_pipeline"] == "failed", f"a refused pipeline start did not fail visibly: {states}")


def an_existing_pipeline_run_started_with_other_settings_fails_the_start() -> None:
    """A pipeline run of the period's name that exists counts as started only when it was started from bronze as this retry day starts it: a run that began at
    ingestion (the form's default), under another run ID, or with no settings is refused by name."""
    day = datetime(2026, 12, schedule.RETRY_LAST_DAY, tzinfo=UTC)
    pipeline = schedule.pipeline_run_for(ORIGINAL)
    for label, stored, parameter in (
        ("the default start", {"run_id": pipeline, "mode": "real"}, "start_from"),
        ("another run ID", {"run_id": "pipeline_a1a1a1a1", "mode": "real", "start_from": "bronze"}, "run_id"),
        ("no settings", None, "run_id"),
        ("another stage", {"run_id": pipeline, "mode": "real", "start_from": "silver"}, "start_from"),
    ):
        with scheduled(retry_rig(), PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, day) as (current, _):
            current.dag_run_exists, current.existing_conf = True, stored
            states = current.run_dag("ingestion_retry_dag.py")
        message = str(current.results.get("start_pipeline"))
        require(states["gate"] == "success" and states["start_pipeline"] == "failed", f"{label}: the existing run was reused: {states}")
        require(
            pipeline in message and parameter in message and "expected" in message and "found" in message, f"{label}: the refusal names too little: {message}"
        )


def the_last_retry_day_starts_the_pipeline_from_bronze_whatever_its_result() -> None:
    pipeline = schedule.pipeline_run_for(ORIGINAL)
    for day in range(schedule.RETRY_FIRST_DAY, schedule.RETRY_LAST_DAY + 1):
        rig = retry_rig()
        with scheduled(rig, PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, datetime(2026, 12, day, tzinfo=UTC)) as (current, _):
            current.run_dag("ingestion_retry_dag.py")
        expected = [(PIPELINE_DAG, pipeline, {"run_id": pipeline, "mode": "real", "start_from": "bronze"})] if day == schedule.RETRY_LAST_DAY else []
        require(current.dag_runs == expected, f"retry day {day}: the pipeline start was {current.dag_runs}")
    steps = importlib.import_module("orchestration.airflow_steps")
    refused = mock.patch.object(steps, "preflight_step", side_effect=TaskRefused("a refusal of the retry day"))
    with scheduled(retry_rig(), PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, datetime(2026, 12, 3, tzinfo=UTC)) as (current, _), refused:
        states = current.run_dag("ingestion_retry_dag.py")
    require(states["gate"] == "failed" and states["start_pipeline"] == "success", f"the end of the retry period did not start the pipeline: {states}")
    require(len(current.dag_runs) == 1 and current.dag_runs[0][1] == pipeline, f"the pipeline start was {current.dag_runs}")


def a_paused_first_dag_fails_the_start_visibly_and_creates_no_run() -> None:
    day = datetime(2026, 12, schedule.RETRY_LAST_DAY, tzinfo=UTC)
    with scheduled(retry_rig(), PRODUCTION, dict(SCHEDULED_RETRY), "scheduled", RETRY_DAG, day) as (current, _):
        current.paused.add(PIPELINE_DAG)
        states = current.run_dag("ingestion_retry_dag.py")
    require(states["gate"] == "success" and states["start_pipeline"] == "failed", f"a paused first DAG did not fail the start: {states}")
    require(
        "paused" in str(current.results["start_pipeline"]) and PIPELINE_DAG in str(current.results["start_pipeline"]), f"{current.results['start_pipeline']}"
    )
    require(not current.dag_runs, f"a run was created under a paused first DAG: {current.dag_runs}")


def a_hand_started_retry_starts_no_pipeline_run() -> None:
    rig = retry_rig()
    with scheduled(rig, PRODUCTION, {"original_run": ORIGINAL, "day": schedule.RETRY_LAST_DAY, "mode": "real"}, "manual", RETRY_DAG) as (current, _):
        states = current.run_dag("ingestion_retry_dag.py")
    require(states == WAITS and not current.dag_runs, f"a hand retry started the pipeline: {states} {current.dag_runs}")


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
        rig.stage_runs(run, "ingestion_ab12cd34")  # code staged under the run's own id must not be a way around the refusal
        rig.preflight.current_code_run = ssm_current_code_run_reader(MissingParameter(), name, required=True)
        params = {"mode": "real"} if blind == "scheduled" else {"run_id": "ingestion_ab12cd34", "mode": "real"}
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


def the_timetables_name_a_scheduled_run_as_its_job_and_leave_a_hand_run_alone() -> None:
    """The two production DAG files give the scheduler their timetables, and the name a timetable gives a scheduled run is the run ID its job gets. The run
    types other than scheduled keep Airflow's own name. The real Airflow classes are checked by ``schedule_timetable_cases`` in the image."""
    environment = {schedule.ENVIRONMENT_SETTING: "prod"}
    with scheduled(Rig(), environment, {"run_id": "ingestion_ab12cd34", "mode": "real"}, "manual") as (current, seen):
        current.run_dag("ingestion_dag.py")
    monthly = seen[0]["schedule"]
    with scheduled(retry_rig(), environment, SCHEDULED_RETRY | {"original_run": ORIGINAL, "day": schedule.RETRY_FIRST_DAY}, "manual", RETRY_DAG) as (
        current,
        seen,
    ):
        current.run_dag("ingestion_retry_dag.py")
    retry = seen[0]["schedule"]
    require(
        monthly.generate_run_id(run_type="scheduled", run_after=LOGICAL_DATE, data_interval=None) == ORIGINAL, "the monthly timetable names the run otherwise"
    )
    for day in (schedule.RETRY_FIRST_DAY, schedule.RETRY_LAST_DAY):
        named = retry.generate_run_id(run_type="scheduled", run_after=LOGICAL_DATE.replace(day=day), data_interval=None)
        require(named == schedule.retry_run_id(ORIGINAL, day), f"the retry timetable names day {day} {named}")
    for timetable in (monthly, retry):
        hand = timetable.generate_run_id(run_type="manual", run_after=LOGICAL_DATE, data_interval=None)
        require(hand.startswith("manual__"), f"a hand run was named {hand}")
        refuses(
            ValueError, lambda timetable=timetable: timetable.generate_run_id(run_type="scheduled", run_after=LOGICAL_DATE.replace(hour=5), data_interval=None)
        )


CASES: dict[str, Callable[[], None]] = {
    "the_timetables_name_a_scheduled_run_as_its_job_and_leave_a_hand_run_alone": the_timetables_name_a_scheduled_run_as_its_job_and_leave_a_hand_run_alone,
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
    "a_scheduled_retry_runs_each_of_the_two_days_once": a_scheduled_retry_runs_each_of_the_two_days_once,
    "a_retry_outside_the_two_days_is_refused_before_any_cloud_call": a_retry_outside_the_two_days_is_refused_before_any_cloud_call,
    "a_hand_triggered_retry_names_its_run_and_day_or_is_refused": a_hand_triggered_retry_names_its_run_and_day_or_is_refused,
    "a_hand_started_ingestion_run_starts_nothing_after_it": a_hand_started_ingestion_run_starts_nothing_after_it,
    "an_existing_pipeline_run_counts_and_an_api_refusal_fails": an_existing_pipeline_run_counts_and_an_api_refusal_fails,
    "an_existing_pipeline_run_started_with_other_settings_fails_the_start": an_existing_pipeline_run_started_with_other_settings_fails_the_start,
    "the_last_retry_day_starts_the_pipeline_from_bronze_whatever_its_result": the_last_retry_day_starts_the_pipeline_from_bronze_whatever_its_result,
    "a_paused_first_dag_fails_the_start_visibly_and_creates_no_run": a_paused_first_dag_fails_the_start_visibly_and_creates_no_run,
    "a_hand_started_retry_starts_no_pipeline_run": a_hand_started_retry_starts_no_pipeline_run,
    "a_retry_of_a_run_with_its_own_code_uses_that_code": a_retry_of_a_run_with_its_own_code_uses_that_code,
}
