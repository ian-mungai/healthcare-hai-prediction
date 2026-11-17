"""Host session cases for the Prometheus history restore that ``start`` runs after the code checkout and before the services are deployed.

The restore one-shot returns the metric blocks of S3 to the data volume only while Prometheus and its shipper are stopped. ``start`` scales both services to
zero and waits until ECS shows no running or pending task and every task STOPPED, on a cold host that ECS has already filled and on a repeated start alike,
and the deployment that follows sets their count back to one. A service that does not stop, is missing or is not in the state stops the start before the
restore. A stack without the one-shot (the history roles are not applied) starts as before and says so. A failed restore stops the start, and no service is
deployed.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from scripts.infrastructure.host_session import HostSessionError
from scripts.infrastructure.host_session_refresh_cases import CASE_ERRORS, CODE_FETCH_FAMILY, FAMILIES, OPENMETADATA_FAMILY, failure, families_run
from scripts.infrastructure.host_session_stand_in import StandInHost

RESTORE_FAMILY = "stand-in-prometheus-restore"
MONITORING = "stand-in-monitoring"
HISTORY = "stand-in-prometheus-history"
HELD = {"monitoring": MONITORING, "prometheus_history": HISTORY}
__all__ = ["CASES", "CASE_ERRORS"]


def build(faults: set[str] | None = None, with_restore: bool = True, with_services: bool = True) -> tuple[StandInHost, Any, list[str]]:
    """A stand-in host and session as ``start`` sees them, with the restore one-shot in the services state's task families when ``with_restore`` and the
    two services that open the Prometheus data folder (the monitoring task and the history shipper) when ``with_services``, each with a task record."""
    cases = importlib.import_module("scripts.infrastructure.host_session_cases")
    host, session, lines = cases.build(faults, openmetadata=True)
    names = session.names
    if with_restore:
        host.families["prometheus_restore"] = RESTORE_FAMILY
        names = replace(names, one_shot={**names.one_shot, "prometheus_restore": RESTORE_FAMILY})
    if with_services:
        for name in HELD.values():
            host.services[name] = {"desired": 1, "forced_at": None, "state": "IN_PROGRESS", "running": 0, "stop_at": None}
        host.service_tasks_for = set(HELD.values())
        names = replace(names, services={**names.services, **HELD})
    session.names = names
    return host, session, lines


def held_state(host: StandInHost) -> dict[str, dict[str, Any]]:
    """What the services that open the data folder looked like when the restore was launched."""
    return host.restore_snapshots[-1] if host.restore_snapshots else {}


def quiet(snapshot: dict[str, dict[str, Any]]) -> bool:
    """True when every held service shows desired 0, running 0 and no task that is not STOPPED."""
    return set(snapshot) == set(HELD.values()) and all(
        item["desired"] == 0 and item["running"] == 0 and set(item["tasks"]) <= {"STOPPED"} for item in snapshot.values()
    )


def deployments(calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The update-service calls that force a deployment; the hold that scales a service to zero is not one."""
    return [entry for entry in calls if entry["operation"] == "update-service" and "--force-new-deployment" in entry["options"]]


def update_index(calls: list[dict[str, Any]]) -> int:
    return calls.index(deployments(calls)[0])


def start_runs_the_restore_after_the_code_and_before_every_service() -> dict[str, bool]:
    host, session, lines = build()
    session.start()
    first = update_index(host.log)
    before = families_run(host.log[:first])
    steps = [line.split(":", 1)[0] for line in lines]
    return {
        "the restore runs after host_prepare and the code tasks": before[:1] == [FAMILIES["host_prepare"]]
        and before[1:3] == [CODE_FETCH_FAMILY] * 2
        and before[3:] == [RESTORE_FAMILY],
        "no service is deployed before the restore finished": RESTORE_FAMILY in before and not deployments(host.log[:first]),
        "the restore ran once": families_run(host.log).count(RESTORE_FAMILY) == 1,
        "the catalog bootstrap still runs last": families_run(host.log)[-1] == OPENMETADATA_FAMILY,
        "the step is reported": "prometheus_restored" in steps and "prometheus_restore_skipped" not in steps,
        "every service runs one task": all(service["running"] == 1 for service in host.services.values()),
    }


def a_failed_restore_stops_the_start_before_any_service() -> dict[str, bool]:
    host, session, lines = build({"exit_code:prometheus_restore=1"})
    message = failure(session.start)
    return {
        "start fails and names the restore": message is not None and "prometheus_restore" in message,
        "no service was deployed": not deployments(host.log),
        "the host is turned off": host.desired == 0,
        "the failure is reported as a failed start": any(line.startswith("start_failed") for line in lines),
    }


def a_stack_without_the_one_shot_starts_as_before_and_says_so() -> dict[str, bool]:
    host, session, lines = build(with_restore=False)
    session.start()
    return {
        "start finishes with every service running": all(service["running"] == 1 for service in host.services.values()),
        "no restore task ran": RESTORE_FAMILY not in families_run(host.log),
        "the skipped restore is reported by name": any(line.startswith("prometheus_restore_skipped") for line in lines),
    }


def refresh_leaves_the_history_alone() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    session.job_prefix = "stand_in_dev_job_"
    session.names = replace(session.names, state_serial=241)
    count = len(host.log)
    session.refresh()
    return {"refresh does not run the restore: Prometheus is already running on a live host": RESTORE_FAMILY not in families_run(host.log[count:])}


def a_repeated_start_holds_the_services_that_open_the_data_folder_until_the_restore_ends() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    running_before = all(item["running"] == 1 and item["tasks"] == ["RUNNING"] for item in host.service_snapshot().values())
    session.start()
    seen = host.restore_snapshots
    return {
        "the first start left both services running with a task": running_before,
        "both starts ran the restore": len(seen) == 2,
        "at the second restore both services were at desired 0, running 0, and every task STOPPED": len(seen) == 2 and quiet(seen[1]),
        "after the second start both run again with one task": all(
            item["desired"] == 1 and item["running"] == 1 and item["tasks"][-1] == "RUNNING" for item in host.service_snapshot().values()
        ),
    }


def a_cold_host_that_schedules_its_services_by_itself_is_held_before_the_restore() -> dict[str, bool]:
    host, session, _ = build({"auto_schedule"})
    session.start()
    return {
        "ECS had started both services by itself before the restore": all(item["tasks"] for item in host.service_snapshot().values()),
        "at the restore both services were stopped": quiet(held_state(host)),
        "after the start both services run": all(item["running"] == 1 for item in host.service_snapshot().values()),
    }


def a_task_that_is_still_stopping_holds_the_restore_back() -> dict[str, bool]:
    host, session, _ = build({"auto_schedule", "slow_container_stop:40"})
    session.start()
    return {"the restore waited until the task was STOPPED, not only until runningCount was 0": quiet(held_state(host))}


def a_service_that_does_not_stop_fails_the_start_before_the_restore() -> dict[str, bool]:
    host, session, lines = build({"auto_schedule", "never_stop_openmetadata"})
    message = failure(session.start)
    return {
        "start fails and names the held service": message is not None and "stop" in message,
        "the restore never ran": RESTORE_FAMILY not in families_run(host.log),
        "the host is turned off": host.desired == 0,
        "no service was redeployed": not deployments(host.log),
        "the failure is reported": any(line.startswith("start_failed") for line in lines),
    }


def a_held_service_that_ecs_does_not_have_fails_closed() -> dict[str, bool]:
    host, session, _ = build()
    del host.services[MONITORING]
    host.service_tasks_for.discard(MONITORING)
    message = failure(session.start)
    return {
        "start fails": message is not None,
        "the restore never ran": RESTORE_FAMILY not in families_run(host.log),
        "the host is turned off": host.desired == 0,
    }


def a_stack_with_the_one_shot_but_no_monitoring_service_fails_closed() -> dict[str, bool]:
    host, session, _ = build(with_services=False)
    message = failure(session.start)
    return {
        "start refuses by name": message is not None and "monitoring" in message,
        "the restore never ran": RESTORE_FAMILY not in families_run(host.log),
        "the host is turned off": host.desired == 0,
    }


def a_failed_restore_leaves_the_services_for_the_next_start_to_release() -> dict[str, bool]:
    host, session, _ = build({"auto_schedule", "exit_code:prometheus_restore=1"})
    failure(session.start)
    held = {name: host.services[name]["desired"] for name in HELD.values()}
    host.faults.discard("exit_code:prometheus_restore=1")
    host.faults.discard("auto_schedule")
    session.start()
    return {
        "after the failed restore both services stay at desired 0 (the host is off)": held == {MONITORING: 0, HISTORY: 0},
        "the next start releases them to desired 1 and they run": all(
            host.services[name]["desired"] == 1 and host.services[name]["running"] == 1 for name in HELD.values()
        ),
    }


def refresh_sets_the_held_services_back_to_one() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    for name in HELD.values():
        host.services[name].update(desired=0, running=0, state="COMPLETED")
    session.job_prefix = "stand_in_dev_job_"
    session.names = replace(session.names, state_serial=241)
    session.refresh()
    return {"a refresh after a held start brings both services back to desired 1": all(host.services[name]["desired"] == 1 for name in HELD.values())}


def the_restore_is_one_shot_of_the_session_deadline() -> dict[str, bool]:
    session_module = importlib.import_module("scripts.infrastructure.host_session")
    return {
        "the restore has its own limit": isinstance(session_module.PROMETHEUS_RESTORE_SECONDS, int) and 0 < session_module.PROMETHEUS_RESTORE_SECONDS <= 3600,
        "the one-shot key is the name the services stack plans": session_module.PROMETHEUS_RESTORE == "prometheus_restore",
        "a HostSessionError type exists for the failure": issubclass(session_module.TaskExitError, HostSessionError),
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "prometheus_restore_runs_after_the_code_and_before_every_service": start_runs_the_restore_after_the_code_and_before_every_service,
    "prometheus_restore_failure_stops_the_start_before_any_service": a_failed_restore_stops_the_start_before_any_service,
    "prometheus_restore_absent_stack_starts_as_before_and_says_so": a_stack_without_the_one_shot_starts_as_before_and_says_so,
    "prometheus_restore_refresh_leaves_the_history_alone": refresh_leaves_the_history_alone,
    "prometheus_restore_repeated_start_holds_the_services": a_repeated_start_holds_the_services_that_open_the_data_folder_until_the_restore_ends,
    "prometheus_restore_cold_host_services_are_held": a_cold_host_that_schedules_its_services_by_itself_is_held_before_the_restore,
    "prometheus_restore_waits_for_a_stopping_task": a_task_that_is_still_stopping_holds_the_restore_back,
    "prometheus_restore_service_that_does_not_stop_fails_the_start": a_service_that_does_not_stop_fails_the_start_before_the_restore,
    "prometheus_restore_missing_held_service_fails_closed": a_held_service_that_ecs_does_not_have_fails_closed,
    "prometheus_restore_without_monitoring_service_fails_closed": a_stack_with_the_one_shot_but_no_monitoring_service_fails_closed,
    "prometheus_restore_failed_restore_services_released_by_next_start": a_failed_restore_leaves_the_services_for_the_next_start_to_release,
    "prometheus_restore_refresh_releases_the_held_services": refresh_sets_the_held_services_back_to_one,
    "prometheus_restore_limits_and_key": the_restore_is_one_shot_of_the_session_deadline,
}
