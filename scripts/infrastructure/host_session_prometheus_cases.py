"""Host session cases for the Prometheus history restore that ``start`` runs after the code checkout and before every service.

The restore one-shot returns the metric blocks of S3 to the data volume while Prometheus is not running yet. A stack without the one-shot (the history roles are
not applied) starts as before and says so. A failed restore stops the start, and no service is deployed.
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
__all__ = ["CASES", "CASE_ERRORS"]


def build(faults: set[str] | None = None, with_restore: bool = True) -> tuple[StandInHost, Any, list[str]]:
    """A stand-in host and session as ``start`` sees them, with the restore one-shot in the services state's task families when ``with_restore``."""
    cases = importlib.import_module("scripts.infrastructure.host_session_cases")
    host, session, lines = cases.build(faults, openmetadata=True)
    if with_restore:
        host.families["prometheus_restore"] = RESTORE_FAMILY
        session.names = replace(session.names, one_shot={**session.names.one_shot, "prometheus_restore": RESTORE_FAMILY})
    return host, session, lines


def update_index(calls: list[dict[str, Any]]) -> int:
    return next(index for index, entry in enumerate(calls) if entry["operation"] == "update-service")


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
        "no service is updated before the restore finished": RESTORE_FAMILY in before
        and all(entry["operation"] != "update-service" for entry in host.log[:first]),
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
        "no service was updated": all(entry["operation"] != "update-service" for entry in host.log),
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
    "prometheus_restore_limits_and_key": the_restore_is_one_shot_of_the_session_deadline,
}
