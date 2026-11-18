"""Cases of ``host_session catalog-setup``: the launch of the catalog setup one-shot, its refusals before launch and its log.

Every case runs against the stand-in host; no AWS service is contacted. The cases join ``host_session_cases.CASES``.
"""

from __future__ import annotations

import dataclasses
import functools
import importlib
import json
from collections.abc import Callable

from scripts.infrastructure.host_session import CATALOG_STEP_SECONDS, HostSessionError, Names, Session, TaskExitError, catalog_command, main
from scripts.infrastructure.host_session_stand_in import CATALOG_SETUP_FAMILY, StandInHost


def build(faults: set[str] | None = None, with_task: bool = True) -> tuple[StandInHost, Session, list[str]]:
    """A stand-in host with the catalog setup one-shot (unless ``with_task`` is false) and a session wired to it."""
    host, session, lines = importlib.import_module("scripts.infrastructure.host_session_cases").build(faults)
    host.families = {**host.families, **({"catalog_setup": CATALOG_SETUP_FAMILY} if with_task else {})}
    names: Names = session.names
    session.names = dataclasses.replace(names, one_shot={**names.one_shot, **({"catalog_setup": CATALOG_SETUP_FAMILY} if with_task else {})})
    return host, session, lines


def raised(call: Callable[[], object]) -> str | None:
    try:
        call()
    except HostSessionError as error:
        return str(error)
    return None


def a_step_runs_as_one_task_with_exactly_its_command() -> dict[str, bool]:
    host, session, lines = build()
    session.start()
    result = session.catalog_setup("principal", "bronze", "approval_1", 120)
    launch = host.catalog_launches[-1]
    return {
        "the task ran once": len(host.catalog_launches) == 1,
        "the command is the deadline, the step, the identity and the approval": launch
        == ["--seconds", "120", "principal", "--identity", "bronze", "--namespace-approval", "approval_1"],
        "the result line is the JSON object the task logged": len(result) == 1 and json.loads(result[0])["step"] == "principal",
        "the noise in the log is not a result": all("noise" not in line for line in result),
        "the result is reported": any(line.startswith("catalog_setup_result: {") for line in lines),
    }


def a_step_without_an_identity_has_no_identity_arguments() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    session.catalog_setup("check")
    session.catalog_setup("catalog", seconds=60)
    return {
        "check carries only the default deadline": host.catalog_launches[0] == ["--seconds", str(CATALOG_STEP_SECONDS), "check"],
        "catalog carries its own deadline": host.catalog_launches[1] == ["--seconds", "60", "catalog"],
    }


def a_restore_step_carries_its_deadline_and_the_dry_run_flag_only() -> dict[str, bool]:
    host, session, lines = build()
    session.start()
    session.catalog_setup("restore", seconds=900)
    session.catalog_setup("restore", seconds=60, dry_run=True)
    return {
        "the restore carries the deadline and the step": host.catalog_launches[0] == ["--seconds", "900", "restore"],
        "the dry run adds its flag after the step": host.catalog_launches[1] == ["--seconds", "60", "restore", "--dry-run"],
        "each launch is a task of the same one-shot": len(host.catalog_launches) == 2,
        "the result is reported": any(line.startswith("catalog_setup_result: {") for line in lines),
    }


def arguments_are_checked_before_anything_is_launched() -> dict[str, bool]:
    bad: dict[str, tuple[str, str | None, str | None, int, bool]] = {
        "an identity on the restore step": ("restore", "bronze", None, 60, False),
        "an approval on the restore step": ("restore", None, "a1", 60, False),
        "a dry run on the check step": ("check", None, None, 60, True),
        "a dry run on the principal step": ("principal", "bronze", None, 60, True),
        "an unknown step": ("rotate", None, None, 60, False),
        "a principal step without an identity": ("principal", None, None, 60, False),
        "an identity outside the seven": ("principal", "admin", None, 60, False),
        "an identity on the check step": ("check", "bronze", None, 60, False),
        "an approval on the inspect step": ("inspect", None, "a1", 60, False),
        "an approval ID with a space": ("principal", "bronze", "a 1", 60, False),
        "an approval ID that is too long": ("principal", "bronze", "a" * 65, 60, False),
        "a deadline of zero": ("check", None, None, 0, False),
        "a deadline above an hour": ("check", None, None, 3601, False),
    }
    found: dict[str, bool] = {}
    for label, arguments in bad.items():
        found[f"{label} is refused"] = raised(functools.partial(catalog_command, *arguments)) is not None
    found["the usage error exits 2 before any AWS call"] = main(["catalog-setup", "principal"]) == 2
    found["a dry run without the restore step exits 2 before any AWS call"] = main(["catalog-setup", "check", "--dry-run"]) == 2
    return found


def a_stack_without_the_one_shot_is_refused_before_launch() -> dict[str, bool]:
    host, session, _ = build(with_task=False)
    session.start()
    message = raised(lambda: session.catalog_setup("check"))
    return {
        "the refusal names the missing one-shot and the variable": message is not None
        and "catalog_setup one-shot is missing" in message
        and "catalog_setup_task_role_arn" in message,
        "nothing was launched": not host.catalog_launches,
    }


def services_that_are_not_stable_refuse_the_step() -> dict[str, bool]:
    host, session, _ = build()
    message = raised(lambda: session.catalog_setup("check"))
    return {
        "postgres and polaris must be stable first": message is not None and "not both stable" in message,
        "nothing was launched": not host.catalog_launches,
    }


def a_refusing_task_shows_its_own_line_without_an_arn() -> dict[str, bool]:
    host, session, lines = build({"catalog_setup_refuses"})
    session.start()
    failed = None
    try:
        session.catalog_setup("principal", "silver")
    except TaskExitError as error:
        failed = str(error)
    shown = [line for line in lines if line.startswith("catalog_setup_refused:")]
    return {
        "the task exit is raised": failed is not None and "catalog_setup exit codes" in failed,
        "the refusal line is shown": len(shown) == 1 and "(credentials)" in shown[0],
        "an ARN in the line is replaced": "arn:aws" not in shown[0] and "<arn>" in shown[0],
        "a line that is no result is not shown": not any("noise" in line for line in lines),
    }


def a_missing_log_fails_at_its_own_deadline() -> dict[str, bool]:
    host, session, _ = build({"no_catalog_log"})
    session.start()
    message = raised(lambda: session.catalog_setup("check"))
    return {
        "the missing log is a named failure": message is not None and "catalog setup log not reached" in message,
        "the task itself ran once": len(host.catalog_launches) == 1,
    }


def a_running_task_is_never_reused_for_another_launch() -> dict[str, bool]:
    """A task of the family that is already running is waited for and never taken as the requested launch, even when its first container argument
    equals: the step, the identity and the approval are the launch."""
    host, session, _ = build()
    session.start()
    overrides = {"containerOverrides": [{"name": "catalog_setup", "command": catalog_command("principal", "bronze", None, 60)}]}
    launched = session.run_task("catalog_setup", CATALOG_SETUP_FAMILY, overrides)
    task = launched["tasks"][0]["taskArn"]
    return {
        "a running task with the identical command is not reused": not session.same_code("catalog_setup", [task], overrides),
        "a running task with another step is not reused": not session.same_code(
            "catalog_setup", [task], {"containerOverrides": [{"name": "catalog_setup", "command": catalog_command("inspect", None, None, 60)}]}
        ),
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "catalog_setup_restore_command": a_restore_step_carries_its_deadline_and_the_dry_run_flag_only,
    "catalog_setup_runs_with_exactly_its_command": a_step_runs_as_one_task_with_exactly_its_command,
    "catalog_setup_steps_without_an_identity": a_step_without_an_identity_has_no_identity_arguments,
    "catalog_setup_arguments_checked_first": arguments_are_checked_before_anything_is_launched,
    "catalog_setup_needs_the_one_shot": a_stack_without_the_one_shot_is_refused_before_launch,
    "catalog_setup_needs_stable_services": services_that_are_not_stable_refuse_the_step,
    "catalog_setup_refusal_line": a_refusing_task_shows_its_own_line_without_an_arn,
    "catalog_setup_missing_log": a_missing_log_fails_at_its_own_deadline,
    "catalog_setup_running_task_not_reused": a_running_task_is_never_reused_for_another_launch,
}
