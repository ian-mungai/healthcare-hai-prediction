"""Host session cases for ``refresh`` (new code on a running host), for the services state serial and for ``status`` with a service ECS does not have.

Every case builds a stand-in host, starts it with the real ``Session`` and then runs the command under test, so the checks see exactly what the command
sent after the host was on.
"""

from __future__ import annotations

import importlib
import json
import tempfile
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure.host_session import Clock, HostSessionError, Names, Session
from scripts.infrastructure.host_session_stand_in import (
    CLUSTER,
    CODE_FETCH_FAMILY,
    FAMILIES,
    GROUP,
    OPENMETADATA_FAMILY,
    OPENMETADATA_SERVICE,
    PROFILE,
    PROJECT,
    PROVIDER,
    REGION,
    SERVICES,
    StandInHost,
)

CASE_ERRORS = (AssertionError, LookupError, StopIteration, TypeError, ValueError, AttributeError, NameError, HostSessionError)
JOB_PREFIX = "stand_in_dev_job_"
SERIAL = 241
CODE_RUN = "20261115T120000Z-abcdef01"
CODE_SHA = "a" * 64
OLD_SHA = "b" * 64
NEW_SERVICE = "stand-in-streamlit"
WRITE_OPERATIONS = {"set-desired-capacity", "run-task", "update-service"}


def module() -> Any:
    """The ``host_session`` module as it is loaded now (a plant replaces it in ``sys.modules``)."""
    return importlib.import_module("scripts.infrastructure.host_session")


def build(faults: set[str] | None = None, code_fetch: bool = True, checkout: str | None = None) -> tuple[StandInHost, Session, list[str]]:
    """A stand-in host with the catalog service and the code one-shot, and a session whose names carry the state serial and whose job prefix is set."""
    host, lines = StandInHost(faults, openmetadata=True, code_fetch=code_fetch, checkout=checkout), list[str]()
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION}, host.runner)
    services = dict(sorted(dict(SERVICES, openmetadata=OPENMETADATA_SERVICE).items(), key=lambda item: item[0] != "postgres"))
    families = dict(FAMILIES, openmetadata_bootstrap=OPENMETADATA_FAMILY, **({"code_fetch": CODE_FETCH_FAMILY} if code_fetch else {}))
    names = Names(CLUSTER, GROUP, PROVIDER, PROJECT, services, families, SERIAL)
    marker = Path(tempfile.mkdtemp(prefix="host_session_refresh_case_")) / "polaris_bootstrap.launched"
    session = Session(cli, names, Clock(lambda: host.now, host.advance), marker, lines.append, job_prefix=JOB_PREFIX)
    return host, session, lines


def running(faults: set[str] | None = None, code_fetch: bool = True, checkout: str | None = OLD_SHA) -> tuple[StandInHost, Session, list[str]]:
    """The same, after a successful ``start`` on a host that shows the older code ``OLD_SHA``; the report lines start empty again."""
    host, session, lines = build(faults, code_fetch, checkout)
    session.start()
    lines.clear()
    return host, session, lines


def failure(action: Callable[[], object]) -> str | None:
    """The message of the HostSessionError the action raised, or None."""
    try:
        action()
    except HostSessionError as error:
        return str(error)
    return None


def after(host: StandInHost, count: int) -> list[dict[str, Any]]:
    """The calls the stand-in saw after the first ``count`` calls."""
    return host.log[count:]


def writes(calls: list[dict[str, Any]]) -> list[str]:
    """The operations of ``calls`` that change state."""
    return [entry["operation"] for entry in calls if entry["operation"] in WRITE_OPERATIONS]


def families_run(calls: list[dict[str, Any]]) -> list[str]:
    """The task definitions that RunTask calls of ``calls`` launched, in order."""
    return [entry["options"]["--task-definition"][0] for entry in calls if entry["operation"] == "run-task"]


def refresh_places_code_and_redeploys_every_named_service() -> dict[str, bool]:
    host, session, lines = running()
    count = len(host.log)
    session.refresh((CODE_RUN, CODE_SHA))
    calls = after(host, count)
    updates = [entry["options"]["--service"][0] for entry in calls if entry["operation"] == "update-service"]
    steps = {line.split(":", 1)[0] for line in lines}
    return {
        "no call changes the desired capacity": "set-desired-capacity" not in writes(calls) and host.desired == 1,
        "the one-shots run in start's order, the code install and its verification between host_prepare and the catalog bootstrap": families_run(calls)
        == [FAMILIES["host_prepare"], CODE_FETCH_FAMILY, CODE_FETCH_FAMILY, OPENMETADATA_FAMILY],
        "the host shows the requested code": host.checkout == CODE_SHA,
        "every service of the state is redeployed, postgres first": updates[0] == SERVICES["postgres"]
        and sorted(updates) == sorted([*SERVICES.values(), OPENMETADATA_SERVICE]),
        "the catalog service is set back to one task": any(
            entry["options"].get("--desired-count") == ["1"] for entry in calls if entry["operation"] == "update-service"
        ),
        "every service is stable with one task": all(service["running"] == 1 and service["state"] == "COMPLETED" for service in host.services.values()),
        "each step is named as start names it": {
            "services_state",
            "refresh_checked",
            "host_prepare",
            "code_verified",
            "services_stable",
            "openmetadata_bootstrap",
        }
        <= steps,
        "the state serial is printed": any(line.startswith(f"services_state: serial {SERIAL}") for line in lines),
        "the output names no ARN, account ID or instance ID": not any("arn:aws" in line or "111111111111" in line or "i-stand-in" in line for line in lines),
    }


def refresh_without_code_checks_the_existing_checkout() -> dict[str, bool]:
    host, session, lines = running(checkout=CODE_SHA)
    count = len(host.log)
    session.refresh()
    calls = after(host, count)
    return {
        "the code one-shot runs once as a check": families_run(calls).count(CODE_FETCH_FAMILY) == 1,
        "the checkout is unchanged": host.checkout == CODE_SHA,
        "every service is stable": all(service["state"] == "COMPLETED" for service in host.services.values()),
    }


def refresh_refuses_code_the_stack_cannot_place() -> dict[str, bool]:
    host, session, _ = running(code_fetch=False)
    count = len(host.log)
    message = failure(lambda: session.refresh((CODE_RUN, CODE_SHA)))
    return {
        "refresh names the missing code one-shot": message is not None and "code_fetch" in message,
        "nothing changes": not writes(after(host, count)) and host.desired == 1,
    }


def refresh_refuses_a_host_that_is_not_one_registered_instance() -> dict[str, bool]:
    never_started, session_a, _ = build()
    message_a = failure(session_a.refresh)
    registering, session_b, _ = build({"never_register"})
    registering.autoscaling_set_desired_capacity({"--desired-capacity": ["1"]})
    message_b = failure(session_b.refresh)
    elsewhere, session_c, _ = build({"other_provider"})
    elsewhere.autoscaling_set_desired_capacity({"--desired-capacity": ["1"]})
    elsewhere.advance(200)
    message_c = failure(session_c.refresh)
    return {
        "a host that is off is refused and start is named": message_a is not None and "start" in message_a,
        "an instance that has not registered is refused": message_b is not None and "start" in message_b,
        "an instance outside the capacity provider is refused": message_c is not None and "capacity provider" in message_c,
        "no refusal changes anything": not writes(never_started.log) and not writes(registering.log) and not writes(elsewhere.log),
        "the desired capacity is left as it was": never_started.desired == 0 and registering.desired == 1 and elsewhere.desired == 1,
    }


def refresh_refuses_while_a_job_task_runs() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, family in (("a reader", f"{JOB_PREFIX}read_silver"), ("a writer", f"{JOB_PREFIX}write_gold")):
        host, session, lines = running()
        arn = host.add_task(family)
        count = len(host.log)
        message = failure(partial(session.refresh, (CODE_RUN, CODE_SHA)))
        checks |= {
            f"{label} task refuses the refresh and is named by family and ID suffix": message is not None and family in message and arn[-8:] in message,
            f"{label} task: no task, deployment or capacity change follows": not writes(after(host, count)) and host.desired == 1,
            f"{label} task: the output names no ARN": not any("arn:aws" in line for line in lines) and "arn:aws" not in (message or ""),
        }
    other, session, _ = running()
    other.add_task("other_dev_job_read_silver")
    other.add_task(SERVICES["polaris"], group=f"service:{SERVICES['polaris']}")
    session.refresh()
    checks["service tasks and the tasks of another project do not refuse it"] = all(service["state"] == "COMPLETED" for service in other.services.values())
    return checks


def refresh_failure_leaves_the_host_on() -> dict[str, bool]:
    faults = {
        "host_prepare exits 2": "exit_code:host_prepare=2",
        "the code task exits 1": "exit_code:code_fetch=1",
        "the services never become stable": "never_stable",
    }
    checks: dict[str, bool] = {}
    for label, fault in faults.items():
        host, session, lines = running()
        host.faults.add(fault)
        count = len(host.log)
        message = failure(partial(session.refresh, (CODE_RUN, CODE_SHA)))
        checks |= {
            f"{label}: refresh fails": message is not None,
            f"{label}: the host stays on and no capacity call is made": host.desired == 1
            and host.instance_up_at is not None
            and "set-desired-capacity" not in writes(after(host, count)),
            f"{label}: the failure is reported as a refresh failure that leaves the host on": any(
                line.startswith("refresh_failed") and "stays on" in line for line in lines
            )
            and not any(line.startswith("host_off") for line in lines),
        }
    host, session, lines = running({"exit_code:openmetadata_bootstrap=1"})
    session.refresh()
    checks["a failed catalog bootstrap is reported and does not fail the refresh"] = any(line.startswith("openmetadata_bootstrap_failed") for line in lines)
    return checks


def refresh_refuses_a_service_that_ecs_does_not_have() -> dict[str, bool]:
    host, session, lines = running()
    session.names = replace(session.names, services={**session.names.services, "streamlit": NEW_SERVICE})
    count = len(host.log)
    message = failure(lambda: session.refresh((CODE_RUN, CODE_SHA)))
    return {
        "refresh names the absent service and the state serial": message is not None and NEW_SERVICE in message and str(SERIAL) in message,
        "nothing is launched or deployed": not writes(after(host, count)),
        "the serial is printed before the refusal": any(line.startswith(f"services_state: serial {SERIAL}") for line in lines),
    }


def status_reports_an_absent_service_by_name() -> dict[str, bool]:
    host, session, _ = running()
    session.names = replace(session.names, services={**session.names.services, "streamlit": NEW_SERVICE})
    report = session.status()
    erroring, failing, _ = running()
    erroring.faults.add("describe_services_error")
    other = failure(failing.status)
    stable = failure(session.services_stable)
    return {
        "status answers instead of raising": isinstance(report, dict),
        "the absent service is named": report.get("absent_services") == [NEW_SERVICE],
        "the services ECS has are still counted": sorted(report["services"]) == sorted([*SERVICES.values(), OPENMETADATA_SERVICE]),
        "the state serial is part of the answer": report.get("services_state_serial") == SERIAL,
        "a describe-services failure of another kind still fails": other is not None
        and "describe-services reported failures" in other
        and erroring.desired == 1,
        "the stability wait still refuses an absent service": stable is not None and NEW_SERVICE in stable,
    }


def status_without_a_missing_service_names_none() -> dict[str, bool]:
    host, session, _ = running()
    report = session.status()
    return {"no service is reported absent": report.get("absent_services") == []}


def the_services_state_serial_is_read_from_the_state_file() -> dict[str, bool]:
    host_session = module()
    platform = {
        "cluster_name": {"value": CLUSTER},
        "host_autoscaling_group_name": {"value": GROUP},
        "ec2_capacity_provider_name": {"value": PROVIDER},
        "project_tag": {"value": PROJECT},
    }
    services = {
        "service_names": {"value": {"postgres": "q"}},
        "one_shot_run_task": {"value": {key: {"task_definition": family} for key, family in FAMILIES.items()}},
    }
    directory = Path(tempfile.mkdtemp(prefix="host_session_state_"))
    state = directory / "terraform.tfstate"

    def serial() -> Any:
        return host_session.services_state_serial(directory)

    state.write_text(json.dumps({"version": 4, "serial": SERIAL, "lineage": "x"}), encoding="utf-8")
    present = serial()
    state.write_text(json.dumps({"serial": True}), encoding="utf-8")
    boolean = serial()
    state.write_text("not json", encoding="utf-8")
    broken = serial()
    state.unlink()
    missing = serial()
    state.write_text(json.dumps({"serial": SERIAL}), encoding="utf-8")
    with (
        mock.patch.object(host_session, "SERVICES_DIRECTORY", directory),
        mock.patch.object(host_session, "terraform_document", lambda folder, *_, **__: platform if folder == host_session.PLATFORM_DIRECTORY else services),
    ):
        loaded = host_session.load_names({})
    return {
        "the serial of the state file is read": present == SERIAL,
        "a serial that is not a number is unknown": boolean is None,
        "an unreadable state file is unknown, not an error": broken is None,
        "an absent state file is unknown, not an error": missing is None,
        "the names carry the serial of the services directory": loaded.state_serial == SERIAL,
    }


def refresh_command_line() -> dict[str, bool]:
    host_session = module()
    called: list[dict[str, Any]] = []
    outcome: dict[str, Any] = {"raise": None}

    class Recorder:
        def refresh(self, code: tuple[str, str] | None = None) -> None:
            called.append({"code": code})
            if outcome["raise"] is not None:
                raise outcome["raise"]

    def exit_code(argv: list[str]) -> int:
        try:
            return int(host_session.main(argv))
        except SystemExit as error:
            return int(error.code or 0)

    refused = {
        "a run ID without a digest": ["refresh", "--code-run", CODE_RUN],
        "a digest without a run ID": ["refresh", "--code-sha256", CODE_SHA],
        "a malformed run ID": ["refresh", "--code-run", "latest", "--code-sha256", CODE_SHA],
        "a malformed digest": ["refresh", "--code-run", CODE_RUN, "--code-sha256", "A" * 64],
        "the keep-on option, which refresh does not have": ["refresh", "--keep-on"],
    }
    with mock.patch.object(host_session, "open_session", Recorder), mock.patch.object(host_session, "install_termination_handler", lambda: lambda: None):
        codes = {name: exit_code(argv) for name, argv in refused.items()}
        refused_calls = len(called)
        plain = exit_code(["refresh"])
        pair = exit_code(["refresh", "--code-run", CODE_RUN, "--code-sha256", CODE_SHA])
        outcome["raise"] = host_session.HostSessionError("refresh refused: a job task is running")
        failed = exit_code(["refresh"])
    settings = {"aws_profile": PROFILE, "aws_region": REGION, "project_name": "stand_in", "environment": "dev"}
    names = Names(CLUSTER, GROUP, PROVIDER, PROJECT, {"postgres": "q"}, {}, None)
    with (
        mock.patch.object(host_session, "load_configuration", lambda *_: (settings, None)),
        mock.patch.object(host_session, "load_names", lambda *_, **__: names),
    ):
        opened = host_session.open_session()
    return {
        **{f"{name} is a usage error": code == 2 for name, code in codes.items()},
        "a refused option opens no session": refused_calls == 0,
        "refresh without code reaches the session with no code": plain == 0 and called[0] == {"code": None},
        "a well-formed pair reaches refresh as the code to place": pair == 0 and called[1] == {"code": (CODE_RUN, CODE_SHA)},
        "a refused refresh exits 1": failed == 1,
        "the session knows the job family prefix of the project and environment": opened.job_prefix == "stand_in_dev_job_",
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "refresh_places_code_and_redeploys_every_named_service": refresh_places_code_and_redeploys_every_named_service,
    "refresh_without_code_checks_the_existing_checkout": refresh_without_code_checks_the_existing_checkout,
    "refresh_refuses_code_the_stack_cannot_place": refresh_refuses_code_the_stack_cannot_place,
    "refresh_refuses_a_host_that_is_not_one_registered_instance": refresh_refuses_a_host_that_is_not_one_registered_instance,
    "refresh_refuses_while_a_job_task_runs": refresh_refuses_while_a_job_task_runs,
    "refresh_failure_leaves_the_host_on": refresh_failure_leaves_the_host_on,
    "refresh_refuses_a_service_that_ecs_does_not_have": refresh_refuses_a_service_that_ecs_does_not_have,
    "status_reports_an_absent_service_by_name": status_reports_an_absent_service_by_name,
    "status_without_a_missing_service_names_none": status_without_a_missing_service_names_none,
    "the_services_state_serial_is_read_from_the_state_file": the_services_state_serial_is_read_from_the_state_file,
    "refresh_command_line": refresh_command_line,
}
