"""End-to-end cases for ``host_session`` against the in-process ECS and Auto Scaling stand-in; each case returns its named checks.

Every case builds a new stand-in host, runs the real ``Session`` through ``AwsCli`` with the stand-in as its runner and a virtual clock, and checks
the calls the stand-in saw and the state it ended in.
"""

from __future__ import annotations

import importlib
import json
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure import host_session_group_signal
from scripts.infrastructure.host_session import REGISTRATION_SECONDS, TERMINATE_SECONDS, Clock, HostSessionError, Names, Session
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

REPO_ROOT = Path(__file__).resolve().parents[2]
ARN_OR_ACCOUNT = re.compile(r"arn:aws|\b\d{12}\b|\bi-[0-9a-f]{8,}\b")
CASE_ERRORS = (AssertionError, LookupError, StopIteration, TypeError, ValueError, AttributeError, NameError, HostSessionError)


def session_module() -> Any:
    """The ``host_session`` module as it is loaded now (a plant replaces it in ``sys.modules``)."""
    return importlib.import_module("scripts.infrastructure.host_session")


def names(openmetadata: bool = False, code_fetch: bool = False) -> Names:
    """The names the stand-in knows; ``openmetadata`` adds the persistent catalog service and its bootstrap task, ``code_fetch`` the code one-shot."""
    services = dict(SERVICES, **({"openmetadata": OPENMETADATA_SERVICE} if openmetadata else {}))
    families = dict(
        FAMILIES, **({"openmetadata_bootstrap": OPENMETADATA_FAMILY} if openmetadata else {}), **({"code_fetch": CODE_FETCH_FAMILY} if code_fetch else {})
    )
    return Names(CLUSTER, GROUP, PROVIDER, PROJECT, dict(sorted(services.items(), key=lambda item: item[0] != "postgres")), families)


def build(
    faults: set[str] | None = None, marker: Path | None = None, openmetadata: bool = False, code_fetch: bool = False, checkout: str | None = None
) -> tuple[StandInHost, Session, list[str]]:
    """A stand-in host and a session wired to it; the third value collects the report lines; ``checkout`` is the code digest the host shows."""
    host, lines = StandInHost(faults, openmetadata=openmetadata, code_fetch=code_fetch, checkout=checkout), list[str]()
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION}, host.runner)
    marker = marker or Path(tempfile.mkdtemp(prefix="host_session_case_")) / "polaris_bootstrap.launched"
    return host, Session(cli, names(openmetadata, code_fetch), Clock(lambda: host.now, host.advance), marker, lines.append), lines


# The code one-shot's own limit (host_session.CODE_FETCH_SECONDS).
CODE_FETCH_LIMIT = 1200


def failure(action: Callable[[], object]) -> str | None:
    """The message of the HostSessionError the action raised, or None."""
    try:
        action()
    except HostSessionError as error:
        return str(error)
    return None


def writes(host: StandInHost) -> list[str]:
    """The operations that change state."""
    return [entry["operation"] for entry in host.log if entry["operation"] in {"set-desired-capacity", "run-task", "update-service"}]


def happy_start() -> dict[str, bool]:
    host, session, lines = build()
    session.start()
    updates = [entry["options"]["--service"][0] for entry in host.log if entry["operation"] == "update-service"]
    run = next(entry["options"] for entry in host.log if entry["operation"] == "run-task")
    order = [entry["operation"] for entry in host.log if entry["operation"] in {"set-desired-capacity", "run-task", "update-service"}]
    return {
        "every service runs one task": all(service["running"] == 1 and service["state"] == "COMPLETED" for service in host.services.values()),
        "postgres is updated first": updates[0] == SERVICES["postgres"] and sorted(updates) == sorted(SERVICES.values()),
        "host_prepare uses the Project tag, propagation and managed tags": run.get("--tags") == [f"key=Project,value={PROJECT}"]
        and run.get("--propagate-tags") == ["TASK_DEFINITION"]
        and "--enable-ecs-managed-tags" in run,
        "order is capacity, host_prepare, then deployments": order[0] == "set-desired-capacity"
        and order[1] == "run-task"
        and set(order[2:]) == {"update-service"},
        "every update forces a new deployment": all(
            "--force-new-deployment" in entry["options"] for entry in host.log if entry["operation"] == "update-service"
        ),
        "host stays on": host.desired == 1,
        "output names no ARN or account ID": not any(ARN_OR_ACCOUNT.search(line) for line in lines),
    }


def registration_timeout() -> dict[str, bool]:
    host, session, _ = build({"never_register"})
    message = failure(session.start)
    return {
        "start fails with the registration deadline": message is not None and f"{REGISTRATION_SECONDS} s" in message,
        "no task and no deployment started": "run-task" not in writes(host) and "update-service" not in writes(host),
        "host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def provider_membership_lag() -> dict[str, bool]:
    host, session, lines = build({"late_provider:200"})
    session.start()
    runs = [entry for entry in host.log if entry["operation"] == "run-task"]
    joined_at = (host.instance_up_at or 0) + host.provider_delay
    waited = [entry for entry in host.log if entry["operation"] == "describe-container-instances" and entry["at"] < joined_at]
    return {
        "start waits for the instance to join the capacity provider": host.desired == 1
        and all(service["state"] == "COMPLETED" for service in host.services.values()),
        "RunTask is called once, and only after the instance joined": len(runs) == 1 and runs[0]["at"] >= joined_at,
        "the wait polled the instance before it joined": len(waited) > 1,
        "host_registered is reported after the membership": any(line.startswith("host_registered") for line in lines),
    }


def provider_membership_never() -> dict[str, bool]:
    host, session, lines = build({"never_provider"})
    message = failure(session.start)
    return {
        "start fails at the registration deadline": message is not None and f"{REGISTRATION_SECONDS} s" in message,
        "the failure names the missing capacity provider membership": message is not None and "capacity provider" in message,
        "the failure names no provider, ARN or account ID": message is not None and PROVIDER not in message and not ARN_OR_ACCOUNT.search(message),
        "no host_registered line, no task and no deployment": not any(line.startswith("host_registered") for line in lines)
        and "run-task" not in writes(host)
        and "update-service" not in writes(host),
        "host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def provider_membership_other() -> dict[str, bool]:
    host, session, lines = build({"other_provider"})
    message = failure(session.start)
    return {
        "start fails at the registration deadline": message is not None and f"{REGISTRATION_SECONDS} s" in message,
        "the failure names the missing capacity provider membership": message is not None and "capacity provider" in message,
        "no host_registered line, no task and no deployment": not any(line.startswith("host_registered") for line in lines)
        and "run-task" not in writes(host)
        and "update-service" not in writes(host),
        "host is off and verified": host.desired == 0 and host.instance_up_at is None,
    }


def run_task_failures() -> dict[str, bool]:
    host, session, _ = build({"run_task_failure"})
    message = failure(session.start)
    return {
        "start fails naming the RunTask failure": message is not None and "RESOURCE:MEMORY" in message,
        "no service update": "update-service" not in writes(host),
        "host is off": host.desired == 0 and host.instance_up_at is None,
    }


def host_prepare_nonzero() -> dict[str, bool]:
    host, session, _ = build({"exit_code:host_prepare=2"})
    message = failure(session.start)
    return {
        "start fails naming the exit code": message is not None and "host_prepare" in message and "2" in message,
        "no service update": "update-service" not in writes(host),
        "host is off": host.desired == 0 and host.instance_up_at is None,
    }


def service_never_stable() -> dict[str, bool]:
    host, session, _ = build({"never_stable"})
    message = failure(session.start)
    pending_host, pending_session, _ = build({"pending_stuck"})
    pending = failure(pending_session.start)
    kept_host, kept_session, kept_lines = build({"never_stable"})
    failure(lambda: kept_session.start(keep_on=True))
    return {
        "start fails at the deadline": message is not None and "services stable" in message,
        "host is off after the failure": host.desired == 0 and host.instance_up_at is None,
        "a pending task keeps a service from counting as stable": pending is not None and pending_host.desired == 0,
        "keep-on leaves the host on": kept_host.desired == 1 and any("host_kept_on" in line for line in kept_lines),
    }


def circuit_breaker() -> dict[str, bool]:
    host, session, _ = build()
    host.prepared = False
    host.advance(0)
    session.host_on()
    for name in SERVICES.values():
        session.call("ecs", "update-service", ["--cluster", CLUSTER, "--service", name, "--force-new-deployment"])
    start = host.now
    message = failure(lambda: session.wait("services stable", session.services_stable, 1200, 15))
    return {
        "a FAILED rollout fails at once with its reason": message is not None and "rollout FAILED" in message and "circuit breaker" in message,
        "no wait to the deadline": host.now - start < 300,
    }


def partial_stop() -> dict[str, bool]:
    host, session, _ = build({"never_terminate"})
    session.host_on()
    message = failure(session.host_off)
    return {
        "stop fails at the deadline": message is not None and f"{TERMINATE_SECONDS} s" in message,
        "the failure says the instance is still there": host.instance_up_at is not None,
    }


def repeated_start() -> dict[str, bool]:
    host, session, _ = build()
    session.host_on()
    session.call(
        "ecs",
        "run-task",
        ["--cluster", CLUSTER, "--task-definition", FAMILIES["host_prepare"], "--capacity-provider-strategy", f"capacityProvider={PROVIDER},weight=1"],
    )
    session.one_shot("host_prepare", 600)
    runs = [entry for entry in host.log if entry["operation"] == "run-task"]
    return {"a running task of the family is awaited, not doubled": len(runs) == 1 and host.prepared}


def host_already_on() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    session.start()
    return {"second start succeeds with the host on": host.desired == 1 and all(service["state"] == "COMPLETED" for service in host.services.values())}


def throttled_poll() -> dict[str, bool]:
    host, session, _ = build({"throttle:2"})
    session.start()
    persistent_host, persistent, _ = build({"api_error"})
    message = failure(persistent.start)
    return {
        "two throttled polls are tolerated": all(service["state"] == "COMPLETED" for service in host.services.values()),
        "a persistent API error fails visibly with the code": message is not None and "ThrottlingException" in message,
        "host is off after the persistent error": persistent_host.desired == 0 or persistent_host.instance_up_at is not None,
    }


def interrupted_start() -> dict[str, bool]:
    host, session, _ = build({"interrupt_run_task"})
    interrupted = False
    try:
        session.start()
    except KeyboardInterrupt:
        interrupted = True
    return {"the interruption propagates": interrupted, "host off was requested": host.desired == 0}


def bootstrap_guards() -> dict[str, bool]:
    host, session, _ = build()
    before = failure(session.bootstrap)
    session.start()
    session.bootstrap()
    second = failure(session.bootstrap)
    runs_after_second = host.bootstrap_runs
    repeated = failure(lambda: session.bootstrap(again=True))
    return {
        "bootstrap before services are stable is refused": before is not None and "not both stable" in before and host.bootstrap_runs >= 0,
        "the first bootstrap ran once": runs_after_second == 1,
        "a second bootstrap is refused": second is not None and "already ran" in second,
        "the flag runs it and the realm-exists exit is reported": repeated is not None and "polaris_bootstrap" in repeated and host.bootstrap_runs == 2,
    }


def lost_run_task_reply() -> dict[str, bool]:
    host, session, _ = build({"lose_run_task_reply", "stale_list_tasks"})
    message = failure(session.start)
    first = [entry["options"].get("--client-token") for entry in host.log if entry["operation"] == "run-task"]
    session.start()
    tokens = [entry["options"].get("--client-token", [None])[0] for entry in host.log if entry["operation"] == "run-task"]
    lost_host, lost_session, _ = build({"lose_run_task_reply:99", "stale_list_tasks"})
    unconfirmed = failure(lost_session.start)
    return {
        "a lost reply does not fail the start": message is None and host.prepared,
        "the retry reuses the client token": len(first) == 2 and first[0] == first[1] and first[0] is not None,
        "the accepted launch is not doubled": len([task for task in host.tasks.values() if task["key"] == "host_prepare"]) == 2 and len(set(tokens)) == 2,
        "a confirmed launch gives the next launch a new token": len(tokens) == 3 and tokens[0] == tokens[1] and tokens[0] != tokens[2],
        "repeated lost replies fail as unconfirmed with the host off": unconfirmed is not None
        and "unconfirmed" in unconfirmed
        and lost_host.desired == 0
        and len(lost_host.tasks) == 1,
    }


def late_success() -> dict[str, bool]:
    host, session, _ = build({"slow_calls:24"})
    session.host_on()
    start, before = host.now, len(host.log)
    message = failure(lambda: session.wait("registration", lambda: len(session.active_instances()) == 1, 10, 15))
    waited = [entry["timeout"] for entry in host.log[before:] if entry["operation"] in {"list-container-instances", "describe-container-instances"}]
    elapsed = host.now - start

    def slow_check() -> bool:
        host.advance(24)
        return True

    slow = failure(lambda: session.wait("slow check", slow_check, 10, 15))
    quiet = host.now
    failure(lambda: session.wait("nothing", lambda: False, 20, 15))
    slept = host.now - quiet
    return {
        "a success after the deadline is rejected": message is not None and "not reached within 10 s" in message,
        "a check that succeeds after the deadline is rejected": slow is not None and "not reached within 10 s" in slow,
        "every call gets the remaining budget": bool(waited) and all(limit is not None and limit <= 10 for limit in waited),
        "the wait ends near its deadline": elapsed <= 10,
        "a sleep never runs past the deadline": slept <= 20,
    }


def bootstrap_failure_stops_host() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    host.faults.add("exit_code:polaris_bootstrap=1")
    message = failure(session.bootstrap)
    kept_host, kept, kept_lines = build()
    kept.start()
    kept_host.faults.add("exit_code:polaris_bootstrap=1")
    failure(lambda: kept.bootstrap(keep_on=True))
    interrupted_host, interrupted, _ = build()
    interrupted.start()
    interrupted_host.faults.add("interrupt_run_task")
    stopped = False
    try:
        interrupted.bootstrap()
    except KeyboardInterrupt:
        stopped = True
    refused_host, refused, _ = build()
    before = failure(refused.bootstrap)
    return {
        "a failed bootstrap fails with the exit code": message is not None and "polaris_bootstrap" in message,
        "a failed bootstrap stops and verifies the host": host.desired == 0 and host.instance_up_at is None,
        "keep-on leaves the host on after a failed bootstrap": kept_host.desired == 1 and any("host_kept_on" in line for line in kept_lines),
        "an interrupted bootstrap stops the host": stopped and interrupted_host.desired == 0 and interrupted_host.instance_up_at is None,
        "a refused bootstrap changes nothing": before is not None and refused_host.desired == 0 and not writes(refused_host),
    }


def terminated_session() -> dict[str, bool]:
    module = session_module()
    restore = module.install_termination_handler()
    try:
        host, session, _ = build({"signal_run_task"})
        stopped = False
        try:
            session.start()
        except module.TerminationRequested:
            stopped = True
        boot_host, boot, _ = build()
        boot.start()
        boot_host.faults.add("signal_run_task")
        boot_stopped = False
        try:
            boot.bootstrap()
        except module.TerminationRequested:
            boot_stopped = True
    finally:
        restore()
    return {
        "SIGTERM ends a start with the termination error": stopped,
        "SIGTERM during a start stops and verifies the host": host.desired == 0 and host.instance_up_at is None,
        "SIGTERM during a bootstrap stops and verifies the host": boot_stopped and boot_host.desired == 0 and boot_host.instance_up_at is None,
    }


def second_signal_during_cleanup() -> dict[str, bool]:
    module = session_module()
    restore = module.install_termination_handler()
    checks = {}
    try:
        for name, first in (("SIGINT", "interrupt_run_task"), ("SIGTERM", "signal_run_task"), ("SIGHUP", "signal_run_task")):
            host, session, lines = build({first, f"second_signal:{name}"})
            raised = None
            try:
                session.start()
            except (KeyboardInterrupt, module.TerminationRequested) as error:
                raised = type(error).__name__
            checks[f"the first stop reason propagates past a {name} during cleanup"] = raised == (
                "KeyboardInterrupt" if first == "interrupt_run_task" else "TerminationRequested"
            )
            checks[f"a {name} during cleanup still stops and verifies the host"] = host.desired == 0 and host.instance_up_at is None
            checks[f"a {name} during cleanup is reported as deferred"] = any("signal_deferred" in line and name in line for line in lines)
    finally:
        restore()
    return checks


def failure_output_privacy() -> dict[str, bool]:
    exit_host, exit_session, exit_lines = build({"leaky_reasons", "exit_code:host_prepare=3"})
    exit_message = failure(exit_session.start)
    rollout_host, rollout_session, rollout_lines = build({"leaky_reasons"})
    rollout_host.prepared = False
    rollout_host.advance(0)
    rollout_session.host_on()
    for name in SERVICES.values():
        rollout_session.call("ecs", "update-service", ["--cluster", CLUSTER, "--service", name, "--force-new-deployment"])
    rollout_message = failure(lambda: rollout_session.wait("services stable", rollout_session.services_stable, 1200, 15))
    run_host, run_session, run_lines = build({"leaky_reasons", "run_task_failure"})
    run_message = failure(run_session.start)
    shown = [*exit_lines, *rollout_lines, *run_lines, exit_message or "", rollout_message or "", run_message or ""]
    return {
        "a stopped reason reaches the output as a category": exit_message is not None and "essential container exited" in exit_message,
        "a rollout reason reaches the output as a category": rollout_message is not None and "circuit breaker" in rollout_message,
        "a RunTask reason keeps its fixed code": run_message is not None and "RESOURCE:MEMORY" in run_message,
        "failure output names no ARN, account ID or instance ID": not any(ARN_OR_ACCOUNT.search(line) for line in shown) and bool(shown),
        "the hosts are off after the start failures": all(item.desired == 0 for item in (exit_host, run_host)),
    }


def load_names_order() -> dict[str, bool]:
    module: Any = importlib.import_module("scripts.infrastructure.host_session")
    platform = {
        "cluster_name": {"value": CLUSTER},
        "host_autoscaling_group_name": {"value": GROUP},
        "ec2_capacity_provider_name": {"value": PROVIDER},
        "project_tag": {"value": PROJECT},
    }
    services = {
        "service_names": {"value": {"airflow_scheduler": "a", "polaris": "p", "postgres": "q"}},
        "one_shot_run_task": {"value": {key: {"task_definition": family} for key, family in FAMILIES.items()}},
    }
    original = module.terraform_document
    module.terraform_document = lambda directory, *_: platform if directory == module.PLATFORM_DIRECTORY else services
    try:
        loaded = module.load_names({})
        broken = dict(services, service_names={"value": {"polaris": "p"}})
        module.terraform_document = lambda directory, *_: platform if directory == module.PLATFORM_DIRECTORY else broken
        try:
            module.load_names({})
            rejected = False
        except module.ConfigurationError:
            rejected = True
    finally:
        module.terraform_document = original
    return {
        "postgres is first": next(iter(loaded.services)) == "postgres",
        "names come from the outputs": loaded.cluster == CLUSTER and loaded.group == GROUP and loaded.one_shot == FAMILIES,
        "a missing postgres service is rejected": rejected,
    }


def status_read_only() -> dict[str, bool]:
    host, session, lines = build()
    session.start()
    count = len(host.log)
    report = session.status()
    new = [entry["operation"] for entry in host.log[count:]]
    return {
        "status reports counts": report["desired_capacity"] == 1 and report["active_container_instances"] == 1 and len(report["services"]) == len(SERVICES),
        "status calls no write": not set(new) & {"set-desired-capacity", "run-task", "update-service"},
    }


def openmetadata_start() -> dict[str, bool]:
    host, session, lines = build(openmetadata=True)
    session.start()
    log = [entry for entry in host.log if entry["operation"] in {"set-desired-capacity", "run-task", "update-service"}]
    updates = {entry["options"]["--service"][0]: entry["options"] for entry in log if entry["operation"] == "update-service"}
    runs = [entry for entry in log if entry["operation"] == "run-task"]
    last_update = max(index for index, entry in enumerate(log) if entry["operation"] == "update-service")
    bootstrap_run = next(
        index for index, entry in enumerate(log) if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [OPENMETADATA_FAMILY]
    )
    return {
        "the catalog service runs one task": host.services[OPENMETADATA_SERVICE]["running"] == 1
        and host.services[OPENMETADATA_SERVICE]["state"] == "COMPLETED",
        "its deployment also sets the desired count to one": updates[OPENMETADATA_SERVICE].get("--desired-count") == ["1"],
        "no other service gets a desired count": all("--desired-count" not in options for name, options in updates.items() if name != OPENMETADATA_SERVICE),
        "the bootstrap runs once, after every deployment": len(runs) == 2 and bootstrap_run > last_update,
        "the bootstrap used the Project tag and propagation": log[bootstrap_run]["options"].get("--tags") == [f"key=Project,value={PROJECT}"],
        "the bootstrap finished with exit code zero": any(line.startswith("openmetadata_bootstrap: exit codes") for line in lines),
        "output names no ARN or account ID": not any(ARN_OR_ACCOUNT.search(line) for line in lines),
    }


def openmetadata_bootstrap_failure_keeps_the_chain_running() -> dict[str, bool]:
    host, session, lines = build({"exit_code:openmetadata_bootstrap=1"}, openmetadata=True)
    session.start()
    return {
        "the host stays on": host.desired == 1,
        "every service stays stable": all(service["state"] == "COMPLETED" and service["running"] == 1 for service in host.services.values()),
        "the failure is reported by name": any(line.startswith("openmetadata_bootstrap_failed") for line in lines),
        "the report says the data chain is not affected": any("data chain is not affected" in line for line in lines),
    }


def openmetadata_bootstrap_absent_without_the_flag() -> dict[str, bool]:
    host, session, lines = build()
    session.start()
    return {
        "only host_prepare runs": [entry["options"]["--task-definition"][0] for entry in host.log if entry["operation"] == "run-task"]
        == [FAMILIES["host_prepare"]],
        "no desired count is sent": all("--desired-count" not in entry["options"] for entry in host.log if entry["operation"] == "update-service"),
        "no catalog line is reported": not any("openmetadata" in line for line in lines),
    }


def openmetadata_stops_before_the_host() -> dict[str, bool]:
    host, session, lines = build(openmetadata=True)
    session.start()
    count = len(host.log)
    session.stop()
    new = [entry for entry in host.log[count:] if entry["operation"] in {"set-desired-capacity", "update-service"}]
    return {
        "the catalog service is scaled to zero first": new[0]["operation"] == "update-service"
        and new[0]["options"]["--service"] == [OPENMETADATA_SERVICE]
        and new[0]["options"]["--desired-count"] == ["0"],
        "then the host is turned off": new[1]["operation"] == "set-desired-capacity" and new[1]["options"]["--desired-capacity"] == ["0"],
        "nothing else is touched": len(new) == 2,
        "the catalog task was gone before the host went": host.services[OPENMETADATA_SERVICE]["running"] == 0,
        "the host is off and verified": host.desired == 0 and host.instance_up_at is None,
        "both steps are reported": any(line.startswith("openmetadata_stopped") for line in lines)
        and any(line.startswith("host_off_verified") for line in lines),
    }


def openmetadata_that_will_not_stop_does_not_hold_the_host() -> dict[str, bool]:
    host, session, lines = build({"never_stop_openmetadata"}, openmetadata=True)
    session.start()
    session.stop()
    return {
        "the host is still turned off and verified": host.desired == 0 and host.instance_up_at is None,
        "the stop failure is reported": any(line.startswith("openmetadata_stop_failed") for line in lines),
        "the report says the host is stopped anyway": any("stopping the host anyway" in line for line in lines),
    }


def openmetadata_restart_after_a_stop() -> dict[str, bool]:
    host, session, _ = build(openmetadata=True)
    session.start()
    session.stop()
    session.start()
    return {
        "the catalog service runs again after the next start": host.services[OPENMETADATA_SERVICE]["running"] == 1
        and host.services[OPENMETADATA_SERVICE]["desired"] == 1,
        "the bootstrap ran again and is repeatable": sum(
            1 for entry in host.log if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [OPENMETADATA_FAMILY]
        )
        == 2,
    }


def stop_without_the_catalog_only_turns_the_host_off() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    count = len(host.log)
    session.stop()
    new = [entry["operation"] for entry in host.log[count:] if entry["operation"] in {"set-desired-capacity", "update-service", "run-task"}]
    return {"stop only sets the capacity to zero": new == ["set-desired-capacity"], "the host is off": host.desired == 0 and host.instance_up_at is None}


def interrupted_catalog_stop() -> dict[str, bool]:
    module = session_module()
    restore = module.install_termination_handler()
    checks = {}
    try:
        for name, fault, expected in (("SIGINT", "interrupt_catalog_stop", "KeyboardInterrupt"), ("SIGTERM", "signal_catalog_stop", "TerminationRequested")):
            host, session, lines = build(openmetadata=True)
            session.start()
            host.faults.add(fault)
            raised = None
            try:
                session.stop()
            except (KeyboardInterrupt, module.TerminationRequested) as error:
                raised = type(error).__name__
            checks[f"a {name} during the catalog drain propagates"] = raised == expected
            checks[f"a {name} during the catalog drain still turns the host off and verifies it"] = host.desired == 0 and host.instance_up_at is None
        second_host, second, second_lines = build(openmetadata=True)
        second.start()
        second_host.faults.add("second_signal:SIGINT")
        try:
            second.stop()
        except KeyboardInterrupt:
            second_lines.append("stop cut short by SIGINT")
        checks["a SIGINT while the host goes off is deferred and the stop completes"] = (
            second_host.desired == 0
            and second_host.instance_up_at is None
            and any("signal_deferred" in line for line in second_lines)
            and "stop cut short by SIGINT" not in second_lines
        )
    finally:
        restore()
    return checks


def terminal_interrupt_during_host_off() -> dict[str, bool]:
    return host_session_group_signal.run(session_module())


CODE_RUN = "20261108T120000Z-aaaaaaaa"
CODE_SHA = "a" * 64
OLD_SHA = "b" * 64


def runs_of(host: StandInHost, family: str) -> list[dict[str, list[str]]]:
    return [entry["options"] for entry in host.log if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [family]]


def code_environment(options: dict[str, list[str]]) -> dict[str, str] | None:
    """The environment that the RunTask overrides give the code container, or None without overrides."""
    if "--overrides" not in options:
        return None
    containers = json.loads(options["--overrides"][0])["containerOverrides"]
    return {item["name"]: item["value"] for container in containers if container["name"] == "code_fetch" for item in container["environment"]}


def code_start_passes_the_run_and_digest() -> dict[str, bool]:
    host, session, lines = build(code_fetch=True)
    session.start(code=(CODE_RUN, CODE_SHA))
    order = [
        entry["options"]["--task-definition"][0] if entry["operation"] == "run-task" else entry["operation"]
        for entry in host.log
        if entry["operation"] in {"run-task", "update-service"}
    ]
    runs = runs_of(host, CODE_FETCH_FAMILY)
    return {
        "the code one-shot runs once": len(runs) == 1,
        "its overrides carry exactly the run ID and the code digest": code_environment(runs[0]) == {"CODE_RUN_ID": CODE_RUN, "CODE_SHA256": CODE_SHA},
        "it runs after host_prepare and before every deployment": order[:2] == [FAMILIES["host_prepare"], CODE_FETCH_FAMILY]
        and set(order[2:]) == {"update-service"},
        "it uses the Project tag and propagation": runs[0].get("--tags") == [f"key=Project,value={PROJECT}"]
        and runs[0].get("--propagate-tags") == ["TASK_DEFINITION"],
        "the host shows the new code": host.checkout == CODE_SHA,
        "every service runs": all(service["running"] == 1 for service in host.services.values()),
        "output names no ARN or account ID": not any(ARN_OR_ACCOUNT.search(line) for line in lines),
    }


def code_start_without_code_keeps_the_checkout() -> dict[str, bool]:
    host, session, _ = build(code_fetch=True, checkout=OLD_SHA)
    session.start()
    runs = runs_of(host, CODE_FETCH_FAMILY)
    return {
        "the code one-shot still runs, to check the checkout": len(runs) == 1,
        "it has no overrides": code_environment(runs[0]) is None,
        "the host keeps its checkout": host.checkout == OLD_SHA,
        "every service runs": all(service["running"] == 1 for service in host.services.values()),
    }


def code_start_without_code_on_an_empty_host_is_refused() -> dict[str, bool]:
    host, session, _ = build(code_fetch=True)
    message = failure(session.start)
    return {
        "start fails": message is not None and "code_fetch" in message,
        "no service is deployed": not any(entry["operation"] == "update-service" for entry in host.log),
        "the host is turned off again": host.desired == 0,
    }


def code_fetch_failure_keeps_the_old_checkout() -> dict[str, bool]:
    host, session, _ = build({"exit_code:code_fetch=1"}, code_fetch=True, checkout=OLD_SHA)
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    return {
        "a refused fetch (hash mismatch or failed download) fails start": message is not None and "code_fetch" in message,
        "no service is deployed on a half-known checkout": not any(entry["operation"] == "update-service" for entry in host.log),
        "the host keeps the old checkout": host.checkout == OLD_SHA,
        "the host is turned off again": host.desired == 0,
    }


def code_start_repeated_for_the_same_code() -> dict[str, bool]:
    host, session, _ = build(code_fetch=True)
    session.start(code=(CODE_RUN, CODE_SHA))
    session.start(code=(CODE_RUN, CODE_SHA))
    runs = runs_of(host, CODE_FETCH_FAMILY)
    return {
        "both starts succeed and run the one-shot with the same overrides": len(runs) == 2 and code_environment(runs[0]) == code_environment(runs[1]),
        "each start uses a new client token": runs[0]["--client-token"] != runs[1]["--client-token"],
        "the host shows the same code": host.checkout == CODE_SHA,
    }


OTHER_RUN = "20261108T130000Z-bbbbbbbb"
OTHER_SHA = "c" * 64


def launch_code_fetch(host: StandInHost, session: Session, run: str, sha: str) -> str:
    """Start a code one-shot for ``run`` the way an earlier start did, outside ``place_code``, and answer its task ARN."""
    environment = [{"name": "CODE_RUN_ID", "value": run}, {"name": "CODE_SHA256", "value": sha}]
    launched = session.run_task("code_fetch", CODE_FETCH_FAMILY, {"containerOverrides": [{"name": "code_fetch", "environment": environment}]})
    return str(launched["tasks"][0]["taskArn"])


def code_fetch_running_for_other_code_is_awaited_before_the_requested_code_runs() -> dict[str, bool]:
    host, session, lines = build(code_fetch=True)
    session.host_on()
    running = launch_code_fetch(host, session, CODE_RUN, CODE_SHA)
    stops_at = host.tasks[running]["stops_at"]
    session.place_code((OTHER_RUN, OTHER_SHA))
    runs = runs_of(host, CODE_FETCH_FAMILY)
    launches = [entry["at"] for entry in host.log if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [CODE_FETCH_FAMILY]]
    return {
        "the requested code gets its own launch": len(runs) == 2 and code_environment(runs[1]) == {"CODE_RUN_ID": OTHER_RUN, "CODE_SHA256": OTHER_SHA},
        "the host shows the requested code, not the running one": host.checkout == OTHER_SHA,
        "the requested task starts only after the running one stopped": len(launches) == 2 and launches[1] >= stops_at,
        "the wait names why": any("another run or digest" in line for line in lines),
        "output names no ARN or account ID": not any(ARN_OR_ACCOUNT.search(line) for line in lines),
    }


def code_fetch_running_that_differs_in_one_value_is_not_reused() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for name, requested in (("run ID", (OTHER_RUN, CODE_SHA)), ("code digest", (CODE_RUN, OTHER_SHA))):
        host, session, _ = build(code_fetch=True)
        session.host_on()
        launch_code_fetch(host, session, CODE_RUN, CODE_SHA)
        session.place_code(requested)
        checks[f"a running task that differs only in the {name} is followed by the requested task"] = (
            len(runs_of(host, CODE_FETCH_FAMILY)) == 2 and host.checkout == requested[1]
        )
    return checks


def code_fetch_running_for_the_same_code_is_awaited_not_doubled() -> dict[str, bool]:
    host, session, lines = build(code_fetch=True)
    session.host_on()
    launch_code_fetch(host, session, CODE_RUN, CODE_SHA)
    session.place_code((CODE_RUN, CODE_SHA))
    return {
        "no second task starts": len(runs_of(host, CODE_FETCH_FAMILY)) == 1,
        "the host shows the code": host.checkout == CODE_SHA,
        "the wait is reported": any("already running; waiting for it" in line for line in lines),
    }


def code_fetch_running_for_other_code_that_never_stops_fails_visibly() -> dict[str, bool]:
    host, session, _ = build(code_fetch=True, checkout=OLD_SHA)
    session.host_on()
    host.add_task(CODE_FETCH_FAMILY)
    message = failure(lambda: session.place_code((OTHER_RUN, OTHER_SHA)))
    return {
        "the placement fails inside the one-shot's bounded wait": message is not None and "code_fetch" in message and "1200 s" in message,
        "no second task of the family starts": runs_of(host, CODE_FETCH_FAMILY) == [],
        "the host keeps its checkout": host.checkout == OLD_SHA,
    }


def code_fetch_wait_and_launch_share_one_deadline() -> dict[str, bool]:
    """The wait for a running fetch of other code, the reads around it and the requested launch share the one-shot's single limit: an earlier fetch
    that stops just before the limit, with slow ECS reads, leaves no time, so start refuses the launch instead of starting a task past the limit."""
    host, session, _ = build(code_fetch=True)
    session.host_on()
    running = launch_code_fetch(host, session, CODE_RUN, CODE_SHA)
    begun = host.now
    host.tasks[running]["stops_at"] = begun + CODE_FETCH_LIMIT - 10
    host.latency = 100
    message = failure(lambda: session.place_code((OTHER_RUN, OTHER_SHA)))
    late = [
        entry["at"] - begun
        for entry in host.log
        if entry["operation"] == "run-task" and entry["at"] > begun and entry["options"]["--task-definition"] == [CODE_FETCH_FAMILY]
    ]
    return {
        "the placement fails inside the one-shot's limit": message is not None and "code_fetch" in message,
        "no task of the family is launched after the limit": all(offset <= CODE_FETCH_LIMIT for offset in late),
        "nothing is launched once the limit is spent": late == [],
        "the whole placement ends within the limit plus one slow read": host.now - begun <= CODE_FETCH_LIMIT + host.latency,
    }


def code_given_without_the_one_shot_is_refused() -> dict[str, bool]:
    host, session, _ = build()
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    return {
        "start refuses code it cannot place": message is not None and "code_fetch" in message,
        "the host is never turned on": not any(entry["operation"] == "set-desired-capacity" for entry in host.log),
    }


def code_options_are_checked_before_any_call() -> dict[str, bool]:
    module = session_module()
    refused = {
        "a run ID without a digest": ["start", "--code-run", CODE_RUN],
        "a digest without a run ID": ["start", "--code-sha256", CODE_SHA],
        "a malformed run ID": ["start", "--code-run", "latest", "--code-sha256", CODE_SHA],
        "a malformed digest": ["start", "--code-run", CODE_RUN, "--code-sha256", "A" * 64],
    }
    started: list[dict[str, Any]] = []

    class Recorder:
        def start(self, keep_on: bool = False, code: tuple[str, str] | None = None) -> None:
            started.append({"keep_on": keep_on, "code": code})

    def exit_code(argv: list[str]) -> int:
        try:
            return module.main(argv)
        except SystemExit as error:
            return int(error.code or 0)

    original, restore = module.open_session, module.install_termination_handler
    module.open_session = Recorder
    module.install_termination_handler = lambda: lambda: None
    try:
        codes = {name: exit_code(argv) for name, argv in refused.items()}
        accepted = exit_code(["start", "--code-run", CODE_RUN, "--code-sha256", CODE_SHA])
    finally:
        module.open_session, module.install_termination_handler = original, restore
    return {
        **{f"{name} is a usage error": code == 2 for name, code in codes.items()},
        "a refused option opens no session": len(started) <= 1,
        "a well-formed pair reaches start as the code to place": accepted == 0 and started == [{"keep_on": False, "code": (CODE_RUN, CODE_SHA)}],
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "happy_start": happy_start,
    "registration_timeout": registration_timeout,
    "provider_membership_lag": provider_membership_lag,
    "provider_membership_never": provider_membership_never,
    "provider_membership_other": provider_membership_other,
    "run_task_failures": run_task_failures,
    "host_prepare_nonzero": host_prepare_nonzero,
    "service_never_stable": service_never_stable,
    "circuit_breaker": circuit_breaker,
    "partial_stop": partial_stop,
    "repeated_start": repeated_start,
    "host_already_on": host_already_on,
    "throttled_poll": throttled_poll,
    "interrupted_start": interrupted_start,
    "bootstrap_guards": bootstrap_guards,
    "lost_run_task_reply": lost_run_task_reply,
    "late_success": late_success,
    "bootstrap_failure_stops_host": bootstrap_failure_stops_host,
    "terminated_session": terminated_session,
    "second_signal_during_cleanup": second_signal_during_cleanup,
    "failure_output_privacy": failure_output_privacy,
    "load_names_order": load_names_order,
    "status_read_only": status_read_only,
    "openmetadata_start": openmetadata_start,
    "openmetadata_bootstrap_failure_keeps_the_chain_running": openmetadata_bootstrap_failure_keeps_the_chain_running,
    "openmetadata_bootstrap_absent_without_the_flag": openmetadata_bootstrap_absent_without_the_flag,
    "openmetadata_stops_before_the_host": openmetadata_stops_before_the_host,
    "openmetadata_that_will_not_stop_does_not_hold_the_host": openmetadata_that_will_not_stop_does_not_hold_the_host,
    "openmetadata_restart_after_a_stop": openmetadata_restart_after_a_stop,
    "stop_without_the_catalog_only_turns_the_host_off": stop_without_the_catalog_only_turns_the_host_off,
    "interrupted_catalog_stop": interrupted_catalog_stop,
    "terminal_interrupt_during_host_off": terminal_interrupt_during_host_off,
    "code_start_passes_the_run_and_digest": code_start_passes_the_run_and_digest,
    "code_start_without_code_keeps_the_checkout": code_start_without_code_keeps_the_checkout,
    "code_start_without_code_on_an_empty_host_is_refused": code_start_without_code_on_an_empty_host_is_refused,
    "code_fetch_failure_keeps_the_old_checkout": code_fetch_failure_keeps_the_old_checkout,
    "code_start_repeated_for_the_same_code": code_start_repeated_for_the_same_code,
    "code_fetch_running_for_other_code_is_awaited_before_the_requested_code_runs": code_fetch_running_for_other_code_is_awaited_before_the_requested_code_runs,
    "code_fetch_running_that_differs_in_one_value_is_not_reused": code_fetch_running_that_differs_in_one_value_is_not_reused,
    "code_fetch_running_for_the_same_code_is_awaited_not_doubled": code_fetch_running_for_the_same_code_is_awaited_not_doubled,
    "code_fetch_running_for_other_code_that_never_stops_fails_visibly": code_fetch_running_for_other_code_that_never_stops_fails_visibly,
    "code_fetch_wait_and_launch_share_one_deadline": code_fetch_wait_and_launch_share_one_deadline,
    "code_given_without_the_one_shot_is_refused": code_given_without_the_one_shot_is_refused,
    "code_options_are_checked_before_any_call": code_options_are_checked_before_any_call,
}
