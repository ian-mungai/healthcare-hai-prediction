"""End-to-end cases for ``host_session`` against the in-process ECS and Auto Scaling stand-in; each case returns its named checks.

Every case builds a new stand-in host, runs the real ``Session`` through ``AwsCli`` with the stand-in as its runner and a virtual clock, and checks
the calls the stand-in saw and the state it ended in.
"""

from __future__ import annotations

import importlib
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure.host_session import REGISTRATION_SECONDS, TERMINATE_SECONDS, Clock, HostSessionError, Names, Session
from scripts.infrastructure.host_session_stand_in import CLUSTER, FAMILIES, GROUP, PROFILE, PROJECT, PROVIDER, REGION, SERVICES, StandInHost

REPO_ROOT = Path(__file__).resolve().parents[2]
ARN_OR_ACCOUNT = re.compile(r"arn:aws|\b\d{12}\b|\bi-[0-9a-f]{8,}\b")
CASE_ERRORS = (AssertionError, LookupError, StopIteration, TypeError, ValueError, AttributeError, NameError, HostSessionError)


def session_module() -> Any:
    """The ``host_session`` module as it is loaded now (a plant replaces it in ``sys.modules``)."""
    return importlib.import_module("scripts.infrastructure.host_session")


def names() -> Names:
    """The names the stand-in knows."""
    return Names(CLUSTER, GROUP, PROVIDER, PROJECT, dict(sorted(SERVICES.items(), key=lambda item: item[0] != "postgres")), dict(FAMILIES))


def build(faults: set[str] | None = None, marker: Path | None = None) -> tuple[StandInHost, Session, list[str]]:
    """A stand-in host and a session wired to it; the third value collects the session's report lines."""
    host, lines = StandInHost(faults), list[str]()
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION}, host.runner)
    marker = marker or Path(tempfile.mkdtemp(prefix="host_session_case_")) / "polaris_bootstrap.launched"
    return host, Session(cli, names(), Clock(lambda: host.now, host.advance), marker, lines.append), lines


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


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "happy_start": happy_start,
    "registration_timeout": registration_timeout,
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
}
