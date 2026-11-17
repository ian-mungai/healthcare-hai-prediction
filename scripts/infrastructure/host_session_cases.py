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
from dataclasses import replace
from pathlib import Path
from typing import Any

from orchestration.training_contract import MODEL_NAMES
from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure import (
    host_session_code_cases,
    host_session_entry_cases,
    host_session_group_signal,
    host_session_refresh_cases,
    host_session_stop_cases,
)
from scripts.infrastructure.deployed_revisions_cases import CASES as DEPLOYED_REVISION_CASES
from scripts.infrastructure.host_session import REGISTRATION_SECONDS, TERMINATE_SECONDS, Clock, HostSessionError, Names, ReloadTask, Session
from scripts.infrastructure.host_session_stand_in import (
    CLUSTER,
    CODE_FETCH_FAMILY,
    FAMILIES,
    GROUP,
    HELD_RUN,
    HOST_ADDRESS,
    JOBS_LOG_GROUP,
    LOG_GROUP,
    OPENMETADATA_FAMILY,
    OPENMETADATA_SERVICE,
    PROFILE,
    PROJECT,
    PROVIDER,
    REGION,
    RELOAD_FAMILY,
    SERVICES,
    StandInHost,
)
from scripts.process import TimeoutExpired

REPO_ROOT = Path(__file__).resolve().parents[2]
REVIEWED_SCRIPT = host_session_code_cases.REVIEWED_SCRIPT
JOB_PREFIX = host_session_refresh_cases.JOB_PREFIX
IDENTIFY_SCRIPT = REPO_ROOT / "infra" / "services" / "scripts" / "code_identify.py"
ARN_OR_ACCOUNT = re.compile(r"arn:aws|\b\d{12}\b|\bi-[0-9a-f]{8,}\b")
CASE_ERRORS = (AssertionError, LookupError, StopIteration, TypeError, ValueError, AttributeError, NameError, HostSessionError)


def session_module() -> Any:
    """The ``host_session`` module as it is loaded now (a plant replaces it in ``sys.modules``)."""
    return importlib.import_module("scripts.infrastructure.host_session")


def reload_task() -> ReloadTask:
    """The features task of the jobs stack as the stand-in knows it: the containers and the empty variables of each, from the job catalog the stack's tests
    compare with the registered task definition."""
    containers = json.loads((REPO_ROOT / "config" / "orchestration" / "ecs_broker_jobs.json").read_text(encoding="utf-8"))["jobs"]["features"]["containers"]
    return ReloadTask(RELOAD_FAMILY, {container: list(variables) for container, variables in containers.items()})


def names(openmetadata: bool = False, code_fetch: bool = True, reload: bool = True) -> Names:
    """The names the stand-in knows; ``openmetadata`` adds the persistent catalog service and its bootstrap task, ``code_fetch`` the code one-shot and
    ``reload`` the features task, the host address and the log group that the online store reload needs."""
    services = dict(SERVICES, **({"openmetadata": OPENMETADATA_SERVICE} if openmetadata else {}))
    families = dict(
        FAMILIES, **({"openmetadata_bootstrap": OPENMETADATA_FAMILY} if openmetadata else {}), **({"code_fetch": CODE_FETCH_FAMILY} if code_fetch else {})
    )
    ordered = dict(sorted(services.items(), key=lambda item: item[0] != "postgres"))
    return Names(
        CLUSTER,
        GROUP,
        PROVIDER,
        PROJECT,
        ordered,
        families,
        reload=reload_task() if reload else None,
        host_address=HOST_ADDRESS if reload else "",
        log_group=LOG_GROUP if reload else "",
        jobs_log_group=JOBS_LOG_GROUP if reload else "",
    )


# The code that a host shows after an earlier start with --code-run: the digest of its release and the run ID recorded beside it.
RECORDED_RUN = "20261108T110000Z-dddddddd"
RECORDED_SHA = "d" * 64


def build(
    faults: set[str] | None = None,
    marker: Path | None = None,
    openmetadata: bool = False,
    code_fetch: bool = True,
    checkout: str | None = RECORDED_SHA,
    code_run: str | None = RECORDED_RUN,
    reload: bool = True,
) -> tuple[StandInHost, Session, list[str]]:
    """A stand-in host and a session wired to it; the third value collects the report lines. ``checkout`` is the code digest the host shows and ``code_run``
    the run ID recorded beside it (a host that never ran an install has neither)."""
    host, lines = StandInHost(faults, openmetadata=openmetadata, code_fetch=code_fetch, checkout=checkout, code_run=code_run), list[str]()
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION}, host.runner)
    marker = marker or Path(tempfile.mkdtemp(prefix="host_session_case_")) / "polaris_bootstrap.launched"
    session = Session(cli, names(openmetadata, code_fetch, reload), Clock(lambda: host.now, host.advance), marker, lines.append, job_prefix=JOB_PREFIX)
    return host, session, lines


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
        "order is capacity, host_prepare, the code tasks, the deployments, then the online store reload": order[0] == "set-desired-capacity"
        and order[1:4] == ["run-task"] * 3
        and order[4:7] == ["update-service"] * 3
        and order[7:] == ["run-task"] * len(session_module().online_sets()),
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
    runs = [entry for entry in host.log if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [FAMILIES["host_prepare"]]]
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
    prepare = [FAMILIES["host_prepare"]]
    first = [
        entry["options"].get("--client-token") for entry in host.log if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == prepare
    ]
    session.start()
    tokens = [
        entry["options"].get("--client-token", [None])[0]
        for entry in host.log
        if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == prepare
    ]
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
    runs = [entry for entry in log if entry["operation"] == "run-task" and entry["options"]["--task-definition"] not in ([CODE_FETCH_FAMILY], [RELOAD_FAMILY])]
    last_reload = max(index for index, entry in enumerate(log) if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [RELOAD_FAMILY])
    last_update = max(index for index, entry in enumerate(log) if entry["operation"] == "update-service")
    bootstrap_run = next(
        index for index, entry in enumerate(log) if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [OPENMETADATA_FAMILY]
    )
    return {
        "the catalog service runs one task": host.services[OPENMETADATA_SERVICE]["running"] == 1
        and host.services[OPENMETADATA_SERVICE]["state"] == "COMPLETED",
        "its deployment also sets the desired count to one": updates[OPENMETADATA_SERVICE].get("--desired-count") == ["1"],
        "no other service gets a desired count": all("--desired-count" not in options for name, options in updates.items() if name != OPENMETADATA_SERVICE),
        "the bootstrap runs once, after every deployment and the online store reload": len(runs) == 2
        and bootstrap_run > last_update
        and bootstrap_run > last_reload,
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
        "no catalog bootstrap runs": [entry["options"]["--task-definition"][0] for entry in host.log if entry["operation"] == "run-task"]
        == [FAMILIES["host_prepare"], CODE_FETCH_FAMILY, CODE_FETCH_FAMILY, *[RELOAD_FAMILY] * len(session_module().online_sets())],
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


def code_command(options: dict[str, list[str]]) -> str | None:
    """The command that the RunTask overrides give the code container, or None."""
    if "--overrides" not in options:
        return None
    commands = [container["command"][0] for container in json.loads(options["--overrides"][0])["containerOverrides"] if container.get("command")]
    return commands[0] if commands else None


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
        "the code one-shot runs twice: the install, then the verification": len(runs) == 2,
        "the install carries exactly the run ID and the code digest": code_environment(runs[0]) == {"CODE_RUN_ID": CODE_RUN, "CODE_SHA256": CODE_SHA},
        "the verification carries the same two values and its mode": code_environment(runs[1])
        == {"CODE_RUN_ID": CODE_RUN, "CODE_SHA256": CODE_SHA, "CODE_MODE": "verify"},
        "both carry the reviewed installer as their command": all(code_command(item) == REVIEWED_SCRIPT.read_text(encoding="utf-8") for item in runs),
        "they run after host_prepare and before every deployment": order[:3] == [FAMILIES["host_prepare"], CODE_FETCH_FAMILY, CODE_FETCH_FAMILY]
        and order[3:6] == ["update-service"] * 3,
        "the reload launches follow the deployments": order[6:] == [RELOAD_FAMILY] * len(session_module().online_sets()),
        "it uses the Project tag and propagation": runs[0].get("--tags") == [f"key=Project,value={PROJECT}"]
        and runs[0].get("--propagate-tags") == ["TASK_DEFINITION"],
        "the host shows the new code": host.checkout == CODE_SHA,
        "every service runs": all(service["running"] == 1 for service in host.services.values()),
        "output names no ARN or account ID": not any(ARN_OR_ACCOUNT.search(line) for line in lines),
    }


def code_start_without_code_keeps_the_checkout() -> dict[str, bool]:
    host, session, _ = build(code_fetch=True, checkout=OLD_SHA, code_run=OTHER_RUN)
    session.start()
    runs = runs_of(host, CODE_FETCH_FAMILY)
    return {
        "the code one-shot still runs, to check the checkout, and then names its run": len(runs) == 2,
        "the check carries no run ID and no digest": code_environment(runs[0]) == {},
        "the check carries the reviewed installer as its command": code_command(runs[0]) == REVIEWED_SCRIPT.read_text(encoding="utf-8"),
        "the identity task carries its own script and its mode": code_command(runs[1]) == IDENTIFY_SCRIPT.read_text(encoding="utf-8")
        and code_environment(runs[1]) == {"CODE_MODE": "identify"},
        "the host keeps its checkout": host.checkout == OLD_SHA,
        "every service runs": all(service["running"] == 1 for service in host.services.values()),
    }


def code_start_without_code_on_an_empty_host_is_refused() -> dict[str, bool]:
    host, session, _ = build(code_fetch=True, checkout=None, code_run=None)
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
        "the message states that repo shows a verified checkout": message is not None and "verified checkout" in message,
        "a check of repo runs after the failure": len(runs_of(host, CODE_FETCH_FAMILY)) == 2,
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
        "both starts succeed and run the install with the same overrides": len(runs) == 4 and code_environment(runs[0]) == code_environment(runs[2]),
        "each launch uses a new client token": len({item["--client-token"][0] for item in runs}) == 4,
        "the host shows the same code": host.checkout == CODE_SHA,
    }


OTHER_RUN = "20261108T130000Z-bbbbbbbb"
OTHER_SHA = "c" * 64


def launch_code_fetch(host: StandInHost, session: Session, run: str, sha: str) -> str:
    """Start a code one-shot for ``run`` the way an earlier start did, outside ``place_code``, and answer its task ARN."""
    environment = [{"name": "CODE_RUN_ID", "value": run}, {"name": "CODE_SHA256", "value": sha}]
    command = [REVIEWED_SCRIPT.read_text(encoding="utf-8")]
    launched = session.run_task(
        "code_fetch", CODE_FETCH_FAMILY, {"containerOverrides": [{"name": "code_fetch", "environment": environment, "command": command}]}
    )
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
        "the requested code gets its own install launch": len(runs) == 3 and code_environment(runs[1]) == {"CODE_RUN_ID": OTHER_RUN, "CODE_SHA256": OTHER_SHA},
        "the host shows the requested code, not the running one": host.checkout == OTHER_SHA,
        "the requested task starts only after the running one stopped": len(launches) == 3 and launches[1] >= stops_at,
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
            len(runs_of(host, CODE_FETCH_FAMILY)) == 3 and host.checkout == requested[1]
        )
    return checks


def code_fetch_running_for_the_same_code_is_awaited_not_doubled() -> dict[str, bool]:
    host, session, lines = build(code_fetch=True)
    session.host_on()
    launch_code_fetch(host, session, CODE_RUN, CODE_SHA)
    session.place_code((CODE_RUN, CODE_SHA))
    return {
        "the install is not launched a second time": [(code_environment(item) or {}).get("CODE_MODE") for item in runs_of(host, CODE_FETCH_FAMILY)]
        == [None, "verify"],
        "the host shows the code": host.checkout == CODE_SHA,
        "the wait is reported": any("already running; waiting for it" in line for line in lines),
    }


def code_fetch_running_without_the_reviewed_command_is_not_reused() -> dict[str, bool]:
    host, session, lines = build(code_fetch=True)
    session.host_on()
    environment = [{"name": "CODE_RUN_ID", "value": CODE_RUN}, {"name": "CODE_SHA256", "value": CODE_SHA}]
    session.run_task("code_fetch", CODE_FETCH_FAMILY, {"containerOverrides": [{"name": "code_fetch", "environment": environment}]})
    session.place_code((CODE_RUN, CODE_SHA))
    runs = runs_of(host, CODE_FETCH_FAMILY)
    return {
        "a running task with the same values but the registered command is not taken for the reviewed install": len(runs) == 3
        and code_command(runs[1]) == REVIEWED_SCRIPT.read_text(encoding="utf-8"),
        "the wait names why": any("another run or digest" in line for line in lines),
        "the host shows the code": host.checkout == CODE_SHA,
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


def code_fetch_launch_retries_stay_inside_the_limit() -> dict[str, bool]:
    """A RunTask whose reply is lost near the end of the one-shot's limit is not retried past it: every AWS call of the one-shot, retries included, runs
    under the one-shot's deadline and is refused once it has passed, so no launch happens after the limit and start fails visibly."""
    host, session, _ = build(code_fetch=True, checkout=OLD_SHA)
    session.host_on()
    running = launch_code_fetch(host, session, CODE_RUN, CODE_SHA)
    begun = host.now
    host.tasks[running]["stops_at"] = begun + CODE_FETCH_LIMIT - 110
    host.latency = 100
    original, lost = host.runner, [False]

    def runner(argv: list[str], **options: Any) -> Any:
        if argv[2] == "run-task" and CODE_FETCH_FAMILY in argv and not lost[0]:
            lost[0] = True
            host.advance(options["timeout"])
            raise TimeoutExpired(argv, options["timeout"])
        return original(argv, **options)

    session.cli.runner = runner
    message = failure(lambda: session.place_code((OTHER_RUN, OTHER_SHA)))
    late = [
        entry["at"] - begun
        for entry in host.log
        if entry["operation"] == "run-task" and entry["at"] > begun and entry["options"]["--task-definition"] == [CODE_FETCH_FAMILY]
    ]
    return {
        "the reply of the first launch was lost": lost[0],
        "the placement fails": message is not None and "code_fetch" in message,
        "no launch happens after the limit": all(offset <= CODE_FETCH_LIMIT for offset in late),
        "the placement ends by the limit": host.now - begun <= CODE_FETCH_LIMIT,
    }


def code_given_without_the_one_shot_is_refused() -> dict[str, bool]:
    host, session, _ = build(code_fetch=False)
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
        def __init__(self, reload: bool = False) -> None:
            self.reload = reload

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


RELOAD_RUN_ID = re.compile(r"reload_([a-z_]+)_[a-f0-9]{8}")


def reload_runs(host: StandInHost) -> list[dict[str, Any]]:
    """The reload launches that the stand-in saw, in order."""
    return list(host.reloads)


def sets_online() -> list[str]:
    return session_module().online_sets()


def start_reloads_every_online_set_after_the_services_are_stable() -> dict[str, bool]:
    host, session, lines = build(openmetadata=True)
    session.start(code=(CODE_RUN, CODE_SHA))
    launches = reload_runs(host)
    log = [entry for entry in host.log if entry["operation"] in {"run-task", "update-service"}]
    last_update = max(index for index, entry in enumerate(log) if entry["operation"] == "update-service")
    reload_at = [index for index, entry in enumerate(log) if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [RELOAD_FAMILY]]
    bootstrap_at = next(index for index, entry in enumerate(log) if entry["options"].get("--task-definition") == [OPENMETADATA_FAMILY])
    release = [item["launch"]["feast"] for item in launches]
    variables = reload_task().variables
    return {
        "the online sets come from the feature schema": sets_online() == ["m1", "m2", "m4"],
        "one launch per online set, in order": [item["model"] for item in launches] == sets_online(),
        "every launch is after the last deployment and every service was stable": len(reload_at) == len(sets_online())
        and min(reload_at) > last_update
        and all(item["services_stable"] for item in launches),
        "the catalog bootstrap runs after the last reload": bootstrap_at > max(reload_at),
        "the launches run one at a time": all(launches[index]["at"] < launches[index + 1]["at"] for index in range(len(launches) - 1)),
        "each launch is a reload of its set with the code run, and the fetch container has the digest": all(
            item["launch"]["feast"]["LAYER_PHASE"] == "reload"
            and item["launch"]["feast"]["RELEASE_MODEL"] == item["model"]
            and item["launch"]["feast"]["CODE_RUN_ID"] == CODE_RUN
            and item["launch"]["feast"]["FEATURES_RUN"] == ""
            and item["launch"]["fetch"]["CODE_RUN_ID"] == CODE_RUN
            and item["launch"]["fetch"]["CODE_SHA256"] == CODE_SHA
            for item in launches
        ),
        "each launch names the host's Polaris address": all(item["POLARIS_URI"] == f"http://{HOST_ADDRESS}:8181/api/catalog" for item in release),
        "each launch has a run ID of its own, reload_<model short name>_<hex>": all(
            (found := RELOAD_RUN_ID.fullmatch(item["RUN_ID"])) is not None and found[1] == MODEL_NAMES[item["RELEASE_MODEL"]] for item in release
        )
        and len({item["RUN_ID"] for item in release}) == len(release),
        "every container of the features task gets exactly its variables": all(
            {container: list(item["launch"][container]) for container in variables} == variables for item in launches
        ),
        "every build container is told the reload phase and the feast container is the one that reloads": all(
            item["launch"][container].get("LAYER_PHASE", "reload") == "reload" for item in launches for container in item["launch"]
        ),
        "every set is online with the code of the start": host.online == {model: (CODE_RUN, CODE_SHA) for model in sets_online()},
        "the launches use the Project tag, propagation and a client token": all(
            entry["options"].get("--tags") == [f"key=Project,value={PROJECT}"] and entry["options"].get("--client-token")
            for entry in host.log
            if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [RELOAD_FAMILY]
        ),
        "output names no ARN or account ID": not any(ARN_OR_ACCOUNT.search(line) for line in lines),
    }


def a_skipped_reload_does_not_stop_start_and_is_reported() -> dict[str, bool]:
    """Owner decision B, Nov 17 2026: a set whose reload ends skipped (a held newer run on a changed schema) loads nothing, and start goes on and says so."""
    host, session, lines = build({"reload_skipped:m2"}, openmetadata=True)
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    skipped = [line for line in lines if line.startswith("online_store_reload_skipped")]
    bootstrap_at = [index for index, entry in enumerate(host.log) if entry["options"].get("--task-definition") == [OPENMETADATA_FAMILY]]
    return {
        "start does not fail": message is None,
        "every online set was launched, in order": [item["model"] for item in reload_runs(host)] == sets_online(),
        "the skipped set is not online and the others are": sorted(host.online) == [model for model in sets_online() if model != "m2"],
        "the catalog bootstrap still runs": len(bootstrap_at) == 1,
        "the host stays on after a successful start": host.desired == 1 and not any(line.startswith("start_failed") for line in lines),
        "the step log names the skipped set, the held run and the reason": len(skipped) == 1
        and skipped[0].startswith("online_store_reload_skipped: m2:")
        and HELD_RUN in skipped[0]
        and "another schema" in skipped[0],
        "the sets that loaded are reported as reloaded and the skipped one is not": "online_store_reloaded: m1" in lines
        and "online_store_reloaded: m4" in lines
        and "online_store_reloaded: m2" not in lines,
        "the end of the reload step lists the sets that serve no online predictions": any(line.startswith("online_store_not_loaded: m2") for line in lines),
        "output names no ARN or account ID": not any(ARN_OR_ACCOUNT.search(line) for line in lines),
    }


def a_start_with_every_set_loaded_reports_no_unloaded_set() -> dict[str, bool]:
    host, session, lines = build()
    session.start(code=(CODE_RUN, CODE_SHA))
    return {
        "every set is reported as reloaded": [line for line in lines if line.startswith("online_store_reloaded")]
        == [f"online_store_reloaded: {model}" for model in sets_online()],
        "no set is reported skipped or unloaded": not any(line.startswith(("online_store_reload_skipped", "online_store_not_loaded")) for line in lines),
        "every set is online": sorted(host.online) == sorted(sets_online()),
    }


def a_reload_with_no_readable_result_is_reported_and_does_not_stop_start() -> dict[str, bool]:
    """A task that exited 0 is a finished reload; when its log has no result line the outcome is unknown, which is reported by name, never as reloaded."""
    host, session, lines = build({"reload_no_log"})
    begun = host.now
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    unknown = [line for line in lines if line.startswith("online_store_reload_outcome_unknown")]
    return {
        "start does not fail": message is None and host.desired == 1,
        "each set is reported with an unknown outcome": [line.split(":")[1].strip() for line in unknown] == sets_online(),
        "no set is reported as reloaded": not any(line.startswith("online_store_reloaded") for line in lines),
        "the wait for the log is bounded": host.now - begun < 3 * 400 + 5000,
    }


def step_lines(lines: list[str], name: str) -> list[str]:
    """The report lines of one step."""
    return [line for line in lines if line.startswith(f"{name}:")]


def a_result_that_is_forged_repeated_or_incomplete_is_reported_unknown() -> dict[str, bool]:
    """The result of a reload is one complete line: a line appended after the genuine one, a second result and a line without the fields of its outcome are
    each an unknown outcome, never a reload, and the last line does not win."""
    faults = {"reload_skipped:m2", "reload_forged_after:m2", "reload_incomplete:m4", "reload_second_result:m1"}
    host, session, lines = build(faults)
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    unknown = step_lines(lines, "online_store_reload_outcome_unknown")
    _, bare_session, bare_lines = build({"reload_skipped:m2", "reload_incomplete:m2"})
    bare_message = failure(lambda: bare_session.start(code=(CODE_RUN, CODE_SHA)))
    return {
        "start does not fail": message is None and host.desired == 1,
        "a bare loaded line after the genuine skipped line leaves m2 unknown": any(
            line.startswith("online_store_reload_outcome_unknown: m2:") for line in lines
        ),
        "a second complete result leaves m1 unknown": any(line.startswith("online_store_reload_outcome_unknown: m1:") for line in lines),
        "a result without the fields of its outcome leaves m4 unknown": any(line.startswith("online_store_reload_outcome_unknown: m4:") for line in lines),
        "all three sets are unknown and nothing else": len(unknown) == 3,
        "no set is reported as reloaded or as skipped": not any(line.startswith(("online_store_reloaded", "online_store_reload_skipped")) for line in lines),
        "a skipped result without its reason and held runs is unknown, not skipped": bare_message is None
        and any(line.startswith("online_store_reload_outcome_unknown: m2:") for line in bare_lines)
        and not step_lines(bare_lines, "online_store_reload_skipped"),
    }


def log_page_calls(host: StandInHost, model: str) -> list[dict[str, list[str]]]:
    """The options of the log page requests that the session made for the reload log of ``model``."""
    reload_tasks = [arn.rsplit("/", 1)[-1] for arn, task in host.tasks.items() if task["key"] == "online_reload"]
    owner = reload_tasks[[item["model"] for item in host.reloads].index(model)]
    return [entry["options"] for entry in host.log if entry["operation"] == "get-log-events" and entry["options"]["--log-stream-name"][0].endswith(f"/{owner}")]


def a_reload_result_is_one_complete_line_across_every_log_page() -> dict[str, bool]:
    """The log of a reload is read to its end, page by page (CloudWatch ``nextForwardToken`` until it repeats): a complete result on a page that is not
    the first still counts, a second complete result on any page leaves the outcome unknown whatever the first page holds, an empty page does not end
    the log, and a log that never ends is unknown at the deadline, never a reload."""
    paged = {"reload_forged_first_page:m2", "reload_skipped:m2", "reload_empty_middle_page:m4", "reload_skipped:m4", "reload_late_result:m1"}
    host, session, lines = build(paged)
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    unknown = step_lines(lines, "online_store_reload_outcome_unknown")
    endless_host, endless_session, endless_lines = build({"reload_endless_pages:m2", "reload_paged:m1"})
    begun = endless_host.now
    endless_message = failure(lambda: endless_session.start(code=(CODE_RUN, CODE_SHA)))
    endless_unknown = step_lines(endless_lines, "online_store_reload_outcome_unknown")
    endless_calls = log_page_calls(endless_host, "m2")
    return {
        "start does not fail": message is None and host.desired == 1 and endless_message is None and endless_host.desired == 1,
        "a forged complete loaded line on page 1 and the genuine skipped line on page 2 leave m2 unknown, not reloaded": any(
            line.startswith("online_store_reload_outcome_unknown: m2:") for line in lines
        )
        and "online_store_reloaded: m2" not in lines
        and not step_lines(lines, "online_store_reload_skipped"),
        "a forged line, an empty page and the genuine line leave m4 unknown": any(line.startswith("online_store_reload_outcome_unknown: m4:") for line in lines)
        and "online_store_reloaded: m4" not in lines,
        "a single complete result after two empty pages is read and m1 is reloaded": "online_store_reloaded: m1" in lines
        and not any(line.startswith("online_store_reload_outcome_unknown: m1:") for line in lines),
        "exactly m2 and m4 are unknown": len(unknown) == 2,
        "the session followed the tokens of the first set to their end": [call.get("--next-token") for call in log_page_calls(host, "m1")][:4]
        == [None, ["f/1"], ["f/2"], ["f/3"]],
        "a log that never ends is unknown for m2 and the other sets are read": len(endless_unknown) == 1
        and endless_unknown[0].startswith("online_store_reload_outcome_unknown: m2:")
        and "online_store_reloaded: m2" not in endless_lines
        and "online_store_reloaded: m1" in endless_lines,
        "the endless log is read within the deadline and no further": 2 <= len(endless_calls) <= 180 // 5 + 2 and endless_host.now - begun < 3 * 400 + 5000,
    }


def an_unknown_reload_outcome_is_not_reported_as_a_confirmed_skip() -> dict[str, bool]:
    """The closing report keeps the sets whose outcome is unknown apart from the sets that a result confirmed as skipped: only a confirmed skip says that a
    set serves no online features."""
    _, unknown_session, unknown_lines = build({"reload_no_log"})
    failure(lambda: unknown_session.start(code=(CODE_RUN, CODE_SHA)))
    _, mixed_session, mixed_lines = build({"reload_skipped:m2", "reload_incomplete:m4"})
    failure(lambda: mixed_session.start(code=(CODE_RUN, CODE_SHA)))
    unconfirmed = step_lines(unknown_lines, "online_store_reload_unconfirmed")
    not_loaded, mixed_unconfirmed = step_lines(mixed_lines, "online_store_not_loaded"), step_lines(mixed_lines, "online_store_reload_unconfirmed")
    return {
        "a start where no outcome could be read confirms no skipped set": not step_lines(unknown_lines, "online_store_not_loaded"),
        "the unknown sets are named in one unconfirmed line": len(unconfirmed) == 1 and all(model in unconfirmed[0] for model in sets_online()),
        "the unconfirmed line does not claim that the sets serve nothing online": bool(unconfirmed) and "no online predictions" not in unconfirmed[0],
        "a confirmed skip is named in the not loaded line and only it": len(not_loaded) == 1 and "m2" in not_loaded[0] and "m4" not in not_loaded[0],
        "the not loaded line says what the skip means": bool(not_loaded) and "no online predictions" in not_loaded[0],
        "an unknown set is named in the unconfirmed line and only it": len(mixed_unconfirmed) == 1
        and "m4" in mixed_unconfirmed[0]
        and "m2" not in mixed_unconfirmed[0],
    }


def start_without_code_reloads_the_recorded_run() -> dict[str, bool]:
    host, session, lines = build(checkout=OLD_SHA, code_run=OTHER_RUN)
    session.start()
    release = [item["launch"]["fetch"] for item in reload_runs(host)]
    reads = [entry for entry in host.log if entry["service"] == "logs" and entry["options"]["--log-group-name"] == [LOG_GROUP]]
    return {
        "every reload carries the run and digest recorded for the checkout": len(release) == len(sets_online())
        and all(item["CODE_RUN_ID"] == OTHER_RUN and item["CODE_SHA256"] == OLD_SHA for item in release),
        "the identity came from the log of the identity task in the code log group, read to its end": [entry["options"].get("--next-token") for entry in reads]
        == [None, ["f/1"]]
        and all(
            entry["options"]["--log-group-name"] == [LOG_GROUP] and entry["options"]["--log-stream-name"][0].startswith("code_fetch/code_fetch/")
            for entry in reads
        ),
        "the identity is reported by run only": any(line == f"code_identity: {OTHER_RUN}" for line in lines),
        "the host keeps its checkout": host.checkout == OLD_SHA,
    }


def the_code_identity_is_read_from_a_later_log_page() -> dict[str, bool]:
    """The identity line of the identity task counts on any page of its log, as the result line of a reload does: a log whose first page is empty is read on."""
    host, session, lines = build({"identity_late_line"}, checkout=OLD_SHA, code_run=OTHER_RUN)
    message = failure(session.start)
    reads = [entry["options"] for entry in host.log if entry["service"] == "logs" and entry["options"]["--log-group-name"] == [LOG_GROUP]]
    return {
        "start does not fail": message is None,
        "the identity of the second page is the code of every reload": len(reload_runs(host)) == len(sets_online())
        and all(item["launch"]["fetch"]["CODE_RUN_ID"] == OTHER_RUN and item["launch"]["fetch"]["CODE_SHA256"] == OLD_SHA for item in reload_runs(host)),
        "the identity is reported by run only": f"code_identity: {OTHER_RUN}" in lines,
        "the second page was asked for with the token of the first": any(call.get("--next-token") == ["f/1"] for call in reads),
    }


def start_with_code_does_not_read_the_record() -> dict[str, bool]:
    host, session, _ = build(checkout=OLD_SHA, code_run=OTHER_RUN)
    session.start(code=(CODE_RUN, CODE_SHA))
    modes = [(code_environment(item) or {}).get("CODE_MODE", "install") for item in runs_of(host, CODE_FETCH_FAMILY)]
    return {
        "the flags win: the code tasks are the install and the verification, with no identity task": modes == ["install", "verify"],
        "no identity log is read": not any(entry["service"] == "logs" and entry["options"]["--log-group-name"] == [LOG_GROUP] for entry in host.log),
        "every reload carries the run and digest given": all(
            item["launch"]["fetch"]["CODE_RUN_ID"] == CODE_RUN and item["launch"]["fetch"]["CODE_SHA256"] == CODE_SHA for item in reload_runs(host)
        ),
        "the install replaces the record of the digest": host.code_run == CODE_RUN and host.checkout == CODE_SHA,
    }


def start_without_a_code_record_is_refused() -> dict[str, bool]:
    host, session, _ = build({"no_code_record"})
    message = failure(session.start)
    return {
        "start fails and names the missing record": message is not None and "code run record" in message and "repo_releases/<digest>/run_id" in message,
        "it names the way out": message is not None and "--code-run" in message and "--code-sha256" in message,
        "no service is deployed and no reload is launched": not any(entry["operation"] == "update-service" for entry in host.log) and reload_runs(host) == [],
        "the host is turned off again": host.desired == 0,
    }


def start_without_a_code_record_keeps_the_host_with_keep_on() -> dict[str, bool]:
    host, session, lines = build({"no_code_record"})
    message = failure(lambda: session.start(keep_on=True))
    return {
        "start fails the same way": message is not None and "code run record" in message,
        "the host is left on for diagnosis": host.desired == 1 and any(line.startswith("host_kept_on") for line in lines),
    }


def an_identity_log_that_never_shows_the_line_fails_visibly() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for fault in ("no_identity_log", "bad_identity_line"):
        host, session, _ = build({fault})
        begun = host.now
        message = failure(session.start)
        checks[f"{fault}: start fails inside the identity wait and names it"] = message is not None and "code identity log" in message
        checks[f"{fault}: no service is deployed, nothing is reloaded and the host is off"] = (
            not any(entry["operation"] == "update-service" for entry in host.log) and reload_runs(host) == [] and host.desired == 0
        )
        checks[f"{fault}: the wait is bounded"] = host.now - begun < 3000
    return checks


def an_unreadable_log_fails_start_by_name() -> dict[str, bool]:
    host, session, _ = build()
    session.names = replace(session.names, log_group="another-log-group")
    message = failure(session.start)
    return {
        "a log group that has no such stream fails start inside the identity wait": message is not None and "code identity log" in message,
        "no service is deployed and the host is off": not any(entry["operation"] == "update-service" for entry in host.log) and host.desired == 0,
    }


def a_failed_reload_stops_the_host_and_names_the_set() -> dict[str, bool]:
    host, session, lines = build({"reload_fails:m2"}, openmetadata=True)
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    return {
        "start fails and names the set": message is not None and "online store reload of set m2 failed" in message and "exit codes" in message,
        "the sets before it were reloaded and the set after it was not launched": list(host.online) == ["m1"]
        and [item["model"] for item in reload_runs(host)] == ["m1", "m2"],
        "the catalog bootstrap does not run": not any(
            entry["options"].get("--task-definition") == [OPENMETADATA_FAMILY] for entry in host.log if entry["operation"] == "run-task"
        ),
        "the host is turned off again": host.desired == 0,
        "the message names no ARN or account ID": not ARN_OR_ACCOUNT.search(message or "") and not any(ARN_OR_ACCOUNT.search(line) for line in lines),
    }


def a_failed_reload_keeps_the_host_with_keep_on() -> dict[str, bool]:
    host, session, lines = build({"exit_code:online_reload=1"})
    message = failure(lambda: session.start(keep_on=True, code=(CODE_RUN, CODE_SHA)))
    return {
        "start fails on the first set": message is not None and "set m1" in message and len(reload_runs(host)) == 1,
        "the host stays on and says so": host.desired == 1 and any(line.startswith("host_kept_on") for line in lines),
    }


def a_reload_task_that_never_starts_is_a_failed_start() -> dict[str, bool]:
    host, session, _ = build({"never_starts:online_reload"})
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    return {
        "start fails and names the set and the stop reason": message is not None and "set m1" in message and "task failed to start" in message,
        "the failure is not taken for a script that ran": message is not None and "Traceback" not in message,
        "the host is turned off again": host.desired == 0,
    }


def a_reload_that_passes_its_deadline_is_a_failed_start() -> dict[str, bool]:
    host, session, _ = build({"task_seconds:online_reload=99999"})
    begun = host.now
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    return {
        "start fails inside the reload's bounded wait": message is not None and "set m1" in message and "2400 s" in message,
        "the wait ended at its limit, not later": host.now - begun < 2400 + 1500,
        "the host is turned off again": host.desired == 0,
    }


def a_lost_reload_reply_launches_the_reload_once() -> dict[str, bool]:
    host, session, _ = build({"stale_list_tasks"})
    session.start(code=(CODE_RUN, CODE_SHA))
    before = len(runs_of(host, RELOAD_FAMILY))
    host.lost_replies = 1
    session.reload_online_store((CODE_RUN, CODE_SHA))
    second = runs_of(host, RELOAD_FAMILY)[before:]
    tokens = [item["--client-token"][0] for item in second]
    return {
        "the lost reply is retried: one launch more than sets": len(second) == len(sets_online()) + 1,
        "the retry of the lost launch carries the same client token": tokens[0] == tokens[1],
        "every set gets its own token": len(set(tokens)) == len(sets_online()),
        "the accepted launch is not doubled": len([task for task in host.tasks.values() if task["key"] == "online_reload"]) == 2 * len(sets_online()),
    }


def a_running_features_task_is_awaited_and_never_taken_for_the_reload() -> dict[str, bool]:
    host, session, lines = build()
    session.host_on()
    running = host.add_task(RELOAD_FAMILY)
    host.tasks[running]["stops_at"] = host.now + 300
    stops_at = host.tasks[running]["stops_at"]
    session.reload_online_store((CODE_RUN, CODE_SHA))
    launches = [entry["at"] for entry in host.log if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [RELOAD_FAMILY]]
    return {
        "the reload launches only after the other features task stopped": len(launches) == len(sets_online()) and launches[0] >= stops_at,
        "the other task was not counted as a reload": len(reload_runs(host)) == len(sets_online()),
        "the wait is reported": any("another run or digest" in line for line in lines),
    }


def start_refuses_a_stack_that_cannot_reload() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    stacks: tuple[tuple[str, dict[str, Any], str], ...] = (
        ("a jobs stack with no features task", {"reload": False}, "host_run_tasks.features.task_definition"),
        ("a stack with no code one-shot and no code given", {"code_fetch": False}, "code_fetch"),
    )
    for name, kwargs, needle in stacks:
        host, session, _ = build(**kwargs)
        message = failure(session.start)
        checks[f"{name} is refused by name"] = message is not None and needle in message
        checks[f"{name}: the host is never turned on"] = not any(entry["operation"] == "set-desired-capacity" for entry in host.log)
    host, session, _ = build()
    session.names = replace(session.names, host_address="")
    message = failure(session.start)
    checks["a platform without the host address is refused by name"] = message is not None and "host_private_ip" in message and host.log == []
    host, session, _ = build()
    session.names = replace(session.names, log_group="")
    message = failure(session.start)
    checks["a start that needs the log and has no log group is refused by name"] = message is not None and "log_group_names" in message and host.log == []
    host, session, _ = build()
    session.names = replace(session.names, log_group="")
    session.start(code=(CODE_RUN, CODE_SHA))
    checks["a start with the code given needs no log group"] = len(reload_runs(host)) == len(sets_online())
    return checks


def a_features_variable_that_the_reload_cannot_fill_is_refused() -> dict[str, bool]:
    host, session, _ = build()
    task = reload_task()
    session.names = replace(session.names, reload=ReloadTask(task.family, {**task.variables, "feast": [*task.variables["feast"], "SURPRISE"]}))
    message = failure(lambda: session.start(code=(CODE_RUN, CODE_SHA)))
    return {
        "the new variable is refused by name after the services, before any launch": message is not None and "SURPRISE" in message and reload_runs(host) == [],
        "the host is turned off again": host.desired == 0,
    }


def the_online_sets_follow_the_feature_schema() -> dict[str, bool]:
    module = session_module()
    document = json.loads(module.FEATURE_SCHEMA.read_text(encoding="utf-8"))
    changed = {**document, "model_sets": {name: {**entry, "online": name == "m3"} for name, entry in document["model_sets"].items()}}
    with tempfile.TemporaryDirectory(prefix="host_session_schema_") as raw:
        schema = Path(raw) / "feature_schema.json"
        schema.write_text(json.dumps(changed), encoding="utf-8")
        original = module.FEATURE_SCHEMA
        module.FEATURE_SCHEMA = schema
        try:
            host, session, _ = build()
            session.start(code=(CODE_RUN, CODE_SHA))
        finally:
            module.FEATURE_SCHEMA = original
    return {
        "the sets that are online in the schema are the sets reloaded": [item["model"] for item in reload_runs(host)] == ["m3"],
        "the real schema lists the sets with an online load": sorted(name for name, entry in document["model_sets"].items() if entry["online"])
        == sets_online(),
    }


def load_names_reads_the_jobs_outputs_for_start_only() -> dict[str, bool]:
    module: Any = importlib.import_module("scripts.infrastructure.host_session")
    platform = {
        "cluster_name": {"value": CLUSTER},
        "host_autoscaling_group_name": {"value": GROUP},
        "ec2_capacity_provider_name": {"value": PROVIDER},
        "project_tag": {"value": PROJECT},
        "host_private_ip": {"value": HOST_ADDRESS},
        "log_group_names": {"value": {"airflow": LOG_GROUP, "polaris": "other"}},
    }
    services = {
        "service_names": {"value": {"airflow_scheduler": "a", "polaris": "p", "postgres": "q"}},
        "one_shot_run_task": {"value": {key: {"task_definition": family} for key, family in FAMILIES.items()}},
    }
    variables = {"fetch": ["RUN_ID"], "feast": ["RUN_ID", "RELEASE_MODEL", "LAYER_PHASE"]}
    jobs: dict[str, Any] = {"host_run_tasks": {"value": {"features": {"task_definition": RELOAD_FAMILY, "container_environment": variables}}}}
    reads: list[Any] = []

    def terraform(document: dict[str, Any]) -> Callable[..., dict[str, Any]]:
        def read(directory: Path, *_: Any, **__: Any) -> dict[str, Any]:
            reads.append(directory)
            return platform if directory == module.PLATFORM_DIRECTORY else jobs if directory == module.JOBS_DIRECTORY else document

        return read

    original = module.terraform_document
    try:
        module.terraform_document = terraform(services)
        plain = module.load_names({})
        plain_reads = list(reads)
        reads.clear()
        full = module.load_names({}, reload=True)
        jobs["host_run_tasks"] = {"value": None}
        off = module.load_names({}, reload=True)
        jobs["host_run_tasks"] = {"value": {"batch": {"task_definition": "x"}}}
        missing = module.load_names({}, reload=True)
        jobs["host_run_tasks"] = {"value": {"features": {"task_definition": RELOAD_FAMILY, "container_environment": {"fetch": ["RUN_ID"]}}}}
        no_feast = module.load_names({}, reload=True)
        jobs["host_run_tasks"] = {"value": {"features": {"task_definition": RELOAD_FAMILY, "container_environment": {"feast": ["RUN_ID", "LAYER_PHASE"]}}}}
        older = module.load_names({}, reload=True)
        platform.pop("host_private_ip")
        platform.pop("log_group_names")
        bare = module.load_names({})
        jobs["host_run_tasks"] = {"value": {"features": {"task_definition": RELOAD_FAMILY, "container_environment": variables}}}
        named = module.load_names({"project_name": "stand_in", "environment": "dev"}, reload=True)
        unnamed = module.load_names({}, reload=True)
        plain_named = module.load_names({"project_name": "stand_in", "environment": "dev"})
    finally:
        module.terraform_document = original
    return {
        "a command that does not start reads no jobs output": module.JOBS_DIRECTORY not in plain_reads and plain.reload is None,
        "start reads the platform, the services and the jobs outputs": set(reads)
        == {module.PLATFORM_DIRECTORY, module.SERVICES_DIRECTORY, module.JOBS_DIRECTORY},
        "the features task, the address and the log group come from the outputs": full.reload == ReloadTask(RELOAD_FAMILY, variables)
        and full.host_address == HOST_ADDRESS
        and full.log_group == LOG_GROUP,
        "a null host_run_tasks, a missing features task, no feast container and an older feast container without RELEASE_MODEL are no reload task": (
            off.reload,
            missing.reload,
            no_feast.reload,
            older.reload,
        )
        == (None, None, None, None),
        "a platform without the two outputs loads, so stop and status never wait on them": bare.host_address == "" and bare.log_group == "",
        "the jobs log group is the one the jobs stack requires, from the project name and environment, for start only": named.jobs_log_group
        == "stand_in_jobs_dev"
        and unnamed.jobs_log_group == ""
        and plain_named.jobs_log_group == "",
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
    "code_fetch_running_without_the_reviewed_command_is_not_reused": code_fetch_running_without_the_reviewed_command_is_not_reused,
    "code_fetch_running_for_other_code_that_never_stops_fails_visibly": code_fetch_running_for_other_code_that_never_stops_fails_visibly,
    "code_fetch_wait_and_launch_share_one_deadline": code_fetch_wait_and_launch_share_one_deadline,
    "code_fetch_launch_retries_stay_inside_the_limit": code_fetch_launch_retries_stay_inside_the_limit,
    "code_given_without_the_one_shot_is_refused": code_given_without_the_one_shot_is_refused,
    "code_options_are_checked_before_any_call": code_options_are_checked_before_any_call,
    "start_reloads_every_online_set_after_the_services_are_stable": start_reloads_every_online_set_after_the_services_are_stable,
    "a_skipped_reload_does_not_stop_start_and_is_reported": a_skipped_reload_does_not_stop_start_and_is_reported,
    "a_start_with_every_set_loaded_reports_no_unloaded_set": a_start_with_every_set_loaded_reports_no_unloaded_set,
    "a_reload_with_no_readable_result_is_reported_and_does_not_stop_start": a_reload_with_no_readable_result_is_reported_and_does_not_stop_start,
    "a_result_that_is_forged_repeated_or_incomplete_is_reported_unknown": a_result_that_is_forged_repeated_or_incomplete_is_reported_unknown,
    "a_reload_result_is_one_complete_line_across_every_log_page": a_reload_result_is_one_complete_line_across_every_log_page,
    "an_unknown_reload_outcome_is_not_reported_as_a_confirmed_skip": an_unknown_reload_outcome_is_not_reported_as_a_confirmed_skip,
    "start_without_code_reloads_the_recorded_run": start_without_code_reloads_the_recorded_run,
    "the_code_identity_is_read_from_a_later_log_page": the_code_identity_is_read_from_a_later_log_page,
    "start_with_code_does_not_read_the_record": start_with_code_does_not_read_the_record,
    "start_without_a_code_record_is_refused": start_without_a_code_record_is_refused,
    "start_without_a_code_record_keeps_the_host_with_keep_on": start_without_a_code_record_keeps_the_host_with_keep_on,
    "an_identity_log_that_never_shows_the_line_fails_visibly": an_identity_log_that_never_shows_the_line_fails_visibly,
    "an_unreadable_log_fails_start_by_name": an_unreadable_log_fails_start_by_name,
    "a_failed_reload_stops_the_host_and_names_the_set": a_failed_reload_stops_the_host_and_names_the_set,
    "a_failed_reload_keeps_the_host_with_keep_on": a_failed_reload_keeps_the_host_with_keep_on,
    "a_reload_task_that_never_starts_is_a_failed_start": a_reload_task_that_never_starts_is_a_failed_start,
    "a_reload_that_passes_its_deadline_is_a_failed_start": a_reload_that_passes_its_deadline_is_a_failed_start,
    "a_lost_reload_reply_launches_the_reload_once": a_lost_reload_reply_launches_the_reload_once,
    "a_running_features_task_is_awaited_and_never_taken_for_the_reload": a_running_features_task_is_awaited_and_never_taken_for_the_reload,
    "start_refuses_a_stack_that_cannot_reload": start_refuses_a_stack_that_cannot_reload,
    "a_features_variable_that_the_reload_cannot_fill_is_refused": a_features_variable_that_the_reload_cannot_fill_is_refused,
    "the_online_sets_follow_the_feature_schema": the_online_sets_follow_the_feature_schema,
    "load_names_reads_the_jobs_outputs_for_start_only": load_names_reads_the_jobs_outputs_for_start_only,
}


CASES.update(host_session_code_cases.CASES)
CASES.update(host_session_refresh_cases.CASES)
CASES.update(host_session_stop_cases.CASES)
CASES.update(host_session_entry_cases.CASES)
CASES.update(DEPLOYED_REVISION_CASES)
