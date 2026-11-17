"""Start and stop the AWS host control plane for a working session: ``start``, ``refresh``, ``bootstrap``, ``stop`` and ``status``.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.host_session start``. The profile and region come from ``.env`` through
``load_configuration``. The cluster, the host group, the capacity provider, the service names and the one-shot task families come from the
``terraform output -json`` of the platform and services stacks. Every AWS call goes through ``AwsCli`` with the project profile. Every wait has a
deadline and fails visibly. Output names no account ID and no ARN, and shows a provider reason only as a fixed category.

``refresh`` places new code on a host that is already on and redeploys every service. It never changes the desired capacity and never stops the host. The
service and one-shot names come from the services state of the checkout it runs in (``infra/services/terraform.tfstate``). The state is a local file, so a
checkout whose copy is older than the applied stack names fewer services than the stack has. Run ``refresh`` from the checkout that holds the newest
services state; ``refresh`` and ``status`` print the serial of the state they read (``services_state: serial <n>``), and ``refresh`` refuses a name that
ECS does not have.

``stop`` lists the running job tasks of the project first, as ``aws_tasks list`` does, and changes nothing while any runs: it refuses, or with
``--wait-seconds N`` polls every 15 seconds until none runs and refuses when the N seconds pass. ``--force-stop`` skips the check. A listing that fails
or cannot be read refuses; it is never read as "no job task is running". Stopping the host ends every task on it; a job launched in the seconds between
the check and the stop is resumed by its DAG retry.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import signal
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orchestration.training_contract import CODE_RUN, MODEL_NAMES, compose_run_id, new_run_hex
from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.infrastructure.render_platform_config import PLATFORM_DIRECTORY, terraform_document
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration
from scripts.process import CompletedProcess

if TYPE_CHECKING:
    from scripts.infrastructure.aws_tasks import JobTask

SERVICES_DIRECTORY = REPO_ROOT / "infra" / "services"
JOBS_DIRECTORY = REPO_ROOT / "infra" / "jobs"
FEATURE_SCHEMA = REPO_ROOT / "data_contracts" / "features" / "feature_schema.json"
BOOTSTRAP_MARKER = REPO_ROOT / "data" / "host_session" / "polaris_bootstrap.launched"
STATE_FILE = "terraform.tfstate"
MISSING = "MISSING"
REGISTRATION_SECONDS = 600
HOST_PREPARE_SECONDS = 600
BOOTSTRAP_SECONDS = 600
SERVICES_STABLE_SECONDS = 1200
SERVICE_RESTART_SECONDS = 900
TERMINATE_SECONDS = 900
POLL_SECONDS = 15
STOP_PROGRESS_SECONDS = 60
CALL_SECONDS = 180
RUN_TASK_ATTEMPTS = 3
UNCONFIRMED = "request_unconfirmed"
TASK_POLL_SECONDS = 6
FAILED_POLLS_ALLOWED = 3
DESCRIBE_SERVICES_LIMIT = 10
DESCRIBE_TASKS_LIMIT = 100
FIRST_SERVICE = "postgres"
HOST_PREPARE = "host_prepare"
POLARIS_BOOTSTRAP = "polaris_bootstrap"
OPENMETADATA_SERVICE = "openmetadata"
OPENMETADATA_BOOTSTRAP = "openmetadata_bootstrap"
CODE_FETCH = "code_fetch"
CODE_FETCH_SECONDS = 1200
PROMETHEUS_RESTORE = "prometheus_restore"
PROMETHEUS_RESTORE_SECONDS = 1800
CODE_RUN_ID = re.compile(CODE_RUN)
CODE_SHA256 = re.compile(r"[a-f0-9]{64}")
CODE_ENVIRONMENT = ("CODE_RUN_ID", "CODE_SHA256", "CODE_MODE")
CODE_IDENTIFY_SCRIPT = REPO_ROOT / "infra" / "services" / "scripts" / "code_identify.py"
CODE_IDENTITY = re.compile(rf"code_identify: identity run ({CODE_RUN}) code ([a-f0-9]{{64}})")
CODE_IDENTITY_SECONDS = 180
CODE_LOG_CONTAINER = "code_fetch"  # the container name is also the awslogs stream prefix of the code task
LOG_COMPONENT = "airflow"  # the log group component of the code one-shot (infra/services/main.tf container_component)
ONLINE_RELOAD = "online_reload"
ONLINE_RELOAD_SECONDS = 2400  # the features task: code fetch, then the load of one set (feast_job.JOB_SECONDS is 1800)
RELOAD_PHASE = "reload"
RELOAD_LOG_STREAM = "features/feast"  # the awslogs stream prefix of the features task (its layer) and the container that runs the reload
RELOAD_OUTCOMES = ("loaded", "already_loaded", "skipped")  # scripts.features.online_reload prints one line of JSON with one of these outcomes
RELOAD_OUTCOME_SECONDS = 180
RELOAD_CONTAINER = (
    "feast"  # the last container of the features task runs the reload (LAYER_PHASE reload); a task of an older revision lacks RELEASE_MODEL there
)
RELOAD_VARIABLE = "RELEASE_MODEL"
POLARIS_PORT = 8181
# The reviewed installer. Every code task carries its text as the container command, so the registered task definition, which may still hold an older
# installer, never decides how the code is installed, verified or restored.
CODE_FETCH_SCRIPT = REPO_ROOT / "infra" / "services" / "scripts" / "code_fetch.py"
OPENMETADATA_BOOTSTRAP_SECONDS = 1500
OPENMETADATA_STOP_SECONDS = 300
FAILED_ROLLOUT = "FAILED"
COMPLETED_ROLLOUT = "COMPLETED"
OTHER_REASON = "other reason"
STOPPED_REASONS = (
    ("essential container", "essential container exited"),
    ("failed to start", "task failed to start"),
    ("terminated", "host terminated"),
    ("stopped by user", "stopped by user"),
    ("user initiated", "stopped by user"),
    ("scaling activity", "scaling activity"),
)
ROLLOUT_REASONS = (("circuit breaker", "deployment circuit breaker"), ("alarm", "deployment alarm"))
RUN_TASK_CODE = re.compile(r"[A-Z][A-Z0-9_]*(:[A-Z0-9_]+)?")


class HostSessionError(Exception):
    """A step failed; the message names the step and the cause, never an ARN or an account ID."""


class TaskExitError(HostSessionError):
    """A one-shot task ran to its end and a container exited with a code other than 0. A task that never started has no exit code and is not this error."""


class TerminationRequested(Exception):
    """The process received SIGTERM or SIGHUP; it unwinds through the same cleanup as any other failure."""


@contextlib.contextmanager
def deferred_signals(report: Callable[[str, str], None]) -> Iterator[None]:
    """Hold SIGINT, SIGTERM and SIGHUP while the host is being stopped, so a second signal cannot cut the cleanup short; each one is reported."""

    def defer(number: int, _frame: object) -> None:
        report("signal_deferred", f"{signal.Signals(number).name} received; stopping the host, please wait")

    earlier = {number: signal.signal(number, defer) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    try:
        yield
    finally:
        for number, handler in earlier.items():
            signal.signal(number, handler)


def install_termination_handler() -> Callable[[], None]:
    """Turn SIGTERM and SIGHUP into ``TerminationRequested``; the returned function restores the earlier handlers."""

    def request(number: int, _frame: object) -> None:
        raise TerminationRequested(signal.Signals(number).name)

    earlier = {number: signal.signal(number, request) for number in (signal.SIGTERM, signal.SIGHUP)}

    def restore() -> None:
        for number, handler in earlier.items():
            signal.signal(number, handler)

    return restore


def age_text(seconds: float) -> str:
    """A run time for the owner: ``45 s``, ``2 min 05 s`` or ``1 h 02 min``."""
    hours, rest = divmod(max(int(seconds), 0), 3600)
    minutes, remainder = divmod(rest, 60)
    if hours:
        return f"{hours} h {minutes:02d} min"
    return f"{minutes} min {remainder:02d} s" if minutes else f"{remainder} s"


def category(reason: object, categories: tuple[tuple[str, str], ...]) -> str:
    """The fixed category of a provider reason; the reason text itself can hold identifiers and never reaches the output."""
    text = str(reason).lower()
    return next((name for needle, name in categories if needle in text), OTHER_REASON)


def result_line(message: str, model: str) -> dict[str, Any] | None:
    """The reload result of ``model`` that a log message holds: a JSON object with the set and one of ``RELOAD_OUTCOMES``, else None. The fields of its
    outcome are checked by ``complete_result``."""
    text = message.strip()
    if not text.startswith("{"):
        return None
    try:
        document = json.loads(text)
    except ValueError:
        return None
    return document if isinstance(document, dict) and document.get("feature_set") == model and document.get("outcome") in RELOAD_OUTCOMES else None


def complete_result(document: dict[str, Any]) -> bool:
    """True when a result line has the fields of its outcome: ``loaded`` and ``already_loaded`` name the run, the base run (run IDs) and the load ID,
    ``skipped`` names a reason and the held runs (a list of run IDs, empty when no run is held)."""

    def text(name: str) -> bool:
        return isinstance(document.get(name), str) and bool(document[name])

    if document["outcome"] == "skipped":
        held = document.get("skipped_held")
        return text("reason") and isinstance(held, list) and all(isinstance(run, str) and CODE_RUN_ID.fullmatch(run) for run in held)
    return text("load_id") and all(text(name) and CODE_RUN_ID.fullmatch(document[name]) for name in ("run_id", "base_run"))


def code_environment(overrides: dict[str, Any] | None, container: str) -> dict[str, str]:
    """The code environment variables (CODE_RUN_ID, CODE_SHA256) that the container overrides of a RunTask call or a described task set for ``container``."""
    containers = (overrides or {}).get("containerOverrides", [])
    return {
        item["name"]: item["value"]
        for entry in containers
        if entry.get("name") == container
        for item in entry.get("environment", [])
        if item.get("name") in CODE_ENVIRONMENT
    }


def code_command(overrides: dict[str, Any] | None, container: str) -> str | None:
    """The command text that the container overrides of a RunTask call or a described task set for ``container``, or None."""
    commands = [entry["command"][0] for entry in (overrides or {}).get("containerOverrides", []) if entry.get("name") == container and entry.get("command")]
    return commands[0] if commands else None


def run_task_code(reason: object) -> str:
    """A RunTask failure reason when it is a fixed code such as ``RESOURCE:MEMORY``, otherwise a fixed category."""
    text = str(reason).split(" ", 1)[0]
    return text if RUN_TASK_CODE.fullmatch(text) else OTHER_REASON


@dataclass(frozen=True)
class Clock:
    """The time source of every wait; a case replaces it to run time forward without sleeping. ``wall`` is the clock time in epoch seconds, which a task's
    start time is measured against."""

    now: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    wall: Callable[[], float] = time.time


@dataclass(frozen=True)
class ReloadTask:
    """The features host task of the jobs stack (output ``host_run_tasks.features``): its family and, per container, the variables a launch must fill."""

    family: str
    variables: dict[str, list[str]]


@dataclass(frozen=True)
class Names:
    """The names a session needs, read from Terraform outputs; ``services`` has the first service first.

    ``reload``, ``host_address`` and ``log_group`` are what ``start`` needs for the online store reload: the features task of the jobs stack, the host's private
    address (the Polaris endpoint the task is given) and the log group of the code one-shot (where its identity line is read). They are empty when the outputs
    are missing or the command does not read them (only ``start`` does), and ``start`` refuses by name when one is empty. ``jobs_log_group`` is the jobs log
    group of the project and environment (the name that the jobs stack requires), where the result line of a reload is read; it is empty for a command that
    does not start or when the settings name no project, and then a reload's result is reported as unknown."""

    cluster: str
    group: str
    provider: str
    project_tag: str
    services: dict[str, str]
    one_shot: dict[str, str]
    state_serial: int | None = None
    reload: ReloadTask | None = None
    host_address: str = ""
    log_group: str = ""
    jobs_log_group: str = ""


def tolerant_runner(*args: Any, **kwargs: Any) -> CompletedProcess[str]:
    """Run an AWS command in its own session, so a terminal Ctrl-C aimed at the whole foreground group cannot kill a host-off request in flight.

    A successful command with no output (``set-desired-capacity``) answers an empty JSON object.
    """
    result = run_argv(*args, own_session=True, **kwargs)
    if result.returncode == 0 and not result.stdout.strip():
        return CompletedProcess(result.args, 0, "{}", result.stderr)
    return result


def output_value(document: dict[str, Any], name: str, kind: type) -> Any:
    """One Terraform output value of the given type, or a configuration error naming the output."""
    item = document.get(name)
    value = item.get("value") if isinstance(item, dict) else None
    if not isinstance(value, kind) or not value:
        raise ConfigurationError(f"The Terraform output {name} is missing or has the wrong type.")
    return value


def services_state_serial(directory: Path) -> int | None:
    """The serial of the local services state file in ``directory``, or None when the file is absent, unreadable or has no whole-number serial."""
    try:
        document = json.loads((directory / STATE_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    serial = document.get("serial") if isinstance(document, dict) else None
    return serial if isinstance(serial, int) and not isinstance(serial, bool) else None


def limit(remaining: Callable[[], float] | None) -> dict[str, float]:
    """The ``timeout`` argument of one lookup under the deadline, or none when the caller has no deadline."""
    return {} if remaining is None else {"timeout": remaining()}


def reload_task(jobs: dict[str, Any]) -> ReloadTask | None:
    """The features host task of the jobs outputs, or None when ``host_run_tasks`` is null (host jobs are off) or has no features task with a family and its
    container variables."""
    tasks = (jobs.get("host_run_tasks") or {}).get("value")
    features = tasks.get("features") if isinstance(tasks, dict) else None
    family = features.get("task_definition") if isinstance(features, dict) else None
    variables = features.get("container_environment") if isinstance(features, dict) else None
    if not isinstance(family, str) or not family or not isinstance(variables, dict) or RELOAD_VARIABLE not in (variables.get(RELOAD_CONTAINER) or []):
        return None
    return ReloadTask(family, {str(container): [str(name) for name in names] for container, names in variables.items()})


def jobs_log_group(settings: dict[str, str]) -> str:
    """The jobs log group of the project and environment, ``<project_name>_jobs_<environment>``: the only name that the jobs stack accepts for its log group
    (infra/jobs/variables.tf, ``log_group_name``). Empty when the settings name no project or environment."""
    project, environment = settings.get("project_name", ""), settings.get("environment", "")
    return f"{project}_jobs_{environment}" if project and environment else ""


def load_names(settings: dict[str, str], remaining: Callable[[], float] | None = None, reload: bool = False) -> Names:
    """Read the platform and services outputs with the project profile; with ``reload`` also the jobs outputs (``start`` only: a stop never waits for them).

    ``remaining`` gives the seconds left under the caller's deadline. It is read before each lookup, which gets only that much time.
    """
    platform = terraform_document(PLATFORM_DIRECTORY, ["output", "-json"], settings, "The platform outputs could not be read.", **limit(remaining))
    services = terraform_document(SERVICES_DIRECTORY, ["output", "-json"], settings, "The services outputs could not be read.", **limit(remaining))
    service_names = output_value(services, "service_names", dict)
    one_shot = output_value(services, "one_shot_run_task", dict)
    ordered = {key: service_names[key] for key in sorted(service_names, key=lambda key: key != FIRST_SERVICE)}
    if FIRST_SERVICE not in ordered:
        raise ConfigurationError("The service_names output has no postgres service.")
    for key in (HOST_PREPARE, POLARIS_BOOTSTRAP):
        if key not in one_shot:
            raise ConfigurationError(f"The one_shot_run_task output has no {key} task.")
    jobs = terraform_document(JOBS_DIRECTORY, ["output", "-json"], settings, "The jobs outputs could not be read.", **limit(remaining)) if reload else {}
    address, groups = (platform.get(name, {}).get("value") for name in ("host_private_ip", "log_group_names"))
    return Names(
        cluster=output_value(platform, "cluster_name", str),
        group=output_value(platform, "host_autoscaling_group_name", str),
        provider=output_value(platform, "ec2_capacity_provider_name", str),
        project_tag=output_value(platform, "project_tag", str),
        services=ordered,
        one_shot={
            key: str(one_shot[key]["task_definition"])
            for key in (HOST_PREPARE, POLARIS_BOOTSTRAP, *(optional for optional in (OPENMETADATA_BOOTSTRAP, CODE_FETCH) if optional in one_shot))
        },
        state_serial=services_state_serial(SERVICES_DIRECTORY),
        reload=reload_task(jobs),
        host_address=address if isinstance(address, str) else "",
        log_group=groups.get(LOG_COMPONENT, "") if isinstance(groups, dict) and isinstance(groups.get(LOG_COMPONENT), str) else "",
        jobs_log_group=jobs_log_group(settings) if reload else "",
    )


class Session:
    """One host session: the AWS adapter, the names, the clock and a step report."""

    def __init__(
        self,
        cli: AwsCli,
        names: Names,
        clock: Clock | None = None,
        marker: Path = BOOTSTRAP_MARKER,
        report: Callable[[str], None] | None = None,
        job_prefix: str | None = None,
    ) -> None:
        """``job_prefix`` is the start of the job task families of the project and environment; ``refresh`` needs it to find the running job tasks."""
        self.cli, self.names, self.clock, self.marker, self.job_prefix = cli, names, clock or Clock(), marker, job_prefix
        self.deadline: float | None = None
        self.launch_tokens: dict[str, str] = {}
        self.report = report or (lambda line: sys.stdout.write(line + "\n"))

    def step(self, name: str, detail: str = "ok") -> None:
        """Report one finished step."""
        self.report(f"{name}: {detail}")

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict[str, Any]:
        """One AWS call within the remaining budget of the running wait (never more than the 180-second limit); an AWS failure becomes a session error."""
        remaining = float(CALL_SECONDS)
        if self.deadline is not None:
            remaining = min(remaining, self.deadline - self.clock.now())
            if remaining <= 0:
                raise HostSessionError(f"{service} {operation} not started: the wait deadline has passed")
        try:
            return self.cli.call_bounded(service, operation, arguments, timeout=remaining)
        except StorageError as error:
            raise HostSessionError(f"{service} {operation} failed ({error})") from None

    def wait(self, what: str, check: Callable[[], Any], seconds: int, every: int) -> Any:
        """Poll ``check`` until it returns a truthy value within ``seconds``; each call gets the remaining budget, and a success after the deadline fails."""
        end, failed, outer = self.clock.now() + seconds, 0, self.deadline
        self.deadline = end if outer is None else min(end, outer)
        try:
            while True:
                try:
                    value = check()
                    failed = 0
                except HostSessionError as error:
                    failed += 1
                    if failed >= FAILED_POLLS_ALLOWED:
                        raise HostSessionError(f"{what}: {FAILED_POLLS_ALLOWED} failed polls in a row; last: {error}") from None
                    value = None
                if value and self.clock.now() <= end:
                    return value
                if self.clock.now() >= end:
                    raise HostSessionError(f"{what} not reached within {seconds} s")
                self.clock.sleep(min(every, end - self.clock.now()))
        finally:
            self.deadline = outer

    def group_state(self) -> dict[str, Any]:
        """The host group, read only."""
        groups: list[dict[str, Any]] = self.call("autoscaling", "describe-auto-scaling-groups", ["--auto-scaling-group-names", self.names.group]).get(
            "AutoScalingGroups", []
        )
        if len(groups) != 1:
            raise HostSessionError("the host group was not found")
        return groups[0]

    def active_instances(self) -> list[dict[str, Any]]:
        """The container instances that are ACTIVE with a connected agent."""
        arns = self.call("ecs", "list-container-instances", ["--cluster", self.names.cluster]).get("containerInstanceArns", [])
        if not arns:
            return []
        described = self.call("ecs", "describe-container-instances", ["--cluster", self.names.cluster, "--container-instances", *arns])
        return [item for item in described.get("containerInstances", []) if item.get("status") == "ACTIVE" and item.get("agentConnected") is True]

    def lookup_services(self) -> tuple[list[dict[str, Any]], list[str]]:
        """The project services ECS has, as it describes them in the session's order, and the names it reports absent (a ``MISSING`` failure entry).

        A failure entry of any other reason raises. The state names a service that the apply has not created yet when an apply was interrupted.
        """
        names = list(self.names.services.values())
        found: dict[str, dict[str, Any]] = {}
        for index in range(0, len(names), DESCRIBE_SERVICES_LIMIT):
            described = self.call("ecs", "describe-services", ["--cluster", self.names.cluster, "--services", *names[index : index + DESCRIBE_SERVICES_LIMIT]])
            if any(item.get("reason") != MISSING for item in described.get("failures", [])):
                raise HostSessionError("describe-services reported failures")
            found.update({item["serviceName"]: item for item in described.get("services", [])})
        return [found[name] for name in names if name in found], [name for name in names if name not in found]

    def service_states(self) -> list[dict[str, Any]]:
        """Every project service as ECS describes it, in the session's order; a service ECS does not have raises."""
        found, absent = self.lookup_services()
        if absent:
            raise HostSessionError(f"services not found: {', '.join(absent)}")
        return found

    @staticmethod
    def settled(service: dict[str, Any]) -> bool:
        """True when the service has one COMPLETED deployment, the desired running count and no pending task; a FAILED rollout raises."""
        deployments = service.get("deployments", [])
        for deployment in deployments:
            if deployment.get("rolloutState") == FAILED_ROLLOUT:
                reason = category(deployment.get("rolloutStateReason", ""), ROLLOUT_REASONS)
                raise HostSessionError(f"service {service['serviceName']} rollout FAILED: {reason}")
        states = [deployment.get("rolloutState") for deployment in deployments]
        return states == [COMPLETED_ROLLOUT] and service.get("runningCount") == service.get("desiredCount") and not service.get("pendingCount")

    def services_stable(self, names: list[str] | None = None) -> bool:
        """True when the named services (default all) are settled."""
        wanted = set(names or self.names.services.values())
        return all(self.settled(service) for service in self.service_states() if service["serviceName"] in wanted)

    def host_on(self) -> None:
        """Set the desired capacity to 1 and wait for one ACTIVE, agent-connected container instance that belongs to the capacity provider.

        An instance can be ACTIVE before ECS lists it in its capacity provider, and RunTask on the provider then fails with "contains no container
        instances"; ``capacityProviderName`` of ``describe-container-instances`` shows the membership.
        """
        self.call("autoscaling", "set-desired-capacity", ["--auto-scaling-group-name", self.names.group, "--desired-capacity", "1", "--no-honor-cooldown"])
        self.step("host_on_requested")
        seen = [""]

        def ready() -> bool:
            active = self.active_instances()
            seen[0] = ""
            if len(active) != 1:
                seen[0] = "no single ACTIVE, agent-connected container instance"
            elif active[0].get("capacityProviderName") != self.names.provider:
                seen[0] = "the container instance is registered but is not in the capacity provider"
            return not seen[0]

        try:
            self.wait("host registration", ready, REGISTRATION_SECONDS, POLL_SECONDS)
        except HostSessionError as error:
            raise HostSessionError(f"{error}; last seen: {seen[0]}" if seen[0] else str(error)) from None
        self.step("host_registered")

    def host_off(self) -> None:
        """Set the desired capacity to 0 and verify the group has no instance left."""
        self.call("autoscaling", "set-desired-capacity", ["--auto-scaling-group-name", self.names.group, "--desired-capacity", "0", "--no-honor-cooldown"])
        self.step("host_off_requested")

        def gone() -> bool:
            group = self.group_state()
            return group.get("DesiredCapacity") == 0 and not group.get("Instances")

        self.wait("host termination", gone, TERMINATE_SECONDS, POLL_SECONDS)
        self.step("host_off_verified")

    def run_task(self, key: str, family: str, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
        """RunTask with a client token kept until a reply arrives, so a launch whose reply was lost is retried as the same launch, never a second one."""
        token = self.launch_tokens.setdefault(key, uuid.uuid4().hex)
        extra = ["--overrides", json.dumps(overrides, separators=(",", ":"), sort_keys=True)] if overrides else []
        arguments = [
            "--cluster",
            self.names.cluster,
            "--task-definition",
            family,
            "--capacity-provider-strategy",
            f"capacityProvider={self.names.provider},weight=1",
            "--propagate-tags",
            "TASK_DEFINITION",
            "--enable-ecs-managed-tags",
            "--tags",
            f"key=Project,value={self.names.project_tag}",
            "--client-token",
            token,
            *extra,
        ]
        for attempt in range(RUN_TASK_ATTEMPTS):
            try:
                launched = self.call("ecs", "run-task", arguments)
            except HostSessionError as error:
                if UNCONFIRMED not in str(error):
                    del self.launch_tokens[key]
                    raise
                self.step(key, f"RunTask reply lost (attempt {attempt + 1} of {RUN_TASK_ATTEMPTS}); retrying the same launch")
                continue
            del self.launch_tokens[key]
            return launched
        raise HostSessionError(f"{key} RunTask unconfirmed after {RUN_TASK_ATTEMPTS} attempts; the task may be running")

    def one_shot(self, key: str, seconds: int, overrides: dict[str, Any] | None = None) -> str:
        """Run ``run_one_shot`` under one session deadline, so every AWS call of the one-shot, a RunTask retry included, gets only the time left of
        ``seconds`` and is refused once it has passed. The answer is the ARN of the task that ran."""
        outer = self.deadline
        end = self.clock.now() + seconds
        self.deadline = end if outer is None else min(end, outer)
        try:
            return self.run_one_shot(key, seconds, overrides)
        except HostSessionError as error:
            # A call refused by the deadline names only the AWS operation; the owner reads which one-shot it was.
            if not str(error).startswith(key):
                raise HostSessionError(f"{key}: {error}") from None
            raise
        finally:
            self.deadline = outer

    def family_of(self, key: str) -> str:
        """The task family of a one-shot key: the features task for an online store reload, otherwise the services stack's one-shot."""
        if key.startswith(ONLINE_RELOAD):
            if self.names.reload is None:
                raise HostSessionError(f"{key}: the jobs stack has no features host task")
            return self.names.reload.family
        return self.names.one_shot[key]

    def run_one_shot(self, key: str, seconds: int, overrides: dict[str, Any] | None) -> str:
        """Run a one-shot task once and require exit code 0. The caller's session deadline bounds everything: each wait is capped by it and each AWS call
        is refused once it has passed, so the wait for an earlier task, the launch with its retries and the wait for the stop share one limit.

        A running task of the family with the same code overrides is awaited, never doubled. A running task with other code overrides is awaited until it
        stops, then the requested task launches, so two tasks of the family never run at once and the requested code is the code that is installed.
        """
        family = self.family_of(key)

        running = self.call("ecs", "list-tasks", ["--cluster", self.names.cluster, "--family", family, "--desired-status", "RUNNING"]).get("taskArns", [])
        if running and not self.same_code(key, running, overrides):
            self.step(key, "a task of this family is already running for another run or digest; waiting for it to stop, then launching the requested one")
            self.wait(f"{key} running task stop", lambda: self.all_stopped(key, running), seconds, TASK_POLL_SECONDS)
            running = []
        if running:
            task = running[0]
            self.step(key, "a task of this family is already running; waiting for it")
        else:
            launched = self.run_task(key, family, overrides)
            tasks = launched.get("tasks", [])
            if launched.get("failures") or len(tasks) != 1:
                reasons = [run_task_code(item.get("reason")) for item in launched.get("failures", [])]
                raise HostSessionError(f"{key} RunTask failures: {reasons or ['no task started']}")
            task = tasks[0]["taskArn"]

        def stopped() -> dict[str, Any] | None:
            described = self.call("ecs", "describe-tasks", ["--cluster", self.names.cluster, "--tasks", task])
            if described.get("failures"):
                raise HostSessionError(f"{key} describe-tasks failures")
            found = described.get("tasks", [])
            return found[0] if len(found) == 1 and found[0].get("lastStatus") == "STOPPED" else None

        stopped_task = self.wait(f"{key} task stop", stopped, seconds, TASK_POLL_SECONDS)
        exits = {item.get("name"): item.get("exitCode") for item in stopped_task.get("containers", [])}
        if not exits or any(code != 0 for code in exits.values()):
            message = f"{key} exit codes: {exits}; stop reason: {category(stopped_task.get('stoppedReason'), STOPPED_REASONS)}"
            # Only a container that ran to its end with a nonzero exit code proves that its script ran. A task that never started has no exit code.
            ran = bool(exits) and all(isinstance(code, int) for code in exits.values()) and stopped_task.get("stopCode") != "TaskFailedToStart"
            raise TaskExitError(message) if ran else HostSessionError(message)
        self.step(key, f"exit codes {exits}")
        return str(task)

    def described(self, key: str, arns: list[str]) -> list[dict[str, Any]]:
        """The described tasks of ``arns``; a failure entry or a missing task raises, as the wait of ``one_shot`` does."""
        found: list[dict[str, Any]] = []
        for index in range(0, len(arns), DESCRIBE_TASKS_LIMIT):
            chunk = arns[index : index + DESCRIBE_TASKS_LIMIT]
            described = self.call("ecs", "describe-tasks", ["--cluster", self.names.cluster, "--tasks", *chunk])
            if described.get("failures") or len(described.get("tasks", [])) != len(chunk):
                raise HostSessionError(f"{key} describe-tasks failures")
            found.extend(described["tasks"])
        return found

    def same_code(self, key: str, arns: list[str], overrides: dict[str, Any] | None) -> bool:
        """True when every task in ``arns`` runs with the code environment (CODE_RUN_ID, CODE_SHA256, CODE_MODE) and the command that ``overrides`` ask for."""
        if key.startswith(ONLINE_RELOAD):
            return False  # a features task of any other launch is never this reload: wait for it to stop, then launch
        wanted = (code_environment(overrides, key), code_command(overrides, key))
        return all((code_environment(task.get("overrides"), key), code_command(task.get("overrides"), key)) == wanted for task in self.described(key, arns))

    def all_stopped(self, key: str, arns: list[str]) -> bool:
        """True when every task in ``arns`` is STOPPED."""
        return all(task.get("lastStatus") == "STOPPED" for task in self.described(key, arns))

    def deploy_services(self) -> None:
        """Force a new deployment of every project service, postgres first, then wait until all are stable."""
        for key, name in self.names.services.items():
            # The catalog service is scaled to zero by stop, so its deployment also sets the count back to one.
            count = ["--desired-count", "1"] if key == OPENMETADATA_SERVICE else []
            self.call("ecs", "update-service", ["--cluster", self.names.cluster, "--service", name, *count, "--force-new-deployment"])
        self.step("services_deployment_forced", ", ".join(self.names.services.values()))
        self.wait("services stable", self.services_stable, SERVICES_STABLE_SECONDS, POLL_SECONDS)
        self.step("services_stable", ", ".join(f"{item['serviceName']}={item['runningCount']}" for item in self.service_states()))

    def restart_service(self, key: str, seconds: int = SERVICE_RESTART_SECONDS) -> None:
        """Force a new deployment of one project service and wait until every task that ran before it is STOPPED and the new deployment is COMPLETED.

        No process that served the service before the call is left when this returns, so no request that process had accepted can still complete. A failed
        call, a rollout that ends FAILED or a wait past ``seconds`` raises HostSessionError.
        """
        name = self.names.services.get(key)
        if name is None:
            raise HostSessionError(f"the service {key} is not in the session's services")
        before = self.call("ecs", "list-tasks", ["--cluster", self.names.cluster, "--service-name", name, "--desired-status", "RUNNING"]).get("taskArns", [])
        updated = self.call("ecs", "update-service", ["--cluster", self.names.cluster, "--service", name, "--force-new-deployment"])
        forced = [item for item in updated.get("service", {}).get("deployments", []) if item.get("status") == "PRIMARY" and item.get("id")]
        if len(forced) != 1:
            raise HostSessionError(f"update-service for {key} answered no single new deployment")
        self.step(f"{key}_restart_forced", f"{len(before)} task(s) to stop")
        self.wait(f"{key} restart", lambda: self.replaced(name, str(forced[0]["id"]), before), seconds, POLL_SECONDS)
        self.step(f"{key}_restarted")

    def replaced(self, name: str, deployment: str, before: list[str]) -> bool:
        """True when every task in ``before`` is STOPPED and the deployment is the only one left on the service, COMPLETED."""
        for index in range(0, len(before), DESCRIBE_TASKS_LIMIT):
            described = self.call("ecs", "describe-tasks", ["--cluster", self.names.cluster, "--tasks", *before[index : index + DESCRIBE_TASKS_LIMIT]])
            if described.get("failures"):
                raise HostSessionError("describe-tasks reported failures")
            if any(task.get("lastStatus") != "STOPPED" for task in described.get("tasks", [])):
                return False
        described = self.call("ecs", "describe-services", ["--cluster", self.names.cluster, "--services", name])
        found = described.get("services", [])
        if described.get("failures") or len(found) != 1:
            raise HostSessionError("describe-services reported failures")
        return self.settled(found[0]) and [item.get("id") for item in found[0].get("deployments", [])] == [deployment]

    def openmetadata_bootstrap(self) -> None:
        """Run the repeatable catalog bootstrap (administrator password, bot tokens) once the services are stable.

        A failure is reported and never stops the session: the data chain does not depend on the catalog.
        """
        if OPENMETADATA_BOOTSTRAP not in self.names.one_shot:
            return
        try:
            self.one_shot(OPENMETADATA_BOOTSTRAP, OPENMETADATA_BOOTSTRAP_SECONDS)
        except HostSessionError as error:
            self.step(
                "openmetadata_bootstrap_failed", f"{error}; the host stays on and the data chain is not affected; run start or refresh again to repeat it"
            )

    def service_stopped(self, name: str) -> bool:
        """True when ECS shows no running and no pending task for the named service."""
        described = self.call("ecs", "describe-services", ["--cluster", self.names.cluster, "--services", name])
        found = described.get("services", [])
        if described.get("failures") or len(found) != 1:
            raise HostSessionError("describe-services reported failures")
        return found[0].get("runningCount") == 0 and not found[0].get("pendingCount")

    def openmetadata_stop(self) -> None:
        """Scale the catalog service to zero and wait, so OpenSearch and the server close their files before the instance goes.

        A failure is reported and never holds the host on.
        """
        name = self.names.services.get(OPENMETADATA_SERVICE)
        if name is None:
            return
        try:
            self.call("ecs", "update-service", ["--cluster", self.names.cluster, "--service", name, "--desired-count", "0"])
            self.wait("catalog service stop", lambda: self.service_stopped(name), OPENMETADATA_STOP_SECONDS, POLL_SECONDS)
            self.step("openmetadata_stopped")
        except HostSessionError as error:
            self.step("openmetadata_stop_failed", f"{error}; stopping the host anyway; the index rebuilds from the database after a restore")

    def running_job_tasks(self) -> list[JobTask]:
        """The job tasks of the project's families that are not STOPPED (a task that is stopping counts), listed as ``aws_tasks list`` lists them under its
        120-second cleanup limit. A listing that fails raises. ``TaskCleanup`` is imported here because ``aws_tasks`` imports this module."""
        from scripts.infrastructure.aws_tasks import TaskCleanup

        if self.job_prefix is None:
            raise HostSessionError("the session has no job task family prefix")
        cleanup = TaskCleanup(self, self.job_prefix)
        with cleanup.bounded():
            return cleanup.discover()

    def job_task_line(self, task: JobTask) -> str:
        """One running job task for the owner: its family, the last 8 characters of its ID and how long it has run, never an ARN."""
        shown = f"{task.family} {task.suffix}" + (" (writer)" if task.writer else "")
        return f"{shown}, run time unknown" if task.started_at is None else f"{shown} running {age_text(self.clock.wall() - task.started_at)}"

    def check_no_job_tasks(self, wait_seconds: int) -> None:
        """Return when no job task of the project runs; change nothing and raise ``HostSessionError`` otherwise.

        With running tasks the check polls every ``POLL_SECONDS`` until none runs and refuses when ``wait_seconds`` have passed (0: it refuses at once). The
        deadline bounds the polling; a listing in flight at the deadline may take up to its own 120-second limit more. A listing that fails is never read as
        "none running": the check refuses at once when the deadline has passed and after ``FAILED_POLLS_ALLOWED`` failed listings in a row otherwise. An
        interrupt or SIGTERM leaves the host on and says so.
        """
        if self.job_prefix is None:
            raise HostSessionError(
                "stop refused: the session has no job task family prefix, so running job tasks cannot be checked; --force-stop skips the check"
            )
        end, failed = self.clock.now() + max(wait_seconds, 0), 0
        shown: set[str] | None = None
        reported = self.clock.now()
        try:
            while True:
                tasks: list[JobTask] | None = None
                try:
                    tasks, failed = self.running_job_tasks(), 0
                except HostSessionError as error:
                    failed += 1
                    if failed >= FAILED_POLLS_ALLOWED or self.clock.now() >= end:
                        raise HostSessionError(
                            f"stop refused: the running job tasks could not be listed ({error}); nothing was changed; --force-stop skips the check"
                        ) from None
                if tasks is not None:
                    if not tasks:
                        self.step("job_tasks", "no job task is running")
                        return
                    lines = [self.job_task_line(task) for task in tasks]
                    now = self.clock.now()
                    if now >= end:
                        for line in lines:
                            self.step("job_task", line)
                        waited = f"; still running after a wait of {wait_seconds} s" if wait_seconds else ""
                        raise HostSessionError(
                            f"stop refused: {len(tasks)} job task(s) running ({'; '.join(lines)}){waited}; wait for them (stop --wait-seconds N) or end them "
                            "(aws_tasks stop); stop --force-stop turns the host off anyway and ends them"
                        )
                    names = {task.arn for task in tasks}
                    if names != shown or now - reported >= STOP_PROGRESS_SECONDS:
                        if names != shown:
                            for line in lines:
                                self.step("job_task", line)
                        self.step("stop_waiting", f"{len(tasks)} job task(s) running; {int(end - now)} s of the wait of {wait_seconds} s left")
                        shown, reported = names, now
                self.clock.sleep(min(POLL_SECONDS, end - self.clock.now()))
        except (KeyboardInterrupt, TerminationRequested):
            self.step("stop_interrupted", "interrupted while checking for job tasks; nothing was stopped and the host is still on; run stop again")
            raise

    def stop(self, wait_seconds: int = 0, force_stop: bool = False) -> None:
        """Check that no job task runs, stop the catalog service, then turn the host off and verify it.

        The check comes first and changes nothing: while a job task runs, ``stop`` refuses, or with ``wait_seconds`` waits up to that deadline for the tasks to
        end and then refuses. ``force_stop`` skips the check and reports that it did. An interruption or SIGTERM during the check leaves the host on. After the
        check, an interruption or SIGTERM during the catalog drain still reaches the host-off step, which then holds every shutdown signal until it is verified.
        """
        if force_stop:
            self.step("stop_forced", "the running job task check is skipped by --force-stop; a job task that runs now ends with the host")
        else:
            self.check_no_job_tasks(wait_seconds)
        try:
            self.openmetadata_stop()
        finally:
            with deferred_signals(self.step):
                self.host_off()

    def stop_after_failure(self, command: str, keep_on: bool) -> None:
        """Stop and verify the host after a failed command, or leave it on for diagnosis; a failed stop is reported and the first failure still propagates.

        It never checks for running job tasks, unlike ``stop``: it is the cleanup of a command that already failed, and a refusal or a wait here would leave a
        failed host running and billed with nobody present. ``--keep-on`` is the way to keep the host; a job that a half-started host loses is resumed by
        its DAG retry, as for a job launched between the check of ``stop`` and its host-off call.
        """
        self.step(f"{command}_failed", "the host is left on" if keep_on else "stopping the host")
        if keep_on:
            self.step("host_kept_on", "left on for diagnosis; run stop when done")
            return
        try:
            with deferred_signals(self.step):
                self.host_off()
        except HostSessionError as error:
            self.step("host_off_failed", f"the host may still be on; run stop ({error})")

    def code_overrides(self, code: tuple[str, str] | None, mode: str = "install") -> dict[str, Any]:
        """The RunTask overrides of a code task: the run ID and digest of ``code`` when given, the CODE_MODE unless it is the default install and the text of
        the reviewed installer as the command."""
        environment = [] if code is None else [{"name": "CODE_RUN_ID", "value": code[0]}, {"name": "CODE_SHA256", "value": code[1]}]
        environment += [] if mode == "install" else [{"name": "CODE_MODE", "value": mode}]
        script = (CODE_IDENTIFY_SCRIPT if mode == "identify" else CODE_FETCH_SCRIPT).read_text(encoding="utf-8")
        return {"containerOverrides": [{"name": CODE_FETCH, "environment": environment, "command": [script]}]}

    def place_code(self, code: tuple[str, str] | None) -> tuple[str, str]:
        """Run the code tasks after host_prepare and answer the code run ID and digest of the checkout: with ``code`` (run ID, code digest) the install of that
        archive as /data/repo and then a separate verification that repo shows the release and the release holds every file of its archive; without it a check
        that the existing checkout is a complete release, which fails on a host that has none, and then the identity task, which reads the run ID that the
        install recorded beside the digest of the release repo shows (``read_identity``). Each task runs a reviewed script as its command. When a task ran and
        failed, a restore task makes repo show a verified checkout again, and the error says what repo shows. start stops the host before any service."""
        if CODE_FETCH not in self.names.one_shot:
            raise HostSessionError("the code_fetch one-shot is missing: apply the services stack with code_fetch_task_role_arn first")
        try:
            self.one_shot(CODE_FETCH, CODE_FETCH_SECONDS, self.code_overrides(code))
            if code is not None:
                self.one_shot(CODE_FETCH, CODE_FETCH_SECONDS, self.code_overrides(code, "verify"))
                self.step("code_verified", code[0])
        except TaskExitError as error:
            raise HostSessionError(f"{error}; {self.restore_code(code)}") from None
        except HostSessionError as error:
            raise HostSessionError(f"{error}; the code task did not run to its end, so what repo shows is not known; no service was deployed") from None
        return code if code is not None else self.read_identity()

    def restore_prometheus(self) -> None:
        """Run the Prometheus history restore one-shot (monitoring/prometheus_history.py restore): the blocks of S3 that the data volume lacks are downloaded,
        verified and placed before any service starts, so Prometheus opens them as its own. A stack with no such one-shot (the history roles are not applied)
        starts as before and says so; a restore that fails stops the start."""
        if PROMETHEUS_RESTORE not in self.names.one_shot:
            self.step("prometheus_restore_skipped", "the services stack plans no prometheus_restore one-shot, so no metric history returns")
            return
        self.one_shot(PROMETHEUS_RESTORE, PROMETHEUS_RESTORE_SECONDS)
        self.step("prometheus_restored", "the metric history of S3 is on the data volume before Prometheus starts")

    def read_identity(self) -> tuple[str, str]:
        """The code run ID and digest of the release that repo shows, from the identity task (``infra/services/scripts/code_identify.py``): it prints one
        line from the record that the install wrote beside the digest, and this reads that line from the task's log. A host with no record is refused by
        name: give --code-run and --code-sha256 once, which writes it."""
        missing = (
            "start without --code-run needs the code run record of the release that repo shows (repo_releases/<digest>/run_id, written by an install with "
            "--code-run and --code-sha256); none was found. Give --code-run and --code-sha256 once; no service was deployed"
        )
        try:
            task = self.one_shot(CODE_FETCH, CODE_FETCH_SECONDS, self.code_overrides(None, "identify"))
        except TaskExitError:
            raise HostSessionError(missing) from None
        except HostSessionError as error:
            raise HostSessionError(f"{error}; the identity task did not run to its end, so the code run is not known; no service was deployed") from None
        found = self.wait("code identity log", lambda: self.identity_line(task), CODE_IDENTITY_SECONDS, TASK_POLL_SECONDS)
        self.step("code_identity", found[0])
        return found

    def log_events(self, group: str, stream: str) -> list[dict[str, Any]] | None:
        """Every event of one log stream, read from its start to its end: CloudWatch answers in pages, some of them empty, and ``nextForwardToken`` repeats
        when no page follows. None while the stream does not exist. The reading is bounded by the deadline of the running wait, and a log that passes it
        raises, so no caller gets the first pages of a log as the whole log."""
        events: list[dict[str, Any]] = []
        token = ""
        while True:
            arguments = ["--log-group-name", group, "--log-stream-name", stream, "--start-from-head"]
            try:
                page = self.call("logs", "get-log-events", arguments + (["--next-token", token] if token else []))
            except HostSessionError as error:
                if "ResourceNotFound" in str(error):
                    return None
                raise
            events += page.get("events", [])
            following = page.get("nextForwardToken")
            if not isinstance(following, str) or not following or following == token:
                return events
            token = following

    def identity_line(self, task: str) -> tuple[str, str] | None:
        """The identity of the identity task's log: (run ID, digest), or None while the log has no such line yet."""
        stream = f"{CODE_LOG_CONTAINER}/{CODE_LOG_CONTAINER}/{task.rsplit('/', 1)[-1]}"
        events = self.log_events(self.names.log_group, stream)
        if events is None:
            return None
        found = [CODE_IDENTITY.fullmatch(str(event.get("message", "")).strip()) for event in events]
        lines = [(item.group(1), item.group(2)) for item in found if item is not None]
        return lines[-1] if lines else None

    def restore_code(self, code: tuple[str, str] | None) -> str:
        """Run the restore task after a failed code task and answer what repo shows, from the task's own exit code."""
        try:
            self.one_shot(CODE_FETCH, CODE_FETCH_SECONDS, self.code_overrides(code, "restore"))
        except TaskExitError:
            return "repo shows no verified checkout, an empty folder; no service was deployed; run start with --code-run and --code-sha256"
        except HostSessionError as error:
            return f"the check of repo after the failure did not run ({error}), so what repo shows is not known; no service was deployed"
        self.step("code_restored", "repo shows a verified checkout")
        return "repo shows a verified checkout, not the requested code; no service was deployed"

    def reload_overrides(self, model: str, code: tuple[str, str]) -> dict[str, Any]:
        """The RunTask overrides of one reload launch: every container of the features task gets the variables the jobs stack leaves empty for it. The launch
        is the feast container reloading ``model`` (LAYER_PHASE reload); every build container ends at once. A variable this tool cannot fill is refused."""
        if self.names.reload is None:
            raise HostSessionError("the jobs stack has no features host task")
        values = {
            "RUN_ID": compose_run_id("reload", new_run_hex(), 1, MODEL_NAMES[model]),
            "CODE_RUN_ID": code[0],
            "CODE_SHA256": code[1],
            "POLARIS_URI": f"http://{self.names.host_address}:{POLARIS_PORT}/api/catalog",
            "LAYER_PHASE": RELOAD_PHASE,
            "RELEASE_MODEL": model,
            "FEATURES_RUN": "",
            "INPUT_RUN_ID": "",
            "FEATURE_SET": "",
        }
        unknown = sorted({name for names in self.names.reload.variables.values() for name in names if name not in values})
        if unknown:
            raise HostSessionError(f"the features task has variables that the reload cannot fill: {', '.join(unknown)}")
        return {
            "containerOverrides": [
                {"name": container, "environment": [{"name": name, "value": values[name]} for name in names]}
                for container, names in self.names.reload.variables.items()
            ]
        }

    def reload_outcome(self, model: str, task: str) -> dict[str, Any] | None:
        """The result of a finished reload launch, from the task's log in the jobs log group: the one JSON line that ``scripts.features.online_reload`` prints
        for ``model`` (outcome ``loaded``, ``already_loaded`` or ``skipped``), with the fields of its outcome (``complete_result``). The log is read to its
        end (``log_events``), so the one line counts across every page.
        None when the log has no such line within ``RELOAD_OUTCOME_SECONDS``, which bounds the reading of the pages too (a log that never ends is
        unknown), has more than one line of a result for the set on any page, has a line without its fields or cannot be read: the task exited 0, so its
        reload finished, and only the report of its outcome is missing."""
        if not self.names.jobs_log_group:
            return None
        stream = f"{RELOAD_LOG_STREAM}/{task.rsplit('/', 1)[-1]}"

        def line() -> list[dict[str, Any]] | None:
            events = self.log_events(self.names.jobs_log_group, stream)
            found = [result_line(str(event.get("message", "")), model) for event in events or []]
            return [item for item in found if item is not None] or None

        try:
            results = self.wait(f"online store reload log of set {model}", line, RELOAD_OUTCOME_SECONDS, TASK_POLL_SECONDS)
        except HostSessionError:
            return None
        return results[0] if len(results) == 1 and complete_result(results[0]) else None

    def reload_online_store(self, code: tuple[str, str]) -> list[str]:
        """Reload the online store of every online model set: one launch of the features task per set (``online_sets``), one after the other, each a
        feast container that loads the newest run of the set with load evidence again (``scripts.features.online_reload``). The store keeps no snapshot, so
        it is empty after every start. A launch that is refused, fails or passes its deadline raises, and ``start`` then stops the host unless ``keep_on``.

        A launch that ends ``skipped`` (a held newer run on a changed schema, owner decision B, Nov 17 2026) is no failure: it loaded nothing, and the report
        names the set, the held run and the reason. A launch whose log has no single complete result line is reported as unknown, never as reloaded. Two
        closing lines keep the two apart: the sets whose result confirmed a skip serve no online features, and the sets whose outcome is unknown may not. The
        answer is the sets that are not confirmed loaded (skipped, then unknown)."""
        skipped: list[str] = []
        unknown: list[str] = []
        for model in online_sets():
            try:
                task = self.one_shot(f"{ONLINE_RELOAD}_{model}", ONLINE_RELOAD_SECONDS, self.reload_overrides(model, code))
            except HostSessionError as error:
                raise HostSessionError(f"online store reload of set {model} failed: {error}") from None
            outcome = self.reload_outcome(model, task)
            if outcome is None:
                unknown.append(model)
                self.step(
                    "online_store_reload_outcome_unknown",
                    f"{model}: the launch exited 0 but its log has no single complete result line; check the set's online store",
                )
            elif outcome["outcome"] == "skipped":
                skipped.append(model)
                self.step("online_store_reload_skipped", f"{model}: {outcome['reason']}")
            else:
                self.step("online_store_reloaded", model)
        if skipped:
            self.step(
                "online_store_not_loaded",
                f"{', '.join(skipped)}: skipped, so no online features and no online predictions from these sets' models, until a reload loads them",
            )
        if unknown:
            self.step(
                "online_store_reload_unconfirmed",
                f"{', '.join(unknown)}: the outcome of the reload is unknown, so these sets may serve no online features; check their online stores",
            )
        return skipped + unknown

    def refuse_missing_inputs(self, code: tuple[str, str] | None) -> None:
        """Refuse, before the host turns on, a start that cannot place its code or reload the online store."""
        if CODE_FETCH not in self.names.one_shot:
            raise HostSessionError(
                "--code-run needs the code_fetch one-shot; apply the services stack with code_fetch_task_role_arn first"
                if code is not None
                else "start needs the code_fetch one-shot to read the code run record; apply the services stack with code_fetch_task_role_arn first"
            )
        if self.names.reload is None:
            raise HostSessionError(
                "the jobs stack has no features host task (output host_run_tasks.features.task_definition is null or missing), so the online store cannot "
                "be reloaded; apply the jobs stack with host jobs enabled first"
            )
        if not self.names.host_address:
            raise HostSessionError("the platform output host_private_ip is missing, so the reload task has no Polaris address")
        if code is None and not self.names.log_group:
            raise HostSessionError("the platform output log_group_names has no airflow group, so the code identity cannot be read")

    def start(self, keep_on: bool = False, code: tuple[str, str] | None = None) -> None:
        """Host on, host_prepare, the code checkout, the Prometheus history restore, every service redeployed and stable, then the online store reloaded.
        Any failure, interruption or SIGTERM stops the host unless ``keep_on``. ``code`` (run ID, code digest) is the code of the reload launch too;
        without it the identity task reads it from the record of the release repo shows. start refuses before the host turns on when the stack has no
        code one-shot or no features task."""
        self.refuse_missing_inputs(code)
        succeeded = False
        try:
            self.host_on()
            self.one_shot(HOST_PREPARE, HOST_PREPARE_SECONDS)
            identity = self.place_code(code)
            self.restore_prometheus()
            self.deploy_services()
            self.reload_online_store(identity)
            self.openmetadata_bootstrap()
            succeeded = True
        finally:
            if not succeeded:
                self.stop_after_failure("start", keep_on)

    def state_report(self) -> str:
        """What the session read its names from: the serial of the services state and the number of services and one-shot tasks."""
        serial = "unknown" if self.names.state_serial is None else str(self.names.state_serial)
        return f"serial {serial}; {len(self.names.services)} service(s), {len(self.names.one_shot)} one-shot task(s)"

    def check_refreshable(self, code: tuple[str, str] | None) -> None:
        """Refuse a refresh before it changes anything: code the stack cannot place, a host that is not one registered instance in the capacity provider,
        a running job task or a service in the state that ECS does not have. ``TaskCleanup`` lists the job tasks as ``aws_tasks list`` does; it is imported
        here because ``aws_tasks`` imports this module."""
        from scripts.infrastructure.aws_tasks import TaskCleanup

        if code is not None and CODE_FETCH not in self.names.one_shot:
            raise HostSessionError("--code-run needs the code_fetch one-shot; apply the services stack with code_fetch_task_role_arn first")
        group, active = self.group_state(), self.active_instances()
        counts = f"desired capacity {group.get('DesiredCapacity')}, group instances {len(group.get('Instances', []))}, active container instances {len(active)}"
        if group.get("DesiredCapacity") != 1 or len(group.get("Instances", [])) != 1 or len(active) != 1:
            raise HostSessionError(f"refresh refused: it needs exactly one registered container instance ({counts}); run start to turn the host on")
        if active[0].get("capacityProviderName") != self.names.provider:
            raise HostSessionError("refresh refused: the container instance is registered but is not in the capacity provider; run start or wait")
        if self.job_prefix is None:
            raise HostSessionError("refresh refused: the session has no job task family prefix, so running job tasks cannot be checked")
        tasks = TaskCleanup(self, self.job_prefix).list_tasks()
        if tasks:
            shown = ", ".join(f"{task.family} {task.suffix}" for task in tasks)
            raise HostSessionError(f"refresh refused: {len(tasks)} job task(s) running ({shown}); wait for them or run aws_tasks stop")
        _, absent = self.lookup_services()
        if absent:
            raise HostSessionError(
                f"refresh refused: ECS does not have {', '.join(absent)}, which the services state names (state {self.state_report()}); "
                "apply the services stack, or run refresh from the checkout that holds the newest services state"
            )

    def refresh(self, code: tuple[str, str] | None = None) -> None:
        """New code on a host that is on: host_prepare, the code checkout, every service of the services state redeployed and stable, the catalog bootstrap.

        The host must be exactly one registered container instance and no job task may run; refresh never changes the desired capacity. A failure after
        the checks, an interruption or SIGTERM never stops the host: it is reported and the host stays on, so the owner reads what failed and runs it again.
        """
        self.step("services_state", self.state_report())
        self.check_refreshable(code)
        self.step("refresh_checked", "one registered instance, no job task running, every named service present")
        try:
            self.one_shot(HOST_PREPARE, HOST_PREPARE_SECONDS)
            self.place_code(code)
            self.deploy_services()
            self.openmetadata_bootstrap()
        except HostSessionError as error:
            self.step("refresh_failed", f"the host stays on and is not stopped; read the failure above and run refresh again ({error})")
            raise

    def bootstrap(self, again: bool = False, keep_on: bool = False) -> None:
        """Run polaris_bootstrap once, only after postgres and polaris are stable. Once begun, any failure or SIGTERM stops the host unless ``keep_on``."""
        if self.marker.exists() and not again:
            raise HostSessionError("polaris_bootstrap already ran; a second run reports that the realm exists (use --again to force)")
        gate = [self.names.services[key] for key in (FIRST_SERVICE, "polaris") if key in self.names.services]
        if len(gate) != 2 or not self.services_stable(gate):
            raise HostSessionError("postgres and polaris are not both stable; run start first")
        succeeded = False
        try:
            self.marker.parent.mkdir(parents=True, exist_ok=True)
            self.marker.write_text("launched\n", encoding="utf-8")
            self.one_shot(POLARIS_BOOTSTRAP, BOOTSTRAP_SECONDS)
            succeeded = True
        finally:
            if not succeeded:
                self.stop_after_failure("bootstrap", keep_on)

    def status(self) -> dict[str, Any]:
        """Host, container instance and per-service counts; read only."""
        group = self.group_state()
        found, absent = self.lookup_services()
        services = {
            item["serviceName"]: {
                "desired": item.get("desiredCount"),
                "running": item.get("runningCount"),
                "pending": item.get("pendingCount"),
                "rollout": [deployment.get("rolloutState") for deployment in item.get("deployments", [])],
            }
            for item in found
        }
        return {
            "absent_services": absent,
            "services_state_serial": self.names.state_serial,
            "desired_capacity": group.get("DesiredCapacity"),
            "group_instances": len(group.get("Instances", [])),
            "active_container_instances": len(self.active_instances()),
            "services": services,
        }


def online_sets() -> list[str]:
    """The model sets that have an online load (m1, m2 and m4 today), from the feature schema, so the list never lives in a second place."""
    document = json.loads(FEATURE_SCHEMA.read_text(encoding="utf-8"))
    return sorted(name for name, entry in document["model_sets"].items() if entry["online"])


def open_session(reload: bool = False) -> Session:
    """The session of the project profile, with the names read from the Terraform outputs; ``reload`` also reads the jobs outputs (the ``start`` command).
    Raises ConfigurationError."""
    settings, _ = load_configuration(REPO_ROOT / ".env")
    session = Session(AwsCli(settings, tolerant_runner), load_names(settings, reload=reload))
    session.job_prefix = f"{settings['project_name']}_{settings['environment']}_job_"
    return session


def whole_seconds(text: str) -> int:
    """A whole number of seconds that is 0 or more, or an argparse error."""
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not a whole number of seconds") from None
    if value < 0:
        raise argparse.ArgumentTypeError(f"{text!r} is below 0 seconds")
    return value


def build_parser() -> argparse.ArgumentParser:
    """The command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start", help="host on, host_prepare, the code checkout, redeploy every service, wait until stable, reload the online store")
    start.add_argument("--keep-on", action="store_true", help="on failure leave the host on for diagnosis")
    start.add_argument(
        "--code-run",
        help="run ID whose staged code archive becomes /data/repo; give it with --code-sha256 (without it start uses the run recorded for /data/repo)",
    )
    start.add_argument("--code-sha256", help="SHA-256 of that code.tar.gz (the content of its code.sha256)")
    refresh = commands.add_parser("refresh", help="new code on a running host: host_prepare, the code checkout, redeploy every service, never stops the host")
    refresh.add_argument("--code-run", help="run ID whose staged code archive becomes /data/repo; give it with --code-sha256")
    refresh.add_argument("--code-sha256", help="SHA-256 of that code.tar.gz (the content of its code.sha256)")
    bootstrap = commands.add_parser("bootstrap", help="run polaris_bootstrap once")
    bootstrap.add_argument("--again", action="store_true", help="run it again although it already ran")
    bootstrap.add_argument("--keep-on", action="store_true", help="on failure leave the host on for diagnosis")
    stop = commands.add_parser("stop", help="refuse while a job task runs, then host off and verify the instance terminated")
    drain = stop.add_mutually_exclusive_group()
    drain.add_argument(
        "--wait-seconds",
        type=whole_seconds,
        default=0,
        metavar="N",
        help="while a job task runs, poll every 15 seconds until none runs and refuse when N seconds have passed (default 0: refuse at once)",
    )
    drain.add_argument("--force-stop", action="store_true", help="skip the running job task check; the host goes off and every job task on it ends")
    commands.add_parser("status", help="host and service counts, read only")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one subcommand and return 0 on success, 1 on a failed, interrupted or terminated step, 2 on a configuration error."""
    arguments = build_parser().parse_args(argv)
    code = None
    if arguments.command in {"start", "refresh"} and (arguments.code_run is not None or arguments.code_sha256 is not None):
        if not CODE_RUN_ID.fullmatch(arguments.code_run or "") or not CODE_SHA256.fullmatch(arguments.code_sha256 or ""):
            sys.stderr.write("usage: give --code-run <run ID> and --code-sha256 <64 lowercase hex characters> together\n")
            return 2
        code = (arguments.code_run, arguments.code_sha256)
    try:
        session = open_session(arguments.command == "start")
    except ConfigurationError as error:
        sys.stderr.write(f"configuration: {error}\n")
        return 2
    restore = install_termination_handler()
    try:
        if arguments.command == "start":
            session.start(arguments.keep_on, code=code)
        elif arguments.command == "refresh":
            session.refresh(code=code)
        elif arguments.command == "bootstrap":
            session.bootstrap(arguments.again, arguments.keep_on)
        elif arguments.command == "stop":
            session.stop(arguments.wait_seconds, arguments.force_stop)
        else:
            sys.stdout.write(json.dumps(session.status(), indent=2, sort_keys=True) + "\n")
    except (HostSessionError, TerminationRequested, KeyboardInterrupt) as error:
        sys.stderr.write(f"{arguments.command} failed: {type(error).__name__} {error}\n")
        return 1
    finally:
        restore()
    return 0


if __name__ == "__main__":
    # ``python -m`` loads this file as ``__main__``, and ``aws_tasks`` imports the module by its package name: a second copy with classes of its own, so its
    # ``HostSessionError`` is not the one that ``main`` catches. Run the package copy, so that every module shares one set of classes.
    from scripts.infrastructure.host_session import main as packaged_main

    raise SystemExit(packaged_main())
