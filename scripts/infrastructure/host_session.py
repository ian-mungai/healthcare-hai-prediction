"""Start and stop the AWS host control plane for a working session: ``start``, ``bootstrap``, ``stop`` and ``status``.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.host_session start``. The profile and region come from ``.env`` through
``load_configuration``. The cluster, the host group, the capacity provider, the service names and the one-shot task families come from the
``terraform output -json`` of the platform and services stacks. Every AWS call goes through ``AwsCli`` with the project profile. Every wait has a
deadline and fails visibly. Output names no account ID and no ARN, and shows a provider reason only as a fixed category.
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
from typing import Any

from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.infrastructure.render_platform_config import PLATFORM_DIRECTORY, terraform_document
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration
from scripts.process import CompletedProcess

SERVICES_DIRECTORY = REPO_ROOT / "infra" / "services"
BOOTSTRAP_MARKER = REPO_ROOT / "data" / "host_session" / "polaris_bootstrap.launched"
REGISTRATION_SECONDS = 600
HOST_PREPARE_SECONDS = 600
BOOTSTRAP_SECONDS = 600
SERVICES_STABLE_SECONDS = 1200
TERMINATE_SECONDS = 900
POLL_SECONDS = 15
CALL_SECONDS = 180
RUN_TASK_ATTEMPTS = 3
UNCONFIRMED = "request_unconfirmed"
TASK_POLL_SECONDS = 6
FAILED_POLLS_ALLOWED = 3
DESCRIBE_SERVICES_LIMIT = 10
FIRST_SERVICE = "postgres"
HOST_PREPARE = "host_prepare"
POLARIS_BOOTSTRAP = "polaris_bootstrap"
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


def category(reason: object, categories: tuple[tuple[str, str], ...]) -> str:
    """The fixed category of a provider reason; the reason text itself can hold identifiers and never reaches the output."""
    text = str(reason).lower()
    return next((name for needle, name in categories if needle in text), OTHER_REASON)


def run_task_code(reason: object) -> str:
    """A RunTask failure reason when it is a fixed code such as ``RESOURCE:MEMORY``, otherwise a fixed category."""
    text = str(reason).split(" ", 1)[0]
    return text if RUN_TASK_CODE.fullmatch(text) else OTHER_REASON


@dataclass(frozen=True)
class Clock:
    """The time source of every wait; a case replaces it to run time forward without sleeping."""

    now: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep


@dataclass(frozen=True)
class Names:
    """The names a session needs, read from Terraform outputs; ``services`` has the first service first."""

    cluster: str
    group: str
    provider: str
    project_tag: str
    services: dict[str, str]
    one_shot: dict[str, str]


def tolerant_runner(*args: Any, **kwargs: Any) -> CompletedProcess[str]:
    """Run an AWS command; a successful command with no output (``set-desired-capacity``) answers an empty JSON object."""
    result = run_argv(*args, **kwargs)
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


def load_names(settings: dict[str, str]) -> Names:
    """Read the platform and services outputs with the project profile."""
    platform = terraform_document(PLATFORM_DIRECTORY, ["output", "-json"], settings, "The platform outputs could not be read.")
    services = terraform_document(SERVICES_DIRECTORY, ["output", "-json"], settings, "The services outputs could not be read.")
    service_names = output_value(services, "service_names", dict)
    one_shot = output_value(services, "one_shot_run_task", dict)
    ordered = {key: service_names[key] for key in sorted(service_names, key=lambda key: key != FIRST_SERVICE)}
    if FIRST_SERVICE not in ordered:
        raise ConfigurationError("The service_names output has no postgres service.")
    for key in (HOST_PREPARE, POLARIS_BOOTSTRAP):
        if key not in one_shot:
            raise ConfigurationError(f"The one_shot_run_task output has no {key} task.")
    return Names(
        cluster=output_value(platform, "cluster_name", str),
        group=output_value(platform, "host_autoscaling_group_name", str),
        provider=output_value(platform, "ec2_capacity_provider_name", str),
        project_tag=output_value(platform, "project_tag", str),
        services=ordered,
        one_shot={key: str(one_shot[key]["task_definition"]) for key in (HOST_PREPARE, POLARIS_BOOTSTRAP)},
    )


class Session:
    """One host session: the AWS adapter, the names, the clock and a step report."""

    def __init__(
        self, cli: AwsCli, names: Names, clock: Clock | None = None, marker: Path = BOOTSTRAP_MARKER, report: Callable[[str], None] | None = None
    ) -> None:
        self.cli, self.names, self.clock, self.marker = cli, names, clock or Clock(), marker
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

    def service_states(self) -> list[dict[str, Any]]:
        """Every project service as ECS describes it, in the session's order."""
        names = list(self.names.services.values())
        found: dict[str, dict[str, Any]] = {}
        for index in range(0, len(names), DESCRIBE_SERVICES_LIMIT):
            described = self.call("ecs", "describe-services", ["--cluster", self.names.cluster, "--services", *names[index : index + DESCRIBE_SERVICES_LIMIT]])
            if described.get("failures"):
                raise HostSessionError("describe-services reported failures")
            found.update({item["serviceName"]: item for item in described.get("services", [])})
        missing = [name for name in names if name not in found]
        if missing:
            raise HostSessionError(f"services not found: {', '.join(missing)}")
        return [found[name] for name in names]

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

    def run_task(self, key: str, family: str) -> dict[str, Any]:
        """RunTask with a client token kept until a reply arrives, so a launch whose reply was lost is retried as the same launch, never a second one."""
        token = self.launch_tokens.setdefault(key, uuid.uuid4().hex)
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

    def one_shot(self, key: str, seconds: int) -> None:
        """Run a one-shot task once (a running task of the same family is awaited, never doubled) and require exit code 0."""
        family = self.names.one_shot[key]
        running = self.call("ecs", "list-tasks", ["--cluster", self.names.cluster, "--family", family, "--desired-status", "RUNNING"]).get("taskArns", [])
        if running:
            task = running[0]
            self.step(key, "a task of this family is already running; waiting for it")
        else:
            launched = self.run_task(key, family)
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
            raise HostSessionError(f"{key} exit codes: {exits}; stop reason: {category(stopped_task.get('stoppedReason'), STOPPED_REASONS)}")
        self.step(key, f"exit codes {exits}")

    def deploy_services(self) -> None:
        """Force a new deployment of every project service, postgres first, then wait until all are stable."""
        for name in self.names.services.values():
            self.call("ecs", "update-service", ["--cluster", self.names.cluster, "--service", name, "--force-new-deployment"])
        self.step("services_deployment_forced", ", ".join(self.names.services.values()))
        self.wait("services stable", self.services_stable, SERVICES_STABLE_SECONDS, POLL_SECONDS)
        self.step("services_stable", ", ".join(f"{item['serviceName']}={item['runningCount']}" for item in self.service_states()))

    def stop_after_failure(self, command: str, keep_on: bool) -> None:
        """Stop and verify the host after a failed command, or leave it on for diagnosis; a failed stop is reported and the first failure still propagates."""
        self.step(f"{command}_failed", "the host is left on" if keep_on else "stopping the host")
        if keep_on:
            self.step("host_kept_on", "left on for diagnosis; run stop when done")
            return
        try:
            with deferred_signals(self.step):
                self.host_off()
        except HostSessionError as error:
            self.step("host_off_failed", f"the host may still be on; run stop ({error})")

    def start(self, keep_on: bool = False) -> None:
        """Host on, host_prepare, every service redeployed and stable. Any failure, interruption or SIGTERM stops the host unless ``keep_on``."""
        succeeded = False
        try:
            self.host_on()
            self.one_shot(HOST_PREPARE, HOST_PREPARE_SECONDS)
            self.deploy_services()
            succeeded = True
        finally:
            if not succeeded:
                self.stop_after_failure("start", keep_on)

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
        services = {
            item["serviceName"]: {
                "desired": item.get("desiredCount"),
                "running": item.get("runningCount"),
                "pending": item.get("pendingCount"),
                "rollout": [deployment.get("rolloutState") for deployment in item.get("deployments", [])],
            }
            for item in self.service_states()
        }
        return {
            "desired_capacity": group.get("DesiredCapacity"),
            "group_instances": len(group.get("Instances", [])),
            "active_container_instances": len(self.active_instances()),
            "services": services,
        }


def build_parser() -> argparse.ArgumentParser:
    """The command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start", help="host on, host_prepare, redeploy every service, wait until stable")
    start.add_argument("--keep-on", action="store_true", help="on failure leave the host on for diagnosis")
    bootstrap = commands.add_parser("bootstrap", help="run polaris_bootstrap once")
    bootstrap.add_argument("--again", action="store_true", help="run it again although it already ran")
    bootstrap.add_argument("--keep-on", action="store_true", help="on failure leave the host on for diagnosis")
    commands.add_parser("stop", help="host off and verify the instance terminated")
    commands.add_parser("status", help="host and service counts, read only")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one subcommand and return 0 on success, 1 on a failed, interrupted or terminated step, 2 on a configuration error."""
    arguments = build_parser().parse_args(argv)
    try:
        settings, _ = load_configuration(REPO_ROOT / ".env")
        session = Session(AwsCli(settings, tolerant_runner), load_names(settings))
    except ConfigurationError as error:
        sys.stderr.write(f"configuration: {error}\n")
        return 2
    restore = install_termination_handler()
    try:
        if arguments.command == "start":
            session.start(arguments.keep_on)
        elif arguments.command == "bootstrap":
            session.bootstrap(arguments.again, arguments.keep_on)
        elif arguments.command == "stop":
            session.host_off()
        else:
            sys.stdout.write(json.dumps(session.status(), indent=2, sort_keys=True) + "\n")
    except (HostSessionError, TerminationRequested, KeyboardInterrupt) as error:
        sys.stderr.write(f"{arguments.command} failed: {type(error).__name__} {error}\n")
        return 1
    finally:
        restore()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
