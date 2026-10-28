"""Start and stop the AWS host control plane for a working session: ``start``, ``bootstrap``, ``stop`` and ``status``.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.host_session start``. The profile and region come from ``.env`` through
``load_configuration``. The cluster, the host group, the capacity provider, the service names and the one-shot task families come from the
``terraform output -json`` of the platform and services stacks. Every AWS call goes through ``AwsCli`` with the project profile. Every wait has a
deadline and fails visibly. Output names no account ID and no ARN.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable
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
TASK_POLL_SECONDS = 6
FAILED_POLLS_ALLOWED = 3
DESCRIBE_SERVICES_LIMIT = 10
FIRST_SERVICE = "postgres"
HOST_PREPARE = "host_prepare"
POLARIS_BOOTSTRAP = "polaris_bootstrap"
FAILED_ROLLOUT = "FAILED"
COMPLETED_ROLLOUT = "COMPLETED"


class HostSessionError(Exception):
    """A step failed; the message names the step and the cause, never an ARN or an account ID."""


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
        self.report = report or (lambda line: sys.stdout.write(line + "\n"))

    def step(self, name: str, detail: str = "ok") -> None:
        """Report one finished step."""
        self.report(f"{name}: {detail}")

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict[str, Any]:
        """One AWS call; an AWS failure becomes a session error naming the operation and the error code."""
        try:
            return self.cli.call(service, operation, arguments)
        except StorageError as error:
            raise HostSessionError(f"{service} {operation} failed ({error})") from None

    def wait(self, what: str, check: Callable[[], Any], seconds: int, every: int) -> Any:
        """Poll ``check`` until it returns a truthy value; fail at the deadline or after repeated failed polls."""
        end, failed = self.clock.now() + seconds, 0
        while True:
            try:
                value = check()
                failed = 0
            except HostSessionError as error:
                failed += 1
                if failed >= FAILED_POLLS_ALLOWED:
                    raise HostSessionError(f"{what}: {FAILED_POLLS_ALLOWED} failed polls in a row; last: {error}") from None
                value = None
            if value:
                return value
            if self.clock.now() >= end:
                raise HostSessionError(f"{what} not reached within {seconds} s")
            self.clock.sleep(every)

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
                raise HostSessionError(f"service {service['serviceName']} rollout FAILED: {deployment.get('rolloutStateReason', 'no reason given')}")
        states = [deployment.get("rolloutState") for deployment in deployments]
        return states == [COMPLETED_ROLLOUT] and service.get("runningCount") == service.get("desiredCount") and not service.get("pendingCount")

    def services_stable(self, names: list[str] | None = None) -> bool:
        """True when the named services (default all) are settled."""
        wanted = set(names or self.names.services.values())
        return all(self.settled(service) for service in self.service_states() if service["serviceName"] in wanted)

    def host_on(self) -> None:
        """Set the desired capacity to 1 and wait for one ACTIVE, agent-connected container instance."""
        self.call("autoscaling", "set-desired-capacity", ["--auto-scaling-group-name", self.names.group, "--desired-capacity", "1", "--no-honor-cooldown"])
        self.step("host_on_requested")
        self.wait("host registration", lambda: len(self.active_instances()) == 1, REGISTRATION_SECONDS, POLL_SECONDS)
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

    def one_shot(self, key: str, seconds: int) -> None:
        """Run a one-shot task once (a running task of the same family is awaited, never doubled) and require exit code 0."""
        family = self.names.one_shot[key]
        running = self.call("ecs", "list-tasks", ["--cluster", self.names.cluster, "--family", family, "--desired-status", "RUNNING"]).get("taskArns", [])
        if running:
            task = running[0]
            self.step(key, "a task of this family is already running; waiting for it")
        else:
            launched = self.call(
                "ecs",
                "run-task",
                [
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
                ],
            )
            tasks = launched.get("tasks", [])
            if launched.get("failures") or len(tasks) != 1:
                reasons = [str(item.get("reason")) for item in launched.get("failures", [])]
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
            raise HostSessionError(f"{key} exit codes: {exits}; stop reason: {stopped_task.get('stoppedReason')}")
        self.step(key, f"exit codes {exits}")

    def deploy_services(self) -> None:
        """Force a new deployment of every project service, postgres first, then wait until all are stable."""
        for name in self.names.services.values():
            self.call("ecs", "update-service", ["--cluster", self.names.cluster, "--service", name, "--force-new-deployment"])
        self.step("services_deployment_forced", ", ".join(self.names.services.values()))
        self.wait("services stable", self.services_stable, SERVICES_STABLE_SECONDS, POLL_SECONDS)
        self.step("services_stable", ", ".join(f"{item['serviceName']}={item['runningCount']}" for item in self.service_states()))

    def start(self, keep_on: bool = False) -> None:
        """Host on, host_prepare, every service redeployed and stable. Any failure stops the host unless ``keep_on``."""
        try:
            self.host_on()
            self.one_shot(HOST_PREPARE, HOST_PREPARE_SECONDS)
            self.deploy_services()
        except BaseException as error:
            self.step("start_failed", f"{type(error).__name__}: {error}")
            if keep_on:
                self.step("host_kept_on", "left on for diagnosis; run stop when done")
            else:
                self.host_off()
            raise

    def bootstrap(self, again: bool = False) -> None:
        """Run polaris_bootstrap once, only after postgres and polaris are stable."""
        if self.marker.exists() and not again:
            raise HostSessionError("polaris_bootstrap already ran; a second run reports that the realm exists (use --again to force)")
        gate = [self.names.services[key] for key in (FIRST_SERVICE, "polaris") if key in self.names.services]
        if len(gate) != 2 or not self.services_stable(gate):
            raise HostSessionError("postgres and polaris are not both stable; run start first")
        self.marker.parent.mkdir(parents=True, exist_ok=True)
        self.marker.write_text("launched\n", encoding="utf-8")
        self.one_shot(POLARIS_BOOTSTRAP, BOOTSTRAP_SECONDS)

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
    commands.add_parser("stop", help="host off and verify the instance terminated")
    commands.add_parser("status", help="host and service counts, read only")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one subcommand and return 0 on success, 1 on a failed step, 2 on a configuration error."""
    arguments = build_parser().parse_args(argv)
    try:
        settings, _ = load_configuration(REPO_ROOT / ".env")
        session = Session(AwsCli(settings, tolerant_runner), load_names(settings))
    except ConfigurationError as error:
        sys.stderr.write(f"configuration: {error}\n")
        return 2
    try:
        if arguments.command == "start":
            session.start(arguments.keep_on)
        elif arguments.command == "bootstrap":
            session.bootstrap(arguments.again)
        elif arguments.command == "stop":
            session.host_off()
        else:
            sys.stdout.write(json.dumps(session.status(), indent=2, sort_keys=True) + "\n")
    except (HostSessionError, KeyboardInterrupt) as error:
        sys.stderr.write(f"{arguments.command} failed: {type(error).__name__} {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
