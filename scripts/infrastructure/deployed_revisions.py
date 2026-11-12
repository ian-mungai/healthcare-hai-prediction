"""Check which task definition revisions the project services really run, before a step that depends on them.

    .venv/bin/python -m scripts.infrastructure.deployed_revisions --volume code_jobs_table=/config/orchestration/job_catalog.json \
        --in airflow_scheduler --forbid-secret BROKER_TOKEN

The latest registered revision of a family says nothing about what runs: a failed or rolled-back deployment leaves the service on an older revision, and a
replacement task of that service then starts from the older definition. So the check follows the deployment. For every project service it reads the
service (one PRIMARY deployment, rollout COMPLETED, the desired count running, nothing pending), every running task (each on the deployment's revision) and
the exact revision the deployment names (its volumes and the secrets of its containers). A service named by ``--in`` must carry each ``--volume`` with a
source path that ends in the given text. A ``--forbid-secret`` name or a ``--forbid-suffix`` source path in any deployed revision is a problem. Anything
that cannot be read is a problem too, never a pass. The report names services and revision numbers only: no ARN, no account ID, no parameter name.
Exit 0 when there is no problem, 1 when there is one, 2 for a usage or configuration error.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from scripts.infrastructure.host_session import HostSessionError, Session, open_session
from scripts.infrastructure.render_project_config import ConfigurationError

DESCRIBE_SERVICES_LIMIT = 10


@dataclass(frozen=True)
class Rules:
    """What a deployed revision must carry and must not carry."""

    volumes: dict[str, str] = field(default_factory=dict)
    required_in: tuple[str, ...] = ()
    forbid_secrets: tuple[str, ...] = ()
    forbid_suffixes: tuple[str, ...] = ()


def revision_of(arn: str) -> str:
    """The revision number at the end of a task definition ARN."""
    return arn.rsplit(":", 1)[-1] if ":" in arn else "unknown"


def service_problems(key: str, service: dict[str, Any], tasks: list[dict[str, Any]], definition: dict[str, Any], rules: Rules) -> list[str]:
    """The problems of one service: its deployment, its running tasks and the deployed definition."""
    found: list[str] = []
    deployments = service.get("deployments", [])
    if len(deployments) != 1 or deployments[0].get("status") != "PRIMARY" or deployments[0].get("rolloutState") != "COMPLETED":
        states = [f"{item.get('status')}/{item.get('rolloutState')}" for item in deployments]
        return [f"{key}: the deployment is not steady ({', '.join(states) or 'none'})"]
    arn = str(deployments[0].get("taskDefinition", ""))
    revision = revision_of(arn)
    desired = service.get("desiredCount")
    if service.get("runningCount") != desired or service.get("pendingCount"):
        found.append(f"{key}: revision {revision} runs {service.get('runningCount')} of {desired} tasks with {service.get('pendingCount')} pending")
    if len(tasks) != desired:
        found.append(f"{key}: {len(tasks)} running tasks listed for {desired} desired")
    for task in tasks:
        if task.get("lastStatus") != "RUNNING" or task.get("taskDefinitionArn") != arn:
            found.append(f"{key}: a task runs revision {revision_of(str(task.get('taskDefinitionArn', '')))}, not the deployed revision {revision}")
    sources = {item.get("name"): str(item.get("host", {}).get("sourcePath", "")) for item in definition.get("volumes", [])}
    for volume, suffix in rules.volumes.items():
        if volume not in sources:
            if key in rules.required_in:
                found.append(f"{key}: revision {revision} has no volume {volume}")
        elif not sources[volume].endswith(suffix):
            found.append(f"{key}: revision {revision} mounts {volume} from a path that does not end in {suffix}")
    for volume, source in sources.items():
        if any(source.endswith(suffix) for suffix in rules.forbid_suffixes):
            found.append(f"{key}: revision {revision} still mounts {volume} from a forbidden path")
    secrets = {str(secret.get("name")) for container in definition.get("containerDefinitions", []) for secret in container.get("secrets", [])}
    found.extend(f"{key}: revision {revision} injects {name}" for name in rules.forbid_secrets if name in secrets)
    return found


def deployed_problems(session: Session, rules: Rules | dict[str, Any]) -> list[str]:
    """Every problem of every project service, or the reason the state could not be read."""
    wanted = (
        rules
        if isinstance(rules, Rules)
        else Rules(
            volumes=dict(rules.get("volumes", {})),
            required_in=tuple(rules.get("required_in", ())),
            forbid_secrets=tuple(rules.get("forbid_secrets", ())),
            forbid_suffixes=tuple(rules.get("forbid_suffixes", ())),
        )
    )
    names = session.names
    by_name = {name: key for key, name in names.services.items()}
    problems: list[str] = []
    try:
        described: dict[str, dict[str, Any]] = {}
        listed = list(by_name)
        for index in range(0, len(listed), DESCRIBE_SERVICES_LIMIT):
            answer = session.call("ecs", "describe-services", ["--cluster", names.cluster, "--services", *listed[index : index + DESCRIBE_SERVICES_LIMIT]])
            if answer.get("failures"):
                return ["describe-services reported failures: nothing was checked"]
            described.update({item["serviceName"]: item for item in answer.get("services", [])})
        for name, key in by_name.items():
            if name not in described:
                problems.append(f"{key}: the service was not found")
                continue
            service = described[name]
            arns = session.call("ecs", "list-tasks", ["--cluster", names.cluster, "--service-name", name, "--desired-status", "RUNNING"]).get("taskArns", [])
            tasks: list[dict[str, Any]] = []
            if arns:
                answer = session.call("ecs", "describe-tasks", ["--cluster", names.cluster, "--tasks", *arns])
                if answer.get("failures") or len(answer.get("tasks", [])) != len(arns):
                    problems.append(f"{key}: the running tasks could not all be read")
                    continue
                tasks = answer["tasks"]
            deployments = service.get("deployments", [])
            definition: dict[str, Any] = {}
            if len(deployments) == 1 and deployments[0].get("taskDefinition"):
                definition = session.call("ecs", "describe-task-definition", ["--task-definition", str(deployments[0]["taskDefinition"])]).get(
                    "taskDefinition", {}
                )
                if not definition:
                    problems.append(f"{key}: the deployed definition could not be read")
                    continue
            problems.extend(service_problems(key, service, tasks, definition, wanted))
    except HostSessionError as error:
        problems.append(f"unreadable: {error}")
    return problems


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--volume", action="append", default=[], metavar="NAME=SUFFIX", help="a volume the services named by --in must mount from a path ending in SUFFIX"
    )
    parser.add_argument("--in", dest="required_in", action="append", default=[], metavar="SERVICE_KEY", help="a service key that must carry every --volume")
    parser.add_argument("--forbid-secret", action="append", default=[], metavar="NAME", help="a secret name no deployed revision may inject")
    parser.add_argument("--forbid-suffix", action="append", default=[], metavar="TEXT", help="a source path ending no deployed volume may have")
    return parser


def main(argv: list[str] | None = None, factory: Callable[[], Session] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    volumes: dict[str, str] = {}
    for item in arguments.volume:
        name, separator, suffix = item.partition("=")
        if not separator or not name or not suffix:
            sys.stderr.write("usage: --volume NAME=SUFFIX\n")
            return 2
        volumes[name] = suffix
    rules = Rules(volumes, tuple(arguments.required_in), tuple(arguments.forbid_secret), tuple(arguments.forbid_suffix))
    try:
        session = (factory or open_session)()
    except ConfigurationError as error:
        sys.stderr.write(f"configuration: {error}\n")
        return 2
    problems = deployed_problems(session, rules)
    for line in problems:
        sys.stdout.write(f"PROBLEM {line}\n")
    sys.stdout.write("deployed revisions: " + ("no problem found\n" if not problems else f"{len(problems)} problem(s); do not go on\n"))
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
