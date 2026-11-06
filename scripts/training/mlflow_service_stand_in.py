"""An in-process stand-in for the ECS calls that restart the MLflow service (``host_session.Session.restart_service``), for offline end-to-end cases.

It is the ``runner`` of ``AwsCli``: it receives the command line and answers with the exit code and JSON the command would. Time is virtual: the session's
clock reads ``now`` and its sleep calls ``advance``. A forced deployment stops the running task, starts a new one and completes. A task that stops ends the
server process, so the alias requests the tracking stand-in still holds as pending never commit. ``faults`` names what goes wrong: ``update_error`` (the
update-service call is refused), ``never_stop`` (the old task never leaves RUNNING) and ``rollout_failed`` (the new deployment ends FAILED).
"""

from __future__ import annotations

import json
from typing import Any

from scripts.infrastructure.host_session_stand_in import PROFILE, REGION, StandInError, parse
from scripts.process import CompletedProcess

CLUSTER = "stand-in-cluster"
SERVICE = "stand-in-mlflow"
STOP_SECONDS = 30
START_SECONDS = 45


class MlflowService:
    """One ECS service with one task. ``tracking`` is the registry stand-in whose pending requests end with the task."""

    def __init__(self, tracking: Any, faults: set[str] | None = None) -> None:
        self.tracking, self.faults = tracking, set(faults or ())
        self.now = 0.0
        self.serial = 1
        self.tasks: dict[str, dict[str, Any]] = {self.arn(1): {"status": "RUNNING", "desired": "RUNNING", "stops_at": None}}
        self.deployments: list[dict[str, Any]] = [{"id": "deployment-1", "status": "PRIMARY", "state": "COMPLETED", "ready_at": 0.0}]
        self.restarts = 0
        self.calls: list[str] = []

    @staticmethod
    def arn(number: int) -> str:
        return f"arn:aws:ecs:stand-in:account:task/{CLUSTER}/{number:032x}"

    @property
    def restart_done(self) -> bool:
        """True when a forced deployment has replaced every earlier task and is the only deployment left, COMPLETED."""
        return self.restarts > 0 and len(self.deployments) == 1 and self.deployments[0]["state"] == "COMPLETED"

    def advance(self, seconds: float) -> None:
        """Move virtual time forward and settle what completes."""
        self.now += seconds
        for task in self.tasks.values():
            if task["status"] == "RUNNING" and task["stops_at"] is not None and self.now >= task["stops_at"]:
                task["status"] = "STOPPED"
                self.tracking.drop_pending()
        newest = self.deployments[0]
        if newest["state"] == "IN_PROGRESS" and self.now >= newest["ready_at"] and all(task["status"] == "STOPPED" for task in self.tasks.values()):
            self.serial += 1
            self.tasks[self.arn(self.serial)] = {"status": "RUNNING", "desired": "RUNNING", "stops_at": None}
            newest["state"] = "COMPLETED"
            self.deployments = [newest]

    def runner(self, argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        """The ``AwsCli`` runner: answer one command line."""
        service, operation, options = argv[1], argv[2], parse(argv)
        self.calls.append(operation)
        if options.get("--profile") != [PROFILE] or options.get("--region") != [REGION] or options.get("--cluster") != [CLUSTER]:
            return CompletedProcess(argv, 255, "", "wrong profile, region or cluster")
        handler = {
            ("ecs", "list-tasks"): self.list_tasks,
            ("ecs", "describe-tasks"): self.describe_tasks,
            ("ecs", "describe-services"): self.describe_services,
            ("ecs", "update-service"): self.update_service,
        }.get((service, operation))
        if handler is None:
            return CompletedProcess(argv, 252, "", "unknown command")
        try:
            return CompletedProcess(argv, 0, json.dumps(handler(options)), "")
        except StandInError as error:
            return CompletedProcess(argv, 254, "", f"\nAn error occurred ({error.code}) when calling the {operation} operation\n")

    def list_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if options.get("--service-name") != [SERVICE]:
            raise StandInError("ServiceNotFoundException")
        desired = options.get("--desired-status", ["RUNNING"])[0]
        return {"taskArns": [arn for arn, task in self.tasks.items() if task["desired"] == desired and (desired == "STOPPED" or task["status"] != "STOPPED")]}

    def describe_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        found = [
            {"taskArn": arn, "lastStatus": self.tasks[arn]["status"], "desiredStatus": self.tasks[arn]["desired"]}
            for arn in options["--tasks"]
            if arn in self.tasks
        ]
        missing = [{"arn": arn, "reason": "MISSING"} for arn in options["--tasks"] if arn not in self.tasks]
        return {"tasks": found, "failures": missing}

    def describe_services(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if options.get("--services") != [SERVICE]:
            return {"services": [], "failures": [{"arn": name, "reason": "MISSING"} for name in options.get("--services", [])]}
        running = sum(task["status"] == "RUNNING" for task in self.tasks.values())
        return {
            "services": [{"serviceName": SERVICE, "desiredCount": 1, "runningCount": running, "pendingCount": 0, "deployments": self.deployment_records()}],
            "failures": [],
        }

    def deployment_records(self) -> list[dict[str, Any]]:
        return [
            {"id": item["id"], "status": item["status"], "rolloutState": item["state"], "rolloutStateReason": "ECS deployment circuit breaker"}
            for item in self.deployments
        ]

    def update_service(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if "update_error" in self.faults:
            raise StandInError("AccessDeniedException")
        if options.get("--service") != [SERVICE]:
            raise StandInError("ServiceNotFoundException")
        self.restarts += 1
        for task in self.tasks.values():
            if task["status"] == "RUNNING" and task["desired"] == "RUNNING":
                task["desired"] = "STOPPED"
                task["stops_at"] = None if "never_stop" in self.faults else self.now + STOP_SECONDS
        for item in self.deployments:
            item["status"] = "ACTIVE"
        state = "FAILED" if "rollout_failed" in self.faults else "IN_PROGRESS"
        self.deployments.insert(
            0, {"id": f"deployment-{self.restarts + 1}", "status": "PRIMARY", "state": state, "ready_at": self.now + STOP_SECONDS + START_SECONDS}
        )
        return {"service": {"serviceName": SERVICE, "deployments": self.deployment_records()}}
