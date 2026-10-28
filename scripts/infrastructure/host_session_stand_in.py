"""An in-process stand-in for the AWS CLI calls of ``host_session`` (ECS and Auto Scaling), for offline end-to-end cases; it never leaves the machine.

It is the ``runner`` of ``AwsCli``: it receives the command line and answers with the exit code and JSON the command would. Time is virtual: the
session's clock reads ``now`` and its sleep calls ``advance``, which moves the host, the tasks and the services forward. The stand-in models the
incident of 2026-10-27: a service starts no task after the host registers until a new deployment is forced, and a forced deployment before
``host_prepare`` finished ends ``FAILED``. ``faults`` names what goes wrong; ``log`` keeps each call's service, operation and options.
"""

from __future__ import annotations

import json
import re
import signal
from typing import Any

from scripts.process import CompletedProcess, TimeoutExpired

PROFILE = "stand-in-profile"
REGION = "stand-in-region"
CLUSTER = "stand-in-cluster"
GROUP = "stand-in-host-group"
PROVIDER = "stand-in-provider"
PROJECT = "stand-in-project"
FAMILIES = {"host_prepare": "stand-in-host-prepare", "polaris_bootstrap": "stand-in-polaris-bootstrap"}
SERVICES = {"airflow_scheduler": "stand-in-airflow-scheduler", "polaris": "stand-in-polaris", "postgres": "stand-in-postgres"}
ACCOUNT_MARK = "arn:aws:ecs:stand-in:000000000000:"
BOOT_SECONDS = 120
TERMINATE_SECONDS = 90
TASK_SECONDS = 20
SERVICE_START_SECONDS = 45
LEAK = f"{ACCOUNT_MARK}task/leak i-0123456789abcdef0 123456789012"
CLIENT_TOKEN_LIMIT = 64
FLAGS = {"--force-new-deployment", "--enable-ecs-managed-tags", "--no-honor-cooldown"}


def parse(argv: list[str]) -> dict[str, list[str]]:
    """The ``--option value ...`` groups of a command line; a flag has an empty list."""
    options: dict[str, list[str]] = {}
    current: list[str] | None = None
    for token in argv[3:]:
        if token.startswith("--"):
            current = options.setdefault(token, [])
            if token in FLAGS:
                current = None
        elif current is not None:
            current.append(token)
    return options


class StandInHost:
    """The state of one host group, its container instance, its services and its one-shot tasks.

    ``faults`` may hold: ``never_register``, ``never_terminate``, ``run_task_failure``, ``exit_code:<family key>=<n>``, ``never_stable``, ``pending_stuck``,
    ``throttle:<n>`` (the next n describe calls fail), ``api_error`` (every describe call fails) and
    ``interrupt_run_task`` (a run-task call raises KeyboardInterrupt), ``signal_run_task`` (a run-task call receives a real SIGTERM),
    ``lose_run_task_reply[:<n>]`` (the next n run-task calls start their task but time out before the reply; one when n is absent),
    ``stale_list_tasks`` (list-tasks never shows a task), ``slow_calls:<n>`` (every describe and list call takes n virtual seconds and
    times out when its limit is shorter) and ``leaky_reasons`` (every provider reason holds an ARN, an instance ID and an account ID).
    A run-task call with a client token that an earlier call used answers with that call's task, as ECS does.
    """

    def __init__(self, faults: set[str] | None = None) -> None:
        self.faults = set(faults or ())
        self.now = 0.0
        self.desired = 0
        self.instance_up_at: float | None = None
        self.terminate_at: float | None = None
        self.prepared = False
        self.bootstrap_runs = 0
        self.tasks: dict[str, dict[str, Any]] = {}
        self.services: dict[str, dict[str, Any]] = {name: {"desired": 1, "forced_at": None, "state": "IN_PROGRESS", "running": 0} for name in SERVICES.values()}
        self.log: list[dict[str, Any]] = []
        self.tokens: dict[str, str] = {}
        self.lost_replies = next((int(fault.split(":")[1]) if ":" in fault else 1 for fault in self.faults if fault.startswith("lose_run_task_reply")), 0)
        self.latency = next((int(fault.split(":")[1]) for fault in self.faults if fault.startswith("slow_calls:")), 0)
        self.handlers = {
            ("autoscaling", "set-desired-capacity"): self.autoscaling_set_desired_capacity,
            ("autoscaling", "describe-auto-scaling-groups"): self.autoscaling_describe_auto_scaling_groups,
            ("ecs", "list-container-instances"): self.ecs_list_container_instances,
            ("ecs", "describe-container-instances"): self.ecs_describe_container_instances,
            ("ecs", "list-tasks"): self.ecs_list_tasks,
            ("ecs", "run-task"): self.ecs_run_task,
            ("ecs", "describe-tasks"): self.ecs_describe_tasks,
            ("ecs", "update-service"): self.ecs_update_service,
            ("ecs", "describe-services"): self.ecs_describe_services,
        }
        self.throttled = next((int(fault.split(":")[1]) for fault in self.faults if fault.startswith("throttle:")), 0)

    def advance(self, seconds: float) -> None:
        """Move virtual time forward and settle what completes."""
        self.now += seconds
        if self.terminate_at is not None and self.now >= self.terminate_at:
            self.instance_up_at, self.terminate_at = None, None
        for task in self.tasks.values():
            if task["status"] != "STOPPED" and self.now >= task["stops_at"]:
                task["status"] = "STOPPED"
                if task["key"] == "host_prepare" and task["exit"] == 0:
                    self.prepared = True
        for name, service in self.services.items():
            forced = service["forced_at"]
            if forced is not None and service["state"] == "IN_PROGRESS" and self.now >= forced + SERVICE_START_SECONDS and "never_stable" not in self.faults:
                postgres_up = self.services[SERVICES["postgres"]]["running"] == 1
                if self.instance_active() and self.prepared and (name == SERVICES["postgres"] or postgres_up):
                    service["running"], service["state"] = 1, "COMPLETED"
                elif not self.prepared:
                    service["state"] = "FAILED"

    def instance_active(self) -> bool:
        """True when the container instance is registered."""
        return self.instance_up_at is not None and self.now >= self.instance_up_at

    def runner(self, argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        """The ``AwsCli`` runner: answer one command line."""
        service, operation, options = argv[1], argv[2], parse(argv)
        limit = kwargs.get("timeout")
        self.log.append({"service": service, "operation": operation, "options": options, "timeout": limit})
        if options.get("--profile") != [PROFILE] or options.get("--region") != [REGION]:
            return CompletedProcess(argv, 255, "", "wrong profile or region")
        if operation.startswith(("describe", "list")) and (self.throttled > 0 or "api_error" in self.faults):
            self.throttled = max(self.throttled - 1, 0)
            return CompletedProcess(argv, 254, "", "\nAn error occurred (ThrottlingException) when calling the operation: Rate exceeded\n")
        if operation.startswith(("describe", "list")) and self.latency:
            if limit is not None and limit < self.latency:
                self.advance(limit)
                raise TimeoutExpired(argv, limit)
            self.advance(self.latency)
        if operation == "run-task" and "interrupt_run_task" in self.faults:
            raise KeyboardInterrupt
        if operation == "run-task" and "signal_run_task" in self.faults:
            signal.raise_signal(signal.SIGTERM)
        handler = self.handlers.get((service, operation))
        if handler is None:
            return CompletedProcess(argv, 252, "", "unknown command")
        answer = json.dumps(handler(options))
        if operation == "run-task" and self.lost_replies > 0:
            self.lost_replies -= 1
            raise TimeoutExpired(argv, limit or 0)
        return CompletedProcess(argv, 0, answer, "")

    def autoscaling_set_desired_capacity(self, options: dict[str, list[str]]) -> dict[str, Any]:
        self.desired = int(options["--desired-capacity"][0])
        if self.desired == 1 and self.instance_up_at is None:
            self.instance_up_at = self.now + (10**9 if "never_register" in self.faults else BOOT_SECONDS)
            self.terminate_at = None
        if self.desired == 0 and self.instance_up_at is not None and self.terminate_at is None:
            self.terminate_at = self.now + (10**9 if "never_terminate" in self.faults else TERMINATE_SECONDS)
        return {}

    def autoscaling_describe_auto_scaling_groups(self, options: dict[str, list[str]]) -> dict[str, Any]:
        return {
            "AutoScalingGroups": [
                {
                    "AutoScalingGroupName": GROUP,
                    "DesiredCapacity": self.desired,
                    "Instances": [{"InstanceId": "i-stand-in"}] if self.instance_up_at is not None else [],
                }
            ]
        }

    def ecs_list_container_instances(self, options: dict[str, list[str]]) -> dict[str, Any]:
        return {"containerInstanceArns": [ACCOUNT_MARK + "container-instance/one"] if self.instance_up_at is not None else []}

    def ecs_describe_container_instances(self, options: dict[str, list[str]]) -> dict[str, Any]:
        active = self.instance_active()
        return {"containerInstances": [{"status": "ACTIVE" if active else "REGISTERING", "agentConnected": active}]}

    def ecs_list_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        family = options["--family"][0]
        if "stale_list_tasks" in self.faults:
            return {"taskArns": []}
        return {"taskArns": [arn for arn, task in self.tasks.items() if task["family"] == family and task["status"] != "STOPPED"]}

    def ecs_run_task(self, options: dict[str, list[str]]) -> dict[str, Any]:
        family = options["--task-definition"][0]
        token = options.get("--client-token", [""])[0]
        if token and (len(token) > CLIENT_TOKEN_LIMIT or not re.fullmatch(r"[A-Za-z0-9_-]+", token)):
            return {"tasks": [], "failures": [{"reason": "InvalidClientToken"}]}
        if token in self.tokens:
            return {"tasks": [{"taskArn": self.tokens[token]}], "failures": []}
        if "run_task_failure" in self.faults or not self.instance_active():
            return {"tasks": [], "failures": [{"reason": f"RESOURCE:MEMORY {LEAK}" if "leaky_reasons" in self.faults else "RESOURCE:MEMORY"}]}
        key = next(name for name, value in FAMILIES.items() if value == family)
        if key == "polaris_bootstrap":
            self.bootstrap_runs += 1
        code = next(
            (int(fault.split("=")[1]) for fault in self.faults if fault.startswith(f"exit_code:{key}=")),
            1 if key == "polaris_bootstrap" and self.bootstrap_runs > 1 else 0,
        )
        arn = f"{ACCOUNT_MARK}task/{key}-{len(self.tasks)}"
        self.tasks[arn] = {"key": key, "family": family, "status": "RUNNING", "stops_at": self.now + TASK_SECONDS, "exit": code, "options": options}
        if token:
            self.tokens[token] = arn
        return {"tasks": [{"taskArn": arn}], "failures": []}

    def ecs_describe_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        task = self.tasks[options["--tasks"][0]]
        container = {"name": task["key"], "exitCode": task["exit"]} if task["status"] == "STOPPED" else {"name": task["key"]}
        reason = "Essential container in task exited" + (f" {LEAK}" if "leaky_reasons" in self.faults else "")
        return {"tasks": [{"lastStatus": task["status"], "stoppedReason": reason, "containers": [container]}], "failures": []}

    def ecs_update_service(self, options: dict[str, list[str]]) -> dict[str, Any]:
        service = self.services[options["--service"][0]]
        service.update(forced_at=self.now, state="IN_PROGRESS", running=0)
        return {}

    def ecs_describe_services(self, options: dict[str, list[str]]) -> dict[str, Any]:
        found = []
        reason = "ECS deployment circuit breaker: tasks failed to start" + (f" {LEAK}" if "leaky_reasons" in self.faults else "")
        for name in options["--services"]:
            service = self.services[name]
            found.append(
                {
                    "serviceName": name,
                    "desiredCount": service["desired"],
                    "runningCount": service["running"],
                    "pendingCount": 1 if "pending_stuck" in self.faults else 0,
                    "deployments": [{"rolloutState": service["state"], "rolloutStateReason": reason}],
                }
            )
        return {"services": found, "failures": []}
