"""An in-process stand-in for the AWS CLI calls of ``host_session`` (ECS and Auto Scaling), for offline end-to-end cases; it never leaves the machine.

It is the ``runner`` of ``AwsCli``: it receives the command line and answers with the exit code and JSON the command would. Time is virtual: the
session's clock reads ``now`` and its sleep calls ``advance``, which moves the host, the tasks and the services forward. The stand-in models the
incident of 2026-10-27: a service starts no task after the host registers until a new deployment is forced, and a forced deployment before
``host_prepare`` finished ends ``FAILED``. It also models the incident of 2026-10-29: the instance is ACTIVE and connected before it joins the capacity
provider, and RunTask on the provider fails until it has joined. ``faults`` names what goes wrong; ``log`` keeps each call's service, operation,
options, time limit and virtual time.
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
NO_INSTANCES = (
    "\nAn error occurred (InvalidParameterException) when calling the RunTask operation: The specified capacity provider contains no container instances.\n"
)
SERVICE_START_SECONDS = 45
LEAK = f"{ACCOUNT_MARK}task/leak i-0123456789abcdef0 123456789012"
CLIENT_TOKEN_LIMIT = 64
STOP_TASK_SECONDS = 15
FLAGS = {"--force-new-deployment", "--enable-ecs-managed-tags", "--no-honor-cooldown", "--no-paginate"}


class StandInError(Exception):
    """A handler refuses the call the way the AWS CLI does: exit code 254 and an ``An error occurred (<code>)`` line."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


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
    ``late_provider:<n>`` (the instance joins the capacity provider n virtual seconds after it registers), ``never_provider`` (it never joins),
    ``other_provider`` (it reports another capacity provider's name),
    ``throttle:<n>`` (the next n describe calls fail), ``api_error`` (every describe call fails) and
    ``interrupt_run_task`` (a run-task call raises KeyboardInterrupt), ``signal_run_task`` (a run-task call receives a real SIGTERM),
    ``second_signal:<name>`` (the cleanup's set-desired-capacity call to 0 receives a real SIGINT, SIGTERM or SIGHUP once),
    ``lose_run_task_reply[:<n>]`` (the next n run-task calls start their task but time out before the reply; one when n is absent),
    ``stale_list_tasks`` (list-tasks never shows a task), ``slow_calls:<n>`` (every describe and list call takes n virtual seconds and
    times out when its limit is shorter) and ``leaky_reasons`` (every provider reason holds an ARN, an instance ID and an account ID).
    ``page_size:<n>`` makes list-tasks answer n ARNs and a ``nextToken`` per page when the call has ``--no-paginate`` (without it the CLI joins the
    pages), ``repeat_token`` makes every page answer the same ``nextToken``, ``ghost_task`` makes list-tasks name one task that describe-tasks reports
    ``MISSING``, ``vanish_after_stop`` makes a stopped task unknown to describe-tasks, ``never_stop_task`` makes stop-task accept the call but leave the
    task running, ``stop_task_error`` makes stop-task fail without effect, ``stop_task_lost_reply`` makes stop-task take effect and then fail and
    ``no_definition`` makes describe-tasks omit ``taskDefinitionArn``. List-tasks answers the tasks whose ``desiredStatus`` is the ``--desired-status``
    value (RUNNING when absent), and a stopped task has desired status STOPPED. ``add_task`` adds a running task of any family and group, so a case
    can hold job tasks, service tasks and writer tasks together.
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
        self.provider_delay = next((int(fault.split(":")[1]) for fault in self.faults if fault.startswith("late_provider:")), 0)
        self.page_size = next((int(fault.split(":")[1]) for fault in self.faults if fault.startswith("page_size:")), 0)
        self.handlers[("ecs", "stop-task")] = self.ecs_stop_task
        self.added = 0

    def add_task(self, family: str, group: str | None = None) -> str:
        """Add a running task of ``family`` (group ``family:<family>`` unless given) that runs until it is stopped; the answer is its ARN."""
        self.added += 1
        arn = f"{ACCOUNT_MARK}task/{CLUSTER}/{'ab' * 12}c0de{self.added:04x}"
        self.tasks[arn] = {
            "key": family,
            "family": family,
            "group": group or f"family:{family}",
            "status": "RUNNING",
            "desired": "RUNNING",
            "stops_at": 10**9,
            "exit": 0,
            "options": {},
        }
        return arn

    def advance(self, seconds: float) -> None:
        """Move virtual time forward and settle what completes."""
        self.now += seconds
        if self.terminate_at is not None and self.now >= self.terminate_at:
            self.instance_up_at, self.terminate_at = None, None
        for task in self.tasks.values():
            if task["status"] != "STOPPED" and self.now >= task["stops_at"]:
                task["status"], task["desired"] = "STOPPED", "STOPPED"
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

    def provider_member(self) -> bool:
        """True when the registered container instance has joined the capacity provider."""
        joined = self.instance_up_at is not None and self.now >= self.instance_up_at + self.provider_delay
        return joined and "never_provider" not in self.faults and "other_provider" not in self.faults

    def runner(self, argv: list[str], **kwargs: Any) -> CompletedProcess[str]:
        """The ``AwsCli`` runner: answer one command line."""
        service, operation, options = argv[1], argv[2], parse(argv)
        limit = kwargs.get("timeout")
        self.log.append({"service": service, "operation": operation, "options": options, "timeout": limit, "at": self.now})
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
        if operation == "run-task" and self.instance_active() and not self.provider_member() and "run_task_failure" not in self.faults:
            return CompletedProcess(argv, 254, "", NO_INSTANCES)
        if operation == "run-task" and "interrupt_run_task" in self.faults:
            raise KeyboardInterrupt
        if operation == "run-task" and "signal_run_task" in self.faults:
            signal.raise_signal(signal.SIGTERM)
        second = next((fault for fault in self.faults if fault.startswith("second_signal:")), None)
        if second and operation == "set-desired-capacity" and options.get("--desired-capacity") == ["0"]:
            self.faults.discard(second)
            signal.raise_signal(signal.Signals[second.split(":")[1]])
        handler = self.handlers.get((service, operation))
        if handler is None:
            return CompletedProcess(argv, 252, "", "unknown command")
        try:
            answer = json.dumps(handler(options))
        except StandInError as error:
            return CompletedProcess(argv, 254, "", f"\nAn error occurred ({error.code}) when calling the {operation} operation\n")
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
        instance: dict[str, Any] = {"status": "ACTIVE" if active else "REGISTERING", "agentConnected": active}
        if self.provider_member():
            instance["capacityProviderName"] = PROVIDER
        elif "other_provider" in self.faults and active:
            instance["capacityProviderName"] = "other-provider"
        return {"containerInstances": [instance]}

    def ecs_list_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        family = options.get("--family", [None])[0]
        if "stale_list_tasks" in self.faults:
            return {"taskArns": []}
        desired = options.get("--desired-status", ["RUNNING"])[0]
        arns = [
            arn
            for arn, task in self.tasks.items()
            if (family is None or task["family"] == family)
            and task.get("desired", "RUNNING") == desired
            and (desired == "STOPPED" or task["status"] != "STOPPED")
        ]
        if "ghost_task" in self.faults:
            arns = [*arns, f"{ACCOUNT_MARK}task/{CLUSTER}/ghost"]
        if not self.page_size or "--no-paginate" not in options:
            return {"taskArns": arns}
        start = int(options.get("--next-token", ["0"])[0])
        page: dict[str, Any] = {"taskArns": arns[start : start + self.page_size]}
        if start + self.page_size < len(arns):
            page["nextToken"] = str(start if "repeat_token" in self.faults else start + self.page_size)
        return page

    def ecs_stop_task(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if "stop_task_error" in self.faults:
            raise StandInError("InvalidParameterException")
        task = self.tasks.get(options["--task"][0])
        if task is None or options.get("--cluster") != [CLUSTER]:
            raise StandInError("InvalidParameterException")
        task["desired"], task["stop_reason"] = "STOPPED", options.get("--reason", [""])[0]
        if "vanish_after_stop" in self.faults:
            del self.tasks[options["--task"][0]]
            return {"task": {"desiredStatus": "STOPPED"}}
        if "never_stop_task" not in self.faults:
            task["stops_at"] = min(task["stops_at"], self.now + STOP_TASK_SECONDS)
        if "stop_task_lost_reply" in self.faults:
            raise StandInError("ServerException")
        return {"task": {"taskArn": options["--task"][0], "desiredStatus": "STOPPED"}}

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
        found: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        reason = "Essential container in task exited" + (f" {LEAK}" if "leaky_reasons" in self.faults else "")
        for arn in options["--tasks"]:
            task = self.tasks.get(arn)
            if task is None:
                failures.append({"arn": arn, "reason": "MISSING"})
                continue
            container = {"name": task["key"], "exitCode": task["exit"]} if task["status"] == "STOPPED" else {"name": task["key"]}
            definition = {} if "no_definition" in self.faults else {"taskDefinitionArn": f"{ACCOUNT_MARK}task-definition/{task['family']}:7"}
            found.append(
                {
                    "taskArn": arn,
                    **definition,
                    "group": task.get("group", f"family:{task['family']}"),
                    "lastStatus": task["status"],
                    "desiredStatus": task.get("desired", "RUNNING"),
                    "stoppedReason": reason,
                    "containers": [container],
                }
            )
        return {"tasks": found, "failures": failures}

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
