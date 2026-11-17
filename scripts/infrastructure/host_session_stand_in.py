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
from datetime import UTC, datetime, timedelta, timezone
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
OPENMETADATA_SERVICE = "stand-in-openmetadata"
OPENMETADATA_FAMILY = "stand-in-openmetadata-bootstrap"
CODE_FETCH_FAMILY = "stand-in-code-fetch"
RELOAD_FAMILY = "stand-in-features"
HELD_RUN = "20261117T010000Z-0000a002"  # a held, not released run that a skipped reload names
OLDER_RUN = "20261116T010000Z-0000a001"  # the run that a loaded reload names
BASE_RUN = "20261116T000000Z-0000b001"  # the base run that a loaded reload names
LOAD_ID = "load-stand-in-1"
LOG_GROUP = "stand-in-airflow-log-group"
JOBS_LOG_GROUP = "stand-in-jobs-log-group"
PAGE_SECONDS = 5  # the virtual time of one log page request while a log never ends
PAGE_LIMIT = 500  # log page requests per task before the stand-in gives up on a client that never stops reading (a runaway, not a result)
PAGE_BREAK = "\f"  # separates the pages of a task's output (a page may be empty)
RELOAD_STREAM = "features/feast"  # the awslogs stream prefix of the features task (the layer) and its container that runs the reload
HOST_ADDRESS = "stand-in-host"
ACCOUNT_MARK = "arn:aws:ecs:stand-in:111111111111:"
BOOT_SECONDS = 120
TERMINATE_SECONDS = 90
TASK_SECONDS = 20
NO_INSTANCES = (
    "\nAn error occurred (InvalidParameterException) when calling the RunTask operation: The specified capacity provider contains no container instances.\n"
)
SERVICE_START_SECONDS = 45
LEAK = f"{ACCOUNT_MARK}task/leak i-0123456789abcdef0 111111111111"
CLIENT_TOKEN_LIMIT = 64
STOP_TASK_SECONDS = 15
# The wall time of virtual second 0 and the zone in which describe-tasks shows ``startedAt``, as the AWS CLI shows it (an ISO string with an offset).
EPOCH = datetime(2026, 11, 17, 20, 0, 0, tzinfo=UTC)
SHOWN_ZONE = timezone(timedelta(hours=-8))
# What RunTask accepts for all container overrides of one call, formatting characters included.
OVERRIDES_LIMIT = 8192
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
    ``interrupt_catalog_stop`` (the call that scales the catalog service to zero raises KeyboardInterrupt), ``signal_catalog_stop`` (it receives a real
    SIGTERM), ``second_signal:<name>`` (the cleanup's set-desired-capacity call to 0 receives a real SIGINT, SIGTERM or SIGHUP once),
    ``lose_run_task_reply[:<n>]`` (the next n run-task calls start their task but time out before the reply; one when n is absent),
    ``stale_list_tasks`` (list-tasks never shows a task), ``slow_calls:<n>`` (every describe and list call takes n virtual seconds and
    times out when its limit is shorter) and ``leaky_reasons`` (every provider reason holds an ARN, an instance ID and an account ID).
    ``page_size:<n>`` makes list-tasks answer n ARNs and a ``nextToken`` per page when the call has ``--no-paginate`` (without it the CLI joins the
    pages), ``repeat_token`` makes every page answer the same ``nextToken``, ``ghost_task`` makes list-tasks name one task that describe-tasks reports
    ``MISSING``, ``vanish_after_stop`` makes a stopped task unknown to describe-tasks, ``never_stop_task`` makes stop-task accept the call but leave the
    task running, ``stop_task_error`` makes stop-task fail without effect, ``stop_task_lost_reply`` makes stop-task take effect and then fail and
    ``describe_tasks_failure:<n>`` makes the next n describe-tasks calls answer a failure entry that is not ``MISSING`` (a transient service failure) and
    ``no_definition`` makes describe-tasks omit ``taskDefinitionArn`` and ``no_started_at`` makes it omit ``startedAt`` (``EPOCH`` plus the virtual start time,
    shown with an offset as the AWS CLI shows it). A service name that the host does not know is answered as ECS answers a missing
    service: a ``MISSING`` failure entry and no service. ``describe_services_error`` adds a failure entry of another reason to every describe-services
    answer. List-tasks answers the tasks whose ``desiredStatus`` is the ``--desired-status`` value (RUNNING when absent), and a stopped task has desired
    status STOPPED. ``add_task`` adds a running task of any family and group, so a case can hold job tasks, service tasks and writer tasks together.
    The features task (``online_reload``) and the code identity are always there: ``reload_fails:<set>`` makes the reload launch of that set exit 1,
    ``never_starts:<key>`` makes a task of the key stop without an exit code (``TaskFailedToStart``), ``task_seconds:<key>=<n>`` makes it run n virtual
    seconds, ``no_code_record`` makes the identity task refuse as a host with no run ID record does, ``no_identity_log`` and ``bad_identity_line`` make its
    log empty or malformed and ``identity_late_line`` puts its line on the second page of its log.
    A reload launch prints its result line (``online_reload`` prints one JSON line) in the jobs log group: ``loaded`` for its set,
    ``reload_skipped:<set>`` makes it print ``skipped`` with a held run and a reason and exit 0, ``reload_no_log`` makes its log empty,
    ``reload_incomplete:<set>`` makes its line lack the fields of its outcome, ``reload_forged_after:<set>`` appends a bare ``loaded`` line and
    ``reload_second_result:<set>`` appends a second complete ``loaded`` line.
    A log request (``get-log-events``) answers the output of a stopped task one page at a time, as CloudWatch does: the token ``f/<n>`` starts at page n,
    every answer carries the token of the next page, and a token past the last page answers an empty page with the token it was given. A log has one
    page, and these faults split a reload log: ``reload_paged:<set>`` puts every line on a page of its own, ``reload_forged_first_page:<set>`` puts a
    complete forged ``loaded`` line on page 1 and the genuine line on page 2, ``reload_empty_middle_page:<set>`` puts an empty page between the forged
    line and the genuine line, ``reload_late_result:<set>`` puts the genuine line after two empty pages and ``reload_endless_pages:<set>`` follows the
    genuine page with empty pages and a new token for ever, each request taking ``PAGE_SECONDS`` of virtual time. A request that is not made against
    a stopped task answers one empty page, and a log request fails with ``ResourceNotFoundException`` for a stream or a group that has no such task.
    A run-task call with a client token that an earlier call used answers with that call's task, as ECS does.

    ``service_tasks_for`` (names of services, empty by default) gives each named service one task record: it runs while the service runs, ``list-tasks`` with
    ``--service-name`` answers it, scaling the service to zero first leaves it STOPPING (the containers still close their files, ``runningCount`` already 0)
    until it stops, and a forced deployment stops it. The faults ``auto_schedule`` (a named service with a desired count starts on its own once the
    instance is registered, as ECS schedules it) and ``slow_container_stop:<n>`` (the task stays STOPPING n seconds after ``runningCount`` reached 0) act on
    those services only. ``omit_service_task_descriptions`` makes describe-tasks leave out the records of those services' tasks (an empty ``tasks`` and
    no failure) and ``list_service_tasks_twice`` makes list-tasks answer each of their tasks under both desired statuses.
    Every ``prometheus_restore`` launch records what those services looked like in ``restore_snapshots``.
    """

    def __init__(
        self, faults: set[str] | None = None, openmetadata: bool = False, code_fetch: bool = False, checkout: str | None = None, code_run: str | None = None
    ) -> None:
        self.faults = set(faults or ())
        self.families = dict(
            FAMILIES,
            online_reload=RELOAD_FAMILY,
            **({"openmetadata_bootstrap": OPENMETADATA_FAMILY} if openmetadata else {}),
            **({"code_fetch": CODE_FETCH_FAMILY} if code_fetch else {}),
        )
        # The code digest that /data/repo shows and the run ID recorded beside it. The code one-shot changes them only when it exits 0 with a digest in its
        # overrides, and exits 1 without overrides on a host that has none, as infra/services/scripts/code_fetch.py does. The identity task prints them.
        self.checkout, self.code_run, self.pending_output = checkout, code_run, ""
        self.pending_endless = False
        self.reloads: list[dict[str, Any]] = []
        self.online: dict[str, tuple[str, str]] = {}
        self.now = 0.0
        self.desired = 0
        self.instance_up_at: float | None = None
        self.terminate_at: float | None = None
        self.prepared = False
        self.bootstrap_runs = 0
        self.tasks: dict[str, dict[str, Any]] = {}
        self.services: dict[str, dict[str, Any]] = {
            name: {"desired": 1, "forced_at": None, "state": "IN_PROGRESS", "running": 0, "stop_at": None}
            for name in [*SERVICES.values(), *([OPENMETADATA_SERVICE] if openmetadata else [])]
        }
        self.log: list[dict[str, Any]] = []
        self.service_tasks_for: set[str] = set()
        self.restore_snapshots: list[dict[str, dict[str, Any]]] = []
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
        self.describe_task_failures = next((int(fault.split(":")[1]) for fault in self.faults if fault.startswith("describe_tasks_failure:")), 0)
        self.handlers[("ecs", "stop-task")] = self.ecs_stop_task
        self.handlers[("logs", "get-log-events")] = self.logs_get_log_events
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
            "started": self.now,
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
            if service["stop_at"] is not None and self.now >= service["stop_at"] and "never_stop_openmetadata" not in self.faults:
                service.update(running=0, stop_at=None)
            unscheduled = service["running"] == 0 and service["forced_at"] is None and service["state"] == "IN_PROGRESS"
            if name in self.service_tasks_for and "auto_schedule" in self.faults and self.instance_active() and service["desired"] > 0 and unscheduled:
                service.update(running=1, state="COMPLETED")
            forced = service["forced_at"]
            if service["desired"] == 0:
                continue
            if forced is not None and service["state"] == "IN_PROGRESS" and self.now >= forced + SERVICE_START_SECONDS and "never_stable" not in self.faults:
                postgres_up = self.services[SERVICES["postgres"]]["running"] == 1
                if self.instance_active() and self.prepared and (name == SERVICES["postgres"] or postgres_up):
                    service["running"], service["state"] = 1, "COMPLETED"
                elif not self.prepared:
                    service["state"] = "FAILED"
        for name in self.service_tasks_for:
            self.settle_service_task(name)

    def settle_service_task(self, name: str) -> None:
        """Keep one task record per service in step with ``running``: a running service has a RUNNING task, a stopped service none that is RUNNING."""
        service = self.services[name]
        live = [task for task in self.tasks.values() if task.get("group") == f"service:{name}" and task["status"] == "RUNNING"]
        if service["running"] == 1 and not live and service["desired"] > 0:
            self.add_task(name, f"service:{name}")

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
        scaling_down = operation == "update-service" and options.get("--desired-count") == ["0"]
        if scaling_down and "interrupt_catalog_stop" in self.faults:
            raise KeyboardInterrupt
        if scaling_down and "signal_catalog_stop" in self.faults:
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
        service_name = options.get("--service-name", [None])[0]
        if service_name in self.service_tasks_for and "list_service_tasks_twice" in self.faults:
            return {"taskArns": [arn for arn, task in self.tasks.items() if task.get("group") == f"service:{service_name}"]}
        arns = [
            arn
            for arn, task in self.tasks.items()
            if (family is None or task["family"] == family)
            and (service_name not in self.service_tasks_for or task.get("group") == f"service:{service_name}")
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
        if len(options.get("--overrides", [""])[0]) > OVERRIDES_LIMIT:
            raise StandInError("ClientException")
        token = options.get("--client-token", [""])[0]
        if token and (len(token) > CLIENT_TOKEN_LIMIT or not re.fullmatch(r"[A-Za-z0-9_-]+", token)):
            return {"tasks": [], "failures": [{"reason": "InvalidClientToken"}]}
        if token in self.tokens:
            return {"tasks": [{"taskArn": self.tokens[token]}], "failures": []}
        if "run_task_failure" in self.faults or not self.instance_active():
            return {"tasks": [], "failures": [{"reason": f"RESOURCE:MEMORY {LEAK}" if "leaky_reasons" in self.faults else "RESOURCE:MEMORY"}]}
        key = next(name for name, value in self.families.items() if value == family)
        if key == "prometheus_restore":
            self.restore_snapshots.append(self.service_snapshot())
        if key == "polaris_bootstrap":
            self.bootstrap_runs += 1
        code = next(
            (int(fault.split("=")[1]) for fault in self.faults if fault.startswith(f"exit_code:{key}=")),
            1 if key == "polaris_bootstrap" and self.bootstrap_runs > 1 else 0,
        )
        if key == "code_fetch":
            code = self.code_fetch_exit(options, code)
        if key == "online_reload":
            code = self.reload_exit(options, code)
        arn = f"{ACCOUNT_MARK}task/{key}-{len(self.tasks)}"
        seconds = next((int(fault.split("=")[1]) for fault in self.faults if fault.startswith(f"task_seconds:{key}=")), TASK_SECONDS)
        self.tasks[arn] = {
            "key": key,
            "family": family,
            "status": "RUNNING",
            "stops_at": self.now + seconds,
            "exit": code,
            "options": options,
            "started": self.now,
            "output": self.pending_output,
            "endless": self.pending_endless,
            "page_requests": 0,
            "unstarted": f"never_starts:{key}" in self.faults,
        }
        self.pending_output, self.pending_endless = "", False
        if token:
            self.tokens[token] = arn
        return {"tasks": [{"taskArn": arn}], "failures": []}

    def code_fetch_exit(self, options: dict[str, list[str]], code: int) -> int:
        """The exit code of the code one-shot; a successful install moves the checkout to the digest of its overrides. The verify mode succeeds when the host
        shows that digest (fault ``verify_fails`` makes it fail) and the restore mode always succeeds, as a host with a verified checkout does."""
        containers = json.loads(options["--overrides"][0])["containerOverrides"] if "--overrides" in options else []
        environment = {item["name"]: item["value"] for container in containers for item in container.get("environment", [])}
        if environment.get("CODE_MODE") == "identify":
            if self.checkout is None or self.code_run is None or "no_code_record" in self.faults:
                return 1
            if "bad_identity_line" in self.faults:
                self.pending_output = f"code_identify: identity run latest code {self.checkout}"
            elif "no_identity_log" not in self.faults:
                line = f"code_identify: identity run {self.code_run} code {self.checkout}"
                self.pending_output = PAGE_BREAK + line if "identity_late_line" in self.faults else line
            return 0
        if environment.get("CODE_MODE") == "verify":
            return 0 if environment.get("CODE_SHA256") == self.checkout and "verify_fails" not in self.faults else 1
        if environment.get("CODE_MODE") == "restore":
            return 0
        if code != 0:
            return code
        if "CODE_SHA256" in environment:
            self.checkout, self.code_run = environment["CODE_SHA256"], environment["CODE_RUN_ID"]
            return 0
        return 0 if self.checkout is not None else 1

    def reload_exit(self, options: dict[str, list[str]], code: int) -> int:
        """The exit code of a reload launch (a features task with LAYER_PHASE reload): the launch is recorded by container, and a success marks the set
        online with the code of the launch. Fault ``reload_fails:<set>`` makes the launch of that set exit 1."""
        containers = json.loads(options["--overrides"][0])["containerOverrides"] if "--overrides" in options else []
        launch = {container["name"]: {item["name"]: item["value"] for item in container["environment"]} for container in containers}
        model = launch.get("feast", {}).get("RELEASE_MODEL", "")
        self.reloads.append(
            {"model": model, "launch": launch, "at": self.now, "services_stable": all(item["state"] == "COMPLETED" for item in self.services.values())}
        )
        if code == 0 and f"reload_fails:{model}" in self.faults:
            return 1
        if code == 0:
            self.pending_output = PAGE_BREAK.join("\n".join(json.dumps(line, sort_keys=True) for line in page) for page in self.reload_pages(model))
            self.pending_endless = f"reload_endless_pages:{model}" in self.faults
        if code == 0 and f"reload_skipped:{model}" not in self.faults:
            fetch = launch.get("fetch", {})
            self.online[model] = (fetch.get("CODE_RUN_ID", ""), fetch.get("CODE_SHA256", ""))
        return code

    def reload_lines(self, model: str) -> list[dict[str, Any]]:
        """The result lines that the reload of ``model`` prints: the genuine line (``skipped`` with ``reload_skipped:<set>``, else ``loaded``), none with
        ``reload_no_log``, the genuine line without its fields with ``reload_incomplete:<set>``, and after it a forged bare ``loaded`` line with
        ``reload_forged_after:<set>`` or a second complete ``loaded`` line with ``reload_second_result:<set>``."""
        if "reload_no_log" in self.faults:
            return []
        reason = f"run {HELD_RUN} is held and not released, and run {OLDER_RUN} is on another schema than set {model} has now; nothing was loaded"
        skipped = {"feature_set": model, "outcome": "skipped", "reason": reason, "skipped_held": [HELD_RUN]}
        loaded = {"feature_set": model, "outcome": "loaded", "run_id": OLDER_RUN, "base_run": BASE_RUN, "load_id": LOAD_ID, "skipped_held": []}
        genuine = skipped if f"reload_skipped:{model}" in self.faults else loaded
        lines = [{"feature_set": model, "outcome": genuine["outcome"]} if f"reload_incomplete:{model}" in self.faults else genuine]
        if f"reload_forged_after:{model}" in self.faults:
            lines.append({"feature_set": model, "outcome": "loaded"})
        if f"reload_second_result:{model}" in self.faults:
            lines.append(loaded)
        return lines

    def reload_pages(self, model: str) -> list[list[dict[str, Any]]]:
        """The pages of the reload log of ``model``: one page of every line, or the split that a paging fault asks for (see the class docstring)."""
        lines = self.reload_lines(model)
        forged = {"feature_set": model, "outcome": "loaded", "run_id": OLDER_RUN, "base_run": BASE_RUN, "load_id": "load-forged", "skipped_held": []}
        if f"reload_forged_first_page:{model}" in self.faults:
            return [[forged], lines]
        if f"reload_empty_middle_page:{model}" in self.faults:
            return [[forged], [], lines]
        if f"reload_late_result:{model}" in self.faults:
            return [[], [], lines]
        if f"reload_paged:{model}" in self.faults:
            return [[line] for line in lines]
        return [lines]

    def logs_get_log_events(self, options: dict[str, list[str]]) -> dict[str, Any]:
        """One page of the log of one task: its output once it stopped. A group other than the stand-in's, or a stream with no such task, is not found."""
        stream = options["--log-stream-name"][0].split("/")
        found = [task for arn, task in self.tasks.items() if arn.rsplit("/", 1)[-1] == stream[-1] and task["key"] == stream[0] == stream[1]]
        group = LOG_GROUP
        if not found and len(stream) == 3 and f"{stream[0]}/{stream[1]}" == RELOAD_STREAM:
            found = [task for arn, task in self.tasks.items() if arn.rsplit("/", 1)[-1] == stream[-1] and task["key"] == "online_reload"]
            group = JOBS_LOG_GROUP
        if options["--log-group-name"] != [group] or len(stream) != 3 or not found:
            raise StandInError("ResourceNotFoundException")
        task = found[0]
        pages = task.get("output", "").split(PAGE_BREAK) if task["status"] == "STOPPED" else []
        index = int(options["--next-token"][0].split("/")[1]) if "--next-token" in options else 0
        if task.get("endless"):
            task["page_requests"] += 1
            self.advance(PAGE_SECONDS)
            if task["page_requests"] > PAGE_LIMIT:
                raise StandInError("a client that reads the log pages for ever")
            events = [{"message": line} for line in pages[index].splitlines()] if index < len(pages) else []
            return {"events": events, "nextForwardToken": f"f/{index + 1}"}
        if index >= len(pages):
            return {"events": [], "nextForwardToken": f"f/{index}"}
        return {"events": [{"message": line} for line in pages[index].splitlines()], "nextForwardToken": f"f/{index + 1}"}

    def ecs_describe_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if self.describe_task_failures > 0:
            self.describe_task_failures -= 1
            return {"tasks": [], "failures": [{"arn": options["--tasks"][0], "reason": "INTERNAL"}]}
        found: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        reason = "Essential container in task exited" + (f" {LEAK}" if "leaky_reasons" in self.faults else "")
        for arn in options["--tasks"]:
            task = self.tasks.get(arn)
            if task is None:
                failures.append({"arn": arn, "reason": "MISSING"})
                continue
            if "omit_service_task_descriptions" in self.faults and str(task.get("group", "")).startswith("service:"):
                continue
            container = {"name": task["key"], "exitCode": task["exit"]} if task["status"] == "STOPPED" else {"name": task["key"]}
            if task["status"] == "STOPPED" and task.get("unstarted"):
                container = {"name": task["key"], "reason": "CannotPullContainerError"}
            definition = {} if "no_definition" in self.faults else {"taskDefinitionArn": f"{ACCOUNT_MARK}task-definition/{task['family']}:7"}
            overrides = json.loads(task["options"]["--overrides"][0]) if "--overrides" in task["options"] else {"containerOverrides": [{"name": task["key"]}]}
            found.append(
                {
                    "taskArn": arn,
                    **definition,
                    "group": task.get("group", f"family:{task['family']}"),
                    "lastStatus": task["status"],
                    "desiredStatus": task.get("desired", "RUNNING"),
                    **(
                        {} if "no_started_at" in self.faults else {"startedAt": (EPOCH + timedelta(seconds=task["started"])).astimezone(SHOWN_ZONE).isoformat()}
                    ),
                    "stoppedReason": "Task failed to start" if task.get("unstarted") and task["status"] == "STOPPED" else reason,
                    **({"stopCode": "TaskFailedToStart"} if task.get("unstarted") and task["status"] == "STOPPED" else {}),
                    "containers": [container],
                    "overrides": overrides,
                }
            )
        return {"tasks": found, "failures": failures}

    def service_snapshot(self) -> dict[str, dict[str, Any]]:
        """What each service with task records shows now: its desired and running counts and the status of each of its tasks."""
        return {
            name: {
                "desired": self.services[name]["desired"],
                "running": self.services[name]["running"],
                "tasks": [task["status"] for task in self.tasks.values() if task.get("group") == f"service:{name}"],
            }
            for name in sorted(self.service_tasks_for)
        }

    def ecs_update_service(self, options: dict[str, list[str]]) -> dict[str, Any]:
        name = options["--service"][0]
        if name not in self.services:
            raise StandInError("ServiceNotFoundException")
        service = self.services[name]
        count = options.get("--desired-count")
        if count is not None and int(count[0]) == 0:
            service.update(desired=0, forced_at=None, state="COMPLETED", stop_at=self.now + STOP_TASK_SECONDS)
            slow = next((int(fault.split(":")[1]) for fault in self.faults if fault.startswith("slow_container_stop:")), 0)
            for task in self.tasks.values():
                if task.get("group") == f"service:{name}" and task["status"] == "RUNNING":
                    task.update(status="STOPPING", desired="STOPPED", stops_at=self.now + STOP_TASK_SECONDS + slow)
            return {}
        if count is not None:
            service["desired"] = int(count[0])
        for task in self.tasks.values():
            if task.get("group") == f"service:{name}" and task["status"] != "STOPPED":
                task.update(status="STOPPED", desired="STOPPED")
        service.update(forced_at=self.now, state="IN_PROGRESS", running=0, stop_at=None)
        return {}

    def ecs_describe_services(self, options: dict[str, list[str]]) -> dict[str, Any]:
        found = []
        reason = "ECS deployment circuit breaker: tasks failed to start" + (f" {LEAK}" if "leaky_reasons" in self.faults else "")
        failures: list[dict[str, str]] = []
        for name in options["--services"]:
            service = self.services.get(name)
            if service is None:
                failures.append({"arn": f"{ACCOUNT_MARK}service/{CLUSTER}/{name}", "reason": "MISSING"})
                continue
            found.append(
                {
                    "serviceName": name,
                    "desiredCount": service["desired"],
                    "runningCount": service["running"],
                    "pendingCount": 1 if "pending_stuck" in self.faults else 0,
                    "deployments": [{"rolloutState": service["state"], "rolloutStateReason": reason}],
                }
            )
        if "describe_services_error" in self.faults:
            failures.append({"arn": f"{ACCOUNT_MARK}service/{CLUSTER}/any", "reason": "OTHER"})
        return {"services": found, "failures": failures}
