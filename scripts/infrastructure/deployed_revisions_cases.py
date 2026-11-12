"""Cases for the deployed-revision check: a service must be judged by the revision it runs, never by the latest revision that is registered.

An in-process stand-in of ``ecs describe-services``, ``list-tasks``, ``describe-tasks`` and ``describe-task-definition`` holds the deployments, the running
tasks and the registered definitions. Nothing leaves the machine.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure import deployed_revisions
from scripts.infrastructure.host_session import Clock, Names, Session
from scripts.infrastructure.host_session_stand_in import ACCOUNT_MARK, CLUSTER, GROUP, PROFILE, PROJECT, PROVIDER, REGION, parse
from scripts.process import CompletedProcess

AIRFLOW = ("airflow_apiserver", "airflow_dag_processor", "airflow_scheduler", "airflow_triggerer")
SERVICES = {key: f"stand-in-{key.replace('_', '-')}" for key in (*AIRFLOW, "postgres")}
NEW_SOURCE = "/data/repo/config/orchestration/job_catalog.json"
NEW_SCHEMA = "/data/repo/config/orchestration/job_catalog.schema.json"
OLD_SOURCE = "/data/repo/config/orchestration/ecs_broker_jobs.json"
OLD_SCHEMA = "/data/repo/config/orchestration/ecs_broker_jobs.schema.json"
RULES = {
    "volumes": {"code_jobs_table": "/config/orchestration/job_catalog.json", "code_jobs_schema": "/config/orchestration/job_catalog.schema.json"},
    "required_in": list(AIRFLOW),
    "forbid_secrets": ["BROKER_TOKEN"],
    "forbid_suffixes": ["/config/orchestration/ecs_broker_jobs.json", "/config/orchestration/ecs_broker_jobs.schema.json"],
}


def definition_arn(service: str, revision: int) -> str:
    return f"{ACCOUNT_MARK}task-definition/{service}:{revision}"


class World:
    """The registered definitions and the deployed state of the project services.

    ``faults`` may hold ``describe_services_fails``, ``describe_definition_fails``, ``list_tasks_fails`` and ``service_missing:<key>``."""

    def __init__(self, faults: set[str] | None = None) -> None:
        self.faults = set(faults or ())
        self.definitions: dict[str, dict[str, Any]] = {}
        self.services: dict[str, dict[str, Any]] = {}
        self.tasks: dict[str, list[str]] = {}
        self.calls: list[str] = []
        for key, name in SERVICES.items():
            self.register(name, 8, new=True, airflow=key in AIRFLOW)
            self.services[name] = {
                "serviceName": name,
                "desiredCount": 1,
                "runningCount": 1,
                "pendingCount": 0,
                "deployments": [{"status": "PRIMARY", "rolloutState": "COMPLETED", "taskDefinition": definition_arn(name, 8)}],
            }
            self.tasks[name] = [definition_arn(name, 8)]

    def register(self, name: str, revision: int, new: bool, airflow: bool = True, secrets: tuple[str, ...] = ()) -> None:
        volumes = (
            [
                {"name": "code_jobs_table", "host": {"sourcePath": NEW_SOURCE if new else OLD_SOURCE}},
                {"name": "code_jobs_schema", "host": {"sourcePath": NEW_SCHEMA if new else OLD_SCHEMA}},
            ]
            if airflow
            else [{"name": "data", "host": {"sourcePath": "/data"}}]
        )
        self.definitions[definition_arn(name, revision)] = {
            "taskDefinitionArn": definition_arn(name, revision),
            "revision": revision,
            "volumes": volumes,
            "containerDefinitions": [{"name": "main", "secrets": [{"name": secret, "valueFrom": f"{ACCOUNT_MARK}parameter/{secret}"} for secret in secrets]}],
        }

    def deploy_old(self, name: str, revision: int = 7, secrets: tuple[str, ...] = ()) -> None:
        """The service still runs an old revision that the latest registered revision does not replace (a rolled-back deployment)."""
        self.register(name, revision, new=False, secrets=secrets)
        self.services[name]["deployments"] = [{"status": "PRIMARY", "rolloutState": "COMPLETED", "taskDefinition": definition_arn(name, revision)}]
        self.tasks[name] = [definition_arn(name, revision)]

    def runner(self, argv: list[str], **_: Any) -> CompletedProcess[str]:
        service, operation, options = argv[1], argv[2], parse(argv)
        self.calls.append(f"{service} {operation}")
        if options.get("--profile") != [PROFILE] or options.get("--region") != [REGION]:
            return CompletedProcess(argv, 255, "", "wrong profile or region")
        fail = CompletedProcess(argv, 254, "", "\nAn error occurred (AccessDeniedException) when calling the operation\n")
        if operation == "describe-services":
            if "describe_services_fails" in self.faults:
                return fail
            names = [
                name
                for name in options["--services"]
                if not any(fault == f"service_missing:{key}" and SERVICES[key] == name for fault in self.faults for key in SERVICES)
            ]
            return CompletedProcess(argv, 0, json.dumps({"services": [self.services[name] for name in names], "failures": []}), "")
        if operation == "list-tasks":
            if "list_tasks_fails" in self.faults:
                return fail
            return CompletedProcess(
                argv,
                0,
                json.dumps(
                    {
                        "taskArns": [
                            f"{ACCOUNT_MARK}task/{CLUSTER}/{options['--service-name'][0]}-{index}"
                            for index, _ in enumerate(self.tasks[options["--service-name"][0]])
                        ]
                    }
                ),
                "",
            )
        if operation == "describe-tasks":
            found = []
            for arn in options["--tasks"]:
                name, index = arn.rsplit("/", 1)[1].rsplit("-", 1)
                found.append({"taskArn": arn, "lastStatus": "RUNNING", "taskDefinitionArn": self.tasks[name][int(index)]})
            return CompletedProcess(argv, 0, json.dumps({"tasks": found, "failures": []}), "")
        if operation == "describe-task-definition":
            if "describe_definition_fails" in self.faults:
                return fail
            return CompletedProcess(argv, 0, json.dumps({"taskDefinition": self.definitions[options["--task-definition"][0]]}), "")
        return CompletedProcess(argv, 252, "", "unknown command")


def build(faults: set[str] | None = None) -> tuple[World, Session, list[str]]:
    world, lines = World(faults), list[str]()
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION}, world.runner)
    names = Names(CLUSTER, GROUP, PROVIDER, PROJECT, dict(SERVICES), {})
    clock = Clock(lambda: 0.0, lambda seconds: None)
    session = Session(cli, names, clock, Path("unused_marker"), lines.append)
    return world, session, lines


def check(session: Session) -> list[str]:
    return deployed_revisions.deployed_problems(session, RULES)


def only(problems: list[str], needle: str) -> bool:
    return bool(problems) and all(needle in line for line in problems)


def all_new_revisions_pass() -> dict[str, bool]:
    world, session, lines = build()
    problems = check(session)
    return {
        "a stack whose services run the new revision has no problem": problems == [],
        "the check reads the deployed state, not only the latest definition": "ecs list-tasks" in world.calls
        and "ecs describe-tasks" in world.calls
        and "ecs describe-services" in world.calls,
        "it reads the exact revision each service deploys": "ecs describe-task-definition" in world.calls,
    }


def latest_clean_but_deployed_old_fails() -> dict[str, bool]:
    """Revision 8 is registered with the new paths but the scheduler still runs revision 7 after a rolled-back deployment."""
    world, session, _ = build()
    world.deploy_old(SERVICES["airflow_scheduler"], 7)
    world.register(SERVICES["airflow_scheduler"], 8, new=True)
    problems = check(session)
    return {
        "the service that runs the old revision is named": any("airflow_scheduler" in line for line in problems),
        "the old catalog path is reported": any("code_jobs_table" in line for line in problems),
        "only that service is a problem": all("airflow_scheduler" in line for line in problems),
    }


def a_running_task_on_an_old_revision_fails() -> dict[str, bool]:
    world, session, _ = build()
    world.services[SERVICES["airflow_triggerer"]].update(desiredCount=2, runningCount=2)
    world.tasks[SERVICES["airflow_triggerer"]] = [definition_arn(SERVICES["airflow_triggerer"], 8), definition_arn(SERVICES["airflow_triggerer"], 7)]
    world.register(SERVICES["airflow_triggerer"], 7, new=False)
    problems = check(session)
    return {
        "a task on a revision other than the deployed one fails": only(problems, "airflow_triggerer") and any("revision 7" in line for line in problems),
    }


def two_deployments_fail() -> dict[str, bool]:
    world, session, _ = build()
    name = SERVICES["airflow_apiserver"]
    world.services[name]["deployments"].append({"status": "ACTIVE", "rolloutState": "COMPLETED", "taskDefinition": definition_arn(name, 7)})
    return {"a rollout that still holds two deployments is not steady": only(check(session), "airflow_apiserver")}


def an_unfinished_or_failed_rollout_fails() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for state in ("IN_PROGRESS", "FAILED"):
        world, session, _ = build()
        world.services[SERVICES["airflow_dag_processor"]]["deployments"][0]["rolloutState"] = state
        checks[f"a {state} rollout fails"] = only(check(session), "airflow_dag_processor")
    world, session, _ = build()
    world.services[SERVICES["airflow_dag_processor"]].update(runningCount=0)
    checks["a service below its desired count fails"] = only(check(session), "airflow_dag_processor")
    world, session, _ = build()
    world.services[SERVICES["airflow_dag_processor"]].update(pendingCount=1)
    checks["a pending task fails"] = only(check(session), "airflow_dag_processor")
    return checks


def an_old_definition_with_the_broker_token_fails_though_the_latest_is_clean() -> dict[str, bool]:
    world, session, _ = build()
    name = SERVICES["airflow_scheduler"]
    world.deploy_old(name, 7, secrets=("BROKER_TOKEN",))
    world.register(name, 8, new=True)
    problems = check(session)
    return {
        "the deployed secret is reported": any("BROKER_TOKEN" in line for line in problems),
        "the problem names the service": any("airflow_scheduler" in line for line in problems),
    }


def a_service_with_no_catalog_volume_is_judged_on_its_secrets() -> dict[str, bool]:
    world, session, _ = build()
    world.deploy_old(SERVICES["postgres"], 3, secrets=("BROKER_TOKEN",))
    problems = check(session)
    return {"a non-Airflow service that injects the token is a problem": only(problems, "postgres") and any("BROKER_TOKEN" in line for line in problems)}


def an_airflow_service_without_the_catalog_volume_fails() -> dict[str, bool]:
    world, session, _ = build()
    name = SERVICES["airflow_scheduler"]
    world.definitions[definition_arn(name, 8)]["volumes"] = [{"name": "data", "host": {"sourcePath": "/data"}}]
    return {"a required volume that is absent fails": only(check(session), "airflow_scheduler")}


def unreadable_state_fails_instead_of_passing() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for fault in ("describe_services_fails", "describe_definition_fails", "list_tasks_fails", f"service_missing:{'airflow_triggerer'}"):
        world, session, _ = build({fault})
        problems = check(session)
        checks[f"{fault} is a problem, never a pass"] = bool(problems)
    return checks


def the_report_names_no_arn_or_account() -> dict[str, bool]:
    world, session, lines = build()
    world.deploy_old(SERVICES["airflow_scheduler"], 7, secrets=("BROKER_TOKEN",))
    text = "\n".join(check(session))
    return {"no ARN, account ID or parameter ARN is printed": "arn:" not in text and re.search(r"[0-9]{12}", text) is None and "parameter/" not in text}


def the_command_maps_problems_to_the_exit_code() -> dict[str, bool]:
    module = deployed_revisions
    results: dict[str, int] = {}
    for label, faults in (("clean", set()), ("old", {"describe_services_fails"})):
        world, session, _ = build(faults)
        original = module.open_session
        module.open_session = lambda session=session: session
        try:
            results[label] = module.main(["--volume", "code_jobs_table=/config/orchestration/job_catalog.json", "--in", "airflow_scheduler"])
        finally:
            module.open_session = original
    return {"a clean stack exits 0": results["clean"] == 0, "a problem exits 1": results["old"] == 1}


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "all_new_revisions_pass": all_new_revisions_pass,
    "latest_clean_but_deployed_old_fails": latest_clean_but_deployed_old_fails,
    "a_running_task_on_an_old_revision_fails": a_running_task_on_an_old_revision_fails,
    "two_deployments_fail": two_deployments_fail,
    "an_unfinished_or_failed_rollout_fails": an_unfinished_or_failed_rollout_fails,
    "an_old_definition_with_the_broker_token_fails_though_the_latest_is_clean": an_old_definition_with_the_broker_token_fails_though_the_latest_is_clean,
    "a_service_with_no_catalog_volume_is_judged_on_its_secrets": a_service_with_no_catalog_volume_is_judged_on_its_secrets,
    "an_airflow_service_without_the_catalog_volume_fails": an_airflow_service_without_the_catalog_volume_fails,
    "unreadable_state_fails_instead_of_passing": unreadable_state_fails_instead_of_passing,
    "the_report_names_no_arn_or_account": the_report_names_no_arn_or_account,
    "the_command_maps_problems_to_the_exit_code": the_command_maps_problems_to_the_exit_code,
}
