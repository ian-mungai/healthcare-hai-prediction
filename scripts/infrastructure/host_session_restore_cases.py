"""Host session cases for the restore after a teardown: the code run record kept outside the data volume and the check of the postgres task's database setup.

The stand-in host here adds a Parameter Store (``get-parameter`` and ``put-parameter``) and a postgres service task whose setup containers end with an exit code
the case chooses. A start on a fresh data volume finds no code record on the host, so it reads the record that an earlier start wrote to Parameter Store.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure.host_session import HostSessionError, Session
from scripts.infrastructure.host_session_stand_in import PROFILE, REGION, SERVICES, StandInError, StandInHost

RECORD_NAME = "/stand-in-project/code_run_record"
MLFLOW_USER_FAMILY = "stand-in-mlflow-user"
POSTGRES_LOG_GROUP = "stand-in-postgres-log-group"
POSTGRES_TASK = "arn:aws:ecs:stand-in:111111111111:task/stand-in-cluster/postgrestask0001"
RUN_NEW = "20261117T150000Z-cccccccc"
SHA_NEW = "c" * 64
RUN_OLD = "20261108T130000Z-bbbbbbbb"
SHA_OLD = "b" * 64
CASE_ERRORS = (AssertionError, LookupError, StopIteration, TypeError, ValueError, AttributeError, NameError, OSError, HostSessionError)


class RecordHost(StandInHost):
    """A stand-in host with a parameter store and a postgres task. ``setup_exits`` maps a setup container of the postgres task to its exit code."""

    def __init__(self, faults: set[str] | None = None, setup_exits: dict[str, int] | None = None, **fields: Any) -> None:
        super().__init__(faults, code_fetch=True, **fields)
        self.parameters: dict[str, str] = {}
        self.puts: list[dict[str, list[str]]] = []
        self.setup_exits = setup_exits
        self.handlers[("ssm", "get-parameter")] = self.ssm_get_parameter
        self.handlers[("ssm", "put-parameter")] = self.ssm_put_parameter

    def ssm_get_parameter(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if "ssm_denied" in self.faults:
            raise StandInError("AccessDeniedException")
        name = options["--name"][0]
        if name not in self.parameters:
            raise StandInError("ParameterNotFound")
        return {"Parameter": {"Name": name, "Value": self.parameters[name]}}

    def ssm_put_parameter(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if "ssm_denied" in self.faults:
            raise StandInError("AccessDeniedException")
        self.puts.append(options)
        self.parameters[options["--name"][0]] = options["--value"][0]
        return {"Version": len(self.puts)}

    def ecs_list_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if self.setup_exits is not None and options.get("--service-name") == [SERVICES["postgres"]]:
            return {"taskArns": [POSTGRES_TASK]}
        return super().ecs_list_tasks(options)

    def ecs_describe_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if self.setup_exits is not None and options["--tasks"] == [POSTGRES_TASK]:
            containers: list[dict[str, Any]] = [{"name": "postgres", "lastStatus": "RUNNING"}]
            for name, code in self.setup_exits.items():
                containers.append({"name": name, "lastStatus": "STOPPED", "exitCode": code})
            return {"tasks": [{"taskArn": POSTGRES_TASK, "lastStatus": "RUNNING", "containers": containers}], "failures": []}
        return super().ecs_describe_tasks(options)

    def logs_get_log_events(self, options: dict[str, list[str]]) -> dict[str, Any]:
        stream = options["--log-stream-name"][0]
        if options["--log-group-name"] == [POSTGRES_LOG_GROUP]:
            reasons = {
                "postgres_databases": "postgres_databases: restore refused: database airflow: pg_restore failed or passed its 1200 second deadline",
                "postgres_restore_fetch": "pg_restore_fetch failed: no completed backup set is usable: the dump of airflow differs from its marker",
            }
            name = stream.split("/")[0]
            if name not in reasons or not stream.endswith("/postgrestask0001"):
                raise StandInError("ResourceNotFoundException")
            return {"events": [{"message": "unrelated line"}, {"message": reasons[name]}], "nextForwardToken": "f/0"}
        return super().logs_get_log_events(options)


def build(
    faults: set[str] | None = None,
    *,
    checkout: str | None = SHA_OLD,
    code_run: str | None = RUN_OLD,
    record: str | None = None,
    setup_exits: dict[str, int] | None = None,
    named: bool = True,
    mlflow_user: bool = False,
) -> tuple[RecordHost, Session, list[str]]:
    """A session over the stand-in. ``record`` is the value of the code run record parameter; ``named`` gives the session the parameter and log group names."""
    from dataclasses import replace

    cases = importlib.import_module("scripts.infrastructure.host_session_cases")
    module = importlib.import_module("scripts.infrastructure.host_session")  # the module as it is loaded now: a plant replaces it in sys.modules
    host, lines = RecordHost(faults, setup_exits, checkout=checkout, code_run=code_run), list[str]()
    if mlflow_user:
        host.families["mlflow_user"] = MLFLOW_USER_FAMILY
    if record is not None:
        host.parameters[RECORD_NAME] = record
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION}, host.runner)
    names = replace(cases.names(), code_record=RECORD_NAME, postgres_log_group=POSTGRES_LOG_GROUP) if named else cases.names()
    if mlflow_user:
        names = replace(names, one_shot={**names.one_shot, "mlflow_user": MLFLOW_USER_FAMILY})
    marker = cases.Path(cases.tempfile.mkdtemp(prefix="host_session_restore_")) / "polaris_bootstrap.launched"
    return host, module.Session(cli, names, module.Clock(lambda: host.now, host.advance), marker, lines.append), lines


def failure(action: Callable[[], object]) -> str | None:
    try:
        action()
    except HostSessionError as error:
        return str(error)
    return None


def installs(host: RecordHost) -> list[str]:
    """The CODE_MODE of every code task the host ran."""
    return list(getattr(host, "modes", []))


def start_with_code_writes_the_record_outside_the_volume() -> dict[str, bool]:
    host, session, lines = build()
    session.start(code=(RUN_NEW, SHA_NEW))
    put = host.puts[-1] if host.puts else {}
    return {
        "the record holds the run ID and the digest": host.parameters.get(RECORD_NAME) == f"{RUN_NEW} {SHA_NEW}",
        "it is one overwrite of a String parameter under the named parameter": put.get("--type") == ["String"]
        and "--overwrite" in put
        and put.get("--name") == [RECORD_NAME],
        "the report names the run only": f"code_record_written: {RUN_NEW}" in lines and not any(SHA_NEW in line for line in lines),
    }


def start_without_flags_writes_the_identity_of_the_checkout() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    return {
        "the record is the run and digest that the host shows": host.parameters.get(RECORD_NAME) == f"{RUN_OLD} {SHA_OLD}",
        "one write": len(host.puts) == 1,
    }


def start_on_a_fresh_volume_uses_the_stored_record() -> dict[str, bool]:
    host, session, lines = build(checkout=None, code_run=None, record=f"{RUN_NEW} {SHA_NEW}")
    session.start()
    return {
        "the stored archive is installed": host.checkout == SHA_NEW and host.code_run == RUN_NEW,
        "the report says the stored record was used": f"code_record_used: {RUN_NEW}" in lines,
        "every reload carries the stored run and digest": bool(host.reloads)
        and all(item["launch"]["fetch"]["CODE_RUN_ID"] == RUN_NEW and item["launch"]["fetch"]["CODE_SHA256"] == SHA_NEW for item in host.reloads),
        "the record is unchanged in content": host.parameters.get(RECORD_NAME) == f"{RUN_NEW} {SHA_NEW}",
        "the host stays on": host.desired == 1,
    }


def start_on_a_fresh_volume_uses_a_stored_record_of_a_run_named_by_the_one_form() -> dict[str, bool]:
    """A code run staged under a pipeline run's ID (``<name>_<8 hex>``, ``training_contract.CODE_RUN``) is a code run record, like the commit time form."""
    run = "gold_abcdef01_2"
    host, session, lines = build(checkout=None, code_run=None, record=f"{run} {SHA_NEW}")
    session.start()
    return {
        "the stored archive is installed": host.checkout == SHA_NEW and host.code_run == run,
        "the report says the stored record was used": f"code_record_used: {run}" in lines,
        "the record is unchanged in content": host.parameters.get(RECORD_NAME) == f"{run} {SHA_NEW}",
    }


def start_prefers_the_record_of_the_checkout_over_the_stored_one() -> dict[str, bool]:
    host, session, lines = build(record=f"{RUN_NEW} {SHA_NEW}")
    session.start()
    return {
        "the checkout is kept": host.checkout == SHA_OLD,
        "the stored record is replaced by what the host shows": host.parameters.get(RECORD_NAME) == f"{RUN_OLD} {SHA_OLD}",
        "the stored record was not used": not any(line.startswith("code_record_used") for line in lines),
    }


def start_without_any_record_is_still_refused() -> dict[str, bool]:
    host, session, _ = build(checkout=None, code_run=None)
    message = failure(session.start)
    return {
        "start fails and deploys nothing": message is not None and "no service was deployed" in message,
        "nothing is deployed and the host is off": not any(entry["operation"] == "update-service" for entry in host.log) and host.desired == 0,
        "no record is written": not host.puts,
    }


def a_malformed_stored_record_is_refused_by_name() -> dict[str, bool]:
    host, session, _ = build(checkout=None, code_run=None, record="not a record")
    message = failure(session.start)
    return {
        "start fails and names the parameter form": message is not None and "code run record" in message and "unexpected form" in message,
        "nothing is deployed and the host is off": not any(entry["operation"] == "update-service" for entry in host.log) and host.desired == 0,
    }


def an_unreadable_stored_record_is_named_when_the_volume_is_fresh() -> dict[str, bool]:
    host, session, _ = build({"ssm_denied"}, checkout=None, code_run=None)
    message = failure(session.start)
    return {
        "start fails and names the unreadable record": message is not None and "code run record" in message and "AccessDenied" in message,
        "the host is off": host.desired == 0,
    }


def a_failed_record_write_is_reported_and_start_continues() -> dict[str, bool]:
    host, session, lines = build({"ssm_denied"})
    session.start(code=(RUN_NEW, SHA_NEW))
    return {
        "start finishes and the host stays on": host.desired == 1 and bool(host.reloads),
        "the report says the record was not written": any(line.startswith("code_record_not_written") for line in lines),
        "the report holds no digest": not any(SHA_NEW in line for line in lines),
    }


def a_session_without_the_parameter_name_neither_reads_nor_writes() -> dict[str, bool]:
    host, session, _ = build(named=False)
    session.start()
    return {"no parameter call": not any(entry["service"] == "ssm" for entry in host.log)}


def start_fails_by_name_when_the_database_setup_failed() -> dict[str, bool]:
    host, session, _ = build(setup_exits={"postgres_databases": 1})
    message = failure(session.start)
    return {
        "start fails and names the setup and the reason from its log": message is not None
        and "postgres setup failed" in message
        and "restore refused: database airflow" in message,
        "it says no service can start on half-restored data": message is not None and "half-restored" in message,
        "the host is turned off": host.desired == 0,
        "the online store is not reloaded": host.reloads == [],
    }


def start_fails_by_name_when_the_backup_fetch_failed() -> dict[str, bool]:
    host, session, _ = build(setup_exits={"postgres_restore_fetch": 1})
    message = failure(session.start)
    return {
        "start fails and names the fetch and the reason from its log": message is not None
        and "postgres setup failed" in message
        and "no completed backup set is usable" in message,
        "the host is turned off": host.desired == 0,
    }


def start_passes_when_the_setup_ended_well() -> dict[str, bool]:
    host, session, _ = build(setup_exits={"postgres_databases": 0, "postgres_restore_fetch": 0})
    session.start()
    return {
        "start finishes": bool(host.reloads) and host.desired == 1,
        "no setup log was read": not any(entry["service"] == "logs" and entry["options"]["--log-group-name"] == [POSTGRES_LOG_GROUP] for entry in host.log),
    }


def start_runs_the_mlflow_user_task_after_the_services_are_stable() -> dict[str, bool]:
    host, session, lines = build(mlflow_user=True)
    session.start()
    log = [entry for entry in host.log if entry["operation"] in {"run-task", "update-service"}]
    last_update = max(index for index, entry in enumerate(log) if entry["operation"] == "update-service")
    ran = [index for index, entry in enumerate(log) if entry["operation"] == "run-task" and entry["options"]["--task-definition"] == [MLFLOW_USER_FAMILY]]
    return {
        "the task runs once": len(ran) == 1,
        "it runs after the last deployment": bool(ran) and ran[0] > last_update,
        "the report says it ended": "mlflow_user_ready: ok" in lines,
        "the host stays on": host.desired == 1,
    }


def a_failed_mlflow_user_task_is_reported_and_start_continues() -> dict[str, bool]:
    host, session, lines = build({"exit_code:mlflow_user=1"}, mlflow_user=True)
    session.start()
    return {
        "start finishes and the online store is reloaded": host.desired == 1 and bool(host.reloads),
        "the report names the failed step and the way out": any(line.startswith("mlflow_user_failed") and "start or refresh" in line for line in lines),
    }


def a_skipped_mlflow_user_task_is_not_reported_ready_and_start_continues() -> dict[str, bool]:
    host, session, lines = build({"exit_code:mlflow_user=3"}, mlflow_user=True)
    session.start()
    return {
        "start finishes and the online store is reloaded": host.desired == 1 and bool(host.reloads),
        "the report says skipped and names the way out": any(
            line.startswith("mlflow_user_skipped") and "not stored" in line and "start or refresh" in line for line in lines
        ),
        "no ready and no failed line is reported": not any(line.startswith(("mlflow_user_ready", "mlflow_user_failed")) for line in lines),
    }


def a_stack_without_the_mlflow_user_task_runs_nothing_for_it() -> dict[str, bool]:
    host, session, lines = build()
    session.start()
    return {"no mlflow_user step is reported": not any(line.startswith("mlflow_user") for line in lines)}


def a_stack_without_setup_containers_is_not_checked() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    return {"start finishes": bool(host.reloads)}


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "restore_" + name: function
    for name, function in sorted(globals().items())
    if callable(function) and function.__module__ == __name__ and name not in {"build", "failure", "installs", "RecordHost"}
}
