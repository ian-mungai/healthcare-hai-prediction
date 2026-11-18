"""Host session cases for the restore after a teardown: the code run record kept outside the data volume and the check of the postgres task's database setup.

The stand-in host here adds the record object in the data bucket (``s3api get-object`` and ``put-object``) and a postgres service task whose setup
containers end with an exit code the case chooses.
A start on a fresh data volume finds no code record on the host, so it reads the record that an earlier start wrote to the data bucket.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure.host_session import CATALOG_LINE_LIMIT, HostSessionError, Session
from scripts.infrastructure.host_session_stand_in import CATALOG_SETUP_FAMILY, PROFILE, REGION, SERVICES, StandInError, StandInHost
from scripts.infrastructure.render_project_config import REPO_ROOT

TASK_DEFINITION_NEW = "arn:aws:ecs:stand-in:111111111111:task-definition/stand-in-postgres:9"
TASK_DEFINITION_OLD = "arn:aws:ecs:stand-in:111111111111:task-definition/stand-in-postgres:8"
OLD_TASK = "arn:aws:ecs:stand-in:111111111111:task/stand-in-cluster/postgrestask0000"
PENDING_KEY = "lakehouse/host_state/catalog_recovery_pending.json"
RECORD_BUCKET = "stand-in-data-bucket"
RECORD_KEY = "lakehouse/host_state/code_run_record.json"
MLFLOW_USER_FAMILY = "stand-in-mlflow-user"
POSTGRES_LOG_GROUP = "stand-in-postgres-log-group"
POSTGRES_TASK = "arn:aws:ecs:stand-in:111111111111:task/stand-in-cluster/postgrestask0001"
RUN_NEW = "20261117T150000Z-cccccccc"
SHA_NEW = "c" * 64
RUN_OLD = "20261108T130000Z-bbbbbbbb"
SHA_OLD = "b" * 64
CASE_ERRORS = (AssertionError, LookupError, StopIteration, TypeError, ValueError, AttributeError, NameError, OSError, HostSessionError)


class RecordHost(StandInHost):
    """A stand-in host with the code record object and a postgres task. ``setup_exits`` maps a setup container of the postgres task to its exit code, to
    None while it runs or to "PENDING" while it has not started. ``old_setup`` adds a task of an older task definition whose setup containers all ended well.
    ``restored`` makes the databases container log a restore."""

    def __init__(
        self,
        faults: set[str] | None = None,
        setup_exits: Mapping[str, int | str | None] | None = None,
        old_setup: bool = False,
        restored: bool = False,
        **fields: Any,
    ) -> None:
        super().__init__(faults, code_fetch=True, **fields)
        self.objects: dict[str, str] = {}
        self.puts: list[dict[str, list[str]]] = []
        self.setup_exits, self.old_setup, self.restored = setup_exits, old_setup, restored
        self.marker_at_launch: list[bool] = []
        self.handlers[("s3api", "get-object")] = self.s3_get_object
        self.handlers[("s3api", "put-object")] = self.s3_put_object
        self.handlers[("s3api", "delete-object")] = self.s3_delete_object

    @property
    def record(self) -> str | None:
        """The code run record object, or None."""
        return self.objects.get(RECORD_KEY)

    @record.setter
    def record(self, value: str | None) -> None:
        if value is None:
            self.objects.pop(RECORD_KEY, None)
        else:
            self.objects[RECORD_KEY] = value

    def denied(self, key: str) -> bool:
        """Every object call is refused by ``s3_denied``; only the pending-recovery marker by ``s3_denied_pending``."""
        return "s3_denied" in self.faults or ("s3_denied_pending" in self.faults and key == PENDING_KEY)

    def read_denied(self, key: str) -> bool:
        """Only reads of the pending marker are refused (``s3_denied_pending_read``); a write still works."""
        return self.denied(key) or ("s3_denied_pending_read" in self.faults and key == PENDING_KEY)

    def s3_get_object(self, options: dict[str, list[str]]) -> dict[str, Any]:
        key, target = options["--key"][0], options["--key"][1]
        if self.read_denied(key):
            raise StandInError("AccessDenied")
        if key not in self.objects:
            raise StandInError("NoSuchKey")
        Path(target).write_text(self.objects[key], encoding="utf-8")
        return {"ContentLength": len(self.objects[key])}

    def s3_put_object(self, options: dict[str, list[str]]) -> dict[str, Any]:
        key = options["--key"][0]
        if self.denied(key):
            raise StandInError("AccessDenied")
        self.puts.append(options)
        self.objects[key] = Path(options["--body"][0]).read_text(encoding="utf-8").strip()
        return {"ETag": "stand-in"}

    def s3_delete_object(self, options: dict[str, list[str]]) -> dict[str, Any]:
        key = options["--key"][0]
        if self.denied(key) or "s3_delete_denied" in self.faults:
            raise StandInError("AccessDenied")
        self.objects.pop(key, None)
        return {}

    def catalog_setup_exit(self, options: dict[str, list[str]], code: int) -> int:
        self.marker_at_launch.append(PENDING_KEY in self.objects)
        return super().catalog_setup_exit(options, code)

    def ecs_list_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if self.setup_exits is not None and options.get("--service-name") == [SERVICES["postgres"]]:
            return {"taskArns": [*([OLD_TASK] if self.old_setup else []), POSTGRES_TASK]}
        return super().ecs_list_tasks(options)

    def ecs_describe_services(self, options: dict[str, list[str]]) -> dict[str, Any]:
        described = super().ecs_describe_services(options)
        if self.setup_exits is not None:
            for service in described.get("services", []):
                if service["serviceName"] == SERVICES["postgres"]:
                    service["deployments"] = [{**item, "status": "PRIMARY", "taskDefinition": TASK_DEFINITION_NEW} for item in service["deployments"]]
        return described

    def containers_of(self, states: Mapping[str, int | str | None]) -> list[dict[str, Any]]:
        containers: list[dict[str, Any]] = [{"name": "postgres", "lastStatus": "RUNNING"}]
        for name, state in states.items():
            if isinstance(state, int):
                containers.append({"name": name, "lastStatus": "STOPPED", "exitCode": state})
            else:
                containers.append({"name": name, "lastStatus": "PENDING" if state == "PENDING" else "RUNNING"})
        return containers

    def ecs_describe_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        if self.setup_exits is not None and options["--tasks"] and set(options["--tasks"]) <= {POSTGRES_TASK, OLD_TASK}:
            found = []
            for arn in options["--tasks"]:
                states = {name: 0 for name in self.setup_exits} if arn == OLD_TASK else self.setup_exits
                definition = TASK_DEFINITION_OLD if arn == OLD_TASK else TASK_DEFINITION_NEW
                found.append({"taskArn": arn, "taskDefinitionArn": definition, "lastStatus": "RUNNING", "containers": self.containers_of(states)})
            return {"tasks": found, "failures": []}
        return super().ecs_describe_tasks(options)

    def logs_get_log_events(self, options: dict[str, list[str]]) -> dict[str, Any]:
        stream = options["--log-stream-name"][0]
        if options["--log-group-name"] == [POSTGRES_LOG_GROUP] and "log_denied" in self.faults:
            raise StandInError("AccessDeniedException")
        if options["--log-group-name"] == [POSTGRES_LOG_GROUP]:
            reasons = {
                "postgres_databases": "postgres_databases: restore refused: database airflow: pg_restore failed or passed its 1200 second deadline",
                "postgres_restore_fetch": "pg_restore_fetch failed: no completed backup set is usable: the dump of airflow differs from its marker",
            }
            name = stream.split("/")[0]
            if name not in reasons or not stream.endswith("/postgrestask0001"):
                raise StandInError("ResourceNotFoundException")
            if self.restored and (self.setup_exits or {}).get(name) == 0:
                return {"events": [{"message": "postgres_databases: restored database polaris in place"}], "nextForwardToken": "f/0"}
            return {"events": [{"message": "unrelated line"}, {"message": reasons[name]}], "nextForwardToken": "f/0"}
        return super().logs_get_log_events(options)


def build(
    faults: set[str] | None = None,
    *,
    checkout: str | None = SHA_OLD,
    code_run: str | None = RUN_OLD,
    record: str | None = None,
    setup_exits: Mapping[str, int | str | None] | None = None,
    named: bool = True,
    mlflow_user: bool = False,
    old_setup: bool = False,
    restored: bool = False,
    catalog_setup: bool = True,
    host: RecordHost | None = None,
    log_group: str = POSTGRES_LOG_GROUP,
    pending_file: Path | None = None,
) -> tuple[RecordHost, Session, list[str]]:
    """A session over the stand-in. ``record`` is the value of the code run record; ``named`` gives the session the parameter and log group names;
    ``catalog_setup`` gives the stack the catalog setup one-shot, which the restore step runs in."""
    from dataclasses import replace

    cases = importlib.import_module("scripts.infrastructure.host_session_cases")
    module = importlib.import_module("scripts.infrastructure.host_session")  # the module as it is loaded now: a plant replaces it in sys.modules
    lines = list[str]()
    if host is None:
        host = RecordHost(faults, setup_exits, old_setup, restored, checkout=checkout, code_run=code_run, catalog_setup=catalog_setup)
        host.record = record
    else:
        host.faults, host.setup_exits, host.restored = set(faults or ()), setup_exits, restored
    if mlflow_user:
        host.families["mlflow_user"] = MLFLOW_USER_FAMILY
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION, "data_bucket_name": RECORD_BUCKET, "expected_account_id": "1" * 12}, host.runner)
    names = replace(cases.names(), code_record_bucket=RECORD_BUCKET, postgres_log_group=log_group) if named else cases.names()
    if mlflow_user:
        names = replace(names, one_shot={**names.one_shot, "mlflow_user": MLFLOW_USER_FAMILY})
    if catalog_setup:
        names = replace(names, one_shot={**names.one_shot, "catalog_setup": CATALOG_SETUP_FAMILY})
    marker = cases.Path(cases.tempfile.mkdtemp(prefix="host_session_restore_")) / "polaris_bootstrap.launched"
    session = module.Session(cli, names, module.Clock(lambda: host.now, host.advance), marker, lines.append)
    session.pending_file = pending_file or marker.parent / "catalog_recovery.pending"
    return host, session, lines


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
        "the record holds the run ID and the digest": host.record == f"{RUN_NEW} {SHA_NEW}",
        "it is one put of the record key": put.get("--key") == [RECORD_KEY],
        "the report names the run only": f"code_record_written: {RUN_NEW}" in lines and not any(SHA_NEW in line for line in lines),
    }


def start_without_flags_writes_the_identity_of_the_checkout() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    return {
        "the record is the run and digest that the host shows": host.record == f"{RUN_OLD} {SHA_OLD}",
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
        "the record is unchanged in content": host.record == f"{RUN_NEW} {SHA_NEW}",
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
        "the record is unchanged in content": host.record == f"{run} {SHA_NEW}",
    }


def start_prefers_the_record_of_the_checkout_over_the_stored_one() -> dict[str, bool]:
    host, session, lines = build(record=f"{RUN_NEW} {SHA_NEW}")
    session.start()
    return {
        "the checkout is kept": host.checkout == SHA_OLD,
        "the stored record is replaced by what the host shows": host.record == f"{RUN_OLD} {SHA_OLD}",
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
        "start fails and names the record form": message is not None and "code run record" in message and "unexpected form" in message,
        "nothing is deployed and the host is off": not any(entry["operation"] == "update-service" for entry in host.log) and host.desired == 0,
    }


def an_unreadable_stored_record_is_named_when_the_volume_is_fresh() -> dict[str, bool]:
    host, session, _ = build({"s3_denied"}, checkout=None, code_run=None)
    message = failure(session.start)
    return {
        "start fails and names the unreadable record": message is not None and "code run record" in message and "AccessDenied" in message,
        "the host is off": host.desired == 0,
    }


def a_failed_record_write_is_reported_and_start_continues() -> dict[str, bool]:
    host, session, lines = build({"s3_denied"})
    session.start(code=(RUN_NEW, SHA_NEW))
    return {
        "start finishes and the host stays on": host.desired == 1 and bool(host.reloads),
        "the report says the record was not written": any(line.startswith("code_record_not_written") for line in lines),
        "the report holds no digest": not any(SHA_NEW in line for line in lines),
    }


def a_session_without_the_bucket_name_neither_reads_nor_writes() -> dict[str, bool]:
    host, session, _ = build(named=False)
    session.start()
    return {"no record call": not any(entry["service"] == "s3api" for entry in host.log)}


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
        "only the databases container log is read, to see whether a restore ran": all(
            entry["options"]["--log-stream-name"][0].startswith("postgres_databases/")
            for entry in host.log
            if entry["service"] == "logs" and entry["options"]["--log-group-name"] == [POSTGRES_LOG_GROUP]
        ),
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


def start_waits_for_a_running_fetch_and_a_pending_setup() -> dict[str, bool]:
    host, session, _ = build(setup_exits={"postgres_restore_fetch": None, "postgres_databases": "PENDING"})
    message = failure(session.start)
    return {
        "start does not call the services stable while the setup has not completed": message is not None and "services stable" in message,
        "the message names the unfinished containers and their states": message is not None
        and "postgres_restore_fetch" in message
        and "RUNNING" in message
        and "postgres_databases" in message
        and "PENDING" in message,
        "nothing is reloaded and the host is turned off": host.reloads == [] and host.desired == 0,
    }


def start_needs_both_setup_containers_to_have_ended_well() -> dict[str, bool]:
    checks = {}
    for name, states in (
        ("fetch ended, databases still running", {"postgres_restore_fetch": 0, "postgres_databases": None}),
        ("databases ended, fetch still running", {"postgres_restore_fetch": None, "postgres_databases": 0}),
    ):
        host, session, _ = build(setup_exits=states)
        message = failure(session.start)
        checks[f"{name}: start fails and reloads nothing"] = message is not None and "services stable" in message and host.reloads == []
    return checks


def a_setup_that_ended_in_an_older_task_definition_does_not_count() -> dict[str, bool]:
    host, session, _ = build(setup_exits={"postgres_restore_fetch": 0, "postgres_databases": None}, old_setup=True)
    message = failure(session.start)
    return {"start fails although the older task finished its setup": message is not None and "services stable" in message and host.reloads == []}


def a_setup_of_the_current_task_definition_with_an_older_task_still_listed_passes() -> dict[str, bool]:
    host, session, _ = build(setup_exits={"postgres_restore_fetch": 0, "postgres_databases": 0}, old_setup=True)
    session.start()
    return {"start finishes": bool(host.reloads) and host.desired == 1}


def start_restores_the_catalog_after_a_restored_database() -> dict[str, bool]:
    host, session, lines = build(setup_exits={"postgres_restore_fetch": 0, "postgres_databases": 0}, restored=True)
    host.restore_counts = {"advanced": 1, "registered": 2}
    session.start()
    return {
        "the catalog restore runs once as the restore step of the catalog setup one-shot": host.catalog_launches == [["--seconds", "900", "restore"]],
        "it runs before the online store reload": host.catalog_launch_reloads == [0] and bool(host.reloads),
        "the report names the counts": any(line.startswith("catalog_restored") and "advanced 1" in line and "registered 2" in line for line in lines),
    }


def start_does_not_touch_the_catalog_when_nothing_was_restored() -> dict[str, bool]:
    host, session, lines = build(setup_exits={"postgres_restore_fetch": 0, "postgres_databases": 0}, restored=False)
    session.start()
    return {"no catalog restore launch": host.catalog_launches == [], "no catalog report line": not any(line.startswith("catalog_restored") for line in lines)}


def a_failed_catalog_restore_fails_start_by_name_and_stops_the_host() -> dict[str, bool]:
    host, session, lines = build({"restore_refuses"}, setup_exits={"postgres_restore_fetch": 0, "postgres_databases": 0}, restored=True)
    message = failure(session.start)
    return {
        "start fails and names the catalog restore and its reason": message is not None and "catalog restore failed" in message and "namespace gold" in message,
        "the refusal line is shown": any(line.startswith("catalog_setup_refused:") and "namespace gold" in line for line in lines),
        "nothing is reloaded and the host is off": host.reloads == [] and host.desired == 0,
    }


def a_failed_catalog_restore_keeps_the_host_with_keep_on() -> dict[str, bool]:
    host, session, lines = build({"restore_refuses"}, setup_exits={"postgres_restore_fetch": 0, "postgres_databases": 0}, restored=True)
    message = failure(lambda: session.start(keep_on=True))
    return {"start fails": message is not None, "the host is left on": host.desired == 1 and any(line.startswith("host_kept_on") for line in lines)}


def a_restored_database_on_a_stack_without_the_one_shot_fails_start_by_name() -> dict[str, bool]:
    host, session, _ = build(setup_exits={"postgres_restore_fetch": 0, "postgres_databases": 0}, restored=True, catalog_setup=False)
    message = failure(session.start)
    return {
        "start fails and names the missing one-shot": message is not None
        and "catalog restore failed" in message
        and "catalog_setup one-shot is missing" in message,
        "nothing is launched and nothing is reloaded": host.catalog_launches == [] and host.reloads == [],
    }


def a_result_line_that_is_not_the_restore_reply_fails_start() -> dict[str, bool]:
    host, session, _ = build(setup_exits={"postgres_restore_fetch": 0, "postgres_databases": 0}, restored=True)
    host.restore_counts = {"registered": "many"}
    message = failure(session.start)
    return {"start fails and says the line is not the restore reply": message is not None and "not the restore reply" in message}


SETUP_OK = {"postgres_restore_fetch": 0, "postgres_databases": 0}


def published_table_rows() -> list[dict[str, str]]:
    """The outcome of every table the published layers hold today (each ledger included), named from the checked contracts: a normal restore reply."""
    rows: list[dict[str, str]] = []
    for filename in ("gold_contract", "silver_publish", "features_publish"):
        contract = json.loads((REPO_ROOT / "data_contracts" / "lakehouse" / f"{filename}.json").read_text(encoding="utf-8"))["publish"]
        namespace = contract["namespaces"]["real"]
        rows += [
            {"table": f"{namespace}.{name}", "backup": f"{namespace}_aaaaaaaa", "outcome": "registered"}
            for name in sorted([*contract["tables"], contract["runs_table"]])
        ]
    return rows


def a_restore_reply_with_every_published_table_completes_the_start() -> dict[str, bool]:
    rows = published_table_rows()
    host, session, lines = build(setup_exits=SETUP_OK, restored=True)
    host.restore_tables, host.restore_counts = rows, {"registered": len(rows)}
    message = failure(session.start)
    shown = [line for line in lines if line.startswith("catalog_setup_result:")]
    return {
        "the reply is longer than the display limit": len(json.dumps(host.restore_tables)) > CATALOG_LINE_LIMIT,
        "start completes": message is None and bool(host.reloads) and host.desired == 1,
        "the report names every table in the counts": any(line.startswith("catalog_restored") and f"registered {len(rows)}" in line for line in lines),
        "the pending marker is cleared": PENDING_KEY not in host.objects,
        "the displayed result line is cut to the display limit and says so": len(shown) == 1
        and len(shown[0]) <= CATALOG_LINE_LIMIT + 200
        and "cut" in shown[0],
    }


def a_failed_catalog_recovery_is_retried_by_the_next_start() -> dict[str, bool]:
    host, session, _ = build({"restore_refuses"}, setup_exits=SETUP_OK, restored=True)
    first = failure(session.start)
    kept = PENDING_KEY in host.objects
    host2, session2, lines2 = build(setup_exits=SETUP_OK, restored=False, host=host)
    session2.start()
    return {
        "the first start fails by name": first is not None and "catalog restore failed" in first,
        "the marker stays after the failure": kept,
        "the next start runs the recovery although nothing was restored in it": len(host.catalog_launches) == 2 and host2 is host,
        "the recovery succeeded and the marker is cleared": PENDING_KEY not in host.objects and any(line.startswith("catalog_restored") for line in lines2),
        "the start after that runs no recovery": run_again_skips(host),
    }


def run_again_skips(host: RecordHost) -> bool:
    launches = len(host.catalog_launches)
    _, session, _ = build(setup_exits=SETUP_OK, restored=False, host=host)
    session.start()
    return len(host.catalog_launches) == launches


def the_pending_marker_is_written_before_the_recovery_and_cleared_after_it() -> dict[str, bool]:
    host, session, lines = build(setup_exits=SETUP_OK, restored=True)
    session.start()
    return {
        "the recovery found the marker already written": host.marker_at_launch == [True],
        "the marker is cleared after the recovery succeeded": PENDING_KEY not in host.objects,
        "the report names the recovery": any(line.startswith("catalog_restored") for line in lines),
    }


def an_unreadable_setup_log_runs_the_recovery() -> dict[str, bool]:
    host, session, _ = build({"log_denied"}, setup_exits=SETUP_OK, restored=False)
    session.start()
    return {"the recovery ran once": len(host.catalog_launches) == 1, "start finished": bool(host.reloads) and host.desired == 1}


def an_unknown_log_group_runs_the_recovery() -> dict[str, bool]:
    host, session, _ = build(setup_exits=SETUP_OK, restored=False, log_group="")
    session.start()
    return {"the recovery ran once": len(host.catalog_launches) == 1}


def an_unreadable_pending_marker_runs_the_recovery() -> dict[str, bool]:
    host, session, lines = build({"s3_denied_pending_read"}, setup_exits=SETUP_OK, restored=False)
    session.start()
    return {
        "the recovery ran once": len(host.catalog_launches) == 1,
        "start finished": bool(host.reloads),
        "the marker was written first and cleared after": PENDING_KEY not in host.objects,
    }


def a_marker_that_cannot_be_written_fails_the_start_before_any_recovery() -> dict[str, bool]:
    host, session, lines = build({"s3_denied_pending"}, setup_exits=SETUP_OK, restored=True)
    message = failure(session.start)
    return {
        "start fails and names the pending marker": message is not None and "pending marker" in message and "not started" in message,
        "no recovery ran": host.catalog_launches == [],
        "the host is turned off and nothing is reloaded": host.desired == 0 and host.reloads == [],
        "the obligation is kept on the Mac": session.pending_file.exists(),
    }


def the_next_start_with_a_working_store_writes_the_marker_and_recovers() -> dict[str, bool]:
    host, session, _ = build({"s3_denied_pending"}, setup_exits=SETUP_OK, restored=True)
    first = failure(session.start)
    host2, session2, lines2 = build(setup_exits=SETUP_OK, restored=False, host=host, pending_file=session.pending_file)
    session2.start()
    return {
        "the first start failed closed": first is not None,
        "the next start recovered although the restore log shows no restore": len(host.catalog_launches) == 1 and host.marker_at_launch == [True],
        "both records are cleared after the recovery succeeded": PENDING_KEY not in host.objects and not session.pending_file.exists(),
        "the report names the recovery": any(line.startswith("catalog_restored") for line in lines2),
    }


def a_marker_that_cannot_be_recorded_anywhere_fails_the_start_and_says_so() -> dict[str, bool]:
    folder = Path(importlib.import_module("tempfile").mkdtemp(prefix="pending_blocked_"))
    blocker = folder / "blocked"
    blocker.write_text("a file where a folder is needed", encoding="utf-8")
    host, session, _ = build({"s3_denied_pending"}, setup_exits=SETUP_OK, restored=True, pending_file=blocker / "catalog_recovery.pending")
    message = failure(session.start)
    return {
        "start fails and says no record could be made": message is not None and "pending marker" in message and "by hand" in message,
        "no recovery ran": host.catalog_launches == [] and host.desired == 0,
    }


def a_pending_marker_of_an_earlier_start_runs_the_recovery_and_is_cleared() -> dict[str, bool]:
    host, session, _ = build(setup_exits=SETUP_OK, restored=False)
    host.objects[PENDING_KEY] = "pending"
    session.start()
    return {"the recovery ran once": len(host.catalog_launches) == 1, "the marker is cleared": PENDING_KEY not in host.objects}


def a_marker_that_cannot_be_cleared_is_reported_and_start_continues() -> dict[str, bool]:
    host, session, lines = build({"s3_delete_denied"}, setup_exits=SETUP_OK, restored=True)
    session.start()
    return {
        "start finished": bool(host.reloads) and host.desired == 1,
        "the marker stays so the next start retries": PENDING_KEY in host.objects,
        "the report says the marker was not cleared": any(line.startswith("catalog_recovery_marker_not_cleared") for line in lines),
    }


def a_readable_log_without_a_restore_and_no_marker_runs_no_recovery() -> dict[str, bool]:
    host, session, _ = build(setup_exits=SETUP_OK, restored=False)
    session.start()
    return {"no recovery ran": host.catalog_launches == [], "no marker was written": PENDING_KEY not in host.objects}


def a_stack_without_setup_containers_is_not_checked() -> dict[str, bool]:
    host, session, _ = build()
    session.start()
    return {"start finishes": bool(host.reloads)}


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "restore_" + name: function
    for name, function in sorted(globals().items())
    if callable(function)
    and function.__module__ == __name__
    and name not in {"build", "failure", "installs", "RecordHost", "run_again_skips", "published_table_rows"}
}
