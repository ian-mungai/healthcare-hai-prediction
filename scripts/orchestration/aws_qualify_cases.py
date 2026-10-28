"""Exercise closed AWS qualification dispatch with synthetic ECS and catalog bindings."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

from orchestration import ecs_admission, ecs_dispatch, ecs_target, launcher, pipeline_render
from scripts.orchestration import ecs_dispatch_cases

ROOT = Path(__file__).resolve().parents[2]
MODE = "aws_qualify"
RUN = "20261025T000000Z-01234567"
SOURCES = (
    "orchestration/aws_qualify.py",
    "orchestration/pipeline_render.py",
    "orchestration/ecs_target.py",
    "orchestration/ecs_dispatch.py",
    "orchestration/ecs_admission.py",
    "data_contracts/orchestration/ledger_line.schema.json",
    "scripts/orchestration/ecs_target_cases.py",
    "orchestration/launcher.py",
    "config/orchestration/ecs_jobs.json",
    "config/orchestration/ecs_jobs.schema.json",
    "data_contracts/orchestration/aws_qualification.schema.json",
    "data_contracts/orchestration/ecs_job.schema.json",
    "data_contracts/orchestration/ecs_target.schema.json",
    "data_contracts/orchestration/execution.schema.json",
    "scripts/orchestration/aws_qualify_cases.py",
    "scripts/orchestration/ecs_dispatch_cases.py",
)


def expected(layer: str, step: str) -> dict[str, Any]:
    """Return literal expectations from the qualification contract."""
    namespace = {"silver": "silver_e2e", "gold": "gold_e2e", "features": "features_e2e"}[layer]
    table = "lakehouse/" + namespace + "/"
    backup = table + "catalog_backups/"
    read = step == "reconcile"
    return {
        "mode": MODE,
        "layer": layer,
        "step": step,
        "job_class": "lakehouse_read" if read else "lakehouse_write",
        "namespace": namespace,
        "table_prefix": table,
        "backup_prefix": backup,
        "output_prefixes": [] if read else [backup if step == "backup" else table],
        "definition": "spark_read" if read else "spark_write_e2e",
        "task_role_key": "read_task" if read else "write_e2e_task",
        "catalog_principal": "lakehouse_reader" if read else "e2e_writer",
    }


def refresh(job: dict[str, Any], reference: dict[str, Any]) -> None:
    """Bind the changed synthetic staged job to its own immutable reference."""
    raw = ecs_dispatch.canonical(job)
    reference.update(sha256=hashlib.sha256(raw).hexdigest(), size=len(raw))


def fixture(folder: Path, *, layer: str = "gold", step: str = "commit") -> tuple[Any, ...]:
    """Use the real admission and reservation with a durable stand-in registry."""
    store, api, dispatcher, target, job, reference = ecs_dispatch_cases.fixture(folder)
    captures = json.loads((ROOT / "data_contracts/orchestration/examples/ecs_target_inputs.json").read_text())
    read = step == "reconcile"
    wanted = expected(layer, step)
    role_key = wanted["task_role_key"]
    target["job_roles"]["write_e2e_task"] = {
        "name": target["project_tag"] + "_job_write_e2e_task",
        "arn": f"arn:aws:iam::{target['account_id']}:role/{target['project_tag']}_job_write_e2e_task",
        "unique_id": "AROA" + "E" * 17,
    }
    description = {
        "task_definition_arn": target["definitions"]["spark_read"]["task_definition_arn"],
        "status": "ACTIVE",
        "container": json.loads(captures["jobs_outputs"]["container_definitions"]["value"]["spark_read"])[0],
        "task": captures["jobs_outputs"]["task_definition_settings"]["value"]["spark_read"],
    }
    writer_description = copy.deepcopy(description)
    family = target["project_tag"] + "_spark_write_e2e"
    writer_description["task_definition_arn"] = f"arn:aws:ecs:{target['region']}:{target['account_id']}:task-definition/{family}:1"
    writer_description["task"].update(family=family, task_role_arn=target["job_roles"]["write_e2e_task"]["arn"])
    prefix = f"arn:aws:ssm:{target['region']}:{target['account_id']}:parameter/{target['project_tag']}/aws_polaris_e2e_client_"
    writer_description["container"]["secrets"] = [
        {"name": "POLARIS_CLIENT_ID", "valueFrom": prefix + "id"},
        {"name": "POLARIS_CLIENT_SECRET", "valueFrom": prefix + "secret"},
    ]
    target["class_networks"]["lakehouse_write"] = "lakehouse"
    target["definitions"]["spark_write_e2e"] = {
        **target["definitions"]["spark_read"],
        "task_definition_arn": writer_description["task_definition_arn"],
        "job_class": "lakehouse_write",
        "task_role_key": "write_e2e_task",
        "execution_sha256": ecs_dispatch.execution_pin(writer_description["container"], writer_description["task"]),
    }
    if not read:
        description = writer_description
    api.description = description
    attempt = ecs_dispatch_cases.initial_attempt()
    attempt.update(template="pipeline_publish", params={"layer": layer, "step": step})
    state = {"launcher": {"catalog_authority": ecs_dispatch_cases.authority()}, "attempts": {attempt["id"]: attempt}}
    ecs_admission.bind(
        state,
        attempt,
        job_class=wanted["job_class"],
        registry_kind="rehearsal",
        run={"id": RUN, "mode": MODE},
        layer=layer,
        authorization=None if read else {"qualification": "synthetic_qualification", "layer_activation": None, "publish_approval": None},
    )
    decision = ecs_target.select(attempt, wanted["job_class"], ecs_target.Size(1024, 4096, 21), ecs_target.Availability(), ecs_dispatch_cases.NOW)
    placement = ecs_target.reserve(attempt, ecs_target.Size(1024, 4096, 21), decision, ecs_dispatch_cases.NOW, ecs_target.Usage())
    store.write(state)
    job.update(
        template="pipeline_publish",
        job_class=wanted["job_class"],
        task_role_id=target["job_roles"][role_key]["unique_id"],
        qualification=wanted,
        secret_names=["POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET"],
        catalog_authority=copy.deepcopy(attempt["execution"]["catalog_authority"]),
        plain_environment={"POLARIS_URI": target["catalog"]["endpoint"]},
    )
    if not read:
        job["writer_authorization"] = copy.deepcopy(attempt["execution"]["writer_authorization"])
    job["command"] = [
        "python",
        "-m",
        f"scripts.lakehouse.{layer}_publish",
        step,
        "--run",
        RUN,
        "--mode",
        MODE,
        "--sealed-sha256",
        "a" * 64,
        "--accepted-result-sha256",
        "b" * 64,
    ]
    if step == "maintenance":
        job["command"] = [
            "python",
            "-m",
            f"scripts.lakehouse.{layer}_maintenance",
            "--run",
            RUN,
            "--mode",
            MODE,
            "--protected",
            "/workspace/out/publish/protected.json",
        ]
    reference["key"] = f"lakehouse/run_stage/{RUN}/g1/tries/{placement['try_id']}/job.json"
    refresh(job, reference)
    return store, api, dispatcher, target, job, reference


def submit(data: tuple[Any, ...]) -> dict[str, Any]:
    """Submit through the same dispatcher that owns the persisted placement."""
    store, _api, dispatcher, target, job, reference = data
    attempt = store.read()["attempts"]["synthetic_attempt"]
    return dispatcher.submit(
        attempt["id"], attempt["execution"]["live_try"], target=target, definition_name=job["qualification"]["definition"], job=job, job_reference=reference
    )


def launcher_refusal_cases(checks: dict[str, bool], observations: dict[str, Any]) -> None:
    """Call the real submit handler without starting a launcher or granting ECS eligibility."""
    server = object.__new__(launcher.Launcher)
    server.ecs_jobs = ecs_target.load_jobs(ROOT)
    server.ecs_runtime = None
    observations["launcher_refusals"] = {}
    for template, code, cause in (
        ("gold_export", "refused", "this template is ineligible"),
        ("pipeline_dbt", "args", "only layer and step on pipeline_publish"),
    ):
        context = launcher.Ctx(
            conn=object.__new__(launcher.Conn),
            method="submit",
            request_id="qualification_refusal",
            params={"template": template, "target": "ecs", "params": {"layer": "gold", "step": "commit"}},
            principal={"kind": "run", "run_id": RUN},
            key="qualification_refusal",
            state={"runs": {RUN: {"mode": MODE}}},
        )
        try:
            server.m_submit(context)
        except launcher.LauncherError as error:
            checks["launcher_refuses_" + template] = error.code == code and cause in str(error)
            observations["launcher_refusals"][template] = {"code": error.code, "message": str(error)}
        except ecs_target.PlacementError as error:
            checks["launcher_refuses_" + template] = False
            observations["launcher_refusals"][template] = {"uncaught": type(error).__name__, "message": str(error)}
        else:
            checks["launcher_refuses_" + template] = False
        try:
            row = ecs_target.row_for(server.ecs_jobs, template, context.params["params"], "offline", mode=MODE)
        except ecs_target.PlacementError:
            checks["no_qualification_row_" + template] = False
        else:
            checks["no_qualification_row_" + template] = row is None


def replay_cases(folder: Path, checks: dict[str, bool], observations: dict[str, Any]) -> None:
    """Change one persisted M1 field and require refusal before any transport call."""
    data = fixture(folder)
    submit(data)
    original = data[0].read()
    placement = original["attempts"]["synthetic_attempt"]["execution"]["placements"][0]
    record = placement["dispatch"]["qualification"]
    changes = (
        ("role_name", {"task_role": {**data[3]["job_roles"]["read_task"], "unique_id": record["task_role"]["unique_id"]}}),
        (
            "role_project",
            {
                "task_role": {
                    **record["task_role"],
                    "name": "another_project_job_write_e2e_task",
                    "arn": record["task_role"]["arn"].rsplit("/", 1)[0] + "/another_project_job_write_e2e_task",
                }
            },
        ),
        ("role_arn", {"task_role": {**record["task_role"], "arn": data[3]["job_roles"]["read_task"]["arn"]}}),
        (
            "role_account",
            {"task_role": {**record["task_role"], "arn": record["task_role"]["arn"].replace("::" + data[3]["account_id"] + ":", "::999999999999:")}},
        ),
        ("role_unique_id", {"task_role": {**record["task_role"], "unique_id": "AROA" + "F" * 17}}),
        ("step", {"step": "candidates"}),
        ("run_id", {"run_id": "20261025T000001Z-01234567"}),
        ("try_id", {"try_id": "try_changed"}),
        ("task_definition_arn", {"task_definition_arn": record["task_definition_arn"].rsplit(":", 1)[0] + ":2"}),
        ("definition", {"definition": "spark_read"}),
        ("capacity_provider", {"capacity_provider": "FARGATE_SPOT"}),
        ("task_role_key", {"task_role_key": "read_task"}),
        ("job_class", {"job_class": "lakehouse_read"}),
        ("target", {"target": ecs_target.TARGET_DOCKER}),
        ("mode", {"mode": "publish_fixture"}),
        ("layer", {"layer": "silver"}),
        ("namespace", {"namespace": "gold"}),
        ("table_prefix", {"table_prefix": "lakehouse/gold/"}),
        ("backup_prefix", {"backup_prefix": "lakehouse/catalog_backups/"}),
        ("output_prefixes", {"output_prefixes": ["lakehouse/gold/"]}),
        ("catalog_principal", {"catalog_principal": "lakehouse_reader"}),
        ("schema_version", {"schema_version": 2}),
        ("evidence", {"evidence": "native_writer"}),
        ("native_qualification_passed", {"native_qualification_passed": True}),
        ("extra_field", {"unrecognized": True}),
    )
    observed = observations["replay_refusals"] = {}

    def request_try(dispatch: dict[str, Any]) -> None:
        dispatch["request"]["startedBy"] = "try_changed"
        dispatch["request_sha256"] = hashlib.sha256(ecs_dispatch.canonical(dispatch["request"])).hexdigest()

    dispatch_changes: tuple[tuple[str, Callable[[dict[str, Any]], None]], ...] = (
        ("request_try_id", request_try),
        ("missing_job_inputs", lambda dispatch: dispatch.pop("qualification_job", None)),
        ("job_step", lambda dispatch: dispatch["qualification_job"].update(step="candidates")),
        ("job_role_id", lambda dispatch: dispatch["qualification_job"].update(task_role_id="AROA" + "F" * 17)),
        ("definition_arn", lambda dispatch: dispatch["definition"].update(task_definition_arn=record["task_definition_arn"].rsplit(":", 1)[0] + ":2")),
        ("definition_class", lambda dispatch: dispatch["definition"].update(job_class="lakehouse_read")),
    )
    for name, alter in dispatch_changes:
        state = copy.deepcopy(original)
        alter(state["attempts"]["synthetic_attempt"]["execution"]["placements"][0]["dispatch"])
        data[0].write(state)
        calls_before = len(data[1].calls)
        try:
            submit(data)
        except ecs_target.PlacementError as error:
            checks["replay_refuses_" + name] = "qualification M1 dispatch record" in str(error) and len(data[1].calls) == calls_before
            observed[name] = {"message": str(error), "transport_calls": len(data[1].calls) - calls_before}
        else:
            checks["replay_refuses_" + name] = False
            observed[name] = {"accepted": True, "transport_calls": len(data[1].calls) - calls_before}
    for name, changed in changes:
        state = copy.deepcopy(original)
        state["attempts"]["synthetic_attempt"]["execution"]["placements"][0]["dispatch"]["qualification"].update(changed)
        data[0].write(state)
        calls_before = len(data[1].calls)
        try:
            submit(data)
        except ecs_target.PlacementError as error:
            checks["replay_refuses_" + name] = "qualification M1 dispatch record" in str(error) and len(data[1].calls) == calls_before
            observed[name] = {"message": str(error), "transport_calls": len(data[1].calls) - calls_before}
        else:
            checks["replay_refuses_" + name] = False
            observed[name] = {"accepted": True, "transport_calls": len(data[1].calls) - calls_before}


def cases(folder: Path, checks: dict[str, bool], observations: dict[str, Any]) -> None:
    """Inspect exact requests and persisted qualification records, including refusal causes."""
    try:
        subject = importlib.import_module("orchestration.aws_qualify")
    except ModuleNotFoundError:
        checks.update(qualification_module_available=False, rollout_closed=False, complete_m1=False)
        return
    for layer in ("silver", "gold", "features"):
        for step in ("candidates", "reconcile", "backup", "commit", "maintenance"):
            checks[f"binding_{layer}_{step}"] = subject.binding("pipeline_publish", {"layer": layer, "step": step}, MODE).document() == expected(layer, step)
    for name, params, mode, cause in (
        ("namespace_override", {"layer": "gold", "step": "commit", "namespace": "gold_e2e"}, MODE, "only layer and step"),
        ("prefix_override", {"layer": "gold", "step": "backup", "prefix": "lakehouse/gold/"}, MODE, "only layer and step"),
        ("fixture_not_qualification", {"layer": "gold", "step": "commit"}, "publish_fixture", "aws_qualify"),
    ):
        try:
            subject.binding("pipeline_publish", params, mode)
        except ecs_target.PlacementError as error:
            checks[name] = cause in str(error)
        else:
            checks[name] = False
    checks["fixture_stays_offline"] = pipeline_render.job_class("pipeline_publish", {"layer": "gold", "step": "commit"}, "publish_fixture") == "offline"
    checks["writer_class"] = pipeline_render.job_class("pipeline_publish", {"layer": "gold", "step": "commit"}, MODE) == "lakehouse_write"
    launcher_refusal_cases(checks, observations)
    jobs = ecs_target.load_jobs(ROOT)
    checks["rollout_closed"] = jobs["enabled"] is False
    for layer in ("silver", "gold", "features"):
        params = {"layer": layer, "step": "commit"}
        checks["writer_row_" + layer] = ecs_target.row_for(jobs, "pipeline_publish", params, "lakehouse_write", mode=MODE) is not None
        checks["no_production_row_" + layer] = ecs_target.row_for(jobs, "pipeline_publish", params, "lakehouse_write", mode="real") is None
    try:
        ecs_target.row_for(jobs, "pipeline_publish", {"layer": "gold", "step": "commit"}, "lakehouse_read", mode=MODE)
    except ecs_target.PlacementError as error:
        checks["selector_class_mismatch"] = "qualification class differs" in str(error)
    else:
        checks["selector_class_mismatch"] = False
    suffixed = fixture(folder / "suffixed_role")
    suffix_role = suffixed[3]["job_roles"]["write_e2e_task"]
    suffix_role.update(name=suffix_role["name"] + "_a1", arn=suffix_role["arn"] + "_a1")
    suffixed[1].description["task"]["task_role_arn"] = suffix_role["arn"]
    suffixed[3]["definitions"]["spark_write_e2e"]["execution_sha256"] = ecs_dispatch.execution_pin(
        suffixed[1].description["container"], suffixed[1].description["task"]
    )
    submit(suffixed)
    suffix_replay = submit(suffixed)
    checks["valid_role_suffix_replays"] = suffix_replay["dispatch"]["qualification"]["task_role"] == suffix_role and len(suffixed[1].run_requests) == 1
    data = fixture(folder / "canonical")
    result = submit(data)
    request = data[1].run_requests[0]
    placed = data[0].read()["attempts"]["synthetic_attempt"]["execution"]["placements"][0]
    record = placed["dispatch"].get("qualification", {})
    observations["canonical"] = {"request": request, "m1": record}
    checks["complete_m1"] = (
        result["state"] == "pending"
        and record.get("mode") == MODE
        and record.get("job_class") == "lakehouse_write"
        and record.get("task_role") == data[3]["job_roles"]["write_e2e_task"]
        and record.get("capacity_provider") == "FARGATE"
        and record.get("namespace") == "gold_e2e"
        and record.get("output_prefixes") == ["lakehouse/gold_e2e/"]
        and record.get("backup_prefix") == "lakehouse/gold_e2e/catalog_backups/"
        and record.get("catalog_principal") == "e2e_writer"
        and record.get("task_definition_arn") == request["taskDefinition"]
        and record.get("try_id") == request["startedBy"]
        and record.get("run_id") == RUN
    )
    checks["m1_job_inputs_saved"] = placed["dispatch"].get("qualification_job") == {"step": "commit", "task_role_id": data[4]["task_role_id"]}
    checks["evidence_boundary"] = record.get("evidence") == "dispatch_binding" and record.get("native_qualification_passed") is False
    checks["writer_on_demand"] = request["capacityProviderStrategy"] == [{"capacityProvider": "FARGATE", "weight": 1}]
    submit(data)
    checks["repeat_dispatch"] = len(data[1].run_requests) == 1
    lost = fixture(folder / "lost_reply")
    lost[1].lost_reply = True
    first = submit(lost)
    second = submit(lost)
    checks["lost_reply_reconciles"] = first["state"] == "unknown" and second["state"] == "pending" and len(lost[1].run_requests) == 1
    local = copy.deepcopy(data[0].read()["attempts"]["synthetic_attempt"])
    local["execution"].update(live_try=None, placements=[], requested_target=ecs_target.TARGET_DOCKER)
    checks["writer_never_local"] = (
        ecs_target.select(local, "lakehouse_write", ecs_target.Size(1024, 4096, 21), ecs_target.Availability(), ecs_dispatch_cases.NOW).action == "refuse"
    )
    gold = fixture(folder / "gold_backup", step="backup")
    submit(gold)
    backup_record = gold[0].read()["attempts"]["synthetic_attempt"]["execution"]["placements"][0]["dispatch"]["qualification"]
    observations["gold_backup"] = {"request": gold[1].run_requests[0], "m1": backup_record}
    checks["gold_test_backup"] = backup_record["output_prefixes"] == ["lakehouse/gold_e2e/catalog_backups/"]
    read = fixture(folder / "reconcile", step="reconcile")
    submit(read)
    read_record = read[0].read()["attempts"]["synthetic_attempt"]["execution"]["placements"][0]["dispatch"]["qualification"]
    observations["reconcile"] = {"request": read[1].run_requests[0], "m1": read_record}
    checks["reconcile_reader"] = (
        read_record["job_class"] == "lakehouse_read"
        and read_record["task_role"] == read[3]["job_roles"]["read_task"]
        and read_record["catalog_principal"] == "lakehouse_reader"
        and read_record["output_prefixes"] == []
        and read_record["task_definition_arn"] == read[3]["definitions"]["spark_read"]["task_definition_arn"]
    )

    submit(read)
    checks["reconcile_replay"] = len(read[1].run_requests) == 1
    replay_cases(folder / "replay", checks, observations)

    def refused(name: str, alter: Callable[[tuple[Any, ...]], None], cause: str) -> None:
        candidate = fixture(folder / name)
        alter(candidate)
        refresh(candidate[4], candidate[5])
        try:
            submit(candidate)
        except ecs_target.PlacementError as error:
            checks[name] = cause in str(error) and not candidate[1].run_requests
        else:
            checks[name] = False

    refused("production_namespace", lambda d: d[4]["qualification"].update(namespace="gold"), "namespace")
    refused("production_prefix", lambda d: d[4]["qualification"].update(output_prefixes=["lakehouse/gold/"]), "output prefixes")
    refused("production_backup", lambda d: d[4]["qualification"].update(backup_prefix="lakehouse/catalog_backups/"), "backup prefix")
    refused("wrong_class", lambda d: d[4].update(job_class="offline"), "qualification class")
    refused("wrong_definition", lambda d: d[4]["qualification"].update(definition="spark_read"), "qualification definition")
    refused("wrong_role", lambda d: d[3]["definitions"]["spark_write_e2e"].update(task_role_key="read_task"), "qualification role")
    refused("command_namespace_override", lambda d: d[4]["command"].extend(["--namespace", "gold"]), "qualification command")
    refused("command_fixture", lambda d: d[4]["command"].__setitem__(7, "publish_fixture"), "qualification command")

    def provider(candidate: tuple[Any, ...]) -> None:
        with candidate[0].tx() as state:
            state["attempts"]["synthetic_attempt"]["execution"]["placements"][0]["capacity_provider"] = "FARGATE_SPOT"

    def principal(candidate: tuple[Any, ...]) -> None:
        candidate[1].description["container"]["secrets"][0]["valueFrom"] = (
            candidate[1].description["container"]["secrets"][0]["valueFrom"].replace("e2e", "gold")
        )
        candidate[3]["definitions"]["spark_write_e2e"]["execution_sha256"] = ecs_dispatch.execution_pin(
            candidate[1].description["container"], candidate[1].description["task"]
        )

    refused("wrong_provider", provider, "on demand")
    refused("wrong_principal", principal, "qualification principal")
    refused("changed_writer_evidence", lambda d: d[4]["writer_authorization"].update(qualification="another_qualification"), "qualification authorization")
    missing = fixture(folder / "missing_m1")
    submit(missing)
    with missing[0].tx() as state:
        del state["attempts"]["synthetic_attempt"]["execution"]["placements"][0]["dispatch"]["qualification"]
    try:
        submit(missing)
    except ecs_target.PlacementError as error:
        checks["missing_m1_refused"] = "qualification M1" in str(error) and len(missing[1].run_requests) == 1
    else:
        checks["missing_m1_refused"] = False


def main() -> int:
    """Write repeatable evidence while keeping native acceptance explicitly unproven."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).is_file() else None for name in SOURCES}
    checks: dict[str, bool] = {}
    observations: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="aws_qualify_") as scratch:
        error_record = None
        try:
            cases(Path(scratch), checks, observations)
        except (ecs_target.PlacementError, pipeline_render.PipelineError, OSError, KeyError, TypeError, ValueError) as error:
            checks["case_completion"] = False
            error_record = {"type": type(error).__name__, "message": str(error)}
    checks["source_snapshot_unchanged"] = before == {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).is_file() else None for name in SOURCES
    }
    report = {
        "time": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.aws_qualify_cases", *sys.argv[1:]],
        "boundary": "closed selector, real admission and dispatcher, durable synthetic registry, fake ECS and STS",
        "source_sha256": before,
        "classification": "Synthetic",
        "environment": {"python": sys.version.split()[0], "jsonschema": version("jsonschema")},
        "checks": checks,
        "observed_dispatch": observations,
        "exception": error_record,
        "failed": [name for name, ok in checks.items() if not ok],
        "native_qualification_passed": False,
        "limits": ["No AWS, Docker, native writer or catalog grant execution", "Live writer integration and activation remain lead gates"],
        "cleanup": "done",
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"report": str(args.output), "failed": report["failed"], "exception": error_record}) + "\n")
    return int(bool(report["failed"]))


if __name__ == "__main__":
    sys.exit(main())
