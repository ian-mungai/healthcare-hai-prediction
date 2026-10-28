"""Close layer qualification targets and record verified dispatch bindings for M1."""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from orchestration.ecs_target import PlacementError

MODE = "aws_qualify"
NAMESPACES = {"silver": "silver_e2e", "gold": "gold_e2e", "features": "features_e2e"}
STEPS = ("candidates", "reconcile", "backup", "commit", "maintenance")


@dataclass(frozen=True)
class Binding:
    """One fixed layer action, catalog identity and storage scope."""

    mode: str
    layer: str
    step: str
    job_class: str
    namespace: str
    table_prefix: str
    backup_prefix: str
    output_prefixes: tuple[str, ...]
    definition: str
    task_role_key: str
    catalog_principal: str

    def document(self) -> dict[str, Any]:
        """Return the JSON declaration carried by the immutable staged job."""
        result = asdict(self)
        result["output_prefixes"] = list(self.output_prefixes)
        return result


def binding(template: str, params: Mapping[str, Any], mode: str) -> Binding:
    """Resolve a closed layer action without accepting namespace or prefix overrides."""
    if mode != MODE:
        raise PlacementError("qualification requires aws_qualify mode")
    if template != "pipeline_publish" or set(params) != {"layer", "step"}:
        raise PlacementError("qualification accepts only layer and step on pipeline_publish")
    layer, step = params["layer"], params["step"]
    if not isinstance(layer, str) or layer not in NAMESPACES or not isinstance(step, str) or step not in STEPS:
        raise PlacementError("qualification layer or step is unknown")
    namespace = NAMESPACES[layer]
    table_prefix = f"lakehouse/{namespace}/"
    backup_prefix = table_prefix + "catalog_backups/"
    read = step == "reconcile"
    return Binding(
        MODE,
        layer,
        step,
        "lakehouse_read" if read else "lakehouse_write",
        namespace,
        table_prefix,
        backup_prefix,
        () if read else (backup_prefix if step == "backup" else table_prefix,),
        "spark_read" if read else "spark_write_e2e",
        "read_task" if read else "write_e2e_task",
        "lakehouse_reader" if read else "e2e_writer",
    )


def _command(job: Mapping[str, Any], selected: Binding, run_id: str) -> None:
    command = job["command"]
    module = f"scripts.lakehouse.{selected.layer}_maintenance" if selected.step == "maintenance" else f"scripts.lakehouse.{selected.layer}_publish"
    prefix = ["python", "-m", module] + ([] if selected.step == "maintenance" else [selected.step])
    options = command[len(prefix) :]
    if command[: len(prefix)] != prefix or len(options) % 2:
        raise PlacementError("qualification command differs from the closed action")
    pairs = list(zip(options[::2], options[1::2], strict=True))
    flags = dict(pairs)
    required = {"--run", "--mode", "--protected"} if selected.step == "maintenance" else {"--run", "--mode", "--sealed-sha256", "--accepted-result-sha256"}
    valid_inputs = (
        flags.get("--protected") == "/workspace/out/publish/protected.json"
        if selected.step == "maintenance"
        else all(re.fullmatch(r"[a-f0-9]{64}", flags.get(key, "")) for key in ("--sealed-sha256", "--accepted-result-sha256"))
    )
    if len(flags) != len(pairs) or set(flags) != required or flags.get("--run") != run_id or flags.get("--mode") != MODE or not valid_inputs:
        raise PlacementError("qualification command differs from the closed action")


def validate_dispatch(
    target: Mapping[str, Any],
    definition_name: str,
    job: Mapping[str, Any],
    attempt: Mapping[str, Any],
    placement: Mapping[str, Any],
    described: Mapping[str, Any],
) -> Binding | None:
    """Refuse a mixed qualification scope or identity before the first RunTask effect."""
    admission = attempt["execution"]["admission"]
    if admission["mode"] != MODE:
        if "qualification" in job:
            raise PlacementError("qualification requires aws_qualify mode")
        return None
    selected = binding(attempt["template"], attempt["params"], admission["mode"])
    declared = job.get("qualification")
    if not isinstance(declared, Mapping):
        raise PlacementError("qualification binding is missing")
    if declared.get("namespace") != selected.namespace or declared.get("table_prefix") != selected.table_prefix:
        raise PlacementError("qualification namespace differs from its test target")
    if declared.get("backup_prefix") != selected.backup_prefix:
        raise PlacementError("qualification backup prefix differs from its test target")
    if declared.get("output_prefixes") != list(selected.output_prefixes):
        raise PlacementError("qualification output prefixes differ from the closed action")
    if definition_name != selected.definition or declared.get("definition") != selected.definition:
        raise PlacementError("qualification definition differs from the closed action")
    if job.get("job_class") != selected.job_class or admission["job_class"] != selected.job_class or placement.get("job_class") != selected.job_class:
        raise PlacementError("qualification class differs from the closed action")
    if admission["layer"] != selected.layer or (selected.job_class == "lakehouse_write" and admission["namespaces"] != [selected.namespace]):
        raise PlacementError("qualification admission differs from the closed action")
    if job.get("writer_authorization") != attempt["execution"]["writer_authorization"]:
        raise PlacementError("qualification authorization differs from admission")
    if declared.get("catalog_principal") != selected.catalog_principal:
        raise PlacementError("qualification principal differs from the closed action")
    if declared != selected.document():
        raise PlacementError("qualification binding differs from the closed action")
    definition = target["definitions"][definition_name]
    if definition["task_role_key"] != selected.task_role_key:
        raise PlacementError("qualification role differs from the closed action")
    role = target["job_roles"][selected.task_role_key]
    suffix = "job_read_task" if selected.job_class == "lakehouse_read" else "job_write_e2e_task"
    if re.fullmatch(re.escape(target["project_tag"] + "_" + suffix) + r"(?:_[a-z0-9]+)*", role["name"]) is None or job["task_role_id"] != role["unique_id"]:
        raise PlacementError("qualification role differs from the closed action")
    pair = "reader" if selected.job_class == "lakehouse_read" else "e2e"
    prefix = f"arn:aws:ssm:{target['region']}:{target['account_id']}:parameter/{target['project_tag']}/aws_polaris_{pair}_client_"
    expected_secrets = [{"name": "POLARIS_CLIENT_ID", "valueFrom": prefix + "id"}, {"name": "POLARIS_CLIENT_SECRET", "valueFrom": prefix + "secret"}]
    secrets = described["container"].get("secrets", [])
    if sorted(secrets, key=lambda item: item["name"]) != expected_secrets or sorted(job["secret_names"]) != ["POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET"]:
        raise PlacementError("qualification principal differs from the closed action")
    if placement["target"] != "ecs" or selected.job_class == "lakehouse_write" and placement["capacity_provider"] != "FARGATE":
        raise PlacementError("qualification writers require ECS on demand")
    _command(job, selected, admission["run_id"])
    return selected


def dispatch_record(
    selected: Binding,
    target: Mapping[str, Any],
    attempt: Mapping[str, Any],
    placement: Mapping[str, Any],
    request: Mapping[str, Any],
) -> dict[str, Any]:
    """Record actual validated dispatch values without claiming that an AWS writer ran."""
    return {
        **selected.document(),
        "schema_version": 1,
        "evidence": "dispatch_binding",
        "native_qualification_passed": False,
        "run_id": attempt["execution"]["admission"]["run_id"],
        "try_id": request["startedBy"],
        "target": placement["target"],
        "capacity_provider": request["capacityProviderStrategy"][0]["capacityProvider"],
        "task_role": copy.deepcopy(target["job_roles"][selected.task_role_key]),
        "task_definition_arn": request["taskDefinition"],
    }


@cache
def _record_validator(kind: str = "dispatch_record") -> Draft202012Validator:
    schema = json.loads((Path(__file__).resolve().parents[1] / "data_contracts/orchestration/aws_qualification.schema.json").read_bytes())
    return Draft202012Validator({"$schema": schema["$schema"], "$defs": schema["$defs"], "$ref": f"#/$defs/{kind}"})


def validate_record(execution: Mapping[str, Any], placement: Mapping[str, Any]) -> None:
    """Check M1 completeness and agreement with the persisted admission and request."""
    dispatch = placement["dispatch"]
    record = dispatch.get("qualification")
    job = dispatch.get("qualification_job")
    if next(_record_validator().iter_errors(record), None) is not None or next(_record_validator("dispatch_job").iter_errors(job), None) is not None:
        raise PlacementError("qualification M1 dispatch record is incomplete")
    admission = execution["admission"]
    selected = binding("pipeline_publish", {"layer": admission["layer"], "step": job["step"]}, admission["mode"])
    role = record["task_role"]
    suffix = "job_read_task" if selected.job_class == "lakehouse_read" else "job_write_e2e_task"
    if any(record[key] != value for key, value in selected.document().items()) or (
        record["run_id"] != admission["run_id"]
        or record["try_id"] != placement["try_id"]
        or record["try_id"] != dispatch["request"]["startedBy"]
        or record["target"] != placement["target"]
        or record["job_class"] != admission["job_class"]
        or record["task_definition_arn"] != dispatch["request"]["taskDefinition"]
        or record["task_definition_arn"] != dispatch["definition"].get("task_definition_arn")
        or record["job_class"] != dispatch["definition"].get("job_class")
        or record["capacity_provider"] != placement["capacity_provider"]
        or record["capacity_provider"] != dispatch["request"]["capacityProviderStrategy"][0]["capacityProvider"]
        or record["task_role_key"] != dispatch["definition"].get("task_role_key")
        or re.fullmatch(re.escape(dispatch["project_tag"] + "_" + suffix) + r"(?:_[a-z0-9]+)*", role["name"]) is None
        or not role["arn"].endswith("/" + role["name"])
        or role["arn"].split(":")[1:5] != [record["task_definition_arn"].split(":")[1], "iam", "", record["task_definition_arn"].split(":")[4]]
        or role["unique_id"] != job["task_role_id"]
    ):
        raise PlacementError("qualification M1 dispatch record differs from its persisted request")
