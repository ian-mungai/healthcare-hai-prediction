"""Write a private ECS application target from exported outputs and digest-bound image measurements."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "data_contracts/orchestration/ecs_target.schema.json"
GIB = 1024**3
DEFINITIONS = {
    "analytics_offline": ("offline", "analytics", "offline_task"),
    "quality_offline": ("offline", "quality", "offline_task"),
    "seal": ("seal", "analytics", "seal_task"),
    "analytics_read": ("lakehouse_read", "analytics", "read_task"),
    "spark_read": ("lakehouse_read", "spark", "read_task"),
}
WRITER_DEFINITIONS = {
    "spark_write_e2e": ("lakehouse_write", "spark", "write_e2e_task"),
    "spark_write_silver": ("lakehouse_write", "spark", "write_silver_task"),
    "spark_write_gold": ("lakehouse_write", "spark", "write_gold_task"),
    "spark_write_features": ("lakehouse_write", "spark", "write_features_task"),
    "spark_write_bronze": ("lakehouse_write", "spark", "write_bronze_task"),
}
ENTRY_POINT = ["/opt/hai/deadline_run", "python", "-I", "/opt/hai/bootstrap/run.py"]


class TargetError(ValueError):
    """Reject an incomplete deployment capture without disclosing its contents."""


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise TargetError("Capture contains a duplicate JSON field.")
        value[key] = item
    return value


def _constant(value: str) -> None:
    raise TargetError("Capture contains a nonfinite JSON number.")


def _decode(content: str) -> Any:
    try:
        return json.loads(content, object_pairs_hook=_pairs, parse_constant=_constant)
    except (ValueError, RecursionError) as error:
        if isinstance(error, TargetError):
            raise
        raise TargetError("Capture contains invalid JSON.") from None


def _allowed_path(path: Path) -> None:
    for candidate in (path, path.resolve()):
        parts = candidate.parts
        if (
            any(part == ".env" or part.startswith(".env.") or part.endswith((".tfvars", ".tfvars.json")) or "terraform.tfstate" in part for part in parts)
            or "secrets" in parts
            or parts[-3:] == ("config", "acquisition", "source_registry.json")
        ):
            raise TargetError("Use exported output captures, not protected deployment or credential files.")


def read_capture(path: Path) -> dict[str, Any]:
    """Read one explicit JSON capture while refusing protected deployment paths."""
    return read_capture_with_digest(path)[0]


def read_capture_with_digest(path: Path) -> tuple[dict[str, Any], str]:
    """Return one capture and its byte digest from the same bounded file read."""
    _allowed_path(path)
    try:
        with path.open("rb") as handle:
            content = handle.read(32 * 1024**2 + 1)
    except OSError:
        raise TargetError("Could not read the selected capture.") from None
    if len(content) > 32 * 1024**2:
        raise TargetError("A capture exceeds the 32 MiB record bound.")
    value = _decode(content.decode("utf-8"))
    if not isinstance(value, dict):
        raise TargetError("A capture must contain a JSON object.")
    return value, hashlib.sha256(content).hexdigest()


def _validate(value: Any, fragment: str | None, cause: str) -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    if fragment:
        schema = {"$schema": schema["$schema"], "$defs": schema["$defs"], "$ref": f"#/$defs/{fragment}"}
    if next(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value), None) is not None:
        raise TargetError(f"Invalid {cause}.")


def _output(outputs: dict[str, Any], name: str) -> Any:
    item = outputs.get(name)
    if not isinstance(item, dict) or set(item) != {"sensitive", "type", "value"} or item["value"] is None:
        raise TargetError(f"Missing or unknown Terraform output: {name}.")
    if item["sensitive"] is not False:
        raise TargetError("Refused a sensitive output capture.")
    return item["value"]


def _roles(roles: dict[str, Any], account: str, project: str) -> None:
    _validate(roles, "job_roles", "role contract")
    for role in roles.values():
        if role["arn"] != f"arn:aws:iam::{account}:role/{role['name']}":
            raise TargetError("Invalid role identity.")
    if len({role["unique_id"] for role in roles.values()}) != len(roles) or len({role["arn"] for role in roles.values()}) != len(roles):
        raise TargetError("Repeated role identity.")

    for _, _, role_key in WRITER_DEFINITIONS.values():
        if role_key not in roles:
            continue
        execution = re.fullmatch(re.escape(project) + r"_job_execution_(dev|prod)", roles["execution"]["name"])
        if execution is None or roles[role_key]["name"] != f"{project}_job_{role_key}_{execution[1]}":
            raise TargetError("Invalid writer role identity.")


def _network(network: dict[str, Any]) -> None:
    _validate(network, "job_network", "network contract")
    if len(set(network["security_group_ids"].values())) != 2:
        raise TargetError("The class networks must use different security groups.")


def _reader_parameters(region: str, account: str, project: str) -> dict[str, str]:
    prefix = f"arn:aws:ssm:{region}:{account}:parameter/{project}/aws_polaris_reader_client_"
    return {"POLARIS_CLIENT_ID": prefix + "id", "POLARIS_CLIENT_SECRET": prefix + "secret"}


def _writer_parameters(jobs: dict[str, Any], selected: set[str], region: str, account: str) -> dict[str, dict[str, str]]:
    if not selected:
        return {}
    contracts = _output(jobs, "writer_dispatch_contracts")
    if not isinstance(contracts, dict) or set(contracts) != selected:
        raise TargetError("Invalid writer dispatch contract inventory.")
    bindings = {}
    for name in sorted(selected):
        contract = contracts[name]
        role_key = WRITER_DEFINITIONS[name][2]
        layer = name.removeprefix("spark_write_")
        parameters = contract.get("parameter_names") if isinstance(contract, dict) else None
        if not isinstance(parameters, list) or len(parameters) != 2 or any(not isinstance(parameter, str) for parameter in parameters):
            raise TargetError("Invalid writer parameter names.")
        for parameter, suffix in zip(parameters, ("id", "secret"), strict=True):
            if (
                len(parameter) > 512
                or re.fullmatch(rf"/(?:[A-Za-z0-9_]+/){{0,14}}aws_polaris_{layer}_client_{suffix}", parameter) is None
                or re.match(r"/(?i:aws|ssm)", parameter)
            ):
                raise TargetError("Invalid writer parameter names.")
        if contract != {
            "job_class": "lakehouse_write",
            "task_role_key": role_key,
            "parameter_names": parameters,
            "capacity_providers": ["FARGATE"],
            "network": "lakehouse",
            "assign_public_ip": "DISABLED",
        }:
            raise TargetError("Invalid writer dispatch contract.")
        bindings[name] = {
            environment: f"arn:aws:ssm:{region}:{account}:parameter{parameter}"
            for environment, parameter in zip(("POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET"), parameters, strict=True)
        }
    return bindings


def _dispatch(jobs: dict[str, Any], selected: set[str]) -> dict[str, Any]:
    contract = _output(jobs, "dispatch_contract")
    if not isinstance(contract, dict):
        raise TargetError("Invalid dispatch contract.")
    writers = bool(selected & WRITER_DEFINITIONS.keys())
    networks = {"offline": "offline", "seal": "offline", "lakehouse_read": "lakehouse"} | ({"lakehouse_write": "lakehouse"} if writers else {})
    capacity = {name: ["FARGATE_SPOT", "FARGATE"] for name in ("offline", "seal", "lakehouse_read")} | ({"lakehouse_write": ["FARGATE"]} if writers else {})
    if contract.get("class_networks") != networks:
        raise TargetError("Invalid dispatch class networks.")
    if contract.get("class_capacity_providers") != capacity or contract.get("capacity_providers") != ["FARGATE_SPOT", "FARGATE"]:
        raise TargetError("Invalid dispatch class capacity providers.")
    if contract.get("definition_classes") != {name: spec[0] for name, spec in (DEFINITIONS | WRITER_DEFINITIONS).items() if name in selected}:
        raise TargetError("Invalid dispatch definition classes.")
    if contract.get("assign_public_ip") != "DISABLED":
        raise TargetError("Invalid dispatch public IP setting.")
    return contract


def _catalog(catalog: dict[str, Any] | None, region: str, account: str, project: str) -> None:
    if catalog is None:
        return
    _validate(catalog, "catalog", "catalog contract")
    address = ipaddress.IPv4Address(catalog["private_ip"])
    if not any(address in ipaddress.IPv4Network(network) for network in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")):
        raise TargetError("A private catalog address is required.")
    if catalog["endpoint"] != f"http://{address}:8181/api/catalog":
        raise TargetError("The private catalog endpoint differs from the verified host address.")
    if catalog["reader_parameters"] != _reader_parameters(region, account, project):
        raise TargetError("The reader parameters differ from the approved reader pair.")


def validate_target(target: dict[str, Any]) -> None:
    """Validate the closed application target and its cross-field identities before a consumer uses it."""
    _validate(target, None, "target contract")
    account, region = target["account_id"], target["region"]
    if not target["cluster_arn"].startswith(f"arn:aws:ecs:{region}:{account}:cluster/"):
        raise TargetError("The cluster identity differs from the target account or region.")
    _roles(target["job_roles"], account, target["project_tag"])
    _network(target["job_network"])
    _catalog(target["catalog"], region, account, target["project_tag"])
    for definition in target["definitions"].values():
        if not definition["task_definition_arn"].startswith(f"arn:aws:ecs:{region}:{account}:task-definition/"):
            raise TargetError("The fixed task revision differs from the target account or region.")
        if not definition["image"].startswith(f"{account}.dkr.ecr.{region}.amazonaws.com/"):
            raise TargetError("The image digest differs from the target account or region.")


def _task_settings(task: dict[str, Any], arn: str, roles: dict[str, Any], role_key: str, project: str, region: str, account: str) -> None:
    match = re.fullmatch(rf"arn:aws:ecs:{re.escape(region)}:{account}:task-definition/([A-Za-z0-9_-]{{1,255}}):[1-9][0-9]*", arn)
    if not match or task.get("family") != match[1]:
        raise TargetError("A matching fixed task revision is required.")
    if (
        task.get("requires_compatibilities") != ["FARGATE"]
        or task.get("network_mode") != "awsvpc"
        or task.get("runtime_platform") != [{"cpu_architecture": "ARM64", "operating_system_family": "LINUX"}]
        or task.get("pid_mode") not in (None, "")
        or task.get("ipc_mode") not in (None, "")
        or task.get("proxy_configuration") not in (None, [])
    ):
        raise TargetError("Invalid private ARM64 Fargate task settings.")
    if task.get("task_role_arn") != roles[role_key]["arn"] or task.get("execution_role_arn") != roles["execution"]["arn"]:
        raise TargetError("Invalid task role binding.")
    if not isinstance(task.get("tags"), dict) or task["tags"].get("Project") != project:
        raise TargetError("Invalid task project tag.")
    volumes = task.get("volume")
    if not isinstance(volumes, list) or len(volumes) != 2 or any(not isinstance(volume, dict) for volume in volumes):
        raise TargetError("Invalid task volumes.")
    if {volume.get("name") for volume in volumes} != {"workspace", "tmp"} or any(
        value not in (None, "", [], False) for volume in volumes for key, value in volume.items() if key != "name"
    ):
        raise TargetError("Invalid task volumes.")


def _container(
    container: dict[str, Any], job_class: str, image: str, region: str, project: str, account: str, log_group: str, parameters: dict[str, str] | None = None
) -> None:
    if container.get("image") != image:
        raise TargetError("The image digest differs from its measured image.")
    if (
        container.get("entryPoint") != ENTRY_POINT
        or container.get("command", []) != []
        or container.get("essential") is not True
        or container.get("privileged", False) is not False
        or container.get("readonlyRootFilesystem") is not True
        or container.get("environmentFiles", []) != []
        or container.get("stopTimeout") != 90
        or not isinstance(container.get("user"), str)
        or re.fullmatch(r"[1-9][0-9]{2,5}:[1-9][0-9]{2,5}", container["user"]) is None
        or container.get("linuxParameters", {}).get("capabilities") != {"add": [], "drop": ["ALL"]}
        or set(container.get("linuxParameters", {})) - {"capabilities", "initProcessEnabled"}
        or any(container.get(key) for key in ("devices", "systemControls", "resourceRequirements", "links", "portMappings"))
    ):
        raise TargetError("Invalid container settings.")
    mounts: list[dict[str, Any]] = [
        {"sourceVolume": "workspace", "containerPath": "/workspace", "readOnly": False},
        {"sourceVolume": "tmp", "containerPath": str(PurePosixPath("/") / "tmp"), "readOnly": False},
    ]
    if sorted(container.get("mountPoints", []), key=lambda row: row.get("sourceVolume", "")) != sorted(mounts, key=lambda row: row["sourceVolume"]):
        raise TargetError("Invalid writable scratch mounts.")
    environment = container.get("environment", [])
    if not isinstance(environment, list) or any(
        not isinstance(row, dict)
        or set(row) != {"name", "value"}
        or not isinstance(row["name"], str)
        or not isinstance(row["value"], str)
        or re.search(r"SECRET|PASSWORD|TOKEN|CREDENTIAL|ACCESS_KEY|CLIENT_ID|(?:^|_)KEY(?:$|_)", row["name"], re.IGNORECASE)
        for row in environment
    ):
        raise TargetError("Invalid plain environment.")
    secrets = container.get("secrets", [])
    if parameters is None:
        parameters = _reader_parameters(region, account, project) if job_class == "lakehouse_read" else {}
    expected = [{"name": name, "valueFrom": value} for name, value in parameters.items()]
    if not isinstance(secrets, list) or any(not isinstance(row, dict) for row in secrets) or sorted(secrets, key=lambda row: str(row.get("name"))) != expected:
        raise TargetError("Invalid writer bindings." if job_class == "lakehouse_write" else "Invalid reader bindings.")
    logs = container.get("logConfiguration", {})
    if (
        logs.get("logDriver") != "awslogs"
        or logs.get("options", {}).get("awslogs-group") != log_group
        or logs.get("options", {}).get("awslogs-region") != region
        or not logs.get("options", {}).get("awslogs-stream-prefix")
        or logs.get("secretOptions")
    ):
        raise TargetError("Invalid task log binding.")


def build_target(
    storage: dict[str, Any],
    platform: dict[str, Any],
    iam: dict[str, Any],
    jobs: dict[str, Any],
    measurements: dict[str, Any],
    catalog: dict[str, Any] | None,
    *,
    enabled: bool = False,
) -> dict[str, Any]:
    """Build a disabled-by-default application target from explicit Terraform captures and measured image bytes."""
    cluster = _output(platform, "cluster_arn")
    match = re.fullmatch(r"arn:aws:ecs:([a-z]{2}(?:-[a-z]+)+-[0-9]+):([0-9]{12}):cluster/[A-Za-z0-9_-]+", cluster) if isinstance(cluster, str) else None
    if not match:
        raise TargetError("Invalid cluster output.")
    region, account = match[1], match[2]
    project = _output(platform, "project_tag")
    roles, network = _output(iam, "job_roles"), _output(iam, "job_network")
    _roles(roles, account, project)
    _network(network)
    if network["subnet_id"] not in _output(platform, "public_subnet_ids"):
        raise TargetError("The endpoint subnet differs from the platform output.")
    _validate(measurements, "image_measurements", "image measurements")
    _catalog(catalog, region, account, project)
    log_group = _output(platform, "log_group_names").get("jobs")
    repositories = _output(platform, "ecr_repository_urls")
    maps = {name: _output(jobs, name) for name in ("task_definition_arns", "container_definitions", "task_definition_settings", "execution_digests")}
    if any(not isinstance(value, dict) for value in maps.values()):
        raise TargetError("The jobs exports must contain exactly the five read definitions and only the closed optional writers.")
    selected = set(maps["task_definition_arns"])
    if (
        not set(DEFINITIONS) <= selected
        or not selected <= set(DEFINITIONS) | set(WRITER_DEFINITIONS)
        or any(set(maps[name]) != selected for name in maps if name != "execution_digests")
    ):
        raise TargetError("The jobs exports must contain exactly the five read definitions and only the closed optional writers.")
    writer_parameters = _writer_parameters(jobs, selected & set(WRITER_DEFINITIONS), region, account)
    dispatch = _dispatch(jobs, selected)
    definitions = {}
    pin_keys = set()
    for name, (job_class, image_name, role_key) in (DEFINITIONS | WRITER_DEFINITIONS).items():
        if name not in selected:
            continue
        if role_key not in roles:
            raise TargetError("Missing writer task role binding.")
        arn = maps["task_definition_arns"][name]
        task = maps["task_definition_settings"][name]
        if not isinstance(task, dict) or not isinstance(arn, str):
            raise TargetError("Invalid Fargate task settings.")
        _task_settings(task, arn, roles, role_key, project, region, account)
        containers = _decode(maps["container_definitions"][name])
        if not isinstance(containers, list) or len(containers) != 1 or not isinstance(containers[0], dict):
            raise TargetError("Each definition must contain one job container.")
        container = containers[0]
        measurement = measurements["images"][image_name]
        image = measurement["image"]
        if image.rsplit("@", 1)[0] != repositories.get(image_name):
            raise TargetError("The image digest differs from its platform repository.")
        _container(container, job_class, image, region, project, account, log_group, writer_parameters.get(name))
        pin_key = name + "/" + str(container.get("name"))
        pin_keys.add(pin_key)
        definitions[name] = {
            "task_definition_arn": arn,
            "container_name": container.get("name"),
            "job_class": job_class,
            "image": image,
            "task_role_key": role_key,
            "execution_sha256": maps["execution_digests"].get(pin_key),
            "image_compressed_gib": (measurement["compressed_bytes"] + GIB - 1) // GIB,
            "image_uncompressed_gib": (measurement["uncompressed_bytes"] + GIB - 1) // GIB,
        }
    if set(maps["execution_digests"]) != pin_keys:
        raise TargetError("The execution pins differ from the exported job containers.")
    target = {
        "schema_version": 1,
        "enabled": enabled,
        "account_id": account,
        "region": region,
        "project_tag": project,
        "bucket_name": _output(storage, "bucket_name"),
        "cluster_arn": cluster,
        "log_group_name": log_group,
        "job_roles": roles,
        "job_network": network,
        "class_networks": dispatch["class_networks"],
        "class_capacity_providers": dispatch["class_capacity_providers"],
        "assign_public_ip": dispatch["assign_public_ip"],
        "definitions": definitions,
        "catalog": catalog,
        "mac_preparation": {"enabled": False, "transport": "session_manager_owned_relay"},
        "cloud_limits": {"max_tasks": 3, "max_vcpu": 16, "max_memory_gib": 120},
        "deadlines": {
            "hard_stop_required": True,
            "pending_seconds": 600,
            "stop_grace_seconds": 90,
            "cleanup_seconds": 120,
            "budget_alert_is_spend_stop": False,
        },
        "max_record_bytes": 32 * 1024**2,
    }
    validate_target(target)
    return target


def write_target(path: Path, target: dict[str, Any], *, check: bool = False) -> None:
    """Validate and atomically write mode-0600 target bytes, or compare without writing."""
    validate_target(target)
    write_private_record(path, target, check=check)


def write_private_record(path: Path, value: dict[str, Any], *, check: bool = False) -> None:
    """Atomically write one deterministic private configuration record within the result-file bound."""
    _allowed_path(path)
    content = json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if len(content.encode("utf-8")) > 32 * 1024**2:
        raise TargetError("The generated record exceeds the 32 MiB record bound.")
    if path.is_symlink():
        raise TargetError("The target path must not be a symbolic link.")
    if check:
        if not path.is_file() or path.read_text(encoding="utf-8") != content or stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise TargetError("Missing or stale target. Render the validated captures again.")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".ecs_target_", delete=False, encoding="utf-8") as handle:
        temporary = Path(handle.name)
        try:
            os.chmod(temporary, 0o600)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def main() -> int:
    """Render an explicit capture set without running a cloud command or reading local deployment files."""
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("storage_outputs", "platform_outputs", "iam_outputs", "jobs_outputs", "image_measurements"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    parser.add_argument("--catalog-binding", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / "data/orchestration/ecs_target.json")
    parser.add_argument("--enable", action="store_true", help="Set the rollout switch explicitly after the lead accepts the deployment.")
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    try:
        target = build_target(
            read_capture(arguments.storage_outputs),
            read_capture(arguments.platform_outputs),
            read_capture(arguments.iam_outputs),
            read_capture(arguments.jobs_outputs),
            read_capture(arguments.image_measurements),
            read_capture(arguments.catalog_binding) if arguments.catalog_binding else None,
            enabled=arguments.enable,
        )
        write_target(arguments.output, target, check=arguments.check)
    except TargetError as error:
        parser.error(str(error))
    except OSError:
        parser.error("Could not read the target file." if arguments.check else "Could not write the target file.")
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        parser.error("The captures do not match the target contract. No target was written.")
    sys.stdout.write("Verified private ECS target.\n" if arguments.check else "Rendered private ECS target.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
