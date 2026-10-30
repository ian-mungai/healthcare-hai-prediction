"""Exercise target rendering and jobs-wrapper commands with synthetic captures and inert executables."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import shutil
import stat
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "scripts/infrastructure/render_ecs_target.py",
    "scripts/infrastructure/render_jobs_config.py",
    "scripts/infrastructure/terraform.sh",
    "scripts/infrastructure/run_ecs_render_e2e.py",
    "data_contracts/orchestration/ecs_target.schema.json",
    "data_contracts/orchestration/examples/ecs_target.json",
    "data_contracts/orchestration/examples/ecs_target_inputs.json",
)
INPUTS = ("storage_outputs", "platform_outputs", "iam_outputs", "jobs_outputs", "image_measurements", "catalog_binding")


def command_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Run the actual file-producing command and reject each invalid capture for its named cause."""
    fixture = json.loads((root / SOURCES[-1]).read_text())
    expected = json.loads((root / SOURCES[-2]).read_text())
    output = folder / "target.json"

    def invoke(value: dict[str, Any], *extra: str, destination: Path | None = None) -> Any:
        arguments = ["-m", "scripts.infrastructure.render_ecs_target"]
        for name in INPUTS:
            path = folder / f"{name}.json"
            path.write_text(json.dumps(value[name]))
            arguments += ["--" + name.replace("_", "-"), str(path)]
        arguments += ["--output", str(destination or output), *extra]
        return run_command(sys.executable, arguments, cwd=root, timeout=10)

    result = invoke(fixture)
    checks = {"render_command": result.returncode == 0 and output.exists() and json.loads(output.read_text()) == expected}
    if not checks["render_command"]:
        return checks
    original = output.read_bytes()
    for name, group in (
        ("platform_log_group", "example_project_jobs_dev"),
        ("slash_log_group", "/example_project/jobs/dev"),
        ("hash_log_group", "example_project#jobs"),
        ("longest_log_group", "x" * 512),
        ("shortest_log_group", "x"),
    ):
        altered = copy.deepcopy(fixture)
        altered["platform_outputs"]["log_group_names"]["value"]["jobs"] = group
        for key, content in altered["jobs_outputs"]["container_definitions"]["value"].items():
            containers = json.loads(content)
            containers[0]["logConfiguration"]["options"]["awslogs-group"] = group
            altered["jobs_outputs"]["container_definitions"]["value"][key] = json.dumps(containers)
        result = invoke(altered)
        checks[name] = result.returncode == 0 and json.loads(output.read_text()) == expected | {"log_group_name": group}
    invoke(fixture)
    checks["private_output"] = stat.S_IMODE(output.stat().st_mode) == 0o600
    checks["repeat_render"] = invoke(fixture).returncode == 0 and output.read_bytes() == original
    checks["check_without_write"] = invoke(fixture, "--check").returncode == 0 and output.read_bytes() == original
    output.chmod(0)
    try:
        result = invoke(fixture, "--check")
        checks["unreadable_check"] = (
            result.returncode == 2
            and "Could not read the target file." in result.stderr
            and "Could not write the target file." not in result.stderr
            and stat.S_IMODE(output.stat().st_mode) == 0
        )
    finally:
        output.chmod(0o600)
    checks["unreadable_check"] = checks["unreadable_check"] and output.read_bytes() == original
    output.write_text("stale target")
    result = invoke(fixture, "--check")
    checks["stale_check"] = result.returncode == 2 and "stale target" in result.stderr and output.read_text() == "stale target"
    invoke(fixture)

    def refused(name: str, value: dict[str, Any], cause: str) -> None:
        before = output.read_bytes()
        result = invoke(value)
        checks[name] = result.returncode == 2 and cause in result.stderr and output.read_bytes() == before

    def change(section: str, key: str, value: Any) -> dict[str, Any]:
        altered = copy.deepcopy(fixture)
        altered[section][key] = value
        return altered

    for name, group in (("empty_log_group", ""), ("overlong_log_group", "x" * 513), ("invalid_log_character", "example:jobs"), ("log_group_newline", "jobs\n")):
        altered = copy.deepcopy(fixture)
        altered["platform_outputs"]["log_group_names"]["value"]["jobs"] = group
        for key, content in altered["jobs_outputs"]["container_definitions"]["value"].items():
            containers = json.loads(content)
            containers[0]["logConfiguration"]["options"]["awslogs-group"] = group
            altered["jobs_outputs"]["container_definitions"]["value"][key] = json.dumps(containers)
        refused(name, altered, "target contract")
    for name, field, replacement, cause in (
        ("wrong_architecture", "runtime_platform", [{"cpu_architecture": "X86_64", "operating_system_family": "LINUX"}], "Fargate task settings"),
        ("wrong_network_mode", "network_mode", "bridge", "Fargate task settings"),
        ("wrong_project_tag", "tags", {"Project": "example_other"}, "task project tag"),
        ("wrong_execution_role", "execution_role_arn", fixture["iam_outputs"]["job_roles"]["value"]["read_task"]["arn"], "task role binding"),
        ("mismatched_family", "family", "example_other", "fixed task revision"),
    ):
        altered = copy.deepcopy(fixture)
        altered["jobs_outputs"]["task_definition_settings"]["value"]["seal"][field] = replacement
        refused(name, altered, cause)
    altered = copy.deepcopy(fixture)
    for field in ("task_definition_arns", "container_definitions", "task_definition_settings"):
        altered["jobs_outputs"][field]["value"]["unexpected_definition"] = altered["jobs_outputs"][field]["value"]["seal"]
    refused("extra_definition", altered, "exactly the five read definitions")
    altered = copy.deepcopy(fixture)
    altered["jobs_outputs"]["container_definitions"]["value"]["unexpected_definition"] = altered["jobs_outputs"]["container_definitions"]["value"]["seal"]
    refused("definition_maps_differ", altered, "exactly the five read definitions")
    altered = copy.deepcopy(fixture)
    altered["jobs_outputs"]["execution_digests"]["value"]["unexpected_definition/job"] = "f" * 64
    refused("extra_execution_pin", altered, "execution pins")
    altered = copy.deepcopy(fixture)
    altered["platform_outputs"]["ecr_repository_urls"]["value"]["analytics"] += "_other"
    refused("wrong_platform_repository", altered, "platform repository")
    refused("missing_roles", change("iam_outputs", "job_roles", None), "job_roles")
    altered = copy.deepcopy(fixture)
    altered["iam_outputs"]["job_roles"]["sensitive"] = True
    refused("sensitive_output", altered, "sensitive output")
    altered = copy.deepcopy(fixture)
    altered["iam_outputs"]["job_roles"]["value"]["read_task"]["unique_id"] = "missing"
    refused("missing_role_id", altered, "role contract")
    altered = copy.deepcopy(fixture)
    altered["iam_outputs"]["job_roles"]["value"]["read_task"]["arn"] = "arn:aws:iam::111111111111:role/other"
    for name in ("analytics_read", "spark_read"):
        altered["jobs_outputs"]["task_definition_settings"]["value"][name]["task_role_arn"] = "arn:aws:iam::111111111111:role/other"
    refused("foreign_role", altered, "role identity")
    altered = copy.deepcopy(fixture)
    altered["iam_outputs"]["job_network"]["value"]["security_group_ids"]["lakehouse"] = altered["iam_outputs"]["job_network"]["value"]["security_group_ids"][
        "offline"
    ]
    refused("mixed_class_network", altered, "class networks")
    altered = copy.deepcopy(fixture)
    altered["iam_outputs"]["job_network"]["value"]["endpoints_enabled"] = False
    refused("endpoints_disabled", altered, "network contract")
    altered = copy.deepcopy(fixture)
    altered["platform_outputs"]["public_subnet_ids"]["value"] = ["subnet-3123456789abcdef0"]
    refused("wrong_subnet", altered, "endpoint subnet")
    altered = copy.deepcopy(fixture)
    altered["jobs_outputs"]["task_definition_arns"]["value"]["seal"] = "example_seal"
    refused("unpinned_revision", altered, "fixed task revision")
    altered = copy.deepcopy(fixture)
    altered["image_measurements"]["images"]["analytics"].pop("compressed_bytes")
    refused("missing_compressed_size", altered, "image measurements")
    altered = copy.deepcopy(fixture)
    altered["image_measurements"]["images"]["analytics"]["image"] = altered["image_measurements"]["images"]["analytics"]["image"].replace("a" * 64, "f" * 64)
    refused("wrong_image_measurement", altered, "image digest")
    altered = copy.deepcopy(fixture)
    altered["jobs_outputs"]["task_definition_settings"]["value"]["seal"]["requires_compatibilities"] = ["EC2", "FARGATE"]
    refused("ec2_compatibility", altered, "Fargate task settings")
    altered = copy.deepcopy(fixture)
    altered["jobs_outputs"]["task_definition_settings"]["value"]["seal"]["task_role_arn"] = altered["iam_outputs"]["job_roles"]["value"]["read_task"]["arn"]
    refused("wrong_task_role", altered, "task role binding")
    altered = copy.deepcopy(fixture)
    altered["jobs_outputs"]["task_definition_settings"]["value"]["seal"]["volume"][0]["host_path"] = "/host"
    refused("host_volume", altered, "task volumes")
    for name, field, replacement, cause in (
        ("tagged_image", "image", "example:latest", "image digest"),
        ("privileged_container", "privileged", True, "container settings"),
        ("wrong_stop_timeout", "stopTimeout", 30, "container settings"),
        ("writable_root", "readonlyRootFilesystem", False, "container settings"),
        ("root_user", "user", "0:0", "container settings"),
        ("wrong_scratch_mounts", "mountPoints", [], "scratch mounts"),
        ("wrong_entry_point", "entryPoint", ["python", "/example_other.py"], "container settings"),
        ("environment_file", "environmentFiles", [{"type": "s3", "value": "example_environment_file"}], "container settings"),
        ("extra_capability", "linuxParameters", {"capabilities": {"add": ["SYS_ADMIN"], "drop": ["ALL"]}}, "container settings"),
        ("plain_secret", "environment", [{"name": "POLARIS_CLIENT_SECRET", "value": "example_do_not_disclose"}], "plain environment"),
        ("plain_api_key", "environment", [{"name": "EXAMPLE_API_KEY", "value": "example_do_not_disclose"}], "plain environment"),
        ("writer_binding", "secrets", [{"name": "POLARIS_CLIENT_SECRET", "valueFrom": "example_writer"}], "reader bindings"),
    ):
        altered = copy.deepcopy(fixture)
        containers = json.loads(altered["jobs_outputs"]["container_definitions"]["value"]["analytics_read"])
        containers[0][field] = replacement
        altered["jobs_outputs"]["container_definitions"]["value"]["analytics_read"] = json.dumps(containers)
        refused(name, altered, cause)
        if name == "plain_secret":
            result = invoke(altered)
            checks["private_diagnostics"] = "example_do_not_disclose" not in result.stdout + result.stderr
    for name, field, replacement in (
        ("wrong_log_group", "options", {"awslogs-group": "example_other", "awslogs-region": "eu-west-1", "awslogs-stream-prefix": "jobs"}),
        ("log_secret_options", "secretOptions", [{"name": "example_option", "valueFrom": "example_parameter"}]),
    ):
        altered = copy.deepcopy(fixture)
        containers = json.loads(altered["jobs_outputs"]["container_definitions"]["value"]["analytics_read"])
        containers[0]["logConfiguration"][field] = replacement
        altered["jobs_outputs"]["container_definitions"]["value"]["analytics_read"] = json.dumps(containers)
        refused(name, altered, "task log binding")
    altered = copy.deepcopy(fixture)
    altered["catalog_binding"].update({"private_ip": "8.8.8.8", "endpoint": "http://8.8.8.8:8181/api/catalog"})
    refused("public_catalog", altered, "private catalog")
    refused("wrong_catalog_endpoint", change("catalog_binding", "endpoint", "http://10.23.0.11:8181/api/catalog"), "private catalog")
    altered = copy.deepcopy(fixture)
    altered["catalog_binding"].pop("source_inventory_sha256")
    refused("missing_source_inventory", altered, "catalog contract")
    altered = copy.deepcopy(fixture)
    altered["catalog_binding"]["reader_parameters"]["POLARIS_CLIENT_SECRET"] = altered["catalog_binding"]["reader_parameters"]["POLARIS_CLIENT_ID"]
    refused("mixed_reader_pair", altered, "reader parameters")
    invoke(fixture)
    path = folder / "image_measurements.json"
    path.write_text(json.dumps(fixture["image_measurements"]).replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1'))
    arguments: list[str] = []
    for name in INPUTS:
        arguments += ["--" + name.replace("_", "-"), str(folder / f"{name}.json")]
    arguments += ["--output", str(output)]
    result = run_command(sys.executable, ["-m", "scripts.infrastructure.render_ecs_target", *arguments], cwd=root, timeout=10)
    checks["duplicate_json"] = result.returncode == 2 and "duplicate JSON field" in result.stderr and output.read_bytes() == original
    path.write_text('{"value": NaN}')
    result = run_command(sys.executable, ["-m", "scripts.infrastructure.render_ecs_target", *arguments], cwd=root, timeout=10)
    checks["nonfinite_json"] = result.returncode == 2 and "nonfinite JSON number" in result.stderr and output.read_bytes() == original
    payload = json.dumps(fixture["image_measurements"]).encode()
    path.write_bytes(payload + b" " * (32 * 1024**2 - len(payload)))
    result = run_command(sys.executable, ["-m", "scripts.infrastructure.render_ecs_target", *arguments], cwd=root, timeout=10)
    checks["input_at_record_bound"] = result.returncode == 0 and output.read_bytes() == original
    with path.open("ab") as handle:
        handle.write(b" ")
    result = run_command(sys.executable, ["-m", "scripts.infrastructure.render_ecs_target", *arguments], cwd=root, timeout=10)
    checks["input_over_record_bound"] = result.returncode == 2 and "capture exceeds the 32 MiB" in result.stderr and output.read_bytes() == original
    invoke(fixture)
    linked = folder / "linked_target.json"
    linked.write_bytes(original)
    output.unlink()
    output.symlink_to(linked)
    result = invoke(fixture)
    checks["symbolic_output"] = result.returncode == 2 and "symbolic link" in result.stderr and output.is_symlink() and linked.read_bytes() == original
    output.unlink()
    invoke(fixture)
    blocked = folder / "blocked_parent"
    blocked.write_text("existing parent file")
    result = invoke(fixture, destination=blocked / "target.json")
    checks["output_write_failure"] = (
        result.returncode == 2
        and "Could not write the target file." in result.stderr
        and "captures do not match" not in result.stderr
        and output.read_bytes() == original
        and blocked.read_text() == "existing parent file"
    )
    arguments[arguments.index("--storage-outputs") + 1] = str(folder / ".env")
    result = run_command(sys.executable, ["-m", "scripts.infrastructure.render_ecs_target", *arguments], cwd=root, timeout=10)
    checks["protected_input_path"] = result.returncode == 2 and "protected deployment" in result.stderr and output.read_bytes() == original
    return checks


def writer_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Render each closed writer binding and reject changed roles, pairs, placement and inventories."""
    fixture = json.loads((root / SOURCES[-1]).read_text())
    reader_target = json.loads((root / SOURCES[-2]).read_text())
    folder.mkdir()
    output = folder / "target.json"
    layers = ("e2e", "silver", "gold", "features", "bronze")
    checks: dict[str, bool] = {}

    def capture(selected: tuple[str, ...]) -> dict[str, Any]:
        value = copy.deepcopy(fixture)
        exports = value["jobs_outputs"]
        contracts: dict[str, Any] = {}
        for layer in selected:
            name, role = "spark_write_" + layer, "write_" + layer + "_task"
            role_name = "example_project_job_" + role + "_dev"
            role_arn = "arn:aws:iam::000000000000:role/" + role_name
            value["iam_outputs"]["job_roles"]["value"][role] = {
                "name": role_name,
                "arn": role_arn,
                "unique_id": "AROA" + ("E", "S", "G", "F", "B")[layers.index(layer)] * 17,
            }
            task = copy.deepcopy(exports["task_definition_settings"]["value"]["spark_read"])
            task.update(family="example_project_dev_job_" + name, task_role_arn=role_arn)
            exports["task_definition_settings"]["value"][name] = task
            exports["task_definition_arns"]["value"][name] = "arn:aws:ecs:eu-west-1:000000000000:task-definition/" + task["family"] + ":1"
            container = json.loads(exports["container_definitions"]["value"]["spark_read"])[0]
            container["name"] = name
            parameters = ["/example_project/aws_polaris_" + layer + "_client_" + suffix for suffix in ("id", "secret")]
            container["secrets"] = [
                {"name": environment, "valueFrom": "arn:aws:ssm:eu-west-1:000000000000:parameter" + parameter}
                for environment, parameter in zip(("POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET"), parameters, strict=True)
            ]
            exports["container_definitions"]["value"][name] = json.dumps([container])
            exports["execution_digests"]["value"][name + "/" + name] = str(layers.index(layer) + 1) * 64
            contracts[name] = {
                "job_class": "lakehouse_write",
                "task_role_key": role,
                "parameter_names": parameters,
                "capacity_providers": ["FARGATE"],
                "network": "lakehouse",
                "assign_public_ip": "DISABLED",
            }
        exports["writer_dispatch_contracts"] = {"sensitive": False, "type": ["map", "object"], "value": contracts}
        dispatch = exports["dispatch_contract"]["value"]
        dispatch["class_networks"]["lakehouse_write"] = "lakehouse"
        dispatch["class_capacity_providers"]["lakehouse_write"] = ["FARGATE"]
        dispatch["definition_classes"].update({"spark_write_" + layer: "lakehouse_write" for layer in selected})
        return value

    def invoke(value: dict[str, Any], *extra: str) -> Any:
        arguments = ["-m", "scripts.infrastructure.render_ecs_target"]
        for name in INPUTS:
            path = folder / (name + ".json")
            path.write_text(json.dumps(value[name]))
            arguments += ["--" + name.replace("_", "-"), str(path)]
        return run_command(sys.executable, [*arguments, "--output", str(output), *extra], cwd=root, timeout=10)

    for selected in (*(tuple([layer]) for layer in layers), layers):
        value = capture(selected)
        result = invoke(value)
        label = "all_writers" if len(selected) > 1 else selected[0] + "_writer"
        checks[label] = result.returncode == 0 and output.exists()
        if not checks[label]:
            (folder / (label + "_failure.txt")).write_text(result.stdout + result.stderr)
            continue
        target = json.loads(output.read_text())
        checks[label + "_closed_bindings"] = (
            set(target["definitions"]) == set(reader_target["definitions"]) | {"spark_write_" + layer for layer in selected}
            and all(target["definitions"][name] == definition for name, definition in reader_target["definitions"].items())
            and target["enabled"] is False
            and target["class_networks"] == reader_target["class_networks"] | {"lakehouse_write": "lakehouse"}
            and target.get("class_capacity_providers")
            == {
                "offline": ["FARGATE_SPOT", "FARGATE"],
                "seal": ["FARGATE_SPOT", "FARGATE"],
                "lakehouse_read": ["FARGATE_SPOT", "FARGATE"],
                "lakehouse_write": ["FARGATE"],
            }
            and target.get("assign_public_ip") == "DISABLED"
            and all(
                target["definitions"]["spark_write_" + layer]["job_class"] == "lakehouse_write"
                and target["definitions"]["spark_write_" + layer]["task_role_key"] == "write_" + layer + "_task"
                and target["definitions"]["spark_write_" + layer]["execution_sha256"] == str(layers.index(layer) + 1) * 64
                and target["definitions"]["spark_write_" + layer]["image"] == reader_target["definitions"]["spark_read"]["image"]
                for layer in selected
            )
        )
    if not checks.get("all_writers"):
        return checks
    value = capture(layers)
    invoke(value)
    original = output.read_bytes()
    checks["writer_render_repeats"] = invoke(value).returncode == 0 and output.read_bytes() == original
    checks["writer_check_preserves_bytes"] = invoke(value, "--check").returncode == 0 and output.read_bytes() == original
    checks["writer_target_is_private"] = stat.S_IMODE(output.stat().st_mode) == 0o600

    def refused(label: str, altered: dict[str, Any], cause: str) -> None:
        result = invoke(altered)
        checks[label] = result.returncode == 2 and cause in result.stderr and output.read_bytes() == original

    for layer, other in zip(layers, (*layers[1:], layers[0]), strict=True):
        name, role = "spark_write_" + layer, "write_" + layer + "_task"
        altered = copy.deepcopy(value)
        altered["jobs_outputs"]["task_definition_settings"]["value"][name]["task_role_arn"] = altered["iam_outputs"]["job_roles"]["value"][
            "write_" + other + "_task"
        ]["arn"]
        refused(layer + "_swapped_role", altered, "task role binding")
        altered = copy.deepcopy(value)
        containers = json.loads(altered["jobs_outputs"]["container_definitions"]["value"][name])
        containers[0]["secrets"] = json.loads(altered["jobs_outputs"]["container_definitions"]["value"]["spark_write_" + other])[0]["secrets"]
        altered["jobs_outputs"]["container_definitions"]["value"][name] = json.dumps(containers)
        refused(layer + "_swapped_pair", altered, "writer bindings")
        for field, replacement in (("capacity_providers", ["FARGATE_SPOT", "FARGATE"]), ("network", "offline"), ("assign_public_ip", "ENABLED")):
            altered = copy.deepcopy(value)
            altered["jobs_outputs"]["writer_dispatch_contracts"]["value"][name][field] = replacement
            refused(layer + "_invalid_" + field, altered, "writer dispatch contract")
        altered = copy.deepcopy(value)
        altered["iam_outputs"]["job_roles"]["value"][role], altered["iam_outputs"]["job_roles"]["value"]["write_" + other + "_task"] = (
            altered["iam_outputs"]["job_roles"]["value"]["write_" + other + "_task"],
            altered["iam_outputs"]["job_roles"]["value"][role],
        )
        for changed_layer in (layer, other):
            altered["jobs_outputs"]["task_definition_settings"]["value"]["spark_write_" + changed_layer]["task_role_arn"] = altered["iam_outputs"]["job_roles"][
                "value"
            ]["write_" + changed_layer + "_task"]["arn"]
        refused(layer + "_swapped_role_inventory", altered, "writer role identity")
        altered = copy.deepcopy(value)
        altered["jobs_outputs"]["writer_dispatch_contracts"]["value"][name]["parameter_names"] = [
            "/example_project/aws_polaris_" + other + "_client_" + suffix for suffix in ("id", "secret")
        ]
        containers = json.loads(altered["jobs_outputs"]["container_definitions"]["value"][name])
        containers[0]["secrets"] = json.loads(altered["jobs_outputs"]["container_definitions"]["value"]["spark_write_" + other])[0]["secrets"]
        altered["jobs_outputs"]["container_definitions"]["value"][name] = json.dumps(containers)
        refused(layer + "_swapped_pair_inventory", altered, "writer parameter names")
        altered = copy.deepcopy(value)
        del altered["jobs_outputs"]["execution_digests"]["value"][name + "/" + name]
        refused(layer + "_missing_pin", altered, "execution pins")
    for reader in reader_target["definitions"]:
        altered = copy.deepcopy(value)
        for field in ("task_definition_arns", "container_definitions", "task_definition_settings"):
            del altered["jobs_outputs"][field]["value"][reader]
        refused("writer_capture_requires_" + reader, altered, "five read definitions")
    altered = copy.deepcopy(value)
    altered["iam_outputs"]["job_roles"]["value"]["write_unknown_task"] = {
        "name": "example_project_job_write_unknown_task_dev",
        "arn": "arn:aws:iam::000000000000:role/example_project_job_write_unknown_task_dev",
        "unique_id": "AROA" + "U" * 17,
    }
    refused("unknown_writer_role", altered, "role contract")
    altered = copy.deepcopy(value)
    altered["jobs_outputs"].pop("writer_dispatch_contracts")
    refused("missing_writer_dispatch_contracts", altered, "writer_dispatch_contracts")
    altered = copy.deepcopy(value)
    containers = json.loads(altered["jobs_outputs"]["container_definitions"]["value"]["spark_write_e2e"])
    containers[0]["secrets"].append({"name": "EXTRA_PARAMETER", "valueFrom": "example_parameter"})
    altered["jobs_outputs"]["container_definitions"]["value"]["spark_write_e2e"] = json.dumps(containers)
    refused("extra_writer_parameter", altered, "writer bindings")
    for field, dispatch_delta, cause in (
        ("class_capacity_providers", {"lakehouse_write": ["FARGATE_SPOT"]}, "class capacity providers"),
        ("class_networks", {"lakehouse_write": "offline"}, "class networks"),
        ("definition_classes", {"spark_write_e2e": "offline"}, "definition classes"),
    ):
        altered = copy.deepcopy(value)
        altered["jobs_outputs"]["dispatch_contract"]["value"][field].update(dispatch_delta)
        refused("dispatch_changed_" + field, altered, cause)
        altered = copy.deepcopy(value)
        del altered["jobs_outputs"]["dispatch_contract"]["value"][field]
        refused("dispatch_missing_" + field, altered, cause)
    altered = copy.deepcopy(value)
    del altered["jobs_outputs"]["dispatch_contract"]["value"]["class_capacity_providers"]["lakehouse_write"]
    refused("dispatch_missing_writer_capacity", altered, "class capacity providers")
    altered = copy.deepcopy(value)
    altered["jobs_outputs"]["dispatch_contract"]["value"]["class_capacity_providers"]["lakehouse_read"] = ["FARGATE"]
    refused("dispatch_changed_reader_capacity", altered, "class capacity providers")
    altered = copy.deepcopy(value)
    altered["jobs_outputs"]["dispatch_contract"]["value"]["assign_public_ip"] = "ENABLED"
    refused("dispatch_public_ip", altered, "public IP")
    altered = copy.deepcopy(value)
    del altered["jobs_outputs"]["dispatch_contract"]
    refused("missing_dispatch_contract", altered, "dispatch_contract")
    return checks


def record_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Exercise the general writer's exact byte limit beyond the closed target schema's small records."""
    output = folder / "bounded_record.json"
    program = (
        "import sys\n"
        "from pathlib import Path\n"
        "from scripts.infrastructure.render_ecs_target import TargetError, write_private_record\n"
        "try:\n"
        "    write_private_record(Path(sys.argv[1]), {'padding': 'x' * int(sys.argv[2])})\n"
        "except TargetError as error:\n"
        "    sys.stderr.write(str(error) + '\\n')\n"
        "    sys.exit(2)\n"
    )
    padding = 32 * 1024**2 - len('{\n  "padding": ""\n}\n')
    result = run_command(sys.executable, ["-c", program, str(output), str(padding)], cwd=root, timeout=10)
    checks = {"output_at_record_bound": result.returncode == 0 and output.exists() and output.stat().st_size == 32 * 1024**2}
    if not checks["output_at_record_bound"]:
        return checks
    original = hashlib.sha256(output.read_bytes()).hexdigest()
    result = run_command(sys.executable, ["-c", program, str(output), str(padding + 1)], cwd=root, timeout=10)
    checks["output_over_record_bound"] = (
        result.returncode == 2 and "generated record exceeds the 32 MiB" in result.stderr and hashlib.sha256(output.read_bytes()).hexdigest() == original
    )
    return checks


def _wrapper_result(workspace: Path, environment: dict[str, str], arguments: list[str]) -> tuple[Any, list[dict[str, Any]]]:
    capture = workspace / "calls.jsonl"
    capture.write_text("")
    result = run_command("bash", arguments, cwd=workspace, env=environment, timeout=10)
    return result, [json.loads(line) for line in capture.read_text().splitlines()]


def wrapper_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Capture the real wrapper's dispatch without starting Terraform or reading deployment files."""
    workspace = (folder / "wrapper").resolve()
    script = workspace / "scripts/infrastructure/terraform.sh"
    script.parent.mkdir(parents=True)
    shutil.copy2(root / "scripts/infrastructure/terraform.sh", script)
    binary = workspace / "bin"
    binary.mkdir()
    capture = workspace / "calls.jsonl"
    settings = binary / "capture_settings.json"
    settings.write_text(json.dumps({"capture": str(capture), "render_exit": 0}))
    for name in ("python_capture", "terraform"):
        executable = binary / name
        executable.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "from pathlib import Path\n"
            "settings = json.loads(Path(__file__).with_name('capture_settings.json').read_text())\n"
            "with open(settings['capture'], 'a') as handle:\n"
            "    handle.write(json.dumps({'name': os.path.basename(sys.argv[0]), 'args': sys.argv[1:]}) + '\\n')\n"
            "sys.exit(settings['render_exit'] if os.path.basename(sys.argv[0]) == 'python_capture' else 0)\n"
        )
        executable.chmod(0o700)
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "TF_"))}
    environment.update({"PATH": str(binary) + os.pathsep + os.environ.get("PATH", ""), "PYTHON_BIN": str(binary / "python_capture")})

    result, calls = _wrapper_result(workspace, environment, ["scripts/infrastructure/terraform.sh", "--jobs", "plan", "-out=review.tfplan"])
    checks = {
        "jobs_plan": result.returncode == 0
        and len(calls) == 2
        and calls[0]["args"] == ["-m", "scripts.infrastructure.render_jobs_config", "--verify-identity"]
        and calls[1]["args"] == [f"-chdir={workspace}/infra/jobs", "plan", "-input=false", "-out=review.tfplan"]
    }
    result, calls = _wrapper_result(workspace, environment, ["scripts/infrastructure/terraform.sh", "--jobs", "apply", "review.tfplan"])
    checks["jobs_apply"] = (
        result.returncode == 0
        and len(calls) == 2
        and calls[0]["args"]
        == ["-m", "scripts.infrastructure.render_jobs_config", "--check-plan", f"{workspace}/infra/jobs/review.tfplan", "--verify-identity"]
    )
    result, calls = _wrapper_result(workspace, environment, ["scripts/infrastructure/terraform.sh", "--jobs", "validate"])
    checks["jobs_validate"] = result.returncode == 0 and len(calls) == 2 and calls[0]["args"] == ["-m", "scripts.infrastructure.render_jobs_config"]
    for name, (result, calls), cause in (
        (
            "multiple_stacks",
            _wrapper_result(workspace, environment, ["scripts/infrastructure/terraform.sh", "--platform", "--jobs", "plan"]),
            "Choose one stack",
        ),
        (
            "jobs_administrator",
            _wrapper_result(workspace, environment, ["scripts/infrastructure/terraform.sh", "--jobs", "--admin-profile", "example_admin", "plan"]),
            "only with --iam",
        ),
        (
            "injected_variable",
            _wrapper_result(workspace, environment, ["scripts/infrastructure/terraform.sh", "--jobs", "plan", "-var=unsafe=true"]),
            "Set project inputs",
        ),
        (
            "injected_environment",
            _wrapper_result(workspace, {**environment, "TF_CLI_ARGS_plan": "-var=unsafe=true"}, ["scripts/infrastructure/terraform.sh", "--jobs", "plan"]),
            "Unset TF_CLI_ARGS_plan",
        ),
        ("apply_without_plan", _wrapper_result(workspace, environment, ["scripts/infrastructure/terraform.sh", "--jobs", "apply"]), "Apply requires one"),
    ):
        checks[name] = result.returncode == 2 and cause in result.stderr and not calls
    settings.write_text(json.dumps({"capture": str(capture), "render_exit": 9}))
    result, calls = _wrapper_result(workspace, environment, ["scripts/infrastructure/terraform.sh", "--jobs", "plan"])
    checks["renderer_failure_stops_dispatch"] = result.returncode == 9 and len(calls) == 1
    return checks


def jobs_config_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Run jobs input rendering and saved-plan refusals against an inert plan reader."""
    workspace = folder / "jobs_config"
    for name in (
        "scripts/process.py",
        "scripts/infrastructure/render_project_config.py",
        "scripts/infrastructure/render_ecs_target.py",
        "scripts/infrastructure/render_jobs_config.py",
    ):
        source = root / name
        if not source.is_file():
            return {"jobs_renderer_present": False}
        destination = workspace / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    environment_file = workspace / "environment.txt"
    environment_file.write_text(
        "AWS_PROFILE=example_project_dev\nAWS_ACCOUNT_ID=000000000000\nAWS_REGION=eu-west-1\n"
        "ENVIRONMENT=dev\nPROJECT_NAME=example_project\nS3_BUCKET=example-project-data\n"
    )
    values = {
        "aws_profile": "example_project_dev",
        "expected_account_id": "000000000000",
        "aws_region": "eu-west-1",
        "environment": "dev",
        "project_name": "example_project",
        "data_bucket_name": "example-project-data",
    }
    declared: dict[str, Any] = {name: {} for name in values} | {"optional_setting": {"default": False}}
    plan: dict[str, Any] = {
        "complete": True,
        "errored": False,
        "variables": {name: {"value": value} for name, value in (values | {"optional_setting": False}).items()},
        "configuration": {"root_module": {"variables": declared}},
    }
    capture = workspace / "plan_capture.json"
    binary = workspace / "bin"
    binary.mkdir()
    executable = binary / "terraform"
    executable.write_text(
        f"#!{sys.executable}\nimport pathlib, sys\nsys.stdout.write((pathlib.Path(__file__).resolve().parents[1] / 'plan_capture.json').read_text())\n"
    )
    executable.chmod(0o700)
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "TF_"))}
    environment.update({"PATH": str(binary) + os.pathsep + os.environ.get("PATH", "")})
    output = workspace / "infra/jobs/deployment.auto.tfvars.json"

    def invoke(value: dict[str, Any] | None = None, *extra: str) -> Any:
        arguments = ["-m", "scripts.infrastructure.render_jobs_config", "--env-file", str(environment_file), *extra]
        if value is not None:
            capture.write_text(json.dumps(value))
            arguments += ["--check-plan", str(workspace / "review.tfplan")]
        return run_command(sys.executable, arguments, cwd=workspace, env=environment, timeout=10)

    result = invoke()
    checks = {"jobs_inputs": result.returncode == 0 and output.exists() and json.loads(output.read_text()) == values}
    if not checks["jobs_inputs"]:
        return checks
    original = output.read_bytes()
    checks["jobs_private_inputs"] = stat.S_IMODE(output.stat().st_mode) == 0o600
    checks["jobs_repeat_inputs"] = invoke().returncode == 0 and output.read_bytes() == original
    checks["reviewed_plan"] = invoke(plan).returncode == 0
    for name, change in (
        ("incomplete_plan", {"complete": False}),
        ("errored_plan", {"errored": True}),
        (
            "secret_plan_resource",
            {"planned_values": {"root_module": {"resources": [{"address": "example_secret", "type": "aws_ssm_parameter", "mode": "data"}]}}},
        ),
    ):
        result = invoke(plan | change)
        checks[name] = result.returncode == 2 and "saved jobs plan" in result.stderr and output.read_bytes() == original
    altered = copy.deepcopy(plan)
    altered["variables"]["aws_region"]["value"] = "eu-west-2"
    result = invoke(altered)
    checks["changed_plan_input"] = result.returncode == 2 and "saved jobs plan" in result.stderr and output.read_bytes() == original
    altered = copy.deepcopy(plan)
    altered["variables"]["optional_setting"]["value"] = True
    result = invoke(altered)
    checks["nondefault_plan_input"] = result.returncode == 2 and "saved jobs plan" in result.stderr and output.read_bytes() == original
    altered = copy.deepcopy(plan)
    altered["variables"].pop("optional_setting")
    result = invoke(altered)
    checks["missing_declared_plan_input"] = result.returncode == 2 and "saved jobs plan" in result.stderr and output.read_bytes() == original
    return checks


def main() -> int:
    """Save command outcomes and source hashes for this bounded offline configuration check."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    started = datetime.now(UTC)
    folder = arguments.output or ROOT / "data/e2e/ecs_target" / started.strftime("render_%Y%m%dT%H%M%S_%fZ")
    folder.mkdir(parents=True, exist_ok=False)
    hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else None for name in SOURCES}
    checks: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="ecs_render_") as temporary:
        scratch = Path(temporary)
        checks["render"] = command_cases(ROOT, scratch)
        checks["writers"] = writer_cases(ROOT, scratch / "writers")
        checks["records"] = record_cases(ROOT, scratch)
        checks["wrapper"] = wrapper_cases(ROOT, scratch)
        checks["jobs_config"] = jobs_config_cases(ROOT, scratch)
    failed = [f"{group}.{name}" for group, values in checks.items() for name, passed in values.items() if not passed]
    after = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else None for name in SOURCES}
    if hashes != after:
        failed.append("source_changed")
    report = {
        "started_utc": started.isoformat(),
        "classification": "Synthetic",
        "scope": "native file-rendering commands and inert wrapper dispatch",
        "source_sha256": hashes,
        "command": [sys.executable, "-m", "scripts.infrastructure.run_ecs_render_e2e", *sys.argv[1:]],
        "checks": checks,
        "failed": failed,
        "cleanup": "done",
        "limits": ["No AWS, Terraform, Docker, launcher, catalog or actual data execution"],
    }
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (folder / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": report["scope"], "created": started.isoformat(), "end_condition": "Review accepted and no record cites this run"}, indent=2
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"report": str(folder / "report.json"), "failed": failed}) + "\n")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
