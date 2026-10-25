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
PROCEDURE_SOURCES = (
    "scripts/infrastructure/ecs_real_runs.py",
    "config/orchestration/ecs_real_runs.json",
    "data_contracts/orchestration/ecs_real_run_request.schema.json",
    "data_contracts/orchestration/examples/ecs_real_run_request.json",
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


def procedure_cases(root: Path, folder: Path) -> dict[str, bool]:
    """Prepare every real-run checklist without dispatch and refuse unsafe or incomplete run inputs."""
    target = json.loads((root / "data_contracts/orchestration/examples/ecs_target.json").read_text())
    target["enabled"] = True
    request = json.loads((root / "data_contracts/orchestration/examples/ecs_real_run_request.json").read_text())
    target_path, request_path, output = (folder / name for name in ("enabled_target.json", "run_request.json", "run_card.json"))

    def invoke(case: str, target_value: dict[str, Any], request_value: dict[str, Any]) -> Any:
        target_path.write_text(json.dumps(target_value))
        request_path.write_text(json.dumps(request_value))
        return run_command(
            sys.executable,
            [
                "-m",
                "scripts.infrastructure.ecs_real_runs",
                "--case",
                case,
                "--target",
                str(target_path),
                "--run-inputs",
                str(request_path),
                "--output",
                str(output),
            ],
            cwd=root,
            timeout=10,
        )

    checks = {}
    for case, outer, first_tasks in (("R1", 7200, 2), ("R2", 3600, 1), ("R3", 14400, 1)):
        result = invoke(case, target, request)
        card = json.loads(output.read_text()) if output.exists() else {}
        checks[case] = (
            result.returncode == 0
            and card.get("preparation_only") is True
            and card.get("execution_verified") is False
            and card.get("proof") == "A"
            and card.get("requested_target") == "ecs"
            and card.get("outer_deadline_seconds") == outer
            and card.get("first_step_task_count") == first_tasks
            and card.get("target_sha256") == hashlib.sha256(target_path.read_bytes()).hexdigest()
            and card.get("request") == request
            and card.get("task_deadline_not_after_utc") == "2026-11-04T10:56:30Z"
        )
    if not all(checks.values()):
        return checks
    original = output.read_bytes()
    for name, target_delta, request_delta, cause in (
        ("disabled_cloud_target", {"enabled": False}, {}, "enabled target"),
        ("mac_preparation_enabled", {"mac_preparation": {"enabled": True, "transport": "session_manager_owned_relay"}}, {}, "target contract"),
        ("changed_cloud_cap", {"cloud_limits": {"max_tasks": 4, "max_vcpu": 16, "max_memory_gib": 120}}, {}, "target contract"),
        ("unregistered_catalog", {"catalog": None}, {}, "registered AWS catalog"),
        ("mac_dataset_upload", {}, {"dataset_uploads": ["example.csv"]}, "run request"),
        ("synthetic_real_input", {}, {"source_kind": "synthetic_fixture"}, "run request"),
        ("wrong_source_inventory", {}, {"source_inventory_sha256": "f" * 64}, "source inventory"),
        ("missing_run_approval", {}, {"owner_approval_reference": ""}, "run request"),
        ("unbounded_run", {}, {"window_deadline_utc": "2026-11-05T10:00:00Z"}, "run window"),
        ("reversed_window", {}, {"window_deadline_utc": "2026-11-04T09:00:00Z"}, "run window"),
        ("no_cleanup_time", {}, {"window_deadline_utc": "2026-11-04T10:01:00Z"}, "run window"),
        ("no_stop_grace_time", {}, {"window_deadline_utc": "2026-11-04T10:03:00Z"}, "run window"),
    ):
        result = invoke("R3", target | target_delta, request | request_delta)
        checks[name] = result.returncode == 2 and cause in result.stderr and output.read_bytes() == original
    for field, cause in (("cluster_arn", "cluster identity"), ("task_definition_arn", "fixed task revision"), ("image", "image digest")):
        for dimension, old, new in (("account", "000000000000", "111111111111"), ("region", "eu-west-1", "eu-west-2")):
            altered = copy.deepcopy(target)
            subject = altered if field == "cluster_arn" else altered["definitions"]["seal"]
            subject[field] = subject[field].replace(old, new)
            result = invoke("R3", altered, request)
            checks[f"{field}_{dimension}"] = result.returncode == 2 and cause in result.stderr and output.read_bytes() == original
    for field in ("unique_id", "arn"):
        altered = copy.deepcopy(target)
        duplicate = altered["job_roles"]["offline_task"]
        duplicate[field] = altered["job_roles"]["execution"][field]
        if field == "arn":
            duplicate["name"] = altered["job_roles"]["execution"]["name"]
        result = invoke("R3", altered, request)
        checks[f"repeated_role_{field}"] = result.returncode == 2 and "Repeated role identity" in result.stderr and output.read_bytes() == original
    return checks


def main() -> int:
    """Save command outcomes and source hashes for this bounded offline configuration check."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    started = datetime.now(UTC)
    folder = arguments.output or ROOT / "data/e2e/ecs_target" / started.strftime("render_%Y%m%dT%H%M%S_%fZ")
    folder.mkdir(parents=True, exist_ok=False)
    hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else None for name in SOURCES + PROCEDURE_SOURCES}
    checks: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="ecs_render_") as temporary:
        scratch = Path(temporary)
        checks["render"] = command_cases(ROOT, scratch)
        checks["records"] = record_cases(ROOT, scratch)
        checks["wrapper"] = wrapper_cases(ROOT, scratch)
        checks["jobs_config"] = jobs_config_cases(ROOT, scratch)
        checks["procedures"] = procedure_cases(ROOT, scratch)
    failed = [f"{group}.{name}" for group, values in checks.items() for name, passed in values.items() if not passed]
    after = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else None for name in SOURCES + PROCEDURE_SOURCES}
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
