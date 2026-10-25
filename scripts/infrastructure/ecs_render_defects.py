"""Plant renderer and case defects in disposable copies and record the command cases that detect them."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.infrastructure.run_ecs_render_e2e import PROCEDURE_SOURCES, ROOT, SOURCES
from scripts.process import run_command

RENDERER = "scripts/infrastructure/render_ecs_target.py"
PROCEDURES = "scripts/infrastructure/ecs_real_runs.py"
MUTATIONS = (
    ("foreign_role_accepted", RENDERER, 'if role["arn"] != f"arn:aws:iam::{account}:role/{role[\'name\']}":', "if False:", "render.foreign_role"),
    ("shared_class_group", RENDERER, 'if len(set(network["security_group_ids"].values())) != 2:', "if False:", "render.mixed_class_network"),
    ("wrong_endpoint_subnet", RENDERER, 'if network["subnet_id"] not in _output(platform, "public_subnet_ids"):', "if False:", "render.wrong_subnet"),
    (
        "compressed_image_omitted",
        RENDERER,
        '(measurement["compressed_bytes"] + GIB - 1) // GIB',
        '(measurement["uncompressed_bytes"] + GIB - 1) // GIB',
        "render.render_command",
    ),
    ("image_rounds_down", RENDERER, '(measurement["compressed_bytes"] + GIB - 1) // GIB', 'measurement["compressed_bytes"] // GIB', "render.render_command"),
    ("privileged_container", RENDERER, '        or container.get("privileged", False) is not False\n', "", "render.privileged_container"),
    (
        "writer_pair_accepted",
        RENDERER,
        "if not isinstance(secrets, list) or any(not isinstance(row, dict) for row in secrets) "
        'or sorted(secrets, key=lambda row: str(row.get("name"))) != expected:',
        "if False:",
        "render.writer_binding",
    ),
    (
        "public_catalog_accepted",
        RENDERER,
        'if not any(address in ipaddress.IPv4Network(network) for network in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")):',
        "if False:",
        "render.public_catalog",
    ),
    ("duplicate_json_accepted", RENDERER, "if key in value:", "if False:", "render.duplicate_json"),
    ("protected_path_read", RENDERER, "    _allowed_path(path)\n    try:\n", "    try:\n", "render.protected_input_path"),
    ("output_mode_public", RENDERER, "os.chmod(temporary, 0o600)", "os.chmod(temporary, 0o644)", "render.private_output"),
    ("check_mode_writes", RENDERER, "    if check:\n", "    if False:\n", "render.stale_check"),
    (
        "secret_plan_accepted",
        "scripts/infrastructure/render_jobs_config.py",
        "verify_plan_resources(plan)",
        "verify_plan_resources({})",
        "jobs_config.secret_plan_resource",
    ),
    ("wrapper_variable_injection", "scripts/infrastructure/terraform.sh", "-var*|-chdir*)", "-unused_option*)", "wrapper.injected_variable"),
    (
        "wrapper_ignores_renderer_failure",
        "scripts/infrastructure/terraform.sh",
        "set -euo pipefail",
        "set -uo pipefail",
        "wrapper.renderer_failure_stops_dispatch",
    ),
    (
        "stop_grace_not_reserved",
        PROCEDURES,
        'target["deadlines"]["stop_grace_seconds"] + target["deadlines"]["cleanup_seconds"]',
        'target["deadlines"]["cleanup_seconds"]',
        "procedures.R3",
    ),
    (
        "unbound_source_inventory",
        PROCEDURES,
        'if request["source_inventory_sha256"] != digest or target["catalog"]["source_inventory_sha256"] != digest:',
        "if False:",
        "procedures.wrong_source_inventory",
    ),
    (
        "reader_binding_case_omitted",
        "scripts/infrastructure/run_ecs_render_e2e.py",
        '        ("writer_binding", "secrets", [{"name": "POLARIS_CLIENT_SECRET", "valueFrom": "example_writer"}], "reader bindings"),\n',
        "",
        "missing:render.writer_binding",
    ),
    (
        "slashless_group_refused",
        "data_contracts/orchestration/ecs_target.schema.json",
        '"^(?:[A-Za-z0-9_./#-]{1,512})$(?![\\\\s\\\\S])"',
        '"^(?:/[A-Za-z0-9_./-]{1,511})$(?![\\\\s\\\\S])"',
        "render.render_command",
    ),
    (
        "invalid_group_accepted",
        "data_contracts/orchestration/ecs_target.schema.json",
        '"^(?:[A-Za-z0-9_./#-]{1,512})$(?![\\\\s\\\\S])"',
        '"^.*$"',
        "render.empty_log_group",
    ),
    (
        "group_newline_accepted",
        "data_contracts/orchestration/ecs_target.schema.json",
        '"^(?:[A-Za-z0-9_./#-]{1,512})$(?![\\\\s\\\\S])"',
        '"^(?:[A-Za-z0-9_./#-]{1,512})$"',
        "render.log_group_newline",
    ),
    (
        "wrong_architecture_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        '        or task.get("runtime_platform") != [{"cpu_architecture": "ARM64", "operating_system_family": "LINUX"}]\n',
        "",
        "render.wrong_architecture",
    ),
    (
        "wrong_network_mode_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        '        or task.get("network_mode") != "awsvpc"\n',
        "",
        "render.wrong_network_mode",
    ),
    (
        "wrong_project_tag_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'if not isinstance(task.get("tags"), dict) or task["tags"].get("Project") != project:',
        'if not isinstance(task.get("tags"), dict):',
        "render.wrong_project_tag",
    ),
    (
        "wrong_execution_role_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        ' or task.get("execution_role_arn") != roles["execution"]["arn"]',
        "",
        "render.wrong_execution_role",
    ),
    ("mismatched_family_accepted", "scripts/infrastructure/render_ecs_target.py", ' or task.get("family") != match[1]', "", "render.mismatched_family"),
    (
        "wrong_stop_timeout_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        '        or container.get("stopTimeout") != 90\n',
        "",
        "render.wrong_stop_timeout",
    ),
    (
        "writable_root_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        '        or container.get("readonlyRootFilesystem") is not True\n',
        "",
        "render.writable_root",
    ),
    (
        "root_user_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        '        or re.fullmatch(r"[1-9][0-9]{2,5}:[1-9][0-9]{2,5}", container["user"]) is None\n',
        "",
        "render.root_user",
    ),
    (
        "wrong_log_group_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        '        or logs.get("options", {}).get("awslogs-group") != log_group\n',
        "",
        "render.wrong_log_group",
    ),
    ("log_secret_options_accepted", "scripts/infrastructure/render_ecs_target.py", '        or logs.get("secretOptions")\n', "", "render.log_secret_options"),
    (
        "wrong_scratch_mounts_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'if sorted(container.get("mountPoints", []), key=lambda row: row.get("sourceVolume", "")) != sorted(mounts, key=lambda row: row["sourceVolume"]):',
        'if not isinstance(container.get("mountPoints", []), list) or not mounts:',
        "render.wrong_scratch_mounts",
    ),
    (
        "wrong_entry_point_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'container.get("entryPoint") != ENTRY_POINT',
        "False",
        "render.wrong_entry_point",
    ),
    (
        "environment_file_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        '        or container.get("environmentFiles", []) != []\n',
        "",
        "render.environment_file",
    ),
    (
        "extra_execution_pin_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'if set(maps["execution_digests"]) != pin_keys:',
        "if False:",
        "render.extra_execution_pin",
    ),
    (
        "extra_definition_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        ' or any(set(maps[name]) != set(DEFINITIONS) for name in maps if name != "execution_digests")',
        "",
        "render.extra_definition",
    ),
    (
        "wrong_platform_repository_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'if image.rsplit("@", 1)[0] != repositories.get(image_name):',
        "if not isinstance(repositories, dict):",
        "render.wrong_platform_repository",
    ),
    (
        "foreign_cluster_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'if not target["cluster_arn"].startswith(f"arn:aws:ecs:{region}:{account}:cluster/"):',
        'if not isinstance(target["cluster_arn"], str):',
        "procedures.cluster_arn_account",
    ),
    (
        "foreign_task_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'if not definition["task_definition_arn"].startswith(f"arn:aws:ecs:{region}:{account}:task-definition/"):',
        'if not isinstance(definition["task_definition_arn"], str):',
        "procedures.task_definition_arn_account",
    ),
    (
        "foreign_image_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'if not definition["image"].startswith(f"{account}.dkr.ecr.{region}.amazonaws.com/"):',
        'if not isinstance(definition["image"], str):',
        "procedures.image_account",
    ),
    (
        "repeated_role_accepted",
        "scripts/infrastructure/render_ecs_target.py",
        'if len({role["unique_id"] for role in roles.values()}) != len(roles) or len({role["arn"] for role in roles.values()}) != len(roles):',
        "if False:",
        "procedures.repeated_role_unique_id",
    ),
    ("symbolic_output_accepted", "scripts/infrastructure/render_ecs_target.py", "if path.is_symlink():", "if False:", "render.symbolic_output"),
    ("unbounded_capture", "scripts/infrastructure/render_ecs_target.py", "if len(content) > 32 * 1024**2:", "if False:", "render.input_over_record_bound"),
    (
        "unbounded_record",
        "scripts/infrastructure/render_ecs_target.py",
        'if len(content.encode("utf-8")) > 32 * 1024**2:',
        "if False:",
        "records.output_over_record_bound",
    ),
    (
        "write_error_misreported",
        "scripts/infrastructure/render_ecs_target.py",
        "Could not write the target file.",
        "The captures do not match the target contract. No target was written.",
        "render.output_write_failure",
    ),
    (
        "network_case_omitted",
        "scripts/infrastructure/run_ecs_render_e2e.py",
        '        ("wrong_network_mode", "network_mode", "bridge", "Fargate task settings"),\n',
        "",
        "missing:render.wrong_network_mode",
    ),
    (
        "record_bound_cases_omitted",
        "scripts/infrastructure/run_ecs_render_e2e.py",
        '        checks["records"] = record_cases(ROOT, scratch)\n',
        "",
        "missing:records.output_over_record_bound",
    ),
)
COPY_PATHS = SOURCES + PROCEDURE_SOURCES + ("scripts/process.py", "scripts/infrastructure/render_project_config.py")


def _copy(root: Path) -> None:
    for name in COPY_PATHS:
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, destination)


def _run(root: Path) -> tuple[int, dict[str, Any]]:
    output = root / "case_artifact"
    result = run_command(sys.executable, ["-m", "scripts.infrastructure.run_ecs_render_e2e", "--output", str(output)], cwd=root, timeout=45)
    path = output / "report.json"
    return result.returncode, json.loads(path.read_text()) if path.is_file() else {}


def main() -> int:
    """Verify intended mutation failures while keeping production files unchanged."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    started = datetime.now(UTC)
    folder = args.output or ROOT / "data/e2e/ecs_target" / started.strftime("render_defects_%Y%m%dT%H%M%S_%fZ")
    folder.mkdir(parents=True, exist_ok=False)
    paths = (*COPY_PATHS, str(Path(__file__).relative_to(ROOT)))
    before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in paths}
    results = {}
    with tempfile.TemporaryDirectory(prefix="ecs_render_defects_") as temporary:
        scratch = Path(temporary)
        baseline_root = scratch / "baseline"
        _copy(baseline_root)
        code, baseline = _run(baseline_root)
        if code or not baseline or baseline["failed"]:
            raise RuntimeError("The command baseline must pass before planting defects.")
        expected_checks = {f"{group}.{name}" for group, values in baseline["checks"].items() for name in values}
        (folder / "baseline.json").write_text(json.dumps(baseline, indent=2) + "\n")
        for name, path, old, new, expected in MUTATIONS:
            root = scratch / name
            _copy(root)
            subject = root / path
            content = subject.read_text()
            if content.count(old) != 1:
                raise RuntimeError(f"The mutation requires one source match: {name}.")
            subject.write_text(content.replace(old, new))
            if subject.suffix == ".py":
                formatting = run_command(sys.executable, ["-m", "ruff", "format", str(subject)], timeout=10)
                lint = run_command(sys.executable, ["-m", "ruff", "check", str(subject)], timeout=10)
                syntax = formatting.returncode == 0 and lint.returncode == 0
            elif subject.suffix == ".json":
                json.loads(subject.read_text())
                syntax = True
            else:
                syntax = run_command("bash", ["-n", "scripts/infrastructure/terraform.sh"], cwd=root, timeout=10).returncode == 0
            code, artifact = _run(root)
            checks = {f"{group}.{key}" for group, values in artifact.get("checks", {}).items() for key in values}
            missing = sorted(expected_checks - checks)
            caught = (
                code == 0 and expected.removeprefix("missing:") in missing
                if expected.startswith("missing:")
                else code == 1 and expected in artifact.get("failed", [])
            )
            results[name] = {
                "caught": caught,
                "lint_or_syntax_passed": syntax,
                "intended_case": expected,
                "exit_code": code,
                "failed": artifact.get("failed", []),
                "missing_cases": missing,
            }
            (folder / f"{name}.json").write_text(json.dumps(artifact, indent=2) + "\n")
            sys.stdout.write(json.dumps({"defect": name, "caught": caught, "lint_or_syntax_passed": syntax}) + "\n")
            sys.stdout.flush()
    after = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in paths}
    failed = [name for name, value in results.items() if not value["caught"]]
    if before != after:
        failed.append("production_source_changed")
    report = {
        "started_utc": started.isoformat(),
        "classification": "Synthetic",
        "source_sha256": before,
        "results": results,
        "failed": failed,
        "cleanup": "done",
        "command": [sys.executable, "-m", "scripts.infrastructure.ecs_render_defects", *sys.argv[1:]],
        "limits": ["Disposable source copies only", "No cloud, Terraform, Docker, launcher or actual data execution", "45-second deadline per command suite"],
    }
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (folder / "retention.json").write_text(
        json.dumps(
            {
                "class": 4,
                "purpose": "Planted renderer and case defects",
                "created": started.isoformat(),
                "end_condition": "Review accepted and no record cites this run",
            },
            indent=2,
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"report": str(folder / "report.json"), "failed": failed}) + "\n")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
