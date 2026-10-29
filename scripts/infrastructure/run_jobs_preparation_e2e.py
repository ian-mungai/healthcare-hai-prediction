"""Exercise reviewed jobs inputs and wrapper dispatch with synthetic files and inert executables."""

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
    "scripts/__init__.py",
    "scripts/process.py",
    "scripts/infrastructure/__init__.py",
    "scripts/infrastructure/terraform.sh",
    "scripts/infrastructure/render_project_config.py",
    "scripts/infrastructure/render_ecs_target.py",
    "scripts/infrastructure/render_jobs_config.py",
    "data_contracts/infrastructure/jobs_preparation.schema.json",
)


def _environment() -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "TF_"))}


def _preparation(root: str = "jobs") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "root": root,
        "variables": {"jobs_enabled": True, "enabled_definitions": ["analytics_read"], "cloud_limits": {"max_tasks": 3, "max_vcpu": 16, "max_memory_gib": 120}}
        if root == "jobs"
        else {"probe_enabled": False},
    }


def _renderer_cases(folder: Path) -> dict[str, bool]:
    workspace = folder / "renderer"
    for name in SOURCES:
        source = ROOT / name
        if source.exists():
            target = workspace / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    environment_file = workspace / "synthetic_environment.txt"
    environment_file.write_text(
        "AWS_REGION=us-west-2\nAWS_PROFILE=example_project_dev\nAWS_ACCOUNT_ID=111111111111\n"
        "PROJECT_NAME=example_project\nENVIRONMENT=dev\nS3_BUCKET=example-project-data-dev\n"
    )
    prep = workspace / "reviewed_preparation.json"
    values = {
        "aws_region": "us-west-2",
        "aws_profile": "example_project_dev",
        "expected_account_id": "111111111111",
        "project_name": "example_project",
        "environment": "dev",
        "data_bucket_name": "example-project-data-dev",
    }
    output = workspace / "infra/jobs/deployment.auto.tfvars.json"

    def invoke(document: dict[str, Any], *extra: str, digest: str | None = None) -> Any:
        prep.write_text(json.dumps(document, sort_keys=True) + "\n")
        sha256 = hashlib.sha256(prep.read_bytes()).hexdigest() if digest is None else digest
        return run_command(
            sys.executable,
            [
                "-m",
                "scripts.infrastructure.render_jobs_config",
                "--env-file",
                str(environment_file),
                "--preparation-inputs",
                str(prep),
                "--preparation-sha256",
                sha256,
                *extra,
            ],
            cwd=workspace,
            env=_environment(),
            timeout=15,
        )

    document = _preparation()
    result = invoke(document)
    expected = values | document["variables"]
    checks = {"reviewed_preparation_is_rendered": result.returncode == 0 and output.exists() and json.loads(output.read_text()) == expected}
    if not checks["reviewed_preparation_is_rendered"]:
        (folder / "renderer_failure.txt").write_text(result.stdout + result.stderr)
        return checks
    original = output.read_bytes()
    checks["reviewed_preparation_is_private"] = stat.S_IMODE(output.stat().st_mode) == 0o600
    checks["reviewed_preparation_repeats"] = invoke(document).returncode == 0 and output.read_bytes() == original
    checks["reviewed_preparation_check"] = invoke(document, "--check").returncode == 0
    unused_writer = document | {
        "variables": document["variables"] | {"job_role_arns": {"try_writer": "arn:aws:iam::111111111111:role/example_project_job_try_writer_dev"}}
    }
    result = invoke(unused_writer)
    checks["unused_writer_input_is_refused"] = result.returncode == 2 and "preparation contract" in result.stderr and output.read_bytes() == original
    checks["reader_inputs_reset_after_role_probe"] = invoke(document).returncode == 0 and output.read_bytes() == original
    unordered = document | {"variables": document["variables"] | {"enabled_definitions": ["spark_read", "analytics_read"]}}
    result = invoke(unordered)
    checks["selection_has_stable_set_order"] = result.returncode == 0 and json.loads(output.read_text())["enabled_definitions"] == [
        "analytics_read",
        "spark_read",
    ]
    checks["selection_reset_restores_bytes"] = invoke(document).returncode == 0 and output.read_bytes() == original
    for name, altered, digest, cause in (
        ("changed_reviewed_bytes", document, "0" * 64, "preparation SHA-256"),
        ("base_identity_override", document | {"variables": document["variables"] | {"aws_profile": "example_admin"}}, None, "preparation contract"),
        ("unknown_preparation_input", document | {"variables": document["variables"] | {"unreviewed_secret": "synthetic"}}, None, "preparation contract"),
        ("wrong_preparation_root", document | {"root": "identity_probe"}, None, "preparation contract"),
    ):
        result = invoke(altered, digest=digest)
        checks[name] = result.returncode == 2 and cause in result.stderr and output.read_bytes() == original
    prep.write_text('{"schema_version":1,"schema_version":1,"root":"jobs","variables":{}}')
    result = run_command(
        sys.executable,
        [
            "-m",
            "scripts.infrastructure.render_jobs_config",
            "--env-file",
            str(environment_file),
            "--preparation-inputs",
            str(prep),
            "--preparation-sha256",
            hashlib.sha256(prep.read_bytes()).hexdigest(),
        ],
        cwd=workspace,
        env=_environment(),
        timeout=15,
    )
    checks["duplicate_preparation_field"] = result.returncode == 2 and "duplicate JSON field" in result.stderr and output.read_bytes() == original
    checks["probe_inputs_are_separate"] = invoke(_preparation("identity_probe"), "--identity-probe").returncode == 0
    probe_output = workspace / "infra/jobs/identity_probe/deployment.auto.tfvars.json"
    checks["probe_omits_application_inputs"] = probe_output.exists() and json.loads(probe_output.read_text()) == {
        key: value for key, value in values.items() if key != "data_bucket_name"
    } | {"probe_enabled": False}

    binary = workspace / "bin"
    binary.mkdir()
    executable = binary / "terraform"
    executable.write_text(f"#!{sys.executable}\nfrom pathlib import Path\nimport sys\nsys.stdout.write((Path(__file__).parent / 'plan.json').read_text())\n")
    executable.chmod(0o700)
    env = _environment() | {"PATH": str(binary) + os.pathsep + os.environ.get("PATH", "")}
    declared: dict[str, Any] = {name: {} for name in expected} | {"optional_setting": {"default": False}}
    plan: dict[str, Any] = {
        "complete": True,
        "errored": False,
        "variables": {key: {"value": value} for key, value in (expected | {"optional_setting": False}).items()},
        "configuration": {"root_module": {"variables": declared}},
    }
    prep.write_text(json.dumps(document, sort_keys=True) + "\n")
    args = [
        "-m",
        "scripts.infrastructure.render_jobs_config",
        "--env-file",
        str(environment_file),
        "--preparation-inputs",
        str(prep),
        "--preparation-sha256",
        hashlib.sha256(prep.read_bytes()).hexdigest(),
        "--check-plan",
        str(workspace / "reviewed.tfplan"),
    ]
    for name, altered, accepted in (
        ("reviewed_preparation_plan", plan, True),
        ("changed_preparation_plan", copy.deepcopy(plan), False),
        ("unreviewed_default_plan", copy.deepcopy(plan), False),
    ):
        if name == "changed_preparation_plan":
            altered["variables"]["jobs_enabled"]["value"] = False
        if name == "unreviewed_default_plan":
            altered["variables"]["optional_setting"]["value"] = True
        (binary / "plan.json").write_text(json.dumps(altered))
        result = run_command(sys.executable, args, cwd=workspace, env=env, timeout=15)
        checks[name] = result.returncode == (0 if accepted else 2) and (accepted or "saved jobs plan" in result.stderr)
    return checks


def _wrapper_result(workspace: Path, environment: dict[str, str], arguments: list[str]) -> tuple[Any, list[dict[str, Any]]]:
    calls_file = workspace / "calls.json"
    calls_file.write_text("")
    result = run_command("bash", arguments, cwd=workspace, env=environment, timeout=15)
    return result, [json.loads(line) for line in calls_file.read_text().splitlines()]


def _wrapper_cases(folder: Path) -> dict[str, bool]:
    workspace = (folder / "wrapper").resolve()
    script = workspace / "scripts/infrastructure/terraform.sh"
    script.parent.mkdir(parents=True)
    shutil.copy2(ROOT / "scripts/infrastructure/terraform.sh", script)
    binary = workspace / "bin"
    binary.mkdir()
    for name in ("python_capture", "terraform"):
        executable = binary / name
        executable.write_text(
            f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n"
            "with (Path(__file__).resolve().parents[1] / 'calls.json').open('a') as handle:\n"
            "    handle.write(json.dumps({'program': Path(__file__).name, 'args': sys.argv[1:]}) + '\\n')\n"
            "sys.exit(int(os.environ.get('SYNTHETIC_RENDER_EXIT', '0')) if Path(__file__).name == 'python_capture' else 0)\n"
        )
        executable.chmod(0o700)
    env = _environment() | {"PATH": str(binary) + os.pathsep + os.environ.get("PATH", ""), "PYTHON_BIN": str(binary / "python_capture")}
    sha256 = "a" * 64
    reviewed = ["--preparation-inputs", "reviewed_inputs.json", "--preparation-sha256", sha256]

    checks = {}
    cases = (
        (
            "jobs",
            "plan",
            "jobs",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "plan",
                ],
            ),
        ),
        (
            "jobs",
            "apply",
            "jobs",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "apply",
                    "reviewed.tfplan",
                ],
            ),
        ),
        (
            "jobs",
            "validate",
            "jobs",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "validate",
                ],
            ),
        ),
        (
            "probe",
            "plan",
            "jobs/identity_probe",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--identity-probe",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "plan",
                ],
            ),
        ),
        (
            "probe",
            "apply",
            "jobs/identity_probe",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--identity-probe",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "apply",
                    "reviewed.tfplan",
                ],
            ),
        ),
        (
            "probe",
            "validate",
            "jobs/identity_probe",
            _wrapper_result(
                workspace,
                env,
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--identity-probe",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "validate",
                ],
            ),
        ),
    )
    (folder / "wrapper_observed.json").write_text(
        json.dumps(
            [
                {"case": name + "_" + action, "exit_code": result.returncode, "stderr": result.stderr, "calls": calls}
                for name, action, directory, (result, calls) in cases
            ],
            indent=2,
        )
    )
    for name, action, directory, (result, calls) in cases:
        renderer_args = ["-m", "scripts.infrastructure.render_jobs_config"]
        if name == "probe":
            renderer_args += ["--identity-probe"]
        renderer_args += reviewed
        if action == "apply":
            renderer_args += ["--check-plan", f"{workspace}/infra/{directory}/reviewed.tfplan"]
        if action != "validate":
            renderer_args += ["--verify-identity"]
        checks[f"{name}_{action}_passes_reviewed_inputs"] = (
            result.returncode == 0
            and len(calls) == 2
            and calls[0]["args"] == renderer_args
            and calls[1]["args"][0] == f"-chdir={workspace}/infra/{directory}"
            and calls[1]["args"][1] == action
        )
    refusals = (
        (
            "preparation_requires_digest",
            "Supply both",
            _wrapper_result(workspace, env | {}, ["scripts/infrastructure/terraform.sh", "--jobs", "--preparation-inputs", "x.json", "plan"]),
        ),
        (
            "digest_requires_preparation",
            "Supply both",
            _wrapper_result(
                workspace,
                env | {},
                [
                    "scripts/infrastructure/terraform.sh",
                    "--jobs",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "plan",
                ],
            ),
        ),
        (
            "preparation_is_jobs_only",
            "only with --jobs",
            _wrapper_result(
                workspace,
                env | {},
                [
                    "scripts/infrastructure/terraform.sh",
                    "--platform",
                    "--preparation-inputs",
                    "reviewed_inputs.json",
                    "--preparation-sha256",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "plan",
                ],
            ),
        ),
        ("probe_is_jobs_only", "only with --jobs", _wrapper_result(workspace, env | {}, ["scripts/infrastructure/terraform.sh", "--identity-probe", "plan"])),
        (
            "direct_variables_stay_refused",
            "Set project inputs only",
            _wrapper_result(workspace, env | {}, ["scripts/infrastructure/terraform.sh", "--jobs", "plan", "-var=jobs_enabled=true"]),
        ),
        (
            "platform_environment_variables_are_refused",
            "Unset TF_VAR_project_name",
            _wrapper_result(workspace, env | {"TF_VAR_project_name": "other_project"}, ["scripts/infrastructure/terraform.sh", "--platform", "plan"]),
        ),
        (
            "environment_variables_are_refused",
            "Unset TF_VAR_jobs_enabled",
            _wrapper_result(workspace, env | {"TF_VAR_jobs_enabled": "true"}, ["scripts/infrastructure/terraform.sh", "--jobs", "plan"]),
        ),
    )
    for name, cause, (result, calls) in refusals:
        checks[name] = result.returncode == 2 and cause in result.stderr and not calls
    result, calls = _wrapper_result(workspace, env | {"SYNTHETIC_RENDER_EXIT": "9"}, ["scripts/infrastructure/terraform.sh", "--jobs", "plan"])
    checks["renderer_failure_stops_dispatch"] = result.returncode == 9 and len(calls) == 1
    return checks


def main() -> int:
    """Run real commands against synthetic inputs and save their bounded verification result."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES if (ROOT / name).exists()}
    args.output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="jobs_preparation_") as temporary:
        folder = Path(temporary)
        checks = _wrapper_cases(folder) | _renderer_cases(folder)
        for name in ("renderer_failure.txt", "wrapper_observed.json"):
            record = folder / name
            if record.exists():
                shutil.copy2(record, args.output / name)
    checks["production_sources_unchanged"] = all(hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest for name, digest in before.items())
    report = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "status": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "source_sha256": before,
        "limits": ["Synthetic command boundary only. No live Terraform, AWS, deployment inputs or data."],
        "cleanup": "done",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(f"Jobs preparation checks: {sum(checks.values())}/{len(checks)} passed.\n")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
