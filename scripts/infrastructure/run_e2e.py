"""Exercise the local configuration lifecycle through real processes; never contact AWS."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = "example_project_profile"
ADMIN = "example_administrator"
SECRET = "example_e2e_secret_do_not_print"
SOURCE_NAMES = ("render_project_config.py", "render_project_config.sh", "terraform.sh")
OUTPUT_NAMES = ("infra/deployment.auto.tfvars.json", "infra/iam/deployment.auto.tfvars.json")
FIXTURE = {
    "AWS_ACCOUNT_ID": "111111111111", "AWS_PROFILE": PROFILE, "AWS_REGION": "us-west-2",
    "PROJECT_NAME": "example_project", "S3_BUCKET": "example-project-e2e-bucket", "MODEL_API_KEY": SECRET,
}

def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def write_json(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
    path.chmod(0o600)

class CheckFailed(RuntimeError):
    pass

class Evidence:
    def __init__(self, output: Path, temporary: Path):
        self.output = output
        self.temporary = temporary
        self.started = time.perf_counter()
        self.redactions = {
            str(output): "<EVIDENCE>", str(temporary): "<TEMP>", str(REPO_ROOT): "<REPO>",
            sys.executable: "<PYTHON>", str(Path.home()): "<HOME>", SECRET: "<REDACTED_SECRET>",
        }
        self.report: dict[str, Any] = {
            "schema_version": 1, "scope": "local_configuration_lifecycle", "status": "running",
            "live_aws_tested": False, "subprocess_responses_mocked": False,
            "started_at_utc": datetime.now(UTC).isoformat(),
            "runtime": {"python": platform.python_version(), "system": platform.system(), "machine": platform.machine()},
            "repeat_command": [".venv/bin/python", "scripts/infrastructure/run_e2e.py", "--output-directory", "<NEW_EVIDENCE_DIRECTORY>"],
            "source_hashes": {}, "steps": [],
        }
        self.environment = {
            "HOME": str(temporary / "home"), "TMPDIR": str(temporary), "PATH": str(temporary / "tools"),
            "PYTHON_BIN": sys.executable, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1",
            "PYTHONUTF8": "1", "LANG": "C", "LC_ALL": "C", "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_CONFIG_FILE": str(temporary / "absent_aws_config"), "AWS_SHARED_CREDENTIALS_FILE": str(temporary / "absent_aws_credentials"),
            "AWS_PROFILE": "example_stale_profile", "AWS_ACCOUNT_ID": "222222222222", "AWS_REGION": "us-east-1",
        }

    def redact(self, value: str) -> str:
        for raw, replacement in sorted(self.redactions.items(), key=lambda item: len(item[0]), reverse=True):
            value = value.replace(raw, replacement)
        return value

    def run(self, name: str, command: list[str], workspace: Path, extra_environment: dict[str, str] | None = None) -> tuple[dict, Any]:
        step: dict[str, Any] = {
            "name": name, "command": [self.redact(item) for item in command], "cwd": self.redact(str(workspace)),
            "started_at_utc": datetime.now(UTC).isoformat(), "assertions": [],
        }
        self.report["steps"].append(step)
        started = time.perf_counter()
        try:
            result = subprocess.run(
                command, cwd=workspace, env={**self.environment, **(extra_environment or {})},
                capture_output=True, text=True, timeout=30, check=False,
            )
            step.update(returncode=result.returncode, stdout=self.redact(result.stdout), stderr=self.redact(result.stderr))
            self.check(step, "diagnostics_do_not_disclose_secret", SECRET not in result.stdout + result.stderr)
            return step, result
        except subprocess.TimeoutExpired as error:
            step.update(returncode=None, stdout=self.redact(as_text(error.stdout)), stderr=self.redact(as_text(error.stderr)), timed_out=True)
            raise CheckFailed(f"{name}: child process exceeded 30 seconds") from None
        finally:
            step["duration_seconds"] = round(time.perf_counter() - started, 6)

    @staticmethod
    def check(step: dict, name: str, condition: bool) -> None:
        step["assertions"].append({"name": name, "passed": condition})
        if not condition:
            raise CheckFailed(f"{step['name']}: {name}")

    def capture_outputs(self, step: dict, workspace: Path, region: str) -> dict[str, str]:
        common = {
            "expected_account_id": "111111111111", "aws_region": region,
            "project_name": "example_project", "data_bucket_name": "example-project-e2e-bucket",
        }
        expected = ({**common, "aws_profile": PROFILE}, {**common, "deployment_user_name": PROFILE})
        hashes = {}
        step["outputs"] = []
        for name, values in zip(OUTPUT_NAMES, expected, strict=True):
            path = workspace / name
            self.check(step, f"{name}: exists", path.is_file())
            content = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            self.check(step, f"{name}: owner_only_permissions", mode == 0o600)
            self.check(step, f"{name}: exact_json", json.loads(content) == values)
            self.check(step, f"{name}: no_secret_or_administrator", SECRET.encode() not in content and ADMIN.encode() not in content)
            artifact = self.output / step["name"] / name
            artifact.parent.mkdir(parents=True, exist_ok=True)
            with artifact.open("xb") as handle:
                handle.write(content)
            artifact.chmod(0o600)
            hashes[name] = sha256(content)
            step["outputs"].append({"path": artifact.relative_to(self.output).as_posix(), "sha256": hashes[name], "mode": oct(mode)})
        self.check(step, "no_temporary_configuration_files", not list(workspace.rglob(".config_*")))
        return hashes

    def finish(self) -> None:
        self.report["finished_at_utc"] = datetime.now(UTC).isoformat()
        self.report["duration_seconds"] = round(time.perf_counter() - self.started, 6)
        self.report["process_count"] = len(self.report["steps"])
        self.report["assertion_count"] = sum(len(step["assertions"]) for step in self.report["steps"])
        write_json(self.output / "report.json", self.report)
        hashes = {path.relative_to(self.output).as_posix(): sha256(path.read_bytes()) for path in sorted(self.output.rglob("*")) if path.is_file()}
        write_json(self.output / "evidence_manifest.json", {"algorithm": "sha256", "files": hashes})

def as_text(value: str | bytes | None) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value or ""

def fixture_sources(evidence: Evidence) -> dict[str, bytes]:
    sources = {}
    for name in SOURCE_NAMES:
        original = (REPO_ROOT / "scripts" / "infrastructure" / name).read_bytes()
        copied = original
        if name == "render_project_config.py":
            source = original.decode("utf-8")
            tree = ast.parse(source)
            assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                           and any(isinstance(target, ast.Name) and target.id == "PROJECT_PROFILE" for target in node.targets)]
            if len(assignments) != 1 or not isinstance(assignments[0].value, ast.Constant):
                raise CheckFailed("Expected one literal PROJECT_PROFILE assignment in the renderer")
            value = assignments[0].value
            if value.lineno != value.end_lineno or not isinstance(value.value, str):
                raise CheckFailed("Expected a single-line string PROJECT_PROFILE")
            lines = source.splitlines(keepends=True)
            line = lines[value.lineno - 1]
            lines[value.lineno - 1] = line[:value.col_offset] + repr(PROFILE) + line[value.end_col_offset:]
            copied = "".join(lines).encode("utf-8")
            value.value = PROFILE
            if ast.dump(tree, include_attributes=False) != ast.dump(ast.parse(copied), include_attributes=False):
                raise CheckFailed("Fixture injection changed executable logic beyond PROJECT_PROFILE")
        sources[name] = copied
        evidence.report["source_hashes"][name] = {"original_sha256": sha256(original), "fixture_sha256": sha256(copied)}
    evidence.report["source_hashes"]["run_e2e.py"] = {"original_sha256": sha256(Path(__file__).read_bytes())}
    evidence.report["fixture_injection"] = {"assignment": "PROJECT_PROFILE", "value": PROFILE, "only_literal_changed": True}
    return sources

def write_environment(workspace: Path, values: dict[str, str]) -> None:
    path = workspace / ".env"
    path.write_text("".join(f'export {key}="{value}" # synthetic fixture\n' for key, value in values.items()), encoding="utf-8")
    path.chmod(0o600)

def copied_workspace(temporary: Path, name: str, sources: dict[str, bytes], values: dict[str, str]) -> Path:
    workspace = temporary / name
    scripts = workspace / "scripts" / "infrastructure"
    scripts.mkdir(parents=True)
    for filename, content in sources.items():
        (scripts / filename).write_bytes(content)
    write_environment(workspace, values)
    return workspace

def run_suite(evidence: Evidence) -> None:
    sources = fixture_sources(evidence)
    (evidence.temporary / "home").mkdir()
    tools = evidence.temporary / "tools"
    tools.mkdir()
    dirname = shutil.which("dirname", path="/usr/bin:/bin")
    bash = shutil.which("bash", path="/bin:/usr/bin")
    if not dirname or not bash:
        raise CheckFailed("This check requires the real bash and dirname utilities")
    (tools / "dirname").symlink_to(dirname)
    workspace = copied_workspace(evidence.temporary, "lifecycle", sources, FIXTURE)
    renderer = [sys.executable, "scripts/infrastructure/render_project_config.py"]
    shell_renderer = [bash, "scripts/infrastructure/render_project_config.sh", "--admin-profile", ADMIN]
    step, result = evidence.run("render_initial", shell_renderer, workspace)
    evidence.check(step, "render_succeeded", result.returncode == 0 and not result.stderr)
    initial_hashes = evidence.capture_outputs(step, workspace, "us-west-2")
    step, result = evidence.run("check_initial", [*renderer, "--check"], workspace)
    evidence.check(step, "matching_inputs_accepted", result.returncode == 0 and "Verified .env-derived" in result.stdout)
    write_environment(workspace, {**FIXTURE, "AWS_REGION": "us-east-2"})
    for stack in ("storage", "iam"):
        step, result = evidence.run(f"reject_stale_{stack}", [*renderer, "--stack", stack, "--check"], workspace)
        evidence.check(step, "stale_inputs_rejected", result.returncode == 2 and "Generated configuration is stale" in result.stderr and not result.stdout)
        evidence.check(step, "outputs_unchanged", {name: sha256((workspace / name).read_bytes()) for name in OUTPUT_NAMES} == initial_hashes)
    step, result = evidence.run("render_refreshed", shell_renderer, workspace)
    evidence.check(step, "rerender_succeeded", result.returncode == 0 and not result.stderr)
    evidence.capture_outputs(step, workspace, "us-east-2")
    step, result = evidence.run("check_refreshed", [*renderer, "--check"], workspace)
    evidence.check(step, "refreshed_inputs_accepted", result.returncode == 0 and "Verified .env-derived" in result.stdout)

    invalid_values = {
        "AWS_ACCOUNT_ID": "invalid_private_account", "AWS_REGION": "invalid_private_region",
        "PROJECT_NAME": "invalid-private-project", "S3_BUCKET": "invalid_private_bucket*",
    }
    for key, value in invalid_values.items():
        evidence.redactions[value] = "<REDACTED_INVALID_VALUE>"
        isolated = copied_workspace(evidence.temporary, f"invalid_{key.lower()}", sources, {**FIXTURE, key: value})
        step, result = evidence.run(f"reject_invalid_{key.lower()}", renderer, isolated)
        evidence.check(step, "invalid_input_rejected", result.returncode == 2 and f"Set a valid {key}" in result.stderr)
        evidence.check(step, "offending_value_not_echoed", value not in result.stdout + result.stderr)
        evidence.check(step, "no_output", not result.stdout and not (isolated / "infra").exists())

    isolated = copied_workspace(evidence.temporary, "missing_env", sources, FIXTURE)
    step, result = evidence.run("reject_missing_env", [*renderer, "--env-file", "missing.env"], isolated)
    evidence.check(step, "missing_env_rejected", result.returncode == 2 and "selected .env file does not exist" in result.stderr)
    evidence.check(step, "no_output", not result.stdout and not (isolated / "infra").exists())
    isolated = copied_workspace(evidence.temporary, "shell_expansion", sources, {**FIXTURE, "AWS_ACCOUNT_ID": "${PRIVATE_ACCOUNT}"})
    step, result = evidence.run("reject_shell_expansion", renderer, isolated, {"PRIVATE_ACCOUNT": "111111111111"})
    evidence.check(step, "shell_value_not_expanded", result.returncode == 2 and "Set a valid AWS_ACCOUNT_ID" in result.stderr)
    evidence.check(step, "no_output_or_value_disclosure", not result.stdout and "${PRIVATE_ACCOUNT}" not in result.stderr and not (isolated / "infra").exists())

    isolated = copied_workspace(evidence.temporary, "wrapper_guards", sources, FIXTURE)
    guards: list[tuple[list[str], dict[str, str], str]] = [
        (["--iam", "plan"], {}, "IAM updates require --admin-profile."),
        (["--admin-profile", ADMIN, "plan"], {}, "Administrator profiles are allowed only with --iam."),
        (["apply"], {}, "Apply requires one reviewed saved-plan path."),
        (["plan", "-var=aws_profile=example_unapproved"], {}, "Set project inputs only in .env."),
        (["plan", "-chdir=example_other"], {}, "Set project inputs only in .env."),
    ]
    for name, arguments in (
        ("TF_CLI_ARGS", ["plan"]), ("TF_CLI_ARGS_plan", ["plan"]), ("TF_CLI_ARGS_apply", ["apply", "example.tfplan"]),
        ("TF_CLI_ARGS_show", ["apply", "example.tfplan"]), ("TF_CLI_ARGS_validate", ["validate"]), ("TF_CLI_ARGS_future", ["validate"]),
    ):
        guards.append((arguments, {name: "-var=project_name=example_unapproved"}, f"Unset {name};"))
    for index, (arguments, exports, message) in enumerate(guards, start=1):
        step, result = evidence.run(
            f"wrapper_guard_{index:02d}", [bash, "scripts/infrastructure/terraform.sh", *arguments], isolated,
            {**exports, "PYTHON_BIN": str(isolated / "must_not_run")},
        )
        step["synthetic_environment_overrides"] = {key: evidence.redact(value) for key, value in exports.items()}
        evidence.check(step, "guard_rejected_before_renderer", result.returncode == 2 and message in result.stderr and "must_not_run" not in result.stderr)
        evidence.check(step, "no_output", not result.stdout and not (isolated / "infra").exists())
        evidence.check(step, "no_injected_value_disclosure", "example_unapproved" not in result.stderr)

    before = {path.relative_to(evidence.output).as_posix(): sha256(path.read_bytes()) for path in evidence.output.rglob("*") if path.is_file()}
    step, result = evidence.run(
        "refuse_existing_evidence", [sys.executable, str(Path(__file__).resolve()), "--output-directory", str(evidence.output)], workspace,
    )
    evidence.check(step, "existing_directory_rejected", result.returncode == 2 and "Output directory already exists" in result.stderr)
    after = {path.relative_to(evidence.output).as_posix(): sha256(path.read_bytes()) for path in evidence.output.rglob("*") if path.is_file()}
    evidence.check(step, "existing_evidence_unchanged", before == after)

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True, help="New directory for redacted evidence; existing paths are refused.")
    arguments = parser.parse_args()
    output = arguments.output_directory.expanduser().absolute()
    try:
        output.mkdir(mode=0o700, parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("Output directory already exists; choose a new directory.")
    output.chmod(0o700)
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix="configuration_e2e_") as directory:
        evidence = Evidence(output, Path(directory))
        try:
            run_suite(evidence)
            evidence.report["status"] = "passed"
        except (CheckFailed, OSError, ValueError, TypeError) as error:
            evidence.report.update(status="failed", failure=evidence.redact(str(error)))
        finally:
            evidence.finish()
    print(f"Configuration E2E {evidence.report['status']}: {evidence.report['process_count']} real processes; evidence written to requested directory.")
    if evidence.report["status"] != "passed":
        raise SystemExit(1)

if __name__ == "__main__":
    main()
