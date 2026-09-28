"""Exercise offline tool installation and real secret scanning with synthetic fixtures."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.metadata
import io
import json
import logging
import os
import platform
import shutil
import sys
import tarfile
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)


class ProcessCheckFailed(ValueError):
    """Retain sanitized expected and observed results for failed process assertions."""

    def __init__(self, evidence: dict[str, Any]) -> None:
        super().__init__("Process assertion failed")
        self.evidence = evidence


def digest(path: Path) -> str:
    """Return the SHA-256 of one fixture or implementation file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(condition: bool, message: str) -> None:
    """Fail an E2E assertion without including source data in diagnostics."""
    if not condition:
        raise ValueError(message)


def run(command: list[str], cwd: Path, *, expected: int = 0, forbidden: str | None = None) -> dict[str, Any]:
    """Run an actual local process and retain only sanitized assertion metadata."""
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "BC_", "GITLEAKS_"))}
    # PYTHONPATH lets module entry points (python -m scripts.quality...) resolve from any working directory, including hook runs.
    environment.update(
        AWS_EC2_METADATA_DISABLED="true", PYTHONDONTWRITEBYTECODE="1", PRE_COMMIT_HOME=str(ROOT / ".tools" / "pre_commit_cache"), PYTHONPATH=str(ROOT)
    )
    result = run_command(command[0], command[1:], cwd=cwd, env=environment, timeout=120)
    evidence = {
        "expected_exit": expected,
        "observed_exit": result.returncode,
        "passed": result.returncode == expected,
        "stdout_sha256": hashlib.sha256(result.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(result.stderr.encode()).hexdigest(),
    }
    if forbidden is not None:
        check(forbidden not in result.stdout + result.stderr, "Process disclosed synthetic sentinel")
        evidence["sentinel_absent_from_diagnostics"] = True
    if result.returncode != expected:
        raise ProcessCheckFailed(evidence)
    return evidence


def installation_cases(workspace: Path, cases: list[dict[str, Any]]) -> None:
    """Verify the installer CLI using hash-pinned local archives, without HTTP."""
    archives = workspace / "archives"
    archives.mkdir()
    selected_platform = f"{platform.system().lower()}_{'amd64' if platform.machine() in {'x86_64', 'AMD64'} else platform.machine()}"
    tools: dict[str, Any] = {}
    for name, kind in [("gitleaks", "tar.gz"), ("tflint", "zip"), ("trivy", "tar.gz")]:
        path = archives / f"{name}.{kind}"
        content = b"synthetic executable fixture; never executed\n"
        if kind == "zip":
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr(name, content)
        else:
            with tarfile.open(path, "w:gz") as archive:
                member = tarfile.TarInfo(name)
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content))
        tools[name] = {
            "version": "1.2.3",
            "releases": {
                selected_platform: {
                    "url": f"https://github.com/example_org/example_tool/releases/download/v1.2.3/{path.name}",
                    "sha256": digest(path),
                    "format": kind,
                }
            },
        }
    manifest = workspace / "manifest.json"
    manifest.write_text(json.dumps({"schema_version": 1, "tools": tools}), encoding="utf-8")
    destination = workspace / "installed"
    command = [
        sys.executable,
        str(ROOT / "scripts/quality/install_tools.py"),
        "--manifest",
        str(manifest),
        "--destination",
        str(destination),
        "--archive-directory",
        str(archives),
    ]
    cases.append({"name": "install_from_verified_offline_archives", **run(command, ROOT)})
    before = {str(p.relative_to(destination)): (digest(p), p.stat().st_mtime_ns) for p in destination.rglob("*") if p.is_file()}
    cases.append({"name": "replay_install", **run(command, ROOT)})
    after = {str(p.relative_to(destination)): (digest(p), p.stat().st_mtime_ns) for p in destination.rglob("*") if p.is_file()}
    check(before == after, "Repeated install changed files")
    corrupted = copy.deepcopy(tools)
    corrupted["gitleaks"]["releases"][selected_platform]["sha256"] = "0" * 64
    manifest.write_text(json.dumps({"schema_version": 1, "tools": corrupted}), encoding="utf-8")
    cases.append({"name": "reject_hash_mismatch", **run(command, ROOT, expected=1)})
    check(
        before == {str(p.relative_to(destination)): (digest(p), p.stat().st_mtime_ns) for p in destination.rglob("*") if p.is_file()},
        "Rejected input changed installed files",
    )
    manifest.write_text(json.dumps({"schema_version": 1, "tools": tools}), encoding="utf-8")
    (destination / "bin/gitleaks").write_bytes(b"corrupted synthetic executable")
    cases.append({"name": "reject_corrupt_existing_binary", **run(command, ROOT, expected=1)})

    # A matching hash must not make an archive's symbolic-link executable acceptable.
    malicious = archives / "gitleaks.tar.gz"
    with tarfile.open(malicious, "w:gz") as archive:
        member = tarfile.TarInfo("gitleaks")
        member.type = tarfile.SYMTYPE
        member.linkname = "../outside_fixture"
        archive.addfile(member)
    tools["gitleaks"]["releases"][selected_platform]["sha256"] = digest(malicious)
    manifest.write_text(json.dumps({"schema_version": 1, "tools": tools}), encoding="utf-8")
    rejected_destination = workspace / "rejected_install"
    unsafe_command = [str(rejected_destination) if argument == str(destination) else argument for argument in command]
    cases.append({"name": "reject_verified_archive_symlink", **run(unsafe_command, ROOT, expected=1)})
    check(not rejected_destination.exists(), "Rejected archive created an installation")


def secret_cases(workspace: Path, cases: list[dict[str, Any]]) -> None:
    """Exercise a real Gitleaks process and pre-commit hook against a temporary index."""
    git = shutil.which("git")
    check(git is not None, "Git is required")
    repository = workspace / "repository"
    repository.mkdir()
    run([str(git), "init", "--quiet", str(repository)], ROOT)
    source = repository / "example.txt"
    source.write_text("Generic non-secret fixture\n", encoding="utf-8")
    run([str(git), "add", "example.txt"], repository)
    scanner = [sys.executable, "-m", "scripts.quality.scan_secrets", "--repository", str(repository), "--staged"]
    cases.append({"name": "clean_staged_file", **run(scanner, ROOT)})
    sentinel = hashlib.sha256(b"public synthetic scanner fixture, not an issued credential").hexdigest()
    source.write_text(f'api_key = "{sentinel}"\n', encoding="utf-8")
    run([str(git), "add", "example.txt"], repository)
    source.write_text("Working copy is clean; index still has the synthetic sentinel\n", encoding="utf-8")
    cases.append({"name": "staged_secret_cannot_hide_behind_clean_worktree", **run(scanner, ROOT, expected=1, forbidden=sentinel)})
    # Keep the actual hook definition; only relocate its executable paths into this disposable repository.
    hook_config = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    hook_config = hook_config.replace(".venv/bin/python -m scripts.quality.scan_secrets", f"{sys.executable} -m scripts.quality.scan_secrets")
    fixture_config = repository / ".pre-commit-config.yaml"
    fixture_config.write_text(hook_config, encoding="utf-8")
    hook = [sys.executable, "-m", "pre_commit", "run", "gitleaks", "--all-files", "--config", str(fixture_config)]
    cases.append({"name": "real_pre_commit_hook_rejects_synthetic_secret", **run(hook, repository, expected=1, forbidden=sentinel)})
    run([str(git), "add", "example.txt"], repository)
    cases.append({"name": "clean_index_replay", **run(scanner, ROOT)})
    cases.append({"name": "real_pre_commit_hook_accepts_clean_index", **run(hook, repository)})


def terraform_cases(workspace: Path, cases: list[dict[str, Any]]) -> None:
    """Confirm the real static-analysis entry point rejects insecure Terraform."""
    fixture = workspace / "terraform"
    fixture.mkdir()
    (fixture / "main.tf").write_text('resource "aws_s3_bucket" "example" {\n  bucket = "example-synthetic-bucket"\n}\n', encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "scripts.quality.static_checks",
        "--directory",
        str(fixture),
        "--output-directory",
        str(workspace / "static_evidence"),
    ]
    cases.append({"name": "reject_insecure_terraform", **run(command, ROOT, expected=1)})
    report = json.loads((workspace / "static_evidence/report.json").read_text(encoding="utf-8"))
    check(report["checkov"]["failed_checks"] > 0, "Checkov did not identify missing S3 protections")
    check(report["tflint"]["returncode"] != 0, "TFLint did not identify missing provider/version declarations")
    check(report["trivy"]["failed_checks"] > 0 and report["trivy"]["returncode"] != 0, "trivy did not identify missing S3 protections")

    # Failure modes: hidden exceptions, global suppression or suppression of an unapproved control.
    scoped = workspace / "scoped_terraform"
    scoped.mkdir()
    deferred = {"CKV2_AWS_62", "CKV_AWS_144", "CKV_AWS_145", "CKV_AWS_18"}
    comments = "\n".join(f"  #checkov:skip={identifier}:Synthetic staged-development exception; review before production." for identifier in sorted(deferred))
    trivy_deferred = {"AWS-0089", "AWS-0132"}
    trivy_comments = "# Synthetic staged-development exception; review before production.\n" + "".join(
        f"#trivy:ignore:{item}\n" for item in sorted(trivy_deferred)
    )
    (scoped / "main.tf").write_text(
        trivy_comments + 'resource "aws_s3_bucket" "scoped" {\n' + comments + '\n  bucket = "example-scoped-bucket"\n}\n'
        'resource "aws_s3_bucket" "unscoped" {\n  bucket = "example-unscoped-bucket"\n}\n',
        encoding="utf-8",
    )
    evidence = workspace / "scoped_evidence"
    scoped_command = [sys.executable, "-m", "scripts.quality.static_checks", "--directory", str(scoped), "--output-directory", str(evidence)]
    outcome = run(scoped_command, ROOT, expected=1)
    scoped_report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))["checkov"]
    exceptions = scoped_report.get("exceptions", [])
    check(scoped_report.get("skipped_checks") == 4, "Exception count was not retained")
    check({item["check_id"] for item in exceptions} == deferred, "Approved exception identifiers were not retained")
    check(all(item["resource"] == "aws_s3_bucket.scoped" and item.get("reason") for item in exceptions), "Exception scope or reason was lost")
    findings = {(item["resource"], item["check_id"]) for item in scoped_report["findings"]}
    check(all(("aws_s3_bucket.unscoped", identifier) in findings for identifier in deferred), "An exception suppressed another resource")
    check(("aws_s3_bucket.scoped", "CKV_AWS_21") in findings, "Unapproved versioning control was suppressed")
    trivy_report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))["trivy"]
    trivy_exceptions = trivy_report["exceptions"]
    check({item["check_id"] for item in trivy_exceptions} == trivy_deferred, "Approved trivy exception identifiers were not retained")
    check(all(item["resource"] == "aws_s3_bucket.scoped" and item["reason"] for item in trivy_exceptions), "trivy exception scope or reason was lost")
    trivy_findings = {(item["resource"], item["check_id"]) for item in trivy_report["findings"]}
    check(all(("aws_s3_bucket.unscoped", identifier) in trivy_findings for identifier in trivy_deferred), "A trivy exception suppressed another resource")
    check(not any(("aws_s3_bucket.scoped", identifier) in trivy_findings for identifier in trivy_deferred), "A scoped trivy exception was not applied")
    check(("aws_s3_bucket.scoped", "AWS-0090") in trivy_findings, "Unapproved trivy versioning control was suppressed")
    cases.append({"name": "resource_scoped_exceptions_remain_visible_and_fail_closed", **outcome})


def main() -> None:
    """Run synthetic E2E cases and publish a repeatable local artifact on pass or fail."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    output = args.output_directory.resolve()
    output.mkdir(parents=True, exist_ok=False)
    os.umask(0o077)
    cases: list[dict[str, Any]] = []
    report: dict[str, Any] = {
        "status": "running",
        "started_at_utc": datetime.now(UTC).isoformat(),
        "cases": cases,
        "fixtures": "Generic synthetic data only",
        "aws_calls": 0,
        "tested_boundary": "Real installer, scanner and pre-commit subprocesses; offline synthetic archives",
        "untested": ["Publisher or AWS ingestion", "Network interruption during binary download"],
        "reproduce": [".venv/bin/python", "-m", "scripts.quality.run_e2e", "--output-directory", "<new_directory>"],
        "prerequisites": "Install pinned requirements and project-local quality tools before running.",
        "runtime": {"python": platform.python_version(), "platform": sys.platform},
        "dependencies": {name: importlib.metadata.version(name) for name in ("pre-commit", "checkov", "ruff", "mypy")},
    }
    try:
        with tempfile.TemporaryDirectory(prefix="quality_e2e_") as temporary:
            workspace = Path(temporary)
            installation_cases(workspace, cases)
            secret_cases(workspace, cases)
            terraform_cases(workspace, cases)
        report["status"] = "passed"
    except (OSError, ValueError, TimeoutExpired) as error:
        if isinstance(error, ProcessCheckFailed):
            cases.append({"name": "failed_process", **error.evidence})
        report.update(
            status="failed", failure_type=type(error).__name__, failure=str(error) if isinstance(error, ValueError) else "Process or filesystem failure"
        )
    finally:
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        report["cleanup"] = "Temporary synthetic workspace removed; local evidence retained. No cloud resources created."
        report["source_sha256"] = {p.name: digest(p) for p in [*sorted(Path(__file__).parent.glob("*.py")), ROOT / "scripts/process.py"]}
        report["requirements_sha256"] = digest(ROOT / "requirements.txt")
        report["configuration_sha256"] = {name: digest(ROOT / name) for name in ("config/quality_tools.json", ".pre-commit-config.yaml")}
        report["native_tool_sha256"] = {
            name: digest(ROOT / ".tools/bin" / name) for name in ("gitleaks", "tflint", "trivy") if (ROOT / ".tools/bin" / name).is_file()
        }
        (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (output / "evidence_manifest.json").write_text(json.dumps({"report.json": digest(output / "report.json")}) + "\n", encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    LOGGER.info("event=quality_e2e status=%s cases=%s", report["status"], len(cases))
    raise SystemExit(0 if report["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
