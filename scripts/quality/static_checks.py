"""Run credential-free Terraform scanners (TFLint, Checkov, trivy config) on source-only copies and retain safe evidence."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import logging
import os
import re
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)
TRIVY_IGNORE = re.compile(r"^\s*#trivy:ignore:(?P<check>[A-Z]+-[0-9]+)\s*$")
RESOURCE = re.compile(r'^resource\s+"(?P<type>[\w-]+)"\s+"(?P<name>[\w-]+)"\s*\{')


def trivy_exceptions(source: Path) -> list[dict[str, str]]:
    """List every trivy ignore with the resource it precedes and the reason in the comments above it."""
    exceptions = []
    for path in sorted(source.glob("*.tf")):
        comments: list[str] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                comments.append(stripped)
                continue
            match = RESOURCE.match(stripped)
            if match:
                reason = " ".join(c.lstrip("# ").strip() for c in comments if not TRIVY_IGNORE.match(c))
                for comment in comments:
                    ignore = TRIVY_IGNORE.match(comment)
                    if ignore:
                        exceptions.append({"check_id": ignore["check"], "resource": f"{match['type']}.{match['name']}", "reason": reason})
            comments = []
    return exceptions


def scan(directory: Path, output: Path) -> bool:
    """Copy Terraform source only, run both scanners and save summaries without values."""
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    report: dict[str, Any] = {"started_at_utc": datetime.now(UTC).isoformat(), "status": "running", "aws_calls": 0, "source_sha256": {}}
    try:
        with tempfile.TemporaryDirectory(prefix="terraform_static_") as temporary:
            scratch = Path(temporary)
            source = scratch / "source"
            source.mkdir()
            for path in sorted(directory.glob("*.tf")):
                if path.is_symlink():
                    raise ValueError("Symlink source is not allowed")
                content = path.read_bytes()
                (source / path.name).write_bytes(content)
                report["source_sha256"][path.name] = hashlib.sha256(content).hexdigest()
            if not report["source_sha256"]:
                raise ValueError("No Terraform sources found")
            policies = directory / "policies"
            if policies.is_dir():
                (source / "policies").mkdir()
                for path in sorted(policies.glob("*.json")):
                    if path.is_symlink():
                        raise ValueError("Symlink policy is not allowed")
                    content = path.read_bytes()
                    (source / "policies" / path.name).write_bytes(content)
                    report["source_sha256"][f"policies/{path.name}"] = hashlib.sha256(content).hexdigest()
            (scratch / "tflint.hcl").write_text('plugin "terraform" {\n  enabled = true\n  preset = "recommended"\n}\n', encoding="utf-8")
            (scratch / "checkov.yaml").write_text("{}\n", encoding="utf-8")
            environment = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "BC_", "CKV_", "PRISMA_", "TFLINT_", "TF_VAR_"))}
            environment.update(
                HOME=str(scratch),
                AWS_EC2_METADATA_DISABLED="true",
                AWS_CONFIG_FILE=str(scratch / "absent"),
                AWS_SHARED_CREDENTIALS_FILE=str(scratch / "absent"),
            )
            trivy_receipt = ROOT / ".tools/receipts/trivy.json"
            report["trivy_version"] = json.loads(trivy_receipt.read_text(encoding="utf-8"))["version"] if trivy_receipt.is_file() else None
            commands = {
                # Embedded checks only: no check-bundle download, version notice or telemetry, so results are reproducible offline.
                "trivy": [
                    str(ROOT / ".tools/bin/trivy"),
                    "config",
                    "--quiet",
                    "--skip-check-update",
                    "--skip-version-check",
                    "--disable-telemetry",
                    "--cache-dir",
                    str(scratch / "trivy_cache"),
                    "--format",
                    "json",
                    "--exit-code",
                    "1",
                    str(source),
                ],
                "tflint": [str(ROOT / ".tools/bin/tflint"), f"--chdir={source}", f"--config={scratch / 'tflint.hcl'}", "--format=json"],
                "checkov": [
                    sys.executable,
                    "-m",
                    "checkov.main",
                    "--directory",
                    str(source),
                    "--framework",
                    "terraform",
                    "--config-file",
                    str(scratch / "checkov.yaml"),
                    "--skip-download",
                    "--skip-results-upload",
                    "--download-external-modules",
                    "false",
                    "--output",
                    "json",
                ],
            }
            for name, command in commands.items():
                result = run_command(command[0], command[1:], cwd=scratch, env=environment, timeout=180)
                parsed = json.loads(result.stdout)
                if not isinstance(parsed, dict):
                    raise ValueError("Unexpected scanner report")
                details: dict[str, Any]
                if name == "trivy":
                    misconfigurations = [item for result in parsed.get("Results") or [] for item in result.get("Misconfigurations") or []]
                    details = {
                        "findings": [
                            {
                                "check_id": item["ID"],
                                "severity": item["Severity"],
                                "title": item["Title"],
                                "resource": (item.get("CauseMetadata") or {}).get("Resource", ""),
                            }
                            for item in misconfigurations
                            if item.get("Status") == "FAIL"
                        ],
                        "exceptions": trivy_exceptions(source),
                    }
                    details["failed_checks"] = len(details["findings"])
                elif name == "tflint":
                    details = {
                        "issues": [{"rule": item["rule"]["name"], "severity": item["rule"]["severity"]} for item in parsed.get("issues", [])],
                        "errors": len(parsed.get("errors", [])),
                    }
                else:
                    summary = parsed.get("summary", {})
                    details = {
                        "passed_checks": summary.get("passed", 0),
                        "failed_checks": summary.get("failed", 0),
                        "skipped_checks": summary.get("skipped", 0),
                        "parsing_errors": summary.get("parsing_errors", 0),
                    }
                    details["findings"] = [
                        {"check_id": item["check_id"], "check_name": item["check_name"], "resource": item["resource"]}
                        for item in parsed.get("results", {}).get("failed_checks", [])
                    ]
                    details["exceptions"] = [
                        {
                            "check_id": item["check_id"],
                            "check_name": item["check_name"],
                            "resource": item["resource"],
                            "reason": item.get("check_result", {}).get("suppress_comment", ""),
                        }
                        for item in parsed.get("results", {}).get("skipped_checks", [])
                    ]
                report[name] = {"returncode": result.returncode, **details}
            report["status"] = "passed" if all(report[name]["returncode"] == 0 for name in commands) else "failed"
            if report["tflint"]["errors"] or report["checkov"]["parsing_errors"]:
                report["status"] = "blocked"
    except (OSError, ValueError, KeyError, TypeError, TimeoutExpired) as error:
        report.update(status="blocked", error_type=type(error).__name__)
    finally:
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        report["checkov_version"] = importlib.metadata.version("checkov")
        report["implementation_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        report["limits"] = (
            "Static source analysis only; no state, tfvars or cloud calls. trivy uses its embedded checks offline. "
            "Resource-scoped source exceptions are reported, not treated as implemented controls. Temporary copies removed."
        )
        report["reproduce"] = [
            ".venv/bin/python",
            "-m",
            "scripts.quality.static_checks",
            "--directory",
            "<source_root>",
            "--output-directory",
            "<new_directory>",
        ]
        (output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    LOGGER.info("event=terraform_static status=%s", report["status"])
    return report["status"] == "passed"


def main() -> None:
    """Run local checks without loading private Terraform inputs or cloud credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(0 if scan(args.directory, args.output_directory) else 1)


if __name__ == "__main__":
    main()
