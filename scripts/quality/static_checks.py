"""Run credential-free Terraform scanners on source-only copies and retain safe evidence."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)


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
            commands = {
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
                result = subprocess.run(command, cwd=scratch, env=environment, capture_output=True, text=True, timeout=180, check=False)  # noqa: S603 - fixed scanner argument arrays.
                parsed = json.loads(result.stdout)
                if not isinstance(parsed, dict):
                    raise ValueError("Unexpected scanner report")
                if name == "tflint":
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
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
        report.update(status="blocked", error_type=type(error).__name__)
    finally:
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        report["checkov_version"] = importlib.metadata.version("checkov")
        report["implementation_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        report["limits"] = (
            "Static source analysis only; no state, tfvars or cloud calls. Resource-scoped source exceptions are reported, "
            "not treated as implemented controls. Temporary copies removed."
        )
        report["reproduce"] = [".venv/bin/python", "scripts/quality/static_checks.py", "--directory", "<source_root>", "--output-directory", "<new_directory>"]
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
