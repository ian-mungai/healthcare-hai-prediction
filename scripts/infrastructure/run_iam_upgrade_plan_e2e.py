"""E2E of the rule that a change to the bronze writer's managed policies updates them in place and never replaces one.

    .venv/bin/python -m scripts.infrastructure.run_iam_upgrade_plan_e2e --output-directory <new folder> [--plugin-dir <providers folder>]

An IAM managed policy has no in-place change of its description: in the installed AWS provider the description is ForceNew, so a plan that changes it destroys
the policy and creates another under a new ARN, and the role loses its grants until the attachment catches up. An empty-state plan cannot show that, and a
mock provider does not run the provider's own diff. So this plans the real IAM stack with the real AWS provider and no AWS call (fake keys, every check
skipped, a targeted plan, no refresh) against ``tests/fixtures/bronze_writer_policies_applied_state.json``, a synthetic state of the seven managed bronze
policies as they were applied (their descriptions and documents, with synthetic names, account and ARNs). The plan must:

1. update ``run_record_writes`` in place (its document gained the mirror attempt keys) and leave the six other policies untouched;
2. destroy, create or replace nothing, and name no ``replace_paths`` anywhere.

A second plan targets the Glue mirror grants: the bronze writer's two inline policies (the bronze and the bronze_dictionary database) are created, the three
applied layer grants are untouched and no other inline policy is planned.

It then plants a changed description and the plan must show the replacement, which proves that the check can see it. Nothing is applied and no
credential is read.
Refresh the fixture only from a plan of the revision that was applied (``terraform show -json`` of a targeted plan of the policies, with synthetic ARNs).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
STACK = ROOT / "infra" / "iam"
FIXTURE = ROOT / "tests" / "fixtures" / "bronze_writer_policies_applied_state.json"
PROVIDER_FOLDERS = (STACK / ".terraform" / "providers", ROOT / "infra" / ".terraform" / "providers")
TIMEOUT_SECONDS = 900.0
TARGET = "aws_iam_policy.bronze_writer"
GRANT_TARGET = "aws_iam_role_policy.athena_mirror_writer"
GRANT_ADDRESS = 'aws_iam_role_policy.athena_mirror_writer["{layer}"]'
CHANGED = 'aws_iam_policy.bronze_writer["run_record_writes"]'
OFFLINE_PROVIDER = """provider "aws" {
  profile                     = null
  allowed_account_ids         = null
  region                      = "us-west-2"
  access_key                  = "synthetic"
  secret_key                  = "synthetic"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  skip_region_validation      = true
}
"""
VARIABLES = {
    "aws_profile": "example_project_dev",
    "environment": "dev",
    "admin_profile": "example_admin",
    "aws_region": "us-west-2",
    "expected_account_id": "111111111111",
    "data_bucket_name": "example-data-bucket",
    "project_name": "example_project",
    "owner_ip_cidrs": [],
    "source_raw_root_enabled": True,
    "bronze_writer_prepared": True,
    "alerts_topic_arn": "arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev",
    "bronze_qualification_collection": "cms/hospitals",
    "athena_mirror_prepared": True,
    "athena_owner_user": "example_owner",
    "job_writer_layers_prepared": True,
    "bronze_live_locations": {
        "bronze": "s3://example-data-bucket/lakehouse/bronze/",
        "bronze_dictionary": "s3://example-data-bucket/lakehouse/bronze_dictionary/",
    },
}
DESCRIPTION_LINE = "description = each.value.description"
PLANTED_LINE = 'description = "${each.value.description} planted"'


def terraform(arguments: list[str], cwd: Path) -> tuple[int, str]:
    """Run terraform in ``cwd`` with no AWS profile or credential of the caller, and return its exit code and combined text."""
    kept = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR", "TF_PLUGIN_CACHE_DIR") if name in os.environ}
    environment = {**kept, "TF_IN_AUTOMATION": "1", "TF_INPUT": "0", "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null"}
    result = run_command("terraform", arguments, cwd=cwd, env=environment, timeout=TIMEOUT_SECONDS)
    return result.returncode, result.stdout + result.stderr


def prepare(work: Path, plugin_directory: Path | None, planted: bool) -> Path:
    """The IAM stack with the offline provider and inputs, beside the config folders it reads, initialized. ``planted`` changes the policy description."""
    stack = work / "infra" / "iam"
    shutil.copytree(STACK, stack, ignore=shutil.ignore_patterns(".terraform", "tests", "*.tfstate*", "*.tfplan", "ci.tfvars.json"))
    for folder in ("config", "data_contracts"):
        (work / folder).symlink_to(ROOT / folder, target_is_directory=True)
    (stack / "provider_override.tf").write_text(OFFLINE_PROVIDER, encoding="utf-8")
    (stack / "inputs.auto.tfvars.json").write_text(json.dumps(VARIABLES, indent=2) + "\n", encoding="utf-8")
    if planted:
        source = (stack / "bronze_writer.tf").read_text(encoding="utf-8")
        if DESCRIPTION_LINE not in source:
            raise RuntimeError("the planted defect cannot find the policy description line in bronze_writer.tf")
        (stack / "bronze_writer.tf").write_text(source.replace(DESCRIPTION_LINE, PLANTED_LINE), encoding="utf-8")
    arguments = ["init", "-backend=false", "-input=false", "-no-color"]
    if plugin_directory is not None:
        arguments.append(f"-plugin-dir={plugin_directory}")
    code, text = terraform(arguments, stack)
    if code != 0:
        raise RuntimeError(f"terraform init failed:\n{text[-1500:]}")
    return stack


def plan(stack: Path, target: str) -> list[dict[str, Any]]:
    """The resource changes of a plan of target against the applied-state fixture."""
    code, text = terraform(["plan", "-refresh=false", "-input=false", "-no-color", f"-state={FIXTURE}", f"-target={target}", "-out=plan.bin"], stack)
    if code != 0:
        raise RuntimeError(f"terraform plan failed:\n{text[-1500:]}")
    code, shown = terraform(["show", "-json", "plan.bin"], stack)
    if code != 0:
        raise RuntimeError(f"terraform show failed:\n{shown[-1500:]}")
    changes: list[dict[str, Any]] = json.loads(shown)["resource_changes"]
    return changes


def grant_verdict(changes: list[dict[str, Any]]) -> Mapping[str, bool]:
    """The Glue grants: the bronze writer's two are created (the bronze database and the bronze_dictionary database), the three applied ones are untouched.
    The roles that the target pulls in are not under test."""
    actions = {change["address"]: change["change"]["actions"] for change in changes if change["type"] == "aws_iam_role_policy"}
    applied = [GRANT_ADDRESS.format(layer=layer) for layer in ("silver", "gold", "features")]
    return {
        "the bronze writer's Glue grant is created": actions.get(GRANT_ADDRESS.format(layer="bronze")) == ["create"],
        "the bronze writer's dictionary Glue grant is created": actions.get(GRANT_ADDRESS.format(layer="bronze_dictionary")) == ["create"],
        "the three applied Glue grants are untouched": all(actions.get(address) == ["no-op"] for address in applied),
        "no other inline policy is planned": len(actions) == 5,
    }


def verdict(changes: list[dict[str, Any]]) -> Mapping[str, bool]:
    """The two rules over the planned changes."""
    actions = {change["address"]: change["change"]["actions"] for change in changes}
    others = [address for address in actions if address != CHANGED]
    return {
        "the changed policy is updated in place": actions.get(CHANGED) == ["update"],
        "the six other policies are untouched": len(others) == 6 and all(actions[address] == ["no-op"] for address in others),
        "nothing is destroyed, created or replaced": not any(action in ("delete", "create") for value in actions.values() for action in value),
        "no resource names a replace path": not any(change["change"].get("replace_paths") for change in changes),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-directory", type=Path, required=True, help="a new folder for report.json")
    parser.add_argument(
        "--plugin-dir", type=Path, help="the folder of the provider binaries (default: the IAM stack's or the root stack's .terraform/providers)"
    )
    args = parser.parse_args(argv)
    if args.output_directory.exists():
        sys.stderr.write(f"iam upgrade plan E2E: {args.output_directory} exists; give a new folder\n")
        return 1
    plugin = args.plugin_dir or next((folder for folder in PROVIDER_FOLDERS if folder.is_dir()), None)
    report: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="iam_upgrade_") as temporary:
        for label, planted in (("current_stack", False), ("planted_description_change", True)):
            work = Path(temporary) / label
            work.mkdir()
            stack = prepare(work, plugin, planted)
            changes = plan(stack, TARGET)
            report[label] = {"actions": {change["address"]: change["change"]["actions"] for change in changes}, "checks": dict(verdict(changes))}
            if not planted:
                grants = plan(stack, GRANT_TARGET)
                report[label]["glue_grant_actions"] = {
                    change["address"]: change["change"]["actions"] for change in grants if change["type"] == "aws_iam_role_policy"
                }
                report[label]["checks"].update(grant_verdict(grants))
    current, planted_checks = report["current_stack"]["checks"], report["planted_description_change"]["checks"]
    report["planted_description_change"]["caught"] = not all(planted_checks.values())
    for name, ok in current.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} current stack: {name}\n")
    sys.stdout.write(f"{'PASS' if report['planted_description_change']['caught'] else 'FAIL'} planted description change: the replacement is seen\n")
    args.output_directory.mkdir(parents=True)
    (args.output_directory / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    passed = all(current.values()) and report["planted_description_change"]["caught"]
    sys.stdout.write(f"iam upgrade plan E2E: {'passed' if passed else 'FAILED'}\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
