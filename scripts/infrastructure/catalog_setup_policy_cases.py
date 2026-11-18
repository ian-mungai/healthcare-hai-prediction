"""Evaluate the catalog setup task role's policy with synthetic resources and the project's IAM evaluator, then plant defects in its template.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.catalog_setup_policy_cases --output <NEW_DIRECTORY>``.
Each case states one request and whether the role may make it. Each planted defect changes the policy template and must make at
least one case fail. The report holds case names and outcomes only.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from string import Template
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, UnsupportedPolicyElement, decide

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "infra/iam/owner_policies/host_catalog_setup.json"
REGION = "us-west-2"
ACCOUNT = "".join(["1"] * 12)
BUCKET = "example-data-bucket"
SSM = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter"
PAIR_NAMES = [
    f"/example_project/aws_polaris_{identity}_client_{part}"
    for identity in ("reader", "e2e", "bronze", "silver", "gold", "features", "serving")
    for part in ("id", "secret")
]
HOLD = "/example_project/services_apply_lock"
RECORDS = f"arn:aws:s3:::{BUCKET}/lakehouse/catalog_setup"
SNAPSHOT_FOLDERS = ("lakehouse/catalog_backups/", "lakehouse/silver/catalog_backups/", "lakehouse/features/catalog_backups/")
CLUSTER = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/example_project_cluster_dev"
VALUES = {
    "PAIR_ARNS": json.dumps([f"{SSM}{name}" for name in PAIR_NAMES]),
    "HOLD_ARN": f"{SSM}{HOLD}",
    "RECORDS_ARN": RECORDS,
    "BUCKET_ARN": f"arn:aws:s3:::{BUCKET}",
    "RECORD_SCOPE": "lakehouse/catalog_setup/",
    "SNAPSHOT_ARNS": json.dumps([f"arn:aws:s3:::{BUCKET}/{folder}*" for folder in SNAPSHOT_FOLDERS]),
    "SNAPSHOT_SCOPES": json.dumps([f"{folder}*" for folder in SNAPSHOT_FOLDERS]),
    "CLUSTER_ARN": CLUSTER,
    "TASK_ARN": f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/example_project_cluster_dev/*",
}
CREATE_ONLY: dict[str, tuple[str, ...]] = {"s3:if-none-match": ("*",)}
Case = tuple[str, str, str, bool, dict[str, tuple[str, ...]]]


def render(defect: tuple[str, str] | None = None) -> dict[str, Any]:
    """The policy rendered from its template; a planted defect replaces text in the template first and must find it."""
    text = TEMPLATE.read_text()
    if defect is not None:
        if defect[0] not in text:
            raise ValueError(f"the planted defect finds no text to replace: {defect[0][:40]}")
        text = text.replace(defect[0], defect[1], 1)
    return dict(json.loads(Template(text).substitute(VALUES)))


def snapshot_cases() -> list[Case]:
    """The reads of the catalog backup folders, which the restore step needs, and the writes it must not make."""
    rows: list[Case] = []
    for folder in SNAPSHOT_FOLDERS:
        rows += [
            (f"reads a catalog backup of {folder}", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/{folder}catalog_backup_20261117T120000Z-aaaaaaaa.json", True, {}),
            (
                f"reads a committed snapshot of {folder}",
                "s3:GetObject",
                f"arn:aws:s3:::{BUCKET}/{folder}catalog_committed_20261117T120000Z-aaaaaaaa.json",
                True,
                {},
            ),
            (f"lists the folder {folder}", "s3:ListBucket", f"arn:aws:s3:::{BUCKET}", True, {"s3:prefix": (folder,)}),
        ]
    return rows + [
        ("writes no catalog backup", "s3:PutObject", f"arn:aws:s3:::{BUCKET}/lakehouse/catalog_backups/catalog_backup_x.json", False, CREATE_ONLY),
        (
            "writes no committed snapshot",
            "s3:PutObject",
            f"arn:aws:s3:::{BUCKET}/lakehouse/silver/catalog_backups/catalog_committed_x.json",
            False,
            CREATE_ONLY,
        ),
        ("deletes no catalog backup", "s3:DeleteObject", f"arn:aws:s3:::{BUCKET}/lakehouse/features/catalog_backups/catalog_backup_x.json", False, {}),
        ("reads no sibling of a backup folder", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/catalog_backups_extra/x.json", False, {}),
        ("reads no other silver object", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/silver/stg_hospital/data/x.parquet", False, {}),
        ("lists no sibling of a backup folder", "s3:ListBucket", f"arn:aws:s3:::{BUCKET}", False, {"s3:prefix": ("lakehouse/silver/",)}),
    ]


def cases() -> list[Case]:
    """Every request the role must make or must not make."""
    rows: list[Case] = []
    for name in PAIR_NAMES:
        rows.append((f"reads {name.rsplit('/', 1)[-1]}", "ssm:GetParameter", f"{SSM}{name}", True, {}))
        rows.append((f"writes {name.rsplit('/', 1)[-1]}", "ssm:PutParameter", f"{SSM}{name}", True, {"ssm:Overwrite": ("true",)}))
    rows += [
        ("deletes no pair", "ssm:DeleteParameter", f"{SSM}{PAIR_NAMES[0]}", False, {}),
        ("reads no other project parameter", "ssm:GetParameter", f"{SSM}/example_project/polaris_silver_client_id", False, {}),
        ("reads no Mac pair name", "ssm:GetParameter", f"{SSM}/example_project/polaris_gold_client_secret", False, {}),
        ("reads no root secret", "ssm:GetParameter", f"{SSM}/example_project/polaris_root_secret", False, {}),
        ("writes no root secret", "ssm:PutParameter", f"{SSM}/example_project/polaris_root_secret", False, {}),
        ("reads no batch of parameters", "ssm:GetParameters", f"{SSM}{PAIR_NAMES[0]}", False, {}),
        ("reads no parameter path", "ssm:GetParametersByPath", f"{SSM}/example_project", False, {}),
        ("reads the apply lock", "ssm:GetParameter", f"{SSM}{HOLD}", True, {}),
        ("creates the apply lock without overwrite", "ssm:PutParameter", f"{SSM}{HOLD}", True, {"ssm:Overwrite": ("false",)}),
        ("deletes the apply lock", "ssm:DeleteParameter", f"{SSM}{HOLD}", True, {}),
        ("never overwrites the apply lock", "ssm:PutParameter", f"{SSM}{HOLD}", False, {"ssm:Overwrite": ("true",)}),
        ("never overwrites the apply lock in other case", "ssm:PutParameter", f"{SSM}{HOLD}", False, {"ssm:Overwrite": ("TRUE",)}),
        ("reads no launch hold parameter", "ssm:GetParameter", f"{SSM}/example_project/host_launch_hold", False, {}),
        ("creates an approval once", "s3:PutObject", f"{RECORDS}/approvals/bronze.json", True, CREATE_ONLY),
        ("never replaces an approval", "s3:PutObject", f"{RECORDS}/approvals/bronze.json", False, {}),
        ("never replaces an approval on a version match", "s3:PutObject", f"{RECORDS}/approvals/bronze.json", False, {"s3:if-match": ("v1",)}),
        ("never deletes an approval", "s3:DeleteObject", f"{RECORDS}/approvals/bronze.json", False, {}),
        ("reads an approval", "s3:GetObject", f"{RECORDS}/approvals/bronze.json", True, {}),
        ("creates a lease once", "s3:PutObject", f"{RECORDS}/locks/bronze_writer.json", True, CREATE_ONLY),
        ("never overwrites a lease", "s3:PutObject", f"{RECORDS}/locks/bronze_writer.json", False, {}),
        ("replaces a lease at a version", "s3:PutObject", f"{RECORDS}/locks/bronze_writer.json", True, {"s3:if-match": ("v1",)}),
        ("never deletes a lease", "s3:DeleteObject", f"{RECORDS}/locks/bronze_writer.json", False, {"s3:if-match": ("v1",)}),
        ("reads a lease", "s3:GetObject", f"{RECORDS}/locks/bronze_writer.json", True, {}),
        ("writes a pending mark", "s3:PutObject", f"{RECORDS}/pending/bronze_writer.json", True, {}),
        ("deletes a pending mark", "s3:DeleteObject", f"{RECORDS}/pending/bronze_writer.json", True, {}),
        ("writes no data object", "s3:PutObject", f"arn:aws:s3:::{BUCKET}/lakehouse/bronze/table/data.parquet", False, CREATE_ONLY),
        ("reads no data object", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/bronze/table/data.parquet", False, {}),
        ("reads no launch record", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/launch_records/run/claim.json", False, {}),
        ("reads no code archive", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/run/inputs/code.tar.gz", False, {}),
        ("writes in no sibling prefix", "s3:PutObject", f"arn:aws:s3:::{BUCKET}/lakehouse/catalog_setup_extra/x.json", False, CREATE_ONLY),
        ("lists its own prefix", "s3:ListBucket", f"arn:aws:s3:::{BUCKET}", True, {"s3:prefix": ("lakehouse/catalog_setup/approvals/bronze.json",)}),
        ("lists no other prefix", "s3:ListBucket", f"arn:aws:s3:::{BUCKET}", False, {"s3:prefix": ("lakehouse/bronze/",)}),
        ("lists the bucket root without a prefix", "s3:ListBucket", f"arn:aws:s3:::{BUCKET}", False, {}),
        *snapshot_cases(),
        ("lists the project's tasks", "ecs:ListTasks", "*", True, {"ecs:cluster": (CLUSTER,)}),
        ("lists no other cluster's tasks", "ecs:ListTasks", "*", False, {"ecs:cluster": (CLUSTER + "_other",)}),
        ("describes the project's tasks", "ecs:DescribeTasks", f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/example_project_cluster_dev/abc", True, {}),
        ("describes no other cluster's tasks", "ecs:DescribeTasks", f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/other_cluster/abc", False, {}),
        ("starts no task", "ecs:RunTask", f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/example_project_dev_job_bronze_host:1", False, {}),
        ("stops no task", "ecs:StopTask", f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/example_project_cluster_dev/abc", False, {}),
        ("passes no role", "iam:PassRole", f"arn:aws:iam::{ACCOUNT}:role/example_project_job_write_bronze_task_dev", False, {}),
        ("reads no secret in Secrets Manager", "secretsmanager:GetSecretValue", f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:example", False, {}),
        ("uses no key", "kms:Decrypt", f"arn:aws:kms:{REGION}:{ACCOUNT}:key/00000000-0000-0000-0000-000000000000", False, {}),
        ("describes no instance", "ec2:DescribeInstances", "*", False, {}),
    ]
    return rows


def observe(document: dict[str, Any], rows: list[Case]) -> list[dict[str, Any]]:
    found = []
    for label, action, resource, expected, context in rows:
        allowed = decide([document], Request(action, resource, context)).allowed
        found.append({"case": label, "expected": expected, "observed": allowed, "passed": allowed == expected})
    return found


def defects() -> dict[str, tuple[str, str]]:
    """Each planted defect: a text in the template and what replaces it."""
    return {
        "the pair grant also deletes": (
            '"Action": ["ssm:GetParameter", "ssm:PutParameter"],\n      "Resource": ${PAIR_ARNS}',
            '"Action": ["ssm:GetParameter", "ssm:PutParameter", "ssm:DeleteParameter"],\n      "Resource": ${PAIR_ARNS}',
        ),
        "the pair grant reads in batches": (
            '"Action": ["ssm:GetParameter", "ssm:PutParameter"],\n      "Resource": ${PAIR_ARNS}',
            '"Action": ["ssm:GetParameter", "ssm:GetParameters", "ssm:PutParameter"],\n      "Resource": ${PAIR_ARNS}',
        ),
        "the pair grant covers every parameter": ('"Resource": ${PAIR_ARNS}', '"Resource": "*"'),
        "the apply lock may be overwritten": ('"Sid": "DenyHoldOverwrite",\n      "Effect": "Deny"', '"Sid": "DenyHoldOverwrite",\n      "Effect": "Allow"'),
        "the apply lock overwrite check is case sensitive": (
            '"StringEqualsIgnoreCase": { "ssm:Overwrite": "true" }',
            '"StringEquals": { "ssm:Overwrite": "true" }',
        ),
        "the apply lock cannot be deleted": (
            '"Action": ["ssm:GetParameter", "ssm:PutParameter", "ssm:DeleteParameter"]',
            '"Action": ["ssm:GetParameter", "ssm:PutParameter"]',
        ),
        "an approval may be replaced": (
            '"Sid": "DenyApprovalCreateWithoutCondition",\n      "Effect": "Deny"',
            '"Sid": "DenyApprovalCreateWithoutCondition",\n      "Effect": "Allow"',
        ),
        "an approval may be replaced at a version": ('"StringNotEquals": { "s3:if-none-match": "*" }', '"StringEquals": { "s3:if-none-match": "x" }'),
        "a lease may be overwritten without a condition": (
            '"Sid": "DenyLockWriteWithoutCondition",\n      "Effect": "Deny"',
            '"Sid": "DenyLockWriteWithoutCondition",\n      "Effect": "Allow"',
        ),
        "a lease needs only one condition header": ('"Null": { "s3:if-none-match": "true", "s3:if-match": "true" }', '"Null": { "s3:if-none-match": "true" }'),
        "approvals and leases may be deleted": (
            '"Sid": "DenyApprovalAndLockDelete",\n      "Effect": "Deny"',
            '"Sid": "DenyApprovalAndLockDelete",\n      "Effect": "Allow"',
        ),
        "leases may be deleted": ('"Resource": ["${RECORDS_ARN}/approvals/*", "${RECORDS_ARN}/locks/*"]', '"Resource": ["${RECORDS_ARN}/approvals/*"]'),
        "the record grant covers the whole bucket": ('"Resource": "${RECORDS_ARN}/*"', '"Resource": "arn:aws:s3:::example-data-bucket/*"'),
        "the record grant covers sibling prefixes": ('"Resource": "${RECORDS_ARN}/*"', '"Resource": "${RECORDS_ARN}*"'),
        "the listing is not limited to the prefix": (
            '"Condition": { "StringLike": { "s3:prefix": ["${RECORD_SCOPE}*"] } }',
            '"Condition": { "StringLike": { "s3:prefix": ["*"] } }',
        ),
        "the snapshot read covers the whole bucket": ('"Resource": ${SNAPSHOT_ARNS}', '"Resource": "arn:aws:s3:::example-data-bucket/*"'),
        "the snapshot read also writes": (
            '"Action": "s3:GetObject",\n      "Resource": ${SNAPSHOT_ARNS}',
            '"Action": ["s3:GetObject", "s3:PutObject"],\n      "Resource": ${SNAPSHOT_ARNS}',
        ),
        "the snapshot folder listing is not limited": ('"s3:prefix": ${SNAPSHOT_SCOPES}', '"s3:prefix": ["*"]'),
        "the task listing covers every cluster": (
            '"Condition": { "ArnEquals": { "ecs:cluster": "${CLUSTER_ARN}" } }',
            '"Condition": { "ArnLike": { "ecs:cluster": "*" } }',
        ),
        "the task description covers every cluster": ('"Resource": "${TASK_ARN}"', '"Resource": "*"'),
        "the role may start a task": ('"Statement": [', '"Statement": [{"Sid": "X", "Effect": "Allow", "Action": "ecs:RunTask", "Resource": "*"},'),
        "the role may pass a role": ('"Statement": [', '"Statement": [{"Sid": "X", "Effect": "Allow", "Action": "iam:PassRole", "Resource": "*"},'),
    }


def verify(output: Path) -> int:
    rows = cases()
    checks = observe(render(), rows)
    planted = []
    for name, defect in defects().items():
        try:
            failures = [item["case"] for item in observe(render(defect), rows) if not item["passed"]]
        except UnsupportedPolicyElement as error:
            failures = [f"the evaluator refuses the policy: {error}"]
        planted.append({"defect": name, "caught": bool(failures), "failed_cases": failures[:3]})
    output.mkdir(parents=True, exist_ok=False)
    (output / "report.json").write_text(json.dumps({"checks": checks, "defects": planted}, indent=2) + "\n")
    failed = [item["case"] for item in checks if not item["passed"]] + [f"defect not caught: {item['defect']}" for item in planted if not item["caught"]]
    sys.stdout.write(f"{len(checks)} cases, {len(planted)} planted defects, {len(failed)} problems\n")
    for line in failed:
        sys.stdout.write(f"FAIL {line}\n")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--output", type=Path, required=True, help="a new directory for the report")
    return verify(parser.parse_args().output)


if __name__ == "__main__":
    sys.exit(main())
