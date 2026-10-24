"""Render the early job policy proposal and exercise its requests without credentials or cloud calls."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from infra.iam.scripts.render_policy import non_whitespace_length, render_template
from scripts.infrastructure.policy_evaluator import Request, decide
from scripts.infrastructure.secret_store import terraform_variables

REPO = Path(__file__).resolve().parents[2]
VALUES = {
    "AWS_REGION": "eu-west-1",
    "AWS_ACCOUNT_ID": "111111111111",
    "PROJECT_NAME": "example_project",
    "ENVIRONMENT": "dev",
    "DATA_BUCKET_NAME": "example-project-ci-bucket",
}
ROLE = "AROEXAMPLEREADER:task_one"
S3 = "arn:aws:s3:::example-project-ci-bucket"
IAM = "arn:aws:iam::111111111111:role/example_project_"
PARAMETER = "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/"
POLICIES = ("job_stage_read", "job_source_read", "job_seal_deny", "job_try_writer", "job_execution", "job_project_delta", "job_try_trust")
Case = tuple[str, str, str, str, bool, dict[str, tuple[str, ...]]]


def cases() -> list[Case]:
    """Specify permitted effects and credible scope regressions independently of the templates."""
    result: list[Case] = []
    for actor in ("offline", "seal", "read"):
        for action in ("s3:GetObject", "s3:GetObjectVersion"):
            result.append((f"{actor} pinned stage read {action}", actor, action, f"{S3}/lakehouse/run_stage/run/inputs/pins.json", True, {}))
        for action, resource, allowed in (
            ("s3:GetObject", f"{S3}/lakehouse/run_stage/run/grants/{ROLE}.json", True),
            ("s3:GetObject", f"{S3}/lakehouse/run_stage/run/grants/AROOTHER:task_two.json", False),
            ("s3:GetObjectVersion", f"{S3}/lakehouse/run_stage/run/grants/{ROLE}.json", False),
            ("s3:GetObject", f"{S3}/lakehouse/run_stage/run/tries/try_one/done.json", True),
            ("s3:PutObject", f"{S3}/lakehouse/run_stage/run/tries/try_one/out.parquet", False),
            ("s3:ListBucket", S3, False),
            ("ssm:GetParameter", PARAMETER + "aws_reader_id", False),
        ):
            result.append((f"{actor} {action} {resource.rsplit('/', 1)[-1]}", actor, action, resource, allowed, {"aws:userid": (ROLE,)}))
    for source in ("bronze", "silver", "gold"):
        for action in ("s3:GetObject", "s3:GetObjectVersion"):
            result.append((f"read {source} {action}", "read", action, f"{S3}/lakehouse/{source}/table/metadata.json", True, {}))
        for actor in ("offline", "seal"):
            result.append((f"{actor} cannot read {source}", actor, "s3:GetObject", f"{S3}/lakehouse/{source}/table/metadata.json", False, {}))
    for action in ("s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"):
        result.append((f"read cannot mutate source {action}", "read", action, f"{S3}/lakehouse/silver/table/metadata.json", False, {}))
    for prefix in ("features", "silver_e2e", "catalog_journal", "bronze_dictionary"):
        result.append((f"read excludes {prefix}", "read", "s3:GetObject", f"{S3}/lakehouse/{prefix}/table/metadata.json", False, {}))
    for prefix, allowed in (
        ("lakehouse/bronze/", True),
        ("lakehouse/silver/table/", True),
        ("lakehouse/gold/", True),
        ("", False),
        ("lakehouse/", False),
        ("lakehouse/silver_e2e/", False),
        ("lakehouse/run_stage/", False),
    ):
        result.append((f"scoped source list {prefix}", "read", "s3:ListBucket", S3, allowed, {"s3:prefix": (prefix,)}))
    for actor, allowed in (("offline", True), ("read", True), ("seal", False)):
        result.append((f"{actor} input tree", actor, "s3:GetObjectVersion", f"{S3}/lakehouse/run_stage/run/inputs/trees/code.tar", allowed, {}))
    for action in ("s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"):
        result.append((f"try writer {action}", "try_writer", action, f"{S3}/lakehouse/run_stage/run/tries/try_one/output", True, {}))
    for action, resource in (
        ("s3:GetObject", f"{S3}/lakehouse/run_stage/run/tries/try_one/output"),
        ("s3:DeleteObject", f"{S3}/lakehouse/run_stage/run/tries/try_one/output"),
        ("s3:PutObject", f"{S3}/lakehouse/run_stage/run/grants/identity.json"),
        ("s3:PutObject", f"{S3}/lakehouse/silver/table/metadata.json"),
        ("s3:ListBucket", S3),
    ):
        result.append((f"try writer scope {action} {resource.rsplit('/', 1)[-1]}", "try_writer", action, resource, False, {}))
    for name in ("aws_reader_id", "aws_reader_secret"):
        result.append((f"execution injects {name}", "execution", "ssm:GetParameters", PARAMETER + name, True, {}))
        result.append((f"project collection reads {name}", "project", "ssm:GetParameter", PARAMETER + name, True, {}))
    for name in ("aws_writer_secret", "polaris_root_secret", "aws_bronze_secret"):
        for actor, action in (("execution", "ssm:GetParameters"), ("project", "ssm:GetParameter")):
            result.append((f"{actor} excludes {name}", actor, action, PARAMETER + name, False, {}))
    for image in ("analytics", "quality", "spark"):
        result.append(
            (
                f"execution pulls {image}",
                "execution",
                "ecr:BatchGetImage",
                f"arn:aws:ecr:eu-west-1:111111111111:repository/example_project_{image}_dev",
                True,
                {},
            )
        )
    result.append(
        ("execution cannot pull unrelated image", "execution", "ecr:BatchGetImage", "arn:aws:ecr:eu-west-1:111111111111:repository/unrelated", False, {})
    )
    for role in ("execution", "offline_task", "seal_task", "read_task"):
        for service, allowed in (("ecs-tasks.amazonaws.com", True), ("ec2.amazonaws.com", False)):
            result.append((f"pass {role} to {service}", "project", "iam:PassRole", f"{IAM}job_{role}_dev", allowed, {"iam:PassedToService": (service,)}))
    result.append(
        (
            "cannot pass a writer role",
            "project",
            "iam:PassRole",
            f"{IAM}job_write_silver_task_dev",
            False,
            {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)},
        )
    )
    for action in ("iam:GetRole", "ec2:CreateVpcEndpoint", "ec2:AuthorizeSecurityGroupEgress"):
        result.append((f"project has no {action}", "project", action, "*", False, {}))
    for session, allowed in (("hai_try_example", True), ("unrelated", False)):
        result.append((f"assume try writer {session}", "project", "sts:AssumeRole", f"{IAM}job_try_writer_dev", allowed, {"sts:RoleSessionName": (session,)}))
    result.append(("project reads versioned stage input", "project", "s3:GetObjectVersion", f"{S3}/lakehouse/run_stage/run/inputs/code.tar", True, {}))
    return result


def evaluate(documents: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Evaluate actual rendered policies against the complete request matrix."""
    actors = {
        "offline": [documents["job_stage_read"]],
        "seal": [documents["job_stage_read"], documents["job_seal_deny"]],
        "read": [documents["job_stage_read"], documents["job_source_read"]],
        "execution": [documents["job_execution"]],
        "try_writer": [documents["job_try_writer"]],
        "project": [documents["job_project_delta"]],
    }
    observations = []
    for label, actor, action, resource, expected, context in cases():
        decision = decide(actors[actor], Request(action, resource, {"aws:userid": (ROLE,), **context}))
        observations.append(
            {
                "case": label,
                "expected": expected,
                "observed": decision.allowed,
                "passed": decision.allowed == expected,
                "effect": decision.effect,
                "statements": decision.statements,
            }
        )
    return observations


def main() -> int:
    """Retain rendered policies, request outcomes and planted-defect results at a new evidence path."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    directory = REPO / "infra/iam/owner_policies"
    paths = [
        *(directory / f"{name}.json" for name in POLICIES),
        REPO / "infra/iam/job_variables.tf",
        Path(__file__),
        REPO / "infra/iam/scripts/render_policy.py",
        REPO / "scripts/infrastructure/policy_evaluator.py",
        REPO / "scripts/infrastructure/secret_store.py",
    ]
    before = {str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None for path in paths}
    report: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "classification": "synthetic",
        "command": [sys.executable, *sys.argv],
        "source_hashes": before,
        "limits": ["Rendered-policy evaluator only", "Terraform mocked plans and all native IAM and network proof pending lead"],
        "cleanup": "done: no child processes or cloud calls",
    }
    try:
        defaults = terraform_variables(REPO / "infra/iam/job_variables.tf")
        expected_defaults = {
            "aws_polaris_reader_client_id_name": "/healthcare_hai_prediction/aws_polaris_reader_client_id",
            "aws_polaris_reader_client_secret_name": "/healthcare_hai_prediction/aws_polaris_reader_client_secret",
        }
        if defaults != expected_defaults:
            raise ValueError("reader parameter defaults differ from the early read contract")
        names = {
            "TASK_USER_ID": "${aws:userid}",
            "PROJECT_USER": "example_project_dev",
            "AWS_POLARIS_READER_CLIENT_ID_NAME": "/example_project/aws_reader_id",
            "AWS_POLARIS_READER_CLIENT_SECRET_NAME": "/example_project/aws_reader_secret",
        }
        documents = {name: render_template((directory / f"{name}.json").read_text(), VALUES, names) for name in POLICIES}
        checks = evaluate(documents)
        trust = documents["job_try_trust"]["Statement"]
        expected_trust = [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::111111111111:user/example_project_dev"},
                "Action": "sts:AssumeRole",
                "Condition": {"StringLike": {"sts:RoleSessionName": "hai_try_*"}},
            }
        ]
        checks.append(
            {
                "case": "try writer trusts only the project user with a try session",
                "expected": expected_trust,
                "observed": trust,
                "passed": trust == expected_trust,
            }
        )
        defects = []
        mutations = {
            "other_task_grant": ("job_stage_read", "OwnGrant", "Resource", f"{S3}/lakehouse/run_stage/*/grants/*.json"),
            "source_write": ("job_source_read", "ReadSources", "Action", ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"]),
            "all_source_prefixes": ("job_source_read", "ReadSources", "Resource", f"{S3}/lakehouse/*"),
            "writer_parameter": (
                "job_execution",
                "InjectReaderPair",
                "Resource",
                [PARAMETER + "aws_reader_id", PARAMETER + "aws_reader_secret", PARAMETER + "aws_writer_secret"],
            ),
            "pass_any_role": ("job_project_delta", "PassReadJobRoles", "Resource", "arn:aws:iam::111111111111:role/*"),
            "seal_reads_tree": ("job_seal_deny", "NoTreeInput", "Resource", f"{S3}/unused/*"),
        }
        for label, (policy, sid, field, value) in mutations.items():
            changed = copy.deepcopy(documents)
            statement = next(item for item in changed[policy]["Statement"] if item["Sid"] == sid)
            statement[field] = value
            failures = [item["case"] for item in evaluate(changed) if not item["passed"]]
            (output / f"defect_{label}.json").write_text(json.dumps(changed[policy], indent=2) + "\n", encoding="utf-8")
            defects.append({"defect": label, "caught": bool(failures), "failed_cases": failures})
        for name, document in documents.items():
            (output / f"{name}.json").write_text(json.dumps(document, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        after = {str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        report.update(
            {
                "assertions": checks,
                "defects": defects,
                "source_unchanged": before == after,
                "policy_sizes": {name: non_whitespace_length(document) for name, document in documents.items()},
                "passed": before == after and all(item["passed"] for item in checks) and all(item["caught"] for item in defects),
            }
        )
    except (OSError, ValueError, KeyError) as error:
        report.update({"passed": False, "error": f"{type(error).__name__}: {error}"})
    (output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    (output / "retention.json").write_text(
        json.dumps(
            {
                "class": 5,
                "purpose": "early read policy regression baseline",
                "created": report["timestamp"],
                "end_condition": "A newer accepted run replaces it",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    sys.stdout.write(json.dumps({"passed": report["passed"], "report": str(output / "report.json")}) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
