"""Exercise the S3 grants of the training roles without AWS calls.

The MLflow service role reads and writes the ``mlflow/`` prefix only. The training job role reads the ``features/`` namespace and the Feast registry
under ``feast/`` and nothing else of the lakehouse. Each grant is checked against requests listed here, apart from the rendered policy resources,
and each scope mutation of the policy must fail at least one request.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, decide

BUCKET = "arn:aws:s3:::example-project-ci-bucket"
OTHER_BUCKET = "arn:aws:s3:::another-example-bucket"
WRITE_ACTIONS = ("s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts")
READ_ACTIONS = ("s3:GetObject", "s3:GetObjectVersion")
Case = tuple[str, str, str, bool, dict[str, tuple[str, ...]]]


@dataclass(frozen=True)
class Grant:
    """One policy: the object actions it allows, the key prefixes they cover and sample keys that must stay refused."""

    policy: str
    actions: tuple[str, ...]
    prefixes: tuple[str, ...]
    sample: str
    refused_keys: tuple[str, ...]
    refused_list_prefixes: tuple[str, ...]
    refused_actions: tuple[str, ...]


GRANTS = (
    Grant(
        "mlflow_artifacts",
        WRITE_ACTIONS,
        ("mlflow/",),
        "mlflow/3/0a1b2c3d4e5f60718293a4b5c6d7e8f9/artifacts/model/MLmodel",
        (
            "lakehouse/gold/table/metadata/v1.metadata.json",
            "lakehouse/features/table/data/part-0.parquet",
            "lakehouse/run_stage/run/tries/one/out",
            "dvc/files/md5/6c/583397875b44dd36bdaf075486a2cf",
            "mlflow",
            "mlflows/file",
            "other/mlflow/file",
        ),
        ("", "lakehouse/", "dvc/", "mlflow", "mlflow-private/x", "mlflows", "cms/mlflow/", "../mlflow/"),
        ("s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:PutObjectAcl", "s3:PutObjectTagging", "s3:ListBucketVersions"),
    ),
    Grant(
        "job_features_read",
        READ_ACTIONS,
        ("lakehouse/features/", "lakehouse/feast/"),
        "lakehouse/features/int_hospital_spine/data/00000-0-a.parquet",
        (
            "lakehouse/gold/table/data/part-0.parquet",
            "lakehouse/silver/table/data/part-0.parquet",
            "lakehouse/bronze/table/data/part-0.parquet",
            "lakehouse/features_e2e/table/data/part-0.parquet",
            "lakehouse/featuresx/file",
            "lakehouse/run_stage/run/tries/one/out",
            "mlflow/3/run/artifacts/model/MLmodel",
            "dvc/files/md5/6c/583397875b44dd36bdaf075486a2cf",
        ),
        ("", "lakehouse/", "lakehouse/gold/", "lakehouse/features", "lakehouse/features_e2e/", "lakehouse/featuresx/", "../lakehouse/features/"),
        ("s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:AbortMultipartUpload", "s3:PutObjectAcl", "s3:ListBucketVersions"),
    ),
)
POLICIES = tuple(grant.policy for grant in GRANTS)


def cases(grant: Grant) -> list[Case]:
    """Specify each required grant and refusal independently of the rendered policy resources."""
    result: list[Case] = []

    def add(label: str, action: str, resource: str, expected: bool, context: dict[str, tuple[str, ...]] | None = None) -> None:
        result.append((f"{grant.policy}: {label}", action, resource, expected, context or {}))

    sample = f"{BUCKET}/{grant.sample}"
    for action in grant.actions:
        add(f"{action} on a granted object", action, sample, True)
        add(f"{action} in the other bucket", action, sample.replace(BUCKET, OTHER_BUCKET), False)
        for key in grant.refused_keys:
            add(f"{action} on {key}", action, f"{BUCKET}/{key}", False)
    for prefix in grant.prefixes:
        add(f"list under {prefix}", "s3:ListBucket", BUCKET, True, {"s3:prefix": (prefix, f"{prefix}sub/")})
    for prefix in grant.refused_list_prefixes:
        add(f"list outside the grant with prefix {prefix!r}", "s3:ListBucket", BUCKET, False, {"s3:prefix": (prefix,)})
    add("list without a prefix", "s3:ListBucket", BUCKET, False)
    add("list in the other bucket", "s3:ListBucket", OTHER_BUCKET, False, {"s3:prefix": (grant.prefixes[0],)})
    for action in grant.refused_actions:
        add(f"no {action} on a granted object", action, sample, False)
    add("no bucket-location grant", "s3:GetBucketLocation", BUCKET, False)
    return result


def evaluate(grant: Grant, document: dict[str, Any]) -> list[dict[str, Any]]:
    """Evaluate one rendered policy against every request."""
    observations = []
    for label, action, resource, expected, context in cases(grant):
        decision = decide([document], Request(action, resource, context))
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


def verify(base: dict[str, dict[str, Any]], output: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Evaluate each rendered policy and retain each scope mutation; every mutation must fail at least one case."""
    checks: list[dict[str, Any]] = []
    defects: list[dict[str, Any]] = []
    for grant in GRANTS:
        document = base[grant.policy]
        checks.extend(evaluate(grant, document))
        for statement in document["Statement"]:
            variants: list[tuple[str, str, Any]] = [("widen_resource", "Resource", "*")]
            if "Condition" in statement:
                variants += [("drop_condition", "Condition", None), ("any_prefix", "Condition", {"StringLike": {"s3:prefix": ["*"]}})]
            else:
                variants += [("add_other_resource", "Resource", [*_listed(statement["Resource"]), f"{BUCKET}/lakehouse/*"])]
                variants += [("add_delete", "Action", [*_listed(statement["Action"]), "s3:DeleteObject"])]
            for label, field, value in variants:
                changed = copy.deepcopy(document)
                target = next(item for item in changed["Statement"] if item["Sid"] == statement["Sid"])
                if value is None:
                    target.pop(field)
                else:
                    target[field] = value
                failures = [item["case"] for item in evaluate(grant, changed) if not item["passed"]]
                name = f"{grant.policy}_{statement['Sid']}_{label}"
                (output / f"defect_{name}.json").write_text(json.dumps(changed, indent=2) + "\n")
                defects.append({"defect": name, "caught": bool(failures), "failed_cases": failures})
    return checks, defects


def _listed(value: Any) -> list[str]:
    return [value] if isinstance(value, str) else list(value)
