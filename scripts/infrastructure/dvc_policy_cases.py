"""Exercise the DVC baseline grant of the training job role (decision 20): read and write on the ``dvc/`` prefix only, without AWS calls."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, decide

BUCKET = "arn:aws:s3:::example-project-ci-bucket"
OTHER_BUCKET = "arn:aws:s3:::another-example-bucket"
POLICY = "job_dvc_baseline"
OBJECT = f"{BUCKET}/dvc/files/md5/6c/583397875b44dd36bdaf075486a2cf"
OBJECT_ACTIONS = ("s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts")
Case = tuple[str, str, str, bool, dict[str, tuple[str, ...]]]


def cases() -> list[Case]:
    """Specify each required grant and refusal independently of the rendered policy resources."""
    result: list[Case] = []

    def add(label: str, action: str, resource: str, expected: bool, context: dict[str, tuple[str, ...]] | None = None) -> None:
        result.append((label, action, resource, expected, context or {}))

    for action in OBJECT_ACTIONS:
        add(f"dvc object {action}", action, OBJECT, True)
        add(f"dvc object in the other bucket {action}", action, OBJECT.replace(BUCKET, OTHER_BUCKET), False)
    for prefix in ("dvc/", "dvc/files/md5/6c/", "dvc"):
        add(f"list under {prefix}", "s3:ListBucket", BUCKET, True, {"s3:prefix": (prefix,)})
    for prefix in ("", "lakehouse/", "lakehouse/gold/", "dvc-other/", "dvcs", "cms/dvc/", "../dvc/"):
        add(f"list outside dvc/ with prefix {prefix!r}", "s3:ListBucket", BUCKET, False, {"s3:prefix": (prefix,)})
    add("list without a prefix", "s3:ListBucket", BUCKET, False)
    add("list in the other bucket", "s3:ListBucket", OTHER_BUCKET, False, {"s3:prefix": ("dvc/",)})
    for action in ("s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:PutObjectAcl", "s3:PutObjectTagging", "s3:ListBucketVersions"):
        add(f"no {action} on a dvc object", action, OBJECT, False)
    add("no version listing under dvc/", "s3:ListBucketVersions", BUCKET, False, {"s3:prefix": ("dvc/",)})
    add("no bucket-location grant", "s3:GetBucketLocation", BUCKET, False)
    for key in (
        "lakehouse/gold/table/metadata/v1.metadata.json",
        "lakehouse/run_stage/run/tries/one/out",
        "cms/hospitals/data/input.csv",
        "dvc",
        "dvcs/file",
        "other/dvc/file",
    ):
        for action in OBJECT_ACTIONS:
            add(f"no {action} on {key}", action, f"{BUCKET}/{key}", False)
    return result


def evaluate(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Evaluate the training role's DVC policy against every request."""
    observations = []
    for label, action, resource, expected, context in cases():
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
    """Evaluate the rendered policy and retain each scope mutation; every mutation must fail at least one case."""
    document = base[POLICY]
    checks = evaluate(document)
    defects = []
    for statement in document["Statement"]:
        variants: list[tuple[str, str, Any]] = [("widen_resource", "Resource", "*")]
        if "Condition" in statement:
            variants += [("drop_condition", "Condition", None), ("any_prefix", "Condition", {"StringLike": {"s3:prefix": ["*"]}})]
        else:
            variants += [("add_lakehouse_resource", "Resource", [*_listed(statement["Resource"]), f"{BUCKET}/lakehouse/*"])]
            variants += [("add_delete", "Action", [*_listed(statement["Action"]), "s3:DeleteObject"])]
        for label, field, value in variants:
            changed = copy.deepcopy(document)
            target = next(item for item in changed["Statement"] if item["Sid"] == statement["Sid"])
            if value is None:
                target.pop(field)
            else:
                target[field] = value
            failures = [item["case"] for item in evaluate(changed) if not item["passed"]]
            name = f"{POLICY}_{statement['Sid']}_{label}"
            (output / f"defect_{name}.json").write_text(json.dumps(changed, indent=2) + "\n")
            defects.append({"defect": name, "caught": bool(failures), "failed_cases": failures})
    return checks, defects


def _listed(value: Any) -> list[str]:
    return [value] if isinstance(value, str) else list(value)
