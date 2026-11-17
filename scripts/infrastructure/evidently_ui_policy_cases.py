"""Exercise the Evidently UI restore grant without AWS calls: the UI task role reads and lists ``drift_demo/`` and writes nothing."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, decide

BUCKET = "arn:aws:s3:::example-project-ci-bucket"
OTHER_BUCKET = "arn:aws:s3:::another-example-bucket"
POLICIES = ("evidently_ui_restore_read",)
POLICY = POLICIES[0]
RUN = "20261117T120000Z-0a1b2c3d"
OBJECT = f"{BUCKET}/drift_demo/{RUN}/snapshot_HAI_1.json"
Case = tuple[str, str, str, bool, dict[str, tuple[str, ...]]]


def cases() -> list[Case]:
    """Specify each grant and refusal independently of the rendered policy. The restore may read a live run's files and list the prefix, and do nothing else."""
    result: list[Case] = []

    def add(label: str, action: str, resource: str, expected: bool, context: dict[str, tuple[str, ...]] | None = None) -> None:
        result.append((label, action, resource, expected, context or {}))

    for name in ("_complete.json", "snapshot_HAI_1.json", "project_HAI_1.json", "drift_HAI_1.json"):
        add(f"read {name} of a live run", "s3:GetObject", f"{BUCKET}/drift_demo/{RUN}/{name}", True)
    for prefix in ("drift_demo/", f"drift_demo/{RUN}/"):
        add(f"list under {prefix}", "s3:ListBucket", BUCKET, True, {"s3:prefix": (prefix,)})
    for action in ("s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutObjectAcl", "s3:PutObjectTagging", "s3:AbortMultipartUpload"):
        add(f"no {action} on a live run object", action, OBJECT, False)
    for action in ("s3:GetObjectVersion", "s3:ListBucketVersions", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"):
        add(f"no {action}", action, OBJECT if "Object" in action else BUCKET, False)
    for key in (
        f"drift/{RUN}/drift_HAI_1.json",
        f"drift/{RUN}/_complete.json",
        "drift_demo",
        "drift_demos/other.json",
        "drift_demo_private/x",
        "lakehouse/gold/table/metadata/v1.metadata.json",
        "dvc/files/md5/6c/583397875b44dd36bdaf075486a2cf",
        "cms/hospitals/data/input.csv",
        "other/drift_demo/file",
    ):
        add(f"no read of {key}", "s3:GetObject", f"{BUCKET}/{key}", False)
    add("no read in the other bucket", "s3:GetObject", OBJECT.replace(BUCKET, OTHER_BUCKET), False)
    for prefix in ("", "drift/", "drift_demo", "drift_demos/", "lakehouse/", "dvc/", "../drift_demo/", "other/drift_demo/"):
        add(f"list outside drift_demo/ with prefix {prefix!r}", "s3:ListBucket", BUCKET, False, {"s3:prefix": (prefix,)})
    add("list without a prefix", "s3:ListBucket", BUCKET, False)
    add("list in the other bucket", "s3:ListBucket", OTHER_BUCKET, False, {"s3:prefix": ("drift_demo/",)})
    return result


def evaluate(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Evaluate the policy against every request."""
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
    """Evaluate the rendered policy and retain each planted defect (write access, a wider prefix or resource, a dropped condition); each must fail a case."""
    document = base[POLICY]
    checks = evaluate(document)
    defects: list[dict[str, Any]] = []
    for statement in document["Statement"]:
        listing = "Condition" in statement
        variants: list[tuple[str, str, Any]] = [("widen_resource", "Resource", "*")]
        if listing:
            variants += [("drop_condition", "Condition", None), ("any_prefix", "Condition", {"StringLike": {"s3:prefix": ["*"]}})]
            variants += [("prefix_wider", "Condition", {"StringLike": {"s3:prefix": ["drift*"]}})]
            variants += [("drift_prefix", "Condition", {"StringLike": {"s3:prefix": ["drift/*", "drift_demo/*"]}})]
        else:
            variants += [("add_put", "Action", [*_listed(statement["Action"]), "s3:PutObject"])]
            variants += [("add_delete", "Action", [*_listed(statement["Action"]), "s3:DeleteObject"])]
            variants += [("resource_wider", "Resource", f"{BUCKET}/drift*"), ("whole_bucket", "Resource", f"{BUCKET}/*")]
            variants += [("add_gate_prefix", "Resource", [*_listed(statement["Resource"]), f"{BUCKET}/drift/*"])]
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
