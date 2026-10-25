"""Evaluate the bronze policy templates with synthetic resources and the project's IAM evaluator."""

from __future__ import annotations

import json
from pathlib import Path
from string import Template
from types import ModuleType
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, decide

ROOT = Path(__file__).resolve().parents[2]
POLICIES = ROOT / "infra/iam/owner_policies"
BUCKET = "arn:aws:s3:::example-data-bucket"
PRODUCTION = ("bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check")
TWINS = ("bronze_e2e", "bronze_dictionary_e2e", "smoke_check_e2e", "duckdb_view_check_e2e")


def values(names: tuple[str, ...], collections: tuple[str, ...]) -> dict[str, str]:
    """Return synthetic Terraform-equivalent inputs for the storage template."""
    return {
        "BUCKET_ARN": BUCKET,
        "TABLE_OBJECT_ARNS": json.dumps([f"{BUCKET}/lakehouse/{name}/*" for name in names]),
        "TABLE_LIST_PREFIXES": json.dumps([f"lakehouse/{name}/*" for name in names]),
        "SOURCE_OBJECT_ARNS": json.dumps([f"{BUCKET}/{collection}/*" for collection in collections]),
        "SOURCE_LIST_PREFIXES": json.dumps([f"{collection}/*" for collection in collections]),
    }


def render(names: tuple[str, ...], collections: tuple[str, ...]) -> dict[str, Any]:
    """Render the exact storage template without reading deployment configuration."""
    return dict(json.loads(Template((POLICIES / "bronze_writer_storage.json").read_text()).substitute(values(names, collections))))


def evaluate(document: dict[str, Any], names: tuple[str, ...], collections: tuple[str, ...]) -> list[str]:
    """Check independently listed allowed and forbidden storage requests."""
    failures = []

    def check(action: str, resource: str, allowed: bool, *, prefix: str | None = None) -> None:
        context = {} if prefix is None else {"s3:prefix": (prefix,)}
        if decide([document], Request(action, resource, context)).allowed != allowed:
            failures.append(f"{action} {'allowed' if allowed else 'denied'} at {resource.removeprefix(BUCKET)}")

    for name in names:
        for action in ("s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"):
            check(action, f"{BUCKET}/lakehouse/{name}/table/data.parquet", True)
        check("s3:GetObjectVersion", f"{BUCKET}/lakehouse/{name}/table/data.parquet", False)
        check("s3:ListBucket", BUCKET, True, prefix=f"lakehouse/{name}/")
        check("s3:ListBucket", BUCKET, True, prefix=f"lakehouse/{name}/table/")
    others = set((*PRODUCTION, *TWINS, "silver", "gold", "features", "catalog_journal", "run_stage", "bronze_dictionary_extra")) - set(names)
    for name in others:
        for action in ("s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"):
            check(action, f"{BUCKET}/lakehouse/{name}/object", False)
        check("s3:ListBucket", BUCKET, False, prefix=f"lakehouse/{name}/")
    for collection in collections:
        for key in (f"{collection}/objects/source.csv", f"{collection}/manifests/capture/manifest.json"):
            check("s3:GetObjectVersion", f"{BUCKET}/{key}", True)
            for action in ("s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:GetObject"):
                check(action, f"{BUCKET}/{key}", False)
        check("s3:ListBucketVersions", BUCKET, True, prefix=f"{collection}/manifests/")
    check("s3:GetObjectVersion", f"{BUCKET}/example/not_mapped/source.csv", False)
    check("s3:ListBucketVersions", BUCKET, False, prefix="example/not_mapped/")
    check("s3:ListBucket", BUCKET, False, prefix="")
    check("s3:GetBucketLocation", BUCKET, False)
    return sorted(failures)


def storage_policy_scope(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Check both identities against the real policy template without reading deployment values."""
    from scripts.lakehouse.bronze_writer_cases import require

    table_map = json.loads((ROOT / "config/lakehouse/bronze_tables.json").read_text())
    collections = tuple(sorted({entry["collection"] for entry in table_map["tables"]}))
    production = render(PRODUCTION, collections)
    qualification = render(TWINS, ("cms/hospitals",))
    failures = evaluate(production, PRODUCTION, collections) + evaluate(qualification, TWINS, ("cms/hospitals",))
    require(not failures, "storage policy scope differs: " + ", ".join(failures[:3]))
    require(len(json.dumps(production, separators=(",", ":"))) <= 10240, "bronze inline policy exceeds the IAM role policy limit")
    account = "1" * 12
    parameter_prefix = f"arn:aws:ssm:us-west-2:{account}:parameter/healthcare_hai_prediction/"
    pair = [parameter_prefix + "aws_polaris_bronze_client_id", parameter_prefix + "aws_polaris_bronze_client_secret"]
    execution = json.loads(Template((POLICIES / "bronze_execution.json").read_text()).substitute(BRONZE_PARAMETER_ARNS=json.dumps(pair)))
    role = f"arn:aws:iam::{account}:role/example_project_job_write_bronze_task_dev"
    project = json.loads(Template((POLICIES / "bronze_project_delta.json").read_text()).substitute(BRONZE_TASK_ROLE_ARN=json.dumps(role)))
    for parameter in pair:
        require(decide([execution], Request("ssm:GetParameters", parameter)).allowed, "ECS execution role cannot inject the bronze pair")
        require(not decide([project], Request("ssm:GetParameters", parameter)).allowed, "project delta grants a writer-pair read")
    for name in ("polaris_root_secret", "aws_polaris_reader_client_id", "aws_polaris_e2e_client_secret"):
        require(not decide([execution], Request("ssm:GetParameters", parameter_prefix + name)).allowed, "bronze execution delta reads another catalog identity")
    require(
        decide([project], Request("iam:PassRole", role, {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)})).allowed,
        "project delta cannot pass the bronze role to ECS",
    )
    require(
        not decide([project], Request("iam:PassRole", role, {"iam:PassedToService": ("ec2.amazonaws.com",)})).allowed,
        "project delta can pass the bronze role outside ECS",
    )
    reader = json.loads(Template((POLICIES / "bronze_reader_storage.json").read_text()).substitute(values(TWINS, ())))
    for name in TWINS:
        resource = f"{BUCKET}/lakehouse/{name}/table/data.parquet"
        require(decide([reader], Request("s3:GetObject", resource)).allowed, "reader cannot verify an affected bronze namespace")
        require(not decide([reader], Request("s3:PutObject", resource)).allowed, "reader can write an affected bronze namespace")
        require(
            decide([reader], Request("s3:ListBucketVersions", BUCKET, {"s3:prefix": (f"lakehouse/{name}/",)})).allowed,
            "reader cannot find the version of a completion metadata object",
        )
    for name in PRODUCTION:
        resource = f"{BUCKET}/lakehouse/{name}/table/data.parquet"
        require(not decide([reader], Request("s3:GetObject", resource)).allowed, "reader delta includes a canonical bronze prefix")
        require(
            not decide([reader], Request("s3:ListBucket", BUCKET, {"s3:prefix": (f"lakehouse/{name}/",)})).allowed,
            "reader delta can list a canonical bronze prefix",
        )
    out.mkdir(parents=True, exist_ok=False)
    (out / "production_policy.json").write_text(json.dumps(production, indent=2) + "\n")
    (out / "qualification_policy.json").write_text(json.dumps(qualification, indent=2) + "\n")
    return {"production_prefixes": list(PRODUCTION), "qualification_prefixes": list(TWINS), "source_collections": list(collections), "evaluator": "offline IAM"}


def planted_storage_defects(out: Path) -> list[dict[str, Any]]:
    """Require independent storage-scope assertions to catch widened or missing policy grants."""
    source = (POLICIES / "bronze_writer_storage.json").read_text()
    defects = (
        ("source_write_allowed", '"s3:GetObjectVersion"', '"s3:GetObjectVersion", "s3:PutObject"'),
        ("all_lakehouse_paths_writable", "${TABLE_OBJECT_ARNS}", json.dumps([f"{BUCKET}/lakehouse/*"])),
        (
            "dictionary_storage_missing",
            "${TABLE_OBJECT_ARNS}",
            json.dumps([f"{BUCKET}/lakehouse/bronze/*", f"{BUCKET}/lakehouse/smoke_check/*", f"{BUCKET}/lakehouse/duckdb_view_check/*"]),
        ),
    )
    results = []
    for name, before, after in defects:
        if before not in source:
            raise ValueError(f"storage defect {name} has no source match")
        document = json.loads(Template(source.replace(before, after)).substitute(values(PRODUCTION, ("cms/hospitals",))))
        failures = evaluate(document, PRODUCTION, ("cms/hospitals",))
        folder = out / name
        folder.mkdir(parents=True, exist_ok=False)
        (folder / "policy.json").write_text(json.dumps(document, indent=2) + "\n")
        results.append({"defect": name, "status": "caught" if failures else "survived", "caught_by": failures})
    return results
