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
PAYLOADS = "bronze_objects/sha256"
KEY_PARAMETER = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/pseudonym_key_v1"
LARGEST_BUCKET = "a" * 63  # the longest valid bucket name
INLINE_QUOTA = 10240  # all inline policies of one role together, whitespace excluded
MANAGED_QUOTA = 6144  # each managed policy, whitespace excluded


MARGIN = 0.9  # each bound keeps a tenth of its quota free
TEST_NAMESPACES = ("silver_e2e", "gold_e2e", "features_e2e", *TWINS)


def other_version(suffix: str) -> str:
    """The key parameter ARN with another version suffix, for requests that must be refused."""
    return KEY_PARAMETER.removesuffix("v1") + suffix


def load(name: str, values: dict[str, str], defect: tuple[str, str, str] | None = None) -> dict[str, Any]:
    """Render one owner policy template. A planted defect replaces text in its template first."""
    text = (POLICIES / f"{name}.json").read_text()
    if defect is not None and defect[0] == name:
        if defect[1] not in text:
            raise ValueError(f"storage defect has no source match in {name}")
        text = text.replace(defect[1], defect[2])
    return dict(json.loads(Template(text).substitute(values)))


def render(
    names: tuple[str, ...],
    collections: tuple[str, ...],
    *,
    bucket: str = BUCKET,
    raw: bool = False,
    defect: tuple[str, str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Render every policy of the bronze role from its exact template: the inline table policy and the managed source and key policies."""
    roots = ("", "raw/") if raw else ("",)
    table = {
        "BUCKET_ARN": bucket,
        "TABLE_OBJECT_ARNS": json.dumps([f"{bucket}/lakehouse/{name}/*" for name in names]),
        "TABLE_LIST_PREFIXES": json.dumps([f"lakehouse/{name}/*" for name in names]),
    }
    documents = {
        "inline": load("bronze_writer_storage", table, defect),
        "source_lists": load(
            "bronze_writer_source_lists",
            {"BUCKET_ARN": bucket, "SOURCE_LIST_PREFIXES": json.dumps([f"{root}{collection}/*" for root in roots for collection in collections])},
            defect,
        ),
        "key": load("bronze_writer_pseudonym_key", {"PSEUDONYM_KEY_ARN": KEY_PARAMETER}, defect),
    }
    for root in roots:
        arns = [*(f"{bucket}/{root}{collection}/*" for collection in collections), f"{bucket}/{root}{PAYLOADS}/*"]
        documents[f"source_objects_{'raw' if root else 'root'}"] = load("bronze_writer_source_objects", {"SOURCE_OBJECT_ARNS": json.dumps(arns)}, defect)
    return documents


def evaluate(documents: dict[str, dict[str, Any]], names: tuple[str, ...], collections: tuple[str, ...], *, raw: bool = False) -> list[str]:
    """Check independently listed allowed and forbidden storage requests. The raw root is readable only when its switch is on."""
    failures = []

    def check(action: str, resource: str, allowed: bool, *, prefix: str | None = None) -> None:
        context = {} if prefix is None else {"s3:prefix": (prefix,)}
        if decide(list(documents.values()), Request(action, resource, context)).allowed != allowed:
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
    payload = f"{BUCKET}/{PAYLOADS}/" + "ab" * 32
    check("s3:GetObjectVersion", payload, True)
    raw_payload = f"{BUCKET}/raw/{PAYLOADS}/" + "ab" * 32
    check("s3:GetObjectVersion", raw_payload, raw)
    for action in ("s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:GetObject"):
        check(action, raw_payload, False)
    for collection in collections:
        check("s3:GetObjectVersion", f"{BUCKET}/raw/{collection}/objects/source.csv", raw)
        check("s3:ListBucketVersions", BUCKET, raw, prefix=f"raw/{collection}/manifests/")
        for action in ("s3:PutObject", "s3:DeleteObject", "s3:GetObject"):
            check(action, f"{BUCKET}/raw/{collection}/objects/source.csv", False)
    for action in ("s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:GetObject"):
        check(action, payload, False)
    check("s3:GetObjectVersion", f"{BUCKET}/bronze_objects/other/" + "ab" * 32, False)
    check("s3:ListBucketVersions", BUCKET, False, prefix=f"{PAYLOADS}/")
    check("s3:GetObjectVersion", f"{BUCKET}/example/not_mapped/source.csv", False)
    check("s3:ListBucketVersions", BUCKET, False, prefix="example/not_mapped/")
    check("s3:ListBucket", BUCKET, False, prefix="")
    check("s3:GetBucketLocation", BUCKET, False)
    check("ssm:GetParameters", KEY_PARAMETER, True)
    for action in ("ssm:PutParameter", "ssm:GetParameter", "ssm:GetParametersByPath", "ssm:DeleteParameter"):
        check(action, KEY_PARAMETER, False)
    check("ssm:GetParameters", other_version("v2"), False)
    check("ssm:GetParameters", KEY_PARAMETER.replace("pseudonym_key_v1", "broker_token"), False)
    return sorted(failures)


def storage_policy_scope(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Check both identities against the real policy template without reading deployment values."""
    from scripts.lakehouse.bronze_writer_cases import require

    table_map = json.loads((ROOT / "config/lakehouse/bronze_tables.json").read_text())
    collections = tuple(sorted({entry["collection"] for entry in table_map["tables"]}))
    production = render(PRODUCTION, collections)
    qualification = render(TWINS, ("cms/hospitals",))
    failures = evaluate(production, PRODUCTION, collections) + evaluate(qualification, TWINS, ("cms/hospitals",))
    failures += evaluate(render(PRODUCTION, collections, raw=True), PRODUCTION, collections, raw=True)
    require(not failures, "storage policy scope differs: " + ", ".join(failures[:3]))
    largest = render(PRODUCTION, collections, bucket=f"arn:aws:s3:::{LARGEST_BUCKET}", raw=True)
    stage = Template((POLICIES / "job_stage_read.json").read_text()).substitute(DATA_BUCKET_NAME=LARGEST_BUCKET, TASK_USER_ID="${aws:userid}")
    inline = len(json.dumps(largest["inline"], separators=(",", ":"))) + len(json.dumps(json.loads(stage), separators=(",", ":")))
    require(
        inline <= int(INLINE_QUOTA * MARGIN), f"bronze inline policies together are {inline} characters at the longest bucket name, over the role quota margin"
    )
    for name, document in largest.items():
        if name != "inline":
            size = len(json.dumps(document, separators=(",", ":")))
            require(
                size <= int(MANAGED_QUOTA * MARGIN),
                f"managed bronze policy {name} is {size} characters at the longest bucket name, over the managed quota margin",
            )
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
    reader_inputs = {
        "BUCKET_ARN": BUCKET,
        "TEST_OBJECT_ARNS": json.dumps([f"{BUCKET}/lakehouse/{name}/*" for name in TEST_NAMESPACES]),
        "TEST_LIST_PREFIXES": json.dumps([f"lakehouse/{name}/*" for name in TEST_NAMESPACES]),
        "BRONZE_TEST_OBJECT_ARNS": json.dumps([f"{BUCKET}/lakehouse/{name}/*" for name in TWINS]),
        "BRONZE_TEST_LIST_PREFIXES": json.dumps([f"lakehouse/{name}/*" for name in TWINS]),
    }
    reader = json.loads(Template((POLICIES / "job_reader_e2e.json").read_text()).substitute(reader_inputs))
    for name in TEST_NAMESPACES:
        resource = f"{BUCKET}/lakehouse/{name}/table/data.parquet"
        context = {"s3:prefix": (f"lakehouse/{name}/",)}
        require(decide([reader], Request("s3:GetObject", resource)).allowed, "reader cannot read a qualification namespace")
        require(decide([reader], Request("s3:ListBucket", BUCKET, context)).allowed, "reader cannot list a qualification namespace")
        for action in ("s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:DeleteObjectVersion"):
            require(not decide([reader], Request(action, resource)).allowed, "reader can mutate a qualification namespace")
        require(
            decide([reader], Request("s3:GetObjectVersion", resource)).allowed == (name in TWINS),
            "reader version reads must cover only the four bronze twins",
        )
        require(
            decide([reader], Request("s3:ListBucketVersions", BUCKET, context)).allowed == (name in TWINS),
            "reader cannot find the version of a completion metadata object or can list versions outside the bronze twins",
        )
    for name in (*PRODUCTION, "silver", "gold", "features", "catalog_backups", "catalog_journal"):
        resource = f"{BUCKET}/lakehouse/{name}/table/data.parquet"
        for action in ("s3:GetObject", "s3:GetObjectVersion"):
            require(not decide([reader], Request(action, resource)).allowed, "reader extension includes a canonical prefix")
        for action in ("s3:ListBucket", "s3:ListBucketVersions"):
            require(
                not decide([reader], Request(action, BUCKET, {"s3:prefix": (f"lakehouse/{name}/",)})).allowed,
                "reader extension can list a canonical prefix",
            )
    out.mkdir(parents=True, exist_ok=False)
    for label, documents in (("production", production), ("qualification", qualification)):
        for name, document in documents.items():
            (out / f"{label}_{name}_policy.json").write_text(json.dumps(document, indent=2) + "\n")
    (out / "reader_policy.json").write_text(json.dumps(reader, indent=2) + "\n")
    return {"production_prefixes": list(PRODUCTION), "qualification_prefixes": list(TWINS), "source_collections": list(collections), "evaluator": "offline IAM"}


def planted_storage_defects(out: Path) -> list[dict[str, Any]]:
    """Require independent storage-scope assertions to catch widened or missing policy grants."""
    defects = (
        ("source_write_allowed", ("bronze_writer_source_objects", '"s3:GetObjectVersion"', '"s3:GetObjectVersion", "s3:PutObject"')),
        ("all_lakehouse_paths_writable", ("bronze_writer_storage", "${TABLE_OBJECT_ARNS}", json.dumps([f"{BUCKET}/lakehouse/*"]))),
        (
            "dictionary_storage_missing",
            (
                "bronze_writer_storage",
                "${TABLE_OBJECT_ARNS}",
                json.dumps([f"{BUCKET}/lakehouse/bronze/*", f"{BUCKET}/lakehouse/smoke_check/*", f"{BUCKET}/lakehouse/duckdb_view_check/*"]),
            ),
        ),
        ("key_write_allowed", ("bronze_writer_pseudonym_key", '"ssm:GetParameters"', '"ssm:GetParameters", "ssm:PutParameter"')),
        ("key_single_read_allowed", ("bronze_writer_pseudonym_key", '"ssm:GetParameters"', '"ssm:GetParameters", "ssm:GetParameter"')),
        ("key_version_wildcard", ("bronze_writer_pseudonym_key", "${PSEUDONYM_KEY_ARN}", KEY_PARAMETER.replace("pseudonym_key_v1", "pseudonym_key_*"))),
    )
    results = []
    for name, defect in defects:
        documents = render(PRODUCTION, ("cms/hospitals",), defect=defect)
        failures = evaluate(documents, PRODUCTION, ("cms/hospitals",))
        folder = out / name
        folder.mkdir(parents=True, exist_ok=False)
        for label, document in documents.items():
            (folder / f"{label}_policy.json").write_text(json.dumps(document, indent=2) + "\n")
        results.append({"defect": name, "status": "caught" if failures else "survived", "caught_by": failures})
    return results
