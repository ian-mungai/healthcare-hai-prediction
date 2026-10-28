"""Exercise the prepared test writer and its read, injection and project-policy boundaries without AWS calls."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from string import Template
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, decide

BUCKET = "arn:aws:s3:::example-project-ci-bucket"
OTHER_BUCKET = "arn:aws:s3:::another-example-bucket"
PARAMETER = "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/"
ROLE = "arn:aws:iam::111111111111:role/example_project_job_write_e2e_task_dev"
BRONZE_TWINS = ("bronze_e2e", "bronze_dictionary_e2e", "smoke_check_e2e", "duckdb_view_check_e2e")
TEST_NAMESPACES = ("silver_e2e", "gold_e2e", "features_e2e", *BRONZE_TWINS)
PRODUCTION = ("silver", "gold", "features", "bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check", "catalog_backups")
WRITER_ACTIONS = ("s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts")
POLICIES = ("job_writer_e2e_storage", "job_reader_e2e", "job_writer_e2e_execution")
Case = tuple[str, str, str, str, bool, dict[str, tuple[str, ...]]]


def cases() -> list[Case]:
    """Specify each required grant and refusal independently of rendered policy resources."""
    result: list[Case] = []

    def add(label: str, actor: str, action: str, resource: str, expected: bool, context: dict[str, tuple[str, ...]] | None = None) -> None:
        result.append((label, actor, action, resource, expected, context or {}))

    for namespace in TEST_NAMESPACES:
        resource = f"{BUCKET}/lakehouse/{namespace}/table/metadata/v1.metadata.json"
        for action in WRITER_ACTIONS:
            add(f"test writer {namespace} {action}", "writer", action, resource, True)
        add(f"test backup {namespace}", "writer", "s3:PutObject", f"{BUCKET}/lakehouse/{namespace}/catalog_backups/head.json", True)
        add(f"reader test {namespace}", "reader", "s3:GetObject", resource, True)
        for action in ("s3:ListBucket", "s3:ListBucketVersions"):
            context: dict[str, tuple[str, ...]] = {"s3:prefix": (f"lakehouse/{namespace}/",)}
            add(f"writer list {namespace} {action}", "writer", action, BUCKET, action == "s3:ListBucket", context)
            add(f"reader list {namespace} {action}", "reader", action, BUCKET, action == "s3:ListBucket" or namespace in BRONZE_TWINS, context)
        add(f"reader version {namespace}", "reader", "s3:GetObjectVersion", resource, namespace in BRONZE_TWINS)
        add(f"writer has no table version {namespace}", "writer", "s3:GetObjectVersion", resource, False)
        for action in (*WRITER_ACTIONS[1:], "s3:DeleteObjectVersion"):
            add(f"reader cannot mutate {namespace} {action}", "reader", action, resource, False)
        add(f"writer cannot delete version {namespace}", "writer", "s3:DeleteObjectVersion", resource, False)
        for actor in ("writer", "reader"):
            add(f"{actor} excludes lookalike {namespace}", actor, "s3:GetObject", f"{BUCKET}/lakehouse/{namespace}_other/table/file", False)
    for namespace in (*PRODUCTION, "another_e2e", "catalog_journal", "catalog_migration"):
        resource = f"{BUCKET}/lakehouse/{namespace}/table/metadata/v1.metadata.json"
        for action in WRITER_ACTIONS:
            add(f"writer excludes {namespace} {action}", "writer", action, resource, False)
        for action in ("s3:ListBucket", "s3:ListBucketVersions"):
            add(f"writer excludes list {namespace} {action}", "writer", action, BUCKET, False, {"s3:prefix": (f"lakehouse/{namespace}/",)})
        add(f"reader delta excludes {namespace}", "reader_delta", "s3:GetObject", resource, False)
    for actor in ("writer", "reader_delta"):
        for action in ("s3:ListBucket", "s3:ListBucketVersions"):
            for prefix in ("", "lakehouse/", "lakehouse/gold", "lakehouse/gold_e2e_other/", "cms/other/", "cms/hospitals"):
                add(f"{actor} list boundary {action} {prefix}", actor, action, BUCKET, False, {"s3:prefix": (prefix,)})
            add(f"{actor} no list without prefix {action}", actor, action, BUCKET, False)
            add(f"{actor} no other bucket list {action}", actor, action, OTHER_BUCKET, False, {"s3:prefix": ("lakehouse/bronze_e2e/",)})
        for action in (*WRITER_ACTIONS, "s3:GetObjectVersion"):
            add(f"{actor} no other bucket {action}", actor, action, f"{OTHER_BUCKET}/lakehouse/bronze_e2e/table/file", False)
        add(f"{actor} no bucket-location grant", actor, "s3:GetBucketLocation", BUCKET, False)
    for key in ("cms/hospitals/data/input.csv", "cms/hospitals/manifests/collection.json"):
        add(f"writer pinned source {key}", "writer", "s3:GetObjectVersion", f"{BUCKET}/{key}", True)
        for action in WRITER_ACTIONS:
            add(f"writer source boundary {key} {action}", "writer", action, f"{BUCKET}/{key}", False)
    for prefix in ("cms/hospitals/", "cms/hospitals/manifests/", "cms/hospitals/data/"):
        add(f"writer source version listing {prefix}", "writer", "s3:ListBucketVersions", BUCKET, True, {"s3:prefix": (prefix,)})
        add(f"writer source no current listing {prefix}", "writer", "s3:ListBucket", BUCKET, False, {"s3:prefix": (prefix,)})
    add("writer no source versions in another bucket", "writer", "s3:ListBucketVersions", OTHER_BUCKET, False, {"s3:prefix": ("cms/hospitals/",)})
    add("writer no source object in another bucket", "writer", "s3:GetObjectVersion", f"{OTHER_BUCKET}/cms/hospitals/data/input.csv", False)
    for prefix in ("cms/other/", "cms/hospitals_other/", "cms/hospitals"):
        add(f"writer no other source {prefix}", "writer", "s3:GetObjectVersion", f"{BUCKET}/{prefix}file", False)
        add(f"writer no other source versions {prefix}", "writer", "s3:ListBucketVersions", BUCKET, False, {"s3:prefix": (prefix,)})
    for name in ("aws_reader_id", "aws_reader_secret", "aws_polaris_e2e_client_id", "aws_polaris_e2e_client_secret"):
        add(f"execution injects application {name}", "execution", "ssm:GetParameters", PARAMETER + name, True)
        add(f"execution excludes single parameter {name}", "execution", "ssm:GetParameter", PARAMETER + name, False)
        for actor in ("writer", "reader"):
            add(f"{actor} cannot fetch parameter {name}", actor, "ssm:GetParameters", PARAMETER + name, False)
    for name in ("aws_reader_id", "aws_reader_secret"):
        add(f"project reads reader only {name}", "project", "ssm:GetParameter", PARAMETER + name, True)
        add(f"project cannot bulk-read reader {name}", "project", "ssm:GetParameters", PARAMETER + name, False)
        add(f"project cannot write reader {name}", "project", "ssm:PutParameter", PARAMETER + name, False)
    for principal in ("e2e", "silver", "gold", "features", "bronze"):
        for suffix in ("client_id", "client_secret"):
            name = f"aws_polaris_{principal}_{suffix}"
            for folder in (PARAMETER, "arn:aws:ssm:eu-west-1:111111111111:parameter/healthcare_hai_prediction/"):
                for action in ("ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath", "ssm:PutParameter"):
                    add(f"project excludes writer {folder.rsplit('/', 2)[-2]} {name} {action}", "project", action, folder + name, False)
            if principal != "e2e":
                add(f"execution excludes production {name}", "execution", "ssm:GetParameters", PARAMETER + name, False)
    for resource in (
        PARAMETER + "polaris_root_secret",
        PARAMETER + "aws_polaris_e2e_client_secret_extra",
        (PARAMETER + "aws_polaris_e2e_client_secret").replace("eu-west-1", "eu-west-2"),
        (PARAMETER + "aws_polaris_e2e_client_secret").replace("111111111111", "222222222222"),
        PARAMETER.replace("/example_project/", "/another_project/") + "aws_polaris_e2e_client_secret",
    ):
        add(f"execution excludes parameter {resource}", "execution", "ssm:GetParameters", resource, False)
    for service, expected in (("ecs-tasks.amazonaws.com", True), ("ec2.amazonaws.com", False), ("lambda.amazonaws.com", False)):
        add(f"pass test role {service}", "project", "iam:PassRole", ROLE, expected, {"iam:PassedToService": (service,)})
    add("pass test role needs service", "project", "iam:PassRole", ROLE, False)
    for resource in (
        ROLE.replace("write_e2e", "write_silver"),
        ROLE.replace("write_e2e", "write_gold"),
        ROLE.replace("write_e2e", "write_features"),
        ROLE.replace("write_e2e", "write_bronze"),
        ROLE.replace("write_e2e_task", "unrelated_task"),
        ROLE.replace("111111111111", "222222222222"),
        ROLE.replace("_dev", "_prod"),
    ):
        add(f"pass excludes role {resource}", "project", "iam:PassRole", resource, False, {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)})
    for resource, expected in (
        (f"{BUCKET}/lakehouse/gold/table/metadata/v1.metadata.json", True),
        (f"{BUCKET}/lakehouse/bronze/schema/table/metadata/v2.metadata.json", True),
        (f"{BUCKET}/lakehouse/features_e2e/table/metadata/v3.metadata.json", True),
        (f"{BUCKET}/lakehouse/gold/table/metadata/manifest.avro", False),
        (f"{BUCKET}/lakehouse/gold/table/data.parquet", False),
        (f"{BUCKET}/lakehouse/gold/table/v1.metadata.json", False),
        (f"{BUCKET}/lakehouse/gold/table/metadata/v1.metadata.json.bak", False),
        (f"{BUCKET}/outside/gold/table/metadata/v1.metadata.json", False),
        (f"{OTHER_BUCKET}/lakehouse/gold/table/metadata/v1.metadata.json", False),
    ):
        add(f"project verifier scope {resource}", "project", "s3:GetObjectVersion", resource, expected)
    grant = f"{BUCKET}/lakehouse/run_stage/run/grants/AROEXAMPLETEST:task_one.json"
    add("writer reads own grant", "writer", "s3:GetObject", grant, True, {"aws:userid": ("AROEXAMPLETEST:task_one",)})
    add("writer cannot read another grant", "writer", "s3:GetObject", grant.replace("task_one", "task_two"), False)
    add("writer reads pinned stage", "writer", "s3:GetObjectVersion", f"{BUCKET}/lakehouse/run_stage/run/inputs/code.tar", True)
    add("writer cannot write stage", "writer", "s3:PutObject", f"{BUCKET}/lakehouse/run_stage/run/tries/one/done.json", False)
    return result


def evaluate(documents: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Evaluate the complete attached-policy set for each synthetic principal."""
    actors = {
        "writer": [documents["job_stage_read"], documents["job_writer_e2e_storage"]],
        "reader": [documents["job_stage_read"], documents["job_source_read"], documents["job_reader_e2e"]],
        "reader_delta": [documents["job_reader_e2e"]],
        "execution": [documents["job_execution"], documents["job_writer_e2e_execution"]],
        "project": [document for name, document in documents.items() if name.startswith("project_")],
    }
    observations = []
    for label, actor, action, resource, expected, context in cases():
        decision = decide(actors[actor], Request(action, resource, {"aws:userid": ("AROEXAMPLETEST:task_one",), **context}))
        observations.append(
            {
                "case": label,
                "expected": expected,
                "observed": decision.allowed,
                "passed": expected == decision.allowed,
                "effect": decision.effect,
                "statements": decision.statements,
            }
        )
    return observations


def verify(root: Path, base: dict[str, dict[str, Any]], output: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Render actual templates, evaluate requests and retain each resource and condition mutation."""
    inputs = {
        "BUCKET_ARN": BUCKET,
        "TEST_OBJECT_ARNS": json.dumps([f"{BUCKET}/lakehouse/{name}/*" for name in TEST_NAMESPACES]),
        "TEST_LIST_PREFIXES": json.dumps([f"lakehouse/{name}/*" for name in TEST_NAMESPACES]),
        "BRONZE_TEST_OBJECT_ARNS": json.dumps([f"{BUCKET}/lakehouse/{name}/*" for name in BRONZE_TWINS]),
        "BRONZE_TEST_LIST_PREFIXES": json.dumps([f"lakehouse/{name}/*" for name in BRONZE_TWINS]),
        "SOURCE_OBJECT_ARNS": json.dumps([f"{BUCKET}/cms/hospitals/*"]),
        "SOURCE_LIST_PREFIXES": json.dumps(["cms/hospitals/*"]),
        "E2E_PARAMETER_ARNS": json.dumps([PARAMETER + name for name in ("aws_polaris_e2e_client_id", "aws_polaris_e2e_client_secret")]),
    }
    documents = copy.deepcopy(base)
    for name in POLICIES:
        path = root / f"infra/iam/owner_policies/{name}.json"
        documents[name] = json.loads(Template(path.read_text()).substitute(inputs)) if path.exists() else {"Version": "2012-10-17", "Statement": []}
    checks = evaluate(documents)
    defects = []
    selected = [(name, statement) for name in POLICIES for statement in documents[name]["Statement"]]
    selected += [("project_iam", statement) for statement in documents["project_iam"]["Statement"] if statement["Sid"] == "PassE2eWriterRole"]
    selected += [("project_s3", statement) for statement in documents["project_s3"]["Statement"] if statement["Sid"] == "ReadCatalogMetadataVersions"]
    for name, statement in selected:
        variants: list[tuple[str, str, Any]] = [("widen_resource", "Resource", "*")]
        if "Condition" in statement:
            variants.append(("drop_condition", "Condition", None))
        if name == "job_writer_e2e_storage" and statement["Sid"] == "TestTableObjects":
            variants += [(f"add_production_{namespace}", "Resource", [*statement["Resource"], f"{BUCKET}/lakehouse/{namespace}/*"]) for namespace in PRODUCTION]
        for label, field, value in variants:
            changed = copy.deepcopy(documents)
            target = next(item for item in changed[name]["Statement"] if item["Sid"] == statement["Sid"])
            if value is None:
                target.pop(field)
            else:
                target[field] = value
            failures = [item["case"] for item in evaluate(changed) if not item["passed"]]
            defect = f"{name}_{statement['Sid']}_{label}"
            (output / f"defect_{defect}.json").write_text(json.dumps(changed[name], indent=2) + "\n")
            defects.append({"defect": defect, "caught": bool(failures), "failed_cases": failures})
    for name in POLICIES:
        (output / f"{name}.json").write_text(json.dumps(documents[name], indent=2) + "\n")
    return checks, defects
