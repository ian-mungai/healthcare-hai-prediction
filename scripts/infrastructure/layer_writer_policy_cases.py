"""Exercise production writer storage, parameter injection and PassRole boundaries without AWS."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from string import Template
from typing import Any

from scripts.infrastructure.bronze_policy_cases import INLINE_QUOTA, KEY_PARAMETER, MANAGED_QUOTA, MARGIN, other_version
from scripts.infrastructure.bronze_policy_cases import LARGEST_BUCKET as LARGEST_BUCKET_NAME
from scripts.infrastructure.job_writer_policy_cases import BUCKET, OTHER_BUCKET, PARAMETER, PRODUCTION, TEST_NAMESPACES, WRITER_ACTIONS, Case
from scripts.infrastructure.policy_evaluator import Request, decide

LAYERS = ("silver", "gold", "features", "bronze")
PREFIXES = {
    "silver": ("silver",),
    "gold": ("gold", "catalog_backups"),
    "features": ("features",),
    "bronze": ("bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check", "bronze_e2e", "bronze_dictionary_e2e"),
}
INPUTS = {"silver": "bronze", "gold": "silver", "features": "gold"}  # the one layer each job reads besides its own
UPLOADS = ("silver", "gold")
REGISTRY = f"{BUCKET}/lakehouse/feast/registry.pb"
REDIS_PARAMETER = PARAMETER + "redis_password"
PAYLOADS = "bronze_objects/sha256"
LARGEST_BUCKET = f"arn:aws:s3:::{LARGEST_BUCKET_NAME}"
MANAGED_PREFIX = "storage_bronze_"
EXTRAS = ("layer_input", "layer_upload", "layer_state", "layer_feast", "layer_activation")
POLICIES = (
    "job_writer_layer_storage",
    "job_writer_execution",
    "bronze_writer_storage",
    "bronze_writer_source_objects",
    "bronze_writer_source_lists",
    "bronze_writer_pseudonym_key",
    "job_writer_layer_input",
    "job_writer_layer_upload",
    "job_writer_layer_state",
    "job_writer_features_feast",
    "job_writer_features_activation",
)


def cases(collections: tuple[str, ...], raw: bool) -> list[Case]:
    """Specify each production grant and cross-identity refusal independently of policy output. The raw root is granted only when its switch is on."""
    result: list[Case] = []

    def add(label: str, actor: str, action: str, resource: str, allowed: bool, context: dict[str, tuple[str, ...]] | None = None) -> None:
        result.append((label, actor, action, resource, allowed, context or {}))

    for layer in LAYERS:
        for namespace in (*PRODUCTION, *TEST_NAMESPACES, "other", "catalog_journal", "catalog_migration"):
            own = namespace in PREFIXES[layer]
            reads = INPUTS.get(layer) == namespace
            resource = f"{BUCKET}/lakehouse/{namespace}/table/metadata/v1.metadata.json"
            for action in WRITER_ACTIONS:
                add(f"{layer} objects {namespace} {action}", layer, action, resource, own or (reads and action == "s3:GetObject"))
            add(f"{layer} list {namespace}", layer, "s3:ListBucket", BUCKET, own or reads, {"s3:prefix": (f"lakehouse/{namespace}/",)})
            add(f"{layer} input versions {namespace}", layer, "s3:ListBucketVersions", BUCKET, reads, {"s3:prefix": (f"lakehouse/{namespace}/",)})
            for action in ("s3:GetObjectVersion", "s3:DeleteObjectVersion", "s3:PutObjectAcl"):
                add(f"{layer} excludes table privilege {namespace} {action}", layer, action, resource, False)
        for namespace in PREFIXES[layer]:
            add(f"{layer} backup {namespace}", layer, "s3:PutObject", f"{BUCKET}/lakehouse/{namespace}/catalog_backups/head.json", True)
            add(f"{layer} lookalike {namespace}", layer, "s3:PutObject", f"{BUCKET}/lakehouse/{namespace}_other/file", False)
        for prefix in ("", "lakehouse/", f"lakehouse/{layer}", f"lakehouse/{layer}_other/"):
            add(f"{layer} list boundary {prefix}", layer, "s3:ListBucket", BUCKET, False, {"s3:prefix": (prefix,)})
        add(f"{layer} list needs prefix", layer, "s3:ListBucket", BUCKET, False)
        add(f"{layer} list other bucket", layer, "s3:ListBucket", OTHER_BUCKET, False, {"s3:prefix": (f"lakehouse/{layer}/",)})
        for action in WRITER_ACTIONS:
            add(f"{layer} other bucket {action}", layer, action, f"{OTHER_BUCKET}/lakehouse/{layer}/file", False)
        add(f"{layer} own grant", layer, "s3:GetObject", f"{BUCKET}/lakehouse/run_stage/run/grants/AROEXAMPLE:task_one.json", True)
        add(f"{layer} other grant", layer, "s3:GetObject", f"{BUCKET}/lakehouse/run_stage/run/grants/AROEXAMPLE:task_two.json", False)
        add(f"{layer} pinned stage", layer, "s3:GetObjectVersion", f"{BUCKET}/lakehouse/run_stage/run/inputs/code.tar", True)
        add(f"{layer} stage write", layer, "s3:PutObject", f"{BUCKET}/lakehouse/run_stage/run/tries/one/done.json", False)
        add(f"{layer} bucket configuration", layer, "s3:GetBucketLocation", BUCKET, False)
        for root in ("", "raw/"):
            root_enabled = root == "" or raw
            for collection in (*collections, "example/not_mapped", "cms/hospitals_other"):
                source_allowed = layer == "bronze" and collection in collections and root_enabled
                for suffix in ("data/input.csv", "manifests/collection.json"):
                    resource = f"{BUCKET}/{root}{collection}/{suffix}"
                    add(f"{layer} source version {root}{collection} {suffix}", layer, "s3:GetObjectVersion", resource, source_allowed)
                    for action in (*WRITER_ACTIONS, "s3:DeleteObjectVersion"):
                        add(f"{layer} source refusal {root}{collection} {suffix} {action}", layer, action, resource, False)
                for prefix in (f"{root}{collection}/", f"{root}{collection}/manifests/"):
                    add(f"{layer} source versions {prefix}", layer, "s3:ListBucketVersions", BUCKET, source_allowed, {"s3:prefix": (prefix,)})
                    add(f"{layer} source current listing {prefix}", layer, "s3:ListBucket", BUCKET, False, {"s3:prefix": (prefix,)})
            payload = f"{BUCKET}/{root}{PAYLOADS}/" + "ab" * 32
            add(f"{layer} payload version {root}", layer, "s3:GetObjectVersion", payload, layer == "bronze" and root_enabled)
            for action in (*WRITER_ACTIONS, "s3:DeleteObjectVersion"):
                add(f"{layer} payload refusal {root} {action}", layer, action, payload, False)
            add(f"{layer} payload other digest path {root}", layer, "s3:GetObjectVersion", f"{BUCKET}/{root}bronze_objects/other/" + "ab" * 32, False)
        add(f"{layer} pseudonym key read", layer, "ssm:GetParameter", KEY_PARAMETER, layer == "bronze")
        for action in ("ssm:PutParameter", "ssm:GetParameters", "ssm:GetParametersByPath", "ssm:DeleteParameter"):
            add(f"{layer} pseudonym key excludes {action}", layer, action, KEY_PARAMETER, False)
        for candidate in (
            other_version("v2"),
            other_version("v10"),
            KEY_PARAMETER.replace("pseudonym_key_v1", "pseudonym_key_"),
            KEY_PARAMETER.replace("pseudonym_key_v1", "broker_token"),
            KEY_PARAMETER.replace("111111111111", "222222222222"),
            KEY_PARAMETER.replace("us-west-2", "eu-west-1"),
            KEY_PARAMETER.replace("healthcare_hai_prediction", "other_project"),
        ):
            add(f"{layer} pseudonym key boundary {candidate}", layer, "ssm:GetParameter", candidate, False)
        for suffix in ("data/input.csv", "manifests/collection.json"):
            add(f"{layer} source other bucket {suffix}", layer, "s3:GetObjectVersion", f"{OTHER_BUCKET}/cms/hospitals/{suffix}", False)
        add(f"{layer} source list other bucket", layer, "s3:ListBucketVersions", OTHER_BUCKET, False, {"s3:prefix": ("cms/hospitals/",)})
        add(f"{layer} source list needs prefix", layer, "s3:ListBucketVersions", BUCKET, False)

    for layer in LAYERS:
        for owner in (*UPLOADS, "features", "bronze", "other"):
            resource = f"{BUCKET}/lakehouse/layer_runs/{owner}/run/export/manifest.json"
            add(f"{layer} run upload {owner}", layer, "s3:PutObject", resource, layer == owner and owner != "bronze" and owner != "other")
            for action in ("s3:GetObject", "s3:DeleteObject", "s3:GetObjectVersion", "s3:PutObjectAcl", "s3:AbortMultipartUpload"):
                add(f"{layer} run upload excludes {owner} {action}", layer, action, resource, False)
            add(f"{layer} run list {owner}", layer, "s3:ListBucket", BUCKET, False, {"s3:prefix": (f"lakehouse/layer_runs/{owner}/",)})
        for owner in (*UPLOADS, "features", "other"):
            state = f"{BUCKET}/lakehouse/layer_runs/{owner}/run/state/pinned.json"
            add(f"{layer} run state {owner}", layer, "s3:PutObject", state, layer == owner and owner != "other")
            add(f"{layer} run state excludes {owner} read", layer, "s3:GetObject", state, False)
        add(f"{layer} run upload lookalike", layer, "s3:PutObject", f"{BUCKET}/lakehouse/layer_runs/{layer}_other/file", False)
        add(f"{layer} run upload other bucket", layer, "s3:PutObject", f"{OTHER_BUCKET}/lakehouse/layer_runs/{layer}/file", False)
        add(f"{layer} run upload bare prefix", layer, "s3:PutObject", f"{BUCKET}/lakehouse/layer_runs/file", False)
        add(f"{layer} input other bucket", layer, "s3:GetObject", f"{OTHER_BUCKET}/lakehouse/{INPUTS.get(layer, 'bronze')}/file", False)
        add(f"{layer} input list other bucket", layer, "s3:ListBucket", OTHER_BUCKET, False, {"s3:prefix": (f"lakehouse/{INPUTS.get(layer, 'bronze')}/",)})
        add(
            f"{layer} input versions other bucket",
            layer,
            "s3:ListBucketVersions",
            OTHER_BUCKET,
            False,
            {"s3:prefix": (f"lakehouse/{INPUTS.get(layer, 'bronze')}/",)},
        )
        add(f"{layer} input versions need a prefix", layer, "s3:ListBucketVersions", BUCKET, False)
        add(f"{layer} feast registry get", layer, "s3:GetObject", REGISTRY, layer == "features")
        add(f"{layer} feast registry put", layer, "s3:PutObject", REGISTRY, layer == "features")
        add(f"{layer} feast registry delete", layer, "s3:DeleteObject", REGISTRY, False)
        add(f"{layer} feast other object", layer, "s3:PutObject", f"{BUCKET}/lakehouse/feast/other.pb", False)
        add(f"{layer} feast e2e registry", layer, "s3:PutObject", f"{BUCKET}/lakehouse/feast_e2e/registry.pb", False)
        add(f"{layer} registry store gets no unprefixed bucket list", layer, "s3:ListBucket", BUCKET, False)
        add(f"{layer} registry bucket check is not a list of the whole bucket", layer, "s3:ListBucket", BUCKET, False, {"s3:prefix": ("",)})
        add(f"{layer} registry bucket check is not a list of another bucket", layer, "s3:ListBucket", OTHER_BUCKET, False)
        add(f"{layer} redis password read", layer, "ssm:GetParameter", REDIS_PARAMETER, layer == "features")
        for action in ("ssm:GetParameters", "ssm:GetParametersByPath", "ssm:PutParameter", "ssm:DeleteParameter"):
            add(f"{layer} redis password excludes {action}", layer, action, REDIS_PARAMETER, False)

    for principal in (*LAYERS, "reader", "e2e"):
        for suffix in ("client_id", "client_secret"):
            parameter = PARAMETER + f"aws_polaris_{principal}_{suffix}"
            add(f"execution pair {principal} {suffix}", "execution_delta", "ssm:GetParameters", parameter, principal in LAYERS)
            add(f"execution no single read {principal} {suffix}", "execution_delta", "ssm:GetParameter", parameter, False)
            for actor in (*LAYERS, "project", "reader"):
                for action in ("ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath", "ssm:PutParameter"):
                    add(f"{actor} excludes pair {principal} {suffix} {action}", actor, action, parameter, False)
            for candidate in (
                parameter + "_extra",
                parameter.replace("eu-west-1", "eu-west-2"),
                parameter.replace("111111111111", "222222222222"),
                parameter.replace("/example_project/", "/other_project/"),
            ):
                add(f"execution parameter boundary {candidate}", "execution_delta", "ssm:GetParameters", candidate, False)
    add("execution excludes administrator", "execution_delta", "ssm:GetParameters", PARAMETER + "polaris_root_secret", False)
    for actor in ("project", "reader", "execution_delta"):
        add(f"{actor} excludes pseudonym key", actor, "ssm:GetParameter", KEY_PARAMETER, False)

    for layer in LAYERS:
        role = f"arn:aws:iam::111111111111:role/example_project_job_write_{layer}_task_dev"
        for service, allowed in (
            ("ecs-tasks.amazonaws.com", True),
            ("ecs.amazonaws.com", False),
            ("ec2.amazonaws.com", False),
            ("lambda.amazonaws.com", False),
        ):
            add(f"pass {layer} to {service}", "project", "iam:PassRole", role, allowed, {"iam:PassedToService": (service,)})
        add(f"pass {layer} needs service", "project", "iam:PassRole", role, False)
        add(f"cannot assume {layer}", "project", "sts:AssumeRole", role, False)
        for candidate in (
            role + "_extra",
            role.replace("111111111111", "222222222222"),
            role.replace("example_project", "other_project"),
            role.replace("_dev", "_prod"),
        ):
            add(f"pass {layer} exact role {candidate}", "project", "iam:PassRole", candidate, False, {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)})
    add(
        "pass refuses sibling job_write_other_task",
        "project",
        "iam:PassRole",
        "arn:aws:iam::111111111111:role/example_project_job_write_other_task_dev",
        False,
        {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)},
    )
    prepared_state = f"{BUCKET}/lakehouse/layer_runs/features/run/state/pinned.json"
    add("features before its activation puts its run claim", "features_prepared", "s3:PutObject", prepared_state, True)
    add("features before its activation uploads nothing", "features_prepared", "s3:PutObject", f"{BUCKET}/lakehouse/layer_runs/features/run/commit.json", False)
    add("features before its activation has no bucket check", "features_prepared", "s3:ListBucket", BUCKET, False)
    return result


def inline_documents(documents: dict[str, dict[str, Any]], layer: str) -> list[dict[str, Any]]:
    """Return the inline policies of one writer role. Managed bronze policies carry their own quota."""
    return [documents["job_stage_read"], documents[f"storage_{layer}"], *(documents[f"{name}_{layer}"] for name in EXTRAS if f"{name}_{layer}" in documents)]


def evaluate(documents: dict[str, dict[str, Any]], collections: tuple[str, ...], raw: bool) -> list[dict[str, Any]]:
    """Evaluate complete writer attachments, inline and managed, and the complete project policy set."""
    actors = {
        **{
            layer: [
                *inline_documents(documents, layer),
                *(document for name, document in documents.items() if layer == "bronze" and name.startswith(MANAGED_PREFIX)),
            ]
            for layer in LAYERS
        },
        # The features role of a silver or gold window: job_features_image_prepared is off, so the activation policy is not attached.
        "features_prepared": [
            documents["job_stage_read"],
            documents["storage_features"],
            *(documents[f"{name}_features"] for name in EXTRAS if name != "layer_activation" and f"{name}_features" in documents),
        ],
        "execution_delta": [documents["job_writer_execution"]],
        "reader": [documents["job_stage_read"], documents["job_source_read"]],
        "project": [document for name, document in documents.items() if name.startswith("project_")],
    }
    observations = []
    for label, actor, action, resource, expected, context in cases(collections, raw):
        decision = decide(actors[actor], Request(action, resource, {"aws:userid": ("AROEXAMPLE:task_one",), **context}))
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


def compact(document: dict[str, Any]) -> int:
    """Count the characters IAM counts: compact JSON without whitespace."""
    return len(json.dumps(document, separators=(",", ":")))


def quota_checks(documents: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Check every role's inline total and every managed bronze policy against its quota with margin."""
    observations = []
    inline_limit = int(INLINE_QUOTA * MARGIN)
    for layer in LAYERS:
        size = sum(compact(document) for document in inline_documents(documents, layer))
        observations.append(
            {
                "case": f"{layer} inline policies together fit the role quota with margin",
                "expected": inline_limit,
                "observed": size,
                "passed": size <= inline_limit,
            }
        )
    managed_limit = int(MANAGED_QUOTA * MARGIN)
    for name, document in documents.items():
        if name.startswith(MANAGED_PREFIX):
            size = compact(document)
            observations.append(
                {"case": f"{name} fits the managed policy quota with margin", "expected": managed_limit, "observed": size, "passed": size <= managed_limit}
            )
    return observations


def load(path: Path, values: dict[str, str]) -> dict[str, Any]:
    """Render one owner policy template, or an empty policy when the template is absent."""
    return dict(json.loads(Template(path.read_text()).substitute(values))) if path.exists() else {"Statement": []}


def render_documents(root: Path, base: dict[str, dict[str, Any]], bucket: str, collections: tuple[str, ...], raw: bool) -> dict[str, dict[str, Any]]:
    """Render every writer policy from its actual template against one bucket ARN, as Terraform does."""
    directory = root / "infra/iam/owner_policies"
    documents = copy.deepcopy(base)
    roots = ("", "raw/") if raw else ("",)
    for layer in LAYERS:
        values = {
            "BUCKET_ARN": bucket,
            "TABLE_OBJECT_ARNS": json.dumps([f"{bucket}/lakehouse/{name}/*" for name in PREFIXES[layer]]),
            "TABLE_LIST_PREFIXES": json.dumps([f"lakehouse/{name}/*" for name in PREFIXES[layer]]),
        }
        documents[f"storage_{layer}"] = load(directory / ("bronze_writer_storage.json" if layer == "bronze" else "job_writer_layer_storage.json"), values)
    for suffix, name in (("root", ""), ("raw", "raw/")):
        if name in roots:
            arns = [f"{bucket}/{name}{collection}/*" for collection in collections] + [f"{bucket}/{name}{PAYLOADS}/*"]
            documents[f"{MANAGED_PREFIX}source_objects_{suffix}"] = load(
                directory / "bronze_writer_source_objects.json", {"SOURCE_OBJECT_ARNS": json.dumps(arns)}
            )
    prefixes = [f"{name}{collection}/*" for name in roots for collection in collections]
    documents[f"{MANAGED_PREFIX}source_lists"] = load(
        directory / "bronze_writer_source_lists.json", {"BUCKET_ARN": bucket, "SOURCE_LIST_PREFIXES": json.dumps(prefixes)}
    )
    documents[f"{MANAGED_PREFIX}key"] = load(directory / "bronze_writer_pseudonym_key.json", {"PSEUDONYM_KEY_ARN": KEY_PARAMETER})
    for layer, source in INPUTS.items():
        values = {
            "BUCKET_ARN": bucket,
            "INPUT_OBJECT_ARNS": json.dumps([f"{bucket}/lakehouse/{source}/*"]),
            "INPUT_LIST_PREFIXES": json.dumps([f"lakehouse/{source}/*"]),
        }
        documents[f"layer_input_{layer}"] = load(directory / "job_writer_layer_input.json", values)
    for layer in UPLOADS:
        documents[f"layer_upload_{layer}"] = load(
            directory / "job_writer_layer_upload.json", {"UPLOAD_OBJECT_ARNS": json.dumps([f"{bucket}/lakehouse/layer_runs/{layer}/*"])}
        )
    documents["layer_state_features"] = load(
        directory / "job_writer_layer_upload.json", {"UPLOAD_OBJECT_ARNS": json.dumps([f"{bucket}/lakehouse/layer_runs/features/*/state/*"])}
    )
    feast = {"REGISTRY_OBJECT_ARNS": json.dumps([f"{bucket}/lakehouse/feast/registry.pb"]), "REDIS_PARAMETER_ARNS": json.dumps([REDIS_PARAMETER])}
    documents["layer_feast_features"] = load(directory / "job_writer_features_feast.json", feast)
    activation = {"BUCKET_ARN": bucket, "RUN_OBJECT_ARNS": json.dumps([f"{bucket}/lakehouse/layer_runs/features/*"])}
    documents["layer_activation_features"] = load(directory / "job_writer_features_activation.json", activation)
    parameters = [PARAMETER + f"aws_polaris_{layer}_{suffix}" for layer in LAYERS for suffix in ("client_id", "client_secret")]
    documents["job_writer_execution"] = load(directory / "job_writer_execution.json", {"WRITER_PARAMETER_ARNS": json.dumps(parameters)})
    return documents


def verify(root: Path, base: dict[str, dict[str, Any]], output: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Retain requests and deliberate defects against the actual storage and execution templates."""
    table_map = json.loads((root / "config/lakehouse/bronze_tables.json").read_text())
    collections = tuple(sorted({table["collection"] for table in table_map["tables"]}))
    documents = render_documents(root, base, BUCKET, collections, raw=True)
    root_only = render_documents(root, base, BUCKET, collections, raw=False)
    largest_base = json.loads(json.dumps(base).replace(BUCKET, LARGEST_BUCKET))
    largest = render_documents(root, largest_base, LARGEST_BUCKET, collections, raw=True)
    checks = evaluate(documents, collections, raw=True)
    checks += [{**item, "case": f"raw root off: {item['case']}"} for item in evaluate(root_only, collections, raw=False)]
    checks += quota_checks(largest)
    defects = []
    selected = [
        (name, statement)
        for name, document in documents.items()
        if name.startswith(("storage_", "layer_")) or name == "job_writer_execution"
        for statement in document["Statement"]
    ]
    selected += [("project_iam", statement) for statement in documents["project_iam"]["Statement"] if statement["Sid"] == "PassLayerWriterRoles"]
    for name, statement in selected:
        variants: list[tuple[str, str, Any]] = [("widen_resource", "Resource", "*"), ("remove_grant", "Action", ["example:Unused"])]
        if "Condition" in statement:
            variants.append(("drop_condition", "Condition", None))
        if statement["Sid"] in ("LayerTableObjects", "BronzeTableObjects"):
            variants += [("delete_versions", "Action", [*statement["Action"], "s3:DeleteObjectVersion"])]
            variants += [
                (f"cross_{namespace}", "Resource", [*statement["Resource"], f"{BUCKET}/lakehouse/{namespace}/*"])
                for namespace in (*PRODUCTION, *TEST_NAMESPACES)
                if f"{BUCKET}/lakehouse/{namespace}/*" not in statement["Resource"]
            ]
        if name == "project_iam" and statement["Sid"] == "PassLayerWriterRoles":
            variants.append(("writer_family_wildcard", "Resource", ["arn:aws:iam::111111111111:role/example_project_job_write_*_task_dev"]))
        if name.startswith(f"{MANAGED_PREFIX}source_objects_") and statement["Sid"] == "SourceObjectVersionsOnly":
            variants += [
                ("missing_collection", "Resource", statement["Resource"][1:]),
                ("extra_collection", "Resource", [*statement["Resource"], f"{BUCKET}/example/not_mapped/*"]),
                ("source_write", "Action", [*statement["Action"], "s3:PutObject"]),
            ]
        if name == f"{MANAGED_PREFIX}key":
            variants += [
                ("next_key_version", "Resource", [other_version("v2")]),
                ("key_prefix_wildcard", "Resource", [KEY_PARAMETER.replace("pseudonym_key_v1", "pseudonym_key_*")]),
                ("key_write", "Action", [*statement["Action"], "ssm:PutParameter"]),
                ("key_path_read", "Action", [*statement["Action"], "ssm:GetParametersByPath"]),
            ]
        for label, field, value in variants:
            changed = copy.deepcopy(documents)
            target = next(item for item in changed[name]["Statement"] if item["Sid"] == statement["Sid"])
            if value is None:
                target.pop(field)
            else:
                target[field] = value
            failures = [item["case"] for item in evaluate(changed, collections, raw=True) if not item["passed"]]
            defect = f"layer_{name}_{statement['Sid']}_{label}"
            (output / f"defect_{defect}.json").write_text(json.dumps(changed[name], indent=2) + "\n")
            defects.append({"defect": defect, "caught": bool(failures), "failed_cases": failures})
    for name, document in documents.items():
        if name.startswith(("storage_", "layer_")) or name == "job_writer_execution":
            (output / f"{name}.json").write_text(json.dumps(document, indent=2) + "\n")
    return checks, defects
