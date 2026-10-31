"""Walk every activation input of the deployment stacks through the real renderers and the real wrapper, with synthetic files and inert executables.

A flag that a Terraform stack declares is useless to the owner when the wrapper refuses to render it. Each row of the table below names one input of one stack
with a reviewed value and a value of the wrong type. For every row the renderer must accept the reviewed value from a file bound to its digest and write it,
must refuse the wrong type and an unreviewed name, and must leave the input out of a default render. For every boolean row the saved-plan check must accept a
plan that carries the reviewed value and refuse a plan that enables it without the reviewed file. A completeness check reads the variables of the stacks and the
owner steps of the documents, so a new flag that no row covers fails the run. Run from the repository root:
``.venv/bin/python -m scripts.infrastructure.run_activation_inputs_e2e --out <new folder>``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, NamedTuple

from scripts.infrastructure.run_e2e import SERVICES_FOUNDATION
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "scripts/__init__.py",
    "scripts/process.py",
    "scripts/infrastructure/__init__.py",
    "scripts/infrastructure/terraform.sh",
    "scripts/infrastructure/render_project_config.py",
    "scripts/infrastructure/render_ecs_target.py",
    "scripts/infrastructure/render_jobs_config.py",
    "scripts/infrastructure/render_platform_config.py",
    "data_contracts/infrastructure/jobs_preparation.schema.json",
    "data_contracts/infrastructure/services_preparation.schema.json",
    "data_contracts/infrastructure/platform_preparation.schema.json",
    "config/aws/iam_preparation.json",
    "config/lakehouse/bronze_tables.json",
)
ACCOUNT = "111111111111"
ADMIN = "example_admin"
STACKS = {"iam": "infra/iam", "platform": "infra/platform", "services": "infra/services", "jobs": "infra/jobs"}
OUTPUTS = {
    "iam": "infra/iam/preparation.auto.tfvars.json",
    "platform": "infra/platform/deployment.auto.tfvars.json",
    "services": "infra/services/deployment.auto.tfvars.json",
    "jobs": "infra/jobs/deployment.auto.tfvars.json",
}
IMAGE = f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/example_project_%s_dev@sha256:" + "a" * 64
ROLE = f"arn:aws:iam::{ACCOUNT}:role/example_project_%s_dev"
RESOURCES = {"cpu": 1024, "memory": 4096, "ephemeral_storage_gib": 21}
FAMILIES = ["serve", "train"]
# The platform defaults of infra/platform/variables.tf, which a case below compares with the file, and the additions the owner steps make to them.
PLATFORM_DEFAULT_REPOSITORIES = [
    "airflow",
    "analytics",
    "egress_proxy",
    "marquez",
    "marquez_web",
    "polaris",
    "polaris_admin",
    "postgres",
    "quality",
    "redis",
    "spark",
]
PLATFORM_DEFAULT_LOG_GROUPS = ["airflow", "jobs", "marquez", "polaris", "postgres", "redis"]
PLATFORM_ADDED_REPOSITORIES = [
    "drift",
    "feature_store",
    "mlflow",
    "openmetadata_database",
    "openmetadata_ingestion",
    "openmetadata_search",
    "openmetadata_server",
    "training",
]
PLATFORM_REPOSITORIES = sorted([*PLATFORM_DEFAULT_REPOSITORIES, *PLATFORM_ADDED_REPOSITORIES])
PLATFORM_LOG_GROUPS = sorted([*PLATFORM_DEFAULT_LOG_GROUPS, "mlflow"])


class Row(NamedTuple):
    """One activation input: the stack that declares it, a reviewed value and a value of the wrong type.

    A row with a default also gets the saved-plan cases. Each value in refuse is well typed and the contract must still refuse it.
    """

    stack: str
    name: str
    good: Any
    bad: Any
    default: Any = None
    refuse: tuple[Any, ...] = ()


def flags(stack: str, *names: str) -> list[Row]:
    return [Row(stack, name, True, "true") for name in names]


ROWS: list[Row] = [
    *flags(
        "iam",
        "athena_mirror_prepared",
        "broker_prepared",
        "drift_prepared",
        "job_endpoints_enabled",
        "job_writer_layers_prepared",
        "job_layer_runs_prepared",
        "model_api_prepared",
        "model_api_prediction_writes",
        "openmetadata_prepared",
        "openmetadata_marquez_reachable",
        "serve_prepared",
        "training_prepared",
        "monitoring_enabled",
    ),
    Row("iam", "athena_owner_user", "example_owner", 5),
    Row("iam", "model_api_registry_key", "lakehouse/feast/registry.pb", "/rooted/registry.pb"),
    Row("iam", "model_api_manifest_prefix", "serving/deployments", "serving/deployments/"),
    Row("iam", "broker_job_families", FAMILIES, "serve"),
    *flags("platform", "monitoring_enabled", "serving_enabled"),
    Row(
        "platform",
        "ecr_repositories",
        PLATFORM_REPOSITORIES,
        "mlflow",
        PLATFORM_DEFAULT_REPOSITORIES,
        (["mlflow"], [*PLATFORM_DEFAULT_REPOSITORIES, "unreviewed_image"], PLATFORM_REPOSITORIES[1:]),
    ),
    Row(
        "platform",
        "log_groups",
        PLATFORM_LOG_GROUPS,
        "mlflow",
        PLATFORM_DEFAULT_LOG_GROUPS,
        (["mlflow"], [*PLATFORM_DEFAULT_LOG_GROUPS, "unreviewed_group"], PLATFORM_LOG_GROUPS[1:]),
    ),
    *flags("services", "model_api_enabled", "streamlit_enabled", "marquez_enabled", "mlflow_enabled", "broker_enabled", "monitoring_enabled"),
    Row("services", "model_api_task_role_arn", ROLE % "model_api_task", "arn:aws:iam::1:role/x"),
    Row("services", "streamlit_task_role_arn", ROLE % "streamlit_task", "arn:aws:iam::1:role/x"),
    Row("services", "broker_task_role_arn", ROLE % "broker_task", "arn:aws:iam::1:role/x"),
    Row("services", "model_api_hai_types", "HAI_1,HAI_2", "HAI_9"),
    Row("services", "streamlit_hai_types", "HAI_1", "HAI_9"),
    Row("services", "model_api_manifest_uri", "s3://example-project-data-dev/serving/deployments", "/serving/deployments"),
    Row("services", "model_api_registry_uri", "s3://example-project-data-dev/lakehouse/feast/registry.pb", "registry.pb"),
    Row("services", "model_api_mlflow_uri", "http://127.0.0.1:5100", "https://mlflow.example.invalid"),
    Row("services", "model_api_rate_per_minute", 120, "120"),
    Row("services", "model_api_burst", 40, "40"),
    Row("services", "prometheus_retention_days", 10, "10"),
    Row("services", "prometheus_retention_size_mb", 1024, "1024"),
    Row(
        "services",
        "monitoring_memory_mib",
        {name: {"limit": 512, "reservation": 256} for name in ("prometheus", "grafana", "statsd_exporter")},
        [],
    ),
    Row(
        "services",
        "broker_jobs",
        {
            "subnet_id": "subnet-0example1",
            "security_group_ids": {"lakehouse": "sg-0example1", "training": "sg-0example2"},
            "log_group_name": "example_project_jobs_dev",
            "bucket_name": "example-project-data-dev",
            "host_ip": "10.0.1.20",
            "families": FAMILIES,
        },
        {"families": "serve"},
    ),
    *flags(
        "jobs",
        "jobs_enabled",
        "train_enabled",
        "layer_jobs_enabled",
        "drift_enabled",
        "bronze_job_enabled",
        "promote_enabled",
        "serve_enabled",
        "openmetadata_enabled",
    ),
    Row("jobs", "layer_jobs", ["silver", "gold"], "silver"),
    Row("jobs", "images", {name: IMAGE % name for name in ("analytics", "quality", "spark")}, {"spark": "latest"}),
    Row("jobs", "train_images", {name: IMAGE % name for name in ("training", "feature_store")}, {"training": "latest"}),
    Row("jobs", "job_role_arns", {"execution": ROLE % "job_execution", "write_silver_task": ROLE % "job_write_silver_task"}, {"write_silver_task": "x"}),
    Row(
        "jobs",
        "writer_parameter_names",
        {"write_silver_task": ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]},
        {"write_silver_task": ["/example_project/aws_polaris_silver_client_id"]},
    ),
    Row("jobs", "log_group_name", "example_project_jobs_dev", 5),
    Row("jobs", "layer_feast_redis_host", "10.0.1.20", 5),
    Row("jobs", "layer_resources", RESOURCES, {"cpu": 1}),
    Row("jobs", "drift_images", {name: IMAGE % name for name in ("training", "feature_store", "drift")}, {"drift": "latest"}),
    Row("jobs", "drift_roles", {"execution": ROLE % "job_execution", "drift_task": ROLE % "job_drift_task"}, {"drift_task": "x"}),
    Row("jobs", "drift_resources", RESOURCES, {"cpu": 1}),
    Row("jobs", "bronze_resources", RESOURCES, {"cpu": 1}),
    Row("jobs", "promote_resources", RESOURCES, {"cpu": 1}),
    Row("jobs", "serve_roles", {"serve_task": ROLE % "job_serve_task"}, {"serve_task": "x"}),
    Row("jobs", "serve_manifest_prefix", "serving/deployments", "serving/deployments/"),
    Row("jobs", "serve_resources", RESOURCES, {"cpu": 1}),
    Row(
        "jobs",
        "openmetadata_images",
        {name: IMAGE % f"openmetadata_{name}" for name in ("server", "ingestion", "search", "database")},
        {"server": "latest"},
    ),
    Row("jobs", "openmetadata_roles", {"execution": ROLE % "job_execution", "openmetadata_task": ROLE % "job_openmetadata_task"}, {"openmetadata_task": "x"}),
    Row("jobs", "openmetadata_resources", RESOURCES, {"cpu": 1}),
    Row("jobs", "openmetadata_lifetime_seconds", 3600, "3600"),
    Row("jobs", "openmetadata_cpu_architecture", "ARM64", "POWER"),
    Row("jobs", "openmetadata_marquez_url", "http://10.0.1.20:5000", 5),
]
# Inputs that no row needs, with the reason. The storage retention flags have their own wrapper options and cases in run_e2e.
EXEMPT = {
    "mlflow_retention_enabled": "wrapper option --mlflow-retention, covered by run_e2e",
    "athena_retention_enabled": "wrapper option --athena-retention, covered by run_e2e",
    "allow_destructive_teardown": "teardown guard of the platform stack, never an activation",
    "bronze_writer_prepared": "required key of the committed IAM preparation file",
    "job_writer_e2e_prepared": "required key of the committed IAM preparation file",
}
TOOL = (
    f"#!{sys.executable}\nimport os, sys\n"
    "args = sys.argv[1:]\n"
    "if 'output' in args:\n    sys.stdout.write(open(os.environ['STANDIN_OUTPUTS']).read())\n"
    "elif 'show' in args:\n    sys.stdout.write(open(os.environ['STANDIN_PLAN']).read())\n"
)


def variables_of(stack_directory: str) -> dict[str, str]:
    """The variables a stack declares, with the type word of each."""
    found: dict[str, str] = {}
    for path in sorted((ROOT / stack_directory).glob("*.tf")):
        for match in re.finditer(r'^variable "([a-z0-9_]+)" \{(.*?)\n\}\n', path.read_text(), re.S | re.M):
            kind = re.search(r"type\s*=\s*([^\n]+)", match.group(2))
            found[match.group(1)] = kind.group(1).strip() if kind else ""
    return found


def added_names(text: str) -> list[str]:
    """The backticked names that a sentence about repositories or log groups adds: the ones after "plus" or after "add"."""
    names: list[str] = []
    for sentence in re.split(r"(?<=[.:])\s+", text):
        if not re.search(r"ecr_repositories|ECR repositories|log_groups|log groups", sentence):
            continue
        found = re.search(r"\bplus\b(.*)", sentence) or re.search(r"\badd(?:s|ed)?\b(.*)", sentence, re.I)
        if found:
            names += re.findall(r"`([a-z][a-z0-9_]+)`", found.group(1))
    return names


def completeness() -> dict[str, bool]:
    """Every boolean input and every input an owner step sets has a row or a stated reason to have none."""
    covered = {(row.stack, row.name) for row in ROWS}
    declared = {stack: variables_of(directory) for stack, directory in STACKS.items()}
    checks: dict[str, bool] = {}
    for stack, names in declared.items():
        missing = sorted(name for name, kind in names.items() if kind == "bool" and (stack, name) not in covered and name not in EXEMPT)
        checks[f"every boolean input of {stack} has a row ({missing})"] = not missing
    platform = (ROOT / "infra/platform/variables.tf").read_text()
    for variable, expected in (("ecr_repositories", PLATFORM_DEFAULT_REPOSITORIES), ("log_groups", PLATFORM_DEFAULT_LOG_GROUPS)):
        block = re.search(rf'variable "{variable}" \{{.*?default\s*=\s*\[(.*?)\]', platform, re.S)
        found = re.findall(r'"([a-z0-9_]+)"', block.group(1)) if block else []
        checks[f"the committed {variable} default equals the one the platform contract keeps ({found})"] = found == expected
    schema = json.loads((ROOT / "data_contracts/infrastructure/platform_preparation.schema.json").read_text())["properties"]["variables"]["properties"]
    checks["the platform contract allows exactly the listed repositories and log groups"] = (
        schema.get("ecr_repositories", {}).get("items", {}).get("enum") == PLATFORM_REPOSITORIES
        and schema.get("log_groups", {}).get("items", {}).get("enum") == PLATFORM_LOG_GROUPS
    )
    # The monitoring and serving flags add their own repositories and log group, and the teardown guard is named in the same lines.
    flagged = {"prometheus", "grafana", "statsd_exporter", "monitoring", "model_api", "streamlit", "allow_destructive_teardown"}
    allowed = {*PLATFORM_REPOSITORIES, *PLATFORM_LOG_GROUPS, *flagged, "ecr_repositories", "log_groups"}
    stray: list[str] = []
    for path in sorted((ROOT / "docs").glob("*.md")):
        stray += [f"{path.name}: {name}" for name in added_names(path.read_text()) if name not in allowed]
    checks[f"every repository or log group that an owner step adds is allowed ({stray})"] = not stray
    for sentence in ("Add `unreviewed_image` to the ECR repositories.", "`ecr_repositories` is the 11 default names plus `redis` and `unreviewed_image`."):
        checks[f"a planted addition is found by the document scan ({sentence})"] = "unreviewed_image" in added_names(sentence)
    checks["names of a reviewed file that is only described are not read as additions"] = not added_names(
        "Write a file with `schema_version` 1, `root` `platform` and the variable `ecr_repositories` set to the defaults."
    )
    everything = {name for names in declared.values() for name in names}
    walked: set[str] = set()
    for path in sorted((ROOT / "docs").glob("*.md")):
        text = path.read_text()
        walked |= {m.group(1) for m in re.finditer(r"`([a-z][a-z0-9_]*)(?:\.[a-z_]+)? = ", text)}
        walked |= {m.group(1) for m in re.finditer(r"(?:set|add|names?|with) `([a-z][a-z0-9_]+)`", text)}
    owner_inputs = sorted(name for name in walked & everything if not any(name in names for names in [{row.name for row in ROWS}]) and name not in EXEMPT)
    secretish = {name for name in owner_inputs if name.endswith("_name")}
    owner_inputs = [name for name in owner_inputs if name not in secretish]
    checks[f"every input an owner step sets has a row ({owner_inputs})"] = not owner_inputs
    return checks


class Workspace:
    """A copy of the renderers and wrapper in a scratch folder, with a stand-in terraform and a synthetic environment file."""

    def __init__(self, folder: Path) -> None:
        self.root = folder.resolve()
        for name in SOURCES:
            source = ROOT / name
            if source.exists():
                target = self.root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        self.environment_file = self.root / "synthetic_environment.txt"
        self.environment_file.write_text(
            "AWS_REGION=us-west-2\nAWS_PROFILE=example_project_dev\nAWS_ACCOUNT_ID=111111111111\n"
            "PROJECT_NAME=example_project\nENVIRONMENT=dev\nS3_BUCKET=example-project-data-dev\n"
        )
        self.tools = self.root / "tools"
        self.tools.mkdir()
        (self.tools / "terraform").write_text(TOOL)
        (self.tools / "terraform").chmod(0o700)
        (self.tools / "aws").write_text(
            f"#!{sys.executable}\nimport json, sys\n"
            'sys.stdout.write(json.dumps({"Account": "111111111111", "Arn": "arn:aws:iam::111111111111:user/example_project_dev"}))\n'
        )
        (self.tools / "aws").chmod(0o700)
        (self.tools / "dirname").symlink_to("/usr/bin/dirname")
        (self.tools / "bash").symlink_to("/bin/bash")
        self.plan = self.root / "standin_plan.json"
        self.outputs = self.root / "standin_outputs.json"
        self.outputs.write_text(json.dumps({name: {"value": value} for name, value in SERVICES_FOUNDATION.items()}))
        for stack in ("services",):
            (self.root / STACKS[stack] / "images.auto.tfvars.json").parent.mkdir(parents=True, exist_ok=True)
            (self.root / STACKS[stack] / "images.auto.tfvars.json").write_text(json.dumps({"image_digests": {}}))
        self.default_iam = (self.root / "config/aws/iam_preparation.json").read_text()

    def environment(self) -> dict[str, str]:
        base = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "TF_"))}
        return {**base, "PATH": str(self.tools), "STANDIN_PLAN": str(self.plan), "STANDIN_OUTPUTS": str(self.outputs), "PYTHON_BIN": sys.executable}

    def reviewed(self, stack: str, variables: dict[str, Any], digest: str | None = None) -> list[str]:
        """Write the reviewed file for a stack and return the renderer arguments that name it and its digest."""
        if stack == "iam":
            return []
        path = self.root / f"reviewed_{stack}.json"
        path.write_text(json.dumps({"schema_version": 1, "root": "jobs" if stack == "jobs" else stack, "variables": variables}, sort_keys=True) + "\n")
        return ["--preparation-inputs", str(path), "--preparation-sha256", digest or hashlib.sha256(path.read_bytes()).hexdigest()]

    def command(self, stack: str, variables: dict[str, Any] | None, *extra: str, digest: str | None = None) -> tuple[str, list[str]]:
        """The renderer (iam, jobs or platform, which also renders services) and its arguments, with no reviewed variables meaning the committed defaults."""
        if stack == "iam":
            document = json.loads(self.default_iam)
            (self.root / "config/aws/iam_preparation.json").write_text(json.dumps({**document, **(variables or {})}, indent=2) + "\n")
            return "iam", [
                "--stack",
                "iam",
                "--env-file",
                str(self.environment_file),
                "--admin-profile",
                ADMIN,
                *extra,
            ]
        module = "jobs" if stack == "jobs" else "platform"
        arguments = ["--env-file", str(self.environment_file)] if stack == "jobs" else ["--stack", stack, "--env-file", str(self.environment_file)]
        prepared = self.reviewed(stack, variables, digest) if variables is not None else []
        return module, [*arguments, *prepared, *extra]

    def run(self, stack: str, variables: dict[str, Any] | None, *extra: str, digest: str | None = None) -> Any:
        module, arguments = self.command(stack, variables, *extra, digest=digest)
        environment = self.environment()
        if module == "iam":
            return run_command(sys.executable, ["-m", "scripts.infrastructure.render_project_config", *arguments], cwd=self.root, env=environment, timeout=60)
        if module == "jobs":
            return run_command(sys.executable, ["-m", "scripts.infrastructure.render_jobs_config", *arguments], cwd=self.root, env=environment, timeout=60)
        return run_command(sys.executable, ["-m", "scripts.infrastructure.render_platform_config", *arguments], cwd=self.root, env=environment, timeout=60)

    def wrapper(self, stack: str, variables: dict[str, Any] | None) -> Any:
        """The wrapper in plan mode, which renders the inputs, verifies the stand-in identity and then runs the stand-in terraform."""
        options = {"iam": ["--iam", "--admin-profile", ADMIN], "platform": ["--platform"], "services": ["--services"], "jobs": ["--jobs"]}[stack]
        if stack == "iam":
            self.command(stack, variables)
        elif variables is not None:
            options += self.reviewed(stack, variables)
        (self.root / ".env").write_text(self.environment_file.read_text())
        line = " ".join(shlex.quote(item) for item in ["scripts/infrastructure/terraform.sh", *options, "plan"])
        (self.root / "call_wrapper.sh").write_text(f"#!/bin/bash\nexec bash {line}\n")
        return run_command("bash", ["call_wrapper.sh"], cwd=self.root, env=self.environment() | {"PYTHON_BIN": sys.executable}, timeout=60)

    def written(self, stack: str) -> dict[str, Any] | None:
        path = self.root / OUTPUTS[stack]
        return json.loads(path.read_text()) if path.is_file() else None

    def clear(self, stack: str) -> None:
        (self.root / OUTPUTS[stack]).unlink(missing_ok=True)


def plan_document(rendered: dict[str, Any], flags_off: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """A synthetic saved plan whose variables are the rendered inputs, the extra changes and the other inputs at their defaults."""
    values = {**flags_off, **rendered, **(extra or {})}
    declared = {name: ({"default": flags_off[name]} if name in flags_off else {}) for name in values}
    return {
        "complete": True,
        "errored": False,
        "variables": {name: {"value": value} for name, value in values.items()},
        "configuration": {"root_module": {"variables": declared}},
    }


def row_cases(workspace: Workspace, row: Row) -> dict[str, bool]:
    """Accept, refuse and default cases of one row through the renderer."""
    stack, name = row.stack, row.name
    label = f"{stack} {name}"
    checks: dict[str, bool] = {}
    workspace.clear(stack)
    accepted = workspace.run(stack, {name: row.good})
    written = workspace.written(stack) or {}
    checks[f"{label}: the reviewed value renders exactly"] = accepted.returncode == 0 and written.get(name) == row.good
    workspace.clear(stack)
    refused = workspace.run(stack, {name: row.bad})
    checks[f"{label}: a value of the wrong type is refused and nothing is written"] = refused.returncode == 2 and workspace.written(stack) is None
    workspace.clear(stack)
    unknown = workspace.run(stack, {f"{name}_unreviewed": row.good})
    checks[f"{label}: an unreviewed name is refused and nothing is written"] = unknown.returncode == 2 and workspace.written(stack) is None
    for index, value in enumerate(row.refuse):
        workspace.clear(stack)
        stray = workspace.run(stack, {name: value})
        checks[f"{label}: a constrained value {index} is refused and nothing is written"] = stray.returncode == 2 and workspace.written(stack) is None
    workspace.clear(stack)
    default = workspace.run(stack, None)
    checks[f"{label}: a default render leaves it out"] = default.returncode == 0 and name not in (workspace.written(stack) or {})
    return checks


def plan_cases(workspace: Workspace, row: Row, siblings: list[Row]) -> dict[str, bool]:
    """The saved-plan check of one row: a plan with the reviewed value passes, and the same plan without the reviewed file is refused."""
    stack, name = row.stack, row.name
    label = f"{stack} {name} plan"
    off = {
        sibling.name: (False if isinstance(sibling.good, bool) else sibling.default)
        for sibling in siblings
        if isinstance(sibling.good, bool) or sibling.default is not None
    }
    checks: dict[str, bool] = {}
    workspace.clear(stack)
    workspace.run(stack, {name: row.good})
    rendered = dict(workspace.written(stack) or {})
    extra: dict[str, Any] = {}
    if stack == "iam":
        deployment = {
            "aws_profile": "example_project_dev",
            "aws_region": "us-west-2",
            "data_bucket_name": "example-project-data-dev",
            "environment": "dev",
            "expected_account_id": ACCOUNT,
            "project_name": "example_project",
        }
        rendered = {**deployment, **rendered}
        extra = {"admin_profile": ADMIN}
    elif stack == "services":
        rendered["image_digests"] = {}
    workspace.plan.write_text(json.dumps(plan_document(rendered, off, extra)))
    good = workspace.run(stack, {name: row.good}, "--check-plan", str(workspace.plan))
    checks[f"{label}: a plan that carries the reviewed value is accepted"] = good.returncode == 0
    without = {key: value for key, value in rendered.items() if key != name}
    workspace.plan.write_text(json.dumps(plan_document(without, off, {name: row.good, **extra})))
    refused = workspace.run(stack, None if stack != "iam" else {}, "--check-plan", str(workspace.plan))
    checks[f"{label}: a plan that enables it without the reviewed file is refused"] = refused.returncode == 2
    return checks


def digest_cases(workspace: Workspace) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for stack, name in (("jobs", "serve_enabled"), ("services", "model_api_enabled"), ("platform", "serving_enabled")):
        workspace.clear(stack)
        wrong = workspace.run(stack, {name: True}, digest="0" * 64)
        checks[f"{stack}: a file that differs from its digest is refused and nothing is written"] = wrong.returncode == 2 and workspace.written(stack) is None
    return checks


def wrapper_cases(workspace: Workspace) -> dict[str, bool]:
    """One activation per stack through terraform.sh itself: it renders the reviewed value and then reaches the stand-in terraform."""
    checks: dict[str, bool] = {}
    for stack, name in (("iam", "serve_prepared"), ("platform", "serving_enabled"), ("services", "model_api_enabled"), ("jobs", "serve_enabled")):
        workspace.clear(stack)
        result = workspace.wrapper(stack, {name: True})
        written = workspace.written(stack) or {}
        checks[f"wrapper {stack}: {name} set to true is rendered and plan runs"] = result.returncode == 0 and written.get(name) is True
        workspace.clear(stack)
        refused = workspace.wrapper(stack, {name: "true"})
        checks[f"wrapper {stack}: {name} set to a string is refused before terraform"] = refused.returncode == 2 and workspace.written(stack) is None
    return checks


def silver_sequence(workspace: Workspace) -> dict[str, bool]:
    """The silver first run in docs/aws_broker.md, step by step.

    The step that sets an input names every flag it needs. The order is the order in which the stacks depend on each other. The reviewed files the steps
    describe pass the real renderers.
    """
    text = (ROOT / "docs/aws_broker.md").read_text()
    found = re.search(r"^## Silver first run\n(.*?)(?=^## |\Z)", text, re.S | re.M)
    section = found.group(1) if found else ""
    steps = re.split(r"(?m)^(?=\d+\. )", section)
    steps = [step for step in steps if re.match(r"\d+\. ", step)]
    numbers = [int(step.split(".", 1)[0]) for step in steps]
    checks: dict[str, bool] = {"the section exists with one numbered list": bool(found) and numbers == list(range(1, len(numbers) + 1)) and len(numbers) >= 8}

    def index(*needles: str) -> int:
        return next((position for position, step in enumerate(steps) if all(needle in step for needle in needles)), -1)

    iam = index("job_writer_layers_prepared", "job_layer_runs_prepared", "job_endpoints_enabled")
    checks["one IAM step sets the two layer flags and the endpoints flag together"] = iam >= 0
    checks["the IAM step is checked against the job_network output before any task starts"] = (
        iam >= 0 and "endpoints_enabled" in steps[iam] and "job_network" in steps[iam]
    )
    order = [
        ("the IAM step", iam),
        ("the catalog identities", index("aws_catalog_setup", "--identity silver")),
        ("the images", index("push_images")),
        ("the jobs file", index("--jobs", "--preparation-sha256", "layer_jobs")),
        ("the code staging", index("code_archive")),
        ("the dry run", index("run-task", "--tags key=Project", "--dry-run")),
        ("the plan in the logs", index("DRY_RUN_PLAN")),
    ]
    positions = [position for _, position in order]
    checks[f"the steps come in the order the stacks depend on each other ({positions})"] = all(position >= 0 for position in positions) and positions == sorted(
        positions
    )
    jobs = index("--jobs", "--preparation-sha256", "layer_jobs")
    checks["the jobs step takes its roles, pair names and log group from the outputs of the earlier steps"] = jobs >= 0 and all(
        word in steps[jobs] for word in ("job_roles", "job_writer_layer_contract", "log_group_names", "images")
    )
    variables = {name for names in (variables_of(directory) for directory in STACKS.values()) for name in names}
    named = {name for name in re.findall(r"`([a-z][a-z0-9_]+)`", section) if name in variables}
    rows = {row.name for row in ROWS}
    checks[f"every input the list names has a row ({sorted(named - rows)})"] = named <= rows
    iam_flags = {"job_writer_layers_prepared": True, "job_layer_runs_prepared": True, "job_endpoints_enabled": True}
    workspace.clear("iam")
    accepted = workspace.run("iam", iam_flags)
    checks["the IAM file of the step renders"] = accepted.returncode == 0 and all((workspace.written("iam") or {}).get(name) is True for name in iam_flags)
    silver = {
        "layer_jobs_enabled": True,
        "layer_jobs": ["silver"],
        "images": {name: IMAGE % name for name in ("analytics", "quality", "spark")},
        "job_role_arns": {"execution": ROLE % "job_execution", "write_silver_task": ROLE % "job_write_silver_task"},
        "writer_parameter_names": {
            "write_silver_task": ["/healthcare_hai_prediction/aws_polaris_silver_client_id", "/healthcare_hai_prediction/aws_polaris_silver_client_secret"]
        },
        "log_group_name": "example_project_jobs_dev",
    }
    workspace.clear("jobs")
    accepted = workspace.run("jobs", silver)
    checks["the jobs file of the step renders"] = accepted.returncode == 0 and (workspace.written("jobs") or {}).get("layer_jobs") == ["silver"]
    return checks


def run(output: Path) -> dict[str, dict[str, bool]]:
    results: dict[str, dict[str, bool]] = {"completeness": completeness()}
    with tempfile.TemporaryDirectory(prefix="activation_inputs_") as folder:
        workspace = Workspace(Path(folder))
        rows: dict[str, bool] = {}
        for row in ROWS:
            rows |= row_cases(workspace, row)
        results["rows"] = rows
        plans: dict[str, bool] = {}
        for row in ROWS:
            if isinstance(row.good, bool) or row.default is not None:
                plans |= plan_cases(workspace, row, [item for item in ROWS if item.stack == row.stack])
        results["plans"] = plans
        results["digests"] = digest_cases(workspace)
        results["wrapper"] = wrapper_cases(workspace)
        results["silver"] = silver_sequence(workspace)
    output.mkdir(parents=True)
    (output / "report.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new folder for the report")
    arguments = parser.parse_args()
    results = run(arguments.out)
    failed = [f"{group}: {name}" for group, checks in results.items() for name, ok in checks.items() if not ok]
    total = sum(len(checks) for checks in results.values())
    for line in failed:
        sys.stdout.write(f"FAIL {line}\n")
    sys.stdout.write(f"activation inputs E2E: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
