"""Walk every activation input of the deployment stacks through the real renderers and the real wrapper, with synthetic files and inert executables.

A flag that a Terraform stack declares is useless to the owner when the wrapper refuses to render it. Each row of the table below names one input of one stack
with a reviewed value and a value of the wrong type. For every row the renderer must accept the reviewed value from a file bound to its digest and write it,
must refuse the wrong type and an unreviewed name, and must leave the input out of a default render. For every boolean row the saved-plan check must accept a
plan that carries the reviewed value and refuse a plan that enables it without the reviewed file. Committed-default rows keep the preparation contract closed.
Their cases accept either reviewed code default and reject saved-plan overrides through the renderer and wrapper. Real Terraform cases in run_e2e also plan both
fixture defaults. A completeness check reads the variables of the stacks, so a new flag that no row covers fails the run. Run from the repository root:
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
# The committed defaults in the shape the renderer reads (the owner edits the committed file itself).
DEFAULT_PREPARATION = "tests/fixtures/iam_preparation_default.json"
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
# The launch role may also start the Run 3 publisher host task (infra/iam/airflow_launch.tf lists publisher among the families).
LAUNCH_FAMILIES = ["publisher", "serve", "train"]
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
# The nine job families of the host_jobs default in infra/jobs/host.tf.
HOST_FAMILIES = ["bronze", "drift", "features", "gold", "metadata", "promote", "serve", "silver", "train"]


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
        "airflow_launch_prepared",
        "athena_mirror_prepared",
        "broker_prepared",
        "drift_prepared",
        "job_endpoints_enabled",
        "job_features_image_prepared",
        "job_writer_layers_prepared",
        "job_layer_runs_prepared",
        "model_api_prepared",
        "model_api_prediction_writes",
        "openmetadata_prepared",
        "openmetadata_marquez_reachable",
        "openmetadata_persistent_prepared",
        "serve_prepared",
        "training_prepared",
        "monitoring_enabled",
        "host_code_fetch_prepared",
        "source_raw_root_enabled",
        "publisher_job_enabled",
        "publisher_keys_enabled",
    ),
    Row(
        "iam",
        "publisher_secret_arns",
        {name: f"arn:aws:secretsmanager:us-west-2:{'1' * 12}:secret:{name}-AbCdEf" for name in ("census_api_key", "bls_api_key", "hud_api_key")},
        {"census_api_key": f"arn:aws:secretsmanager:us-west-2:{'1' * 12}:secret:census_api_key-*"},
    ),
    Row("iam", "athena_owner_user", "example_owner", 5),
    Row("iam", "model_api_registry_key", "lakehouse/feast/registry.pb", "/rooted/registry.pb"),
    Row("iam", "model_api_manifest_prefix", "serving/deployments", "serving/deployments/"),
    Row("iam", "broker_job_families", FAMILIES, "serve"),
    Row(
        "iam",
        "airflow_launch_families",
        LAUNCH_FAMILIES,
        "serve",
        None,
        (["write_gold"], ["train", "train"], ["Train"], [1], ["train", None], ["publish"], ["publisher", "publisher"], ["publisher", "unknown"]),
    ),
    Row(
        "iam",
        "airflow_launch_record_prefix",
        "example/records/",
        5,
        "lakehouse/launch_records/",
        (
            "/lakehouse/launch_records/",
            "lakehouse/launch_records",
            "lakehouse/*/",
            "lakehouse/../",
            "lakehouse/a..b/",
            "",
            "/",
            "lakehouse//records/",
            "lakehouse/launch records/",
            "records/${aws:username}/",
            "lakehouse/.hidden/",
            "a" * 257 + "/",
        ),
    ),
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
    Row("platform", "scratch_volume_gb", 120, "120", 60, (0, -1, 1025, 2048, 1.5, 0.5, True, None)),
    *flags(
        "services",
        "model_api_enabled",
        "streamlit_enabled",
        "marquez_enabled",
        "mlflow_enabled",
        "broker_enabled",
        "monitoring_enabled",
        "openmetadata_persistent_enabled",
    ),
    Row("services", "openmetadata_task_role_arn", ROLE % "openmetadata_host_task", "arn:aws:iam::1:role/x"),
    Row("services", "code_fetch_task_role_arn", ROLE % "host_code_fetch_task", "arn:aws:iam::1:role/x", refuse=(ROLE % "openmetadata_host_task",)),
    Row("services", "code_bucket_name", "example-project-data-dev", 5, refuse=("Example_Bucket",)),
    Row("services", "model_api_task_role_arn", ROLE % "model_api_task", "arn:aws:iam::1:role/x"),
    Row("services", "streamlit_task_role_arn", ROLE % "streamlit_task", "arn:aws:iam::1:role/x"),
    Row("services", "serving_execution_role_arn", ROLE % "serving_execution", "arn:aws:iam::1:role/x"),
    Row("services", "broker_task_role_arn", ROLE % "broker_task", "arn:aws:iam::1:role/x"),
    Row("services", "airflow_launch_task_role_arn", ROLE % "airflow_launch_task", "arn:aws:iam::1:role/x"),
    Row("services", "airflow_launch_bucket_name", "example-project-data-dev", "s3://example-project-data-dev"),
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
        "publisher_job_enabled",
        "publisher_keys_enabled",
        "promote_enabled",
        "serve_enabled",
        "openmetadata_enabled",
        "host_jobs_enabled",
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
    Row("jobs", "layer_resources", {"silver": RESOURCES, "gold": RESOURCES, "features": RESOURCES}, {"cpu": 1}),
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
    Row("jobs", "openmetadata_server_url", "http://10.0.1.20:8585", 5),
    Row(
        "jobs",
        "host_jobs",
        ["metadata", "silver"],
        "metadata",
        HOST_FAMILIES,
        (["unreviewed_family"], ["silver", "Metadata"], ["fargate"]),
    ),
    Row(
        "jobs",
        "host_scratch_root",
        "/data/job_scratch/spill",
        5,
        "/data/job_scratch",
        (
            "data/job_scratch",
            "/data/job_scratch/",
            "/data/../etc",
            "/data/a..b",
            "/data//job_scratch",
            "/",
            "/a",
            "/data/job scratch",
            "/data/job_scratch.",
            "/data/scratch_a",
            "/data",
            "/mnt/job_scratch",
            "/data/job_scratch_other",
            "/data/job_scratch/../postgres",
        ),
    ),
    Row(
        "jobs",
        "host_openmetadata_api",
        "http://172.31.0.7:8585/api",
        5,
        None,
        (
            "http://127.0.0.1:8585/api",
            "http://8.8.8.8:8585/api",
            "http://172.15.0.1:8585/api",
            "http://172.32.0.1:8585/api",
            "http://192.169.0.1:8585/api",
            "http://10.0.1.256:8585/api",
            "http://10.0.01.20:8585/api",
            "http://10.0.1.20:8586/api",
            "http://10.0.1.20:8585",
            "http://10.0.1.20:8585/api/",
            "https://10.0.1.20:8585/api",
            "http://catalog.internal:8585/api",
            "10.0.1.20:8585/api",
        ),
    ),
    Row(
        "jobs",
        "host_repo_path",
        "/opt/hai/repo",
        5,
        None,
        ("opt/hai/repo", "/opt/hai/repo/", "/opt/../repo", "/opt/hai..repo", "/opt//hai", "/", "/opt/hai repo"),
    ),
    Row(
        "jobs",
        "openmetadata_ingestion_token_name",
        "/example_project/openmetadata_bot_ingestion",
        5,
        "/healthcare_hai_prediction/openmetadata_bot_ingestion",
        (
            "openmetadata_bot_ingestion",
            "/example_project/openmetadata_bot_other",
            "/example_project/nested/openmetadata_bot_ingestion",
            "/Example_Project/openmetadata_bot_ingestion",
            "/example_project/openmetadata_bot_ingestion/",
        ),
    ),
]
COMMITTED_DEFAULTS = [Row("iam", "github_oidc_enabled", True, "true", False)]
# Inputs that no row needs, with the reason. The storage retention flags have their own wrapper options and cases in run_e2e.
EXEMPT = {
    "mlflow_retention_enabled": "wrapper option --mlflow-retention, covered by run_e2e",
    "athena_retention_enabled": "wrapper option --athena-retention, covered by run_e2e",
    "allow_destructive_teardown": "teardown guard of the platform stack, never an activation",
    "bronze_writer_prepared": "required key of the committed IAM preparation file",
    "job_writer_e2e_prepared": "required key of the committed IAM preparation file",
    "host_job_memory_mib": "rendered by render_jobs_config from the job memory of the services stack output host_capacity_binding",
    "host_capacity_binding": "rendered by render_jobs_config from the services stack output host_capacity_binding",
}
TOOL = (
    f"#!{sys.executable}\nimport json, os, sys\n"
    "args = sys.argv[1:]\n"
    "with open(os.environ['STANDIN_CALLS'], 'a') as log:\n    log.write(json.dumps(args) + '\\n')\n"
    "if 'output' in args:\n    outputs = json.load(open(os.environ['STANDIN_OUTPUTS']))\n"
    "    named = args[-1] if args[-1] not in ('output', '-json') else None\n"
    "    sys.stdout.write(json.dumps(outputs[named]['value'] if named else outputs))\n"
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


def completeness() -> dict[str, bool]:
    """Every activation has preparation-file coverage, committed-default coverage or a stated reason for exemption."""
    covered = {(row.stack, row.name) for row in (*ROWS, *COMMITTED_DEFAULTS)}
    declared = {stack: variables_of(directory) for stack, directory in STACKS.items()}
    checks: dict[str, bool] = {}
    for stack, names in declared.items():
        missing = sorted(name for name, kind in names.items() if kind == "bool" and (stack, name) not in covered and name not in EXEMPT)
        checks[f"every boolean input of {stack} has preparation or committed-default coverage ({missing})"] = not missing
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
    size = re.search(r'variable "scratch_volume_gb" \{.*?>= (\d+) && var\.scratch_volume_gb <= (\d+)', platform, re.S)
    contract = schema.get("scratch_volume_gb", {})
    checks["the scratch size bounds of the platform contract equal the Terraform validation"] = (
        size is not None
        and contract.get("type") == "integer"
        and (contract.get("minimum"), contract.get("maximum")) == (int(size.group(1)), int(size.group(2)))
    )
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
        self.calls = self.root / "terraform_calls.jsonl"
        self.outputs = self.root / "standin_outputs.json"
        # The platform outputs the services renderer reads, and the services output host_capacity_binding the jobs renderer reads for host jobs
        # (TEST FIXTURE ONLY, not measured values).
        binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
        self.outputs.write_text(json.dumps({name: {"value": value} for name, value in {**SERVICES_FOUNDATION, "host_capacity_binding": binding}.items()}))
        for stack in ("services",):
            (self.root / STACKS[stack] / "images.auto.tfvars.json").parent.mkdir(parents=True, exist_ok=True)
            (self.root / STACKS[stack] / "images.auto.tfvars.json").write_text(json.dumps({"image_digests": {}}))
        shutil.copy2(ROOT / DEFAULT_PREPARATION, self.root / "config/aws/iam_preparation.json")
        self.default_iam = (self.root / "config/aws/iam_preparation.json").read_text()

    def environment(self) -> dict[str, str]:
        base = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "TF_"))}
        return {
            **base,
            "PATH": str(self.tools),
            "STANDIN_PLAN": str(self.plan),
            "STANDIN_OUTPUTS": str(self.outputs),
            "STANDIN_CALLS": str(self.calls),
            "PYTHON_BIN": sys.executable,
        }

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

    def wrapper(self, stack: str, variables: dict[str, Any] | None, *arguments: str) -> Any:
        """Run the wrapper with an inert identity command and Terraform command log. The default action is plan."""
        options = {"iam": ["--iam", "--admin-profile", ADMIN], "platform": ["--platform"], "services": ["--services"], "jobs": ["--jobs"]}[stack]
        if stack == "iam":
            self.command(stack, variables)
        elif variables is not None:
            options += self.reviewed(stack, variables)
        (self.root / ".env").write_text(self.environment_file.read_text())
        line = " ".join(shlex.quote(item) for item in ["scripts/infrastructure/terraform.sh", *options, *(arguments or ("plan",))])
        (self.root / "call_wrapper.sh").write_text(f"#!/bin/bash\nexec bash {line}\n")
        return run_command("bash", ["call_wrapper.sh"], cwd=self.root, env=self.environment() | {"PYTHON_BIN": sys.executable}, timeout=60)

    def written(self, stack: str) -> dict[str, Any] | None:
        path = self.root / OUTPUTS[stack]
        return json.loads(path.read_text()) if path.is_file() else None

    def clear(self, stack: str) -> None:
        (self.root / OUTPUTS[stack]).unlink(missing_ok=True)

    def called(self) -> list[list[str]]:
        return [json.loads(line) for line in self.calls.read_text().splitlines()] if self.calls.exists() else []


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


def committed_default_cases(workspace: Workspace, row: Row) -> dict[str, bool]:
    """Check a committed-default route through the real renderer and wrapper with synthetic saved plans and inert commands."""
    stack, name = row.stack, row.name
    label = f"{stack} {name} committed default"
    checks: dict[str, bool] = {}
    workspace.clear(stack)
    result = workspace.run(stack, None)
    deployment_path = workspace.root / STACKS[stack] / "deployment.auto.tfvars.json"
    deployment = json.loads(deployment_path.read_text())
    preparation = workspace.written(stack) or {}
    checks[f"{label}: rendering leaves activation to the committed default"] = result.returncode == 0 and name not in deployment and name not in preparation
    workspace.calls.unlink(missing_ok=True)
    result = workspace.wrapper(stack, None)
    directory = f"-chdir={workspace.root / STACKS[stack]}"
    checks[f"{label}: wrapper plan reaches terraform without an activation override"] = result.returncode == 0 and workspace.called() == [
        [directory, "plan", "-input=false", f"-var=admin_profile={ADMIN}"]
    ]
    for default in (False, True):
        prefix = f"{label} {str(default).lower()}"
        plan = plan_document({**deployment, **preparation}, {name: default}, {"admin_profile": ADMIN})
        workspace.plan.write_text(json.dumps(plan))
        result = workspace.run(stack, None, "--check-plan", str(workspace.plan))
        checks[f"{prefix}: the renderer accepts the matching saved plan"] = result.returncode == 0 and "Rendered private Terraform inputs" in result.stdout
        workspace.calls.unlink(missing_ok=True)
        result = workspace.wrapper(stack, None, "apply", str(workspace.plan))
        show = [directory, "show", "-json", str(workspace.plan)]
        checks[f"{prefix}: wrapper apply checks the plan before reaching terraform apply"] = result.returncode == 0 and workspace.called() == [
            show,
            [directory, "apply", "-input=false", str(workspace.plan)],
        ]
        checks[f"{prefix}: wrapper rendering keeps the activation out of both input files"] = name not in json.loads(
            deployment_path.read_text()
        ) and name not in (workspace.written(stack) or {})
        plan["variables"][name]["value"] = not default
        workspace.plan.write_text(json.dumps(plan))
        message = f"Saved plan input {name} is not its committed default"
        result = workspace.run(stack, None, "--check-plan", str(workspace.plan))
        checks[f"{prefix}: the renderer refuses an override of the committed default"] = (
            result.returncode == 2 and message in result.stderr and not result.stdout
        )
        workspace.calls.unlink(missing_ok=True)
        result = workspace.wrapper(stack, None, "apply", str(workspace.plan))
        checks[f"{prefix}: wrapper refuses the override before terraform apply"] = (
            result.returncode == 2 and message in result.stderr and not result.stdout and workspace.called() == [show]
        )
    for value in (row.default, row.good, row.bad):
        workspace.clear(stack)
        result = workspace.run(stack, {name: value})
        message = "The IAM preparation file must hold the four inputs"
        checks[f"{label}: preparation input {value!r} is refused by the closed contract"] = (
            result.returncode == 2 and message in result.stderr and workspace.written(stack) is None
        )
        workspace.calls.unlink(missing_ok=True)
        result = workspace.wrapper(stack, {name: value})
        checks[f"{label}: wrapper refuses preparation input {value!r} before terraform"] = (
            result.returncode == 2 and message in result.stderr and workspace.written(stack) is None and not workspace.called()
        )
    workspace.command(stack, None)
    return checks


def wrapper_cases(workspace: Workspace) -> dict[str, bool]:
    """One activation per stack through terraform.sh itself: it renders the reviewed value and then reaches the stand-in terraform."""
    checks: dict[str, bool] = {}
    for stack, name in (
        ("iam", "serve_prepared"),
        ("iam", "airflow_launch_prepared"),
        ("platform", "serving_enabled"),
        ("services", "model_api_enabled"),
        ("jobs", "serve_enabled"),
    ):
        workspace.clear(stack)
        result = workspace.wrapper(stack, {name: True})
        written = workspace.written(stack) or {}
        checks[f"wrapper {stack}: {name} set to true is rendered and plan runs"] = result.returncode == 0 and written.get(name) is True
        workspace.clear(stack)
        refused = workspace.wrapper(stack, {name: "true"})
        checks[f"wrapper {stack}: {name} set to a string is refused before terraform"] = refused.returncode == 2 and workspace.written(stack) is None
    return checks | launch_wrapper_cases(workspace) | launch_refusal_cases(workspace)


def launch_refusal_cases(workspace: Workspace) -> dict[str, bool]:
    """Each refused launch value fails for its own reason: the message says to set that input, not that the name is unknown."""
    checks: dict[str, bool] = {}
    for row in (item for item in ROWS if item.name.startswith("airflow_launch_") and item.refuse):
        for index, value in enumerate(row.refuse):
            workspace.clear("iam")
            result = workspace.run("iam", {row.name: value})
            checks[f"{row.name}: constrained value {index} is refused by the rule of that input"] = (
                result.returncode == 2 and f"Set {row.name} " in (result.stderr + result.stdout) and workspace.written("iam") is None
            )
    workspace.clear("iam")
    result = workspace.run("iam", {"airflow_launch_prepared": True, "airflow_launch_families": []})
    checks["airflow_launch_families: an empty list is refused while the launch role is prepared"] = (
        result.returncode == 2 and "Set airflow_launch_families " in (result.stderr + result.stdout) and workspace.written("iam") is None
    )
    workspace.clear("iam")
    result = workspace.run("iam", {"airflow_launch_families": []})
    checks["airflow_launch_families: an empty list is accepted while the launch role is not prepared"] = result.returncode == 0
    return checks


def launch_wrapper_cases(workspace: Workspace) -> dict[str, bool]:
    """The Airflow launch role through terraform.sh: an enabled plan with its families and prefix, a disabled plan, and the refusal of a -var override."""
    checks: dict[str, bool] = {}
    enabled = {"airflow_launch_prepared": True, "airflow_launch_families": ["serve", "train"], "airflow_launch_record_prefix": "example/records/"}
    workspace.clear("iam")
    result = workspace.wrapper("iam", enabled)
    checks["wrapper iam: an enabled launch role with its families and prefix is rendered whole and plan runs"] = result.returncode == 0 and all(
        (workspace.written("iam") or {}).get(key) == value for key, value in enabled.items()
    )
    workspace.clear("iam")
    result = workspace.wrapper("iam", {"airflow_launch_prepared": False})
    checks["wrapper iam: a disabled launch role is rendered as false and plan runs"] = (
        result.returncode == 0 and (workspace.written("iam") or {}).get("airflow_launch_prepared") is False
    )
    workspace.clear("iam")
    result = workspace.wrapper("iam", {"airflow_launch_prepared": True, "airflow_launch_families": []})
    checks["wrapper iam: an enabled launch role with no family is refused before terraform"] = result.returncode == 2 and workspace.written("iam") is None
    workspace.clear("iam")
    result = workspace.wrapper("iam", None, "plan", "-var=airflow_launch_prepared=true")
    checks["wrapper iam: a -var override of the launch flag is refused"] = result.returncode == 2 and workspace.written("iam") is None
    return checks


def scratch_size_cases(workspace: Workspace) -> dict[str, bool]:
    """The reviewed scratch volume size through the renderer and the wrapper: its bounds, and a saved plan that must carry exactly the reviewed size."""
    checks: dict[str, bool] = {}
    stack, name = "platform", "scratch_volume_gb"
    siblings = [row for row in ROWS if row.stack == stack]
    off = {row.name: (False if isinstance(row.good, bool) else row.default) for row in siblings if isinstance(row.good, bool) or row.default is not None}
    for size in (1, 60, 120, 1024):
        workspace.clear(stack)
        result = workspace.run(stack, {name: size})
        checks[f"scratch size {size} GiB: the bounded whole number renders exactly"] = (
            result.returncode == 0 and (workspace.written(stack) or {}).get(name) == size
        )
    workspace.clear(stack)
    result = workspace.wrapper(stack, {name: 120})
    checks["wrapper platform: a reviewed 120 GiB scratch size is rendered and plan runs"] = (
        result.returncode == 0 and (workspace.written(stack) or {}).get(name) == 120
    )
    invalid: tuple[Any, ...] = (0, 1025, 1.5, "120")
    for refused_size in invalid:
        workspace.clear(stack)
        result = workspace.wrapper(stack, {name: refused_size})
        checks[f"wrapper platform: scratch size {refused_size!r} is refused before terraform"] = result.returncode == 2 and workspace.written(stack) is None
    workspace.clear(stack)
    workspace.run(stack, {name: 120})
    rendered = dict(workspace.written(stack) or {})
    directory = f"-chdir={workspace.root / STACKS[stack]}"
    for planned, accepted in ((120, True), (60, False), (100, False), (1024, False)):
        workspace.plan.write_text(json.dumps(plan_document({**rendered, name: planned}, off)))
        result = workspace.run(stack, {name: 120}, "--check-plan", str(workspace.plan))
        checks[f"scratch size plan {planned} against reviewed 120: the renderer {'accepts' if accepted else 'refuses'} it"] = (
            result.returncode == 0 if accepted else result.returncode == 2 and "Saved plan inputs differ" in result.stderr
        )
        workspace.calls.unlink(missing_ok=True)
        result = workspace.wrapper(stack, {name: 120}, "apply", str(workspace.plan))
        show = [directory, "show", "-json", str(workspace.plan)]
        expected = [show, [directory, "apply", "-input=false", str(workspace.plan)]] if accepted else [show]
        checks[f"scratch size plan {planned} against reviewed 120: the wrapper {'applies' if accepted else 'stops before apply for'} it"] = (
            result.returncode == (0 if accepted else 2) and workspace.called() == expected
        )
    return checks


def scratch_root_cases(workspace: Workspace) -> dict[str, bool]:
    """The jobs scratch root through the renderer and the wrapper: only the scratch mount boot creates, or a folder inside it, is admitted."""
    checks: dict[str, bool] = {}
    stack, name = "jobs", "host_scratch_root"
    for root in ("/data/job_scratch", "/data/job_scratch/spill"):
        workspace.clear(stack)
        result = workspace.run(stack, {name: root})
        checks[f"scratch root {root}: the scratch mount and a folder in it render exactly"] = (
            result.returncode == 0 and (workspace.written(stack) or {}).get(name) == root
        )
    for root in ("/data/scratch_a", "/data"):
        workspace.clear(stack)
        result = workspace.run(stack, {name: root})
        checks[f"scratch root {root}: the renderer refuses a folder of the data volume and writes nothing"] = (
            result.returncode == 2 and workspace.written(stack) is None
        )
        workspace.clear(stack)
        workspace.calls.unlink(missing_ok=True)
        result = workspace.wrapper(stack, {name: root})
        checks[f"wrapper jobs: scratch root {root} is refused before terraform"] = (
            result.returncode == 2 and workspace.written(stack) is None and not workspace.called()
        )
    workspace.clear(stack)
    result = workspace.wrapper(stack, {name: "/data/job_scratch"})
    checks["wrapper jobs: the default scratch root is rendered and plan runs"] = (
        result.returncode == 0 and (workspace.written(stack) or {}).get(name) == "/data/job_scratch"
    )
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
        results["scratch_size"] = scratch_size_cases(workspace)
        results["scratch_root"] = scratch_root_cases(workspace)
        results["committed_defaults"] = {name: ok for row in COMMITTED_DEFAULTS for name, ok in committed_default_cases(workspace, row).items()}
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
