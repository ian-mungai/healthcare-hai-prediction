"""The services and jobs inputs are the Terraform defaults plus the environment values the renderers build from .env, stack outputs and image digests."""

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import render_jobs_config as jobs_renderer
from scripts.infrastructure import render_platform_config as services_renderer
from scripts.process import CompletedProcess
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
SERVICES_LIVE = FIXTURES / "services_live_inputs_sanitized.json"
JOBS_LIVE = FIXTURES / "jobs_live_inputs_sanitized.json"
ACCOUNT = "111111111111"
BUCKET = "example-data-bucket"
HOST_IP = "10.0.0.5"
PROJECT = "healthcare_hai_prediction"
SERVICES_SETTINGS = {
    "airflow_lineage_http_enabled",
    "host_job_cap_mib",
    "mlflow_enabled",
    "model_api_enabled",
    "model_api_hai_types",
    "model_api_mlflow_uri",
    "monitoring_enabled",
    "openmetadata_persistent_enabled",
    "streamlit_enabled",
    "streamlit_hai_types",
}
JOBS_SETTINGS = {
    "drift_enabled",
    "host_jobs",
    "host_jobs_enabled",
    "host_repo_path",
    "layer_jobs",
    "layer_jobs_enabled",
    "promote_enabled",
    "publisher_job_enabled",
    "publisher_keys_enabled",
    "serve_enabled",
    "serve_manifest_prefix",
    "train_enabled",
}
# The jobs inputs that come from stack outputs, never from .env or a file.
JOBS_STACK_OUTPUTS = {"host_capacity_binding", "host_job_memory_mib", "host_private_ip", "alerts_topic_arn"}
IMAGE_RECORD_NAMES = {
    ("images", "analytics"): "analytics",
    ("images", "quality"): "quality",
    ("images", "spark"): "spark",
    ("train_images", "training"): "training",
    ("train_images", "feature_store"): "feature_store",
    ("openmetadata_images", "ingestion"): "openmetadata_ingestion",
    ("drift_images", "drift"): "drift",
}


def environment(project: str = PROJECT) -> dict[str, str]:
    return {
        "AWS_ACCOUNT_ID": ACCOUNT,
        "AWS_PROFILE": f"{project}_dev",
        "AWS_REGION": "us-west-2",
        "ENVIRONMENT": "dev",
        "PROJECT_NAME": project,
        "S3_BUCKET": BUCKET,
    }


def write_environment(tmp_path: Path, project: str = PROJECT, bucket: str = BUCKET) -> Path:
    path = tmp_path / ".env"
    path.write_text("\n".join(f"{key}={value}" for key, value in {**environment(project), "S3_BUCKET": bucket}.items()) + "\n", encoding="utf-8")
    return path


def sanitize(document: object) -> Any:
    text = json.dumps(document, sort_keys=True).replace(ACCOUNT, "<account>").replace(BUCKET, "<bucket>").replace(HOST_IP, "<host_ip>")
    return json.loads(text)


def restore(document: object) -> Any:
    return json.loads(json.dumps(document).replace("<account>", ACCOUNT).replace("<bucket>", BUCKET).replace("<host_ip>", HOST_IP))


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def terraform_default(stack: str, name: str) -> Any:
    """The committed default of one input variable: a one-line bool, number, string or list of strings, read from the stack's source."""
    for source in sorted((ROOT / "infra" / stack).glob("*.tf")):
        block = re.search(rf'^variable "{name}" \{{\n(.*?)^\}}\n', source.read_text(encoding="utf-8"), re.S | re.M)
        if block is not None:
            default = re.search(r"^\s+default\s*=\s*(.+)$", block.group(1), re.M)
            if default is None:
                raise AssertionError(f"{name} has no default")
            return json.loads(default.group(1).strip())
    raise AssertionError(f"{name} is not declared in infra/{stack}")


def terraform_defaults(stack: str, names: set[str]) -> dict[str, Any]:
    return {name: terraform_default(stack, name) for name in names}


def as_sets(document: dict[str, Any]) -> dict[str, Any]:
    """Terraform plans the set-typed inputs host_jobs and layer_jobs in sorted order, so their lists compare sorted."""
    return {name: sorted(value) if name in {"host_jobs", "layer_jobs"} else value for name, value in document.items()}


def reviewed(tmp_path: Path, root: str, variables: dict[str, Any]) -> list[str]:
    path = tmp_path / f"reviewed_{root}.json"
    path.write_text(json.dumps({"schema_version": 1, "root": root, "variables": variables}), encoding="utf-8")
    return ["--preparation-inputs", str(path), "--preparation-sha256", hashlib.sha256(path.read_bytes()).hexdigest()]


def render_services(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *extra: str, project: str = PROJECT, bucket: str = BUCKET) -> dict[str, Any]:
    """Run the services render command line in a synthetic workspace with the platform outputs of the live inputs."""
    live = restore(load(SERVICES_LIVE))
    outputs = {name: {"value": live[name]} for name in services_renderer.SERVICES_PLATFORM_KEYS}
    monkeypatch.setattr(services_renderer, "terraform_document", lambda *arguments, **keywords: outputs)
    monkeypatch.setattr(services_renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "services", "--env-file", str(write_environment(tmp_path, project, bucket)), *extra])
    services_renderer.main()
    return load(tmp_path / "infra" / "services" / "deployment.auto.tfvars.json")


def image_record(tmp_path: Path, drop: str | None = None) -> Path:
    """The pushed image digests, each repository with its own digest taken from the live inputs."""
    live = load(JOBS_LIVE)
    digests = {name: live[group][key].split("@")[1] for (group, key), name in IMAGE_RECORD_NAMES.items() if name != drop}
    path = tmp_path / "images.auto.tfvars.json"
    path.write_text(json.dumps({"image_digests": digests}), encoding="utf-8")
    return path


def stack_outputs(binding: str | None = None) -> dict[tuple[str, str], object]:
    live = restore(load(JOBS_LIVE))
    return {
        ("services", "host_capacity_binding"): binding or live["host_capacity_binding"],
        ("platform", "host_private_ip"): live["host_private_ip"],
        ("platform", "alerts_topic_arn"): live["alerts_topic_arn"],
    }


def render_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *extra: str,
    project: str = PROJECT,
    record: Path | None = None,
    outputs: dict[tuple[str, str], object] | None = None,
) -> dict[str, Any]:
    """Run the jobs render command line in a synthetic workspace with stand-ins for the services and platform outputs."""
    answers = outputs or stack_outputs()

    def terraform(program: str, arguments: list[str], **keywords: Any) -> CompletedProcess[str]:
        stack, name = Path(arguments[0].removeprefix("-chdir=")).name, arguments[-1]
        return CompletedProcess([program, *arguments], 0, json.dumps(answers[(stack, name)]), "")

    monkeypatch.setattr(jobs_renderer, "run_command", terraform)
    monkeypatch.setattr(jobs_renderer, "JOBS_DIRECTORY", tmp_path / "infra" / "jobs")
    monkeypatch.setattr(jobs_renderer, "IMAGE_RECORD", record or image_record(tmp_path))
    monkeypatch.setattr(sys, "argv", ["renderer", "--env-file", str(write_environment(tmp_path, project)), *extra])
    jobs_renderer.main()
    return load(tmp_path / "infra" / "jobs" / "deployment.auto.tfvars.json")


def test_services_inputs_are_the_committed_defaults_plus_the_environment_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    live = load(SERVICES_LIVE)
    rendered = render_services(tmp_path, monkeypatch)
    check(not SERVICES_SETTINGS & rendered.keys(), "the render writes none of the settings that are Terraform defaults")
    check(
        sanitize(rendered) == {name: value for name, value in live.items() if name not in SERVICES_SETTINGS}, "the render holds exactly the other live inputs"
    )
    check(sanitize(rendered) | terraform_defaults("services", SERVICES_SETTINGS) == live, "the render and the committed defaults equal the live inputs")


def test_jobs_inputs_are_the_committed_defaults_plus_the_environment_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    live = load(JOBS_LIVE)
    rendered = render_jobs(tmp_path, monkeypatch)
    check(not JOBS_SETTINGS & rendered.keys(), "the render writes none of the settings that are Terraform defaults")
    check(sanitize(rendered) == {name: value for name, value in live.items() if name not in JOBS_SETTINGS}, "the render holds exactly the other live inputs")
    check(
        as_sets(sanitize(rendered) | terraform_defaults("jobs", JOBS_SETTINGS)) == as_sets(live), "the render and the committed defaults equal the live inputs"
    )
    check(rendered.keys() >= JOBS_STACK_OUTPUTS, "the job room, the binding, the host address and the alerts topic come from the stack outputs")


def test_the_live_fixtures_hold_no_account_number_bucket_or_host_address() -> None:
    for path in (SERVICES_LIVE, JOBS_LIVE):
        text = path.read_text(encoding="utf-8")
        check(
            not re.search(r"[0-9]{12}", text) and "imungai" not in text and not re.search(r"\b10\.[0-9]+\.[0-9]+\.[0-9]+\b", text),
            f"{path.name} holds no private value",
        )


def test_services_environment_values_follow_the_project_and_the_bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rendered = render_services(tmp_path, monkeypatch, project="example_project", bucket="other-bucket")
    roles = {
        name: value
        for name, value in rendered.items()
        if name.endswith("_role_arn") and name not in {"execution_role_arn", "polaris_task_role_arn", "control_plane_task_role_arn"}
    }
    check(
        len(roles) == 7 and all(value.startswith(f"arn:aws:iam::{ACCOUNT}:role/example_project_") and value.endswith("_dev") for value in roles.values()),
        "roles follow the project",
    )
    buckets = {rendered[name] for name in ("airflow_launch_bucket_name", "code_bucket_name", "mlflow_artifact_bucket")}
    check(buckets == {"other-bucket"}, "every bucket input is the .env bucket")
    check(
        rendered["model_api_manifest_uri"].startswith("s3://other-bucket/") and rendered["model_api_registry_uri"].startswith("s3://other-bucket/"),
        "the URIs name the bucket",
    )


def test_jobs_environment_values_follow_the_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rendered = render_jobs(
        tmp_path,
        monkeypatch,
        project="example_project",
        outputs={key: value for key, value in stack_outputs().items()}
        | {("platform", "alerts_topic_arn"): f"arn:aws:sns:us-west-2:{ACCOUNT}:example_project_alerts_dev"},
    )
    check(rendered["log_group_name"] == "example_project_jobs_dev", "the log group names the project")
    check(
        rendered["writer_parameter_names"]["write_silver_task"]
        == ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"],
        "writer pairs",
    )
    check(rendered["job_role_arns"]["execution"] == f"arn:aws:iam::{ACCOUNT}:role/example_project_job_execution_dev", "roles name the project")
    check(
        rendered["openmetadata_images"]["ingestion"].startswith(
            f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:"
        ),
        "images name it",
    )


def test_a_second_render_writes_the_same_bytes_and_check_mode_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    render_services(tmp_path, monkeypatch)
    target = tmp_path / "infra" / "services" / "deployment.auto.tfvars.json"
    first = target.read_bytes()
    render_services(tmp_path, monkeypatch)
    check(target.read_bytes() == first, "a rerun writes the same bytes")
    render_services(tmp_path, monkeypatch, "--check")
    with pytest.raises(SystemExit):
        render_services(tmp_path, monkeypatch, "--check", bucket="other-bucket")


def test_one_off_services_overrides_take_the_named_file_and_leave_every_other_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plain = render_services(tmp_path, monkeypatch)
    overridden = render_services(tmp_path, monkeypatch, *reviewed(tmp_path, "services", {"monitoring_enabled": False, "host_job_cap_mib": 20000}))
    check(overridden == plain | {"monitoring_enabled": False, "host_job_cap_mib": 20000}, "only the named inputs change")


def test_one_off_jobs_overrides_take_the_named_file_and_leave_every_other_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plain = render_jobs(tmp_path, monkeypatch)
    overridden = render_jobs(tmp_path, monkeypatch, *reviewed(tmp_path, "jobs", {"publisher_job_enabled": False, "host_jobs": ["silver"]}))
    check(overridden == plain | {"publisher_job_enabled": False, "host_jobs": ["silver"]}, "only the named inputs change")


@pytest.mark.parametrize("name", ["analytics", "feature_store", "openmetadata_ingestion"])
def test_an_unrecorded_image_digest_renders_nothing_and_names_the_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], name: str
) -> None:
    with pytest.raises(SystemExit):
        render_jobs(tmp_path, monkeypatch, record=image_record(tmp_path, drop=name))
    error = capsys.readouterr().err
    check(name in error and "push_images" in error, "the error names the image and the command that records it")
    check(not (tmp_path / "infra" / "jobs").exists(), "nothing is rendered")


@pytest.mark.parametrize("answer", ["job_memory_mib=1 usable_memory_mib=32768 system_reserve_mib=2048 service_reservations_mib=7920", "not a binding"])
def test_an_unusable_job_room_renders_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], answer: str) -> None:
    with pytest.raises(SystemExit):
        render_jobs(tmp_path, monkeypatch, outputs=stack_outputs(answer))
    check("job room" in capsys.readouterr().err and not (tmp_path / "infra" / "jobs").exists(), "the render refuses by name and writes nothing")
