"""The services and jobs inputs are the Terraform defaults plus the environment values the renderers build from .env, stack outputs and image digests."""

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest

from orchestration import ecs_preflight
from scripts.infrastructure import render_jobs_config as jobs_renderer
from scripts.infrastructure import render_platform_config as services_renderer
from scripts.infrastructure import services_apply, stack_settings
from scripts.process import CompletedProcess, run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
SERVICES_LIVE = FIXTURES / "services_live_inputs_sanitized.json"
JOBS_LIVE = FIXTURES / "jobs_live_inputs_sanitized.json"
IMAGE_RECORD_FIXTURE = FIXTURES / "image_record_sanitized.json"
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


OUTPUTS_ASKED: list[str] = []


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


def reviewed(tmp_path: Path, root: str, variables: dict[str, Any], at: Path | None = None) -> list[str]:
    """Write an overrides file, by default outside the synthetic repository (which the renderers accept without a Git check), and return its flags."""
    folder = tmp_path.with_name(f"{tmp_path.name}_overrides")
    folder.mkdir(exist_ok=True)
    path = at or folder / f"reviewed_{root}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schema_version": 1, "root": root, "variables": variables}), encoding="utf-8")
    return ["--preparation-inputs", str(path), "--preparation-sha256", hashlib.sha256(path.read_bytes()).hexdigest()]


def render_services(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *extra: str, project: str = PROJECT, bucket: str = BUCKET) -> dict[str, Any]:
    """Run the services render command line in a synthetic workspace with the platform outputs of the live inputs."""
    live = json.loads(json.dumps(restore(load(SERVICES_LIVE))).replace(PROJECT, project))
    outputs = {name: {"value": live[name]} for name in services_renderer.SERVICES_PLATFORM_KEYS}
    monkeypatch.setattr(services_renderer, "terraform_document", lambda *arguments, **keywords: outputs)
    monkeypatch.setattr(services_renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["renderer", "--stack", "services", "--env-file", str(write_environment(tmp_path, project, bucket)), *extra])
    services_renderer.main()
    return load(tmp_path / "infra" / "services" / "deployment.auto.tfvars.json")


def image_record(tmp_path: Path, drop: tuple[str, ...] = (), malformed: tuple[str, ...] = ()) -> Path:
    """A copy of the explicit record fixture (the digests the live jobs run) with the named images removed or given a malformed digest."""
    digests = {
        name: ("sha256:short" if name in malformed else digest) for name, digest in load(IMAGE_RECORD_FIXTURE)["image_digests"].items() if name not in drop
    }
    path = tmp_path.with_name(f"{tmp_path.name}_record.json")
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
    answers = stack_outputs() if outputs is None else outputs
    requested: list[str] = []

    def terraform(program: str, arguments: list[str], **keywords: Any) -> CompletedProcess[str]:
        stack, name = Path(arguments[0].removeprefix("-chdir=")).name, arguments[-1]
        requested.append(name)
        return CompletedProcess([program, *arguments], 0, json.dumps(answers[(stack, name)]), "")

    monkeypatch.setattr(jobs_renderer, "run_command", terraform)
    monkeypatch.setattr(jobs_renderer, "JOBS_DIRECTORY", tmp_path / "infra" / "jobs")
    monkeypatch.setattr(jobs_renderer, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(jobs_renderer, "IMAGE_RECORD", record or image_record(tmp_path))
    monkeypatch.setattr(sys, "argv", ["renderer", "--env-file", str(write_environment(tmp_path, project)), *extra])
    jobs_renderer.main()
    OUTPUTS_ASKED[:] = requested
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
    unneeded = {"host_private_ip", "alerts_topic_arn", "openmetadata_images"}
    check(
        overridden == {name: value for name, value in plain.items() if name not in unneeded} | {"publisher_job_enabled": False, "host_jobs": ["silver"]},
        "the named inputs change and the inputs of the host jobs left out go with them",
    )


@pytest.mark.parametrize("name", ["analytics", "feature_store", "openmetadata_ingestion"])
def test_an_unrecorded_image_digest_renders_nothing_and_names_the_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], name: str
) -> None:
    with pytest.raises(SystemExit):
        render_jobs(tmp_path, monkeypatch, record=image_record(tmp_path, drop=(name,)))
    error = capsys.readouterr().err
    check(name in error and "push_images" in error, "the error names the image and the command that records it")
    check(not (tmp_path / "infra" / "jobs").exists(), "nothing is rendered")


@pytest.mark.parametrize("answer", ["job_memory_mib=1 usable_memory_mib=32768 system_reserve_mib=2048 service_reservations_mib=7920", "not a binding"])
def test_an_unusable_job_room_renders_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], answer: str) -> None:
    with pytest.raises(SystemExit):
        render_jobs(tmp_path, monkeypatch, outputs=stack_outputs(answer))
    check("job room" in capsys.readouterr().err and not (tmp_path / "infra" / "jobs").exists(), "the render refuses by name and writes nothing")


def project_outputs() -> dict[tuple[str, str], object]:
    return stack_outputs() | {("platform", "alerts_topic_arn"): f"arn:aws:sns:us-west-2:{ACCOUNT}:example_project_alerts_dev"}


def namespaced_variables(stack: str) -> set[str]:
    """The variables of a stack whose committed default names a Parameter Store path of the default project, read from its source without descriptions."""
    names: set[str] = set()
    for source in sorted((ROOT / "infra" / stack).glob("*.tf")):
        for block in re.finditer(r'^variable "(\w+)" \{\n(.*?)^\}\n', source.read_text(encoding="utf-8"), re.S | re.M):
            lines = [line for line in block.group(2).splitlines() if not line.lstrip().startswith(("description", "condition", "error_message"))]
            if any(f"/{PROJECT}/" in line for line in lines):
                names.add(block.group(1))
    return names


@pytest.mark.parametrize("stack", ["services", "jobs"])
def test_every_project_bound_parameter_name_follows_the_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stack: str) -> None:
    rendered = (
        render_services(tmp_path, monkeypatch, project="example_project")
        if stack == "services"
        else render_jobs(tmp_path, monkeypatch, project="example_project", outputs=project_outputs())
    )
    names = namespaced_variables(stack)
    check(len(names) >= 5, f"the {stack} source holds project-bound parameter names")
    check(names <= rendered.keys(), f"project-bound variables not rendered for the project: {sorted(names - rendered.keys())}")
    check(PROJECT not in json.dumps(rendered), f"the rendered {stack} inputs still name the default project")


def test_the_hold_and_lock_names_follow_the_project() -> None:
    check(services_apply.hold_parameter_name("example_project") == "/example_project/host_launch_hold", "the hold name follows the project")
    check(services_apply.lock_parameter_name("example_project") == "/example_project/services_apply_lock", "the lock name follows the project")
    check(services_apply.hold_parameter_name(PROJECT) == f"/{PROJECT}/host_launch_hold", "the default project keeps the committed hold name")


SILVER_ONLY = {
    "publisher_job_enabled": False,
    "train_enabled": False,
    "drift_enabled": False,
    "promote_enabled": False,
    "serve_enabled": False,
    "host_jobs": ["silver"],
    "layer_jobs": ["silver"],
}


def test_a_silver_only_selection_needs_only_its_own_images_and_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    record = image_record(tmp_path, drop=("training", "feature_store", "drift", "openmetadata_ingestion"))
    binding = stack_outputs()[("services", "host_capacity_binding")]
    rendered = render_jobs(
        tmp_path, monkeypatch, *reviewed(tmp_path, "jobs", SILVER_ONLY), record=record, outputs={("services", "host_capacity_binding"): binding}
    )
    check(OUTPUTS_ASKED == ["host_capacity_binding"], f"only the job room is read, not {OUTPUTS_ASKED}")
    check(rendered["images"].keys() == {"analytics", "quality", "spark"}, "the layer images are rendered")
    unused = {"train_images", "drift_images", "openmetadata_images", "host_private_ip", "alerts_topic_arn"}
    check(not unused & rendered.keys(), f"inputs of disabled jobs are not rendered: {sorted(unused & rendered.keys())}")


def test_a_selection_without_host_jobs_or_publisher_reads_no_job_room(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    selection = SILVER_ONLY | {"host_jobs_enabled": False}
    rendered = render_jobs(tmp_path, monkeypatch, *reviewed(tmp_path, "jobs", selection), outputs={})
    check(OUTPUTS_ASKED == [] and not rendered.keys() & JOBS_STACK_OUTPUTS, "no stack output is read and none is rendered")


def test_an_incomplete_or_stale_image_record_names_every_image(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    record = image_record(tmp_path, drop=("feature_store", "drift"), malformed=("spark",))
    with pytest.raises(SystemExit):
        render_jobs(tmp_path, monkeypatch, record=record)
    error = capsys.readouterr().err
    check(
        all(name in error for name in ("feature_store", "drift", "spark", "push_images")), "each missing or malformed image and the recording command are named"
    )
    check(not (tmp_path / "infra" / "jobs").exists(), "nothing is rendered")


def test_the_image_record_fixture_holds_the_digests_the_live_jobs_run() -> None:
    digests = load(IMAGE_RECORD_FIXTURE)["image_digests"]
    live = load(JOBS_LIVE)
    check(set(digests) == set(IMAGE_RECORD_NAMES.values()), "the fixture names exactly the jobs images")
    for (group, key), name in IMAGE_RECORD_NAMES.items():
        check(live[group][key].endswith(f"@{digests[name]}"), f"the {name} digest is the one the live jobs run")


def git(root: Path, *arguments: str) -> None:
    check(run_command("git", ["-C", str(root), *arguments], timeout=30).returncode == 0, f"git {arguments[0]} failed")


@pytest.mark.parametrize("stack", ["services", "jobs"])
@pytest.mark.parametrize(("location", "accepted"), [("tracked_overrides.json", False), ("loose_overrides.json", False), ("overrides/reviewed.json", True)])
def test_an_overrides_file_inside_the_repository_must_be_untracked_and_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], stack: str, location: str, accepted: bool
) -> None:
    git(tmp_path, "init", "-q")
    (tmp_path / ".gitignore").write_text("overrides/\n", encoding="utf-8")
    variables = {"monitoring_enabled": False} if stack == "services" else {"publisher_job_enabled": False}
    flags = reviewed(tmp_path, stack, variables, at=tmp_path / location)
    if location.startswith("tracked"):
        git(tmp_path, "add", location)
    render = render_services if stack == "services" else render_jobs
    if accepted:
        render(tmp_path, monkeypatch, *flags)
        return
    with pytest.raises(SystemExit):
        render(tmp_path, monkeypatch, *flags)
    check("untracked" in capsys.readouterr().err, "the refusal says the file must be untracked and ignored")


def no_change_plan(*actions: list[str]) -> dict[str, Any]:
    changes = [{"address": f'aws_ecs_task_definition.host["family_{index}"]', "change": {"actions": action}} for index, action in enumerate(actions)]
    return {"complete": True, "errored": False, "resource_changes": changes}


def run_no_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plan: dict[str, Any], sha256: str | None = None) -> int:
    record = tmp_path / "images.auto.tfvars.json"
    record.write_bytes(IMAGE_RECORD_FIXTURE.read_bytes())
    monkeypatch.setattr(stack_settings, "IMAGE_RECORD", record)
    monkeypatch.setattr(stack_settings, "run_command", lambda program, arguments, **keywords: CompletedProcess([program, *arguments], 0, json.dumps(plan), ""))
    digest = sha256 or hashlib.sha256(IMAGE_RECORD_FIXTURE.read_bytes()).hexdigest()
    monkeypatch.setattr(sys, "argv", ["check", "--stack", "jobs", "--plan", str(tmp_path / "jobs.tfplan"), "--image-record-sha256", digest])
    return stack_settings.main()


def test_a_plan_without_changes_made_with_the_recorded_images_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    check(run_no_change(tmp_path, monkeypatch, no_change_plan(["no-op"], ["no-op"])) == 0, "the check passes")
    line = capsys.readouterr().out
    check(hashlib.sha256(IMAGE_RECORD_FIXTURE.read_bytes()).hexdigest() in line and "changes=0" in line, "the evidence line carries the record digest")


@pytest.mark.parametrize("actions", [["update"], ["create"], ["delete", "create"]])
def test_a_plan_that_changes_a_resource_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], actions: list[str]
) -> None:
    with pytest.raises(SystemExit):
        run_no_change(tmp_path, monkeypatch, no_change_plan(["no-op"], actions))
    check("family_1" in capsys.readouterr().err, "the refusal names the changed resource")


def test_a_plan_made_with_another_image_record_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        run_no_change(tmp_path, monkeypatch, no_change_plan(["no-op"]), sha256="0" * 64)
    check("differs" in capsys.readouterr().err, "the refusal says the record differs")


def test_an_errored_or_incomplete_plan_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for plan in ({**no_change_plan(["no-op"]), "errored": True}, {**no_change_plan(["no-op"]), "complete": False}, {"complete": True}):
        with pytest.raises(SystemExit):
            run_no_change(tmp_path, monkeypatch, plan)


def test_the_job_room_constants_equal_the_services_default() -> None:
    cap = terraform_default("services", "host_job_cap_mib")
    check(
        cap == ecs_preflight.JOB_ROOM_MEMORY_MIB, f"orchestration/ecs_preflight.py names {ecs_preflight.JOB_ROOM_MEMORY_MIB} MiB, the services default is {cap}"
    )
    host = load(ROOT / "config/orchestration/host_jobs.json")
    whole = {name: row["memory_mib"] for name, row in host["jobs"].items() if not row.get("reserve")}
    check(whole and set(whole.values()) == {cap}, f"every whole-room row of config/orchestration/host_jobs.json says {cap}: {whole}")
    check(f"{cap:,} MiB" in host["reserve_note"] and "26,816" not in host["reserve_note"], "the reserve note names the services default")
