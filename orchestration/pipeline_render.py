"""Build fixed layer-job specifications without performing Docker or catalog operations."""

from __future__ import annotations

import configparser
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from orchestration import network_policy
from orchestration.pipeline_protocol import PipelineError, accepted_completion, read_record
from scripts.lakehouse.gold_publish_record import SHA256


class PipelineInput(Protocol):
    """The run metadata a layer renderer reads from the launcher's render input."""

    template: str
    run_folder: Path | None
    run_mode: str
    args: Sequence[str]
    params: Mapping[str, Any]
    pipeline: Mapping[str, Any]
    values: Mapping[str, str]
    snapshot: Path | None
    provenance: Mapping[str, str]
    generation: int


SILVER_WRITER = ("POLARIS_SILVER_WRITER_CLIENT_ID", "POLARIS_SILVER_WRITER_CLIENT_SECRET")
GOLD_WRITER = ("POLARIS_GOLD_WRITER_CLIENT_ID", "POLARIS_GOLD_WRITER_CLIENT_SECRET")
READER = ("POLARIS_READER_CLIENT_ID", "POLARIS_READER_CLIENT_SECRET")


@dataclass
class JobSpec:
    """A fixed image, command, environment and mount set for the launcher's assembler."""

    service: str
    module: str
    arguments: list[str]
    environment: dict[str, str]
    mounts: list[tuple[str, str, bool]]
    network: str = "none"
    network_class: str = "offline"
    extra: dict[str, Any] = field(default_factory=dict)


def job_class(template: str, params: dict[str, Any], mode: str) -> str:
    """Return the enforced class of a closed job in its run mode."""
    if template == "pipeline_probe":
        value = str(params.get("class", ""))
        if value not in (*network_policy.CLASSES, "offline"):
            raise PipelineError("the probe class is unknown")
        return value
    if mode == "publish_fixture":
        return "offline"
    if template == "pipeline_dbt":
        return "lakehouse_read" if params.get("layer") == "silver" else "offline"
    if template == "pipeline_spark":
        return "lakehouse_read"
    if template == "pipeline_publish":
        return "lakehouse_read" if params.get("step") == "reconcile" else "lakehouse_write"
    return "offline"


def needed_values(template: str, params: dict[str, Any], mode: str) -> tuple[str, ...]:
    """Name only the credential and region entries the selected class needs."""
    selected = job_class(template, params, mode)
    if selected == "offline" or template == "pipeline_probe":
        return ()
    principal = READER if selected == "lakehouse_read" else SILVER_WRITER if params.get("layer") == "silver" else GOLD_WRITER
    profile = "PIPELINE_READER_AWS_PROFILE" if selected == "lakehouse_read" else "PIPELINE_WRITER_AWS_PROFILE"
    return ("AWS_REGION", profile, *principal)


def scoped_credentials(folder: Path, profile: str) -> None:
    """Require standalone files containing exactly the selected role's static profile."""
    for basename, section in (("config", f"profile {profile}"), ("credentials", profile)):
        path = folder / basename
        if folder.is_symlink() or path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
            raise PipelineError("the class-scoped AWS files are missing or not private")
        parser = configparser.RawConfigParser()
        try:
            parser.read_string(path.read_text())
        except (OSError, configparser.Error):
            raise PipelineError("the class-scoped AWS file is unreadable") from None
        if parser.defaults() or parser.sections() != [section]:
            raise PipelineError("a class-scoped AWS file must contain exactly its selected profile")
        allowed = {"region", "output"} if basename == "config" else {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"}
        keys = set(parser[section])
        if not keys <= allowed or (basename == "credentials" and not {"aws_access_key_id", "aws_secret_access_key"} <= keys):
            raise PipelineError("the selected profile contains an unsupported credential source")


@dataclass
class BuildContext:
    """Fixed run metadata and resource settings shared by the pipeline renderers."""

    inp: PipelineInput
    checkout: Path
    folder: Path
    params: dict[str, Any]
    state: Mapping[str, Any]
    selected: str
    environment: dict[str, str]
    mounts: list[tuple[str, str, bool]]
    common: list[str]


def dbt_spec(context: BuildContext) -> JobSpec:
    """Return the fixed pipeline_dbt job specification."""
    inp = context.inp
    params = context.params
    state = context.state
    environment = context.environment
    mounts = context.mounts
    common = context.common
    if set(params) != {"layer"} or params["layer"] not in ("silver", "gold") or inp.snapshot is None:
        raise PipelineError("a layer build requires a frozen project snapshot and a known layer")
    layer = str(params["layer"])
    source = str(state.get("source_sha256", ""))
    input_sha = str(state.get("silver_pin", {}).get("sha256", "")) if layer == "gold" else str(state.get("bronze_sha256", ""))
    if not SHA256.fullmatch(source) or not SHA256.fullmatch(input_sha):
        raise PipelineError("the layer build has no accepted source and input digests")
    mounts += [(str(inp.snapshot / "project"), "/workspace/dbt", True), (str(inp.snapshot / "packages"), "/workspace/packages", True)]
    environment.update(
        {
            "DBT_PACKAGES_INSTALL_PATH": "/workspace/packages",
            "DBT_TARGET_PATH": "/workspace/out/target",
            "DBT_LOG_PATH": "/workspace/out/logs",
            "STAGING_E2E_CASE": "/workspace/out",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }
    )
    if layer == "silver" and inp.run_mode == "real":
        environment.update(inp.provenance)
    arguments = [*common, "--layer", layer, "--source-sha256", source, "--input-sha256", input_sha]
    if layer == "gold":
        extracted = state.get("silver_input", {})
        if extracted.get("pins_sha256") != input_sha:
            raise PipelineError("the accepted Iceberg extract does not match the silver pin")
        arguments += ["--extract-sha256", str(extracted["sha256"])]
    spec = JobSpec("analytics-dbt", "scripts.lakehouse.pipeline_build", arguments, environment, mounts)
    return spec


def export_spec(context: BuildContext) -> JobSpec:
    """Return the fixed pipeline_export job specification."""
    inp = context.inp
    folder = context.folder
    params = context.params
    state = context.state
    environment = context.environment
    mounts = context.mounts
    if set(params) != {"layer"} or params["layer"] not in ("silver", "gold"):
        raise PipelineError("the export layer is unknown")
    layer = str(params["layer"])
    completion = accepted_completion({"run_id": folder.name, "generation": inp.generation, "layer_completions": state.get("layer_completions", {})}, layer)
    publish = folder / "publish"
    publish.mkdir(exist_ok=True)
    mounts = [mount for mount in mounts if mount[1] != "/workspace/out"]
    mounts += [(str(folder / completion["file"]), "/workspace/sealed/sealed.duckdb", True), (str(publish), "/workspace/out/publish", False)]
    arguments = ["--run", folder.name, "--sealed-sha256", completion["sha256"]]
    spec = JobSpec("quality", f"scripts.lakehouse.{layer}_export", arguments, environment, mounts)
    return spec


def spark_spec(context: BuildContext) -> JobSpec:
    """Return the fixed pipeline_spark job specification."""
    inp = context.inp
    checkout = context.checkout
    folder = context.folder
    params = context.params
    state = context.state
    environment = context.environment
    mounts = context.mounts
    common = context.common
    if set(params) - {"job", "source_run"} or params.get("job") not in (
        "pin_silver",
        "read_silver",
        "record_silver_snapshot",
        "pin_bronze",
        "verify_bronze",
    ):
        raise PipelineError("the snapshot job is unknown")
    job = params["job"]
    if "source_run" in params and job != "pin_silver":
        raise PipelineError("only a silver pin selects a producer")
    if job in ("pin_bronze", "verify_bronze"):
        arguments = ["pin" if job == "pin_bronze" else "verify", "--run", folder.name]
        module = "scripts.lakehouse.pins"
        mounts += [
            (str(checkout / "config/lakehouse"), "/workspace/config/lakehouse", True),
            (str(folder), f"/workspace/data/analytics/dbt/runs/{folder.name}", False),
        ]
    elif job == "read_silver":
        pin = state.get("silver_pin", {})
        arguments = [*common, "--expected-pin-sha256", str(pin.get("sha256", ""))]
        module = "scripts.lakehouse.silver_input"
    else:
        step = "pin" if job == "pin_silver" else "record"
        arguments = [step, *common, "--record", "/workspace/out/silver_pins.json" if job == "pin_silver" else "/workspace/out/silver_snapshot.json"]
        if job == "pin_silver" and params.get("source_run") not in (None, "latest"):
            arguments += ["--source-run", str(params["source_run"])]
        module = "scripts.lakehouse.silver_publish"
    if inp.run_mode == "publish_fixture":
        arguments += ["--local-warehouse", "/workspace/local_catalog/warehouse"]
        mounts.append((str(state["local_catalog"]), "/workspace/local_catalog", False))
    spec = JobSpec("spark", module, arguments, environment, mounts)
    return spec


def publish_spec(context: BuildContext) -> JobSpec:
    """Return the fixed pipeline_publish job specification."""
    inp = context.inp
    checkout = context.checkout
    folder = context.folder
    params = context.params
    state = context.state
    environment = context.environment
    mounts = context.mounts
    common = context.common
    if (
        set(params) != {"layer", "step"}
        or params["layer"] not in ("silver", "gold")
        or params["step"] not in ("candidates", "reconcile", "backup", "commit", "maintenance")
    ):
        raise PipelineError("the publication step is unknown")
    layer, step = str(params["layer"]), str(params["step"])
    if step == "maintenance":
        if layer != "gold":
            raise PipelineError("only gold maintenance is supported")
        module = "scripts.lakehouse.gold_maintenance"
        arguments = [*common, "--protected", "/workspace/out/publish/protected.json"]
    else:
        completion = accepted_completion({"run_id": folder.name, "generation": inp.generation, "layer_completions": state.get("layer_completions", {})}, layer)
        acceptance = state.get("layer_exports", {}).get(layer)
        if not isinstance(acceptance, dict) or acceptance.get("sealed_sha256") != completion["sha256"] or acceptance.get("generation") != inp.generation:
            raise PipelineError("the layer has no accepted export in this generation")
        _, current = read_record(folder, "publish/consumer_result.json")
        if current != acceptance["sha256"]:
            raise PipelineError("the accepted export result changed")
        arguments = [step, *common, "--sealed-sha256", completion["sha256"], "--accepted-result-sha256", current]
        module = f"scripts.lakehouse.{layer}_publish"
    if inp.run_mode == "publish_fixture":
        arguments += ["--local-warehouse", "/workspace/local_catalog/warehouse"]
        mounts.append((str(state["local_catalog"]), "/workspace/local_catalog", False))
    elif step == "backup":
        mounts.append((str(checkout / "infra/deployment.auto.tfvars.json"), "/workspace/infra/deployment.auto.tfvars.json", True))
    spec = JobSpec("spark", module, arguments, environment, mounts)
    return spec


def probe_spec(context: BuildContext) -> JobSpec:
    """Return the fixed pipeline_probe job specification."""
    params = context.params
    selected = context.selected
    environment = context.environment
    mounts = [mount for mount in context.mounts if mount[1] != "/workspace/out"]
    if set(params) != {"class", "case"} or params["case"] not in ("traffic", "catalog"):
        raise PipelineError("the traffic probe case is unknown")
    spec = JobSpec("analytics-dbt", "scripts.orchestration.traffic_probe", ["--case", str(params["case"]), "--class", selected], environment, mounts)
    return spec


def apply_credentials(context: BuildContext, credentials: Path, values: dict[str, str], spec: JobSpec) -> None:
    """Mount only the selected class profile and inject its catalog principal."""
    inp = context.inp
    params = context.params
    selected = context.selected
    profile_key = "PIPELINE_READER_AWS_PROFILE" if selected == "lakehouse_read" else "PIPELINE_WRITER_AWS_PROFILE"
    role = "reader" if selected == "lakehouse_read" else "writer"
    if profile_key not in values:
        raise PipelineError("the selected AWS profile is missing")
    scoped_credentials(credentials / role, values[profile_key])
    for basename in ("config", "credentials"):
        credential_file = credentials / role / basename
        if credential_file.is_symlink() or not credential_file.is_file() or credential_file.stat().st_mode & 0o077:
            raise PipelineError("the class-scoped AWS files are missing or not private")
        spec.mounts.append((str(credential_file), f"/run/pipeline/aws/{basename}", True))
    principal = READER if role == "reader" else SILVER_WRITER if params.get("layer") == "silver" else GOLD_WRITER
    if any(key not in values for key in (*principal, profile_key, "AWS_REGION")):
        raise PipelineError("the class-scoped catalog principal or AWS profile is missing")
    spec.environment.update(
        {
            "AWS_PROFILE": values[profile_key],
            "AWS_REGION": values["AWS_REGION"],
            "AWS_CONFIG_FILE": "/run/pipeline/aws/config",
            "AWS_SHARED_CREDENTIALS_FILE": "/run/pipeline/aws/credentials",
        }
    )
    if inp.template == "pipeline_dbt":
        spec.environment.update({"DBT_ENV_SECRET_POLARIS_CLIENT_ID": values[principal[0]], "DBT_ENV_SECRET_POLARIS_CLIENT_SECRET": values[principal[1]]})
    else:
        spec.environment.update(
            {
                "POLARIS_URI": "http://polaris:8181/api/catalog",
                "POLARIS_CLIENT_ID": values[principal[0]],
                "POLARIS_CLIENT_SECRET": values[principal[1]],
                "JAVA_TOOL_OPTIONS": (
                    "-Dhttps.proxyHost=hai-egress -Dhttps.proxyPort=3128 "
                    "-Dhttp.proxyHost=hai-egress -Dhttp.proxyPort=3128 -Dhttp.nonProxyHosts=polaris|localhost|127.*"
                ),
            }
        )


def apply_network(context: BuildContext, credentials: Path, values: dict[str, str], spec: JobSpec) -> None:
    """Attach the enforced network and class-scoped credentials of this run."""
    inp = context.inp
    selected = context.selected
    state = context.state
    if selected != "offline":
        network = state.get("network", {})
        if network.get("phase") != "ready" or selected not in network.get("names", {}):
            raise PipelineError("the requested network class is not enforced for this run")
        spec.network = str(network["names"][selected])
        spec.environment.update(
            {
                "HTTP_PROXY": "http://hai-egress:3128",
                "HTTPS_PROXY": "http://hai-egress:3128",
                "http_proxy": "http://hai-egress:3128",
                "https_proxy": "http://hai-egress:3128",
                "NO_PROXY": "polaris,localhost,127.0.0.1",
                "no_proxy": "polaris,localhost,127.0.0.1",
            }
        )
        if inp.template != "pipeline_probe":
            apply_credentials(context, credentials, values, spec)


def build(inp: PipelineInput, checkout: Path, credentials: Path, values: dict[str, str]) -> JobSpec:
    """Construct a fixed job from authoritative run metadata; refuse caller arguments and unknown parameters."""
    if inp.run_folder is None or inp.run_mode not in ("publish_fixture", "real") or inp.args:
        raise PipelineError("a pipeline job requires a publish-mode run and no caller command")
    if inp.run_mode == "publish_fixture" and inp.pipeline.get("registry_kind") != "test":
        raise PipelineError("publish_fixture requires a test registry")
    folder = inp.run_folder
    params = dict(inp.params)
    state = inp.pipeline
    selected = job_class(inp.template, params, inp.run_mode)
    environment = {key: str(inp.values[key]) for key in ("JOB_THREADS", "DUCKDB_MEMORY_LIMIT", "SPARK_JOB_MEMORY") if key in inp.values}
    mounts = [
        (str(checkout / "scripts"), "/workspace/scripts", True),
        (str(checkout / "orchestration"), "/workspace/orchestration", True),
        (str(checkout / "data_contracts/lakehouse"), "/workspace/data_contracts/lakehouse", True),
        (str(folder), "/workspace/out", False),
    ]
    common = ["--run", folder.name, "--mode", inp.run_mode]
    context = BuildContext(inp, checkout, folder, params, state, selected, environment, mounts, common)
    handlers = {
        "pipeline_dbt": dbt_spec,
        "pipeline_export": export_spec,
        "pipeline_spark": spark_spec,
        "pipeline_publish": publish_spec,
        "pipeline_probe": probe_spec,
    }
    handler = handlers.get(inp.template)
    if handler is None:
        raise PipelineError("the pipeline template is unknown")
    spec = handler(context)
    spec.network_class = selected
    apply_network(context, credentials, values, spec)
    return spec
