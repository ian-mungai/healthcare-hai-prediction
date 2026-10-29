"""Build fixed layer-job specifications without performing Docker or catalog operations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from orchestration import network_policy, online_store
from orchestration.pipeline_protocol import PipelineError, accepted_completion, read_record
from orchestration.pipeline_session import CLASS_ROLE, RoleSession
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
FEATURES_WRITER = ("POLARIS_FEATURES_WRITER_CLIENT_ID", "POLARIS_FEATURES_WRITER_CLIENT_SECRET")
READER = ("POLARIS_READER_CLIENT_ID", "POLARIS_READER_CLIENT_SECRET")
WRITERS = {"silver": SILVER_WRITER, "gold": GOLD_WRITER, "features": FEATURES_WRITER}
BUILD_LAYERS = ("silver", "gold", "features")
# Each layer's build input: the accepted pin (or bronze digest) its build reads and the accepted extract of that pin (none for silver).
BUILD_INPUTS = {"gold": ("silver_pin", "silver_input"), "features": ("gold_pin", "gold_input")}
# Spark snapshot jobs that write one record: (module, sub-command, the record file). Only the pins (PIN_JOBS) select a producer run.
SPARK_JOBS = {
    "pin_silver": ("scripts.lakehouse.silver_publish", "pin", "/workspace/out/silver_pins.json"),
    "record_silver_snapshot": ("scripts.lakehouse.silver_publish", "record", "/workspace/out/silver_snapshot.json"),
    "pin_gold": ("scripts.features.features_input", "pin", "/workspace/out/gold_pins.json"),
    "record_features_snapshot": ("scripts.features.features_publish", "record", "/workspace/out/features_snapshot.json"),
}
PIN_JOBS = ("pin_silver", "pin_gold")
FEAST_STEPS = {"publish_fixture": ("apply", "materialize"), "real": ("apply", "materialize", "probe")}
FEAST_NAMESPACES = {"publish_fixture": "features_e2e", "real": "features"}
# Real Feast jobs name the S3 key of their registry; the job builds the URI from the bucket of the mounted deployment file (design 3.1, 6.5).
FEAST_REGISTRY_KEYS = {"probe": "lakehouse/feast_e2e/registry.pb", "apply": "lakehouse/feast/registry.pb", "materialize": "lakehouse/feast/registry.pb"}


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
    if template in ("pipeline_spark", "pipeline_feast"):
        return "lakehouse_read"
    if template == "pipeline_publish":
        return "lakehouse_read" if params.get("step") == "reconcile" else "lakehouse_write"
    return "offline"


def needed_values(template: str, params: dict[str, Any], mode: str) -> tuple[str, ...]:
    """Name only the catalog principal and region entries the selected class needs; the AWS access comes from a role session."""
    selected = job_class(template, params, mode)
    if selected == "offline" or template == "pipeline_probe":
        return ()
    principal = READER if selected == "lakehouse_read" else writer_principal(params)
    return ("AWS_REGION", *principal)


def session_role(template: str, params: dict[str, Any], mode: str) -> str | None:
    """The one role whose session the job needs, or None for a job that needs no AWS access. Only a real run gets one."""
    if mode != "real":
        return None
    selected = job_class(template, params, mode)
    if selected == "offline" or template == "pipeline_probe":
        return None
    return CLASS_ROLE[selected]


def writer_principal(params: Mapping[str, Any]) -> tuple[str, str]:
    """The catalog writer of the job's layer; a layer without a writer is refused, never given another layer's writer."""
    principal = WRITERS.get(str(params.get("layer")))
    if principal is None:
        raise PipelineError("the publication layer has no catalog writer")
    return principal


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
    if set(params) != {"layer"} or params["layer"] not in BUILD_LAYERS or inp.snapshot is None:
        raise PipelineError("a layer build requires a frozen project snapshot and a known layer")
    layer = str(params["layer"])
    source = str(state.get("source_sha256", ""))
    pin_key, extract_key = BUILD_INPUTS.get(layer, ("", ""))
    input_sha = str(state.get(pin_key, {}).get("sha256", "")) if pin_key else str(state.get("bronze_sha256", ""))
    if not SHA256.fullmatch(source) or not SHA256.fullmatch(input_sha):
        raise PipelineError("the layer build has no accepted source and input digests")
    mounts += [(str(inp.snapshot / "project"), "/workspace/dbt", True), (str(inp.snapshot / "packages"), "/workspace/packages", True)]
    if layer == "features":
        # features_build hashes the schema file into feature_build_inputs (design 1.4); only the features build reads it.
        mounts.append((str(context.checkout / "data_contracts/features"), "/workspace/data_contracts/features", True))
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
    if extract_key:
        extracted = state.get(extract_key, {})
        if extracted.get("pins_sha256") != input_sha:
            raise PipelineError(f"the accepted Iceberg extract does not match the {pin_key.removesuffix('_pin')} pin")
        arguments += ["--extract-sha256", str(extracted["sha256"])]
    # The module assignments stay literal: scripts/quality/container_mounts.py reads them to check the imports of every entry point.
    module = "scripts.lakehouse.pipeline_build"
    if layer == "features":
        module = "scripts.features.features_build"
    spec = JobSpec("analytics-dbt", module, arguments, environment, mounts)
    return spec


def export_spec(context: BuildContext) -> JobSpec:
    """Return the fixed pipeline_export job specification."""
    inp = context.inp
    folder = context.folder
    params = context.params
    state = context.state
    environment = context.environment
    mounts = context.mounts
    if set(params) != {"layer"} or params["layer"] not in BUILD_LAYERS:
        raise PipelineError("the export layer is unknown")
    layer = str(params["layer"])
    completion = accepted_completion({"run_id": folder.name, "generation": inp.generation, "layer_completions": state.get("layer_completions", {})}, layer)
    publish = folder / "publish"
    publish.mkdir(exist_ok=True)
    mounts = [mount for mount in mounts if mount[1] != "/workspace/out"]
    mounts += [(str(folder / completion["file"]), "/workspace/sealed/sealed.duckdb", True), (str(publish), "/workspace/out/publish", False)]
    arguments = ["--run", folder.name, "--sealed-sha256", completion["sha256"]]
    module = f"scripts.lakehouse.{layer}_export"
    if layer == "features":
        module = "scripts.features.features_export"
    spec = JobSpec("quality", module, arguments, environment, mounts)
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
        "pin_gold",
        "read_gold",
        "record_features_snapshot",
    ):
        raise PipelineError("the snapshot job is unknown")
    job = params["job"]
    if "source_run" in params and job not in PIN_JOBS:
        raise PipelineError("only a silver or gold pin selects a producer")
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
    elif job == "read_gold":
        pin = state.get("gold_pin", {})
        if not SHA256.fullmatch(str(pin.get("sha256", ""))):
            raise PipelineError("the gold read has no accepted gold pin")
        arguments = ["read", *common, "--expected-pin-sha256", str(pin["sha256"])]
        module = "scripts.features.features_input"
    else:
        module, step, record = SPARK_JOBS[job]
        arguments = [step, *common, "--record", record]
        if job in PIN_JOBS and params.get("source_run") not in (None, "latest"):
            arguments += ["--source-run", str(params["source_run"])]
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
        or params["layer"] not in BUILD_LAYERS
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
        if layer == "features":
            module = "scripts.features.features_publish"
    if inp.run_mode == "publish_fixture":
        arguments += ["--local-warehouse", "/workspace/local_catalog/warehouse"]
        mounts.append((str(state["local_catalog"]), "/workspace/local_catalog", False))
    elif step == "backup":
        mounts.append((str(checkout / "infra/deployment.auto.tfvars.json"), "/workspace/infra/deployment.auto.tfvars.json", True))
    spec = JobSpec("spark", module, arguments, environment, mounts)
    return spec


def feast_spec(context: BuildContext) -> JobSpec:
    """Return the fixed pipeline_feast job specification (design 3.4, 4.3 and 5): Feast apply, materialization or the real-registry probe."""
    inp = context.inp
    checkout = context.checkout
    folder = context.folder
    params = context.params
    if set(params) != {"step"} or params["step"] not in FEAST_STEPS[inp.run_mode]:
        raise PipelineError("the Feast step is unknown in this run mode")
    step = str(params["step"])
    mounts = [
        (str(checkout / "scripts"), "/workspace/scripts", True),
        (str(checkout / "orchestration"), "/workspace/orchestration", True),
        (str(checkout / "feature_store"), "/workspace/feature_store", True),
        (str(checkout / "data_contracts/features"), "/workspace/data_contracts/features", True),
        (str(folder), "/workspace/out", False),
    ]
    environment = dict(context.environment)
    environment.update({"FEAST_NAMESPACE": FEAST_NAMESPACES[inp.run_mode], "FEAST_REDIS_HOST": "redis"})
    extra: dict[str, Any] = {}
    network = "none"
    if step != "probe":
        # Apply and materialization reach Redis only on the launcher's internal online-store network; the launcher resolves the password reference.
        online = context.state.get("online_store", {})
        if online.get("phase") != "up" or not str(online.get("network", "")).startswith("hai-"):
            raise PipelineError("the online store is not up for this Feast step")
        name = str(online["network"])
        network = name if inp.run_mode == "publish_fixture" else "none"
        extra["online_store"] = {"password_env": online_store.AUTH_ENV, "joins": {} if inp.run_mode == "publish_fixture" else {name: []}}
    if inp.run_mode == "publish_fixture":
        # The fixture SQL catalog's metadata points at the local warehouse; Feast only reads it.
        if not context.state.get("local_catalog"):
            raise PipelineError("the fixture Feast job has no local catalog")
        mounts.append((str(context.state["local_catalog"]), "/workspace/local_catalog", True))
        environment.update({"FEAST_CATALOG_TYPE": "sql", "FEAST_REGISTRY_PATH": "/workspace/out/feast/registry.pb"})
    else:
        environment.update({"FEAST_CATALOG_TYPE": "rest", "FEAST_REGISTRY_KEY": FEAST_REGISTRY_KEYS[step]})
        mounts.append((str(checkout / "infra/deployment.auto.tfvars.json"), "/workspace/infra/deployment.auto.tfvars.json", True))
    return JobSpec("feature-store", "scripts.features.feast_job", [step, *context.common], environment, mounts, network=network, extra=extra)


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


def apply_credentials(context: BuildContext, values: dict[str, str], spec: JobSpec, session: RoleSession | None) -> None:
    """Give the job its role session as environment values and inject its catalog principal. No AWS file is mounted."""
    inp = context.inp
    params = context.params
    selected = context.selected
    if session is None:
        raise PipelineError("the class needs a role session and none was given")
    if session.role != CLASS_ROLE[selected]:
        raise PipelineError("the role session belongs to another class")
    principal = READER if selected == "lakehouse_read" else writer_principal(params)
    if any(key not in values for key in (*principal, "AWS_REGION")):
        raise PipelineError("the class-scoped catalog principal or AWS region is missing")
    spec.environment.update(session.environment(values["AWS_REGION"]))
    if inp.template == "pipeline_dbt":
        spec.environment.update({"DBT_ENV_SECRET_POLARIS_CLIENT_ID": values[principal[0]], "DBT_ENV_SECRET_POLARIS_CLIENT_SECRET": values[principal[1]]})
    elif inp.template == "pipeline_feast":
        spec.environment.update(
            {"POLARIS_URI": "http://polaris:8181/api/catalog", "POLARIS_CLIENT_ID": values[principal[0]], "POLARIS_CLIENT_SECRET": values[principal[1]]}
        )
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


def apply_network(context: BuildContext, values: dict[str, str], spec: JobSpec, session: RoleSession | None) -> None:
    """Attach the enforced network and the class-scoped credentials of this run."""
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
            apply_credentials(context, values, spec, session)


def require_publish_run(inp: PipelineInput) -> Path:
    """The run folder of a publish-mode job; refuse any other run or a caller command, before any contract, session or credential step."""
    if inp.run_folder is None or inp.run_mode not in ("publish_fixture", "real") or inp.args:
        raise PipelineError("a pipeline job requires a publish-mode run and no caller command")
    return inp.run_folder


def build(inp: PipelineInput, checkout: Path, values: dict[str, str], session: RoleSession | None) -> JobSpec:
    """Construct a fixed job from authoritative run metadata; refuse caller arguments and unknown parameters.

    ``session`` is the role session of the job's class. A job that needs no AWS access, and every fixture run, refuses one.
    """
    folder = require_publish_run(inp)
    if inp.run_mode == "publish_fixture" and inp.pipeline.get("registry_kind") != "test":
        raise PipelineError("publish_fixture requires a test registry")
    params = dict(inp.params)
    state = inp.pipeline
    selected = job_class(inp.template, params, inp.run_mode)
    if session is not None and (selected == "offline" or inp.template == "pipeline_probe" or inp.run_mode == "publish_fixture"):
        raise PipelineError("a job that needs no AWS access, and a fixture run, never receive a role session")
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
        "pipeline_feast": feast_spec,
    }
    handler = handlers.get(inp.template)
    if handler is None:
        raise PipelineError("the pipeline template is unknown")
    spec = handler(context)
    spec.network_class = selected
    apply_network(context, values, spec, session)
    return spec
