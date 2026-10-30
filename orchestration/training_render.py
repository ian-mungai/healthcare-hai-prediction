"""Render local training jobs from a verified snapshot and action-specific inputs."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from orchestration import code_digest, training_contract
from orchestration.pipeline_render import JobSpec, PipelineInput
from orchestration.pipeline_session import RoleSession
from scripts.training.step_records import verify_done


def environment_kind(template: str, params: dict[str, Any], mode: str) -> str:
    """Select the credential kind from the closed action matrix."""
    return training_contract.action_environment(template, str(params.get("action", "")), mode)


def role_for(template: str, params: dict[str, Any], mode: str) -> str | None:
    """Return only the reader or DVC writer role for a real extraction or versioning job."""
    kind = environment_kind(template, params, mode)
    return {"lakehouse_read": "reader", "dvc_write": "dvc_writer"}.get(kind) if mode == "real" else None


def build(inp: PipelineInput, values: dict[str, str], session: RoleSession | None) -> JobSpec:
    """Build a fixed module command, isolated input mounts and the class-scoped environment."""
    if inp.run_folder is None or inp.args or inp.run_mode not in ("fixture", "real"):
        raise ValueError("training jobs require a run and accept no caller command")
    folder = inp.run_folder
    if set(inp.params) != {"action", "parameters"}:
        raise ValueError("training jobs accept only their fixed action and validated parameters")
    state = inp.pipeline
    binding = state.get("training", {})
    parameters = training_contract.parameters_for(str(binding.get("dag_id", "")), inp.params["parameters"])
    if parameters != binding.get("parameters"):
        raise ValueError("training parameters differ from the run's acquired identity")
    if json.loads((folder / "training_parameters.json").read_bytes()) != parameters:
        raise ValueError("training parameter file differs from the run's acquired identity")
    if inp.run_mode == "fixture" and state.get("registry_kind") != "test":
        raise ValueError("fixture training requires a test registry")
    action = str(inp.params["action"])
    kind = environment_kind(inp.template, dict(inp.params), inp.run_mode)
    record = code_digest.verify_snapshot(folder, folder.name)
    recorded = state.get("training_snapshot", {})
    if recorded.get("phase") != "complete" or recorded.get("record_sha256") != hashlib.sha256((folder / code_digest.RECORD).read_bytes()).hexdigest():
        raise ValueError("training snapshot record differs from the launcher's completion")
    for step in training_contract.PREREQUISITES[action]:
        verify_done(folder / "outputs" / step, step, record["code_digest"])
    output = folder / "outputs" / action
    output.mkdir(parents=True, exist_ok=True)
    mounts = [(str(path), f"/workspace/{path.name}", True) for path in sorted((folder / "code").iterdir())]
    mounts += [
        (str(folder / code_digest.RECORD), "/workspace/snapshot_training.json", True),
        (str(folder / "training_parameters.json"), "/workspace/training_parameters.json", True),
        (str(output), "/workspace/out", False),
    ]
    extracts = folder / "outputs/extract"
    if action in ("version_extract", "fit", "mediation", "log_run", "promotion_check"):
        for hai_type in parameters.get("hai_types", []):
            for filename in (f"extract_{hai_type}.parquet", f"manifest_{hai_type}.json"):
                mounts.append((str(extracts / filename), f"/workspace/in/extract/{filename}", True))
    auxiliary = {
        "version_extract": ("mediator.parquet", "hac.parquet", "adjacency.parquet"),
        "fit": ("adjacency.parquet",),
        "mediation": ("mediator.parquet",),
        "hac_validate": ("hac.parquet",),
    }
    for filename in auxiliary.get(action, ()):
        mounts.append((str(extracts / filename), f"/workspace/in/extract/{filename}", True))
    for step in training_contract.INPUT_STEPS[action]:
        if step != "extract":
            mounts.append((str(folder / "outputs" / step), f"/workspace/in/{step}", True))
    environment = {key: str(inp.values[key]) for key in ("JOB_THREADS", "DUCKDB_MEMORY_LIMIT") if key in inp.values}
    environment.update(
        {
            "PYTHONHASHSEED": "0",
            "PYTHONDONTWRITEBYTECODE": "1",
            "MLFLOW_DISABLE_TELEMETRY": "true",
            "DO_NOT_TRACK": "true",
            "DVC_NO_ANALYTICS": "1",
            "OMP_NUM_THREADS": environment.get("JOB_THREADS", "1"),
            "OPENBLAS_NUM_THREADS": environment.get("JOB_THREADS", "1"),
            "MKL_NUM_THREADS": environment.get("JOB_THREADS", "1"),
            "TRAINING_ENVIRONMENT_KIND": kind,
            "TMPDIR": "/workspace/tmp",
        }
    )
    arguments = [
        action,
        "--run",
        folder.name,
        "--mode",
        inp.run_mode,
        "--run-dir",
        "/workspace/out",
        "--parameters",
        "/workspace/training_parameters.json",
        "--snapshot-record",
        "/workspace/snapshot_training.json",
    ]
    spec = JobSpec(
        "feature-store" if action == "extract" else "training",
        "scripts.features.training_extract" if action == "extract" else "scripts.training.job",
        arguments,
        environment,
        mounts,
    )
    role = role_for(inp.template, dict(inp.params), inp.run_mode)
    if role is None and session is not None or role is not None and (session is None or session.role != role):
        raise ValueError("training credentials do not match the action's environment kind")
    if kind == "mlflow":
        network = state.get("training_mlflow", {})
        if network.get("phase") != "ready" or not network.get("name"):
            raise ValueError("the run's isolated MLflow network is not ready")
        spec.network, spec.network_class = str(network["name"]), "mlflow"
        environment.update({"MLFLOW_TRACKING_URI": "http://mlflow:5100", "MLFLOW_HTTP_REQUEST_TIMEOUT": "20", "MLFLOW_HTTP_REQUEST_MAX_RETRIES": "0"})
        if action == "promote":
            mounts.append((str(folder / "promotion_approval.json"), "/workspace/approval.json", True))
            arguments += ["--approval", "/workspace/approval.json"]
    elif kind in ("lakehouse_read", "dvc_write"):
        network = state.get("network", {})
        selected = "lakehouse_read" if kind == "lakehouse_read" else "lakehouse_write"
        if network.get("phase") != "ready" or selected not in network.get("names", {}):
            raise ValueError("the real training action has no enforced S3 network")
        spec.network, spec.network_class = str(network["names"][selected]), selected
        if session is None:
            raise ValueError("a real training read or DVC write requires its role session")
        environment.update(session.environment(values["AWS_REGION"]))
        environment.update({"HTTP_PROXY": "http://hai-egress:3128", "HTTPS_PROXY": "http://hai-egress:3128", "NO_PROXY": "polaris,localhost,127.0.0.1"})
        if kind == "lakehouse_read":
            environment.update(
                {
                    "POLARIS_URI": "http://polaris:8181/api/catalog",
                    "POLARIS_CLIENT_ID": values["POLARIS_READER_CLIENT_ID"],
                    "POLARIS_CLIENT_SECRET": values["POLARIS_READER_CLIENT_SECRET"],
                }
            )
        else:
            environment["DVC_REMOTE_URL"] = f"s3://{values['DATA_BUCKET_NAME']}/dvc/"
    if action == "extract":
        environment.update({"FEAST_RUN_FOLDER": "/workspace/tmp", "FEAST_NAMESPACE": "features_e2e" if inp.run_mode == "fixture" else "features"})
        if inp.run_mode == "fixture":
            publication = state.get("training_publication", {})
            if not publication.get("local_catalog") or not publication.get("registry"):
                raise ValueError("fixture extraction needs the coordinating agent's synthetic publication and applied Feast registry")
            mounts += [
                (str(publication["local_catalog"]), "/workspace/local_catalog", True),
                (str(publication["registry"]), "/workspace/tmp/feast/registry.pb", True),
            ]
            environment.update({"FEAST_CATALOG_TYPE": "sql", "FEAST_REGISTRY_PATH": "/workspace/tmp/feast/registry.pb"})
        else:
            environment.update({"FEAST_CATALOG_TYPE": "rest", "FEAST_REGISTRY_PATH": f"s3://{values['DATA_BUCKET_NAME']}/lakehouse/feast/registry.pb"})
    if action == "version_extract" and inp.run_mode == "fixture":
        remote = folder / "dvc_remote"
        remote.mkdir(exist_ok=True)
        mounts.append((str(remote), "/workspace/dvc_remote", False))
        environment["DVC_REMOTE_URL"] = "/workspace/dvc_remote"
    return spec
