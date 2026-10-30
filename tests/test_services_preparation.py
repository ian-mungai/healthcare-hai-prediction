"""Reviewed services inputs: the MLflow switch and its addresses reach the saved-plan check only through a file with a reviewed digest."""

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import render_platform_config as renderer
from scripts.infrastructure.render_project_config import ConfigurationError
from tests.support import check

ROLE = "arn:aws:iam::111111111111:role/example_project_mlflow_task_dev"
VARIABLES: dict[str, Any] = {
    "mlflow_enabled": True,
    "mlflow_task_role_arn": ROLE,
    "mlflow_host_ip": "10.0.1.20",
    "mlflow_artifact_bucket": "example-project-data-dev",
}


def reviewed(tmp_path: Path, variables: dict[str, Any] | None = None, root: str = "services") -> tuple[Path, str]:
    path = tmp_path / "reviewed_services.json"
    path.write_text(json.dumps({"schema_version": 1, "root": root, "variables": VARIABLES if variables is None else variables}), encoding="utf-8")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_reviewed_file_with_its_digest_is_read(tmp_path: Path) -> None:
    path, digest = reviewed(tmp_path)
    check(renderer.services_preparation(path, digest) == VARIABLES, "the reviewed variables are returned unchanged")


def test_no_file_means_no_inputs() -> None:
    check(renderer.services_preparation(None, None) == {}, "without a file the stack keeps its committed defaults")


def test_file_and_digest_travel_together(tmp_path: Path) -> None:
    path, digest = reviewed(tmp_path)
    for arguments in ((path, None), (None, digest)):
        with pytest.raises(ConfigurationError):
            renderer.services_preparation(*arguments)


def test_changed_bytes_are_refused(tmp_path: Path) -> None:
    path, _ = reviewed(tmp_path)
    with pytest.raises(ConfigurationError) as refusal:
        renderer.services_preparation(path, "0" * 64)
    check("SHA-256" in str(refusal.value), "the message names the digest")


@pytest.mark.parametrize(
    "change",
    [
        {"image_digests": {}},
        {"cluster_name": "example"},
        {"mlflow_enabled": "true"},
        {"mlflow_host_ip": "203.0.113.9"},
        {"mlflow_host_ip": "mlflow.example.com"},
        {"mlflow_task_role_arn": "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"},
        {"mlflow_artifact_bucket": "Not_A_Bucket"},
    ],
)
def test_inputs_outside_the_contract_are_refused(tmp_path: Path, change: dict[str, Any]) -> None:
    path, digest = reviewed(tmp_path, VARIABLES | change)
    with pytest.raises(ConfigurationError) as refusal:
        renderer.services_preparation(path, digest)
    check("preparation contract" in str(refusal.value), "the message names the contract")


def test_other_root_is_refused(tmp_path: Path) -> None:
    path, digest = reviewed(tmp_path, root="jobs")
    with pytest.raises(ConfigurationError):
        renderer.services_preparation(path, digest)


def plan(**values: Any) -> dict[str, Any]:
    declared = {"mlflow_enabled": {"default": False}, "mlflow_host_ip": {"default": None}, "aws_region": {}}
    return {
        "complete": True,
        "errored": False,
        "configuration": {"root_module": {"variables": declared}},
        "variables": {name: {"value": value} for name, value in values.items()},
    }


def test_saved_plan_with_the_reviewed_switch_is_accepted_only_with_the_reviewed_file() -> None:
    expected = {"aws_region": "us-west-2", "mlflow_enabled": True, "mlflow_host_ip": "10.0.1.20"}
    renderer.verify_platform_plan(plan(aws_region="us-west-2", mlflow_enabled=True, mlflow_host_ip="10.0.1.20"), expected, "services")
    with pytest.raises(ConfigurationError):
        renderer.verify_platform_plan(plan(aws_region="us-west-2", mlflow_enabled=True, mlflow_host_ip="10.0.1.20"), {"aws_region": "us-west-2"}, "services")
