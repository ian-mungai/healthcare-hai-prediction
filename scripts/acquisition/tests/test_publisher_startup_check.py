"""The publisher job's startup check, through its real entry point, with stand-ins for the sockets and AWS."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import collect_mmd_api, mmd_api_contract, publisher_run
from scripts.acquisition import redownload as harness
from scripts.acquisition.source_registry import canonical_hash, load_registry
from scripts.acquisition.tests.test_publisher_run import Cloud, environment, staged
from scripts.infrastructure import startup_check
from tests.startup_check_support import POLARIS_HOST, SERVICE_HOSTS, Network, host_environment, install_network
from tests.support import check

QUEUE = json.dumps({"queue": [{"disposition": "api", "mode": "mmd_api", "flags": ["browser_export_moved_to_api"]}]}).encode()
PLAN_FOLDER = "data/datasets/historical_acquisition/mmd_api_history"


class Reached(Exception):
    """The harness started, so the startup check passed."""


def mmd_tree(monkeypatch: pytest.MonkeyPatch, root: Path, leave_out: str = "") -> dict[str, Path]:
    """A synthetic MMD plan tree at absolute paths, patched into the contract the way the code root holds the real one."""
    registry = canonical_hash(load_registry())
    paths = {
        "plan": root / PLAN_FOLDER / "20260925/collection_plan.json",
        "additions_plan": root / PLAN_FOLDER / "20260926/collection_plan_additions.json",
        "report": root / "data/mmd/comparison_report.json",
        "reference": root / "data/mmd/reference.json",
        "original": root / "data/mmd/original_2019.csv",
        "lock": root / "data/mmd/registry_additions_lock.json",
        "template": root / "data/mmd/template_plan.json",
    }
    body = {
        "comparison_report": str(paths["report"]),
        "references": {"one": {"path": str(paths["reference"]), "sha256": "0" * 64}},
        "registry_sha256": registry,
    }
    contents: dict[str, str | dict[str, Any]] = {
        "plan": body,
        "additions_plan": body,
        "report": {"years": [{"original_path": str(paths["original"])}]},
        "reference": {},
        "original": "year\n",
        "lock": {},
        "template": {},
    }
    for name, path in paths.items():
        if name != leave_out:
            path.parent.mkdir(parents=True, exist_ok=True)
            content = contents[name]
            path.write_text(content if isinstance(content, str) else json.dumps(content))
    monkeypatch.setattr(
        mmd_api_contract, "PLANS", {mmd_api_contract.PLAN_SHA256: paths["plan"], mmd_api_contract.ADDITIONS_PLAN_SHA256: paths["additions_plan"]}
    )
    monkeypatch.setattr(mmd_api_contract, "ADDITIONS_LOCK_PATH", paths["lock"])
    monkeypatch.setattr(collect_mmd_api, "TEMPLATE", paths["template"])
    return paths


def run_job(monkeypatch: pytest.MonkeyPatch, workspace: Path) -> Cloud:
    stage, digest = staged(
        {
            "mirror/data/q/queue.json": QUEUE,
            "mirror/plan_inputs.json": json.dumps({"data/x": hashlib.sha256(b"x").hexdigest()}).encode(),
            "mirror/data/x": b"x",
            "publisher_hosts.json": b"{}",
            "publisher_manifest.json": b'{"queue": "mirror/data/q/queue.json"}',
        }
    )
    cloud = Cloud(stage)

    def started(*_arguments: Any) -> None:
        raise Reached

    monkeypatch.setattr(harness, "run_from", started)
    monkeypatch.setattr(harness, "REPO_ROOT", workspace / "code_root")
    environ = environment(workspace, digest) | {"PUBLISHER_QUEUE_SHA256": hashlib.sha256(QUEUE).hexdigest()}
    publisher_run.main(environ, cloud.factory)
    return cloud


def test_a_plan_file_that_is_not_in_the_task_is_named_before_anything_runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Live failure 3: the publisher read the MMD collection plan, which was not in the task."""
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())
    mmd_tree(monkeypatch, tmp_path / "code", leave_out="plan")
    with pytest.raises(startup_check.StartupCheckError, match="input_file_missing") as caught:
        run_job(monkeypatch, tmp_path / "workspace")
    check("mmd_api code root: plan" in str(caught.value), f"the missing file's collector, root and kind are named, and no digest of it: {caught.value}")


@pytest.mark.parametrize("leave_out", ["report", "reference", "original", "lock", "template"])
def test_every_file_the_mmd_plan_load_reads_is_checked(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, leave_out: str) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())
    mmd_tree(monkeypatch, tmp_path / "code", leave_out=leave_out)
    with pytest.raises(startup_check.StartupCheckError, match="input_file_missing") as caught:
        run_job(monkeypatch, tmp_path / "workspace")
    kind = {"lock": "additions_lock"}.get(leave_out, leave_out)
    check(f"mmd_api code root: {kind}" in str(caught.value), f"the kind of the missing file is named: {caught.value}")


def test_the_environment_is_checked_before_any_aws_call(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network(refuse={SERVICE_HOSTS[2]: TimeoutError("timed out")}))
    mmd_tree(monkeypatch, tmp_path / "code")
    stage, digest = staged()
    cloud = Cloud(stage)
    with pytest.raises(startup_check.StartupCheckError, match=rf"ecr_unreachable {SERVICE_HOSTS[2]}:443"):
        publisher_run.main(environment(tmp_path, digest), cloud.factory)
    check(cloud.puts == [] and not (tmp_path / "inputs").exists(), "no read and no write happened")


@pytest.mark.parametrize("execution", ["AWS_ECS_EC2", "AWS_ECS_FARGATE"])
def test_a_publisher_with_its_files_and_endpoints_starts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, execution: str, capsys: pytest.CaptureFixture[str]
) -> None:
    host_environment(monkeypatch, execution=execution)
    network = install_network(monkeypatch, Network())
    mmd_tree(monkeypatch, tmp_path / "code")
    with pytest.raises(Reached):
        run_job(monkeypatch, tmp_path / "workspace")
    check({host for host, _ in network.seen} == set(SERVICE_HOSTS), f"the publisher needs no Polaris: {network.seen}")
    check(POLARIS_HOST not in capsys.readouterr().err, "Polaris is not probed")


def test_a_queue_without_mmd_units_names_no_mmd_file() -> None:
    other = {"queue": [{"disposition": "api", "mode": "bls_api_v2", "flags": []}]}
    check(startup_check.queue_files(other) == [], "only the modes the queue runs name files")
    check(startup_check.queue_files({}) == [], "an empty queue names none")


def test_outside_a_task_the_publisher_discovers_no_plan_and_checks_no_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The startup check does nothing outside an ECS task, so it must not read the queue, import a collector or read a collection plan either."""
    for name in ("AWS_EXECUTION_ENV", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "ECS_CONTAINER_METADATA_URI_V4", "ECS_CONTAINER_METADATA_URI"):
        monkeypatch.delenv(name, raising=False)
    network = install_network(monkeypatch, Network())
    mmd_tree(monkeypatch, tmp_path / "code", leave_out="plan")
    seen: list[str] = []
    real_read_json, real_queue_files = startup_check.read_json, startup_check.queue_files

    def recorded_read_json(path: str | Path) -> Any:
        seen.append(f"read_json {path}")
        return real_read_json(path)

    def recorded_queue_files(*arguments: Any, **keywords: Any) -> Any:
        seen.append("queue_files")
        return real_queue_files(*arguments, **keywords)

    monkeypatch.setattr(startup_check, "read_json", recorded_read_json)
    monkeypatch.setattr(startup_check, "queue_files", recorded_queue_files)
    with pytest.raises(Reached):
        run_job(monkeypatch, tmp_path / "workspace")
    check(seen == [] and network.seen == [], f"the startup check discovered plans or connected outside a task: {seen} {network.seen}")
