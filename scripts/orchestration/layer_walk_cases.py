"""Walk a real request from dispatch to publish with nothing but the real variable lists.

The layer suite runs each phase with the arguments it chooses, so a variable that a container list drops can stay hidden. These cases start a silver task
through the real broker, hand each container of the task exactly the variables the broker sent it, run the fetch command that ``infra/jobs/layers.tf`` gives
the container and then each driver phase as that file says. A direct run and a chain child are walked. Only boto3 and the work of the image modules are
stood in for. Before a module that opens a Spark session does its stand-in work, the real task settings check its environment, so a container without the
catalog endpoint fails.
"""

from __future__ import annotations

import contextlib
import copy
import io
import json
import os
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.lakehouse import layer_run, session
from scripts.lakehouse import run_layer_run_e2e as layer_e2e
from scripts.lakehouse.run_layer_run_e2e import require
from scripts.orchestration import ecs_broker_cases as brokers
from scripts.process import run_command

FETCH_STUB = """
import json, pathlib, shutil
CONFIG = json.loads((pathlib.Path(__file__).parent / "stub.json").read_text())


class Client:
    def download_file(self, bucket, key, path):
        with open(CONFIG["log"], "a") as handle:
            handle.write(key + "\\n")
        if key != CONFIG["expected"]:
            raise OSError("no such object " + key)
        shutil.copyfile(CONFIG["archive"], path)


def client(name):
    return Client()
"""


LAYERS_TF = Path(__file__).resolve().parents[2] / "infra/jobs/layers.tf"
FARGATE = {"AWS_EXECUTION_ENV": "AWS_ECS_FARGATE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/v2/credentials/SyntheticWalkProvider"}


def task_definition_environment() -> dict[str, str]:
    """The literal variables that ``layers.tf`` sets on every container of a layer task, such as the Spark scratch folder. Values built from Terraform
    expressions are not literal and are left out."""
    found = re.search(r"environment = concat\(\n\s*\[(.*?)\],\n\s*\[for name in lookup", LAYERS_TF.read_text(), re.DOTALL)
    require(found is not None, "layers.tf has no container environment list")
    pairs = re.findall(r'\{ name = "(\w+)", value = "([^"$]*)" \}', found.group(1) if found else "")
    require(bool(pairs), "layers.tf sets no literal container variable")
    return dict(pairs)


def opens_spark_session(module: str) -> bool:
    """True when the module's source opens the project's Spark session, which is where the real task checks its credentials and catalog endpoint."""
    source = Path(__file__).resolve().parents[2] / (module.replace(".", "/") + ".py")
    return source.is_file() and "spark_session(" in source.read_text()


def real_environment_standin(job: Any, module: str, arguments: Any, environment: dict[str, str] | None = None) -> None:
    """The stand-in for an image module, behind the checks that the real module makes of its environment before it does any work. The module gets the
    container's variables and what Fargate adds. A module that opens a Spark session runs the real task settings: without the catalog endpoint they refuse."""
    seen = {**os.environ, **FARGATE, **(environment or {})}
    if opens_spark_session(module) and "--local-warehouse" not in [str(item) for item in arguments]:
        with mock.patch.dict(os.environ, seen, clear=True):
            try:
                session.task_settings()
            except RuntimeError as error:
                raise layer_run.StepError(f"{module} cannot open its Spark session in this container: {error}") from None
    layer_e2e.standin(job, module, arguments, environment)


def run_fetch_container(case: layer_e2e.Case, work: Path, environment: dict[str, str], code_run: str) -> list[str]:
    """Run the fetch command that ``infra/jobs/layers.tf`` gives the container, with only that container's variables. Only boto3 is replaced: the stand-in
    serves the archive from the key under the code run's prefix and records every key asked for."""
    text = (Path(__file__).resolve().parents[2] / "infra/jobs/layers.tf").read_text()
    found = re.search(r"layer_fetch_python = trimspace\(<<-EOT\n(.*?)\n\s*EOT", text, re.DOTALL)
    require(found is not None, "layers.tf has no fetch command")
    scratch = work.parent / (work.name + "_fetch")
    scratch.mkdir()
    (scratch / "boto3.py").write_text(FETCH_STUB)
    log = scratch / "keys.log"
    (scratch / "stub.json").write_text(
        json.dumps({"log": str(log), "archive": str(case.archive), "expected": f"lakehouse/run_stage/{code_run}/inputs/code.tar.gz"})
    )
    command = scratch / "fetch.py"
    command.write_text(found.group(1).strip().replace("/workspace", str(work)) if found else "")
    done = run_command(sys.executable, [str(command)], cwd=scratch, env={**environment, "PATH": os.environ.get("PATH", "")}, timeout=120)
    keys = log.read_text().split() if log.exists() else []
    require(done.returncode == 0, f"fetch failed with the variables {sorted(environment)}: {done.stderr.strip()[-300:]} (asked for {keys})")
    return keys


def walk_task(case: layer_e2e.Case, environments: dict[str, dict[str, str]], layer: str, code_run: str, label: str) -> None:
    """Run the containers of one broker-started task in order. Each container gets exactly the variables the broker sent it, plus the bucket that the task
    definition sets for all of them. The driver containers run ``layer_run <phase> --layer <layer> --run $RUN_ID`` as ``layers.tf`` says."""
    work = case.root / f"walk_{label}"
    work.mkdir()
    layer_e2e.CODE_RUN_EXPECTED[0] = code_run
    for name, variables in environments.items():
        environment = {**task_definition_environment(), **variables, "DATA_BUCKET_NAME": layer_e2e.BUCKET}
        if name == "fetch":
            keys = run_fetch_container(case, work, environment, code_run)
            require(keys == [f"lakehouse/run_stage/{code_run}/inputs/code.tar.gz"], f"fetch asked for {keys}")
            continue
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(layer_run, "run_module", real_environment_standin),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = layer_run.main([name, "--layer", layer, "--run", variables["RUN_ID"], "--work", str(work)])
        require(code == 0, f"container {name} of the {label} task failed with the variables {sorted(variables)}: {err.getvalue().strip()[-400:]}")


def task_environments(current: Any, family: str) -> dict[str, dict[str, str]]:
    """The environment per container of the last task the real broker started for ``family``, in container order."""
    sent = [arguments for name, arguments in current.ecs.calls if name == "run_task" and arguments["taskDefinition"].endswith(f"_job_{family}")][-1]
    return {item["name"]: {pair["name"]: pair["value"] for pair in item["environment"]} for item in sent["overrides"]["containerOverrides"]}


def real_variables_reach_every_phase_of_a_direct_and_a_chain_run() -> None:
    """Dispatch to publish with nothing but the real variable lists: a request goes through the real broker, the variables it sends reach the fetch command
    of the task definition and each driver phase, and a variable that one list drops fails the run. A direct silver run and a silver chain child."""
    with tempfile.TemporaryDirectory(prefix="layer_walk_") as folder, layer_e2e.patched(Path(folder)) as case:
        walk(case)


def walk(case: layer_e2e.Case) -> None:
    with brokers.rig() as current:
        direct = brokers.SILVER_RUN
        current.s3.objects[f"lakehouse/run_stage/{direct}/inputs/code.sha256"] = case.digest.encode() + b"\n"
        brokers.wait_for(current, current.client.submit(brokers.silver_request(direct)))
        walk_task(case, task_environments(current, "silver"), "silver", direct, "direct")
    require(
        len(layer_e2e.STATE) == 1 and any(key.endswith(f"layer_runs/silver/{direct}/state/pinned.json") for key in layer_e2e.STATE),
        "the direct run did not claim its run ID",
    )
    with brokers.rig() as current:
        bronze, child = brokers.BRONZE_RUN, brokers.derived_run_id(brokers.BRONZE_RUN, "silver")
        current.s3.objects[f"lakehouse/run_stage/{bronze}/inputs/code.sha256"] = case.digest.encode() + b"\n"
        brokers.wait_for(current, current.client.submit(brokers.request("chain_gate", run=bronze, dag="bronze_chain_dag")))
        brokers.wait_for(current, current.client.submit(brokers.request("pin_bronze", run=child, dag="silver_transform_dag")))
        walk_task(case, task_environments(current, "silver"), "silver", bronze, "chain")
    require(any(key.endswith(f"layer_runs/silver/{child}/state/pinned.json") for key in layer_e2e.STATE), "the chain child did not claim its run ID")


def walk_without_publish_variable(variable: str) -> str:
    """Walk with ``variable`` removed from the silver publish container of the job table; the error of the walk, or an empty string when it stayed green."""
    original = brokers.load_jobs

    def changed(root: Path) -> Any:
        document = copy.deepcopy(original(root))
        document["jobs"]["silver"]["containers"]["publish"].remove(variable)
        return document

    with mock.patch.object(brokers, "load_jobs", changed):
        try:
            real_variables_reach_every_phase_of_a_direct_and_a_chain_run()
        except AssertionError as error:
            return str(error)
    return ""


def a_publish_container_without_the_catalog_endpoint_fails_the_walk() -> None:
    """The planted defect: the job table drops ``POLARIS_URI`` from publish. The real Spark setup refuses the task without it, so the walk must too."""
    detail = walk_without_publish_variable("POLARIS_URI")
    require("task_catalog_endpoint_refused" in detail and "publish" in detail, f"the walk did not refuse a publish without the endpoint: {detail!r}")


def a_publish_container_without_the_code_run_fails_the_walk() -> None:
    detail = walk_without_publish_variable("CODE_RUN_ID")
    require("CODE_RUN_ID" in detail and "publish" in detail, f"the walk did not refuse a publish without the code run: {detail!r}")


CASES: dict[str, Callable[[], None]] = {
    "real_variables_reach_every_phase_of_a_direct_and_a_chain_run": real_variables_reach_every_phase_of_a_direct_and_a_chain_run,
    "a_publish_container_without_the_catalog_endpoint_fails_the_walk": a_publish_container_without_the_catalog_endpoint_fails_the_walk,
    "a_publish_container_without_the_code_run_fails_the_walk": a_publish_container_without_the_code_run_fails_the_walk,
}
