"""Checks that run inside the pinned Airflow image with no network.

``scripts/orchestration/offline_checks.py`` mounts ``dags/``, ``orchestration/``, this file and the constraints file
read-only and runs ``python in_image_checks.py``. It prints one JSON object:

- ``dags``: every file in the DAG folder imported by Airflow's DagBag; import errors and cycles are listed.
- ``graph_cases``: synthetic DAGs built from real Airflow operators; the reference graph must pass the
  ``transform_dag`` validator and each mutation must fail with its rule.
- ``transform_graph``: the validator on the real ``transform_dag`` object that Airflow imported from the DAG folder.
- ``constraints``: every installed distribution is either in the Airflow 3.3.2 constraints file at the pinned version, or one of the pip and uv build
  tools at the version the image records, or in the exact release set of ``config/orchestration/platform.json`` at its own pinned version. Nothing is
  exempt by name prefix: any other distribution, an extra provider or the task SDK included, is reported as unpinned.

The lineage probe checks the provider and client already installed in the pinned image. It starts no service or network delivery.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

from airflow.dag_processing.dagbag import DagBag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator

from orchestration.graph import EXPECTED_RULES, EXPECTED_UPSTREAM, PIPELINE_CHAINS, graph_from_dag, validate_transform_graph

DAG_FOLDER = Path("/opt/hai/dags")
CONSTRAINTS = Path("/opt/hai/constraints.txt")
RELEASE_FILE = Path("/opt/hai/platform.json")
PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;#]+)")


def dags() -> dict[str, Any]:
    """Import every DAG file; any import error or cycle fails."""
    bag = DagBag(dag_folder=str(DAG_FOLDER))
    files = sorted(path.name for path in DAG_FOLDER.glob("*.py"))
    return {"files": files, "dag_ids": sorted(bag.dag_ids), "import_errors": {Path(path).name: text[-400:] for path, text in bag.import_errors.items()}}


def transform_graph() -> dict[str, Any]:
    """The graph validator on the real ``transform_dag``; every rule it breaks is listed."""
    dag = DagBag(dag_folder=str(DAG_FOLDER)).dags.get("transform_dag")
    if dag is None:
        return {"present": False, "violations": []}
    violations = validate_transform_graph(graph_from_dag(dag))
    return {
        "present": True,
        "tasks": sorted(dag.task_dict),
        "violations": [{"rule": item.rule, "task": item.task, "detail": item.detail} for item in violations],
    }


def pipeline_graphs() -> dict[str, Any]:
    """Validate each shipped split DAG's actual task objects, including failed-work cleanup edges."""
    bag = DagBag(dag_folder=str(DAG_FOLDER))
    results = {}
    for name in PIPELINE_CHAINS:
        dag = bag.dags.get(name)
        violations = validate_transform_graph(graph_from_dag(dag), name) if dag is not None else []
        results[name] = {"present": dag is not None, "violations": [{"rule": item.rule, "task": item.task, "detail": item.detail} for item in violations]}
    return results


def build(
    dag_id: str, change: Callable[[dict[str, BaseOperator]], None] | None = None, rules: dict[str, str] | None = None, drop: tuple[str, str] | None = None
) -> DAG:
    """The reference graph with real operators; ``rules`` overrides trigger rules, ``drop`` removes one edge, ``change``
    edits the built tasks."""
    wanted = EXPECTED_RULES | (rules or {})
    with DAG(dag_id=dag_id, schedule=None) as dag:
        tasks: dict[str, BaseOperator] = {
            task_id: EmptyOperator(task_id=task_id, trigger_rule=wanted.get(task_id, "all_success")) for task_id in EXPECTED_UPSTREAM
        }
        for task_id, upstream in EXPECTED_UPSTREAM.items():
            for name in sorted(upstream):
                if (name, task_id) != drop:
                    tasks[name] >> tasks[task_id]
        if change is not None:
            change(tasks)
    return dag


def extra_leaf(tasks: dict[str, BaseOperator]) -> None:
    """A task after final_gate besides release_run."""
    tasks["final_gate"] >> EmptyOperator(task_id="notify", trigger_rule="all_done")


def setup_task(tasks: dict[str, BaseOperator]) -> None:
    """acquire_run marked as a setup task."""
    tasks["acquire_run"].as_setup()


def teardown_task(tasks: dict[str, BaseOperator]) -> None:
    """release_run marked as a teardown task of acquire_run."""
    tasks["release_run"].as_teardown(setups=tasks["acquire_run"])


def cycle(tasks: dict[str, BaseOperator]) -> None:
    """gold_build feeds back into silver_build."""
    tasks["gold_build"] >> tasks["silver_build"]


def graph_cases() -> dict[str, Any]:
    """The reference graph passes; each mutation fails with its rule."""
    cases: dict[str, tuple[dict[str, Any], str | None]] = {
        "reference": ({}, None),
        "extra leaf": ({"change": extra_leaf}, "leaf"),
        "setup task": ({"change": setup_task}, "setup_or_teardown"),
        "teardown task": ({"change": teardown_task}, "setup_or_teardown"),
        "final_gate all_success": ({"rules": {"final_gate": "all_success"}}, "trigger_rule"),
        "release_run all_success": ({"rules": {"release_run": "all_success"}}, "trigger_rule"),
        "work task all_done": ({"rules": {"silver_build": "all_done"}}, "trigger_rule"),
        "missing edge into final_gate": ({"drop": ("silver_build", "final_gate")}, "upstream"),
        "missing edge into catalog_release": ({"drop": ("run_record", "catalog_release")}, "upstream"),
        "cycle": ({"change": cycle}, "cycle"),
    }
    results = {}
    for number, (name, (options, rule)) in enumerate(cases.items()):
        rules = sorted({violation.rule for violation in validate_transform_graph(graph_from_dag(build(f"case_{number}", **options)))})
        results[name] = {"expected": rule, "rules": rules, "ok": rules == [] if rule is None else rule in rules}
    return results


def normalized(name: str) -> str:
    """A distribution name in the form the constraints file and the metadata share."""
    return name.lower().replace("_", "-")


def release_packages() -> dict[str, str]:
    """The exact set of Airflow release distributions that the constraints file does not list, each with its own pinned version."""
    packages: dict[str, str] = json.loads(RELEASE_FILE.read_text())["airflow"]["release_packages"]
    return {normalized(name): version for name, version in packages.items()}


def constraints(tools: dict[str, str]) -> dict[str, Any]:
    """Installed versions against the constraints file and the release pins. ``tools`` are the official image's build tools (pip and uv) at the versions
    its build records, read from the image's own environment. Every installed distribution must be in one of the three sets; nothing is exempt by prefix."""
    pins = {match.group(1).lower().replace("_", "-"): match.group(2) for line in CONSTRAINTS.read_text().splitlines() if (match := PIN.match(line))}
    release = release_packages()
    installed = {normalized(str(dist.metadata["Name"])): dist.version for dist in metadata.distributions()}
    differing = {
        name: {"pinned": pins[name], "installed": version}
        for name, version in sorted(installed.items())
        if name in pins and name not in tools and pins[name] != version
    }
    build_tools = {name: {"installed": installed.get(name), "image_build": version, "ok": installed.get(name) == version} for name, version in tools.items()}
    unpinned = sorted(name for name in installed if name not in pins and name not in tools and name not in release)
    release_problems = []
    for name, version in sorted(release.items()):
        if name in pins:
            release_problems.append(f"{name} is in the constraints file and must not also be a release pin")
        elif name not in installed:
            release_problems.append(f"{name} is pinned at {version} but not installed")
        elif installed[name] != version:
            release_problems.append(f"{name} is {installed[name]}, not its pinned {version}")
    return {
        "pinned": len(pins),
        "installed": len(installed),
        "differing": differing,
        "unpinned": unpinned,
        "build_tools": build_tools,
        "release": {name: installed.get(name) for name in sorted(release)},
        "release_problems": release_problems,
        "lineage_packages": {
            name: installed.get(name) for name in ("apache-airflow-providers-openlineage", "openlineage-python", "openlineage-integration-common")
        },
    }


def lineage_transport() -> dict[str, Any]:
    """Check native pinned event classes, transport factory and sanitized local persistence without network delivery."""
    from airflow.providers.openlineage.plugins.facets import AirflowRunFacet
    from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
    from openlineage.client.facet_v2 import parent_run
    from openlineage.client.transport.factory import DefaultTransportFactory

    provider = importlib.import_module("hai_lineage.provider_events")
    transport_module = importlib.import_module("hai_lineage.provider_transport")
    sentinel = "synthetic-provider-metadata-must-not-leave-memory"
    run_id, parent_id = "13d9e9ef-93b2-4efc-90d5-066b1becf67d", "3589fe84-f7d0-49fe-b456-5fa256a72938"
    facet = AirflowRunFacet(
        dag={"dag_id": "transform_dag", "owner": sentinel},
        dagRun={"run_id": "manual__provider_probe", "conf": {"secret": sentinel}},
        task={"task_id": "silver_build", "source_code": sentinel},
        taskInstance={"try_number": 1, "map_index": None},
        taskUuid=run_id,
    )
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(UTC).isoformat(),
        run=Run(
            runId=run_id,
            facets={
                "airflow": facet,
                "parent": parent_run.ParentRunFacet(
                    run=parent_run.Run(runId=parent_id), job=parent_run.Job(namespace=provider.NAMESPACE, name="transform_dag")
                ),
            },
        ),
        job=Job(namespace=provider.NAMESPACE, name="transform_dag.silver_build"),
        producer=provider.PRODUCER,
    )
    transport = DefaultTransportFactory().create({"type": "hai_lineage.provider_transport.SafeTransport", "path": "/opt/hai/lineage_provider"})
    try:
        transport_module.SpoolConfig.from_dict({"type": "hai_lineage.provider_transport.SafeTransport", "path": "/opt/hai/unapproved"})
    except ValueError:
        pass
    else:
        raise ValueError("native transport accepted an unapproved spool path")
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        if not provider.emit(root, event):
            raise ValueError("native provider event did not reach the sanitized spool")
        files = list(root.glob("*.json"))
        if len(files) != 1:
            raise ValueError("native provider event did not persist once")
        envelope = provider.validate(json.loads(files[0].read_text()))
        if envelope["event"]["run"]["runId"] != run_id or sentinel in files[0].read_text():
            raise ValueError("native provider projection lost correlation or retained metadata")
    if (
        not isinstance(transport, transport_module.SafeTransport)
        or transport.config_class is not transport_module.SpoolConfig
        or transport.kind != "hai-lineage-spool"
        or not transport.close(timeout=0)
    ):
        raise ValueError("native provider factory did not create the local transport")
    return {"ok": True, "factory": transport.kind, "native_run_uuid_preserved": True, "sentinel_absent": True, "network": "none"}


def main() -> int:
    """Print every result as JSON."""
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    tools = {name: os.environ.get(variable, "") for name, variable in (("pip", "AIRFLOW_PIP_VERSION"), ("uv", "AIRFLOW_UV_VERSION"))}
    report = {
        "dags": dags(),
        "graph_cases": graph_cases(),
        "transform_graph": transform_graph(),
        "pipeline_graphs": pipeline_graphs(),
        "constraints": constraints(tools),
    }
    try:
        report["lineage_transport"] = lineage_transport()
    except (OSError, ValueError, TypeError, AttributeError, ImportError) as error:
        report["lineage_transport"] = {"ok": False, "error": type(error).__name__}
    sys.stdout.write("IN_IMAGE " + json.dumps(report) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
