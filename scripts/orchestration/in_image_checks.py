"""Checks that run inside the pinned Airflow image with no network (Airflow plan section 11.1; failure modes 814, 823).

``scripts/orchestration/offline_checks.py`` mounts ``dags/``, ``orchestration/``, this file and the constraints file
read-only and runs ``python in_image_checks.py``. It prints one JSON object:

- ``dags``: every file in the DAG folder imported by Airflow's DagBag; import errors and cycles are listed.
- ``graph_cases``: synthetic DAGs built from real Airflow operators; the section 5 reference graph must pass the
  ``transform_dag`` validator and each mutation must fail with its rule.
- ``transform_graph``: the validator on the real ``transform_dag`` object that Airflow imported from the DAG folder.
- ``constraints``: every installed distribution is either in the Airflow 3.4.0 constraints file at the pinned version, or one of the pip and uv build
  tools at the version the image records, or in the exact release set of ``config/orchestration/platform.json`` at its own pinned version. Nothing is
  exempt by name prefix: any other distribution, an extra provider or the task SDK included, is reported as unpinned.

- ``real_dags``: every DAG that launches a host job, run with Airflow's own test runner and the real Amazon provider operator against the stand-in cloud
  of the offline rig (``scripts/orchestration/real_image_dags.py``), so a fault in how Airflow treats our classes shows here and not in the live run.

The lineage probe checks the provider and client already installed in the pinned image. It starts no service or network delivery.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import re
import sys
from collections.abc import Callable
from importlib import metadata
from pathlib import Path
from typing import Any

from airflow.dag_processing.dagbag import DagBag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator

from orchestration.graph import EXPECTED_RULES, EXPECTED_UPSTREAM, GRAPH_DAGS, graph_from_dag, validate_transform_graph

DAG_FOLDER = Path("/opt/hai/dags")
CONSTRAINTS = Path("/opt/hai/constraints.txt")
RELEASE_FILE = Path("/opt/hai/platform.json")
REPO = Path("/opt/hai/repo")  # the read-only checkout folders that the real-image DAG runs need (docker-compose.orchestration.yaml, service airflow-check)
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
    for name in GRAPH_DAGS:
        dag = bag.dags.get(name)
        violations = validate_transform_graph(graph_from_dag(dag), name) if dag is not None else []
        results[name] = {"present": dag is not None, "violations": [{"rule": item.rule, "task": item.task, "detail": item.detail} for item in violations]}
    return results


def build(
    dag_id: str, change: Callable[[dict[str, BaseOperator]], None] | None = None, rules: dict[str, str] | None = None, drop: tuple[str, str] | None = None
) -> DAG:
    """The section 5 graph with real operators; ``rules`` overrides trigger rules, ``drop`` removes one edge, ``change``
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
    return re.sub(r"[-_.]+", "-", name).lower()


def release_packages() -> dict[str, str]:
    """The exact set of Airflow release distributions that the constraints file does not list, each with its own pinned version."""
    packages: dict[str, str] = json.loads(RELEASE_FILE.read_text())["airflow"]["release_packages"]
    return {normalized(name): version for name, version in packages.items()}


def constraints(tools: dict[str, str]) -> dict[str, Any]:
    """Installed versions against the constraints file and the release pins. ``tools`` are the official image's build tools (pip and uv) at the versions
    its build records, read from the image's own environment. Every installed distribution must be in one of the three sets; nothing is exempt by prefix."""
    pins = {normalized(match.group(1)): match.group(2) for line in CONSTRAINTS.read_text().splitlines() if (match := PIN.match(line))}
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


def lineage_provider_off() -> dict[str, Any]:
    """Check that the OpenLineage provider sends nothing unless it is enabled with a transport, and that the image ships no lineage spool code."""
    from unittest.mock import patch

    from airflow.providers.openlineage import conf

    shipped = [name for name in ("provider_events", "provider_transport") if importlib.util.find_spec(f"hai_lineage.{name}") is not None]
    if shipped:
        raise ValueError(f"the lineage package still ships spool code: {shipped}")
    http = {"OPENLINEAGE__TRANSPORT__TYPE": "http", "OPENLINEAGE__TRANSPORT__URL": "http://127.0.0.1:1"}
    cases = {
        "nothing_configured": ({}, True),
        "disabled_overrides_a_transport": ({"AIRFLOW__OPENLINEAGE__DISABLED": "true", **http}, True),
        "enabled_with_a_transport": ({"AIRFLOW__OPENLINEAGE__DISABLED": "false", **http}, False),
    }
    caches = (conf.is_disabled, conf.transport, conf.config_path, conf.config_conn_id)
    results = {}
    for name, (environment, expected) in cases.items():
        with patch.dict(os.environ):
            for key in [key for key in os.environ if key.startswith(("AIRFLOW__OPENLINEAGE__", "OPENLINEAGE"))]:
                del os.environ[key]
            os.environ.update(environment)
            for cached in caches:
                cached.cache_clear()  # the provider reads its settings once per process, so each case starts clean
            results[name] = {"disabled": conf.is_disabled(), "expected": expected}
    for cached in caches:
        cached.cache_clear()  # leave the process as it was found for the checks that follow
    if any(row["disabled"] is not row["expected"] for row in results.values()):
        raise ValueError(f"the provider switch does not follow its configuration: {results}")
    return {"ok": True, "cases": results, "spool_code_shipped": shipped, "network": "none"}


def real_dags() -> dict[str, Any]:
    """Run each host-launching DAG in this image with Airflow's own test runner (``scripts/orchestration/real_image_dags.py``). A crash of the run is the
    section's error, so the other checks still report."""
    try:
        path = REPO / "scripts/orchestration/real_image_dags.py"
        spec = importlib.util.spec_from_file_location(
            "real_image_dags", path
        )  # loaded by path: its folder is the checkout copy, not a package root of this image
        if spec is None or spec.loader is None:
            raise ImportError(f"{path} cannot be loaded")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        report: dict[str, Any] = module.run_all(None)
    except Exception as error:
        return {"ok": False, "error": f"{type(error).__name__}: {error}"[:1000]}
    return report


def lineage_http_transport() -> dict[str, Any]:
    """Run the scheduler's HTTP lineage transport against a loopback stand-in (``lineage_http_cases.py``). A crash is the section's error."""
    try:
        path = REPO / "scripts/orchestration/lineage_http_cases.py"
        spec = importlib.util.spec_from_file_location("lineage_http_cases", path)  # loaded by path, as real_dags is
        if spec is None or spec.loader is None:
            raise ImportError(f"{path} cannot be loaded")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        report: dict[str, Any] = module.run()
    except Exception as error:
        return {"ok": False, "error": f"{type(error).__name__}: {error}"[:1000]}
    return report


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
        report["lineage_provider_off"] = lineage_provider_off()
    except (OSError, ValueError, TypeError, AttributeError, ImportError) as error:
        report["lineage_provider_off"] = {"ok": False, "error": f"{type(error).__name__}: {error}"[:1000]}
    report["lineage_http_transport"] = lineage_http_transport()
    report["real_dags"] = real_dags()
    sys.stdout.write("IN_IMAGE " + json.dumps(report, default=str) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
