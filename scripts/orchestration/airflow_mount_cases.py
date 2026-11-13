"""Check that every real DAG imports in the Airflow container as the services stack deploys it.

The scheduler and the DAG processor see only what ``infra/services/airflow.tf`` mounts: the dags folder and a list of single files of the ``orchestration``
package under ``/opt/hai/orchestration``. A module that a DAG needs and the list lacks makes the DAG fail to import, and no launch is ever made. This
suite copies exactly the mounted files into a scratch package, replaces only the Airflow SDK by a permissive stand-in and imports every file of ``dags/``
in a fresh interpreter (``airflow_import_probe``).
"""

from __future__ import annotations

import json
import re
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

from scripts.orchestration.ecs_adapter_cases import require
from scripts.process import run_command
from scripts.quality.container_imports import Container, Entry, ImportGraph, locations, modules_at
from scripts.quality.container_mounts import Declarations, inventory

ROOT = Path(__file__).resolve().parents[2]
CHECK_COMPOSE = "services/orchestration/docker-compose.orchestration.yaml"
CHECK_SERVICE = "airflow-check"
CHECK_CHECKOUT = "/opt/hai/repo"  # where the check container's checkout folders sit; real_image_dags puts it first on the module search path
# The graph follows every definition of a module that the run imports. This one is an offline case of the rig that the in-image run never calls.
NOT_RUN = frozenset({"scripts/orchestration/airflow_mode_cases.py scripts.infrastructure.services_apply"})
MOUNT = re.compile(r'containerPath\s*=\s*"/opt/hai/orchestration/([A-Za-z0-9_]+\.py)"')


def mounted_modules() -> list[str]:
    """The ``orchestration`` files that the Airflow containers receive, as the services stack lists them."""
    names = MOUNT.findall((ROOT / "infra/services/airflow.tf").read_text())
    require(bool(names) and "__init__.py" in names, f"the Airflow mount lists no orchestration package: {names}")
    return names


def import_dags(modules: list[str]) -> dict[str, str]:
    """Import every DAG file against a package holding only ``modules``; return the errors by file name."""
    with tempfile.TemporaryDirectory(prefix="airflow_mount_") as scratch:
        package = Path(scratch) / "orchestration"
        package.mkdir()
        for name in modules:
            shutil.copyfile(ROOT / "orchestration" / name, package / name)
        dags = sorted(str(path) for path in (ROOT / "dags").glob("*.py"))
        require(bool(dags), "no DAG file found")
        done = run_command(sys.executable, ["-m", "scripts.orchestration.airflow_import_probe", scratch, json.dumps(dags)], cwd=ROOT, timeout=120)
        require(done.returncode == 0, done.stderr[-600:])
        found: dict[str, str] = json.loads(done.stdout.strip().splitlines()[-1])
        return found


def every_dag_imports_against_the_mounted_modules() -> None:
    failed = import_dags(mounted_modules())
    require(not failed, f"DAGs that cannot import in the Airflow container: {failed}")


def a_missing_module_is_found() -> None:
    """The check fails for the planted defect it exists for: a mount list without the training contract."""
    modules = [name for name in mounted_modules() if name != "training_contract.py"]
    failed = import_dags(modules)
    require(bool(failed) and all("training_contract" in message for message in failed.values()), f"the planted omission was not named: {failed}")


def the_real_image_run_imports_only_mounted_files() -> None:
    """The in-image DAG run (``real_image_dags``, started from ``in_image_checks``) imports first-party modules only from files that the
    ``airflow-check`` service mounts, including the ones that its production modules import (``layer_report`` needs ``scripts.lakehouse``)."""
    files = inventory(ROOT)
    declarations = Declarations(ROOT, files)
    binds = declarations.compose_binds(CHECK_COMPOSE, CHECK_SERVICE)
    roots = ("/opt/hai", CHECK_CHECKOUT)
    first_party = {name.split(".")[0] for name in modules_at(locations(declarations.python, binds), roots)}
    container = Container(f"{CHECK_COMPOSE}:{CHECK_SERVICE}", binds, roots, Entry("scripts.orchestration.real_image_dags"), CHECK_COMPOSE)
    found = sorted({f"{item.path} {item.name}" for item in ImportGraph(ROOT, declarations.python, container, first_party, {}).check()} - NOT_RUN)
    require(not found, f"the in-image DAG run imports files that the airflow-check service does not mount: {found}")


CASES: dict[str, Callable[[], None]] = {
    "every_dag_imports_against_the_mounted_modules": every_dag_imports_against_the_mounted_modules,
    "a_missing_module_is_found": a_missing_module_is_found,
    "the_real_image_run_imports_only_mounted_files": the_real_image_run_imports_only_mounted_files,
}
