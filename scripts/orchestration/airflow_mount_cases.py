"""Check that every real DAG imports in the Airflow container as the services stack deploys it.

The scheduler and the DAG processor see only what ``infra/services/airflow.tf`` mounts: the dags folder and a list of single files of the ``orchestration``
package under ``/opt/hai/orchestration``. A module that a DAG needs and the list lacks makes the DAG fail to import, and no broker call is ever made. This
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

ROOT = Path(__file__).resolve().parents[2]
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


CASES: dict[str, Callable[[], None]] = {
    "every_dag_imports_against_the_mounted_modules": every_dag_imports_against_the_mounted_modules,
    "a_missing_module_is_found": a_missing_module_is_found,
}
