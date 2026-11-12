"""The checkout copy that the local Airflow stack mounts, for the training checks: Compose files, the DAG-side modules, the DAG folder and ``db_dump``."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PLATFORM_FILE = ROOT / "config" / "orchestration" / "platform.json"


def prepare_checkout(folder: Path) -> None:
    """A copy of what the Airflow stack mounts, so no test file enters ``dags/``: Compose files, the DAG-side modules, the DAG folder and ``db_dump``."""
    (folder / "services" / "orchestration").mkdir(parents=True)
    (folder / "services" / "analytics").mkdir(parents=True)
    shutil.copytree(ROOT / "services" / "lineage", folder / "services" / "lineage", ignore=shutil.ignore_patterns("__pycache__"))
    (folder / "scripts" / "orchestration").mkdir(parents=True)
    (folder / "orchestration").mkdir()
    (folder / "dags").mkdir()
    (folder / "data").mkdir()
    shutil.copy2(ROOT / "docker-compose.yaml", folder / "docker-compose.yaml")
    shutil.copy2(ROOT / "services" / "analytics" / "resources.sql", folder / "services" / "analytics" / "resources.sql")
    platform = json.loads(PLATFORM_FILE.read_text())
    shutil.copy2(ROOT / platform["compose"]["file"], folder / platform["compose"]["file"])
    shutil.copy2(ROOT / "scripts" / "orchestration" / "db_dump.py", folder / "scripts" / "orchestration" / "db_dump.py")
    for name in ("__init__.py", "interface.py", "client.py", "dag_runtime.py", "training_contract.py"):
        shutil.copy2(ROOT / "orchestration" / name, folder / "orchestration" / name)
    shutil.copy2(ROOT / "tests/orchestration/fixtures/combined_dag.py", folder / "dags" / "transform_dag.py")
