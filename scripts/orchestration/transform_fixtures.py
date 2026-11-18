"""The checkout copy that the local Airflow stack mounts, for the training checks: Compose files, the DAG-side modules, the DAG folder and ``xcom_dump``."""

from __future__ import annotations

import importlib
import json
import shutil
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
PLATFORM_FILE = ROOT / "config" / "orchestration" / "platform.json"


def prepare_checkout(folder: Path) -> None:
    """A copy of what the Airflow stack mounts, so no test file enters ``dags/``: Compose files, the DAG-side modules, the DAG folder and ``xcom_dump``."""
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
    shutil.copy2(ROOT / "scripts" / "orchestration" / "xcom_dump.py", folder / "scripts" / "orchestration" / "xcom_dump.py")
    for name in mounted_modules(platform):
        shutil.copy2(ROOT / "orchestration" / name, folder / "orchestration" / name)


def mounted_modules(platform: dict[str, Any]) -> list[str]:
    """The ``orchestration/`` modules that the Airflow services of the platform file mount, read from their Compose file so the copy follows the stack."""
    document = importlib.import_module("yaml").safe_load((ROOT / platform["compose"]["file"]).read_bytes())
    prefix = "${HAI_ORCH_REPO}/orchestration/"
    names = {
        source.removeprefix(prefix)
        for service in platform["compose"]["services"]
        for source in (str(volume).split(":")[0] for volume in document["services"][service].get("volumes", []))
        if source.startswith(prefix) and source.endswith(".py")
    }
    return sorted(names)
