"""The host job broker's HTTP client and its broker-era DAG fixture are gone, and nothing mounts or names them.

Tasks launch through the launch library (``orchestration/airflow_steps.py``), so no Airflow container carries a broker client, its session folder or a
fixture DAG built on it. Names are spelled in pieces so that this file does not name what it guards against.
"""

from __future__ import annotations

from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
CLIENT_CLASS = "Broker" + "Client"
CLIENT_ERROR = "Broker" + "Unavailable"
SESSION_MOUNT = "/opt/hai/" + "broker"
CLIENT_STEM = "cli" + "ent"
SEARCHED = ("dags", "orchestration", "scripts", "services", "infra/services", "config", "tests")
SUFFIXES = (".py", ".tf", ".hcl", ".yaml", ".yml", ".json")
SKIPPED_PARTS = (".terraform", "__pycache__")


def tracked_text(folders: tuple[str, ...]) -> dict[str, str]:
    """Every source, configuration and Terraform file under the folders, without this guard."""
    found: dict[str, str] = {}
    for folder in folders:
        for path in sorted((ROOT / folder).rglob("*")):
            if path.suffix in SUFFIXES and path.is_file() and not any(part in SKIPPED_PARTS for part in path.parts) and path != Path(__file__).resolve():
                found[str(path.relative_to(ROOT))] = path.read_text(errors="ignore")
    return found


def test_the_broker_client_module_and_its_dag_fixture_are_gone() -> None:
    present = [str(path.relative_to(ROOT)) for path in (ROOT / "orchestration").glob(f"{CLIENT_STEM}*")]
    present += [str(path.relative_to(ROOT)) for path in (ROOT / "tests/orchestration/fixtures").glob("combined_d*")]
    check(not present, f"the broker client or its DAG fixture is back: {present}")


def test_no_file_names_the_broker_client() -> None:
    named = [name for name, text in tracked_text(SEARCHED).items() if CLIENT_CLASS in text or CLIENT_ERROR in text]
    check(not named, f"these files name the broker client: {named}")


def test_no_container_mounts_the_broker_client_or_its_session_folder() -> None:
    module_path = f"orchestration/{CLIENT_STEM}.py"
    users = [name for name, text in tracked_text(("services", "infra/services", "scripts", "config")).items() if module_path in text or SESSION_MOUNT in text]
    check(not users, f"these files still mount or list the broker client or its session folder: {users}")
