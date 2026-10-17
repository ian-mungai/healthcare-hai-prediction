"""Fixtures for the transform DAG's E2E runners: a tiny dbt project, the run folders it seeds, the test inventory and the checkout copy that the Airflow stack
mounts.

The tiny project (``tests/orchestration/fixtures/tiny``) has one model per step family, tagged ``catalog``, ``silver`` or ``gold``, and tests tagged
``silver_checks``, ``gold_checks`` and ``cross_checks``. A run's seed folder holds a copy of it with an overlay that breaks exactly one step, so a case
chooses its failure by the run it triggers. The test inventory maps the work steps to ``fixture_dbt`` jobs on that project, except ``silver_validate``, which
runs the shipped ``quality_validate`` row on the tiny suites (``tiny_ge``) so that the validator reports the build file's SHA-256. The control rows, the
intentional no-ops and every other row are the shipped ones. ``seed_shipped_fixture`` builds the seed of the shipped fixture rows with the package fixture kit.
"""

from __future__ import annotations

import copy
import json
import shutil
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
TINY = ROOT / "tests" / "orchestration" / "fixtures" / "tiny"
TINY_SUITES = ROOT / "tests" / "orchestration" / "fixtures" / "tiny_ge"
TEMPLATES_FILE = ROOT / "config" / "orchestration" / "broker_templates.json"
PLATFORM_FILE = ROOT / "config" / "orchestration" / "platform.json"
SEED_FOLDER = ROOT / "data" / "orchestration" / "fixture_seed"
CASE_CONTAINER = "/workspace/out/case"
OVERLAYS: dict[str, dict[str, str]] = {
    "good": {},
    "bad_catalog": {"models/catalog_marker.sql": "{{ config(tags=['catalog']) }}\nselect 1 {% endif %}\n"},
    "bad_silver_build": {"models/silver_two.sql": "{{ config(tags=['silver']) }}\nselect * from silver_table_that_does_not_exist\n"},
    "bad_silver_test": {"models/silver_one.sql": "{{ config(tags=['silver']) }}\nselect 1 as id\nunion all\nselect 1 as id\n"},
    "bad_gold_build": {"models/gold_one.sql": "{{ config(tags=['gold']) }}\nselect * from gold_table_that_does_not_exist\n"},
    "bad_gold_test": {"models/gold_one.sql": "{{ config(tags=['gold']) }}\nselect cast(null as integer) as id\n"},
    "bad_cross_layer": {"models/gold_one.sql": "{{ config(tags=['gold']) }}\nselect id from {{ ref('silver_two') }}\nwhere id = 1\n"},
}
STEP_ARGUMENTS = {
    "silver_build": ["run", "--select", "tag:silver"],
    "gold_build": ["run", "--select", "tag:gold"],
    "gold_validate": ["test", "--select", "tag:gold_checks"],
    "cross_layer_checks": ["test", "--select", "tag:cross_checks"],
}
CATALOG_ARGUMENTS = ["parse"]
CHECKPOINT_KEYS = ("requires_build_file",)


def seed_run(seeds: Path, run_id: str, overlay: str) -> Path:
    """The seed folder of one run: the tiny project under ``project/`` with the overlay's files replacing its own."""
    folder = seeds / run_id
    shutil.rmtree(folder, ignore_errors=True)
    shutil.copytree(TINY, folder / "project")
    for name, text in OVERLAYS[overlay].items():
        (folder / "project" / name).write_text(text)
    return folder


def dbt_row(arguments: list[str], packages: Path, seconds: int = 300) -> dict[str, Any]:
    """A broker row that runs ``fixture_dbt`` on the run folder."""
    params = {"case_host": "{run_dir}", "case_container": CASE_CONTAINER, "packages_host": str(packages)}
    return {"class": "offline", "no_op": False, "launcher": {"template": "fixture_dbt", "args": arguments, "params": params, "timeout_seconds": seconds}}


def test_inventory(seeds: Path, packages: Path, catalog_job: bool = False, extra: dict[str, list[str]] | None = None) -> dict[str, Any]:
    """The shipped inventory with the work steps on the tiny project and the acquire row seeded from ``seeds/<run_id>``.

    ``catalog_job`` turns the fixture ``catalog_up`` into a job on the tiny project (its shipped row is an intentional no-op), so a case can fail it;
    ``extra`` replaces the arguments of named steps."""
    document: dict[str, Any] = copy.deepcopy(json.loads(TEMPLATES_FILE.read_text()))
    templates = document["templates"]
    for name, arguments in {**STEP_ARGUMENTS, **(extra or {})}.items():
        shipped = templates[name]["modes"]["fixture"].get("launcher") or {}
        row = dbt_row(arguments, packages)
        row["launcher"].update({key: shipped[key] for key in CHECKPOINT_KEYS if key in shipped})
        templates[name]["modes"]["fixture"] = row
    validate = templates["silver_validate"]["modes"]["fixture"]["launcher"]["params"]
    validate["suites_host"] = "{checkout}/data_contracts/great_expectations/tiny_suites"
    validate["baselines_host"] = "{checkout}/data_contracts/great_expectations/tiny_baselines.json"
    if catalog_job:
        templates["catalog_up"]["modes"]["fixture"] = dbt_row(CATALOG_ARGUMENTS, packages)
    control = templates["acquire_run"]["modes"]["fixture"]["control"]
    control["params"] = {**control["params"], "seed": f"{seeds}/{{run_id}}"}
    return document


def prepare_checkout(folder: Path, extra_dags: dict[str, str] | None = None) -> None:
    """A copy of what the Airflow stack mounts, so no test file enters ``dags/``: Compose files, the DAG-side modules, the DAG folder and ``db_dump``.

    ``extra_dags`` adds DAG files to the copy's DAG folder by file name (a variant of the transform DAG that fails inside one task, or a probe DAG)."""
    (folder / "services" / "orchestration").mkdir(parents=True)
    (folder / "services" / "analytics").mkdir(parents=True)
    (folder / "scripts" / "orchestration").mkdir(parents=True)
    (folder / "orchestration").mkdir()
    (folder / "dags").mkdir()
    (folder / "data").mkdir()
    suites = folder / "data_contracts" / "great_expectations"
    suites.mkdir(parents=True)
    (suites / "tiny_suites").mkdir()
    shutil.copy2(TINY_SUITES / "suites" / "tiny_silver.json", suites / "tiny_suites" / "tiny_silver.json")
    shutil.copy2(TINY_SUITES / "baselines.json", suites / "tiny_baselines.json")
    shutil.copy2(ROOT / "docker-compose.yaml", folder / "docker-compose.yaml")
    shutil.copy2(ROOT / "services" / "analytics" / "resources.sql", folder / "services" / "analytics" / "resources.sql")
    platform = json.loads(PLATFORM_FILE.read_text())
    shutil.copy2(ROOT / platform["compose"]["file"], folder / platform["compose"]["file"])
    shutil.copy2(ROOT / "scripts" / "orchestration" / "db_dump.py", folder / "scripts" / "orchestration" / "db_dump.py")
    for name in ("__init__.py", "interface.py", "client.py"):
        shutil.copy2(ROOT / "orchestration" / name, folder / "orchestration" / name)
    shutil.copy2(ROOT / "dags" / "broker_probe_dag.py", folder / "dags" / "broker_probe_dag.py")
    shutil.copy2(ROOT / "dags" / "transform_dag.py", folder / "dags" / "transform_dag.py")
    for name, text in (extra_dags or {}).items():
        (folder / "dags" / name).write_text(text)


def dag_variant(dag_id: str, replacements: list[tuple[str, str]]) -> str:
    """The text of ``dags/transform_dag.py`` as the DAG ``dag_id`` with each named text replaced once; a replacement that does not match fails loudly."""
    text = (ROOT / "dags" / "transform_dag.py").read_text().replace('dag_id="transform_dag"', f'dag_id="{dag_id}"')
    for old, new in replacements:
        if text.count(old) != 1:
            raise ValueError(f"the DAG text {old!r} occurs {text.count(old)} times, not once")
        text = text.replace(old, new)
    return text


def seed_shipped_fixture() -> int:
    """Build the seed of the shipped fixture rows with the package fixture kit: the case folder of a base fixture (this checkout's dbt project, its
    synthetic bronze database and CSV files) copied to ``data/orchestration/fixture_seed``. It needs the launcher's Docker and the dbt packages installed."""
    from scripts.lakehouse import package_fixture

    with package_fixture.reservation() as session:
        case = package_fixture.make_case("transform_seed", session, [])
    if SEED_FOLDER.exists():
        sys.stderr.write(f"transform_fixtures: {SEED_FOLDER} exists; remove it first\n")
        return 3
    shutil.copytree(case.folder, SEED_FOLDER, ignore=shutil.ignore_patterns("target", "logs", "*.duckdb.wal"))
    sys.stdout.write(f"seed written: {SEED_FOLDER.relative_to(ROOT)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(seed_shipped_fixture())
