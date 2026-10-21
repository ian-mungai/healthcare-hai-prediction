"""Prepare isolated publication DAG checkouts from shipped source and the synthetic fixture kit."""

from __future__ import annotations

import ast
import contextlib
import json
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.orchestration import transform_fixtures
from scripts.orchestration.run_transform_e2e import TEMP_ROOT, Stack

ROOT = Path(__file__).resolve().parents[2]
DAGS = ("silver_transform_dag", "gold_transform_dag", "gold_maintenance_dag")
CONTRACTS = {"silver": "silver_publish.json", "gold": "gold_contract.json"}
OUTLET_RULE = 'name == "commit_publish"'
REGISTERED_KEEP = "            keep.add(str(item.run_id))\n"
PENDING_KEEP = "            keep |= set(known[start:])\n"
HIT = "        self.hits.append(point)\n"
BARRIER = """        self.hits.append(point)
        barrier = Path("/workspace/out/publish/a3_barrier.txt")
        if barrier.is_file():
            with (barrier.parent / "a3_progress.log").open("a") as progress:
                progress.write(point + "\\n")
            if point.startswith(barrier.read_text().strip()):
                import time

                time.sleep(600)
"""
AFTER_MERGE = '        ctx.faults.hit(f"candidates:after_merge:{name}")\n'
FAST_FORWARD_LOOP = "    for name, candidate in snapshot_set.items():\n        if heads[name] != candidate:\n"
EARLY_ROW = """    ensure_runs_table(ctx)
    early = {
        "run_id": ctx.run_id,
        "mode": ctx.mode,
        "committed_at": datetime.now(UTC).isoformat(),
        "sealed_sha256": ctx.sealed_sha256,
        "export_manifest_sha256": manifest_sha,
        "backup_name": backup["name"],
        "backup_sha256": backup["backup_sha256"],
        "snapshot_set": snapshot_set,
    }
    ctx.spark.sql(MERGE_RUN.format(table=ctx.fq(ctx.plan.runs_table), view=runs_view(ctx, early)))
"""


def work_steps(dag: str) -> tuple[str, ...]:
    """The ordered work steps a shipped DAG file declares, read without importing Airflow."""
    for node in ast.parse((ROOT / "dags" / f"{dag}.py").read_text()).body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "WORK_STEPS" for target in node.targets):
            return tuple(str(step) for step in ast.literal_eval(node.value))
    raise RuntimeError(f"{dag} declares no WORK_STEPS")


def replace_once(path: Path, old: str, new: str) -> None:
    """Edit a private checkout file in exactly one place or fail before any run."""
    source = path.read_text()
    if source.count(old) != 1:
        raise RuntimeError(f"{path.name}: the edit no longer matches exactly one place: {old.strip()[:60]!r}")
    path.write_text(source.replace(old, new))


def table_state(table: Path) -> dict[str, Any]:
    """The main snapshot, branch names and live record count of one Iceberg table, read from its current metadata file."""
    metadata = table / "metadata"
    version = (metadata / "version-hint.text").read_text().strip()
    document = json.loads((metadata / f"v{version}.metadata.json").read_text())
    refs = {name: int(ref["snapshot-id"]) for name, ref in document.get("refs", {}).items()}
    snapshots = {int(item["snapshot-id"]): item for item in document.get("snapshots", [])}
    main = refs.get("main")
    return {
        "main": main,
        "branches": sorted(name for name in refs if name != "main"),
        "records": int(snapshots[main]["summary"]["total-records"]) if main in snapshots else 0,
    }


def table_states(warehouse: Path, layer: str) -> tuple[dict[str, dict[str, Any]], str]:
    """The state of every table in the layer's publish namespace, and the name of its run table."""
    entry = json.loads((ROOT / "data_contracts/lakehouse" / CONTRACTS[layer]).read_text())["publish"]
    folder = warehouse / str(entry["namespaces"]["publish_fixture"])
    tables = {path.name: table_state(path) for path in sorted(folder.iterdir()) if (path / "metadata").is_dir()} if folder.is_dir() else {}
    return tables, str(entry["runs_table"])


def candidate_branches(warehouse: Path, layer: str) -> dict[str, list[str]]:
    """The tables that hold a non-main branch (a candidate in flight), with the branch names."""
    return {name: state["branches"] for name, state in table_states(warehouse, layer)[0].items() if state["branches"]}


def published(warehouse: Path, layer: str) -> dict[str, Any]:
    """What a reader of the layer's namespace can see: each table's main snapshot and the number of committed publish rows."""
    tables, runs = table_states(warehouse, layer)
    return {
        "mains": {name: state["main"] for name, state in tables.items() if name != runs and state["main"] is not None},
        "published_runs": tables[runs]["records"] if runs in tables else 0,
    }


@contextlib.contextmanager
def verifier_accepting_mismatch() -> Iterator[None]:
    """Make the broker's control layer accept the launcher's refusal of `fixture_verify`: the real verification is still submitted and refuses."""
    from scripts.orchestration import pipeline_control
    from scripts.orchestration.run_control import ControlRefused

    original = pipeline_control.perform

    def accepting(broker: Any, request: Any, action: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            return original(broker, request, action, params)
        except ControlRefused:
            if action != "fixture_verify":
                raise
            return {"unchanged": True}

    with mock.patch.object(pipeline_control, "perform", accepting):
        yield


class PipelineStack(Stack):
    """Run the shipped publication rows against a private checkout and local Iceberg catalog."""

    mutant: str | None = None
    retain_sets: int | None = None
    barrier: bool = False

    @staticmethod
    def dag_ids() -> tuple[str, ...]:
        return DAGS

    def base_inventory(self) -> dict[str, Any]:
        return dict(json.loads(transform_fixtures.TEMPLATES_FILE.read_text()))

    def visible(self, layer: str) -> dict[str, Any]:
        """The layer's published state as read from the catalog folder on the host."""
        return published(self.warehouse(), layer)

    def branches(self, layer: str) -> dict[str, list[str]]:
        """The layer's tables that hold a candidate branch, read from the catalog folder on the host."""
        return candidate_branches(self.warehouse(), layer)

    def warehouse(self) -> Path:
        return self.registry.parent / "pipeline_catalog" / "warehouse"

    def prepare_checkout(self, extra: dict[str, str]) -> None:
        """Copy the production code and synthetic seeds; mount no main-checkout data or credentials."""
        for name in ("orchestration", "scripts", "services", "config", "data_contracts", "dbt", "dags"):
            shutil.copytree(ROOT / name, self.checkout / name, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".env", "*.tfvars.json"))
        runtime = self.checkout / "orchestration/dag_runtime.py"
        maintenance = self.checkout / "scripts/lakehouse/gold_maintenance.py"
        if self.mutant == "asset_on_build":
            replace_once(runtime, OUTLET_RULE, 'name in ("gold_build", "commit_publish")')
        publish = self.checkout / "scripts/lakehouse/gold_publish.py"
        if self.mutant == "maintenance_ignores_registrations":
            replace_once(maintenance, REGISTERED_KEEP, "            pass\n")
        if self.mutant == "maintenance_ignores_pending":
            replace_once(maintenance, PENDING_KEEP, "            pass\n")
        if self.mutant == "candidates_visible":
            replace_once(publish, AFTER_MERGE, "        ctx.sql(FAST_FORWARD, name=name, branch=ctx.branch)\n" + AFTER_MERGE)
        if self.mutant == "row_before_fast_forward":
            replace_once(publish, FAST_FORWARD_LOOP, EARLY_ROW + FAST_FORWARD_LOOP)
        if self.barrier:
            replace_once(publish, HIT, BARRIER)
        if self.retain_sets is not None:
            replace_once(self.checkout / "data_contracts/lakehouse/gold_contract.json", '"retain_sets": 3', f'"retain_sets": {self.retain_sets}')
        shutil.copy2(ROOT / "docker-compose.yaml", self.checkout / "docker-compose.yaml")
        shutil.copytree(ROOT / "data/analytics/dbt/dbt_packages", self.checkout / "data/analytics/dbt/dbt_packages")
        seed = ROOT / "data/orchestration/fixture_seed"
        if not (seed / "fixture_lakehouse.duckdb").is_file() or not (seed / "project").is_dir():
            raise RuntimeError("publication fixture seed is absent; generate it with --seed first")
        shutil.copytree(seed, self.checkout / "data/orchestration/fixture_seed")
        shutil.copytree(seed / "project", self.checkout / "dbt", dirs_exist_ok=True)


def make_seed() -> dict[str, Any]:
    """Generate a fresh, synthetic bronze database using only launcher-owned fixture attempts."""
    from scripts.lakehouse import package_fixture

    destination = ROOT / "data/orchestration/fixture_seed"
    if destination.exists():
        raise RuntimeError("the fixture seed already exists; retain it or choose a fresh review copy")
    from scripts.lakehouse.run_gold_publish_e2e import Session

    owned = Session()
    previous = package_fixture.TEST_REGISTRY
    try:
        owned.start()
        package_fixture.TEST_REGISTRY = str(owned.registry)
        case = package_fixture.make_case("a3_publication_seed", package_fixture.Session(owned.client, owned.allocation), [])
        shutil.copytree(case.folder, destination, ignore=shutil.ignore_patterns("target", "logs"))
    finally:
        package_fixture.TEST_REGISTRY = previous
        cleanup = owned.close()
    if cleanup["errors"] or cleanup["launcher_left"] or cleanup["containers_left"]:
        raise RuntimeError("the synthetic seed test registry did not clean up")
    return {"seed": str(destination), "source": "package_fixture.make_case", "synthetic": True, "registry_kind": "test", "cleanup": cleanup}


def thaw_owned_directories(work: Path) -> None:
    """Allow removal of this stopped test session's frozen snapshot directories."""
    import os
    import stat

    if work.parent.resolve() != TEMP_ROOT.resolve() or not work.name.startswith("a3dag_") or work.is_symlink():
        raise RuntimeError("snapshot cleanup requires an owned A3 test-session folder")
    for path in (work, *work.rglob("*")):
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            if info.st_uid != os.getuid():
                raise RuntimeError("snapshot cleanup found a directory owned by another user")
            path.chmod(stat.S_IMODE(info.st_mode) | 0o700)


def finish_publication(stack: PipelineStack, work: Path) -> dict[str, Any]:
    """Verify stack shutdown and launcher exit before thawing and removing test snapshots."""
    from scripts.orchestration import run_transform_e2e as harness

    stack.keep_work = True
    result = harness.finish(stack, work)
    if result["errors"]:
        return result
    try:
        thaw_owned_directories(work)
        result["removal"] = harness.remove_work(work, stack.launcher_process)
        result["errors"] += result["removal"]
    except (OSError, RuntimeError) as error:
        result["errors"].append(f"snapshot directory cleanup: {type(error).__name__}: {error}")
    result["temporary_folder_removed"] = not work.exists()
    if result["errors"]:
        result["retained"] = str(work)
        result["finish_with"] = f".venv/bin/python -m scripts.orchestration.run_transform_e2e --finish-cleanup {work}"
    return result
