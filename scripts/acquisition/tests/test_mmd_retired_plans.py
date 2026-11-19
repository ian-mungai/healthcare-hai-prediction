"""The MMD contract holds the two current plans only, and the two plans retired with the registry rewrite are refused by name."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acquisition import collect_mmd_api as collector
from scripts.acquisition import mmd_api_contract as contract
from scripts.acquisition.run_mmd_api_collection import verify_completion
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import RegistryError, read_json
from scripts.acquisition.tests.mmd_world import API_MEASURE, YEAR, World
from tests.support import check

RETIRED_PLAN = "97f72f7b82b39bfbb8afec75778fe335aceb59f57c0b5fa72c773517377fbfe4"
RETIRED_ADDITIONS_PLAN = "1525659d4da7ffd41ce12e4513dc2e04b0e4122a93b21439af2653e19f8656a8"
RETIRED = "MMD plan retired with the registry rewrite"


def test_the_contract_holds_the_two_current_plans_only() -> None:
    check(set(contract.PLANS) == {contract.PLAN_SHA256, contract.ADDITIONS_PLAN_SHA256}, f"a measure is in exactly one plan: {sorted(contract.PLANS)}")
    check(not {RETIRED_PLAN, RETIRED_ADDITIONS_PLAN} & set(contract.PLANS), "a retired plan is not accepted")


@pytest.mark.parametrize("plan_sha256", [RETIRED_PLAN, RETIRED_ADDITIONS_PLAN])
def test_a_retired_plan_is_refused_by_name_when_loaded(plan_sha256: str) -> None:
    with pytest.raises(RegistryError, match=RETIRED):
        contract.load_plan(plan_sha256)


@pytest.mark.parametrize("plan_sha256", [RETIRED_PLAN, RETIRED_ADDITIONS_PLAN])
def test_a_capture_made_under_a_retired_plan_is_refused_by_name(plan_sha256: str) -> None:
    lineage = {"plan_sha256": plan_sha256, "mode": "mmd_api"}
    with pytest.raises(RegistryError, match=RETIRED):
        contract.verify_capture({}, {"source_id": "MMD"}, lineage, contract.PLAN_PATH, False)


def test_an_unknown_plan_hash_keeps_the_generic_refusal() -> None:
    with pytest.raises(RegistryError, match="MMD authorization binding differs"):
        contract.load_plan("0" * 64)


RETIRED_REGISTRY = "12545f626ca9faf9df44382cdf642356a606b3049249e45e665cc659b7ce0b0c"


def retired_branch(world: World, tmp_path: Path) -> Path:
    """A finished branch whose receipt names a retired plan and the registry that went with it, a registry that is no longer shipped."""
    receipt_path = world.capture_api(API_MEASURE, tmp_path / "stored")
    receipt = read_json(receipt_path)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"]) | {"plan_sha256": RETIRED_PLAN, "registry_sha256": RETIRED_REGISTRY}
    receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
    receipt_path.write_bytes(encoded_json(receipt))
    return receipt_path


def test_a_finished_branch_of_a_retired_plan_is_refused_by_name_before_its_registry_is_read(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    world = World(tmp_path, monkeypatch)
    receipt_path = retired_branch(world, tmp_path)
    with pytest.raises(RegistryError, match=RETIRED):
        collector.reuse_capture(receipt_path.parents[2])


def test_a_completed_capture_of_a_retired_plan_is_refused_by_name_before_its_registry_is_read(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    world = World(tmp_path, monkeypatch)
    receipt_path = retired_branch(world, tmp_path)
    receipt = read_json(receipt_path)
    reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
    reconciliation.write_bytes(encoded_json({"snapshot_id": receipt["snapshot_id"], "objects": [], "manifests": []}))
    completed = receipt_path.parents[2] / "completed.json"
    completed.write_bytes(encoded_json({"model_eligible": False, "reconciliation_path": str(reconciliation), "measure_id": API_MEASURE, "year": YEAR}))
    with pytest.raises(RegistryError, match=RETIRED):
        verify_completion(completed, True)
