"""The MMD contract holds the two current plans only, and the two plans retired with the registry rewrite are refused by name."""

from __future__ import annotations

import pytest

from scripts.acquisition import mmd_api_contract as contract
from scripts.acquisition.source_registry import RegistryError
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
