"""The expectations of the MMD E2E runner hold for the real collector CLI, over a synthetic capture made under the current plan.

The runner needs the retained captures of the pilot years, so this case checks its expectation table without them: every refusal it expects, the named
refusal of a retired plan and the acceptance of a current-plan capture. The three county-code cases need the real rows of a retained year, so they stay
with the runner.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from scripts.acquisition import collect_mmd_api as collector
from scripts.acquisition import mmd_api_contract as contract
from scripts.acquisition import run_mmd_api_e2e as runner
from scripts.acquisition.capture import CaptureError
from scripts.acquisition.source_registry import RegistryError
from scripts.acquisition.tests.mmd_world import API_MEASURE, YEAR, World
from tests.support import check

REFUSALS = (RegistryError, CaptureError, ValueError, KeyError, OSError)
CHECKED_KINDS = sorted((set(runner.NEGATIVE_EXPECTED) - runner.NEEDS_STORED_ROWS) | set(runner.RETIRED_EXPECTED))


def validate(monkeypatch: pytest.MonkeyPatch, receipt: Path) -> None:
    arguments = ["collect_mmd_api", "--measure-id", API_MEASURE, "--year", str(YEAR), "--validate-receipt", str(receipt)]
    monkeypatch.setattr(sys, "argv", arguments)
    collector.main()


def test_the_runner_retires_exactly_the_two_plans_the_contract_retires() -> None:
    check({runner.RETIRED_PLAN, runner.RETIRED_ADDITIONS_PLAN} == set(contract.RETIRED_PLAN_SHA256S), "the runner names the retired plans the contract refuses")
    check(set(runner.RETIRED_EXPECTED.values()) == {contract.RETIRED_MESSAGE}, "a retired plan is expected to be refused by the contract's own message")


def test_the_runner_expects_a_capture_to_pass_only_under_a_plan_the_contract_accepts() -> None:
    check(runner.plan_status({"plan_sha256": contract.PLAN_SHA256}) == "current", "the first plan is current")
    check(runner.plan_status({"plan_sha256": contract.ADDITIONS_PLAN_SHA256}) == "current", "the additions plan is current")
    check(runner.plan_status({"plan_sha256": runner.RETIRED_PLAN}) == "retired", "the retired plan is retired")
    check(runner.plan_status({"plan_sha256": runner.RETIRED_ADDITIONS_PLAN}) == "retired", "the retired additions plan is retired")
    check(runner.plan_status({"plan_sha256": "0" * 64}) == "unknown", "any other plan is unknown")


def test_a_current_plan_capture_passes_the_cli_validation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    world = World(tmp_path, monkeypatch)
    receipt = world.capture_api(API_MEASURE, tmp_path / "pilot")
    lineage = json.loads(json.loads(receipt.read_text())["lineage"]["extraction_or_query"])
    check(runner.plan_status(lineage) == "current", "the synthetic capture is made under a current plan")
    validate(monkeypatch, receipt)
    check('"status": "valid"' in capsys.readouterr().out, "the CLI reports the capture valid")


@pytest.mark.parametrize("kind", CHECKED_KINDS)
def test_each_expected_refusal_is_the_one_the_cli_gives(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str) -> None:
    world = World(tmp_path, monkeypatch)
    receipt = world.capture_api(API_MEASURE, tmp_path / "pilot")
    mutated = runner.negative_copy(receipt, kind, tmp_path / "disposable")
    expected = {**runner.NEGATIVE_EXPECTED, **runner.RETIRED_EXPECTED}[kind]
    with pytest.raises(REFUSALS) as caught:
        validate(monkeypatch, mutated)
    check(expected in str(caught.value), f"{kind}: expected {expected!r}, got {caught.value}")
