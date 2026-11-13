"""The MMD plans before the registry rewrite stay accepted for the captures made under them, and no other plan hash is."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import mmd_api_contract as contract
from scripts.acquisition.source_registry import RegistryError
from tests.support import check

OLD_PLAN = "97f72f7b82b39bfbb8afec75778fe335aceb59f57c0b5fa72c773517377fbfe4"
OLD_ADDITIONS_PLAN = "1525659d4da7ffd41ce12e4513dc2e04b0e4122a93b21439af2653e19f8656a8"
OLD_PAGE = "data/e2e/mmd_api_comparison/20260925/references/map.html"


def test_a_capture_may_name_the_plan_before_or_after_the_registry_rewrite() -> None:
    check(
        set(contract.PLANS) == {OLD_PLAN, OLD_ADDITIONS_PLAN, contract.PLAN_SHA256, contract.ADDITIONS_PLAN_SHA256},
        "the old and the new plan hash and the old and the new additions plan hash",
    )
    check(OLD_PLAN != contract.PLAN_SHA256 and OLD_ADDITIONS_PLAN != contract.ADDITIONS_PLAN_SHA256, "the old hashes are not the new ones")


def test_a_new_capture_reads_only_the_current_plans() -> None:
    check(set(contract.CURRENT_PLANS) == {contract.PLAN_SHA256, contract.ADDITIONS_PLAN_SHA256}, "a measure is in exactly one current plan")


def test_an_unknown_plan_hash_is_refused() -> None:
    with pytest.raises(RegistryError, match="MMD authorization binding differs"):
        contract.load_plan("0" * 64)


def test_an_old_plan_without_its_archived_bytes_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setitem(contract.PLANS, OLD_PLAN, tmp_path / "absent" / "collection_plan.json")
    with pytest.raises(OSError):
        contract.load_plan(OLD_PLAN)


def test_an_old_plan_reads_its_saved_page_from_the_archive_and_the_new_one_from_the_current_path() -> None:
    check({OLD_PAGE: str(contract.LEGACY_ARCHIVE / "references/map.html")} == contract.LEGACY_MOVES, "only the redacted page moved")
    check(all(str(contract.LEGACY_ARCHIVE) in target for target in contract.LEGACY_MOVES.values()), "the old page sits in the archive")


def test_a_plan_pinned_to_an_older_registry_reads_the_additions_of_that_registry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    older = {"additions": tmp_path / "old_additions.json", "additions_lock": tmp_path / "old_lock.json"}
    older["additions_lock"].write_bytes(b"lock")
    seen: dict[str, Any] = {}

    def registry_for(expected_sha256: str | None = None) -> dict[str, Any]:
        return {"sha": expected_sha256 or "current"}

    def paths(registry_sha256: str) -> dict[str, Path]:
        seen["registry"] = registry_sha256
        return older

    def additions(registry: dict[str, Any], path: Path, lock_path: Path) -> list[dict[str, Any]]:
        seen["files"] = (path, lock_path)
        return [{"id": "C258.80", "gate_status": "not_run", "preserved_controls": {"current_review_decision": "held"}}]

    monkeypatch.setattr(contract, "load_registry", registry_for)
    monkeypatch.setattr(contract, "canonical_hash", lambda registry: registry["sha"])
    monkeypatch.setattr(contract, "load_registry_additions", additions)
    monkeypatch.setattr("scripts.acquisition.source_registry.registry_version_paths", paths)
    plan = {
        "registry_sha256": "older",
        "registry_additions_lock_sha256": contract.digest(b"lock"),
        "conditions": [{"measure_id": "C258.80", "gate_status": "not_run", "review_decision": "held"}],
    }
    contract.verify_additions(plan)
    check(seen["registry"] == "older" and seen["files"] == (older["additions"], older["additions_lock"]), "the older registry's own additions and lock")
