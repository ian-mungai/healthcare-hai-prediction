"""The MMD plans follow the rewritten registry and the redacted publisher page, and the tracked bundle holds exactly the files the contract pins."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.acquisition import mmd_api_contract as contract
from scripts.acquisition import repin_mmd_plans as repin
from scripts.acquisition.source_registry import canonical_hash
from tests.support import check

BUNDLE = Path("config/acquisition/baseline_run/plan_inputs")
PLAN = {"registry_sha256": "1" * 64, "references": {"map.html": {"path": "p/map.html", "sha256": "a" * 64}, "other.js": {"path": "p/o.js", "sha256": "b" * 64}}}


def test_redaction_replaces_every_mailbox_and_keeps_the_rest() -> None:
    page = '.attr("href", "mailto:Someone@example.test").text("Someone@example.test");'
    check(repin.redact_page(page) == '.attr("href", "mailto:(redacted)").text("(redacted)");', "both mailbox strings")


def test_a_plan_moves_to_the_new_registry_page_digest_and_additions_lock() -> None:
    raw = (json.dumps({**PLAN, "registry_additions_lock_sha256": "c" * 64}, indent=1, sort_keys=True) + "\n").encode()
    moved = json.loads(repin.repin_plan(raw, "1" * 64, "2" * 64, "d" * 64, "e" * 64))
    check(moved["registry_sha256"] == "2" * 64 and moved["references"]["map.html"]["sha256"] == "d" * 64, "registry and page")
    check(moved["references"]["other.js"]["sha256"] == "b" * 64 and moved["registry_additions_lock_sha256"] == "e" * 64, "others unchanged, lock moved")


def test_the_bundle_holds_the_plans_and_references_the_contract_pins() -> None:
    for sha, path in contract.CURRENT_PLANS.items():
        raw = (BUNDLE / path).read_bytes()
        check(hashlib.sha256(raw).hexdigest() == sha, f"bundle plan digest differs from the contract constant for {path.name}")
        plan = json.loads(raw)
        for reference in plan["references"].values():
            held = not (BUNDLE / reference["path"]).is_file()
            check(
                held or hashlib.sha256((BUNDLE / reference["path"]).read_bytes()).hexdigest() == reference["sha256"], f"reference differs: {reference['path']}"
            )
        versions = json.loads(Path("config/acquisition/registry_versions.json").read_text())["legacy_versions"]
        check(any(v["registry_sha256"] == plan["registry_sha256"] for v in versions), "plan registry hash is catalogued")
        check(
            canonical_hash(json.loads((BUNDLE / "data/acquisition_planning/registry_legacy_20260929/source_registry.json").read_text()))
            == plan["registry_sha256"],
            "bundle registry matches the plan",
        )
