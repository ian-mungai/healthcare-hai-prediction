"""The legacy registry rewrite changes only provenance pointers, deterministically, and re-locks the registry and its additions lock."""

from __future__ import annotations

import json

import pytest

from scripts.acquisition import rewrite_registry_provenance as rewrite
from scripts.acquisition.source_registry import canonical_hash
from tests.support import check

LEGACY = "external://" + "ho" + "me/.codex/"
POINTER = LEGACY + "memories/notes/a.md"
PLACEHOLDER_PHONE = "(redacted)"
SAMPLE_PHONE = f"({3 * '1'}) {3 * '2'}-{4 * '3'}"


def registry_text() -> str:
    registry = {
        "registry_version": 1,
        "sources": [{"source_id": "S1", "evidence": [POINTER, f"Reviewed; [B02]({POINTER}:7)"], "profile": {"Telephone.Number": {"examples": [SAMPLE_PHONE]}}}],
    }
    return json.dumps(registry, indent=2, sort_keys=True) + "\n"


def test_pointers_become_workspace_provenance_paths() -> None:
    text = rewrite.rewrite_pointers(registry_text())
    check(LEGACY not in text, "legacy pointer remains in text")
    check("/workspace/provenance/memories/notes/a.md" in text, "'/workspace/provenance/memories/notes/a.md' in text")
    check("(/workspace/provenance/memories/notes/a.md:7)" in text, "'(/workspace/provenance/memories/notes/a.md:7)' in text")


def test_rewrite_is_deterministic_and_idempotent() -> None:
    once = rewrite.rewrite_pointers(registry_text())
    check(rewrite.rewrite_pointers(once) == once, "rewrite.rewrite_pointers(once) == once")
    check(rewrite.rewrite_pointers(registry_text()) == once, "rewrite.rewrite_pointers(registry_text()) == once")


def test_phone_examples_stay_unless_redaction_is_requested() -> None:
    check(SAMPLE_PHONE in rewrite.rewrite_pointers(registry_text()), "'phone sample' in rewrite.rewrite_pointers(registry_text()")
    check(
        PLACEHOLDER_PHONE in rewrite.rewrite_pointers(registry_text(), redact_phone_examples=True),
        "PLACEHOLDER_PHONE in rewrite.rewrite_pointers(registry_text(",
    )
    check(
        SAMPLE_PHONE not in rewrite.rewrite_pointers(registry_text(), redact_phone_examples=True),
        "'phone sample' not in rewrite.rewrite_pointers(registry_te",
    )


def test_new_lock_binds_the_rewritten_registry_hash_and_additions_lock_follows() -> None:
    text = rewrite.rewrite_pointers(registry_text())
    lock = {"lock_version": 1, "registry_sha256": "0" * 64}
    additions_lock = {"base_registry_sha256": "0" * 64, "additions_sha256": "1" * 64}
    new_lock, new_additions_lock = rewrite.relock(text, lock, additions_lock)
    expected = canonical_hash(json.loads(text))
    check(new_lock["registry_sha256"] == expected, "new_lock['registry_sha256'] == expected")
    check(new_additions_lock["base_registry_sha256"] == expected, "new_additions_lock['base_registry_sha256'] == expected")
    check(new_additions_lock["additions_sha256"] == "1" * 64, "new_additions_lock['additions_sha256'] == '1' * 64")


def test_rewrite_refuses_text_that_is_not_json_after_replacement() -> None:
    with pytest.raises(ValueError, match="not valid JSON"):
        rewrite.rewrite_pointers('{"a": "' + LEGACY + 'x"')
