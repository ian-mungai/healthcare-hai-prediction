"""The source registry's notes state the current person-field handling; revision 4 records the change and revision 3 stays catalogued."""

from __future__ import annotations

import json
from typing import Any

from scripts.acquisition.registry_additions import additions_lock
from scripts.acquisition.repin_registry import pin_files
from scripts.acquisition.source_registry import REGISTRY_PATH, VERSION_CATALOG_LOCK_PATH, VERSION_CATALOG_PATH, canonical_hash, read_json
from tests.support import check

CONFIG = REGISTRY_PATH.parent
REV3 = "60b89b7ac6d1913e8f8ba0d305d02ba7ab023d0165ef78927631cb3c10303a25"
# The retired handling: an original kept apart on the Mac, owner-only, with only some rows going to storage.
RETIRED = ("kept on this mac", "owner-only", "never leave the local original", "unredacted original")


def strings(value: Any) -> list[str]:
    """Every text value in a JSON document."""
    if isinstance(value, str):
        return [value]
    items = value.values() if isinstance(value, dict) else value if isinstance(value, list) else []
    return [text for item in items for text in strings(item)]


def values_of(value: Any, key: str) -> list[Any]:
    """Every value stored under KEY at any depth."""
    found: list[Any] = []
    if isinstance(value, dict):
        found += [value[key]] if key in value else []
        found += [inner for item in value.values() for inner in values_of(item, key)]
    elif isinstance(value, list):
        found += [inner for item in value for inner in values_of(item, key)]
    return found


def test_no_registry_text_describes_the_retired_owner_only_originals() -> None:
    for name in ("source_registry.json", "registry_additions.json"):
        hits = [text for text in strings(read_json(CONFIG / name)) if any(phrase in text.lower() for phrase in RETIRED)]
        check(not hits, f"{name} still describes the retired owner-only originals handling in {len(hits)} notes: {hits[:1]}")


def test_the_person_field_notes_state_what_the_code_does() -> None:
    registry = read_json(REGISTRY_PATH)
    owners = values_of(registry, "privacy")
    check(len(owners) == 3 and len(set(owners)) == 1, f"the CMS owners privacy note is expected three times, once as one text: {owners}")
    scopes = [text for text in values_of(registry, "scope") if "CMS Hospital All Owners" in text]
    check(len(scopes) == 6 and len(set(scopes)) == 1, f"the CMS owners scope note is expected six times, once as one text: {scopes}")
    hcai = values_of(registry, "original_2012_2017")
    check(len(hcai) == 3 and len(set(hcai)) == 1, f"the HCAI utilization original note is expected three times, once as one text: {hcai}")
    for text in (owners[0], scopes[0], hcai[0]):
        check("bronze pseudonymizes" in text and "no original is kept" in text, text)
    check("keyed HMAC" in owners[0] and "keyed HMAC" in hcai[0], "the notes name the keyed HMAC")


def test_revision_4_is_recorded_with_revision_3_as_its_parent() -> None:
    registry = read_json(REGISTRY_PATH)
    check(registry["registry_revision"] == 4, "the registry is revision 4")
    check(registry["closeout"]["revision_4"]["parent_registry_sha256"] == REV3, "revision 4 names revision 3 as its parent")
    check(canonical_hash(registry) != REV3, "revision 4 differs from revision 3")
    lock = read_json(CONFIG / "source_registry_lock.json")
    check(lock["registry_sha256"] == canonical_hash(registry), "the registry lock binds revision 4")
    additions = read_json(CONFIG / "registry_additions.json")
    check(additions["base_registry_sha256"] == canonical_hash(registry), "the additions are based on revision 4")
    check(read_json(CONFIG / "registry_additions_lock.json") == additions_lock(additions, registry), "the additions lock binds revision 4")


def test_the_catalog_lists_revision_3_as_a_predecessor_and_revision_4_as_current() -> None:
    catalog = read_json(VERSION_CATALOG_PATH)
    check(catalog["current_registry_sha256"] == canonical_hash(read_json(REGISTRY_PATH)), "the catalog names revision 4 as current")
    old = [item for item in catalog["legacy_versions"] if item["registry_sha256"] == REV3]
    check(len(old) == 1 and set(old[0]["files"]) == {"registry", "lock", "additions", "additions_lock"}, "revision 3 is catalogued with its four files")
    check(read_json(VERSION_CATALOG_LOCK_PATH)["catalog_sha256"] == canonical_hash(catalog), "the catalog lock matches the catalog")


def test_no_plan_or_log_still_pins_revision_3() -> None:
    pinned = [path.name for path in pin_files(CONFIG) if REV3 in path.read_text(encoding="utf-8")]
    check(not pinned, f"these plans or records still pin revision 3: {pinned}")
    locks = [path.name for path in sorted(CONFIG.glob("*.lock.json")) if REV3 in path.read_text(encoding="utf-8")]
    check(not locks, f"these locks still pin revision 3: {locks}")
    for name in ("census_acs_tables_plan", "nber_hospital_compare_plan", "reference_downloads_plan"):
        plan = json.loads((CONFIG / f"{name}.json").read_text(encoding="utf-8"))
        check(plan["registry_sha256"] == read_json(VERSION_CATALOG_PATH)["current_registry_sha256"], f"{name} pins the current registry")
