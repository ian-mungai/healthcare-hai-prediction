"""Build source registry revision 4 from the archived revision 3 bytes (owner decision A, Nov 17 2026, about 3:17 PM PST).

Twelve notes still described the retired owner-only originals handling: the CMS Hospital All Owners and HCAI utilization originals kept on the Mac with
only some rows going to storage. They now state what the code does: each file is stored whole as published, no original is kept, and bronze pseudonymizes
the person fields at load with the keyed HMAC. Nothing else in the registry changes. The registry lock, the additions file and its lock, and the version
catalog and its lock are rebound to the new hash; revision 3 is catalogued with its archived bytes so receipts that recorded it keep verifying. Every plan and
record that pinned revision 3 moves to revision 4 and is re-locked, and each old plan is archived and listed as a predecessor of the new one (the way
scripts/acquisition/repin_registry.py does it). Every run rebuilds from the archive, so a rerun writes identical bytes. Run from the repository root:

    .venv/bin/python -m scripts.acquisition.one_off.registry_rev4.build_rev4          # check: no file changes
    .venv/bin/python -m scripts.acquisition.one_off.registry_rev4.build_rev4 --write
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.legacy_versions import ARCHIVE
from scripts.acquisition.registry_additions import additions_lock, canonical_json
from scripts.acquisition.repin_registry import legacy_catalog, pin_files, relock_policies, relock_to_fixed_point, replace_pin
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, require

CONFIG = REPO_ROOT / "config/acquisition"
REV3_ARCHIVE = ARCHIVE / "registry_rev3"
PLANS_ARCHIVE = "data/acquisition_planning/acquisition_legacy_20261009/pre_registry_rev4_20261117"
REV3 = "60b89b7ac6d1913e8f8ba0d305d02ba7ab023d0165ef78927631cb3c10303a25"
FILES = {
    "registry": "source_registry.json",
    "lock": "source_registry_lock.json",
    "additions": "registry_additions.json",
    "additions_lock": "registry_additions_lock.json",
}
CATALOG = CONFIG / "registry_versions.json"
CATALOG_LOCK = CONFIG / "registry_versions.lock.json"
DECIDED = "2026-11-17"
RETIRED = ("kept on this mac", "owner-only", "never leave the local original", "unredacted original")
# The retired note, its replacement and how often the registry holds it: the CMS owners privacy note (the source and the Group 2 decision), the CMS
# owners scope note (controls C067 and C068, twice each, and the Group 2 decision twice) and the HCAI utilization original note (the same three places).
NOTES = {
    "original kept on this Mac only, unchanged, owner-only permissions, like HCAI_UTIL; only organisation owner rows go to S3": (
        "no original is kept; each release is stored whole as published and bronze pseudonymizes the person fields of individual owners at load with the "
        "keyed HMAC (owner decision, Nov 17 2026)",
        3,
    ),
    "organisation owner rows only from CMS Hospital All Owners; individual owner rows never leave the local original": (
        "all owner rows from CMS Hospital All Owners, stored whole as published; no original is kept and bronze pseudonymizes the person fields of individual "
        "owners at load",
        6,
    ),
    "keep the unredacted original until collection closeout, then delete it after the user confirms the exact list; stop iCloud syncing it now": (
        "no original is kept (owner decision, Nov 17 2026: originals deleted); the HCAI utilization workbooks are stored whole as published and bronze "
        "pseudonymizes their person cells at load with the keyed HMAC",
        3,
    ),
}


def dump(value: Any) -> str:
    """The registry files' own format: 2-space indent, ASCII escapes, one trailing newline."""
    return json.dumps(value, indent=2, ensure_ascii=True, allow_nan=False) + "\n"


def replaced(value: Any, counts: dict[str, int]) -> Any:
    """A copy of the JSON value with every retired note replaced; COUNTS gains one per replacement."""
    if isinstance(value, str):
        if value in NOTES:
            counts[value] += 1
            return NOTES[value][0]
        return value
    if isinstance(value, list):
        return [replaced(item, counts) for item in value]
    if isinstance(value, dict):
        return {key: replaced(item, counts) for key, item in value.items()}
    return value


def texts(value: Any) -> list[str]:
    """Every text value in a JSON document."""
    if isinstance(value, str):
        return [value]
    items = value.values() if isinstance(value, dict) else value if isinstance(value, list) else []
    return [text for item in items for text in texts(item)]


def archived() -> dict[str, Any]:
    """Read revision 3 from the archive; its bytes must reproduce under the registry format and hash to revision 3."""
    documents = {}
    for key, name in FILES.items():
        raw = (REV3_ARCHIVE / name).read_text(encoding="utf-8")
        documents[key] = json.loads(raw)
        require(dump(documents[key]) == raw or canonical_json(documents[key]) == raw, f"archived {name} does not reproduce its bytes")
    require(canonical_hash(documents["registry"]) == REV3, "archived registry is not revision 3")
    return documents


def archive_revision_3() -> None:
    """Copy the four revision 3 files into the archive once, from the tracked files while they still are revision 3."""
    if REV3_ARCHIVE.is_dir():
        return
    require(
        canonical_hash(json.loads((CONFIG / FILES["registry"]).read_text(encoding="utf-8"))) == REV3,
        "the tracked registry is not revision 3; no archive to make",
    )
    REV3_ARCHIVE.mkdir(parents=True)
    for name in FILES.values():
        shutil.copyfile(CONFIG / name, REV3_ARCHIVE / name)


def apply_notes(registry: dict[str, Any]) -> dict[str, Any]:
    """Return revision 4: the retired notes replaced and the revision recorded."""
    counts = {note: 0 for note in NOTES}
    new = replaced(registry, counts)
    for note, (_, expected) in NOTES.items():
        require(counts[note] == expected, f"a retired note occurs {counts[note]} times, expected {expected}: {note[:40]}")
    require(not [text for text in texts(new) if any(phrase in text.lower() for phrase in RETIRED)], "a retired phrase remains")
    closeout = new["closeout"]
    closeout["user_decisions"].append(
        {
            "amends": closeout["user_decisions"][-1]["amends"],
            "decision": (
                f"Owner decision A, {DECIDED} (about 3:17 PM PST): correct the registry notes that describe the retired handling of originals kept apart."
            ),
            "decision_date": DECIDED,
            "measures": ["C067", "C068"],
            "model_eligible": False,
            "rule": (
                "Notes only, for CMS_OWNERS and HCAI_UTIL: no original is kept; each file is stored whole as published and bronze pseudonymizes the person "
                "fields at load with the keyed HMAC. Replaces the original-keeping wording of the 2026-09-29 Group 2 decision."
            ),
        }
    )
    closeout["revision_4"] = {
        "parent_registry_sha256": REV3,
        "reason": (
            "Corrects twelve notes that described the retired handling of originals kept apart; "
            "revision 3 is catalogued with its archived bytes so its receipts keep verifying."
        ),
    }
    new["registry_revision"] = 4
    return new


def build() -> dict[Path, str]:
    """Return every file revision 4 writes outside the plans and records it re-pins, keyed by path."""
    old = archived()
    registry = apply_notes(old["registry"])
    require([c["id"] for c in old["registry"]["measure_controls"]] == [c["id"] for c in registry["measure_controls"]], "control IDs or order changed")
    require(old["registry"]["counts"] == registry["counts"] and old["registry"]["inputs"] == registry["inputs"], "counts or inputs changed")
    new_hash = canonical_hash(registry)
    lock = {**old["lock"], "registry_sha256": new_hash}
    additions = {**old["additions"], "base_registry_sha256": new_hash}
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    rev3 = {
        "registry_sha256": REV3,
        "registry_path": str((REV3_ARCHIVE / FILES["registry"]).relative_to(REPO_ROOT)),
        "lock_path": str((REV3_ARCHIVE / FILES["lock"]).relative_to(REPO_ROOT)),
        "files": {
            key: {"path": str((REV3_ARCHIVE / name).relative_to(REPO_ROOT)), "sha256": hashlib.sha256((REV3_ARCHIVE / name).read_bytes()).hexdigest()}
            for key, name in FILES.items()
        },
    }
    catalog["legacy_versions"] = [item for item in catalog["legacy_versions"] if item["registry_sha256"] != REV3] + [rev3]
    catalog["current_registry_sha256"] = new_hash
    return {
        CONFIG / FILES["registry"]: dump(registry),
        CONFIG / FILES["lock"]: dump(lock),
        CONFIG / FILES["additions"]: dump(additions),
        CONFIG / FILES["additions_lock"]: canonical_json(additions_lock(additions, registry)),
        CATALOG: dump(catalog),
        CATALOG_LOCK: dump({"catalog_sha256": canonical_hash(catalog)}),
    }


def repin(new_hash: str, before: dict[Path, bytes]) -> list[str]:
    """Move every pin of revision 3 to revision 4, re-lock what changed and catalogue each old plan as a predecessor; return the changed plan names."""
    replace_pin(CONFIG, REV3, new_hash)
    changed = relock_to_fixed_point(REPO_ROOT)
    legacy_catalog(REPO_ROOT, changed, before, PLANS_ARCHIVE)
    relock_policies(CONFIG)
    return sorted(base.name for base, _, _ in changed.values())


def pending_pins() -> list[str]:
    """The plans and records that still pin revision 3."""
    return [path.name for path in pin_files(CONFIG) if REV3 in path.read_text(encoding="utf-8")]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", help="write the files; without it, report which would change")
    args = parser.parse_args()
    if not args.write and not REV3_ARCHIVE.is_dir():
        sys.stdout.write("the revision 3 archive does not exist yet; run with --write once\n")
        return 1
    current = canonical_hash(json.loads((CONFIG / FILES["registry"]).read_text(encoding="utf-8")))
    if args.write and current == REV3:
        archive_revision_3()
    outputs = build()
    new_hash = canonical_hash(json.loads(outputs[CONFIG / FILES["registry"]]))
    changed = [path for path, text in outputs.items() if not path.exists() or path.read_text(encoding="utf-8") != text]
    pins = pending_pins()
    for path in changed:
        sys.stdout.write(f"{'wrote' if args.write else 'would change'} {path.relative_to(REPO_ROOT)}\n")
    for name in pins:
        sys.stdout.write(f"{'re-pinning' if args.write else 'would re-pin'} {name}\n")
    if args.write:
        require(current in (REV3, new_hash), "the tracked registry is neither revision 3 nor revision 4")
        before = {path: path.read_bytes() for path in CONFIG.rglob("*.json")}
        for path in changed:
            path.write_text(outputs[path], encoding="utf-8")
        if pins:
            sys.stdout.write(f"locks changed: {repin(new_hash, before)}\n")
        loaded = load_registry()
        require(canonical_hash(load_registry(expected_sha256=REV3)) == REV3, "revision 3 no longer resolves")
        sys.stdout.write(f"registry revision {loaded['registry_revision']} {canonical_hash(loaded)[:12]}; revision 3 resolves\n")
    elif not changed and not pins:
        sys.stdout.write("no file changes\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
