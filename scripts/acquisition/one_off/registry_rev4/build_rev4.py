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
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.legacy_versions import ARCHIVE
from scripts.acquisition.registry_additions import additions_lock, canonical_json
from scripts.acquisition.repin_registry import LOCK_KEYS, catalog_bytes, digest, pin_files, relock_policies, relock_to_fixed_point, replace_pin
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, require

ROOT = REPO_ROOT
CONFIG = ROOT / "config/acquisition"
REV3_ARCHIVE = ARCHIVE / "registry_rev3"
# A test names a write step here to stop the run right after it, as an interruption would.
STOP_AFTER: str | None = None
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


class Interrupted(Exception):
    """The run was stopped after a named write step (a test's stand-in for an interruption)."""


def done(step: str) -> None:
    """Mark a write step as finished; stop here when a test asked for it."""
    if step == STOP_AFTER:
        raise Interrupted(step)


def write_atomic(path: Path, body: bytes) -> None:
    """Write a file so that an interruption leaves the old file or the whole new one, never a part."""
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    part.write_bytes(body)
    part.replace(path)


def archive_revision_3() -> None:
    """Make the revision 3 archive complete: copy each missing or different file from the tracked files while they still are revision 3."""
    tracked_registry = CONFIG / FILES["registry"]
    tracked_is_rev3 = canonical_hash(json.loads(tracked_registry.read_text(encoding="utf-8"))) == REV3
    for name in FILES.values():
        target = REV3_ARCHIVE / name
        if tracked_is_rev3:
            if not target.is_file() or target.read_bytes() != (CONFIG / name).read_bytes():
                write_atomic(target, (CONFIG / name).read_bytes())
        else:
            require(target.is_file(), f"the revision 3 archive lacks {name} and the tracked registry is no longer revision 3")
    archived()


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
        "registry_path": str((REV3_ARCHIVE / FILES["registry"]).relative_to(ROOT)),
        "lock_path": str((REV3_ARCHIVE / FILES["lock"]).relative_to(ROOT)),
        "files": {
            key: {"path": str((REV3_ARCHIVE / name).relative_to(ROOT)), "sha256": hashlib.sha256((REV3_ARCHIVE / name).read_bytes()).hexdigest()}
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


def lock_key(lock_path: Path) -> str | None:
    """The key of a plan or record lock, or None for any other lock."""
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    return next((key for key in LOCK_KEYS if set(lock) == {key}), None)


def earlier_versions(new_hash: str) -> dict[Path, tuple[bytes, str]]:
    """Each plan or record that pins revision 4, with its bytes and canonical hash from before the pin moved, found by moving the pin back."""
    found = {}
    for lock_path in sorted(CONFIG.glob("*.lock.json")):
        base = lock_path.with_name(lock_path.name.removesuffix(".lock.json") + ".json")
        text = base.read_text(encoding="utf-8") if lock_key(lock_path) and base.is_file() else ""
        if new_hash in text:
            earlier = text.replace(new_hash, REV3).encode()
            found[base] = (earlier, canonical_hash(json.loads(earlier)))
    return found


def archive_plans(earlier: dict[Path, tuple[bytes, str]]) -> None:
    """Make the archive of the old plans and records complete: write each missing or different file."""
    for base, (body, _) in earlier.items():
        target = ROOT / PLANS_ARCHIVE / base.name
        if not target.is_file() or target.read_bytes() != body:
            write_atomic(target, body)


def catalog_predecessors(earlier: dict[Path, tuple[bytes, str]]) -> None:
    """List each old plan or record as a predecessor of the new one, once, and write the catalog lock from the catalog."""
    path = CONFIG / "legacy_versions.json"
    catalog = json.loads(path.read_text(encoding="utf-8"))
    for base, (body, old) in earlier.items():
        entry = {"archive_path": f"{PLANS_ARCHIVE}/{base.name}", "sha256": digest(body), "canonical_sha256": old}
        fresh = canonical_hash(json.loads(base.read_text(encoding="utf-8")))
        if lock_key(base.with_name(base.name.removesuffix(".json") + ".lock.json")) == "plan_sha256":
            kept = [*catalog["plans"].pop(old, []), *catalog["plans"].get(fresh, [])]
            catalog["plans"][fresh] = [*kept, *([] if any(item["canonical_sha256"] == old for item in kept) else [entry])]
        else:
            records = catalog["records"].setdefault(base.relative_to(ROOT).as_posix(), [])
            records.extend([] if any(item["canonical_sha256"] == old for item in records) else [entry])
    write_atomic(path, catalog_bytes(catalog))
    write_atomic(CONFIG / "legacy_versions.lock.json", encoded_json({"catalog_sha256": canonical_hash(catalog)}))


def repin(new_hash: str, before: dict[Path, bytes]) -> list[str]:
    """Move every pin of revision 3 to revision 4, recompute every plan and record lock from its file and catalogue each old plan; safe to rerun."""
    replace_pin(CONFIG, REV3, new_hash)
    done("replace_pin")
    earlier = earlier_versions(new_hash)
    changed = relock_to_fixed_point(ROOT)
    done("relock")
    for old, (base, _, _) in changed.items():
        if base in earlier:
            require(
                earlier[base][1] == old, f"the earlier {base.name} rebuilt from the file does not match the lock it had; the file changed after the pin moved"
            )
        else:
            body = before[base]
            require(canonical_hash(json.loads(body)) == old, f"cannot rebuild the earlier {base.name} after an interrupted run")
            earlier[base] = (body, old)
    archive_plans(earlier)
    done("archive_plans")
    catalog_predecessors(earlier)
    done("legacy_catalog")
    relock_policies(CONFIG)
    done("policies")
    return sorted(base.name for base in earlier)


def stale_locks() -> list[str]:
    """The plans and records whose lock no longer matches the file it locks."""
    stale = []
    for lock_path in sorted(CONFIG.glob("*.lock.json")):
        key = lock_key(lock_path)
        base = lock_path.with_name(lock_path.name.removesuffix(".lock.json") + ".json")
        if key and base.is_file() and json.loads(lock_path.read_text(encoding="utf-8"))[key] != canonical_hash(json.loads(base.read_text(encoding="utf-8"))):
            stale.append(base.name)
    return stale


def pending_pins() -> list[str]:
    """The plans and records that still pin revision 3."""
    return [path.name for path in pin_files(CONFIG) if REV3 in path.read_text(encoding="utf-8")]


def run(write: bool) -> int:
    """Check, or with WRITE bring the repository to revision 4; every step derives its result from the files on disk, so a rerun finishes an interrupted run."""
    if not write and not REV3_ARCHIVE.is_dir():
        sys.stdout.write("the revision 3 archive does not exist yet; run with --write once\n")
        return 1
    current = canonical_hash(json.loads((CONFIG / FILES["registry"]).read_text(encoding="utf-8")))
    if write:
        archive_revision_3()
        done("archive")
    outputs = build()
    new_hash = canonical_hash(json.loads(outputs[CONFIG / FILES["registry"]]))
    changed = [path for path, text in outputs.items() if not path.exists() or path.read_text(encoding="utf-8") != text]
    pins, stale = pending_pins(), stale_locks()
    for path in changed:
        sys.stdout.write(f"{'wrote' if write else 'would change'} {path.relative_to(ROOT)}\n")
    for name in pins:
        sys.stdout.write(f"{'re-pinning' if write else 'would re-pin'} {name}\n")
    for name in stale:
        sys.stdout.write(f"{'re-locking' if write else 'would re-lock'} {name}\n")
    if write:
        require(current in (REV3, new_hash), "the tracked registry is neither revision 3 nor revision 4")
        before = {path: path.read_bytes() for path in CONFIG.rglob("*.json")}
        for path in changed:
            write_atomic(path, outputs[path].encode())
            done(f"write:{path.name}")
        sys.stdout.write(f"plans and records re-pinned: {repin(new_hash, before)}\n")
        loaded = load_registry(CONFIG / FILES["registry"], CONFIG / FILES["lock"])
        require(
            canonical_hash(load_registry(CONFIG / FILES["registry"], CONFIG / FILES["lock"], expected_sha256=REV3)) == REV3, "revision 3 no longer resolves"
        )
        sys.stdout.write(f"registry revision {loaded['registry_revision']} {canonical_hash(loaded)[:12]}; revision 3 resolves\n")
    elif not changed and not pins and not stale:
        sys.stdout.write("no file changes\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", help="write the files; without it, report which would change")
    return run(parser.parse_args().write)


if __name__ == "__main__":
    sys.exit(main())
