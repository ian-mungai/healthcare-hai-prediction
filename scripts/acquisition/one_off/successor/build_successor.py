"""Build the current-state successors of the locked acquisition plans and records (failure modes S1 to S5, S10, S15).

Each original is copied byte for byte into the private archive once; every run rebuilds the successors from those
archived bytes, so a rerun writes identical files. Only decision wording in string values changes; plan chains, the
HUD 2014 Q2 repeat decision and the locks are rebound to the successors. The catalog lists each archived predecessor
so captures that recorded it keep verifying. Run from the repository root:

    .venv/bin/python -m scripts.acquisition.one_off.successor.build_successor          # check: no file changes
    .venv/bin/python -m scripts.acquisition.one_off.successor.build_successor --write
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.legacy_versions import ARCHIVE, CATALOG_LOCK_PATH, CATALOG_PATH
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, require
from scripts.process import run_command

CONFIG = REPO_ROOT / "config/acquisition"
PLANS = [
    "census_acs_api_plan.json",
    "census_acs_api_plan_dp02pr.json",
    "census_acs_detailed_plan.json",
    "cms_owners_plan.json",
    "hcai_util_2018_2025_plan.json",
    "hud_api_plan.json",
    "hud_api_plan_2021q2_2025q4.json",
    "hud_xlsx_plan_2010q1_2020q4.json",
    "onc_mu_hospital_plan.json",
    "wonder_export_plan.json",
]
# Plan 2 names plan 1's hash; the successor pair is rebound together (failure mode S4).
CHAINS = {"census_acs_api_plan_dp02pr.json": "census_acs_api_plan.json", "hud_api_plan_2021q2_2025q4.json": "hud_api_plan.json"}
DECISION = "hud_xlsx_exact_repeats_2014q2.json"
DECISION_PLAN = "hud_xlsx_plan_2010q1_2020q4.json"
RECORDS = ["access_releases_20260929.json", "access_releases_20261002.json", "manifest_corrections.json", "reference_download_requests.json"]
# The dataset move rewrote these plans' paths; captures made before it record the plan as committed at 3af1abb.
PRE_MOVE_REVISION = "3af1abb"
PRE_MOVE = ["bls_api_plan.json", "census_acs_api_plan.json", "census_acs_api_plan_dp02pr.json", "census_acs_detailed_plan.json"]
CODE_VERSIONS = sorted(path.name for path in CONFIG.glob("*_code_versions.json"))
INDENT = {"access_releases_20260929.json": 1, "access_releases_20261002.json": 1, "reference_download_requests.json": 1}
DATE = r"(?:\d{4}-\d{2}-\d{2}|[A-Z][a-z]+ \d{1,2},? 2026)"
WHO = r"(?:[Uu]ser|[Oo]wner)"


def neutral(text: str, dated_entries: bool) -> str:
    """Drop who decided and when; keep what the rule is."""
    value = re.sub(rf"^{WHO} decisions? {DATE} \((.*)\)\.$", r"\1.", text)
    value = re.sub(rf"^{WHO} (?:decisions?|approvals?|proceed),? (?:on )?{DATE}[:;]\s*", "", value)
    value = re.sub(rf"^{WHO} decision, Oct \d+ 2026: ", "", value)
    value = re.sub(rf"\s?\({WHO} decisions?,? (?:on )?{DATE}\)", "", value)
    value = re.sub(rf"\s?\({WHO} decisions?\)", "", value)
    value = re.sub(rf"\s{WHO} decisions? {DATE}\.", "", value)
    value = value.replace("Dated user decisions that release", "Records that release").replace(" by owner decision", "")
    value = value.replace(", as in the 2026-09-29 record;", ", as in the first release record;")
    if dated_entries:
        value = re.sub(r"^\d{4}-\d{2}-\d{2}: ", "", value)
    return value[:1].upper() + value[1:] if value != text and value else value


def transform(value: Any, dated_entries: bool) -> Any:
    if isinstance(value, dict):
        return {key: transform(item, dated_entries) for key, item in value.items()}
    if isinstance(value, list):
        return [transform(item, dated_entries) for item in value]
    return neutral(value, dated_entries) if isinstance(value, str) else value


def leaves(value: Any, path: str = "$") -> dict[str, Any]:
    if isinstance(value, dict):
        return {k: v for key, item in value.items() for k, v in leaves(item, f"{path}.{key}").items()}
    if isinstance(value, list):
        return {k: v for index, item in enumerate(value) for k, v in leaves(item, f"{path}[{index}]").items()}
    return {path: value}


def encode(value: Any, name: str) -> bytes:
    return (json.dumps(value, indent=INDENT.get(name, 2), ensure_ascii=True) + "\n").encode()


def original(name: str) -> bytes:
    """The archived original; on the first run the tracked file is archived first (failure mode S15)."""
    archived = ARCHIVE / "config/acquisition" / name
    if archived.is_file():
        return archived.read_bytes()
    return (CONFIG / name).read_bytes()


def pre_move(name: str) -> bytes:
    """The plan as committed before the dataset move, byte for byte (archived on the first run)."""
    archived = ARCHIVE / f"config/acquisition/pre_move_{PRE_MOVE_REVISION}" / name
    if archived.is_file():
        return archived.read_bytes()
    result = run_command("git", ["show", f"{PRE_MOVE_REVISION}:config/acquisition/{name}"], cwd=REPO_ROOT, check=True)
    return result.stdout.encode()


def build() -> tuple[dict[str, bytes], dict[str, bytes], dict]:
    """Return successor files, originals to archive and the catalog."""
    names = PLANS + [DECISION] + RECORDS + CODE_VERSIONS
    sources = {name: original(name) for name in names}
    sources |= {Path(name).with_suffix(".lock.json").name: original(Path(name).with_suffix(".lock.json").name) for name in PLANS + [DECISION]}
    parsed = {name: json.loads(sources[name]) for name in names}
    successors = {name: transform(parsed[name], dated_entries=name.startswith("access_releases")) for name in names}
    for plan_two, plan_one in CHAINS.items():
        successors[plan_two]["supplements_plan_sha256"] = canonical_hash(successors[plan_one])
    successors[DECISION]["plan_sha256"] = canonical_hash(successors[DECISION_PLAN])
    # The HCAI plan binds its access-release record by file SHA-256.
    release = successors["hcai_util_2018_2025_plan.json"]["access_release"]
    release["sha256"] = hashlib.sha256(encode(successors[Path(release["path"]).name], Path(release["path"]).name)).hexdigest()
    for name in CODE_VERSIONS:
        # Versions listed after the archive was taken are kept as they are; only the archived ones get current wording.
        listed = json.loads((CONFIG / name).read_bytes())["versions"]
        successors[name]["versions"].extend(listed[len(parsed[name]["versions"]) :])
    for name in names:
        # Only string wording and the rebound hashes may differ (failure mode S1).
        before = leaves(parsed[name])
        after = {key: value for key, value in leaves(successors[name]).items() if key in before or not name.endswith("_code_versions.json")}
        require(before.keys() == after.keys(), f"{name}: structure changed")
        for path in before:
            if before[path] != after[path]:
                rebound = path in ("$.supplements_plan_sha256", "$.plan_sha256", "$.access_release.sha256")
                require(
                    isinstance(before[path], str) and (rebound or after[path] == neutral(before[path], name.startswith("access_releases"))), f"{name}: {path}"
                )
    files = {name: encode(successors[name], name) for name in names}
    for name in PLANS:
        files[Path(name).with_suffix(".lock.json").name] = encode({"plan_sha256": canonical_hash(successors[name])}, "lock")
    files[Path(DECISION).with_suffix(".lock.json").name] = encode({"decision_sha256": canonical_hash(successors[DECISION])}, "lock")
    changed = {name for name in files if files[name] != sources[name]}
    archive = {name: sources[name] for name in sorted(changed)}

    def entry(name: str) -> dict:
        body = sources[name]
        return {
            "archive_path": f"{ARCHIVE.relative_to(REPO_ROOT)}/config/acquisition/{name}",
            "sha256": hashlib.sha256(body).hexdigest(),
            "canonical_sha256": canonical_hash(json.loads(body)),
        }

    catalog: dict[str, Any] = {
        "catalog_version": 1,
        "archive_policy": "Exact earlier plans and records are local-only and owner-readable; captures that recorded them need them. Never publish.",
        "plans": {canonical_hash(successors[name]): [entry(name)] for name in PLANS if name in changed},
        "records": {f"config/acquisition/{name}": [entry(name)] for name in [DECISION, *RECORDS, *CODE_VERSIONS] if name in changed},
    }
    for name in PRE_MOVE:
        body = pre_move(name)
        current = canonical_hash(successors[name]) if name in successors else canonical_hash(json.loads((CONFIG / name).read_bytes()))
        catalog["plans"].setdefault(current, []).append(
            {
                "archive_path": f"{ARCHIVE.relative_to(REPO_ROOT)}/config/acquisition/pre_move_{PRE_MOVE_REVISION}/{name}",
                "sha256": hashlib.sha256(body).hexdigest(),
                "canonical_sha256": canonical_hash(json.loads(body)),
            }
        )
        archive[f"pre_move_{PRE_MOVE_REVISION}/{name}"] = body
    files[CATALOG_PATH.name] = encode(catalog, CATALOG_PATH.name)
    files[CATALOG_LOCK_PATH.name] = encode({"catalog_sha256": canonical_hash(catalog)}, "lock")
    return files, archive, catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="archive the originals and write the successors")
    args = parser.parse_args()
    files, archive, catalog = build()
    pending = sorted(name for name, body in files.items() if not (CONFIG / name).is_file() or (CONFIG / name).read_bytes() != body)
    if args.write:
        target = ARCHIVE / "config/acquisition"
        (target / f"pre_move_{PRE_MOVE_REVISION}").mkdir(parents=True, exist_ok=True)
        for folder in (ARCHIVE, ARCHIVE / "config", target, target / f"pre_move_{PRE_MOVE_REVISION}"):
            folder.chmod(0o700)
        for name, body in archive.items():
            path = target / name
            if path.exists():
                require(path.read_bytes() == body, f"Archived {name} differs; refusing to overwrite")
            else:
                path.write_bytes(body)
                path.chmod(0o600)
        for name in pending:
            (CONFIG / name).write_bytes(files[name])
    summary = {"archived": len(archive), "plans": len(catalog["plans"]), "records": len(catalog["records"]), "pending": [] if args.write else pending}
    sys.stdout.write(json.dumps(summary, indent=1) + "\n")


if __name__ == "__main__":
    main()
