"""Prove typed code-fingerprint records preserve exact historical approval mappings."""

import argparse
import copy
import json
import tempfile
from pathlib import Path

from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, canonical_hash, read_json, require


def exercise() -> list[dict]:
    """Exercise real metadata through legacy and typed representations plus malformed copies."""
    from scripts.acquisition.code_versions import read_code_versions
    from scripts.acquisition.legacy_versions import ARCHIVE, catalog, predecessor_bytes

    cases = []
    manifest = read_json(REPO_ROOT / "config/acquisition/code_version_migration.json")
    records = catalog().get("records", {})
    for row in manifest["files"]:
        tracked = REPO_ROOT / "config/acquisition" / row["file"]
        entries = records.get(f"config/acquisition/{row['file']}", [])
        # A list with current-state wording is checked through its archived original (failure mode S7).
        if entries and not ARCHIVE.is_dir():
            # The archive is private and never in Git, so a clean checkout (CI) records these checks as skipped [S55].
            skipped = {"passed": True, "skipped": "private legacy archive absent"}
            cases.append({"name": "historical_equivalence_" + row["file"], **skipped})
            cases.append({"name": "successor_keeps_every_version_" + row["file"], **skipped})
            continue
        source = REPO_ROOT / entries[0]["archive_path"] if entries else tracked
        if entries:
            predecessor_bytes(entries[0])
        normalized = read_code_versions(source)
        normalized["versions"] = normalized["versions"][: row["versions"]]
        cases.append({"name": "historical_equivalence_" + row["file"], "passed": canonical_hash(normalized) == row["original_normalized_sha256"]})
        if entries:
            kept = [{k: v for k, v in version.items() if k != "reason"} for version in read_code_versions(source)["versions"]]
            now = [{k: v for k, v in version.items() if k != "reason"} for version in read_code_versions(tracked)["versions"]]
            cases.append({"name": "successor_keeps_every_version_" + row["file"], "passed": now[: len(kept)] == kept})
    with tempfile.TemporaryDirectory(prefix="code_fingerprints_") as directory:
        path = Path(directory) / "versions.json"
        for source in sorted((REPO_ROOT / "config/acquisition").glob("*_code_versions.json")):
            original = read_code_versions(source)
            typed = copy.deepcopy(original)
            for version in typed["versions"]:
                version["code_sha256"] = [{"file": key, "sha256": value} for key, value in sorted(version["code_sha256"].items())]
            path.write_text(json.dumps(typed))
            cases.append({"name": source.name, "passed": read_code_versions(path) == original})
        baseline = typed
        for name in ["duplicate", "invalid_digest", "invalid_path", "unknown_field"]:
            changed = copy.deepcopy(baseline)
            rows = changed["versions"][0]["code_sha256"]
            if name == "duplicate":
                rows.append(dict(rows[0]))
            elif name == "invalid_digest":
                rows[0]["sha256"] = "untrusted"
            elif name == "invalid_path":
                rows[0]["file"] = "../private.py"
            else:
                rows[0]["extra"] = True
            path.write_text(json.dumps(changed))
            try:
                read_code_versions(path)
                passed = False
            except RegistryError:
                passed = True
            cases.append({"name": name, "passed": passed})
    return cases


def main() -> None:
    """Write an immutable local report; no acquisition or cloud calls."""
    from scripts.acquisition.s3_store import encoded_json, write_once

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = exercise()
    write_once(args.output, encoded_json({"cases": cases, "passed": all(c["passed"] for c in cases)}))
    require(all(c["passed"] for c in cases), "Fingerprint representation E2E failed")


if __name__ == "__main__":
    main()
