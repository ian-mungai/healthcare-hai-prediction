"""Exercise registry version selection and rejection using disposable real configuration copies."""

import argparse
import copy
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from scripts.acquisition import source_registry as registry
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once


def exercise() -> list[dict]:
    """Read exact current and legacy versions, rejecting unknown hashes and modified legacy bytes."""
    results = []
    with tempfile.TemporaryDirectory(prefix="registry_replay_") as directory:
        root = Path(directory)
        current = registry.read_json(registry.REGISTRY_PATH)
        lock = registry.read_json(registry.LOCK_PATH)
        catalog = registry.read_json(registry.VERSION_CATALOG_PATH)
        legacy = catalog["legacy_versions"][0]
        paths = [registry.REGISTRY_PATH, registry.LOCK_PATH, registry.VERSION_CATALOG_PATH, registry.VERSION_CATALOG_LOCK_PATH]
        paths.extend(registry.REPO_ROOT / item["path"] for item in legacy["files"].values())
        for path in paths:
            target = root / path.relative_to(registry.REPO_ROOT)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(path.read_bytes())
        selected = root / registry.REGISTRY_PATH.relative_to(registry.REPO_ROOT)
        selected_lock = root / registry.LOCK_PATH.relative_to(registry.REPO_ROOT)
        catalog_path = root / registry.VERSION_CATALOG_PATH.relative_to(registry.REPO_ROOT)
        catalog_lock_path = root / registry.VERSION_CATALOG_LOCK_PATH.relative_to(registry.REPO_ROOT)
        with (
            patch.object(registry, "REPO_ROOT", root),
            patch.object(registry, "VERSION_CATALOG_PATH", catalog_path),
            patch.object(registry, "VERSION_CATALOG_LOCK_PATH", catalog_lock_path),
        ):
            loaded = registry.load_registry(selected, selected_lock)
            results.append({"name": "current_exact_lock", "passed": loaded == current and registry.canonical_hash(loaded) == lock["registry_sha256"]})
            historic = registry.load_registry(selected, selected_lock, expected_sha256=legacy["registry_sha256"])
            results.append({"name": "legacy_exact_hash", "passed": registry.canonical_hash(historic) == legacy["registry_sha256"]})
            for name, expected, mutate in [("unknown_hash", "0" * 64, False), ("modified_legacy", legacy["registry_sha256"], True)]:
                if mutate:
                    path = root / legacy["registry_path"]
                    changed = copy.deepcopy(historic)
                    changed["execution_status"] = "substituted"
                    path.write_bytes(encoded_json(changed))
                try:
                    registry.load_registry(selected, selected_lock, expected_sha256=expected)
                    passed = False
                except registry.RegistryError:
                    passed = True
                results.append({"name": name, "passed": passed})
            (root / legacy["registry_path"]).unlink()
            try:
                registry.load_registry(selected, selected_lock, expected_sha256=legacy["registry_sha256"])
                passed = False
            except registry.RegistryError:
                passed = True
            results.append({"name": "missing_legacy", "passed": passed})
    return results


def main() -> None:
    """Write repeatable verification evidence; never download, upload or change real inputs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = exercise()
    report = {"cases": cases, "passed": all(case["passed"] for case in cases), "runner_sha256": fingerprint(Path(__file__))[0], "network": False}
    write_once(args.output, encoded_json(report))
    if not report["passed"]:
        raise ValueError("Registry migration E2E failed")
    json.dump({"passed": len(cases), "artifact": str(args.output)}, __import__("sys").stdout)


if __name__ == "__main__":
    main()
