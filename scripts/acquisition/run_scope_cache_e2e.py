"""Prove cached exclusions stay bound to current immutable registry and lock bytes."""

import argparse
import tempfile
from pathlib import Path
from unittest.mock import patch

from scripts.acquisition import source_registry as registry
from scripts.acquisition.s3_store import encoded_json, write_once


def main() -> None:
    """Warm a real metadata cache, then reject altered registry or lock copies."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    registry._verified_exclusions.cache_clear()
    cases = []
    with tempfile.TemporaryDirectory(prefix="scope_cache_") as directory:
        path, lock = Path(directory) / "registry.json", Path(directory) / "lock.json"
        raw, locked = registry.REGISTRY_PATH.read_bytes(), registry.LOCK_PATH.read_bytes()
        path.write_bytes(raw)
        lock.write_bytes(locked)
        with patch.object(registry, "REGISTRY_PATH", path), patch.object(registry, "LOCK_PATH", lock):
            for name in ["cold_excluded", "warm_excluded", "registry_drift", "lock_drift"]:
                if name == "registry_drift":
                    value = registry.parse_json(raw, "fixture")
                    value["execution_status"] = "untrusted"
                    path.write_bytes(encoded_json(value))
                elif name == "lock_drift":
                    path.write_bytes(raw)
                    value = registry.parse_json(locked, "fixture")
                    value["registry_sha256"] = "0" * 64
                    lock.write_bytes(encoded_json(value))
                try:
                    registry.require_collection_scope("AHRF" if "excluded" in name else "HUD")
                    passed = False
                except registry.RegistryError:
                    passed = True
                cases.append({"name": name, "passed": passed})
            path.write_bytes(raw)
            lock.write_bytes(locked)
            registry.require_collection_scope("HUD")
    write_once(args.output, encoded_json({"cases": cases, "passed": all(c["passed"] for c in cases), "network": False}))
    registry.require(all(c["passed"] for c in cases), "Cached exclusion binding failed")


if __name__ == "__main__":
    main()
