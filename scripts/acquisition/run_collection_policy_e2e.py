"""Check current exclusion policy independently from historical receipt validation."""

import argparse
from pathlib import Path

from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import RegistryError, require_collection_scope


def main() -> None:
    """Check excluded sources fail without any network or storage effect."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = []
    for name in ["AHRF", "AHA", "LEAP", "HASC", "NDNQI", "APIC", "S26_CLH"]:
        try:
            require_collection_scope(name)
            passed = False
        except RegistryError:
            passed = True
        cases.append({"source": name, "excluded": passed})
    require_collection_scope("HUD")
    require_collection_scope("ACS")
    write_once(args.output, encoded_json({"cases": cases, "passed": all(c["excluded"] for c in cases), "network": False}))
    if not all(c["excluded"] for c in cases):
        raise RegistryError("An excluded source remained collectible")


if __name__ == "__main__":
    main()
