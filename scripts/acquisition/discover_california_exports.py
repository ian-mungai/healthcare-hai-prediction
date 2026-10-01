"""Select full CSV exports explicitly linked by publisher resource pages."""

import argparse
import sys
from pathlib import Path

from scripts.acquisition.discover_history import index
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    args = parser.parse_args()
    candidates = []
    for original in read_json(args.candidates)["candidates"]:
        if "/download/" not in original["url"]:
            continue
        url = original["url"].split("/download/")[0]
        links, evidence = index(url, args.state_root)
        for item in links:
            if item["label"] == "CSV" and item["url"].startswith("https://data.chhs.ca.gov/datastore/dump/"):
                candidates.append(
                    {
                        **original,
                        "url": item["url"],
                        "format": "csv",
                        "route_type": "permitted_export",
                        "max_bytes": 256 * 1024**2,
                        "evidence": {"publisher_index": url, "index_sha256": canonical_hash(evidence), "original": original["evidence"]},
                    }
                )
    write_once(args.state_root / "candidates.json", encoded_json({"candidates": candidates}))
    sys.stdout.write(str(f"Publisher-listed complete CSV alternatives: {len(candidates)}") + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
