"""One-time development step: pin the plan inputs that stay in S3 and list those that are neither tracked nor stored.

The stored index (key, version, digest and length of each stored object) and the plan's own digest must agree. The pins carry no bucket name.
Plan inputs the index does not hold, and the parked ones, are listed as pending with a reason. The host fetches and verifies each pin before a run.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def build(plan_digests: dict[str, str], index: dict[str, dict[str, Any]], candidates: list[str], reasons: dict[str, str]) -> bytes:
    """The remote pin document for CANDIDATES. A candidate the index lacks needs a reason in REASONS and becomes pending."""
    remote, pending = [], []
    for path in sorted(candidates):
        item = index.get(path)
        if item is None:
            pending.append({"path": path, "reason": reasons[path]})
            continue
        if item["sha256"] != plan_digests[path]:
            raise ValueError(f"The stored index disagrees with the plan digest for {path}")
        remote.append({"path": path, "sha256": item["sha256"], "bytes": item["byte_count"], "s3_key": item["key"], "s3_version_id": item["version_id"]})
    for path, reason in sorted(reasons.items()):
        if path not in candidates:
            pending.append({"path": path, "reason": reason})
    document = {"expected_plan_inputs": sorted(plan_digests), "remote": remote, "pending": sorted(pending, key=lambda item: item["path"])}
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def main() -> None:
    """Write remote_pins.json from the mirror's plan input digests, the stored index and the lists of candidates and pending inputs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan_inputs", type=Path, help="the mirror's plan_inputs.json (path to SHA-256)")
    parser.add_argument("stored_index", type=Path)
    parser.add_argument("candidates", type=Path, help="JSON list of plan input paths that stay in S3")
    parser.add_argument("reasons", type=Path, help="JSON object: path to the reason it is pending")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    document = build(
        json.loads(args.plan_inputs.read_text(encoding="utf-8")),
        json.loads(args.stored_index.read_text(encoding="utf-8"))["objects"],
        json.loads(args.candidates.read_text(encoding="utf-8")),
        json.loads(args.reasons.read_text(encoding="utf-8")),
    )
    args.output.write_bytes(document)
    summary = json.loads(document)
    sys.stdout.write(json.dumps({"remote": len(summary["remote"]), "pending": len(summary["pending"])}) + "\n")


if __name__ == "__main__":
    main()
