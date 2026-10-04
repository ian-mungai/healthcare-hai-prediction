"""Profile archive members nested more than one level deep, which the member review did not open.

For the selected sources, every archive is opened recursively (up to four levels, in memory). Members at the second
level or deeper are profiled with the member review's own reader, so the output has the same shape: header names
that pass the contact filter, category counts and identifier shapes, never cell values.

Usage (the interpreter needs openpyxl, pypdf and xlrd)::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.inspect_deep_archives --source-root SOURCE_ROOT \
        --inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output deep_run1.json
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import zipfile
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.review.inspect_hai_archives import is_resource_fork, sha256
from scripts.review.inspect_members import profile_member
from scripts.review.review_remaining import digest, safe_path, write_json

SOURCES = ("CMS_OCCMIX", "main-cmi-ipps", "CMS_IPPS")
MAX_DEPTH = 4
MEMBER_LIMIT = 512 << 20


def walk(source: str, label: str, data: bytes, depth: int, members: dict[str, dict[str, Any]], findings: Counter[str]) -> None:
    """Recurse into an archive; profile members inside archives nested two or more levels deep."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for info in sorted((i for i in archive.infolist() if not i.is_dir()), key=lambda i: i.filename):
            path = PurePosixPath(info.filename)
            if is_resource_fork(info.filename) or path.is_absolute() or ".." in path.parts:
                continue
            if info.file_size > MEMBER_LIMIT:
                findings["member_over_size_limit"] += 1
                continue
            content = archive.read(info)
            location = f"{label}!{info.filename}"
            if path.suffix.lower() == ".zip":
                if depth + 1 >= MAX_DEPTH:
                    findings["deeper_than_limit"] += 1
                else:
                    walk(source, location, content, depth + 1, members, findings)
                continue
            if depth < 2:
                continue  # the member review already profiled the top archive and its first nested archive
            member_sha = sha256(content)
            entry = members.setdefault(member_sha, {"bytes": len(content), "nesting_level": depth, "locations": [], "profile": None})
            entry["locations"].append({"source_id": source, "location": location})
            if entry["profile"] is None:
                entry["profile"] = profile_member(info.filename, content, source)


def main() -> int:
    """Run the deep-archive profile from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.source_root.resolve()
    members: dict[str, dict[str, Any]] = {}
    findings: Counter[str] = Counter()
    for candidate in sorted(json.loads(args.inventory.read_text())["candidates"], key=lambda c: (c["source_id"], c["snapshot_id"])):
        if candidate["source_id"] not in SOURCES or candidate["status"] != "candidate_not_authorized_for_execution":
            continue
        receipt = safe_path(root, candidate["receipt"])
        for artifact in json.loads(receipt.read_text())["artifacts"]:
            path = receipt.parent / artifact["storage_path"]
            if not artifact["stored_file_name"].lower().endswith(".zip"):
                continue
            if not path.is_file() or digest(path) != artifact["sha256"]:
                findings["artifact_not_local_or_changed"] += 1
                continue
            walk(candidate["source_id"], artifact["stored_file_name"], path.read_bytes(), 0, members, findings)
    for entry in members.values():
        entry["locations"] = sorted(entry["locations"], key=lambda loc: (loc["source_id"], loc["location"]))
    status = Counter(f"{loc['source_id']}:{m['profile']['status']}" for m in members.values() for loc in m["locations"][:1])
    write_json(
        args.output,
        {
            "version": 1,
            "code_sha256": digest(Path(__file__)),
            "inventory_sha256": digest(args.inventory),
            "findings": dict(sorted(findings.items())),
            "member_status": dict(sorted(status.items())),
            "members": dict(sorted(members.items())),
            "model_eligible": False,
        },
    )
    sys.stdout.write(json.dumps({"deep_members": len(members), "status": dict(sorted(status.items()))}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
