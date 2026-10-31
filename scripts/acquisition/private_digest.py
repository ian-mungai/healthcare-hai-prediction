"""Owner script: record a digest, a size and a file count for each original that names people. It reads bytes only.

This tool never downloads, compares or records such a unit. The owner checks it locally: save the original under ``<folder>/<unit id>`` (one file or a
folder), run this script and compare the digest with the stored one by hand. The script parses nothing, prints only counts and writes an owner-only file
keyed by the unit IDs of the public queue. It records and prints no file name, folder name or content, and a failure becomes a status for the unit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.acquisition.redownload_route import ACTIVE, is_private_unit

CHUNK = 1024 * 1024


class FixedParser(argparse.ArgumentParser):
    """An argument parser whose errors never echo what was typed."""

    def error(self, message: str) -> Any:
        sys.stderr.write("private_digest: the arguments are not valid\n")
        raise SystemExit(2)


def file_digest(path: str) -> tuple[str, int]:
    digest, size = hashlib.sha256(), 0
    with open(path, "rb") as handle:
        while chunk := handle.read(CHUNK):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def read_unit(root: Path) -> dict[str, Any]:
    """Hash every file under ``root`` and combine the digests in sorted order, so no name decides the result. Names are never kept."""
    if not os.path.lexists(root):
        return {"status": "missing"}
    digests: list[str] = []
    total = 0
    pending = [str(root)]
    try:
        if root.is_symlink():
            return {"status": "refused"}
        while pending:
            current = pending.pop()
            if os.path.isfile(current) and not os.path.islink(current):
                digest, size = file_digest(current)
                digests.append(digest)
                total += size
                continue
            with os.scandir(current) as entries:
                for entry in entries:
                    if entry.is_symlink():
                        return {"status": "refused"}
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        digest, size = file_digest(entry.path)
                        digests.append(digest)
                        total += size
                    else:
                        return {"status": "refused"}
    except OSError:
        return {"status": "unreadable"}
    return {"sha256": hashlib.sha256("\n".join(sorted(digests)).encode()).hexdigest(), "bytes": total, "files": len(digests), "status": "read"}


def main(argv: Sequence[str] | None = None) -> int:
    parser = FixedParser(description="Record a digest, a size and a file count for each private original. It reads bytes only.")
    parser.add_argument("--queue", required=True, type=Path)
    parser.add_argument("--folder", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.output.parent.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        sys.stderr.write("private_digest: the output folder is not owner-only\n")
        return 2
    body = args.queue.read_bytes()
    units = sorted(unit["snapshot_id"] for unit in json.loads(body)["queue"] if unit["disposition"] in ACTIVE and is_private_unit(unit))
    results: dict[str, dict[str, Any]] = {identity: read_unit(args.folder / identity) for identity in units}
    record = {"kind": "private_original_digests", "queue_sha256": hashlib.sha256(body).hexdigest(), "units": results}
    encoded = (json.dumps(record, sort_keys=True, indent=2) + "\n").encode()
    if args.output.exists():
        if args.output.read_bytes() != encoded:
            sys.stderr.write("private_digest: the output file exists with a different result and is not overwritten\n")
            return 3
    else:
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
    statuses = [item["status"] for item in results.values()]
    read, missing = statuses.count("read"), statuses.count("missing")
    unreadable = len(units) - read - missing
    sys.stdout.write(f"{len(units)} private units: {read} read, {missing} missing, {unreadable} unreadable\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
