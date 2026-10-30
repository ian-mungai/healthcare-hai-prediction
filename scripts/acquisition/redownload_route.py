"""Offline gate for run 3 on the AWS route: removal first, the raw root, a locked queue and no removed file left in the queue.

The gate reads local records only. It makes no AWS call and starts no download.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from scripts.acquisition.collection_layout import RAW_ROOT, capture_root
from scripts.acquisition.transport import CaptureError
from scripts.acquisition.unused_files import approved_plan

ACTIVE = ("api", "direct_download", "approved_manual")


def check(queue_path: Path, plan_path: Path, approval_path: Path, record_path: Path, root: str) -> list[str]:
    """Return every reason run 3 cannot start yet; an empty list means the route is ready."""
    problems: list[str] = []
    if root != RAW_ROOT:
        problems.append(f"the capture root must be {RAW_ROOT} on this route")
    queue_body = queue_path.read_bytes()
    lock = json.loads(queue_path.with_name("queue.lock.json").read_text(encoding="utf-8"))
    if hashlib.sha256(queue_body).hexdigest() != lock["queue_sha256"]:
        problems.append("the queue differs from its lock")
    removed: set[str] = set()
    try:
        removal = approved_plan(plan_path, approval_path)
        removed = {item["snapshot_id"] for item in removal["items"]}
    except (CaptureError, OSError, KeyError, ValueError) as error:
        problems.append(f"the approved removal plan is not usable: {error}")
        removal = None
    if not record_path.is_file():
        problems.append("the removal record is missing, so removal has not run")
    elif removal is not None and json.loads(record_path.read_text(encoding="utf-8")).get("plan_sha256") != removal["plan_sha256"]:
        problems.append("the removal record belongs to another plan")
    for unit in json.loads(queue_body)["queue"]:
        if unit["snapshot_id"] in removed and unit["disposition"] in ACTIVE:
            problems.append(f"{unit['snapshot_id']} is in the queue but the plan removes it")
    return problems


def main(argv: Sequence[str] | None = None, root: str | None = None) -> None:
    parser = argparse.ArgumentParser(description="Check that run 3 may start on the AWS route.")
    for name in ("queue", "plan", "approval", "record"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        found = check(args.queue, args.plan, args.approval, args.record, capture_root() if root is None else root)
    except (OSError, KeyError, ValueError) as error:
        parser.error(str(error))
    if found:
        sys.stderr.write("\n".join(found) + "\n")
        raise SystemExit(1)
    sys.stdout.write(json.dumps({"route": "ready", "problems": 0}) + "\n")


if __name__ == "__main__":
    main()
