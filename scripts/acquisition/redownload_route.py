"""Offline gate for run 3 on the AWS route: removal first, the raw root, a locked queue and no removed file left in the queue.

The gate reads local records only. It makes no AWS call and starts no download.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.parse
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from scripts.acquisition.collection_layout import RAW_ROOT, capture_root
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.transport import SIGNED_REDIRECT_ROUTES, CaptureError, hosts_digest
from scripts.acquisition.unused_files import approved_plan

ACTIVE = ("api", "direct_download", "approved_manual")


@dataclass(frozen=True)
class RouteFiles:
    """The records run 3 needs on the AWS route: the approved removal plan, its record and the approved hosts file."""

    plan: Path
    approval: Path
    record: Path
    hosts: Path
    hosts_approval: Path


def record_problems(record_path: Path, removal: dict | None) -> list[str]:
    """Return why the removal record does not prove that every planned file got its delete marker."""
    if not record_path.is_file():
        return ["the removal record is missing, so removal has not run"]
    if removal is None:
        return []
    try:
        record = json.loads(record_path.read_text(encoding="utf-8"))
        if record.get("plan_sha256") != removal["plan_sha256"]:
            return ["the removal record belongs to another plan"]
        if record.get("bucket") != removal["bucket"]:
            return ["the removal record names another bucket than the plan"]
        planned = {item["s3_key"]: item["version_id"] for item in removal["items"]}
        recorded = [(item["s3_key"], item["version_id"], item["delete_marker_version_id"]) for item in record["items"]]
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        return [f"the removal record is unusable: {error}"]
    complete = (
        len(recorded) == len({key for key, _, _ in recorded})
        and {key: version for key, version, _ in recorded} == planned
        and all(isinstance(marker, str) and marker for _, _, marker in recorded)
    )
    return [] if complete else ["the removal record does not show one delete marker for every planned file"]


def queue_hosts(queue: dict) -> set[str]:
    """Return the hosts of the active queue units, plus the host of each reviewed signed redirect."""
    hosts: set[str] = set()
    for unit in queue["queue"]:
        if unit["disposition"] in ACTIVE:
            hosts.add(str(urllib.parse.urlsplit(unit["url"]).hostname))
            if unit["url"] in SIGNED_REDIRECT_ROUTES:
                hosts.add(str(urllib.parse.urlsplit(SIGNED_REDIRECT_ROUTES[unit["url"]]).hostname))
    return hosts


def derive_hosts(queue_path: Path, output: Path) -> dict:
    """Write the hosts file the owner reviews and approves. It lists the hosts the queue needs and binds the queue by SHA-256."""
    body = queue_path.read_bytes()
    hosts = sorted(queue_hosts(json.loads(body)))
    document = {"hosts": hosts, "hosts_sha256": hosts_digest(hosts), "queue_sha256": hashlib.sha256(body).hexdigest()}
    write_once(output, encoded_json(document))
    return document


def host_problems(queue_body: bytes, hosts_path: Path, approval_path: Path) -> list[str]:
    """Return why the approved hosts file cannot govern this queue. Any failure in the file is a problem, never a pass."""
    try:
        document = json.loads(hosts_path.read_text(encoding="utf-8"))
        hosts = document["hosts"]
        if document["hosts_sha256"] != hosts_digest(hosts):
            return ["the hosts file differs from its digest"]
        if document["queue_sha256"] != hashlib.sha256(queue_body).hexdigest():
            return ["the hosts file belongs to another queue"]
    except (OSError, ValueError, KeyError, TypeError) as error:
        return [f"the hosts file is unusable: {error}"]
    problems: list[str] = []
    try:
        approval = json.loads(approval_path.read_text(encoding="utf-8"))
        approved = approval.get("hosts_sha256") == document["hosts_sha256"] and bool(approval.get("approved_by"))
    except (OSError, ValueError, AttributeError):
        approved = False
    if not approved:
        problems.append("the hosts approval is missing or names another hosts file")
    problems.extend(f"{host} is in the queue but not on the approved hosts file" for host in sorted(queue_hosts(json.loads(queue_body)) - set(hosts)))
    return problems


def check(queue_path: Path, plan_path: Path, approval_path: Path, record_path: Path, root: str, hosts_path: Path, hosts_approval_path: Path) -> list[str]:
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
    problems.extend(record_problems(record_path, removal))
    problems.extend(host_problems(queue_body, hosts_path, hosts_approval_path))
    for unit in json.loads(queue_body)["queue"]:
        if unit["snapshot_id"] in removed and unit["disposition"] in ACTIVE:
            problems.append(f"{unit['snapshot_id']} is in the queue but the plan removes it")
    return problems


def main(argv: Sequence[str] | None = None, root: str | None = None) -> None:
    parser = argparse.ArgumentParser(description="Derive the hosts file and check that run 3 may start on the AWS route.")
    commands = parser.add_subparsers(dest="command", required=True)
    deriving = commands.add_parser("hosts")
    deriving.add_argument("--queue", required=True, type=Path)
    deriving.add_argument("--output", required=True, type=Path)
    checking = commands.add_parser("check")
    for name in ("queue", "plan", "approval", "record", "hosts", "hosts-approval"):
        checking.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "hosts":
            document = derive_hosts(args.queue, args.output)
            sys.stdout.write(json.dumps({"hosts": len(document["hosts"]), "hosts_sha256": document["hosts_sha256"]}) + "\n")
            return
        found = check(args.queue, args.plan, args.approval, args.record, capture_root() if root is None else root, args.hosts, args.hosts_approval)
    except (OSError, KeyError, ValueError) as error:
        parser.error(str(error))
    if found:
        sys.stderr.write("\n".join(found) + "\n")
        raise SystemExit(1)
    sys.stdout.write(json.dumps({"route": "ready", "problems": 0}) + "\n")


if __name__ == "__main__":
    main()
