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
from scripts.acquisition.unused_files import read_plan

ACTIVE = ("api", "direct_download", "approved_manual")
# These collectors read an API key from the secret store. The AWS task holds no secret, so only the owner's Mac lane may run them.
SECRET_MODES = frozenset({"bls_api_v2", "census_acs_api", "census_acs_detailed", "hud_api"})
LANE_NAME = "mac_keyed_units"
PERSON_LANE = "mac_person_units"
PERSON_CLASSES = frozenset({"needs_person", "needs_owner_decision_private"})
PERSON_DEPARTURE = "owner decision 82 (run 3 on the AWS route) for the browser saves and the originals that name people, by owner decision 120"
LANE_DEPARTURE = "owner decision 82 (run 3 on the AWS route) for the units of the listed modes only"


@dataclass(frozen=True)
class RouteFiles:
    """The records run 3 needs on the AWS route: the removal plan, the owner's confirmation that every planned key was removed and the hosts file."""

    plan: Path
    confirmation: Path
    hosts: Path
    hosts_approval: Path


def lane_template(queue_body: bytes, lane: str = LANE_NAME) -> dict:
    """The approval the owner reviews and signs for a Mac lane: the queue, what the lane takes and how many units that is."""
    from scripts.acquisition import redownload

    queue = json.loads(queue_body)["queue"]
    active = [unit for unit in queue if unit["disposition"] in ACTIVE]
    queue_sha256 = hashlib.sha256(queue_body).hexdigest()
    if lane == PERSON_LANE:
        return {
            "lane": PERSON_LANE,
            "departs_from": PERSON_DEPARTURE,
            "queue_sha256": queue_sha256,
            "classes": sorted(PERSON_CLASSES),
            "units": sum(1 for unit in active if redownload.lane_of(unit) in PERSON_CLASSES),
            "originals_stay_local": True,
            "records": "digests, sizes and counts only",
            "approved_by": None,
        }
    return {
        "lane": LANE_NAME,
        "departs_from": LANE_DEPARTURE,
        "queue_sha256": queue_sha256,
        "modes": sorted(SECRET_MODES),
        "units": sum(1 for unit in active if unit["mode"] in SECRET_MODES),
        "keys_stay_off_aws": True,
        "approved_by": None,
    }


def write_lane_template(queue_path: Path, output: Path, lane: str = LANE_NAME) -> dict:
    document = lane_template(queue_path.read_bytes(), lane)
    write_once(output, encoded_json(document))
    return document


def lane_problems(queue_path: Path, approval_path: Path, lane: str = LANE_NAME) -> list[str]:
    """Return why the Mac lane may not start. Only an approval that names this lane, queue, modes and unit count lets it."""
    try:
        approval = json.loads(approval_path.read_text(encoding="utf-8"))
        expected = lane_template(queue_path.read_bytes(), lane)
    except (OSError, ValueError, KeyError, TypeError) as error:
        return [f"the lane approval is missing or unreadable: {error}"]
    if not isinstance(approval, dict):
        return ["the lane approval is not a record"]
    named = {key: approval.get(key) for key in expected if key != "approved_by"}
    if named != {key: value for key, value in expected.items() if key != "approved_by"}:
        return ["the lane approval does not name this lane, queue, modes and unit count"]
    if not (isinstance(approval.get("approved_by"), str) and approval["approved_by"].strip()):
        return ["the lane approval has no approver"]
    return []


def confirmation_problems(confirmation_path: Path, removal: dict) -> list[str]:
    """Return why the owner's confirmation does not show that every planned key was removed by hand."""
    if not confirmation_path.is_file():
        return ["the removal confirmation is missing, so the planned removal is not confirmed"]
    try:
        document = json.loads(confirmation_path.read_text(encoding="utf-8"))
        keys = document["keys"]
        if document["plan_sha256"] != removal["plan_sha256"]:
            return ["the removal confirmation belongs to another plan"]
        if document["bucket"] != removal["bucket"]:
            return ["the removal confirmation names another bucket than the plan"]
        if not (isinstance(keys, list) and all(isinstance(key, str) for key in keys)):
            return ["the removal confirmation is unusable: its keys are not a list of names"]
        confirmer, kind = document["confirmed_by"], document["kind"]
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        return [f"the removal confirmation is unusable: {error}"]
    problems: list[str] = []
    if kind != "unused_file_removal_confirmation":
        problems.append("the removal confirmation is unusable: it is not a removal confirmation")
    if not (isinstance(confirmer, str) and confirmer.strip()):
        problems.append("the removal confirmation has no confirming person")
    if len(keys) != len(set(keys)):
        problems.append("the removal confirmation repeats a key")
    planned = {item["s3_key"] for item in removal["items"]}
    missing = sorted(planned - set(keys))
    extra = sorted(set(keys) - planned)
    if missing:
        problems.append(f"the removal confirmation does not confirm {len(missing)} planned key{'s' if len(missing) != 1 else ''}, such as {missing[0]}")
    if extra:
        problems.append(f"the removal confirmation names {len(extra)} keys the plan does not hold, such as {extra[0]}")
    return problems


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


def check(queue_path: Path, plan_path: Path, confirmation_path: Path, root: str, hosts_path: Path, hosts_approval_path: Path) -> list[str]:
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
        removal = read_plan(plan_path)
        removed = {item["snapshot_id"] for item in removal["items"]}
        problems.extend(confirmation_problems(confirmation_path, removal))
    except (CaptureError, OSError, KeyError, ValueError, TypeError) as error:
        problems.append(f"the removal plan is not usable: {error}")
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
    templating = commands.add_parser("lane-template")
    templating.add_argument("--queue", required=True, type=Path)
    templating.add_argument("--output", required=True, type=Path)
    templating.add_argument("--lane", choices=[LANE_NAME, PERSON_LANE], default=LANE_NAME)
    checking = commands.add_parser("check")
    for name in ("queue", "plan", "confirmation", "hosts", "hosts-approval"):
        checking.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "hosts":
            document = derive_hosts(args.queue, args.output)
            sys.stdout.write(json.dumps({"hosts": len(document["hosts"]), "hosts_sha256": document["hosts_sha256"]}) + "\n")
            return
        if args.command == "lane-template":
            document = write_lane_template(args.queue, args.output, args.lane)
            sys.stdout.write(json.dumps({"units": document["units"], "approved_by": None}) + "\n")
            return
        found = check(args.queue, args.plan, args.confirmation, capture_root() if root is None else root, args.hosts, args.hosts_approval)
    except (OSError, KeyError, ValueError) as error:
        parser.error(str(error))
    if found:
        sys.stderr.write("\n".join(found) + "\n")
        raise SystemExit(1)
    sys.stdout.write(json.dumps({"route": "ready", "problems": 0}) + "\n")


if __name__ == "__main__":
    main()
