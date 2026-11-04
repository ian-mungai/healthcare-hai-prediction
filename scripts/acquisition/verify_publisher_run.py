"""The owner's read-only check after a publisher task. It recomputes every outcome from S3 by version and names any claim whose capture was not made inside
the task's window, by the store's own time and by the receipt's.

    .venv/bin/python -m scripts.acquisition.verify_publisher_run --queue <queue.json> --state-key <key> --state-version <id> --state-sha256 <digest> \\
        --since <start of the task, ISO 8601> --until <end of the task, ISO 8601> [--prior-state-key <k> --prior-state-version <id> --prior-state-sha256 <d>]

A replacement task inherits the approved outcomes of the task before it. Name that task's saved state as the prior state: an outcome with the same record in
both states is inherited. It is recomputed from S3 like the rest, but its window was checked when the owner verified the prior state, so only the new
outcomes are held to this task's window. Without a prior state every outcome is held to this window.

Exit 0 means every claim holds. Exit 1 lists the unit IDs that do not. The task's writer could still make a capture of old bytes during the window: this
narrows that limit and does not remove it.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import tarfile
import tempfile
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from scripts.acquisition import publisher_run, restored_outcomes
from scripts.acquisition import redownload as harness
from scripts.acquisition.s3_store import AwsCli
from scripts.acquisition.source_registry import REPO_ROOT, RegistryError


def retrieved_at(receipt: dict) -> datetime:
    return datetime.fromisoformat(receipt["acquisition"]["retrieved_at_utc"])


def stored_at(client: Any, item: dict) -> datetime:
    """The store's own time for one version. A writer cannot set it."""
    moment = client.call("s3api", "head-object", ["--key", item["key"], "--version-id", item["version_id"]])["LastModified"]
    return moment if isinstance(moment, datetime) else datetime.fromisoformat(str(moment))


def read_records(body: bytes) -> dict[str, bytes] | None:
    """The records of one saved state by name, or None when it holds anything that is not a record."""
    try:
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as saved:
            if not publisher_run.records_only(saved):
                return None
            return {member.name: reader.read() for member in saved.getmembers() if (reader := saved.extractfile(member)) is not None}
    except (tarfile.TarError, OSError):
        return None


def check_state(run: Any, body: bytes, since: datetime, until: datetime, client: Any, prior: bytes | None = None) -> dict:
    """Check every outcome in one saved state and report by unit ID. No unit stops the check. An outcome that the prior state holds unchanged is inherited."""
    units = restored_outcomes.active_units(run)
    report: dict[str, Any] = {"outcomes": 0, "verified": 0, "not_verified": [], "outside_window": []}
    records = read_records(body)
    if records is None:
        report["not_verified"].append("saved state")
        return report
    earlier = read_records(prior) if prior is not None else {}
    if prior is not None:
        report["inherited"] = []
    if earlier is None:
        report["not_verified"].append("prior state")
        earlier = {}
    outcomes: dict[str, dict] = {}
    for name, body in records.items():
        if not name.startswith("outcomes/"):
            continue
        identity = Path(name).stem
        report["outcomes"] += 1
        try:
            outcomes[identity] = json.loads(body)
            receipt = restored_outcomes.check_record(identity, outcomes[identity], units, run, client)
            if receipt is None:
                continue
            times = (stored_at(client, outcomes[identity]["stored"]["receipt"]), retrieved_at(receipt))
        except (RegistryError, KeyError, ValueError, TypeError, OSError):
            report["not_verified"].append(identity)
            continue
        report["verified"] += 1
        if earlier.get(name) == body:
            report["inherited"].append(identity)
        elif not all(since <= moment <= until for moment in times):
            report["outside_window"].append(identity)
    try:
        restored_outcomes.check_ledgers({name: body for name, body in records.items() if name.startswith((".budgets/", ".attempts/"))}, outcomes, units)
    except RegistryError:
        report["not_verified"].append("saved state")
    return report


def fetch_state(client: Any, scratch: str, key: str, version: str, digest: str) -> bytes:
    """One saved state by key and version, refused when its digest differs from the one the owner named."""
    path = Path(scratch) / f"{digest}.tar.gz"
    client.call("s3api", "get-object", ["--key", key, "--version-id", version, str(path)])
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != digest:
        raise SystemExit("The saved state differs from the digest you named.")
    return body


def main(argv: Sequence[str] | None = None, client: Any = None, base: Path = REPO_ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for name in ("queue", "state-key", "state-version", "state-sha256", "since", "until"):
        parser.add_argument(f"--{name}", required=True)
    for name in ("key", "version", "sha256"):
        parser.add_argument(f"--prior-state-{name}")
    args = parser.parse_args(argv)
    prior = [args.prior_state_key, args.prior_state_version, args.prior_state_sha256]
    if any(prior) and not all(prior):
        parser.error("Name the prior state by its key, version and digest together.")
    if client is None:
        from scripts.infrastructure.render_project_config import load_configuration

        client = AwsCli(load_configuration(REPO_ROOT / ".env")[0])
    with tempfile.TemporaryDirectory() as scratch:
        body = fetch_state(client, scratch, args.state_key, args.state_version, args.state_sha256)
        earlier = fetch_state(client, scratch, *prior) if all(prior) else None
        run = harness.Run(Path(args.queue), Path(scratch) / "unused", base=base, task_storage=client)
        harness.begin(run)
        report = check_state(run, body, datetime.fromisoformat(args.since), datetime.fromisoformat(args.until), client, earlier)
    sys.stdout.write(json.dumps(report, indent=2) + "\n")
    return 1 if report["not_verified"] or report["outside_window"] else 0


if __name__ == "__main__":
    sys.exit(main())
