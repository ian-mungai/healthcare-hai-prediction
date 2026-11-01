"""The owner's read-only check after a publisher task. It recomputes every outcome from S3 by version with the project role and names any claim whose capture
was not made inside the task's window, by the store's own time and by the receipt's.

    .venv/bin/python -m scripts.acquisition.verify_publisher_run --queue <queue.json> --state-key <key> --state-version <id> --state-sha256 <digest> \\
        --since <start of the task, ISO 8601> --until <end of the task, ISO 8601>

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


def check_state(run: Any, body: bytes, since: datetime, until: datetime, client: Any) -> dict:
    """Check every outcome in one saved state and report by unit ID. No unit stops the check."""
    units = restored_outcomes.active_units(run)
    report: dict[str, Any] = {"outcomes": 0, "verified": 0, "not_verified": [], "outside_window": []}
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as saved:
        if not publisher_run.records_only(saved):
            report["not_verified"].append("saved state")
            return report
        records = {member.name: reader.read() for member in saved.getmembers() if (reader := saved.extractfile(member)) is not None}
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
        if not all(since <= moment <= until for moment in times):
            report["outside_window"].append(identity)
    try:
        restored_outcomes.check_ledgers({name: body for name, body in records.items() if name.startswith((".budgets/", ".attempts/"))}, outcomes, units)
    except RegistryError:
        report["not_verified"].append("saved state")
    return report


def main(argv: Sequence[str] | None = None, client: Any = None, base: Path = REPO_ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for name in ("queue", "state-key", "state-version", "state-sha256", "since", "until"):
        parser.add_argument(f"--{name}", required=True)
    args = parser.parse_args(argv)
    if client is None:
        from scripts.infrastructure.render_project_config import load_configuration

        client = AwsCli(load_configuration(REPO_ROOT / ".env")[0])
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / "state.tar.gz"
        client.call("s3api", "get-object", ["--key", args.state_key, "--version-id", args.state_version, str(path)])
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != args.state_sha256:
            parser.error("The saved state differs from the digest you named.")
        run = harness.Run(Path(args.queue), Path(scratch) / "unused", base=base, task_storage=client)
        harness.begin(run)
        report = check_state(run, body, datetime.fromisoformat(args.since), datetime.fromisoformat(args.until), client)
    sys.stdout.write(json.dumps(report, indent=2) + "\n")
    return 1 if report["not_verified"] or report["outside_window"] else 0


if __name__ == "__main__":
    sys.exit(main())
