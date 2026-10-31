"""Check canonical supervisor files and run Linux source or baked-image smoke cases."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.orchestration.sdk_runtime_cases import ROOT, source_checks
from scripts.orchestration.sdk_standins import Scenario, WorkerStandin

BOOTSTRAP_SOURCES = {
    "supervisor.py": "services/hai/bootstrap/supervisor.py",
    "sdk_worker.py": "orchestration/sdk_worker.py",
    "worker_protocol.py": "orchestration/worker_protocol.py",
    "worker_stderr.py": "orchestration/worker_stderr.py",
    "sdk_worker.schema.json": "data_contracts/orchestration/sdk_worker.schema.json",
    "deadline_run.py": "services/hai/deadline_run.py",
}


def main() -> int:
    """Record explicit static or Linux runtime evidence without real AWS credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--bootstrap-folder", type=Path, default=Path("/opt/hai/bootstrap"))
    parser.add_argument("--source-smoke", action="store_true")
    parser.add_argument("--mode", choices=("static", "native"), default="native")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.mode == "native" and os.uname().sysname != "Linux":
        args.output.write_text(json.dumps({"status": "blocked", "reason": "requires Linux process-table evidence", "cleanup": "done"}, indent=2) + "\n")
        return 2
    if args.source_smoke:
        args.bootstrap_folder = Path(tempfile.mkdtemp(prefix="sdk_bootstrap_", dir=args.output.parent))
        (args.bootstrap_folder / "retention.json").write_text(
            json.dumps(
                {
                    "class": 3,
                    "purpose": "Canonical supervisor source smoke payload",
                    "created": datetime.now(UTC).isoformat(),
                    "end_condition": "The source smoke and its review are accepted",
                },
                indent=2,
            )
            + "\n"
        )
        for name, relative in BOOTSTRAP_SOURCES.items():
            (args.bootstrap_folder / name).write_bytes((ROOT / relative).read_bytes())
    hashes = {name: hashlib.sha256((args.bootstrap_folder / name).read_bytes()).hexdigest() for name in BOOTSTRAP_SOURCES}
    if args.mode == "static":
        checks = source_checks()
        report = {
            "classification": "Synthetic",
            "mode": "static",
            "source_layout": args.source_smoke,
            "baked_source_sha256": hashes,
            "checks": checks,
            "failed": [name for name, ok in checks.items() if not ok],
            "cleanup": "done",
            "limits": ["Source checks only", "Linux runtime and the real ARM64 image remain separate gates"],
        }
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        return int(bool(report["failed"]))
    spec = importlib.util.spec_from_file_location("hai_baked_supervisor_smoke", args.bootstrap_folder / "supervisor.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("baked_supervisor_loader_unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    rows: list[dict[str, Any]] = []
    for mode in ("normal", "drip_body"):
        with WorkerStandin(Scenario("describe_tasks", mode, b'{"tasks":[],"failures":[]}', lifetime_s=15, drip_interval_s=0.05)) as standin:
            supervisor = module.Supervisor("us-west-2", test_endpoint=standin.endpoint, stderr_folder=args.output.parent / f"stderr_{mode}")
            started = time.monotonic()
            try:
                deadline = started + (5 if mode == "normal" else 0.8)
                reply = supervisor.call("describe_tasks", {"cluster": "synthetic_cluster", "tasks": ["synthetic_task"], "include": ["TAGS"]}, deadline=deadline)
            finally:
                cleaned = supervisor.close(deadline=time.monotonic() + 5)
            checks = {
                "expected_reply": reply["ok"] if mode == "normal" else reply.get("error", {}).get("kind") == "deadline",
                "group_cleanup": cleaned,
                "reaped_handles": all(handle.state == "read_only" and handle.confirmed for handle in supervisor.handles),
                "stderr_readers_complete": all(handle.stderr_reader is not None and handle.stderr_reader.complete for handle in supervisor.handles),
            }
            rows.append(
                {
                    "mode": mode,
                    "checks": checks,
                    # The typed error of a failed reply and what the stand-in saw name the cause of a failed check; they hold no payload or secret.
                    "reply_error": reply.get("error"),
                    "elapsed_s": round(time.monotonic() - started, 3),
                    "stand_in_events": [
                        {"kind": event.kind, "connection": event.connection, "elapsed_s": round(event.elapsed_s, 3)} for event in standin.events
                    ],
                    "stderr": [handle.stderr_reader.snapshot() for handle in supervisor.handles],
                }
            )
            if not cleaned:
                break
    unchanged = hashes == {name: hashlib.sha256((args.bootstrap_folder / name).read_bytes()).hexdigest() for name in BOOTSTRAP_SOURCES}
    report = {
        "classification": "Synthetic",
        "mode": "native",
        "source_layout": args.source_smoke,
        "baked_source_sha256": hashes,
        "cases": rows,
        "failed": [row["mode"] for row in rows if not all(row["checks"].values())] + ([] if unchanged else ["sources_changed"]),
        "cleanup": "done" if all(row["checks"]["group_cleanup"] for row in rows) else "unconfirmed",
        "limits": ["Linux source smoke is not image acceptance" if args.source_smoke else "Compare hashes inside the real ARM64 image", "No cloud acceptance"],
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"report": str(args.output), "failed": report["failed"]}) + "\n")
    return int(bool(report["failed"]))


if __name__ == "__main__":
    sys.exit(main())
