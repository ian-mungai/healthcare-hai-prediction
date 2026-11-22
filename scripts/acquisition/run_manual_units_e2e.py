"""Repeatable offline verification of the host's manual-unit handler, with a result file that names every case.

The cases are in ``tests/test_manual_units.py`` (the handler) and ``tests/test_manual_host_run.py`` (the host run: the seed release download and the capture
of manual units through the normal path). They use a stand-in publisher, a stand-in for the GitHub release, a create-once S3 stand-in and a clock that never
sleeps, so nothing reaches a network or AWS. One host-run case, the loopback trickle case, uses a real connection on the loopback address only: a local
server answers a release download with quick headers and then one byte every 0.1 s, below the socket timeout. Another case stalls the name lookup past the
deadline. Each download must stop at its absolute deadline, with no capture request before it.
Run: ``python -m scripts.acquisition.run_manual_units_e2e --output data/e2e/ingestion_manual_units/<name>``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition.source_registry import REPO_ROOT

CASES = tuple(Path(__file__).resolve().parent / "tests" / name for name in ("test_manual_units.py", "test_manual_host_run.py"))
MODULES = ("manual_units.py", "manual_content.py", "manual_receipt.py", "stale_record.py", "completion_record.py", "host_inputs.py", "capture_mode.py")


class Collector:
    """A pytest plugin that records the outcome of every case's call phase (setup and teardown failures count as errors)."""

    def __init__(self) -> None:
        self.outcomes: dict[str, str] = {}

    def pytest_runtest_logreport(self, report: Any) -> None:
        if report.when == "call" or (report.when != "call" and report.outcome != "passed"):
            self.outcomes[report.nodeid.rsplit("::", 1)[-1]] = report.outcome if report.when == "call" else "error"


def code_sha256() -> str:
    """One digest over the handler, its content checks, its record schema and the cases, so a result names the code it ran on."""
    digest = hashlib.sha256()
    for path in [*(CASES[0].parent.parent / name for name in MODULES), *CASES]:
        digest.update(path.name.encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=REPO_ROOT / "data/e2e/ingestion_manual_units" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    arguments = parser.parse_args()
    collector = Collector()
    started = code_sha256()
    status = int(pytest.main([*map(str, CASES), "-q", "-p", "no:cacheprovider"], plugins=[collector]))
    counts = {name: sum(1 for value in collector.outcomes.values() if value == name) for name in ("passed", "failed", "error", "skipped")}
    result = {
        "suite": "manual_units",
        "exit_status": status,
        "counts": counts,
        "cases": dict(sorted(collector.outcomes.items())),
        "code_sha256": started,
        "code_unchanged_during_run": code_sha256() == started,
        "network": "none: stand-in publisher, stand-in release, create-once S3 stand-in and an injected clock",
    }
    arguments.output.mkdir(parents=True, exist_ok=True)
    (arguments.output / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps({"counts": counts, "exit_status": status, "result": str(arguments.output / "result.json")}) + "\n")
    return 0 if status == 0 and result["code_unchanged_during_run"] else 1


if __name__ == "__main__":
    sys.exit(main())
