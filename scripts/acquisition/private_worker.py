"""Run one queue unit whose original names people, in a process whose output goes nowhere.

The logging parent never loads such a unit. It starts this worker with only paths, and the worker closes its input and output descriptors
and silences library logging before it imports the harness. It writes one small answer file: digests, sizes, counts and fixed codes, or the
category of a failure. An exception message, a path or a traceback never leaves this process.
"""

from __future__ import annotations

import contextlib
import faulthandler
import json
import logging
import os
import re
import sys
import warnings
from pathlib import Path
from typing import Any

FAILED = 70


def silence() -> None:
    """Point the standard descriptors at the null device and mute every Python level output route, before anything else is imported."""
    null = os.open(os.devnull, os.O_RDWR)
    for descriptor in (0, 1, 2):
        os.dup2(null, descriptor)
    sys.stdout = sys.stderr = os.fdopen(os.dup(null), "w", encoding="utf-8")
    sys.excepthook = lambda *_: None
    logging.disable(logging.CRITICAL)
    warnings.simplefilter("ignore")
    faulthandler.disable()


def write_answer(path: Path, answer: dict[str, Any]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(answer, handle, sort_keys=True)


def run_job(job: dict[str, Any]) -> dict[str, Any]:
    """Run the unit and return a fixed-shape answer. Any failure becomes its category, with the original error dropped."""
    from scripts.acquisition import redownload as harness
    from scripts.acquisition import redownload_controls as controls

    try:
        run = harness.Run(
            queue_path=Path(job["queue_path"]),
            state_root=Path(job["state_root"]),
            base=Path(job["base"]),
            state_parent=Path(job["state_parent"]) if job.get("state_parent") else None,
            stored_root=Path(job["stored_root"]) if job.get("stored_root") else None,
            manual_folder=Path(job["manual_folder"]) if job.get("manual_folder") else None,
        )
        unit = next(item for item in harness.load_queue(run) if item["snapshot_id"] == job["snapshot_id"])
        run.cache["controls_sha256"] = harness.review_gate(run)
        run.cache["started_at"] = harness.started_at(run)
        boundary = controls.Boundary(run.state_root, run.base, unit["snapshot_id"], run.byte_cap, used=harness.used_bytes(run))
        try:
            with controls.active(boundary):
                fields = harness.private_safe(harness.handle(unit, run))
        finally:
            used = boundary.used
        return {"kind": "ok", "fields": fields, "budget_used": used}
    except harness.Pause as pause:
        delay = re.search(r"retry_after_seconds=([0-9]+)", str(pause))
        return {"kind": "pause", "retry_after_seconds": min(int(delay[1]), 86400) if delay else 0}
    except harness.Pending:
        return {"kind": "pending"}
    except controls.BudgetExceeded as failure:
        return {"kind": "budget", "bytes": "Byte cap" in str(failure)}
    except BaseException as error:
        category = harness.error_category(error)
        with contextlib.suppress(BaseException):
            if isinstance(error, Exception):
                harness.record_diagnostic(run, job["snapshot_id"], error)
        return {"kind": "failed", "category": category}


def main() -> None:
    silence()
    job = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    try:
        answer = run_job(job)
        write_answer(Path(job["result_path"]), answer)
    except BaseException:
        os._exit(FAILED)
    os._exit(0)


if __name__ == "__main__":
    main()
