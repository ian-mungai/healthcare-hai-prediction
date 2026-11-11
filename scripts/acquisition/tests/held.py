"""Hold synthetic units in a run the way acceptance does: write the queue, then accept exactly those bytes."""

from __future__ import annotations

import json

from scripts.acquisition import redownload as harness


def hold(run: harness.Run, units: list[dict], number: int = 2) -> harness.Accepted:
    """Write the queue, make the run hold those bytes and return what it holds."""
    body = json.dumps({"run": number, "queue": units}).encode()
    run.queue_path.write_bytes(body)
    run.cache["accepted"] = harness.Accepted.build(body)
    return run.cache["accepted"]
