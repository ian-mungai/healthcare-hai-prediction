"""A synthetic run 3 in the task context: the queue, its lock and classification, the route files, the mirror and the approved hosts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts.acquisition import redownload as harness
from scripts.acquisition import redownload_route as route
from scripts.acquisition.run_redownload_controls_e2e import approve_fixture
from scripts.acquisition.tests import test_redownload_route as route_cases


def task_run(folder: Path, fixture: Any, **fields: Any) -> harness.Run:
    """Freeze the fixture's units as a run 3 queue and return the task's run over a mirror of their stored receipts."""
    queue = fixture.freeze()
    document = json.loads(queue.read_bytes()) | {"run": 3}
    queue.write_bytes(json.dumps(document).encode())
    queue.with_name("queue.lock.json").write_text(json.dumps({"entries": len(fixture.entries), "queue_sha256": harness.digest(queue.read_bytes())}))
    fixture.classify(queue)
    mirror = folder / "mirror"
    for entry in fixture.entries:
        for source in (folder / entry["receipt"]).parent.rglob("*"):
            if source.is_file():
                target = mirror / source.relative_to(folder)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(source.read_bytes())
    run = harness.Run(queue, folder / "state", base=folder, stored_root=mirror, sleep=lambda _s: None, **fields)
    approve_fixture(run)
    route_folder = folder / "route"
    route_folder.mkdir()
    paths = route_cases.write_state(route_folder, [route_cases.unit("unused")], [])
    route.derive_hosts(queue, folder / "hosts.json")
    digest = json.loads((folder / "hosts.json").read_bytes())["hosts_sha256"]
    (folder / "hosts_approval.json").write_text(json.dumps({"hosts_sha256": digest, "approved_by": "synthetic_owner"}))
    run.route = route.RouteFiles(paths["plan"], paths["confirmation"], folder / "hosts.json", folder / "hosts_approval.json")
    return run
