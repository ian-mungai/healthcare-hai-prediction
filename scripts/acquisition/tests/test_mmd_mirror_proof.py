"""Proof that the MMD API collector verifies a stored capture from the mirror alone. It reads the local data folder and changes nothing in it.

Set HAI_LOCAL_DATA_ROOT to the repository folder that holds data/. Without it the case is skipped. No request leaves the machine: a stand-in
connection replays the responses the stored capture retained, and name lookups and sockets fail the case if used.
"""

from __future__ import annotations

import http.client
import io
import json
import os
import socket
from email.message import Message
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from scripts.acquisition import collect_mmd_api, mmd_api_contract, source_registry, stored_mirror
from scripts.acquisition import redownload as harness
from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.data_paths import current
from tests.support import check

ROOT = Path(__file__).resolve().parents[3]
DATA = os.environ.get("HAI_LOCAL_DATA_ROOT")
QUEUE = "data/redownload_checks/20261004/queue.json"

pytestmark = pytest.mark.skipif(DATA is None, reason="HAI_LOCAL_DATA_ROOT is not set")


BODIES: dict[str, bytes] = {}
SEEN: list[str] = []


class Answer(io.BytesIO):
    status = 200

    def getheader(self, name: str, default: str = "") -> str:
        return "application/json" if name.lower() == "content-type" else default


class Replay:
    """Answers each request target from the bodies the stored capture kept, and records them."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.target = ""

    def request(self, method: str, target: str, headers: Message | dict | None = None, **kwargs: Any) -> None:
        self.target = target
        SEEN.append(target)

    def getresponse(self) -> Answer:
        return Answer(BODIES[self.target])

    def close(self) -> None:
        pass


def target_of(url: str) -> str:
    parts = urlsplit(url)
    return parts.path + "?" + parts.query


def test_an_mmd_unit_is_verified_from_the_mirror_with_the_stored_responses_replayed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base = Path(str(DATA))
    units = json.loads((base / QUEUE).read_text())["queue"]
    unit = next(item for item in units if item["disposition"] == "api" and item["mode"] == "mmd_api")
    stored_receipt = json.loads((base / current(unit["receipt"])).read_text())
    capture = (base / current(unit["receipt"])).parent
    roles = {item["role"]: item for item in stored_receipt["artifacts"]}
    raw = (capture / roles["api_page"]["storage_path"]).read_bytes()
    evidence = json.loads((capture / roles["export_receipt"]["storage_path"]).read_text())
    BODIES.clear()
    BODIES.update({target_of(evidence["main"]["url"]): raw} | {target_of(probe["url"]): probe["body_utf8"].encode() for probe in evidence["probes"]})
    SEEN.clear()

    queue = tmp_path / "queue.json"
    queue.write_text(json.dumps({"run": 2, "queue": [unit]}))
    inventory = tmp_path / "inventory.csv"
    inventory.write_text(
        "source_id,snapshot_id,s3_key,s3_version_id,sha256\nMMD," + unit["snapshot_id"] + ",raw/a/b/datasets/t/r/" + "1" * 64 + "/f.csv,v1," + "1" * 64 + "\n"
    )
    # The legacy registry versions resolve from the code root, so the build looks for them in the local data folder.
    with monkeypatch.context() as local:
        local.setattr(source_registry, "REPO_ROOT", base)
        stored_mirror.build(queue, base, inventory, tmp_path / "out", "synthetic-bucket")
    mirror = stored_mirror.Mirror(tmp_path / "out" / "mirror", json.loads((tmp_path / "out" / "stored_index.json").read_text()), client=None)
    check("mmd_api" in mirror.carried, "the mirror says it carries MMD")

    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("a name lookup or socket was used")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(http.client, "HTTPSConnection", Replay)
    # Stand-in for the owner's approval of the changed collector code, which the real list holds after review.
    approved = [mmd_api_contract.current_code_hashes()]
    monkeypatch.setattr(collect_mmd_api, "reviewed_code_versions", lambda: approved)
    monkeypatch.setattr(mmd_api_contract, "reviewed_code_versions", lambda: approved)
    monkeypatch.chdir(tmp_path)

    run = harness.Run(
        queue_path=queue, state_root=tmp_path / "run", base=ROOT, stored_root=mirror.root, carried_modes=mirror.carried, materialize=mirror.materialize
    )
    run.cache["queue"] = [unit]
    run.state_root.mkdir()
    boundary = controls.Boundary(run.state_root, run.base, unit["snapshot_id"], run.byte_cap)
    with controls.active(boundary):
        outcome = harness.handle(unit, run)
    result = {
        "snapshot_id": unit["snapshot_id"],
        "outcome": outcome["outcome"],
        "requests_replayed": len(SEEN),
        "differing": outcome.get("differing_data_files", []),
    }
    target = os.environ.get("HAI_PROOF_OUTPUT")
    if target:
        Path(target).write_text(json.dumps(result, indent=2) + "\n")
    check(
        outcome["outcome"] == "exact_match" and result["requests_replayed"] >= 4,
        f"the proof run result {result} {outcome} {[f.read_text()[:600] for f in run.state_root.rglob('diagnostics/*.json')]}",
    )
