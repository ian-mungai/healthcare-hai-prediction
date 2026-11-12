"""The offline baseline run check refuses a changed byte, a missing or extra file, a non-active or duplicate unit, a wrong budget and an uncovered host."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

from scripts.acquisition import build_bootstrap_queue as build
from scripts.acquisition import check_baseline_run as check_run
from scripts.acquisition.tests.test_build_bootstrap_queue import HOSTS, source_bytes
from scripts.acquisition.transport import hosts_digest
from tests.support import check

PLAN_INPUT = "plan_inputs/data/example/input.json"


def bundle(root: Path, *, mutate: Callable[[dict], None] | None = None) -> Path:
    """Write a clean bundle: trimmed queue, lock, hosts file, approval, one plan input and the inputs lock."""
    body = source_bytes()
    trimmed = build.trim_queue(body, hashlib.sha256(body).hexdigest())
    queue = json.loads(trimmed)
    queue["budgets"] = {"acquisition_units": 3, "recorded_bytes": 30, "pages_recorded": 3, "units_by_route": {"publisher_url": 3}}
    queue["queue"][0]["route"] = queue["queue"][1]["route"] = queue["queue"][2]["route"] = "publisher_url"
    if mutate:
        mutate(queue)
    trimmed = build.encode_queue(queue)
    approved = {"hosts": HOSTS, "hosts_sha256": hosts_digest(HOSTS), "queue_sha256": "0" * 64}
    root.mkdir(parents=True)
    (root / "queue.json").write_bytes(trimmed)
    (root / "queue.lock.json").write_bytes(build.queue_lock(trimmed))
    (root / "publisher_hosts_scope.json").write_bytes(
        build.rewrite_hosts(approved, trimmed) if not mutate else build.rewrite_hosts(approved, build.encode_queue(source_active_only(queue)))
    )
    (root / "hosts_approval.json").write_text(json.dumps({"hosts_sha256": hosts_digest(HOSTS), "approved_by": "owner"}))
    (root / PLAN_INPUT).parent.mkdir(parents=True)
    (root / PLAN_INPUT).write_text('{"a": 1}\n')
    (root / "inputs.lock.json").write_bytes(check_run.build_lock(root))
    return root


def source_active_only(queue: dict) -> dict:
    return {**queue, "queue": [u for u in queue["queue"] if u["disposition"] in ("api", "direct_download", "approved_manual")]}


def test_clean_bundle_passes(tmp_path: Path) -> None:
    check(check_run.problems(bundle(tmp_path / "b")) == [], "clean bundle has no problems")


def test_changed_byte_is_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    (root / PLAN_INPUT).write_text('{"a": 2}\n')
    check(any("differs from the lock" in p for p in check_run.problems(root)), "changed byte")


def test_missing_and_extra_files_are_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    (root / PLAN_INPUT).unlink()
    (root / "plan_inputs/data/example/extra.json").write_text("{}\n")
    found = " ".join(check_run.problems(root))
    check("missing from the bundle" in found and "not in the lock" in found, "missing and extra")


def test_lock_binds_a_bundle_digest(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    lock = json.loads((root / "inputs.lock.json").read_text())
    lock["bundle_sha256"] = "0" * 64
    (root / "inputs.lock.json").write_text(json.dumps(lock))
    check(any("bundle digest" in p for p in check_run.problems(root)), "bundle digest")


def test_non_active_and_duplicate_units_are_refused(tmp_path: Path) -> None:
    def add(queue: dict) -> None:
        queue["queue"].append({**queue["queue"][0], "disposition": "dropped"})
        queue["queue"].append(dict(queue["queue"][1]))

    root = bundle(tmp_path / "b", mutate=add)
    found = " ".join(check_run.problems(root))
    check("not an active disposition" in found and "duplicate snapshot ID" in found, "non-active and duplicate")


def test_wrong_budget_is_refused(tmp_path: Path) -> None:
    def skew(queue: dict) -> None:
        queue["budgets"]["recorded_bytes"] += 1

    check(any("recorded_bytes" in p for p in check_run.problems(bundle(tmp_path / "b", mutate=skew))), "budget")


def test_uncovered_host_is_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    queue = json.loads((root / "queue.json").read_text())
    queue["queue"][0]["url"] = "https://other.example.invalid/x"
    (root / "queue.json").write_bytes(build.encode_queue(queue))
    body = (root / "queue.json").read_bytes()
    (root / "queue.lock.json").write_bytes(build.queue_lock(body))
    hosts = {"hosts": HOSTS, "hosts_sha256": hosts_digest(HOSTS), "queue_sha256": hashlib.sha256(body).hexdigest()}
    (root / "publisher_hosts_scope.json").write_text(json.dumps(hosts))
    (root / "inputs.lock.json").write_bytes(check_run.build_lock(root))
    found = " ".join(check_run.problems(root))
    check("other.example.invalid" in found, "uncovered host")
