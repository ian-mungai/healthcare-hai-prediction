"""End-to-end check of the run 3 queue builder for the owner's BLS scope (2006 to 2025 only), on synthetic inputs."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

from tests.support import check

SCRIPT = Path(__file__).resolve().parents[1] / "one_off/redownload/build_queue_run3_bls2006.py"
FLAG = "run3_scope:owner_bls_2006_to_2025_only"


def builder() -> ModuleType:
    spec = importlib.util.spec_from_file_location("build_queue_run3_bls2006", SCRIPT)
    check(spec is not None and spec.loader is not None, "the builder script exists")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def batch_id(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def bls_unit(label: str, disposition: str = "api") -> dict:
    identifier = batch_id(label)
    return {
        "bytes": 100,
        "disposition": disposition,
        "flags": [],
        "mode": "bls_api_v2",
        "pages": 1,
        "receipt": f"data/historical_acquisition/bls_api_history/2026-09-26/batches/{identifier}/capture/receipt.json",
        "receipt_sha256": batch_id("receipt-" + label),
        "route": "publisher_api" if disposition == "api" else None,
        "snapshot_id": f"BLS_API__20260926T201226Z__{identifier[:32]}",
        "source_id": "BLS",
        "url": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
    }


def other_unit(name: str, mode: str, disposition: str, source: str = "ACS") -> dict:
    return {
        "bytes": 10,
        "disposition": disposition,
        "flags": [],
        "mode": mode,
        "pages": 1,
        "receipt": f"data/captures/{name}/receipt.json",
        "receipt_sha256": batch_id(name),
        "route": {"api": "publisher_api", "approved_manual": "manual_browser_download", "direct_download": "publisher_url"}.get(disposition),
        "snapshot_id": f"{source}__{name}",
        "source_id": source,
        "url": f"https://example.gov/{name}",
    }


def budgets_of(units: list[dict]) -> dict:
    active = [unit for unit in units if unit["disposition"] in ("api", "direct_download", "approved_manual")]
    routes: dict[str, int] = {}
    for unit in active:
        routes[unit["route"]] = routes.get(unit["route"], 0) + 1
    calls = sum(1 for unit in active if unit["source_id"] == "BLS" and unit["mode"] == "bls_api_v2")
    return {
        "acquisition_units": len(active),
        "units_by_route": dict(sorted(routes.items())),
        "max_primary_attempts": 2 * len(active),
        "pages_recorded": sum(unit["pages"] for unit in active),
        "recorded_bytes": sum(unit["bytes"] for unit in active),
        "byte_cap": 128 * 2**30,
        "bls_calls": calls,
        "bls_rolling_24h_limit": 450,
        "bls_minimum_windows": -(-calls // 450),
    }


def write_root(root: Path, windows: dict[str, tuple[int, int]], extra: list[dict] | None = None) -> list[dict]:
    units = [bls_unit(label) for label in windows]
    units += [bls_unit("manual-reference", "approved_manual") | {"mode": "reference_download", "route": "manual_browser_download"}]
    units += [
        other_unit("acs1", "census_acs_api", "api"),
        other_unit("file1", "file", "direct_download", "CMS"),
        other_unit("gone", "file", "not_used_by_bronze", "CMS"),
    ]
    units += extra or []
    parent = root / "data/redownload_checks/20261004"
    parent.mkdir(parents=True)
    document = {
        "kind": "bounded_redownload_queue",
        "execution_status": "frozen_offline_no_requests_made",
        "run": 3,
        "model_eligible": False,
        "entries": len(units),
        "dispositions": {},
        "loaded_files_no_unit_covers": ["X"],
        "budgets": budgets_of(units),
        "inputs_sha256": {"data/redownload_checks/20261001/queue.json": batch_id("run2")},
        "queue": units,
    }
    body = (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()
    (parent / "queue.json").write_bytes(body)
    (parent / "queue.lock.json").write_text(json.dumps({"queue_sha256": hashlib.sha256(body).hexdigest(), "entries": len(units)}))
    plan = {"batches": [{"id": batch_id(label), "start_year": start, "end_year": end, "series_ids": []} for label, (start, end) in windows.items()]}
    (root / "config/acquisition").mkdir(parents=True)
    (root / "config/acquisition/bls_api_plan.json").write_text(json.dumps(plan))
    return units


WINDOWS = {"old-a": (1990, 2005), "old-b": (1990, 2005), "new-a": (2006, 2025), "new-b": (2006, 2025), "new-c": (2006, 2025)}


def run(root: Path, expected: int, out: str = "data/redownload_checks/20261111_bls2006") -> None:
    builder().main(["--root", str(root), "--output", out, "--expect-dropped", str(expected)])


def test_only_batches_before_2006_leave_the_queue(tmp_path: Path) -> None:
    units = write_root(tmp_path, WINDOWS)
    run(tmp_path, 2)
    folder = tmp_path / "data/redownload_checks/20261111_bls2006"
    document = json.loads((folder / "queue.json").read_text())
    by_id = {unit["snapshot_id"]: unit for unit in document["queue"]}
    before = {unit["snapshot_id"]: unit for unit in units}
    check(list(by_id) == list(before), "every entry stays, in order")
    dropped = sorted(name for name, unit in by_id.items() if unit["disposition"] == "not_used_by_bronze" and FLAG in unit["flags"])
    check(dropped == sorted(f"BLS_API__20260926T201226Z__{batch_id(label)[:32]}" for label in ("old-a", "old-b")), "exactly the two 1990 to 2005 units leave")
    for name, unit in by_id.items():
        if name not in dropped:
            check(unit == before[name], f"{name} is unchanged")
    for name in dropped:
        check(
            by_id[name]["route"] is None and by_id[name]["receipt_sha256"] == before[name]["receipt_sha256"],
            "a dropped unit keeps its receipt and has no route",
        )
    budgets = document["budgets"]
    check(budgets == budgets_of(document["queue"]), "budgets match the active units")
    check(budgets["bls_calls"] == 3 and budgets["bls_minimum_windows"] == 1, "three BLS calls remain in one window")
    check(document["dispositions"]["not_used_by_bronze"] == 3, "the dropped units join the not used units")


def test_the_lock_binds_the_new_queue_and_the_parent_is_an_input(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    run(tmp_path, 2)
    folder = tmp_path / "data/redownload_checks/20261111_bls2006"
    body = (folder / "queue.json").read_bytes()
    lock = json.loads((folder / "queue.lock.json").read_text())
    document = json.loads(body)
    check(lock == {"queue_sha256": hashlib.sha256(body).hexdigest(), "entries": len(document["queue"])}, "the lock names the new queue and its entry count")
    parent_body = (tmp_path / "data/redownload_checks/20261004/queue.json").read_bytes()
    check(
        document["inputs_sha256"]["data/redownload_checks/20261004/queue.json"] == hashlib.sha256(parent_body).hexdigest(), "the parent queue is a frozen input"
    )
    check(document["inputs_sha256"]["data/redownload_checks/20261001/queue.json"] == batch_id("run2"), "the parent's own inputs stay")
    check(not (folder / "controls.json").exists() and not (folder / "independent_review.json").exists(), "no controls or review are written")


def test_a_second_run_changes_nothing_and_a_different_result_is_refused(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    run(tmp_path, 2)
    folder = tmp_path / "data/redownload_checks/20261111_bls2006"
    first = (folder / "queue.json").read_bytes()
    run(tmp_path, 2)
    check((folder / "queue.json").read_bytes() == first, "a second run is identical")
    (folder / "queue.json").write_bytes(first + b" ")
    with pytest.raises(SystemExit) as stopped:
        run(tmp_path, 2)
    check("do not overwrite" in str(stopped.value), "a different existing queue is never overwritten")


def test_an_unexpected_count_is_refused_and_writes_nothing(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    with pytest.raises(SystemExit) as stopped:
        run(tmp_path, 3)
    check("expected 3" in str(stopped.value), "the count must match the owner's number")
    check(not (tmp_path / "data/redownload_checks/20261111_bls2006").exists(), "nothing is written")


def test_a_batch_that_spans_the_cut_is_refused(tmp_path: Path) -> None:
    write_root(tmp_path, {**WINDOWS, "span": (2000, 2010)})
    with pytest.raises(SystemExit) as stopped:
        run(tmp_path, 2)
    check("spans" in str(stopped.value), "a batch that starts before 2006 and ends after it is not split")


def test_a_unit_without_a_planned_batch_is_refused(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS, [bls_unit("not-in-plan")])
    with pytest.raises(SystemExit) as stopped:
        run(tmp_path, 2)
    check("not in the plan" in str(stopped.value), "every BLS unit must be a planned batch")


def test_a_parent_queue_that_differs_from_its_lock_is_refused(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    path = tmp_path / "data/redownload_checks/20261004/queue.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(SystemExit) as stopped:
        run(tmp_path, 2)
    check("differs from its lock" in str(stopped.value), "the parent queue must match its lock")
