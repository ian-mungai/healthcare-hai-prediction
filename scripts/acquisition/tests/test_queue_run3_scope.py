"""End-to-end check of the run 3 scope tools on synthetic inputs: the BLS exclusion list and the queue builder that an exclusion list drives."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

from tests.support import check

FOLDER = Path(__file__).resolve().parents[1] / "one_off/redownload"
REASON = "owner_bls_2006_to_2025_only"
OUTPUT = "data/redownload_checks/20261111_run3_scope"
EXCLUSIONS = f"{OUTPUT}/exclusions.json"


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, FOLDER / f"{name}.py")
    if spec is None or spec.loader is None:
        raise AssertionError(f"the script {name} exists")
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


def snapshot(label: str) -> str:
    return bls_unit(label)["snapshot_id"]


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


def write_list(root: Path, entries: list[dict], path: str = EXCLUSIONS) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"kind": "run3_scope_exclusions", "excluded": entries}, indent=1, sort_keys=True) + "\n")


def entry(label: str, reason: str = REASON) -> dict:
    return {"snapshot_id": snapshot(label), "reason": reason}


def list_bls(root: Path, before: int = 2006) -> None:
    load("list_run3_exclusions_bls").main(["--root", str(root), "--output", EXCLUSIONS, "--before-year", str(before), "--reason", REASON])


def build(root: Path, expected: int, exclusions: str = EXCLUSIONS, out: str = OUTPUT) -> None:
    load("build_queue_run3_scope").main(["--root", str(root), "--exclusions", exclusions, "--output", out, "--expect-dropped", str(expected)])


def test_the_bls_list_names_exactly_the_batches_that_end_before_the_cut(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    list_bls(tmp_path)
    document = json.loads((tmp_path / EXCLUSIONS).read_text())
    check(document["kind"] == "run3_scope_exclusions", "the list says what it is")
    check(
        document["excluded"] == sorted([entry("old-a"), entry("old-b")], key=lambda item: item["snapshot_id"]),
        "the two 1990 to 2005 units, each with its reason",
    )
    before = (tmp_path / EXCLUSIONS).read_bytes()
    list_bls(tmp_path)
    check((tmp_path / EXCLUSIONS).read_bytes() == before, "a second run changes nothing")


def test_the_bls_list_refuses_a_batch_that_spans_the_cut_and_a_unit_outside_the_plan(tmp_path: Path) -> None:
    write_root(tmp_path, {**WINDOWS, "span": (2000, 2010)})
    with pytest.raises(SystemExit) as stopped:
        list_bls(tmp_path)
    check("spans" in str(stopped.value), "a batch that starts before 2006 and ends after it is not split")
    check(not (tmp_path / EXCLUSIONS).exists(), "nothing is written")
    other = tmp_path / "other"
    write_root(other, WINDOWS, [bls_unit("not-in-plan")])
    with pytest.raises(SystemExit) as stopped:
        list_bls(other)
    check("not in the plan" in str(stopped.value), "every BLS unit must be a planned batch")


def test_only_the_listed_units_leave_the_queue(tmp_path: Path) -> None:
    units = write_root(tmp_path, WINDOWS)
    list_bls(tmp_path)
    build(tmp_path, 2)
    document = json.loads((tmp_path / OUTPUT / "queue.json").read_text())
    by_id = {unit["snapshot_id"]: unit for unit in document["queue"]}
    before = {unit["snapshot_id"]: unit for unit in units}
    check(list(by_id) == list(before), "every entry stays, in order")
    dropped = sorted(name for name, unit in by_id.items() if unit["disposition"] == "not_used_by_bronze" and f"run3_scope:{REASON}" in unit["flags"])
    check(dropped == sorted([snapshot("old-a"), snapshot("old-b")]), "exactly the listed units leave, flagged with their reason")
    for name, unit in by_id.items():
        if name not in dropped:
            check(unit == before[name], f"{name} is unchanged")
    for name in dropped:
        check(
            by_id[name]["route"] is None and by_id[name]["receipt_sha256"] == before[name]["receipt_sha256"],
            "a dropped unit keeps its receipt and has no route",
        )
    check(document["budgets"] == budgets_of(document["queue"]), "budgets match the active units")
    check(document["budgets"]["bls_calls"] == 3 and document["budgets"]["bls_minimum_windows"] == 1, "three BLS calls remain in one window")
    check(document["dispositions"]["not_used_by_bronze"] == 3, "the dropped units join the not used units")


def test_a_longer_list_is_a_rerun_with_each_reason_on_its_unit(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    write_list(
        tmp_path,
        sorted([entry("old-a"), entry("old-b"), {"snapshot_id": "ACS__acs1", "reason": "audit_not_used_downstream"}], key=lambda item: item["snapshot_id"]),
    )
    build(tmp_path, 3)
    document = json.loads((tmp_path / OUTPUT / "queue.json").read_text())
    by_id = {unit["snapshot_id"]: unit for unit in document["queue"]}
    check(
        by_id["ACS__acs1"]["disposition"] == "not_used_by_bronze" and by_id["ACS__acs1"]["flags"] == ["run3_scope:audit_not_used_downstream"],
        "a non-BLS unit leaves on its own reason",
    )
    check(by_id[snapshot("old-a")]["flags"] == [f"run3_scope:{REASON}"], "the BLS units keep theirs")
    check(document["budgets"]["acquisition_units"] == 5, "the budgets follow the longer list")


def test_the_lock_binds_the_new_queue_and_the_parent_and_the_list_are_inputs(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    list_bls(tmp_path)
    build(tmp_path, 2)
    folder = tmp_path / OUTPUT
    body = (folder / "queue.json").read_bytes()
    lock = json.loads((folder / "queue.lock.json").read_text())
    document = json.loads(body)
    check(lock == {"queue_sha256": hashlib.sha256(body).hexdigest(), "entries": len(document["queue"])}, "the lock names the new queue and its entry count")
    inputs = document["inputs_sha256"]
    parent_body = (tmp_path / "data/redownload_checks/20261004/queue.json").read_bytes()
    check(inputs["data/redownload_checks/20261004/queue.json"] == hashlib.sha256(parent_body).hexdigest(), "the parent queue is a frozen input")
    check(inputs[EXCLUSIONS] == hashlib.sha256((tmp_path / EXCLUSIONS).read_bytes()).hexdigest(), "the exclusion list is a frozen input")
    check(inputs["data/redownload_checks/20261001/queue.json"] == batch_id("run2"), "the parent's own inputs stay")
    check(not (folder / "controls.json").exists() and not (folder / "independent_review.json").exists(), "no controls or review are written")


def test_a_second_run_changes_nothing_and_a_different_result_is_refused(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    list_bls(tmp_path)
    build(tmp_path, 2)
    first = (tmp_path / OUTPUT / "queue.json").read_bytes()
    build(tmp_path, 2)
    check((tmp_path / OUTPUT / "queue.json").read_bytes() == first, "a second run is identical")
    (tmp_path / OUTPUT / "queue.json").write_bytes(first + b" ")
    with pytest.raises(SystemExit) as stopped:
        build(tmp_path, 2)
    check("do not overwrite" in str(stopped.value), "a different existing queue is never overwritten")


@pytest.mark.parametrize(
    ("entries", "expected", "message"),
    [
        ([entry("old-a")], 2, "expected 2"),
        ([entry("old-a"), entry("old-a")], 2, "twice"),
        ([{"snapshot_id": "NOPE__1", "reason": REASON}], 1, "not in the queue"),
        ([{"snapshot_id": "CMS__gone", "reason": REASON}], 1, "not an active unit"),
        ([{"snapshot_id": snapshot("old-a"), "reason": ""}], 1, "reason"),
        ([{"snapshot_id": snapshot("old-a"), "reason": "Has Spaces"}], 1, "reason"),
    ],
)
def test_a_list_that_does_not_fit_the_queue_is_refused_and_writes_nothing(tmp_path: Path, entries: list[dict], expected: int, message: str) -> None:
    write_root(tmp_path, WINDOWS)
    write_list(tmp_path, entries)
    with pytest.raises(SystemExit) as stopped:
        build(tmp_path, expected)
    check(message in str(stopped.value), f"the refusal says {message}")
    check(not (tmp_path / OUTPUT / "queue.json").exists(), "nothing is written")


def test_a_parent_queue_that_differs_from_its_lock_is_refused(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    list_bls(tmp_path)
    path = tmp_path / "data/redownload_checks/20261004/queue.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(SystemExit) as stopped:
        build(tmp_path, 2)
    check("differs from its lock" in str(stopped.value), "the parent queue must match its lock")
