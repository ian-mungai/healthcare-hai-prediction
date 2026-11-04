from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition import redownload_route as route
from scripts.acquisition.tests.held import classification_body, hold
from scripts.acquisition.tests.test_redownload_route import write_state
from tests.support import check

# One unit of each kind the real run 3 queue holds: (mode, disposition, flags) and the lane the harness must give it.
KINDS = {
    "file": ("file", "direct_download", [], "task"),
    "page": ("il_directory", "direct_download", [], "task"),
    "mmd": ("mmd_api", "api", [], "task"),
    "mmd_moved": ("file", "api", ["browser_export_moved_to_api:parity_verified"], "task"),
    "mmd_personal": ("mmd_api", "api", [], "private_local"),
    "mmd_moved_personal": ("file", "api", ["browser_export_moved_to_api:parity_verified"], "private_local"),
    "mmd_unreviewed": ("mmd_api", "api", [], "not_reviewed"),
    "mmd_moved_unreviewed": ("file", "api", ["browser_export_moved_to_api:parity_verified"], "not_reviewed"),
    "bls": ("bls_api_v2", "api", [], "task_keyed"),
    "census": ("census_acs_api", "api", [], "task_keyed"),
    "census_detailed": ("census_acs_detailed", "api", [], "task_keyed"),
    "hud": ("hud_api", "api", [], "task_keyed"),
    "hud_xlsx": ("hud_xlsx", "approved_manual", [], "needs_person"),
    "wonder": ("wonder_export", "approved_manual", [], "needs_person"),
    "reference": ("reference_download", "approved_manual", [], "needs_person"),
    "manual_file": ("file", "approved_manual", [], "needs_person"),
    "owners": ("cms_owners_org", "direct_download", [], "task_filtered"),
    "owners_personal": ("cms_owners_org", "direct_download", [], "private_local"),
    "hcai": ("hcai_util_workbook", "direct_download", [], "private_local"),
    "onc": ("onc_mu_hospital", "direct_download", [], "task_filtered"),
    "onc_personal": ("onc_mu_hospital", "direct_download", [], "private_local"),
    "contacts_file": ("file", "direct_download", [], "private_local"),
    "unreviewed_file": ("file", "direct_download", [], "not_reviewed"),
}


def make_unit(name: str, index: int) -> dict:
    mode, disposition, flags, _lane = KINDS[name]
    return {
        "snapshot_id": f"{name}__{index}",
        "source_id": name.upper(),
        "disposition": disposition,
        "mode": mode,
        "route": "publisher_url",
        "url": f"https://{name.replace('_', '-')}.example.gov/file.csv",
        "bytes": 10,
        "flags": flags,
        "receipt": f"stored/{name}/receipt.json",
        "receipt_sha256": "0" * 64,
    }


UNITS = [make_unit(name, index) for index, name in enumerate(KINDS)]
LANES = {unit["snapshot_id"]: KINDS[unit["snapshot_id"].split("__")[0]][3] for unit in UNITS}
# The owner's review: personal where the lane says private, nothing where the unit is unreviewed, non personal otherwise.
CLASSES = {unit_id: ("personal" if lane == "private_local" else "non_personal") for unit_id, lane in LANES.items() if lane != "not_reviewed"}


def test_every_kind_of_unit_in_the_queue_has_exactly_one_lane() -> None:
    for unit in UNITS:
        check(harness.lane_of(unit, CLASSES) == LANES[unit["snapshot_id"]], f"{unit['snapshot_id']} lane {harness.lane_of(unit, CLASSES)}")


def test_an_unknown_mode_has_no_lane_to_run_in() -> None:
    check(
        harness.lane_of(make_unit("file", 0) | {"mode": "new_collector"}, CLASSES | {"file__0": "non_personal"}) == "unclassified",
        "an unknown mode is unclassified",
    )


COLLECTORS_OF_PERSONAL_ORIGINALS = frozenset({"hcai_util_workbook"})


def test_every_adapter_mode_is_classified() -> None:
    for mode in harness.ADAPTERS:
        unit = make_unit("file", 0) | {"mode": mode, "disposition": "api"}
        lane = harness.lane_of(unit, {"file__0": "non_personal"})
        check((lane == "private_local") == (mode in COLLECTORS_OF_PERSONAL_ORIGINALS), f"{mode} has lane {lane}")


def dispatch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, **run_fields: Any) -> tuple[list[str], dict]:
    seen: list[str] = []

    def handle(unit: dict, run: Any) -> dict:
        seen.append(unit["snapshot_id"])
        return {"outcome": "exact_match"}

    monkeypatch.setattr(harness, "handle", handle)
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    run = harness.Run(queue_path=tmp_path / "queue.json", state_root=tmp_path / "run", base=tmp_path, **run_fields)
    hold(run, UNITS, CLASSES, [unit["snapshot_id"] for unit in UNITS if LANES[unit["snapshot_id"]] == "not_reviewed"])
    run.state_root.mkdir()
    summary = harness.execute_locked(run, False, True, None, None, None)
    return seen, summary


def test_the_task_attempts_only_the_units_its_mirror_can_serve_and_lists_the_rest_by_reason(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    seen, summary = dispatch(monkeypatch, tmp_path, stored_root=tmp_path / "mirror")
    served = sorted(unit_id for unit_id, lane in LANES.items() if lane == "task")
    check(sorted(seen) == served, f"the task attempted {sorted(seen)}")
    for lane in ("needs_person", "private_local", "task_keyed", "task_filtered", "not_reviewed"):
        expected = sorted(unit_id for unit_id, found in LANES.items() if found == lane)
        check(sorted(summary["not_attempted"][lane]) == expected, f"{lane} units are listed: {summary.get('not_attempted')}")


def test_a_unit_with_no_lane_is_never_attempted_by_the_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    units = [make_unit("file", 0) | {"snapshot_id": "new__0", "mode": "new_collector", "source_id": "NEW"}]
    monkeypatch.setattr(harness, "handle", lambda unit, run: pytest.fail("an unclassified unit was attempted"))
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    run = harness.Run(queue_path=tmp_path / "queue.json", state_root=tmp_path / "run", base=tmp_path, stored_root=tmp_path / "mirror")
    hold(run, units)
    run.state_root.mkdir()
    summary = harness.execute_locked(run, False, True, None, None, None)
    check(summary["not_attempted"]["unclassified"] == ["new__0"], "the unknown mode is listed")


def classify(folder: Path, queue_body: bytes) -> None:
    unreviewed = [unit["snapshot_id"] for unit in UNITS if LANES[unit["snapshot_id"]] == "not_reviewed"]
    (folder / "unit_classification.json").write_bytes(classification_body(queue_body, UNITS, CLASSES, unreviewed))


def lane_files(tmp_path: Path, **changes: Any) -> tuple[harness.Run, Path, route.RouteFiles]:
    folder = tmp_path / "route"
    folder.mkdir(parents=True)
    units = []
    for unit in UNITS:
        receipt = tmp_path / "base" / unit["receipt"]
        receipt.parent.mkdir(parents=True, exist_ok=True)
        receipt.write_bytes(b"{}")
        units.append(unit | {"receipt_sha256": hashlib.sha256(b"{}").hexdigest()})
    paths = write_state(folder, units, [])
    queue = folder / "queue.json"
    document = json.loads(queue.read_text())
    document["run"] = 3
    body = json.dumps(document).encode()
    queue.write_bytes(body)
    (folder / "queue.lock.json").write_text(json.dumps({"queue_sha256": hashlib.sha256(body).hexdigest(), "entries": len(UNITS)}))
    classify(folder, body)
    hosts = folder / "publisher_hosts.json"
    hosts.unlink()
    route.derive_hosts(queue, hosts)
    paths["hosts_approval"].write_text(json.dumps({"hosts_sha256": json.loads(hosts.read_text())["hosts_sha256"], "approved_by": "owner"}))
    files = route.RouteFiles(paths["plan"], paths["confirmation"], hosts, paths["hosts_approval"])
    approval = folder / "lane_approval.json"
    route.write_lane_template(queue, approval)
    document = json.loads(approval.read_text())
    document["approved_by"] = "owner"
    approval.write_text(json.dumps({**document, **changes}))
    run = harness.Run(
        queue_path=queue,
        state_root=tmp_path / "run",
        base=tmp_path / "base",
        route=files,
        person_lane=True,
        lane_approval=approval,
    )
    return run, approval, files


def refused(monkeypatch: pytest.MonkeyPatch, run: harness.Run, message: str) -> None:
    called: list[str] = []

    def record(unit: dict, run: Any) -> dict:
        called.append(unit["snapshot_id"])
        return {}

    monkeypatch.setattr(harness, "handle", record)
    with pytest.raises(ValueError, match=message):
        harness.execute(run, allow_network=True)
    check(called == [], "nothing was attempted")


def test_with_both_flags_the_task_takes_the_keyed_and_filtered_units_and_still_no_personal_browser_or_unreviewed_unit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    seen, summary = dispatch(monkeypatch, tmp_path, stored_root=tmp_path / "mirror", task_keys=object(), task_filtered=True)
    served = sorted(unit_id for unit_id, lane in LANES.items() if lane in {"task", "task_keyed", "task_filtered"})
    check(sorted(seen) == served, f"the task attempted {sorted(seen)}")
    check(
        set(summary["not_attempted"]) == {"needs_person", "private_local", "not_reviewed"},
        f"the rest are named: {summary['not_attempted']}",
    )


def test_a_key_flag_alone_opens_only_the_keyed_units(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    seen, _ = dispatch(monkeypatch, tmp_path, stored_root=tmp_path / "mirror", task_keys=object())
    check(sorted(seen) == sorted(unit_id for unit_id, lane in LANES.items() if lane in {"task", "task_keyed"}), f"the task attempted {sorted(seen)}")


def test_the_mac_keyed_lane_is_gone() -> None:
    for name in ("LANE_NAME", "LANE_DEPARTURE", "SECRET_MODES"):
        check(not hasattr(route, name), f"{name} is gone from the route")
    check("keyed_lane" not in harness.Run.__dataclass_fields__, "the run has no keyed lane field")
    check(not hasattr(harness, "KEYED"), "the harness has no Mac keyed lane")


def test_the_person_lane_approval_names_only_the_browser_units(tmp_path: Path) -> None:
    folder = tmp_path / "route"
    folder.mkdir()
    write_state(folder, UNITS, [])
    classify(folder, (folder / "queue.json").read_bytes())
    document = route.write_lane_template(folder / "queue.json", folder / "template.json")
    check(document["lane"] == "mac_person_units" and document["classes"] == ["needs_person"], "the only lane template is the person lane")
    check("keys_stay_off_aws" not in document and "modes" not in document, "no keyed lane field remains")
