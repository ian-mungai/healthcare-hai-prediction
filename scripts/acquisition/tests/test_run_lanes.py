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
    "mmd": ("mmd_api", "api", [], "needs_local_data"),
    "mmd_moved": ("file", "api", ["browser_export_moved_to_api"], "needs_local_data"),
    "bls": ("bls_api_v2", "api", [], "keyed_mac_lane"),
    "census": ("census_acs_api", "api", [], "keyed_mac_lane"),
    "census_detailed": ("census_acs_detailed", "api", [], "keyed_mac_lane"),
    "hud": ("hud_api", "api", [], "keyed_mac_lane"),
    "hud_xlsx": ("hud_xlsx", "approved_manual", [], "needs_person"),
    "wonder": ("wonder_export", "approved_manual", [], "needs_person"),
    "reference": ("reference_download", "approved_manual", [], "needs_person"),
    "manual_file": ("file", "approved_manual", [], "needs_person"),
    "owners": ("cms_owners_org", "direct_download", [], "private_local"),
    "hcai": ("hcai_util_workbook", "direct_download", [], "private_local"),
    "onc": ("onc_mu_hospital", "direct_download", [], "private_local"),
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


COLLECTORS_OF_PERSONAL_ORIGINALS = frozenset({"cms_owners_org", "hcai_util_workbook", "onc_mu_hospital"})


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
    for lane in ("needs_local_data", "needs_person", "private_local", "keyed_mac_lane", "not_reviewed"):
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


def lane_files(tmp_path: Path, template_lane: str = "mac_keyed_units", **changes: Any) -> tuple[harness.Run, Path, route.RouteFiles]:
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
    route.write_lane_template(queue, approval, template_lane)
    document = json.loads(approval.read_text())
    document["approved_by"] = "owner"
    approval.write_text(json.dumps({**document, **changes}))
    run = harness.Run(
        queue_path=queue,
        state_root=tmp_path / "run",
        base=tmp_path / "base",
        route=files,
        keyed_lane=template_lane == "mac_keyed_units",
        person_lane=template_lane == "mac_person_units",
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


def test_the_lane_template_is_not_an_approval(tmp_path: Path) -> None:
    folder = tmp_path / "route"
    folder.mkdir()
    write_state(folder, UNITS, [])
    classify(folder, (folder / "queue.json").read_bytes())
    template = folder / "template.json"
    route.write_lane_template(folder / "queue.json", template)
    document = json.loads(template.read_text())
    check(
        document["approved_by"] is None and document["lane"] == "mac_keyed_units" and document["keys_stay_off_aws"] is True,
        "the template names the lane and no approver",
    )
    check(document["modes"] == sorted(route.SECRET_MODES) and document["units"] == 4, "the template names the modes and the unit count")
    check(any("approval" in item for item in route.lane_problems(folder / "queue.json", template)), "an unsigned template is not accepted")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"approved_by": None}, "lane approval"),
        ({"queue_sha256": "0" * 64}, "lane approval"),
        ({"modes": ["bls_api_v2"]}, "lane approval"),
        ({"units": 3}, "lane approval"),
        ({"lane": "something_else"}, "lane approval"),
        ({"keys_stay_off_aws": False}, "lane approval"),
    ],
)
def test_the_lane_refuses_to_start_unless_the_owner_approval_names_it(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: dict, message: str) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, _, _ = lane_files(tmp_path, **change)
    refused(monkeypatch, run, message)


def test_the_lane_refuses_to_start_without_any_approval_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, approval, _ = lane_files(tmp_path)
    approval.unlink()
    refused(monkeypatch, run, "lane approval")


def test_the_lane_refuses_to_start_inside_a_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, _, _ = lane_files(tmp_path)
    monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/example")
    refused(monkeypatch, run, "keys stay off AWS")
    run.stored_root = run.base
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    refused(monkeypatch, run, "keys stay off AWS")


def test_the_lane_needs_the_same_gate_and_the_raw_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    run, _, files = lane_files(tmp_path)
    refused(monkeypatch, run, "raw/")
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    files.confirmation.unlink()
    refused(monkeypatch, run, "removal confirmation")


def test_an_approved_lane_attempts_only_the_keyed_units_and_the_task_never_the_same_ones(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    seen, summary = dispatch(monkeypatch, tmp_path, keyed_lane=True)
    keyed = sorted(unit_id for unit_id, lane in LANES.items() if lane == "keyed_mac_lane")
    check(sorted(seen) == keyed, f"the lane attempted {sorted(seen)}")
    check("keyed_mac_lane" not in summary.get("not_attempted", {}), "the lane does not list its own units as blocked")
    (tmp_path / "task").mkdir()
    task_seen, _ = dispatch(monkeypatch, tmp_path / "task", stored_root=tmp_path / "mirror")
    check(not set(task_seen) & set(seen), "no unit runs in both lanes")


def test_an_approved_lane_on_the_mac_passes_both_gates_and_stops_at_the_next_gate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    for name in ("HAI_APPROVED_HOSTS", "HAI_APPROVED_HOSTS_APPROVAL"):
        monkeypatch.setenv(name, "placeholder")
    for name in harness.TASK_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    run, _, _ = lane_files(tmp_path)
    with pytest.raises(ValueError, match="independent review"):
        harness.execute(run, allow_network=True)
