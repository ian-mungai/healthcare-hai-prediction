from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import collect_mmd_api, mmd_api_contract, stored_mirror
from scripts.acquisition import redownload as harness
from scripts.acquisition.transport import CaptureError
from tests.support import check

PLAN = "data/mmd/collection_plan.json"
REPORT = "data/mmd/report.json"
ORIGINAL = "data/mmd/original_2020.csv"
REFERENCE = "data/mmd/references/countynames.tsv"
TEMPLATE = "data/mmd/template.json"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def synthetic_base(base: Path) -> dict[str, Any]:
    """A repository folder with a synthetic MMD plan, its comparison report, one AMI original and one reference, and a queue with one MMD unit."""
    files = {ORIGINAL: b"fips,county\n1001,A\n", REFERENCE: b"id\tname\n1\tA\n"}
    for relative, data in files.items():
        (base / relative).parent.mkdir(parents=True, exist_ok=True)
        (base / relative).write_bytes(data)
    report = {"years": [{"year": 2020, "original_path": ORIGINAL, "original_sha256": sha(files[ORIGINAL])}]}
    (base / REPORT).write_text(json.dumps(report))
    plan = {
        "comparison_report": REPORT,
        "comparison_report_sha256": sha((base / REPORT).read_bytes()),
        "registry_sha256": "b" * 64,
        "references": {"countynames.tsv": {"path": REFERENCE, "sha256": sha(files[REFERENCE])}},
    }
    (base / TEMPLATE).write_text("{}")
    (base / PLAN).write_text(json.dumps(plan))
    receipt = base / "stored/mmd/receipt.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_bytes(json.dumps({"artifacts": []}).encode())
    unit = {
        "snapshot_id": "MMD__1",
        "source_id": "MMD",
        "disposition": "api",
        "mode": "mmd_api",
        "route": "api",
        "url": "https://data.cms.gov/data-api/v1/mmd-tool/",
        "bytes": 1,
        "flags": [],
        "receipt": "stored/mmd/receipt.json",
        "receipt_sha256": sha(receipt.read_bytes()),
    }
    queue = base / "queue.json"
    queue.write_text(json.dumps({"run": 2, "queue": [unit]}))
    inventory = base / "inventory.csv"
    inventory.write_text("source_id,snapshot_id,s3_key,s3_version_id,sha256\nMMD,MMD__1,raw/a/b/datasets/t/r/" + "1" * 64 + "/f.csv,v1," + "1" * 64 + "\n")
    return {"queue": queue, "inventory": inventory, "files": [PLAN, REPORT, ORIGINAL, REFERENCE, TEMPLATE]}


def authorize(monkeypatch: pytest.MonkeyPatch, base: Path) -> None:
    monkeypatch.setattr(mmd_api_contract, "PLANS", {sha((base / PLAN).read_bytes()): Path(PLAN)})
    monkeypatch.setattr(collect_mmd_api, "TEMPLATE", Path(TEMPLATE))

    def current_registry(digest: str) -> dict[str, Path]:
        raise stored_mirror.RegistryError("Unknown or ambiguous registry fingerprint")

    monkeypatch.setattr(stored_mirror, "registry_version_paths", current_registry)


def build(base: Path, out: Path) -> Path:
    stored_mirror.build(base / "queue.json", base, base / "inventory.csv", out, "bucket")
    return out / "mirror"


def test_the_mirror_carries_the_mmd_plans_references_report_and_originals(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    state = synthetic_base(tmp_path / "src")
    authorize(monkeypatch, tmp_path / "src")
    mirror = build(tmp_path / "src", tmp_path / "out")
    for relative in state["files"]:
        check((mirror / relative).read_bytes() == (tmp_path / "src" / relative).read_bytes(), f"{relative} is carried byte for byte")
    index = json.loads((tmp_path / "out" / "stored_index.json").read_text())
    check(index["carries"] == ["file", "il_directory", "mmd_api"], "the index states the modes the mirror carries")


def test_a_queue_with_mmd_units_and_a_missing_mmd_file_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    synthetic_base(tmp_path / "src")
    authorize(monkeypatch, tmp_path / "src")
    (tmp_path / "src" / ORIGINAL).unlink()
    with pytest.raises(CaptureError, match="MMD"):
        build(tmp_path / "src", tmp_path / "out")


def test_a_plan_whose_digest_is_not_the_authorized_one_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    synthetic_base(tmp_path / "src")
    authorize(monkeypatch, tmp_path / "src")
    monkeypatch.setattr(mmd_api_contract, "PLANS", {"0" * 64: Path(PLAN)})
    with pytest.raises(CaptureError, match="MMD"):
        build(tmp_path / "src", tmp_path / "out")


def test_a_queue_without_mmd_units_does_not_claim_to_carry_mmd(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    synthetic_base(tmp_path / "src")
    authorize(monkeypatch, tmp_path / "src")
    queue = tmp_path / "src" / "queue.json"
    document = json.loads(queue.read_text())
    document["queue"][0]["mode"] = "file"
    queue.write_text(json.dumps(document))
    build(tmp_path / "src", tmp_path / "out")
    index = json.loads((tmp_path / "out" / "stored_index.json").read_text())
    check(index["carries"] == ["file", "il_directory"], "no MMD units, no MMD claim")


def test_the_working_folder_is_the_code_root_with_the_mirror_data_linked_in_and_removed_after(tmp_path: Path) -> None:
    code = tmp_path / "code"
    (code / "scripts").mkdir(parents=True)
    (code / "scripts" / "tool.py").write_text("code")
    mirror = tmp_path / "mirror"
    (mirror / "data").mkdir(parents=True)
    (mirror / "data" / "plan.json").write_text("plan")
    run = harness.Run(queue_path=tmp_path / "queue.json", state_root=tmp_path / "run", base=code, stored_root=mirror)
    before = Path.cwd()
    with harness.working_folder(run):
        check(Path.cwd() == code.resolve(), "the process works in the code root")
        check(Path("scripts/tool.py").read_text() == "code" and Path("data/plan.json").read_text() == "plan", "code and data resolve from one folder")
        check((code / "data" / "plan.json").read_text() == "plan", "absolute paths from the code root find the mirror data too")
    check(Path.cwd() == before and not (code / "data").exists(), "the folder is restored and the link is removed")


def test_a_code_root_that_already_has_a_data_folder_is_not_overlaid(tmp_path: Path) -> None:
    (tmp_path / "code" / "data").mkdir(parents=True)
    (tmp_path / "mirror" / "data").mkdir(parents=True)
    run = harness.Run(queue_path=tmp_path / "q.json", state_root=tmp_path / "run", base=tmp_path / "code", stored_root=tmp_path / "mirror")
    with pytest.raises(CaptureError, match="already"), harness.working_folder(run):
        pass
    check((tmp_path / "code" / "data").is_dir() and not (tmp_path / "code" / "data").is_symlink(), "the existing folder is untouched")


def test_a_mirror_without_a_data_folder_cannot_give_a_working_folder(tmp_path: Path) -> None:
    (tmp_path / "code").mkdir()
    (tmp_path / "mirror").mkdir()
    run = harness.Run(queue_path=tmp_path / "q.json", state_root=tmp_path / "run", base=tmp_path / "code", stored_root=tmp_path / "mirror")
    with pytest.raises(CaptureError, match="data"), harness.working_folder(run):
        pass


def test_without_a_mirror_the_folder_is_left_alone(tmp_path: Path) -> None:
    run = harness.Run(queue_path=tmp_path / "q.json", state_root=tmp_path / "run", base=tmp_path)
    before = Path.cwd()
    with harness.working_folder(run):
        check(Path.cwd() == before, "no mirror, no change of folder")


def test_the_mirror_carries_the_legacy_registry_version_files_the_mmd_plans_bind(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    synthetic_base(tmp_path / "src")
    authorize(monkeypatch, tmp_path / "src")
    legacy = {"registry": "data/legacy/registry.json", "additions_lock": "data/legacy/additions_lock.json"}
    for relative in legacy.values():
        (tmp_path / "src" / relative).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / "src" / relative).write_text(relative)
    plan = json.loads((tmp_path / "src" / PLAN).read_text())
    plan["registry_sha256"] = "a" * 64
    (tmp_path / "src" / PLAN).write_text(json.dumps(plan))
    monkeypatch.setattr(mmd_api_contract, "PLANS", {sha((tmp_path / "src" / PLAN).read_bytes()): Path(PLAN)})
    asked: list[str] = []

    def paths(digest: str) -> dict[str, Path]:
        asked.append(digest)
        return {name: tmp_path / "src" / relative for name, relative in legacy.items()}

    monkeypatch.setattr(stored_mirror, "registry_version_paths", paths)
    mirror = build(tmp_path / "src", tmp_path / "out")
    check(asked == ["a" * 64], "the builder asked for the plan's registry version")
    for relative in legacy.values():
        check((mirror / relative).read_text() == relative, f"{relative} is carried")


def test_a_plan_bound_to_the_current_registry_adds_no_legacy_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    synthetic_base(tmp_path / "src")
    authorize(monkeypatch, tmp_path / "src")

    def unknown(digest: str) -> dict[str, Path]:
        raise stored_mirror.RegistryError("Unknown or ambiguous registry fingerprint")

    monkeypatch.setattr(stored_mirror, "registry_version_paths", unknown)
    mirror = build(tmp_path / "src", tmp_path / "out")
    check((mirror / PLAN).is_file(), "the build still succeeds and carries the plan")


def test_a_unit_is_the_tasks_only_when_the_mirror_carries_its_mode(tmp_path: Path) -> None:
    unit = {"snapshot_id": "M", "disposition": "api", "mode": "mmd_api", "flags": []}
    check(harness.lane_of(unit, frozenset({"file", "il_directory", "mmd_api"})) == "task", "carried, so the task's")
    check(harness.lane_of(unit, frozenset({"file", "il_directory"})) == "needs_local_data", "not carried, so listed")
    check(harness.lane_of(unit) == "task", "with no mirror the lane is decided by the mode alone")


def test_a_legacy_registry_error_other_than_an_unknown_fingerprint_stops_the_build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    synthetic_base(tmp_path / "src")
    authorize(monkeypatch, tmp_path / "src")

    def broken(digest: str) -> dict[str, Path]:
        raise stored_mirror.RegistryError("Legacy registry file is missing or symlinked")

    monkeypatch.setattr(stored_mirror, "registry_version_paths", broken)
    with pytest.raises(stored_mirror.RegistryError, match="missing"):
        build(tmp_path / "src", tmp_path / "out")
