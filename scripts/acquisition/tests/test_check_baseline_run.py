"""The offline baseline run check refuses a changed byte, a missing or extra file, a non-active or duplicate unit, a wrong budget, an uncovered host,
a plan input without exactly one home and a bootstrap seed that differs from its manifest, the queue or the Git LFS rule."""

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
    (root / "capture_specs.json").write_bytes(specs_for(trimmed))
    write_homes(root, homes_document())
    write_seed(root, seed_document(queue_sha256=hashlib.sha256(trimmed).hexdigest()))
    (root / PLAN_INPUT).parent.mkdir(parents=True)
    (root / PLAN_INPUT).write_text('{"a": 1}\n')
    (root / "inputs.lock.json").write_bytes(check_run.build_lock(root))
    return root


SEED_PATH = "data/example/seeded.csv"
HELD_PATH = "data/example/held.txt"
SEED_BODY = b"a,b\n1,2\n"


def seed_of(root: Path) -> Path:
    """The seed folder of a fixture bundle sits beside the bundle folder."""
    return root.parent / "seed"


def homes_document(**changes: object) -> dict:
    """Three expected plan inputs: one tracked in the bundle, one in the seed and one in the seed with a hold."""
    document = {"expected_plan_inputs": sorted([PLAN_INPUT.removeprefix("plan_inputs/"), SEED_PATH, HELD_PATH])}
    return document | changes


def seed_document(queue_sha256: str = "", **changes: object) -> dict:
    """The manifest of the fixture seed: one present file with its digest and one held file, no digest."""
    document = {
        "kind": "bootstrap_seed_manifest",
        "queue_sha256": queue_sha256,
        "files": [
            {"path": "example/seeded.csv", "sha256": hashlib.sha256(SEED_BODY).hexdigest(), "plan_input": SEED_PATH},
            {"path": "example/held.txt", "sha256": None, "plan_input": HELD_PATH, "hold": "waits for a decision"},
        ],
    }
    return document | changes


def write_homes(root: Path, document: dict) -> None:
    (root / "expected_plan_inputs.json").write_text(json.dumps(document, indent=1, sort_keys=True))


def write_seed(root: Path, document: dict, *, files: dict[str, bytes] | None = None) -> None:
    """Write the manifest and the seed files (the fixture's present file by default)."""
    folder = seed_of(root)
    folder.mkdir(exist_ok=True)
    (folder / "manifest.json").write_text(json.dumps(document, indent=1, sort_keys=True))
    for name, body in (files if files is not None else {"example/seeded.csv": SEED_BODY}).items():
        (folder / name).parent.mkdir(parents=True, exist_ok=True)
        (folder / name).write_bytes(body)


def relock(root: Path) -> None:
    (root / "inputs.lock.json").write_bytes(check_run.build_lock(root))


def seed_problems(root: Path, **options: bool) -> list[str]:
    return check_run.problems(root, seed_of(root), **options)


def specs_for(trimmed: bytes) -> bytes:
    queue = json.loads(trimmed)
    document = {
        "kind": "capture_specs",
        "queue_sha256": hashlib.sha256(trimmed).hexdigest(),
        "units": len(queue["queue"]),
        "specs": {u["snapshot_id"]: {"mode": "file", "stored_data_files": []} for u in queue["queue"]},
    }
    return json.dumps(document).encode()


def source_active_only(queue: dict) -> dict:
    return {**queue, "queue": [u for u in queue["queue"] if u["disposition"] in ("api", "direct_download", "approved_manual")]}


def test_clean_bundle_passes(tmp_path: Path) -> None:
    check(seed_problems(bundle(tmp_path / "b")) == [], "clean bundle has no problems")


def test_changed_byte_is_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    (root / PLAN_INPUT).write_text('{"a": 2}\n')
    check(any("differs from the lock" in p for p in seed_problems(root)), "changed byte")


def test_missing_and_extra_files_are_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    (root / PLAN_INPUT).unlink()
    (root / "plan_inputs/data/example/extra.json").write_text("{}\n")
    found = " ".join(seed_problems(root))
    check("missing from the bundle" in found and "not in the lock" in found, "missing and extra")


def test_lock_binds_a_bundle_digest(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    lock = json.loads((root / "inputs.lock.json").read_text())
    lock["bundle_sha256"] = "0" * 64
    (root / "inputs.lock.json").write_text(json.dumps(lock))
    check(any("bundle digest" in p for p in seed_problems(root)), "bundle digest")


def test_non_active_and_duplicate_units_are_refused(tmp_path: Path) -> None:
    def add(queue: dict) -> None:
        queue["queue"].append({**queue["queue"][0], "disposition": "dropped"})
        queue["queue"].append(dict(queue["queue"][1]))

    root = bundle(tmp_path / "b", mutate=add)
    found = " ".join(seed_problems(root))
    check("not an active disposition" in found and "duplicate snapshot ID" in found, "non-active and duplicate")


def test_wrong_budget_is_refused(tmp_path: Path) -> None:
    def skew(queue: dict) -> None:
        queue["budgets"]["recorded_bytes"] += 1

    check(any("recorded_bytes" in p for p in seed_problems(bundle(tmp_path / "b", mutate=skew))), "budget")


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
    found = " ".join(seed_problems(root))
    check("other.example.invalid" in found, "uncovered host")


def test_specs_that_name_another_queue_or_miss_a_unit_are_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    document = json.loads((root / "capture_specs.json").read_text())
    document["queue_sha256"] = "0" * 64
    del document["specs"]["a"]
    (root / "capture_specs.json").write_text(json.dumps(document))
    (root / "inputs.lock.json").write_bytes(check_run.build_lock(root))
    found = " ".join(seed_problems(root))
    check("capture specification belongs to another queue" in found and "has no capture specification: a" in found, "specs binding")


def test_specs_with_a_bucket_key_or_local_path_are_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    document = json.loads((root / "capture_specs.json").read_text())
    document["specs"]["a"]["note"] = "s3://example/key"
    (root / "capture_specs.json").write_text(json.dumps(document))
    (root / "inputs.lock.json").write_bytes(check_run.build_lock(root))
    check(any("capture specification holds" in p for p in seed_problems(root)), "environment value")


def test_a_plan_input_with_no_tracked_copy_or_seed_entry_is_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    write_homes(root, homes_document(expected_plan_inputs=[PLAN_INPUT.removeprefix("plan_inputs/"), SEED_PATH, HELD_PATH, "data/example/forgotten.csv"]))
    relock(root)
    check(any("forgotten.csv has no tracked copy or seed entry" in p for p in seed_problems(root)), "unaccounted plan input")


def test_a_plan_input_with_two_homes_or_a_listing_the_expected_list_lacks_is_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    both = {"path": "example/dup.json", "sha256": None, "plan_input": PLAN_INPUT.removeprefix("plan_inputs/"), "hold": "x"}
    unknown = {"path": "example/other.csv", "sha256": None, "plan_input": "data/example/other.csv", "hold": "x"}
    write_seed(root, seed_document(queue_sha256=json.loads((seed_of(root) / "manifest.json").read_text())["queue_sha256"]))
    manifest = json.loads((seed_of(root) / "manifest.json").read_text())
    manifest["files"] += [both, unknown]
    write_seed(root, manifest)
    found = " ".join(seed_problems(root))
    check("is tracked and also in the seed manifest" in found and "data/example/other.csv is not an expected plan input" in found, found)


def test_a_seed_that_belongs_to_another_queue_is_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    write_seed(root, seed_document(queue_sha256="0" * 64))
    check(any("seed manifest belongs to another queue" in p for p in seed_problems(root)), "queue binding")


def test_a_seed_file_with_a_changed_byte_or_no_recorded_digest_or_no_manifest_line_is_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    queue_sha = json.loads((seed_of(root) / "manifest.json").read_text())["queue_sha256"]
    write_seed(root, seed_document(queue_sha256=queue_sha), files={"example/seeded.csv": b"changed\n", "example/held.txt": b"x", "example/extra.csv": b"x"})
    found = " ".join(seed_problems(root))
    check("example/seeded.csv differs from the manifest digest" in found, "changed byte")
    check("example/held.txt has no recorded digest" in found, "present file without a digest")
    check("example/extra.csv is not in the manifest" in found, "unlisted file")


def test_missing_seed_files_pass_unless_required_and_a_held_one_passes_always(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    queue_sha = json.loads((seed_of(root) / "manifest.json").read_text())["queue_sha256"]
    write_seed(root, seed_document(queue_sha256=queue_sha), files={})
    (seed_of(root) / "example/seeded.csv").unlink()
    check(seed_problems(root) == [], "absent files are pending")
    found = seed_problems(root, require_files=True)
    check(found == ["example/seeded.csv is missing from the seed folder"], f"required files: {found}")


def test_the_seed_holds_exactly_the_stored_files_of_the_manual_units(tmp_path: Path) -> None:
    def manual(queue: dict) -> None:
        queue["queue"][2]["route"] = "manual_browser_download"

    root = bundle(tmp_path / "b", mutate=manual)
    specs = json.loads((root / "capture_specs.json").read_text())
    specs["specs"]["c"]["stored_data_files"] = ["c.xlsx", "crosswalk.csv"]
    (root / "capture_specs.json").write_text(json.dumps(specs))
    relock(root)
    queue_sha = json.loads((seed_of(root) / "manifest.json").read_text())["queue_sha256"]
    found = " ".join(seed_problems(root))
    check("the seed manifest lacks the stored file crosswalk.csv of unit c" in found and "lacks the stored file c.xlsx of unit c" in found, found)
    unit = [{"path": f"manual_units/c/{name}", "sha256": None, "unit": "c", "hold": "x"} for name in ("c.xlsx", "crosswalk.csv")]
    extra = {"path": "manual_units/zz/z.xlsx", "sha256": None, "unit": "zz", "hold": "x"}
    document = seed_document(queue_sha256=queue_sha)
    write_seed(root, document | {"files": [*document["files"], *unit, extra]})
    found = " ".join(seed_problems(root))
    check("manual_units/zz/z.xlsx names a stored file the queue does not expect" in found and "lacks" not in found, found)


def test_a_seed_path_outside_the_folder_or_listed_twice_is_refused(tmp_path: Path) -> None:
    root = bundle(tmp_path / "b")
    queue_sha = json.loads((seed_of(root) / "manifest.json").read_text())["queue_sha256"]
    document = seed_document(queue_sha256=queue_sha)
    bad = [{"path": "../escape.csv", "sha256": None, "plan_input": SEED_PATH}, dict(document["files"][1])]
    write_seed(root, document | {"files": [*document["files"], *bad]})
    found = " ".join(seed_problems(root))
    check("../escape.csv is not a relative path inside the seed folder" in found and "example/held.txt is listed twice" in found, found)


def test_every_path_of_the_committed_seed_manifest_is_stored_through_git_lfs() -> None:
    manifest = json.loads((check_run.SEED_ROOT / "manifest.json").read_text(encoding="utf-8"))
    paths = [item["path"] for item in manifest["files"]]
    check(bool(paths) and check_run.lfs_problems(paths) == [], f"not LFS: {check_run.lfs_problems(paths)[:3]}")
    check(check_run.lfs_problems(["notes.json"]) == ["notes.json is not stored through Git LFS"], "a path outside the rule is named")


def test_the_committed_bundle_and_seed_pass_the_whole_check() -> None:
    check(check_run.problems() == [], f"{check_run.problems()[:3]}")
