"""The MMD units run on the task: both variants are dispatched, their fresh captures are stored under raw/ before they count, and the next task keeps them.

The two variants are the `mmd_api` units and the `file` units that carry the flag `browser_export_moved_to_api`. Failure modes written before the code:

1. A reviewed non personal MMD unit of either variant is listed as needing local data and the task never dispatches it.
2. A personal or an unreviewed MMD unit is dispatched because the lane opened.
3. The mirror leaves out a file the capture reads: the locked plan, its references, the baseline of the year or the plan template.
4. The adapter requests no upload, so the fresh capture stays in the workspace and still counts.
5. A refused put or a read back that differs still ends in a match.
6. An object is created outside raw/.
7. A changed file of the moved variant is counted as a match, or is not stored.
8. The next task refuses a restored MMD outcome, or accepts the capture of another planned item.
9. A second pass or a second task repeats a request or stores a second copy.
10. The adapter reads an API key or needs a task switch.
11. The MMD units share one pilot, so the second unit waits for the first to match.

The publisher, the bucket and the code root are stand-ins. The collector, the harness, the storage code and the receipt checks are the real ones.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import collect_mmd_api as collector
from scripts.acquisition import collection_layout, publisher_run, s3_store, stored_mirror, transport
from scripts.acquisition import redownload as harness
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.tests import mmd_world
from scripts.acquisition.tests.mmd_world import MOVED_MEASURE, YEAR, World, counting
from scripts.acquisition.tests.task_fixture import task_run
from scripts.acquisition.tests.test_task_owners import TaskS3, storage
from tests.support import check


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real collection routes, and the approved hosts names restored after each case."""
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)


def no_key(mode: str) -> Any:
    check(False, f"the MMD adapter asked for the key of {mode}")


SWITCHES: dict[str, Callable[[], dict[str, Any]]] = {"both": lambda: {"task_keys": no_key, "task_filtered": True}, "none": dict}


class Task:
    """One task over the world: its run, its bucket and the units it dispatched."""

    def __init__(self, world: World, run: harness.Run, bucket: TaskS3, seen: list[str]) -> None:
        self.world, self.run, self.bucket, self.seen = world, run, bucket, seen

    def passes(self, count: int = 2) -> list[dict]:
        return [harness.execute(self.run) for _ in range(count)]

    def outcome(self, unit: dict) -> dict:
        return harness.outcome(self.run, unit["snapshot_id"]) or {}

    def next_task(self) -> Task:
        """A later task: a new run over the same state root, queue, mirror and bucket."""
        seen: list[str] = []
        run = dataclasses.replace(self.run, cache={}, adapters=counting(harness.ADAPTERS, seen))
        return Task(self.world, run, self.bucket, seen)


def start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    units: tuple[str, ...] = ("api", "moved"),
    bucket: TaskS3 | None = None,
    moved_body: bytes | None = None,
    prepare: Callable[[World], None] | None = None,
    **fields: Any,
) -> tuple[Task, dict[str, dict]]:
    world = World(tmp_path, monkeypatch)
    entries = {}
    if "api" in units:
        entries["api"] = world.add_api_unit()
    if "moved" in units:
        entries["moved"] = world.add_moved_unit(moved_body)
    if prepare is not None:
        prepare(world)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    bucket = bucket or TaskS3()
    seen: list[str] = []
    run = task_run(tmp_path, world.fixture, task_storage=storage(bucket), adapters=counting(harness.ADAPTERS, seen), **fields)
    return Task(world, run, bucket, seen), entries


def bodies(bucket: TaskS3) -> dict[str, bytes]:
    return {key: data for key, (_, data, _) in bucket.objects.items()}


def fresh_csv(task: Task, measure: str) -> bytes:
    folder = task.run.state_root / "collectors" / "mmd_api" / measure.replace(".", "_").lower() / str(YEAR)
    (path,) = folder.glob("captures/*/derived/*.csv")
    return path.read_bytes()


@pytest.mark.parametrize("switches", sorted(SWITCHES))
def test_the_task_dispatches_each_mmd_variant_once_and_asks_for_no_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, switches: str) -> None:
    task, entries = start(monkeypatch, tmp_path, **SWITCHES[switches]())
    first, second = task.passes()
    pilot, follower = entries["api"]["snapshot_id"], entries["moved"]["snapshot_id"]
    check(task.seen == [pilot, follower], f"expected the two MMD variants, once each, in queue order: {task.seen}")
    check(first["attempted"] == 1 and second["attempted"] == 1, f"the pilot runs first and the other unit follows: {first['attempted']} {second['attempted']}")
    check(second["outcomes"] == {"exact_match": 1} and first["outcomes"] == {"exact_match": 1}, f"both match: {first['outcomes']} {second['outcomes']}")
    listed = {**first.get("not_attempted", {}), **second.get("not_attempted", {})}
    check("needs_local_data" not in listed, f"no MMD unit is left as needing local data: {listed}")


@pytest.mark.parametrize("kind", ["personal", "unreviewed"])
def test_a_personal_or_unreviewed_mmd_unit_is_still_not_dispatched(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str) -> None:
    def hold(world: World) -> None:
        ids = {entry["snapshot_id"] for entry in world.fixture.entries}
        (world.fixture.personal if kind == "personal" else world.fixture.unreviewed).update(ids)

    task, entries = start(monkeypatch, tmp_path, prepare=hold, **SWITCHES["both"]())
    summary = harness.execute(task.run)
    lane = "private_local" if kind == "personal" else "not_reviewed"
    check(task.seen == [] and summary["attempted"] == 0, f"nothing was dispatched: {task.seen}")
    check(sorted(summary["not_attempted"][lane]) == sorted(entry["snapshot_id"] for entry in entries.values()), f"both units are named under {lane}")


def test_each_fresh_capture_is_stored_under_raw_and_the_outcome_names_it(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    task, entries = start(monkeypatch, tmp_path)
    task.passes()
    stored = bodies(task.bucket)
    check(bool(stored) and all(key.startswith("raw/") for key in stored), f"keys outside raw/: {sorted(stored)}")
    for variant, measure in (("api", mmd_world.API_MEASURE), ("moved", MOVED_MEASURE)):
        record = task.outcome(entries[variant])
        check(record["outcome"] == "exact_match", f"{variant} matched: {record}")
        check(fresh_csv(task, measure) in stored.values(), f"the derived file of {variant} is stored")
        check(record["stored"]["receipt"]["key"] in stored, f"{variant} names its stored receipt")
        check(bool(record["stored"]["manifests"]) and all(item["key"] in stored for item in record["stored"]["manifests"]), f"{variant} names its manifest")
    raw = [json.loads(data) for data in stored.values() if data.startswith(b"[")]
    check(len(raw) >= 2, f"the raw API responses are stored too: {len(raw)}")


def test_a_refused_store_blocks_the_unit_and_records_no_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    task, entries = start(monkeypatch, tmp_path, bucket=TaskS3(refuse_puts=True))
    summary = harness.execute(task.run)
    record = task.outcome(entries["api"])
    check(summary["outcomes"] == {"blocked": 1} and "stored" not in record, f"a refused store is no success: {summary} {record}")
    check(not task.bucket.objects, "nothing was created")


def test_a_read_back_that_differs_blocks_the_unit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    original = bucket.put_object

    def corrupt(**arguments: Any) -> dict:
        answer = original(**arguments)
        version, data, fields = bucket.objects[arguments["Key"]]
        bucket.objects[arguments["Key"]] = (version, data + b"x", fields)
        return answer

    monkeypatch.setattr(bucket, "put_object", corrupt)
    task, entries = start(monkeypatch, tmp_path, bucket=bucket)
    summary = harness.execute(task.run)
    check(summary["outcomes"] == {"blocked": 1} and "stored" not in task.outcome(entries["api"]), f"a wrong read back is no success: {summary}")


def test_a_changed_moved_unit_is_stored_and_named_changed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    task, entries = start(monkeypatch, tmp_path, moved_body=b"population,year\nsomething,else\n")
    task.passes()
    record = task.outcome(entries["moved"])
    check(record["outcome"] == "changed_needs_review", f"the other stored file is a change: {record}")
    check(fresh_csv(task, MOVED_MEASURE) in bodies(task.bucket).values() and "stored" in record, "the changed capture is stored and named")


@pytest.mark.parametrize("kinds", [("api", "moved"), ("moved",)], ids=["both variants", "moved variant only"])
def test_the_mirror_carries_everything_a_task_capture_reads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kinds: tuple[str, ...]) -> None:
    world = World(tmp_path, monkeypatch)
    if "api" in kinds:
        world.add_api_unit()
    world.add_moved_unit()
    queue = world.fixture.freeze()
    built = tmp_path / "built"
    stored_mirror.build(queue, tmp_path, world.inventory(tmp_path / "inventory.csv"), built, "example-bucket")
    listed = json.loads((built / "mirror" / stored_mirror.PLAN_INPUTS_NAME).read_text())
    wanted = {mmd_world.PLAN_FILE, mmd_world.TEMPLATE_FILE, mmd_world.REPORT_FILE, mmd_world.BASELINE_FILE}
    wanted |= {mmd_world.PLAN_FOLDER / "references" / name for name in mmd_world.reference_files()}
    check(wanted <= {Path(name) for name in listed}, f"the plan inputs lack {sorted(str(path) for path in wanted - {Path(name) for name in listed})}")
    task_code = tmp_path / "task_code"
    World.write_code_root(task_code)
    publisher_run.install_plan_inputs(built / "mirror", task_code)
    monkeypatch.chdir(task_code)
    receipt = collector.capture(MOVED_MEASURE, YEAR, True, root=tmp_path / "fresh")
    check(receipt.is_file(), "a capture made from the carried inputs alone reads every file it needs")


def test_a_second_task_keeps_the_restored_outcomes_and_repeats_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    task, entries = start(monkeypatch, tmp_path)
    task.passes()
    objects, requests = dict(task.bucket.objects), list(task.world.cms.requests)
    later = task.next_task()
    summary = harness.execute(later.run)
    check(summary["attempted"] == 0 and later.seen == [], f"the restored units are not dispatched again: {summary} {later.seen}")
    check(set(task.bucket.objects) == set(objects) and task.world.cms.requests == requests, "no second request and no second copy")
    check(all(later.outcome(entry)["outcome"] == "exact_match" for entry in entries.values()), "both outcomes are kept")


def test_a_restored_outcome_of_the_changed_moved_unit_is_kept(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    task, entries = start(monkeypatch, tmp_path, moved_body=b"population,year\nsomething,else\n")
    task.passes()
    later = task.next_task()
    check(harness.execute(later.run)["attempted"] == 0, "the changed unit waits for the owner's review and is not dispatched again")
    check(later.outcome(entries["moved"])["outcome"] == "changed_needs_review", "its label is kept")


def test_a_restored_outcome_that_names_the_capture_of_another_planned_item_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    task, entries = start(monkeypatch, tmp_path)
    task.passes()
    check(harness.execute(task.next_task().run)["attempted"] == 0, "the honest outcomes are accepted first")
    folder = task.run.state_root / "outcomes"
    api, moved = (read_json(folder / f"{entries[name]['snapshot_id']}.json") for name in ("api", "moved"))
    (folder / f"{entries['api']['snapshot_id']}.json").write_bytes(encoded_json(api | {"stored": moved["stored"]}))
    with pytest.raises(ValueError, match="another planned item"):
        harness.execute(task.next_task().run)
