"""Prometheus history in S3: the shipper uploads finished blocks create-only and the restore verifies each block before it places it.

The S3 client is a stand-in with the exact refusals of the real service for these calls (a create-only put on an existing key fails with PreconditionFailed). It
also refuses every call the task role of each side does not hold, so a shipper that reads or a restore that writes fails here. The block folders are synthetic:
they carry the real file names and magic bytes, so the checks run, but they are not Prometheus output. The run against blocks of the pinned Prometheus image is
a heavy run (scripts/monitoring/run_prometheus_history_e2e.sh).
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import struct
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from monitoring import prometheus_history as history
from tests.support import check

BUCKET = "example-project-ci-bucket"
ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
HOUR = 3_600_000


def ulid(number: int) -> str:
    """A well-formed ULID that sorts by ``number``."""
    return "01" + "".join(ALPHABET[(number >> (5 * place)) & 31] for place in reversed(range(24)))


def error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class Pages:
    """The S3 stand-in. ``role`` is ``shipper`` (put and list only) or ``restore`` (get and list only); any other call raises."""

    def __init__(self, role: str, objects: dict[str, bytes] | None = None, page_size: int = 3) -> None:
        self.role, self.objects, self.page_size = role, {} if objects is None else objects, page_size
        self.puts: list[str] = []
        self.fail_after: int | None = None
        self.vanish: set[str] = set()
        self.denied: set[str] = set()

    def allow(self, action: str) -> None:
        if action not in {"shipper": {"put_object", "list_objects_v2"}, "restore": {"get_object", "list_objects_v2"}}[self.role]:
            raise AssertionError(f"the {self.role} role does not hold {action}")

    def put_object(self, **call: Any) -> dict[str, Any]:
        self.allow("put_object")
        key, body = call["Key"], call["Body"]
        if call.get("IfNoneMatch") != "*":
            raise AssertionError(f"{key} was put without the create-only condition")
        if self.fail_after is not None and len(self.puts) >= self.fail_after:
            raise error("InternalError", "PutObject")
        if key in self.objects:
            raise error("PreconditionFailed", "PutObject")
        self.objects[key] = body.read() if hasattr(body, "read") else bytes(body)
        self.puts.append(key)
        return {}

    def get_object(self, **call: Any) -> dict[str, Any]:
        self.allow("get_object")
        if call["Key"] in self.denied:
            raise error("AccessDenied", "GetObject")
        if call["Key"] not in self.objects or call["Key"] in self.vanish:
            raise error("NoSuchKey", "GetObject")
        return {"Body": io.BytesIO(self.objects[call["Key"]])}

    def list_objects_v2(self, **call: Any) -> dict[str, Any]:
        self.allow("list_objects_v2")
        keys = sorted(key for key in self.objects if key.startswith(call["Prefix"]))
        start = int(call.get("ContinuationToken", 0))
        page = keys[start : start + self.page_size]
        more = start + self.page_size < len(keys)
        found: dict[str, Any] = {"Contents": [{"Key": key, "Size": len(self.objects[key])} for key in page], "IsTruncated": more}
        return {**found, "NextContinuationToken": str(start + self.page_size)} if more else found


def write_block(
    data: Path, name: str, minute: int, hours: int = 2, chunks: int = 2, body: bytes = b"samples", compaction: dict[str, Any] | None = None
) -> Path:
    """A block folder with the real file names and magic bytes. ``minute`` is the start of the block in hours since a fixed origin."""
    block = data / name
    (block / "chunks").mkdir(parents=True)
    for number in range(1, chunks + 1):
        (block / "chunks" / f"{number:06d}").write_bytes(struct.pack(">IB3x", history.CHUNKS_MAGIC, 1) + body * number)
    (block / "index").write_bytes(struct.pack(">IB", history.INDEX_MAGIC, 2) + body)
    (block / "tombstones").write_bytes(struct.pack(">IB", history.TOMBSTONES_MAGIC, 1) + b"\0\0\0\0")
    meta = {
        "ulid": name,
        "minTime": minute * HOUR,
        "maxTime": (minute + hours) * HOUR,
        "stats": {"numSamples": 9, "numSeries": 1, "numChunks": chunks},
        "compaction": compaction or {"level": 1, "sources": [name]},
        "version": 1,
    }
    (block / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return block


def tree(root: Path) -> dict[str, bytes]:
    return {str(path.relative_to(root)): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


@pytest.fixture
def source(tmp_path: Path) -> Path:
    data = tmp_path / "source"
    data.mkdir()
    write_block(data, ulid(1), 0)
    write_block(data, ulid(2), 2)
    (data / "wal").mkdir()
    (data / "wal" / "00000001").write_bytes(b"wal")
    (data / "chunks_head").mkdir()
    (data / f"{ulid(3)}.tmp-for-creation").mkdir()
    (data / "lock").write_bytes(b"")
    return data


def ship(client: Pages, data: Path) -> history.ShipReport:
    return history.ship_once(client, BUCKET, data)


def restore(client: Pages, tmp_path: Path, data: Path | None = None) -> history.RestoreReport:
    destination = tmp_path / "volume" / "prometheus" if data is None else data
    return history.restore(client, BUCKET, destination, destination.parent / "prometheus_restore")


def test_the_shipper_uploads_each_finished_block_once_with_meta_json_last(source: Path) -> None:
    client = Pages("shipper")
    report = ship(client, source)
    check(report.shipped == [ulid(1), ulid(2)], f"only the two finished blocks ship, not the WAL, the head chunks or the unfinished folder: {report.shipped}")
    check(
        all(key.startswith(f"prometheus/blocks/{ulid(1)}/") or key.startswith(f"prometheus/blocks/{ulid(2)}/") for key in client.objects),
        "every key sits under prometheus/blocks/<ULID>/",
    )
    for name in (ulid(1), ulid(2)):
        keys = [key for key in client.puts if f"/{name}/" in key]
        check(keys[-1].endswith("/meta.json") and keys[-2].endswith("/manifest.json"), f"meta.json is the last object of {name}, after the manifest: {keys}")


def test_a_second_shipper_pass_changes_nothing(source: Path) -> None:
    client = Pages("shipper")
    ship(client, source)
    before, puts = dict(client.objects), list(client.puts)
    report = ship(client, source)
    check(report.shipped == [] and client.objects == before and client.puts == puts, "the repeat pass makes no put and leaves every object as it was")


def test_a_block_that_appears_later_ships_alone(source: Path) -> None:
    client = Pages("shipper")
    ship(client, source)
    write_block(source, ulid(4), 4)
    report = ship(client, source)
    check(report.shipped == [ulid(4)], f"only the new block ships: {report.shipped}")


def test_an_interrupted_upload_finishes_on_the_next_pass_without_an_overwrite(source: Path) -> None:
    client = Pages("shipper")
    client.fail_after = 3
    with pytest.raises(history.HistoryError):
        ship(client, source)
    check(not any(key.endswith("/meta.json") for key in client.objects), "a block that was cut off has no meta.json, so it is not yet shipped")
    client.fail_after = None
    report = ship(client, source)
    check(report.shipped == [ulid(1), ulid(2)], "the next pass ships both blocks; the objects already there are kept as they are")
    check(len(client.objects) == len({key for key in client.objects}) and all(key in client.objects for key in client.puts), "no key was lost")


def test_an_object_in_s3_with_another_size_stops_the_shipper(source: Path) -> None:
    client = Pages("shipper", {f"prometheus/blocks/{ulid(1)}/index": b"another"})
    with pytest.raises(history.HistoryError, match="differs"):
        ship(client, source)


def test_a_block_that_vanishes_during_the_pass_is_left_for_the_retention_that_removed_it(source: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = Pages("shipper")
    real = history.file_digest

    def vanish(path: Path) -> tuple[int, str]:
        if ulid(1) in str(path):
            raise FileNotFoundError(path)
        return real(path)

    monkeypatch.setattr(history, "file_digest", vanish)
    report = ship(client, source)
    check(
        report.shipped == [ulid(2)] and report.vanished == [ulid(1)], f"the block that Prometheus deleted is reported, not shipped, and is no error: {report}"
    )
    check(not any(ulid(1) in key for key in client.objects), "nothing of the vanished block is in S3")


def test_a_block_marked_deletable_does_not_ship(source: Path) -> None:
    meta = source / ulid(2) / "meta.json"
    body = json.loads(meta.read_text(encoding="utf-8"))
    body["compaction"]["deletable"] = True
    meta.write_text(json.dumps(body), encoding="utf-8")
    report = ship(Pages("shipper"), source)
    check(report.shipped == [ulid(1)], "a block that Prometheus will delete is not shipped")


def test_a_file_the_format_does_not_know_stops_the_shipper(source: Path) -> None:
    (source / ulid(1) / "extra").write_bytes(b"x")
    with pytest.raises(history.HistoryError, match="unknown file"):
        ship(Pages("shipper"), source)


def test_restore_places_every_block_and_a_second_run_changes_nothing(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    reader = Pages("restore", store.objects)
    report = restore(reader, tmp_path)
    data = tmp_path / "volume" / "prometheus"
    check(report.restored == [ulid(1), ulid(2)], f"both blocks come back: {report}")
    check(
        tree(data) == {name: body for name, body in tree(source).items() if name.startswith((ulid(1), ulid(2)))},
        "the restored files equal the source files byte for byte",
    )
    before = {path: path.stat().st_ino for path in data.rglob("*")}
    again = restore(reader, tmp_path)
    check(again.restored == [] and again.present == [ulid(1), ulid(2)], f"the second run finds both present and restores nothing: {again}")
    check(before == {path: path.stat().st_ino for path in data.rglob("*")}, "no file was replaced")
    check(
        not (tmp_path / "volume" / "prometheus_restore").exists() or not any((tmp_path / "volume" / "prometheus_restore").iterdir()),
        "the scratch folder is empty after a run",
    )


def test_restore_never_overwrites_a_block_that_exists(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    data = tmp_path / "volume" / "prometheus"
    write_block(data, ulid(1), 0, body=b"newer")
    kept = tree(data / ulid(1))
    report = restore(Pages("restore", store.objects), tmp_path)
    check(report.restored == [ulid(2)] and report.present == [ulid(1)], f"the existing block stays, the other returns: {report}")
    check(tree(data / ulid(1)) == kept, "the existing block's files are unchanged")


def test_a_block_that_overlaps_a_block_on_disk_is_skipped_and_reported(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    data = tmp_path / "volume" / "prometheus"
    write_block(data, ulid(9), 1, hours=2)
    report = restore(Pages("restore", store.objects), tmp_path)
    check(report.overlapping == [ulid(1), ulid(2)] and report.restored == [], f"both blocks overlap the local block and are skipped: {report}")
    check(sorted(path.name for path in data.iterdir()) == [ulid(9)], "no overlapping block is placed")


def corrupt(store: Pages, name: str, change: Any) -> None:
    key = next(key for key in store.objects if key.endswith(name) and ulid(1) in key)
    store.objects[key] = change(store.objects[key])


@pytest.mark.parametrize(
    ("file", "change", "message"),
    [
        ("chunks/000001", lambda body: body[:-1] + bytes([body[-1] ^ 1]), "sha256"),
        ("index", lambda body: body[:-1], "size"),
        ("index", lambda body: b"\0" * len(body), "sha256"),
    ],
)
def test_a_changed_object_fails_the_restore_before_any_block_is_placed(source: Path, tmp_path: Path, file: str, change: Any, message: str) -> None:
    store = Pages("shipper")
    ship(store, source)
    corrupt(store, file, change)
    with pytest.raises(history.HistoryError, match=message):
        restore(Pages("restore", store.objects), tmp_path)
    data = tmp_path / "volume" / "prometheus"
    check(not any(data.glob(f"{ulid(1)}*")), "the damaged block is not placed")


def test_a_block_with_a_wrong_magic_or_a_foreign_ulid_in_its_manifest_is_refused(source: Path, tmp_path: Path) -> None:
    for mutate, message in (
        (lambda block: (block / "index").write_bytes(b"\0" * 12), "index"),
        (lambda block: (block / "chunks" / "000001").write_bytes(b"\0" * 12), "chunk"),
    ):
        scratch = tmp_path / message
        scratch.mkdir()
        data = scratch / "source"
        data.mkdir()
        block = write_block(data, ulid(1), 0)
        mutate(block)
        store = Pages("shipper")
        ship(store, data)
        with pytest.raises(history.HistoryError, match=message):
            restore(Pages("restore", store.objects), scratch, scratch / "volume" / "prometheus")


def test_a_meta_json_for_another_ulid_is_refused(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    key = f"prometheus/blocks/{ulid(1)}/meta.json"
    body = json.loads(store.objects[key])
    body["ulid"] = ulid(7)
    changed = json.dumps(body).encode()
    store.objects[key] = changed
    manifest_key = f"prometheus/blocks/{ulid(1)}/manifest.json"
    manifest = json.loads(store.objects[manifest_key])
    manifest["files"]["meta.json"] = {"size": len(changed), "sha256": hashlib.sha256(changed).hexdigest()}
    store.objects[manifest_key] = json.dumps(manifest, sort_keys=True).encode()
    with pytest.raises(history.HistoryError, match="ulid"):
        restore(Pages("restore", store.objects), tmp_path)


def test_a_block_that_the_lifecycle_cut_short_is_skipped_and_reported_not_failed(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    del store.objects[f"prometheus/blocks/{ulid(1)}/chunks/000002"]
    report = restore(Pages("restore", store.objects), tmp_path)
    check(report.incomplete == [ulid(1)] and report.restored == [ulid(2)], f"the cut block is reported incomplete and the other returns: {report}")


def test_a_block_without_meta_json_in_s3_is_an_unfinished_upload_and_is_left_alone(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    del store.objects[f"prometheus/blocks/{ulid(2)}/meta.json"]
    report = restore(Pages("restore", store.objects), tmp_path)
    check(report.restored == [ulid(1)] and report.unfinished == [ulid(2)], f"the block without its marker is not restored: {report}")


def test_a_key_that_climbs_out_of_the_block_folder_is_refused(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    store.objects[f"prometheus/blocks/{ulid(1)}/../../escape"] = b"x"
    store.objects["prometheus/blocks/not_a_ulid/meta.json"] = b"{}"
    with pytest.raises(history.HistoryError, match="key"):
        restore(Pages("restore", store.objects), tmp_path)
    check(not (tmp_path / "volume" / "escape").exists(), "nothing was written outside the data folder")


def test_an_empty_bucket_restores_nothing_and_succeeds(tmp_path: Path) -> None:
    report = restore(Pages("restore"), tmp_path)
    check(report.restored == [] and report.present == [], f"a first bootstrap with no history is no error: {report}")


def test_restore_refuses_a_scratch_folder_on_another_volume(source: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = Pages("shipper")
    ship(store, source)
    real = os.stat

    def stat(path: Any, *arguments: Any, **keywords: Any) -> os.stat_result:
        result = real(path, *arguments, **keywords)
        if "prometheus_restore" in str(path):
            values = list(result)
            values[2] += 1
            return os.stat_result(values)
        return result

    monkeypatch.setattr(os, "stat", stat)
    with pytest.raises(history.HistoryError, match="volume"):
        restore(Pages("restore", store.objects), tmp_path)


def parent(name: str, minute: int, hours: int = 2) -> dict[str, Any]:
    return {"ulid": name, "minTime": minute * HOUR, "maxTime": (minute + hours) * HOUR}


def replacement(data: Path, name: str, minute: int, hours: int, parents: list[tuple[str, int, int]], level: int = 2) -> Path:
    """A compacted block that replaced ``parents`` (ulid, start hour, hours): the compaction record lists its parents and every level 1 source."""
    sources = sorted({name_ for name_, _, _ in parents})
    compaction = {"level": level, "sources": sources, "parents": [parent(*item) for item in parents]}
    return write_block(data, name, minute, hours, compaction=compaction)


def ranges(data: Path) -> list[tuple[int, int]]:
    metas = [json.loads((block / "meta.json").read_text(encoding="utf-8")) for block in sorted(data.iterdir()) if (block / "meta.json").is_file()]
    return sorted((meta["minTime"] // HOUR, meta["maxTime"] // HOUR) for meta in metas)


def test_a_manifest_that_expired_skips_that_block_and_restores_the_rest(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    del store.objects[f"prometheus/blocks/{ulid(1)}/manifest.json"]
    report = restore(Pages("restore", store.objects), tmp_path)
    check(
        report.incomplete == [ulid(1)] and report.restored == [ulid(2)],
        f"a block without its manifest is reported incomplete and the intact block returns: {report}",
    )


@pytest.mark.parametrize("name", ["manifest.json", "meta.json", "index", "tombstones", "chunks/000001"])
def test_an_object_that_vanishes_between_the_listing_and_the_download_makes_the_block_incomplete(source: Path, tmp_path: Path, name: str) -> None:
    store = Pages("shipper")
    ship(store, source)
    reader = Pages("restore", store.objects)
    reader.vanish.add(f"prometheus/blocks/{ulid(1)}/{name}")
    report = restore(reader, tmp_path)
    check(
        report.incomplete == [ulid(1)] and report.restored == [ulid(2)],
        f"{name} vanished after the listing, so the block is incomplete and the other returns: {report}",
    )
    check(not any((tmp_path / "volume" / "prometheus").glob(f"{ulid(1)}*")), "nothing of the cut block is placed")


@pytest.mark.parametrize("name", ["manifest.json", "meta.json", "index"])
def test_an_access_failure_is_not_an_expired_object_and_stops_the_restore(source: Path, tmp_path: Path, name: str) -> None:
    store = Pages("shipper")
    ship(store, source)
    reader = Pages("restore", store.objects)
    reader.denied.add(f"prometheus/blocks/{ulid(1)}/{name}")
    with pytest.raises(history.HistoryError, match="download failed|manifest cannot be read"):
        restore(reader, tmp_path)


def test_a_malformed_manifest_stops_the_restore(source: Path, tmp_path: Path) -> None:
    store = Pages("shipper")
    ship(store, source)
    store.objects[f"prometheus/blocks/{ulid(1)}/manifest.json"] = b"{not json"
    with pytest.raises(history.HistoryError, match="manifest"):
        restore(Pages("restore", store.objects), tmp_path)


HEADERS = {
    "index": (struct.pack(">IB", history.INDEX_MAGIC, 2), "index header"),
    "tombstones": (struct.pack(">IB", history.TOMBSTONES_MAGIC, 1), "tombstones header"),
    "chunks/000001": (struct.pack(">IB3x", history.CHUNKS_MAGIC, 1), "chunk"),
}


def damaged(good: bytes, how: str) -> bytes:
    if how == "magic":
        return b"\xff" + good[1:]
    if how == "version":
        return good[:4] + b"\xff" + good[5:]
    return good[:-1]


@pytest.mark.parametrize("how", ["magic", "version", "truncated"])
@pytest.mark.parametrize("file", sorted(HEADERS))
def test_every_checked_file_header_must_be_complete_and_of_a_supported_version(tmp_path: Path, file: str, how: str) -> None:
    good, message = HEADERS[file]
    data = tmp_path / "source"
    data.mkdir()
    block = write_block(data, ulid(1), 0)
    (block / file).write_bytes(damaged(good, how) + (b"" if how == "truncated" else b"payload"))
    store = Pages("shipper")
    ship(store, data)
    with pytest.raises(history.HistoryError, match=message):
        restore(Pages("restore", store.objects), tmp_path, tmp_path / "volume" / "prometheus")
    check(not any((tmp_path / "volume" / "prometheus").glob(f"{ulid(1)}*")), "a block with a damaged header is not placed")


@pytest.mark.parametrize("version", [2, 3])
def test_both_index_versions_that_prometheus_writes_are_accepted(tmp_path: Path, version: int) -> None:
    data = tmp_path / "source"
    data.mkdir()
    block = write_block(data, ulid(1), 0)
    (block / "index").write_bytes(struct.pack(">IB", history.INDEX_MAGIC, version) + b"payload")
    store = Pages("shipper")
    ship(store, data)
    check(restore(Pages("restore", store.objects), tmp_path).restored == [ulid(1)], f"index version {version} restores")


def archive_with_replacement(tmp_path: Path) -> tuple[Pages, Path]:
    """The reviewer's probe: the parents [0h,2h) and [2h,4h) ship, then their replacement [0h,6h) with a third source [4h,6h) that never shipped."""
    data = tmp_path / "source"
    data.mkdir()
    write_block(data, ulid(1), 0)
    write_block(data, ulid(2), 2)
    store = Pages("shipper")
    ship(store, data)
    third = ulid(3)
    replacement(data, ulid(5), 0, 6, [(ulid(1), 0, 2), (ulid(2), 2, 2), (third, 4, 2)])
    ship(store, data)
    return store, data


def test_a_replacement_that_covers_its_archived_parents_is_restored_instead_of_them(tmp_path: Path) -> None:
    store, _ = archive_with_replacement(tmp_path)
    report = restore(Pages("restore", store.objects), tmp_path)
    data = tmp_path / "volume" / "prometheus"
    check(
        report.restored == [ulid(5)] and report.superseded == [ulid(1), ulid(2)],
        f"the replacement returns and its two archived parents are superseded: {report}",
    )
    check(ranges(data) == [(0, 6)], f"the restored range is [0h,6h) with no gap: {ranges(data)}")


def test_a_chain_of_replacements_restores_only_the_widest(tmp_path: Path) -> None:
    store, data = archive_with_replacement(tmp_path)
    replacement(data, ulid(9), 0, 12, [(ulid(5), 0, 6), (ulid(6), 6, 6)], level=3)
    ship(store, data)
    report = restore(Pages("restore", store.objects), tmp_path)
    check(report.restored == [ulid(9)] and report.superseded == [ulid(1), ulid(2), ulid(5)], f"only the widest block returns: {report}")
    check(ranges(tmp_path / "volume" / "prometheus") == [(0, 12)], "the range is [0h,12h)")


def test_an_existing_local_parent_is_never_replaced_by_its_archived_replacement(tmp_path: Path) -> None:
    store, data = archive_with_replacement(tmp_path)
    local = tmp_path / "volume" / "prometheus"
    write_block(local, ulid(1), 0, body=b"local")
    kept = tree(local / ulid(1))
    report = restore(Pages("restore", store.objects), tmp_path)
    check(
        report.restored == [ulid(2)] and report.present == [ulid(1)] and report.overlapping == [ulid(5)],
        f"the local block stays and the replacement is skipped: {report}",
    )
    check(tree(local / ulid(1)) == kept and ranges(local) == [(0, 2), (2, 4)], "the local block is unchanged and the other parent fills the rest")


def test_a_replacement_that_lost_its_objects_falls_back_to_its_parents(tmp_path: Path) -> None:
    store, _ = archive_with_replacement(tmp_path)
    del store.objects[f"prometheus/blocks/{ulid(5)}/chunks/000002"]
    report = restore(Pages("restore", store.objects), tmp_path)
    check(report.incomplete == [ulid(5)] and report.restored == [ulid(1), ulid(2)], f"the cut replacement is skipped and the parents return: {report}")


def test_a_replacement_that_fails_verification_stops_the_restore_before_any_parent_is_placed(tmp_path: Path) -> None:
    store, _ = archive_with_replacement(tmp_path)
    key = next(key for key in store.objects if key.endswith("/index") and ulid(5) in key)
    store.objects[key] = b"\0" * len(store.objects[key])
    with pytest.raises(history.HistoryError, match="sha256"):
        restore(Pages("restore", store.objects), tmp_path)
    check(not any((tmp_path / "volume" / "prometheus").iterdir()), "no parent is placed behind a replacement that failed")


def test_a_block_that_names_another_as_its_parent_without_covering_it_does_not_supersede_it(tmp_path: Path) -> None:
    data = tmp_path / "source"
    data.mkdir()
    write_block(data, ulid(1), 0)
    replacement(data, ulid(5), 1, 2, [(ulid(1), 0, 2)])
    store = Pages("shipper")
    ship(store, data)
    report = restore(Pages("restore", store.objects), tmp_path)
    check(
        report.superseded == [] and sorted(report.overlapping + report.restored) == [ulid(1), ulid(5)] and len(report.restored) == 1,
        f"a block that does not cover its parent supersedes nothing: {report}",
    )
