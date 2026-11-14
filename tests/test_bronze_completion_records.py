"""Bronze loads exactly the objects the newest completion record of each unit names, or the last capture a newer stale record points to.

The records are built here by an independent builder from the written schema, so the reader is never checked against its own parser.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import stale_record
from scripts.lakehouse import bronze, bronze_metadata, bronze_pin
from tests.support import check

BUCKET = "bucket"
COLLECTION = "state/hospitals"
OTHER_COLLECTION = "state/clinics"
UNIT_A = hashlib.sha256(b"unit a").hexdigest()
UNIT_B = hashlib.sha256(b"unit b").hexdigest()
UNIT_C = hashlib.sha256(b"unit c").hexdigest()
CODE = hashlib.sha256(b"code").hexdigest()
BUNDLE = hashlib.sha256(b"bundle").hexdigest()
OLD, MID, NEW = "2026-11-01T10:00:00Z", "2026-11-05T10:00:00Z", "2026-11-09T10:00:00Z"
ROWS_OLD, ROWS_NEW, ROWS_LEGACY = b"a,b\n1,2\n", b"a,b\n3,4\n", b"a,b\n5,6\n"


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Lake:
    """Versioned objects in memory standing in for S3. A role without the test-folder grant gets AccessDenied, like the real bronze role."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.listed: list[str] = []
        self.read: list[str] = []
        self.unlisted: set[tuple[str, str]] = set()

    def put(self, key: str, data: bytes) -> str:
        version = f"v-{digest(key.encode() + data)[:12]}"
        self.objects[(key, version)] = data
        return version

    def refuse_test_folder(self, name: str) -> None:
        if name.startswith("ingestion_test/") or name.startswith("raw/ingestion_test/"):
            raise PermissionError(f"AccessDenied: the bronze role has no grant on {name}")

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        self.listed.append(prefix)
        self.refuse_test_folder(prefix)
        return sorted(
            (key, version) for key, version in self.objects if key.startswith(prefix) and key.endswith("/manifest.json") and (key, version) not in self.unlisted
        )

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        self.listed.append(prefix)
        self.refuse_test_folder(prefix)
        return sorted((key, version) for key, version in self.objects if key.startswith(prefix))

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        self.read.append(key)
        self.refuse_test_folder(key)
        return self.objects[(key, version_id)]

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        self.read.append(key)
        self.refuse_test_folder(key)
        path.write_bytes(self.objects[(key, version_id)])


def capture(
    lake: Lake,
    *,
    root: str,
    snapshot: str,
    files: list[tuple[str, str, bytes]],
    collection: str = COLLECTION,
    alias: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Store data and other objects, a receipt and a manifest the way the acquisition storage step does. Return what a record would name.

    With ``alias`` the manifest also lists each data object again under ``alias["role"]``, as the storage writer does for a response body it keeps as
    transport evidence, and ``alias["object"]`` changes fields of that second entry's object.
    """
    objects: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    for role, name, data in files:
        sha = digest(data)
        key = f"{root}{collection}/datasets/ca/release_date=2026-01-01/{sha}/{name}"
        version = lake.put(key, data)
        objects.append({"role": role, "key": key, "version_id": version, "sha256": sha, "byte_count": len(data), "stored_file_name": name})
        entries.append(
            {"role": role, "dataset_id": "ca", "object": {"bucket": BUCKET, "key": key, "version_id": version, "sha256": sha, "byte_count": len(data)}}
        )
        if alias is not None and role == "data":
            entries.append({"role": alias["role"], "dataset_id": "ca", "object": {**entries[-1]["object"], **alias.get("object", {})}})
    base = f"{root}{collection}/manifests/release_date=2026-01-01/{snapshot}/ca"
    receipt_bytes = canonical({"snapshot_id": snapshot})
    receipt = {
        "key": f"{base}/receipt.json",
        "version_id": lake.put(f"{base}/receipt.json", receipt_bytes),
        "sha256": digest(receipt_bytes),
        "byte_count": len(receipt_bytes),
    }
    manifest = {"snapshot_id": snapshot, "source_id": "CA", "dataset_id": "ca", "release_id": "2026-01-01", "objects": entries}
    manifest_bytes = json.dumps(manifest).encode()
    manifest_ref = {"key": f"{base}/manifest.json", "version_id": lake.put(f"{base}/manifest.json", manifest_bytes), "sha256": digest(manifest_bytes)}
    return {"objects": objects, "receipt": receipt, "manifest": manifest_ref | {"byte_count": len(manifest_bytes)}, "collection": collection}


def plain(item: dict[str, Any]) -> dict[str, Any]:
    return {name: item[name] for name in ("key", "version_id", "sha256", "byte_count")}


def completion_body(stored: dict[str, Any], unit: str, retrieved: str, names: list[str] | None = None) -> dict[str, Any]:
    chosen = [item for item in stored["objects"] if names is None or item["stored_file_name"] in names]
    data = [item for item in chosen if item["role"] == "data"]
    triples = sorted(["data", item["stored_file_name"], item["sha256"]] for item in data)
    return {
        "kind": "ingestion_unit_completion",
        "schema_version": 1,
        "unit_key": unit,
        "payload_sha256": digest(canonical(triples)),
        "collection": stored["collection"],
        "source_id": "CA",
        "mode": "direct",
        "run_id": "20261110T000000Z-aaaaaaaa",
        "code_sha256": CODE,
        "bundle_sha256": BUNDLE,
        "retrieved_at_utc": retrieved,
        "receipt": stored["receipt"],
        "manifests": [plain(stored["manifest"])],
        "data_objects": [{"role": "data", **plain(item), "stored_file_name": item["stored_file_name"]} for item in data],
        "other_objects": [{"role": item["role"], **plain(item)} for item in chosen if item["role"] != "data"],
    }


def write_completion(
    lake: Lake,
    stored: dict[str, Any],
    unit: str,
    retrieved: str,
    *,
    root: str = "raw/",
    names: list[str] | None = None,
    change: Callable[[dict], None] | None = None,
    rekey: bool = False,
    run: str | None = None,
) -> str:
    """Store a record. A change that keeps the record consistent with its own key (rekey) can only be caught by checking the record against its manifest.

    With ``run`` the record is a reversion record: that run's ID is in the body and in the key's name.
    """
    body = completion_body(stored, unit, retrieved, names)
    if run is not None:
        body["run_id"] = run
    tail = f".{run}" if run is not None else ""
    key = f"{root}ingestion_units/{unit}/{body['payload_sha256']}{tail}.json"
    if change is not None:
        change(body)
    if rekey:
        triples = sorted(["data", item["stored_file_name"], item["sha256"]] for item in body["data_objects"])
        body["payload_sha256"] = digest(canonical(triples))
        key = f"{root}ingestion_units/{unit}/{body['payload_sha256']}{tail}.json"
    lake.put(key, canonical(body) + b"\n")
    return key


def pointer(item: dict[str, Any], retrieved: str, read_root: str) -> dict[str, Any]:
    return {"role": item["role"], **plain(item), "stored_file_name": item["stored_file_name"], "retrieved_at_utc": retrieved, "read_root": read_root}


def write_stale(
    lake: Lake,
    unit: str,
    run: str,
    attempted: str,
    last_capture: list[dict[str, Any]],
    *,
    verified: bool | None = None,
    collection: str = COLLECTION,
    reason: str = "http_status",
    change: Callable[[dict], None] | None = None,
    key_run: str | None = None,
) -> str:
    """Store a stale record built by the real writer's own builder. A change edits the checked body, so a malformed record can be stored."""
    named: list[Mapping[str, Any]] = [*last_capture]
    body = stale_record.build(
        unit_key=unit,
        run_id=run,
        collection=collection,
        source_id="CA",
        snapshot_id=f"CA__{run}",
        attempted_at_utc=attempted,
        reason=reason,
        http_status=403 if reason == "http_status" else None,
        code_sha256=CODE,
        bundle_sha256=BUNDLE,
        last_capture_verified=bool(last_capture) if verified is None else verified,
        last_capture=named,
    )
    key = stale_record.stale_key("raw/", unit, run)
    if change is not None:
        change(body)
        key = f"raw/ingestion_stale/{unit}/{key_run or run}.json"
        lake.put(key, canonical(body) + b"\n")
        return key
    lake.put(key, stale_record.encode(body))
    return key


def config(tmp_path: Path, *, collection: str = COLLECTION) -> dict:
    tables = [
        {"table": "hospitals", "group": "r", "collection": collection, "member_pattern": "data\\.csv$", "format": "csv"},
        {"table": "hospital_notes", "group": "r", "collection": collection, "member_pattern": "notes\\.csv$", "format": "csv", "roles": ["dictionary"]},
    ]
    path = tmp_path / "tables.json"
    path.write_text(json.dumps({"version": 1, "namespace": "bronze", "tables": tables}))
    return bronze.load_table_map(path)


def discover(lake: Lake, tmp_path: Path, names: list[str] | None = None) -> list[dict]:
    return bronze.discover(lake, BUCKET, config(tmp_path), names or ["hospitals"])[0]


@pytest.fixture(autouse=True)
def record_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")


def refused(action: Callable[[], object], *fragments: str) -> bool:
    try:
        action()
    except bronze.BronzeError as error:
        return all(fragment in str(error) for fragment in fragments)
    return False


@pytest.mark.parametrize("old_bytes,new_bytes", [(ROWS_OLD, ROWS_NEW), (ROWS_NEW, ROWS_OLD)])
def test_two_captures_of_one_unit_load_only_the_newest_records_objects(tmp_path: Path, old_bytes: bytes, new_bytes: bytes) -> None:
    lake = Lake()
    old = capture(lake, root="raw/", snapshot="CA__old", files=[("data", "data.csv", old_bytes)])
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", new_bytes)])
    write_completion(lake, old, UNIT_A, OLD)
    write_completion(lake, new, UNIT_A, NEW)
    inputs = discover(lake, tmp_path)
    check(
        [(item["key"], item["version_id"]) for item in inputs] == [(new["objects"][0]["key"], new["objects"][0]["version_id"])],
        "only the newest record's object loads",
    )
    check(inputs[0]["freshness"] == "current" and inputs[0]["last_retrieved_at_utc"] == NEW, "a current object carries its record's retrieval time")


def test_each_unit_loads_its_own_newest_capture(tmp_path: Path) -> None:
    lake = Lake()
    two_files = capture(lake, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_OLD), ("data", "extra_data.csv", ROWS_LEGACY)])
    other = capture(lake, root="raw/", snapshot="CA__b", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, two_files, UNIT_A, NEW)
    write_completion(lake, other, UNIT_B, NEW)
    keys = {item["key"] for item in discover(lake, tmp_path)}
    check(keys == {item["key"] for item in two_files["objects"]} | {other["objects"][0]["key"]}, "each unit loads the objects its own record names")


def test_an_object_the_manifest_lists_but_the_record_does_not_name_stops_the_table_by_name(tmp_path: Path) -> None:
    lake = Lake()
    two_files = capture(lake, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_OLD), ("data", "extra_data.csv", ROWS_LEGACY)])
    write_completion(lake, two_files, UNIT_A, NEW, names=["data.csv"])
    check(
        refused(lambda: discover(lake, tmp_path), "hospitals", "named by no completion or stale record", two_files["objects"][1]["key"]),
        "an object no record names is neither loaded nor dropped silently",
    )


def test_a_record_loads_the_other_roles_it_names_for_the_tables_that_take_them(tmp_path: Path) -> None:
    lake = Lake()
    stored = capture(lake, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", b"name,meaning\nx,y\n")])
    write_completion(lake, stored, UNIT_A, NEW)
    inputs = discover(lake, tmp_path, ["hospitals", "hospital_notes"])
    check(
        {(item["table"], item["file_name"]) for item in inputs} == {("hospitals", "data.csv"), ("hospital_notes", "notes.csv")},
        "each table gets its named role",
    )


def test_a_stale_unit_loads_its_last_capture_with_the_label(tmp_path: Path) -> None:
    lake = Lake()
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, new, UNIT_A, MID)
    stale_key = write_stale(lake, UNIT_A, "20261109T000000Z-bbbbbbbb", NEW, [pointer(new["objects"][0], MID, "raw/")], reason="robots_disallowed")
    inputs = discover(lake, tmp_path)
    check([item["key"] for item in inputs] == [new["objects"][0]["key"]], "the newer stale record loads the newest completion's objects")
    item = inputs[0]
    check(item["freshness"] == "stale" and item["stale_reason"] == "robots_disallowed", "the label and the reason are carried")
    check(item["stale_record"]["key"] == stale_key and item["stale_record"]["version_id"].startswith("v-"), "the stale record is named by key and version")
    check(item["last_retrieved_at_utc"] == MID, "the last capture's own retrieval time is carried")


def test_a_unit_with_only_a_stale_record_loads_its_last_capture_and_an_older_stale_record_does_not_hide_a_newer_completion(tmp_path: Path) -> None:
    lake = Lake()
    legacy = capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    write_stale(lake, UNIT_A, "20261101T000000Z-cccccccc", OLD, [pointer(legacy["objects"][0], OLD, "")])
    write_stale(lake, UNIT_B, "20261101T000000Z-dddddddd", OLD, [pointer(legacy["objects"][0], OLD, "")])
    write_completion(lake, new, UNIT_B, NEW)
    by_unit = {item["freshness"]: item["key"] for item in discover(lake, tmp_path)}
    check(by_unit == {"stale": legacy["objects"][0]["key"], "current": new["objects"][0]["key"]}, "one stale unit and one current unit")


def test_a_completion_exactly_as_new_as_the_stale_record_counts_as_stale(tmp_path: Path) -> None:
    lake = Lake()
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, new, UNIT_A, NEW)
    write_stale(lake, UNIT_A, "20261109T000000Z-eeeeeeee", NEW, [pointer(new["objects"][0], NEW, "raw/")])
    check([item["freshness"] for item in discover(lake, tmp_path)] == ["stale"], "current needs a strictly newer completion")


@pytest.mark.parametrize("case", ["unverified", "empty", "absent_from_manifests", "wrong_digest"])
def test_a_missing_last_capture_fails_the_table_by_name(tmp_path: Path, case: str) -> None:
    lake = Lake()
    legacy = capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    pointed = pointer(legacy["objects"][0], OLD, "")
    if case == "absent_from_manifests":
        pointed["key"] = pointed["key"].replace("data.csv", "gone.csv")
    if case == "wrong_digest":
        pointed["sha256"] = digest(b"other bytes")
    write_stale(lake, UNIT_A, "20261109T000000Z-ffffffff", NEW, [] if case == "empty" else [pointed], verified=case not in {"unverified", "empty"})
    check(refused(lambda: discover(lake, tmp_path), "hospitals", UNIT_A, "last capture"), f"{case}: the error names the table, the unit and the last capture")


def test_a_unit_of_another_collection_without_a_last_capture_does_not_fail_this_table(tmp_path: Path) -> None:
    lake = Lake()
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, new, UNIT_A, NEW)
    write_stale(lake, UNIT_C, "20261109T000000Z-99999999", NEW, [], verified=False, collection=OTHER_COLLECTION)
    check(len(discover(lake, tmp_path)) == 1, "only the selected tables' collections must have a loadable unit")


def test_a_table_with_no_record_for_its_collection_fails_by_name_instead_of_loading_nothing(tmp_path: Path) -> None:
    lake = Lake()
    capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    check(
        refused(lambda: discover(lake, tmp_path), "hospitals", "no completion or stale record"),
        "a legacy root capture without a record loads nothing and says so",
    )


def forge_unit(body: dict) -> None:
    body["unit_key"] = UNIT_B


def forge_digest(body: dict) -> None:
    body["data_objects"][0]["sha256"] = digest(b"forged")


def forge_manifest_sha(body: dict) -> None:
    body["manifests"][0]["sha256"] = digest(b"forged manifest")


def forge_object_version(body: dict) -> None:
    body["data_objects"][0]["version_id"] = "v-forged"


def forge_missing_manifest_version(body: dict) -> None:
    body["manifests"][0]["version_id"] = "v-missing"


def forge_extra_field(body: dict) -> None:
    body["extra"] = 1


def forge_unnamed_object(body: dict) -> None:
    body["data_objects"][0]["key"] = body["data_objects"][0]["key"].replace("data.csv", "gone.csv")


FORGERIES = [forge_unit, forge_digest, forge_manifest_sha, forge_object_version, forge_missing_manifest_version, forge_extra_field, forge_unnamed_object]


@pytest.mark.parametrize("forgery", FORGERIES, ids=lambda function: function.__name__)
def test_a_forged_completion_record_fails_visibly_and_is_never_skipped(tmp_path: Path, forgery: Callable[[dict], None]) -> None:
    lake = Lake()
    good = capture(lake, root="raw/", snapshot="CA__good", files=[("data", "data.csv", ROWS_OLD)])
    write_completion(lake, good, UNIT_C, OLD)
    forged = capture(lake, root="raw/", snapshot="CA__forged", files=[("data", "data.csv", ROWS_NEW)])
    key = write_completion(lake, forged, UNIT_A, NEW, change=forgery)
    check(refused(lambda: discover(lake, tmp_path), key), f"{forgery.__name__}: the run stops and names the record")


def test_a_self_consistent_record_that_disagrees_with_its_manifest_fails_visibly(tmp_path: Path) -> None:
    lake = Lake()
    stored = capture(lake, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", b"name,meaning\nx,y\n")])
    key = write_completion(lake, stored, UNIT_A, NEW, change=lambda body: body["data_objects"][0].update(sha256=digest(b"another file")), rekey=True)
    check(refused(lambda: discover(lake, tmp_path), key, "differs from its manifest"), "a digest that the manifest does not list is refused")
    lake2 = Lake()
    stored2 = capture(lake2, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", b"name,meaning\nx,y\n")])
    key2 = write_completion(lake2, stored2, UNIT_A, NEW, change=lambda body: body["other_objects"][0].update(role="reference"))
    check(
        refused(lambda: discover(lake2, tmp_path, ["hospitals", "hospital_notes"]), key2, "differs from its manifest"),
        "a role that the manifest does not give is refused",
    )


def test_a_record_that_names_an_object_in_the_test_folder_fails_visibly(tmp_path: Path) -> None:
    lake = Lake()
    good = capture(lake, root="raw/", snapshot="CA__good", files=[("data", "data.csv", ROWS_OLD)])
    key = write_completion(
        lake,
        good,
        UNIT_A,
        OLD,
        change=lambda body: body["data_objects"][0].update(key="ingestion_test/20261110T000000Z-aaaaaaaa/" + body["data_objects"][0]["key"]),
    )
    check(refused(lambda: discover(lake, tmp_path), key, "test folder"), "a record never makes bronze read the test folder")


def test_a_stale_pointer_to_another_version_of_the_object_counts_as_a_missing_last_capture(tmp_path: Path) -> None:
    lake = Lake()
    legacy = capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    pointed = pointer(legacy["objects"][0], OLD, "")
    pointed["version_id"] = "v-another"
    write_stale(lake, UNIT_A, "20261109T000000Z-13131313", NEW, [pointed])
    check(refused(lambda: discover(lake, tmp_path), "hospitals", UNIT_A, "last capture"), "the exact version must be listed by a manifest")


def test_a_valid_record_of_another_collection_neither_loads_nor_fails_this_run(tmp_path: Path) -> None:
    lake = Lake()
    mine = capture(lake, root="raw/", snapshot="CA__mine", files=[("data", "data.csv", ROWS_NEW)])
    other = capture(lake, root="raw/", snapshot="CA__other", files=[("data", "data.csv", ROWS_OLD)], collection=OTHER_COLLECTION)
    write_completion(lake, mine, UNIT_A, NEW)
    write_completion(lake, other, UNIT_C, NEW)
    check([item["key"] for item in discover(lake, tmp_path)] == [mine["objects"][0]["key"]], "only the selected tables' collections are read")


def test_an_explicit_root_list_keeps_the_manifest_listing_even_with_the_switch_on(tmp_path: Path) -> None:
    lake = Lake()
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    inputs = bronze.discover(lake, BUCKET, config(tmp_path), ["hospitals"], roots=("raw/",))[0]
    check(
        [item["key"] for item in inputs] == [new["objects"][0]["key"]] and "freshness" not in inputs[0], "no record is needed when the caller names its roots"
    )


def test_an_unreadable_record_and_a_record_under_a_wrong_name_fail_visibly(tmp_path: Path) -> None:
    lake = Lake()
    good = capture(lake, root="raw/", snapshot="CA__good", files=[("data", "data.csv", ROWS_OLD)])
    write_completion(lake, good, UNIT_A, OLD)
    lake.put(f"raw/ingestion_units/{UNIT_B}/{digest(b'x')}.json", b"not json at all")
    check(refused(lambda: discover(lake, tmp_path), f"raw/ingestion_units/{UNIT_B}/"), "an unreadable record stops the run")
    lake2 = Lake()
    write_completion(lake2, good, UNIT_A, OLD)
    lake2.put(f"raw/ingestion_units/{UNIT_B}/notes.txt", b"{}")
    check(refused(lambda: discover(lake2, tmp_path), "ingestion_units"), "an object in a unit folder that is no record stops the run")


def test_a_forged_or_unreadable_stale_record_fails_visibly(tmp_path: Path) -> None:
    lake = Lake()
    legacy = capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    key = write_stale(lake, UNIT_A, "20261109T000000Z-12121212", NEW, [pointer(legacy["objects"][0], OLD, "")])
    lake.put(key, lake.objects[next(identity for identity in lake.objects if identity[0] == key)].replace(UNIT_A.encode(), UNIT_B.encode()))
    check(refused(lambda: discover(lake, tmp_path), key), "a stale record whose unit differs from its key is refused")
    lake2 = Lake()
    lake2.put(f"raw/ingestion_stale/{UNIT_A}/20261109T000000Z-34343434.json", b"{broken")
    check(refused(lambda: discover(lake2, tmp_path), "ingestion_stale"), "an unreadable stale record is refused")


def test_two_completion_records_with_one_retrieval_time_are_ambiguous(tmp_path: Path) -> None:
    lake = Lake()
    first = capture(lake, root="raw/", snapshot="CA__first", files=[("data", "data.csv", ROWS_OLD)])
    second = capture(lake, root="raw/", snapshot="CA__second", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, first, UNIT_A, NEW)
    write_completion(lake, second, UNIT_A, NEW)
    check(refused(lambda: discover(lake, tmp_path), "hospitals", UNIT_A, "same retrieval time"), "no silent pick between two newest records")


def test_the_test_folder_is_never_listed_or_read(tmp_path: Path) -> None:
    lake = Lake()
    test_capture = capture(lake, root="ingestion_test/20261110T000000Z-aaaaaaaa/", snapshot="CA__test", files=[("data", "data.csv", ROWS_LEGACY)])
    write_completion(lake, test_capture, UNIT_B, NEW, root="ingestion_test/20261110T000000Z-aaaaaaaa/")
    good = capture(lake, root="raw/", snapshot="CA__good", files=[("data", "data.csv", ROWS_OLD)])
    write_completion(lake, good, UNIT_A, OLD)
    inputs = discover(lake, tmp_path)
    check([item["key"] for item in inputs] == [good["objects"][0]["key"]], "only the raw/ record loads")
    check(not any("ingestion_test" in name for name in [*lake.listed, *lake.read]), "no listing or read touched the test folder")
    check(
        sorted(set(lake.listed)) == ["raw/ingestion_stale/", "raw/ingestion_units/"]
        or all(prefix.startswith(("raw/ingestion_", "raw/state/")) or prefix.startswith("state/") for prefix in lake.listed),
        "listings stay under the two record folders and the collection",
    )


def test_the_loader_report_lists_every_labelled_object_with_its_identity_and_retrieval_time(tmp_path: Path) -> None:
    lake = Lake()
    legacy = capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, new, UNIT_A, NEW)
    another = capture(lake, root="raw/", snapshot="CA__another", files=[("data", "data.csv", ROWS_OLD)])
    write_completion(lake, another, UNIT_C, MID)
    stale_key = write_stale(lake, UNIT_B, "20261109T000000Z-56565656", NEW, [pointer(legacy["objects"][0], OLD, "")], reason="network")
    report = bronze.freshness_report(discover(lake, tmp_path))
    check(report["current_objects"] == 2 and report["stale_objects"] == 1, "the run report counts both kinds")
    check(len(report["objects"]) == 3, "one row for each labelled object")
    check([row["key"] for row in report["objects"]] == sorted(row["key"] for row in report["objects"]), "the rows come in a fixed order")
    current = next(row for row in report["objects"] if row["key"] == new["objects"][0]["key"])
    stale = next(row for row in report["objects"] if row["freshness"] == "stale")
    check(
        (current["table"], current["key"], current["version_id"], current["sha256"])
        == ("hospitals", *(new["objects"][0][name] for name in ("key", "version_id", "sha256"))),
        "a current row names its table and exact object",
    )
    check(
        current["last_retrieved_at_utc"] == NEW and current["stale_reason"] is None and current["stale_record"] is None,
        "a current row carries its record's time and no stale fields",
    )
    check(
        stale["stale_record"]["key"] == stale_key
        and stale["last_retrieved_at_utc"] == OLD
        and stale["stale_reason"] == "network"
        and stale["key"] == legacy["objects"][0]["key"],
        "a stale row names its record, reason, object and the last capture's retrieval time",
    )


def test_a_pinned_inventory_replays_the_record_selection_exactly(tmp_path: Path) -> None:
    lake = Lake()
    legacy = capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, new, UNIT_A, NEW)
    write_stale(lake, UNIT_B, "20261109T000000Z-78787878", NEW, [pointer(legacy["objects"][0], OLD, "")])
    inventory = bronze_pin.build_inventory(lake, BUCKET, config(tmp_path), ["hospitals"])
    later = capture(lake, root="raw/", snapshot="CA__later", files=[("data", "data.csv", ROWS_OLD)])
    write_completion(lake, later, UNIT_A, "2026-11-10T10:00:00Z")
    pinned = bronze_pin.PinnedStorage(lake, inventory)
    replay = bronze.discover(pinned, BUCKET, config(tmp_path), ["hospitals"])[0]
    bronze_pin.verify_replay(inventory, replay)
    check(
        sorted(item["key"] for item in replay) == sorted([legacy["objects"][0]["key"], new["objects"][0]["key"]]),
        "a record that arrives after the pin is not part of the run",
    )


def test_the_switch_off_keeps_the_root_listing_and_never_touches_a_record_folder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_READ_RAW_ROOT", raising=False)
    lake = Lake()
    legacy = capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, new, UNIT_A, NEW)
    write_stale(lake, UNIT_B, "20261109T000000Z-90909090", NEW, [])
    inputs = discover(lake, tmp_path)
    check([item["key"] for item in inputs] == [legacy["objects"][0]["key"]], "the root capture loads exactly as before")
    check(all("freshness" not in item for item in inputs), "an input carries no freshness field while the switch is off")
    check(bronze.freshness_report(inputs) == {}, "the report has no freshness section while the switch is off")
    check(
        not any("ingestion_" in name for name in [*lake.listed, *lake.read]) and not any(name.startswith("raw/") for name in lake.listed),
        "no record folder or raw folder was touched",
    )
    check(
        bronze.discover(lake, BUCKET, config(tmp_path), ["hospitals"], roots=("raw/",))[0][0]["key"] == new["objects"][0]["key"],
        "an explicit root list still lists manifests",
    )


def stale_with(change: Callable[[dict], None], *, key_run: str | None = None) -> tuple[Lake, str]:
    lake = Lake()
    legacy = capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY)])
    key = write_stale(lake, UNIT_A, "20261109T000000Z-abababab", NEW, [pointer(legacy["objects"][0], OLD, "")], change=change, key_run=key_run)
    return lake, key


def last_capture_item(**fields: Any) -> Callable[[dict], None]:
    return lambda body: body["last_capture"][0].update(fields)


STALE_MALFORMED = {
    "extra_field": lambda body: body.update(extra=1),
    "missing_snapshot_id": lambda body: body.pop("snapshot_id"),
    "empty_snapshot_id": lambda body: body.update(snapshot_id=""),
    "status_below_100": lambda body: body.update(http_status=99),
    "status_above_599": lambda body: body.update(http_status=600),
    "status_boolean": lambda body: body.update(http_status=True),
    "status_text": lambda body: body.update(http_status="403"),
    "code_digest_short": lambda body: body.update(code_sha256="abc"),
    "bundle_digest_upper": lambda body: body.update(bundle_sha256="A" * 64),
    "reason_outside_the_list": lambda body: body.update(reason="timeout"),
    "kind_other": lambda body: body.update(kind="ingestion_unit_completion"),
    "verified_not_boolean": lambda body: body.update(last_capture_verified="yes"),
    "verified_without_objects": lambda body: body.update(last_capture=[], last_capture_verified=True),
    "attempt_with_fractional_seconds": lambda body: body.update(attempted_at_utc="2026-11-09T10:00:00.5Z"),
    "attempt_with_an_offset": lambda body: body.update(attempted_at_utc="2026-11-09T10:00:00+00:00"),
    "attempt_on_an_invalid_date": lambda body: body.update(attempted_at_utc="2026-13-45T25:61:61Z"),
    "pointer_time_on_an_invalid_date": last_capture_item(retrieved_at_utc="2026-02-30T10:00:00Z"),
    "pointer_time_with_fractional_seconds": last_capture_item(retrieved_at_utc="2026-11-01T10:00:00.5Z"),
    "pointer_version_null": last_capture_item(version_id="null"),
    "pointer_byte_count_negative": last_capture_item(byte_count=-1),
    "pointer_byte_count_boolean": last_capture_item(byte_count=True),
    "pointer_digest_short": last_capture_item(sha256="abc"),
    "pointer_extra_field": last_capture_item(extra=1),
    "pointer_without_stored_file_name": lambda body: body["last_capture"][0].pop("stored_file_name"),
    "pointer_root_unknown": last_capture_item(read_root="other/"),
    "pointer_key_outside_its_root": last_capture_item(read_root="raw/"),
    "pointer_raw_key_under_the_empty_root": last_capture_item(key="raw/state/hospitals/datasets/x.csv"),
    "pointer_into_the_test_folder": lambda body: body["last_capture"][0].update(read_root="raw/", key="raw/ingestion_test/20261110T000000Z-aaaaaaaa/x.csv"),
}


@pytest.mark.parametrize("name", sorted(STALE_MALFORMED))
def test_a_malformed_stale_record_of_a_selected_collection_fails_naming_the_table_and_the_record(tmp_path: Path, name: str) -> None:
    lake, key = stale_with(STALE_MALFORMED[name])
    check(refused(lambda: discover(lake, tmp_path), "hospitals", key), f"{name}: the error names the table and the record")


@pytest.mark.parametrize("run", ["a/../b", "..", "a b", "x" * 101])
def test_a_stale_record_under_an_unsafe_run_id_fails_naming_the_record(tmp_path: Path, run: str) -> None:
    lake, key = stale_with(lambda body: body.update(run_id=run), key_run=run)
    check(refused(lambda: discover(lake, tmp_path), "hospitals", key), "an unsafe run id is refused")


def test_a_malformed_record_of_another_collection_does_not_stop_this_collections_tables(tmp_path: Path) -> None:
    lake = Lake()
    mine = capture(lake, root="raw/", snapshot="CA__mine", files=[("data", "data.csv", ROWS_NEW)])
    other = capture(lake, root="raw/", snapshot="CA__other", files=[("data", "data.csv", ROWS_OLD)], collection=OTHER_COLLECTION)
    write_completion(lake, mine, UNIT_A, NEW)
    write_completion(lake, other, UNIT_C, NEW, change=forge_extra_field)
    write_stale(lake, UNIT_B, "20261109T000000Z-cdcdcdcd", NEW, [], collection=OTHER_COLLECTION, change=lambda body: body.update(extra=1))
    check([item["key"] for item in discover(lake, tmp_path)] == [mine["objects"][0]["key"]], "records of other collections are not this run's concern")


def test_a_record_that_cannot_be_read_at_all_stops_the_run_because_its_collection_is_unknown(tmp_path: Path) -> None:
    lake = Lake()
    mine = capture(lake, root="raw/", snapshot="CA__mine", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, mine, UNIT_A, NEW)
    lake.put(f"raw/ingestion_stale/{UNIT_B}/20261109T000000Z-efefefef.json", b"[1, 2]")
    check(refused(lambda: discover(lake, tmp_path), "ingestion_stale", UNIT_B), "a record with no readable collection cannot be skipped")


@pytest.mark.parametrize("retrieved", ["2026-11-09T10:00:00Z", "2026-11-09T10:00:00.123456+00:00", "2026-11-09T02:00:00-08:00", "2026-11-09T10:00:00+00:00"])
def test_a_completion_time_in_the_form_the_capture_writer_stamps_is_accepted_and_ordered(tmp_path: Path, retrieved: str) -> None:
    lake = Lake()
    old = capture(lake, root="raw/", snapshot="CA__old", files=[("data", "data.csv", ROWS_OLD)])
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, old, UNIT_A, "2026-11-01T10:00:00.000001+00:00")
    write_completion(lake, new, UNIT_A, retrieved)
    inputs = discover(lake, tmp_path)
    check(
        [item["key"] for item in inputs] == [new["objects"][0]["key"]] and inputs[0]["last_retrieved_at_utc"] == retrieved, "the newer time wins in every form"
    )


@pytest.mark.parametrize("retrieved", ["2026-13-45T25:61:61Z", "2026-11-09", "2026-11-09T10:00:00", "yesterday"])
def test_a_completion_time_that_is_not_a_utc_time_fails_naming_the_record(tmp_path: Path, retrieved: str) -> None:
    lake = Lake()
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW)])
    key = write_completion(lake, new, UNIT_A, retrieved)
    check(refused(lambda: discover(lake, tmp_path), "hospitals", key), "a bad time is a named refusal and never a bare ValueError")


NOTES = b"name,meaning\nx,y\n"
NOTES_V2 = b"name,meaning\nx,z\n"
BOTH = ["hospitals", "hospital_notes"]


def publish(lake: Lake, tmp_path: Path, previous: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """One bronze run's discovery and metadata build exactly as the loader does them, with the previous generation's canonical identities."""
    keys = {(row["table_name"], row["sha256"]): row["_object_key"] for row in previous if row["kind"] == "object"}
    observations = {(row["table_name"], row["sha256"]): row["observation_id"] for row in previous if row["kind"] == "object"}
    published = {row["observation_id"] for row in previous if row["kind"] == "observation"}
    retired = {row["observation_id"]: row["retirement_record_json"] for row in previous if row["kind"] == "observation" and row["retired"]}
    inputs, unselected = bronze.discover(
        lake,
        BUCKET,
        config(tmp_path),
        BOTH,
        None,
        keys,
        observations,
        published,
        retired_before=retired,
        previous_observations=[row for row in previous if row["kind"] == "observation"],
    )
    rows = bronze_metadata.build(BOTH, inputs, unselected, previous, BOTH)
    bronze_metadata.validate_successor(previous, rows)
    return inputs, unselected, rows


def observed(rows: list[dict], key: str) -> list[dict]:
    return [row for row in rows if row["kind"] == "observation" and row["s3_key"] == key]


def loaded_keys(inputs: list[dict]) -> set[str]:
    return {item["key"] for item in inputs}


def test_two_successive_changed_captures_retire_each_superseded_capture_and_keep_every_observation(tmp_path: Path) -> None:
    lake = Lake()
    first = capture(lake, root="raw/", snapshot="CA__first", files=[("data", "data.csv", ROWS_OLD), ("dictionary", "notes.csv", NOTES)])
    first_record = write_completion(lake, first, UNIT_A, OLD)
    _, _, one = publish(lake, tmp_path, [])
    second = capture(lake, root="raw/", snapshot="CA__second", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", NOTES_V2)])
    write_completion(lake, second, UNIT_A, MID)
    inputs, _, two = publish(lake, tmp_path, one)
    check(loaded_keys(inputs) == {item["key"] for item in second["objects"]}, "only the second capture loads")
    old_data = observed(two, first["objects"][0]["key"])[0]
    check(old_data["retired"] and not old_data["loaded"], "the superseded capture stays as a retired observation")
    reason = json.loads(old_data["retirement_record_json"])
    check(reason["reason"] == "superseded_by_newer_record" and reason["record_key"] == first_record, "its retirement names the record that had named it")
    third = capture(lake, root="raw/", snapshot="CA__third", files=[("data", "data.csv", ROWS_LEGACY), ("dictionary", "notes.csv", NOTES)])
    write_completion(lake, third, UNIT_A, NEW)
    inputs, _, three = publish(lake, tmp_path, two)
    check(loaded_keys(inputs) == {item["key"] for item in third["objects"]}, "only the third capture loads")
    check(
        {row["observation_id"] for row in one if row["kind"] == "observation"} <= {row["observation_id"] for row in three if row["kind"] == "observation"},
        "no observation of any earlier generation is lost",
    )
    check(sum(row["retired"] for row in three if row["kind"] == "observation") >= 2, "the first and the second capture's data are retired")
    check(publish(lake, tmp_path, three)[2] == three, "a rerun with the same records publishes the same generation")


REVERTED = "20261113T000000Z-dddddddd"
LATER = "2026-11-10T10:00:00Z"


def first_then_second(lake: Lake, tmp_path: Path) -> tuple[dict[str, Any], dict[str, Any], list[dict]]:
    """Completion X is loaded, then completion Y of the same unit supersedes it. Returns both captures and the published generation."""
    first = capture(lake, root="raw/", snapshot="CA__first", files=[("data", "data.csv", ROWS_LEGACY), ("dictionary", "notes.csv", NOTES)])
    write_completion(lake, first, UNIT_A, OLD)
    _, _, rows = publish(lake, tmp_path, [])
    second = capture(lake, root="raw/", snapshot="CA__second", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", NOTES_V2)])
    write_completion(lake, second, UNIT_A, MID)
    _, _, rows = publish(lake, tmp_path, rows)
    check(sum(row["retired"] for row in rows if row["kind"] == "observation") == 2, "the superseding completion retires both observations of the first")
    return first, second, rows


def test_a_changed_unit_with_an_unchanged_dictionary_keeps_loading_the_one_stored_dictionary_object(tmp_path: Path) -> None:
    lake = Lake()
    first = capture(lake, root="raw/", snapshot="CA__first", files=[("data", "data.csv", ROWS_OLD), ("dictionary", "notes.csv", NOTES)])
    write_completion(lake, first, UNIT_A, OLD)
    inputs_one, _, one = publish(lake, tmp_path, [])
    second = capture(lake, root="raw/", snapshot="CA__second", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", NOTES)])
    write_completion(lake, second, UNIT_A, NEW)
    inputs_two, _, two = publish(lake, tmp_path, one)
    notes_one = next(item for item in inputs_one if item["table"] == "hospital_notes")
    notes_two = next(item for item in inputs_two if item["table"] == "hospital_notes")
    check(
        notes_two["object_key"] == notes_one["object_key"] and notes_two["key"] == notes_one["key"], "the stored object has one key, so the canonical key stays"
    )
    check(notes_two["freshness"] == "current" and notes_two["last_retrieved_at_utc"] == NEW, "it carries the newest record's label")
    check(
        next(item for item in inputs_two if item["table"] == "hospitals")["key"] == second["objects"][0]["key"],
        "the changed data loads from the newest capture",
    )
    check(observed(two, first["objects"][0]["key"])[0]["retired"] and not observed(two, notes_one["key"])[0]["retired"], "only the changed data is retired")
    check(publish(lake, tmp_path, two)[2] == two, "a rerun with the same records publishes the same generation")


def test_a_stale_record_naming_the_newest_completions_own_objects_loads_them_as_stale_without_resurrecting_a_retired_capture(tmp_path: Path) -> None:
    lake = Lake()
    first, second, rows = first_then_second(lake, tmp_path)
    write_stale(lake, UNIT_A, "20261109T000000Z-b1b1b1b1", NEW, [pointer(item, MID, "raw/") for item in second["objects"]])
    inputs, _, after = publish(lake, tmp_path, rows)
    check(loaded_keys(inputs) == {item["key"] for item in second["objects"]}, "the newest completion's objects are the live ones and load")
    check({item["freshness"] for item in inputs} == {"stale"}, "they carry the stale label")
    check(all(observed(after, item["key"])[0]["retired"] for item in first["objects"]), "the superseded captures stay retired")


def test_a_stale_record_naming_a_superseded_capture_is_refused_by_name_before_any_metadata_build(tmp_path: Path) -> None:
    lake = Lake()
    first, _, rows = first_then_second(lake, tmp_path)
    late = write_stale(lake, UNIT_A, "20261110T000000Z-c2c2c2c2", LATER, [pointer(item, OLD, "raw/") for item in first["objects"]])
    check(
        refused(lambda: discover(lake, tmp_path, BOTH), "stale pointer older than the newest completion", UNIT_A, late, first["objects"][0]["key"]),
        "the refusal names the unit, the stale record and the superseded objects",
    )
    check(not any(row["loaded"] and row["retired"] for row in rows if row["kind"] == "observation"), "the published generation is unchanged")


def test_a_stale_record_naming_another_version_of_the_newest_completions_object_is_refused_by_name(tmp_path: Path) -> None:
    lake = Lake()
    _, second, _ = first_then_second(lake, tmp_path)
    other = {**pointer(second["objects"][0], MID, "raw/"), "version_id": "v-another"}
    late = write_stale(lake, UNIT_A, "20261110T000000Z-a6a6a6a6", LATER, [other])
    check(
        refused(lambda: discover(lake, tmp_path, BOTH), "stale pointer older than the newest completion", late),
        "an object version the completion does not name is refused",
    )


def test_an_older_stale_records_object_is_retired_once_a_newer_completion_exists(tmp_path: Path) -> None:
    lake = Lake()
    first = capture(lake, root="raw/", snapshot="CA__first", files=[("data", "data.csv", ROWS_LEGACY), ("dictionary", "notes.csv", NOTES)])
    write_stale(lake, UNIT_A, "20261101T000000Z-b2b2b2b2", OLD, [pointer(item, OLD, "raw/") for item in first["objects"]])
    inputs_one, _, one = publish(lake, tmp_path, [])
    check(
        loaded_keys(inputs_one) == {item["key"] for item in first["objects"]} and {item["freshness"] for item in inputs_one} == {"stale"},
        "a stale-only unit loads",
    )
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", NOTES_V2)])
    write_completion(lake, new, UNIT_A, NEW)
    inputs, _, rows = publish(lake, tmp_path, one)
    check(loaded_keys(inputs) == {item["key"] for item in new["objects"]}, "the newer completion loads")
    old = observed(rows, first["objects"][0]["key"])[0]
    check(
        old["retired"] and json.loads(old["retirement_record_json"])["reason"] == "superseded_by_newer_record", "the capture is retired by the newer completion"
    )
    check(publish(lake, tmp_path, rows)[2] == rows, "a rerun with the same records publishes the same generation")


def test_a_reversion_record_makes_the_older_payloads_objects_the_newest_and_bronze_loads_them(tmp_path: Path) -> None:
    lake = Lake()
    first = capture(lake, root="raw/", snapshot="CA__first", files=[("data", "data.csv", ROWS_OLD), ("dictionary", "notes.csv", NOTES)])
    plain_key = write_completion(lake, first, UNIT_A, OLD)
    second = capture(lake, root="raw/", snapshot="CA__second", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", NOTES_V2)])
    write_completion(lake, second, UNIT_A, MID)
    reverted = write_completion(lake, first, UNIT_A, NEW, run=REVERTED)
    check(reverted != plain_key and reverted.endswith(f".{REVERTED}.json"), f"the reversion record has the run ID in its key: {reverted}")
    inputs, unselected, rows = publish(lake, tmp_path, [])
    check(loaded_keys(inputs) == {item["key"] for item in first["objects"]}, "bronze loads the objects of the payload that is newest again")
    check({item["last_retrieved_at_utc"] for item in inputs} == {NEW}, "they carry the reversion record's retrieval time")
    check(all(observed(rows, item["key"])[0]["retired"] for item in second["objects"]), "the captured payload in between stays a retired observation")
    check(publish(lake, tmp_path, rows)[2] == rows, "a rerun with the same records publishes the same generation")


def test_a_reversion_to_objects_an_earlier_generation_retired_loads_them_as_new_observations_and_keeps_the_old_ones_retired(tmp_path: Path) -> None:
    lake = Lake()
    first, second, rows = first_then_second(lake, tmp_path)
    old = {row["observation_id"]: row for item in first["objects"] for row in observed(rows, item["key"])}
    write_completion(lake, first, UNIT_A, NEW, run=REVERTED)
    inputs, _, after = publish(lake, tmp_path, rows)
    check(loaded_keys(inputs) == {item["key"] for item in first["objects"]}, "bronze loads the objects the reversion record names")
    check(
        {item["freshness"] for item in inputs} == {"current"} and {item["last_retrieved_at_utc"] for item in inputs} == {NEW},
        "with the reversion record's label",
    )
    check(not {item["observation_id"] for item in inputs} & set(old), "every loaded observation is new")
    for identity, before in old.items():
        kept = next(row for row in after if row["kind"] == "observation" and row["observation_id"] == identity)
        check(
            kept["retired"] and not kept["loaded"] and kept["retirement_record_json"] == before["retirement_record_json"],
            "the old observation stays retired, unchanged",
        )
    check(after[0]["generation"] != rows[0]["generation"], "the metadata generation advances to new content")
    check(all(observed(after, item["key"])[0]["retired"] for item in second["objects"]), "the capture in between is retired")
    check(publish(lake, tmp_path, after)[2] == after, "a rerun of the same reversion publishes the same generation")


def test_each_reversion_run_binds_its_own_new_observations(tmp_path: Path) -> None:
    lake = Lake()
    first, second, rows = first_then_second(lake, tmp_path)
    ids = []
    for run, instant, stored in (
        ("20261113T000000Z-aaaaaaaa", "2026-11-10T10:00:00Z", first),
        ("20261114T000000Z-bbbbbbbb", "2026-11-11T10:00:00Z", second),
        ("20261115T000000Z-cccccccc", "2026-11-12T10:00:00Z", first),
    ):
        write_completion(lake, stored, UNIT_A, instant, run=run)
        inputs, _, rows = publish(lake, tmp_path, rows)
        check(loaded_keys(inputs) == {item["key"] for item in stored["objects"]}, f"{run} loads its payload")
        ids.append({item["observation_id"] for item in inputs})
        check(publish(lake, tmp_path, rows)[2] == rows, f"{run} reruns the same generation")
    check(ids[0].isdisjoint(ids[2]), "the same objects named by two reversion runs are two sets of observations")
    check(sum(row["retired"] for row in rows if row["kind"] == "observation") >= 6, "every earlier observation stays retired")


def test_a_record_that_is_not_a_reversion_and_names_objects_an_earlier_generation_retired_is_still_refused(tmp_path: Path) -> None:
    lake = Lake()
    first, _, rows = first_then_second(lake, tmp_path)
    other = write_completion(lake, first, UNIT_B, NEW)
    check(
        refused(lambda: publish(lake, tmp_path, rows), "hospitals", other, first["objects"][0]["key"], "an earlier generation retired"),
        "a plain record cannot bring retired objects back",
    )
    check(all(observed(rows, item["key"])[0]["retired"] for item in first["objects"]), "the published generation is unchanged")


def test_the_newest_record_is_found_across_both_key_forms_by_retrieval_time(tmp_path: Path) -> None:
    lake = Lake()
    first = capture(lake, root="raw/", snapshot="CA__first", files=[("data", "data.csv", ROWS_OLD)])
    second = capture(lake, root="raw/", snapshot="CA__second", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, first, UNIT_A, OLD)
    write_completion(lake, second, UNIT_A, NEW, run=REVERTED)
    check([item["key"] for item in discover(lake, tmp_path)] == [second["objects"][0]["key"]], "a later reversion-form record outranks the plain one")
    write_completion(lake, first, UNIT_A, LATER, run="20261114T000000Z-eeeeeeee")
    check([item["key"] for item in discover(lake, tmp_path)] == [first["objects"][0]["key"]], "a still later one outranks both")


def test_a_reversion_record_under_another_runs_name_is_refused_by_name(tmp_path: Path) -> None:
    lake = Lake()
    first = capture(lake, root="raw/", snapshot="CA__first", files=[("data", "data.csv", ROWS_OLD)])
    key = write_completion(lake, first, UNIT_A, NEW, run=REVERTED)
    version = next(version for name, version in lake.objects if name == key)
    body = lake.objects.pop((key, version))
    wrong = key.replace(REVERTED, "20261113T000000Z-ffffffff")
    lake.put(wrong, body)
    check(
        refused(lambda: discover(lake, tmp_path), wrong, "differs from the unit and payload its key names"),
        "a record copied under another run's name is refused",
    )


TRANSPORT = {"role": "transport_audit"}


def test_a_data_object_the_manifest_also_lists_as_transport_evidence_loads_once_through_its_record(tmp_path: Path) -> None:
    lake = Lake()
    stored = capture(lake, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", NOTES)], alias=TRANSPORT)
    write_completion(lake, stored, UNIT_A, NEW)
    inputs = discover(lake, tmp_path, BOTH)
    check(loaded_keys(inputs) == {item["key"] for item in stored["objects"]} and len(inputs) == 2, "the data object loads once, and so does the dictionary")
    inputs_after, _, rows = publish(lake, tmp_path, [])
    check(len(inputs_after) == 2 and publish(lake, tmp_path, rows)[2] == rows, "the publication and its rerun are the same generation")


@pytest.mark.parametrize("field", ["sha256", "byte_count"])
def test_a_transport_alias_of_a_named_object_that_disagrees_on_digest_or_length_is_refused(tmp_path: Path, field: str) -> None:
    lake = Lake()
    other = digest(b"another") if field == "sha256" else 99
    stored = capture(lake, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_NEW)], alias=TRANSPORT | {"object": {field: other}})
    key = write_completion(lake, stored, UNIT_A, NEW)
    check(
        refused(lambda: discover(lake, tmp_path), key, "differs from its manifest"), "an entry for the named object with another digest or length stops the run"
    )


def test_a_record_role_that_no_manifest_entry_for_the_object_carries_is_refused_beside_a_transport_alias(tmp_path: Path) -> None:
    lake = Lake()
    stored = capture(lake, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", NOTES)], alias=TRANSPORT)
    key = write_completion(lake, stored, UNIT_A, NEW, change=lambda body: body["other_objects"][0].update(role="reference"))
    check(
        refused(lambda: discover(lake, tmp_path, BOTH), key, "differs from its manifest"),
        "the alias on the data object does not excuse a wrong role on the dictionary",
    )


def alias_record(lake: Lake, stored: dict[str, Any], *, field: str | None) -> str:
    """A record that names the one data object twice: as data, with a field changed or not, then as transport evidence with the manifest's true facts."""

    def twice(body: dict) -> None:
        body["other_objects"] = [{"role": "transport_audit", **plain(stored["objects"][0])}]
        if field == "sha256":
            body["data_objects"][0]["sha256"] = digest(b"a different record claim")
        elif field == "byte_count":
            body["data_objects"][0]["byte_count"] = 99

    return write_completion(lake, stored, UNIT_A, NEW, change=twice, rekey=True)


@pytest.mark.parametrize("field", ["sha256", "byte_count"])
def test_a_record_that_names_one_object_twice_with_contradictory_digest_or_length_is_refused_in_discovery_and_the_inventory(tmp_path: Path, field: str) -> None:
    lake = Lake()
    stored = capture(lake, root="raw/", snapshot="CA__collision", files=[("data", "data.csv", ROWS_NEW)], alias=TRANSPORT)
    key = alias_record(lake, stored, field=field)
    check(refused(lambda: discover(lake, tmp_path), key, "differs"), "discovery refuses the contradictory declarations and names the record")
    check(refused(lambda: bronze_pin.build_inventory(lake, BUCKET, config(tmp_path), ["hospitals"]), key, "differs"), "so does the inventory builder")


def test_a_record_that_names_one_object_twice_with_the_same_digest_and_length_still_loads_it_once(tmp_path: Path) -> None:
    lake = Lake()
    stored = capture(lake, root="raw/", snapshot="CA__twice", files=[("data", "data.csv", ROWS_NEW)], alias=TRANSPORT)
    alias_record(lake, stored, field=None)
    inputs = discover(lake, tmp_path)
    check(loaded_keys(inputs) == {stored["objects"][0]["key"]} and len(inputs) == 1, "consistent declarations of one object load it once")


def test_a_data_declaration_is_checked_against_the_data_entry_when_a_transport_entry_lists_other_facts(tmp_path: Path) -> None:
    lake = Lake()
    stored = capture(
        lake, root="raw/", snapshot="CA__entries", files=[("data", "data.csv", ROWS_NEW)], alias=TRANSPORT | {"object": {"sha256": digest(b"other")}}
    )
    key = write_completion(lake, stored, UNIT_A, NEW)
    check(refused(lambda: discover(lake, tmp_path), key, "differs from its manifest"), "every manifest entry for a named object must agree with the record")


def test_a_stale_pointer_to_the_newest_completion_that_is_a_reversion_loads_its_rebound_observations_as_stale(tmp_path: Path) -> None:
    lake = Lake()
    first, _, previous = first_then_second(lake, tmp_path)
    write_completion(lake, first, UNIT_A, NEW, run=REVERTED)
    current_inputs, _, live = publish(lake, tmp_path, previous)
    write_stale(lake, UNIT_A, "20261114T000000Z-aaaaaaaa", LATER, [pointer(item, NEW, "raw/") for item in first["objects"]])
    inputs, _, after = publish(lake, tmp_path, live)
    check(
        loaded_keys(inputs) == {item["key"] for item in first["objects"]} and {item["freshness"] for item in inputs} == {"stale"},
        "the pointer loads with the stale label",
    )
    check(
        {item["observation_id"] for item in inputs} == {item["observation_id"] for item in current_inputs},
        "it keeps the rebound observations the reversion loaded",
    )
    check(publish(lake, tmp_path, after)[2] == after, "a rerun publishes the same generation")


def test_a_stale_pointer_to_a_plain_completion_naming_retired_objects_is_still_refused(tmp_path: Path) -> None:
    lake = Lake()
    first, _, previous = first_then_second(lake, tmp_path)
    write_completion(lake, first, UNIT_A, NEW)
    write_stale(lake, UNIT_A, "20261114T000000Z-bbbbbbbb", LATER, [pointer(item, NEW, "raw/") for item in first["objects"]])
    check(
        refused(lambda: publish(lake, tmp_path, previous), "an earlier generation retired"),
        "a plain completion cannot bring retired objects back through a stale pointer",
    )


def test_a_stored_object_no_record_names_stops_the_table_and_retires_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    lake = Lake()
    capture(lake, root="", snapshot="CA__legacy", files=[("data", "data.csv", ROWS_LEGACY), ("dictionary", "notes.csv", NOTES)])
    monkeypatch.delenv("HAI_READ_RAW_ROOT")
    _, _, root_rows = publish(lake, tmp_path, [])
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    new = capture(lake, root="raw/", snapshot="CA__new", files=[("data", "data.csv", ROWS_NEW), ("dictionary", "notes.csv", NOTES_V2)])
    write_completion(lake, new, UNIT_B, NEW)
    check(
        refused(lambda: publish(lake, tmp_path, root_rows), "hospitals", "named by no completion or stale record"),
        "an object of a unit without a record stops its table by name",
    )
    check(not any(row["retired"] for row in root_rows if row["kind"] == "observation"), "the published generation still has no retired observation")


def test_a_manifest_version_a_record_names_loads_even_when_the_current_listing_does_not_return_it(tmp_path: Path) -> None:
    lake = Lake()
    stored = capture(lake, root="raw/", snapshot="CA__a", files=[("data", "data.csv", ROWS_NEW)])
    write_completion(lake, stored, UNIT_A, NEW)
    lake.unlisted.add((stored["manifest"]["key"], stored["manifest"]["version_id"]))
    check(
        [item["key"] for item in discover(lake, tmp_path)] == [stored["objects"][0]["key"]],
        "the record's exact manifest version is read whatever the listing returns",
    )


def test_an_object_a_record_names_that_only_another_manifest_lists_fails_naming_the_record(tmp_path: Path) -> None:
    lake = Lake()
    other = capture(lake, root="raw/", snapshot="CA__other", files=[("data", "data.csv", ROWS_OLD)])
    mine = capture(lake, root="raw/", snapshot="CA__mine", files=[("data", "data.csv", ROWS_NEW)])
    key = write_completion(lake, mine, UNIT_A, NEW, change=lambda body: body["data_objects"][0].update(other["objects"][0] | {"role": "data"}), rekey=True)
    check(
        refused(lambda: discover(lake, tmp_path), key, "no manifest it names lists"), "a manifest the record does not name cannot confirm what the record names"
    )
