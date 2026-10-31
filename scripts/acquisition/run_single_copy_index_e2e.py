"""Synthetic E2E for the single-copy starting-index builder: the real command on a stand-in for the bucket listing and generated download records, no AWS.

.venv/bin/python -m scripts.acquisition.run_single_copy_index_e2e --output data/e2e/single_copy_index/<run>.json

Every refusal scenario starts from one good set, breaks exactly one thing and must be refused for that reason alone.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import shutil
import signal
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from scripts.acquisition import s3_store, single_copy_index, verify_storage_records
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.single_copy import load_policy
from scripts.acquisition.tests.test_s3_store import FakeS3
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError

BUCKET = "synthetic-bucket"
PREFIX = "example_pub/example_collection"
CONFIGURATION = {"aws_profile": "example_project_user", "aws_region": "us-west-2", "data_bucket_name": BUCKET, "expected_account_id": "1" * 12}
STATUS_STORED = "acquired_unvalidated"
NOT_IGNORED_OUTPUT = REPO_ROOT / "docs/not_an_ignored_index.json"
OLDEST, FIRST, SECOND, THIRD = "2025-12-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00Z", "2026-03-01T00:00:00+00:00"
DATA_ONE, DATA_TWO, DATA_THREE = b"id,value\n1,2\n", b"id,value\n3,4\n", b"id,value\n5,6\n"
DICTIONARY, AUDIT, ODD, GONE = b"%PDF-1.7 synthetic dictionary\n", b"HTTP/1.1 200 OK\n", b"id,value\n7,8\n", b"id,value\n9,0\n"
DELETED, MANIFEST_ONE, MANIFEST_TWO, RECEIPT_FOUR = b"deleted\n", b'{"manifest": "one"}\n', b'{"manifest": "two"}\n', b'{"receipt": "four"}\n'
ANOTHER_RUN = b"another run\n"
UPLOADED, UNNAMED = b"id,value\n11,12\n", b"%PDF-1.7 synthetic note\n"
REASON = "synthetic object that only the reasons file explains"
LAKEHOUSE_KEY = "lakehouse/table/data/file.parquet"
MAX_PAGES = 200


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def key_for(category: str, content: bytes, name: str, capture: str) -> str:
    """Build a stored key in the layout the earlier download runs used."""
    sha = digest(content)
    return {
        "datasets": f"{PREFIX}/datasets/ds_one/release_date=2026-01-01/{sha}/{name}",
        "references": f"{PREFIX}/references/capture_id={capture}/{sha}/{name}",
        "audit": f"{PREFIX}/audit/{capture}/{sha}/{name}",
        "manifests": f"{PREFIX}/manifests/release_date=2026-01-01/{capture}/{sha}/{name}",
    }[category]


@dataclass
class Obj:
    """One version in the stand-in listing and the captures whose records name it."""

    category: str
    name: str
    content: bytes
    version: str
    captures: tuple[str, ...]
    modified: str = FIRST
    latest: bool = True
    listed: bool = True
    key: str | None = None
    size: int | None = None
    upload: bool = False
    reason: bool = False
    saved_listing: bool = False

    @property
    def full_key(self) -> str:
        return self.key or key_for(self.category, self.content, self.name, self.captures[0] if self.captures else "zero")

    def entry(self) -> dict[str, Any]:
        size = len(self.content) if self.size is None else self.size
        return {"Key": self.full_key, "VersionId": self.version, "IsLatest": self.latest, "Size": size, "LastModified": self.modified, "ETag": '"synthetic"'}


def good_objects() -> list[Obj]:
    receipt_four_key = f"{PREFIX}/audit/four/{digest(RECEIPT_FOUR)}/receipt.json"
    return [
        Obj("datasets", "data_one.csv", DATA_ONE, "v_d1", ("one", "two")),
        Obj("references", "dictionary.pdf", DICTIONARY, "v_r1", ("one",)),
        Obj("audit", "response.txt", AUDIT, "v_a1", ("one",)),
        Obj("manifests", "manifest.json", MANIFEST_ONE, "v_m1", ("one",)),
        Obj("datasets", "data_two.csv", DATA_TWO, "v_d2", ("two",), SECOND),
        Obj("datasets", "data_two.csv", DATA_TWO, "v_d2_old", (), OLDEST, latest=False, upload=True),
        Obj("datasets", "data_three.csv", DATA_THREE, "v_d3", ("two",), SECOND),
        Obj("datasets", "data_three_copy.csv", DATA_THREE, "v_d3_copy", ("two",), THIRD),
        Obj("datasets", "name with spaces é.csv", ODD, "v_odd", ("two",), SECOND),
        Obj("datasets", "gone.csv", GONE, "v_gone", ("two",), listed=False),
        Obj("datasets", "deleted.csv", DELETED, "v_deleted_old", (), OLDEST, latest=False),
        Obj("manifests", "manifest.json", MANIFEST_TWO, "v_m2", ("two",), SECOND),
        Obj("audit", "receipt.json", RECEIPT_FOUR, "v_a4", (), SECOND, key=receipt_four_key),
        Obj("lakehouse", "file.parquet", b"table", "v_lake", (), key=LAKEHOUSE_KEY),
        Obj("datasets", "uploaded_only.csv", UPLOADED, "v_up", (), SECOND, upload=True),
        Obj("references", "unnamed_note.pdf", UNNAMED, "v_un", (), SECOND, reason=True, saved_listing=True),
    ]


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded_json(value))


def stored_record(obj: Obj) -> dict[str, Any]:
    return {"bucket": BUCKET, "byte_count": len(obj.content), "key": obj.full_key, "sha256": digest(obj.content), "version_id": obj.version}


def record_line(obj: Obj) -> dict[str, Any]:
    return {"dataset_id": "ds_one", "role": "manifest" if obj.category == "manifests" else "data", "storage_path": "raw/file", "object": stored_record(obj)}


def reason_line(obj: Obj) -> dict[str, Any]:
    return {"key": obj.full_key, "version_id": obj.version, "byte_count": len(obj.content), "sha256": digest(obj.content), "reason": REASON}


def write_capture(folder: Path, snapshot: str, status: str, objects: list[Obj], *, reconciled: bool) -> None:
    write_json(folder / "receipt.json", {"snapshot_id": snapshot, "snapshot_status": status, "artifacts": []})
    if reconciled:
        named = [obj for obj in objects if snapshot in obj.captures]
        write_json(
            folder / "s3_collections_reconciliation.json",
            {
                "storage_contract_version": "4.0.0",
                "snapshot_id": snapshot,
                "publisher": "example_pub",
                "collection": "example_collection",
                "manifests": [record_line(obj) for obj in named if obj.category == "manifests"],
                "objects": [record_line(obj) for obj in named if obj.category != "manifests"],
            },
        )


class Workspace:
    """One generated bucket listing, record set, retirement record, disposition record and output path."""

    def __init__(self, root: Path, change: Callable[[list[Obj]], object] | None = None) -> None:
        self.root = root
        self.records = root / "records"
        self.retirements = [root / "retired_objects.json"]
        self.dispositions_path = root / "dispositions.json"
        self.dispositions: Path | None = self.dispositions_path
        self.evidence = root / "evidence"
        self.upload_path = self.evidence / "jobs/upload_one/record.json"
        self.object_dispositions_path = root / "object_dispositions.json"
        self.object_dispositions: Path | None = self.object_dispositions_path
        self.output = root / "out" / "index.json"
        self.restore: list[Path] = []
        self.objects = good_objects()
        if change:
            change(self.objects)
        for name, status, reconciled in (
            ("one", STATUS_STORED, True),
            ("two", STATUS_STORED, True),
            ("three", "rejected", False),
            ("four", STATUS_STORED, False),
        ):
            write_capture(self.capture(name), name, status, self.objects, reconciled=reconciled)
        write_json(self.records / "attempts/attempt_one/receipt.json", {"attempt": 1, "failure": "http_status"})
        retired = [{"key": obj.full_key, "version_id": obj.version, "status": "deleted_verified"} for obj in self.objects if not obj.listed]
        write_json(self.retirements[0], {"objects": retired})
        receipt_checksum = digest((self.capture("four") / "receipt.json").read_bytes())
        capture_four = {"path": "jobs/capture_four", "receipt_sha256": receipt_checksum, "reason": "synthetic capture kept local"}
        write_json(self.dispositions_path, {"kind": "unstored_capture_dispositions", "captures": [capture_four]})
        write_json(self.upload_path, {"files": [stored_record(obj) for obj in self.objects if obj.upload]})
        write_json(self.evidence / "saved_listings/listing.json", {"Versions": [obj.entry() for obj in self.objects if obj.saved_listing]})
        write_json(self.object_dispositions_path, {"kind": "unnamed_object_dispositions", "objects": [reason_line(obj) for obj in self.objects if obj.reason]})

    def capture(self, name: str) -> Path:
        return self.records / "jobs" / f"capture_{name}"

    def arguments(self, *extra: str) -> list[str]:
        arguments = [
            "--records",
            str(self.records),
            "--evidence-root",
            str(self.evidence),
            "--output",
            str(self.output),
            "--retirements",
            *map(str, self.retirements),
        ]
        if self.dispositions is not None:
            arguments += ["--dispositions", str(self.dispositions)]
        if self.object_dispositions is not None:
            arguments += ["--object-dispositions", str(self.object_dispositions)]
        return [*arguments, *extra]

    def entries(self) -> list[tuple[str, dict[str, Any]]]:
        """Return the listing in key order: versions and the delete marker of the deleted key, each with its kind."""
        rows = [("Versions", obj.entry()) for obj in self.objects if obj.listed]
        deleted = [obj for obj in self.objects if obj.version == "v_deleted_old"]
        markers = [("DeleteMarkers", {"Key": obj.full_key, "VersionId": "v_deleted_marker", "IsLatest": True, "LastModified": SECOND}) for obj in deleted]
        return sorted([*rows, *markers], key=lambda row: (row[1]["Key"], row[1]["LastModified"]))


class FakeAws:
    """Answer the version-listing calls from a table of entries in pages and record every call."""

    def __init__(self, entries: list[tuple[str, dict[str, Any]]]) -> None:
        self.entries = entries
        self.calls: list[list[str]] = []
        self.timeouts: list[tuple[str, float]] = []
        self.fail_page: int | None = None
        self.after_page: Callable[[int, dict[str, Any]], object] | None = None
        self.identity_account = CONFIGURATION["expected_account_id"]
        self.identity_user = CONFIGURATION["aws_profile"]
        self.identity_account_field = CONFIGURATION["expected_account_id"]
        self.identity_fails = False

    @property
    def pages(self) -> int:
        return len([call for call in self.calls if call[2] == "list-object-versions"])

    def __call__(self, argv: list[str], **options: Any) -> SimpleNamespace:
        self.calls.append(list(argv))
        self.timeouts.append((argv[2], options["timeout"]))
        if argv[2] == "get-caller-identity" and self.identity_fails:
            return SimpleNamespace(returncode=254, stdout="", stderr="An error occurred (AccessDenied) when calling the GetCallerIdentity operation: denied")
        if argv[2] == "get-caller-identity":
            arn = f"arn:aws:iam::{self.identity_account}:user/{self.identity_user}"
            return SimpleNamespace(returncode=0, stdout=json.dumps({"Account": self.identity_account_field, "Arn": arn}), stderr="")
        if argv[2] != "list-object-versions":
            return SimpleNamespace(returncode=255, stdout="", stderr="An error occurred (UnexpectedCall) when calling the operation")
        size = int(argv[argv.index("--max-items") + 1])
        start = int(argv[argv.index("--starting-token") + 1]) if "--starting-token" in argv else 0
        number = self.pages
        if self.fail_page == number or number > MAX_PAGES:
            return SimpleNamespace(returncode=254, stdout="", stderr="An error occurred (InternalError) when calling the ListObjectVersions operation: failed")
        page: dict[str, Any] = {"Versions": [], "DeleteMarkers": []}
        for kind, entry in self.entries[start : start + size]:
            page[kind].append(entry)
        if start + size < len(self.entries):
            page["NextToken"] = str(start + size)
        if self.after_page:
            self.after_page(number, page)
        return SimpleNamespace(returncode=0, stdout=json.dumps(page), stderr="")


def run(arguments: list[str]) -> tuple[int, dict[str, Any], str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = single_copy_index.main(arguments)
    return code, json.loads(out.getvalue()), err.getvalue()


def build(
    workspace: Workspace, fake: FakeAws | None = None, *extra: str, page_size: int = 1000, configuration_error: str | None = None
) -> tuple[int, dict[str, Any], str, FakeAws]:
    """Run the real command against the stand-in listing."""
    fake = fake or FakeAws(workspace.entries())

    def configure(_path: Path) -> tuple[dict[str, str], dict[str, str]]:
        if configuration_error:
            raise ConfigurationError(configuration_error)
        return CONFIGURATION, {}

    with (
        patch.object(single_copy_index, "load_configuration", configure),
        patch.object(single_copy_index, "PAGE_SIZE", page_size),
        patch.object(s3_store, "run_argv", fake),
    ):
        code, summary, err = run(workspace.arguments(*extra))
    return code, summary, err, fake


def edit(path: Path, change: Callable[[Any], object]) -> None:
    value = json.loads(path.read_text())
    change(value)
    path.write_bytes(encoded_json(value))


def reconciliation_of(workspace: Workspace, capture: str) -> Path:
    return workspace.capture(capture) / "s3_collections_reconciliation.json"


def remove_line(workspace: Workspace, capture: str, version: str) -> None:
    edit(
        reconciliation_of(workspace, capture),
        lambda value: value.__setitem__("objects", [line for line in value["objects"] if line["object"]["version_id"] != version]),
    )


def add_line(workspace: Workspace, capture: str, **changes: Any) -> None:
    def add(value: dict[str, Any]) -> None:
        line = json.loads(json.dumps(value["objects"][0]))
        line["object"].update(changes)
        value["objects"].append(line)

    edit(reconciliation_of(workspace, capture), add)


def add_manifest_line(workspace: Workspace, capture: str, **changes: Any) -> None:
    def add(value: dict[str, Any]) -> None:
        line = json.loads(json.dumps(value["manifests"][0]))
        line["object"].update(changes)
        value["manifests"].append(line)

    edit(reconciliation_of(workspace, capture), add)


def nest_invalid_receipt(workspace: Workspace) -> None:
    nested = workspace.capture("one") / "raw" / "inner"
    nested.mkdir(parents=True)
    (nested / "receipt.json").write_text("{not json")


def make_folder_unreadable(path: Path, workspace: Workspace) -> None:
    path.chmod(0)
    workspace.restore.append(path)


def link_capture_folder(workspace: Workspace) -> None:
    folder = workspace.capture("one")
    moved = workspace.root / "moved_capture_one"
    folder.rename(moved)
    folder.symlink_to(moved)


def write_other_output(workspace: Workspace) -> None:
    workspace.output.parent.mkdir(parents=True)
    workspace.output.write_text("{}\n")


def add_garbage_retirement(workspace: Workspace) -> None:
    garbage = workspace.root / "garbage_retirements.json"
    garbage.write_text("not json")
    workspace.retirements.append(garbage)


def add_unverified_retirement(workspace: Workspace) -> None:
    extra = workspace.root / "unverified_retirements.json"
    write_json(extra, {"objects": [{"key": key_for("datasets", b"other", "other.csv", "x"), "version_id": "v_x", "status": "planned"}]})
    workspace.retirements.append(extra)


def empty_records(workspace: Workspace) -> None:
    shutil.rmtree(workspace.records)
    workspace.records.mkdir()
    workspace.dispositions = None


def only_manifests(objects: list[Obj]) -> None:
    objects[:] = [obj for obj in objects if obj.category == "manifests" or obj.version == "v_deleted_old"]


def by_version(objects: list[Obj], version: str) -> Obj:
    return next(obj for obj in objects if obj.version == version)


def change_entry(version: str, change: Callable[[dict[str, Any]], object]) -> Callable[[FakeAws], object]:
    def apply(fake: FakeAws) -> None:
        for _, entry in fake.entries:
            if entry["VersionId"] == version:
                change(entry)

    return apply


def hook(change: Callable[[dict[str, Any]], object], page_number: int = 1) -> Callable[[FakeAws], object]:
    def apply(fake: FakeAws) -> None:
        fake.after_page = lambda number, page: change(page) if number == page_number else None

    return apply


def extra_object(**changes: Any) -> Callable[[list[Obj]], object]:
    """Add a second current copy of the bytes of data one, named by the second capture."""
    values: dict[str, Any] = {
        "category": "datasets",
        "name": "data_one_elsewhere.csv",
        "content": DATA_ONE,
        "version": "v_extra",
        "captures": ("two",),
        "modified": SECOND,
    }
    values.update(changes)
    return lambda objects: objects.append(Obj(**values))


@dataclass
class Refusal:
    name: str
    reason: str
    world: Callable[[list[Obj]], object] | None = None
    mutate: Callable[[Workspace], object] | None = None
    fake: Callable[[FakeAws], object] | None = None
    shows: Callable[[Workspace], list[str]] | None = None
    also: tuple[str, ...] = ()
    page_size: int = 1000
    configuration_error: str | None = None


def dispositions_change(change: Callable[[dict[str, Any]], object]) -> Callable[[Workspace], object]:
    return lambda workspace: edit(workspace.dispositions_path, change)


def reasons_change(change: Callable[[list[dict[str, Any]]], object]) -> Callable[[Workspace], object]:
    return lambda workspace: edit(workspace.object_dispositions_path, lambda value: change(value["objects"]))


def reason_of(lines: list[dict[str, Any]], version: str = "v_un") -> dict[str, Any]:
    return next(line for line in lines if line["version_id"] == version)


def change_reason(field: str, value: object, version: str = "v_un") -> Callable[[Workspace], object]:
    return reasons_change(lambda lines: reason_of(lines, version).__setitem__(field, value))


def drop_reason_field(field: str) -> Callable[[Workspace], object]:
    return reasons_change(lambda lines: reason_of(lines).pop(field))


def reason_for(version: str) -> Callable[[Workspace], object]:
    return lambda workspace: edit(
        workspace.object_dispositions_path, lambda value: value["objects"].append(reason_line(by_version(workspace.objects, version)))
    )


def change_upload(field: str, value: object) -> Callable[[Workspace], object]:
    return lambda workspace: edit(workspace.upload_path, lambda record: reason_of(record["files"], "v_up").__setitem__(field, value))


def earlier_index(folder: str) -> Callable[[Workspace], object]:
    """Store a complete schema 2 index that names the unnamed object, in one folder of the evidence root, and remove the object's reason."""

    def write(workspace: Workspace) -> None:
        named = by_version(workspace.objects, "v_un")
        index = {"schema_version": 2, "generation": "0" * 64, "objects": {digest(named.content): stored_record(named)}, "evidence_objects": {}}
        write_json(workspace.evidence / folder / "index_earlier.json", index)
        edit(workspace.object_dispositions_path, lambda value: value["objects"].clear())

    return write


def drop_upload_field(field: str) -> Callable[[Workspace], object]:
    return lambda workspace: edit(workspace.upload_path, lambda record: reason_of(record["files"], "v_up").pop(field))


def add_upload_record(version: str = "v_up", **changes: Any) -> Callable[[Workspace], object]:
    """Add an upload record for the object with the given version, with the given fields changed; a field set to None is removed."""

    def add(workspace: Workspace) -> None:
        copy = {**stored_record(by_version(workspace.objects, version)), **changes}
        edit(workspace.upload_path, lambda record: record["files"].append({name: value for name, value in copy.items() if value is not None}))

    return add


def add_retirement(version: str, **changes: Any) -> Callable[[Workspace], object]:
    """Add a verified retirement line for the object with the given version, with extra fields."""

    def add(workspace: Workspace) -> None:
        obj = by_version(workspace.objects, version)
        line = {"key": obj.full_key, "version_id": obj.version, "status": "deleted_verified", **changes}
        edit(workspace.retirements[0], lambda value: value["objects"].append(line))

    return add


def retire_delete_marker(workspace: Workspace) -> None:
    deleted = by_version(workspace.objects, "v_deleted_old")
    line = {"key": deleted.full_key, "version_id": "v_deleted_marker", "status": "deleted_verified"}
    edit(workspace.retirements[0], lambda value: value["objects"].append(line))


def in_sequence(*steps: Callable[[Workspace], object]) -> Callable[[Workspace], object]:
    return lambda workspace: [step(workspace) for step in steps]


def mark_page(page_number: int, **changes: Any) -> Callable[[FakeAws], object]:
    """Set fields on the page with the given number, as the service would."""
    return hook(lambda page: page.update(changes), page_number)


def refusal_cases() -> list[Refusal]:
    unexplained = ("acquired_capture_without_record",)
    unnamed = ("stored_object_without_record",)
    cases = [
        Refusal("listing_page_fails_after_others", "listing_failed", fake=lambda fake: setattr(fake, "fail_page", 3), page_size=3),
        Refusal("listing_first_page_fails", "listing_failed", fake=lambda fake: setattr(fake, "fail_page", 1)),
        Refusal("listing_versions_not_a_list", "listing_malformed", fake=hook(lambda page: page.__setitem__("Versions", 7))),
        Refusal("listing_entry_without_key", "listing_malformed", fake=change_entry("v_d1", lambda entry: entry.pop("Key"))),
        Refusal("listing_entry_without_version", "listing_malformed", fake=change_entry("v_d1", lambda entry: entry.__setitem__("VersionId", ""))),
        Refusal("listing_entry_without_size", "listing_malformed", fake=change_entry("v_d1", lambda entry: entry.pop("Size"))),
        Refusal("listing_entry_with_boolean_size", "listing_malformed", fake=change_entry("v_d1", lambda entry: entry.__setitem__("Size", True))),
        Refusal("listing_entry_with_negative_size", "listing_malformed", fake=change_entry("v_d1", lambda entry: entry.__setitem__("Size", -1))),
        Refusal(
            "listing_entry_without_zone_in_time",
            "listing_malformed",
            fake=change_entry("v_d1", lambda entry: entry.__setitem__("LastModified", "2026-01-01T00:00:00")),
        ),
        Refusal("listing_entry_latest_not_boolean", "listing_malformed", fake=change_entry("v_d1", lambda entry: entry.__setitem__("IsLatest", "true"))),
        Refusal(
            "listing_entry_repeated",
            "listing_malformed",
            fake=hook(lambda page: page["Versions"].append(next(dict(entry) for entry in page["Versions"] if not entry["IsLatest"]))),
        ),
        Refusal("listing_two_current_versions_of_a_key", "listing_malformed", fake=change_entry("v_d2_old", lambda entry: entry.__setitem__("IsLatest", True))),
        Refusal("listing_token_repeats", "listing_malformed", fake=hook(lambda page: page.__setitem__("NextToken", "3"), 2), page_size=3),
        Refusal(
            "listing_token_repeats_on_empty_pages",
            "listing_malformed",
            fake=lambda fake: setattr(
                fake, "after_page", lambda number, page: page.update(Versions=[], DeleteMarkers=[], NextToken="3") if number >= 2 else None
            ),
            page_size=3,
        ),
        Refusal("listing_token_empty", "listing_malformed", fake=hook(lambda page: page.__setitem__("NextToken", ""), 1), page_size=3),
        Refusal("listing_terminal_page_is_truncated", "listing_incomplete", fake=mark_page(1, IsTruncated=True), shows=lambda w: ["page 1"]),
        Refusal("listing_terminal_page_is_truncated_as_text", "listing_incomplete", fake=mark_page(1, IsTruncated="true"), shows=lambda w: ["page 1"]),
        Refusal("listing_terminal_page_has_a_next_key_marker", "listing_incomplete", fake=mark_page(1, NextKeyMarker="next/key"), shows=lambda w: ["page 1"]),
        Refusal(
            "listing_terminal_page_has_a_next_version_marker", "listing_incomplete", fake=mark_page(1, NextVersionIdMarker="v_next"), shows=lambda w: ["page 1"]
        ),
        Refusal(
            "listing_last_of_several_pages_is_truncated",
            "listing_incomplete",
            fake=mark_page(6, IsTruncated=True),
            shows=lambda w: ["page 6"],
            page_size=3,
        ),
        Refusal("configuration_missing", "configuration_invalid", configuration_error="the configuration file is missing"),
        Refusal("identity_of_another_account", "identity_not_verified", fake=lambda fake: setattr(fake, "identity_account", "9" * 12)),
        Refusal("identity_account_field_differs_from_the_arn", "identity_not_verified", fake=lambda fake: setattr(fake, "identity_account_field", "9" * 12)),
        Refusal("identity_of_another_user", "identity_not_verified", fake=lambda fake: setattr(fake, "identity_user", "another_user")),
        Refusal("identity_call_fails", "identity_not_verified", fake=lambda fake: setattr(fake, "identity_fails", True)),
        Refusal("key_in_unknown_folder", "unclassified_key", world=extra_object(key=f"{PREFIX}/data/ds_one/release/{digest(DATA_ONE)}/x.csv", captures=())),
        Refusal("key_with_an_empty_part", "unclassified_key", world=extra_object(key=f"{PREFIX}/datasets//release/{digest(DATA_ONE)}/x.csv", captures=())),
        Refusal("key_without_a_checksum", "key_checksum_unreadable", world=extra_object(key=f"{PREFIX}/datasets/ds_one/release/x.csv", captures=())),
        Refusal(
            "key_with_two_checksums",
            "key_checksum_unreadable",
            world=extra_object(key=f"{PREFIX}/datasets/ds_one/{digest(DATA_ONE)}/{digest(DATA_TWO)}/x.csv", captures=()),
        ),
        Refusal(
            "key_with_uppercase_checksum",
            "key_checksum_unreadable",
            world=extra_object(key=f"{PREFIX}/datasets/ds_one/rel/{digest(DATA_ONE).upper()}/x.csv", captures=()),
        ),
        Refusal("current_version_is_null", "version_missing_or_null", world=lambda objects: setattr(by_version(objects, "v_d1"), "version", "null")),
        Refusal("checksum_in_both_scopes", "checksum_in_both_scopes", world=extra_object(category="audit", name="same_bytes.txt")),
        Refusal("copies_with_other_sizes", "checksum_sizes_disagree", world=extra_object(size=999)),
        Refusal("bucket_holds_no_payload", "no_stored_payloads", world=only_manifests),
        Refusal("stored_object_without_record", "stored_object_without_record", mutate=lambda w: remove_line(w, "two", "v_d2"), shows=lambda w: ["v_d2"]),
        Refusal(
            "stored_object_in_the_audit_folder_of_another_run",
            "stored_object_without_record",
            world=extra_object(category="audit", name="r.json", content=ANOTHER_RUN, captures=(), key=key_for("audit", ANOTHER_RUN, "r.json", "another_run")),
        ),
        Refusal(
            "listing_alone_is_not_a_name",
            "stored_object_without_record",
            mutate=reasons_change(lambda lines: lines.clear()),
            shows=lambda w: ["v_un"],
        ),
        Refusal("no_object_reasons_given", "stored_object_without_record", mutate=lambda w: setattr(w, "object_dispositions", None), shows=lambda w: ["v_un"]),
        Refusal(
            "upload_record_names_another_version", "stored_object_without_record", mutate=change_upload("version_id", "v_up_other"), shows=lambda w: ["v_up"]
        ),
        Refusal(
            "upload_record_names_another_bucket", "stored_object_without_record", mutate=change_upload("bucket", "another-bucket"), shows=lambda w: ["v_up"]
        ),
        Refusal(
            "upload_record_gives_another_size", "upload_record_size_differs", mutate=change_upload("byte_count", len(UPLOADED) + 1), shows=lambda w: ["v_up"]
        ),
        Refusal("upload_record_without_a_size", "upload_record_size_invalid", mutate=drop_upload_field("byte_count"), shows=lambda w: ["v_up"]),
        Refusal("upload_record_with_text_size", "upload_record_size_invalid", mutate=change_upload("byte_count", str(len(UPLOADED))), shows=lambda w: ["v_up"]),
        Refusal("upload_record_with_negative_size", "upload_record_size_invalid", mutate=change_upload("byte_count", -1), shows=lambda w: ["v_up"]),
        Refusal(
            "upload_record_with_boolean_size",
            "upload_record_size_invalid",
            world=lambda objects: setattr(by_version(objects, "v_up"), "size", 1),
            mutate=change_upload("byte_count", True),
            shows=lambda w: ["v_up"],
        ),
        Refusal(
            "upload_record_with_two_sizes_that_differ",
            "upload_record_size_invalid",
            mutate=change_upload("bytes", len(UPLOADED) + 1),
            shows=lambda w: ["v_up"],
        ),
        Refusal(
            "upload_records_with_two_sizes_and_one_matches",
            "upload_record_sizes_conflict",
            mutate=add_upload_record(byte_count=len(UPLOADED) + 1),
            shows=lambda w: ["v_up"],
        ),
        Refusal(
            "upload_record_repeated_without_a_size",
            "upload_record_size_invalid",
            mutate=add_upload_record(byte_count=None),
            shows=lambda w: ["v_up"],
        ),
        Refusal(
            "upload_record_without_a_size_for_a_captured_object",
            "upload_record_size_invalid",
            mutate=add_upload_record("v_d1", byte_count=None),
            shows=lambda w: ["v_d1"],
        ),
        Refusal(
            "upload_record_with_text_size_for_a_captured_object",
            "upload_record_size_invalid",
            mutate=add_upload_record("v_d1", byte_count=str(len(DATA_ONE))),
            shows=lambda w: ["v_d1"],
        ),
        Refusal(
            "upload_records_with_two_sizes_for_a_captured_object",
            "upload_record_sizes_conflict",
            mutate=in_sequence(add_upload_record("v_d1"), add_upload_record("v_d1", byte_count=len(DATA_ONE) + 1)),
            shows=lambda w: ["v_d1"],
        ),
        Refusal(
            "upload_record_gives_another_size_for_a_captured_object",
            "upload_record_size_differs",
            mutate=add_upload_record("v_d1", byte_count=len(DATA_ONE) + 1),
            shows=lambda w: ["v_d1"],
        ),
        Refusal("upload_record_not_json", "upload_records_unreadable", mutate=lambda w: w.upload_path.write_text("{not json")),
        Refusal("evidence_root_missing", "evidence_root_missing", mutate=lambda w: shutil.rmtree(w.evidence)),
        Refusal(
            "earlier_index_in_a_nested_folder_is_not_a_record", "stored_object_without_record", mutate=earlier_index("review/nested"), shows=lambda w: ["v_un"]
        ),
        Refusal(
            "earlier_index_in_the_single_copy_folder_is_not_a_record",
            "stored_object_without_record",
            mutate=earlier_index("single_copy"),
            shows=lambda w: ["v_un"],
        ),
        Refusal("object_reason_for_another_key", "object_disposition_unmatched", mutate=change_reason("key", "elsewhere/other.pdf"), also=unnamed),
        Refusal("object_reason_for_another_version", "object_disposition_unmatched", mutate=change_reason("version_id", "v_un_other"), also=unnamed),
        Refusal("object_reason_for_another_byte_count", "object_disposition_unmatched", mutate=change_reason("byte_count", len(UNNAMED) + 1), also=unnamed),
        Refusal("object_reason_for_another_sha256", "object_disposition_unmatched", mutate=change_reason("sha256", digest(b"other bytes")), also=unnamed),
        Refusal("object_reason_for_an_old_version", "object_disposition_unmatched", mutate=reason_for("v_d2_old")),
        Refusal("object_reason_for_a_manifest", "object_disposition_unmatched", mutate=reason_for("v_m1")),
        Refusal("object_reason_for_an_object_a_record_names", "object_disposition_unneeded", mutate=reason_for("v_d1")),
        Refusal("object_reason_for_an_object_an_upload_record_names", "object_disposition_unneeded", mutate=reason_for("v_up")),
        Refusal("object_reason_for_an_object_a_capture_reason_explains", "object_disposition_unneeded", mutate=reason_for("v_a4")),
        Refusal("object_reason_repeated", "object_disposition_invalid", mutate=reasons_change(lambda lines: lines.append(dict(lines[0])))),
        Refusal("object_reason_blank", "object_disposition_invalid", mutate=change_reason("reason", " ")),
        Refusal("object_reason_missing", "object_disposition_invalid", mutate=drop_reason_field("reason")),
        Refusal("object_reason_without_key", "object_disposition_invalid", mutate=drop_reason_field("key")),
        Refusal("object_reason_without_version", "object_disposition_invalid", mutate=drop_reason_field("version_id")),
        Refusal("object_reason_with_text_byte_count", "object_disposition_invalid", mutate=change_reason("byte_count", str(len(UNNAMED)))),
        Refusal("object_reason_with_boolean_byte_count", "object_disposition_invalid", mutate=change_reason("byte_count", True)),
        Refusal("object_reason_with_malformed_sha256", "object_disposition_invalid", mutate=change_reason("sha256", "xyz")),
        Refusal("object_reasons_file_not_a_record", "object_disposition_invalid", mutate=lambda w: w.object_dispositions_path.write_text("[]")),
        Refusal("object_reasons_file_missing", "object_disposition_invalid", mutate=lambda w: w.object_dispositions_path.unlink()),
        Refusal(
            "record_names_a_version_the_listing_lacks",
            "record_object_not_in_listing",
            mutate=lambda w: add_line(w, "two", version_id="v_missing"),
            shows=lambda w: ["v_missing"],
        ),
        Refusal(
            "record_names_a_manifest_the_listing_lacks",
            "record_object_not_in_listing",
            mutate=lambda w: add_manifest_line(w, "two", version_id="v_missing_manifest"),
        ),
        Refusal(
            "record_names_an_old_version",
            "record_object_not_in_listing",
            mutate=lambda w: add_line(w, "two", key=by_version(w.objects, "v_d2_old").full_key, version_id="v_d2_old"),
        ),
        Refusal("record_names_another_bucket", "record_object_not_in_listing", mutate=lambda w: add_line(w, "two", bucket="another-bucket")),
        Refusal("retired_version_without_retirement_record", "record_object_not_in_listing", mutate=lambda w: setattr(w, "retirements", [])),
        Refusal("retirement_file_missing", "retirement_unreadable", mutate=lambda w: w.retirements.append(w.root / "no_such_retirements.json")),
        Refusal("retirement_not_json", "retirement_unreadable", mutate=add_garbage_retirement),
        Refusal("retirement_not_verified", "retirement_unverified", mutate=add_unverified_retirement),
        Refusal(
            "retirement_names_another_bucket", "retirement_other_bucket", mutate=add_retirement("v_gone", bucket="another-bucket"), shows=lambda w: ["v_gone"]
        ),
        Refusal("retirement_bucket_not_text", "retirement_invalid", mutate=add_retirement("v_gone", bucket=7)),
        Refusal("retirement_with_text_byte_count", "retirement_invalid", mutate=add_retirement("v_gone", bytes=str(len(GONE)))),
        Refusal("retirement_with_boolean_byte_count", "retirement_invalid", mutate=add_retirement("v_gone", bytes=True)),
        Refusal("retirement_with_negative_byte_count", "retirement_invalid", mutate=add_retirement("v_gone", bytes=-1)),
        Refusal("retirement_with_malformed_sha256", "retirement_invalid", mutate=add_retirement("v_gone", sha256="xyz")),
        Refusal(
            "retirement_repeated_with_another_byte_count",
            "retirement_conflict",
            mutate=in_sequence(add_retirement("v_gone", bytes=len(GONE)), add_retirement("v_gone", bytes=len(GONE) + 1)),
            shows=lambda w: ["v_gone"],
        ),
        Refusal(
            "retirement_repeated_with_another_sha256",
            "retirement_conflict",
            mutate=in_sequence(add_retirement("v_gone", sha256=digest(GONE)), add_retirement("v_gone", sha256=digest(b"other bytes"))),
            shows=lambda w: ["v_gone"],
        ),
        Refusal(
            "retirement_repeated_with_another_bucket",
            "retirement_conflict",
            mutate=in_sequence(add_retirement("v_gone", bucket=BUCKET), add_retirement("v_gone", bucket="another-bucket")),
            shows=lambda w: ["v_gone"],
        ),
        Refusal("retirement_of_a_current_version", "retired_object_still_listed", mutate=add_retirement("v_d1"), shows=lambda w: ["v_d1"]),
        Refusal("retirement_of_an_old_version", "retired_object_still_listed", mutate=add_retirement("v_d2_old"), shows=lambda w: ["v_d2_old"]),
        Refusal("retirement_of_a_delete_marker", "retired_object_still_listed", mutate=retire_delete_marker, shows=lambda w: ["v_deleted_marker"]),
        Refusal(
            "acquired_capture_without_record",
            "acquired_capture_without_record",
            mutate=lambda w: setattr(w, "dispositions", None),
            shows=lambda w: [digest((w.capture("four") / "receipt.json").read_bytes())],
        ),
        Refusal(
            "disposition_for_no_capture",
            "disposition_unused",
            mutate=dispositions_change(
                lambda v: v["captures"].append({"path": "jobs/capture_nine", "receipt_sha256": digest(b"no such receipt"), "reason": "synthetic stale line"})
            ),
        ),
        Refusal(
            "disposition_with_another_receipt_checksum",
            "disposition_unused",
            mutate=dispositions_change(lambda v: v["captures"][0].__setitem__("receipt_sha256", digest(b"another receipt"))),
            also=unexplained,
        ),
        Refusal(
            "disposition_with_another_path",
            "disposition_unused",
            mutate=dispositions_change(lambda v: v["captures"][0].__setitem__("path", "jobs/capture_nine")),
            also=unexplained,
        ),
        Refusal(
            "disposition_without_reason",
            "disposition_invalid",
            mutate=dispositions_change(lambda v: v["captures"][0].__setitem__("reason", " ")),
            also=unexplained,
        ),
        Refusal("disposition_without_path", "disposition_invalid", mutate=dispositions_change(lambda v: v["captures"][0].pop("path")), also=unexplained),
        Refusal("disposition_repeated", "disposition_invalid", mutate=dispositions_change(lambda v: v["captures"].append(dict(v["captures"][0])))),
        Refusal("disposition_file_not_a_record", "disposition_invalid", mutate=lambda w: w.dispositions_path.write_text("[]"), also=unexplained),
        Refusal("receipt_not_json", "record_unreadable", mutate=lambda w: (w.capture("one") / "receipt.json").write_text("{not json")),
        Refusal("receipt_missing_beside_a_record", "record_unreadable", mutate=lambda w: (w.capture("two") / "receipt.json").unlink()),
        Refusal(
            "receipt_status_not_text",
            "record_unreadable",
            mutate=lambda w: edit(w.capture("four") / "receipt.json", lambda v: v.__setitem__("snapshot_status", 7)),
            also=("disposition_unused",),
        ),
        Refusal("reconciliation_truncated", "record_unreadable", mutate=lambda w: reconciliation_of(w, "one").write_text('{"objects": [')),
        Refusal(
            "reconciliation_wrong_contract",
            "record_unreadable",
            mutate=lambda w: edit(reconciliation_of(w, "one"), lambda v: v.__setitem__("storage_contract_version", "3.0.0")),
        ),
        Refusal(
            "reconciliation_of_another_snapshot",
            "record_unreadable",
            mutate=lambda w: edit(reconciliation_of(w, "one"), lambda v: v.__setitem__("snapshot_id", "other")),
        ),
        Refusal("reconciliation_without_object_list", "record_unreadable", mutate=lambda w: edit(reconciliation_of(w, "one"), lambda v: v.pop("objects"))),
        Refusal(
            "reconciliation_line_without_version",
            "record_unreadable",
            mutate=lambda w: edit(reconciliation_of(w, "one"), lambda v: v["objects"][0]["object"].pop("version_id")),
        ),
        Refusal(
            "reconciliation_line_without_object",
            "record_unreadable",
            mutate=lambda w: edit(reconciliation_of(w, "one"), lambda v: v["objects"][0].pop("object")),
        ),
        Refusal("records_folder_missing", "records_folder_missing", mutate=lambda w: shutil.rmtree(w.records)),
        Refusal("no_captures_at_all", "no_captures_found", mutate=empty_records),
        Refusal("capture_folder_is_a_link", "records_scan_error", mutate=link_capture_folder),
        Refusal("output_has_other_bytes", "output_differs", mutate=write_other_output),
        Refusal("output_not_ignored", "output_not_ignored", mutate=lambda w: setattr(w, "output", NOT_IGNORED_OUTPUT)),
    ]
    if os.geteuid() != 0:
        cases += [
            Refusal("capture_folder_unreadable", "records_scan_error", mutate=lambda w: make_folder_unreadable(w.capture("one"), w)),
            Refusal("parent_of_captures_unreadable", "records_scan_error", mutate=lambda w: make_folder_unreadable(w.records / "jobs", w)),
            Refusal("records_folder_unreadable", "records_scan_error", mutate=lambda w: make_folder_unreadable(w.records, w)),
        ]
    return cases


def summary_reasons(summary: dict[str, Any]) -> set[str]:
    return set(summary.get("refusals", {}))


def refusal_scenario(case: Refusal, root: Path) -> dict[str, Any]:
    workspace = Workspace(root / case.name, case.world)
    if case.mutate:
        case.mutate(workspace)
    fake = FakeAws(workspace.entries())
    if case.fake:
        case.fake(fake)
    existed_before = workspace.output.exists()
    try:
        code, summary, _, _ = build(workspace, fake, page_size=case.page_size, configuration_error=case.configuration_error)
    finally:
        for folder in workspace.restore:
            folder.chmod(0o755)
    expected = sorted({case.reason, *case.also})
    observed = sorted(summary_reasons(summary))
    wrote = workspace.output.exists() and not existed_before
    examples = " ".join(summary["refusals"].get(case.reason, {}).get("examples", []))
    missing = [text for text in (case.shows(workspace) if case.shows else []) if text not in examples]
    passed = code == 1 and observed == expected and not wrote and summary["status"] == "refused" and not missing
    return {
        "name": case.name,
        "kind": "refusal",
        "expected": {"exit_code": 1, "reasons": expected, "index_written": False, "missing_in_detail": []},
        "observed": {"exit_code": code, "reasons": observed, "index_written": wrote, "missing_in_detail": missing},
        "passed": passed,
    }


def read_index(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def success(name: str, expected: Any, observed: Any, passed: bool) -> dict[str, Any]:
    return {"name": name, "kind": "success", "expected": expected, "observed": observed, "passed": passed}


def good_scenarios(root: Path) -> tuple[list[dict[str, Any]], Workspace, dict[str, Any]]:
    workspace = Workspace(root / "good")
    code, summary, switch_text, fake = build(workspace)
    if code != 0 or not workspace.output.exists():
        refused = {"exit_code": code, "reasons": sorted(summary_reasons(summary))}
        return [success("good_set_builds_expected_index", {"exit_code": 0}, refused, False)], workspace, summary
    results: list[dict[str, Any]] = []
    index = read_index(workspace.output)
    raw = workspace.output.read_bytes()
    expected_objects = {
        digest(DATA_ONE): ("v_d1", key_for("datasets", DATA_ONE, "data_one.csv", "one")),
        digest(DATA_TWO): ("v_d2", key_for("datasets", DATA_TWO, "data_two.csv", "two")),
        digest(DATA_THREE): ("v_d3", key_for("datasets", DATA_THREE, "data_three.csv", "two")),
        digest(DICTIONARY): ("v_r1", key_for("references", DICTIONARY, "dictionary.pdf", "one")),
        digest(ODD): ("v_odd", key_for("datasets", ODD, "name with spaces é.csv", "two")),
        digest(UPLOADED): ("v_up", key_for("datasets", UPLOADED, "uploaded_only.csv", "zero")),
        digest(UNNAMED): ("v_un", key_for("references", UNNAMED, "unnamed_note.pdf", "zero")),
    }
    expected_evidence = {digest(AUDIT): "v_a1", digest(RECEIPT_FOUR): "v_a4"}
    found_objects = {sha: (record["version_id"], record["key"]) for sha, record in index["objects"].items()}
    found_evidence = {sha: record["version_id"] for sha, record in index["evidence_objects"].items()}
    results.append(
        success(
            "good_set_builds_expected_index",
            {"exit_code": 0, "objects": sorted(expected_objects.values()), "evidence": sorted(expected_evidence.items())},
            {"exit_code": code, "objects": sorted(found_objects.values()), "evidence": sorted(found_evidence.items())},
            code == 0 and found_objects == expected_objects and found_evidence == expected_evidence,
        )
    )
    expected_counts: dict[str, dict[str, Any]] = {
        "copies": {"objects": 8, "evidence_objects": 2},
        "entries": {"objects": 7, "evidence_objects": 2},
        "listing": {"delete_markers": 1, "versions": 15, "pages": 1},
        "payload": {"alias_copies": 1, "not_payload": 3},
        "cross_check": {
            "objects_named_by_records": 10,
            "retired_objects_absent": 1,
            "stored_objects_named": 7,
            "stored_objects_named_by_upload_record": 1,
            "stored_objects_explained_by_disposition": 1,
            "stored_objects_explained_by_object_disposition": 1,
            "retirement_identities_absent": 1,
        },
        "captures": {"stored": 2, "attempt_records": 1, "dispositions_applied": 1, "not_stored": {"rejected": 1}},
    }
    observed_counts = {name: {key: summary[name].get(key) for key in value} for name, value in expected_counts.items()}
    results.append(success("summary_counts_what_was_read_and_cross_checked", expected_counts, observed_counts, observed_counts == expected_counts))
    by_source = {name: summary["cross_check"].get(name, 0) for name in expected_counts["cross_check"] if name.startswith("stored_objects_")}
    stored_total = sum(summary["copies"].values())
    counted_once = {"stored_objects": stored_total, "by_source": sum(by_source.values())}
    results.append(
        success(
            "each_stored_object_is_counted_once_by_one_source",
            {"stored_objects": stored_total, "by_source": stored_total},
            counted_once,
            stored_total == sum(by_source.values()),
        )
    )
    shape_ok = (
        set(index) == {"schema_version", "generation", "objects", "evidence_objects"}
        and index["schema_version"] == 2
        and all(
            set(record) == {"bucket", "byte_count", "key", "sha256", "version_id"}
            for scope in ("objects", "evidence_objects")
            for record in index[scope].values()
        )
        and list(index["objects"]) == sorted(index["objects"])
        and index["objects"][digest(DATA_ONE)]["byte_count"] == len(DATA_ONE)
    )
    results.append(
        success(
            "index_has_only_the_fields_the_policy_reads", "schema_version 2, generation, sorted checksum maps, five fields per object", sorted(index), shape_ok
        )
    )
    upload_record = json.loads(workspace.upload_path.read_text())
    recognized = verify_storage_records.is_index_document(index) and not verify_storage_records.is_index_document(upload_record)
    results.append(success("built_index_is_recognized_by_its_format_and_an_upload_record_is_not", True, recognized, recognized))
    reported = summary["index"]
    results.append(
        success(
            "printed_checksum_is_the_file_checksum",
            digest(raw),
            reported["sha256"],
            reported["sha256"] == digest(raw) and reported["path"] == str(workspace.output) and reported["bytes"] == len(raw),
        )
    )
    expected_alias = [
        {
            "scope": "objects",
            "sha256": digest(DATA_THREE),
            "kept": {"key": expected_objects[digest(DATA_THREE)][1], "version_id": "v_d3", "stored_at": "2026-02-01T00:00:00Z"},
            "aliases": [{"key": key_for("datasets", DATA_THREE, "data_three_copy.csv", "two"), "version_id": "v_d3_copy", "stored_at": "2026-03-01T00:00:00Z"}],
        }
    ]
    results.append(success("alias_report_names_the_kept_copy_and_the_other_copy", expected_alias, summary["aliases"], summary["aliases"] == expected_alias))
    policy_path = workspace.root / "storage_policy.json"
    original_policy = (REPO_ROOT / "config/acquisition/storage_policy.json").read_bytes()
    command_ok = reported["sha256"] in switch_text and str(workspace.output) in switch_text and "single_copy_enabled" in switch_text
    results.append(success("switch_on_command_is_printed", "the command names the index path and checksum", command_ok, command_ok))
    write_json(policy_path, {"single_copy_enabled": True, "bootstrap_index": str(workspace.output), "bootstrap_index_sha256": reported["sha256"]})
    loaded = load_policy(policy_path)
    results.append(
        success(
            "load_policy_accepts_the_built_index",
            {"enabled": True, "objects": 7, "evidence_objects": 2, "bootstrap_sha256": reported["sha256"]},
            {
                "enabled": loaded["enabled"],
                "objects": len(loaded["objects"]),
                "evidence_objects": len(loaded["evidence_objects"]),
                "bootstrap_sha256": loaded["bootstrap_sha256"],
            },
            loaded["enabled"] and len(loaded["objects"]) == 7 and len(loaded["evidence_objects"]) == 2 and loaded["bootstrap_sha256"] == reported["sha256"],
        )
    )
    results.append(reuse_scenario(workspace, loaded))
    tampered = workspace.root / "tampered_index.json"
    tampered.write_bytes(raw.replace(b"v_d1", b"v_dX", 1))
    write_json(policy_path, {"single_copy_enabled": True, "bootstrap_index": str(tampered), "bootstrap_index_sha256": reported["sha256"]})
    try:
        load_policy(policy_path)
        rejected = False
    except CaptureError as error:
        rejected = "differ from the approved SHA-256" in str(error)
    results.append(success("load_policy_rejects_an_edited_index", "checksum difference reported", rejected, rejected))
    unchanged = (REPO_ROOT / "config/acquisition/storage_policy.json").read_bytes() == original_policy
    results.append(success("shipped_setting_is_untouched", "config/acquisition/storage_policy.json unchanged by the build", unchanged, unchanged))
    code_again, summary_again, _, _ = build(workspace)
    results.append(
        success(
            "rerun_gives_identical_bytes",
            {"exit_code": 0, "sha256": reported["sha256"]},
            {"exit_code": code_again, "sha256": summary_again["index"]["sha256"]},
            code_again == 0 and summary_again["index"]["sha256"] == reported["sha256"] and workspace.output.read_bytes() == raw,
        )
    )
    only_current = len(index["objects"]) + len(index["evidence_objects"]) == 9 and len([1 for kind, _ in fake.entries if kind == "Versions"]) == 15
    results.append(
        success(
            "only_current_payload_versions_enter_the_index",
            "15 listed versions and 1 delete marker give 9 entries: old versions, the deleted key, manifests and the lakehouse folder stay out",
            only_current,
            only_current,
        )
    )
    return results, workspace, summary


def name_by_upload_record(objects: list[Obj]) -> None:
    note = by_version(objects, "v_un")
    note.reason, note.upload = False, True


def listing_order_scenarios(root: Path, good: dict[str, Any]) -> list[dict[str, Any]]:
    """The same bucket in other page sizes and another order must give the same index bytes and the same listing checksum."""
    results = []
    for name, page_size, reverse in (("pages_of_one_entry", 1, False), ("pages_of_three_entries", 3, False), ("reversed_listing_order", 1000, True)):
        workspace = Workspace(root / name)
        fake = FakeAws(workspace.entries()[::-1] if reverse else workspace.entries())
        code, summary, _, fake = build(workspace, fake, page_size=page_size)
        pages = fake.pages
        passed = code == 0 and summary["index"]["sha256"] == good["index"]["sha256"] and summary["listing"]["sha256"] == good["listing"]["sha256"]
        passed = passed and summary["listing"]["pages"] == pages and (pages > 1 or page_size == 1000)
        results.append(
            success(
                f"listing_in_{name}_gives_the_same_index",
                {"exit_code": 0, "index_sha256": good["index"]["sha256"], "listing_sha256": good["listing"]["sha256"]},
                {"exit_code": code, "index_sha256": summary.get("index", {}).get("sha256"), "listing_sha256": summary["listing"]["sha256"], "pages": pages},
                passed,
            )
        )
    forward = Workspace(root / "alias_order_forward", extra_object(modified=OLDEST))
    backward = Workspace(root / "alias_order_backward", extra_object(modified=OLDEST))
    code_forward, summary_forward, _, _ = build(forward)
    code_backward, summary_backward, _, _ = build(backward, FakeAws(backward.entries()[::-1]))
    same = code_forward == code_backward == 0 and summary_forward["aliases"] == summary_backward["aliases"] and len(summary_forward["aliases"]) == 2
    same = same and forward.output.read_bytes() == backward.output.read_bytes()
    results.append(success("alias_report_order_ignores_listing_order", "two alias groups in the same order and the same index bytes", same, same))
    swapped = Workspace(root / "object_reason_replaced_by_an_upload_record", name_by_upload_record)
    code_swapped, summary_swapped, _, _ = build(swapped)
    same_index = code_swapped == 0 and summary_swapped["index"]["sha256"] == good["index"]["sha256"]
    results.append(success("the_source_of_a_name_does_not_change_the_index", {"exit_code": 0, "index_sha256": good["index"]["sha256"]}, same_index, same_index))
    nested = Workspace(root / "nested_receipt")
    nest_invalid_receipt(nested)
    code_nested, summary_nested, _, _ = build(nested)
    unchanged = code_nested == 0 and summary_nested["index"]["sha256"] == good["index"]["sha256"]
    results.append(success("capture_folders_are_not_entered", "an invalid receipt inside a capture folder is not read as a capture", unchanged, unchanged))
    return results


def same_index_scenario(
    root: Path,
    name: str,
    good: dict[str, Any],
    *,
    mutate: Callable[[Workspace], object] | None = None,
    fake: Callable[[FakeAws], object] | None = None,
    page_size: int = 1000,
) -> dict[str, Any]:
    """A variation that is valid must give the same index as the good set."""
    workspace = Workspace(root / name)
    if mutate:
        mutate(workspace)
    stand_in = FakeAws(workspace.entries())
    if fake:
        fake(stand_in)
    code, summary, _, _ = build(workspace, stand_in, page_size=page_size)
    observed = {"exit_code": code, "index_sha256": summary.get("index", {}).get("sha256"), "reasons": sorted(summary_reasons(summary))}
    expected = {"exit_code": 0, "index_sha256": good["index"]["sha256"], "reasons": []}
    return success(name, expected, observed, observed == expected)


def valid_variation_scenarios(root: Path, good: dict[str, Any]) -> list[dict[str, Any]]:
    """Valid variations of the records and of the end of the listing must not be refused."""

    def end_of_listing(fake: FakeAws) -> None:
        def mark(number: int, page: dict[str, Any]) -> None:
            if "NextToken" in page:
                page["IsTruncated"] = True
            else:
                page.update(IsTruncated=False, NextKeyMarker="", NextVersionIdMarker=None)

        fake.after_page = mark

    def retired_again(workspace: Workspace) -> None:
        add_retirement("v_gone", bytes=len(GONE))(workspace)
        add_retirement("v_gone", bytes=len(GONE))(workspace)
        again = workspace.root / "retired_objects_again.json"
        write_json(
            again,
            {"objects": [{"key": by_version(workspace.objects, "v_gone").full_key, "version_id": "v_gone", "status": "already_absent", "bytes": len(GONE)}]},
        )
        workspace.retirements.append(again)

    return [
        same_index_scenario(root, "pages_before_the_last_may_say_truncated_and_the_last_page_says_it_is_complete", good, fake=end_of_listing, page_size=3),
        same_index_scenario(
            root,
            "retirement_with_the_project_bucket_and_matching_fields_is_accepted",
            good,
            mutate=add_retirement("v_gone", bucket=BUCKET, bytes=len(GONE), sha256=digest(GONE)),
        ),
        same_index_scenario(root, "retirement_lines_that_repeat_with_the_same_fields_are_one_identity", good, mutate=retired_again),
        same_index_scenario(root, "upload_records_that_repeat_with_the_listing_size_are_accepted", good, mutate=add_upload_record()),
        same_index_scenario(root, "upload_record_with_the_listing_size_for_a_captured_object_is_accepted", good, mutate=add_upload_record("v_d1")),
    ]


def alias_scenario(root: Path, name: str, change: Callable[[list[Obj]], object], kept: str, aliases: list[str]) -> dict[str, Any]:
    """Build with copies of the bytes of data one; the index must name the copy stored first and the report must list the others."""
    workspace = Workspace(root / name, change)
    code, summary, _, _ = build(workspace)
    named = read_index(workspace.output)["objects"].get(digest(DATA_ONE), {}) if workspace.output.exists() else {}
    group = [item for item in summary["aliases"] if item["sha256"] == digest(DATA_ONE)]
    observed = {"exit_code": code, "named": named.get("version_id"), "aliases": [item["version_id"] for item in group[0]["aliases"]] if group else []}
    expected = {"exit_code": 0, "named": kept, "aliases": aliases}
    return success(name, expected, observed, observed == expected)


def alias_scenarios(root: Path) -> list[dict[str, Any]]:
    def replaced(objects: list[Obj]) -> None:
        extra_object(modified=SECOND)(objects)
        by_version(objects, "v_d1").modified = THIRD

    def old_copy(objects: list[Obj]) -> None:
        objects.append(Obj("datasets", "data_one.csv", DATA_ONE, "v_d1_old", (), OLDEST, latest=False))

    return [
        alias_scenario(root, "copies_name_the_earliest_stored_copy", extra_object(modified=OLDEST), "v_extra", ["v_d1"]),
        alias_scenario(root, "copies_with_equal_time_name_the_smaller_key", extra_object(modified=FIRST), "v_d1", ["v_extra"]),
        alias_scenario(
            root, "copies_with_equal_time_and_a_smaller_key_name_that_key", extra_object(modified=FIRST, name="a_data_one_elsewhere.csv"), "v_extra", ["v_d1"]
        ),
        alias_scenario(root, "a_replaced_copy_counts_with_the_time_of_its_current_version", replaced, "v_extra", ["v_d1"]),
        alias_scenario(root, "an_old_version_of_a_copy_is_not_an_alias", old_copy, "v_d1", []),
    ]


def reuse_scenario(workspace: Workspace, policy: dict[str, Any]) -> dict[str, Any]:
    """Offer the stored-object index to the real store step and require the existing object to be reused, not copied."""
    entry = policy["objects"][digest(DATA_ONE)]
    client = FakeS3()
    client.configuration["data_bucket_name"] = BUCKET
    client.storage_policy = policy
    checksum = base64.b64encode(hashlib.sha256(DATA_ONE).digest()).decode()
    client.objects[entry["key"]] = {
        "VersionId": entry["version_id"],
        "ContentLength": len(DATA_ONE),
        "ChecksumSHA256": checksum,
        "ServerSideEncryption": "AES256",
        "body": DATA_ONE,
    }
    local = workspace.root / "local_data_one.csv"
    local.write_bytes(DATA_ONE)
    record, created = s3_store.store_file(client, local, key_for("datasets", DATA_ONE, "data_one.csv", "a_later_capture"))
    operations = [operation for operation, _ in client.calls]
    passed = (
        not created
        and "put-object" not in operations
        and record["key"] == entry["key"]
        and record["version_id"] == entry["version_id"]
        and record["sha256"] == entry["sha256"]
    )
    return success(
        "store_step_reuses_an_indexed_object_without_writing",
        {"created": False, "writes": 0, "key": entry["key"]},
        {"created": created, "writes": operations.count("put-object"), "key": record["key"]},
        passed,
    )


def budget_scenario(root: Path) -> dict[str, Any]:
    """The identity call and every page get only the time that is left of the one deadline."""
    code, _, _, fake = build(Workspace(root / "budget"), None, "--deadline", "5")
    values = [value for _, value in fake.timeouts]
    operations = [operation for operation, _ in fake.timeouts]
    in_range = bool(values) and all(0 < value <= 5 for value in values)
    return success(
        "identity_call_and_pages_use_the_remaining_deadline",
        {"exit_code": 0, "first_call": "get-caller-identity", "timeouts_within": 5, "timeouts_do_not_grow": True},
        {"exit_code": code, "calls": operations, "timeouts": [round(value, 2) for value in values]},
        code == 0 and operations[:1] == ["get-caller-identity"] and in_range and values == sorted(values, reverse=True),
    )


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@dataclass
class LauncherTrace:
    """What the launcher did on its own clock: its deadline, when it started each AWS call and the process limit it gave that call."""

    skew: float = 0.0
    deadline_at: float | None = None
    calls: list[tuple[str, float, float]] = field(default_factory=list)

    def now(self) -> float:
        """Return the launcher's clock: real time plus the seconds the identity answer is said to have taken."""
        return time.monotonic() + self.skew

    def budget_kept(self) -> bool:
        """Return True when no call was given a process limit that ends after the deadline; a few milliseconds of rounding are allowed."""
        return self.deadline_at is not None and all(started + limit <= self.deadline_at + 0.05 for _, started, limit in self.calls)

    def started(self, operation: str) -> int:
        return len([1 for name, _, _ in self.calls if name == operation])


def traced_runner(trace: LauncherTrace, identity_wait: float) -> type[single_copy_index.DeadlineRunner]:
    """Return the launcher's deadline runner, recording each call; the identity call, once answered, counts `identity_wait` seconds on the launcher's clock."""

    class TracedRunner(single_copy_index.DeadlineRunner):
        def __init__(self, run: Callable[..., Any], deadline_at: float) -> None:
            def recorded(argv: list[str], **options: Any) -> Any:
                trace.calls.append((argv[2], trace.now(), options["timeout"]))
                result = run(argv, **options)
                if argv[2] == "get-caller-identity":
                    trace.skew += identity_wait
                return result

            trace.deadline_at = deadline_at
            super().__init__(recorded, deadline_at)

    return TracedRunner


def stand_in_script(pid_file: Path, identity: str, listing: str) -> str:
    """Return an `aws` stand-in whose identity and listing commands each wait, answer or both."""
    arn = f"arn:aws:iam::{CONFIGURATION['expected_account_id']}:user/{CONFIGURATION['aws_profile']}"
    reply = json.dumps({"Account": CONFIGURATION["expected_account_id"], "Arn": arn})
    answers = {"never": "exec sleep 30", "now": f"echo '{reply}'"}
    return f'#!/bin/sh\necho "$1 $$" >> \'{pid_file}\'\ncase "$1" in\n  sts) {answers[identity]} ;;\n  s3api) {listing} ;;\nesac\n'


def launcher_scenario(
    root: Path,
    name: str,
    identity: str,
    listing: str,
    deadline: float,
    *,
    limit: float,
    reasons: list[str],
    listing_requests: int | None = None,
    identity_wait: float = 0.0,
    mutate: Callable[[Workspace], object] | None = None,
) -> dict[str, Any]:
    """Run the real launcher against an `aws` stand-in; no process may outlive the one overall deadline. No reason means the build must succeed.

    The limits are on the launcher's own clock, so a slow machine cannot fail a case that keeps its deadline: every process limit ends by the
    deadline, the count of listing requests is the launcher's own, and the wall clock only bounds a launcher that waits for a stand-in that never
    answers. It runs from the launcher's own deadline start, not from the start of the command. `identity_wait` is the time an answered identity
    call is said to have taken: it moves the launcher's clock and never sleeps, so the case does not race a real sleep against the deadline.
    """
    bin_folder = root / f"stand_in_bin_{name}"
    bin_folder.mkdir()
    pid_file = bin_folder / "pids.txt"
    stand_in = bin_folder / "aws"
    stand_in.write_text(stand_in_script(pid_file, identity, listing))
    stand_in.chmod(0o755)
    workspace = Workspace(root / f"launcher_{name}")
    if mutate:
        mutate(workspace)
    trace = LauncherTrace()
    started = time.monotonic()
    with (
        patch.dict(os.environ, {"PATH": f"{bin_folder}{os.pathsep}{os.environ['PATH']}"}),
        patch.object(single_copy_index, "load_configuration", lambda _path: (CONFIGURATION, {})),
        patch.object(single_copy_index, "time", SimpleNamespace(monotonic=trace.now)),
        patch.object(single_copy_index, "DeadlineRunner", traced_runner(trace, identity_wait)),
    ):
        code, summary, _ = run(workspace.arguments("--deadline", str(deadline)))
    elapsed = time.monotonic() - (started if trace.deadline_at is None else trace.deadline_at - deadline)
    lines = [line.split() for line in pid_file.read_text().splitlines()] if pid_file.exists() else []
    alive = [int(pid) for _, pid in lines if process_alive(int(pid))]
    for pid in alive:
        os.kill(pid, signal.SIGKILL)
    started_listing = trace.started("list-object-versions")
    observed = {
        "exit_code": code,
        "reasons": sorted(summary_reasons(summary)),
        "seconds": round(elapsed, 2),
        "process_limits": {f"{operation} at {started - (trace.deadline_at or 0) + deadline:.2f}": round(limit, 2) for operation, started, limit in trace.calls},
        "process_limits_end_by_the_deadline": trace.budget_kept(),
        "listing_requests_started": started_listing,
        "processes_alive_at_return": len(alive),
        "index_written": workspace.output.exists(),
    }
    passed = code == (1 if reasons else 0) and observed["reasons"] == reasons and elapsed < limit and not alive and workspace.output.exists() == (not reasons)
    passed = passed and trace.budget_kept() and (listing_requests is None or started_listing == listing_requests)
    expected = {
        "exit_code": 1 if reasons else 0,
        "reasons": reasons,
        "seconds_below": limit,
        "process_limits_end_by_the_deadline": True,
        "listing_requests_started": listing_requests,
        "processes_alive_at_return": 0,
    }
    return {"name": name, "kind": "refusal" if reasons else "success", "expected": expected, "observed": observed, "passed": passed}


def launcher_scenarios(root: Path) -> list[dict[str, Any]]:
    objects = good_objects()
    page_file = root / "one_page.json"
    page_file.write_text(json.dumps({"Versions": [obj.entry() for obj in objects if obj.listed], "DeleteMarkers": []}))
    return [
        launcher_scenario(root, "launcher_listing_answers", "now", f"cat '{page_file}'", 5.0, limit=4.0, reasons=[], listing_requests=1),
        launcher_scenario(
            root, "launcher_listing_never_answers_deadline_1", "now", "exec sleep 30", 1.0, limit=4.0, reasons=["listing_deadline"], listing_requests=1
        ),
        launcher_scenario(
            root, "launcher_listing_never_answers_deadline_half", "now", "exec sleep 30", 0.5, limit=3.5, reasons=["listing_deadline"], listing_requests=1
        ),
        launcher_scenario(root, "launcher_identity_never_answers", "never", "exec sleep 30", 1.0, limit=4.0, reasons=["listing_deadline"], listing_requests=0),
        launcher_scenario(
            root,
            "launcher_identity_time_counts_against_the_deadline",
            "now",
            "exec sleep 30",
            3.0,
            limit=4.0,
            reasons=["listing_deadline"],
            listing_requests=1,
            identity_wait=2.0,
        ),
        launcher_scenario(
            root,
            "launcher_listing_answers_and_the_reasons_file_is_missing",
            "now",
            f"cat '{page_file}'",
            5.0,
            limit=4.0,
            reasons=["stored_object_without_record"],
            listing_requests=1,
            mutate=lambda w: setattr(w, "object_dispositions", None),
        ),
        launcher_scenario(root, "launcher_listing_answers_garbage", "now", "echo 'not json'", 5.0, limit=4.0, reasons=["listing_failed"], listing_requests=1),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="the run artifact; its folder must be ignored by Git")
    arguments = parser.parse_args()
    started = datetime.now(UTC)
    arguments.output.resolve().parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="single_copy_index_work_", dir=arguments.output.resolve().parent))
    scenarios: list[dict[str, Any]] = []
    try:
        good_results, _, good_summary = good_scenarios(work)
        scenarios.extend(good_results)
        scenarios.extend(refusal_scenario(case, work) for case in refusal_cases())
        if "index" in good_summary:
            scenarios.extend(listing_order_scenarios(work, good_summary))
            scenarios.extend(alias_scenarios(work))
            scenarios.extend(valid_variation_scenarios(work, good_summary))
            scenarios.append(budget_scenario(work))
            scenarios.extend(launcher_scenarios(work))
    finally:
        shutil.rmtree(work)
    passed = all(item["passed"] for item in scenarios)
    report = {
        "kind": "single_copy_index_e2e",
        "status": "passed" if passed else "failed",
        "started_utc": started.isoformat(),
        "entry_point": "scripts.acquisition.single_copy_index.main",
        "implementation_sha256": {path.name: digest(path.read_bytes()) for path in (Path(single_copy_index.__file__), Path(__file__))},
        "python": sys.version.split()[0],
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_single_copy_index_e2e --output {arguments.output}",
        "total": len(scenarios),
        "passed": sum(1 for item in scenarios if item["passed"]),
        "scenarios": scenarios,
        "limits": [
            "The bucket listing, the download records and the identity answer are generated; no AWS call and no real download record is read.",
            "Most cases answer the AWS CLI calls from a stand-in function; the real AWS command line and the real bucket's answers are not covered.",
            "The launcher cases use the real process launcher and an `aws` stand-in that answers or never answers; the real AWS CLI is not run.",
            "The late identity answer moves the launcher's clock and never sleeps. The launcher cases check the process limits it gave, not the wall clock.",
            "Reuse by the store step is covered through the existing fake store, not a real bucket.",
        ],
        "cleanup": "The generated records were removed after the run.",
    }
    write_once(arguments.output, encoded_json(report))
    if not passed:
        failed = [item["name"] for item in scenarios if not item["passed"]]
        sys.stderr.write(f"Failed scenarios: {failed}\n")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
