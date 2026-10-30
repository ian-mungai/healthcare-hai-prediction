from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, cast

import pytest

from scripts.acquisition import collection_layout as layout
from scripts.acquisition import s3_store
from scripts.acquisition.s3_store import AwsCli
from scripts.acquisition.single_copy import reference_scope
from scripts.acquisition.transport import CaptureError
from tests.support import check

ROUTE = {"publisher": "example", "collection": "collection"}
BUCKET = "bucket"
ROLES = (("data", False), ("capture_receipt", False), ("dictionary", False), ("transport_audit", True))


def test_without_a_capture_root_the_layout_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(layout.CAPTURE_ROOT_VARIABLE, raising=False)
    check(layout.object_prefix(ROUTE, "table", "data", "2026", "run", False) == "example/collection/datasets/table/release_date=2026", "root layout unchanged")


@pytest.mark.parametrize(("role", "evidence"), ROLES)
def test_the_raw_route_puts_every_new_prefix_under_raw(monkeypatch: pytest.MonkeyPatch, role: str, evidence: bool) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, "raw/")
    prefix = layout.object_prefix(ROUTE, "table", role, "2026", "run", evidence)
    check(prefix.startswith("raw/example/collection/"), f"{role} prefix starts with raw/")


def test_a_capture_root_other_than_empty_or_raw_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, "elsewhere/")
    with pytest.raises(CaptureError):
        layout.object_prefix(ROUTE, "table", "data", "2026", "run", False)


class Stop(Exception):
    pass


class Client:
    def __init__(self) -> None:
        self.storage_policy: dict[str, Any] = {"enabled": False}
        self.configuration = {"data_bucket_name": BUCKET}

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        raise Stop(operation)


def test_a_capture_never_writes_outside_raw(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, "raw/")
    path = tmp_path / "file.csv"
    path.write_bytes(b"a,b\n1,2\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    root_key = f"example/collection/datasets/table/release_date=2026/{digest}/file.csv"
    with pytest.raises(CaptureError, match="outside raw/"):
        s3_store.store_file(cast(AwsCli, Client()), path, root_key)
    with pytest.raises(Stop):
        s3_store.store_file(cast(AwsCli, Client()), path, f"raw/{root_key}")


def test_a_single_copy_reference_under_raw_has_the_same_scope_as_one_at_the_root() -> None:
    digest = "a" * 64
    tail = f"example/collection/datasets/table/release_date=2026/{digest}/file.csv"
    check(reference_scope(f"raw/{tail}", digest) == reference_scope(tail, digest) == "objects", "raw and root payload keys share a scope")
    audit = f"example/collection/audit/run/{digest}/file.json"
    check(reference_scope(f"raw/{audit}", digest) == reference_scope(audit, digest) == "evidence_objects", "raw and root evidence keys share a scope")
