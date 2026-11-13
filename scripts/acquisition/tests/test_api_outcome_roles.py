"""Owner decision A, Nov 12 2026: the recheck and the verifier compare the artifacts the capture compared, so a changed raw API page does not undo a match."""

from __future__ import annotations

import copy
import json
from datetime import timedelta
from functools import partial
from pathlib import Path

import pytest

from scripts.acquisition import collection_layout, s3_store, task_role, transport
from scripts.acquisition import redownload as harness
from scripts.acquisition import verify_publisher_run as verifier
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.tests.task_fixture import task_run
from scripts.acquisition.tests.test_task_keys import ENVIRONMENT, KEY_OF_MODE, Cloud
from scripts.acquisition.tests.test_task_owners import storage
from scripts.acquisition.tests.test_task_raw_store import STORED, file_run, restart, rewrite_outcome, stored_unit
from scripts.acquisition.tests.test_verify_publisher_run import SINCE, Stamped, state_body
from tests.support import check

PAGE = b'{"status": "REQUEST_SUCCEEDED", "message": []}\n'
CHANGED_PAGE = b'{"status": "REQUEST_SUCCEEDED", "message": ["changed"]}\n'
CHANGED_DATA = b"a,b\n3,4\n"


@pytest.fixture(autouse=True)
def raw_route(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")


def fresh_download(folder: Path, unit: dict, name: str, body: bytes) -> harness.Download:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(body)
    return harness.Download(
        path=path,
        requested_url=unit["url"],
        resolved_url=unit["url"],
        retrieved_at_utc="2026-11-01T12:00:00+00:00",
        http_status=200,
        byte_count=len(body),
        sha256=harness.digest(body),
        media_type=None,
        complete=True,
        failure=None,
        partial=False,
        attempt=1,
        original_file_name=name,
    )


def api_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, bucket: Stamped, mode: str, data: bytes, page: bytes) -> tuple[harness.Run, dict]:
    """An API unit as the Nov 12 run saw it: the stored capture holds the derived data and the raw page, and the collector makes a fresh capture of both."""
    fixture, unit = stored_unit(monkeypatch, tmp_path, STORED, "a.csv")
    receipt_path = tmp_path / unit["receipt"]
    document = read_json(receipt_path)
    (receipt_path.parent / "raw" / "response.json").write_bytes(PAGE)
    stored_page = copy.deepcopy(document["artifacts"][0]) | {
        "artifact_id": "artifact_0002",
        "role": "api_page",
        "original_file_name": "response.json",
        "stored_file_name": "response.json",
        "storage_path": "raw/response.json",
        "byte_count": len(PAGE),
        "sha256": harness.digest(PAGE),
    }
    requests = [{"requested_url": unit["url"]}, {"requested_url": unit["url"]}]
    lineage = json.loads(document["lineage"]["extraction_or_query"]) | {"batch_id": "b1", "requests": requests}
    document["lineage"]["extraction_or_query"] = json.dumps(lineage)
    document["artifacts"].append(stored_page)
    receipt_path.write_bytes(encoded_json(document))
    unit |= {"mode": mode, "disposition": "api", "route": "publisher_api", "receipt_sha256": harness.digest(receipt_path.read_bytes())}

    def collector(unit: dict, run: harness.Run) -> Path:
        folder = run.state_root / "files" / unit["snapshot_id"]
        downloads = [fresh_download(folder, unit, document["artifacts"][0]["stored_file_name"], data), fresh_download(folder, unit, "response.json", page)]
        return harness.store_capture(unit, run, downloads)

    cloud = Cloud()
    keys = partial(task_role.TaskKeys, environment=ENVIRONMENT, factory=cloud.factory)
    return task_run(tmp_path, fixture, adapters={mode: collector}, task_keys=keys, task_storage=storage(bucket)), unit


def verified(run: harness.Run, monkeypatch: pytest.MonkeyPatch) -> dict:
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))
    return verifier.check_state(run, state_body(run), SINCE, SINCE + timedelta(days=1), run.task_storage)


MODES = sorted(KEY_OF_MODE)
ALL_VERIFIED = {"outcomes": 1, "verified": 1, "not_verified": [], "outside_window": []}


@pytest.mark.parametrize("mode", MODES)
def test_an_api_unit_with_the_same_data_and_a_changed_raw_page_is_an_exact_match_the_recheck_and_the_verifier_accept(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mode: str
) -> None:
    run, _unit = api_run(monkeypatch, tmp_path, Stamped(SINCE + timedelta(hours=1)), mode, STORED, CHANGED_PAGE)
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "the capture compares the data only")
    check(harness.execute(restart(run))["attempted"] == 0, "the restore recheck accepts the claim")
    report = verified(run, monkeypatch)
    check(report == ALL_VERIFIED, str(report))


@pytest.mark.parametrize("page", [PAGE, CHANGED_PAGE])
def test_an_api_unit_with_changed_data_is_changed_on_both_sides_whatever_its_raw_page_does(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, page: bytes
) -> None:
    run, unit = api_run(monkeypatch, tmp_path, Stamped(SINCE + timedelta(hours=1)), "bls_api_v2", CHANGED_DATA, page)
    check(harness.execute(run)["outcomes"] == {"changed_needs_review": 1}, "the capture sees the changed data")
    check(harness.execute(restart(run))["attempted"] == 0, "the restore recheck accepts the claim")
    check(verified(run, monkeypatch) == ALL_VERIFIED, "the verifier accepts the claim")
    rewrite_outcome(run, unit, outcome="exact_match")
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(restart(run))


def test_an_api_unit_with_nothing_changed_is_exact_and_one_relabelled_changed_stops_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    run, unit = api_run(monkeypatch, tmp_path, Stamped(SINCE + timedelta(hours=1)), "bls_api_v2", STORED, PAGE)
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "nothing changed")
    check(harness.execute(restart(run))["attempted"] == 0, "the claim holds")
    rewrite_outcome(run, unit, outcome="changed_needs_review")
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(restart(run))


def test_a_direct_download_unit_is_decided_as_before(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "an unchanged file is an exact match")
    check(harness.execute(restart(run))["attempted"] == 0, "the recheck accepts it")
    check(verified(run, monkeypatch) == ALL_VERIFIED, "the verifier accepts it")
