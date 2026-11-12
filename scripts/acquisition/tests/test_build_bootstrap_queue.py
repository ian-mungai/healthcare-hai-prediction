"""The trimmed bootstrap queue keeps only active units, records where it came from and binds a rewritten hosts file, deterministically."""

from __future__ import annotations

import hashlib
import json

import pytest

from scripts.acquisition import build_bootstrap_queue as build
from scripts.acquisition.transport import hosts_digest
from tests.support import check

HOSTS = ["api.example.invalid", "files.example.invalid", "unused.example.invalid"]


def unit(snapshot: str, disposition: str, host: str = "files.example.invalid") -> dict:
    return {
        "bytes": 10,
        "disposition": disposition,
        "flags": [],
        "mode": "file",
        "pages": 1,
        "route": "publisher_url",
        "snapshot_id": snapshot,
        "source_id": "S",
        "url": f"https://{host}/{snapshot}",
    }


def source_queue() -> dict:
    units = [
        unit("a", "api", "api.example.invalid"),
        unit("b", "direct_download"),
        unit("c", "approved_manual"),
        unit("d", "not_used_by_bronze"),
        unit("e", "superseded_duplicate"),
        unit("f", "dropped"),
    ]
    return {
        "budgets": {"acquisition_units": 3, "recorded_bytes": 30},
        "dispositions": {"api": 1, "approved_manual": 1, "direct_download": 1, "dropped": 1, "not_used_by_bronze": 1, "superseded_duplicate": 1},
        "entries": 6,
        "execution_status": "frozen_offline_no_requests_made",
        "inputs_sha256": {"x": "0" * 64},
        "kind": "bounded_redownload_queue",
        "loaded_files_no_unit_covers": ["L1"],
        "model_eligible": False,
        "queue": units,
        "run": 3,
    }


def source_bytes() -> bytes:
    return (json.dumps(source_queue(), indent=1, sort_keys=True) + "\n").encode()


def test_source_digest_mismatch_is_refused() -> None:
    with pytest.raises(ValueError, match="source queue digest"):
        build.trim_queue(source_bytes(), "f" * 64)


def test_trimmed_queue_keeps_only_active_units_and_records_its_source() -> None:
    body = source_bytes()
    digest = hashlib.sha256(body).hexdigest()
    trimmed = json.loads(build.trim_queue(body, digest))
    check([u["snapshot_id"] for u in trimmed["queue"]] == ["a", "b", "c"], "kept units")
    check(trimmed["kind"] == "bootstrap_capture_queue" and "run" not in trimmed, "kind replaces run")
    check(trimmed["entries"] == 3 and trimmed["dispositions"] == {"api": 1, "approved_manual": 1, "direct_download": 1}, "header counts")
    derived = trimmed["derived_from"]
    check(derived["sha256"] == digest and derived["entries"] == 6, "source digest and entry count")
    check(derived["dropped_by_disposition"] == {"dropped": 1, "not_used_by_bronze": 1, "superseded_duplicate": 1}, "dropped counts")
    check(derived["dropped_snapshot_ids_sha256"] == hashlib.sha256("\n".join(["d", "e", "f"]).encode()).hexdigest(), "dropped id digest")
    check(derived["inputs_sha256"] == {"x": "0" * 64}, "source inputs copied")


def test_trim_is_deterministic() -> None:
    body = source_bytes()
    digest = hashlib.sha256(body).hexdigest()
    check(build.trim_queue(body, digest) == build.trim_queue(body, digest), "same bytes twice")


def test_a_source_that_already_holds_a_kept_disposition_it_cannot_classify_is_refused() -> None:
    queue = source_queue()
    queue["queue"].append(unit("g", "mystery"))
    body = (json.dumps(queue, indent=1, sort_keys=True) + "\n").encode()
    with pytest.raises(ValueError, match="unknown disposition"):
        build.trim_queue(body, hashlib.sha256(body).hexdigest())


def test_hosts_file_keeps_the_approved_list_and_binds_the_new_queue() -> None:
    body = source_bytes()
    trimmed = build.trim_queue(body, hashlib.sha256(body).hexdigest())
    approved = {"hosts": HOSTS, "hosts_sha256": hosts_digest(HOSTS), "queue_sha256": "0" * 64}
    document = json.loads(build.rewrite_hosts(approved, trimmed))
    check(document["hosts"] == HOSTS and document["hosts_sha256"] == hosts_digest(HOSTS), "host list kept")
    check(document["queue_sha256"] == hashlib.sha256(trimmed).hexdigest(), "binds the trimmed queue")


def test_hosts_file_that_misses_a_queue_host_is_refused() -> None:
    body = source_bytes()
    trimmed = build.trim_queue(body, hashlib.sha256(body).hexdigest())
    short = [HOSTS[0]]
    with pytest.raises(ValueError, match="hosts the queue needs"):
        build.rewrite_hosts({"hosts": short, "hosts_sha256": hosts_digest(short), "queue_sha256": "0" * 64}, trimmed)


def test_a_flag_with_a_date_suffix_that_the_secret_scan_reads_as_a_key_is_shortened_and_recorded() -> None:
    queue = source_queue()
    long_flag = "browser_export_moved_to_api:parity_verified_" + "2" * 8 + "_" + "3" * 8
    queue["queue"][0]["flags"] = [long_flag, "other"]
    body = (json.dumps(queue, indent=1, sort_keys=True) + "\n").encode()
    trimmed = json.loads(build.trim_queue(body, hashlib.sha256(body).hexdigest()))
    check(trimmed["queue"][0]["flags"] == ["browser_export_moved_to_api:parity_verified", "other"], "flag shortened")
    expected = {"browser_export_moved_to_api:parity_verified": {"dated_suffixes_removed": ["2" * 8 + "_" + "3" * 8], "units": 1}}
    check(trimmed["derived_from"]["flag_rewrites"] == expected, "rewrite recorded without the long flag")
