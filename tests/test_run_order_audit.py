"""The order audit: a run ID carries no time, so code that needs the time order of runs reads the recorded times (commit times, object times) instead."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from scripts.features import online_reload, retired_publications
from scripts.infrastructure import openmetadata_raw_files as raw_files
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
EARLY = datetime(2026, 11, 11, 23, 59, 59, tzinfo=UTC)
LATE = datetime(2026, 11, 12, 0, 0, 1, tzinfo=UTC)


def retirement_file(tmp_path: Path, before: str = "2026-11-12T00:00:00Z") -> Path:
    path = tmp_path / "retired.json"
    path.write_text(json.dumps({"schema": 1, "namespaces": {"features": {"retired_before": before}}}))
    return path


def test_a_publication_is_retired_by_its_recorded_commit_time_and_not_by_its_run_id(tmp_path: Path) -> None:
    path = retirement_file(tmp_path)
    check(retired_publications.is_retired("features", EARLY, path), "a publication committed before the cutover is not retired")
    check(not retired_publications.is_retired("features", LATE, path), "a publication committed after the cutover is retired")
    check(not retired_publications.is_retired("features", "2026-11-12T00:00:00+00:00", path), "a text commit time at the cutover is retired")
    check(retired_publications.is_retired("features", "2026-11-11T23:00:00Z", path), "a text commit time before the cutover is not retired")
    check(not retired_publications.is_retired("features_e2e", EARLY, path), "a namespace without an entry retires something")
    naive = datetime(2026, 11, 11, 1, 0, 0)
    check(retired_publications.is_retired("features", naive, path), "a naive commit time is not read as UTC")


@pytest.mark.parametrize("value", ["20261112T000000Z", "2026-11-12", "yesterday", "", 5])
def test_a_cutover_that_is_not_a_utc_time_is_refused(tmp_path: Path, value: Any) -> None:
    with pytest.raises(retired_publications.RetiredError):
        retired_publications.cutover("features", retirement_file(tmp_path, value))


@pytest.mark.parametrize("value", ["not a time", None, 5])
def test_a_commit_time_that_is_not_a_time_is_refused(tmp_path: Path, value: Any) -> None:
    with pytest.raises(retired_publications.RetiredError):
        retired_publications.is_retired("features", value, retirement_file(tmp_path))


def test_the_shipped_retirement_record_has_a_utc_cutover() -> None:
    found = retired_publications.cutover("features")
    check(found == datetime(2026, 11, 12, tzinfo=UTC), f"the shipped cutover is {found}")


class Table:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def scan(self, selected_fields: Any = None) -> Table:
        return self

    def to_arrow(self) -> Table:
        return self

    def to_pylist(self) -> list[dict[str, Any]]:
        return self.rows


def test_the_committed_runs_of_a_set_are_newest_first_by_commit_time_whatever_their_run_ids_say(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = {"model_sets": {"m2": {"table": "features_m2"}}}
    rows = [
        {"run_id": "features_elastic_net_ffffffff", "committed_at": datetime(2026, 12, 1, tzinfo=UTC), "snapshot_set": {"features_m2": 1}},
        {"run_id": "features_elastic_net_00000000", "committed_at": datetime(2026, 12, 3, tzinfo=UTC), "snapshot_set": {"features_m2": 2}},
        {"run_id": "features_elastic_net_7a7a7a7a", "committed_at": datetime(2026, 12, 2, tzinfo=UTC), "snapshot_set": {"features_m2": 3}},
    ]
    monkeypatch.setattr(online_reload.reads, "load_table", lambda catalog, namespace, name: Table(rows))
    catalog: Any = object()
    found = online_reload.committed_runs(catalog, "features_e2e", "m2", schema)
    check(found == ["features_elastic_net_00000000", "features_elastic_net_7a7a7a7a", "features_elastic_net_ffffffff"], f"{found}")


class Head:
    def __init__(self, times: dict[str, datetime]) -> None:
        self.times = times

    def head_object(self, **arguments: str) -> dict[str, Any]:
        run = arguments["Key"].split("/")[2]
        return {"LastModified": self.times[run]}


def test_the_bronze_runs_are_written_oldest_first_by_the_time_of_their_run_record() -> None:
    times = {
        "bronze_ffffffff": datetime(2026, 12, 1, tzinfo=UTC),
        "bronze_00000000": datetime(2026, 12, 3, tzinfo=UTC),
        "bronze_7a7a7a7a_2": datetime(2026, 12, 2, tzinfo=UTC),
    }
    found = raw_files.recorded_order(Head(times), "bucket", list(times))
    check(found == ["bronze_ffffffff", "bronze_7a7a7a7a_2", "bronze_00000000"], f"{found}")
    same = {"bronze_bbbbbbbb": datetime(2026, 12, 1, tzinfo=UTC), "bronze_aaaaaaaa": datetime(2026, 12, 1, tzinfo=UTC)}
    check(raw_files.recorded_order(Head(same), "bucket", list(same)) == ["bronze_aaaaaaaa", "bronze_bbbbbbbb"], "a tie is not broken by the run ID")


def test_a_bronze_run_with_no_record_is_written_first_and_fails_by_name() -> None:
    class Missing:
        def head_object(self, **arguments: str) -> dict[str, Any]:
            from botocore.exceptions import ClientError

            if "gone" in arguments["Key"]:
                raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
            return {"LastModified": datetime(2026, 12, 1, tzinfo=UTC)}

    found = raw_files.recorded_order(Missing(), "bucket", ["bronze_bbbbbbbb", "bronze_gone0000"])
    check(found == ["bronze_gone0000", "bronze_bbbbbbbb"], f"{found}")


TIME_FROM_ID = re.compile(r"run[a-z_]*\[:\s*1[56]\]|\[:STAMP_LENGTH\]|strptime\([^)]*run|_run\[:16\]")


FOLDERS = ("dags", "orchestration", "scripts", "infra", "services", "monitoring", "feature_store", "features", ".github")


def tracked_code() -> list[Path]:
    found = []
    for folder in FOLDERS:
        for path in sorted((ROOT / folder).rglob("*")):
            relative = path.relative_to(ROOT).as_posix()
            skipped = "/tests/" in relative or "e2e" in relative or "_cases" in relative or ".terraform" in relative
            if path.suffix in (".py", ".sh", ".tf") and path.is_file() and not skipped:
                found.append(path)
    return found


def test_no_code_reads_a_time_out_of_a_run_id() -> None:
    found = []
    for path in tracked_code():
        if path.name == "test_run_order_audit.py" or not path.exists():
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if TIME_FROM_ID.search(line):
                found.append(f"{path.relative_to(ROOT)}:{number}: {line.strip()[:100]}")
    check(not found, "code reads a time from a run ID:\n" + "\n".join(found))
