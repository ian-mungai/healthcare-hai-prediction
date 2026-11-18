"""A declared table with no source object yet is left out of a full load and recorded, and appears normally once its first source object arrives.

Live failure of Nov 18 2026 (run bronze_80d24059): ``bronze_stage`` left ``hcai_util_workbook_sheet_rows`` out of the request because no object matched its
member pattern, and the metadata build then refused the generation because the committed map had one table more than the observations.

Failure modes of the guard, each with a case below: the left-out table is not in the map; it is also in the request; the previous generation already holds
observations of it; the pinned manifests name a source object for it; the generation misses another declared table; the first publication names tables that
are neither requested nor left out.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from scripts.lakehouse import bronze, bronze_metadata, bronze_run
from tests.support import check

RUN = "bronze_3f9a1c07"
BUCKET = "example-bucket"
DECLARED = ["first", "second", "third"]


def item(name: str) -> dict[str, Any]:
    return dict(
        table=name,
        observation_id=name,
        object_key=name,
        sha256=hashlib.sha256(name.encode()).hexdigest(),
        bucket="synthetic",
        key=name,
        version_id="v1",
        source_id="synthetic",
        snapshot_id="capture",
        dataset_id=name,
        release_id="2026-01-01",
        release_partition="2026-01-01",
        manifest_key=name + "/manifest.json",
        manifest_version_id="v1",
        manifest_sha256="0" * 64,
        member_chain=[name + ".csv"],
        file_name=name + ".csv",
        byte_count=1,
    )


def generation(names: list[str], previous: list[dict[str, Any]] | None = None, unsourced: list[str] | None = None) -> list[dict[str, Any]]:
    """The metadata build of a run that requests ``names`` for the three declared tables, with the left-out tables passed only when ``unsourced`` is given."""
    extra = {} if unsourced is None else {"unsourced": unsourced}
    return bronze_metadata.build(names, [item(name) for name in names], [], previous or [], DECLARED, **extra)


def tables_of(rows: list[dict[str, Any]]) -> set[str]:
    return {row["table_name"] for row in rows if row["kind"] == "observation"}


def earlier() -> list[dict[str, Any]]:
    """The published generation before the failing run: two of the three declared tables, as the live bucket's metadata was."""
    return bronze_metadata.build(["first", "second"], [item("first"), item("second")], [], [], ["first", "second"])


def test_a_declared_table_nobody_named_as_left_out_is_still_refused() -> None:
    with pytest.raises(bronze_metadata.MetadataError, match="missing a committed bronze table"):
        generation(["first", "second"], earlier())


def test_a_declared_table_with_no_source_object_is_left_out_and_the_generation_holds_every_other_table() -> None:
    rows = generation(["first", "second"], earlier(), unsourced=["third"])
    check(tables_of(rows) == {"first", "second"}, f"{tables_of(rows)}")
    bronze_metadata.validate(rows)
    bronze_metadata.validate_successor(earlier(), rows)
    check(rows == generation(["first", "second"], earlier(), unsourced=["third"]), "a retry built another generation")


def test_the_diagnostic_of_the_same_run_accepts_the_left_out_table_too() -> None:
    report = bronze_metadata.diagnose(["first", "second"], [item("first"), item("second")], [], earlier(), DECLARED, unsourced=["third"])
    check(report["passed"] is True, f"{report.get('error')}")


@pytest.mark.parametrize(
    "unsourced",
    [["nowhere"], ["first"], ["second", "third"]],
    ids=["a table of no map", "a requested table", "a table with published observations"],
)
def test_a_left_out_table_that_is_undeclared_requested_or_already_published_is_refused(unsourced: list[str]) -> None:
    with pytest.raises(bronze_metadata.MetadataError):
        generation(["first"], earlier(), unsourced=unsourced)


def test_a_table_with_sources_that_lost_its_observations_is_refused_whether_or_not_another_table_is_left_out() -> None:
    previous = generation(["first", "second", "third"])
    with pytest.raises(bronze_metadata.MetadataError):
        bronze_metadata.build(["first", "second", "third"], [item("first"), item("second")], [], previous, DECLARED, unsourced=[])
    with pytest.raises(bronze_metadata.MetadataError):
        bronze_metadata.build(["first", "second", "third"], [item("first"), item("second")], [], previous, DECLARED)
    with pytest.raises(bronze_metadata.MetadataError, match="published observations"):
        bronze_metadata.build(["first", "second"], [item("first"), item("second")], [], previous, DECLARED, unsourced=["third"])


def test_a_generation_that_misses_a_second_table_is_refused_although_one_table_is_left_out() -> None:
    only_first = bronze_metadata.build(["first"], [item("first")], [], [], ["first"])
    with pytest.raises(bronze_metadata.MetadataError, match="missing a committed bronze table"):
        generation(["first"], only_first, unsourced=["third"])


def test_the_first_publication_may_leave_out_a_table_but_every_other_declared_table_must_be_requested() -> None:
    rows = generation(["first", "second"], unsourced=["third"])
    check(tables_of(rows) == {"first", "second"}, f"{tables_of(rows)}")
    with pytest.raises(bronze_metadata.MetadataError, match="first metadata publication"):
        generation(["first"], unsourced=["third"])
    with pytest.raises(bronze_metadata.MetadataError, match="first metadata publication"):
        generation(["first", "second"])


def test_the_table_appears_normally_in_the_run_where_its_first_source_object_arrives() -> None:
    before = generation(["first", "second"], earlier(), unsourced=["third"])
    after = generation(["first", "second", "third"], before, unsourced=[])
    bronze_metadata.validate(after)
    bronze_metadata.validate_successor(before, after)
    check(tables_of(after) == set(DECLARED), f"{tables_of(after)}")
    check(after[0]["generation"] != before[0]["generation"], "the generation did not advance")
    same = generation(["first", "second", "third"], after)
    check(same == after, "a retry after the table appeared built another generation")


CONFIG = {"namespace": "bronze", "tables": [{"table": name, "collection": f"c_{name}", "roles": ["data"]} for name in DECLARED]}


def stub_discover(monkeypatch: pytest.MonkeyPatch, found: list[dict[str, Any]]) -> list[list[str]]:
    asked: list[list[str]] = []

    def discover(storage: Any, bucket: str, config: dict[str, Any], names: list[str], *rest: Any, **keywords: Any) -> tuple[list[dict[str, Any]], list[Any]]:
        asked.append(list(names))
        return found, []

    monkeypatch.setattr(bronze, "discover", discover)
    return asked


def test_the_loader_accepts_a_left_out_table_with_no_published_observation_and_no_pinned_source(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = stub_discover(monkeypatch, [])
    bronze.check_unsourced(object(), BUCKET, CONFIG, ["first", "second"], ["third"], earlier())
    check(asked == [["third"]], f"the pinned sources were not read for the left-out table: {asked}")
    bronze.check_unsourced(object(), BUCKET, CONFIG, ["first", "second", "third"], [], earlier())
    check(asked == [["third"]], "nothing left out still read the sources")


@pytest.mark.parametrize(
    ("names", "unsourced", "found", "message"),
    [
        (["first", "second"], ["nowhere"], [], "not a table of the table map"),
        (["first", "second"], ["second"], [], "also requested"),
        (["first"], ["second"], [], "published observations"),
        (["first", "second"], ["third"], [item("third")], "source object"),
        (["first", "second"], ["third", "third"], [], "twice"),
    ],
)
def test_the_loader_refuses_a_left_out_table_that_is_unknown_requested_published_listed_or_repeated(
    monkeypatch: pytest.MonkeyPatch, names: list[str], unsourced: list[str], found: list[dict[str, Any]], message: str
) -> None:
    stub_discover(monkeypatch, found)
    with pytest.raises(bronze.BronzeError, match=message):
        bronze.check_unsourced(object(), BUCKET, CONFIG, names, unsourced, earlier())


class Recorded:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, skipped: list[str]) -> None:
        self.arguments: list[str] = []
        monkeypatch.setattr(bronze_run, "run_module", lambda job, module, arguments, environment=None: self.arguments.extend(map(str, arguments)))
        monkeypatch.setattr(bronze_run, "pinned_inventory", lambda job: {})
        monkeypatch.setattr(bronze_run, "staged_skipped", lambda job: [{"table": name, "reason": bronze_run.SKIPPED_REASON} for name in skipped])


def job(target: str = "real") -> bronze_run.Job:
    return bronze_run.Job(RUN, Path("/workspace"), BUCKET, ("first", "second"), target)


def test_the_load_phase_names_the_tables_the_stage_left_out_to_the_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded = Recorded(monkeypatch, ["third"])
    bronze_run.load(job())
    at = recorded.arguments.index("--unsourced-tables")
    check(recorded.arguments[at + 1] == "third" and recorded.arguments[recorded.arguments.index("--tables") + 1] == "first,second", f"{recorded.arguments}")


def test_the_load_phase_names_no_left_out_table_when_the_stage_left_none_out_and_for_a_qualification_run(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded = Recorded(monkeypatch, [])
    bronze_run.load(job())
    check("--unsourced-tables" not in recorded.arguments, f"{recorded.arguments}")
    other = Recorded(monkeypatch, ["third"])
    bronze_run.load(job("qualify"))
    check("--unsourced-tables" not in other.arguments, f"a qualification run has no stage report: {other.arguments}")


def test_the_check_phase_refuses_a_loader_report_whose_left_out_tables_differ_from_the_stage_report() -> None:
    report = {"mode": "load", "passed": True, "tables": ["first", "second"], "snapshots": {"first": 1, "second": 2}, "unsourced_tables": ["third"]}
    report["metadata_index"] = {"generation": "7"}
    check(bronze_run.accepted_unsourced(report, [{"table": "third", "reason": bronze_run.SKIPPED_REASON}]) is None, "the matching report was refused")
    with pytest.raises(bronze_run.StepError, match="left out"):
        bronze_run.accepted_unsourced(report, [])
    with pytest.raises(bronze_run.StepError, match="left out"):
        bronze_run.accepted_unsourced({k: v for k, v in report.items() if k != "unsourced_tables"}, [{"table": "third", "reason": bronze_run.SKIPPED_REASON}])
    check(bronze_run.accepted_unsourced({k: v for k, v in report.items() if k != "unsourced_tables"}, []) is None, "a run that left nothing out was refused")
