"""The monthly hold report: every file a listing shows that the locked queue does not hold is named for the owner, and nothing is added to a queue."""

from __future__ import annotations

import json
from typing import Any

import pytest

from scripts.acquisition import plan_rebuild
from scripts.acquisition.transport import CaptureError
from tests.support import check

HOSTS = frozenset({"data.example.gov"})
RUN = "20261201T000000Z-0a0a0a0a"
URL = "https://data.example.gov/files/measures_2026.csv"


def entry(**changes: Any) -> dict[str, Any]:
    return {"source_id": "SRC", "url": URL, "role": "data", "mode": "file"} | changes


def decide(entries: list[dict[str, Any]], known: frozenset[str] = frozenset()) -> plan_rebuild.Decision:
    return plan_rebuild.decide(plan_rebuild.read_entries(entries), plan_rebuild.Reference(HOSTS, known))


def test_a_new_file_that_passes_every_check_is_still_held_for_the_owner_and_nothing_is_added() -> None:
    decision = decide([entry()])
    check(len(decision.held) == 1 and decision.held[0].candidate.url == URL and decision.held[0].notes == (), f"held with no note: {decision}")
    check(not hasattr(decision, "locked"), "the decision has no list of files to add")


def test_a_file_the_queue_already_holds_is_counted_and_not_reported() -> None:
    decision = decide([entry(), entry(url=URL.replace("2026", "2025"))], known=frozenset({URL}))
    check(decision.known == 1 and [item.candidate.url for item in decision.held] == [URL.replace("2026", "2025")], f"{decision}")


@pytest.mark.parametrize(
    ("change", "note"),
    [
        ({"url": "https://other.example.org/measures_2026.csv"}, "host_not_approved"),
        ({"url": "http://data.example.gov/measures_2026.csv"}, "host_not_approved"),
        ({"role": "slides"}, "role_unknown"),
        ({"mode": "browser_form"}, "collector_not_task_lane"),
    ],
)
def test_a_note_names_what_would_keep_a_file_out_of_the_queue_without_changing_that_it_is_held(change: dict[str, Any], note: str) -> None:
    decision = decide([entry(**change)])
    check(len(decision.held) == 1 and note in decision.held[0].notes, f"the note {note} is named: {decision.held}")


def test_the_same_url_listed_twice_with_different_details_is_one_held_entry_with_a_note() -> None:
    decision = decide([entry(), entry(role="dictionary")])
    check(len(decision.held) == 1 and decision.held[0].notes == ("listed_twice_differently",), f"{decision.held}")


def test_the_decision_does_not_depend_on_the_order_of_the_listing() -> None:
    rows = [entry(url=URL.replace("2026", str(year))) for year in (2023, 2025, 2024)]
    check(decide(rows) == decide(list(reversed(rows))), "the order of the listing changed the decision")
    check([item.candidate.url[-8:-4] for item in decide(rows).held] == ["2023", "2024", "2025"], "held files are in URL order")


@pytest.mark.parametrize("row", [{}, {**entry(), "source_id": 1}, {**entry(), "extra": "x"}, {**entry(), "url": None}])
def test_a_listing_entry_that_is_open_or_malformed_stops_the_read(row: dict[str, Any]) -> None:
    with pytest.raises(CaptureError):
        plan_rebuild.read_entries([row])


def test_the_report_names_the_held_files_the_unreached_pages_and_the_links_outside_the_reviewed_paths() -> None:
    decision = decide([entry()])
    unreached = [{"source_id": "SRC", "url": "https://data.example.gov/files/", "reason": "index_unreadable"}]
    outside = [{"source_id": "SRC", "url": "https://data.example.gov/x/measures_2026.csv", "reason": "outside_reviewed_paths"}]
    found = plan_rebuild.report(decision, RUN, unreached, outside)
    check(found["kind"] == "plan_rebuild_report" and found["run_id"] == RUN and found["known"] == 0, f"{found}")
    check([item["url"] for item in found["held"]] == [URL] and found["unreached"] == unreached and found["outside"] == outside, f"{found}")
    check("added" not in json.dumps(found) and "locked" not in json.dumps(found), "the report adds nothing to a queue")
    text = plan_rebuild.report_text(decision, RUN, unreached, outside)
    check(
        URL in text and "https://data.example.gov/files/" in text and "Nothing was added to any queue." in text,
        f"the text names the file, the page and the rule: {text}",
    )


def test_the_report_keys_sit_under_the_monthly_run_folder_of_the_capture_root() -> None:
    check(
        plan_rebuild.report_keys("raw/", RUN) == (f"raw/ingestion_runs/{RUN}/plan_rebuild/report.json", f"raw/ingestion_runs/{RUN}/plan_rebuild/report.txt"),
        "keys",
    )
    with pytest.raises(CaptureError):
        plan_rebuild.report_keys("raw/", "latest")
