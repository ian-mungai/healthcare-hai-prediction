"""Synthetic registry-extension checks before enabling historical downloads."""

import copy
from typing import Any

import pytest

from scripts.acquisition import build_source_registry as builder
from tests.support import check
from tests.test_source_registry import approved_inputs as approved_inputs


# Failures: discarded controls, changed old routes, cleared holds, duplicate jobs,
# unbounded acquisition and a registry lock that does not match its routes.
@pytest.mark.parametrize("role,expected", [("data", "data"), ("reference", "methodology")])
def test_history_registration_preserves_locked_inputs(approved_inputs: Any, role: str, expected: str) -> None:
    from scripts.acquisition.history_routes import extend_registry

    original, lock = builder.build_registry(*approved_inputs)
    before = copy.deepcopy(original)
    source = next(s for s in original["sources"] if s["preferred_route"] == "file_download")
    candidates = [
        {
            "source_id": source["source_id"],
            "url": "https://example.org/earlier.csv",
            "format": "csv",
            "role": role,
            "label": "Example earlier release",
            "evidence": {"publisher_index": "https://example.org/data"},
        }
    ]
    registry, new_lock, jobs = extend_registry(original, lock, candidates, {"privacy_review_sources": [], "large_file_review_sources": []})
    check(
        original == before and registry["measure_controls"] == original["measure_controls"],
        'original == before and registry["measure_controls"] == original["measure_controls"]',
    )
    check(
        registry["counts"] == original["counts"] and new_lock != lock and len(jobs) == 1,
        'registry["counts"] == original["counts"] and new_lock != lock and len(jobs) == 1',
    )
    check(jobs[0]["plan"]["release"]["release_date"] is None, 'jobs[0]["plan"]["release"]["release_date"] is None')
    check(jobs[0]["plan"]["role"] == expected, 'jobs[0]["plan"]["role"] == expected')
    check(
        registry["sources"][0]["file_routes"][: len(original["sources"][0]["file_routes"])] == original["sources"][0]["file_routes"],
        'registry["sources"][0]["file_routes"][: len(original["sources"][0]["file_routes"])] == original["sources"][0]["file_routes"]',
    )


@pytest.mark.parametrize("hold", ["access", "privacy", "large_file"])
def test_history_registration_does_not_clear_holds(approved_inputs: Any, hold: str) -> None:
    from scripts.acquisition.history_routes import extend_registry

    original, lock = builder.build_registry(*approved_inputs)
    route = "access_hold" if hold == "access" else "file_download"
    source = next(s for s in original["sources"] if s["preferred_route"] == route)
    rules = {
        "privacy_review_sources": [source["source_id"]] if hold == "privacy" else [],
        "large_file_review_sources": [source["source_id"]] if hold == "large_file" else [],
    }
    candidate = {
        "source_id": source["source_id"],
        "url": "https://example.org/earlier.csv",
        "format": "csv",
        "role": "data",
        "label": "Example",
        "evidence": {},
    }
    with pytest.raises(ValueError):
        extend_registry(original, lock, [candidate], rules)


# Failures: an advertised size silently exceeds the file cap or retries escape the batch budget.
def test_history_download_reservation_bounds_retries() -> None:
    from scripts.acquisition.history_routes import bounded_jobs

    jobs = [{"candidate": {"max_bytes": 12}}, {"candidate": {"max_bytes": 20}}]
    selected, deferred, reserved = bounded_jobs(jobs, 30)
    check(selected == jobs[:1] and deferred == jobs[1:] and reserved == 24, "selected == jobs[:1] and deferred == jobs[1:] and reserved == 24")
    with pytest.raises(ValueError):
        bounded_jobs([{"candidate": {"max_bytes": 513 * 1024**2}}], 2**40)
