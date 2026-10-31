from __future__ import annotations

import pytest

from scripts.lakehouse import bronze_writer
from scripts.lakehouse.bronze_writer_cases import planned
from tests.support import check

BOTH = ("example/collection/", "raw/example/collection/")


ROOT_ONLY = ("example/collection/",)


def test_a_qualification_run_reads_only_the_root_while_the_raw_capability_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HAI_READ_RAW_ROOT", raising=False)
    check(planned(bronze_writer, "bronze").source_prefixes == ROOT_ONLY, "the plan declares the root only")


def test_a_qualification_run_reads_its_collection_at_the_root_and_under_raw(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    plan = planned(bronze_writer, "bronze")
    check(plan.source_prefixes == BOTH, "a qualification run declares both roots")


def test_a_production_run_reads_every_mapped_collection_at_both_roots(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    plan = planned(
        bronze_writer,
        "bronze",
        mode="real",
        layer_state="on",
        qualification_passed=True,
        activation_approved=True,
        run_approval="synthetic_run",
        recovery_hold=False,
    )
    check(plan.source_prefixes == BOTH, "a production run declares both roots")
