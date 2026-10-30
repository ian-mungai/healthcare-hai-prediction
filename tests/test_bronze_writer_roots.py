from __future__ import annotations

from scripts.lakehouse import bronze_writer
from scripts.lakehouse.bronze_writer_cases import planned
from tests.support import check

BOTH = ("example/collection/", "raw/example/collection/")


def test_a_qualification_run_reads_its_collection_at_the_root_and_under_raw() -> None:
    plan = planned(bronze_writer, "bronze")
    check(plan.source_prefixes == BOTH, "a qualification run declares both roots")


def test_a_production_run_reads_every_mapped_collection_at_both_roots() -> None:
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
