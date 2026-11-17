"""The Prometheus history roles hold exactly their grants, and the cases that judge them catch each planted defect."""

from pathlib import Path

import pytest

from scripts.infrastructure.prometheus_history_policy_cases import CASES
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("name", sorted(CASES))
def test_every_policy_case_holds(name: str) -> None:
    results = CASES[name](ROOT)
    failed = [label for label, passed in results.items() if not passed]
    check(results and not failed, f"{name} failed: {failed}")
