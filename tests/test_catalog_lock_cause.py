"""Keep the principal-lock mutation bound to the actual lock refusal."""

import contextlib
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.lakehouse import run_aws_catalog_setup_e2e as suite
from tests.support import check


def test_held_principal_lock_is_the_positive_control(tmp_path: Path) -> None:
    checks = suite.case_lock_before_changes(tmp_path)
    check(all(checks.values()), "the real held lock must pass its complete case")


@pytest.mark.parametrize("category", ["deadline", "credentials"])
def test_bypassed_lock_cannot_use_a_later_failure_as_its_proof(tmp_path: Path, category: str) -> None:
    with (
        patch.object(suite.principal_credentials, "serialized", lambda *args: contextlib.nullcontext()),
        patch.object(suite.setup, "read_root", side_effect=suite.setup.SetupError(category, "synthetic later failure")),
    ):
        checks = suite.case_lock_before_changes(tmp_path)
    unchanged = {name: value for name, value in checks.items() if name != "the refusal identifies the principal lock"}
    check(all(unchanged.values()), "the later failure preserves every old no-effect and deadline observation")
    check(not all(checks.values()), "an unrelated later failure must not establish principal-lock enforcement")
