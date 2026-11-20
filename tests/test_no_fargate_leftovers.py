"""The job roles of the IAM stack are the execution role and the try writer role, and no other role."""

from __future__ import annotations

from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]


def test_the_job_roles_are_only_the_execution_and_try_writer_roles() -> None:
    roles = (ROOT / "infra/iam/job_roles.tf").read_text()
    check('for name in ["execution", "try_writer"]' in roles, "the job roles are not just the execution and try writer roles")
