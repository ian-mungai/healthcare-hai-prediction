"""No Fargate-only resource or address is left in the platform, IAM and services stacks.

Every job runs as a bridge task on the host, so these stacks hold no awsvpc job networking and no Fargate provider. The jobs stack and its module folder
belong to the host-only jobs change and have their own checks.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
STACKS = ("infra/platform", "infra/iam", "infra/services")
SECURITY_GROUP_USE = re.compile(r"aws_security_group\.(task|training)\b|aws_security_group\.job\[\"(offline|lakehouse)\"\]|aws_security_group\.job\[each")
# Negative checks may name Fargate to say that a definition or a grant is absent.
NEGATIVE_FILES = frozenset({"infra/iam/tests/airflow_launch.tftest.hcl", "infra/platform/tests/platform.tftest.hcl"})


def files(folders: tuple[str, ...], suffixes: tuple[str, ...]) -> list[Path]:
    return sorted(
        path for folder in folders for path in (ROOT / folder).rglob("*") if path.suffix in suffixes and ".terraform" not in path.parts and path.is_file()
    )


def test_the_stacks_register_no_fargate_provider_or_capacity() -> None:
    found = [
        str(path.relative_to(ROOT))
        for path in files(STACKS, (".tf", ".hcl"))
        if "fargate" in path.read_text().lower() and str(path.relative_to(ROOT)) not in NEGATIVE_FILES
    ]
    check(not found, f"these stack files still name Fargate: {found}")


def test_the_iam_stack_builds_no_security_group_for_an_awsvpc_job() -> None:
    text = "".join(path.read_text() for path in files(("infra/iam",), (".tf",)))
    check(not re.search(r'resource\s+"aws_security_group"\s+"(task|training)"', text), "the task or training group is still declared")
    check(
        'for_each = toset(["endpoints", "ssm_endpoint"])' in (ROOT / "infra/iam/job_network.tf").read_text(),
        "the job groups are not just the two endpoint groups",
    )


def test_no_stack_file_uses_an_awsvpc_job_group_or_the_task_group() -> None:
    users = [str(path.relative_to(ROOT)) for path in files(STACKS, (".tf", ".hcl")) if SECURITY_GROUP_USE.search(path.read_text())]
    check(not users, f"these files still use an awsvpc job group, the task group or the training group: {users}")


@pytest.mark.parametrize("name", ["task_security_group_id", "fargate_capacity_provider_names"])
def test_the_platform_stack_outputs_no_fargate_value(name: str) -> None:
    text = "".join(path.read_text() for path in files(("infra/platform",), (".tf",)))
    check(f'output "{name}"' not in text and not re.search(r"data\.aws_security_group\.task\b", text), f"the platform stack still holds {name}")
