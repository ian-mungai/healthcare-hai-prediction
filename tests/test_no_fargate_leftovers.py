"""No Fargate-only resource or address is left in the platform, IAM and services stacks.

Every job runs as a bridge task on the host, so these stacks hold no awsvpc job networking and no Fargate provider, and the launcher holds no ECS
placement. The jobs stack, its module folder and the target renderer contract belong to the host-only jobs change and have their own checks.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
STACKS = ("infra/platform", "infra/iam", "infra/services")
# The contracts that must name no Fargate provider. The ledger, request and execution contracts keep the ecs target and the Fargate providers as history
# values (old lines read, group and recover); only the admission of a new submission refuses them (tests in run_contracts_e2e).
CONTRACTS = (
    "sdk_worker.schema.json",
    "examples/sdk_worker.json",
)
GONE = (
    "config/orchestration/ecs_jobs.json",
    "config/orchestration/ecs_jobs.schema.json",
)
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


def test_the_launcher_holds_no_ecs_placement_and_no_fargate_name() -> None:
    named = [str(path.relative_to(ROOT)) for path in files(("orchestration",), (".py",)) if "fargate" in path.read_text().lower()]
    named += [name for name in CONTRACTS if "fargate" in (ROOT / "data_contracts/orchestration" / name).read_text().lower()]
    check(not named, f"these files still name Fargate: {named}")
    present = [name for name in GONE if (ROOT / name).exists()]
    present += [
        str(path.relative_to(ROOT))
        for folder, pattern in (("orchestration", "ecs_tar*"), ("scripts/orchestration", "*ecs_tar*"))
        for path in (ROOT / folder).glob(pattern)
    ]
    check(not present, f"the legacy launcher ECS target is still present: {present}")
    launcher = (ROOT / "orchestration/launcher.py").read_text()
    check("ecs_target" not in launcher and "ecs_runtime" not in launcher and "dispatch_ecs" not in launcher, "the launcher still wires an ECS placement")


def test_no_iam_role_or_binding_remains_for_the_retired_read_jobs() -> None:
    names = ("offline_task", "seal_task", "read_task", "job_read_task", "job_offline_task", "job_seal_task")
    holders = [
        *files(("infra/iam",), (".tf", ".hcl", ".json")),
        ROOT / "config/aws/iam_preflight_checks.json",
        ROOT / "config/lakehouse/aws_bronze_templates.json",
    ]
    found = [str(path.relative_to(ROOT)) for path in holders if any(name in path.read_text() for name in names)]
    check(not found, f"these files still name a retired read job role: {found}")
    roles = (ROOT / "infra/iam/job_roles.tf").read_text()
    check('for name in ["execution", "try_writer"]' in roles, "the job roles are not just the execution and try writer roles")


def test_the_fargate_identity_probe_and_its_tooling_are_gone() -> None:
    check(not (ROOT / "infra/jobs/identity_probe").exists(), "the identity probe root is still present")
    wrapper = (ROOT / "scripts/infrastructure/terraform.sh").read_text() + (ROOT / "scripts/infrastructure/render_jobs_config.py").read_text()
    check("identity" + "-probe" not in wrapper, "the wrapper or the jobs renderer still takes the probe option")
    schema = (ROOT / "data_contracts/infrastructure/jobs_preparation.schema.json").read_text()
    check("identity" + "_probe" not in schema, "the jobs preparation contract still names the probe root")


def test_the_legacy_cloud_staging_and_remote_run_subsystem_is_gone() -> None:
    patterns = (
        ("orchestration", "remote_r*"),
        ("orchestration", "ecs_sta*"),
        ("scripts/orchestration", "ecs_sta*"),
        ("scripts/orchestration", "run_ecs_prep*"),
        ("scripts/orchestration", "ecs_prep*"),
        ("scripts/orchestration", "ecs_fake_s*"),
        ("data_contracts/orchestration", "remote_r*"),
        ("data_contracts/orchestration", "ecs_job*"),
    )
    present = [str(path.relative_to(ROOT)) for folder, pattern in patterns for path in (ROOT / folder).glob(pattern)]
    check(not present, f"the legacy cloud subsystem is still present: {present}")
