from __future__ import annotations

import pytest

from scripts.infrastructure import render_ecs_target as renderer
from tests.support import check

CONTRACT = {
    "job_class": "publisher",
    "task_role_key": "publisher_task",
    "capacity_providers": ["FARGATE"],
    "network": "publisher",
    "assign_public_ip": "ENABLED",
    "capture_root": "raw/",
    "container": "publisher_fetch",
    "log_container": "publisher_fetch",
    "deadline_variable": "HAI_DEADLINE_UTC",
    "hosts_file": "/workspace/inputs/publisher_hosts.json",
    "hosts_approval_file": "/workspace/inputs/hosts_approval.json",
    "stored_mirror_root": "/workspace/inputs/mirror",
    "stored_index_file": "/workspace/inputs/stored_index.json",
}


def jobs(contracts: dict | None) -> dict:
    return {"publisher_dispatch_contracts": {"sensitive": False, "type": ["object", {}], "value": contracts}}


def test_the_exact_publisher_contract_is_accepted() -> None:
    check(renderer.publisher_dispatch(jobs({"publisher_fetch": CONTRACT}), {"publisher_fetch"}) == {"publisher_fetch": CONTRACT}, "exact contract accepted")


def test_no_selection_needs_no_contract() -> None:
    check(renderer.publisher_dispatch({}, set()) == {}, "unselected class is not read")


def test_a_null_publisher_output_means_no_selection() -> None:
    check(renderer.publisher_dispatch(jobs(None), set()) == {}, "a disabled publisher exports no contract")
    with pytest.raises(renderer.TargetError):
        renderer.publisher_dispatch(jobs(None), {"publisher_fetch"})


@pytest.mark.parametrize(
    "change",
    [
        {"assign_public_ip": "DISABLED"},
        {"network": "lakehouse"},
        {"capacity_providers": ["FARGATE_SPOT", "FARGATE"]},
        {"capture_root": ""},
        {"task_role_key": "read_task"},
        {"job_class": "lakehouse_write"},
        {"extra": "field"},
        {"container": "drift"},
        {"log_container": "other"},
        {"deadline_variable": "DEADLINE"},
        {"hosts_file": "/workspace/other/hosts.json"},
        {"hosts_approval_file": ""},
        {"stored_mirror_root": "/workspace/code"},
        {"stored_index_file": "/workspace/other.json"},
    ],
)
def test_any_other_publisher_contract_is_refused(change: dict) -> None:
    with pytest.raises(renderer.TargetError):
        renderer.publisher_dispatch(jobs({"publisher_fetch": CONTRACT | change}), {"publisher_fetch"})


def test_a_contract_for_a_definition_that_is_not_selected_is_refused() -> None:
    with pytest.raises(renderer.TargetError):
        renderer.publisher_dispatch(jobs({"publisher_fetch": CONTRACT}), set())


def test_the_application_target_does_not_carry_the_publisher_class_yet() -> None:
    check("publisher_fetch" not in renderer.DEFINITIONS and "publisher_fetch" not in renderer.WRITER_DEFINITIONS, "publisher stays out of the closed target")
    check(renderer.PUBLISHER_DEFINITIONS == {"publisher_fetch": ("publisher", "analytics", "publisher_task")}, "publisher definition spec")
