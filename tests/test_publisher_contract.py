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
}


def jobs(contracts: dict) -> dict:
    return {"publisher_dispatch_contracts": {"sensitive": False, "type": ["object", {}], "value": contracts}}


def test_the_exact_publisher_contract_is_accepted() -> None:
    check(renderer.publisher_dispatch(jobs({"publisher_fetch": CONTRACT}), {"publisher_fetch"}) == {"publisher_fetch": CONTRACT}, "exact contract accepted")


def test_no_selection_needs_no_contract() -> None:
    check(renderer.publisher_dispatch({}, set()) == {}, "unselected class is not read")


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
