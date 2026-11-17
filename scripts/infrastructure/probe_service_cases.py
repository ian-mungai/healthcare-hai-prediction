"""Cases for the service label of the health probes (review of 84b3722..7fc9030, finding 7).

``ServiceHealthDown`` joins ``hai_service_probe_up`` to ``hai_service_tasks_desired`` on the ``service`` label, and the desired-task series carries the ECS
service name. A probe whose ``service`` is not an ECS service of ``infra/services/main.tf`` matches nothing, so its alert can never fire. The case reads the
service names from the Terraform file and judges every probe of the host state exporter against them, and judges the old name ``airflow`` the same way.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

from monitoring import host_state_exporter

SERVICES_FILE = "infra/services/main.tf"
SERVICE_KEY = re.compile(r"\n\s+(\w+) = \{\n\s+task_role_arn")


def ecs_services(root: Path) -> set[str]:
    return set(SERVICE_KEY.findall((root / SERVICES_FILE).read_text(encoding="utf-8")))


def every_probe_names_an_ecs_service(root: Path) -> dict[str, bool]:
    services = ecs_services(root)
    unmatched = {name: probe.service for name, probe in host_state_exporter.PROBES.items() if probe.service not in services}
    return {
        "the service names were read from the Terraform file": {"polaris", "airflow_apiserver", "airflow_scheduler", "mlflow", "openmetadata"} <= services,
        "every probe belongs to an ECS service that exists": unmatched == {},
        "the two Airflow probes name the api server and the scheduler": {
            host_state_exporter.PROBES["airflow_api"].service,
            host_state_exporter.PROBES["airflow_scheduler"].service,
        }
        == {"airflow_apiserver", "airflow_scheduler"},
    }


def the_old_probe_name_is_found_by_the_check(root: Path) -> dict[str, bool]:
    return {"a probe of the service 'airflow' matches no ECS service": "airflow" not in ecs_services(root)}


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "probes_every_probe_names_an_ecs_service": every_probe_names_an_ecs_service,
    "probes_the_old_probe_name_is_found_by_the_check": the_old_probe_name_is_found_by_the_check,
}
