"""Put the AWS stand-in under the real entry point ``python -m scripts.infrastructure.host_session``, for ``host_session_entry_cases``.

The case starts the real command in a child process with a ``sitecustomize.py`` on its path that calls ``install``. ``install`` runs before the command's
module loads. It replaces what the command reads from outside (the project configuration, the Terraform outputs, the ``aws`` program and the clock) and
nothing of ``host_session`` or ``aws_tasks``, so the command runs as ``__main__`` and ``aws_tasks`` loads its own copy of the module, exactly as in
production. The scenario comes from the environment: ``HOST_SESSION_ENTRY_SCENARIO`` (JSON: ``faults``, ``tasks`` as ``{"family": ..., "ends_at": seconds
or null}``, ``host_on`` and ``environment``, ``dev`` unless given), ``HOST_SESSION_ENTRY_RESULT`` (the file that receives the end state) and, for the plants,
``HOST_SESSION_ENTRY_SOURCE`` (a file that loads in place of ``scripts/infrastructure/host_session.py``). Time is virtual: a sleep advances the stand-in.
"""

from __future__ import annotations

import atexit
import importlib.abc
import importlib.machinery
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from unittest import mock

SCENARIO = "HOST_SESSION_ENTRY_SCENARIO"
RESULT = "HOST_SESSION_ENTRY_RESULT"
SOURCE = "HOST_SESSION_ENTRY_SOURCE"
MODULE = "scripts.infrastructure.host_session"
WRITES = {"set-desired-capacity", "run-task", "update-service", "stop-task"}


class SourceFinder(importlib.abc.MetaPathFinder):
    """Load ``MODULE`` from another file, for every import of it, including the one that ``python -m`` makes."""

    def __init__(self, source: str) -> None:
        self.source = source

    def find_spec(self, name: str, path: object = None, target: object = None) -> importlib.machinery.ModuleSpec | None:
        return importlib.util.spec_from_file_location(name, self.source) if name == MODULE else None


def outputs(stand_in: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """The platform and services Terraform outputs that name the stand-in's cluster, group, provider, services and one-shot tasks."""
    platform = {
        "cluster_name": {"value": stand_in.CLUSTER},
        "host_autoscaling_group_name": {"value": stand_in.GROUP},
        "ec2_capacity_provider_name": {"value": stand_in.PROVIDER},
        "project_tag": {"value": stand_in.PROJECT},
    }
    services = {
        "service_names": {"value": dict(stand_in.SERVICES)},
        "one_shot_run_task": {"value": {key: {"task_definition": family} for key, family in stand_in.FAMILIES.items()}},
    }
    return platform, services


def install() -> None:
    """Set the stand-in up for this process; called from ``sitecustomize``."""
    from scripts.acquisition import cli_tools
    from scripts.infrastructure import host_session_stand_in as stand_in
    from scripts.infrastructure import render_platform_config, render_project_config

    scenario = json.loads(os.environ[SCENARIO])
    host = stand_in.StandInHost(set(scenario.get("faults", [])))
    if scenario.get("host_on"):
        host.desired, host.instance_up_at = 1, 0.0
    for task in scenario.get("tasks", []):
        arn = host.add_task(task["family"])
        if task.get("ends_at") is not None:
            host.tasks[arn]["stops_at"] = float(task["ends_at"])
    platform, services = outputs(stand_in)
    settings = {"aws_profile": stand_in.PROFILE, "aws_region": stand_in.REGION, "project_name": "stand_in", "environment": scenario.get("environment", "dev")}

    def terraform_outputs(directory: Path, *_arguments: object, **_keywords: object) -> dict[str, Any]:
        return services if Path(directory).name == "services" else platform

    # Under the real ``tolerant_runner``: only what the command reads from outside is replaced.
    mock.patch.object(render_project_config, "load_configuration", lambda *_: (settings, None)).start()
    mock.patch.object(render_platform_config, "terraform_document", terraform_outputs).start()
    mock.patch.object(cli_tools, "run_argv", lambda argv, **kwargs: host.runner(argv, **kwargs)).start()
    mock.patch.object(time, "monotonic", lambda: host.now).start()
    mock.patch.object(time, "sleep", host.advance).start()
    mock.patch.object(time, "time", lambda: stand_in.EPOCH.timestamp() + host.now).start()

    def record() -> None:
        state = {
            "desired": host.desired,
            "instance_up": host.instance_up_at is not None,
            "writes": [e["operation"] for e in host.log if e["operation"] in WRITES],
        }
        Path(os.environ[RESULT]).write_text(json.dumps(state), encoding="utf-8")

    atexit.register(record)
    if SOURCE in os.environ:
        sys.meta_path.insert(0, SourceFinder(os.environ[SOURCE]))
