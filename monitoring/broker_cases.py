"""E2E cases for the broker template that ``drift_monitoring_dag`` submits (``drift_run``) and its launcher entry.

The broker loads its templates with the same validation a running broker applies, so a template the launcher cannot name or a class the broker does not
run is refused here. The Docker route has no drift job. The launcher must refuse it by name instead of failing inside a render.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

Problems = list[str]


def broker_template(folder: Path) -> Problems:
    broker = importlib.import_module("scripts.orchestration.broker")
    document = broker.load_templates()
    row = document["templates"].get("drift_run")
    if row is None:
        return ["the broker has no drift_run template"]
    problems = []
    if sorted(row["modes"]) != ["real"]:
        problems.append(f"drift_run has the modes {sorted(row['modes'])}, expected real only")
    mode = row["modes"].get("real", {})
    launcher_row = mode.get("launcher", {})
    if (mode.get("class"), mode.get("no_op"), launcher_row.get("template"), launcher_row.get("args"), launcher_row.get("params")) != (
        "lakehouse_read",
        False,
        "drift_job",
        [],
        {},
    ):
        problems.append(f"the real mode is {mode}")
    if launcher_row.get("timeout_seconds") != 3600:
        problems.append(f"the timeout is {launcher_row.get('timeout_seconds')}")
    return problems


def launcher_template(folder: Path) -> Problems:
    launcher = importlib.import_module("orchestration.launcher")
    template = launcher.TEMPLATES.get("drift_job")
    if template is None:
        return ["the launcher has no drift_job template"]
    shape = (template.finite, template.owners, template.run_modes, template.run_domains, template.max_seconds)
    return [] if shape == (True, ("run",), ("real",), ("transform",), 3600) else [f"drift_job is {shape}"]


def docker_route_refuses_by_name(folder: Path) -> Problems:
    launcher = importlib.import_module("orchestration.launcher")
    fake: Any = object.__new__(launcher.Renderer)
    try:
        fake.render_drift_job(SimpleNamespace(), launcher.TEMPLATES["drift_job"], "name", "registry")
    except launcher.LauncherError as error:
        return [] if error.code == "aws_only" else [f"the refusal code is {error.code}"]
    return ["the Docker route rendered a drift job"]


def builder_is_registered(folder: Path) -> Problems:
    source = (Path(__file__).resolve().parents[1] / "orchestration/launcher.py").read_text()
    return [] if '"drift_job": self.render_drift_job' in source else ["the launcher has no render entry for drift_job"]


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    return [
        ("broker_has_the_drift_run_template", broker_template),
        ("launcher_has_the_drift_job_template", launcher_template),
        ("launcher_registers_a_builder_for_drift_job", builder_is_registered),
        ("docker_route_refuses_the_drift_job_by_name", docker_route_refuses_by_name),
    ]
