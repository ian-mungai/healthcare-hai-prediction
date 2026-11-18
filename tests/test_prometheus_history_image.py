"""The image of the Prometheus history shipper and restore one-shot provides every third-party package that monitoring/prometheus_history.py imports.

Both tasks run `python -m monitoring.prometheus_history` from the code checkout. The module imports boto3 and botocore inside make_client, so a missing
package shows only when the task starts (live, Nov 18 2026: ModuleNotFoundError for boto3 in the Airflow image under user 65534 with HOME=/tmp). The
Airflow image installs nothing from this repository's lock, so no offline check can prove what its Python sees. An image that the repository builds from
a dependency group of uv.lock can be proven: the group must pin every package the module imports.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from importlib.metadata import packages_distributions
from pathlib import Path

from scripts.quality.lock_env import IMAGE_FILES, normalize
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "monitoring" / "prometheus_history.py"
TERRAFORM = ROOT / "infra" / "services" / "prometheus_history.tf"
TASKS = ("prometheus_history", "prometheus_restore")
FIRST_PARTY = {"monitoring", "scripts", "tests"}
IMAGE_LINE = re.compile(r'name\s*=\s*"(prometheus_history|prometheus_restore)"\s*\n\s*image\s*=\s*lookup\(local\.image,\s*"([a-z_]+)"')
REQUIREMENT_NAME = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)")


def third_party_imports(path: Path) -> set[str]:
    """The top-level names of every import in the module, function-level ones included, that are neither standard library nor first party."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names - set(sys.stdlib_module_names) - FIRST_PARTY - {"__future__"}


def task_images() -> dict[str, str]:
    """The image key (`lookup(local.image, "<key>", "")`) of each history task, read from the Terraform source."""
    return {name: key for name, key in IMAGE_LINE.findall(TERRAFORM.read_text(encoding="utf-8"))}


def group_packages(group: str) -> set[str]:
    """The normalized package names of one dependency group of pyproject.toml."""
    groups = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["dependency-groups"]
    return {normalize(match.group(1)) for line in groups[group] if isinstance(line, str) and (match := REQUIREMENT_NAME.match(line))}


def test_both_history_tasks_name_an_image() -> None:
    check(set(task_images()) == set(TASKS), f"{TERRAFORM.name} must give each of {TASKS} one image lookup; found {task_images()}")


def test_the_module_imports_what_the_test_expects() -> None:
    check(third_party_imports(MODULE) == {"boto3", "botocore"}, f"the third-party imports changed: {sorted(third_party_imports(MODULE))}; review the image")


def test_each_history_task_image_is_built_from_a_group_that_pins_every_import() -> None:
    needed = third_party_imports(MODULE)
    owners = packages_distributions()
    packages = {normalize(distribution) for name in needed for distribution in owners.get(name, [name])}
    problems = []
    for task, key in sorted(task_images().items()):
        if key not in IMAGE_FILES:
            problems.append(f"{task}: image {key!r} is not built from a dependency group of uv.lock, so nothing proves it holds {sorted(needed)}")
            continue
        missing = sorted(packages - group_packages(key))
        if missing:
            problems.append(f"{task}: the {key} group does not pin {missing}")
    check(not problems, "; ".join(problems))
