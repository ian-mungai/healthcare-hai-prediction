"""The SDK worker plane's leftover modules, their cases, checks and fixtures are gone, and nothing lists, imports or runs them.

The worker pool, its stderr reader, its frame protocol and the host process-group stop only served the SDK worker plane and the catalog tunnel, and both
are removed. Names are spelled in pieces so that this file does not name what it guards against.
"""

from __future__ import annotations

from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
GROUP = "process" + "_group"
STDERR = "worker" + "_stderr"
PROTOCOL = "worker" + "_protocol"
CASES = "sdk" + "_worker_cases"
STANDINS = "sdk" + "_standins"
SOURCE_CHECKS = "sdk" + "_source_checks"
MUTATIONS = "sdk" + "_static_mutations"
STOP_CASES = GROUP + "_stop_cases"
SCHEMA = "sdk" + "_worker"
GONE = (
    f"scripts/{GROUP}.py",
    f"orchestration/{STDERR}.py",
    f"orchestration/{PROTOCOL}.py",
    f"scripts/orchestration/{STOP_CASES}.py",
    f"scripts/orchestration/{SOURCE_CHECKS}.py",
    f"scripts/orchestration/{MUTATIONS}.py",
    f"scripts/orchestration/{CASES}.py",
    f"scripts/orchestration/{STANDINS}.py",
    f"data_contracts/orchestration/{SCHEMA}.schema.json",
    f"data_contracts/orchestration/examples/{SCHEMA}.json",
    f"data_contracts/orchestration/examples/{CASES}/case_matrix.json",
    f"data_contracts/orchestration/examples/{CASES}/mutation_cases.json",
)
# The registrations that listed those files: their path forms and the module forms that start them.
NAMES = (
    f"scripts/{GROUP}.py",
    f"scripts.{GROUP}",
    f"orchestration/{STDERR}.py",
    f"orchestration.{STDERR}",
    f"orchestration/{PROTOCOL}.py",
    f"orchestration.{PROTOCOL}",
    f"orchestration import {PROTOCOL}",
    CASES,
    STANDINS,
    SOURCE_CHECKS,
    MUTATIONS,
    STOP_CASES,
    f"{SCHEMA}.schema.json",
    f"examples/{SCHEMA}.json",
)
SEARCHED = ("config", "scripts", "orchestration", "services", "dags", "tests", "infra", ".importlinter", ".github")
SUFFIXES = (".py", ".sh", ".json", ".yaml", ".yml", ".tf", ".hcl", ".toml", ".importlinter")
SKIPPED_PARTS = (".terraform", "__pycache__")


def tracked_text() -> dict[str, str]:
    """Every source, configuration and workflow file under the searched paths, without this guard."""
    found: dict[str, str] = {}
    for entry in SEARCHED:
        base = ROOT / entry
        paths = [base] if base.is_file() else sorted(base.rglob("*")) if base.exists() else []
        for path in paths:
            skipped = any(part in SKIPPED_PARTS for part in path.parts) or path == Path(__file__).resolve()
            if path.is_file() and (path.suffix in SUFFIXES or path.name == ".importlinter") and not skipped:
                found[str(path.relative_to(ROOT))] = path.read_text(errors="ignore")
    return found


def test_the_worker_plane_modules_cases_and_fixtures_are_gone() -> None:
    present = [name for name in GONE if (ROOT / name).exists()]
    check(not present, f"these worker plane files are back: {present}")
    folder = ROOT / "data_contracts/orchestration/examples" / CASES
    check(not folder.exists(), f"the worker case fixture folder is back: {folder}")


def test_no_file_lists_imports_or_runs_a_removed_worker_plane_file() -> None:
    named = sorted(name for name, text in tracked_text().items() if any(piece in text for piece in NAMES))
    check(not named, f"these files still name a removed worker plane file: {named}")
