"""The committed lists that a change can make stale and that only hosted jobs checked before: the collector code versions and the launcher input list.

    .venv/bin/python -m scripts.quality.repo_checks collector-code-versions
    .venv/bin/python -m scripts.quality.repo_checks launcher-list

Each check calls the check that the hosted job runs; none reimplements it.

- ``collector-code-versions`` runs scripts/acquisition/tests/test_committed_code_versions.py (the hosted acquisition shards run it): the code closure of each
  collector entry module must be in its config/acquisition/*_code_versions.json. The hook's ``files`` pattern in .pre-commit-config.yaml names every file a
  closure holds, and a test of that file fails when a closure holds a file the pattern misses, so a closure that grows is caught by the commit that grows it.
- ``launcher-list`` runs ``launcher_list_equals_computed`` (scripts/quality/run_ci_groups_e2e.py: launcher_inputs.problems) and
  ``launcher_inventory_lists_every_file_of_the_folders_it_reads`` (scripts/quality/model_code_cases.py) over config/quality/launcher_inputs.json. The list
  holds more than a thousand paths and the folders the launcher suite reads, so no hook ``files`` pattern could name them. The check runs when the staged
  paths select the ``launcher`` group of config/quality/ci_groups.json, the selection of the hosted launcher job, so no change that selects that job skips it.

E2E: ``.venv/bin/python -m scripts.quality.run_checks_e2e --hook launcher-list``.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from scripts.process import TimeoutExpired, run_command
from scripts.quality import ci_groups

ROOT = Path(__file__).resolve().parents[2]
COLLECTOR_TEST = "scripts/acquisition/tests/test_committed_code_versions.py"
PYTEST_SECONDS = 300
GIT_SECONDS = 60
SHOWN = 12


def collector_problems() -> list[tuple[str, str]]:
    """(location, problem) of each way the collector closures are not in their committed lists: the output of the hosted test, as it fails."""
    # The test folder of pytest lives in a scratch folder that is removed afterwards, so a run leaves nothing in the repository whatever TMPDIR is.
    with tempfile.TemporaryDirectory(prefix="hai_collector_lists_") as scratch:
        command = ["-m", "pytest", "-q", "-p", "no:cacheprovider", "--no-header", f"--basetemp={scratch}/pytest", COLLECTOR_TEST]
        try:
            result = run_command(sys.executable, command, cwd=ROOT, timeout=PYTEST_SECONDS)
        except TimeoutExpired:
            return [(COLLECTOR_TEST, f"the test exceeded its deadline of {PYTEST_SECONDS} seconds and was stopped")]
    if result.returncode == 0:
        return []
    shown = [line for line in (result.stdout + result.stderr).splitlines() if line.strip()][-SHOWN * 2 :]
    return [(COLLECTOR_TEST, "the collector code closures are not all in their committed version lists:\n  " + "\n  ".join(shown))]


def launcher_problems() -> list[tuple[str, str]]:
    """(location, problem) of each way the launcher input list is not what the suite needs, when the staged paths select the launcher group."""
    if "launcher" not in ci_groups.plan("staged", "launcher")["run"]:
        return []
    from scripts.quality import launcher_inputs, model_code_cases

    tracked_text = run_command("git", ["ls-files"], cwd=ROOT, timeout=GIT_SECONDS, check=True).stdout
    tracked = {line for line in tracked_text.splitlines() if line}
    committed = json.loads((ROOT / launcher_inputs.LIST).read_text(encoding="utf-8"))
    found = [("launcher_list_equals_computed", line) for line in launcher_inputs.problems(ROOT, tracked, committed)]
    found += [
        ("launcher_inventory_lists_every_file_of_the_folders_it_reads", line)
        for line in model_code_cases.case_launcher_inventory_lists_every_file_of_the_folders_it_reads()
    ]
    return [(str(launcher_inputs.LIST), f"{case}: {line}") for case, line in found[:SHOWN]]
