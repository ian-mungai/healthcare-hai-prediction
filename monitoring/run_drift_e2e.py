"""E2E for the drift check (project decision 21): the tracked configuration and its check.

Run from the repository root: ``.venv/bin/python -m monitoring.run_drift_e2e``. Every case starts the real command as a child process on a temporary copy of
the tracked configuration and judges the exit code, the message and the files it leaves. Planted defects must fail for their own cause, so each rejection case
names the key the check must report. Results go to ``data/e2e/drift/<UTC>/report.json``.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from monitoring import drift_cases
from scripts.process import SubprocessError, run_command

ROOT = Path(__file__).resolve().parents[1]
PYTHON = str(ROOT / ".venv/bin/python")
CONFIG = ROOT / "config/monitoring/drift.json"
TIMEOUT = 120
Problems = list[str]
Mutation = Callable[[dict[str, Any]], object]

CONFIG_REJECTIONS: tuple[tuple[str, Mutation, str], ...] = (
    ("lower_drift_share", lambda d: d.update(drift_share=0.4), "drift_share"),
    ("higher_drift_share", lambda d: d.update(drift_share=0.6), "drift_share"),
    ("looser_test_threshold", lambda d: d.update(test_threshold=0.1), "test_threshold"),
    ("other_numeric_test", lambda d: d.update(numeric_test="wasserstein"), "numeric_test"),
    ("other_categorical_test", lambda d: d.update(categorical_test="jensenshannon"), "categorical_test"),
    ("exclusive_share_boundary", lambda d: d.update(share_boundary_inclusive=False), "share_boundary_inclusive"),
    ("sensitivity_may_trigger", lambda d: d.update(trigger_populations=["primary", "sensitivity"]), "trigger_populations"),
    ("single_row_minimum", lambda d: d.update(minimum_current_rows=1), "minimum_current_rows"),
    ("endless_deadline", lambda d: d.update(deadline_seconds=0), "deadline_seconds"),
    ("missing_share", lambda d: d.pop("drift_share"), "drift_share"),
    ("unknown_key", lambda d: d.update(drift_shares=0.5), "drift_shares"),
)


def drift_config(*args: str) -> tuple[int, str]:
    """Run the configuration check as a module and return its exit code and combined text output."""
    done = run_command(PYTHON, ["-m", "monitoring.drift_config", *args], cwd=ROOT, timeout=TIMEOUT)
    return done.returncode, done.stdout + done.stderr


def config_copy(folder: Path, mutate: Mutation | None = None) -> Path:
    data = json.loads(CONFIG.read_text())
    if mutate is not None:
        mutate(data)
    path = folder / "drift.json"
    path.write_text(json.dumps(data, indent=2) + "\n")
    return path


def tracked_config_passes(folder: Path) -> Problems:
    code, text = drift_config("--check")
    return [] if code == 0 and "ok" in text else [f"the tracked configuration was refused (exit {code}): {text[:300]}"]


def copy_passes(folder: Path) -> Problems:
    code, text = drift_config("--check", "--config", str(config_copy(folder)))
    return [] if code == 0 else [f"an unchanged copy was refused (exit {code}): {text[:300]}"]


def rejection(mutate: Mutation, key: str) -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        code, text = drift_config("--check", "--config", str(config_copy(folder, mutate)))
        if code == 0:
            return [f"the check accepted a configuration with a defect in {key}"]
        return [] if key in text else [f"the check refused it, but the message does not name {key}: {text[:300]}"]

    return case


def unreadable_configuration(folder: Path) -> Problems:
    path = folder / "drift.json"
    path.write_text("{not json")
    code, text = drift_config("--check", "--config", str(path))
    return [] if code != 0 and "drift.json" in text else [f"a malformed file was not refused by name (exit {code}): {text[:300]}"]


def missing_configuration(folder: Path) -> Problems:
    code, text = drift_config("--check", "--config", str(folder / "absent.json"))
    return [] if code != 0 and "absent.json" in text else [f"a missing file was not refused by name (exit {code}): {text[:300]}"]


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    config: list[tuple[str, Callable[[Path], Problems]]] = [("tracked_config_passes", tracked_config_passes), ("copy_passes", copy_passes)]
    config += [(f"config_rejects_{name}", rejection(mutate, key)) for name, mutate, key in CONFIG_REJECTIONS]
    config += [("config_rejects_malformed_file", unreadable_configuration), ("config_rejects_missing_file", missing_configuration)]
    return config + drift_cases.cases()


def main() -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = ROOT / "data" / "e2e" / "drift" / stamp
    out.mkdir(parents=True)
    top = Path(tempfile.mkdtemp(prefix="drift_e2e_"))
    results: list[dict[str, Any]] = []
    try:
        for name, check in cases():
            folder = top / name
            folder.mkdir()
            try:
                problems = check(folder)
            except drift_cases.Blocked as reason:
                results.append({"case": name, "passed": False, "blocked": True, "problems": [f"blocked: {reason}"]})
                continue
            except (SubprocessError, OSError, ValueError, KeyError) as error:
                problems = [f"{type(error).__name__}: {error}"]
            results.append({"case": name, "passed": not problems, "blocked": False, "problems": problems})
    finally:
        shutil.rmtree(top, ignore_errors=True)
    passed = sum(result["passed"] for result in results)
    blocked = sum(result["blocked"] for result in results)
    failed = len(results) - passed - blocked
    report = out / "report.json"
    report.write_text(json.dumps({"pass": failed == 0 and blocked == 0, "failed": failed, "blocked": blocked, "cases": results}, indent=2) + "\n")
    for result in results:
        sys.stdout.write(f"{'BLOCKED' if result['blocked'] else 'PASS' if result['passed'] else 'FAIL'} {result['case']}\n")
        for problem in result["problems"]:
            sys.stdout.write(f"  {problem}\n")
    sys.stdout.write(f"{passed}/{len(results)} passed, {blocked} blocked, {failed} failed; report {report}\n")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    os.umask(0o022)
    raise SystemExit(main())
