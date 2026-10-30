"""Run the monitoring file cases and keep the report under data/e2e/monitoring/.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.run_monitoring_e2e``. No AWS service is contacted and no container starts.
A case that raises is recorded as failed with the type of its exception, and the report is still saved.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

from scripts.infrastructure.monitoring_cases import CASES, REPO_ROOT

OUTPUT = REPO_ROOT / "data" / "e2e" / "monitoring"


def run_cases() -> dict[str, dict[str, bool]]:
    """The outcome of every case; a case that raises is one failed check named after the type of its exception, never its message."""
    results: dict[str, dict[str, bool]] = {}
    for name, function in CASES.items():
        try:
            results[name] = function(REPO_ROOT)
        except Exception as error:
            results[name] = {f"the case ended with {type(error).__name__}": False}
    return results


def main() -> int:
    """Run every case, write the outcomes and return 1 when any check failed or the report cannot be written."""
    results = run_cases()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    passed = all(all(checks.values()) for checks in results.values())
    try:
        OUTPUT.mkdir(parents=True, exist_ok=True)
        (OUTPUT / f"report_{stamp}.json").write_text(
            json.dumps({"run_at_utc": stamp, "results": results, "passed": passed}, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    except OSError as error:
        sys.stderr.write(f"The report could not be saved under {OUTPUT}: {type(error).__name__}\n")
        passed = False
    for name, checks in results.items():
        failed = [check for check, ok in checks.items() if not ok]
        sys.stdout.write(f"{'PASS' if not failed else 'FAIL'} {name}\n")
        for check in failed:
            sys.stdout.write(f"  failed: {check}\n")
    sys.stdout.write(f"{sum(all(c.values()) for c in results.values())} of {len(results)} cases passed.\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
