"""Run the host session cases against the in-process ECS and Auto Scaling stand-in and keep the report under data/e2e/host_session/.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.run_host_session_e2e``. No AWS service is contacted and no container starts.
A case that raises is recorded as failed with the type of its exception, and the report is still saved.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

from scripts.infrastructure.host_session_cases import CASES, REPO_ROOT

OUTPUT = REPO_ROOT / "data" / "e2e" / "host_session"


def run_cases() -> dict[str, dict[str, bool]]:
    """The outcome of every case; a case that raises is one failed check named after the type of its exception, never its message."""
    results: dict[str, dict[str, bool]] = {}
    for name, function in CASES.items():
        try:
            results[name] = function()
        except Exception as error:
            results[name] = {f"the case ended with {type(error).__name__}": False}
    return results


def main() -> int:
    """Run every case, write the outcomes (names only) and return 1 when any check failed or the report cannot be written."""
    results = run_cases()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    passed = all(all(checks.values()) for checks in results.values())
    try:
        OUTPUT.mkdir(parents=True, exist_ok=True)
        (OUTPUT / f"report_{stamp}.json").write_text(json.dumps({"run_at_utc": stamp, "results": results, "passed": passed}, indent=2, sort_keys=True) + "\n")
    except OSError as error:
        sys.stderr.write(f"The report could not be saved under {OUTPUT}: {type(error).__name__}\n")
        passed = False
    for name, checks in results.items():
        for check, ok in checks.items():
            sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
    sys.stdout.write(f"{sum(sum(c.values()) for c in results.values())} of {sum(len(c) for c in results.values())} checks passed\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
