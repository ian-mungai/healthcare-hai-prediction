"""Run the direct ECS launch core offline: the failing-first matrix against the fake ECS, log and bucket clients, then the operator cases on the stand-in and
the Run 3 publisher job cases.

No AWS call, no Docker and no credential is used. Each run writes ``data/e2e/direct_ecs_launch_core/<timestamp>.json`` (ignored by Git) with the revision,
the changed files, the substitutions and one line per case. Run from the repository root:
``.venv/bin/python -m scripts.orchestration.run_ecs_launch_e2e [--only core|operator|publisher] [--case NAME]``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from scripts.orchestration import ecs_launch_cases, ecs_launch_operator_cases, ecs_launch_publisher_cases
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "orchestration/ecs_adapter.py",
    "orchestration/ecs_launch.py",
    "orchestration/ecs_launch_operator.py",
    "orchestration/ecs_launch_provider.py",
    "scripts/orchestration/ecs_launch_cases.py",
    "scripts/orchestration/ecs_launch_operator_cases.py",
    "orchestration/publisher_launch.py",
    "scripts/orchestration/ecs_launch_publisher_cases.py",
    "scripts/orchestration/ecs_launch_fake.py",
    "scripts/orchestration/ecs_launch_standin.py",
    "scripts/orchestration/ecs_broker_fake.py",
    "config/orchestration/ecs_launch.json",
    "scripts/orchestration/run_ecs_launch_e2e.py",
)
SUITES: dict[str, dict[str, Callable[[], None]]] = {
    "core": ecs_launch_cases.CASES,
    "operator": ecs_launch_operator_cases.CASES,
    "publisher": ecs_launch_publisher_cases.CASES,
}
SUBSTITUTIONS = [
    "fake ECS, Logs and write-once S3 clients",
    "virtual clock (waits of minutes take no real time)",
    "operator cases: a stand-in of the Amazon provider 9.36.0 operator, not the real package (rerun them against the real provider in the Airflow image)",
]


def git(*arguments: str) -> str:
    return run_command("git", list(arguments), cwd=ROOT, timeout=30).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", choices=tuple(SUITES))
    parser.add_argument("--case")
    args = parser.parse_args()
    results = []
    for suite, cases in SUITES.items():
        if args.only and args.only != suite:
            continue
        for name, case in cases.items():
            if args.case and args.case != name:
                continue
            try:
                case()
                results.append({"suite": suite, "case": name, "outcome": "pass"})
            except Exception as error:
                detail = "".join(traceback.format_exception_only(type(error), error)).strip()
                results.append({"suite": suite, "case": name, "outcome": "fail", "detail": detail})
    failed = [item for item in results if item["outcome"] == "fail"]
    started = datetime.now(UTC)
    artifact = {
        "feature": "direct ECS launch core",
        "at": started.isoformat(),
        "revision": git("rev-parse", "HEAD") or "unknown",
        "uncommitted": git("status", "--short").splitlines(),
        "mode": "full" if not (args.only or args.case) else "iteration",
        "command": " ".join(["python", "-m", "scripts.orchestration.run_ecs_launch_e2e", *sys.argv[1:]]),
        "substitutions": SUBSTITUTIONS,
        "sources": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES if (ROOT / name).exists()},
        "results": results,
    }
    folder = ROOT / "data" / "e2e" / "direct_ecs_launch_core"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{started.strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    for item in failed:
        sys.stdout.write(f"FAIL {item['suite']}/{item['case']}: {item['detail']}\n")
    sys.stdout.write(f"{len(results) - len(failed)} passed, {len(failed)} failed\n")
    return 1 if failed or not results else 0


if __name__ == "__main__":
    sys.exit(main())
