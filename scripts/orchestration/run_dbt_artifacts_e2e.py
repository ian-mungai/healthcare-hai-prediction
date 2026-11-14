"""Run the cases of the dbt artifact rewrite for OpenMetadata's dbt connector offline: every model of each layer moves to its published table.

No dbt, no OpenMetadata, no AWS call and no credential is used: the manifests are stand-ins. Each run writes ``data/e2e/dbt_artifacts/<timestamp>.json``
(ignored by Git) with the revision, the changed files, the substitutions and one line per case. Run from the repository root:
``.venv/bin/python -m scripts.orchestration.run_dbt_artifacts_e2e [--case NAME]``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

from scripts.orchestration import dbt_artifact_cases
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "scripts/lakehouse/dbt_catalog_artifacts.py",
    "scripts/orchestration/dbt_artifact_cases.py",
    "scripts/orchestration/run_dbt_artifacts_e2e.py",
    "config/metadata/ingestion/dbt.json",
)
SUBSTITUTIONS = ["stand-in dbt manifests, catalogs and run results with the fields the OpenMetadata 2.0.7 dbt connector and the rewrite read; no dbt runs"]


def git(*arguments: str) -> str:
    return run_command("git", list(arguments), cwd=ROOT, timeout=30).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case")
    args = parser.parse_args()
    results = []
    for name, case in dbt_artifact_cases.CASES.items():
        if args.case and args.case != name:
            continue
        try:
            case()
            results.append({"case": name, "outcome": "pass"})
        except Exception as error:
            detail = "".join(traceback.format_exception_only(type(error), error)).strip()
            results.append({"case": name, "outcome": "fail", "detail": detail})
    failed = [item for item in results if item["outcome"] == "fail"]
    started = datetime.now(UTC)
    artifact = {
        "feature": "dbt artifact rewrite",
        "at": started.isoformat(),
        "revision": git("rev-parse", "HEAD") or "unknown",
        "uncommitted": git("status", "--short").splitlines(),
        "mode": "full" if not args.case else "iteration",
        "command": " ".join(["python", "-m", "scripts.orchestration.run_dbt_artifacts_e2e", *sys.argv[1:]]),
        "substitutions": SUBSTITUTIONS,
        "sources": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES if (ROOT / name).exists()},
        "results": results,
    }
    folder = ROOT / "data" / "e2e" / "dbt_artifacts"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{started.strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    for item in failed:
        sys.stdout.write(f"FAIL {item['case']}: {item['detail']}\n")
    sys.stdout.write(f"{len(results) - len(failed)} passed, {len(failed)} failed\n")
    return 1 if failed or not results else 0


if __name__ == "__main__":
    sys.exit(main())
