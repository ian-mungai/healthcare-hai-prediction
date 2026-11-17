"""Run the lineage cases of the direct ECS jobs offline: facets, events, refusals, delivery to a local stand-in receiver and the replay command.

No AWS call, no Docker, no OpenMetadata and no credential is used: the bucket is in memory and the receiver is a local HTTP stand-in on a loopback port.
Each run writes ``data/e2e/direct_ecs_lineage/<timestamp>.json`` (ignored by Git) with the revision, the changed files, the substitutions and one line per
case. Run from the repository root: ``.venv/bin/python -m scripts.orchestration.run_ecs_lineage_e2e [--case NAME]``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

from scripts.orchestration import ecs_lineage_cases, job_lineage_cases, raw_lineage_cases
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "orchestration/ecs_lineage.py",
    "orchestration/raw_lineage.py",
    "orchestration/job_lineage.py",
    "scripts/lakehouse/layer_run.py",
    "scripts/orchestration/raw_lineage_cases.py",
    "scripts/orchestration/job_lineage_cases.py",
    "orchestration/snapshots.py",
    "orchestration/ecs_launch_operator.py",
    "data_contracts/orchestration/launch_lineage_facet.schema.json",
    "data_contracts/openlineage/OpenLineage-2-0-2.json",
    "config/metadata/ingestion/glue.json",
    "config/lakehouse/athena_mirror.json",
    "scripts/orchestration/ecs_lineage_cases.py",
    "scripts/orchestration/run_ecs_lineage_e2e.py",
)
SUBSTITUTIONS = [
    "in-memory write-once bucket for the launch records and the S3 evidence of the layer runs",
    "fake ECS and Logs clients and a virtual clock for the launch that writes the launch records",
    "a local HTTP stand-in of the OpenMetadata lineage route (no OpenMetadata runs; dataset resolution is not tested)",
    "a stand-in of the Amazon provider 9.36.0 operator; the provider's OpenLineage listener is not run",
    "small declared Glue mirror datasets of the cases beside the contract-derived lineage, which the cases derive again from the pin, load and publish code",
]


def git(*arguments: str) -> str:
    return run_command("git", list(arguments), cwd=ROOT, timeout=30).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case")
    args = parser.parse_args()
    results = []
    for name, case in {**ecs_lineage_cases.CASES, **raw_lineage_cases.CASES, **job_lineage_cases.CASES}.items():
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
        "feature": "direct ECS lineage",
        "at": started.isoformat(),
        "revision": git("rev-parse", "HEAD") or "unknown",
        "uncommitted": git("status", "--short").splitlines(),
        "mode": "full" if not args.case else "iteration",
        "command": " ".join(["python", "-m", "scripts.orchestration.run_ecs_lineage_e2e", *sys.argv[1:]]),
        "substitutions": SUBSTITUTIONS,
        "sources": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES if (ROOT / name).exists()},
        "results": results,
    }
    folder = ROOT / "data" / "e2e" / "direct_ecs_lineage"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{started.strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    for item in failed:
        sys.stdout.write(f"FAIL {item['case']}: {item['detail']}\n")
    sys.stdout.write(f"{len(results) - len(failed)} passed, {len(failed)} failed\n")
    return 1 if failed or not results else 0


if __name__ == "__main__":
    sys.exit(main())
