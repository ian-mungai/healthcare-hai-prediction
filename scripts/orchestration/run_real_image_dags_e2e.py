"""Run every DAG that launches a host job in the pinned Airflow image and record the result.

The run is the ``real_dags`` section of the in-image check (``scripts/orchestration/in_image_checks.py``, which runs ``real_image_dags.py``): Airflow's own
test runner executes each DAG's tasks with the real task SDK, the real Amazon provider operator and the production launch services, against the offline
rig's fake ECS, SSM and S3 behind a loopback bridge. The launcher runs it on no network, as it runs the other in-image checks. This runner makes no Docker
call. Run from the repository root with the launcher up:

``.venv/bin/python -m scripts.orchestration.run_real_image_dags_e2e [--test-registry <folder>/registry]``

``--report <file>`` records the output of an in-image run made elsewhere instead (a file with a ``REAL_IMAGE_DAGS`` or ``IN_IMAGE`` line). Each run writes
``data/e2e/real_image_dags/<UTC timestamp>.json`` (ignored by Git) with the revision, the command, the substitutions, the versions of Airflow and the
Amazon provider and one row per scenario with its DAG runs. The exit code is 0 when every scenario passed and every DAG file of the image was run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.orchestration import offline_checks, real_image_dags
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "orchestration/airflow_steps.py",
    "orchestration/dag_runtime.py",
    "orchestration/ecs_launch.py",
    "orchestration/ecs_launch_operator.py",
    "orchestration/ecs_launch_provider.py",
    "scripts/orchestration/aws_http_bridge.py",
    "scripts/orchestration/real_image_dags.py",
    "scripts/orchestration/in_image_checks.py",
    "scripts/orchestration/run_real_image_dags_e2e.py",
    "services/orchestration/docker-compose.orchestration.yaml",
)
LINE_PREFIXES = (real_image_dags.REPORT_PREFIX, "IN_IMAGE ")


def git(*arguments: str) -> str:
    return run_command("git", list(arguments), cwd=ROOT, timeout=30).stdout.strip()


def read_report(path: Path) -> tuple[dict[str, Any], list[str]]:
    """The real-image report and the DAG ids of the image from a file with one report line."""
    lines = [line for line in path.read_text().splitlines() if line.startswith(LINE_PREFIXES)]
    if len(lines) != 1:
        raise ValueError(f"{path} must hold exactly one REAL_IMAGE_DAGS or IN_IMAGE line, not {len(lines)}")
    prefix = next(item for item in LINE_PREFIXES if lines[0].startswith(item))
    loaded = json.loads(lines[0].removeprefix(prefix))
    if prefix == "IN_IMAGE ":
        return loaded["real_dags"], loaded["dags"]["dag_ids"]
    return loaded, sorted(set(loaded["dags"]) | set(loaded.get("import_errors", {})))


def launcher_report(test_registry: str | None) -> tuple[dict[str, Any], list[str]]:
    """Run the in-image check through the launcher and return its real-image section and the DAG ids of the image."""
    platform = json.loads(offline_checks.PLATFORM_FILE.read_text())
    image = offline_checks.in_image(platform, test_registry)
    if "error" in image:
        raise RuntimeError(f"the in-image check did not run: {image['error']}")
    return image["real_dags"], image["dags"]["dag_ids"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--test-registry", help="the launcher's registry folder of a test launcher")
    parser.add_argument("--report", type=Path, help="record the output of an in-image run made elsewhere")
    args = parser.parse_args()
    started = datetime.now(UTC)
    if args.report is not None:
        section, dag_ids = read_report(args.report)
    else:
        section, dag_ids = launcher_report(args.test_registry)
    scenarios = section.get("scenarios", [])
    failed = [item for item in scenarios if item.get("ok") is not True]
    uncovered = sorted(set(dag_ids) - set(section.get("dags", [])))
    problem = offline_checks.real_dags_problem(section)
    ok = not problem and "error" not in section and not failed and not uncovered and not section.get("import_errors")
    artifact = {
        "feature": "every host DAG runs in the pinned Airflow image",
        "at": started.isoformat(),
        "revision": git("rev-parse", "HEAD") or "unknown",
        "uncommitted": git("status", "--short").splitlines(),
        "command": " ".join(["python", "-m", "scripts.orchestration.run_real_image_dags_e2e", *sys.argv[1:]]),
        "source": str(args.report) if args.report else "launcher: orchestration_check (in_image_checks.py real_dags)",
        "substitutions": section.get("substitutions", list(real_image_dags.SUBSTITUTIONS)),
        "versions": section.get("versions", {}),
        "sources": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES if (ROOT / name).exists()},
        "dag_ids_in_image": dag_ids,
        "dags_run": section.get("dags", []),
        "uncovered": uncovered,
        "import_errors": section.get("import_errors", {}),
        "error": section.get("error") or problem or None,
        "seconds": section.get("seconds"),
        "scenarios": scenarios,
        "ok": ok,
    }
    folder = ROOT / "data" / "e2e" / "real_image_dags"
    folder.mkdir(parents=True, exist_ok=True)
    record = folder / f"{started.strftime('%Y%m%dT%H%M%SZ')}.json"
    record.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    for item in failed:
        sys.stdout.write(f"FAIL {item.get('name')}: {str(item.get('detail'))[:600]}\n")
    for name in uncovered:
        sys.stdout.write(f"FAIL no scenario runs {name}\n")
    if artifact["error"]:
        sys.stdout.write(f"FAIL {artifact['error']}\n")
    sys.stdout.write(f"{len(scenarios) - len(failed)} passed, {len(failed)} failed; {len(artifact['dags_run'])} of {len(dag_ids)} DAGs run; record {record}\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
