"""Run every DAG that launches a host job in the pinned Airflow image and record the result.

One command does all of it in one process (owner decision A, Nov 11 2026): it starts the pinned image through the launcher, the image runs every scenario of
``scripts/orchestration/real_image_dags.py`` and returns its results, and this process judges those results and writes the record. No report made by another
process is read, so a result can never be older than the source it certifies. The scenarios run Airflow's own test runner (``DAG.test``, the task SDK runner)
with the real Amazon provider operator and the production launch services against the offline rig's fake ECS, SSM and S3 behind a loopback bridge. The
launcher runs the image on no network, as it runs the other in-image checks (``in_image_checks.py``). This runner makes no Docker call. Run from the
repository root with the launcher up:

``.venv/bin/python -m scripts.orchestration.run_real_image_dags_e2e [--test-registry <folder>/registry]``

The verdict is a pass only when every condition holds: the run declares a pass, every scenario passed, every recorded step ended in ``success`` or ``failed``
and in the state its scenario expected (also when another step covers the same DAG), the image holds exactly the DAG ids that the files of ``dags/`` define
(read from the files, not from the results), each of them has a DAG run that finished in a terminal state its scenario expected and no source or
``dags/*.py`` file was changed, added or removed while the image ran (the inventory with hashes is taken before the run and compared after it) and the
result is bound to this attempt (``real_image_dags.binding_problems``: a run time from the start to the end of the attempt, with no clock allowance, and the
inputs of this checkout, which the host hashed before and after the attempt). The record keeps the attempt id and any binding problem. Each run
writes ``data/e2e/real_image_dags/<UTC timestamp>-<8 hex digits>.json`` (ignored by Git), created exclusively so that no record is ever replaced. It holds the
revision, the command, the hashes of the sources and the DAG files that the image ran from, the substitutions, the versions of Airflow and the Amazon provider
and one row per scenario. The exit code is 0 for a pass.

Accepted limit (owner decision, Nov 11 2026): the binding cannot tell a replay from a new run. A result that was returned by an earlier attempt, carries a run
time inside this attempt's window and names the same file hashes still passes, because the launcher's ``render_orchestration_check`` takes no argument, so
the host cannot pass the image a one-time value to hand back. Closing the limit needs a launcher change. The issue register entry for it is
``data/e2e/real_image_dags_20261111/issue_register_replay.md``.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import secrets
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.orchestration import offline_checks, real_image_dags
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = real_image_dags.SOURCES
RECORD_TRIES = 20


def git(*arguments: str) -> str:
    return run_command("git", list(arguments), cwd=ROOT, timeout=30).stdout.strip()


def now() -> datetime:
    return datetime.now(UTC)


def discover_dag_ids(folder: Path) -> list[str]:
    """The DAG ids that the files of the DAG folder define, read from their source (``DAG(dag_id="...")``), so the inventory never comes from a result.
    A file that defines no DAG the reader can name is an error, because it could hide a DAG."""
    found: set[str] = set()
    for path in sorted(folder.glob("*.py")):
        named = {
            keyword.value.value
            for node in ast.walk(ast.parse(path.read_text()))
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "DAG"
            for keyword in node.keywords
            if keyword.arg == "dag_id" and isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str)
        }
        if not named:
            raise ValueError(f"{path.name} defines no DAG with a literal dag_id")
        found |= named
    return sorted(found)


def image_run(test_registry: str | None) -> tuple[dict[str, Any], list[str], str | None, list[str]]:
    """Start the image through the launcher and return the real-image section of its results, the DAG ids the image holds, the identity of the attempt and
    the shared judgement of whether the section is this attempt's result on these inputs (``binding_problems``, empty when it is)."""
    platform = json.loads(offline_checks.PLATFORM_FILE.read_text())
    image = offline_checks.in_image(platform, test_registry)
    if "error" in image:
        raise RuntimeError(f"the in-image check did not run: {image['error']}")
    run = image.get("real_dags_run")
    attempt_id = run.get("attempt_id") if isinstance(run, dict) and isinstance(run.get("attempt_id"), str) else None
    return image["real_dags"], image["dags"]["dag_ids"], attempt_id, real_image_dags.binding_problems(image["real_dags"], run)


def input_changes(before: dict[str, dict[str, str]], after: dict[str, dict[str, str]]) -> list[str]:
    """Each source or DAG file that was changed, added or removed between two inventories, by name."""
    changes = []
    for group in ("sources", "dag_files"):
        for name in sorted(set(before[group]) | set(after[group])):
            if name not in after[group]:
                changes.append(f"removed {name}")
            elif name not in before[group]:
                changes.append(f"added {name}")
            elif before[group][name] != after[group][name]:
                changes.append(f"changed {name}")
    return changes


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_record(folder: Path, started: datetime, artifact: dict[str, Any]) -> Path:
    """Write the record under a name no earlier record has, created exclusively: a name that exists is never opened for writing."""
    folder.mkdir(parents=True, exist_ok=True)
    for _ in range(RECORD_TRIES):
        path = folder / f"{started.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(4)}.json"
        try:
            with path.open("x") as handle:
                handle.write(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
        except FileExistsError:
            continue
        return path
    raise RuntimeError(f"no free record name in {folder} after {RECORD_TRIES} tries")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--test-registry", help="the launcher's registry folder of a test launcher")
    args = parser.parse_args()
    started = now()
    before = real_image_dags.inventory(ROOT)
    section, image_dag_ids, attempt_id, binding = image_run(args.test_registry)
    changes = input_changes(before, real_image_dags.inventory(ROOT))
    scenarios = section.get("scenarios", [])
    failed = [item for item in scenarios if item.get("ok") is not True]
    expected = discover_dag_ids(ROOT / "dags")
    finished = real_image_dags.finished_dags(scenarios)
    uncovered = sorted(set(expected) - set(finished))
    problem = offline_checks.real_dags_problem(section)
    bad_steps = real_image_dags.step_problems(scenarios) if isinstance(scenarios, list) else []
    differs = None
    if set(image_dag_ids) != set(expected):
        differs = (
            f"the image holds {sorted(set(image_dag_ids) - set(expected))} that the DAG folder lacks, and lacks {sorted(set(expected) - set(image_dag_ids))}"
        )
    ok = (
        section.get("ok") is True
        and not problem
        and not differs
        and "error" not in section
        and not failed
        and not uncovered
        and not changes
        and not bad_steps
        and not binding
        and not section.get("import_errors")
    )
    artifact = {
        "feature": "every host DAG runs in the pinned Airflow image",
        "at": started.isoformat(),
        "image_run_at": section.get("at"),
        "revision": git("rev-parse", "HEAD") or "unknown",
        "uncommitted": git("status", "--short").splitlines(),
        "command": " ".join(["python", "-m", "scripts.orchestration.run_real_image_dags_e2e", *sys.argv[1:]]),
        "source": "launcher: orchestration_check (in_image_checks.py real_dags)",
        "substitutions": section.get("substitutions", list(real_image_dags.SUBSTITUTIONS)),
        "versions": section.get("versions", {}),
        "sources": before["sources"],
        "dag_files": before["dag_files"],
        "inputs_unchanged": not changes,
        "input_changes": changes,
        "step_problems": bad_steps,
        "attempt_id": attempt_id,
        "binding_problems": binding,
        "dag_ids_in_image": image_dag_ids,
        "dag_ids_in_folder": expected,
        "dags_run": finished,
        "uncovered": uncovered,
        "import_errors": section.get("import_errors", {}),
        "error": "; ".join(str(item) for item in (section.get("error") or problem or differs, *changes, *bad_steps, *binding) if item) or None,
        "seconds": section.get("seconds"),
        "scenarios": scenarios,
        "ok": ok,
    }
    record = write_record(ROOT / "data" / "e2e" / "real_image_dags", started, artifact)
    for item in failed:
        sys.stdout.write(f"FAIL {item.get('name')}: {str(item.get('detail'))[:600]}\n")
    for name in uncovered:
        sys.stdout.write(f"FAIL no finished DAG run of {name}\n")
    if artifact["error"]:
        sys.stdout.write(f"FAIL {artifact['error']}\n")
    if section.get("ok") is not True and not failed and not artifact["error"]:
        sys.stdout.write("FAIL the image run did not declare a pass\n")
    sys.stdout.write(f"{len(scenarios) - len(failed)} passed, {len(failed)} failed; {len(finished)} of {len(expected)} DAGs run; record {record}\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
