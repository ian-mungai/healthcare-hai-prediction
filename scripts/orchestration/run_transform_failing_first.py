"""Case-level failing-first evidence for the transform broker runner (``run_transform_broker_e2e``).

Run ``.venv/bin/python -m scripts.orchestration.run_transform_failing_first [--case NAME ...]``. For every case in ``GUARDS`` it makes two runs, each in a fresh
copy of the source tree, and writes one log per case under ``plans/wp/a2_logs/failing_first/``:

1. the **control**: the case in an unchanged copy. Every check must pass.
2. the **mutant**: the same case in a copy with one guard removed (the text edit named in ``MUTANTS``, shown in the log as a diff). The case must fail the check
   the table names (the *intended check*) and must not stop with an exception; every other failed check is listed.

An edit names the exact text it replaces and how often it must occur, so a mutant that no longer matches the code fails loudly. The copies live under
``data/e2e/orchestration_transform_failing_first/<UTC>/runs/`` (the cases mount folders of their report folder, which the launcher allows only under a
``data`` folder) and are removed after each run. Only fake Docker and test registries are used.
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "orchestration_transform_failing_first"
LOGS = ROOT / "plans" / "wp" / "a2_logs" / "failing_first"
SOURCE = ("scripts", "orchestration", "config", "data_contracts", "services", "dags", "dbt", "tests", "docker-compose.yaml", "pyproject.toml")
RUNNER = "scripts.orchestration.run_transform_broker_e2e"
REPORTS = "orchestration_transform_broker"
LEFTOVER = "nothing of the case is left over"
RUN_CONTROL = "scripts/orchestration/run_control.py"
BROKER = "scripts/orchestration/broker.py"
TEMPLATES = "config/orchestration/broker_templates.json"


@dataclass(frozen=True)
class Edit:
    path: str
    old: str
    new: str
    count: int = 1


@dataclass(frozen=True)
class Mutant:
    what: str
    edits: tuple[Edit, ...]


@dataclass(frozen=True)
class Guard:
    case: str
    mutant: str
    check: str


def block(*lines: str) -> str:
    return "".join(f"{line}\n" for line in lines)


REFUSED_LOCK = '            raise ControlRefused(f"the launcher refused the lock: {error} [{error.code}]") from None'
MUTANTS: dict[str, Mutant] = {
    "acquire_refusal_ignored": Mutant(
        "a launcher refusal of the lock is answered with a success",
        (Edit(RUN_CONTROL, REFUSED_LOCK, '            return {"run_folder": "", "seeded": False, "already_held": True}'),),
    ),
    "gate_accepts_any_state": Mutant(
        "an accepted gate result is written whatever state the run is in",
        (
            Edit(
                RUN_CONTROL,
                block(
                    '    if result == "accepted" and state != "finalizing":',
                    "        raise ControlRefused(f\"an accepted result needs a finalizing run, not {state or 'no lock'}\")",
                ),
                "",
            ),
        ),
    ),
    "release_ignores_gate_result": Mutant(
        "a release goes ahead without a recorded accepted result",
        (
            Edit(
                RUN_CONTROL,
                block(
                    '    if body is None or body.get("gate", {}).get("result") != "accepted":',
                    '        raise ControlRefused("the recorded gate result is not accepted")',
                ),
                "",
            ),
        ),
    ),
    "build_file_path_unchecked": Mutant(
        "a build file outside the run folder is deleted",
        (
            Edit(
                RUN_CONTROL,
                block(
                    "        if target.parent.resolve() != folder.resolve():",
                    '            raise ControlRefused(f"the build file {target.name} is outside the run folder")',
                ),
                "",
            ),
        ),
    ),
    "record_does_not_finalize": Mutant(
        "the record step writes the file and leaves the run held",
        (
            Edit(
                RUN_CONTROL,
                block(
                    "    try:",
                    '        broker.call("finalize", {}, broker.principal(request.run_id))',
                    "    except launcher.ClientError as error:",
                    '        raise ControlRefused(f"the launcher refused to finalize: {error} [{error.code}]") from None',
                ),
                "",
            ),
        ),
    ),
    "rejected_release_keeps_lock_held": Mutant(
        "a rejected release does not retain the lock",
        (Edit(RUN_CONTROL, '            broker.call("mark", {"state": "failed_retained"}, broker.principal(request.run_id))', "            pass"),),
    ),
    "rejected_release_leaves_attempts": Mutant(
        "a rejected release does not stop the open attempts",
        (
            Edit(
                RUN_CONTROL,
                block("    stopped = stop_open(broker, request)", "    state = run_state(broker, request)", '    if state in ("held", "finalizing"):'),
                block("    stopped: list[str] = []", "    state = run_state(broker, request)", '    if state in ("held", "finalizing"):'),
            ),
        ),
    ),
    "seed_unchecked": Mutant(
        "a seed is copied without checking where it is or what it holds",
        (
            Edit(
                RUN_CONTROL,
                block(
                    '    if not source.is_dir() or not inside(source, [broker.repo_root / "data", broker.paths.state]):',
                    '        raise ControlRefused(f"the seed {source.name} is not a folder inside the data folder")',
                    '    if any(item.is_symlink() for item in source.rglob("*")):',
                    '        raise ControlRefused("the seed holds a link")',
                ),
                "",
            ),
        ),
    ),
    "no_control_recovery": Mutant(
        "a control intent that survived a crash is not repeated by the reconciliation",
        (
            Edit(
                BROKER,
                block(
                    "        if self.recorded_as_control(lines[0]):",
                    "            self.complete_control(request, lines[0])",
                    "            return",
                    "        self.require_serving()",
                ),
                block("        self.require_serving()"),
            ),
        ),
    ),
    "row_check_accepts_anything": Mutant(
        "a control row with missing or extra parameters loads",
        (
            Edit(
                RUN_CONTROL,
                block('    action = control.get("action")', "    if action not in ACTIONS:"),
                block("    return", '    action = control.get("action")', "    if action not in ACTIONS:"),
            ),
        ),
    ),
    "control_identity_from_row": Mutant(
        "a submission is taken for a control action when the present row is one, whatever its intent was",
        (
            Edit(
                BROKER,
                '        return CONTROL_INPUT in intent["inputs"]',
                '        return "control" in self.templates.get(intent["template"], {}).get("modes", {}).get(intent["mode"], {})',
            ),
        ),
    ),
    "build_file_unchecked": Mutant(
        "gold_build is sent without comparing the build file with the validated one",
        (Edit(BROKER, '        problem = run_control.checkpoint_problem(intent["inputs"])\n', "        problem = None\n"),),
    ),
    "build_file_not_recorded": Mutant(
        "silver_validate records nothing about the build file",
        (Edit(BROKER, "        if RECORDS_INPUT not in inputs:\n            return {}\n", "        return {}\n"),),
    ),
    "completion_records_the_present_file": Mutant(
        "silver_validate is recorded without comparing the file on the host with the SHA-256 the validator reported",
        (
            Edit(
                RUN_CONTROL,
                '    if digest != reported:\n        raise ControlFailed(f"the build file changed while',
                '    if False:\n        raise ControlFailed(f"the build file changed while',
            ),
        ),
    ),
    "completion_ignores_validator": Mutant(
        "silver_validate records the SHA-256 of the file on the host and never reads what the validator reported",
        (
            Edit(
                RUN_CONTROL,
                "    reported = reported_sha256(result)\n    digest = file_sha256(path)\n",
                "    digest = file_sha256(path)\n    reported = digest\n",
            ),
        ),
    ),
    "completion_from_present_row": Mutant(
        "silver_validate takes its checkpoint and SHA-256 from the present row, not from the intent",
        (
            Edit(
                BROKER,
                block('        inputs = self.lines_of(request)[0]["inputs"]', "        if RECORDS_INPUT not in inputs:"),
                block("        inputs = self.checkpoint_inputs(request, self.row_for(request))", "        if RECORDS_INPUT not in inputs:"),
            ),
        ),
    ),
    "dispatch_from_present_row": Mutant(
        "gold_build is checked against the checkpoint of the present row, not the intent's",
        (
            Edit(
                BROKER,
                '        problem = run_control.checkpoint_problem(intent["inputs"])\n',
                "        problem = run_control.checkpoint_problem(self.checkpoint_inputs(request, row))\n",
            ),
        ),
    ),
    "gate_task_states_dropped": Mutant(
        "the gate writes no task states into the run record",
        (
            Edit(
                RUN_CONTROL,
                '    evidence = task_state_evidence(params["expected_states"], snapshot)\n',
                '    evidence = {"status": "unavailable", "reason": "not read"}\n',
            ),
        ),
    ),
    "gate_accepts_failed_predecessor": Mutant(
        "an accepted gate request is written as accepted although a predecessor task did not end as expected",
        (Edit(RUN_CONTROL, '    contradicted = result == "accepted" and bool(evidence.get("failed_tasks"))\n', "    contradicted = False\n"),),
    ),
    "unavailable_lookup_made_complete": Mutant(
        "a lookup that cannot answer is recorded as an empty complete snapshot",
        (
            Edit(
                RUN_CONTROL,
                '        return {"status": "unavailable", "reason": str(snapshot["unavailable"])}\n',
                '        return {"status": "read", "dag_run_id": "", "dag_run_state": "", "predecessors": [], "failed_tasks": []}\n',
            ),
        ),
    ),
    "shipped_gold_selector_wrong": Mutant(
        "the shipped gold_build row selects the silver models",
        (Edit(TEMPLATES, '"path:models/gold"', '"path:models/silver"'),),
    ),
}
GUARDS = (
    Guard("control_acquire", "acquire_refusal_ignored", "a second run is refused by the launcher with exit code 125"),
    Guard("control_gate", "gate_accepts_any_state", "an accepted result while the run is only held is refused even with a record"),
    Guard("control_release_needs_gate", "release_ignores_gate_result", "a release with no recorded gate result is refused"),
    Guard("control_release_accepted", "build_file_path_unchecked", "a build file outside the run folder is refused and nothing is released"),
    Guard("control_record", "record_does_not_finalize", "the first record names the run and finalizing, and the lock is finalizing"),
    Guard("control_release_rejected", "rejected_release_keeps_lock_held", "the lock is failed_retained and the build file is kept"),
    Guard("control_release_stops_open", "rejected_release_leaves_attempts", "the control stopped one attempt"),
    Guard("control_acquire_seed", "seed_unchecked", "a seed with a link is refused"),
    Guard("control_crash_after_delete", "no_control_recovery", "the lock is released and the ledger ends with finished"),
    Guard("control_crash_after_record", "no_control_recovery", "the reconciliation finished the submission and the run is finalizing"),
    Guard("control_crash_after_finalize", "no_control_recovery", "the reconciliation finished the submission and the run is still finalizing"),
    Guard("control_rows_schema", "row_check_accepts_anything", "a missing parameter is refused"),
    Guard("shipped_rows_render", "shipped_gold_selector_wrong", "gold_build builds the gold models"),
    Guard("control_release_stops_forwarded_after_row_drift", "control_identity_from_row", "the release stopped the forwarded attempt"),
    Guard("build_file_checkpoint", "build_file_unchecked", "a changed build file ends gold_build with exit code 125 and no container"),
    Guard("build_file_checkpoint", "build_file_not_recorded", "silver_validate records the SHA-256, the run and the attempt of the file"),
    Guard(
        "build_file_changed_before_completion",
        "completion_records_the_present_file",
        "a file that changed after the validator exited is not recorded as validated",
    ),
    Guard(
        "build_file_replaced_during_validation",
        "completion_ignores_validator",
        "a file replaced while the validator read it and restored before the end is not recorded as validated",
    ),
    Guard(
        "build_file_replaced_during_validation",
        "completion_records_the_present_file",
        "a file replaced while the validator read it and restored before the end is not recorded as validated",
    ),
    Guard("build_file_result_unusable", "completion_ignores_validator", "a job that wrote no result file ends with exit code 1 and records nothing"),
    Guard(
        "build_file_checkpoint_from_intent", "completion_from_present_row", "its completion followed the intent: the SHA-256 of the validated file is recorded"
    ),
    Guard(
        "build_file_checkpoint_from_intent",
        "dispatch_from_present_row",
        "after its row stopped requiring the file, gold_build is still refused for the changed file",
    ),
    Guard("control_gate_task_states", "gate_task_states_dropped", "a task that failed before any submission is named with its expected and observed state"),
    Guard(
        "control_gate_task_states",
        "gate_accepts_failed_predecessor",
        "an accepted request against a failed predecessor is refused and the record holds the rejection",
    ),
    Guard(
        "control_gate_task_states", "unavailable_lookup_made_complete", "an unavailable lookup is recorded as unavailable with its reason and no predecessors"
    ),
)


def copy_tree(destination: Path) -> Path:
    destination.mkdir(parents=True)
    for name in SOURCE:
        source = ROOT / name
        if source.is_dir():
            shutil.copytree(source, destination / name, ignore=shutil.ignore_patterns("__pycache__", ".mypy_cache", ".ruff_cache"), symlinks=True)
        else:
            shutil.copy2(source, destination / name)
    return destination


def apply(tree: Path, mutant: Mutant) -> str:
    diff: list[str] = []
    for edit in mutant.edits:
        path = tree / edit.path
        text = path.read_text()
        if text.count(edit.old) != edit.count:
            raise SystemExit(f"mutant edit does not match {edit.path}: {edit.old[:60]!r} occurs {text.count(edit.old)} times, not {edit.count}")
        changed = text.replace(edit.old, edit.new)
        path.write_text(changed)
        diff += difflib.unified_diff(text.splitlines(), changed.splitlines(), f"a/{edit.path}", f"b/{edit.path}", lineterm="", n=1)
    return "\n".join(diff)


def run_case(tree: Path, case: str) -> dict[str, Any]:
    environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR") if name in os.environ}
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    process = run_command(sys.executable, ["-m", RUNNER, "--case", case], cwd=tree, env=environment, timeout=900)
    reports = sorted((tree / "data" / "e2e" / REPORTS).glob("*/report.json"))
    if not reports:
        return {
            "passed": False,
            "checks": {},
            "error": f"the runner wrote no report (exit {process.returncode}): {(process.stderr or process.stdout)[-300:]}",
            "seconds": 0,
        }
    result = json.loads(reports[-1].read_text())["cases"][0]
    return {"passed": result["status"] == "pass", "checks": result["checks"], "error": result["error"], "seconds": result["seconds"]}


def failed(result: dict[str, Any]) -> list[str]:
    return [name for name, ok in result["checks"].items() if not ok and name != LEFTOVER]


def lines_of(checks: dict[str, bool]) -> list[str]:
    return [f"  {'ok' if ok else 'FAILED'}: {name}" for name, ok in checks.items()]


def verdict(control: dict[str, Any], mutant: dict[str, Any], intended: str) -> tuple[bool, str]:
    if not control["passed"]:
        return False, "the control run did not pass"
    if mutant["error"]:
        return False, "the mutant run stopped with an exception, not on an assertion"
    if not any(intended in name for name in failed(mutant)):
        return False, "the intended check did not fail"
    return True, "the intended check failed on the mutant and passed on the control"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", action="append", default=[], choices=sorted({guard.case for guard in GUARDS}))
    args = parser.parse_args()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    runs = OUT / stamp / "runs"
    LOGS.mkdir(parents=True, exist_ok=True)
    summary: list[dict[str, Any]] = []
    controls: dict[str, dict[str, Any]] = {}
    for number, guard in enumerate(guard for guard in GUARDS if not args.case or guard.case in args.case):
        if guard.case not in controls:
            tree = copy_tree(runs / f"control_{guard.case}")
            controls[guard.case] = run_case(tree, guard.case)
            shutil.rmtree(tree, ignore_errors=True)
        tree = copy_tree(runs / f"mutant_{number}")
        diff = apply(tree, MUTANTS[guard.mutant])
        result = run_case(tree, guard.case)
        shutil.rmtree(tree, ignore_errors=True)
        ok, why = verdict(controls[guard.case], result, guard.check)
        control = controls[guard.case]
        log = [
            f"case {guard.case}",
            "",
            f"control: an unchanged copy of the tree: {'pass' if control['passed'] else 'FAIL'} ({control['seconds']} s)",
            *lines_of(control["checks"]),
        ]
        log += [
            "",
            f"mutant {guard.mutant}: {MUTANTS[guard.mutant].what}",
            f"  intended check: {guard.check}",
            f"  verdict: {'failing-first verified' if ok else 'NOT VERIFIED'}: {why}",
        ]
        log += ["  the code change:", *[f"    {line}" for line in diff.splitlines()], *lines_of(result["checks"])]
        if result["error"]:
            log.append(f"  error: {result['error'].splitlines()[0]}")
        (LOGS / f"{guard.case}__{guard.mutant}.log").write_text("\n".join(log) + "\n")
        summary.append({"case": guard.case, "mutant": guard.mutant, "verified": ok, "why": why, "failed_checks": failed(result)})
        sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {guard.case} / {guard.mutant}: {why}\n")
    folder = OUT / stamp
    (folder / "report.json").write_text(json.dumps({"started_utc": stamp, "guards": summary}, indent=2) + "\n")
    shutil.rmtree(runs, ignore_errors=True)
    sys.stdout.write(f"{sum(1 for item in summary if item['verified'])}/{len(summary)}: {(folder / 'report.json').relative_to(ROOT)}\n")
    return 0 if all(item["verified"] for item in summary) else 1


if __name__ == "__main__":
    sys.exit(main())
