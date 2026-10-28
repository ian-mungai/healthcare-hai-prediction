"""Case-level failing-first evidence for the orchestration runners.

Run ``.venv/bin/python -m scripts.orchestration.run_failing_first [--runner NAME ...] [--case NAME ...]``, or with ``--check`` (a fast check in local CI that
every mutant still matches the code and every case has its evidence). For every case of ``run_broker_e2e``,
``run_supervisor_e2e`` and ``run_stack_e2e`` that has an entry in ``guard_mutants.GUARDS``, and for each validator group of ``offline_checks``, it makes two
kinds of run, each in a fresh copy of the source tree, and writes one log per case:

1. the **control**: the case in an unchanged copy, with a valid input. Every check must pass.
2. the **mutant**: the same case in a copy whose code has one guard removed (the text edits named in ``guard_mutants.py``, shown in the log as a diff).
   The case must fail on the check the table names (the *intended assertion*) and must not stop with an exception; every other check is listed.

A case is *failing-first verified* when its control passes and each of its mutants fails the intended check without an exception. The logs are
``<logs>/<runner>/<case>.log``; the summary is ``<logs>/summary.json`` and ``data/e2e/orchestration_failing_first/<UTC>/report.json``. The mutants are
reconstructions of the code before each guard: no earlier tree snapshot is available.
Only fake Docker and test registries are used. The copies live under ``data/e2e/orchestration_failing_first/<UTC>/runs/`` (the cases mount folders of
their report folder, which the launcher allows only under a ``data`` folder) and are removed after each run.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.orchestration import guard_mutants, run_broker_e2e, run_stack_e2e, run_supervisor_e2e
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "orchestration_failing_first"
LOGS = ROOT / "plans" / "wp" / "a1_logs" / "r3" / "failing_first"
# What a runner needs from the repository; the copy leaves out data, plans, documents and the Git folder.
SOURCE = ("scripts", "orchestration", "config", "data_contracts", "services", "dags", "dbt", "docker-compose.yaml", "pyproject.toml")
# The folder under data/e2e where each runner writes its report.
RUNNERS = {"broker": "orchestration_broker", "supervisor": "orchestration_supervisor", "stack": "orchestration_stack"}
LEFTOVER = "nothing of the case is left over"
OFFLINE_SECTIONS = ("mutations", "graph", "distribution")


def copy_tree(destination: Path) -> Path:
    """A copy of the source the runners need."""
    destination.mkdir(parents=True)
    for name in SOURCE:
        source = ROOT / name
        if source.is_dir():
            shutil.copytree(source, destination / name, ignore=shutil.ignore_patterns("__pycache__", ".mypy_cache", ".ruff_cache"), symlinks=True)
        else:
            shutil.copy2(source, destination / name)
    return destination


def in_tree(tree: Path, module: str, arguments: list[str]) -> Any:
    """Run a module of the copy with the repository's Python, from the copy's root."""
    environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR") if name in os.environ}
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return run_command(sys.executable, ["-m", module, *arguments], cwd=tree, env=environment, timeout=1500)


def latest_report(tree: Path, folder: str) -> dict[str, Any]:
    """The report the last run in a copy wrote."""
    reports = sorted((tree / "data" / "e2e" / folder).glob("*/report.json"))
    if not reports:
        return {}
    loaded: dict[str, Any] = json.loads(reports[-1].read_text())
    return loaded


def run_case(runner: str, case: str, tree: Path) -> dict[str, Any]:
    """One case of a runner in a copy: its checks, its error and whether it passed."""
    if runner == "broker":
        process = in_tree(tree, "scripts.orchestration.run_broker_e2e", ["--case", case])
    elif runner == "supervisor":
        process = in_tree(tree, "scripts.orchestration.run_supervisor_e2e", ["--case", case])
    else:
        process = in_tree(tree, "scripts.orchestration.run_stack_e2e", ["--case", case])
    cases = latest_report(tree, RUNNERS[runner]).get("cases") or [{}]
    result: dict[str, Any] = cases[0]
    if not result:
        error = f"the runner wrote no report (exit {process.returncode}): {(process.stderr or process.stdout)[-300:]}"
        return {"passed": False, "checks": {}, "error": error, "seconds": 0}
    return {"passed": result["status"] == "pass", "checks": result["checks"], "error": result["error"], "seconds": result["seconds"]}


def failed_checks(result: dict[str, Any]) -> list[str]:
    """The checks of a case run that failed, not counting the leftover check."""
    return [name for name, ok in result["checks"].items() if not ok and name != LEFTOVER]


def verdict(control: dict[str, Any], mutant: dict[str, Any], intended: str) -> tuple[bool, str]:
    """Whether a mutant run shows the intended failure on a clean control, and why not when it does not."""
    if not control["passed"]:
        return False, "the control run did not pass"
    if mutant["error"]:
        return False, "the mutant run stopped with an exception, not on an assertion"
    if not any(intended in name for name in failed_checks(mutant)):
        return False, "the intended check did not fail"
    return True, "the intended check failed on the mutant and passed on the control"


def check_lines(checks: dict[str, bool]) -> list[str]:
    """Every check, one line each."""
    return [f"  {'ok' if ok else 'FAILED'}: {name}" for name, ok in checks.items()]


def write_log(path: Path, title: str, control: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    """The log of one case: the control run, then each mutant with its diff, its verdict and every check."""
    lines = [f"case {title}", "", f"control: an unchanged copy of the tree, valid input: {'pass' if control['passed'] else 'FAIL'} ({control['seconds']} s)"]
    lines += check_lines(control["checks"])
    if control["error"]:
        lines.append(f"  error: {control['error'].splitlines()[0]}")
    for row in rows:
        lines += ["", f"mutant {row['mutant']}: {guard_mutants.MUTANTS[row['mutant']].what}", f"  intended check: {row['intended']}"]
        lines += [f"  verdict: {'failing-first verified' if row['ok'] else 'NOT VERIFIED'}: {row['why']}", "  the code change:"]
        lines += [f"    {line}" for line in row["diff"].rstrip("\n").splitlines()]
        lines += check_lines(row["result"]["checks"])
        if row["result"]["error"]:
            lines.append(f"  error: {row['result']['error'].splitlines()[0]}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The mutant rows without their bulky run results and diffs."""
    return [{key: value for key, value in row.items() if key not in ("result", "diff")} for row in rows]


def run_runner(runner: str, selected: list[str], logs: Path, scratch: Path) -> list[dict[str, Any]]:
    """Control and mutant runs of the guarded cases of one runner."""
    rows: list[dict[str, Any]] = []
    for case, guards in guard_mutants.GUARDS[runner].items():
        if selected and case not in selected:
            continue
        tree = copy_tree(scratch / f"{case}_control")
        control = run_case(runner, case, tree)
        shutil.rmtree(tree, ignore_errors=True)
        sys.stdout.write(f"{runner}/{case}: control {'ok' if control['passed'] else 'FAIL'}\n")
        sys.stdout.flush()
        case_rows: list[dict[str, Any]] = []
        for guard in guards:
            tree = copy_tree(scratch / f"{case}_{guard.mutant}")
            diff = guard_mutants.apply(tree, guard_mutants.MUTANTS[guard.mutant])
            result = run_case(runner, case, tree)
            shutil.rmtree(tree, ignore_errors=True)
            ok, why = verdict(control, result, guard.check)
            case_rows.append({"mutant": guard.mutant, "intended": guard.check, "ok": ok, "why": why, "diff": diff, "result": result})
            sys.stdout.write(f"  {'ok' if ok else 'FAIL'} {guard.mutant}: {why}\n")
            sys.stdout.flush()
        write_log(logs / runner / f"{case}.log", f"{runner}/{case}", control, case_rows)
        rows.append({"runner": runner, "case": case, "control_passed": control["passed"], "mutants": summarize(case_rows)})
    return rows


def offline_entries(tree: Path) -> dict[str, bool]:
    """The entries of the offline checks that the mutation, graph and distribution sections produce in a copy, by name."""
    arguments = [item for section in OFFLINE_SECTIONS for item in ("--only", section)]
    in_tree(tree, "scripts.orchestration.offline_checks", arguments)
    results = latest_report(tree, "orchestration_offline").get("results") or {}
    return {name: bool(entry["ok"]) for name, entry in results.items()}


def run_offline(selected: list[str], logs: Path, scratch: Path) -> list[dict[str, Any]]:
    """Control and mutant runs of the offline validators. The control is every mutation, graph and distribution entry passing on the unchanged code; a
    mutant turns one validator off, and the entry that feeds it a bad input must then fail."""
    tree = copy_tree(scratch / "offline_control")
    control_entries = offline_entries(tree)
    shutil.rmtree(tree, ignore_errors=True)
    control = {"passed": bool(control_entries) and all(control_entries.values()), "checks": control_entries, "error": "", "seconds": 0}
    rows: list[dict[str, Any]] = []
    for group, guards in guard_mutants.GUARDS["offline"].items():
        if selected and group not in selected:
            continue
        case_rows: list[dict[str, Any]] = []
        for guard in guards:
            tree = copy_tree(scratch / f"offline_{guard.mutant}")
            diff = guard_mutants.apply(tree, guard_mutants.MUTANTS[guard.mutant])
            entries = offline_entries(tree)
            shutil.rmtree(tree, ignore_errors=True)
            result = {"passed": all(entries.values()), "checks": entries, "error": "", "seconds": 0}
            ok = control["passed"] and entries.get(guard.check) is False
            why = "the intended entry failed on the mutant and passed on the control" if ok else "the intended entry did not fail, or the control did not pass"
            case_rows.append({"mutant": guard.mutant, "intended": guard.check, "ok": ok, "why": why, "diff": diff, "result": result})
            sys.stdout.write(f"offline/{group}: {'ok' if ok else 'FAIL'} {guard.mutant}: {sum(1 for v in entries.values() if not v)} entries fail\n")
            sys.stdout.flush()
        write_log(logs / "offline" / f"{group}.log", f"offline/{group}", control, case_rows)
        rows.append({"runner": "offline", "case": group, "control_passed": control["passed"], "mutants": summarize(case_rows)})
    return rows


def check_table() -> list[str]:
    """The problems of the table itself: a mutant that no longer matches the code, a guard that names a missing mutant or case, a mutant no guard uses,
    and a runner case with neither a guard nor a before-fix log. It reads the repository and runs no case."""
    problems: list[str] = []
    cases: dict[str, dict[str, Any]] = {"broker": run_broker_e2e.CASES, "supervisor": run_supervisor_e2e.CASES, "stack": run_stack_e2e.CASES}
    used = {guard.mutant for table in guard_mutants.GUARDS.values() for guards in table.values() for guard in guards}
    for name, mutant in guard_mutants.MUTANTS.items():
        try:
            guard_mutants.apply(ROOT, mutant, write=False)
        except (guard_mutants.MutantError, StopIteration, OSError) as error:
            problems.append(f"mutant {name} does not match the code: {error}")
        if name not in used:
            problems.append(f"mutant {name} is no guard of any case")
    for runner, table in guard_mutants.GUARDS.items():
        for case, guards in table.items():
            if runner in cases and case not in cases[runner]:
                problems.append(f"{runner}/{case} is not a case of the runner")
            problems += [f"{runner}/{case}: no mutant {guard.mutant}" for guard in guards if guard.mutant not in guard_mutants.MUTANTS]
    for runner, registered in cases.items():
        covered = set(guard_mutants.GUARDS[runner]) | set(guard_mutants.BEFORE_FIX.get(runner, ()))
        problems += [f"{runner}/{case} has neither a guard mutant nor a before-fix log" for case in registered if case not in covered]
        problems += [
            f"{runner}/{case} is listed as having a before-fix log but is not a case"
            for case in guard_mutants.BEFORE_FIX.get(runner, ())
            if case not in registered
        ]
    return problems


def main() -> int:
    """Run the table and write the logs and the report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--runner", action="append", default=[], choices=[*RUNNERS, "offline"])
    parser.add_argument("--case", action="append", default=[], help="a case (or offline group) name; default: every guarded one")
    parser.add_argument("--logs", type=Path, default=LOGS, help="where the per-case logs go")
    parser.add_argument("--check", action="store_true", help="only check that the table matches the code and the runners' cases; run no case")
    args = parser.parse_args()
    if args.check:
        problems = check_table()
        sys.stdout.write(
            "".join(f"{problem}\n" for problem in problems)
            + (f"{len(problems)} problem(s)\n" if problems else f"ok: {len(guard_mutants.MUTANTS)} mutants match the code\n")
        )
        return 1 if problems else 0
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    folder = OUT / stamp
    scratch = folder / "runs"
    scratch.mkdir(parents=True)
    rows: list[dict[str, Any]] = []
    for runner in args.runner or [*RUNNERS, "offline"]:
        rows += run_offline(args.case, args.logs, scratch) if runner == "offline" else run_runner(runner, args.case, args.logs, scratch)
    shutil.rmtree(scratch, ignore_errors=True)
    verified = sum(1 for row in rows if row["control_passed"] and row["mutants"] and all(m["ok"] for m in row["mutants"]))
    options = "".join(f" --runner {runner}" for runner in args.runner) + "".join(f" --case {case}" for case in args.case)
    summary = {
        "feature": "case-level failing-first evidence: control on an unchanged copy, mutant with one guard removed",
        "command": ".venv/bin/python -m scripts.orchestration.run_failing_first" + options,
        "started_utc": stamp,
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "cases": rows,
        "verified": verified,
        "total": len(rows),
        "limits": ["The mutants reconstruct the code before each guard; no earlier version of the round 2 tree exists. Fake Docker and test registries only."],
    }
    (folder / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    args.logs.mkdir(parents=True, exist_ok=True)
    (args.logs / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{verified}/{len(rows)} cases failing-first verified: {folder.relative_to(ROOT)}/report.json\n")
    return 0 if verified == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
