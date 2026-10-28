"""Plan and combine complete feature E2E shards without running feature builds."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SUITES = ("sources", "columns")


@contextmanager
def timed_case(suite: str, name: str) -> Iterator[float]:
    """Print and flush a case's start and end times, including when the case raises an error."""
    started = time.monotonic()
    stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    sys.stdout.write(f"[timing] start feature {suite} case {name} at {stamp}\n")
    sys.stdout.flush()
    try:
        yield started
    finally:
        stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        sys.stdout.write(f"[timing] end feature {suite} case {name} at {stamp}: {time.monotonic() - started:.3f} s\n")
        sys.stdout.flush()


def select(names: list[str], shard: str = "", only: list[str] | None = None) -> list[str]:
    """Select every Nth case for one-based K/N after applying the optional substring filter."""
    if len(names) != len(set(names)) or not names:
        raise ValueError("feature case inventory must be nonempty and contain unique names")
    filtered = [name for name in names if not only or any(part in name for part in only)]
    if not filtered:
        raise ValueError("no feature cases match --only")
    if not shard:
        return filtered
    match = re.fullmatch(r"([1-9][0-9]*)/([1-9][0-9]*)", shard)
    if match is None or int(match[1]) > int(match[2]):
        raise ValueError(f"--shard must be K/N with 1 <= K <= N, not {shard!r}")
    index, count = int(match[1]), int(match[2])
    selected = filtered[index - 1 :: count]
    if not selected:
        raise ValueError(f"shard {shard} has no feature cases")
    return selected


def names_of(suite: str) -> list[str]:
    if suite == "sources":
        from scripts.lakehouse import run_feature_sources_e2e

        return run_feature_sources_e2e.case_names()
    if suite == "columns":
        from scripts.lakehouse import run_feature_columns_e2e

        return run_feature_columns_e2e.case_names()
    raise ValueError(f"unknown feature suite {suite!r}")


def snapshot(suite: str) -> dict[str, str]:
    """Hash the feature implementation and committed dbt inputs, excluding generated project folders."""
    files = [
        ROOT / f"scripts/lakehouse/run_feature_{suite}_e2e.py",
        ROOT / "scripts/lakehouse/feature_shards.py",
        ROOT / "scripts/lakehouse/feature_sources.py",
        ROOT / "scripts/lakehouse/feature_columns.py",
        ROOT / "scripts/lakehouse/feature_allowlist.py",
        ROOT / "data_contracts/lakehouse/gold_contract.json",
        ROOT / "requirements.txt",
        ROOT / "pyproject.toml",
        ROOT / "scripts/process.py",
        ROOT / "scripts/lakehouse/pipeline_build.py",
        ROOT / "scripts/lakehouse/run_gold_e2e.py",
        ROOT / "config/orchestration/broker_templates.json",
        ROOT / "scripts/quality/sqlfluff/requirements.txt",
        ROOT / "config/lakehouse/dbt_package_archive.json",
    ]
    files += [
        path
        for path in (ROOT / "dbt").rglob("*")
        if path.is_file() and not {"target", "logs", "dbt_packages"}.intersection(path.relative_to(ROOT / "dbt").parts)
    ]
    return {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(files)}


def finish(output: Path, suite: str, shard: str, inventory: list[str], selected: list[str], before: dict[str, str], report: dict[str, Any]) -> dict[str, Any]:
    """Persist one shard's exact membership and candidate identity, failing if inputs changed during execution."""
    actual = [row["case"] for row in report["cases"]]
    problems = []
    if Counter(actual) != Counter(selected):
        problems.append("executed cases differ from the selected shard")
    if snapshot(suite) != before:
        problems.append("feature inputs changed during the run")
    report.update(
        {"suite": suite, "shard": shard or "1/1", "case_inventory": inventory, "selected_cases": selected, "source_hashes": before, "problems": problems}
    )
    if problems:
        report["status"] = "fail"
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def combine(suite: str, count: int, directory: Path) -> dict[str, Any]:
    """Require a successful report from every shard with the current candidate and exactly the expected cases."""
    inventory = names_of(suite)
    identity = snapshot(suite)
    rows: list[dict[str, Any]] = []
    problems: list[str] = []
    if count < 1 or count > len(inventory):
        raise ValueError("shard count must be between 1 and the number of cases")
    for index in range(1, count + 1):
        shard = f"{index}/{count}"
        path = directory / f"shard_{index}" / "report.json"
        try:
            report = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            problems.append(f"missing or invalid shard {shard}: {error}")
            continue
        expected = select(inventory, shard)
        if not isinstance(report, dict):
            problems.append(f"invalid shard {shard}: report is not an object")
            continue
        if report.get("suite") != suite or report.get("shard") != shard:
            problems.append(f"shard {shard}: suite or shard identity differs")
        if report.get("status") != "pass" or report.get("problems"):
            problems.append(f"shard {shard}: did not pass")
        if report.get("source_hashes") != identity or report.get("case_inventory") != inventory:
            problems.append(f"shard {shard}: candidate or case inventory differs")
        cases = report.get("cases")
        if not isinstance(cases, list) or any(not isinstance(row, dict) or not isinstance(row.get("case"), str) or row.get("ok") is not True for row in cases):
            problems.append(f"shard {shard}: invalid or failed case results")
            continue
        names = [row["case"] for row in cases]
        if Counter(names) != Counter(expected) or report.get("selected_cases") != expected:
            problems.append(f"shard {shard}: missing, repeated or unexpected cases")
        rows.extend(cases)
    if Counter(row["case"] for row in rows) != Counter(inventory):
        problems.append("combined cases do not cover every expected case exactly once")
    result = {
        "written_utc": datetime.now(UTC).isoformat(),
        "suite": suite,
        "shards": count,
        "source_hashes": identity,
        "case_inventory": inventory,
        "cases": rows,
        "problems": problems,
        "status": "fail" if problems else "pass",
    }
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "combined.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "combine"))
    parser.add_argument("--suite", choices=SUITES, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    try:
        if args.count < 1 or args.count > len(names_of(args.suite)):
            raise ValueError("shard count must be between 1 and the number of cases")
        if args.command == "plan":
            names = names_of(args.suite)
            result = {
                "suite": args.suite,
                "case_inventory": names,
                "source_hashes": snapshot(args.suite),
                "shards": {f"{index}/{args.count}": select(names, f"{index}/{args.count}") for index in range(1, args.count + 1)},
            }
        else:
            if args.directory is None:
                parser.error("combine requires --directory")
            result = combine(args.suite, args.count, args.directory)
    except ValueError as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps(result, indent=2) + "\n")
    return int(result.get("status") == "fail")


if __name__ == "__main__":
    raise SystemExit(main())
