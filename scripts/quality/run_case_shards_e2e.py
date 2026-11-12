"""E2E for the case shard helper: complete and disjoint shards, repeatable packing, and a combine that fails on a missing or doubled case.

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_case_shards_e2e``. Each scenario runs ``python -m scripts.quality.case_shards``
(plan, select and combine) on a synthetic case inventory and synthetic saved costs. The shard manifests that the runners of a suite write come from the
helper's own ``write_manifest``. A scenario that plants a defect (a missing shard, a missing, doubled or failed case, another inventory) passes only when the
combine fails and names it. The report goes to data/e2e/case_shards/<UTC>_<pid>/report.json.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.quality import case_shards

ROOT = Path(__file__).resolve().parents[2]
HELPER = "scripts.quality.case_shards"
SUITE = "demo"
INVENTORY = [f"case_{number:02d}" for number in range(1, 13)]
COSTS = {**{f"case_{number:02d}": 20.0 for number in range(1, 9)}, "case_11": 90.0, "case_12": 80.0}
COUNT = 4
SECONDS = 30
Scenario = Callable[[Path], str]


def run_helper(arguments: list[str]) -> tuple[int, str, dict[str, Any]]:
    """Run the helper and return its exit code, its combined output and its standard output read as JSON (empty when it is not JSON)."""
    result = run_command(sys.executable, ["-m", HELPER, *arguments], cwd=ROOT, timeout=SECONDS)
    try:
        document = json.loads(result.stdout)
    except json.JSONDecodeError:
        document = {}
    return result.returncode, result.stdout + result.stderr, document if isinstance(document, dict) else {}


def write_inputs(work: Path, inventory: list[str] | None = None, costs: dict[str, Any] | None = None) -> list[str]:
    """Write the inventory file and the costs file of the demo suite into ``work`` and return the common arguments that name them."""
    (work / "inventory.json").write_text(json.dumps(INVENTORY if inventory is None else inventory))
    (work / "costs.json").write_text(json.dumps({"suites": {SUITE: COSTS if costs is None else costs}}))
    return ["--suite", SUITE, "--inventory", str(work / "inventory.json"), "--costs", str(work / "costs.json")]


def plan(work: Path, count: int = COUNT) -> dict[str, list[str]]:
    code, output, document = run_helper(["plan", *write_inputs(work), "--count", str(count)])
    if code != 0:
        raise AssertionError(f"plan failed: {output.strip()}")
    shards: dict[str, list[str]] = document["shards"]
    return shards


def write_shards(work: Path, count: int = COUNT, change: Callable[[int, list[str]], list[str]] | None = None) -> Path:
    """Write the manifests that a complete run of every shard leaves; ``change`` may alter the cases a shard reports as run."""
    root = work / "shards"
    for index in range(1, count + 1):
        selected = case_shards.select(INVENTORY, f"{index}/{count}", COSTS)
        executed = selected if change is None else change(index, selected)
        rows = [{"case": name, "ok": True, "seconds": 1.0} for name in executed]
        case_shards.write_manifest(root / f"shard_{index}", SUITE, f"{index}/{count}", INVENTORY, selected, rows)
    return root


def combine(work: Path, root: Path, count: int = COUNT) -> tuple[int, str, dict[str, Any]]:
    return run_helper(["combine", *write_inputs(work), "--count", str(count), "--directory", str(root)])


def expect_refusal(arguments: list[str], text: str) -> str:
    code, output, _ = run_helper(arguments)
    if code != 2 or text not in output:
        return f"expected exit 2 naming {text!r}, got exit {code}: {output.strip()[-200:]}"
    return ""


def expect_combine_failure(work: Path, root: Path, text: str, count: int = COUNT) -> str:
    code, output, document = combine(work, root, count)
    if code != 1 or document.get("status") != "fail" or text not in " | ".join(document.get("problems", [])):
        return f"expected exit 1 naming {text!r}, got exit {code}: {output.strip()[-300:]}"
    return ""


def plan_covers_every_case_once(work: Path) -> str:
    shards = plan(work)
    flat = [name for names in shards.values() for name in names]
    if sorted(flat) != INVENTORY or len(shards) != COUNT:
        return f"the shards do not hold every case once: {shards}"
    return "" if all(shards.values()) else "a shard is empty"


def plan_is_a_pure_function(work: Path) -> str:
    first, second = plan(work), plan(work)
    if first != second:
        return "two plans of one inventory and one costs file differ"
    shuffled = write_inputs(work, costs=dict(reversed(list(COSTS.items()))))
    code, _, document = run_helper(["plan", *shuffled, "--count", str(COUNT)])
    return "" if code == 0 and document["shards"] == first else "the order of the costs changed the plan"


def plan_balances_by_cost(work: Path) -> str:
    """The heaviest shard stays within the guarantee of packing the heaviest case first, which inventory order or round robin breaks for late heavy cases."""
    shards = plan(work)
    median = sorted(COSTS.values())[len(COSTS) // 2]
    loads = [sum(COSTS.get(name, median) for name in names) for names in shards.values()]
    bound = (4 / 3 - 1 / (3 * COUNT)) * max(max(COSTS.values()), sum(loads) / COUNT)
    return "" if max(loads) <= bound else f"the heaviest shard holds {max(loads)} s, more than the bound {bound} s"


def plan_without_costs_goes_round_robin(work: Path) -> str:
    code, _, document = run_helper(["plan", *write_inputs(work, costs={}), "--count", str(COUNT)])
    expected = {f"{index + 1}/{COUNT}": INVENTORY[index::COUNT] for index in range(COUNT)}
    return "" if code == 0 and document.get("shards") == expected else f"expected round robin, got {document.get('shards')}"


def plan_ignores_costs_of_unknown_cases(work: Path) -> str:
    code, _, document = run_helper(["plan", *write_inputs(work, costs={**COSTS, "case_gone": 500.0}), "--count", str(COUNT)])
    flat = [name for names in document.get("shards", {}).values() for name in names]
    return "" if code == 0 and sorted(flat) == INVENTORY else f"a cost of a case that is not in the inventory changed the cases: {flat}"


def select_matches_the_plan(work: Path) -> str:
    shards = plan(work)
    for index in range(1, COUNT + 1):
        code, _, document = run_helper(["select", *write_inputs(work), "--shard", f"{index}/{COUNT}"])
        if code != 0 or document.get("cases") != shards[f"{index}/{COUNT}"]:
            return f"the command for shard {index}/{COUNT} names other cases than the plan: {document}"
    return ""


def shard_option_is_checked(work: Path) -> str:
    common = write_inputs(work)
    for spec in ("0/4", "5/4", "2", "a/b", "1/0", "-1/4"):
        problem = expect_refusal(["select", *common, f"--shard={spec}"], "K/N")
        if problem:
            return f"{spec}: {problem}"
    return ""


def inventory_and_costs_are_checked(work: Path) -> str:
    cases = (
        ("duplicate case names", dict(inventory=["a", "a", "b"]), "unique"),
        ("an empty inventory", dict(inventory=[]), "nonempty"),
        ("a zero cost", dict(costs={"case_01": 0}), "positive"),
        ("a text cost", dict(costs={"case_01": "slow"}), "positive"),
    )
    for label, change, text in cases:
        problem = expect_refusal(["plan", *write_inputs(work, **change), "--count", "2"], text)
        if problem:
            return f"{label}: {problem}"
    for count in ("0", str(len(INVENTORY) + 1)):
        problem = expect_refusal(["plan", *write_inputs(work), "--count", count], "shard count")
        if problem:
            return f"count {count}: {problem}"
    return ""


def combine_passes_complete_shards(work: Path) -> str:
    code, output, document = combine(work, write_shards(work))
    cases = [row["case"] for row in document.get("cases", [])]
    if code != 0 or document.get("status") != "pass" or sorted(cases) != INVENTORY:
        return f"expected a pass with every case once, got exit {code}: {output.strip()[-300:]}"
    return "" if (work / "shards" / "combined.json").is_file() else "combined.json was not written"


def combine_fails_a_missing_shard(work: Path) -> str:
    root = write_shards(work)
    shutil.rmtree(root / "shard_3")
    return expect_combine_failure(work, root, "missing or invalid shard 3/4")


def combine_fails_a_missing_case(work: Path) -> str:
    root = write_shards(work, change=lambda index, selected: selected[:-1] if index == 2 else selected)
    return expect_combine_failure(work, root, "shard 2/4: missing, repeated or unexpected cases")


def combine_fails_a_doubled_case(work: Path) -> str:
    stolen = case_shards.select(INVENTORY, f"1/{COUNT}", COSTS)[0]
    root = write_shards(work, change=lambda index, selected: [*selected, stolen] if index == 2 else selected)
    return expect_combine_failure(work, root, "shard 2/4: missing, repeated or unexpected cases")


def combine_fails_a_case_run_twice_in_one_shard(work: Path) -> str:
    root = write_shards(work, change=lambda index, selected: [*selected, selected[0]] if index == 4 else selected)
    return expect_combine_failure(work, root, "shard 4/4: missing, repeated or unexpected cases")


def combine_fails_a_failed_case(work: Path) -> str:
    root = write_shards(work)
    path = root / "shard_1" / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["cases"][0]["ok"] = False
    path.write_text(json.dumps(manifest))
    return expect_combine_failure(work, root, "shard 1/4: a case failed or the manifest is invalid")


def combine_fails_another_inventory(work: Path) -> str:
    root = write_shards(work)
    path = root / "shard_2" / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["inventory_sha256"] = "0" * 64
    path.write_text(json.dumps(manifest))
    return expect_combine_failure(work, root, "shard 2/4: another case inventory")


def combine_fails_the_wrong_selection(work: Path) -> str:
    root = write_shards(work)
    path = root / "shard_2" / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["selected_cases"] = list(reversed(manifest["selected_cases"]))
    path.write_text(json.dumps(manifest))
    return expect_combine_failure(work, root, "shard 2/4: missing, repeated or unexpected cases")


def combine_fails_a_copied_manifest(work: Path) -> str:
    root = write_shards(work)
    shutil.copyfile(root / "shard_1" / "manifest.json", root / "shard_2" / "manifest.json")
    return expect_combine_failure(work, root, "shard 2/4: another suite or shard")


def combine_fails_another_shard_count(work: Path) -> str:
    return expect_combine_failure(work, write_shards(work), "shard 1/3: another suite or shard", count=3)


def combine_fails_a_broken_manifest(work: Path) -> str:
    root = write_shards(work)
    (root / "shard_4" / "manifest.json").write_text("{not json")
    return expect_combine_failure(work, root, "missing or invalid shard 4/4")


SCENARIOS: tuple[Scenario, ...] = (
    plan_covers_every_case_once,
    plan_is_a_pure_function,
    plan_balances_by_cost,
    plan_without_costs_goes_round_robin,
    plan_ignores_costs_of_unknown_cases,
    select_matches_the_plan,
    shard_option_is_checked,
    inventory_and_costs_are_checked,
    combine_passes_complete_shards,
    combine_fails_a_missing_shard,
    combine_fails_a_missing_case,
    combine_fails_a_doubled_case,
    combine_fails_a_case_run_twice_in_one_shard,
    combine_fails_a_failed_case,
    combine_fails_another_inventory,
    combine_fails_the_wrong_selection,
    combine_fails_a_copied_manifest,
    combine_fails_another_shard_count,
    combine_fails_a_broken_manifest,
)


def run_scenario(scenario: Scenario) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="hai_case_shards_") as scratch:
        try:
            problem = scenario(Path(scratch))
        except (AssertionError, KeyError, OSError, ValueError) as error:
            problem = f"{type(error).__name__}: {error}"
    return {"name": scenario.__name__, "correct": problem == "", "problem": problem}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--output-directory", type=Path, default=ROOT / "data" / "e2e" / "case_shards")
    args = parser.parse_args()
    started = datetime.now(UTC)
    results = [run_scenario(scenario) for scenario in SCENARIOS]
    for result in results:
        sys.stdout.write(f"{'ok' if result['correct'] else 'WRONG':<6} {result['name']}{'' if result['correct'] else ': ' + result['problem']}\n")
    failures = sum(not result["correct"] for result in results)
    folder = args.output_directory / f"{started:%Y%m%dT%H%M%SZ}_{os.getpid()}"
    folder.mkdir(parents=True)
    report = {"started": started.isoformat(), "passed": len(results) - failures, "total": len(results), "cases": results}
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"{report['passed']} of {report['total']} scenarios behaved as expected; report {folder / 'report.json'}\n")
    return int(failures > 0)


if __name__ == "__main__":
    sys.exit(main())
