"""Plan, record and combine case shards for any E2E runner or check that splits its cases into independent parts.

A suite names its cases in a fixed inventory order. The helper packs them into ``--shard K/N`` by the saved cost of each case
(``config/quality/shard_costs.json``), heaviest first, each into the lightest shard so far, so the assignment is a pure function of the inventory and the
costs. Each shard writes one manifest of the cases it selected and ran, and ``combine`` fails when a shard is missing, a case is missing, a case runs twice or
a case failed. The same rules serve every suite, in place of one combine step per suite. Run from the repository root:

    .venv/bin/python -m scripts.quality.case_shards plan --suite <name> --inventory <cases.json> --count <N>
    .venv/bin/python -m scripts.quality.case_shards select --suite <name> --inventory <cases.json> --shard <K/N>
    .venv/bin/python -m scripts.quality.case_shards combine --suite <name> --inventory <cases.json> --count <N> --directory <folder>

``combine`` reads ``<folder>/shard_<K>/manifest.json`` for every K and writes ``<folder>/combined.json``.
E2E: ``.venv/bin/python -m scripts.quality.run_case_shards_e2e``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
COSTS = ROOT / "config" / "quality" / "shard_costs.json"
SHARD = re.compile(r"([1-9][0-9]*)/([1-9][0-9]*)")


def parse_shard(text: str) -> tuple[int, int]:
    """Read one-based ``K/N`` into (K, N)."""
    match = SHARD.fullmatch(text)
    if match is None or int(match[1]) > int(match[2]):
        raise ValueError(f"--shard must be K/N with 1 <= K <= N, not {text!r}")
    return int(match[1]), int(match[2])


def check_inventory(names: list[str]) -> None:
    if not names or len(names) != len(set(names)):
        raise ValueError("the case inventory must be nonempty and contain unique names")


def check_count(names: list[str], count: int) -> None:
    if count < 1 or count > len(names):
        raise ValueError("the shard count must be between 1 and the number of cases")


def saved_costs(suite: str, path: Path = COSTS) -> dict[str, float]:
    """The saved cost in seconds of each case of the suite; a suite or a case without a saved cost counts as the median cost when packing."""
    suites = json.loads(path.read_text(encoding="utf-8")).get("suites", {})
    costs = suites.get(suite, {}) if isinstance(suites, dict) else {}
    for name, cost in costs.items():
        if isinstance(cost, bool) or not isinstance(cost, int | float) or cost <= 0:
            raise ValueError(f"the saved costs must be positive numbers: {name!r} of suite {suite!r} is {cost!r}")
    return {name: float(cost) for name, cost in costs.items()}


def weighted_shards(names: list[str], costs: dict[str, float], count: int) -> list[list[str]]:
    """Pack the cases into ``count`` shards: heaviest first, each into the lightest shard so far (ties: the earlier case, the lower shard).

    A case without a saved cost counts as the median saved cost, or as 1 when no case has one, which gives round robin in inventory order. Every case is in
    exactly one shard, and each shard lists its cases in inventory order.
    """
    known = sorted(costs[name] for name in names if name in costs)
    fallback = known[len(known) // 2] if known else 1.0
    weight = [costs.get(name, fallback) for name in names]
    loads = [0.0] * count
    members: list[list[int]] = [[] for _ in range(count)]
    for index in sorted(range(len(names)), key=lambda position: (-weight[position], position)):
        target = min(range(count), key=lambda shard: (loads[shard], shard))
        members[target].append(index)
        loads[target] += weight[index]
    return [[names[index] for index in sorted(shard)] for shard in members]


def select(names: list[str], shard: str, costs: dict[str, float]) -> list[str]:
    """The cases of one-based K/N."""
    check_inventory(names)
    index, count = parse_shard(shard)
    check_count(names, count)
    return weighted_shards(names, costs, count)[index - 1]


def inventory_sha256(names: list[str]) -> str:
    return hashlib.sha256(json.dumps(names).encode()).hexdigest()


def write_manifest(directory: Path, suite: str, shard: str, inventory: list[str], selected: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Write the manifest of one shard: the cases it selected and the rows (``case``, ``ok``, ``seconds``) of the cases it ran.

    The status is ``fail`` when the cases that ran are not the cases selected, each once, or when a case failed.
    """
    problems = []
    if Counter(row["case"] for row in rows) != Counter(selected):
        problems.append("the cases that ran differ from the selected cases")
    if any(row.get("ok") is not True for row in rows):
        problems.append("a case failed")
    manifest = {
        "suite": suite,
        "shard": shard,
        "inventory_sha256": inventory_sha256(inventory),
        "selected_cases": selected,
        "cases": rows,
        "problems": problems,
        "status": "fail" if problems else "pass",
    }
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def shard_problems(manifest: Any, suite: str, shard: str, inventory: list[str], expected: list[str]) -> list[str]:
    """What is wrong with one shard's manifest, judged against the inventory and the shard's expected cases."""
    if not isinstance(manifest, dict):
        return [f"invalid shard {shard}: the manifest is not an object"]
    problems = []
    if manifest.get("suite") != suite or manifest.get("shard") != shard:
        problems.append(f"shard {shard}: another suite or shard")
    if manifest.get("status") != "pass" or manifest.get("problems"):
        problems.append(f"shard {shard}: did not pass")
    if manifest.get("inventory_sha256") != inventory_sha256(inventory):
        problems.append(f"shard {shard}: another case inventory")
    rows = manifest.get("cases")
    if not isinstance(rows, list) or any(not isinstance(row, dict) or not isinstance(row.get("case"), str) or row.get("ok") is not True for row in rows):
        return [*problems, f"shard {shard}: a case failed or the manifest is invalid"]
    if Counter(row["case"] for row in rows) != Counter(expected) or manifest.get("selected_cases") != expected:
        problems.append(f"shard {shard}: missing, repeated or unexpected cases")
    return problems


def combine(suite: str, inventory: list[str], count: int, directory: Path, costs: dict[str, float]) -> dict[str, Any]:
    """Require a passing manifest from every shard with exactly its expected cases, and every case of the inventory once in all."""
    check_inventory(inventory)
    check_count(inventory, count)
    rows: list[dict[str, Any]] = []
    problems: list[str] = []
    for index in range(1, count + 1):
        shard = f"{index}/{count}"
        try:
            manifest = json.loads((directory / f"shard_{index}" / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            problems.append(f"missing or invalid shard {shard}: {error}")
            continue
        problems.extend(shard_problems(manifest, suite, shard, inventory, select(inventory, shard, costs)))
        if isinstance(manifest, dict) and isinstance(manifest.get("cases"), list):
            rows.extend(row for row in manifest["cases"] if isinstance(row, dict))
    if Counter(row.get("case") for row in rows) != Counter(inventory):
        problems.append("the combined cases do not cover every case of the inventory exactly once")
    result = {
        "suite": suite,
        "shards": count,
        "inventory_sha256": inventory_sha256(inventory),
        "cases": rows,
        "problems": problems,
        "status": "fail" if problems else "pass",
    }
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "combined.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def read_inventory(path: Path) -> list[str]:
    names = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise ValueError(f"{path} must hold a JSON list of case names")
    return names


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("command", choices=("plan", "select", "combine"))
    parser.add_argument("--suite", required=True)
    parser.add_argument("--inventory", type=Path, required=True, help="JSON list of the case names of the suite, in inventory order")
    parser.add_argument("--costs", type=Path, default=COSTS)
    parser.add_argument("--count", type=int, help="the number of shards (plan and combine)")
    parser.add_argument("--shard", help="one-based K/N (select)")
    parser.add_argument("--directory", type=Path, help="the folder of the shard_<K> folders (combine)")
    args = parser.parse_args()
    try:
        names = read_inventory(args.inventory)
        check_inventory(names)
        costs = saved_costs(args.suite, args.costs)
        if args.command == "select":
            if args.shard is None:
                parser.error("select requires --shard")
            result: dict[str, Any] = {"suite": args.suite, "shard": args.shard, "cases": select(names, args.shard, costs)}
        else:
            if args.count is None:
                parser.error(f"{args.command} requires --count")
            check_count(names, args.count)
            if args.command == "plan":
                shards = weighted_shards(names, costs, args.count)
                result = {
                    "suite": args.suite,
                    "inventory_sha256": inventory_sha256(names),
                    "shards": {f"{index}/{args.count}": cases for index, cases in enumerate(shards, 1)},
                }
            else:
                if args.directory is None:
                    parser.error("combine requires --directory")
                result = combine(args.suite, names, args.count, args.directory, costs)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps(result, indent=2) + "\n")
    return int(result.get("status") == "fail")


if __name__ == "__main__":
    raise SystemExit(main())
