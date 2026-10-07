"""Compute DuckDB's memory limit for a real dbt build from the containers running now (failure modes 463 to 467).

Run from the repository root with Docker running:

    .venv/bin/python -m scripts.lakehouse.memory_budget            # print the limit, for example 26GB
    .venv/bin/python -m scripts.lakehouse.memory_budget --explain  # also print the figures it comes from

Owner decision (Oct 7 2026): memory is shared across every project's containers, so no limit is hardcoded. The limit is
Docker's total memory minus what every running container uses at that moment minus a fixed headroom, rounded down to
whole gigabytes. A budget below the floor, an unreadable Docker or an unknown size unit stops with the figures instead of
falling back to a fixed value. Failure modes: data/lakehouse_planning/group_c_20261006/failure_modes_c4.md.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from dataclasses import dataclass

from scripts.process import run_command

GIB = 1024**3
# Room for the dbt process itself, DuckDB's overshoot of its limit and the Docker VM [463]. Measured Oct 7 2026: a 30 GB
# limit beside 5 GB of other containers was killed on a 37.8 GB VM, so the build overshot its limit by more than 2.7 GB.
HEADROOM = 8 * GIB
# Below this a build would spill so much that it is better to free memory first [466].
FLOOR = 4 * GIB
UNITS = {"B": 1, "KB": 1000, "KIB": 1024, "MB": 1000**2, "MIB": 1024**2, "GB": 1000**3, "GIB": 1024**3, "TB": 1000**4, "TIB": 1024**4}
SIZE = re.compile(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*")


class BudgetError(RuntimeError):
    """Docker could not be read, a size was not understood or too little memory is free."""


@dataclass(frozen=True)
class Budget:
    """The figures a limit comes from, in bytes, and the limit as DuckDB reads it."""

    total: int
    used_by_containers: int
    headroom: int
    limit_gb: int

    @property
    def setting(self) -> str:
        """Return the limit as DuckDB's memory_limit setting."""
        return f"{self.limit_gb}GB"

    def record(self) -> dict[str, int | str]:
        """Return the figures and the setting, for a report or the --explain output [463]."""
        return {"total": self.total, "used_by_containers": self.used_by_containers, "headroom": self.headroom, "setting": self.setting}


def parse_size(text: str) -> int:
    """Return a Docker size such as 1.136GiB in bytes [464]."""
    match = SIZE.fullmatch(text)
    if not match or match.group(2).upper() not in UNITS:
        raise BudgetError(f"unknown Docker size {text!r}")
    return int(float(match.group(1)) * UNITS[match.group(2).upper()])


def docker(*args: str) -> str:
    """Return a docker command's output, or stop when Docker cannot be read [465]."""
    binary = shutil.which("docker")
    if not binary:
        raise BudgetError("docker is not on PATH")
    result = run_command(binary, list(args), timeout=120)
    if result.returncode:
        raise BudgetError(f"docker {' '.join(args)} failed: {result.stderr.strip()[-200:]}")
    return result.stdout


def compute(total: int, usages: list[int]) -> Budget:
    """Return the budget for a total and the running containers' use; too little free memory stops [463] [466]."""
    used = sum(usages)
    free = total - used - HEADROOM
    if free < FLOOR:
        raise BudgetError(
            f"only {free / GIB:.1f} GiB free for DuckDB: Docker {total / GIB:.1f} GiB, containers {used / GIB:.1f} GiB, headroom {HEADROOM / GIB:.0f} GiB"
        )
    return Budget(total=total, used_by_containers=used, headroom=HEADROOM, limit_gb=int(free // 1000**3))


def current() -> Budget:
    """Return the budget from Docker's total memory and every running container's current use [467]."""
    total = int(docker("info", "--format", "{{.MemTotal}}").strip())
    lines = [line for line in docker("stats", "--no-stream", "--format", "{{.MemUsage}}").splitlines() if line.strip()]
    return compute(total, [parse_size(line.split("/")[0]) for line in lines])


def main() -> int:
    """Print DuckDB's limit, and the figures behind it when asked."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--explain", action="store_true", help="also print the figures the limit comes from")
    args = parser.parse_args()
    try:
        budget = current()
    except BudgetError as error:
        sys.stderr.write(f"memory budget: {error}\n")
        return 1
    if args.explain:
        sys.stderr.write(json.dumps(budget.record()) + "\n")
    sys.stdout.write(budget.setting + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
