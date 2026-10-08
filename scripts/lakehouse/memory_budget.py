"""Compute DuckDB's memory limit and Spark's job heap from the containers running now (failure modes 463 to 467, 486 to 491).

Run from the repository root with Docker running:

    .venv/bin/python -m scripts.lakehouse.memory_budget            # print DuckDB's limit, for example 26GB
    .venv/bin/python -m scripts.lakehouse.memory_budget --spark    # print Spark's heap, for example 24g
    .venv/bin/python -m scripts.lakehouse.memory_budget --explain  # also print the figures it comes from

Owner decision (Oct 7 2026): memory is shared across every project's containers, so no limit is hardcoded. The limit is
the smaller of Docker's total memory minus what every running container uses at that moment minus a fixed headroom, and
the memory the Mac can give without swapping (vm_stat free, file-backed and purgeable pages) minus its own headroom; the
Mac cap is skipped once the Docker VM already holds all of Docker's memory (bundle v8.18.0, infrastructure.md item 3).
It is rounded down to whole gigabytes for DuckDB (GB) and whole gibibytes for Spark (g), whose heap also leaves room for
the JVM. A budget below the floor, an unreadable Docker or an
unknown size unit stops with the figures instead of falling back to a fixed value. Failure modes:
plans/group_c_20261006/failure_modes_c4.md and plans/spark_memory_20261008/failure_modes.md.
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
# Room left for macOS when the Mac's own free memory caps the limit (owner, Oct 8 2026) [493].
MAC_HEADROOM = 2 * GIB
# Below this a build would spill so much that it is better to free memory first [466] [497].
FLOOR = 4 * GIB
VM_PROCESS = "com.apple.Virtualization.VirtualMachine"
VM_STAT_PAGE = re.compile(r"page size of (\d+) bytes")
VM_STAT_COUNTED = ("Pages free", "File-backed pages", "Pages purgeable")
# Spark's documented driver overhead outside the heap: the larger of 10% of the heap or 384 MiB (owner fix for LKH-001,
# Oct 7 2026: the heap is the budget minus the JVM's own overhead) [492].
SPARK_OVERHEAD_FACTOR = 0.10
SPARK_MIN_OVERHEAD = 384 * 1024**2
UNITS = {"B": 1, "KB": 1000, "KIB": 1024, "MB": 1000**2, "MIB": 1024**2, "GB": 1000**3, "GIB": 1024**3, "TB": 1000**4, "TIB": 1024**4}
SIZE = re.compile(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*")


class BudgetError(RuntimeError):
    """Docker could not be read, a size was not understood or too little memory is free."""


@dataclass(frozen=True)
class Budget:
    """The figures a limit comes from, in bytes, and the limit as DuckDB and Spark read it."""

    total: int
    used_by_containers: int
    headroom: int
    # The Mac's free, file-backed and purgeable memory; None when the Docker VM already holds its memory [495].
    mac_available: int | None = None
    mac_headroom: int = MAC_HEADROOM

    @property
    def free(self) -> int:
        """Return the smaller of Docker's free memory and the Mac's, each less its headroom [493]."""
        docker_free = self.total - self.used_by_containers - self.headroom
        if self.mac_available is None:
            return docker_free
        return min(docker_free, self.mac_available - self.mac_headroom)

    @property
    def setting(self) -> str:
        """Return the limit as DuckDB's memory_limit setting, in whole decimal gigabytes."""
        return f"{self.free // 1000**3}GB"

    @property
    def spark_setting(self) -> str:
        """Return Spark's heap: the free memory less the JVM's overhead, in whole GiB (Spark's g unit), rounded down [487] [492]."""
        heap = min(self.free / (1 + SPARK_OVERHEAD_FACTOR), self.free - SPARK_MIN_OVERHEAD)
        return f"{int(heap // GIB)}g"

    def record(self) -> dict[str, int | str | None]:
        """Return the figures and the settings, for a report or the --explain output [463] [493]."""
        return {
            "total": self.total,
            "used_by_containers": self.used_by_containers,
            "headroom": self.headroom,
            "mac_available": self.mac_available,
            "mac_headroom": self.mac_headroom,
            "setting": self.setting,
            "spark_setting": self.spark_setting,
        }


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


def compute(total: int, usages: list[int], mac_available: int | None = None) -> Budget:
    """Return the budget for a total, the running containers' use and the Mac's available memory (None: no Mac cap);
    too little free memory stops with every figure [463] [466] [493] [497]."""
    budget = Budget(total=total, used_by_containers=sum(usages), headroom=HEADROOM, mac_available=mac_available)
    if budget.free < FLOOR:
        mac = "not capping" if mac_available is None else f"{mac_available / GIB:.1f} GiB less {MAC_HEADROOM / GIB:.0f} GiB headroom"
        raise BudgetError(
            f"only {budget.free / GIB:.1f} GiB free for DuckDB or Spark: Docker {total / GIB:.1f} GiB, "
            f"containers {budget.used_by_containers / GIB:.1f} GiB, headroom {HEADROOM / GIB:.0f} GiB; Mac {mac}"
        )
    return budget


def service_limit(budget: Budget, share: float, minimum: int) -> int:
    """Return a long-running service's memory limit: its share of the plan's free memory, at least the minimum, in whole
    MiB (owner, Oct 8 2026: Polaris 25% and at least 1 GiB, its database 10% and at least 256 MiB) [498] [503]."""
    mib = 1024**2
    return max(minimum, int(budget.free * share)) // mib * mib


def parse_vm_stat(text: str) -> int:
    """Return the bytes vm_stat counts as free, file-backed or purgeable; compressed memory is not counted [494] [496]."""
    page = VM_STAT_PAGE.search(text)
    if not page:
        raise BudgetError("vm_stat output has no page size")
    pages = {}
    for line in text.splitlines():
        name, _, value = line.partition(":")
        if name.strip() in VM_STAT_COUNTED:
            pages[name.strip()] = int(value.strip().rstrip("."))
    missing = [name for name in VM_STAT_COUNTED if name not in pages]
    if missing:
        raise BudgetError(f"vm_stat output lacks {', '.join(missing)}")
    return sum(pages.values()) * int(page.group(1))


def mac_available() -> int:
    """Return the memory the Mac can give without swapping, from vm_stat [493] [496]."""
    result = run_command("vm_stat", [], timeout=60)
    if result.returncode:
        raise BudgetError(f"vm_stat failed: {result.stderr.strip()[-200:]}")
    return parse_vm_stat(result.stdout)


def vm_resident() -> int:
    """Return the Docker VM process's resident memory in bytes, or 0 when it cannot be found, which keeps the Mac cap [495] [496]."""
    result = run_command("ps", ["-axo", "rss=,comm="], timeout=60)
    sizes = [int(line.split(None, 1)[0]) * 1024 for line in result.stdout.splitlines() if line.strip().endswith(VM_PROCESS)]
    return max(sizes, default=0)


def current() -> Budget:
    """Return the budget from Docker's total memory, every running container's current use and the Mac's available memory [467] [495]."""
    total = int(docker("info", "--format", "{{.MemTotal}}").strip())
    lines = [line for line in docker("stats", "--no-stream", "--format", "{{.MemUsage}}").splitlines() if line.strip()]
    mac = None if vm_resident() >= total else mac_available()
    return compute(total, [parse_size(line.split("/")[0]) for line in lines], mac)


def main() -> int:
    """Print DuckDB's limit or Spark's heap, and the figures behind it when asked."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spark", action="store_true", help="print Spark's job heap instead of DuckDB's limit")
    parser.add_argument("--explain", action="store_true", help="also print the figures the limit comes from")
    args = parser.parse_args()
    try:
        budget = current()
    except BudgetError as error:
        sys.stderr.write(f"memory budget: {error}\n")
        return 1
    if args.explain:
        sys.stderr.write(json.dumps(budget.record()) + "\n")
    sys.stdout.write((budget.spark_setting if args.spark else budget.setting) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
