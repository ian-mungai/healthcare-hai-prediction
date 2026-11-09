"""Derive the engine limits of an AWS task from the capacity its container has at run time.

A task definition names its size (CPU units and memory) and nothing about the engines inside it. Each job entry point calls ``apply`` before an engine starts,
and ``apply`` sets ``DUCKDB_MEMORY_LIMIT``, ``SPARK_JOB_MEMORY``, ``JOB_THREADS`` (with the OpenMP, OpenBLAS and MKL counts) and, when the task names a spill
folder, ``HAI_DUCKDB_MAX_TEMP_BYTES`` from what it reads:

1. Memory is the smallest finite figure among the cgroup limit (v2 ``memory.max``, v1 ``memory.limit_in_bytes``), the memory of the task metadata and
   ``MemTotal``. CPUs are the smallest among the cgroup quota (v2 ``cpu.max``, v1 ``cfs_quota_us``), the cpuset, the scheduler affinity and the CPU of the task
   metadata. A source that holds no limit is skipped, and a task with no readable source stops by name.
2. The memory left for an engine is the memory less a headroom (6% of it, at least 1 GiB, for the driver, dbt and the page cache) less a reserve for each other
   container that runs at the same time (the guard of a layer task), read from the task metadata. DuckDB takes 80% of it, the share ``memory_budget`` uses on a
   Mac. Spark gets the whole of it as heap less its own overhead. Less than 2 GiB left stops the task by name.
3. Threads are the CPUs, and no more than the memory left holds at the memory a thread needs: 3.5 GiB, or 7 GiB for the features build, which ran out of
   memory at 4 threads on 16 GiB and ran 8 on 64 GiB.
4. The spill cap is 60% of the free space of the filesystem that holds the spill folder, read when the phase starts.

An explicit value in the environment (a launch override) is clipped to the derived one and never raised, so a small value stays exact. Outside an ECS task
nothing is read or set, because the launcher plans local jobs (``memory_budget``)."""

from __future__ import annotations

import http.client
import json
import os
import re
import shutil
import sys
import urllib.parse
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from scripts.lakehouse.memory_budget import BudgetError, parse_size, spark_heap

GIB = 1024**3
MIB = 1024**2
UNLIMITED = 1 << 60
HEADROOM_FLOOR = GIB
HEADROOM_PERCENT = 6
SIBLING_RESERVE = 256 * MIB
ENGINE_FLOOR = 2 * GIB
DUCKDB_SHARE_PERCENT = 80
THREAD_MEMORY = {"default": 7 * GIB // 2, "features_build": 7 * GIB}
SPILL_SHARE_PERCENT = 60
SPILL_FLOOR = GIB
METADATA_SECONDS = 5.0
THREAD_ALIASES = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")
OVERRIDES = ("DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "SPARK_JOB_MEMORY", "HAI_DUCKDB_MAX_TEMP_BYTES")
CGROUP = "sys/fs/cgroup"
SPARK_HEAP = re.compile(r"[1-9][0-9]*g")


class CapacityError(RuntimeError):
    """The capacity cannot be read, is below the floor, or an explicit override is not understood."""


@dataclass(frozen=True)
class Capacity:
    """What the container has: memory and CPUs with the source of each figure, the other running containers and the free bytes of the spill filesystem."""

    memory: int
    memory_source: str
    cpus: int
    cpu_source: str
    siblings: int
    sibling_source: str
    spill_free: int | None


@dataclass(frozen=True)
class Limits:
    """The engine settings derived from a capacity, with the figures they come from."""

    duckdb_memory: str
    spark_heap: str
    threads: int
    spill_bytes: int | None
    evidence: dict[str, Any]

    def environment(self) -> dict[str, str]:
        found = {"DUCKDB_MEMORY_LIMIT": self.duckdb_memory, "SPARK_JOB_MEMORY": self.spark_heap, "JOB_THREADS": str(self.threads)}
        if self.spill_bytes is not None:
            found["HAI_DUCKDB_MAX_TEMP_BYTES"] = str(self.spill_bytes)
        return found


def read_text(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def whole(path: Path, text: str) -> int:
    if not re.fullmatch(r"-?[0-9]+", text):
        raise CapacityError(f"{path} holds {text!r}, not a whole number")
    return int(text)


def cgroup_file(root: Path, *names: str) -> tuple[Path, str] | None:
    for name in names:
        path = root / CGROUP / name
        text = read_text(path)
        if text is not None:
            return path, text
    return None


def metadata_limit(metadata: dict[str, Any] | None, key: str) -> float | None:
    found = ((metadata or {}).get("task") or {}).get("Limits", {}).get(key)
    return float(found) if isinstance(found, int | float) and found > 0 else None


def read_memory(root: Path, metadata: dict[str, Any] | None) -> tuple[int, str]:
    """The smallest finite memory figure among the cgroup limit, the task metadata and MemTotal, with its source."""
    found: list[tuple[int, str]] = []
    limit = cgroup_file(root, "memory.max", "memory/memory.limit_in_bytes")
    if limit is not None and limit[1] != "max" and whole(*limit) < UNLIMITED:
        found.append((whole(*limit), f"cgroup {limit[0].name}"))
    task = metadata_limit(metadata, "Memory")
    if task is not None:
        found.append((int(task * MIB), "task metadata Limits.Memory"))
    total = read_text(root / "proc/meminfo")
    match = re.search(r"^MemTotal:\s+([0-9]+) kB", total or "", re.MULTILINE)
    if match:
        found.append((int(match.group(1)) * 1024, "MemTotal"))
    if not found:
        raise CapacityError("the memory of this container cannot be read: no finite cgroup limit, task metadata limit or MemTotal")
    return min(found)


def cpu_list(path: Path, text: str) -> int:
    count = 0
    for part in text.split(","):
        low, _, high = part.strip().partition("-")
        if not low.isdecimal() or (high and not high.isdecimal()):
            raise CapacityError(f"{path} holds {text!r}, not a list of CPUs")
        count += int(high or low) - int(low) + 1
    return count


def read_cpus(root: Path, affinity: int | None, metadata: dict[str, Any] | None) -> tuple[int, str]:
    """The smallest CPU count among the cgroup quota, the cpuset, the scheduler affinity and the task metadata, with its source."""
    found: list[tuple[int, str]] = []
    quota = cgroup_file(root, "cpu.max")
    if quota is not None:
        figure, _, period = quota[1].partition(" ")
        if figure != "max":
            found.append((max(1, whole(quota[0], figure) // max(1, whole(quota[0], period))), "cgroup cpu.max"))
    else:
        v1 = cgroup_file(root, "cpu/cpu.cfs_quota_us", "cpu,cpuacct/cpu.cfs_quota_us")
        period_v1 = cgroup_file(root, "cpu/cpu.cfs_period_us", "cpu,cpuacct/cpu.cfs_period_us")
        if v1 is not None and period_v1 is not None and whole(*v1) > 0:
            found.append((max(1, whole(*v1) // max(1, whole(*period_v1))), "cgroup cfs_quota_us"))
    cpuset = cgroup_file(root, "cpuset.cpus.effective", "cpuset/cpuset.cpus", "cpuset.cpus")
    if cpuset is not None and cpuset[1]:
        found.append((cpu_list(*cpuset), f"cgroup {cpuset[0].name} (cpuset)"))
    if affinity is not None:
        found.append((affinity, "scheduler affinity"))
    task = metadata_limit(metadata, "CPU")
    if task is not None:
        found.append((max(1, int(task)), "task metadata Limits.CPU"))
    if not found:
        raise CapacityError("the CPU count of this container cannot be read: no cgroup quota or cpuset, affinity or task metadata limit")
    return min(found)


def read_siblings(metadata: dict[str, Any] | None) -> tuple[int, str]:
    """The containers of the task that run beside this one. Without the task metadata one is assumed, the guard of a layer task."""
    if metadata is None:
        return 1, "task metadata unreadable, so one sibling is reserved"
    me = (metadata.get("container") or {}).get("Name")
    running = [item for item in (metadata.get("task") or {}).get("Containers", []) if item.get("KnownStatus") == "RUNNING" and item.get("Name") != me]
    return len(running), f"task metadata: {len(running)} other running containers"


def nearest_existing(path: Path) -> Path:
    while not path.exists() and path != path.parent:
        path = path.parent
    return path


def read_capacity(
    root: Path, spill_path: Path | None, metadata: dict[str, Any] | None, affinity: int | None, disk_free: Callable[[Path], int] | None
) -> Capacity:
    """Read the memory, CPUs, siblings and spill space of this container. ``root`` is the filesystem root (a test points it at fake cgroup files)."""
    memory, memory_source = read_memory(root, metadata)
    cpus, cpu_source = read_cpus(root, affinity, metadata)
    siblings, sibling_source = read_siblings(metadata)
    free = None
    if spill_path is not None:
        place = nearest_existing(spill_path)
        free = disk_free(place) if disk_free is not None else shutil.disk_usage(place).free
    return Capacity(memory, memory_source, cpus, cpu_source, siblings, sibling_source, free)


def derive(capacity: Capacity, profile: str = "default") -> Limits:
    """The DuckDB memory, Spark heap, threads and spill cap of a capacity. A capacity below the floor stops by name with every figure."""
    if profile not in THREAD_MEMORY:
        raise CapacityError(f"unknown capacity profile {profile!r}")
    headroom = max(HEADROOM_FLOOR, capacity.memory * HEADROOM_PERCENT // 100)
    reserve = capacity.siblings * SIBLING_RESERVE
    available = capacity.memory - headroom - reserve
    if available < ENGINE_FLOOR:
        raise CapacityError(
            f"only {available / GIB:.2f} GiB is left for the engines, below the floor of {ENGINE_FLOOR / GIB:.0f} GiB: memory {capacity.memory / GIB:.2f} GiB "
            f"({capacity.memory_source}), headroom {headroom / GIB:.2f} GiB, {capacity.siblings} other containers at {SIBLING_RESERVE // MIB} MiB"
        )
    spill = None
    if capacity.spill_free is not None:
        spill = capacity.spill_free * SPILL_SHARE_PERCENT // 100
        if spill < SPILL_FLOOR:
            raise CapacityError(
                f"only {capacity.spill_free / GIB:.2f} GiB is free for the spill, and its cap would be below the floor of {SPILL_FLOOR / GIB:.0f} GiB"
            )
    threads = max(1, min(capacity.cpus, available // THREAD_MEMORY[profile]))
    evidence = {
        "profile": profile,
        "memory": capacity.memory,
        "memory_source": capacity.memory_source,
        "headroom": headroom,
        "siblings": capacity.siblings,
        "sibling_reserve": reserve,
        "sibling_source": capacity.sibling_source,
        "available": available,
        "cpus": capacity.cpus,
        "cpu_source": capacity.cpu_source,
        "thread_memory": THREAD_MEMORY[profile],
        "spill_free": capacity.spill_free,
    }
    return Limits(f"{available * DUCKDB_SHARE_PERCENT // 100 // GIB}GiB", spark_heap(available), threads, spill, evidence)


def clip(environ: Mapping[str, str], limits: Limits) -> Limits:
    """Apply the explicit values of ``environ`` to derived limits: an explicit value above its derived one is clipped to it, a smaller one stays as it is."""
    clipped: list[str] = []
    memory, heap, threads, spill = limits.duckdb_memory, limits.spark_heap, limits.threads, limits.spill_bytes
    if environ.get("DUCKDB_MEMORY_LIMIT"):
        given = environ["DUCKDB_MEMORY_LIMIT"]
        try:
            larger = parse_size(given) > parse_size(limits.duckdb_memory)
        except BudgetError as error:
            raise CapacityError(f"DUCKDB_MEMORY_LIMIT is {given!r}, not a size such as 150MB or 12GiB") from error
        memory = limits.duckdb_memory if larger else given
        if larger:
            clipped.append("DUCKDB_MEMORY_LIMIT")
    if environ.get("JOB_THREADS"):
        if not environ["JOB_THREADS"].isdecimal() or int(environ["JOB_THREADS"]) < 1:
            raise CapacityError(f"JOB_THREADS is {environ['JOB_THREADS']!r}, not a positive whole number")
        threads = min(int(environ["JOB_THREADS"]), limits.threads)
        if int(environ["JOB_THREADS"]) > limits.threads:
            clipped.append("JOB_THREADS")
    if environ.get("SPARK_JOB_MEMORY"):
        if not SPARK_HEAP.fullmatch(environ["SPARK_JOB_MEMORY"]):
            raise CapacityError(f"SPARK_JOB_MEMORY is {environ['SPARK_JOB_MEMORY']!r}, not a whole number of GiB such as 20g")
        if int(environ["SPARK_JOB_MEMORY"][:-1]) > int(limits.spark_heap[:-1]):
            clipped.append("SPARK_JOB_MEMORY")
        else:
            heap = environ["SPARK_JOB_MEMORY"]
    if environ.get("HAI_DUCKDB_MAX_TEMP_BYTES"):
        if not environ["HAI_DUCKDB_MAX_TEMP_BYTES"].isdecimal() or int(environ["HAI_DUCKDB_MAX_TEMP_BYTES"]) < 1:
            raise CapacityError(f"HAI_DUCKDB_MAX_TEMP_BYTES is {environ['HAI_DUCKDB_MAX_TEMP_BYTES']!r}, not a positive whole number of bytes")
        given_bytes = int(environ["HAI_DUCKDB_MAX_TEMP_BYTES"])
        if limits.spill_bytes is not None and given_bytes > limits.spill_bytes:
            clipped.append("HAI_DUCKDB_MAX_TEMP_BYTES")
        else:
            spill = given_bytes
    evidence = {**limits.evidence, "clipped": clipped} if clipped else limits.evidence
    return replace(limits, duckdb_memory=memory, spark_heap=heap, threads=threads, spill_bytes=spill, evidence=evidence)


def fetch_metadata(environ: Mapping[str, str]) -> dict[str, Any] | None:
    """The container and task documents of the version 4 metadata endpoint, or None when it is not set or does not answer within a few seconds."""
    address = urllib.parse.urlsplit(environ.get("ECS_CONTAINER_METADATA_URI_V4", ""))
    if not address.hostname:
        return None
    documents = []
    for suffix in ("", "/task"):
        connection = http.client.HTTPConnection(address.hostname, address.port or 80, timeout=METADATA_SECONDS)
        try:
            connection.request("GET", address.path.rstrip("/") + suffix)
            reply = connection.getresponse()
            if reply.status != 200:
                return None
            documents.append(json.loads(reply.read()))
        except (OSError, ValueError, http.client.HTTPException):
            return None
        finally:
            connection.close()
    return {"container": documents[0], "task": documents[1]}


def current_affinity() -> int | None:
    found = getattr(os, "sched_getaffinity", None)
    return len(found(0)) if found else None


def apply(
    profile: str = "default",
    environ: MutableMapping[str, str] | None = None,
    root: Path = Path("/"),
    metadata: dict[str, Any] | None = None,
    affinity: int | None = None,
    disk_free: Callable[[Path], int] | None = None,
) -> Limits | None:
    """Derive the engine limits of this task and set them in ``environ`` (the process environment by default) before an engine starts.

    Outside an ECS task nothing is read or set and the result is None. The derivation is written to standard error, so the task log shows the figures.
    ``metadata`` and ``affinity`` are read from the task and the process when they are None."""
    target = os.environ if environ is None else environ
    if not target.get("AWS_EXECUTION_ENV", "").startswith("AWS_ECS"):
        return None
    spill = Path(target["HAI_DUCKDB_TEMP_DIRECTORY"]) if target.get("HAI_DUCKDB_TEMP_DIRECTORY") else None
    capacity = read_capacity(
        root, spill, metadata if metadata is not None else fetch_metadata(target), affinity if affinity is not None else current_affinity(), disk_free
    )
    limits = clip(target, derive(capacity, profile))
    target.update(limits.environment())
    for name in THREAD_ALIASES:
        existing = target.get(name, "")
        target[name] = str(min(int(existing), limits.threads)) if existing.isdecimal() and int(existing) >= 1 else str(limits.threads)
    sys.stderr.write("task_capacity: " + json.dumps({"limits": limits.environment(), "evidence": limits.evidence}, sort_keys=True) + "\n")
    return limits
