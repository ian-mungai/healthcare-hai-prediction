"""Log the peak memory and peak scratch use of a job container in one line when the job exits.

Every ECS job entry point runs its ``main`` through ``recorded``. The record is one stderr line, ``event=resource_peaks {json}``, which the task's log
driver keeps in CloudWatch Logs. It costs no metric: Container Insights stays off.

Memory comes from the container's own cgroup counter when the kernel exposes it (cgroup v2 ``memory.peak``, then cgroup v1
``memory.max_usage_in_bytes``), and otherwise from the largest resident set of this process or of a finished child (``getrusage``). The resident set
misses page cache and a child that is still running, so the record names its source.

Scratch use is the allocated size of the job's work and temporary folders, walked by a background thread every ``interval`` seconds and once more at
exit. A peak that rises and falls between two walks is missed, so the record states the measure and the number of walks.

The recording never changes the job's result: a counter or walk that fails records null and a reason, and the job's exit code or exception passes on.
"""

from __future__ import annotations

import json
import os
import resource
import sys
import tempfile
import threading
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, NoReturn

CGROUP_V2_PEAK = Path("/sys/fs/cgroup/memory.peak")
CGROUP_V1_PEAK = Path("/sys/fs/cgroup/memory/memory.max_usage_in_bytes")
SCRATCH = (Path("/workspace"), Path(tempfile.gettempdir()))
INTERVAL_SECONDS = 5.0
WALK_LIMIT = 500_000
SCRATCH_MEASURE = "sampled_folder_allocation"
getrusage = resource.getrusage


def folder_allocation(folders: Iterable[Path]) -> int:
    """Allocated bytes of every file below the folders, without following links, skipping unreadable folders; a walk past WALK_LIMIT entries is refused."""
    total, seen = 0, 0
    pending = [str(folder) for folder in folders if folder.is_dir()]
    while pending:
        try:
            listing = os.scandir(pending.pop())
        except (FileNotFoundError, PermissionError):
            # A folder another user owns (in a shared temporary folder) or one removed during the walk holds none of the job's files.
            continue
        with listing as entries:
            for entry in entries:
                seen += 1
                if seen > WALK_LIMIT:
                    raise OSError(f"more than {WALK_LIMIT} entries below the scratch folders")
                try:
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        total += entry.stat(follow_symlinks=False).st_blocks * 512
                except FileNotFoundError:
                    continue
    return total


class _Sampler(threading.Thread):
    """Walk the scratch folders until stopped and keep the largest allocation seen."""

    def __init__(self, folders: tuple[Path, ...], interval: float) -> None:
        super().__init__(name="resource_peaks", daemon=True)
        self.folders, self.interval = folders, interval
        self.stopped = threading.Event()
        self.peak: int | None = None
        self.samples = 0
        self.error = ""

    def sample(self) -> None:
        """Take one walk; a failed walk ends the measure with its reason."""
        if self.error:
            return
        try:
            size = folder_allocation(self.folders)
        except Exception as error:
            self.error = f"{type(error).__name__}: {error}"
            return
        self.samples += 1
        self.peak = size if self.peak is None else max(self.peak, size)

    def run(self) -> None:
        """Walk at the start and then once per interval."""
        self.sample()
        while not self.stopped.wait(self.interval):
            self.sample()


def peak_memory() -> tuple[int | None, str, str]:
    """The container's peak memory in bytes, the counter it came from and, when every counter failed, why."""
    reasons = []
    for path, source in ((CGROUP_V2_PEAK, "cgroup_v2_memory_peak"), (CGROUP_V1_PEAK, "cgroup_v1_max_usage")):
        try:
            return int(path.read_text(encoding="ascii").strip()), source, ""
        except (OSError, ValueError) as error:
            reasons.append(f"{path}: {type(error).__name__}")
    try:
        largest = max(getrusage(resource.RUSAGE_SELF).ru_maxrss, getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
    except (OSError, ValueError) as error:
        reasons.append(f"getrusage: {type(error).__name__}: {error}")
        return None, "unavailable", "; ".join(reasons)
    # Linux reports ru_maxrss in KiB and macOS in bytes.
    return int(largest) * (1 if sys.platform == "darwin" else 1024), "rusage_max_rss", ""


def _exit_code(value: object) -> int:
    if value is None:
        return 0
    return value if isinstance(value, int) else 1


def _emit(sampler: _Sampler, exit_code: int) -> None:
    try:
        sampler.stopped.set()
        sampler.join(timeout=sampler.interval + 30)
        if not sampler.is_alive():
            sampler.sample()
        memory, source, reason = peak_memory()
        record: dict[str, Any] = {
            "exit_code": exit_code,
            "peak_memory_bytes": memory,
            "peak_memory_reason": reason,
            "peak_memory_source": source,
            "peak_scratch_bytes": None if sampler.error else sampler.peak,
            "scratch_measure": f"unavailable: {sampler.error}" if sampler.error else SCRATCH_MEASURE,
            "scratch_samples": sampler.samples,
        }
        sys.stderr.write("event=resource_peaks " + json.dumps(record, sort_keys=True) + "\n")
        sys.stderr.flush()
    except Exception as error:
        sys.stderr.write(f"event=resource_peaks_failed {type(error).__name__}: {error}\n")


def recorded(main: Callable[[], object], *, scratch: tuple[Path, ...] = SCRATCH, interval: float = INTERVAL_SECONDS) -> NoReturn:
    """Run a job entry point, log its resource peaks once and exit with the job's own code or exception."""
    sampler = _Sampler(scratch, interval)
    sampler.start()
    code = 1
    try:
        code = _exit_code(main())
    except SystemExit as stop:
        code = _exit_code(stop.code)
        raise
    finally:
        _emit(sampler, code)
    raise SystemExit(code)
