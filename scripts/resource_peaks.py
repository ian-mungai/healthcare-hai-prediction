"""Log the peak memory and peak scratch use of a job container in one line when the job exits.

Every ECS job entry point runs its ``main`` through ``recorded``. The record is one stderr line, ``event=resource_peaks {json}``, which the task's log
driver keeps in CloudWatch Logs. It costs no metric: Container Insights stays off.

Memory comes from the container's own cgroup counter when the kernel exposes it (cgroup v2 ``memory.peak``, then cgroup v1
``memory.max_usage_in_bytes``), and otherwise from the largest resident set of this process or of a finished child (``getrusage``). The resident set
misses page cache and a child that is still running, so the record names its source.

Scratch use is the allocated size of the job's work and temporary folders, walked by a background thread every ``interval`` seconds and once more at
exit. A peak that rises and falls between two walks is missed, so the record states the measure and the number of walks.

The recording never changes the job's result: a counter, walk or sampler thread that fails (built, started or walked) records null and a reason, and
a stderr that cannot be written or an interrupt inside the recorder loses only the record. The job's exit code or exception, its own interrupt
included, passes on.
"""

from __future__ import annotations

import contextlib
import json
import os
import resource
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, NoReturn, TextIO, cast

from scripts import metrics_push, phase_failure

CGROUP_V2_PEAK = Path("/sys/fs/cgroup/memory.peak")
CGROUP_V1_PEAK = Path("/sys/fs/cgroup/memory/memory.max_usage_in_bytes")
CGROUP_V2_CURRENT = Path("/sys/fs/cgroup/memory.current")
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


def _emit(sampler: _Sampler | None, unavailable: str, exit_code: int, after: Callable[[dict[str, Any]], None] | None = None) -> None:
    """Write the one record; any failure here, an interrupt included, ends the record and never the job's result."""
    try:
        if sampler is not None:
            sampler.stopped.set()
            if sampler.ident is not None:
                sampler.join(timeout=sampler.interval + 30)
            if not sampler.is_alive():
                sampler.sample()
            unavailable = sampler.error
        memory, source, reason = peak_memory()
        record: dict[str, Any] = {
            "exit_code": exit_code,
            "peak_memory_bytes": memory,
            "peak_memory_reason": reason,
            "peak_memory_source": source,
            "peak_scratch_bytes": None if unavailable or sampler is None else sampler.peak,
            "scratch_measure": f"unavailable: {unavailable}" if unavailable else SCRATCH_MEASURE,
            "scratch_samples": 0 if sampler is None else sampler.samples,
        }
        sys.stderr.write("event=resource_peaks " + json.dumps(record, sort_keys=True) + "\n")
        sys.stderr.flush()
        if after is not None:
            after(record)
    except BaseException as error:
        # The recorder's own failure, an interrupt during the join or a closed stderr, loses only the record; the job's result still passes on.
        # After an interrupt the record is abandoned with no further write: stderr may be a full pipe whose reader stopped, and a write would block.
        if isinstance(error, KeyboardInterrupt):
            return
        with contextlib.suppress(BaseException):
            sys.stderr.write(f"event=resource_peaks_failed {type(error).__name__}: {error}\n")


def _start(folders: tuple[Path, ...], interval: float) -> tuple[_Sampler | None, str, bool]:
    """Build and start the scratch sampler; a sampler that cannot be built or started leaves the measure unavailable.

    The third value is True after an interrupt here: the record is then abandoned, with no stderr write that could block on a full pipe.
    """
    try:
        sampler = _Sampler(folders, interval)
    except BaseException as error:
        return None, f"sampler not built: {type(error).__name__}: {error}", isinstance(error, KeyboardInterrupt)
    try:
        sampler.start()
    except BaseException as error:
        sampler.error = f"sampler did not start: {type(error).__name__}: {error}"
        return sampler, "", isinstance(error, KeyboardInterrupt)
    return sampler, "", False


def _current_memory() -> int | None:
    try:
        return int(CGROUP_V2_CURRENT.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return None


def _readings(sampler: _Sampler | None) -> dict[str, float | None]:
    """What a heartbeat reports: the container's memory now and at its peak and the scratch peak so far."""
    peak = peak_memory()[0]
    return {"memory_current": _current_memory(), "memory_peak": peak, "scratch_peak": None if sampler is None else sampler.peak}


def _pusher() -> metrics_push.Pusher | None:
    """The metrics pusher of this process, or None without an address; any failure to build it is written and the job runs on."""
    try:
        return metrics_push.Pusher.create(os.environ, sys.argv)
    except Exception as error:
        metrics_push.line("metrics_push_skipped", reason=f"{type(error).__name__}: {error}")
        return None


def _exit_push(
    pusher: metrics_push.Pusher | None, heartbeat: metrics_push.Heartbeat | None, started: float, interrupted: bool
) -> Callable[[dict[str, Any]], None] | None:
    """The call that ends the pushes after the record is written: stop the heartbeat, then push the outcome. It never raises."""
    if pusher is None:
        return None

    def push(record: dict[str, Any]) -> None:
        try:
            if heartbeat is not None:
                heartbeat.stopped.set()
            outcome = "interrupted" if interrupted else ("succeeded" if record["exit_code"] == 0 else "failed")
            pusher.finish(
                outcome,
                exit_code=record["exit_code"],
                duration=time.monotonic() - started,
                peak_memory=record["peak_memory_bytes"],
                peak_scratch=record["peak_scratch_bytes"],
                samples=metrics_push.drain(),
            )
        except Exception as error:
            metrics_push.line("metrics_push_failed", step="put", reason=f"{type(error).__name__}: {error}")

    return push


def _watch_stderr() -> phase_failure.StderrTail | None:
    """Keep the last line of standard error while the job runs. Anything that goes wrong leaves the stream as it was."""
    try:
        tail = phase_failure.StderrTail(sys.stderr)
        sys.stderr = cast(TextIO, tail)
        return tail
    except Exception:
        return None


def _release_stderr(tail: phase_failure.StderrTail | None) -> None:
    if tail is not None:
        sys.stderr = tail.stream
        tail.finish()


def _write_failure(tail: phase_failure.StderrTail | None, failure: str) -> None:
    """The failure line of a failed job that wrote none: its last stderr line (or the type of its exception), for the launcher to keep in the end record."""
    try:
        if tail is None or tail.marked:
            return
        phase_failure.emit(phase_failure.phase_of(sys.argv), failure or tail.last or "the job ended with a failure and wrote no line")
    except Exception as error:
        with contextlib.suppress(BaseException):
            sys.stderr.write(f"event=phase_failure_failed {type(error).__name__}\n")


def recorded(main: Callable[[], object], *, scratch: tuple[Path, ...] = SCRATCH, interval: float = INTERVAL_SECONDS) -> NoReturn:
    """Run a job entry point, log its resource peaks once, push its metrics when an address is set and exit with the job's own code or exception."""
    sampler, unavailable, abandoned = _start(scratch, interval)
    pusher = _pusher()
    heartbeat = None
    if pusher is not None and not abandoned:
        heartbeat = metrics_push.Heartbeat(pusher, lambda: _readings(sampler))
        try:
            heartbeat.start()
        except Exception as error:
            heartbeat = None
            metrics_push.line("metrics_push_failed", step="put", reason=f"{type(error).__name__}: {error}")
    started = time.monotonic()
    interrupted = False
    code = 1
    failure = ""
    tail = _watch_stderr()
    try:
        code = _exit_code(main())
    except KeyboardInterrupt:
        interrupted = True
        raise
    except SystemExit as stop:
        code = _exit_code(stop.code)
        raise
    except Exception as error:
        failure = type(error).__name__  # the type alone: the message of an unexpected error may carry anything
        raise
    finally:
        _release_stderr(tail)
        if abandoned:
            if sampler is not None:
                sampler.stopped.set()
        else:
            _emit(sampler, unavailable, code, _exit_push(pusher, heartbeat, started, interrupted))
        if code != 0 and not interrupted:
            _write_failure(tail, failure)
    raise SystemExit(code)
