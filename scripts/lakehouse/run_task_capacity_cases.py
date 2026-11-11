"""Cases of the engine limits that an AWS task derives from the capacity its container has at run time, with fake cgroup files and no engine.

    .venv/bin/python -m scripts.lakehouse.run_task_capacity_cases

The cases cover five things. The reader (cgroup v1 and v2 files, an unlimited cgroup, a tiny one, no readable source, the CPU quota and the cpuset). The
derived DuckDB memory, Spark heap, threads and spill cap, with the exact figures of the 16 GiB and 64 GiB tasks. The clipping of an explicit override.
The entry points that call the reader before an engine starts. And the deadline of the features build, derived from what is left of the task deadline."""

from __future__ import annotations

import ast
import contextlib
import io
import os
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from unittest import mock

from orchestration.pipeline_protocol import PipelineError
from scripts.features import features_build
from scripts.lakehouse import ecs_engine, layer_run, task_capacity

ROOT = Path(__file__).resolve().parents[2]
GIB = 1024**3
MIB = 1024**2
# Each AWS job entry point and the first call of its main that starts work with an engine: the limits are derived before it.
ENTRY_POINTS = {
    "scripts/lakehouse/layer_run.py": "dispatch",
    "scripts/lakehouse/bronze_run.py": "dispatch",
    "scripts/training/aws_run.py": "handler",
    "monitoring/aws_run.py": "deadline",
    "scripts/lakehouse/aws_bronze.py": "execute",
}
GUARD = {"Name": "guard", "KnownStatus": "RUNNING"}
PHASE = {"Name": "build", "KnownStatus": "RUNNING"}
PENDING = {"Name": "publish", "KnownStatus": "PENDING"}


def expect(condition: object, message: object = "") -> None:
    if not condition:
        raise AssertionError(message)


def write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def meminfo(root: Path, total_kib: int) -> None:
    write(root, "proc/meminfo", f"MemTotal:       {total_kib} kB\nMemFree:        1 kB\n")


def v2(root: Path, memory: str, cpu: str, cpuset: str) -> None:
    write(root, "sys/fs/cgroup/memory.max", memory + "\n")
    write(root, "sys/fs/cgroup/cpu.max", cpu + "\n")
    write(root, "sys/fs/cgroup/cpuset.cpus.effective", cpuset + "\n")


def v1(root: Path, memory: str, quota: str, cpuset: str) -> None:
    write(root, "sys/fs/cgroup/memory/memory.limit_in_bytes", memory + "\n")
    write(root, "sys/fs/cgroup/cpu/cpu.cfs_quota_us", quota + "\n")
    write(root, "sys/fs/cgroup/cpu/cpu.cfs_period_us", "100000\n")
    write(root, "sys/fs/cgroup/cpuset/cpuset.cpus", cpuset + "\n")


def task(memory_mib: int, cpu: float, containers: list[dict[str, str]]) -> dict[str, object]:
    return {"container": {"Name": "build"}, "task": {"Limits": {"CPU": cpu, "Memory": memory_mib}, "Containers": containers}}


def capacity(memory: int, cpus: int, siblings: int = 1, spill: int | None = 100 * GIB) -> task_capacity.Capacity:
    return task_capacity.Capacity(memory, "case", cpus, "case", siblings, "case", spill)


def expect_error(call: Callable[[], object], text: str) -> None:
    try:
        call()
    except (task_capacity.CapacityError, PipelineError, layer_run.StepError) as error:
        expect(text in str(error), f"the error says {str(error)!r}, not {text!r}")
        return
    raise AssertionError(f"no error that says {text!r}")


def read(root: Path, metadata: dict[str, object] | None = None, affinity: int | None = None) -> tuple[int, str, int, str]:
    memory, memory_source = task_capacity.read_memory(root, metadata)
    cpus, cpu_source = task_capacity.read_cpus(root, affinity, metadata)
    return memory, memory_source, cpus, cpu_source


def case_cgroup_v2_limits(root: Path) -> None:
    v2(root, str(64 * GIB), "1600000 100000", "0-15")
    memory, memory_source, cpus, cpu_source = read(root)
    expect(memory == 64 * GIB and "memory.max" in memory_source, (memory, memory_source))
    expect(cpus == 16 and "cpu.max" in cpu_source, (cpus, cpu_source))


def case_cgroup_v1_limits(root: Path) -> None:
    v1(root, str(16 * GIB), "400000", "0-3")
    memory, memory_source, cpus, cpu_source = read(root)
    expect(memory == 16 * GIB and "memory.limit_in_bytes" in memory_source, (memory, memory_source))
    expect(cpus == 4 and "cfs_quota_us" in cpu_source, (cpus, cpu_source))


def case_v2_unlimited_uses_the_smaller_of_meminfo_and_the_task_metadata(root: Path) -> None:
    v2(root, "max", "max 100000", "0-7")
    meminfo(root, 32 * GIB // 1024)
    memory, memory_source, cpus, cpu_source = read(root)
    expect(memory == 32 * GIB and "MemTotal" in memory_source, (memory, memory_source))
    expect(cpus == 8 and "cpuset" in cpu_source, (cpus, cpu_source))
    memory, memory_source, _, _ = read(root, task(16384, 4, []))
    expect(memory == 16 * GIB and "task metadata" in memory_source, (memory, memory_source))


def case_v1_unlimited_sentinel_is_not_a_limit(root: Path) -> None:
    v1(root, "9223372036854771712", "-1", "0-1")
    meminfo(root, 8 * GIB // 1024)
    memory, memory_source, cpus, _ = read(root)
    expect(memory == 8 * GIB and "MemTotal" in memory_source, (memory, memory_source))
    expect(cpus == 2, cpus)


def case_a_v1_sentinel_alone_is_no_memory_figure(root: Path) -> None:
    v1(root, "9223372036854771712", "-1", "0-1")
    expect_error(lambda: task_capacity.read_memory(root, None), "memory")


def case_the_smallest_cpu_source_wins(root: Path) -> None:
    v2(root, str(8 * GIB), "200000 100000", "0-7")
    expect(read(root)[2] == 2, "the quota of 2 CPUs is below the cpuset of 8")
    v2(root, str(8 * GIB), "800000 100000", "0-1,4")
    expect(read(root)[2] == 3, "the cpuset of 3 CPUs is below the quota of 8")
    expect(read(root, affinity=2)[2] == 2, "the scheduler affinity is a source too")
    v2(root, str(8 * GIB), "50000 100000", "0-7")
    expect(read(root)[2] == 1, "half a CPU of quota still runs one thread")
    expect(read(root, task(8192, 1.0, []))[2] == 1, "the task metadata limit is a source too")


def case_nothing_readable_fails_by_name(root: Path) -> None:
    expect_error(lambda: task_capacity.read_memory(root, None), "memory")
    expect_error(lambda: task_capacity.read_cpus(root, None, None), "CPU")


def case_garbled_cgroup_file_fails_by_name(root: Path) -> None:
    v2(root, "plenty", "1600000 100000", "0-15")
    expect_error(lambda: task_capacity.read_memory(root, None), "memory.max")


def case_siblings_come_from_the_task_metadata(root: Path) -> None:
    v2(root, str(16 * GIB), "400000 100000", "0-3")
    one = task_capacity.read_capacity(root, None, task(16384, 4, [GUARD, PHASE, PENDING]), None, None)
    expect(one.siblings == 1, one)
    two = task_capacity.read_capacity(root, None, task(16384, 4, [GUARD, PHASE, {"Name": "log_router", "KnownStatus": "RUNNING"}]), None, None)
    expect(two.siblings == 2, two)
    none = task_capacity.read_capacity(root, None, task(16384, 4, [PHASE]), None, None)
    expect(none.siblings == 0, none)


def case_unreadable_metadata_reserves_for_one_sibling(root: Path) -> None:
    v2(root, str(16 * GIB), "400000 100000", "0-3")
    found = task_capacity.read_capacity(root, None, None, None, None)
    expect(found.siblings == 1 and "unreadable" in found.sibling_source, found)


def case_spill_space_comes_from_the_filesystem(root: Path) -> None:
    v2(root, str(16 * GIB), "400000 100000", "0-3")
    (root / "workspace").mkdir()
    spill = root / "workspace/duckdb_spill"
    found = task_capacity.read_capacity(root, spill, task(16384, 4, [PHASE]), None, lambda path: 90 * GIB if path == root / "workspace" else -1)
    expect(found.spill_free == 90 * GIB, found)


def case_the_16_gib_task(root: Path) -> None:
    default = task_capacity.derive(capacity(16 * GIB, 4))
    headroom, sibling = GIB, 256 * MIB
    available = 16 * GIB - headroom - sibling
    expect(default.evidence["available"] == available, default.evidence)
    expect(default.duckdb_memory == f"{available * 80 // 100 // GIB}GiB" == "11GiB", default.duckdb_memory)
    expect(default.spark_heap == "13g", default.spark_heap)
    expect(default.threads == 4, default.threads)
    expect(
        task_capacity.derive(capacity(16 * GIB, 4), "features_build").threads == 2,
        "the features build holds 7 GiB for each thread, as the proven 2 threads on 16 GiB",
    )


def case_the_64_gib_task(root: Path) -> None:
    headroom, sibling = 64 * GIB * 6 // 100, 256 * MIB
    available = 64 * GIB - headroom - sibling
    build = task_capacity.derive(capacity(64 * GIB, 16), "features_build")
    expect(build.evidence["available"] == available and build.evidence["headroom"] == headroom, build.evidence)
    expect(build.duckdb_memory == f"{available * 80 // 100 // GIB}GiB" == "47GiB", build.duckdb_memory)
    expect(build.spark_heap == "54g", build.spark_heap)
    expect(build.threads == 8, "the proven features retry ran 8 build threads on 16 vCPU and 64 GiB")
    expect(task_capacity.derive(capacity(64 * GIB, 16)).threads == 16, "another phase may use every CPU")
    expect(task_capacity.derive(capacity(64 * GIB, 2), "features_build").threads == 2, "the CPUs bound the threads too")


def case_more_siblings_leave_less_memory(root: Path) -> None:
    alone = task_capacity.derive(capacity(16 * GIB, 4, siblings=0))
    beside = task_capacity.derive(capacity(16 * GIB, 4, siblings=3))
    expect(alone.evidence["available"] - beside.evidence["available"] == 3 * 256 * MIB, (alone.evidence, beside.evidence))


def case_spill_cap_is_a_share_of_the_free_space(root: Path) -> None:
    expect(task_capacity.derive(capacity(16 * GIB, 4, spill=100 * GIB)).spill_bytes == 60 * GIB)
    expect(task_capacity.derive(capacity(16 * GIB, 4, spill=None)).spill_bytes is None)
    expect_error(lambda: task_capacity.derive(capacity(16 * GIB, 4, spill=GIB)), "spill")


def case_below_the_floor_fails_by_name(root: Path) -> None:
    expect_error(lambda: task_capacity.derive(capacity(GIB, 1)), "floor")
    expect_error(lambda: task_capacity.derive(capacity(2 * GIB, 1)), "floor")
    expect_error(lambda: task_capacity.derive(capacity(16 * GIB, 4, siblings=60)), "floor")
    expect(task_capacity.derive(capacity(4 * GIB, 1)).threads == 1)


def case_a_small_override_stays_exact(root: Path) -> None:
    derived = task_capacity.derive(capacity(16 * GIB, 4))
    environ = {"DUCKDB_MEMORY_LIMIT": "150MB", "JOB_THREADS": "2", "SPARK_JOB_MEMORY": "2g", "HAI_DUCKDB_MAX_TEMP_BYTES": str(3 * MIB)}
    clipped = task_capacity.clip(environ, derived)
    expect(clipped.duckdb_memory == "150MB" and clipped.threads == 2 and clipped.spark_heap == "2g" and clipped.spill_bytes == 3 * MIB, clipped)


def case_a_large_override_is_clipped_to_capacity(root: Path) -> None:
    derived = task_capacity.derive(capacity(16 * GIB, 4))
    environ = {"DUCKDB_MEMORY_LIMIT": "99GB", "JOB_THREADS": "64", "SPARK_JOB_MEMORY": "100g", "HAI_DUCKDB_MAX_TEMP_BYTES": str(500 * GIB)}
    clipped = task_capacity.clip(environ, derived)
    expect((clipped.duckdb_memory, clipped.threads, clipped.spark_heap, clipped.spill_bytes) == ("11GiB", 4, "13g", 60 * GIB), clipped)
    expect(clipped.evidence["clipped"] == ["DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "SPARK_JOB_MEMORY", "HAI_DUCKDB_MAX_TEMP_BYTES"], clipped.evidence)


def case_no_override_changes_nothing(root: Path) -> None:
    derived = task_capacity.derive(capacity(16 * GIB, 4))
    expect(task_capacity.clip({}, derived) == derived)


def case_an_override_that_is_not_understood_fails_by_name(root: Path) -> None:
    derived = task_capacity.derive(capacity(16 * GIB, 4))
    expect_error(lambda: task_capacity.clip({"DUCKDB_MEMORY_LIMIT": "lots"}, derived), "DUCKDB_MEMORY_LIMIT")
    expect_error(lambda: task_capacity.clip({"JOB_THREADS": "0"}, derived), "JOB_THREADS")
    expect_error(lambda: task_capacity.clip({"SPARK_JOB_MEMORY": "12"}, derived), "SPARK_JOB_MEMORY")
    expect_error(lambda: task_capacity.clip({"HAI_DUCKDB_MAX_TEMP_BYTES": "-1"}, derived), "HAI_DUCKDB_MAX_TEMP_BYTES")


def task_environment(spill: Path | None, execution: str = "AWS_ECS_FARGATE") -> dict[str, str]:
    found = {"AWS_EXECUTION_ENV": execution}
    if spill is not None:
        found["HAI_DUCKDB_TEMP_DIRECTORY"] = str(spill)
    return found


def case_apply_sets_the_engine_environment_in_a_task(root: Path) -> None:
    v2(root, str(64 * GIB), "1600000 100000", "0-15")
    spill = root / "workspace/duckdb_spill"
    environ = task_environment(spill) | {"OMP_NUM_THREADS": "99"}
    limits = task_capacity.apply("features_build", environ, root, task(65536, 16, [GUARD, PHASE]), 1024, lambda path: 100 * GIB)
    if limits is None:
        raise AssertionError("a task derives its limits")
    expect(limits.threads == 8, limits)
    expect((environ["JOB_THREADS"], environ["DUCKDB_MEMORY_LIMIT"], environ["SPARK_JOB_MEMORY"]) == ("8", limits.duckdb_memory, limits.spark_heap), environ)
    expect(environ["HAI_DUCKDB_MAX_TEMP_BYTES"] == str(60 * GIB), environ)
    expect(all(environ[name] == "8" for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")), environ)
    with mock.patch.dict(os.environ, environ, clear=True):
        found = ecs_engine.duckdb_temp_settings()
    expect(found == {"temp_directory": str(spill), "max_temp_directory_size": f"{60 * GIB}B"}, found)


def case_apply_derives_the_limits_of_a_task_on_the_ec2_host(root: Path) -> None:
    v2(root, str(64 * GIB), "1600000 100000", "0-15")
    environ = task_environment(root / "workspace/duckdb_spill", "AWS_ECS_EC2")
    limits = task_capacity.apply("features_build", environ, root, task(65536, 16, [GUARD, PHASE]), 1024, lambda path: 100 * GIB)
    expect(limits is not None and limits.threads == 8, limits)
    with mock.patch.dict(os.environ, environ, clear=True):
        found = ecs_engine.duckdb_temp_settings()
    expect(found.get("max_temp_directory_size") == f"{60 * GIB}B", found)


def case_apply_does_nothing_outside_a_task(root: Path) -> None:
    environ = {"DUCKDB_MEMORY_LIMIT": "150MB", "JOB_THREADS": "2"}
    expect(task_capacity.apply("default", environ, root, None, None, None) is None)
    expect(environ == {"DUCKDB_MEMORY_LIMIT": "150MB", "JOB_THREADS": "2"}, environ)


def case_apply_fails_by_name_when_the_task_is_tiny(root: Path) -> None:
    v2(root, str(GIB), "100000 100000", "0")
    expect_error(lambda: task_capacity.apply("default", task_environment(None), root, task(1024, 1, [PHASE]), 1024, None), "floor")


def case_apply_leaves_the_spill_cap_unset_without_a_spill_folder(root: Path) -> None:
    v2(root, str(16 * GIB), "400000 100000", "0-3")
    environ = task_environment(None)
    task_capacity.apply("default", environ, root, task(16384, 4, [PHASE]), 1024, None)
    expect("HAI_DUCKDB_MAX_TEMP_BYTES" not in environ and environ["JOB_THREADS"] == "4", environ)


def case_entry_points_derive_their_limits_before_an_engine_starts(root: Path) -> None:
    for relative, engine in ENTRY_POINTS.items():
        tree = ast.parse((ROOT / relative).read_text())
        mains = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "main"]
        expect(mains, f"{relative} has no main")
        calls = [node for node in ast.walk(mains[0]) if isinstance(node, ast.Call)]
        derive = [node.lineno for node in calls if ast.unparse(node.func) == "task_capacity.apply"]
        start = [node.lineno for node in calls if ast.unparse(node.func) == engine]
        expect(derive, f"{relative} main does not call task_capacity.apply")
        expect(start and min(derive) < min(start), f"{relative} derives its limits after it starts work with {engine}")


def case_the_layers_choose_the_profile_by_phase(root: Path) -> None:
    expect(layer_run.capacity_profile("features", "build") == "features_build")
    expect(
        {layer_run.capacity_profile(layer, phase) for layer in layer_run.PHASES for phase in layer_run.PHASES[layer] if (layer, phase) != ("features", "build")}
        == {"default"}
    )


def started_job(folder: Path, seconds_ago: int) -> layer_run.Job:
    (folder / "task_started").write_text(f"{layer_run.time.time() - seconds_ago:.0f}\n")
    return layer_run.Job("features", "20261109T000000Z-0a1b2c3d", "real", folder / "code")


def case_the_layer_passes_the_phase_deadline_to_the_build(root: Path) -> None:
    job = started_job(root, 600)
    left = layer_run.task_seconds_left(job)
    with mock.patch.object(layer_run, "STEP_SECONDS", 10**6):
        span = float(layer_run.phase_deadline_environment(job)["HAI_PHASE_DEADLINE_EPOCH"]) - layer_run.time.time()
    expect(abs(span - left) < 5 and left < 21600 - 590, (span, left))
    with mock.patch.object(layer_run, "STEP_SECONDS", 3600):
        capped = float(layer_run.phase_deadline_environment(job)["HAI_PHASE_DEADLINE_EPOCH"]) - layer_run.time.time()
    expect(3590 < capped < 3602, "a phase never outlives the limit of one step of the driver")
    expect_error(lambda: layer_run.phase_deadline_environment(started_job(root, 30000)), "deadline")


def case_features_build_limits_come_from_the_remaining_deadline(root: Path) -> None:
    now = [1000.0]
    budget = features_build.BuildBudget(1000.0 + 21000, lambda: now[0])
    expect(budget.limit("copy") == 15750 * 600 // 8400 == 1125, budget.limit("copy"))
    now[0] += 500
    expect(budget.limit("seed") == 15250 * 900 // 7800, budget.limit("seed"))
    now[0] += 3000
    expect(budget.limit("build") == 15750 - 3500, "the build gets all that is left of the share")
    expect(budget.limit("build") > 3300, "today's run needed more than the fixed 3300 seconds it had")


def case_features_build_keeps_a_reserve_for_the_later_phases(root: Path) -> None:
    now = [0.0]
    budget = features_build.BuildBudget(20000.0, lambda: now[0])
    now[0] = 14999.0
    expect(budget.limit("build") == 1, "the share of the phases after the build is never given to the build")
    now[0] = 15000.0
    expect_error(lambda: budget.limit("build"), "no time")


def case_features_build_refuses_a_deadline_that_is_too_short(root: Path) -> None:
    expect_error(lambda: features_build.BuildBudget(600.0, lambda: 0.0), "left")
    expect_error(lambda: features_build.BuildBudget(-5.0, lambda: 0.0), "left")
    features_build.BuildBudget(4000.0, lambda: 0.0)


def case_features_build_reads_the_deadline_from_the_environment(root: Path) -> None:
    now = 5000.0
    found = features_build.BuildBudget.from_environment({"HAI_PHASE_DEADLINE_EPOCH": str(now + 8000)}, lambda: now)
    expect(found.limit("copy") == int(8000 * 75 // 100 * 600 // 8400), found.limit("copy"))
    default = features_build.BuildBudget.from_environment({}, lambda: now)
    expect(default.deadline == now + 4 * 3600, "without a task deadline the budget is one step of the driver")
    expect_error(lambda: features_build.BuildBudget.from_environment({"HAI_PHASE_DEADLINE_EPOCH": "soon"}, lambda: now), "HAI_PHASE_DEADLINE_EPOCH")


def case_features_build_names_the_derivation_when_a_step_runs_long(root: Path) -> None:
    budget = features_build.BuildBudget(21000.0, lambda: 0.0)
    expect("21000" in budget.describe("copy") and "1125" in budget.describe("copy"), budget.describe("copy"))
    source = (ROOT / "scripts/features/features_build.py").read_text()
    expect(not any(name in source for name in ("COPY_SECONDS", "SEED_SECONDS", "BUILD_SECONDS", "WORKER_SECONDS")), "a fixed limit is left")


CASES = [value for name, value in sorted(globals().items()) if name.startswith("case_")]


def main() -> int:
    failed = 0
    for case in CASES:
        with tempfile.TemporaryDirectory(prefix="task_capacity_") as folder:
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    case(Path(folder))
            except (AssertionError, AttributeError, KeyError, ValueError, TypeError, RuntimeError, OSError) as error:
                failed += 1
                sys.stdout.write(f"FAIL {case.__name__}: {type(error).__name__}: {error}\n")
            else:
                sys.stdout.write(f"PASS {case.__name__}\n")
    sys.stdout.write(f"{len(CASES) - failed} of {len(CASES)} cases passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
