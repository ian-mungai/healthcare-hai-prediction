"""The closed failure record that a host job writes when its phase fails.

A failed phase writes one line, ``phase_failure: {json}``, as its last line. The record has exactly four keys: ``v`` (1), ``phase`` (a name from ``PHASES``),
``codes`` (one to eight names from ``CODES``) and ``facts`` (numbers under the names of ``FACTS``: the capacity figures of a refusal and an exit code). It holds
no stderr, no exception text and no log line, so no job value, credential, bucket name or location can reach it. A number is data too, so the facts are only the
capacity figures of a refusal that the capacity module measured (``measured``), each in its own range and consistent with the others; a job's own exception can
carry none, and the exit code is added by the launcher from ECS. A failure of a class that has no code of its own is the one safe code ``unexpected_error``.

The launcher reads the line from the log of the failing container (``orchestration.ecs_adapter.parse_failure_line``, the reader, which the Airflow image holds
and this module is not part of), validates it against the same lists, adds the container identity and keeps it in the end record of the launch. A message for a
person is rendered from fixed templates. ``tests/test_phase_failure.py`` keeps the lists of the two modules equal. ``resource_peaks.recorded``, which every ECS
job entry point runs through, writes a generic record for a failed job that wrote none."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TextIO

MARKER = "phase_failure: "
VERSION = 1
MAX_CODES = 8
MAX_LINE = 2000
PHASES = (
    "init",
    "fetch",
    "pin",
    "build",
    "validate",
    "verify",
    "export",
    "publish",
    "upload",
    "maintain",
    "feast",
    "load",
    "check",
    "inventory",
    "stage",
    "prepare",
    "extract",
    "train",
    "promote",
    "research",
    "canary",
    "notify",
    "baseline",
    "current",
    "serve",
    "ingest",
    "run",
    "guard",
    "unknown",
)
# The findings of the host job startup check that can fail (scripts.infrastructure.startup_check).
STARTUP_CODES = (
    "credentials",
    "dbt_extension_directory_missing",
    "dbt_extension_missing",
    "dbt_profile_unreadable",
    "dbt_target_downloads_extensions",
    "dbt_target_not_executed",
    "dbt_target_refused",
    "dbt_target_unknown",
    "ecr_address_unusable",
    "ecr_unreachable",
    "input_file_missing",
    "input_file_unreadable",
    "mlflow_address_unusable",
    "mlflow_unreachable",
    "mode_files_unenumerated",
    "openmetadata_address_unusable",
    "openmetadata_unreachable",
    "plan_inputs_empty",
    "plan_inputs_missing",
    "polaris_address_unusable",
    "polaris_unreachable",
    "redis_address_unusable",
    "redis_unreachable",
    "region_invalid",
    "region_unset",
    "s3_address_unusable",
    "s3_unreachable",
    "startup_check_internal_error",
    "task_environment_unrecognised",
)
# The refusals of the task capacity (scripts.lakehouse.task_capacity).
CAPACITY_CODES = (
    "capacity_cpu_unreadable",
    "capacity_engine_floor",
    "capacity_file_garbled",
    "capacity_memory_unreadable",
    "capacity_override_invalid",
    "capacity_profile_unknown",
    "capacity_spill_floor",
)
# One safe category for each kind of error that a job entry point names, and the two states of a task: a phase that failed with no code of its own, and a
# container that ECS stopped with SIGTERM when another one failed.
ERROR_CODES = (
    "archive_error",
    "binding_error",
    "bronze_error",
    "check_error",
    "config_error",
    "deploy_refused",
    "dispatch_error",
    "guard_timeout",
    "key_error",
    "os_error",
    "phase_failed",
    "phase_stopped",
    "pipeline_error",
    "provenance_error",
    "publish_error",
    "run_refused",
    "step_error",
    "step_failed",
    "type_error",
    "unexpected_error",
    "value_error",
)
# What ECS says of a container that never ran or never exited (the launcher derives these from the container status, never from the text of its reason).
CONTAINER_CODES = (
    "container_create_error",
    "container_initialization_error",
    "container_inspect_error",
    "container_out_of_memory",
    "container_pull_error",
    "container_reason_other",
    "container_runtime_error",
    "container_start_error",
    "container_timeout",
)
CODES = (*STARTUP_CODES, *CAPACITY_CODES, *ERROR_CODES, *CONTAINER_CODES)
# The facts a job may write, all capacity figures in bytes except the count of other containers. The exit code is a fact of the stored record only: the launcher
# takes it from ECS.
LINE_FACTS = ("available", "engine_floor", "headroom", "memory", "sibling_reserve", "siblings", "spill_floor", "spill_free")
FACTS = (*LINE_FACTS, "exit_code")
# The fixed figures of the capacity module (scripts.lakehouse.task_capacity), which a test keeps equal.
GIB = 1 << 30
ENGINE_FLOOR = 2 * GIB
SPILL_FLOOR = GIB
SIBLING_RESERVE = 256 * (1 << 20)
HEADROOM_FLOOR = GIB
HEADROOM_PERCENT = 6
SPILL_SHARE_PERCENT = 60
MAX_SIBLINGS = 10  # an ECS task has at most ten containers
# The range of each fact: bytes up to 16 TiB (a spill filesystem up to 1 PiB), a count of other containers, an exit code. A refusal fixes the rest.
RANGES = {
    "available": (-(1 << 44), ENGINE_FLOOR - 1),
    "engine_floor": (ENGINE_FLOOR, ENGINE_FLOOR),
    "exit_code": (1, 255),
    "headroom": (HEADROOM_FLOOR, 1 << 44),
    "memory": (1 << 20, 1 << 44),
    "sibling_reserve": (0, MAX_SIBLINGS * SIBLING_RESERVE),
    "siblings": (0, MAX_SIBLINGS),
    "spill_floor": (SPILL_FLOOR, SPILL_FLOOR),
    "spill_free": (0, 1 << 50),
}
ENGINE_FACTS = frozenset({"memory", "headroom", "siblings", "sibling_reserve", "available", "engine_floor"})
SPILL_FACTS = frozenset({"spill_free", "spill_floor"})
# The class of an error, by the name of any class in its bases, to its category. The text of an error is never read.
ERROR_CLASSES = {
    "BindingError": "binding_error",
    "BronzeError": "bronze_error",
    "CheckError": "check_error",
    "ConfigError": "config_error",
    "DeployRefused": "deploy_refused",
    "DispatchError": "dispatch_error",
    "KeyError": "key_error",
    "OSError": "os_error",
    "PipelineError": "pipeline_error",
    "ProvenanceError": "provenance_error",
    "PublishError": "publish_error",
    "RunRefused": "run_refused",
    "StepError": "step_error",
    "StepFailed": "step_failed",
    "TarError": "archive_error",
    "TypeError": "type_error",
    "ValueError": "value_error",
}


class Measured(dict[str, int]):
    """Facts that the capacity module measured. Only an instance of this class is kept in a record: a job's own exception cannot carry facts."""


def measured(facts: Mapping[str, int]) -> Measured:
    """The listed capacity facts of a measurement, as the one kind of facts that a record keeps."""
    return Measured({name: value for name, value in facts.items() if name in LINE_FACTS and type(value) is int})


def facts_fit(codes: Sequence[str], facts: Mapping[str, object], *, stored: bool) -> bool:
    """Whether the facts belong to the codes and are what a refusal of the capacity module can produce: only the facts of the capacity codes present (and the
    exit code in a stored record), each an integer in its range, and the figures of a refusal consistent with the fixed floors, so that they follow from the
    memory and the count of other containers alone."""
    allowed = FACTS if stored else LINE_FACTS
    for name, value in facts.items():
        low, high = RANGES.get(name, (1, 0))
        if name not in allowed or type(value) is not int or not low <= value <= high:
            return False
    figures: dict[str, int] = {name: value for name, value in facts.items() if isinstance(value, int)}
    need: set[str] = set()
    if "capacity_engine_floor" in codes:
        need |= ENGINE_FACTS
    if "capacity_spill_floor" in codes:
        need |= SPILL_FACTS
    if {name for name in figures if name != "exit_code"} != need:
        return False
    if "capacity_engine_floor" in codes:
        memory, siblings = figures["memory"], figures["siblings"]
        headroom = max(HEADROOM_FLOOR, memory * HEADROOM_PERCENT // 100)
        reserve = siblings * SIBLING_RESERVE
        expected = (headroom, reserve, memory - headroom - reserve)
        if (figures["headroom"], figures["sibling_reserve"], figures["available"]) != expected or figures["available"] >= ENGINE_FLOOR:
            return False
    return not ("capacity_spill_floor" in codes and figures["spill_free"] * SPILL_SHARE_PERCENT // 100 >= SPILL_FLOOR)


def record(phase: str, codes: Sequence[str], facts: Mapping[str, int] | None = None) -> dict[str, Any]:
    """The closed record of a failure. A phase or code that is not on its list becomes ``unknown`` or ``unexpected_error``. Facts are kept only when they are
    ``measured`` and fit their codes; otherwise the capacity codes that need them are dropped with them."""
    chosen = list(dict.fromkeys(code for code in codes if isinstance(code, str) and code in CODES))[:MAX_CODES]
    kept = dict(facts) if isinstance(facts, Measured) else {}
    if not facts_fit(chosen, kept, stored=False):
        chosen = [code for code in chosen if code not in ("capacity_engine_floor", "capacity_spill_floor")]
        kept = {}
    return {"v": VERSION, "phase": phase if phase in PHASES else "unknown", "codes": chosen or ["unexpected_error"], "facts": kept}


def record_for(phase: str, error: BaseException | None) -> dict[str, Any]:
    """The record of an error: the codes that the error carries (``failure_codes``) and the facts of a capacity refusal (``failure_facts``, kept only when
    measured), else the category of its class, else ``unexpected_error``; no error is a phase that failed with no code of its own. The message of the error is
    never read."""
    if error is None:
        return record(phase, ("phase_failed",))
    tagged = getattr(error, "failure_codes", None)
    codes = (
        tuple(tagged)
        if isinstance(tagged, tuple | list)
        else (next((ERROR_CLASSES[kind.__name__] for kind in type(error).__mro__ if kind.__name__ in ERROR_CLASSES), "unexpected_error"),)
    )
    return record(phase, codes, getattr(error, "failure_facts", None))


def line(found: Mapping[str, Any]) -> str:
    """The failure line of a record, without its line end."""
    return MARKER + json.dumps(found, sort_keys=True, separators=(",", ":"))


def emit(found: Mapping[str, Any], stream: TextIO | None = None) -> None:
    """Write a record on standard error, as the last line of the phase. It is best effort: any fault of the stream, an interrupt included, ends it and never
    reaches the job, whose result it must not change."""
    try:
        target = sys.stderr if stream is None else stream
        target.write(line(found) + "\n")
        target.flush()
    except BaseException:
        return


def emit_error(phase: str, error: BaseException | None, stream: TextIO | None = None) -> None:
    """The record of an error, written like ``emit``: best effort, never raising."""
    try:
        emit(record_for(phase, error), stream)
    except BaseException:
        return


def phase_of(argv: Sequence[str]) -> str:
    """The phase an entry point runs: its first argument when that is a phase name, else the name of its script when that is one, else ``unknown``."""
    if len(argv) > 1 and argv[1] in PHASES:
        return argv[1]
    stem = Path(argv[0]).stem if argv and argv[0] else ""
    return stem if stem in PHASES else "unknown"


class StderrTail:
    """Stands in for standard error while a job runs. Every write goes through, and one fact is kept: whether the job wrote a failure line itself. It keeps no
    text of the job, so the recorder cannot copy any of it."""

    def __init__(self, stream: TextIO) -> None:
        self.stream = stream
        self.marked = False
        self.head = ""

    def write(self, text: str) -> int:
        written = self.stream.write(text)
        for index, part in enumerate(text.split("\n")):
            if index:
                self.settle()
                self.head = ""
            if len(self.head) < len(MARKER):
                self.head = (self.head + part)[: len(MARKER)]
        return written

    def settle(self) -> None:
        if self.head == MARKER:
            self.marked = True

    def finish(self) -> None:
        """Count the text after the last line end as a line."""
        self.settle()
        self.head = ""

    def __getattr__(self, name: str) -> Any:
        return getattr(self.stream, name)
