"""Startup check of a host job: find a broken environment in seconds and name the cause in the first log lines.

A job on the EC2 host or on Fargate runs this at the start of its first code step, from the staged code. It checks only what the job needs, each check with a
fixed bound, and logs one line per check on stderr. Failures come first. A run outside an ECS task checks nothing.

- ``check_phase`` and ``check_environment``: the task environment is recognised, the task credential source is the native provider, the host services the
  job needs (Polaris, MLflow, OpenMetadata, Redis) accept a TCP connect at their configured addresses, and S3 and ECR accept a connect on 443. The connects run
  side by side and share one bound. ``JOB_PHASES`` holds, for every host job, the phases that check and the services each needs.
- ``check_build``: the dbt target that the build selects (``pipeline_build.build_target``) and, for a profile that lists DuckDB extensions, that its extension
  directory exists and holds each of them, so dbt downloads nothing.
- ``check_files``: every repository-relative file that the job will read exists in the task. ``queue_files`` derives them from the collector plans of the
  modes that a publisher queue runs, and ``check_queue_files`` does both for a queue, and only in a task.

The check opens a socket and reads files. It makes no request that needs a grant.

The checks run in a child Python process (``startup_check_child``), started without user site packages, bytecode files and the current folder on its
path, with a minimal environment passed by name, in a private folder. (``-I`` cannot be used with ``-m`` and a path variable, so its effects are set by
flags and the explicit environment.)
The child's standard output and error go to a pipe that the parent reads and never prints, so nothing that a dependency logs, warns, dumps or prints reaches
the job output. The child reports only records: a reason code and parts from a closed grammar (a fixed phrase, a listed name, a parsed host and port, a digest,
a count). The parent parses every record strictly, checks each code and part against its own fixed lists, and prints the text from its own phrases. Anything
else, an unparseable line, an unknown code, a crash or a timeout, ends as ``startup_check_internal_error``. The parent never calls the text conversion of an
exception.
"""

from __future__ import annotations

import dataclasses
import errno
import functools
import hashlib
import importlib
import ipaddress
import json
import math
import os
import re
import socket
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from scripts.lakehouse import ecs_engine
from scripts.process import run_command

# One bound for all connects of a check. They run side by side, so a check adds at most this and a little more.
CONNECT_SECONDS = 3.0
# The bound of the child process: its imports, its connects and its file reads.
CHILD_SECONDS = 60.0
# Run the checks in a child process. The tests of the checks themselves run them in this process, with socket stand-ins.
ISOLATED = True
CHILD_MODULE = "scripts.infrastructure.startup_check_child"
REPOSITORY = Path(__file__).resolve().parents[2]
# Variable names that the tests of the isolation add to the child's environment.
CHILD_ENV_EXTRA: tuple[str, ...] = ()
RESULT_NAME = "results.jsonl"
REQUEST_NAME = "request.json"
MAX_LINE_CHARACTERS = 4096
MAX_RECORDS = 5000
MAX_RESULT_BYTES = 2_000_000
# ECS gives these to every container of a task, on Fargate and on the host.
ECS_MARKERS = ("ECS_CONTAINER_METADATA_URI_V4", "ECS_CONTAINER_METADATA_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
DEFAULT_PORTS = {"http": 80, "https": 443}
# The host jobs of config/orchestration/host_jobs.json, the phase at which each checks and the host services that phase needs. A phase that is not listed
# checks nothing. The first code step of a job is its first listed phase and checks every service that any later phase uses, so a job with an unreachable
# service stops before it prepares or pins anything. The layer jobs check their build separately (``check_build``).
JOB_PHASES: dict[str, dict[str, tuple[str, ...]]] = {
    "silver": {"pin": ("polaris",)},
    "gold": {"pin": ("polaris",)},
    "features": {"pin": ("polaris", "redis"), "feast": ("polaris", "redis")},
    "bronze": {"pin": ("polaris",), "load": ("polaris",)},
    "train": {"prepare": ("polaris", "mlflow"), "extract": ("polaris",), "train": ("mlflow",)},
    "promote": {"promote": ("mlflow",)},
    "drift": {"baseline": ("mlflow", "polaris"), "current": ("polaris",)},
    "serve": {"serve": ("mlflow",)},
    "metadata": {"ingest": ("openmetadata",)},
    "publisher": {"run": ()},
}
# The variable that holds each service address. Redis runs on the host of POLARIS_URI, as scripts.features.feast_aws derives it.
SERVICE_VARIABLES = {"polaris": "POLARIS_URI", "mlflow": "MLFLOW_TRACKING_URI", "openmetadata": "OM_API"}

# Everything below is the allowlist of what a diagnostic may show. The child can only name these, and the parent checks each record against them again.
SERVICES = ("polaris", "mlflow", "openmetadata", "redis", "s3", "ecr")
VARIABLES = frozenset({"POLARIS_URI", "MLFLOW_TRACKING_URI", "OM_API", "AWS_EXECUTION_ENV", "AWS_REGION", "AWS_DEFAULT_REGION"})
EXECUTION_LABELS = frozenset({"AWS_ECS_FARGATE", "AWS_ECS_EC2", "AWS_ECS_MANAGED_INSTANCES", "AWS_ECS_EXTERNAL"})
AWS_REGIONS = frozenset(
    (
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "ca-central-1",
        "ca-west-1",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-central-1",
        "eu-central-2",
        "eu-north-1",
        "eu-south-1",
        "eu-south-2",
        "ap-south-1",
        "ap-south-2",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-southeast-3",
        "ap-southeast-4",
        "ap-southeast-5",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-east-1",
        "sa-east-1",
        "me-south-1",
        "me-central-1",
        "af-south-1",
        "il-central-1",
        "us-gov-east-1",
        "us-gov-west-1",
    )
)
AWS_HOST = re.compile(r"(s3|api\.ecr|dkr\.ecr)\.(" + "|".join(sorted(AWS_REGIONS)) + r")\.amazonaws\.com")
SPILL_CODE = "temporary_storage_settings_incomplete"
CREDENTIAL_CODES = frozenset(
    {
        "task_credential_source_refused",
        "task_credential_provider_missing",
        "task_catalog_endpoint_refused",
        "fixture_role_session_refused",
        "task_credential_check_failed",
    }
)
DBT_TARGETS = frozenset(
    {"lakehouse_task", "lakehouse_session", "lakehouse_session_bounded", "pinned_silver", "pinned_silver_bounded", "fixture", "fixture_bounded"}
)
DBT_EXTENSIONS = frozenset({"iceberg", "httpfs", "aws", "parquet", "json"})
EXTENSION_NAME = re.compile(r"[a-z0-9_]{1,40}")
FILE_KINDS = frozenset(
    {
        "input",
        "plan",
        "lock",
        "release",
        "terms",
        "reference",
        "legacy_registry",
        "report",
        "original",
        "additions_lock",
        "template",
        "config_input",
        "controls",
        "listing",
    }
)
# The collector modes of the harness (``redownload`` and ``stored_mirror`` list them; a test keeps this list complete).
KNOWN_MODES = frozenset(
    {
        "bls_api_v2",
        "census_acs_api",
        "census_acs_detailed",
        "cms_owners_org",
        "file",
        "hcai_util_workbook",
        "hud_api",
        "hud_xlsx",
        "il_directory",
        "mmd_api",
        "onc_mu_hospital",
        "reference_download",
        "wonder_export",
    }
)
JOBS = frozenset({*JOB_PHASES, "unlisted"})
CHECK_LABELS = frozenset({"environment", "build", "files", "checks"})
TASK_VALUES = " or ".join(sorted(ecs_engine.TASK_ENVIRONMENTS))
# The lists that a label part is checked against, by name.
LABEL_SETS: dict[str, frozenset[str]] = {
    "exec": EXECUTION_LABELS | ecs_engine.TASK_ENVIRONMENTS,
    "target": DBT_TARGETS,
    "ext": DBT_EXTENSIONS,
    "cred": CREDENTIAL_CODES | {SPILL_CODE},
    "root": frozenset({"code", "mirror"}),
    "expected": frozenset({TASK_VALUES}),
}
# Every fixed phrase that a finding may contain. The child names one by its text, and the parent accepts only these.
PHRASES = frozenset(
    {
        "a task value",
        "unset",
        "an unrecognised value",
        "is",
        "in an ECS task, so the job would not use task settings; expected",
        "native task provider",
        "is not set",
        "is not a valid address",
        "has an invalid port",
        "names no host",
        "names no port",
        "timed out",
        "timed out after",
        "connection refused",
        "connection reset",
        "name not resolved",
        "connect failed",
        "neither AWS_REGION nor AWS_DEFAULT_REGION is set",
        "the region in AWS_REGION or AWS_DEFAULT_REGION is not a known AWS Region",
        "an unlisted variable",
        "an unlisted extension",
        "an unlisted target",
        "an unlisted",
        "the dbt profile cannot be read as a profile of outputs",
        "is not an output of the dbt profile",
        "(no DuckDB extensions)",
        "lists",
        "and names no extension directory, so dbt would download them and the job has no egress",
        "the extension directory does not exist in this image",
        "not in the extension directory",
        "(extensions",
        "present)",
        "the build target was refused",
        SPILL_CODE,
        "is a runnable plan mode with no file list in the startup check, so its files would go unchecked",
        "root:",
        "does not read as a JSON object",
        ": the staged listing of plan input files is absent, so no plan file reaches the code root",
        ": the staged listing of plan input files holds no entry, so no plan file reaches the code root",
        "(",
        "files)",
        "none named",
    }
)
CODES = frozenset(
    {
        *SERVICES,
        *(f"{service}_unreachable" for service in SERVICES),
        *(f"{service}_address_unusable" for service in SERVICES),
        "task_environment",
        "task_environment_unrecognised",
        "credentials",
        "region_unset",
        "region_invalid",
        "dbt_profile_unreadable",
        "dbt_target_unknown",
        "dbt_target",
        "dbt_target_downloads_extensions",
        "dbt_extension_directory_missing",
        "dbt_extension_missing",
        "dbt_target_refused",
        "input_file",
        "input_file_missing",
        "input_file_unreadable",
        "plan_inputs",
        "plan_inputs_missing",
        "plan_inputs_empty",
        "input_files",
        "mode_files_unenumerated",
        "startup_check_internal_error",
    }
)
ERROR_NAMES = frozenset(errno.errorcode.values())
DIGEST = re.compile(r"[0-9a-f]{12}")
MODE_DIGEST = re.compile(r"mode#[0-9a-f]{12}")
LARGEST_COUNT = 10_000_000
LARGEST_SECONDS = 1_000_000.0


class StartupCheckError(RuntimeError):
    """One or more startup checks failed. The message names each cause with text from the fixed lists only."""


class InvalidRecords(ValueError):
    """The child's result was not a strictly valid transcript. The message is fixed and repeats nothing of the input."""

    def __init__(self) -> None:
        super().__init__("the result of the check was not valid")


Part = tuple[Any, ...]
Detail = tuple[Part, ...]


def phrase(text: str) -> Detail:
    """A fixed phrase from ``PHRASES``."""
    if text not in PHRASES:
        raise ValueError("a phrase that is not listed")
    return (("p", text),)


def join(*details: Detail) -> Detail:
    return tuple(part for detail in details for part in detail)


def label(name: str, value: object, other: Detail) -> Detail:
    """The value itself when it is on the named list, else the fixed phrase ``other``."""
    return (("l", name, value),) if isinstance(value, str) and value in LABEL_SETS[name] else other


def digest(text: str) -> str:
    """A short fingerprint that lets the owner match a name to a configured path or host without the log repeating it."""
    return hashlib.sha256(text.encode("utf-8", "backslashreplace")).hexdigest()[:12]


def variable(name: str) -> Detail:
    return (("v", name),) if name in VARIABLES else phrase("an unlisted variable")


def count(number: int) -> Detail:
    return (("n", int(number)),)


def seconds(value: float) -> Detail:
    return (("t", float(value)),)


def endpoint(host: str, port: int) -> Detail:
    """A host and port. The host shows only as an IP address or an AWS service endpoint of a known Region, else as a digest."""
    if "%" not in host:
        try:
            return (("h", "ip", ipaddress.ip_address(host).compressed, int(port)),)
        except ValueError:
            pass
    if AWS_HOST.fullmatch(host):
        return (("h", "aws", host, int(port)),)
    return (("h", "digest", digest(host), int(port)),)


def mode_label(owner: str) -> Detail:
    """A collector mode, or modes joined by a plus sign, when the harness knows each; else a digest."""
    known = bool(owner) and all(part in KNOWN_MODES for part in owner.split("+"))
    return (("m", owner if known else f"mode#{digest(owner)}"),)


def file_label(kind: str, path: str) -> Detail:
    """A file by its kind and the digest of its path, because a configured path may carry anything."""
    return (("f", kind if kind in FILE_KINDS else "input", digest(path)),)


def file_digest(path: str | Path) -> str:
    return digest(str(path))


def error_detail(error: BaseException) -> Detail:
    """A connect failure by its class, and its errno name when it has one. Its message and text conversion are never used."""
    if isinstance(error, TimeoutError):
        return phrase("timed out")
    if isinstance(error, ConnectionRefusedError):
        return phrase("connection refused")
    if isinstance(error, ConnectionResetError):
        return phrase("connection reset")
    if isinstance(error, socket.gaierror):
        return phrase("name not resolved")
    code = errno.errorcode.get(error.errno, "") if isinstance(error, OSError) and isinstance(error.errno, int) else ""
    return join(phrase("connect failed"), (("r", code),) if code else ())


@dataclass(frozen=True)
class Finding:
    name: str
    ok: bool
    detail: Detail

    def __post_init__(self) -> None:
        if self.name not in CODES or not isinstance(self.detail, tuple):
            raise TypeError("a finding takes a listed code and a detail of parts")


# ---- the parent: strict parsing of the child's records and the text it prints, from its own lists ----


def is_int(value: object, largest: int = LARGEST_COUNT) -> bool:
    return type(value) is int and 0 <= value <= largest


def is_number(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(float(value)) and 0.0 <= float(value) <= LARGEST_SECONDS


def valid_part(part: object) -> Part:
    """One part of a detail, or ``InvalidRecords``. Each kind has its own shape and its own list."""
    if type(part) is not list or not part or type(part[0]) is not str:
        raise InvalidRecords
    kind, *values = part
    shapes = {"p": 1, "v": 1, "l": 2, "m": 1, "f": 2, "h": 3, "n": 1, "t": 1, "g": 1, "r": 1}
    if kind not in shapes or len(values) != shapes[kind]:
        raise InvalidRecords
    if kind == "p":
        ok = values[0] in PHRASES
    elif kind == "v":
        ok = values[0] in VARIABLES
    elif kind == "l":
        ok = type(values[0]) is str and values[0] in LABEL_SETS and values[1] in LABEL_SETS[values[0]]
    elif kind == "m":
        ok = type(values[0]) is str and (bool(MODE_DIGEST.fullmatch(values[0])) or all(item in KNOWN_MODES for item in values[0].split("+")))
    elif kind == "f":
        ok = values[0] in FILE_KINDS and type(values[1]) is str and bool(DIGEST.fullmatch(values[1]))
    elif kind == "h":
        ok = valid_host(values[0], values[1]) and is_int(values[2], 65535)
    elif kind == "n":
        ok = is_int(values[0])
    elif kind in ("t", "g"):
        ok = is_number(values[0])
    else:
        ok = values[0] in ERROR_NAMES
    if not ok:
        raise InvalidRecords
    return tuple(part)


def valid_host(form: object, text: object) -> bool:
    if type(text) is not str:
        return False
    if form == "digest":
        return bool(DIGEST.fullmatch(text))
    if form == "aws":
        return bool(AWS_HOST.fullmatch(text))
    if form != "ip" or "%" in text:
        return False
    try:
        return ipaddress.ip_address(text).compressed == text
    except ValueError:
        return False


def valid_record(record: object) -> dict[str, Any]:
    """One record of the transcript: a finding, the summary or the end, with exactly its keys."""
    if type(record) is not dict:
        raise InvalidRecords
    kind = record.get("k")
    if kind == "e" and set(record) == {"k"}:
        return {"k": "e"}
    if kind == "s" and set(record) == {"k", "j", "l", "n", "x", "t"}:
        if record["j"] not in JOBS or record["l"] not in CHECK_LABELS or not is_int(record["n"]) or not is_int(record["x"]) or not is_number(record["t"]):
            raise InvalidRecords
        return dict(record)
    if kind == "f" and set(record) == {"k", "c", "o", "d"}:
        if record["c"] not in CODES or type(record["o"]) is not bool or type(record["d"]) is not list:
            raise InvalidRecords
        return {"k": "f", "c": record["c"], "o": record["o"], "d": tuple(valid_part(part) for part in record["d"])}
    raise InvalidRecords


def reject_constant(_name: str) -> None:
    raise InvalidRecords


def parse_records(lines: list[str]) -> list[dict[str, Any]]:
    """The transcript of the child, or ``InvalidRecords``: findings, then one summary, then the end, and nothing after it."""
    if not lines or len(lines) > MAX_RECORDS:
        raise InvalidRecords
    records = []
    for line in lines:
        if type(line) is not str or len(line) > MAX_LINE_CHARACTERS:
            raise InvalidRecords
        try:
            records.append(valid_record(json.loads(line, parse_constant=reject_constant)))
        except Exception:
            raise InvalidRecords from None
    kinds = [record["k"] for record in records]
    if kinds[-2:] != ["s", "e"] or kinds.count("s") != 1 or kinds.count("e") != 1 or set(kinds[:-2]) != ({"f"} if len(kinds) > 2 else set()):
        raise InvalidRecords
    return records


def text_of(part: Part) -> str:
    """The text of one validated part. Only the parent's own lists and the parsed numbers are in it."""
    kind = part[0]
    if kind == "h":
        return f"host#{part[2]}:{part[3]}" if part[1] == "digest" else f"{part[2]}:{part[3]}"
    if kind == "f":
        return f"{part[1]} {part[2]}"
    if kind == "l":
        return str(part[2])
    if kind == "n":
        return str(part[1])
    if kind == "t":
        return f"{part[1]:.2f} s"
    if kind == "g":
        return f"{part[1]:g} s"
    return str(part[1])


def render(detail: Iterable[Part]) -> str:
    return " ".join(text_of(part) for part in detail)


def emit(records: list[dict[str, Any]]) -> None:
    """Print the validated transcript on stderr, failures first, and raise when one failed."""
    findings = [record for record in records if record["k"] == "f"]
    failed = [record for record in findings if not record["o"]]
    for record in failed:
        sys.stderr.write(f"startup_check: FAILED {record['c']} {render(record['d'])}\n")
    for record in findings:
        if record["o"]:
            sys.stderr.write(f"startup_check: ok {record['c']} {render(record['d'])}\n")
    summary = next(record for record in records if record["k"] == "s")
    sys.stderr.write(f"startup_check: {summary['j']} {summary['l']}: {summary['n']} checks, {summary['x']} failed, {summary['t']:.1f} s\n")
    sys.stderr.flush()
    if failed:
        raise StartupCheckError("startup check failed: " + "; ".join(f"{record['c']} {render(record['d'])}" for record in failed))


def guarded[T](function: Callable[..., T]) -> Callable[..., T]:
    """The top level of every entry point. Any other error ends as a fixed code, with no message, no chained error and no traceback text."""

    @functools.wraps(function)
    def wrapper(*arguments: Any, **keywords: Any) -> T:
        try:
            return function(*arguments, **keywords)
        except StartupCheckError:
            raise
        except Exception:
            sys.stderr.write("startup_check: FAILED startup_check_internal_error\n")
            sys.stderr.flush()
            raise StartupCheckError("startup check failed: startup_check_internal_error") from None

    return wrapper


def child_environment_names() -> tuple[str, ...]:
    """The only variables that the child gets from this process, by name: what the checks read, and the tests' additions."""
    names = (
        *ECS_MARKERS,
        *SERVICE_VARIABLES.values(),
        "AWS_EXECUTION_ENV",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        *ecs_engine.FORBIDDEN_CREDENTIAL_VARIABLES,
        "HAI_DUCKDB_TEMP_DIRECTORY",
        "HAI_DUCKDB_MAX_TEMP_BYTES",
        *CHILD_ENV_EXTRA,
    )
    return tuple(dict.fromkeys(names))


def child_environment(folder: str) -> dict[str, str]:
    environment = {name: os.environ[name] for name in child_environment_names() if name in os.environ}
    return {**environment, "PATH": "/usr/bin:/bin", "HOME": folder, "TMPDIR": folder, "PYTHONPATH": str(REPOSITORY)}


def dump(records: Iterable[dict[str, Any]]) -> list[str]:
    return [json.dumps(record, separators=(",", ":"), allow_nan=False) for record in records]


def run_child(request: dict[str, Any]) -> list[str]:
    """Run the request in the isolated child and return the lines it wrote to its result file. The child's streams are captured and dropped."""
    with tempfile.TemporaryDirectory(prefix="startup_check_") as name:
        folder = Path(name)
        (folder / REQUEST_NAME).write_text(json.dumps(request), encoding="utf-8")
        done = run_command(
            sys.executable, ["-s", "-B", "-P", "-m", CHILD_MODULE, name], cwd=folder, env=child_environment(name), timeout=CHILD_SECONDS, errors="replace"
        )
        results = folder / RESULT_NAME
        if done.returncode != 0 or not results.is_file() or results.stat().st_size > MAX_RESULT_BYTES:
            raise InvalidRecords
        return results.read_text(encoding="utf-8").splitlines()


def run_checks(request: dict[str, Any]) -> list[dict[str, Any]]:
    """The validated transcript of one request. In this process the same records go through the same strict parser."""
    return parse_records(run_child(request) if ISOLATED else dump(execute(request)))


# The root of a need that stands for a runnable plan mode that has no file list here, so the check refuses it instead of passing it unchecked.
UNENUMERATED = "unenumerated"


@dataclass(frozen=True)
class Need:
    """One file that a collector mode reads: the mode, the root that its reader resolves the path from (``code`` or ``mirror``), the path and its kind.

    A ``nonempty`` need is a JSON listing that must hold at least one entry. A ``plan`` or ``lock`` need must also read as a JSON object."""

    owner: str
    root: str
    path: str
    nonempty: bool = False
    kind: str = "input"


def read_json(path: str | Path) -> Mapping | None:
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return document if isinstance(document, dict) else None


def legacy_registry_files(owner: str, registry_sha256: object) -> list[Need]:
    """The legacy registry files at the code root that a plan bound to an older registry loads, from the committed version catalog."""
    registry = importlib.import_module("scripts.acquisition.source_registry")
    catalog = read_json(registry.VERSION_CATALOG_PATH) or {}
    if not registry_sha256 or registry_sha256 == catalog.get("current_registry_sha256"):
        return []
    versions = [item for item in catalog.get("legacy_versions", []) if item.get("registry_sha256") == registry_sha256]
    return [Need(owner, "code", entry["path"], kind="legacy_registry") for version in versions for entry in version.get("files", {}).values()]


def plan_needs(owner: str, plan_path: str | Path, *, lock: bool = True) -> list[Need]:
    """The code root files that one locked plan reads while it loads: the plan, its lock, its legacy registry, the references it lists, its access
    release and its terms.

    The plan and the lock come first, so a plan that cannot be read fails by name instead of dropping the files it would have listed."""
    current = importlib.import_module("scripts.acquisition.data_paths").current
    needs = [Need(owner, "code", str(plan_path), kind="plan")]
    if lock:
        needs.append(Need(owner, "code", str(Path(plan_path).with_suffix(".lock.json")), kind="lock"))
    plan = read_json(plan_path)
    if plan is None:
        return needs
    references = plan.get("references") or []
    references = list(references.values()) if isinstance(references, dict) else references
    for field, kind in (("access_release", "release"), ("terms", "terms")):
        record = plan.get(field)
        if isinstance(record, dict) and isinstance(record.get("path"), str):
            needs.append(Need(owner, "code", record["path"] if field == "access_release" else str(current(record["path"])), kind=kind))
    return [
        *needs,
        *legacy_registry_files(owner, plan.get("registry_sha256")),
        *(Need(owner, "code", str(current(item["path"])), kind="reference") for item in references),
    ]


def mmd_needs() -> list[Need]:
    """The code root files that ``mmd_api_contract.load_plan`` and ``collect_mmd_api.task_inputs`` read, from the contract's plan list and the plans."""
    collect_mmd_api = importlib.import_module("scripts.acquisition.collect_mmd_api")
    contract = importlib.import_module("scripts.acquisition.mmd_api_contract")
    current = importlib.import_module("scripts.acquisition.data_paths").current
    needs: list[Need] = []
    for sha, path in contract.PLANS.items():
        needs.extend(plan_needs("mmd_api", path, lock=False))
        plan = read_json(path)
        if plan is None:
            continue
        report = str(current(plan.get("comparison_report", "")))
        needs.append(Need("mmd_api", "code", report, kind="report"))
        needs.extend(Need("mmd_api", "code", str(current(item["original_path"])), kind="original") for item in (read_json(report) or {}).get("years", []))
        if sha == contract.ADDITIONS_PLAN_SHA256 and not legacy_registry_files("mmd_api", plan.get("registry_sha256")):
            needs.append(Need("mmd_api", "code", str(contract.ADDITIONS_LOCK_PATH), kind="additions_lock"))
    needs.append(Need("mmd_api", "code", str(collect_mmd_api.TEMPLATE), kind="template"))
    return needs


def cms_plans() -> list[Path]:
    return [importlib.import_module("scripts.acquisition.cms_owners_contract").PLAN_PATH]


def onc_plans() -> list[Path]:
    return [importlib.import_module("scripts.acquisition.onc_mu_contract").PLAN_PATH]


def bls_plans() -> list[Path]:
    return [importlib.import_module("scripts.acquisition.bls_api_contract").PLAN_PATH]


def hud_plans() -> list[Path]:
    contract = importlib.import_module("scripts.acquisition.hud_api_contract")
    return [contract.PLAN_PATH, contract.RANGE_PLAN_PATH]


def census_plans() -> list[Path]:
    contract = importlib.import_module("scripts.acquisition.census_acs_api_contract")
    return [contract.PLAN_PATH, contract.PR_PLAN_PATH]


def census_detailed_plans() -> list[Path]:
    return [importlib.import_module("scripts.acquisition.census_acs_detailed_contract").PLAN_PATH]


def hcai_plans() -> list[Path]:
    return [importlib.import_module("scripts.acquisition.hcai_util_contract").PLAN_PATH]


# The locked plans of each plan-based collector, from the plan paths its contract holds.
PLAN_PATHS = {
    "cms_owners_org": cms_plans,
    "onc_mu_hospital": onc_plans,
    "bls_api_v2": bls_plans,
    "hud_api": hud_plans,
    "census_acs_api": census_plans,
    "census_acs_detailed": census_detailed_plans,
    "hcai_util_workbook": hcai_plans,
}


def contract_needs(mode: str) -> list[Need]:
    return [need for path in PLAN_PATHS[mode]() for need in plan_needs(mode, path)]


# The file lists the publisher owns, by collector mode: what the mode's locked plans read at the code root while they load.
MODE_FILES = {"mmd_api": mmd_needs, **{mode: partial(contract_needs, mode) for mode in PLAN_PATHS}}
CONFIG_PREFIX = "config/acquisition/"


def mirror_config_needs(mode: str, controls: Path) -> list[Need]:
    """The operational inputs under ``config/acquisition/`` that the controls bind, read from the mirror root, or the controls file if unreadable."""
    document = read_json(controls)
    if document is None:
        return [Need(mode, "mirror", str(controls), kind="controls")]
    return [Need(mode, "mirror", key, kind="config_input") for key in sorted(document.get("operational_sha256", {})) if key.startswith(CONFIG_PREFIX)]


def runnable_plan_modes() -> frozenset[str]:
    """The plan modes that the harness can run, from the runtime's own lists: the planned modes and the staged plan modes, without the manual ones."""
    redownload = importlib.import_module("scripts.acquisition.redownload")
    stored_mirror = importlib.import_module("scripts.acquisition.stored_mirror")
    return frozenset((set(redownload.PLANNED) | set(stored_mirror.PLAN_MODES)) - redownload.MANUAL_MODES)


def queue_files(document: Mapping, *, controls: Path | None = None, keys: bool = False) -> list[Need]:
    """Every file that the modes this task attempts read, by the lane and handler rule of the harness, and the plan listing that the plan modes need.

    A keyed mode counts only when the task has keys. ``controls`` is the controls file beside the queue."""
    redownload = importlib.import_module("scripts.acquisition.redownload")
    stored_mirror = importlib.import_module("scripts.acquisition.stored_mirror")
    units = [
        {"flags": [], **item}
        for item in document.get("queue", [])
        if isinstance(item, dict) and item.get("disposition") in stored_mirror.ACTIVE and "mode" in item
    ]
    lanes = {redownload.TASK, *([redownload.TASK_KEYED] if keys else [])}
    modes = sorted({redownload.handler_mode(item) for item in units if redownload.lane_of(item) in lanes})
    runnable = runnable_plan_modes()
    # A mode in ``redownload.PLANNED`` re-hashes every operational input of the controls from the mirror root before it plans (``redownload.planned``).
    controlled = set(redownload.PLANNED) - redownload.MANUAL_MODES
    needs: list[Need] = []
    for mode in modes:
        if mode in MODE_FILES:
            needs.extend(MODE_FILES[mode]())
        elif mode in runnable:
            needs.append(Need(mode, UNENUMERATED, ""))
        if mode in controlled and controls is not None:
            needs.extend(mirror_config_needs(mode, controls))
    planned = [mode for mode in modes if mode in runnable]
    if planned:
        needs.append(Need("+".join(planned), "mirror", stored_mirror.PLAN_INPUTS_NAME, nonempty=True, kind="listing"))
    return list(dict.fromkeys(needs))


# ---- the child: the checks themselves, which return findings and write nothing ----


def in_ecs_job() -> bool:
    return ecs_engine.in_task() or any(os.environ.get(name) for name in ECS_MARKERS)


def check_task_environment() -> Finding:
    value = os.environ.get("AWS_EXECUTION_ENV", "")
    if ecs_engine.in_task():
        return Finding("task_environment", True, label("exec", value, phrase("a task value")))
    shown = phrase("unset") if not value else label("exec", value, phrase("an unrecognised value"))
    detail = join(
        variable("AWS_EXECUTION_ENV"),
        phrase("is"),
        shown,
        phrase("in an ECS task, so the job would not use task settings; expected"),
        label("expected", TASK_VALUES, phrase("a task value")),
    )
    return Finding("task_environment_unrecognised", False, detail)


def check_credentials(catalog: bool) -> Finding:
    try:
        ecs_engine.validate_task_credentials(require_catalog=catalog)
    except RuntimeError as error:
        code = ecs_engine.refusal_code(error, "task_credential_check_failed")
        return Finding("credentials", False, label("cred", code if code in CREDENTIAL_CODES else "task_credential_check_failed", phrase("unset")))
    return Finding("credentials", True, phrase("native task provider"))


def address_fault(text: str) -> Detail | None:
    """Why a service address has no usable host and port, as a fixed phrase, or ``None`` when it has both. The phrase never repeats any part of the address."""
    if not text:
        return phrase("is not set")
    try:
        address = urlsplit(text)
    except ValueError:
        return phrase("is not a valid address")
    try:
        port = address.port
    except ValueError:
        return phrase("has an invalid port")
    if not address.hostname:
        return phrase("names no host")
    return None if port or address.scheme in DEFAULT_PORTS else phrase("names no port")


def service_target(name: str, name_variable: str) -> tuple[str, str, int] | Finding:
    """The host and port of a service address in the environment, or the finding that says why it has none. Only the host and port are ever shown."""
    text = os.environ.get(name_variable, "")
    fault = address_fault(text)
    if fault:
        return Finding(f"{name}_address_unusable", False, join(variable(name_variable), fault))
    address = urlsplit(text)
    return name, str(address.hostname), address.port or DEFAULT_PORTS[address.scheme]


def probe(targets: list[tuple[str, str, int]]) -> list[Finding]:
    """Connect to every target at the same time. A connect that is not done at the shared bound fails as timed out, and no thread outlives the process."""
    outcomes: dict[int, tuple[BaseException | None, float]] = {}

    def attempt(index: int, host: str, port: int) -> None:
        started = time.monotonic()
        try:
            open_connection(host, port, CONNECT_SECONDS)
        except BaseException as error:
            outcomes[index] = (error, time.monotonic() - started)
        else:
            outcomes[index] = (None, time.monotonic() - started)

    threads = [threading.Thread(target=attempt, args=(index, host, port), daemon=True) for index, (_, host, port) in enumerate(targets)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + CONNECT_SECONDS
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    findings = []
    for index, (name, host, port) in enumerate(targets):
        outcome = outcomes.get(index)
        where = endpoint(host, port)
        if outcome is None:
            findings.append(Finding(f"{name}_unreachable", False, join(where, phrase("timed out after"), (("g", CONNECT_SECONDS),))))
        elif outcome[0] is not None:
            findings.append(Finding(f"{name}_unreachable", False, join(where, error_detail(outcome[0]))))
        else:
            findings.append(Finding(name, True, join(where, seconds(outcome[1]))))
    return findings


def open_connection(host: str, port: int, timeout: float) -> None:
    """Connect to a TCP port and close at once. The tests replace this seam."""
    socket.create_connection((host, port), timeout=timeout).close()


def endpoint_targets() -> list[tuple[str, str, int]] | Finding:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "")
    if not region:
        return Finding("region_unset", False, phrase("neither AWS_REGION nor AWS_DEFAULT_REGION is set"))
    if region not in AWS_REGIONS:
        return Finding("region_invalid", False, phrase("the region in AWS_REGION or AWS_DEFAULT_REGION is not a known AWS Region"))
    return [("s3", f"s3.{region}.amazonaws.com", 443), ("ecr", f"api.ecr.{region}.amazonaws.com", 443), ("ecr", f"dkr.ecr.{region}.amazonaws.com", 443)]


def redis_target() -> tuple[str, str, int] | Finding:
    online_protocol = importlib.import_module("scripts.features.online_protocol")
    target = service_target("redis", "POLARIS_URI")
    return Finding("redis_address_unusable", False, target.detail) if isinstance(target, Finding) else ("redis", target[1], online_protocol.PORT)


def environment_findings(*, polaris: bool, mlflow: bool, openmetadata: bool, redis: bool) -> list[Finding]:
    environment = check_task_environment()
    findings = [environment]
    if environment.ok:
        findings.append(check_credentials(catalog=polaris))
    targets: list[tuple[str, str, int]] = []
    for needed, name in ((polaris, "polaris"), (mlflow, "mlflow"), (openmetadata, "openmetadata")):
        if needed:
            target = service_target(name, SERVICE_VARIABLES[name])
            if isinstance(target, Finding):
                findings.append(target)
            else:
                targets.append(target)
    if redis:
        target = redis_target()
        if isinstance(target, Finding):
            findings.append(target)
        else:
            targets.append(target)
    endpoints = endpoint_targets()
    if isinstance(endpoints, Finding):
        findings.append(endpoints)
    else:
        targets.extend(endpoints)
    return [*findings, *probe(targets)]


def profile_output(profiles: Path, target: str) -> Mapping | None:
    yaml = importlib.import_module("yaml")
    document = yaml.safe_load(profiles.read_text(encoding="utf-8"))
    for profile in document.values():
        if isinstance(profile, dict) and target in (profile.get("outputs") or {}):
            return profile["outputs"][target]
    return None


def extension_label(name: str) -> Detail:
    return label("ext", name, phrase("an unlisted extension"))


def check_target(target: str, profiles: Path) -> Finding:
    """A target that lists DuckDB extensions must name an extension directory that holds them, because a job has no egress to download one."""
    shown = label("target", target, phrase("an unlisted target"))
    unreadable = Finding("dbt_profile_unreadable", False, phrase("the dbt profile cannot be read as a profile of outputs"))
    try:
        output = profile_output(profiles, target)
    except (OSError, ValueError, AttributeError, TypeError, importlib.import_module("yaml").YAMLError):
        return unreadable
    if output is None:
        return Finding("dbt_target_unknown", False, join(shown, phrase("is not an output of the dbt profile")))
    extensions = output.get("extensions") or []
    options = output.get("config_options") or {}
    if not isinstance(options, dict) or not isinstance(extensions, list):
        return unreadable
    directory = options.get("extension_directory")
    named = all(isinstance(name, str) and EXTENSION_NAME.fullmatch(name) for name in extensions)
    if not named or directory is not None and not isinstance(directory, str):
        return unreadable
    if not extensions:
        return Finding("dbt_target", True, join(shown, phrase("(no DuckDB extensions)")))
    names = join(*(extension_label(name) for name in extensions))
    if not directory:
        return Finding(
            "dbt_target_downloads_extensions",
            False,
            join(shown, phrase("lists"), names, phrase("and names no extension directory, so dbt would download them and the job has no egress")),
        )
    if not Path(directory).is_dir():
        return Finding("dbt_extension_directory_missing", False, join(shown, phrase("the extension directory does not exist in this image")))
    missing = [name for name in extensions if not any(Path(directory).rglob(f"{name}.duckdb_extension"))]
    if missing:
        return Finding(
            "dbt_extension_missing", False, join(shown, join(*(extension_label(name) for name in missing)), phrase("not in the extension directory"))
        )
    return Finding("dbt_target", True, join(shown, phrase("(extensions"), names, phrase("present)")))


def build_findings(layer: str, mode: str, profiles: Path) -> list[Finding]:
    """The target the build would select and what that target needs on disk. A features build writes its own profile."""
    pipeline_error = importlib.import_module("orchestration.pipeline_protocol").PipelineError
    pipeline_build = importlib.import_module("scripts.lakehouse.pipeline_build")

    findings = [check_task_environment()]
    if layer != "features":
        try:
            target = pipeline_build.build_target(layer, mode)
        except pipeline_error as error:
            code = ecs_engine.refusal_code(error, "the build target was refused")
            findings.append(Finding("dbt_target_refused", False, label("cred", code, phrase("the build target was refused"))))
        except ecs_engine.TemporaryStorageError:
            findings.append(Finding("dbt_target_refused", False, phrase(SPILL_CODE)))
        else:
            findings.append(check_target(target, profiles))
    return findings


def check_need(need: Need, base: Path) -> Finding:
    if need.root == UNENUMERATED:
        return Finding(
            "mode_files_unenumerated",
            False,
            join(mode_label(need.owner), phrase("is a runnable plan mode with no file list in the startup check, so its files would go unchecked")),
        )
    path = base / need.path
    where = join(mode_label(need.owner), label("root", need.root, phrase("an unlisted")), phrase("root:"), file_label(need.kind, need.path))
    if not need.nonempty:
        if not path.is_file():
            return Finding("input_file_missing", False, where)
        if need.kind in ("plan", "lock") and read_json(path) is None:
            return Finding("input_file_unreadable", False, join(where, phrase("does not read as a JSON object")))
        return Finding("input_file", True, where)
    if not path.is_file():
        return Finding(
            "plan_inputs_missing", False, join(where, phrase(": the staged listing of plan input files is absent, so no plan file reaches the code root"))
        )
    document = read_json(path)
    if not document:
        return Finding(
            "plan_inputs_empty", False, join(where, phrase(": the staged listing of plan input files holds no entry, so no plan file reaches the code root"))
        )
    return Finding("plan_inputs", True, join(where, phrase("("), count(len(document)), phrase("files)")))


def file_findings(files: Iterable[Need], roots: Mapping[str, Path], code: Path) -> list[Finding]:
    bases = {"code": code, **roots}
    findings = [check_need(need, bases.get(need.root, code)) for need in files]
    return findings or [Finding("input_files", True, phrase("none named"))]


def records_of(job: str, check_label: str, findings: list[Finding], started: float) -> list[dict[str, Any]]:
    """The records the child reports: one per finding, the summary and the end."""
    records: list[dict[str, Any]] = [{"k": "f", "c": item.name, "o": item.ok, "d": [list(part) for part in item.detail]} for item in findings]
    failed = sum(1 for item in findings if not item.ok)
    records.append(
        {
            "k": "s",
            "j": job if job in JOBS else "unlisted",
            "l": check_label,
            "n": len(findings),
            "x": failed,
            "t": round(time.monotonic() - started, 2),
        }
    )
    return [*records, {"k": "e"}]


def execute(request: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Run one request and return its records. This is the only code the child runs after its imports."""
    started = time.monotonic()
    kind, job = request["kind"], request["job"]
    code = Path(request["cwd"])
    if kind == "environment":
        return records_of(job, "environment", environment_findings(**request["flags"]), started)
    if kind == "build":
        return records_of(job, "build", build_findings(request["layer"], request["mode"], Path(request["profiles"])), started)
    roots = {name: Path(path) for name, path in request.get("roots", {}).items()}
    if kind == "queue":
        document = json.loads(Path(request["queue"]).read_text(encoding="utf-8"))
        controls = Path(request["controls"]) if request["controls"] else None
        needs = queue_files(document, controls=controls, keys=request["keys"])
    else:
        needs = [Need(**item) for item in request["needs"]]
    return records_of(job, "files", file_findings(needs, roots, code), started)


# ---- the parent: the entry points ----


def absolute(path: str | Path) -> str:
    return str(Path(path).absolute())


@guarded
def check_phase(job: str, phase: str) -> None:
    """Check the environment for one phase of a host job, with the services that ``JOB_PHASES`` lists for it. An unlisted phase checks nothing."""
    needs = JOB_PHASES[job].get(phase)
    if needs is not None:
        check_environment(job, polaris="polaris" in needs, mlflow="mlflow" in needs, openmetadata="openmetadata" in needs, redis="redis" in needs)


@guarded
def check_environment(job: str, *, polaris: bool = False, mlflow: bool = False, openmetadata: bool = False, redis: bool = False) -> None:
    """The first-step check: the task environment and credentials, the host services that the job needs, and S3 and ECR."""
    if not in_ecs_job():
        return
    flags = {"polaris": polaris, "mlflow": mlflow, "openmetadata": openmetadata, "redis": redis}
    emit(run_checks({"kind": "environment", "job": job, "cwd": absolute("."), "flags": flags}))


@guarded
def check_build(layer: str, mode: str, profiles: Path) -> None:
    """The check at the start of the build: the target it would select and what that target needs on disk. A features build writes its own profile."""
    if not in_ecs_job():
        return
    emit(run_checks({"kind": "build", "job": layer, "cwd": absolute("."), "layer": layer, "mode": mode, "profiles": absolute(profiles)}))


@guarded
def check_files(job: str, files: Iterable[Need], roots: Mapping[str, Path] | None = None) -> None:
    """Every named file exists under the root that its reader uses (``code`` defaults to the working folder). A listing must hold an entry."""
    if not in_ecs_job():
        return
    given = {name: absolute(path) for name, path in (roots or {}).items()}
    emit(run_checks({"kind": "files", "job": job, "cwd": absolute("."), "roots": given, "needs": [dataclasses.asdict(need) for need in files]}))


@guarded
def check_queue_files(job: str, queue: Path, *, controls: Path | None, keys: bool, roots: Mapping[str, Path] | None = None) -> None:
    """The file check of a publisher queue. Outside a task it reads neither the queue nor any collector plan."""
    if not in_ecs_job():
        return
    given = {name: absolute(path) for name, path in (roots or {}).items()}
    request = {
        "kind": "queue",
        "job": job,
        "cwd": absolute("."),
        "roots": given,
        "queue": absolute(queue),
        "controls": absolute(controls) if controls else "",
        "keys": keys,
    }
    emit(run_checks(request))
