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
path, with an environment that holds only the settings its request needs (a forbidden credential variable shows as a fixed presence marker, never its value),
in a private folder. (``-I`` cannot be used with ``-m`` and a path variable, so its effects are set by flags and the explicit environment.)
The child's standard output and error go to a pipe that the parent reads and never prints, so nothing that a dependency logs, warns, dumps or prints reaches
the job output. The child reports only reason codes: each finding is a code, its outcome and fixed phrases and names from the parent's own lists. It reports no
number, digest or address. The parent opens the result file once, without following a link, reads it up to a byte limit under one deadline, parses every record
strictly and binds the transcript to its request: it requires exactly the findings that the request makes mandatory, derives the counts and the elapsed time
itself, and adds any context (an endpoint; a file's kind with its repository-relative path when it is under the code root and passes the privacy
scan's home-path rule, else its digest) from its own inputs, never from the child. A queue run's findings show mode, root and kind only. Anything else,
an unparseable line, an unknown code, a missing or extra finding, a crash or a timeout, ends as ``startup_check_internal_error``.
The parent never calls the text conversion of an exception.

Every entry point is a job-entry boundary (``guarded``). It runs the check to its end, prints the fixed diagnostics, and raises one ``StartupCheckError`` that
holds fixed text and none of the request's arguments, from a frame that holds none, so a caller's diagnostic hook sees no raw path or value.
"""

from __future__ import annotations

import contextlib
import dataclasses
import errno
import functools
import hashlib
import importlib
import ipaddress
import json
import os
import re
import socket
import stat
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

from scripts import process
from scripts.lakehouse import ecs_engine
from scripts.process import run_command

# One bound for all connects of a check. They run side by side, so a check adds at most this and a little more.
CONNECT_SECONDS = 3.0
# The bound of the child process: its imports, its connects and its file reads.
CHILD_SECONDS = 60.0
# The part of that bound that is kept after the child and the read of its result stop. It holds three budgets in order: the TERM grace of the child (short,
# because it is disposable), the launcher's whole bound for confirming that the child is gone after KILL (``scripts.process.CONFIRM_SECONDS``, read when it is
# used), and ``DELETE_SECONDS`` to delete the private folder, which is the nonzero budget that confirmation cannot take. A short bound keeps half of it.
DELETE_SECONDS = 3.0
CHILD_STOP_GRACE_SECONDS = 0.5
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
MAX_PARTS = 32
MAX_RESULT_BYTES = 2_000_000
# What the child sees of a forbidden credential variable that is set: the marker, never the value. The validator needs only the presence.
PRESENT = "1"
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
# The label of each kind of request in the summary line. The parent takes it from the request, never from the child.
CHECK_LABELS = {"environment": "environment", "build": "build", "files": "files", "queue": "files"}
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
        "none named",
        "an unlisted mode",
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
# Repository filename syntax: ASCII letters, digits, dots, underscores, hyphens and slashes only.
FILENAME_SYNTAX = re.compile(r"[A-Za-z0-9._/-]+", re.ASCII)
# What a finding's code says of its outcome. ``credentials`` has both: its reason is the child's label.
PASSING = frozenset({*SERVICES, "task_environment", "dbt_target", "input_file", "plan_inputs", "input_files"})
EITHER = frozenset({"credentials"})
BUILD_CODES = frozenset(
    {
        "dbt_target_refused",
        "dbt_profile_unreadable",
        "dbt_target_unknown",
        "dbt_target",
        "dbt_target_downloads_extensions",
        "dbt_extension_directory_missing",
        "dbt_extension_missing",
    }
)
FILE_CODES = frozenset(
    {
        "input_file",
        "input_file_missing",
        "input_file_unreadable",
        "plan_inputs",
        "plan_inputs_missing",
        "plan_inputs_empty",
        "input_files",
        "mode_files_unenumerated",
    }
)
CHILD_CODES = CODES - {"startup_check_internal_error"}


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


def endpoint(host: str, port: int) -> Detail:
    """A host and port from the parent's own inputs. The host shows only as an IP address or an AWS service endpoint of a known Region, else as a digest."""
    if "%" not in host:
        try:
            return (("h", "ip", ipaddress.ip_address(host).compressed, int(port)),)
        except ValueError:
            pass
    if AWS_HOST.fullmatch(host):
        return (("h", "aws", host, int(port)),)
    return (("h", "digest", digest(host), int(port)),)


def known_mode(owner: str) -> bool:
    return bool(owner) and all(part in KNOWN_MODES for part in owner.split("+"))


def mode_label(owner: str) -> Detail:
    """A collector mode, or modes joined by a plus sign, when the harness knows each; else a digest. Only the parent uses it, on a need that it holds."""
    return (("m", owner if known_mode(owner) else f"mode#{digest(owner)}"),)


def file_label(kind: str, path: str) -> Detail:
    """A file by its kind and the digest of its path, because a configured path may carry anything. Only the parent uses it, on a need that it holds."""
    return (("f", kind if kind in FILE_KINDS else "input", digest(path)),)


def mode_part(owner: str) -> Detail:
    """The mode as the child may name it: a listed mode, else a fixed phrase. It never carries a digest."""
    return (("m", owner),) if known_mode(owner) else phrase("an unlisted mode")


def kind_part(kind: str) -> Detail:
    return (("f", kind if kind in FILE_KINDS else "input"),)


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
    """One check's result: its code, its outcome, the reason in fixed phrases and, for a file, where it is by lists only."""

    name: str
    ok: bool
    detail: Detail
    where: Detail = ()

    def __post_init__(self) -> None:
        if self.name not in CODES or not isinstance(self.detail, tuple):
            raise TypeError("a finding takes a listed code and a detail of parts")


# ---- the parent: strict parsing of the child's records, the binding to the request, and the text it prints, from its own lists and inputs ----

Report = tuple[str, bool, str]


def valid_part(part: object) -> Part:
    """One part of a detail, or ``InvalidRecords``. Each kind has its own shape and its own list. No part carries a number, a digest or an address."""
    if type(part) is not list or not part or type(part[0]) is not str:
        raise InvalidRecords
    kind, *values = part
    shapes = {"p": 1, "v": 1, "l": 2, "m": 1, "f": 1, "r": 1}
    if kind not in shapes or len(values) != shapes[kind]:
        raise InvalidRecords
    if kind == "p":
        ok = values[0] in PHRASES
    elif kind == "v":
        ok = values[0] in VARIABLES
    elif kind == "l":
        ok = type(values[0]) is str and values[0] in LABEL_SETS and values[1] in LABEL_SETS[values[0]]
    elif kind == "m":
        ok = type(values[0]) is str and known_mode(values[0])
    elif kind == "f":
        ok = values[0] in FILE_KINDS
    else:
        ok = values[0] in ERROR_NAMES
    if not ok:
        raise InvalidRecords
    return tuple(part)


def valid_parts(parts: object) -> Detail:
    if type(parts) is not list or len(parts) > MAX_PARTS:
        raise InvalidRecords
    return tuple(valid_part(part) for part in parts)


def valid_record(record: object) -> dict[str, Any]:
    """One record of the transcript: a finding or the end, with exactly its keys. A finding's outcome must agree with its code."""
    if type(record) is not dict:
        raise InvalidRecords
    kind = record.get("k")
    if kind == "e" and set(record) == {"k"}:
        return {"k": "e"}
    if kind == "f" and set(record) == {"k", "c", "o", "d", "w"}:
        code, passed = record["c"], record["o"]
        if code not in CHILD_CODES or type(passed) is not bool or (code not in EITHER and passed != (code in PASSING)):
            raise InvalidRecords
        return {"k": "f", "c": code, "o": passed, "d": valid_parts(record["d"]), "w": valid_parts(record["w"])}
    raise InvalidRecords


def reject_constant(_name: str) -> None:
    raise InvalidRecords


def parse_records(lines: list[str]) -> list[dict[str, Any]]:
    """The findings of the child's transcript, or ``InvalidRecords``: findings, then one end record, and nothing after it."""
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
    if records[-1]["k"] != "e" or any(record["k"] != "f" for record in records[:-1]):
        raise InvalidRecords
    return records[:-1]


def text_of(part: Part) -> str:
    """The text of one part. A part of the child is validated against the lists; a part of the parent comes from its own inputs."""
    kind = part[0]
    if kind == "h":
        return f"host#{part[2]}:{part[3]}" if part[1] == "digest" else f"{part[2]}:{part[3]}"
    if kind == "f":
        return " ".join(str(item) for item in part[1:])
    if kind == "l":
        return str(part[2])
    return str(part[1])


def render(detail: Iterable[Part]) -> str:
    return " ".join(text_of(part) for part in detail)


def same(record: Mapping[str, Any], finding: Finding) -> bool:
    return record["c"] == finding.name and record["o"] == finding.ok


def bind_environment(flags: Mapping[str, bool], findings: list[dict[str, Any]]) -> list[Report]:
    """The findings of an environment request: the ones that the parent's own environment settles, the credentials, and one per connect it expects."""
    leading, targets = environment_plan(**flags)
    if len(findings) != len(leading) + len(targets):
        raise InvalidRecords
    reports: list[Report] = []
    for record, mine in zip(findings, leading, strict=False):
        if mine is None:
            if record["c"] != "credentials":
                raise InvalidRecords
            reports.append((record["c"], record["o"], render(record["d"])))
        elif same(record, mine):
            reports.append((mine.name, mine.ok, render(mine.detail)))
        else:
            raise InvalidRecords
    for record, (name, host, port) in zip(findings[len(leading) :], targets, strict=True):
        if record["c"] not in (name, f"{name}_unreachable"):
            raise InvalidRecords
        reason = f" {CONNECT_SECONDS:g} s" if ("p", "timed out after") in record["d"] else ""
        reports.append((record["c"], record["o"], render((*endpoint(host, port), *record["d"])) + reason))
    return reports


def bind_build(layer: str, findings: list[dict[str, Any]]) -> list[Report]:
    """A build request: the task environment, from the parent's own environment, and, unless the layer writes its own profile, one target finding."""
    environment = check_task_environment()
    if len(findings) != (1 if layer == "features" else 2) or not same(findings[0], environment):
        raise InvalidRecords
    reports = [(environment.name, environment.ok, render(environment.detail))]
    if layer != "features":
        if findings[1]["c"] not in BUILD_CODES:
            raise InvalidRecords
        reports.append((findings[1]["c"], findings[1]["o"], render(findings[1]["d"])))
    return reports


def need_codes(need: Need) -> frozenset[str]:
    if need.root == UNENUMERATED:
        return frozenset({"mode_files_unenumerated"})
    if need.nonempty:
        return frozenset({"plan_inputs", "plan_inputs_missing", "plan_inputs_empty"})
    return frozenset({"input_file", "input_file_missing"} | ({"input_file_unreadable"} if need.kind in ("plan", "lock") else set()))


def relative_path(need: Need, code: Path) -> str | None:
    """The path of a code root need below the code root, as text that may be shown, or ``None``.

    It shows only a path that is under the code root and written in repository filename syntax: ASCII letters, digits, dots, underscores, hyphens and slashes,
    with no empty, ``.`` or ``..`` segment. Anything else (a query, an escape, a backslash, a look-alike character, a space) is never shown, because such a
    form can carry a credential that no list of forms will cover. The privacy scan's line check, which is the scan's own function, must also report nothing
    of any kind on the exact text that would print."""
    if need.root != "code":
        return None
    root = os.path.normpath(code).rstrip(os.sep) + os.sep
    text = need.path[len(root) :] if os.path.isabs(need.path) and need.path.startswith(root) else need.path
    if not FILENAME_SYNTAX.fullmatch(text) or any(segment in ("", ".", "..") for segment in text.split("/")):
        return None
    try:
        findings = importlib.import_module("scripts.quality.repo_checks").line_privacy(f"{need.kind if need.kind in FILE_KINDS else 'input'} {text}", [])
    except ImportError:
        return None
    return None if findings else text


def need_where(need: Need, code: Path) -> Detail:
    """Where a need is, from the parent's own copy of it: its mode and root, and its kind with the relative path or, when that may not show, the digest."""
    if need.root == UNENUMERATED:
        return mode_label(need.owner)
    where = join(mode_label(need.owner), label("root", need.root, phrase("an unlisted")), phrase("root:"))
    shown = relative_path(need, code)
    return join(where, (("f", need.kind if need.kind in FILE_KINDS else "input", shown),) if shown else file_label(need.kind, need.path))


def bind_files(needs: list[Need], code: Path, findings: list[dict[str, Any]]) -> list[Report]:
    """A files request: one finding per need, in order, with a code that its need allows, and where each is from the parent's own needs."""
    if not needs:
        if len(findings) != 1 or findings[0]["c"] != "input_files":
            raise InvalidRecords
        return [("input_files", True, render(findings[0]["d"]))]
    if len(findings) != len(needs):
        raise InvalidRecords
    reports = []
    for record, need in zip(findings, needs, strict=True):
        if record["c"] not in need_codes(need):
            raise InvalidRecords
        reports.append((record["c"], record["o"], render((*need_where(need, code), *record["d"]))))
    return reports


def bind_queue(findings: list[dict[str, Any]]) -> list[Report]:
    """A queue request: the parent holds no needs, so it takes one or more file findings, each placed by the child's lists only, with no digest."""
    if not findings or any(record["c"] not in FILE_CODES for record in findings):
        raise InvalidRecords
    return [(record["c"], record["o"], render((*record["w"], *record["d"]))) for record in findings]


def bind(request: Mapping[str, Any], findings: list[dict[str, Any]]) -> list[Report]:
    """The reports of a validated transcript, bound to the request that was made, or ``InvalidRecords`` when the transcript is not the one it requires."""
    kind = request["kind"]
    if kind == "environment":
        return bind_environment(request["flags"], findings)
    if kind == "build":
        return bind_build(request["layer"], findings)
    if kind == "files":
        return bind_files([Need(**item) for item in request["needs"]], Path(request.get("roots", {}).get("code") or request["cwd"]), findings)
    return bind_queue(findings)


def line_of(code: str, text: str) -> str:
    return f"{code} {text}".rstrip()


def emit(request: Mapping[str, Any], reports: list[Report], elapsed: float) -> str | None:
    """Print the bound reports on stderr, failures first, then the summary from the parent's own counts. Return the failure message, or ``None``."""
    failed = [report for report in reports if not report[1]]
    for code, _, text in failed:
        sys.stderr.write(f"startup_check: FAILED {line_of(code, text)}\n")
    for code, passed, text in reports:
        if passed:
            sys.stderr.write(f"startup_check: ok {line_of(code, text)}\n")
    job = request["job"] if request["job"] in JOBS else "unlisted"
    sys.stderr.write(f"startup_check: {job} {CHECK_LABELS[request['kind']]}: {len(reports)} checks, {len(failed)} failed, {elapsed:.1f} s\n")
    sys.stderr.flush()
    return "startup check failed: " + "; ".join(line_of(code, text) for code, _, text in failed) if failed else None


INTERNAL_ERROR = "startup check failed: startup_check_internal_error"


def shielded(function: Callable[..., str | None], arguments: tuple[Any, ...], keywords: dict[str, Any]) -> str | None:
    """Run one entry point and return its failure message. Any error ends as the fixed internal error: no message, no chained error and no traceback leaves."""
    try:
        return function(*arguments, **keywords)
    except Exception:
        sys.stderr.write("startup_check: FAILED startup_check_internal_error\n")
        sys.stderr.flush()
        return INTERNAL_ERROR


def guarded[**P](function: Callable[P, str | None]) -> Callable[P, None]:
    """The job-entry boundary of an entry point. The check, the diagnostics and every failure (expected, internal and a timeout) end inside ``shielded``.

    The one error raised here holds fixed text, and this frame drops the request's arguments before it raises, so a hook that captures the locals of the
    frames between the job and the raise sees no raw path or value."""

    @functools.wraps(function)
    def wrapper(*arguments: P.args, **keywords: P.kwargs) -> None:
        message = shielded(function, arguments, keywords)
        del arguments, keywords
        if message is not None:
            raise StartupCheckError(message)

    return wrapper


def child_environment_names(request: Mapping[str, Any]) -> tuple[str, ...]:
    """The only variables that the child gets from this process, by name: the settings that its request needs, and the tests' additions."""
    kind = request["kind"]
    names: list[str] = []
    if kind == "environment":
        flags = request["flags"]
        names = ["AWS_EXECUTION_ENV", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_REGION", "AWS_DEFAULT_REGION"]
        names += [SERVICE_VARIABLES[name] for name in ("polaris", "mlflow", "openmetadata") if flags[name]]
        names += ["POLARIS_URI"] if flags["redis"] else []
    elif kind == "build":
        names = ["AWS_EXECUTION_ENV"]
        if request["layer"] != "features":
            names += ["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "HAI_DUCKDB_TEMP_DIRECTORY", "HAI_DUCKDB_MAX_TEMP_BYTES"]
            names += ["POLARIS_URI"] if request["layer"] == "silver" and request["mode"] == "real" else []
    return tuple(dict.fromkeys([*names, *CHILD_ENV_EXTRA]))


def child_environment(folder: str, request: Mapping[str, Any]) -> dict[str, str]:
    environment = {name: os.environ[name] for name in child_environment_names(request) if name in os.environ}
    if request["kind"] in ("environment", "build") and request.get("layer") != "features":
        environment.update({name: PRESENT for name in ecs_engine.FORBIDDEN_CREDENTIAL_VARIABLES if os.environ.get(name)})
    return {**environment, "PATH": "/usr/bin:/bin", "HOME": folder, "TMPDIR": folder, "PYTHONPATH": str(REPOSITORY)}


def dump(records: Iterable[dict[str, Any]]) -> list[str]:
    return [json.dumps(record, separators=(",", ":"), allow_nan=False) for record in records]


def remaining(deadline: float) -> float:
    return max(0.1, deadline - time.monotonic())


def write_request(directory: int, request: Mapping[str, Any]) -> None:
    descriptor = os.open(REQUEST_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(request))


def read_result(directory: int, deadline: float) -> list[str]:
    """Open the result file once, without following a link or blocking on a special file, check the descriptor and read at most the limit plus one."""
    try:
        descriptor = os.open(RESULT_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    except OSError:
        raise InvalidRecords from None
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise InvalidRecords
        chunks: list[bytes] = []
        total = 0
        while total <= MAX_RESULT_BYTES:
            if time.monotonic() > deadline:
                raise TimeoutError("the result was not read in time")
            block = os.read(descriptor, min(65536, MAX_RESULT_BYTES + 1 - total))
            if not block:
                break
            chunks.append(block)
            total += len(block)
    except OSError:
        raise InvalidRecords from None
    finally:
        os.close(descriptor)
    if total > MAX_RESULT_BYTES:
        raise InvalidRecords
    return b"".join(chunks).decode("utf-8").splitlines()


def remove_contents(directory: int, deadline: float) -> None:
    """Delete everything in the folder behind the descriptor, never following a link, and fail when the deadline has passed."""
    with os.scandir(directory) as entries:
        found = [(entry.name, entry.is_dir(follow_symlinks=False)) for entry in entries]
    for name, is_folder in found:
        if time.monotonic() > deadline:
            raise TimeoutError("the private folder was not removed in time")
        if is_folder:
            inner = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            try:
                remove_contents(inner, deadline)
            finally:
                os.close(inner)
            os.rmdir(name, dir_fd=directory)
        else:
            os.unlink(name, dir_fd=directory)


def run_child(request: dict[str, Any]) -> list[str]:
    """Run the request in the isolated child and return the lines it wrote to its result file. The child's streams are captured and dropped.

    One deadline covers the child, its stop, the read of its result and the removal of its folder, and nothing resets it. The child and the read stop at a
    cutoff that leaves the whole reserve before the deadline: the TERM grace, the launcher's full bound to confirm that the child is gone (the stop is refused
    when that cannot be confirmed) and the deletion budget. A confirmation that takes nearly all of its bound therefore still leaves the deletion its time. The
    folder is used through one descriptor, so renaming it or replacing a file in it changes nothing that this function opens."""
    deadline = time.monotonic() + CHILD_SECONDS
    reserve = min(CHILD_STOP_GRACE_SECONDS + process.CONFIRM_SECONDS + DELETE_SECONDS, CHILD_SECONDS / 2)
    cutoff = deadline - reserve
    grace = min(CHILD_STOP_GRACE_SECONDS, reserve / 10)
    name = tempfile.mkdtemp(prefix="startup_check_")
    try:
        directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            write_request(directory, request)
            done = run_command(
                sys.executable,
                ["-s", "-B", "-P", "-m", CHILD_MODULE, name],
                cwd=Path(name),
                env=child_environment(name, request),
                timeout=remaining(cutoff),
                grace_seconds=grace,
                errors="replace",
            )
            if done.returncode != 0:
                raise InvalidRecords
            return read_result(directory, cutoff)
        finally:
            try:
                remove_contents(directory, deadline)
            finally:
                os.close(directory)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.rmdir(name)


def run_checks(request: dict[str, Any]) -> str | None:
    """Run one request, validate and bind its transcript, print it and return the failure message, or ``None``. Without a child the same records are parsed."""
    started = time.monotonic()
    findings = parse_records(run_child(request) if ISOLATED else dump(execute(request)))
    return emit(request, bind(request, findings), time.monotonic() - started)


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
MODE_FILES: dict[str, Callable[[], list[Need]]] = {"mmd_api": mmd_needs, **{mode: partial(contract_needs, mode) for mode in PLAN_PATHS}}
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
    outcomes: dict[int, BaseException | None] = {}

    def attempt(index: int, host: str, port: int) -> None:
        try:
            open_connection(host, port, CONNECT_SECONDS)
        except BaseException as error:
            outcomes[index] = error
        else:
            outcomes[index] = None

    threads = [threading.Thread(target=attempt, args=(index, host, port), daemon=True) for index, (_, host, port) in enumerate(targets)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + CONNECT_SECONDS
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    findings = []
    for index, (name, _host, _port) in enumerate(targets):
        error = outcomes.get(index)
        if index not in outcomes:
            findings.append(Finding(f"{name}_unreachable", False, phrase("timed out after")))
        elif error is not None:
            findings.append(Finding(f"{name}_unreachable", False, error_detail(error)))
        else:
            findings.append(Finding(name, True, ()))
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


def environment_plan(*, polaris: bool, mlflow: bool, openmetadata: bool, redis: bool) -> tuple[list[Finding | None], list[tuple[str, str, int]]]:
    """What the environment settles without a connect, in the order of the findings, and the connects that follow it. The parent builds the same plan from its
    own environment to bind a transcript to its request. ``None`` stands for the credential check, which only the child makes."""
    environment = check_task_environment()
    findings: list[Finding | None] = [environment]
    if environment.ok:
        findings.append(None)
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
    return findings, targets


def environment_findings(*, polaris: bool, mlflow: bool, openmetadata: bool, redis: bool) -> list[Finding]:
    leading, targets = environment_plan(polaris=polaris, mlflow=mlflow, openmetadata=openmetadata, redis=redis)
    return [*(check_credentials(catalog=polaris) if item is None else item for item in leading), *probe(targets)]


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
        reason = phrase("is a runnable plan mode with no file list in the startup check, so its files would go unchecked")
        return Finding("mode_files_unenumerated", False, reason, mode_part(need.owner))
    path = base / need.path
    where = join(mode_part(need.owner), label("root", need.root, phrase("an unlisted")), phrase("root:"), kind_part(need.kind))
    if not need.nonempty:
        if not path.is_file():
            return Finding("input_file_missing", False, (), where)
        if need.kind in ("plan", "lock") and read_json(path) is None:
            return Finding("input_file_unreadable", False, phrase("does not read as a JSON object"), where)
        return Finding("input_file", True, (), where)
    if not path.is_file():
        return Finding("plan_inputs_missing", False, phrase(": the staged listing of plan input files is absent, so no plan file reaches the code root"), where)
    document = read_json(path)
    if not document:
        return Finding(
            "plan_inputs_empty", False, phrase(": the staged listing of plan input files holds no entry, so no plan file reaches the code root"), where
        )
    return Finding("plan_inputs", True, (), where)


def file_findings(files: Iterable[Need], roots: Mapping[str, Path], code: Path) -> list[Finding]:
    bases = {"code": code, **roots}
    findings = [check_need(need, bases.get(need.root, code)) for need in files]
    return findings or [Finding("input_files", True, phrase("none named"))]


def records_of(findings: list[Finding]) -> list[dict[str, Any]]:
    """The records the child reports: one per finding, in order, and the end. The child reports no count, no time and no job: the parent derives them."""
    records: list[dict[str, Any]] = [
        {"k": "f", "c": item.name, "o": item.ok, "d": [list(part) for part in item.detail], "w": [list(part) for part in item.where]} for item in findings
    ]
    return [*records, {"k": "e"}]


def execute(request: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Run one request and return its records. This is the only code the child runs after its imports."""
    kind = request["kind"]
    code = Path(request["cwd"])
    if kind == "environment":
        return records_of(environment_findings(**request["flags"]))
    if kind == "build":
        return records_of(build_findings(request["layer"], request["mode"], Path(request["profiles"])))
    roots = {name: Path(path) for name, path in request.get("roots", {}).items()}
    if kind == "queue":
        document = json.loads(Path(request["queue"]).read_text(encoding="utf-8"))
        controls = Path(request["controls"]) if request["controls"] else None
        needs = queue_files(document, controls=controls, keys=request["keys"])
    else:
        needs = [Need(**item) for item in request["needs"]]
    return records_of(file_findings(needs, roots, code))


# ---- the parent: the entry points ----


def absolute(path: str | Path) -> str:
    return str(Path(path).absolute())


def run_phase(job: str, phase: str) -> str | None:
    needs = JOB_PHASES[job].get(phase)
    if needs is None:
        return None
    return run_environment(job, polaris="polaris" in needs, mlflow="mlflow" in needs, openmetadata="openmetadata" in needs, redis="redis" in needs)


def run_environment(job: str, *, polaris: bool = False, mlflow: bool = False, openmetadata: bool = False, redis: bool = False) -> str | None:
    if not in_ecs_job():
        return None
    flags = {"polaris": polaris, "mlflow": mlflow, "openmetadata": openmetadata, "redis": redis}
    return run_checks({"kind": "environment", "job": job, "cwd": absolute("."), "flags": flags})


def run_build(layer: str, mode: str, profiles: Path) -> str | None:
    if not in_ecs_job():
        return None
    return run_checks({"kind": "build", "job": layer, "cwd": absolute("."), "layer": layer, "mode": mode, "profiles": absolute(profiles)})


def run_files(job: str, files: Iterable[Need], roots: Mapping[str, Path] | None = None) -> str | None:
    if not in_ecs_job():
        return None
    given = {name: absolute(path) for name, path in (roots or {}).items()}
    return run_checks({"kind": "files", "job": job, "cwd": absolute("."), "roots": given, "needs": [dataclasses.asdict(need) for need in files]})


def run_queue(job: str, queue: Path, *, controls: Path | None, keys: bool, roots: Mapping[str, Path] | None = None) -> str | None:
    if not in_ecs_job():
        return None
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
    return run_checks(request)


# Each entry point runs inside the boundary. A failure ends as its fixed text and one ``StartupCheckError``; outside an ECS task it checks nothing.
# ``check_phase``: the environment for one phase of a host job, with the services that ``JOB_PHASES`` lists for it. An unlisted phase checks nothing.
# ``check_environment``: the first-step check: the task environment and credentials, the host services that the job needs, and S3 and ECR.
# ``check_build``: the check at the start of the build: the target it would select and what that target needs on disk. A features build writes its own profile.
# ``check_files``: every named file exists under the root that its reader uses (``code`` is the working folder). A listing must hold an entry.
# ``check_queue_files``: the file check of a publisher queue. Outside a task it reads neither the queue nor any collector plan.
check_phase = guarded(run_phase)
check_environment = guarded(run_environment)
check_build = guarded(run_build)
check_files = guarded(run_files)
check_queue_files = guarded(run_queue)
