"""Expose the live capacity of the container host and the live health of OpenMetadata to Prometheus.

    python -m monitoring.host_state_exporter [--listen 127.0.0.1:9103] [--interval 120]

The monitoring task runs it from the code checkout in the Airflow image (boto3 and the standard library only). Two sources are read on a timer by a background
thread, and a scrape only renders the last result, so Prometheus never waits for AWS:

- ``host``: the container instances of the cluster (``ecs:ListContainerInstances`` and ``ecs:DescribeContainerInstances``) and the running tasks
  (``ecs:ListTasks``). These are the numbers the launcher places a job by, not stored records. Only ACTIVE instances count.
- ``openmetadata``: the ``/healthcheck`` of the OpenMetadata admin port on loopback, the path the ECS health check of that service already uses.

A source that fails is marked down (``host_state_exporter_source_up`` 0, ``host_state_exporter_source_errors_total`` up by one) and its series are
dropped, so a panel shows no data and the alert fires; old numbers are never served as current. Every series is in ``CATALOG`` with a closed label set,
and a sample outside it is dropped.
Environment: ``ECS_CLUSTER``, ``AWS_REGION``, ``HAI_LAUNCH_HOST_JOB_CAP_MIB`` (the value the launch preflight reads) and ``OM_HEALTH_URL``.
Optional: ``EVIDENTLY_UI_ENABLED`` (true or false, default false: the Evidently UI probe on 8600 is reported disabled while false),
``OPENSEARCH_HEALTH_URL`` (the OpenSearch cluster status source exists only when set) and ``BOOTSTRAP_TASKS`` (comma-separated names of the one-shot
tasks whose last exit code and stop time are read from ECS).
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import http.client
import ipaddress
import json
import math
import os
import re
import shutil
import signal
import socket
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

LISTEN = "127.0.0.1:9103"
INTERVAL_SECONDS = 120.0
OM_TIMEOUT_SECONDS = 5.0
DEFAULT_OM_URL = "http://127.0.0.1:8586/healthcheck"
SOURCES = ("host", "services", "probes", "disk", "openmetadata", "opensearch", "bootstrap")
BOOTSTRAP_TASK_NAMES = ("code_fetch", "polaris_bootstrap", "openmetadata_bootstrap", "catalog_setup")
CHECK_NAME = re.compile(r"[a-z][a-z0-9_]{0,40}")
NAME = re.compile(r"[a-z][a-z0-9_]{0,40}")
STOP_CODES = ("TaskFailedToStart", "EssentialContainerExited", "UserInitiated", "ServiceSchedulerInitiated", "SpotInterruption", "TerminationNotice")
LABEL_VALUES: dict[str, Callable[[str], bool]] = {
    "check": lambda value: CHECK_NAME.fullmatch(value) is not None,
    "source": lambda value: value in SOURCES,
    "service": lambda value: NAME.fullmatch(value) is not None,
    "probe": lambda value: NAME.fullmatch(value) is not None,
    "stop_code": lambda value: value in (*STOP_CODES, "other"),
    "volume": lambda value: value in ("data", "scratch"),
    "task": lambda value: value in BOOTSTRAP_TASK_NAMES,
}
STOPPED_WINDOW = timedelta(hours=1)
STOPPED_LIMIT = 500
PROBE_TIMEOUT_SECONDS = 3.0
PROBE_URL = re.compile(r"http://127\.0\.0\.1:[0-9]{2,5}/[A-Za-z0-9_/.?=&-]*")
PATH = re.compile(r"/[A-Za-z0-9_./-]*")
CLUSTER = re.compile(r"[A-Za-z0-9_-]{1,255}")
REGION = re.compile(r"[a-z]{2}(?:-[a-z]+)+-[0-9]+")
OM_URL = re.compile(r"http://127\.0\.0\.1:[0-9]{2,5}/healthcheck")
OPENSEARCH_URL = re.compile(r"http://127\.0\.0\.1:[0-9]{2,5}/_cluster/health")
CATALOG: dict[str, tuple[str, tuple[str, ...], str]] = {
    "hai_host_instances": ("Active container instances in the cluster.", (), "gauge"),
    "hai_host_memory_registered_mib": ("Memory in MiB that ECS can place on the active instances.", (), "gauge"),
    "hai_host_memory_remaining_mib": ("Memory in MiB that ECS can still place on the active instances.", (), "gauge"),
    "hai_host_cpu_registered_units": ("CPU units that ECS can place on the active instances.", (), "gauge"),
    "hai_host_cpu_remaining_units": ("CPU units that ECS can still place on the active instances.", (), "gauge"),
    "hai_host_running_tasks": ("Tasks of the cluster that ECS reports as running.", (), "gauge"),
    "hai_host_job_cap_mib": ("The host job cap in MiB that the launch preflight reads.", (), "gauge"),
    "hai_service_tasks_desired": ("Tasks that an ECS service of the cluster should run.", ("service",), "gauge"),
    "hai_service_tasks_running": ("Tasks that an ECS service of the cluster runs.", ("service",), "gauge"),
    "hai_service_tasks_pending": ("Tasks that an ECS service of the cluster is starting.", ("service",), "gauge"),
    "hai_service_tasks_stopped_recent": (
        "Tasks of a service that ECS still lists as stopped, about the last hour, by stop code.",
        ("service", "stop_code"),
        "gauge",
    ),
    "hai_service_probe_up": ("1 when the health endpoint of a service answered with an accepted status.", ("probe", "service"), "gauge"),
    "hai_service_probe_seconds": ("Seconds the health endpoint of a service took to answer.", ("probe", "service"), "gauge"),
    "hai_host_disk_free_bytes": ("Free bytes of the volume.", ("volume",), "gauge"),
    "hai_host_disk_total_bytes": ("Size in bytes of the volume.", ("volume",), "gauge"),
    "hai_host_scratch_separate_volume": ("1 when the job scratch folder is on a volume of its own, 0 when it shares the data volume.", (), "gauge"),
    "hai_openmetadata_healthy": ("1 when every check of the OpenMetadata health check is healthy.", (), "gauge"),
    "hai_openmetadata_check_healthy": ("1 when one named check of the OpenMetadata health check is healthy.", ("check",), "gauge"),
    "hai_service_probe_disabled": (
        "1 when the service of an optional health probe is switched off, so its probe is not asked and is not down.",
        ("probe", "service"),
        "gauge",
    ),
    "hai_opensearch_cluster_status": ("Status of the OpenSearch cluster: 0 green, 1 yellow, 2 red.", (), "gauge"),
    "hai_bootstrap_task_last_exit_code": (
        "Exit code of the latest run of a one-shot bootstrap task: the first non-zero code of its containers, -1 when none was recorded.",
        ("task",),
        "gauge",
    ),
    "hai_bootstrap_task_last_run_timestamp_seconds": ("Unix time the latest run of a one-shot bootstrap task stopped.", ("task",), "gauge"),
    "host_state_exporter_source_up": ("1 when the latest read of the source succeeded.", ("source",), "gauge"),
    "host_state_exporter_source_errors_total": ("Reads of the source that failed since the exporter started.", ("source",), "counter"),
    "host_state_exporter_source_last_success_timestamp_seconds": ("Unix time of the latest successful read of the source.", ("source",), "gauge"),
}


class SourceError(Exception):
    """A source could not be read or answered something this exporter cannot use."""


@dataclass(frozen=True)
class Sample:
    name: str
    value: float
    labels: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Settings:
    cluster: str
    region: str
    job_cap_mib: int | None
    om_url: str
    service_prefix: str = ""
    data_path: str = ""
    scratch_path: str = ""
    evidently_ui: bool = False
    opensearch_url: str = ""
    bootstrap_tasks: tuple[str, ...] = ()


def settings(environ: Mapping[str, str]) -> Settings:
    """The exporter's settings from the environment, or a ValueError that names the variable."""
    cluster, region = environ.get("ECS_CLUSTER", ""), environ.get("AWS_REGION", "")
    if not CLUSTER.fullmatch(cluster):
        raise ValueError("ECS_CLUSTER must name the ECS cluster (letters, digits, dashes and underscores)")
    if not REGION.fullmatch(region):
        raise ValueError("AWS_REGION must be an AWS region name")
    cap = environ.get("HAI_LAUNCH_HOST_JOB_CAP_MIB", "")
    if cap and not re.fullmatch(r"[1-9][0-9]{0,8}", cap):
        raise ValueError("HAI_LAUNCH_HOST_JOB_CAP_MIB must be a whole number of MiB")
    url = environ.get("OM_HEALTH_URL", DEFAULT_OM_URL)
    if not OM_URL.fullmatch(url):
        raise ValueError("OM_HEALTH_URL must be a plain http address on 127.0.0.1 ending in /healthcheck")
    prefix = environ.get("ECS_SERVICE_PREFIX", "")
    if not re.fullmatch(r"[A-Za-z0-9_-]{0,100}", prefix):
        raise ValueError("ECS_SERVICE_PREFIX may hold letters, digits, dashes and underscores only")
    paths = {name: environ.get(name, "") for name in ("HOST_DATA_PATH", "HOST_SCRATCH_PATH")}
    for name, value in paths.items():
        if value and (not PATH.fullmatch(value) or ".." in value.split("/")):
            raise ValueError(f"{name} must be an absolute path without a parent step")
    return Settings(cluster, region, int(cap) if cap else None, url, prefix, paths["HOST_DATA_PATH"], paths["HOST_SCRATCH_PATH"], *gap_settings(environ))


def gap_settings(environ: Mapping[str, str]) -> tuple[bool, str, tuple[str, ...]]:
    """The Evidently UI switch, the OpenSearch health address and the one-shot task names, or a ValueError that names the variable."""
    switch = environ.get("EVIDENTLY_UI_ENABLED", "false")
    if switch not in ("true", "false"):
        raise ValueError("EVIDENTLY_UI_ENABLED must be true or false")
    url = environ.get("OPENSEARCH_HEALTH_URL", "")
    if url and not OPENSEARCH_URL.fullmatch(url):
        raise ValueError("OPENSEARCH_HEALTH_URL must be a plain http address on 127.0.0.1 ending in /_cluster/health")
    names = tuple(name for name in environ.get("BOOTSTRAP_TASKS", "").split(",") if name)
    if len(set(names)) != len(names) or any(name not in BOOTSTRAP_TASK_NAMES for name in names):
        raise ValueError("BOOTSTRAP_TASKS must list each of code_fetch, polaris_bootstrap and openmetadata_bootstrap at most once, separated by commas")
    return switch == "true", url, names


def ecs_client_config() -> Any:
    """The botocore settings of the ECS client: hard connect and read timeouts and two attempts at most."""
    from botocore.config import Config

    return Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 2, "mode": "standard"})


def resource(item: Mapping[str, Any], group: str, name: str) -> int:
    for entry in item.get(group, []):
        if entry.get("name") == name and isinstance(entry.get("integerValue"), int):
            return int(entry["integerValue"])
    raise SourceError(f"a container instance has no {name} in {group}")


def paged(call: Callable[..., Mapping[str, Any]], key: str, **arguments: Any) -> list[Any]:
    """Every item of a listing, following nextToken."""
    found: list[Any] = []
    token = ""
    for _ in range(100):
        reply = call(**arguments, **({"nextToken": token} if token else {}))
        found += list(reply.get(key, []))
        token = str(reply.get("nextToken") or "")
        if not token:
            return found
    raise SourceError("a listing did not end after 100 pages")


def host_samples(ecs: Any, cluster: str, job_cap_mib: int | None) -> list[Sample]:
    """The capacity series of the ACTIVE container instances and the running task count, from live ECS reads."""
    arns = paged(ecs.list_container_instances, "containerInstanceArns", cluster=cluster, status="ACTIVE")
    active: list[Mapping[str, Any]] = []
    for start in range(0, len(arns), 100):
        reply = ecs.describe_container_instances(cluster=cluster, containerInstances=arns[start : start + 100])
        if reply.get("failures"):
            raise SourceError("ECS reported a failure describing a container instance")
        active += [item for item in reply.get("containerInstances", []) if item.get("status") == "ACTIVE"]
    tasks = len(paged(ecs.list_tasks, "taskArns", cluster=cluster, desiredStatus="RUNNING"))
    samples = [Sample("hai_host_instances", float(len(active))), Sample("hai_host_running_tasks", float(tasks))]
    if active:
        for name, group, resource_name in (
            ("hai_host_memory_registered_mib", "registeredResources", "MEMORY"),
            ("hai_host_memory_remaining_mib", "remainingResources", "MEMORY"),
            ("hai_host_cpu_registered_units", "registeredResources", "CPU"),
            ("hai_host_cpu_remaining_units", "remainingResources", "CPU"),
        ):
            samples.append(Sample(name, float(sum(resource(item, group, resource_name) for item in active))))
    if job_cap_mib is not None:
        samples.append(Sample("hai_host_job_cap_mib", float(job_cap_mib)))
    return samples


def openmetadata_samples(url: str, timeout: float = OM_TIMEOUT_SECONDS) -> list[Sample]:
    """The health of OpenMetadata from its admin port. An unhealthy answer (the port answers 500 with the failing checks) is data, not an error."""
    parsed = urlparse(url)
    connection = http.client.HTTPConnection(parsed.hostname or "", parsed.port or 80, timeout=timeout)
    try:
        connection.request("GET", parsed.path or "/")
        raw = connection.getresponse().read()
    except Exception as error:
        raise SourceError(f"{type(error).__name__}") from None
    finally:
        connection.close()
    try:
        checks = json.loads(raw)
    except ValueError:
        raise SourceError("the health check answer is not JSON") from None
    if not isinstance(checks, dict) or not checks or not all(isinstance(item, dict) and isinstance(item.get("healthy"), bool) for item in checks.values()):
        raise SourceError("the health check answer has no checks with a boolean healthy")
    samples = [Sample("hai_openmetadata_healthy", 1.0 if all(item["healthy"] for item in checks.values()) else 0.0)]
    for name, item in checks.items():
        label = re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_")
        if CHECK_NAME.fullmatch(label):
            samples.append(Sample("hai_openmetadata_check_healthy", 1.0 if item["healthy"] else 0.0, {"check": label}))
    return samples


def short_name(name: str, prefix: str) -> str:
    return name[len(prefix) :] if prefix and name.startswith(prefix) else name


def service_samples(ecs: Any, cluster: str, prefix: str, now: datetime | None = None) -> list[Sample]:
    """The desired, running and pending tasks of each ECS service, and the stopped tasks that ECS still lists (about an hour), by service and stop code."""
    arns = paged(ecs.list_services, "serviceArns", cluster=cluster)
    samples: list[Sample] = []
    for start in range(0, len(arns), 10):
        reply = ecs.describe_services(cluster=cluster, services=arns[start : start + 10])
        if reply.get("failures"):
            raise SourceError("ECS reported a failure describing a service")
        for item in reply.get("services", []):
            name = short_name(str(item["serviceName"]), prefix)
            for metric, field_name in (
                ("hai_service_tasks_desired", "desiredCount"),
                ("hai_service_tasks_running", "runningCount"),
                ("hai_service_tasks_pending", "pendingCount"),
            ):
                samples.append(Sample(metric, float(item[field_name]), {"service": name}))
    stopped = paged(ecs.list_tasks, "taskArns", cluster=cluster, desiredStatus="STOPPED")[:STOPPED_LIMIT]
    cutoff = (now or datetime.now(UTC)) - STOPPED_WINDOW
    counts: dict[tuple[str, str], int] = {}
    for start in range(0, len(stopped), 100):
        reply = ecs.describe_tasks(cluster=cluster, tasks=stopped[start : start + 100])
        if reply.get("failures"):
            raise SourceError("ECS reported a failure describing a stopped task")
        for task in reply.get("tasks", []):
            group = str(task.get("group", ""))
            stopped_at = task.get("stoppedAt")
            if not group.startswith("service:") or not isinstance(stopped_at, datetime) or stopped_at < cutoff:
                continue
            code = str(task.get("stopCode", ""))
            key = (short_name(group.removeprefix("service:"), prefix), code if code in STOP_CODES else "other")
            counts[key] = counts.get(key, 0) + 1
    samples += [
        Sample("hai_service_tasks_stopped_recent", float(count), {"service": name, "stop_code": code}) for (name, code), count in sorted(counts.items())
    ]
    return samples


@dataclass(frozen=True)
class Probe:
    """One health endpoint on loopback: the ECS service it belongs to, its address and the statuses that count as up."""

    service: str
    url: str
    ok: tuple[int, ...] = (200,)


PROBES: dict[str, Probe] = {
    "polaris": Probe("polaris", "http://127.0.0.1:8182/q/health"),
    "mlflow": Probe("mlflow", "http://127.0.0.1:5100/health", (200, 401)),
    "airflow_api": Probe("airflow_apiserver", "http://127.0.0.1:8080/api/v2/version"),
    "airflow_scheduler": Probe("airflow_scheduler", "http://127.0.0.1:8974/health"),
    "model_api": Probe("model_api", "http://127.0.0.1:8000/v1/live"),
    "streamlit": Probe("streamlit", "http://127.0.0.1:8501/_stcore/health"),
    "opensearch": Probe("openmetadata", "http://127.0.0.1:9200/_cluster/health"),
}


def probe_samples(probes: Mapping[str, Probe], timeout: float = PROBE_TIMEOUT_SECONDS) -> list[Sample]:
    """Ask each endpoint once. A wrong status, a refused connection and a slow answer are all down; none of them is an error of the source."""
    samples: list[Sample] = []
    for name, probe in probes.items():
        parsed = urlparse(probe.url)
        labels = {"probe": name, "service": probe.service}
        started = time.monotonic()
        up = False
        connection = http.client.HTTPConnection(parsed.hostname or "", parsed.port or 80, timeout=timeout)
        try:
            connection.request("GET", (parsed.path or "/") + (f"?{parsed.query}" if parsed.query else ""))
            response = connection.getresponse()
            response.read()
            up = response.status in probe.ok
            samples.append(Sample("hai_service_probe_seconds", time.monotonic() - started, labels))
        except Exception:
            up = False
        finally:
            connection.close()
        samples.append(Sample("hai_service_probe_up", 1.0 if up else 0.0, labels))
    return samples


EVIDENTLY_PROBE = Probe("evidently_ui", "http://127.0.0.1:8600/api/version")


def probe_table(evidently_ui: bool) -> dict[str, Probe]:
    """The probes to ask: the default ones, and the Evidently UI only while its switch is on."""
    return {**PROBES, **({"evidently_ui": EVIDENTLY_PROBE} if evidently_ui else {})}


def disabled_probe_samples(evidently_ui: bool) -> list[Sample]:
    """The Evidently UI is reported disabled while its switch is off. It has no up series then, so it is never down."""
    if evidently_ui:
        return []
    return [Sample("hai_service_probe_disabled", 1.0, {"probe": "evidently_ui", "service": EVIDENTLY_PROBE.service})]


OPENSEARCH_STATUS = {"green": 0.0, "yellow": 1.0, "red": 2.0}


def deadline_get(url: str, timeout: float, limit: int = 1024 * 1024) -> tuple[int, bytes]:
    """One HTTP/1.0 GET with one absolute deadline over the connect, the send, the headers and every read of the body, so an answer that trickles in byte by
    byte ends at the deadline like a silent one. The host must be an IP address: a name is refused, so no name lookup runs outside the deadline. There is one
    connect attempt, with the time that is left. The answer is read to its end and cut at ``limit`` bytes. Raises SourceError."""
    parsed = urlparse(url)
    try:
        address = ipaddress.ip_address(parsed.hostname or "")
    except ValueError:
        raise SourceError("the address host is not an IP address") from None
    deadline = time.monotonic() + timeout

    def left() -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SourceError("TimeoutError")
        return remaining

    try:
        with socket.socket(socket.AF_INET6 if address.version == 6 else socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.settimeout(left())
            connection.connect((str(address), parsed.port or 80))
            connection.settimeout(left())
            connection.sendall(f"GET {parsed.path or '/'} HTTP/1.0\r\nHost: {address}\r\nConnection: close\r\n\r\n".encode("ascii"))
            raw = b""
            while len(raw) <= limit:
                connection.settimeout(left())
                chunk = connection.recv(65536)
                if not chunk:
                    break
                raw += chunk
    except SourceError:
        raise
    except Exception as error:
        raise SourceError(f"{type(error).__name__}") from None
    head, _, body = raw.partition(b"\r\n\r\n")
    parts = head.split(b"\r\n", 1)[0].split(b" ", 2)
    if len(raw) > limit or len(parts) < 2 or not parts[0].startswith(b"HTTP/") or not parts[1].isdigit():
        raise SourceError("the answer is not a bounded HTTP answer")
    return int(parts[1]), body


def opensearch_samples(url: str, timeout: float = OM_TIMEOUT_SECONDS) -> list[Sample]:
    """The cluster status of OpenSearch on its internal port, read-only, within one absolute deadline. Red is data; an unreadable answer is an error."""
    status, raw = deadline_get(url, timeout)
    if status != 200:
        raise SourceError(f"HTTP_{status}")
    try:
        colour = json.loads(raw).get("status")
    except (ValueError, AttributeError):
        raise SourceError("the cluster health answer is not a JSON object") from None
    if colour not in OPENSEARCH_STATUS:
        raise SourceError("the cluster health answer has no green, yellow or red status")
    return [Sample("hai_opensearch_cluster_status", OPENSEARCH_STATUS[colour])]


class BootstrapMemory:
    """The latest stopped run of each one-shot task. ECS lists a stopped task for about an hour only, so the exporter keeps what it last read."""

    def __init__(self) -> None:
        self.latest: dict[str, tuple[datetime, int]] = {}


def exit_code_of(task: Mapping[str, Any]) -> int:
    """The first non-zero exit code of the containers of a task, else -1 when a container has no exit code (it never ran) and 0 when all exited with 0."""
    codes = [container.get("exitCode") for container in task.get("containers", [])]
    failed = next((int(code) for code in codes if isinstance(code, int) and code != 0), None)
    if failed is not None:
        return failed
    return -1 if not codes or any(not isinstance(code, int) for code in codes) else 0


def bootstrap_samples(ecs: Any, cluster: str, prefix: str, names: Sequence[str], memory: BootstrapMemory) -> list[Sample]:
    """The last exit code and stop time of each one-shot task, from ListTasks and DescribeTasks of the task definition family, read-only."""
    for name in names:
        arns = paged(ecs.list_tasks, "taskArns", cluster=cluster, family=f"{prefix}{name}", desiredStatus="STOPPED")[:STOPPED_LIMIT]
        found: list[Mapping[str, Any]] = []
        for start in range(0, len(arns), 100):
            reply = ecs.describe_tasks(cluster=cluster, tasks=arns[start : start + 100])
            if reply.get("failures"):
                raise SourceError("ECS reported a failure describing a one-shot task")
            found += [task for task in reply.get("tasks", []) if isinstance(task.get("stoppedAt"), datetime)]
        if found:
            newest = max(found, key=lambda task: task["stoppedAt"])
            if name not in memory.latest or newest["stoppedAt"] >= memory.latest[name][0]:
                memory.latest[name] = (newest["stoppedAt"], exit_code_of(newest))
    samples: list[Sample] = []
    for name in names:
        if name in memory.latest:
            stopped_at, code = memory.latest[name]
            samples += [
                Sample("hai_bootstrap_task_last_exit_code", float(code), {"task": name}),
                Sample("hai_bootstrap_task_last_run_timestamp_seconds", stopped_at.timestamp(), {"task": name}),
            ]
    return samples


def disk_usage(path: str) -> tuple[int, int, int]:
    total, used, free = shutil.disk_usage(path)
    return total, used, free


def device_of(path: str) -> int:
    return os.stat(path).st_dev


def disk_samples(paths: Mapping[str, str], usage: Callable[[str], tuple[int, int, int]] = disk_usage, device: Callable[[str], int] = device_of) -> list[Sample]:
    """Free and total bytes of the data volume and of the job scratch folder, and whether the scratch folder has a volume of its own."""
    samples: list[Sample] = []
    try:
        for volume, path in paths.items():
            total, _, free = usage(path)
            samples += [
                Sample("hai_host_disk_free_bytes", float(free), {"volume": volume}),
                Sample("hai_host_disk_total_bytes", float(total), {"volume": volume}),
            ]
        if "data" in paths and "scratch" in paths:
            samples.append(Sample("hai_host_scratch_separate_volume", 1.0 if device(paths["data"]) != device(paths["scratch"]) else 0.0))
    except OSError as error:
        raise SourceError(type(error).__name__) from None
    return samples


MLFLOW_URL = re.compile(r"http://127\.0\.0\.1:[0-9]{2,5}/metrics")
RELAY_PATH = "/mlflow_metrics"
RELAY_LIMIT_BYTES = 4 * 1024 * 1024
RELAY_USER = "admin"


def relay_url_ok(url: str) -> bool:
    return MLFLOW_URL.fullmatch(url) is not None


def mlflow_relay(url: str, login_phrase: str, timeout: float = PROBE_TIMEOUT_SECONDS) -> Callable[[], tuple[int, bytes]]:
    """A function that asks MLflow for ``/metrics`` with the admin credential and returns (200, text), or (502, nothing) on any failure.

    MLflow answers ``/metrics`` to its admin user only. The credential lives in this process only: it is never written to a file, a log line or an answer, and
    the relay serves nothing but the relayed text.
    """
    parsed = urlparse(url)
    header = "Basic " + base64.b64encode(f"{RELAY_USER}:{login_phrase}".encode()).decode()

    def relay() -> tuple[int, bytes]:
        connection = http.client.HTTPConnection(parsed.hostname or "", parsed.port or 80, timeout=timeout)
        try:
            connection.request("GET", parsed.path, headers={"Authorization": header})
            response = connection.getresponse()
            body = response.read(RELAY_LIMIT_BYTES + 1)
            if response.status != 200 or len(body) > RELAY_LIMIT_BYTES:
                sys.stderr.write(f"event=exporter_relay_failed reason=HTTP_{response.status}\n")
                return 502, b""
            return 200, body
        except Exception as error:
            sys.stderr.write(f"event=exporter_relay_failed reason={type(error).__name__}\n")
            return 502, b""
        finally:
            connection.close()

    return relay


def collectors(configured: Settings, ecs: Any) -> dict[str, Callable[[], list[Sample]]]:
    """The sources of one exporter: the disk source exists only when both folders are named."""
    found: dict[str, Callable[[], list[Sample]]] = {
        "host": lambda: host_samples(ecs, configured.cluster, configured.job_cap_mib),
        "services": lambda: service_samples(ecs, configured.cluster, configured.service_prefix),
        "probes": lambda: probe_samples(probe_table(configured.evidently_ui)) + disabled_probe_samples(configured.evidently_ui),
        "openmetadata": lambda: openmetadata_samples(configured.om_url),
    }
    if configured.opensearch_url:
        found["opensearch"] = lambda: opensearch_samples(configured.opensearch_url)
    if configured.bootstrap_tasks:
        memory = BootstrapMemory()
        found["bootstrap"] = lambda: bootstrap_samples(ecs, configured.cluster, configured.service_prefix, configured.bootstrap_tasks, memory)
    if configured.data_path and configured.scratch_path:
        found["disk"] = lambda: disk_samples({"data": configured.data_path, "scratch": configured.scratch_path})
    return found


def problem(sample: Sample) -> bool:
    """True when the sample may not be exposed: not in the catalog, other labels than its set or a value that is not a finite number."""
    entry = CATALOG.get(sample.name)
    if entry is None or set(sample.labels) != set(entry[1]) or not math.isfinite(sample.value):
        return True
    return any(not LABEL_VALUES[label](str(value)) for label, value in sample.labels.items() if label in LABEL_VALUES)


def number(value: float) -> str:
    return str(int(value)) if float(value).is_integer() and abs(value) < 1e15 else repr(float(value))


class Exporter:
    """The last result of each source and the exposition of it."""

    def __init__(self, collectors: Mapping[str, Callable[[], list[Sample]]]) -> None:
        self.collectors = dict(collectors)
        self.lock = threading.Lock()
        self.data: dict[str, list[Sample]] = {}
        self.up: dict[str, bool] = {}
        self.errors: dict[str, int] = {name: 0 for name in self.collectors}
        self.succeeded: dict[str, float] = {}

    def refresh(self) -> None:
        """Read every source once. A source that fails is down and keeps no series; the others are read anyway."""
        for name, collect in self.collectors.items():
            try:
                found = [sample for sample in collect() if not problem(sample)]
                ok = True
            except Exception as error:
                found, ok = [], False
                sys.stderr.write(f"event=exporter_source_failed source={name} reason={type(error).__name__}\n")
            with self.lock:
                self.data[name], self.up[name] = found, ok
                if ok:
                    self.succeeded[name] = time.time()
                else:
                    self.errors[name] += 1

    def samples(self) -> list[Sample]:
        with self.lock:
            found = [sample for name in self.data for sample in self.data[name]]
            for name in self.up:
                found.append(Sample("host_state_exporter_source_up", 1.0 if self.up[name] else 0.0, {"source": name}))
                found.append(Sample("host_state_exporter_source_errors_total", float(self.errors[name]), {"source": name}))
                if name in self.succeeded:
                    found.append(Sample("host_state_exporter_source_last_success_timestamp_seconds", self.succeeded[name], {"source": name}))
        return found

    def render(self) -> str:
        order: dict[str, list[str]] = {}
        for sample in self.samples():
            labels = ",".join(f'{key}="{sample.labels[key]}"' for key in CATALOG[sample.name][1])
            order.setdefault(sample.name, []).append(f"{sample.name}{{{labels}}} {number(sample.value)}" if labels else f"{sample.name} {number(sample.value)}")
        lines: list[str] = []
        for name, rows in order.items():
            lines += [f"# HELP {name} {CATALOG[name][0]}", f"# TYPE {name} {CATALOG[name][2]}", *rows]
        return "\n".join(lines) + "\n" if lines else ""


class Running:
    """A started exporter: the listener, the refresh thread and the way to stop both."""

    def __init__(self, server: ThreadingHTTPServer, stopped: threading.Event, threads: Sequence[threading.Thread]) -> None:
        self.server, self.stopped, self.threads = server, stopped, threads

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def stop(self) -> None:
        self.stopped.set()
        self.server.shutdown()
        self.server.server_close()
        for thread in self.threads:
            thread.join(timeout=10)


def serve(exporter: Exporter, host: str, port: int, *, interval: float, relay: Callable[[], tuple[int, bytes]] | None = None) -> Running:
    """Listen on loopback and read the sources every ``interval`` seconds, the first read at once. Any other address is refused."""
    if host != "127.0.0.1":
        raise ValueError("the exporter listens on 127.0.0.1 only")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path = self.path.split("?")[0]
            if path == RELAY_PATH and relay is not None:
                status, payload = relay()
            elif path == "/metrics":
                status, payload = 200, exporter.render().encode()
            else:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    stopped = threading.Event()

    def refresh_loop() -> None:
        while not stopped.is_set():
            exporter.refresh()
            stopped.wait(interval)

    threads = [
        threading.Thread(target=server.serve_forever, name="host_state_exporter_server", daemon=True),
        threading.Thread(target=refresh_loop, name="host_state_exporter_refresh", daemon=True),
    ]
    for thread in threads:
        thread.start()
    return Running(server, stopped, threads)


def main(argv: Sequence[str] | None = None, environ: Mapping[str, str] | None = None) -> int:
    import os

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--listen", default=LISTEN, help="host:port, host 127.0.0.1")
    parser.add_argument("--interval", type=float, default=INTERVAL_SECONDS)
    arguments = parser.parse_args(argv)
    try:
        configured = settings(os.environ if environ is None else environ)
        host, _, port = arguments.listen.rpartition(":")
        import boto3

        ecs = boto3.client("ecs", region_name=configured.region, config=ecs_client_config())
        exporter = Exporter(collectors(configured, ecs))
        # The credential is read once and dropped from the environment, so no child process or dump can show it.
        login_phrase = (environ if environ is not None else os.environ).get("MLFLOW_ADMIN_PASSWORD", "")
        os.environ.pop("MLFLOW_ADMIN_PASSWORD", None)
        relay_address = (environ if environ is not None else os.environ).get("MLFLOW_METRICS_URL", "http://127.0.0.1:5100/metrics")
        relay = mlflow_relay(relay_address, login_phrase) if login_phrase and relay_url_ok(relay_address) else None
        running = serve(exporter, host, int(port), interval=arguments.interval, relay=relay)
    except (ValueError, ImportError) as error:
        sys.stderr.write(f"host_state_exporter: {error}\n")
        return 2
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    with contextlib.suppress(KeyboardInterrupt):
        stop.wait()
    running.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
