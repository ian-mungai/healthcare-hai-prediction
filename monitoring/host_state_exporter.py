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
"""

from __future__ import annotations

import argparse
import contextlib
import http.client
import json
import math
import re
import signal
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

LISTEN = "127.0.0.1:9103"
INTERVAL_SECONDS = 120.0
OM_TIMEOUT_SECONDS = 5.0
DEFAULT_OM_URL = "http://127.0.0.1:8586/healthcheck"
SOURCES = ("host", "openmetadata")
CHECK_NAME = re.compile(r"[a-z][a-z0-9_]{0,40}")
CLUSTER = re.compile(r"[A-Za-z0-9_-]{1,255}")
REGION = re.compile(r"[a-z]{2}(?:-[a-z]+)+-[0-9]+")
OM_URL = re.compile(r"http://127\.0\.0\.1:[0-9]{2,5}/healthcheck")
CATALOG: dict[str, tuple[str, tuple[str, ...], str]] = {
    "hai_host_instances": ("Active container instances in the cluster.", (), "gauge"),
    "hai_host_memory_registered_mib": ("Memory in MiB that ECS can place on the active instances.", (), "gauge"),
    "hai_host_memory_remaining_mib": ("Memory in MiB that ECS can still place on the active instances.", (), "gauge"),
    "hai_host_cpu_registered_units": ("CPU units that ECS can place on the active instances.", (), "gauge"),
    "hai_host_cpu_remaining_units": ("CPU units that ECS can still place on the active instances.", (), "gauge"),
    "hai_host_running_tasks": ("Tasks of the cluster that ECS reports as running.", (), "gauge"),
    "hai_host_job_cap_mib": ("The host job cap in MiB that the launch preflight reads.", (), "gauge"),
    "hai_openmetadata_healthy": ("1 when every check of the OpenMetadata health check is healthy.", (), "gauge"),
    "hai_openmetadata_check_healthy": ("1 when one named check of the OpenMetadata health check is healthy.", ("check",), "gauge"),
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
    return Settings(cluster, region, int(cap) if cap else None, url)


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


def problem(sample: Sample) -> bool:
    """True when the sample may not be exposed: not in the catalog, other labels than its set or a value that is not a finite number."""
    entry = CATALOG.get(sample.name)
    return entry is None or set(sample.labels) != set(entry[1]) or not math.isfinite(sample.value)


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


def serve(exporter: Exporter, host: str, port: int, *, interval: float) -> Running:
    """Listen on loopback and read the sources every ``interval`` seconds, the first read at once. Any other address is refused."""
    if host != "127.0.0.1":
        raise ValueError("the exporter listens on 127.0.0.1 only")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path.split("?")[0] != "/metrics":
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            payload = exporter.render().encode()
            self.send_response(200)
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
        exporter = Exporter(
            {
                "host": lambda: host_samples(ecs, configured.cluster, configured.job_cap_mib),
                "openmetadata": lambda: openmetadata_samples(configured.om_url),
            }
        )
        running = serve(exporter, host, int(port), interval=arguments.interval)
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
