"""Cases for the host exporter: live ECS reads and the live OpenMetadata health probe become Prometheus metrics, and a failing source never shows stale numbers.

A fake ECS client answers the reads the exporter makes, and a loopback stand-in answers the OpenMetadata ``/healthcheck``. The last cases start the real
exporter server on loopback and scrape it over HTTP. No AWS service is contacted. Every server and thread is closed before its case returns.
"""

from __future__ import annotations

import base64
import json
import os
import re
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from monitoring import host_state_exporter
from scripts.infrastructure.metrics_push_cases import quiet, valid_exposition

REPO_ROOT = Path(__file__).resolve().parents[2]
CLUSTER = "example-project-dev"
RELAY_PHRASE = "relay-test-" + "value-4711"


def instance(status: str = "ACTIVE", cpu: tuple[int, int] = (4096, 1024), memory: tuple[int, int] = (32100, 9388), tasks: int = 2) -> dict[str, Any]:
    return {
        "status": status,
        "runningTasksCount": tasks,
        "registeredResources": [
            {"name": "CPU", "integerValue": cpu[0]},
            {"name": "MEMORY", "integerValue": memory[0]},
            {"name": "PORTS", "stringSetValue": ["22"]},
        ],
        "remainingResources": [{"name": "CPU", "integerValue": cpu[1]}, {"name": "MEMORY", "integerValue": memory[1]}],
    }


class FakeEcs:
    """The three ECS reads of the exporter. ``fail`` raises on every call; ``pages`` splits the container instance list across pages."""

    def __init__(
        self,
        instances: list[dict[str, Any]],
        tasks: int = 7,
        fail: bool = False,
        failures: list[dict[str, str]] | None = None,
        pages: int = 1,
        services: list[dict[str, Any]] | None = None,
        stopped: list[dict[str, Any]] | None = None,
    ) -> None:
        self.instances, self.tasks, self.fail, self.failures, self.pages = instances, tasks, fail, failures or [], pages
        self.services, self.stopped = services or [], stopped or []
        self.calls: list[str] = []

    def list_container_instances(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append("list_container_instances")
        if self.fail:
            raise RuntimeError("AccessDeniedException")
        names = [f"arn:aws:ecs:us-west-2:111111111111:container-instance/{CLUSTER}/{index:032x}" for index in range(len(self.instances))]
        size = max(1, -(-len(names) // self.pages))
        start = int(arguments.get("nextToken", "0"))
        result: dict[str, Any] = {"containerInstanceArns": names[start : start + size]}
        if start + size < len(names):
            result["nextToken"] = str(start + size)
        return result

    def describe_container_instances(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append("describe_container_instances")
        if self.fail:
            raise RuntimeError("AccessDeniedException")
        wanted = [int(arn.rsplit("/", 1)[1], 16) for arn in arguments["containerInstances"]]
        return {"containerInstances": [self.instances[index] for index in wanted], "failures": self.failures}

    def list_tasks(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append("list_tasks")
        if self.fail:
            raise RuntimeError("AccessDeniedException")
        if arguments.get("desiredStatus") == "STOPPED":
            return {"taskArns": [f"arn:aws:ecs:us-west-2:111111111111:task/{CLUSTER}/stopped{index:026x}" for index in range(len(self.stopped))]}
        return {"taskArns": [f"arn:aws:ecs:us-west-2:111111111111:task/{CLUSTER}/{index:032x}" for index in range(self.tasks)]}

    def list_services(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append("list_services")
        if self.fail:
            raise RuntimeError("AccessDeniedException")
        return {"serviceArns": [f"arn:aws:ecs:us-west-2:111111111111:service/{CLUSTER}/{item['serviceName']}" for item in self.services]}

    def describe_services(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append("describe_services")
        if self.fail:
            raise RuntimeError("AccessDeniedException")
        if len(arguments["services"]) > 10:
            raise RuntimeError("InvalidParameterException: more than 10 services")
        names = {arn.rsplit("/", 1)[1] for arn in arguments["services"]}
        return {"services": [item for item in self.services if item["serviceName"] in names], "failures": []}

    def describe_tasks(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append("describe_tasks")
        if self.fail:
            raise RuntimeError("AccessDeniedException")
        if len(arguments["tasks"]) > 100:
            raise RuntimeError("InvalidParameterException: more than 100 tasks")
        wanted = [int(arn.rsplit("stopped", 1)[1], 16) for arn in arguments["tasks"]]
        return {"tasks": [self.stopped[index] for index in wanted], "failures": []}


class OpenMetadataStandIn:
    """A loopback admin port: ``body`` is the /healthcheck JSON, ``status`` its HTTP status, ``delay`` the wait before it answers, ``raw`` a non-JSON body."""

    def __init__(self, body: dict[str, Any] | None = None, status: int = 200, delay: float = 0.0, raw: bytes | None = None) -> None:
        self.body = body if body is not None else {"deadlocks": {"healthy": True}, "database": {"healthy": True}}
        self.status, self.delay, self.raw = status, delay, raw
        self.paths: list[str] = []
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                stand_in.paths.append(self.path)
                time.sleep(stand_in.delay)
                payload = stand_in.raw if stand_in.raw is not None else json.dumps(stand_in.body).encode()
                self.send_response(stand_in.status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/healthcheck"

    def __enter__(self) -> OpenMetadataStandIn:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


def values(samples: list[host_state_exporter.Sample]) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return {(sample.name, tuple(sorted(sample.labels.items()))): sample.value for sample in samples}


def collected(exporter: host_state_exporter.Exporter) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    exporter.refresh()
    return values(exporter.samples())


def host_capacity_is_exact(root: Path) -> dict[str, bool]:
    ecs = FakeEcs([instance()], tasks=7)
    found = values(host_state_exporter.host_samples(ecs, CLUSTER, 22712))
    return {
        "one instance": found[("hai_host_instances", ())] == 1,
        "registered memory": found[("hai_host_memory_registered_mib", ())] == 32100,
        "remaining memory": found[("hai_host_memory_remaining_mib", ())] == 9388,
        "registered cpu": found[("hai_host_cpu_registered_units", ())] == 4096,
        "remaining cpu": found[("hai_host_cpu_remaining_units", ())] == 1024,
        "running tasks come from the cluster listing": found[("hai_host_running_tasks", ())] == 7,
        "the job cap is the launch preflight value": found[("hai_host_job_cap_mib", ())] == 22712,
        "only the cluster was asked": all(call in ("list_container_instances", "describe_container_instances", "list_tasks") for call in ecs.calls),
    }


def several_instances_are_summed_and_only_active_ones_count(root: Path) -> dict[str, bool]:
    ecs = FakeEcs([instance(), instance(memory=(16000, 4000), cpu=(2048, 512)), instance("DRAINING", memory=(99999, 99999))], pages=2)
    found = values(host_state_exporter.host_samples(ecs, CLUSTER, None))
    return {
        "active instances are counted": found[("hai_host_instances", ())] == 2,
        "memory is summed over active instances only": found[("hai_host_memory_registered_mib", ())] == 48100
        and found[("hai_host_memory_remaining_mib", ())] == 13388,
        "cpu is summed over active instances only": found[("hai_host_cpu_registered_units", ())] == 6144
        and found[("hai_host_cpu_remaining_units", ())] == 1536,
        "a missing job cap leaves the cap metric out": ("hai_host_job_cap_mib", ()) not in found,
        "the second page of instances was read": ecs.calls.count("list_container_instances") == 2,
    }


def no_instance_is_a_host_with_zero_capacity(root: Path) -> dict[str, bool]:
    found = values(host_state_exporter.host_samples(FakeEcs([]), CLUSTER, 22712))
    return {
        "zero instances is reported as zero, not as an error": found[("hai_host_instances", ())] == 0,
        "no capacity series is invented": ("hai_host_memory_remaining_mib", ()) not in found,
    }


def ecs_failure_marks_the_source_down_and_drops_its_series(root: Path) -> dict[str, bool]:
    ecs = FakeEcs([instance()])
    exporter = host_state_exporter.Exporter({"host": lambda: host_state_exporter.host_samples(ecs, CLUSTER, 22712)})
    first = collected(exporter)
    ecs.fail = True
    second = collected(exporter)
    return {
        "the first read is up": first[("host_state_exporter_source_up", (("source", "host"),))] == 1 and ("hai_host_instances", ()) in first,
        "a failed read marks the source down": second[("host_state_exporter_source_up", (("source", "host"),))] == 0,
        "the old capacity is not served as if it were current": ("hai_host_instances", ()) not in second
        and ("hai_host_memory_remaining_mib", ()) not in second,
        "the error is counted once": second[("host_state_exporter_source_errors_total", (("source", "host"),))] == 1,
        "the last success time is kept": ("host_state_exporter_source_last_success_timestamp_seconds", (("source", "host"),)) in second,
    }


def ecs_failures_in_the_reply_are_errors(root: Path) -> dict[str, bool]:
    broken = FakeEcs([instance()], failures=[{"reason": "MISSING"}])
    exporter = host_state_exporter.Exporter({"host": lambda: host_state_exporter.host_samples(broken, CLUSTER, 1)})
    found = collected(exporter)
    return {"a described instance that ECS reports as failed marks the source down": found[("host_state_exporter_source_up", (("source", "host"),))] == 0}


def openmetadata_health_is_read_live(root: Path) -> dict[str, bool]:
    body = {"deadlocks": {"healthy": True}, "database": {"healthy": True}, "OpenMetadata Search": {"healthy": False, "message": "red"}}
    with OpenMetadataStandIn(body) as stand_in:
        found = values(host_state_exporter.openmetadata_samples(stand_in.url, 3.0))
        path = stand_in.paths
    return {
        "the admin health check path is read": path == ["/healthcheck"],
        "each check is a series with a lower-case name": found[("hai_openmetadata_check_healthy", (("check", "database"),))] == 1
        and found[("hai_openmetadata_check_healthy", (("check", "openmetadata_search"),))] == 0,
        "the server is unhealthy when any check is": found[("hai_openmetadata_healthy", ())] == 0,
        "no message text becomes a label": all("red" not in value for _, labels in found for _, value in labels),
    }


def openmetadata_all_healthy_is_one(root: Path) -> dict[str, bool]:
    with OpenMetadataStandIn() as stand_in:
        found = values(host_state_exporter.openmetadata_samples(stand_in.url, 3.0))
    return {"all checks healthy gives 1": found[("hai_openmetadata_healthy", ())] == 1}


def openmetadata_failures_mark_the_source_down(root: Path) -> dict[str, bool]:
    results: dict[str, bool] = {}
    with OpenMetadataStandIn(body={"database": {"healthy": False}}, status=500) as stand_in:
        results["a 500 answer with a failing check is read as unhealthy data, not as an error"] = (
            values(host_state_exporter.openmetadata_samples(stand_in.url, 3.0)).get(("hai_openmetadata_healthy", ())) == 0
        )
    with OpenMetadataStandIn(raw=b"not json") as stand_in:
        try:
            host_state_exporter.openmetadata_samples(stand_in.url, 3.0)
            results["a body that is not JSON is an error"] = False
        except host_state_exporter.SourceError:
            results["a body that is not JSON is an error"] = True
    with OpenMetadataStandIn(body={"database": {"healthy": "yes"}}) as stand_in:
        try:
            host_state_exporter.openmetadata_samples(stand_in.url, 3.0)
            results["a check without a boolean healthy is an error"] = False
        except host_state_exporter.SourceError:
            results["a check without a boolean healthy is an error"] = True
    with OpenMetadataStandIn() as stand_in:
        url = stand_in.url
    started = time.monotonic()
    try:
        host_state_exporter.openmetadata_samples(url, 1.0)
        results["an unreachable server is an error"] = False
    except host_state_exporter.SourceError:
        results["an unreachable server is an error"] = True
    results["it fails fast"] = time.monotonic() - started < 2.0
    with OpenMetadataStandIn(delay=2.0) as stand_in:
        started = time.monotonic()
        try:
            host_state_exporter.openmetadata_samples(stand_in.url, 0.3)
            results["a slow server is an error"] = False
        except host_state_exporter.SourceError:
            results["a slow server is an error"] = True
        results["the wait is bounded by the timeout"] = time.monotonic() - started < 1.5
    return results


def invalid_check_names_are_dropped(root: Path) -> dict[str, bool]:
    body = {"ok": {"healthy": True}, "x" * 80: {"healthy": True}, "3start": {"healthy": True}, "Run 20261117": {"healthy": True}}
    with OpenMetadataStandIn(body) as stand_in:
        found = values(host_state_exporter.openmetadata_samples(stand_in.url, 3.0))
    names = {dict(labels)["check"] for name, labels in found if name == "hai_openmetadata_check_healthy"}
    return {
        "a plain name stays": "ok" in names,
        "a name that is too long or starts with a digit is dropped": names <= {"ok", "run_20261117"},
        "the overall health still counts every check": found[("hai_openmetadata_healthy", ())] == 1,
    }


def a_failing_collector_does_not_stop_the_others(root: Path) -> dict[str, bool]:
    def broken() -> list[host_state_exporter.Sample]:
        raise RuntimeError("boom")

    exporter = host_state_exporter.Exporter({"host": broken, "openmetadata": lambda: [host_state_exporter.Sample("hai_openmetadata_healthy", 1.0, {})]})
    found = collected(exporter)
    return {
        "the failing source is down": found[("host_state_exporter_source_up", (("source", "host"),))] == 0,
        "the other source is up with its data": found[("host_state_exporter_source_up", (("source", "openmetadata"),))] == 1
        and ("hai_openmetadata_healthy", ()) in found,
    }


def exposition_is_valid_and_closed(root: Path) -> dict[str, bool]:
    exporter = host_state_exporter.Exporter({"host": lambda: host_state_exporter.host_samples(FakeEcs([instance()]), CLUSTER, 22712)})
    exporter.refresh()
    text = exporter.render()
    names = {line.split("{")[0].split(" ")[0] for line in text.splitlines() if line and not line.startswith("#")}
    bad = host_state_exporter.Sample("hai_not_in_the_catalog", 1.0, {})
    other = host_state_exporter.Exporter({"host": lambda: [bad, host_state_exporter.Sample("hai_host_instances", 1.0, {"run_id": "x"})]})
    other.refresh()
    return {
        "the text is valid exposition": valid_exposition(text),
        "the catalog names every series that is exposed": names <= set(host_state_exporter.CATALOG),
        "a sample outside the catalog or with a label outside its set is dropped": "hai_not_in_the_catalog" not in other.render()
        and "run_id" not in other.render(),
    }


def server_answers_a_scrape_over_http(root: Path) -> dict[str, bool]:
    ecs = FakeEcs([instance()])
    exporter = host_state_exporter.Exporter({"host": lambda: host_state_exporter.host_samples(ecs, CLUSTER, 22712)})
    running = host_state_exporter.serve(exporter, "127.0.0.1", 0, interval=0.1)
    try:
        deadline = time.monotonic() + 5
        body, status, wrong, kind = "", 0, 0, ""
        while time.monotonic() < deadline:
            connection = HTTPConnection("127.0.0.1", running.port, timeout=3)
            connection.request("GET", "/metrics")
            response = connection.getresponse()
            status, kind, body = response.status, response.getheader("Content-Type") or "", response.read().decode()
            connection.close()
            if "hai_host_instances" in body:
                break
            time.sleep(0.1)
        other = HTTPConnection("127.0.0.1", running.port, timeout=3)
        other.request("GET", "/admin")
        wrong = other.getresponse().status
        other.close()
        before = ecs.calls.count("list_container_instances")
        time.sleep(0.5)
        after = ecs.calls.count("list_container_instances")
    finally:
        running.stop()
    return {
        "a scrape is answered 200 with text exposition": status == 200 and kind.startswith("text/plain") and valid_exposition(body),
        "the capacity is in the scrape": 'host_state_exporter_source_up{source="host"} 1' in body and "hai_host_instances 1" in body,
        "another path is 404": wrong == 404,
        "the sources are read on the timer and not on each scrape": after > before and after - before <= 8,
        "the threads end when the server stops": not any(thread.name.startswith("host_state_exporter") for thread in threading.enumerate()),
    }


def a_scrape_never_waits_for_a_source(root: Path) -> dict[str, bool]:
    gate = threading.Event()

    def slow() -> list[host_state_exporter.Sample]:
        gate.wait(5)
        return []

    exporter = host_state_exporter.Exporter({"host": slow})
    running = host_state_exporter.serve(exporter, "127.0.0.1", 0, interval=60)
    try:
        started = time.monotonic()
        connection = HTTPConnection("127.0.0.1", running.port, timeout=3)
        connection.request("GET", "/metrics")
        status = connection.getresponse().status
        connection.close()
        elapsed = time.monotonic() - started
    finally:
        gate.set()
        running.stop()
    return {"the scrape is answered at once while a source is still reading": status == 200 and elapsed < 1.5}


def listener_must_be_loopback(root: Path) -> dict[str, bool]:
    results: dict[str, bool] = {}
    for address in (".".join(["0"] * 4), "10.0.0.5", "localhost", ""):
        try:
            host_state_exporter.serve(host_state_exporter.Exporter({}), address, 0, interval=1)
            results[f"{address or 'empty'} is refused"] = False
        except ValueError:
            results[f"{address or 'empty'} is refused"] = True
    return results


def command_reads_its_settings_from_the_environment(root: Path) -> dict[str, bool]:
    settings = host_state_exporter.settings(
        {"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2", "HAI_LAUNCH_HOST_JOB_CAP_MIB": "22712", "OM_HEALTH_URL": "http://127.0.0.1:8586/healthcheck"}
    )
    problems: list[str] = []
    for bad in (
        {},
        {"ECS_CLUSTER": "bad name!", "AWS_REGION": "us-west-2"},
        {"ECS_CLUSTER": CLUSTER, "AWS_REGION": "nowhere"},
        {"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2", "HAI_LAUNCH_HOST_JOB_CAP_MIB": "-1"},
    ):
        try:
            host_state_exporter.settings(bad)
        except ValueError as error:
            problems.append(str(error))
    default = host_state_exporter.settings({"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2"})
    return {
        "a complete environment gives the settings": settings.cluster == CLUSTER
        and settings.job_cap_mib == 22712
        and settings.om_url.endswith(":8586/healthcheck"),
        "the OpenMetadata address defaults to the admin port on loopback": default.om_url == "http://127.0.0.1:8586/healthcheck"
        and default.job_cap_mib is None,
        "each bad environment is refused by name": len(problems) == 4
        and "ECS_CLUSTER" in problems[0]
        and "AWS_REGION" in problems[2]
        and "HAI_LAUNCH_HOST_JOB_CAP_MIB" in problems[3],
        "the OpenMetadata address must be plain http on loopback": _refuses(
            {"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2", "OM_HEALTH_URL": "http://10.0.0.5:8586/healthcheck"}
        ),
    }


def _refuses(environment: dict[str, str]) -> bool:
    try:
        host_state_exporter.settings(environment)
    except ValueError:
        return True
    return False


def ecs_client_has_hard_timeouts(root: Path) -> dict[str, bool]:
    config = host_state_exporter.ecs_client_config()
    return {
        "connect and read timeouts are bounded": config.connect_timeout <= 5 and config.read_timeout <= 10,
        "retries are bounded": config.retries["max_attempts"] <= 2,
    }


def catalog_has_a_help_text_and_closed_labels(root: Path) -> dict[str, bool]:
    return {
        "every metric has a help text": all(text.strip() for text, _, _ in host_state_exporter.CATALOG.values()),
        "the only label names are source, check, service, stop_code, probe and volume": {
            label for _, labels, _ in host_state_exporter.CATALOG.values() for label in labels
        }
        <= {"source", "check", "service", "stop_code", "probe", "volume"},
        "metric names are valid": all(re.fullmatch(r"[a-z][a-z0-9_]*", name) for name in host_state_exporter.CATALOG),
        "counters end in _total": all(name.endswith("_total") == (kind == "counter") for name, (_, _, kind) in host_state_exporter.CATALOG.items()),
    }


def service(name: str, desired: int = 1, running: int = 1, pending: int = 0) -> dict[str, Any]:
    return {"serviceName": f"example_project_dev_{name}", "desiredCount": desired, "runningCount": running, "pendingCount": pending}


def stopped(group: str, code: str = "EssentialContainerExited", minutes_ago: int = 10) -> dict[str, Any]:
    return {"group": group, "stopCode": code, "stoppedAt": datetime.now(UTC) - timedelta(minutes=minutes_ago)}


PREFIX = "example_project_dev_"


def services_report_desired_running_and_pending(root: Path) -> dict[str, bool]:
    ecs = FakeEcs([], services=[service("postgres"), service("airflow", 1, 0, 1), service("model_api", 0, 0, 0), service("other", 1, 1, 0)])
    found = values(host_state_exporter.service_samples(ecs, CLUSTER, PREFIX))
    return {
        "a healthy service has desired and running equal": found[("hai_service_tasks_desired", (("service", "postgres"),))] == 1
        and found[("hai_service_tasks_running", (("service", "postgres"),))] == 1,
        "a service that is starting shows pending and no running task": found[("hai_service_tasks_running", (("service", "airflow"),))] == 0
        and found[("hai_service_tasks_pending", (("service", "airflow"),))] == 1,
        "a service switched off has desired zero": found[("hai_service_tasks_desired", (("service", "model_api"),))] == 0,
        "the project prefix is cut from the name": ("hai_service_tasks_desired", (("service", "example_project_dev_postgres"),)) not in found,
    }


def services_are_described_ten_at_a_time(root: Path) -> dict[str, bool]:
    ecs = FakeEcs([], services=[service(f"svc_{index}") for index in range(25)])
    found = values(host_state_exporter.service_samples(ecs, CLUSTER, PREFIX))
    return {
        "all 25 services are reported": sum(1 for name, _ in found if name == "hai_service_tasks_desired") == 25,
        "the description is split into batches of ten": ecs.calls.count("describe_services") == 3,
    }


def stopped_tasks_are_counted_per_service_and_stop_code_for_the_last_hour(root: Path) -> dict[str, bool]:
    ecs = FakeEcs(
        [],
        services=[service("airflow_scheduler"), service("mlflow")],
        stopped=[
            stopped("service:example_project_dev_airflow_scheduler"),
            stopped("service:example_project_dev_airflow_scheduler"),
            stopped("service:example_project_dev_airflow_scheduler", "TaskFailedToStart", 5),
            stopped("service:example_project_dev_mlflow", "SomethingNew"),
            stopped("service:example_project_dev_mlflow", "EssentialContainerExited", 120),
            stopped("family:example_project_dev_job_silver_host", "UserInitiated"),
        ],
    )
    found = values(host_state_exporter.service_samples(ecs, CLUSTER, PREFIX))
    key = "hai_service_tasks_stopped_recent"
    return {
        "two exits of one service are counted together": found[(key, (("service", "airflow_scheduler"), ("stop_code", "EssentialContainerExited")))] == 2,
        "a failed start has its own series": found[(key, (("service", "airflow_scheduler"), ("stop_code", "TaskFailedToStart")))] == 1,
        "a stop code ECS may add later becomes other": found[(key, (("service", "mlflow"), ("stop_code", "other")))] == 1,
        "a stop older than an hour is not counted": (key, (("service", "mlflow"), ("stop_code", "EssentialContainerExited"))) not in found,
        "a job task is not a service": not any(dict(labels).get("service", "").startswith("job") for name, labels in found if name == key),
    }


def too_many_stopped_tasks_are_read_in_bounded_batches(root: Path) -> dict[str, bool]:
    ecs = FakeEcs([], services=[service("airflow")], stopped=[stopped("service:example_project_dev_airflow") for _ in range(250)])
    found = values(host_state_exporter.service_samples(ecs, CLUSTER, PREFIX))
    return {
        "every stop is counted when the batches stay within the API limit of 100": found[
            ("hai_service_tasks_stopped_recent", (("service", "airflow"), ("stop_code", "EssentialContainerExited")))
        ]
        == 250,
        "the tasks are described in three calls": ecs.calls.count("describe_tasks") == 3,
    }


def address(stand_in: OpenMetadataStandIn) -> str:
    return stand_in.url.replace("/healthcheck", "")


class ProbeStandIn(OpenMetadataStandIn):
    """A loopback answer for any path with one status, for the probes."""

    def __init__(self, status: int = 200, delay: float = 0.0) -> None:
        super().__init__(status=status, delay=delay, raw=b"ok")


def probes_report_up_for_an_accepted_status_and_down_otherwise(root: Path) -> dict[str, bool]:
    with ProbeStandIn(200) as good, ProbeStandIn(401) as auth, ProbeStandIn(503) as bad:
        probes = {
            "polaris": host_state_exporter.Probe("polaris", address(good) + "/q/health", (200,)),
            "mlflow": host_state_exporter.Probe("mlflow", address(auth) + "/health", (200, 401)),
            "streamlit": host_state_exporter.Probe("streamlit", address(bad) + "/_stcore/health", (200,)),
        }
        found = values(host_state_exporter.probe_samples(probes, 3.0))
    return {
        "an accepted status is up": found[("hai_service_probe_up", (("probe", "polaris"), ("service", "polaris")))] == 1,
        "a login answer is up for the service that has one": found[("hai_service_probe_up", (("probe", "mlflow"), ("service", "mlflow")))] == 1,
        "a refusing service is down": found[("hai_service_probe_up", (("probe", "streamlit"), ("service", "streamlit")))] == 0,
        "the answer time is exposed": ("hai_service_probe_seconds", (("probe", "polaris"), ("service", "polaris"))) in found,
    }


def a_probe_that_cannot_connect_or_is_slow_is_down_and_never_raises(root: Path) -> dict[str, bool]:
    with ProbeStandIn() as closed:
        url = closed.url.replace("/healthcheck", "")
    with ProbeStandIn(delay=2.0) as slow:
        started = time.monotonic()
        found = values(
            host_state_exporter.probe_samples(
                {
                    "model_api": host_state_exporter.Probe("model_api", url + "/v1/live", (200,)),
                    "airflow_api": host_state_exporter.Probe("airflow_apiserver", slow.url.replace("/healthcheck", "") + "/api/v2/version", (200,)),
                },
                0.4,
            )
        )
        elapsed = time.monotonic() - started
    return {
        "a closed port is down": found[("hai_service_probe_up", (("probe", "model_api"), ("service", "model_api")))] == 0,
        "a slow answer is down": found[("hai_service_probe_up", (("probe", "airflow_api"), ("service", "airflow_apiserver")))] == 0,
        "the wait is bounded by the timeout for each probe": elapsed < 2.5,
    }


def the_default_probes_are_the_health_checks_the_services_already_use(root: Path) -> dict[str, bool]:
    probes = host_state_exporter.PROBES
    return {
        "the probes cover Polaris, MLflow, Airflow, the model API, Streamlit and OpenSearch": {probe.service for probe in probes.values()}
        >= {"polaris", "mlflow", "airflow_apiserver", "airflow_scheduler", "model_api", "streamlit", "openmetadata"},
        "every address is plain http on loopback": all(host_state_exporter.PROBE_URL.fullmatch(probe.url) for probe in probes.values()),
        "the paths match the container health checks": probes["polaris"].url.endswith(":8182/q/health")
        and probes["mlflow"].url.endswith(":5100/health")
        and probes["model_api"].url.endswith(":8000/v1/live")
        and probes["streamlit"].url.endswith(":8501/_stcore/health")
        and probes["airflow_api"].url.endswith(":8080/api/v2/version")
        and probes["mlflow"].ok == (200, 401),
    }


def disk_free_is_reported_for_the_data_and_the_scratch_volume(root: Path) -> dict[str, bool]:
    usage = {"/host/data": (1000, 400, 600), "/host/data/job_scratch": (500, 450, 50)}
    devices = {"/host/data": 1, "/host/data/job_scratch": 2}
    found = values(host_state_exporter.disk_samples({"data": "/host/data", "scratch": "/host/data/job_scratch"}, usage.__getitem__, devices.__getitem__))
    return {
        "free bytes of the data volume": found[("hai_host_disk_free_bytes", (("volume", "data"),))] == 600,
        "total bytes of the scratch volume": found[("hai_host_disk_total_bytes", (("volume", "scratch"),))] == 500,
        "a scratch folder on its own volume is flagged": found[("hai_host_scratch_separate_volume", ())] == 1,
    }


def a_scratch_folder_on_the_data_volume_is_flagged(root: Path) -> dict[str, bool]:
    found = values(host_state_exporter.disk_samples({"data": "/d", "scratch": "/d/s"}, lambda _path: (10, 5, 5), lambda _path: 7))
    return {"the same device means the scratch folder shares the data volume": found[("hai_host_scratch_separate_volume", ())] == 0}


def a_missing_disk_path_is_a_source_error(root: Path) -> dict[str, bool]:
    def broken(path: str) -> tuple[int, int, int]:
        raise FileNotFoundError(path)

    try:
        host_state_exporter.disk_samples({"data": "/nope", "scratch": "/nope/s"}, broken, lambda _path: 1)
        raised = False
    except host_state_exporter.SourceError:
        raised = True
    return {"the source fails by name, so the series are dropped and the alert fires": raised}


def the_real_disk_reader_reads_a_real_folder(root: Path) -> dict[str, bool]:
    found = values(host_state_exporter.disk_samples({"data": str(root), "scratch": str(root)}, host_state_exporter.disk_usage, host_state_exporter.device_of))
    return {
        "free is a positive number below the total": 0
        < found[("hai_host_disk_free_bytes", (("volume", "data"),))]
        <= found[("hai_host_disk_total_bytes", (("volume", "data"),))],
        "one folder is one volume": found[("hai_host_scratch_separate_volume", ())] == 0,
        "the real folder has an st_dev": os.stat(root).st_dev == host_state_exporter.device_of(str(root)),
    }


def settings_gain_the_prefix_and_the_two_disk_paths(root: Path) -> dict[str, bool]:
    base = {"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2"}
    configured = host_state_exporter.settings(
        {**base, "ECS_SERVICE_PREFIX": PREFIX, "HOST_DATA_PATH": "/host/data", "HOST_SCRATCH_PATH": "/host/data/job_scratch"}
    )
    default = host_state_exporter.settings(base)

    def refused(extra: dict[str, str]) -> bool:
        try:
            host_state_exporter.settings({**base, **extra})
        except ValueError:
            return True
        return False

    return {
        "the values are read": configured.service_prefix == PREFIX
        and configured.data_path == "/host/data"
        and configured.scratch_path == "/host/data/job_scratch",
        "without them the services, probes and disk sources are still built from defaults": default.service_prefix == "" and default.data_path == "",
        "a prefix with a bad character is refused": refused({"ECS_SERVICE_PREFIX": "bad prefix!"}),
        "a relative or climbing path is refused": refused({"HOST_DATA_PATH": "host/data"}) and refused({"HOST_DATA_PATH": "/host/../etc"}),
    }


def the_command_builds_every_source(root: Path) -> dict[str, bool]:
    configured = host_state_exporter.settings(
        {
            "ECS_CLUSTER": CLUSTER,
            "AWS_REGION": "us-west-2",
            "ECS_SERVICE_PREFIX": PREFIX,
            "HOST_DATA_PATH": "/host/data",
            "HOST_SCRATCH_PATH": "/host/data/job_scratch",
        }
    )
    names = set(host_state_exporter.collectors(configured, FakeEcs([])))
    without_disk = set(host_state_exporter.collectors(host_state_exporter.settings({"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2"}), FakeEcs([])))
    return {
        "five sources with the disk paths set": names == {"host", "services", "probes", "disk", "openmetadata"},
        "no disk source without its paths": "disk" not in without_disk,
    }


class MlflowStandIn:
    """A loopback MLflow /metrics that answers only the admin user's Basic credential, like the real one with its authentication on."""

    def __init__(self, user: str = "admin", phrase: str = RELAY_PHRASE, status: int = 200, delay: float = 0.0) -> None:
        self.user, self.phrase, self.status, self.delay = user, phrase, status, delay
        self.seen: list[str | None] = []
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                stand_in.seen.append(self.headers.get("Authorization"))
                time.sleep(stand_in.delay)
                expected = "Basic " + base64.b64encode(f"{stand_in.user}:{stand_in.phrase}".encode()).decode()
                ok = self.path == "/metrics" and self.headers.get("Authorization") == expected and stand_in.status == 200
                payload = b"# TYPE mlflow_http_request_total counter\nmlflow_http_request_total 3\n" if ok else b"denied"
                self.send_response(200 if ok else (stand_in.status if stand_in.status != 200 else 401))
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8" if ok else "text/plain")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/metrics"

    def __enter__(self) -> MlflowStandIn:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


def fetch(port: int, path: str) -> tuple[int, str, str]:
    connection = HTTPConnection("127.0.0.1", port, timeout=5)
    connection.request("GET", path)
    response = connection.getresponse()
    body = response.read().decode()
    kind = response.getheader("Content-Type") or ""
    connection.close()
    return response.status, kind, body


def running_with_relay(relay: Callable[[], tuple[int, bytes]] | None) -> hai_exporter_running:
    return host_state_exporter.serve(host_state_exporter.Exporter({}), "127.0.0.1", 0, interval=60, relay=relay)


hai_exporter_running = host_state_exporter.Running


def the_relay_serves_only_the_mlflow_metrics_text_with_the_admin_credential(root: Path) -> dict[str, bool]:
    with MlflowStandIn() as mlflow:
        relay = host_state_exporter.mlflow_relay(mlflow.url, RELAY_PHRASE, 3.0)
        running = running_with_relay(relay)
        try:
            status, kind, body = fetch(running.port, "/mlflow_metrics")
            other = fetch(running.port, "/mlflow_metrics/extra")[0], fetch(running.port, "/admin")[0], fetch(running.port, "/metrics")[0]
        finally:
            running.stop()
        seen = list(mlflow.seen)
    return {
        "the text of MLflow's /metrics is relayed as it came": status == 200 and kind.startswith("text/plain") and "mlflow_http_request_total 3" in body,
        "MLflow got the admin Basic credential": seen[:1] == ["Basic " + base64.b64encode(f"admin:{RELAY_PHRASE}".encode()).decode()],
        "no other path is relayed": other[0] == 404 and other[1] == 404 and other[2] == 200,
        "the credential is not in what the relay answered": RELAY_PHRASE not in body and RELAY_PHRASE not in kind,
    }


def a_refusing_or_unreachable_mlflow_gives_an_empty_502_and_no_credential(root: Path) -> dict[str, bool]:
    with MlflowStandIn(phrase="another") as wrong:
        relay = host_state_exporter.mlflow_relay(wrong.url, RELAY_PHRASE, 3.0)
        running = running_with_relay(relay)
        try:
            refused = fetch(running.port, "/mlflow_metrics")
        finally:
            running.stop()
    with MlflowStandIn() as closed:
        url = closed.url
    relay = host_state_exporter.mlflow_relay(url, RELAY_PHRASE, 1.0)
    running = running_with_relay(relay)
    try:
        unreachable = fetch(running.port, "/mlflow_metrics")
    finally:
        running.stop()
    with MlflowStandIn(delay=2.0) as slow:
        relay = host_state_exporter.mlflow_relay(slow.url, RELAY_PHRASE, 0.3)
        running = running_with_relay(relay)
        started = time.monotonic()
        try:
            late = fetch(running.port, "/mlflow_metrics")
        finally:
            running.stop()
        waited = time.monotonic() - started
    return {
        "a refused credential is a 502 with no body": refused[0] == 502 and refused[2] == "",
        "an unreachable MLflow is a 502 with no body": unreachable[0] == 502 and unreachable[2] == "",
        "a slow MLflow is a 502 inside the timeout": late[0] == 502 and waited < 1.8,
        "nothing the relay answered holds the credential": all(RELAY_PHRASE not in text for answer in (refused, unreachable, late) for text in answer[1:]),
    }


def the_relay_never_logs_or_writes_the_credential(root: Path) -> dict[str, bool]:
    with MlflowStandIn(phrase="another") as wrong, tempfile.TemporaryDirectory(prefix="relay_case_") as scratch:
        relay = host_state_exporter.mlflow_relay(wrong.url, RELAY_PHRASE, 3.0)
        running = running_with_relay(relay)
        try:
            _, text = quiet(lambda: fetch(running.port, "/mlflow_metrics"))
        finally:
            running.stop()
        written = list(Path(scratch).iterdir())
    return {
        "standard error names the failure without the credential": RELAY_PHRASE not in text,
        "the relay writes no file": written == [],
        "the module source holds no credential text": RELAY_PHRASE not in Path(host_state_exporter.__file__).read_text(encoding="utf-8"),
    }


def without_a_password_there_is_no_relay(root: Path) -> dict[str, bool]:
    configured = host_state_exporter.settings({"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2"})
    running = running_with_relay(None)
    try:
        status = fetch(running.port, "/mlflow_metrics")[0]
    finally:
        running.stop()
    return {
        "the path is 404 when no relay is configured": status == 404,
        "the settings carry no credential field": not hasattr(configured, "mlflow_password"),
        "the relay address must be MLflow's metrics path on loopback": not host_state_exporter.relay_url_ok("http://10.0.0.5:5100/metrics")
        and host_state_exporter.relay_url_ok("http://127.0.0.1:5100/metrics"),
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "exporter_host_capacity_is_exact": host_capacity_is_exact,
    "exporter_several_instances_are_summed_and_only_active_ones_count": several_instances_are_summed_and_only_active_ones_count,
    "exporter_no_instance_is_a_host_with_zero_capacity": no_instance_is_a_host_with_zero_capacity,
    "exporter_ecs_failure_marks_the_source_down_and_drops_its_series": ecs_failure_marks_the_source_down_and_drops_its_series,
    "exporter_ecs_failures_in_the_reply_are_errors": ecs_failures_in_the_reply_are_errors,
    "exporter_openmetadata_health_is_read_live": openmetadata_health_is_read_live,
    "exporter_openmetadata_all_healthy_is_one": openmetadata_all_healthy_is_one,
    "exporter_openmetadata_failures_mark_the_source_down": openmetadata_failures_mark_the_source_down,
    "exporter_invalid_check_names_are_dropped": invalid_check_names_are_dropped,
    "exporter_a_failing_collector_does_not_stop_the_others": a_failing_collector_does_not_stop_the_others,
    "exporter_exposition_is_valid_and_closed": exposition_is_valid_and_closed,
    "exporter_server_answers_a_scrape_over_http": server_answers_a_scrape_over_http,
    "exporter_a_scrape_never_waits_for_a_source": a_scrape_never_waits_for_a_source,
    "exporter_listener_must_be_loopback": listener_must_be_loopback,
    "exporter_command_reads_its_settings_from_the_environment": command_reads_its_settings_from_the_environment,
    "exporter_ecs_client_has_hard_timeouts": ecs_client_has_hard_timeouts,
    "exporter_catalog_has_a_help_text_and_closed_labels": catalog_has_a_help_text_and_closed_labels,
    "exporter_services_report_desired_running_and_pending": services_report_desired_running_and_pending,
    "exporter_services_are_described_ten_at_a_time": services_are_described_ten_at_a_time,
    "exporter_stopped_tasks_are_counted_per_service_and_stop_code_for_the_last_hour": stopped_tasks_are_counted_per_service_and_stop_code_for_the_last_hour,
    "exporter_too_many_stopped_tasks_are_read_in_bounded_batches": too_many_stopped_tasks_are_read_in_bounded_batches,
    "exporter_probes_report_up_for_an_accepted_status_and_down_otherwise": probes_report_up_for_an_accepted_status_and_down_otherwise,
    "exporter_a_probe_that_cannot_connect_or_is_slow_is_down_and_never_raises": a_probe_that_cannot_connect_or_is_slow_is_down_and_never_raises,
    "exporter_the_default_probes_are_the_health_checks_the_services_already_use": the_default_probes_are_the_health_checks_the_services_already_use,
    "exporter_disk_free_is_reported_for_the_data_and_the_scratch_volume": disk_free_is_reported_for_the_data_and_the_scratch_volume,
    "exporter_a_scratch_folder_on_the_data_volume_is_flagged": a_scratch_folder_on_the_data_volume_is_flagged,
    "exporter_a_missing_disk_path_is_a_source_error": a_missing_disk_path_is_a_source_error,
    "exporter_the_real_disk_reader_reads_a_real_folder": the_real_disk_reader_reads_a_real_folder,
    "exporter_settings_gain_the_prefix_and_the_two_disk_paths": settings_gain_the_prefix_and_the_two_disk_paths,
    "exporter_the_command_builds_every_source": the_command_builds_every_source,
    "exporter_the_relay_serves_only_the_mlflow_metrics_text_with_the_admin_credential": the_relay_serves_only_the_mlflow_metrics_text_with_the_admin_credential,
    "exporter_a_refusing_or_unreachable_mlflow_gives_an_empty_502_and_no_credential": a_refusing_or_unreachable_mlflow_gives_an_empty_502_and_no_credential,
    "exporter_the_relay_never_logs_or_writes_the_credential": the_relay_never_logs_or_writes_the_credential,
    "exporter_without_a_password_there_is_no_relay": without_a_password_there_is_no_relay,
}
