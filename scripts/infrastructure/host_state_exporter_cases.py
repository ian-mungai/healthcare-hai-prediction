"""Cases for the host exporter: live ECS reads and the live OpenMetadata health probe become Prometheus metrics, and a failing source never shows stale numbers.

A fake ECS client answers the reads the exporter makes, and a loopback stand-in answers the OpenMetadata ``/healthcheck``. The last cases start the real
exporter server on loopback and scrape it over HTTP. No AWS service is contacted. Every server and thread is closed before its case returns.
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Callable
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from monitoring import host_state_exporter
from scripts.infrastructure.metrics_push_cases import valid_exposition

REPO_ROOT = Path(__file__).resolve().parents[2]
CLUSTER = "example-project-dev"


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
        self, instances: list[dict[str, Any]], tasks: int = 7, fail: bool = False, failures: list[dict[str, str]] | None = None, pages: int = 1
    ) -> None:
        self.instances, self.tasks, self.fail, self.failures, self.pages = instances, tasks, fail, failures or [], pages
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
        return {"taskArns": [f"arn:aws:ecs:us-west-2:111111111111:task/{CLUSTER}/{index:032x}" for index in range(self.tasks)]}


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
        "the only label names are source and check": {label for _, labels, _ in host_state_exporter.CATALOG.values() for label in labels}
        <= {"source", "check"},
        "metric names are valid": all(re.fullmatch(r"[a-z][a-z0-9_]*", name) for name in host_state_exporter.CATALOG),
        "counters end in _total": all(name.endswith("_total") == (kind == "counter") for name, (_, _, kind) in host_state_exporter.CATALOG.items()),
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
}
