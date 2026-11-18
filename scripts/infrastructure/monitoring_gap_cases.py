"""Cases for the monitoring gaps: every service and job reports to Prometheus (owner, Nov 17 2026, "add the gaps").

Four gaps are covered, each with its dashboard panel and alert rule: the Streamlit and Evidently UI probes (the Evidently UI is reported disabled, never
down, while its switch is off), the OpenSearch cluster status, the last exit status and run time of the one-shot bootstrap tasks (read from ECS, read-only) and
the result of the online store reload at host start, pushed per set to the Pushgateway. A fake ECS client and loopback stand-ins answer; no AWS service is
contacted and every server is closed before its case returns.
"""

from __future__ import annotations

import json
import os
import re
import socket
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

import yaml

from monitoring import host_state_exporter as exporter
from scripts import metrics_push
from scripts.infrastructure.host_state_exporter_cases import CLUSTER, PREFIX, OpenMetadataStandIn, ProbeStandIn, address, values
from scripts.infrastructure.metrics_push_cases import quiet, valid_exposition

RUN = "features_0a1b2c3d"
TASK_ARN = "arn:aws:ecs:us-west-2:111111111111:task/" + CLUSTER + "/{}"


def stopped_task(name: str, minutes_ago: int, *exit_codes: int | None, stop_code: str = "EssentialContainerExited") -> dict[str, Any]:
    containers = [{"name": f"c{index}", **({} if code is None else {"exitCode": code})} for index, code in enumerate(exit_codes)]
    return {
        "taskArn": TASK_ARN.format(f"{name}-{minutes_ago}"),
        "group": f"family:{PREFIX}{name}",
        "stoppedAt": datetime.now(UTC) - timedelta(minutes=minutes_ago),
        "stopCode": stop_code,
        "containers": containers,
    }


class BootstrapEcs:
    """The two ECS reads of the bootstrap source. Tasks are listed by family; every call is kept so a case can judge its arguments."""

    def __init__(self, tasks: dict[str, list[dict[str, Any]]]) -> None:
        self.tasks = tasks
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def list_tasks(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(("list_tasks", arguments))
        family = str(arguments.get("family", "")).removeprefix(PREFIX)
        found = self.tasks.get(family, []) if arguments.get("desiredStatus") == "STOPPED" else []
        return {"taskArns": [task["taskArn"] for task in found]}

    def describe_tasks(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(("describe_tasks", arguments))
        wanted = set(arguments["tasks"])
        return {"tasks": [task for found in self.tasks.values() for task in found if task["taskArn"] in wanted], "failures": []}


NAMES = ("code_fetch", "polaris_bootstrap", "openmetadata_bootstrap")


def read(ecs: BootstrapEcs, memory: Any = None, names: tuple[str, ...] = NAMES) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return values(exporter.bootstrap_samples(ecs, CLUSTER, PREFIX, names, exporter.BootstrapMemory() if memory is None else memory))


def base_settings(**extra: str) -> exporter.Settings:
    return exporter.settings({"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2", **extra})


def refused(**extra: str) -> bool:
    try:
        base_settings(**extra)
    except ValueError:
        return True
    return False


# 1. Streamlit and the Evidently UI


def the_evidently_ui_is_probed_on_8600_only_while_its_switch_is_on(root: Path) -> dict[str, bool]:
    on, off = exporter.probe_table(True), exporter.probe_table(False)
    return {
        "with the switch on the probe is the Evidently UI version endpoint": on["evidently_ui"].url == "http://127.0.0.1:8600/api/version"
        and on["evidently_ui"].service == "evidently_ui",
        "with the switch off there is no probe to fail": "evidently_ui" not in off,
        "the Streamlit probe stays on 8501 /_stcore/health": on["streamlit"].url == "http://127.0.0.1:8501/_stcore/health"
        and off["streamlit"] == on["streamlit"],
        "every other probe is unchanged by the switch": {k: v for k, v in on.items() if k != "evidently_ui"} == off,
        "the address passes the loopback pattern": exporter.PROBE_URL.fullmatch(on["evidently_ui"].url) is not None,
    }


def a_disabled_evidently_ui_is_reported_disabled_and_never_down(root: Path) -> dict[str, bool]:
    labels = (("probe", "evidently_ui"), ("service", "evidently_ui"))
    off = values(exporter.disabled_probe_samples(False))
    on = values(exporter.disabled_probe_samples(True))
    with ProbeStandIn(503) as bad:
        down = values(exporter.probe_samples({"evidently_ui": exporter.Probe("evidently_ui", address(bad) + "/api/version")}, 3.0))
    return {
        "off reports the disabled series": off.get(("hai_service_probe_disabled", labels)) == 1,
        "off reports no up series": ("hai_service_probe_up", labels) not in off,
        "on reports no disabled series": not on,
        "an enabled UI that answers 503 is down, not disabled": down.get(("hai_service_probe_up", labels)) == 0,
    }


def the_probes_source_follows_the_switch_through_the_collector(root: Path) -> dict[str, bool]:
    def found(flag: str) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
        configured = base_settings(EVIDENTLY_UI_ENABLED=flag)
        with mock.patch.object(exporter, "PROBE_TIMEOUT_SECONDS", 0.3):
            return values(exporter.collectors(configured, object())["probes"]())

    labels = (("probe", "evidently_ui"), ("service", "evidently_ui"))
    off, on = found("false"), found("true")
    default = exporter.settings({"ECS_CLUSTER": CLUSTER, "AWS_REGION": "us-west-2"})
    return {
        "off: disabled is 1 and no up series": off.get(("hai_service_probe_disabled", labels)) == 1 and ("hai_service_probe_up", labels) not in off,
        "on: the probe is asked and no disabled series exists": ("hai_service_probe_up", labels) in on and ("hai_service_probe_disabled", labels) not in on,
        "the default is off, like the Terraform variable": default.evidently_ui is False,
        "a value other than true or false is refused": refused(EVIDENTLY_UI_ENABLED="yes") and refused(EVIDENTLY_UI_ENABLED=""),
    }


# 2. OpenSearch cluster status


def opensearch_status_is_a_number_per_colour(root: Path) -> dict[str, bool]:
    found: dict[str, float] = {}
    for colour in ("green", "yellow", "red"):
        with OpenMetadataStandIn(body={"cluster_name": "x", "status": colour, "number_of_nodes": 1}) as stand_in:
            found[colour] = values(exporter.opensearch_samples(stand_in.url))[("hai_opensearch_cluster_status", ())]
    return {
        "green is 0, yellow 1 and red 2": found == {"green": 0.0, "yellow": 1.0, "red": 2.0},
        "the metric has no label": exporter.CATALOG["hai_opensearch_cluster_status"][1] == (),
    }


def a_bad_opensearch_answer_is_a_source_error(root: Path) -> dict[str, bool]:
    def fails(**options: Any) -> bool:
        with OpenMetadataStandIn(**options) as stand_in:
            try:
                exporter.opensearch_samples(stand_in.url, 1.0)
            except exporter.SourceError:
                return True
        return False

    with OpenMetadataStandIn() as closed:
        url = closed.url
    try:
        exporter.opensearch_samples(url, 1.0)
        refused_port = False
    except exporter.SourceError:
        refused_port = True
    return {
        "an unknown status word is an error": fails(body={"status": "purple"}),
        "a body that is not JSON is an error": fails(raw=b"<html>"),
        "a body without a status is an error": fails(body={"cluster_name": "x"}),
        "a server error is an error": fails(status=503, raw=b"nope"),
        "a closed port is an error": refused_port,
    }


def the_opensearch_source_exists_only_with_its_address(root: Path) -> dict[str, bool]:
    url = "http://127.0.0.1:9200/_cluster/health"
    with_url = exporter.collectors(base_settings(OPENSEARCH_HEALTH_URL=url), object())
    without = exporter.collectors(base_settings(), object())
    return {
        "with the address there is an opensearch source": "opensearch" in with_url,
        "without it there is none, so a stack with no OpenMetadata never reports it down": "opensearch" not in without,
        "an address that is not loopback health is refused": refused(OPENSEARCH_HEALTH_URL="http://10.0.0.5:9200/_cluster/health")
        and refused(OPENSEARCH_HEALTH_URL="http://127.0.0.1:9200/_cat/indices"),
        "the source name is in the closed list": "opensearch" in exporter.SOURCES,
    }


# 3. Last status of the one-shot bootstrap tasks


def the_newest_stopped_task_of_each_family_gives_its_exit_code_and_time(root: Path) -> dict[str, bool]:
    newest = stopped_task("polaris_bootstrap", 5, 0)
    ecs = BootstrapEcs(
        {
            "polaris_bootstrap": [stopped_task("polaris_bootstrap", 50, 1), newest],
            "code_fetch": [stopped_task("code_fetch", 20, 0, 3)],
            "openmetadata_bootstrap": [stopped_task("openmetadata_bootstrap", 9, 0, 0)],
        }
    )
    found = read(ecs)
    return {
        "a family with an old failure and a new success reports the success": found[("hai_bootstrap_task_last_exit_code", (("task", "polaris_bootstrap"),))]
        == 0,
        "the run time is the stop time of the newest task": abs(
            found[("hai_bootstrap_task_last_run_timestamp_seconds", (("task", "polaris_bootstrap"),))] - newest["stoppedAt"].timestamp()
        )
        < 1,
        "the first non-zero exit code of any container is the code": found[("hai_bootstrap_task_last_exit_code", (("task", "code_fetch"),))] == 3,
        "all containers at zero give zero": found[("hai_bootstrap_task_last_exit_code", (("task", "openmetadata_bootstrap"),))] == 0,
        "no label other than the closed task name": {dict(labels)["task"] for _, labels in found} == set(NAMES),
    }


def a_task_that_never_started_has_exit_code_minus_one(root: Path) -> dict[str, bool]:
    ecs = BootstrapEcs({"code_fetch": [stopped_task("code_fetch", 3, None, stop_code="TaskFailedToStart")]})
    found = read(ecs, names=("code_fetch",))
    return {
        "no exit code recorded is -1, which a non-zero alert still catches": found[("hai_bootstrap_task_last_exit_code", (("task", "code_fetch"),))] == -1,
    }


def a_family_with_no_task_has_no_series(root: Path) -> dict[str, bool]:
    found = read(BootstrapEcs({}))
    return {"nothing is invented for a task that never ran": found == {}}


def ecs_forgets_stopped_tasks_but_the_last_result_is_kept(root: Path) -> dict[str, bool]:
    memory = exporter.BootstrapMemory()
    first = read(BootstrapEcs({"code_fetch": [stopped_task("code_fetch", 30, 0)]}), memory, ("code_fetch",))
    later = read(BootstrapEcs({}), memory, ("code_fetch",))
    newer = read(BootstrapEcs({"code_fetch": [stopped_task("code_fetch", 1, 2)]}), memory, ("code_fetch",))
    key = ("hai_bootstrap_task_last_exit_code", (("task", "code_fetch"),))
    return {
        "the first read has the result": first[key] == 0,
        "after ECS lists nothing the same result and time are still reported": later == first,
        "a newer task replaces it": newer[key] == 2,
    }


def the_bootstrap_reads_are_read_only_and_scoped_to_the_cluster(root: Path) -> dict[str, bool]:
    ecs = BootstrapEcs({name: [stopped_task(name, 4, 0)] for name in NAMES})
    read(ecs)
    methods = {method for method, _ in ecs.calls}
    policy = json.loads((root / "infra/iam/owner_policies/host_state_read.json").read_text(encoding="utf-8"))
    granted = {
        action for statement in policy["Statement"] for action in ([statement["Action"]] if isinstance(statement["Action"], str) else statement["Action"])
    }
    conditioned = {
        action: statement.get("Condition", {}).get("ArnEquals", {}).get("ecs:cluster")
        for statement in policy["Statement"]
        for action in ([statement["Action"]] if isinstance(statement["Action"], str) else statement["Action"])
    }
    return {
        "only ListTasks and DescribeTasks are called": methods == {"list_tasks", "describe_tasks"},
        "every call names the cluster": all(arguments.get("cluster") == CLUSTER for _, arguments in ecs.calls),
        "each family is listed by its own task definition family": {arguments["family"] for method, arguments in ecs.calls if method == "list_tasks"}
        == {PREFIX + name for name in NAMES},
        "only stopped tasks are listed": all(arguments.get("desiredStatus") == "STOPPED" for method, arguments in ecs.calls if method == "list_tasks"),
        "the policy already grants both actions": {"ecs:ListTasks", "ecs:DescribeTasks"} <= granted,
        "both are scoped with the same ecs:cluster condition": all(
            conditioned[action] == "${CLUSTER_ARN}" for action in ("ecs:ListTasks", "ecs:DescribeTasks")
        ),
    }


def an_ecs_failure_in_the_bootstrap_source_is_an_error(root: Path) -> dict[str, bool]:
    class Failing(BootstrapEcs):
        def describe_tasks(self, **arguments: Any) -> dict[str, Any]:
            return {"tasks": [], "failures": [{"reason": "MISSING"}]}

    try:
        read(Failing({"code_fetch": [stopped_task("code_fetch", 3, 0)]}), names=("code_fetch",))
        failed = False
    except exporter.SourceError:
        failed = True
    return {"a failure in the describe reply marks the source down": failed}


def the_bootstrap_settings_are_closed(root: Path) -> dict[str, bool]:
    configured = base_settings(BOOTSTRAP_TASKS="code_fetch,polaris_bootstrap")
    names = set(exporter.collectors(configured, BootstrapEcs({})))
    return {
        "the names are read": configured.bootstrap_tasks == ("code_fetch", "polaris_bootstrap"),
        "a bootstrap source exists only when names are set": "bootstrap" in names and "bootstrap" not in exporter.collectors(base_settings(), BootstrapEcs({})),
        "an unknown task name is refused": refused(BOOTSTRAP_TASKS="code_fetch,rm_rf"),
        "a duplicate is refused": refused(BOOTSTRAP_TASKS="code_fetch,code_fetch"),
        "the source name is in the closed list": "bootstrap" in exporter.SOURCES,
    }


def the_new_series_render_as_valid_closed_exposition(root: Path) -> dict[str, bool]:
    ecs = BootstrapEcs({"code_fetch": [stopped_task("code_fetch", 3, 0)]})
    with OpenMetadataStandIn(body={"status": "yellow"}) as stand_in:
        built = exporter.Exporter(
            {
                "bootstrap": lambda: exporter.bootstrap_samples(ecs, CLUSTER, PREFIX, ("code_fetch",), exporter.BootstrapMemory()),
                "opensearch": lambda: exporter.opensearch_samples(stand_in.url),
                "probes": lambda: exporter.disabled_probe_samples(False),
            }
        )
        built.refresh()
        text = built.render()
    return {
        "the exposition is valid": valid_exposition(text),
        "every new metric is exposed": all(
            name in text
            for name in (
                "hai_bootstrap_task_last_exit_code",
                "hai_bootstrap_task_last_run_timestamp_seconds",
                "hai_opensearch_cluster_status",
                "hai_service_probe_disabled",
            )
        ),
        "a task label outside the closed names is dropped": exporter.problem(exporter.Sample("hai_bootstrap_task_last_exit_code", 0, {"task": "x; drop"})),
        "a sample with another label set is dropped": exporter.problem(exporter.Sample("hai_opensearch_cluster_status", 0, {"status": "green"})),
    }


# 4. The reload push


class GroupGateway:
    """A loopback Pushgateway that stores groups under their whole path, so the grouping key with the set is visible."""

    def __init__(
        self, *, delay: float = 0.0, status: int = 200, delete_status: int = 202, delete_delay: float = 0.0, seed: dict[str, str] | None = None
    ) -> None:
        self.groups: dict[str, str] = dict(seed or {})
        self.requests: list[tuple[str, str]] = []
        self.delete_status, self.delete_delay = delete_status, delete_delay
        gateway = self

        class Handler(BaseHTTPRequestHandler):
            def handle_request(self, method: str) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode() if length else ""
                gateway.requests.append((method, self.path))
                time.sleep(delay + (gateway.delete_delay if method == "DELETE" else 0.0))
                if method == "DELETE":
                    if gateway.delete_status < 300:
                        gateway.groups.pop(self.path, None)
                elif status < 300 and valid_exposition(body):
                    gateway.groups[self.path] = body
                self.send_response(status if method == "PUT" else gateway.delete_status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_PUT(self) -> None:
                self.handle_request("PUT")

            def do_DELETE(self) -> None:
                self.handle_request("DELETE")

            def log_message(self, format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self) -> GroupGateway:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


GROUP = "/metrics/job/features/phase/reload/outcome/{}/feature_set/{}"
LOADED = json.dumps({"outcome": "loaded", "feature_set": "m4", "run_id": RUN, "base_run": RUN, "load_id": "1", "rows": 1234, "skipped_held": []})
SKIPPED_HELD = json.dumps({"outcome": "skipped", "feature_set": "m2", "reason": "free text, never a label", "skipped_held": [RUN]})
SKIPPED = json.dumps({"outcome": "skipped", "feature_set": "m1", "reason": "free text, never a label", "skipped_held": []})


def pushed(text: str | None, model: str, seconds: float = 12.5, **environ: str) -> tuple[GroupGateway, bool]:
    from scripts.features import feature_metrics

    with GroupGateway() as gateway:
        environment = {"PUSHGATEWAY_URL": gateway.url, **environ}
        with mock.patch.dict(os.environ, environment, clear=False):
            ok = bool(feature_metrics.push_reload(model, text + "\n" if text is not None else None, seconds))
    return gateway, ok


def a_loaded_reload_is_pushed_per_set_with_rows_and_duration(root: Path) -> dict[str, bool]:
    gateway, ok = pushed(LOADED, "m4")
    body = gateway.groups.get(GROUP.format("succeeded", "m4"), "")
    return {
        "the push is accepted": ok,
        "the group key is job, phase, outcome and the set": list(gateway.groups) == [GROUP.format("succeeded", "m4")],
        "the exposition is valid": valid_exposition(body),
        "the result is loaded": 'hai_online_reload_result{result="loaded"} 1' in body,
        "the rows are pushed": "hai_online_reload_rows 1234" in body,
        "the duration is pushed": "hai_online_reload_duration_seconds 12.5" in body,
        "the time is pushed": "hai_online_reload_timestamp_seconds" in body,
        "there is no reason series for a load": "hai_online_reload_skip_reason" not in body,
        "no run ID and no free text reaches the gateway": RUN not in body and "free text" not in body,
        "the stale failed group is deleted": ("DELETE", GROUP.format("failed", "m4")) in gateway.requests,
    }


def a_skipped_reload_names_a_closed_reason(root: Path) -> dict[str, bool]:
    none, _ = pushed(SKIPPED, "m1")
    held, _ = pushed(SKIPPED_HELD, "m2")
    none_body = none.groups.get(GROUP.format("succeeded", "m1"), "")
    held_body = held.groups.get(GROUP.format("succeeded", "m2"), "")
    return {
        "skipped with no evidence says no_load_evidence": 'hai_online_reload_skip_reason{reason="no_load_evidence"} 1' in none_body,
        "skipped behind a hold says held_not_released": 'hai_online_reload_skip_reason{reason="held_not_released"} 1' in held_body,
        "the result is skipped": 'hai_online_reload_result{result="skipped"} 1' in none_body,
        "a skip has no rows series": "hai_online_reload_rows" not in none_body,
        "the free text of the record never reaches the gateway": "free text" not in none_body + held_body,
    }


def a_failed_reload_is_pushed_as_failed_with_its_duration(root: Path) -> dict[str, bool]:
    gateway, ok = pushed(None, "m4", 7.0)
    body = gateway.groups.get(GROUP.format("failed", "m4"), "")
    return {
        "the push is accepted": ok,
        "the failed group holds result failed and the duration": 'hai_online_reload_result{result="failed"} 1' in body
        and "hai_online_reload_duration_seconds 7" in body,
        "the succeeded group of that set is deleted, so a failure never sits beside an old success": ("DELETE", GROUP.format("succeeded", "m4"))
        in gateway.requests,
    }


def the_grouping_key_is_bounded(root: Path) -> dict[str, bool]:
    with GroupGateway() as gateway:
        failures = []
        for bad in ("m4/../x", "M4", "", "a" * 40, "m 4", "m4\n"):
            with mock.patch.dict(os.environ, {"PUSHGATEWAY_URL": gateway.url}):
                from scripts.features import feature_metrics

                failures.append(bool(quiet(partial(feature_metrics.push_reload, bad, LOADED, 1.0))[0]))
        try:
            metrics_push.Pusher(gateway.url, "features", "reload", group={"run_id": RUN})
            other_key = False
        except ValueError:
            other_key = True
    return {
        "a set name outside the closed pattern sends nothing": not any(failures) and gateway.requests == [],
        "a grouping key other than feature_set is refused": other_key,
        "the phase reload is in the closed table": "reload" in metrics_push.JOB_PHASES["features"],
    }


def the_reload_push_ends_at_one_absolute_deadline(root: Path) -> dict[str, bool]:
    from scripts.features import feature_metrics

    with (
        GroupGateway(delay=2.0) as gateway,
        mock.patch.dict(os.environ, {"PUSHGATEWAY_URL": gateway.url}),
        mock.patch.object(metrics_push, "REQUEST_TIMEOUT", 0.5),
        mock.patch.object(metrics_push, "TOTAL_TIMEOUT", 1.0),
    ):
        started = time.monotonic()
        ok, _ = quiet(lambda: feature_metrics.push_reload("m4", LOADED, 1.0))
        elapsed = time.monotonic() - started
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PUSHGATEWAY_URL", None)
        nothing = feature_metrics.push_reload("m4", LOADED, 1.0)
    return {
        "a slow gateway ends the push at the total deadline": elapsed < 1.9 and not ok,
        "without an address nothing is sent and nothing raises": nothing is False,
    }


def the_reload_phase_pushes_its_result_and_a_failure_pushes_failed(root: Path) -> dict[str, bool]:
    from scripts.lakehouse import layer_run

    job = layer_run.Job(layer="features", run=RUN, mode="real", code=Path("/nonexistent"), feature_set="")

    def run(output: str | Exception) -> tuple[GroupGateway, BaseException | None]:
        def step(*_args: Any, **_kwargs: Any) -> str:
            if isinstance(output, Exception):
                raise output
            return output + "\n"

        raised: BaseException | None = None
        with (
            GroupGateway() as gateway,
            mock.patch.dict(os.environ, {layer_run.MODEL_VARIABLE: "m4", "PUSHGATEWAY_URL": gateway.url}),
            mock.patch.object(layer_run, "run_module", step),
        ):
            try:
                quiet(lambda: layer_run.reload(job))
            except BaseException as error:
                raised = error
        return gateway, raised

    refusal = layer_run.StepError("online_reload failed: refused")
    (ok, ok_raised), (failed, failed_raised) = run(LOADED), run(refusal)
    return {
        "a loaded reload pushes the succeeded group of its set": GROUP.format("succeeded", "m4") in ok.groups,
        "a loaded reload raises nothing": ok_raised is None,
        "a refused reload pushes the failed group": GROUP.format("failed", "m4") in failed.groups,
        "a refused reload still fails the job with its own error": failed_raised is refusal,
    }


def seeded(outcome: str, model: str = "m4") -> dict[str, str]:
    return {
        GROUP.format(outcome, model): '# HELP hai_online_reload_result old\n# TYPE hai_online_reload_result gauge\nhai_online_reload_result{result="old"} 1\n'
    }


def push_into(gateway: GroupGateway, text: str | None, model: str = "m4", *, request: float | None = None, total: float | None = None) -> tuple[bool, str]:
    from scripts.features import feature_metrics

    with (
        mock.patch.dict(os.environ, {"PUSHGATEWAY_URL": gateway.url}),
        mock.patch.object(metrics_push, "REQUEST_TIMEOUT", request if request is not None else metrics_push.REQUEST_TIMEOUT),
        mock.patch.object(metrics_push, "TOTAL_TIMEOUT", total if total is not None else metrics_push.TOTAL_TIMEOUT),
    ):
        ok, written = quiet(lambda: feature_metrics.push_reload(model, None if text is None else text + "\n", 3.0))
    return bool(ok), written


def a_set_has_one_current_group_after_each_transition(root: Path) -> dict[str, bool]:
    with GroupGateway(seed=seeded("failed")) as recovered:
        ok_recovered, _ = push_into(recovered, LOADED)
    with GroupGateway(seed=seeded("succeeded")) as broken:
        ok_broken, _ = push_into(broken, None)
    with GroupGateway(seed={**seeded("succeeded"), **seeded("succeeded", "m1")}) as others:
        push_into(others, None)
    return {
        "failed then loaded leaves only the succeeded group": ok_recovered and list(recovered.groups) == [GROUP.format("succeeded", "m4")],
        "succeeded then failed leaves only the failed group": ok_broken and list(broken.groups) == [GROUP.format("failed", "m4")],
        "the group of another set is never touched": GROUP.format("succeeded", "m1") in others.groups,
    }


def the_opposite_group_is_deleted_and_confirmed_before_the_publish(root: Path) -> dict[str, bool]:
    with GroupGateway(seed=seeded("failed")) as gateway:
        push_into(gateway, LOADED)
    methods = [method for method, _ in gateway.requests]
    return {
        "the delete comes first and the put second": methods == ["DELETE", "PUT"],
        "the delete names the opposite outcome": gateway.requests[0] == ("DELETE", GROUP.format("failed", "m4")),
    }


def a_rejected_delete_publishes_nothing_and_names_the_stale_group(root: Path) -> dict[str, bool]:
    with GroupGateway(delete_status=503, seed=seeded("succeeded")) as gateway:
        ok, written = push_into(gateway, None)
    return {
        "the push reports failure, so no caller sees a success": ok is False,
        "no failed group is published beside the old success": list(gateway.groups) == [GROUP.format("succeeded", "m4")],
        "no put was sent": all(method != "PUT" for method, _ in gateway.requests),
        "the message names the group that may be stale": "event=metrics_push_stale" in written
        and "outcome=succeeded" in written
        and "feature_set=m4" in written,
    }


def a_delete_that_fails_once_leaves_one_group_after_the_retry(root: Path) -> dict[str, bool]:
    with GroupGateway(delete_status=503, seed=seeded("succeeded")) as gateway:
        first, _ = push_into(gateway, None)
        gateway.delete_status = 202
        second, _ = push_into(gateway, None)
    return {
        "the first push fails": first is False,
        "the retry succeeds": second is True,
        "one current group is left, the failed one": list(gateway.groups) == [GROUP.format("failed", "m4")],
    }


def a_delete_that_times_out_publishes_nothing(root: Path) -> dict[str, bool]:
    with GroupGateway(delete_delay=1.5, seed=seeded("succeeded")) as gateway:
        started = time.monotonic()
        ok, written = push_into(gateway, None, request=0.3, total=1.0)
        elapsed = time.monotonic() - started
    return {
        "the push reports failure": ok is False,
        "no put follows the timed out delete": all(method != "PUT" for method, _ in gateway.requests),
        "it ends inside the total deadline": elapsed < 1.4,
        "the stale group is named": "event=metrics_push_stale" in written,
    }


def the_delete_and_the_publish_share_one_total_deadline(root: Path) -> dict[str, bool]:
    from scripts.features import feature_metrics

    seen: list[tuple[str, float]] = []

    def send(self: Any, method: str, outcome: str, body: str, timeout: float, *, quiet: bool = False) -> bool:
        seen.append((method, timeout))
        if len(seen) == 1:
            time.sleep(0.6)
        return True

    with (
        mock.patch.dict(os.environ, {"PUSHGATEWAY_URL": "http://127.0.0.1:9"}),
        mock.patch.object(metrics_push, "REQUEST_TIMEOUT", 1.0),
        mock.patch.object(metrics_push, "TOTAL_TIMEOUT", 1.2),
        mock.patch.object(metrics_push.Pusher, "send", send),
    ):
        ok = feature_metrics.push_reload("m4", LOADED + "\n", 1.0)
    return {
        "both requests were sent": ok is True and [method for method, _ in seen] == ["DELETE", "PUT"],
        "the first request is cut to the request timeout": seen[0][1] <= 1.0,
        "the second request gets only what is left of the total, not a fresh budget": seen[1][1] < 0.8,
    }


class TrickleOpenSearch:
    """A loopback peer that sends a valid cluster health answer one byte every ``gap`` seconds: ``body_only`` sends the headers at once."""

    def __init__(self, gap: float, *, body_only: bool = False) -> None:
        import socket

        self.gap, self.body_only, self.stopped = gap, body_only, threading.Event()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(4)
        self.listener.settimeout(0.2)
        self.thread = threading.Thread(target=self.serve, daemon=True)

    def serve(self) -> None:
        payload = b'{"status":"green"}'
        head = b"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n"
        while not self.stopped.is_set():
            try:
                connection, _ = self.listener.accept()
            except OSError:
                continue
            with connection:
                try:
                    connection.settimeout(1.0)
                    connection.recv(4096)
                    if self.body_only:
                        connection.sendall(head)
                        data = payload
                    else:
                        data = head + payload
                    for index in range(len(data)):
                        if self.stopped.is_set():
                            break
                        connection.sendall(data[index : index + 1])
                        time.sleep(self.gap)
                except OSError:
                    continue

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.listener.getsockname()[1]}/_cluster/health"

    def __enter__(self) -> TrickleOpenSearch:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stopped.set()
        self.listener.close()
        self.thread.join(timeout=10)


def opensearch_resolves_no_name_and_connects_once_within_the_deadline(root: Path) -> dict[str, bool]:
    resolved: list[str] = []
    real = socket.getaddrinfo

    def slow_resolver(host: Any, *arguments: Any, **options: Any) -> Any:
        resolved.append(str(host))
        time.sleep(0.2)
        return real(host, *arguments, **options)

    class StalledSocket:
        """A socket whose connect never completes: it waits for the timeout that was set on it, as a real stalled connect does."""

        connects: list[float] = []

        def __init__(self, *arguments: Any, **options: Any) -> None:
            self.timeout = 0.0

        def settimeout(self, value: float) -> None:
            self.timeout = value

        def connect(self, address: Any) -> None:
            StalledSocket.connects.append(self.timeout)
            time.sleep(self.timeout)
            raise TimeoutError("timed out")

        def close(self) -> None:
            return

        def __enter__(self) -> StalledSocket:
            return self

        def __exit__(self, *exc: object) -> None:
            return

    with OpenMetadataStandIn(body={"status": "green"}) as peer, mock.patch.object(socket, "getaddrinfo", slow_resolver):
        started = time.monotonic()
        try:
            found = values(exporter.opensearch_samples(peer.url, 0.1))
        except exporter.SourceError:
            found = {}
        prompt = time.monotonic() - started
    resolved_for_name: list[str] = []

    def resolve(host: object, *_: object, **__: object) -> list[Any]:
        resolved_for_name.append(str(host))
        return []

    with mock.patch.object(socket, "getaddrinfo", resolve):
        try:
            exporter.deadline_get("http://localhost:9200/_cluster/health", 1.0)
            name_refused = False
        except exporter.SourceError:
            name_refused = True
    with mock.patch.object(socket, "socket", StalledSocket):
        started = time.monotonic()
        try:
            exporter.deadline_get("http://127.0.0.1:9200/_cluster/health", 0.1)
            stalled_failed = False
        except exporter.SourceError:
            stalled_failed = True
        stalled = time.monotonic() - started
    return {
        "a numeric loopback address is never resolved, so a slow resolver costs nothing": resolved == []
        and found.get(("hai_opensearch_cluster_status", ())) == 0.0,
        "the read with a slow resolver ends well inside the budget": prompt < 0.18,
        "a host name is refused and no name lookup runs": name_refused and resolved_for_name == [],
        "a stalled connect makes exactly one attempt": len(StalledSocket.connects) == 1,
        "the connect gets no more than the budget": all(value <= 0.1 for value in StalledSocket.connects),
        "a stalled connect ends within the budget": stalled_failed and stalled < 0.2,
        "the settings refuse a name host": refused(OPENSEARCH_HEALTH_URL="http://localhost:9200/_cluster/health"),
    }


def an_opensearch_answer_over_the_cap_is_an_error(root: Path) -> dict[str, bool]:
    """A valid JSON answer padded with whitespace stays valid JSON when it is cut, so only the cap check can refuse it, never the JSON parser."""

    def read(pad: int) -> tuple[str, float | None]:
        raw = b'{"status":"green"}' + b" " * pad
        with OpenMetadataStandIn(raw=raw) as peer:
            try:
                answer = values(exporter.opensearch_samples(peer.url, 5.0))
            except exporter.SourceError as error:
                return str(error), None
            return "", answer.get(("hai_opensearch_cluster_status", ()))

    over, over_value = read(2 * 1024 * 1024)
    under, under_value = read(512 * 1024)
    return {
        "an answer above 1 MiB is refused with the cap message and no status": over == "the answer is not a bounded HTTP answer" and over_value is None,
        "an answer below the cap is read as green": under == "" and under_value == 0.0,
    }


def a_trickling_opensearch_answer_ends_at_one_absolute_deadline(root: Path) -> dict[str, bool]:
    def timed(**options: Any) -> tuple[bool, float]:
        with TrickleOpenSearch(0.02, **options) as peer:
            started = time.monotonic()
            try:
                exporter.opensearch_samples(peer.url, 0.06)
                failed = False
            except exporter.SourceError:
                failed = True
            return failed, time.monotonic() - started

    whole_failed, whole = timed()
    body_failed, body = timed(body_only=True)
    with TrickleOpenSearch(0.0) as quick:
        answer = values(exporter.opensearch_samples(quick.url, 2.0))
    return {
        "a byte-by-byte answer from the first byte is an error at the deadline": whole_failed and whole < 0.25,
        "a byte-by-byte body after quick headers is an error at the deadline": body_failed and body < 0.25,
        "a prompt answer still reads": answer.get(("hai_opensearch_cluster_status", ())) == 0.0,
    }


def the_loaded_record_carries_the_rows_of_a_fresh_load(root: Path) -> dict[str, bool]:
    from scripts.features import feast_sets, online_reload

    entities: dict[str, int] = {}

    def load(request: Any) -> str:
        entities[request.feature_set] = 321
        return "load-1"

    loader = feast_sets.SetLoader(load, lambda _request: None, entities)
    recovered = feast_sets.SetLoader(load, lambda _request: "load-0", entities)
    document = {"model_sets": {"m4": {"online": True}}}
    common: dict[str, Any] = {
        "mode": "real",
        "schema": (document, "sha"),
        "holds": mock.Mock(),
        "runs": lambda _name: [RUN],
        "production": lambda: {},
    }
    with (
        mock.patch.object(online_reload, "evidence_of", lambda *_a, **_k: ({"base_run": RUN}, False)),
        mock.patch.object(online_reload.feature_schema, "set_schema_sha256", lambda *_a, **_k: "h"),
        mock.patch.object(online_reload.feast_sets, "check_production", lambda *_a, **_k: None),
    ):
        fresh = online_reload.reload_checked("m4", loader=loader, **common)
        again = online_reload.reload_checked("m4", loader=recovered, **common)
    return {
        "a fresh load names its entity count": fresh["outcome"] == "loaded" and fresh["rows"] == 321,
        "an already loaded set loaded nothing and names no rows": again["outcome"] == "already_loaded" and "rows" not in again,
    }


# Dashboards and alert rules


def files(root: Path) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    panels: dict[str, list[dict[str, Any]]] = {}
    for path in (root / "monitoring/grafana/dashboards").glob("*.json"):
        panels[path.name] = json.loads(path.read_text(encoding="utf-8"))["panels"]
    rules: dict[str, dict[str, Any]] = {}
    for path in (root / "monitoring/grafana/provisioning/alerting").glob("rules_*.yml"):
        for group in yaml.safe_load(path.read_text(encoding="utf-8"))["groups"]:
            for rule in group["rules"]:
                rules[rule["uid"]] = rule
    return panels, rules


def expressions(item: dict[str, Any]) -> str:
    if "data" in item:
        return " ".join(str(step["model"].get("expr", "")) for step in item["data"])
    return " ".join(target["expr"] for target in item.get("targets", []))


def every_gap_has_a_panel_and_an_alert_rule(root: Path) -> dict[str, bool]:
    panels, rules = files(root)
    services = " ".join(expressions(panel) for panel in panels["hai_services.json"])
    store = " ".join(expressions(panel) for panel in panels["hai_feature_store.json"])
    alerts = " ".join(expressions(rule) for rule in rules.values())
    health = expressions(rules["service_health_down"])
    return {
        "a panel shows Streamlit and the Evidently UI with the disabled state": "hai_service_probe_disabled" in services
        and "evidently_ui" in services
        and "streamlit" in services,
        "a panel shows the OpenSearch cluster status": "hai_opensearch_cluster_status" in services,
        "a panel shows the last exit code of the bootstrap tasks": "hai_bootstrap_task_last_exit_code" in services,
        "a panel shows the time since the last bootstrap run": "hai_bootstrap_task_last_run_timestamp_seconds" in services,
        "the feature store dashboard shows the reload result, rows, duration and reason": all(
            name in store
            for name in ("hai_online_reload_result", "hai_online_reload_rows", "hai_online_reload_duration_seconds", "hai_online_reload_skip_reason")
        ),
        "the health alert reads every probe, so both UIs are covered": "hai_service_probe_up" in health
        and "probe=" not in health
        and "hai_service_tasks_desired" in health,
        "an alert fires on a red OpenSearch cluster": "opensearch_cluster_red" in rules
        and "hai_opensearch_cluster_status" in expressions(rules["opensearch_cluster_red"]),
        "an alert fires on a yellow OpenSearch cluster": "opensearch_cluster_yellow" in rules,
        "an alert fires on a failed bootstrap task": "bootstrap_task_failed" in rules
        and "hai_bootstrap_task_last_exit_code" in expressions(rules["bootstrap_task_failed"]),
        "an alert fires on a failed reload": "online_reload_failed" in rules and "hai_online_reload_result" in expressions(rules["online_reload_failed"]),
        "no alert reads a metric the panels do not know": "hai_bootstrap_task_last_exit_code" in alerts,
    }


def the_new_alerts_have_a_summary_a_severity_and_the_no_data_rule(root: Path) -> dict[str, bool]:
    _, rules = files(root)
    wanted = ("opensearch_cluster_red", "opensearch_cluster_yellow", "bootstrap_task_failed", "online_reload_failed")
    found = [rules[name] for name in wanted if name in rules]
    return {
        "all four rules exist": len(found) == 4,
        "each has a summary and a known severity": all(
            rule["annotations"]["summary"] and rule["labels"]["severity"] in ("warning", "critical") for rule in found
        ),
        "no data is OK, so a stack without the service stays quiet": all(rule["noDataState"] == "OK" for rule in found),
        "the red cluster alert is critical": rules.get("opensearch_cluster_red", {}).get("labels", {}).get("severity") == "critical",
        "a rule's duration is a whole number of minutes": all(re.fullmatch(r"[0-9]+m", str(rule.get("for", "0m"))) for rule in found),
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "gaps_the_evidently_ui_is_probed_on_8600_only_while_its_switch_is_on": the_evidently_ui_is_probed_on_8600_only_while_its_switch_is_on,
    "gaps_a_disabled_evidently_ui_is_reported_disabled_and_never_down": a_disabled_evidently_ui_is_reported_disabled_and_never_down,
    "gaps_the_probes_source_follows_the_switch_through_the_collector": the_probes_source_follows_the_switch_through_the_collector,
    "gaps_opensearch_status_is_a_number_per_colour": opensearch_status_is_a_number_per_colour,
    "gaps_a_bad_opensearch_answer_is_a_source_error": a_bad_opensearch_answer_is_a_source_error,
    "gaps_the_opensearch_source_exists_only_with_its_address": the_opensearch_source_exists_only_with_its_address,
    "gaps_the_newest_stopped_task_of_each_family_gives_its_exit_code_and_time": the_newest_stopped_task_of_each_family_gives_its_exit_code_and_time,
    "gaps_a_task_that_never_started_has_exit_code_minus_one": a_task_that_never_started_has_exit_code_minus_one,
    "gaps_a_family_with_no_task_has_no_series": a_family_with_no_task_has_no_series,
    "gaps_ecs_forgets_stopped_tasks_but_the_last_result_is_kept": ecs_forgets_stopped_tasks_but_the_last_result_is_kept,
    "gaps_the_bootstrap_reads_are_read_only_and_scoped_to_the_cluster": the_bootstrap_reads_are_read_only_and_scoped_to_the_cluster,
    "gaps_an_ecs_failure_in_the_bootstrap_source_is_an_error": an_ecs_failure_in_the_bootstrap_source_is_an_error,
    "gaps_the_bootstrap_settings_are_closed": the_bootstrap_settings_are_closed,
    "gaps_the_new_series_render_as_valid_closed_exposition": the_new_series_render_as_valid_closed_exposition,
    "gaps_a_loaded_reload_is_pushed_per_set_with_rows_and_duration": a_loaded_reload_is_pushed_per_set_with_rows_and_duration,
    "gaps_a_skipped_reload_names_a_closed_reason": a_skipped_reload_names_a_closed_reason,
    "gaps_a_failed_reload_is_pushed_as_failed_with_its_duration": a_failed_reload_is_pushed_as_failed_with_its_duration,
    "gaps_the_grouping_key_is_bounded": the_grouping_key_is_bounded,
    "gaps_the_reload_push_ends_at_one_absolute_deadline": the_reload_push_ends_at_one_absolute_deadline,
    "gaps_the_reload_phase_pushes_its_result_and_a_failure_pushes_failed": the_reload_phase_pushes_its_result_and_a_failure_pushes_failed,
    "gaps_a_set_has_one_current_group_after_each_transition": a_set_has_one_current_group_after_each_transition,
    "gaps_the_opposite_group_is_deleted_and_confirmed_before_the_publish": the_opposite_group_is_deleted_and_confirmed_before_the_publish,
    "gaps_a_rejected_delete_publishes_nothing_and_names_the_stale_group": a_rejected_delete_publishes_nothing_and_names_the_stale_group,
    "gaps_a_delete_that_fails_once_leaves_one_group_after_the_retry": a_delete_that_fails_once_leaves_one_group_after_the_retry,
    "gaps_a_delete_that_times_out_publishes_nothing": a_delete_that_times_out_publishes_nothing,
    "gaps_the_delete_and_the_publish_share_one_total_deadline": the_delete_and_the_publish_share_one_total_deadline,
    "gaps_opensearch_resolves_no_name_and_connects_once_within_the_deadline": opensearch_resolves_no_name_and_connects_once_within_the_deadline,
    "gaps_an_opensearch_answer_over_the_cap_is_an_error": an_opensearch_answer_over_the_cap_is_an_error,
    "gaps_a_trickling_opensearch_answer_ends_at_one_absolute_deadline": a_trickling_opensearch_answer_ends_at_one_absolute_deadline,
    "gaps_the_loaded_record_carries_the_rows_of_a_fresh_load": the_loaded_record_carries_the_rows_of_a_fresh_load,
    "gaps_every_gap_has_a_panel_and_an_alert_rule": every_gap_has_a_panel_and_an_alert_rule,
    "gaps_the_new_alerts_have_a_summary_a_severity_and_the_no_data_rule": the_new_alerts_have_a_summary_a_severity_and_the_no_data_rule,
}
