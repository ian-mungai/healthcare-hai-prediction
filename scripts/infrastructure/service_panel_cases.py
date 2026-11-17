"""Cases for the Polaris, MLflow and Grafana panels and alert rules: the committed files carry the requested panels, and each planted defect is reported.

A metric name in these dashboards must be one that the pinned version really exposes. The names come from the pinned sources and are recorded in
``monitoring/known_metrics.json``: Polaris 1.8.0 (Quarkus 3.39.3, Micrometer 1.17.1), MLflow 3.17.2 (prometheus-flask-exporter 0.23.2, prometheus-client 0.26.0)
and Grafana 13.2.5. Every case copies the tracked monitoring folder into a scratch tree, plants one defect and runs the real check.
No Docker, AWS or network is used.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.infrastructure.monitoring_cases import KNOWN, edit, reported

DASHBOARDS = "monitoring/grafana/dashboards"
POLARIS = f"{DASHBOARDS}/hai_polaris.json"
MLFLOW = f"{DASHBOARDS}/hai_mlflow.json"
GRAFANA = f"{DASHBOARDS}/hai_grafana.json"
RULES = "monitoring/grafana/provisioning/alerting/rules_service_metrics.yml"
POLARIS_TERRAFORM = "infra/services/polaris.tf"
HISTOGRAM_SETTING = "POLARIS_METRICS_HTTP_SERVER_REQUESTS_HISTOGRAM_SLOS"
BUCKET = "http_server_requests_seconds_bucket"
PINNED = {"polaris": "1.8.0", "mlflow": "3.17.2", "grafana": "13.2.5"}
WANTED_RULES = (
    "PolarisServerErrors",
    "PolarisHeapHigh",
    "MlflowServerErrors",
    "MlflowSlowRequests",
    "GrafanaRuleEvaluationFailing",
    "GrafanaNotificationsFailing",
    "GrafanaSchedulerBehind",
)


def expressions(root: Path, relative: str) -> list[str]:
    """Every query expression of one dashboard file; an empty list when the file is missing or is not a dashboard."""
    path = root / relative
    if not path.is_file():
        return []
    try:
        document: Any = json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return []
    panels = document.get("panels", []) if isinstance(document, dict) else []
    return [str(target.get("expr", "")) for panel in panels if isinstance(panel, dict) for target in panel.get("targets", [])]


def mentions(found: list[str], *needles: str) -> bool:
    """True when one expression holds every needle, so a request for a panel is met by one query that does what it says."""
    return any(all(needle in expression for needle in needles) for expression in found)


def polaris_panels_cover_the_request(root: Path) -> dict[str, bool]:
    found = expressions(root, POLARIS)
    return {
        "the Polaris dashboard has panels": len(found) >= 8,
        "request rate": mentions(found, "rate(http_server_requests_seconds_count{", 'job="polaris"'),
        "4xx rate": mentions(found, "http_server_requests_seconds_count", 'status=~"4.."'),
        "5xx rate": mentions(found, "http_server_requests_seconds_count", 'status=~"5.."'),
        "error ratio divides by all requests": mentions(found, 'status=~"5.."', "/ sum(rate(http_server_requests_seconds_count"),
        "mean latency from the sum and the count": mentions(found, "http_server_requests_seconds_sum", "http_server_requests_seconds_count"),
        "latency by operation is limited by topk": mentions(found, "topk(", "http_server_requests_seconds_sum", "method, uri"),
        "slowest recent request by operation": mentions(found, "http_server_requests_seconds_max", "topk("),
        "95th percentile latency": mentions(found, "histogram_quantile(0.95", "sum by (le) (rate(http_server_requests_seconds_bucket{"),
        "95th percentile latency by operation, limited by topk": mentions(found, "topk(10, histogram_quantile(0.95", "sum by (le, method, uri)"),
        "JVM heap used": mentions(found, 'jvm_memory_used_bytes{job="polaris",area="heap"}'),
        "JVM heap share of the maximum": mentions(found, "jvm_memory_used_bytes", "clamp_min(jvm_memory_max_bytes"),
    }


def mlflow_panels_cover_the_request(root: Path) -> dict[str, bool]:
    found = expressions(root, MLFLOW)
    return {
        "the MLflow dashboard has panels": len(found) >= 7,
        "request rate": mentions(found, "rate(mlflow_http_request_total{", 'job="mlflow"'),
        "4xx rate": mentions(found, "mlflow_http_request_total", 'status=~"4.."'),
        "5xx rate": mentions(found, "mlflow_http_request_total", 'status=~"5.."'),
        "unauthorized rate": mentions(found, "mlflow_http_request_total", 'status="401"'),
        "exceptions": mentions(found, "mlflow_http_request_exceptions_total"),
        "95th percentile latency": mentions(found, "histogram_quantile(0.95", "mlflow_http_request_duration_seconds_bucket"),
        "95th percentile latency by method": mentions(found, "histogram_quantile(0.95", "sum by (le, method)"),
        "mean latency": mentions(found, "mlflow_http_request_duration_seconds_sum", "mlflow_http_request_duration_seconds_count"),
    }


def grafana_panels_cover_the_request(root: Path) -> dict[str, bool]:
    found = expressions(root, GRAFANA)
    return {
        "the Grafana dashboard has panels": len(found) >= 10,
        "rule evaluations": mentions(found, "grafana_alerting_rule_evaluations_total", 'job="grafana"'),
        "rule evaluation failures": mentions(found, "grafana_alerting_rule_evaluation_failures_total"),
        "rule evaluation duration": mentions(found, "histogram_quantile(0.95", "grafana_alerting_rule_evaluation_duration_seconds_bucket"),
        "scheduler lag": mentions(found, "grafana_alerting_scheduler_behind_seconds"),
        "alerts by state": mentions(found, "grafana_alerting_alertmanager_alerts", "sum by (state)"),
        "notifications by integration": mentions(found, "grafana_alerting_notifications_total", "sum by (integration)"),
        "failed notifications": mentions(found, "grafana_alerting_notifications_failed_total"),
        "notification latency": mentions(found, "grafana_alerting_notification_latency_seconds_bucket"),
        "data source requests": mentions(found, "grafana_datasource_request_total", "sum by (code)"),
        "data source latency": mentions(found, "histogram_quantile(0.95", "grafana_datasource_request_duration_seconds_bucket"),
        "data source requests in flight": mentions(found, "grafana_datasource_request_in_flight"),
    }


def alert_rules_exist(root: Path) -> dict[str, bool]:
    path = root / RULES
    text = path.read_text(encoding="utf-8") if path.is_file() else ""
    titles = set(re.findall(r"^\s+title:\s*(\S+)\s*$", text, flags=re.MULTILINE))
    return {f"the rule {title} exists": title in titles for title in WANTED_RULES}


def known_list_names_the_pinned_versions(root: Path) -> dict[str, bool]:
    path = root / KNOWN
    document = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    checks: dict[str, bool] = {}
    for source, version in PINNED.items():
        entry = document.get(source)
        emitted_by = str(entry.get("emitted_by", "")) if isinstance(entry, dict) else ""
        metrics = entry.get("metrics") if isinstance(entry, dict) else None
        checks[f"{source} lists its metrics with their labels"] = isinstance(metrics, dict) and len(metrics) > 0
        checks[f"{source} names the pinned version {version} as its source"] = version in emitted_by
    return checks


def polaris_latency_buckets_need_the_histogram_setting(root: Path) -> dict[str, bool]:
    """Polaris 1.8.0 publishes a latency bucket only when the SLO setting is set: the setting, the known list and the panel must agree."""
    known = json.loads((root / KNOWN).read_text(encoding="utf-8"))
    listed = known.get("polaris", {}).get("metrics", {}).get(BUCKET)
    terraform = (root / POLARIS_TERRAFORM).read_text(encoding="utf-8")
    value = re.search(rf'name\s*=\s*"{HISTOGRAM_SETTING}",\s*value\s*=\s*"([^"]*)"', terraform)
    found = expressions(root, POLARIS)
    return {
        "the Polaris task sets the histogram setting": value is not None,
        "the setting is a list of durations that Quarkus reads": value is not None
        and re.fullmatch(r"[0-9]+(ms|s)(,[0-9]+(ms|s))*", value.group(1)) is not None,
        "the known list names the bucket with its le label": isinstance(listed, dict) and "le" in listed.get("labels", []),
        "the 95th percentile panel reads the bucket": mentions(found, BUCKET),
    }


def polaris_panel_reads_a_bucket_nobody_publishes(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, POLARIS, "http_server_requests_seconds_bucket", "http_server_requests_seconds_buckets"),
        "http_server_requests_seconds_buckets is not a metric",
    )


def polaris_task_drops_the_histogram_setting(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, POLARIS_TERRAFORM, f'{{ name = "{HISTOGRAM_SETTING}", value = "10ms,50ms,100ms,500ms,1s,5s" }},\n', ""),
        f"does not set {HISTOGRAM_SETTING}",
    )


def polaris_histogram_setting_is_not_a_duration_list(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, POLARIS_TERRAFORM, "10ms,50ms,100ms,500ms,1s,5s", "10ms,fast"), "is not a list of durations")


def polaris_bucket_is_left_off_the_known_list(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, KNOWN, '"http_server_requests_seconds_bucket"', '"http_server_requests_seconds_buckets"'), "lacks the bucket")


def polaris_panel_label_not_on_the_metric(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(
            tree, POLARIS, 'jvm_memory_used_bytes{job=\\"polaris\\",area=\\"heap\\"}', 'jvm_memory_used_bytes{job=\\"polaris\\",region=\\"heap\\"}'
        ),
        "label region",
    )


def polaris_panel_groups_by_uri_without_topk(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, POLARIS, "topk(10, ", "("), "groups by uri without topk")


def mlflow_panel_asks_for_a_route_label(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(
            tree, MLFLOW, 'mlflow_http_request_total{job=\\"mlflow\\",status=~\\"5..\\"}', 'mlflow_http_request_total{job=\\"mlflow\\",route=~\\"5..\\"}'
        ),
        "mlflow_http_request_total has no label route",
    )


def mlflow_panel_metric_is_renamed(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, MLFLOW, "mlflow_http_request_duration_seconds_bucket", "mlflow_http_request_latency_seconds_bucket"),
        "mlflow_http_request_latency_seconds_bucket is not a metric",
    )


def grafana_panel_metric_is_renamed(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, GRAFANA, "grafana_alerting_rule_evaluations_total", "grafana_alerting_rule_evals_total"),
        "grafana_alerting_rule_evals_total is not a metric",
    )


def grafana_panel_reads_an_unscraped_job(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, GRAFANA, 'grafana_datasource_request_total{job=\\"grafana\\"', 'grafana_datasource_request_total{job=\\"grafana_server\\"'),
        "the job grafana_server is not a scrape job",
    )


def alert_rule_reads_a_metric_nobody_exposes(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES, "jvm_memory_max_bytes", "jvm_memory_limit_bytes"), "jvm_memory_limit_bytes is not a metric")


def known_list_loses_a_polaris_metric(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, KNOWN, '"http_server_requests_seconds_count"', '"http_server_requests_total"'), "http_server_requests_seconds_count"
    )


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "service_polaris_panels_cover_the_request": polaris_panels_cover_the_request,
    "service_mlflow_panels_cover_the_request": mlflow_panels_cover_the_request,
    "service_grafana_panels_cover_the_request": grafana_panels_cover_the_request,
    "service_alert_rules_exist": alert_rules_exist,
    "service_known_list_names_the_pinned_versions": known_list_names_the_pinned_versions,
    "service_polaris_latency_buckets_need_the_histogram_setting": polaris_latency_buckets_need_the_histogram_setting,
    "service_polaris_panel_reads_a_bucket_nobody_publishes": polaris_panel_reads_a_bucket_nobody_publishes,
    "service_polaris_task_drops_the_histogram_setting": polaris_task_drops_the_histogram_setting,
    "service_polaris_histogram_setting_is_not_a_duration_list": polaris_histogram_setting_is_not_a_duration_list,
    "service_polaris_bucket_is_left_off_the_known_list": polaris_bucket_is_left_off_the_known_list,
    "service_polaris_panel_label_not_on_the_metric": polaris_panel_label_not_on_the_metric,
    "service_polaris_panel_groups_by_uri_without_topk": polaris_panel_groups_by_uri_without_topk,
    "service_mlflow_panel_asks_for_a_route_label": mlflow_panel_asks_for_a_route_label,
    "service_mlflow_panel_metric_is_renamed": mlflow_panel_metric_is_renamed,
    "service_grafana_panel_metric_is_renamed": grafana_panel_metric_is_renamed,
    "service_grafana_panel_reads_an_unscraped_job": grafana_panel_reads_an_unscraped_job,
    "service_alert_rule_reads_a_metric_nobody_exposes": alert_rule_reads_a_metric_nobody_exposes,
    "service_known_list_loses_a_polaris_metric": known_list_loses_a_polaris_metric,
}
