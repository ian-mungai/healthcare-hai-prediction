"""Cases for the monitoring file check: the committed files pass, and each planted defect in a copy of them is reported with its own message.

Every case copies the tracked monitoring folder and the Terraform file that mounts it into a scratch tree, changes one thing and runs the real check.
A case returns named checks. No Docker, AWS or network is used.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path

from scripts.infrastructure import monitoring_files

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = "monitoring/grafana/dashboards/hai_overview.json"
ALERTS = "monitoring/prometheus/alerts.yml"
SCRAPE = "monitoring/prometheus/prometheus.yml"
MAPPING = "monitoring/statsd/mapping.yml"
DATASOURCE = "monitoring/grafana/provisioning/datasources/prometheus.yml"
TERRAFORM = "infra/services/monitoring.tf"
Plant = Callable[[Path], None]


def scratch_copy(root: Path, destination: Path) -> Path:
    """A copy of the monitoring folder and its Terraform file below ``destination``."""
    shutil.copytree(root / "monitoring", destination / "monitoring")
    (destination / "infra/services").mkdir(parents=True)
    shutil.copy2(root / TERRAFORM, destination / TERRAFORM)
    return destination


def edit(tree: Path, relative: str, old: str, new: str) -> None:
    """Replace ``old`` with ``new`` in one file; refuse a plant that changed nothing, so a case cannot pass for the wrong reason."""
    path = tree / relative
    text = path.read_text(encoding="utf-8")
    if old not in text:
        raise AssertionError(f"the plant text is not in {relative}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def reported(root: Path, plant: Plant, expected: str) -> dict[str, bool]:
    """Plant one defect in a scratch copy: the check must report a problem with ``expected`` in it, and the unplanted copy must still pass."""
    with tempfile.TemporaryDirectory(prefix="monitoring_case_") as scratch:
        clean = scratch_copy(root, Path(scratch) / "clean")
        planted = scratch_copy(root, Path(scratch) / "planted")
        plant(planted)
        problems = monitoring_files.check(planted)
        return {
            "the unplanted copy passes": monitoring_files.check(clean) == [],
            f"the planted copy reports: {expected}": any(expected in problem for problem in problems),
        }


def committed_files_pass(root: Path) -> dict[str, bool]:
    """The tracked files satisfy every rule."""
    return {"the tracked monitoring files have no problem": monitoring_files.check(root) == []}


def dashboard_metric_typo(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, DASHBOARD, "airflow_dagrun_duration_seconds_count{state=", "airflow_dagrun_duration_secs_count{state="),
        "airflow_dagrun_duration_secs_count",
    )


def alert_metric_no_one_emits(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, ALERTS, "airflow_dag_processing_import_errors > 0", "airflow_dag_import_failures > 0"), "airflow_dag_import_failures"
    )


def dashboard_label_not_on_metric(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(
            tree, DASHBOARD, 'airflow_dagrun_duration_seconds_count{state=\\"success\\"}', 'airflow_dagrun_duration_seconds_count{status=\\"success\\"}'
        ),
        "label status",
    )


def api_metric_missing_from_the_contract(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, "monitoring/known_metrics.json", '"hai_api_requests_total"', '"hai_api_request_count"'), "hai_api_requests_total"
    )


def dashboard_is_not_json(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, DASHBOARD, '"title": "HAI overview",', '"title": "HAI overview" "x",'), "hai_overview.json is not valid JSON")


def dashboard_uses_an_unprovisioned_datasource(root: Path) -> dict[str, bool]:
    def plant(tree: Path) -> None:
        path = tree / DASHBOARD
        path.write_text(path.read_text(encoding="utf-8").replace('"uid": "hai_prometheus"', '"uid": "other_source"'), encoding="utf-8")

    return reported(root, plant, "other_source")


def dashboard_lacks_the_api_row(root: Path) -> dict[str, bool]:
    def plant(tree: Path) -> None:
        path = tree / DASHBOARD
        document = json.loads(path.read_text(encoding="utf-8"))
        document["panels"] = [panel for panel in document["panels"] if panel["type"] != "row" or panel["title"] != "API"]
        path.write_text(json.dumps(document), encoding="utf-8")

    return reported(root, plant, "row API")


def panel_without_an_expression(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, DASHBOARD, '"expr": "sum(airflow_dag_processing_import_errors)"', '"expr": ""'), "has no expression")


def alert_without_summary(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, ALERTS, "summary: The Airflow scheduler sent no heartbeat for 10 minutes.", "description: x"),
        "AirflowSchedulerSilent has no summary",
    )


def duplicate_alert_name(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, ALERTS, "alert: AirflowTaskFailed", "alert: AirflowDagRunFailed"), "AirflowDagRunFailed is defined twice")


def alert_for_a_job_nobody_scrapes(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "job_name: statsd_exporter", "job_name: statsd"), "statsd_exporter")


def rule_file_missing(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "/etc/prometheus/alerts.yml", "/etc/prometheus/rules.yml"), "rules.yml")


def scrape_port_differs_from_terraform(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "127.0.0.1:9102", "127.0.0.1:9103"), "9103")


def datasource_port_differs_from_terraform(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, DATASOURCE, "http://127.0.0.1:9090", "http://127.0.0.1:9091"), "9091")


def mapping_without_the_drop_rule(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, MAPPING, "action: drop", "action: map"), "drop")


def mapping_uses_a_missing_wildcard(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, MAPPING, 'dag_id: "$1"\n      state: "success"', 'dag_id: "$2"\n      state: "success"'), "$2")


def counter_without_total_suffix(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, MAPPING, "airflow_scheduler_heartbeat_total", "airflow_scheduler_heartbeat"), "airflow_scheduler_heartbeat")


def mapping_is_not_yaml(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, MAPPING, "mappings:", "mappings: ["), "mapping.yml is not valid YAML")


def mounted_folder_missing(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: shutil.rmtree(tree / "monitoring/statsd"), "monitoring/statsd")


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "committed_files_pass": committed_files_pass,
    "dashboard_metric_typo": dashboard_metric_typo,
    "alert_metric_no_one_emits": alert_metric_no_one_emits,
    "dashboard_label_not_on_metric": dashboard_label_not_on_metric,
    "api_metric_missing_from_the_contract": api_metric_missing_from_the_contract,
    "dashboard_is_not_json": dashboard_is_not_json,
    "dashboard_uses_an_unprovisioned_datasource": dashboard_uses_an_unprovisioned_datasource,
    "dashboard_lacks_the_api_row": dashboard_lacks_the_api_row,
    "panel_without_an_expression": panel_without_an_expression,
    "alert_without_summary": alert_without_summary,
    "duplicate_alert_name": duplicate_alert_name,
    "alert_for_a_job_nobody_scrapes": alert_for_a_job_nobody_scrapes,
    "rule_file_missing": rule_file_missing,
    "scrape_port_differs_from_terraform": scrape_port_differs_from_terraform,
    "datasource_port_differs_from_terraform": datasource_port_differs_from_terraform,
    "mapping_without_the_drop_rule": mapping_without_the_drop_rule,
    "mapping_uses_a_missing_wildcard": mapping_uses_a_missing_wildcard,
    "counter_without_total_suffix": counter_without_total_suffix,
    "mapping_is_not_yaml": mapping_is_not_yaml,
    "mounted_folder_missing": mounted_folder_missing,
}
