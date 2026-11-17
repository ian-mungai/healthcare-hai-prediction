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
from scripts.infrastructure.host_state_exporter_cases import CASES as EXPORTER_CASES
from scripts.infrastructure.host_state_policy_cases import CASES as HOST_POLICY_CASES
from scripts.infrastructure.job_identity_cases import CASES as JOB_IDENTITY_CASES
from scripts.infrastructure.layer_publish_cases import CASES as PUBLISH_CASES
from scripts.infrastructure.metrics_push_cases import CASES as PUSH_CASES
from scripts.infrastructure.monitoring_panel_cases import CASES as PANEL_CASES
from scripts.infrastructure.postgres_activity_cases import CASES as POSTGRES_CASES
from scripts.infrastructure.publisher_metrics_cases import CASES as PUBLISHER_CASES
from scripts.infrastructure.redis_acl_cases import CASES as REDIS_CASES
from scripts.infrastructure.spark_progress_cases import CASES as SPARK_CASES

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = "monitoring/grafana/dashboards/hai_overview.json"
ALERTS = "monitoring/prometheus/alerts.yml"
ALERTING = "monitoring/grafana/provisioning/alerting"
RULES_AIRFLOW = f"{ALERTING}/rules_airflow.yml"
RULES_API = f"{ALERTING}/rules_api.yml"
RULES_MONITORING = f"{ALERTING}/rules_monitoring.yml"
CONTACT = f"{ALERTING}/contact_points.yml"
POLICIES = f"{ALERTING}/policies.yml"
RULES_JOBS = f"{ALERTING}/rules_jobs.yml"
RULES_SPARK = f"{ALERTING}/rules_spark.yml"
KNOWN = "monitoring/known_metrics.json"
SCRAPE = "monitoring/prometheus/prometheus.yml"
MAPPING = "monitoring/statsd/mapping.yml"
DATASOURCE = "monitoring/grafana/provisioning/datasources/prometheus.yml"
TERRAFORM = "infra/services/monitoring.tf"
Plant = Callable[[Path], None]


def scratch_copy(root: Path, destination: Path) -> Path:
    """A copy of the monitoring folder and its Terraform file below ``destination``."""
    shutil.copytree(root / "monitoring", destination / "monitoring")
    (destination / "infra/services").mkdir(parents=True)
    for relative in (TERRAFORM, "infra/services/polaris.tf", "infra/services/openmetadata.tf"):
        shutil.copy2(root / relative, destination / relative)
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
        root,
        lambda tree: edit(tree, RULES_AIRFLOW, "airflow_dag_processing_import_errors > bool 0", "airflow_dag_import_failures > bool 0"),
        "airflow_dag_import_failures",
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
        lambda tree: edit(tree, RULES_AIRFLOW, "summary: The Airflow scheduler sent no heartbeat for 10 minutes.", "description: x"),
        "AirflowSchedulerSilent has no summary",
    )


def duplicate_alert_name(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, RULES_AIRFLOW, "title: AirflowTaskFailed", "title: AirflowDagRunFailed"), "AirflowDagRunFailed is defined twice"
    )


def duplicate_alert_uid(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, RULES_AIRFLOW, "uid: airflow_task_failed", "uid: airflow_dag_run_failed"), "uid airflow_dag_run_failed is used twice"
    )


def alert_for_a_job_nobody_scrapes(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "job_name: statsd_exporter", "job_name: statsd"), "statsd_exporter")


def alerts_file_is_still_there(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: (tree / ALERTS).write_text("groups: []\n", encoding="utf-8"), f"{ALERTS} must not exist")


def prometheus_still_loads_rule_files(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "scrape_configs:", "rule_files:\n  - /etc/prometheus/alerts.yml\n\nscrape_configs:"), "rule_files")


def scrape_port_differs_from_terraform(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "127.0.0.1:9102", "127.0.0.1:9103"), "9103")


def datasource_port_differs_from_terraform(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, DATASOURCE, "http://127.0.0.1:9090", "http://127.0.0.1:9099"), "9099")


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


def scrape_job_without_targets(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, 'targets: ["127.0.0.1:9102"]', "targets: []"), "job statsd_exporter must scrape 127.0.0.1:9102")


def scrape_job_with_another_target(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, SCRAPE, 'targets: ["127.0.0.1:8000"]', 'targets: ["127.0.0.1:8001"]'), "job hai_api must scrape 127.0.0.1:8000"
    )


def scheduler_alert_misses_a_silent_start(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, RULES_AIRFLOW, " or absent(airflow_scheduler_heartbeat_total)", ""),
        "AirflowSchedulerSilent must also fire when no heartbeat series exists",
    )


def rule_label_not_on_metric(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, RULES_AIRFLOW, "airflow_task_instance_finished_total{state=", "airflow_task_instance_finished_total{status="),
        "label status",
    )


def rule_severity_is_unknown(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES_AIRFLOW, "severity: critical", "severity: page"), "AirflowSchedulerSilent needs a severity")


def rule_condition_names_a_missing_step(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES_AIRFLOW, "condition: C", "condition: Z"), "condition Z is not a step")


def rule_reads_an_unprovisioned_datasource(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES_AIRFLOW, "datasourceUid: hai_prometheus", "datasourceUid: other"), "uses the data source other")


def rule_is_paused(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES_AIRFLOW, "execErrState: Error", "execErrState: Error\n        isPaused: true"), "is paused")


def rule_duration_is_not_valid(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES_AIRFLOW, "for: 10m", "for: 10 minutes"), "10 minutes")


def rule_names_another_receiver(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, RULES_API, "severity: critical", "severity: critical\n        notification_settings:\n          receiver: other"),
        "routes to other",
    )


def rule_file_has_no_rules(root: Path) -> dict[str, bool]:
    def plant(tree: Path) -> None:
        for path in (tree / ALERTING).glob("rules_*.yml"):
            path.unlink()

    return reported(root, plant, "holds no alert rule")


def contact_point_has_a_literal_topic(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, CONTACT, "topic_arn: ${ALERTS_TOPIC_ARN}", "topic_arn: arn:aws:sns:us-west-2:111111111111:example_alerts"),
        "topic_arn must be ${ALERTS_TOPIC_ARN}",
    )


def contact_point_has_a_static_key(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, CONTACT, "          sigv4:\n", "          sigv4:\n            access_key: x\n"), "access_key")


def contact_point_is_not_sns(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, CONTACT, "type: sns", "type: email"), "must be of type sns")


def contact_point_has_no_subject(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, CONTACT, "          subject: '[{{ .Status | toUpper }}] {{ .CommonLabels.alertname }}'\n", ""), "needs a subject"
    )


def contact_point_file_is_missing(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: (tree / CONTACT).unlink(), f"{CONTACT} does not exist")


def policy_default_receiver_is_another(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, POLICIES, "receiver: hai_sns", "receiver: other"), "default receiver")


def policy_has_a_route_to_another_receiver(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, POLICIES, "    group_by:", "    routes:\n      - receiver: other\n    group_by:"), "routes to other")


def grafana_environment_lacks_the_topic_arn(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, TERRAFORM, '{ name = "ALERTS_TOPIC_ARN", value = var.alerts_topic_arn },', ""), "ALERTS_TOPIC_ARN")


def dropped_panel_reads_the_unmapped_counter(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, DASHBOARD, 'statsd_exporter_events_actions_total{action=\\"drop\\"}', "statsd_exporter_events_unmapped_total"),
        "statsd_exporter_events_unmapped_total",
    )


def pushgateway_scrape_drops_the_pushed_labels(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "honor_labels: true", "honor_labels: false"), "pushgateway must set honor_labels")


def pushgateway_scrape_port_differs_from_terraform(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "127.0.0.1:9091", "127.0.0.1:9092"), "job pushgateway must scrape 127.0.0.1:9091")


def host_state_exporter_scrape_is_missing(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "job_name: host_state_exporter", "job_name: host_state"), "no scrape job host_state_exporter")


def known_list_lacks_a_pushed_metric(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, KNOWN, '"hai_job_duration_seconds": ', '"hai_job_duration_secs": '), "hai_job_duration_seconds")


def known_list_gives_a_pushed_metric_another_label_set(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(
            tree,
            KNOWN,
            '"hai_drift_share": {"type": "gauge", "labels": ["job", "phase", "outcome", "model_set", "job_type", "hai_type"]}',
            '"hai_drift_share": {"type": "gauge", "labels": ["job", "phase", "outcome", "model_set", "job_type"]}',
        ),
        "hai_drift_share",
    )


def known_list_lacks_a_host_state_metric(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, KNOWN, '"hai_host_memory_remaining_mib"', '"hai_host_memory_free_mib"'), "hai_host_memory_remaining_mib")


def job_rule_names_a_family_nobody_pushes(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES_JOBS, 'up{job="pushgateway"}', 'up{job="pushgateways"}'), "pushgateways")


def job_rule_uses_a_label_the_pushed_metric_lacks(root: Path) -> dict[str, bool]:
    return reported(
        root,
        lambda tree: edit(tree, RULES_JOBS, 'hai_job_finish_timestamp_seconds{outcome="succeeded"}', 'hai_job_finish_timestamp_seconds{result="succeeded"}'),
        "label result",
    )


def spark_rule_reads_a_metric_nobody_pushes(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES_SPARK, "hai_spark_tasks_failed", "hai_spark_task_failures"), "hai_spark_task_failures")


def new_dashboard_without_a_row_is_reported(root: Path) -> dict[str, bool]:
    def plant(tree: Path) -> None:
        path = tree / "monitoring/grafana/dashboards/hai_jobs.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document["panels"] = [panel for panel in document["panels"] if panel.get("type") != "row"]
        path.write_text(json.dumps(document), encoding="utf-8")

    return reported(root, plant, "lies above the first row")


def dashboard_uid_is_used_twice(root: Path) -> dict[str, bool]:
    def plant(tree: Path) -> None:
        for name, other in (("hai_drift", "hai_jobs"),):
            path = tree / f"monitoring/grafana/dashboards/{name}.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["uid"] = other
            path.write_text(json.dumps(document), encoding="utf-8")

    return reported(root, plant, "uid hai_jobs is used by two dashboards")


def polaris_scrape_port_differs(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "127.0.0.1:8182", "127.0.0.1:8183"), "job polaris must scrape 127.0.0.1:8182")


def polaris_scrape_path_is_wrong(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "metrics_path: /q/metrics", "metrics_path: /metrics"), "job polaris must use the path /q/metrics")


def openmetadata_scrape_path_is_wrong(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, SCRAPE, "metrics_path: /prometheus", "metrics_path: /metrics"), "job openmetadata must use the path /prometheus"
    )


def mlflow_scrape_skips_the_relay(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, SCRAPE, "metrics_path: /mlflow_metrics", "metrics_path: /metrics"), "job mlflow must use the path /mlflow_metrics"
    )


def redis_exporter_keeps_every_db_series(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "action: drop", "action: keep"), "redis_exporter must drop")


def node_exporter_scrape_port_differs(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "127.0.0.1:9100", "127.0.0.1:9101"), "job node_exporter must scrape 127.0.0.1:9100")


def exporter_scrape_job_is_missing(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, SCRAPE, "job_name: postgres_exporter", "job_name: pg"), "no scrape job postgres_exporter")


def known_list_lacks_redis_up(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, KNOWN, '"redis_up"', '"redis_status"'), "redis_up")


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
    "duplicate_alert_uid": duplicate_alert_uid,
    "alert_for_a_job_nobody_scrapes": alert_for_a_job_nobody_scrapes,
    "alerts_file_is_still_there": alerts_file_is_still_there,
    "prometheus_still_loads_rule_files": prometheus_still_loads_rule_files,
    "scrape_port_differs_from_terraform": scrape_port_differs_from_terraform,
    "datasource_port_differs_from_terraform": datasource_port_differs_from_terraform,
    "mapping_without_the_drop_rule": mapping_without_the_drop_rule,
    "mapping_uses_a_missing_wildcard": mapping_uses_a_missing_wildcard,
    "counter_without_total_suffix": counter_without_total_suffix,
    "mapping_is_not_yaml": mapping_is_not_yaml,
    "mounted_folder_missing": mounted_folder_missing,
    "scrape_job_without_targets": scrape_job_without_targets,
    "scrape_job_with_another_target": scrape_job_with_another_target,
    "scheduler_alert_misses_a_silent_start": scheduler_alert_misses_a_silent_start,
    "rule_label_not_on_metric": rule_label_not_on_metric,
    "rule_severity_is_unknown": rule_severity_is_unknown,
    "rule_condition_names_a_missing_step": rule_condition_names_a_missing_step,
    "rule_reads_an_unprovisioned_datasource": rule_reads_an_unprovisioned_datasource,
    "rule_is_paused": rule_is_paused,
    "rule_duration_is_not_valid": rule_duration_is_not_valid,
    "rule_names_another_receiver": rule_names_another_receiver,
    "rule_file_has_no_rules": rule_file_has_no_rules,
    "contact_point_has_a_literal_topic": contact_point_has_a_literal_topic,
    "contact_point_has_a_static_key": contact_point_has_a_static_key,
    "contact_point_is_not_sns": contact_point_is_not_sns,
    "contact_point_has_no_subject": contact_point_has_no_subject,
    "contact_point_file_is_missing": contact_point_file_is_missing,
    "policy_default_receiver_is_another": policy_default_receiver_is_another,
    "policy_has_a_route_to_another_receiver": policy_has_a_route_to_another_receiver,
    "grafana_environment_lacks_the_topic_arn": grafana_environment_lacks_the_topic_arn,
    "dropped_panel_reads_the_unmapped_counter": dropped_panel_reads_the_unmapped_counter,
    "pushgateway_scrape_drops_the_pushed_labels": pushgateway_scrape_drops_the_pushed_labels,
    "pushgateway_scrape_port_differs_from_terraform": pushgateway_scrape_port_differs_from_terraform,
    "host_state_exporter_scrape_is_missing": host_state_exporter_scrape_is_missing,
    "known_list_lacks_a_pushed_metric": known_list_lacks_a_pushed_metric,
    "known_list_gives_a_pushed_metric_another_label_set": known_list_gives_a_pushed_metric_another_label_set,
    "known_list_lacks_a_host_state_metric": known_list_lacks_a_host_state_metric,
    "job_rule_names_a_family_nobody_pushes": job_rule_names_a_family_nobody_pushes,
    "job_rule_uses_a_label_the_pushed_metric_lacks": job_rule_uses_a_label_the_pushed_metric_lacks,
    "spark_rule_reads_a_metric_nobody_pushes": spark_rule_reads_a_metric_nobody_pushes,
    "polaris_scrape_port_differs": polaris_scrape_port_differs,
    "polaris_scrape_path_is_wrong": polaris_scrape_path_is_wrong,
    "openmetadata_scrape_path_is_wrong": openmetadata_scrape_path_is_wrong,
    "mlflow_scrape_skips_the_relay": mlflow_scrape_skips_the_relay,
    "redis_exporter_keeps_every_db_series": redis_exporter_keeps_every_db_series,
    "node_exporter_scrape_port_differs": node_exporter_scrape_port_differs,
    "exporter_scrape_job_is_missing": exporter_scrape_job_is_missing,
    "known_list_lacks_redis_up": known_list_lacks_redis_up,
    "new_dashboard_without_a_row_is_reported": new_dashboard_without_a_row_is_reported,
    "dashboard_uid_is_used_twice": dashboard_uid_is_used_twice,
}

CASES |= PANEL_CASES
CASES |= PUSH_CASES
CASES |= EXPORTER_CASES
CASES |= SPARK_CASES
CASES |= PUBLISHER_CASES
CASES |= PUBLISH_CASES
CASES |= REDIS_CASES
CASES |= POSTGRES_CASES
CASES |= HOST_POLICY_CASES
CASES |= JOB_IDENTITY_CASES
