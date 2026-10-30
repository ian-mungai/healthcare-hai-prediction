"""Check the tracked monitoring files offline: they parse, they agree with each other and with the Terraform task, and they name only metrics that exist.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.monitoring_files``. The check reads these files and nothing else:

- ``monitoring/statsd/mapping.yml``: the Prometheus metrics Airflow's StatsD events become.
- ``monitoring/known_metrics.json``: the other metrics a dashboard or alert may name, and the contract the model API has to meet.
- ``monitoring/prometheus/prometheus.yml`` and ``alerts.yml``: the scrape jobs and the alert rules.
- ``monitoring/grafana/provisioning/**`` and ``monitoring/grafana/dashboards/*.json``: the data source and the dashboards.
- ``infra/services/monitoring.tf``: the ports and the folders the task mounts.

A metric is real when the mapping produces it, a known list names it or the API contract lists it. The check reads label matchers too, so a label typo fails.
It does not run PromQL and it does not prove that Airflow sends an event the mapping lists: that needs a running scheduler.
"""

from __future__ import annotations

import importlib
import json
import re
import sys
from pathlib import Path
from typing import Any

yaml = importlib.import_module("yaml")

ROOT = Path(__file__).resolve().parents[2]
MONITORING = "monitoring"
TERRAFORM = "infra/services/monitoring.tf"
KNOWN = f"{MONITORING}/known_metrics.json"
MAPPING = f"{MONITORING}/statsd/mapping.yml"
SCRAPE = f"{MONITORING}/prometheus/prometheus.yml"
ALERTS = f"{MONITORING}/prometheus/alerts.yml"
DATASOURCE = f"{MONITORING}/grafana/provisioning/datasources/prometheus.yml"
PROVIDER = f"{MONITORING}/grafana/provisioning/dashboards/dashboards.yml"
DASHBOARDS = f"{MONITORING}/grafana/dashboards"
REQUIRED_JOBS = ("prometheus", "statsd_exporter", "hai_api")
REQUIRED_ROWS = {"DAG": 3, "Jobs": 2, "API": 3}
SEVERITIES = ("critical", "warning")
METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
DURATION = re.compile(r"^[0-9]+[smh]$")
HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")
# The function, aggregation and operator words of PromQL that appear in the tracked expressions and could be read as metric names.
PROMQL_WORDS = frozenset(
    {
        "sum",
        "min",
        "max",
        "avg",
        "count",
        "group",
        "stddev",
        "stdvar",
        "topk",
        "bottomk",
        "quantile",
        "count_values",
        "rate",
        "irate",
        "increase",
        "delta",
        "idelta",
        "deriv",
        "predict_linear",
        "histogram_quantile",
        "histogram_fraction",
        "label_replace",
        "label_join",
        "vector",
        "scalar",
        "time",
        "timestamp",
        "absent",
        "absent_over_time",
        "abs",
        "ceil",
        "floor",
        "round",
        "clamp",
        "clamp_min",
        "clamp_max",
        "exp",
        "ln",
        "log2",
        "log10",
        "sqrt",
        "avg_over_time",
        "min_over_time",
        "max_over_time",
        "sum_over_time",
        "count_over_time",
        "quantile_over_time",
        "last_over_time",
        "present_over_time",
        "changes",
        "resets",
        "sort",
        "sort_desc",
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
        "and",
        "or",
        "unless",
        "bool",
        "offset",
        "inf",
        "nan",
    }
)
SELECTOR = re.compile(r"([a-zA-Z_:][a-zA-Z0-9_:]*)\s*\{([^}]*)\}")
MATCHER = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=~|!~|!=|=)\s*"(?:[^"\\]|\\.)*"')
GROUPING = re.compile(r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)")
IDENTIFIER = re.compile(r"(?<![0-9A-Za-z_:.])[a-zA-Z_:][a-zA-Z0-9_:]*(?![a-zA-Z0-9_:])")
HOST_PORT = re.compile(r"hostPort\s*=\s*([0-9]+)")
MOUNT_FOLDER = re.compile(r'"\$\{local\.code_path\}/(monitoring/[A-Za-z0-9_/]+)"')
CONTAINER_PATH = re.compile(r'sourceVolume\s*=\s*"(code_monitoring_[a-z_]+)",\s*containerPath\s*=\s*"([^"]+)"')
VOLUME_FOLDER = re.compile(r'(code_monitoring_[a-z_]+)\s*=\s*"\$\{local\.code_path\}/(monitoring/[A-Za-z0-9_/]+)"')


def read_text(root: Path, relative: str, problems: list[str]) -> str | None:
    """The text of a tracked file, or None with a problem when it is missing."""
    path = root / relative
    if not path.is_file():
        problems.append(f"{relative} does not exist")
        return None
    return path.read_text(encoding="utf-8")


def read_yaml(root: Path, relative: str, problems: list[str]) -> Any:
    """A YAML file parsed, or None with a problem when it is missing or invalid."""
    text = read_text(root, relative, problems)
    if text is None:
        return None
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        problems.append(f"{relative} is not valid YAML")
        return None


def read_json(root: Path, relative: str, problems: list[str]) -> Any:
    """A JSON file parsed, or None with a problem when it is missing or invalid."""
    text = read_text(root, relative, problems)
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        problems.append(f"{relative} is not valid JSON")
        return None


def mapping_metrics(document: Any, problems: list[str]) -> dict[str, set[str] | None]:
    """The metric names the StatsD mapping produces, each with its label names. Histogram names also yield their bucket, sum and count series."""
    metrics: dict[str, set[str] | None] = {}
    rules = document.get("mappings") if isinstance(document, dict) else None
    if not isinstance(rules, list) or not rules:
        problems.append(f"{MAPPING} needs a mappings list")
        return metrics
    last = rules[-1]
    if not (isinstance(last, dict) and last.get("action") == "drop" and last.get("match_type") == "regex" and last.get("match") == "."):
        problems.append(
            f'{MAPPING} must end with a rule that drops every other event (match ".", match_type regex, action drop), or the series count is unbounded'
        )
    for rule in rules[:-1]:
        if not isinstance(rule, dict) or not isinstance(rule.get("match"), str) or not isinstance(rule.get("name"), str):
            problems.append(f"{MAPPING} has a rule without a match and a name")
            continue
        name, kind = rule["name"], rule.get("match_metric_type", "timer")
        labels = rule.get("labels") or {}
        if not METRIC_NAME.match(name):
            problems.append(f"{MAPPING}: {name} is not a valid Prometheus metric name")
        if kind == "counter" and not name.endswith("_total"):
            problems.append(f"{MAPPING}: counter {name} must end in _total")
        if kind not in ("counter", "gauge", "timer"):
            problems.append(f"{MAPPING}: {name} has the unknown type {kind}")
        wildcards = rule["match"].count("*")
        for label, value in labels.items():
            for reference in re.findall(r"\$([0-9]+)", str(value)):
                if int(reference) < 1 or int(reference) > wildcards:
                    problems.append(f"{MAPPING}: {name} label {label} uses ${reference} but the match has {wildcards} wildcards")
        names = [name] if kind != "timer" else [f"{name}{suffix}" for suffix in HISTOGRAM_SUFFIXES]
        for produced in names:
            label_names = set(labels) | ({"le"} if produced.endswith("_bucket") else set())
            if produced in metrics and metrics[produced] != label_names:
                problems.append(f"{MAPPING}: {name} is mapped with different label sets, which Prometheus refuses")
            metrics[produced] = label_names
    return metrics


def known_metrics(document: Any, problems: list[str]) -> dict[str, set[str] | None]:
    """The metrics of the known list and the API contract. A value of None means any label is accepted (a metric of Prometheus or the exporter itself)."""
    metrics: dict[str, set[str] | None] = {}
    if not isinstance(document, dict):
        problems.append(f"{KNOWN} must be an object")
        return metrics
    for source in ("prometheus", "statsd_exporter"):
        for name in document.get(source, {}).get("metrics", []):
            metrics[name] = None
    for name, spec in document.get("hai_api", {}).get("metrics", {}).items():
        labels = set(spec.get("labels", []))
        if spec.get("type") == "histogram":
            for suffix in HISTOGRAM_SUFFIXES:
                metrics[f"{name}{suffix}"] = labels | ({"le"} if suffix == "_bucket" else set())
        else:
            metrics[name] = labels
    return metrics


def expression_problems(where: str, expression: str, metrics: dict[str, set[str] | None], jobs: set[str]) -> list[str]:
    """Problems with one PromQL expression: a metric that nothing produces, a label the metric lacks or a job nobody scrapes."""
    problems: list[str] = []
    stripped = expression
    for name, body in SELECTOR.findall(expression):
        for label, in_job in ((m.group(1), m.group(0)) for m in MATCHER.finditer(body)):
            if label == "job":
                value = re.search(r'"([^"]*)"', in_job)
                if value and value.group(1) not in jobs:
                    problems.append(f"{where}: the job {value.group(1)} is not a scrape job")
                continue
            allowed = metrics.get(name, set())
            if allowed is not None and label not in allowed and name in metrics:
                problems.append(f"{where}: {name} has no label {label}")
    stripped = SELECTOR.sub(lambda match: match.group(1), stripped)
    stripped = re.sub(r"\[[^\]]*\]", "", stripped)
    stripped = re.sub(r'"(?:[^"\\]|\\.)*"', "", stripped)
    stripped = GROUPING.sub("", stripped)
    for token in IDENTIFIER.findall(stripped):
        end = stripped.find(token)
        followed_by_call = stripped[end + len(token) :].lstrip().startswith("(")
        if token in PROMQL_WORDS or followed_by_call:
            continue
        if token not in metrics:
            problems.append(f"{where}: {token} is not a metric that the mapping, the known list or the API contract produces")
    return problems


def alert_problems(document: Any, metrics: dict[str, set[str] | None], jobs: set[str]) -> list[str]:
    """Problems with the alert rules file."""
    problems: list[str] = []
    seen: set[str] = set()
    groups = document.get("groups") if isinstance(document, dict) else None
    if not isinstance(groups, list) or not groups:
        return [f"{ALERTS} needs a groups list"]
    for group in groups:
        for rule in group.get("rules", []):
            name = str(rule.get("alert", ""))
            if not name:
                problems.append(f"{ALERTS} has a rule without an alert name")
                continue
            if name in seen:
                problems.append(f"{ALERTS}: {name} is defined twice")
            seen.add(name)
            if not isinstance(rule.get("expr"), str) or not rule["expr"].strip():
                problems.append(f"{ALERTS}: {name} has no expression")
            else:
                problems.extend(expression_problems(f"{ALERTS}: {name}", rule["expr"], metrics, jobs))
            if rule.get("labels", {}).get("severity") not in SEVERITIES:
                problems.append(f"{ALERTS}: {name} needs a severity label of {' or '.join(SEVERITIES)}")
            if not str(rule.get("annotations", {}).get("summary", "")).strip():
                problems.append(f"{ALERTS}: {name} has no summary")
            if "for" in rule and not DURATION.match(str(rule["for"])):
                problems.append(f"{ALERTS}: {name} has the duration {rule['for']}, which is not a whole number of seconds, minutes or hours")
    return problems


def dashboard_problems(relative: str, dashboard: Any, datasource_uid: str, metrics: dict[str, set[str] | None], jobs: set[str]) -> list[str]:
    """Problems with one dashboard file: panels, data source, the DAG, Jobs and API rows and every expression."""
    problems: list[str] = []
    if not isinstance(dashboard, dict) or not isinstance(dashboard.get("panels"), list) or not dashboard.get("uid") or not dashboard.get("title"):
        return [f"{relative} needs a uid, a title and a panels list"]
    row_title: str | None = None
    per_row: dict[str, int] = {}
    ids: set[int] = set()
    for panel in dashboard["panels"]:
        title = str(panel.get("title", ""))
        if panel.get("id") in ids:
            problems.append(f"{relative}: panel id {panel.get('id')} is used twice")
        ids.add(panel.get("id"))
        if panel.get("type") == "row":
            row_title = title
            per_row.setdefault(title, 0)
            continue
        if not title:
            problems.append(f"{relative} has a panel without a title")
        if row_title is None:
            problems.append(f"{relative}: panel {title} lies above the first row")
        else:
            per_row[row_title] += 1
        sources = [panel.get("datasource", {})] + [target.get("datasource", {}) for target in panel.get("targets", [])]
        for source in sources:
            if source.get("uid") != datasource_uid:
                problems.append(f"{relative}: panel {title} uses the data source {source.get('uid')}, which is not provisioned (only {datasource_uid} is)")
        if not panel.get("targets"):
            problems.append(f"{relative}: panel {title} has no query")
        for target in panel.get("targets", []):
            expression = target.get("expr")
            if not isinstance(expression, str) or not expression.strip():
                problems.append(f"{relative}: panel {title} has no expression")
            else:
                problems.extend(expression_problems(f"{relative}: panel {title}", expression, metrics, jobs))
    for row, minimum in REQUIRED_ROWS.items():
        if row not in per_row:
            problems.append(f"{relative} has no row {row}")
        elif per_row[row] < minimum:
            problems.append(f"{relative}: row {row} has {per_row[row]} panels, and the dashboard needs at least {minimum}")
    return problems


def terraform_problems(root: Path, scrape: Any, datasource: Any, provider: Any) -> list[str]:
    """Problems where the files disagree with the Terraform task: ports, mounted folders and the container paths the configuration names."""
    problems: list[str] = []
    text = read_text(root, TERRAFORM, problems)
    if text is None:
        return problems
    ports = {int(port) for port in HOST_PORT.findall(text)}
    for folder in sorted(set(MOUNT_FOLDER.findall(text))):
        if not (root / folder).is_dir():
            problems.append(f"{TERRAFORM} mounts {folder}, which does not exist")
    mounted = dict(CONTAINER_PATH.findall(text))
    folders = dict(VOLUME_FOLDER.findall(text))
    prometheus_path = mounted.get("code_monitoring_prometheus")
    for entry in (scrape or {}).get("rule_files", []):
        if prometheus_path is None or not str(entry).startswith(prometheus_path + "/"):
            problems.append(f"{SCRAPE}: the rule file {entry} is not inside the folder the task mounts for Prometheus")
        elif not (root / folders.get("code_monitoring_prometheus", "monitoring/prometheus") / str(entry)[len(prometheus_path) + 1 :]).is_file():
            problems.append(f"{SCRAPE}: the rule file {entry} does not exist in {folders.get('code_monitoring_prometheus')}")
    for job in (scrape or {}).get("scrape_configs", []):
        if job.get("job_name") in ("prometheus", "statsd_exporter"):
            for config in job.get("static_configs", []):
                for target in config.get("targets", []):
                    port = int(str(target).rsplit(":", maxsplit=1)[1])
                    if port not in ports:
                        problems.append(f"{SCRAPE}: job {job['job_name']} scrapes port {port}, which {TERRAFORM} does not publish")
    for source in (datasource or {}).get("datasources", []):
        found = re.search(r":([0-9]+)$", str(source.get("url", "")))
        if found is None or int(found.group(1)) not in ports:
            problems.append(f"{DATASOURCE}: the URL {source.get('url')} does not name a port that {TERRAFORM} publishes")
    dashboards_path = mounted.get("code_monitoring_grafana_dashboards")
    for entry in (provider or {}).get("providers", []):
        if entry.get("options", {}).get("path") != dashboards_path:
            problems.append(
                f"{PROVIDER}: the dashboard path {entry.get('options', {}).get('path')} is not the container path {dashboards_path} that {TERRAFORM} mounts"
            )
    return problems


def check(root: Path = ROOT) -> list[str]:
    """Every problem of the monitoring files under ``root``, as text lines; an empty list means they are valid."""
    problems: list[str] = []
    mapping = read_yaml(root, MAPPING, problems)
    known = read_json(root, KNOWN, problems)
    scrape = read_yaml(root, SCRAPE, problems)
    alerts = read_yaml(root, ALERTS, problems)
    datasource = read_yaml(root, DATASOURCE, problems)
    provider = read_yaml(root, PROVIDER, problems)
    metrics = {**known_metrics(known, problems), **(mapping_metrics(mapping, problems) if mapping is not None else {})}
    jobs = {str(job.get("job_name")) for job in (scrape or {}).get("scrape_configs", [])}
    for required in REQUIRED_JOBS:
        if scrape is not None and required not in jobs:
            problems.append(f"{SCRAPE} has no scrape job {required}")
    if alerts is not None:
        problems.extend(alert_problems(alerts, metrics, jobs))
    datasources = (datasource or {}).get("datasources", [])
    uids = [str(source.get("uid")) for source in datasources]
    if datasource is not None and len(uids) != 1:
        problems.append(f"{DATASOURCE} must provision exactly one data source")
    folder = root / DASHBOARDS
    files = sorted(folder.glob("*.json")) if folder.is_dir() else []
    if not files:
        problems.append(f"{DASHBOARDS} holds no dashboard")
    for path in files:
        relative = path.relative_to(root).as_posix()
        dashboard = read_json(root, relative, problems)
        if dashboard is not None and uids:
            problems.extend(dashboard_problems(relative, dashboard, uids[0], metrics, jobs))
    problems.extend(terraform_problems(root, scrape, datasource, provider))
    return problems


def main() -> int:
    """Print every problem and return 1, or print that the files are valid and return 0."""
    problems = check()
    sys.stdout.write("\n".join(problems or ["The monitoring files are valid."]) + "\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
