"""Check the tracked monitoring files offline: they parse, they agree with each other and with the Terraform task, and they name only metrics that exist.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.monitoring_files``. The check reads these files and nothing else:

- ``monitoring/statsd/mapping.yml``: the Prometheus metrics Airflow's StatsD events become.
- ``monitoring/known_metrics.json``: the other metrics a dashboard or alert may name, and the contract the model API has to meet.
- ``monitoring/prometheus/prometheus.yml``: the scrape jobs. ``alerts.yml`` must not exist: the alert rules are Grafana-managed.
- ``monitoring/grafana/provisioning/alerting/``: the alert rules (``rules_*.yml``), the one Amazon SNS contact point and the notification policy.
- ``monitoring/grafana/provisioning/**`` and ``monitoring/grafana/dashboards/*.json``: the data source and the dashboards.
- ``infra/services/monitoring.tf``: the ports and the folders the task mounts.
- ``infra/services/polaris.tf``: the setting that makes Polaris publish latency buckets.
- ``monitoring/promtool/``: the promtool rule file and fixtures of the service alert rules. This check ties them to the Grafana rules.
  Native ``promtool test rules`` runs them.

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

from scripts.infrastructure import promql_subset

yaml = importlib.import_module("yaml")

ROOT = Path(__file__).resolve().parents[2]
MONITORING = "monitoring"
TERRAFORM = "infra/services/monitoring.tf"
KNOWN = f"{MONITORING}/known_metrics.json"
MAPPING = f"{MONITORING}/statsd/mapping.yml"
SCRAPE = f"{MONITORING}/prometheus/prometheus.yml"
ALERTS = f"{MONITORING}/prometheus/alerts.yml"
ALERTING = f"{MONITORING}/grafana/provisioning/alerting"
CONTACT = f"{ALERTING}/contact_points.yml"
POLICIES = f"{ALERTING}/policies.yml"
TEMPLATES = f"{ALERTING}/templates.yml"
TEMPLATE_GROUP = "hai_sns"
TEMPLATE_PARTS = (
    "hai.sns.subject",
    "hai.sns.message",
    "hai.model_name",
    "hai.hai_name",
    "hai.model_short",
    "hai.hai_short",
    "hai.alert",
    "hai.time",
    "hai.which",
    "hai.subject_target",
    "hai.line",
    "hai.sentences",
    "hai.safe13",
    "hai.safe17",
    "hai.safe22",
    "hai.safe24",
    "hai.safe32",
    "hai.mid10",
    "hai.mid14",
    "hai.mid20",
    "hai.dag",
)
NOTICE_ANNOTATIONS = ("headline", "recovered", "resolved_affects", "resolved_next", "affects", "next_step", "dashboard")
HEADLINE_LIMIT = 32
CONTACT_NAME = "hai_sns"
TOPIC_REFERENCE = "${ALERTS_TOPIC_ARN}"
STATIC_CREDENTIALS = ("access_key", "secret_key", "profile", "role_arn")
STATES = ("OK", "Alerting", "NoData", "Error")
DATASOURCE = f"{MONITORING}/grafana/provisioning/datasources/prometheus.yml"
PROVIDER = f"{MONITORING}/grafana/provisioning/dashboards/dashboards.yml"
DASHBOARDS = f"{MONITORING}/grafana/dashboards"
REQUIRED_JOBS = (
    "prometheus",
    "statsd_exporter",
    "hai_api",
    "pushgateway",
    "host_state_exporter",
    "polaris",
    "openmetadata",
    "grafana",
    "mlflow",
    "redis_exporter",
    "postgres_exporter",
    "node_exporter",
)
EXPECTED_TARGETS = {
    "prometheus": ["127.0.0.1:9090"],
    "statsd_exporter": ["127.0.0.1:9102"],
    "hai_api": ["127.0.0.1:8000"],
    "pushgateway": ["127.0.0.1:9091"],
    "host_state_exporter": ["127.0.0.1:9103"],
    "polaris": ["127.0.0.1:8182"],
    "openmetadata": ["127.0.0.1:8586"],
    "grafana": ["127.0.0.1:3001"],
    "mlflow": ["127.0.0.1:9103"],
    "redis_exporter": ["127.0.0.1:9121"],
    "postgres_exporter": ["127.0.0.1:9187"],
    "node_exporter": ["127.0.0.1:9100"],
}
EXPECTED_PATHS = {"polaris": "/q/metrics", "openmetadata": "/prometheus", "mlflow": "/mlflow_metrics"}
SERVICE_RULES = f"{ALERTING}/rules_service_metrics.yml"
PROMTOOL_RULES = f"{MONITORING}/promtool/service_alerts.yml"
PROMTOOL_TESTS = f"{MONITORING}/promtool/service_alerts_test.yml"
PROMTOOL_SCENARIOS = (
    "first_failure",
    "later_failure",
    "restart_after_a_gap",
    "short_restart_same_labels",
    "restart_without_failure",
    "offset_hits_stale_marker",
    "second_integration_first_failure",
    "all_undefined_heap_maxima",
    "three_observed_5xx",
    "job_results_stale_host_on",
    "job_results_missing_host_on",
    "job_results_stale_host_off",
    "job_results_missing_host_off",
)
POLARIS_TERRAFORM = "infra/services/polaris.tf"
POLARIS_BUCKET = "http_server_requests_seconds_bucket"
POLARIS_SETTING = "POLARIS_METRICS_HTTP_SERVER_REQUESTS_HISTOGRAM_SLOS"
POLARIS_SETTING_VALUE = re.compile(rf'name\s*=\s*"{POLARIS_SETTING}",\s*value\s*=\s*"([^"]*)"')
DURATION_LIST = re.compile(r"[0-9]+(?:ms|s)(?:,[0-9]+(?:ms|s))*")
PORT_FILES = ("infra/services/monitoring.tf", "infra/services/polaris.tf", "infra/services/openmetadata.tf")
TERRAFORM_JOBS = (
    "prometheus",
    "statsd_exporter",
    "pushgateway",
    "host_state_exporter",
    "polaris",
    "openmetadata",
    "grafana",
    "redis_exporter",
    "postgres_exporter",
    "node_exporter",
)
OVERVIEW = "hai_overview"
PUSH_GROUPING = ("job", "phase", "outcome", "model_set", "job_type")
SCHEDULER_ALERT = "AirflowSchedulerSilent"
SCHEDULER_ABSENT = "absent(airflow_scheduler_heartbeat_total)"
REQUIRED_ROWS = {"DAG": 3, "Jobs": 2, "API": 3}  # the rows of the overview dashboard only
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
BY_CLAUSE = re.compile(r"\bby\s*\(([^)]*)\)")
QUOTED = re.compile(r'"(?:[^"\\]|\\.)*"')
LIMITER = re.compile(r"\b(?:topk|bottomk)\s*\(")
# A JVM pool without a maximum reports -1, so a query may read the maximum only after it filters to the pools that have one.
UNGUARDED_MAXIMUM = re.compile(r"jvm_memory_max_bytes\s*\{[^}]*\}(?!\s*>\s*0(?![0-9.]))")
# Labels whose value count the metric's source does not bound by itself: a query may group by one only inside topk or bottomk, which limits the series drawn.
OPEN_LABELS = frozenset({"uri", "path", "url", "rule_group", "folder_uid", "realm_id", "principal", "pid"})
GROUPING = re.compile(r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)")
IDENTIFIER = re.compile(r"(?<![0-9A-Za-z_:.])[a-zA-Z_:][a-zA-Z0-9_:]*(?![a-zA-Z0-9_:])")
HOST_PORT = re.compile(r"hostPort\s*=\s*([0-9]+)")
PORT_ASSIGN = re.compile(r"\b[a-z_]*port\s*=\s*([0-9]{2,5})\b")
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
    """The metrics of the known list: a source that lists names has any label set (None), one that lists specs has typed metrics with labels."""
    metrics: dict[str, set[str] | None] = {}
    if not isinstance(document, dict):
        problems.append(f"{KNOWN} must be an object")
        return metrics
    for entry in document.values():
        listed = entry.get("metrics") if isinstance(entry, dict) else None
        if isinstance(listed, list):
            for name in listed:
                metrics[name] = None
        elif isinstance(listed, dict):
            for name, spec in listed.items():
                labels = set(spec.get("labels", []))
                if spec.get("type") == "histogram":
                    for suffix in HISTOGRAM_SUFFIXES:
                        metrics[f"{name}{suffix}"] = labels | ({"le"} if suffix == "_bucket" else set())
                else:
                    metrics[name] = labels
    return metrics


def late_counters(document: Any) -> frozenset[str]:
    """The known counters marked ``starts_at_first_event``: their source creates the series at the first event, so no sample shows the step from zero to one."""
    found: set[str] = set()
    for entry in document.values() if isinstance(document, dict) else []:
        listed = entry.get("metrics") if isinstance(entry, dict) else None
        for name, spec in listed.items() if isinstance(listed, dict) else []:
            if isinstance(spec, dict) and spec.get("starts_at_first_event") is True:
                found.add(name)
    return frozenset(found)


def catalog_problems(document: Any) -> list[str]:
    """Problems where the known list and the code that emits the metrics disagree: the push catalog of the jobs and the catalog of the host state exporter."""
    from monitoring import host_state_exporter
    from scripts import metrics_push

    problems: list[str] = []
    if not isinstance(document, dict):
        return problems
    wanted = {
        "pushgateway": {
            name: set(PUSH_GROUPING) | set(metrics_push.GROUP_LABELS.get(name, ())) | set(labels) for name, (_, labels) in metrics_push.CATALOG.items()
        },
        "host_state_exporter": {name: set(labels) for name, (_, labels, _) in host_state_exporter.CATALOG.items()},
    }
    sources = {"pushgateway": "scripts/metrics_push.py", "host_state_exporter": "monitoring/host_state_exporter.py"}
    for source, expected in wanted.items():
        listed = document.get(source, {}).get("metrics") if isinstance(document.get(source), dict) else None
        found = {name: set(spec.get("labels", [])) for name, spec in listed.items()} if isinstance(listed, dict) else {}
        for name in sorted(set(expected) - set(found)):
            problems.append(f"{KNOWN}: {source} lacks {name}, which {sources[source]} emits")
        for name in sorted(set(found) - set(expected)):
            problems.append(f"{KNOWN}: {source} lists {name}, which {sources[source]} does not emit")
        for name in sorted(set(expected) & set(found)):
            if expected[name] != found[name]:
                problems.append(f"{KNOWN}: {source} gives {name} the labels {sorted(found[name])}, but {sources[source]} emits {sorted(expected[name])}")
    return problems


def limiter_spans(text: str) -> list[tuple[int, int]]:
    """The character ranges of every topk and bottomk call in an expression whose quoted strings are blanked."""
    spans: list[tuple[int, int]] = []
    for call in LIMITER.finditer(text):
        depth = 0
        for position in range(call.end() - 1, len(text)):
            depth += {"(": 1, ")": -1}.get(text[position], 0)
            if depth == 0:
                spans.append((call.start(), position))
                break
    return spans


def late_counter_problems(where: str, expression: str, late: frozenset[str]) -> list[str]:
    """Problems with a rule that takes increase or rate of a counter that has no series before its first event: such a rule misses the first event.

    Two things hide it. The series is new, so a sample before it does not exist (compare it with the same series at an offset). A restart can also return
    the same labels and the same value, so the series is not new (compare it with the process start time).
    """
    problems: list[str] = []
    for name in sorted(late):
        if re.search(rf"\b(?:increase|rate|irate)\(\s*{name}\b", expression) is None:
            continue
        if re.search(rf"\b{name}\s*(?:\{{[^}}]*\}})?\s+offset\b", expression) is None:
            problems.append(f"{where}: {name} has no series before its first event, so increase and rate miss that event; also compare it with {name} offset")
        if "process_start_time_seconds" not in expression:
            problems.append(f"{where}: {name} keeps its labels and value across a restart, so also compare it with process_start_time_seconds")
    return problems


def expression_problems(where: str, expression: str, metrics: dict[str, set[str] | None], jobs: set[str]) -> list[str]:
    """Problems with one PromQL expression: a metric that nothing produces, a label it lacks, a job nobody scrapes or an open label outside topk."""
    problems: list[str] = []
    stripped = expression
    try:
        promql_subset.validate(expression)
    except promql_subset.Unsupported as error:
        problems.append(f"{where}: not a valid PromQL query ({error}): {expression}")
    masked = QUOTED.sub(lambda found: '"' + " " * (len(found.group(0)) - 2) + '"', expression)
    spans = limiter_spans(masked)
    for clause in BY_CLAUSE.finditer(masked):
        limited = any(start < clause.start() < end for start, end in spans)
        for label in (part.strip() for part in clause.group(1).split(",")):
            if label in OPEN_LABELS and not limited:
                problems.append(f"{where}: groups by {label} without topk, so the number of series has no bound")
    for unguarded in UNGUARDED_MAXIMUM.finditer(masked):
        problems.append(f"{where}: jvm_memory_max_bytes is -1 when a pool has no maximum, so read it as {expression[unguarded.start() : unguarded.end()]} > 0")
    for name, body in SELECTOR.findall(expression):
        for label, in_job in ((m.group(1), m.group(0)) for m in MATCHER.finditer(body)):
            if label == "job":
                value = re.search(r'"([^"]*)"', in_job)
                if value and value.group(1) not in jobs:
                    problems.append(f"{where}: the job {value.group(1)} is not a scrape job or a pushed job family")
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


def step_problems(
    where: str, rule: dict[str, Any], datasource_uid: str, metrics: dict[str, set[str] | None], jobs: set[str], late: frozenset[str] = frozenset()
) -> list[str]:
    """Problems with the query and expression steps of one Grafana-managed rule: the condition, the data source and every PromQL expression."""
    problems: list[str] = []
    steps = [step for step in rule.get("data", []) if isinstance(step, dict)]
    refs = {str(step.get("refId")) for step in steps}
    if str(rule.get("condition")) not in refs:
        problems.append(f"{where}: condition {rule.get('condition')} is not a step of the rule")
    queries = 0
    for step in steps:
        raw_model = step.get("model")
        model: dict[str, Any] = raw_model if isinstance(raw_model, dict) else {}
        if step.get("datasourceUid") == "__expr__":
            if str(model.get("expression", "")) not in refs:
                problems.append(f"{where}: step {step.get('refId')} reads {model.get('expression')}, which is not a step of the rule")
            continue
        queries += 1
        if step.get("datasourceUid") != datasource_uid:
            problems.append(
                f"{where}: step {step.get('refId')} uses the data source {step.get('datasourceUid')}, which is not provisioned (only {datasource_uid} is)"
            )
        expression = model.get("expr")
        if not isinstance(expression, str) or not expression.strip():
            problems.append(f"{where}: step {step.get('refId')} has no expression")
        else:
            problems.extend(expression_problems(where, expression, metrics, jobs))
            problems.extend(late_counter_problems(where, expression, late))
    if queries == 0:
        problems.append(f"{where}: has no query")
    return problems


def alert_problems(
    root: Path, metrics: dict[str, set[str] | None], jobs: set[str], datasource_uid: str, receivers: set[str], late: frozenset[str] = frozenset()
) -> list[str]:
    """Problems with the Grafana-managed alert rule files: identity, steps, severity, summary, duration, pause state and routing."""
    problems: list[str] = []
    titles: set[str] = set()
    uids: set[str] = set()
    total = 0
    folder = root / ALERTING
    for path in sorted(folder.glob("rules_*.yml")) if folder.is_dir() else []:
        relative = path.relative_to(root).as_posix()
        document = read_yaml(root, relative, problems)
        groups = document.get("groups") if isinstance(document, dict) else None
        if not isinstance(groups, list) or not groups:
            problems.append(f"{relative} needs a groups list")
            continue
        for group in groups:
            if not DURATION.match(str(group.get("interval", ""))):
                problems.append(f"{relative}: group {group.get('name')} needs an interval of whole seconds, minutes or hours")
            for rule in group.get("rules", []):
                total += 1
                name = str(rule.get("title", ""))
                where = f"{relative}: {name}"
                if not name:
                    problems.append(f"{relative} has a rule without a title")
                    continue
                if name in titles:
                    problems.append(f"{relative}: {name} is defined twice")
                titles.add(name)
                uid = str(rule.get("uid", ""))
                if not uid:
                    problems.append(f"{where} has no uid")
                elif uid in uids:
                    problems.append(f"{where}: uid {uid} is used twice")
                uids.add(uid)
                problems.extend(step_problems(where, rule, datasource_uid, metrics, jobs, late))
                if rule.get("labels", {}).get("severity") not in SEVERITIES:
                    problems.append(f"{where} needs a severity label of {' or '.join(SEVERITIES)}")
                if not str(rule.get("annotations", {}).get("summary", "")).strip():
                    problems.append(f"{where} has no summary")
                problems.extend(notice_annotation_problems(where, rule.get("annotations", {}), dashboard_titles(root)))
                if "for" in rule and not DURATION.match(str(rule["for"])):
                    problems.append(f"{where} has the duration {rule['for']}, which is not a whole number of seconds, minutes or hours")
                for state in ("noDataState", "execErrState"):
                    if rule.get(state) not in STATES:
                        problems.append(f"{where}: {state} must be one of {', '.join(STATES)}")
                if rule.get("isPaused"):
                    problems.append(f"{where} is paused, so it never alerts")
                routed = (rule.get("notification_settings") or {}).get("receiver")
                if routed is not None and routed not in receivers:
                    problems.append(f"{where} routes to {routed}, which is not the SNS contact point")
                if name == SCHEDULER_ALERT and SCHEDULER_ABSENT not in json.dumps(rule):
                    problems.append(f"{where}: {SCHEDULER_ALERT} must also fire when no heartbeat series exists")
    if total == 0:
        problems.append(f"{ALERTING} holds no alert rule")
    return problems


def dashboard_titles(root: Path) -> set[str]:
    """The titles of the tracked dashboards, which the dashboard annotation of a rule names."""
    folder = root / DASHBOARDS
    titles: set[str] = set()
    for path in sorted(folder.glob("*.json")) if folder.is_dir() else []:
        try:
            titles.add(str(json.loads(path.read_text(encoding="utf-8")).get("title")))
        except (OSError, ValueError, AttributeError):
            continue
    return titles


def notice_annotation_problems(where: str, annotations: Any, titles: set[str]) -> list[str]:
    """Problems with the annotations that the alert email reads: each is text, the headline fits a subject and the dashboard exists."""
    problems: list[str] = []
    annotations = annotations if isinstance(annotations, dict) else {}
    for key in NOTICE_ANNOTATIONS:
        if not str(annotations.get(key, "")).strip():
            problems.append(f"{where} has no {key} annotation, which the alert email reads")
    if len(str(annotations.get("headline", ""))) > HEADLINE_LIMIT:
        problems.append(f"{where}: the headline is longer than {HEADLINE_LIMIT} characters, and an SNS subject holds 100")
    dashboard = str(annotations.get("dashboard", "")).strip()
    if dashboard and titles and dashboard not in titles:
        problems.append(f"{where}: the dashboard annotation names {dashboard!r}, which is no dashboard title")
    for key, value in annotations.items():
        if "$" in str(value) or "{{" in str(value):
            problems.append(f"{where}: the {key} annotation holds a template or a variable, and the alert email shows it as written")
    return problems


def template_problems(root: Path, contact: Any) -> list[str]:
    """Problems with the notification template group: the file parses, it defines every part, the contact point calls the subject and the message, every
    annotation it reads is one the rules must hold, and its names of models and infection types are the names of the contract."""
    problems: list[str] = []
    document = read_yaml(root, TEMPLATES, problems)
    if document is None:
        return problems
    entries = document.get("templates") if isinstance(document, dict) else None
    if not isinstance(entries, list) or len(entries) != 1 or entries[0].get("name") != TEMPLATE_GROUP or not str(entries[0].get("template", "")).strip():
        return [*problems, f"{TEMPLATES} must provision exactly one template group named {TEMPLATE_GROUP}"]
    text = str(entries[0]["template"])
    if text.count("{{") != text.count("}}"):
        problems.append(f"{TEMPLATES}: the template has an unclosed action")
    defined = set(re.findall(r'\{\{\s*define\s+"([^"]+)"', text))
    for part in TEMPLATE_PARTS:
        if part not in defined:
            problems.append(f"{TEMPLATES} does not define {part}")
    for call in set(re.findall(r'\{\{-?\s*template\s+"([^"]+)"', text)) - defined:
        problems.append(f"{TEMPLATES} calls {call}, which it does not define")
    read = set(re.findall(r"\.(?:Common)?Annotations\.([a-z_]+)", text))
    for key in sorted(read - {"summary", *NOTICE_ANNOTATIONS}):
        problems.append(f"{TEMPLATES} reads the annotation {key}, which no rule is required to hold")
    for key in sorted(set(NOTICE_ANNOTATIONS) - read):
        problems.append(f"{TEMPLATES} does not read the annotation {key}, which every rule must hold")
    settings = {}
    points = contact.get("contactPoints") if isinstance(contact, dict) else None
    if isinstance(points, list) and points and points[0].get("receivers"):
        settings = points[0]["receivers"][0].get("settings", {})
    for field, part in (("subject", "hai.sns.subject"), ("message", "hai.sns.message")):
        if f'template "{part}"' not in str(settings.get(field, "")):
            problems.append(f"{CONTACT}: the {field} must call the template {part} of {TEMPLATES}")
    from orchestration import training_contract

    for part, labels in (("hai.model_name", training_contract.MODEL_LABELS), ("hai.hai_name", training_contract.HAI_LABELS)):
        found = dict(re.findall(r'\{\{ (?:else )?if eq \. "([^"]+)" \}\}(.+?) \(\1\)', text.split(f'define "{part}"', 1)[-1].split("{{- end }}", 1)[0]))
        if found != dict(labels):
            problems.append(f"{TEMPLATES}: {part} names {found}, but the contract names {dict(labels)}")
    for part, labels in (("hai.model_short", training_contract.MODEL_LABELS), ("hai.hai_short", training_contract.HAI_LABELS)):
        body = text.split(f'define "{part}"', 1)[-1].split("{{- end }}", 1)[0]
        found = dict(re.findall(r'\{\{ (?:else )?if eq \. "([^"]+)" \}\}(.+?) \(\1\)', body))
        if set(found) != set(labels) or any(not labels[key].startswith(name) for key, name in found.items()):
            problems.append(f"{TEMPLATES}: {part} must give each key a short name that starts the contract name, but it names {found}")
    return problems


def contact_point_problems(contact: Any, policies: Any) -> tuple[list[str], set[str]]:
    """Problems with the contact point and the policy tree, and the receiver names that are valid."""
    problems: list[str] = []
    points = contact.get("contactPoints") if isinstance(contact, dict) else None
    receivers: set[str] = set()
    if not isinstance(points, list) or len(points) != 1 or len(points[0].get("receivers", [])) != 1:
        problems.append(f"{CONTACT} must provision exactly one contact point with one integration")
    else:
        name, integration = points[0].get("name"), points[0]["receivers"][0]
        receivers.add(str(name))
        settings = integration.get("settings") if isinstance(integration.get("settings"), dict) else {}
        if name != CONTACT_NAME:
            problems.append(f"{CONTACT}: the contact point must be named {CONTACT_NAME}")
        if integration.get("type") != "sns":
            problems.append(f"{CONTACT}: the integration must be of type sns")
        if settings.get("topic_arn") != TOPIC_REFERENCE:
            problems.append(f"{CONTACT}: topic_arn must be {TOPIC_REFERENCE}, so the ARN comes from Terraform and is never typed here")
        if not str(settings.get("subject", "")).strip():
            problems.append(f"{CONTACT} needs a subject, because SNS refuses an empty one")
        raw_sigv4 = settings.get("sigv4")
        sigv4: dict[str, Any] = raw_sigv4 if isinstance(raw_sigv4, dict) else {}
        for field in STATIC_CREDENTIALS:
            if field in sigv4:
                problems.append(f"{CONTACT}: sigv4 sets {field}, but the contact point must use the task role through the default credential chain")
    tree = policies.get("policies") if isinstance(policies, dict) else None
    if not isinstance(tree, list) or len(tree) != 1:
        problems.append(f"{POLICIES} must provision exactly one policy tree")
        return problems, receivers

    def walk(node: dict[str, Any], default: bool) -> None:
        if node.get("receiver") not in receivers:
            problems.append(
                f"{POLICIES}: the {'default receiver' if default else 'route'} routes to {node.get('receiver')}, which is not the SNS contact point"
            )
        for child in node.get("routes", []):
            walk(child, False)

    walk(tree[0], True)
    return problems, receivers


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
    for row, minimum in REQUIRED_ROWS.items() if dashboard.get("uid") == OVERVIEW else []:
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
    for extra in PORT_FILES[1:]:
        more = read_text(root, extra, problems) or ""
        ports |= {int(port) for port in HOST_PORT.findall(more)} | {int(port) for port in PORT_ASSIGN.findall(more)}
    for variable in ("ALERTS_TOPIC_ARN", "AWS_REGION"):
        if f'name = "{variable}"' not in text:
            problems.append(f"{TERRAFORM}: the Grafana container must set {variable}, which {CONTACT} reads")
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
        if job.get("job_name") in TERRAFORM_JOBS:
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


def polaris_histogram_problems(root: Path, metrics: dict[str, set[str] | None]) -> list[str]:
    """Problems where the Polaris task and the known list disagree about the latency bucket, which Polaris publishes only when the task sets the SLO list."""
    problems: list[str] = []
    text = read_text(root, POLARIS_TERRAFORM, problems)
    if text is None:
        return problems
    found = POLARIS_SETTING_VALUE.search(text)
    listed = POLARIS_BUCKET in metrics
    if found is None and listed:
        problems.append(f"{KNOWN}: lists {POLARIS_BUCKET}, but {POLARIS_TERRAFORM} does not set {POLARIS_SETTING}, so Polaris publishes no such series")
    if found is not None and not listed:
        problems.append(f"{KNOWN}: polaris lacks the bucket {POLARIS_BUCKET}, which the {POLARIS_SETTING} setting in {POLARIS_TERRAFORM} makes Polaris publish")
    if found is not None and not DURATION_LIST.fullmatch(found.group(1)):
        problems.append(f"{POLARIS_TERRAFORM}: {POLARIS_SETTING} is not a list of durations in ms or s that Quarkus reads, for example 10ms,50ms,1s")
    return problems


def promtool_problems(root: Path) -> list[str]:
    """Problems where the promtool fixtures and the Grafana service rules disagree. Native promtool decides what a query does."""
    problems: list[str] = []
    grafana = read_yaml(root, SERVICE_RULES, problems)
    fixtures = read_yaml(root, PROMTOOL_RULES, problems)
    tests = read_yaml(root, PROMTOOL_TESTS, problems)
    if grafana is None or fixtures is None or tests is None:
        return problems
    wanted: dict[str, tuple[str, str]] = {}
    for group in grafana.get("groups", []):
        for rule in group.get("rules", []):
            expression = next(
                (str(step["model"]["expr"]) for step in rule.get("data", []) if isinstance(step.get("model"), dict) and step["model"].get("expr")), ""
            )
            wanted[str(rule.get("title"))] = (expression, str(rule.get("labels", {}).get("severity")))
    found: dict[str, tuple[str, str]] = {}
    for group in fixtures.get("groups", []):
        for rule in group.get("rules", []):
            found[str(rule.get("alert"))] = (str(rule.get("expr")), str(rule.get("labels", {}).get("severity")))
    for title, (expression, severity) in wanted.items():
        if title not in found:
            problems.append(f"{PROMTOOL_RULES}: {title} has no promtool rule")
        elif found[title] != (f"({expression}) > 0", severity):
            problems.append(f"{PROMTOOL_RULES}: the query or severity of {title} differs from the Grafana rule in {SERVICE_RULES}")
    for title in sorted(set(found) - set(wanted)):
        problems.append(f"{PROMTOOL_RULES}: {title} has no Grafana rule in {SERVICE_RULES}")
    if tests.get("rule_files") != ["service_alerts.yml"]:
        problems.append(f"{PROMTOOL_TESTS}: rule_files must name service_alerts.yml only")
    names = {str(test.get("name")) for test in tests.get("tests", [])}
    for scenario in PROMTOOL_SCENARIOS:
        if scenario not in names:
            problems.append(f"{PROMTOOL_TESTS} lacks the scenario {scenario}")
    fires: dict[str, bool] = {title: False for title in wanted}
    silent: dict[str, bool] = {title: False for title in wanted}
    for test in tests.get("tests", []):
        for evaluation in test.get("alert_rule_test", []):
            title = str(evaluation.get("alertname"))
            if title in wanted:
                (fires if evaluation.get("exp_alerts") else silent)[title] = True
    for title in wanted:
        if not fires[title]:
            problems.append(f"{PROMTOOL_TESTS}: {title} has no promtool evaluation that fires")
        if not silent[title]:
            problems.append(f"{PROMTOOL_TESTS}: {title} has no promtool evaluation that stays silent")
    return problems


def check(root: Path = ROOT) -> list[str]:
    """Every problem of the monitoring files under ``root``, as text lines; an empty list means they are valid."""
    problems: list[str] = []
    mapping = read_yaml(root, MAPPING, problems)
    known = read_json(root, KNOWN, problems)
    scrape = read_yaml(root, SCRAPE, problems)
    datasource = read_yaml(root, DATASOURCE, problems)
    provider = read_yaml(root, PROVIDER, problems)
    metrics = {**known_metrics(known, problems), **(mapping_metrics(mapping, problems) if mapping is not None else {})}
    problems.extend(catalog_problems(known))
    problems.extend(polaris_histogram_problems(root, metrics))
    problems.extend(promtool_problems(root))
    from scripts import metrics_push

    jobs = {str(job.get("job_name")) for job in (scrape or {}).get("scrape_configs", [])} | set(metrics_push.JOB_PHASES)
    scraped = {str(job.get("job_name")) for job in (scrape or {}).get("scrape_configs", [])}
    for required in REQUIRED_JOBS:
        if scrape is not None and required not in scraped:
            problems.append(f"{SCRAPE} has no scrape job {required}")
    for job in (scrape or {}).get("scrape_configs", []):
        expected = EXPECTED_TARGETS.get(str(job.get("job_name")))
        targets = [str(target) for config in job.get("static_configs", []) for target in config.get("targets", [])]
        if expected is not None and targets != expected:
            problems.append(f"{SCRAPE}: job {job['job_name']} must scrape {', '.join(expected)}, not {targets}")
        wanted_path = EXPECTED_PATHS.get(str(job.get("job_name")))
        if wanted_path is not None and job.get("metrics_path") != wanted_path:
            problems.append(f"{SCRAPE}: job {job['job_name']} must use the path {wanted_path}, not {job.get('metrics_path', '/metrics')}")
        if job.get("job_name") == "redis_exporter" and not any(
            item.get("action") == "drop" and "redis_db_keys" in str(item.get("regex")) for item in job.get("metric_relabel_configs", [])
        ):
            problems.append(f"{SCRAPE}: the job redis_exporter must drop redis_db_keys of the databases that hold no data (0 and 3 to 15)")
        if job.get("job_name") == "pushgateway" and job.get("honor_labels") is not True:
            problems.append(f"{SCRAPE}: the job pushgateway must set honor_labels: true, or the pushed job, phase and outcome labels are renamed")
    datasources = (datasource or {}).get("datasources", [])
    uids = [str(source.get("uid")) for source in datasources]
    if datasource is not None and len(uids) != 1:
        problems.append(f"{DATASOURCE} must provision exactly one data source")
    if (root / ALERTS).exists():
        problems.append(f"{ALERTS} must not exist: the alert rules are Grafana-managed (monitoring/grafana/provisioning/alerting)")
    if scrape is not None and (scrape.get("rule_files") or scrape.get("alerting")):
        problems.append(f"{SCRAPE}: rule_files and alerting must not be set, because Grafana evaluates the alert rules")
    contact = read_yaml(root, CONTACT, problems)
    policies = read_yaml(root, POLICIES, problems)
    contact_problems, receivers = contact_point_problems(contact, policies) if contact is not None and policies is not None else ([], set())
    problems.extend(contact_problems)
    problems.extend(template_problems(root, contact) if contact is not None else [])
    if uids:
        problems.extend(alert_problems(root, metrics, jobs, uids[0], receivers, late_counters(known)))
    folder = root / DASHBOARDS
    files = sorted(folder.glob("*.json")) if folder.is_dir() else []
    if not files:
        problems.append(f"{DASHBOARDS} holds no dashboard")
    seen_uids: dict[str, str] = {}
    for path in files:
        relative = path.relative_to(root).as_posix()
        dashboard = read_json(root, relative, problems)
        if dashboard is not None and uids:
            problems.extend(dashboard_problems(relative, dashboard, uids[0], metrics, jobs))
        uid = str(dashboard.get("uid")) if isinstance(dashboard, dict) else ""
        if uid and uid in seen_uids:
            problems.append(f"{relative}: uid {uid} is used by two dashboards ({seen_uids[uid]} and {relative})")
        seen_uids.setdefault(uid, relative)
    problems.extend(terraform_problems(root, scrape, datasource, provider))
    return problems


def main() -> int:
    """Print every problem and return 1, or print that the files are valid and return 0."""
    problems = check()
    sys.stdout.write("\n".join(problems or ["The monitoring files are valid."]) + "\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
