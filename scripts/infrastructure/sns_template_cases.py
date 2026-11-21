"""Cases for the alert email template: each case renders the committed template for sample alert notifications and judges the email text.

The template is the group in ``monitoring/grafana/provisioning/alerting/templates.yml``.
``monitoring/grafana/render_notice.go`` runs it with the Go text/template engine that Grafana uses and with stand-ins for Grafana's template functions.
So these cases prove that the template, the annotations and the names agree. They do not prove that Grafana 13.2.5 accepts the file or runs the
functions the same way: only a real notification does that. No Docker, AWS or network is used. A case returns named checks, like ``monitoring_cases``.
"""

from __future__ import annotations

import importlib
import json
import os
import re
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.process import find_program, run_command

yaml = importlib.import_module("yaml")

ALERTING = "monitoring/grafana/provisioning/alerting"
TEMPLATES = f"{ALERTING}/templates.yml"
RENDERER = "monitoring/grafana/render_notice.go"
STARTS = "2026-11-19T18:13:00Z"
ENDS = "2026-11-19T21:08:00Z"
NEVER = "0001-01-01T00:00:00Z"
BLOCKS = ("What happened:", "When:", "What it affects:", "What to do next:", "Where to look:", "Details:")
BARE_KEY = re.compile(r"(?<![\w(])(m[1-6]|HAI_[1-6])(?![\w])")
SAMPLE_VALUES = {
    "job": "silver",
    "phase": "build",
    "model_set": "m2",
    "job_type": "HAI_1",
    "dag_id": "silver_transform_dag",
    "task_id": "job",
    "feature_set": "m2",
    "hai_type": "HAI_1",
    "service": "airflow",
    "kind": "row_counts",
    "source": "disk",
    "task": "catalog_setup",
}
SAMPLE_OVERRIDES = {"drift_triggered": {"job": "drift", "phase": "check"}, "spark_tasks_failed": {"job": "bronze", "phase": "load"}}
LABELS_NOT_SHOWN = {"le", "outcome"}
# A job that runs once carries no model set and no HAI type, so its alerts carry no such label (the email of the silver job shows job and phase only).
INSTANCE_LABELS = {"model_set", "job_type"}
WITH_INSTANCE_LABELS = {"drift_triggered"}
EXTRA_LABELS = {
    "service_tasks_missing": ["service"],
    "service_health_down": ["service"],
    "host_state_source_down": ["source"],
    "bootstrap_task_failed": ["task"],
}
LINE_LIMIT_BEFORE_NEXT_STEP = 16
SUBJECT_LIMIT = 90
ALERT_BLOCK_LIMIT = 10
MESSAGE_LIMIT = 60_000
WORD_LIMIT = 25
FIRING_FIELDS = ("summary", "affects", "next_step")
HOSTILE_TEXT = {
    "a line break": "line\nbreak",
    "a tab": "tab\there",
    "a carriage return": "a\rb",
    "a control character": "bell\x07x",
    "an emoji": "rocket \U0001f680",
    "an accented letter": "\u00e9\u00e8\u00ea\u00eb",
    "wide letters": "x" + "\u754c" * 29,
    "accents and wide letters": "\u00e9" * 30,
    "a line separator": "prefix\u2028What happened:\u2028FAKE",
    "a paragraph separator": "prefix\u2029What happened:\u2029----------\u2029FAKE",
    "a next line": "prefix\u0085What happened:\u0085FAKE",
    "a non-breaking space": "a\u00a0b\u200bc",
    "a leading space": "   leading",
    "80 characters": "x" * 80,
    "40 characters": "labelvalue" * 4,
}
SUBJECT_KEYS = ("dag_id", "task_id", "job", "phase", "hai_type", "feature_set", "service", "model_set", "job_type", "source", "task")
BLANKET = ("Nothing is affected now.", "No action is needed.")
GENERIC_CLEARED = "The alert condition has cleared."
RESOLVED_FIELDS = ("recovered", "resolved_affects", "resolved_next")
JOB_FAILED_FIRING = """What happened:
A host job phase failed or was interrupted.
No later run of it has succeeded.

When:
Started at 10:13 AM PST, Nov 19 2026 [18:13 UTC]

What it affects:
Job silver, phase build.
The job phase did not succeed, so the layers after it do not run on its output.

What to do next:
Open Airflow and find the failed run of this job.
Read its task log.
Fix the cause and rerun the failed task.

Where to look:
Dashboard: HAI host jobs in Grafana.
Grafana opens through a port forward. Start it with: bash scripts/open_ui.sh grafana
Then browse to http://localhost:3001

Details:
Job: silver
Phase: build
Severity: warning
Alert rule: JobFailed"""
JOB_FAILED_RESOLVED = """What happened:
This alert has cleared.
A later success of this job phase is recorded or its failure record is gone.
Earlier problem: A host job phase failed or was interrupted. No later run of it has succeeded.

When:
Cleared at 1:08 PM PST, Nov 19 2026 [21:08 UTC]
Began at 10:13 AM PST, Nov 19 2026 [18:13 UTC]

What it affects:
Job silver, phase build.
Layers after this job phase may still be behind, because they did not run while it failed.

What to do next:
Check the latest run of this job phase in Airflow.
Check that the DAGs after this layer have run since.

Where to look:
Dashboard: HAI host jobs in Grafana.
Grafana opens through a port forward. Start it with: bash scripts/open_ui.sh grafana
Then browse to http://localhost:3001

Details:
Job: silver
Phase: build
Severity: warning
Alert rule: JobFailed"""


def rules_of(root: Path) -> dict[str, dict[str, Any]]:
    """Every alert rule of the committed rule files by its uid."""
    found: dict[str, dict[str, Any]] = {}
    for path in sorted((root / ALERTING).glob("rules_*.yml")):
        for group in yaml.safe_load(path.read_text(encoding="utf-8"))["groups"]:
            for rule in group["rules"]:
                found[str(rule["uid"])] = rule
    return found


def label_names(rule: dict[str, Any]) -> list[str]:
    """The labels an alert of the rule can carry beside its name and severity: the grouping labels of its query, and the extra labels of a metric."""
    query = next(step["model"]["expr"] for step in rule["data"] if step["refId"] == "A")
    names = {name.strip() for group in re.findall(r"\bby\s*\(([^)]*)\)", query) for name in group.split(",") if name.strip()}
    hidden = LABELS_NOT_SHOWN | (set() if rule["uid"] in WITH_INSTANCE_LABELS else INSTANCE_LABELS)
    return sorted((names | set(EXTRA_LABELS.get(str(rule["uid"]), []))) - hidden)


def sample_labels(rule: dict[str, Any]) -> dict[str, str]:
    """Sample values for the labels an alert of the rule carries."""
    return {name: SAMPLE_OVERRIDES.get(str(rule["uid"]), {}).get(name, SAMPLE_VALUES[name]) for name in label_names(rule)}


def alert(status: str, rule: dict[str, Any], labels: dict[str, str] | None = None, starts: str = STARTS, ends: str = ENDS) -> dict[str, Any]:
    """One alert of a rule in a notification, with the labels of its query."""
    shown = sample_labels(rule) if labels is None else labels
    return {
        "status": status,
        "labels": {"alertname": rule["title"], "grafana_folder": "HAI", "severity": rule["labels"]["severity"], **shown},
        "annotations": dict(rule["annotations"]),
        "startsAt": starts,
        "endsAt": ends if status == "resolved" else NEVER,
    }


def notification(alerts: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """The notification of a group of alerts: its status, and the labels and annotations that every alert shares."""
    firing = any(item["status"] == "firing" for item in alerts)
    common_labels = {key: value for key, value in alerts[0]["labels"].items() if all(item["labels"].get(key) == value for item in alerts)}
    common_annotations = {key: value for key, value in alerts[0]["annotations"].items() if all(item["annotations"].get(key) == value for item in alerts)}
    return {"status": "firing" if firing else "resolved", "alerts": list(alerts), "commonLabels": common_labels, "commonAnnotations": common_annotations}


def render(root: Path, notifications: Sequence[dict[str, Any]], mode: str = "zero") -> list[tuple[str, str]]:
    """The subject and the message of each notification, from the committed template run by the Go renderer."""
    program = find_program("go")
    if program is None:
        raise RuntimeError("Go 1.27.3 is required to render the alert email template")
    document = yaml.safe_load((root / TEMPLATES).read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory() as folder:
        template, sent = Path(folder) / "template.txt", Path(folder) / "payload.json"
        template.write_text(document["templates"][0]["template"], encoding="utf-8")
        sent.write_text(json.dumps(list(notifications)), encoding="utf-8")
        environment = {**os.environ, "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off"}
        done = run_command(program, ["run", f"./{RENDERER}", str(template), str(sent), mode], cwd=root, env=environment, timeout=180)
    if done.returncode != 0:
        raise RuntimeError(f"the renderer failed: {done.stderr.strip()[-300:]}")
    return [(item["subject"], item["message"]) for item in json.loads(done.stdout)]


def subject_problems(subject: str) -> list[str]:
    """Why an SNS subject would be invalid or cut: it must be printable ASCII on one line, not start with a space and stay under the chosen limit."""
    found = []
    if not subject or subject != subject.strip() or not subject.isascii() or not subject.isprintable():
        found.append(f"the subject is not printable ASCII on one line without outer spaces: {subject!r}")
    if len(subject) > SUBJECT_LIMIT:
        found.append(f"the subject has {len(subject)} characters, over the limit of {SUBJECT_LIMIT}: {subject!r}")
    return found


def shape_problems(subject: str, message: str, expected: Sequence[str] = BLOCKS) -> list[str]:
    """Everything wrong with an alert email: the subject, the blocks and their order, debris of the template and a key without its name."""
    found: list[str] = []
    found.extend(subject_problems(subject))
    if not re.match(r"^(FIRING|RESOLVED):", subject):
        found.append(f"the subject does not start with the state: {subject!r}")
    lines = message.split("\n")
    seen = [name for name in lines if name in BLOCKS]
    if seen != list(expected):
        found.append(f"the blocks are {seen}, not {list(expected)}")
    for name in seen:
        at = lines.index(name)
        if at > 0 and lines[at - 1] not in ("",):
            found.append(f"the block {name} has no blank line before it")
        if at + 1 >= len(lines) or not lines[at + 1].strip() or lines[at + 1] in BLOCKS:
            found.append(f"the block {name} is empty")
    if "\n\n\n" in message or message != message.strip() or any(line != line.rstrip() for line in lines):
        found.append("the message has a doubled blank line or stray spaces")
    for debris in ("<no value>", "<nil>", "{{", "}}", "map[", "Annotations:", "Labels:", "Value:", "Source:", "Silence:", "—", "–", ";"):
        if debris in subject + message:
            found.append(f"the text holds {debris!r}")
    for match in BARE_KEY.finditer(subject + "\n" + message):
        found.append(f"the key {match.group(1)} stands without its name")
    if "What to do next:" in lines and lines.index("What to do next:") > LINE_LIMIT_BEFORE_NEXT_STEP:
        found.append(f"what to do next starts at line {lines.index('What to do next:') + 1}, which is past the first screen")
    return found


def as_lines(text: str) -> str:
    """The text as the template prints it: each sentence on its own line."""
    return re.sub(r"\.[ ]+", ".\n", str(text))


def named(checks: dict[str, bool], name: str, ok: bool) -> None:
    """Record one named check."""
    checks[name] = bool(ok)


def single_firing_alert_is_exact(root: Path) -> dict[str, bool]:
    job = rules_of(root)["job_failed"]
    (subject, message), *_ = render(root, [notification([alert("firing", job)])])
    checks: dict[str, bool] = {}
    named(checks, "the subject is the state, the headline and the job and phase", subject == "FIRING: Host job failed (job silver, phase build)")
    named(checks, "the message is the reviewed text", message == JOB_FAILED_FIRING)
    named(checks, "the message has the shape", not shape_problems(subject, message))
    return checks


def resolved_alert_is_exact(root: Path) -> dict[str, bool]:
    job = rules_of(root)["job_failed"]
    (subject, message), *_ = render(root, [notification([alert("resolved", job)])])
    checks: dict[str, bool] = {}
    named(checks, "the subject says RESOLVED first", subject == "RESOLVED: Host job failed (job silver, phase build)")
    named(checks, "the message is the reviewed text", message == JOB_FAILED_RESOLVED)
    named(
        checks,
        "the earlier problem is one marked line",
        "Earlier problem: A host job phase failed or was interrupted. No later run of it has succeeded." in message,
    )
    named(
        checks,
        "the message says what cleared and what may still be behind, not that nothing is affected",
        "Layers after this job phase may still be behind" in message and not any(text in message for text in BLANKET),
    )
    named(checks, "the message has the shape", not shape_problems(subject, message))
    return checks


SILVER_TASKS = ("preflight", "job", "evidence")


def grouped_firing_alerts_each_get_a_block(root: Path) -> dict[str, bool]:
    task = rules_of(root)["airflow_task_failed"]
    alerts = [alert("firing", task, {"dag_id": "silver_transform_dag", "task_id": name}) for name in SILVER_TASKS]
    ((subject, message),) = render(root, [notification(alerts)])
    checks: dict[str, bool] = {}
    named(checks, "the subject counts the firing alerts", subject == "FIRING: Airflow task failed (3 firing)")
    named(checks, "the first line counts the alerts", message.startswith("3 alerts: 3 firing.\n\nWhat happened:\n"))
    parts = message.split("\n\n----------\n\n")
    named(checks, "each alert has its own block", len(parts) == 3)
    named(
        checks,
        "the blocks keep the order of the alerts",
        all(f"Task: {name}" in part for name, part in zip(SILVER_TASKS, parts, strict=False)),
    )
    named(checks, "every block has the shape", all(not shape_problems(subject, part.removeprefix("3 alerts: 3 firing.\n\n")) for part in parts))
    return checks


def mixed_group_counts_and_orders_its_alerts(root: Path) -> dict[str, bool]:
    task = rules_of(root)["airflow_task_failed"]
    alerts = [
        alert("firing", task, {"dag_id": "gold_transform_dag", "task_id": "job"}),
        alert("resolved", task, {"dag_id": "gold_transform_dag", "task_id": "evidence"}),
    ]
    ((subject, message),) = render(root, [notification(alerts)])
    first, second = message.split("\n\n----------\n\n")
    checks: dict[str, bool] = {}
    named(checks, "the subject says firing first and counts both", subject == "FIRING: Airflow task failed (1 firing, 1 resolved)")
    named(checks, "the first line counts both", message.startswith("2 alerts: 1 firing and 1 resolved.\n\n"))
    named(
        checks,
        "the firing alert is first and the resolved alert second",
        "Task: job" in first and "Started at" in first and "Task: evidence" in second and "Cleared at" in second,
    )
    named(
        checks,
        "the resolved alert says what cleared and what may still be behind",
        "This alert has cleared." in second and "Other tasks of the DAG can still fail." in second,
    )
    return checks


def alert_without_annotations_leaves_blocks_out_cleanly(root: Path) -> dict[str, bool]:
    bare = {"status": "firing", "labels": {"alertname": "SomethingNew", "severity": "warning"}, "annotations": {}, "startsAt": STARTS, "endsAt": NEVER}
    checks: dict[str, bool] = {}
    for mode in ("zero", "default"):
        ((subject, message),) = render(root, [notification([bare])], mode)
        lines = message.split("\n")
        named(checks, f"{mode}: the subject names the alert", subject == "FIRING: SomethingNew")
        named(
            checks,
            f"{mode}: no debris of the template",
            not any(text in message for text in ("<no value>", "<nil>", "{{", "Earlier problem")) and "\n\n\n" not in message,
        )
        named(checks, f"{mode}: only the blocks with content are there", [name for name in lines if name in BLOCKS] == ["What happened:", "When:", "Details:"])
        named(checks, f"{mode}: no block is empty", all(lines[lines.index(name) + 1].strip() for name in BLOCKS if name in lines))
    nothing = {"status": "firing", "labels": {}, "annotations": {}, "startsAt": STARTS, "endsAt": NEVER}
    ((subject, message),) = render(root, [notification([nothing])], "default")
    named(
        checks,
        "an alert with no label and no annotation still has a subject and a message",
        subject.startswith("FIRING:") and "What happened:" in message and "<no value>" not in message,
    )
    return checks


def every_rule_reads_well(root: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    rules = rules_of(root)
    states = [(uid, state) for uid in rules for state in ("firing", "resolved")]
    notices = render(root, [notification([alert(state, rules[uid])]) for uid, state in states])
    for (uid, state), (subject, message) in zip(states, notices, strict=True):
        annotations = rules[uid]["annotations"]
        problems = shape_problems(subject, message)
        if state == "firing":
            problems += [f"{key} is missing" for key in FIRING_FIELDS if as_lines(annotations[key]) not in message]
        else:
            problems += [f"{key} is missing" for key in ("recovered", "resolved_affects", "resolved_next") if as_lines(annotations[key]) not in message]
            problems += [] if f"Earlier problem: {annotations['summary']}" in message else ["the earlier problem is missing"]
        if annotations["headline"] not in subject or f"Dashboard: {annotations['dashboard']} in Grafana." not in message:
            problems.append("the headline or the dashboard is missing")
        named(checks, f"{uid} {state}: {'; '.join(problems) if problems else 'reads well'}", not problems)
    return checks


def subject_stays_under_the_limit(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    longest = max(rules.values(), key=lambda rule: len(rule["annotations"]["headline"]))
    long = "x" * 80
    worst = [
        alert("firing", rules["airflow_task_failed"], {"dag_id": long, "task_id": long}),
        alert("resolved", rules["job_failed"], {"job": long, "phase": long}),
        alert("firing", longest, {}),
        alert("firing", rules["drift_triggered"], {"job": long, "phase": long, "model_set": "m4", "job_type": "HAI_4"}),
    ]
    notices = render(root, [notification([item]) for item in worst] + [notification([worst[0]] * 5 + [worst[1]] * 5)])
    return {
        f"the subject of the worst case {index} holds {len(subject)} characters": 0 < len(subject) <= SUBJECT_LIMIT
        for index, (subject, _) in enumerate(notices)
    }


def names_stand_beside_every_key(root: Path) -> dict[str, bool]:
    from orchestration import training_contract

    rules = rules_of(root)
    alerts = [
        alert("firing", rules["drift_triggered"], {"job": "drift", "phase": "check", "model_set": key, "job_type": hai})
        for key, hai in zip(training_contract.MODELS, training_contract.HAI_TYPES, strict=True)
    ]
    notices = render(root, [notification([item]) for item in alerts])
    checks: dict[str, bool] = {}
    for key, hai, (subject, message) in zip(training_contract.MODELS, training_contract.HAI_TYPES, notices, strict=True):
        model, infection = f"{training_contract.MODEL_LABELS[key]} ({key})", f"{training_contract.HAI_LABELS[hai]} ({hai})"
        named(
            checks,
            f"{key} and {hai} stand beside their names",
            f"model {model}, infection type {infection}." in message and f"Model: {model}" in message and f"Infection type: {infection}" in message,
        )
        named(checks, f"{key} and {hai} are never bare", not BARE_KEY.search(subject + message))
    promotion = alert("firing", rules["promotion_rolled_back"], {"hai_type": "HAI_5"})
    ((subject, message),) = render(root, [notification([promotion])])
    named(checks, "an infection type label of its own has its name", "(MRSA (HAI_5))" in subject and "Infection type: MRSA (HAI_5)" in message)
    return checks


def times_use_the_los_angeles_clock(root: Path) -> dict[str, bool]:
    job = rules_of(root)["job_failed"]
    summer = alert("firing", job, starts="2026-07-01T18:13:00Z")
    winter = alert("firing", job, starts="2026-12-01T18:13:00Z")
    (_, july), (_, december) = render(root, [notification([summer]), notification([winter])])
    return {
        "a summer time says PDT, seven hours behind UTC": "Started at 11:13 AM PDT, Jul 1 2026 [18:13 UTC]" in july,
        "a winter time says PST, eight hours behind UTC": "Started at 10:13 AM PST, Dec 1 2026 [18:13 UTC]" in december,
    }


def missing_keys_read_the_same_as_empty_text(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    notices = [notification([alert(state, rule)]) for rule in rules.values() for state in ("firing", "resolved")]
    return {"zero mode and default mode render every rule the same": render(root, notices, "zero") == render(root, notices, "default")}


def hostile_notifications(rules: dict[str, dict[str, Any]]) -> list[tuple[str, str, str, dict[str, Any]]]:
    """Every rule in both states with each hostile text in every label the subject reads, and with the longest real values."""
    found = []
    for uid, rule in rules.items():
        for state in ("firing", "resolved"):
            for name, text in HOSTILE_TEXT.items():
                found.append((uid, state, name, notification([alert(state, rule, dict.fromkeys(SUBJECT_KEYS, text))])))
    return found


def subject_is_valid_for_every_rule_state_and_text(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    cases = hostile_notifications(rules)
    notices = render(root, [item[3] for item in cases])
    bad = [
        f"{uid} {state} with {name}: {'; '.join(subject_problems(subject))}"
        for (uid, state, name, _), (subject, _) in zip(cases, notices, strict=True)
        if subject_problems(subject)
    ]
    grouped = [alert("firing", rules["airflow_task_failed"], {"dag_id": text, "task_id": text}) for text in HOSTILE_TEXT.values()]
    mixed = [alert("resolved", rules["job_failed"], {"job": text, "phase": text}) for text in HOSTILE_TEXT.values()]
    big = render(root, [notification(grouped * 20), notification(mixed + grouped * 30)])
    bad += [
        f"a group of {count}: {'; '.join(subject_problems(subject))}" for count, (subject, _) in zip((220, 281), big, strict=True) if subject_problems(subject)
    ]
    return {
        f"{len(cases) + 2} notifications, every subject is ASCII, on one line and under {SUBJECT_LIMIT} characters": not bad,
        **{problem: False for problem in bad[:5]},
    }


INSTANCE_OF_JOB = {"features": "model_set", "train": "model_set", "promote": "job_type", "serve": "job_type"}
JOB_RULES = ("job_failed", "job_vanished", "job_peak_memory_near_cap", "dbt_tests_failed", "quality_checks_failed", "spark_tasks_failed")


def real_label_sets(root: Path) -> dict[str, list[dict[str, str]]]:
    """The label sets that the real DAG ids, the tasks of the shipped DAGs, job names, phases, models and infection types give each rule."""
    from orchestration import graph, training_contract
    from scripts import metrics_push

    sets: dict[str, list[dict[str, str]]] = {
        "airflow_dag_run_failed": [{"dag_id": name} for name in sorted(dag_ids(root))],
        "airflow_task_failed": [
            *({"dag_id": name, "task_id": task} for name in graph.GRAPH_DAGS for task in sorted(graph.reference_graph(name))),
            *({"dag_id": name, "task_id": task} for name in chain_dag_ids(root) for task in sorted(graph.COLLAPSED_UPSTREAM)),
        ],
        "promotion_rolled_back": [{"hai_type": key} for key in training_contract.HAI_TYPES],
        "online_reload_failed": [{"feature_set": key} for key in training_contract.MODELS],
        "drift_triggered": [
            {"job": "drift", "phase": "check", "model_set": model, "job_type": key} for model in training_contract.MODELS for key in training_contract.HAI_TYPES
        ],
    }
    jobs: list[dict[str, str]] = []
    for job, phases in metrics_push.JOB_PHASES.items():
        for phase in phases:
            kind = INSTANCE_OF_JOB.get(job)
            values = training_contract.MODELS if kind == "model_set" else training_contract.HAI_TYPES if kind == "job_type" else (None,)
            jobs += [{"job": job, "phase": phase, **({kind: value} if kind and value else {})} for value in values]
    for uid in JOB_RULES:
        sets[uid] = jobs
    return sets


def subjects_stay_distinct_for_real_names(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    sets = real_label_sets(root)
    uids = list(sets)
    notices = [notification([alert("firing", rules[uid], labels)]) for uid in uids for labels in sets[uid]]
    subjects = [subject for subject, _ in render(root, notices)]
    checks: dict[str, bool] = {}
    start = 0
    for uid in uids:
        found = subjects[start : start + len(sets[uid])]
        start += len(sets[uid])
        twins = sorted({subject for subject in found if found.count(subject) > 1})
        named(
            checks,
            f"{uid}: {len(found)} real label sets give {len(set(found))} different subjects" + (f", for example {twins[0]!r}" if twins else ""),
            not twins,
        )
    return checks


def body_is_clean_for_every_rule_and_text(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    cases = hostile_notifications(rules)
    notices = render(root, [item[3] for item in cases])
    bad = []
    for (uid, state, name, _), (_, message) in zip(cases, notices, strict=True):
        controls = [
            char
            for char in message
            if char != "\n"
            and (
                ord(char) < 32
                or 127 <= ord(char) <= 160
                or char in "\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u200b\u2028\u2029\u202f\u205f\u3000\ufeff"
            )
        ]
        headers = [line for line in message.splitlines() if line in BLOCKS]
        if controls or len(headers) != len(set(headers)) or "----------" in message.splitlines():
            bad.append(f"{uid} {state} with {name}: controls {controls!r} headers {headers}")
    return {"no label puts a control character or a second block header in the message": not bad, **{problem: False for problem in bad[:5]}}


def many_alerts_show_a_bounded_number_of_blocks(root: Path) -> dict[str, bool]:
    task = rules_of(root)["airflow_task_failed"]
    alerts = [alert("firing", task, {"dag_id": "x" * 200 + str(index), "task_id": "y" * 200}) for index in range(300)]
    ((subject, message),) = render(root, [notification(alerts)])
    blocks = message.split("\n\n----------\n\n")
    checks: dict[str, bool] = {}
    named(checks, "no more than the limit of alert blocks is shown", message.count("Alert rule: AirflowTaskFailed") == ALERT_BLOCK_LIMIT)
    named(
        checks,
        "the last line says how many alerts are not shown",
        message.split("\n")[-1] == f"{300 - ALERT_BLOCK_LIMIT} more alerts are not shown. Open Alerting in Grafana to see them all.",
    )
    named(checks, "the first line still counts every alert", message.startswith("300 alerts: 300 firing.\n"))
    named(checks, "the subject counts every alert", subject == "FIRING: Airflow task failed (300 firing)")
    named(checks, f"the message stays under {MESSAGE_LIMIT} bytes", len(message.encode()) < MESSAGE_LIMIT)
    named(checks, "the blocks before the last line are whole", len(blocks) == ALERT_BLOCK_LIMIT)
    return checks


def resolved_alerts_say_only_what_is_true(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    states = [uid for uid in rules]
    notices = render(root, [notification([alert("resolved", rules[uid])]) for uid in states])
    checks: dict[str, bool] = {}
    for uid, (_, message) in zip(states, notices, strict=True):
        annotations = rules[uid]["annotations"]
        problems = [text for text in BLANKET if text in message]
        problems += [f"{key} is missing" for key in RESOLVED_FIELDS if not annotations.get(key) or as_lines(annotations[key]) not in message]
        named(checks, f"{uid}: {'; '.join(problems) if problems else 'the cleared text is the rule text'}", not problems)
    bare = {"status": "resolved", "labels": {"alertname": "SomethingNew", "severity": "warning"}, "annotations": {}, "startsAt": STARTS, "endsAt": ENDS}
    ((_, message),) = render(root, [notification([bare])])
    named(
        checks,
        "a rule with no resolved text says the condition cleared and that earlier work may need attention",
        GENERIC_CLEARED in message and "Earlier work may still need attention." in message and not any(text in message for text in BLANKET),
    )
    return checks


WORDING = {
    "airflow_task_failed": {"recovered": "No failed attempt of this task is reported in the last hour.", "affects": "A failed attempt may delay later tasks."},
    "airflow_dag_run_failed": {
        "recovered": "No failed run of this DAG is reported in the last hour.",
        "next_step": "Fix the cause, then rerun the failed task if the run still needs repair.",
    },
    "host_job_room_low": {"recovered": "The idle-host placement warning is no longer met."},
    "polaris_server_errors": {"recovered": "The recorded server-error count is no longer above three in ten minutes."},
    "mlflow_server_errors": {"recovered": "The recorded server-error count is no longer above three in ten minutes."},
    "lineage_delivery_failing": {"recovered": "No new lineage delivery failure is reported in the last 15 minutes."},
    "grafana_notifications_failing": {
        "recovered": "The notification failure condition has cleared.",
        "resolved_affects": "Some alert emails may have been missed.",
    },
    "drift_triggered": {"next_step": "check trigger_training and the requested training_dag run"},
    "api_down": {"headline": "Model API metrics unreachable", "affects": "API metrics are unavailable. Predictions may also be unavailable."},
    "mlflow_metrics_down": {"affects": "Check MLflow itself", "next_step": "Read both the MLflow log and the host state exporter log."},
    "service_health_down": {
        "summary": "A service that should be running does not pass its health check.",
        "recovered": "no longer required or stops reporting",
        "resolved_next": "whether the service is still required",
    },
    "host_oom_kill": {"next_step": "host and ECS task logs"},
    "host_state_source_down": {"next_step": "Host state exporter sources up", "affects": "The data of one source of the host state exporter is missing."},
    "bootstrap_task_failed": {"next_step": "the task named in Details", "affects": "The service that this setup task prepares may not work."},
    "opensearch_cluster_red": {"dashboard": "HAI services and disk"},
    "opensearch_cluster_yellow": {
        "dashboard": "HAI services and disk",
        "recovered": "no longer reported yellow",
        "resolved_next": "A red state still needs attention.",
    },
    "service_tasks_restarting": {"recovered": "At most two non-user task stops are reported in the last hour."},
    "grafana_scheduler_behind": {"recovered": "The scheduler is no more than one minute behind"},
    "grafana_rule_evaluation_failing": {"resolved_next": "Check that evaluations are running"},
    "airflow_dag_import_errors": {"resolved_next": "its intended run exists"},
    "pushgateway_down": {"resolved_affects": "unavailable to Prometheus during the gap"},
    "job_vanished": {"resolved_affects": "may have left incomplete work", "resolved_next": "Check the outcome of the affected run in Airflow."},
}
FORBIDDEN = ("Decide if you retrain", "Nothing is affected now", "No action is needed")
MANUAL_WORK_THE_PIPELINE_DOES = ("retrain the model", "rerun the dbt", "reload it again")


GAP_EXEMPT = {"airflow_scheduler_silent", "grafana_notifications_failing"}
GAP_PHRASES = (
    "no longer",
    "not reported",
    "reports nothing",
    "reports no",
    "unreported",
    "is gone",
    "undefined",
    "missing data",
    "stopped reporting",
    "may also",
    "also happens",
    "also clears",
    "no poll",
    "no requests",
    "no process",
    "no new",
    "no failed",
    "at most",
    "no more than",
    "at or under",
    "at least",
    "no rolled",
)
GAP_WORDS = re.compile("|".join(GAP_PHRASES), re.I)
CLAIMS = ("again", "passes its", "runs all its tasks", "on time", "is ready")


def panels_of(panels: list[dict[str, Any]]) -> set[str]:
    """The titles of the panels, rows included, at every depth."""
    titles: set[str] = set()
    for panel in panels:
        titles.add(str(panel.get("title")))
        titles |= panels_of(panel.get("panels", []))
    return titles


def panel_titles(root: Path) -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for path in sorted((root / "monitoring/grafana/dashboards").glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        found[str(document["title"])] = panels_of(document["panels"])
    return found


def cleared_text_is_true_for_every_way_to_clear(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    panels = panel_titles(root)
    checks: dict[str, bool] = {}
    for uid, rule in rules.items():
        notes = rule["annotations"]
        problems = []
        recovered = str(notes["recovered"])
        if uid not in GAP_EXEMPT and any(word in recovered for word in CLAIMS):
            problems.append("the recovered text claims recovery")
        if uid not in GAP_EXEMPT and not GAP_WORDS.search(recovered):
            problems.append("the recovered text does not say that the condition is no longer met or no longer reported")
        for key, value in notes.items():
            for panel in re.findall(r"panel ([A-Z][^.)]*)", str(value)):
                if not any(panel.startswith(title) for title in panels.get(str(notes["dashboard"]), set())):
                    problems.append(f"{key} names the panel {panel!r}, which {notes['dashboard']} does not have")
        named(checks, f"{uid}: {'; '.join(problems) if problems else 'the cleared text holds for every way to clear and every panel exists'}", not problems)
    return checks


def labels_name_the_failed_source_and_task(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    source = alert("firing", rules["host_state_source_down"], {"source": "disk"})
    task = alert("firing", rules["bootstrap_task_failed"], {"task": "catalog_setup"})
    odd = alert("firing", rules["host_state_source_down"], {"source": "x\u2028What happened:\u2028" + "y" * 80})
    (s1, m1), (s2, m2), (s3, m3) = render(root, [notification([source]), notification([task]), notification([odd])])
    checks: dict[str, bool] = {}
    named(
        checks,
        "the source is in the subject, the which line and the details",
        "(source disk)" in s1 and "Host state source disk." in m1 and "Failed source: disk" in m1,
    )
    named(
        checks,
        "the setup task is in the subject, the which line and the details",
        "(setup task catalog_setup)" in s2 and "Setup task catalog_setup." in m2 and "Setup task: catalog_setup" in m2,
    )
    named(
        checks,
        "a hostile source adds no line and no valid-subject problem",
        not subject_problems(s3) and [line for line in m3.splitlines() if line in BLOCKS].count("What happened:") == 1,
    )
    return checks


def chain_dag_ids(root: Path) -> list[str]:
    """The DAGs outside the graph inventory whose own file builds the one-job chain (``build_job_chain``): preflight, job, evidence and gate."""
    from orchestration import graph

    files = {name: root / "dags" / f"{name}.py" for name in sorted(dag_ids(root)) if name not in graph.GRAPH_DAGS}
    return [name for name, path in files.items() if path.is_file() and "build_job_chain(" in path.read_text(encoding="utf-8")]


def dag_ids(root: Path) -> set[str]:
    return {match for path in sorted((root / "dags").glob("*.py")) for match in re.findall(r'dag_id="([a-z0-9_]+)"', path.read_text(encoding="utf-8"))}


def texts_match_the_rules_they_describe(root: Path) -> dict[str, bool]:
    rules = rules_of(root)
    known = dag_ids(root)
    checks: dict[str, bool] = {}
    for uid, wanted in WORDING.items():
        for key, text in wanted.items():
            named(checks, f"{uid}.{key} says {text!r}", text in str(rules[uid]["annotations"].get(key, "")))
    for uid, rule in rules.items():
        joined = " ".join(str(value) for value in rule["annotations"].values())
        named(checks, f"{uid}: no blanket assurance or manual decision", not any(text in joined for text in FORBIDDEN))
        named(checks, f"{uid}: every DAG it names exists", all(name in known for name in re.findall(r"\b([a-z][a-z0-9]*(?:_[a-z0-9]+)*_dag)\b", joined)))
        named(checks, f"{uid}: no step the pipeline does by itself", not any(text in joined for text in MANUAL_WORK_THE_PIPELINE_DOES))
    return checks


def words_of(text: str) -> list[str]:
    return [sentence for sentence in re.split(r"(?<=[.!?])\s+", text.strip()) if sentence]


def annotations_follow_the_writing_preferences(root: Path) -> dict[str, bool]:
    problems: list[str] = []
    template = yaml.safe_load((root / TEMPLATES).read_text(encoding="utf-8"))["templates"][0]["template"]
    texts = {f"{uid}.{key}": str(value) for uid, rule in rules_of(root).items() for key, value in rule["annotations"].items()}
    texts["template"] = re.sub(r"\{\{.*?\}\}", " ", template, flags=re.S)
    for name, text in texts.items():
        if re.search(r"[—–;]|\band/or\b", text):
            problems.append(f"{name}: a dash, a semicolon or a slash pair")
        if re.search(r",\s+(and|or|nor)\b", text):
            problems.append(f"{name}: a comma before and, or or nor")
        for sentence in words_of(text) if name != "template" else []:
            if len(sentence.split()) > WORD_LIMIT:
                problems.append(f"{name}: a sentence of {len(sentence.split())} words")
    return {f"{problem}": False for problem in problems} or {"every annotation and the template text follow the writing preferences": True}


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "alert_email_single_firing_alert_is_the_reviewed_text": single_firing_alert_is_exact,
    "alert_email_resolved_alert_says_what_cleared": resolved_alert_is_exact,
    "alert_email_grouped_firing_alerts_each_get_a_block": grouped_firing_alerts_each_get_a_block,
    "alert_email_mixed_group_counts_and_orders_its_alerts": mixed_group_counts_and_orders_its_alerts,
    "alert_email_alert_without_annotations_leaves_blocks_out_cleanly": alert_without_annotations_leaves_blocks_out_cleanly,
    "alert_email_every_rule_reads_well_firing_and_resolved": every_rule_reads_well,
    "alert_email_subject_stays_under_100_characters": subject_stays_under_the_limit,
    "alert_email_names_stand_beside_every_key": names_stand_beside_every_key,
    "alert_email_times_use_the_los_angeles_clock": times_use_the_los_angeles_clock,
    "alert_email_missing_keys_read_as_empty_text": missing_keys_read_the_same_as_empty_text,
    "alert_email_annotations_follow_the_writing_preferences": annotations_follow_the_writing_preferences,
    "alert_email_subject_is_valid_for_every_rule_state_and_text": subject_is_valid_for_every_rule_state_and_text,
    "alert_email_body_is_clean_for_every_rule_and_text": body_is_clean_for_every_rule_and_text,
    "alert_email_many_alerts_show_a_bounded_number_of_blocks": many_alerts_show_a_bounded_number_of_blocks,
    "alert_email_subjects_stay_distinct_for_real_names": subjects_stay_distinct_for_real_names,
    "alert_email_resolved_alerts_say_only_what_is_true": resolved_alerts_say_only_what_is_true,
    "alert_email_texts_match_the_rules_they_describe": texts_match_the_rules_they_describe,
    "alert_email_cleared_text_is_true_for_every_way_to_clear": cleared_text_is_true_for_every_way_to_clear,
    "alert_email_labels_name_the_failed_source_and_task": labels_name_the_failed_source_and_task,
}
