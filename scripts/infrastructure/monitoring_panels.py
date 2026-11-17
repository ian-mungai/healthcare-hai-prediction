"""Check that every provisioned Grafana panel has data: run each panel expression against a Prometheus and list the panels that return nothing.

Run from the repository root after a chain run, with the Prometheus of the host reachable through the Session Manager tunnel:
``.venv/bin/python -m scripts.infrastructure.monitoring_panels --url http://127.0.0.1:19090``. The check reads ``monitoring/grafana/dashboards/*.json`` and
``monitoring/panel_data_allowed_empty.json`` and sends one instant query per panel target to ``/api/v1/query``. It writes nothing to Prometheus or AWS.

- A target has data when the query returns at least one sample that is not NaN. A NaN sample (a division of zero by zero) counts as no data.
- A panel may be empty only when the allowance file names it with a reason. An allowance for a panel that has data or for a panel that does not exist is a
  problem, so the list stays honest.
- Exit codes: 0 every panel has data, 1 at least one problem, 2 Prometheus cannot be reached.
- Every wait has a deadline: each request has its own timeout and the whole check has one deadline.
"""

from __future__ import annotations

import argparse
import http.client
import json
import math
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS = "monitoring/grafana/dashboards"
ALLOWED = "monitoring/panel_data_allowed_empty.json"
OUTPUT = Path("data/e2e/monitoring_panels")
INTERVALS = {"$__rate_interval": "5m", "$__interval": "5m", "$__range": "1h"}
VARIABLE = re.compile(r"\$\{?[A-Za-z_]\w*\}?")
REQUEST_TIMEOUT = 10.0
DEADLINE = 120.0


@dataclass(frozen=True)
class Target:
    dashboard: str
    title: str
    ref: str
    expression: str


class Unreachable(Exception):
    """Prometheus did not answer at the address."""


class DeadlinePassed(Exception):
    """The deadline ended a request."""


def panel_list(panels: list[Any], dashboard: str, found: list[Target]) -> None:
    for panel in panels:
        if not isinstance(panel, dict):
            continue
        nested = panel.get("panels")
        if isinstance(nested, list):
            panel_list(nested, dashboard, found)
        for target in panel.get("targets") or []:
            if isinstance(target, dict) and isinstance(target.get("expr"), str) and target["expr"].strip() and not target.get("hide"):
                found.append(Target(dashboard, str(panel.get("title", "")), str(target.get("refId", "")), target["expr"]))


def targets(root: Path, problems: list[str]) -> list[Target]:
    """Every visible target with an expression in the committed dashboards, in file and panel order."""
    found: list[Target] = []
    for path in sorted((root / DASHBOARDS).glob("*.json")):
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            problems.append(f"{DASHBOARDS}/{path.name} cannot be read as JSON ({type(error).__name__})")
            continue
        if isinstance(document, dict) and isinstance(document.get("panels"), list):
            panel_list(document["panels"], path.name, found)
    return found


def panels(root: Path) -> list[Target]:
    return targets(root, [])


def allowance(root: Path, problems: list[str]) -> dict[tuple[str, str], str]:
    """The reviewed empty panels as {(dashboard file, panel title): reason}; a missing file allows nothing."""
    path = root / ALLOWED
    if not path.is_file():
        return {}
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        problems.append(f"{ALLOWED} cannot be read as JSON ({type(error).__name__})")
        return {}
    entries = document.get("allowed") if isinstance(document, dict) else None
    if not isinstance(entries, list):
        problems.append(f'{ALLOWED} must be an object with an "allowed" list')
        return {}
    allowed: dict[tuple[str, str], str] = {}
    for entry in entries:
        reason = str(entry.get("reason", "")).strip() if isinstance(entry, dict) else ""
        key = (str(entry.get("dashboard", "")), str(entry.get("panel", ""))) if isinstance(entry, dict) else ("", "")
        if not reason:
            problems.append(f"{ALLOWED}: the allowance for '{key[1]}' in {key[0]} needs a reason")
            continue
        allowed[key] = reason
    return allowed


def resolved(expression: str) -> str:
    for name, value in INTERVALS.items():
        expression = expression.replace(name, value)
    return expression


def has_sample(data: dict[str, Any]) -> bool:
    result = data.get("result")
    if data.get("resultType") == "scalar":
        result = [{"value": result}] if isinstance(result, list) and result else []
    for item in result if isinstance(result, list) else []:
        try:
            if not math.isnan(float(item["value"][1])):
                return True
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    return False


def ask(base: tuple[str, int], expression: str, timeout: float) -> tuple[str, str]:
    """One instant query: ("data", "") or ("empty", "") or ("error", "<type>: <message>")."""
    connection = http.client.HTTPConnection(base[0], base[1], timeout=timeout)
    try:
        connection.request("GET", f"/api/v1/query?query={quote(expression, safe='')}")
        response = connection.getresponse()
        raw = response.read()
    except TimeoutError:
        raise DeadlinePassed from None
    except OSError as error:
        raise Unreachable(type(error).__name__) from None
    finally:
        connection.close()
    try:
        body = json.loads(raw)
    except ValueError:
        return "error", f"HTTP {response.status} with a body that is not JSON"
    if not isinstance(body, dict) or body.get("status") != "success":
        detail = body if isinstance(body, dict) else {}
        return "error", f"{detail.get('errorType', 'HTTP ' + str(response.status))}: {detail.get('error', '')}".strip()
    data = body.get("data")
    return ("data" if isinstance(data, dict) and has_sample(data) else "empty"), ""


def run_check(root: Path, url: str, request_timeout: float, deadline: float) -> tuple[list[str], bool, dict[str, Any]]:
    """The problems, whether Prometheus was unreachable and a report of every target."""
    problems: list[str] = []
    parsed = urlparse(url)
    if parsed.scheme != "http" or not parsed.hostname:
        return [f"the Prometheus address {url} must be an http address with a host"], False, {}
    base = (parsed.hostname, parsed.port or 80)
    found = targets(root, problems)
    allowed = allowance(root, problems)
    if not found:
        problems.append(f"{DASHBOARDS} holds no panel with an expression, so there is no panel to check")
    ends = time.monotonic() + deadline
    outcome: dict[tuple[str, str], list[tuple[Target, str, str]]] = {}
    answered = 0
    unreachable = False
    for target in found:
        left = ends - time.monotonic()
        text = resolved(target.expression)
        if VARIABLE.search(text):
            problems.append(
                f"{target.dashboard}: panel '{target.title}' target {target.ref} uses a dashboard variable that this check cannot fill: {target.expression}"
            )
            continue
        try:
            if left <= 0:
                raise DeadlinePassed
            state, detail = ask(base, text, min(request_timeout, left))
        except DeadlinePassed:
            problems.append(f"the deadline of {deadline:g} seconds passed with {answered} of {len(found)} targets answered")
            break
        except Unreachable as error:
            problems.append(f"Prometheus at {url} is unreachable ({error})")
            unreachable = True
            break
        answered += 1
        outcome.setdefault((target.dashboard, target.title), []).append((target, state, detail))
    seen = {(target.dashboard, target.title) for target in found}
    for dashboard, title in allowed:
        if (dashboard, title) not in seen:
            problems.append(f"{ALLOWED}: the allowance for '{title}' in {dashboard} matches no panel; remove it")
    for (dashboard, title), results in outcome.items():
        permitted = (dashboard, title) in allowed
        for target, state, detail in results:
            if state == "error":
                problems.append(f"{dashboard}: panel '{title}' target {target.ref} was refused by Prometheus ({detail}): {target.expression}")
            elif state == "empty" and not permitted:
                problems.append(f"{dashboard}: panel '{title}' target {target.ref} has no data: {target.expression}")
        if permitted and all(state == "data" for _, state, _ in results):
            problems.append(f"{ALLOWED}: panel '{title}' in {dashboard} has data now; remove its allowance")
    report = {
        "url": url,
        "targets": [
            {"dashboard": d, "panel": t, "ref": target.ref, "expression": target.expression, "state": state, "allowed": (d, t) in allowed}
            for (d, t), results in outcome.items()
            for target, state, _ in results
        ],
    }
    return problems, unreachable, report


def check(root: Path, url: str, *, request_timeout: float = REQUEST_TIMEOUT, deadline: float = DEADLINE) -> list[str]:
    """The problems of the committed dashboards against the Prometheus at ``url``; empty when every panel has data."""
    return run_check(root, url, request_timeout, deadline)[0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", required=True, help="the Prometheus address, for example http://127.0.0.1:19090 through the tunnel")
    parser.add_argument("--root", type=Path, default=ROOT, help="the repository root that holds monitoring/")
    parser.add_argument("--request-timeout", type=float, default=REQUEST_TIMEOUT)
    parser.add_argument("--deadline", type=float, default=DEADLINE)
    parser.add_argument("--no-report", action="store_true", help="do not write the report file")
    arguments = parser.parse_args(argv)
    problems, unreachable, report = run_check(arguments.root, arguments.url, arguments.request_timeout, arguments.deadline)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    if not arguments.no_report:
        try:
            folder = arguments.root / OUTPUT
            folder.mkdir(parents=True, exist_ok=True)
            (folder / f"report_{stamp}.json").write_text(
                json.dumps({"run_at_utc": stamp, "problems": problems, **report}, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        except OSError as error:
            problems.append(f"the report could not be saved ({type(error).__name__})")
    for problem in problems:
        sys.stdout.write(f"FAIL {problem}\n")
    sys.stdout.write(f"{len(report.get('targets', []))} targets answered, {len(problems)} problems.\n")
    if unreachable:
        return 2
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
