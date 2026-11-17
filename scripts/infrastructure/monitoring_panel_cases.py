"""Cases for the panel data check: a stand-in Prometheus on loopback answers each query, and each planted defect must be reported with its own message.

The stand-in answers ``/api/v1/query`` with a sample, an empty vector, a NaN sample, an error status or after a delay, chosen per expression. No AWS service is
contacted and no container starts. Every stand-in server is closed before its case returns.
"""

from __future__ import annotations

import json
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from scripts.infrastructure import monitoring_panels

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE = "1"


class StandIn:
    """A loopback Prometheus. ``modes`` maps an exact expression to ``empty``, ``nan``, ``error`` or ``slow``; any other expression returns one sample."""

    def __init__(self, modes: Mapping[str, str] | None = None, delay: float = 0.0) -> None:
        self.modes = dict(modes or {})
        self.delay = delay
        self.queries: list[str] = []
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                expression = parse_qs(parsed.query).get("query", [""])[0]
                stand_in.queries.append(expression)
                mode = stand_in.modes.get(expression, "sample")
                body: dict[str, Any]
                if mode == "slow":
                    time.sleep(stand_in.delay)
                if parsed.path != "/api/v1/query" or mode == "error":
                    body = {"status": "error", "errorType": "bad_data", "error": "parse error"}
                elif mode == "empty":
                    body = {"status": "success", "data": {"resultType": "vector", "result": []}}
                else:
                    value = "NaN" if mode == "nan" else SAMPLE
                    body = {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [0, value]}]}}
                payload = json.dumps(body).encode()
                self.send_response(200 if body["status"] == "success" else 400)
                self.send_header("Content-Type", "application/json")
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
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self) -> StandIn:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


def tree(scratch: Path, panels: list[dict[str, Any]], allowed: object | None = None) -> Path:
    """A scratch repository root with one dashboard holding ``panels`` and, when given, the allowance file."""
    folder = scratch / "monitoring/grafana/dashboards"
    folder.mkdir(parents=True)
    (folder / "demo.json").write_text(json.dumps({"uid": "demo", "title": "Demo", "panels": panels}), encoding="utf-8")
    if allowed is not None:
        (scratch / monitoring_panels.ALLOWED).write_text(json.dumps(allowed), encoding="utf-8")
    return scratch


def panel(title: str, *expressions: str, kind: str = "timeseries") -> dict[str, Any]:
    return {"type": kind, "title": title, "targets": [{"refId": chr(65 + index), "expr": expression} for index, expression in enumerate(expressions)]}


PANELS = [panel("Up", "up"), panel("Rate", "rate(hai_x_total[$__rate_interval])"), {"type": "row", "title": "Row", "panels": []}]


def run(root: Path, stand_in: StandIn, **options: Any) -> list[str]:
    return monitoring_panels.check(root, stand_in.url, **options)


def committed_dashboards_pass_except_the_allowed_empty_ones(root: Path) -> dict[str, bool]:
    """Every committed panel target is queried, and the tracked allowance file is valid when the server answers every expression."""
    allowed = {(entry["dashboard"], entry["panel"]) for entry in json.loads((root / monitoring_panels.ALLOWED).read_text(encoding="utf-8"))["allowed"]}
    found = monitoring_panels.panels(root)
    empty = {target.expression: "empty" for target in found if (target.dashboard, target.title) in allowed}
    with StandIn(empty) as stand_in:
        problems = monitoring_panels.check(root, stand_in.url)
    return {
        "every allowed panel names a real panel of a committed dashboard": allowed <= {(target.dashboard, target.title) for target in found},
        "the committed dashboards have no problem": problems == [],
        "every committed target was queried": len(stand_in.queries) >= len(found) > 0,
    }


def every_panel_has_data(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn() as stand_in:
        problems = run(tree(Path(scratch), PANELS), stand_in)
    return {"no problem when every query returns a sample": problems == [], "the row panel is not queried": len(stand_in.queries) == 2}


def empty_result_is_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn({"up": "empty"}) as stand_in:
        problems = run(tree(Path(scratch), PANELS), stand_in)
    return {
        "the empty panel is named with its dashboard and expression": any("demo.json" in p and "'Up'" in p and "no data" in p and "up" in p for p in problems),
        "the other panel is not": len(problems) == 1,
    }


def nan_result_is_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn({"up": "nan"}) as stand_in:
        problems = run(tree(Path(scratch), PANELS), stand_in)
    return {"a NaN sample counts as no data": any("'Up'" in p and "no data" in p for p in problems)}


def second_target_empty_is_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn({"b_metric": "empty"}) as stand_in:
        problems = run(tree(Path(scratch), [panel("Pair", "a_metric", "b_metric")]), stand_in)
    return {"each target of a panel must have data": len(problems) == 1 and "b_metric" in problems[0] and "target B" in problems[0]}


def grafana_interval_variables_are_replaced(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn() as stand_in:
        problems = run(tree(Path(scratch), PANELS), stand_in)
    return {
        "the rate interval is sent as five minutes": "rate(hai_x_total[5m])" in stand_in.queries,
        "no problem is reported for the variable": problems == [],
    }


def other_variable_is_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn() as stand_in:
        problems = run(tree(Path(scratch), [panel("Var", 'up{job="$job"}')]), stand_in)
    return {"an unresolved dashboard variable is reported": any("$job" in p and "variable" in p for p in problems), "it is not sent": stand_in.queries == []}


def server_error_is_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn({"up": "error"}) as stand_in:
        problems = run(tree(Path(scratch), PANELS), stand_in)
    return {"a query Prometheus refuses names the panel and the error": any("'Up'" in p and "bad_data" in p for p in problems)}


def unreachable_prometheus_is_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch:
        with StandIn() as stand_in:
            url = stand_in.url
        problems = monitoring_panels.check(tree(Path(scratch), PANELS), url, request_timeout=2.0)
    return {"a closed port is one problem naming the server": len(problems) == 1 and "unreachable" in problems[0] and url in problems[0]}


def deadline_is_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn({"up": "slow"}, delay=1.5) as stand_in:
        started = time.monotonic()
        problems = run(tree(Path(scratch), [panel("Slow", "up"), panel("After", "other")]), stand_in, request_timeout=5.0, deadline=1.0)
        elapsed = time.monotonic() - started
    return {
        "the deadline is reported with the panels left unchecked": any("deadline" in p and "0 of 2" in p for p in problems),
        "the check ends soon after the deadline": elapsed < 4.0,
    }


def allowed_empty_panel_passes_with_its_reason(root: Path) -> dict[str, bool]:
    allowed = {"allowed": [{"dashboard": "demo.json", "panel": "Up", "reason": "no traffic before the first prediction"}]}
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn({"up": "empty"}) as stand_in:
        problems = run(tree(Path(scratch), PANELS, allowed), stand_in)
    return {"a reviewed empty panel is not a problem": problems == []}


def allowance_for_a_missing_panel_is_reported(root: Path) -> dict[str, bool]:
    allowed = {"allowed": [{"dashboard": "demo.json", "panel": "Gone", "reason": "was removed"}]}
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn() as stand_in:
        problems = run(tree(Path(scratch), PANELS, allowed), stand_in)
    return {"an allowance that matches no panel is reported": any("Gone" in p and "no panel" in p for p in problems)}


def allowance_for_a_panel_with_data_is_reported(root: Path) -> dict[str, bool]:
    allowed = {"allowed": [{"dashboard": "demo.json", "panel": "Up", "reason": "no traffic"}]}
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn() as stand_in:
        problems = run(tree(Path(scratch), PANELS, allowed), stand_in)
    return {"a stale allowance is reported so the list stays honest": any("'Up'" in p and "has data" in p and "remove" in p for p in problems)}


def allowance_without_a_reason_is_reported(root: Path) -> dict[str, bool]:
    allowed = {"allowed": [{"dashboard": "demo.json", "panel": "Up", "reason": " "}]}
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn({"up": "empty"}) as stand_in:
        problems = run(tree(Path(scratch), PANELS, allowed), stand_in)
    return {"an allowance needs a reason": any("reason" in p for p in problems), "the panel still counts as empty": any("no data" in p for p in problems)}


def allowance_file_is_not_valid_is_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn() as stand_in:
        problems = run(tree(Path(scratch), PANELS, ["not", "an", "object"]), stand_in)
    return {"a malformed allowance file is reported": any(monitoring_panels.ALLOWED in p for p in problems)}


def dashboards_without_targets_are_reported(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch, StandIn() as stand_in:
        problems = run(tree(Path(scratch), [{"type": "row", "title": "Row", "panels": []}]), stand_in)
    return {"no panel to check is a problem, not a pass": any("no panel" in p for p in problems)}


def command_exit_codes(root: Path) -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="panel_case_") as scratch:
        folder = tree(Path(scratch), PANELS)
        with StandIn({"up": "empty"}) as stand_in:
            empty = monitoring_panels.main(["--url", stand_in.url, "--root", str(folder), "--no-report"])
            stand_in.modes.clear()
            full = monitoring_panels.main(["--url", stand_in.url, "--root", str(folder), "--no-report"])
            url = stand_in.url
        dead = monitoring_panels.main(["--url", url, "--root", str(folder), "--no-report", "--request-timeout", "2"])
    return {"an empty panel exits 1": empty == 1, "a full set exits 0": full == 0, "an unreachable server exits 2": dead == 2}


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "panel_committed_dashboards_pass_except_the_allowed_empty_ones": committed_dashboards_pass_except_the_allowed_empty_ones,
    "panel_every_panel_has_data": every_panel_has_data,
    "panel_empty_result_is_reported": empty_result_is_reported,
    "panel_nan_result_is_reported": nan_result_is_reported,
    "panel_second_target_empty_is_reported": second_target_empty_is_reported,
    "panel_grafana_interval_variables_are_replaced": grafana_interval_variables_are_replaced,
    "panel_other_variable_is_reported": other_variable_is_reported,
    "panel_server_error_is_reported": server_error_is_reported,
    "panel_unreachable_prometheus_is_reported": unreachable_prometheus_is_reported,
    "panel_deadline_is_reported": deadline_is_reported,
    "panel_allowed_empty_panel_passes_with_its_reason": allowed_empty_panel_passes_with_its_reason,
    "panel_allowance_for_a_missing_panel_is_reported": allowance_for_a_missing_panel_is_reported,
    "panel_allowance_for_a_panel_with_data_is_reported": allowance_for_a_panel_with_data_is_reported,
    "panel_allowance_without_a_reason_is_reported": allowance_without_a_reason_is_reported,
    "panel_allowance_file_is_not_valid_is_reported": allowance_file_is_not_valid_is_reported,
    "panel_dashboards_without_targets_are_reported": dashboards_without_targets_are_reported,
    "panel_command_exit_codes": command_exit_codes,
}
