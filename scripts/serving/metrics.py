"""The model API's Prometheus metrics, kept in this process and rendered in the text exposition format.

The families are the contract with the monitoring stack (``data_contracts/serving/metrics.json``): a request counter with the labels ``route``, ``method`` and
``status``, a duration histogram with the label ``route``, the state of the online feature store's marker as the API last read it (per feature set) and a
counter of features refusals by one closed reason. Every label value comes from a closed set, so the number of series stays bounded however many paths a
scanner tries. No key, hospital, request id or load ID ever becomes a label. The state is a few small tables behind one lock.
"""

from __future__ import annotations

import re
import threading

ROUTES = ("/v1/predict", "/v1/health", "/v1/live", "/metrics")
METHODS = ("GET", "POST", "HEAD", "OPTIONS")
STATUSES = (200, 400, 401, 404, 405, 408, 413, 422, 429, 500, 503)
BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
OTHER = "other"
SINGLE = "single"
SET_NAME = re.compile(r"[a-z][a-z0-9_]{0,15}")
MAX_SETS = 8
MARKER_STATES = ("ready", "loading", "absent", "unreadable", OTHER)
REASONS = (
    "marker_absent",
    "marker_loading",
    "marker_not_ready",
    "schema_hash_mismatch",
    "marker_changed",
    "marker_read_failed",
    "feature_read_failed",
    "marker_recheck_failed",
    "features_missing",
    "view_row_absent",
    "no_references",
    "reference_invalid",
    "set_not_online",
    "set_readers_missing",
    "hospital_window_not_found",
)
MARKER_STATE = "hai_api_marker_state"
MARKER_MATERIALIZED = "hai_api_marker_materialized_timestamp_seconds"
MARKER_ENTITIES = "hai_api_marker_entities"
MARKER_READ = "hai_api_marker_read_timestamp_seconds"
REFUSALS = "hai_api_feature_refusals_total"
REQUESTS = "hai_api_requests_total"
DURATION = "hai_api_request_duration_seconds"


def route_label(path: str) -> str:
    return path if path in ROUTES else OTHER


def method_label(method: str) -> str:
    return method if method in METHODS else OTHER


def status_label(status: int) -> str:
    return str(status) if status in STATUSES else OTHER


def reason_label(reason: str) -> str:
    return reason if reason in REASONS else OTHER


def state_label(state: str) -> str:
    return state if state in MARKER_STATES else OTHER


def bound_text(bound: float) -> str:
    return repr(bound) if bound != int(bound) else str(int(bound))


class Metrics:
    def __init__(self) -> None:
        self._requests: dict[tuple[str, str, str], int] = {}
        self._durations: dict[str, tuple[list[int], float, int]] = {}
        self._refusals: dict[str, int] = {}
        self._markers: dict[str, tuple[str, float | None, int | None, float]] = {}
        self._lock = threading.Lock()

    def observe(self, route: str, method: str, status: str, seconds: float) -> None:
        """Count one answered request and file its duration. The arguments are label values from the closed sets above."""
        with self._lock:
            key = (route, method, status)
            self._requests[key] = self._requests.get(key, 0) + 1
            counts, total, count = self._durations.get(route, ([0] * len(BUCKETS), 0.0, 0))
            for index, bound in enumerate(BUCKETS):
                if seconds <= bound:
                    counts[index] += 1
            self._durations[route] = (counts, total + seconds, count + 1)

    def refusal(self, reason: str) -> None:
        """Count one features refusal by its reason; a reason outside the closed set is ``other``."""
        with self._lock:
            label = reason_label(reason)
            self._refusals[label] = self._refusals.get(label, 0) + 1

    def marker_read(self, feature_set: str, state: str, materialized_at: float | None, entities: int | None, at: float) -> None:
        """Keep what the latest marker read of a feature set showed. A set name outside the closed form, or a ninth set, is not kept."""
        label = feature_set or SINGLE
        with self._lock:
            if label != SINGLE and not SET_NAME.fullmatch(label):
                return
            if label not in self._markers and len(self._markers) >= MAX_SETS:
                return
            self._markers[label] = (state_label(state), materialized_at, entities, at)

    def render(self) -> str:
        with self._lock:
            requests, durations = dict(self._requests), {route: (list(counts), total, count) for route, (counts, total, count) in self._durations.items()}
            refusals, markers = dict(self._refusals), dict(self._markers)
        lines = [f"# HELP {REQUESTS} Requests answered by the model API.", f"# TYPE {REQUESTS} counter"]
        for (route, method, status), value in sorted(requests.items()):
            lines.append(f'{REQUESTS}{{route="{route}",method="{method}",status="{status}"}} {value}')
        lines += [f"# HELP {DURATION} Seconds from the request arriving to its answer.", f"# TYPE {DURATION} histogram"]
        for route, (counts, total, count) in sorted(durations.items()):
            for bound, value in zip(BUCKETS, counts, strict=True):
                lines.append(f'{DURATION}_bucket{{route="{route}",le="{bound_text(bound)}"}} {value}')
            lines.append(f'{DURATION}_bucket{{route="{route}",le="+Inf"}} {count}')
            lines.append(f'{DURATION}_sum{{route="{route}"}} {total!r}')
            lines.append(f'{DURATION}_count{{route="{route}"}} {count}')
        lines += [
            f"# HELP {MARKER_STATE} 1 for the state the latest marker read of the feature set showed, 0 for the other states.",
            f"# TYPE {MARKER_STATE} gauge",
        ]
        for name, (state, _, _, _) in sorted(markers.items()):
            lines += [f'{MARKER_STATE}{{feature_set="{name}",state="{known}"}} {int(known == state)}' for known in MARKER_STATES]
        gauges = (
            (MARKER_MATERIALIZED, "Unix time the online store of the feature set was materialized, from its marker.", 1),
            (MARKER_ENTITIES, "Entities the marker of the feature set reports.", 2),
            (MARKER_READ, "Unix time of the latest marker read of the feature set.", 3),
        )
        for metric, text, position in gauges:
            lines += [f"# HELP {metric} {text}", f"# TYPE {metric} gauge"]
            for name, found in sorted(markers.items()):
                if found[position] is not None:
                    lines.append(f'{metric}{{feature_set="{name}"}} {found[position]!r}')
        lines += [f"# HELP {REFUSALS} Features refusals of the model API by reason.", f"# TYPE {REFUSALS} counter"]
        lines += [f'{REFUSALS}{{reason="{reason}"}} {value}' for reason, value in sorted(refusals.items())]
        return "\n".join(lines) + "\n"
