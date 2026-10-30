"""The model API's Prometheus metrics, kept in this process and rendered in the text exposition format.

The two families are the contract with the monitoring stack (``data_contracts/serving/metrics.json``): a request counter with the labels ``route``, ``method``
and ``status`` and a duration histogram with the label ``route``. Every label value comes from a closed set, so the number of series stays bounded however many
paths a scanner tries. No key, hospital or request id ever becomes a label. The state is a few small tables behind one lock.
"""

from __future__ import annotations

import threading

ROUTES = ("/v1/predict", "/v1/health", "/metrics")
METHODS = ("GET", "POST", "HEAD", "OPTIONS")
STATUSES = (200, 400, 401, 404, 405, 408, 413, 422, 429, 500, 503)
BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
OTHER = "other"
REQUESTS = "hai_api_requests_total"
DURATION = "hai_api_request_duration_seconds"


def route_label(path: str) -> str:
    return path if path in ROUTES else OTHER


def method_label(method: str) -> str:
    return method if method in METHODS else OTHER


def status_label(status: int) -> str:
    return str(status) if status in STATUSES else OTHER


def bound_text(bound: float) -> str:
    return repr(bound) if bound != int(bound) else str(int(bound))


class Metrics:
    def __init__(self) -> None:
        self._requests: dict[tuple[str, str, str], int] = {}
        self._durations: dict[str, tuple[list[int], float, int]] = {}
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

    def render(self) -> str:
        with self._lock:
            requests, durations = dict(self._requests), {route: (list(counts), total, count) for route, (counts, total, count) in self._durations.items()}
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
        return "\n".join(lines) + "\n"
