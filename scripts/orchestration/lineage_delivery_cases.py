"""Cases for the lineage delivery counts that the scheduler's OpenLineage transport sends to the StatsD exporter.

Run from the repository root: ``.venv/bin/python -m scripts.orchestration.lineage_delivery_cases``. The counting module ``services/lineage/delivery_counts.py``
uses only the standard library, so the cases load it by its path and need no OpenLineage client. A loopback UDP socket stands in for the StatsD exporter.
Each send of an event ends in one of four closed results (sent, refused, timed_out, failed), counted without a table, job or run name. A counting problem never
changes what the transport returns or raises. The image cases of ``lineage_http_cases.py`` run the same wiring against the real client.
"""

from __future__ import annotations

import importlib.util
import socket
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "services" / "lineage" / "delivery_counts.py"
MAPPING = ROOT / "monitoring" / "statsd" / "mapping.yml"


def load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("lineage_delivery_counts", MODULE)
    if spec is None or spec.loader is None:
        raise RuntimeError("the counting module is missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HTTPError(Exception):
    """Stands in for requests.HTTPError, which the plain HTTP transport raises for an error status."""


class Timeout(Exception):
    """Stands in for requests.Timeout."""


class ReadTimeout(Timeout):
    """Stands in for requests.ReadTimeout."""


class ConnectTimeout(ConnectionError, Timeout):
    """Stands in for requests.ConnectTimeout, which is both a connection error and a timeout."""


class RetryError(Exception):
    """Stands in for requests.RetryError, which the client raises when the retries of a 5xx answer ran out."""


class ResponseError(Exception):
    """Stands in for urllib3's ResponseError, the cause of that retry error."""


class ReadTimeoutError(Exception):
    """Stands in for urllib3's ReadTimeoutError."""


class ConnectTimeoutError(Exception):
    """Stands in for urllib3's ConnectTimeoutError."""


class NewConnectionError(ConnectTimeoutError):
    """Stands in for urllib3's NewConnectionError, which is a subclass of ConnectTimeoutError although the host refused or was not found."""


class MaxRetryError(Exception):
    """Stands in for urllib3's MaxRetryError, which holds the last reason of the retries."""

    def __init__(self, reason: Exception) -> None:
        super().__init__("Max retries exceeded")
        self.reason = reason


def listener() -> socket.socket:
    found = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    found.bind(("127.0.0.1", 0))
    found.settimeout(1.0)
    return found


def datagrams(found: socket.socket) -> list[str]:
    got: list[str] = []
    found.settimeout(0.2)
    try:
        while True:
            got.append(found.recv(512).decode())
    except OSError:
        return got


def environment(found: socket.socket) -> dict[str, str]:
    return {"AIRFLOW__METRICS__STATSD_HOST": "127.0.0.1", "AIRFLOW__METRICS__STATSD_PORT": str(found.getsockname()[1])}


def an_error_becomes_one_of_four_closed_results(root: Path) -> dict[str, bool]:
    module = load()
    return {
        "no error is a sent event": module.result_of(None) == "sent",
        "an HTTP error status is refused": module.result_of(HTTPError("400")) == "refused",
        "a timeout, a read timeout and a connect timeout are timed out": all(
            module.result_of(error) == "timed_out" for error in (Timeout(), ReadTimeout(), ConnectTimeout(), TimeoutError())
        ),
        "an unreachable host and any other error are failed": module.result_of(ConnectionRefusedError()) == "failed"
        and module.result_of(ValueError("x")) == "failed",
    }


def the_errors_the_pinned_client_raises_are_read_through_their_causes(root: Path) -> dict[str, bool]:
    module = load()
    refused, slow, down = (
        RetryError(MaxRetryError(ResponseError("too many 500 error responses"))),
        ConnectionError(MaxRetryError(ReadTimeoutError("read timed out"))),
        ConnectionError(MaxRetryError(OSError("refused"))),
    )
    unreachable = ConnectionError(MaxRetryError(NewConnectionError("connection refused")))
    connect_timeout = ConnectionError(MaxRetryError(ConnectTimeoutError("connect timed out")))
    caused = ValueError("outer")
    caused.__cause__ = ReadTimeoutError("read timed out")
    return {
        "retries that ran out on a 5xx answer are refused": module.result_of(refused) == "refused",
        "a connection error whose retries ended in a read timeout is timed out": module.result_of(slow) == "timed_out",
        "a connection error whose retries ended in a refused connection is failed": module.result_of(down) == "failed",
        "a refused connection is failed although its class descends from the connect timeout": module.result_of(unreachable) == "failed",
        "a real connect timeout is timed out": module.result_of(connect_timeout) == "timed_out",
        "a cause chain is read too": module.result_of(caused) == "timed_out",
        "a loop in the causes ends": module.result_of(loop()) == "failed",
    }


def loop() -> Exception:
    first, second = ValueError("a"), ValueError("b")
    first.__cause__, second.__cause__ = second, first
    return first


def a_result_is_one_counter_datagram_to_the_exporter(root: Path) -> dict[str, bool]:
    module = load()
    found = listener()
    try:
        env = environment(found)
        for result in ("sent", "refused", "timed_out", "failed"):
            module.count(result, env)
        module.count("sent", env)
        got = datagrams(found)
    finally:
        found.close()
    return {
        "each result is one counter datagram with its name": got[:4]
        == [f"hai.lineage.events.{name}:1|c" for name in ("sent", "refused", "timed_out", "failed")],
        "a repeat is another datagram": got[4:] == ["hai.lineage.events.sent:1|c"],
    }


def anything_unexpected_sends_nothing_and_raises_nothing(root: Path) -> dict[str, bool]:
    module = load()
    found = listener()
    try:
        env = environment(found)
        module.count("surprise", env)
        module.count("sent", {})
        module.count("sent", {"AIRFLOW__METRICS__STATSD_HOST": "127.0.0.1"})
        module.count("sent", {**env, "AIRFLOW__METRICS__STATSD_PORT": "not a port"})
        module.count("sent", {**env, "AIRFLOW__METRICS__STATSD_PORT": "0"})
        module.count("sent", {"AIRFLOW__METRICS__STATSD_HOST": "host that does not exist.invalid", "AIRFLOW__METRICS__STATSD_PORT": "9125"})
        got = datagrams(found)
    finally:
        found.close()
    return {
        "an unknown result, a missing setting or a bad port sends nothing": got == [],
        "none of them raises": True,
    }


def the_transport_counts_every_send_and_changes_nothing_else(root: Path) -> dict[str, bool]:
    source = (root / "services/lineage/http_transport.py").read_text(encoding="utf-8")
    start = source.index("    def emit(self")
    body = source[start:]
    return {
        "the transport imports the counting module": "from . import delivery_counts" in source,
        "a sent event is counted after the plain send returned": "delivery_counts.count(delivery_counts.result_of(None))" in body,
        "a failed send is counted and the same error is raised again": "delivery_counts.count(delivery_counts.result_of(error))" in body and "raise" in body,
    }


def the_exporter_maps_the_events_to_one_counter_by_result(root: Path) -> dict[str, bool]:
    text = MAPPING.read_text(encoding="utf-8")
    return {
        "the mapping has the rule": 'match: "hai.lineage.events.*"' in text and 'name: "hai_lineage_events_total"' in text,
        "the result is the only label": 'result: "$1"' in text.split('match: "hai.lineage.events.*"', 1)[-1].split("- match:", 1)[0],
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "lineage_an_error_becomes_one_of_four_closed_results": an_error_becomes_one_of_four_closed_results,
    "lineage_the_errors_the_pinned_client_raises_are_read_through_their_causes": the_errors_the_pinned_client_raises_are_read_through_their_causes,
    "lineage_a_result_is_one_counter_datagram_to_the_exporter": a_result_is_one_counter_datagram_to_the_exporter,
    "lineage_anything_unexpected_sends_nothing_and_raises_nothing": anything_unexpected_sends_nothing_and_raises_nothing,
    "lineage_the_transport_counts_every_send_and_changes_nothing_else": the_transport_counts_every_send_and_changes_nothing_else,
    "lineage_the_exporter_maps_the_events_to_one_counter_by_result": the_exporter_maps_the_events_to_one_counter_by_result,
}


def main() -> int:
    """Run every case, print one line each and return 1 when any check failed. A case that raises is one failed check named after its exception type."""
    failed = 0
    for name, function in CASES.items():
        try:
            checks = function(ROOT)
        except Exception as error:
            checks = {f"the case ended with {type(error).__name__}": False}
        bad = [check for check, ok in checks.items() if not ok]
        failed += bool(bad)
        sys.stdout.write(f"{'FAIL' if bad else 'PASS'} {name}\n")
        for check in bad:
            sys.stdout.write(f"  failed: {check}\n")
    sys.stdout.write(f"{len(CASES) - failed} of {len(CASES)} cases passed.\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
