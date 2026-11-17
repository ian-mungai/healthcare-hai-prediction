"""Count each OpenLineage send of the scheduler's HTTP transport as one StatsD counter, by one of four closed results.

The scheduler already sends its own metrics by UDP to the StatsD exporter (``AIRFLOW__METRICS__STATSD_HOST`` and ``AIRFLOW__METRICS__STATSD_PORT``, set only
while monitoring is on). A send ends as ``sent``, ``refused`` (the route answered with an error status), ``timed_out`` or ``failed`` (the route was not reached
or anything else went wrong). No table, job or run name is sent. Counting never raises and never waits for an answer, so it cannot change what a send returns
or raises. Only the standard library is used.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping

RESULTS = ("sent", "refused", "timed_out", "failed")
REFUSED = frozenset({"RetryError", "ResponseError"})
TIMED_OUT = frozenset({"Timeout", "ReadTimeout", "ConnectTimeout", "ReadTimeoutError", "ConnectTimeoutError", "TimeoutError", "timeout"})
MAX_DEPTH = 6
HOST_VARIABLE = "AIRFLOW__METRICS__STATSD_HOST"
PORT_VARIABLE = "AIRFLOW__METRICS__STATSD_PORT"


def chain_names(error: BaseException) -> set[str]:
    """The class names of the error and of every error it names as its cause, its context, its reason or an argument, to a fixed depth, so a loop ends."""
    names: set[str] = set()
    pending: list[tuple[BaseException, int]] = [(error, 0)]
    while pending:
        item, depth = pending.pop()
        names.update(kind.__name__ for kind in type(item).__mro__)
        if depth >= MAX_DEPTH:
            continue
        inner = [item.__cause__, item.__context__, getattr(item, "reason", None), *item.args]
        pending.extend((found, depth + 1) for found in inner if isinstance(found, BaseException))
    return names


def result_of(error: BaseException | None) -> str:
    """The closed result of one send: no error is ``sent``. The error is read by the class names of its chain, so the module needs no HTTP library.

    The plain client raises an HTTP error for a 4xx answer, a retry error when the retries of a 5xx answer ran out, and a connection error that holds the
    last reason of its retries. Only the raised error itself counts as an HTTP error, because the base class of every urllib3 error is also named HTTPError.
    A refused or unknown connection is failed although its urllib3 class descends from the connect timeout.
    """
    if error is None:
        return "sent"
    names = chain_names(error)
    if "HTTPError" in {kind.__name__ for kind in type(error).__mro__} or names & REFUSED:
        return "refused"
    if "NewConnectionError" not in names and names & TIMED_OUT:
        return "timed_out"
    return "failed"


def count(result: str, environment: Mapping[str, str] | None = None) -> None:
    """Send one counter datagram for the result to the exporter named by the environment; anything else is ignored."""
    try:
        env = os.environ if environment is None else environment
        host, port = env.get(HOST_VARIABLE, ""), env.get(PORT_VARIABLE, "")
        if result not in RESULTS or not host or not port.isdecimal() or not 0 < int(port) < 65536:
            return
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.settimeout(0.05)
            sender.sendto(f"hai.lineage.events.{result}:1|c".encode(), (host, int(port)))
    except Exception:
        return
