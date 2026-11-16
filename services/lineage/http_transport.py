"""OpenLineage HTTP transport that sends eventTime in UTC with a trailing Z, because OpenMetadata 2.0.7 refuses a numeric offset such as +00:00."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import attr
from openlineage.client.transport.http import HttpTransport

FRACTION = re.compile(r":\d{2}[.,](\d+)")


def utc_time(value: Any) -> Any:
    """Return a time in UTC with a trailing Z, keeping the fractional digits (at most six), or the value itself when it is not an ISO 8601 date-time."""
    if isinstance(value, datetime):
        moment, digits = value, 6 if value.microsecond else 0
    elif isinstance(value, str) and "t" in value.lower() and not value.endswith("Z"):
        try:
            moment = datetime.fromisoformat(value)
        except ValueError:
            return value
        match = FRACTION.search(value)
        digits = min(len(match.group(1)), 6) if match else 0
    else:
        return value
    try:
        moment = (moment if moment.tzinfo else moment.replace(tzinfo=UTC)).astimezone(UTC)
    except OverflowError:
        return value
    fraction = f".{moment.microsecond:06d}"[: digits + 1] if digits else ""
    return moment.isoformat(timespec="seconds").removesuffix("+00:00") + fraction + "Z"


def utc_event(event: Any) -> Any:
    """Return a copy of the event with only eventTime rewritten, or the event itself when it has no time to rewrite or the copy cannot be made."""
    if not attr.has(type(event)) or not hasattr(event, "eventTime"):
        return event
    changed = utc_time(event.eventTime)
    if changed is event.eventTime or changed == event.eventTime:
        return event
    try:
        return attr.evolve(event, eventTime=changed)
    except (TypeError, ValueError, AttributeError):
        return event


class UtcHttpTransport(HttpTransport):
    """The client's HTTP transport (same settings, credential, timeout, retries and errors) with the event time written in the UTC Z form."""

    kind = "hai-lineage-http"

    def emit(self, event: Any) -> Any:
        """Send the event with its time rewritten; every send failure is the HTTP transport's own, which the provider logs without failing the task."""
        return super().emit(utc_event(event))
