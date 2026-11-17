"""OpenLineage HTTP transport that sends eventTime in UTC with a trailing Z, because OpenMetadata 2.0.7 refuses a numeric offset such as +00:00."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import attr
from openlineage.client.transport.http import HttpTransport


def utc_time(value: Any) -> Any:
    """Return a time in UTC with a trailing Z and no fraction (when it has no whole millisecond) or exactly three fraction digits (milliseconds, cut and not
    rounded), or the value itself when it is not an ISO 8601 date-time. OpenMetadata 2.0.7 reads the digits after the point as whole milliseconds
    (JsonUtils.java, ``SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")``), so ``.123456`` is stored as 123456 ms, two minutes late, and ``.1`` as 1 ms;
    three digits are the only count it stores as written. ``orchestration/ecs_lineage.py`` ``utc_time`` is the same code for the direct ECS jobs' events."""
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str) and "t" in value.lower():
        try:
            moment = datetime.fromisoformat(value)
        except ValueError:
            return value
    else:
        return value
    try:
        moment = (moment if moment.tzinfo else moment.replace(tzinfo=UTC)).astimezone(UTC)
    except OverflowError:
        return value
    millis = moment.microsecond // 1000
    fraction = f".{millis:03d}" if millis else ""
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
