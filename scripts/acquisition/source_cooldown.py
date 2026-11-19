"""The durable per-source cooldown a publisher's HTTP 429 sets, so a later run does not send a request before the publisher's delay ends.

A cooldown is one create-once object ``<root>ingestion_cooldowns/<source_id>/<resume time>.<run_id>.json``. The resume time in the key name (UTC, compact ISO
form) is the whole decision: the source may be asked again at or after the largest resume time listed under its folder, whichever run saved it. Records are
never replaced or removed, so a later record cannot shorten an earlier delay, and a retried save of the same bytes creates nothing new. The body repeats the
facts for audit and holds no header text. Nothing here reads or writes storage or a clock; the capture run does, so every test can inject both.

The publisher's Retry-After header comes in two forms (RFC 9110, section 10.2.3): whole seconds or an HTTP date. A missing or malformed header gets the
stated DEFAULT_SECONDS. A delay above MAX_SECONDS is stored as MAX_SECONDS, which only keeps a stored time sane; a larger delay is not expected.
"""

import re
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from math import ceil

from scripts.acquisition.completion_record import RUN_ID
from scripts.acquisition.s3_store import encoded_json

FOLDER = "ingestion_cooldowns/"
KIND = "source_cooldown"
SCHEMA_VERSION = 1
REASON = "http_429"
DEFAULT_SECONDS = 3600
MAX_SECONDS = 31 * 86400
SECONDS_FORM = re.compile(r"[0-9]{1,10}")
SOURCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,39}")
TIME_FORM = "%Y%m%dT%H%M%SZ"
NAME = re.compile(rf"(?P<when>[0-9]{{8}}T[0-9]{{6}}Z)\.(?P<run>{RUN_ID.pattern})\.json")


class CooldownError(ValueError):
    """A source, run ID or stored key that is no valid cooldown. It is refused, never skipped over, so the source stays blocked."""


def retry_after_seconds(header: str | None, now: datetime) -> tuple[int, str]:
    """The delay in whole seconds a Retry-After header names, with its form: ``seconds``, ``http_date`` or ``default`` (missing or malformed).

    A date in the past means no delay. A date in the future rounds up to a whole second, so the delay is never shortened. NOW must be timezone aware.
    """
    text = (header or "").strip()
    if SECONDS_FORM.fullmatch(text) is not None:
        return min(int(text), MAX_SECONDS), "seconds"
    try:
        when = parsedate_to_datetime(text)
    except (ValueError, TypeError, IndexError, OverflowError):
        return DEFAULT_SECONDS, "default"
    if when.tzinfo is None:
        return DEFAULT_SECONDS, "default"
    return min(max(0, ceil((when - now).total_seconds())), MAX_SECONDS), "http_date"


def prefix(root: str, source: str) -> str:
    """The folder of one source's cooldown records under a capture root."""
    if SOURCE.fullmatch(source) is None:
        raise CooldownError("Source ID cannot join a cooldown key")
    return f"{root}{FOLDER}{source}/"


def key(root: str, source: str, not_before: datetime, run_id: str) -> str:
    """The key of one cooldown record: its resume time sorts in time order, and the run that saved it keeps two runs' records apart."""
    if RUN_ID.fullmatch(run_id) is None:
        raise CooldownError("Run ID cannot join a cooldown key")
    return f"{prefix(root, source)}{not_before.astimezone(UTC).strftime(TIME_FORM)}.{run_id}.json"


def latest(root: str, source: str, keys: Iterable[str]) -> datetime | None:
    """The largest resume time among the listed keys of one source, or None when there are none. A key that is not a cooldown record raises."""
    folder, found = prefix(root, source), []
    for name in keys:
        match = NAME.fullmatch(name[len(folder) :]) if name.startswith(folder) else None
        if match is None:
            raise CooldownError("A stored key under the cooldown folder is no cooldown record")
        try:
            found.append(datetime.strptime(match.group("when"), TIME_FORM).replace(tzinfo=UTC))
        except ValueError:
            raise CooldownError("A cooldown record names no valid resume time") from None
    return max(found, default=None)


def body(source: str, run_id: str, observed: datetime, seconds: int, form: str, not_before: datetime) -> bytes:
    """The stored facts of one cooldown: who, when, the normalized delay and its form. The header text itself is not stored."""
    document = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "source_id": source,
        "reason": REASON,
        "run_id": run_id,
        "observed_at_utc": observed.isoformat(),
        "retry_after_seconds": seconds,
        "retry_after_form": form,
        "not_before_utc": not_before.isoformat(),
    }
    return encoded_json(document)


def resume_time(observed: datetime, seconds: int) -> datetime:
    """The time the source may be asked again: the observation plus the delay."""
    return observed + timedelta(seconds=seconds)
