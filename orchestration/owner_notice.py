"""One plain-text shape for every owner notice that Python code sends to the alerts topic.

Email by SNS shows plain text: no HTML, no Markdown and a subject of at most 100 characters. A notice holds a subject and four blocks that every notice
has, in this order: what happened, when, what it affects and what to do next. Two blocks follow when the notice has something to put in them: where to
look and the details. A block with no content is left out, never printed empty. The time is the time of the send in the owner's zone (America/Los_Angeles,
named by its abbreviation) with UTC in brackets. A model or an infection type is shown with its name beside its key (``model_name``, ``hai_name``).

``render`` returns the subject and the message. The Grafana alert notices have the same shape in ``monitoring/grafana/provisioning/alerting/templates.yml``.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from orchestration import training_contract

OWNER_ZONE = "America/Los_Angeles"
# SNS allows 100 characters and Grafana cuts a subject at 100 bytes. Ten characters of room keep both from ever acting on a subject.
SUBJECT_LIMIT = 90
ELLIPSIS = "..."


def utc_now() -> datetime:
    """The current time in UTC."""
    return datetime.now(UTC)


def nth_sunday(year: int, month: int, number: int) -> datetime:
    """The first moment (00:00, naive) of the ``number``-th Sunday of a month."""
    first = datetime(year, month, 1)
    return first + timedelta(days=(6 - first.weekday()) % 7 + 7 * (number - 1))


def pacific(moment: datetime) -> tuple[datetime, str]:
    """The moment on the Pacific clock and its zone name. The time zone database decides; a machine without it (a slim image) uses the United States rule:
    daylight time runs from 2:00 on the second Sunday of March to 2:00 on the first Sunday of November."""
    try:
        local = moment.astimezone(ZoneInfo(OWNER_ZONE))
        return local, str(local.tzname())
    except ZoneInfoNotFoundError:
        standard = moment.astimezone(timezone(timedelta(hours=-8)))
        starts = nth_sunday(standard.year, 3, 2) + timedelta(hours=2)
        ends = nth_sunday(standard.year, 11, 1) + timedelta(hours=1)
        if starts <= standard.replace(tzinfo=None) < ends:
            return moment.astimezone(timezone(timedelta(hours=-7))), "PDT"
        return standard, "PST"


def when_text(moment: datetime) -> str:
    """The time of a notice as the owner reads it, for example ``10:13 AM PST, Nov 19 2026 [18:13 UTC]``."""
    local, zone = pacific(moment)
    clock = f"{local.hour % 12 or 12}:{local.minute:02d} {'AM' if local.hour < 12 else 'PM'}"
    utc = moment.astimezone(UTC)
    return f"{clock} {zone}, {local:%b} {local.day} {local.year} [{utc:%H:%M} UTC]"


def model_name(key: str) -> str:
    """A model key with its name beside it (``Elastic Net (m2)``); a key the contract does not name stays as it is."""
    return f"{training_contract.MODEL_LABELS[key]} ({key})" if key in training_contract.MODEL_LABELS else key


def hai_name(key: str) -> str:
    """An infection type key with its name beside it (``CLABSI (HAI_1)``); a key the contract does not name stays as it is."""
    return f"{training_contract.HAI_LABELS[key]} ({key})" if key in training_contract.HAI_LABELS else key


def shorten(text: str, limit: int = SUBJECT_LIMIT) -> str:
    """A subject SNS accepts: printable ASCII on one line. Controls and spaces become one space, a letter outside ASCII becomes ``?``, and a text longer
    than the limit is cut at a word end with ``...``."""
    line = re.sub(r"[^ -~]", "?", re.sub(r"[\x00-\x20\x7f]+", " ", text)).strip()
    if len(line) <= limit:
        return line
    return line[: limit - len(ELLIPSIS)].rsplit(" ", 1)[0].rstrip(" ,:") + ELLIPSIS


@dataclass(frozen=True)
class Notice:
    """The content of one owner notice. ``where`` and ``details`` are lines, and an empty one leaves its block out."""

    subject: str
    happened: str
    affects: str
    next_step: str
    where: Sequence[str] = ()
    details: Sequence[str] = ()


def block(title: str, lines: Sequence[str]) -> str:
    """One block: its title on a line, then each non-empty line."""
    return "\n".join([f"{title}:", *[line.rstrip() for line in lines if line.strip()]])


def render(notice: Notice, now: datetime | None = None) -> tuple[str, str]:
    """The subject and the message of a notice, with the time of the send (``now``, the clock when absent)."""
    blocks = [
        block("What happened", [notice.happened]),
        block("When", [when_text(now or utc_now())]),
        block("What it affects", [notice.affects]),
        block("What to do next", [notice.next_step]),
    ]
    if any(line.strip() for line in notice.where):
        blocks.append(block("Where to look", notice.where))
    if any(line.strip() for line in notice.details):
        blocks.append(block("Details", notice.details))
    return shorten(notice.subject), "\n\n".join(blocks)
