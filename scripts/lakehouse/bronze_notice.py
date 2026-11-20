"""The owner notice of a bronze run: the source files carried over and the tables the stage left out, in the shape of ``orchestration.owner_notice``."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

from orchestration import ingestion_schedule, owner_notice

NOTICE_LIST_LIMIT = 1000


def count(number: int, noun: str) -> str:
    """A count with its noun in the right number: ``1 file``, ``2 files``."""
    return f"{number} {noun}{'' if number == 1 else 's'}"


def listed(title: str, items: Sequence[str], shown: int) -> list[str]:
    """A titled list of the first ``shown`` items, one line each. A list that shows fewer items than it has ends with a line that gives, as exact numbers,
    how many it shows and how many it does not."""
    lines = [f"{title}:", *[f"- {owner_notice.clean_line(item)}" for item in items[:shown]]]
    if shown < len(items):
        lines.append(f"Showing {shown} of {len(items)}. Not shown: {len(items) - shown}.")
    return lines


def carried_over_notice(
    bronze_run: str,
    ingestion_run: str,
    counts: Mapping[str, int] | None,
    files: Sequence[str],
    skipped: Sequence[Mapping[str, str]] = (),
    unread: Mapping[str, str] | None = None,
    read: Sequence[str] = (),
    now: datetime | None = None,
) -> tuple[str, str]:
    """The subject and the message of the owner notice of a bronze run, sent for every run that follows a capture: the files the capture captured, failed
    and held (``counts``, None when no run record could be read, otherwise the counts of the records that were read), the files still not captured, the
    tables the stage left out with their reasons, the capture records it read (``read``) and one line for each run record that could not be read
    (``unread``, the run and why). It links the bronze run to the ingestion run only by those records, and says a good month only when every record was
    read.

    The message claims only what it holds. Each list shows at most ``NOTICE_LIST_LIMIT`` names, and the whole message stays within the byte limit of the
    notice form (``owner_notice.BODY_LIMIT``): the names that do not fit are left out, the largest number that fits is shown, and each cut list ends with
    the exact number of names it shows and the number it does not. The numbers are those of the final text. The message points to no place outside itself."""
    unread = unread or {}
    happened, affects, next_steps, details, pieces = [], [], [], [], []
    happened.append(f"Bronze run {bronze_run} completed.\nCapture records read: {', '.join(read) or 'none'}.")
    if counts is None:
        happened.append("The notice does not count the captured, failed or held files.")
    else:
        happened.append(f"Captured files: {counts['captured']}\nFailed files: {counts['failed']}\nHeld files: {counts['held']}")
        if counts["held"]:
            details.append("A held file is one the capture did not try, because its source was paused or needs a key.")
    happened.extend(f"The run record of {name} {why}, so its files are not counted." for name, why in unread.items())
    if unread:
        pieces.append(f"could not read {count(len(unread), 'run record')}")
        affects.append("The counts leave out the files of every run record that could not be read.")
        next_steps.append(
            "Find out why a run record is missing or cannot be read.\nThe run may have stopped before it saved its record, after it captured files."
        )
    if files:
        pieces.insert(0, f"carried over {count(len(files), 'file')}")
        some = f"{'In the run records that could be read, ' if unread else ''}{count(len(files), 'source file')}"
        happened.append(f"{some} {'is' if len(files) == 1 else 'are'} not captured after the retry days.")
        affects.append("The tables built from these files may hold older data.")
        next_steps.append("Read the file list below.\nThen decide if the earlier capture is good enough.\nIf it is, no action is needed.")
        details.append(f"Files carried over: {len(files)}")
    if skipped:
        pieces.insert(1 if files else 0, f"left out {count(len(skipped), 'table')}")
        happened.append(f"The run left out {count(len(skipped), 'table')}.\n{'It has' if len(skipped) == 1 else 'They have'} no source file yet.")
        one = len(skipped) == 1
        affects.append(f"This run left the {'table' if one else 'tables'} out.\nIts published data does not include {'the table' if one else 'them'}.")
        next_steps.append("Check if the skipped table should have a source file.\nIf it should, find out why the ingestion run did not capture one.")
        details.append(f"Tables skipped: {len(skipped)}")
    if counts is not None and not (files or skipped or unread):
        pieces.append(f"captured {count(counts['captured'], 'file')}")
        happened.append("Every file was captured.")
        affects.append("No file is missing and no table was left out.")
        next_steps.append("No action is needed.")
    lists = [
        (title, items)
        for title, items in (("Carried-over files", list(files)), ("Skipped tables", [f"{i['table']} ({i['reason']})" for i in skipped]))
        if items
    ]
    retries = ingestion_schedule.ingestion_runs_of(bronze_run)[1]
    now = now or owner_notice.utc_now()

    def render(total: int) -> tuple[str, str]:
        """The notice that shows ``total`` names in all, the carried-over files first, and no more of a list than its limit."""
        lines, left = list(details), total
        for title, items in lists:
            lines.extend(listed(title, items, shown := min(left, len(items), NOTICE_LIST_LIMIT)))
            left -= shown
        notice = owner_notice.Notice(
            subject=f"Bronze run {bronze_run} {' and '.join(pieces)}",
            happened="\n".join(happened),
            affects="\n".join(affects),
            next_step="\n".join(next_steps),
            where=(f"Bronze run in Airflow: {bronze_run}", f"Capture runs in Airflow: {', '.join((ingestion_run, *retries))}"),
            details=lines,
        )
        return owner_notice.render(notice, now)

    low, high = 0, sum(min(len(items), NOTICE_LIST_LIMIT) for _, items in lists)
    while low < high:
        middle = (low + high + 1) // 2
        low, high = (middle, high) if owner_notice.TRUNCATED not in render(middle)[1] else (low, middle - 1)
    return render(low)
