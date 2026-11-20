"""The owner notice of a bronze run: the source files carried over and the tables the stage left out, in the shape of ``orchestration.owner_notice``."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

from orchestration import owner_notice

NOTICE_LIST_LIMIT = 1000


def count(number: int, noun: str) -> str:
    """A count with its noun in the right number: ``1 file``, ``2 files``."""
    return f"{number} {noun}{'' if number == 1 else 's'}"


def listed(title: str, items: Sequence[str]) -> list[str]:
    """A titled list of at most ``NOTICE_LIST_LIMIT`` lines, with a last line that says how many more the run record holds."""
    lines = [f"{title}:", *[f"- {owner_notice.clean_line(item)}" for item in items[:NOTICE_LIST_LIMIT]]]
    if len(items) > NOTICE_LIST_LIMIT:
        lines.append(f"and {len(items) - NOTICE_LIST_LIMIT} more (the run record lists them all)")
    return lines


def carried_over_notice(
    bronze_run: str,
    ingestion_run: str,
    counts: Mapping[str, int] | None,
    files: Sequence[str],
    skipped: Sequence[Mapping[str, str]] = (),
    unread: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> tuple[str, str]:
    """The subject and the message of the owner notice of a bronze run, sent for every run that follows a capture: the files the capture captured, failed
    and held (``counts``, None when the original run record could not be read), every file still not captured by name, every table the stage left out with
    its reason and one line for each run record that could not be read (``unread``, the run and why). A good month says so only when every record was read."""
    unread = unread or {}
    happened, affects, next_steps, details, pieces = [], [], [], [], []
    record = f"lakehouse/bronze_runs/{bronze_run}/carried_over.json"
    happened.append(f"Bronze run {bronze_run} loaded the data of ingestion run {ingestion_run}.")
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
        next_steps.append("Read the file list in the run record.\nThen decide if the earlier capture is good enough.\nIf it is, no action is needed.")
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
    if files:
        details.extend(listed("Carried-over files", list(files)))
    if skipped:
        details.extend(listed("Skipped tables", [f"{item['table']} ({item['reason']})" for item in skipped]))
    notice = owner_notice.Notice(
        subject=f"Bronze run {bronze_run} {' and '.join(pieces)}",
        happened="\n".join(happened),
        affects="\n".join(affects),
        next_step="\n".join(next_steps),
        where=(f"Bronze run: {bronze_run}", f"Ingestion run: {ingestion_run}", f"Run record: {record} (in the project data bucket)"),
        details=details,
    )
    return owner_notice.render(notice, now)
