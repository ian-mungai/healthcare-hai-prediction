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
    bronze_run: str, ingestion_run: str, files: Sequence[str], skipped: Sequence[Mapping[str, str]] = (), now: datetime | None = None
) -> tuple[str, str]:
    """The subject and the message of the owner notice of a bronze run: every carried-over file by name and every table the stage left out, with its reason.

    A notice needs at least one file or one table: a run with neither sends none."""
    if not files and not skipped:
        raise ValueError("a bronze notice needs a carried over file or a skipped table")
    happened, affects, next_steps, details, pieces = [], [], [], [], []
    record = f"lakehouse/bronze_runs/{bronze_run}/carried_over.json"
    if files:
        pieces.append(f"carried over {count(len(files), 'file')}")
        happened.append(f"Bronze run {bronze_run} loaded the earlier capture of {count(len(files), 'source file')}.\nNo capture of a newer one exists.")
        affects.append("The tables built from these files may hold older data.")
        next_steps.append("Read the file list in the run record.\nThen decide if the earlier capture is good enough.\nIf it is, no action is needed.")
        details.append(f"Files carried over: {len(files)}")
    if skipped:
        pieces.append(f"left out {count(len(skipped), 'table')}")
        subject_of = "The run" if files else f"Bronze run {bronze_run}"
        happened.append(f"{subject_of} left out {count(len(skipped), 'table')}.\n{'It has' if len(skipped) == 1 else 'They have'} no source file yet.")
        one = len(skipped) == 1
        affects.append(f"This run left the {'table' if one else 'tables'} out.\nIts published data does not include {'the table' if one else 'them'}.")
        next_steps.append("Check if the skipped table should have a source file.\nIf it should, find out why the ingestion run did not capture one.")
        details.append(f"Tables skipped: {len(skipped)}")
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
