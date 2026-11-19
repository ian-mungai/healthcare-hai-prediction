"""The sign-off of the Glue mirror of a bronze run: the recorded outcome, the successful result of the run's matching task and that task's own loader report.

    python -m scripts.lakehouse.bronze_mirror_signoff --run <bronze run> --describe <FILE> --report <FILE> [--bucket NAME]

``--describe`` holds the output of ``aws ecs describe-tasks`` for the latest task of the run and ``--report`` the loader report that the same task wrote
(``data/e2e/bronze_load/report_<time>.json`` in the task's code folder; the owner's profile reads both, nothing here calls ECS). The sign-off passes only
when all three parts hold:

1. ``bronze_mirror.acceptance`` reads ``ok``, ``unchanged`` or ``skipped`` (the numbered attempt records joined to the load receipt),
2. the task is the run's own successful task: exactly one task, STOPPED, every container of the bronze job and its init container at exit 0, the
   bronze job's host task definition, and ``RUN_ID`` equal to this run in every container override that carries it, and
3. the loader report is a readable JSON object whose SHA-256 (of its bytes, as the check phase computes it) is the report digest of the LAST stored
   attempt. A report that is missing, unreadable, matches no stored attempt or matches only an earlier attempt refuses the sign-off.

The second part exists because of a documented limit of the first: a retry whose attempt record cannot be written (the role lost its grant, or the task
stopped before it wrote) leaves the earlier ``ok`` as the last record, and the retry's own check phase fails. The third part closes the case that part two
cannot see: with both mirror grants withdrawn in full after a recorded ``ok``, the retry exits 0 as unavailable and stores nothing, so the old ``ok`` and
the stopped task both pass, but the retry's loader report matches no stored attempt. Pass the task and the report of the LATEST attempt: a successful older
task proves nothing about a later retry. No automated join of a task to its report exists (the owner pairs them by taking both from the same task), and the
report's time and digest are printed so the pair can be checked by eye. Exit 0 only for a passed sign-off. The texts are fixed and echo nothing from the
files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.lakehouse import bronze_mirror, glue_mirror

MAX_BYTES = 1_048_576
PASSING = ("ok", "unchanged", "skipped")


class SignoffError(ValueError):
    """The describe file cannot be read as the output of describe-tasks. The text is fixed."""


def read_describe(path: Path) -> Any:
    """The JSON of a regular file that is not a link and within the size limit."""
    try:
        status = path.lstat()
        if not stat.S_ISREG(status.st_mode) or status.st_size > MAX_BYTES:
            raise SignoffError("the describe file is not a regular file within the size limit")
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        raise SignoffError("the describe file cannot be read") from None
    except ValueError:
        raise SignoffError("the describe file is not JSON") from None


def read_report(path: Path) -> tuple[dict[str, Any], str]:
    """The loader report of the latest task and the SHA-256 of its bytes: a regular file that is not a link, within the size limit, holding a JSON object."""
    try:
        status = path.lstat()
        if not stat.S_ISREG(status.st_mode) or status.st_size > MAX_BYTES:
            raise SignoffError("the loader report cannot be read")
        body = path.read_bytes()
    except OSError:
        raise SignoffError("the loader report cannot be read") from None
    try:
        document = json.loads(body)
    except ValueError:
        document = None
    if not isinstance(document, dict):
        raise SignoffError("the loader report is not a JSON object")
    return document, hashlib.sha256(body).hexdigest()


def report_problems(path: Path | None, found: Sequence[Mapping[str, Any]] | None) -> tuple[list[str], str]:
    """What is wrong with the latest task's loader report as the evidence for the last stored attempt, and the report's time and digest for display.

    ``found`` is the attempt records in number order, or None when they cannot be read. The report must exist, be readable and carry the digest of the
    last attempt: no attempt, an unreadable chain, a digest that no attempt holds and a digest of an earlier attempt each refuse.
    """
    if path is None:
        return ["no loader report was given: the latest task's report is required"], ""
    try:
        document, digest = read_report(path)
    except SignoffError as error:
        return [str(error)], ""
    stamp = document.get("run_at_utc")
    shown = f"{stamp if bronze_mirror.report_time(stamp) else 'no valid time'}, sha256 {digest[:12]}"
    if found is None:
        return ["the loader report cannot be matched: the attempt records cannot be read"], shown
    if not found:
        return ["the loader report matches no stored attempt: the run has no stored attempt record"], shown
    if digest == found[-1]["report_sha256"]:
        return [], shown
    if any(digest == record["report_sha256"] for record in found):
        return ["the loader report is an earlier attempt, not the latest"], shown
    return ["the loader report matches no stored attempt: the latest task's mirror outcome was not recorded"], shown


def task_problems(run: str, described: Any) -> list[str]:
    """What is wrong with the described task as the successful task of ``run``. Empty when it is that task."""
    from scripts.lakehouse import layer_acceptance

    problems = layer_acceptance.task_problems("bronze", described)
    tasks = described.get("tasks") if isinstance(described, dict) else None
    if not isinstance(tasks, list) or len(tasks) != 1 or not isinstance(tasks[0], dict):
        return problems
    task = tasks[0]
    if not layer_acceptance.definition_pattern("bronze").fullmatch(str(task.get("taskDefinitionArn"))):
        problems.append("the task is not the bronze job's task definition")
    values = []
    for item in layer_acceptance.mapping(task.get("overrides")).get("containerOverrides") or []:
        environment = item.get("environment") if isinstance(item, dict) else None
        values += [entry.get("value") for entry in environment or [] if isinstance(entry, dict) and entry.get("name") == "RUN_ID"]
    if not values or any(value != run for value in values):
        problems.append("the task does not carry this run's ID")
    return problems


def main(argv: Sequence[str] | None = None, client: Any = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run", required=True)
    parser.add_argument("--describe", required=True, type=Path)
    parser.add_argument("--report", type=Path, help="the loader report of the latest task (required: no sign-off passes without it)")
    parser.add_argument("--bucket", help="the data bucket (default: DATA_BUCKET_NAME)")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if not bronze_mirror.RUN_ID.fullmatch(args.run):
        parser.error("the run ID must look like bronze_0a1b2c3d")
    bucket = args.bucket or os.environ.get("DATA_BUCKET_NAME", "")
    if not bucket:
        parser.error("the sign-off needs the data bucket: pass --bucket or set DATA_BUCKET_NAME")
    if client is None:
        client = glue_mirror.s3_client()
    found: list[dict[str, Any]] | None = None
    try:
        verdict = bronze_mirror.acceptance(client, bucket, args.run)
        sys.stdout.write(bronze_mirror.render(verdict))
        state, found = verdict["state"], verdict["attempts"]
    except bronze_mirror.AttemptError as error:
        state = "unavailable"
        sys.stdout.write(f"acceptance: unavailable: {error}\n")
    try:
        problems = task_problems(args.run, read_describe(args.describe))
    except SignoffError as error:
        problems = [str(error)]
    sys.stdout.write("task: ok\n" if not problems else f"task: {'; '.join(problems)}\n")
    evidence, shown = report_problems(args.report, found)
    sys.stdout.write(f"report: ok ({shown})\n" if not evidence else f"report: {'; '.join(evidence)}{f' ({shown})' if shown else ''}\n")
    passed = state in PASSING and not problems and not evidence
    sys.stdout.write("signoff: ok\n" if passed else "signoff: refused\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
