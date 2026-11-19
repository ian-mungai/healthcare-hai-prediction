"""The sign-off of the Glue mirror of a bronze run: the recorded outcome and the successful result of the run's matching task.

    python -m scripts.lakehouse.bronze_mirror_signoff --run <bronze run> --describe <FILE> [--bucket NAME]

``FILE`` holds the output of ``aws ecs describe-tasks`` for the latest task of the run (the owner's profile reads it; nothing here calls ECS). The
sign-off passes only when both parts hold:

1. ``bronze_mirror.acceptance`` reads ``ok``, ``unchanged`` or ``skipped`` (the numbered attempt records joined to the load receipt), and
2. the task is the run's own successful task: exactly one task, STOPPED, every container of the bronze job and its init container at exit 0, the
   bronze job's host task definition, and ``RUN_ID`` equal to this run in every container override that carries it.

The second part exists because of a documented limit of the first: a retry whose attempt record cannot be written (the role lost its grant, or the
task stopped before it wrote) leaves the earlier ``ok`` as the last record, and the retry's own check phase fails. Pass the task of the LATEST
attempt: a successful older task proves nothing about a later retry, and no record links a task to an attempt number. Exit 0 only for a passed
sign-off. The texts are fixed and echo nothing from the file."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from collections.abc import Sequence
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
    parser.add_argument("--bucket", help="the data bucket (default: DATA_BUCKET_NAME)")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if not bronze_mirror.RUN_ID.fullmatch(args.run):
        parser.error("the run ID must look like bronze_0a1b2c3d")
    bucket = args.bucket or os.environ.get("DATA_BUCKET_NAME", "")
    if not bucket:
        parser.error("the sign-off needs the data bucket: pass --bucket or set DATA_BUCKET_NAME")
    if client is None:
        client = glue_mirror.s3_client()
    try:
        verdict = bronze_mirror.acceptance(client, bucket, args.run)
        sys.stdout.write(bronze_mirror.render(verdict))
        state = verdict["state"]
    except bronze_mirror.AttemptError as error:
        state = "unavailable"
        sys.stdout.write(f"acceptance: unavailable: {error}\n")
    try:
        problems = task_problems(args.run, read_describe(args.describe))
    except SignoffError as error:
        problems = [str(error)]
    sys.stdout.write("task: ok\n" if not problems else f"task: {'; '.join(problems)}\n")
    passed = state in PASSING and not problems
    sys.stdout.write("signoff: ok\n" if passed else "signoff: refused\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
