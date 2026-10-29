"""Plant one defect at a time in ``aws_tasks`` and require the cases to catch each; run: ``.venv/bin/python -m scripts.infrastructure.aws_tasks_plants``.

Each plant replaces one piece of the module's source in memory (the file stays unchanged) and reruns every case. A plant is caught when a check
fails or a case raises. The run exits 1 when any plant survives or its text is not found.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
import tempfile
from pathlib import Path

MODULE = "scripts.infrastructure.aws_tasks"
SOURCE = Path(__file__).with_name("aws_tasks.py")
PLANTS: dict[str, tuple[str, str]] = {
    "C2 stops tasks of every family": ("if family is not None and family.startswith(self.prefix)", "if family is not None"),
    "C2 accepts the prefix of another project": ("family.startswith(self.prefix)]", 'family.startswith("other")]'),
    "T14 lists only the desired status RUNNING": ('DESIRED_STATUSES = ("RUNNING", "STOPPED")', 'DESIRED_STATUSES = ("RUNNING",)'),
    "T14 lists only the desired status STOPPED": ('DESIRED_STATUSES = ("RUNNING", "STOPPED")', 'DESIRED_STATUSES = ("STOPPED",)'),
    "T14 reads only the first page of the second status": (
        "            arns |= dict.fromkeys(self.page_status(desired))",
        "            arns |= dict.fromkeys(self.page_status(desired)[:2])",
    ),
    "T15 takes the family from the group": ("else None\n            ours", "else grouped\n            ours"),
    "T15 does not compare group and definition": ("grouped != defined)", "False)"),
    "T15 stops a mismatched task": (
        "                if task.mismatch:\n                    problems.append",
        "                if False:\n                    problems.append",
    ),
    "T15 stops a mismatched task that the owner confirmed": (
        "                if task.mismatch:\n",
        "                if task.mismatch and task.suffix not in confirmed:\n",
    ),
    "T15 prints the group text": ("shown = family if FAMILY_FORM.fullmatch(family) else UNNAMED", "shown = family"),
    "T16 does not describe a task whose stop call failed": (
        "                    failed[task] = str(error)\n                sent.append(task)",
        "                    failed[task] = str(error)\n                    continue\n                sent.append(task)",
    ),
    "C3 reads only the first page": ('            token = page.get("nextToken")\n', "            token = None\n"),
    "C3 lets the CLI join the pages and skips the token": ('"--no-paginate"', '"--cli-binary-format"'),
    "C3 follows a repeated token": ("if token in seen", "if False"),
    "C3 describes more than 100 tasks at once": ("DESCRIBE_LIMIT = 100", "DESCRIBE_LIMIT = 1000"),
    "C3 fails on a task gone before description": ('failure.get("reason") != MISSING', "True"),
    "C4 does not wait for STOPPED": ("lambda: not self.not_stopped(sent)", "lambda: True"),
    "C4 treats a running task as stopped": ("states.get(task.arn) != STOPPED", "states.get(task.arn) is None"),
    "C4 treats a vanished task as stopped": ("states.get(task.arn) != STOPPED", "states.get(task.arn, STOPPED) != STOPPED"),
    "C5 stops a writer without confirmation": ("task.writer and task.suffix not in confirmed", "False"),
    "C5 lets one flag cover every writer": ("task.writer and task.suffix not in confirmed", "task.writer and not confirmed"),
    "C5 leaves no reason on the stop": (', "--reason", reason]', "]"),
    "C5 leaves no trace of the owner's confirmation": ("WRITER_STOP_REASON if task.writer else STOP_REASON", "STOP_REASON"),
    "T10 stops at the first failed stop call": (
        "                    failed[task] = str(error)\n                sent.append(task)",
        "                    failed[task] = str(error)\n                    break\n                sent.append(task)",
    ),
    "T7 ends the loop at a refused writer": ("                    continue\n                reason =", "                    break\n                reason ="),
    "C6 prints the task ARN": ('f"{task.family} {task.suffix}" + (" (writer)"', 'f"{task.family} {task.suffix} {task.arn}" + (" (writer)"'),
    "C6 prints the full task ID": ('return self.arn.rsplit("/", 1)[-1][-SUFFIX_LENGTH:]', 'return self.arn.rsplit("/", 1)[-1]'),
    "C6 prints the ARN of a task not stopped": ('+ "not STOPPED" for task in left', '+ f"{task.arn} not STOPPED" for task in left'),
    "deadline gives the whole command no bound": ("self.session.deadline = begin + CLEANUP_SECONDS", "pass"),
    "deadline lets the stop grace run 15 minutes": ("STOP_GRACE_SECONDS = 90", "STOP_GRACE_SECONDS = 900"),
    "deadline does not cut the wait to the time left": ("self.grace(), STOP_POLL_SECONDS", "STOP_GRACE_SECONDS, STOP_POLL_SECONDS"),
    "T18 starts the deadline after the setup": (
        "begin = self.session.clock.now() if self.started is None else self.started",
        "begin = self.session.clock.now()",
    ),
    "T18 reads the start time after the setup": ("    started = clock.now()\n    try:\n", "    started = None\n    try:\n"),
    "T18 gives the session another clock than the deadline": ("clock.now()), clock)", "clock.now()))"),
    "T20 gives the lookups no deadline": ("load_names(settings, lambda: started + CLEANUP_SECONDS - clock.now())", "load_names(settings)"),
    "T20 gives each lookup the whole deadline": ("lambda: started + CLEANUP_SECONDS - clock.now()", "lambda: CLEANUP_SECONDS"),
    "T19 returns at once on an empty cluster": (
        '                self.session.step("tasks", "no job task is running")\n            sent: list[JobTask] = []',
        '                self.session.step("tasks", "no job task is running")\n                return\n            sent: list[JobTask] = []',
    ),
    "T19 does not check the confirmations": ("for value in sorted(confirmed - {task.suffix", "for value in sorted(set() - {task.suffix"),
    "deadline is not applied to the listing": (
        "with self.bounded():\n            tasks = self.discover()\n        for task",
        "with contextlib.nullcontext():\n            tasks = self.discover()\n        for task",
    ),
    "deadline is not applied to the stop": (
        "        problems: list[str] = []\n        with self.bounded():",
        "        problems: list[str] = []\n        with contextlib.nullcontext():",
    ),
}


def caught(source: str) -> bool:
    """True when the cases detect the planted source."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "aws_tasks_planted.py"
        path.write_text(source, encoding="utf-8")
        spec = importlib.util.spec_from_file_location(MODULE, path)
        if spec is None or spec.loader is None:
            raise ImportError(MODULE)
        planted = importlib.util.module_from_spec(spec)
        sys.modules[MODULE] = planted
        spec.loader.exec_module(planted)
    cases = importlib.import_module("scripts.infrastructure.aws_tasks_cases")
    importlib.reload(cases)
    for function in cases.CASES.values():
        try:
            if not all(function().values()):
                return True
        except cases.CASE_ERRORS:
            return True
    return False


def main() -> int:
    """Run every plant and return 1 when any survives."""
    original = SOURCE.read_text(encoding="utf-8")
    survivors = 0
    try:
        for name, (old, new) in PLANTS.items():
            if old not in original:
                sys.stdout.write(f"MISSING {name}\n")
                survivors += 1
                continue
            result = caught(original.replace(old, new, 1))
            sys.stdout.write(f"{'CAUGHT' if result else 'SURVIVED'} {name}\n")
            survivors += not result
    finally:
        sys.modules.pop(MODULE, None)
    sys.stdout.write(f"{len(PLANTS) - survivors} of {len(PLANTS)} plants caught\n")
    return 1 if survivors else 0


if __name__ == "__main__":
    raise SystemExit(main())
