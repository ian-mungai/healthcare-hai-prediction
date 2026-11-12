"""The parent process of the boundary tests: it runs one real startup check against the real or the fault child, with a diagnostic hook that captures locals.

``python -m tests.startup_check_parent_probe <scenario>`` runs in a task-like environment that the test sets. A marker in a path stands for any value that a
caller's diagnostic hook must never see. The hook prints every frame's locals of an uncaught error, as a hook that reports crashes would.
"""

from __future__ import annotations

import glob
import os
import shutil
import sys
import types
from pathlib import Path

from scripts.infrastructure import startup_check

MARKER = "SYNTHETIC_PARENT_MARKER"
RESULT_NAME = "results.jsonl"
SCENARIOS = ("expected", "internal", "timeout", "fifo", "symlink")


def show_locals(kind: type[BaseException], error: BaseException, trace: types.TracebackType | None) -> None:
    """Print the error and the locals of every frame under ``scripts``. The frames of this probe stand for the caller, which holds its own values."""
    sys.stdout.write(f"HOOK {kind.__name__}: {error}\n")
    while trace is not None:
        frame = trace.tb_frame
        if os.sep + "scripts" + os.sep in frame.f_code.co_filename:
            sys.stdout.write(f"HOOK frame {frame.f_code.co_name} locals {frame.f_locals!r}\n")
        trace = trace.tb_next


def replace_result_when_opened(replacement: str) -> None:
    """Replace the result file with a FIFO or a symlink at the moment the parent opens it, which is the latest point of a check-then-use race."""
    folders = Path(os.environ["TMPDIR"])
    done: list[bool] = []

    def hook(event: str, arguments: tuple) -> None:
        if event != "open" or done or not str(arguments[0]).endswith(RESULT_NAME):
            return
        for folder in glob.glob(str(folders / "startup_check_*")):
            target = Path(folder) / RESULT_NAME
            if not target.exists() or target.is_symlink():
                continue
            done.append(True)
            if replacement == "fifo":
                target.unlink()
                os.mkfifo(target)
            else:
                shutil.move(target, Path(folder) / "elsewhere.jsonl")
                target.symlink_to(Path(folder) / "elsewhere.jsonl")

    sys.addaudithook(hook)


def main(scenario: str) -> None:
    sys.excepthook = show_locals
    queue = Path(f"{MARKER}_queue.json")
    if scenario == "expected":
        startup_check.check_files("publisher", [startup_check.Need("hcai_util_workbook", "code", f"../{MARKER}_missing.json")])
    elif scenario == "internal":
        startup_check.check_queue_files("publisher", queue, controls=None, keys=False)
    elif scenario == "timeout":
        startup_check.CHILD_MODULE = "tests.startup_check_child_faults"
        startup_check.CHILD_ENV_EXTRA = ("STARTUP_CHECK_TEST_FAULT",)
        startup_check.CHILD_SECONDS = 2.0
        startup_check.check_queue_files("publisher", queue, controls=None, keys=False)
    elif scenario in ("fifo", "symlink"):
        replace_result_when_opened(scenario)
        startup_check.check_files("publisher", [startup_check.Need("hcai_util_workbook", "code", "tests/startup_check_parent_probe.py")])
    else:
        raise ValueError(scenario)


if __name__ == "__main__":
    main(sys.argv[1])
