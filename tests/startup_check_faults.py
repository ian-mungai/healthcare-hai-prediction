"""Synthetic faults that the isolation tests install in the startup check's child process, each one a way a dependency can write a marker to a sink."""

from __future__ import annotations

import atexit
import faulthandler
import json
import logging
import os
import sys
import threading
import time
import traceback
import types
import warnings
from pathlib import Path

MARKER = "SYNTHETIC_FAULT_MARKER"


class LeakyRefusal(RuntimeError):
    """An exception whose text conversion writes the marker to every sink it can reach."""

    def __str__(self) -> str:
        logging.getLogger("dependency").warning("value %s", MARKER)
        sys.stderr.write(MARKER + "\n")
        return "task_credential_source_refused"

    __repr__ = __str__


def refuse(*_arguments: object, **_keywords: object) -> bool:
    raise LeakyRefusal("task_credential_source_refused")


def spill(*_arguments: object) -> None:
    logging.getLogger("dependency").warning("connect %s", MARKER)
    warnings.warn(MARKER, stacklevel=1)
    sys.stderr.write(MARKER + "\n")
    sys.stdout.write(MARKER + "\n")
    os.write(2, (MARKER + "\n").encode())
    faulthandler.dump_traceback()


def interrupt(*_arguments: object) -> None:
    raise KeyboardInterrupt(MARKER)


def show_locals(kind: type[BaseException], error: BaseException, trace: types.TracebackType | None) -> None:
    sys.stderr.write(MARKER + "".join(traceback.format_exception(kind, error, trace, capture_locals=True)))


def show_thread(arguments: threading.ExceptHookArgs) -> None:
    sys.stderr.write(f"{MARKER} {arguments.exc_value!r}\n")
    os.write(2, (MARKER + "\n").encode())


def install(fault: str, folder: Path) -> None:
    """Make every connect succeed, then add the fault. The fault names the sink."""
    from scripts.infrastructure import startup_check
    from scripts.lakehouse import ecs_engine, pipeline_build

    startup_check.open_connection = lambda *_arguments: None
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    warnings.simplefilter("always")
    if fault == "log":
        logging.getLogger("dependency").warning(MARKER)
        startup_check.open_connection = spill
    elif fault == "spill":
        startup_check.open_connection = spill
    elif fault == "faulthandler":
        faulthandler.enable()
        startup_check.open_connection = lambda *_arguments: faulthandler.dump_traceback(all_threads=True)
    elif fault == "excepthook":
        sys.excepthook = show_locals
        threading.excepthook = show_thread
        startup_check.open_connection = interrupt
    elif fault == "exception_str":
        ecs_engine.validate_task_credentials = refuse
        pipeline_build.validate_task_credentials = refuse
    elif fault == "exit_write":
        atexit.register(lambda: os.write(1, (MARKER + "\n").encode()))
        atexit.register(lambda: os.write(2, (MARKER + "\n").encode()))
    elif fault == "file_log":
        logging.getLogger("dependency").addHandler(logging.FileHandler("dependency.log"))
        logging.getLogger("dependency").warning(MARKER)
    elif fault == "crash":
        os._exit(3)
    elif fault == "hang":
        time.sleep(300)
    elif fault in ("garbage_line", "free_text", "unknown_code", "no_end"):
        lines = {
            "garbage_line": ["{not json " + MARKER],
            "free_text": [json.dumps({"k": "f", "c": "credentials", "o": True, "d": [["p", "native provider " + MARKER]]}), json.dumps({"k": "e"})],
            "unknown_code": [json.dumps({"k": "f", "c": MARKER, "o": True, "d": []}), json.dumps({"k": "e"})],
            "no_end": [json.dumps({"k": "f", "c": "credentials", "o": True, "d": [["p", "native task provider"]]})],
        }[fault]
        (folder / "results.jsonl").write_text("\n".join(lines) + "\n")
        os._exit(0)
    elif fault == "environment_probe":
        names = sorted(os.environ)
        Path(os.environ["STARTUP_CHECK_TEST_PROBE"]).write_text(
            json.dumps(
                {
                    "names": names,
                    "cwd": os.getcwd(),
                    "safe_path": sys.flags.safe_path,
                    "no_user_site": sys.flags.no_user_site,
                    "dont_write_bytecode": sys.flags.dont_write_bytecode,
                    "home": os.environ.get("HOME", ""),
                    "tmp": os.environ.get("TMPDIR", ""),
                }
            )
        )
    else:
        raise ValueError(fault)
