"""Synthetic faults that the isolation tests install in the startup check's child process, each one a way a dependency can write a marker to a sink.

tests/startup_check_child_faults.py is the child entry that installs one of them.
"""

from __future__ import annotations

import atexit
import faulthandler
import json
import logging
import os
import signal
import sys
import threading
import time
import traceback
import types
import warnings
from pathlib import Path

MARKER = "SYNTHETIC_FAULT_MARKER"
# Synthetic secrets that a faulty child puts in the value fields of a transcript, by the fault that does it.
FORGED_SECRETS = {"forged_digest": "a1b2c3d4e5f6", "forged_ip": "203.0.113.47", "forged_count": "314159"}
FORGED_TRANSCRIPTS = (*FORGED_SECRETS, "contradictory_summary", "empty_summary", "empty_end", "wrong_check")


class LeakyRefusal(RuntimeError):
    """An exception whose text conversion writes the marker to every sink it can reach."""

    def __str__(self) -> str:
        logging.getLogger("dependency").warning("value %s", MARKER)
        sys.stderr.write(MARKER + "\n")
        return "task_credential_source_refused"

    __repr__ = __str__


def refuse(*_arguments: object, **_keywords: object) -> bool:
    raise LeakyRefusal("task_credential_source_refused")


def spill(host: str, port: int, timeout: float) -> None:
    logging.getLogger("dependency").warning("connect %s", MARKER)
    warnings.warn(MARKER, stacklevel=1)
    sys.stderr.write(MARKER + "\n")
    sys.stdout.write(MARKER + "\n")
    os.write(2, (MARKER + "\n").encode())
    faulthandler.dump_traceback()


def interrupt(host: str, port: int, timeout: float) -> None:
    raise KeyboardInterrupt(MARKER)


def show_locals(kind: type[BaseException], error: BaseException, trace: types.TracebackType | None) -> None:
    sys.stderr.write(MARKER + "".join(traceback.TracebackException(kind, error, trace, capture_locals=True).format()))


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
    elif fault == "ignore_term":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        time.sleep(300)
    elif fault in ("garbage_line", "free_text", "unknown_code", "no_end", *FORGED_TRANSCRIPTS):
        lines = {
            "garbage_line": ["{not json " + MARKER],
            "free_text": [json.dumps({"k": "f", "c": "credentials", "o": True, "d": [["p", "native provider " + MARKER]]}), json.dumps({"k": "e"})],
            "unknown_code": [json.dumps({"k": "f", "c": MARKER, "o": True, "d": []}), json.dumps({"k": "e"})],
            "no_end": [json.dumps({"k": "f", "c": "credentials", "o": True, "d": [["p", "native task provider"]]})],
            **forged_transcripts(),
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
                    "values": dict(os.environ),
                    "home": os.environ.get("HOME", ""),
                    "tmp": os.environ.get("TMPDIR", ""),
                }
            )
        )
    else:
        raise ValueError(fault)


def forged_transcripts() -> dict[str, list[str]]:
    """Complete transcripts in the form that an earlier child wrote: findings, one summary and the end. Each one is a lie that the parent must not print."""
    ok = {"k": "f", "c": "credentials", "o": True}
    end = json.dumps({"k": "e"})

    def summary(label: str, checks: int, failed: int) -> str:
        return json.dumps({"k": "s", "j": "silver", "l": label, "n": checks, "x": failed, "t": 0.25})

    return {
        "forged_digest": [json.dumps({**ok, "d": [["f", "plan", FORGED_SECRETS["forged_digest"]]]}), summary("environment", 1, 0), end],
        "forged_ip": [json.dumps({**ok, "d": [["h", "ip", FORGED_SECRETS["forged_ip"], 443]]}), summary("environment", 1, 0), end],
        "forged_count": [json.dumps({**ok, "d": [["n", int(FORGED_SECRETS["forged_count"])]]}), summary("environment", 1, 0), end],
        "contradictory_summary": [json.dumps({**ok, "d": [["p", "native task provider"]]}), summary("environment", 0, 999), end],
        "empty_summary": [summary("environment", 0, 0), end],
        "empty_end": [end],
        "wrong_check": [json.dumps({"k": "f", "c": "input_files", "o": True, "d": [["p", "none named"]]}), summary("files", 1, 0), end],
    }
