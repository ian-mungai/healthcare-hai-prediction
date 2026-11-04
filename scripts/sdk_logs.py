"""The one protection every secret read with the AWS SDK uses: no DEBUG log record while any read runs.

The SDK logs the body of each response at DEBUG, and the body of a Secrets Manager or Parameter Store read holds the decrypted value. A caller that reads a
secret value wraps the call in ``quiet_sdk_logs``. The switch is global to the process, so a record from another thread during a read is lost too.

Reads may overlap, in threads or nested. One lock and one count guard the switch: the first reader to enter saves the current threshold and raises it, and the
last reader to leave restores the saved one. A reader that leaves early therefore never switches logging back on under a reader that is still inside.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager


class _Switch:
    """The count of readers inside the guard and the threshold that the first of them found."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.inside = 0
        self.saved = logging.NOTSET


_SWITCH = _Switch()


@contextmanager
def quiet_sdk_logs() -> Iterator[None]:
    """Switch off DEBUG logging until the last reader inside this guard has left, then restore the threshold the first reader found."""
    with _SWITCH.lock:
        if _SWITCH.inside == 0:
            _SWITCH.saved = logging.root.manager.disable
            logging.disable(max(_SWITCH.saved, logging.DEBUG))
        _SWITCH.inside += 1
    try:
        yield
    finally:
        with _SWITCH.lock:
            _SWITCH.inside -= 1
            if _SWITCH.inside == 0:
                logging.disable(_SWITCH.saved)
