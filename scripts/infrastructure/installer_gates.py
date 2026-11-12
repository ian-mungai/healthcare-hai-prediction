"""Helpers for the cases of the two code installers (host ``code_fetch.py`` and the image's ``fetch_code.py``) that run two fetches at once.

A case starts each fetch in a named thread and pauses or interrupts one of them right after it unpacks its first archive member, which is the moment a
second fetch can disturb it. Every wait has a deadline and every thread is joined with a deadline, so a case that hangs fails visibly instead of waiting.
"""

from __future__ import annotations

import tarfile
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any
from unittest import mock

# One barrier wait. A fetch that holds the installer lock waits this long for a second fetch that cannot enter, then goes on.
GATE_SECONDS = 2.0
# The longest a case waits for its threads to finish.
JOIN_SECONDS = 30.0
WORKER_ERRORS = (OSError, ValueError, LookupError, TypeError, KeyboardInterrupt)

# The method of ``tarfile.TarFile`` that unpacks one member for ``extractall``.
EXTRACT_ONE = "_extract_one"


class Worker(threading.Thread):
    """A named thread that keeps what its function returned, or the error that ended it."""

    def __init__(self, name: str, function: Callable[[], Any]) -> None:
        super().__init__(name=name, daemon=True)
        self.function = function
        self.result: Any = None
        self.error: BaseException | None = None

    def run(self) -> None:
        try:
            self.result = self.function()
        except WORKER_ERRORS as error:
            self.error = error


def reached(event: threading.Event, what: str) -> None:
    """Wait for ``event`` up to the barrier deadline and raise an AssertionError that names ``what`` when it does not happen."""
    if not event.wait(GATE_SECONDS * 5):
        raise AssertionError(f"{what} did not happen within {GATE_SECONDS * 5:.0f} seconds")


def joined(*workers: Worker) -> bool:
    """True when every worker finished within the join deadline."""
    for worker in workers:
        worker.join(JOIN_SECONDS)
    return not any(worker.is_alive() for worker in workers)


@contextmanager
def after_first_member(hooks: dict[str, Callable[[], None]]) -> Iterator[None]:
    """While active, the thread named in ``hooks`` calls its hook once, right after it unpacks its first archive member."""
    original = vars(tarfile.TarFile)[EXTRACT_ONE]
    fired: set[str] = set()
    guard = threading.Lock()

    def wrapped(*arguments: Any, **options: Any) -> Any:
        result = original(*arguments, **options)
        name = threading.current_thread().name
        with guard:
            fire = name in hooks and name not in fired
            fired.add(name)
        if fire:
            hooks[name]()
        return result

    with mock.patch.object(tarfile.TarFile, EXTRACT_ONE, wrapped):
        yield
