"""A bounded buffer between answered requests and the predictions Iceberg table.

A request reserves room before it does any work, adds its row when it answers and gives the room back when it ends without a row. A full buffer refuses
the reservation, so the service answers 503 with a retry time and never drops a row it acknowledged. A flush (the service's background thread, or a case)
sends the pending rows to the sink with a deadline and removes them only when the sink returned, so a failed append is sent again and a successful one is
never sent twice. Iceberg commits are atomic, so a failed append wrote nothing. An append whose answer was lost may be sent again, and a reader removes
such rows by ``request_id``.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

LOG = logging.getLogger(__name__)


RETRY_AFTER_SECONDS = 10


class RowRefused(ValueError):
    """A row whose columns differ from the table contract."""


class BufferFull(Exception):
    """No room for another row: the pending rows and the reserved rooms fill the capacity."""


class Sink(Protocol):
    def append(self, rows: Sequence[Mapping[str, Any]], timeout: float) -> None: ...


class PredictionBuffer:
    def __init__(self, sink: Sink, *, columns: Mapping[str, str], capacity: int, batch: int, deadline_seconds: float, clock: Callable[[], float]) -> None:
        if capacity < 1 or batch < 1 or deadline_seconds <= 0:
            raise ValueError("the capacity, the batch and the deadline must be positive")
        self._sink, self._columns = sink, frozenset(columns)
        self.capacity, self.batch, self.deadline_seconds = capacity, batch, deadline_seconds
        self._rows: list[dict[str, Any]] = []
        self._reserved = 0
        self._lock = threading.Lock()
        self._flushing = threading.Lock()

    def reserve(self) -> None:
        """Take room for one row, or raise ``BufferFull``. Every reservation ends in ``add`` or ``release``."""
        with self._lock:
            if len(self._rows) + self._reserved >= self.capacity:
                raise BufferFull
            self._reserved += 1

    def release(self) -> None:
        with self._lock:
            self._reserved -= 1

    def add(self, row: Mapping[str, Any]) -> None:
        """Add the row for a reservation and end that reservation. A row with other columns is refused and the reservation stays."""
        if set(row) != self._columns:
            raise RowRefused("the row has other columns than the predictions contract")
        with self._lock:
            self._rows.append(dict(row))
            self._reserved -= 1

    def pending(self) -> int:
        with self._lock:
            return len(self._rows)

    def flush(self) -> int:
        """Send the pending rows once. Return the number appended; 0 when nothing was pending, another flush runs or the sink failed."""
        if not self._flushing.acquire(blocking=False):
            return 0
        try:
            with self._lock:
                snapshot = list(self._rows)
            if not snapshot:
                return 0
            try:
                self._sink.append(snapshot, self.deadline_seconds)
            except Exception as error:
                LOG.warning("prediction append failed (%s); %d rows kept", type(error).__name__, len(snapshot))
                return 0
            with self._lock:
                del self._rows[: len(snapshot)]
            return len(snapshot)
        finally:
            self._flushing.release()
