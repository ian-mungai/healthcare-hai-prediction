"""A token bucket for each known API key, held in this process.

One service process serves a deployment, so the bucket needs no shared store. A restart gives each key a full bucket. Only identified keys get a
bucket, so a caller who sends many unknown keys cannot grow the table.
"""

from __future__ import annotations

import math
import threading
from collections.abc import Callable


class RateLimiter:
    def __init__(self, *, rate_per_minute: int, burst: int, clock: Callable[[], float]) -> None:
        if rate_per_minute < 1 or burst < 1:
            raise ValueError("the rate and the burst must be positive")
        self._per_second = rate_per_minute / 60.0
        self._burst = float(burst)
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def check(self, key_id: str) -> float | None:
        """Take one token. Return None when the request may go on, else the seconds (rounded up, at least 1) until a token exists."""
        now = self._clock()
        with self._lock:
            tokens, stamp = self._buckets.get(key_id, (self._burst, now))
            tokens = min(self._burst, tokens + max(0.0, now - stamp) * self._per_second)
            if tokens >= 1.0:
                self._buckets[key_id] = (tokens - 1.0, now)
                return None
            self._buckets[key_id] = (tokens, now)
            return float(max(1, math.ceil((1.0 - tokens) / self._per_second)))

    def tracked(self) -> int:
        with self._lock:
            return len(self._buckets)
