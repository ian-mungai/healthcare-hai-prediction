"""The size and time limits of a request body that arrives in pieces.

The web adapter reads a body only after the key check and the rate limit. A body may arrive without a declared length (chunked), so the limit is on
the bytes actually received. A client that sends too slowly is cut off by a total deadline for the whole body. The budget holds only counts and a
clock, so the same rules run in the cases and in the adapter.
"""

from __future__ import annotations

from collections.abc import Callable

MAX_BODY_BYTES = 1024
BODY_SECONDS = 5.0


class BodyRefused(Exception):
    """A body the API refuses to read further. ``status`` is the HTTP status of the answer and ``error`` its short name."""

    status = 400
    error = "invalid_body"


class BodyInvalid(BodyRefused):
    status = 400
    error = "invalid_body"


class BodyTooLarge(BodyRefused):
    status = 413
    error = "body_too_large"


class BodyTooSlow(BodyRefused):
    status = 408
    error = "body_too_slow"


class BodyBudget:
    def __init__(self, *, limit: int = MAX_BODY_BYTES, seconds: float = BODY_SECONDS, clock: Callable[[], float]) -> None:
        self.limit, self.total = limit, 0
        self._clock = clock
        self._expires = clock() + seconds

    def declare(self, content_length: str | None) -> None:
        """Check a declared length before any byte is read. A body without one is checked as it arrives."""
        if content_length is None:
            return
        if not content_length.isascii() or not content_length.isdigit():
            raise BodyInvalid("the declared length is not a number")
        if int(content_length) > self.limit:
            raise BodyTooLarge("the declared length is over the limit")

    def remaining(self) -> float:
        """The seconds left for the whole body, or ``BodyTooSlow`` when none are left."""
        left = self._expires - self._clock()
        if left <= 0:
            raise BodyTooSlow("the body did not arrive in time")
        return left

    def add(self, chunk: bytes) -> None:
        self.remaining()
        self.total += len(chunk)
        if self.total > self.limit:
            raise BodyTooLarge("the body is over the limit")
