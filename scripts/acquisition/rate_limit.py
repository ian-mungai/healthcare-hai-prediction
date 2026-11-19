"""A publisher's HTTP 429 answer as one typed outcome, for each transport that stops at it instead of retrying.

The outcome carries the publisher's Retry-After header as the short printable text it sent, or none. It is not a ValueError, so a batch loop that holds a
ValueError and goes on to its next request never swallows it. The capture run turns the text into a delay and a stored per-source cooldown
(``source_cooldown``); nothing here reads a clock or storage.
"""

from typing import Any

HEADER_LIMIT = 64


class RateLimited(Exception):
    """The publisher answered 429. ``retry_after`` is its Retry-After header text, at most HEADER_LIMIT printable ASCII characters, or None."""

    def __init__(self, retry_after: str | None = None) -> None:
        super().__init__("Publisher rate limit")
        self.retry_after = retry_after


def from_response(response: Any) -> RateLimited:
    """The typed outcome of a 429 response: its Retry-After header when it is short printable ASCII, else none. The header is never logged here."""
    value = response.getheader("Retry-After", "") if hasattr(response, "getheader") else response.headers.get("Retry-After", "")
    text = value if isinstance(value, str) else ""
    keep = 0 < len(text) <= HEADER_LIMIT and text.isascii() and text.isprintable()
    return RateLimited(text if keep else None)
