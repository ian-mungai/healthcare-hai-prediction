"""The one protection every secret read with the AWS SDK uses: no DEBUG log record while the call runs.

The SDK logs the body of each response at DEBUG, and the body of a Secrets Manager or Parameter Store read holds the decrypted value. A caller that reads a
secret value wraps the call in ``quiet_sdk_logs``. The switch is global to the process, so a record from another thread during the call is lost too.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def quiet_sdk_logs() -> Iterator[None]:
    """Switch off DEBUG logging for the duration of a secret read: the SDK logs response bodies at DEBUG, and the body holds the decrypted value."""
    previous = logging.root.manager.disable
    logging.disable(max(previous, logging.DEBUG))
    try:
        yield
    finally:
        logging.disable(previous)
