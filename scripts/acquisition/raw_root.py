"""The bucket roots that source objects live under, kept free of repository imports so the bronze worker snapshot stays small."""

import os
import re

RAW_ROOT = "raw/"
READ_RAW_VARIABLE = "HAI_READ_RAW_ROOT"
# A test capture lives in its own folder beside raw/, one per test run (owner decision D4). Bronze never reads it.
TEST_ROOT = re.compile(r"ingestion_test/[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}/")


def is_test_root(root: str) -> bool:
    """True for exactly ``ingestion_test/<run ID>/``."""
    return TEST_ROOT.fullmatch(root) is not None


def read_roots() -> tuple[str, ...]:
    """The roots bronze may list. Raw joins the bucket root only when the role holds the raw read grant (``HAI_READ_RAW_ROOT=true``)."""
    return ("", RAW_ROOT) if os.environ.get(READ_RAW_VARIABLE) == "true" else ("",)


def strip_root(key: str) -> str:
    """Return the key without a leading read root, for code that reads the publisher and collection by position."""
    return key.removeprefix(RAW_ROOT)
