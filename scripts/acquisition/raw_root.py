"""The bucket roots that source objects live under, kept import-free so the bronze worker snapshot stays small."""

RAW_ROOT = "raw/"
READ_ROOTS = ("", RAW_ROOT)


def strip_root(key: str) -> str:
    """Return the key without a leading read root, for code that reads the publisher and collection by position."""
    return key.removeprefix(RAW_ROOT)
