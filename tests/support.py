"""Helpers shared by the repository's pytest safeguards."""


def check(condition: object, message: str) -> None:
    """Fail the current test with ``message`` when ``condition`` is false.

    Used instead of bare ``assert`` so tests keep their meaning under ``python -O`` and need no lint exception.
    Raises ``AssertionError``, which pytest reports as a failure exactly as it reports a failed ``assert``.
    """
    if not condition:
        raise AssertionError(message)
